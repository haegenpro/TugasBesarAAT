"""Run-owned libvirt VMs and pinned k3s bootstrap; no shell interpolation locally."""
from contextlib import contextmanager
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
import time
import uuid
from urllib.request import urlopen
import xml.etree.ElementTree as ET

from .inventory import fingerprint, health_rbac, k3s_config, memory_budget, owned, render, require, resource_uuid, validate_resize

LABEL = "aat.platform.cluster"


class Cluster:
    def __init__(self, cfg):
        self.cfg = cfg
        self.state = Path(cfg["state_dir"])
        self.private = self.state / "secrets"
        self.key = self.private / "ssh_ed25519"
        self.redactions = []

    def run(self, args, *, input=None, check=True, timeout=120, cwd=None):
        result = subprocess.run([str(a) for a in args], input=input, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout, cwd=cwd, env={**os.environ, "LC_ALL": "C"})
        if check and result.returncode:
            detail = result.stderr.decode(errors="replace")[-2000:]
            for secret in self.redactions:
                detail = detail.replace(secret, "[redacted]")
            raise RuntimeError(f"{args[0]} failed ({result.returncode}): {detail.strip()}")
        return result

    def virsh(self, *args, **kwargs):
        return self.run(["virsh", "--connect", self.cfg["libvirt_uri"], *args], **kwargs)

    def docker(self, *args, **kwargs):
        return self.run(["docker", *args], **kwargs)

    def doctor(self):
        programs = ("virsh", "qemu-img", "qemu-system-x86_64", "genisoimage", "ssh", "ssh-keygen", "openssl", "docker", "ip")
        missing = [p for p in programs if not shutil.which(p)]
        report = {"cluster_id": self.cfg["cluster_id"], "missing_programs": missing,
                  "kvm_accessible": os.access("/dev/kvm", os.R_OK | os.W_OK),
                  "maximum_guest_disks_gib": sum(n["root_gib"] + n["data_gib"] for n in self.cfg["nodes"])}
        location = self.state
        while not location.exists():
            location = location.parent
        report["free_disk_gib"] = round(shutil.disk_usage(location).free / (1024**3), 2)
        report["libvirt_accessible"] = not missing and self.virsh("list", "--all", "--name", check=False).returncode == 0
        report["docker_accessible"] = bool(shutil.which("docker")) and self.docker("version", "--format", "{{.Server.Version}}", check=False).returncode == 0
        meminfo = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
        total, available = (int(meminfo[field].split()[0]) // 1024 for field in ("MemTotal", "MemAvailable"))
        rss = 0
        if report["libvirt_accessible"]:
            active = self.virsh("list", "--name").stdout.decode().split()
            for node in self.cfg["nodes"]:
                if node["name"] in active:
                    xml = self.virsh("dumpxml", node["name"]).stdout.decode()
                    if owned(xml, self.cfg):
                        stats = self.virsh("dommemstat", node["name"], check=False)
                        for line in stats.stdout.decode().splitlines():
                            if line.startswith("rss "):
                                rss += int(line.split()[1]) // 1024
        report.update(memory_budget(self.cfg, total, available, rss))
        report["ready"] = not missing and report["kvm_accessible"] and report["libvirt_accessible"] and report["docker_accessible"] and report["free_disk_gib"] >= 8 and report["memory_budget_ok"]
        report["capacity_note"] = "Bootstrap resources only; full Citus/app/load fit requires measurement. Sparse disk maxima are not reserved host capacity."
        return report

    @contextmanager
    def locked(self, allow_resize=False):
        require(not self.state.is_symlink(), "state directory must not be a symlink")
        self.state.mkdir(parents=True, exist_ok=True, mode=0o755)
        require(self.state.stat().st_uid == os.geteuid(), "state directory must belong to the invoking user")
        marker = self.state / "owner.json"
        with (self.state / ".lock").open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("another platform operation is using this state directory") from exc
            if marker.exists():
                value = json.loads(marker.read_text())
                require(value.get("cluster_id") == self.cfg["cluster_id"], "state belongs to another cluster")
                require(allow_resize or value.get("inventory_sha256") == fingerprint(self.cfg),
                        "inventory changed since provisioning; preserve data and explicitly reconcile the existing topology before resuming")
            else:
                require(not allow_resize, "resize requires an existing owned state marker")
                require(set(p.name for p in self.state.iterdir()) <= {".lock"}, "refusing to adopt an unmarked nonempty state directory")
                marker.write_text(json.dumps({"cluster_id": self.cfg["cluster_id"], "inventory_sha256": fingerprint(self.cfg)}, indent=2) + "\n")
            for name in ("cache", "disks", "seeds", "render", "evidence"):
                (self.state / name).mkdir(exist_ok=True, mode=0o755)
            self.private.mkdir(exist_ok=True, mode=0o700)
            self.private.chmod(0o700)
            yield

    def fetch(self, url, sha256, name):
        destination = self.state / "cache" / name
        def digest(path):
            with path.open("rb") as stream:
                return hashlib.file_digest(stream, "sha256").hexdigest()
        if destination.exists():
            require(digest(destination) == sha256, f"cached {name} checksum mismatch")
            return destination
        print(f"Downloading pinned artifact: {name}", flush=True)
        partial = destination.with_suffix(destination.suffix + ".partial")
        with urlopen(url, timeout=60) as response, partial.open("wb") as output:
            shutil.copyfileobj(response, output, 1024*1024)
        require(digest(partial) == sha256, f"downloaded {name} checksum mismatch; artifact was not used")
        partial.replace(destination)
        destination.chmod(0o644)
        return destination

    def assert_resource(self, xml, name):
        root = ET.fromstring(xml)
        require(owned(xml, self.cfg) and root.findtext("uuid") == resource_uuid(self.cfg, name),
                f"refusing to modify unowned libvirt resource {name}")
        if root.tag == "domain":
            expected = {str(self.state / "disks" / f"{name}-{kind}.qcow2") for kind in ("root", "data")}
            actual = {disk.find("source").get("file") for disk in root.findall("./devices/disk")
                      if disk.get("device") == "disk" and disk.find("source") is not None}
            require(actual == expected, f"refusing to adopt different disk paths on {name}")

    def preflight_resources(self):
        networks = self.virsh("net-list", "--all", "--name").stdout.decode().split()
        domains = self.virsh("list", "--all", "--name").stdout.decode().split()
        import ipaddress
        selected = ipaddress.IPv4Network(self.cfg["network"]["cidr"])
        for name in networks:
            xml = self.virsh("net-dumpxml", name).stdout.decode()
            if name == self.cfg["network"]["name"]:
                self.assert_resource(xml, "network")
            else:
                for ip in ET.fromstring(xml).findall("ip"):
                    if ip.get("address") and ":" not in ip.get("address"):
                        other = ipaddress.IPv4Network(f"{ip.get('address')}/{ip.get('netmask', ip.get('prefix', '24'))}", strict=False)
                        require(not selected.overlaps(other), f"network conflicts with existing libvirt network {name}")
        for node in self.cfg["nodes"]:
            if node["name"] in domains:
                self.assert_resource(self.virsh("dumpxml", node["name"]).stdout.decode(), node["name"])

    def prepare(self):
        if not self.key.exists():
            self.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", self.cfg["cluster_id"], "-f", self.key])
        self.key.chmod(0o600)
        render(self.cfg, self.state / "render", self.key.with_suffix(".pub").read_text().strip())
        base = self.fetch(self.cfg["cloud_image"]["url"], self.cfg["cloud_image"]["sha256"], "ubuntu-noble.qcow2")
        for node in self.cfg["nodes"]:
            root = self.state / "disks" / f"{node['name']}-root.qcow2"
            data = self.state / "disks" / f"{node['name']}-data.qcow2"
            if not root.exists():
                self.run(["qemu-img", "create", "-f", "qcow2", "-F", "qcow2", "-b", base, root, f"{node['root_gib']}G"])
            if not data.exists():
                self.run(["qemu-img", "create", "-f", "qcow2", data, f"{node['data_gib']}G"])
            seed = self.state / "seeds" / node["name"]
            seed.mkdir(exist_ok=True)
            if not (seed / "seed.iso").exists():
                for filename in ("user-data", "meta-data", "network-config"):
                    shutil.copyfile(self.state / "render" / node["name"] / filename, seed / filename)
                self.run(["genisoimage", "-quiet", "-output", seed / "seed.iso", "-volid", "cidata", "-joliet", "-rock",
                          "user-data", "meta-data", "network-config"], cwd=seed)

    def start_vms(self):
        network = self.cfg["network"]["name"]
        names = self.virsh("net-list", "--all", "--name").stdout.decode().split()
        if network not in names:
            self.virsh("net-define", self.state / "render" / "network.xml", "--validate")
        if network not in self.virsh("net-list", "--name").stdout.decode().split():
            self.virsh("net-start", network)
        names = self.virsh("list", "--all", "--name").stdout.decode().split()
        active = self.virsh("list", "--name").stdout.decode().split()
        for node in self.cfg["nodes"]:
            if node["name"] not in names:
                self.virsh("define", self.state / "render" / node["name"] / "domain.xml", "--validate")
            if node["name"] not in active:
                print(f"Starting VM {node['name']}", flush=True)
                self.virsh("start", node["name"])

    def proxy(self):
        name = self.cfg["cluster_id"] + "-api"
        image = self.cfg["api_proxy_image"]
        config_hash = hashlib.sha256((self.state / "render" / "haproxy.cfg").read_bytes()
                                    + (self.private / "health.pem").read_bytes()
                                    + (self.private / "server-ca.crt").read_bytes()).hexdigest()
        inspect = self.docker("inspect", name, check=False)
        if inspect.returncode == 0:
            value = json.loads(inspect.stdout)[0]
            require((value["Config"].get("Labels") or {}).get(LABEL) == self.cfg["cluster_id"], "API proxy container belongs to another owner")
            require(value["Config"]["Image"] == image, "existing proxy image differs from inventory")
            if (value["Config"].get("Labels") or {}).get(LABEL + ".config") == config_hash:
                if not value["State"]["Running"]:
                    self.docker("start", name)
                return
            self.docker("rm", "--force", name)
        self.docker("pull", "--platform", "linux/amd64", image, timeout=300)
        args = ["--network", "host", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                "--memory", "128m", "--cpus", "0.5", "--label", f"{LABEL}={self.cfg['cluster_id']}",
                "--label", f"{LABEL}.config={config_hash}", "--user", f"{os.getuid()}:{os.getgid()}",
                "--mount", f"type=bind,src={self.private / 'health.pem'},dst=/usr/local/etc/haproxy/health.pem,readonly",
                "--mount", f"type=bind,src={self.private / 'server-ca.crt'},dst=/usr/local/etc/haproxy/server-ca.crt,readonly",
                "--mount", f"type=bind,src={self.state / 'render' / 'haproxy.cfg'},dst=/usr/local/etc/haproxy/haproxy.cfg,readonly"]
        self.docker("run", "--rm", *args, image, "haproxy", "-c", "-f", "/usr/local/etc/haproxy/haproxy.cfg")
        self.docker("run", "--detach", "--name", name, "--restart", "unless-stopped", *args,
                    image, "haproxy", "-W", "-db", "-f", "/usr/local/etc/haproxy/haproxy.cfg")

    def ssh(self, node, args, *, input=None, check=True, timeout=120):
        return self.run(["ssh", "-F", "/dev/null", "-i", self.key, "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none",
                         "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                         "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={self.private / 'known_hosts'}",
                         "-o", "LogLevel=ERROR", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2",
                         f"{self.cfg['ssh_user']}@{node['address']}", shlex.join(args)],
                        input=input, check=check, timeout=timeout)

    def wait_ssh(self, node):
        deadline = time.monotonic() + 240
        while self.ssh(node, ["true"], check=False, timeout=10).returncode:
            require(time.monotonic() < deadline, f"SSH did not become ready on {node['name']}")
            time.sleep(2)
        self.ssh(node, ["sudo", "-n", "cloud-init", "status", "--wait"], timeout=180)
        self.ssh(node, ["findmnt", "/var/lib/aat"])

    def upload(self, node, path, data, mode="0600"):
        self.ssh(node, ["sudo", "-n", "install", "-D", "-m", mode, "/dev/stdin", path], input=data, timeout=180)

    def install_node(self, node, binary, installer, token, first, join_server=None):
        print(f"Bootstrapping k3s {node['role']} {node['name']}", flush=True)
        self.wait_ssh(node)
        expected = {"cluster_id": self.cfg["cluster_id"], "inventory_sha256": fingerprint(self.cfg)}
        desired = k3s_config(self.cfg, node, token, first)
        if join_server and not first:
            desired["server"] = f"https://{join_server}:6443"
        config_data = (json.dumps(desired, indent=2) + "\n").encode()
        marker = self.ssh(node, ["sudo", "-n", "cat", "/var/lib/aat/platform-owner.json"], check=False)
        if marker.returncode == 0:
            actual = json.loads(marker.stdout)
            accepted = json.loads((self.state / "owner.json").read_text()).get("previous_guest_fingerprints", [])
            require(actual == expected or (actual.get("cluster_id") == self.cfg["cluster_id"] and actual.get("inventory_sha256") in accepted),
                    f"guest marker mismatch on {node['name']}")
            if actual != expected:
                self.upload(node, "/var/lib/aat/platform-owner.json", (json.dumps(expected) + "\n").encode())
            service = "k3s" if node["role"] == "server" else "k3s-agent"
            configured = self.ssh(node, ["sudo", "-n", "sha256sum", "/etc/rancher/k3s/config.yaml"]).stdout.decode().split()[0]
            changed = configured != hashlib.sha256(config_data).hexdigest()
            if changed:
                self.upload(node, "/etc/rancher/k3s/config.yaml", config_data)
            if self.ssh(node, ["test", "-f", f"/etc/systemd/system/{service}.service"], check=False).returncode:
                self.ssh(node, ["sudo", "-n", "env", "INSTALL_K3S_SKIP_DOWNLOAD=true", f"INSTALL_K3S_EXEC={node['role']}",
                               "sh", "/tmp/aat-k3s-install.sh"], timeout=240)
            self.ssh(node, ["sudo", "-n", "systemctl", "restart" if changed else "start", service], timeout=240)
            return
        existing = self.ssh(node, ["test", "-e", "/etc/rancher/k3s/config.yaml"], check=False)
        require(existing.returncode != 0, f"unmarked k3s configuration on {node['name']}; inspect before retrying")
        self.upload(node, "/usr/local/bin/k3s", binary.read_bytes(), "0755")
        self.upload(node, "/tmp/aat-k3s-install.sh", installer.read_bytes(), "0700")
        self.upload(node, "/etc/rancher/k3s/config.yaml", config_data)
        # Mark the owned configuration before invoking systemd so a failed first
        # start can be retried without adopting an unrelated installation.
        self.upload(node, "/var/lib/aat/platform-owner.json", (json.dumps(expected) + "\n").encode())
        self.ssh(node, ["sudo", "-n", "env", "INSTALL_K3S_SKIP_DOWNLOAD=true", f"INSTALL_K3S_EXEC={node['role']}",
                        "sh", "/tmp/aat-k3s-install.sh"], timeout=240)

    def ready_server(self, node):
        deadline = time.monotonic() + 240
        while self.ssh(node, ["sudo", "-n", "k3s", "kubectl", "get", "--raw=/readyz", "--request-timeout=5s"], check=False, timeout=15).returncode:
            require(time.monotonic() < deadline, f"API did not become ready on {node['name']}")
            time.sleep(3)

    def health_identity(self, first):
        """Issue a restricted client identity; never give the proxy admin keys."""
        def remote(*args, input=None):
            return self.ssh(first, ["sudo", "-n", "k3s", "kubectl", *args], input=input, timeout=60)
        remote("apply", "-f", "-", input=json.dumps(health_rbac(self.cfg)).encode())
        for name in ("server", "client"):
            contents = self.ssh(first, ["sudo", "-n", "cat", f"/var/lib/rancher/k3s/server/tls/{name}-ca.crt"]).stdout
            (self.private / f"{name}-ca.crt").write_bytes(contents)
        pem = self.private / "health.pem"
        if pem.exists():
            valid = self.run(["openssl", "x509", "-checkend", "86400", "-noout", "-in", pem], check=False).returncode == 0
            trusted = self.run(["openssl", "verify", "-CAfile", self.private / "client-ca.crt", pem], check=False).returncode == 0
            if valid and trusted:
                return
        key, request = self.private / "health.key", self.private / "health.csr"
        identity = self.cfg["cluster_id"] + "-api-health"
        self.run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                  "-out", request, "-subj", "/CN=" + identity])
        key.chmod(0o600)
        name = identity + "-" + uuid.uuid4().hex[:10]
        csr = {"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest",
               "metadata": {"name": name, "labels": {"platform.aat/cluster": self.cfg["cluster_id"]}},
               "spec": {"request": base64.b64encode(request.read_bytes()).decode(),
                        "signerName": "kubernetes.io/kube-apiserver-client", "expirationSeconds": 2592000,
                        "usages": ["client auth"]}}
        remote("create", "-f", "-", input=json.dumps(csr).encode())
        remote("certificate", "approve", name)
        deadline = time.monotonic() + 45
        while True:
            value = json.loads(remote("get", "csr", name, "-o", "json").stdout)
            certificate = value.get("status", {}).get("certificate")
            if certificate:
                pem.write_bytes(key.read_bytes() + b"\n" + base64.b64decode(certificate, validate=True))
                pem.chmod(0o600)
                break
            require(time.monotonic() < deadline, "API health certificate was not issued")
            time.sleep(1)
        remote("delete", "csr", name, "--wait=false")

    def bootstrap(self):
        capacity = self.doctor()
        require(capacity["ready"], "bootstrap prerequisites or memory headroom failed; run doctor")
        self.redactions = []
        version = self.cfg["k3s"]
        binary = self.fetch(version["binary_url"], version["binary_sha256"], "k3s")
        binary.chmod(0o755)
        installer = self.fetch(version["installer_url"], version["installer_sha256"], "k3s-install.sh")
        token_path = self.private / "bootstrap-token"
        if not token_path.exists():
            token_path.write_text(secrets.token_hex(32) + "\n")
            token_path.chmod(0o600)
        token = token_path.read_text().strip()
        self.redactions.append(token)
        seed = json.loads((self.state / "owner.json").read_text()).get("bootstrap_seed", self.cfg["nodes"][0]["name"])
        first = next(n for n in self.cfg["nodes"] if n["name"] == seed and n["role"] == "server")
        self.install_node(first, binary, installer, token, True)
        self.ready_server(first)
        self.health_identity(first)
        self.proxy()
        secure_token = self.ssh(first, ["sudo", "-n", "cat", "/var/lib/rancher/k3s/server/token"]).stdout.decode().strip()
        require(secure_token.startswith("K10") and "::" in secure_token, "server did not issue a CA-pinned join token")
        self.redactions.append(secure_token)
        for node in self.cfg["nodes"]:
            if node["name"] == first["name"]:
                continue
            self.install_node(node, binary, installer, secure_token, False, join_server=first["address"])
            if node["role"] == "server":
                self.ready_server(node)
        raw = self.ssh(first, ["sudo", "-n", "cat", "/etc/rancher/k3s/k3s.yaml"]).stdout.decode()
        require("server: https://127.0.0.1:6443" in raw, "unexpected k3s kubeconfig endpoint")
        kubeconfig = self.private / "kubeconfig"
        kubeconfig.write_text(raw.replace("server: https://127.0.0.1:6443", "server: " + self.cfg["api_endpoint"]))
        kubeconfig.chmod(0o600)
        self.kubectl("wait", "--for=condition=Ready", "nodes", "--all", "--timeout=240s", timeout=270)
        self.kubectl("apply", "-f", self.state / "render" / "namespace.json")
        self.verify()

    def kubectl(self, *args, **kwargs):
        return self.run([self.state / "cache" / "k3s", "kubectl", "--kubeconfig", self.private / "kubeconfig", *args], **kwargs)

    def install_tester(self):
        tester = self.cfg["tester"]
        manifest = self.fetch(tester["manifest_url"], tester["manifest_sha256"], "tester-agent.yaml")
        require(tester["image"] in manifest.read_text(), "Tester manifest does not contain required image")
        self.kubectl("apply", "-f", manifest)
        self.kubectl("rollout", "status", "daemonset/tester-agent", "-n", "tester-agent", "--timeout=240s", timeout=270)
        daemonset = json.loads(self.kubectl("get", "daemonset", "tester-agent", "-n", "tester-agent", "-o", "json").stdout)
        require(daemonset["status"]["desiredNumberScheduled"] == 4 and daemonset["status"]["numberReady"] == 4,
                "Tester Agent must be Ready on all four nodes")
        (self.state / "evidence" / "tester-agent.json").write_text(json.dumps(daemonset, indent=2) + "\n")

    def verify(self):
        nodes = json.loads(self.kubectl("get", "nodes", "-o", "json").stdout)
        expected = {n["name"] for n in self.cfg["nodes"]}
        require({n["metadata"]["name"] for n in nodes["items"]} == expected, "cluster node inventory differs")
        boot_ids = {n["status"]["nodeInfo"]["bootID"] for n in nodes["items"]}
        require(len(boot_ids) == 4, "four distinct guest kernel boot IDs required")
        for node in nodes["items"]:
            require(any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"]["conditions"]), "node is not Ready")
            require(node["status"]["nodeInfo"]["kubeletVersion"] == self.cfg["k3s"]["version"], "unexpected Kubernetes version")
            require(not any(c["type"] in ("MemoryPressure", "DiskPressure", "PIDPressure") and c["status"] == "True"
                            for c in node["status"]["conditions"]), "node reports resource pressure")
        for deployment in ("coredns", "local-path-provisioner", "metrics-server"):
            self.kubectl("rollout", "status", "deployment/" + deployment, "-n", "kube-system", "--timeout=180s", timeout=210)
        evidence = self.state / "evidence"
        evidence.mkdir(exist_ok=True)
        (evidence / "nodes.json").write_text(json.dumps(nodes, indent=2) + "\n")
        summary = {"inventory_sha256": fingerprint(self.cfg), "node_count": 4, "distinct_boot_ids": 4,
                   "k3s_version": self.cfg["k3s"]["version"], "api_endpoint": self.cfg["api_endpoint"],
                   "scope": "Kubernetes bootstrap only; application HA and load acceptance remain pending"}
        (evidence / "bootstrap.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2), flush=True)

    def verify_failover(self):
        """Bounded infrastructure fault against our first VM; always restore it."""
        self.verify()
        first = self.cfg["nodes"][0]
        agent = next(n for n in self.cfg["nodes"] if n["role"] == "agent")
        self.assert_resource(self.virsh("dumpxml", first["name"]).stdout.decode(), first["name"])
        result = {"scenario": "first-server VM pause", "node": first["name"],
                  "scope": "Kubernetes API/agent continuity only; not canonical database durability", "passed": False}
        started = time.monotonic()
        try:
            require(self.virsh("domstate", first["name"]).stdout.decode().strip() == "running", "first VM must be running before the fault")
            print(f"Testing API continuity while {first['name']} is paused", flush=True)
            self.virsh("suspend", first["name"])
            require(self.virsh("domstate", first["name"]).stdout.decode().strip() == "paused", "first VM did not pause")
            deadline = time.monotonic() + 60
            while True:
                reply = self.kubectl("get", "--raw=/readyz", "--request-timeout=5s", check=False, timeout=10)
                if reply.returncode == 0:
                    break
                require(time.monotonic() < deadline, "fixed API endpoint did not recover within 60s")
                time.sleep(2)
            result["api_available_after_seconds"] = round(time.monotonic() - started, 3)
            lease_args = ("get", "lease", agent["name"], "-n", "kube-node-lease", "-o", "json", "--request-timeout=5s")
            before = json.loads(self.kubectl(*lease_args).stdout)["metadata"]["resourceVersion"]
            deadline = time.monotonic() + 60
            while True:
                current = json.loads(self.kubectl(*lease_args).stdout)["metadata"]["resourceVersion"]
                if current != before:
                    break
                require(time.monotonic() < deadline, "agent did not renew through surviving control-plane nodes")
                time.sleep(2)
            probe = self.cfg["cluster_id"] + "-failover-probe-" + uuid.uuid4().hex[:8]
            self.kubectl("create", "configmap", probe, "-n", self.cfg["namespace"], "--from-literal=probe=api-write", "--request-timeout=5s")
            self.kubectl("delete", "configmap", probe, "-n", self.cfg["namespace"], "--request-timeout=5s")
            result.update({"agent_renewed_while_first_server_paused": True, "control_plane_write_succeeded": True})
        finally:
            state = self.virsh("domstate", first["name"]).stdout.decode().strip()
            print(f"Restoring {first['name']}", flush=True)
            if state == "paused":
                self.virsh("resume", first["name"])
            elif state == "shut off":
                self.virsh("start", first["name"])
            result["elapsed_seconds"] = round(time.monotonic() - started, 3)
            (self.state / "evidence" / "api-failover.json").write_text(json.dumps(result, indent=2) + "\n")
        self.ready_server(first)
        self.kubectl("wait", "--for=condition=Ready", "node/" + first["name"], "--timeout=240s", timeout=270)
        self.verify()
        result.update({"passed": True, "first_server_restored": True, "elapsed_seconds": round(time.monotonic() - started, 3)})
        (self.state / "evidence" / "api-failover.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2), flush=True)

    def stop(self):
        # Deliberately preserves disks, definitions and network. No delete/reset
        # command is supplied; application data must not be removed implicitly.
        self.preflight_resources()
        for node in reversed(self.cfg["nodes"]):
            if node["name"] in self.virsh("list", "--name").stdout.decode().split():
                self.virsh("shutdown", node["name"])
        name = self.cfg["cluster_id"] + "-api"
        inspect = self.docker("inspect", name, check=False)
        if inspect.returncode == 0:
            value = json.loads(inspect.stdout)[0]
            require((value["Config"].get("Labels") or {}).get(LABEL) == self.cfg["cluster_id"], "refusing to stop an unowned proxy")
            self.docker("stop", name)

    def resize(self, previous):
        validate_resize(previous, self.cfg)
        marker_path = self.state / "owner.json"
        marker = json.loads(marker_path.read_text())
        require(marker["inventory_sha256"] == fingerprint(previous), "previous inventory does not match provisioned state")
        self.preflight_resources()
        stopped = set(self.virsh("list", "--all", "--state-shutoff", "--name").stdout.decode().split())
        require(all(n["name"] in stopped for n in self.cfg["nodes"]), "all four owned VMs must be shut off before resize")
        for node in self.cfg["nodes"]:
            xml = self.virsh("dumpxml", "--inactive", node["name"]).stdout.decode()
            self.assert_resource(xml, node["name"])
            root = ET.fromstring(xml)
            for tag in ("memory", "currentMemory"):
                element = root.find(tag)
                if element is not None:
                    element.set("unit", "MiB")
                    element.text = str(node["memory_mib"])
            root.find("vcpu").text = str(node["vcpus"])
            destination = self.state / "render" / node["name"] / "resized-domain.xml"
            ET.ElementTree(root).write(destination, encoding="unicode")
            self.virsh("define", destination, "--validate")
        old = marker.setdefault("previous_guest_fingerprints", [])
        if fingerprint(previous) not in old:
            old.append(fingerprint(previous))
        marker["inventory_sha256"] = fingerprint(self.cfg)
        marker_path.write_text(json.dumps(marker, indent=2) + "\n")
        render(self.cfg, self.state / "render", self.key.with_suffix(".pub").read_text().strip())
        print("Updated stopped VM CPU/RAM; disks, node identities and cluster settings preserved.", flush=True)

    def up(self):
        report = self.doctor()
        print(json.dumps(report, indent=2), flush=True)
        require(report["ready"], "host prerequisites failed; see doctor output")
        self.preflight_resources()
        self.prepare()
        self.start_vms()
        self.bootstrap()
        self.install_tester()
