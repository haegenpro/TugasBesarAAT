"""Linux controller for a fresh two-host formation; legacy guest bootstrap reused."""
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

from .cluster import LocalCluster, LABEL
from .host_agent import HostAgent, MAX_REQUEST, MAX_RESPONSE, digest, bounded_execute
from .inventory import fingerprint, render, require


class HostClient:
    def __init__(self, cluster, host):
        self.cluster, self.host = cluster, host

    def options(self):
        h = self.host
        require(Path(h["ssh_key_file"]).is_file() and Path(h["known_hosts"]).is_file(), "management key and verified known_hosts required")
        return ["-F", "/dev/null", "-i", h["ssh_key_file"], "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none",
                "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=yes",
                "-o", "UserKnownHostsFile=" + h["known_hosts"], "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2"]

    def call(self, operation, **args):
        if self.host["transport"] == "local":
            return HostAgent(self.cluster.cfg, self.host["id"]).dispatch(operation, args)
        request = json.dumps({"version": 1, "inventory": self.cluster.cfg, "host": self.host["id"], "operation": operation, "args": args}).encode()
        require(len(request) <= MAX_REQUEST, "host request quota exceeded")
        # Remote command is constant; all user inputs remain in JSON stdin.
        result = bounded_execute(["ssh", *self.options(), self.host["ssh_user"] + "@" + self.host["address"], "aat-platform-host"], request)
        require(len(result.stdout) <= MAX_RESPONSE and result.returncode == 0, f"{self.host['id']} helper refused {operation}; inspect host prerequisites/state")
        response = json.loads(result.stdout)
        require(response.get("version") == 1 and "result" in response, "unexpected host protocol response")
        return response["result"]

    def transfer(self, path, name):
        sha = digest(path)
        status = self.call("upload-status", name=name, sha256=sha)
        if status["complete"]:
            return status
        if self.host["transport"] == "local":
            agent = HostAgent(self.cluster.cfg, self.host["id"])
            target = agent.path("artifacts/" + name)
            if target.exists():
                require(digest(target) == sha, "immutable artifact differs")
            else:
                shutil.copyfile(path, agent.path("artifacts/" + name + ".partial"))
        else:
            # SFTP is a data protocol; no remote shell sees destination paths.
            quote = lambda p: '"' + str(p).replace("\\", "/").replace('"', '\\"') + '"'
            remote = self.host["state_dir"].replace("\\", "/") + "/artifacts/" + name + ".partial"
            batch = "put " + quote(path) + " " + quote(remote) + "\n"
            self.cluster.run(["sftp", *self.options(), "-b", "-", self.host["ssh_user"] + "@" + self.host["address"]], input=batch.encode(), timeout=600)
        return self.call("seal-upload", name=name, sha256=sha)


class MultiHostCluster(LocalCluster):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.clients = {h["id"]: HostClient(self, h) for h in cfg["hosts"]}

    def virsh(self, *args, **kwargs):
        raise ValueError("raw libvirt operation refused for v2; use provider lifecycle methods; disk replacement is not implemented")

    def doctor(self):
        reports, errors = {}, {}
        if platform.system() != "Linux":
            return {"ready": False, "errors": {"controller": "live v2 controller must run on Linux"}}
        missing = [p for p in ("qemu-img", "genisoimage", "ssh", "sftp", "ssh-keygen", "openssl", "docker") if not shutil.which(p)]
        for name, client in self.clients.items():
            try:
                reports[name] = client.call("doctor")
            except (ValueError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
                errors[name] = type(exc).__name__ + ": host doctor unavailable; verify helper, SSH trust, Ethernet and state ownership"
        distinct = len({r["machine_sha256"] for r in reports.values()}) == 2
        docker_ok = not missing and self.docker("version", "--format", "{{.Server.Version}}", check=False).returncode == 0
        return {"version": 2, "hosts": reports, "errors": errors, "missing": missing, "distinct_machine_identities": distinct,
                "ready": len(reports) == 2 and not errors and not missing and docker_ok and distinct and all(r["ready"] for r in reports.values()),
                "full_stack_acceptance": "blocked: bootstrap sizing is not a full workload/recovery capacity proof"}

    def preflight_resources(self):
        for client in self.clients.values():
            client.call("status")

    def prepare(self):
        for client in self.clients.values():
            client.call("init")
        if not self.key.exists():
            require(all(v["state"] == "absent" for client in self.clients.values() for v in client.call("status")), "original guest SSH key missing; automatic rotation/adoption refused")
            self.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", self.cfg["cluster_id"], "-f", self.key])
        self.key.chmod(0o600)
        render(self.cfg, self.state / "render", self.key.with_suffix(".pub").read_text().strip())
        base = self.fetch(self.cfg["cloud_image"]["url"], self.cfg["cloud_image"]["sha256"], "ubuntu-noble.qcow2")
        vdi = self.state / "cache" / "base.vdi"
        receipt = vdi.with_suffix(".receipt.json")
        version = self.run(["qemu-img", "--version"]).stdout.decode().splitlines()[0]
        if vdi.exists():
            require(receipt.exists() and json.loads(receipt.read_text()) == {"source_sha256": digest(base), "sha256": digest(vdi), "converter": version}, "conversion artifact/receipt differs; preserve and inspect")
        else:
            partial = vdi.with_suffix(".partial.vdi")
            require(not partial.exists(), "interrupted conversion retained; inspect before an explicit retry")
            self.run(["qemu-img", "convert", "-f", "qcow2", "-O", "vdi", base, partial], timeout=600)
            info = json.loads(self.run(["qemu-img", "info", "--output=json", partial]).stdout)
            require(info["format"] == "vdi" and info["virtual-size"] <= min(n["root_gib"] for n in self.cfg["nodes"]) * 2**30, "converted image exceeds root budget")
            partial.replace(vdi)
            receipt.write_text(json.dumps({"source_sha256": digest(base), "sha256": digest(vdi), "converter": version}))
        for host in self.cfg["hosts"]:
            self.clients[host["id"]].transfer(base if host["provider"] == "libvirt" else vdi, "cloud.qcow2" if host["provider"] == "libvirt" else "base.vdi")
        for node in self.cfg["nodes"]:
            folder = self.state / "render" / node["name"]
            seed = folder / "seed.iso"
            # Existing seed is immutable under the controller fingerprint lock.
            if not seed.exists():
                self.run(["genisoimage", "-quiet", "-output", seed, "-volid", "cidata", "-joliet", "-rock", "user-data", "meta-data", "network-config"], cwd=folder)
            client = self.clients[node["physical_host"]]
            client.transfer(seed, node["name"] + "-seed.iso")
            client.call("create", node=node["name"])

    def start_vms(self):
        for node in self.cfg["nodes"]:
            self.vm_action(node, "start")

    def vm_state(self, node):
        return next(v["state"] for v in self.clients[node["physical_host"]].call("status") if v["node"] == node["name"])

    def vm_action(self, node, action):
        require(action in ("start", "stop", "pause", "resume"), "invalid provider action")
        return self.clients[node["physical_host"]].call(action, node=node["name"])

    def status(self):
        return {name: client.call("status") for name, client in self.clients.items()}

    def stop(self):
        errors = []
        for node in reversed(self.cfg["nodes"]):
            try:
                if self.vm_state(node) != "absent":
                    self.vm_action(node, "stop")
            except (ValueError, OSError, RuntimeError, subprocess.SubprocessError):
                errors.append(node["name"])
        name = self.cfg["cluster_id"] + "-api"
        reply = self.docker("inspect", name, check=False)
        if reply.returncode == 0:
            require(json.loads(reply.stdout)[0]["Config"].get("Labels", {}).get(LABEL) == self.cfg["cluster_id"], "unowned API proxy")
            self.docker("stop", name)
        require(not errors, "some VMs could not be shut down: " + ",".join(errors))

    def resize(self, previous):
        raise ValueError("v2 resize/move/replacement is not implemented; preserve existing disks and create a distinct fresh formation")

    def verify(self):
        super().verify()
        actual = json.loads(self.kubectl("get", "nodes", "-o", "json").stdout)["items"]
        for n in self.cfg["nodes"]:
            observed = next(v for v in actual if v["metadata"]["name"] == n["name"])
            require(observed["metadata"]["labels"].get("platform.aat/physical-host") == n["physical_host"], "physical-host label differs")
            require(any(v["type"] == "InternalIP" and v["address"] == n["address"] for v in observed["status"]["addresses"]), "node advertised the wrong LAN address")
        self.preflight_resources()
        (self.state / "evidence" / "physical-placement.json").write_text(json.dumps({"hosts": [{"id": h["id"], "provider": h["provider"], "nodes": [n["name"] for n in self.cfg["nodes"] if n["physical_host"] == h["id"]]} for h in self.cfg["hosts"]], "physical_demo_verification": "requires both host displays; machine IDs alone are not physical proof"}, indent=2))
