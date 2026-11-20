"""Administer an existing four-node cluster; never manage VM lifecycle or SSH directly.

VMs run on separate physical laptops that this controller cannot reach. VM
actions (pause/resume/stop/start) and guest steps are therefore performed by an
operator on the owning host, using scripts/platform-vm-control.py or an exported
guest script. The controller prints the exact instruction and then *observes*
the outcome itself (kubelet reachability and the Kubernetes API) before it
continues, so an action that was not performed can never be recorded as done.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import sys
import time

from .inventory import NAMESPACE, fingerprint, require

KUBELET_PORT = 10250
VM_ACTIONS = {"pause": False, "stop": False, "resume": True, "start": True}  # action -> reachable afterwards


def kubelet_reachable(address, timeout=2.0):
    """A paused or powered-off VM stops accepting TCP connections immediately."""
    try:
        socket.create_connection((address, KUBELET_PORT), timeout).close()
        return True
    except OSError:
        return False


def operator_timeout(default=300):
    value = int(os.environ.get("AAT_OPERATOR_TIMEOUT", str(default)))
    require(30 <= value <= 7200, "AAT_OPERATOR_TIMEOUT must be 30-7200 seconds")
    return value


def validate(cfg):
    require(cfg.get("version") == 3, "existing-cluster inventory version must be 3")
    cluster = cfg.get("cluster_id", "")
    require(re.fullmatch(r"aat-[a-z0-9](?:[a-z0-9-]{0,22}[a-z0-9])?", cluster), "invalid cluster ID")
    require(cfg.get("namespace") == NAMESPACE, "assignment namespace must be preserved")
    require(cfg.get("storage_layout") == "root-filesystem", "declare the existing single-disk layout")
    for field in ("state_dir", "kubeconfig"):
        path = Path(cfg.get(field, ""))
        require(path.is_absolute() and ".." not in path.parts, "absolute private path required: " + field)
    require(Path(cfg["state_dir"]).name == cluster, "state directory must end in the cluster ID")
    command = cfg.get("kubectl_command")
    require(isinstance(command, list) and command and all(isinstance(a, str) and a and "\x00" not in a for a in command), "explicit kubectl command required")
    require(len(command) == 1 or len(command) == 2 and command[1] == "kubectl", "use kubectl or k3s kubectl")
    nodes = cfg.get("nodes", [])
    require(len(nodes) == 4 and [n.get("role") for n in nodes] == ["server", "server", "server", "agent"], "ordered server A/B/C and agent D required")
    for key in ("name", "address", "physical_host", "uid"):
        require(len({n.get(key) for n in nodes}) == 4, "four distinct node " + key + " values required")
    for n in nodes:
        require(re.fullmatch(re.escape(cluster) + r"-[a-z0-9-]+", n.get("name", "")), "invalid existing node name")
        require(isinstance(n.get("uid"), str) and n["uid"], "capture original node UID")
        require(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}", n.get("physical_host", "")), "physical host label required")
        require(ipaddress.IPv4Address(n["address"]).is_private, "private node IPv4 address required")
        require(type(n.get("memory_mib")) is int and n["memory_mib"] >= 1024, "capture allocatable node memory")
        require(type(n.get("vcpus")) is int and n["vcpus"] > 0, "capture node CPU count")
        if "vm_name" in n:  # optional: VirtualBox VM name, only printed in operator instructions
            require(isinstance(n["vm_name"], str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._()-]{0,79}", n["vm_name"]), "invalid vm_name")
    pods, services = (ipaddress.IPv4Network(cfg[k]) for k in ("pod_cidr", "service_cidr"))
    require(pods.is_private and services.is_private and not pods.overlaps(services), "distinct private pod/service CIDRs required")
    require(all(ipaddress.IPv4Address(n["address"]) not in network for n in nodes for network in (pods, services)), "node addresses overlap pod/service networks")
    return cfg


def node_ready(node):
    return any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"].get("conditions", []))


def from_nodes(document, *, cluster_id, kubeconfig, kubectl_command, state_dir, tolerate_unready=()):
    """Capture actual API identities/allocatable memory without provider fiction.

    tolerate_unready names nodes that are allowed to be NotReady (a node under
    an explicit fault or replacement); their identity must still match.
    """
    from bundle_lib import memory_mib
    items = document["items"]
    selected = []
    for letter in "abcd":
        matches = [n for n in items if n["metadata"]["name"] == cluster_id + "-" + letter]
        require(len(matches) == 1, "missing/duplicate node " + letter)
        n = matches[0]
        labels = n["metadata"].get("labels", {})
        require(labels.get("platform.aat/cluster") == cluster_id and labels.get("platform.aat/physical-host") == "host-" + letter, "node ownership/physical host labels differ")
        require(node_ready(n) or n["metadata"]["name"] in tolerate_unready, "all four nodes must be Ready")
        server = "node-role.kubernetes.io/control-plane" in labels
        require(server == (letter != "d"), "server/agent role differs")
        addresses = [a["address"] for a in n["status"]["addresses"] if a["type"] == "InternalIP" and ":" not in a["address"]]
        require(len(addresses) == 1, "one internal IPv4 address per node required")
        selected.append({"name": n["metadata"]["name"], "uid": n["metadata"]["uid"], "role": "server" if server else "agent",
                         "address": addresses[0], "physical_host": labels["platform.aat/physical-host"],
                         "memory_mib": int(memory_mib(n["status"]["allocatable"]["memory"])), "vcpus": int(n["status"]["capacity"]["cpu"])})
    return validate({"version": 3, "cluster_id": cluster_id, "namespace": NAMESPACE,
                     "state_dir": str(Path(state_dir).resolve()), "kubeconfig": str(Path(kubeconfig).resolve()),
                     "kubectl_command": kubectl_command, "storage_layout": "root-filesystem",
                     "pod_cidr": "10.42.0.0/16", "service_cidr": "10.43.0.0/16", "nodes": selected})


class ExistingCluster:
    def __init__(self, cfg, *, reachable=kubelet_reachable, notify=None, sleep=time.sleep, clock=time.monotonic):
        self.cfg = validate(cfg)
        self.state = Path(cfg["state_dir"])
        self.redactions = []
        # Nodes allowed to be NotReady during an explicit fault/replacement.
        self.tolerate_unready = set()
        self.declared = {}  # node -> state declared by an observed operator action
        self.actions = []   # observed operator actions, for evidence
        self.reachable = reachable
        self.notify = notify or (lambda message: print(message, file=sys.stderr, flush=True))
        self.sleep = sleep
        self.clock = clock

    def run(self, *args, **kwargs):
        from .cluster import LocalCluster
        return LocalCluster.run(self, *args, **kwargs)

    def kubectl(self, *args, **kwargs):
        config = Path(self.cfg["kubeconfig"])
        require(config.is_file() and not config.is_symlink() and config.stat().st_uid == os.geteuid() and config.stat().st_mode & 0o077 == 0,
                "kubeconfig must be a private regular file owned by this user")
        return self.run([*self.cfg["kubectl_command"], "--kubeconfig", str(config), *args], **kwargs)

    def docker(self, *args, **kwargs):
        return self.run(["docker", *args], **kwargs)

    @contextmanager
    def locked(self, allow_resize=False):
        import fcntl
        require(not allow_resize, "existing-cluster inventory never resizes VMs")
        self.verify()
        require(not self.state.is_symlink(), "state directory must not be a symlink")
        self.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        require(self.state.stat().st_uid == os.geteuid() and self.state.stat().st_mode & 0o077 == 0, "private controller state directory required")
        with (self.state / ".lock").open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("another operation is using this cluster state") from exc
            marker = self.state / "owner.json"
            expected = {"provider": "existing", "inventory_sha256": fingerprint(self.cfg)}
            if marker.exists():
                require(not marker.is_symlink() and json.loads(marker.read_text()) == expected, "existing-cluster inventory changed; reconcile explicitly before continuing")
            else:
                require({p.name for p in self.state.iterdir()} <= {".lock"}, "refusing unmarked nonempty controller state")
                marker.write_text(json.dumps(expected) + "\n")
                marker.chmod(0o600)
            yield

    def verify(self):
        actual = json.loads(self.kubectl("get", "nodes", "-o", "json").stdout)
        observed = from_nodes(actual, cluster_id=self.cfg["cluster_id"], kubeconfig=self.cfg["kubeconfig"],
                              kubectl_command=self.cfg["kubectl_command"], state_dir=self.cfg["state_dir"],
                              tolerate_unready=self.tolerate_unready)
        for wanted, found in zip(self.cfg["nodes"], observed["nodes"]):
            require(all(wanted[k] == found[k] for k in ("name", "uid", "address", "role", "physical_host")), "live node identity/address differs from captured inventory")
            if found["name"] not in self.tolerate_unready:
                require(found["memory_mib"] >= wanted["memory_mib"] and found["vcpus"] >= wanted["vcpus"], "live node capacity decreased")
        return {"ready": True, "provider": "existing", "nodes": observed["nodes"], "tolerated_unready": sorted(self.tolerate_unready),
                "scope": "API identity/capacity only; guest disk and runtime headroom require separate checks"}

    doctor = verify
    status = verify

    def node(self, name):
        found = [n for n in self.cfg["nodes"] if n["name"] == name]
        require(len(found) == 1, "unknown inventory node: " + str(name))
        return found[0]

    def vm_state(self, node):
        """running when the kubelet answers; otherwise the state an observed operator action declared."""
        if self.reachable(node["address"]):
            self.declared.pop(node["name"], None)
            return "running"
        return self.declared.get(node["name"], "unreachable")

    def wait_reachable(self, node, wanted, seconds, purpose):
        deadline = self.clock() + seconds
        while self.reachable(node["address"]) != wanted:
            require(self.clock() < deadline, purpose + " was not observed on " + node["name"] + " within " + str(seconds) + "s")
            self.sleep(1)

    def vm_action(self, node, action):
        """Ask the operator on the owning physical host to act on the VM, then observe the result."""
        require(action in VM_ACTIONS, "unsupported VM action: " + str(action))
        node = self.node(node["name"])
        vm = node.get("vm_name") or "<VirtualBox VM name of " + node["name"] + ">"
        timeout = operator_timeout()
        wanted = VM_ACTIONS[action]
        requested = datetime.now(timezone.utc).isoformat()
        self.notify(f"[operator] On physical host {node['physical_host']} run:  python scripts/platform-vm-control.py {action} --vm \"{vm}\""
                    f"   (waiting up to {timeout}s for {node['name']} to become {'reachable' if wanted else 'unreachable'})")
        started = self.clock()
        self.wait_reachable(node, wanted, timeout, "operator " + action)
        if wanted:
            self.declared.pop(node["name"], None)
        else:
            self.declared[node["name"]] = "paused" if action == "pause" else "shut off"
        self.actions.append({"node": node["name"], "action": action, "requested_at": requested,
                             "observed_at": datetime.now(timezone.utc).isoformat(), "observed_after_seconds": round(self.clock() - started, 3),
                             "evidence": "kubelet TCP " + ("reachable" if wanted else "unreachable")})

    def wait_node_ready(self, name, seconds, uid=None):
        """Wait until the Kubernetes node object is Ready (optionally with an unchanged UID)."""
        deadline = self.clock() + seconds
        while True:
            result = self.kubectl("get", "node", name, "-o", "json", "--request-timeout=5s", check=False)
            if result.returncode == 0:
                value = json.loads(result.stdout)
                if node_ready(value):
                    require(uid is None or value["metadata"]["uid"] == uid, "node " + name + " was re-registered with another UID; recapture the inventory")
                    return value
            require(self.clock() < deadline, "node " + name + " did not become Ready within " + str(seconds) + "s")
            self.sleep(2)

    def guest_step(self, node, step, script, payload=b"", run_id="manual"):
        """Export one reviewed guest script; the operator runs it inside the VM and confirms.

        Confirmation is a `done` file in the exported directory (written by the
        operator after a successful run), so this also works non-interactively.
        """
        require(re.fullmatch(r"[a-z0-9][a-z0-9:._-]{0,80}", step), "invalid guest step name")
        folder = self.state / "guest" / run_id / node["name"] / step.replace(":", "_")
        if not (folder / "done").exists():
            folder.mkdir(parents=True, exist_ok=True, mode=0o700)
            (folder / "step.py").write_text(script)
            (folder / "payload.bin").write_bytes(payload)
            for path in folder.iterdir():
                path.chmod(0o600)
            self.notify(f"[operator] Copy {folder}/step.py and payload.bin into VM {node['name']} (host {node['physical_host']}), "
                        f"run:  sudo python3 step.py < payload.bin   then on this controller:  touch {folder}/done")
            deadline = self.clock() + operator_timeout(3600)
            while not (folder / "done").exists():
                require(self.clock() < deadline, "guest step " + step + " was not confirmed on " + node["name"])
                self.sleep(2)
        return {"step": step, "node": node["name"], "confirmed": True, "exported": str(folder)}

    def ssh(self, *args, **kwargs):
        raise ValueError("use platform-existing.py volumes and run the reviewed scripts inside each VM; SSH automation is not configured")
