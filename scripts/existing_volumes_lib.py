"""Export reviewed root-filesystem volume preparation for manually managed VMs."""
import importlib.util
import json
from pathlib import Path
import re

from bundle_lib import inspect
from service_lib import storage_bytes
from storage_lib import require

ROOT = Path(__file__).resolve().parent.parent
BASE = Path("/var/lib/aat/local-pv")

# This executes only on an operator-selected guest, under interactive sudo.
GUEST_HEADER = '''import io, json, os, pathlib, shutil, subprocess, sys
data = DATA
if os.geteuid() != 0:
    raise RuntimeError("run with sudo inside the intended VM")
addresses = json.loads(subprocess.check_output(["ip", "-j", "-4", "address", "show"], text=True))
if not any(a.get("local") == data["address"] for nic in addresses for a in nic.get("addr_info", [])):
    raise RuntimeError("this VM does not have the captured node address")
base = pathlib.Path("/var/lib/aat/local-pv")
for path in (base.parent, base):
    if os.path.realpath(path) != str(path):
        raise RuntimeError("storage ancestors must not be symlinks")
    path.mkdir(parents=True, exist_ok=True, mode=0o755)
    if path.stat().st_uid != 0 or path.stat().st_mode & 0o022:
        raise RuntimeError("storage ancestors must be root-owned and not group/world writable")
expected = {"cluster_id": data["cluster_id"], "provider": "existing", "node": data["node"], "node_uid": data["node_uid"]}
marker = base.parent / "platform-owner.json"
if marker.exists() or marker.is_symlink():
    if marker.is_symlink() or not marker.is_file() or marker.stat().st_uid != 0 or json.loads(marker.read_text()) != expected:
        raise RuntimeError("guest storage ownership differs; refuse adoption")
elif any(base.iterdir()):
    raise RuntimeError("refusing to adopt unmarked existing local-pv data")
available = shutil.disk_usage(base).free
if available < data["minimum_free_bytes"]:
    raise RuntimeError("insufficient free disk: have %.2f GiB, require %.2f GiB (%s)" % (available / 2**30, data["minimum_free_bytes"] / 2**30, data["disk_capacity_check"]))
if not marker.exists():
    with marker.open("x") as stream:
        stream.write(json.dumps(expected) + "\\n")
    marker.chmod(0o600)
for payload in data["formations"]:
    sys.stdin = io.StringIO(json.dumps(payload))
    exec(PREPARE, {})
print(json.dumps({"prepared": True, "node": data["node"], "volume_count": data["volume_count"], "disk_capacity_check": data["disk_capacity_check"], "free_gib": round(shutil.disk_usage(base).free / 2**30, 2)}))
'''


def export(inventory, bundle, output, *, allow_disk_overcommit=False):
    require(inventory.get("version") == 3, "existing-cluster inventory required")
    output = Path(output)
    require(not output.exists() and not output.is_symlink(), "volume script output already exists")
    report, snapshots = inspect(bundle, inventory)
    objects = [o for raw in snapshots.values() for o in json.loads(raw)["items"]]
    nodes = {n["name"]: n for n in inventory["nodes"]}
    owners = {}
    for obj in objects:
        if obj["kind"] not in ("StatefulSet", "Deployment", "DaemonSet", "Job"):
            continue
        pod = obj["spec"]["template"]["spec"]
        for volume in pod.get("volumes", []):
            if "persistentVolumeClaim" not in volume:
                continue
            claim = volume["persistentVolumeClaim"]["claimName"]
            uid = pod.get("securityContext", {}).get("fsGroup")
            require(type(uid) is int and uid > 0, "local PVC requires an explicit filesystem owner")
            require(claim not in owners or owners[claim] == uid, "PVC has conflicting filesystem owners")
            owners[claim] = uid
    payloads = {name: {} for name in nodes}
    sizes = {name: 0 for name in nodes}
    allowed = {*report["formations"], inventory["cluster_id"] + "-services"}
    volumes = []
    for obj in objects:
        if obj["kind"] != "PersistentVolume":
            continue
        spec = obj["spec"]
        require("local" in spec, "manual volume exporter accepts local PVs only")
        path = Path(spec["local"]["path"])
        require(path.parent.parent == BASE and path.parent.name in allowed and re.fullmatch(re.escape(inventory["cluster_id"]) + r"-[a-z0-9-]+", path.name), "local PV path is outside the owned formation")
        claim = spec["claimRef"]
        require(claim["namespace"] == inventory["namespace"] and claim["name"] in owners, "PV must bind a workload PVC in the assignment namespace")
        terms = spec["nodeAffinity"]["required"]["nodeSelectorTerms"]
        require(len(terms) == 1 and len(terms[0].get("matchExpressions", [])) == 1 and not terms[0].get("matchFields"), "one explicit local-PV node required")
        expression = terms[0]["matchExpressions"][0]
        require(expression["key"] == "kubernetes.io/hostname" and expression["operator"] == "In" and len(expression["values"]) == 1, "local PV must select one hostname")
        name = expression["values"][0]
        require(name in nodes, "PV selects a node outside the inventory")
        amount = storage_bytes(spec["capacity"]["storage"])
        entry = {"name": path.name, "uid": owners[claim["name"]]}
        payloads[name].setdefault(path.parent.name, []).append(entry)
        sizes[name] += amount
        volumes.append({"node": name, "path": str(path), "uid": entry["uid"], "capacity_bytes": amount})
    module_spec = importlib.util.spec_from_file_location("platform_storage_export", ROOT / "scripts/platform-storage.py")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    output.mkdir(parents=True, mode=0o700)
    policy = "free-space-only" if allow_disk_overcommit else "declared-capacities-plus-headroom"
    minimum = {name: (0 if allow_disk_overcommit else size) + 2 * 2**30 for name, size in sizes.items()}
    for name, node in nodes.items():
        data = {"cluster_id": inventory["cluster_id"], "node": name, "node_uid": node["uid"], "address": node["address"],
                "minimum_free_bytes": minimum[name], "disk_capacity_check": policy, "volume_count": sum(len(v) for v in payloads[name].values()),
                "formations": [{"cluster_id": inventory["cluster_id"], "formation": formation, "paths": paths} for formation, paths in payloads[name].items()]}
        script = "PREPARE = " + repr(module.PREPARE_GUEST) + "\n" + GUEST_HEADER.replace("DATA", repr(data), 1)
        target = output / (name + ".py")
        target.write_text(script)
        target.chmod(0o600)
    summary = {"volumes": volumes, "minimum_free_bytes": minimum, "declared_capacity_bytes": sizes, "disk_capacity_check": policy,
               "scope": "Only 2 GiB free space required per guest; declared PV capacities may exceed available disk. Monitor actual usage; local PV sizes do not impose filesystem quotas. No guest was contacted or changed." if allow_disk_overcommit else "Declared PV capacities plus 2 GiB free space per guest; physical VDI host space must also be checked. No guest was contacted or changed."}
    target = output / "plan.json"
    target.write_text(json.dumps(summary, indent=2) + "\n")
    target.chmod(0o600)
    return summary
