#!/usr/bin/env python3
"""Plan, record, or reconcile physical network-disconnect evidence.

v2 (wired provider hosts): the operator removes/restores the Ethernet cable; this
tool never changes NICs, firewalls, VM power, application state, leases or
replica placement during observation.

v3 (existing Wi-Fi laptops): the controller observes the cluster while
platform-host-probe.py runs on every laptop; on one laptop that probe really
disables and then re-enables its own Wi-Fi adapter.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

from platform_lib.cluster import Cluster
from platform_lib.inventory import load, fingerprint, require
from platform_lib.network_evidence import MAX_LOG, reconcile, validate_probes, download_collector

ROOT = Path(__file__).resolve().parent.parent


def run(cluster, output, probes=None, durations=(60, 30, 180), sleep=time.sleep):
    require(cluster.cfg["version"] == 2, "physical-host recorder requires v2")
    require(len(durations) == 3 and all(type(v) is int and 1 <= v <= 300 for v in durations) and sum(durations) <= 540, "invalid phase durations")
    cluster.verify()
    probe = json.loads((cluster.state / "evidence" / "network-probe.json").read_text())
    require(probe["status"] == "passed" and probe["inventory_sha256"] == fingerprint(cluster.cfg), "complete cross-host pod/service probe required first")
    require(not output.exists(), "use a fresh evidence directory")
    output.mkdir(parents=True, mode=0o700)
    run_id = uuid.uuid4().hex
    start = {"version": 1, "run_id": run_id, "inventory_sha256": fingerprint(cluster.cfg), "status": "blocked", "durations": list(durations),
             "scope": "physical network evidence; application and demo reconciliation required", "hosts": {}, "events": []}
    (output / "run.json").write_text(json.dumps(start, indent=2))
    started_clients = []
    try:
        for host, client in cluster.clients.items():
            selected = (probes or {}).get(host, [])
            validate_probes(selected, cluster.cfg)
            result = client.call("collector-start", run_id=run_id, seconds=max(60, sum(durations) + 30), probes=selected)
            start["hosts"][host] = result
            started_clients.append(host)
            (output / "run.json").write_text(json.dumps(start, indent=2))
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, timeout=5)
        start["revision"] = revision.stdout.strip()
        start["worktree_dirty"] = bool(subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, timeout=5).stdout.strip())
        fault_host=next(h["id"]for h in cluster.cfg["hosts"]if h["id"]!=cluster.cfg["controller_host"])
        fault_phase="disconnect_"+fault_host+"_ethernet"
        start["physical_fault_host"]=fault_host
        for phase, seconds in zip(("baseline", fault_phase, "reconnect_and_observe"), durations):
            message = {"baseline": "Leave all Ethernet links connected.", fault_phase: "Remove Ethernet from host "+fault_host+" now; do not repair/restart applications.",
                       "reconnect_and_observe": "Reconnect that same cable now; observe automatic recovery without application intervention."}[phase]
            print(message, flush=True)
            start["events"].append({"phase": phase, "controller_monotonic_ns": time.monotonic_ns(), "controller_wall_ns": time.time_ns()})
            (output / "run.json").write_text(json.dumps(start, indent=2))
            sleep(seconds)
        # All collectors have their own deadline; wait for the fixed terminal marker.
        deadline = time.monotonic() + 90
        for host, client in cluster.clients.items():
            download_collector(client, run_id, output / (host + ".jsonl"), deadline=deadline, sleep=sleep)
        logs = {h: [json.loads(line) for line in (output / (h + ".jsonl")).read_text().splitlines()] for h in cluster.clients}
        start["network_reconciliation"] = reconcile(cluster.cfg, run_id, logs)
        require(start["network_reconciliation"]["status"] != "failed", "network assertions failed; inspect retained logs")
        cluster.verify()
    except BaseException as exc:
        start["status"] = "failed"
        start["error"] = type(exc).__name__
        raise
    finally:
        start["started_collectors"] = started_clients
        start["note"] = "Collectors self-terminate; a cable or host still disconnected requires operator restoration. Existing identity/evidence is retained."
        (output / "run.json").write_text(json.dumps(start, indent=2))
    return start


def run_existing(cluster, output, seconds, sleep=time.sleep):
    """v3 (Wi-Fi laptops): controller observation while host probes record the real cut."""
    from platform_lib.wifi_evidence import observe
    require(60 <= seconds <= 900, "observation must last 60-900 seconds")
    require(not output.exists(), "use a fresh evidence directory")
    cluster.verify()
    output.mkdir(parents=True, mode=0o700)
    run_id = uuid.uuid4().hex
    record = {"version": 1, "run_id": run_id, "inventory_sha256": fingerprint(cluster.cfg), "seconds": seconds, "status": "blocked",
              "scope": "physical Wi-Fi cut evidence; application and demo reconciliation required"}
    (output / "run.json").write_text(json.dumps(record, indent=2))
    print("Start on EVERY laptop now, with this run id (only ONE laptop adds --disconnect-after/--disconnect-seconds):\n"
          f"  python scripts/platform-host-probe.py --inventory <existing.json> --run-id {run_id} --seconds {seconds} --interface <Wi-Fi adapter>\n"
          f"Then copy each <run>-<host>.jsonl into {output}/hosts/ named <physical_host>.jsonl, and run reconcile.", flush=True)
    cluster.tolerate_unready.update(n["name"] for n in cluster.cfg["nodes"])  # one node will drop out on purpose
    with (output / "controller.jsonl").open("x") as stream:
        observe(cluster, run_id, seconds, stream, sleep=sleep)
    cluster.tolerate_unready.clear()
    return record


def reconcile_existing(cfg, output, application_evidence=None, demo_attestation=None):
    from platform_lib.wifi_evidence import MAX_LOG, reconcile as reconcile_wifi
    record = json.loads((output / "run.json").read_text())
    require(record["inventory_sha256"] == fingerprint(cfg), "recording inventory differs")
    logs = {}
    for node in cfg["nodes"]:
        file = output / "hosts" / (node["physical_host"] + ".jsonl")
        require(file.is_file() and file.stat().st_size <= MAX_LOG, "missing or oversized host log: " + node["physical_host"])
        logs[node["physical_host"]] = [json.loads(line) for line in file.read_text().splitlines() if line]
    controller = [json.loads(line) for line in (output / "controller.jsonl").read_text().splitlines() if line]
    return reconcile_wifi(cfg, record["run_id"], logs, controller, application_evidence, demo_attestation)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("plan", "run", "reconcile", "fetch"))
    p.add_argument("--inventory", type=Path, default=ROOT / "deployment/platform/inventory.multihost.example.json")
    p.add_argument("--output", type=Path)
    p.add_argument("--seconds", type=int, default=240, help="v3 observation length; must cover the host probes")
    p.add_argument("--application-probes", type=Path, help="private per-host probe mapping; never logged")
    p.add_argument("--application-evidence", type=Path, help="existing recovery evidence plus network_run_id and same-host latency markers")
    p.add_argument("--demo-attestation", type=Path, help="reviewer and physical demo artifact references")
    a = p.parse_args()
    cfg = load(a.inventory)
    if cfg["version"] == 3:
        if a.command == "plan":
            print(json.dumps({"version": 1, "inventory_sha256": fingerprint(cfg), "provider": "existing (Wi-Fi laptops)",
                "steps": ["platform-network.py run --output DIR (prints the run id)", "platform-host-probe.py on all four laptops; one with --disconnect-after",
                          "copy host logs to DIR/hosts/<physical_host>.jsonl", "platform-network.py reconcile --output DIR --application-evidence ... --demo-attestation ..."],
                "status": "blocked", "note": "No fault, network mutation or provisioning executed."}, indent=2))
            return 0
        require(a.output is not None, "run/reconcile requires --output")
        if a.command == "run":
            result = run_existing(Cluster(cfg), a.output, a.seconds)
            print(json.dumps({"run_id": result["run_id"], "status": "blocked", "next": "collect host logs, then reconcile"}))
            return 2
        require(a.command == "reconcile", "fetch is not available for the existing cluster; copy host logs manually")
        result = reconcile_existing(cfg, a.output, json.loads(a.application_evidence.read_text()) if a.application_evidence else None,
                                    json.loads(a.demo_attestation.read_text()) if a.demo_attestation else None)
        (a.output / "reconciliation.json").write_text(json.dumps(result, indent=2))
        print(json.dumps({"status": result["status"], "report": str(a.output / "reconciliation.json")}))
        return {"passed": 0, "failed": 1, "blocked": 2}[result["status"]]
    require(cfg["version"] == 2, "v2 or v3 inventory required")
    if a.command == "plan":
        print(json.dumps({"version": 1, "inventory_sha256": fingerprint(cfg), "phases_seconds": {"baseline": 60, "physical_cut": 30, "recovery": 180},
            "requirements": ["both host screens/demo artifacts", "passed pod/service network probe", "host-local collectors", "real cable removal", "owner acknowledgement/checksum/timing evidence"],
            "status": "blocked", "note": "No fault, network mutation or provisioning executed."}, indent=2))
        return 0
    require(a.output is not None, "run/fetch/reconcile requires --output")
    if a.command == "run":
        probes = json.loads(a.application_probes.read_text()) if a.application_probes else None
        with Cluster(cfg).locked():
            result = run(Cluster(cfg), a.output, probes)
        print(json.dumps({"run_id": result["run_id"], "status": "blocked", "next": "reconcile with actual application and physical demo evidence"}))
        return 2  # Never turn a network-only recording into a bonus pass.
    record = json.loads((a.output / "run.json").read_text())
    require(record["inventory_sha256"] == fingerprint(cfg), "recording inventory differs")
    if a.command == "fetch":
        cluster = Cluster(cfg)
        for host, client in cluster.clients.items():
            download_collector(client, record["run_id"], a.output / (host + ".jsonl"))
        return 0
    logs = {}
    for h in cfg["hosts"]:
        file = a.output / (h["id"] + ".jsonl")
        require(file.stat().st_size <= MAX_LOG, "log quota exceeded")
        logs[h["id"]] = [json.loads(line) for line in file.read_text().splitlines()]
    result = reconcile(cfg, record["run_id"], logs, json.loads(a.application_evidence.read_text()) if a.application_evidence else None,
                       json.loads(a.demo_attestation.read_text()) if a.demo_attestation else None)
    (a.output / "reconciliation.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({"status": result["status"], "report": str(a.output / "reconciliation.json")}))
    return {"passed": 0, "failed": 1, "blocked": 2}[result["status"]]


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, RuntimeError, subprocess.SubprocessError):
        print("Network evidence failed/blocked; inspect the retained run and local host prerequisites.", file=sys.stderr)
        raise SystemExit(1)
