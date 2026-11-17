#!/usr/bin/env python3
"""Plan, record, or reconcile physical Ethernet-disconnect evidence.

The operator removes/restores the cable; this tool never changes NICs, firewalls,
VM power, application state, leases or replica placement during observation.
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


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("plan", "run", "reconcile", "fetch"))
    p.add_argument("--inventory", type=Path, default=ROOT / "deployment/platform/inventory.multihost.example.json")
    p.add_argument("--output", type=Path)
    p.add_argument("--application-probes", type=Path, help="private per-host probe mapping; never logged")
    p.add_argument("--application-evidence", type=Path, help="existing recovery evidence plus network_run_id and same-host latency markers")
    p.add_argument("--demo-attestation", type=Path, help="reviewer and physical demo artifact references")
    a = p.parse_args()
    cfg = load(a.inventory)
    require(cfg["version"] == 2, "v2 inventory required")
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
