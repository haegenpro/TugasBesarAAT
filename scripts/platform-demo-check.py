#!/usr/bin/env python3
"""Run one live M2 demo check and keep its evidence (exit 0 passed, 1 failed, 2 blocked).

  distribution     P1.1-1.3  data spread per node, ordered cross-location reads, checksums
  takeover         P2.2-2.3  crash the lease holder; another instance resumes <=60s, exactly once
  fencing          P2.4-2.5  after a clock shift (Tester Agent) or a VM freeze: no double owner
  malformed        P3.3      broken messages refused without any crash/restart
  ops-only         P3.5      failed messages readable only by BNPB Internal Ops
  schema           P3.1-3.2  incompatible PVMBG change is parked with reason/source/time
  replay           P3.4      replay parked messages after adapting; no duplicates
  field-reports    P3.6      202-accepted reports survive receiver crashes, processed once
  processor-crash  P4.5      crash the wildfire processor; final result equals the reference
  coastal-breaker  P5.4      unhealthy sensor: no request surge, dependent reads answer fast
  tester           P1.4/P2/P5.6  run a Tester Agent scenario, then observe recovery only

Tokens are read from private files (mode 0600) and never printed.
Evidence: evidence/platform-demo/<check>-<run>/result.json
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

import demo_checks_lib as checks
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import require

ROOT = Path(__file__).resolve().parent.parent


def api(url, token_file, name):
    require(url, "--" + name + "-url is required for this check")
    return checks.Api(url, checks.read_token(token_file))


def observer(args, cluster):
    from source_validation_lib import Observer
    require(args.receipt, "--receipt (maintenance receipt with original database identities) is required for this check")
    owner = json.loads(args.owner_config.read_text()) if args.owner_config else {}
    return Observer(cluster, owner, json.loads(args.receipt.read_text()))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("check", choices=["distribution", "takeover", "fencing", "malformed", "ops-only", "schema", "replay",
                                     "field-reports", "processor-crash", "coastal-breaker", "tester"])
    p.add_argument("--inventory", type=Path, required=True)
    p.add_argument("--output", type=Path)
    p.add_argument("--receipt", type=Path, help="maintenance receipt (database identities for read-only queries)")
    p.add_argument("--owner-config", type=Path, help="role1 configuration (satellite namespace)")
    for name in ("audit", "client", "ingestion", "pvmbg", "prometheus"):
        p.add_argument("--" + name + "-url")
    for name in ("audit", "field", "media", "ops", "pvmbg-admin"):
        p.add_argument("--" + name + "-token-file", type=Path)
    p.add_argument("--sample", type=int, default=0, help="distribution: locations for N evenly spaced events (0 = all)")
    p.add_argument("--scope", default="acquisition:PVMBG", help="ownership scope, e.g. acquisition:BMKG")
    p.add_argument("--app", help="instance app label (default per check)")
    p.add_argument("--upstream", help="upstream mock app label (default from scope)")
    p.add_argument("--since", help="fencing: analyze from this UTC time (e.g. Tester Agent scenario start)")
    p.add_argument("--freeze-node", help="fencing: freeze this node's VM (operator) before analyzing")
    p.add_argument("--freeze-seconds", type=int, default=90)
    p.add_argument("--mode", help="schema: rename|type_change|missing_required; tester: practice|exam|list")
    p.add_argument("--namespace-id", help="replay: source history namespace")
    p.add_argument("--adapter", default="pvmbg-adapter-v2")
    p.add_argument("--count", type=int, default=20)
    p.add_argument("--reference-file", type=Path, help="processor-crash: JSON {count, checksum} from the assistant or an undisturbed run")
    p.add_argument("--path", action="append", default=[], help="coastal-breaker: client path to time (repeatable)")
    p.add_argument("--rate-query", help="coastal-breaker: PromQL for the request rate to Sensor Pantai")
    p.add_argument("--seconds", type=int, default=120)
    p.add_argument("--scenario", help="tester: scenario name")
    p.add_argument("--recovery-seconds", type=int, default=300)
    p.add_argument("tester_args", nargs="*", help="tester: extra arguments passed to /app run (e.g. the exam key)")
    a = p.parse_args(argv)
    inventory = load(a.inventory)
    require(inventory.get("version") == 3, "existing-cluster (v3) inventory required")
    cluster = Cluster(inventory)
    evidence = checks.Evidence(a.check, a.output)
    blocked = []
    try:
        if a.check == "distribution":
            client = api(a.client_url, a.field_token_file, "client") if a.client_url else None
            checks.distribution(cluster, api(a.audit_url, a.audit_token_file, "audit"), inventory, client=client, sample=a.sample, evidence=evidence)
        elif a.check == "takeover":
            upstream = a.upstream or a.scope.split(":")[-1].lower() + "-mock"
            app = a.app or ("ingestion" if a.scope.endswith("BMKG") else "aggregator")
            checks.takeover(cluster, observer(a, cluster), api(a.audit_url, a.audit_token_file, "audit"), inventory, scope=a.scope, app=app, upstream=upstream, evidence=evidence)
        elif a.check == "fencing":
            since = a.since or checks.now()
            if a.freeze_node:
                node = cluster.node(a.freeze_node)
                cluster.vm_action(node, "pause");time.sleep(a.freeze_seconds);cluster.vm_action(node, "resume")
                evidence.result["freeze"] = cluster.actions
                time.sleep(30)  # let the resumed instance attempt its stale work before analysis
            upstream = a.upstream or a.scope.split(":")[-1].lower() + "-mock"
            checks.fencing(cluster, observer(a, cluster), api(a.audit_url, a.audit_token_file, "audit"), inventory, scope=a.scope, upstream=upstream, since=since, evidence=evidence)
        elif a.check == "malformed":
            checks.malformed(cluster, api(a.client_url, a.field_token_file, "client"), inventory, evidence=evidence)
        elif a.check == "ops-only":
            require(a.audit_url and a.ingestion_url, "--audit-url and --ingestion-url are required")
            tokens = {role: checks.read_token(getattr(a, role + "_token_file")) for role in ("audit", "ops", "field", "media")}
            if not tokens["ops"] or not (tokens["field"] or tokens["media"]):
                blocked.append("supply --ops-token-file and at least one of --field/--media-token-file")
            else:
                checks.ops_only(a.audit_url, a.ingestion_url, tokens, evidence=evidence)
        elif a.check == "schema":
            checks.schema_break(api(a.pvmbg_url, a.pvmbg_admin_token_file, "pvmbg"), api(a.audit_url, a.audit_token_file, "audit"), mode=a.mode, evidence=evidence)
        elif a.check == "replay":
            require(a.namespace_id, "--namespace-id is required")
            checks.replay(api(a.ingestion_url, a.ops_token_file, "ingestion"), api(a.audit_url, a.audit_token_file, "audit"), namespace=a.namespace_id, adapter=a.adapter, evidence=evidence)
        elif a.check == "field-reports":
            require(2 <= a.count <= 500, "--count must be 2-500")
            checks.field_reports(cluster, api(a.client_url, a.field_token_file, "client"), inventory, count=a.count, evidence=evidence)
        elif a.check == "processor-crash":
            reference = json.loads(a.reference_file.read_text()) if a.reference_file else None
            if reference is None:
                blocked.append("supply --reference-file with the reference {count, checksum}")
            checks.processor_crash(cluster, observer(a, cluster), api(a.audit_url, a.audit_token_file, "audit"), inventory,
                                   app=a.app or "wildfire-processor", reference=reference, evidence=evidence)
        elif a.check == "coastal-breaker":
            require(a.path, "--path is required (for example /hazards?source=SENSOR_PANTAI)")
            prometheus = checks.Api(a.prometheus_url) if a.prometheus_url else None
            if not a.rate_query:
                blocked.append("supply --prometheus-url and --rate-query to prove the request rate did not surge")
            checks.coastal_breaker(prometheus, api(a.client_url, a.field_token_file, "client"), paths=a.path, rate_query=a.rate_query, seconds=a.seconds, evidence=evidence)
        else:
            require(a.mode in ("practice", "exam", "list"), "--mode practice|exam|list is required")
            checks.tester(cluster, api(a.audit_url, a.audit_token_file, "audit") if a.mode != "list" else None, inventory, scenario=a.scenario,
                          mode=a.mode, extra=a.tester_args, recovery_seconds=a.recovery_seconds, evidence=evidence)
    except BaseException as exc:
        evidence.result["error"] = type(exc).__name__ + ": " + str(exc)
        evidence.finish(["check stopped before completion"])
        raise
    result = evidence.finish(blocked)
    print(json.dumps({"check": a.check, "status": result["status"], "assertions": result["assertions"], "evidence": str(evidence.dir / "result.json")}, indent=2))
    return {"passed": 0, "failed": 1, "blocked": 2}[result["status"]]


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print("Demo check stopped: " + str(exc), file=sys.stderr)
        sys.exit(1)
