#!/usr/bin/env python3
"""Isolated PostgreSQL/HTTP freeze proof. No VMs, live sources or host ports."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
REQUIRED = {
    "TestDispatchFreezeAfterPermitReusesReceiptAcrossTakeover",
    "TestDispatchFreezeBeforePermitRejectsStaleSender",
    "TestDispatchLostResponseRemainsBlockedAfterRestart",
    "TestDispatchStreamingHandoffWaitsForBodyClose",
    "TestDispatchEnforcesRequestIdentityLimitsAndNoRedirect",
    "TestDispatchFailedWorkKeepsReceiptGenerationAndSnapshotBoundary",
    "TestDispatchLostReceiptAckReusesSavedResponse",
    "TestDispatchUnconfirmedIntentNeverReachesNetwork",
    "TestDispatchTLSUsesHTTP1WithoutHiddenHTTP2Retries",
    "TestSharedStationsOnlySourceOwnerFetches",
    "TestCoastalEmptyPageAdvancesDispatchGeneration",
}


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    os.umask(0o077)
    run = uuid.uuid4().hex[:12]
    prefix = "aat-dispatch-check-" + run
    output = ROOT / "evidence/platform-acquisition-dispatch" / run
    output.mkdir(parents=True)
    report = {"run_id": run, "started_at": datetime.now(timezone.utc).isoformat(), "passed": False,
              "scope": "isolated PostgreSQL plus real HTTP pause/takeover protocol proof; not four-node HA or takeover-SLA acceptance"}
    password = secrets.token_hex(18)
    env = {**os.environ, "POSTGRES_PASSWORD": password,
           "M2_DATABASE_URL": "postgres://postgres:" + password + "@127.0.0.1:5432/postgres?sslmode=disable"}
    created = []

    def docker(*args, check=True):
        return subprocess.run(["docker", *map(str, args)], env=env, capture_output=True, text=True, check=check, timeout=240)

    try:
        pg = prefix + "-pg"
        docker("run", "-d", "--name", pg, "--label", "aat.dispatch.check=" + run, "--network", "none",
               "--memory", "512m", "--cpus", "1", "-e", "POSTGRES_PASSWORD", "postgres:16-alpine3.24",
               "-c", "shared_buffers=32MB", "-c", "max_prepared_transactions=128")
        created.append(pg)
        deadline = time.monotonic() + 45
        while docker("exec", pg, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", check=False).returncode:
            if time.monotonic() > deadline:
                raise RuntimeError("temporary database startup deadline")
            time.sleep(.2)
        runner = prefix + "-go"
        pattern = "TestDispatch|TestPlatformAcquisition|TestSourceProgress|TestSourceReceipt|TestM3IngestionBytes|TestAcquisitionPublishes|TestSharedStations|TestCoastalEmptyPage"
        docker("create", "--name", runner, "--label", "aat.dispatch.check=" + run, "--network", "container:" + pg,
               "--memory", "2g", "--cpus", "3", "--mount", "type=bind,src=" + str(ROOT) + ",dst=/src,readonly",
               "-v", "r3-go-mod:/go/pkg/mod", "-v", "r3-go-build:/root/.cache/go-build", "-e", "M2_DATABASE_URL",
               "-w", "/src/aggregator", "golang:1.27.1-alpine3.24", "go", "test", "-mod=readonly", "-count=1",
               "-timeout=180s", "-json", "./internal/store", "./internal/ingestion", "./internal/coastal", "./internal/acquisition", "-run", pattern)
        created.append(runner)
        process = docker("start", "-a", runner, check=False)
        (output / "tests.log").write_text((process.stdout + process.stderr).replace(password, "[redacted]"))
        counts = {"pass": 0, "fail": 0, "skip": 0}
        passed = set()
        for line in process.stdout.splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("Test") and event.get("Action") in counts:
                counts[event["Action"]] += 1
                if event["Action"] == "pass":
                    passed.add(event["Test"])
        missing = sorted(REQUIRED - passed)
        report.update(passed=process.returncode == 0 and not missing and counts["skip"] == 0,
                      returncode=process.returncode, counts=counts, missing_required=missing)
    finally:
        cleanup = []
        for name in reversed(created):
            identity = docker("inspect", "--format", '{{ index .Config.Labels "aat.dispatch.check" }}', name, check=False).stdout.strip()
            if identity == run:
                if docker("rm", "-f", "-v", name, check=False).returncode:
                    cleanup.append(name)
            else:
                cleanup.append(name)
        report["cleanup_required"] = cleanup
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        if cleanup:
            report["passed"] = False
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"evidence": str(output), "passed": report["passed"], "counts": report.get("counts"), "cleanup_required": cleanup}))
    return int(not report["passed"])


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print("Isolated dispatch checks failed: " + type(exc).__name__, file=sys.stderr)
        sys.exit(1)
