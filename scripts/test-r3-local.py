#!/usr/bin/env python3
"""Run local Go checks in Docker with a disposable, private PostgreSQL.

No application env files, databases or running containers are used. Raw test
events and explicit skips are retained under the ignored evidence directory.
"""
import datetime
import argparse
import json
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker-only", action="store_true", help="Run real single-broker integration checks")
    args = parser.parse_args()
    run = uuid.uuid4().hex[:12]
    pg, runner, broker = f"aat-r3-review-pg-{run}", f"aat-r3-review-go-{run}", f"aat-r3-review-nats-{run}"
    output = ROOT / "evidence" / "r3-review" / run
    output.mkdir(parents=True)
    summary = {"run_id": run, "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "scope": "single PostgreSQL; not replicated Kubernetes acceptance", "checks": []}

    def docker(*args, **kwargs):
        return subprocess.run(["docker", *args], check=True, **kwargs)

    def check(name, module, command, database=False):
        env = (["-e", "M2_DATABASE_URL=postgres://postgres:review@127.0.0.1:5432/postgres?sslmode=disable",
                "-e", "TEST_POSTGRES_URL=postgres://postgres:review@127.0.0.1:5432/postgres?sslmode=disable"] if database else [])
        if args.broker_only:
            env += ["-e", "TEST_NATS_URL=nats://127.0.0.1:4222"]
        with (output / f"{name}.log").open("wb") as log:
            result = subprocess.run(["docker", "exec", *env, "-w", f"/src/{module}", runner, *command],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=1200)
        counts = {"pass": 0, "fail": 0, "skip": 0}
        for line in (output / f"{name}.log").read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("Test") and event.get("Action") in counts:
                counts[event["Action"]] += 1
        summary["checks"].append({"name": name, "module": module, "command": command,
                                  "database": database, "returncode": result.returncode, **counts})
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(f"{name}: {'PASS' if result.returncode == 0 else 'FAIL'} {counts}", flush=True)

    print(f"Evidence: {output}", flush=True)
    try:
        docker("run", "-d", "--name", pg, "--label", f"aat.r3.review={run}",
               "-e", "POSTGRES_PASSWORD=review", "postgres:16-alpine3.24", stdout=subprocess.DEVNULL)
        docker("run", "-d", "--name", runner, "--label", f"aat.r3.review={run}",
               "--network", f"container:{pg}", "-v", f"{ROOT}:/src:ro",
               "-v", "r3-go-mod:/go/pkg/mod", "-v", "r3-go-build:/root/.cache/go-build",
               "golang:1.27.1-alpine3.24", "tail", "-f", "/dev/null", stdout=subprocess.DEVNULL)
        docker("exec", runner, "apk", "add", "--no-cache", "gcc", "musl-dev", stdout=subprocess.DEVNULL)
        if args.broker_only:
            docker("run", "-d", "--name", broker, "--label", f"aat.r3.review={run}", "--network", f"container:{pg}",
                   "nats@sha256:b270f5e2428354c0335612694d7dd2fb588148e567a5757fdff325ef9c9332e6",
                   "-js", "-sd", "/data", stdout=subprocess.DEVNULL)
            for module in ("broker", "consumer-dashboard", "consumer-notifier"):
                check(module + "-broker", module, ["go", "test", "-mod=readonly", "-count=1", "-race", "-timeout=90s", "-json", "./..."], database=True)
            check("field-report-broker", "aggregator", ["go", "test", "-mod=readonly", "-count=1", "-race", "-timeout=90s", "-json", "./internal/fieldreport", "./internal/publisher"], database=True)
            return int(any(c["returncode"] for c in summary["checks"]))
        modules = sorted({str(p.parent.relative_to(ROOT)).replace("\\", "/") for p in ROOT.glob("*/go.mod")}
                         | {"monitoring/launcher", "experiments/m1"})
        for module in modules:
            check(module.replace("/", "-") + "-test", module,
                  ["go", "test", "-mod=readonly", "-count=1", "-timeout=300s", "-json", "./..."], database=module != "aggregator")
            check(module.replace("/", "-") + "-vet", module, ["go", "vet", "-mod=readonly", "./..."])
        check("r3-database", "aggregator", ["go", "test", "-mod=readonly", "-count=1", "-timeout=300s", "-json",
              "./internal/fieldreport", "./internal/coastal", "./internal/satellite", "./internal/ingestion",
              "./internal/clients", "./internal/poller"], database=True)
        check("pvmbg-replay-database", "aggregator", ["go", "test", "-mod=readonly", "-count=1", "-timeout=90s", "-json",
              "./internal/store", "-run", "^TestLegacyReplay|^TestLegacyFailure"], database=True)
        check("r3-failure-race", "aggregator", ["go", "test", "-mod=readonly", "-count=1", "-race", "-timeout=120s", "-json",
              "./internal/store", "-run", "^TestLegacyReplay|^TestLegacyFailure"], database=True)
        check("r3-race", "aggregator", ["go", "test", "-mod=readonly", "-count=1", "-race", "-timeout=300s", "-json",
              "./internal/fieldreport", "./internal/coastal", "./internal/satellite", "./internal/ingestion",
              "./internal/clients", "./internal/poller"], database=True)
        for module in ("client-api", "consumer-notifier", "pvmbg-mock"):
            check(module + "-race", module, ["go", "test", "-mod=readonly", "-count=1", "-race", "-timeout=300s", "-json", "./..."], database=True)
        return int(any(c["returncode"] for c in summary["checks"]))
    finally:
        for name in (broker, runner, pg):
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    raise SystemExit(main())
