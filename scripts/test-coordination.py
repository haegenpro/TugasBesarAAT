#!/usr/bin/env python3
"""Run ownership integration tests against one disposable, run-owned PostgreSQL.

This proves the local fencing protocol, not replicated durability or VM failover.
Never reads application .env files or connects to application databases.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
LABEL = "aat.platform.ownership.run"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--image", default="postgres:16-alpine3.24")
    args = parser.parse_args()
    run_id = uuid.uuid4().hex
    name = "aat-ownership-" + run_id
    evidence = ROOT / "evidence" / "platform-ownership" / run_id
    evidence.mkdir(parents=True, mode=0o700)
    password = secrets.token_hex(24)
    env = {**os.environ, "POSTGRES_PASSWORD": password}
    started = time.monotonic()
    exit_code = 1
    result = {"run_id": run_id, "scope": "single-copy PostgreSQL fencing", "race": args.race, "passed": False}
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True)
    if revision.returncode == 0:
        result["source_revision"] = revision.stdout.strip()
        status = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, text=True, capture_output=True)
        result["source_dirty"] = status.returncode != 0 or bool(status.stdout.strip())

    def docker(*arguments, check=True):
        return subprocess.run(["docker", *arguments], env=env, text=True, capture_output=True, check=check, timeout=120)

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        docker("version", "--format", "{{.Server.Version}}")
        docker("run", "--detach", "--name", name, "--label", f"{LABEL}={run_id}",
               "--memory", "1g", "--memory-reservation", "512m", "--cpus", "2",
               "--tmpfs", "/var/lib/postgresql/data:rw,nosuid,size=268435456",
               "--env", "POSTGRES_PASSWORD", "--env", "POSTGRES_DB=postgres",
               "--publish", "127.0.0.1::5432", args.image,
               "-c", "shared_buffers=32MB", "-c", "max_connections=40")
        deadline = time.monotonic() + 45
        while docker("exec", name, "pg_isready", "-U", "postgres", check=False).returncode:
            if time.monotonic() >= deadline:
                raise RuntimeError("isolated PostgreSQL did not become ready")
            time.sleep(0.2)
        # initdb's temporary server does not listen on TCP; wait for final startup.
        while docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", check=False).returncode:
            if time.monotonic() >= deadline:
                raise RuntimeError("isolated PostgreSQL TCP listener did not become ready")
            time.sleep(0.2)
        address = docker("port", name, "5432/tcp").stdout.strip()
        host, port = address.rsplit(":", 1)
        if host != "127.0.0.1" or not port.isdecimal():
            raise RuntimeError("test database must bind only to loopback")
        result["image_id"] = docker("inspect", "--format", "{{.Image}}", name).stdout.strip()
        result["postgres_version"] = docker("exec", name, "postgres", "--version").stdout.strip()
        test_env = {key: value for key, value in os.environ.items()
                    if not key.startswith("M2_") and key != "TEST_POSTGRES_URL"}
        test_env.update({"M2_DATABASE_URL": f"postgres://postgres:{password}@127.0.0.1:{port}/postgres?sslmode=disable",
                         "DB_MAX_OPEN_CONNS": "6", "CANONICAL_OWNERSHIP_ENABLED": "false"})
        command = ["go", "test", "-mod=readonly", "-count=1", "-timeout=90s", "-v"]
        if args.race:
            command.append("-race")
        base_command = list(command)
        command.extend(["./internal/store", "./internal/canonical", "./internal/config", "-run",
                        "^(TestPlatformOwnership|TestOwnership)"])
        print(f"Isolated ownership tests; evidence: {evidence.relative_to(ROOT)}", flush=True)
        tests = subprocess.run(command, cwd=ROOT / "aggregator", env=test_env,
                               text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=240)
        output = tests.stdout.replace(password, "[redacted]")
        print(output, end="", flush=True)
        exit_code = tests.returncode
        if exit_code == 0:
            migrations = subprocess.run(base_command + ["./internal/store", "-run",
                                        "^TestM2Migrations$/^(postgres_upgrade_|runtime_grants|reject_partial)"],
                                        cwd=ROOT / "aggregator", env=test_env, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=240)
            migration_output = migrations.stdout.replace(password, "[redacted]")
            output += migration_output
            print(migration_output, end="", flush=True)
            exit_code = migrations.returncode
        (evidence / "tests.log").write_text(output)
        result["passed"] = exit_code == 0
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"Coordination tests failed: {str(exc).replace(password, '[redacted]')}", file=sys.stderr)
    except KeyboardInterrupt:
        print("Coordination tests interrupted; removing this run's container.", file=sys.stderr)
        exit_code = 130
    finally:
        signal.signal(signal.SIGTERM, previous)
        # Inspect ownership before removing anything, including after failed run.
        try:
            owned = docker("inspect", "--format", '{{ index .Config.Labels "' + LABEL + '" }}', name, check=False)
            if owned.returncode == 0 and owned.stdout.strip() == run_id:
                state = json.loads(docker("inspect", "--format", "{{json .State}}", name).stdout)
                result["oom_killed"] = state.get("OOMKilled", False)
                if result["oom_killed"]:
                    result["passed"] = False
                    exit_code = 1
                docker("rm", "--force", "--volumes", name)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            result["cleanup_error"] = str(exc).replace(password, "[redacted]")
            result["passed"] = False
            exit_code = 1
            print(f"Cleanup failed for run-owned container {name}; see evidence.", file=sys.stderr)
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
