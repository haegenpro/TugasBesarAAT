#!/usr/bin/env python3
"""Deferred HA checks. Executes only when invoked; never provisions infrastructure.

observe is read-only. single-vm and two-vm write identifiable canonical fixtures,
pause owned running VMs, check acknowledgement/retry behavior, and restore them.
This runner does not establish disk-loss, process-freeze or full load acceptance.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler
import uuid

from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import durability_config, plan, require, write_json

ROOT = Path(__file__).resolve().parent.parent


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


SERVICE_PROBE = r'''
import json, os, psycopg2, sys
with psycopg2.connect(host=sys.argv[1], dbname='hazards', user='aggregator_app',
                     password=os.environ['APP_PASSWORD'], connect_timeout=2) as c:
    with c.cursor() as q:
        q.execute("SET statement_timeout='1500ms'")
        q.execute('SELECT public.platform_replication_status()')
        print(json.dumps(q.fetchone()[0]))
'''


class Checks:
    def __init__(self, args):
        self.args = args
        self.inventory = load(args.inventory)
        self.cluster = Cluster(self.inventory)
        self.storage = json.loads(args.plan.read_text())
        require(self.storage == plan(self.inventory, self.storage["mode"], self.storage["image"]), "plan does not match inventory/renderer")
        self.config = json.loads(args.config.read_text())
        expected = durability_config(self.storage)
        for group, actual in zip(expected["groups"], self.config["groups"]):
            require(re.fullmatch(r"[1-9][0-9]{0,19}", actual["system_id"]), "capture real bootstrap system IDs first")
            group["system_id"] = actual["system_id"]
        require(self.config == expected, "captured runtime config does not match planned formation")
        require(len({g["system_id"] for g in self.config["groups"]}) == len(self.config["groups"]), "reused physical group identity")
        self.token = os.environ.get("CANONICAL_SERVICE_TOKEN", "")
        self.holder = "ha-check-" + uuid.uuid4().hex
        self.opener = build_opener(NoRedirects)
        self.report = {"scenario": args.scenario, "run_id": self.holder, "passed": False,
                       "started_at": datetime.now(timezone.utc).isoformat(), "formation": self.storage["formation"],
                       "api_entry": args.api_url,
                       "config_sha256": hashlib.sha256(json.dumps(self.config, sort_keys=True).encode()).hexdigest(),
                       "scope": "WAL prefix and canonical retry checks; not disk-loss, all freeze cases or load acceptance", "events": []}
        self.output = ROOT / "evidence" / "platform-storage" / self.holder
        self.output.mkdir(parents=True, mode=0o700)
        self.paused = []
        self.excluded = set()
        self.owner = None

    def event(self, action, **fields):
        self.report["events"].append({"action": action, "at": datetime.now(timezone.utc).isoformat(), **fields})
        write_json(self.output / "result.json", self.report)

    def pod(self, member):
        result = json.loads(self.cluster.kubectl("get", "pod", member["name"], "-n", self.storage["namespace"], "-o", "json", "--request-timeout=5s").stdout)
        require(result["metadata"]["labels"].get("platform.aat/formation") == self.storage["formation"], "pod is not owned by this formation")
        require(result["spec"]["nodeName"] == member["node"], "member is on an unexpected VM")
        return result

    def statuses(self):
        executor = next((m for g in self.storage["groups"] for m in g["members"] if m["node"] not in self.excluded), None)
        require(executor, "no surviving probe executor")
        self.pod(executor)
        statuses = []
        for group, cfg in zip(self.storage["groups"], self.config["groups"]):
            leaders = []
            for member in group["members"]:
                if member["node"] in self.excluded:
                    continue
                direct = self.cluster.kubectl("exec", "-n", self.storage["namespace"], member["name"], "-c", "postgres", "--",
                    "psql", "-U", "postgres", "-d", "hazards", "-Atc", "SELECT public.platform_replication_status()", timeout=12)
                status = json.loads(direct.stdout)
                require(status["formation"] == self.storage["formation"] and status["group_id"] == group["id"]
                        and status["system_id"] == cfg["system_id"] and status["member"] == member["name"],
                        "surviving physical member identity mismatch")
                if status["primary"]:
                    leaders.append(member["name"])
            require(len(leaders) == 1, "physical group does not have exactly one surviving primary")
            host = f"{self.storage['formation']}-g{group['id']}-primary.{self.storage['namespace']}.svc.cluster.local"
            result = self.cluster.kubectl("exec", "-n", self.storage["namespace"], executor["name"], "-c", "postgres", "--",
                "/opt/patroni/bin/python", "-c", SERVICE_PROBE, host, timeout=12)
            status = json.loads(result.stdout)
            allowed = {m["name"] for m in group["members"] if m["node"] not in self.excluded}
            require(status["primary"] and status["member"] in allowed and status["formation"] == self.storage["formation"]
                    and status["group_id"] == group["id"] and status["system_id"] == cfg["system_id"], "primary routing/physical identity mismatch")
            require(status["member"] == leaders[0], "primary service did not route to the unique physical primary")
            require(status["fsync"] == "on" and status["full_page_writes"] == "on" and status["synchronous_commit"] == "on", "unsafe durability settings")
            statuses.append(status)
        return statuses

    def confirmed(self, seconds=60):
        deadline = time.monotonic() + seconds
        last = "replication evidence unavailable"
        while time.monotonic() < deadline:
            try:
                before = self.statuses()
                time.sleep(0.1)
                after = self.statuses()
                for old, status, group in zip(before, after, self.storage["groups"]):
                    require(all(status[k] == old[k] for k in ("member", "started", "timeline", "standby_names")), "primary changed during WAL observation")
                    target = parse_lsn(old["insert_lsn"])
                    require(target > 0 and parse_lsn(status["flush_lsn"]) >= target, "primary flush has not reached captured insert prefix")
                    match = re.fullmatch(r"ANY\s+([12])\s*\((.+)\)", status["standby_names"])
                    require(match, "explicit synchronous quorum is missing")
                    candidates = [s.strip().strip('"') for s in match[2].split(",")]
                    allowed = {m["name"] for m in group["members"] if m["node"] not in self.excluded} - {status["member"]}
                    # Patroni may temporarily retain a failed candidate in ANY 1.
                    all_names = {m["name"] for m in group["members"]} - {status["member"]}
                    require(len(candidates) == len(set(candidates)) and set(candidates) <= all_names, "unknown/reused synchronous member")
                    peers = {p["member"] for p in status["replicas"] if p["member"] in allowed & set(candidates)
                             and p["state"] == "streaming" and p["sync_state"] in ("quorum", "sync")
                             and p["flush_lsn"] and parse_lsn(p["flush_lsn"]) >= target}
                    require(len(peers) >= int(match[1]), "synchronous copies have not flushed the observed WAL prefix")
                self.event("replicated-prefix-confirmed", groups=after)
                return after
            except (ValueError, KeyError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
                last = str(exc)
                time.sleep(1)
        raise RuntimeError("replication did not settle within deadline: " + last)

    def request(self, method, path, payload=None):
        raw = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        req = Request(self.args.api_url.rstrip("/") + path, data=raw, method=method,
                      headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with self.opener.open(req, timeout=4) as response:
                status, body = response.status, response.read(1 << 20)
        except HTTPError as exc:
            status, body = exc.code, exc.read(1 << 20)
        except (URLError, TimeoutError, OSError):
            self.event("http-unknown", method=method, path=path)
            return None, None
        try:
            result = json.loads(body)
        except (ValueError, UnicodeError):
            raise RuntimeError("API returned a non-JSON response")
        self.event("http", method=method, path=path, status=status, response=result)
        return status, result

    def batch(self, suffix):
        ref = self.holder + "-" + suffix
        batch = {"contract_version": 1, "batch_id": ref, "ownership": {}, "mutations": [{
            "operation_id": ref, "source": "BMKG", "source_ref_id": ref, "operation": "upsert",
            "source_order": {"namespace": self.holder, "position": 1}, "expected_revision": 0,
            "event": {"hazard_type": "SEISMIC", "severity": "NORMAL", "area_name": "HA validation fixture",
                      "latitude": 0, "longitude": 0, "occurred_at": datetime.now(timezone.utc).isoformat(),
                      "attributes": {"platform_validation_run": self.holder}}}]}
        # Persist original logical identities/content before any unknown outcome.
        self.event("fixture-prepared", batch=json.loads(json.dumps(batch)))
        return batch

    def apply(self, batch, seconds=60):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            action = "renew" if self.owner else "acquire"
            request = self.owner or {"scope": "canonical:BMKG", "holder": self.holder}
            status, lease = self.request("POST", "/internal/ownership/" + action, request)
            if status == 403:
                self.owner = None
            elif status == 200:
                self.owner = lease["ownership"]
                batch["ownership"] = self.owner.copy()
                code, result = self.request("POST", "/internal/canonical/batches", batch)
                if code == 200:
                    require(len(result["items"]) == 1 and result["items"][0]["revision"] == 1, "fixture applied with unexpected revision")
                    return result
                require(code in (None, 403, 503), "unexpected canonical result")
                if code == 403:
                    self.owner = None
            else:
                require(status in (None, 503), "validation scope is owned or API rejected the request; stop writers using canonical:BMKG first")
            time.sleep(1)
        raise RuntimeError("canonical success was not observed within deadline; original identities retained in evidence")

    def reconcile(self, batch, result):
        ref = batch["mutations"][0]["source_ref_id"]
        status, entity = self.request("GET", "/internal/canonical/entities?" + urlencode({"source": "BMKG", "source_ref_id": ref}))
        require(status == 200 and entity["hazard_id"] == result["items"][0]["hazard_id"] and entity["revision"] == 1,
                "acknowledged canonical record is missing, changed identity or repeated")
        require(entity["event"]["attributes"].get("platform_validation_run") == self.holder, "canonical fixture content differs")

    def pause(self, name):
        self.cluster.assert_resource(self.cluster.virsh("dumpxml", name).stdout.decode(), name)
        require(self.cluster.virsh("domstate", name).stdout.decode().strip() == "running", "fault target must already be running")
        self.paused.append(name)  # include in cleanup even if interrupted during virsh
        self.cluster.virsh("suspend", name)
        require(self.cluster.virsh("domstate", name).stdout.decode().strip() == "paused", "VM did not pause")
        self.excluded.add(name)
        self.event("vm-paused", node=name)

    def restore(self):
        failures = []
        for name in reversed(self.paused):
            try:
                state = self.cluster.virsh("domstate", name).stdout.decode().strip()
                if state == "paused":
                    self.cluster.virsh("resume", name)
                require(self.cluster.virsh("domstate", name).stdout.decode().strip() == "running", "VM was not restored to running")
                self.event("vm-restored", node=name)
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
                failures.append(f"{name}: {exc}")
        self.excluded.clear()
        self.paused.clear()
        require(not failures, "VM cleanup failed: " + "; ".join(failures))

    def run(self):
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, timeout=5)
        dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, timeout=5)
        self.report.update({"revision": revision.stdout.strip(), "dirty_paths": dirty.stdout.splitlines(), "plan": self.storage})
        with self.cluster.locked():
            try:
                pods = []
                for group in self.storage["groups"]:
                    for member in group["members"]:
                        pod = self.pod(member)
                        require(pod["status"].get("phase") == "Running" and
                                all(c.get("ready") for c in pod["status"].get("containerStatuses", [])),
                                "all planned members must be healthy before a fault scenario")
                        images = [{"name": c["name"], "image_id": c.get("imageID")} for c in pod["status"].get("containerStatuses", [])]
                        require(images and all(c["image_id"] for c in images), "runtime image identity missing")
                        pods.append({"name": member["name"], "node": pod["spec"]["nodeName"], "images": images})
                self.event("runtime-identities", pods=pods)
                before = self.confirmed()
                if self.args.scenario != "observe":
                    require(self.token, "CANONICAL_SERVICE_TOKEN required for fault scenarios")
                    baseline = self.batch("before")
                    result = self.apply(baseline)
                    self.reconcile(baseline, result)
                    primary = before[0]["member"]
                    members = self.storage["groups"][0]["members"]
                    target = next(m["node"] for m in members if m["name"] == primary)
                    self.pause(target)
                    if self.args.scenario == "single-vm":
                        self.confirmed(seconds=90)
                        replay = self.apply(baseline)
                        require(replay["outcome"] == "duplicate" and replay["items"] == result["items"], "retry did not preserve the original result")
                        self.reconcile(baseline, result)
                        next_batch = self.batch("during")
                        next_result = self.apply(next_batch)
                        self.reconcile(next_batch, next_result)
                    else:
                        self.pause(next(m["node"] for m in members if m["node"] != target))
                        pending = self.batch("insufficient")
                        pending["ownership"] = self.owner.copy()
                        deadline = time.monotonic() + 20
                        unavailable = 0
                        while time.monotonic() < deadline:
                            code, _ = self.request("POST", "/internal/canonical/batches", pending)
                            require(code in (None, 503), "insufficient-copy write returned success or unexpected rejection")
                            unavailable += code == 503
                            time.sleep(1)
                        require(unavailable > 0, "only transport failures observed; controlled 503 behavior is unproven")
                    self.restore()
                    self.confirmed(seconds=90)
                    replay = self.apply(baseline)
                    require(replay["outcome"] == "duplicate" and replay["items"] == result["items"], "acknowledged result changed after restoration")
                    self.reconcile(baseline, result)
                    if self.args.scenario == "single-vm":
                        self.reconcile(next_batch, next_result)
                    else:
                        reconciled = self.apply(pending)
                        self.reconcile(pending, reconciled)
                    if self.owner:
                        code, _ = self.request("POST", "/internal/ownership/release", self.owner)
                        require(code == 200, "validation owner release failed")
                self.report["passed"] = True
            finally:
                try:
                    if self.paused:
                        self.restore()
                finally:
                    self.report["finished_at"] = datetime.now(timezone.utc).isoformat()
                    write_json(self.output / "result.json", self.report)
        print(f"Checks passed; evidence: {self.output}")


def parse_lsn(raw):
    require(isinstance(raw, str) and re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", raw), "invalid WAL position")
    high, low = raw.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["observe", "single-vm", "two-vm"])
    parser.add_argument("--inventory", type=Path, default=ROOT / "deployment/platform/inventory.example.json")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--api-url", default="")
    args = parser.parse_args()
    if args.scenario != "observe":
        url = urlsplit(args.api_url)
        require(url.scheme in ("http", "https") and url.hostname and not url.username and not url.password and not url.query and not url.fragment,
                "provide a healthy shared canonical API entry with --api-url")
    checks = Checks(args)
    try:
        checks.run()
    except BaseException as exc:
        checks.report["passed"] = False
        checks.report["error"] = type(exc).__name__ + ": " + str(exc)
        write_json(checks.output / "result.json", checks.report)
        raise


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"HA check failed: {exc}", file=sys.stderr)
        sys.exit(1)
