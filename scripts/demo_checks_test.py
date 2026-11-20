"""Demo checks, Wi-Fi network evidence and existing-cluster replacement steps, all with fakes."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import demo_checks_lib as checks
from platform_lib import wifi_evidence

ROOT = Path(__file__).resolve().parent.parent


class Clock:
    def __init__(self):self.now = 0.0
    def __call__(self):return self.now
    def sleep(self, seconds):self.now += max(seconds, 0.01)


def done(value=b"", code=0):
    return subprocess.CompletedProcess([], code, value if isinstance(value, bytes) else json.dumps(value).encode(), b"")


def inventory():
    nodes = [{"name": "aat-fourhost-" + l, "address": "192.168.100." + str(27 + i), "role": "agent" if l == "d" else "server", "physical_host": "host-" + l, "uid": "u-" + l}
             for i, l in enumerate("abcd")]
    return {"version": 3, "cluster_id": "aat-fourhost", "namespace": "sistem-informasi-kebencanaan-bnpb", "nodes": nodes}


class FakeApi:
    def __init__(self, routes):self.routes, self.calls = routes, []
    def request(self, method, path, body=None, params=None, headers=None, timeout=10, raw=None):
        self.calls.append((method, path, params, headers, body, raw))
        value = self.routes(method, path, params or {}, headers or {}, body, raw)
        # A full HTTP response carries "ms"; anything else is a 200 JSON body.
        return value if isinstance(value, dict) and "ms" in value else {"status": 200, "json": value, "ms": 5.0}


class FakeCluster:
    def __init__(self, handler):self.handler, self.calls = handler, []
    def kubectl(self, *args, **kwargs):
        self.calls.append(args)
        return self.handler(args)


def evidence(root, name="check"):
    return checks.Evidence(name, Path(root) / name)


def audit_routes(events, locations):
    def routes(method, path, params, headers, body, raw):
        if path == "/internal/audit/events":
            source = params.get("source")
            return {"items": [e for e in events if not source or e["source"] == source], "next_cursor": None, "partial": False, "unavailable_units": []}
        if path == "/internal/audit/checksum":
            ids = [e["source_ref_id"] for e in events if e["source"] == params["source"]]
            return {"source": params["source"], "count": len(ids), "checksum": checks.checksum(ids), "partial": False}
        if path == "/internal/audit/location":
            return {"hazard_id": params["hazard_id"], "locations": locations[params["hazard_id"]], "partial": False}
        raise AssertionError(path)
    return routes


def store_cluster():
    def handler(args):
        if args[0] == "top":return done(b"x 10m 20Mi\n")
        if args[:2] == ("get", "pods"):return done({"items": [{"metadata": {"name": "x"}, "spec": {"nodeName": "aat-fourhost-a"}, "status": {"phase": "Running"}}]})
        return done({"items": []})
    return FakeCluster(handler)


class DistributionChecks(unittest.TestCase):
    def events(self):
        return [{"hazard_id": "h%02d" % i, "source": "BMKG", "source_ref_id": "r%02d" % i, "ingested_at": "2026-11-20T00:00:%02dZ" % i} for i in range(8)]

    def test_spread_replicated_data_passes(self):
        events = self.events();names = [n["name"] for n in inventory()["nodes"]]
        locations = {e["hazard_id"]: [{"node": names[i % 4], "unit": "g", "role": "primary"}, {"node": names[(i + 1) % 4], "unit": "g-r", "role": "replica"}] for i, e in enumerate(events)}
        with tempfile.TemporaryDirectory() as folder:
            result = checks.distribution(store_cluster(), FakeApi(audit_routes(events, locations)), inventory(), evidence=evidence(folder)).finish()
        self.assertTrue(result["passed"], result["assertions"])
        self.assertEqual(result["distribution"]["per_node"]["aat-fourhost-a"]["primary"], 2)

    def test_single_node_or_unordered_data_fails(self):
        events = self.events()
        everything = {e["hazard_id"]: [{"node": "aat-fourhost-a", "unit": "g", "role": "primary"}] for e in events}
        with tempfile.TemporaryDirectory() as folder:
            result = checks.distribution(store_cluster(), FakeApi(audit_routes(events, everything)), inventory(), evidence=evidence(folder)).finish()
        self.assertFalse(result["assertions"]["no_node_holds_all_data"])
        self.assertFalse(result["assertions"]["every_event_has_two_homes"])
        shuffled = list(reversed(events));names = [n["name"] for n in inventory()["nodes"]]
        locations = {e["hazard_id"]: [{"node": names[0], "role": "primary"}, {"node": names[1], "role": "replica"}] for e in events}
        with tempfile.TemporaryDirectory() as folder:
            result = checks.distribution(store_cluster(), FakeApi(audit_routes(shuffled, locations)), inventory(), evidence=evidence(folder)).finish()
        self.assertFalse(result["assertions"]["audit_order_ingested_at_then_hazard_id"])
        self.assertFalse(result["passed"])


class AcquisitionChecks(unittest.TestCase):
    def test_upstream_duplicate_and_stale_epoch_requests_are_detected(self):
        good = [{"request_id": "a", "epoch": 1, "time": "1"}, {"request_id": "a", "epoch": 1, "recovery": True, "time": "2"}, {"request_id": "b", "epoch": 2, "time": "3"}]
        self.assertTrue(checks.single_acquirer(good)["passed"])
        self.assertFalse(checks.single_acquirer(good + [{"request_id": "b", "epoch": 2, "time": "4"}])["passed"])
        self.assertFalse(checks.single_acquirer(good + [{"request_id": "c", "epoch": 1, "time": "5"}])["passed"])

    def test_takeover_crashes_until_the_holder_changes_and_measures_resumption(self):
        clock = Clock();state = {"holder": "worker-1", "epoch": 1, "killed": []}
        class Observer:
            def query(self, suffix, expression):
                if "canonical_ownership" in expression:
                    if suffix != "storage":return {"data": None}
                    if "aggregator-b" in state["killed"] and clock.now > state["taken_at"]:
                        return {"data": {"holder": "worker-2", "epoch": 2, "expires_at": "t", "now": "t"}}
                    return {"data": {"holder": state["holder"], "epoch": state["epoch"], "expires_at": "t", "now": "t"}}
                return {"data": [{"lane": "l", "epoch": 2, "state": "complete", "completed_at": "t"}]}
        logs = "\n".join(json.dumps({"time": "2026-11-20T00:00:0%dZ" % i, "acquisition": {"request_id": "r%d" % i, "epoch": 1 if i < 3 else 2}}) for i in range(6)).encode()
        def handler(args):
            if args[:2] == ("get", "pods"):
                return done({"items": [{"metadata": {"name": n}, "status": {"phase": "Running"}} for n in ("aggregator-a", "aggregator-b", "aggregator-c")]})
            if args[0] == "delete":
                state["killed"].append(args[2])
                if args[2] == "aggregator-b":state["taken_at"] = clock.now + 12  # lease expiry, then successor
                return done()
            if args[0] == "logs":return done(logs)
            raise AssertionError(args)
        events = [{"hazard_id": "h1", "source": "PVMBG", "source_ref_id": "r1", "ingested_at": "t"}]
        with tempfile.TemporaryDirectory() as folder:
            result = checks.takeover(FakeCluster(handler), Observer(), FakeApi(audit_routes(events, {})), inventory(), scope="acquisition:PVMBG", app="aggregator",
                                     upstream="pvmbg-mock", evidence=evidence(folder), sleep=clock.sleep, clock=clock).finish()
        self.assertTrue(result["passed"], result["assertions"])
        self.assertEqual(result["takeover"]["crashed"], "aggregator-b")
        self.assertEqual(state["killed"], ["aggregator-a", "aggregator-b"])
        self.assertLessEqual(result["takeover"]["resumed"]["seconds"], 60)


class ReportChecks(unittest.TestCase):
    def cluster(self, restarts=0):
        def handler(args):
            if args[:2] == ("get", "pods"):
                return done({"items": [{"metadata": {"name": "ingestion-a"}, "status": {"phase": "Running", "containerStatuses": [{"restartCount": restarts}]}}]})
            return done()
        return FakeCluster(handler)

    def test_malformed_messages_are_refused_and_valid_ones_accepted(self):
        def routes(method, path, params, headers, body, raw):
            return {"status": 400 if raw is not None else 202, "json": {"receipt_id": "x"}, "ms": 1}
        with tempfile.TemporaryDirectory() as folder:
            result = checks.malformed(self.cluster(), FakeApi(routes), inventory(), evidence=evidence(folder), sleep=lambda s: None).finish()
        self.assertTrue(result["passed"], result["assertions"])
        self.assertEqual(set(result["responses"]), set(checks.MALFORMED))

    def test_field_reports_keep_one_receipt_and_are_processed_once(self):
        clock = Clock();receipts = {};polls = {"n": 0}
        def routes(method, path, params, headers, body, raw):
            if method == "POST":
                key = headers["Idempotency-Key"];receipts.setdefault(key, "rcpt-" + key[-3:])
                return {"status": 202, "json": {"receipt_id": receipts[key], "status": "accepted"}, "ms": 3}
            polls["n"] += 1
            return {"status": "processed" if polls["n"] > 10 else "accepted"}
        cluster = self.cluster()
        with tempfile.TemporaryDirectory() as folder:
            result = checks.field_reports(cluster, FakeApi(routes), inventory(), count=6, evidence=evidence(folder), sleep=clock.sleep, clock=clock).finish()
        self.assertTrue(result["passed"], result["assertions"])
        self.assertTrue(any(c[0] == "delete" for c in cluster.calls))

    def test_ops_only_access_matrix(self):
        statuses = {"audit-token": {"/internal/audit/unprocessed": 200, "/internal/ingestion/failures": 403},
                    "ops-token": {"/internal/audit/unprocessed": 403, "/internal/ingestion/failures": 200},
                    "field-token": {"/internal/audit/unprocessed": 401, "/internal/ingestion/failures": 403}}
        class Api:
            def __init__(self, url, token):self.token = token
            def request(self, method, path, **kwargs):return {"status": statuses[self.token][path]}
        with patch.object(checks, "Api", Api), tempfile.TemporaryDirectory() as folder:
            result = checks.ops_only("http://a", "http://i", {"audit": "audit-token", "ops": "ops-token", "field": "field-token", "media": None}, evidence=evidence(folder)).finish()
        self.assertTrue(result["passed"], result["assertions"])
        statuses["field-token"]["/internal/ingestion/failures"] = 200
        with patch.object(checks, "Api", Api), tempfile.TemporaryDirectory() as folder:
            result = checks.ops_only("http://a", "http://i", {"ops": "ops-token", "field": "field-token"}, evidence=evidence(folder)).finish()
        self.assertFalse(result["passed"])


class WifiEvidenceChecks(unittest.TestCase):
    def logs(self):
        logs = {}
        for letter in "abcd":
            cut = letter == "c";rows = [{"type": "start", "run_id": "r", "disconnect": cut}]
            for t in range(10):
                down = cut and 3 <= t < 6
                rows.append({"type": "sample", "run_id": "r", "link_up": not down, "peers": [{"node": "aat-fourhost-" + o, "rtt_ms": None if down else 4.0 + t} for o in "abcd"]})
            if cut:
                rows.insert(4, {"type": "link-disabled", "run_id": "r"});rows.insert(8, {"type": "link-enabled", "run_id": "r"})
            rows.append({"type": "end", "run_id": "r"});logs["host-" + letter] = rows
        controller = [{"type": "sample", "run_id": "r", "nodes": [{"node": "aat-fourhost-" + l, "reachable": not (l == "c" and 3 <= t < 6), "ready": not (l == "c" and 3 <= t < 7)} for l in "abcd"]} for t in range(10)]
        return logs, controller

    def test_real_cut_with_recovery_and_application_evidence_passes(self):
        logs, controller = self.logs()
        result = wifi_evidence.reconcile(inventory(), "r", logs, controller)
        self.assertTrue(all(result["assertions"].values()), result["assertions"])
        self.assertEqual(result["status"], "blocked")  # network-only evidence never awards the bonus
        self.assertEqual(wifi_evidence.reconcile(inventory(), "r", logs, controller, {"acknowledged": True}, {"video": "x"})["status"], "passed")
        self.assertEqual(result["fault"]["node"], "aat-fourhost-c")

    def test_unrestored_adapter_or_foreign_run_fails(self):
        logs, controller = self.logs()
        logs["host-c"] = [r for r in logs["host-c"] if r["type"] != "link-enabled"]
        self.assertEqual(wifi_evidence.reconcile(inventory(), "r", logs, controller, {"a": 1}, {"b": 1})["status"], "failed")
        logs, controller = self.logs();logs["host-a"][1]["run_id"] = "other"
        with self.assertRaises(ValueError):wifi_evidence.reconcile(inventory(), "r", logs, controller)

    def test_host_probe_disables_then_always_restores_wifi(self):
        spec = importlib.util.spec_from_file_location("host_probe", ROOT / "scripts/platform-host-probe.py")
        probe = importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
        class Link:
            def __init__(self):self.enabled, self.changes = True, []
            def up(self):return self.enabled
            def set(self, value):self.enabled = value;self.changes.append(value)
        class Args:
            run_id, seconds, interface, http_url, disconnect_after, disconnect_seconds = "abcdef12", 60, "Wi-Fi", None, 15, 10
        clock = Clock()
        with tempfile.TemporaryDirectory() as folder:
            link = Link();path = Path(folder) / "h.jsonl"
            probe.probe(Args, inventory(), link, path, sleep=clock.sleep, clock=clock, rtt=lambda a, p: 3.0)
            rows = [json.loads(l) for l in path.read_text().splitlines()]
        self.assertEqual(link.changes, [False, True])
        self.assertEqual([r["type"] for r in rows if r["type"] not in ("sample",)], ["start", "link-disabled", "link-enabled", "end"])
        self.assertTrue(any(r["type"] == "sample" and r["link_up"] is False for r in rows))
        clock = Clock()
        def failing(address, port):
            if clock.now > 20:raise KeyboardInterrupt
            return 3.0
        with tempfile.TemporaryDirectory() as folder:
            link = Link()
            with self.assertRaises(KeyboardInterrupt):probe.probe(Args, inventory(), link, Path(folder) / "h.jsonl", sleep=clock.sleep, clock=clock, rtt=failing)
        self.assertEqual(link.changes, [False, True])
        self.assertTrue(link.enabled)


class ExistingReplacementSteps(unittest.TestCase):
    def runner(self):
        from replacement_resume_lib import ReplacementRunner
        runner = object.__new__(ReplacementRunner)
        runner.existing = True;runner.inventory = inventory();runner.target = runner.inventory["nodes"][2];runner.ns = runner.inventory["namespace"];runner.run_id = "run1"
        runner.prepared = {"volumes": [{"claim": "aat-fourhost-storage-g0-c", "path": "/var/lib/aat/local-pv/aat-fourhost-storage/aat-fourhost-storage-g0-c"},
                                       {"claim": "aat-fourhost-storage-etcd-c", "path": "/var/lib/aat/local-pv/aat-fourhost-storage/aat-fourhost-storage-etcd-c"},
                                       {"claim": "aat-fourhost-broker-c", "path": "/var/lib/aat/local-pv/aat-fourhost-services/aat-fourhost-broker-c", "uid": 1001}]}
        class Cluster:
            def __init__(self):self.calls, self.state = [], "unreachable"
            def vm_state(self, node):return self.state
            def kubectl(self, *args, **kwargs):
                self.calls.append(args)
                if args[0] == "get" and args[1] == "node":return done({"metadata": {"uid": "u-c"}})
                if args[0] == "get" and args[1] == "pod":return done({"metadata": {"uid": "p1"}})
                return done()
            def guest_step(self, node, step, script, payload, run_id):
                self.calls.append(("guest", step, json.loads(payload) if step == "prepare-volumes" else len(payload), script));return {"confirmed": True}
        runner.cluster = Cluster()
        return runner

    def test_fencing_keeps_the_k3s_node_and_recreates_owned_volumes(self):
        runner = self.runner()
        runner.fenced()
        runner.cluster.state = "running"
        with self.assertRaises(ValueError):runner.fenced()
        runner.cluster.state = "shut off"
        runner.remove_node({"node_uid": "u-c", "pods": [{"name": "aat-fourhost-storage-g0-c-0", "uid": "p1"}]})
        self.assertFalse(any(c[:2] == ("delete", "node") for c in runner.cluster.calls))
        self.assertTrue(any(c[:2] == ("delete", "pod") for c in runner.cluster.calls))
        runner.prepare_volumes()
        step = next(c for c in runner.cluster.calls if c[0] == "guest")
        self.assertEqual([v["uid"] for v in step[2]["volumes"]], [999, 1000, 1001])
        self.assertEqual(step[2]["address"], "192.168.100.29")
        self.assertIn("os.replace(path,target)", step[3])  # original data is moved aside, never deleted
        self.assertNotIn("rmtree", step[3])

    def test_restore_exports_the_same_restore_program_with_its_fixed_argument(self):
        import hashlib
        runner = self.runner()
        with tempfile.TemporaryDirectory() as folder:
            archive = Path(folder) / "a.tar.gz";archive.write_bytes(b"archive")
            volume = {"claim": "aat-fourhost-bmkg-a", "path": "/var/lib/aat/local-pv/x/y", "uid": 10001, "archive": str(archive), "sha256": hashlib.sha256(b"archive").hexdigest(),
                      "controller": {"kind": "StatefulSet", "name": "bmkg"}}
            runner.report = {"steps": {"preflight": {"result": {"holds": {"statefulset/bmkg": {"uid": "p1", "replicas": 1}}}}}}
            runner.cluster.kubectl = lambda *args, **kw: done({"metadata": {"uid": "p1"}, "spec": {"replicas": 0}})
            runner.restore(volume)
        step = next(c for c in runner.cluster.calls if c[0] == "guest")
        self.assertEqual(step[1], "restore:aat-fourhost-bmkg-a")
        self.assertTrue(step[3].startswith("import sys\nsys.argv="))


if __name__ == "__main__":
    unittest.main()
