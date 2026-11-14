"""Hand-specified recovery failures; no storage or infrastructure actions."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from recovery_checks import audit_digest, content_digest, placement_checks, verify

ROOT = Path(__file__).resolve().parent.parent


def evidence():
    ack = {"source": "BMKG", "source_ref_id": "a", "operation_id": "op-a", "batch_id": "batch-a",
           "hazard_id": "00000000-0000-4000-8000-000000000001", "revision": 1, "active": False,
           "ingested_at": "2026-01-01T00:00:00Z", "content_sha256": None, "result": {"revision": 1}}
    entity = {k: ack[k] for k in ("source", "source_ref_id", "hazard_id", "revision", "active", "ingested_at")}
    entity["event"] = None
    checksum = {"source": "BMKG", "count": 0, "checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
    return {"version": 1, "dataset": {"id": "synthetic", "sha256": "a"*64},
            "checkpoint": {"id": "stable-1", "stable": True, "checksum": checksum},
            "acknowledgements": [ack], "uncertain_operations": [],
            "observed": {"entities": [entity], "operations": [{k: ack[k] for k in ("source", "operation_id", "batch_id", "result")}],
                         "checksum": {**checksum, "partial": False}}}


def topology():
    units = []
    for group in range(4):
        for node in range(4):
            if group != node:
                units.append({"name": f"g{group}-n{node}", "group": group, "node": f"node-{node}",
                              "identity_verified": True, "holdings_complete": True, "complete": True, "shards": [f"s{group}"]})
    return {"verified": True, "units": units}


class RecoveryChecks(unittest.TestCase):
    def test_stable_inactive_identity_and_empty_checksum(self):
        self.assertEqual(verify(evidence())["status"], "passed")

    def test_missing_acknowledged_operation_or_entity(self):
        for collection in ("operations", "entities"):
            v = evidence(); v["observed"][collection] = []
            with self.assertRaises(ValueError): verify(v)

    def test_changed_outcome_identity_or_tombstone(self):
        for field, value in (("revision", 2), ("hazard_id", "different"), ("ingested_at", "different"), ("active", True)):
            v = evidence(); v["observed"]["entities"][0][field] = value
            with self.assertRaises(ValueError): verify(v)
        v = evidence(); v["observed"]["operations"][0]["result"] = {"revision": 2}
        with self.assertRaises(ValueError): verify(v)

    def test_duplicate_logical_copies_rejected(self):
        for collection in ("operations", "entities"):
            v = evidence(); v["observed"][collection] *= 2
            with self.assertRaises(ValueError): verify(v)

    def test_uncertain_outcome_cannot_pass_by_age(self):
        v = evidence(); v["uncertain_operations"] = [{"operation_id": "unknown", "age_seconds": 999999}]
        self.assertEqual(verify(v)["status"], "blocked")

    def test_incomplete_or_unstable_checksum_rejected(self):
        for field in ("partial", "stable"):
            v = evidence()
            if field == "partial": v["observed"]["checksum"][field] = True
            else: v["checkpoint"][field] = False
            with self.assertRaises(ValueError): verify(v)

    def test_unicode_and_cross_source_duplicate_checksum(self):
        # Independent serialization: duplicate a retained, U+00E9 follows ASCII.
        want = hashlib.sha256(b"a\na\nz\n\xc3\xa9").hexdigest()
        self.assertEqual(audit_digest(["é", "a", "z", "a"]), want)
        self.assertEqual(audit_digest([]), evidence()["checkpoint"]["checksum"]["checksum"])
        fixture = json.loads((ROOT/"docs/fixtures/storage-recovery-v1.json").read_text(encoding="utf-8"))
        for vector in fixture["audit_vectors"]:
            self.assertEqual(audit_digest(vector["references"]), vector["sha256"])

    def test_fresh_control_snapshots_must_agree(self):
        v = evidence(); c = {"primary": True, "pending": False, "generation": 17}
        v["observed"]["control"] = {"before": c, "after": copy.deepcopy(c)}
        self.assertEqual(verify(v)["status"], "passed")
        for field, value in (("generation", 18), ("pending", True), ("primary", False)):
            changed = copy.deepcopy(v); changed["observed"]["control"]["after"][field] = value
            with self.assertRaises(ValueError): verify(changed)

    def test_content_digest_has_explicit_wire_serialization(self):
        event = {"hazard_type": "SEISMIC", "severity": "NORMAL", "area_name": "A", "latitude": 0,
                 "longitude": 0, "occurred_at": "2026-01-01T00:00:00Z", "attributes": {}}
        expected = b'{"area_name":"A","attributes":{},"hazard_type":"SEISMIC","latitude":0,"longitude":0,"occurred_at":"2026-01-01T00:00:00Z","severity":"NORMAL"}'
        self.assertEqual(content_digest(event), hashlib.sha256(expected).hexdigest())
        v = evidence(); v["acknowledgements"][0].update(active=True, content_sha256=hashlib.sha256(expected).hexdigest())
        v["observed"]["entities"][0].update(active=True, event=event)
        self.assertEqual(verify(v)["status"], "passed")
        v["observed"]["entities"][0]["event"]["attributes"] = {"changed": True}
        with self.assertRaises(ValueError): verify(v)

    def test_four_node_replica_union(self):
        self.assertEqual(len(placement_checks(topology(), True)), 4)
        for field in ("identity_verified", "holdings_complete", "complete"):
            v = topology(); v["units"][0][field] = False
            with self.assertRaises(ValueError): placement_checks(v, True)

    def test_transient_copy_can_make_node_hold_entire_dataset(self):
        v = topology()
        v["units"].append({"name": "transient-g0-n0", "group": 0, "node": "node-0", "identity_verified": True,
                           "holdings_complete": True, "complete": False, "shards": ["s0"]})
        with self.assertRaisesRegex(ValueError, "complete dataset"): placement_checks(v)

    def test_cli_failure_is_nonzero_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"evidence.json"
            v = evidence(); v["observed"]["entities"] = []; path.write_text(json.dumps(v), encoding="utf-8")
            run = subprocess.run([sys.executable, str(ROOT/"scripts/check-storage-recovery.py"), "verify", "--evidence", str(path)], capture_output=True, text=True, timeout=5)
            self.assertEqual(run.returncode, 1)
            self.assertEqual(json.loads(run.stderr)["status"], "failed")
        spec = importlib.util.spec_from_file_location("recovery_cli", ROOT/"scripts/check-storage-recovery.py")
        cli = importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"large.json"; path.write_bytes(b"x"*32)
            with self.assertRaises(ValueError): cli.read_json(path, 16)

    def test_offline_plan_reuses_original_replacement_boundaries(self):
        from storage_lib import plan
        inventory = json.loads((ROOT/"deployment/platform/inventory.example.json").read_text())
        storage = plan(inventory, "citus", "example.invalid/pg@sha256:"+"a"*64)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"plan.json";path.write_text(json.dumps(storage), encoding="utf-8")
            run=subprocess.run([sys.executable,str(ROOT/"scripts/check-storage-recovery.py"),"plan","--storage-plan",str(path),"--node",inventory["nodes"][3]["name"]],capture_output=True,text=True,timeout=5)
            self.assertEqual(run.returncode,0,run.stderr)
            result=json.loads(run.stdout)
            self.assertEqual(result["status"],"blocked")
            self.assertIn("prove VM shut off", result["infrastructure_plan"]["phases"][0])

    def test_observe_is_read_only_and_enforces_collection_quota(self):
        spec=importlib.util.spec_from_file_location("recovery_cli_observe",ROOT/"scripts/check-storage-recovery.py")
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        expected=evidence()
        class Cluster:
            calls=[]
            def kubectl(self,*args,**kwargs):
                self.calls.append(args)
                if args[0]=="get":
                    value={"metadata":{"labels":{"platform.aat/formation":"test"}},"spec":{"nodeName":"node-a"}}
                elif "platform_replication_status" in args[-1]:
                    value={"formation":"test","group_id":0,"member":"member-a","system_id":"100","primary":True}
                else:
                    sql=args[-1]
                    if "canonical_visibility" in sql: value={"primary":True,"pending":False,"generation":7}
                    elif "canonical_operations" in sql: value=expected["observed"]["operations"]
                    elif "canonical_entities" in sql: value=[{"source":"BMKG","source_ref_id":"a","ingested_at":"2026-01-01T00:00:00Z"}]
                    else: raise AssertionError(sql)
                    self.assert_read_only(sql)
                return SimpleNamespace(stdout=json.dumps(value).encode())
            def assert_read_only(self,sql):
                if not sql.startswith("BEGIN READ ONLY;"): raise AssertionError("SQL is not read-only")
        class Response:
            def __init__(self,value): self.raw=json.dumps(value).encode()
            def __enter__(self): return self
            def __exit__(self,*_): pass
            def read(self,n): return self.raw[:n]
        class Opener:
            def open(self,request,**_kwargs):
                if request.get_method()!="GET": raise AssertionError("non-read HTTP method")
                value=expected["observed"]["checksum"] if "/audit/" in request.full_url else expected["observed"]["entities"][0]
                return Response(value)
        args=SimpleNamespace(api_url="http://127.0.0.1:1234",max_operations=1000,max_bytes=1<<20,seconds=5)
        storage={"namespace":"test","formation":"test","groups":[{"id":0,"members":[{"name":"member-a","node":"node-a"}]}]}
        config={"groups":[{"id":0,"system_id":"100"}]}
        with patch.object(cli,"platform",return_value=(Cluster(),storage,config,{})),patch.object(cli,"build_opener",return_value=Opener()),patch.dict("os.environ",{"AUDIT_BEARER_TOKEN":"audit-secret","CANONICAL_SERVICE_TOKEN":"canonical-secret"}):
            document=cli.observe(args,copy.deepcopy(expected))
            self.assertEqual(verify(document)["status"],"passed")
            self.assertNotIn("secret",json.dumps(document))
            args.max_bytes=1
            with self.assertRaisesRegex(ValueError,"quota"): cli.observe(args,copy.deepcopy(expected))


if __name__ == "__main__":
    unittest.main()
