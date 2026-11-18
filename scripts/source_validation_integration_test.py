"""Reference-based zero-loss/timing checks and guarded disconnect cleanup."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from source_validation_lib import analyze,burst,literal,Observer,timestamp


class SourceValidationChecks(unittest.TestCase):
    def fixture(self):
        identity="warning-v1:"+"a"*64
        reference={"satellite":{"namespace":"satellite","records":[{"offset":0,"payload_sha256":"a"*64},{"offset":1,"payload_sha256":"b"*64}]},"coastal":{"namespace":"coastal","critical_notification_ids":[identity]},"audit":{"source":"SATELIT","count":2,"checksum":"c"*64}}
        observed={"satellite":[dict(r,state="applied")for r in reference["satellite"]["records"]],"progress":{"received_through":1,"prepared_through":1,"applied_through":1},"coastal_expected":[{"notification_id":identity,"observation_id":"obs","first_received_at":"2026-11-18T00:00:00Z","rejected":False}],"coastal_receipts":[{"notification_id":identity,"observation_id":"obs","first_received_at":"2026-11-18T00:00:00Z","committed_at":"2026-11-18T00:00:02Z"}],"dispatch":{"storage":[],"ingestion":[]},"coastal_conflicts":0,"clock_samples":{"ingestion":[[-.01,.01]],"notifier-db":[[-.01,.01]]},"audit":dict(reference["audit"],partial=False,unavailable_units=[])}
        return reference,observed

    def test_zero_loss_checks_use_independent_denominator_and_conservative_latency(self):
        reference,observed=self.fixture();result=analyze(reference,observed)
        self.assertTrue(result["passed"]);self.assertAlmostEqual(result["critical_p95_upper_bound_ms"],2020,delta=.01)
        for fault in("source-missing","receipt-missing","payload-changed","unapplied","conflict","clock","time-origin","late","partial-audit","failed-burst","pending-dispatch","missing-dispatch"):
            with self.subTest(fault=fault):
                changed=copy.deepcopy(observed)
                if fault=="pending-dispatch":changed["dispatch"]["ingestion"]=[{"blocking":True}]
                if fault=="missing-dispatch":changed.pop("dispatch")
                if fault=="source-missing":changed["coastal_expected"]=[]
                if fault=="receipt-missing":changed["coastal_receipts"]=[]
                if fault=="payload-changed":changed["satellite"][0]["payload_sha256"]="changed"
                if fault=="unapplied":changed["progress"]["applied_through"]=0
                if fault=="conflict":changed["coastal_conflicts"]=1
                if fault=="clock":changed["clock_samples"]["notifier-db"]=[[30,31]]
                if fault=="time-origin":changed["coastal_receipts"][0]["first_received_at"]="2026-11-18T00:00:01Z"
                if fault=="late":changed["coastal_receipts"][0]["committed_at"]="2026-11-18T00:00:06Z"
                if fault=="partial-audit":changed["audit"]["partial"]=True
                if fault=="failed-burst":changed["burst"]={"speed":60,"new_source_offsets":10,"target_offset":10,"applied_through":10,"catchup_seconds":61}
                self.assertFalse(analyze(reference,changed)["passed"])

    def test_sql_literals_cannot_close_quoted_input_and_reader_requires_original_identity(self):
        value="' ; DROP TABLE anything; -- $aat_input$";quoted=literal(value)
        self.assertIn(value,quoted);self.assertNotEqual(quoted.split("'",1)[0],"")
        class API:
            cfg={"namespace":"ns"}
            def kubectl(self,*args,**kw):
                self.sql=kw["input"].decode()
                return subprocess.CompletedProcess(args,0,json.dumps({"physical":{"formation":"formation","system_id":"999","member":"member-0","group_id":0,"primary":True},"clock_epoch":1,"data":{}}).encode(),b"")
        api=API();observer=Observer(api,{}, {"durability":{"storage":{"formation":"formation","groups":[{"system_id":"123","members":[{"name":"member-0"}]}]}}})
        with self.assertRaisesRegex(ValueError,"original captured"):observer.query("storage","SELECT '{}'::jsonb")
        self.assertTrue(api.sql.startswith("BEGIN READ ONLY"))

    def test_satellite_entry_timing_requires_new_operation_coverage_and_all_vm_clocks(self):
        reference,observed=self.fixture();reference["satellite"]["entry_p95_max_ms"]=3000
        observed["burst"]={"speed":60,"new_source_offsets":2,"initial_applied_through":-1,"target_offset":1,"applied_through":1,"catchup_seconds":2}
        observed["satellite_node_clock_intervals"]=[[-.01,.01]for _ in range(4)]
        for row in observed["satellite"]:
            row["first_received_at"]="2026-11-18T00:00:00Z";row["applied_observed_epoch"]=timestamp(row["first_received_at"])+2.5
        result=analyze(reference,observed)
        self.assertTrue(result["passed"],result["failures"]);self.assertAlmostEqual(result["satellite_entry_timing"]["p95_upper_bound_ms"],2510,delta=.01)
        changed=copy.deepcopy(observed);changed["satellite"][0].pop("first_received_at")
        self.assertFalse(analyze(reference,changed)["passed"])
        changed=copy.deepcopy(observed);changed["satellite_node_clock_intervals"].pop()
        self.assertFalse(analyze(reference,changed)["passed"])
        reference["satellite"]["entry_p95_max_ms"]=1000
        self.assertFalse(analyze(reference,observed)["passed"])

    def test_lost_scale_reply_restores_only_original_controller(self):
        for recreate in(False,True):
            with self.subTest(recreate=recreate),tempfile.TemporaryDirectory()as folder:
                class API:
                    cfg={"cluster_id":"aat-test","namespace":"ns"}
                    def __init__(self):self.replicas=3;self.uid="original";self.patches=0
                    def kubectl(self,*args,**kw):
                        if args[0]=="get":
                            value={"metadata":{"uid":self.uid},"spec":{"replicas":self.replicas,"template":{"metadata":{"labels":{"platform.aat/cluster":"aat-test","app.kubernetes.io/name":"ingestion"}}}}}
                            return subprocess.CompletedProcess(args,0,json.dumps(value).encode(),b"")
                        changes=json.loads(args[-1]);self.patches+=1
                        if changes[0]["value"]!=self.uid:raise ValueError("UID mismatch")
                        self.replicas=changes[-1]["value"]
                        if self.patches==1:
                            if recreate:self.uid="replacement"
                            raise RuntimeError("reply lost after hold")
                        return subprocess.CompletedProcess(args,0,b"",b"")
                api=API()
                with patch("source_validation_lib.http_json",return_value={"group":"group","profile":"practice","speed":60,"latest_offset":10}):
                    with self.assertRaises((ValueError,RuntimeError)):burst(api,None,"https://feed.invalid","private",2,Path(folder))
                state=json.loads((Path(folder)/"burst-state.json").read_text())
                if recreate:self.assertEqual(api.replicas,0);self.assertIn("cleanup_required",state);self.assertEqual(api.patches,1)
                else:self.assertEqual(api.replicas,3);self.assertTrue(state["restored"])


if __name__=="__main__":unittest.main()
