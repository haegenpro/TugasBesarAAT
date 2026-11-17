"""Focused offline checks for separately owned formations and broker limits."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from bundle_lib import inspect
from service_lib import broker_capacity
from storage_lib import manifests,plan

ROOT=Path(__file__).resolve().parent.parent

class BundleIntegrationChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=json.loads((ROOT/"deployment/platform/inventory.example.json").read_text())
        for node in self.inventory["nodes"]:node["memory_mib"]=8192
        self.image="registry.invalid/storage@sha256:"+"a"*64

    def inspect_resources(self,resources):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)
            (root/"resources.json").write_text(json.dumps(resources))
            (root/"bundle.json").write_text(json.dumps({"version":1,"node_reserve_mib":512,"stages":[{"id":"storage","manifest":"resources.json"}]}))
            return inspect(root/"bundle.json",self.inventory)[0]

    def test_ingestion_storage_is_owned_but_foreign_formation_is_rejected(self):
        resources=manifests(plan(self.inventory,"postgres",self.image,"ingestion"))
        report=self.inspect_resources(resources)
        self.assertTrue(report["ready_for_apply"])
        self.assertIn("aat-m2-ingestion",report["formations"])
        workload=next(o for o in resources["items"]if o["kind"]=="StatefulSet")
        workload["spec"]["template"]["metadata"]["labels"]["platform.aat/formation"]="aat-other-ingestion"
        with self.assertRaisesRegex(ValueError,"undeclared storage formation"):self.inspect_resources(resources)

    def test_cluster_label_cannot_hide_conflicting_formation(self):
        resources=manifests(plan(self.inventory,"postgres",self.image))
        workload=next(o for o in resources["items"]if o["kind"]=="StatefulSet")
        workload["spec"]["template"]["metadata"]["labels"]["platform.aat/cluster"]="aat-other"
        with self.assertRaisesRegex(ValueError,"another cluster"):self.inspect_resources(resources)

    def test_broker_reservations_and_disk_headroom_are_both_enforced(self):
        catalog=json.loads((ROOT/"deployment/services/catalog.json").read_text())
        value=broker_capacity(catalog)
        self.assertEqual(value["reserved_bytes_per_broker"],(5<<30)+(3*(256<<20)))
        for limit,volume in ((1<<30,"10Gi"),(8<<30,"2Gi")):
            bad=copy.deepcopy(catalog);bad["jetstream"]["max_file_store_bytes"]=limit
            next(s for s in bad["services"]if s["name"]=="broker")["storage"]["size"]=volume
            with self.assertRaises(ValueError):broker_capacity(bad)

    def test_missing_tls_and_password_keys_in_supplied_secrets_block_release(self):
        from storage_lib import manifests,plan
        resources=manifests(plan(self.inventory,"postgres",self.image))
        names={c["valueFrom"]["secretKeyRef"]["name"] for o in resources["items"]if o["kind"]=="StatefulSet"for c in o["spec"]["template"]["spec"]["containers"][0].get("env",[])if "secretKeyRef"in c.get("valueFrom",{})}
        name=next(iter(names))
        resources["items"].append({"apiVersion":"v1","kind":"Secret","metadata":{"name":name,"namespace":self.inventory["namespace"]},"stringData":{"POSTGRES_PASSWORD":"private"}})
        report=self.inspect_resources(resources)
        self.assertFalse(report["ready_for_apply"])
        self.assertTrue(any("APP_PASSWORD"in message for message in report["blocked"]))
        self.assertNotIn("private",json.dumps(report))

if __name__=="__main__":unittest.main()
