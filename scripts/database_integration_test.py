"""Owned service database isolation, capacity and release assembly checks."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from bundle_lib import inspect
from database_lib import render, schema_sql
from monitoring_lib import IMAGES
from platform_lib.inventory import load
from release_lib import assemble
from storage_lib import SERVICE_DATABASES, durability_config

ROOT=Path(__file__).resolve().parent.parent

class DatabaseIntegrationChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        self.image="registry.invalid/fixture@sha256:"+"a"*64

    def test_each_owner_has_three_distinct_node_copies_and_restricted_role(self):
        resources,report=render(self.inventory,self.image)
        self.assertEqual(report["memory_requests_mib"],8704)
        placements=set()
        for storage in report["formations"]:
            suffix=storage["formation_suffix"];group=storage["groups"][0]
            nodes={m["node"]for m in group["members"]}
            self.assertEqual(len(nodes),3);placements.add(frozenset(nodes))
            self.assertEqual(storage["runtime_role"],SERVICE_DATABASES[suffix][1])
            alias=next(o for o in resources["items"]if o["kind"]=="Service"and o["metadata"]["name"]==suffix)
            routers=[o for o in resources["items"]if o["kind"]=="Deployment"and all(o["spec"]["template"]["metadata"]["labels"].get(k)==v for k,v in alias["spec"]["selector"].items())]
            self.assertEqual(len(routers),1)
            self.assertEqual(routers[0]["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/name"],suffix)
            self.assertIn("REVOKE CREATE ON SCHEMA public",schema_sql(storage))
        self.assertEqual(len(placements),4)
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)
            (root/"resources.json").write_text(json.dumps(resources))
            (root/"bundle.json").write_text(json.dumps({"version":1,"formations":[s["formation"]for s in report["formations"]],"node_reserve_mib":512,"stages":[{"id":"owners","manifest":"resources.json"}]}))
            budget,_=inspect(root/"bundle.json",self.inventory)
            self.assertEqual(budget["memory_requests_mib"],8704)

    def test_integrated_release_owns_all_databases_and_reports_capacity(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)
            catalog=json.loads((ROOT/"deployment/services/catalog.json").read_text())
            owner=json.loads((ROOT/"deployment/role1/config.example.json").read_text())
            owner.update(analytics_enabled=True,field_priority_enabled=True,bmkg_namespace="bmkg-test-release",satellite_namespace="satellite-test-release")
            owner["r3"]={"field_reports_namespace":"reports-test-release","coastal_namespace":"coastal-test-release","satellite_feed_url":"https://feed.invalid"}
            processor=json.loads((ROOT/"docs/fixtures/wildfire-m6/config.json").read_text())
            processor["namespace"]=owner["satellite_namespace"]
            owner["satellite_initial_offset"]=processor["initial_offset"]
            owner["wildfire"]={"config_json":json.dumps(processor),"province_json":(ROOT/"docs/fixtures/wildfire-m5/provinces.json").read_text()}
            images={name:self.image for name in IMAGES}
            images.update({s["image_key"]:self.image for s in catalog["services"]if "image_key"in s})
            images.update(storage=self.image,role1=self.image)
            aggregator=json.loads((ROOT/"deployment/aggregator/config.example.json").read_text());aggregator["storage_mode"]="citus"
            inputs={"images":images,"aggregator":aggregator,"role1":owner,"services":catalog,"external_flows":[{"app":"ingestion","cidr":"192.0.2.1/32","ports":[443]}]}
            config={"version":1,"node_reserve_mib":512}
            for name,value in inputs.items():
                config[name]=name+".json";(root/config[name]).write_text(json.dumps(value))
            (root/"release.json").write_text(json.dumps(config))
            report=assemble(self.inventory,root/"release.json",root/"output")
            self.assertFalse(report["ready_for_apply"])
            self.assertTrue(any("memory"in v for v in report["blocked"]))
            for suffix in SERVICE_DATABASES:
                self.assertIn(self.inventory["cluster_id"]+"-"+suffix,report["formations"])
                self.assertTrue((root/"output"/(suffix+"-plan.json")).is_file())
            monitoring=json.loads((root/"output/artifacts/monitoring.json").read_text())
            prom=next(o for o in monitoring["items"]if o["kind"]=="ConfigMap"and o["metadata"]["name"]=="prometheus-config")
            jobs=json.loads(prom["data"]["prometheus.json"])["scrape_configs"]
            self.assertEqual(len(next(j for j in jobs if j["job_name"]=="patroni")["static_configs"]),30)
            self.assertEqual(len(next(j for j in jobs if j["job_name"]=="storage-etcd")["static_configs"]),18)
            self.assertEqual(len(report["formations"]),6)
            # Private Secrets are staged before any storage, and supplied-but-
            # incomplete keys cannot disappear from offline preflight.
            (root/"secrets.json").write_text(json.dumps({"kind":"List","items":[{"apiVersion":"v1","kind":"Secret","metadata":{"name":owner["runtime_secret"],"namespace":self.inventory["namespace"]},"stringData":{"AUDIT_CURSOR_KEY":"private"}}]}))
            config["secrets"]="secrets.json"
            (root/"release.json").write_text(json.dumps(config))
            with_secrets=assemble(self.inventory,root/"release.json",root/"with-secrets")
            self.assertEqual(with_secrets["stages"][0]["id"],"secrets")
            self.assertEqual(with_secrets["stages"][1]["requires"],["secrets"])
            self.assertTrue(any("AUDIT_TOKEN"in v and "missing keys"in v for v in with_secrets["blocked"]))
            namespaces=[]
            for stage in with_secrets["stages"]:
                namespaces += [o for o in json.loads(Path(stage["manifest"]).read_text())["items"]if o["kind"]=="Namespace"]
            self.assertEqual(len(namespaces),1)

if __name__=="__main__":unittest.main()
