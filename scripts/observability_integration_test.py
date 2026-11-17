"""Offline discovery, broker-member coverage and truthful SLO dashboard checks."""
import json
from pathlib import Path
import unittest
from isolation_lib import render as isolation
from monitoring_lib import IMAGES,render
from role1_integration_lib import render as role1
from storage_lib import plan

ROOT=Path(__file__).resolve().parent.parent

class ObservabilityIntegrationChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=json.loads((ROOT/"deployment/platform/inventory.fourhost.example.json").read_text())
        self.image="registry.invalid/test@sha256:"+"a"*64

    def test_all_broker_members_are_observed_and_collection_failures_are_visible(self):
        resources,_=render(self.inventory,plan(self.inventory,"citus",self.image),{name:self.image for name in IMAGES})
        exporters=[o for o in resources["items"]if o["kind"]=="Deployment"and o["metadata"]["name"].startswith("nats-exporter-")]
        self.assertEqual(len(exporters),3)
        targets={o["spec"]["template"]["spec"]["containers"][0]["command"][-1]for o in exporters}
        self.assertEqual(len(targets),3)
        for letter in "abc":self.assertTrue(any("broker-"+letter+"-0."in target for target in targets))
        maps={o["metadata"]["name"]:o["data"]for o in resources["items"]if o["kind"]=="ConfigMap"}
        alerts=json.loads(maps["prometheus-config"]["alerts.json"])
        budget=next(r for r in alerts["groups"][0]["rules"]if r["alert"]=="CoastalTimelinessBudgetExhausted")
        for signal in ("collection_success","coverage_complete","clock_anomalies"):self.assertIn(signal,budget["expr"])
        policies=isolation(self.inventory)
        broker=next(o["spec"]for o in policies["items"]if o["metadata"]["name"].endswith("-broker"))
        for letter in "abc":self.assertIn("nats-exporter-"+letter,json.dumps(broker["ingress"]))

    def test_each_owner_deployment_declares_metrics_discovery(self):
        aggregator=json.loads((ROOT/"deployment/aggregator/config.example.json").read_text())
        config=json.loads((ROOT/"deployment/role1/config.example.json").read_text())
        config["r3"]={"field_reports_namespace":"field-v1","coastal_namespace":"coastal-v1","satellite_feed_url":"https://feed.invalid"}
        objects=role1(self.inventory,aggregator,config,self.image,self.image)
        names=set()
        for o in objects["items"]:
            if o["kind"]!="Deployment":continue
            template=o["spec"]["template"]
            self.assertEqual(template["metadata"]["annotations"]["prometheus.io/scrape"],"true")
            names.add(template["metadata"]["labels"]["app.kubernetes.io/name"])
        self.assertEqual(names,{"aggregator","ingestion","coastal-processor"})

if __name__=="__main__":unittest.main()
