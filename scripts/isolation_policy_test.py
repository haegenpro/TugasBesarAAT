"""Deferred policy-structure checks; runtime connectivity still needs CNI proof."""
import json
from pathlib import Path
import unittest
from isolation_lib import render
ROOT=Path(__file__).resolve().parent.parent
class IsolationChecks(unittest.TestCase):
    def setUp(self):self.inventory=json.loads((ROOT/"deployment/platform/inventory.example.json").read_text())
    def test_default_deny_and_no_consumer_canonical_access(self):
        items=render(self.inventory)["items"]
        deny=next(p for p in items if p["metadata"]["name"].endswith("default-deny"))["spec"]
        self.assertEqual(deny["ingress"],[]);self.assertEqual(deny["egress"],[])
        pg=next(p for p in items if p["metadata"]["name"].endswith("canonical-postgres"))
        allowed=json.dumps(pg["spec"]["ingress"])
        self.assertIn("aggregator",allowed);self.assertNotIn("consumer-dashboard",allowed);self.assertNotIn("client-api",allowed)
        self.assertTrue(all(p["metadata"]["namespace"]==self.inventory["namespace"] for p in items))
        self.assertNotIn("tester-agent",json.dumps(items))
    def test_unbounded_external_egress_rejected(self):
        with self.assertRaises(ValueError):render(self.inventory,[{"app":"ingestion","cidr":"0.0.0.0/0","ports":[443]}])
if __name__=="__main__":unittest.main()
