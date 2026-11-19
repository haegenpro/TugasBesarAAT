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
        patroni={p['podSelector']['matchLabels'].get('app.kubernetes.io/name')for rule in pg['spec']['ingress']if any(port['port']==8008 for port in rule['ports'])for p in rule.get('from',[])if 'podSelector'in p}
        self.assertIn('aggregator',patroni)
        self.assertNotIn('client-api',patroni);self.assertNotIn('consumer-dashboard',patroni)
        self.assertTrue(all(p["metadata"]["namespace"]==self.inventory["namespace"] for p in items))
        self.assertNotIn("tester-agent",json.dumps(items))
    def test_only_the_four_owners_reach_the_shared_service_databases(self):
        items=render(self.inventory)["items"]
        shared=next(p for p in items if p["metadata"]["name"]==self.inventory["cluster_id"]+"-service-db")["spec"]
        sources={peer["podSelector"]["matchLabels"]["app.kubernetes.io/name"]for rule in shared["ingress"]if 5432 in[p["port"]for p in rule["ports"]]for peer in rule["from"]}
        owners={"auth-service","schema-registry","consumer-dashboard","consumer-notifier"}
        self.assertEqual(sources,owners|{"auth-db-init","registry-db-init","dashboard-db-init","notifier-db-init","service-db"})
        for name in("aggregator","client-api","ingestion","wildfire-processor","coastal-processor"):
            self.assertNotIn(name,sources)
        reaching={p["metadata"]["name"].removeprefix(self.inventory["cluster_id"]+"-")for p in items for rule in p["spec"].get("egress",[])for peer in rule.get("to",[])
                  if peer.get("podSelector",{}).get("matchLabels",{}).get("app.kubernetes.io/name")=="service-db"and 5432 in[x["port"]for x in rule["ports"]]}
        self.assertEqual(reaching,{"auth","registry","consumer-dashboard","consumer-notifier","service-db","auth-db-init","registry-db-init","dashboard-db-init","notifier-db-init"})
    def test_unbounded_external_egress_rejected(self):
        with self.assertRaises(ValueError):render(self.inventory,[{"app":"ingestion","cidr":"0.0.0.0/0","ports":[443]}])
if __name__=="__main__":unittest.main()
