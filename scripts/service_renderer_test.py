"""Deferred manifest contract checks; no deployment or image execution."""
import copy
import json
from pathlib import Path
import unittest
from service_lib import render,validate

ROOT=Path(__file__).resolve().parent.parent

class ServiceContractChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=json.loads((ROOT/"deployment/platform/inventory.example.json").read_text())
        self.catalog=json.loads((ROOT/"deployment/services/catalog.json").read_text())
        self.images={s["image_key"]:"example.invalid/image@sha256:"+"a"*64 for s in self.catalog["services"] if "image_key"in s}
    def test_broker_copies_have_distinct_retained_local_disks(self):
        resources,report=render(self.inventory,self.catalog,self.images)
        broker=[v for v in report["local_volumes"] if "broker-"in v["claim"]]
        self.assertEqual(len(broker),3);self.assertEqual(len({v["node"]for v in broker}),3)
        self.assertEqual(len({v["path"]for v in broker}),3)
        pvs=[r for r in resources["items"]if r["kind"]=="PersistentVolume"]
        self.assertTrue(all(r["spec"]["persistentVolumeReclaimPolicy"]=="Retain"for r in pvs))
        self.assertIn("auth-db",report["external_dependencies"])
    def test_missing_pin_or_inline_secret_or_cycle_fails(self):
        bad=copy.deepcopy(self.images);bad["broker"]="nats:latest"
        with self.assertRaises(ValueError):render(self.inventory,self.catalog,bad)
        bad=copy.deepcopy(self.catalog);bad["services"][0]["environment"]["PASSWORD"]="secret"
        with self.assertRaises(ValueError):validate(bad)
        bad=copy.deepcopy(self.catalog);bad["services"][0]["dependencies"]=["broker-init"]
        with self.assertRaises(ValueError):validate(bad)

if __name__=="__main__":unittest.main()
