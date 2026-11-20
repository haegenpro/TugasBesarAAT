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
    def test_broker_recovery_is_not_treated_as_a_dead_server(self):
        resources,_=render(self.inventory,self.catalog,self.images)
        brokers=[o for o in resources['items']if o['kind']=='StatefulSet'and o['metadata']['name'].startswith('broker-')]
        self.assertEqual(len(brokers),3)
        for obj in brokers:
            container=obj['spec']['template']['spec']['containers'][0]
            self.assertEqual(container['readinessProbe']['httpGet']['path'],'/healthz?js-server-only=true')
            self.assertEqual(container['livenessProbe']['httpGet']['path'],'/healthz?js-enabled-only=true')
            self.assertGreaterEqual(container['startupProbe']['periodSeconds']*container['startupProbe']['failureThreshold'],900)
            self.assertEqual(container['resources']['limits']['memory'],'1024Mi')

    def test_singleton_placement_rejects_unknown_nodes_and_unversioned_moves(self):
        target=self.inventory["nodes"][1]["name"]
        for placement in (
            {"bmkg-mock":{"node":"unknown","volume_suffix":"move1"}},
            {"bmkg-mock":{"node":target}},
            {"bmkg-mock":{"node":target,"volume_suffix":"../outside"}},
            {"broker":{"node":target,"volume_suffix":"move1"}},
            {"schema-registry":{"node":target,"volume_suffix":"move1"}},
        ):
            with self.subTest(placement=placement), self.assertRaises(ValueError):
                render(self.inventory,self.catalog,self.images,placement=placement)
    def test_registry_warmup_has_time_to_finish_within_its_memory_limit(self):
        resources,_=render(self.inventory,self.catalog,self.images)
        obj=next(o for o in resources['items']if o['kind']=='Deployment'and o['metadata']['name']=='schema-registry')
        container=obj['spec']['template']['spec']['containers'][0]
        self.assertEqual(container['startupProbe']['httpGet']['path'],'/health/live')
        self.assertGreaterEqual(container['startupProbe']['periodSeconds']*container['startupProbe']['failureThreshold'],900)
        self.assertEqual(container['resources']['limits']['memory'],'256Mi')
        bad=copy.deepcopy(self.catalog);bad["services"][0]["dependencies"]=["broker-init"]
        with self.assertRaises(ValueError):validate(bad)

if __name__=="__main__":unittest.main()
