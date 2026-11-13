"""Deferred renderer checks; no host, Kubernetes, network or secret operations."""
import json
from pathlib import Path
import unittest

from storage_lib import durability_config, manifests, plan

ROOT = Path(__file__).resolve().parent.parent


class StoragePlacementChecks(unittest.TestCase):
    def setUp(self):
        self.inventory = json.loads((ROOT / "deployment/platform/inventory.example.json").read_text())
        # Fixture digest, never an executable image claim.
        self.image = "example.invalid/patroni@sha256:" + "a" * 64

    def test_four_worker_groups_never_put_the_complete_dataset_on_one_vm(self):
        storage = plan(self.inventory, "citus", self.image)
        groups_by_node = {n["name"]: set() for n in self.inventory["nodes"]}
        for group in storage["groups"][1:]:
            nodes = {m["node"] for m in group["members"]}
            self.assertEqual(len(nodes), 3)
            for node in nodes:
                groups_by_node[node].add(group["id"])
        self.assertTrue(all(len(groups) == 3 for groups in groups_by_node.values()))
        requests = sum(storage["requests_mib_before_routers"].values()) + storage["router_requests_mib"]
        self.assertEqual(requests, 8320)
        self.assertGreater(requests, sum(n["memory_mib"] for n in self.inventory["nodes"]))

    def test_local_disks_are_retained_and_bound_to_the_member_vm(self):
        storage = plan(self.inventory, "postgres", self.image)
        objects = manifests(storage)["items"]
        volumes = {o["metadata"]["name"]: o for o in objects if o["kind"] == "PersistentVolume"}
        for group in storage["groups"]:
            for member in group["members"]:
                volume = volumes[member["workload"]]["spec"]
                self.assertEqual(volume["persistentVolumeReclaimPolicy"], "Retain")
                self.assertEqual(volume["claimRef"]["name"], member["workload"])
                term = volume["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]
                self.assertEqual(term["values"], [member["node"]])
        self.assertFalse(any(o["kind"] == "Secret" for o in objects))

    def test_primary_routing_checks_patroni_leadership(self):
        storage = plan(self.inventory, "citus", self.image)
        objects = manifests(storage)["items"]
        proxy = next(o for o in objects if o["kind"] == "ConfigMap")["data"]["haproxy.cfg"]
        self.assertEqual(proxy.count("option httpchk GET /primary"), 5)
        self.assertEqual(proxy.count("on-marked-down shutdown-sessions"), 15)
        config = durability_config(storage)
        self.assertEqual([g["id"] for g in config["groups"]], list(range(5)))
        self.assertTrue(all(g["system_id"] == "RECORD_AFTER_BOOTSTRAP" for g in config["groups"]))
        self.assertNotIn("password", json.dumps(config).lower())

    def test_mutable_image_tags_are_rejected(self):
        with self.assertRaises(ValueError):
            plan(self.inventory, "postgres", "example.invalid/patroni:latest")


if __name__ == "__main__":
    unittest.main()
