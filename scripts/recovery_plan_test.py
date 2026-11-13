"""Deferred offline recovery-plan checks; no VM/data operations."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest
from storage_lib import plan,durability_config

ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location("recovery",ROOT/"scripts/platform-recovery.py")
recovery=importlib.util.module_from_spec(spec);spec.loader.exec_module(recovery)

class RecoveryPlanChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=json.loads((ROOT/"deployment/platform/inventory.example.json").read_text())
        self.storage=plan(self.inventory,"citus","example.invalid/pg@sha256:"+"a"*64)
    def test_replacement_uses_only_original_missing_copies(self):
        for node in self.inventory["nodes"]:
            result=recovery.recovery_plan(self.inventory,self.storage,node["name"])
            workers=[g for g in result["groups"]if g["id"]>0]
            self.assertEqual(len(workers),3)
            for group in result["groups"]:
                self.assertEqual(group["replacement"]["node"],node["name"])
                self.assertEqual(len({m["node"]for m in group["survivors"]}),2)
    def test_original_system_ids_are_required(self):
        config=durability_config(self.storage)
        with self.assertRaises(ValueError):recovery.validate_config(self.storage,config)
        for i,g in enumerate(config["groups"]):g["system_id"]=str(i+100)
        recovery.validate_config(self.storage,config)
        bad=copy.deepcopy(config);bad["groups"][1]["system_id"]=bad["groups"][0]["system_id"]
        with self.assertRaises(ValueError):recovery.validate_config(self.storage,bad)
        with self.assertRaises(ValueError):recovery.recovery_plan(self.inventory,self.storage,"unowned-node")

if __name__=="__main__":unittest.main()
