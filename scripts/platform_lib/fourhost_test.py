"""Offline checks for one owned VM per Windows/Linux physical machine."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from platform_lib.inventory import load,validate
from platform_lib.multihost import MultiHostCluster

ROOT=Path(__file__).resolve().parents[2]

class FourHostChecks(unittest.TestCase):
    def setUp(self):self.cfg=load(ROOT/"deployment/platform/inventory.fourhost.example.json")

    def test_four_independent_hosts_with_remote_linux_and_windows(self):
        self.assertEqual(len({n["physical_host"]for n in self.cfg["nodes"]}),4)
        cluster=MultiHostCluster(self.cfg)
        self.assertEqual(cluster.clients["host-c"].host["provider"],"libvirt")
        self.assertEqual(cluster.clients["host-c"].host["transport"],"ssh")
        self.assertEqual(cluster.clients["host-b"].host["provider"],"virtualbox")

    def test_colocation_unknown_host_and_remote_local_transport_are_rejected(self):
        for change in (lambda c:c["nodes"][1].update(physical_host="host-a"),lambda c:c["nodes"][1].update(physical_host="unknown"),lambda c:c["hosts"][2].update(transport="local")):
            cfg=copy.deepcopy(self.cfg);change(cfg)
            with self.assertRaises(ValueError):validate(cfg)

    def test_doctor_requires_all_four_distinct_machine_identities(self):
        cluster=MultiHostCluster(self.cfg)
        def call(client,operation,**kwargs):return {"ready":True,"machine_sha256":client.host["id"]}
        with patch("platform_lib.multihost.platform.system",return_value="Linux"),patch("platform_lib.multihost.shutil.which",return_value="/tool"),patch("platform_lib.multihost.HostClient.call",autospec=True,side_effect=call),patch.object(cluster,"docker")as docker:
            docker.return_value.returncode=0
            self.assertTrue(cluster.doctor()["ready"])
            with patch("platform_lib.multihost.HostClient.call",return_value={"ready":True,"machine_sha256":"same-machine"}):
                self.assertFalse(cluster.doctor()["ready"])

if __name__=="__main__":unittest.main()
