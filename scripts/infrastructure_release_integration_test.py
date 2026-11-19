"""Bootstrap must remain independently preparable while external inputs are pending."""
import json
from pathlib import Path
import tempfile
import unittest

from infrastructure_release_lib import assemble
from platform_lib.inventory import load

ROOT=Path(__file__).resolve().parent.parent


class InfrastructureReleaseChecks(unittest.TestCase):
    def test_storage_only_uses_no_application_image_or_province_input(self):
        inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        for node in inventory["nodes"]:node["memory_mib"]=8192
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)
            (root/"images.json").write_text(json.dumps({"storage":"registry.invalid/patroni@sha256:"+"a"*64,"role1":None}))
            config={"version":1,"images":"images.json","secrets":"secrets.json","unresolved":["Audit token pending","province reference pending"]}
            (root/"secrets.json").write_text(json.dumps({"kind":"List","items":[]}))
            (root/"release.json").write_text(json.dumps(config))
            report=assemble(inventory,root/"release.json",root/"prepared")
            self.assertTrue(report["ready_for_apply"]);self.assertEqual(report["phase"],"infrastructure")
            self.assertFalse(report["ready_for_activation"])
            self.assertIn("Audit token pending",report["activation_pending"])
            self.assertEqual(len(report["formations"]),3)
            self.assertEqual(report["memory_requests_mib"],12672)
            resources=[o for stage in report["stages"]for o in json.loads(Path(stage["manifest"]).read_text())["items"]]
            self.assertEqual(sum(o["kind"]=="StatefulSet"and o["spec"]["template"]["metadata"]["labels"].get("component")=="storage-postgres"for o in resources),21)
            self.assertFalse(any(o["kind"]=="Job"for o in resources))
            self.assertTrue(all(w["name"].endswith("-router")or w["kind"]=="StatefulSet"for w in report["workloads"]))
            self.assertEqual(sum(o["kind"]=="Namespace"for o in resources),1)
            policies=[o for o in resources if o["kind"]=="NetworkPolicy"]
            self.assertTrue(any(o["metadata"]["name"].endswith("bootstrap-ingestion-etcd")for o in policies))
            self.assertTrue((root/"prepared/canonical-plan.json").is_file());self.assertTrue((root/"prepared/service-db-plan.json").is_file())
            config["infrastructure_unresolved"]=["operator disk prerequisite pending"]
            (root/"release.json").write_text(json.dumps(config))
            self.assertFalse(assemble(inventory,root/"release.json",root/"blocked")["ready_for_apply"])


if __name__=="__main__":unittest.main()
