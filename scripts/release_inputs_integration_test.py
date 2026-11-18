"""External input completion preserves credentials and never invents missing data."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from image_release_lib import REQUIRED
from platform_lib.inventory import load
from release_config_lib import prepare
import release_config_integration_test as scaffold_checks
from release_inputs_lib import complete

ROOT=Path(__file__).resolve().parent.parent


class InputCompletionChecks(unittest.TestCase):
    def test_supplied_inputs_resolve_generated_blockers_without_rotating_internal_keys(self):
        inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        with tempfile.TemporaryDirectory()as folder,patch("release_config_lib.storage_credentials",scaffold_checks.ReleaseConfigurationChecks.fake_storage):
            root=Path(folder);prepare(inventory,root/"original")
            original=json.loads((root/"original/secrets.json").read_text())
            token=root/"audit-token";token.write_text("instructor-private");token.chmod(0o600)
            satellite=root/"satellite";satellite.write_text("satellite-private");satellite.chmod(0o600)
            province=root/"province.json";province.write_text('[{"id":"ID-JB","name":"West Region","latitude":-6.9,"longitude":107.6}]')
            images=root/"images.json";images.write_text(json.dumps({k:"registry.invalid/"+k+"@sha256:"+"a"*64 for k in REQUIRED}))
            flows=root/"flows.json";flows.write_text('[{"app":"ingestion","cidr":"192.0.2.1/32","ports":[443]}]')
            source={"bmkg_namespace":"chosen-bmkg","bmkg_initial_offset":1,"satellite_namespace":"chosen-satellite","satellite_initial_offset":0}
            inputs={"version":1,"audit_token_file":"audit-token","satellite_token_file":"satellite","province_reference":"province.json","images":"images.json","external_flows":"flows.json","source_identity":source}
            (root/"inputs.json").write_text(json.dumps(inputs))
            report=complete(inventory,root/"original/release.json",root/"inputs.json",root/"completed")
            self.assertTrue(report["external_inputs_complete"])
            changed=json.loads((root/"completed/secrets.json").read_text())
            for before,after in zip(original["items"],changed["items"]):
                for key,value in before.get("stringData",{}).items():self.assertEqual(after["stringData"][key],value)
            self.assertNotIn("instructor-private",json.dumps(report));self.assertEqual((root/"completed/provinces.json").read_bytes(),province.read_bytes())
            self.assertEqual(json.loads((root/"completed/wildfire.json").read_text())["namespace"],source["satellite_namespace"])
            self.assertEqual(json.loads((root/"completed/release.json").read_text())["unresolved"],[])
            with self.assertRaisesRegex(ValueError,"overwrite"):complete(inventory,root/"original/release.json",root/"inputs.json",root/"completed")

    def test_absent_external_values_remain_pending_and_untracked(self):
        inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        with tempfile.TemporaryDirectory()as folder,patch("release_config_lib.storage_credentials",scaffold_checks.ReleaseConfigurationChecks.fake_storage):
            root=Path(folder);prepare(inventory,root/"original");(root/"inputs.json").write_text('{"version":1}')
            report=complete(inventory,root/"original/release.json",root/"inputs.json",root/"completed")
            self.assertFalse(report["external_inputs_complete"]);self.assertTrue(any("AUDIT_TOKEN"in p for p in report["pending"]))
            self.assertFalse((root/"completed/provinces.json").exists())


if __name__=="__main__":unittest.main()
