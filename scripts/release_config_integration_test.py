"""Cross-service credential wiring and fail-closed private release preparation."""
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from platform_lib.inventory import load
from release_config_lib import DEFAULT_PROVINCE_REFERENCE,prepare,province_bytes
from storage_lib import write_json

ROOT=Path(__file__).resolve().parent.parent


class ReleaseConfigurationChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")

    @staticmethod
    def fake_storage(layout,folder):
        folder.mkdir(parents=True,mode=0o700)
        write_json(folder/"secrets.json",{"items":[]})
        if layout.get("formation_suffix")=="service-db":
            for entry in layout["databases"]:
                values={"DATABASE_URL":"postgresql://"+entry["runtime_role"]+"@"+entry["service"]+"/"+entry["database"]}
                if entry["service"]=="registry-db":values.update(APICURIO_DATASOURCE_USERNAME="registry_app",APICURIO_DATASOURCE_PASSWORD="registry-private")
                (folder/(entry["service"]+".env")).write_text("\n".join(k+"="+v for k,v in values.items()))
            return
        values={g["dsn_env"]:"postgresql://"+layout["formation"]+"/"+str(g["id"])for g in layout["groups"]}
        values["INGESTION_DATABASE_URL"if layout.get("formation_suffix")=="ingestion"else "DATABASE_URL"]="postgresql://"+layout["formation"]
        (folder/"application.env").write_text("\n".join(k+"="+v for k,v in values.items()))

    def test_bundled_reference_and_pending_credentials(self):
        with tempfile.TemporaryDirectory()as folder,patch("release_config_lib.storage_credentials",self.fake_storage):
            output=Path(folder)/"release"
            token=Path(folder)/"satellite";token.write_text("not-a-live-token");token.chmod(0o600)
            report=prepare(self.inventory,output,satellite_token_file=token)
            objects=json.loads((output/"secrets.json").read_text())["items"]
            secrets={o["metadata"]["name"]:o["stringData"]for o in objects}
            role=secrets[self.inventory["cluster_id"]+"-role1-runtime"];agg=secrets[self.inventory["cluster_id"]+"-aggregator-runtime"]
            self.assertNotIn("AUDIT_TOKEN",role)
            raw=(output/"provinces.json").read_bytes()
            self.assertEqual(raw,DEFAULT_PROVINCE_REFERENCE.read_bytes())
            self.assertEqual(len(json.loads(raw)),38)
            processor=json.loads((output/"wildfire.json").read_text())
            self.assertEqual(processor["province_file"],"provinces.json")
            self.assertEqual(processor["province_sha256"],hashlib.sha256(raw).hexdigest())
            self.assertFalse(any("province"in item for item in report["pending"]))
            self.assertEqual(role["SATELLITE_FEED_TOKEN"],"not-a-live-token")
            self.assertEqual(role["BMKG_API_KEY"],secrets["bmkg-runtime"]["BMKG_API_KEY"])
            self.assertEqual(agg["PVMBG_TOKEN"],secrets["pvmbg-runtime"]["PVMBG_TOKEN"])
            self.assertEqual(secrets["auth-runtime"]["AUTH_PRIORITY_KEY"],secrets["client-api-runtime"]["AUTH_PRIORITY_KEY"])
            self.assertEqual(role["FIELD_READ_TOKEN"],secrets["client-api-runtime"]["FIELD_READ_TOKEN"])
            self.assertEqual(agg["NATS_URL"],secrets["broker-init-runtime"]["NATS_URL"])
            self.assertNotIn("NATS_SYSTEM_URL",role)
            self.assertNotEqual(secrets["broker-runtime"]["NATS_URL"],secrets["broker-runtime"]["NATS_SYSTEM_URL"])
            self.assertEqual(secrets["registry-runtime"],{"APICURIO_DATASOURCE_USERNAME":"registry_app","APICURIO_DATASOURCE_PASSWORD":"registry-private"})
            # Each owner Secret carries only its own database login.
            for name,role,database in (("auth-runtime","auth_app","auth"),("dashboard-runtime","dashboard_app","dashboard"),("notifier-runtime","notifier_app","notifier")):
                self.assertTrue(secrets[name]["DATABASE_URL"].startswith("postgresql://"+role+"@"))
                self.assertTrue(secrets[name]["DATABASE_URL"].endswith("/"+database))
            self.assertNotIn("not-a-live-token",json.dumps(report))
            self.assertTrue(any("AUDIT_TOKEN"in item for item in report["pending"]))
            for path in output.rglob("*"):self.assertEqual(path.stat().st_mode&0o777,0o700 if path.is_dir()else 0o600)
            before=(output/"secrets.json").read_bytes()
            with self.assertRaisesRegex(ValueError,"rotation"):prepare(self.inventory,output)
            self.assertEqual((output/"secrets.json").read_bytes(),before)

    def test_explicit_reference_overrides_bundled_table(self):
        with tempfile.TemporaryDirectory()as folder,patch("release_config_lib.storage_credentials",self.fake_storage):
            root=Path(folder);province=root/"custom.json"
            raw=b'[{"id":"chosen","name":"Chosen Region","latitude":-6.9,"longitude":107.6}]\n'
            province.write_bytes(raw)
            prepare(self.inventory,root/"release",province_reference=province)
            self.assertEqual((root/"release/provinces.json").read_bytes(),raw)
            processor=json.loads((root/"release/wildfire.json").read_text())
            self.assertEqual(processor["province_sha256"],hashlib.sha256(raw).hexdigest())

    def test_fixture_reference_and_public_token_file_are_refused_before_generation(self):
        with tempfile.TemporaryDirectory()as folder,patch("release_config_lib.storage_credentials")as generate:
            root=Path(folder);token=root/"token";token.write_text("private");token.chmod(0o644)
            with self.assertRaisesRegex(ValueError,"private permissions"):prepare(self.inventory,root/"out",satellite_token_file=token)
            with self.assertRaisesRegex(ValueError,"fixture"):
                province_bytes(ROOT/"docs/fixtures/wildfire-m5/provinces.json")
            generate.assert_not_called();self.assertFalse((root/"out").exists())


if __name__=="__main__":unittest.main()
