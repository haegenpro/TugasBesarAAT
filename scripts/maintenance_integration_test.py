"""Maintenance sequencing, refusal and receipt binding with a simulated API."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from infrastructure_release_lib import assemble
from maintenance_lib import inputs,run,verify_receipt,expected_schemas
from platform_lib.inventory import load

ROOT=Path(__file__).resolve().parent.parent


class API:
    def __init__(self,inventory,formations,owner,fail=None):
        self.cfg=inventory;self.owner=owner;self.fail=fail;self.objects={};self.calls=[];self.members={}
        identity=100
        for storage in formations.values():
            for group in storage["groups"]:
                identity+=1
                for i,member in enumerate(group["members"]):
                    self.members[member["name"]]={"formation":storage["formation"],"group_id":group["id"],"member":member["name"],"system_id":str(identity),"primary":i==0}

    def kubectl(self,*args,**kw):
        self.calls.append(args);value={};code=0
        if args[0]=="get"and args[1]in("deployments,statefulsets","pods"):
            value={"items":[]}
            if self.fail=="live-writer"and args[1]=="deployments,statefulsets":
                value["items"]=[{"metadata":{"name":"aggregator"},"spec":{"replicas":1,"template":{"metadata":{"labels":{"app.kubernetes.io/name":"aggregator"}}}}}]
        elif args[0]=="exec":
            if "platform_replication_status"in args[-1]:value=self.members[args[3]]
            elif args[-1]=="-":value={}
            elif "canonical_schema_migrations"in args[-1]:value=expected_schemas()["canonical"]
            elif "ingestion_migrations"in args[-1]:value=expected_schemas()["ingestion"]
            elif "service_schema_migrations"in args[-1]:
                formation=self.members[args[3]]["formation"];suffix=next(s for s in expected_schemas()["owners"]if formation.endswith("-"+s));value=expected_schemas()["owners"][suffix]
            elif "input_cutovers"in args[-1]:value={"namespace":self.owner["bmkg_namespace"],"state":"ready"}
            else:raise AssertionError(args)
        elif args[0]=="create":
            value=json.loads(kw["input"]);value["metadata"]["uid"]="uid-"+value["metadata"]["name"];self.objects[value["metadata"]["name"]]=value
        elif args[0]=="wait":
            if self.fail=="ingestion-job"and "maintenance-ingestion-"in args[2]:raise RuntimeError("injected Job failure")
        elif args[0]=="get"and args[1]in("job","pod"):
            value=copy.deepcopy(self.objects[args[2]])
            if args[1]=="job":value["status"]={"conditions":[{"type":"Complete","status":"True"}]}
        elif args[0]=="logs":return subprocess.CompletedProcess(args,0,b"bounded tool result\n",b"")
        elif args[0]=="delete":pass
        else:raise AssertionError(args)
        return subprocess.CompletedProcess(args,code,json.dumps(value).encode(),b"")


class MaintenanceChecks(unittest.TestCase):
    def fixture(self,root):
        inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        images={k:"registry.invalid/"+k+"@sha256:"+"a"*64 for k in("storage","role1","schema-registry")}
        config={"version":1,"node_reserve_mib":512,"unresolved":[]}
        for name,value in(("images",images),("aggregator",json.loads((ROOT/"deployment/aggregator/config.integrated.example.json").read_text())),("role1",json.loads((ROOT/"deployment/role1/config.integrated.example.json").read_text()))):
            config[name]=name+".json";(root/config[name]).write_text(json.dumps(value))
        (root/"release.json").write_text(json.dumps(config));assemble(inventory,root/"release.json",root/"infra")
        values=inputs(inventory,root/"release.json",root/"infra")
        return inventory,values

    def test_completed_sequence_binds_source_images_schemas_and_all_original_identities(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);inventory,v=self.fixture(root);api=API(inventory,v[6],v[4])
            receipt=run(api,root/"release.json",root/"infra",root/"maintenance")
            self.assertTrue(receipt["passed"]);self.assertTrue((root/"maintenance/release-ready.json").is_file())
            self.assertEqual([s["action"]for s in receipt["steps"]][-3:],["canonical","ingestion","cutover"])
            self.assertEqual(len(receipt["durability"]),6)
            verify_receipt(receipt,inventory,v[3],v[4],v[2])
            for mutation in("image","schema","namespace","identity"):
                bad=copy.deepcopy(receipt);images=copy.deepcopy(v[2]);owner=copy.deepcopy(v[4])
                if mutation=="image":images["role1"]="registry.invalid/other@sha256:"+"b"*64
                if mutation=="schema":bad["schemas"]["ingestion"]["0001_ingestion.sql"]="changed"
                if mutation=="namespace":owner["bmkg_namespace"]="different-history"
                if mutation=="identity":bad["durability"]["ingestion"]["groups"][0]["system_id"]=bad["durability"]["storage"]["groups"][0]["system_id"]
                with self.assertRaises(ValueError):verify_receipt(bad,inventory,v[3],owner,images)
            self.assertFalse(any(call[0]in("scale","set")for call in api.calls))

    def test_live_writers_or_failed_job_cannot_produce_activation_configuration(self):
        for fault in("live-writer","ingestion-job"):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory()as folder:
                root=Path(folder);inventory,v=self.fixture(root);api=API(inventory,v[6],v[4],fault)
                with self.assertRaises((ValueError,RuntimeError)):run(api,root/"release.json",root/"infra",root/"maintenance")
                self.assertFalse((root/"maintenance/release-ready.json").exists())
                self.assertFalse(json.loads((root/"maintenance/receipt.json").read_text())["passed"])
                if fault=="live-writer":self.assertFalse(any(call[0]=="create"for call in api.calls))

    def test_bound_receipt_removes_initialization_blockers_from_full_release(self):
        from image_release_lib import REQUIRED
        from release_lib import assemble as full_release
        import hashlib
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);inventory,v=self.fixture(root)
            for node in inventory["nodes"]:node["memory_mib"]=8192
            # Re-render with this declared test allocation before capturing it.
            images={k:"registry.invalid/"+k+"@sha256:"+"a"*64 for k in REQUIRED}
            (root/"images.json").write_text(json.dumps(images))
            config=json.loads((root/"release.json").read_text())
            owner=json.loads((root/"role1.json").read_text())
            province=(ROOT/"docs/fixtures/wildfire-m5/provinces.json").read_bytes()
            processor=json.loads((ROOT/"deployment/role1/wildfire.integrated.example.json").read_text())
            processor["province_sha256"]=hashlib.sha256(province).hexdigest()
            (root/"province.json").write_bytes(province);(root/"wildfire.json").write_text(json.dumps(processor))
            (root/"services.json").write_text((ROOT/"deployment/services/catalog.json").read_text())
            (root/"flows.json").write_text('[{"app":"ingestion","cidr":"192.0.2.1/32","ports":[443]}]')
            config.update(services="services.json",province_reference="province.json",wildfire_config="wildfire.json",external_flows="flows.json")
            (root/"release.json").write_text(json.dumps(config));assemble(inventory,root/"release.json",root/"infra-complete")
            values=inputs(inventory,root/"release.json",root/"infra-complete");api=API(inventory,values[6],owner)
            run(api,root/"release.json",root/"infra-complete",root/"maintenance")
            report=full_release(inventory,root/"maintenance/release-ready.json",root/"full")
            self.assertTrue(report["ready_for_apply"],report["blocked"])
            changed=json.loads((root/"maintenance/storage-durability.json").read_text());changed["groups"][0]["system_id"]="999999"
            (root/"maintenance/storage-durability.json").write_text(json.dumps(changed))
            with self.assertRaisesRegex(ValueError,"original maintenance capture"):
                full_release(inventory,root/"maintenance/release-ready.json",root/"changed")


if __name__=="__main__":unittest.main()
