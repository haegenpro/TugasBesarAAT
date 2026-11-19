"""Offline checks: all-volume coverage, retained disks and unsafe restore refusal."""
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import tarfile

from platform_lib.host_agent import HostAgent
from platform_lib.inventory import load,resource_uuid
from replacement_lib import load_plan,validate_archive
from service_lib import render as services
from storage_lib import FORMATIONS,plan,manifests,durability_config

ROOT=Path(__file__).resolve().parent.parent

class ReplacementIntegrationChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        for node in self.inventory["nodes"]:node["memory_mib"]=32768
        self.image="registry.invalid/fixture@sha256:"+"a"*64

    def test_all_formations_are_included_and_unbacked_source_state_blocks(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);items=[];formation_inputs=[]
            for ordinal,suffix in enumerate(FORMATIONS):
                storage=plan(self.inventory,"citus"if suffix=="storage"else"postgres",self.image,suffix)
                config=durability_config(storage)
                for i,group in enumerate(config["groups"]):group["system_id"]=str(1000+ordinal*10+i)
                for label,value in ((suffix+"-plan",storage),(suffix+"-config",config)):(root/(label+".json")).write_text(json.dumps(value))
                formation_inputs.append({"plan":suffix+"-plan.json","config":suffix+"-config.json"})
                items += [o for o in manifests(storage)["items"]if o["kind"]!="Namespace"]
            catalog=json.loads((ROOT/"deployment/services/catalog.json").read_text());images={s["image_key"]:self.image for s in catalog["services"]if "image_key"in s}
            resource,requirements=services(self.inventory,catalog,images);items+=resource["items"]
            (root/"services-requirements.json").write_text(json.dumps(requirements))
            (root/"resources.json").write_text(json.dumps({"kind":"List","items":items}))
            (root/"bundle.json").write_text(json.dumps({"version":1,"formations":[self.inventory["cluster_id"]+"-"+s for s in FORMATIONS],"node_reserve_mib":512,"stages":[{"id":"all","manifest":"resources.json"}]}))
            (root/"replacement.json").write_text(json.dumps({"version":1,"bundle":"bundle.json","formations":formation_inputs,"volume_requirements":["services-requirements.json"],"volume_policy":{}}))
            value=load_plan(self.inventory,root/"replacement.json",self.inventory["nodes"][0]["name"])
            self.assertFalse(value["ready"])
            self.assertEqual(len(value["formations"]),3)
            self.assertTrue(any("bmkg"in problem for problem in value["blocked"]))
            self.assertTrue(any(v["kind"]=="postgres"and "ingestion"in v["claim"]for v in value["volumes"]))

    def test_provider_quarantines_both_disks_once_and_never_starts_vm(self):
        with tempfile.TemporaryDirectory()as folder:
            agent=HostAgent(self.inventory,"host-b",root=Path(folder)/"host")
            agent.init();node=self.inventory["nodes"][1];name=node["name"]
            expected={**agent.owner,"node":name,"uuid":resource_uuid(self.inventory,name)}
            agent._json("intents/"+name+".json",expected);agent._json("intents/"+name+".complete.json",expected)
            for kind in ("root","data"):agent.path(f"disks/{name}-{kind}.vdi").write_bytes(("original-"+kind).encode())
            calls=[]
            def vbox(*args,**kwargs):
                from types import SimpleNamespace
                calls.append(args);return SimpleNamespace(returncode=0)
            def create(_name):
                for kind in ("root","data"):agent.path(f"disks/{name}-{kind}.vdi").write_bytes(("fresh-"+kind).encode())
            with patch.object(agent,"status",return_value={"state":"shut off"}),patch.object(agent,"_vbox",side_effect=vbox),patch.object(agent,"create",side_effect=create)as created:
                result=agent.replace_empty(name,"a"*32);agent.replace_empty(name,"a"*32)
                self.assertEqual(created.call_count,1)
            quarantine=Path(result["quarantine"])
            for kind in ("root","data"):self.assertEqual((quarantine/f"{name}-{kind}.vdi").read_bytes(),("original-"+kind).encode())
            self.assertFalse(any("--delete"in args or "startvm"in args for args in calls))
            with patch.object(agent,"status",return_value={"state":"running"}),self.assertRaises(ValueError):agent.replace_empty(name,"b"*32)
            self.assertEqual(agent.replacement_status(name,'a'*32)['phase'],'complete')
            replacement=agent.path('disks/changed.tmp');replacement.write_bytes(b'foreign disk')
            replacement.replace(agent.path(f'disks/{name}-data.vdi'))
            with self.assertRaisesRegex(ValueError,'disks changed'):agent.replacement_status(name,'a'*32)

    def test_restore_rejects_traversal_and_symlink_before_node_mutation(self):
        for unsafe in ("../escape","/absolute","symlink"):
            with tempfile.TemporaryDirectory()as folder:
                path=Path(folder)/"backup.tar"
                with tarfile.open(path,"w")as archive:
                    entry=tarfile.TarInfo(unsafe)
                    if unsafe=="symlink":entry.type=tarfile.SYMTYPE;entry.linkname="../escape";archive.addfile(entry)
                    else:entry.size=1;archive.addfile(entry,io.BytesIO(b"x"))
                with self.assertRaises(ValueError):validate_archive(path)

if __name__=="__main__":unittest.main()
