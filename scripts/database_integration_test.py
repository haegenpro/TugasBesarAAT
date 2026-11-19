"""Shared owned-database formation: isolation, capacity and release assembly checks."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from bundle_lib import inspect
from database_lib import check_isolation, render, schema_sql
from monitoring_lib import IMAGES
from platform_lib.inventory import load
from release_lib import assemble
from storage_lib import SERVICE_CONNECTION_LIMITS, SERVICE_DATABASES, SERVICE_FORMATION, SERVICE_MAX_CONNECTIONS, SERVICE_OWNERS

ROOT=Path(__file__).resolve().parent.parent


def load_image_module(name):
    """Import a Patroni image script without its runtime-only dependencies."""
    stubs={"psycopg2":types.ModuleType("psycopg2"),"psycopg2.sql":types.ModuleType("psycopg2.sql"),"psycopg2.extensions":types.ModuleType("psycopg2.extensions")}
    stubs["psycopg2"].sql=stubs["psycopg2.sql"];stubs["psycopg2.extensions"].parse_dsn=dict
    with patch.dict(sys.modules,stubs):
        spec=importlib.util.spec_from_file_location("image_"+name,ROOT/"deployment/storage"/(name+".py"))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def hba_view(rules):
    """The pg_hba_file_rules shape PostgreSQL reports for the rendered lines."""
    view=[]
    for line in rules:
        fields=line.split();kind=fields[0]
        view.append({"type":kind,"database":[fields[1]],"user":[fields[2]],"method":fields[-1],"error":None})
    return view


class DatabaseIntegrationChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        self.image="registry.invalid/fixture@sha256:"+"a"*64
        self.entrypoint=load_image_module("entrypoint")

    def shared(self):
        resources,report=render(self.inventory,self.image)
        self.assertEqual(len(report["formations"]),1)
        return resources,report,report["formations"][0]

    def observed(self,storage):
        """A correct live answer from one member, built from the rendered pg_hba."""
        owners=storage["databases"]
        with patch.dict(os.environ,{"OWNED_DATABASES":next(e["value"]for e in self.pod_env(storage)if e["name"]=="OWNED_DATABASES")}):
            rules=self.entrypoint.access_rules(storage["pod_cidr"])
        return {"connect":{a["runtime_role"]+"@"+b["database"]:a is b for a in owners for b in owners},
                "admin_connect":{d["runtime_role"]+"@"+admin:False for d in owners for admin in("postgres","template1")},
                "public_connect":[],
                "roles":{d["runtime_role"]:{"login":True,"superuser":False,"createdb":False,"createrole":False,"replication":False,"bypassrls":False,"inherit":False,"connection_limit":d["connection_limit"]}for d in owners},
                "memberships":0,"max_connections":storage["max_connections"],"hba":hba_view(rules)}

    def pod_env(self,storage):
        resources,_=render(self.inventory,self.image)
        pod=next(o for o in resources["items"]if o["kind"]=="StatefulSet"and o["spec"]["template"]["metadata"]["labels"].get("component")=="storage-postgres")
        return pod["spec"]["template"]["spec"]["containers"][0]["env"]

    def test_one_formation_hosts_four_owned_databases_on_three_nodes(self):
        resources,report,storage=self.shared()
        # 3 PostgreSQL x 512 + 3 etcd x 128 + routers 256, instead of four formations (8704).
        self.assertEqual(report["memory_requests_mib"],2176)
        self.assertEqual(storage["formation"],self.inventory["cluster_id"]+"-"+SERVICE_FORMATION)
        self.assertEqual(storage["database"],"postgres")
        nodes=[m["node"]for m in storage["groups"][0]["members"]]
        names=[n["name"]for n in self.inventory["nodes"]]
        self.assertEqual(nodes,[names[0],names[1],names[3]])  # keep the shared copies off C
        self.assertEqual({d["service"]:(d["database"],d["runtime_role"])for d in storage["databases"]},SERVICE_DATABASES)
        self.assertLess(sum(SERVICE_CONNECTION_LIMITS.values()),SERVICE_MAX_CONNECTIONS)
        router=[o for o in resources["items"]if o["kind"]=="Deployment"]
        self.assertEqual(len(router),1)
        for service in SERVICE_DATABASES:
            alias=next(o for o in resources["items"]if o["kind"]=="Service"and o["metadata"]["name"]==service)
            self.assertTrue(all(router[0]["spec"]["template"]["metadata"]["labels"].get(k)==v for k,v in alias["spec"]["selector"].items()))
            self.assertIn("REVOKE CREATE ON SCHEMA public",schema_sql(service))
        env={e["name"]:e for e in self.pod_env(storage)}
        self.assertNotIn("APP_USER",env);self.assertNotIn("APP_PASSWORD",env)
        self.assertEqual(env["MAX_CONNECTIONS"]["value"],str(SERVICE_MAX_CONNECTIONS))
        for entry in storage["databases"]:
            self.assertEqual(env[entry["password_key"]]["valueFrom"]["secretKeyRef"]["key"],entry["password_key"])
        policies={o["metadata"]["name"]:o for o in resources["items"]if o["kind"]=="NetworkPolicy"}
        allowed={peer["podSelector"]["matchLabels"]["app.kubernetes.io/name"]for rule in policies[self.inventory["cluster_id"]+"-owned-service-db-router"]["spec"]["ingress"]for peer in rule["from"]}
        self.assertEqual(allowed,set(SERVICE_OWNERS.values())|{s+"-init"for s in SERVICE_DATABASES})
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)
            (root/"resources.json").write_text(json.dumps(resources))
            (root/"bundle.json").write_text(json.dumps({"version":1,"formations":[storage["formation"]],"node_reserve_mib":512,"stages":[{"id":"owners","manifest":"resources.json"}]}))
            budget,_=inspect(root/"bundle.json",self.inventory)
            self.assertEqual(budget["memory_requests_mib"],2176)

    def test_rendered_hba_admits_each_owner_only_to_its_own_database(self):
        _,_,storage=self.shared()
        observed=self.observed(storage)
        extensions={d["database"]:[]for d in storage["databases"]}
        self.assertTrue(check_isolation(storage,observed,extensions)["passed"])
        rules=observed["hba"]
        self.assertEqual(rules[-1],{"type":"host","database":["all"],"user":["all"],"method":"reject","error":None})
        self.assertFalse(any(r["database"]==["all"]and r["user"]==["all"]and r["method"]!="reject"for r in rules))
        # The single-database formations keep their original pg_hba unchanged.
        with patch.dict(os.environ,{},clear=False):
            os.environ.pop("OWNED_DATABASES",None)
            self.assertIn("host all all 10.42.0.0/16 scram-sha-256",self.entrypoint.access_rules("10.42.0.0/16"))

    def test_every_broken_isolation_layer_is_refused(self):
        _,_,storage=self.shared()
        good=self.observed(storage);extensions={d["database"]:[]for d in storage["databases"]}
        def mutate(change):
            value=copy.deepcopy(good);change(value);return value
        cases={
            "cross-connect":mutate(lambda v:v["connect"].__setitem__("auth_app@notifier",True)),
            "admin-db":mutate(lambda v:v["admin_connect"].__setitem__("auth_app@postgres",True)),
            "public":mutate(lambda v:v.__setitem__("public_connect",["auth"])),
            "superuser":mutate(lambda v:v["roles"]["dashboard_app"].__setitem__("superuser",True)),
            "createrole":mutate(lambda v:v["roles"]["registry_app"].__setitem__("createrole",True)),
            "inherit":mutate(lambda v:v["roles"]["auth_app"].__setitem__("inherit",True)),
            "limit":mutate(lambda v:v["roles"]["notifier_app"].__setitem__("connection_limit",-1)),
            "membership":mutate(lambda v:v.__setitem__("memberships",1)),
            "max-connections":mutate(lambda v:v.__setitem__("max_connections",64)),
            "hba-all":mutate(lambda v:v["hba"].insert(0,{"type":"host","database":["all"],"user":["all"],"method":"scram-sha-256","error":None})),
            "hba-cross":mutate(lambda v:v["hba"].insert(0,{"type":"host","database":["notifier"],"user":["auth_app"],"method":"scram-sha-256","error":None})),
            "hba-error":mutate(lambda v:v["hba"][0].__setitem__("error","invalid")),
            "hba-no-final-reject":mutate(lambda v:v["hba"].pop()),
        }
        for name,observed in cases.items():
            with self.subTest(name=name),self.assertRaises(ValueError):check_isolation(storage,observed,extensions)
        with self.assertRaises(ValueError):check_isolation(storage,good,{**extensions,"auth":["dblink"]})
        with self.assertRaises(ValueError):check_isolation(storage,good,{"auth":[]})

    def test_bootstrap_declaration_is_validated(self):
        bootstrap=load_image_module("bootstrap")
        _,_,storage=self.shared()
        valid=next(e["value"]for e in self.pod_env(storage)if e["name"]=="OWNED_DATABASES")
        with patch.dict(os.environ,{"OWNED_DATABASES":valid}):
            self.assertEqual([e["database"]for e in bootstrap.owned_databases()],["auth","registry","dashboard","notifier"])
        entries=json.loads(valid)
        for bad in ([*entries,entries[0]],[{**entries[0],"database":"postgres"}],[{**entries[0],"connection_limit":0}]):
            with self.subTest(bad=bad),patch.dict(os.environ,{"OWNED_DATABASES":json.dumps(bad)}),self.assertRaises(RuntimeError):bootstrap.owned_databases()
        with patch.dict(os.environ,{"OWNED_DATABASES":json.dumps([{**entries[0],"database":"auth all"}])}),self.assertRaises(RuntimeError):
            self.entrypoint.access_rules("10.42.0.0/16")

    def release(self,root,inventory):
        catalog=json.loads((ROOT/"deployment/services/catalog.json").read_text())
        owner=json.loads((ROOT/"deployment/role1/config.example.json").read_text())
        owner.update(analytics_enabled=True,field_priority_enabled=True,bmkg_namespace="bmkg-test-release",satellite_namespace="satellite-test-release")
        owner["r3"]={"field_reports_namespace":"reports-test-release","coastal_namespace":"coastal-test-release","satellite_feed_url":"https://feed.invalid"}
        processor=json.loads((ROOT/"docs/fixtures/wildfire-m6/config.json").read_text())
        processor["namespace"]=owner["satellite_namespace"]
        owner["satellite_initial_offset"]=processor["initial_offset"]
        owner["wildfire"]={"config_json":json.dumps(processor),"province_json":(ROOT/"docs/fixtures/wildfire-m5/provinces.json").read_text()}
        images={name:self.image for name in IMAGES}
        images.update({s["image_key"]:self.image for s in catalog["services"]if "image_key"in s})
        images.update(storage=self.image,role1=self.image)
        aggregator=json.loads((ROOT/"deployment/aggregator/config.example.json").read_text());aggregator["storage_mode"]="citus"
        config={"version":1,"node_reserve_mib":512}
        for name,value in {"images":images,"aggregator":aggregator,"role1":owner,"services":catalog,"external_flows":[{"app":"ingestion","cidr":"192.0.2.1/32","ports":[443]}]}.items():
            config[name]=name+".json";(root/config[name]).write_text(json.dumps(value))
        (root/"release.json").write_text(json.dumps(config))
        return assemble(inventory,root/"release.json",root/"output")

    def test_full_release_fits_the_measured_two_large_two_small_cluster(self):
        # Allocatable memory reported by the actual K3s nodes (8 GB, 8 GB, 4 GB, 4 GB VMs).
        inventory=copy.deepcopy(self.inventory)
        for node,memory in zip(inventory["nodes"],(7941,7941,3915,3915)):node["memory_mib"]=memory
        with tempfile.TemporaryDirectory()as folder:
            report=self.release(Path(folder),inventory)
            self.assertEqual(report["memory_requests_mib"],19328)  # formerly 25856
            self.assertFalse(any("exceed"in problem for problem in report["blocked"]),report["blocked"])
            names=[n["name"]for n in inventory["nodes"]]
            ingestion=json.loads((Path(folder)/"output/ingestion-plan.json").read_text())
            self.assertEqual([m["node"]for m in ingestion["groups"][0]["members"]],[names[0],names[1],names[3]])
            self.assertLessEqual(report["fixed_requests_mib"][names[2]],3915-512)

    def test_integrated_release_owns_all_databases_and_reports_capacity(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)
            catalog=json.loads((ROOT/"deployment/services/catalog.json").read_text())
            owner=json.loads((ROOT/"deployment/role1/config.example.json").read_text())
            owner.update(analytics_enabled=True,field_priority_enabled=True,bmkg_namespace="bmkg-test-release",satellite_namespace="satellite-test-release")
            owner["r3"]={"field_reports_namespace":"reports-test-release","coastal_namespace":"coastal-test-release","satellite_feed_url":"https://feed.invalid"}
            processor=json.loads((ROOT/"docs/fixtures/wildfire-m6/config.json").read_text())
            processor["namespace"]=owner["satellite_namespace"]
            owner["satellite_initial_offset"]=processor["initial_offset"]
            owner["wildfire"]={"config_json":json.dumps(processor),"province_json":(ROOT/"docs/fixtures/wildfire-m5/provinces.json").read_text()}
            images={name:self.image for name in IMAGES}
            images.update({s["image_key"]:self.image for s in catalog["services"]if "image_key"in s})
            images.update(storage=self.image,role1=self.image)
            aggregator=json.loads((ROOT/"deployment/aggregator/config.example.json").read_text());aggregator["storage_mode"]="citus"
            inputs={"images":images,"aggregator":aggregator,"role1":owner,"services":catalog,"external_flows":[{"app":"ingestion","cidr":"192.0.2.1/32","ports":[443]}]}
            config={"version":1,"node_reserve_mib":512}
            for name,value in inputs.items():
                config[name]=name+".json";(root/config[name]).write_text(json.dumps(value))
            (root/"release.json").write_text(json.dumps(config))
            report=assemble(self.inventory,root/"release.json",root/"output")
            self.assertFalse(report["ready_for_apply"])
            self.assertTrue(any("memory"in v for v in report["blocked"]))
            self.assertEqual(set(report["formations"]),{self.inventory["cluster_id"]+"-"+s for s in("storage","ingestion",SERVICE_FORMATION)})
            self.assertTrue((root/"output/service-db-plan.json").is_file())
            for service in SERVICE_DATABASES:self.assertFalse((root/"output"/(service+"-plan.json")).exists())
            monitoring=json.loads((root/"output/artifacts/monitoring.json").read_text())
            prom=next(o for o in monitoring["items"]if o["kind"]=="ConfigMap"and o["metadata"]["name"]=="prometheus-config")
            jobs=json.loads(prom["data"]["prometheus.json"])["scrape_configs"]
            self.assertEqual(len(next(j for j in jobs if j["job_name"]=="patroni")["static_configs"]),21)
            self.assertEqual(len(next(j for j in jobs if j["job_name"]=="storage-etcd")["static_configs"]),9)
            # Private Secrets are staged before any storage, and supplied-but-
            # incomplete keys cannot disappear from offline preflight.
            (root/"secrets.json").write_text(json.dumps({"kind":"List","items":[{"apiVersion":"v1","kind":"Secret","metadata":{"name":owner["runtime_secret"],"namespace":self.inventory["namespace"]},"stringData":{"AUDIT_CURSOR_KEY":"private"}}]}))
            config["secrets"]="secrets.json"
            (root/"release.json").write_text(json.dumps(config))
            with_secrets=assemble(self.inventory,root/"release.json",root/"with-secrets")
            self.assertEqual(with_secrets["stages"][0]["id"],"secrets")
            self.assertEqual(with_secrets["stages"][1]["requires"],["secrets"])
            self.assertTrue(any("AUDIT_TOKEN"in v and "missing keys"in v for v in with_secrets["blocked"]))
            namespaces=[]
            for stage in with_secrets["stages"]:
                namespaces += [o for o in json.loads(Path(stage["manifest"]).read_text())["items"]if o["kind"]=="Namespace"]
            self.assertEqual(len(namespaces),1)

if __name__=="__main__":unittest.main()
