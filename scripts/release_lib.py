"""Assemble the existing owner renderers into one reviewed offline release."""
import json
from pathlib import Path

from bundle_lib import inspect
from isolation_lib import render as render_isolation
from monitoring_lib import render as render_monitoring
from role1_integration_lib import render as render_role1
from service_lib import render as render_services
from storage_lib import durability_config,manifests,plan,require,write_json

ROOT=Path(__file__).resolve().parent.parent


def assemble(inventory,config_path,output):
    config_path=Path(config_path).resolve();base=config_path.parent
    config=json.loads(config_path.read_text())
    require(config.get("version")==1,"release configuration version must be 1")
    output=Path(output)
    require(not output.exists(),"release output already exists; preserve reviewed artifacts")
    def read(name):
        value=config.get(name)
        require(isinstance(value,str)and value,"explicit release input path required: "+name)
        return json.loads((base/value).read_text())
    images=read("images")
    owner=read("role1")
    if config.get("wildfire_config")and config.get("province_reference"):
        owner["wildfire"]={"config_json":(base/config["wildfire_config"]).read_text(encoding="utf-8"),"province_json":(base/config["province_reference"]).read_text(encoding="utf-8")}
    require(owner.get("analytics_enabled")is True,"release must enable the required analytics dashboard")
    require(isinstance(owner.get("wildfire"),dict)and isinstance(owner.get("r3"),dict),"release needs explicit wildfire and R3 configuration")
    for name in ("bmkg_namespace","satellite_namespace"):
        require(not owner[name].startswith(("synthetic-","fixture-")),"replace synthetic release source namespaces")
    aggregator=read("aggregator")
    require(aggregator["storage_mode"]=="citus","full release requires the distributed canonical formation")
    catalog=read("services")
    client=next(s for s in catalog["services"]if s["name"]=="client-api")
    client["environment"].update({"ANALYTICS_ENABLED":"true","INGESTION_BASE_URL":"http://ingestion:8080"})
    catalog.setdefault("external",[]).extend(n for n in ("ingestion","coastal-processor")if n not in catalog.get("external",[]))
    # Prevent accidental duplicate Aggregator resources: role1 extends its base.
    application=render_role1(inventory,aggregator,owner,images["role1"],images["role1"])
    service_resources,service_requirements=render_services(inventory,catalog,images)
    canonical=plan(inventory,"citus",images["storage"])
    ingestion=plan(inventory,"postgres",images["storage"],"ingestion")
    monitoring,monitor_requirements=render_monitoring(inventory,canonical,images)
    flows=read("external_flows")if config.get("external_flows")else []
    require(any(f.get("app")=="ingestion"and 443 in f.get("ports",[])for f in flows),"declare bounded satellite HTTPS egress before deployment")
    stages=[];artifacts={};namespace_written=False
    def add(stage,resource,dependencies=(),wait=()):
        nonlocal namespace_written
        items=[]
        for obj in resource["items"]:
            if obj["kind"]=="Namespace":
                if namespace_written:continue
                namespace_written=True
            items.append(obj)
        artifacts[stage]={"apiVersion":"v1","kind":"List","items":items}
        stages.append({"id":stage,"manifest":"artifacts/"+stage+".json","requires":list(dependencies),"wait":list(wait)})
    add("canonical-storage",manifests(canonical))
    add("ingestion-storage",manifests(ingestion),["canonical-storage"])
    add("isolation",render_isolation(inventory,flows),["ingestion-storage"])
    blockers=list(config.get("unresolved",[]))
    if config.get("owner_databases"):
        add("owner-databases",read("owner_databases"),["isolation"])
    else:
        stages.append({"id":"owner-databases","manifest":"artifacts/owner-databases.json","requires":["isolation"],"wait":[]})
        blockers.append("Provide owned registry/auth/dashboard/notifier database HA/schema resources")
    add("services",service_resources,["owner-databases"],[{"kind":"Job","name":"broker-init"}])
    add("processing",application,["services"],[{"kind":"Deployment","name":inventory["cluster_id"]+"-aggregator"}])
    add("monitoring",monitoring,["processing"])
    captured=[]
    for storage,key,map_name in ((canonical,"canonical_durability",aggregator["durability_configmap"]),(ingestion,"ingestion_durability",owner["ingestion_durability_configmap"])):
        if not config.get(key):
            blockers.append("Capture original three-copy PostgreSQL identities for "+storage["formation"])
            continue
        value=read(key);expected=durability_config(storage)
        require(len(value.get("groups",[]))==len(expected["groups"]),"durability group count differs")
        import re
        for planned,actual in zip(expected["groups"],value["groups"]):
            require(isinstance(actual.get("system_id"),str)and re.fullmatch(r"[1-9][0-9]{0,19}",actual["system_id"]),"actual PostgreSQL identities required")
            planned["system_id"]=actual["system_id"]
        require(expected==value and len({g["system_id"]for g in value["groups"]})==len(value["groups"]),"captured durability configuration differs from release placement")
        captured.append({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":map_name,"namespace":inventory["namespace"]},"data":{"durability.json":json.dumps(value,separators=(",",":"))}})
    if captured:
        artifacts["isolation"]["items"].extend(captured)
    blockers.append("Run recorded canonical/ingestion offline migrations and BMKG cutover with writers stopped before first activation")
    output.mkdir(parents=True,mode=0o700);(output/"artifacts").mkdir(mode=0o700)
    for stage,resource in artifacts.items():write_json(output/"artifacts"/(stage+".json"),resource)
    write_json(output/"canonical-plan.json",canonical);write_json(output/"ingestion-plan.json",ingestion)
    write_json(output/"service-requirements.json",service_requirements);write_json(output/"monitoring-requirements.json",monitor_requirements)
    bundle={"version":1,"node_reserve_mib":config.get("node_reserve_mib",512),"stages":stages,"unresolved":blockers}
    write_json(output/"bundle.json",bundle)
    report,_=inspect(output/"bundle.json",inventory)
    write_json(output/"plan.json",report)
    return report
