"""Assemble the existing owner renderers into one reviewed offline release."""
import json
from pathlib import Path

from bundle_lib import inspect
from citus_auth_lib import render as render_citus_auth
from database_lib import render as render_databases
from isolation_lib import render as render_isolation
from monitoring_lib import render as render_monitoring
from role1_integration_lib import render as render_role1
from service_lib import render as render_services
from storage_lib import FORMATIONS,durability_config,manifests,plan,require,write_json

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
    require(owner.get('field_priority_enabled')is True,'integrated release requires Field read pool isolation')
    require(isinstance(owner.get("wildfire"),dict)and isinstance(owner.get("r3"),dict),"release needs explicit wildfire and R3 configuration")
    for name in ("bmkg_namespace","satellite_namespace"):
        require(not owner[name].startswith(("synthetic-","fixture-")),"replace synthetic release source namespaces")
    aggregator=read("aggregator")
    require(aggregator["storage_mode"]=="citus","full release requires the distributed canonical formation")
    catalog=read("services")
    placement=config.get("local_placement",{})
    require(isinstance(placement,dict) and set(placement)<={"services","monitoring"},
            "local_placement must contain services and/or monitoring maps")
    require(all(isinstance(value,dict) for value in placement.values()),"local_placement sections must be objects")
    initialized=False;initialization=None
    if config.get("initialization_receipt"):
        from maintenance_lib import verify_receipt
        require(not config.get("owner_databases"),"initialization receipt requires the owned database renderer")
        initialization=read("initialization_receipt")
        verify_receipt(initialization,inventory,aggregator,owner,images)
        initialized=True
    client=next(s for s in catalog["services"]if s["name"]=="client-api")
    client["environment"].update({"ANALYTICS_ENABLED":"true","INGESTION_BASE_URL":"http://ingestion:8080"})
    catalog.setdefault("external",[]).extend(n for n in ("ingestion","coastal-processor")if n not in catalog.get("external",[]))
    # Prevent accidental duplicate Aggregator resources: role1 extends its base.
    application=render_role1(inventory,aggregator,owner,images["role1"],images["role1"])
    service_resources,service_requirements=render_services(inventory,catalog,images,placement=placement.get("services"))
    canonical=plan(inventory,"citus",images["storage"])
    ingestion=plan(inventory,"postgres",images["storage"],"ingestion")
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
    if config.get("secrets"):
        resolved=output.resolve()
        require(not resolved.is_relative_to(ROOT)or resolved.is_relative_to(ROOT/"evidence"),"keep a release containing Secrets outside the repository or under ignored evidence/")
        credentials=read("secrets")
        require(credentials.get("kind")=="List"and all(o.get("kind")=="Secret"for o in credentials["items"]),"secrets input must contain only Secret objects")
        add("secrets",{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Namespace","metadata":{"name":inventory["namespace"]}},*credentials["items"]]})
    add("canonical-storage",manifests(canonical),["secrets"]if config.get("secrets")else [])
    add("ingestion-storage",manifests(ingestion),["canonical-storage"])
    add("isolation",render_isolation(inventory,flows),["ingestion-storage"])
    blockers=list(config.get("unresolved",[]))
    owner_database_report=None
    if config.get("owner_databases"):
        add("owner-databases",read("owner_databases"),["isolation"])
    else:
        owner_resources,owner_database_report=render_databases(inventory,images["storage"])
        add("owner-databases",owner_resources,["isolation"])
        if not initialized:blockers.append("Initialize owned service database schemas with owning applications stopped, including the pinned registry bootstrap")
    monitored_owners=owner_database_report["formations"]if owner_database_report else []
    monitoring,monitor_requirements=render_monitoring(inventory,canonical,images,[ingestion,*monitored_owners],placement=placement.get("monitoring"))
    services_dependency="owner-databases"
    if initialized:
        auth,auth_name=render_citus_auth(canonical,initialization['durability']['storage'])
        add("canonical-authentication",auth,["owner-databases"],[{"kind":"Job","name":auth_name}])
        services_dependency="canonical-authentication"
    add("services",service_resources,[services_dependency],[{"kind":"Job","name":"broker-init"}])
    add("processing",application,["services"],[{"kind":"Deployment","name":inventory["cluster_id"]+"-aggregator"}])
    add("monitoring",monitoring,["processing"])
    captured=[]
    for storage,key,map_name in ((canonical,"canonical_durability",aggregator["durability_configmap"]),(ingestion,"ingestion_durability",owner["ingestion_durability_configmap"])):
        if not config.get(key):
            blockers.append("Capture original three-copy PostgreSQL identities for "+storage["formation"])
            continue
        value=read(key);expected=durability_config(storage)
        if initialized:
            suffix="storage"if key=="canonical_durability"else "ingestion"
            require(value==initialization["durability"][suffix],"release identities differ from original maintenance capture")
        require(len(value.get("groups",[]))==len(expected["groups"]),"durability group count differs")
        import re
        for planned,actual in zip(expected["groups"],value["groups"]):
            require(isinstance(actual.get("system_id"),str)and re.fullmatch(r"[1-9][0-9]{0,19}",actual["system_id"]),"actual PostgreSQL identities required")
            planned["system_id"]=actual["system_id"]
        require(expected==value and len({g["system_id"]for g in value["groups"]})==len(value["groups"]),"captured durability configuration differs from release placement")
        captured.append({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":map_name,"namespace":inventory["namespace"]},"data":{"durability.json":json.dumps(value,separators=(",",":"))}})
    if captured:
        artifacts["isolation"]["items"].extend(captured)
    if not initialized:blockers.append("Run recorded canonical/ingestion offline migrations and BMKG cutover with writers stopped before first activation")
    output.mkdir(parents=True,mode=0o700);(output/"artifacts").mkdir(mode=0o700)
    for stage,resource in artifacts.items():write_json(output/"artifacts"/(stage+".json"),resource)
    write_json(output/"canonical-plan.json",canonical);write_json(output/"ingestion-plan.json",ingestion)
    write_json(output/"service-requirements.json",service_requirements);write_json(output/"monitoring-requirements.json",monitor_requirements)
    if owner_database_report:
        write_json(output/"database-requirements.json",owner_database_report)
        for storage in owner_database_report["formations"]:write_json(output/(storage["formation_suffix"]+"-plan.json"),storage)
    bundle={"version":1,"formations":[inventory["cluster_id"]+"-"+suffix for suffix in FORMATIONS],"node_reserve_mib":config.get("node_reserve_mib",512),"stages":stages,"unresolved":blockers}
    write_json(output/"bundle.json",bundle)
    report,_=inspect(output/"bundle.json",inventory)
    write_json(output/"plan.json",report)
    return report
