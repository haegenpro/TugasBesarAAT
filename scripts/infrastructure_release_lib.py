"""Prepare storage/Secrets independently of application activation prerequisites."""
import json
from pathlib import Path

from bundle_lib import inspect
from database_lib import render as render_databases
from isolation_lib import render as render_isolation
from platform_lib.inventory import fingerprint
from storage_lib import manifests,plan,require,write_json

ROOT=Path(__file__).resolve().parent.parent


def ingestion_policies(inventory):
    """Storage-only bootstrap cannot depend on rendering wildfire/application data."""
    cluster=inventory["cluster_id"];ns=inventory["namespace"];formation=cluster+"-ingestion"
    pg={"platform.aat/formation":formation,"component":"storage-postgres"}
    router={"platform.aat/formation":formation,"component":"storage-router"}
    etcd={"platform.aat/formation":formation,"component":"storage-etcd"}
    def peer(labels):return {"podSelector":{"matchLabels":labels}}
    def ports(*values):return [{"protocol":"TCP","port":v}for v in values]
    clients=[{"app.kubernetes.io/name":name}for name in("ingestion","coastal-processor","ingestion-migrate")]
    monitor={"app.kubernetes.io/name":"prometheus"};admin={"app.kubernetes.io/name":"ingestion-migrate"}
    rules=[("pg",pg,[{"from":[peer(p)for p in[pg,router,*clients]],"ports":ports(5432)},{"from":[peer(p)for p in(pg,router,monitor)],"ports":ports(8008)}],[{"to":[peer(pg)],"ports":ports(5432,8008)},{"to":[peer(etcd)],"ports":ports(2379)}]),
           ("router",router,[{"from":[peer(p)for p in clients],"ports":ports(5432)}],[{"to":[peer(pg)],"ports":ports(5432,8008)}]),
           ("etcd",etcd,[{"from":[peer(pg),peer(etcd)],"ports":ports(2379)},{"from":[peer(etcd)],"ports":ports(2380)},{"from":[peer(monitor)],"ports":ports(2381)}],[{"to":[peer(etcd)],"ports":ports(2379,2380)}]),
           ("admin",admin,[],[{"to":[peer(pg),peer(router)],"ports":ports(5432)}])]
    return [{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":cluster+"-bootstrap-ingestion-"+name,"namespace":ns},"spec":{"podSelector":{"matchLabels":selector},"policyTypes":["Ingress","Egress"],"ingress":ingress,"egress":egress}}for name,selector,ingress,egress in rules]


def assemble(inventory,config_path,output):
    config_path=Path(config_path).resolve();base=config_path.parent
    config=json.loads(config_path.read_text());require(config.get("version")==1,"release configuration version must be 1")
    output=Path(output);require(not output.exists(),"infrastructure output exists; preserve reviewed artifacts")
    def read(key):
        require(isinstance(config.get(key),str)and config[key],"explicit input path required: "+key)
        return json.loads((base/config[key]).read_text())
    image=read("images").get("storage")
    canonical=plan(inventory,"citus",image)
    ingestion=plan(inventory,"postgres",image,"ingestion")
    owners,requirements=render_databases(inventory,image)
    artifacts={};stages=[]
    namespace={"apiVersion":"v1","kind":"Namespace","metadata":{"name":inventory["namespace"]}}
    first=[namespace]
    if config.get("secrets"):
        resolved=output.resolve()
        require(not resolved.is_relative_to(ROOT)or resolved.is_relative_to(ROOT/"evidence"),"keep a release containing Secrets outside the repository or under ignored evidence/")
        credentials=read("secrets")
        require(credentials.get("kind")=="List"and all(o.get("kind")=="Secret"for o in credentials["items"]),"secrets input must contain only Secret objects")
        first+=credentials["items"]
    def add(name,items,dependencies=()):
        artifacts[name]={"apiVersion":"v1","kind":"List","items":[o for o in items if o["kind"]!="Namespace"or name=="prerequisites"]}
        stages.append({"id":name,"manifest":"artifacts/"+name+".json","requires":list(dependencies)})
    add("prerequisites",first)
    # Satellite/Audit/reference inputs are activation prerequisites. No source
    # or application is started here, so their absence must not prevent PKI/DB
    # bootstrap. Default deny and the precise storage flows still apply now.
    add("isolation",[*render_isolation(inventory)["items"],*ingestion_policies(inventory)],["prerequisites"])
    add("canonical-storage",manifests(canonical)["items"],["isolation"])
    add("ingestion-storage",manifests(ingestion)["items"],["isolation"])
    add("owner-databases",owners["items"],["isolation"])
    formations=[canonical,ingestion,*requirements["formations"]]
    output.mkdir(parents=True,mode=0o700);(output/"artifacts").mkdir(mode=0o700)
    for name,value in artifacts.items():write_json(output/"artifacts"/(name+".json"),value)
    for storage in formations:
        suffix=storage.get("formation_suffix","canonical")
        write_json(output/(suffix+"-plan.json"),storage)
    write_json(output/"database-requirements.json",requirements)
    bundle={"version":1,"phase":"infrastructure","formations":[s["formation"]for s in formations],"node_reserve_mib":config.get("node_reserve_mib",512),"stages":stages,"unresolved":config.get("infrastructure_unresolved",[])}
    bundle["activation_pending"]=[*config.get("unresolved",[]),"Initialize owned service schemas and pinned Registry schema while owning applications are stopped, then verify owner isolation","Capture original three-copy identities for all "+str(len(formations))+" formations","Run offline canonical/ingestion migrations and BMKG cutover before full activation"]
    write_json(output/"bundle.json",bundle)
    report,_=inspect(output/"bundle.json",inventory)
    report.update(phase="infrastructure",inventory_sha256=fingerprint(inventory),note="Storage/Secrets only; no application, source or publisher is started. Full activation remains a separate reviewed release.")
    write_json(output/"plan.json",report)
    return report
