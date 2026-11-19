"""Offline plans and explicit stopped-writer maintenance with bound receipts."""
import copy
import hashlib
import json
from pathlib import Path
import re
import uuid

from bundle_lib import inspect
from database_admin_lib import initialize,original_primary,verify_isolation
from database_lib import schema_sql
from platform_lib.inventory import fingerprint
from service_lib import DIGEST
from storage_lib import FORMATIONS,SERVICE_DATABASES,SERVICE_FORMATION,durability_config,plan,require,service_database,write_json

STEPS=(*("initialize-"+s for s in SERVICE_DATABASES),"verify-"+SERVICE_FORMATION+"-isolation","canonical","ingestion","cutover")

ROOT=Path(__file__).resolve().parent.parent
APPLICATIONS={"aggregator","ingestion","coastal-processor","wildfire-processor","auth-service","client-api","consumer-dashboard","consumer-notifier","schema-registry","bmkg-mock","pvmbg-mock","sensor-pantai","broker-init","canonical-migrate","ingestion-migrate","registry-db-init"}


def hash_json(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()


def expected_schemas():
    canonical={p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted((ROOT/"store/migrations").glob("*.sql"))}
    source=(ROOT/"aggregator/internal/ingestion/migrate.go").read_text()
    names=re.search(r'names := \[\]string\{([^}]+)\}',source)
    require(names,"cannot identify ingestion migration contract")
    ingestion={}
    for name in re.findall(r'"([0-9a-z_]+\.sql)"',names[1]):
        path=ROOT/"aggregator/internal/ingestion/migrations"/name
        if not path.is_file():path=ROOT/"store/migrations"/name
        ingestion[name]=hashlib.sha256(path.read_bytes()).hexdigest()
    owners={}
    for service in SERVICE_DATABASES:
        owners[service]=re.search(r"VALUES\('owner-v1','([0-9a-f]{64})'\)",schema_sql(service))[1]
    return {"canonical":canonical,"ingestion":ingestion,"owners":owners}


def contract(inventory,aggregator,owner,images):
    for name in("storage","role1","schema-registry"):
        require(isinstance(images.get(name),str)and DIGEST.fullmatch(images[name]),"actual immutable maintenance image required: "+name)
    files=["aggregator/internal/migrations/migrations.go","aggregator/internal/ingestion/migrate.go","aggregator/internal/store/input_cutover.go","aggregator/cmd/input-cutover/main.go","aggregator/cmd/platform-maintenance/main.go","scripts/database_admin_lib.py","scripts/database_lib.py","scripts/maintenance_lib.py"]
    return {"version":1,"inventory_sha256":fingerprint(inventory),"images":{k:images[k]for k in("storage","role1","schema-registry")},"source_identity":{k:owner[k]for k in("bmkg_namespace","bmkg_initial_offset","satellite_namespace","satellite_initial_offset")},"canonical_storage_mode":aggregator["storage_mode"],"storage_secret":aggregator["storage_secret"],"schemas":expected_schemas(),"tools":{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()for name in files}}


def verify_receipt(receipt,inventory,aggregator,owner,images):
    expected=contract(inventory,aggregator,owner,images)
    require(receipt.get("version")==1 and receipt.get("kind")=="platform-initialization"and receipt.get("passed")is True,"successful initialization receipt required")
    require(receipt.get("contract")==expected and receipt.get("contract_sha256")==hash_json(expected),"initialization receipt differs from current source, images, inventory or namespace")
    require(receipt.get("schemas")==expected["schemas"],"observed schema checksums do not match current migrations")
    require(receipt.get("cutover")=={"namespace":owner["bmkg_namespace"],"state":"ready"},"BMKG cutover was not verified ready")
    steps=receipt.get("steps",[])
    require([s.get("action")for s in steps]==list(STEPS)and all(s.get("completed")is True for s in steps),"initialization sequence is incomplete")
    require(all(isinstance(s.get("uid"),str)and s["uid"]and s.get("image")==images["role1"]for s in steps[-3:]),"missing original completed migration Job identities")
    isolation=receipt.get("service_isolation",{})
    members=[m["name"]for m in plan(inventory,"postgres",images["storage"],SERVICE_FORMATION)["groups"][0]["members"]]
    require(isolation.get("passed")is True and [m.get("member")for m in isolation.get("members",[])]==members and all(m.get("passed")is True for m in isolation["members"]),"owner database isolation was not verified on every copy")
    captures=receipt.get("durability",{});systems=set()
    for suffix in FORMATIONS:
        wanted=durability_config(plan(inventory,"citus"if suffix=="storage"else "postgres",images["storage"],suffix))
        actual=captures.get(suffix)
        require(isinstance(actual,dict)and len(actual.get("groups",[]))==len(wanted["groups"]),"missing original formation identity capture")
        for target,observed in zip(wanted["groups"],actual["groups"]):
            value=observed.get("system_id")
            require(isinstance(value,str)and re.fullmatch(r"[1-9][0-9]{0,19}",value)and value not in systems,"invalid or reused original PostgreSQL identity")
            systems.add(value);target["system_id"]=value
        require(wanted==actual,"captured formation differs from initialization placement")
    return expected


def stopped(cluster):
    ns=cluster.cfg["namespace"]
    controllers=json.loads(cluster.kubectl("get","deployments,statefulsets","-n",ns,"-o","json").stdout)["items"]
    for obj in controllers:
        app=obj["spec"]["template"]["metadata"].get("labels",{}).get("app.kubernetes.io/name")
        if app in APPLICATIONS:require(obj["spec"].get("replicas",1)==0,"stop application controller before maintenance: "+obj["metadata"]["name"])
    pods=json.loads(cluster.kubectl("get","pods","-n",ns,"-o","json").stdout)["items"]
    require(not any(p["metadata"].get("labels",{}).get("app.kubernetes.io/name")in APPLICATIONS and p.get("status",{}).get("phase")not in("Succeeded","Failed")for p in pods),"active application or maintenance pod remains; wait for stopped writers")


def inputs(inventory,config_path,infrastructure):
    config_path=Path(config_path).resolve();base=config_path.parent;config=json.loads(config_path.read_text());infrastructure=Path(infrastructure)
    def read(key):return json.loads((base/config[key]).read_text())
    images=read("images");aggregator=read("aggregator");owner=read("role1");bound=contract(inventory,aggregator,owner,images)
    report,snapshots=inspect(infrastructure/"bundle.json",inventory)
    require(report["phase"]=="infrastructure","maintenance requires the prepared storage-only bundle")
    formations={}
    for suffix in FORMATIONS:
        path=infrastructure/("canonical-plan.json"if suffix=="storage"else suffix+"-plan.json")
        value=json.loads(path.read_text());require(value==plan(inventory,"citus"if suffix=="storage"else "postgres",images["storage"],suffix),"infrastructure formation plan differs")
        formations[suffix]=value
    return config,base,images,aggregator,owner,bound,formations,{name:hashlib.sha256(value).hexdigest()for name,value in snapshots.items()}


def maintenance_job(inventory,storage,aggregator,owner,image,action,run):
    cluster=inventory["cluster_id"];name=cluster+"-maintenance-"+action+"-"+run[:8];ns=inventory["namespace"]
    app="ingestion-migrate"if action=="ingestion"else "canonical-migrate"
    env=[{"name":"PGHOST","value":storage["formation"]+"-g0-primary."+ns+".svc.cluster.local"},{"name":"PGDATABASE","value":storage.get("database","hazards")},{"name":"PGPASSWORD","valueFrom":{"secretKeyRef":{"name":storage["formation"]+"-credentials","key":"POSTGRES_PASSWORD"}}},{"name":"DB_MAX_OPEN_CONNS","value":"4"}]
    container={"name":"maintenance","image":image,"command":["/usr/local/bin/platform-maintenance","--action",action],"env":env,"resources":{"requests":{"memory":"128Mi","cpu":"100m"},"limits":{"memory":"256Mi","cpu":"1"}}}
    pod={"restartPolicy":"Never","automountServiceAccountToken":False,"containers":[container],"securityContext":{"runAsNonRoot":True,"runAsUser":10001,"runAsGroup":10001},"volumes":[]}
    if action=="cutover":
        container["command"] += ["--namespace",owner["bmkg_namespace"]]
        env.append({"name":"CANONICAL_DURABILITY_CONFIG","value":"/etc/aat/durability.json"})
        for group in storage["groups"]:env.append({"name":group["dsn_env"],"valueFrom":{"secretKeyRef":{"name":aggregator["storage_secret"],"key":group["dsn_env"]}}})
        container["volumeMounts"]=[{"name":"durability","mountPath":"/etc/aat","readOnly":True}]
        pod["volumes"]=[{"name":"durability","configMap":{"name":cluster+"-maintenance-"+run[:8]}}]
    return {"apiVersion":"batch/v1","kind":"Job","metadata":{"name":name,"namespace":ns,"labels":{"platform.aat/cluster":cluster,"platform.aat/maintenance":run}},"spec":{"backoffLimit":0,"activeDeadlineSeconds":900,"template":{"metadata":{"labels":{"app.kubernetes.io/name":app,"role":"other","platform.aat/cluster":cluster,"platform.aat/maintenance":run}},"spec":pod}}}


def sql_json(cluster,storage,query,database=None):
    leader,_=original_primary(cluster,storage)
    result=cluster.kubectl("exec","-n",storage["namespace"],leader["name"],"-c","postgres","--","psql","-U","postgres","-d",database or storage.get("database","hazards"),"-v","ON_ERROR_STOP=1","-Atc",query)
    return json.loads(result.stdout)


def run(cluster,config_path,infrastructure,output):
    config,base,images,aggregator,owner,bound,formations,artifacts=inputs(cluster.cfg,config_path,infrastructure)
    output=Path(output);require(not output.exists(),"maintenance output exists; retain prior receipt and use a new run directory")
    output.mkdir(parents=True,mode=0o700);run=uuid.uuid4().hex;ns=cluster.cfg["namespace"]
    receipt={"version":1,"kind":"platform-initialization","run_id":run,"passed":False,"contract":bound,"contract_sha256":hash_json(bound),"infrastructure_artifacts":artifacts,"durability":{},"steps":[]}
    def save():write_json(output/"receipt.json",receipt)
    save()
    try:
        stopped(cluster)
        for suffix,storage in formations.items():
            captured=durability_config(storage)
            for group,result in zip(storage["groups"],captured["groups"]):
                _,statuses=original_primary(cluster,storage,group);result["system_id"]=statuses[0]["system_id"]
            receipt["durability"][suffix]=captured;write_json(output/(suffix+"-durability.json"),captured);save()
        shared=formations[SERVICE_FORMATION]
        for service in SERVICE_DATABASES:
            stopped(cluster);initialize(cluster,shared,service,images["schema-registry"]if service=="registry-db"else None)
            receipt["steps"].append({"action":"initialize-"+service,"completed":True});save()
        receipt["service_isolation"]=verify_isolation(cluster,shared)
        receipt["steps"].append({"action":"verify-"+SERVICE_FORMATION+"-isolation","completed":True});save()
        cm={"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":cluster.cfg["cluster_id"]+"-maintenance-"+run[:8],"namespace":ns,"labels":{"platform.aat/maintenance":run}},"immutable":True,"data":{"durability.json":json.dumps(receipt["durability"]["storage"])}}
        cluster.kubectl("create","-f","-",input=json.dumps(cm).encode())
        for action in("canonical","ingestion","cutover"):
            stopped(cluster);storage=formations["ingestion"if action=="ingestion"else "storage"]
            job=maintenance_job(cluster.cfg,storage,aggregator,owner,images["role1"],action,run);name=job["metadata"]["name"]
            write_json(output/(action+"-job.json"),job)
            created=json.loads(cluster.kubectl("create","-f","-","-o","json",input=json.dumps(job).encode()).stdout)
            try:
                cluster.kubectl("wait","--for=condition=Complete","job/"+name,"-n",ns,"--timeout=910s",timeout=940)
            finally:
                log=cluster.kubectl("logs","job/"+name,"-n",ns,"--all-containers=true","--tail=2000",check=False)
                (output/(action+".log")).write_bytes(log.stdout);(output/(action+".log")).chmod(0o600)
            current=json.loads(cluster.kubectl("get","job",name,"-n",ns,"-o","json").stdout)
            require(current["metadata"]["uid"]==created["metadata"]["uid"]and any(c["type"]=="Complete"and c["status"]=="True"for c in current.get("status",{}).get("conditions",[])),"maintenance Job identity/completion changed")
            receipt["steps"].append({"action":action,"uid":created["metadata"]["uid"],"image":images["role1"],"completed":True});save()
        stopped(cluster)
        schemas={}
        for suffix,table in(("storage","canonical_schema_migrations"),("ingestion","ingestion_migrations")):
            schemas["canonical"if suffix=="storage"else "ingestion"]=sql_json(cluster,formations[suffix],"SELECT jsonb_object_agg(version,checksum) FROM "+table)
        schemas["owners"]={service:sql_json(cluster,shared,"SELECT to_jsonb(checksum) FROM service_schema_migrations WHERE version='owner-v1'",service_database(shared,service)["database"])for service in SERVICE_DATABASES}
        # SQL values use literal quoting; namespaces cannot inject a statement.
        namespace=owner["bmkg_namespace"].replace("'","''")
        receipt["cutover"]=sql_json(cluster,formations["storage"],"SELECT jsonb_build_object('namespace',namespace,'state',state) FROM input_cutovers WHERE namespace='"+namespace+"'")
        receipt["schemas"]=schemas;receipt["passed"]=True
        verify_receipt(receipt,cluster.cfg,aggregator,owner,images)
        # Repeat original identity checks after all DDL/cutover operations.
        for suffix,storage in formations.items():
            for group,original in zip(storage["groups"],receipt["durability"][suffix]["groups"]):
                _,statuses=original_primary(cluster,storage,group)
                require(all(s["system_id"]==original["system_id"]for s in statuses),"database identity changed during maintenance")
        save()
        ready=copy.deepcopy(config)
        for key in("images","aggregator","role1","services","secrets","external_flows","wildfire_config","province_reference","owner_databases"):
            if ready.get(key):ready[key]=str((base/ready[key]).resolve())
        ready.update(initialization_receipt=str((output/"receipt.json").resolve()),canonical_durability=str((output/"storage-durability.json").resolve()),ingestion_durability=str((output/"ingestion-durability.json").resolve()))
        write_json(output/"release-ready.json",ready)
        return receipt
    except BaseException:
        receipt["passed"]=False;save();raise
