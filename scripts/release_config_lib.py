"""Prepare unused private credentials and explicit pending release inputs offline."""
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import secrets
from urllib.parse import quote

from monitoring_lib import IMAGES
from service_lib import DIGEST
from storage_lib import SERVICE_DATABASES,formation_layout,require,write_json

ROOT=Path(__file__).resolve().parent.parent


def storage_credentials(layout,output):
    # Reuse the same PKI/credential implementation as platform-storage.
    spec=importlib.util.spec_from_file_location("platform_storage",ROOT/"scripts/platform-storage.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    module.make_secrets(layout,output)


def read_token(path):
    path=Path(path)
    require(path.is_file()and not path.is_symlink(),"token input must be a regular file")
    require(path.stat().st_mode&0o077==0,"token input must have private permissions")
    require(path.stat().st_size<=16384,"token input is too large")
    value=path.read_text().strip()
    require(value and not any(c.isspace()for c in value),"token must be one nonempty value")
    return value


def province_bytes(path):
    raw=Path(path).read_bytes()
    require(len(raw)<=1<<20,"province reference is too large")
    rows=json.loads(raw);seen=set()
    require(isinstance(rows,list)and rows,"province reference must be a nonempty array")
    for row in rows:
        require(isinstance(row,dict)and set(row)=={"id","name","latitude","longitude"},"province reference row must contain id, name, latitude, longitude")
        require(all(isinstance(row[k],str)and row[k].strip()for k in ("id","name")),"province ID and name required")
        require(row["id"]not in seen,"duplicate province ID");seen.add(row["id"])
        require(not any(word in (row["id"]+" "+row["name"]).lower()for word in ("fixture","synthetic")),"fixture province reference cannot prepare a release")
        for key,limit in (("latitude",90),("longitude",180)):
            value=row[key]
            require(type(value)in(int,float)and math.isfinite(value)and abs(value)<=limit,"invalid province coordinate")
    return raw


def prepare(inventory,output,*,satellite_token_file=None,audit_token_file=None,province_reference=None,images_file=None,external_flows_file=None):
    output=Path(output)
    resolved=output.resolve()
    require(not resolved.is_relative_to(ROOT)or resolved.is_relative_to(ROOT/"evidence"),"private configuration must be outside the repository or under ignored evidence/")
    require(not output.exists()and not output.is_symlink(),"output exists; refuse implicit credential rotation")
    # Validate external inputs before generating anything. Missing inputs stay absent.
    tokens={key:read_token(path)for key,path in (("SATELLITE_FEED_TOKEN",satellite_token_file),("AUDIT_TOKEN",audit_token_file))if path}
    provinces=province_bytes(province_reference)if province_reference else None
    catalog=json.loads((ROOT/"deployment/services/catalog.json").read_text())
    image_names=set(IMAGES)|{"storage","role1"}|{s["image_key"]for s in catalog["services"]if s.get("image_key")}
    supplied_images=json.loads(Path(images_file).read_text())if images_file else {}
    require(isinstance(supplied_images,dict)and set(supplied_images)<=image_names,"unknown image keys")
    require(all(isinstance(v,str)and DIGEST.fullmatch(v)for v in supplied_images.values()),"provided images must use immutable digests")
    flows=json.loads(Path(external_flows_file).read_text())if external_flows_file else []
    from isolation_lib import render as render_isolation
    render_isolation(inventory,flows)  # Validate explicit bounded CIDRs without DNS guesses.
    old_umask=os.umask(0o077)
    try:
        output.mkdir(parents=True,mode=0o700)
        return _write(inventory,output,catalog,image_names,supplied_images,flows,tokens,provinces)
    finally:os.umask(old_umask)


def _write(inventory,output,catalog,image_names,images,flows,tokens,provinces):
    cluster,ns=inventory["cluster_id"],inventory["namespace"]
    pending=[];items=[];database_env={}
    for suffix in ("storage","ingestion",*SERVICE_DATABASES):
        layout=formation_layout(inventory,"citus"if suffix=="storage"else "postgres",suffix)
        folder=output/"storage"/suffix
        storage_credentials(layout,folder)
        items.extend(json.loads((folder/"secrets.json").read_text())["items"])
        database_env[suffix]=dict(line.split("=",1)for line in(folder/"application.env").read_text().splitlines())
    keys="AGGREGATOR_ADMIN_TOKEN BMKG_API_KEY PVMBG_TOKEN PVMBG_ADMIN_TOKEN CANONICAL_SERVICE_TOKEN PROCESSOR_SERVICE_TOKEN HISTORY_SERVICE_TOKEN INGESTION_SERVICE_TOKEN FIELD_REPORT_SERVICE_TOKEN COASTAL_SLO_TOKEN ANALYTICS_SERVICE_TOKEN AUDIT_CURSOR_KEY INGESTION_CURSOR_KEY AUTH_INTERNAL_KEY AUTH_PRIORITY_KEY FIELD_READ_TOKEN PANTAI_AUTH_TOKEN NATS_APP_PASSWORD NATS_SYSTEM_PASSWORD".split()
    values={key:secrets.token_hex(32)for key in keys};values.update(tokens)
    values.update(NATS_APP_USER="aat_app",NATS_SYSTEM_USER="aat_recovery")
    for prefix in ("MEDIA","FIELD","INTERNAL"):
        values[prefix+"_CLIENT_ID"]=cluster+"-"+prefix.lower()
        values[prefix+"_CLIENT_SECRET"]=secrets.token_hex(32)
    values["NATS_URL"]="nats://aat_app:"+quote(values["NATS_APP_PASSWORD"],safe="")+"@broker:4222"
    values["NATS_SYSTEM_URL"]="nats://aat_recovery:"+quote(values["NATS_SYSTEM_PASSWORD"],safe="")+"@broker:4222"
    def select(*names):return {k:values[k]for k in names if k in values}
    def secret(name,data):
        items.append({"apiVersion":"v1","kind":"Secret","type":"Opaque","metadata":{"name":name,"namespace":ns,"labels":{"platform.aat/cluster":cluster}},"stringData":data})
    agg=json.loads((ROOT/"deployment/aggregator/config.integrated.example.json").read_text())
    role1=json.loads((ROOT/"deployment/role1/config.integrated.example.json").read_text())
    for cfg,names in ((agg,("runtime_secret","storage_secret","durability_configmap")),(role1,("runtime_secret","ingestion_storage_secret","ingestion_durability_configmap"))):
        for name in names:cfg[name]=cfg[name].replace("aat-m2",cluster,1)
    secret(agg["runtime_secret"],select("AGGREGATOR_ADMIN_TOKEN","BMKG_API_KEY","PVMBG_TOKEN","CANONICAL_SERVICE_TOKEN","PROCESSOR_SERVICE_TOKEN","NATS_URL"))
    secret(agg["storage_secret"],database_env["storage"])
    secret(role1["ingestion_storage_secret"],database_env["ingestion"])
    secret(role1["runtime_secret"],select("HISTORY_SERVICE_TOKEN","INGESTION_SERVICE_TOKEN","AUDIT_TOKEN","AUDIT_CURSOR_KEY","INGESTION_CURSOR_KEY","AUTH_INTERNAL_KEY","BMKG_API_KEY","NATS_URL","ANALYTICS_SERVICE_TOKEN","FIELD_REPORT_SERVICE_TOKEN","SATELLITE_FEED_TOKEN","CANONICAL_SERVICE_TOKEN","COASTAL_SLO_TOKEN","FIELD_READ_TOKEN"))
    auth=select("AUTH_INTERNAL_KEY","AUTH_PRIORITY_KEY",*(p+"_CLIENT_"+k for p in ("MEDIA","FIELD","INTERNAL")for k in ("ID","SECRET")))
    secret("auth-runtime",{**auth,"DATABASE_URL":database_env["auth-db"]["DATABASE_URL"]})
    secret("client-api-runtime",select("AUTH_INTERNAL_KEY","AUTH_PRIORITY_KEY","FIELD_READ_TOKEN","ANALYTICS_SERVICE_TOKEN","FIELD_REPORT_SERVICE_TOKEN"))
    secret("broker-runtime",select("NATS_APP_USER","NATS_APP_PASSWORD","NATS_SYSTEM_USER","NATS_SYSTEM_PASSWORD","NATS_URL","NATS_SYSTEM_URL"))
    secret("broker-init-runtime",select("NATS_URL"))
    for suffix,name in (("dashboard-db","dashboard-runtime"),("notifier-db","notifier-runtime")):
        secret(name,{"DATABASE_URL":database_env[suffix]["DATABASE_URL"],**select("NATS_URL",*( ["COASTAL_SLO_TOKEN"]if suffix=="notifier-db"else []))})
    secret("registry-runtime",{k:v for k,v in database_env["registry-db"].items()if k.startswith("APICURIO_")})
    secret("bmkg-runtime",select("BMKG_API_KEY"));secret("pvmbg-runtime",select("PVMBG_TOKEN","PVMBG_ADMIN_TOKEN"))
    secret("pantai-runtime",select("PANTAI_AUTH_TOKEN"))
    secret("grafana-runtime",{"GF_SECURITY_ADMIN_USER":"admin","GF_SECURITY_ADMIN_PASSWORD":secrets.token_hex(32)})
    write_json(output/"secrets.json",{"apiVersion":"v1","kind":"List","items":items})
    write_json(output/"client-credentials.json",select(*(p+"_CLIENT_"+k for p in ("MEDIA","FIELD","INTERNAL")for k in ("ID","SECRET"))))
    processor=json.loads((ROOT/"deployment/role1/wildfire.integrated.example.json").read_text())
    if provinces:
        (output/"provinces.json").write_bytes(provinces)
        processor["province_sha256"]=hashlib.sha256(provinces).hexdigest()
    else:pending.append("Supply the agreed real province-centre reference; provinces.json is intentionally absent")
    for key in ("AUDIT_TOKEN","SATELLITE_FEED_TOKEN"):
        if key not in values:pending.append("Supply external credential "+key+"; its Secret key is intentionally absent")
    missing_images=sorted(image_names-images.keys())
    if missing_images:pending.append("Build/publish immutable images: "+", ".join(missing_images))
    if not any(f.get("app")=="ingestion"and 443 in f.get("ports",[])for f in flows):pending.append("Supply bounded satellite HTTPS egress CIDRs in external-flows.json")
    pending+= ["Confirm source session namespaces and initial offsets before activation", "Populate actual four-host inventory and measured capacity; this scaffold does not provision nodes"]
    config=json.loads((ROOT/"deployment/platform/release.example.json").read_text())
    config["unresolved"]=pending
    for name,value in (("aggregator",agg),("role1",role1),("services",catalog),("wildfire",processor),("images",{name:images.get(name)for name in sorted(image_names)}),("external-flows",flows),("release",config)):
        write_json(output/(name+".json"),value)
    report={"version":1,"ready_for_activation":False,"pending":pending,"secret_count":len(items),"note":"Unused private credentials only. No VM, database, registry or cluster was contacted. Do not rerun to rotate credentials."}
    write_json(output/"preparation.json",report)
    for path in output.rglob("*"):path.chmod(0o700 if path.is_dir()else 0o600)
    return report
