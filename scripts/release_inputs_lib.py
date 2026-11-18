"""Complete external release inputs without regenerating internal credentials."""
import copy
import hashlib
import json
import os
from pathlib import Path

from image_release_lib import REQUIRED
from isolation_lib import render as isolation
from release_config_lib import DEFAULT_PROVINCE_REFERENCE,province_bytes,read_token
from service_lib import DIGEST
from storage_lib import require,write_json

ROOT=Path(__file__).resolve().parent.parent
GENERATED_PENDING=("Supply the agreed real province-centre reference", "Supply external credential ","Build/publish immutable images:","Supply bounded satellite HTTPS egress", "Confirm source session namespaces", "Populate actual four-host inventory", "Supply source_identity", "Missing immutable images:")


def complete(inventory,config_path,inputs_path,output):
    config_path=Path(config_path).resolve();base=config_path.parent;inputs_path=Path(inputs_path).resolve()
    selected=json.loads(inputs_path.read_text());require(selected.get("version")==1,"external input version must be 1")
    output=Path(output);resolved=output.resolve()
    require(not resolved.is_relative_to(ROOT)or resolved.is_relative_to(ROOT/"evidence"),"private completed inputs must stay outside the repository or under ignored evidence/")
    require(not output.exists(),"completed output exists; refuse to overwrite credentials or reviewed inputs")
    config=json.loads(config_path.read_text());require(config.get("version")==1,"release configuration version must be 1")
    def original(key):return json.loads((base/config[key]).read_text())
    def selected_path(key):
        value=selected.get(key);return (inputs_path.parent/value).resolve()if isinstance(value,str)and value else None
    aggregator=original("aggregator");owner=original("role1");services=original("services");secrets=original("secrets")
    require(secrets.get("kind")=="List"and all(o.get("kind")=="Secret"for o in secrets["items"]),"original private Secret List required")
    secrets=copy.deepcopy(secrets);pending=[]
    runtime=[o for o in secrets["items"]if o["metadata"]["name"]==owner["runtime_secret"]]
    require(len(runtime)==1,"one original role1 runtime Secret required")
    for key,field in(("AUDIT_TOKEN","audit_token_file"),("SATELLITE_FEED_TOKEN","satellite_token_file")):
        path=selected_path(field)
        if path:
            value=read_token(path);runtime[0].setdefault("stringData",{})[key]=value;runtime[0].get("data",{}).pop(key,None)
        present=runtime[0].get("stringData",{}).get(key)or runtime[0].get("data",{}).get(key)
        if not present:pending.append("Supply external credential "+key)
    binding=selected.get("source_identity")
    fields=("bmkg_namespace","bmkg_initial_offset","satellite_namespace","satellite_initial_offset")
    if binding is None:pending.append("Supply source_identity with chosen stable namespaces and starting offsets")
    else:
        require(isinstance(binding,dict)and set(binding)==set(fields),"source_identity requires exactly the two namespaces and two initial offsets")
        for key in fields:
            value=binding[key]
            if key.endswith("namespace"):require(isinstance(value,str)and 0<len(value)<=200 and "\x00"not in value and not value.startswith(("fixture-","synthetic-")),"choose a real stable source namespace")
            else:require(type(value)is int and value>=0,"initial offsets must be explicit nonnegative integers")
        owner.update(binding)
    processor=original("wildfire_config")
    processor.update(namespace=owner["satellite_namespace"],initial_offset=owner["satellite_initial_offset"],province_file="provinces.json")
    province=selected_path("province_reference")
    if province is None and config.get("province_reference")and(base/config["province_reference"]).is_file():province=(base/config["province_reference"]).resolve()
    if province is None:province=DEFAULT_PROVINCE_REFERENCE
    raw=province_bytes(province)
    processor["province_sha256"]=hashlib.sha256(raw).hexdigest()
    image_path=selected_path("images")
    images=json.loads(image_path.read_text())if image_path else original("images")
    require(isinstance(images,dict)and set(images)<=REQUIRED,"unknown image-map key")
    require(all(value is None or isinstance(value,str)and DIGEST.fullmatch(value)for value in images.values()),"images must be null or actual immutable references")
    missing=sorted(k for k in REQUIRED if not images.get(k))
    if missing:pending.append("Missing immutable images: "+", ".join(missing))
    flow_path=selected_path("external_flows")
    flows=json.loads(flow_path.read_text())if flow_path else original("external_flows")
    isolation(inventory,flows)
    if not any(f.get("app")=="ingestion"and 443 in f.get("ports",[])for f in flows):pending.append("Supply bounded satellite HTTPS egress")
    # Preserve owner-specific blockers; replace only prerequisites emitted by
    # our generators. Hardware allocation is the declared input assumption.
    extra=[s for s in config.get("unresolved",[])if not any(s.startswith(prefix)for prefix in GENERATED_PENDING)]
    config["unresolved"]=extra+pending
    for key in("owner_databases","initialization_receipt","canonical_durability","ingestion_durability"):
        if config.get(key):config[key]=str((base/config[key]).resolve())
    values={"aggregator":aggregator,"role1":owner,"services":services,"secrets":secrets,"images":images,"external-flows":flows,"wildfire":processor}
    for key,filename in(("aggregator","aggregator"),("role1","role1"),("services","services"),("secrets","secrets"),("images","images"),("external_flows","external-flows"),("wildfire_config","wildfire"),("province_reference","provinces")):config[key]=filename+".json"
    old=os.umask(0o077)
    try:
        output.mkdir(parents=True,mode=0o700)
        for name,value in values.items():write_json(output/(name+".json"),value)
        (output/"provinces.json").write_bytes(raw)
        write_json(output/"release.json",config)
        report={"version":1,"external_inputs_complete":not config["unresolved"],"pending":config["unresolved"],"capacity_assumption":"Use declared inventory allocation; existing live capacity checks run at deployment. No hardware measurements requested by this command.","internal_credentials":"preserved; only explicitly supplied external credentials updated"}
        write_json(output/"preparation.json",report)
        return report
    finally:os.umask(old)
