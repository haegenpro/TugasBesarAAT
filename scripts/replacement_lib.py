"""Offline all-volume recovery planning. Unknown or unbacked data blocks mutation."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tarfile

from bundle_lib import inspect
from platform_lib.inventory import fingerprint
from storage_lib import require

ROOT=Path(__file__).resolve().parent.parent

def validate_archive(path):
    # Deliberately accept bounded uncompressed tar only; a compressed bomb or
    # symlink must be rejected before fencing/replacing a physical member.
    require(path.stat().st_size<=64<<20,"restore tar exceeds 64 MiB bound")
    count=0;size=0;names=set()
    with tarfile.open(path,mode="r:")as archive:
        for item in archive:
            count+=1;size+=item.size
            name=Path(item.name)
            require(count<=10000 and size<=64<<20,"restore archive quota exceeded")
            require(not name.is_absolute() and ".."not in name.parts and "\\"not in item.name and (item.isdir()or item.isfile()),"unsafe restore archive entry")
            require(item.name not in names,"duplicate restore archive entry");names.add(item.name)

def recovery_module():
    spec=importlib.util.spec_from_file_location("canonical_recovery",ROOT/"scripts/platform-recovery.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def validate_checkpoint(inventory,volume,policy,base):
    receipt=json.loads((base/policy['receipt']).read_text())
    require(receipt.get('version')==1 and receipt.get('writers_stopped')is True,'source checkpoint was not captured with stopped writers')
    require(receipt.get('cluster_id')==inventory['cluster_id'] and receipt.get('namespace')==inventory['namespace'] and receipt.get('inventory_sha256')==fingerprint(inventory),'source checkpoint belongs to another deployment')
    recorded=receipt['volume']
    require(all(recorded.get(k)==volume.get(k)for k in('claim','path','uid','node')),'source checkpoint volume identity differs')
    require(recorded.get('app')==volume['controller']['app'] and recorded.get('controller')==volume['controller']['name'],'source checkpoint owning controller differs')
    require(receipt.get('sha256')==policy['sha256'],'source checkpoint/archive digest differs')
    return {'captured_at':receipt['captured_at'],'sha256':receipt['sha256'],'sqlite':receipt.get('sqlite',[]),'scope':receipt.get('scope')}

def load_plan(inventory,path,node_name):
    require(inventory.get("version")==2 and len(inventory.get("hosts",[]))==4,"integrated replacement requires four physical provider hosts")
    path=Path(path).resolve();base=path.parent;config=json.loads(path.read_text())
    require(config.get("version")==1,"replacement configuration version must be 1")
    target=next((n for n in inventory["nodes"]if n["name"]==node_name),None);require(target,"unknown replacement node")
    bundle,snapshots=inspect(base/config["bundle"],inventory)
    blockers=list(bundle["blocked"]);module=recovery_module();formations=[];storage_claims=set()
    for item in config["formations"]:
        storage=json.loads((base/item["plan"]).read_text());captured=json.loads((base/item["config"]).read_text())
        module.validate_config(storage,captured)
        planned=module.recovery_plan(inventory,storage,node_name,provider_aware=True)
        formations.append({"plan":storage,"config":captured,"recovery":planned})
        storage_claims.update(m["workload"]for g in storage["groups"]for m in g["members"]if m["node"]==node_name)
        index=storage["nodes"].index(node_name)
        if index<3:storage_claims.add(storage["formation"]+"-etcd-"+chr(97+index))
    require({f["plan"]["formation"]for f in formations}>={inventory["cluster_id"]+"-storage",inventory["cluster_id"]+"-ingestion"},"canonical and ingestion recovery configurations required")
    identities=[g["system_id"]for f in formations for g in f["config"]["groups"]]
    require(len(set(identities))==len(identities),"physical database identity reused between owned formations")
    volume_requirements={}
    for filename in config.get("volume_requirements",[]):
        for v in json.loads((base/filename).read_text())["local_volumes"]:
            require(v["claim"]not in volume_requirements,"duplicate local-volume requirement")
            volume_requirements[v["claim"]]=v
    objects=[o for raw in snapshots.values()for o in json.loads(raw)["items"]]
    controllers={};volumes=[]
    for obj in objects:
        if obj["kind"]not in("StatefulSet","Deployment"):continue
        pod=obj["spec"]["template"]["spec"]
        for v in pod.get("volumes",[]):
            claim=v.get("persistentVolumeClaim",{}).get("claimName")
            if claim:controllers[claim]={"kind":obj["kind"],"name":obj["metadata"]["name"],"app":obj["spec"]["template"]["metadata"]["labels"].get("app.kubernetes.io/name"),"replicas":obj["spec"].get("replicas",1)}
    for pv in (o for o in objects if o["kind"]=="PersistentVolume"):
        spec=pv["spec"];terms=spec.get("nodeAffinity",{}).get("required",{}).get("nodeSelectorTerms",[])
        assigned={value for term in terms for expr in term.get("matchExpressions",[])if expr.get("key")=="kubernetes.io/hostname"and expr.get("operator")=="In"for value in expr["values"]}
        if node_name not in assigned:continue
        require(assigned=={node_name},"recovery requires exactly one original node per local volume")
        claim=spec["claimRef"]["name"];controller=controllers.get(claim);require(controller,"local volume has no known owning workload")
        volume={"claim":claim,"path":spec["local"]["path"],"node":node_name,"controller":controller}
        if claim in storage_claims:volume["kind"]="postgres"
        else:
            declared=volume_requirements.get(claim)
            require(declared and declared["node"]==node_name and declared["path"]==volume["path"],"service volume ownership/placement differs")
            volume["uid"]=declared["uid"]
            policy=config.get("volume_policy",{}).get(claim,{})
            kind=policy.get("kind")
            if kind=="jetstream":
                require(controller["app"]=="broker","JetStream recovery only applies to broker data")
                require(policy.get("streams")and len(set(policy["streams"]))==len(policy["streams"]),"explicit distinct broker streams required")
                volume.update(kind=kind,streams=policy["streams"])
            elif kind=="observability":
                require(controller["app"]in("prometheus","loki","tempo","grafana"),"only declared observability data may be recreated without restoration")
                volume["kind"]=kind
            elif kind=="restore":
                archive=(base/policy.get("archive","")).resolve()
                if not archive.is_file():blockers.append("missing restore artifact for "+claim)
                else:
                    require(archive.stat().st_size<=64<<20,"restore archive exceeds 64 MiB compressed bound")
                    with archive.open("rb")as f:actual=hashlib.file_digest(f,"sha256").hexdigest()
                    require(actual==policy.get("sha256"),"restore artifact hash differs for "+claim)
                    validate_archive(archive)
                volume.update(kind=kind,archive=str(archive),sha256=policy.get("sha256"))
                if policy.get('receipt'):volume['checkpoint']=validate_checkpoint(inventory,volume,policy,base)
            else:
                volume["kind"]="unresolved";blockers.append("restore/rebuild policy required for "+claim)
        volumes.append(volume)
    require(storage_claims<={v["claim"]for v in volumes},"bundle omits original storage replicas")
    return {"version":1,"target":target,"ready":not blockers,"blocked":blockers,"formations":formations,"volumes":volumes,"volume_requirements":[str(base/p)for p in config.get("volume_requirements",[])],"namespace":inventory["namespace"],"scope":"Fenced empty-VM recovery; no accepted-data or restore success is inferred from a healthy pod"}

RESTORE=r'''
import hashlib,io,json,os,pathlib,sys,tarfile
p=json.loads(sys.argv[1]);root=pathlib.Path(p['path'])
if root.is_symlink() or not root.is_dir() or root.stat().st_uid!=p['uid'] or any(root.iterdir()):raise RuntimeError('restore requires empty owned volume')
raw=sys.stdin.buffer.read((64<<20)+1)
if len(raw)>64<<20 or hashlib.sha256(raw).hexdigest()!=p['sha256']:raise RuntimeError('restore artifact mismatch')
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:')as archive:
    entries=[];total=0
    for entry in archive:
        total+=entry.size
        if len(entries)>=10000 or total>64<<20:raise RuntimeError('restore quota exceeded')
        entries.append(entry)
        name=pathlib.PurePosixPath(entry.name)
        if name.is_absolute() or '..'in name.parts or '\\'in entry.name or not(entry.isdir()or entry.isfile()):raise RuntimeError('unsafe archive entry')
        target=root.joinpath(*name.parts)
        if not target.resolve().is_relative_to(root.resolve()):raise RuntimeError('restore escapes volume')
    for entry in entries:
        target=root.joinpath(*pathlib.PurePosixPath(entry.name).parts)
        if entry.isdir():target.mkdir(parents=True,exist_ok=True)
        else:
            target.parent.mkdir(parents=True,exist_ok=True)
            with archive.extractfile(entry)as source,target.open('xb')as destination:
                while True:
                    block=source.read(65536)
                    if not block:break
                    destination.write(block)
        os.chown(target,p['uid'],p['uid'])
        os.chmod(target,0o700 if entry.isdir()else 0o600)
    for directory in root.rglob('*'):
        if directory.is_dir():os.chown(directory,p['uid'],p['uid']);os.chmod(directory,0o700)
'''
