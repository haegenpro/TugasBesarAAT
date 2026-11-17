"""Offline all-volume recovery planning. Unknown or unbacked data blocks mutation."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tarfile
import tempfile

from bundle_lib import inspect
from platform_lib.inventory import fingerprint
from storage_lib import require

ROOT=Path(__file__).resolve().parent.parent

def write_report(path,value):
    """Durably replace a private report without exposing a truncated journal."""
    path=Path(path);temporary=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',dir=path.parent,prefix='.replacement-',delete=False)as stream:
            temporary=Path(stream.name);json.dump(value,stream,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if temporary is not None:temporary.unlink(missing_ok=True)

def controller_holds(cluster,prepared):
    """Inspect every lost-member controller before any replica-count mutation."""
    result={};ns=prepared['namespace'];target=prepared['target']['name']
    formations={f['plan']['formation']for f in prepared['formations']}
    def owned(labels):
        cluster_label=labels.get('platform.aat/cluster');formation=labels.get('platform.aat/formation')
        return cluster_label in(None,cluster.cfg['cluster_id'])and(formation is None or formation in formations)and(cluster_label==cluster.cfg['cluster_id']or formation in formations)
    for volume in prepared['volumes']:
        owner=volume['controller'];key=owner['kind'].lower()+'/'+owner['name']
        current=json.loads(cluster.kubectl('get',key,'-n',ns,'-o','json').stdout)
        meta=current['metadata'];labels=meta.get('labels',{})
        require(meta['name']==owner['name']and meta['namespace']==ns and current['kind']==owner['kind'],'live persistent controller identity differs')
        require(owned(labels),'refusing to hold an unowned or conflicting controller')
        pod=current['spec']['template']
        require(owned(pod['metadata']['labels']),'persistent pod ownership differs from controller')
        require(pod['spec'].get('nodeSelector',{}).get('kubernetes.io/hostname')==target,'persistent controller moved off the lost node')
        require(pod['metadata']['labels'].get('app.kubernetes.io/name')==owner['app'],'persistent controller owning app differs')
        require(any(v.get('persistentVolumeClaim',{}).get('claimName')==volume['claim']for v in pod['spec'].get('volumes',[])),'persistent controller no longer owns its recorded claim')
        count=current['spec'].get('replicas',1)
        require(type(count)is int and count>=0 and isinstance(meta.get('uid'),str)and meta['uid'],'invalid live controller replica count/UID')
        identity={'uid':meta['uid'],'replicas':count}
        require(key not in result or result[key]==identity,'persistent controller changed during preflight')
        result[key]=identity
    return result

def set_controller_hold(cluster,namespace,key,identity,restore=False):
    # JSON Patch tests and mutation execute atomically at the API server. Status
    # updates can advance resourceVersion without invalidating this identity test.
    expected=0 if restore else identity['replicas'];desired=identity['replicas']if restore else 0
    patch=[{'op':'test','path':'/metadata/uid','value':identity['uid']},
           {'op':'test','path':'/spec/replicas','value':expected},
           {'op':'replace','path':'/spec/replicas','value':desired}]
    cluster.kubectl('patch',key,'-n',namespace,'--type=json','-p',json.dumps(patch))

def read_report(inventory,run_id,node_name):
    import re
    require(isinstance(run_id,str)and re.fullmatch(r'[0-9a-f]{32}',run_id),'original 32-character recovery run ID required')
    root=Path(inventory['state_dir'])/'evidence'/('integrated-replacement-'+run_id)
    require(root.resolve()==root and not(root/'result.json').is_symlink(),'recovery report path must not use symlinks')
    value=json.loads((root/'result.json').read_text())
    require(value.get('run_id')==run_id and value.get('node')==node_name and value.get('cluster_id')==inventory['cluster_id'],'recovery report belongs to another run/node/cluster')
    return value

def validate_archive(path):
    # Deliberately accept bounded uncompressed tar only; a compressed bomb or
    # symlink must be rejected before fencing/replacing a physical member.
    require(path.stat().st_size<=64<<20,"restore tar exceeds 64 MiB bound")
    count=0;size=0;names={}
    with tarfile.open(path,mode="r:")as archive:
        for item in archive:
            count+=1;size+=item.size
            name=Path(item.name)
            require(count<=10000 and size<=64<<20,"restore archive quota exceeded")
            require(not name.is_absolute() and name.as_posix()not in('','.','/') and ".."not in name.parts and "\\"not in item.name and (item.isdir()or item.isfile()),"unsafe restore archive entry")
            require(name.as_posix()not in names,"duplicate restore archive entry");names[name.as_posix()]=item.isdir()
        for value in names:
            require(all(parent.as_posix()not in names or names[parent.as_posix()]for parent in Path(value).parents),'archive file shadows a parent directory')

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

RESTORE=(ROOT/'deployment/platform/restore_source.py').read_text()
