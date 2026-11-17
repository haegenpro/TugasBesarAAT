"""Plan and capture source checkpoints without ever stopping or starting a writer."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path

from bundle_lib import inspect
from platform_lib.inventory import fingerprint
from replacement_lib import validate_archive
from storage_lib import require,write_json

ROOT=Path(__file__).resolve().parent.parent
SOURCE_APPS={'bmkg-mock','pvmbg-mock','sensor-pantai'}

def plan(inventory,bundle_path,requirements,claims=()):
    report,snapshots=inspect(bundle_path,inventory)
    objects=[o for raw in snapshots.values()for o in json.loads(raw)['items']]
    declared={}
    for path in requirements:
        for volume in json.loads(Path(path).read_text())['local_volumes']:
            require(volume['claim']not in declared,'duplicate volume requirement')
            declared[volume['claim']]=volume
    volumes=[];seen=set()
    for obj in objects:
        if obj['kind']!='StatefulSet':continue
        template=obj['spec']['template'];app=template['metadata']['labels'].get('app.kubernetes.io/name')
        if app not in SOURCE_APPS:continue
        for mount in template['spec'].get('volumes',[]):
            claim=mount.get('persistentVolumeClaim',{}).get('claimName')
            if not claim or(claims and claim not in claims):continue
            require(claim not in seen,'source claim is shared between controllers');seen.add(claim)
            require(claim in declared,'source volume missing from requirements');volume=declared[claim]
            node=template['spec']['nodeSelector']['kubernetes.io/hostname']
            require(node==volume['node'] and any(n['name']==node for n in inventory['nodes']),'source volume node differs')
            expected='/var/lib/aat/local-pv/'+inventory['cluster_id']+'-services/'+claim
            require(volume['path']==expected and type(volume['uid'])is int and volume['uid']>0,'source volume path/owner differs')
            pv=next((o for o in objects if o['kind']=='PersistentVolume'and o['spec'].get('claimRef',{}).get('name')==claim),None)
            require(pv and pv['spec'].get('local',{}).get('path')==expected and pv['spec']['claimRef']['namespace']==inventory['namespace'],'source PV differs from requirement')
            volumes.append({**volume,'app':app,'controller':obj['metadata']['name'],'pv':pv['metadata']['name']})
    require(volumes and (not claims or set(claims)==seen),'requested source claims absent from bundle')
    return {'version':1,'cluster_id':inventory['cluster_id'],'inventory_sha256':fingerprint(inventory),'namespace':inventory['namespace'],'volumes':volumes,'bundle_blockers':report['blocked'],'scope':'consistent stopped-writer checkpoints, not continuous replication or zero-loss acceptance'}

def stopped(cluster,prepared,volume,uid=None):
    ns=prepared['namespace'];name=volume['controller']
    current=json.loads(cluster.kubectl('get','statefulset',name,'-n',ns,'-o','json').stdout)
    require(current['metadata'].get('labels',{}).get('platform.aat/cluster')==prepared['cluster_id'],'live source controller belongs to another cluster')
    pod=current['spec']['template'];labels=pod['metadata']['labels']
    require(labels.get('app.kubernetes.io/name')==volume['app'] and current['spec'].get('replicas',1)==0,'owning source controller must already be scaled to zero')
    require(uid is None or current['metadata']['uid']==uid,'source controller was replaced during capture')
    require(pod['spec'].get('nodeSelector',{}).get('kubernetes.io/hostname')==volume['node'],'live source placement differs')
    require(any(v.get('persistentVolumeClaim',{}).get('claimName')==volume['claim']for v in pod['spec'].get('volumes',[])),'live source claim differs')
    pods=json.loads(cluster.kubectl('get','pods','-n',ns,'-o','json').stdout)['items']
    require(not any(any(v.get('persistentVolumeClaim',{}).get('claimName')==volume['claim']for v in p['spec'].get('volumes',[]))for p in pods),'a pod still references the source volume')
    claim=json.loads(cluster.kubectl('get','pvc',volume['claim'],'-n',ns,'-o','json').stdout)
    require(claim['spec']['volumeName']==volume['pv'] and claim['status']['phase']=='Bound','live source claim binding differs')
    pv=json.loads(cluster.kubectl('get','pv',volume['pv'],'-o','json').stdout)
    spec=pv['spec'];ref=spec['claimRef']
    require(spec.get('local',{}).get('path')==volume['path'] and ref['name']==volume['claim'] and ref['namespace']==ns and ref['uid']==claim['metadata']['uid'],'live source PV ownership differs')
    assigned={v for term in spec.get('nodeAffinity',{}).get('required',{}).get('nodeSelectorTerms',[])for expr in term.get('matchExpressions',[])if expr.get('key')=='kubernetes.io/hostname'and expr.get('operator')=='In'for v in expr['values']}
    require(assigned=={volume['node']},'live source PV placement differs')
    return current['metadata']['uid']

def capture(cluster,prepared,output):
    output=Path(output)
    require(not output.exists(),'backup output already exists; keep immutable checkpoints')
    resolved=output.resolve()
    require(not resolved.is_relative_to(ROOT)or resolved.is_relative_to(ROOT/'evidence'),'repository-local backups must stay under ignored evidence/')
    # Check every writer before creating an accepted artifact or accessing data.
    identities={v['claim']:stopped(cluster,prepared,v)for v in prepared['volumes']}
    output.mkdir(parents=True,mode=0o700)
    records=[]
    try:
        script=(ROOT/'deployment/platform/capture_source.py').read_text()
        for volume in prepared['volumes']:
            node=next(n for n in cluster.cfg['nodes']if n['name']==volume['node'])
            cluster.ssh(node,['findmnt','--mountpoint','/var/lib/aat'])
            response=cluster.ssh(node,['sudo','-n','python3','-c',script],input=json.dumps({**volume,'cluster_id':prepared['cluster_id']}).encode(),timeout=90).stdout
            header,raw=response.split(b'\n',1)
            require(len(header)<=1<<20 and len(raw)<=64<<20,'source capture response exceeds quota')
            metadata=json.loads(header)
            require(metadata.get('version')==1 and metadata['bytes']==len(raw)and metadata['sha256']==hashlib.sha256(raw).hexdigest(),'source capture digest/length differs')
            stopped(cluster,prepared,volume,identities[volume['claim']])
            partial=output/(volume['claim']+'.tar.partial');partial.write_bytes(raw);partial.chmod(0o600)
            validate_archive(partial)
            archive=output/(volume['claim']+'.tar');partial.rename(archive)
            receipt={**metadata,'cluster_id':prepared['cluster_id'],'inventory_sha256':prepared['inventory_sha256'],'namespace':prepared['namespace'],'volume':volume,'controller_uid':identities[volume['claim']],'captured_at':datetime.now(timezone.utc).isoformat(),'writers_stopped':True,'scope':prepared['scope']}
            receipt_path=output/(volume['claim']+'.receipt.json');write_json(receipt_path,receipt)
            records.append({'claim':volume['claim'],'archive':archive.name,'sha256':metadata['sha256'],'receipt':receipt_path.name})
    finally:write_json(output/'checkpoints.json',{'version':1,'complete':len(records)==len(prepared['volumes']),'checkpoints':records,'scope':prepared['scope']})
    return records
