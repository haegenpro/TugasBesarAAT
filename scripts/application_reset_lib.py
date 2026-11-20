"""Explicit application-only reset for existing VMs; preserve K3s and node identity."""
import json
from pathlib import Path
import re
import uuid

from platform_lib.inventory import fingerprint
from service_lib import DIGEST
from storage_lib import require, write_json


GUEST_RESET = r'''
import json,os,pathlib,shutil
data=json.loads(os.environ['RESET_INPUT'])
base=pathlib.Path('/var/lib/aat')
if os.geteuid()!=0:raise RuntimeError('root required for owned local volume maintenance')
for p in (base,base/'local-pv'):
    if p.is_symlink() or os.path.realpath(p)!=str(p) or p.stat().st_uid!=0 or p.stat().st_mode&0o022:
        raise RuntimeError('unsafe local volume ancestor')
expected={'cluster_id':data['cluster_id'],'provider':'existing','node':data['node'],'node_uid':data['node_uid']}
marker=base/'platform-owner.json'
if marker.is_symlink() or marker.stat().st_uid!=0 or json.loads(marker.read_text())!=expected:
    raise RuntimeError('guest ownership or Kubernetes node identity differs')
if shutil.disk_usage(base).free<2*2**30:raise RuntimeError('less than 2 GiB actual free space')
checked=[]
for volume in data['volumes']:
    p=pathlib.Path(volume['path']);formation=p.parent
    if p.parent.parent!=base/'local-pv' or not formation.name.startswith(data['cluster_id']+'-'):
        raise RuntimeError('unowned volume path')
    owner=formation/'.owner.json'
    if formation.is_symlink() or owner.is_symlink() or json.loads(owner.read_text())!={'cluster_id':data['cluster_id'],'formation':formation.name}:
        raise RuntimeError('formation ownership differs')
    if p.is_symlink() or not p.is_dir() or p.stat().st_uid!=volume['uid']:
        raise RuntimeError('volume ownership or type differs')
    checked.append((p,volume['uid']))
if not data['check_only']:
    for p,uid in checked:
        shutil.rmtree(p)
        p.mkdir(mode=0o700);os.chown(p,uid,uid)
print(json.dumps({'checked':True,'reset':not data['check_only'],'node':data['node'],'volumes':len(checked),'data_archive':False}))
'''


def build_plan(inventory, namespace, nodes, pvs, desired, image):
    require(inventory.get('version') == 3, 'application reset requires existing-cluster inventory')
    require(isinstance(image, str) and DIGEST.fullmatch(image), 'immutable maintenance image required')
    ns=inventory['namespace'];cluster=inventory['cluster_id']
    require(ns not in ('default','kube-system','kube-public','kube-node-lease','tester-agent') and namespace['metadata']['name']==ns, 'assignment namespace required')
    expected={n['name']:n['uid'] for n in inventory['nodes']}
    actual={n['metadata']['name']:n['metadata']['uid'] for n in nodes if n['metadata']['name'] in expected}
    require(actual==expected, 'Kubernetes node identities differ from the inventory')
    volumes={(v['node'],v['path']):v for v in desired}
    selected=[];seen=set()
    for pv in pvs:
        spec=pv['spec'];ref=spec.get('claimRef',{})
        if ref.get('namespace')!=ns:continue
        name=pv['metadata']['name'];path=Path(spec.get('local',{}).get('path',''))
        require(name.startswith(cluster+'-') and path.parent.parent==Path('/var/lib/aat/local-pv')
                and path.parent.name in {cluster+'-'+s for s in ('storage','ingestion','service-db','services')}
                and re.fullmatch(re.escape(cluster)+r'-[a-z0-9-]+',path.name), 'PV is outside owned application storage')
        terms=spec['nodeAffinity']['required']['nodeSelectorTerms']
        require(len(terms)==1 and len(terms[0].get('matchExpressions',[]))==1 and not terms[0].get('matchFields'), 'ambiguous PV node affinity')
        expression=terms[0]['matchExpressions'][0]
        require(expression['key']=='kubernetes.io/hostname' and expression['operator']=='In' and len(expression['values'])==1, 'one explicit PV node required')
        node=expression['values'][0];key=(node,str(path))
        require(key in volumes and node in expected and key not in seen, 'PV differs from the reviewed volume plan')
        seen.add(key)
        selected.append({'name':name,'uid':pv['metadata']['uid'],'node':node,'path':str(path),'owner_uid':volumes[key]['uid']})
    require(selected and seen==set(volumes), 'live application volumes differ from the complete fresh volume plan')
    return {'version':1,'run_id':uuid.uuid4().hex[:12],'cluster_id':cluster,'namespace':ns,'namespace_uid':namespace['metadata']['uid'],
            'inventory_sha256':fingerprint(inventory),'node_uids':expected,'image':image,'volumes':sorted(selected,key=lambda v:v['name']),
            'scope':'Delete the assignment namespace and its exact local PV objects; delete old volume contents and recreate empty owned directories without data archives. Preserve VMs, K3s, node UIDs and Tester Agent.'}


def guest_job(inventory, plan, node, check_only):
    app=inventory['cluster_id'];run=plan['run_id'];name=app+'-'+('check' if check_only else 'reset')+'-'+node['name'].rsplit('-',1)[-1]+'-'+run
    data={'cluster_id':app,'run_id':run,'node':node['name'],'node_uid':node['uid'],'check_only':check_only,
          'volumes':[{'path':v['path'],'uid':v['owner_uid']}for v in plan['volumes']if v['node']==node['name']]}
    return {'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':app+'-maintenance','labels':{'platform.aat/reset':run}},
            'spec':{'backoffLimit':0,'activeDeadlineSeconds':600,'template':{'metadata':{'labels':{'platform.aat/reset':run}},'spec':{
                'nodeSelector':{'kubernetes.io/hostname':node['name']},'automountServiceAccountToken':False,'restartPolicy':'Never',
                'containers':[{'name':'volumes','image':plan['image'],'command':['/opt/patroni/bin/python','-c',GUEST_RESET],
                    'env':[{'name':'RESET_INPUT','value':json.dumps(data,separators=(',',':'))}],
                    'resources':{'requests':{'cpu':'100m','memory':'128Mi'},'limits':{'cpu':'1','memory':'256Mi'}},
                    'securityContext':{'runAsUser':0,'runAsGroup':0,'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,
                        'capabilities':{'drop':['ALL'],'add':['CHOWN','DAC_OVERRIDE','FOWNER']}},
                    'volumeMounts':[{'name':'data','mountPath':'/var/lib/aat','readOnly':check_only},{'name':'tmp','mountPath':'/tmp'}]}],
                'volumes':[{'name':'data','hostPath':{'path':'/var/lib/aat','type':'Directory'}},{'name':'tmp','emptyDir':{'sizeLimit':'32Mi'}}]}}}}


def apply(cluster, plan, output, *, delete_data):
    require(delete_data, 'explicit --delete-data required')
    require(plan['inventory_sha256']==fingerprint(cluster.cfg), 'reset plan inventory changed')
    output=Path(output);require(not output.exists(), 'reset output exists; preserve recovery records')
    output.mkdir(parents=True,mode=0o700)
    ns=plan['namespace'];admin=plan['cluster_id']+'-maintenance'
    current=json.loads(cluster.kubectl('get','namespace',ns,'-o','json').stdout)
    require(current['metadata']['uid']==plan['namespace_uid'], 'namespace identity changed since planning')
    nodes=json.loads(cluster.kubectl('get','nodes','-o','json').stdout)['items']
    require({n['metadata']['name']:n['metadata']['uid']for n in nodes if n['metadata']['name']in plan['node_uids']}==plan['node_uids'], 'node identities changed')
    live=json.loads(cluster.kubectl('get','pv','-o','json').stdout)['items']
    require({p['metadata']['name']:p['metadata']['uid']for p in live if p['spec'].get('claimRef',{}).get('namespace')==ns}=={v['name']:v['uid']for v in plan['volumes']}, 'PV identities changed')
    write_json(output/'plan.json',plan)
    write_json(output/'namespace-before.json',json.loads(cluster.kubectl('get','all,configmap,secret,networkpolicy,pvc,serviceaccount,role,rolebinding,poddisruptionbudget','-n',ns,'-o','json').stdout))
    write_json(output/'pv-before.json',{'items':[p for p in live if p['metadata']['name']in {v['name']for v in plan['volumes']}]})
    existing=cluster.kubectl('get','namespace',admin,'--ignore-not-found','-o','json').stdout
    require(not existing.strip(), 'maintenance namespace already exists; inspect its owner before resetting')
    cluster.kubectl('create','-f','-',input=json.dumps({'apiVersion':'v1','kind':'Namespace','metadata':{'name':admin,'labels':{'platform.aat/reset':plan['run_id']}}}).encode())
    def jobs(check_only):
        prepared=[]
        for node in cluster.cfg['nodes']:
            obj=guest_job(cluster.cfg,plan,node,check_only);name=obj['metadata']['name']
            cluster.kubectl('create','-f','-',input=json.dumps(obj).encode());prepared.append(name)
        for name in prepared:
            cluster.kubectl('wait','--for=condition=Complete','job/'+name,'-n',admin,'--timeout=600s',timeout=630)
            (output/(name+'.log')).write_bytes(cluster.kubectl('logs','job/'+name,'-n',admin).stdout)
    jobs(True)
    print('All four guest ownership and disk preflights passed',flush=True)
    cluster.kubectl('delete','namespace',ns,'--wait=true','--timeout=240s',timeout=270)
    cluster.kubectl('delete','pv',*[v['name']for v in plan['volumes']],'--wait=true','--timeout=120s',timeout=150)
    jobs(False)
    cluster.kubectl('delete','namespace',admin,'--wait=true','--timeout=120s',timeout=150)
    write_json(output/'result.json',{'passed':True,'run_id':plan['run_id'],'volumes_reset':len(plan['volumes']),'node_uids':plan['node_uids'],'data_archived':False})
    print('Application namespace and data reset; VM and K3s node identities preserved',flush=True)
