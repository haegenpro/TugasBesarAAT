"""Durable recovery phases; interrupted mutations reconcile their observed state."""
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import time
import uuid

from platform_lib.cluster import Cluster
from platform_lib.inventory import fingerprint,k3s_config
from replacement_lib import RESTORE,controller_holds,read_report,recovery_module,set_controller_hold,write_report
from storage_lib import require,write_json

ROOT=Path(__file__).resolve().parent.parent

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()

class PhaseJournal:
    def __init__(self,path,report):self.path=Path(path);self.report=report
    def save(self):write_report(self.path,self.report)
    def event(self,phase,**fields):
        self.report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat(),**fields});self.save();print(phase,flush=True)
    def step(self,name,action):
        step=self.report['steps'].get(name)
        if step and step['state']=='complete':return step.get('result')
        self.report['steps'][name]={'state':'running','attempts':(step or{}).get('attempts',0)+1}
        self.report['current_phase']=name;self.event(name+': intent')
        result=action()
        self.report['steps'][name].update(state='complete',result=json.loads(json.dumps(result)))
        self.event(name+': complete');return result

def reconcile_hold(cluster,namespace,key,identity,restore=False):
    current=json.loads(cluster.kubectl('get',key,'-n',namespace,'-o','json').stdout)
    require(current['metadata']['uid']==identity['uid'],'original controller was recreated during recovery')
    expected=0 if restore else identity['replicas'];desired=identity['replicas']if restore else 0
    count=current['spec'].get('replicas',1)
    if count==desired:return
    require(count==expected,'controller replica count drifted during recovery')
    set_controller_hold(cluster,namespace,key,identity,restore)

def reconcile_dcs(helper,original,identity):
    formation=helper.storage['formation'];index=helper.storage['nodes'].index(helper.target['name'])
    target=f'{formation}-etcd-{chr(97+index)}-0'
    survivor=f'{formation}-etcd-{next(chr(97+i)for i in range(3)if i!=index)}-0'
    peer=f'https://{target}.{formation}-etcd-members.{helper.storage["namespace"]}.svc.cluster.local:2380'
    old=next(m for m in original if m.get('name')==target)
    def members():return json.loads(helper.etcd(survivor,'member','list','--write-out=json').stdout)['members']
    current=members()
    survivors={m['ID']:(m['name'],m['peerURLs'])for m in original if m['ID']!=old['ID']}
    def check(values):
        require(all(any(m['ID']==key and(m.get('name'),m['peerURLs'])==value for m in values)for key,value in survivors.items()),'original DCS survivors changed')
        require(all(m['ID']in survivors or m['ID']==old['ID']or m.get('peerURLs')==[peer]for m in values),'foreign DCS membership during replacement')
    check(current)
    if any(m['ID']==old['ID']for m in current):
        helper.etcd(survivor,'member','remove',format(old['ID'],'x'));current=members();check(current)
    candidates=[m for m in current if m.get('peerURLs')==[peer]and m['ID']!=old['ID']]
    require(len(candidates)<=1,'ambiguous DCS replacement member')
    if not candidates:
        require(len(current)==2 and not any(m.get('isLearner')for m in current),'DCS learner admission state differs')
        # If the reply is lost, the next run discovers this exact peer URL.
        helper.etcd(survivor,'member','add',target,'--peer-urls='+peer,'--learner','--write-out=json')
        current=members();check(current);candidates=[m for m in current if m.get('peerURLs')==[peer]]
    require(len(candidates)==1 and candidates[0].get('isLearner')and candidates[0].get('name','')in('',target),'replacement must be the expected unpromoted DCS learner')
    new=candidates[0];names=[]
    for member in current:
        name=target if member['ID']==new['ID']else member.get('name')
        require(name,'DCS member name unavailable');names.append(name+'='+member['peerURLs'][0])
    key='statefulset/'+target[:-2];ss=json.loads(helper.cluster.kubectl('get',key,'-n',helper.storage['namespace'],'-o','json').stdout)
    require(ss['metadata']['uid']==identity['uid']and ss['spec']['replicas']==0,'DCS controller identity/hold changed')
    args=ss['spec']['template']['spec']['containers'][0]['args']
    desired=['--initial-cluster-state=existing'if arg.startswith('--initial-cluster-state=')else'--initial-cluster='+','.join(names)if arg.startswith('--initial-cluster=')else arg for arg in args]
    if desired!=args:
        patch=[{'op':'test','path':'/metadata/uid','value':identity['uid']},{'op':'test','path':'/spec/replicas','value':0},{'op':'test','path':'/spec/template/spec/containers/0/args','value':args},{'op':'replace','path':'/spec/template/spec/containers/0/args','value':desired}]
        helper.cluster.kubectl('patch',key,'-n',helper.storage['namespace'],'--type=json','-p',json.dumps(patch))
    return [survivor,new['ID']]

GUEST_MARK=r'''
import json,os,pathlib,socket,sys,tempfile
p=json.load(sys.stdin);root=pathlib.Path('/var/lib/aat');marker=root/'replacement-run.json'
if socket.gethostname()!=p['node']or not os.path.ismount(root):raise RuntimeError('replacement guest identity or data mount differs')
if marker.exists():
 if marker.is_symlink()or json.loads(marker.read_text())!=p:raise RuntimeError('guest belongs to another replacement generation')
else:
 local=root/'local-pv'
 if p['verify_only']or(local.exists()and any(local.iterdir())):raise RuntimeError('original replacement marker missing or unexpected local data')
 with tempfile.NamedTemporaryFile(mode='w',dir=root,delete=False)as f:
  json.dump(p,f);f.flush();os.fsync(f.fileno());temporary=f.name
 os.replace(temporary,marker)
'''

class ReplacementRunner:
    def __init__(self,inventory,prepared,run_id=None,cluster=None):
        require(prepared['ready'],'resolve every recovery blocker before mutation')
        self.inventory=inventory;self.prepared=prepared;self.cluster=cluster or Cluster(inventory);self.target=prepared['target'];self.ns=inventory['namespace']
        self.run_id=run_id or uuid.uuid4().hex;self.output=self.cluster.state/'evidence'/('integrated-replacement-'+self.run_id)
        if run_id:
            report=read_report(inventory,run_id,self.target['name'])
            require(report.get('version')==3 and report.get('prepared_sha256')==digest(prepared)and report.get('inventory_sha256')==fingerprint(inventory),'resume requires the original version-3 inventory, artifacts and recovery plan')
            require(not report.get('passed'),'completed replacement runs are inspected with status, not resumed')
        else:
            report={'version':3,'cluster_id':inventory['cluster_id'],'inventory_sha256':fingerprint(inventory),'prepared_sha256':digest(prepared),'run_id':self.run_id,'node':self.target['name'],'started_at':datetime.now(timezone.utc).isoformat(),'passed':False,'events':[],'steps':{},'scope':prepared['scope']}
        self.journal=PhaseJournal(self.output/'result.json',report);self.report=report;self.module=recovery_module();self.helpers={}
        for f in prepared['formations']:
            helper=self.module.Recovery(inventory,f['plan'],f['config'],self.target['name'],provider_aware=True);helper.cluster=self.cluster
            self.helpers[helper.storage['formation']]=helper
    def wait(self,check,message,seconds=300):
        deadline=time.monotonic()+seconds
        while True:
            try:return check()
            except (ValueError,KeyError,RuntimeError,subprocess.SubprocessError):
                require(time.monotonic()<deadline,message);time.sleep(2)
    def fenced(self):require(self.cluster.vm_state(self.target)=='shut off','original VM must remain fenced before disk replacement')
    def get(self,kind,name,namespace=None):
        result=self.cluster.kubectl('get',kind,name,*(['-n',namespace]if namespace else[]),'--ignore-not-found=true','-o','json')
        return json.loads(result.stdout)if result.stdout.strip()else None
    def guest(self,verify=False):
        payload={'cluster_id':self.inventory['cluster_id'],'inventory_sha256':fingerprint(self.inventory),'node':self.target['name'],'run_id':self.run_id,'verify_only':False}
        # verify_only is an execution instruction, not part of stored identity.
        script=GUEST_MARK.replace("if p['verify_only']or",'if '+str(verify)+' or')
        self.cluster.ssh(self.target,['sudo','-n','python3','-c',script],input=json.dumps(payload).encode())
    def broker(self,volume,operation):
        require(operation in('observe','verify'),'same-name recovery never removes broker membership')
        lost=volume['controller']['name']+'-0';original=['broker-'+letter+'-0'for letter in'abc'];require(lost in original,'unknown broker identity')
        survivor=next(name for name in original if name!=lost)
        result=self.cluster.kubectl('exec','-n',self.ns,survivor,'-c','broker','--','/usr/local/bin/recover-node','--operation',operation,'--server',lost,'--members',','.join(original),'--streams',','.join(volume['streams']),timeout=15)
        return json.loads(result.stdout)
    def preflight(self):
        c=self.cluster;self.fenced();require(c.doctor()['ready'],'physical hosts need recovery capacity')
        pods=json.loads(c.kubectl('get','pods','-A','--field-selector','spec.nodeName='+self.target['name'],'-o','json').stdout)['items'];known={v['claim']for v in self.prepared['volumes']};recorded=[]
        for pod in pods:
            meta=pod['metadata']
            if meta['namespace']in('kube-system','tester-agent'):continue
            require(meta['namespace']==self.ns,'foreign workload on failed node')
            for volume in pod['spec'].get('volumes',[]):
                if'persistentVolumeClaim'in volume:require(volume['persistentVolumeClaim']['claimName']in known,'unknown persistent claim on failed node')
                if'hostPath'in volume:require(meta.get('labels',{}).get('app.kubernetes.io/name')=='node-exporter'and volume['hostPath']['path']in('/','/proc','/sys'),'unknown hostPath on failed node')
            recorded.append({k:meta[k]for k in('name','namespace','uid')})
        anchors={name:helper.validate_survivors()for name,helper in self.helpers.items()}
        holds=controller_holds(c,self.prepared);members={}
        for name,helper in self.helpers.items():
            index=helper.storage['nodes'].index(self.target['name'])
            if index<3:
                pod=name+'-etcd-'+next(chr(97+i)for i in range(3)if i!=index)+'-0'
                members[name]=json.loads(helper.etcd(pod,'member','list','--write-out=json').stdout)['members']
                require(len(members[name])==3 and{m.get('name')for m in members[name]}=={name+'-etcd-'+letter+'-0'for letter in'abc'}and not any(m.get('isLearner')for m in members[name]),'fresh replacement needs the original three DCS voters')
        brokers={v['claim']:self.wait(lambda v=v:self.broker(v,'observe'),'broker quorum/system credential unavailable',60)for v in self.prepared['volumes']if v['kind']=='jetstream'}
        current=self.get('node',self.target['name']);node_uid=current['metadata']['uid']if current else None
        return {'holds':holds,'anchors':anchors,'dcs_members':members,'brokers':brokers,'pods':recorded,'node_uid':node_uid}
    def remove_node(self,baseline):
        c=self.cluster;self.fenced();current=self.get('node',self.target['name'])
        if current:
            require(current['metadata']['uid']==baseline['node_uid'],'Kubernetes node was recreated outside original replacement')
            c.kubectl('delete','node',self.target['name'],'--wait=true','--timeout=60s',timeout=70)
        for pod in baseline['pods']:
            current=self.get('pod',pod['name'],self.ns)
            if current and current['metadata']['uid']==pod['uid']:
                c.kubectl('delete','pod',pod['name'],'-n',self.ns,'--grace-period=0','--force','--ignore-not-found=true')
    def provider(self):
        self.fenced();self.survivor_prefix(self.report['steps']['preflight']['result']);status=self.cluster.replacement_status(self.target,self.run_id)
        if status['phase']=='complete':return status
        self.cluster.replace_empty(self.target,self.run_id)
        status=self.cluster.replacement_status(self.target,self.run_id);require(status['phase']=='complete','provider did not finish original replacement');return status
    def boot(self):
        c=self.cluster;require(c.replacement_status(self.target,self.run_id)['phase']=='complete','original provider generation unavailable')
        state=c.vm_state(self.target);require(state in('running','shut off'),'replacement VM has an unexpected state')
        if state=='shut off':
            c.run(['ssh-keygen','-R',self.target['address'],'-f',c.private/'known_hosts'],check=False);c.vm_action(self.target,'start')
        c.wait_ssh(self.target);self.guest()
    def join(self):
        c=self.cluster;self.guest(verify=True)
        seed=next(n for n in self.inventory['nodes']if n['role']=='server'and n['name']!=self.target['name'])
        token=c.ssh(seed,['sudo','-n','cat','/var/lib/rancher/k3s/server/token']).stdout.decode().strip()
        require(token.startswith('K10')and'::'in token,'CA-pinned original join token required');c.redactions.append(token)
        version=self.inventory['k3s'];binary=c.fetch(version['binary_url'],version['binary_sha256'],'k3s');installer=c.fetch(version['installer_url'],version['installer_sha256'],'k3s-install.sh')
        marker=c.ssh(self.target,['sudo','-n','test','-f','/var/lib/aat/platform-owner.json'],check=False)
        configured=c.ssh(self.target,['sudo','-n','test','-f','/etc/rancher/k3s/config.yaml'],check=False)
        require(marker.returncode in(0,1)and configured.returncode in(0,1),'guest configuration inspection unavailable')
        if marker.returncode==1 and configured.returncode==0:
            desired=k3s_config(self.inventory,self.target,token,False);desired['server']='https://'+seed['address']+':6443'
            expected=hashlib.sha256((json.dumps(desired,indent=2)+'\n').encode()).hexdigest()
            actual=c.ssh(self.target,['sudo','-n','sha256sum','/etc/rancher/k3s/config.yaml']).stdout.decode().split()[0]
            require(actual==expected,'unmarked partial K3s config differs from original replacement intent')
            require(c.ssh(self.target,['sudo','-n','sha256sum','/usr/local/bin/k3s']).stdout.decode().split()[0]==version['binary_sha256'],'partial K3s binary differs')
            c.upload(self.target,'/var/lib/aat/platform-owner.json',(json.dumps({'cluster_id':self.inventory['cluster_id'],'inventory_sha256':fingerprint(self.inventory)})+'\n').encode())
        c.install_node(self.target,binary,installer,token,False,join_server=seed['address'])
        marker=json.loads((c.state/'owner.json').read_text());marker['bootstrap_seed']=seed['name'];write_json(c.state/'owner.json',marker)
    def prepare_volumes(self):
        self.guest(verify=True)
        def module(name,path):
            spec=importlib.util.spec_from_file_location(name,ROOT/'scripts'/path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
        setup=module('resume_storage_setup','platform-storage.py')
        for helper in self.helpers.values():setup.prepare_disks(self.cluster,helper.storage)
        module('resume_service_setup','platform-deploy.py').prepare_volumes(self.cluster,[Path(p)for p in self.prepared['volume_requirements']])
    def restore(self,volume):
        self.guest(verify=True)
        owner=volume['controller'];key=owner['kind'].lower()+'/'+owner['name'];expected=self.report['steps']['preflight']['result']['holds'][key]
        current=json.loads(self.cluster.kubectl('get',key,'-n',self.ns,'-o','json').stdout)
        require(current['metadata']['uid']==expected['uid']and current['spec'].get('replicas',1)==0,'source controller lost its original restoration hold')
        raw=Path(volume['archive']).read_bytes()
        require(len(raw)<=64<<20 and hashlib.sha256(raw).hexdigest()==volume['sha256'],'checkpoint changed since original plan')
        self.cluster.ssh(self.target,['sudo','-n','python3','-c',RESTORE,json.dumps({k:volume[k]for k in('path','uid','sha256')})],input=raw)
    def promote(self,helper,learner):
        if not learner:return
        values=json.loads(helper.etcd(learner[0],'member','list','--write-out=json').stdout)['members']
        current=next((m for m in values if m['ID']==learner[1]),None);require(current,'original replacement learner disappeared')
        if current.get('isLearner'):helper.etcd(learner[0],'member','promote',format(learner[1],'x'))
    def survivor_prefix(self,baseline):
        observed={}
        for name,helper in self.helpers.items():
            anchors=helper.validate_survivors()
            for group in helper.plan['groups']:
                old=baseline['anchors'][name].get(str(group['id']),baseline['anchors'][name].get(group['id']))
                now=anchors[group['id']]
                require(now['system_id']==old['system_id']and self.module.lsn(now['target_lsn'])>=self.module.lsn(old['target_lsn']),'original survivor WAL prefix regressed')
                require(now['timeline']==old['timeline'],'primary timeline changed during interrupted replacement; explicit ancestry/reconciliation review required')
            observed[name]=anchors
        return observed
    def verify(self,baseline):
        observed=self.survivor_prefix(baseline)
        for name,helper in self.helpers.items():
            for group in helper.plan['groups']:
                now=observed[name][group['id']]
                def caught_up():
                    status=helper.member_status(group['replacement']);require(status['system_id']==now['system_id']and not status['primary'],'replacement identity or role differs')
                    replay=helper.sql(group['replacement'],'SELECT to_json(pg_last_wal_replay_lsn()::text)');require(replay and self.module.lsn(replay)>=self.module.lsn(now['target_lsn']),'replica has not replayed original survivor prefix')
                    receiver=helper.sql(group['replacement'],"SELECT coalesce((SELECT jsonb_build_object('host',sender_host,'timeline',received_tli,'state',status) FROM pg_stat_wal_receiver),'{}'::jsonb)")
                    leader=next(m for m in group['survivors']if m['name']==now['primary'])
                    require(receiver.get('host')==leader['host']and receiver.get('timeline')==int(now['timeline'],16)and receiver.get('state')=='streaming','replacement follows another primary/timeline')
                self.wait(caught_up,'original PostgreSQL catch-up deadline')
        for volume in self.prepared['volumes']:
            if volume['kind']!='jetstream':continue
            before=baseline['brokers'][volume['claim']]
            def healthy():
                after=self.broker(volume,'verify');streams={s['name']:s for s in after['streams']}
                for stream in before['streams']:
                    require(stream['name']in streams and streams[stream['name']]['last_sequence']>=stream['last_sequence'],'broker prefix regressed')
                    consumers={c['name']:c['ack_floor_stream']for c in streams[stream['name']].get('consumers',[])}
                    require(all(c['name']in consumers and consumers[c['name']]>=c['ack_floor_stream']for c in stream.get('consumers',[])),'durable acknowledgement prefix regressed')
                return after
            self.wait(healthy,'original JetStream catch-up deadline')
        require(controller_holds(self.cluster,self.prepared)==baseline['holds'],'original controller identities, placement or replica counts changed')
        for key in baseline['holds']:self.cluster.kubectl('rollout','status',key,'-n',self.ns,'--timeout=300s',timeout=330)
        self.cluster.verify()
    def run(self):
        c=self.cluster
        with c.locked():
            if not self.output.exists():
                for path in(c.state/'evidence').glob('integrated-replacement-*/result.json'):
                    previous=json.loads(path.read_text())
                    require(previous.get('node')!=self.target['name']or previous.get('passed')is True,'resume the original unfinished replacement instead of starting another run')
                self.output.mkdir(parents=True,mode=0o700)
            require(all(c.vm_state(n)=='running'for n in self.inventory['nodes']if n['name']!=self.target['name']),'only one failed VM is supported')
            try:
                if self.report['steps'].get('boot',{}).get('state')=='complete':
                    self.boot();self.guest(verify=True)
                baseline=self.journal.step('preflight',self.preflight)
                self.survivor_prefix(baseline)
                for key,identity in baseline['holds'].items():self.journal.step('hold:'+key,lambda key=key,identity=identity:reconcile_hold(c,self.ns,key,identity))
                learners={}
                for name,helper in self.helpers.items():
                    if name in baseline['dcs_members']:
                        index=helper.storage['nodes'].index(self.target['name']);key='statefulset/'+name+'-etcd-'+chr(97+index)
                        learners[name]=self.journal.step('dcs:'+name,lambda helper=helper,name=name,key=key:reconcile_dcs(helper,baseline['dcs_members'][name],baseline['holds'][key]))
                self.journal.step('remove-original-node',lambda:self.remove_node(baseline))
                self.journal.step('provider-empty-disks',self.provider)
                self.journal.step('boot',self.boot);self.journal.step('join',self.join)
                self.journal.step('prepare-volumes',self.prepare_volumes)
                for volume in self.prepared['volumes']:
                    if volume['kind']=='restore':self.journal.step('restore:'+volume['claim'],lambda volume=volume:self.restore(volume))
                    elif volume['kind']=='observability':self.journal.event('declared observability evidence gap',claim=volume['claim'])
                for key,identity in baseline['holds'].items():self.journal.step('release:'+key,lambda key=key,identity=identity:reconcile_hold(c,self.ns,key,identity,True))
                for name,learner in learners.items():self.journal.step('promote:'+name,lambda name=name,learner=learner:self.wait(lambda:self.promote(self.helpers[name],learner),'DCS learner catch-up deadline'))
                # Re-run final observations even if the previous final result reply was lost.
                self.verify(baseline);self.report['passed']=True;self.report.pop('error',None);self.report['resume_available']=False;self.report['completed_at']=datetime.now(timezone.utc).isoformat()
                self.journal.event('original placement restored; independent logical reconciliation still required')
            except BaseException as exc:
                self.report['error']=type(exc).__name__;self.report['resume_available']=True;raise
            finally:self.journal.save()
        return self.output
