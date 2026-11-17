"""Fault-injected phase replay with real journals/restores and simulated providers."""
import copy
from contextlib import contextmanager,redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
import unittest

from platform_lib.inventory import load
from replacement_resume_lib import ReplacementRunner,reconcile_dcs

ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('resume_restore',ROOT/'deployment/platform/restore_source.py')
restore=importlib.util.module_from_spec(spec);spec.loader.exec_module(restore)

class ProviderFixture:
    def __init__(self,inventory):
        self.cfg=inventory;self.state=Path(inventory['state_dir']);self.counts={};self.vm='shut off';self.data=None
        self.controller={'metadata':{'uid':'original'},'spec':{'replicas':1}}
    @contextmanager
    def locked(self):
        (self.state/'evidence').mkdir(parents=True,exist_ok=True);yield
    def vm_state(self,node):return self.vm if node['name']==self.cfg['nodes'][0]['name']else'running'
    def kubectl(self,*args,**kwargs):
        if args[0]=='get':return SimpleNamespace(stdout=json.dumps(self.controller).encode())
        if args[0]=='patch':
            candidate=copy.deepcopy(self.controller)
            for op in json.loads(args[-1]):
                left,right=op['path'].strip('/').split('/')
                if op['op']=='test'and candidate[left][right]!=op['value']:raise RuntimeError('concurrent controller change')
                if op['op']=='replace':candidate[left][right]=op['value']
            self.controller=candidate;self.counts['patch']=self.counts.get('patch',0)+1
            if candidate['spec']['replicas']==1:self.data.write_bytes(b'new accepted source state')
            return SimpleNamespace(stdout=b'')
        raise AssertionError(args)

class RunnerFixture(ReplacementRunner):
    def once(self,name):self.cluster.counts[name]=1
    def preflight(self):return {'holds':{'statefulset/source-a':{'uid':'original','replicas':1}},'anchors':{},'dcs_members':{},'brokers':{},'pods':[],'node_uid':'old'}
    def remove_node(self,baseline):self.once('remove-node')
    def provider(self):self.once('provider');return {'phase':'complete'}
    def boot(self):self.once('boot');self.cluster.vm='running'
    def join(self):self.once('join')
    def prepare_volumes(self):self.once('prepare')
    def guest(self,verify=False):pass
    def restore(self,volume):restore.restore({k:volume[k]for k in('path','uid','sha256')},Path(volume['archive']).read_bytes())
    def verify(self,baseline):
        if self.cluster.controller!={'metadata':{'uid':'original'},'spec':{'replicas':1}}:raise ValueError('original controller not restored')
        if self.cluster.data.read_bytes()!=b'new accepted source state':raise ValueError('post-restore source writes overwritten')

class ResumeEngineChecks(unittest.TestCase):
    def setUp(self):
        redirect=redirect_stdout(io.StringIO());redirect.__enter__();self.addCleanup(redirect.__exit__,None,None,None)
    def fixture(self,folder):
        inventory=load(ROOT/'deployment/platform/inventory.fourhost.example.json');inventory['state_dir']=str(Path(folder)/'state')
        target=inventory['nodes'][0];volume=Path(folder)/'volume';volume.mkdir()
        buffer=io.BytesIO()
        with tarfile.open(fileobj=buffer,mode='w')as archive:
            entry=tarfile.TarInfo('source.db');entry.size=10;archive.addfile(entry,io.BytesIO(b'checkpoint'))
        raw=buffer.getvalue();archive=Path(folder)/'backup.tar';archive.write_bytes(raw)
        prepared={'ready':True,'target':target,'namespace':inventory['namespace'],'formations':[],'volumes':[{'kind':'restore','claim':'source-claim','path':str(volume),'uid':os.getuid(),'sha256':hashlib.sha256(raw).hexdigest(),'archive':str(archive)}],'volume_requirements':[],'scope':'phase fault fixture'}
        cluster=ProviderFixture(inventory);cluster.data=volume/'source.db'
        return inventory,prepared,cluster

    def test_lost_completion_at_each_mutating_phase_resumes_without_replacing_again(self):
        phases=['hold:statefulset/source-a','remove-original-node','provider-empty-disks','boot','join','prepare-volumes','restore:source-claim','release:statefulset/source-a']
        for phase in phases:
            with self.subTest(phase=phase),tempfile.TemporaryDirectory()as folder:
                inventory,prepared,cluster=self.fixture(folder);runner=RunnerFixture(inventory,prepared,cluster=cluster)
                save=runner.journal.save
                def fail_completion():
                    if runner.report.get('current_phase')==phase and runner.report['steps'].get(phase,{}).get('state')=='complete':raise OSError('injected lost durable completion')
                    save()
                runner.journal.save=fail_completion
                with self.assertRaises(OSError):runner.run()
                saved=json.loads((runner.output/'result.json').read_text());self.assertEqual(saved['steps'][phase]['state'],'running')
                with self.assertRaisesRegex(ValueError,'original unfinished'):RunnerFixture(inventory,prepared,cluster=cluster).run()
                resumed=RunnerFixture(inventory,prepared,runner.run_id,cluster=cluster);resumed.run()
                self.assertTrue(json.loads((runner.output/'result.json').read_text())['passed'])
                self.assertEqual(cluster.counts['provider'],1);self.assertEqual(cluster.counts['patch'],2)
                self.assertEqual(cluster.data.read_bytes(),b'new accepted source state')

    def test_resume_refuses_changed_plan_or_recreated_controller(self):
        with tempfile.TemporaryDirectory()as folder:
            inventory,prepared,cluster=self.fixture(folder);runner=RunnerFixture(inventory,prepared,cluster=cluster)
            save=runner.journal.save
            def fail():
                if runner.report.get('current_phase')=='hold:statefulset/source-a'and runner.report['steps']['hold:statefulset/source-a']['state']=='complete':raise OSError('lost hold completion')
                save()
            runner.journal.save=fail
            with self.assertRaises(OSError):runner.run()
            changed=copy.deepcopy(prepared);changed['scope']='different artifact set'
            with self.assertRaisesRegex(ValueError,'original version-3'):RunnerFixture(inventory,changed,runner.run_id,cluster=cluster)
            cluster.controller['metadata']['uid']='foreign'
            with self.assertRaisesRegex(ValueError,'recreated'):RunnerFixture(inventory,prepared,runner.run_id,cluster=cluster).run()
            self.assertNotIn('provider',cluster.counts)

    def test_real_provider_phase_observes_lost_reply_instead_of_wiping_again(self):
        with tempfile.TemporaryDirectory()as folder:
            inventory,prepared,cluster=self.fixture(folder);cluster.provider_record={'phase':'absent'};cluster.replacements=0
            cluster.replacement_status=lambda target,run:cluster.provider_record
            def replace(target,run):
                cluster.replacements+=1;cluster.provider_record={'phase':'complete','run_id':run};raise RuntimeError('lost provider completion reply')
            cluster.replace_empty=replace
            runner=ReplacementRunner(inventory,prepared,cluster=cluster);runner.report['steps']['preflight']={'result':{'anchors':{}}}
            with self.assertRaises(RuntimeError):runner.provider()
            self.assertEqual(runner.provider()['phase'],'complete');self.assertEqual(cluster.replacements,1)

class DCSFixture:
    def __init__(self):
        self.storage={'formation':'fixture-storage','namespace':'assignment','nodes':['node-a','node-b','node-c','node-d']};self.target={'name':'node-a'}
        self.current=[{'ID':i+1,'name':'fixture-storage-etcd-'+letter+'-0','peerURLs':['https://fixture-storage-etcd-'+letter+'-0.fixture-storage-etcd-members.assignment.svc.cluster.local:2380'],'isLearner':False}for i,letter in enumerate('abc')]
        self.original=copy.deepcopy(self.current);self.uid='original-dcs';self.args=['--initial-cluster-state=new','--initial-cluster=old'];self.cluster=self
        self.remove_count=0;self.add_count=0;self.promote_count=0;self.lose=None
    def etcd(self,pod,*args):
        action=args[1]
        if action=='remove':
            self.current=[m for m in self.current if m['ID']!=int(args[2],16)];self.remove_count+=1
        elif action=='add':
            self.current.append({'ID':99,'name':'','peerURLs':[args[3].split('=',1)[1]],'isLearner':True});self.add_count+=1
        elif action=='promote':
            next(m for m in self.current if m['ID']==int(args[2],16))['isLearner']=False;self.promote_count+=1
        if self.lose==action:self.lose=None;raise RuntimeError('injected lost DCS reply')
        return SimpleNamespace(stdout=json.dumps({'members':self.current}).encode())
    def kubectl(self,*args):
        if args[0]=='get':return SimpleNamespace(stdout=json.dumps({'metadata':{'uid':self.uid},'spec':{'replicas':0,'template':{'spec':{'containers':[{'args':self.args}]}}}}).encode())
        self.args=json.loads(args[-1])[-1]['value'];return SimpleNamespace(stdout=b'')

class DCSResumeChecks(unittest.TestCase):
    def test_lost_remove_or_add_reply_reconciles_exact_original_survivors(self):
        for action in('remove','add'):
            helper=DCSFixture();helper.lose=action
            with self.assertRaises(RuntimeError):reconcile_dcs(helper,helper.original,{'uid':helper.uid})
            learner=reconcile_dcs(helper,helper.original,{'uid':helper.uid})
            self.assertEqual(learner[1],99);self.assertEqual(helper.remove_count,1);self.assertEqual(helper.add_count,1)
            self.assertIn('--initial-cluster-state=existing',helper.args)
    def test_foreign_survivor_or_recreated_dcs_controller_is_refused(self):
        helper=DCSFixture();helper.current[1]['peerURLs']=['https://foreign:2380']
        with self.assertRaisesRegex(ValueError,'survivors changed'):reconcile_dcs(helper,helper.original,{'uid':helper.uid})
        self.assertEqual(helper.remove_count,0)
    def test_lost_promote_reply_is_observed_without_second_promotion(self):
        helper=DCSFixture();learner=reconcile_dcs(helper,helper.original,{'uid':helper.uid});helper.lose='promote'
        runner=object.__new__(ReplacementRunner)
        with self.assertRaises(RuntimeError):runner.promote(helper,learner)
        runner.promote(helper,learner);self.assertEqual(helper.promote_count,1)

if __name__=='__main__':unittest.main()
