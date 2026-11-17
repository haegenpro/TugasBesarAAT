"""Conditional controller holds and durable interrupted-run evidence checks."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from replacement_lib import controller_holds,read_report,set_controller_hold,write_report

class ControllerAPI:
    def __init__(self):
        self.cfg={'cluster_id':'fixture'};self.mutations=[]
        self.resources={'statefulset/broker-a':{'kind':'StatefulSet','metadata':{'name':'broker-a','namespace':'assignment','uid':'original-controller','labels':{'platform.aat/cluster':'fixture'}},'spec':{'replicas':1,'template':{'metadata':{'labels':{'app.kubernetes.io/name':'broker','platform.aat/cluster':'fixture'}},'spec':{'nodeSelector':{'kubernetes.io/hostname':'node-a'},'volumes':[{'persistentVolumeClaim':{'claimName':'broker-claim'}}]}}}}}

    def kubectl(self,*args):
        resource=self.resources[args[1]]
        if args[0]=='get':return SimpleNamespace(stdout=json.dumps(resource).encode())
        operations=json.loads(args[-1]);candidate=copy.deepcopy(resource)
        for op in operations:
            parts=op['path'].strip('/').split('/');parent=candidate
            for part in parts[:-1]:parent=parent[part]
            if op['op']=='test':
                if parent[parts[-1]]!=op['value']:raise RuntimeError('API server rejected concurrent resource change')
            elif op['op']=='replace':parent[parts[-1]]=op['value']
            else:raise AssertionError('unexpected mutation')
        self.resources[args[1]]=candidate;self.mutations.append(args)
        return SimpleNamespace(stdout=b'')

class ReplacementSafetyChecks(unittest.TestCase):
    def setUp(self):
        self.cluster=ControllerAPI()
        self.prepared={'namespace':'assignment','target':{'name':'node-a'},'formations':[{'plan':{'formation':'fixture-storage'}}],'volumes':[{'claim':'broker-claim','controller':{'kind':'StatefulSet','name':'broker-a','app':'broker'}}]}

    def test_holds_restore_only_the_recorded_controller_and_original_count(self):
        holds=controller_holds(self.cluster,self.prepared);key='statefulset/broker-a';identity=holds[key]
        set_controller_hold(self.cluster,'assignment',key,identity)
        self.assertEqual(self.cluster.resources[key]['spec']['replicas'],0)
        set_controller_hold(self.cluster,'assignment',key,identity,restore=True)
        self.assertEqual(self.cluster.resources[key]['spec']['replicas'],1)
        self.assertEqual(len(self.cluster.mutations),2)

    def test_recreated_or_concurrently_scaled_controller_is_not_changed(self):
        for changed in ('uid','replicas'):
            cluster=ControllerAPI();key='statefulset/broker-a';identity=controller_holds(cluster,self.prepared)[key]
            if changed=='uid':cluster.resources[key]['metadata']['uid']='replacement-controller'
            else:cluster.resources[key]['spec']['replicas']=2
            before=copy.deepcopy(cluster.resources)
            with self.assertRaises(RuntimeError):set_controller_hold(cluster,'assignment',key,identity)
            self.assertEqual(cluster.resources,before);self.assertEqual(cluster.mutations,[])

    def test_preflight_refuses_foreign_moved_or_changed_claim_ownership(self):
        for changed in ('owner','conflicting-owner','pod-owner','node','claim'):
            cluster=ControllerAPI();resource=cluster.resources['statefulset/broker-a']
            if changed=='owner':resource['metadata']['labels']['platform.aat/cluster']='other'
            elif changed=='conflicting-owner':resource['metadata']['labels'].update({'platform.aat/cluster':'other','platform.aat/formation':'fixture-storage'})
            elif changed=='pod-owner':resource['spec']['template']['metadata']['labels']['platform.aat/cluster']='other'
            elif changed=='node':resource['spec']['template']['spec']['nodeSelector']['kubernetes.io/hostname']='node-b'
            else:resource['spec']['template']['spec']['volumes']=[]
            with self.assertRaises(ValueError):controller_holds(cluster,self.prepared)
            self.assertEqual(cluster.mutations,[])

    def test_intentionally_stopped_controller_is_never_activated(self):
        key='statefulset/broker-a';self.cluster.resources[key]['spec']['replicas']=0
        identity=controller_holds(self.cluster,self.prepared)[key]
        set_controller_hold(self.cluster,'assignment',key,identity)
        set_controller_hold(self.cluster,'assignment',key,identity,restore=True)
        self.assertEqual(self.cluster.resources[key]['spec']['replicas'],0)

    def test_storage_formation_ownership_without_application_labels_is_supported(self):
        key='statefulset/broker-a';resource=self.cluster.resources[key]
        resource['metadata']['labels']={'platform.aat/formation':'fixture-storage'}
        resource['spec']['template']['metadata']['labels']={'platform.aat/formation':'fixture-storage'}
        self.prepared['volumes'][0]['controller']['app']=None
        identity=controller_holds(self.cluster,self.prepared)[key]
        set_controller_hold(self.cluster,'assignment',key,identity)
        self.assertEqual(self.cluster.resources[key]['spec']['replicas'],0)

    def test_failed_report_replace_preserves_last_complete_journal(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);path=root/'result.json';original={'events':[{'phase':'controller hold intent','mutation_intent':True}]}
            write_report(path,original)
            with patch('replacement_lib.os.replace',side_effect=OSError('injected replacement failure')):
                with self.assertRaises(OSError):write_report(path,{'events':[]})
            self.assertEqual(json.loads(path.read_text()),original)
            self.assertEqual(list(root.iterdir()),[path])
            self.assertEqual(path.stat().st_mode&0o777,0o600)

    def test_offline_status_is_bound_to_original_run_node_and_cluster(self):
        with tempfile.TemporaryDirectory()as folder:
            inventory={'state_dir':folder,'cluster_id':'fixture'};run='a'*32
            root=Path(folder)/'evidence'/('integrated-replacement-'+run);root.mkdir(parents=True)
            value={'version':2,'cluster_id':'fixture','run_id':run,'node':'node-a','passed':False,'events':[]}
            write_report(root/'result.json',value)
            self.assertEqual(read_report(inventory,run,'node-a'),value)
            with self.assertRaises(ValueError):read_report(inventory,run,'node-b')
            with self.assertRaises(ValueError):read_report(inventory,'../different','node-a')
            other={**inventory,'cluster_id':'foreign'}
            with self.assertRaises(ValueError):read_report(other,run,'node-a')

if __name__=='__main__':unittest.main()
