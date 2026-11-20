import copy
import unittest
from application_reset_lib import build_plan, guest_job
from demo_checks_test import inventory


class ApplicationResetSafety(unittest.TestCase):
    def fixture(self):
        cfg=inventory();cfg.update(state_dir='/tmp/reset-owned',kubeconfig='/private/kubeconfig',kubectl_command=['kubectl'])
        nodes=[{'metadata':{'name':n['name'],'uid':n['uid']}}for n in cfg['nodes']]
        path='/var/lib/aat/local-pv/aat-fourhost-storage/aat-fourhost-storage-g0-a'
        pv={'metadata':{'name':'aat-fourhost-storage-g0-a','uid':'pv-owned'},'spec':{'local':{'path':path},'claimRef':{'namespace':cfg['namespace']},'nodeAffinity':{'required':{'nodeSelectorTerms':[{'matchExpressions':[{'key':'kubernetes.io/hostname','operator':'In','values':[cfg['nodes'][0]['name']]}]}]}}}}
        namespace={'metadata':{'name':cfg['namespace'],'uid':'ns-owned'}}
        volumes=[{'node':cfg['nodes'][0]['name'],'path':path,'uid':999}]
        return cfg,namespace,nodes,[pv],volumes,'ghcr.io/example/storage@sha256:'+'1'*64

    def test_only_owned_namespace_volumes_are_planned(self):
        args=list(self.fixture());foreign=copy.deepcopy(args[3][0]);foreign['spec']['claimRef']['namespace']='unrelated';foreign['metadata']['name']='foreign';args[3].append(foreign)
        plan=build_plan(*args)
        self.assertEqual(len(plan['volumes']),1)
        self.assertEqual(plan['volumes'][0]['uid'],'pv-owned')
        job=guest_job(args[0],plan,args[0]['nodes'][0],True)
        pod=job['spec']['template']['spec']
        self.assertFalse(pod['automountServiceAccountToken'])
        self.assertEqual(pod['nodeSelector'],{'kubernetes.io/hostname':'aat-fourhost-a'})
        self.assertTrue(pod['containers'][0]['volumeMounts'][0]['readOnly'])

    def test_system_namespace_foreign_path_and_changed_node_are_refused(self):
        for case in ('system','path','node','missing'):
            with self.subTest(case=case):
                args=list(self.fixture())
                if case=='system':args[0]['namespace']='kube-system';args[1]['metadata']['name']='kube-system'
                elif case=='path':args[3][0]['spec']['local']['path']='/var/lib/rancher/k3s'
                elif case=='node':args[2][0]['metadata']['uid']='reinstalled'
                else:args[3]=[]
                with self.assertRaises(ValueError):build_plan(*args)

if __name__=='__main__':unittest.main()
