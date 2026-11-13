import copy
import json
from pathlib import Path
import unittest
from role1_integration_lib import render
from storage_lib import plan

ROOT=Path(__file__).resolve().parent.parent

class Role1RendererTests(unittest.TestCase):
    def setUp(self):
        self.inventory=json.loads((ROOT/'deployment/platform/inventory.example.json').read_text())
        self.aggregator=json.loads((ROOT/'deployment/aggregator/config.example.json').read_text())
        self.config=json.loads((ROOT/'deployment/role1/config.example.json').read_text())
        self.image='registry.example/aat@sha256:'+'a'*64
    def test_isolated_databases_rbac_resources_and_configuration(self):
        result=render(self.inventory,self.aggregator,self.config,self.image,self.image)
        role=next(v for v in result['items'] if v['kind']=='Role')
        self.assertEqual(role['rules'],[{'apiGroups':[''],'resources':['pods'],'verbs':['get','list']}])
        deployments=[v for v in result['items'] if v['kind']=='Deployment']
        self.assertEqual(len(deployments),2)
        for d in deployments:
            pod=d['spec']['template']['spec']; self.assertFalse(pod['automountServiceAccountToken'])
            container=pod['containers'][0]; self.assertIn('@sha256:',container['image'])
            self.assertTrue(container['readinessProbe']); self.assertTrue(container['livenessProbe'])
            if container['name']=='ingestion':
                self.assertEqual(container['resources']['requests']['memory'],'128Mi')
                keys={v['name'] for v in container['env']};self.assertIn('INGESTION_DATABASE_URL',keys);self.assertNotIn('DATABASE_URL',keys)
        original=plan(self.inventory,'postgres',self.image);owned=plan(self.inventory,'postgres',self.image,'ingestion')
        self.assertNotEqual(original['formation'],owned['formation']);self.assertEqual(owned['groups'][0]['dsn_env'],'INGESTION_GROUP_0_URL')
    def test_refuse_mutable_images_and_unvalidated_quota(self):
        with self.assertRaises(ValueError):render(self.inventory,self.aggregator,self.config,'aat:latest',self.image)
        with self.assertRaises(ValueError):render(self.inventory,self.aggregator,self.config,self.image,'aat:latest')
        bad=copy.deepcopy(self.config);bad['payload_quota_bytes']=0
        with self.assertRaises(ValueError):render(self.inventory,self.aggregator,bad,self.image,self.image)

if __name__=='__main__':unittest.main()
