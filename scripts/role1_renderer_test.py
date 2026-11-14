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

    def wildfire_configuration(self):
        config=copy.deepcopy(self.config)
        text=(ROOT/'docs/fixtures/wildfire-m6/config.json').read_text(encoding='utf-8')
        processor=json.loads(text)
        config['satellite_namespace']=processor['namespace']
        config['satellite_initial_offset']=processor['initial_offset']
        config['wildfire']={'config_json':text,'province_json':(ROOT/'docs/fixtures/wildfire-m5/provinces.json').read_text(encoding='utf-8')}
        return config

    def test_wildfire_is_explicit_immutable_and_uses_existing_resource_class(self):
        config=self.wildfire_configuration()
        result=render(self.inventory,self.aggregator,config,self.image,self.image)
        table=next(v for v in result['items'] if v['kind']=='ConfigMap' and '-wildfire-' in v['metadata']['name'])
        self.assertTrue(table['immutable'])
        processor=json.loads(table['data']['config.json'])
        self.assertEqual(processor['province_file'],'provinces.json')
        aggregators=[v for v in result['items'] if v['kind']=='Deployment' and v['spec']['template']['spec']['containers'][0]['name']!='ingestion']
        container=aggregators[0]['spec']['template']['spec']['containers'][0]
        self.assertEqual(container['resources']['limits']['memory'],'512Mi')
        self.assertIn({'name':'WILDFIRE_PROCESSOR_ENABLED','value':'true'},container['env'])
        ingestion=next(v for v in result['items'] if v['kind']=='Deployment' and v['spec']['template']['spec']['containers'][0]['name']=='ingestion')
        self.assertNotIn('WILDFIRE_PROCESSOR_ENABLED',{v['name'] for v in ingestion['spec']['template']['spec']['containers'][0]['env']})

    def test_wildfire_rejects_bad_digest_scope_and_budget(self):
        for change in ('digest','scope','quota'):
            config=self.wildfire_configuration()
            if change=='digest':config['wildfire']['province_json']+=' '
            elif change=='scope':config['satellite_namespace']='different'
            else:
                processor=json.loads(config['wildfire']['config_json']);processor['prepared_bytes']=processor['cache_bytes'];config['wildfire']['config_json']=json.dumps(processor)
            with self.assertRaises(ValueError):render(self.inventory,self.aggregator,config,self.image,self.image)

if __name__=='__main__':unittest.main()
