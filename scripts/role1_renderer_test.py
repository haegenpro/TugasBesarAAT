import copy
import json
from pathlib import Path
import unittest
from role1_integration_lib import render
from storage_lib import plan

ROOT=Path(__file__).resolve().parent.parent

class Role1RendererTests(unittest.TestCase):
    def test_optional_analytics_and_snapshot_configuration(self):
        config=copy.deepcopy(self.config)
        config.update(analytics_enabled=True,audit_sessions_enabled=True)
        result=render(self.inventory,self.aggregator,config,self.image,self.image)
        aggregator=next(v for v in result['items'] if v['kind']=='Deployment' and v['spec']['template']['spec']['containers'][0]['name']!='ingestion')
        container=aggregator['spec']['template']['spec']['containers'][0]
        env={v['name']:v for v in container['env']}
        self.assertEqual(env['ANALYTICS_ENABLED']['value'],'true')
        self.assertEqual(env['AUDIT_SESSIONS_ENABLED']['value'],'true')
        self.assertIn('valueFrom',env['ANALYTICS_SERVICE_TOKEN'])
        self.assertEqual(container['resources']['limits']['memory'],'512Mi')
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
    def test_r3_components_are_explicit_and_isolated(self):
        config=copy.deepcopy(self.config)
        config['r3']={'field_reports_namespace':'field-reports-v1','coastal_namespace':'coastal-v1','satellite_feed_url':'https://satellite.example','pvmbg_adapter_version':2}
        result=render(self.inventory,self.aggregator,config,self.image,self.image)
        deployments={v['metadata']['name']:v for v in result['items'] if v['kind']=='Deployment'}
        self.assertEqual(len(deployments),3)
        coastal=next(v for k,v in deployments.items() if k.endswith('-coastal-processor'))
        self.assertEqual(coastal['spec']['template']['metadata']['labels']['role'],'consumer')
        container=coastal['spec']['template']['spec']['containers'][0]
        env={v['name']:v for v in container['env']}
        self.assertEqual(env['INGESTION_COMPONENTS']['value'],'coastal')
        self.assertIn('valueFrom',env['PANTAI_AUTH_TOKEN'])
        self.assertIn('valueFrom',env['COASTAL_SLO_TOKEN'])
        self.assertNotIn('BMKG_API_KEY',env)
        self.assertEqual(container['resources']['limits']['memory'],'256Mi')
        ingestion=next(v for k,v in deployments.items() if k.endswith('-ingestion'))['spec']['template']['spec']['containers'][0]
        env={v['name']:v for v in ingestion['env']}
        self.assertEqual(env['INGESTION_COMPONENTS']['value'],'bmkg,field-reports,satellite')
        self.assertEqual(env['SATELLITE_NAMESPACE']['value'],config['satellite_namespace'])
        self.assertIn('valueFrom',env['SATELLITE_FEED_TOKEN'])
        aggregator=next(v for k,v in deployments.items() if not k.endswith(('-ingestion','-coastal-processor')))['spec']['template']['spec']['containers'][0]
        env={v['name']:v for v in aggregator['env']}
        self.assertEqual(env['PVMBG_ADAPTER_VERSION']['value'],'2')
        self.assertEqual(env['PVMBG_READER_SCHEMA']['value'],'/schemas/volcanic-adapter-v2.avsc')
        schema=next(v for v in result['items'] if v['kind']=='ConfigMap' and v['metadata']['name'].endswith('-raw-schema'))
        self.assertIn('coastal-warning.avsc',schema['data'])
        policies={v['metadata']['name'] for v in result['items'] if v['kind']=='NetworkPolicy'}
        for name in ('field-reports','client-api-reports','coastal-processor','coastal-status','coastal-storage','coastal-router','notifier-coastal-slo'):
            self.assertTrue(any(p.endswith('-role1-'+name) for p in policies),name)
        coastal_policy=next(v['spec'] for v in result['items'] if v['kind']=='NetworkPolicy' and v['metadata']['name'].endswith('-role1-coastal-processor'))
        self.assertIn('consumer-notifier',json.dumps(coastal_policy['ingress']))
        bad=copy.deepcopy(config);bad['r3']['satellite_feed_url']='http://plain'
        with self.assertRaises(ValueError):render(self.inventory,self.aggregator,bad,self.image,self.image)
        bad=copy.deepcopy(config);bad['r3']['pvmbg_adapter_version']=3
        with self.assertRaises(ValueError):render(self.inventory,self.aggregator,bad,self.image,self.image)
    def test_ops_introspection_is_allowed_in_both_directions(self):
        result=render(self.inventory,self.aggregator,self.config,self.image,self.image)
        policies=[p['spec'] for p in result['items'] if p['kind']=='NetworkPolicy']
        def permits(app, direction, peer, port):
            for policy in policies:
                if policy['podSelector']['matchLabels'].get('app.kubernetes.io/name')!=app:
                    continue
                for rule in policy[direction]:
                    if any(p.get('port')==port for p in rule.get('ports',[])) and any(
                        p.get('podSelector',{}).get('matchLabels',{}).get('app.kubernetes.io/name')==peer
                        for p in rule.get('to' if direction=='egress' else 'from',[])):
                        return True
            return False
        self.assertTrue(permits('ingestion','egress','auth-service',8083))
        self.assertTrue(permits('auth-service','ingress','ingestion',8083))
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

    def test_integrated_province_table_is_embedded_without_reformatting(self):
        config=copy.deepcopy(self.config)
        text=(ROOT/'deployment/role1/wildfire.integrated.example.json').read_text(encoding='utf-8')
        province=(ROOT/'deployment/role1/provinces.json').read_text(encoding='utf-8')
        processor=json.loads(text)
        config.update(satellite_namespace=processor['namespace'],satellite_initial_offset=processor['initial_offset'])
        config['wildfire']={'config_json':text,'province_json':province}
        result=render(self.inventory,self.aggregator,config,self.image,self.image)
        table=next(v for v in result['items'] if v['kind']=='ConfigMap' and '-wildfire-' in v['metadata']['name'])
        self.assertTrue(table['immutable'])
        self.assertEqual(table['data']['provinces.json'],province)
        self.assertEqual(len(json.loads(table['data']['provinces.json'])),38)
        self.assertEqual(json.loads(table['data']['config.json'])['province_sha256'],processor['province_sha256'])

if __name__=='__main__':unittest.main()
