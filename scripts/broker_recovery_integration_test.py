"""Rendered account boundaries and private application connection wiring."""
import copy
import json
from pathlib import Path
import unittest
from aggregator_lib import render as aggregator
from service_lib import render

ROOT=Path(__file__).resolve().parent.parent

class BrokerCredentialChecks(unittest.TestCase):
    def setUp(self):
        self.inventory=json.loads((ROOT/'deployment/platform/inventory.fourhost.example.json').read_text())
        self.catalog=json.loads((ROOT/'deployment/services/catalog.json').read_text())
        self.image='fixture.invalid/test@sha256:'+'a'*64
        self.images={s['image_key']:self.image for s in self.catalog['services']if 'image_key'in s}

    def test_system_credentials_are_confined_to_broker_servers(self):
        resources,_=render(self.inventory,self.catalog,self.images)
        config=next(o for o in resources['items']if o['kind']=='ConfigMap'and o['metadata']['name']=='broker-config')['data']['nats.conf']
        self.assertIn('system_account: SYS',config)
        self.assertIn('password: $NATS_SYSTEM_PASSWORD',config)
        for obj in resources['items']:
            if obj['kind']not in('Deployment','StatefulSet','Job'):continue
            container=obj['spec']['template']['spec']['containers'][0]
            env={v['name']:v for v in container['env']}
            name=container['name']
            if name=='broker':self.assertIn('secretKeyRef',env['NATS_SYSTEM_URL']['valueFrom'])
            else:
                self.assertNotIn('NATS_SYSTEM_URL',env)
                self.assertNotIn('broker-runtime',{e['secretRef']['name']for e in container.get('envFrom',[])})
            if name in('broker-init','consumer-dashboard','consumer-notifier'):
                self.assertIn('secretKeyRef',env['NATS_URL']['valueFrom'])

    def test_public_url_cannot_override_private_app_credentials(self):
        config=json.loads((ROOT/'deployment/aggregator/config.integrated.example.json').read_text())
        objects=aggregator(self.inventory,config,self.image)
        pod=next(o for o in objects['items']if o['kind']=='Deployment')['spec']['template']['spec']
        url=next(v for v in pod['containers'][0]['env']if v['name']=='NATS_URL')
        self.assertEqual(url['valueFrom']['secretKeyRef']['name'],config['runtime_secret'])
        bad=copy.deepcopy(self.catalog)
        next(s for s in bad['services']if s['name']=='broker-init')['environment']['NATS_URL']='nats://broker:4222'
        with self.assertRaisesRegex(ValueError,'shadowed'):render(self.inventory,bad,self.images)

if __name__=='__main__':unittest.main()
