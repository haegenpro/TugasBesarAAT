"""Worker credentials must be guarded by the captured physical identities."""
import copy
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock,Mock,patch

from citus_auth_lib import SCRIPT,render
from platform_lib.inventory import load
from storage_lib import plan,durability_config

ROOT=Path(__file__).resolve().parent.parent


class CitusAuthenticationChecks(unittest.TestCase):
    def fixture(self):
        s=plan(load(ROOT/'deployment/platform/inventory.fourhost.example.json'),'citus','registry.invalid/storage@sha256:'+'a'*64)
        d=durability_config(s)
        for i,g in enumerate(d['groups']):g['system_id']=str(100+i)
        return s,d

    def test_job_uses_existing_secret_references_and_captured_placement(self):
        s,d=self.fixture();resource,name=render(s,d);job=resource['items'][0];pod=job['spec']['template']['spec'];container=pod['containers'][0]
        self.assertEqual(render(s,d)[1],name)
        self.assertEqual(pod['automountServiceAccountToken'],False)
        self.assertEqual(container['resources']['limits']['memory'],'256Mi')
        env={e['name']:e for e in container['env']}
        for key in ('APP_PASSWORD','POSTGRES_PASSWORD'):
            self.assertNotIn('value',env[key]);self.assertEqual(env[key]['valueFrom']['secretKeyRef']['name'],s['formation']+'-credentials')
        groups=json.loads(env['CAPTURED_GROUPS']['value']);self.assertEqual(len(groups),5)
        self.assertEqual([g['system_id']for g in groups],[g['system_id']for g in d['groups']])
        bad=copy.deepcopy(d);bad['groups'][1]['members'][0]['name']='unrelated'
        with self.assertRaisesRegex(ValueError,'differs'):render(s,bad)

    def execute(self,bad_identity=False):
        s,d=self.fixture();resource,_=render(s,d);env={v['name']:v['value']for v in resource['items'][0]['spec']['template']['spec']['containers'][0]['env']if 'value'in v};env.update(POSTGRES_PASSWORD='admin test value',APP_PASSWORD="runtime test ' value")
        connections=[]
        for i,g in enumerate(d['groups']):
            c=MagicMock();c.cursor.return_value.__enter__.return_value.fetchone.return_value=({'primary':True,'formation':s['formation'],'group_id':g['id'],'system_id':'wrong'if bad_identity and i==0 else g['system_id']},);connections.append(c)
        runtime=MagicMock();runtime.cursor.return_value.__enter__.return_value.fetchall.return_value=[(True,'aggregator_app')]*4
        driver=SimpleNamespace(connect=Mock(side_effect=[*connections,runtime]),OperationalError=ConnectionError)
        extensions=SimpleNamespace(make_dsn=Mock(return_value='libpq-escaped-password'))
        with patch.dict(sys.modules,{'psycopg2':driver,'psycopg2.extensions':extensions}),patch.dict(os.environ,env):
            if bad_identity:
                with self.assertRaisesRegex(SystemExit,'unexpected database identity'):exec(SCRIPT,{})
            else:exec(SCRIPT,{})
        return connections,runtime,driver,extensions

    def test_configures_both_roles_on_each_primary_then_checks_runtime_workers(self):
        connections,runtime,driver,extensions=self.execute()
        self.assertEqual(driver.connect.call_count,6);self.assertEqual(extensions.make_dsn.call_count,10)
        for c in connections:
            writes=[a.args for a in c.cursor.return_value.__enter__.return_value.execute.call_args_list if a.args[0].startswith('INSERT')]
            self.assertEqual([a[1][0]for a in writes],['postgres','aggregator_app'])
            self.assertTrue(all('password'not in a[0]for a in writes));c.close.assert_called_once()
        runtime.close.assert_called_once()

    def test_wrong_physical_identity_prevents_credential_changes(self):
        connections,_,driver,extensions=self.execute(True)
        self.assertEqual(driver.connect.call_count,1);extensions.make_dsn.assert_not_called()
        self.assertFalse(any(a.args[0].startswith('INSERT')for a in connections[0].cursor.return_value.__enter__.return_value.execute.call_args_list))


if __name__=='__main__':unittest.main()
