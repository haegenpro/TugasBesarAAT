"""Infrastructure invariants: destructive scope, isolation, placement and budgets."""
import copy
import importlib.util
import json
from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import patch

from common import NAMESPACE, ROOT, owned_path, read_config, runtime_secrets
from render import GROUPS, inventory, render


class FoundationTests(unittest.TestCase):
    def test_minikube_profile_name_is_configurable_for_a_second_checkout(self):
        import importlib
        import minikube
        with patch.dict(os.environ, {'M2_MINIKUBE_PROFILE': 'aat-run'}):
            importlib.reload(minikube)
            self.assertEqual(minikube.NAME, 'aat-run')
            self.assertIn('profiles/aat-run/', str(minikube.PROFILE))
        importlib.reload(minikube)
        self.assertEqual(minikube.NAME, 'aat-m2')

    def setUp(self):
        self.config = json.loads((ROOT/'deployment/m2/config.example.json').read_text())
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)/'config.json'

    def load_config(self, value):
        self.path.write_text(json.dumps(value))
        return read_config(self.path)

    def test_destructive_roots_and_colliding_nodes_are_rejected(self):
        for root in ('/', '/var/lib', '/var/lib/rancher/aat-m2', '/etc/aat-m2',
                     '/var/lib/../aat-m2', '/home/user/aat-m2', '/var/lib/something-else',
                     '//var/lib/rancher/aat-m2', '/var/lib/aat-m2/'):
            cfg = copy.deepcopy(self.config)
            cfg['storage_root'] = root
            with self.subTest(root=root), self.assertRaises(ValueError):
                self.load_config(cfg)
        self.config['nodes']['b'] = self.config['nodes']['a']
        with self.assertRaises(ValueError):
            self.load_config(self.config)

    def test_secret_identity_survives_restart_and_private_permissions(self):
        path = Path(self.directory.name)/'secrets.json'
        first = runtime_secrets(path)
        self.assertEqual(first, runtime_secrets(path))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(len(first), len(set(first.values())))
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            runtime_secrets(path)

    def test_postgres_kubernetes_dcs_and_shared_database_hba(self):
        spec = importlib.util.spec_from_file_location('m2_postgres', ROOT/'deployment/m2/storage/entrypoint.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        env = {'FORMATION': 'services', 'POD_NAME': 'services-a-0',
               'POD_CIDR': '10.42.0.0/16', 'GROUP_ID': '-1',
               'MEMBER_SERVICE': 'services-a-peer', 'POD_NAMESPACE': NAMESPACE,
               'POD_IP': '10.42.0.5', 'POSTGRES_DB': 'services',
               'PGDATA': '/var/lib/postgresql/data/pgdata',
               'POSTGRES_PASSWORD': 'test-only-admin', 'REPLICATION_PASSWORD': 'test-only-replication'}
        cfg = module.configuration(env)
        self.assertIn('kubernetes', cfg)
        self.assertNotIn('etcd3', cfg)
        self.assertEqual(cfg['kubernetes']['role_label'], 'database-role')
        self.assertTrue(cfg['bootstrap']['dcs']['synchronous_mode_strict'])
        hba = cfg['postgresql']['pg_hba']
        self.assertIn('host auth auth_app 10.42.0.0/16 scram-sha-256', hba)
        self.assertFalse(any(line.startswith('host all auth_app') for line in hba))
        self.assertEqual(hba[-2:], ['host all all all reject', 'local all all reject'])

    def test_symlink_secret_file_is_refused(self):
        path = Path(self.directory.name)/'real.json'
        runtime_secrets(path)
        link = path.with_name('link.json')
        link.symlink_to(path)
        with self.assertRaises(ValueError):
            runtime_secrets(link)

    def test_storage_broker_placement_and_class_limits(self):
        credentials = runtime_secrets(Path(self.directory.name)/'secrets.json')
        manifest = render(self.config, credentials, foundation=True)
        stateful = [o for o in manifest['items'] if o['kind'] == 'StatefulSet']
        stores = [o for o in stateful if o['spec']['template']['metadata']['labels']['role'] == 'store']
        brokers = [o for o in stateful if o['spec']['template']['metadata']['labels']['role'] == 'broker']
        self.assertEqual(len(stores), len(GROUPS)*2)
        self.assertEqual(len(brokers), 3)
        counts = {node: 0 for node in self.config['nodes'].values()}
        for obj in brokers:
            counts[obj['spec']['template']['spec']['nodeSelector']['kubernetes.io/hostname']] += 1
        self.assertEqual(sorted(counts.values()), [1, 2])
        inv = inventory(manifest)
        self.assertEqual(sum(n['requests_mib'] for n in inv['nodes'].values()), 6784)
        for obj in stateful:
            container = obj['spec']['template']['spec']['containers'][0]
            self.assertEqual(container['resources']['requests']['memory'], '512Mi')
            self.assertEqual(container['resources']['limits']['memory'], '1024Mi')
            self.assertNotIn('POSTGRES_PASSWORD', str(container.get('env', [])))
        for volume in inv['volumes']:
            self.assertTrue(volume['aat.bnpb/owned-path'].startswith(self.config['storage_root']+'/'))
        self.assertEqual(len(inv['volumes']), 13)

    def test_required_namespace_and_tester_access(self):
        cfg = copy.deepcopy(self.config)
        cfg['namespace'] = 'kube-system'
        with self.assertRaises(ValueError):
            self.load_config(cfg)
        cfg = self.config
        manifest = render(cfg, runtime_secrets(Path(self.directory.name)/'secrets.json'))
        for obj in manifest['items']:
            if obj['kind'] not in ('Namespace', 'PersistentVolume', 'ClusterRole', 'ClusterRoleBinding'):
                self.assertEqual(obj['metadata']['namespace'], NAMESPACE)
            if obj['kind'] == 'NetworkPolicy' and any('from' in rule for rule in obj['spec'].get('ingress', [])):
                self.assertIn('tester-agent', json.dumps(obj['spec']))

    def test_broker_capacity_needs_headroom_and_cidrs_must_not_overlap(self):
        for kind in ('capacity', 'cidr'):
            cfg = copy.deepcopy(self.config)
            if kind == 'capacity':
                cfg['limits']['broker_message_mib'] = cfg['limits']['broker_volume_gib']*1024
            else:
                cfg['network']['service_cidr'] = cfg['network']['pod_cidr']
            with self.assertRaises(ValueError):
                self.load_config(cfg)

    def test_full_deployment_uses_three_aggregators_and_bounded_owner_pools(self):
        manifest=render(self.config,runtime_secrets(Path(self.directory.name)/'secrets.json'))
        deployments={o['metadata']['name']:o for o in manifest['items'] if o['kind']=='Deployment'}
        secrets={o['metadata']['name']:o['stringData'] for o in manifest['items'] if o['kind']=='Secret'}
        configs={o['metadata']['name']:o['data'] for o in manifest['items'] if o['kind']=='ConfigMap'}
        self.assertEqual(deployments['aggregator']['spec']['replicas'],3)
        self.assertEqual(configs['aggregator-config']['DB_MAX_OPEN_CONNS'],'6')
        self.assertEqual(sum(deployments[name]['spec']['replicas']*int(configs[name+'-config']['DB_MAX_OPEN_CONNS'])
                             for name in ('ingestion','wildfire-processor','coastal-processor','field-report-processor')),32)
        for name in ('ingestion','wildfire-processor','coastal-processor','field-report-processor'):
            self.assertNotIn('canonical_app',secrets[name+'-runtime']['DATABASE_URL'])
        self.assertNotIn('ingestion_app',secrets['aggregator-runtime']['DATABASE_URL'])
        self.assertNotIn('DATABASE_URL',configs['aggregator-config'])
        self.assertNotIn('NATS_URL',configs['aggregator-config'])
        self.assertNotIn('AUDIT_API_TOKEN',configs['aggregator-config'])

    def test_prometheus_discovers_processes_and_uses_real_vendor_metrics_port(self):
        manifest=render(self.config,runtime_secrets(Path(self.directory.name)/'secrets.json'))
        objects={(o['kind'],o['metadata']['name']):o for o in manifest['items']}
        config=json.loads(objects[('ConfigMap','prometheus-config')]['data']['prometheus.yaml'])
        discovery=config['scrape_configs'][0]['kubernetes_sd_configs'][0]
        self.assertEqual(discovery,{'role':'pod','namespaces':{'names':[NAMESPACE]}})
        registry=objects[('Deployment','schema-registry')]['spec']['template']
        self.assertEqual(registry['metadata']['annotations']['prometheus.io/port'],'9000')
        sensor=objects[('Deployment','sensor-pantai')]['spec']['template']
        self.assertEqual(sensor['metadata']['annotations']['prometheus.io/scrape'],'false')
        grafana=objects[('Deployment','grafana')]['spec']['template']['spec']['containers'][0]
        self.assertIn({'name':'GOMEMLIMIT','value':'128MiB'},grafana['env'])


if __name__ == '__main__':
    unittest.main()
