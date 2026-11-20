"""Infrastructure invariants: destructive scope, isolation, placement and budgets."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from common import NAMESPACE, ROOT, owned_path, read_config, runtime_secrets
from render import GROUPS, inventory, render


class FoundationTests(unittest.TestCase):
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
        manifest = render(self.config, credentials)
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
        self.assertEqual(sum(n['requests_mib'] for n in inv['nodes'].values()), 6656)
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
            if obj['kind'] not in ('Namespace', 'PersistentVolume'):
                self.assertEqual(obj['metadata']['namespace'], NAMESPACE)
            if obj['kind'] == 'NetworkPolicy' and obj['spec'].get('ingress'):
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


if __name__ == '__main__':
    unittest.main()
