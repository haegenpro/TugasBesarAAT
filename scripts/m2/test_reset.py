"""Destructive behavior needs filesystem tests, including symlink escapes."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from common import ROOT
from render import render
from reset import reset_plan, wipe_contents


class ResetTests(unittest.TestCase):
    def test_only_selected_owned_directories_are_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('canonical-0-a', 'grafana', 'unrelated'):
                (root/name).mkdir()
                (root/name/'data').write_text('keep until explicitly selected')
            wipe_contents(str(root), ['canonical-0-a', 'already-absent'])
            self.assertFalse((root/'canonical-0-a').exists())
            self.assertTrue((root/'grafana/data').exists())
            self.assertTrue((root/'unrelated/data').exists())

    def test_symlink_escape_and_traversal_are_refused_before_deletion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'safe').mkdir()
            (root/'safe/data').write_text('preserve')
            (root/'escape').symlink_to(root/'safe', target_is_directory=True)
            for names in (['safe', 'escape'], ['safe', '../other']):
                with self.assertRaises(ValueError):
                    wipe_contents(str(root), names)
                self.assertTrue((root/'safe/data').exists())
            link = root/'root-link'
            link.symlink_to(root, target_is_directory=True)
            with self.assertRaises(OSError):
                wipe_contents(str(link), ['safe'])

    def test_volume_plan_checks_live_path_claim_and_node(self):
        cfg = json.loads((ROOT/'deployment/m2/config.example.json').read_text())
        # Renderer only needs opaque nonempty credential values in these tests.
        from collections import defaultdict
        credentials = defaultdict(lambda: 'fixture-secret-value')
        volumes = [o for o in render(cfg, credentials)['items'] if o['kind'] == 'PersistentVolume']
        with patch('reset.runtime_secrets', return_value=credentials):
            plan = reset_plan(cfg, 'application', volumes)
            obs = reset_plan(cfg, 'observability', volumes)
            self.assertEqual({o['volume'] for o in obs}, {'prometheus', 'loki', 'grafana'})
            self.assertFalse({o['volume'] for o in plan} & {o['volume'] for o in obs})
            for mutation in ('path', 'node', 'claim', 'annotation'):
                changed = copy.deepcopy(volumes)
                pv = changed[0]
                if mutation == 'path': pv['spec']['hostPath']['path'] = '/var/lib/unrelated'
                if mutation == 'node': pv['spec']['nodeAffinity'] = {}
                if mutation == 'claim': pv['spec']['claimRef']['name'] = 'unrelated'
                if mutation == 'annotation': pv['metadata']['annotations']['aat.bnpb/owned-path'] = '/tmp/escape'
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    reset_plan(cfg, 'application', changed)
