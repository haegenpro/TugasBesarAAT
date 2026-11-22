"""`manage.py satellite`: token custody, configuration edits and when it deploys."""
import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from common import ROOT
import manage


class SatelliteToggleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.local = Path(directory.name)
        self.config = self.local/'k3s.json'
        cfg = json.loads((ROOT/'deployment/m2/config.example.json').read_text())
        manage.private_json(self.config, cfg)
        self.token = self.local/'satellite-token'
        for target, value in [('LOCAL', self.local), ('SATELLITE_TOKEN', self.token)]:
            patcher = patch.object(manage, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.calls = []
        for name, fake in [('kubectl', lambda cfg, *a, **k: self.calls.append(a) or '{}'),
                           ('validate_nodes', lambda cfg: {}),
                           ('render', lambda cfg, creds: {'items': []}),
                           ('runtime_secrets', lambda path: {})]:
            patcher = patch.object(manage, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_toggle(self, action, token_file=None, no_deploy=False):
        args = argparse.Namespace(config=str(self.config), action=action,
                                  token_file=token_file, no_deploy=no_deploy)
        manage.satellite(args, manage.read_config(self.config))
        return json.loads(self.config.read_text())['external']['satellite_token']

    def write_source(self, text, mode=0o600):
        source = self.local/'source-token'
        source.write_text(text)
        source.chmod(mode)
        return str(source)

    def test_on_off_on_reuses_saved_token_and_deploys_each_change(self):
        self.assertEqual(self.run_toggle('on', self.write_source('abc123\n')), 'abc123')
        self.assertEqual(self.token.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.run_toggle('off'), '')
        self.assertEqual(self.run_toggle('on'), 'abc123')
        applies = [c for c in self.calls if c[0] == 'apply']
        rollouts = [c for c in self.calls if c[0] == '-n' and 'rollout' in c]
        self.assertEqual((len(applies), len(rollouts)), (3, 3))

    def test_unchanged_state_does_not_deploy(self):
        self.run_toggle('off')
        self.assertEqual(self.calls, [])

    def test_stopped_application_only_updates_configuration(self):
        (self.local/'stopped.json').write_text('{}')
        self.assertEqual(self.run_toggle('on', self.write_source('abc123')), 'abc123')
        self.assertEqual(self.calls, [])

    def test_no_deploy_only_updates_configuration(self):
        self.run_toggle('on', self.write_source('abc123'), no_deploy=True)
        self.assertEqual(self.calls, [])

    def test_off_preserves_a_token_that_only_lived_in_the_configuration(self):
        cfg = json.loads(self.config.read_text())
        cfg['external']['satellite_token'] = 'legacy'
        manage.private_json(self.config, cfg)
        self.run_toggle('off')
        self.assertEqual(self.token.read_text().strip(), 'legacy')

    def test_rejects_world_readable_or_malformed_token_files(self):
        with self.assertRaises(ValueError):
            self.run_toggle('on', self.write_source('abc123', mode=0o644))
        with self.assertRaises(ValueError):
            self.run_toggle('on', self.write_source('two tokens'))
        self.assertEqual(json.loads(self.config.read_text())['external']['satellite_token'], '')


if __name__ == '__main__':
    unittest.main()
