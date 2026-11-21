"""Full-load memory protection must stop and remove its own generator."""
from argparse import Namespace
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import p2_load_test as load_run


class LoadRunnerTests(unittest.TestCase):
    def test_full_load_memory_guard_cleans_the_generator_not_a_processor(self):
        self.check_memory_guard('minikube')

    def test_k3s_profile_never_inspects_node_containers_through_docker(self):
        inspected = self.check_memory_guard('k3s')
        self.assertNotIn('node-a', inspected)
        self.assertNotIn('node-b', inspected)

    def check_memory_guard(self, profile):
        inspected = []
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'run'
            cfg = {'nodes': {'a': 'node-a', 'b': 'node-b'}, 'image_prefix': 'aat', 'image_tag': 'fixture',
                   'external': {}, 'coastal': {'profile': 'practice', 'speed': 60}, 'profile': profile}
            controllers = [{'metadata': {'name': name}, 'spec': {'replicas': 2,
                'template': {'spec': {'containers': [{'name': name, 'image': 'fixture', 'env': []}]}}},
                'status': {'readyReplicas': 2}} for name in
                ('aggregator', 'client-api', 'wildfire-processor', 'coastal-processor', 'field-report-processor')]

            def kube(_cfg, *args):
                if 'service/client-api' in args:
                    return json.dumps({'spec': {'ports': [{'nodePort': 30080}]}})
                if 'node' in args:
                    return json.dumps({'status': {'addresses': [{'type': 'InternalIP', 'address': '127.0.0.2'}]}})
                return json.dumps({'items': controllers if 'deployments' in args else []})

            class Process:
                returncode = None
                name = None

                def poll(self): return self.returncode

                def wait(self, timeout):
                    if self.returncode is None:
                        raise subprocess.TimeoutExpired('generator still running', timeout)
                    return self.returncode

            process = Process()
            stops, removals = [], []

            def start(command, **_kwargs):
                process.name = command[command.index('--name')+1]
                return process

            def run(command, **_kwargs):
                if command[:2] == ['docker', 'stop']:
                    stops.append(command[-1])
                    if command[-1] == process.name: process.returncode = 137
                elif command[:2] == ['docker', 'rm']:
                    removals.append(command[-1])
                if command[:2] == ['docker', 'inspect']:
                    return subprocess.CompletedProcess(command, 0, json.dumps({'Status': 'exited', 'ExitCode': 137}))
                return subprocess.CompletedProcess(command, 0, '')

            def check_output(command, **kwargs):
                if command[:2] == ['docker', 'inspect']:
                    inspected.append(command[-1])
                    return '{}'
                return '' if kwargs.get('text') else b''

            args = Namespace(config='fixture', output=str(output), vus=10000, ramp_seconds=30,
                hold_seconds=135, think_seconds=1, report_every=10, memory_mib=4096, cpus=2,
                base_url=None, roll_from=None, scale=False)
            with contextlib.ExitStack() as stack:
                for name, value in {'read_config': lambda _: cfg,
                    'runtime_secrets': lambda _: {'media': 'fixture', 'field': 'fixture'},
                    'kubectl': kube, 'settings': lambda _: {}, 'application_env': lambda *_: {},
                    'node_usage': lambda _cfg: {},
                    'available_memory': lambda: 128*1024**2,
                    'request_json': lambda *_: {'data': {'attributes': {'sample': {'value': 10000}}}}}.items():
                    stack.enter_context(patch.object(load_run, name, value))
                socket = stack.enter_context(patch.object(load_run.socket, 'socket'))
                socket.return_value.__enter__.return_value.getsockname.return_value = ('127.0.0.1', 19000)
                stack.enter_context(patch.object(load_run.subprocess, 'Popen', side_effect=start))
                stack.enter_context(patch.object(load_run.subprocess, 'run', side_effect=run))
                stack.enter_context(patch.object(load_run.subprocess, 'check_output', side_effect=check_output))
                stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                self.assertEqual(load_run.run(args), 137)

            evidence = json.loads((output/'run.json').read_text())
            self.assertTrue(evidence['host_memory_guard'])
            self.assertEqual(stops, [process.name])
            self.assertEqual(removals, [process.name])
            self.assertEqual(evidence['generator_container_name'], process.name)
            self.assertEqual(evidence['generator_state']['ExitCode'], 137)
            self.assertEqual(evidence['profile'], profile)
            return inspected


if __name__ == '__main__':
    unittest.main()
