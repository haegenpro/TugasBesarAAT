#!/usr/bin/env python3
"""Manage the dedicated M2 deployment; generated files and secrets stay private."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

from common import (APPLICATION, LABEL, LOCAL, NAMESPACE, ROOT, config_digest,
                    kubectl, private_json, read_config, runtime_secrets)
from render import inventory, render


def validate_nodes(cfg: dict) -> dict:
    nodes = json.loads(kubectl(cfg, 'get', 'nodes', '-o', 'json'))['items']
    by_name = {n['metadata']['name']: n for n in nodes}
    for name in cfg['nodes'].values():
        if name not in by_name:
            raise ValueError(f'configured node {name} is absent from the selected context')
        if not any(c['type'] == 'Ready' and c['status'] == 'True' for c in by_name[name]['status']['conditions']):
            raise ValueError(f'configured node {name} is not Ready')
    return by_name


def init(args) -> None:
    path = Path(args.config)
    if path.exists():
        read_config(path)
        print(f'Existing configuration preserved: {path}')
        return
    cfg = json.loads((ROOT/'deployment/m2/config.example.json').read_text())
    cfg['profile'] = args.profile
    cfg['context'] = args.context
    cfg['nodes'] = {'a': args.node_a, 'b': args.node_b}
    if args.profile == 'k3s':
        cfg['network'] = {'pod_cidr': '10.42.0.0/16', 'service_cidr': '10.43.0.0/16'}
    private_json(path, cfg)
    read_config(path)
    runtime_secrets(LOCAL/'secrets.json')
    print(f'Created private configuration: {path}')
    print('Credentials are in .m2-local/secrets.json (mode 0600); no values printed.')


def controller_state(cfg: dict) -> list[dict]:
    items = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'deployment,statefulset',
                               '-l', f'{LABEL}={APPLICATION}', '-o', 'json'))['items']
    return [{'kind': o['kind'].lower(), 'name': o['metadata']['name'],
             'replicas': o['spec'].get('replicas', 1)} for o in items]


def stop(cfg: dict) -> None:
    path = LOCAL/'stopped.json'
    state = controller_state(cfg)
    if not state:
        raise ValueError('no managed controllers found; refusing to invent a stop state')
    if path.exists():
        prior = json.loads(path.read_text())
        if prior['context'] != cfg['context']:
            raise ValueError('stop state belongs to another context')
        if not any(o['replicas'] for o in state):
            print('Application is already stopped; original replica counts preserved.')
            return
    private_json(path, {'context': cfg['context'], 'digest': config_digest(cfg), 'controllers': state})
    # Stop data-producing applications before their databases/brokers.
    state.sort(key=lambda o: o['kind'] == 'statefulset')
    for obj in state:
        kubectl(cfg, '-n', NAMESPACE, 'scale', obj['kind']+'/'+obj['name'], '--replicas=0')
    print('Application controllers stopped; persistent volumes and configuration preserved.')


def start(cfg: dict) -> None:
    path = LOCAL/'stopped.json'
    state = json.loads(path.read_text())
    if state['context'] != cfg['context'] or state['digest'] != config_digest(cfg):
        raise ValueError('configuration/context changed since stop; deploy the reviewed configuration instead')
    for obj in sorted(state['controllers'], key=lambda o: o['kind'] != 'statefulset'):
        kubectl(cfg, '-n', NAMESPACE, 'scale', obj['kind']+'/'+obj['name'], '--replicas='+str(obj['replicas']))
    path.unlink()
    print('Original application replica counts restored; inspect readiness with status.')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    commands = parser.add_subparsers(dest='command', required=True)
    initial = commands.add_parser('init')
    initial.add_argument('--profile', choices=['minikube', 'k3s'], default='minikube')
    initial.add_argument('--context', default='aat-m2')
    initial.add_argument('--node-a', default='aat-m2')
    initial.add_argument('--node-b', default='aat-m2-m02')
    renderer = commands.add_parser('render')
    renderer.add_argument('--output', default=str(LOCAL/'manifests.json'))
    commands.add_parser('plan')
    commands.add_parser('deploy')
    commands.add_parser('status')
    commands.add_parser('stop')
    commands.add_parser('start')
    args = parser.parse_args()
    try:
        if args.command == 'init':
            init(args)
            return 0
        cfg = read_config(args.config)
        if args.command in ('render', 'plan', 'deploy'):
            credentials = runtime_secrets(LOCAL/'secrets.json')
            manifest = render(cfg, credentials)
            if args.command == 'plan':
                print(json.dumps(inventory(manifest), indent=2))
            elif args.command == 'render':
                private_json(Path(args.output), manifest)
                print(f'Wrote private Kubernetes manifest: {args.output}')
            else:
                validate_nodes(cfg)
                kubectl(cfg, 'apply', '-f', '-', input_text=json.dumps(manifest))
                print('Applied the M2 resources. Use status to inspect readiness.')
        elif args.command == 'status':
            print(kubectl(cfg, '-n', NAMESPACE, 'get', 'deployment,statefulset,pods,pvc', '-o', 'wide'))
        elif args.command == 'stop':
            stop(cfg)
        elif args.command == 'start':
            start(cfg)
        return 0
    except (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired) as error:
        # Do not include remote output or serialized credentials in diagnostics.
        print(f'M2 operation failed: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
