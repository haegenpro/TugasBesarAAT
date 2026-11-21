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
    if (LOCAL/'reset-pending.json').exists():
        raise ValueError('application data was reset; use up to initialize schemas before starting writers')
    path = LOCAL/'stopped.json'
    state = json.loads(path.read_text())
    if state['context'] != cfg['context'] or state['digest'] != config_digest(cfg):
        raise ValueError('configuration/context changed since stop; deploy the reviewed configuration instead')
    for obj in sorted(state['controllers'], key=lambda o: o['kind'] != 'statefulset'):
        kubectl(cfg, '-n', NAMESPACE, 'scale', obj['kind']+'/'+obj['name'], '--replicas='+str(obj['replicas']))
    path.unlink()
    print('Original application replica counts restored; inspect readiness with status.')


def initialize(cfg: dict) -> None:
    validate_nodes(cfg)
    pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-l',
                             'role=store,database-role=primary', '-o', 'json'))['items']
    if len(pods) != 5:
        raise ValueError('wait for all five PostgreSQL groups to elect primaries')
    script = (ROOT/'deployment/m2/storage/initialize.py').read_text()
    for pod in pods:
        name = pod['metadata']['name']
        kubectl(cfg, '-n', NAMESPACE, 'exec', '-i', name, '-c', 'postgres', '--',
                '/opt/patroni/bin/python', '-', input_text=script)
        print(f'Initialized runtime roles on {name}.')


def migrate(cfg: dict) -> None:
    from lifecycle import require_offline
    require_offline(cfg)
    pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-l',
                             'formation=canonical,citus-group=0,database-role=primary', '-o', 'json'))['items']
    if len(pods) != 1:
        raise ValueError('canonical coordinator primary is not unambiguous')
    schemas = {name: (ROOT/'store'/name).read_text() for name in ('init.sql', 'm2.sql')}
    script = '''import json,os,psycopg2
schemas=json.loads(SCHEMAS)
c=psycopg2.connect(host='127.0.0.1',dbname='hazards',user='canonical_app',password=os.environ['CANONICAL_PASSWORD'])
with c.cursor() as cur:
 cur.execute("SELECT to_regclass('hazard_events') IS NOT NULL")
 if not cur.fetchone()[0]:
  cur.execute(schemas['init.sql'])
  c.commit()
 cur.execute(schemas['m2.sql'])
c.commit()
print('Canonical schema migrated as canonical_app.')
'''.replace('SCHEMAS', repr(json.dumps(schemas)))
    kubectl(cfg, '-n', NAMESPACE, 'exec', '-i', pods[0]['metadata']['name'], '-c', 'postgres',
            '--', '/opt/patroni/bin/python', '-', input_text=script, timeout=120)
    print('Canonical schema initialized and distributed across eight shards.')


def migrate_services(cfg: dict) -> None:
    from lifecycle import require_offline
    require_offline(cfg)
    schemas = {'ingestion': (ROOT/'aggregator/internal/ingest/schema.sql').read_text(),
               'auth': (ROOT/'auth-service/schema.sql').read_text(),
               'dashboard': (ROOT/'consumer-dashboard/schema.sql').read_text(),
               'notifier': (ROOT/'consumer-notifier/schema.sql').read_text()}
    for formation, databases in [('services', ['auth', 'dashboard', 'notifier']), ('ingestion', ['ingestion'])]:
        pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-l',
                                 f'formation={formation},database-role=primary', '-o', 'json'))['items']
        if len(pods) != 1:
            raise ValueError(f'{formation} primary is unavailable')
        for database in databases:
            text = schemas[database]
            if database == 'auth':
                text = text.replace('CREATE TABLE sessions', 'CREATE TABLE IF NOT EXISTS sessions').replace('CREATE INDEX sessions_', 'CREATE INDEX IF NOT EXISTS sessions_')
            script = '''import os,psycopg2
c=psycopg2.connect(host='127.0.0.1',dbname=DATABASE,user=DATABASE+'_app',password=os.environ[DATABASE.upper()+'_PASSWORD'])
with c.cursor() as cursor: cursor.execute(SCHEMA)
c.commit()
'''.replace('DATABASE', repr(database)).replace('SCHEMA', repr(text))
            kubectl(cfg, '-n', NAMESPACE, 'exec', '-i', pods[0]['metadata']['name'], '-c', 'postgres',
                    '--', '/opt/patroni/bin/python', '-', input_text=script)
            print(f'Initialized {database} schema with its own runtime role.')


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
    renderer.add_argument('--foundation', action='store_true')
    commands.add_parser('plan').add_argument('--foundation',action='store_true')
    commands.add_parser('deploy').add_argument('--foundation',action='store_true')
    commands.add_parser('initialize')
    commands.add_parser('migrate')
    commands.add_parser('migrate-services')
    commands.add_parser('status')
    commands.add_parser('stop')
    commands.add_parser('start')
    commands.add_parser('up')
    commands.add_parser('ready').add_argument('--timeout', type=int, default=600)
    commands.add_parser('scale').add_argument('--aggregators',type=int,choices=(1,2,3,5),required=True)
    args = parser.parse_args()
    try:
        if args.command == 'init':
            init(args)
            return 0
        cfg = read_config(args.config)
        if args.command in ('render', 'plan', 'deploy'):
            credentials = runtime_secrets(LOCAL/'secrets.json')
            manifest = render(cfg, credentials,foundation=args.foundation)
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
        elif args.command == 'initialize':
            initialize(cfg)
        elif args.command == 'migrate':
            migrate(cfg)
        elif args.command == 'migrate-services':
            migrate_services(cfg)
        elif args.command == 'stop':
            stop(cfg)
        elif args.command == 'start':
            start(cfg)
        elif args.command == 'up':
            from lifecycle import up
            up(cfg)
        elif args.command == 'ready':
            from lifecycle import ready
            ready(cfg, timeout=args.timeout)
        elif args.command == 'scale':
            validate_nodes(cfg)
            kubectl(cfg,'-n',NAMESPACE,'scale','deployment/aggregator','--replicas='+str(args.aggregators))
            print(f'Aggregator test replica count: {args.aggregators}. Normal deployment count is 3; restore it after the test.')
        return 0
    except (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired) as error:
        # Do not include remote output or serialized credentials in diagnostics.
        print(f'M2 operation failed: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
