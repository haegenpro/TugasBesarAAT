#!/usr/bin/env python3
"""Install, check and drive the course Tester Agent without altering its image.

Criteria: Tester Agent contract; drives the failures of P1-4..P1-6, P2-3..P2-5, P3-6, P4-5, P4-8, P5-6.

install    apply the pinned manifest (dry run unless --execute)
status     one agent pod per node, pinned image digest, privileges untouched by our policies
list       list practice scenarios
practice   run one practice scenario and stream its output
logs       print the agent's action log from every node (the failure-injection evidence)
"""
import argparse
import json
import subprocess
import sys

from common import LOCAL, NAMESPACE, kube_env, kubectl, kubectl_binary, read_config

MANIFEST = ('https://raw.githubusercontent.com/susTuna/if4031-tester-agent/'
            'ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml')
IMAGE = 'sustuna/tester-agent@sha256:b3143da159cf23a8744ede12b847da6208353754234d53a5a7b46d071cf277ec'
AGENT_NAMESPACE = 'tester-agent'


def check_status(pods, daemonsets, nodes):
    """Contract checks from the specification, as {name: (ok, detail)}."""
    result = {}
    images = {c['image'] for d in daemonsets for c in d['spec']['template']['spec']['containers']}
    result['pinned image digest, unmodified'] = (images == {IMAGE}, ', '.join(sorted(images)) or 'no daemonset')
    placed = sorted(p['spec'].get('nodeName') for p in pods if p['status']['phase'] == 'Running')
    result['one running pod on every node'] = (placed == sorted(nodes), f'pods on {placed}, nodes {sorted(nodes)}')
    privileged = any(c.get('securityContext', {}).get('privileged') for d in daemonsets
                     for c in d['spec']['template']['spec']['containers'])
    result['requested privileges present (not stripped)'] = (privileged or not daemonsets, 'privileged' if privileged else 'not privileged')
    return result


def kube(cfg, *args, stream=False):
    command = [kubectl_binary(), '--context', cfg['context'], *args]
    if stream:
        return subprocess.run(command, env=kube_env(cfg)).returncode
    return kubectl(cfg, *args)


def cmd_install(cfg, args):
    print('kubectl apply -f '+MANIFEST)
    if not args.execute:
        print('Dry run. Add --execute to install the Tester Agent on this cluster (image must not be changed).')
        return 0
    print(kube(cfg, 'apply', '-f', MANIFEST))
    print(kube(cfg, '-n', AGENT_NAMESPACE, 'rollout', 'status', 'daemonset/tester-agent', '--timeout=180s'))
    return cmd_status(cfg, args)


def cmd_status(cfg, args):
    pods = json.loads(kube(cfg, '-n', AGENT_NAMESPACE, 'get', 'pods', '-o', 'json'))['items']
    daemonsets = json.loads(kube(cfg, '-n', AGENT_NAMESPACE, 'get', 'daemonset', '-o', 'json'))['items']
    result = check_status(pods, daemonsets, list(cfg['nodes'].values()))
    for name, (ok, detail) in result.items():
        print(('PASS ' if ok else 'FAIL ')+f'{name}: {detail}')
    app_pods = json.loads(kube(cfg, '-n', NAMESPACE, 'get', 'pods', '-o', 'json'))['items']
    roles = {'aggregator', 'store', 'broker', 'consumer', 'api', 'mock', 'sensor', 'other'}
    bad = [p['metadata']['name'] for p in app_pods if p['metadata'].get('labels', {}).get('role') not in roles
           and any(o['kind'] in ('ReplicaSet', 'StatefulSet') for o in p['metadata'].get('ownerReferences', []))]
    print(('PASS ' if not bad else 'FAIL ')+'every application pod carries a valid role label'+('' if not bad else ': '+', '.join(bad)))
    return 0 if all(ok for ok, _ in result.values()) and not bad else 1


def cmd_list(cfg, args):
    return kube(cfg, '-n', AGENT_NAMESPACE, 'exec', 'daemonset/tester-agent', '--', '/app', 'list', stream=True)


def cmd_practice(cfg, args):
    return kube(cfg, '-n', AGENT_NAMESPACE, 'exec', 'daemonset/tester-agent', '--', '/app', 'run', 'practice',
                args.scenario, stream=True)


def cmd_logs(cfg, args):
    pods = json.loads(kube(cfg, '-n', AGENT_NAMESPACE, 'get', 'pods', '-o', 'json'))['items']
    for pod in sorted(pods, key=lambda p: p['metadata']['name']):
        print(f"===== {pod['metadata']['name']} on {pod['spec'].get('nodeName')} =====", flush=True)
        extra = ['--since='+args.since] if args.since else []
        kube(cfg, '-n', AGENT_NAMESPACE, 'logs', pod['metadata']['name'], *extra, stream=True)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('install').add_argument('--execute', action='store_true')
    commands.add_parser('status')
    commands.add_parser('list')
    commands.add_parser('practice').add_argument('scenario')
    commands.add_parser('logs').add_argument('--since')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    return {'install': cmd_install, 'status': cmd_status, 'list': cmd_list, 'practice': cmd_practice,
            'logs': cmd_logs}[args.command](cfg, args)


if __name__ == '__main__':
    sys.exit(main())
