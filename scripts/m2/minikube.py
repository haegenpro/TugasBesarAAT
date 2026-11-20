#!/usr/bin/env python3
"""Start the isolated development cluster without changing the host's kubeconfig."""
import argparse
import json
import os
import subprocess

from common import LOCAL


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('start', 'status', 'stop'))
    parser.add_argument('--memory', type=int, default=6144, help='MiB per development node')
    parser.add_argument('--cpus', type=int, default=2)
    args = parser.parse_args()
    env = os.environ.copy()
    env['MINIKUBE_HOME'] = str(LOCAL/'minikube')
    env['KUBECONFIG'] = str(LOCAL/'kubeconfig')
    command = [str(LOCAL/'bin/minikube'), '-p', 'aat-m2', args.command]
    if args.command == 'start':
        profile = LOCAL/'minikube/.minikube/profiles/aat-m2/config.json'
        existing = json.loads(profile.read_text()) if profile.exists() else {}
        if existing.get('StaticIP'):
            parser.error('this profile has an incompatible static-IP setting; use a clean development profile')
        # Minikube applies --static-ip to every new node. Bootstrap the first
        # node separately, then let Docker allocate the worker address normally.
        count = max(1, len(existing.get('Nodes', [])))
        command += ['--driver=docker', '--nodes='+str(count), '--cpus='+str(args.cpus),
                    '--memory='+str(args.memory), '--kubernetes-version=v1.35.0',
                    '--cni=calico',
                    '--delete-on-failure=false']
    result = subprocess.call(command, env=env)
    if result == 0 and args.command == 'start':
        state = json.loads(profile.read_text())
        if len(state['Nodes']) == 1:
            return subprocess.call([str(LOCAL/'bin/minikube'), '-p', 'aat-m2', 'node', 'add'], env=env)
    return result


if __name__ == '__main__':
    raise SystemExit(main())
