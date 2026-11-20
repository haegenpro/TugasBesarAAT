#!/usr/bin/env python3
"""Build/load application images without a registry or changing other projects."""
import argparse
import os
import subprocess

from common import LOCAL, ROOT, read_config
from render import image

COMPONENTS = ['storage', 'broker', 'schema-registry', 'bmkg-mock', 'pvmbg-mock',
              'aggregator', 'auth-service', 'client-api', 'consumer-dashboard',
              'consumer-notifier', 'situation-summarizer']


def build(cfg, component):
    if component == 'storage':
        command = ['docker', 'build', '-t', image(cfg, component), '-f',
                   'deployment/m2/storage/Dockerfile', '.']
    else:
        directory = 'registry' if component == 'schema-registry' else component
        if not (ROOT/directory/'Dockerfile').exists():
            raise ValueError(f'{component}: source/Dockerfile not installed yet')
        command = ['docker', 'build', '-t', image(cfg, component), directory]
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('command', choices=('build', 'load-minikube', 'export'))
    parser.add_argument('--component', action='append', choices=COMPONENTS)
    parser.add_argument('--output', default=str(LOCAL/'images.tar'))
    args = parser.parse_args()
    cfg = read_config(args.config)
    names = args.component or COMPONENTS
    env = os.environ.copy()
    env.update(MINIKUBE_HOME=str(LOCAL/'minikube'), KUBECONFIG=str(LOCAL/'kubeconfig'))
    if args.command == 'export':
        subprocess.run(['docker', 'save', '-o', args.output, *[image(cfg, n) for n in names]], check=True)
    else:
        for name in names:
            if args.command == 'build':
                build(cfg, name)
            else:
                subprocess.run([str(LOCAL/'bin/minikube'), '-p', cfg['context'], 'image', 'load', image(cfg, name)],
                               env=env, check=True)


if __name__ == '__main__':
    main()
