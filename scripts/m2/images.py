#!/usr/bin/env python3
"""Build/load application images without a registry or changing other projects."""
import argparse
import os
import subprocess
import shutil

from common import LOCAL, ROOT, read_config
from render import image, render
from common import runtime_secrets

COMPONENTS = ['storage', 'broker', 'schema-registry', 'bmkg-mock', 'pvmbg-mock',
              'aggregator', 'auth-service', 'client-api', 'consumer-dashboard',
              'consumer-notifier', 'situation-summarizer']


def require_space(minimum):
    free = shutil.disk_usage(ROOT).free / 1024**3
    if free < minimum:
        raise ValueError(f'only {free:.1f} GiB free; need {minimum:g} GiB before image operations. '
                         'Inspect docker system df; prune unused build cache without deleting volumes.')


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
    parser.add_argument('--include-runtime', action='store_true',
                        help='export/load pinned sensor and observability images as well')
    parser.add_argument('--min-free-gib', type=float, default=8,
                        help='required free disk before image operations (default: 8 GiB)')
    args = parser.parse_args()
    cfg = read_config(args.config)
    if args.min_free_gib < 2:
        parser.error('retain at least 2 GiB of free disk')
    require_space(args.min_free_gib)
    names = args.component or COMPONENTS
    env = os.environ.copy()
    env.update(MINIKUBE_HOME=str(LOCAL/'minikube'), KUBECONFIG=str(LOCAL/'kubeconfig'))
    tags = [image(cfg, n) for n in names]
    if args.include_runtime:
        manifest = render(cfg, runtime_secrets(LOCAL/'secrets.json'))
        for obj in manifest['items']:
            pod = obj.get('spec', {}).get('template', {}).get('spec', {})
            for container in pod.get('containers', []) + pod.get('initContainers', []):
                if container['image'] not in tags:
                    tags.append(container['image'])
    if args.command == 'export':
        for tag in tags:
            require_space(args.min_free_gib)
            present = subprocess.run(['docker', 'image', 'inspect', tag], stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL).returncode == 0
            if not present:
                if tag.startswith(cfg['image_prefix']+'/'):
                    raise ValueError('build the missing application image first: '+tag)
                subprocess.run(['docker', 'pull', tag], check=True)
        subprocess.run(['docker', 'save', '-o', args.output, *tags], check=True)
        import hashlib
        from pathlib import Path
        path = Path(args.output)
        with path.open('rb') as archive:
            digest = hashlib.file_digest(archive, 'sha256').hexdigest()
        path.with_suffix(path.suffix+'.sha256').write_text(digest+'  '+path.name+'\n')
        print('Exported image archive and SHA-256 checksum. Import the same archive on A and B.')
    elif args.command == 'load-minikube':
        for tag in tags:
            require_space(args.min_free_gib)
            subprocess.run(['docker','image','inspect',tag],stdout=subprocess.DEVNULL,check=True)
            subprocess.run([str(LOCAL/'bin/minikube'), '-p', cfg['context'], 'image', 'load', tag], env=env, check=True)
    else:
        for name in names:
            require_space(args.min_free_gib)
            build(cfg, name)


if __name__ == '__main__':
    main()
