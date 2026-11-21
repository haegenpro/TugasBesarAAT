#!/usr/bin/env python3
"""Expose one application browser locally; no public ingress or credentials."""
import argparse
import os
import subprocess
from common import LOCAL, NAMESPACE, kube_env, kubectl_binary, read_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('service', choices=('client-api', 'grafana', 'prometheus', 'jaeger'))
    parser.add_argument('--port', type=int, default=0)
    args = parser.parse_args()
    cfg = read_config(args.config)
    remote = {'client-api': 8080, 'grafana': 3000, 'prometheus': 9090, 'jaeger': 16686}[args.service]
    local = args.port or remote
    if not 1024 <= local <= 65535:
        raise ValueError('choose an unprivileged local port')
    env = kube_env(cfg)
    binary = kubectl_binary()
    print(f'{args.service}: http://127.0.0.1:{local} (Ctrl-C to close)', flush=True)
    subprocess.run([binary, '--context', cfg['context'], '-n', NAMESPACE, 'port-forward',
                    '--address=127.0.0.1', 'service/'+args.service, f'{local}:{remote}'], env=env, check=True)


if __name__ == '__main__': main()
