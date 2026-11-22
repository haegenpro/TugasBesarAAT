#!/usr/bin/env python3
"""Plan the deliberate empty-B test on the Minikube development cluster.

The Minikube counterpart of rebuild-b.py, which targets a dedicated K3s agent VM. Node B is
a Minikube node container; its application replicas live in the same owned directories
(hostPath, DirectoryOrCreate). With --execute this stops B's kubelet and every pod on B,
erases only B's application replicas with the same symlink-safe wipe_contents used by the
reset tool, and starts the kubelet again, so B rejoins with empty disks and its replicas are
rebuilt from A. It never touches node A, Kubernetes identity, credentials or observability.

Criteria: P1-5 (the same node with an empty disk rejoins and carries data and load without
stopping reads or writes); run evidence_outage_probe.py alongside, then
p1_placement_load.py distribution. Supports P1-6 afterwards.
"""
import argparse
import base64
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts/m2'))
from common import LOCAL, is_minikube, kubectl, owned_path, read_config
from reset import wipe_contents

NAMES = ['canonical-0-b', 'canonical-1-b', 'canonical-2-b',
         'ingestion-b', 'services-b', 'broker-b1']


def node_shell(cfg, node, command, timeout=180):
    """Run a shell command on a Minikube node (the profile is the configured context)."""
    env = os.environ.copy()
    env.update(MINIKUBE_HOME=str(LOCAL/'minikube'), KUBECONFIG=str(LOCAL/'kubeconfig'))
    result = subprocess.run([str(LOCAL/'bin/minikube'), '-p', cfg['context'], 'ssh', '-n', node, '--', command],
                            env=env, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f'command on {node} failed: {command.split()[0:3]} ({result.stderr.strip()[-200:]})')
    return result.stdout


def node_ready(cfg, node):
    status = json.loads(kubectl(cfg, 'get', 'node', node, '-o', 'json'))['status']['conditions']
    return any(c['type'] == 'Ready' and c['status'] == 'True' for c in status)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    cfg = read_config(args.config)
    if not is_minikube(cfg):
        parser.error('this is the Minikube variant; use deployment/m2/rebuild-b.py on the K3s B VM')
    node_a, node_b = cfg['nodes']['a'], cfg['nodes']['b']
    if node_b == node_a:
        parser.error('node B must differ from node A')
    labels = json.loads(kubectl(cfg, 'get', 'node', node_b, '-o', 'json'))['metadata'].get('labels', {})
    if 'node-role.kubernetes.io/control-plane' in labels:
        parser.error('node B is a control-plane node; refusing to erase it')
    paths = [owned_path(cfg, name) for name in NAMES]
    roots = {str(Path(p).parent) for p in paths}
    if len(roots) != 1:
        parser.error('owned paths do not share one application root')
    root = roots.pop()
    print(json.dumps({'profile': cfg['context'], 'node': node_b, 'execute': args.execute, 'paths': paths,
                      'preserved': ['node A data', 'Kubernetes identity', 'credentials', 'observability']}, indent=2), flush=True)
    if not args.execute:
        print('Dry run. --execute stops B, erases only these application replicas and restarts its kubelet.')
        return
    node_shell(cfg, node_b, 'sudo systemctl stop kubelet')
    # Every pod on B stops before node-local replicas are touched (kubelet first, so nothing restarts them).
    node_shell(cfg, node_b, "sudo sh -c 'crictl pods -q | xargs -r crictl stopp >/dev/null'")
    if node_shell(cfg, node_b, 'sudo crictl ps -q').strip():
        raise RuntimeError('containers still running on node B; refusing to erase replicas')
    print('Node B kubelet and pods stopped.', flush=True)
    program = inspect.getsource(wipe_contents) + f'\nwipe_contents({root!r}, {NAMES!r})\n'
    encoded = base64.b64encode(program.encode()).decode()
    node_shell(cfg, node_b, f'echo {encoded} | base64 -d | sudo python3 -')
    print('Erased node B application replicas: ' + ', '.join(NAMES), flush=True)
    node_shell(cfg, node_b, 'sudo systemctl start kubelet')
    deadline = time.monotonic() + 180
    while not node_ready(cfg, node_b):
        if time.monotonic() > deadline:
            raise RuntimeError('node B did not become Ready within 180 s after the kubelet restart')
        time.sleep(3)
    print('Node B kubelet restarted with empty application replicas and is Ready. Watch its replicas catch up from A '
          '(manage.py status, p1_placement_load.py distribution); do not initialize or reset A.')


if __name__ == '__main__':
    main()
