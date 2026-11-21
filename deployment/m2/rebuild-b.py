#!/usr/bin/env python3
"""Plan the deliberate empty-B test on a dedicated K3s agent VM.

Run only outside production and only as the declared B host. This does not erase
A or Kubernetes identity. It is deliberately separate from a full demo reset.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts/m2'))
from reset import wipe_contents

NAMES = ['canonical-0-b', 'canonical-1-b', 'canonical-2-b',
         'ingestion-b', 'services-b', 'broker-b1']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--hostname', required=True, help='exact Ubuntu hostname of B')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if args.hostname != socket.gethostname():
        parser.error('this is not the declared B hostname')
    if Path('/etc/systemd/system/k3s.service').exists():
        parser.error('a K3s server is present; refusing to erase a control-plane host')
    if not Path('/etc/systemd/system/k3s-agent.service').exists():
        parser.error('expected a dedicated K3s agent VM')
    root = Path('/var/lib/aat-m2')
    if not root.is_dir() or root.is_symlink():
        parser.error('expected a real dedicated application storage root')
    for name in NAMES:
        child = root/name
        if child.is_symlink() or (child.exists() and not child.is_dir()):
            parser.error('owned path is not a real directory: '+name)
    print(json.dumps({'host': args.hostname, 'execute': args.execute,
                      'paths': [str(root/name) for name in NAMES],
                      'preserved': ['A data', 'K3s identity', 'VPN', 'credentials']}, indent=2), flush=True)
    if not args.execute:
        print('Dry run. --execute stops B, erases only these application replicas and restarts its agent.')
        return
    if os.geteuid() != 0:
        parser.error('execution requires root on the dedicated B VM')
    killall = Path('/usr/local/bin/k3s-killall.sh')
    if not killall.is_file():
        parser.error('official K3s killall helper is missing; refusing to erase mounted stores')
    subprocess.run(['systemctl', 'stop', 'k3s-agent'], check=True)
    subprocess.run([str(killall)], check=True)
    # kubelet and its containers are down before touching node-local replicas.
    wipe_contents(str(root), NAMES)
    subprocess.run(['systemctl', 'start', 'k3s-agent'], check=True)
    print('B agent restarted with empty application replicas. Inspect catch-up and checksums from A; do not initialize or reset A.')


if __name__ == '__main__': main()
