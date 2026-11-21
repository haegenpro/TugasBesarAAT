#!/usr/bin/env python3
"""Read-only Ubuntu VM checks. Run on each real VM before installing K3s."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--interface', default='wg0')
    parser.add_argument('--address', required=True)
    parser.add_argument('--peer', required=True)
    args = parser.parse_args()
    address, peer = str(ipaddress.IPv4Address(args.address)), str(ipaddress.IPv4Address(args.peer))
    checks = []
    def check(name, passed, detail):
        checks.append({'check': name, 'passed': bool(passed), 'detail': detail})
    cpu = os.cpu_count() or 0
    memory = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:')))//1024
    disk = shutil.disk_usage('/var/lib')
    os_info = Path('/etc/os-release').read_text()
    check('ubuntu', 'ID=ubuntu\n' in os_info, os_info.splitlines()[0])
    check('processors', cpu >= 4, cpu)
    check('memory_mib', memory >= 7*1024, memory)
    check('disk_total_gib', disk.total >= 28*1024**3, round(disk.total/1024**3, 2))
    check('disk_free_gib', disk.free >= 8*1024**3, round(disk.free/1024**3, 2))
    try:
        result = subprocess.run(['ip', '-j', '-4', 'address', 'show', 'dev', args.interface],
                                capture_output=True, text=True, check=True)
        interfaces = json.loads(result.stdout)
        configured = any(a['local'] == address for i in interfaces for a in i.get('addr_info', []))
        check('node_address', configured, address+' on '+args.interface)
        check('interface_mtu', bool(interfaces) and interfaces[0]['mtu'] >= 1280,
              interfaces[0]['mtu'] if interfaces else None)
    except (OSError, subprocess.CalledProcessError):
        check('node_address', False, 'interface unavailable')
    try:
        result = subprocess.run(['ping', '-n', '-I', args.interface, '-c', '5', '-W', '2', peer],
                                capture_output=True, text=True, timeout=20)
        check('peer_ping', result.returncode == 0, result.stdout[-1500:])
    except (OSError, subprocess.TimeoutExpired):
        check('peer_ping', False, 'ping unavailable or timed out')
    root = Path('/var/lib/aat-m2')
    check('storage_root', not root.is_symlink() and (not root.exists() or root.is_dir()), str(root))
    check('clock_synchronized', subprocess.run(['timedatectl', 'show', '-p', 'NTPSynchronized', '--value'],
          capture_output=True, text=True).stdout.strip() == 'yes', 'timedatectl NTPSynchronized')
    print(json.dumps({'checks': checks, 'passed': all(c['passed'] for c in checks)}, indent=2))
    return 0 if all(c['passed'] for c in checks) else 1


if __name__ == '__main__':
    raise SystemExit(main())
