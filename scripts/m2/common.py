"""Small, dependency-free shared infrastructure helpers; no application logic."""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import subprocess

ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / '.m2-local'
NAMESPACE = 'sistem-informasi-kebencanaan-bnpb'
LABEL = 'app.kubernetes.io/part-of'
APPLICATION = 'aat-m2'
MEMORY = {'aggregator': (256, 512), 'store': (512, 1024),
          'broker': (512, 1024), 'consumer': (128, 256),
          'api': (128, 256), 'mock': (64, 128), 'sensor': (128, 256),
          'other': (128, 256)}


def read_config(path: str | Path) -> dict:
    cfg = json.loads(Path(path).read_text())
    if cfg.get('version') != 1 or cfg.get('namespace') != NAMESPACE:
        raise ValueError('version=1 and the assignment application namespace are required')
    if cfg.get('profile') not in ('minikube', 'k3s') or not cfg.get('context'):
        raise ValueError('profile must be minikube or k3s; context must be explicit')
    nodes = cfg.get('nodes', {})
    if set(nodes) != {'a', 'b'} or nodes['a'] == nodes['b']:
        raise ValueError('two distinct node names (a and b) are required')
    for name in nodes.values():
        if not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]*', name):
            raise ValueError('invalid Kubernetes node name')
    path = PurePosixPath(cfg.get('storage_root', ''))
    # The reset tool uses this exact owned subtree. Never accept broad host roots.
    if not path.is_absolute() or '..' in path.parts or len(path.parts) < 4 or path.name != APPLICATION:
        raise ValueError('storage_root must be a dedicated absolute subtree ending in /aat-m2')
    if str(path) != cfg['storage_root'] or str(path).startswith('//'):
        raise ValueError('storage_root must be a normalized absolute path')
    if str(path).startswith(('/var/lib/rancher/', '/var/lib/kubelet/', '/etc/', '/home/')):
        raise ValueError('storage_root overlaps protected host configuration')
    net = cfg.get('network', {})
    pod = ipaddress.ip_network(net['pod_cidr'])
    service = ipaddress.ip_network(net['service_cidr'])
    if pod.overlaps(service):
        raise ValueError('pod and service CIDRs overlap')
    for key, lo, hi in [('broker_volume_gib', 1, 16), ('broker_message_mib', 64, 8192),
                        ('postgres_volume_gib', 1, 16), ('quarantine_mib', 1, 4096)]:
        value = cfg.get('limits', {}).get(key)
        if type(value) is not int or not lo <= value <= hi:
            raise ValueError(f'invalid limits.{key}')
    if cfg['limits']['broker_message_mib'] >= cfg['limits']['broker_volume_gib'] * 1024:
        raise ValueError('broker volume needs headroom above the message ceiling')
    satellite = cfg.get('satellite', {})
    if type(satellite.get('initial_offset')) is not int or satellite['initial_offset'] < 0:
        raise ValueError('satellite initial_offset must be nonnegative')
    if type(satellite.get('lateness_seconds')) is not int or satellite['lateness_seconds'] < 0:
        raise ValueError('satellite lateness_seconds must be nonnegative')
    from tuning import settings
    settings(cfg)
    return cfg


def private_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Refuse symlinks and set permissions at creation, before writing secrets.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as out:
        os.fchmod(out.fileno(), 0o600)
        json.dump(value, out, indent=2)
        out.write('\n')


def runtime_secrets(path: Path) -> dict:
    keys = ('postgres_admin', 'replication', 'auth_db', 'registry_db', 'dashboard_db',
            'notifier_db', 'canonical_db', 'ingestion_db', 'bmkg', 'pvmbg', 'admin',
            'auth_internal', 'field_read', 'priority', 'ingestion_internal', 'canonical_internal',
            'coastal', 'coastal_slo', 'nats', 'media', 'field', 'internal', 'grafana', 'audit')
    if path.is_symlink():
        raise ValueError('secret file cannot be a symlink')
    if path.exists():
        data = json.loads(path.read_text())
        if not isinstance(data, dict) or any(not data.get(k) for k in keys):
            raise ValueError('existing secrets are incomplete; refusing implicit credential regeneration')
        if path.stat().st_mode & 0o077:
            raise ValueError('secret file must be readable only by its owner (chmod 600)')
        return data
    data = {key: secrets.token_urlsafe(32) for key in keys}
    private_json(path, data)
    return data


def is_minikube(config: dict) -> bool:
    """The profile written by `manage.py init --profile` selects Minikube-only behaviour."""
    return config.get('profile') == 'minikube'


def kubectl_binary() -> str:
    """Prefer the private pinned binary; fall back to the host's kubectl (k3s hosts)."""
    return str(LOCAL / 'bin/kubectl') if (LOCAL / 'bin/kubectl').exists() else 'kubectl'


def kube_env(config: dict) -> dict:
    """Environment for kubectl; only Minikube uses the private kubeconfig."""
    env = os.environ.copy()
    if is_minikube(config) and (LOCAL / 'kubeconfig').exists():
        env['KUBECONFIG'] = str(LOCAL / 'kubeconfig')
    return env


def node_usage(config: dict) -> dict:
    """Kubelet CPU/memory summary per configured node; works on Minikube and k3s."""
    usage = {}
    for node in config['nodes'].values():
        try:
            summary = json.loads(kubectl(config, 'get', '--raw', f'/api/v1/nodes/{node}/proxy/stats/summary', timeout=30))
        except (RuntimeError, ValueError, subprocess.TimeoutExpired):
            usage[node] = None
            continue
        stats = summary.get('node', {})
        usage[node] = {'cpu_usage_nano_cores': stats.get('cpu', {}).get('usageNanoCores'),
                       'memory_working_set_bytes': stats.get('memory', {}).get('workingSetBytes'),
                       'memory_available_bytes': stats.get('memory', {}).get('availableBytes'),
                       'fs_available_bytes': stats.get('fs', {}).get('availableBytes')}
    return usage


def kubectl(config: dict, *args: str, input_text: str | None = None, timeout: int = 120) -> str:
    binary = kubectl_binary()
    env = kube_env(config)
    command = [binary, '--context', config['context'], *args]
    result = subprocess.run(command, input=input_text, text=True, capture_output=True, env=env, timeout=timeout)
    if result.returncode:
        # Kubernetes errors can include submitted Secret manifests. Never print them.
        raise RuntimeError(f'kubectl {args[0] if args else ""} failed (exit {result.returncode}); inspect the selected context')
    return result.stdout


def labels(name: str, role: str | None = None) -> dict:
    value = {LABEL: APPLICATION, 'app': name}
    if role:
        value['role'] = role
    return value


def resource(kind: str, name: str, namespace: str = NAMESPACE, **body) -> dict:
    group = 'apps/v1' if kind in ('Deployment', 'StatefulSet', 'DaemonSet') else 'v1'
    group = {'Job': 'batch/v1', 'NetworkPolicy': 'networking.k8s.io/v1',
             'Role': 'rbac.authorization.k8s.io/v1', 'RoleBinding': 'rbac.authorization.k8s.io/v1'}.get(kind, group)
    meta = {'name': name, 'labels': labels(name)}
    if kind not in ('Namespace', 'PersistentVolume'):
        meta['namespace'] = namespace
    return {'apiVersion': group, 'kind': kind, 'metadata': meta, **body}


def resources(role: str) -> dict:
    request, limit = MEMORY[role]
    return {'requests': {'memory': f'{request}Mi'}, 'limits': {'memory': f'{limit}Mi'}}


def owned_path(config: dict, name: str) -> str:
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', name):
        raise ValueError('invalid volume name')
    return str(PurePosixPath(config['storage_root']) / name)


def local_volume(config: dict, name: str, node: str, gib: int) -> list[dict]:
    annotations = {'aat.bnpb/owned-path': owned_path(config, name), 'aat.bnpb/node': node}
    pv = resource('PersistentVolume', name, spec={
        'capacity': {'storage': f'{gib}Gi'}, 'volumeMode': 'Filesystem',
        'accessModes': ['ReadWriteOnce'], 'persistentVolumeReclaimPolicy': 'Retain',
        'storageClassName': '', 'hostPath': {'path': owned_path(config, name), 'type': 'DirectoryOrCreate'},
        'claimRef': {'namespace': NAMESPACE, 'name': name},
        'nodeAffinity': {'required': {'nodeSelectorTerms': [{'matchExpressions': [
            {'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': [node]}]}]}}})
    pv['metadata']['annotations'] = annotations
    pvc = resource('PersistentVolumeClaim', name, spec={
        'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': f'{gib}Gi'}},
        'storageClassName': '', 'volumeName': name})
    pvc['metadata']['annotations'] = annotations
    return [pv, pvc]


def config_digest(config: dict) -> str:
    public = {k: v for k, v in config.items() if k != 'external'}
    return hashlib.sha256(json.dumps(public, sort_keys=True).encode()).hexdigest()
