"""Render explicit two-node Kubernetes resources without Helm or a YAML dependency."""
from __future__ import annotations

import json
from urllib.parse import quote

from common import (APPLICATION, LABEL, NAMESPACE, ROOT, config_digest, labels,
                    local_volume, resource, resources)

GROUPS = [('canonical', 0), ('canonical', 1), ('canonical', 2), ('ingestion', None), ('services', None)]
OWNERS = {'auth': 'auth-service', 'registry': 'schema-registry',
          'dashboard': 'consumer-dashboard', 'notifier': 'consumer-notifier'}
APPS = {
    'aggregator': ('aggregator', 8080, 3), 'auth-service': ('api', 8083, 2),
    'client-api': ('api', 8080, 2), 'consumer-dashboard': ('consumer', 8091, 2),
    'consumer-notifier': ('consumer', 8092, 2), 'ingestion': ('other', 8080, 2),
    'coastal-processor': ('consumer', 8080, 2), 'wildfire-processor': ('consumer', 8080, 2),
    'bmkg-mock': ('mock', 8081, 1), 'pvmbg-mock': ('mock', 8082, 1),
    'schema-registry': ('other', 8080, 1), 'sensor-pantai': ('sensor', 8080, 1),
    'situation-summarizer': ('other', 8094, 1),
}


def image(cfg: dict, component: str) -> str:
    return f'{cfg["image_prefix"].rstrip("/")}/{component}:{cfg["image_tag"]}'


def secret(name: str, values: dict) -> dict:
    return resource('Secret', name, type='Opaque', stringData={k: str(v) for k, v in values.items()})


def configmap(name: str, values: dict) -> dict:
    return resource('ConfigMap', name, data={k: str(v) for k, v in values.items()})


def service(name: str, selector: dict, ports: dict, headless=False, public=False) -> dict:
    spec = {'selector': selector, 'ports': [{'name': k, 'port': v, 'targetPort': v} for k, v in ports.items()]}
    if headless:
        spec.update(clusterIP='None', publishNotReadyAddresses=True)
    if public:
        spec['type'] = 'NodePort'
    return resource('Service', name, spec=spec)


def policy(name: str, selector: dict, callers: list[str], ports: list[int], public=False, stores=False) -> dict:
    sources = [{'podSelector': {'matchLabels': {'app': c}}} for c in callers]
    if stores:
        sources.append({'podSelector': {'matchLabels': {'role': 'store'}}})
    # The official tester must not be obstructed by our isolation policies.
    sources.append({'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'tester-agent'}}})
    rule = {'ports': [{'protocol': 'TCP', 'port': port} for port in ports]}
    if not public:
        rule['from'] = sources
    return resource('NetworkPolicy', name, spec={'podSelector': {'matchLabels': selector},
                                               'policyTypes': ['Ingress'], 'ingress': [rule]})


def pod_env(values: dict) -> list[dict]:
    env = [{'name': k, 'value': str(v)} for k, v in values.items()]
    for name, field in [('POD_NAME', 'metadata.name'), ('POD_NAMESPACE', 'metadata.namespace'),
                        ('POD_IP', 'status.podIP'), ('NODE_NAME', 'spec.nodeName')]:
        env.append({'name': name, 'valueFrom': {'fieldRef': {'fieldPath': field}}})
    return env


def postgres(cfg: dict, credentials: dict) -> list[dict]:
    result = []
    for formation, group in GROUPS:
        stem = f'canonical-{group}' if group is not None else formation
        selector = {'formation': formation}
        if group is not None:
            selector['citus-group'] = str(group)
        result.append(service(stem, {**selector, 'database-role': 'primary'}, {'postgres': 5432}))
        for node_id in ('a', 'b'):
            name = f'{stem}-{node_id}'
            node = cfg['nodes'][node_id]
            result += local_volume(cfg, name, node, cfg['limits']['postgres_volume_gib'])
            lab = {**labels(name, 'store'), **selector, 'cluster-name': formation}
            result.append(service(name+'-peer', {'app': name}, {'postgres': 5432, 'patroni': 8008}, headless=True))
            env = {'FORMATION': formation, 'GROUP_ID': str(group if group is not None else -1),
                   'MEMBER_SERVICE': name+'-peer', 'POD_CIDR': cfg['network']['pod_cidr'],
                   'PREFERRED_NODE': node_id, 'PGDATA': '/var/lib/postgresql/data/pgdata',
                   'POSTGRES_DB': 'hazards' if group is not None else formation,
                   'GOMEMLIMIT': '192MiB'}
            container = {'name': 'postgres', 'image': image(cfg, 'storage'),
                         'env': pod_env(env), 'envFrom': [{'secretRef': {'name': 'storage-runtime'}}],
                         'resources': resources('store'), 'ports': [{'containerPort': 5432}, {'containerPort': 8008}],
                         'volumeMounts': [{'name': 'data', 'mountPath': '/var/lib/postgresql/data'}],
                         'readinessProbe': {'httpGet': {'path': '/readiness', 'port': 8008}, 'periodSeconds': 3},
                         'livenessProbe': {'httpGet': {'path': '/liveness', 'port': 8008}, 'periodSeconds': 5,
                                           'failureThreshold': 6},
                         'startupProbe': {'httpGet': {'path': '/liveness', 'port': 8008},
                                          'periodSeconds': 5, 'failureThreshold': 60},
                         'securityContext': {'allowPrivilegeEscalation': False}}
            pod = {'serviceAccountName': 'storage', 'nodeSelector': {'kubernetes.io/hostname': node},
                   'terminationGracePeriodSeconds': 30, 'securityContext': {'fsGroup': 999},
                   'initContainers': [{'name': 'prepare-volume', 'image': image(cfg, 'storage'),
                                       'command': ['sh', '-ec', 'chown 999:999 /data; chmod 700 /data'],
                                       'securityContext': {'runAsUser': 0}, 'resources': resources('other'),
                                       'volumeMounts': [{'name': 'data', 'mountPath': '/data'}]}],
                   'containers': [container], 'volumes': [{'name': 'data', 'persistentVolumeClaim': {'claimName': name}}]}
            result.append(resource('StatefulSet', name, spec={'serviceName': name+'-peer', 'replicas': 1,
                'selector': {'matchLabels': {'app': name}}, 'template': {'metadata': {'labels': lab}, 'spec': pod}}))
        allowed = ['aggregator'] if formation == 'canonical' else (
            ['ingestion', 'coastal-processor', 'wildfire-processor'] if formation == 'ingestion' else list(OWNERS.values()))
        result.append(policy(stem+'-ownership', selector, allowed, [5432, 8008], stores=True))
    return result


def nats(cfg: dict) -> list[dict]:
    result = []
    names = ['broker-a1', 'broker-a2', 'broker-b1']
    for name in names:
        node = cfg['nodes']['a' if name.startswith('broker-a') else 'b']
        result += local_volume(cfg, name, node, cfg['limits']['broker_volume_gib'])
        routes = ','.join(f'"nats://{n}-0.{n}-peer:6222"' for n in names if n != name)
        text = '\n'.join([
            f'server_name: "{name}"', 'port: 4222', 'http_port: 8222',
            'authorization { users: [{user: "application", password: $NATS_PASSWORD}] }',
            f'cluster {{ name: "aat-m2", listen: "0.0.0.0:6222", routes: [{routes}] }}',
            f'jetstream {{ store_dir: "/data", max_memory_store: 32MB, max_file_store: {cfg["limits"]["broker_message_mib"]}MB, sync_interval: always }}',
        ])
        result.append(configmap(name, {'nats.conf': text}))
        result.append(service(name+'-peer', {'app': name}, {'client': 4222, 'route': 6222, 'monitor': 8222}, headless=True))
        lab = {**labels(name, 'broker'), 'component': 'broker'}
        container = {'name': 'broker', 'image': image(cfg, 'broker'),
                     'command': ['nats-server', '-c', '/config/nats.conf'],
                     'envFrom': [{'secretRef': {'name': 'broker-runtime'}}], 'resources': resources('broker'),
                     'ports': [{'containerPort': p} for p in (4222, 6222, 8222)],
                     'volumeMounts': [{'name': 'data', 'mountPath': '/data'}, {'name': 'config', 'mountPath': '/config'}],
                     'readinessProbe': {'httpGet': {'path': '/healthz?js-server-only=true', 'port': 8222}, 'periodSeconds': 3},
                     'livenessProbe': {'httpGet': {'path': '/healthz?js-enabled-only=true', 'port': 8222}, 'periodSeconds': 5}}
        result.append(resource('StatefulSet', name, spec={'serviceName': name+'-peer', 'replicas': 1,
            'selector': {'matchLabels': {'app': name}}, 'template': {'metadata': {'labels': lab}, 'spec': {
                'nodeSelector': {'kubernetes.io/hostname': node}, 'containers': [container],
                'securityContext': {'fsGroup': 1000},
                'volumes': [{'name': 'data', 'persistentVolumeClaim': {'claimName': name}},
                            {'name': 'config', 'configMap': {'name': name}}]}}}))
    result.append(service('broker', {'component': 'broker'}, {'client': 4222, 'monitor': 8222}))
    result.append(policy('broker', {'component': 'broker'}, list(APPS)+names, [4222, 6222, 8222]))
    return result


def bootstrap(cfg: dict, credentials: dict) -> list[dict]:
    # Initializers use administrator credentials; application pods never do.
    data = {'POSTGRES_PASSWORD': credentials['postgres_admin'],
            'REPLICATION_PASSWORD': credentials['replication']}
    for key in ('auth', 'registry', 'dashboard', 'notifier', 'canonical', 'ingestion'):
        data[key.upper()+'_PASSWORD'] = credentials[key+'_db']
    result = [resource('Namespace', NAMESPACE), resource('ServiceAccount', 'storage'),
              resource('Role', 'storage', rules=[
                  {'apiGroups': [''], 'resources': ['pods', 'endpoints', 'configmaps'],
                   'verbs': ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete']}]),
              resource('RoleBinding', 'storage', subjects=[{'kind': 'ServiceAccount', 'name': 'storage', 'namespace': NAMESPACE}],
                       roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'storage'}),
              resource('NetworkPolicy', 'default-deny-ingress', spec={'podSelector': {}, 'policyTypes': ['Ingress']}),
              secret('storage-runtime', data), secret('broker-runtime', {'NATS_PASSWORD': credentials['nats']})]
    return result


def render(cfg: dict, credentials: dict) -> dict:
    items = bootstrap(cfg, credentials) + postgres(cfg, credentials) + nats(cfg)
    return {'apiVersion': 'v1', 'kind': 'List', 'items': items}


def inventory(manifest: dict) -> dict:
    totals = {}
    volumes = []
    for obj in manifest['items']:
        if obj['kind'] == 'PersistentVolume':
            volumes.append({'name': obj['metadata']['name'], **obj['metadata']['annotations'],
                            'capacity': obj['spec']['capacity']['storage']})
        if obj['kind'] not in ('Deployment', 'StatefulSet', 'DaemonSet'):
            continue
        spec = obj['spec']['template']['spec']
        node = spec.get('nodeSelector', {}).get('kubernetes.io/hostname', 'scheduled')
        total = totals.setdefault(node, {'requests_mib': 0, 'limits_mib': 0})
        for c in spec['containers']:
            for field in ('requests', 'limits'):
                text = c['resources'][field]['memory']
                assert text.endswith('Mi')
                total[field+'_mib'] += int(text[:-2]) * obj['spec'].get('replicas', 1)
    return {'nodes': totals, 'volumes': volumes}
