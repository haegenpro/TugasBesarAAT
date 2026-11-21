"""Ordered startup and bounded readiness checks; never clears persistent data."""
import json
import time
from common import APPLICATION, LABEL, LOCAL, NAMESPACE, kubectl

PASSIVE = {'storage-guardian', 'prometheus', 'loki', 'jaeger', 'grafana',
           'fluent-bit-a', 'fluent-bit-b'}


def objects(cfg, kinds):
    return json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', kinds, '-l',
                              f'{LABEL}={APPLICATION}', '-o', 'json'))['items']


def wait_for(description, check, timeout=600):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        pending = check()
        if not pending:
            print(description + ': ready.', flush=True)
            return
        message = ', '.join(pending)
        if message != last:
            print(description + ': waiting for ' + message, flush=True)
            last = message
        time.sleep(3)
    raise RuntimeError(description + ' timed out; inspect status and pod events')


def stop_writers(cfg):
    for obj in objects(cfg, 'deployment'):
        name = obj['metadata']['name']
        if name not in PASSIVE:
            kubectl(cfg, '-n', NAMESPACE, 'scale', 'deployment/'+name, '--replicas=0')
    wait_writers(cfg)


def wait_writers(cfg):
    def pending():
        return [p['metadata']['name'] for p in objects(cfg, 'pods')
                if p['metadata'].get('labels', {}).get('app') not in PASSIVE
                and any(o['kind'] == 'ReplicaSet' for o in p['metadata'].get('ownerReferences', []))
                and p['status']['phase'] not in ('Succeeded', 'Failed')]
    wait_for('Writers stopped', pending, 180)


def require_offline(cfg):
    running = [o['metadata']['name'] for o in objects(cfg, 'deployment')
               if o['metadata']['name'] not in PASSIVE and o['spec'].get('replicas', 1)]
    if running:
        raise ValueError('schema changes require stopped writers; use up or stop first')
    wait_writers(cfg)


def ready(cfg, foundation=False, timeout=600):
    kinds = 'statefulset' if foundation else 'deployment,statefulset'
    def pending():
        out = []
        controllers = objects(cfg,kinds)
        if not controllers: return ['managed controllers']
        for obj in controllers:
            desired = obj['spec'].get('replicas', 1)
            status = obj.get('status', {})
            if (status.get('observedGeneration', 0) < obj['metadata']['generation']
                    or status.get('readyReplicas', 0) != desired
                    or (desired and status.get('updatedReplicas', 0) != desired)):
                out.append(obj['metadata']['name'])
        if not foundation:
            for job in objects(cfg, 'jobs'):
                if job['metadata']['labels'].get('app') != 'broker-initialize':
                    continue
                if job.get('status', {}).get('failed', 0) > job['spec'].get('backoffLimit', 6):
                    raise RuntimeError('broker initialization failed; inspect its logs')
                if not job.get('status', {}).get('succeeded'):
                    out.append(job['metadata']['name'])
        return out
    wait_for('Foundation' if foundation else 'Application', pending, timeout)


def up(cfg):
    # Imports stay here so render's application/monitoring modules remain acyclic.
    from common import runtime_secrets
    from manage import initialize, migrate, migrate_services, validate_nodes
    from render import render
    validate_nodes(cfg)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    # Compile both manifests before interrupting a running application.
    base = render(cfg, credentials, foundation=True)
    full = render(cfg, credentials)
    stop_writers(cfg)
    enabled={o['metadata']['name'] for o in full['items'] if o['kind']=='Deployment'}
    for obj in objects(cfg,'deployment'):
        if obj['metadata']['name'] not in enabled:
            kubectl(cfg,'-n',NAMESPACE,'scale','deployment/'+obj['metadata']['name'],'--replicas=0')
    kubectl(cfg, 'apply', '-f', '-', input_text=json.dumps(base))
    ready(cfg, foundation=True)
    initialize(cfg)
    migrate(cfg)
    migrate_services(cfg)
    kubectl(cfg, 'apply', '-f', '-', input_text=json.dumps(full))
    ready(cfg)
    for path in (LOCAL/'stopped.json', LOCAL/'reset-pending.json'):
        path.unlink(missing_ok=True)
    print('Schemas and workloads ready. Source/reference and acceptance checks are separate.')
