#!/usr/bin/env python3
"""Plan an owned-volume wipe. Deletion requires --execute; default is read-only."""
import argparse
import json
from pathlib import PurePosixPath
import sys
import uuid
from common import (APPLICATION, LABEL, LOCAL, NAMESPACE, config_digest, kubectl,
                    private_json, read_config, resource, resources, runtime_secrets)
from render import image, render

OBSERVABILITY = {'prometheus', 'loki', 'grafana'}


def wipe_contents(root, names):
    """Runs inside a node-pinned helper with only the dedicated root mounted.

    Directory file descriptors and rmtree's symlink-safe implementation prevent
    following an unexpected symlink outside that mount. Keep this function stdlib
    only: its exact source is embedded in the Job, and exercised by local tests.
    """
    import os
    import shutil
    import stat
    if not shutil.rmtree.avoids_symlink_attacks:
        raise RuntimeError('a symlink-safe rmtree implementation is required')
    descriptor = os.open(root, os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        # Check the whole plan before removing the first directory.
        for name in names:
            if '/' in name or name in ('', '.', '..'):
                raise ValueError('invalid child directory')
            try:
                mode = os.stat(name, dir_fd=descriptor, follow_symlinks=False).st_mode
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(mode):
                raise ValueError('owned volume is not a real directory: '+name)
        for name in names:
            try:
                shutil.rmtree(name, dir_fd=descriptor)
            except FileNotFoundError:
                pass
    finally:
        os.close(descriptor)


def reset_plan(cfg, scope, actual):
    # Secret values never enter the returned plan or its printed JSON.
    expected = {o['metadata']['name']: o for o in render(cfg, runtime_secrets(LOCAL/'secrets.json'))['items']
                if o['kind'] == 'PersistentVolume'}
    found = {o['metadata']['name']: o for o in actual}
    plan = []
    for name, pv in expected.items():
        if (name in OBSERVABILITY) != (scope == 'observability'):
            continue
        live = found.get(name)
        if live is None:
            raise ValueError('expected owned volume is missing: '+name)
        annotations = pv['metadata']['annotations']
        if (live['metadata'].get('labels', {}).get(LABEL) != APPLICATION
                or any(live['metadata'].get('annotations', {}).get(k) != v for k, v in annotations.items())
                or live['spec'].get('hostPath') != pv['spec']['hostPath']
                or live['spec'].get('nodeAffinity') != pv['spec']['nodeAffinity']
                or live['spec'].get('claimRef', {}).get('namespace') != NAMESPACE
                or live['spec'].get('claimRef', {}).get('name') != name):
            raise ValueError('volume ownership/path/node mapping changed: '+name)
        path = PurePosixPath(annotations['aat.bnpb/owned-path'])
        if str(path.parent) != cfg['storage_root'] or path.name != name:
            raise ValueError('volume is outside the dedicated storage root')
        plan.append({'volume': name, 'node': annotations['aat.bnpb/node'], 'path': str(path)})
    return plan


def cleanup_job(cfg, node, names):
    import inspect
    program = inspect.getsource(wipe_contents) + '\nwipe_contents("/owned", '+repr(names)+')\n'
    return resource('Job', 'storage-reset-'+uuid.uuid4().hex[:10], spec={
        'backoffLimit': 0, 'activeDeadlineSeconds': 180, 'ttlSecondsAfterFinished': 3600,
        'template': {'metadata': {'labels': {LABEL: APPLICATION, 'app': 'storage-reset', 'role': 'other'}},
                     'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                              'nodeSelector': {'kubernetes.io/hostname': node},
                              'containers': [{'name': 'wipe', 'image': image(cfg, 'storage'),
                                  'command': ['/opt/patroni/bin/python', '-c', program],
                                  'securityContext': {'runAsUser': 0}, 'resources': resources('other'),
                                  'volumeMounts': [{'name': 'owned', 'mountPath': '/owned'}]}],
                              'volumes': [{'name': 'owned', 'hostPath': {'path': cfg['storage_root'], 'type': 'Directory'}}]}}})


def execute(cfg, scope, plan):
    from manage import stop, validate_nodes
    from lifecycle import objects, wait_for
    validate_nodes(cfg)
    stop(cfg)
    for job in objects(cfg, 'jobs'):
        if not job.get('status', {}).get('succeeded'):
            kubectl(cfg, '-n', NAMESPACE, 'delete', 'job', job['metadata']['name'])
    def pending():
        return [p['metadata']['name'] for p in objects(cfg, 'pods')
                if p['status']['phase'] not in ('Succeeded', 'Failed')]
    wait_for('All application pods stopped', pending, 180)
    if scope == 'application':
        private_json(LOCAL/'reset-pending.json', {'context': cfg['context'], 'digest': config_digest(cfg)})
    for node in cfg['nodes'].values():
        names = [v['volume'] for v in plan if v['node'] == node]
        if not names:
            continue
        job = cleanup_job(cfg, node, names)
        kubectl(cfg, 'apply', '-f', '-', input_text=json.dumps(job))
        kubectl(cfg, '-n', NAMESPACE, 'wait', '--for=condition=complete',
                'job/'+job['metadata']['name'], '--timeout=190s', timeout=200)
        print('Cleared reviewed directories on '+node, flush=True)
    if scope == 'application':
        # Empty PGDATA cannot be bootstrapped with stale Patroni initialization
        # records. Delete only this application's five formation DCS records.
        records = objects(cfg, 'configmaps')
        for record in records:
            if record['metadata'].get('labels', {}).get('cluster-name') in ('canonical', 'ingestion', 'services'):
                kubectl(cfg, '-n', NAMESPACE, 'delete', 'configmap', record['metadata']['name'])
        for job in objects(cfg,'jobs'):
            if job['metadata']['name'].startswith('broker-initialize-'):
                kubectl(cfg,'-n',NAMESPACE,'delete','job',job['metadata']['name'])
        print('Application data cleared, including source history/cursors. Run manage.py up to initialize empty stores.')
    else:
        print('Observability data cleared. Run manage.py start to restore the saved replica counts.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--scope', choices=('application', 'observability'), default='application')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    try:
        cfg = read_config(args.config)
        actual = json.loads(kubectl(cfg, 'get', 'pv', '-l', f'{LABEL}={APPLICATION}', '-o', 'json'))['items']
        plan = reset_plan(cfg, args.scope, actual)
        print(json.dumps({'context': cfg['context'], 'namespace': NAMESPACE, 'scope': args.scope,
                          'execute': args.execute, 'volumes': plan}, indent=2), flush=True)
        if args.execute:
            execute(cfg, args.scope, plan)
        else:
            print('Dry run: no controllers or files changed. Add --execute to erase these directories.')
        return 0
    except (ValueError, RuntimeError, OSError) as error:
        print('Reset refused: '+str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
