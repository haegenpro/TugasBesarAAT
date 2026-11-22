"""Small Kubernetes-DCS Patroni entrypoint for two-node demo formations."""
from __future__ import annotations

import ipaddress
import json
import os
from pathlib import Path
import re


def configuration(env: dict) -> dict:
    formation, name = env['FORMATION'], env['POD_NAME']
    if not re.fullmatch(r'[a-z][a-z0-9-]*', formation) or not re.fullmatch(r'[a-z][a-z0-9-]*', name):
        raise ValueError('invalid formation/member name')
    cidr = str(ipaddress.ip_network(env['POD_CIDR']))
    group = int(env['GROUP_ID'])
    host = f'{name}.{env["MEMBER_SERVICE"]}.{env["POD_NAMESPACE"]}.svc.cluster.local'
    hba = ['local all postgres peer', 'host all postgres 127.0.0.1/32 scram-sha-256',
           f'host all postgres {cidr} scram-sha-256',
           f'host replication replicator {cidr} scram-sha-256',
           'host replication replicator 127.0.0.1/32 scram-sha-256']
    pairs = [('auth', 'auth_app'), ('registry', 'registry_app'),
             ('dashboard', 'dashboard_app'), ('notifier', 'notifier_app')]
    if formation == 'services':
        for database, role in pairs:
            hba += [f'host {database} {role} {cidr} scram-sha-256',
                    f'host {database} {role} 127.0.0.1/32 scram-sha-256']
    else:
        database = 'hazards' if group >= 0 else 'ingestion'
        role = 'canonical_app' if group >= 0 else 'ingestion_app'
        hba += [f'host {database} {role} {cidr} scram-sha-256',
                f'host {database} {role} 127.0.0.1/32 scram-sha-256']
    hba += ['host all all all reject', 'local all all reject']
    parameters = {
        'shared_buffers': '32MB', 'work_mem': '1MB', 'maintenance_work_mem': '32MB',
        'max_connections': 64 if formation != 'services' else 80,
        'max_prepared_transactions': 128, 'max_wal_senders': 8, 'max_replication_slots': 8,
        'wal_level': 'logical', 'wal_log_hints': 'on', 'fsync': 'on',
        'full_page_writes': 'on', 'synchronous_commit': 'on',
        'tcp_keepalives_idle': 2, 'tcp_keepalives_interval': 1, 'tcp_keepalives_count': 3,
        'wal_sender_timeout': '5s', 'wal_receiver_timeout': '5s', 'max_slot_wal_keep_size': '512MB',
        'max_wal_size': '256MB', 'min_wal_size': '80MB', 'password_encryption': 'scram-sha-256',
        'idle_in_transaction_session_timeout': '10s', 'statement_timeout': '5s', 'lock_timeout': '2s',
    }
    if group >= 0:
        parameters.update({'citus.shard_count': 8, 'citus.recover_2pc_interval': 1000,
                           'citus.node_conninfo': 'connect_timeout=1',
                           'citus.max_adaptive_executor_pool_size': 1})
    conf = {
        'scope': formation, 'name': name,
        # B cannot independently become the coordinator or ingestion authority.
        # Worker 2 normally leads on B; coordinator access still depends on A.
        'tags': {'nofailover': env.get('PREFERRED_NODE') == 'b' and group != 2},
        'kubernetes': {'namespace': env['POD_NAMESPACE'], 'use_endpoints': False,
                       'scope_label': 'cluster-name', 'role_label': 'database-role',
                       'labels': {'app.kubernetes.io/part-of': 'aat-m2', 'cluster-name': formation},
                       'pod_ip': env['POD_IP']},
        'restapi': {'listen': '0.0.0.0:8008', 'connect_address': host+':8008',
                    'authentication': {'username': 'postgres', 'password': env['POSTGRES_PASSWORD']}},
        'bootstrap': {'dcs': {'ttl': 15, 'loop_wait': 2, 'retry_timeout': 3,
                               'synchronous_mode': True, 'synchronous_mode_strict': True,
                               'synchronous_node_count': 1, 'check_timeline': True,
                               # A leader that cannot reach the Kubernetes API keeps its role only
                               # while every other member confirms it over the REST API; a slow API
                               # server no longer demotes a healthy primary (retry_timeout is short).
                               'failsafe_mode': True, 'postgresql': {'use_pg_rewind': True,
                                                                     'parameters': parameters}},
                      'initdb': [{'encoding': 'UTF8'}, 'data-checksums']},
        'postgresql': {'listen': '0.0.0.0:5432', 'connect_address': host+':5432',
                       'data_dir': env['PGDATA'], 'bin_dir': '/usr/lib/postgresql/16/bin',
                       'pgpass': '/tmp/pgpass', 'basebackup': [{'checkpoint': 'fast'}],
                       'authentication': {
                           'superuser': {'username': 'postgres', 'password': env['POSTGRES_PASSWORD']},
                           'replication': {'username': 'replicator', 'password': env['REPLICATION_PASSWORD']},
                           'rewind': {'username': 'postgres', 'password': env['POSTGRES_PASSWORD']}},
                       'pg_hba': hba, 'parameters': {'cluster_name': name}},
    }
    if group >= 0:
        conf['citus'] = {'group': group, 'database': env['POSTGRES_DB']}
    return conf


def main() -> None:
    os.umask(0o077)
    data = Path(os.environ['PGDATA'])
    if data.is_symlink():
        raise RuntimeError('PGDATA cannot be a symlink')
    if data.exists():
        data.chmod(0o700)
    conf = configuration(os.environ)
    path = Path('/tmp/patroni.json')
    path.write_text(json.dumps(conf))
    path.chmod(0o600)
    os.execv('/opt/patroni/bin/patroni', ['patroni', str(path)])


if __name__ == '__main__':
    main()
