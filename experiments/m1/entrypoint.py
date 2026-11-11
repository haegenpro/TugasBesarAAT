"""Experimental Patroni configuration; never imports the application's env files."""
import os
from pathlib import Path

import yaml

name = os.environ['M1_NAME']
ha = os.environ['M1_SUITE'] == 'ha'
password = os.environ['M1_PASSWORD']
parameters = {
    'shared_buffers': '32MB', 'max_connections': 32,
    'work_mem': '1MB', 'maintenance_work_mem': '16MB',
    'max_prepared_transactions': 64, 'max_wal_senders': 8,
    'max_replication_slots': 8, 'wal_level': 'logical',
    'wal_log_hints': 'on', 'fsync': 'on', 'full_page_writes': 'on',
    'synchronous_commit': 'on', 'citus.shard_count': 4,
    'citus.recover_2pc_interval': 1000,
}
config = {
    'scope': os.environ['M1_SCOPE'], 'namespace': '/role1-m1/', 'name': name,
    'citus': {'group': int(os.environ['M1_GROUP']), 'database': 'm1'},
    'etcd3': {'hosts': os.environ['M1_ETCD'].split(',')},
    'restapi': {
        'listen': '0.0.0.0:8008', 'connect_address': name + ':8008',
        'authentication': {'username': 'admin', 'password': password},
    },
    'bootstrap': {
        'dcs': {
            'ttl': 15, 'loop_wait': 2, 'retry_timeout': 3,
            'synchronous_mode': 'quorum' if ha else False,
            'synchronous_mode_strict': ha, 'synchronous_node_count': 1,
            'failsafe_mode': False,
            'postgresql': {'use_pg_rewind': True, 'parameters': parameters},
        },
        'initdb': [{'encoding': 'UTF8'}, 'data-checksums'],
    },
    'postgresql': {
        'listen': '0.0.0.0:5432', 'connect_address': name + ':5432',
        'data_dir': '/var/lib/postgresql/data/pgdata',
        'bin_dir': '/usr/lib/postgresql/16/bin',
        # Avoid pg_basebackup's spread-checkpoint startup delay in this small lab.
        'basebackup': [{'checkpoint': 'fast'}],
        'authentication': {
            'superuser': {'username': 'postgres', 'password': password},
            'replication': {'username': 'replicator', 'password': password},
            'rewind': {'username': 'postgres', 'password': password},
        },
        'pg_hba': [
            'local all all trust',
            'host all all 0.0.0.0/0 scram-sha-256',
            'host replication replicator 0.0.0.0/0 scram-sha-256',
        ],
    },
}
path = Path('/tmp/patroni-m1.yml')
path.write_text(yaml.safe_dump(config), encoding='utf-8')
os.execv('/opt/patroni/bin/patroni', ['patroni', str(path)])
