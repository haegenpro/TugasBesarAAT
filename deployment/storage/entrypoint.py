"""Patroni configuration for the rendered platform formation (not a test)."""
import os
from pathlib import Path

import yaml


def main():
    os.umask(0o077)
    name = os.environ["POD_NAME"]
    host = f"{name}.{os.environ['MEMBER_SERVICE']}.{os.environ['POD_NAMESPACE']}.svc.cluster.local"
    parameters = {
        "shared_buffers": "32MB", "max_connections": 64,
        "work_mem": "1MB", "maintenance_work_mem": "16MB",
        "max_prepared_transactions": 128, "max_wal_senders": 8,
        "max_replication_slots": 8, "wal_level": "logical",
        "wal_log_hints": "on", "fsync": "on", "full_page_writes": "on",
        "synchronous_commit": "on", "password_encryption": "scram-sha-256",
        "idle_in_transaction_session_timeout": "15s",
    }
    if os.environ["STORAGE_MODE"] == "citus":
        # Citus 14.2 uses built-in 2PC; the former GUC is rejected.
        parameters.update({"citus.shard_count": 4, "citus.recover_2pc_interval": 1000})
    config = {
        "scope": os.environ["FORMATION"], "namespace": "/aat-platform/", "name": name,
        "etcd3": {
            "hosts": os.environ["ETCD_ENDPOINTS"].split(","), "protocol": "https",
            "cacert": "/etcd-tls/ca.crt", "cert": "/etcd-tls/tls.crt", "key": "/etcd-tls/tls.key",
        },
        "restapi": {"listen": "0.0.0.0:8008", "connect_address": host + ":8008",
                    "authentication": {"username": "admin", "password": os.environ["PATRONI_PASSWORD"]}},
        "bootstrap": {
            "dcs": {"ttl": 15, "loop_wait": 2, "retry_timeout": 3,
                    "synchronous_mode": "quorum", "synchronous_mode_strict": True,
                    "synchronous_node_count": 1, "check_timeline": True,
                    "failsafe_mode": False,
                    "postgresql": {"use_pg_rewind": True, "parameters": parameters}},
            "initdb": [{"encoding": "UTF8"}, "data-checksums"],
            "post_bootstrap": "/opt/patroni/bin/python /opt/platform/bootstrap.py",
        },
        "postgresql": {
            "listen": "0.0.0.0:5432", "connect_address": host + ":5432",
            "data_dir": "/var/lib/postgresql/data/pgdata", "bin_dir": "/usr/lib/postgresql/16/bin",
            "pgpass": "/tmp/platform-pgpass", "basebackup": [{"checkpoint": "fast"}],
            "parameters": {"cluster_name": name},
            "authentication": {
                "superuser": {"username": "postgres", "password": os.environ["POSTGRES_PASSWORD"]},
                "replication": {"username": "replicator", "password": os.environ["REPLICATION_PASSWORD"]},
                "rewind": {"username": "postgres", "password": os.environ["POSTGRES_PASSWORD"]},
            },
            "pg_hba": [
                "local all postgres peer", "local all all scram-sha-256",
                "host all all 127.0.0.1/32 scram-sha-256",
                f"host all all {os.environ['POD_CIDR']} scram-sha-256",
                f"host replication replicator {os.environ['POD_CIDR']} scram-sha-256",
            ],
        },
    }
    if os.environ["STORAGE_MODE"] == "citus":
        config["citus"] = {"group": int(os.environ["GROUP_ID"]), "database": os.environ["POSTGRES_DB"]}
    path = Path("/tmp/platform-patroni.yml")
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    os.execv("/opt/patroni/bin/patroni", ["patroni", str(path)])


if __name__ == "__main__":
    main()
