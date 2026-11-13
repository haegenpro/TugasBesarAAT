"""Called by Patroni once after initializing a physical group, as administrator."""
import os
from pathlib import Path
import sys

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn


def main():
    # Patroni supplies a superuser URL and PGPASSFILE. Never print either.
    connection = parse_dsn(sys.argv[1])
    database = os.environ["POSTGRES_DB"]
    initial = psycopg2.connect(**connection)
    try:
        initial.autocommit = True
        with initial.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,))
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='aggregator_app'")
            if cursor.fetchone() is None:
                cursor.execute("CREATE ROLE aggregator_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %s",
                               (os.environ["APP_PASSWORD"],))
    finally:
        initial.close()
    connection["dbname"] = database
    with psycopg2.connect(**connection) as target:
        with target.cursor() as cursor:
            cursor.execute(Path("/opt/platform/replication-probe.sql").read_text())
            formation, group = os.environ["FORMATION"], int(os.environ["GROUP_ID"])
            cursor.execute("INSERT INTO public.platform_storage_identity(singleton,formation,group_id) VALUES(true,%s,%s) ON CONFLICT DO NOTHING",
                           (formation, group))
            cursor.execute("SELECT formation,group_id FROM public.platform_storage_identity WHERE singleton")
            if cursor.fetchone() != (formation, group):
                raise RuntimeError("existing physical storage identity does not match formation/group")
            cursor.execute("GRANT CONNECT ON DATABASE {} TO aggregator_app".format(sql.Identifier(database).as_string(target)))
            cursor.execute("GRANT USAGE ON SCHEMA public TO aggregator_app")
            cursor.execute("GRANT EXECUTE ON FUNCTION public.platform_replication_status() TO aggregator_app")
    # Application DDL/migrations are a separate offline administrator operation.


if __name__ == "__main__":
    main()
