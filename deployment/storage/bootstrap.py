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
    role=os.environ.get("APP_USER","aggregator_app")
    initial = psycopg2.connect(**connection)
    try:
        initial.autocommit = True
        with initial.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,))
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s",(role,))
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %s").format(sql.Identifier(role)),
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
            cursor.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(database)))
            cursor.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database),sql.Identifier(role)))
            cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
            cursor.execute(sql.SQL("GRANT EXECUTE ON FUNCTION public.platform_replication_status() TO {}").format(sql.Identifier(role)))
    # Application DDL/migrations are a separate offline administrator operation.


if __name__ == "__main__":
    main()
