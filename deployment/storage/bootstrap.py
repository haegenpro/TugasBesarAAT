"""Called by Patroni once after initializing a physical group, as administrator."""
import json
import os
from pathlib import Path
import sys

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn

ROLE_ATTRIBUTES = "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"
# Owners of the shared service formation additionally cannot bypass row
# security or inherit another role's privileges, and have a connection cap.
OWNER_ATTRIBUTES = ROLE_ATTRIBUTES + " NOBYPASSRLS NOINHERIT"


def owned_databases():
    """Shared service formation: [{database, runtime_role, password_key, connection_limit}]."""
    raw = os.environ.get("OWNED_DATABASES")
    if not raw:
        return None
    entries = json.loads(raw)
    names = [e["database"] for e in entries]
    roles = [e["runtime_role"] for e in entries]
    if not entries or len(set(names)) != len(names) or len(set(roles)) != len(roles) or "postgres" in names:
        raise RuntimeError("invalid owned database declaration")
    for entry in entries:
        if type(entry["connection_limit"]) is not int or entry["connection_limit"] < 1:
            raise RuntimeError("owned database connection limit must be a positive integer")
    return entries


def ensure_role(cursor, role, password, limit=None):
    """Create a runtime role; with a limit, also enforce owner attributes on reuse."""
    attributes = ROLE_ATTRIBUTES if limit is None else OWNER_ATTRIBUTES
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
    if cursor.fetchone() is None:
        cursor.execute(sql.SQL("CREATE ROLE {} " + attributes + " PASSWORD %s").format(sql.Identifier(role)), (password,))
    if limit is not None:
        cursor.execute(sql.SQL("ALTER ROLE {} " + attributes + " CONNECTION LIMIT {}").format(
            sql.Identifier(role), sql.Literal(limit)))


def ensure_database(cursor, database):
    cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,))
    if cursor.fetchone() is None:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))


def install_identity(connection, database, roles):
    """Physical identity and replication probe; only listed roles may execute it."""
    target = dict(connection, dbname=database)
    with psycopg2.connect(**target) as conn:
        with conn.cursor() as cursor:
            cursor.execute(Path("/opt/platform/replication-probe.sql").read_text())
            formation, group = os.environ["FORMATION"], int(os.environ["GROUP_ID"])
            cursor.execute("INSERT INTO public.platform_storage_identity(singleton,formation,group_id) VALUES(true,%s,%s) ON CONFLICT DO NOTHING",
                           (formation, group))
            cursor.execute("SELECT formation,group_id FROM public.platform_storage_identity WHERE singleton")
            if cursor.fetchone() != (formation, group):
                raise RuntimeError("existing physical storage identity does not match formation/group")
            cursor.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(database)))
            cursor.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            for role in roles:
                cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), sql.Identifier(role)))
                cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
                cursor.execute(sql.SQL("GRANT EXECUTE ON FUNCTION public.platform_replication_status() TO {}").format(sql.Identifier(role)))
    conn.close()


def main():
    # Patroni supplies a superuser URL and PGPASSFILE. Never print either.
    connection = parse_dsn(sys.argv[1])
    database = os.environ["POSTGRES_DB"]
    owned = owned_databases()
    initial = psycopg2.connect(**connection)
    try:
        initial.autocommit = True
        with initial.cursor() as cursor:
            ensure_database(cursor, database)
            if owned is None:
                ensure_role(cursor, os.environ.get("APP_USER", "aggregator_app"), os.environ["APP_PASSWORD"])
            else:
                for entry in owned:
                    ensure_database(cursor, entry["database"])
                    ensure_role(cursor, entry["runtime_role"], os.environ[entry["password_key"]], entry["connection_limit"])
                # No owner may open the administrator databases either.
                for admin in ("postgres", "template1"):
                    cursor.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(admin)))
    finally:
        initial.close()
    if owned is None:
        install_identity(connection, database, [os.environ.get("APP_USER", "aggregator_app")])
    else:
        # Each owner receives CONNECT on its own database only.
        install_identity(connection, database, [])
        for entry in owned:
            install_identity(connection, entry["database"], [entry["runtime_role"]])
    # Application DDL/migrations are a separate offline administrator operation.


if __name__ == "__main__":
    main()
