"""Run inside a current primary, using its local administrator connection.

Provision credentials only; application schemas are owned/migrated by their
runtime roles. Re-running preserves databases and data.
"""
import os
import psycopg2
from psycopg2 import sql


def main():
    formation = os.environ['FORMATION']
    pairs = [('auth', 'auth_app'), ('registry', 'registry_app'),
             ('dashboard', 'dashboard_app'), ('notifier', 'notifier_app')]
    if formation != 'services':
        pairs = [('hazards' if formation == 'canonical' else 'ingestion', formation+'_app')]
    connection = psycopg2.connect(dbname='postgres', user='postgres')
    connection.autocommit = True
    with connection.cursor() as cursor:
        if formation == 'canonical':
            cursor.execute('SET citus.enable_ddl_propagation=off')
        cursor.execute('SELECT pg_is_in_recovery()')
        if cursor.fetchone()[0]:
            raise RuntimeError('initialization must target the current primary')
        for database, role in pairs:
            password = os.environ[('CANONICAL' if database == 'hazards' else database.upper())+'_PASSWORD']
            cursor.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,))
            if not cursor.fetchone():
                cursor.execute(sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS').format(sql.Identifier(role)))
            cursor.execute(sql.SQL('ALTER ROLE {} PASSWORD %s').format(sql.Identifier(role)), (password,))
            cursor.execute('SELECT 1 FROM pg_database WHERE datname=%s', (database,))
            if not cursor.fetchone():
                cursor.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(database), sql.Identifier(role)))
            cursor.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(database)))
            cursor.execute(sql.SQL('ALTER DATABASE {} OWNER TO {}').format(sql.Identifier(database), sql.Identifier(role)))
            with psycopg2.connect(dbname=database, user='postgres') as db:
                with db.cursor() as c:
                    if formation == 'canonical':
                        c.execute('SET citus.enable_ddl_propagation=off')
                    c.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
                    c.execute(sql.SQL('GRANT USAGE, CREATE ON SCHEMA public TO {}').format(sql.Identifier(role)))
                    if formation == 'canonical':
                        c.execute('CREATE EXTENSION IF NOT EXISTS citus')
                        if os.environ.get('GROUP_ID') == '0':
                            c.execute(sql.SQL('GRANT SELECT ON pg_dist_transaction TO {}').format(sql.Identifier(role)))
                        # This catalog is superuser-only. Runtime pods receive
                        # their own password, never database administrator keys.
                        c.execute('DELETE FROM pg_dist_authinfo WHERE nodeid=0 AND rolename=%s', (role,))
                        c.execute('INSERT INTO pg_dist_authinfo(nodeid,rolename,authinfo) VALUES(0,%s,%s)', (role, 'password='+password))
    connection.close()
    print('Runtime databases and isolated roles initialized.')


if __name__ == '__main__':
    main()
