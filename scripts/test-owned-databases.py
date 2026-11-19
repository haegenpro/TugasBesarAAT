#!/usr/bin/env python3
"""Isolated checks of the shared owned-database server: owner separation, schema/grants and real Registry bootstrap.

One PostgreSQL server hosts the four owner databases, configured like the shared
service formation: per-owner role and password, the platform-rendered pg_hba.conf,
revoked PUBLIC access and the shared connection budget. Every role is then tried
against every database over the network; only its own database may accept it.
"""
import importlib.util
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
from unittest.mock import patch
import uuid

from database_lib import EXTENSION_SQL,check_isolation,isolation_sql,schema_sql
from storage_lib import SERVICE_DATABASES,SERVICE_MAX_CONNECTIONS,service_databases

ROOT=Path(__file__).resolve().parent.parent
# Same statements as deployment/storage/bootstrap.py, in the same order.
OWNER_ATTRIBUTES="LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT"


def access_rules(entries,cidr):
    spec=importlib.util.spec_from_file_location("image_entrypoint",ROOT/"deployment/storage/entrypoint.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    declared=json.dumps([{k:e[k]for k in("database","runtime_role","password_key","connection_limit")}for e in entries])
    with patch.dict(os.environ,{"OWNED_DATABASES":declared}):return module.access_rules(cidr)


def main():
    os.umask(0o077)
    run=uuid.uuid4().hex[:12];network="aat-owner-check-"+run
    output=ROOT/"evidence/platform-databases"/run;output.mkdir(parents=True)
    entries=service_databases()
    superuser=secrets.token_hex(24);passwords={e["runtime_role"]:secrets.token_hex(24)for e in entries}
    hidden=[superuser,*passwords.values()]
    env={**os.environ,"POSTGRES_PASSWORD":superuser}
    created=[];network_created=False;checks=[]
    report={"passed":False,"scope":"single-copy shared-server owner isolation, schema/privilege and Registry image checks; not HA acceptance"}
    def redact(text):
        for value in hidden:text=text.replace(value,"[redacted]")
        return text
    def docker(*args,input=None,check=True,extra=None):
        result=subprocess.run(["docker",*map(str,args)],input=input,env={**env,**(extra or{})},capture_output=True,text=True,timeout=90)
        if check and result.returncode:raise RuntimeError("Docker operation failed: "+redact(result.stderr)[-1200:])
        return result
    def sql(db,text,check=True):return docker("exec","-i","-u","postgres",network+"-pg","psql","-X","-U","postgres","-d",db,"-v","ON_ERROR_STOP=1","-Atf","-",input=text,check=check)
    def login(role,db):
        # Network path through the server's own address on the isolated subnet.
        return docker("exec","-e","PGPASSWORD",network+"-pg","psql","-X","-h","pg","-U",role,"-d",db,"-Atc","SELECT current_user||'@'||current_database()",check=False,extra={"PGPASSWORD":passwords[role]})
    def start_registry(name,initialize,username,password):
        docker("run","-d","--name",name,"--label","aat.owner.check="+run,"--network",network,"--memory","256m","--cpus","1",
               "-e","APICURIO_STORAGE_KIND=sql","-e","APICURIO_STORAGE_SQL_KIND=postgresql","-e","APICURIO_SQL_INIT="+initialize,
               "-e","APICURIO_DATASOURCE_URL=jdbc:postgresql://pg:5432/registry","-e","APICURIO_DATASOURCE_USERNAME="+username,
               "-e","APICURIO_DATASOURCE_PASSWORD","-e","REGISTRY_RETRY_DELAY_SEC=5","-e","QUARKUS_HTTP_PORT=8080","-e","QUARKUS_MANAGEMENT_PORT=9000",registry,
               extra={"APICURIO_DATASOURCE_PASSWORD":password})
        created.append(name)
        deadline=time.monotonic()+65
        while docker("exec",name,"curl","-fsS","http://localhost:9000/health/ready",check=False).returncode:
            if time.monotonic()>deadline:
                logs=docker("logs",name,check=False).stdout
                (output/(name+".log")).write_text(redact(logs))
                raise RuntimeError("Registry readiness deadline; inspect private evidence")
            time.sleep(.5)
    try:
        # Resolve cached images to actual IDs; no network pulls or mutable image
        # claims are used in generated deployment artifacts.
        pg=docker("image","inspect","postgres:16-alpine3.24","--format","{{.Id}}").stdout.strip()
        # Locally built registry/ image; the Compose project name decides its tag.
        registry=docker("image","inspect",os.environ.get("OWNER_CHECK_REGISTRY_IMAGE","aat-m1-prep-check-schema-registry:latest"),"--format","{{.Id}}").stdout.strip()
        report.update(postgres_image=pg,registry_image=registry)
        docker("network","create","--internal","--label","aat.owner.check="+run,network);network_created=True
        cidr=json.loads(docker("network","inspect",network,"--format","{{json .IPAM.Config}}").stdout)[0]["Subnet"]
        docker("run","-d","--name",network+"-pg","--label","aat.owner.check="+run,"--network",network,"--network-alias","pg","--memory","1g","--cpus","1","-e","POSTGRES_PASSWORD",pg,
               "postgres","-c","max_connections="+str(SERVICE_MAX_CONNECTIONS),"-c","password_encryption=scram-sha-256")
        created.append(network+"-pg")
        deadline=time.monotonic()+35
        while docker("exec",network+"-pg","pg_isready","-U","postgres",check=False).returncode:
            if time.monotonic()>deadline:raise RuntimeError("PostgreSQL startup deadline")
            time.sleep(.2)
        probe=(ROOT/"store/migrations/0005_replication_probe.sql").read_text()
        identity="INSERT INTO public.platform_storage_identity(singleton,formation,group_id) VALUES(true,'owner-check',0) ON CONFLICT DO NOTHING;"
        for e in entries:
            sql("postgres",f'CREATE DATABASE "{e["database"]}";')
            sql("postgres",f'CREATE ROLE "{e["runtime_role"]}" {OWNER_ATTRIBUTES} PASSWORD \'{passwords[e["runtime_role"]]}\'; ALTER ROLE "{e["runtime_role"]}" {OWNER_ATTRIBUTES} CONNECTION LIMIT {e["connection_limit"]};')
        sql("postgres",'REVOKE ALL ON DATABASE "postgres" FROM PUBLIC; REVOKE ALL ON DATABASE "template1" FROM PUBLIC;')
        sql("postgres",probe+"\n"+identity+'\nREVOKE ALL ON DATABASE "postgres" FROM PUBLIC; REVOKE CREATE ON SCHEMA public FROM PUBLIC;')
        for e in entries:
            db,role=e["database"],e["runtime_role"]
            sql(db,probe+"\n"+identity+f'\nREVOKE ALL ON DATABASE "{db}" FROM PUBLIC; REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT CONNECT ON DATABASE "{db}" TO "{role}"; GRANT USAGE ON SCHEMA public TO "{role}"; GRANT EXECUTE ON FUNCTION public.platform_replication_status() TO "{role}";')
        rules=access_rules(entries,cidr)
        docker("exec","-i","-u","postgres",network+"-pg","sh","-c",'cat > "$PGDATA/pg_hba.conf"',input="\n".join(rules)+"\n")
        sql("postgres","SELECT pg_reload_conf();")
        time.sleep(1)
        storage={"formation_suffix":"service-db","databases":entries,"max_connections":SERVICE_MAX_CONNECTIONS}
        observed=json.loads(sql("postgres",isolation_sql(storage)+";").stdout)
        extensions={e["database"]:json.loads(sql(e["database"],EXTENSION_SQL+";").stdout)for e in entries}
        report["isolation"]=check_isolation(storage,observed,extensions)
        checks.append("catalog: CONNECT only on own database, restricted role attributes, no memberships, PUBLIC revoked, pg_hba pairs only, no dblink/postgres_fdw")
        matrix={}
        for e in entries:
            for target in entries:
                result=login(e["runtime_role"],target["database"]);allowed=result.returncode==0
                matrix[e["runtime_role"]+"@"+target["database"]]=allowed
                if allowed!=(e is target):raise RuntimeError("network login matrix differs: "+e["runtime_role"]+" -> "+target["database"])
                if allowed and result.stdout.strip()!=e["runtime_role"]+"@"+target["database"]:raise RuntimeError("unexpected session identity")
                if not allowed and "pg_hba.conf rejects connection"not in result.stderr:raise RuntimeError("cross-owner login refused for an unexpected reason: "+redact(result.stderr)[-300:])
            if login(e["runtime_role"],"postgres").returncode==0:raise RuntimeError("owner reached the administrator database")
        report["login_matrix"]=matrix
        checks.append("network: each owner logs in to its own database only; all 12 cross-owner and administrator logins rejected by pg_hba")
        for service,(db,role)in SERVICE_DATABASES.items():
            if service=="registry-db":
                if sql(db,schema_sql(service),False).returncode==0:raise RuntimeError("uninitialized Registry schema accepted")
                start_registry(network+"-registry-init","true","postgres",superuser)
                docker("stop",network+"-registry-init")
            else:
                sql(db,"CREATE TABLE untracked_legacy_table(id int);")
                if sql(db,schema_sql(service),False).returncode==0:raise RuntimeError("untracked schema silently adopted")
                sql(db,"DROP TABLE untracked_legacy_table;")
            for _ in range(2):sql(db,schema_sql(service))
            table="sessions"if service=="auth-db"else"records"if service!="registry-db"else sql(db,"SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('platform_storage_identity','service_schema_migrations') ORDER BY tablename LIMIT 1;").stdout.strip()
            sql(db,f'SET ROLE "{role}"; SELECT count(*) FROM "{table}";')
            for denied in ('CREATE TABLE forbidden(id int);','SELECT * FROM platform_storage_identity;','UPDATE service_schema_migrations SET checksum=\'tampered\';'):
                if sql(db,f'SET ROLE "{role}"; '+denied,False).returncode==0:raise RuntimeError("runtime role received forbidden privilege")
            sql(db,"UPDATE service_schema_migrations SET checksum='invalid';")
            if sql(db,schema_sql(service),False).returncode==0:raise RuntimeError("schema checksum mismatch accepted")
            checks.append(service+": initial/repeat initialization, runtime reads, forbidden DDL/identity/journal, checksum refusal")
        start_registry(network+"-registry-runtime","false","registry_app",passwords["registry_app"])
        checks.append("Registry ready using restricted runtime role with startup schema initialization disabled")
        report["passed"]=True
        print(json.dumps({"evidence":str(output),"passed":True,"checks":len(checks)}))
    finally:
        cleanup=[]
        for name in reversed(created):
            owner=docker("inspect","--format",'{{ index .Config.Labels "aat.owner.check" }}',name,check=False).stdout.strip()
            if owner==run and docker("rm","-f","-v",name,check=False).returncode:cleanup.append(name)
        if network_created:
            owner=docker("network","inspect","--format",'{{ index .Labels "aat.owner.check" }}',network,check=False).stdout.strip()
            if owner==run and docker("network","rm",network,check=False).returncode:cleanup.append(network)
        report.update(checks=checks,cleanup_required=cleanup)
        if cleanup:report["passed"]=False
        (output/"result.json").write_text(json.dumps(report,indent=2)+"\n")
    return int(not report["passed"])

if __name__=="__main__":
    try:sys.exit(main())
    except (OSError,RuntimeError,ValueError,subprocess.SubprocessError)as exc:print("Isolated database checks stopped: "+str(exc),file=sys.stderr);sys.exit(1)
