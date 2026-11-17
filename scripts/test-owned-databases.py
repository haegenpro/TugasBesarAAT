#!/usr/bin/env python3
"""Isolated schema/grant checks, including real Registry bootstrap and runtime."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

from database_lib import schema_sql
from storage_lib import SERVICE_DATABASES

ROOT=Path(__file__).resolve().parent.parent

def main():
    os.umask(0o077)
    run=uuid.uuid4().hex[:12];network="aat-owner-check-"+run
    output=ROOT/"evidence/platform-databases"/run;output.mkdir(parents=True)
    password=secrets.token_hex(24)
    env={**os.environ,"POSTGRES_PASSWORD":password,"APICURIO_DATASOURCE_PASSWORD":password}
    created=[];network_created=False;checks=[]
    report={"passed":False,"scope":"single-copy schema/privilege and Registry image checks; not HA acceptance"}
    def docker(*args,input=None,check=True):
        result=subprocess.run(["docker",*map(str,args)],input=input,env=env,capture_output=True,text=True,timeout=90)
        if check and result.returncode:raise RuntimeError("Docker operation failed: "+result.stderr.replace(password,"[redacted]")[-1200:])
        return result
    def sql(db,text,check=True):return docker("exec","-i",network+"-pg","psql","-X","-U","postgres","-d",db,"-v","ON_ERROR_STOP=1","-Atf","-",input=text,check=check)
    def start_registry(name,initialize,username):
        docker("run","-d","--name",name,"--label","aat.owner.check="+run,"--network",network,"--memory","256m","--cpus","1",
               "-e","APICURIO_STORAGE_KIND=sql","-e","APICURIO_STORAGE_SQL_KIND=postgresql","-e","APICURIO_SQL_INIT="+initialize,
               "-e","APICURIO_DATASOURCE_URL=jdbc:postgresql://pg:5432/registry","-e","APICURIO_DATASOURCE_USERNAME="+username,
               "-e","APICURIO_DATASOURCE_PASSWORD","-e","REGISTRY_RETRY_DELAY_SEC=5","-e","QUARKUS_HTTP_PORT=8080","-e","QUARKUS_MANAGEMENT_PORT=9000",registry)
        created.append(name)
        deadline=time.monotonic()+65
        while docker("exec",name,"curl","-fsS","http://localhost:9000/health/ready",check=False).returncode:
            if time.monotonic()>deadline:
                logs=docker("logs",name,check=False).stdout
                (output/(name+".log")).write_text(logs.replace(password,"[redacted]"))
                raise RuntimeError("Registry readiness deadline; inspect private evidence")
            time.sleep(.5)
    try:
        # Resolve cached images to actual IDs; no network pulls or mutable image
        # claims are used in generated deployment artifacts.
        pg=docker("image","inspect","postgres:16-alpine3.24","--format","{{.Id}}").stdout.strip()
        registry=docker("image","inspect","aat-m1-prep-check-schema-registry:latest","--format","{{.Id}}").stdout.strip()
        report.update(postgres_image=pg,registry_image=registry)
        docker("network","create","--internal","--label","aat.owner.check="+run,network);network_created=True
        docker("run","-d","--name",network+"-pg","--label","aat.owner.check="+run,"--network",network,"--network-alias","pg","--memory","256m","--cpus","1","-e","POSTGRES_PASSWORD",pg)
        created.append(network+"-pg")
        deadline=time.monotonic()+35
        while docker("exec",network+"-pg","pg_isready","-U","postgres",check=False).returncode:
            if time.monotonic()>deadline:raise RuntimeError("PostgreSQL startup deadline")
            time.sleep(.2)
        probe=(ROOT/"store/migrations/0005_replication_probe.sql").read_text()
        for suffix,(db,role)in SERVICE_DATABASES.items():
            sql("postgres",f'CREATE DATABASE "{db}"; CREATE ROLE "{role}" LOGIN PASSWORD \'{password}\';')
            sql(db,probe+f'\nREVOKE ALL ON DATABASE "{db}" FROM PUBLIC; REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT CONNECT ON DATABASE "{db}" TO "{role}";')
            storage={"formation_suffix":suffix,"runtime_role":role}
            if suffix=="registry-db":
                if sql(db,schema_sql(storage),False).returncode==0:raise RuntimeError("uninitialized Registry schema accepted")
                start_registry(network+"-registry-init","true","postgres")
                docker("stop",network+"-registry-init")
            else:
                sql(db,"CREATE TABLE untracked_legacy_table(id int);")
                if sql(db,schema_sql(storage),False).returncode==0:raise RuntimeError("untracked schema silently adopted")
                sql(db,"DROP TABLE untracked_legacy_table;")
            for _ in range(2):sql(db,schema_sql(storage))
            table="sessions"if suffix=="auth-db"else"records"if suffix!="registry-db"else sql(db,"SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('platform_storage_identity','service_schema_migrations') ORDER BY tablename LIMIT 1;").stdout.strip()
            sql(db,f'SET ROLE "{role}"; SELECT count(*) FROM "{table}";')
            for denied in ('CREATE TABLE forbidden(id int);','SELECT * FROM platform_storage_identity;','UPDATE service_schema_migrations SET checksum=\'tampered\';'):
                if sql(db,f'SET ROLE "{role}"; '+denied,False).returncode==0:raise RuntimeError("runtime role received forbidden privilege")
            sql(db,"UPDATE service_schema_migrations SET checksum='invalid';")
            if sql(db,schema_sql(storage),False).returncode==0:raise RuntimeError("schema checksum mismatch accepted")
            checks.append(suffix+": initial/repeat initialization, runtime reads, forbidden DDL/identity/journal, checksum refusal")
        start_registry(network+"-registry-runtime","false","registry_app")
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
    except (OSError,RuntimeError,subprocess.SubprocessError)as exc:print("Isolated database checks stopped: "+str(exc),file=sys.stderr);sys.exit(1)
