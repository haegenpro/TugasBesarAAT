#!/usr/bin/env python3
"""Check production source-observer SQL on three disposable PostgreSQL fixtures."""
from datetime import datetime,timedelta,timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

from source_validation_lib import Observer,analyze,literal

ROOT=Path(__file__).resolve().parent.parent


def main():
    os.umask(0o077);run=uuid.uuid4().hex[:12];prefix="aat-source-validation-"+run
    output=ROOT/"evidence/platform-source-validation"/run;output.mkdir(parents=True)
    created=[];containers={};captures={};password=secrets.token_hex(32);env={**os.environ,"POSTGRES_PASSWORD":password}
    report={"passed":False,"scope":"real local PostgreSQL query/fixture reconciliation; no live source, Citus, replication or real-node acceptance"}
    def docker(*args,**kw):return subprocess.run(["docker",*args],env=env,capture_output=True,timeout=60,**kw)
    def sql(name,database,query):
        value=docker("exec","-i",name,"psql","-qAt","-v","ON_ERROR_STOP=1","-U","postgres","-d",database,"-f","-",input=query.encode())
        if value.returncode:raise RuntimeError("fixture SQL failed; "+value.stderr.decode().replace(password,"[redacted]"))
        return value.stdout
    try:
        probe=(ROOT/"store/migrations/0005_replication_probe.sql").read_text()
        for suffix in("storage","ingestion","notifier-db"):
            name=prefix+"-"+suffix;member="test-"+suffix+"-0";containers[member]=name
            value=docker("run","-d","--name",name,"--label","aat.source.validation="+run,"--network","none","--memory","256m","-e","POSTGRES_PASSWORD","postgres:16-alpine3.24","-c","cluster_name="+member)
            if value.returncode:raise RuntimeError("temporary PostgreSQL could not start")
            created.append(name);deadline=time.monotonic()+30
            while docker("exec",name,"pg_isready","-h","127.0.0.1","-U","postgres").returncode:
                if time.monotonic()>deadline:raise RuntimeError("temporary PostgreSQL startup deadline")
                time.sleep(.2)
            database="hazards"if suffix!="notifier-db"else "notifier"
            sql(name,"postgres","CREATE DATABASE "+database+";")
            sql(name,database,probe+"\nINSERT INTO platform_storage_identity VALUES(true,'test-"+suffix+"',0);")
            identity=json.loads(sql(name,database,"SELECT public.platform_replication_status();"))["system_id"]
            captures[suffix]={"formation":"test-"+suffix,"groups":[{"id":0,"system_id":identity,"members":[{"name":member}]}]}
        ids=["warning-v1:"+"a"*64];payloads=[b'{"operation":"test-0"}',b'{"operation":"test-1"}']
        reference={"version":1,"satellite":{"namespace":"test-satellite","records":[{"offset":i,"payload_sha256":hashlib.sha256(raw).hexdigest()}for i,raw in enumerate(payloads)]},"coastal":{"namespace":"test-coastal","critical_notification_ids":ids},"audit":{"source":"SATELIT","count":2,"checksum":"c"*64}}
        sql(containers["test-storage-0"],"hazards","CREATE TABLE input_sessions(source text,namespace text,received_through bigint,prepared_through bigint,applied_through bigint);INSERT INTO input_sessions VALUES('SATELIT','test-satellite',1,1,1);CREATE TABLE input_history(source text,namespace text,position bigint,original_payload bytea,state text,envelope jsonb DEFAULT '{}'::jsonb);"+"".join("INSERT INTO input_history(source,namespace,position,original_payload,state) VALUES('SATELIT','test-satellite',"+str(i)+",decode('"+raw.hex()+"','hex'),'applied');"for i,raw in enumerate(payloads)))
        sql(containers["test-ingestion-0"],"hazards",(ROOT/"aggregator/internal/ingestion/migrations/0004_coastal_accounting.sql").read_text())
        first=datetime.now(timezone.utc)-timedelta(seconds=3);committed=first+timedelta(seconds=2)
        sql(containers["test-ingestion-0"],"hazards","INSERT INTO coastal_warning_expected(notification_id,namespace,observation_id,operation_id,payload_sha256,first_received_at,observed_at) VALUES("+literal(ids[0])+",'test-coastal','obs','operation','payload',"+literal(first.isoformat())+",'observed');")
        sql(containers["test-notifier-db-0"],"notifier",(ROOT/"consumer-notifier/schema.sql").read_text())
        sql(containers["test-notifier-db-0"],"notifier","INSERT INTO coastal_warning_receipts VALUES("+literal(ids[0])+",'warning-digest','obs',"+literal(first.isoformat())+","+literal(first.isoformat())+","+literal(committed.isoformat())+");")
        class API:
            cfg={"namespace":"test"}
            def kubectl(self,*args,**kwargs):
                # Execute the observer's exact psql argv/input against the
                # fixture selected by its captured member identity.
                member=args[4];command=args[args.index("--")+1:]
                return docker("exec","-i",containers[member],*command,input=kwargs["input"])
        observer=Observer(API(),{"satellite_namespace":"test-satellite"},{"durability":captures})
        observed=observer.collect(reference);observed["audit"]={**reference["audit"],"partial":False,"unavailable_units":[]}
        initial=analyze(reference,observed,max_clock_uncertainty_ms=2000)
        if not initial["passed"]:raise RuntimeError("fixture reconciliation failed: "+json.dumps(initial["failures"]))
        sql(containers["test-notifier-db-0"],"notifier","DELETE FROM coastal_warning_receipts;")
        missing=observer.collect(reference);missing["audit"]=observed["audit"]
        if analyze(reference,missing,max_clock_uncertainty_ms=2000)["passed"]:raise RuntimeError("missing receipt was incorrectly accepted")
        sql(containers["test-notifier-db-0"],"notifier","INSERT INTO coastal_warning_receipts VALUES("+literal(ids[0])+",'warning-digest','obs',"+literal(first.isoformat())+","+literal(first.isoformat())+","+literal(committed.isoformat())+");")
        sql(containers["test-storage-0"],"hazards","UPDATE input_history SET original_payload='changed'::bytea WHERE position=0;")
        changed=observer.collect(reference);changed["audit"]=observed["audit"]
        checked=analyze(reference,changed,max_clock_uncertainty_ms=2000)
        if checked["passed"]or checked["missing_receipts"]or not any("satellite payload"in failure for failure in checked["failures"]):raise RuntimeError("changed payload was not independently rejected")
        captures["storage"]["groups"][0]["system_id"]="999999"
        try:observer.progress()
        except ValueError as exc:
            if "original captured physical member"not in str(exc):raise
        else:raise RuntimeError("wrong original identity was incorrectly accepted")
        report.update(passed=True,checks={"actual_read_only_SQL_and_complete_fixture":True,"missing_receipt_refused":True,"changed_payload_refused":True,"wrong_identity_refused":True},observed_clock_uncertainty_ms=initial["clock_uncertainty_ms"])
    finally:
        cleanup=[]
        for name in reversed(created):
            identity=docker("inspect","--format",'{{ index .Config.Labels "aat.source.validation" }}',name).stdout.decode().strip()
            if identity==run and docker("rm","-f","-v",name).returncode:cleanup.append(name)
        report["cleanup_required"]=cleanup
        if cleanup:report["passed"]=False
        (output/"result.json").write_text(json.dumps(report,indent=2)+"\n")
    print(json.dumps({"evidence":str(output),**report}));return int(not report["passed"])


if __name__=="__main__":sys.exit(main())
