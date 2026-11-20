#!/usr/bin/env python3
"""Focused isolated protocol checks in owned temporary containers; no VM actions."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parent.parent
CITUS="citusdata/citus@sha256:d107b77c5d07ed435b72da9c57f6e96494912a52dc84c6a4113de8eb920e7261"

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--citus",action="store_true")
    parser.add_argument("--native",action="store_true",help="run Go on the controller via the published loopback ports")
    a=parser.parse_args()
    os.umask(0o077);run=uuid.uuid4().hex[:12];prefix="aat-platform-check-"+run
    output=ROOT/"evidence/platform-integration"/run;output.mkdir(parents=True)
    report={"run_id":run,"started_at":datetime.now(timezone.utc).isoformat(),"passed":False,"scope":"isolated single-copy protocol regression, not four-node replication/failover acceptance"}
    password=secrets.token_hex(18)
    env={name:value for name,value in os.environ.items()
         if not name.startswith(("M2_","M3_","M4_","TEST_DATABASE_")) and name!="DATABASE_URL"}
    env["POSTGRES_PASSWORD"]=password;created=[];network=False
    def docker(*args,check=True):return subprocess.run(["docker",*map(str,args)],env=env,capture_output=True,text=True,check=check,timeout=180)
    def owned(name):return docker("inspect","--format",'{{ index .Config.Labels "aat.platform.check" }}',name,check=False).stdout.strip()==run
    try:
        docker("network","create","--label","aat.platform.check="+run,prefix);network=True
        urls={}
        for alias in (("coord","worker1","worker2")if a.citus else("coord",)):
            name=prefix+"-"+alias;image=CITUS if a.citus else"postgres:16-alpine3.24"
            docker("run","-d","--name",name,"--label","aat.platform.check="+run,"--memory","768m","--cpus","1","--network",prefix,"--network-alias",alias,"-p","127.0.0.1::5432","-e","POSTGRES_PASSWORD",image,"-c","shared_buffers=32MB","-c","max_prepared_transactions=128",*(["-c","citus.recover_2pc_interval=1000"]if a.citus else[]));created.append(name)
            deadline=time.monotonic()+45
            while docker("exec",name,"pg_isready","-h","127.0.0.1","-U","postgres",check=False).returncode:
                if time.monotonic()>deadline:raise RuntimeError("temporary database startup deadline")
                time.sleep(.2)
            port=docker("port",name,"5432/tcp").stdout.strip();urls[alias]="postgres://postgres:"+password+"@"+port+"/postgres?sslmode=disable"
        test_env={"M2_DATABASE_URL":urls["coord"]}
        for index in (1,2):
            if "worker"+str(index)in urls:test_env["M2_WORKER"+str(index)+"_URL"]=urls["worker"+str(index)]
        args=[]
        if not a.native:
            runner=prefix+"-go"
            docker("run","-d","--name",runner,"--label","aat.platform.check="+run,"--network","host","--memory","2g","--cpus","3","--mount","type=bind,src="+str(ROOT)+",dst=/src,readonly","-v","r3-go-mod:/go/pkg/mod","-v","r3-go-build:/root/.cache/go-build","golang:1.27.1-alpine3.24","tail","-f","/dev/null");created.append(runner)
            args=["docker","exec"]
            for name in test_env:args += ["-e",name]
            args += ["-w","/src/aggregator",runner]
        pattern="TestMultiEntity|TestObservedPractice|TestReplication|TestM6Normalized|TestM2RecoveryAndPublicationFence|TestM2OwnerAndAdmission"
        if a.citus:pattern += "|TestM2LostCommitReply|TestM2Visibility|TestM4"
        args += ["go","test","-mod=readonly","-count=1","-timeout=180s","-json","./internal/store","./internal/satellite","./internal/wildfireprocessor","-run",pattern]
        with(output/"tests.log").open("w")as log:
            process=subprocess.run(args,env={**env,**test_env},cwd=ROOT/"aggregator",capture_output=True,text=True,timeout=600)
            log.write((process.stdout+process.stderr).replace(password,"[redacted]"))
        counts={"pass":0,"fail":0,"skip":0}
        for line in process.stdout.splitlines():
            try:event=json.loads(line)
            except ValueError:continue
            if event.get("Test")and event.get("Action")in counts:counts[event["Action"]]+=1
        report.update(passed=process.returncode==0,returncode=process.returncode,counts=counts,citus=a.citus)
        print(json.dumps({"evidence":str(output),"passed":report["passed"],"counts":counts}))
        return int(not report["passed"])
    finally:
        cleanup=[]
        for name in reversed(created):
            if owned(name)and docker("rm","-f","-v",name,check=False).returncode:cleanup.append(name)
        if network:
            identity=docker("network","inspect","--format",'{{ index .Labels "aat.platform.check" }}',prefix,check=False).stdout.strip()
            if identity==run and docker("network","rm",prefix,check=False).returncode:cleanup.append(prefix)
        report["cleanup_required"]=cleanup;report["finished_at"]=datetime.now(timezone.utc).isoformat()
        if cleanup:report["passed"]=False
        (output/"result.json").write_text(json.dumps(report,indent=2)+"\n")

if __name__=="__main__":
    try:sys.exit(main())
    except (OSError,RuntimeError,subprocess.SubprocessError)as exc:print("Isolated checks failed: "+type(exc).__name__,file=sys.stderr);sys.exit(1)
