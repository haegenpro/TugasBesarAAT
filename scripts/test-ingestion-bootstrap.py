#!/usr/bin/env python3
"""Check ingestion migrations against real isolated Patroni-style bootstrap SQL."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parent.parent


def main():
    os.umask(0o077);run=uuid.uuid4().hex[:12];prefix="aat-ingestion-bootstrap-"+run;created=[]
    output=ROOT/"evidence/platform-ingestion-bootstrap"/run;output.mkdir(parents=True)
    password=secrets.token_hex(32)
    env={**os.environ,"POSTGRES_PASSWORD":password,"TEST_POSTGRES_URL":"postgresql://postgres:"+password+"@127.0.0.1:5432/postgres?sslmode=disable"}
    report={"passed":False,"scope":"real PostgreSQL migration/guard checks, not replication or four-host acceptance"}
    def docker(*args,check=True):return subprocess.run(["docker",*map(str,args)],env=env,capture_output=True,text=True,timeout=180,check=check)
    try:
        pg=prefix+"-pg"
        docker("run","-d","--name",pg,"--label","aat.ingestion.check="+run,"--network","none","--memory","256m","-e","POSTGRES_PASSWORD","postgres:16-alpine3.24");created.append(pg)
        deadline=time.monotonic()+30
        while docker("exec",pg,"pg_isready","-U","postgres",check=False).returncode:
            if time.monotonic()>deadline:raise RuntimeError("isolated PostgreSQL startup deadline")
            time.sleep(.2)
        runner=prefix+"-go"
        docker("create","--name",runner,"--label","aat.ingestion.check="+run,"--network","container:"+pg,"--memory","2g","--cpus","3","--mount","type=bind,src="+str(ROOT)+",dst=/src,readonly","-v","r3-go-mod:/go/pkg/mod","-v","r3-go-build:/root/.cache/go-build","-e","TEST_POSTGRES_URL","-w","/src/aggregator","golang:1.27.1-alpine3.24","go","test","-mod=readonly","-count=1","-timeout=90s","-json","./internal/ingestion","-run","^TestPlatformIngestionBootstrapMigration$");created.append(runner)
        result=docker("start","-a",runner,check=False)
        (output/"checks.log").write_text((result.stdout+result.stderr).replace(password,"[redacted]"))
        counts={"pass":0,"fail":0,"skip":0}
        for line in result.stdout.splitlines():
            try:event=json.loads(line)
            except ValueError:continue
            if event.get("Test")and event.get("Action")in counts:counts[event["Action"]]+=1
        report.update(passed=result.returncode==0 and counts["pass"]>=8 and not counts["skip"],counts=counts)
    finally:
        cleanup=[]
        for name in reversed(created):
            if docker("inspect","--format",'{{ index .Config.Labels "aat.ingestion.check" }}',name,check=False).stdout.strip()==run:
                if docker("rm","-f","-v",name,check=False).returncode:cleanup.append(name)
        report["cleanup_required"]=cleanup
        if cleanup:report["passed"]=False
        (output/"result.json").write_text(json.dumps(report,indent=2)+"\n")
    print(json.dumps({"evidence":str(output),**report}))
    return int(not report["passed"])


if __name__=="__main__":sys.exit(main())
