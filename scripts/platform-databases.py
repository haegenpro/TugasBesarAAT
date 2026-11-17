#!/usr/bin/env python3
"""Render owned database formations or explicitly initialize stopped services' schemas."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from database_lib import OWNERS,render,schema_sql
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from service_lib import DIGEST
from storage_lib import SERVICE_DATABASES,plan,require,write_json

ROOT=Path(__file__).resolve().parent.parent

def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["render","initialize","bootstrap-registry"])
    p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.fourhost.example.json")
    p.add_argument("--image");p.add_argument("--output",type=Path);p.add_argument("--plan",type=Path);p.add_argument("--writers-stopped",action="store_true",help="acknowledge maintenance window; all owning application pods must be stopped")
    a=p.parse_args();inventory=load(a.inventory)
    if a.action=="render":
        require(a.image and a.output,"render needs immutable --image and --output")
        resources,report=render(inventory,a.image);require(not a.output.exists(),"output already exists")
        a.output.mkdir(parents=True,mode=0o700);write_json(a.output/"resources.json",resources);write_json(a.output/"requirements.json",report)
        for storage in report["formations"]:write_json(a.output/(storage["formation_suffix"]+"-plan.json"),storage)
        return
    require(a.writers_stopped and a.plan,"administrator initialization requires --writers-stopped and an original --plan")
    storage=json.loads(a.plan.read_text());suffix=storage.get("formation_suffix")
    require(suffix in SERVICE_DATABASES and storage==plan(inventory,"postgres",storage["image"],suffix),"owner storage plan differs")
    c=Cluster(inventory);ns=inventory["namespace"]
    with c.locked():
        pods=json.loads(c.kubectl("get","pods","-n",ns,"-l","app.kubernetes.io/name="+OWNERS[suffix],"-o","json").stdout)["items"]
        require(not any(o.get("status",{}).get("phase")not in("Succeeded","Failed")for o in pods),"stop all owning application pods before schema initialization")
        if a.action=="bootstrap-registry":
            require(suffix=="registry-db"and a.image and DIGEST.fullmatch(a.image),"registry bootstrap requires its actual pinned application image")
            run_id=uuid.uuid4().hex;name=inventory["cluster_id"]+"-registry-bootstrap-"+run_id[:8]
            env={"APICURIO_STORAGE_KIND":"sql","APICURIO_STORAGE_SQL_KIND":"postgresql","APICURIO_SQL_INIT":"true","APICURIO_DATASOURCE_URL":"jdbc:postgresql://registry-db:5432/registry","APICURIO_DATASOURCE_USERNAME":"postgres","REGISTRY_RETRY_DELAY_SEC":"5","QUARKUS_HTTP_PORT":"8080","QUARKUS_MANAGEMENT_PORT":"9000"}
            pod={"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":ns,"labels":{"app.kubernetes.io/name":"registry-db-init","role":"other","platform.aat/cluster":inventory["cluster_id"],"platform.aat/bootstrap":run_id}},"spec":{"restartPolicy":"Never","automountServiceAccountToken":False,"activeDeadlineSeconds":300,"containers":[{"name":"registry-bootstrap","image":a.image,"env":[{"name":k,"value":v}for k,v in env.items()]+[{"name":"APICURIO_DATASOURCE_PASSWORD","valueFrom":{"secretKeyRef":{"name":storage["formation"]+"-credentials","key":"POSTGRES_PASSWORD"}}}],"resources":{"requests":{"memory":"128Mi","cpu":"100m"},"limits":{"memory":"256Mi","cpu":"1"}},"readinessProbe":{"httpGet":{"path":"/health/ready","port":9000},"periodSeconds":3}}]}}
            try:
                c.kubectl("create","-f","-",input=json.dumps(pod).encode())
                c.kubectl("wait","--for=condition=Ready","pod/"+name,"-n",ns,"--timeout=240s",timeout=270)
            finally:
                current=c.kubectl("get","pod",name,"-n",ns,"-o","json",check=False)
                if current.returncode==0 and json.loads(current.stdout)["metadata"].get("labels",{}).get("platform.aat/bootstrap")==run_id:c.kubectl("delete","pod",name,"-n",ns,"--wait=true","--timeout=30s",timeout=40)
        else:
            leaders=[];identities=set()
            for member in storage["groups"][0]["members"]:
                value=c.kubectl("exec","-n",ns,member["name"],"-c","postgres","--","psql","-U","postgres","-d",storage["database"],"-Atc","SELECT public.platform_replication_status()")
                status=json.loads(value.stdout)
                require(status["formation"]==storage["formation"] and status["group_id"]==0 and status["member"]==member["name"],"database physical owner mismatch")
                identities.add(status["system_id"])
                if status["primary"]:leaders.append(member)
            require(len(leaders)==1 and len(identities)==1,"one primary and three copies of the same original database required")
            c.kubectl("exec","-i","-n",ns,leaders[0]["name"],"-c","postgres","--","psql","-U","postgres","-d",storage["database"],"-v","ON_ERROR_STOP=1","-f","-",input=schema_sql(storage).encode(),timeout=60)
        print("Explicit owner database initialization complete; no runtime application credential performed DDL.")

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:print("Owner database operation stopped: "+str(exc),file=sys.stderr);sys.exit(1)
