"""Explicit stopped-owner initialization shared by CLI and maintenance workflow."""
import json
from pathlib import Path
import subprocess
import time
import uuid
from database_lib import EXTENSION_SQL,OWNERS,check_isolation,isolation_sql,schema_sql
from service_lib import DIGEST
from storage_lib import require,service_database


def original_primary(cluster,storage,group=None):
    group=group or storage["groups"][0];leaders=[];identities=set();statuses=[]
    for member in group["members"]:
        value=cluster.kubectl("exec","-n",storage["namespace"],member["name"],"-c","postgres","--","psql","-U","postgres","-d",storage.get("database","hazards"),"-Atc","SELECT public.platform_replication_status()")
        status=json.loads(value.stdout)
        require(status["formation"]==storage["formation"]and status["group_id"]==group["id"]and status["member"]==member["name"],"database physical owner mismatch")
        identities.add(status["system_id"]);statuses.append(status)
        if status["primary"]is True:leaders.append(member)
    require(len(leaders)==1 and len(identities)==1,"one primary and three copies of the same original database required")
    return leaders[0],statuses


def bootstrap_diagnostics(cluster,namespace,name,output):
    """Save private startup evidence before the temporary initializer is deleted."""
    output=Path(output);output.mkdir(parents=True,mode=0o700,exist_ok=True)
    commands={"pod.json":("get","pod",name,"-o","json"),
              "registry.log":("logs",name,"--all-containers=true","--tail=1000"),
              "events.json":("get","events","--field-selector","involvedObject.name="+name,"-o","json")}
    for filename,args in commands.items():
        try:
            result=cluster.kubectl("-n",namespace,*args,check=False,timeout=20)
            content=result.stdout if result.returncode==0 else result.stderr
        except (RuntimeError,OSError,subprocess.SubprocessError)as exc:
            content=str(exc).encode()
        path=output/filename;path.write_bytes(content);path.chmod(0o600)


def wait_registry_bootstrap(cluster,namespace,name,timeout=600):
    deadline=time.monotonic()+timeout
    while True:
        pod=json.loads(cluster.kubectl("get","pod",name,"-n",namespace,"-o","json",timeout=20).stdout)
        status=pod.get("status",{})
        if status.get("phase")in("Failed","Succeeded"):
            failures=[s.get("state",{}).get("terminated",{})for s in status.get("containerStatuses",[])]
            detail=", ".join(str(s.get("reason","terminated"))+" exit="+str(s.get("exitCode","?"))for s in failures if s)
            raise RuntimeError("Registry initializer terminated before readiness: "+name+" "+detail)
        if any(c.get("type")=="Ready"and c.get("status")=="True"for c in status.get("conditions",[])):return
        require(time.monotonic()<deadline,"Registry initializer readiness timed out: "+name)
        time.sleep(3)


def initialize(cluster,storage,service,registry_image=None,diagnostics=None):
    """Initialize one owner's database in the shared formation; its owner must be stopped."""
    ns=storage["namespace"];entry=service_database(storage,service)
    pods=json.loads(cluster.kubectl("get","pods","-n",ns,"-l","app.kubernetes.io/name="+OWNERS[service],"-o","json").stdout)["items"]
    require(not any(o.get("status",{}).get("phase")not in("Succeeded","Failed")for o in pods),"stop all owning application pods before schema initialization")
    leader,_=original_primary(cluster,storage)
    if registry_image:
        require(service=="registry-db"and DIGEST.fullmatch(registry_image),"registry bootstrap requires its actual pinned application image")
        run=uuid.uuid4().hex;name=cluster.cfg["cluster_id"]+"-registry-bootstrap-"+run[:8]
        env={"APICURIO_STORAGE_KIND":"sql","APICURIO_STORAGE_SQL_KIND":"postgresql","APICURIO_SQL_INIT":"true","APICURIO_DATASOURCE_URL":"jdbc:postgresql://registry-db:5432/"+entry["database"],"APICURIO_DATASOURCE_USERNAME":"postgres","REGISTRY_RETRY_DELAY_SEC":"5","QUARKUS_HTTP_PORT":"8080","QUARKUS_MANAGEMENT_PORT":"9000"}
        pod={"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":ns,"labels":{"app.kubernetes.io/name":"registry-db-init","role":"other","platform.aat/cluster":cluster.cfg["cluster_id"],"platform.aat/bootstrap":run}},"spec":{"restartPolicy":"Never","automountServiceAccountToken":False,"activeDeadlineSeconds":660,"containers":[{"name":"registry-bootstrap","image":registry_image,"env":[{"name":k,"value":v}for k,v in env.items()]+[{"name":"APICURIO_DATASOURCE_PASSWORD","valueFrom":{"secretKeyRef":{"name":storage["formation"]+"-credentials","key":"POSTGRES_PASSWORD"}}}],"resources":{"requests":{"memory":"128Mi","cpu":"100m"},"limits":{"memory":"256Mi","cpu":"1"}},"readinessProbe":{"httpGet":{"path":"/health/ready","port":9000},"periodSeconds":3,"timeoutSeconds":3}}]}}
        created=False
        try:
            cluster.kubectl("create","-f","-",input=json.dumps(pod).encode());created=True
            print("Waiting for Registry schema initializer: "+name,flush=True)
            wait_registry_bootstrap(cluster,ns,name)
        finally:
            if created:
                if diagnostics is not None:
                    bootstrap_diagnostics(cluster,ns,name,diagnostics)
                    print("Registry startup diagnostics: "+str(diagnostics),flush=True)
                current=cluster.kubectl("get","pod",name,"-n",ns,"-o","json",check=False)
                if current.returncode==0 and json.loads(current.stdout)["metadata"].get("labels",{}).get("platform.aat/bootstrap")==run:
                    cluster.kubectl("delete","pod",name,"-n",ns,"--wait=true","--timeout=30s",timeout=40)
    cluster.kubectl("exec","-i","-n",ns,leader["name"],"-c","postgres","--","psql","-U","postgres","-d",entry["database"],"-v","ON_ERROR_STOP=1","-f","-",input=schema_sql(service).encode(),timeout=60)


def verify_isolation(cluster,storage):
    """Check owner separation on every copy: privileges, role attributes, pg_hba and extensions."""
    reports=[]
    for member in storage["groups"][0]["members"]:
        def query(database,text):
            result=cluster.kubectl("exec","-n",storage["namespace"],member["name"],"-c","postgres","--","psql","-U","postgres","-d",database,"-v","ON_ERROR_STOP=1","-Atc",text)
            return json.loads(result.stdout)
        observed=query(storage["database"],isolation_sql(storage))
        extensions={d["database"]:query(d["database"],EXTENSION_SQL)for d in storage["databases"]}
        reports.append({"member":member["name"],**check_isolation(storage,observed,extensions)})
    return {"passed":True,"members":reports}
