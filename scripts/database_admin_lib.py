"""Explicit stopped-owner initialization shared by CLI and maintenance workflow."""
import json
import uuid
from database_lib import OWNERS,schema_sql
from service_lib import DIGEST
from storage_lib import require


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


def initialize(cluster,storage,registry_image=None):
    ns=storage["namespace"];suffix=storage["formation_suffix"]
    pods=json.loads(cluster.kubectl("get","pods","-n",ns,"-l","app.kubernetes.io/name="+OWNERS[suffix],"-o","json").stdout)["items"]
    require(not any(o.get("status",{}).get("phase")not in("Succeeded","Failed")for o in pods),"stop all owning application pods before schema initialization")
    leader,_=original_primary(cluster,storage)
    if registry_image:
        require(suffix=="registry-db"and DIGEST.fullmatch(registry_image),"registry bootstrap requires its actual pinned application image")
        run=uuid.uuid4().hex;name=cluster.cfg["cluster_id"]+"-registry-bootstrap-"+run[:8]
        env={"APICURIO_STORAGE_KIND":"sql","APICURIO_STORAGE_SQL_KIND":"postgresql","APICURIO_SQL_INIT":"true","APICURIO_DATASOURCE_URL":"jdbc:postgresql://registry-db:5432/registry","APICURIO_DATASOURCE_USERNAME":"postgres","REGISTRY_RETRY_DELAY_SEC":"5","QUARKUS_HTTP_PORT":"8080","QUARKUS_MANAGEMENT_PORT":"9000"}
        pod={"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":ns,"labels":{"app.kubernetes.io/name":"registry-db-init","role":"other","platform.aat/cluster":cluster.cfg["cluster_id"],"platform.aat/bootstrap":run}},"spec":{"restartPolicy":"Never","automountServiceAccountToken":False,"activeDeadlineSeconds":300,"containers":[{"name":"registry-bootstrap","image":registry_image,"env":[{"name":k,"value":v}for k,v in env.items()]+[{"name":"APICURIO_DATASOURCE_PASSWORD","valueFrom":{"secretKeyRef":{"name":storage["formation"]+"-credentials","key":"POSTGRES_PASSWORD"}}}],"resources":{"requests":{"memory":"128Mi","cpu":"100m"},"limits":{"memory":"256Mi","cpu":"1"}},"readinessProbe":{"httpGet":{"path":"/health/ready","port":9000},"periodSeconds":3}}]}}
        created=False
        try:
            cluster.kubectl("create","-f","-",input=json.dumps(pod).encode());created=True
            cluster.kubectl("wait","--for=condition=Ready","pod/"+name,"-n",ns,"--timeout=240s",timeout=270)
        finally:
            if created:
                current=cluster.kubectl("get","pod",name,"-n",ns,"-o","json",check=False)
                if current.returncode==0 and json.loads(current.stdout)["metadata"].get("labels",{}).get("platform.aat/bootstrap")==run:
                    cluster.kubectl("delete","pod",name,"-n",ns,"--wait=true","--timeout=30s",timeout=40)
    cluster.kubectl("exec","-i","-n",ns,leader["name"],"-c","postgres","--","psql","-U","postgres","-d",storage["database"],"-v","ON_ERROR_STOP=1","-f","-",input=schema_sql(storage).encode(),timeout=60)
