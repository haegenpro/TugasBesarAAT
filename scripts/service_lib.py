"""Common service deployment contract; owner-contributed entries use this schema."""
import copy
import re

from storage_lib import require

BUDGETS = {"aggregator": (256,512), "store": (512,1024), "broker": (512,1024),
           "consumer": (128,256), "api": (128,256), "mock": (64,128), "sensor": (128,256), "other": (128,256)}
DIGEST = re.compile(r"[a-z0-9./:_-]+@sha256:[0-9a-f]{64}")
NAME = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")


def validate(catalog):
    require(catalog["version"]==1,"unsupported service catalog version")
    services=catalog["services"]
    names={s["name"] for s in services}
    require(len(names)==len(services),"duplicate service name")
    known=names|set(catalog.get("external",[]))|{"aggregator"}
    for s in services:
        require(NAME.fullmatch(s["name"]) and len(s["name"])<=40,"invalid service name")
        require(s["role"] in BUDGETS and s["kind"] in ("Deployment","StatefulSet","Job"),"invalid workload role/kind")
        require(type(s["replicas"]) is int and 1<=s["replicas"]<=5,"invalid replica count")
        require(s["kind"]=="StatefulSet" or "storage" not in s,"local data requires StatefulSet placement")
        require(set(s.get("dependencies",[]))<=known,"unknown dependency")
        for key,value in s.get("environment",{}).items():
            require(not re.search(r"PASSWORD|TOKEN|API_KEY|SECRET|DATABASE_URL|INTERNAL_KEY",key),"credential belongs in the service Secret")
            require(isinstance(value,str) and "\n" not in value,"public environment values must be strings")
            require(not re.search(r"://[^/\s]*@",value),"URL credentials belong in a Secret")
        for port in s["ports"].values():
            require(type(port) is int and 1024<=port<=65535,"invalid service port")
    # Reject cycles in owner additions; external dependencies are resolved by
    # the deployment orchestrator, not silently assumed to be running.
    visiting,visited=set(),set()
    lookup={s["name"]:s for s in services}
    def visit(name):
        require(name not in visiting,"dependency cycle")
        if name in visited or name not in lookup:return
        visiting.add(name)
        for dependency in lookup[name].get("dependencies",[]):visit(dependency)
        visiting.remove(name);visited.add(name)
    for name in names:visit(name)
    return catalog


def render(inventory,catalog,images):
    validate(catalog)
    ns,cluster=inventory["namespace"],inventory["cluster_id"]
    objects=[];volumes=[];requests=0
    def add(api,kind,name,**fields):
        meta={"name":name,"labels":{"platform.aat/cluster":cluster}}
        if kind!="PersistentVolume":meta["namespace"]=ns
        item={"apiVersion":api,"kind":kind,"metadata":meta,**fields};objects.append(item);return item
    def service(name,selector,ports,headless=False):
        spec={"selector":selector,"ports":[{"name":n,"port":p,"targetPort":p} for n,p in ports.items()]}
        if headless:spec.update({"clusterIP":"None","publishNotReadyAddresses":True})
        add("v1","Service",name,spec=spec)
    for entry in catalog["services"]:
        s=copy.deepcopy(entry);name=s["name"]
        image=s.get("image") or images.get(s.get("image_key"))
        require(isinstance(image,str) and DIGEST.fullmatch(image),f"supply immutable image for {name}")
        selector={"app.kubernetes.io/name":name,"platform.aat/cluster":cluster}
        labels={**selector,"role":s["role"]}
        if s["ports"]:service(name,selector,s["ports"])
        if s["kind"]=="StatefulSet":service(name+"-members",selector,s["ports"],True)
        request,limit=BUDGETS[s["role"]];requests+=request*s["replicas"]
        env=[{"name":k,"value":v} for k,v in s.get("environment",{}).items()]
        env += [{"name":"POD_NAME","valueFrom":{"fieldRef":{"fieldPath":"metadata.name"}}},
                {"name":"INSTANCE_ID","valueFrom":{"fieldRef":{"fieldPath":"metadata.name"}}}]
        uid=s.get("storage",{}).get("uid",s.get("uid",10001))
        container={"name":name,"image":image,"env":env,"ports":[{"name":n,"containerPort":p} for n,p in s["ports"].items()],
            "resources":{"requests":{"memory":f"{request}Mi","cpu":"100m"},"limits":{"memory":f"{limit}Mi","cpu":"1"}},
            "securityContext":{"allowPrivilegeEscalation":False,"readOnlyRootFilesystem":s.get("read_only_root",True),"capabilities":{"drop":["ALL"]}},
            "volumeMounts":[{"name":"tmp","mountPath":"/tmp"}]}
        if s.get("secret"):container["envFrom"]=[{"secretRef":{"name":s["secret"]}}]
        if s.get("command"):container["command"]=s["command"]
        for target,field in (("ready","readinessProbe"),("live","livenessProbe")):
            if s.get(target):container[field]={"httpGet":s[target],"periodSeconds":5,"timeoutSeconds":2,"failureThreshold":3}
        if s.get("live"):container["startupProbe"]={"httpGet":s["live"],"periodSeconds":3,"failureThreshold":40}
        pod={"automountServiceAccountToken":False,"terminationGracePeriodSeconds":45,
             "securityContext":{"runAsNonRoot":True,"runAsUser":uid,"runAsGroup":uid,"fsGroup":uid,"seccompProfile":{"type":"RuntimeDefault"}},
             "containers":[container],"volumes":[{"name":"tmp","emptyDir":{"sizeLimit":"64Mi"}}]}
        if s["kind"]!="Job":pod["topologySpreadConstraints"]=[{"maxSkew":1,"topologyKey":"kubernetes.io/hostname","whenUnsatisfiable":"DoNotSchedule","labelSelector":{"matchLabels":selector}}]
        for index,mount in enumerate(s.get("config_mounts",[])):
            volume="config-"+str(index)
            pod["volumes"].append({"name":volume,"configMap":{"name":mount["name"]}})
            container["volumeMounts"].append({"name":volume,"mountPath":mount["path"],"readOnly":True})
        count=s["replicas"] if s["kind"]=="StatefulSet" else 1
        for ordinal in range(count):
            member=name+"-"+chr(97+ordinal) if s["kind"]=="StatefulSet" else name
            current=copy.deepcopy(pod)
            member_labels={**labels,"platform.aat/workload":member}
            match={**selector,"platform.aat/workload":member} if s["kind"]=="StatefulSet" else selector
            if s["kind"]=="StatefulSet":
                node=inventory["nodes"][ordinal%4]["name"]
                current["nodeSelector"]={"kubernetes.io/hostname":node}
                storage=s.get("storage")
                if storage:
                    pv=cluster+"-"+member
                    path=f"/var/lib/aat/local-pv/{cluster}-services/{pv}"
                    add("v1","PersistentVolume",pv,spec={"capacity":{"storage":storage["size"]},"accessModes":["ReadWriteOnce"],"storageClassName":"",
                        "persistentVolumeReclaimPolicy":"Retain","local":{"path":path},"claimRef":{"name":pv,"namespace":ns},
                        "nodeAffinity":{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":[node]}]}]}}})
                    add("v1","PersistentVolumeClaim",pv,spec={"accessModes":["ReadWriteOnce"],"storageClassName":"","volumeName":pv,"resources":{"requests":{"storage":storage["size"]}}})
                    current["volumes"].append({"name":"data","persistentVolumeClaim":{"claimName":pv}})
                    current["containers"][0]["volumeMounts"].append({"name":"data","mountPath":storage["path"]})
                    volumes.append({"node":node,"path":path,"uid":uid,"claim":pv})
            template={"metadata":{"labels":member_labels},"spec":current}
            if s.get("metrics_port"):
                template["metadata"]["annotations"]={"prometheus.io/scrape":"true","prometheus.io/port":str(s["metrics_port"]),"prometheus.io/path":"/metrics"}
            if s["kind"]=="Job":
                current["restartPolicy"]="Never"
                add("batch/v1","Job",member,spec={"backoffLimit":6,"activeDeadlineSeconds":180,"template":template})
            else:
                spec={"replicas":1 if s["kind"]=="StatefulSet" else s["replicas"],"selector":{"matchLabels":match},"template":template}
                if s["kind"]=="StatefulSet":spec["serviceName"]=name+"-members"
                else:spec["strategy"]={"type":"RollingUpdate","rollingUpdate":{"maxUnavailable":0,"maxSurge":1}}
                add("apps/v1",s["kind"],member,spec=spec)
    # Broker route identities match the stable per-node member workloads above.
    routes=",".join(f'"nats://broker-{letter}-0.broker-members.{ns}.svc.cluster.local:6222"' for letter in "abc")
    if any(s["name"]=="broker" for s in catalog["services"]):
        config=f'''server_name: $POD_NAME
port: 4222
http_port: 8222
cluster {{name: "{cluster}-broker", port: 6222, routes: [{routes}]}}
jetstream {{store_dir: "/data", max_memory_store: 64MB, max_file_store: 1GB, sync_interval: always}}
'''
        add("v1","ConfigMap","broker-config",data={"nats.conf":config})
    return {"apiVersion":"v1","kind":"List","items":objects},{"local_volumes":volumes,"memory_requests_mib":requests,
        "external_dependencies":catalog.get("external",[]),"note":"Rendered integration resources; external databases, source HA and acceptance remain owner inputs."}
