"""Review prepared deployment artifacts and estimate memory without cluster calls."""
import hashlib
import json
from pathlib import Path
import re
from service_lib import BUDGETS,DIGEST
from storage_lib import require


def memory_mib(value):
    require(isinstance(value,str),"memory quantities must be strings")
    match=re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(Ki|Mi|Gi|Ti|k|M|G)?",value)
    require(match,"unsupported memory quantity")
    return float(match[1])*{None:1/(1<<20),"Ki":1/1024,"Mi":1,"Gi":1024,"Ti":1<<20,"k":1000/(1<<20),"M":1e6/(1<<20),"G":1e9/(1<<20)}[match[2]]


def pod_memory(pod):
    app=sum(memory_mib(c["resources"]["requests"]["memory"])for c in pod.get("containers",[]))
    # Sidecar-style init containers are not used by the platform manifests.
    # Refuse rather than underestimate an owner addition with different rules.
    require(all(c.get("restartPolicy")!="Always" for c in pod.get("initContainers",[])),"account for restartable init sidecars before deployment")
    init=max([memory_mib(c["resources"]["requests"]["memory"])for c in pod.get("initContainers",[])]+[0])
    return max(app,init)+memory_mib(pod.get("overhead",{}).get("memory","0"))


def inspect(bundle_path,inventory):
    path=Path(bundle_path).resolve();bundle=json.loads(path.read_text())
    require(bundle["version"]==1,"unsupported bundle version")
    stages=bundle["stages"];lookup={s["id"]:s for s in stages}
    require(stages and len(lookup)==len(stages),"empty bundle or duplicate stage")
    ordered=[];visiting=set();done=set()
    def visit(name):
        require(name in lookup and name not in visiting,"unknown/cyclic stage dependency")
        if name in done:return
        visiting.add(name)
        for dependency in lookup[name].get("requires",[]):visit(dependency)
        visiting.remove(name);done.add(name);ordered.append(lookup[name])
    for s in stages:visit(s["id"])
    require(type(bundle.get("node_reserve_mib"))is int and bundle["node_reserve_mib"]>=256,"declare >=256 MiB per node for guest/Kubernetes overhead")
    blocked=[];objects=[];seen=set();snapshots={};planned=[]
    total=0;fixed={n["name"]:0 for n in inventory["nodes"]};external=set();supplied=set();apps=set()
    for stage in ordered:
        require(isinstance(stage.get("manifest"),str),"stage manifest path required")
        artifact=(path.parent/stage["manifest"]).resolve()
        summary={"id":stage["id"],"manifest":str(artifact),"requires":stage.get("requires",[]),"wait":stage.get("wait",[])};planned.append(summary)
        for wait in summary["wait"]:
            require(set(wait)=={"kind","name"} and wait["kind"]in("Deployment","StatefulSet","DaemonSet","Job"),"wait must select an explicit workload kind/name")
            require(re.fullmatch(r"[a-z0-9][-a-z0-9.]*",wait["name"]),"invalid wait name")
        if not artifact.is_file():blocked.append("missing artifact for "+stage["id"]);continue
        raw=artifact.read_bytes();snapshots[stage["id"]]=raw;summary["sha256"]=hashlib.sha256(raw).hexdigest()
        resource=json.loads(raw);entries=resource["items"]if resource.get("kind")=="List"else[resource]
        for obj in entries:
            kind=obj["kind"];meta=obj["metadata"];name=meta["name"]
            require(kind not in ("ClusterRole","ClusterRoleBinding","CustomResourceDefinition","Node"),"bundle may not change cluster privileges or nodes")
            require(kind in ("Namespace","PersistentVolume")or meta.get("namespace")==inventory["namespace"],"artifact outside assignment namespace")
            if kind=="Namespace":require(name==inventory["namespace"],"unexpected namespace")
            identity=(obj["apiVersion"],kind,meta.get("namespace"),name)
            require(identity not in seen,"duplicate object across stages: "+kind+"/"+name);seen.add(identity)
            if kind=="Secret":supplied.add(name)
            if kind not in ("Deployment","StatefulSet","DaemonSet","Job"):continue
            template=obj["spec"]["template"];pod=template["spec"];labels=template["metadata"]["labels"];role=labels.get("role")
            require(role in BUDGETS,"missing/invalid assignment role on "+name)
            require(labels.get("platform.aat/cluster")==inventory["cluster_id"] or labels.get("platform.aat/formation")==inventory["cluster_id"]+"-storage","workload belongs to another cluster")
            if labels.get("app.kubernetes.io/name"):apps.add(labels["app.kubernetes.io/name"])
            for container in pod.get("containers",[])+pod.get("initContainers",[]):
                require(DIGEST.fullmatch(container["image"]),"mutable workload image on "+name)
                request,limit=BUDGETS[role]
                require(memory_mib(container["resources"]["requests"]["memory"])==request and memory_mib(container["resources"]["limits"]["memory"])==limit,"memory budget mismatch on "+name)
                for env in container.get("env",[]):
                    secret=env.get("valueFrom",{}).get("secretKeyRef")
                    if secret:external.add(secret["name"])
                for source in container.get("envFrom",[]):
                    if "secretRef"in source:external.add(source["secretRef"]["name"])
            for volume in pod.get("volumes",[]):
                if "secret"in volume:external.add(volume["secret"]["secretName"])
            count=len(inventory["nodes"])if kind=="DaemonSet"else obj["spec"].get("replicas",1)
            amount=pod_memory(pod);total+=amount*count
            node=pod.get("nodeSelector",{}).get("kubernetes.io/hostname")
            if node:require(node in fixed,"fixed workload on unknown node");fixed[node]+=amount*count
            if kind=="DaemonSet":
                for node_name in fixed:fixed[node_name]+=amount
            objects.append({"stage":stage["id"],"kind":kind,"name":name,"replicas":count,"memory_requests_mib":amount*count})
    available=sum(n["memory_mib"]-bundle["node_reserve_mib"]for n in inventory["nodes"])
    if total>available:blocked.append("declared workload requests exceed VM memory after guest/Kubernetes reserve")
    for node in inventory["nodes"]:
        if fixed[node["name"]]>node["memory_mib"]-bundle["node_reserve_mib"]:blocked.append("fixed workloads exceed memory on "+node["name"])
    blocked+=bundle.get("unresolved",[])
    return {"version":1,"ready_for_apply":not blocked,"blocked":blocked,"stages":planned,"workloads":objects,"memory_requests_mib":total,"estimated_available_mib":available,"fixed_requests_mib":fixed,"required_existing_secrets":sorted(external-supplied),"apps":sorted(apps),"note":"Offline lower-bound capacity estimate. Live resources, CPU/disk, scheduling, rollout surge and failover headroom still need verification."},snapshots
