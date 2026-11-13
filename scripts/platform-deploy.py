#!/usr/bin/env python3
"""Plan prepared artifacts offline, or explicitly apply them later in dependency order."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from bundle_lib import inspect,memory_mib
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import require,write_json

ROOT=Path(__file__).resolve().parent.parent


def prepare_volumes(cluster,requirements):
    spec=importlib.util.spec_from_file_location("platform_storage",ROOT/"scripts/platform-storage.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    formation=cluster.cfg["cluster_id"]+"-services"
    paths={n["name"]:[]for n in cluster.cfg["nodes"]}
    for source in requirements:
        for volume in json.loads(source.read_text())["local_volumes"]:
            path=Path(volume["path"])
            require(path.parent==Path("/var/lib/aat/local-pv")/formation and path.name.startswith(cluster.cfg["cluster_id"]+"-"),"unowned volume path")
            require(volume["node"]in paths and type(volume["uid"])is int and volume["uid"]>=1,"invalid volume owner/node")
            paths[volume["node"]].append({"name":path.name,"uid":volume["uid"]})
    for node in cluster.cfg["nodes"]:
        if not paths[node["name"]]:continue
        cluster.ssh(node,["findmnt","--mountpoint","/var/lib/aat"])
        cluster.ssh(node,["sudo","-n","python3","-c",module.PREPARE_GUEST],input=json.dumps({"cluster_id":cluster.cfg["cluster_id"],"formation":formation,"paths":paths[node["name"]]}).encode())


def apply(cluster,report,snapshots):
    require(report["ready_for_apply"],"resolve all bundle blockers before applying")
    ns=cluster.cfg["namespace"]
    nodes=json.loads(cluster.kubectl("get","nodes","-o","json").stdout)["items"]
    selected=[n for n in nodes if n["metadata"]["name"]in {v["name"]for v in cluster.cfg["nodes"]}]
    require(len(selected)==4 and all(any(c["type"]=="Ready"and c["status"]=="True"for c in n["status"]["conditions"])for n in selected),"all four existing nodes must be Ready; this command never boots VMs")
    available=sum(memory_mib(n["status"]["allocatable"]["memory"])for n in selected)
    pods=json.loads(cluster.kubectl("get","pods","--all-namespaces","-o","json").stdout)["items"]
    existing=0
    for pod in pods:
        if pod.get("status",{}).get("phase")in("Succeeded","Failed"):continue
        if pod["spec"].get("nodeName")not in {n["metadata"]["name"]for n in selected}:continue
        meta=pod["metadata"]
        if meta["namespace"]==ns and (meta.get("labels",{}).get("app.kubernetes.io/name")in report["apps"] or meta.get("labels",{}).get("platform.aat/formation")==cluster.cfg["cluster_id"]+"-storage"):continue
        # Existing system pods can omit memory requests. Their actual use still
        # requires observation and is covered by the offline node reserve too.
        for c in pod["spec"].get("containers",[]):existing+=memory_mib(c.get("resources",{}).get("requests",{}).get("memory","0"))
    require(report["memory_requests_mib"]+existing<=available,"live allocatable memory is insufficient")
    for secret in report["required_existing_secrets"]:cluster.kubectl("get","secret",secret,"-n",ns,"-o","name")
    for stage in report["stages"]:
        # Use the exact bytes reviewed by the planner, avoiding a file change
        # between validation and apply. Do not print manifest bodies or secrets.
        cluster.kubectl("apply","-f","-",input=snapshots[stage["id"]],timeout=60)
        for wait in stage["wait"]:
            resource=wait["kind"].lower()+"/"+wait["name"]
            if wait["kind"]=="Job":cluster.kubectl("wait","--for=condition=Complete",resource,"-n",ns,"--timeout=300s",timeout=330)
            else:cluster.kubectl("rollout","status",resource,"-n",ns,"--timeout=300s",timeout=330)
        print("Applied stage: "+stage["id"],flush=True)
    # Some service readiness dependencies cross stage boundaries (Client API
    # waits for Aggregator). Check every workload after all objects are present.
    for workload in report["workloads"]:
        resource=workload["kind"].lower()+"/"+workload["name"]
        if workload["kind"]=="Job":cluster.kubectl("wait","--for=condition=Complete",resource,"-n",ns,"--timeout=300s",timeout=330)
        else:cluster.kubectl("rollout","status",resource,"-n",ns,"--timeout=300s",timeout=330)
    print("Prepared bundle applied and workload readiness observed; HA/load acceptance remains separate.")


def main():
    os.umask(0o077)
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("action",choices=["plan","apply","prepare-volumes"])
    p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.example.json")
    p.add_argument("--bundle",type=Path);p.add_argument("--output",type=Path);p.add_argument("--requirements",type=Path,action="append",default=[])
    a=p.parse_args();inventory=load(a.inventory)
    if a.action=="prepare-volumes":
        require(a.requirements,"supply service/monitoring requirements files")
        cluster=Cluster(inventory)
        with cluster.locked():prepare_volumes(cluster,a.requirements)
        return
    require(a.bundle,"supply prepared bundle descriptor")
    report,snapshots=inspect(a.bundle,inventory)
    if a.output:write_json(a.output,report)
    if a.action=="plan":print(json.dumps(report,indent=2));return
    cluster=Cluster(inventory)
    with cluster.locked():apply(cluster,report,snapshots)

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:print(f"Deployment stopped: {exc}",file=sys.stderr);sys.exit(1)
