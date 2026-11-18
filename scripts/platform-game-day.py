#!/usr/bin/env python3
"""Plan, or explicitly execute later, owned replica/fault experiments during M2 load."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import time
import uuid

from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import require, write_json
from service_lib import DIGEST
from generator_evidence_lib import Sampler,health
from maintenance_lib import hash_json
from source_validation_lib import patch_replicas

ROOT=Path(__file__).resolve().parent.parent


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def schedule(scenario,seconds):
    require(seconds>=120,"M2 duration must be >=120 seconds")
    if scenario=="scale":return [(seconds*.30,"scale",5),(seconds*.65,"scale",2)]
    if scenario=="rollout":return [(seconds*.35,"rollout",None)]
    if scenario=="node-pause":return [(seconds*.30,"pause",None),(seconds*.55,"resume",None)]
    if scenario=="combined":return [(seconds*.20,"scale",5),(seconds*.35,"rollout",None),(seconds*.45,"pause",None),(seconds*.60,"resume",None),(seconds*.75,"scale",2)]
    return []


def run(args,inventory):
    require(args.vus>=10000,"M2 requires >=10000 VUs")
    require(args.token_file and args.token_file.is_file(),"private --token-file is required")
    require(args.dataset_sha256 and len(args.dataset_sha256)==64 and all(c in "0123456789abcdef" for c in args.dataset_sha256),"record the actual dataset SHA256")
    if args.scenario in("rollout","combined"):require(args.image and DIGEST.fullmatch(args.image),"rollout requires immutable replacement image")
    if args.scenario in("node-pause","combined"):require(args.node in {n["name"] for n in inventory["nodes"]},"select an owned inventory node")
    cluster=Cluster(inventory);run_id="m2-"+uuid.uuid4().hex
    output=getattr(args,"output",None)or ROOT/"evidence/platform-game-day"/run_id
    require(not output.exists(),"game-day output exists; preserve original evidence");output.mkdir(parents=True,mode=0o700)
    report={"version":1,"run_id":run_id,"scenario":args.scenario,"started_at":datetime.now(timezone.utc).isoformat(),"passed":False,"status":"running","dataset_sha256":args.dataset_sha256,"script_sha256":digest(ROOT/"scripts/load-test-m2.js"),"events":[],"scope":"read-load and infrastructure actions; owner delivery reconciliation required"}
    if args.scenario in("rollout","combined"):report["requested_rollout_image"]=args.image
    workload=None;paused=False;original=None;generator=Sampler(output/"generator-samples.json");expected_replicas=None;expected_image=None
    def save():write_json(output/"result.json",report)
    def event(action,**fields):report["events"].append({"at":datetime.now(timezone.utc).isoformat(),"action":action,**fields});save()
    def snapshot(label):
        for resource in ("pods","deployments","statefulsets","nodes"):
            query=["get",resource,"-o","json","--request-timeout=5s"]
            if resource!="nodes":query += ["-n",inventory["namespace"]]
            value=json.loads(cluster.kubectl(*query).stdout)
            # These objects contain Secret references, not Secret values. Never
            # export literal container environments or arbitrary pod annotations.
            if resource in ("pods","deployments","statefulsets"):
                value={"items":[{"metadata":{"name":o["metadata"]["name"],"uid":o["metadata"].get("uid"),"generation":o["metadata"].get("generation"),"labels":o["metadata"].get("labels",{})},"spec_replicas":o["spec"].get("replicas"),"spec_sha256":hash_json(o["spec"]),"template_sha256":hash_json(o["spec"].get("template",{})),"status":o.get("status",{}),"images":[c["image"] for c in (o["spec"].get("template",{}).get("spec",o["spec"])).get("containers",[])]}for o in value["items"]]}
            write_json(output/(label+"-"+resource+".json"),value)
        top=cluster.kubectl("top","pods","-n",inventory["namespace"],"--containers","--request-timeout=5s",check=False)
        (output/(label+"-top.txt")).write_bytes(top.stdout)
    def mutate(action,value):
        nonlocal expected_replicas,expected_image
        require(workload.poll() is None,"load stopped before infrastructure action")
        current=json.loads(cluster.kubectl("get","deployment",deployment,"-n",inventory["namespace"],"-o","json").stdout)
        require(current["metadata"]["uid"]==original["metadata"]["uid"]and current["spec"]["replicas"]==expected_replicas and current["spec"]["template"]["spec"]["containers"][0]["image"]==expected_image,"deployment identity/count/image changed outside the scenario")
        if action=="scale":
            patch_replicas(cluster,deployment,current["metadata"]["uid"],expected_replicas,value);expected_replicas=value
        else:
            patch=[{"op":"test","path":"/metadata/uid","value":current["metadata"]["uid"]},{"op":"test","path":"/spec/template/spec/containers/0/image","value":expected_image},{"op":"replace","path":"/spec/template/spec/containers/0/image","value":value}]
            cluster.kubectl("patch","deployment",deployment,"-n",inventory["namespace"],"--type=json","-p",json.dumps(patch));expected_image=value
    try:
        with cluster.locked():
            deployment=inventory["cluster_id"]+"-aggregator"
            original=json.loads(cluster.kubectl("get","deployment",deployment,"-n",inventory["namespace"],"-o","json").stdout)
            require(original["spec"]["replicas"]==args.baseline_replicas,"deployment must already match requested baseline replicas")
            require(original["spec"]["template"]["metadata"]["labels"].get("platform.aat/cluster")==inventory["cluster_id"],"unowned deployment")
            require(original["spec"]["template"]["spec"]["containers"][0]["name"]=="aggregator","unexpected Aggregator container layout")
            expected_replicas=original["spec"]["replicas"];expected_image=original["spec"]["template"]["spec"]["containers"][0]["image"]
            if args.scenario in("rollout","combined"):require(expected_image!=args.image,"rollout must select a different immutable image")
            if args.scenario in ("scale","rollout","node-pause","combined"):require(args.baseline_replicas==3,"fault/scale scenario begins at three replicas")
            snapshot("before")
            report["git_revision"]=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
            entries=subprocess.check_output(["git","status","--porcelain","-z"],cwd=ROOT).decode().split("\0")
            excluded={"docs/IF4031 - Tugas Besar - M1.md","docs/IF4031 - Tugas Besar - M2.md"}
            report["worktree_dirty"]=any(row and row[3:]not in excluded for row in entries)
            env={**os.environ,"BASE_URL":args.base_url,"VUS":str(args.vus),"DURATION_SECONDS":str(args.seconds),"RUN_ID":run_id,"AUTH_TOKEN_FILE":str(args.token_file.resolve()),"SUMMARY_PATH":str(output/"load.json")}
            with (output/"k6.log").open("wb")as log:
                workload=subprocess.Popen(["k6","run","--quiet",str(ROOT/"scripts/load-test-m2.js")],env=env,stdout=log,stderr=subprocess.STDOUT)
                generator.start();started=time.monotonic();actions=schedule(args.scenario,args.seconds);sample=0
                while workload.poll() is None:
                    elapsed=time.monotonic()-started
                    if elapsed>args.seconds+90:raise RuntimeError("load exceeded bounded runtime")
                    while actions and elapsed>=actions[0][0]:
                        _,action,value=actions.pop(0)
                        if action=="scale":mutate("scale",value)
                        elif action=="rollout":mutate("rollout",args.image)
                        elif action=="pause":
                            target=next(n for n in inventory["nodes"] if n["name"]==args.node)
                            require(cluster.vm_state(target)=="running","node must be running before pause")
                            cluster.vm_action(target,"pause");paused=True
                        elif action=="resume":cluster.vm_action(next(n for n in inventory["nodes"] if n["name"]==args.node),"resume");paused=False
                        event(action,value=value)
                    if elapsed>=sample*15:snapshot("sample-"+str(sample));sample+=1
                    time.sleep(.5)
                require(not actions,"load ended before scheduled actions")
                report["load_exit_code"]=workload.returncode
                snapshot("after")
                require(workload.returncode==0,"k6 workload or thresholds failed")
                cluster.kubectl("rollout","status","deployment/"+deployment,"-n",inventory["namespace"],"--timeout=180s",timeout=190)
                load_report=json.loads((output/"load.json").read_text());report["load_configuration"]=load_report["configuration"]
                report["status"]="needs-owner-reconciliation"
                event("load-finished")
    finally:
        generator.stop()
        if workload is not None and workload.poll() is None:
            workload.terminate()
            try:workload.wait(timeout=10)
            except subprocess.TimeoutExpired:workload.kill();workload.wait()
        if paused:
            try:cluster.vm_action(next(n for n in inventory["nodes"] if n["name"]==args.node),"resume");event("cleanup-resume")
            except Exception:report["cleanup_required"]="resume owned VM "+args.node
        # Record original configuration; do not roll back an explicitly requested
        # rollout or scale behind the operator's back after the load window.
        if original:report["original_deployment"]={"name":original["metadata"]["name"],"replicas":original["spec"]["replicas"],"images":[c["image"]for c in original["spec"]["template"]["spec"]["containers"]]}
        if report["status"]=="running":report["status"]="failed-or-interrupted"
        report["finished_at"]=datetime.now(timezone.utc).isoformat();save()
        report["generator_health"]=health(generator.samples,getattr(args,"generator_network_mbps",None))
        report["generator_sampling_errors"]=generator.errors
        if generator.errors:report["generator_health"]["passed"]=False
        save()
    print("Evidence captured: "+str(output)+"; acceptance remains pending owner reconciliation.")


def main():
    os.umask(0o077)
    def interrupted(signum,frame):raise KeyboardInterrupt("game day interrupted")
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGHUP,interrupted)
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["plan","run"])
    p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.example.json")
    p.add_argument("--scenario",choices=["baseline","scale","rollout","node-pause","combined"],default="scale")
    p.add_argument("--vus",type=int,default=10000);p.add_argument("--seconds",type=int,default=150)
    p.add_argument("--baseline-replicas",type=int,choices=[1,3],default=3)
    p.add_argument("--base-url");p.add_argument("--token-file",type=Path);p.add_argument("--dataset-sha256");p.add_argument("--image");p.add_argument("--node")
    p.add_argument("--output",type=Path);p.add_argument("--generator-network-mbps",type=float,help="measured link capacity used only for later generator-headroom validation")
    a=p.parse_args();inventory=load(a.inventory);actions=schedule(a.scenario,a.seconds)
    if a.action=="plan":print(json.dumps({"scenario":a.scenario,"vus":a.vus,"seconds":a.seconds,"actions":actions,"executed":False,"requires":["capacity","healthy deployment","private valid tokens","dataset hash","owner delivery/uniqueness reconciliation","load-generator resource evidence"]},indent=2))
    else:
        require(a.base_url and a.base_url.startswith(("http://","https://")),"provide shared --base-url")
        run(a,inventory)

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,RuntimeError,subprocess.SubprocessError)as exc:print(f"Game day failed: {exc}",file=sys.stderr);sys.exit(1)
