"""Input-driven execution and analysis of source checks and combined game days."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from contextlib import nullcontext

from generator_evidence_lib import health
from maintenance_lib import hash_json
from platform_lib.cluster import Cluster
from platform_lib.inventory import fingerprint,load
from release_config_lib import read_token
from service_lib import DIGEST
from source_validation_lib import Observer,http_json,load_inputs,patch_replicas,run as source_run
from storage_lib import require,write_json
from load_auth_lib import credentials,token_inventory

ROOT=Path(__file__).resolve().parent.parent
TESTS={"satellite-burst","baseline-1","baseline-3","scale","rollout","node-pause","combined","reconcile"}


def game_module():
    spec=importlib.util.spec_from_file_location("platform_game_day",ROOT/"scripts/platform-game-day.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def plan(config_path,output):
    config_path=Path(config_path).resolve();base=config_path.parent;config=json.loads(config_path.read_text())
    require(config.get("version")==1,"acceptance configuration version must be 1")
    inventory_path=(base/config["inventory"]).resolve();inventory=load(inventory_path)
    source_path=(base/config["source_validation"]).resolve();source=load_inputs(inventory,source_path)
    tests=config.get("tests",["satellite-burst","baseline-1","baseline-3","combined","reconcile"])
    require(isinstance(tests,list)and tests and len(set(tests))==len(tests)and set(tests)<=TESTS,"select distinct supported acceptance stages")
    require(("baseline-1"in tests)==("baseline-3"in tests),"throughput comparison needs both one- and three-replica baselines")
    if "baseline-1"in tests:require(tests.index("baseline-1")<tests.index("baseline-3"),"baseline one must precede baseline three")
    workload=config.get("load",{})
    if any(t not in("satellite-burst","reconcile")for t in tests):
        require(type(workload.get("vus",10000))is int and workload.get("vus",10000)>=10000 and type(workload.get("seconds",150))is int and workload.get("seconds",150)>=120,"M2 needs at least 10000 VUs for at least 120 seconds")
        require(isinstance(workload.get("base_url"),str)and workload["base_url"].startswith(("http://","https://")),"shared Client API URL required")
        require(type(workload.get("generator_network_mbps"))in(int,float)and workload["generator_network_mbps"]>0,"supply the load-generator link budget for automatic headroom analysis")
        if workload.get("token_file"):
            tokens=base/workload["token_file"];require(tokens.is_file()and tokens.stat().st_mode&0o077==0,"private authenticated load token inventory required")
            values=json.loads(tokens.read_text());require(isinstance(values,list)and all(any(r.get("role")==role and r.get("access_token")for r in values)for role in("field","media")),"Field and Media load tokens are required")
        else:credentials(base/workload["client_credentials_file"])
    if any(t in("rollout","combined")for t in tests):require(isinstance(workload.get("rollout_image"),str)and DIGEST.fullmatch(workload["rollout_image"]),"immutable rollout image input required")
    if any(t in("node-pause","combined")for t in tests):require(workload.get("pause_node")in {n["name"]for n in inventory["nodes"]},"select an owned VM for the pause scenario")
    return {"version":1,"inventory_sha256":fingerprint(inventory),"reference_sha256":source[-1],"tests":tests,"output":str(Path(output).resolve()),"executed":False,"mutations":"baseline replica preparation; selected scale/rollout/owned-VM pause and UID-guarded ingestion disconnect only","assumption":"Use declared VM allocation; no hardware questionnaire. Existing live prerequisites are checked when used."},(config,base,inventory,inventory_path,source_path,source)


def load_measurements(directory,expected_scenario=None):
    directory=Path(directory);report=json.loads((directory/"result.json").read_text());work=json.loads((directory/"load.json").read_text())
    require(report.get("status")=="needs-owner-reconciliation"and report.get("load_exit_code")==0 and not report.get("cleanup_required"),"load run or VM restoration did not complete")
    require(not report.get("worktree_dirty"),"load evidence must record the committed implementation")
    if expected_scenario:require(report["scenario"]==expected_scenario,"unexpected game-day scenario")
    configuration=work["configuration"]
    require(configuration["run_id"]==report["run_id"]and configuration["vus"]>=10000 and configuration["duration_seconds"]>=120,"load identity/duration/VUs are invalid")
    metrics=work["metrics"];rate=metrics["m2_unexpected_failure_rate"]["values"]["rate"]
    require(type(rate)in(int,float)and 0<=rate<.01,"unexpected HTTP errors exceed the M2 target")
    for role in("field","media"):require(metrics["m2_successful_reads{role:"+role+"}"]["values"]["count"]>0,"no successful "+role+" requests")
    samples=json.loads((directory/"generator-samples.json").read_text());measured=health(samples,report.get("generator_health",{}).get("declared_network_capacity_mbps"))
    require(measured["passed"]and not report.get("generator_sampling_errors"),"load generator was saturated or its measurements are incomplete")
    snapshots=[]
    for path in sorted(directory.glob("sample-*-deployments.json")):
        snapshots += [o for o in json.loads(path.read_text())["items"]if o["metadata"]["name"]==report["original_deployment"]["name"]]
    require(snapshots,"no deployment readiness samples taken while load was active")
    events=[e["action"]for e in report["events"]];scenario=report["scenario"]
    expected_events={"scale":["scale","scale"],"rollout":["rollout"],"node-pause":["pause","resume"],"combined":["scale","rollout","pause","resume","scale"]}.get(scenario,[])
    require([e for e in events if e in("scale","rollout","pause","resume")]==expected_events,"scheduled fault/rollout/scale actions are incomplete")
    if scenario in("scale","combined"):
        for count in(5,2):require(any(o.get("spec_replicas")==count and o["status"].get("readyReplicas",0)>=count and o["status"].get("observedGeneration",0)>=o["metadata"].get("generation",1)for o in snapshots),"replica target never became Ready during load: "+str(count))
    if scenario in("rollout","combined"):
        image=report.get("requested_rollout_image")
        require(image and image not in report["original_deployment"]["images"],"rollout image did not change")
        ready=False
        for path in directory.glob("sample-*-pods.json"):
            for pod in json.loads(path.read_text())["items"]:
                if pod["metadata"].get("labels",{}).get("app.kubernetes.io/name")=="aggregator"and image in pod["images"]and any(c["type"]=="Ready"and c["status"]=="True"for c in pod.get("status",{}).get("conditions",[])):ready=True
        require(ready,"new rollout image never became Ready while load was active")
    return {"passed":True,"run_id":report["run_id"],"scenario":scenario,"unexpected_error_rate":rate,"successful_rps":metrics["m2_successful_reads"]["values"]["count"]/configuration["duration_seconds"],"generator_health":measured},report,work


def compare(one,three):
    a,ra,wa=load_measurements(one,"baseline");b,rb,wb=load_measurements(three,"baseline")
    for key in("dataset_sha256","script_sha256","git_revision"):require(ra[key]==rb[key],"baseline comparison differs in "+key)
    require(ra["original_deployment"]["images"]==rb["original_deployment"]["images"],"Aggregator image changed between throughput baselines")
    for key in("vus","duration_seconds","field_vus","think_time_seconds","workload"):require(wa["configuration"][key]==wb["configuration"][key],"baseline workloads differ")
    require(ra["original_deployment"]["replicas"]==1 and rb["original_deployment"]["replicas"]==3,"baselines do not use one and three replicas")
    markers=[json.loads((Path(folder)/"dataset-before.json").read_text())for folder in(one,three)]
    require(markers[0]==markers[1],"canonical state changed between throughput baselines")
    # Hold every upstream image and desired replica allocation fixed too.
    environments=[]
    for folder in(one,three):
        snapshot=json.loads((Path(folder)/"before-deployments.json").read_text())["items"]
        snapshot+=json.loads((Path(folder)/"before-statefulsets.json").read_text())["items"]
        environments.append(sorted((o["metadata"]["name"],o["spec_sha256"]if o["metadata"]["name"]!=ra["original_deployment"]["name"]else o["template_sha256"])for o in snapshot))
    require(environments[0]==environments[1],"upstream replica counts/images differ between baselines")
    require(a["successful_rps"]>0,"single replica throughput was zero")
    ratio=b["successful_rps"]/a["successful_rps"]
    return {"passed":ratio>=2,"successful_rps_one":a["successful_rps"],"successful_rps_three":b["successful_rps"],"ratio":ratio}


def run(config_path,output):
    prepared,values=plan(config_path,output);config,base,inventory,inventory_path,source_path,source=values
    output=Path(output);require(not output.exists(),"acceptance output exists; preserve original evidence")
    output.mkdir(parents=True,mode=0o700);write_json(output/"plan.json",prepared)
    summary={"version":1,"passed":False,"reference_sha256":prepared["reference_sha256"],"stages":[],"scope":"selected measured source, HTTP load and combined rollout/VM-pause scenarios; other physical acceptance rows remain separate"}
    write_json(output/"result.json",summary);cluster=Cluster(inventory);observer=Observer(cluster,source[2],source[3]);module=game_module()
    workload=config.get("load",{})
    def dataset():
        result=observer.query("storage","SELECT jsonb_build_object('generation',(SELECT generation FROM canonical_visibility WHERE singleton),'pending',(SELECT count(*) FROM canonical_attempts WHERE state='pending'))")["data"]
        require(result["pending"]==0,"resolve pending canonical attempts before the throughput baseline")
        return result
    try:
        for test in prepared["tests"]:
            folder=output/test
            if test in("satellite-burst","reconcile"):
                if test=="satellite-burst":
                    with cluster.locked():result=source_run(cluster,source_path,folder,test)
                else:result=source_run(cluster,source_path,folder)
            else:
                from types import SimpleNamespace
                baseline=1 if test=="baseline-1"else 3;scenario="baseline"if test.startswith("baseline-")else test
                name=inventory["cluster_id"]+"-aggregator";ns=inventory["namespace"]
                with cluster.locked():
                    current=json.loads(cluster.kubectl("get","deployment",name,"-n",ns,"-o","json").stdout)
                    require(current["spec"]["template"]["metadata"]["labels"].get("platform.aat/cluster")==inventory["cluster_id"],"unowned baseline deployment")
                    if current["spec"]["replicas"]!=baseline:patch_replicas(cluster,name,current["metadata"]["uid"],current["spec"]["replicas"],baseline)
                    cluster.kubectl("rollout","status","deployment/"+name,"-n",ns,"--timeout=180s",timeout=190)
                before=dataset()if scenario=="baseline"else None
                token_context=nullcontext((base/workload["token_file"]).resolve())if workload.get("token_file")else token_inventory(workload["base_url"],base/workload["client_credentials_file"],output/(test+"-tokens.json"),workload.get("seconds",150))
                with token_context as token_path:
                    args=SimpleNamespace(scenario=scenario,vus=workload.get("vus",10000),seconds=workload.get("seconds",150),token_file=token_path,dataset_sha256=prepared["reference_sha256"],image=workload.get("rollout_image"),node=workload.get("pause_node"),baseline_replicas=baseline,base_url=workload["base_url"],output=folder,generator_network_mbps=workload["generator_network_mbps"])
                    module.run(args,inventory)
                if before is not None:
                    after=dataset();write_json(folder/"dataset-before.json",before);write_json(folder/"dataset-after.json",after)
                    require(before==after,"canonical writes changed the dataset during the throughput baseline")
                result=load_measurements(folder,scenario)[0]
            summary["stages"].append({"test":test,"passed":result["passed"],"result_sha256":hashlib.sha256((folder/"result.json").read_bytes()).hexdigest()});write_json(output/"result.json",summary)
            require(result["passed"],"acceptance stage failed: "+test)
        if "baseline-1"in prepared["tests"]:
            summary["throughput_comparison"]=compare(output/"baseline-1",output/"baseline-3")
            require(summary["throughput_comparison"]["passed"],"three-replica successful throughput is below two times one replica")
        summary["passed"]=True;write_json(output/"result.json",summary);return summary
    except BaseException:
        summary["passed"]=False;summary["execution_incomplete_or_failed"]=True;write_json(output/"result.json",summary);raise
