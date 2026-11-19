"""Fixed staged commands for first deployment; no arbitrary shell hooks."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
try:
    import fcntl
except ImportError:
    fcntl=None

from maintenance_lib import hash_json
from replacement_lib import write_report
from storage_lib import SERVICE_FORMATION,require,write_json

ROOT=Path(__file__).resolve().parent.parent
PHASES=("images","inputs","cluster","bootstrap","maintenance","activate","access","validate")


def plan(path,phase="all"):
    path=Path(path).resolve();base=path.parent;config=json.loads(path.read_text())
    require(config.get("version")==1,"workflow version must be 1")
    require(phase in(*PHASES,"all"),"unknown workflow phase")
    def source(key):
        require(isinstance(config.get(key),str)and config[key],"workflow path input required: "+key)
        return str((base/config[key]).resolve())
    root=Path(source("output"));inventory=source("inventory")
    require(not root.is_relative_to(ROOT)or root.is_relative_to(ROOT/"evidence"),"private workflow output must stay outside the repository or under ignored evidence/")
    def command(script,*args):return [sys.executable,str(ROOT/"scripts"/script),*map(str,args)]
    inv=["--inventory",inventory];config_path=root/"inputs/release.json";bootstrap=root/"bootstrap";ready=root/"maintenance/release-ready.json";full=root/"full"
    commands={
        "images":[command("platform-images.py","publish","--config",source("image_config"),"--output",root/"images")],
        "inputs":[command("platform-inputs.py",*inv,"--config",source("private_release_config"),"--inputs",source("external_inputs"),"--output",root/"inputs")],
        "cluster":[command("platform-cluster.py",*inv,action)for action in("doctor","up","verify")],
        "bootstrap":[command("platform-release.py",*inv,"--phase","infrastructure","--config",config_path,"--output",bootstrap)],
        "maintenance":[command("platform-maintenance.py","run",*inv,"--config",config_path,"--infrastructure",bootstrap,"--output",root/"maintenance","--writers-stopped")],
        "activate":[command("platform-release.py",*inv,"--config",ready,"--output",full),command("platform-deploy.py","prepare-volumes",*inv,"--requirements",full/"service-requirements.json","--requirements",full/"monitoring-requirements.json"),command("platform-deploy.py","apply",*inv,"--bundle",full/"bundle.json")],
        "access":[command("platform-access.py","render",*inv,"--output",root/"access"),command("platform-access.py","start",*inv,"--output",root/"access")],
        "validate":[command("platform-acceptance.py","run","--config",root/"acceptance-inputs.json","--output",root/"acceptance")]
    }
    for suffix in("canonical","ingestion",SERVICE_FORMATION):commands["bootstrap"].append(command("platform-storage.py","prepare-disks",*inv,"--plan",bootstrap/(suffix+"-plan.json")))
    commands["bootstrap"].append(command("platform-deploy.py","apply",*inv,"--bundle",bootstrap/"bundle.json"))
    access=config.get("access",{})
    require(isinstance(access,dict)and set(access)<={"client_port","audit_port","client_node_port","audit_node_port"},"unknown validation access input")
    for key,value in access.items():
        require(type(value)is int,"access port inputs must be integers")
        commands["access"][0] += ["--"+key.replace("_","-"),str(value)]
    selected=list(PHASES)if phase=="all"else[phase]
    return {"version":1,"workflow_sha256":hash_json(config),"output":str(root),"phases":[{"name":p,"commands":commands[p]}for p in selected],"executed":False,"assumption":"Use declared machine allocation; live prerequisites are verified during cluster/apply. No machine measurements needed to write this plan."},config,base


def validation_inputs(config,base,root):
    values=config.get("validation");require(isinstance(values,dict),"validation input block required")
    access=json.loads((root/"access/access.json").read_text())
    def path(key):require(isinstance(values.get(key),str)and values[key],"validation path required: "+key);return str((base/values[key]).resolve())
    source={"version":1,"release_config":str(root/"maintenance/release-ready.json"),"source_reference":path("source_reference"),"audit_token_file":path("audit_token_file"),"satellite_token_file":path("satellite_token_file"),"audit_base_url":values.get("audit_base_url")or access["audit_base_url"],"disconnect_seconds":values.get("disconnect_seconds",10),"settle_seconds":values.get("settle_seconds",60),"max_clock_uncertainty_ms":values.get("max_clock_uncertainty_ms",500)}
    load=dict(values.get("load",{}))
    if load.get("token_file"):load["token_file"]=str((base/load["token_file"]).resolve())
    elif load.get("client_credentials_file"):load["client_credentials_file"]=str((base/load["client_credentials_file"]).resolve())
    else:load["client_credentials_file"]=str((base/config["private_release_config"]).resolve().parent/"client-credentials.json")
    load["base_url"]=load.get("base_url")or access["client_base_url"]
    acceptance={"version":1,"inventory":str((base/config["inventory"]).resolve()),"source_validation":str(root/"source-validation-inputs.json"),"load":load,"tests":values.get("tests",["satellite-burst","baseline-1","baseline-3","combined","reconcile"])}
    write_json(root/"source-validation-inputs.json",source);write_json(root/"acceptance-inputs.json",acceptance)


def execute(command,*,stdout,stderr,timeout):
    process=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True)
    try:
        return subprocess.CompletedProcess(command,process.wait(timeout=timeout))
    except BaseException:
        if process.poll()is None:
            os.killpg(process.pid,signal.SIGTERM)
            try:process.wait(timeout=45)
            except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
        raise


def run(path,phase="all",runner=execute):
    prepared,config,base=plan(path,phase);root=Path(prepared["output"])
    require(not root.is_symlink(),"workflow output must not be a symlink")
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    require(root.stat().st_uid==os.geteuid(),"workflow output must belong to the invoking user");root.chmod(0o700)
    require(fcntl is not None,"live workflow execution requires the Linux controller")
    with (root/"workflow.lock").open("a")as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError("another workflow process is using this output directory")from None
        return _run(prepared,config,base,root,runner)


def _run(prepared,config,base,root,runner):
    state_path=root/"workflow-state.json"
    state=json.loads(state_path.read_text())if state_path.exists()else {"version":1,"workflow_sha256":prepared["workflow_sha256"],"steps":{}}
    require(state.get("workflow_sha256")==prepared["workflow_sha256"],"workflow inputs changed; preserve this run and select a new output directory")
    for phase_value in prepared["phases"]:
        name=phase_value["name"]
        if name=="validate":validation_inputs(config,base,root)
        for index,command in enumerate(phase_value["commands"]):
            key=name+"-"+str(index);old=state["steps"].get(key)
            if old:
                require(old["command_sha256"]==hash_json(command),"recorded workflow command differs")
                if old.get("completed"):continue
                raise ValueError("workflow step has an uncertain or failed prior result: "+key+"; inspect retained log/artifacts before selecting the next phase or a new run")
            state["steps"][key]={"command_sha256":hash_json(command),"intent":True,"completed":False};write_report(state_path,state)
            log=root/(key+".log")
            with log.open("wb")as stream:result=runner(command,stdout=stream,stderr=subprocess.STDOUT,timeout=14400)
            log.chmod(0o600);require(result.returncode==0,"workflow step failed: "+key+"; inspect private log "+str(log))
            state["steps"][key]["completed"]=True;write_report(state_path,state)
            print("Completed workflow step: "+key,flush=True)
    return state
