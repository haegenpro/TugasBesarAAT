#!/usr/bin/env python3
"""Plan or explicitly run fenced, all-volume empty-VM recovery across four hosts."""
import argparse
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import time
import uuid

from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from replacement_lib import RESTORE,controller_holds,load_plan,read_report,recovery_module,set_controller_hold,write_report
from storage_lib import require,write_json

ROOT=Path(__file__).resolve().parent.parent

def execute(inventory,prepared):
    require(prepared["ready"],"resolve every recovery blocker before mutating the node")
    c=Cluster(inventory);target=prepared["target"];ns=inventory["namespace"];run_id=uuid.uuid4().hex
    output=c.state/"evidence"/("integrated-replacement-"+run_id)
    report={"version":2,"cluster_id":inventory['cluster_id'],"run_id":run_id,"node":target["name"],"passed":False,"started_at":datetime.now(timezone.utc).isoformat(),"events":[],"scope":"original replica placement and retained-prefix recovery; independent accepted-operation/checksum reconciliation still required"}
    def record(phase,**fields):
        report["events"].append({"phase":phase,"at":datetime.now(timezone.utc).isoformat(),**fields});write_report(output/"result.json",report);print(phase,flush=True)
    def wait(check,message,seconds=300):
        deadline=time.monotonic()+seconds
        while True:
            try:return check()
            except (ValueError,KeyError,RuntimeError,subprocess.SubprocessError):
                require(time.monotonic()<deadline,message);time.sleep(2)
    module=recovery_module();helpers=[];held={};brokers=[]
    with c.locked():
        output.mkdir(parents=True,mode=0o700)
        try:
            require(c.vm_state(target)=="shut off","failed VM must already be fenced shut off")
            require(all(c.vm_state(n)=="running"for n in inventory["nodes"]if n["name"]!=target["name"]),"recovery supports one failed member with all other VMs running")
            require(c.doctor()["ready"],"all physical hosts need recovery memory/disk headroom")
            pods=json.loads(c.kubectl("get","pods","-A","--field-selector","spec.nodeName="+target["name"],"-o","json").stdout)["items"]
            known={v["claim"]for v in prepared["volumes"]}
            for pod in pods:
                if pod["metadata"]["namespace"]in("kube-system","tester-agent"):continue
                require(pod["metadata"]["namespace"]==ns,"foreign workload on replacement node")
                for volume in pod["spec"].get("volumes",[]):
                    if "persistentVolumeClaim"in volume:require(volume["persistentVolumeClaim"]["claimName"]in known,"live persistent workload is absent from recovery plan")
                    if "hostPath"in volume:
                        require(pod["metadata"].get("labels",{}).get("app.kubernetes.io/name")=="node-exporter"and volume["hostPath"]["path"]in("/","/proc","/sys"),"unknown hostPath recovery refused")
            for formation in prepared["formations"]:
                helper=module.Recovery(inventory,formation["plan"],formation["config"],target["name"],provider_aware=True)
                helper.cluster=c
                helper.record=lambda phase,_name=formation["plan"]["formation"],**fields:record(_name+": "+phase,**fields)
                anchors=helper.validate_survivors();helpers.append((helper,anchors,None))
            record("all PostgreSQL formations retain two original flushed copies",original_wal_anchors={h.storage['formation']:a for h,a,_ in helpers})
            def broker_call(volume,operation):
                workload=volume["controller"]["name"];lost=workload+"-0"
                original=["broker-"+letter+"-0"for letter in "abc"]
                require(lost in original,"unexpected broker member identity")
                survivor=next(name for name in original if name!=lost)
                require(operation in('observe','verify'),'same-name replacement must retain original broker membership')
                response=c.kubectl("exec","-n",ns,survivor,"-c","broker","--","/usr/local/bin/recover-node","--operation",operation,"--server",lost,"--members",",".join(original),"--streams",",".join(volume["streams"]),timeout=15)
                return json.loads(response.stdout)
            for volume in prepared["volumes"]:
                if volume["kind"]=="jetstream":
                    baseline=wait(lambda:broker_call(volume,"observe"),"surviving broker quorum or system credential unavailable",60)
                    brokers.append((volume,baseline))
            record("broker quorum and original-cluster system credential verified")
            # Stop only the already-lost member controllers. This prevents a
            # freshly booted source from writing into a directory being restored.
            held=controller_holds(c,prepared);report['held_controllers']=held
            record("controller hold intent recorded",mutation_intent=True,original_controllers=held)
            for key,identity in held.items():
                set_controller_hold(c,ns,key,identity)
                record('original controller held',controller=key,uid=identity['uid'])
            for volume,baseline in brokers:
                record("original broker membership retained for same-name empty-storage rejoin",member=volume["controller"]["name"]+"-0",baseline=baseline)
            for index,(helper,anchors,_)in enumerate(helpers):
                if target['name']in helper.storage['nodes'][:3]:record('DCS replacement intent',formation=helper.storage['formation'],mutation_intent=True)
                helpers[index]=(helper,anchors,helper.replace_dcs_member())
            record('fenced Kubernetes node removal intent',mutation_intent=True)
            c.kubectl("delete","node",target["name"],"--wait=true","--timeout=60s",timeout=70)
            for pod in pods:
                if pod["metadata"]["namespace"]==ns:c.kubectl("delete","pod",pod["metadata"]["name"],"-n",ns,"--grace-period=0","--force","--ignore-not-found=true")
            record('provider empty-disk replacement intent',mutation_intent=True)
            retained=c.replace_empty(target,run_id);record("provider retained old disks and prepared stopped empty VM",provider=retained)
            c.run(["ssh-keygen","-R",target["address"],"-f",c.private/"known_hosts"],check=False)
            record('empty replacement boot intent',mutation_intent=True)
            c.vm_action(target,"start");c.wait_ssh(target)
            seed=next(n for n in inventory["nodes"]if n["role"]=="server"and n["name"]!=target["name"])
            token=c.ssh(seed,["sudo","-n","cat","/var/lib/rancher/k3s/server/token"]).stdout.decode().strip()
            require(token.startswith("K10")and"::"in token,"CA-pinned surviving join token required");c.redactions.append(token)
            version=inventory["k3s"]
            binary=c.fetch(version["binary_url"],version["binary_sha256"],"k3s");installer=c.fetch(version["installer_url"],version["installer_sha256"],"k3s-install.sh")
            record('replacement Kubernetes join intent',seed=seed['name'],mutation_intent=True)
            c.install_node(target,binary,installer,token,False,join_server=seed["address"])
            marker=json.loads((c.state/"owner.json").read_text());marker["bootstrap_seed"]=seed["name"];write_json(c.state/"owner.json",marker)
            spec=importlib.util.spec_from_file_location("storage_setup",ROOT/"scripts/platform-storage.py");setup=importlib.util.module_from_spec(spec);spec.loader.exec_module(setup)
            record('original volume preparation intent',mutation_intent=True)
            for helper,_,_ in helpers:setup.prepare_disks(c,helper.storage)
            spec=importlib.util.spec_from_file_location("service_setup",ROOT/"scripts/platform-deploy.py");services=importlib.util.module_from_spec(spec);spec.loader.exec_module(services)
            services.prepare_volumes(c,[Path(p)for p in prepared["volume_requirements"]])
            for volume in prepared["volumes"]:
                if volume["kind"]=="restore":
                    archive=Path(volume["archive"]);raw=archive.read_bytes()
                    require(len(raw)<=64<<20 and hashlib.sha256(raw).hexdigest()==volume["sha256"],"restore artifact changed since planning")
                    record('source checkpoint restore intent',claim=volume['claim'],sha256=volume['sha256'],mutation_intent=True)
                    c.ssh(target,["sudo","-n","python3","-c",RESTORE,json.dumps({k:volume[k]for k in("path","uid","sha256")})],input=raw)
                elif volume["kind"]=="observability":record("observability volume recreated with declared evidence gap",claim=volume["claim"])
            record('original controller release intent',mutation_intent=True)
            for key,identity in held.items():
                set_controller_hold(c,ns,key,identity,restore=True)
                record('original controller released',controller=key,uid=identity['uid'],replicas=identity['replicas'])
            for helper,_,learner in helpers:
                if learner:
                    record('DCS learner promotion intent',formation=helper.storage['formation'],member_id=format(learner[1],'x'),mutation_intent=True)
                    wait(lambda h=helper,l=learner:h.etcd(l[0],"member","promote",format(l[1],"x")),"DCS learner catch-up deadline")
                anchors=helper.validate_survivors()
                for group in helper.plan["groups"]:
                    def caught_up(h=helper,g=group):
                        status=h.member_status(g["replacement"]);anchor=anchors[g["id"]]
                        require(status["system_id"]==anchor["system_id"] and not status["primary"],"replacement physical identity or role differs")
                        replay=h.sql(g["replacement"],"SELECT to_json(pg_last_wal_replay_lsn()::text)")
                        require(replay and module.lsn(replay)>=module.lsn(anchor["target_lsn"]),"replacement has not replayed original survivor prefix")
                        receiver=h.sql(g["replacement"],"SELECT coalesce((SELECT jsonb_build_object('host',sender_host,'timeline',received_tli,'state',status) FROM pg_stat_wal_receiver),'{}'::jsonb)")
                        leader=next(m for m in g["survivors"]if m["name"]==anchor["primary"])
                        require(receiver.get("host")==leader["host"]and receiver.get("timeline")==int(anchor["timeline"],16)and receiver.get("state")=="streaming","replacement follows another primary/timeline")
                    wait(caught_up,"PostgreSQL replica catch-up deadline")
                record("original three-copy formation rebuilt",formation=helper.storage["formation"])
            for volume,baseline in brokers:
                def broker_ready(v=volume,b=baseline):
                    current=broker_call(v,"verify");before={s["name"]:s["last_sequence"]for s in b["streams"]}
                    require(all(s["last_sequence"]>=before[s["name"]]for s in current["streams"]),"broker stream sequence regressed")
                    durable_before={(s['name'],consumer['name']):consumer['ack_floor_stream']for s in b['streams']for consumer in s.get('consumers',[])}
                    durable_after={(s['name'],consumer['name']):consumer['ack_floor_stream']for s in current['streams']for consumer in s.get('consumers',[])}
                    require(all(key in durable_after and durable_after[key]>=floor for key,floor in durable_before.items()),'durable consumer disappeared or its acknowledgement prefix regressed')
                    return current
                record("broker replacement current on original stream groups",state=wait(broker_ready,"JetStream replica catch-up deadline"))
            for key in held:c.kubectl("rollout","status",key,"-n",ns,"--timeout=300s",timeout=330)
            c.verify();report["passed"]=True;record("all planned workloads restored; independent logical reconciliation still required")
        except BaseException as exc:
            report["error"]=type(exc).__name__;report["manual_recovery_required"]=True
            # Do not boot quarantined old disks or release uncertain data behind
            # the operator's back. Keep original identities and all journals.
            raise
        finally:write_report(output/"result.json",report)

def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["plan","replace-node","status"]);p.add_argument("--inventory",type=Path,required=True);p.add_argument("--config",type=Path);p.add_argument("--node",required=True);p.add_argument('--run')
    a=p.parse_args();inventory=load(a.inventory)
    if a.action=='status':print(json.dumps(read_report(inventory,a.run,a.node),indent=2));return
    require(a.config,'plan/replacement requires --config');prepared=load_plan(inventory,a.config,a.node)
    if a.action=="plan":print(json.dumps(prepared,indent=2));return
    execute(inventory,prepared)

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,RuntimeError,tarfile.TarError,subprocess.SubprocessError)as exc:print("Replacement stopped: "+str(exc),file=sys.stderr);sys.exit(1)
