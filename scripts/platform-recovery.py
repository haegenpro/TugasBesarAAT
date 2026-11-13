#!/usr/bin/env python3
"""Explicit, unverified recovery workflow; never invoked by ordinary bootstrap.

plan is offline. replace-node requires the failed owned VM already shut off,
preserves old disks, rejoins an empty replacement and checks replica catch-up.
Supports platform bootstrap/canonical storage only; unknown local data blocks it.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import durability_config,plan,require,write_json

ROOT=Path(__file__).resolve().parent.parent


def recovery_plan(inventory,storage,node_name):
    require(storage==plan(inventory,storage["mode"],storage["image"]),"storage plan differs from inventory")
    target=next((n for n in inventory["nodes"] if n["name"]==node_name),None)
    require(target,"replacement target is not an owned node")
    groups=[{"id":g["id"],"replacement":next(m for m in g["members"] if m["node"]==node_name),
             "survivors":[m for m in g["members"] if m["node"]!=node_name]} for g in storage["groups"] if any(m["node"]==node_name for m in g["members"])]
    require(all(len(g["survivors"])==2 for g in groups),"two original copies are required")
    return {"node":target,"groups":groups,"retained_disks":[f"{node_name}-{kind}.qcow2"for kind in ("root","data")],
            "phases":["prove VM shut off and reject unknown local workloads","prove two surviving flushed copies",
                      "replace lost DCS membership as learner","remove fenced Kubernetes node",
                      "quarantine old disks and create empty replacement","join through a surviving server",
                      "prepare only original local paths","promote caught-up DCS learner","verify fresh PostgreSQL catch-up and four-node health"],
            "scope":"Bootstrap and canonical storage; unknown application persistence requires owner recovery hooks."}


def lsn(value):
    high,low=value.split("/");return int(high,16)<<32|int(low,16)

def validate_config(storage,config):
    expected=durability_config(storage)
    require(len(config["groups"])==len(expected["groups"]),"captured group count differs")
    for group,actual in zip(expected["groups"],config["groups"]):
        require(isinstance(actual["system_id"],str) and actual["system_id"].isdecimal() and int(actual["system_id"])>0,"real bootstrap system IDs required")
        group["system_id"]=actual["system_id"]
    require(config==expected and len({g["system_id"]for g in config["groups"]})==len(config["groups"]),"original formation configuration differs")


class Recovery:
    def __init__(self,inventory,storage,config,target):
        validate_config(storage,config)
        self.cluster=Cluster(inventory);self.inventory=inventory;self.storage=storage;self.config=config
        self.plan=recovery_plan(inventory,storage,target);self.target=self.plan["node"]
        self.run_id=uuid.uuid4().hex
        self.evidence=self.cluster.state/"evidence"/("replacement-"+self.run_id)
        self.report={"run_id":self.run_id,"node":target,"passed":False,"completed":[]}
    def record(self,phase,**details):
        self.report["completed"].append({"phase":phase,**details})
        write_json(self.evidence/"result.json",self.report)
        print(phase,flush=True)
    def sql(self,member,query):
        result=self.cluster.kubectl("exec","-n",self.storage["namespace"],member["name"],"-c","postgres","--",
            "psql","-U","postgres","-d","hazards","-Atc",query,timeout=12)
        return json.loads(result.stdout)
    def member_status(self,member):return self.sql(member,"SELECT public.platform_replication_status()")
    def validate_survivors(self):
        anchors={}
        for group in self.plan["groups"]:
            statuses=[self.member_status(m) for m in group["survivors"]]
            expected=self.config["groups"][group["id"]]["system_id"]
            require(all(s["system_id"]==expected and s["formation"]==self.storage["formation"] and s["group_id"]==group["id"] for s in statuses),"survivor identity mismatch")
            leaders=[s for s in statuses if s["primary"]]
            require(len(leaders)==1,"survivors must have exactly one primary")
            primary=leaders[0];target=lsn(primary["insert_lsn"])
            peer=next(s for s in statuses if not s["primary"])
            require(primary["fsync"]=="on" and primary["full_page_writes"]=="on" and primary["synchronous_commit"]=="on","unsafe primary settings")
            leader=next(m for m in group["survivors"] if m["name"]==primary["member"])
            deadline=time.monotonic()+5
            while True:
                current=self.member_status(leader)
                require(current["primary"] and all(current[k]==primary[k]for k in("member","system_id","timeline","started")),"survivor primary changed during confirmation")
                ready=lsn(current["flush_lsn"])>=target and any(p["member"]==peer["member"] and p["state"]=="streaming" and p["sync_state"]in("sync","quorum") and p["flush_lsn"] and lsn(p["flush_lsn"])>=target for p in current["replicas"])
                if ready:break
                require(time.monotonic()<deadline,"two survivors have not flushed captured WAL");time.sleep(0.1)
            anchors[group["id"]]={"primary":primary["member"],"system_id":expected,"timeline":primary["timeline"],"target_lsn":primary["insert_lsn"]}
        return anchors
    def etcd(self,pod,*arguments):
        return self.cluster.kubectl("exec","-n",self.storage["namespace"],pod,"--","/usr/local/bin/etcdctl",
            "--endpoints=https://127.0.0.1:2379","--cacert=/tls/ca.crt","--cert=/tls/tls.crt","--key=/tls/tls.key",*arguments,timeout=15)
    def replace_dcs_member(self):
        index=self.storage["nodes"].index(self.target["name"])
        if index>=3:return None
        formation=self.storage["formation"];target=f"{formation}-etcd-{chr(97+index)}-0"
        survivor=f"{formation}-etcd-{next(chr(97+i)for i in range(3)if i!=index)}-0"
        members=json.loads(self.etcd(survivor,"member","list","--write-out=json").stdout)["members"]
        old=next(m for m in members if m["name"]==target)
        self.etcd(survivor,"member","remove",format(old["ID"],"x"))
        peer=f"https://{target}.{formation}-etcd-members.{self.storage['namespace']}.svc.cluster.local:2380"
        added=json.loads(self.etcd(survivor,"member","add",target,"--peer-urls="+peer,"--learner","--write-out=json").stdout)
        new_id=added["member"]["ID"]
        names=[]
        for member in added["members"]:
            name=member.get("name") or (target if member["ID"]==new_id else "")
            require(name,"DCS member name unavailable")
            names.append(name+"="+member["peerURLs"][0])
        ss=json.loads(self.cluster.kubectl("get","statefulset",target[:-2],"-n",self.storage["namespace"],"-o","json").stdout)
        require(ss["metadata"]["labels"].get("platform.aat/formation")==formation,"unowned DCS workload")
        args=ss["spec"]["template"]["spec"]["containers"][0]["args"]
        args=["--initial-cluster-state=existing"if a.startswith("--initial-cluster-state=")else "--initial-cluster="+",".join(names)if a.startswith("--initial-cluster=")else a for a in args]
        self.cluster.kubectl("patch","statefulset",target[:-2],"-n",self.storage["namespace"],"--type=json","-p",json.dumps([{"op":"replace","path":"/spec/template/spec/containers/0/args","value":args}]))
        self.record("DCS replacement admitted as learner",member_id=format(new_id,"x"))
        return survivor,new_id
    def run(self):
        c=self.cluster
        with c.locked():
            self.evidence.mkdir(mode=0o700)
            try:
                c.assert_resource(c.virsh("dumpxml",self.target["name"]).stdout.decode(),self.target["name"])
                require(c.virsh("domstate",self.target["name"]).stdout.decode().strip()=="shut off","failed VM must be fenced shut off before replacement")
                require(c.doctor()["ready"],"replacement needs host memory/disk headroom")
                pods=json.loads(c.kubectl("get","pods","-A","--field-selector","spec.nodeName="+self.target["name"],"-o","json").stdout)["items"]
                for pod in pods:
                    persistent=any("persistentVolumeClaim"in v or "hostPath"in v for v in pod["spec"].get("volumes",[]))
                    owned=pod["metadata"].get("labels",{}).get("platform.aat/formation")==self.storage["formation"]
                    system=pod["metadata"]["namespace"]in("kube-system","tester-agent")
                    require(not persistent or owned or system,"unknown persistent workload requires its owner's recovery hook before disk replacement")
                anchors=self.validate_survivors();self.record("surviving copies confirmed",groups=anchors)
                learner=self.replace_dcs_member()
                c.kubectl("delete","node",self.target["name"],"--wait=true","--timeout=60s",timeout=70)
                for pod in pods:
                    if pod["metadata"]["namespace"]==self.storage["namespace"]:
                        c.kubectl("delete","pod",pod["metadata"]["name"],"-n",self.storage["namespace"],"--grace-period=0","--force","--ignore-not-found=true")
                quarantine=c.state/"quarantine"/self.run_id;quarantine.mkdir(parents=True,mode=0o700)
                for filename in self.plan["retained_disks"]:
                    disk=c.state/"disks"/filename
                    require(disk.is_file() and not disk.is_symlink(),"expected owned disk is missing or a symlink")
                    disk.rename(quarantine/filename)
                self.record("old disks quarantined",path=str(quarantine))
                c.prepare()
                c.run(["ssh-keygen","-R",self.target["address"],"-f",c.private/"known_hosts"],check=False)
                c.virsh("start",self.target["name"]);c.wait_ssh(self.target)
                seed=next(n for n in self.inventory["nodes"] if n["role"]=="server" and n["name"]!=self.target["name"])
                state=json.loads((c.state/"owner.json").read_text());state["bootstrap_seed"]=seed["name"];write_json(c.state/"owner.json",state)
                version=self.inventory["k3s"];binary=c.fetch(version["binary_url"],version["binary_sha256"],"k3s")
                installer=c.fetch(version["installer_url"],version["installer_sha256"],"k3s-install.sh")
                token=c.ssh(seed,["sudo","-n","cat","/var/lib/rancher/k3s/server/token"]).stdout.decode().strip()
                require(token.startswith("K10")and"::"in token,"surviving server did not provide a CA-pinned join token")
                c.redactions.append(token)
                c.install_node(self.target,binary,installer,token,False,join_server=seed["address"])
                spec=importlib.util.spec_from_file_location("storage_setup",ROOT/"scripts/platform-storage.py")
                module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
                module.prepare_disks(c,self.storage)
                self.record("empty node joined with original storage placement")
                if learner:
                    deadline=time.monotonic()+300
                    while True:
                        try:self.etcd(learner[0],"member","promote",format(learner[1],"x"));break
                        except RuntimeError:
                            require(time.monotonic()<deadline,"DCS learner did not catch up; retain evidence and do not restore old disks")
                            time.sleep(5)
                    self.record("DCS learner promoted after catch-up")
                anchors=self.validate_survivors()
                deadline=time.monotonic()+300
                for group in self.plan["groups"]:
                    while True:
                        try:
                            status=self.member_status(group["replacement"])
                            require(status["system_id"]==anchors[group["id"]]["system_id"] and not status["primary"],"replacement identity/role not established")
                            replay=self.sql(group["replacement"],"SELECT to_json(pg_last_wal_replay_lsn()::text)")
                            require(replay and lsn(replay)>=lsn(anchors[group["id"]]["target_lsn"]),"replacement has not replayed captured survivor WAL")
                            leader=next(m for m in group["survivors"]if m["name"]==anchors[group["id"]]["primary"])
                            receiver=self.sql(group["replacement"],"SELECT coalesce((SELECT jsonb_build_object('host',sender_host,'timeline',received_tli,'state',status) FROM pg_stat_wal_receiver),'{}'::jsonb)")
                            require(receiver.get("host")==leader["host"]and receiver.get("timeline")==int(anchors[group["id"]]["timeline"],16)and receiver.get("state")=="streaming","replacement is not following the recorded primary timeline")
                            break
                        except (RuntimeError,ValueError,KeyError,subprocess.SubprocessError):
                            require(time.monotonic()<deadline,"replacement copy did not catch up")
                            time.sleep(5)
                c.verify();self.report["passed"]=True;self.record("original three-copy placement restored through recorded WAL prefix")
            except BaseException as error:
                self.report["error"]=type(error).__name__+": "+str(error);raise
            finally:write_json(self.evidence/"result.json",self.report)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("command",choices=["plan","replace-node"])
    p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.example.json")
    p.add_argument("--storage-plan",type=Path,required=True);p.add_argument("--node",required=True);p.add_argument("--config",type=Path)
    a=p.parse_args();inventory=load(a.inventory);storage=json.loads(a.storage_plan.read_text())
    if a.command=="plan":print(json.dumps(recovery_plan(inventory,storage,a.node),indent=2));return
    require(a.config,"replacement requires the original captured durability config")
    config=json.loads(a.config.read_text());require(config["formation"]==storage["formation"] and config["mode"]==storage["mode"],"formation config mismatch")
    Recovery(inventory,storage,config,a.node).run()

if __name__=="__main__":
    try:main()
    except (OSError,ValueError,KeyError,RuntimeError,subprocess.SubprocessError)as e:print(f"Recovery stopped: {e}",file=sys.stderr);sys.exit(1)
