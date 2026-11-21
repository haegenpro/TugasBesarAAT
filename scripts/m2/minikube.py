#!/usr/bin/env python3
"""Start the isolated development cluster without changing the host kubeconfig."""
import argparse
import json
import os
import subprocess
from common import LOCAL, ROOT, private_json

PROFILE=LOCAL/'minikube/.minikube/profiles/aat-m2/config.json'


def command(env,*args,check=True):
    return subprocess.run([str(LOCAL/'bin/minikube'),'-p','aat-m2',*args],env=env,check=check)


def actual_nodes(env):
    output=subprocess.check_output([str(LOCAL/'bin/kubectl'),'--context','aat-m2',
                                    'get','nodes','-o','json'],env=env,text=True)
    return json.loads(output)['items']


def reconcile(env):
    nodes=actual_nodes(env)
    # Minikube's Docker IP allocator can leave a failed node with no assigned
    # IP when the primary took its proposed address. Only forget such an empty
    # cache entry after another node successfully joined; never delete a node,
    # container or volume here.
    if len(nodes)!=2:
        raise ValueError('expected exactly two real Kubernetes nodes')
    state=json.loads(PROFILE.read_text())
    state['Nodes']=[n for n in state['Nodes'] if n.get('IP')]
    private_json(PROFILE,state)
    primary=next(n['metadata']['name'] for n in nodes if 'node-role.kubernetes.io/control-plane' in n['metadata'].get('labels',{}))
    worker=next(n['metadata']['name'] for n in nodes if n['metadata']['name']!=primary)
    path=LOCAL/'config.json'
    cfg=json.loads(path.read_text()) if path.exists() else json.loads((ROOT/'deployment/m2/config.example.json').read_text())
    expected={'a':primary,'b':worker}
    if cfg['nodes']!=expected:
        deployed=subprocess.run([str(LOCAL/'bin/kubectl'),'--context','aat-m2','get','statefulsets',
                                '-n',cfg['namespace'],'-o','name'],env=env,capture_output=True,text=True)
        if deployed.stdout.strip():
            raise ValueError('deployed node names differ; review config and volume affinity before changing them')
        cfg['nodes']=expected
    private_json(path,cfg)
    print(f"Development nodes: A={primary}, B={worker}; private configuration updated.")


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('start','status','stop'))
    parser.add_argument('--memory',type=int,default=6144,help='MiB Docker limit per development node')
    parser.add_argument('--cpus',type=int,default=2)
    args=parser.parse_args()
    env=os.environ.copy();env.update(MINIKUBE_HOME=str(LOCAL/'minikube'),KUBECONFIG=str(LOCAL/'kubeconfig'))
    if args.command!='start':return command(env,args.command,check=False).returncode
    existing=json.loads(PROFILE.read_text()) if PROFILE.exists() else {}
    if existing.get('StaticIP'):parser.error('static IP profiles cannot add workers; use a clean development profile')
    command(env,'start','--driver=docker','--nodes='+str(max(1,len([n for n in existing.get('Nodes',[]) if n.get('IP')]))),
            '--cpus='+str(args.cpus),'--memory='+str(args.memory),'--kubernetes-version=v1.35.0','--cni=calico','--delete-on-failure=false')
    if len(actual_nodes(env))==1:
        result=command(env,'node','add',check=False)
        if result.returncode:
            state=json.loads(PROFILE.read_text())
            if not any(not n.get('IP') for n in state['Nodes']):
                raise ValueError('worker creation failed without an empty address entry; inspect Minikube diagnostics')
            # A second attempt uses the next free address; keep the failed entry
            # until that allocation has completed.
            command(env,'node','add')
    reconcile(env)
    return 0


if __name__=='__main__':raise SystemExit(main())
