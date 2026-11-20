#!/usr/bin/env python3
"""Plan or explicitly reset only the existing cluster's assignment application."""
import argparse
import json
import os
from pathlib import Path
import subprocess
from application_reset_lib import apply, build_plan
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import require, write_json

def main():
    os.umask(0o077)
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['plan','apply']);p.add_argument('--inventory',type=Path,required=True)
    p.add_argument('--volumes',type=Path);p.add_argument('--image');p.add_argument('--plan',type=Path)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--delete-data',action='store_true')
    a=p.parse_args();cfg=load(a.inventory);cluster=Cluster(cfg)
    if a.action=='plan':
        require(a.volumes and a.image and not a.output.exists(),'new output, volume plan and immutable maintenance image required')
        namespace=json.loads(cluster.kubectl('get','namespace',cfg['namespace'],'-o','json').stdout)
        nodes=json.loads(cluster.kubectl('get','nodes','-o','json').stdout)['items']
        pvs=json.loads(cluster.kubectl('get','pv','-o','json').stdout)['items']
        result=build_plan(cfg,namespace,nodes,pvs,json.loads(a.volumes.read_text())['volumes'],a.image)
        write_json(a.output,result);print('Reviewed reset plan: '+str(a.output));return
    require(a.plan,'--plan required')
    with cluster.locked():apply(cluster,json.loads(a.plan.read_text()),a.output,delete_data=a.delete_data)

if __name__=='__main__':
    try:main()
    except (ValueError,KeyError,OSError,RuntimeError,subprocess.SubprocessError)as exc:raise SystemExit('Application reset stopped: '+str(exc))
