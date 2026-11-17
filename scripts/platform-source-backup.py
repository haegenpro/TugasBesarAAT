#!/usr/bin/env python3
"""Plan source checkpoints or capture them after operators stop all selected writers."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from source_backup_lib import capture,plan
from storage_lib import require

def main():
    os.umask(0o077);parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['plan','capture']);parser.add_argument('--inventory',type=Path,required=True)
    parser.add_argument('--bundle',type=Path,required=True);parser.add_argument('--requirements',type=Path,action='append',required=True)
    parser.add_argument('--claim',action='append',default=[]);parser.add_argument('--output',type=Path);parser.add_argument('--writers-stopped',action='store_true')
    args=parser.parse_args();inventory=load(args.inventory)
    prepared=plan(inventory,args.bundle,args.requirements,args.claim)
    if args.action=='plan':print(json.dumps(prepared,indent=2));return
    require(args.writers_stopped and args.output,'capture requires --writers-stopped and a new private --output')
    cluster=Cluster(inventory)
    with cluster.locked():records=capture(cluster,prepared,args.output)
    print('Captured '+str(len(records))+' immutable source checkpoints; writers remain stopped.')

if __name__=='__main__':
    try:main()
    except (ValueError,KeyError,OSError,RuntimeError,subprocess.SubprocessError)as exc:print('Source backup stopped: '+str(exc),file=sys.stderr);sys.exit(1)
