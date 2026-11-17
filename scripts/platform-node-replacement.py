#!/usr/bin/env python3
"""Plan, execute, inspect or resume fenced empty-disk recovery across four hosts."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from platform_lib.inventory import load
from replacement_lib import load_plan,read_report
from replacement_resume_lib import ReplacementRunner
from storage_lib import require

def execute(inventory,prepared,run_id=None):
    return ReplacementRunner(inventory,prepared,run_id).run()

def main():
    os.umask(0o077);parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['plan','replace-node','resume-node','status'])
    parser.add_argument('--inventory',type=Path,required=True);parser.add_argument('--config',type=Path)
    parser.add_argument('--node',required=True);parser.add_argument('--run')
    args=parser.parse_args();inventory=load(args.inventory)
    if args.action=='status':print(json.dumps(read_report(inventory,args.run,args.node),indent=2));return
    require(args.config,'plan/replacement requires --config');prepared=load_plan(inventory,args.config,args.node)
    if args.action=='plan':print(json.dumps(prepared,indent=2));return
    require((args.action=='resume-node')==bool(args.run),'resume-node requires its original --run; fresh replacement must not supply one')
    output=execute(inventory,prepared,args.run)
    print('Recovery evidence: '+str(output))

if __name__=='__main__':
    try:main()
    except (ValueError,KeyError,OSError,RuntimeError,tarfile.TarError,subprocess.SubprocessError)as exc:
        print('Replacement stopped: '+str(exc),file=sys.stderr);sys.exit(1)
