#!/usr/bin/env python3
"""Render owned database formations or explicitly initialize stopped services' schemas."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from database_lib import render
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from service_lib import DIGEST
from storage_lib import SERVICE_DATABASES,plan,require,write_json

ROOT=Path(__file__).resolve().parent.parent

def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["render","initialize","bootstrap-registry"])
    p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.fourhost.example.json")
    p.add_argument("--image");p.add_argument("--output",type=Path);p.add_argument("--plan",type=Path);p.add_argument("--writers-stopped",action="store_true",help="acknowledge maintenance window; all owning application pods must be stopped")
    a=p.parse_args();inventory=load(a.inventory)
    if a.action=="render":
        require(a.image and a.output,"render needs immutable --image and --output")
        resources,report=render(inventory,a.image);require(not a.output.exists(),"output already exists")
        a.output.mkdir(parents=True,mode=0o700);write_json(a.output/"resources.json",resources);write_json(a.output/"requirements.json",report)
        for storage in report["formations"]:write_json(a.output/(storage["formation_suffix"]+"-plan.json"),storage)
        return
    require(a.writers_stopped and a.plan,"administrator initialization requires --writers-stopped and an original --plan")
    storage=json.loads(a.plan.read_text());suffix=storage.get("formation_suffix")
    require(suffix in SERVICE_DATABASES and storage==plan(inventory,"postgres",storage["image"],suffix),"owner storage plan differs")
    c=Cluster(inventory);ns=inventory["namespace"]
    with c.locked():
        from database_admin_lib import initialize
        if a.action=="bootstrap-registry":
            require(suffix=="registry-db"and a.image and DIGEST.fullmatch(a.image),"registry bootstrap requires its actual pinned application image")
        initialize(c,storage,a.image if a.action=="bootstrap-registry"else None)
        print("Explicit owner database initialization complete; no runtime application credential performed DDL.")

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:print("Owner database operation stopped: "+str(exc),file=sys.stderr);sys.exit(1)
