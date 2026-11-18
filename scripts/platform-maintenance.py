#!/usr/bin/env python3
"""Plan or execute stopped-writer initialization and produce verified release receipts."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from maintenance_lib import inputs,run
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import require


def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["plan","run"])
    for name in("inventory","config","infrastructure"):p.add_argument("--"+name,type=Path,required=True)
    p.add_argument("--output",type=Path);p.add_argument("--writers-stopped",action="store_true")
    a=p.parse_args();inventory=load(a.inventory)
    if a.action=="plan":
        values=inputs(inventory,a.config,a.infrastructure)
        print(json.dumps({"contract":values[5],"steps":["verify stopped application controllers/pods","capture all six original formations","initialize owner schemas and pinned Registry","canonical migration","ingestion migration","BMKG cutover","verify schema journals, cutover and unchanged identities","write bound receipt and release-ready.json"],"executed":False},indent=2));return
    require(a.writers_stopped and a.output,"run needs --writers-stopped and a new --output directory")
    cluster=Cluster(inventory)
    with cluster.locked():run(cluster,a.config,a.infrastructure,a.output)
    print("Maintenance verified; next release configuration: "+str(a.output/"release-ready.json"))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:
        print("Maintenance stopped: "+str(exc),file=sys.stderr);sys.exit(1)
