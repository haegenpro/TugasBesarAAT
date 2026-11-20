#!/usr/bin/env python3
"""Render or start the owned loopback application validation proxy."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from validation_access_lib import render,start


def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["render","start"])
    p.add_argument("--inventory",type=Path,required=True);p.add_argument("--output",type=Path,required=True)
    for name,default in(("client-port",18080),("audit-port",18081),("client-node-port",31080),("audit-node-port",31081)):p.add_argument("--"+name,type=int,default=default)
    p.add_argument("--controller-address",help="private Linux controller IPv4 address for inventory v3")
    p.add_argument("--generator-address",help="optional private IPv4 address for direct NodePort access")
    a=p.parse_args();inventory=load(a.inventory)
    if a.action=="render":report=render(inventory,a.output,a.client_port,a.audit_port,a.client_node_port,a.audit_node_port,
                                       controller_address=a.controller_address,generator_address=a.generator_address)
    else:
        if a.controller_address or a.generator_address:p.error("caller addresses belong to the reviewed render step")
        cluster=Cluster(inventory)
        with cluster.locked():report=start(cluster,a.output)
    print(json.dumps(report,indent=2))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:print("Validation access stopped: "+str(exc),file=sys.stderr);sys.exit(1)
