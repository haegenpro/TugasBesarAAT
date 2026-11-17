#!/usr/bin/env python3
"""Prepare a complete owner-integrated release; never builds or deploys it."""
import argparse
import os
from pathlib import Path
import sys
from platform_lib.inventory import load
from release_lib import assemble

ROOT=Path(__file__).resolve().parent.parent

def main():
    os.umask(0o077)
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.example.json")
    p.add_argument("--config",type=Path,required=True);p.add_argument("--output",type=Path,required=True)
    p.add_argument("--phase",choices=["full","infrastructure"],default="full",help="infrastructure prepares only storage, Secrets and isolation; never activates sources")
    a=p.parse_args()
    renderer=assemble
    if a.phase=="infrastructure":
        from infrastructure_release_lib import assemble as renderer
    report=renderer(load(a.inventory),a.config,a.output)
    print("Release assembled; readiness: "+str(report["ready_for_apply"])+"; blockers: "+str(len(report["blocked"])))
    print("Review "+str(a.output/"plan.json")+" before deployment.")

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError)as exc:print("Release preparation stopped: "+str(exc),file=sys.stderr);sys.exit(1)
