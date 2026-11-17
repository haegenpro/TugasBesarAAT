#!/usr/bin/env python3
"""Generate a private integrated release scaffold without contacting a cluster."""
import argparse
from pathlib import Path
import sys
from platform_lib.inventory import load
from release_config_lib import prepare


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventory",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    for name in ("satellite-token-file","audit-token-file","province-reference","images-file","external-flows-file"):
        p.add_argument("--"+name,type=Path)
    a=p.parse_args()
    report=prepare(load(a.inventory),a.output,**{k:v for k,v in vars(a).items()if k not in("inventory","output")})
    print("Private release inputs prepared at "+str(a.output)+"; pending items: "+str(len(report["pending"])))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError)as exc:
        print("Configuration preparation stopped: "+str(exc),file=sys.stderr);sys.exit(1)
