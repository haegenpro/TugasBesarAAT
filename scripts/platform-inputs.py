#!/usr/bin/env python3
"""Fill supplied external release inputs while preserving private internal credentials."""
import argparse
import os
from pathlib import Path
import sys
from platform_lib.inventory import load
from release_inputs_lib import complete


def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    for name in("inventory","config","inputs","output"):p.add_argument("--"+name,type=Path,required=True)
    a=p.parse_args();report=complete(load(a.inventory),a.config,a.inputs,a.output)
    print("External inputs completed: "+str(report["external_inputs_complete"])+"; pending: "+str(len(report["pending"]))+"; config: "+str(a.output/"release.json"))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError)as exc:print("Input completion stopped: "+str(exc),file=sys.stderr);sys.exit(1)
