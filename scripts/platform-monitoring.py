#!/usr/bin/env python3
"""Render Kubernetes observability offline, without building images or starting VMs."""
import argparse
import json
from pathlib import Path
import sys
from monitoring_lib import render
from platform_lib.inventory import load
from storage_lib import write_json

ROOT=Path(__file__).resolve().parent.parent

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.example.json")
    parser.add_argument("--storage-plan",type=Path,required=True)
    parser.add_argument("--images",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    resources,requirements=render(load(args.inventory),json.loads(args.storage_plan.read_text()),json.loads(args.images.read_text()))
    args.output.mkdir(parents=True,exist_ok=True)
    write_json(args.output/"resources.json",resources)
    write_json(args.output/"requirements.json",requirements)
    print("Monitoring rendered; runtime and capacity verification remain deferred.")

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError)as exc:print(f"Monitoring render failed: {exc}",file=sys.stderr);sys.exit(1)
