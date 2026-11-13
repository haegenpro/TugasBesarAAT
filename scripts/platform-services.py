#!/usr/bin/env python3
"""Render service-owner integration inputs offline; no builds/deployments."""
import argparse
import json
from pathlib import Path
import sys
from platform_lib.inventory import load
from service_lib import render
from storage_lib import write_json

ROOT=Path(__file__).resolve().parent.parent

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.example.json")
    p.add_argument("--catalog",type=Path,default=ROOT/"deployment/services/catalog.json")
    p.add_argument("--images",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
    resources,report=render(load(a.inventory),json.loads(a.catalog.read_text()),json.loads(a.images.read_text()))
    write_json(a.output/"resources.json",resources);write_json(a.output/"requirements.json",report)
    print("Service resources rendered without deployment; owner-provided dependencies remain explicit.")

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError)as e:print(f"Service render failed: {e}",file=sys.stderr);sys.exit(1)
