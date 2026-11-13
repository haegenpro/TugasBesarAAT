#!/usr/bin/env python3
"""Render application NetworkPolicies offline. Does not apply them."""
import argparse,json
from pathlib import Path
from isolation_lib import render
from platform_lib.inventory import load
from storage_lib import write_json
ROOT=Path(__file__).resolve().parent.parent
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--inventory",type=Path,default=ROOT/"deployment/platform/inventory.example.json")
    p.add_argument("--extra-flows",type=Path);p.add_argument("--output",type=Path,required=True)
    a=p.parse_args();extra=json.loads(a.extra_flows.read_text())if a.extra_flows else[]
    write_json(a.output,render(load(a.inventory),extra))
if __name__=="__main__":main()
