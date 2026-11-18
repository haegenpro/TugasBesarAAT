#!/usr/bin/env python3
"""Plan, execute or reanalyze source/recipient reference checks on existing nodes."""
import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from source_validation_lib import analyze,load_inputs,run
from storage_lib import require


def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["plan","run","analyze"])
    for name in("inventory","config"):p.add_argument("--"+name,type=Path,required=True)
    p.add_argument("--scenario",choices=["reconcile","satellite-burst"],default="reconcile")
    p.add_argument("--output",type=Path);p.add_argument("--evidence",type=Path)
    a=p.parse_args();inventory=load(a.inventory);values=load_inputs(inventory,a.config)
    if a.action=="plan":print(json.dumps({"scenario":a.scenario,"reference_sha256":values[-1],"satellite_operations":len(values[-2]["satellite"]["records"]),"critical_operations":len(values[-2]["coastal"]["critical_notification_ids"]),"mutations":["UID-guarded ingestion stop/restore"]if a.scenario=="satellite-burst"else [],"executed":False},indent=2));return
    if a.action=="analyze":
        require(a.evidence,"analyze requires --evidence directory")
        result=analyze(values[-2],json.loads((a.evidence/"observation.json").read_text()),values[0].get("max_clock_uncertainty_ms",500))
    else:
        require(a.output,"run requires a new --output directory")
        def interrupt(signum,frame):raise KeyboardInterrupt("source validation interrupted")
        signal.signal(signal.SIGTERM,interrupt);signal.signal(signal.SIGHUP,interrupt)
        cluster=Cluster(inventory)
        with cluster.locked()if a.scenario=="satellite-burst"else nullcontext():result=run(cluster,a.config,a.output,a.scenario)
    print(json.dumps(result,indent=2));return int(not result["passed"])


if __name__=="__main__":
    try:sys.exit(main())
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:print("Source validation incomplete: "+str(exc),file=sys.stderr);sys.exit(1)
