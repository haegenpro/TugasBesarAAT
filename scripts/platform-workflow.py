#!/usr/bin/env python3
"""Plan or explicitly run first-deployment phases from one private workflow file."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from workflow_lib import PHASES,plan,run


def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["plan","run"]);p.add_argument("--config",type=Path,required=True);p.add_argument("--phase",choices=["all",*PHASES],default="all")
    a=p.parse_args()
    if a.action=="plan":print(json.dumps(plan(a.config,a.phase)[0],indent=2));return
    def interrupt(signum,frame):raise KeyboardInterrupt("workflow interrupted; inspect its durable journal")
    signal.signal(signal.SIGTERM,interrupt);signal.signal(signal.SIGHUP,interrupt)
    run(a.config,a.phase)


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:print("Workflow stopped: "+str(exc),file=sys.stderr);sys.exit(1)
