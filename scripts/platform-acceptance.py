#!/usr/bin/env python3
"""Plan or run selected real-node acceptance stages from one private input file."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from acceptance_lib import plan,run


def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["plan","run"])
    for name in("config","output"):p.add_argument("--"+name,type=Path,required=True)
    a=p.parse_args()
    def interrupt(signum,frame):raise KeyboardInterrupt("acceptance interrupted")
    signal.signal(signal.SIGTERM,interrupt);signal.signal(signal.SIGHUP,interrupt)
    result=plan(a.config,a.output)[0]if a.action=="plan"else run(a.config,a.output)
    print(json.dumps(result,indent=2))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,RuntimeError,subprocess.SubprocessError)as exc:print("Acceptance stopped: "+str(exc),file=sys.stderr);sys.exit(1)
