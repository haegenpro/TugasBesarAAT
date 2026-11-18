#!/usr/bin/env python3
"""Plan or explicitly build/publish release images; credentials use Docker login."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from image_release_lib import plan,publish
from storage_lib import require,write_json


def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=["plan","publish"]);p.add_argument("--config",type=Path,required=True);p.add_argument("--output",type=Path)
    a=p.parse_args();config=json.loads(a.config.read_text())
    if a.action=="plan":
        report,_=plan(config)
        if a.output:require(not a.output.exists(),"plan output exists");write_json(a.output,report)
        print(json.dumps(report,indent=2));return
    require(a.output,"publish requires a new --output directory")
    publish(config,a.output);print("Image map ready: "+str(a.output/"images.json"))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError,subprocess.SubprocessError)as exc:
        print("Image preparation stopped: "+str(exc),file=sys.stderr);sys.exit(1)
