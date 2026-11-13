#!/usr/bin/env python3
"""Render M3/M4 owner artifacts offline; never start/apply/migrate anything."""
import argparse
import json
from pathlib import Path
from role1_integration_lib import render

def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("inventory", "aggregator-config", "config", "aggregator-image", "ingestion-image", "output"):
        p.add_argument("--" + name, required=True)
    a = p.parse_args()
    read = lambda path: json.loads(Path(path).read_text(encoding="utf-8"))
    result = render(read(a.inventory), read(a.aggregator_config), read(a.config), a.aggregator_image, a.ingestion_image)
    output = Path(a.output)
    if output.exists(): raise ValueError("refusing to overwrite prepared artifacts")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("Rendered owner artifacts; bootstrap, offline migrations/cutover and runtime validation remain explicit.")

if __name__ == "__main__": main()
