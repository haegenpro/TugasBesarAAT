#!/usr/bin/env python3
"""Render the replicated Aggregator deployment; never starts VMs or applies it."""
import argparse
import json
import os
from pathlib import Path
import sys

from aggregator_lib import render
from platform_lib.inventory import load
from storage_lib import write_json

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "deployment/platform/inventory.example.json")
    parser.add_argument("--config", type=Path, default=ROOT / "deployment/aggregator/config.example.json")
    parser.add_argument("--image", required=True, help="actual built image repository@sha256:...")
    parser.add_argument("--replicas", type=int, choices=[2, 3, 5], default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    result = render(load(args.inventory), json.loads(args.config.read_text()), args.image, args.replicas)
    write_json(args.output, result)
    print(f"Rendered {args.replicas} Aggregator replicas ({args.replicas * 256} MiB requests); no infrastructure changed.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, TypeError) as exc:
        print(f"Aggregator render failed: {exc}", file=sys.stderr)
        sys.exit(1)
