#!/usr/bin/env python3
"""Inspect source HTTP intents on captured original primaries; never unlock them."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from source_validation_lib import Observer, dispatch_status
from storage_lib import require


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True, help="existing maintenance receipt with original durability identities")
    args = parser.parse_args()
    inventory = load(args.inventory)
    receipt = json.loads(args.receipt.read_text())
    for suffix in ("storage", "ingestion"):
        formation = receipt["durability"][suffix]
        expected = inventory["cluster_id"] + "-" + suffix
        require(formation["formation"] == expected, "receipt belongs to another formation")
        group = formation["groups"][0]
        require(group["id"] == 0 and re.fullmatch(r"[1-9][0-9]{0,19}", group["system_id"]), "original coordinator identity required")
        require(group["members"] and all(re.fullmatch(re.escape(expected) + r"-g0-[a-d]-0", member["name"]) for member in group["members"]), "unexpected captured member")
    values = dispatch_status(Observer(Cluster(inventory), {}, receipt))
    blocked = [row for rows in values.values() for row in rows if row["blocking"]]
    print(json.dumps({"dispatch": values, "blocked_requests": len(blocked), "read_only": True,
                      "recovery": "The current source owner automatically retries unfinished reads after their grace period. Inspect ownership, retry_after and upstream health if recovery stalls; no force unlock is required."}, indent=2))
    return int(bool(blocked))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print("Acquisition inspection incomplete: " + str(exc), file=sys.stderr)
        sys.exit(1)
