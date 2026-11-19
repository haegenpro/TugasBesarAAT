#!/usr/bin/env python3
"""Capture an existing cluster inventory, verify it, or export guest volume scripts."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from existing_volumes_lib import export
from platform_lib.existing import ExistingCluster, from_nodes
from platform_lib.inventory import load, require


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    capture = commands.add_parser("inventory")
    for key in ("nodes-file", "kubeconfig", "state-dir", "output"):
        capture.add_argument("--" + key, type=Path, required=True)
    capture.add_argument("--cluster-id", default="aat-fourhost")
    capture.add_argument("--k3s-binary", type=Path, help="local k3s binary to use for kubectl; otherwise use kubectl on PATH")
    check = commands.add_parser("check")
    check.add_argument("--inventory", type=Path, required=True)
    volumes = commands.add_parser("volumes")
    for key in ("inventory", "bundle", "output"):
        volumes.add_argument("--" + key, type=Path, required=True)
    volumes.add_argument("--allow-disk-overcommit", action="store_true", help="demo: require only 2 GiB free per guest instead of backing all declared PV capacities; retain ownership checks")
    args = parser.parse_args()
    if args.action == "inventory":
        require(not args.output.exists(), "inventory output exists; preserve original identities")
        command = [str(args.k3s_binary.resolve()), "kubectl"] if args.k3s_binary else ["kubectl"]
        cfg = from_nodes(json.loads(args.nodes_file.read_text()), cluster_id=args.cluster_id,
                         kubeconfig=args.kubeconfig, kubectl_command=command, state_dir=args.state_dir)
        args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with args.output.open("x") as stream:
            stream.write(json.dumps(cfg, indent=2) + "\n")
        print("Existing-cluster inventory written: " + str(args.output))
    elif args.action == "check":
        print(json.dumps(ExistingCluster(load(args.inventory)).verify(), indent=2))
    else:
        result = export(load(args.inventory), args.bundle, args.output, allow_disk_overcommit=args.allow_disk_overcommit)
        print("Exported " + str(len(result["volumes"])) + " local volumes into four guest scripts at " + str(args.output))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, TypeError, RuntimeError, subprocess.SubprocessError) as exc:
        print("Existing-cluster operation stopped: " + str(exc), file=sys.stderr)
        sys.exit(1)
