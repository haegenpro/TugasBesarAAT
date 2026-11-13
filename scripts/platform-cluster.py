#!/usr/bin/env python3
"""Provision the four-node QEMU/k3s bootstrap with an explicit inventory.

doctor/render/status are read-only with respect to VM/network state. up creates
run-owned VMs/network/API proxy and installs k3s plus the supplied Tester Agent.
stop requests guest shutdown and preserves disks, definitions and network.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from platform_lib.cluster import Cluster
from platform_lib.inventory import load, render

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "deployment/platform/inventory.example.json")
    parser.add_argument("command", choices=["doctor", "render", "up", "bootstrap", "install-tester", "verify", "verify-failover", "status", "stop", "resize"])
    parser.add_argument("--output", type=Path, help="render destination")
    parser.add_argument("--public-key-file", type=Path, help="ed25519 public key for preview rendering")
    parser.add_argument("--previous-inventory", type=Path, help="exact prior inventory for a stopped CPU/RAM-only resize")
    args = parser.parse_args()
    try:
        cfg = load(args.inventory)
        cluster = Cluster(cfg)
        if args.command == "render":
            if not args.output or not args.public_key_file:
                parser.error("render requires --output and --public-key-file")
            render(cfg, args.output, args.public_key_file.read_text().strip())
            print(f"Rendered review artifacts to {args.output}; no VMs or network changed.")
        elif args.command == "doctor":
            report = cluster.doctor()
            print(json.dumps(report, indent=2))
            return 0 if report["ready"] else 1
        elif args.command == "status":
            print(cluster.virsh("list", "--all").stdout.decode(), end="")
        else:
            if args.command == "resize" and not args.previous_inventory:
                parser.error("resize requires --previous-inventory")
            with cluster.locked(allow_resize=args.command == "resize"):
                if args.command == "resize":
                    cluster.resize(load(args.previous_inventory))
                    return 0
                {"up": cluster.up, "bootstrap": cluster.bootstrap, "install-tester": cluster.install_tester,
                 "verify": cluster.verify, "verify-failover": cluster.verify_failover, "stop": cluster.stop}[args.command]()
        return 0
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"Platform operation failed: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Platform operation interrupted; owned state/data are retained for inspection and retry.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
