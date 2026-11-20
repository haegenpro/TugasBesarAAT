#!/usr/bin/env python3
"""Host-local network evidence for the four-laptop Wi-Fi cluster (bonus: real network cut).

Run the same command, with the same --run-id, on every physical laptop for the
same duration. Every second it records this host's Wi-Fi link state and the TCP
round-trip time to every node (kubelet 10250, K3s API 6443 on servers), plus an
optional HTTP probe. On exactly one laptop add --disconnect-after/--disconnect-seconds:
that laptop really disables its Wi-Fi adapter, and ALWAYS re-enables it
afterwards (also on Ctrl+C). Copy each JSONL file to the controller's run
directory and reconcile there with platform-network.py.

    python scripts/platform-host-probe.py --inventory existing.json --run-id RUN \
        --seconds 240 --interface "Wi-Fi" --disconnect-after 60 --disconnect-seconds 30
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import time
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
INTERFACE = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,40}")
RUN = re.compile(r"[a-f0-9]{8,64}")


def tcp_rtt(address, port, timeout=1.0):
    started = time.perf_counter()
    try:
        socket.create_connection((address, port), timeout).close()
        return round((time.perf_counter() - started) * 1000, 3)
    except OSError:
        return None


def http_probe(url, timeout=2.0):
    started = time.perf_counter()
    try:
        with urlopen(Request(url, headers={"User-Agent": "aat-host-probe"}), timeout=timeout) as response:
            return {"status": response.status, "ms": round((time.perf_counter() - started) * 1000, 3)}
    except Exception as exc:  # status only; never record bodies
        return {"status": getattr(exc, "code", None), "ms": None, "error": type(exc).__name__}


class Link:
    """Read and change the Wi-Fi adapter state with the OS's own tools."""
    def __init__(self, interface, run=subprocess.run, system=None):
        if not INTERFACE.fullmatch(interface):
            raise ValueError("invalid interface name")
        self.interface, self.run, self.system = interface, run, system or platform.system()

    def up(self):
        if self.system == "Windows":
            out = self.run(["netsh", "interface", "show", "interface", "name=" + self.interface], capture_output=True, text=True, timeout=10)
            if out.returncode:
                return None
            admin = re.search(r"Administrative state:\s*(\w+)", out.stdout)
            connected = re.search(r"Connect state:\s*(\w+)", out.stdout)
            return bool(admin and connected and admin.group(1).lower() == "enabled" and connected.group(1).lower() == "connected")
        state = Path("/sys/class/net") / self.interface / "operstate"
        return state.read_text().strip() == "up" if state.exists() else None

    def set(self, enabled):
        if self.system == "Windows":
            command = ["netsh", "interface", "set", "interface", "name=" + self.interface, "admin=" + ("enabled" if enabled else "disabled")]
        else:
            command = ["nmcli", "device", "connect" if enabled else "disconnect", self.interface]
        result = self.run(command, capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError("could not " + ("enable" if enabled else "disable") + " " + self.interface + " (run as administrator/sudo): " + (result.stderr or result.stdout).strip()[-300:])


def targets(inventory):
    out = []
    for node in inventory["nodes"]:
        out.append({"node": node["name"], "address": node["address"], "port": 10250})
        if node["role"] == "server":
            out.append({"node": node["name"], "address": node["address"], "port": 6443})
    return out


def probe(args, inventory, link, output, *, sleep=time.sleep, clock=time.monotonic, rtt=tcp_rtt, http=http_probe):
    cut_at = args.disconnect_after
    cut = cut_at is not None
    if cut and not (10 <= cut_at and 5 <= args.disconnect_seconds <= 120 and cut_at + args.disconnect_seconds + 30 <= args.seconds):
        raise ValueError("disconnect window must start >=10s, last 5-120s and leave >=30s of recovery observation")
    host = os.environ.get("COMPUTERNAME") or platform.node()
    disabled = False
    with output.open("x") as stream:
        def emit(kind, **fields):
            stream.write(json.dumps({"type": kind, "run_id": args.run_id, "host": host, "monotonic_ns": time.monotonic_ns(),
                                     "wall_time_ns": time.time_ns(), **fields}) + "\n")
            stream.flush()
        emit("start", seconds=args.seconds, interface=args.interface, disconnect=cut,
             disconnect_after=cut_at, disconnect_seconds=args.disconnect_seconds if cut else None,
             utc=datetime.now(timezone.utc).isoformat(), clock_domain="host-local monotonic; wall time for coarse alignment only")
        started = clock()
        try:
            tick = 0
            while clock() - started < args.seconds:
                elapsed = clock() - started
                if cut and not disabled and elapsed >= cut_at and elapsed < cut_at + args.disconnect_seconds:
                    link.set(False);disabled = True;emit("link-disabled")
                if disabled and elapsed >= cut_at + args.disconnect_seconds:
                    link.set(True);disabled = False;emit("link-enabled")
                sample = {"elapsed_s": round(elapsed, 3), "link_up": link.up(),
                          "peers": [{**t, "rtt_ms": rtt(t["address"], t["port"])} for t in targets(inventory)]}
                if args.http_url:
                    sample["http"] = http(args.http_url)
                emit("sample", **sample)
                tick += 1
                sleep(max(0.0, started + tick - clock()))
        finally:
            if disabled:  # never leave the laptop offline
                link.set(True);emit("link-enabled", reason="cleanup")
            emit("end", utc=datetime.now(timezone.utc).isoformat())
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inventory", type=Path, required=True, help="existing-cluster (v3) inventory with node addresses")
    parser.add_argument("--run-id", required=True, help="the run id printed by platform-network.py run")
    parser.add_argument("--seconds", type=int, default=240)
    parser.add_argument("--interface", default="Wi-Fi" if platform.system() == "Windows" else "wlan0")
    parser.add_argument("--http-url", help="optional application URL to probe, for example the Client API /ready")
    parser.add_argument("--disconnect-after", type=int, help="on the ONE laptop to disconnect: seconds before disabling Wi-Fi")
    parser.add_argument("--disconnect-seconds", type=int, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if not RUN.fullmatch(args.run_id):
        raise ValueError("invalid --run-id")
    if not 60 <= args.seconds <= 900:
        raise ValueError("--seconds must be 60-900")
    if args.http_url and not args.http_url.startswith(("http://", "https://")):
        raise ValueError("--http-url must be http(s)")
    inventory = json.loads(args.inventory.read_text())
    if inventory.get("version") != 3 or len(inventory.get("nodes", [])) != 4:
        raise ValueError("existing-cluster (v3) inventory required")
    host = (os.environ.get("COMPUTERNAME") or platform.node()).lower()
    output = args.output or ROOT / "evidence" / "platform-network" / (args.run_id + "-" + re.sub(r"[^a-z0-9-]", "-", host) + ".jsonl")
    output.parent.mkdir(parents=True, exist_ok=True)
    probe(args, inventory, Link(args.interface), output)
    print(json.dumps({"run_id": args.run_id, "log": str(output), "next": "copy this file into the controller run directory, then reconcile"}))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, KeyError, subprocess.SubprocessError) as exc:
        print("Host probe stopped: " + str(exc), file=sys.stderr)
        sys.exit(1)
