#!/usr/bin/env python3
"""Pause, resume, freeze, power off or start one VirtualBox VM on THIS physical host.

Run it on the laptop that hosts the VM (Windows or Linux). It is the operator
side of VM actions for the existing four-laptop cluster: the controller prints
which command to run and then observes the effect through the network and the
Kubernetes API. Every action is recorded with UTC and monotonic timestamps.

    python scripts/platform-vm-control.py freeze --vm "AAT - B" --seconds 90

freeze pauses the VM, waits, and always resumes it (also on Ctrl+C), which is
the P2 "laptop ditutup lalu dibuka lagi" scenario. poweroff is an abrupt power
cut (node mati); start boots the VM again headless.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._()-]{0,79}")


def vboxmanage():
    found = shutil.which("VBoxManage") or shutil.which("vboxmanage")
    if found:
        return found
    for base in (os.environ.get("ProgramFiles", r"C:\Program Files"), os.environ.get("ProgramW6432", r"C:\Program Files")):
        candidate = Path(base) / "Oracle" / "VirtualBox" / "VBoxManage.exe"
        if candidate.is_file():
            return str(candidate)
    raise RuntimeError("VBoxManage not found; install VirtualBox or add it to PATH")


class VM:
    def __init__(self, name, run=subprocess.run, binary=None):
        if not NAME.fullmatch(name):
            raise ValueError("invalid VM name")
        self.name, self.run, self.binary = name, run, binary or vboxmanage()

    def manage(self, *args):
        result = self.run([self.binary, *args], capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise RuntimeError("VBoxManage " + args[0] + " failed: " + (result.stderr or result.stdout).strip()[-400:])
        return result.stdout

    def state(self):
        info = self.manage("showvminfo", self.name, "--machinereadable")
        match = re.search(r'^VMState="([^"]+)"', info, re.M)
        if not match:
            raise RuntimeError("VM state unavailable")
        return match.group(1)


def act(vm, action, seconds, record, sleep=time.sleep):
    def event(name, **fields):
        record["events"].append({"event": name, "utc": datetime.now(timezone.utc).isoformat(), "monotonic_ns": time.monotonic_ns(), "vm_state": vm.state(), **fields})

    before = vm.state()
    record["initial_state"] = before
    if action in ("pause", "freeze", "poweroff"):
        if before != "running":
            raise RuntimeError("VM must be running before " + action + " (state: " + before + ")")
    if action == "resume" and before != "paused":
        raise RuntimeError("VM is not paused (state: " + before + ")")
    if action == "start" and before not in ("poweroff", "aborted", "saved"):
        raise RuntimeError("VM is not powered off (state: " + before + ")")
    if action == "pause":
        vm.manage("controlvm", vm.name, "pause"); event("paused")
    elif action == "resume":
        vm.manage("controlvm", vm.name, "resume"); event("resumed")
    elif action == "poweroff":
        vm.manage("controlvm", vm.name, "poweroff"); event("powered-off")
    elif action == "start":
        vm.manage("startvm", vm.name, "--type", "headless"); event("started")
    else:  # freeze: always resume, even when interrupted
        vm.manage("controlvm", vm.name, "pause"); event("paused", planned_seconds=seconds)
        try:
            sleep(seconds)
        finally:
            vm.manage("controlvm", vm.name, "resume"); event("resumed")
    record["final_state"] = vm.state()
    expected = {"pause": "paused", "resume": "running", "freeze": "running", "poweroff": "poweroff", "start": "running"}[action]
    if record["final_state"] != expected:
        raise RuntimeError("VM ended in state " + record["final_state"] + ", expected " + expected)
    record["passed"] = True
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=["pause", "resume", "freeze", "poweroff", "start"])
    parser.add_argument("--vm", required=True, help='VirtualBox VM name, for example "AAT - B"')
    parser.add_argument("--seconds", type=int, default=90, help="freeze duration (longer than the ownership lease)")
    parser.add_argument("--output", type=Path, help="evidence file (default evidence/platform-vm-control/<run>.json)")
    args = parser.parse_args(argv)
    if not 5 <= args.seconds <= 1800:
        raise ValueError("--seconds must be 5-1800")
    run_id = uuid.uuid4().hex[:12]
    output = args.output or ROOT / "evidence" / "platform-vm-control" / (args.action + "-" + run_id + ".json")
    if output.exists():
        raise ValueError("evidence output exists; keep the original record")
    output.parent.mkdir(parents=True, exist_ok=True)
    record = {"version": 1, "run_id": run_id, "vm": args.vm, "action": args.action, "host": os.environ.get("COMPUTERNAME") or os.uname().nodename,
              "seconds": args.seconds if args.action == "freeze" else None, "events": [], "passed": False}
    try:
        act(VM(args.vm), args.action, args.seconds, record)
    finally:
        output.write_text(json.dumps(record, indent=2) + "\n")
        print(json.dumps({"action": args.action, "vm": args.vm, "passed": record["passed"], "evidence": str(output)}))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
        print("VM control stopped: " + str(exc), file=sys.stderr)
        sys.exit(1)
