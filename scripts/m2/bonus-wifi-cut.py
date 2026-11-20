#!/usr/bin/env python3
"""Bonus: Real network cut between physical hosts: Wi-Fi disabled and restored on one laptop, latency measured on the real network.

Runs: scripts/platform-network.py run
Example:
  python scripts/m2/bonus-wifi-cut.py --inventory /private/existing.json --output /private/network-run
Run scripts/platform-host-probe.py on every laptop (one with --disconnect-after), copy logs, then 'reconcile'.
"""
from _forward import forward

forward('platform-network.py', actions=('plan', 'run', 'reconcile'), default_action='run', doc=__doc__)
