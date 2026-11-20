#!/usr/bin/env python3
"""P2.7: Throughput with 3 instances is at least twice the throughput with 1 instance.

Runs: scripts/platform-game-day.py run --scenario baseline
Example:
  python scripts/m2/p2-throughput.py --baseline-replicas 1 --inventory /private/existing.json --base-url URL --token-file /private/tokens.json --dataset-sha256 SHA
Run once with --baseline-replicas 1 and once with --baseline-replicas 3, then compare.
"""
from _forward import forward

forward('platform-game-day.py', actions=('plan', 'run'), default_action='run', defaults=(('--scenario', 'baseline'),), doc=__doc__)
