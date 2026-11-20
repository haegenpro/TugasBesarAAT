#!/usr/bin/env python3
"""P2.8: Replace the Aggregator version during load without failed requests.

Runs: scripts/platform-game-day.py run --scenario rollout
Example:
  python scripts/m2/p2-rollout.py --image repository@sha256:DIGEST --inventory /private/existing.json --base-url URL --token-file /private/tokens.json --dataset-sha256 SHA
"""
from _forward import forward

forward('platform-game-day.py', actions=('plan', 'run'), default_action='run', defaults=(('--scenario', 'rollout'),), doc=__doc__)
