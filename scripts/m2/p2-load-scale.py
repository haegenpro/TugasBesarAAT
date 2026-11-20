#!/usr/bin/env python3
"""P2.6: Sustained >=10,000 VU for >=120 s while Aggregator instances change 3 -> 5 -> 2; errors below 1%.

Runs: scripts/platform-game-day.py run --scenario scale
Example:
  python scripts/m2/p2-load-scale.py --inventory /private/existing.json --base-url URL --token-file /private/tokens.json --dataset-sha256 SHA
"""
from _forward import forward

forward('platform-game-day.py', actions=('plan', 'run'), default_action='run', defaults=(('--scenario', 'scale'),), doc=__doc__)
