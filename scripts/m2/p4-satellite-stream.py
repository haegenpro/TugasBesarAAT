#!/usr/bin/env python3
"""P4.1-4.4, 4.6, 4.8: Satellite feed: 60x burst catch-up, reconnect without loss/duplicates, comparison with the reference.

Runs: scripts/platform-source-validation.py run --scenario satellite-burst
Example:
  python scripts/m2/p4-satellite-stream.py --config /private/source-validation.json
Uses the private source-validation configuration and the assistant's reference data.
"""
from _forward import forward

forward('platform-source-validation.py', actions=('plan', 'run', 'analyze'), default_action='run', defaults=(('--scenario', 'satellite-burst'),), doc=__doc__)
