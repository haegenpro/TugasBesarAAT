#!/usr/bin/env python3
"""P1.5-1.6: Bring the lost node back with an empty disk while reads and writes continue; afterwards survive another loss.

Runs: scripts/platform-node-replacement.py replace-node
Example:
  python scripts/m2/p1-replace-node.py --inventory /private/existing.json --config /private/replacement.json --node aat-fourhost-c
Use 'plan' first. The operator powers the VM off/on and runs the exported guest steps.
"""
from _forward import forward

forward('platform-node-replacement.py', actions=('plan', 'replace-node', 'resume-node', 'status'), default_action='replace-node', doc=__doc__)
