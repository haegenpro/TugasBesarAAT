#!/usr/bin/env python3
"""P1.4: A node is killed and its data deleted by the Tester Agent; no HazardEvent is lost and requests keep answering.

Runs: scripts/platform-demo-check.py tester --mode practice
Example:
  python scripts/m2/p1-node-loss.py --scenario <node-loss scenario> --recovery-seconds 300 --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token
List scenarios with p5-game-day.py --mode list. Afterwards run scripts/check-storage-recovery.py observe.
"""
from _forward import forward

forward('platform-demo-check.py', ('tester',), defaults=(('--mode', 'practice'),), required=('--scenario',), doc=__doc__)
