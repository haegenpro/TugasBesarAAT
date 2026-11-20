#!/usr/bin/env python3
"""P5.6: Run a Tester Agent scenario (exam or practice) and only observe recovery; no intervention during the window.

Runs: scripts/platform-demo-check.py tester --mode exam
Example:
  python scripts/m2/p5-game-day.py --scenario <name> --recovery-seconds 300 --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token -- <exam key arguments>
--mode list shows the scenarios; --mode practice runs a practice scenario.
"""
from _forward import forward

forward('platform-demo-check.py', ('tester',), defaults=(('--mode', 'exam'),), doc=__doc__)
