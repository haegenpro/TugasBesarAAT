#!/usr/bin/env python3
"""P2.4: After a node clock shift (+45 s, then -30 s) no two instances both stored work for the same period.

Runs: scripts/platform-demo-check.py fencing
Example:
  python scripts/m2/p2-clock-shift.py --since <scenario start UTC> --scope acquisition:PVMBG --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token --receipt /private/maintenance/receipt.json --owner-config /private/inputs/role1.json
Shift the clock with the Tester Agent clock scenario first.
"""
from _forward import forward

forward('platform-demo-check.py', ('fencing',), required=('--since',), doc=__doc__)
