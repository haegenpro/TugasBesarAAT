#!/usr/bin/env python3
"""P4.5: Crash the wildfire processor while a fire is being recognized; the final result equals the undisturbed reference.

Runs: scripts/platform-demo-check.py processor-crash
Example:
  python scripts/m2/p4-processor-crash.py --reference-file /private/satellite-reference.json --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token --receipt /private/maintenance/receipt.json --owner-config /private/inputs/role1.json
"""
from _forward import forward

forward('platform-demo-check.py', ('processor-crash',), doc=__doc__)
