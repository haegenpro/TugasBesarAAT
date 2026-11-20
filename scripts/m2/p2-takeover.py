#!/usr/bin/env python3
"""P2.2-2.3: Crash the acquisition lease holder; another instance resumes within 60 s with no duplicate and no double upstream request.

Runs: scripts/platform-demo-check.py takeover
Example:
  python scripts/m2/p2-takeover.py --scope acquisition:PVMBG --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token --receipt /private/maintenance/receipt.json --owner-config /private/inputs/role1.json
Use --scope acquisition:BMKG for BMKG (ingestion instances).
"""
from _forward import forward

forward('platform-demo-check.py', ('takeover',), doc=__doc__)
