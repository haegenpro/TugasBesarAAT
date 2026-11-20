#!/usr/bin/env python3
"""P2.5: Freeze the node running the leader longer than the lease; after waking it stores nothing until its status is refreshed.

Runs: scripts/platform-demo-check.py fencing
Example:
  python scripts/m2/p2-freeze-node.py --freeze-node aat-fourhost-b --freeze-seconds 90 --scope acquisition:PVMBG --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token --receipt /private/maintenance/receipt.json --owner-config /private/inputs/role1.json
When asked, run scripts/platform-vm-control.py pause/resume on that laptop.
"""
from _forward import forward

forward('platform-demo-check.py', ('fencing',), required=('--freeze-node',), doc=__doc__)
