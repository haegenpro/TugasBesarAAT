#!/usr/bin/env python3
"""P3.5: Failed messages are readable only by BNPB Internal Ops.

Runs: scripts/platform-demo-check.py ops-only
Example:
  python scripts/m2/p3-ops-only.py --ingestion-url URL --ops-token-file /private/ops-token --media-token-file /private/media-token --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token
"""
from _forward import forward

forward('platform-demo-check.py', ('ops-only',), doc=__doc__)
