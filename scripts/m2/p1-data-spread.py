#!/usr/bin/env python3
"""P1.1-1.3: No node holds every HazardEvent; load is spread; cross-location reads are ordered and checksums match.

Runs: scripts/platform-demo-check.py distribution
Example:
  python scripts/m2/p1-data-spread.py --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token --client-url URL --field-token-file /private/field-token
Run it again while a regional burst or load is running to record per-node load (P1.2).
"""
from _forward import forward

forward('platform-demo-check.py', ('distribution',), doc=__doc__)
