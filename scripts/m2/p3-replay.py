#!/usr/bin/env python3
"""P3.4: After adapting the system, replay the parked messages: all recover, no duplicates, order per source_ref_id kept.

Runs: scripts/platform-demo-check.py replay
Example:
  python scripts/m2/p3-replay.py --namespace-id <history namespace> --ingestion-url URL --ops-token-file /private/ops-token --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token
Roll out pvmbg_adapter_version 2 before replaying.
"""
from _forward import forward

forward('platform-demo-check.py', ('replay',), required=('--namespace-id',), doc=__doc__)
