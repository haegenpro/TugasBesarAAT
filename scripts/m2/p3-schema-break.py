#!/usr/bin/env python3
"""P3.1-3.2: An incompatible PVMBG schema change is parked intact with reason, source and time, not guessed.

Runs: scripts/platform-demo-check.py schema --mode rename
Example:
  python scripts/m2/p3-schema-break.py --pvmbg-url URL --pvmbg-admin-token-file /private/pvmbg-admin-token --inventory /private/existing.json --audit-url URL --audit-token-file /private/audit-token
Other incompatible modes: --mode type_change, --mode missing_required.
"""
from _forward import forward

forward('platform-demo-check.py', ('schema',), defaults=(('--mode', 'rename'),), doc=__doc__)
