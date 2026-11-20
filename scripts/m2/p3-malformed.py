#!/usr/bin/env python3
"""P3.3: Truncated, deeply nested, invalid UTF-8 and oversized messages are refused without any crash or restart.

Runs: scripts/platform-demo-check.py malformed
Example:
  python scripts/m2/p3-malformed.py --inventory /private/existing.json --client-url URL --field-token-file /private/field-token
"""
from _forward import forward

forward('platform-demo-check.py', ('malformed',), doc=__doc__)
