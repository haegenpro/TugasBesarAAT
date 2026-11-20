#!/usr/bin/env python3
"""P3.6: POST /field-reports answers 202 quickly; receivers crash mid-stream; no accepted report is lost or processed twice.

Runs: scripts/platform-demo-check.py field-reports
Example:
  python scripts/m2/p3-field-reports.py --count 20 --inventory /private/existing.json --client-url URL --field-token-file /private/field-token
"""
from _forward import forward

forward('platform-demo-check.py', ('field-reports',), doc=__doc__)
