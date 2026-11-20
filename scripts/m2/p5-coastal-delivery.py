#!/usr/bin/env python3
"""P5.1: Every critical coastal event reaches Tim Lapangan: 0 lost, p95 <= 5 s.

Runs: scripts/platform-source-validation.py run --scenario reconcile
Example:
  python scripts/m2/p5-coastal-delivery.py --config /private/source-validation.json
Reconciles the source reference with the recipient inbox.
"""
from _forward import forward

forward('platform-source-validation.py', actions=('plan', 'run', 'analyze'), default_action='run', defaults=(('--scenario', 'reconcile'),), doc=__doc__)
