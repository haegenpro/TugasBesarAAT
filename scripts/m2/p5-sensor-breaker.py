#!/usr/bin/env python3
"""P5.4: While Sensor Pantai is unhealthy, requests to it do not surge and dependent reads answer fast.

Runs: scripts/platform-demo-check.py coastal-breaker
Example:
  python scripts/m2/p5-sensor-breaker.py --path "/hazards?source=SENSOR_PANTAI" --prometheus-url URL --rate-query "<PromQL>" --inventory /private/existing.json --client-url URL --field-token-file /private/field-token
"""
from _forward import forward

forward('platform-demo-check.py', ('coastal-breaker',), doc=__doc__)
