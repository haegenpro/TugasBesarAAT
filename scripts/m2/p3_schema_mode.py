#!/usr/bin/env python3
"""Select the PVMBG source contract for a controlled P3 demonstration.

Criteria: P3-1 (incompatible PVMBG contract); prerequisite for P3-2 and P3-4.
"""
import argparse
import json
import urllib.error
import urllib.request

from common import LOCAL, read_config, runtime_secrets
from evidence_integration_tests import forward


MODES = ('v1', 'compatible', 'breaking')


def set_mode(cfg, mode):
    """Switch the PVMBG mock contract and return its answer (also used by p3_schema_behavior)."""
    credentials = runtime_secrets(LOCAL/'secrets.json')
    with forward(cfg, 'pvmbg-mock', 8082) as port:
        request = urllib.request.Request(
            f'http://127.0.0.1:{port}/admin/schema-version',
            data=json.dumps({'mode': mode}).encode(), method='POST',
            headers={'Authorization': 'Bearer '+credentials['admin'],
                     'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('mode', choices=MODES)
    args = parser.parse_args()
    cfg = read_config(args.config)
    result = set_mode(cfg, args.mode)
    print('PVMBG source schema: '+str(result['schema_version']))
    if args.mode == 'breaking':
        print('The source renames alert_level to level, sends the count as text, and omits ash height.')
        print('Use Internal Ops to inspect retained inputs, activate reader version 2, then replay.')


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, urllib.error.HTTPError):
        raise SystemExit('Schema selection failed; inspect the chosen cluster and mock readiness. Credentials were not printed.')
