#!/usr/bin/env python3
"""What the system does with each PVMBG contract: results to compare with the declared policy.

Criteria: P3-1 (declared accept/reject policy versus actual behavior on the PVMBG mock).

For each mode (v1, compatible, breaking) the tool switches the mock, waits, and prints
how many PVMBG HazardEvents were added and how many unprocessed messages appeared by
status (retrying/parked) and retryability, with sample reasons. It also prints the
declared schema policy from Internal Ops. It reaches no verdict: compare the table
with the policy you declared. Switching changes the mock, so it is a dry run unless
--execute is given, and the mock is returned to v1 afterwards.
"""
import argparse
import json
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_audit import AuditClient, pages, remote_checksum
from evidence_report import Report, new_output_dir
from p3_schema_mode import MODES, set_mode


def class_counts(items):
    """{'status/retryable': n} for unprocessed ledger items."""
    counts = {}
    for item in items:
        label = f"{item['status']}/{'retryable' if item['retryable'] else 'not-retryable'}"
        counts[label] = counts.get(label, 0)+1
    return counts


def summarize_mode(before, after):
    """Differences between two observations of the PVMBG source."""
    labels = sorted(set(before['unprocessed'])|set(after['unprocessed']))
    return {'events_added': after['events']-before['events'],
            'unprocessed_added': {l: after['unprocessed'].get(l, 0)-before['unprocessed'].get(l, 0) for l in labels},
            'new_reasons': sorted(set(after['reasons'])-set(before['reasons']))}


def observe(client):
    items = []
    for page in pages(client, '/internal/audit/unprocessed', {'source': 'PVMBG'}, limit=1000):
        items += page['items']
    return {'events': remote_checksum(client, 'PVMBG')['count'], 'unprocessed': class_counts(items),
            'reasons': sorted({i['reason'] for i in items})}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--modes', nargs='+', choices=MODES, default=list(MODES))
    parser.add_argument('--wait', type=float, default=30, help='seconds to let the aggregator poll after each switch')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['internal'], credentials['admin']])
    report.line(f"modes {', '.join(args.modes)}; wait {args.wait:g}s each; the mock is returned to v1 afterwards")
    if not args.execute:
        report.line('Dry run. Add --execute to switch the PVMBG mock and observe.')
        return 0
    directory = new_output_dir('p3-schema-behavior', args.output)
    status, body, _, _ = Session(args.base_url or nodeport(cfg), credentials, 'internal').request('/ops/api/schema-policy')
    report.line(f'declared policy (GET /ops/api/schema-policy, HTTP {status}):')
    report.line(json.dumps(json.loads(body), indent=2)[:2500] if status == 200 else '(unavailable)')
    rows, data = [], {}
    try:
        with AuditClient(cfg) as client:
            for mode in args.modes:
                before = observe(client)
                set_mode(cfg, mode)
                time.sleep(args.wait)
                after = observe(client)
                result = summarize_mode(before, after)
                data[mode] = {'before': before, 'after': after, 'summary': result}
                added = ', '.join(f'{k}: {v:+d}' for k, v in result['unprocessed_added'].items() if v) or 'none'
                rows.append((mode, f"{before['events']} -> {after['events']} ({result['events_added']:+d})", added,
                             '; '.join(r[:80] for r in result['new_reasons'][:3]) or '-'))
    finally:
        if args.modes[-1] != 'v1':
            set_mode(cfg, 'v1')
            report.line('PVMBG mock returned to v1.')
    report.line()
    report.table(['mode', 'PVMBG events', 'new unprocessed (status/retryable)', 'new reasons (sample)'], rows)
    report.line('No verdict: compare each row with the policy declared above and in the report.')
    report.data['modes'] = data
    report.save(directory)
    return 0


if __name__ == '__main__':
    sys.exit(main())
