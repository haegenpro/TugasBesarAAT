#!/usr/bin/env python3
"""What the system does with each PVMBG contract under each reader version, judged against the declared design.

Criteria: P3-1 (declared accept/reject policy versus actual behavior on the PVMBG mock).

Scenarios, run in this order with --execute (the tool changes the mock and the reader policy):
  1. reader v1, mock v1          compatible baseline: new events, nothing quarantined
  2. reader v1, mock compatible  extra writer fields are tolerated: new events, nothing quarantined
  3. reader v1, mock breaking    the incompatible contract is quarantined and not retryable
  4. replay                      the approved reader v2 is activated and the retained messages replayed
  5. reader v2, mock breaking    the approved reader now accepts the breaking contract
The mock is returned to v1 at the end. Each scenario prints the events added and the unprocessed
messages added by status/retryability, then a PASS/FAIL line for the declared expectation.
Scenario 3 only fails when polling is not blocked by messages still retained from an earlier
run, so the tool refuses to start while unresolved PVMBG messages exist (use --allow-retained).
"""
import argparse
import json
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_audit import AuditClient, pages, remote_checksum
from evidence_report import Report, new_output_dir
from p3_schema_mode import set_mode

SCENARIOS = (('reader v1, mock v1', 1, 'v1'), ('reader v1, mock compatible', 1, 'compatible'),
             ('reader v1, mock breaking', 1, 'breaking'), ('reader v2, mock breaking', 2, 'breaking'))


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


def judge(reader, mode, summary):
    """(expectation, passed) for one scenario from its summary; the declared design is in the module docstring."""
    added = sum(max(0, v) for v in summary['unprocessed_added'].values())
    parked = sum(max(0, v) for k, v in summary['unprocessed_added'].items() if k.startswith('parked/'))
    if reader == 1 and mode == 'breaking':
        return ('the breaking contract is quarantined as parked and not retryable',
                parked >= 1 and all(k.endswith('/not-retryable') for k, v in summary['unprocessed_added'].items() if v > 0))
    subject = 'extra writer fields are' if mode == 'compatible' else 'the contract is'
    return (f'{subject} accepted: new events and nothing quarantined', summary['events_added'] > 0 and added == 0)


def observe(client):
    items = []
    for page in pages(client, '/internal/audit/unprocessed', {'source': 'PVMBG'}, limit=1000):
        items += page['items']
    return {'events': remote_checksum(client, 'PVMBG')['count'], 'unprocessed': class_counts(items),
            'reasons': sorted({i['reason'] for i in items}), 'ids': [i['message_id'] for i in items if i['status'] != 'resolved']}


def parse(body):
    try:
        return json.loads(body)
    except ValueError:
        return {}


def adopt_reader(session, version, wait=40):
    """PUT the reader version and wait until the Aggregator reports it as adopted."""
    status, body, _, _ = session.request('/ops/api/schema-policy', {'version': version}, method='PUT')
    deadline = time.monotonic()+wait
    while status in (200, 202) and time.monotonic() < deadline:
        code, text, _, _ = session.request('/ops/api/schema-policy')
        if code == 200 and parse(text).get('local_version') == version:
            return True
        time.sleep(2)
    return False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--wait', type=float, default=30, help='seconds to let the aggregator poll after each switch')
    parser.add_argument('--allow-retained', action='store_true', help='start although unresolved PVMBG messages are retained')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['internal'], credentials['admin']])
    report.line(f"{len(SCENARIOS)} scenarios, {args.wait:g}s of polling each; the mock is returned to v1 afterwards")
    if not args.execute:
        report.line('Dry run. Add --execute to switch the PVMBG mock and the reader version and observe.')
        return 0
    directory = new_output_dir('p3-schema-behavior', args.output)
    session = Session(args.base_url or nodeport(cfg), credentials, 'internal')
    status, body, _, _ = session.request('/ops/api/schema-policy')
    report.line(f'declared policy before (GET /ops/api/schema-policy, HTTP {status}): {json.dumps(parse(body))}')
    rows, data, verdicts = [], {}, []
    try:
        with AuditClient(cfg) as client:
            start = observe(client)
            if start['ids'] and not args.allow_retained:
                report.line(f"FAIL {len(start['ids'])} unresolved PVMBG message(s) are retained from an earlier run and block polling; "
                            'replay them (p3_replay_flow.py) or pass --allow-retained')
                return 1
            if not adopt_reader(session, 1):
                report.line('FAIL reader version 1 was not adopted')
                return 1
            for index, (label, reader, mode) in enumerate(SCENARIOS):
                if index == 3:
                    # Step 4: activate the approved reader v2, replay what was retained, let polling resume.
                    waiting = observe(client)['ids']
                    adopted = adopt_reader(session, 2)
                    code, text, _, _ = session.request('/ops/api/replays', {'message_ids': waiting[:1000]}, method='POST') if waiting else (0, b'{}', None, 0)
                    selected = parse(text).get('selected', 0)
                    report.line(f'replay: reader v2 adopted={adopted}, HTTP {code}, selected {selected} of {len(waiting)} retained message(s)')
                    verdicts.append(('reader version 2 is adopted and the retained messages are replayed',
                                     adopted and (not waiting or (code == 202 and selected == len(waiting)))))
                    time.sleep(args.wait)
                    set_mode(cfg, 'v1')
                before = observe(client)
                set_mode(cfg, mode)
                time.sleep(args.wait)
                after = observe(client)
                result = summarize_mode(before, after)
                expectation, passed = judge(reader, mode, result)
                data[label] = {'before': before, 'after': after, 'summary': result, 'expectation': expectation, 'passed': passed}
                verdicts.append((f'{label}: {expectation}', passed))
                added = ', '.join(f'{k}: {v:+d}' for k, v in result['unprocessed_added'].items() if v) or 'none'
                rows.append((label, f"{before['events']} -> {after['events']} ({result['events_added']:+d})", added,
                             '; '.join(r[:80] for r in result['new_reasons'][:2]) or '-'))
    finally:
        set_mode(cfg, 'v1')
        report.line('PVMBG mock returned to v1; the reader is left on the last adopted version.')
    report.line()
    report.table(['scenario', 'PVMBG events', 'new unprocessed (status/retryable)', 'new reasons (sample)'], rows)
    report.line()
    for name, ok in verdicts:
        report.line(('PASS ' if ok else 'FAIL ')+name)
    report.data.update(scenarios=data, checks=dict(verdicts))
    report.save(directory)
    return 0 if all(ok for _, ok in verdicts) else 1


if __name__ == '__main__':
    sys.exit(main())
