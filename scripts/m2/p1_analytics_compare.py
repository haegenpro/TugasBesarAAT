#!/usr/bin/env python3
"""Compare the analytics dashboard's numbers with a direct count from the Audit API.

Criteria: P1-7 (analytics numbers equal the Audit API count; incomplete data is flagged).

Reads /analytics/summary through the Client API as Internal Ops, scans every source
through the Audit API, counts the same active events by hazard type, severity and
area over the same occurred_at window, and prints both columns with their
difference. The page itself (charts, the "incomplete" banner) must still be shown
in a browser. Run it again with one node stopped to see the partial flags.
"""
import argparse
import datetime
import json
import sys
import urllib.parse

from acceptance_support import Session, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_audit import AuditClient, SOURCES, parse_time, scan_source
from evidence_report import Report, new_output_dir

FIELDS = ('hazard_type', 'severity', 'area_name', 'occurred_at')


def count_by(items, field):
    counts = {}
    for item in items:
        counts[item[field]] = counts.get(item[field], 0)+1
    return counts


def in_window(items, start, end):
    """Events whose occurred_at is inside [start, end), as the analytics query filters."""
    return [i for i in items if start <= parse_time(i['occurred_at']) < end]


def compare_counts(analytics, audit):
    """Rows (key, analytics, audit, difference) over the union of keys, and whether all match."""
    rows = []
    for key in sorted(set(analytics)|set(audit)):
        a, b = analytics.get(key, 0), audit.get(key, 0)
        rows.append((key, a, b, a-b))
    return rows, all(row[3] == 0 for row in rows)


def stamp(moment):
    return moment.strftime('%Y-%m-%dT%H:%M:%SZ')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--audit-url', help='Aggregator URL; defaults to a port-forward')
    parser.add_argument('--days', type=int, default=365, help='window length (analytics allows at most 366)')
    parser.add_argument('--areas', type=int, default=15, help='area rows to show besides any that differ')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    if not 1 <= args.days <= 366:
        parser.error('days must be 1..366')
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['internal']])
    directory = new_output_dir('p1-analytics-compare', args.output)
    now = datetime.datetime.now(datetime.timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)+datetime.timedelta(days=1)
    start = end-datetime.timedelta(days=args.days)
    query = urllib.parse.urlencode({'from': stamp(start), 'to': stamp(end), 'interval': 'day'})
    session = Session(args.base_url or nodeport(cfg), credentials, 'internal')
    status, body, _, seconds = session.request('/analytics/summary?'+query)
    report.line(f'GET /analytics/summary window {stamp(start)} .. {stamp(end)}: HTTP {status} in {seconds*1000:.0f} ms')
    if status != 200:
        report.data['analytics_http_status'] = status
        report.save(directory)
        return 1
    analytics = json.loads(body)
    scanned_at = datetime.datetime.now(datetime.timezone.utc)
    items, partial, unavailable = [], False, set()
    with AuditClient(cfg, base_url=args.audit_url) as client:
        for source in SOURCES:
            scanned = scan_source(client, source, fields=FIELDS)
            items += scanned['items']
            partial |= scanned['partial']
            unavailable.update(scanned['unavailable_units'])
    inside = in_window(items, start, end)
    report.line(f"analytics: as_of={analytics['as_of']} mode={analytics['mode']} generation={analytics['generation']} "
                f"data_complete={analytics['data_complete']} partial={analytics['partial']} "
                f"unavailable_units={analytics['unavailable_units']}")
    report.line(f"source_freshness: {json.dumps(analytics.get('source_freshness'))}")
    report.line(f"audit scan finished {stamp(scanned_at)}: {len(items)} active events, {len(inside)} inside the window, "
                f"partial={partial} unavailable_units={sorted(unavailable)}")
    report.line('Counts include only active events with occurred_at in the window. New events arriving between the two '
                'reads (compare as_of with the scan time) show up as differences; pause acquisition for an exact comparison.')
    all_match = analytics['total'] == len(inside)
    groups = {}
    for title, a_map, field in (('by hazard type', analytics['by_type'], 'hazard_type'),
                                ('by severity', analytics['by_severity'], 'severity'),
                                ('by area', {a['area_name']: a['count'] for a in analytics['by_area']}, 'area_name')):
        rows, matches = compare_counts(a_map, count_by(inside, field))
        all_match &= matches
        shown = rows
        if title == 'by area':
            ranked = sorted(rows, key=lambda r: -max(r[1], r[2]))[:args.areas]
            shown = sorted({r[0]: r for r in ranked+[r for r in rows if r[3]]}.values(), key=lambda r: -max(r[1], r[2]))
        report.line()
        report.line(f'{title}:')
        report.table(['key', 'dashboard', 'audit', 'difference'], shown)
        groups[title] = rows
    report.line()
    report.line(f"total: dashboard={analytics['total']} audit={len(inside)} difference={analytics['total']-len(inside)}")
    report.line('NUMBERS MATCH' if all_match else 'DIFFERENCES FOUND (inspect as_of, partial flags and ingestion during the scan)')
    report.data.update(window={'from': stamp(start), 'to': stamp(end)}, analytics=analytics,
                       audit_total_in_window=len(inside), audit_total=len(items), comparisons=groups,
                       audit_partial=partial, all_match=all_match)
    report.save(directory)
    return 0 if all_match else 1


if __name__ == '__main__':
    sys.exit(main())
