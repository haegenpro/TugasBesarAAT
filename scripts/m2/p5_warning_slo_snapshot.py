#!/usr/bin/env python3
"""Critical-warning delivery measurements with a reconciliation against what ingestion acquired.

Criteria: P5-1 (every critical warning delivered, p95 at most 5 s), P5-2 (rejected inputs and
missing receipts stay visible in reconciliation), P5-8 (how much of the reliability target was used).

By default the tool asks the notifier for its report over the whole period ingestion has
data for (since = the first stored sensor input), which is the only window whose coverage can
be complete; --rolling reads the notifier's own gauges instead (one rolling hour, one pod).
It then lists the expected warnings from ingestion and checks that the books close: every
expected warning is delivered, rejected (and visible as such) or still pending. PASS/FAIL
lines are mechanical; NOT MEASURABLE says why the data cannot support a verdict.
Reconciliation against the sensor's own list of critical events remains a separate comparison.
"""
import argparse
import datetime
import math
import sys
import time

from common import LOCAL, read_config, runtime_secrets
from evidence_metrics import PodScrapers, Scraper, is_missing, select
from evidence_report import Report, new_output_dir

P95_LIMIT_SECONDS = 5.0
ORDER = ['expected', 'delivered', 'pending', 'pending_overdue', 'rejected', 'evaluated', 'clock_anomalies', 'receipt_only',
         'coverage_complete', 'p95_complete', 'p95_delivered_seconds', 'p95_lower_bound_seconds', 'on_time_ratio',
         'error_budget_used', 'collection_success', 'collection_age_seconds']
PREFIX = 'app_coastal_slo_'
NUMBERS = ('expected', 'delivered', 'pending', 'pending_overdue', 'rejected', 'evaluated', 'clock_anomalies', 'receipt_only',
           'p95_lower_bound_seconds', 'on_time_ratio', 'error_budget_used', 'on_time', 'conflicts', 'max_seconds')


def extract(metrics):
    """{short name: float or NaN} from a scrape; absent metrics are omitted."""
    return {name[len(PREFIX):]: samples[0][1] for name, samples in select(metrics, PREFIX+r'.*').items()}


def from_report(report):
    """Gauge-style values from the notifier's /warnings/slo JSON; absent numbers become NaN."""
    values = {name: (float('nan') if report.get(name) is None else float(report[name])) for name in NUMBERS}
    values['p95_delivered_seconds'] = float('nan') if report.get('p95_seconds') is None else float(report['p95_seconds'])
    values['error_budget_used'] = float('nan') if report.get('error_budget_used') is None else float(report['error_budget_used'])
    values['coverage_complete'] = 1.0 if report.get('coverage_complete') else 0.0
    values['p95_complete'] = 1.0 if report.get('p95_complete') else 0.0
    values['collection_success'] = 1.0
    values['all_acquired_critical_delivered'] = 1.0 if report.get('all_acquired_critical_delivered') else 0.0
    return values


def show(number):
    return 'not measurable' if is_missing(number) else (f'{number:g}')


def p95_lines(values):
    """P5-1 mechanical lines as (label, 'PASS'|'FAIL'|'NOT MEASURABLE', detail)."""
    complete = values.get('coverage_complete') == 1 and values.get('p95_complete') == 1
    p95 = values.get('p95_delivered_seconds')
    lines = []
    if not complete or is_missing(p95):
        lower = values.get('p95_lower_bound_seconds')
        lines.append(('p95 delivery latency <= 5 s', 'NOT MEASURABLE',
                      f"coverage_complete={show(values.get('coverage_complete'))} p95_complete={show(values.get('p95_complete'))} "
                      f"lower bound={show(lower)}"))
    else:
        lines.append(('p95 delivery latency <= 5 s', 'PASS' if p95 <= P95_LIMIT_SECONDS else 'FAIL', f'p95={p95:.3f}s'))
    overdue = values.get('pending_overdue')
    if values.get('collection_success') != 1 or is_missing(overdue):
        lines.append(('no overdue pending warning', 'NOT MEASURABLE', f"collection_success={show(values.get('collection_success'))}"))
    else:
        lines.append(('no overdue pending warning', 'PASS' if overdue == 0 else 'FAIL', f'pending_overdue={overdue:g}'))
    if 'all_acquired_critical_delivered' in values:
        if values.get('coverage_complete') != 1:
            lines.append(('every acquired critical warning delivered', 'NOT MEASURABLE', 'coverage is not complete for this window'))
        else:
            lines.append(('every acquired critical warning delivered', 'PASS' if values['all_acquired_critical_delivered'] == 1 else 'FAIL',
                          f"expected={show(values.get('expected'))} delivered={show(values.get('delivered'))} rejected={show(values.get('rejected'))} "
                          f"pending={show(values.get('pending'))}"))
    return lines


def books_close(values):
    """P5-2/P5-1: expected = delivered + rejected + pending, with rejected ones visible. (label, state, detail)."""
    needed = ('expected', 'delivered', 'rejected', 'pending')
    if any(is_missing(values.get(k)) for k in needed):
        return ('reconciliation closes', 'NOT MEASURABLE', 'counts missing')
    expected, delivered, rejected, pending = (values[k] for k in needed)
    gap = expected-delivered-rejected-pending
    detail = f'expected={expected:g} delivered={delivered:g} rejected={rejected:g} pending={pending:g} unexplained={gap:g}'
    if expected == 0:
        return ('reconciliation closes', 'NOT MEASURABLE', 'no expected warning in this window; '+detail)
    return ('reconciliation closes (no warning unaccounted for)', 'PASS' if gap == 0 else 'FAIL', detail)


def coverage_and_expected(ingestion, token):
    """(coverage_since, expected items) from ingestion's reconciliation endpoint over the whole coverage period."""
    now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    code, page, _ = ingestion.get_json('/internal/coastal/warnings/expected?since='+now, token)
    if code != 200 or not page or not page.get('coverage_since'):
        return None, []
    since = page['coverage_since']
    items, after, through, as_of = [], None, None, None
    while True:
        query = f'/internal/coastal/warnings/expected?since={since}'
        if after is not None:
            query += f'&after={after}&through={through}&as_of={as_of}'
        code, page, _ = ingestion.get_json(query, token, timeout=20)
        if code != 200 or page is None:
            return since, items
        if after is None:
            through, as_of = page['through'], page['as_of']
        items += page['items']
        if not page['items'] or page['next'] == after:
            return since, items
        after = page['next']
        if after >= through:
            return since, items


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--samples', type=int, default=1)
    parser.add_argument('--interval', type=float, default=30)
    parser.add_argument('--rolling', action='store_true', help="read the notifier's rolling-hour gauges instead of the whole coverage period")
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['coastal_slo']])
    directory = new_output_dir('p5-warning-slo', args.output)
    series, latest, rejected_ids, since = [], {}, [], None
    with Scraper(cfg, 'consumer-notifier', 8092) as notifier, PodScrapers(cfg, 'ingestion', 8080) as ingestion:
        if not args.rolling:
            since, items = coverage_and_expected(ingestion, credentials['coastal_slo'])
            rejected_ids = [i['notification_id'] for i in items if i.get('rejected')]
            report.line(f'window: since {since} (first stored sensor input), {len(items)} expected warning(s) listed by ingestion, '
                        f'{len(rejected_ids)} flagged rejected')
        for index in range(args.samples):
            now = datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S')
            values = None
            if args.rolling or not since:
                metrics = notifier.metrics()
                values = extract(metrics) if metrics is not None else None
            else:
                code, body, _ = notifier.get_json('/warnings/slo?since='+since, timeout=20)
                values = from_report(body) if code == 200 and body else None
            if values is None:
                report.line(f'{now} measurements unavailable')
            else:
                latest = values
                series.append({'at': now, 'values': {k: (None if math.isnan(v) else v) for k, v in latest.items()}})
                report.line(f'sample {index+1}/{args.samples} at {now} UTC ({"rolling gauges, one pod" if args.rolling or not since else "whole coverage period"})')
                names = [n for n in ORDER if n in latest]+sorted(n for n in latest if n not in ORDER)
                report.table(['measurement', 'value'], [[n, show(latest[n])] for n in names])
            if index+1 < args.samples:
                time.sleep(args.interval)
    verdicts = p95_lines(latest) if latest else []
    if latest and not args.rolling:
        verdicts.append(books_close(latest))
        verdicts.append(('rejected inputs stay visible in the reconciliation', 'PASS' if (latest.get('rejected', 0) == len(rejected_ids)) else 'FAIL',
                         f"notifier rejected={show(latest.get('rejected'))}, ingestion lists {len(rejected_ids)} rejected"
                         + ('' if rejected_ids else '; no invalid input was acquired in this window, so only the empty case is shown')))
    report.line()
    for label, state, detail in verdicts:
        report.line(f'{state:15} {label} ({detail})')
    if rejected_ids:
        report.line('rejected notification ids: '+', '.join(rejected_ids[:10]))
    report.line('P5-8: error_budget_used, on_time_ratio and the counts above show how much of the target was consumed; no verdict.')
    report.data.update(since=since, samples=series, rejected_ids=rejected_ids, p5_1_lines=[list(v) for v in verdicts])
    report.save(directory)
    return 1 if any(state == 'FAIL' for _, state, _ in verdicts) else 0


if __name__ == '__main__':
    sys.exit(main())
