#!/usr/bin/env python3
"""Snapshot of the critical-warning delivery measurements exported by the notifier.

Criteria: P5-1 (every critical warning delivered, p95 at most 5 s), P5-8 (how much of the
reliability target was used).

Scrapes the app_coastal_slo_* gauges from consumer-notifier and prints them (NaN is
shown as "not measurable"). P5-1's two mechanical lines are judged only when the
notifier reports a complete cohort and a valid p95; otherwise they say NOT MEASURABLE.
Error-budget figures are printed for P5-8 without a verdict. The measurement covers
the warnings the system acquired; reconciliation against the sensor's own list of
critical events remains a separate comparison. Use --samples to follow a game day.
"""
import argparse
import datetime
import math
import sys
import time

from evidence_metrics import Scraper, is_missing, select
from evidence_report import Report, new_output_dir
from common import LOCAL, read_config

P95_LIMIT_SECONDS = 5.0
ORDER = ['expected', 'delivered', 'pending', 'pending_overdue', 'rejected', 'evaluated', 'clock_anomalies', 'receipt_only',
         'coverage_complete', 'p95_complete', 'p95_delivered_seconds', 'p95_lower_bound_seconds', 'on_time_ratio',
         'error_budget_used', 'collection_success', 'collection_age_seconds']
PREFIX = 'app_coastal_slo_'


def extract(metrics):
    """{short name: float or NaN} from a scrape; absent metrics are omitted."""
    return {name[len(PREFIX):]: samples[0][1] for name, samples in select(metrics, PREFIX+r'.*').items()}


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
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--samples', type=int, default=1)
    parser.add_argument('--interval', type=float, default=30)
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    report = Report()
    directory = new_output_dir('p5-warning-slo', args.output)
    series, latest = [], {}
    with Scraper(cfg, 'consumer-notifier', 8092) as scraper:
        for index in range(args.samples):
            metrics = scraper.metrics()
            now = datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S')
            if metrics is None:
                report.line(f'{now} metrics unavailable')
            else:
                latest = extract(metrics)
                series.append({'at': now, 'values': {k: (None if math.isnan(v) else v) for k, v in latest.items()}})
                report.line(f'sample {index+1}/{args.samples} at {now} UTC (one notifier pod)')
                names = [n for n in ORDER if n in latest]+sorted(n for n in latest if n not in ORDER)
                report.table(['measurement', 'value'], [[n, show(latest[n])] for n in names])
            if index+1 < args.samples:
                time.sleep(args.interval)
    verdicts = p95_lines(latest) if latest else []
    report.line()
    for label, state, detail in verdicts:
        report.line(f'{state:15} {label} ({detail})')
    report.line('P5-8: error_budget_used, on_time_ratio and the counts above show how much of the target was consumed; no verdict.')
    report.data.update(samples=series, p5_1_lines=[list(v) for v in verdicts])
    report.save(directory)
    return 1 if any(state == 'FAIL' for _, state, _ in verdicts) else 0


if __name__ == '__main__':
    sys.exit(main())
