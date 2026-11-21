#!/usr/bin/env python3
"""Watch source progress: received/prepared/applied offsets, lag, watermark and late data.

Criteria: P4-2 (late-arrival counter), P4-4 (lag during and after a 60x burst), P4-7 (live
feed), and as supporting evidence for P4-8 (positions continue after a reconnect).

Polls GET /ops/api/progress (Internal Ops) and prints, per source and namespace, the
original feed offsets received, prepared and applied, the lag between them, the
event-time watermark against the virtual clock, and the counters for singletons,
excluded detections and late mutations, with rates. It only reports; compare the
timeline with your declared lateness bound and the 60-second catch-up target.
"""
import argparse
import datetime
import json
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_audit import parse_time
from evidence_metrics import Scraper, value
from evidence_report import Report, format_table, new_output_dir

COLUMNS = ['t(s)', 'source/namespace', 'received', 'prepared', 'applied', 'lag', 'recv/s', 'applied/s', 'virtual_now',
           'watermark age(s)', 'late', 'singletons', 'excluded', 'hist.pending']


def age_seconds(virtual_now, watermark):
    if not virtual_now or not watermark:
        return None
    return (parse_time(virtual_now)-parse_time(watermark)).total_seconds()


def sample_row(elapsed, item, previous, seconds):
    """One printable/serialisable row from a progress item and the previous sample of the same key."""
    received_rate = applied_rate = None
    if previous is not None and seconds > 0:
        received_rate = (item['received']-previous['received'])/seconds
        applied_rate = (item['applied']-previous['applied'])/seconds
    return {'t': round(elapsed, 1), 'key': f"{item['source']}/{item['namespace']}", 'received': item['received'],
            'prepared': item['prepared'], 'applied': item['applied'], 'lag': item['received']-item['applied'],
            'received_per_s': received_rate, 'applied_per_s': applied_rate, 'virtual_now': item.get('virtual_now'),
            'watermark_age_s': age_seconds(item.get('virtual_now'), item.get('watermark')),
            'late_mutations': item['late_mutations'], 'singletons': item['singletons'], 'excluded': item['excluded'],
            'historical_pending': item['historical_pending']}


def summarize(rows):
    """Per key: peak lag, when it happened, whether and when lag returned to zero afterwards."""
    summary = {}
    for key in sorted({r['key'] for r in rows}):
        series = [r for r in rows if r['key'] == key]
        peak = max(series, key=lambda r: r['lag'])
        back = next((r for r in series if r['t'] > peak['t'] and r['lag'] <= 0), None) if peak['lag'] > 0 else None
        summary[key] = {'samples': len(series), 'peak_lag': peak['lag'], 'peak_at_s': peak['t'], 'final_lag': series[-1]['lag'],
                        'seconds_peak_to_zero': (back['t']-peak['t']) if back else None,
                        'received_delta': series[-1]['received']-series[0]['received'],
                        'applied_delta': series[-1]['applied']-series[0]['applied'],
                        'late_mutations_delta': series[-1]['late_mutations']-series[0]['late_mutations']}
    return summary


def lag_seconds(rows, window=6):
    """[(t, lag in seconds of feed time, inflow per second)].

    The feed rate is averaged from the start of the run up to each sample, so a stall of the
    acquisition itself (which collapses the instantaneous rate) cannot inflate the backlog
    figure; `window` is the number of samples needed before the first judgement."""
    result = []
    for index in range(window, len(rows)):
        first, last = rows[0], rows[index]
        span = last['t']-first['t']
        inflow = (last['received']-first['received'])/span if span > 0 else 0
        if inflow > 0:
            result.append((last['t'], max(0, last['lag'])/inflow, inflow))
    return result


def catchup_verdict(rows, limit=60, min_inflow=5):
    """(state, detail) for 'the backlog never exceeds `limit` seconds of feed'; not measurable without a live feed."""
    points = [p for p in lag_seconds(rows) if p[2] >= min_inflow]
    if not points:
        return ('NOT MEASURABLE', f'the feed delivered less than {min_inflow} inputs per second, so lag cannot be expressed in feed seconds')
    worst = max(points, key=lambda p: p[1])
    detail = f'worst backlog {worst[1]:.0f} s of feed at +{worst[0]:.0f} s (inflow {worst[2]:.0f}/s); final {points[-1][1]:.0f} s'
    return ('PASS' if worst[1] <= limit else 'FAIL', detail)


def cell(number, digits=1):
    return '-' if number is None else f'{number:.{digits}f}'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--minutes', type=float, default=5)
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--only', help='show only keys containing this text (case-insensitive)')
    parser.add_argument('--metrics', action='store_true', help='also scrape satellite gauges from one ingestion pod')
    parser.add_argument('--lag-limit-seconds', type=float, default=0,
                        help='judge that the backlog never exceeds this many seconds of feed (60 for P4-4); 0 gives no verdict')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['internal']])
    directory = new_output_dir('p4-ingestion-progress', args.output)
    session = Session(args.base_url or nodeport(cfg), credentials, 'internal')
    scraper = Scraper(cfg, 'ingestion', 8080).__enter__() if args.metrics else None
    started = time.monotonic()
    previous, rows, metric_rows = {}, [], []
    report.line(f'Polling /ops/api/progress every {args.interval:g}s for {args.minutes:g} minute(s).')
    try:
        header_done = False
        while True:
            tick = time.monotonic()
            status, body, _, _ = session.request('/ops/api/progress')
            elapsed = tick-started
            if status != 200:
                report.line(f'+{elapsed:.0f}s HTTP {status} (progress unavailable)')
            else:
                batch = []
                for item in json.loads(body)['items']:
                    key = f"{item['source']}/{item['namespace']}"
                    if args.only and args.only.lower() not in key.lower():
                        continue
                    last = previous.get(key)
                    row = sample_row(elapsed, item, last['item'] if last else None, tick-last['at'] if last else 0)
                    previous[key] = {'item': item, 'at': tick}
                    rows.append(row)
                    batch.append([row['t'], row['key'], row['received'], row['prepared'], row['applied'], row['lag'],
                                  cell(row['received_per_s']), cell(row['applied_per_s']), row['virtual_now'] or '-',
                                  cell(row['watermark_age_s'], 0), row['late_mutations'], row['singletons'],
                                  row['excluded'], row['historical_pending']])
                if batch:
                    if not header_done:
                        report.table(COLUMNS, batch)
                        header_done = True
                    else:
                        for line in format_table(COLUMNS, batch)[2:]:
                            report.line(line)
            if scraper:
                metrics = scraper.metrics()
                if metrics is not None:
                    point = {'t': round(elapsed, 1), 'satellite_received_position': value(metrics, 'app_satellite_received_position'),
                             'satellite_reconnects_total': value(metrics, 'app_satellite_reconnects_total')}
                    metric_rows.append(point)
                    report.line(f"   metrics (one ingestion pod): received_position={point['satellite_received_position']} "
                                f"reconnects={point['satellite_reconnects_total']}")
            if time.monotonic()-started >= args.minutes*60:
                break
            time.sleep(max(0, args.interval-(time.monotonic()-tick)))
    finally:
        if scraper:
            scraper.close()
    report.line()
    summary = summarize(rows)
    report.table(['source/namespace', 'samples', 'peak lag', 'peak at (s)', 'lag at end', 'seconds peak->0',
                  'received +', 'applied +', 'late mutations +'],
                 [[k, s['samples'], s['peak_lag'], s['peak_at_s'], s['final_lag'],
                   '-' if s['seconds_peak_to_zero'] is None else s['seconds_peak_to_zero'], s['received_delta'],
                   s['applied_delta'], s['late_mutations_delta']] for k, s in summary.items()])
    verdicts = []
    if args.lag_limit_seconds:
        for key in sorted({r['key'] for r in rows}):
            state, detail = catchup_verdict([r for r in rows if r['key'] == key], args.lag_limit_seconds)
            verdicts.append((key, state, detail))
            report.line(f'{state:14} {key}: backlog stays within {args.lag_limit_seconds:g} s of feed ({detail})')
    report.line('Compare the late counter with the declared bound; the lag verdict divides the backlog by the feed rate averaged since the start of the run.')
    report.data.update(samples=rows, summary=summary, metrics=metric_rows, verdicts=[list(v) for v in verdicts],
                       finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    report.save(directory)
    return 1 if any(state == 'FAIL' for _, state, _ in verdicts) else 0


if __name__ == '__main__':
    sys.exit(main())
