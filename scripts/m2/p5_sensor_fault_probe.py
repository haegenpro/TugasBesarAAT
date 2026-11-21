#!/usr/bin/env python3
"""Timeline and verdicts for how the system behaves while the coastal sensor is unhealthy.

Criteria: P5-3 (a failing source does not take the system down) and P5-4 (client requests stay
fast with last data or an unavailability status, the breaker opens, requests to the sensor
do not surge while it is unhealthy, the system recovers by itself).

The tool repeatedly reads /hazards?limit=50 through the Client API and records latency, HTTP
status and the per-source status (fresh/stale/unavailable). Every few seconds it scrapes
EVERY ingestion pod (only the source lease holder polls the sensor) for the coastal
circuit-breaker gauges, short-circuit and failure counters and the request counts to the
sensor, plus the ingestion source_health table. With --break-at and --restore-at it also
scales the sensor deployment to 0 and back to 1 itself, and then judges the timeline:
PASS/FAIL lines for the mechanical parts; the full timeline is printed for the report.
"""
import argparse
import datetime
import json
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, NAMESPACE, kubectl, read_config, runtime_secrets
from evidence_metrics import PodScrapers, rate_between, select, value
from evidence_report import Report, new_output_dir

STATE_NAMES = {0: 'closed', 1: 'half-open', 2: 'open'}
REQUESTS = r'app_coastal_.+_request_seconds_count'
FAILURES = r'app_coastal_fetch_failures_total'
SHORTED = r'app_coastal_.+_short_circuited_total'


def coastal_keys(statuses):
    return [k for k in statuses if 'PANTAI' in k.upper() or 'COASTAL' in k.upper()]


def source_states(statuses):
    return {k: v.get('state') for k, v in sorted(statuses.items())}


def latency_summary(seconds):
    ordered = sorted(seconds)
    if not ordered:
        return {}
    return {'requests': len(ordered), 'min_ms': ordered[0]*1000, 'avg_ms': sum(ordered)/len(ordered)*1000,
            'p95_ms': ordered[min(len(ordered)-1, int(len(ordered)*0.95))]*1000, 'max_ms': ordered[-1]*1000}


def breaker_states(metrics):
    """{breaker: state name}; with several pods the most open state wins."""
    result = {}
    for name, samples in select(metrics, r'app_coastal_.+_breaker_state').items():
        result[name.replace('app_coastal_', '').replace('_breaker_state', '')] = STATE_NAMES.get(int(max(v for _, v in samples)), 'unknown')
    return result


def evaluate(ticks, scrapes, break_at, restore_at, latency_limit_ms=500, recovery_limit=60):
    """Mechanical verdicts as (name, True/False/None, detail); None means the data cannot support a verdict."""
    inside = [t for t in ticks if break_at <= t['t'] < restore_at]
    result = []
    if not inside:
        return [('requests were made while the sensor was down', None, 'no request inside the fault window')]
    summary = latency_summary([t['seconds'] for t in inside])
    result.append((f'client answers stay fast while the sensor is down (p95 <= {latency_limit_ms} ms)',
                   summary['p95_ms'] <= latency_limit_ms, f"p95={summary['p95_ms']:.0f} ms max={summary['max_ms']:.0f} ms over {summary['requests']} requests"))
    failed = [t for t in inside if t['http'] != 200]
    result.append(('no client read fails while the sensor is down', not failed,
                   f"{len(failed)} of {len(inside)} answered {sorted({t['http'] for t in failed})}" if failed else f'{len(inside)} of {len(inside)} answered 200'))
    states = {s for t in inside for s in t['sources'].values()}
    result.append(('the unhealthy source is shown as stale or unavailable', bool(states & {'stale', 'unavailable'}),
                   f'statuses seen: {sorted(states)}'))
    during = [s for s in scrapes if break_at <= s['t'] < restore_at]
    if not scrapes or not any(s['breakers'] for s in scrapes):
        result.append(('a circuit breaker opened', None, 'no breaker gauge was scraped; the metrics scrape found nothing'))
    else:
        opened = sorted({name for s in during for name, state in s['breakers'].items() if state == 'open'})
        result.append(('a circuit breaker opened', bool(opened), f'open breakers: {opened or "none"}'))
    before = [s['sensor_requests_per_s'] for s in scrapes if s['t'] < break_at and 'sensor_requests_per_s' in s]
    rates = [s['sensor_requests_per_s'] for s in during if 'sensor_requests_per_s' in s]
    if not before or not rates:
        result.append(('requests to the sensor do not surge while it is down', None, 'no rate before or during the fault'))
    else:
        baseline, peak = sum(before)/len(before), max(rates)
        result.append(('requests to the sensor do not surge while it is down', peak <= max(2*baseline, baseline+1),
                       f'baseline {baseline:.2f}/s, peak during the fault {peak:.2f}/s'))
    after = [t for t in ticks if t['t'] >= restore_at]
    recovered = next((t['t']-restore_at for t in after if t['sources'] and all(v == 'fresh' for k, v in t['sources'].items() if 'PANTAI' in k.upper())), None)
    if not after:
        result.append(('the sensor sources are fresh again', None, 'the run ended before the sensor was restored'))
    else:
        result.append((f'the sensor sources are fresh again within {recovery_limit} s of the restore', recovered is not None and recovered <= recovery_limit,
                       f'{recovered:.0f} s after the restore' if recovered is not None else 'not fresh again before the run ended'))
    return result


def scale_sensor(cfg, replicas):
    kubectl(cfg, '-n', NAMESPACE, 'scale', 'deploy/sensor-pantai', f'--replicas={replicas}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--role', choices=('media', 'field'), default='media')
    parser.add_argument('--duration', type=float, default=180)
    parser.add_argument('--interval', type=float, default=2)
    parser.add_argument('--scrape-every', type=float, default=5)
    parser.add_argument('--break-at', type=float, help='seconds into the run: scale the sensor deployment to 0')
    parser.add_argument('--restore-at', type=float, help='seconds into the run: scale it back to 1')
    parser.add_argument('--latency-limit-ms', type=float, default=500)
    parser.add_argument('--recovery-limit', type=float, default=60)
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    if (args.break_at is None) != (args.restore_at is None) or (args.break_at is not None and args.restore_at <= args.break_at):
        parser.error('--break-at and --restore-at go together, in that order')
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['media'], credentials['field'], credentials['ingestion_internal']])
    directory = new_output_dir('p5-sensor-fault-probe', args.output)
    session = Session(args.base_url or nodeport(cfg), credentials, args.role)
    ticks, scrapes, health_log, failures = [], [], [], []
    last_line, last_health, last_scrape_at, last_metrics = None, None, None, None
    broken = restored = False
    report.line(f'Reading /hazards?limit=50 every {args.interval:g}s for {args.duration:g}s'
                + ('' if args.break_at is not None else '; break the sensor now.'))
    started = time.monotonic()
    try:
        with PodScrapers(cfg, 'ingestion', 8080) as scraper:
            while time.monotonic()-started < args.duration:
                tick = time.monotonic()
                elapsed = tick-started
                if args.break_at is not None and not broken and elapsed >= args.break_at:
                    scale_sensor(cfg, 0)
                    broken = True
                    report.line(f'+{elapsed:6.1f}s sensor-pantai scaled to 0')
                if args.restore_at is not None and not restored and elapsed >= args.restore_at:
                    scale_sensor(cfg, 1)
                    restored = True
                    report.line(f'+{elapsed:6.1f}s sensor-pantai scaled to 1')
                status, body, _, seconds = session.request('/hazards?limit=50', timeout=5)
                states, partial = {}, None
                if status == 200:
                    try:
                        parsed = json.loads(body)
                        states, partial = source_states(parsed.get('source_status', {})), parsed.get('partial')
                    except ValueError:
                        pass
                else:
                    failures.append({'t': round(elapsed, 1), 'http': status, 'body': body[:200].decode('utf-8', 'replace')})
                ticks.append({'t': round(elapsed, 1), 'http': status, 'seconds': seconds, 'partial': partial, 'sources': states})
                line = (status, partial, tuple(states.items()))
                if line != last_line:
                    report.line(f"+{elapsed:6.1f}s HTTP {status or '---'} {seconds*1000:6.0f} ms partial={partial} "
                                f"{' '.join(f'{k}={v}' for k, v in states.items())}")
                    last_line = line
                if last_scrape_at is None or tick-last_scrape_at >= args.scrape_every:
                    metrics = scraper.metrics()
                    if metrics is not None:
                        point = {'t': round(elapsed, 1), 'breakers': breaker_states(metrics),
                                 'fetch_failures_total': sum(v for _, v in metrics.get('app_coastal_fetch_failures_total', [])),
                                 'sensor_requests_total': sum(v for samples in select(metrics, REQUESTS).values() for _, v in samples)}
                        if last_metrics is not None:
                            dt = tick-last_scrape_at
                            point['sensor_requests_per_s'] = sum(rate_between(last_metrics, metrics, dt, REQUESTS).values())
                            point['failures_per_s'] = sum(rate_between(last_metrics, metrics, dt, FAILURES).values())
                            point['short_circuited_per_s'] = sum(rate_between(last_metrics, metrics, dt, SHORTED).values())
                            report.line(f"+{elapsed:6.1f}s sensor requests/s={point['sensor_requests_per_s']:.2f} "
                                        f"failures/s={point['failures_per_s']:.2f} short-circuited/s={point['short_circuited_per_s']:.2f} "
                                        f"breakers={point['breakers']}")
                        scrapes.append(point)
                        last_metrics, last_scrape_at = metrics, tick
                    else:
                        last_scrape_at = tick
                    code, health, _ = scraper.get_json('/internal/ingestion/health', credentials['ingestion_internal'])
                    if code == 200 and health:
                        summary = {i['endpoint']: i['state'] for i in health.get('items', [])}
                        health_log.append({'t': round(elapsed, 1), 'source_health': summary})
                        if summary != last_health:
                            report.line(f'+{elapsed:6.1f}s source_health {summary}')
                            last_health = summary
                time.sleep(max(0, args.interval-(time.monotonic()-tick)))
    finally:
        if broken and not restored:
            scale_sensor(cfg, 1)
            report.line('sensor-pantai scaled back to 1 after an early stop')
    report.line()
    summary = latency_summary([t['seconds'] for t in ticks])
    report.table(['client requests', 'min ms', 'avg ms', 'p95 ms', 'max ms'],
                 [[summary.get('requests', 0), *[f"{summary.get(k, 0):.0f}" for k in ('min_ms', 'avg_ms', 'p95_ms', 'max_ms')]]])
    codes = {}
    for t in ticks:
        codes[t['http']] = codes.get(t['http'], 0)+1
    report.line('HTTP status counts: '+json.dumps(codes))
    for failure in failures[:5]:
        report.line(f"non-200 at +{failure['t']}s: HTTP {failure['http']} {failure['body']}")
    rates = [s['sensor_requests_per_s'] for s in scrapes if 'sensor_requests_per_s' in s]
    if rates:
        report.line(f'requests to the sensor per second: min {min(rates):.2f}, max {max(rates):.2f} over {len(rates)} interval(s)')
    verdicts = []
    if args.break_at is not None:
        verdicts = evaluate(ticks, scrapes, args.break_at, args.restore_at, args.latency_limit_ms, args.recovery_limit)
        report.line()
        for name, ok, detail in verdicts:
            report.line(f"{'NOT MEASURABLE' if ok is None else 'PASS' if ok else 'FAIL':14} {name} ({detail})")
    else:
        report.line('No verdict without --break-at/--restore-at: compare the timeline with P5-4.')
    report.data.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), requests=ticks, scrapes=scrapes,
                       source_health=health_log, latency=summary, http_status_counts=codes, non_200=failures[:50],
                       verdicts=[list(v) for v in verdicts])
    report.save(directory)
    return 1 if any(ok is False for _, ok, _ in verdicts) else 0


if __name__ == '__main__':
    sys.exit(main())
