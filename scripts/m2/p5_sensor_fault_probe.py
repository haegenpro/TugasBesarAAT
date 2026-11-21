#!/usr/bin/env python3
"""Timeline of how the system behaves while the coastal sensor is unhealthy.

Criteria: P5-4 (client requests stay fast with last data or an unavailability status, and
requests to the sensor do not surge while it is unhealthy); supports P5-3.

While you break the sensor (Tester Agent practice profile or manual), this tool
repeatedly reads /hazards?limit=50 through the Client API and records latency, HTTP
status and the per-source status (fresh/stale/unavailable). Every few seconds it
scrapes one ingestion pod for the coastal circuit-breaker gauges, short-circuit and
failure counters and the request counts to the sensor, and prints their rates, plus
the ingestion source_health table. It reaches no verdict: compare the timeline with
the criterion. Metrics come from a single ingestion pod; the port-forward reconnects
if that pod is replaced.
"""
import argparse
import datetime
import json
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_metrics import Scraper, rate_between, select, value
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
    return {name.replace('app_coastal_', '').replace('_breaker_state', ''): STATE_NAMES.get(int(samples[0][1]), samples[0][1])
            for name, samples in select(metrics, r'app_coastal_.+_breaker_state').items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--role', choices=('media', 'field'), default='media')
    parser.add_argument('--duration', type=float, default=180)
    parser.add_argument('--interval', type=float, default=2)
    parser.add_argument('--scrape-every', type=float, default=10)
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['media'], credentials['field'], credentials['ingestion_internal']])
    directory = new_output_dir('p5-sensor-fault-probe', args.output)
    session = Session(args.base_url or nodeport(cfg), credentials, args.role)
    ticks, scrapes, health_log = [], [], []
    last_line, last_health, last_scrape_at, last_metrics = None, None, None, None
    report.line(f'Reading /hazards?limit=50 every {args.interval:g}s for {args.duration:g}s; break the sensor now.')
    started = time.monotonic()
    with Scraper(cfg, 'ingestion', 8080) as scraper:
        while time.monotonic()-started < args.duration:
            tick = time.monotonic()
            elapsed = tick-started
            status, body, _, seconds = session.request('/hazards?limit=50', timeout=5)
            states, partial = {}, None
            if status == 200:
                try:
                    parsed = json.loads(body)
                    states, partial = source_states(parsed.get('source_status', {})), parsed.get('partial')
                except ValueError:
                    pass
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
                             'fetch_failures_total': value(metrics, 'app_coastal_fetch_failures_total'),
                             'sensor_requests_total': sum(s[0][1] for s in select(metrics, REQUESTS).values())}
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
    report.line()
    summary = latency_summary([t['seconds'] for t in ticks])
    report.table(['client requests', 'min ms', 'avg ms', 'p95 ms', 'max ms'],
                 [[summary.get('requests', 0), *[f"{summary.get(k, 0):.0f}" for k in ('min_ms', 'avg_ms', 'p95_ms', 'max_ms')]]])
    codes = {}
    for t in ticks:
        codes[t['http']] = codes.get(t['http'], 0)+1
    report.line('HTTP status counts: '+json.dumps(codes))
    rates = [s['sensor_requests_per_s'] for s in scrapes if 'sensor_requests_per_s' in s]
    if rates:
        report.line(f'requests to the sensor per second: min {min(rates):.2f}, max {max(rates):.2f} over {len(rates)} interval(s)')
    report.line('No verdict: compare the timeline with P5-4 (fast answers, no surge in requests to the sensor, self-recovery).')
    report.data.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), requests=ticks, scrapes=scrapes,
                       source_health=health_log, latency=summary, http_status_counts=codes)
    report.save(directory)
    return 0


if __name__ == '__main__':
    sys.exit(main())
