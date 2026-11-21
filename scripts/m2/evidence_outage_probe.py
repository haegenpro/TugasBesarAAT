#!/usr/bin/env python3
"""Request-continuity probe for node loss, rejoin and instance failure (P1-4..P1-6, P2-1, P2-8).

Criteria: P1-4, P1-5, P1-6, P2-1, P2-8, P5-3 (request continuity under failure).

Reads the cross-shard hazard list through the real Client API entry point at a steady
rate while a failure is injected elsewhere. It reports whether every request was
answered quickly, whether answers were complete or explicitly partial, and how long
the system took to return to complete answers. It never injects failures itself.
"""
import argparse
import datetime
import json
from pathlib import Path
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, NAMESPACE, kubectl, private_json, read_config, runtime_secrets


def classify(status, body):
    """complete | partial | rejected | error | hung for one probe answer."""
    if status == 0:
        return 'hung'
    if status == 429:
        return 'rejected'
    if status >= 500 or status in (401, 403, 404) or status != 200:
        return 'error'
    try:
        parsed = json.loads(body)
    except ValueError:
        return 'error'
    return 'partial' if parsed.get('partial') else 'complete'


def summarize(records, max_latency):
    """Continuity verdict over [{'t','status','kind','seconds'}] records."""
    kinds = {}
    for record in records:
        kinds[record['kind']] = kinds.get(record['kind'], 0)+1
    latencies = sorted(r['seconds'] for r in records)
    p95 = latencies[min(len(latencies)-1, int(len(latencies)*0.95))] if latencies else None
    degraded = [r for r in records if r['kind'] in ('partial', 'error', 'hung')]
    recovery = None
    if degraded:
        # Stable recovery is the first complete answer after the last degraded one.
        stable = next((r['t'] for r in records if r['t'] > degraded[-1]['t'] and r['kind'] == 'complete'), None)
        recovery = (stable-degraded[0]['t']) if stable is not None else None
    slow = [r for r in records if r['seconds'] > max_latency]
    return {'requests': len(records), 'by_kind': kinds, 'max_seconds': latencies[-1] if latencies else None,
            'p95_seconds': p95, 'slower_than_limit': len(slow), 'first_degraded_at': degraded[0]['t'] if degraded else None,
            'seconds_until_stable_complete': recovery,
            'ok': bool(records) and not kinds.get('error') and not kinds.get('hung') and not slow}


def readiness(cfg):
    try:
        nodes = json.loads(kubectl(cfg, 'get', 'nodes', '-o', 'json'))['items']
        deployment = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'deployment/aggregator', '-o', 'json'))
        ready = sum(1 for n in nodes if any(c['type'] == 'Ready' and c['status'] == 'True' for c in n['status']['conditions']))
        return {'nodes_ready': f'{ready}/{len(nodes)}',
                'aggregator_ready': f"{deployment['status'].get('readyReplicas', 0)}/{deployment['spec']['replicas']}"}
    except (RuntimeError, ValueError, KeyError, OSError):
        return {'nodes_ready': '?', 'aggregator_ready': '?'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--role', choices=('media', 'field'), default='media')
    parser.add_argument('--duration', type=float, default=180)
    parser.add_argument('--interval', type=float, default=0.5)
    parser.add_argument('--timeout', type=float, default=5)
    parser.add_argument('--max-latency', type=float, default=4.0, help='an answer slower than this counts as hanging')
    parser.add_argument('--watch', action='store_true', help='also sample node/Aggregator readiness every 5 s')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    base = args.base_url or nodeport(cfg)
    session = Session(base, runtime_secrets(LOCAL/'secrets.json'), args.role)
    records, marks = [], []
    started = time.monotonic()
    last_kind, last_beat, last_ready = None, 0.0, None
    print(f'Probing {base}/hazards?limit=50 as {args.role} for {args.duration:g}s; inject the failure now.')
    while time.monotonic()-started < args.duration:
        tick = time.monotonic()
        status, body, _, seconds = session.request('/hazards?limit=50', timeout=args.timeout)
        kind = classify(status, body)
        elapsed = tick-started
        records.append({'t': elapsed, 'status': status, 'kind': kind, 'seconds': seconds})
        if kind != last_kind or elapsed-last_beat >= 10:
            print(f'+{elapsed:6.1f}s HTTP {status or "---"} {kind:9} {seconds*1000:6.0f} ms', flush=True)
            last_kind, last_beat = kind, elapsed
        if args.watch and (not marks or elapsed-marks[-1]['t'] >= 5):
            state = readiness(cfg)
            marks.append({'t': elapsed, **state})
            if state != last_ready:
                print(f"+{elapsed:6.1f}s nodes Ready {state['nodes_ready']}, aggregator ready {state['aggregator_ready']}", flush=True)
                last_ready = state
        time.sleep(max(0, args.interval-(time.monotonic()-tick)))
    result = summarize(records, args.max_latency)
    print(json.dumps(result, indent=2))
    print('CONTINUITY OK: every request answered, complete or explicitly partial' if result['ok']
          else 'CONTINUITY FAILED: errors, hung requests or slow answers observed')
    if args.output:
        private_json(Path(args.output), {'base_url': base, 'role': args.role, 'started_at':
                     datetime.datetime.now(datetime.timezone.utc).isoformat(), 'summary': result,
                     'records': records, 'readiness': marks})
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
