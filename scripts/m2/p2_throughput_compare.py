#!/usr/bin/env python3
"""P2-7 evidence: one-versus-three Aggregator throughput on the same warmed dataset.

Criteria: P2-7 (3 instances at least 2x the throughput of 1).

Runs the same dedicated read benchmark once per replica count with source acquisition
paused so the dataset stays fixed, then compares complete hazard pages per second.
The requirement is a ratio of at least 2.0; the measured ratio is reported either way.
Replica count, acquisition and processors are restored afterwards.
"""
import argparse
from argparse import Namespace
import json
from pathlib import Path
import sys

from acceptance_support import quiesce_sources, restore_sources
from common import LOCAL, NAMESPACE, kubectl, private_json, read_config
import p2_load_test as load_run

REQUIRED_RATIO = 2.0


def compare(results, required=REQUIRED_RATIO):
    """Verdict from {replicas: throughput-dict}; validity problems void the comparison."""
    low, high = min(results), max(results)
    base, scaled = results[low], results[high]
    problems = []
    for replicas, item in results.items():
        if item['partial_pages'] or item['invalid_responses']:
            problems.append(f'{replicas} instance(s): partial or invalid pages were returned')
        if item['transport_failures']:
            problems.append(f'{replicas} instance(s): {item["transport_failures"]} transport failures')
        if item['unexpected_rate'] not in (0, 0.0, None):
            problems.append(f'{replicas} instance(s): unexpected error rate {item["unexpected_rate"]}')
    ratio = scaled['pages_per_second']/base['pages_per_second'] if base['pages_per_second'] else 0.0
    return {'low_replicas': low, 'high_replicas': high, 'ratio': ratio, 'required': required,
            'meets_requirement': ratio >= required and not problems, 'validity_problems': problems}


def wait_aggregators(cfg, replicas):
    kubectl(cfg, '-n', NAMESPACE, 'scale', 'deployment/aggregator', f'--replicas={replicas}')
    kubectl(cfg, '-n', NAMESPACE, 'rollout', 'status', 'deployment/aggregator', '--timeout=180s', timeout=190)


def dataset_rows(cfg):
    from evidence_audit import AuditClient, SOURCES, remote_checksum
    with AuditClient(cfg) as client:
        return sum(remote_checksum(client, source)['count'] for source in SOURCES)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--output', required=True, help='new private evidence directory')
    parser.add_argument('--base-url')
    parser.add_argument('--counts', type=int, nargs=2, default=(1, 3), metavar=('LOW', 'HIGH'))
    parser.add_argument('--vus', type=int, default=200)
    parser.add_argument('--ramp-seconds', type=int, default=10)
    parser.add_argument('--hold-seconds', type=int, default=60)
    parser.add_argument('--think-seconds', type=float, default=0.01)
    parser.add_argument('--memory-mib', type=int, default=4096)
    parser.add_argument('--cpus', type=float, default=2)
    parser.add_argument('--keep-sources-running', action='store_true',
                        help='do not pause acquisition; the dataset will then change between runs')
    args = parser.parse_args(argv)
    if args.vus >= 10000:
        parser.error('use p2_load_test.py for the 10,000-VU run; this comparison is a dedicated read benchmark')
    cfg = read_config(args.config)
    root = Path(args.output).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    results, rows = {}, {}
    try:
        if not args.keep_sources_running:
            quiesce_sources(cfg)
        for replicas in sorted(args.counts):
            wait_aggregators(cfg, replicas)
            rows[replicas] = {'before': dataset_rows(cfg)}
            code = load_run.run(Namespace(config=args.config, output=str(root/f'aggregators-{replicas}'),
                base_url=args.base_url, vus=args.vus, ramp_seconds=args.ramp_seconds, hold_seconds=args.hold_seconds,
                think_seconds=args.think_seconds, memory_mib=args.memory_mib, cpus=args.cpus, report_every=0,
                scale=False, roll_from=None, strict_zero_failures=False, audit_check=False))
            rows[replicas]['after'] = dataset_rows(cfg)
            summary = json.loads((root/f'aggregators-{replicas}'/'summary.json').read_text())
            results[replicas] = load_run.throughput(summary)
            results[replicas]['generator_exit_code'] = code
            print(f"{replicas} Aggregator(s): {results[replicas]['pages_per_second']:.1f} complete pages/s "
                  f"({results[replicas]['complete_pages']} pages in {results[replicas]['seconds']:.0f}s), "
                  f"dataset rows {rows[replicas]['before']} -> {rows[replicas]['after']}", flush=True)
    finally:
        wait_aggregators(cfg, 3)
        if not args.keep_sources_running:
            restore_sources(cfg)
    verdict = compare(results)
    verdict['dataset_rows'] = rows
    verdict['dataset_fixed'] = all(r['before'] == r['after'] for r in rows.values()) and \
        len({r['before'] for r in rows.values()}) == 1
    private_json(root/'comparison.json', {'results': results, 'verdict': verdict})
    print(f"ratio {verdict['low_replicas']} -> {verdict['high_replicas']} instances: {verdict['ratio']:.4f}x "
          f"(required {verdict['required']}x); dataset fixed: {verdict['dataset_fixed']}")
    for problem in verdict['validity_problems']:
        print('VALIDITY: '+problem)
    print('P2-7 MET' if verdict['meets_requirement'] and verdict['dataset_fixed'] else 'P2-7 NOT MET')
    return 0 if verdict['meets_requirement'] and verdict['dataset_fixed'] else 1


if __name__ == '__main__':
    sys.exit(main())
