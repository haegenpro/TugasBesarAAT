#!/usr/bin/env python3
"""P2-7 evidence: one-versus-three Aggregator throughput on the same warmed dataset.

Criteria: P2-7 (3 instances at least 2x the throughput of 1).

Runs the same read workload once per replica count with source acquisition paused so the dataset
stays fixed, then compares complete hazard pages per second. The workload is the one of the
P2-6 scaling run (same VUs, ramp and login path) with the total request rate capped:

  --target-rps N      offer N requests per second in total during the hold, identical for both
                      samples, spread evenly over all VUs (the platform's own P2 generator does
                      the same). The cap must be above what ONE Aggregator can serve, otherwise
                      both samples simply meet the cap and the ratio is 1.0. The run is INVALID
                      when the 1-instance sample reaches 90% of the cap.
  --target-rps auto   measure the saturated 1-instance rate in a short pilot at a very high cap,
                      then use cap = cap-factor x that rate for both samples.
  --target-rps 0      closed loop (every VU sleeps --think-seconds), no cap.

When the 3-instance sample reaches the cap, the ratio is reported as a lower bound. Replica
count, acquisition and processors are restored afterwards.
"""
import argparse
from argparse import Namespace
import json
import math
from pathlib import Path
import sys

from acceptance_support import quiesce_sources, restore_sources
from common import LOCAL, NAMESPACE, kubectl, private_json, read_config
import p2_load_test as load_run

REQUIRED_RATIO = 2.0
CAP_HEADROOM = 0.9      # the 1-instance sample must stay below this share of the cap
PILOT_RPS = 20000       # offered rate of the pilot; far above what one instance serves
PILOT_HOLD = 30


def compare(results, cap=None, required=REQUIRED_RATIO):
    """Verdict from {replicas: throughput-dict}; validity problems void the comparison.

    With a cap, the comparison is also INVALID when the low sample did not stay below the cap
    (nothing proves one instance was saturated), and the ratio is a lower bound when the high
    sample reached the cap.
    """
    low, high = min(results), max(results)
    base, scaled = results[low], results[high]
    problems, invalid = [], []
    for replicas, item in results.items():
        if item['partial_pages'] or item['invalid_responses']:
            problems.append(f'{replicas} instance(s): partial or invalid pages were returned')
        if item['transport_failures']:
            problems.append(f'{replicas} instance(s): {item["transport_failures"]} transport failures')
        if item['unexpected_rate'] not in (0, 0.0, None):
            problems.append(f'{replicas} instance(s): unexpected error rate {item["unexpected_rate"]}')
        if cap and (item.get('vus_exercised') or 0) < (item.get('configured_vus') or 0):
            problems.append(f'{replicas} instance(s): only {item.get("vus_exercised") or 0} of '
                            f'{item.get("configured_vus")} VUs sent a measured request')
    ratio = scaled['pages_per_second']/base['pages_per_second'] if base['pages_per_second'] else 0.0
    lower_bound = False
    if cap:
        if base['pages_per_second'] >= CAP_HEADROOM*cap:
            invalid.append(f'{low} instance(s) served {base["pages_per_second"]:.1f} pages/s, at or above '
                           f'{CAP_HEADROOM:.0%} of the {cap:g} RPS cap: the cap is too low to show that one '
                           'instance saturates; raise --target-rps')
        lower_bound = scaled['pages_per_second'] >= load_run.PACED_SATURATION*cap
    return {'low_replicas': low, 'high_replicas': high, 'ratio': ratio, 'required': required, 'cap': cap or None,
            'ratio_is_lower_bound': lower_bound, 'valid': not invalid, 'invalid_reasons': invalid,
            'meets_requirement': not invalid and ratio >= required and not problems,
            'validity_problems': problems}


def cap_from_pilot(pilot_rate, vus, hold_seconds, factor):
    """Cap for the real samples: factor x the saturated 1-instance rate, never below the pacing minimum."""
    minimum = math.ceil(vus/(hold_seconds-10)) if hold_seconds > 10 else 1
    return max(math.ceil(factor*pilot_rate), minimum)


def wait_aggregators(cfg, replicas):
    kubectl(cfg, '-n', NAMESPACE, 'scale', 'deployment/aggregator', f'--replicas={replicas}')
    kubectl(cfg, '-n', NAMESPACE, 'rollout', 'status', 'deployment/aggregator', '--timeout=180s', timeout=190)


def aggregator_nodes(cfg):
    """Node of every running Aggregator pod, so a three-instance sample can show it was spread."""
    pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-l', 'app=aggregator', '-o', 'json'))['items']
    return sorted(pod['spec'].get('nodeName', '') for pod in pods
                  if pod['status'].get('phase') == 'Running' and not pod['metadata'].get('deletionTimestamp'))


def dataset_rows(cfg):
    from evidence_audit import AuditClient, SOURCES, remote_checksum
    with AuditClient(cfg) as client:
        return sum(remote_checksum(client, source)['count'] for source in SOURCES)


def sample(args, root, name, replicas, target_rps, hold_seconds):
    """One load run at a replica count; returns (throughput dict, dataset rows, node list)."""
    wait_aggregators(args.cfg, replicas)
    nodes = aggregator_nodes(args.cfg)
    rows = {'before': dataset_rows(args.cfg)}
    code = load_run.run(Namespace(config=args.config, output=str(root/name), base_url=args.base_url, vus=args.vus,
        ramp_seconds=args.ramp_seconds, hold_seconds=hold_seconds, think_seconds=args.think_seconds,
        memory_mib=args.memory_mib, cpus=args.cpus, report_every=0, target_rps=target_rps, fixed_dataset=True,
        scale=False, roll_from=None, strict_zero_failures=False, audit_check=False))
    rows['after'] = dataset_rows(args.cfg)
    summary = json.loads((root/name/'summary.json').read_text())
    result = load_run.throughput(summary)
    result['generator_exit_code'] = code
    result['aggregator_nodes'] = nodes
    print(f"{name}: {replicas} Aggregator(s) on {len(set(nodes))} node(s), "
          f"{result['pages_per_second']:.1f} complete pages/s over {result['seconds']:.0f}s"
          + (f" (offered {target_rps:g} RPS)" if target_rps else '')
          + f", dataset rows {rows['before']} -> {rows['after']}", flush=True)
    return result, rows


def parse_target(value):
    if value == 'auto':
        return 'auto'
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("use a number of requests per second, 0, or 'auto'")
    if number < 0:
        raise argparse.ArgumentTypeError('must not be negative')
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--output', required=True, help='new private evidence directory')
    parser.add_argument('--base-url')
    parser.add_argument('--counts', type=int, nargs=2, default=(1, 3), metavar=('LOW', 'HIGH'))
    parser.add_argument('--vus', type=int, default=10000, help='same as the P2-6 scaling run')
    parser.add_argument('--ramp-seconds', type=int, default=30)
    parser.add_argument('--hold-seconds', type=int, default=135)
    parser.add_argument('--target-rps', type=parse_target, default='auto', metavar='N|auto|0')
    parser.add_argument('--cap-factor', type=float, default=3.0,
                        help='auto: cap = factor x the saturated one-instance rate (default 3.0)')
    parser.add_argument('--think-seconds', type=float, default=0.01, help='only with --target-rps 0')
    parser.add_argument('--memory-mib', type=int, default=4096)
    parser.add_argument('--cpus', type=float, default=2)
    parser.add_argument('--keep-sources-running', action='store_true',
                        help='do not pause acquisition; the dataset will then change between runs')
    args = parser.parse_args(argv)
    if args.vus < 2 or args.hold_seconds < 20 or args.cap_factor <= 1:
        parser.error('need --vus >= 2, --hold-seconds >= 20 and --cap-factor > 1')
    if isinstance(args.target_rps, float) and args.target_rps > 0 and \
            args.hold_seconds < args.vus/args.target_rps + 10:
        parser.error('hold must let every VU send a measured request: hold >= vus/target-rps + 10 seconds')
    args.cfg = read_config(args.config)
    cfg = args.cfg
    root = Path(args.output).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    results, rows, pilot, cap = {}, {}, None, None
    try:
        if not args.keep_sources_running:
            quiesce_sources(cfg)
        low = min(args.counts)
        if args.target_rps == 'auto':
            pilot_rps = max(PILOT_RPS, args.vus/(PILOT_HOLD-10))
            pilot, _ = sample(args, root, f'pilot-{low}', low, pilot_rps, PILOT_HOLD)
            cap = cap_from_pilot(pilot['pages_per_second'], args.vus, args.hold_seconds, args.cap_factor)
            print(f"pilot: {low} Aggregator(s) serve {pilot['pages_per_second']:.1f} pages/s; "
                  f"cap for both samples = {cap:g} RPS", flush=True)
        elif args.target_rps > 0:
            cap = args.target_rps
        for replicas in sorted(args.counts):
            results[replicas], rows[replicas] = sample(args, root, f'aggregators-{replicas}', replicas,
                                                      cap or 0, args.hold_seconds)
    finally:
        wait_aggregators(cfg, 3)
        if not args.keep_sources_running:
            restore_sources(cfg)
    verdict = compare(results, cap)
    verdict['pilot'] = pilot
    verdict['dataset_rows'] = rows
    verdict['dataset_fixed'] = all(r['before'] == r['after'] for r in rows.values()) and \
        len({r['before'] for r in rows.values()}) == 1
    spread = {replicas: len(set(item['aggregator_nodes'])) for replicas, item in results.items()}
    verdict['distinct_nodes'] = spread
    if len(cfg.get('nodes', {})) >= verdict['high_replicas'] and spread[verdict['high_replicas']] < verdict['high_replicas']:
        verdict['validity_problems'].append(
            f"{verdict['high_replicas']} instances ran on only {spread[verdict['high_replicas']]} node(s); "
            'the comparison does not show scaling across nodes')
        verdict['meets_requirement'] = False
    private_json(root/'comparison.json', {'results': results, 'verdict': verdict})
    bound = ' (lower bound: the high sample reached the cap)' if verdict['ratio_is_lower_bound'] else ''
    print(f"ratio {verdict['low_replicas']} -> {verdict['high_replicas']} instances: {verdict['ratio']:.4f}x"
          f"{bound} (required {verdict['required']}x)"
          + (f", cap {cap:g} RPS" if cap else ', no cap')
          + f"; dataset fixed: {verdict['dataset_fixed']}", flush=True)
    for reason in verdict['invalid_reasons']:
        print('INVALID: '+reason)
    for problem in verdict['validity_problems']:
        print('VALIDITY: '+problem)
    if not verdict['valid']:
        print('P2-7 INVALID')
        return 2
    met = verdict['meets_requirement'] and verdict['dataset_fixed']
    print('P2-7 MET' if met else 'P2-7 NOT MET')
    return 0 if met else 1


if __name__ == '__main__':
    sys.exit(main())
