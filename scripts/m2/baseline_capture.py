#!/usr/bin/env python3
"""Capture a comparable baseline of the ingestion path or of read scaling.

Criteria: performance work on the ingestion pipeline (batching, publication, delivery) and on
Aggregator read scaling. Run it once before a change and once after, with the same dataset and
generator, then compare the two result.json files with `compare`.

capture   Records what makes a run comparable (source revision, deployed images, replicas, resource
          limits, redacted configuration, dataset digest, generator CPU use), scrapes the pipeline metrics
          before, during and after the run, polls source progress, and judges the run against an
          expected-result file. The load itself comes from the command after `--` (for example the
          satellite burst or p2_load_test.py), or from a plain wait with --for.
compare   Prints two captures side by side: throughput, stage, commit, lock, publication and
          acknowledgement figures, plus peak backlog and oldest age.

  baseline_capture.py capture --kind ingestion --label before-batching --dataset burst.json \
      --expected expected.json -- python3 p4_ingestion_progress.py --minutes 5
  baseline_capture.py capture --kind read-scaling --label one-aggregator --aggregators 1 --for 120 -- python3 p2_load_test.py ...
  baseline_capture.py compare evidence-baseline-a/result.json evidence-baseline-b/result.json

The expected file maps "source/namespace" to the final offsets and flags that must hold, for example
{"progress": {"SATELIT/satellite-practice": {"received": 5000, "applied": 5000}}, "max_unapplied": 0}.
Secrets are never recorded: configuration keys that look like credentials are masked.
"""
import argparse
import datetime
import hashlib
import json
import os
import re
import subprocess
import sys
import time

from common import LOCAL, NAMESPACE, ROOT, kubectl, read_config, runtime_secrets
from evidence_metrics import PodScrapers, parse_metrics, select
from evidence_report import Report, format_table, new_output_dir

PIPELINE_APPS = [('ingestion', 8080), ('wildfire-processor', 8080), ('coastal-processor', 8080), ('field-report-processor', 8080)]
SECRET_KEY = re.compile(r'token|secret|password|passwd|api_?key|credential|private', re.I)
GAUGE_PEAKS = ['app_ingest_backlog_raw_outbox', 'app_ingest_backlog_unprepared', 'app_ingest_backlog_canonical_outbox',
               'app_ingest_backlog_oldest_age_seconds_raw_outbox', 'app_ingest_backlog_oldest_age_seconds_unprepared',
               'app_ingest_backlog_oldest_age_seconds_canonical_outbox', 'app_ingest_pg_sessions_waiting_syncrep',
               'app_ingest_pg_sessions_waiting_lock', 'app_ingest_pg_replication_replay_lag_seconds', 'app_ingest_pool_acquired_conns']


def redact(value, key=''):
    """Copy of a JSON value with every credential-looking field masked."""
    if isinstance(value, dict):
        return {k: redact(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v, key) for v in value]
    if SECRET_KEY.search(str(key)) and value not in (None, '', False, True, 0):
        return '***'
    return value


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def git(*args):
    try:
        return subprocess.run(['git', '-C', str(ROOT), *args], capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ''


def deployed(cfg):
    """Images, replicas and resource limits of every workload of the application."""
    out = {}
    for kind in ('deployments', 'statefulsets'):
        try:
            items = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', kind, '-o', 'json'))['items']
        except (RuntimeError, ValueError, subprocess.TimeoutExpired):
            continue
        for item in items:
            containers = item['spec']['template']['spec']['containers']
            out[item['metadata']['name']] = {
                'kind': kind, 'replicas': item['spec'].get('replicas'),
                'containers': {c['name']: {'image': c['image'], 'resources': c.get('resources', {})} for c in containers}}
    return out


def provenance(cfg, args, generator):
    dataset = None
    if args.dataset:
        dataset = {'path': args.dataset, 'bytes': os.path.getsize(args.dataset), 'sha256': sha256_file(args.dataset)}
    return {'kind': args.kind, 'label': args.label, 'revision': git('rev-parse', 'HEAD'),
            'dirty_files': len([line for line in git('status', '--porcelain').splitlines() if '.m2-local' not in line]),
            'image_tag': cfg.get('image_tag'), 'config': redact({k: v for k, v in cfg.items() if k != 'external'}),
            'features': cfg.get('features'), 'deployed': deployed(cfg), 'dataset': dataset, 'dataset_note': args.dataset_note,
            'aggregators_requested': args.aggregators, 'generator': generator,
            'host': {'cpus': os.cpu_count()}}


def generator_usage(cpu_seconds, wall_seconds, cpus):
    """CPU use of the load generator as a fraction of the machine it ran on."""
    if wall_seconds <= 0 or not cpus:
        return None
    return {'cpu_seconds': round(cpu_seconds, 2), 'wall_seconds': round(wall_seconds, 2), 'cpus': cpus,
            'utilization': round(cpu_seconds/(wall_seconds*cpus), 4)}


def total(metrics, name):
    return sum(v for _, v in (metrics or {}).get(name, []))


def peak(metrics, name):
    values = [v for _, v in (metrics or {}).get(name, [])]
    return max(values) if values else None


def histogram_names(metrics):
    """Pipeline histograms (ingest_* and the end-to-end canonical_apply_seconds) present in a scrape."""
    names = []
    for name in metrics:
        base = name[:-len('_count')]
        if name.endswith('_count') and base+'_bucket' in metrics and base.startswith(('app_ingest', 'app_canonical_apply')):
            names.append(base)
    return sorted(names)


def bucket_quantile(before, after, base, q):
    """Estimate a quantile (upper bucket bound) from the increase of a histogram's buckets."""
    def buckets(metrics):
        sums = {}
        for labels, v in (metrics or {}).get(base+'_bucket', []):
            sums[labels.get('le')] = sums.get(labels.get('le'), 0) + v
        return sums
    old, new = buckets(before), buckets(after)
    rows = sorted(((float('inf') if le == '+Inf' else float(le), new[le]-old.get(le, 0)) for le in new), key=lambda r: r[0])
    count = rows[-1][1] if rows else 0
    if count <= 0:
        return None
    for bound, cumulative in rows:
        if cumulative >= q*count:
            return bound
    return rows[-1][0]


def histogram_stats(before, after, base):
    count = total(after, base+'_count')-total(before, base+'_count')
    if count <= 0:
        return {'count': 0}
    seconds = total(after, base+'_sum')-total(before, base+'_sum')
    return {'count': int(count), 'mean_s': seconds/count, 'p50_s': bucket_quantile(before, after, base, .5),
            'p95_s': bucket_quantile(before, after, base, .95), 'p99_s': bucket_quantile(before, after, base, .99)}


def summarise(before, after, series, seconds):
    """Derived figures between two merged scrapes: histograms, counters per second, batch sizes, gauge peaks."""
    histograms = {name: histogram_stats(before, after, name) for name in histogram_names(after or {})}
    counters = {}
    for name in sorted(after or {}):
        if name.endswith('_total') or '_total_' in name:
            delta = total(after, name)-total(before, name)
            counters[name] = {'delta': delta, 'per_s': delta/seconds if seconds > 0 else None}
    batches = {}
    for name in counters:
        match = re.fullmatch(r'app_ingest_batches_total_(\w+)', name)
        if match:
            stage = match.group(1)
            n = counters[name]['delta']
            records = counters.get('app_ingest_batch_records_total_'+stage, {}).get('delta', 0)
            payload = counters.get('app_ingest_batch_bytes_total_'+stage, {}).get('delta', 0)
            batches[stage] = {'batches': n, 'records': records, 'bytes': payload, 'records_per_batch': records/n if n else None,
                              'bytes_per_batch': payload/n if n else None, 'records_per_s': records/seconds if seconds > 0 else None}
    peaks = {name: max([peak(point, name) or 0 for point in series] + [0]) for name in GAUGE_PEAKS}
    pool = {'acquire_wait_s': total(after, 'app_ingest_pool_acquire_wait_seconds_total')-total(before, 'app_ingest_pool_acquire_wait_seconds_total'),
            'acquires': total(after, 'app_ingest_pool_acquire_total')-total(before, 'app_ingest_pool_acquire_total'),
            'empty_acquires': total(after, 'app_ingest_pool_empty_acquire_total')-total(before, 'app_ingest_pool_empty_acquire_total')}
    return {'histograms': histograms, 'counters': counters, 'batches': batches, 'peaks': peaks, 'pool': pool}


def check_expected(items, expected):
    """Verdict lines [(state, text)] from final progress items against an expected-result mapping."""
    verdicts = []
    by_key = {f"{i['source']}/{i['namespace']}": i for i in items}
    for key, want in sorted((expected.get('progress') or {}).items()):
        item = by_key.get(key)
        if item is None:
            verdicts.append(('FAIL', f'{key}: no progress row'))
            continue
        for field, number in sorted(want.items()):
            got = item.get(field)
            verdicts.append(('PASS' if got == number else 'FAIL', f'{key} {field}: expected {number}, got {got}'))
    if 'max_unapplied' in expected:
        for key, item in sorted(by_key.items()):
            gap = item['received']-item['applied']
            verdicts.append(('PASS' if gap <= expected['max_unapplied'] else 'FAIL',
                             f'{key} unapplied={gap} (limit {expected["max_unapplied"]})'))
    return verdicts


def fetch_progress(session):
    status, body, _, _ = session.request('/ops/api/progress')
    return json.loads(body)['items'] if status == 200 else []


def run_generator(command, duration, interval, sample):
    """Run the load command (or wait), calling sample() every interval; returns (cpu seconds, wall seconds)."""
    started = time.monotonic()
    cpu_before = os.times()
    process = subprocess.Popen(command) if command else None
    try:
        while True:
            sample()
            if process and process.poll() is not None:
                break
            if not process and time.monotonic()-started >= duration:
                break
            time.sleep(interval)
    finally:
        if process and process.poll() is None:
            process.terminate()
    cpu_after = os.times()
    children = (cpu_after.children_user+cpu_after.children_system)-(cpu_before.children_user+cpu_before.children_system)
    return children, time.monotonic()-started


def capture(args, command):
    from acceptance_support import Session, nodeport
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials.get('internal', '')])
    directory = new_output_dir('baseline-'+args.label, args.output)
    session = Session(args.base_url or nodeport(cfg), credentials, 'internal')
    scrapers = [PodScrapers(cfg, app, port).__enter__() for app, port in PIPELINE_APPS]

    def scrape():
        from evidence_metrics import merge_metrics
        return merge_metrics([s.metrics() for s in scrapers]) or {}
    series = []
    before = scrape()
    try:
        cpu, wall = run_generator(command, args.duration, args.interval, lambda: series.append(scrape()))
        after = scrape()
        items = fetch_progress(session)
    finally:
        for s in scrapers:
            s.__exit__()
    generator = generator_usage(cpu, wall, os.cpu_count())
    meta = provenance(cfg, args, {'command': command, **(generator or {})})
    figures = summarise(before, after, series, wall)
    report.line(f"Baseline {args.label} ({args.kind}) at revision {meta['revision'][:10]}{' (+uncommitted changes)' if meta['dirty_files'] else ''}, "
                f"image tag {meta['image_tag']}, run {wall:.0f} s, generator utilization {generator['utilization'] if generator else 'n/a'}")
    rows = [[n.replace('app_', ''), s['count'], *(f"{s[k]*1000:.1f}" if s.get(k) is not None else '-' for k in ('mean_s', 'p95_s', 'p99_s'))]
            for n, s in figures['histograms'].items() if s['count']]
    report.table(['duration metric', 'count', 'mean ms', 'p95 ms (bucket)', 'p99 ms (bucket)'], rows)
    report.table(['stage', 'batches', 'records', 'records/batch', 'KiB/batch', 'records/s'],
                 [[st, int(b['batches']), int(b['records']), f"{b['records_per_batch'] or 0:.1f}", f"{(b['bytes_per_batch'] or 0)/1024:.1f}",
                   f"{b['records_per_s'] or 0:.1f}"] for st, b in sorted(figures['batches'].items())])
    report.table(['gauge peak', 'value'], [[n.replace('app_ingest_', ''), f'{v:g}'] for n, v in figures['peaks'].items()])
    report.line(f"pool: acquire wait {figures['pool']['acquire_wait_s']:.2f} s over {figures['pool']['acquires']:.0f} acquires, "
                f"{figures['pool']['empty_acquires']:.0f} had to wait for a connection")
    verdicts = []
    if args.expected:
        with open(args.expected) as handle:
            verdicts = check_expected(items, json.load(handle))
        for state, text in verdicts:
            report.line(f'{state:6} {text}')
    report.data.update(provenance=meta, figures=figures, progress=items, verdicts=[list(v) for v in verdicts],
                       finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    report.save(directory)
    return 1 if any(state == 'FAIL' for state, _ in verdicts) else 0


def compare(left_path, right_path):
    left, right = (json.load(open(p)) for p in (left_path, right_path))
    names = sorted(set(left['figures']['histograms']) | set(right['figures']['histograms']))
    def cell(result, name, key):
        value = result['figures']['histograms'].get(name, {}).get(key)
        return '-' if value is None else f'{value*1000:.1f}'
    for text in format_table(['duration metric', 'A mean ms', 'B mean ms', 'A p95 ms', 'B p95 ms'],
                             [[n.replace('app_', ''), cell(left, n, 'mean_s'), cell(right, n, 'mean_s'), cell(left, n, 'p95_s'), cell(right, n, 'p95_s')] for n in names]):
        print(text)
    stages = sorted(set(left['figures']['batches']) | set(right['figures']['batches']))
    print()
    def number(result, stage, key):
        return f"{result['figures']['batches'].get(stage, {}).get(key) or 0:.1f}"
    for text in format_table(['stage', 'A records/s', 'B records/s', 'A records/batch', 'B records/batch'],
                             [[s, number(left, s, 'records_per_s'), number(right, s, 'records_per_s'),
                               number(left, s, 'records_per_batch'), number(right, s, 'records_per_batch')] for s in stages]):
        print(text)
    print()
    for key in ('revision', 'image_tag', 'aggregators_requested'):
        a, b = left['provenance'].get(key), right['provenance'].get(key)
        print(f"{key}: A={a} B={b}{'  (same)' if a == b else ''}")
    a, b = (r['provenance'].get('dataset') or {} for r in (left, right))
    print(f"dataset: {'same digest' if a.get('sha256') == b.get('sha256') and a else 'DIFFERENT or missing - runs are not comparable'}")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    cap = sub.add_parser('capture')
    cap.add_argument('--kind', choices=['ingestion', 'read-scaling'], required=True)
    cap.add_argument('--label', required=True)
    cap.add_argument('--dataset', help='file that defines the fixed input; its digest is recorded')
    cap.add_argument('--dataset-note', default='', help='free text, e.g. generator seed and speed profile')
    cap.add_argument('--expected', help='JSON of expected final progress; the run fails when it does not hold')
    cap.add_argument('--aggregators', type=int, default=0, help='Aggregator replicas this run is meant to use (read scaling)')
    cap.add_argument('--for', dest='duration', type=float, default=120, help='seconds to observe when no command follows --')
    cap.add_argument('--interval', type=float, default=5)
    cap.add_argument('--config', default=LOCAL/'config.json')
    cap.add_argument('--base-url')
    cap.add_argument('--output')
    cap.add_argument('generator', nargs=argparse.REMAINDER, help='-- command that produces the load')
    cmp_ = sub.add_parser('compare')
    cmp_.add_argument('left')
    cmp_.add_argument('right')
    args = parser.parse_args(argv)
    if args.command == 'compare':
        return compare(args.left, args.right)
    command = args.generator[1:] if args.generator and args.generator[0] == '--' else args.generator
    return capture(args, command or None)


if __name__ == '__main__':
    sys.exit(main())
