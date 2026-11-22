#!/usr/bin/env python3
"""P4-4 burst test on the deployed cluster: a 10 s burst at 60x the busiest 1x feed rate, then up to
60 s to be up to date again.

Criteria: P4-4 (no loss, recovery within 60 s after a 60x burst, end-to-end p95 reported).

What it does, in order (every change is undone in `finally`, and `--restore` repeats the undo
from the journal if the tool itself was killed):
 1. installs the controlled feed p4_fixture_feed.py as a temporary Deployment `p4-fixture` (code in a
    ConfigMap, a random token in a Secret, a NetworkPolicy that admits only ingestion);
 2. seeds the province reference for a fresh satellite namespace (refused if it already has data);
 3. points ingestion at the fixture with `kubectl set env` overrides (URL, token, namespace, initial
    offset 0); the live feed acquisition pauses and resumes from its stored progress afterwards;
 4. waits until other satellite namespaces have no backlog (they share the stream and the worker),
    runs the fixture at the normal rate until `applied` keeps up, records the baseline lag;
 5. triggers the burst and samples, every --interval s, lag = fixture latest_offset - received and
    latest_offset - applied, timed by the fixture's own clock;
 6. stops the fixture, waits for the pipeline to drain, then measures per-event latency (available
    at the fixture -> raw_inputs.received_at -> canonical_batches.applied_at) and checks that every
    published pair became exactly one wildfire and nothing was lost, duplicated or merged.
Verdicts: received and applied each back within the baseline lag no later than --recovery-limit s
after the burst ends; complete latency coverage; clean oracle. Exit 0 PASS, 1 FAIL, 2 INVALID (the
run could not judge: burst not observed, fixture too slow to publish on time, unhealthy probes,
backlog that never drained).

Input rates come from p4_feed_rate.py (`--rate-from result.json`: normal = busiest 1x rate over 60
virtual s) or `--normal-rate`. Run it on the machine that runs the cluster, from the repository.
"""
import argparse
import datetime
import hashlib
import json
import math
import secrets
import subprocess
import sys
import time
from pathlib import Path

from acceptance_support import Session, canonical_sql, http, ingestion_sql, nodeport
from common import LOCAL, NAMESPACE, ROOT, kubectl, private_json, read_config, resource, resources, runtime_secrets
from evidence_integration_tests import forward
from evidence_metrics import PodScrapers, value
from evidence_report import Report, new_output_dir
from p4_fixture_feed import pair_index
from p4_province_reference import validate
from render import image, policy

FIXTURE = 'p4-fixture'
FIXTURE_PORT = 9000
JOURNAL = LOCAL/'p4-burst-restore.json'
PROVINCES = ROOT/'deployment'/'m2'/'provinces.json'
PROVINCES_SHA256 = 'a82363d09ff6f8e85d48db143e4e6a99d6be4283769574fd4a802f82f647cf20'
OVERRIDES = ('SATELLITE_URL', 'SATELLITE_TOKEN', 'SATELLITE_NAMESPACE', 'SATELLITE_INITIAL_OFFSET')
TEST_LABELS = {'app': FIXTURE, 'aat.bnpb/test': 'p4-burst'}  # not part of the application: lifecycle tools skip it


# ---- pure functions -------------------------------------------------------------------------

def operation_id(namespace, position):
    """wildfire.BatchIdentity: the canonical operation an input position produces."""
    raw = '[' + json.dumps(namespace, ensure_ascii=False) + ',' + str(int(position)) + ']'
    return 'fire-batch-v1:' + hashlib.sha256(raw.encode()).hexdigest()


def lag(latest, position):
    """Events published but not yet at `position`; a missing position counts as -1."""
    return max(int(latest) - (-1 if position is None else int(position)), 0)


def tolerance(samples, key, floor=2):
    """Baseline lag allowed after recovery: the largest steady-state lag seen, at least one pair."""
    return max([floor] + [s[key] for s in samples])


def recovery(samples, burst_end, key, allowed):
    """Seconds after the burst end at which lag `key` first fell to `allowed`; None if it never did."""
    for s in samples:
        if s['t'] >= burst_end and s[key] <= allowed:
            return s['t'] - burst_end
    return None


def peak(samples, key):
    return max([0] + [s[key] for s in samples])


def percentile(values, q):
    """Nearest-rank percentile (q in 0..1); None for no values."""
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def join_latencies(namespace, ingest_rows, canonical_rows):
    """Per-position latencies from [(position, received_epoch, available_epoch)] and
    [(operation_id, applied_epoch)]; returns (rows, missing positions)."""
    applied = {op: at for op, at in canonical_rows}
    rows, missing = [], []
    for position, received, available in ingest_rows:
        at = applied.get(operation_id(namespace, position))
        if at is None:
            missing.append(position)
            continue
        rows.append({'position': position, 'available_to_received': None if available is None else received - available,
                     'received_to_applied': at - received,
                     'available_to_applied': None if available is None else at - available})
    return rows, missing


def latency_summary(rows, key):
    values = [r[key] for r in rows if r[key] is not None]
    return {'count': len(values), 'p50': percentile(values, .5), 'p95': percentile(values, .95),
            'p99': percentile(values, .99), 'max': max(values) if values else None}


def key_pair(key):
    """(pair index, satellite) of a fixture detection key `sat|date|time|lat|lon`; None if not one."""
    parts = key.split('|') if isinstance(key, str) else []
    if len(parts) != 5 or parts[0] not in ('N20', 'N21'):
        return None
    try:
        return pair_index(parts[3], parts[4], parts[0]), parts[0]
    except ValueError:
        return None


def pair_oracle(hazards, total_pairs):
    """Check [(ref, detection_count, keys)] against pairs 0..total_pairs-1: each exactly one fire of
    its own two detections. Returns {missing, duplicated, merged, bad_count, foreign} (lists of pairs/refs)."""
    seen, duplicated, merged, bad_count, foreign = {}, [], [], [], []
    for ref, count, keys in hazards:
        pairs = [key_pair(k) for k in (keys or [])]
        if not pairs or any(p is None for p in pairs):
            foreign.append(ref)
            continue
        indexes = {p[0] for p in pairs}
        if len(indexes) != 1:
            merged.append(ref)
            continue
        index = indexes.pop()
        if sorted(p[1] for p in pairs) != ['N20', 'N21'] or str(count) != '2':
            bad_count.append(ref)
        if index in seen:
            duplicated.append(index)
        seen[index] = ref
    missing = [k for k in range(total_pairs) if k not in seen]
    beyond = [k for k in seen if k >= total_pairs]
    return {'missing': missing, 'duplicated': duplicated, 'merged': merged, 'bad_count': bad_count,
            'foreign': foreign, 'beyond': beyond}


def verdicts(result, limit):
    """[(state, text)] from the collected result; INVALID checks come first."""
    out = []
    if not result['saw_burst']:
        out.append(('INVALID', 'the fixture never reported the burst phase'))
    if (result.get('generation_utilization') or 0) > 0.8:
        out.append(('INVALID', f"the fixture spent {result['generation_utilization']:.0%} of the burst formatting events; "
                               'it may not have published on time'))
    if result['probe_failures'] > 0.1 * max(1, result['probe_total']):
        out.append(('INVALID', f"{result['probe_failures']} of {result['probe_total']} probes failed"))
    for key, name in (('lag_received', 'received'), ('lag_applied', 'applied')):
        seconds = result['recovery'][key]
        state = 'PASS' if seconds is not None and seconds <= limit else 'FAIL'
        shown = 'never within the run' if seconds is None else f'{seconds:.1f} s'
        out.append((state, f"{name} back within {result['tolerance'][key]} events of the feed {shown} after the burst "
                           f"(limit {limit:g} s; peak lag {result['peak'][key]})"))
    coverage = result['latency']['missing']
    out.append(('PASS' if not coverage else 'FAIL',
                f"every published event reached the canonical store ({result['latency']['joined']} joined, "
                f'{len(coverage)} missing)'))
    oracle = result['oracle']
    problems = {k: len(v) for k, v in oracle.items() if v and k != 'foreign'}
    out.append(('PASS' if not problems else 'FAIL',
                f"each of {result['total_pairs']} pairs is exactly one wildfire ({problems or 'no missing, duplicate or merged fire'})"))
    return out


# ---- cluster plumbing -----------------------------------------------------------------------

def fixture_objects(cfg, token):
    code = (Path(__file__).resolve().parent/'p4_fixture_feed.py').read_text()
    container = {'name': 'fixture', 'image': image(cfg, 'storage'), 'imagePullPolicy': 'IfNotPresent',
                 'command': ['/opt/patroni/bin/python', '-u', '/app/p4_fixture_feed.py', '--port', str(FIXTURE_PORT)],
                 'envFrom': [{'secretRef': {'name': FIXTURE+'-runtime'}}],
                 'ports': [{'name': 'http', 'containerPort': FIXTURE_PORT}],
                 'readinessProbe': {'httpGet': {'path': '/health', 'port': FIXTURE_PORT}, 'periodSeconds': 2},
                 'resources': resources('other'),
                 'volumeMounts': [{'name': 'code', 'mountPath': '/app', 'readOnly': True}]}
    pod = {'containers': [container], 'automountServiceAccountToken': False,
           'nodeSelector': {'kubernetes.io/hostname': cfg['nodes']['a']},
           'volumes': [{'name': 'code', 'configMap': {'name': FIXTURE+'-code'}}]}
    objects = [
        resource('ConfigMap', FIXTURE+'-code', data={'p4_fixture_feed.py': code}),
        resource('Secret', FIXTURE+'-runtime', type='Opaque', stringData={'P4_FIXTURE_TOKEN': token}),
        resource('Deployment', FIXTURE, spec={'replicas': 1, 'strategy': {'type': 'Recreate'},
                 'selector': {'matchLabels': {'app': FIXTURE}},
                 'template': {'metadata': {'labels': dict(TEST_LABELS)}, 'spec': pod}}),
        resource('Service', FIXTURE, spec={'selector': {'app': FIXTURE},
                 'ports': [{'name': 'http', 'port': FIXTURE_PORT, 'targetPort': FIXTURE_PORT}]}),
        policy(FIXTURE, {'app': FIXTURE}, ['ingestion'], [FIXTURE_PORT]),
    ]
    for item in objects:
        item['metadata']['labels'] = dict(TEST_LABELS)
    return objects


def apply(cfg, objects):
    kubectl(cfg, 'apply', '-f', '-', input_text=json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': objects}))


def rollout(cfg, name, timeout=240):
    kubectl(cfg, '-n', NAMESPACE, 'rollout', 'status', 'deployment/'+name, f'--timeout={timeout}s', timeout=timeout+10)


def restore(cfg, journal, report):
    """Undo everything the journal says was changed; safe to repeat."""
    steps = journal.get('steps', [])
    if 'ingestion' in steps:
        kubectl(cfg, '-n', NAMESPACE, 'set', 'env', 'deployment/ingestion', *[k+'-' for k in OVERRIDES])
        rollout(cfg, 'ingestion')
        report.line('ingestion: satellite overrides removed; the configured feed resumes from its stored progress')
    if 'fixture' in steps:
        for kind in ('deployment', 'service', 'networkpolicy', 'configmap', 'secret'):
            name = FIXTURE if kind in ('deployment', 'service', 'networkpolicy') else FIXTURE + ('-code' if kind == 'configmap' else '-runtime')
            kubectl(cfg, '-n', NAMESPACE, 'delete', kind, name, '--ignore-not-found')
        report.line('fixture objects deleted')
    if JOURNAL.exists():
        JOURNAL.unlink()


def write_journal(journal):
    private_json(JOURNAL, journal)


def seed_provinces(cfg, namespace):
    raw = PROVINCES.read_bytes()
    rows = validate(raw, PROVINCES_SHA256)
    used = ingestion_sql(cfg, "SELECT (SELECT count(*) FROM wildfire_observations WHERE namespace=%s)"
                              "+(SELECT count(*) FROM source_progress WHERE source='SATELIT' AND namespace=%s)",
                         (namespace, namespace))
    if used and used[0][0]:
        raise ValueError(f'satellite namespace {namespace} already holds data; choose a fresh --namespace')
    ingestion_sql(cfg, "INSERT INTO provinces(namespace,id,payload) SELECT %s,r->>'id',r FROM jsonb_array_elements(%s::jsonb) r "
                       "ON CONFLICT(namespace,id) DO UPDATE SET payload=excluded.payload", (namespace, json.dumps(rows)))
    count = ingestion_sql(cfg, 'SELECT count(*) FROM provinces WHERE namespace=%s', (namespace,))[0][0]
    if count != len(rows):
        raise RuntimeError('province reference was not installed completely')
    return count


class Probe:
    """Fixture session (through a port-forward) and ingestion progress (through the Client API)."""

    def __init__(self, cfg, credentials, port, token, namespace):
        self.base, self.token, self.namespace = f'http://127.0.0.1:{port}', token, namespace
        self.session = Session(nodeport(cfg), credentials, 'internal')
        self.total = self.failures = 0

    def fixture(self, path='/v1/session', body=None):
        status, raw, _, _ = http(self.base, path, body, self.token, timeout=5, method='POST' if body is not None else None)
        if status != 200:
            raise RuntimeError(f'fixture {path} returned HTTP {status}')
        return json.loads(raw)

    def progress(self):
        status, raw, _, _ = self.session.request('/ops/api/progress', timeout=3)
        if status != 200:
            return None
        return [i for i in json.loads(raw).get('items', []) if i.get('source') == 'SATELIT']

    def sample(self, t0=None):
        """One sample; None when a probe failed (counted)."""
        self.total += 1
        try:
            first = self.fixture()
            items = self.progress()
            second = self.fixture()
        except (RuntimeError, ValueError, OSError):
            items = None
        if items is None:
            self.failures += 1
            return None
        mine = next((i for i in items if i.get('namespace') == self.namespace), {})
        others = sum(max(0, i['received'] - i['applied']) for i in items if i.get('namespace') != self.namespace)
        latest = second['latest_offset']
        clock = second['elapsed_seconds'] if t0 is None else time.monotonic() - t0
        return {'t': round(clock, 3), 'latest': latest, 'received': mine.get('received', -1),
                'prepared': mine.get('prepared', -1), 'applied': mine.get('applied', -1),
                'lag_received': lag(latest, mine.get('received')), 'lag_prepared': lag(latest, mine.get('prepared')),
                'lag_applied': lag(latest, mine.get('applied')), 'phase': second.get('phase'),
                'speed': first.get('speed'), 'others_backlog': others,
                'generation_utilization': second.get('generation_utilization')}


def scrape(cfg):
    out = {}
    for app in ('ingestion', 'wildfire-processor'):
        try:
            with PodScrapers(cfg, app, 8080) as scrapers:
                out[app] = scrapers.metrics() or {}
        except (RuntimeError, OSError):
            out[app] = {}
    return out


def metric_delta(before, after, app, name):
    total = lambda m: sum(v for _, v in (m.get(app) or {}).get(name, []))
    return total(after) - total(before)


# ---- the test -------------------------------------------------------------------------------

def run(cfg, args, report):
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report._secrets.append(credentials['internal'])
    if args.rate_from:
        source = json.loads(Path(args.rate_from).read_text())
        advice = source.get('recommendation') or {}
        normal = float(advice['normal_rate'])
        report.data['rate_source'] = {'path': args.rate_from, 'sha256': hashlib.sha256(Path(args.rate_from).read_bytes()).hexdigest(),
                                      'recommendation': advice}
    else:
        normal = float(args.normal_rate)
    if normal <= 0:
        raise ValueError('the normal rate must be positive')
    namespace = args.namespace or 'satellite-p4-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dt%H%M%S')
    token = secrets.token_urlsafe(32)
    report._secrets.append(token)
    burst_rate = normal * args.factor
    report.line(f'Namespace {namespace}; normal {normal:.3f}/s, burst {burst_rate:.1f}/s for {args.seconds:g} s '
                f'(about {burst_rate*args.seconds:.0f} events); recovery limit {args.recovery_limit:g} s.')
    journal = {'context': cfg['context'], 'namespace': namespace, 'steps': []}
    write_journal(journal)
    result = {'namespace': namespace, 'normal_rate': normal, 'burst_rate': burst_rate, 'factor': args.factor,
              'seconds': args.seconds, 'probe_total': 0, 'probe_failures': 0}
    try:
        report.line(f'Province reference for {namespace}: {seed_provinces(cfg, namespace)} rows')
        journal['steps'].append('fixture')
        write_journal(journal)
        apply(cfg, fixture_objects(cfg, token))
        rollout(cfg, FIXTURE)
        journal['steps'].append('ingestion')
        write_journal(journal)
        kubectl(cfg, '-n', NAMESPACE, 'set', 'env', 'deployment/ingestion', f'SATELLITE_URL=http://{FIXTURE}:{FIXTURE_PORT}',
                f'SATELLITE_TOKEN={token}', f'SATELLITE_NAMESPACE={namespace}', 'SATELLITE_INITIAL_OFFSET=0')
        rollout(cfg, 'ingestion')
        report.line('Fixture running; ingestion reads it.')
        before = scrape(cfg)
        canonical_start = canonical_sql(cfg, 'SELECT now()::text')[0][0]
        with forward(cfg, FIXTURE, FIXTURE_PORT) as port:
            probe = Probe(cfg, credentials, port, token, namespace)
            result.update(measure(probe, args, normal, report))
            probe.fixture('/admin/stop', {})
            drained = drain(probe, args.drain_seconds)
            final = probe.fixture()
            result['probe_total'], result['probe_failures'] = probe.total, probe.failures
        result['drained'] = drained
        result['total_pairs'] = final['total_pairs']
        result['latest_offset'] = final['latest_offset']
        report.line(f"Published {final['latest_offset']+1} events ({final['total_pairs']} pairs); drained: {drained}")
        result['latency'] = latencies(cfg, namespace, result)
        hazards = canonical_sql(cfg, "SET statement_timeout='120s'; SELECT source_ref_id,attributes->>'detection_count',"
                                     "attributes->'detection_keys' FROM hazard_events WHERE source='SATELIT' AND "
                                     "hazard_type='WILDFIRE' AND active AND ingested_at>=%s::timestamptz", (canonical_start,))
        result['oracle'] = pair_oracle([(r[0], r[1], r[2] if isinstance(r[2], list) else json.loads(r[2] or '[]'))
                                        for r in hazards], final['total_pairs'])
        after = scrape(cfg)
        result['metrics'] = {name: metric_delta(before, after, app, name) for app, name in (
            ('wildfire-processor', 'app_canonical_apply_seconds_count'), ('wildfire-processor', 'app_canonical_apply_seconds_sum'),
            ('wildfire-processor', 'app_ingest_canonical_owner_call_seconds_count'),
            ('wildfire-processor', 'app_ingest_canonical_owner_call_seconds_sum'),
            ('ingestion', 'app_satellite_reconnects_total'))}
        result['peak_backlog'] = {name: value(after.get('ingestion') or {}, name) for name in (
            'app_ingest_backlog_raw_outbox', 'app_ingest_backlog_unprepared', 'app_ingest_backlog_canonical_outbox')}
    finally:
        restore(cfg, journal, report)
    return result


def measure(probe, args, normal, report):
    """Warm-up, baseline, burst and recovery sampling. Returns the parts of the result it owns."""
    out = {'saw_burst': False, 'samples': [], 'baseline': [], 'others_backlog_at_start': None}
    deadline = time.monotonic() + args.idle_wait
    while True:
        s = probe.sample(time.monotonic())
        if s and s['others_backlog'] == 0:
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"other satellite namespaces still have {s['others_backlog'] if s else 'unknown'} unapplied "
                               f'inputs after {args.idle_wait:g} s; they share the stream and the worker')
        time.sleep(2)
    out['others_backlog_at_start'] = 0
    probe.fixture('/admin/rate', {'rate': normal})
    t0, steady, deadline = time.monotonic(), None, time.monotonic() + args.warmup_max
    # Steady state: applied trails the feed by at most --steady-lag-seconds of the normal rate for 5 s.
    allowed = max(2, math.ceil(normal * args.steady_lag_seconds))
    while time.monotonic() < deadline:
        s = probe.sample(t0)
        if s and s['latest'] >= 1 and s['lag_applied'] <= allowed:
            steady = steady or time.monotonic()
            if time.monotonic() - steady >= 5:
                break
        else:
            steady = None
        time.sleep(args.interval)
    else:
        raise TimeoutError(f'applied did not keep up with {normal:.3f}/s within {args.warmup_max:g} s of warm-up')
    end = time.monotonic() + args.baseline_seconds
    while time.monotonic() < end:
        s = probe.sample(t0)
        if s:
            out['baseline'].append(s)
        time.sleep(args.interval)
    out['tolerance'] = {key: tolerance(out['baseline'], key) for key in ('lag_received', 'lag_applied')}
    report.line(f"Baseline lag allowed after recovery: received {out['tolerance']['lag_received']}, "
                f"applied {out['tolerance']['lag_applied']} events")
    burst = probe.fixture('/admin/burst', {'normal_rate': normal, 'factor': args.factor, 'seconds': args.seconds, 'before': 2})
    out['burst'] = {k: burst.get(k) for k in ('burst_first_offset', 'burst_last_offset', 'burst_end_elapsed_seconds', 'burst_rate')}
    burst_end = burst['burst_end_elapsed_seconds']
    report.line(f"Burst offsets {burst['burst_first_offset']}..{burst['burst_last_offset']}; ends at +{burst_end:g} s")
    done = {}
    stop_at = burst_end + args.recovery_limit + 5
    utilization = None
    while True:
        s = probe.sample()
        if s:
            out['samples'].append(s)
            out['saw_burst'] |= s['speed'] == args.factor or s['phase'] == 'burst'
            utilization = s['generation_utilization'] if s['phase'] in ('burst', 'after') else utilization
            for key in ('lag_received', 'lag_applied'):
                if key not in done and s['t'] >= burst_end and s[key] <= out['tolerance'][key]:
                    done[key] = s['t']
            if len(out['samples']) % 20 == 0:
                report.line(f"+{s['t']:.1f}s phase={s['phase']} latest={s['latest']} lag received={s['lag_received']} "
                            f"prepared={s['lag_prepared']} applied={s['lag_applied']}")
            if s['t'] >= stop_at or (len(done) == 2 and s['t'] >= max(done.values()) + 5):
                break
        time.sleep(args.interval)
    out['generation_utilization'] = utilization
    out['recovery'] = {key: recovery(out['samples'], burst_end, key, out['tolerance'][key]) for key in ('lag_received', 'lag_applied')}
    out['peak'] = {key: peak(out['samples'], key) for key in ('lag_received', 'lag_applied')}
    return out


def drain(probe, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        s = probe.sample(time.monotonic())
        if s and s['applied'] >= s['latest']:
            return True
        time.sleep(1)
    return False


def latencies(cfg, namespace, result):
    last = result['latest_offset']
    ingest = ingestion_sql(cfg, "SET statement_timeout='120s'; SELECT position,extract(epoch FROM received_at)::float8,"
                                "(convert_from(payload,'UTF8')::jsonb->>'available_at')::float8 FROM raw_inputs "
                                "WHERE source='SATELIT' AND namespace=%s AND position BETWEEN 0 AND %s ORDER BY position",
                           (namespace, last))
    canonical = canonical_sql(cfg, "SET statement_timeout='120s'; SELECT operation_id,extract(epoch FROM applied_at)::float8 "
                                   "FROM canonical_batches WHERE source='SATELIT' AND namespace=%s", (namespace+':wildfire-output',))
    rows, missing = join_latencies(namespace, [tuple(r) for r in ingest], [tuple(r) for r in canonical])
    stored = {r[0] for r in ingest}
    missing += [p for p in range(last + 1) if p not in stored]
    first, final = (result.get('burst') or {}).get('burst_first_offset'), (result.get('burst') or {}).get('burst_last_offset')
    burst_rows = [r for r in rows if first is not None and first <= r['position'] <= final]
    summary = {'joined': len(rows), 'missing': sorted(set(missing))[:200], 'missing_count': len(set(missing))}
    for label, subset in (('all', rows), ('burst', burst_rows)):
        summary[label] = {key: latency_summary(subset, key) for key in ('available_to_received', 'received_to_applied', 'available_to_applied')}
    return summary


def git_revision():
    try:
        return subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], capture_output=True, text=True, timeout=10).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ''


def cell(value, digits=2):
    return '-' if value is None else f'{value:.{digits}f}'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    rate = parser.add_mutually_exclusive_group()
    rate.add_argument('--rate-from', help='result.json of p4_feed_rate.py')
    rate.add_argument('--normal-rate', type=float, help='normal events per second (1x)')
    parser.add_argument('--factor', type=float, default=60)
    parser.add_argument('--seconds', type=float, default=10)
    parser.add_argument('--recovery-limit', type=float, default=60)
    parser.add_argument('--interval', type=float, default=0.5)
    parser.add_argument('--namespace', help='fresh satellite namespace for the test (default satellite-p4-<UTC time>)')
    parser.add_argument('--idle-wait', type=float, default=600, help='seconds to wait for other satellite backlog to drain')
    parser.add_argument('--warmup-max', type=float, default=300)
    parser.add_argument('--steady-lag-seconds', type=float, default=10,
                        help='warm-up is steady once applied trails the feed by at most this many seconds of input')
    parser.add_argument('--baseline-seconds', type=float, default=10)
    parser.add_argument('--drain-seconds', type=float, default=300)
    parser.add_argument('--restore', action='store_true', help='only undo a previous run recorded in the journal')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    report = Report()
    if args.restore:
        if not JOURNAL.exists():
            print('No journal: nothing to restore.')
            return 0
        restore(cfg, json.loads(JOURNAL.read_text()), report)
        return 0
    if JOURNAL.exists():
        print(f'A previous run left {JOURNAL}; run with --restore first.', file=sys.stderr)
        return 2
    if not args.rate_from and not args.normal_rate:
        parser.error('give --rate-from or --normal-rate')
    directory = new_output_dir('p4-burst-cluster', args.output)
    report.data.update(revision=git_revision(), image_tag=cfg.get('image_tag'), context=cfg['context'],
                       started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), arguments=vars(args))
    try:
        result = run(cfg, args, report)
    except (TimeoutError, ValueError, RuntimeError, KeyError, subprocess.TimeoutExpired) as error:
        report.line(f'INVALID  the run could not complete: {error}')
        report.data['error'] = str(error)
        report.save(directory)
        return 2
    lines = verdicts(result, args.recovery_limit)
    report.line()
    latency = result['latency']
    report.table(['latency (s)', 'p50', 'p95', 'p99', 'max', 'events'],
                 [[f'{label}: {key}', cell(s['p50'], 3), cell(s['p95'], 3), cell(s['p99'], 3), cell(s['max'], 3), s['count']]
                  for label in ('burst', 'all') for key, s in latency[label].items()])
    for state, text in lines:
        report.line(f'{state:8} {text}')
    report.data.update(result=result, verdicts=[list(v) for v in lines],
                       finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    samples = result.get('samples', [])
    csv = ['t,phase,latest,received,prepared,applied,lag_received,lag_prepared,lag_applied']
    csv += [f"{s['t']},{s['phase']},{s['latest']},{s['received']},{s['prepared']},{s['applied']},"
            f"{s['lag_received']},{s['lag_prepared']},{s['lag_applied']}" for s in samples]
    (directory/'samples.csv').write_text('\n'.join(csv)+'\n')
    report.save(directory)
    states = {state for state, _ in lines}
    return 2 if 'INVALID' in states else (1 if 'FAIL' in states else 0)


if __name__ == '__main__':
    sys.exit(main())
