#!/usr/bin/env python3
"""Kill the wildfire processor repeatedly while satellite input is being applied and judge the outcome.

Criteria: P4-5 (kill/restart the processor during processing; no identity lost or duplicated, processing
resumes). It does not by itself prove equality with a separate uninterrupted run: it proves the
properties such an equality rests on (applied offsets never go back, nothing is lost or
duplicated, work resumes after every kill).

The tool snapshots the WILDFIRE identities from Audit, then force-deletes one wildfire-processor
pod every --kill-every seconds (--kills times) while polling /ops/api/progress. After the last kill
it keeps watching for --settle seconds, snapshots again and prints PASS/FAIL lines:
  applied offsets never decrease; applied advances within --resume-limit s of every kill
  (NOT MEASURABLE when no backlog was waiting); no WILDFIRE identity present before is missing
  afterwards; no duplicated hazard id or source reference.
"""
import argparse
import json
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, NAMESPACE, kubectl, read_config, runtime_secrets
from evidence_audit import AuditClient, analyze, scan_source
from evidence_report import Report, new_output_dir


def evaluate(samples, kill_times, resume_limit=60, ids_before=None, ids_after=None, duplicates=None):
    """Verdicts as (name, True/False/None, detail) from progress samples [{t, applied, prepared, received}]."""
    result = []
    drops = [(a['t'], b['t']) for a, b in zip(samples, samples[1:]) if b['applied'] < a['applied']]
    result.append(('applied offsets never decrease', not drops, f'{len(drops)} decrease(s) between samples' if drops else f'{len(samples)} samples'))
    for index, killed in enumerate(kill_times, 1):
        around = [s for s in samples if s['t'] >= killed]
        if not around:
            result.append((f'kill {index}: processing resumes', None, 'no sample after the kill'))
            continue
        base = around[0]
        if base['prepared'] <= base['applied'] and base['received'] <= base['applied']:
            result.append((f'kill {index}: applied advances within {resume_limit} s', None, 'no backlog was waiting at the kill'))
            continue
        advanced = next((s['t']-killed for s in around if s['applied'] > base['applied']), None)
        result.append((f'kill {index}: applied advances within {resume_limit} s', advanced is not None and advanced <= resume_limit,
                       f'{advanced:.0f} s after the kill' if advanced is not None else f"applied stayed at {base['applied']} until the run ended"))
    if samples:
        moved = samples[-1]['applied']-samples[0]['applied']
        backlog = samples[0]['received'] > samples[0]['applied']
        result.append(('processing made progress during the run', (moved > 0) if backlog else None,
                       f'applied {samples[0]["applied"]} -> {samples[-1]["applied"]} ({moved:+d})' if backlog else 'no backlog at the start'))
    if ids_before is not None and ids_after is not None:
        lost = sorted(set(ids_before)-set(ids_after))
        result.append(('no WILDFIRE identity present before the kills is missing afterwards', not lost,
                       f'{len(lost)} missing of {len(ids_before)}' if lost else f'{len(ids_before)} before, {len(ids_after)} after'))
    if duplicates is not None:
        result.append(('no duplicated hazard id or source reference', not duplicates['duplicate_hazard_ids'] and not duplicates['duplicate_source_refs'],
                       f"{len(duplicates['duplicate_hazard_ids'])} hazard id(s), {len(duplicates['duplicate_source_refs'])} source reference(s) duplicated"))
    return result


def sample(session, source):
    status, body, _, _ = session.request('/ops/api/progress')
    if status != 200:
        return None
    for item in json.loads(body).get('items', []):
        if item['source'] == source:
            return {'applied': item['applied'], 'prepared': item['prepared'], 'received': item['received']}
    return None


def kill_pod(cfg, app):
    pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-l', f'app={app}', '-o', 'json'))['items']
    running = [p['metadata']['name'] for p in pods if p['status'].get('phase') == 'Running' and not p['metadata'].get('deletionTimestamp')]
    if not running:
        return None
    kubectl(cfg, '-n', NAMESPACE, 'delete', 'pod', running[0], '--grace-period=0', '--force', '--wait=false')
    return running[0]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--source', default='SATELIT')
    parser.add_argument('--app', default='wildfire-processor')
    parser.add_argument('--kills', type=int, default=3)
    parser.add_argument('--kill-every', type=float, default=45)
    parser.add_argument('--settle', type=float, default=90)
    parser.add_argument('--interval', type=float, default=3)
    parser.add_argument('--resume-limit', type=float, default=60)
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['internal']])
    directory = new_output_dir('p4-processor-kill-check', args.output)
    session = Session(args.base_url or nodeport(cfg), credentials, 'internal')
    samples, kill_times = [], []
    with AuditClient(cfg) as client:
        before = scan_source(client, 'SATELIT')
        ids_before = [i['hazard_id'] for i in before['items']]
        report.line(f"{len(ids_before)} {args.source} identities before the kills (partial={before['partial']})")
        started = time.monotonic()
        next_kill = args.kill_every
        total = args.kills*args.kill_every+args.settle
        while time.monotonic()-started < total:
            elapsed = time.monotonic()-started
            if len(kill_times) < args.kills and elapsed >= next_kill:
                victim = kill_pod(cfg, args.app)
                kill_times.append(elapsed)
                next_kill += args.kill_every
                report.line(f'+{elapsed:6.1f}s force-deleted {victim}')
            point = sample(session, args.source)
            if point:
                samples.append({'t': round(elapsed, 1), **point})
                if len(samples) % 5 == 1:
                    report.line(f"+{elapsed:6.1f}s received={point['received']} prepared={point['prepared']} applied={point['applied']} lag={point['received']-point['applied']}")
            time.sleep(args.interval)
        after = scan_source(client, 'SATELIT')
    ids_after = [i['hazard_id'] for i in after['items']]
    duplicates = analyze(after['items'])
    verdicts = evaluate(samples, kill_times, args.resume_limit, ids_before if not before['partial'] else None,
                        ids_after if not after['partial'] else None, duplicates)
    report.line()
    for name, ok, detail in verdicts:
        report.line(f"{'NOT MEASURABLE' if ok is None else 'PASS' if ok else 'FAIL':14} {name} ({detail})")
    report.line('Equality with a separate uninterrupted run is not shown by this tool; it checks loss, duplication and resumption.')
    report.data.update(samples=samples, kill_times=kill_times, identities_before=len(ids_before), identities_after=len(ids_after),
                       verdicts=[list(v) for v in verdicts])
    report.save(directory)
    return 1 if any(ok is False for _, ok, _ in verdicts) else 0


if __name__ == '__main__':
    sys.exit(main())
