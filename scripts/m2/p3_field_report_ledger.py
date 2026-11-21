#!/usr/bin/env python3
"""Ledger of Field reports acknowledged with 202, reconciled after a failure.

Criteria: P3-6 (no report acknowledged with 202 is lost or processed twice).

submit     post N synthetic reports with an Idempotency-Key each, retrying the same key
           and bytes when the answer is ambiguous, and record every attempt in a ledger
reconcile  look up every receipt, count statuses and compare with the ingestion database

Run `submit` while the Tester Agent (or `kubectl delete pod`) disturbs the receiving
component, then `reconcile`. The mechanical checks print PASS/FAIL; whether reports
are still `accepted` or already `processed` is only reported.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import sys
import time

from acceptance_support import Session, ingestion_sql, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_report import Report, new_output_dir


def make_body(index, prefix):
    return json.dumps({'report_type': 'coastal_observation', 'occurred_at': datetime.datetime.now(datetime.timezone.utc)
                       .strftime('%Y-%m-%dT%H:%M:%SZ'), 'latitude': -6.2, 'longitude': 106.8,
                       'description': f'Synthetic ledger fixture {prefix} #{index}', 'attributes': {'synthetic': True}}).encode()


def summarize_ledger(entries, lookups):
    """Mechanical reconciliation of ledger entries with receipt lookups {key: (http, status)}."""
    acknowledged = [e for e in entries if e.get('receipt_id')]
    never_acked = [e['key'] for e in entries if not e.get('receipt_id')]
    multi = [e['key'] for e in entries if len(set(a['receipt_id'] for a in e['attempts'] if a.get('receipt_id'))) > 1]
    by_receipt = {}
    for e in acknowledged:
        by_receipt.setdefault(e['receipt_id'], []).append(e['key'])
    shared = {r: k for r, k in by_receipt.items() if len(k) > 1}
    statuses, missing, unreadable = {}, [], []
    for e in acknowledged:
        code, state = lookups.get(e['key'], (0, None))
        if code == 404:
            missing.append(e['key'])
        elif code != 200:
            unreadable.append(e['key'])
        else:
            statuses[state] = statuses.get(state, 0)+1
    return {'keys': len(entries), 'acknowledged': len(acknowledged), 'never_acknowledged': never_acked,
            'keys_with_several_receipts': multi, 'receipts_shared_by_keys': shared,
            'receipt_status_counts': statuses, 'receipts_not_found': missing, 'receipts_unreadable': unreadable}


def latency_summary(entries):
    """Seconds of the 202 answers (p50, p95, max) and how many reports needed more than one attempt."""
    seconds = sorted(a['seconds'] for e in entries for a in e['attempts'] if a.get('http') == 202 and 'seconds' in a)
    retried = sum(1 for e in entries if len(e['attempts']) > 1)
    if not seconds:
        return {'answers_202': 0, 'retried_keys': retried}
    rank = lambda q: seconds[min(len(seconds)-1, int(q*len(seconds)))]
    return {'answers_202': len(seconds), 'p50_seconds': rank(0.5), 'p95_seconds': rank(0.95),
            'max_seconds': seconds[-1], 'retried_keys': retried}


def checks(summary, db):
    """PASS/FAIL list of the mechanical checks; db is None when the database was not queried."""
    result = [('every acknowledged report has a retrievable receipt',
               not summary['receipts_not_found'] and not summary['receipts_unreadable']),
              ('one receipt per Idempotency-Key', not summary['keys_with_several_receipts']),
              ('no receipt shared by two keys', not summary['receipts_shared_by_keys'])]
    if db is not None:
        # Reports stored without a 202 ever seen (lost response) are possible and only reported.
        result += [('ingestion holds every acknowledged report', db['reports'] >= summary['acknowledged']),
                   ('no report has more than one processing result', db['duplicate_results'] == 0)]
    return result


def cmd_submit(args, cfg, credentials, report):
    base = args.base_url or nodeport(cfg)
    session = Session(base, credentials, 'field')
    directory = new_output_dir('p3-field-report-ledger', args.output)
    ledger = Path(args.ledger) if args.ledger else directory/'ledger.jsonl'
    prefix = args.prefix or datetime.datetime.now(datetime.timezone.utc).strftime('ledger-%Y%m%dT%H%M%SZ')
    with ledger.open('a') as out:
        out.write(json.dumps({'meta': {'prefix': prefix, 'count': args.count, 'base_url': base}})+'\n')
        for index in range(args.count):
            key, body = f'{prefix}-{index:05d}', make_body(index, prefix)
            entry = {'key': key, 'body_sha256': hashlib.sha256(body).hexdigest(), 'attempts': [], 'receipt_id': None}
            for attempt in range(1, args.retries+2):
                status, payload, _, seconds = session.request('/field-reports', body, {'Idempotency-Key': key}, timeout=5)
                receipt = None
                if status == 202:
                    receipt = json.loads(payload).get('receipt_id')
                entry['attempts'].append({'n': attempt, 'http': status, 'seconds': round(seconds, 3), 'receipt_id': receipt})
                if status == 202:
                    entry['receipt_id'] = receipt
                    break
                time.sleep(1)
            out.write(json.dumps(entry)+'\n')
            out.flush()
            if entry['receipt_id'] is None or len(entry['attempts']) > 1:
                report.line(f"{key}: {[a['http'] for a in entry['attempts']]} receipt={entry['receipt_id']}")
            time.sleep(args.interval)
    report.line(f'ledger: {ledger}')
    report.data['ledger'] = str(ledger)
    report.save(directory)
    return 0


def read_ledger(path):
    meta, entries = {}, []
    for line in Path(path).read_text().splitlines():
        value = json.loads(line)
        if 'meta' in value:
            meta = value['meta']
        else:
            entries.append(value)
    return meta, entries


def cmd_reconcile(args, cfg, credentials, report):
    meta, entries = read_ledger(args.ledger)
    session = Session(args.base_url or nodeport(cfg), credentials, 'field')
    lookups = {}
    for entry in entries:
        if entry.get('receipt_id'):
            status, payload, _, _ = session.request('/field-reports/'+entry['receipt_id'])
            lookups[entry['key']] = (status, json.loads(payload).get('status') if status == 200 else None)
    summary = summarize_ledger(entries, lookups)
    db = None
    prefix = (meta.get('prefix') or '')+'%'
    try:
        rows = ingestion_sql(cfg, 'SELECT status,count(*) FROM field_reports WHERE idempotency_key LIKE %s GROUP BY status', (prefix,))
        results = ingestion_sql(cfg, 'SELECT count(*) FROM field_results r JOIN field_reports f USING (receipt_id) '
                                'WHERE f.idempotency_key LIKE %s', (prefix,))
        duplicates = ingestion_sql(cfg, 'SELECT count(*) FROM (SELECT receipt_id FROM field_results GROUP BY receipt_id '
                                   'HAVING count(*)>1) x', ())
        db = {'reports': sum(int(n) for _, n in rows), 'by_status': {s: int(n) for s, n in rows},
              'results': int(results[0][0]), 'duplicate_results': int(duplicates[0][0])}
    except (RuntimeError, ValueError, OSError):
        report.line('Ingestion database counts unavailable (owner primary not reachable).')
    report.line(f"ledger {args.ledger}: {summary['keys']} keys, {summary['acknowledged']} acknowledged with 202, "
                f"{len(summary['never_acknowledged'])} never acknowledged")
    report.line('receipt status as seen by the submitter: '+(json.dumps(summary['receipt_status_counts']) or '{}'))
    latency = latency_summary(entries)
    report.line('202 answer latency and retries: '+json.dumps(latency))
    report.data['latency'] = latency
    if db is not None:
        report.line(f"ingestion database for prefix {meta.get('prefix')}: {db['reports']} reports {json.dumps(db['by_status'])}, "
                    f"{db['results']} processing results, {db['duplicate_results']} receipts with several results")
        stored_only = db['reports']-summary['acknowledged']
        if stored_only > 0:
            report.line(f'{stored_only} report(s) are stored without a 202 seen by the submitter (lost response); informational.')
    report.line('"accepted" means durably stored and still waiting for the processor; "processed" means a result exists.')
    verdicts = checks(summary, db)
    for name, ok in verdicts:
        report.line(('PASS ' if ok else 'FAIL ')+name)
    report.data.update(meta=meta, summary=summary, database=db, checks=dict(verdicts))
    report.save(new_output_dir('p3-field-report-reconcile', args.output))
    return 0 if all(ok for _, ok in verdicts) else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--output')
    commands = parser.add_subparsers(dest='command', required=True)
    sub = commands.add_parser('submit')
    sub.add_argument('--count', type=int, default=200)
    sub.add_argument('--prefix')
    sub.add_argument('--ledger', help='ledger file to append to (default: inside the output directory)')
    sub.add_argument('--retries', type=int, default=5)
    sub.add_argument('--interval', type=float, default=0.2)
    sub = commands.add_parser('reconcile')
    sub.add_argument('--ledger', required=True)
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['field']])
    return {'submit': cmd_submit, 'reconcile': cmd_reconcile}[args.command](args, cfg, credentials, report)


if __name__ == '__main__':
    sys.exit(main())
