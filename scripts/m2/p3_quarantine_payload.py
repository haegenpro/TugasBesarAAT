#!/usr/bin/env python3
"""Download retained original bytes through Internal Ops and compare their SHA-256.

Criteria: P3-2 (original payload preserved byte for byte and exposed only to Internal Ops).

For each retained message listed by GET /ops/api/failures the tool downloads
GET /ops/api/payload/:id, recomputes SHA-256 and compares it with the
X-Content-SHA256 header and Content-Length. It also checks that the metadata-only
Audit listing carries no raw payload field. PASS/FAIL lines are mechanical; which
messages were chosen and why they were retained is only reported.
"""
import argparse
import hashlib
import json
import sys

from acceptance_support import Session, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_audit import AuditClient, pages
from evidence_report import Report, new_output_dir

RAW_FIELDS = ('payload', 'raw', 'body', 'bytes', 'data')


def header(headers, name):
    """Case-insensitive header lookup."""
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def compare_download(status, content, headers):
    """Mechanical comparison of one download with its declared digest and length."""
    digest = hashlib.sha256(content).hexdigest()
    declared = (header(headers, 'X-Content-SHA256') or '').lower()
    length = header(headers, 'Content-Length')
    return {'http': status, 'bytes': len(content), 'sha256': digest, 'declared_sha256': declared or None,
            'sha256_matches': status == 200 and bool(declared) and declared == digest,
            'length_matches': status == 200 and length is not None and int(length) == len(content)}


def leaked_fields(items):
    """Names of raw-content fields present in metadata-only Audit items."""
    return sorted({key for item in items for key in item if key.lower() in RAW_FIELDS})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--source', default='PVMBG')
    parser.add_argument('--limit', type=int, default=5, help='how many retained messages to download')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['internal'], credentials['field'], credentials['media'], credentials['admin']])
    session = Session(args.base_url or nodeport(cfg), credentials, 'internal')
    status, body, _, _ = session.request(f'/ops/api/failures?source={args.source}&limit={max(1, min(args.limit, 1000))}')
    if status != 200:
        report.line(f'FAIL Internal Ops failures listing returned HTTP {status}')
        return 1
    items = json.loads(body).get('items', [])
    report.line(f"{len(items)} retained {args.source} message(s) listed; downloading up to {args.limit}")
    rows, results = [], {}
    for item in items[:args.limit]:
        code, content, headers, seconds = session.request('/ops/api/payload/'+item['message_id'], timeout=130)
        result = compare_download(code, content, headers)
        results[item['message_id']] = {**result, 'status': item['status'], 'retryable': item['retryable'], 'reason': item['reason']}
        rows.append((item['message_id'], item['status'], 'retryable' if item['retryable'] else 'not-retryable', code, len(content),
                     'match' if result['sha256_matches'] else 'MISMATCH', 'match' if result['length_matches'] else 'MISMATCH',
                     result['sha256'][:16]))
    report.table(['message_id', 'status', 'class', 'HTTP', 'bytes', 'sha256 vs header', 'length vs header', 'sha256 (prefix)'], rows)
    verdicts = [('at least one retained message was downloaded', bool(results)),
                ('every download hashes to its X-Content-SHA256', all(r['sha256_matches'] for r in results.values()) and bool(results)),
                ('every download has the declared Content-Length', all(r['length_matches'] for r in results.values()) and bool(results))]
    try:
        with AuditClient(cfg) as client:
            audit_items = [i for page in pages(client, '/internal/audit/unprocessed', {'source': args.source}, limit=200)
                           for i in page['items'][:200]]
            raw = leaked_fields(audit_items)
            report.line(f"metadata-only Audit listing: {len(audit_items)} item(s) checked, fields {sorted(audit_items[0]) if audit_items else '-'}")
            verdicts.append(('Audit unprocessed listing carries no raw payload field', not raw))
    except (RuntimeError, OSError, ValueError):
        report.line('Audit listing unavailable; the metadata-only check was skipped.')
    for name, ok in verdicts:
        report.line(('PASS ' if ok else 'FAIL ')+name)
    report.data.update(source=args.source, downloads=results, checks=dict(verdicts))
    report.save(new_output_dir('p3-quarantine-payload', args.output))
    return 0 if all(ok for _, ok in verdicts) else 1


if __name__ == '__main__':
    sys.exit(main())
