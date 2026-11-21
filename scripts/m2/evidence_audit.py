#!/usr/bin/env python3
"""Audit API evidence tool: checksum, full scan, snapshot/compare, gaps and ledgers.

Criteria: P1-1, P1-4, P1-6, P2-2..P2-6, P3-2, P3-4, P4-1, P4-3, P4-5, P4-8, P5-2 (Audit API evidence).

Every command prints a compact result suitable for a terminal screenshot and exits
non-zero when a check fails. It reaches the Aggregator through an owned port-forward
(reconnected if its pod is replaced) or an explicit --base-url, and never prints tokens.
"""
import argparse
import datetime
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from common import LOCAL, private_json, read_config, runtime_secrets

SOURCES = ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI')


def audit_token(cfg):
    return cfg['external'].get('audit_token') or runtime_secrets(LOCAL/'secrets.json')['audit']


def parse_time(value):
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))


class AuditClient:
    """Authenticated Audit API client that survives replacement of the forwarded pod."""

    def __init__(self, cfg, token=None, base_url=None):
        self.cfg, self.token, self.base = cfg, token or audit_token(cfg), base_url
        self._stack, self.port = None, None

    def __enter__(self):
        self._open()
        return self

    def __exit__(self, *_):
        self.close()

    def _open(self):
        if self.base:
            return
        from evidence_integration_tests import forward
        self.close()
        self._stack = ExitStack()
        self.port = self._stack.enter_context(forward(self.cfg, 'aggregator', 8080))

    def close(self):
        if self._stack:
            self._stack.close()
            self._stack = None

    def get(self, path, params=None, timeout=15):
        query = {k: v for k, v in (params or {}).items() if v is not None}
        suffix = '?'+urllib.parse.urlencode(query) if query else ''
        for attempt in (0, 1):
            root = self.base or f'http://127.0.0.1:{self.port}'
            request = urllib.request.Request(root+path+suffix, headers={'Authorization': 'Bearer '+self.token})
            started = time.monotonic()
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return response.status, json.load(response), time.monotonic()-started
            except urllib.error.HTTPError as error:
                try:
                    body = json.loads(error.read())
                except ValueError:
                    body = None
                return error.code, body, time.monotonic()-started
            except (OSError, ValueError):
                if attempt == 0 and not self.base:
                    self._open()
                    continue
                return 0, None, time.monotonic()-started
        return 0, None, 0.0


def pages(client, path, params, limit=1000):
    cursor = None
    while True:
        status, body, _ = client.get(path, {**params, 'limit': limit, 'cursor': cursor})
        if status != 200 or not isinstance(body, dict):
            raise RuntimeError(f'{path} returned HTTP {status}')
        yield body
        cursor = body.get('next_cursor')
        if not cursor:
            return


def checksum_of(refs):
    """Audit checksum: SHA-256 over byte-sorted source_ref_id values joined by newlines."""
    ordered = sorted(refs, key=lambda ref: ref.encode())
    return hashlib.sha256('\n'.join(ordered).encode()).hexdigest()


def analyze(items):
    """Duplicates and ordering violations over audit items (ingested_at, then hazard_id)."""
    seen_ids, seen_refs = set(), set()
    duplicate_ids, duplicate_refs = [], []
    violations, previous = 0, None
    for item in items:
        key = (parse_time(item['ingested_at']), item['hazard_id'])
        if previous is not None and key < previous:
            violations += 1
        previous = key
        if item['hazard_id'] in seen_ids:
            duplicate_ids.append(item['hazard_id'])
        seen_ids.add(item['hazard_id'])
        ref = (item['source'], item['source_ref_id'])
        if ref in seen_refs:
            duplicate_refs.append(ref)
        seen_refs.add(ref)
    return {'items': len(items), 'duplicate_hazard_ids': duplicate_ids,
            'duplicate_source_refs': duplicate_refs, 'order_violations': violations}


def find_gaps(timestamps, threshold):
    """Intervals longer than threshold seconds between consecutive sorted timestamps."""
    ordered = sorted(timestamps)
    gaps = []
    for earlier, later in zip(ordered, ordered[1:]):
        seconds = (later-earlier).total_seconds()
        if seconds > threshold:
            gaps.append({'from': earlier.isoformat(), 'to': later.isoformat(), 'seconds': seconds})
    longest = max(((b-a).total_seconds() for a, b in zip(ordered, ordered[1:])), default=0.0)
    return {'samples': len(ordered), 'longest_gap_seconds': longest, 'gaps_over_threshold': gaps}


def compare_snapshots(before, after):
    """Zero-loss verdict: every source_ref_id acknowledged before must still exist."""
    result = {'sources': {}, 'ok': True}
    for source, old in before['sources'].items():
        new = after['sources'].get(source)
        entry = {'before_count': old['count'], 'after_count': new['count'] if new else None}
        if new is None:
            entry['verdict'] = 'missing from the later snapshot'
            result['ok'] = False
        elif old.get('ids') is not None and new.get('ids') is not None:
            lost = sorted(set(old['ids'])-set(new['ids']))
            entry.update(lost=len(lost), lost_sample=lost[:5], gained=len(set(new['ids'])-set(old['ids'])))
            entry['verdict'] = 'no acknowledged identity lost' if not lost else 'IDENTITIES LOST'
            result['ok'] &= not lost
        elif old['checksum'] == new['checksum'] and old['count'] == new['count']:
            entry['verdict'] = 'identical count and checksum'
        else:
            entry['verdict'] = 'changed; capture both snapshots with --ids to prove nothing was lost'
            result['ok'] &= new['count'] >= old['count']
        if old.get('partial') or (new and new.get('partial')):
            entry['warning'] = 'a snapshot was partial; do not treat it as complete'
            result['ok'] = False
        result['sources'][source] = entry
    return result


def scan_source(client, source, since=None, fields=()):
    """Scan one source; `fields` names extra event fields to keep (e.g. severity, area_name)."""
    items, partial, unavailable, count = [], False, set(), 0
    for page in pages(client, '/internal/audit/events', {'source': source, 'since': since}):
        partial |= bool(page.get('partial'))
        unavailable.update(page.get('unavailable_units') or [])
        for item in page['items']:
            count += 1
            kept = {'hazard_id': item['hazard_id'], 'source': item['source'],
                    'source_ref_id': item['source_ref_id'], 'ingested_at': item['ingested_at']}
            kept.update({field: item.get(field) for field in fields})
            items.append(kept)
    return {'items': items, 'partial': partial, 'unavailable_units': sorted(unavailable)}


def remote_checksum(client, source):
    status, body, _ = client.get('/internal/audit/checksum', {'source': source})
    if status != 200:
        raise RuntimeError(f'checksum for {source} returned HTTP {status}')
    return body


def cmd_checksum(client, args):
    ok = True
    for source in ([args.source] if args.source else SOURCES):
        body = remote_checksum(client, source)
        flag = ' PARTIAL unavailable=' + ','.join(body.get('unavailable_units') or []) if body.get('partial') else ''
        print(f"{source:14} count={body['count']:<8} sha256={body['checksum']} as_of={body.get('as_of')}{flag}")
        ok &= not body.get('partial')
    return ok


def cmd_scan(client, args):
    ok = True
    for source in ([args.source] if args.source else SOURCES):
        scanned = scan_source(client, source, args.since)
        report = analyze(scanned['items'])
        recomputed = checksum_of([i['source_ref_id'] for i in scanned['items']])
        remote = remote_checksum(client, source) if not args.since else None
        match = 'n/a (--since)' if remote is None else str(remote['checksum'] == recomputed and remote['count'] == report['items'])
        problems = (report['duplicate_hazard_ids'] or report['duplicate_source_refs'] or report['order_violations']
                    or scanned['partial'] or match == 'False')
        print(f"{source:14} items={report['items']:<8} duplicate_ids={len(report['duplicate_hazard_ids'])} "
              f"duplicate_refs={len(report['duplicate_source_refs'])} order_violations={report['order_violations']} "
              f"partial={scanned['partial']} checksum_matches_recomputation={match}")
        if scanned['partial']:
            print('   unavailable units: '+', '.join(scanned['unavailable_units']))
        ok &= not problems
    return ok


def cmd_location(client, args):
    status, body, seconds = client.get('/internal/audit/location', {'hazard_id': args.hazard_id})
    print(f'HTTP {status} in {seconds*1000:.0f} ms')
    print(json.dumps(body, indent=2))
    return status == 200 and not (body or {}).get('partial')


def cmd_unprocessed(client, args):
    counts, total = {}, 0
    rows = []
    for page in pages(client, '/internal/audit/unprocessed', {'source': args.source}, limit=1000):
        for item in page['items']:
            total += 1
            key = (item['status'], 'retryable' if item['retryable'] else 'not-retryable')
            counts[key] = counts.get(key, 0)+1
            rows.append(item)
    print(f'unprocessed messages: {total}')
    for (state, retry), n in sorted(counts.items()):
        print(f'  {state:9} {retry:14} {n}')
    for item in rows[:args.show]:
        print(f"  {item['message_id']} {item['source']} attempts={item['attempts']} {item['status']} {item['reason'][:90]}")
    return True


def cmd_snapshot(client, args):
    snapshot = {'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'sources': {}}
    for source in SOURCES:
        remote = remote_checksum(client, source)
        entry = {'count': remote['count'], 'checksum': remote['checksum'], 'partial': bool(remote.get('partial')),
                 'unavailable_units': remote.get('unavailable_units') or []}
        if args.ids:
            scanned = scan_source(client, source)
            entry['ids'] = sorted(i['source_ref_id'] for i in scanned['items'])
            entry['partial'] |= scanned['partial']
        snapshot['sources'][source] = entry
        print(f"{source:14} count={entry['count']:<8} sha256={entry['checksum']} partial={entry['partial']}")
    private_json(Path(args.output), snapshot)
    print('saved '+args.output)
    return not any(e['partial'] for e in snapshot['sources'].values())


def cmd_compare(_client, args):
    result = compare_snapshots(json.loads(Path(args.before).read_text()), json.loads(Path(args.after).read_text()))
    for source, entry in result['sources'].items():
        detail = ' '.join(f'{k}={v}' for k, v in entry.items() if k not in ('verdict', 'lost_sample'))
        print(f"{source:14} {detail} -> {entry['verdict']}")
    print('ZERO LOSS: '+('yes' if result['ok'] else 'NO'))
    return result['ok']


def cmd_gaps(client, args):
    scanned = scan_source(client, args.source, args.since)
    report = find_gaps([parse_time(i['ingested_at']) for i in scanned['items']], args.max_gap)
    print(f"{args.source}: {report['samples']} events, longest gap {report['longest_gap_seconds']:.1f}s, "
          f"{len(report['gaps_over_threshold'])} gap(s) over {args.max_gap:g}s")
    for gap in report['gaps_over_threshold'][:10]:
        print(f"  {gap['from']} -> {gap['to']} ({gap['seconds']:.1f}s)")
    duplicates = analyze(scanned['items'])
    print(f"duplicates: ids={len(duplicates['duplicate_hazard_ids'])} source_refs={len(duplicates['duplicate_source_refs'])}")
    return not report['gaps_over_threshold'] and not duplicates['duplicate_hazard_ids'] and not duplicates['duplicate_source_refs']


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='reach the Aggregator directly instead of through a port-forward')
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('checksum', 'scan'):
        sub = commands.add_parser(name)
        sub.add_argument('--source', choices=SOURCES)
        if name == 'scan':
            sub.add_argument('--since')
    sub = commands.add_parser('location')
    sub.add_argument('--hazard-id', required=True)
    sub = commands.add_parser('unprocessed')
    sub.add_argument('--source', choices=SOURCES)
    sub.add_argument('--show', type=int, default=10)
    sub = commands.add_parser('snapshot', help='record per-source count/checksum (and identities with --ids)')
    sub.add_argument('--output', required=True)
    sub.add_argument('--ids', action='store_true')
    sub = commands.add_parser('compare', help='prove no acknowledged identity was lost between two snapshots')
    sub.add_argument('before')
    sub.add_argument('after')
    sub = commands.add_parser('gaps', help='ingestion gaps and duplicates for one source (takeover evidence)')
    sub.add_argument('--source', required=True, choices=SOURCES)
    sub.add_argument('--since')
    sub.add_argument('--max-gap', type=float, default=60.0)
    args = parser.parse_args(argv)
    handler = {'checksum': cmd_checksum, 'scan': cmd_scan, 'location': cmd_location, 'unprocessed': cmd_unprocessed,
               'snapshot': cmd_snapshot, 'compare': cmd_compare, 'gaps': cmd_gaps}[args.command]
    if args.command == 'compare':
        return 0 if handler(None, args) else 1
    cfg = read_config(args.config)
    with AuditClient(cfg, base_url=args.base_url) as client:
        return 0 if handler(client, args) else 1


if __name__ == '__main__':
    sys.exit(main())
