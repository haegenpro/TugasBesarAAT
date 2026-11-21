#!/usr/bin/env python3
"""Activate the approved reader version, replay retained PVMBG messages, observe the result.

Criteria: P3-4 (replay after an approved reader/adapter version, stable canonical identity).

Steps, all through the Client API Internal Ops endpoints:
  1. read the declared reader policy and the retained failures (status, retryable)
  2. PUT the reader version (--reader-version, default 2) and wait for adoption
  3. POST /ops/api/replays for the selected retained messages
  4. repeat the same replay request: it must select nothing new
  5. poll the failures list until the selected messages leave `retrying`/`parked`
  6. compare the Audit PVMBG count and duplicate scan before and after

It changes the cluster, so it is a dry run unless --execute is given. The mechanical
checks print PASS/FAIL; whether a retained message is replayable at all (retryable,
status) is reported as observed.
"""
import argparse
import json
import sys
import time

from acceptance_support import Session, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_audit import AuditClient, analyze, remote_checksum, scan_source
from evidence_report import Report, new_output_dir


def pick_messages(items, limit):
    """Retained messages that are still waiting, oldest first."""
    return [i['message_id'] for i in items if i['status'] != 'resolved'][:limit]


def duplicate_identities(client, source):
    """Hazard ids and (source, source_ref_id) pairs that occur more than once in an Audit scan."""
    scanned = scan_source(client, source)
    found = analyze(scanned['items'])
    return found['duplicate_hazard_ids'], found['duplicate_source_refs'], found['items'], scanned['partial']


def json_or_none(body):
    try:
        return json.loads(body)
    except ValueError:
        return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--source', default='PVMBG')
    parser.add_argument('--reader-version', type=int, choices=(1, 2), default=2)
    parser.add_argument('--limit', type=int, default=5, help='retained messages to replay')
    parser.add_argument('--wait', type=float, default=60, help='seconds to wait for the replay to finish')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials['internal'], credentials['admin']])
    if not args.execute:
        report.line(f'Dry run. --execute activates reader version {args.reader_version} and replays up to {args.limit} retained {args.source} message(s).')
        return 0
    session = Session(args.base_url or nodeport(cfg), credentials, 'internal')
    directory = new_output_dir('p3-replay-flow', args.output)
    verdicts, data = [], {}

    def failures():
        status, body, _, _ = session.request(f'/ops/api/failures?source={args.source}&limit=1000')
        return (json_or_none(body) or {}).get('items', []) if status == 200 else []

    status, body, _, _ = session.request('/ops/api/schema-policy')
    report.line(f'reader policy before (HTTP {status}): {body.decode(errors="replace")[:300]}')
    before_items = failures()
    chosen = pick_messages(before_items, args.limit)
    report.line(f'{len(before_items)} retained {args.source} message(s); replaying {len(chosen)}')
    try:
        with AuditClient(cfg) as client:
            events_before = remote_checksum(client, args.source)['count']
            status, body, _, _ = session.request('/ops/api/schema-policy', {'version': args.reader_version}, method='PUT')
            report.line(f'activate reader version {args.reader_version}: HTTP {status} {body.decode(errors="replace")[:120]}')
            verdicts.append(('reader version accepted by Internal Ops', status in (200, 202)))
            time.sleep(10)
            status, body, _, _ = session.request('/ops/api/replays', {'message_ids': chosen}, method='POST')
            first = json_or_none(body) or {}
            report.line(f'replay request: HTTP {status} {json.dumps(first)}')
            verdicts.append(('replay request accepted', status == 202 and first.get('selected', 0) == len(chosen)))
            status, body, _, _ = session.request('/ops/api/replays', {'message_ids': chosen}, method='POST')
            again = json_or_none(body) or {}
            report.line(f'identical replay request again: HTTP {status} {json.dumps(again)}')
            verdicts.append(('repeating the replay selects no new message', status in (202, 409) and again.get('selected', 0) == 0))
            deadline = time.monotonic()+args.wait
            while time.monotonic() < deadline:
                waiting = [i for i in failures() if i['message_id'] in chosen and i['status'] != 'resolved']
                if not waiting:
                    break
                time.sleep(3)
            after = {i['message_id']: i for i in failures() if i['message_id'] in chosen}
            states = {}
            for item in after.values():
                states[item['status']] = states.get(item['status'], 0)+1
            report.line(f'status of the selected messages after the replay: {json.dumps(states)}')
            events_after = remote_checksum(client, args.source)['count']
            duplicate_ids, duplicate_refs, total, partial = duplicate_identities(client, args.source)
            report.line(f'Audit {args.source} events {events_before} -> {events_after} ({events_after-events_before:+d}); '
                        f'{total} scanned, {len(duplicate_ids)} duplicated hazard id(s), {len(duplicate_refs)} duplicated source reference(s), partial={partial}')
            verdicts.append(('no duplicated canonical identity after the replay', not duplicate_ids and not duplicate_refs and not partial))
            data.update(selected=chosen, status_after=states, events_before=events_before, events_after=events_after)
    except (RuntimeError, OSError, ValueError) as error:
        report.line(f'FAIL tool error: {error}')
        verdicts.append(('replay flow completed', False))
    for name, ok in verdicts:
        report.line(('PASS ' if ok else 'FAIL ')+name)
    report.line('Resolved/parked counts and the event delta are observations: compare them with the retained messages chosen.')
    report.data.update(data, checks=dict(verdicts))
    report.save(directory)
    return 0 if all(ok for _, ok in verdicts) else 1


if __name__ == '__main__':
    sys.exit(main())
