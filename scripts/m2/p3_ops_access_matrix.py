#!/usr/bin/env python3
"""Who can inspect failed messages: HTTP status for every identity and endpoint.

Criteria: P3-5 (failed messages can be inspected only by BNPB Internal Ops).

Calls the Internal Ops endpoints of the Client API as nobody, Media, Field and
Internal Ops, and as Media with forged role headers, then prints a table of HTTP
status codes. A second small table shows how the Audit API answers different
bearer tokens (informational). The verdict line covers only the Client API
endpoints: Internal Ops must succeed and every other identity must be refused.
"""
import argparse
import datetime
import sys
import urllib.parse

from acceptance_support import Session, http, nodeport
from common import LOCAL, read_config, runtime_secrets
from evidence_metrics import Scraper
from evidence_report import Report, new_output_dir

FORGED = {'X-Role': 'internal', 'X-User-Role': 'internal', 'X-Client-Role': 'internal', 'X-Internal-Ops': 'true'}
IDENTITIES = ('no token', 'media', 'field', 'internal', 'media + forged role headers')


def endpoints():
    end = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    query = urllib.parse.urlencode({'from': (end-datetime.timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    'to': end.strftime('%Y-%m-%dT%H:%M:%SZ'), 'interval': 'day'})
    return [('GET /ops/api/failures', '/ops/api/failures?limit=1'), ('GET /ops/api/progress', '/ops/api/progress'),
            ('GET /ops/api/schema-policy', '/ops/api/schema-policy'), ('GET /analytics/summary', '/analytics/summary?'+query)]


def evaluate(matrix):
    """Violations (a non-internal identity got 2xx) and inconclusive rows (internal did not get 2xx)."""
    violations, inconclusive = [], []
    for endpoint, row in matrix.items():
        if not 200 <= row['internal'] < 300:
            inconclusive.append(f"{endpoint}: Internal Ops got HTTP {row['internal']}")
        for identity, status in row.items():
            if identity != 'internal' and 200 <= status < 300:
                violations.append(f'{endpoint}: {identity} got HTTP {status}')
    return {'violations': violations, 'inconclusive': inconclusive, 'ok': not violations and not inconclusive}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='Client API URL; defaults to node A NodePort')
    parser.add_argument('--no-audit', action='store_true', help='skip the informational Audit API table')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    report = Report(secrets=[credentials[k] for k in ('media', 'field', 'internal', 'audit')])
    directory = new_output_dir('p3-ops-access-matrix', args.output)
    base = args.base_url or nodeport(cfg)
    sessions = {role: Session(base, credentials, role) for role in ('media', 'field', 'internal')}
    matrix = {}
    for label, path in endpoints():
        row = {'no token': http(base, path)[0]}
        for role in ('media', 'field', 'internal'):
            row[role] = sessions[role].request(path)[0]
        row['media + forged role headers'] = sessions['media'].request(path, headers=FORGED)[0]
        matrix[label] = row
    report.line(f'Client API {base}: HTTP status by identity')
    report.table(['endpoint', *IDENTITIES], [[label, *[row[i] for i in IDENTITIES]] for label, row in matrix.items()])
    verdict = evaluate(matrix)
    report.data.update(client_api=matrix, verdict=verdict)
    if not args.no_audit:
        tokens = {'no token': None, 'media token': sessions['media'].session and sessions['media'].session['access_token'],
                  'field token': sessions['field'].session and sessions['field'].session['access_token'],
                  'internal token': sessions['internal'].session and sessions['internal'].session['access_token'],
                  'audit token': credentials['audit']}
        audit_rows = []
        with Scraper(cfg, 'aggregator', 8080) as scraper:
            for label, token in tokens.items():
                audit_rows.append((label, scraper.get_text('/internal/audit/unprocessed?limit=1', token)[0]))
        report.line()
        report.line('Audit API /internal/audit/unprocessed by bearer token (informational):')
        report.table(['bearer token', 'HTTP'], audit_rows)
        report.data['audit_api'] = dict(audit_rows)
    report.line()
    for problem in verdict['violations']:
        report.line('VIOLATION '+problem)
    for note in verdict['inconclusive']:
        report.line('INCONCLUSIVE '+note)
    report.line('ONLY INTERNAL OPS CAN INSPECT FAILED MESSAGES' if verdict['ok'] else
                'ACCESS CHECK NOT CLEAN (see VIOLATION / INCONCLUSIVE lines)')
    report.save(directory)
    return 0 if verdict['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
