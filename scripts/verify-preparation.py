#!/usr/bin/env python3
"""Verify a running disposable Compose project; restarts Auth/consumers unless --read-only."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent

def envfile(name):
    result = {}
    for line in (ROOT / name).read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1)
            result[key] = value.strip().strip('"\'')
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', required=True)
    parser.add_argument('--read-only', action='store_true')
    args = parser.parse_args()
    settings, auth = envfile('.env'), envfile('auth-service/.env')
    base = f"http://127.0.0.1:{int(settings['CLIENT_API_HOST_PORT'])}"
    compose = ['docker', 'compose', '--project-name', args.project]
    def docker(arguments, **options):
        return subprocess.run(compose + arguments, cwd=ROOT, capture_output=True, text=True, timeout=60, **options)
    def check(ok, message):
        if not ok:
            raise RuntimeError(message)
        print('PASS ' + message, flush=True)
    def request(path, method='GET', body=None, token=None):
        headers = {'X-Correlation-ID': 'prep-' + uuid.uuid4().hex}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        if token:
            headers['Authorization'] = 'Bearer ' + token
        req = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, method=method, headers=headers)
        try:
            response = urllib.request.urlopen(req, timeout=12)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            payload = response.read()
            return response.status, json.loads(payload) if payload else None, response.headers.get('X-Trace-ID')
    def until(fn, message, seconds=60):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                value = fn()
                if value:
                    return value
            except (RuntimeError, urllib.error.URLError):
                pass
            time.sleep(1)
        raise RuntimeError('Timed out: ' + message)
    until(lambda: request('/ready')[0] == 200, 'client readiness')
    code, session, _ = request('/auth/login', 'POST', {'client_id': auth['FIELD_CLIENT_ID'], 'client_secret': auth['FIELD_CLIENT_SECRET']})
    check(code == 200, 'Field login')
    token = session['access_token']
    until(lambda: request('/hazards?limit=2', token=token)[1].get('count', 0) > 0, 'first canonical observations')
    code, first, trace_id = request('/hazards?limit=2', token=token)
    check(code == 200 and first['count'] <= 2 and not first['partial'] and first['unavailable_units'] == [], 'bounded complete hazard page')
    ids, previous, cursor = set(), None, None
    while True:
        path = '/hazards?limit=2' + ('&cursor=' + urllib.parse.quote(cursor, safe='') if cursor else '')
        code, page, _ = request(path, token=token)
        check(code == 200, 'next page is available')
        for item in page['data']:
            key = (item['occurred_at'], item['hazard_id'])
            check(item['hazard_id'] not in ids and (previous is None or key < previous), 'pagination preserves unique descending order')
            ids.add(item['hazard_id']); previous = key
        if page['next_cursor'] is None:
            break
        cursor = page['next_cursor']
    check(bool(ids), 'canonical observations are available')
    for path in ['/hazards?limit=0', '/hazards?cursor=invalid']:
        code, error, _ = request(path, token=token)
        check(code == 400 and set(error['error']) == {'code', 'message'}, 'structured pagination error')
    if not args.read_only:
        result = docker(['restart', 'auth-service'])
        check(result.returncode == 0, 'Auth restarted without removing its database')
        until(lambda: request('/hazards?limit=1', token=token)[0] == 200, 'existing session after Auth restart')
        check(True, 'session survives Auth process replacement')
    for name in ['consumer-dashboard', 'consumer-notifier']:
        if not args.read_only:
            result = docker(['restart', name]); check(result.returncode == 0, name + ' restarted')
        def ready():
            port = int(settings['DASHBOARD_PORT' if name == 'consumer-dashboard' else 'NOTIFIER_PORT'])
            result = docker(['exec', '-T', name, 'wget', '-qO-', f'http://localhost:{port}/ready'])
            return result.returncode == 0
        until(ready, name + ' ready')
    for name in ['postgres', 'auth-db', 'dashboard-db', 'notifier-db']:
        sql = "SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM pg_roles WHERE rolname=current_user; SELECT has_schema_privilege(current_user,'public','CREATE');"
        result = docker(['exec', '-T', name, 'sh', '-c', 'PGPASSWORD="$APP_PASSWORD" psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PGPORT" -U "$APP_USER" -d "$POSTGRES_DB"'], input=sql)
        check(result.returncode == 0 and result.stdout.strip().splitlines() == ['f', 'f'], name + ' runtime account cannot administer or change schemas')
        result = docker(['exec', '-T', name, 'sh', '-c', 'PGPASSWORD="$APP_PASSWORD" psql -X -qAt -h 127.0.0.1 -p "$PGPORT" -U "$APP_USER" -d postgres -c "SELECT 1"'])
        check(result.returncode != 0, name + ' runtime account cannot connect to the maintenance database')
    for service, port_key in [('aggregator', 'AGGREGATOR_PORT'), ('auth-service', 'AUTH_PORT'), ('client-api', 'CLIENT_API_PORT'), ('consumer-dashboard', 'DASHBOARD_PORT'), ('consumer-notifier', 'NOTIFIER_PORT'), ('bmkg-mock', 'BMKG_PORT'), ('pvmbg-mock', 'PVMBG_PORT')]:
        result = docker(['exec', '-T', service, 'wget', '-qO-', f"http://localhost:{int(settings[port_key])}/metrics"])
        check(result.returncode == 0 and 'app_http_requests_total' in result.stdout, service + ' exposes application metrics')
    check(bool(re.fullmatch('[0-9a-f]{32}', trace_id or '')), 'HTTP response contains trace identity')
    def stored_trace():
        result = docker(['exec', '-T', 'client-api', 'wget', '-qO-', f"http://tempo:{int(settings['TEMPO_PORT'])}/api/traces/{trace_id}"])
        if result.returncode != 0:
            return False
        try:
            value = json.loads(result.stdout)
            batches = value.get('batches', value.get('resourceSpans', []))
            services = {a.get('value', {}).get('stringValue') for batch in batches for a in batch.get('resource', {}).get('attributes', []) if a.get('key') == 'service.name'}
            return {'client-api', 'auth-service', 'aggregator'} <= services
        except (ValueError, TypeError):
            return False
    until(stored_trace, 'exported trace in Tempo', 30)
    check(True, 'request trace includes Client API, Auth and Aggregator in Tempo')
    def message_trace():
        query = '{ resource.service.name = "consumer-notifier" && name = "consume hazard.events" }'
        result = docker(['exec', '-T', 'client-api', 'wget', '-qO-', f"http://tempo:{int(settings['TEMPO_PORT'])}/api/search?q=" + urllib.parse.quote(query)])
        if result.returncode:
            return False
        for trace in json.loads(result.stdout).get('traces', []):
            result = docker(['exec', '-T', 'client-api', 'wget', '-qO-', f"http://tempo:{int(settings['TEMPO_PORT'])}/api/traces/" + trace['traceID']])
            if result.returncode:
                continue
            batches = json.loads(result.stdout).get('batches', [])
            names = {a.get('value', {}).get('stringValue') for batch in batches for a in batch.get('resource', {}).get('attributes', []) if a.get('key') == 'service.name'}
            if {'aggregator', 'consumer-dashboard', 'consumer-notifier'} <= names:
                return True
        return False
    until(message_trace, 'durable outbox and NATS trace', 30)
    check(True, 'message trace continues through Aggregator and both consumers')
    print('Preparation verification passed.', flush=True)

if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Never include subprocess output, request bodies, or configuration in failures.
        print('FAIL ' + (str(error) if isinstance(error, RuntimeError) else type(error).__name__))
        raise SystemExit(1)
