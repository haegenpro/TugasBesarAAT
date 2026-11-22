#!/usr/bin/env python3
"""Poll the Feed Satelit session state and count each speed value it reports.

Criteria: supporting evidence for P4-4 (which feed speed the 60x catch-up runs against) and P4-7
(live feed). Calls GET /v1/session once per interval (default every second for 5 minutes) with the
group token from the private configuration, then reports how often each speed (and profile) value
was seen, plus failed calls. It only observes; the token is never printed or saved.
"""
import argparse
import collections
import json
import sys
import time
import urllib.error
import urllib.request

from common import LOCAL, read_config
from evidence_report import Report, new_output_dir


def fetch(url, token, timeout):
    # The feed is behind Cloudflare, which rejects the default Python-urllib user agent with 403.
    request = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}', 'User-Agent': 'feed-session-watch/1.0'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read(1 << 16))
    except urllib.error.HTTPError as error:
        return error.code, None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
        return 0, type(error).__name__


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--interval', type=float, default=1.0, help='seconds between calls')
    parser.add_argument('--duration', type=float, default=300.0, help='total seconds to poll')
    parser.add_argument('--timeout', type=float, default=5.0, help='per-call timeout in seconds')
    parser.add_argument('--output', help='new result directory (default .m2-local/evidence-...)')
    args = parser.parse_args()
    cfg = read_config(args.config)
    token = cfg.get('external', {}).get('satellite_token', '')
    if not token:
        sys.exit('external.satellite_token is empty in the private configuration')
    url = cfg['satellite']['url'].rstrip('/') + '/v1/session'
    out = new_output_dir('p4_feed_session_watch', args.output)

    samples = []
    start = time.monotonic()
    calls = max(1, int(args.duration / args.interval))
    for n in range(calls):
        due = start + n * args.interval
        time.sleep(max(0.0, due - time.monotonic()))
        at = time.time()
        status, body = fetch(url, token, args.timeout)
        sample = {'t': round(at, 3), 'status': status}
        if status == 200 and isinstance(body, dict):
            sample.update({k: body.get(k) for k in ('profile', 'speed', 'latest_offset', 'virtual_now')})
        elif isinstance(body, str):
            sample['error'] = body
        samples.append(sample)
        if n % 30 == 0:
            print(f'{n}/{calls} calls; last status {status} speed {sample.get("speed")}', file=sys.stderr, flush=True)

    ok = [s for s in samples if s['status'] == 200 and 'speed' in s]
    speeds = collections.Counter(s['speed'] for s in ok)
    profiles = collections.Counter(s['profile'] for s in ok)
    failures = collections.Counter(s.get('error') or s['status'] for s in samples if s not in ok)
    changes = [{'t': b['t'], 'from': a['speed'], 'to': b['speed']} for a, b in zip(ok, ok[1:]) if a['speed'] != b['speed']]
    offsets = [s['latest_offset'] for s in ok if isinstance(s.get('latest_offset'), int)]
    span = ok[-1]['t'] - ok[0]['t'] if len(ok) > 1 else 0

    report = Report(secrets=(token,))
    report.line(f'GET {url} every {args.interval:g}s for {args.duration:g}s: {len(samples)} calls, {len(ok)} answered')
    report.line('')
    report.line('Speed values')
    report.table(['speed', 'count', 'share'], [[k, v, f'{100 * v / len(ok):.1f}%'] for k, v in speeds.most_common()])
    report.line('')
    report.line('Profile values')
    report.table(['profile', 'count'], [[k, v] for k, v in profiles.most_common()])
    if failures:
        report.line('')
        report.line('Failed calls')
        report.table(['status/error', 'count'], [[k, v] for k, v in failures.most_common()])
    report.line('')
    report.line(f'Speed changes during the run: {len(changes)}')
    if offsets and span > 0:
        report.line(f'latest_offset {offsets[0]} -> {offsets[-1]} (+{offsets[-1] - offsets[0]} in {span:.0f}s, '
                     f'{(offsets[-1] - offsets[0]) / span:.1f} events/s)')
    report.data = {'url': url, 'interval': args.interval, 'duration': args.duration,
                   'speeds': {str(k): v for k, v in speeds.items()}, 'profiles': dict(profiles),
                   'failures': {str(k): v for k, v in failures.items()}, 'changes': changes, 'samples': samples}
    report.save(out)


if __name__ == '__main__':
    main()
