#!/usr/bin/env python3
"""Measure the live satellite feed's event rate at its running speed (60x) and at 1x.

Criteria: P4-4 input sizing (the normal and burst rates used by p4_burst_cluster.py); supports P4-7.

The tool polls GET /v1/session of the assistants' feed every --interval seconds for --minutes and
records latest_offset, virtual_now, speed and profile. From those samples:
  running rate  offsets published per real second while the session runs at its own speed
                (the practice session reports speed 60, so this is the live 60x rate);
  1x rate       offsets published per virtual second, i.e. the same feed at speed 1;
  busiest       the largest rate over any window of W virtual seconds (60, 300, 600) or of
                --real-window real seconds, next to the averages.
The recommendation written to result.json is what the burst test reads: normal = the busiest 1x
rate over 60 virtual seconds, burst = --factor x normal, events in a 10 s burst = 10 x burst.

Offsets also count heartbeats that carry an offset, so every figure is an upper bound for
detections. The tool only reads the session endpoint: it opens no stream and moves no offsets. The
group token is read from the private configuration (or --token-file) and never printed.
"""
import argparse
import datetime
import json
import re
import sys
import time

from acceptance_support import http
from common import LOCAL, read_config
from evidence_report import Report, format_table, new_output_dir

USER_AGENT = 'curl/8.5.0'  # the feed sits behind Cloudflare, which blocks the Python-urllib agent
_FRACTION = re.compile(r'\.(\d+)')


def parse_rfc3339(text):
    """Epoch seconds from an RFC 3339 timestamp (Z or numeric offset, up to nanoseconds); None if unreadable."""
    if not isinstance(text, str) or not text.strip():
        return None
    value = text.strip()
    if value[-1] in 'zZ':
        value = value[:-1] + '+00:00'
    value = _FRACTION.sub(lambda m: '.' + (m.group(1) + '000000')[:6], value, count=1)
    try:
        moment = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.timestamp()


def sample_from(body, t, wall):
    """One sample from a /v1/session body, or None when latest_offset is missing."""
    try:
        session = json.loads(body)
    except ValueError:
        return None
    offset = session.get('latest_offset') if isinstance(session, dict) else None
    if type(offset) is not int:
        return None
    speed = session.get('speed')
    return {'t': t, 'wall': wall, 'offset': offset, 'virtual': parse_rfc3339(session.get('virtual_now')),
            'speed': speed if isinstance(speed, (int, float)) else None, 'profile': str(session.get('profile') or '')}


def segments(samples):
    """Runs of samples that belong to one uninterrupted session: split where the offset or the virtual
    clock goes backwards (a session reset) or the speed or profile changes."""
    result, current = [], []
    for sample in samples:
        if current:
            last = current[-1]
            reset = sample['offset'] < last['offset'] or (
                sample['virtual'] is not None and last['virtual'] is not None and sample['virtual'] < last['virtual'])
            changed = sample['speed'] != last['speed'] or sample['profile'] != last['profile']
            if reset or changed:
                result.append(current)
                current = []
        current.append(sample)
    if current:
        result.append(current)
    return result


def average_rates(segment):
    """{'offsets', 'real_seconds', 'virtual_seconds', 'real_rate', 'virtual_rate', 'observed_speed'} of one segment."""
    first, last = segment[0], segment[-1]
    offsets = last['offset'] - first['offset']
    real = last['t'] - first['t']
    clocked = [s for s in segment if s['virtual'] is not None]
    virtual = clocked[-1]['virtual'] - clocked[0]['virtual'] if len(clocked) >= 2 else 0.0
    virtual_offsets = clocked[-1]['offset'] - clocked[0]['offset'] if len(clocked) >= 2 else 0
    return {'offsets': offsets, 'real_seconds': real, 'virtual_seconds': virtual, 'virtual_offsets': virtual_offsets,
            'real_rate': offsets / real if real > 0 else None,
            'virtual_rate': virtual_offsets / virtual if virtual > 0 else None,
            'observed_speed': virtual / real if real > 0 and virtual > 0 else None}


def busiest_window(points, window):
    """Largest (offset increase / span) over spans of at least `window`, from [(x, offset)] with x
    non-decreasing. Each span starts at a sample and ends at the first sample `window` or more later."""
    best, j = None, 0
    for i in range(len(points)):
        j = max(j, i)
        while j < len(points) and points[j][0] - points[i][0] < window:
            j += 1
        if j >= len(points):
            break
        span = points[j][0] - points[i][0]
        rate = (points[j][1] - points[i][1]) / span
        if best is None or rate > best['rate']:
            best = {'rate': rate, 'start': points[i][0], 'end': points[j][0], 'span': span,
                    'events': points[j][1] - points[i][1]}
    return best


def analyze(samples, virtual_windows=(60, 300, 600), real_window=10):
    """Averages and busiest windows over every segment of the samples."""
    parts = [s for s in segments(samples) if len(s) >= 2]
    averages = [average_rates(s) for s in parts]
    real_seconds = sum(a['real_seconds'] for a in averages)
    virtual_seconds = sum(a['virtual_seconds'] for a in averages)
    result = {
        'samples': len(samples), 'segments': len(parts),
        'speeds': sorted({s['speed'] for s in samples if s['speed'] is not None}),
        'profiles': sorted({s['profile'] for s in samples}),
        'real_seconds': real_seconds, 'virtual_seconds': virtual_seconds,
        'offsets': sum(a['offsets'] for a in averages),
        'average_running_rate': sum(a['offsets'] for a in averages) / real_seconds if real_seconds > 0 else None,
        'average_1x_rate': sum(a['virtual_offsets'] for a in averages) / virtual_seconds if virtual_seconds > 0 else None,
        'busiest_1x': {}, 'busiest_running': None,
    }
    for window in virtual_windows:
        best = None
        for part in parts:
            found = busiest_window([(s['virtual'], s['offset']) for s in part if s['virtual'] is not None], window)
            if found and (best is None or found['rate'] > best['rate']):
                best = found
        result['busiest_1x'][str(window)] = best
    for part in parts:
        found = busiest_window([(s['t'], s['offset']) for s in part], real_window)
        if found and (result['busiest_running'] is None or found['rate'] > result['busiest_running']['rate']):
            result['busiest_running'] = found
    return result


def recommend(result, window=60, factor=60):
    """Burst-test input: normal = busiest 1x rate over `window` virtual seconds (the 1x average when
    no window of that length was observed), burst = factor x normal."""
    best = result['busiest_1x'].get(str(window))
    normal, basis = (best['rate'], f'busiest {window} virtual s') if best else (result['average_1x_rate'], '1x average')
    if not normal:
        return None
    return {'normal_rate': normal, 'factor': factor, 'burst_rate': factor * normal, 'burst_events_10s': 10 * factor * normal,
            'basis': basis}


def cell(value, digits=2):
    return '-' if value is None else f'{value:.{digits}f}'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url', help='feed URL; defaults to satellite.url of the configuration')
    parser.add_argument('--token-file', help='file holding the group token; defaults to external.satellite_token')
    parser.add_argument('--minutes', type=float, default=30)
    parser.add_argument('--interval', type=float, default=0.5)
    parser.add_argument('--virtual-windows', default='60,300,600', help='busiest-window sizes in virtual seconds')
    parser.add_argument('--real-window', type=float, default=10)
    parser.add_argument('--factor', type=float, default=60)
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    if args.token_file:
        with open(args.token_file) as handle:
            token = handle.read().strip()
    else:
        token = (cfg.get('external') or {}).get('satellite_token', '')
    if not token:
        print('No satellite group token: set external.satellite_token or pass --token-file.', file=sys.stderr)
        return 2
    base = (args.base_url or cfg['satellite']['url']).rstrip('/')
    windows = [int(w) for w in args.virtual_windows.split(',') if w.strip()]
    report = Report(secrets=[token])
    directory = new_output_dir('p4-feed-rate', args.output)
    headers = {'User-Agent': USER_AGENT, 'Accept': 'application/json'}
    report.line(f'Polling {base}/v1/session every {args.interval:g}s for {args.minutes:g} minute(s); read-only.')
    samples, gaps = [], []
    started = time.monotonic()
    failures, last_note = 0, started
    while time.monotonic()-started < args.minutes*60:
        tick = time.monotonic()
        status, body, _, _ = http(base, '/v1/session', token=token, headers=headers, timeout=5)
        sample = sample_from(body, tick, time.time()) if status == 200 else None
        if sample:
            samples.append(sample)
            failures = 0
        else:
            failures += 1
            gaps.append({'t': round(tick-started, 1), 'status': status})
        if tick-last_note >= 60 and samples:
            last = samples[-1]
            report.line(f"+{tick-started:.0f}s offset={last['offset']} speed={last['speed']} profile={last['profile']} "
                        f"gaps={len(gaps)}")
            last_note = tick
        pause = args.interval if not failures else min(10.0, args.interval*2**failures)
        time.sleep(max(0.0, pause-(time.monotonic()-tick)))
    if len(samples) < 2:
        report.line('Fewer than two readable session samples: no rate can be computed.')
        report.data.update(samples=samples, gaps=gaps)
        report.save(directory)
        return 2
    result = analyze(samples, windows, args.real_window)
    advice = recommend(result, 60, args.factor)
    report.line()
    report.line(f"{result['samples']} samples in {result['segments']} segment(s); speeds seen {result['speeds']}; "
                f"profiles {result['profiles']}; {result['offsets']} offsets over {result['real_seconds']:.0f} real s "
                f"= {result['virtual_seconds']/3600:.1f} virtual h; {len(gaps)} failed polls")
    rows = [['running speed, average', cell(result['average_running_rate']), f"{result['real_seconds']:.0f} real s", ''],
            ['running speed, busiest', cell((result['busiest_running'] or {}).get('rate')), f'{args.real_window:g} real s',
             f"{(result['busiest_running'] or {}).get('events', '-')} offsets"],
            ['1x, average', cell(result['average_1x_rate'], 4), f"{result['virtual_seconds']:.0f} virtual s", '']]
    for window in windows:
        best = result['busiest_1x'].get(str(window))
        rows.append([f'1x, busiest', cell((best or {}).get('rate'), 4), f'{window} virtual s',
                     f"{best['events']} offsets" if best else 'not observed'])
    if result['average_1x_rate'] is not None:
        rows.append([f'{args.factor:g}x of the 1x average', cell(args.factor*result['average_1x_rate']), 'derived', ''])
    report.table(['measure', 'offsets/s', 'window', 'note'], rows)
    if advice:
        report.line(f"Burst-test input ({advice['basis']}): normal {advice['normal_rate']:.3f}/s, burst "
                    f"{advice['burst_rate']:.1f}/s, {advice['burst_events_10s']:.0f} events in 10 s.")
    report.line('Offsets include heartbeats that carry an offset; the figures are upper bounds for detections.')
    report.data.update(base_url=base, interval=args.interval, minutes=args.minutes, analysis=result,
                       recommendation=advice, gaps=gaps, samples=samples,
                       finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    report.save(directory)
    return 0


if __name__ == '__main__':
    sys.exit(main())
