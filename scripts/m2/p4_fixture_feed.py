#!/usr/bin/env python3
"""Controlled satellite feed for the P4-4 burst test, speaking the event format our ingestion reads.

Criteria: P4-4 (controlled input: a steady normal rate, then a burst of --factor x that rate for a
few seconds, then the normal rate again, so recovery after the burst can be timed).

Standard library only: p4_burst_cluster.py ships this file in a ConfigMap and runs it in the
cluster with the storage image's Python. It serves the same interface as the assistants' feed:
  GET  /v1/session              latest_offset, speed, virtual_now, plus the burst bookkeeping below
  GET  /v1/stream?from=N        SSE from offset N (inclusive): id, event: detection, data: JSON
  POST /admin/rate   {rate}     steady state at `rate` events per second (0 pauses publication)
  POST /admin/burst  {normal_rate, factor, seconds, before}   normal, factor x normal, normal
  POST /admin/stop   {}         stop publishing (already published offsets stay available)
  GET  /health                  no token needed
Every other request needs `Authorization: Bearer $P4_FIXTURE_TOKEN`.

Publication is computed from the server's monotonic clock, never timed by sleeps, so the number of
available events at any moment is exact. Events come in pairs (satellites N20 and N21, 0.001 deg
apart, the same minute), and pairs sit 0.03 deg apart on a grid, so every pair forms exactly one
fire and no two pairs merge; pair_index() inverts the layout for the oracle. A payload depends only
on its offset, so a replay after a reconnect sends the same bytes. acq_time follows the feed's
virtual clock, so no detection is ever late. available_at (epoch seconds) is when the event became
available, for end-to-end latency.
"""
import argparse
import datetime
import json
import math
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ROWS, COLS = 564, 1530          # latitude -10.95..5.94, longitude 95.05..140.92, 0.03 deg apart
LAT0, LON0, STEP, PAIR_GAP = -10.95, 95.05, 0.03, 0.001
MAX_EVENTS_PER_WRITE = 1000
MAX_PROJECTED_EVENTS = 2000000


class Schedule:
    """Piecewise-linear publication plan in pairs per second, with a virtual clock running at `speed`.
    The past is never rewritten, so offsets and payloads stay fixed once published."""

    def __init__(self, virtual_start):
        self.segments = [{'start': 0.0, 'pairs': 0.0, 'rate': 0.0, 'speed': 1.0, 'virtual': float(virtual_start)}]

    def _segment(self, t):
        current = self.segments[0]
        for segment in self.segments:
            if segment['start'] <= t:
                current = segment
            else:
                break
        return current

    def pairs_at(self, t):
        s = self._segment(t)
        return s['pairs'] + s['rate'] * max(0.0, t - s['start'])

    def virtual_at(self, t):
        s = self._segment(t)
        return s['virtual'] + s['speed'] * max(0.0, t - s['start'])

    def speed_at(self, t):
        return self._segment(t)['speed']

    def rate_at(self, t):
        """Events per second at t."""
        return 2 * self._segment(t)['rate']

    def available_events(self, t):
        """Events published by t; always whole pairs."""
        return 2 * int(math.floor(self.pairs_at(t) + 1e-9))

    def time_of_pair(self, k):
        """When pair k (0-based) was or will be published; None if the plan never reaches it."""
        need = k + 1
        for index, s in enumerate(self.segments):
            following = self.segments[index + 1]['start'] if index + 1 < len(self.segments) else None
            if s['rate'] <= 0:
                continue
            reach = None if following is None else s['pairs'] + s['rate'] * (following - s['start'])
            if need >= s['pairs'] - 1e-9 and (reach is None or need <= reach + 1e-9):
                return s['start'] + max(0.0, need - s['pairs']) / s['rate']
        return None

    def set_from(self, t, plan):
        """Replace everything after t with plan = [(duration or None, pairs per second, speed), ...]."""
        pairs, virtual = self.pairs_at(t), self.virtual_at(t)
        self.segments = [s for s in self.segments if s['start'] < t] or [self.segments[0]]
        start = t
        for duration, rate, speed in plan:
            self.segments.append({'start': start, 'pairs': pairs, 'rate': float(rate), 'speed': float(speed),
                                  'virtual': virtual})
            if duration is None:
                break
            pairs += rate * duration
            virtual += speed * duration
            start += duration


def pair_position(k):
    """(latitude, longitude) strings of the first detection of pair k."""
    if k < 0 or k >= ROWS * COLS:
        raise ValueError('pair index outside the fixture grid')
    row, col = k % ROWS, k // ROWS
    return f'{LAT0 + STEP * row:.5f}', f'{LON0 + STEP * col:.5f}'


def pair_index(latitude, longitude, satellite):
    """Inverse of the layout: the pair a detection belongs to (N21 is the second member)."""
    lat = float(latitude) - (PAIR_GAP if satellite == 'N21' else 0.0)
    row = int(round((lat - LAT0) / STEP))
    col = int(round((float(longitude) - LON0) / STEP))
    return col * ROWS + row


def rfc3339(epoch):
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def event_payload(offset, schedule, mono0, wall0):
    """The JSON object of the event at `offset`; deterministic for a given offset."""
    pair, member = divmod(offset, 2)
    at = schedule.time_of_pair(pair)
    virtual = schedule.virtual_at(at)
    minute = datetime.datetime.fromtimestamp(virtual - virtual % 60, datetime.timezone.utc)
    latitude, longitude = pair_position(pair)
    if member:
        latitude = f'{float(latitude) + PAIR_GAP:.5f}'
    return {'offset': offset, 'virtual_now': rfc3339(virtual), 'available_at': round(wall0 + (at - mono0), 6),
            'detection': {'satellite': 'N21' if member else 'N20', 'acq_date': minute.strftime('%Y-%m-%d'),
                          'acq_time': minute.strftime('%H%M'), 'latitude': latitude, 'longitude': longitude,
                          'confidence': 'n', 'frp': 2.0}}


def sse_frame(offset, data):
    return f'id: {offset}\nevent: detection\ndata: {json.dumps(data, separators=(",", ":"))}\n\n'.encode()


class Fixture:
    """Shared state of one fixture process. `clock` returns seconds since start (monotonic)."""

    def __init__(self, token, clock=None, wall=None):
        self.token = token
        self.started = time.monotonic()
        self.clock = clock or (lambda: time.monotonic() - self.started)
        self.wall0 = wall if wall is not None else time.time()
        self.mono0 = self.clock()
        self.schedule = Schedule(self.wall0)
        self.lock = threading.Condition()
        self.phase = 'idle'
        self.trigger = None
        self.burst = {}
        self.normal_rate = 0.0
        self.connections = 0
        self.last_from = None
        self.format_seconds = 0.0
        self.format_at_trigger = 0.0

    def session(self):
        now = self.clock()
        available = self.schedule.available_events(now)
        elapsed = now - self.trigger if self.trigger is not None else 0.0
        phase = self.phase
        if self.trigger is not None and self.phase == 'burst-plan':
            before, seconds = self.burst['before'], self.burst['seconds']
            phase = 'before' if elapsed < before else ('burst' if elapsed < before + seconds else 'after')
        busy = None
        if self.trigger is not None and elapsed > 0:
            busy = (self.format_seconds - self.format_at_trigger) / elapsed
        return {'group': 'p4-fixture', 'profile': 'p4-fixture', 'speed': self.schedule.speed_at(now),
                'virtual_now': rfc3339(self.schedule.virtual_at(now)), 'latest_offset': available - 1,
                'published_count': available, 'phase': phase, 'rate': self.schedule.rate_at(now),
                'normal_rate': self.normal_rate, 'elapsed_seconds': elapsed,
                'burst_start_elapsed_seconds': self.burst.get('before'),
                'burst_end_elapsed_seconds': self.burst.get('end'),
                'burst_first_offset': self.burst.get('first_offset'), 'burst_last_offset': self.burst.get('last_offset'),
                'burst_rate': self.burst.get('rate'), 'total_pairs': available // 2, 'connections': self.connections,
                'last_from': self.last_from, 'wall_now': time.time(), 'generation_utilization': busy}

    def set_rate(self, rate):
        now = self.clock()
        self.schedule.set_from(now, [(None, rate / 2.0, 1)])
        self.normal_rate, self.phase, self.trigger, self.burst = rate, ('normal' if rate > 0 else 'stopped'), None, {}

    def start_burst(self, normal, factor, seconds, before):
        now = self.clock()
        self.schedule.set_from(now, [(before, normal / 2.0, 1), (seconds, factor * normal / 2.0, factor),
                                     (None, normal / 2.0, 1)])
        self.normal_rate, self.phase, self.trigger = normal, 'burst-plan', now
        self.format_at_trigger = self.format_seconds
        self.burst = {'before': before, 'seconds': seconds, 'end': before + seconds, 'rate': factor * normal,
                      'factor': factor, 'first_offset': self.schedule.available_events(now + before),
                      'last_offset': self.schedule.available_events(now + before + seconds) - 1}

    def stop(self):
        now = self.clock()
        self.schedule.set_from(now, [(None, 0.0, 1)])
        self.phase = 'stopped'


def number(body, key, low, high, default=None):
    value = body.get(key, default)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not low <= value <= high:
        raise ValueError(f'{key} must be a number between {low} and {high}')
    return float(value)


class Handler(BaseHTTPRequestHandler):
    server_version = 'p4-fixture'

    def log_message(self, *_):
        pass

    def reply(self, status, value):
        raw = json.dumps(value).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def authorized(self):
        fixture = self.server.fixture
        if self.headers.get('Authorization') == 'Bearer ' + fixture.token:
            return True
        self.reply(401, {'error': 'unauthorized'})
        return False

    def do_GET(self):
        fixture = self.server.fixture
        url = urlparse(self.path)
        if url.path == '/health':
            return self.reply(200, {'ok': True})
        if not self.authorized():
            return None
        if url.path == '/v1/session':
            with fixture.lock:
                return self.reply(200, fixture.session())
        if url.path != '/v1/stream':
            return self.reply(404, {'error': 'not_found'})
        try:
            cursor = int(parse_qs(url.query).get('from', ['0'])[0])
            if cursor < 0:
                raise ValueError
        except ValueError:
            return self.reply(400, {'error': 'bad_request'})
        with fixture.lock:
            latest = fixture.schedule.available_events(fixture.clock()) - 1
            if cursor > latest + 1:
                # The pipeline is ahead of this feed: the fixture restarted while progress was kept.
                return self.reply(409, {'error': 'offset_ahead_of_feed', 'latest_offset': latest})
            fixture.connections += 1
            fixture.last_from = cursor
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        try:
            while True:
                with fixture.lock:
                    latest = fixture.schedule.available_events(fixture.clock()) - 1
                    upto = min(latest, cursor + MAX_EVENTS_PER_WRITE - 1)
                    started = time.perf_counter()
                    chunk = b''.join(sse_frame(o, event_payload(o, fixture.schedule, fixture.mono0, fixture.wall0))
                                     for o in range(cursor, upto + 1))
                    fixture.format_seconds += time.perf_counter() - started
                wrote = upto >= cursor
                cursor = upto + 1 if wrote else cursor
                self.wfile.write(chunk + b': heartbeat\n\n')
                self.wfile.flush()
                with fixture.lock:
                    fixture.lock.wait(0.02 if wrote else 0.25)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return None

    def do_POST(self):
        fixture = self.server.fixture
        if not self.authorized():
            return None
        length = int(self.headers.get('Content-Length') or 0)
        if length > 4096:
            return self.reply(400, {'error': 'body too large'})
        try:
            body = json.loads(self.rfile.read(length) or b'{}')
            if not isinstance(body, dict):
                raise ValueError('body must be a JSON object')
            with fixture.lock:
                if self.path == '/admin/rate':
                    fixture.set_rate(number(body, 'rate', 0, 100000))
                elif self.path == '/admin/burst':
                    normal = number(body, 'normal_rate', 0.001, 100000)
                    factor = number(body, 'factor', 1, 1000, 60)
                    seconds = number(body, 'seconds', 1, 600, 10)
                    before = number(body, 'before', 0, 600, 2)
                    if normal * factor * seconds + normal * before > MAX_PROJECTED_EVENTS:
                        raise ValueError('burst would publish too many events')
                    fixture.start_burst(normal, factor, seconds, before)
                elif self.path == '/admin/stop':
                    fixture.stop()
                else:
                    return self.reply(404, {'error': 'not_found'})
                fixture.lock.notify_all()
                return self.reply(200, fixture.session())
        except ValueError as error:
            return self.reply(400, {'error': str(error)})


def make_server(token, host='0.0.0.0', port=9000, fixture=None):
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.fixture = fixture or Fixture(token)
    return server


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=int(os.environ.get('P4_FIXTURE_PORT', '9000')))
    parser.add_argument('--rate', type=float, default=float(os.environ.get('P4_FIXTURE_INITIAL_RATE', '0')))
    args = parser.parse_args(argv)
    token = os.environ.get('P4_FIXTURE_TOKEN', '')
    if not token:
        print('P4_FIXTURE_TOKEN is required', file=sys.stderr)
        return 2
    server = make_server(token, args.host, args.port)
    if args.rate > 0:
        server.fixture.set_rate(args.rate)
    print(f'p4 fixture feed on {args.host}:{args.port}', flush=True)
    server.serve_forever()
    return 0


if __name__ == '__main__':
    sys.exit(main())
