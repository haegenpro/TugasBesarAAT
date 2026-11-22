"""Pure-logic checks for the P4-4 burst tools; no cluster, token or external network is used."""
import hashlib
import http.client
import json
import threading
import unittest

import p4_burst_cluster as burst
import p4_feed_rate as rate
import p4_fixture_feed as fixture


def samples(points, speed=60, profile='practice'):
    """[(real t, offset, virtual epoch)] -> feed-rate samples."""
    return [{'t': t, 'wall': t, 'offset': o, 'virtual': v, 'speed': speed, 'profile': profile} for t, o, v in points]


class FeedRateTests(unittest.TestCase):
    def test_rfc3339_variants(self):
        self.assertEqual(rate.parse_rfc3339('1970-01-01T00:01:00Z'), 60.0)
        self.assertAlmostEqual(rate.parse_rfc3339('1970-01-01T00:01:00.123456789Z'), 60.123456, places=5)
        self.assertEqual(rate.parse_rfc3339('1970-01-01T07:00:00+07:00'), 0.0)
        self.assertIsNone(rate.parse_rfc3339('not a time'))
        self.assertIsNone(rate.parse_rfc3339(None))

    def test_session_sample_needs_an_integer_offset(self):
        good = rate.sample_from(json.dumps({'latest_offset': 5, 'virtual_now': '1970-01-01T00:00:10Z', 'speed': 60}), 1.0, 2.0)
        self.assertEqual((good['offset'], good['virtual'], good['speed']), (5, 10.0, 60))
        self.assertIsNone(rate.sample_from('{"latest_offset":"5"}', 1, 1))
        self.assertIsNone(rate.sample_from('oops', 1, 1))

    def test_segments_split_on_reset_and_speed_change(self):
        data = samples([(0, 10, 0), (1, 20, 60), (2, 5, 120)]) + samples([(3, 8, 180)], speed=1)
        parts = rate.segments(data)
        self.assertEqual([len(p) for p in parts], [2, 1, 1])

    def test_averages_and_busiest_window_find_a_spike(self):
        # 60x feed: 1 real second = 60 virtual seconds; 2 offsets per second except a 5 s spike of 50/s.
        points, offset = [], 0
        for t in range(0, 61):
            points.append((float(t), offset, 1000.0 + 60 * t))
            offset += 50 if 20 <= t < 25 else 2
        result = rate.analyze(samples(points), (60, 300), real_window=5)
        self.assertEqual(result['segments'], 1)
        self.assertAlmostEqual(result['average_running_rate'], points[-1][1] / 60.0)
        self.assertAlmostEqual(result['average_1x_rate'], points[-1][1] / 3600.0)
        self.assertAlmostEqual(result['busiest_running']['rate'], 50.0)
        self.assertAlmostEqual(result['busiest_1x']['300']['rate'], 50.0 / 60.0)
        advice = rate.recommend(result, window=60, factor=60)
        self.assertAlmostEqual(advice['normal_rate'], 50.0 / 60.0)
        self.assertAlmostEqual(advice['burst_rate'], 50.0)
        self.assertAlmostEqual(advice['burst_events_10s'], 500.0)

    def test_busiest_window_needs_a_full_window(self):
        self.assertIsNone(rate.busiest_window([(0, 0), (5, 10)], 10))
        self.assertEqual(rate.busiest_window([(0, 0), (10, 10), (20, 40)], 10)['rate'], 3.0)


class FixtureScheduleTests(unittest.TestCase):
    def setUp(self):
        self.now = [0.0]
        self.feed = fixture.Fixture('token', clock=lambda: self.now[0], wall=1000.0)

    def test_steady_rate_publishes_whole_pairs(self):
        self.feed.set_rate(10)
        self.now[0] = 1.05
        self.assertEqual(self.feed.schedule.available_events(self.now[0]), 10)
        self.now[0] = 1.15
        self.assertEqual(self.feed.schedule.available_events(self.now[0]) % 2, 0)

    def test_burst_counts_and_offsets(self):
        self.feed.set_rate(2)
        self.now[0] = 10.0
        published = self.feed.schedule.available_events(10.0)
        self.feed.start_burst(2, 60, 10, 2)
        info = self.feed.burst
        self.assertEqual(info['first_offset'], published + 4)          # 2 s at 2 events/s before the burst
        self.assertEqual(info['last_offset'] - info['first_offset'] + 1, 1200)  # 600 x the normal rate
        self.assertEqual(self.feed.schedule.speed_at(13.0), 60)
        self.assertEqual(self.feed.schedule.speed_at(23.0), 1)
        self.now[0] = 15.0
        session = self.feed.session()
        self.assertEqual(session['phase'], 'burst')
        self.assertEqual(session['burst_end_elapsed_seconds'], 12)

    def test_the_past_never_changes(self):
        self.feed.set_rate(4)
        self.now[0] = 5.0
        before = [fixture.event_payload(o, self.feed.schedule, self.feed.mono0, self.feed.wall0) for o in range(10)]
        self.feed.start_burst(4, 60, 3, 0)
        self.now[0] = 9.0
        self.feed.stop()
        after = [fixture.event_payload(o, self.feed.schedule, self.feed.mono0, self.feed.wall0) for o in range(10)]
        self.assertEqual(before, after)

    def test_pair_layout_round_trips_and_keeps_pairs_apart(self):
        for k in (0, 1, 563, 564, 12345, fixture.ROWS * fixture.COLS - 1):
            lat, lon = fixture.pair_position(k)
            self.assertEqual(fixture.pair_index(lat, lon, 'N20'), k)
            self.assertEqual(fixture.pair_index(f'{float(lat) + fixture.PAIR_GAP:.5f}', lon, 'N21'), k)
        a, b = fixture.pair_position(0), fixture.pair_position(1)
        self.assertGreaterEqual(abs(float(a[0]) - float(b[0])), 0.03 - 1e-9)
        with self.assertRaises(ValueError):
            fixture.pair_position(fixture.ROWS * fixture.COLS)

    def test_payload_has_what_the_normalizer_reads(self):
        self.feed.set_rate(2)
        self.now[0] = 3.0
        first = fixture.event_payload(0, self.feed.schedule, self.feed.mono0, self.feed.wall0)
        second = fixture.event_payload(1, self.feed.schedule, self.feed.mono0, self.feed.wall0)
        self.assertEqual(first['offset'], 0)
        for key in ('satellite', 'acq_date', 'acq_time', 'latitude', 'longitude', 'confidence', 'frp'):
            self.assertIn(key, first['detection'])
        self.assertEqual((first['detection']['satellite'], second['detection']['satellite']), ('N20', 'N21'))
        self.assertEqual(first['detection']['acq_time'], second['detection']['acq_time'])
        self.assertRegex(first['detection']['acq_time'], r'^\d{4}$')
        self.assertTrue(first['virtual_now'].endswith('Z'))
        frame = fixture.sse_frame(7, {'offset': 7}).decode()
        self.assertTrue(frame.startswith('id: 7\nevent: detection\ndata: {"offset":7}'))


class FixtureServerTests(unittest.TestCase):
    def setUp(self):
        self.server = fixture.make_server('secret', '127.0.0.1', 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def call(self, method, path, body=None, token='secret'):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        headers = {'Authorization': 'Bearer ' + token} if token else {}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        connection.request(method, path, json.dumps(body) if body is not None else None, headers)
        return connection, connection.getresponse()

    def test_token_health_stream_and_ahead_of_feed(self):
        _, response = self.call('GET', '/health', token=None)
        self.assertEqual(response.status, 200)
        _, response = self.call('GET', '/v1/session', token=None)
        self.assertEqual(response.status, 401)
        _, response = self.call('POST', '/admin/rate', {'rate': 2000})
        self.assertEqual(response.status, 200)
        self.server.fixture.schedule.set_from(self.server.fixture.clock(), [(None, 1000.0, 1)])
        connection, response = self.call('GET', '/v1/stream?from=0')
        self.assertEqual(response.status, 200)
        ids = []
        while len(ids) < 3:
            line = response.fp.readline().decode()
            if line.startswith('id: '):
                ids.append(int(line[4:]))
        connection.close()
        self.assertEqual(ids, [0, 1, 2])
        _, response = self.call('GET', '/v1/stream?from=100000000')
        self.assertEqual(response.status, 409)
        _, response = self.call('POST', '/admin/burst', {'normal_rate': -1})
        self.assertEqual(response.status, 400)


class BurstLogicTests(unittest.TestCase):
    def test_operation_id_follows_batch_identity(self):
        expected = 'fire-batch-v1:' + hashlib.sha256(b'["satellite-p4-x",5]').hexdigest()
        self.assertEqual(burst.operation_id('satellite-p4-x', 5), expected)

    def test_lag_tolerance_recovery_and_peak(self):
        self.assertEqual(burst.lag(10, None), 11)
        self.assertEqual(burst.lag(10, 12), 0)
        baseline = [{'lag_applied': 3}, {'lag_applied': 5}]
        self.assertEqual(burst.tolerance(baseline, 'lag_applied'), 5)
        self.assertEqual(burst.tolerance([], 'lag_applied'), 2)
        run = [{'t': 10, 'lag_applied': 900}, {'t': 12, 'lag_applied': 800}, {'t': 40, 'lag_applied': 4}, {'t': 41, 'lag_applied': 3}]
        self.assertEqual(burst.recovery(run, 12, 'lag_applied', 5), 28)
        self.assertIsNone(burst.recovery(run, 12, 'lag_applied', 1))
        self.assertEqual(burst.peak(run, 'lag_applied'), 900)

    def test_percentile_is_nearest_rank(self):
        self.assertEqual(burst.percentile(list(range(1, 101)), .95), 95)
        self.assertEqual(burst.percentile([7], .5), 7)
        self.assertIsNone(burst.percentile([], .5))

    def test_latency_join_reports_missing_positions(self):
        ns = 'satellite-p4-x'
        ingest = [(0, 100.0, 99.5), (1, 101.0, 100.0), (2, 102.0, None)]
        canonical = [(burst.operation_id(ns, 0), 103.0), (burst.operation_id(ns, 2), 110.0)]
        rows, missing = burst.join_latencies(ns, ingest, canonical)
        self.assertEqual(missing, [1])
        self.assertEqual(rows[0]['available_to_applied'], 3.5)
        self.assertIsNone(rows[1]['available_to_received'])
        summary = burst.latency_summary(rows, 'received_to_applied')
        self.assertEqual((summary['count'], summary['max']), (2, 8.0))

    def test_oracle_finds_missing_duplicate_merged_and_bad_fires(self):
        def keys(k, satellites=('N20', 'N21')):
            lat, lon = fixture.pair_position(k)
            out = []
            for satellite in satellites:
                shown = lat if satellite == 'N20' else f'{float(lat) + fixture.PAIR_GAP:.5f}'
                out.append(f'{satellite}|2026-11-22|0100|{shown}|{lon}')
            return out
        clean = [('a', '2', keys(0)), ('b', '2', keys(1))]
        result = burst.pair_oracle(clean, 2)
        self.assertFalse(any(result[k] for k in ('missing', 'duplicated', 'merged', 'bad_count', 'beyond')))
        messy = [('a', '2', keys(0)), ('a2', '2', keys(0)), ('m', '4', keys(1) + keys(2)), ('x', '1', keys(3, ('N20',))),
                 ('live', '3', ['N20|2026-11-22|0100|abc|def'])]
        result = burst.pair_oracle(messy, 5)
        self.assertEqual(result['duplicated'], [0])
        self.assertEqual(result['merged'], ['m'])
        self.assertEqual(result['bad_count'], ['x'])
        self.assertEqual(result['missing'], [1, 2, 4])
        self.assertEqual(result['foreign'], ['live'])

    def test_verdicts(self):
        result = {'saw_burst': True, 'generation_utilization': 0.1, 'probe_failures': 0, 'probe_total': 100,
                  'recovery': {'lag_received': 3.0, 'lag_applied': 45.0}, 'tolerance': {'lag_received': 2, 'lag_applied': 40},
                  'peak': {'lag_received': 100, 'lag_applied': 9000}, 'total_pairs': 10,
                  'latency': {'joined': 20, 'missing': []},
                  'oracle': {'missing': [], 'duplicated': [], 'merged': [], 'bad_count': [], 'foreign': ['live'], 'beyond': []}}
        self.assertEqual({s for s, _ in burst.verdicts(result, 60)}, {'PASS'})
        result['recovery']['lag_applied'] = None
        self.assertIn('FAIL', {s for s, _ in burst.verdicts(result, 60)})
        result['saw_burst'] = False
        self.assertEqual(burst.verdicts(result, 60)[0][0], 'INVALID')


if __name__ == '__main__':
    unittest.main()
