"""Pure-logic checks for baseline_capture; no cluster, token or network is used."""
import unittest

import baseline_capture as base
from evidence_metrics import parse_metrics

BEFORE = parse_metrics("""app_ingest_commit_seconds_append_sum 1
app_ingest_commit_seconds_append_count 100
app_ingest_commit_seconds_append_bucket{le="0.005"} 10
app_ingest_commit_seconds_append_bucket{le="0.01"} 60
app_ingest_commit_seconds_append_bucket{le="0.025"} 100
app_ingest_commit_seconds_append_bucket{le="+Inf"} 100
app_ingest_batches_total_append 100
app_ingest_batch_records_total_append 100
app_ingest_batch_bytes_total_append 5000
app_ingest_pool_acquire_total 100
app_ingest_pool_acquire_wait_seconds_total 0.5
app_ingest_pool_empty_acquire_total 2
""")
AFTER = parse_metrics("""app_ingest_commit_seconds_append_sum 3
app_ingest_commit_seconds_append_count 300
app_ingest_commit_seconds_append_bucket{le="0.005"} 10
app_ingest_commit_seconds_append_bucket{le="0.01"} 160
app_ingest_commit_seconds_append_bucket{le="0.025"} 290
app_ingest_commit_seconds_append_bucket{le="+Inf"} 300
app_ingest_batches_total_append 150
app_ingest_batch_records_total_append 600
app_ingest_batch_bytes_total_append 30000
app_ingest_pool_acquire_total 400
app_ingest_pool_acquire_wait_seconds_total 2.5
app_ingest_pool_empty_acquire_total 12
app_ingest_backlog_raw_outbox 7
app_ingest_backlog_raw_outbox 9
""")


class BaselineCapture(unittest.TestCase):
    def test_redact_masks_credentials_but_keeps_the_rest(self):
        out = base.redact({'image_tag': 'm2-d1', 'external': {'satellite_token': 'abc', 'openrouter_api_key': 'k', 'summary_model': 'm'},
                           'profile': 'minikube', 'password': '', 'list': [{'client_secret': 'x', 'name': 'n'}]})
        self.assertEqual(out['external'], {'satellite_token': '***', 'openrouter_api_key': '***', 'summary_model': 'm'})
        self.assertEqual(out['image_tag'], 'm2-d1')
        self.assertEqual(out['password'], '')
        self.assertEqual(out['list'], [{'client_secret': '***', 'name': 'n'}])

    def test_histogram_stats_use_the_increase_between_scrapes(self):
        stats = base.histogram_stats(BEFORE, AFTER, 'app_ingest_commit_seconds_append')
        self.assertEqual(stats['count'], 200)
        self.assertAlmostEqual(stats['mean_s'], 0.01)
        self.assertEqual(stats['p50_s'], 0.01)
        self.assertEqual(stats['p95_s'], 0.025)
        self.assertEqual(base.histogram_stats(AFTER, AFTER, 'app_ingest_commit_seconds_append'), {'count': 0})

    def test_summary_reports_batch_size_rates_pool_wait_and_backlog_peak(self):
        series = [AFTER, parse_metrics('app_ingest_backlog_raw_outbox 30\n')]
        out = base.summarise(BEFORE, AFTER, series, 10)
        append = out['batches']['append']
        self.assertEqual(append['batches'], 50)
        self.assertEqual(append['records'], 500)
        self.assertAlmostEqual(append['records_per_batch'], 10)
        self.assertAlmostEqual(append['records_per_s'], 50)
        self.assertAlmostEqual(append['bytes_per_batch'], 500)
        self.assertAlmostEqual(out['pool']['acquire_wait_s'], 2.0)
        self.assertEqual(out['pool']['empty_acquires'], 10)
        self.assertEqual(out['peaks']['app_ingest_backlog_raw_outbox'], 30)
        self.assertIn('app_ingest_commit_seconds_append', out['histograms'])

    def test_expected_results_pass_and_fail_visibly(self):
        items = [{'source': 'SATELIT', 'namespace': 'p', 'received': 100, 'applied': 100},
                 {'source': 'SENSOR_PANTAI', 'namespace': 'q', 'received': 50, 'applied': 40}]
        ok = base.check_expected(items, {'progress': {'SATELIT/p': {'received': 100, 'applied': 100}}})
        self.assertEqual([s for s, _ in ok], ['PASS', 'PASS'])
        bad = base.check_expected(items, {'progress': {'SATELIT/p': {'applied': 99}, 'MISSING/x': {'received': 1}}, 'max_unapplied': 5})
        failed = [text for state, text in bad if state == 'FAIL']
        self.assertTrue(any(t.startswith('SATELIT/p applied: expected 99') for t in failed))
        self.assertTrue(any(t.startswith('MISSING/x') for t in failed))
        self.assertTrue(any(t.startswith('SENSOR_PANTAI/q unapplied=10') for t in failed))
        self.assertFalse(any(t.startswith('SATELIT/p unapplied') for t in failed))

    def test_generator_utilization_is_a_fraction_of_the_machine(self):
        self.assertEqual(base.generator_usage(30, 60, 4)['utilization'], 0.125)
        self.assertIsNone(base.generator_usage(1, 0, 4))


if __name__ == '__main__':
    unittest.main()
