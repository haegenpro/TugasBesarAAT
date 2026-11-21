"""Pure-logic checks for the observation tools; no cluster, token or network is used."""
import contextlib
import io
import json
import math
import os
from pathlib import Path
import tempfile
import unittest

import evidence_metrics as metrics
import evidence_report as report_module
import p1_analytics_compare as analytics
import p3_field_report_ledger as ledger
import p3_ops_access_matrix as access
import p3_quarantine_payload as quarantine
import p3_replay_flow as replay_flow
import p3_schema_behavior as schema
import p4_ingestion_progress as progress
import p5_sensor_fault_probe as sensor
import p5_warning_slo_snapshot as slo

SAMPLE = """# HELP x demo
app_coastal_slo_pending 3
app_coastal_slo_on_time_ratio NaN
app_coastal_observations_breaker_state 2
app_http_requests_total{service="a",route="/hazards"} 12
app_http_requests_total{service="b",route="/hazards"} 8
app_x_bucket{le="+Inf"} +Inf
broken line without number
"""


class MetricsTests(unittest.TestCase):
    def test_parser_reads_labels_nan_inf_and_skips_noise(self):
        parsed = metrics.parse_metrics(SAMPLE)
        self.assertEqual(metrics.value(parsed, 'app_coastal_slo_pending'), 3.0)
        self.assertTrue(math.isnan(metrics.value(parsed, 'app_coastal_slo_on_time_ratio')))
        self.assertEqual(parsed['app_http_requests_total'][1][0], {'service': 'b', 'route': '/hazards'})
        self.assertEqual(metrics.value(parsed, 'app_x_bucket'), math.inf)
        self.assertNotIn('broken', ''.join(parsed))
        self.assertIsNone(metrics.value(parsed, 'absent'))
        self.assertTrue(metrics.is_missing(float('nan')) and metrics.is_missing(None) and not metrics.is_missing(0.0))

    def test_select_and_rate(self):
        before = metrics.parse_metrics('app_a_request_seconds_count 10\napp_b_request_seconds_count 5\n')
        after = metrics.parse_metrics('app_a_request_seconds_count 30\napp_b_request_seconds_count 5\n')
        rates = metrics.rate_between(before, after, 10, r'app_.+_request_seconds_count')
        self.assertEqual(rates, {'app_a_request_seconds_count': 2.0, 'app_b_request_seconds_count': 0.0})
        self.assertEqual(metrics.rate_between(before, after, 0, '.*'), {})
        self.assertEqual(set(metrics.select(after, r'app_a_.*')), {'app_a_request_seconds_count'})


class ReportTests(unittest.TestCase):
    def test_table_aligns_columns(self):
        lines = report_module.format_table(['key', 'n'], [['a', 1], ['longer', 22]])
        self.assertEqual(lines[0], 'key     n')
        self.assertEqual(lines[1], '------  --')
        self.assertEqual(lines[2], 'a       1')

    def test_secrets_are_redacted_from_printed_and_saved_text(self):
        report = report_module.Report(secrets=['s3cret-value'])
        with contextlib.redirect_stdout(io.StringIO()) as out:
            report.line('token s3cret-value was used')
        self.assertNotIn('s3cret-value', out.getvalue())
        self.assertIn('***', report.lines[0])

    @unittest.skipUnless(hasattr(os, 'O_NOFOLLOW'), 'private files need a POSIX host')
    def test_save_writes_private_text_and_json_without_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            report = report_module.Report(secrets=['s3cret-value'])
            with contextlib.redirect_stdout(io.StringIO()):
                report.line('hello s3cret-value')
                report.data['note'] = 's3cret-value'
                report.save(directory)
            text, raw = (Path(directory)/'result.txt').read_text(), (Path(directory)/'result.json').read_text()
            self.assertNotIn('s3cret-value', text+raw)
            self.assertEqual(json.loads(raw)['note'], '***')
            self.assertEqual((Path(directory)/'result.json').stat().st_mode & 0o077, 0)


class AnalyticsCompareTests(unittest.TestCase):
    ITEMS = [{'hazard_type': 'SEISMIC', 'severity': 'AWAS', 'area_name': 'Aceh', 'occurred_at': '2026-11-10T00:00:00Z'},
             {'hazard_type': 'SEISMIC', 'severity': 'SIAGA', 'area_name': 'Aceh', 'occurred_at': '2026-11-11T00:00:00Z'},
             {'hazard_type': 'WILDFIRE', 'severity': 'SIAGA', 'area_name': 'Riau', 'occurred_at': '2020-01-01T00:00:00Z'}]

    def test_window_and_counts(self):
        import datetime
        start = datetime.datetime(2026, 11, 1, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2026, 12, 1, tzinfo=datetime.timezone.utc)
        inside = analytics.in_window(self.ITEMS, start, end)
        self.assertEqual(len(inside), 2)
        self.assertEqual(analytics.count_by(inside, 'hazard_type'), {'SEISMIC': 2})

    def test_comparison_reports_differences_and_matches(self):
        rows, same = analytics.compare_counts({'SEISMIC': 2, 'COASTAL': 0}, {'SEISMIC': 2})
        self.assertTrue(same)
        rows, same = analytics.compare_counts({'SEISMIC': 3}, {'SEISMIC': 2, 'WILDFIRE': 1})
        self.assertFalse(same)
        self.assertIn(('SEISMIC', 3, 2, 1), rows)
        self.assertIn(('WILDFIRE', 0, 1, -1), rows)


class AccessMatrixTests(unittest.TestCase):
    @staticmethod
    def row(**overrides):
        row = {'no token': 401, 'media': 403, 'field': 403, 'internal': 200, 'media + forged role headers': 403}
        row.update(overrides)
        return row

    def test_clean_matrix_passes(self):
        self.assertTrue(access.evaluate({'GET /ops/api/failures': self.row()})['ok'])

    def test_any_other_identity_getting_through_is_a_violation(self):
        result = access.evaluate({'GET /ops/api/failures': self.row(**{'media + forged role headers': 200})})
        self.assertFalse(result['ok'])
        self.assertEqual(len(result['violations']), 1)

    def test_internal_not_succeeding_is_inconclusive_not_a_pass(self):
        result = access.evaluate({'GET /ops/api/progress': self.row(internal=503)})
        self.assertFalse(result['ok'])
        self.assertEqual(len(result['inconclusive']), 1)


class LedgerTests(unittest.TestCase):
    @staticmethod
    def entry(key, receipt, attempts=None):
        return {'key': key, 'receipt_id': receipt,
                'attempts': attempts or [{'n': 1, 'http': 202 if receipt else 503, 'receipt_id': receipt}]}

    def test_clean_run(self):
        entries = [self.entry('k1', 'r1'), self.entry('k2', 'r2')]
        summary = ledger.summarize_ledger(entries, {'k1': (200, 'processed'), 'k2': (200, 'accepted')})
        self.assertEqual(summary['receipt_status_counts'], {'processed': 1, 'accepted': 1})
        self.assertTrue(all(ok for _, ok in ledger.checks(summary, {'reports': 2, 'duplicate_results': 0})))

    def test_missing_receipt_two_receipts_per_key_and_duplicates_fail(self):
        entries = [self.entry('k1', 'r1'), self.entry('k2', 'r2', [{'n': 1, 'http': 202, 'receipt_id': 'rA'},
                                                                   {'n': 2, 'http': 202, 'receipt_id': 'r2'}]),
                   self.entry('k3', None)]
        summary = ledger.summarize_ledger(entries, {'k1': (404, None), 'k2': (200, 'processed')})
        self.assertEqual(summary['receipts_not_found'], ['k1'])
        self.assertEqual(summary['keys_with_several_receipts'], ['k2'])
        self.assertEqual(summary['never_acknowledged'], ['k3'])
        results = dict(ledger.checks(summary, {'reports': 1, 'duplicate_results': 2}))
        self.assertFalse(results['every acknowledged report has a retrievable receipt'])
        self.assertFalse(results['one receipt per Idempotency-Key'])
        self.assertFalse(results['ingestion holds every acknowledged report'])
        self.assertFalse(results['no report has more than one processing result'])

    def test_a_receipt_shared_by_two_keys_is_reported(self):
        summary = ledger.summarize_ledger([self.entry('a', 'r'), self.entry('b', 'r')],
                                          {'a': (200, 'accepted'), 'b': (200, 'accepted')})
        self.assertEqual(summary['receipts_shared_by_keys'], {'r': ['a', 'b']})

    def test_latency_summary_uses_only_202_answers(self):
        entries = [{'key': 'a', 'attempts': [{'http': 503, 'seconds': 9.0}, {'http': 202, 'seconds': 0.2}]},
                   {'key': 'b', 'attempts': [{'http': 202, 'seconds': 0.1}]},
                   {'key': 'c', 'attempts': [{'http': 202, 'seconds': 0.4}]}]
        result = ledger.latency_summary(entries)
        self.assertEqual((result['answers_202'], result['retried_keys'], result['max_seconds']), (3, 1, 0.4))
        self.assertEqual(ledger.latency_summary([{'key': 'x', 'attempts': [{'http': 0, 'seconds': 5}]}]),
                         {'answers_202': 0, 'retried_keys': 0})

    def test_report_body_is_valid_json_and_stable_per_call(self):
        self.assertEqual(json.loads(ledger.make_body(3, 'p'))['attributes'], {'synthetic': True})


class QuarantinePayloadTests(unittest.TestCase):
    def test_digest_and_length_are_compared_case_insensitively(self):
        import hashlib
        content = b'original bytes'
        digest = hashlib.sha256(content).hexdigest()
        good = quarantine.compare_download(200, content, {'x-content-sha256': digest.upper(), 'Content-Length': str(len(content))})
        self.assertTrue(good['sha256_matches'] and good['length_matches'])
        bad = quarantine.compare_download(200, content+b'!', {'X-Content-SHA256': digest, 'Content-Length': str(len(content))})
        self.assertFalse(bad['sha256_matches'] or bad['length_matches'])
        self.assertFalse(quarantine.compare_download(403, b'', {})['sha256_matches'])
        self.assertFalse(quarantine.compare_download(200, content, {})['sha256_matches'])

    def test_raw_fields_in_metadata_listing_are_flagged(self):
        self.assertEqual(quarantine.leaked_fields([{'message_id': 'a', 'reason': 'x'}]), [])
        self.assertEqual(quarantine.leaked_fields([{'message_id': 'a', 'Payload': 'zzz'}]), ['Payload'])


class ReplayFlowTests(unittest.TestCase):
    def test_only_waiting_messages_are_selected_oldest_first(self):
        items = [{'message_id': 'a', 'status': 'resolved'}, {'message_id': 'b', 'status': 'parked'},
                 {'message_id': 'c', 'status': 'retrying'}, {'message_id': 'd', 'status': 'parked'}]
        self.assertEqual(replay_flow.pick_messages(items, 2), ['b', 'c'])


class SchemaBehaviorTests(unittest.TestCase):
    def test_class_counts_and_mode_summary(self):
        items = [{'status': 'parked', 'retryable': False}, {'status': 'parked', 'retryable': False},
                 {'status': 'retrying', 'retryable': True}]
        counts = schema.class_counts(items)
        self.assertEqual(counts, {'parked/not-retryable': 2, 'retrying/retryable': 1})
        before = {'events': 10, 'unprocessed': {'retrying/retryable': 1}, 'reasons': ['a']}
        after = {'events': 10, 'unprocessed': counts, 'reasons': ['a', 'schema_incompatible: x']}
        result = schema.summarize_mode(before, after)
        self.assertEqual(result['events_added'], 0)
        self.assertEqual(result['unprocessed_added'], {'parked/not-retryable': 2, 'retrying/retryable': 0})
        self.assertEqual(result['new_reasons'], ['schema_incompatible: x'])


class ProgressTests(unittest.TestCase):
    @staticmethod
    def item(received, applied, **extra):
        base = {'source': 'SATELIT', 'namespace': 'ns', 'received': received, 'prepared': applied, 'applied': applied,
                'virtual_now': '2026-11-12T10:00:00Z', 'watermark': '2026-11-12T09:30:00Z', 'singletons': 4, 'excluded': 2,
                'late_mutations': 1, 'historical_pending': 0}
        base.update(extra)
        return base

    def test_row_lag_rates_and_watermark_age(self):
        previous = self.item(1000, 900)
        row = progress.sample_row(10.0, self.item(1600, 1000), previous, 10)
        self.assertEqual(row['lag'], 600)
        self.assertEqual(row['received_per_s'], 60.0)
        self.assertEqual(row['applied_per_s'], 10.0)
        self.assertEqual(row['watermark_age_s'], 1800.0)
        first = progress.sample_row(0, self.item(10, 10), None, 0)
        self.assertIsNone(first['received_per_s'])
        self.assertIsNone(progress.age_seconds(None, '2026-11-12T09:30:00Z'))

    def test_summary_finds_peak_and_time_back_to_zero(self):
        lags = [(0, 0), (5, 300), (10, 120), (15, 0), (20, 0)]
        rows = [progress.sample_row(t, self.item(1000+t, 1000+t-lag), None, 0) for t, lag in lags]
        summary = progress.summarize(rows)['SATELIT/ns']
        self.assertEqual((summary['peak_lag'], summary['peak_at_s']), (300, 5))
        self.assertEqual(summary['seconds_peak_to_zero'], 10)
        self.assertEqual(summary['final_lag'], 0)

    def test_never_caught_up_has_no_zero_time(self):
        rows = [progress.sample_row(t, self.item(100*t, 10*t), None, 0) for t in (1, 2, 3)]
        self.assertIsNone(progress.summarize(rows)['SATELIT/ns']['seconds_peak_to_zero'])


class SloTests(unittest.TestCase):
    @staticmethod
    def values(**overrides):
        base = {'coverage_complete': 1.0, 'p95_complete': 1.0, 'p95_delivered_seconds': 2.4, 'p95_lower_bound_seconds': 2.0,
                'pending_overdue': 0.0, 'collection_success': 1.0}
        base.update(overrides)
        return base

    def test_extract_strips_prefix_and_keeps_nan(self):
        values = slo.extract(metrics.parse_metrics(SAMPLE))
        self.assertEqual(values['pending'], 3.0)
        self.assertTrue(math.isnan(values['on_time_ratio']))
        self.assertEqual(slo.show(values['on_time_ratio']), 'not measurable')

    def test_complete_cohort_is_judged(self):
        states = {label: state for label, state, _ in slo.p95_lines(self.values())}
        self.assertEqual(states['p95 delivery latency <= 5 s'], 'PASS')
        self.assertEqual(states['no overdue pending warning'], 'PASS')
        slow = {label: state for label, state, _ in slo.p95_lines(self.values(p95_delivered_seconds=7.5, pending_overdue=2.0))}
        self.assertEqual(set(slow.values()), {'FAIL'})

    def test_incomplete_cohort_or_failed_collection_is_not_measurable_never_a_pass(self):
        for values in (self.values(p95_complete=0.0), self.values(coverage_complete=0.0),
                       self.values(p95_delivered_seconds=float('nan'))):
            states = {label: state for label, state, _ in slo.p95_lines(values)}
            self.assertEqual(states['p95 delivery latency <= 5 s'], 'NOT MEASURABLE')
        failed = {label: state for label, state, _ in slo.p95_lines(self.values(collection_success=0.0))}
        self.assertEqual(failed['no overdue pending warning'], 'NOT MEASURABLE')


class SensorProbeTests(unittest.TestCase):
    def test_keys_states_and_latency(self):
        statuses = {'BMKG': {'state': 'fresh'}, 'SENSOR_PANTAI': {'state': 'stale', 'stale_since': 'x'}}
        self.assertEqual(sensor.coastal_keys(statuses), ['SENSOR_PANTAI'])
        self.assertEqual(sensor.source_states(statuses), {'BMKG': 'fresh', 'SENSOR_PANTAI': 'stale'})
        summary = sensor.latency_summary([0.1, 0.2, 0.3, 1.0])
        self.assertEqual(summary['requests'], 4)
        self.assertAlmostEqual(summary['max_ms'], 1000.0)
        self.assertEqual(sensor.latency_summary([]), {})

    def test_breaker_state_names(self):
        parsed = metrics.parse_metrics('app_coastal_observations_breaker_state 2\napp_coastal_stations_breaker_state 0\n')
        self.assertEqual(sensor.breaker_states(parsed), {'observations': 'open', 'stations': 'closed'})


if __name__ == '__main__':
    unittest.main()
