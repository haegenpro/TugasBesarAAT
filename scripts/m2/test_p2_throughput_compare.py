"""P2-7 comparison: paced throughput accounting and the verdict, without a cluster or k6."""
import argparse
import unittest

import p2_load_test as load_run
import p2_throughput_compare as compare_run


def counter(value):
    return {'values': {'count': value}}


def paced_summary(complete, target=1000, hold=100, vus=500, exercised=500, **extra):
    metrics = {'m2_paced_complete_hazard_reads': counter(complete), 'm2_paced_requests': counter(complete),
               'm2_paced_vus_exercised': counter(exercised), 'm2_partial_hazard_reads': counter(0),
               'm2_invalid_hazard_responses': counter(0), 'm2_transport_failures': counter(0),
               'm2_unexpected_errors': {'values': {'rate': 0, 'passes': 0, 'fails': 1}}}
    metrics.update(extra)
    return {'configuration': {'target_rps': target, 'hold_seconds': hold, 'vus': vus},
            'state': {'testRunDurationMs': 190000}, 'metrics': metrics}


def item(rate, cap=3000, vus=500, exercised=500, **extra):
    result = {'pages_per_second': rate, 'partial_pages': 0, 'invalid_responses': 0, 'transport_failures': 0,
              'unexpected_rate': 0, 'vus_exercised': exercised, 'configured_vus': vus}
    result.update(extra)
    return result


class PacedThroughputTests(unittest.TestCase):
    def test_a_paced_run_counts_only_the_hold_window(self):
        result = load_run.throughput(paced_summary(complete=90000))
        self.assertEqual(result['seconds'], 100)
        self.assertAlmostEqual(result['pages_per_second'], 900.0)
        self.assertEqual(result['target_rps'], 1000)
        self.assertEqual(result['vus_exercised'], 500)
        self.assertTrue(result['saturated'])      # 900 < 95% of 1000

    def test_a_paced_run_that_meets_the_offered_rate_is_not_saturated(self):
        result = load_run.throughput(paced_summary(complete=99000))
        self.assertFalse(result['saturated'])

    def test_an_unpaced_run_keeps_the_whole_run_accounting(self):
        summary = {'state': {'testRunDurationMs': 50000},
                   'metrics': {'m2_complete_hazard_reads': counter(5000)}}
        result = load_run.throughput(summary)
        self.assertEqual(result['seconds'], 50)
        self.assertAlmostEqual(result['pages_per_second'], 100.0)
        self.assertIsNone(result['target_rps'])
        self.assertNotIn('saturated', result)

    def test_evaluate_requires_every_vu_to_take_part_in_a_paced_run(self):
        args = argparse.Namespace(vus=500, scale=False, roll_from=None, target_rps=1000)
        checks = load_run.evaluate(paced_summary(complete=1, exercised=499), {}, args)
        paced = [c for c in checks if c['criterion'].startswith('paced run')]
        self.assertEqual([c['ok'] for c in paced], [False])
        checks = load_run.evaluate(paced_summary(complete=1), {}, args)
        self.assertTrue(all(c['ok'] for c in checks if c['criterion'].startswith('paced run')))


class VerdictTests(unittest.TestCase):
    def test_ratio_above_two_below_the_cap_is_met(self):
        verdict = compare_run.compare({1: item(1000), 3: item(2600)}, cap=3000)
        self.assertTrue(verdict['valid'])
        self.assertTrue(verdict['meets_requirement'])
        self.assertAlmostEqual(verdict['ratio'], 2.6)
        self.assertFalse(verdict['ratio_is_lower_bound'])

    def test_a_high_sample_at_the_cap_is_reported_as_a_lower_bound(self):
        verdict = compare_run.compare({1: item(1000), 3: item(2990)}, cap=3000)
        self.assertTrue(verdict['meets_requirement'])
        self.assertTrue(verdict['ratio_is_lower_bound'])

    def test_a_low_sample_at_the_cap_is_invalid_not_a_pass(self):
        verdict = compare_run.compare({1: item(2800), 3: item(3000)}, cap=3000)
        self.assertFalse(verdict['valid'])
        self.assertFalse(verdict['meets_requirement'])
        self.assertIn('raise --target-rps', verdict['invalid_reasons'][0])

    def test_a_ratio_below_two_is_not_met_but_valid(self):
        verdict = compare_run.compare({1: item(1000), 3: item(1500)}, cap=3000)
        self.assertTrue(verdict['valid'])
        self.assertFalse(verdict['meets_requirement'])

    def test_partial_pages_void_the_comparison(self):
        verdict = compare_run.compare({1: item(1000), 3: item(2600, partial_pages=4)}, cap=3000)
        self.assertFalse(verdict['meets_requirement'])
        self.assertTrue(verdict['validity_problems'])

    def test_unexercised_vus_void_the_comparison(self):
        verdict = compare_run.compare({1: item(1000), 3: item(2600, exercised=300)}, cap=3000)
        self.assertFalse(verdict['meets_requirement'])
        self.assertIn('300 of 500 VUs', verdict['validity_problems'][0])

    def test_without_a_cap_the_old_ratio_rule_applies(self):
        slow = {'pages_per_second': 100.0, 'partial_pages': 0, 'invalid_responses': 0,
                'transport_failures': 0, 'unexpected_rate': None}
        fast = dict(slow, pages_per_second=210.0)
        verdict = compare_run.compare({1: slow, 3: fast})
        self.assertTrue(verdict['meets_requirement'])
        self.assertIsNone(verdict['cap'])
        self.assertTrue(verdict['valid'])


class CapTests(unittest.TestCase):
    def test_cap_is_the_factor_times_the_pilot_rate(self):
        self.assertEqual(compare_run.cap_from_pilot(1234.5, 10000, 135, 3.0), 3704)

    def test_cap_never_drops_below_what_lets_every_vu_send_a_request(self):
        # 10,000 VUs need hold >= vus/rps + 10, i.e. rps >= 10000/125 = 80 for a 135 s hold.
        self.assertEqual(compare_run.cap_from_pilot(1.0, 10000, 135, 3.0), 80)

    def test_target_parser(self):
        self.assertEqual(compare_run.parse_target('auto'), 'auto')
        self.assertEqual(compare_run.parse_target('2500'), 2500.0)
        self.assertEqual(compare_run.parse_target('0'), 0.0)
        for bad in ('fast', '-1'):
            with self.assertRaises(argparse.ArgumentTypeError):
                compare_run.parse_target(bad)


if __name__ == '__main__':
    unittest.main()
