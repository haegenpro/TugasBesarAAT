"""Pure-logic checks for the evidence tools; no cluster, token or network is used."""
import copy
import hashlib
import json
import os
import unittest
from unittest.mock import patch

import evidence_audit as audit
import common
import p2_load_test as load_run
import evidence_outage_probe as outage
import p1_placement_load as placement
import evidence_tester_agent as tester
import p2_throughput_compare as throughput
from argparse import Namespace


def item(identity, ingested, source='BMKG', hazard=None):
    return {'hazard_id': hazard or 'hz-'+identity, 'source': source, 'source_ref_id': identity, 'ingested_at': ingested}


class AuditLogicTests(unittest.TestCase):
    def test_checksum_follows_the_specified_byte_order_and_format(self):
        self.assertEqual(audit.checksum_of(['b', 'a', 'c']), hashlib.sha256(b'a\nb\nc').hexdigest())
        self.assertEqual(audit.checksum_of(['é', 'z']), hashlib.sha256('z\né'.encode()).hexdigest())
        self.assertEqual(audit.checksum_of([]), hashlib.sha256(b'').hexdigest())

    def test_nanosecond_timestamps_parse_and_order_compares_instants(self):
        early = audit.parse_time('2026-11-12T02:43:08Z')
        late = audit.parse_time('2026-11-12T02:43:08.123456789Z')
        self.assertLess(early, late)
        report = audit.analyze([item('a', '2026-11-12T02:43:08Z'), item('b', '2026-11-12T02:43:08.5Z')])
        self.assertEqual(report['order_violations'], 0)

    def test_duplicates_and_order_violations_are_reported(self):
        report = audit.analyze([item('a', '2026-11-12T02:00:02Z'), item('a', '2026-11-12T02:00:01Z', hazard='hz-a')])
        self.assertEqual(report['duplicate_hazard_ids'], ['hz-a'])
        self.assertEqual(report['duplicate_source_refs'], [('BMKG', 'a')])
        self.assertEqual(report['order_violations'], 1)

    def test_gaps_find_takeover_pauses(self):
        times = [audit.parse_time(t) for t in ('2026-11-12T02:00:00Z', '2026-11-12T02:00:01Z', '2026-11-12T02:01:30Z')]
        report = audit.find_gaps(times, 60)
        self.assertEqual(len(report['gaps_over_threshold']), 1)
        self.assertEqual(report['longest_gap_seconds'], 89.0)
        self.assertEqual(audit.find_gaps(times[:2], 60)['gaps_over_threshold'], [])

    def test_snapshot_comparison_detects_loss_and_accepts_growth(self):
        before = {'sources': {'BMKG': {'count': 3, 'checksum': 'x', 'ids': ['a', 'b', 'c'], 'partial': False}}}
        grown = {'sources': {'BMKG': {'count': 4, 'checksum': 'y', 'ids': ['a', 'b', 'c', 'd'], 'partial': False}}}
        lost = {'sources': {'BMKG': {'count': 2, 'checksum': 'z', 'ids': ['a', 'b'], 'partial': False}}}
        partial = {'sources': {'BMKG': {'count': 3, 'checksum': 'x', 'ids': ['a', 'b', 'c'], 'partial': True}}}
        self.assertTrue(audit.compare_snapshots(before, grown)['ok'])
        self.assertFalse(audit.compare_snapshots(before, lost)['ok'])
        self.assertEqual(audit.compare_snapshots(before, lost)['sources']['BMKG']['lost'], 1)
        self.assertFalse(audit.compare_snapshots(before, partial)['ok'])
        self.assertTrue(audit.compare_snapshots(before, before)['ok'])


class PlacementTests(unittest.TestCase):
    NODES = ['node-a', 'node-b']

    @staticmethod
    def row(hazard, primary, replica, unit):
        return {'hazard_id': hazard, 'partial': False, 'locations': [
            {'node': primary, 'unit': unit, 'role': 'primary'}, {'node': replica, 'unit': unit+'-r1', 'role': 'replica'}]}

    def test_balanced_cross_node_placement_passes(self):
        rows = [self.row('1', 'node-a', 'node-b', 'shard-1'), self.row('2', 'node-b', 'node-a', 'shard-2')]
        report = placement.summarize_locations(rows, self.NODES)
        self.assertTrue(placement.placement_ok(report))
        self.assertEqual(report['primaries_per_node'], {'node-a': 1, 'node-b': 1})

    def test_missing_replica_same_node_replica_and_single_primary_node_fail(self):
        no_replica = [{'hazard_id': '1', 'partial': False, 'locations': [{'node': 'node-a', 'unit': 'shard-1', 'role': 'primary'}]},
                      self.row('2', 'node-b', 'node-a', 'shard-2')]
        self.assertFalse(placement.placement_ok(placement.summarize_locations(no_replica, self.NODES)))
        same = [self.row('1', 'node-a', 'node-a', 'shard-1'), self.row('2', 'node-b', 'node-a', 'shard-2')]
        self.assertEqual(placement.summarize_locations(same, self.NODES)['replica_on_primary_node'], ['1'])
        one_primary = [self.row('1', 'node-a', 'node-b', 'shard-1'), self.row('2', 'node-a', 'node-b', 'shard-2')]
        self.assertFalse(placement.placement_ok(placement.summarize_locations(one_primary, self.NODES)))

    def test_partial_answers_are_not_counted_as_placed(self):
        rows = [{'hazard_id': '1', 'partial': True, 'locations': []}, self.row('2', 'node-a', 'node-b', 'shard-1')]
        report = placement.summarize_locations(rows, self.NODES)
        self.assertEqual(report['unreachable'], ['1'])
        self.assertFalse(placement.placement_ok(report))

    def test_spike_events_stay_in_one_region_with_stable_identities(self):
        events = list(placement.spike_events('r1', 30, 'Sumatera Barat', -0.95, 100.35, '2026-11-21T00:00:00Z'))
        self.assertEqual(len({e['source_ref_id'] for e in events}), 30)
        self.assertEqual({e['area_name'] for e in events}, {'Sumatera Barat'})
        self.assertTrue(all(abs(e['latitude']+0.95) < 0.01 and abs(e['longitude']-100.35) < 0.02 for e in events))
        self.assertEqual(events[0]['source_ref_id'], 'spike-r1-000000')


class OutageTests(unittest.TestCase):
    def test_classification(self):
        self.assertEqual(outage.classify(0, b''), 'hung')
        self.assertEqual(outage.classify(500, b''), 'error')
        self.assertEqual(outage.classify(429, b''), 'rejected')
        self.assertEqual(outage.classify(200, b'{"partial": true}'), 'partial')
        self.assertEqual(outage.classify(200, b'{"partial": false}'), 'complete')
        self.assertEqual(outage.classify(200, b'not json'), 'error')

    @staticmethod
    def records(kinds, seconds=0.05):
        return [{'t': i*0.5, 'status': 200, 'kind': kind, 'seconds': seconds} for i, kind in enumerate(kinds)]

    def test_partial_then_complete_is_acceptable_and_recovery_is_measured(self):
        result = outage.summarize(self.records(['complete', 'partial', 'partial', 'complete', 'complete']), 4)
        self.assertTrue(result['ok'])
        self.assertEqual(result['seconds_until_stable_complete'], 1.0)

    def test_errors_hangs_and_slow_answers_fail_the_verdict(self):
        self.assertFalse(outage.summarize(self.records(['complete', 'error']), 4)['ok'])
        self.assertFalse(outage.summarize(self.records(['complete', 'hung']), 4)['ok'])
        self.assertFalse(outage.summarize(self.records(['complete', 'complete'], seconds=9), 4)['ok'])
        self.assertFalse(outage.summarize([], 4)['ok'])


class ThroughputTests(unittest.TestCase):
    @staticmethod
    def sample(pages_per_second, **extra):
        return {'pages_per_second': pages_per_second, 'partial_pages': 0, 'invalid_responses': 0,
                'transport_failures': 0, 'unexpected_rate': 0.0, **extra}

    def test_ratio_uses_lowest_and_highest_replica_counts(self):
        verdict = throughput.compare({1: self.sample(400.0), 3: self.sample(900.0)})
        self.assertAlmostEqual(verdict['ratio'], 2.25)
        self.assertTrue(verdict['meets_requirement'])
        self.assertFalse(throughput.compare({1: self.sample(400.0), 3: self.sample(480.0)})['meets_requirement'])

    def test_partial_pages_void_a_passing_ratio(self):
        verdict = throughput.compare({1: self.sample(400.0), 3: self.sample(900.0, partial_pages=2)})
        self.assertFalse(verdict['meets_requirement'])
        self.assertTrue(verdict['validity_problems'])


class LoadEvaluationTests(unittest.TestCase):
    @staticmethod
    def summary(rate=0.001, passes=3, invalid=0, server=0, transport=0):
        return {'state': {'testRunDurationMs': 100000}, 'metrics': {
            'm2_unexpected_errors': {'values': {'rate': rate, 'passes': passes}},
            'm2_invalid_hazard_responses': {'values': {'count': invalid}},
            'm2_transport_failures': {'values': {'count': transport}},
            'm2_http_status{status:503}': {'values': {'count': server}},
            'm2_complete_hazard_reads': {'values': {'count': 50000}},
            'm2_partial_hazard_reads': {'values': {'count': 0}}}}

    def verdicts(self, summary, report=None, **flags):
        args = Namespace(vus=10000, scale=True, roll_from='previous', strict_zero_failures=False, **flags)
        report = report or {'observed_target_vu_seconds': 130, 'actions': [{'name': 'scale-5'}, {'name': 'scale-2'}, {'name': 'roll-version'}]}
        return {c['criterion']: c['ok'] for c in load_run.evaluate(summary, report, args)}

    def test_a_clean_run_passes_every_check(self):
        self.assertTrue(all(self.verdicts(self.summary()).values()))

    def test_error_rate_hold_time_and_missing_actions_fail(self):
        self.assertFalse(all(self.verdicts(self.summary(rate=0.02)).values()))
        self.assertFalse(all(self.verdicts(self.summary(), {'observed_target_vu_seconds': 90, 'actions': []}).values()))

    def test_strict_mode_rejects_any_failure_but_default_does_not(self):
        report = {'observed_target_vu_seconds': 130, 'actions': [{'name': 'scale-5'}, {'name': 'scale-2'}, {'name': 'roll-version'}]}
        args = Namespace(vus=10000, scale=True, roll_from='previous', strict_zero_failures=True)
        strict = {c['criterion']: c['ok'] for c in load_run.evaluate(self.summary(passes=2), report, args)}
        self.assertFalse(strict['P2-8 no failed request (controlled 429 with Retry-After excluded)'])
        args.strict_zero_failures = False
        self.assertNotIn('P2-8 no failed request (controlled 429 with Retry-After excluded)',
                         {c['criterion'] for c in load_run.evaluate(self.summary(passes=2), report, args)})

    def test_duplicates_fail_the_p2_2_check(self):
        args = Namespace(vus=200, scale=False, roll_from=None, strict_zero_failures=False)
        checks = load_run.evaluate(self.summary(), {}, args, {'duplicate_ids': 1, 'duplicate_refs': 0})
        self.assertFalse({c['criterion']: c['ok'] for c in checks}['P2-2 no duplicate HazardEvent after the run'])

    def test_throughput_counts_complete_pages_over_the_whole_run(self):
        self.assertEqual(load_run.throughput(self.summary())['pages_per_second'], 500.0)


class TesterAgentTests(unittest.TestCase):
    @staticmethod
    def daemonset(image):
        return {'spec': {'template': {'spec': {'containers': [{'image': image, 'securityContext': {'privileged': True}}]}}}}

    @staticmethod
    def pod(node, phase='Running'):
        return {'spec': {'nodeName': node}, 'status': {'phase': phase}}

    def test_pinned_image_and_one_pod_per_node_pass(self):
        result = tester.check_status([self.pod('a'), self.pod('b')], [self.daemonset(tester.IMAGE)], ['a', 'b'])
        self.assertTrue(all(ok for ok, _ in result.values()))

    def test_altered_image_or_missing_node_pod_fail(self):
        changed = tester.check_status([self.pod('a'), self.pod('b')], [self.daemonset('sustuna/tester-agent:latest')], ['a', 'b'])
        self.assertFalse(changed['pinned image digest, unmodified'][0])
        missing = tester.check_status([self.pod('a')], [self.daemonset(tester.IMAGE)], ['a', 'b'])
        self.assertFalse(missing['one running pod on every node'][0])


class ProfileTests(unittest.TestCase):
    def test_only_minikube_uses_the_private_kubeconfig(self):
        with patch.object(common.LOCAL.__class__, 'exists', lambda self: True):
            self.assertIn('KUBECONFIG', common.kube_env({'profile': 'minikube'}))
            expected = os.environ.get('KUBECONFIG')
            self.assertEqual(common.kube_env({'profile': 'k3s'}).get('KUBECONFIG'), expected)

    def test_profile_helper(self):
        self.assertTrue(common.is_minikube({'profile': 'minikube'}))
        self.assertFalse(common.is_minikube({'profile': 'k3s'}))


if __name__ == '__main__':
    unittest.main()
