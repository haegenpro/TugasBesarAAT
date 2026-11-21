"""Operator overrides must reach their owners and trigger configuration rollouts."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from common import ROOT, read_config, runtime_secrets
from render import render
from tuning import DEFAULTS, settings


class TuningTests(unittest.TestCase):
    def setUp(self):
        self.cfg = json.loads((ROOT/'deployment/m2/config.example.json').read_text())
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.credentials = runtime_secrets(Path(self.tmp.name)/'secrets.json')

    def objects(self, cfg):
        return {(o['kind'], o['metadata']['name']): o for o in render(cfg, self.credentials)['items']}

    def load(self, cfg):
        path = Path(self.tmp.name)/'config.json'
        path.write_text(json.dumps(cfg))
        return read_config(path)

    def test_legacy_files_and_explicit_defaults_render_identically(self):
        legacy = {k: v for k, v in self.cfg.items() if k not in DEFAULTS}
        self.assertEqual(settings(legacy), DEFAULTS)
        self.assertEqual(self.objects(legacy), self.objects(self.load(self.cfg)))
        objects = self.objects(legacy)
        config = lambda name: objects[('ConfigMap', name+'-config')]['data']
        self.assertEqual(config('aggregator')['DB_STATEMENT_TIMEOUT'], '1500ms')
        self.assertEqual(config('ingestion')['DB_STATEMENT_TIMEOUT'], '5000ms')
        self.assertEqual(config('ingestion')['DB_LOCK_TIMEOUT'], '2000ms')
        self.assertEqual(config('ingestion')['DB_IDLE_TRANSACTION_TIMEOUT'], '10000ms')
        self.assertEqual(config('client-api')['CLIENT_API_FIELD_RESERVED'], '16')
        self.assertEqual(config('ingestion')['PANTAI_REQUEST_TIMEOUT_MS'], '1200')
        self.assertEqual(config('aggregator')['OTEL_TRACES_SAMPLER_ARG'], '0.05')
        self.assertEqual(config('aggregator')['AGGREGATOR_FIELD_DB_CONNS'], '3')

    def test_partial_overrides_reach_owners_and_rollout_hashes(self):
        before = self.objects(self.cfg)
        cfg = copy.deepcopy(self.cfg)
        cfg['observability'] = {'trace_sample_ratio': 1, 'trace_queue_spans': 64,
                                'trace_batch_spans': 8, 'trace_export_timeout_ms': 400}
        cfg['database'] = {'services': {'ingestion': {'statement_timeout_ms': 900,
                                                     'lock_timeout_ms': 250, 'max_open': 5}},
                           'aggregator_field_read_connections': 2, 'registry_max_open': 3}
        cfg['admission'] = {'client_max_inflight': 48, 'client_field_reserved': 12}
        cfg['sources'] = {'satellite_idle_timeout_ms': 5000, 'coastal_request_timeout_ms': 300}
        cfg['http'] = {'max_connections_per_host': 32, 'max_idle_per_host': 16,
                       'max_idle_connections': 64, 'field_max_connections_per_host': 16}
        objects = self.objects(self.load(cfg))
        config = lambda name: objects[('ConfigMap', name+'-config')]['data']
        self.assertEqual(config('ingestion')['DB_STATEMENT_TIMEOUT'], '900ms')
        self.assertEqual(config('ingestion')['DB_LOCK_TIMEOUT'], '250ms')
        self.assertEqual(config('ingestion')['DB_MAX_OPEN_CONNS'], '5')
        self.assertEqual(config('wildfire-processor')['DB_STATEMENT_TIMEOUT'], '5000ms')
        self.assertEqual(config('aggregator')['AGGREGATOR_FIELD_DB_CONNS'], '2')
        self.assertEqual(config('schema-registry')['QUARKUS_DATASOURCE_JDBC_MAX_SIZE'], '3')
        self.assertEqual(config('client-api')['CLIENT_API_MAX_INFLIGHT'], '48')
        self.assertEqual(config('client-api')['CLIENT_API_FIELD_RESERVED'], '12')
        self.assertEqual(config('ingestion')['SATELLITE_IDLE_TIMEOUT_MS'], '5000')
        self.assertEqual(config('ingestion')['PANTAI_REQUEST_TIMEOUT_MS'], '300')
        self.assertEqual(config('client-api')['HTTP_MAX_CONNS_PER_HOST'], '32')
        self.assertEqual(config('client-api')['FIELD_HTTP_MAX_CONNS_PER_HOST'], '16')
        for name in ('aggregator', 'client-api', 'auth-service', 'ingestion', 'wildfire-processor',
                     'coastal-processor', 'field-report-processor', 'consumer-dashboard', 'consumer-notifier',
                     'bmkg-mock', 'pvmbg-mock'):
            self.assertEqual(config(name)['OTEL_TRACES_SAMPLER_ARG'], '1')
            self.assertEqual(config(name)['OTEL_BSP_MAX_QUEUE_SIZE'], '64')
            self.assertEqual(config(name)['OTEL_BSP_MAX_EXPORT_BATCH_SIZE'], '8')
            self.assertEqual(config(name)['OTEL_BSP_EXPORT_TIMEOUT'], '400')
            key = ('Deployment', name)
            annotation = lambda objects: objects[key]['spec']['template']['metadata']['annotations']
            self.assertNotEqual(annotation(before), annotation(objects))

    def test_monitoring_overrides_render_and_restart_backends(self):
        before = self.objects(self.cfg)
        self.cfg['observability'] = {'prometheus_scrape_seconds': 10, 'prometheus_retention_hours': 2,
                                    'prometheus_retention_mib': 128, 'jaeger_max_traces': 500,
                                    'loki_retention_hours': 2}
        objects = self.objects(self.load(self.cfg))
        prom = json.loads(objects[('ConfigMap', 'prometheus-config')]['data']['prometheus.yaml'])
        self.assertEqual(prom['global']['scrape_interval'], '10s')
        args = objects[('Deployment', 'prometheus')]['spec']['template']['spec']['containers'][0]['args']
        self.assertIn('--storage.tsdb.retention.time=2h', args)
        self.assertIn('--storage.tsdb.retention.size=128MB', args)
        self.assertIn('max_traces: 500', objects[('ConfigMap', 'jaeger-config')]['data']['jaeger.yaml'])
        self.assertIn('retention_period: 2h', objects[('ConfigMap', 'loki-config')]['data']['loki.yaml'])
        for name in ('prometheus', 'jaeger', 'loki'):
            template = lambda objects: objects[('Deployment', name)]['spec']['template']
            self.assertNotEqual(template(before), template(objects))

    def test_runtime_and_optional_ai_preserve_defaults_and_accept_overrides(self):
        cfg = copy.deepcopy(self.cfg)
        cfg['features']['ai'] = True
        cfg['external'].update(openrouter_api_key='test-only', summary_model='test-only')
        objects = self.objects(self.load(cfg))
        config = lambda name: objects[('ConfigMap', name+'-config')]['data']
        self.assertEqual(config('situation-summarizer')['OTEL_BSP_EXPORT_TIMEOUT'], '1000')
        self.assertEqual(config('situation-summarizer')['SUMMARY_MAX_CALLS_PER_DAY'], '45')
        self.assertEqual(config('situation-summarizer')['CHAT_MAX_CONCURRENT'], '2')
        self.assertEqual(config('aggregator')['GOMEMLIMIT'], '384MiB')
        self.assertEqual(config('bmkg-mock')['GOMEMLIMIT'], '96MiB')
        cfg['runtime'] = {'application_memory_mib': 160, 'aggregator_memory_mib': 320,
                          'mock_memory_mib': 80, 'monitoring_memory_mib': 160,
                          'grafana_memory_mib': 112, 'go_processors': 1}
        cfg['ai'] = {'summary_calls_per_day': 20, 'chat_calls_per_day': 0,
                     'chat_concurrency': 1, 'refresh_interval_seconds': 600}
        cfg['observability'] = {'ai_trace_export_timeout_ms': 300}
        objects = self.objects(self.load(cfg))
        self.assertEqual(config('situation-summarizer')['OTEL_BSP_EXPORT_TIMEOUT'], '300')
        self.assertEqual(config('situation-summarizer')['SUMMARY_MAX_CALLS_PER_DAY'], '20')
        self.assertEqual(config('situation-summarizer')['CHAT_MAX_CALLS_PER_DAY'], '0')
        self.assertEqual(config('situation-summarizer')['CHAT_MAX_CONCURRENT'], '1')
        self.assertEqual(config('situation-summarizer')['SUMMARY_REFRESH_INTERVAL'], '600s')
        self.assertEqual(config('aggregator')['GOMEMLIMIT'], '320MiB')
        self.assertEqual(config('ingestion')['GOMEMLIMIT'], '160MiB')
        self.assertEqual(config('pvmbg-mock')['GOMEMLIMIT'], '80MiB')
        for name, memory in [('grafana', '112MiB'), ('jaeger', '160MiB')]:
            env = objects[('Deployment', name)]['spec']['template']['spec']['containers'][0]['env']
            self.assertIn({'name': 'GOMEMLIMIT', 'value': memory}, env)
            self.assertIn({'name': 'GOMAXPROCS', 'value': '1'}, env)

    def test_invalid_and_misspelled_settings_fail_before_deployment(self):
        cases = [
            ('observability', {'trace_sample_ratio': float('nan')}),
            ('observability', {'trace_sample_ratio': float('inf')}),
            ('observability', {'trace_sample_ratio': 10**1000}),
            ('observability', {'trace_sample_ratio': True}),
            ('observability', {'trace_sample_ratio': 1.01}),
            ('observability', {'trace_queue_spans': 16, 'trace_batch_spans': 32}),
            ('observability', {'trace_queue_spans': 0}),
            ('observability', {'trace_sample_rato': 0.1}),
            ('observability', {'loki_retention_hours': 0}),
            ('database', {'services': {'ingeston': {'max_open': 4}}}),
            ('database', {'services': {'auth-service': {'max_open': 3}}}),
            ('database', {'services': {'ingestion': {'statement_timeout_ms': 0}}}),
            ('admission', {'client_max_inflight': 16, 'client_field_reserved': 16}),
            ('admission', {'client_auth_field_reserved': 32}),
            ('admission', {'client_per_client': 65}),
            ('admission', {'auth_field_reserved': 32}),
            ('sources', {'coastal_breaker_cooldown_ms': 20000}),
            ('sources', {'satellite_idle_timeout_ms': -1}),
            ('sources', {'coastal_request_timeout_ms': 2147483648}),
            ('sources', {'coastal_poll_interval_ms': False}),
            ('runtime', {'aggregator_memory_mib': 512}),
            ('runtime', {'mock_memory_mib': 128}),
            ('ai', {'chat_concurrency': 17}),
            ('ai', {'reasoning_tokens': 4096, 'max_tokens': 4096}),
            ('ai', {'chat_calls_per_day': -1}),
            ('http', {'max_connections_per_host': 2, 'max_idle_per_host': 4}),
        ]
        for group, override in cases:
            with self.subTest(group=group, override=override):
                cfg = copy.deepcopy(self.cfg)
                cfg[group] = override
                with self.assertRaises(ValueError):
                    self.load(cfg)


if __name__ == '__main__':
    unittest.main()
