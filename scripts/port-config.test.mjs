// Requires the local env files used by Compose; starts no containers.
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const names = ['BMKG_PORT', 'PVMBG_PORT', 'AGGREGATOR_PORT', 'AUTH_PORT', 'CLIENT_API_PORT',
  'DASHBOARD_PORT', 'NOTIFIER_PORT', 'DEMO_CONSUMER_PORT', 'NATS_PORT', 'NATS_MONITOR_PORT',
  'SCHEMA_REGISTRY_PORT', 'SCHEMA_REGISTRY_MANAGEMENT_PORT', 'POSTGRES_PORT', 'REGISTRY_DB_PORT',
  'GRAFANA_PORT', 'PROMETHEUS_PORT', 'LOKI_PORT', 'ALLOY_PORT', 'NATS_EXPORTER_PORT'];
const ports = Object.fromEntries(names.map((key, i) => [key, String(24000 + i)]));
function config(overrides = {}) {
  return spawnSync('docker', ['compose', '--profile', 'demo', '--profile', 'consumer-demo',
    '--profile', 'tools', 'config', '--format', 'json'], {
    cwd: root, env: { ...process.env, ...ports, ...overrides }, encoding: 'utf8', maxBuffer: 4 << 20,
  });
}

test('root ports configure listeners, mappings, client URLs, and probes together', () => {
  const result = config();
  assert.equal(result.status, 0, 'Compose rejected a non-default port plan');
  const services = JSON.parse(result.stdout).services;
  const port = (service, key, rootKey = key) => assert.equal(services[service].environment[key], ports[rootKey], `${service}.${key}`);
  for (const [service, key, rootKey] of [
    ['bmkg-mock', 'BMKG_PORT'], ['pvmbg-mock', 'PVMBG_PORT'], ['aggregator', 'AGGREGATOR_PORT'],
    ['auth-service', 'AUTH_PORT'], ['client-api', 'CLIENT_API_PORT'], ['broker', 'NATS_PORT'], ['broker', 'NATS_MONITOR_PORT'],
    ['schema-registry', 'QUARKUS_HTTP_PORT', 'SCHEMA_REGISTRY_PORT'], ['schema-registry', 'QUARKUS_MANAGEMENT_PORT', 'SCHEMA_REGISTRY_MANAGEMENT_PORT'],
    ['consumer-dashboard', 'CONSUMER_PORT', 'DASHBOARD_PORT'], ['consumer-notifier', 'CONSUMER_PORT', 'NOTIFIER_PORT'],
    ['consumer-dashboard-demo', 'CONSUMER_PORT', 'DEMO_CONSUMER_PORT'], ['grafana', 'GF_SERVER_HTTP_PORT', 'GRAFANA_PORT'],
    ['prometheus', 'PROMETHEUS_PORT'], ['loki', 'LOKI_PORT'], ['alloy', 'ALLOY_PORT'], ['nats-exporter', 'NATS_EXPORTER_PORT'],
    ['postgres', 'PGPORT', 'POSTGRES_PORT'], ['registry-db', 'PGPORT', 'REGISTRY_DB_PORT'],
  ]) port(service, key, rootKey);
  for (const [service, key] of [['bmkg-mock', 'BMKG_PORT'], ['pvmbg-mock', 'PVMBG_PORT'],
    ['client-api', 'CLIENT_API_PORT'], ['grafana', 'GRAFANA_PORT']]) {
    assert.equal(services[service].ports[0].target, Number(ports[key]), `${service} published target`);
  }
  const expected = {
    NATS_URL: 'NATS_PORT', NATS_MONITOR_URL: 'NATS_MONITOR_PORT', SCHEMA_REGISTRY_URL: 'SCHEMA_REGISTRY_PORT',
    BMKG_BASE_URL: 'BMKG_PORT', PVMBG_BASE_URL: 'PVMBG_PORT', AUTH_BASE_URL: 'AUTH_PORT',
    AGGREGATOR_BASE_URL: 'AGGREGATOR_PORT', BNPB_API_URL: 'CLIENT_API_PORT',
    PROMETHEUS_URL: 'PROMETHEUS_PORT', LOKI_URL: 'LOKI_PORT', ALLOY_METRICS_URL: 'ALLOY_PORT',
    LOKI_METRICS_URL: 'LOKI_PORT', NATS_EXPORTER_METRICS_URL: 'NATS_EXPORTER_PORT',
    PROMETHEUS_WRITE_URL: 'PROMETHEUS_PORT', LOKI_PUSH_URL: 'LOKI_PORT',
    BMKG_HEALTH_URL: 'BMKG_PORT', PVMBG_HEALTH_URL: 'PVMBG_PORT', AGGREGATOR_HEALTH_URL: 'AGGREGATOR_PORT',
    AGGREGATOR_READY_URL: 'AGGREGATOR_PORT', AUTH_HEALTH_URL: 'AUTH_PORT', CLIENT_API_HEALTH_URL: 'CLIENT_API_PORT',
    CLIENT_API_READY_URL: 'CLIENT_API_PORT', DASHBOARD_HEALTH_URL: 'DASHBOARD_PORT', DASHBOARD_READY_URL: 'DASHBOARD_PORT',
    NOTIFIER_HEALTH_URL: 'NOTIFIER_PORT', NOTIFIER_READY_URL: 'NOTIFIER_PORT', REGISTRY_READY_URL: 'SCHEMA_REGISTRY_MANAGEMENT_PORT',
    NATS_HEALTH_URL: 'NATS_MONITOR_PORT', DATABASE_URL: 'POSTGRES_PORT', APICURIO_DATASOURCE_URL: 'REGISTRY_DB_PORT',
  };
  for (const [name, service] of Object.entries(services)) {
    for (const [key, rootKey] of Object.entries(expected)) {
      const value = service.environment?.[key];
      if (value) assert.equal(new URL(value.replace(/^jdbc:/, '')).port, ports[rootKey], `${name}.${key} port`);
    }
    for (const arg of service.healthcheck?.test || []) {
      if (arg.startsWith('http://')) {
        const expectedKey = { 'schema-registry': 'SCHEMA_REGISTRY_MANAGEMENT_PORT', 'grafana': 'GRAFANA_PORT',
          'prometheus': 'PROMETHEUS_PORT', 'loki': 'LOKI_PORT', 'alloy': 'ALLOY_PORT', 'nats-exporter': 'NATS_EXPORTER_PORT' }[name];
        assert.equal(new URL(arg).port, ports[expectedKey], `${name} probe port`);
      }
    }
  }
  assert.equal(services.broker.ports, undefined, 'broker must remain internal');
  assert.equal(services['schema-registry'].ports, undefined, 'registry must remain internal');
  assert.deepEqual(Object.keys(services['client-tui'].networks), ['client'], 'downstream client must be isolated from backend administration');
});

test('missing root port fails instead of silently using a default', () => {
  const result = config({ CLIENT_API_PORT: '' });
  assert.notEqual(result.status, 0);
  assert.ok(result.stderr.includes('CLIENT_API_PORT'));
});
