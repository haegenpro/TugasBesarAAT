// Offline verification: no Docker, HTTP requests, credentials, or live evidence.
import { readFileSync } from 'node:fs';
import { Script, createContext } from 'node:vm';
import { test } from 'node:test';
import assert from 'node:assert/strict';

const original = readFileSync(new URL('./load-test.js', import.meta.url), 'utf8');
const source = original.replace(/^import .*;\r?\n/gm, '')
  .replace('export const options', 'const options')
  .replace('export default function ()', 'function iteration()')
  .replace('export function handleSummary', 'function handleSummary')
  + '\nglobalThis.api = { options, iteration, handleSummary };';
function fixture(environment = {}, statuses = []) {
  const samples = {};
  class Metric {
    constructor(name) { this.name = name; samples[name] = []; }
    add(value, tags) { samples[this.name].push({ value, tags }); }
  }
  const response = status => ({ status, json: key => ({ access_token: 'synthetic-token', expires_in: 60 }[key]), timings: { duration: 120 } });
  const context = createContext({ __ENV: { BASE_URL: 'http://offline.invalid', CLIENT_ID: 'synthetic', CLIENT_SECRET: 'synthetic',
    MODE: 'mixed', VUS: '50', DURATION: '65s', ...environment }, __VU: 2,
    Counter: Metric, Rate: Metric, Trend: Metric, sleep() {},
    http: { expectedStatuses() {}, setResponseCallback() {},
      post: () => response(statuses.shift() ?? 200), get: () => response(statuses.shift() ?? 200) } });
  new Script(source, { filename: 'load-test.js' }).runInContext(context);
  return { ...context.api, samples };
}

test('milestone workload minimums cannot be bypassed by wrappers or direct k6', () => {
  assert.throws(() => fixture({ VUS: '49' }), /at least 50 VUs/);
  assert.throws(() => fixture({ DURATION: '59s' }), /at least 50 VUs/);
  assert.throws(() => fixture({ DURATION: 'invalid' }), /DURATION/);
  assert.equal(fixture({ PURPOSE: 'overload', VUS: '300', DURATION: '30s' }).options.thresholds.controlled_rejections_429[0], 'count>0');
  assert.equal(fixture().options.thresholds['http_req_duration{kind:seismic}'][0], 'p(95)<300');
});

test('429s are counted without diluting separate hazard error denominator', () => {
  const run = fixture({}, [200, 429, 500, 200]);
  run.iteration(); run.iteration(); run.iteration();
  assert.deepEqual(run.samples.hazard_failure_rate.map(s => s.value), [true, false]);
  assert.deepEqual(run.samples.login_failure_rate.map(s => s.value), [false]);
  assert.deepEqual(run.samples.controlled_rejections_429.map(s => s.value), [1]);
  assert.equal(run.samples.successful_hazard_duration.length, 1);
  assert.equal(run.samples.hazard_attempts.length, 3);
});

test('summary prints milliseconds, p50/p95/p99, successful throughput, zero 429s and failed thresholds', () => {
  const { handleSummary } = fixture({ SUMMARY_PATH: '/evidence/offline.json' });
  const metrics = {
    http_reqs: { values: { count: 2000, rate: 30.5 } },
    successful_hazard_requests: { values: { count: 1900, rate: 29 } },
    'successful_hazard_duration{kind:seismic}': { values: { med: 125, 'p(95)': 350, 'p(99)': 420 }, thresholds: { 'p(95)<300': { ok: false } } },
    hazard_failure_rate: { values: { rate: 0.02 }, thresholds: { 'rate<0.01': { ok: false } } },
  };
  const output = handleSummary({ metrics });
  assert.match(output.stdout, /LATENCY \(ms\)/);
  assert.match(output.stdout, /p50\s+p95\s+p99/);
  assert.match(output.stdout, /Successful seismic reads\s+125\.00\s+350\.00\s+420\.00/);
  assert.match(output.stdout, /Successful hazard reads \(HTTP 200\):\s+1900 \(29\.00\/s\)/);
  assert.match(output.stdout, /controlled_rejections_429:\s+0 \(0\.00\/s\)/);
  assert.match(output.stdout, /hazard_failure_rate.*2\.00%/);
  assert.match(output.stdout, /OVERALL LOAD THRESHOLDS: FAIL/);
  const saved = JSON.parse(output['/evidence/offline.json']);
  assert.equal(saved.configuration.duration_seconds, 65);
  assert.equal(saved.metrics.hazard_failure_rate.values.rate, 0.02);
  assert.ok(!output.stdout.includes('synthetic-token'));
  assert.ok(!Object.hasOwn(saved.configuration, 'CLIENT_SECRET'));
});
