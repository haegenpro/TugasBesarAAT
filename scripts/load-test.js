// k6 load test for P2.1 (head-of-line blocking) and P2.2 (sustained load).
// MODE=mixed     : half the VUs request SEISMIC (BMKG-only), half VOLCANIC.
// PURPOSE=isolation gates seismic p95 < 300ms; milestone reports latency under load.
// MODE=sustained : VUS parallel connections for DURATION on /hazards.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

function required(name) {
  const value = __ENV[name];
  if (!value || !value.trim()) throw new Error(`${name} is required`);
  return value;
}
const BASE = required('BASE_URL');
const MODE = required('MODE');
const vusValue = required('VUS');
if (!/^[1-9][0-9]*$/.test(vusValue)) throw new Error('VUS must be a positive integer');
const VUS = Number(vusValue);
const DURATION = required('DURATION');
const durationMatch = /^(\d+)(s|m|h)$/.exec(DURATION);
if (!durationMatch) throw new Error('DURATION must be integer seconds, minutes, or hours');
const durationSeconds = Number(durationMatch[1]) * ({ s: 1, m: 60, h: 3600 }[durationMatch[2]]);
if (durationSeconds <= 0) throw new Error('DURATION must be positive');
const PURPOSE = __ENV.PURPOSE || 'milestone';
if (!['milestone', 'isolation', 'overload'].includes(PURPOSE)) throw new Error('PURPOSE must be milestone, isolation or overload');
if (PURPOSE === 'isolation' && (MODE !== 'mixed' || VUS < 2 || durationSeconds < 60)) {
  throw new Error('Isolation evidence requires mixed mode, at least 2 VUs and 60 seconds');
}
if (PURPOSE === 'milestone' && (VUS < 50 || durationSeconds < 60)) {
  throw new Error('Milestone evidence requires at least 50 VUs and 60 seconds; use PURPOSE=overload for the separate rejection test');
}
required('CLIENT_ID');
required('CLIENT_SECRET');
if (!['sustained', 'mixed'].includes(MODE)) throw new Error('MODE must be sustained or mixed');
// Re-login follows the actual lifetime returned by Auth, rather than assuming a TTL.
let refreshAfterMs = 0;

const rejected = new Counter('controlled_rejections_429');
const unexpected = new Counter('unexpected_errors');
const unexpectedRate = new Rate('unexpected_failure_rate');
const successful = new Counter('successful_hazard_requests');
const successfulDuration = new Trend('successful_hazard_duration', true);
const hazardFailureRate = new Rate('hazard_failure_rate');
const loginFailureRate = new Rate('login_failure_rate');
const hazardAttempts = new Counter('hazard_attempts');
const loginAttempts = new Counter('login_attempts');

http.setResponseCallback(http.expectedStatuses(200, 429));

export const options = {
  scenarios: { load: { executor: 'constant-vus', vus: VUS, duration: DURATION } },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    unexpected_errors: ['count>=0'],
    controlled_rejections_429: ['count>=0'],   // forces the counter to print even when 0
    successful_hazard_requests: ['count>0'],
    successful_hazard_duration: ['p(95)>=0'],
    unexpected_failure_rate: ['rate<0.01'], // excludes controlled rejections from both numerator and denominator
    hazard_failure_rate: ['rate<0.01'],
    login_failure_rate: ['rate<0.01'],
    ...(PURPOSE === 'overload' ? { controlled_rejections_429: ['count>0'] } : {}),
    ...(MODE === 'mixed' ? { 'http_req_duration{kind:volcanic}': ['p(95)>=0'] } : {}), // forces a volcanic line in the summary
    'http_req_duration{kind:login}': ['p(95)>=0'],
    ...(MODE === 'sustained' ? { 'http_req_duration{kind:sustained}': ['p(95)>=0'] } : {}),
    ...(MODE === 'mixed' ? {
      'http_req_duration{kind:seismic}': [PURPOSE === 'isolation' ? 'p(95)<300' : 'p(95)>=0'],
      'successful_hazard_duration{kind:seismic}': [PURPOSE === 'isolation' ? 'p(95)<300' : 'p(95)>=0'],
      'successful_hazard_duration{kind:volcanic}': ['p(95)>=0'],
      'successful_hazard_requests{kind:seismic}': ['count>0'],
      'successful_hazard_requests{kind:volcanic}': ['count>0'],
    } : {}),
    http_req_failed: ['rate<0.01'],
  },
};

let token = null;
let obtained = 0;

function login() {
  loginAttempts.add(1);
  const res = http.post(`${BASE}/auth/login`,
    JSON.stringify({ client_id: __ENV.CLIENT_ID, client_secret: __ENV.CLIENT_SECRET }),
    { headers: { 'Content-Type': 'application/json' }, tags: { kind: 'login' } });
  if (res.status !== 429) {
    unexpectedRate.add(res.status !== 200);
    loginFailureRate.add(res.status !== 200);
  }
  if (res.status === 200) {
    token = res.json('access_token');
    obtained = Date.now();
    const expiresIn = Number(res.json('expires_in'));
    if (!Number.isFinite(expiresIn) || expiresIn <= 0) throw new Error('Auth must return a positive expires_in');
    refreshAfterMs = expiresIn * 1000 * 0.75;
    return true;
  }
  // A rejected login (429 under overload) is not an error; back off and retry next iteration.
  if (res.status === 429) rejected.add(1);
  else unexpected.add(1);
  token = null;
  return false;
}

export default function () {
  if (!token || Date.now() - obtained > refreshAfterMs) {
    if (!login()) { sleep(0.5); return; }
  }

  let kind = 'sustained';
  let path = '/hazards';
  if (MODE === 'mixed') {
    kind = __VU % 2 === 0 ? 'seismic' : 'volcanic';
    path = `/hazards?hazard_type=${kind.toUpperCase()}`;
  }
  successful.add(0, { kind });
  hazardAttempts.add(1, { kind });
  const res = http.get(`${BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { kind },
  });
  if (res.status !== 429) {
    unexpectedRate.add(res.status !== 200);
    hazardFailureRate.add(res.status !== 200, { kind });
  }
  if (res.status === 200) {
    successful.add(1, { kind });
    successfulDuration.add(res.timings.duration, { kind });
  }
  if (res.status === 429) rejected.add(1);
  else if (res.status !== 200) { unexpected.add(1); if (res.status === 401) token = null; }
  sleep(0.05);
}

// Machine-readable results contain measurements and public workload configuration,
// never credentials or tokens. SUMMARY_PATH is optional and must be writable.
export function handleSummary(data) {
  const report = {
    configuration: { mode: MODE, purpose: PURPOSE, vus: VUS, duration: DURATION,
      duration_seconds: durationSeconds, executor: 'constant-vus', think_time_seconds: 0.05,
      error_denominator: 'non-429 responses; separate hazard and login rates',
      latency_scope: 'all hazard attempts plus successful-only trends; login reported separately' },
    ...data,
  };
  const output = JSON.stringify(report, null, 2);
  const metric = name => data.metrics[name]?.values;
  const number = (value, decimals = 2) => Number.isFinite(value) ? value.toFixed(decimals) : 'N/A';
  const counter = name => {
    const value = metric(name);
    return value ? `${number(value.count, 0)} (${number(value.rate)}/s)` : '0 (0.00/s)';
  };
  const rate = name => {
    const value = metric(name);
    return value ? `${number(value.rate * 100)}%` : 'N/A (no non-rejected samples)';
  };
  const lines = [
    '', 'MILESTONE 1 LOAD TEST',
    `Mode: ${MODE} | Purpose: ${PURPOSE} | ${VUS} constant VUs | ${DURATION} | think time: 50 ms`,
    'VUs are concurrent users; capture TCP observations separately for connection evidence.',
    '', 'THROUGHPUT / COUNTS',
    `All HTTP requests (including login and 429): ${counter('http_reqs')}`,
    `Hazard attempts:                            ${counter('hazard_attempts')}`,
    `Successful hazard reads (HTTP 200):         ${counter('successful_hazard_requests')}`,
    `Login attempts:                             ${counter('login_attempts')}`,
    `controlled_rejections_429:                  ${counter('controlled_rejections_429')}`,
    `unexpected_errors:                         ${counter('unexpected_errors')}`,
    '', 'LATENCY (ms)',
    `${'Request group'.padEnd(36)} ${'p50'.padStart(10)} ${'p95'.padStart(10)} ${'p99'.padStart(10)}`,
  ];
  const durationRows = [
    ['All HTTP (includes login / 429)', 'http_req_duration'],
    ['Login', 'http_req_duration{kind:login}'],
    ['All hazard attempts', 'http_req_duration{kind:sustained}'],
    ['Seismic attempts (includes 429)', 'http_req_duration{kind:seismic}'],
    ['Volcanic attempts (includes 429)', 'http_req_duration{kind:volcanic}'],
    ['Successful hazard reads', 'successful_hazard_duration'],
    ['Successful seismic reads', 'successful_hazard_duration{kind:seismic}'],
    ['Successful volcanic reads', 'successful_hazard_duration{kind:volcanic}'],
  ];
  for (const [label, name] of durationRows) {
    const value = metric(name);
    if (value) lines.push(`${label.padEnd(36)} ${number(value.med).padStart(10)} ${number(value['p(95)']).padStart(10)} ${number(value['p(99)']).padStart(10)}`);
  }
  lines.push('', 'ERROR RATES',
    `http_req_failed (429 stays in denominator): ${rate('http_req_failed')}`,
    `unexpected_failure_rate (excludes 429):     ${rate('unexpected_failure_rate')}`,
    `hazard_failure_rate (excludes 429/login):   ${rate('hazard_failure_rate')}`,
    `login_failure_rate (excludes 429):          ${rate('login_failure_rate')}`,
    '', 'THRESHOLD RESULTS');
  let passed = true;
  for (const [name, item] of Object.entries(data.metrics).sort()) {
    for (const [expression, result] of Object.entries(item.thresholds || {})) {
      if (expression === 'count>=0' || expression === 'p(95)>=0') continue; // display-only submetrics
      lines.push(`${result.ok ? 'PASS' : 'FAIL'} ${name}: ${expression}`);
      passed = passed && result.ok;
    }
  }
  lines.push(`OVERALL LOAD THRESHOLDS: ${passed ? 'PASS' : 'FAIL'}`, 'Runtime health/restarts and source delay need the separate runbook evidence.', '');
  return __ENV.SUMMARY_PATH ? { [__ENV.SUMMARY_PATH]: output, stdout: lines.join('\n') + '\n' }
    : { stdout: lines.join('\n') + '\n' };
}
