// Run against Client API's private NodePort; record any generator/cluster colocation.
// Secrets are read from a private JSON file, never printed or sent as tags.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const credentials = JSON.parse(open(__ENV.M2_CREDENTIALS_FILE));
const base = (__ENV.M2_BASE_URL || '').replace(/\/$/, '');
if (!/^https?:\/\//.test(base)) throw new Error('M2_BASE_URL is required');
const total = Number(__ENV.M2_VUS || 10000);
const fieldVUs = Math.max(1, Math.floor(total * Number(__ENV.M2_FIELD_FRACTION || 0.1)));
const think = Number(__ENV.M2_THINK_SECONDS || 1);
const reportEvery = Number(__ENV.M2_REPORT_EVERY || 0);
if (!Number.isInteger(total) || total < 2 || fieldVUs >= total || think < 0 || (reportEvery > 0 && !__ENV.M2_RUN_ID))
  throw new Error('invalid load settings');

const unsuccessful = new Rate('m2_unsuccessful');
const unexpected = new Rate('m2_unexpected_errors');
const unexpectedAll = new Rate('m2_unexpected_all_operations');
const statuses = new Counter('m2_http_status');
const rejections = new Counter('m2_admission_rejections');
const timeouts = new Counter('m2_transport_failures');
const completed = new Counter('m2_completed');
const latency = new Trend('m2_operation_duration', true);
const successLatency = new Trend('m2_successful_duration', true);
const successfulReads = new Counter('m2_successful_hazard_reads');
const completeReads = new Counter('m2_complete_hazard_reads');
const partialReads = new Counter('m2_partial_hazard_reads');
const invalidReads = new Counter('m2_invalid_hazard_responses');
const phases = [
  { duration: __ENV.M2_RAMP || '60s', target: 1 },
  { duration: __ENV.M2_HOLD || '120s', target: 1 },
  { duration: '15s', target: 0 },
];
function scenario(vus, exec) {
  return { executor: 'ramping-vus', exec, startVUs: 0,
    stages: phases.map(p => ({ duration: p.duration, target: p.target * vus })),
    gracefulRampDown: '10s', gracefulStop: '10s' };
}
export const options = {
  scenarios: { media: scenario(total - fieldVUs, 'media'), field: scenario(fieldVUs, 'field') },
  thresholds: { m2_unexpected_errors: ['rate<0.01'],
    m2_successful_hazard_reads: ['count>0'],
    m2_complete_hazard_reads: ['count>=0'], m2_partial_hazard_reads: ['count>=0'],
    m2_invalid_hazard_responses: ['count==0'],
    m2_admission_rejections: ['count>=0'], m2_transport_failures: ['count>=0'],
    'm2_unsuccessful{role:field}': ['rate>=0'], 'm2_unsuccessful{role:media}': ['rate>=0'],
    ...Object.fromEntries(['field','media'].flatMap(role => ['login','refresh','hazards','field-report'].map(operation =>
      [`m2_admission_rejections{role:${role},operation:${operation}}`, ['count>=0']]))),
    ...Object.fromEntries([0,200,202,400,401,403,409,429,500,502,503,504].map(status =>
      [`m2_http_status{status:${status}}`, ['count>=0']])),
    'm2_completed{role:field,operation:login}': ['count>0'],
    'm2_completed{role:field,operation:hazards}': ['count>0'],
    'm2_successful_duration{role:field,operation:hazards}': ['p(95)>=0'],
    'm2_successful_duration{role:media,operation:hazards}': ['p(95)>=0'],
    ...(reportEvery > 0 ? {'m2_completed{role:field,operation:field-report}': ['count>0'],
      'm2_successful_duration{role:field,operation:field-report}': ['p(95)>=0']} : {}) },
  summaryTrendStats: ['min', 'avg', 'med', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: true,
};
let session = null;
let retryReport = null;
function measure(response, role, operation, expected) {
  const tags = { role, operation };
  let success = response.status === expected;
  let partial;
  if (success && operation === 'hazards') {
    try {
      partial = response.json('partial');
      const count = response.json('count');
      const unavailable = response.json('unavailable_units.#');
      const asOf = response.json('as_of');
      success = typeof partial === 'boolean' && Number.isInteger(count) && count >= 0 &&
        response.json('data.#') === count && typeof asOf === 'string' && Number.isFinite(Date.parse(asOf)) &&
        Number.isInteger(unavailable) && (partial ? unavailable > 0 : unavailable === 0);
    } catch (_) { success = false; }
    if (!success) invalidReads.add(1, tags);
  }
  statuses.add(1, { ...tags, status: String(response.status) });
  unsuccessful.add(!success, tags);
  // M2 permits controlled rejection. Retain it in the all-outcome metric, and
  // exempt only explicit rate limits carrying Retry-After from unexpected errors.
  const controlled = response.status === 429 && Boolean(response.headers['Retry-After']);
  unexpectedAll.add(!success && !controlled, tags);
  // The grading rate excludes controlled rejections from its denominator too.
  if (!controlled) unexpected.add(!success, tags);
  if (response.status === 429) rejections.add(1, tags);
  if (response.status === 0) timeouts.add(1, tags);
  if (success) {
    completed.add(1, tags);
    successLatency.add(response.timings.duration, tags);
    if (operation === 'hazards') {
      successfulReads.add(1, tags);
      (partial ? partialReads : completeReads).add(1, tags);
    }
  }
  latency.add(response.timings.duration, tags);
  return success;
}
function token(role) {
  if (session && Date.now() < session.until) return session.access_token;
  const refreshing = session && session.refresh_token;
  const payload = refreshing ? { refresh_token: session.refresh_token } : {
    client_id: role === 'field' ? 'field-demo' : 'media-demo', client_secret: credentials[role] };
  const response = http.post(base + (refreshing ? '/auth/refresh' : '/auth/login'), JSON.stringify(payload), {
    headers: { 'Content-Type': 'application/json' }, timeout: '5s', responseType: 'text',
    tags: { name: refreshing ? 'refresh' : 'login', role },
  });
  if (!measure(response, role, refreshing ? 'refresh' : 'login', 200)) {
    if (response.status === 401) session = null;
    return null;
  }
  session = response.json();
  session.until = Date.now() + Math.max(1, Number(session.expires_in) - 5) * 1000;
  return session.access_token;
}
function run(role) {
  const access = token(role);
  if (!access) { sleep(1); return; }
  const headers = { Authorization: 'Bearer ' + access, 'X-Correlation-ID': `load-${role}-${__VU}-${__ITER}` };
  if (role === 'field' && reportEvery > 0 && (__ITER % reportEvery === 0 || retryReport)) {
    // Preserve the exact key and bytes after an ambiguous/lost response.
    if (!retryReport) retryReport = { key: `load-${__ENV.M2_RUN_ID || 'run'}-${__VU}-${__ITER}`,
      body: JSON.stringify({ report_type: 'coastal_observation', occurred_at: new Date().toISOString(),
        latitude: -6.2, longitude: 106.8, description: 'Synthetic load fixture', attributes: { synthetic: true } }) };
    const response = http.post(base + '/field-reports', retryReport.body, { headers: { ...headers,
      'Content-Type': 'application/json', 'Idempotency-Key': retryReport.key }, timeout: '5s',
      tags: { name: 'field-report', role } });
    if (measure(response, role, 'field-report', 202)) retryReport = null;
  } else {
    const response = http.get(base + '/hazards?limit=50', { headers, timeout: '5s', responseType: 'text', tags: { name: 'hazards', role } });
    measure(response, role, 'hazards', 200);
    if (response.status === 401) session = null;
  }
  sleep(think);
}
export function media() { run('media'); }
export function field() { run('field'); }
export function handleSummary(data) {
  return { [__ENV.M2_SUMMARY_FILE || 'load-summary.json']: JSON.stringify({
    configuration: { vus: total, field_vus: fieldVUs, think_seconds: think, report_every: reportEvery,
      ramp: __ENV.M2_RAMP || '60s', hold: __ENV.M2_HOLD || '120s',
      unexpected_error_denominator: 'successful and unexpected operations; controlled rejections excluded',
      all_outcome_metric: 'm2_unsuccessful' }, ...data }, null, 2) };
}
