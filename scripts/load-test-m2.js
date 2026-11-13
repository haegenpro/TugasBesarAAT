// Deferred M2 sustained workload. Token inventory stays outside the repository.
import http from 'k6/http';
import { sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Rate, Trend } from 'k6/metrics';

const vus = Number(__ENV.VUS || '10000');
const seconds = Number(__ENV.DURATION_SECONDS || '150');
if (!Number.isInteger(vus) || vus < 10000 || !Number.isInteger(seconds) || seconds < 120) {
  throw new Error('M2 requires >=10000 VUs and >=120 seconds');
}
const base = __ENV.BASE_URL;
if (!base || !/^https?:\/\//.test(base) || !__ENV.AUTH_TOKEN_FILE || !__ENV.RUN_ID) {
  throw new Error('BASE_URL, AUTH_TOKEN_FILE and RUN_ID are required');
}
function tokens(role) {
  return new SharedArray(`tokens-${role}`, () => {
    const rows = JSON.parse(open(__ENV.AUTH_TOKEN_FILE)).filter(row => row.role === role);
    if (!rows.length || rows.some(row => typeof row.access_token !== 'string' || !row.access_token)) {
      throw new Error('Provide valid field and media token inventories');
    }
    return rows;
  });
}
const media = tokens('media');
const field = tokens('field');
const attempts = new Counter('m2_attempts');
const good = new Counter('m2_successful_reads');
const rejected = new Counter('m2_controlled_429');
const errors = new Counter('m2_unexpected_errors');
const errorRate = new Rate('m2_unexpected_failure_rate');
const latency = new Trend('m2_successful_duration', true);
const fieldVus = Math.max(1, Math.floor(vus * 0.05));

http.setResponseCallback(http.expectedStatuses(200, 429));
export const options = {
  scenarios: {
    media: { executor: 'constant-vus', vus: vus - fieldVus, duration: `${seconds}s`, exec: 'mediaReads' },
    field: { executor: 'constant-vus', vus: fieldVus, duration: `${seconds}s`, exec: 'fieldReads' },
  },
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    m2_unexpected_failure_rate: ['rate<0.01'],
    'm2_successful_reads{role:field}': ['count>0'],
    'm2_successful_reads{role:media}': ['count>0'],
    'm2_successful_duration{role:field}': ['p(95)>=0'],
    'm2_successful_duration{role:media}': ['p(95)>=0'],
    ...(__ENV.EXPECT_OVERLOAD === 'true' ? { 'm2_controlled_429{role:media}': ['count>0'] } : {}),
  },
};

function read(role, pool) {
  const token = pool[(__VU - 1) % pool.length].access_token;
  const response = http.get(`${base.replace(/\/$/, '')}/hazards?limit=25`, {
    headers: { Authorization: `Bearer ${token}`, 'X-Correlation-ID': `${__ENV.RUN_ID}-${role}-${__VU}-${__ITER}` },
    tags: { role }, timeout: '10s', redirects: 0,
  });
  attempts.add(1, { role });
  if (response.status === 429) { rejected.add(1, { role }); }
  else {
    // Controlled rejection does not dilute the unexpected-error denominator.
    errorRate.add(response.status !== 200, { role });
    if (response.status === 200) { good.add(1, { role }); latency.add(response.timings.duration, { role }); }
    else errors.add(1, { role });
  }
  sleep(0.05);
}
export function mediaReads() { read('media', media); }
export function fieldReads() { read('field', field); }
export function handleSummary(data) {
  const report = { configuration: { version: 1, run_id: __ENV.RUN_ID, vus, duration_seconds: seconds,
    field_vus: fieldVus, workload: 'authenticated-hazard-reads', think_time_seconds: 0.05,
    unexpected_error_denominator: 'non-429 attempts', critical_delivery_proven: false }, ...data };
  const output = { stdout: 'M2 load finished; inspect measurements, replica events and owner reconciliation before acceptance.\n' };
  if (__ENV.SUMMARY_PATH) output[__ENV.SUMMARY_PATH] = JSON.stringify(report, null, 2);
  return output;
}
