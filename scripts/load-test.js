// k6 load test for P2.1 (head-of-line blocking) and P2.2 (sustained load).
// MODE=mixed     : half the VUs request SEISMIC (BMKG-only), half VOLCANIC; p95(seismic) < 300ms.
// MODE=sustained : VUS parallel connections for DURATION on /hazards.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

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
required('CLIENT_ID');
required('CLIENT_SECRET');
if (!['sustained', 'mixed'].includes(MODE)) throw new Error('MODE must be sustained or mixed');
// Re-login follows the actual lifetime returned by Auth, rather than assuming a TTL.
let refreshAfterMs = 0;

const rejected = new Counter('controlled_rejections_429');
const unexpected = new Counter('unexpected_errors');

http.setResponseCallback(http.expectedStatuses(200, 429));

export const options = {
  scenarios: { load: { executor: 'constant-vus', vus: VUS, duration: DURATION } },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    unexpected_errors: ['count>=0'],
    controlled_rejections_429: ['count>=0'],   // forces the counter to print even when 0
    ...(MODE === 'mixed' ? { 'http_req_duration{kind:volcanic}': ['p(95)>=0'] } : {}), // forces a volcanic line in the summary
    ...(MODE === 'mixed' ? { 'http_req_duration{kind:seismic}': ['p(95)<300'] } : {}),
    http_req_failed: ['rate<0.01'],
  },
};

let token = null;
let obtained = 0;

function login() {
  const res = http.post(`${BASE}/auth/login`,
    JSON.stringify({ client_id: __ENV.CLIENT_ID, client_secret: __ENV.CLIENT_SECRET }),
    { headers: { 'Content-Type': 'application/json' }, tags: { kind: 'login' } });
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
  const res = http.get(`${BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { kind },
  });
  if (res.status === 429) rejected.add(1);
  else if (res.status !== 200) { unexpected.add(1); if (res.status === 401) token = null; }
  sleep(0.05);
}
