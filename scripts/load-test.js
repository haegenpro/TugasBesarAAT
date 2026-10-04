// k6 load test for P2.1 (head-of-line blocking) and P2.2 (sustained load).
// MODE=mixed     : half the VUs request SEISMIC (BMKG-only), half VOLCANIC; p95(seismic) < 300ms.
// MODE=sustained : VUS parallel connections for DURATION on /hazards.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const MODE = __ENV.MODE || 'sustained';
const VUS = parseInt(__ENV.VUS || '60');
const DURATION = __ENV.DURATION || '70s';
const REFRESH_AFTER_MS = 45000; // access token TTL defaults to 60s

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
    return true;
  }
  // A rejected login (429 under overload) is not an error; back off and retry next iteration.
  if (res.status === 429) rejected.add(1);
  else unexpected.add(1);
  token = null;
  return false;
}

export default function () {
  if (!token || Date.now() - obtained > REFRESH_AFTER_MS) {
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
