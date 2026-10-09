// Optional evidence commands. Nothing starts/rebuilds Docker or deletes data.
// schema/outage explicitly exercise their respective source admin endpoints.
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const [mode, ...args] = process.argv.slice(2);
const arg = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
const modes = ['mapping', 'schema', 'storage', 'runtime', 'health', 'trace', 'outage', 'secrets'];
if (!modes.includes(mode)) {
  console.log('Usage: node scripts/evidence-probes.mjs <mapping|schema|storage|runtime|health|trace|outage|secrets>');
  console.log('Options: --name <evidence filename stem>, --seconds <duration>, --project <Compose project>');
  console.log('Runtime default: 80 seconds. Outage default: 600 seconds; restores outage in finally.');
  console.log('Run mapping before schema; schema saves a baseline used by storage. Run runtime in a second terminal during k6.');
  process.exit(mode === '--help' || !mode ? 0 : 2);
}
const name = arg('--name', mode);
if (!/^[A-Za-z0-9._-]+$/.test(name)) throw new Error('Invalid evidence filename');
const evidence = join(root, 'evidence');
mkdirSync(evidence, { recursive: true });
const log = [];
function say(line) { log.push(line); console.log(line); }
function save(file, value) { writeFileSync(join(evidence, file), JSON.stringify(value, null, 2) + '\n'); }
function check(condition, message) {
  say(`${condition ? 'PASS' : 'FAIL'} ${message}`);
  if (!condition) throw new Error(message);
}
function run(exe, argv, options = {}) {
  return execFileSync(exe, argv, { cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024,
    timeout: 30000, stdio: ['pipe', 'pipe', 'pipe'], ...options });
}
const project = arg('--project', null);
const compose = argv => run('docker', ['compose', ...(project ? ['-p', project] : []), ...argv]);
function env(file) {
  return Object.fromEntries(readFileSync(join(root, file), 'utf8').split(/\r?\n/)
    .map(line => /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/.exec(line))
    .filter(Boolean).map(m => [m[1], m[2].replace(/^(['"])(.*)\1$/, '$2')]));
}
const settings = env('.env');
const auth = env('auth-service/.env');
const bmkg = env('bmkg-mock/.env');
const pvmbg = env('pvmbg-mock/.env');
const base = `http://localhost:${settings.CLIENT_API_HOST_PORT}`;
const bbase = `http://localhost:${settings.BMKG_HOST_PORT}`;
const pbase = `http://localhost:${settings.PVMBG_HOST_PORT}`;
const bh = { 'X-BMKG-Key': bmkg.BMKG_API_KEY };
const ph = { Authorization: `Bearer ${pvmbg.PVMBG_TOKEN}` };
const ah = { Authorization: `Bearer ${pvmbg.PVMBG_ADMIN_TOKEN}` };
const wait = ms => new Promise(done => setTimeout(done, ms));
let interrupted = false;
if (mode === 'outage') for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { interrupted = true; });
async function request(url, headers = {}, body) {
  const start = performance.now();
  const response = await fetch(url, { headers: { ...headers, ...(body ? { 'Content-Type': 'application/json' } : {}) },
    method: body ? 'POST' : 'GET', ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(12000) });
  const text = await response.text();
  return { code: response.status, ms: performance.now() - start, body: text ? JSON.parse(text) : null };
}
let session, expiresAt = 0;
async function hazards(type) {
  if (!session) {
    const result = await request(`${base}/auth/login`, {}, { client_id: auth.FIELD_CLIENT_ID, client_secret: auth.FIELD_CLIENT_SECRET });
    if (result.code !== 200) throw new Error(`Field login HTTP ${result.code}`);
    session = result.body;
    expiresAt = Date.now() + session.expires_in * 750;
  } else if (Date.now() >= expiresAt) {
    const result = await request(`${base}/auth/refresh`, {}, { refresh_token: session.refresh_token });
    if (result.code !== 200) throw new Error(`Field refresh HTTP ${result.code}`);
    session = result.body;
    expiresAt = Date.now() + session.expires_in * 750;
  }
  return request(`${base}/hazards${type ? `?hazard_type=${type}` : ''}`, { Authorization: `Bearer ${session.access_token}` });
}
async function until(fn, label, seconds = 120) {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) {
    if (interrupted) throw new Error('Interrupted; restoring outage before exit');
    const result = await fn();
    if (result) return result;
    await wait(2000);
  }
  throw new Error(`Timed out: ${label}`);
}
// Pass query on stdin rather than building a shell command containing SQL.
function query(queryText) {
  return run('docker', ['compose', ...(project ? ['-p', project] : []), 'exec', '-T', 'postgres',
    'sh', '-c', 'psql -X -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'], { input: queryText });
}
function snapshot() {
  const ids = compose(['ps', '--all', '-q']).trim().split(/\s+/).filter(Boolean);
  if (!ids.length) throw new Error('No containers found in selected Compose project');
  return JSON.parse(run('docker', ['inspect', ...ids])).map(c => ({
    service: c.Config.Labels['com.docker.compose.service'], id: c.Id, started: c.State.StartedAt,
    restarts: c.RestartCount, state: c.State.Status, health: c.State.Health?.Status || 'none',
    health_command: c.Config.Healthcheck?.Test || [],
  })).sort((a, b) => a.service.localeCompare(b.service));
}
function stable(before, after) {
  return before.length === after.length && before.every(a => after.some(b =>
    a.service === b.service && a.id === b.id && a.started === b.started && a.restarts === b.restarts));
}
function catalog() {
  return JSON.parse(query(`SELECT coalesce(json_agg(t ORDER BY t.table_name,t.ordinal_position),'[]') FROM
    (SELECT table_name,column_name,data_type,ordinal_position FROM information_schema.columns
     WHERE table_schema='public') t;`));
}
const equivalent = (a, b) => JSON.stringify(a) === JSON.stringify(b);
function equal(a, b, label) { if (!equivalent(a, b)) throw new Error(`Mapping mismatch: ${label}`); }
function validHazard(h) {
  const keys = ['hazard_id', 'source', 'source_ref_id', 'hazard_type', 'severity', 'area_name',
    'latitude', 'longitude', 'occurred_at', 'ingested_at', 'attributes'].sort();
  equal(Object.keys(h).sort(), keys, `${h.source_ref_id}: canonical fields`);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(h.hazard_id)
      || !['NORMAL', 'WASPADA', 'SIAGA', 'AWAS'].includes(h.severity)
      || !Number.isFinite(Date.parse(h.ingested_at)) || !Number.isFinite(Date.parse(h.occurred_at))
      || !Number.isFinite(h.latitude) || !Number.isFinite(h.longitude)
      || !h.attributes || Array.isArray(h.attributes)) throw new Error(`Invalid HazardEvent ${h.source_ref_id}`);
}

async function mapping() {
  const [seismic, warnings, reports] = await Promise.all([
    request(`${bbase}/seismic-events`, bh), request(`${bbase}/tsunami-warnings`, bh), request(`${pbase}/volcanic-reports`, ph),
  ]);
  check([seismic, warnings, reports].every(r => r.code === 200), 'all source endpoints HTTP 200');
  const refs = JSON.parse(query('SELECT json_agg(t) FROM (SELECT * FROM volcano_reference ORDER BY volcano_id) t;'));
  const reference = new Map(refs.map(r => [r.volcano_id, r]));
  const rank = { Waspada: 1, Siaga: 2, Awas: 3 };
  const pairs = [];
  await until(async () => {
    const result = await hazards();
    if (result.code !== 200) return false;
    const rows = new Map(result.body.data.map(r => [`${r.source}:${r.source_ref_id}`, r]));
    if (!seismic.body.every(s => rows.has(`BMKG:${s.event_id}`)) || !reports.body.every(s => rows.has(`PVMBG:${s.report_id}`))) return false;
    pairs.length = 0;
    for (const s of seismic.body) {
      const h = rows.get(`BMKG:${s.event_id}`);
      validHazard(h);
      const w = s.potential_tsunami ? warnings.body.filter(w => w.related_event_id === s.event_id)
        .sort((a, b) => rank[b.threat_level] - rank[a.threat_level])[0] : null;
      equal(h.hazard_type, 'SEISMIC', s.event_id);
      equal(h.area_name, s.region_name, s.event_id);
      equal(h.latitude, s.epicenter_lat, s.event_id); equal(h.longitude, s.epicenter_lon, s.event_id);
      equal(Date.parse(h.occurred_at), Date.parse(s.occurred_at), s.event_id);
      equal(h.severity, w ? w.threat_level.toUpperCase() : s.magnitude < 5 ? 'NORMAL' : s.magnitude < 6.5 ? 'WASPADA' : 'SIAGA', s.event_id);
      for (const key of ['magnitude', 'depth_km', 'potential_tsunami']) equal(h.attributes[key], s[key], `${s.event_id}.${key}`);
      if (w) for (const key of ['warning_id', 'related_event_id', 'threat_level', 'affected_zones']) equal(h.attributes[key], w[key], `${s.event_id}.${key}`);
      if (w) equal(Date.parse(h.attributes.estimated_arrival), Date.parse(w.estimated_arrival), s.event_id);
      pairs.push({ source: s, warning: w, canonical: h });
    }
    for (const s of reports.body) {
      const h = rows.get(`PVMBG:${s.report_id}`), ref = reference.get(s.volcano_id);
      validHazard(h);
      if (!ref) throw new Error(`No reference for ${s.volcano_id}`);
      equal(h.hazard_type, 'VOLCANIC', s.report_id); equal(h.severity, s.alert_level.toUpperCase(), s.report_id);
      for (const key of ['area_name', 'latitude', 'longitude']) equal(h[key], ref[key], s.report_id);
      equal(Date.parse(h.occurred_at), Date.parse(s.reported_at), s.report_id);
      for (const key of ['eruption_count_24h', 'ash_column_height_m']) equal(h.attributes[key], s[key], s.report_id);
      if ('confidence_level' in s) equal(h.attributes.confidence_level, s.confidence_level, s.report_id);
      pairs.push({ source: s, reference: ref, canonical: h });
    }
    return true;
  }, 'source records ingested');
  save(`${name}.json`, pairs);
  say(`Matched ${seismic.body.length} seismic and ${reports.body.length} volcanic records; ${warnings.body.length} warning records.`);
  check(pairs.length > 0, 'source fields, severity, timestamps and volcano references match canonical records');
  say('Exact 5.0 / 6.5 boundaries: capture go test -v ./internal/mapping in aggregator separately.');
}

async function schema() {
  const before = (await hazards('VOLCANIC')).body;
  check(before.data.length > 0, 'pre-change canonical volcanic records exist');
  const source = await request(`${pbase}/volcanic-reports`, ph);
  check(source.body.every(r => !('confidence_level' in r)), 'source is on old schema before toggle');
  const starts = snapshot(), columns = catalog(), ids = new Set(source.body.map(r => r.report_id));
  save('schema-before.json', { canonical: before, source: source.body, containers: starts, columns });
  const toggle = await request(`${pbase}/admin/schema-version`, ah, {});
  check(toggle.code === 200 && toggle.body.schema_version === 'v2', 'live schema toggle returned v2');
  const changed = await request(`${pbase}/volcanic-reports`, ph);
  const fresh = changed.body.find(r => !ids.has(r.report_id) && r.report_id.startsWith('rep-schema-'));
  check(Boolean(fresh), 'discover actual new report ID instead of a hardcoded ID');
  say(`New report: ${fresh.report_id}`);
  const h = await until(async () => {
    const result = await hazards('VOLCANIC');
    return result.body?.data.find(r => r.source_ref_id === fresh.report_id && r.attributes.confidence_level === fresh.confidence_level);
  }, 'new schema mapped');
  save('2.3-new-report.json', h);
  validHazard(h);
  save('schema-after.json', { source: fresh, canonical: h, containers: snapshot(), columns: catalog() });
  check(!('confidence_level' in h) && h.attributes.confidence_level === 0.88, 'new field preserved in attributes; canonical shape unchanged');
  check(stable(starts, snapshot()), 'all container IDs, start times and restart counts unchanged');
  check(equivalent(columns, catalog()), 'database column catalog unchanged across live schema toggle');
}

async function storage() {
  const before = JSON.parse(readFileSync(join(evidence, 'schema-before.json'), 'utf8'));
  const after = JSON.parse(readFileSync(join(evidence, 'schema-after.json'), 'utf8'));
  const rows = (await hazards('VOLCANIC')).body.data;
  const old = rows.find(r => r.hazard_id === before.canonical.data[0].hazard_id);
  const fresh = rows.find(r => r.hazard_id === after.canonical.hazard_id);
  check(Boolean(old && fresh && old.hazard_id !== fresh.hazard_id), 'pre-change and post-change volcanic rows coexist and are readable through API');
  check(equivalent(before.columns, after.columns) && equivalent(after.columns, catalog()), 'no column migration for confidence_level');
  check(catalog().some(c => c.table_name === 'hazard_events' && c.column_name === 'attributes' && c.data_type === 'jsonb'), 'attributes stored in JSONB');
  save(`${name}.json`, { old_before: before.canonical.data[0], old_now: old, new_now: fresh, columns: catalog() });
  say(`Old: ${old.source_ref_id} / ${old.hazard_id}; confidence=${old.attributes.confidence_level ?? 'absent'}`);
  say(`New: ${fresh.source_ref_id} / ${fresh.hazard_id}; confidence=${fresh.attributes.confidence_level}`);
}

async function runtime() {
  const seconds = Number(arg('--seconds', '80'));
  if (!(seconds > 0)) throw new Error('--seconds must be positive');
  const before = snapshot(), samples = [], start = Date.now();
  save(`${name}-before.json`, before);
  while (Date.now() - start < seconds * 1000) {
    const containers = snapshot();
    const health = await request(`${base}/health`);
    const tcp = compose(['exec', '-T', 'client-api', 'cat', '/proc/net/tcp', '/proc/net/tcp6']);
    const port = Number(env('client-api/.env').CLIENT_API_PORT).toString(16).toUpperCase().padStart(4, '0');
    const connections = tcp.split('\n').filter(l => {
      const fields = l.trim().split(/\s+/); return fields[1]?.endsWith(`:${port}`) && fields[3] === '01';
    }).length;
    const sample = { time: new Date().toISOString(), seconds: (Date.now() - start) / 1000,
      connections, http: health.code, latency_ms: health.ms, stable: stable(before, containers),
      healthy: containers.every(c => c.state === 'running' && c.health === 'healthy'), containers };
    samples.push(sample);
    say(`${sample.time} established_tcp=${connections} health=${health.code} ${health.ms.toFixed(1)}ms stable=${sample.stable} all_healthy=${sample.healthy}`);
    await wait(2000);
  }
  const after = snapshot(); save(`${name}-after.json`, after); save(`${name}.json`, samples);
  check(stable(before, after) && samples.every(s => s.stable && s.healthy && s.http === 200), 'no observed crash/recreate/restart/unhealthy sample during window');
  say(`TCP observations: min=${Math.min(...samples.map(s => s.connections))}, max=${Math.max(...samples.map(s => s.connections))}; sampling interval >=2s.`);
  say('Connection samples include idle keep-alive sockets and probes; they are not simultaneous in-flight request counts. Match timestamps to the k6 window.');
}

async function health() {
  const rows = snapshot();
  for (const c of rows) {
    check(c.state === 'running' && c.health === 'healthy', `${c.service}: ${c.state}, ${c.health}`);
    say(`  Health command: ${JSON.stringify(c.health_command)}`);
  }
  save(`${name}.json`, rows);
  say('HTTP checks cover application services; Postgres uses pg_isready. CLI-only TUI has no HTTP server.');
}

async function trace() {
  const text = compose(['logs', '--no-color', '--since', '5m', 'aggregator', 'bmkg-mock', 'pvmbg-mock', 'consumer-dashboard', 'consumer-notifier']);
  const rows = text.split('\n').map(l => { try { return JSON.parse(l.slice(l.indexOf('{'))); } catch { return null; } }).filter(r => r?.correlation_id);
  const groups = Map.groupBy ? Map.groupBy(rows, r => r.correlation_id) : rows.reduce((m, r) => m.set(r.correlation_id, [...(m.get(r.correlation_id) || []), r]), new Map());
  const selected = [];
  for (const service of ['bmkg-mock', 'pvmbg-mock', 'consumer-dashboard', 'consumer-notifier']) {
    const pair = [...groups].find(([, group]) => group.some(r => r.service === service) && group.some(r => r.service === 'aggregator'));
    check(Boolean(pair), `correlation ID shared by aggregator and ${service}`);
    const [cid, group] = pair;
    say(`correlation_id=${cid}; services=${[...new Set(group.map(r => r.service))].join(',')}`);
    const calls = group.filter(r => r.msg === 'outbound call' && Number.isFinite(r.latency_ms));
    check(calls.length > 0, `${cid}: outbound latency recorded`);
    for (const r of calls) say(`  ${r.service} -> ${r.target_service || r.operation || 'outbound'} latency_ms=${r.latency_ms}`);
    selected.push(...group);
  }
  save(`${name}.json`, selected);
}

async function outage() {
  const seconds = Number(arg('--seconds', '600'));
  if (!(seconds > 0)) throw new Error('--seconds must be positive');
  const before = await hazards('VOLCANIC'), starts = snapshot(), samples = [];
  check(before.code === 200 && before.body.data.length > 0 && before.body.source_status.PVMBG.state === 'fresh', 'warm cache and fresh PVMBG baseline');
  save(`${name}-before.json`, before);
  const start = Date.now();
  try {
    check((await request(`${pbase}/admin/outage`, ah, { enabled: true })).code === 200, 'outage enabled');
    check((await request(`${pbase}/volcanic-reports`, ph)).code === 503, 'source data endpoint HTTP 503');
    await until(async () => (await hazards('VOLCANIC')).body.source_status.PVMBG.state === 'stale', 'stale marker');
    const during = await hazards('VOLCANIC'); save(`${name}-during.json`, during);
    const cached = new Map(during.body.data.map(r => [r.hazard_id, r]));
    check(before.body.data.every(r => equivalent(r, cached.get(r.hazard_id))), 'pre-outage canonical IDs/content preserved in cache');
    check(Boolean(during.body.source_status.PVMBG.stale_since), 'stale_since supplied');
    while (Date.now() - start < seconds * 1000) {
      if (interrupted) throw new Error('Interrupted; restoring outage before exit');
      const volcanic = await hazards('VOLCANIC'), seismic = await hazards('SEISMIC');
      check(volcanic.code === 200 && seismic.code === 200 && volcanic.body.source_status.PVMBG.state === 'stale', 'both reads served; volcanic marked stale');
      const cachedNow = new Map(volcanic.body.data.map(r => [r.hazard_id, r]));
      check(before.body.data.every(r => equivalent(r, cachedNow.get(r.hazard_id))), 'cached IDs/content remain readable during outage');
      const row = { time: new Date().toISOString(), elapsed_s: (Date.now() - start) / 1000,
        volcanic_http: volcanic.code, volcanic_ms: volcanic.ms, seismic_http: seismic.code, seismic_ms: seismic.ms,
        stale_since: volcanic.body.source_status.PVMBG.stale_since, volcanic_count: volcanic.body.count };
      samples.push(row); say(JSON.stringify(row));
      await wait(Math.min(10000, Math.max(0, seconds * 1000 - (Date.now() - start))));
    }
  } finally {
    save(`${name}-timeline.json`, samples);
    check((await request(`${pbase}/admin/outage`, ah, { enabled: false })).code === 200, 'outage disabled');
  }
  if (interrupted) throw new Error('Interrupted; outage restored, full-duration evidence incomplete');
  const source = await request(`${pbase}/volcanic-reports`, ph);
  const seen = new Set(before.body.data.map(r => r.source_ref_id));
  const fresh = source.body.find(r => !seen.has(r.report_id));
  check(Boolean(fresh), 'source produced a new record during outage');
  const after = await until(async () => {
    const r = await hazards('VOLCANIC');
    return r.code === 200 && r.body.source_status.PVMBG.state === 'fresh' && r.body.data.some(h => h.source_ref_id === fresh.report_id) ? r : null;
  }, 'fresh state and new ingestion after recovery', Number(env('aggregator/.env').SOURCE_RETRY_MAX_SEC) + 45);
  save(`${name}-after.json`, after);
  say(`Recovered new report: ${fresh.report_id}; held outage for at least ${seconds}s.`);
  check(stable(starts, snapshot()), 'recovered without restart, recreation or changed restart counts');
}

async function secrets() {
  const files = run('git', ['ls-files', '-z']).split('\0').filter(Boolean);
  check(!files.some(p => /(^|\/)\.env(?:$|\.)/.test(p) && !p.endsWith('.example')), 'no real env files tracked');
  check(files.includes('.env.example'), '.env.example tracked');
  const candidates = new Set();
  const envFiles = ['.env', ...['auth-service', 'bmkg-mock', 'pvmbg-mock', 'aggregator', 'client-api', 'store', 'registry', 'broker', 'consumer-dashboard', 'consumer-notifier', 'monitoring/grafana'].map(s => `${s}/.env`)];
  for (const file of envFiles) if (existsSync(join(root, file))) for (const [key, value] of Object.entries(env(file))) {
    if (/(?:SECRET|TOKEN|PASSWORD|(?:^|_)KEY)$/.test(key) && value) candidates.add(value);
    if (key.endsWith('_URL')) { try { const password = new URL(value).password; if (password) candidates.add(decodeURIComponent(password)); } catch {} }
  }
  const objects = run('git', ['rev-list', '--objects', '--all']).trim().split('\n').map(line => {
    const space = line.indexOf(' '); return [space < 0 ? line : line.slice(0, space), space < 0 ? '' : line.slice(space + 1)];
  });
  let blobs = 0, findings = 0;
  const inspect = (text, label, path) => {
    if (/(^|\/)\.env(?:$|\.)/.test(path) && !path.endsWith('.example')) { say(`REVIEW ${label}: real env file in repository/history`); findings++; }
    if ([...candidates].some(value => text.includes(value))) { say(`REVIEW ${label}: current secret value found (value redacted)`); findings++; }
    if (/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/.test(text)) { say(`REVIEW ${label}: private key material`); findings++; }
    // Literal assignments can find former values absent from today's env files.
    // Templates, docs and tests are reviewed separately as intentional examples.
    if (!/(?:^|\/)(?:docs|evidence)\/|\.example$|_test\.go$|(?:^|\/)(?:test|tests)\//.test(path)) {
      const literals = /\b[A-Za-z_]\w*(?:SECRET|TOKEN|PASSWORD|KEY)["']?\s*(?::=|=|:)\s*["']([^"'\r\n]{8,})["']/gi;
      for (const match of text.matchAll(literals)) if (!/^(?:\$|%|https?:|nats:)/.test(match[1])) {
        say(`REVIEW ${label}: potential hardcoded secret assignment (value redacted)`); findings++; break;
      }
    }
  };
  for (const file of files) if (existsSync(join(root, file))) inspect(readFileSync(join(root, file), 'utf8'), `working:${file}`, file);
  for (const [id, path] of objects) if (run('git', ['cat-file', '-t', id]).trim() === 'blob') {
    blobs++; inspect(run('git', ['cat-file', '-p', id]), `history:${id.slice(0, 12)}:${path}`, path);
  }
  say(`Scanned ${files.length} working files and ${blobs} reachable historical blobs; findings=${findings}. Values never printed.`);
  say('Scope: current configured secrets (including signing/internal keys and DB passwords), all file types/history, private-key markers, and heuristic literal assignments.');
  say('Former unknown secrets still require general secret scanning and manual review; these heuristics cannot prove their absence.');
  check(findings === 0, 'no findings within declared scan scope');
}

say(`M1 EVIDENCE: ${mode}; UTC ${new Date().toISOString()}; project=${project || process.env.COMPOSE_PROJECT_NAME || 'Compose default'}`);
try {
  await ({ mapping, schema, storage, runtime, health, trace, outage, secrets }[mode])();
  say('RESULT: PASS (within the printed scope)');
} catch (error) {
  // Do not print execFile errors: they can contain commands or sensitive output.
  say(`RESULT: FAIL${error.status !== undefined ? ` (subprocess exit ${error.status})` : `: ${error.message}`}`);
  process.exitCode = 1;
} finally { writeFileSync(join(evidence, `${name}.txt`), log.join('\n') + '\n'); }
