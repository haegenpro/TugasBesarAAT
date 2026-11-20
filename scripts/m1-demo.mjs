// Operator interface for live M1 demos. No dependencies beyond Node.js 18+ and Docker.
import { execFileSync, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync, unlinkSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const HELP = `Usage: node scripts/m1-demo.mjs COMMAND [arguments] [--project NAME]

  preflight                     Check config, Docker, source delays, API and consumers
  request media|field|internal [PATH] [--expect CODE]
                                GET /hazards by default; print JSON and latency
  source bmkg|tsunami|pvmbg      Read source JSON, credentials hidden, print latency
  db summary|latest|schema|psql  Inspect canonical DB (psql opens interactive CLI)
  records [dashboard|notifier|demo]  Inspect consumer records through its own API
  broker                        Stream, durable names, pending/ack counters
  fanout [--rebuild]             Stop notifier, follow a NEW event, restart and verify replay
                                --rebuild also builds/recreates only the notifier (P4)
  third-consumer                Add consumer-dashboard-demo; verify a NEW event reaches all 3
  ownership                     Verify store network isolation and API access
  delay 3000|restore            Save/restore PVMBG delay; recreate only PVMBG
  outage on|off                 Explicit PVMBG outage control for scenario director

All non-interactive commands save timestamped evidence/m1-*.json (no login tokens).
--help is offline. fanout always attempts to start notifier in finally, including Ctrl+C.
Pre-build images before the demo. Existing evidence-probes.mjs covers mapping/schema/storage,
trace/runtime/long outage; smoke.sh covers natural token expiry; load-test.sh runs k6.
`;

// Read dotenv data without executing shell code. Compose interpolation is left literal.
export function parseEnv(text) {
  const result = {};
  for (const line of text.replace(/^\uFEFF/, '').split(/\r?\n/)) {
    if (!line.trim() || line.trimStart().startsWith('#')) continue;
    const match = /^\s*(?:export\s+)?([A-Za-z_]\w*)\s*=\s*(.*?)\s*$/.exec(line);
    if (!match) throw new Error('Invalid .env entry');
    const [, key, raw] = match;
    if (Object.hasOwn(result, key)) throw new Error(`Duplicate .env key: ${key}`);
    let value;
    if (raw.startsWith('"') || raw.startsWith("'")) {
      const quoted = /^(['"])((?:\\.|[^\\])*?)\1\s*(?:#.*)?$/.exec(raw);
      if (!quoted) throw new Error(`Invalid quoted .env value: ${key}`);
      value = quoted[2];
      if (quoted[1] === '"') value = value.replace(/\\([nrt"\\$])/g,
        (_, c) => ({ n: '\n', r: '\r', t: '\t' }[c] ?? c));
      else value = value.replace(/\\'/g, "'");
    } else value = raw.replace(/\s+#.*$/, '').trimEnd();
    result[key] = value;
  }
  return result;
}

export function unchanged(before, after, except = []) {
  const a = before.filter(c => !except.includes(c.service));
  const b = after.filter(c => !except.includes(c.service));
  return a.length === b.length && a.every(c => b.some(d => c.service === d.service
    && c.id === d.id && c.started === d.started && c.restarts === d.restarts && c.state === d.state));
}

export function createDemo({ root = ROOT, project, run, fetcher = fetch, log = console.log,
  sleep = ms => new Promise(done => setTimeout(done, ms)), cancelled = () => false } = {}) {
  run ??= (exe, argv, options = {}) => execFileSync(exe, argv, {
    cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024,
    timeout: 30000, stdio: ['pipe', 'pipe', 'pipe'], ...options,
  });
  const lines = [], artifacts = [];
  const say = message => { lines.push(message); log(message); };
  const check = (condition, message) => {
    say(`${condition ? 'PASS' : 'FAIL'} ${message}`);
    if (!condition) throw new Error(message);
  };
  const config = file => parseEnv(readFileSync(join(root, file), 'utf8'));
  const required = (values, key) => {
    if (!values[key]) throw new Error(`Missing configuration: ${key}`);
    return values[key];
  };
  const composeArgs = argv => ['compose', '-f', join(root, 'docker-compose.yml'), ...(project ? ['-p', project] : []), ...argv];
  const compose = (argv, options) => run('docker', composeArgs(argv), options);
  const settings = config('.env');
  const url = key => {
    const port = Number(required(settings, key));
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error(`Invalid port: ${key}`);
    return `http://localhost:${port}`;
  };
  const api = url('CLIENT_API_HOST_PORT');
  async function request(target, headers = {}, body) {
    const cid = `m1-demo-${randomUUID()}`, start = performance.now();
    const response = await fetcher(target, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { ...headers, 'X-Correlation-ID': cid, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: AbortSignal.timeout(12000),
    });
    const text = await response.text();
    let data;
    try { data = JSON.parse(text); } catch { data = text; }
    const result = { http: response.status, latency_ms: +(performance.now() - start).toFixed(2), correlation_id: cid, data };
    say(`${body === undefined ? 'GET' : 'POST'} ${new URL(target).pathname}${new URL(target).search}: HTTP ${result.http}, ${result.latency_ms} ms, correlation_id=${cid}`);
    return result;
  }
  async function login(role) {
    if (!['media', 'field', 'internal'].includes(role)) throw new Error('Role must be media, field or internal');
    const auth = config('auth-service/.env'), prefix = role.toUpperCase();
    const result = await request(`${api}/auth/login`, {}, {
      client_id: required(auth, `${prefix}_CLIENT_ID`), client_secret: required(auth, `${prefix}_CLIENT_SECRET`),
    });
    check(result.http === 200 && result.data.role === role && result.data.access_token, `login ${role} (token hidden)`);
    return result.data;
  }
  async function hazards(path = '/hazards', role = 'field', expected = 200) {
    // Prevent credentials being forwarded to arbitrary URLs supplied as a path.
    if (!/^\/hazards(?:[/?]|$)/.test(path) || path.includes('\\')) throw new Error('Path must start with /hazards');
    const target = new URL(path, api);
    if (target.origin !== api) throw new Error('Cross-origin request refused');
    const session = await login(role);
    const result = await request(target.href, { Authorization: `Bearer ${session.access_token}` });
    if (expected === 200 && result.http === 200 && Array.isArray(result.data?.data) && !target.searchParams.has('limit') && !target.searchParams.has('cursor')) {
      const seen = new Set();
      while (result.data.next_cursor) {
        if (seen.has(result.data.next_cursor)) throw new Error('Pagination cursor repeated');
        seen.add(result.data.next_cursor);
        const next = new URL(target); next.searchParams.set('cursor', result.data.next_cursor);
        const page = await request(next.href, { Authorization: `Bearer ${session.access_token}` });
        check(page.http === 200, 'next hazard page available');
        result.data.data.push(...page.data.data); result.data.metadata = { ...result.data.metadata, ...page.data.metadata };
        result.data.partial ||= page.data.partial; result.data.unavailable_units = [...new Set([...(result.data.unavailable_units || []), ...(page.data.unavailable_units || [])])];
        result.data.next_cursor = page.data.next_cursor;
      }
      result.data.count = result.data.data.length;
    }
    check(result.http === expected, `${role} ${path}: expected HTTP ${expected}`);
    return result;
  }
  function internal(service, path) {
    const allowed = ['consumer-dashboard', 'consumer-notifier', 'consumer-dashboard-demo'];
    if (!allowed.includes(service)) throw new Error('Unknown consumer');
    if (!['/records', '/ready'].includes(path)) throw new Error('Unknown consumer path');
    return JSON.parse(compose(['exec', '-T', service, 'sh', '-c',
      'wget -qO- "http://localhost:${CONSUMER_PORT:?}$1"', 'sh', path]));
  }
  const records = service => internal(service, '/records').data;
  function snapshot() {
    const ids = compose(['ps', '--all', '-q']).trim().split(/\s+/).filter(Boolean);
    if (!ids.length) throw new Error('No containers in selected Compose project');
    return JSON.parse(run('docker', ['inspect', ...ids])).map(c => ({
      service: c.Config.Labels['com.docker.compose.service'], id: c.Id, started: c.State.StartedAt,
      restarts: c.RestartCount, state: c.State.Status, health: c.State.Health?.Status,
      networks: Object.keys(c.NetworkSettings.Networks),
    }));
  }
  async function until(fn, label, seconds = 90, allowCancelled = false) {
    const end = Date.now() + seconds * 1000;
    while (Date.now() < end) {
      if (cancelled() && !allowCancelled) throw new Error('Interrupted');
      const value = await fn();
      if (value) return value;
      say(`WAIT ${label}`);
      await sleep(2000);
    }
    throw new Error(`Timeout: ${label}`);
  }
  async function waitRecord(service, id) {
    return until(() => records(service).find(r => r.hazard_id === id), `${service} receives ${id}`);
  }
  function broker() {
    const data = JSON.parse(compose(['exec', '-T', 'broker', 'sh', '-c',
      'wget -qO- "http://localhost:${NATS_MONITOR_PORT:?}/jsz?streams=true&consumers=true&config=true"']));
    const streamName = required(config('broker/.env'), 'NATS_STREAM');
    const stream = (data.account_details || []).flatMap(a => a.stream_detail || []).find(s => s.name === streamName);
    check(Boolean(stream), `broker stream ${streamName} exists`);
    const summary = { name: stream.name, subjects: stream.config?.subjects, state: stream.state,
      consumers: (stream.consumer_detail || []).map(c => ({ name: c.name, durable: c.config?.durable_name,
        pending: c.num_pending, ack_pending: c.num_ack_pending, redelivered: c.num_redelivered, ack_floor: c.ack_floor })) };
    artifacts.push({ broker: summary });
    say(JSON.stringify(summary, null, 2));
    return summary;
  }
  async function newEvent() {
    const baseline = (await hazards()).data.data;
    check(Array.isArray(baseline) && baseline.length > 0, 'canonical baseline exists');
    const ids = new Set(baseline.map(r => r.hazard_id));
    const event = await until(async () => (await hazards()).data.data.find(r => !ids.has(r.hazard_id)), 'new canonical hazard');
    say(`NEW hazard_id=${event.hazard_id} source=${event.source} source_ref_id=${event.source_ref_id}`);
    return event;
  }
  async function fanout(rebuild = false) {
    internal('consumer-dashboard', '/ready'); internal('consumer-notifier', '/ready');
    const before = snapshot();
    check(before.some(c => c.service === 'consumer-notifier' && c.state === 'running'), 'notifier initially running');
    artifacts.push({ before });
    let event;
    try {
      compose(['stop', 'consumer-notifier']);
      check(snapshot().some(c => c.service === 'consumer-notifier' && c.state === 'exited'), 'notifier actually stopped');
      event = await newEvent(); // baseline is taken AFTER the stop, so event was published during downtime.
      const dashboard = await waitRecord('consumer-dashboard', event.hazard_id);
      artifacts.push({ event_during_downtime: event, dashboard });
      check(Boolean(dashboard), 'dashboard receives new event while notifier is stopped');
      const during = snapshot();
      check(unchanged(before, during, ['consumer-notifier']), 'other container IDs/start times/restarts/states unchanged during downtime');
      const stream = broker();
      const durable = config('consumer-notifier/.env').NATS_DURABLE;
      check(stream.consumers.some(c => c.name === durable && c.pending > 0), 'stopped notifier has a broker backlog');
      if (rebuild) {
        say('Building only consumer-notifier (may take several minutes).');
        compose(['build', 'consumer-notifier'], { timeout: 600000 });
        compose(['up', '-d', '--no-deps', '--force-recreate', 'consumer-notifier']);
      }
    } finally {
      // Attempt restoration even if stop/build/check or an operator interrupt failed.
      compose(['start', 'consumer-notifier']);
      say('RESTORE consumer-notifier start requested');
    }
    if (cancelled()) throw new Error('Interrupted; notifier start requested');
    await until(() => {
      try { return internal('consumer-notifier', '/ready'); } catch { return null; }
    }, 'notifier ready');
    const notifier = await waitRecord('consumer-notifier', event.hazard_id), after = snapshot();
    artifacts.push({ notifier_after_recovery: notifier, after });
    check(unchanged(before, after, ['consumer-notifier']), 'all unrelated containers unchanged after recovery');
    if (rebuild) check(before.find(c => c.service === 'consumer-notifier').id !== after.find(c => c.service === 'consumer-notifier').id,
      'only notifier was recreated');
    check(Boolean(notifier), `same hazard ${event.hazard_id} replayed from broker after recovery`);
    broker();
  }
  async function thirdConsumer() {
    const before = snapshot();
    check(!before.some(c => c.service === 'consumer-dashboard-demo'), 'third consumer absent before addition (use a fresh demo project for rehearsal)');
    const stream = broker();
    check(!stream.consumers.some(c => c.name === 'dashboard-demo-v1'), 'third durable absent before addition');
    compose(['build', 'consumer-dashboard-demo'], { timeout: 600000 });
    compose(['--profile', 'consumer-demo', 'up', '-d', '--no-deps', 'dashboard-demo-db', 'consumer-dashboard-demo']);
    await until(() => { try { return internal('consumer-dashboard-demo', '/ready'); } catch { return null; } }, 'third consumer ready');
    const event = await newEvent(), received = {};
    for (const service of ['consumer-dashboard', 'consumer-notifier', 'consumer-dashboard-demo']) received[service] = await waitRecord(service, event.hazard_id);
    const after = snapshot();
    check(unchanged(before, after, ['consumer-dashboard-demo', 'dashboard-demo-db']), 'producer and all existing containers unchanged');
    const summary = broker();
    const expected = [config('consumer-dashboard/.env').NATS_DURABLE, config('consumer-notifier/.env').NATS_DURABLE, 'dashboard-demo-v1'];
    check(new Set(expected).size === 3 && expected.every(name => summary.consumers.some(c => c.name === name)), 'three independent durables on same stream');
    artifacts.push({ before, after, event, received });
    check(Object.keys(received).length === 3, `same new hazard ${event.hazard_id} reaches all three consumers`);
  }
  function query(sql) {
    return compose(['exec', '-T', 'postgres', 'sh', '-c',
      'psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB"'], { input: `BEGIN READ ONLY;\n${sql}\nCOMMIT;\n` });
  }
  async function execute(command, args = []) {
    say(`M1 DEMO ${command}; UTC ${new Date().toISOString()}; project=${project || process.env.COMPOSE_PROJECT_NAME || 'default'}`);
    if (command === 'request') {
      const expectedIndex = args.indexOf('--expect');
      const positional = args.filter((_, i) => expectedIndex < 0 || (i !== expectedIndex && i !== expectedIndex + 1));
      const expected = expectedIndex < 0 ? 200 : Number(args[expectedIndex + 1]);
      const result = await hazards(positional[1] || '/hazards', positional[0], expected);
      artifacts.push(result); say(JSON.stringify(result.data, null, 2));
    } else if (command === 'source') {
      const type = args[0];
      if (!['bmkg', 'tsunami', 'pvmbg'].includes(type)) throw new Error('Source must be bmkg, tsunami or pvmbg');
      const isPV = type === 'pvmbg';
      const headers = isPV ? { Authorization: `Bearer ${required(config('pvmbg-mock/.env'), 'PVMBG_TOKEN')}` }
        : { 'X-BMKG-Key': required(config('bmkg-mock/.env'), 'BMKG_API_KEY') };
      const result = await request(`${url(isPV ? 'PVMBG_HOST_PORT' : 'BMKG_HOST_PORT')}/${isPV ? 'volcanic-reports' : type === 'tsunami' ? 'tsunami-warnings' : 'seismic-events'}`, headers);
      check(result.http === 200, 'source HTTP 200'); artifacts.push(result); say(JSON.stringify(result.data, null, 2));
    } else if (command === 'db') {
      const sql = {
        summary: "SELECT source, count(*) AS records, count(*) FILTER (WHERE attributes ? 'confidence_level') AS with_confidence FROM hazard_events GROUP BY source ORDER BY source; SELECT count(*) AS unpublished FROM hazard_outbox;",
        latest: 'SELECT hazard_id,source,source_ref_id,severity,area_name,ingested_at,attributes FROM hazard_events ORDER BY ingested_at DESC LIMIT 12;',
        schema: "SELECT column_name,data_type FROM information_schema.columns WHERE table_name='hazard_events' ORDER BY ordinal_position;",
      }[args[0]];
      if (!sql) throw new Error('DB command must be summary, latest, schema or psql');
      const output = query(sql); say(output); artifacts.push({ sql, output });
    } else if (command === 'broker') broker();
    else if (command === 'records') {
      const service = { dashboard: 'consumer-dashboard', notifier: 'consumer-notifier', demo: 'consumer-dashboard-demo' }[args[0] || 'dashboard'];
      const rows = records(service); say(JSON.stringify(rows, null, 2)); artifacts.push({ service, records: rows });
    } else if (command === 'fanout') await fanout(args.includes('--rebuild'));
    else if (command === 'third-consumer') await thirdConsumer();
    else if (command === 'outage') {
      if (!['on', 'off'].includes(args[0])) throw new Error('Use outage on|off');
      const result = await request(`${url('PVMBG_HOST_PORT')}/admin/outage`,
        { Authorization: `Bearer ${required(config('pvmbg-mock/.env'), 'PVMBG_ADMIN_TOKEN')}` }, { enabled: args[0] === 'on' });
      check(result.http === 200 && result.data.outage === (args[0] === 'on'), `PVMBG outage ${args[0]}`); artifacts.push(result);
    } else if (command === 'delay') {
      if (!['3000', 'restore'].includes(args[0])) throw new Error('Use delay 3000|restore');
      const directory = join(root, 'evidence'), saved = join(directory, 'pvmbg-delay-original.json');
      const file = join(root, 'pvmbg-mock/.env'), current = config('pvmbg-mock/.env');
      const keys = ['PVMBG_MIN_DELAY_MS', 'PVMBG_MAX_DELAY_MS'];
      const baseline = Object.fromEntries(keys.map(key => [key, required(current, key)]));
      check(keys.every(key => /^\d+$/.test(baseline[key])), 'current PVMBG delay values valid');
      mkdirSync(directory, { recursive: true });
      if (args[0] === '3000' && !existsSync(saved)) writeFileSync(saved, JSON.stringify(baseline) + '\n', { flag: 'wx' });
      const values = args[0] === '3000' ? Object.fromEntries(keys.map(key => [key, '3000'])) : JSON.parse(readFileSync(saved, 'utf8'));
      check(keys.every(key => /^\d+$/.test(String(values[key]))) && Number(values[keys[0]]) <= Number(values[keys[1]]), 'target PVMBG delay valid');
      let text = readFileSync(file, 'utf8');
      for (const key of keys) text = text.replace(new RegExp(`^(\\s*(?:export\\s+)?${key}\\s*=)[^\\r\\n]*`, 'm'), (_, prefix) => `${prefix}${values[key]}`);
      writeFileSync(file, text);
      say(`PVMBG delay set to ${values[keys[0]]}..${values[keys[1]]} ms; recreating only pvmbg-mock (schema/outage reset).`);
      compose(['up', '-d', '--no-deps', '--force-recreate', 'pvmbg-mock']);
      if (args[0] === 'restore') unlinkSync(saved);
      artifacts.push({ original: baseline, applied: values });
    } else if (command === 'ownership') {
      const containers = snapshot(), postgres = containers.find(c => c.service === 'postgres');
      check(Boolean(postgres), 'canonical postgres container exists');
      const peers = containers.filter(c => c.networks.some(n => postgres.networks.includes(n))).map(c => c.service).sort();
      check(JSON.stringify(peers) === JSON.stringify(['aggregator', 'postgres']), `canonical store network peers: ${peers.join(', ')}`);
      const ports = JSON.parse(run('docker', ['inspect', '--format', '{{json .NetworkSettings.Ports}}', postgres.id]));
      check(Object.values(ports || {}).every(p => !p?.length), 'canonical Postgres has no published host ports');
      compose(['exec', '-T', 'aggregator', 'sh', '-c',
        'command -v nc >/dev/null && nc -z -w 2 postgres "$1"', 'sh', required(settings, 'POSTGRES_PORT')]);
      check(true, 'positive control: Aggregator can connect to canonical Postgres');
      for (const service of ['client-api', 'consumer-dashboard', 'consumer-notifier']) {
        const output = compose(['exec', '-T', service, 'sh', '-c',
          'command -v nc >/dev/null || exit 127; if nc -z -w 2 postgres "$1" >/dev/null 2>&1; then echo reachable; else echo isolated; fi', 'sh', required(settings, 'POSTGRES_PORT')]).trim();
        check(output === 'isolated', `${service} cannot connect directly to canonical Postgres`);
      }
      check((await hazards()).data.count > 0, 'public API still reads canonical data via Aggregator');
      artifacts.push({ containers, peers, ports });
    } else if (command === 'preflight') {
      for (const file of ['.env', 'bmkg-mock/.env', 'pvmbg-mock/.env', 'aggregator/.env', 'store/.env', 'registry/.env',
        'broker/.env', 'auth-service/.env', 'client-api/.env', 'client-tui/.env', 'consumer-dashboard/.env', 'consumer-notifier/.env',
        'monitoring/grafana/.env', 'monitoring/prometheus/.env', 'monitoring/loki/.env']) check(existsSync(join(root, file)), `${file} present`);
      compose(['config', '--quiet']); check(true, 'Compose configuration valid (values hidden)');
      const containers = snapshot(); artifacts.push({ containers });
      const expected = ['broker', 'postgres', 'registry-db', 'schema-registry', 'bmkg-mock', 'pvmbg-mock', 'aggregator', 'auth-service', 'client-api', 'consumer-dashboard', 'consumer-notifier'];
      for (const service of expected) check(containers.some(c => c.service === service && c.state === 'running' && c.health === 'healthy'), `${service} running and healthy`);
      const b = config('bmkg-mock/.env'), p = config('pvmbg-mock/.env'), a = config('aggregator/.env'), auth = config('auth-service/.env');
      for (const role of ['MEDIA', 'FIELD', 'INTERNAL']) for (const suffix of ['CLIENT_ID', 'CLIENT_SECRET']) required(auth, `${role}_${suffix}`);
      required(b, 'BMKG_API_KEY'); required(p, 'PVMBG_TOKEN');
      check(b.BMKG_API_KEY === a.BMKG_API_KEY && p.PVMBG_TOKEN === a.PVMBG_TOKEN, 'source credentials match Aggregator');
      check(new Set([auth.MEDIA_CLIENT_ID, auth.FIELD_CLIENT_ID, auth.INTERNAL_CLIENT_ID]).size === 3, 'three distinct client IDs');
      check(new Set([auth.MEDIA_CLIENT_SECRET, auth.FIELD_CLIENT_SECRET, auth.INTERNAL_CLIENT_SECRET]).size === 3, 'three distinct client secrets');
      say(`CONFIG access TTL=${auth.ACCESS_TOKEN_TTL_SEC}s; poll=${a.POLL_INTERVAL}s; PVMBG delay=${p.PVMBG_MIN_DELAY_MS}..${p.PVMBG_MAX_DELAY_MS}ms`);
      for (const [target, headers] of [[`${api}/ready`, {}], [`${url('BMKG_HOST_PORT')}/health`, {}], [`${url('PVMBG_HOST_PORT')}/health`, {}]]) {
        const result = await request(target, headers); check(result.http === 200, `${target} ready`);
      }
      for (const service of ['consumer-dashboard', 'consumer-notifier']) check(Boolean(internal(service, '/ready')), `${service} processing ready`);
      const r = await hazards();
      for (const source of ['BMKG', 'PVMBG']) check(r.data.data.some(h => h.source === source), `${source} canonical records present`);
      const raw = await request(`${url('PVMBG_HOST_PORT')}/volcanic-reports`, { Authorization: `Bearer ${p.PVMBG_TOKEN}` });
      check(raw.http === 200, 'PVMBG source available (outage off)');
      say(`BASELINE PVMBG confidence visible=${raw.data.some(r => 'confidence_level' in r)} (must be false before schema demo)`);
      broker();
      say('Preflight is readiness evidence; run mapping/schema/auth/fanout/load separately for acceptance.');
    } else throw new Error(`Unknown command: ${command}`);
    say('RESULT: PASS (within printed scope)');
  }
  return { execute, composeArgs, report: () => ({ lines, artifacts }) };
}

async function main() {
  const args = process.argv.slice(2);
  if (!args.length || args.includes('--help') || args.includes('-h')) { console.log(HELP); return; }
  let project;
  const pi = args.indexOf('--project');
  if (pi >= 0) {
    project = args[pi + 1];
    if (!project || project.startsWith('-')) throw new Error('--project requires a name');
    args.splice(pi, 2);
  }
  const [command, ...rest] = args;
  let interrupted = false;
  if (command === 'fanout') for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { interrupted = true; });
  const demo = createDemo({ project, cancelled: () => interrupted });
  if (command === 'db' && rest[0] === 'psql') {
    const result = spawnSync('docker', demo.composeArgs(['exec', '-e', 'PGOPTIONS=-c default_transaction_read_only=on', 'postgres',
      'sh', '-c', 'psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB"']), { cwd: ROOT, stdio: 'inherit' });
    process.exitCode = result.status ?? 1; return;
  }
  const started = new Date().toISOString();
  let success = false;
  let failure = null;
  try { await demo.execute(command, rest); success = true; }
  catch (error) {
    // execFile exceptions can include command arguments and credentials. Hide captured output.
    failure = error.status !== undefined || error.code ? 'Docker/process/connection error; check service status' : error.message;
    console.error(`RESULT: FAIL: ${failure}`);
    process.exitCode = 1;
  } finally {
    const directory = join(ROOT, 'evidence'); mkdirSync(directory, { recursive: true });
    const filename = join(directory, `m1-${command.replace(/[^a-z0-9-]/gi, '_')}-${started.replace(/[:.]/g, '-')}.json`);
    writeFileSync(filename, JSON.stringify({ command, args: rest, project: project || null, started, ended: new Date().toISOString(), success, failure, ...demo.report() }, null, 2) + '\n');
    console.log(`Evidence: ${filename}`);
  }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(`FAIL: ${error.message}`); process.exitCode = 1; });
}
