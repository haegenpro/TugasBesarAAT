import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { createDemo, parseEnv, unchanged } from './m1-demo.mjs';

const scriptRoot = dirname(fileURLToPath(import.meta.url));
test('dotenv handles comments, quoted # and shell text as data; rejects duplicates', () => {
  assert.deepEqual(parseEnv(`\uFEFFexport PORT=123 # note\nKEY="a # b" # note\nTOKEN='$(touch /tmp/no)'\nURL=postgres://a:b@db/x\n`),
    { PORT: '123', KEY: 'a # b', TOKEN: '$(touch /tmp/no)', URL: 'postgres://a:b@db/x' });
  assert.throws(() => parseEnv('A=1\nA=2'), /Duplicate/);
});

test('environment generator creates matching random credentials and preserves existing files', t => {
  const root = mkdtempSync(join(tmpdir(), 'm1-setup-test-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const setup = () => execFileSync('python3', [join(scriptRoot, 'setup-env.py'), '--output-root', root], { encoding: 'utf8' });
  setup();
  const read = name => parseEnv(readFileSync(join(root, name, '.env'), 'utf8'));
  const agg = read('aggregator'), auth = read('auth-service'), store = read('store'), reg = read('registry');
  assert.equal(agg.BMKG_API_KEY, read('bmkg-mock').BMKG_API_KEY);
  assert.equal(agg.PVMBG_TOKEN, read('pvmbg-mock').PVMBG_TOKEN);
  assert.equal(auth.AUTH_INTERNAL_KEY, read('client-api').AUTH_INTERNAL_KEY);
  assert.ok(agg.DATABASE_URL.includes(store.APP_PASSWORD));
  assert.equal(reg.POSTGRES_PASSWORD, reg.APICURIO_DATASOURCE_PASSWORD);
  assert.notEqual(reg.POSTGRES_PASSWORD, store.POSTGRES_PASSWORD);
  assert.equal(new Set([auth.MEDIA_CLIENT_SECRET, auth.FIELD_CLIENT_SECRET, auth.INTERNAL_CLIENT_SECRET]).size, 3);
  assert.equal(auth.ACCESS_TOKEN_TTL_SEC, '60');
  assert.equal(read('consumer-notifier').NATS_DURABLE, 'notifier-v1');
  assert.equal(read('consumer-dashboard').NATS_DURABLE, 'dashboard-v1');
  assert.equal(readdirSync(root, { recursive: true }).filter(p => p === '.env' || p.endsWith('/.env')).length, 20);
  assert.match(setup(), /preserved/);
  assert.equal(read('auth-service').FIELD_CLIENT_SECRET, auth.FIELD_CLIENT_SECRET);
  rmSync(join(root, 'store', '.env'));
  assert.throws(setup, error => error.status === 1 && String(error.stderr).includes('no files written'));
  assert.equal(read('auth-service').FIELD_CLIENT_SECRET, auth.FIELD_CLIENT_SECRET);
});

function fixture(t, { failDashboard = false, failBuild = false, restartAggregator = false, interruptDuring = false } = {}) {
  const root = mkdtempSync(join(tmpdir(), 'm1-operator-test-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const put = (name, text) => { mkdirSync(dirname(join(root, name)), { recursive: true }); writeFileSync(join(root, name), text); };
  put('.env', 'CLIENT_API_HOST_PORT=18080\nBMKG_HOST_PORT=18081\nPVMBG_HOST_PORT=18082\n');
  put('auth-service/.env', 'FIELD_CLIENT_ID=field\nFIELD_CLIENT_SECRET=hidden-secret\n');
  put('broker/.env', 'NATS_STREAM=HAZARDS\n');
  put('consumer-dashboard/.env', 'NATS_DURABLE=dashboard-v1\n');
  put('consumer-notifier/.env', 'NATS_DURABLE=notifier-v1\n');
  let stopped = false, recreated = false, started = false, third = false, hazardReads = 0;
  const calls = [], messages = [];
  const canonical = id => ({ hazard_id: id, source: 'BMKG', source_ref_id: `ref-${id}` });
  function container(service) {
    return { Id: service === 'consumer-notifier' && recreated ? 'new-notifier' : service,
      Config: { Labels: { 'com.docker.compose.service': service } }, RestartCount: 0,
      State: { StartedAt: service === 'consumer-notifier' && started ? 'later' : service === 'aggregator' && stopped && restartAggregator ? 'unexpected-restart' : 'original',
        Status: service === 'consumer-notifier' && stopped ? 'exited' : 'running', Health: { Status: 'healthy' } },
      NetworkSettings: { Networks: { backend: {} } } };
  }
  const run = (exe, argv) => {
    assert.equal(exe, 'docker'); calls.push(argv);
    if (argv[0] === 'inspect') return JSON.stringify(['aggregator', 'consumer-dashboard', 'consumer-notifier', ...(third ? ['consumer-dashboard-demo'] : [])].map(container));
    assert.equal(argv[0], 'compose');
    const action = argv.slice(argv.indexOf(join(root, 'docker-compose.yml')) + 1);
    assert.equal(action[0], '-p'); assert.equal(action[1], 'test-project'); action.splice(0, 2);
    if (action[0] === 'ps') return 'aggregator\nconsumer-dashboard\nconsumer-notifier';
    if (action[0] === 'stop') { stopped = true; return ''; }
    if (action[0] === 'start') { stopped = false; started = true; return ''; }
    if (action[0] === 'build') { if (failBuild) throw Object.assign(new Error('hidden Docker output'), { status: 1 }); return ''; }
    if (action[0] === 'up') { recreated = true; stopped = false; started = true; return ''; }
    if (action[0] === '--profile') { third = true; return ''; }
    if (action[0] === 'exec') {
      const service = action[2], path = action.at(-1);
      if (service === 'broker') return JSON.stringify({ account_details: [{ stream_detail: [{ name: 'HAZARDS', config: { subjects: ['hazard.events'] }, state: { messages: 5 },
        consumer_detail: ['dashboard-v1', 'notifier-v1', ...(third ? ['dashboard-demo-v1'] : [])].map(name => ({ name, num_pending: stopped && name === 'notifier-v1' ? 1 : 0 })) }] }] });
      if (path === '/ready') return '{"status":"ready"}';
      if (path === '/records') {
        if (failDashboard && stopped && service === 'consumer-dashboard') throw new Error('dashboard processing failed');
        return JSON.stringify({ data: [{ hazard_id: 'new-event', correlation_id: 'cid' }] });
      }
    }
    throw new Error(`Unexpected fake Docker operation: ${JSON.stringify(action)}`);
  };
  const fetcher = async (url, options) => {
    let data;
    if (url.endsWith('/auth/login')) data = { access_token: 'never-print-this-token', role: 'field', expires_in: 60 };
    else {
      assert.equal(options.headers.Authorization, 'Bearer never-print-this-token');
      data = { count: 1, data: [canonical(++hazardReads === 1 ? 'old-event' : 'new-event')] };
    }
    return { status: 200, text: async () => JSON.stringify(data) };
  };
  const demo = createDemo({ root, project: 'test-project', run, fetcher, log: line => messages.push(line), sleep: async () => {}, cancelled: () => interruptDuring && stopped });
  return { root, demo, calls, messages, isRestored: () => started && !stopped };
}

test('fanout follows an event created during downtime and verifies replay after recovery', async t => {
  const f = fixture(t);
  await f.demo.execute('fanout');
  assert.equal(f.isRestored(), true);
  const report = JSON.stringify(f.demo.report());
  assert.ok(report.includes('new-event'));
  assert.ok(!report.includes('never-print-this-token') && !report.includes('hidden-secret'));
  assert.ok(f.messages.some(l => l.includes('broker backlog')));
});

test('fanout restores notifier after processing failure, build failure and unrelated restart', async t => {
  for (const options of [{ failDashboard: true }, { failBuild: true }, { restartAggregator: true }, { interruptDuring: true }]) {
    const f = fixture(t, options);
    await assert.rejects(f.demo.execute('fanout', ['--rebuild']));
    assert.equal(f.isRestored(), true);
  }
});

test('rebuild recreates only notifier and preserves all other containers', async t => {
  const f = fixture(t);
  await f.demo.execute('fanout', ['--rebuild']);
  assert.ok(f.calls.some(a => a.includes('--no-deps') && a.includes('--force-recreate')));
  assert.ok(f.messages.some(l => l.includes('only notifier was recreated')));
});

test('third consumer requires fresh durable and proves a new event reaches all three', async t => {
  const f = fixture(t);
  await f.demo.execute('third-consumer');
  assert.ok(f.messages.some(l => l.includes('reaches all three')));
  await assert.rejects(f.demo.execute('third-consumer'), /third consumer absent/);
});

test('request rejects URL injection before forwarding a token', async t => {
  const f = fixture(t);
  await assert.rejects(f.demo.execute('request', ['field', '//elsewhere/hazards']), /Path must start/);
  await assert.rejects(f.demo.execute('request', ['field', '/hazards\\@elsewhere']), /Path must start/);
});

test('container comparison rejects restarts and missing services', () => {
  const a = [{ service: 'aggregator', id: 'a', started: 'one', restarts: 0, state: 'running' }];
  assert.equal(unchanged(a, [{ ...a[0], restarts: 1 }]), false);
  assert.equal(unchanged(a, []), false);
  assert.equal(unchanged(a, [{ ...a[0], id: 'b' }], ['aggregator']), true);
});

test('delay controls restore original values without copying secrets into evidence', async t => {
  const f = fixture(t);
  mkdirSync(join(f.root, 'pvmbg-mock'));
  const file = join(f.root, 'pvmbg-mock/.env');
  const original = 'PVMBG_TOKEN=private-source-token\nPVMBG_MIN_DELAY_MS="500" # min\nPVMBG_MAX_DELAY_MS=2500\n';
  writeFileSync(file, original);
  await f.demo.execute('delay', ['3000']);
  assert.equal(parseEnv(readFileSync(file, 'utf8')).PVMBG_MIN_DELAY_MS, '3000');
  const saved = readFileSync(join(f.root, 'evidence/pvmbg-delay-original.json'), 'utf8');
  assert.ok(!saved.includes('private-source-token'));
  await f.demo.execute('delay', ['3000']); // repeated fixed setting must preserve the original baseline
  await f.demo.execute('delay', ['restore']);
  const restored = parseEnv(readFileSync(file, 'utf8'));
  assert.equal(restored.PVMBG_MIN_DELAY_MS, '500');
  assert.equal(restored.PVMBG_MAX_DELAY_MS, '2500');
  assert.equal(restored.PVMBG_TOKEN, 'private-source-token');
  assert.throws(() => readFileSync(join(f.root, 'evidence/pvmbg-delay-original.json')), /ENOENT/);
  assert.ok(f.calls.filter(a => a.includes('up')).every(a => a.at(-1) === 'pvmbg-mock' && a.includes('--no-deps')));
});
