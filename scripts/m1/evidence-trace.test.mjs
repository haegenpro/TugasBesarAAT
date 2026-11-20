import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { Script, createContext } from 'node:vm';

const source = readFileSync(new URL('./evidence-probes.mjs', import.meta.url), 'utf8');
const traceSource = source.slice(source.indexOf('async function trace()'), source.indexOf('async function outage()'));

function fixture(rows) {
  const messages = [];
  const saved = new Map();
  const context = createContext({
    name: 'offline-trace',
    compose: argv => {
      assert.equal(argv[0], 'logs');
      return 'non-JSON infrastructure line\n' + rows.map(r => `service | ${JSON.stringify(r)}`).join('\n');
    },
    check: (condition, message) => { if (!condition) throw new Error(message); },
    say: line => messages.push(line),
    save: (file, value) => saved.set(file, value),
  });
  new Script(traceSource + '\nglobalThis.runTrace = trace;').runInContext(context);
  return { run: context.runTrace, messages, saved };
}

test('trace evidence includes Aggregator message logs and consumer msg logs', async () => {
  const rows = [];
  for (const [index, service] of ['bmkg-mock', 'pvmbg-mock', 'consumer-dashboard', 'consumer-notifier'].entries()) {
    const cid = `flow-${index}`;
    rows.push({ service: 'aggregator', correlation_id: cid, message: 'outbound call', target_service: service, latency_ms: index });
    rows.push({ service, correlation_id: cid, msg: 'outbound call', target_service: 'nats', latency_ms: 12 });
    rows.push({ service: 'aggregator', correlation_id: cid, message: 'http request', latency_ms: 999 });
    rows.push({ service: 'aggregator', correlation_id: cid, message: 'outbound call', latency_ms: 'invalid' });
  }
  const trace = fixture(rows);
  await trace.run();
  for (const [index, service] of ['bmkg-mock', 'pvmbg-mock', 'consumer-dashboard', 'consumer-notifier'].entries()) {
    assert.ok(trace.messages.includes(`  aggregator -> ${service} latency_ms=${index}`));
    assert.ok(trace.messages.includes(`  ${service} -> nats latency_ms=12`));
  }
  assert.ok(!trace.messages.some(line => /latency_ms=(999|invalid)/.test(line)));
  assert.equal(trace.saved.get('offline-trace.json').length, rows.length);
});

test('trace rejects a correlated flow with no valid outbound duration', async () => {
  const trace = fixture([
    { service: 'aggregator', correlation_id: 'missing-duration', message: 'outbound call' },
    { service: 'bmkg-mock', correlation_id: 'missing-duration', msg: 'http request', latency_ms: 10 },
  ]);
  await assert.rejects(trace.run(), /outbound latency recorded/);
  assert.equal(trace.saved.size, 0);
});
