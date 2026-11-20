import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { Script, createContext } from 'node:vm';

const source = readFileSync(new URL('./evidence-probes.mjs', import.meta.url), 'utf8');
const activationSource = source.slice(source.indexOf('async function activateDiscoveredReader()'), source.indexOf('async function schema()'));
const storageSource = source.slice(source.indexOf('async function storage()'), source.indexOf('async function runtime()'));

function activationFixture(responses) {
  let calls = 0;
  const messages = [];
  const context = createContext({
    compose: () => {
      calls++;
      const response = responses.shift();
      if (response instanceof Error) throw response;
      assert.notEqual(response, undefined, 'unexpected extra toggle');
      return response;
    },
    say: message => messages.push(message),
    until: async (fn, label, seconds) => {
      assert.equal(seconds, 120);
      for (let i = 0; i < 3; i++) {
        const result = await fn();
        if (result) return result;
      }
      throw new Error(`Timed out: ${label}`);
    },
  });
  new Script(activationSource + '\nglobalThis.activate = activateDiscoveredReader;').runInContext(context);
  return { run: context.activate, calls: () => calls, messages };
}

const httpError = status => Object.assign(new Error('sensitive subprocess details'), {
  stderr: `  HTTP/1.1 ${status} Error\nwget: server returned error`,
});

test('reader activation waits for live discovery after history has already ingested v2', async () => {
  const probe = activationFixture([httpError(409), httpError(409), '{"reader_version":"v2"}']);
  assert.equal((await probe.run()).reader_version, 'v2');
  assert.equal(probe.calls(), 3);
  assert.equal(probe.messages.length, 2);
});

test('reader discovery timeout remains a failure', async () => {
  const probe = activationFixture([httpError(409), httpError(409), httpError(409)]);
  await assert.rejects(probe.run(), /Timed out: live polling/);
});

test('activation never retries other errors or an ambiguous successful response', async () => {
  for (const response of [httpError(401), httpError(503), new Error('sensitive transport details'), 'invalid JSON']) {
    const probe = activationFixture([response]);
    await assert.rejects(probe.run(), error => {
      assert.match(error.message, /Aggregator reader activation failed/);
      assert.ok(!error.message.includes('sensitive'));
      return true;
    });
    assert.equal(probe.calls(), 1);
  }
});

function storageFixture(attributes) {
  const old = { hazard_id: 'old', source_ref_id: 'old-source', attributes: {} };
  const fresh = { hazard_id: 'new', source_ref_id: 'new-source', attributes };
  const columns = [{ table_name: 'hazard_events', column_name: 'attributes', data_type: 'jsonb' }];
  const snapshots = {
    'schema-before.json': { canonical: { data: [old] }, columns },
    'schema-after.json': { source: { confidence_level: 0.88 }, canonical: { ...fresh, attributes: { confidence_level: 0.88 } }, columns },
  };
  const context = createContext({
    evidence: '', name: 'offline-storage', join: (...parts) => parts.filter(Boolean).join('/'),
    readFileSync: file => JSON.stringify(snapshots[file]),
    hazards: async () => ({ body: { data: [old, fresh] } }),
    catalog: () => columns, equivalent: (a, b) => JSON.stringify(a) === JSON.stringify(b),
    check: (condition, message) => { if (!condition) throw new Error(message); },
    save() {}, say() {},
  });
  new Script(storageSource + '\nglobalThis.runStorage = storage;').runInContext(context);
  return context.runStorage;
}

test('storage requires the current new record to retain the saved source confidence', async () => {
  await storageFixture({ confidence_level: 0.88 })();
  for (const attributes of [{}, { confidence_level: null }, { confidence_level: 0.95 }]) {
    await assert.rejects(storageFixture(attributes)(), /retains confidence_level/);
  }
});
