import { test } from 'node:test';
import assert from 'node:assert/strict';
import { streamText, simulateReadableStream, readUIMessageStream, createUIMessageStream } from 'ai';
import * as aiTest from 'ai/test';
import { HibiraChatTransport, chatChannel } from '../index.js';
import { publishUIMessageStream } from '../server.js';
import { createBus } from './bus.mjs';

// The newest mock model this ai version has (v7: V4, v6: V3, v5: V2).
const MockModel = aiTest.MockLanguageModelV4 ?? aiTest.MockLanguageModelV3 ?? aiTest.MockLanguageModelV2;
const finish = aiTest.MockLanguageModelV2 && !aiTest.MockLanguageModelV3
  ? { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }
  : { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } } };

function model(words, delay = 3) {
  return new MockModel({
    doStream: async () => ({
      stream: simulateReadableStream({
        chunkDelayInMs: delay,
        chunks: [{ type: 'text-start', id: 't1' }, ...words.map((w) => ({ type: 'text-delta', id: 't1', delta: w })), { type: 'text-end', id: 't1' }, finish],
      }),
    }),
  });
}

// A chat API route: starts the run and answers at once, publishing in the
// background (as with after() / waitUntil()).
function route(bus, words, { delay, runs = [] } = {}) {
  return async (url, init) => {
    const body = JSON.parse(init.body);
    const result = streamText({ model: model(words, delay), prompt: 'hi' });
    runs.push(publishUIMessageStream(bus.server, { chatId: body.id, runId: body.runId, stream: result.toUIMessageStream() }));
    return new Response(JSON.stringify({ runId: body.runId }), { status: 202 });
  };
}

async function textOf(stream) {
  let last;
  for await (const m of readUIMessageStream({ stream })) last = m;
  return last?.parts.filter((p) => p.type === 'text').map((p) => p.text).join('');
}

const words = Array.from({ length: 40 }, (_, i) => `word${i} `);

test('sendMessages streams the answer through the channel, with few events', async () => {
  const bus = createBus();
  const runs = [];
  const pusher = bus.client();
  const t = new HibiraChatTransport({ pusher, fetch: route(bus, words, { runs }) });
  const stream = await t.sendMessages({ chatId: 'c1', messages: [], trigger: 'submit-message', messageId: undefined, abortSignal: undefined });
  assert.equal(await textOf(stream), words.join(''));
  const [res] = await Promise.all(runs);
  assert.ok(res.events < res.chunks / 2, `events ${res.events} chunks ${res.chunks}`);
  assert.equal(pusher.channels.size, 0, 'unsubscribed after the run');
  assert.equal(bus.log[0][0], chatChannel('c1'));
});

test('a second tab on the chat resumes the running answer from history', async () => {
  const bus = createBus();
  const a = new HibiraChatTransport({ pusher: bus.client(), fetch: route(bus, words, { delay: 5 }) });
  const first = await a.sendMessages({ chatId: 'c2', messages: [], trigger: 'submit-message', abortSignal: undefined });
  const firstText = textOf(first);
  await new Promise((r) => setTimeout(r, 80));
  const b = new HibiraChatTransport({ pusher: bus.client() });
  const resumed = await b.reconnectToStream({ chatId: 'c2' });
  assert.ok(resumed, 'resumable');
  assert.equal(await textOf(resumed), words.join(''));
  assert.equal(await firstText, words.join(''));
  assert.equal(await b.reconnectToStream({ chatId: 'c2' }), null, 'finished run is not resumed');
});

test('a dropped connection catches up with a rewind', async () => {
  const bus = createBus();
  const pusher = bus.client();
  const t = new HibiraChatTransport({ pusher, fetch: route(bus, words, { delay: 5 }) });
  const stream = await t.sendMessages({ chatId: 'c3', messages: [], trigger: 'submit-message', abortSignal: undefined });
  const text = textOf(stream);
  setTimeout(() => pusher.drop(), 40);
  setTimeout(() => pusher.restore(), 120);
  assert.equal(await text, words.join(''));
});

test('without history, a lost middle ends the stream with an error', async () => {
  const bus = createBus({ history: false });
  const pusher = bus.client();
  const t = new HibiraChatTransport({ pusher, fetch: route(bus, words, { delay: 5 }) });
  const stream = await t.sendMessages({ chatId: 'c4', messages: [], trigger: 'submit-message', abortSignal: undefined });
  setTimeout(() => pusher.drop(), 40);
  setTimeout(() => pusher.restore(), 120);
  const types = [];
  for await (const c of stream) types.push(c.type === 'error' ? `error:${c.errorText}` : c.type);
  assert.match(types.at(-1), /^error:Part of the answer was lost/);
  assert.equal(await new HibiraChatTransport({ pusher: bus.client() }).reconnectToStream({ chatId: 'c4' }), null);
});

test('big chunks are split to fit, and failures reach the client', async () => {
  const bus = createBus();
  const big = 'x'.repeat(9000);
  const stream = createUIMessageStream({
    execute: ({ writer }) => {
      writer.write({ type: 'start' });
      writer.write({ type: 'text-start', id: 'a' });
      writer.write({ type: 'text-delta', id: 'a', delta: big });
      writer.write({ type: 'text-end', id: 'a' });
      writer.write({ type: 'data-report', data: { rows: Array.from({ length: 300 }, (_, i) => ({ i, name: `row ${i}` })) } });
      writer.write({ type: 'finish' });
    },
  });
  const pusher = bus.client();
  const t = new HibiraChatTransport({
    pusher,
    fetch: async (_, init) => {
      const { id, runId } = JSON.parse(init.body);
      publishUIMessageStream(bus.server, { chatId: id, runId, stream });
      return new Response('{}');
    },
  });
  const out = await t.sendMessages({ chatId: 'c5', messages: [], trigger: 'submit-message', abortSignal: undefined });
  let last;
  for await (const m of readUIMessageStream({ stream: out })) last = m;
  assert.equal(last.parts.find((p) => p.type === 'text').text, big);
  assert.equal(last.parts.find((p) => p.type === 'data-report').data.rows.length, 300);

  async function* failing() {
    yield { type: 'start' };
    throw new Error('model overloaded');
  }
  const res = await publishUIMessageStream(bus.server, { chatId: 'c6', stream: failing() });
  assert.equal(res.error, 'model overloaded');
  assert.deepEqual(bus.log.at(-1)[2], { r: res.runId, s: 1, d: 1, e: 'model overloaded' });
});

test('a failed request throws and releases the channel; abort closes the stream', async () => {
  const bus = createBus();
  const pusher = bus.client();
  const t = new HibiraChatTransport({ pusher, fetch: async () => new Response('nope', { status: 401 }) });
  await assert.rejects(t.sendMessages({ chatId: 'c7', messages: [], trigger: 'submit-message' }), /nope/);
  assert.equal(pusher.channels.size, 0);

  const ac = new AbortController();
  const t2 = new HibiraChatTransport({ pusher, fetch: route(bus, words, { delay: 10 }) });
  const stream = await t2.sendMessages({ chatId: 'c8', messages: [], trigger: 'submit-message', abortSignal: ac.signal });
  setTimeout(() => ac.abort(), 30);
  const types = [];
  for await (const c of stream) types.push(c.type);
  assert.ok(!types.includes('finish'));
  assert.equal(pusher.channels.size, 0);
});

test('sends the request body the route expects', async () => {
  const bus = createBus();
  let seen;
  const t = new HibiraChatTransport({
    pusher: bus.client(),
    api: '/chat',
    headers: () => ({ Authorization: 'Bearer x' }),
    body: { model: 'fast' },
    fetch: async (url, init) => {
      seen = { url, headers: init.headers, body: JSON.parse(init.body) };
      const res = publishUIMessageStream(bus.server, { chatId: seen.body.id, runId: seen.body.runId, stream: createUIMessageStream({ execute: ({ writer }) => writer.write({ type: 'finish' }) }) });
      await res;
      return new Response('{}');
    },
  });
  const s = await t.sendMessages({ chatId: 'c9', messages: [{ id: 'm1', role: 'user', parts: [] }], trigger: 'submit-message', messageId: undefined, body: { extra: 1 } });
  for await (const _ of s);
  assert.equal(seen.url, '/chat');
  assert.equal(seen.headers.Authorization, 'Bearer x');
  assert.equal(seen.body.model, 'fast');
  assert.equal(seen.body.extra, 1);
  assert.equal(seen.body.id, 'c9');
  assert.equal(seen.body.trigger, 'submit-message');
  assert.match(seen.body.runId, /^[0-9a-z]{16,}$/);
});
