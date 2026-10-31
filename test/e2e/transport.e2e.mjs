// End-to-end test against a running Hibira (or Pusher-compatible) server
// with channel history on for the app. Skipped unless HIBIRA_E2E_KEY is set:
//
//   HIBIRA_E2E_HOST=127.0.0.1 HIBIRA_E2E_PORT=6001 HIBIRA_E2E_TLS=0 \
//   HIBIRA_E2E_APP_ID=... HIBIRA_E2E_KEY=... HIBIRA_E2E_SECRET=... npm run test:e2e
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Pusher from 'pusher-js';
import PusherServer from 'pusher';
import { streamText, simulateReadableStream, readUIMessageStream } from 'ai';
import * as aiTest from 'ai/test';
import { HibiraChatTransport } from '../../index.js';
import { publishUIMessageStream } from '../../server.js';

const env = process.env;
const skip = !env.HIBIRA_E2E_KEY && 'set HIBIRA_E2E_KEY to run';
const host = env.HIBIRA_E2E_HOST ?? '127.0.0.1';
const port = Number(env.HIBIRA_E2E_PORT ?? 8080);
const tls = env.HIBIRA_E2E_TLS === '1';

const server = skip
  ? null
  : new PusherServer({ appId: env.HIBIRA_E2E_APP_ID, key: env.HIBIRA_E2E_KEY, secret: env.HIBIRA_E2E_SECRET, host, port: String(port), useTLS: tls });

const client = () =>
  new Pusher(env.HIBIRA_E2E_KEY, {
    cluster: 'e2e',
    wsHost: host,
    wsPort: port,
    wssPort: port,
    forceTLS: tls,
    enabledTransports: ['ws'],
    channelAuthorization: { customHandler: ({ socketId, channelName }, cb) => cb(null, server.authorizeChannel(socketId, channelName)) },
  });

// The newest mock model this ai version has (v7: V4, v6: V3, v5: V2).
const MockModel = aiTest.MockLanguageModelV4 ?? aiTest.MockLanguageModelV3 ?? aiTest.MockLanguageModelV2;
const finish = aiTest.MockLanguageModelV2 && !aiTest.MockLanguageModelV3
  ? { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }
  : { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } } };
const words = Array.from({ length: 120 }, (_, i) => `token${i} `);

async function textOf(stream) {
  let last;
  for await (const m of readUIMessageStream({ stream })) last = m;
  return last?.parts.filter((p) => p.type === 'text').map((p) => p.text).join('');
}

test('useChat transport on a real server: stream, second device resume', { skip, timeout: 30000 }, async () => {
  const runs = [];
  const route = async (_, init) => {
    const { id, runId } = JSON.parse(init.body);
    const model = new MockModel({
      doStream: async () => ({
        stream: simulateReadableStream({
          chunkDelayInMs: 10,
          chunks: [{ type: 'text-start', id: 't' }, ...words.map((delta) => ({ type: 'text-delta', id: 't', delta })), { type: 'text-end', id: 't' }, finish],
        }),
      }),
    });
    const result = streamText({ model, prompt: 'hi' });
    runs.push(publishUIMessageStream(server, { chatId: id, runId, stream: result.toUIMessageStream() }));
    return new Response('{}', { status: 202 });
  };
  const a = client();
  const b = client();
  try {
    const chatId = `e2e${Date.now()}`;
    const first = await new HibiraChatTransport({ pusher: a, fetch: route }).sendMessages({ chatId, messages: [], trigger: 'submit-message', messageId: undefined, abortSignal: undefined });
    const firstText = textOf(first);
    await new Promise((r) => setTimeout(r, 400)); // the answer is part written
    const resumed = await new HibiraChatTransport({ pusher: b }).reconnectToStream({ chatId });
    assert.ok(resumed, 'the running answer can be resumed from channel history');
    const [ta, tb] = await Promise.all([firstText, textOf(resumed)]);
    assert.equal(ta, words.join(''));
    assert.equal(tb, words.join(''));
    const [res] = await Promise.all(runs);
    console.log(`  ${res.chunks} chunks sent as ${res.events} events`);
    assert.equal(await new HibiraChatTransport({ pusher: b }).reconnectToStream({ chatId }), null);
  } finally {
    a.disconnect();
    b.disconnect();
  }
});
