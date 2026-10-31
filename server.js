// Server side: send an AI SDK UI message stream to a channel, for Node,
// edge and serverless backends that use the `pusher` package (any object
// with the same trigger(channel, event, data) method works).
//
// The request that starts a run returns at once; the run keeps going after
// the browser disconnects (use after() / waitUntil() on serverless), and
// every tab or device subscribed to the chat's channel sees it.

import { UI_EVENT, bytes, chatChannel, randomId, splitText } from './protocol.js';

export { UI_EVENT, chatChannel };

const MERGEABLE = new Set(['text-delta', 'reasoning-delta']);

function iterate(source) {
  if (source && typeof source[Symbol.asyncIterator] === 'function') return source;
  if (source && typeof source.getReader === 'function') {
    return (async function* () {
      const reader = source.getReader();
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) return;
          yield value;
        }
      } finally {
        reader.releaseLock();
      }
    })();
  }
  throw new TypeError('publishUIMessageStream: stream must be a ReadableStream or an async iterable of UI message chunks');
}

/**
 * Publish a UI message stream (result.toUIMessageStream(), or
 * createUIMessageStream()) to a channel as HibiraChatTransport reads it.
 *
 * @param {object} pusher a `pusher` (Node) instance
 * @param {object} options
 * @param {ReadableStream|AsyncIterable} options.stream UI message chunks
 * @param {string} [options.channel] channel to send to (default chatChannel(chatId))
 * @param {string} [options.chatId] the chat's id, used for the default channel
 * @param {string} [options.runId] the run id the transport sent (body.runId); default a new one
 * @param {string} [options.event="ai-ui"] event name
 * @param {number} [options.interval=50] ms to gather chunks before sending them
 * @param {number} [options.maxBytes=3500] largest event payload, in bytes
 * @param {AbortSignal} [options.signal] stop reading; the run ends with error "aborted"
 * @returns {Promise<{runId: string, events: number, chunks: number, error: string | null}>}
 */
export async function publishUIMessageStream(pusher, options = {}) {
  const { stream, chatId, runId = randomId(), event = UI_EVENT, interval = 50, maxBytes = 3500, signal } = options;
  const channel = options.channel ?? (chatId != null ? chatChannel(chatId) : null);
  if (!channel) throw new TypeError('publishUIMessageStream: give channel or chatId');
  const envelope = bytes(JSON.stringify({ r: runId, s: 1e9, k: [] }));
  const budget = maxBytes - envelope;
  if (budget < 64) throw new RangeError('publishUIMessageStream: maxBytes is too small');

  let seq = 0;
  let events = 0;
  let chunks = 0;
  let buffer = [];
  let timer = null;
  let chain = Promise.resolve();
  let sendError = null;

  const send = (payload) => {
    events++;
    chain = chain.then(async () => {
      if (sendError) return;
      try {
        await pusher.trigger(channel, event, payload);
      } catch (err) {
        sendError = err;
      }
    });
  };

  // Pack buffered chunks into as few events as fit.
  const flush = () => {
    if (timer) clearTimeout(timer);
    timer = null;
    let batch = [];
    let size = 2;
    const out = () => {
      if (batch.length) send({ r: runId, s: seq++, k: batch });
      batch = [];
      size = 2;
    };
    for (const chunk of buffer) {
      for (const piece of fit(chunk, budget)) {
        if (typeof piece === 'string') {
          out();
          const slices = splitText(piece, budget - 24);
          slices.forEach((j, p) => send({ r: runId, s: seq++, j, p, n: slices.length }));
          continue;
        }
        const n = bytes(JSON.stringify(piece)) + 1;
        if (size + n > budget) out();
        batch.push(piece);
        size += n;
      }
    }
    out();
    buffer = [];
  };

  let error = null;
  try {
    for await (const chunk of iterate(stream)) {
      if (signal?.aborted) throw signal.reason ?? new Error('aborted');
      if (sendError) throw sendError;
      if (!chunk || typeof chunk !== 'object') continue;
      chunks++;
      const last = buffer[buffer.length - 1];
      if (last && MERGEABLE.has(chunk.type) && last.type === chunk.type && last.id === chunk.id && typeof chunk.delta === 'string') {
        buffer[buffer.length - 1] = { ...last, delta: last.delta + chunk.delta };
      } else {
        buffer.push(chunk);
      }
      if (chunk.type === 'finish' || chunk.type === 'error' || chunk.type === 'abort') flush();
      else if (!timer) timer = setTimeout(flush, interval);
    }
  } catch (err) {
    error = err;
  }
  flush();
  const done = { r: runId, s: seq++, d: 1 };
  if (error) done.e = String(error?.message ?? error).slice(0, 500);
  send(done);
  await chain;
  if (sendError) throw sendError;
  return { runId, events, chunks, error: done.e ?? null };
}

// The chunk as pieces that fit: itself, a delta split in several, or (for
// anything else too big) its JSON text, sent in slices.
function fit(chunk, budget) {
  if (bytes(JSON.stringify(chunk)) + 1 <= budget) return [chunk];
  if (MERGEABLE.has(chunk.type) && typeof chunk.delta === 'string') {
    const rest = bytes(JSON.stringify({ ...chunk, delta: '' })) + 1;
    if (budget - rest >= 16) return splitText(chunk.delta, budget - rest).map((delta) => ({ ...chunk, delta }));
  }
  return [JSON.stringify(chunk)];
}
