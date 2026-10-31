// Client side: a ChatTransport for the AI SDK's useChat() (React, Vue,
// Svelte, Angular) that sends messages to your API route and reads the
// answer from a Hibira channel instead of the HTTP response. The answer keeps
// streaming to every tab and device on the chat, a reconnecting browser
// catches up from channel history, and a reloaded page resumes an answer
// that is still being written (useChat({ resume: true })).

import { UI_EVENT, chatChannel, randomId } from './protocol.js';

export { UI_EVENT, chatChannel };

/** Seconds of slack added to the gap asked for after a reconnect. */
export const REWIND_SLACK = 2;

export class HibiraChatTransport {
  /**
   * @param {object} options
   * @param {object} options.pusher a pusher-js client (or Laravel Echo's echo.connector.pusher)
   * @param {string} [options.api="/api/chat"] the route that starts a run
   * @param {(chatId: string) => string} [options.channel] the chat's channel (default chatChannel)
   * @param {string} [options.event="ai-ui"]
   * @param {object|(() => object)} [options.headers]
   * @param {object|(() => object)} [options.body] extra JSON sent with each request
   * @param {RequestCredentials} [options.credentials]
   * @param {typeof fetch} [options.fetch]
   * @param {number} [options.historySeconds=600] how far back reconnectToStream looks
   * @param {number} [options.timeout=10000] ms to wait for the subscription and history
   */
  constructor(options = {}) {
    if (!options.pusher?.subscribe) throw new TypeError('HibiraChatTransport: pusher must be a pusher-js client');
    this.pusher = options.pusher;
    this.api = options.api ?? '/api/chat';
    this.channelName = options.channel ?? chatChannel;
    this.event = options.event ?? UI_EVENT;
    this.headers = options.headers;
    this.body = options.body;
    this.credentials = options.credentials;
    this.fetch = options.fetch;
    this.historySeconds = options.historySeconds ?? 600;
    this.timeout = options.timeout ?? 10000;
    this.resumeStreamIsReplay = true;
    this.uses = new Map(); // channel name -> { count, owned }
  }

  async sendMessages({ chatId, messages, abortSignal, trigger, messageId, headers, body, metadata }) {
    const runId = randomId();
    const name = this.channelName(chatId);
    const channel = await this.#acquire(name);
    const run = this.#follow(channel, runId, abortSignal, () => this.#release(name));
    try {
      const doFetch = this.fetch ?? globalThis.fetch;
      const res = await doFetch(this.api, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...resolve(this.headers), ...plain(headers) },
        body: JSON.stringify({ ...resolve(this.body), ...body, id: chatId, messages, trigger, messageId, runId, ...(metadata !== undefined ? { metadata } : {}) }),
        credentials: this.credentials,
        signal: abortSignal,
      });
      if (!res.ok) throw new Error((await res.text().catch(() => '')) || `Chat request failed: HTTP ${res.status}`);
    } catch (err) {
      run.cancel();
      throw err;
    }
    return run.stream;
  }

  async reconnectToStream({ chatId, abortSignal }) {
    const name = this.channelName(chatId);
    const channel = await this.#acquire(name);
    const runs = new Map(); // run id -> payloads, in arrival order
    const collect = (p) => {
      if (!p || typeof p !== 'object' || p.r == null) return;
      const id = String(p.r);
      if (!runs.has(id)) runs.set(id, []);
      runs.get(id).push(p);
    };
    channel.bind(this.event, collect);
    const result = await this.#rewind(channel, this.historySeconds);
    channel.unbind(this.event, collect);
    let latest = null;
    for (const [id, list] of runs) latest = [id, list];
    // Nothing to resume: no history, no recent run, the run already ended,
    // or its start is older than the history window.
    if (result.error || !latest || latest[1].some((p) => p.d) || !latest[1].some((p) => p.s === 0) || abortSignal?.aborted) {
      this.#release(name);
      return null;
    }
    const run = this.#follow(channel, latest[0], abortSignal, () => this.#release(name));
    for (const p of latest[1]) run.apply(p);
    return run.stream;
  }

  close() {
    for (const [name, use] of this.uses) if (use.owned) this.pusher.unsubscribe(name);
    this.uses.clear();
  }

  // Subscribe (or share an existing subscription) and wait until it is up.
  async #acquire(name) {
    let use = this.uses.get(name);
    if (!use) {
      const existing = this.pusher.channel?.(name);
      use = { count: 0, owned: !existing };
      this.uses.set(name, use);
    }
    use.count++;
    const channel = this.pusher.channel?.(name) ?? this.pusher.subscribe(name);
    if (!channel.subscribed) {
      try {
        await new Promise((ok, fail) => {
          const timer = setTimeout(() => done(new Error(`Timed out subscribing to ${name}`)), this.timeout);
          const onOk = () => done();
          const onErr = (e) => done(new Error(`Could not subscribe to ${name}: ${e?.error ?? e?.status ?? 'refused'}`));
          function done(err) {
            clearTimeout(timer);
            channel.unbind('pusher:subscription_succeeded', onOk);
            channel.unbind('pusher:subscription_error', onErr);
            err ? fail(err) : ok();
          }
          channel.bind('pusher:subscription_succeeded', onOk);
          channel.bind('pusher:subscription_error', onErr);
        });
      } catch (err) {
        this.#release(name);
        throw err;
      }
    }
    return channel;
  }

  #release(name) {
    const use = this.uses.get(name);
    if (!use || --use.count > 0) return;
    this.uses.delete(name);
    if (use.owned) this.pusher.unsubscribe(name);
  }

  // Ask channel history for the last `seconds`; the events come in through
  // the channel's normal handlers before pusher:rewind_succeeded.
  #rewind(channel, seconds) {
    return new Promise((done) => {
      if (!this.pusher.send_event) return done({ error: 'no send_event' });
      const finish = (result) => {
        clearTimeout(timer);
        channel.unbind('pusher:rewind_succeeded', onOk);
        channel.unbind('pusher:rewind_error', onErr);
        done(result);
      };
      const onOk = (data) => finish({ count: data?.count ?? 0 });
      const onErr = (data) => finish({ error: data?.error ?? 'Channel history is unavailable' });
      const timer = setTimeout(() => finish({ error: 'Channel history did not answer in time' }), this.timeout);
      channel.bind('pusher:rewind_succeeded', onOk);
      channel.bind('pusher:rewind_error', onErr);
      this.pusher.send_event('pusher:rewind', { channel: channel.name, seconds }, channel.name);
    });
  }

  // One run's chunks, put back in order, as a ReadableStream.
  #follow(channel, runId, abortSignal, cleanup) {
    let controller;
    let next = 0;
    const early = new Map();
    let slices = '';
    let last = Date.now();
    let finished = false;
    let gapTimer = null;

    const end = (error) => {
      if (finished) return;
      finished = true;
      clearTimeout(gapTimer);
      channel.unbind(this.event, onEvent);
      channel.unbind('pusher:subscription_succeeded', onResubscribe);
      abortSignal?.removeEventListener('abort', onAbort);
      try {
        if (error) controller.enqueue({ type: 'error', errorText: error });
        controller.close();
      } catch {}
      cleanup();
    };
    const apply = (p) => {
      if (finished || !p || typeof p !== 'object' || String(p.r) !== runId) return;
      const s = Number(p.s);
      if (!Number.isInteger(s) || s < next) return;
      last = Date.now();
      early.set(s, p);
      while (early.has(next)) {
        const e = early.get(next);
        early.delete(next);
        next++;
        if (Array.isArray(e.k)) for (const chunk of e.k) controller.enqueue(chunk);
        if (typeof e.j === 'string') {
          slices += e.j;
          if (e.p === e.n - 1) {
            try {
              controller.enqueue(JSON.parse(slices));
            } catch {}
            slices = '';
          }
        }
        if (e.d) return end(e.e ? String(e.e) : null);
      }
      // The end arrived but chunks before it are missing: wait for them a
      // little (a rewind may still bring them), then give up.
      if (!gapTimer && [...early.values()].some((e) => e.d)) {
        gapTimer = setTimeout(() => end('Part of the answer was lost (turn on channel history to recover it)'), 3000);
      }
    };
    const onEvent = (p) => apply(p);
    const onResubscribe = () => {
      if (this.pusher.send_event) {
        const seconds = Math.ceil((Date.now() - last) / 1000) + REWIND_SLACK;
        this.pusher.send_event('pusher:rewind', { channel: channel.name, seconds }, channel.name);
      }
    };
    const onAbort = () => end(null);

    const stream = new ReadableStream({
      start(c) {
        controller = c;
      },
      cancel() {
        end(null);
      },
    });
    channel.bind(this.event, onEvent);
    channel.bind('pusher:subscription_succeeded', onResubscribe);
    abortSignal?.addEventListener('abort', onAbort);
    return { stream, apply, cancel: () => end(null) };
  }
}

function resolve(v) {
  return typeof v === 'function' ? v() : v ?? {};
}

function plain(h) {
  if (!h) return {};
  if (typeof Headers !== 'undefined' && h instanceof Headers) return Object.fromEntries(h.entries());
  return h;
}
