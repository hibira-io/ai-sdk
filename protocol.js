// The wire format shared by publishUIMessageStream() (./server.js) and
// HibiraChatTransport (./index.js): one AI SDK UI message stream (a "run")
// sent to a channel as a numbered run of events, all named "ai-ui":
//
//   { r, s: 0, k: [chunk, chunk, ...] }   UI message chunks, in order
//   { r, s: 1, j: "{\"type\":\"tool-ou", p: 0, n: 2 }   one chunk too big
//   { r, s: 2, j: "tput-available\"...}", p: 1, n: 2 }  for a message, in slices
//   { r, s: 3, d: 1 }                     the run ended
//   { r, s: 3, d: 1, e: "message" }       the run failed
//
// r is the run id (one per request), s the sequence number. Consecutive
// text and reasoning deltas are merged before sending, so a model writing 50
// tokens a second sends a handful of events a second, each under the plan's
// message size limit.

export const UI_EVENT = 'ai-ui';

/** The channel a chat's runs are sent to by default. */
export function chatChannel(chatId) {
  return `private-ai-chat-${chatId}`;
}

const encoder = new TextEncoder();
export const bytes = (s) => encoder.encode(s).length;

export function randomId() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID().replace(/-/g, '');
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 12)}`;
}

/**
 * Split text so each piece's JSON-escaped size fits `budget` bytes, without
 * cutting a character in two.
 */
export function splitText(text, budget) {
  const out = [];
  let piece = '';
  let size = 0;
  for (const ch of text) {
    const n = bytes(JSON.stringify(ch)) - 2;
    if (size + n > budget && piece) {
      out.push(piece);
      piece = '';
      size = 0;
    }
    piece += ch;
    size += n;
  }
  if (piece) out.push(piece);
  return out;
}
