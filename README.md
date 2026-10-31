# @hibira-io/ai-sdk

A [Vercel AI SDK](https://ai-sdk.dev) transport over [Hibira](https://hibira.io) channels. Hibira is a Pusher-compatible realtime service. With this transport, `useChat()` answers:

- **survive dropped connections.** A tab that goes offline catches up from channel history when it comes back.
- **resume after a reload.** `useChat({ resume: true })` picks up an answer that is still being written. You don't need Redis or a stream store.
- **reach every tab and device** subscribed to the chat, not only the one that asked.
- **keep going after the browser leaves.** The API route answers at once, and generation runs to the end in the background.

It works with AI SDK 5, 6 and 7 and with any `useChat` (React, Vue, Svelte, Angular). Any Pusher-compatible server works. Resuming needs channel history, which Hibira offers on Hobby plans and up (turn it on in the app's settings).

```sh
npm install @hibira-io/ai-sdk pusher-js pusher
```

## Client

```tsx
'use client';
import { useChat } from '@ai-sdk/react';
import Pusher from 'pusher-js';
import { HibiraChatTransport } from '@hibira-io/ai-sdk';

const pusher = new Pusher(process.env.NEXT_PUBLIC_HIBIRA_KEY!, {
  cluster: 'hkg',
  wsHost: 'ws.hibira.io',
  forceTLS: true,
  channelAuthorization: { endpoint: '/api/hibira/auth', transport: 'ajax' },
});
const transport = new HibiraChatTransport({ pusher, api: '/api/chat' });

export function Chat({ id, initialMessages }) {
  const { messages, sendMessage, status } = useChat({ id, messages: initialMessages, transport, resume: true });
  // ...
}
```

The transport subscribes to `private-ai-chat-{chatId}` (change it with `channel: (chatId) => ...`). It POSTs `{ id, messages, trigger, messageId, runId, ...body }` to `api` and streams the answer from the channel. Laravel Echo users can pass `echo.connector.pusher`.

## Server

```ts
// app/api/chat/route.ts
import { after } from 'next/server';
import { streamText, convertToModelMessages } from 'ai';
import Pusher from 'pusher';
import { publishUIMessageStream } from '@hibira-io/ai-sdk/server';

const pusher = new Pusher({
  appId: process.env.HIBIRA_APP_ID!,
  key: process.env.NEXT_PUBLIC_HIBIRA_KEY!,
  secret: process.env.HIBIRA_SECRET!,
  host: 'ws.hibira.io',
  useTLS: true,
});

export async function POST(req: Request) {
  const { id, messages, runId } = await req.json();
  const result = streamText({ model: 'anthropic/claude-sonnet-5-5', messages: await convertToModelMessages(messages) });
  after(() =>
    publishUIMessageStream(pusher, {
      chatId: id,
      runId,
      stream: result.toUIMessageStream({ originalMessages: messages, onFinish: ({ messages }) => saveChat(id, messages) }),
    }),
  );
  return Response.json({ runId }, { status: 202 });
}
```

Use `after()` in Next.js, `waitUntil()` on other serverless platforms, or a plain un-awaited promise on a long-running server. Save the finished messages in `onFinish`: a reloaded page loads them from your storage, and only an answer still in progress comes from the channel.

**Authorize the channel.** `private-ai-chat-{chatId}` is a private channel. Your auth endpoint should allow it only for the chat's owner. See the [Hibira guides](https://hibira.io/guides).

## How it works

Each request is a run with its own id. `publishUIMessageStream` merges consecutive text and reasoning deltas, collecting them for 50 ms by default. It sends them as numbered `ai-ui` events, each under `maxBytes` (3500 by default; Free plans allow 4 KB per message), and splits a chunk that is too big into slices. A last event marks the end, or carries the error if the model failed.

The transport puts the events back in order and hands useChat the original UI message chunks. When the connection drops and comes back, it asks channel history (`pusher:rewind`) for what it missed. `reconnectToStream` replays the latest unfinished run from its start (`resumeStreamIsReplay`). Without history, a lost middle ends the answer with an error chunk instead of a broken message.

Stopping in the browser (`stop()`) only stops reading. The run on the server keeps going, as it would with any resumable stream.

## Options

| `HibiraChatTransport` | default | |
|---|---|---|
| `pusher` | | pusher-js client |
| `api` | `/api/chat` | route that starts a run |
| `channel` | `chatChannel` | `(chatId) => channel name` |
| `headers`, `body` | | object or function, sent with each request |
| `credentials`, `fetch` | | passed to fetch |
| `historySeconds` | `600` | how far back `reconnectToStream` looks |
| `timeout` | `10000` | ms to wait for the subscription and history |

| `publishUIMessageStream` | default | |
|---|---|---|
| `stream` | | `result.toUIMessageStream()` or `createUIMessageStream()` |
| `chatId` / `channel` | | where to send |
| `runId` | new id | pass the request's `runId` |
| `interval` | `50` | ms to gather chunks |
| `maxBytes` | `3500` | largest event payload |
| `signal` | | stop reading |

## Tests

`npm test` runs the transport against an in-memory server. `npm run test:e2e` runs it against a real Hibira or Pusher-compatible server with channel history on; see the header of `test/e2e/transport.e2e.mjs`.

## License

MIT
