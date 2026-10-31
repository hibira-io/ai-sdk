import Pusher from 'pusher-js';
import PusherServer from 'pusher';
import { streamText, type ChatTransport, type UIMessage } from 'ai';
import { HibiraChatTransport, chatChannel } from '../index.js';
import { publishUIMessageStream } from '../server.js';

declare const model: Parameters<typeof streamText>[0]['model'];

const pusher = new Pusher('key', { cluster: 'hkg' });
const transport: ChatTransport<UIMessage> = new HibiraChatTransport({ pusher, api: '/api/chat', body: () => ({ model: 'fast' }) });
const name: string = chatChannel('abc');

const server = new PusherServer({ appId: '1', key: 'k', secret: 's', host: 'ws.hibira.io' });
const result = streamText({ model, prompt: 'hi' });
const done: Promise<{ runId: string; events: number }> = publishUIMessageStream(server, { chatId: 'abc', runId: 'r', stream: result.toUIMessageStream() });
void [transport, name, done];
