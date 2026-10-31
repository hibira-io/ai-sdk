import type { ChatTransport, UIMessage, UIMessageChunk, ChatRequestOptions } from 'ai';

export { UI_EVENT, chatChannel } from './protocol.js';

/** Seconds of slack added to the gap asked for after a reconnect. */
export declare const REWIND_SLACK: number;

/** The pusher-js client methods the transport uses. */
export interface PusherClient {
  subscribe(name: string): any;
  unsubscribe(name: string): void;
  channel?(name: string): any;
  send_event?(event: string, data: unknown, channel?: string): boolean | void;
}

export interface HibiraChatTransportOptions {
  /** A pusher-js client, or Laravel Echo's echo.connector.pusher. */
  pusher: PusherClient;
  /** The route that starts a run; default "/api/chat". */
  api?: string;
  /** The chat's channel; default chatChannel (private-ai-chat-{chatId}). */
  channel?: (chatId: string) => string;
  /** Event name; default "ai-ui". */
  event?: string;
  headers?: Record<string, string> | (() => Record<string, string>);
  /** Extra JSON sent with each request. */
  body?: object | (() => object);
  credentials?: RequestCredentials;
  fetch?: typeof fetch;
  /** How far back reconnectToStream looks, in seconds; default 600. */
  historySeconds?: number;
  /** Milliseconds to wait for the subscription and history; default 10000. */
  timeout?: number;
}

/**
 * A ChatTransport for useChat(): POSTs { id, messages, trigger, messageId,
 * runId, ...body } to `api` and reads the answer from the chat's channel.
 */
export declare class HibiraChatTransport<UI_MESSAGE extends UIMessage = UIMessage> implements ChatTransport<UI_MESSAGE> {
  constructor(options: HibiraChatTransportOptions);
  readonly resumeStreamIsReplay: true;
  sendMessages(options: {
    trigger: 'submit-message' | 'regenerate-message';
    chatId: string;
    messageId: string | undefined;
    messages: UI_MESSAGE[];
    abortSignal: AbortSignal | undefined;
  } & ChatRequestOptions): Promise<ReadableStream<UIMessageChunk>>;
  reconnectToStream(options: { chatId: string; abortSignal?: AbortSignal } & ChatRequestOptions): Promise<ReadableStream<UIMessageChunk> | null>;
  /** Unsubscribes the channels the transport subscribed. */
  close(): void;
}
