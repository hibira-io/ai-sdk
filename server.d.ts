export { UI_EVENT, chatChannel } from './protocol.js';

/** Anything with the `pusher` package's trigger(). */
export interface Trigger {
  trigger(channel: string, event: string, data: unknown): Promise<unknown> | unknown;
}

export interface PublishUIMessageStreamOptions {
  /** UI message chunks: result.toUIMessageStream() or createUIMessageStream(). */
  stream: ReadableStream<object> | AsyncIterable<object>;
  /** Channel to send to; default chatChannel(chatId). */
  channel?: string;
  /** The chat's id (the request body's `id`). */
  chatId?: string;
  /** The run id the transport sent (the request body's `runId`). */
  runId?: string;
  /** Event name; default "ai-ui". */
  event?: string;
  /** Milliseconds to gather chunks before sending them; default 50. */
  interval?: number;
  /** Largest event payload in bytes; default 3500 (Free plans allow 4 KB). */
  maxBytes?: number;
  /** Stop reading; the run ends with error "aborted". */
  signal?: AbortSignal;
}

export interface PublishUIMessageStreamResult {
  runId: string;
  events: number;
  chunks: number;
  /** Why the stream failed, or null. */
  error: string | null;
}

/** Publish a UI message stream to a channel as HibiraChatTransport reads it. */
export declare function publishUIMessageStream(pusher: Trigger, options: PublishUIMessageStreamOptions): Promise<PublishUIMessageStreamResult>;
