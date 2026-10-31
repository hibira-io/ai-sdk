# Changelog

## 0.1.0

- `HibiraChatTransport`: a `ChatTransport` for `useChat()` (AI SDK 5, 6 and 7) that reads answers from a Hibira channel. Answers stream to every tab and device on the chat, a dropped connection catches up from channel history, and `useChat({ resume: true })` picks up an answer that is still being written after a reload.
- `publishUIMessageStream()` (`@hibira-io/ai-sdk/server`): sends a UI message stream to the chat's channel. It merges text and reasoning deltas, so 126 chunks went out as 26 events in the end-to-end test, and it keeps every event under the plan's message size limit by splitting large chunks.
