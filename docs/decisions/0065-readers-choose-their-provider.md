# 0065 · Readers choose their provider

- **Status**: Proposed · 2026-09-29 · migration 0101 · two cuts, to be reviewed and merged in order; Ark transcription is deferred
- **Written**: 2026-09-28
- **Discussion**: [#1006](https://github.com/deeplethe/utopia/issues/1006), [review of #1007](https://github.com/deeplethe/utopia/pull/1007#pullrequestreview-5347893667)
- **Related**: [0040](0040-a-chunk-says-where-its-words-came-from.md), [Ark OCR design and usage](../design/ark-ocr.md)

## Problem

A model name cannot safely select a wire protocol. MinerU reads scans through task submission
and polling; OpenAI-compatible transcription accepts a recording through HTTP. Ark vision
uses chat with images. Each reader still returns the same `Reading`, with the evidence
contract of 0040, so chunking and extraction do not need another path.

Reader identity must also be narrower than the global settings timestamp. A chat or embedding
save must not invalidate completed OCR pages, and a retry must not re-read those pages merely
because the document has failed. Recovery should fit the existing queue and its single-process
deployment rather than introduce another shared reader lifecycle.

## Decisions

1. OCR and transcription carry their own provider. Migration 0101 defaults existing rows to
   `mineru` and `openai`, preserving their configured addresses, models and credentials. OCR
   also gains a separate nullable model. No transcription resource ID is stored before an
   HTTP protocol and its subscription entitlement are established.
2. Credentials remain sealed by the existing settings store. An empty key preserves the old
   key only when the provider is unchanged; a provider change clears it unless a new key is
   supplied. The provider comparison and write happen in one SQL statement. An omitted
   provider preserves the current database value, rather than writing a value read earlier.
3. An omitted OCR model preserves it when the provider is unchanged; an explicit empty model
   clears it. Changing protocol without a model clears the previous model. Legacy store
   calls keep their signatures and use these omission rules. The existing transcription
   model replacement behavior remains unchanged.
4. A reserved database value does not enable a protocol. The first cut accepts only MinerU
   OCR and OpenAI transcription through the API, and readers reject other provider values.
   Its settings page offers no Ark choice. The second cut opens Ark OCR with its implementation.
5. Ark OCR sends the original static PNG, JPEG or WebP bytes with their MIME type. Bounded
   header reads determine dimensions and reject animation or inconsistent headers without
   local image decoding, resizing or re-encoding. This is preflight, not complete validation
   of compressed data or checksums. PDFs use the existing Poppler tools, rendering the
   requested page with `pdftoppm -scale-to 3000`. The reader applies its own documented byte,
   pixel, page, response, text and time limits.
6. The model returns only visible written text in the exact text JSON schema. A partial or
   refused result is not a completed page. The `Reading` records OCR origin and the configured
   model, with real page numbers; blank pages do not renumber later text. No bounding boxes
   or image descriptions are manufactured. Recognition quality requires comparison with
   actual source pages: valid JSON and connectivity tests do not make generated text
   authoritative evidence.
7. Ownership and checkpoint protection belong to the Ark path. An Ark-specific per-document
   process try-lock covers reading, post-processing and failure or ready writes; duplicate
   attempts yield, and cancellation or a process exit releases ownership. Its weak registry
   does not retain every document. Existing MinerU, OpenAI transcription, ordinary parsing,
   failure formatting and worker recovery retain their behavior.
8. A completed page is persisted under an identity derived from the file hash and effective
   OCR provider, Base URL, model and key. Server and store use the same identity rule; chat,
   embedding, transcription and global settings timestamps are excluded. Short transactional
   compare-and-set writes check the file, effective configuration and current task JSON.
   Network calls do not retain a database lock connection. Changed input, configuration or
   deletion fences late page, chunk, failure and ready writes.
9. More pages use the existing queue's `Deferred` path and its one-hour continuous waiting
   window, after which waiting falls back to ordinary retries. HTTP 408, 429, 5xx, connection
   failures and timeouts use ordinary retries and their existing attempt budget. Input,
   authentication and output protocol failures are terminal.
   Completed pages survive all of these failures, exhausted budgets and later indexing or
   embedding failure. Manually requeuing a failed document resumes missing pages if its
   identity still matches. Success at `ready` clears the checkpoint, so a later manual
   reprocess starts a fresh reading rather than using a permanent OCR cache.
10. A provider-specific unsaved form draft restores the actual address, model, backend and
    entered key when the user switches back. Defaults apply only to a fresh draft. Saved-key
    state is associated with both provider and endpoint; changing workspace or successfully
    saving the relevant draft clears transient keys and drafts.

## Cuts

1. Provider columns, settings routes, atomic credential changes and compatibility tests.
   No new protocol, visible provider choice, dependency or worker behavior.
2. Ark OCR for images and PDFs, its provider choice and drafts, and Ark-specific checkpoint
   recovery. This change depends on the first cut and is reviewed separately after it.

Ark transcription remains deferred. The
[Agent Plan voice documentation](https://www.volcengine.com/docs/82379/2516286?lang=zh)
lists WebSocket ASR routes; `bigmodel_nostream` still uses WebSocket. The
[ordinary HTTP file-recognition API](https://www.volcengine.com/docs/6561/1354868?lang=zh)
uses different routes, resources and credential requirements. As of this review, official
documentation does not establish that the plan's dedicated key or subscription entitlement
applies to that HTTP service. A future transcription proposal needs that evidence before
choosing a protocol and exposing its settings.

## Consequences

Persisted pages are reused across failed attempts, but a page can succeed remotely before its
checkpoint commits. A crash in that interval can cause the in-flight page to be requested and
charged again. Without remote idempotency the reader cannot promise exactly-once requests or
charges. The process lock follows the current single-process deployment; it is not a distributed
lease for independently running server processes.

No `image`, `tokio-tungstenite` or FFmpeg dependency, Docker package, toolchain change or
lockfile update is introduced. Poppler is already in the runtime image. The baseline lockfile
already contains `calamine 0.36.1` and `jsonwebtoken 10.4.0`, which require Rust 1.88; the older
Rust 1.85 statement in the README is a pre-existing documentation mismatch.

## Revision · 2026-09-29

The original proposal included WebSocket transcription, local image decoding and shared
reader leases tied to `llm_settings.updated_at`. Review narrowed it to the two cuts above.
WebSocket framing and its dependencies are removed; ordinary HTTP ASR is deferred until its
subscription support is established. Original images pass through header preflight, and
existing Poppler handles PDF rendering. Global settings timestamps cannot identify a reader
configuration, and time leases or a connection held during a network request introduce a second
recovery policy or consume the database pool. Ark-specific process ownership and short CAS
writes fit the existing queue while keeping completed pages across failures. These cuts are
independently runnable and must merge in order.
