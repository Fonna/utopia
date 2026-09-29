# 0065 · Readers choose their provider

- **Status**: In progress · 2026-09-29 · migration 0101 · open: Ark OCR; non-streaming Ark transcription if the subscription supports it
- **Written**: 2026-09-28
- **Discussion**: [#1006](https://github.com/deeplethe/utopia/issues/1006), [review of #1007](https://github.com/deeplethe/utopia/pull/1007#pullrequestreview-5347893667)
- **Related**: [0040](0040-a-chunk-says-where-its-words-came-from.md)

## Problem

A model name cannot safely select a wire protocol. MinerU reads scans through task submission
and polling; OpenAI-compatible transcription accepts a recording through HTTP. Ark vision
uses chat with images. Each reader still returns the same `Reading`, with the evidence
contract of 0040, so chunking and extraction do not need another path.

## Decisions

1. OCR and transcription carry their own provider. Migration 0101 defaults existing rows to
   `mineru` and `openai`, preserving their configured addresses, models and credentials. OCR
   also gains a separate nullable model. The transcription resource ID is not stored before
   its HTTP protocol and subscription support are established.
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
   Its settings page offers no Ark choice. Ark OCR is opened only with its implementation.
5. Ownership and checkpoint protection for Ark belong to the Ark path. Existing MinerU,
   OpenAI, ordinary parsing, failure formatting and worker recovery are not changed. A
   change to chat or embedding settings must not invalidate an OCR operation.
6. Ark OCR sends original supported image bytes with their MIME type; bounded header reads
   determine dimensions without server-side image decoding or re-encoding. PDF pages are
   rendered with existing Poppler and retain their real page numbers. Completed pages stay
   checkpointed across failed reads, so retries resume the missing page. Rate limits and
   timeouts use the existing queue's retry rules.

## Cuts

1. Provider columns, settings routes, atomic credential changes and compatibility tests.
   No new protocol, visible provider choice, dependency or worker behavior.
2. Ark OCR for images and PDFs, its provider choice and per-provider form drafts. Returning
   to a provider restores its actual address and options; defaults apply only to a fresh
   draft. A saved key is associated with both its provider and its endpoint.
3. Ark transcription through ordinary HTTP submission and polling, only if the user's
   subscription supports that interface. Deferred until that is established.

## Revision · 2026-09-29

The original proposal included WebSocket transcription, local image decoding and shared
reader leases tied to `llm_settings.updated_at`. Review narrowed it to the cuts above:
the WebSocket framing and its dependencies are removed, and global settings timestamps
cannot identify a reader configuration. Ark-specific safeguards must not change existing
reader behavior. These cuts are independently runnable and reviewed in order.
