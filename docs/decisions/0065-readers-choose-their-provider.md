# 0065 · OCR readers choose their provider

- **Status**: In progress · 2026-09-30 · PR #1007 · migration 0101
- **Written**: 2026-09-28
- **Discussion**: [#1006](https://github.com/deeplethe/utopia/issues/1006), [reviews of #1007](https://github.com/deeplethe/utopia/pull/1007)
- **Related**: [0040](0040-a-chunk-says-where-its-words-came-from.md), [Ark OCR design and usage](../design/ark-ocr.md)

## Problem

A model name cannot safely select a wire protocol. MinerU reads scans through task submission
and polling; Ark vision uses chat with images. Both OCR readers return the same `Reading`,
with the evidence contract of 0040, so chunking and extraction do not need another path.

Reader identity must also be narrower than the global settings timestamp. A chat or embedding
save must not invalidate completed OCR pages, and a retry must not re-read those pages merely
because the document has failed. Recovery should fit the existing queue and its single-process
deployment rather than introduce another shared reader lifecycle.

## Decisions

1. OCR carries its own provider and nullable model. Migration 0101 defaults existing rows to
   `mineru`, preserving their configured OCR address, backend and credential. The settings API
   accepts MinerU and Ark OCR; the reader selects the corresponding protocol. An unsupported
   provider fails closed instead of being treated as MinerU.
2. Credentials remain sealed by the existing settings store. An empty key preserves the old
   key only when the provider is unchanged; a provider change clears it unless a new key is
   supplied. The provider comparison and write happen in one SQL statement. An omitted
   provider preserves the current database value, rather than writing a value read earlier.
3. An omitted OCR model preserves it when the provider is unchanged; an explicit empty model
   clears it. Changing provider without a model clears the previous model.
4. Ark OCR sends the original static PNG, JPEG or WebP bytes with their MIME type. Bounded
   header reads determine dimensions and reject animation or inconsistent headers without
   local image decoding, resizing or re-encoding. This is preflight, not complete validation
   of compressed data or checksums. PDFs use the existing Poppler tools, rendering the
   requested page with `pdftoppm -scale-to 3000`. The reader applies its own documented byte,
   pixel, page, response, text and time limits.
5. The model returns only visible written text in the exact text JSON schema. A partial or
   refused result is not a completed page. The `Reading` records OCR origin and the configured
   model, with real page numbers; blank pages do not renumber later text. No bounding boxes
   or image descriptions are manufactured. Recognition quality requires comparison with
   actual source pages: valid JSON and connectivity tests do not make generated text
   authoritative evidence.
6. Ownership and checkpoint protection belong to the Ark path. An Ark-specific per-document
   process try-lock covers reading, post-processing and failure or ready writes; duplicate
   attempts yield, and cancellation or a process exit releases ownership. Its weak registry
   does not retain every document. Existing MinerU, OpenAI transcription, ordinary parsing,
   failure formatting and worker recovery retain their behavior.
7. A completed page is persisted under an identity derived from the file hash and effective
   OCR provider, Base URL, model and key. Server and store use the same identity rule; chat,
   embedding, transcription and global settings timestamps are excluded. Short transactional
   compare-and-set writes check the file, effective configuration and current task JSON.
   Network calls do not retain a database lock connection. Changed input, configuration or
   deletion fences late page, chunk, failure and ready writes.
8. More pages use the existing queue's `Deferred` path and its one-hour continuous waiting
   window, after which waiting falls back to ordinary retries. HTTP 408, 429, 5xx, connection
   failures and timeouts use ordinary retries and their existing attempt budget. Input,
   authentication and output protocol failures are terminal.
   Completed pages survive all of these failures, exhausted budgets and later indexing or
   embedding failure. Manually requeuing a failed document resumes missing pages if its
   identity still matches. Success at `ready` clears the checkpoint, so a later manual
   reprocess starts a fresh reading rather than using a permanent OCR cache.
9. A provider-specific unsaved form draft restores the actual address, model, backend and
    entered key when the user switches back. Defaults apply only to a fresh draft. Saved-key
    state is associated with both provider and endpoint; changing workspace clears transient
    keys and drafts. A successful save clears all previous drafts and the current key input
    only if no edits occurred after the request was submitted.

## Consequences

Persisted pages are reused across failed attempts, but a page can succeed remotely before its
checkpoint commits. A crash in that interval can cause the in-flight page to be requested and
charged again. Without remote idempotency the reader cannot promise exactly-once requests or
charges. The process lock follows the current single-process deployment; it is not a distributed
lease for independently running server processes.

No image-decoding dependency or new Docker package is introduced. Poppler is already in the
runtime image.

## Revision · 2026-09-30

Review of #1007 narrowed this decision to the OCR configuration and reader implemented in
the same change. Original images pass through header preflight, and existing Poppler handles
PDF rendering. The global settings timestamp cannot identify an OCR configuration. Ark-specific
process ownership and short compare-and-set writes fit the existing queue while keeping
completed pages across failures.
