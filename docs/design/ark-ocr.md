# Ark OCR reads visible text one page at a time

Records: [0065](../decisions/0065-readers-choose-their-provider.md) (provider selection and
recovery), [0040](../decisions/0040-a-chunk-says-where-its-words-came-from.md) (origin and
evidence). This guide covers the Ark OCR implementation in the second provider change; merge
the provider settings change first, then the OCR change.

## Configure a workspace

In **Administration → Models**, choose **Ark** in the OCR card and enter the Base URL, API key
and a vision-capable model. OCR has its own credentials and model; saving chat, embedding or
transcription settings does not change the OCR configuration.

For Agent Plan, use `https://ark.cn-beijing.volces.com/api/plan/v3` with the plan's dedicated
API key and a vision model available to that key. For the ordinary Ark API, use its matching
Base URL, key and model or endpoint ID. The reader appends `/chat/completions` to the Base URL;
enter the API base, rather than the complete operation URL. Check the provider's current
[Chat API](https://docs.volcengine.com/docs/ark/chat-api?lang=zh&redirect=1),
[model list](https://docs.volcengine.com/docs/ark/model-list?lang=zh) and
[Agent Plan connection guide](https://docs.volcengine.com/docs/ark/agent-plan-enterprise-opencode?lang=zh)
for the route and model supported by your account.

Keys use the existing sealed settings store and are not returned to the browser. A blank key
keeps the saved key when the provider is unchanged. Changing provider clears that key unless
you enter a replacement. If you change the endpoint while keeping the same provider, enter
the key for the new endpoint: a blank input still retains the stored key. Each provider keeps
its own unsaved form draft; switching back restores the edited address and options. Defaults
apply only to a fresh draft, and the saved-key indicator belongs to the saved provider and
endpoint. Saving successfully or changing workspace clears the relevant drafts and key input.

Save before using **Test**. The test sends a valid, blank 32 × 32 PNG and verifies that the
endpoint returns the complete text JSON protocol. This is a model request and can consume
quota. It establishes connectivity and protocol support; assess recognition quality with real
files as described below. Saving an available reader requeues documents waiting for missing
OCR configuration. Other failed documents can be reprocessed through the existing document
workflow.

## Input and resource limits

These are this reader's limits, rather than a statement of every Ark model's limits.

| Item | Limit or behavior |
|---|---|
| Image formats | Static PNG, JPEG and WebP |
| Original image size | At most 8 MiB |
| Image dimensions | Positive width and height, each at most 12,000 pixels; 196–25,000,000 pixels in total |
| PDF input | Nonempty, at most 32 MiB, with 1–100 pages |
| PDF rendering | One requested page at a time; `pdftoppm -png -singlefile -scale-to 3000`, with a 3,000-pixel maximum edge |
| Rendered PNG | Must also satisfy the image byte and dimension limits |
| HTTP response | At most 1 MiB, including after HTTP decompression |
| Text per page | At most 256 KiB of UTF-8 text |
| Text per document | At most 4 MiB for the sum of page texts |
| Model request | 180 seconds, including the response body |
| Poppler invocation | 120 seconds; bounded stdout, with termination on overflow, timeout or cancellation |

The image path sends the original bytes with `image/png`, `image/jpeg` or `image/webp` in a
Base64 `image_url`. It reads bounded container headers for dimensions and animation markers;
it does not decode, resize or re-encode the original image. Header preflight rejects unsupported,
animated, truncated or inconsistent headers and inputs outside the limits. It is not complete
format validation: compressed image data and checksums are left to the receiving service.

PDFs use `pdfinfo` to count pages and the existing Poppler tools to render each page. The
application does not decode those PNGs locally. The Docker runtime already includes Poppler
and its CJK data; a local deployment needs `pdfinfo` and `pdftoppm` on `PATH`. No `image`,
`tokio-tungstenite` or FFmpeg dependency is added for this reader.

## Reading and evidence

The model is asked to transcribe only visible written text, in reading order, and to return
exactly `{"text":"..."}`. Instructions printed in the image are source text. Object descriptions,
chart interpretation and guesses at illegible characters are outside this OCR contract. The
adapter requires one completed choice, `finish_reason: "stop"`, no refusal and the exact text
schema; truncated, refused or malformed results are not accepted as completed pages.

The resulting `Reading` uses `origin = ocr`, records the configured Ark model and anchors text
to the real, one-based page number. A blank page keeps its place in the sequence, so text on
page 3 remains on page 3 when page 2 is blank. The adapter supplies no bounding boxes or
invented coordinates. A file with no readable text fails as unreadable.

Model-generated text can contain omissions, mistaken digits or invented wording even when
its JSON is valid. Before adopting a model, compare representative scans and screenshots
against their originals, including the languages, names, amounts, tables and stamped pages
used in your workflow. A successful connection test or mock test does not establish this
quality. Treat OCR as a transcription to check against the original page, rather than an
authoritative statement of facts.

## Checkpoints and retries

Ark runs inside the existing `process_document` job. A completed page is persisted before
the job yields, and the next run requests the first missing page. The checkpoint identity is
derived from the file hash and the effective OCR provider, Base URL, model and key; unrelated
settings and the global settings timestamp do not invalidate it.

| Event | Checkpoint and queue behavior |
|---|---|
| Another page remains | Persist the completed page and return `Deferred`; waiting does not consume ordinary retry attempts |
| HTTP 408, 429 or 5xx, a connection failure or timeout | Use the existing queue's ordinary backoff and retry budget; keep completed pages |
| Invalid input, authentication failure or output protocol failure | Mark the failure terminal; keep completed pages for a later reprocess |
| Retry budget exhausted or later indexing/embedding fails | Keep completed pages even after the document fails |
| A failed document is manually requeued | Resume missing pages when the file and effective OCR identity still match |
| File, effective OCR configuration or document existence changes | Reject stale page, chunk, failure and ready writes; an obsolete checkpoint cannot be reused |
| Processing reaches `ready` | Clear the checkpoint; a subsequent manual reprocess starts a fresh reading |

`Deferred` retains the queue's existing one-hour continuous waiting window. This change does
not extend it or create another worker retry policy. After that window, further `Deferred`
results follow ordinary backoff and consume the existing retry budget. If the document
eventually fails, its persisted pages remain available when it is requeued.

Within the existing single-process deployment, an Ark-specific per-document try-lock covers
reading, post-processing and the final status write. A duplicate attempt yields through the
queue; cancellation or a process exit releases ownership. Checkpoint writes use a short
compare-and-set transaction against the file hash, effective OCR configuration and current
task JSON. Network requests do not hold a database lock connection. These protections apply
to Ark; MinerU, OpenAI transcription and ordinary parsing keep their existing behavior.

A request may succeed remotely and consume quota before its page is persisted locally. If the
process stops in that interval, the in-flight page may be requested again. Persisted completed
pages are reused, but the provider supplies no remote idempotency guarantee here, so the reader
does not promise exactly-once requests or charges.

## Scope and development baseline

Ark audio transcription is deferred. The documented Agent Plan ASR routes are WebSocket
routes, including the route named `bigmodel_nostream`. The ordinary HTTP file-recognition
service has different routes and credential/resource requirements; official evidence has not
established that it accepts the plan's dedicated key or uses its subscription entitlement.
See the [Agent Plan voice documentation](https://www.volcengine.com/docs/82379/2516286?lang=zh)
and [HTTP file-recognition documentation](https://www.volcengine.com/docs/6561/1354868?lang=zh).
There is no Ark transcription choice in these changes.

The OCR change does not update `Cargo.lock` or the toolchain policy. The existing lockfile
already selects `calamine 0.36.1` and `jsonwebtoken 10.4.0`, whose published manifests require
Rust 1.88. The README's older Rust 1.85 statement predates this change; that baseline mismatch
is separate from adding OCR.
