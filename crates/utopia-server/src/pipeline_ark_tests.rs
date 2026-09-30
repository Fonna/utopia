//! 方舟读取的检查点必须经真实摄入和数据库提交验证，不能只测序列化。
use super::{fixture, FakeEmbed, FakeTranscriber, Fx, MP3};
use crate::readers::ark_checkpoint::Checkpoint;
use crate::readers::ark_ocr::PROBE_PNG;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use utopia_store::documents::{self, ArkOcrSnapshot};
use utopia_store::settings;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

#[derive(Clone)]
struct OcrReplies {
    statuses: Vec<u16>,
    images: Arc<Mutex<Vec<String>>>,
    delay: Duration,
}

impl OcrReplies {
    fn new(statuses: Vec<u16>) -> Self {
        Self {
            statuses,
            images: Arc::new(Mutex::new(Vec::new())),
            delay: Duration::ZERO,
        }
    }
}

impl Respond for OcrReplies {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = request.body_json().expect("OCR JSON body");
        let ordinal = {
            let mut images = self.images.lock().unwrap();
            images.push(
                body["messages"][1]["content"][1]["image_url"]["url"]
                    .as_str()
                    .unwrap()
                    .into(),
            );
            images.len()
        };
        let status = self.statuses.get(ordinal - 1).copied().unwrap_or(200);
        let response = ResponseTemplate::new(status).set_delay(self.delay);
        if status != 200 {
            return response.set_body_string("remote failure");
        }
        response.set_body_json(json!({"choices": [{"finish_reason":"stop", "message": {
            "content": json!({"text": format!("recognized request {ordinal}")}).to_string()
        }}]}))
    }
}

async fn configure(f: &Fx, replies: &OcrReplies) -> anyhow::Result<MockServer> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(replies.clone())
        .mount(&server)
        .await;
    settings::upsert_ocr(
        &f.pool,
        f.ws,
        Some(&server.uri()),
        Some("test-ocr-key"),
        None,
        Some("ark"),
        Some("test-vision"),
    )
    .await?;
    Ok(server)
}

async fn task(f: &Fx, doc: Uuid) -> anyhow::Result<Value> {
    documents::reader_task(&f.pool, doc)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint was cleared"))
}

fn make_pdf(objects: &[String]) -> Vec<u8> {
    let mut pdf = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.extend_from_slice(format!("{} 0 obj\n{object}\nendobj\n", index + 1).as_bytes());
    }
    let xref = pdf.len();
    pdf.extend_from_slice(
        format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).as_bytes(),
    );
    for offset in offsets {
        pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    pdf.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
            objects.len() + 1
        )
        .as_bytes(),
    );
    pdf
}

fn pdf_stream(content: &str) -> String {
    format!(
        "<< /Length {} >>\nstream\n{content}\nendstream",
        content.len()
    )
}

fn two_page_scan() -> Vec<u8> {
    make_pdf(&[
        "<< /Type /Catalog /Pages 2 0 R >>".into(),
        "<< /Type /Pages /Count 2 /Kids [3 0 R 5 0 R] >>".into(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] /Resources << >> /Contents 4 0 R >>"
            .into(),
        pdf_stream("1 g 0 0 200 200 re f"),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] /Resources << >> /Contents 6 0 R >>"
            .into(),
        pdf_stream("0 g 0 0 200 200 re f"),
    ])
}

fn has_poppler() -> bool {
    let available = ["pdfinfo", "pdftoppm"].into_iter().all(|tool| {
        std::process::Command::new(tool)
            .arg("-v")
            .output()
            .is_ok_and(|output| output.status.success())
    });
    assert!(
        available || std::env::var_os("UTOPIA_TEST_REQUIRE_PDF").is_none(),
        "PDF tests were required but Poppler is unavailable"
    );
    available
}

#[tokio::test]
async fn page_failure_and_exhausted_queue_budget_keep_completed_pages_for_manual_retry(
) -> anyhow::Result<()> {
    if !has_poppler() {
        return Ok(());
    }
    for status in [429, 503, 401] {
        let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
            return Ok(());
        };
        let replies = OcrReplies::new(vec![200, status, 200]);
        let _server = configure(&f, &replies).await?;
        let doc = f.document_with_bytes("scan.pdf", &two_page_scan()).await?;
        let first = crate::pipeline::process_document(&f.state, doc)
            .await
            .unwrap_err();
        assert!(utopia_core::is_deferred(&first).is_some());
        let completed = task(&f, doc).await?;
        assert_eq!(completed["pages"], json!(["recognized request 1"]));
        let error = crate::pipeline::process_document(&f.state, doc)
            .await
            .unwrap_err();
        assert_eq!(utopia_core::is_terminal(&error), status == 401);
        assert!(
            utopia_core::is_deferred(&error).is_none(),
            "HTTP failures use the existing retry budget"
        );
        assert_eq!(task(&f, doc).await?["pages"], completed["pages"]);
        let job_id = utopia_store::jobs::enqueue_with_max_attempts(
            &f.pool,
            "process_document",
            json!({"document_id":doc}),
            1,
        )
        .await?;
        let job = utopia_store::jobs::Job {
            id: job_id,
            kind: "process_document".into(),
            payload: json!({"document_id":doc}),
            attempts: 1,
            max_attempts: 1,
        };
        utopia_store::jobs::mark_failed(&f.pool, &job, &error).await?;
        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id=$1")
            .bind(job_id)
            .fetch_one(&f.pool)
            .await?;
        assert_eq!(status, "failed");
        documents::set_status(&f.pool, doc, "pending").await?;
        crate::pipeline::process_document(&f.state, doc).await?;
        assert_eq!(documents::get(&f.pool, doc).await?.status, "ready");
        assert!(documents::reader_task(&f.pool, doc).await?.is_none());
        let images = replies.images.lock().unwrap().clone();
        assert_eq!(images.len(), 3);
        assert_ne!(images[0], images[1], "the reader rendered page one twice");
        assert_eq!(
            images[1], images[2],
            "retry must read only the missing page"
        );
        let anchors: Vec<Value> =
            sqlx::query_scalar("SELECT anchor FROM chunks WHERE document_id=$1 ORDER BY seq")
                .bind(doc)
                .fetch_all(&f.pool)
                .await?;
        assert_eq!(anchors, [json!({"page":1}), json!({"page":2})]);
        f.cleanup().await?;
    }
    Ok(())
}

struct UnavailableBlob(Arc<dyn crate::blob::BlobStore>);

#[async_trait::async_trait]
impl crate::blob::BlobStore for UnavailableBlob {
    async fn put(&self, sha256: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.0.put(sha256, bytes).await
    }

    async fn get(&self, _sha256: &str) -> anyhow::Result<Vec<u8>> {
        anyhow::bail!("temporary blob failure")
    }

    async fn exists(&self, sha256: &str) -> anyhow::Result<bool> {
        self.0.exists(sha256).await
    }

    async fn delete(&self, sha256: &str) -> anyhow::Result<()> {
        self.0.delete(sha256).await
    }
}

#[tokio::test]
async fn a_blob_failure_before_parsing_keeps_paid_pages_for_the_next_run() -> anyhow::Result<()> {
    if !has_poppler() {
        return Ok(());
    }
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let _server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.pdf", &two_page_scan()).await?;
    let first = crate::pipeline::process_document(&f.state, doc)
        .await
        .unwrap_err();
    assert!(utopia_core::is_deferred(&first).is_some());
    let completed = task(&f, doc).await?;
    assert_eq!(completed["pages"], json!(["recognized request 1"]));

    let mut unavailable = f.state.clone();
    unavailable.blob = Arc::new(UnavailableBlob(f.state.blob.clone()));
    let error = crate::pipeline::process_document(&unavailable, doc)
        .await
        .unwrap_err();
    assert!(!utopia_core::is_terminal(&error));
    assert!(error.to_string().contains("temporary blob failure"));
    assert_eq!(documents::get(&f.pool, doc).await?.status, "failed");
    assert_eq!(task(&f, doc).await?, completed);
    assert_eq!(replies.images.lock().unwrap().len(), 1);

    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(documents::get(&f.pool, doc).await?.status, "ready");
    assert_eq!(
        replies.images.lock().unwrap().len(),
        2,
        "the already paid first page must survive preparation failures"
    );
    f.cleanup().await
}

#[tokio::test]
async fn downstream_failure_reuses_completed_ocr_but_successful_reprocess_reads_again(
) -> anyhow::Result<()> {
    let mut fake = FakeEmbed::new(Duration::ZERO);
    fake.fail_request = Some(1);
    let Some(f) = fixture(fake).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let _server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    crate::pipeline::process_document(&f.state, doc)
        .await
        .unwrap_err();
    assert_eq!(documents::get(&f.pool, doc).await?.status, "failed");
    assert_eq!(
        task(&f, doc).await?["pages"],
        json!(["recognized request 1"])
    );
    documents::set_status(&f.pool, doc, "pending").await?;
    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(
        replies.images.lock().unwrap().len(),
        1,
        "embedding retry must not repeat OCR"
    );
    assert!(documents::reader_task(&f.pool, doc).await?.is_none());
    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(
        replies.images.lock().unwrap().len(),
        1,
        "a duplicate ready job must not repeat OCR"
    );
    documents::set_status(&f.pool, doc, "pending").await?;
    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(
        replies.images.lock().unwrap().len(),
        2,
        "explicit reprocess after success requests new OCR"
    );
    f.cleanup().await
}

#[tokio::test]
async fn unrelated_settings_saves_keep_the_ocr_identity_and_next_page() -> anyhow::Result<()> {
    if !has_poppler() {
        return Ok(());
    }
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let _server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.pdf", &two_page_scan()).await?;
    assert!(utopia_core::is_deferred(
        &crate::pipeline::process_document(&f.state, doc)
            .await
            .unwrap_err()
    )
    .is_some());
    let checkpoint = task(&f, doc).await?;
    let previous = settings::get(&f.pool, f.ws).await?.unwrap();
    settings::upsert(
        &f.pool,
        f.ws,
        Some("https://chat.invalid/v1"),
        Some("other-key"),
        Some("other-model"),
        previous.embed_base_url.as_deref(),
        None,
        previous.embed_model.as_deref(),
        previous.embed_dim,
    )
    .await?;
    settings::set_chat_reasoning_effort(&f.pool, f.ws, Some("low")).await?;
    settings::upsert_transcribe(
        &f.pool,
        f.ws,
        Some("https://asr.invalid/v1"),
        Some("another-key"),
        Some("other-asr"),
    )
    .await?;
    let current = settings::get(&f.pool, f.ws).await?.unwrap();
    assert_ne!(previous.updated_at, current.updated_at);
    assert_eq!(
        settings::ocr_configuration_fingerprint(&previous),
        settings::ocr_configuration_fingerprint(&current)
    );
    assert_eq!(task(&f, doc).await?, checkpoint);
    // 不排图谱任务：测试只关心 OCR 的第二页，不调用配置中的 chat 端点。
    settings::upsert(
        &f.pool,
        f.ws,
        None,
        None,
        None,
        current.embed_base_url.as_deref(),
        None,
        current.embed_model.as_deref(),
        current.embed_dim,
    )
    .await?;
    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(replies.images.lock().unwrap().len(), 2);
    f.cleanup().await
}

#[tokio::test]
async fn configuring_the_reader_again_preserves_completed_pages() -> anyhow::Result<()> {
    if !has_poppler() {
        return Ok(());
    }
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.pdf", &two_page_scan()).await?;
    assert!(utopia_core::is_deferred(
        &crate::pipeline::process_document(&f.state, doc)
            .await
            .unwrap_err()
    )
    .is_some());
    let paid = task(&f, doc).await?;
    settings::upsert_ocr(&f.pool, f.ws, None, None, None, Some("ark"), None).await?;
    let error = crate::pipeline::process_document(&f.state, doc)
        .await
        .unwrap_err();
    assert!(utopia_core::is_terminal(&error));
    assert_eq!(
        documents::get(&f.pool, doc).await?.reader_needed.as_deref(),
        Some("ocr")
    );
    assert_eq!(task(&f, doc).await?["pages"], paid["pages"]);

    settings::upsert_ocr(
        &f.pool,
        f.ws,
        Some(&server.uri()),
        None,
        None,
        Some("ark"),
        None,
    )
    .await?;
    assert_eq!(
        documents::requeue_waiting_for_reader(&f.pool, f.ws, "ocr").await?,
        [(doc, f.kb)]
    );
    let resumed = task(&f, doc).await?;
    assert_eq!(resumed["pages"], paid["pages"]);
    assert_ne!(resumed["run_token"], paid["run_token"]);
    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(
        replies.images.lock().unwrap().len(),
        2,
        "restoring the same OCR inputs must read only the missing page"
    );
    f.cleanup().await
}

#[tokio::test]
async fn extraction_enqueue_failure_rolls_back_finish_and_keeps_completed_ocr() -> anyhow::Result<()>
{
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let _server = configure(&f, &replies).await?;
    let previous = settings::get(&f.pool, f.ws).await?.unwrap();
    settings::upsert(
        &f.pool,
        f.ws,
        Some("https://chat.invalid/v1"),
        Some("test-key"),
        Some("test-chat"),
        previous.embed_base_url.as_deref(),
        None,
        previous.embed_model.as_deref(),
        previous.embed_dim,
    )
    .await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    // 只拒绝本测试文档的抽取任务，其它并行测试的 jobs 不受影响。
    sqlx::query("UPDATE documents SET graph_error = 'previous extraction failed' WHERE id = $1")
        .bind(doc)
        .execute(&f.pool)
        .await?;
    let name = format!("ark_enqueue_failure_{}", doc.simple());
    sqlx::query(&format!(
        "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF NEW.kind = 'extract_document' AND NEW.payload->>'document_id' = '{doc}'
         THEN RAISE EXCEPTION 'test extraction enqueue failure'; END IF; RETURN NEW; END; $$"
    ))
    .execute(&f.pool)
    .await?;
    sqlx::query(&format!(
        "CREATE TRIGGER {name} BEFORE INSERT ON jobs FOR EACH ROW EXECUTE FUNCTION {name}()"
    ))
    .execute(&f.pool)
    .await?;
    let result = crate::pipeline::process_document(&f.state, doc).await;
    sqlx::query(&format!("DROP TRIGGER {name} ON jobs"))
        .execute(&f.pool)
        .await?;
    sqlx::query(&format!("DROP FUNCTION {name}()"))
        .execute(&f.pool)
        .await?;
    let error = result.unwrap_err();
    assert!(format!("{error:#}").contains("test extraction enqueue failure"));
    let failed = documents::get(&f.pool, doc).await?;
    assert_eq!(
        failed.status, "failed",
        "failure after the first ready must not be swallowed"
    );
    assert_eq!(
        failed.graph_status, "none",
        "graph state and job enqueue must roll back together"
    );
    assert_eq!(
        failed.graph_error.as_deref(),
        Some("previous extraction failed")
    );
    assert_eq!(
        task(&f, doc).await?["pages"],
        json!(["recognized request 1"])
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='extract_document' AND payload->>'document_id'=$1",
    )
    .bind(doc.to_string())
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(count, 0);

    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(
        replies.images.lock().unwrap().len(),
        1,
        "the extraction retry must not charge for OCR again"
    );
    assert!(documents::reader_task(&f.pool, doc).await?.is_none());
    let ready = documents::get(&f.pool, doc).await?;
    assert_eq!(ready.status, "ready");
    assert_eq!(ready.graph_status, "queued");
    assert_eq!(
        ready.graph_error, None,
        "a new extraction attempt clears the previous error"
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='extract_document' AND payload->>'document_id'=$1",
    )
    .bind(doc.to_string())
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(count, 1);
    f.cleanup().await
}

async fn wait_for_request(replies: &OcrReplies) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while replies.images.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_live_jobs_defer_without_changing_the_owners_checkpoint() -> anyhow::Result<()> {
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let mut replies = OcrReplies::new(vec![]);
    replies.delay = Duration::from_secs(1);
    let _server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    let state = f.state.clone();
    let owner = tokio::spawn(async move { crate::pipeline::process_document(&state, doc).await });
    wait_for_request(&replies).await;
    let checkpoint = task(&f, doc).await?;
    let error = crate::pipeline::process_document(&f.state, doc)
        .await
        .unwrap_err();
    assert!(utopia_core::is_deferred(&error).is_some());
    assert_eq!(task(&f, doc).await?, checkpoint);
    assert_eq!(documents::get(&f.pool, doc).await?.status, "parsing");
    owner.await??;
    assert_eq!(replies.images.lock().unwrap().len(), 1);
    f.cleanup().await
}

#[tokio::test]
async fn cancelling_an_active_read_releases_ownership_without_a_time_lease() -> anyhow::Result<()> {
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let mut replies = OcrReplies::new(vec![]);
    replies.delay = Duration::from_secs(1);
    let server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    let state = f.state.clone();
    let owner = tokio::spawn(async move { crate::pipeline::process_document(&state, doc).await });
    wait_for_request(&replies).await;
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    assert_eq!(task(&f, doc).await?["pages"], json!([]));
    server.reset().await;
    let resumed = OcrReplies::new(vec![]);
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(resumed.clone())
        .mount(&server)
        .await;
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::pipeline::process_document(&f.state, doc),
    )
    .await??;
    assert_eq!(resumed.images.lock().unwrap().len(), 1);
    assert_eq!(documents::get(&f.pool, doc).await?.status, "ready");
    f.cleanup().await
}

#[tokio::test]
async fn manual_reprocess_during_read_rejects_old_ready_and_failure() -> anyhow::Result<()> {
    for response in [200, 401] {
        let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
            return Ok(());
        };
        let mut replies = OcrReplies::new(vec![response]);
        replies.delay = Duration::from_secs(1);
        let _server = configure(&f, &replies).await?;
        let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
        let state = f.state.clone();
        let owner =
            tokio::spawn(async move { crate::pipeline::process_document(&state, doc).await });
        wait_for_request(&replies).await;
        let old = task(&f, doc).await?;
        documents::set_status(&f.pool, doc, "pending").await?;
        let pending = task(&f, doc).await?;
        assert_eq!(old["pages"], pending["pages"]);
        assert_ne!(old["run_token"], pending["run_token"]);
        let result = owner.await?.unwrap_err();
        assert!(utopia_core::is_deferred(&result).is_some());
        let row = documents::get(&f.pool, doc).await?;
        assert_eq!(row.status, "pending");
        assert!(row.error.is_none());
        assert_eq!(task(&f, doc).await?, pending);
        crate::pipeline::process_document(&f.state, doc).await?;
        assert_eq!(documents::get(&f.pool, doc).await?.status, "ready");
        f.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_configuration_change_during_read_rejects_the_late_page_and_ready() -> anyhow::Result<()>
{
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let mut replies = OcrReplies::new(vec![]);
    replies.delay = Duration::from_secs(1);
    let server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    let state = f.state.clone();
    let owner = tokio::spawn(async move { crate::pipeline::process_document(&state, doc).await });
    wait_for_request(&replies).await;
    let checkpoint = task(&f, doc).await?;
    settings::upsert_ocr(
        &f.pool,
        f.ws,
        Some(&server.uri()),
        None,
        None,
        Some("ark"),
        Some("new-vision-model"),
    )
    .await?;
    let error = owner.await?.unwrap_err();
    assert!(utopia_core::is_deferred(&error).is_some());
    assert_eq!(task(&f, doc).await?, checkpoint);
    assert_eq!(documents::get(&f.pool, doc).await?.status, "parsing");
    assert!(f.stored(doc).await?.is_empty());
    crate::pipeline::process_document(&f.state, doc).await?;
    assert_eq!(documents::get(&f.pool, doc).await?.status, "ready");
    assert_eq!(replies.images.lock().unwrap().len(), 2);
    f.cleanup().await
}

#[tokio::test]
async fn a_damaged_matching_checkpoint_fails_without_rebilling_completed_pages(
) -> anyhow::Result<()> {
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let _server = configure(&f, &replies).await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    let current = snapshot(&f, doc).await?;
    let mut damaged =
        Checkpoint::new(&current.sha256, &current.configuration_fingerprint, 1)?.task()?;
    damaged["pages"] = json!(["already paid sensitive page", "impossible extra page"]);
    assert!(documents::compare_and_set_ark_ocr_task(&f.pool, doc, &current, &damaged).await?);
    let error = crate::pipeline::process_document(&f.state, doc)
        .await
        .unwrap_err();
    assert!(utopia_core::is_terminal(&error));
    assert!(!format!("{error:#}").contains("sensitive page"));
    assert_eq!(task(&f, doc).await?, damaged);
    assert!(replies.images.lock().unwrap().is_empty());
    f.cleanup().await
}

#[tokio::test]
async fn only_actual_ocr_inputs_use_ark_ownership() -> anyhow::Result<()> {
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let _server = configure(&f, &replies).await?;
    let native_pdf = make_pdf(&[
        "<< /Type /Catalog /Pages 2 0 R >>".into(),
        "<< /Type /Pages /Count 1 /Kids [3 0 R] >>".into(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>".into(),
        pdf_stream("BT /F1 12 Tf 20 100 Td (Native PDF text.) Tj ET"),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".into(),
    ]);
    assert!(utopia_ingest::parse("native.pdf", &native_pdf).is_ok());
    for doc in [
        f.document_with_text("native markdown").await?,
        f.document_with_bytes("native.pdf", &native_pdf).await?,
    ] {
        crate::pipeline::process_document(&f.state, doc).await?;
        assert_eq!(documents::get(&f.pool, doc).await?.status, "ready");
        assert!(documents::reader_task(&f.pool, doc).await?.is_none());
    }
    let transcriber = FakeTranscriber {
        labels: true,
        ..Default::default()
    };
    super::with_transcriber(&f, &transcriber, "test-asr").await?;
    let recording = f.document_with_bytes("meeting.mp3", &MP3).await?;
    crate::pipeline::process_document(&f.state, recording).await?;
    assert_eq!(transcriber.requests.lock().unwrap().len(), 1);
    assert!(replies.images.lock().unwrap().is_empty());
    f.cleanup().await
}

async fn snapshot(f: &Fx, doc: Uuid) -> anyhow::Result<ArkOcrSnapshot> {
    let row = documents::get(&f.pool, doc).await?;
    let settings = settings::get(&f.pool, f.ws).await?.unwrap();
    Ok(ArkOcrSnapshot {
        sha256: row.sha256,
        configuration_fingerprint: settings::ocr_configuration_fingerprint(&settings),
        task: documents::reader_task(&f.pool, doc).await?,
        prepared_updated_at: Some(row.updated_at),
    })
}

#[tokio::test]
async fn stale_checkpoints_cannot_write_pages_chunks_status_ready_or_failure() -> anyhow::Result<()>
{
    for change in ["configuration", "file", "delete", "manual"] {
        let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
            return Ok(());
        };
        let _server = configure(&f, &OcrReplies::new(vec![])).await?;
        let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
        let mut old = snapshot(&f, doc).await?;
        let mut checkpoint = Checkpoint::new(&old.sha256, &old.configuration_fingerprint, 2)?;
        checkpoint.record_page("kept completed page".into())?;
        let task = checkpoint.task()?;
        assert!(documents::compare_and_set_ark_ocr_task(&f.pool, doc, &old, &task).await?);
        old.task = Some(task);
        old.prepared_updated_at = None;
        match change {
            "configuration" => {
                settings::upsert_ocr(
                    &f.pool,
                    f.ws,
                    Some("https://other.invalid"),
                    None,
                    None,
                    Some("ark"),
                    Some("other-model"),
                )
                .await?;
            }
            "file" => {
                sqlx::query("UPDATE documents SET sha256='new-file',status='pending',updated_at=now() WHERE id=$1").bind(doc).execute(&f.pool).await?;
            }
            "delete" => {
                sqlx::query("UPDATE documents SET deleted_at=now() WHERE id=$1")
                    .bind(doc)
                    .execute(&f.pool)
                    .await?;
            }
            "manual" => {
                documents::set_status(&f.pool, doc, "pending").await?;
            }
            _ => unreachable!(),
        }
        let before = documents::get(&f.pool, doc).await?;
        let current_task = documents::reader_task(&f.pool, doc).await?;
        checkpoint.record_page("late page".into())?;
        assert!(
            !documents::compare_and_set_ark_ocr_task(&f.pool, doc, &old, &checkpoint.task()?)
                .await?
        );
        assert!(!documents::set_ark_ocr_status_if_current(&f.pool, doc, &old, "embedding").await?);
        assert!(
            !documents::finish_ark_ocr_if_current(&f.pool, doc, &old, 100, 1, Some("queued"))
                .await?
        );
        assert!(!documents::fail_ark_ocr_if_current(&f.pool, doc, &old, "late error", None).await?);
        let pieces = utopia_ingest::chunk_with_budget("late text", 100);
        assert!(
            documents::replace_ark_ocr_chunks_if_current(&f.pool, f.kb, doc, &pieces, &old)
                .await?
                .is_none()
        );
        let after = documents::get(&f.pool, doc).await?;
        assert_eq!(after.status, before.status);
        assert_eq!(after.error, before.error);
        assert_eq!(after.updated_at, before.updated_at);
        assert_eq!(documents::reader_task(&f.pool, doc).await?, current_task);
        assert!(f.stored(doc).await?.is_empty());
        f.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_manual_restart_before_the_first_claim_cannot_be_borrowed_by_old_preparation(
) -> anyhow::Result<()> {
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let _server = configure(&f, &OcrReplies::new(vec![])).await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    let old = snapshot(&f, doc).await?;
    documents::set_status(&f.pool, doc, "pending").await?;
    let next = Checkpoint::new(&old.sha256, &old.configuration_fingerprint, 1)?.task()?;
    assert!(!documents::compare_and_set_ark_ocr_task(&f.pool, doc, &old, &next).await?);
    assert!(documents::reader_task(&f.pool, doc).await?.is_none());
    let fresh = snapshot(&f, doc).await?;
    assert!(documents::compare_and_set_ark_ocr_task(&f.pool, doc, &fresh, &next).await?);
    f.cleanup().await
}

#[tokio::test]
async fn configuration_fingerprint_matches_only_the_four_effective_ocr_inputs() -> anyhow::Result<()>
{
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let _server = configure(&f, &OcrReplies::new(vec![])).await?;
    let original = settings::get(&f.pool, f.ws).await?.unwrap();
    let fingerprint = settings::ocr_configuration_fingerprint(&original);
    let mut normalized = original.clone();
    normalized.ocr_base_url = Some(format!(" {}/ ", original.ocr_base_url.as_deref().unwrap()));
    normalized.ocr_model = Some(format!(" {} ", original.ocr_model.as_deref().unwrap()));
    normalized.ocr_backend = Some("unrelated-legacy-backend".into());
    normalized.chat_model = Some("unrelated-chat".into());
    normalized.embed_model = Some("unrelated-embedding".into());
    normalized.transcribe_model = Some("unrelated-asr".into());
    normalized.updated_at += chrono::Duration::seconds(1);
    assert_eq!(
        settings::ocr_configuration_fingerprint(&normalized),
        fingerprint
    );

    for field in ["provider", "base", "model", "key"] {
        let mut changed = original.clone();
        match field {
            "provider" => changed.ocr_provider = "mineru".into(),
            "base" => changed.ocr_base_url = Some("https://other.invalid".into()),
            "model" => changed.ocr_model = Some("other-model".into()),
            "key" => changed.ocr_api_key = Some("different-key".into()),
            _ => unreachable!(),
        }
        assert_ne!(
            settings::ocr_configuration_fingerprint(&changed),
            fingerprint,
            "changed OCR {field} must invalidate the checkpoint"
        );
    }
    assert!(!fingerprint.contains(original.ocr_api_key.as_deref().unwrap()));
    let mut empty = original.clone();
    empty.ocr_api_key = None;
    let no_key = settings::ocr_configuration_fingerprint(&empty);
    empty.ocr_api_key = Some(String::new());
    assert_eq!(settings::ocr_configuration_fingerprint(&empty), no_key);
    f.cleanup().await
}

#[tokio::test]
async fn a_restart_after_ready_finishes_the_checkpoint_without_repeating_ocr() -> anyhow::Result<()>
{
    let Some(f) = fixture(FakeEmbed::new(Duration::ZERO)).await? else {
        return Ok(());
    };
    let replies = OcrReplies::new(vec![]);
    let _server = configure(&f, &replies).await?;
    let previous = settings::get(&f.pool, f.ws).await?.unwrap();
    settings::upsert(
        &f.pool,
        f.ws,
        Some("https://chat.invalid/v1"),
        Some("test-key"),
        Some("test-chat"),
        previous.embed_base_url.as_deref(),
        None,
        previous.embed_model.as_deref(),
        previous.embed_dim,
    )
    .await?;
    let doc = f.document_with_bytes("scan.png", PROBE_PNG).await?;
    let mut current = snapshot(&f, doc).await?;
    let mut checkpoint = Checkpoint::new(&current.sha256, &current.configuration_fingerprint, 1)?;
    checkpoint.record_page("already paid and persisted OCR text".into())?;
    let stored = checkpoint.task()?;
    assert!(documents::compare_and_set_ark_ocr_task(&f.pool, doc, &current, &stored).await?);
    current.task = Some(stored.clone());
    current.prepared_updated_at = None;
    assert!(documents::set_ark_ocr_ready_if_current(&f.pool, doc, &current, 35, 1).await?);
    assert_eq!(documents::get(&f.pool, doc).await?.status, "ready");
    assert_eq!(task(&f, doc).await?, stored);

    // 上轮已经退出，没有 live guard；持久 ready + task 仍有未完成的排队/清理工作。
    crate::pipeline::process_document(&f.state, doc).await?;
    assert!(replies.images.lock().unwrap().is_empty());
    assert!(documents::reader_task(&f.pool, doc).await?.is_none());
    let completed = documents::get(&f.pool, doc).await?;
    assert_eq!(completed.status, "ready");
    assert_eq!(completed.graph_status, "queued");
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='extract_document' AND payload->>'document_id'=$1",
    )
    .bind(doc.to_string())
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(count, 1);
    f.cleanup().await
}
