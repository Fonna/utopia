//! 真实认证路由：兼容旧配置调用，只开放 Ark OCR；转写仍拒绝 Ark。
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tower::ServiceExt;
use utopia_core::secrets;
use uuid::Uuid;

async fn call(
    app: &Router,
    token: Option<&str>,
    method: &str,
    path: &str,
    body: Value,
) -> anyhow::Result<(StatusCode, Value)> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string()))?)
        .await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 65536).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

#[test]
fn legacy_payloads_omit_provider_and_explicit_choices_fail_closed() -> anyhow::Result<()> {
    let legacy: super::PutOcrReq =
        serde_json::from_value(json!({"base_url": "http://mineru", "backend": "vlm-auto-engine"}))?;
    assert!(legacy.provider.is_none());
    assert!(legacy.model.is_none());
    let speech: super::PutTranscribeReq =
        serde_json::from_value(json!({"base_url": "https://speech", "model": "diarize"}))?;
    assert!(speech.provider.is_none());
    assert_eq!(super::implemented_provider(None, "mineru")?, None);
    assert_eq!(
        super::implemented_provider(Some(" mineru "), "mineru")?,
        Some("mineru")
    );
    assert_eq!(
        super::implemented_provider(Some("openai"), "openai")?,
        Some("openai")
    );
    assert!(super::implemented_provider(Some("openai"), "mineru").is_err());
    assert!(super::implemented_provider(Some("mineru"), "openai").is_err());
    for provider in ["", "  ", "unknown", "ark"] {
        assert!(super::implemented_provider(Some(provider), "mineru").is_err());
        assert!(super::implemented_provider(Some(provider), "openai").is_err());
    }
    Ok(())
}

#[tokio::test]
async fn reader_settings_routes_preserve_legacy_keys_and_reject_unimplemented_protocols(
) -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    secrets::init(secrets::generate_key());
    let pool = sqlx::PgPool::connect(&url).await?;
    let (org, ws, user) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let dir = tempfile::tempdir()?;
    let cfg = utopia_core::config::AppConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        ..Default::default()
    };
    let state = crate::state::AppState::new(
        pool.clone(),
        &cfg,
        Arc::new(utopia_search::SearchIndex::open(
            &dir.path().join("search"),
        )?),
        "reader-settings-test".into(),
    );
    let app = crate::api::router(state.clone(), &cfg);
    let token = crate::auth::issue_token(&state, user)?;
    sqlx::query("INSERT INTO organizations(id, name) VALUES ($1, 'reader-routes-test')")
        .bind(org)
        .execute(&pool)
        .await?;
    let run = async {
        sqlx::query("INSERT INTO workspaces(id, org_id, name) VALUES ($1, $2, 'reader-routes-test')")
            .bind(ws).bind(org).execute(&pool).await?;
        sqlx::query("INSERT INTO users(id, org_id, email, password_hash, display_name) VALUES ($1, $2, $1::text || '@reader.test', 'unused', 'Reader')")
            .bind(user).bind(org).execute(&pool).await?;
        sqlx::query("INSERT INTO memberships(user_id, workspace_id, role) VALUES ($1, $2, 'admin')")
            .bind(user).bind(ws).execute(&pool).await?;
        let base = format!("/api/v1/workspaces/{ws}/settings");
        for (reader, payload) in [
            ("ocr", json!({"base_url":"http://mineru.example.test", "api_key":"ocr-secret", "backend":"vlm-auto-engine"})),
            ("transcribe", json!({"base_url":"https://speech.example.test/v1", "api_key":"speech-secret", "model":"diarize"})),
        ] {
            let path = format!("{base}/{reader}");
            assert_eq!(call(&app, None, "PUT", &path, payload.clone()).await?.0, StatusCode::UNAUTHORIZED);
            assert_eq!(call(&app, Some(&token), "PUT", &path, payload.clone()).await?.0, StatusCode::OK);
            let before = call(&app, Some(&token), "GET", &base, Value::Null).await?.1;
            let rejected_providers: &[&str] = if reader == "ocr" {
                &["unknown", "", "  "]
            } else {
                &["ark", "unknown", "", "  "]
            };
            for provider in rejected_providers {
                let mut rejected = payload.clone();
                rejected["provider"] = json!(provider);
                rejected["api_key"] = json!("must-not-be-written");
                let (status, error) = call(&app, Some(&token), "PUT", &path, rejected).await?;
                assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
                assert_eq!(error["code"], "unsupported_reader_provider");
                assert_eq!(call(&app, Some(&token), "GET", &base, Value::Null).await?.1, before);
            }
            let mut legacy = payload;
            legacy["api_key"] = json!("  ");
            assert_eq!(call(&app, Some(&token), "PUT", &path, legacy).await?.0, StatusCode::OK);
        }
        let saved = utopia_store::settings::get(&pool, ws).await?.unwrap();
        assert_eq!(saved.ocr_provider, "mineru");
        assert_eq!(saved.transcribe_provider, "openai");
        assert_eq!(saved.ocr_api_key.as_deref(), Some("ocr-secret"));
        assert_eq!(saved.transcribe_api_key.as_deref(), Some("speech-secret"));
        assert!(saved.ocr_ready() && saved.transcribe_ready());
        assert!(crate::readers::Ocr::from_settings(&saved).is_some());
        assert!(crate::readers::Transcriber::from_settings(&saved).is_some());
        let view = call(&app, Some(&token), "GET", &base, Value::Null).await?.1;
        assert_eq!(view["ocr_provider"], "mineru");
        assert_eq!(view["transcribe_provider"], "openai");
        assert_eq!(view["has_ocr_key"], true);
        assert_eq!(view["has_transcribe_key"], true);
        assert!(view.get("ocr_model").is_some());
        assert!(view.get("ocr_api_key").is_none() && view.get("transcribe_api_key").is_none());
        assert!(!view.to_string().contains("secret"));

        // DB 预留值与损坏/未来配置同样不得退回旧协议发送请求。
        for provider in ["ark", "unknown", ""] {
            let mut reserved = saved.clone();
            reserved.ocr_provider = provider.into();
            reserved.transcribe_provider = provider.into();
            assert!(!reserved.ocr_ready() && !reserved.transcribe_ready());
            assert!(crate::readers::Ocr::from_settings(&reserved).is_none());
            assert!(crate::readers::Transcriber::from_settings(&reserved).is_none());
        }
        sqlx::query("UPDATE memberships SET role = 'viewer' WHERE user_id = $1 AND workspace_id = $2")
            .bind(user).bind(ws).execute(&pool).await?;
        for reader in ["ocr", "transcribe"] {
            let path = format!("{base}/{reader}");
            assert_eq!(call(&app, Some(&token), "PUT", &path, json!({"provider":"ark"})).await?.0, StatusCode::FORBIDDEN);
        }
        assert_eq!(call(&app, Some(&token), "GET", &base, Value::Null).await?.0, StatusCode::FORBIDDEN);
        anyhow::Ok(())
    }.await;
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(org)
        .execute(&pool)
        .await?;
    run
}

struct ReaderRoutesFixture {
    pool: sqlx::PgPool,
    org: Uuid,
    ws: Uuid,
    user: Uuid,
    app: Router,
    token: String,
    _directory: tempfile::TempDir,
}

impl ReaderRoutesFixture {
    async fn new() -> anyhow::Result<Option<Self>> {
        let Some(url) = utopia_store::test_db::url() else {
            return Ok(None);
        };
        secrets::init(secrets::generate_key());
        let pool = sqlx::PgPool::connect(&url).await?;
        let (org, ws, user) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let directory = tempfile::tempdir()?;
        let cfg = utopia_core::config::AppConfig {
            data_dir: directory.path().to_string_lossy().into_owned(),
            ..Default::default()
        };
        let state = crate::state::AppState::new(
            pool.clone(),
            &cfg,
            Arc::new(utopia_search::SearchIndex::open(
                &directory.path().join("search"),
            )?),
            "ark-reader-routes-test".into(),
        );
        let app = crate::api::router(state.clone(), &cfg);
        let token = crate::auth::issue_token(&state, user)?;
        sqlx::query("INSERT INTO organizations(id, name) VALUES ($1, 'ark-reader-routes-test')")
            .bind(org)
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO workspaces(id, org_id, name) VALUES ($1, $2, 'ark-reader-routes-test')",
        )
        .bind(ws)
        .bind(org)
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO users(id, org_id, email, password_hash, display_name) VALUES ($1, $2, $1::text || '@ark-reader.test', 'unused', 'Reader')")
            .bind(user)
            .bind(org)
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO memberships(user_id, workspace_id, role) VALUES ($1, $2, 'admin')",
        )
        .bind(user)
        .bind(ws)
        .execute(&pool)
        .await?;
        Ok(Some(Self {
            pool,
            org,
            ws,
            user,
            app,
            token,
            _directory: directory,
        }))
    }

    fn base(&self) -> String {
        format!("/api/v1/workspaces/{}/settings", self.ws)
    }

    async fn saved(&self) -> anyhow::Result<utopia_core::models::LlmSettings> {
        Ok(utopia_store::settings::get(&self.pool, self.ws)
            .await?
            .expect("the test saved settings"))
    }

    async fn stored(&self) -> anyhow::Result<Value> {
        Ok(
            sqlx::query_scalar("SELECT to_jsonb(s) FROM llm_settings s WHERE workspace_id = $1")
                .bind(self.ws)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn waiting_document(&self) -> anyhow::Result<Uuid> {
        let (kb, document) = (Uuid::now_v7(), Uuid::now_v7());
        sqlx::query("INSERT INTO knowledge_bases(id, workspace_id, name) VALUES ($1, $2, 'waiting-for-ocr')")
            .bind(kb)
            .bind(self.ws)
            .execute(&self.pool)
            .await?;
        sqlx::query("INSERT INTO documents(id, kb_id, filename, sha256, status, reader_needed) VALUES ($1, $2, 'scan.png', $1::text, 'failed', 'ocr')")
            .bind(document)
            .bind(kb)
            .execute(&self.pool)
            .await?;
        Ok(document)
    }

    async fn document_status(&self, document: Uuid) -> anyhow::Result<String> {
        Ok(
            sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
                .bind(document)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn processing_jobs(&self, document: Uuid) -> anyhow::Result<i64> {
        Ok(sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'process_document' AND payload->>'document_id' = $1")
            .bind(document.to_string())
            .fetch_one(&self.pool)
            .await?)
    }

    async fn make_viewer(&self) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE memberships SET role = 'viewer' WHERE user_id = $1 AND workspace_id = $2",
        )
        .bind(self.user)
        .bind(self.ws)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM jobs WHERE kind = 'process_document' AND payload->>'document_id' IN (SELECT d.id::text FROM documents d JOIN knowledge_bases k ON k.id = d.kb_id WHERE k.workspace_id = $1)")
            .bind(self.ws)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM organizations WHERE id = $1")
            .bind(self.org)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

struct LocalModel {
    base: String,
    calls: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl LocalModel {
    async fn new() -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let app = Router::new().fallback(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                axum::Json(json!({
                    "id": "local-probe", "object": "chat.completion", "created": 0,
                    "model": "local-model",
                    "choices": [{
                        "index": 0,
                        "finish_reason": "stop",
                        "message": { "role": "assistant", "content": "{\"text\":\"\"}" }
                    }],
                    "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 }
                }))
            }
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(Self {
            base,
            calls,
            server,
        })
    }

    fn count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Drop for LocalModel {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn ark_ocr_configuration_is_partial_and_invalid_legacy_updates_roll_back(
) -> anyhow::Result<()> {
    let Some(fixture) = ReaderRoutesFixture::new().await? else {
        return Ok(());
    };
    let model = LocalModel::new().await?;
    let base = format!("{}/api/plan/v3", model.base);
    let path = format!("{}/ocr", fixture.base());
    let run = async {
        let waiting = fixture.waiting_document().await?;
        let partial = json!({ "provider": "ark", "base_url": base, "api_key": "local-ocr-key" });
        assert_eq!(call(&fixture.app, None, "PUT", &path, partial.clone()).await?.0, StatusCode::UNAUTHORIZED);
        assert!(utopia_store::settings::get(&fixture.pool, fixture.ws).await?.is_none());
        let (status, result) = call(&fixture.app, Some(&fixture.token), "PUT", &path, partial).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["requeued"], 0);
        let saved = fixture.saved().await?;
        assert_eq!(saved.ocr_provider, "ark");
        assert!(saved.ocr_model.is_none());
        assert!(!saved.ocr_ready());
        assert!(crate::readers::Ocr::from_settings(&saved).is_none());
        assert_eq!(fixture.document_status(waiting).await?, "failed");
        assert_eq!(fixture.processing_jobs(waiting).await?, 0);
        assert_eq!(model.count(), 0, "saving partial settings called the model");

        // 旧请求省略 provider/key；补 model 后采用实际 Ark 配置并唤醒缺读取器的文档。
        let (status, result) = call(&fixture.app, Some(&fixture.token), "PUT", &path,
            json!({ "base_url": base, "model": "  local-vision-model  " })).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["requeued"], 1);
        let saved = fixture.saved().await?;
        assert_eq!(saved.ocr_provider, "ark");
        assert_eq!(saved.ocr_api_key.as_deref(), Some("local-ocr-key"));
        assert_eq!(saved.ocr_model.as_deref(), Some("local-vision-model"));
        assert!(saved.ocr_ready());
        assert!(crate::readers::Ocr::from_settings(&saved).is_some());
        assert_eq!(fixture.document_status(waiting).await?, "pending");
        assert_eq!(fixture.processing_jobs(waiting).await?, 1);

        // 全部三个新增字段缺席时仍保留已选择的协议、模型与密钥。
        assert_eq!(call(&fixture.app, Some(&fixture.token), "PUT", &path,
            json!({ "base_url": base, "backend": "legacy-ignored-backend" })).await?.0, StatusCode::OK);
        let saved = fixture.saved().await?;
        assert_eq!(saved.ocr_provider, "ark");
        assert_eq!(saved.ocr_model.as_deref(), Some("local-vision-model"));
        assert_eq!(saved.ocr_api_key.as_deref(), Some("local-ocr-key"));
        let before = fixture.stored().await?;
        let still_waiting = fixture.waiting_document().await?;
        for payload in [
            json!({ "base_url": format!("{base}?key=route-private-secret"), "api_key": "must-not-be-written" }),
            json!({ "base_url": base.replacen("http://", "http://route-private-secret@", 1), "model": "must-not-be-written" }),
            json!({ "base_url": base, "api_key": "route-private-secret\ninjected-header" }),
            json!({ "base_url": base, "model": "local-model\u{7}" }),
        ] {
            let (status, error) = call(&fixture.app, Some(&fixture.token), "PUT", &path, payload).await?;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
            assert_eq!(error["code"], "bad_ocr_config");
            assert!(!error.to_string().contains("route-private-secret"));
            assert_eq!(fixture.stored().await?, before, "a rejected save changed the row");
            assert_eq!(fixture.document_status(still_waiting).await?, "failed");
            assert_eq!(fixture.processing_jobs(still_waiting).await?, 0);
        }

        let (status, error) = call(&fixture.app, Some(&fixture.token), "PUT",
            &format!("{}/transcribe", fixture.base()),
            json!({ "provider": "ark", "base_url": base, "model": "speech", "api_key": "must-not-be-written" })).await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(error["code"], "unsupported_reader_provider");
        assert_eq!(fixture.stored().await?, before);

        // 空模型和空地址各自关闭读取器，不探测、不重新排队缺读取器的文件。
        for payload in [
            json!({ "provider": "ark", "base_url": base, "model": " " }),
            json!({ "provider": "ark", "base_url": " ", "model": "local-vision-model" }),
        ] {
            let (status, result) = call(&fixture.app, Some(&fixture.token), "PUT", &path, payload).await?;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(result["requeued"], 0);
            let saved = fixture.saved().await?;
            assert!(!saved.ocr_ready());
            assert!(crate::readers::Ocr::from_settings(&saved).is_none());
            assert_eq!(fixture.document_status(still_waiting).await?, "failed");
            assert_eq!(fixture.processing_jobs(still_waiting).await?, 0);
        }
        assert_eq!(model.count(), 0, "a settings save performed a model request");
        anyhow::Ok(())
    }.await;
    fixture.cleanup().await?;
    run
}

#[tokio::test]
async fn scoped_connectivity_tests_only_call_the_selected_model_and_require_admin(
) -> anyhow::Result<()> {
    let Some(fixture) = ReaderRoutesFixture::new().await? else {
        return Ok(());
    };
    let (chat, embed, ocr, transcribe) = (
        LocalModel::new().await?,
        LocalModel::new().await?,
        LocalModel::new().await?,
        LocalModel::new().await?,
    );
    let base = fixture.base();
    let scoped = format!("{base}/test?scope=ocr");
    let run = async {
        // 工作区尚无设置行时，未选中的卡片仍必须为 null；旧调用仍返回四项结果。
        assert!(utopia_store::settings::get(&fixture.pool, fixture.ws).await?.is_none());
        assert_eq!(call(&fixture.app, None, "POST", &scoped, Value::Null).await?.0, StatusCode::UNAUTHORIZED);
        let (status, result) = call(&fixture.app, Some(&fixture.token), "POST", &scoped, Value::Null).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["ocr"], json!({ "ok": false, "error": "Not configured" }));
        for skipped in ["chat", "embed", "transcribe"] {
            assert_eq!(result.get(skipped), Some(&Value::Null));
        }
        let (status, legacy) = call(&fixture.app, Some(&fixture.token), "POST", &format!("{base}/test"), Value::Null).await?;
        assert_eq!(status, StatusCode::OK);
        for card in ["chat", "embed", "ocr", "transcribe"] {
            assert_eq!(legacy[card], json!({ "ok": false, "error": "Not configured" }));
        }

        for (suffix, payload) in [
            ("", json!({ "chat_base_url": format!("{}/v1", chat.base), "chat_api_key": "local-chat-key", "chat_model": "chat-model", "embed_base_url": format!("{}/v1", embed.base), "embed_api_key": "local-embed-key", "embed_model": "embed-model" })),
            ("/ocr", json!({ "provider": "ark", "base_url": format!("{}/api/plan/v3", ocr.base), "api_key": "local-ocr-key", "model": "local-vision-model" })),
            ("/transcribe", json!({ "provider": "openai", "base_url": format!("{}/v1", transcribe.base), "api_key": "local-transcribe-key", "model": "diarize-model" })),
        ] {
            assert_eq!(call(&fixture.app, Some(&fixture.token), "PUT", &format!("{base}{suffix}"), payload).await?.0, StatusCode::OK);
        }
        let saved = fixture.saved().await?;
        assert!(saved.chat_ready() && saved.embed_ready() && saved.ocr_ready() && saved.transcribe_ready());
        assert!(crate::llm_util::chat_client(&saved).is_some());
        assert!(crate::llm_util::embed_client(&saved).is_some());
        assert!(crate::readers::Ocr::from_settings(&saved).is_some());
        assert!(crate::readers::Transcriber::from_settings(&saved).is_some());
        for model in [&chat, &embed, &ocr, &transcribe] {
            assert_eq!(model.count(), 0, "configuration or an unconfigured test called a model");
        }

        let (status, result) = call(&fixture.app, Some(&fixture.token), "POST", &scoped, Value::Null).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["ocr"]["ok"], true);
        assert_eq!(result["ocr"]["version"], "local-vision-model");
        for skipped in ["chat", "embed", "transcribe"] {
            assert_eq!(result.get(skipped), Some(&Value::Null));
        }
        assert_eq!(ocr.count(), 1);
        assert_eq!(chat.count(), 0);
        assert_eq!(embed.count(), 0);
        assert_eq!(transcribe.count(), 0);

        // 测试聊天也不得额外调用付费 OCR；反向路径不能只靠OCR的scope用例推断。
        let (status, result) = call(&fixture.app, Some(&fixture.token), "POST",
            &format!("{base}/test?scope=chat"), Value::Null).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["chat"]["ok"], true);
        for skipped in ["ocr", "embed", "transcribe"] {
            assert_eq!(result.get(skipped), Some(&Value::Null));
        }
        assert_eq!(chat.count(), 1);
        assert_eq!(ocr.count(), 1);
        assert_eq!(embed.count(), 0);
        assert_eq!(transcribe.count(), 0);

        assert_eq!(call(&fixture.app, None, "POST", &scoped, Value::Null).await?.0, StatusCode::UNAUTHORIZED);
        fixture.make_viewer().await?;
        assert_eq!(call(&fixture.app, Some(&fixture.token), "POST", &scoped, Value::Null).await?.0, StatusCode::FORBIDDEN);
        assert_eq!(call(&fixture.app, Some(&fixture.token), "PUT", &format!("{base}/ocr"),
            json!({ "provider": "ark", "base_url": ocr.base, "model": "replacement" })).await?.0, StatusCode::FORBIDDEN);
        assert_eq!(ocr.count(), 1, "an unauthorized probe called the OCR model");
        assert_eq!(chat.count(), 1);
        assert_eq!(embed.count(), 0);
        assert_eq!(transcribe.count(), 0);
        anyhow::Ok(())
    }.await;
    fixture.cleanup().await?;
    run
}
