//! 配置基础可以先合：旧请求仍可用，未实现协议在真实认证路由与读取器两层都关闭。
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use std::sync::Arc;
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
            for provider in ["ark", "unknown", "", "  "] {
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
