//! OCR 协议与密钥在同一条 SQL 中切换；省略协议的请求不能把它改回默认值。
use sqlx::PgPool;
use utopia_core::secrets;
use utopia_store::settings;
use uuid::Uuid;

#[tokio::test]
async fn ocr_provider_and_key_change_atomically() -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    secrets::init(secrets::generate_key());
    let pool = PgPool::connect(&url).await?;
    let (org, ws) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO organizations(id, name) VALUES ($1, 'ocr-settings-test')")
        .bind(org)
        .execute(&pool)
        .await?;
    let run = async {
        sqlx::query(
            "INSERT INTO workspaces(id, org_id, name) VALUES ($1, $2, 'ocr-settings-test')",
        )
        .bind(ws)
        .bind(org)
        .execute(&pool)
        .await?;
        settings::upsert(
            &pool,
            ws,
            None,
            Some("chat-key"),
            None,
            None,
            Some("embed-key"),
            None,
            None,
        )
        .await?;
        let initial = settings::upsert_ocr(
            &pool,
            ws,
            Some("https://reader.example.test"),
            Some("mineru-key"),
            None,
            Some("mineru"),
            None,
        )
        .await?;
        assert_eq!(initial.ocr_provider, "mineru");
        for empty in [None, Some(""), Some("  ")] {
            let unchanged = settings::upsert_ocr(
                &pool,
                ws,
                Some("https://reader.example.test"),
                empty,
                None,
                Some("mineru"),
                None,
            )
            .await?;
            assert_eq!(unchanged.ocr_api_key.as_deref(), Some("mineru-key"));
        }
        let switched = settings::upsert_ocr(
            &pool,
            ws,
            Some("https://reader.example.test"),
            None,
            None,
            Some("ark"),
            Some("vision"),
        )
        .await?;
        assert_eq!(switched.ocr_provider, "ark");
        assert_eq!(
            switched.ocr_api_key, None,
            "a new provider cannot inherit the MinerU key"
        );
        assert_eq!(switched.ocr_model.as_deref(), Some("vision"));
        let configured = settings::upsert_ocr(
            &pool,
            ws,
            Some("https://reader.example.test"),
            Some("ark-key"),
            None,
            Some("ark"),
            None,
        )
        .await?;
        assert_eq!(configured.ocr_api_key.as_deref(), Some("ark-key"));
        assert_eq!(configured.ocr_model.as_deref(), Some("vision"));
        let stored_key: Option<String> =
            sqlx::query_scalar("SELECT ocr_api_key FROM llm_settings WHERE workspace_id = $1")
                .bind(ws)
                .fetch_one(&pool)
                .await?;
        assert!(secrets::is_sealed(stored_key.as_deref().unwrap()));
        assert!(settings::upsert_ocr(
            &pool,
            ws,
            Some("https://reader.example.test"),
            None,
            None,
            Some("unknown"),
            None,
        )
        .await
        .is_err());
        assert_eq!(settings::get(&pool, ws).await?.unwrap().ocr_provider, "ark");

        // 显式切换与省略 provider 的保存并发；两种落库顺序都不得还原旧协议。
        settings::upsert_ocr(
            &pool,
            ws,
            Some("https://mineru.example.test"),
            Some("previous-key"),
            None,
            Some("mineru"),
            None,
        )
        .await?;
        let (explicit, omitted) = tokio::join!(
            settings::upsert_ocr(
                &pool,
                ws,
                Some("https://ark.example.test"),
                Some("next-key"),
                None,
                Some("ark"),
                Some("next-vision"),
            ),
            settings::upsert_ocr(
                &pool,
                ws,
                Some("https://ark.example.test"),
                None,
                None,
                None,
                None,
            ),
        );
        explicit?;
        omitted?;
        let current = settings::get(&pool, ws).await?.unwrap();
        assert_eq!(current.ocr_provider, "ark");
        assert_eq!(current.ocr_api_key.as_deref(), Some("next-key"));
        assert_eq!(current.ocr_model.as_deref(), Some("next-vision"));
        let cleared = settings::upsert_ocr(
            &pool,
            ws,
            Some("https://ark.example.test"),
            None,
            None,
            None,
            Some("  "),
        )
        .await?;
        assert_eq!(cleared.ocr_model, None, "an explicit blank model clears it");
        let restored = settings::upsert_ocr(
            &pool,
            ws,
            Some("https://mineru.example.test"),
            None,
            None,
            Some("mineru"),
            None,
        )
        .await?;
        assert_eq!(restored.ocr_model, None);
        assert_eq!(restored.ocr_api_key, None);
        assert_eq!(restored.chat_api_key.as_deref(), Some("chat-key"));
        assert_eq!(restored.embed_api_key.as_deref(), Some("embed-key"));
        anyhow::Ok(())
    }
    .await;
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(org)
        .execute(&pool)
        .await?;
    run
}
