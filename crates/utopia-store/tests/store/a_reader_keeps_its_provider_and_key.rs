//! 0065：协议切换与凭据清除必须是一条原子写入；旧调用不能把协议或新增模型改回默认值。
use sqlx::PgPool;
use utopia_core::{models::LlmSettings, secrets};
use utopia_store::settings;
use uuid::Uuid;

async fn save(
    pool: &PgPool,
    ws: Uuid,
    reader: &str,
    provider: Option<&str>,
    key: Option<&str>,
) -> anyhow::Result<LlmSettings> {
    Ok(match reader {
        "ocr" => {
            settings::upsert_ocr_with_provider(
                pool,
                ws,
                Some("https://reader.example.test"),
                key,
                None,
                provider,
                None,
            )
            .await?
        }
        "transcribe" => {
            settings::upsert_transcribe_with_provider(
                pool,
                ws,
                Some("https://reader.example.test"),
                key,
                Some("speech"),
                provider,
            )
            .await?
        }
        _ => unreachable!(),
    })
}

fn key<'a>(settings: &'a LlmSettings, reader: &str) -> Option<&'a str> {
    match reader {
        "ocr" => settings.ocr_api_key.as_deref(),
        "transcribe" => settings.transcribe_api_key.as_deref(),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn a_reader_keeps_its_provider_and_key_in_one_write() -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    secrets::init(secrets::generate_key());
    let pool = PgPool::connect(&url).await?;
    let (org, ws) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO organizations(id, name) VALUES ($1, 'reader-settings-test')")
        .bind(org)
        .execute(&pool)
        .await?;
    let run = async {
        sqlx::query(
            "INSERT INTO workspaces(id, org_id, name) VALUES ($1, $2, 'reader-settings-test')",
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
        for (reader, original) in [("ocr", "mineru"), ("transcribe", "openai")] {
            let initial = save(&pool, ws, reader, None, Some("original-key")).await?;
            assert_eq!(initial.ocr_provider, "mineru");
            assert_eq!(initial.transcribe_provider, "openai");
            for empty in [None, Some(""), Some("  ")] {
                let unchanged = save(&pool, ws, reader, Some(original), empty).await?;
                assert_eq!(key(&unchanged, reader), Some("original-key"));
            }
            let switched = save(&pool, ws, reader, Some("ark"), None).await?;
            assert_eq!(
                key(&switched, reader),
                None,
                "a different protocol loses the old credential"
            );
            assert_eq!(
                key(
                    &save(&pool, ws, reader, Some("ark"), Some("new-key")).await?,
                    reader
                ),
                Some("new-key")
            );
            let omitted = save(&pool, ws, reader, None, None).await?;
            assert_eq!(key(&omitted, reader), Some("new-key"));
            assert_eq!(
                if reader == "ocr" {
                    &omitted.ocr_provider
                } else {
                    &omitted.transcribe_provider
                },
                "ark"
            );
            let stored: Option<String> = sqlx::query_scalar(&format!(
                "SELECT {reader}_api_key FROM llm_settings WHERE workspace_id = $1"
            ))
            .bind(ws)
            .fetch_one(&pool)
            .await?;
            assert!(secrets::is_sealed(stored.as_deref().unwrap()));
            assert!(save(&pool, ws, reader, Some("unknown"), None)
                .await
                .is_err());
            assert_eq!(
                key(&settings::get(&pool, ws).await?.unwrap(), reader),
                Some("new-key")
            );
            let restored = save(&pool, ws, reader, Some(original), Some("replacement-key")).await?;
            assert_eq!(key(&restored, reader), Some("replacement-key"));
            assert_eq!(restored.chat_api_key.as_deref(), Some("chat-key"));
            assert_eq!(restored.embed_api_key.as_deref(), Some("embed-key"));
        }

        // 不读旧 provider 的调用与明确换协议并发，先后两种次序都必须留在新协议上。
        let (switched, legacy) = tokio::join!(
            settings::upsert_ocr_with_provider(
                &pool,
                ws,
                Some("https://ark.example.test"),
                Some("ark-key"),
                None,
                Some("ark"),
                Some("vision")
            ),
            settings::upsert_ocr(&pool, ws, Some("https://ark.example.test"), None, None),
        );
        switched?;
        legacy?;
        let got = settings::get(&pool, ws).await?.unwrap();
        assert_eq!(got.ocr_provider, "ark");
        assert_eq!(got.ocr_api_key.as_deref(), Some("ark-key"));
        assert_eq!(got.ocr_model.as_deref(), Some("vision"));
        let cleared = settings::upsert_ocr_with_provider(
            &pool,
            ws,
            Some("https://ark.example.test"),
            None,
            None,
            None,
            Some("  "),
        )
        .await?;
        assert_eq!(cleared.ocr_model, None, "an explicit empty model clears it");
        settings::upsert_ocr_with_provider(
            &pool,
            ws,
            Some("https://ark.example.test"),
            None,
            None,
            None,
            Some("vision"),
        )
        .await?;
        let restored = settings::upsert_ocr_with_provider(
            &pool,
            ws,
            Some("http://mineru.example.test"),
            None,
            Some("vlm-auto-engine"),
            Some("mineru"),
            None,
        )
        .await?;
        assert_eq!(
            restored.ocr_model, None,
            "a protocol change cannot inherit the previous model"
        );
        assert_eq!(restored.ocr_api_key, None);
        let legacy = settings::upsert_transcribe(&pool, ws, None, None, None).await?;
        assert_eq!(legacy.transcribe_provider, "openai");
        assert_eq!(
            legacy.transcribe_model, None,
            "the original transcription replacement contract is unchanged"
        );
        assert_eq!(
            legacy.transcribe_api_key.as_deref(),
            Some("replacement-key")
        );
        anyhow::Ok(())
    }
    .await;
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(org)
        .execute(&pool)
        .await?;
    run
}

#[tokio::test]
async fn reader_provider_migration_preserves_existing_configurations() -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    let pool = PgPool::connect(&url).await?;
    let mut tx = pool.begin().await?;
    // 临时表遮住真表，在同一连接验证旧行升级；不改共用测试库的迁移或已有设置。
    sqlx::raw_sql(
        "CREATE TEMP TABLE llm_settings (ocr_api_key TEXT, transcribe_api_key TEXT) ON COMMIT DROP;
                   INSERT INTO llm_settings VALUES ('old-ocr-key', 'old-speech-key')",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../../migrations/0101_readers_choose_their_provider.sql"
    ))
    .execute(&mut *tx)
    .await?;
    let row: (String, String, Option<String>, String, String) = sqlx::query_as(
        "SELECT ocr_provider, transcribe_provider, ocr_model, ocr_api_key, transcribe_api_key FROM llm_settings",
    )
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(
        row,
        (
            "mineru".into(),
            "openai".into(),
            None,
            "old-ocr-key".into(),
            "old-speech-key".into()
        )
    );
    tx.rollback().await?;
    Ok(())
}
