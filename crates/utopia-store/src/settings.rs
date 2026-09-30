use sqlx::PgPool;
use utopia_core::models::LlmSettings;
use utopia_core::{secrets, AppError, AppResult};
use uuid::Uuid;

/// OCR 任务的配置身份只包含其实际输入；同一密钥的重新封印、其它卡片保存不改变它。
/// 调用方必须先解封 OCR key。None 与空 key 等价，地址与读取器一样去掉末尾斜线。
pub fn ocr_configuration_fingerprint(settings: &LlmSettings) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for value in [
        settings.ocr_provider.as_str(),
        settings
            .ocr_base_url
            .as_deref()
            .unwrap_or("")
            .trim()
            .trim_end_matches('/'),
        settings.ocr_model.as_deref().unwrap_or("").trim(),
        settings.ocr_api_key.as_deref().unwrap_or(""),
    ] {
        // 长度前缀避免相邻字段拼接后出现同一身份。
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 文档先锁、设置再锁，与检查点提交保持同一个短事务；只解封 OCR key。
pub(crate) async fn locked_ocr_configuration_fingerprint(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: Uuid,
) -> AppResult<Option<String>> {
    let row: Option<LlmSettings> =
        sqlx::query_as("SELECT * FROM llm_settings WHERE workspace_id = $1 FOR SHARE")
            .bind(workspace_id)
            .fetch_optional(&mut **tx)
            .await?;
    row.map(|mut settings| {
        settings.ocr_api_key =
            secrets::open_opt(settings.ocr_api_key.as_deref()).map_err(AppError::Other)?;
        Ok(ocr_configuration_fingerprint(&settings))
    })
    .transpose()
}

/// 出库即开封：四把 API key 在库里是封印的（`utopia_core::secrets`）。
/// 任何返回 `LlmSettings` 的查询都从这里过
fn opened(mut s: LlmSettings) -> AppResult<LlmSettings> {
    s.chat_api_key = secrets::open_opt(s.chat_api_key.as_deref()).map_err(AppError::Other)?;
    s.embed_api_key = secrets::open_opt(s.embed_api_key.as_deref()).map_err(AppError::Other)?;
    s.ocr_api_key = secrets::open_opt(s.ocr_api_key.as_deref()).map_err(AppError::Other)?;
    s.transcribe_api_key =
        secrets::open_opt(s.transcribe_api_key.as_deref()).map_err(AppError::Other)?;
    Ok(s)
}

pub async fn get(pool: &PgPool, workspace_id: Uuid) -> AppResult<Option<LlmSettings>> {
    let row: Option<LlmSettings> =
        sqlx::query_as("SELECT * FROM llm_settings WHERE workspace_id = $1")
            .bind(workspace_id)
            .fetch_optional(pool)
            .await?;
    row.map(opened).transpose()
}

/// 任取一个配了对话模型的工作区设置。给端点探针用：端点地址是部署共用的，
/// 从哪个工作区的配置读到的都是同一个地方，而探针没有"当前工作区"这个上下文。
pub async fn any_with_chat(pool: &PgPool) -> AppResult<Option<LlmSettings>> {
    let row: Option<LlmSettings> = sqlx::query_as(
        "SELECT * FROM llm_settings
         WHERE chat_base_url IS NOT NULL AND chat_model IS NOT NULL
         ORDER BY workspace_id LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    row.map(opened).transpose()
}

/// upsert；api_key 传 None 表示保留旧值（前端不回传密钥）。
#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    pool: &PgPool,
    workspace_id: Uuid,
    chat_base_url: Option<&str>,
    chat_api_key: Option<&str>,
    chat_model: Option<&str>,
    embed_base_url: Option<&str>,
    embed_api_key: Option<&str>,
    embed_model: Option<&str>,
    embed_dim: Option<i32>,
) -> AppResult<LlmSettings> {
    // 入库即封印；None 仍是 None（保留旧值）
    let chat_api_key = secrets::seal_opt(chat_api_key);
    let embed_api_key = secrets::seal_opt(embed_api_key);
    let row: LlmSettings = sqlx::query_as(
        "INSERT INTO llm_settings
             (workspace_id, chat_base_url, chat_api_key, chat_model,
              embed_base_url, embed_api_key, embed_model, embed_dim, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
         ON CONFLICT (workspace_id) DO UPDATE SET
             chat_base_url  = EXCLUDED.chat_base_url,
             chat_api_key   = COALESCE(EXCLUDED.chat_api_key, llm_settings.chat_api_key),
             chat_model     = EXCLUDED.chat_model,
             embed_base_url = EXCLUDED.embed_base_url,
             embed_api_key  = COALESCE(EXCLUDED.embed_api_key, llm_settings.embed_api_key),
             embed_model    = EXCLUDED.embed_model,
             embed_dim      = EXCLUDED.embed_dim,
             updated_at     = now()
         RETURNING *",
    )
    .bind(workspace_id)
    .bind(chat_base_url)
    .bind(chat_api_key)
    .bind(chat_model)
    .bind(embed_base_url)
    .bind(embed_api_key)
    .bind(embed_model)
    .bind(embed_dim)
    .fetch_one(pool)
    .await?;
    opened(row)
}

/// 对话模型的推理强度，单独改：它跟着对话模型那张卡走，但 `upsert` 的整体替换不认识它，
/// 老调用方不传也不该把它清掉。`None` = 清空（回到端点默认）
pub async fn set_chat_reasoning_effort(
    pool: &PgPool,
    workspace_id: Uuid,
    effort: Option<&str>,
) -> AppResult<()> {
    sqlx::query(
        "UPDATE llm_settings SET chat_reasoning_effort = $2, updated_at = now() WHERE workspace_id = $1",
    )
    .bind(workspace_id)
    .bind(effort)
    .execute(pool)
    .await?;
    Ok(())
}

/// 版面识别服务的设置，单独存：它在管理页上是自己的一张卡片，存它不该碰对话和嵌入那几列
/// （反过来也一样——`upsert` 不写这三列）。`api_key` 传 None 保留旧值；地址传 None = 关掉
/// 缺席 provider/model 的旧调用保留当前值；显式空 model 清空。
/// 换协议时，没给的新密钥与模型都清掉。比较留在同一条 SQL 中，不能先读再写（0065）。
pub async fn upsert_ocr(
    pool: &PgPool,
    workspace_id: Uuid,
    base_url: Option<&str>,
    api_key: Option<&str>,
    backend: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
) -> AppResult<LlmSettings> {
    let mut tx = pool.begin().await?;
    let settings = upsert_ocr_tx(
        &mut tx,
        workspace_id,
        base_url,
        api_key,
        backend,
        provider,
        model,
    )
    .await?;
    tx.commit().await?;
    Ok(settings)
}

/// API 根据实际保存后的协议验证，然后提交；旧请求省略 provider 也不能绕过验证。
pub async fn upsert_ocr_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: Uuid,
    base_url: Option<&str>,
    api_key: Option<&str>,
    backend: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
) -> AppResult<LlmSettings> {
    let api_key = secrets::seal_opt(api_key.filter(|key| !key.trim().is_empty()));
    let model = model.map(str::trim);
    let row: LlmSettings = sqlx::query_as(
        "INSERT INTO llm_settings
             (workspace_id, ocr_base_url, ocr_api_key, ocr_backend, ocr_provider, ocr_model, updated_at)
         VALUES ($1, $2, $3, $4, COALESCE($5, 'mineru'), NULLIF($6, ''), now())
         ON CONFLICT (workspace_id) DO UPDATE SET
             ocr_base_url = EXCLUDED.ocr_base_url,
             ocr_api_key = CASE
                 WHEN $5 IS NOT NULL AND $5 <> llm_settings.ocr_provider
                 THEN EXCLUDED.ocr_api_key
                 ELSE COALESCE(EXCLUDED.ocr_api_key, llm_settings.ocr_api_key) END,
             ocr_backend  = EXCLUDED.ocr_backend,
             ocr_provider = COALESCE($5, llm_settings.ocr_provider),
             ocr_model = CASE
                 WHEN $5 IS NOT NULL AND $5 <> llm_settings.ocr_provider
                 THEN EXCLUDED.ocr_model
                 WHEN $6 IS NULL THEN llm_settings.ocr_model
                 ELSE EXCLUDED.ocr_model END,
             updated_at   = now()
         RETURNING *",
    )
    .bind(workspace_id)
    .bind(base_url)
    .bind(api_key)
    .bind(backend)
    .bind(provider)
    .bind(model)
    .fetch_one(&mut **tx)
    .await?;
    opened(row)
}

/// 转写模型的设置，跟版面识别服务一样单独存（管理页上各是一张卡片）
pub async fn upsert_transcribe(
    pool: &PgPool,
    workspace_id: Uuid,
    base_url: Option<&str>,
    api_key: Option<&str>,
    model: Option<&str>,
) -> AppResult<LlmSettings> {
    let api_key = secrets::seal_opt(api_key);
    let row: LlmSettings = sqlx::query_as(
        "INSERT INTO llm_settings
             (workspace_id, transcribe_base_url, transcribe_api_key, transcribe_model, updated_at)
         VALUES ($1, $2, $3, $4, now())
         ON CONFLICT (workspace_id) DO UPDATE SET
             transcribe_base_url = EXCLUDED.transcribe_base_url,
             transcribe_api_key = COALESCE(EXCLUDED.transcribe_api_key, llm_settings.transcribe_api_key),
             transcribe_model    = EXCLUDED.transcribe_model,
             updated_at          = now()
         RETURNING *",
    )
    .bind(workspace_id)
    .bind(base_url)
    .bind(api_key)
    .bind(model)
    .fetch_one(pool)
    .await?;
    opened(row)
}
