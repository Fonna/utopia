//! 摄入管道：parse → chunk → 全文索引 → embedding（可选）→ ready。
//! 每步幂等：重跑会先清掉旧分块与旧索引条目。

use crate::llm_util;
use crate::state::AppState;
use futures_util::{stream, StreamExt};
use utopia_core::models::{Document, LlmSettings, Proposer};
use utopia_llm::LlmClient;
use uuid::Uuid;

/// 这份文档的来源要不要抽取。没有来源的文档（直接上传、记忆片段）照旧抽。
/// 文档所属来源的种类；没有来源（上传）为 None。抽取那边也要问同一个问题（0054）
pub(crate) async fn source_kind(
    state: &AppState,
    source_id: Option<Uuid>,
) -> anyhow::Result<Option<String>> {
    let Some(id) = source_id else {
        return Ok(None);
    };
    Ok(Some(
        utopia_store::sources::get(&state.pool, id).await?.kind,
    ))
}

async fn source_extracts(state: &AppState, source_id: Option<Uuid>) -> anyhow::Result<bool> {
    let Some(id) = source_id else {
        return Ok(true);
    };
    Ok(utopia_store::sources::get(&state.pool, id)
        .await?
        .extracts())
}

/// 每次送多少条去嵌入。
///
/// 与 ontology_index 的 64 不同，这里没量过，先不动：那边的注释说批大小要拿真实文本量，
/// 块文本比类标签长得多，16 未必错。并发与批大小两个旋钮别在一刀里同时拧，否则结果读不出来。
const EMBED_BATCH: usize = 16;

/// 同时在飞的嵌入批数上限（#513）。
///
/// 嵌入模型的并发闸门（`model_concurrency`，缺省 10）是部署级共享的：本体补齐
/// （ontology_index，上限 4）、类型消解、每次提问的查询嵌入都走它。摄入是用户等着看的
/// 前台活，比后台补齐更有资格用闸门；但它也是能一次带来上千个分块的那一路，不设上限
/// 就把补齐、消解、检索全饿死——ontology_index 记着一次 `join_all` 把闸门占死、五篇文档
/// 卡在 embedding 十五分钟的事故。4 与补齐相同：两路同时跑合计 8，闸门还剩 2 给别人。
const EMBED_JOBS: usize = 4;

pub async fn process_document(state: &AppState, document_id: Uuid) -> anyhow::Result<()> {
    let mut ark = None;
    let outcome = async {
        match prepare_ark_read(state, document_id, &mut ark).await? {
            ArkPreparation::Skip => Ok(()),
            ArkPreparation::Original => run(state, document_id, None, &mut ark).await,
            ArkPreparation::Prepared(prepared) => {
                run(state, document_id, Some(*prepared), &mut ark).await
            }
        }
    }
    .await;
    match outcome {
        Ok(()) => Ok(()),
        // 在等读字的服务读完：文档照旧是 parsing，任务过一会儿再来问
        Err(e) if utopia_core::is_deferred(&e).is_some() => Err(e),
        Err(e) => {
            let doc = utopia_store::documents::get(&state.pool, document_id)
                .await
                .ok();
            // 字要靠模型读、而那种模型没配（0040）：**降级**。文件留着，文档停在 failed
            // 并记下缺哪一种，报一条库级告警；配上模型会自己重新处理。重试没用——
            // 模型不会在两分钟里自己配上，所以挂 Terminal。
            // 转写回来却分不出说话人，跟没配一样对待（决定 5）：换个会标说话人的模型再来
            let waiting = e
                .downcast_ref::<utopia_ingest::NeedsReader>()
                .map(|n| n.reader)
                .or_else(|| {
                    e.downcast_ref::<utopia_ingest::NoSpeakers>()
                        .map(|_| utopia_ingest::Reader::Transcribe)
                });
            if let Some(ark) = &ark {
                let failed = utopia_store::documents::fail_ark_ocr_if_current(
                    &state.pool,
                    document_id,
                    &ark.snapshot,
                    &format!("{e:#}"),
                    waiting.map(|reader| reader.as_str()),
                )
                .await?;
                if !failed {
                    return discard_ark_read(state, document_id, &ark.snapshot.sha256).await;
                }
                if let (Some(reader), Some(doc)) = (waiting, &doc) {
                    crate::alerting::observe_document_needs_reader(
                        state,
                        doc.kb_id,
                        document_id,
                        &doc.filename,
                        reader,
                        &e.to_string(),
                    )
                    .await;
                }
                if let Some(doc) = &doc {
                    state.emit_document(doc.kb_id, document_id);
                }
                return if waiting.is_some()
                    || e.downcast_ref::<utopia_ingest::Unreadable>().is_some()
                {
                    Err(e.context(utopia_core::Terminal))
                } else {
                    Err(e)
                };
            }
            if let Some(reader) = waiting {
                let _ = utopia_store::documents::set_needs_reader(
                    &state.pool,
                    document_id,
                    reader.as_str(),
                    &e.to_string(),
                )
                .await;
                if let Some(doc) = &doc {
                    crate::alerting::observe_document_needs_reader(
                        state,
                        doc.kb_id,
                        document_id,
                        &doc.filename,
                        reader,
                        &e.to_string(),
                    )
                    .await;
                    state.emit_document(doc.kb_id, document_id);
                }
                return Err(e.context(utopia_core::Terminal));
            }
            let _ =
                utopia_store::documents::set_failed(&state.pool, document_id, &e.to_string()).await;
            if let Some(doc) = &doc {
                state.emit_document(doc.kb_id, document_id);
            }
            // 读不了的格式（视频、二进制、空文件）换多少次也一样
            if e.downcast_ref::<utopia_ingest::Unreadable>().is_some() {
                return Err(e.context(utopia_core::Terminal));
            }
            Err(e)
        }
    }
}

struct PreparedRead {
    doc: Document,
    bytes: Vec<u8>,
    parsed: anyhow::Result<utopia_ingest::ParsedDoc>,
}

enum ArkPreparation {
    Original,
    Prepared(Box<PreparedRead>),
    Skip,
}

struct ArkReadRun {
    // 从首次状态变化前一直握到后处理、错误落库结束。
    _guard: tokio::sync::OwnedMutexGuard<()>,
    snapshot: utopia_store::documents::ArkOcrSnapshot,
}

fn wait_for_ark_change() -> anyhow::Error {
    anyhow::anyhow!("the Ark OCR document or configuration changed").context(
        utopia_core::Deferred::new(std::time::Duration::from_secs(1)),
    )
}

// 已确认方舟但尚未得到完整快照时，没有权限落失败状态；下轮重新取得写入权。
fn retry_ark_preparation(error: impl Into<anyhow::Error>) -> anyhow::Error {
    error
        .into()
        .context(utopia_core::Deferred::new(std::time::Duration::from_secs(
            1,
        )))
}

/// 只在已配置方舟时预检；普通正文、文本 PDF、录音不取得方舟快照或锁。
/// 预检结果交给原流程接着用，避免重复解析。其它供应商保持原来的准备顺序。
async fn prepare_ark_read(
    state: &AppState,
    document_id: Uuid,
    ark: &mut Option<ArkReadRun>,
) -> anyhow::Result<ArkPreparation> {
    let Ok(doc) = utopia_store::documents::get(&state.pool, document_id).await else {
        return Ok(ArkPreparation::Original);
    };
    if doc.deleted_at.is_some() {
        return Ok(ArkPreparation::Skip);
    }
    let Ok(kb) = utopia_store::kbs::get(&state.pool, doc.kb_id).await else {
        return Ok(ArkPreparation::Original);
    };
    let Ok(settings) = utopia_store::settings::get(&state.pool, kb.workspace_id).await else {
        return Ok(ArkPreparation::Original);
    };
    if !settings
        .as_ref()
        .is_some_and(|settings| settings.ocr_provider == "ark")
    {
        return Ok(ArkPreparation::Original);
    }
    // 已有检查点证明同一文件曾走方舟 OCR；原文暂时不可读也要保护已经付费的页。
    let stored = utopia_store::documents::reader_task(&state.pool, document_id)
        .await
        .map_err(retry_ark_preparation)?;
    let known_ark = stored.as_ref().is_some_and(|task| {
        task["reader"] == "ocr" && task["provider"] == "ark" && task["sha256"] == doc.sha256
    });
    let doc = if known_ark {
        let Some(current) = acquire_ark_run(state, &doc, kb.workspace_id, ark).await? else {
            return Ok(ArkPreparation::Skip);
        };
        current
    } else {
        doc
    };
    let bytes = state.blob.get(&doc.sha256).await?;
    let filename = doc.filename.clone();
    let (parsed, bytes) =
        tokio::task::spawn_blocking(move || (utopia_ingest::parse(&filename, &bytes), bytes))
            .await?;
    let needs_ocr = parsed
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<utopia_ingest::NeedsReader>())
        .is_some_and(|needs| matches!(needs.reader, utopia_ingest::Reader::Ocr));
    if !needs_ocr {
        if parsed.is_ok()
            || parsed
                .as_ref()
                .err()
                .and_then(|error| error.downcast_ref::<utopia_ingest::NeedsReader>())
                .is_some_and(|needs| matches!(needs.reader, utopia_ingest::Reader::Transcribe))
        {
            // 解析器现在能直接读取原文，或输入属于转写；不把方舟约束推广给它们。
            *ark = None;
        }
        return Ok(ArkPreparation::Prepared(Box::new(PreparedRead {
            doc,
            bytes,
            parsed,
        })));
    }
    let current = if ark.is_none() {
        let Some(current) = acquire_ark_run(state, &doc, kb.workspace_id, ark).await? else {
            return Ok(ArkPreparation::Skip);
        };
        current
    } else {
        doc
    };
    Ok(ArkPreparation::Prepared(Box::new(PreparedRead {
        doc: current,
        bytes,
        parsed,
    })))
}

async fn acquire_ark_run(
    state: &AppState,
    doc: &Document,
    workspace_id: Uuid,
    ark: &mut Option<ArkReadRun>,
) -> anyhow::Result<Option<Document>> {
    let document_id = doc.id;
    let guard = crate::readers::ark_checkpoint::try_lock_document(document_id)?;
    let current = utopia_store::documents::get(&state.pool, document_id)
        .await
        .map_err(retry_ark_preparation)?;
    if current.deleted_at.is_some() {
        return Ok(None);
    }
    if current.sha256 != doc.sha256 {
        return Err(wait_for_ark_change());
    }
    let settings = utopia_store::settings::get(&state.pool, workspace_id)
        .await
        .map_err(retry_ark_preparation)?
        .filter(|settings| settings.ocr_provider == "ark")
        .ok_or_else(wait_for_ark_change)?;
    let task = utopia_store::documents::reader_task(&state.pool, document_id)
        .await
        .map_err(retry_ark_preparation)?;
    if current.status == "ready" && task.is_none() {
        return Ok(None);
    }
    let snapshot = utopia_store::documents::ArkOcrSnapshot {
        sha256: current.sha256.clone(),
        configuration_fingerprint: utopia_store::settings::ocr_configuration_fingerprint(&settings),
        prepared_updated_at: Some(current.updated_at),
        task,
    };
    *ark = Some(ArkReadRun {
        _guard: guard,
        snapshot,
    });
    Ok(Some(current))
}

async fn discard_ark_read(state: &AppState, id: Uuid, sha256: &str) -> anyhow::Result<()> {
    let current = utopia_store::documents::get(&state.pool, id).await?;
    if current.deleted_at.is_some()
        || current.sha256 != sha256
        || (current.status == "ready"
            && utopia_store::documents::reader_task(&state.pool, id)
                .await?
                .is_none())
    {
        Ok(())
    } else {
        Err(wait_for_ark_change())
    }
}

async fn read_ark_pages(
    state: &AppState,
    doc: &Document,
    settings: &LlmSettings,
    bytes: &[u8],
    ark: &mut ArkReadRun,
) -> anyhow::Result<utopia_ingest::Reading> {
    use crate::readers::{ark_checkpoint::Checkpoint, ark_ocr::ArkOcr};
    if utopia_store::settings::ocr_configuration_fingerprint(settings)
        != ark.snapshot.configuration_fingerprint
    {
        return Err(wait_for_ark_change());
    }
    let reader = ArkOcr::new(
        settings.ocr_base_url.as_deref().unwrap_or_default(),
        settings.ocr_api_key.as_deref(),
        settings.ocr_model.as_deref().unwrap_or_default(),
    );
    reader.validate()?;
    let mut checkpoint = match Checkpoint::from_task(
        ark.snapshot.task.as_ref(),
        &doc.sha256,
        &ark.snapshot.configuration_fingerprint,
    )? {
        Some(checkpoint) => checkpoint,
        None => Checkpoint::new(
            &doc.sha256,
            &ark.snapshot.configuration_fingerprint,
            reader.page_count(bytes).await?,
        )?,
    };
    let task = checkpoint.task()?;
    if !utopia_store::documents::compare_and_set_ark_ocr_task(
        &state.pool,
        doc.id,
        &ark.snapshot,
        &task,
    )
    .await?
    {
        return Err(wait_for_ark_change());
    }
    ark.snapshot.task = Some(task);
    ark.snapshot.prepared_updated_at = None;
    state.emit_document(doc.kb_id, doc.id);
    if let Some(page) = checkpoint.next_page() {
        checkpoint.record_page(reader.read_page(bytes, page).await?)?;
        let task = checkpoint.task()?;
        if !utopia_store::documents::compare_and_set_ark_ocr_task(
            &state.pool,
            doc.id,
            &ark.snapshot,
            &task,
        )
        .await?
        {
            return Err(wait_for_ark_change());
        }
        ark.snapshot.task = Some(task);
        if checkpoint.next_page().is_some() {
            return Err(anyhow::anyhow!("the next Ark OCR page is queued").context(
                utopia_core::Deferred::new(std::time::Duration::from_secs(1)),
            ));
        }
    }
    reader.reading(checkpoint.pages())
}

async fn processing_status(
    state: &AppState,
    doc: &Document,
    ark: Option<&ArkReadRun>,
    status: &str,
) -> anyhow::Result<bool> {
    if let Some(ark) = ark {
        Ok(utopia_store::documents::set_ark_ocr_status_if_current(
            &state.pool,
            doc.id,
            &ark.snapshot,
            status,
        )
        .await?)
    } else {
        utopia_store::documents::set_status(&state.pool, doc.id, status).await?;
        Ok(true)
    }
}

async fn run(
    state: &AppState,
    document_id: Uuid,
    prepared: Option<PreparedRead>,
    ark: &mut Option<ArkReadRun>,
) -> anyhow::Result<()> {
    let (doc, prepared) = match prepared {
        Some(prepared) => (prepared.doc, Some((prepared.parsed, prepared.bytes))),
        None => (
            utopia_store::documents::get(&state.pool, document_id).await?,
            None,
        ),
    };
    // 排队之后被删了（#268）：墓碑不重建分块、不回索引；清过的连原文都没了
    if doc.deleted_at.is_some() {
        tracing::info!(document = %document_id, "skipping a deleted document");
        return Ok(());
    }

    // 1. 解析（CPU 密集，放 blocking 线程）
    if ark.is_none() {
        utopia_store::documents::set_status(&state.pool, document_id, "parsing").await?;
        state.emit_document(doc.kb_id, document_id);
    }
    let (parsed, bytes) = match prepared {
        Some(prepared) => prepared,
        None => {
            let bytes = state.blob.get(&doc.sha256).await?;
            let filename = doc.filename.clone();
            tokio::task::spawn_blocking(move || (utopia_ingest::parse(&filename, &bytes), bytes))
                .await?
        }
    };
    let kb_row = utopia_store::kbs::get(&state.pool, doc.kb_id).await?;
    let settings = utopia_store::settings::get(&state.pool, kb_row.workspace_id).await?;
    let pushed_statements =
        source_kind(state, doc.source_id).await?.as_deref() == Some("statements");

    // 2. 分块 + 入库
    let (text, pieces) = match parsed {
        Ok(parsed) => {
            // 解析出来的正文可能夹着 NUL（PDF 文本层常见），入库之前剥掉（#611）——与记忆
            // 那条路共用 `utopia_core::without_nul`（#665）。剥必须在算长度、分块之前：之后的
            // text_len、分块偏移、全文索引、嵌入读的都是这一份，彼此才对得上
            let text = utopia_core::without_nul(&parsed.text).into_owned();
            // 推送来的陈述（0054）：载荷就是契约，整份是一块。分块预算是给模型的注意力
            // 定的，这条路没有模型读；切开了契约就解析不回来
            let pieces = if pushed_statements {
                vec![utopia_ingest::ChunkPiece {
                    seq: 0,
                    char_start: 0,
                    char_end: text.chars().count() as i32,
                    heading: None,
                    provenance: utopia_ingest::Provenance::stated(),
                    text: text.clone(),
                }]
            } else {
                utopia_ingest::chunk_with_budget(&text, state.chunk_tokens)
            };
            (text, pieces)
        }
        // 没有文本层的扫描件、图片：工作区配了版面识别服务就交给它读（0040 第二刀），
        // 按页分段切块，每块记着页码和框。录音交给会标说话人的转写模型（第三刀），
        // 每块记着起止时刻和说话人。没配的照旧往上抛，由外面降级
        Err(e) => {
            let Some(needs) = e.downcast_ref::<utopia_ingest::NeedsReader>() else {
                return Err(e);
            };
            let reading = match (needs.reader, settings.as_ref()) {
                (utopia_ingest::Reader::Ocr, Some(s)) => {
                    if let Some(ark) = ark.as_mut() {
                        if !s.ocr_ready() {
                            return Err(e);
                        }
                        read_ark_pages(state, &doc, s, &bytes, ark).await?
                    } else if s.ocr_provider == "ark" {
                        // 设置在普通准备期间换成了方舟，下轮先取得它自己的锁和快照。
                        return Err(wait_for_ark_change());
                    } else {
                        match crate::readers::Ocr::from_settings(s) {
                            Some(ocr) => ocr.read(state, &doc, bytes).await?,
                            None => return Err(e),
                        }
                    }
                }
                (utopia_ingest::Reader::Transcribe, Some(s)) => {
                    match crate::readers::Transcriber::from_settings(s) {
                        Some(t) => t.read(&doc, bytes).await?,
                        None => return Err(e),
                    }
                }
                (_, None) => return Err(e),
            };
            if reading.text.trim().is_empty() {
                return Err(utopia_ingest::Unreadable(
                    "The reader found no text in this file".into(),
                )
                .into());
            }
            let pieces = reading.chunk(state.chunk_tokens);
            (reading.text, pieces)
        }
    };
    let text_len = text.chars().count() as i32;
    let chunk_pairs = if let Some(ark) = ark.as_ref() {
        utopia_store::documents::replace_ark_ocr_chunks_if_current(
            &state.pool,
            doc.kb_id,
            document_id,
            &pieces,
            &ark.snapshot,
        )
        .await?
    } else {
        utopia_store::documents::replace_chunks_if_current(
            &state.pool,
            doc.kb_id,
            document_id,
            &pieces,
            &doc.sha256,
        )
        .await?
    };
    let Some(chunk_pairs) = chunk_pairs else {
        // 读取期间源文档可能已更新或删除，丢弃过期结果，
        // 不再改写新任务的索引、状态和抽取队列。
        tracing::info!(%document_id, "discarding a superseded document read");
        return if ark.is_some() {
            discard_ark_read(state, document_id, &doc.sha256).await
        } else {
            Ok(())
        };
    };
    let chunk_count = chunk_pairs.len() as i32;

    // 3. 全文索引（Tantivy）
    if !processing_status(state, &doc, ark.as_ref(), "indexing").await? {
        return discard_ark_read(state, document_id, &doc.sha256).await;
    }
    state.emit_document(doc.kb_id, document_id);
    let search = state.search.clone();
    let kb = doc.kb_id.to_string();
    let did = document_id.to_string();
    tokio::task::spawn_blocking(move || search.reindex_document(&kb, &did, &chunk_pairs)).await??;

    // 4. embedding（工作区配置了 embedding 模型才做；没配也算 ready，先享受 BM25 搜索）
    if let Some((settings, client)) = embedder(settings.as_ref()) {
        if !processing_status(state, &doc, ark.as_ref(), "embedding").await? {
            return discard_ark_read(state, document_id, &doc.sha256).await;
        }
        state.emit_document(doc.kb_id, document_id);
        embed_pending(state, settings, &client, document_id).await?;
    }

    if let Some(ark) = ark.as_ref() {
        if !utopia_store::documents::set_ark_ocr_ready_if_current(
            &state.pool,
            document_id,
            &ark.snapshot,
            text_len,
            chunk_count,
        )
        .await?
        {
            return discard_ark_read(state, document_id, &doc.sha256).await;
        }
    } else {
        utopia_store::documents::set_ready(&state.pool, document_id, text_len, chunk_count).await?;
    }

    if let Some(ark) = ark.as_ref() {
        let graph_status = if !source_extracts(state, doc.source_id).await? {
            Some("skipped")
        } else if pushed_statements || settings.as_ref().is_some_and(|s| s.chat_ready()) {
            Some("queued")
        } else {
            None
        };
        if !utopia_store::documents::finish_ark_ocr_if_current(
            &state.pool,
            document_id,
            &ark.snapshot,
            text_len,
            chunk_count,
            graph_status,
        )
        .await?
        {
            return discard_ark_read(state, document_id, &doc.sha256).await;
        }
        state.emit_document(doc.kb_id, document_id);
        tracing::info!(%document_id, chunks = chunk_count, "文档处理完成");
        return Ok(());
    }

    // 来源说了不抽取的，到这里为止：可搜、可问，不进图。
    //
    // schema 文档就是这一类（0035 决定 7）——它是给问数检索表结构的语料，进抽取的
    // 结果是抽取器把每个列名当成一个实体（宽表语料上四十个概念实体里二十八个是
    // 列名，#553）。状态记成 `skipped` 而不是留在 `none`：`none` 在 Library 里读作
    // 「还没排到」，而它永远不会排到
    if !source_extracts(state, doc.source_id).await? {
        utopia_store::documents::set_graph_status(&state.pool, document_id, "skipped").await?;
        state.emit_document(doc.kb_id, document_id);
        tracing::info!(%document_id, chunks = chunk_count, "文档处理完成，来源不抽取");
        return Ok(());
    }

    // 两段式：索引就绪后，若配置了对话模型则排队图谱抽取（不阻塞可搜可问）。
    // 推送来的陈述不问模型（0054），没配也排
    if pushed_statements || settings.as_ref().is_some_and(|s| s.chat_ready()) {
        utopia_store::documents::set_graph_status(&state.pool, document_id, "queued").await?;
        utopia_store::jobs::enqueue(
            &state.pool,
            "extract_document",
            serde_json::json!({ "document_id": document_id }),
        )
        .await?;
    }
    state.emit_document(doc.kb_id, document_id);

    tracing::info!(%document_id, chunks = chunk_count, "文档处理完成");
    Ok(())
}

/// 记忆摄入（episodes 快速路径的后半程）：新 episode chunk 补 embedding、
/// 重建全文索引、触发增量抽取（extracted_at 为空的新 chunk 才会被抽）。
/// 免解析免分块——episode 落库时已是 chunk。
///
/// `proposer`：说这句话的人，以及经 MCP 时那个 agent。一路传到抽取，落在
/// `pending_facts.proposed_by` / `proposed_token`（0015、0026）
pub async fn memory_ingest(
    state: &AppState,
    document_id: Uuid,
    proposer: Proposer,
) -> anyhow::Result<()> {
    let doc = utopia_store::documents::get(&state.pool, document_id).await?;
    if doc.deleted_at.is_some() {
        tracing::info!(document = %document_id, "skipping a deleted document");
        return Ok(());
    }
    let kb_row = utopia_store::kbs::get(&state.pool, doc.kb_id).await?;
    let settings = utopia_store::settings::get(&state.pool, kb_row.workspace_id).await?;

    if let Some((settings, client)) = embedder(settings.as_ref()) {
        embed_pending(state, settings, &client, document_id).await?;
    }

    let chunks = utopia_store::documents::chunks_full(&state.pool, document_id).await?;
    let pairs: Vec<(String, String)> = chunks
        .iter()
        .map(|c| (c.id.to_string(), c.text.clone()))
        .collect();
    let search = state.search.clone();
    let kb = doc.kb_id.to_string();
    let did = document_id.to_string();
    tokio::task::spawn_blocking(move || search.reindex_document(&kb, &did, &pairs)).await??;

    if settings.as_ref().is_some_and(|s| s.chat_ready()) {
        utopia_store::documents::set_graph_status(&state.pool, document_id, "queued").await?;
        utopia_store::jobs::enqueue(
            &state.pool,
            "extract_document",
            serde_json::json!({
                "document_id": document_id,
                "proposed_by": proposer.user_id,
                "proposed_token": proposer.token_id,
            }),
        )
        .await?;
    }
    state.emit_document(doc.kb_id, document_id);
    Ok(())
}

/// 工作区配了嵌入模型才有客户端；设置与客户端一起交出去，闸门许可证要按设置取。
fn embedder(settings: Option<&LlmSettings>) -> Option<(&LlmSettings, LlmClient)> {
    let s = settings?;
    llm_util::embed_client(s).map(|c| (s, c))
}

/// 把这篇文档还没有向量的分块全部嵌完，返回嵌了多少条。文档摄入与记忆摄入共用——
/// 从前两处各抄一份同样的循环，且都是一批等完再发下一批。
///
/// 批与批之间互不依赖（向量按位置配对，只在一批之内），`EMBED_JOBS` 个批次同时在飞，
/// 每个批次握一张闸门许可证只到自己写完。
///
/// **数量对不上就整批放弃。** 配对是按位置的，少一条就全体错位，把一条的向量写到另一条
/// 身上；这种错落库之后再也看不出来——正文还在，向量是别人的。
///
/// **任一批失败整篇失败。** 串行时错误自然一路 unwind；并发下要明确地把它传出去，让调用方
/// 把文档标成 failed，而不是留它停在 embedding。已经写进去的向量留着，重跑只补缺的
/// （`chunks_pending_embedding` 只挑 embedding 为空的）。
async fn embed_pending(
    state: &AppState,
    settings: &LlmSettings,
    client: &LlmClient,
    document_id: Uuid,
) -> anyhow::Result<usize> {
    let pending =
        utopia_store::documents::chunks_pending_embedding(&state.pool, document_id).await?;
    // 批次先切成拥有的数据：借来的切片捕进并发驱动的 future 过不了 Send 边界
    //（ontology_index 里同一句话）
    let batches: Vec<Vec<(Uuid, String)>> =
        pending.chunks(EMBED_BATCH).map(<[_]>::to_vec).collect();
    let mut batches = stream::iter(batches)
        .map(|batch| async move {
            let texts: Vec<String> = batch.iter().map(|(_, t)| t.clone()).collect();
            let _permit = llm_util::acquire_embed(state, settings).await;
            let embeddings = client.embed(&texts).await?;
            if embeddings.len() != batch.len() {
                anyhow::bail!(
                    "嵌入返回 {} 条，送去的是 {} 条",
                    embeddings.len(),
                    batch.len()
                );
            }
            let items: Vec<(Uuid, Vec<f32>)> =
                batch.iter().map(|(id, _)| *id).zip(embeddings).collect();
            utopia_store::documents::set_embeddings(&state.pool, &items).await?;
            Ok::<usize, anyhow::Error>(items.len())
        })
        .buffer_unordered(EMBED_JOBS);
    let mut done = 0;
    while let Some(written) = batches.next().await {
        done += written?;
    }
    Ok(done)
}

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod pipeline_tests;
