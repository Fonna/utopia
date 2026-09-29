//! 方舟 OCR 已完成的页。进程锁只管活着的运行，数据库检查点只管已落库的成果。
//!
//! 页失败交给现有任务队列重试；这里没有另一套尝试次数或跨进程时间租约。

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use utopia_core::{Deferred, Terminal};
use uuid::Uuid;

use super::ark_ocr::{MAX_PAGES, MAX_PAGE_TEXT_BYTES, MAX_TEXT_BYTES};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Checkpoint {
    reader: String,
    provider: String,
    schema: u32,
    sha256: String,
    configuration_fingerprint: String,
    // 手动 pending 只换这个 token，不清 pages；完整任务 CAS 让旧运行失去写入权。
    run_token: Uuid,
    page_count: u32,
    pages: Vec<String>,
}

impl std::fmt::Debug for Checkpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 检查点可能含整份私有文档；日志只需要页数，不需要页面正文。
        f.debug_struct("Checkpoint")
            .field("page_count", &self.page_count)
            .field("completed_pages", &self.pages.len())
            .finish_non_exhaustive()
    }
}

impl Checkpoint {
    pub(crate) fn new(
        sha256: &str,
        configuration_fingerprint: &str,
        page_count: u32,
    ) -> anyhow::Result<Self> {
        let checkpoint = Self {
            reader: "ocr".into(),
            provider: "ark".into(),
            schema: 1,
            sha256: sha256.into(),
            configuration_fingerprint: configuration_fingerprint.into(),
            run_token: Uuid::new_v4(),
            page_count,
            pages: Vec::new(),
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// 只接用同一文件、同一有效 OCR 配置。匹配身份但损坏的检查点不能悄悄从头收费。
    pub(crate) fn from_task(
        task: Option<&Value>,
        sha256: &str,
        configuration_fingerprint: &str,
    ) -> anyhow::Result<Option<Self>> {
        let Some(task) = task else {
            return Ok(None);
        };
        if task["reader"] != "ocr"
            || task["provider"] != "ark"
            || task["sha256"] != sha256
            || task["configuration_fingerprint"] != configuration_fingerprint
        {
            return Ok(None);
        }
        let checkpoint: Self = serde_json::from_value(task.clone())
            .map_err(|_| anyhow!("The stored Ark OCR checkpoint is invalid").context(Terminal))?;
        checkpoint.validate()?;
        Ok(Some(checkpoint))
    }

    pub(crate) fn task(&self) -> anyhow::Result<Value> {
        Ok(serde_json::to_value(self)?)
    }

    pub(crate) fn pages(&self) -> &[String] {
        &self.pages
    }

    /// 下一个缺页按真实文件顺序编号；空白页也算已读，不能让后面的页码前移。
    pub(crate) fn next_page(&self) -> Option<u32> {
        (self.pages.len() < self.page_count as usize).then_some(self.pages.len() as u32 + 1)
    }

    pub(crate) fn record_page(&mut self, text: String) -> anyhow::Result<()> {
        if self.next_page().is_none() {
            return Err(
                anyhow!("The Ark OCR checkpoint already contains every page").context(Terminal),
            );
        }
        if text.len() > MAX_PAGE_TEXT_BYTES
            || self.pages.iter().map(String::len).sum::<usize>() + text.len() > MAX_TEXT_BYTES
        {
            return Err(anyhow!("The Ark OCR document exceeds its text limit").context(Terminal));
        }
        self.pages.push(text);
        Ok(())
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.reader != "ocr"
            || self.provider != "ark"
            || self.schema != 1
            || self.sha256.is_empty()
            || self.configuration_fingerprint.is_empty()
            || !(1..=MAX_PAGES).contains(&self.page_count)
            || self.pages.len() > self.page_count as usize
            || self
                .pages
                .iter()
                .any(|text| text.len() > MAX_PAGE_TEXT_BYTES)
            || self.pages.iter().map(String::len).sum::<usize>() > MAX_TEXT_BYTES
        {
            return Err(anyhow!("The stored Ark OCR checkpoint is invalid").context(Terminal));
        }
        Ok(())
    }
}

#[derive(Default)]
struct DocumentLocks(Mutex<HashMap<Uuid, Weak<AsyncMutex<()>>>>);

impl DocumentLocks {
    fn try_lock(&self, document_id: Uuid) -> anyhow::Result<OwnedMutexGuard<()>> {
        let lock = {
            let mut table = self.0.lock().expect("Ark OCR document lock table poisoned");
            table.retain(|_, lock| lock.strong_count() > 0);
            match table.get(&document_id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(AsyncMutex::new(()));
                    table.insert(document_id, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.try_lock_owned().map_err(|_| {
            anyhow!("another run is reading this Ark OCR document")
                .context(Deferred::new(Duration::from_secs(1)))
        })
    }
}

static PER_DOCUMENT: LazyLock<DocumentLocks> = LazyLock::new(Default::default);

/// guard 必须由摄入入口握到后处理和失败落库结束；取消或进程退出不留下时间租约。
pub(crate) fn try_lock_document(document_id: Uuid) -> anyhow::Result<OwnedMutexGuard<()>> {
    PER_DOCUMENT.try_lock(document_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blank_completed_page_does_not_change_the_next_real_page_number() {
        let mut checkpoint = Checkpoint::new("file", "configuration", 3).unwrap();
        checkpoint.record_page("page one".into()).unwrap();
        checkpoint.record_page(String::new()).unwrap();
        let restored =
            Checkpoint::from_task(Some(&checkpoint.task().unwrap()), "file", "configuration")
                .unwrap()
                .unwrap();
        assert_eq!(restored.next_page(), Some(3));
        assert_eq!(restored.pages(), ["page one", ""]);
    }

    #[test]
    fn only_the_same_file_and_ocr_configuration_resume_saved_pages() {
        let mut checkpoint = Checkpoint::new("file", "configuration", 2).unwrap();
        checkpoint.record_page("already paid for".into()).unwrap();
        let task = checkpoint.task().unwrap();
        assert!(
            Checkpoint::from_task(Some(&task), "other file", "configuration")
                .unwrap()
                .is_none()
        );
        assert!(
            Checkpoint::from_task(Some(&task), "file", "other configuration")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            Checkpoint::from_task(Some(&task), "file", "configuration")
                .unwrap()
                .unwrap()
                .next_page(),
            Some(2)
        );
    }

    #[test]
    fn a_damaged_matching_checkpoint_fails_without_restarting_paid_pages() {
        let checkpoint = Checkpoint::new("file", "configuration", 1).unwrap();
        let mut task = checkpoint.task().unwrap();
        task["pages"] = serde_json::json!(["page one", "unexpected second page"]);
        let error = Checkpoint::from_task(Some(&task), "file", "configuration").unwrap_err();
        assert!(utopia_core::is_terminal(&error));
        assert!(!format!("{error:#}").contains("page one"));
        task["pages"] = serde_json::json!(["x".repeat(MAX_PAGE_TEXT_BYTES + 1)]);
        assert!(Checkpoint::from_task(Some(&task), "file", "configuration").is_err());
    }

    #[test]
    fn a_text_limit_failure_keeps_the_pages_already_read() {
        let mut checkpoint = Checkpoint::new("file", "configuration", 17).unwrap();
        for _ in 0..16 {
            checkpoint
                .record_page("x".repeat(MAX_PAGE_TEXT_BYTES))
                .unwrap();
        }
        assert!(checkpoint.record_page("too much".into()).is_err());
        assert_eq!(checkpoint.next_page(), Some(17));
        assert_eq!(checkpoint.pages().len(), 16);
        assert!(!format!("{checkpoint:?}").contains("xxxxxxxx"));
    }

    #[tokio::test]
    async fn duplicate_jobs_defer_and_cancellation_releases_the_document_immediately() {
        let locks = Arc::new(DocumentLocks::default());
        let document = Uuid::new_v4();
        let held = locks.try_lock(document).unwrap();
        assert!(utopia_core::is_deferred(&locks.try_lock(document).unwrap_err()).is_some());
        assert!(locks.try_lock(Uuid::new_v4()).is_ok());
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _held = held;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(locks.try_lock(document).is_ok());
    }

    #[test]
    fn finished_documents_do_not_accumulate_live_lock_entries() {
        let locks = DocumentLocks::default();
        for _ in 0..1000 {
            drop(locks.try_lock(Uuid::new_v4()).unwrap());
        }
        assert_eq!(locks.0.lock().unwrap().len(), 1);
        assert!(locks
            .0
            .lock()
            .unwrap()
            .values()
            .all(|lock| lock.strong_count() == 0));
    }
}
