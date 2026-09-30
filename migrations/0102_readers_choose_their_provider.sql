-- 读扫描件的服务按供应商选协议（0065）：MinerU 是交任务再问，方舟是把每页送给视觉模型。
-- 已有的配置默认还是 MinerU，地址、后端、密钥一列不动。`ocr_model` 只有方舟用：视觉模型的名字
ALTER TABLE llm_settings
    ADD COLUMN ocr_provider TEXT NOT NULL DEFAULT 'mineru'
        CHECK (ocr_provider IN ('mineru', 'ark')),
    ADD COLUMN ocr_model TEXT;
