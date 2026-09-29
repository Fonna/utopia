-- 供应商决定读取协议；旧配置仍走原协议。预留值的可用性由服务端实现决定（0065）。
ALTER TABLE llm_settings
    ADD COLUMN ocr_provider TEXT NOT NULL DEFAULT 'mineru'
        CHECK (ocr_provider IN ('mineru', 'ark')),
    ADD COLUMN ocr_model TEXT,
    ADD COLUMN transcribe_provider TEXT NOT NULL DEFAULT 'openai'
        CHECK (transcribe_provider IN ('openai', 'ark'));
