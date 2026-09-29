import type { LlmSettingsView } from "../api";

export type OcrProvider = "mineru" | "ark";

export interface OcrDraft {
  provider: OcrProvider;
  base_url: string;
  api_key: string;
  backend: string;
  model: string;
}

export interface OcrForm {
  workspaceId: string;
  savedIdentity: string | null;
  changed: boolean;
  editRevision: number;
  current: OcrDraft;
  drafts: Partial<Record<OcrProvider, OcrDraft>>;
}

export function isCurrentOcrOperation(
  operation: { workspaceId: string; generation: number } | undefined,
  workspaceId: string, generation: number,
): boolean {
  return operation?.workspaceId === workspaceId && operation.generation === generation;
}

// reset 只清观察者、不取消请求；在途时 reset 会让 Save 提前恢复，导致保存乱序。
export function resetCompletedOcrSave(mutation: { isPending: boolean; reset: () => void }): void {
  if (!mutation.isPending) mutation.reset();
}

function defaults(provider: OcrProvider): OcrDraft {
  return {
    provider,
    base_url: provider === "ark" ? "https://ark.cn-beijing.volces.com/api/plan/v3" : "",
    api_key: "",
    backend: "",
    model: provider === "ark" ? "doubao-seed-2.1-pro" : "",
  };
}

export function ocrFormFromSettings(workspaceId: string, saved?: LlmSettingsView): OcrForm {
  const provider = saved?.ocr_provider === "ark" ? "ark" : "mineru";
  return {
    workspaceId,
    savedIdentity: saved ? ocrIdentity(saved) : null,
    changed: false,
    editRevision: 0,
    current: {
      provider,
      base_url: saved?.ocr_base_url ?? "",
      api_key: "",
      backend: saved?.ocr_backend ?? "",
      model: saved?.ocr_model ?? "",
    },
    drafts: {},
  };
}

function ocrIdentity(saved: LlmSettingsView): string {
  return JSON.stringify([
    saved.ocr_provider ?? "mineru",
    saved.ocr_base_url ?? "",
    saved.ocr_backend ?? "",
    saved.ocr_model ?? "",
    !!saved.has_ocr_key,
  ]);
}

// 其他卡片保存也会刷新设置；只在 OCR 本身改变时清草稿，避免丢掉未保存的编辑。
export function syncOcrForm(form: OcrForm, workspaceId: string, saved?: LlmSettingsView): OcrForm {
  if (form.workspaceId !== workspaceId) return ocrFormFromSettings(workspaceId, saved);
  if (!saved || form.savedIdentity === ocrIdentity(saved)) return form;
  if (form.changed && form.savedIdentity !== null) {
    return { ...form, savedIdentity: ocrIdentity(saved) };
  }
  return { ...ocrFormFromSettings(workspaceId, saved), editRevision: form.editRevision + 1 };
}

export function editOcrDraft(form: OcrForm, field: Exclude<keyof OcrDraft, "provider">, value: string): OcrForm {
  return { ...form, changed: true, editRevision: form.editRevision + 1, current: { ...form.current, [field]: value } };
}

export function switchOcrProvider(form: OcrForm, provider: OcrProvider): OcrForm {
  if (provider === form.current.provider) return form;
  return {
    ...form,
    changed: true,
    editRevision: form.editRevision + 1,
    drafts: { ...form.drafts, [form.current.provider]: form.current },
    current: form.drafts[provider] ?? defaults(provider),
  };
}

function endpoint(base: string): string {
  return base.trim().replace(/\/+$/, "");
}

export function hasSavedOcrKey(draft: OcrDraft, saved?: LlmSettingsView): boolean {
  return !!saved?.has_ocr_key &&
    draft.provider === (saved.ocr_provider ?? "mineru") &&
    endpoint(draft.base_url) === endpoint(saved.ocr_base_url ?? "");
}

// 请求完成时可能已换工作区或继续编辑；只清理确实保存了的那一份密钥。
export function completeOcrSave(form: OcrForm, workspaceId: string, submitted: OcrDraft, editRevision: number): OcrForm {
  if (form.workspaceId !== workspaceId || form.editRevision !== editRevision ||
      JSON.stringify(form.current) !== JSON.stringify(submitted)) return form;
  return { ...form, changed: false, editRevision: form.editRevision + 1, current: { ...form.current, api_key: "" }, drafts: {} };
}
