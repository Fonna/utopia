import { describe, expect, it } from "vitest";
import { MutationObserver, QueryClient } from "@tanstack/react-query";
import type { LlmSettingsView } from "../api";
import {
  completeOcrSave,
  editOcrDraft,
  hasSavedOcrKey,
  ocrFormFromSettings,
  switchOcrProvider,
  syncOcrForm,
  resetCompletedOcrSave,
  isCurrentOcrOperation,
} from "./ocrSettingsForm";

const mineru: LlmSettingsView = {
  ocr_provider: "mineru",
  ocr_base_url: "https://private.example/mineru",
  ocr_backend: "custom-backend",
  has_ocr_key: true,
};

const ark: LlmSettingsView = {
  ocr_provider: "ark",
  ocr_base_url: "https://proxy.example/plan",
  ocr_model: "custom-vision-model",
  has_ocr_key: true,
};

describe("OCR provider drafts", () => {
  it("keeps an in-flight save observed while editing and only resets after it completes", async () => {
    const client = new QueryClient();
    let finish!: () => void;
    const response = new Promise<void>((resolve) => { finish = resolve; });
    let requests = 0;
    const observer = new MutationObserver(client, {
      mutationFn: async () => { requests++; await response; },
    });
    const unsubscribe = observer.subscribe(() => {});
    const saving = observer.mutate();
    await Promise.resolve();
    const form = editOcrDraft(ocrFormFromSettings("workspace", ark), "model", "later-model");
    resetCompletedOcrSave({ ...observer.getCurrentResult(), reset: () => observer.reset() });
    // Settings disables Save from this observed state, even after editing another provider.
    expect(observer.getCurrentResult().isPending).toBe(true);
    expect(switchOcrProvider(form, "mineru").changed).toBe(true);
    finish();
    await saving;
    expect(requests).toBe(1);
    resetCompletedOcrSave({ ...observer.getCurrentResult(), reset: () => observer.reset() });
    expect(observer.getCurrentResult().isIdle).toBe(true);
    unsubscribe();
    client.clear();
  });

  it("hides old save and test outcomes after A to B to A", () => {
    const submitted = { workspaceId: "A", generation: 0 };
    const editedA = editOcrDraft(ocrFormFromSettings("A", ark), "api_key", "key-A");
    const b = syncOcrForm(editedA, "B", mineru);
    const returned = syncOcrForm(b, "A", ark);
    expect(returned.current.api_key).toBe("");
    expect(isCurrentOcrOperation(submitted, "B", 1)).toBe(false);
    expect(isCurrentOcrOperation(submitted, "A", 2)).toBe(false);
    expect(isCurrentOcrOperation({ workspaceId: "A", generation: 2 }, "A", 2)).toBe(true);
  });

  it("restores the actual saved MinerU configuration after switching back", () => {
    const initial = ocrFormFromSettings("workspace", mineru);
    const changed = switchOcrProvider(initial, "ark");
    expect(changed.current.model).toBe("doubao-seed-2.1-pro");
    const restored = switchOcrProvider(changed, "mineru");
    expect(restored.current).toEqual(initial.current);
    expect(hasSavedOcrKey(restored.current, mineru)).toBe(true);
  });

  it("restores a custom Ark endpoint instead of pairing its key with defaults", () => {
    const initial = ocrFormFromSettings("workspace", ark);
    const restored = switchOcrProvider(switchOcrProvider(initial, "mineru"), "ark");
    expect(restored.current.base_url).toBe(ark.ocr_base_url);
    expect(restored.current.model).toBe(ark.ocr_model);
    expect(hasSavedOcrKey(restored.current, ark)).toBe(true);
  });

  it("keeps unsaved keys and fields only in the draft of their own provider", () => {
    const initial = ocrFormFromSettings("workspace", mineru);
    initial.current = { ...initial.current, api_key: "new-mineru-key" };
    const changed = switchOcrProvider(initial, "ark");
    expect(changed.current.api_key).toBe("");
    changed.current = { ...changed.current, api_key: "new-ark-key", model: "edited" };
    const restored = switchOcrProvider(changed, "mineru");
    expect(restored.current.api_key).toBe("new-mineru-key");
    const again = switchOcrProvider(restored, "ark");
    expect(again.current.api_key).toBe("new-ark-key");
    expect(again.current.model).toBe("edited");
  });

  it("recognizes a saved key only for the same provider and endpoint", () => {
    const draft = ocrFormFromSettings("workspace", ark).current;
    expect(hasSavedOcrKey({ ...draft, base_url: draft.base_url + "/" }, ark)).toBe(true);
    expect(hasSavedOcrKey({ ...draft, base_url: "https://other.example/plan" }, ark)).toBe(false);
    expect(hasSavedOcrKey({ ...draft, provider: "mineru" }, ark)).toBe(false);
  });

  it("preserves edits when chat, embedding or transcription settings refresh", () => {
    const initial = ocrFormFromSettings("workspace", ark);
    initial.current = { ...initial.current, api_key: "pending-key", model: "pending-model" };
    const refreshed = syncOcrForm(initial, "workspace", {
      ...ark,
      chat_model: "changed-chat",
      embed_model: "changed-embedding",
      transcribe_model: "changed-transcription",
    });
    expect(refreshed).toBe(initial);
    expect(refreshed.current.api_key).toBe("pending-key");
  });

  it("discards private drafts immediately when the workspace changes", () => {
    const initial = ocrFormFromSettings("workspace", ark);
    initial.current.api_key = "private-key";
    const changed = switchOcrProvider(initial, "mineru");
    const next = syncOcrForm(changed, "other-workspace");
    expect(next.current.api_key).toBe("");
    expect(next.current.base_url).toBe("");
    expect(next.drafts).toEqual({});
  });

  it("clears saved input keys and stale drafts without erasing later edits", () => {
    const initial = ocrFormFromSettings("workspace", ark);
    initial.current = { ...initial.current, api_key: "saved-key" };
    const submitted = { ...initial.current };
    const complete = completeOcrSave(initial, "workspace", submitted, initial.editRevision);
    expect(complete.current.api_key).toBe("");
    expect(complete.drafts).toEqual({});
    const edited = editOcrDraft(initial, "model", "later-edit");
    expect(completeOcrSave(edited, "workspace", submitted, initial.editRevision)).toBe(edited);
    expect(completeOcrSave(initial, "other-workspace", submitted, initial.editRevision)).toBe(initial);
  });

  it("keeps later edits through the successful save and its server refresh", () => {
    const initial = editOcrDraft(ocrFormFromSettings("workspace", ark), "model", "submitted-model");
    const submitted = { ...initial.current };
    const later = editOcrDraft(initial, "model", "later-edit");
    const complete = completeOcrSave(later, "workspace", submitted, initial.editRevision);
    const refreshed = syncOcrForm(complete, "workspace", { ...ark, ocr_model: "submitted-model" });
    expect(refreshed.current.model).toBe("later-edit");
    expect(refreshed.changed).toBe(true);
    const switched = switchOcrProvider(initial, "mineru");
    const switchedAfterRefresh = syncOcrForm(
      completeOcrSave(switched, "workspace", submitted, initial.editRevision),
      "workspace",
      { ...ark, ocr_model: "submitted-model" },
    );
    expect(switchedAfterRefresh.current.provider).toBe("mineru");
    expect(switchedAfterRefresh.changed).toBe(true);
  });

  it("keeps another provider's late key when the current draft returns to its submitted value", () => {
    const initial = editOcrDraft(ocrFormFromSettings("workspace", ark), "model", "submitted-model");
    const submitted = { ...initial.current };
    const other = editOcrDraft(switchOcrProvider(initial, "mineru"), "api_key", "later-mineru-key");
    const returned = switchOcrProvider(other, "ark");
    expect(returned.current).toEqual(submitted);
    const afterSave = completeOcrSave(returned, "workspace", submitted, initial.editRevision);
    const refreshed = syncOcrForm(afterSave, "workspace", { ...ark, ocr_model: "submitted-model" });
    expect(switchOcrProvider(refreshed, "mineru").current.api_key).toBe("later-mineru-key");
    expect(refreshed.changed).toBe(true);
  });
});
