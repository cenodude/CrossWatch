/* assets/helpers/clipboard.js */
/* CrossWatch - Clipboard copy that also works on http and in Safari */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  if (typeof window.cwCopyText === "function") return;

  function copyWithSelection(text) {
    const active = document.activeElement;
    const host = active?.closest?.('[role="dialog"], dialog') || document.querySelector("dialog[open]") || document.body;
    const area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.tabIndex = -1;
    area.style.cssText = "position:fixed;top:0;left:0;width:1px;height:1px;padding:0;border:0;opacity:0;pointer-events:none;font-size:16px";
    host.appendChild(area);
    let ok = false;
    try {
      area.focus({ preventScroll: true });
      area.select();
      area.setSelectionRange(0, text.length);
      ok = document.execCommand("copy") === true;
    } catch {
      ok = false;
    }
    area.remove();
    try { active?.focus?.({ preventScroll: true }); } catch {}
    return ok;
  }

  function selectField(field) {
    if (!field || typeof field.select !== "function") return;
    try {
      field.focus({ preventScroll: true });
      field.select();
      field.setSelectionRange?.(0, String(field.value || "").length);
    } catch {}
  }

  window.cwCopyText = (value) => {
    const text = String(value ?? "");
    if (!text) return Promise.resolve(false);
    if (copyWithSelection(text)) return Promise.resolve(true);
    if (!window.isSecureContext || !navigator.clipboard?.writeText) return Promise.resolve(false);
    return navigator.clipboard.writeText(text).then(() => true, () => false);
  };
  window.cwCopyTextFrom = (source) => {
    const pending = Promise.resolve(source).then((value) => String(value ?? ""));
    if (window.isSecureContext && navigator.clipboard?.write && typeof window.ClipboardItem === "function") {
      try {
        const item = new window.ClipboardItem({ "text/plain": pending.then((text) => new Blob([text], { type: "text/plain" })) });
        return navigator.clipboard.write([item]).then(() => true, () => pending.then((text) => window.cwCopyText(text)));
      } catch {}
    }
    return pending.then((text) => window.cwCopyText(text));
  };
  window.cwSelectCopyField = selectField;

  let justFocused = null;
  let downX = null;
  document.addEventListener("mousedown", (event) => {
    downX = event.target?.closest?.("input[data-copy-field]") ? event.clientX : null;
  }, true);
  document.addEventListener("focusin", (event) => {
    if (!event.target?.matches?.("input[data-copy-field]")) return;
    justFocused = event.target;
    selectField(event.target);
  }, true);
  document.addEventListener("mouseup", (event) => {
    const field = event.target?.closest?.("input[data-copy-field]");
    if (field && field === justFocused) event.preventDefault();
    justFocused = null;
  }, true);
  document.addEventListener("click", (event) => {
    const field = event.target?.closest?.("input[data-copy-field]");
    if (field && (downX === null || Math.abs(event.clientX - downX) < 4)) {
      selectField(field);
      setTimeout(() => selectField(field), 0);
    }
    downX = null;
  }, true);
})();
