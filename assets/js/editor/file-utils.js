/* assets/js/editor/file-utils.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  async function fetchJSON(url, opts) {
    if (window.cwIsAuthSetupPending?.() === true) throw new Error("auth setup pending");
    const res = await fetch(url, Object.assign({ cache: "no-store" }, opts || {}));
    if (!res.ok) {
      let detail = "";
      try {
        const data = await res.json();
        detail = data && (data.detail || data.error || data.message) ? String(data.detail || data.error || data.message) : "";
      } catch (_) {}
      throw new Error(detail || `Request failed: ${res.status}`);
    }
    return await res.json();
  }

  async function fetchBlob(url) {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`Download failed: ${res.status}`);
    return await res.blob();
  }

  function saveBlob(blob, filename) {
    const href = URL.createObjectURL(blob);
    const a = Object.assign(document.createElement("a"), { href, download: filename });
    document.body.appendChild(a);
    a.click();
    setTimeout(() => {
      URL.revokeObjectURL(href);
      a.remove();
    }, 0);
  }

  async function downloadFile(url, filename, toast, ctx = {}) {
    try {
      ctx.setTag("warn", "Preparing download...");
      saveBlob(await fetchBlob(url), filename);
      ctx.setTag("loaded", "Ready");
      if (toast && window.cxToast) window.cxToast(toast);
    } catch (e) {
      console.error(e);
      ctx.setTag("error", "Download failed");
      ctx.setStatus(String(e));
    }
  }

  async function uploadJSON(url, file) {
    const fd = new FormData();
    fd.append("file", file);
    const res = await fetch(url, { method: "POST", body: fd });
    if (!res.ok) {
      let msg = `Import failed: ${res.status}`;
      try {
        const err = await res.json();
        if (err && err.detail) msg += ` - ${err.detail}`;
      } catch (_) {}
      throw new Error(msg);
    }
    return await res.json();
  }

  const listParts = (data, defs) => defs.flatMap(([k, label]) => data && data[k] != null ? [`${data[k]} ${label}${data[k] === 1 ? "" : "s"}`] : []);

  function bindFileImport(btn, input, url, done, ctx = {}) {
    if (!btn || !input) return;
    ctx.on(btn, "click", () => input.click());
    ctx.on(input, "change", async () => {
      const file = input.files && input.files[0];
      if (!file) return;
      try {
        ctx.setTag("warn", "Importing...");
        ctx.setStatus("");
        await done(await uploadJSON(url, file));
      } catch (e) {
        console.error(e);
        ctx.setTag("error", "Import failed");
        ctx.setStatus(String(e));
      } finally {
        try { input.value = ""; } catch (_) {}
      }
    });
  }

  Editor.FileUtils = { fetchJSON, fetchBlob, saveBlob, downloadFile, uploadJSON, listParts, bindFileImport };
  window.CrossWatchEditorFileUtils = Editor.FileUtils;
})();
