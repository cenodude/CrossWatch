/* assets/js/editor/importers.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function stateOf(ctx) {
    return (ctx && ctx.state) || {};
  }

  function setImportBusy(on, message, ctx = {}) {
    if (ctx.importProgressWrap) ctx.importProgressWrap.style.display = on ? "" : "none";
    if (ctx.importProgressText) ctx.importProgressText.textContent = message || "";
    const disabled = !!on;
    if (ctx.importRunBtn) ctx.importRunBtn.disabled = disabled;
    if (ctx.importProviderSel) ctx.importProviderSel.disabled = disabled;
    if (ctx.importModeSel) ctx.importModeSel.disabled = disabled;
    if (ctx.importWatchlistCb) ctx.importWatchlistCb.disabled = disabled || ctx.importWatchlistCb.disabled;
    if (ctx.importHistoryCb) ctx.importHistoryCb.disabled = disabled || ctx.importHistoryCb.disabled;
    if (ctx.importRatingsCb) ctx.importRatingsCb.disabled = disabled || ctx.importRatingsCb.disabled;
    if (ctx.importProgressCb) ctx.importProgressCb.disabled = disabled || ctx.importProgressCb.disabled;
  }

  function syncImportUI(ctx = {}) {
    const state = stateOf(ctx);
    if (!ctx.importRow) return;
    const show = state.source === "state" && state.importEnabled;
    ctx.importRow.style.display = show ? "" : "none";
    if (!show) return;

    if (ctx.importModeSel) ctx.importModeSel.value = state.importMode || "replace";

    const all = Array.isArray(state.importProviders) ? state.importProviders : [];
    const list = all.filter(p => p && p.configured && p.name);

    if (ctx.importProviderSel) {
      const current = ctx.importProviderSel.value || state.importProvider || "";
      const opts = list
        .map(p => {
          const name = p && p.name ? String(p.name) : "";
          const label = ctx.providerLabel?.(name, p && p.label ? String(p.label) : name) || name;
          return `<option value="${name}">${label}</option>`;
        })
        .join("");

      ctx.importProviderSel.innerHTML = opts || `<option value="">No configured providers</option>`;

      const names = list.map(p => String(p.name));
      let next = current;

      if (!next || !names.includes(next)) {
        next = state.snapshot && names.includes(state.snapshot) ? state.snapshot : "";
      }
      if (!next) next = names[0] || "";

      state.importProvider = next;
      ctx.importProviderSel.value = next;
      ctx.importProviderSel.disabled = !names.length;
      ctx.syncProviderIconSelect?.(ctx.importProviderSel, true);
    }

    const sel = state.importProvider || (ctx.importProviderSel ? ctx.importProviderSel.value : "");
    const p = list.find(x => String((x || {}).name || "") === String(sel || ""));
    const feats = (p && p.features) ? p.features : {};

    if (ctx.importInstanceSel) {
      const ids = (p && Array.isArray(p.instances)) ? p.instances : ["default"];
      const instObjs = ids.map(x => {
        if (x && typeof x === "object") {
          const id = String(x.id || "default");
          return { id, label: String(x.label || x.display_label || id) };
        }
        const id = String(x || "default");
        return { id, label: id === "default" ? "Default" : id };
      });
      const nextInst = ctx.renderInstanceOptions?.(ctx.importInstanceSel, instObjs, state.importProviderInstance);
      if (nextInst !== state.importProviderInstance) {
        state.importProviderInstance = nextInst;
        ctx.persistUIState?.();
      }
      const instanceField = ctx.importInstanceSel.closest(".cw-import-field");
      if (instanceField) instanceField.style.display = state.importProvider ? "" : "none";
      else ctx.importInstanceSel.style.display = state.importProvider ? "" : "none";
      ctx.syncProfileIconSelect?.(ctx.importInstanceSel, !!state.importProvider);
    }

    const setCb = (wrap, cb, key) => {
      const supported = !!feats[key];
      if (wrap) wrap.style.display = supported ? "" : "none";
      if (!cb) return;
      cb.disabled = !supported;
      if (!supported) cb.checked = false;
      else if (state.importFeatures && typeof state.importFeatures[key] === "boolean") cb.checked = !!state.importFeatures[key];
    };

    setCb(ctx.importWatchlistWrap, ctx.importWatchlistCb, "watchlist");
    setCb(ctx.importHistoryWrap, ctx.importHistoryCb, "history");
    setCb(ctx.importRatingsWrap, ctx.importRatingsCb, "ratings");
    setCb(ctx.importProgressFeatWrap, ctx.importProgressCb, "progress");

    if (ctx.importRunBtn) ctx.importRunBtn.disabled = !state.importProvider;
  }

  async function loadImportProviders(ctx = {}) {
    const state = stateOf(ctx);
    state.importEnabled = false;
    state.importProviders = [];
    if (!ctx.importRow) return;
    try {
      const data = await ctx.fetchJSON("/api/editor/state/import/providers");
      state.importEnabled = !!(data && data.enabled);
      state.importProviders = Array.isArray(data && data.providers) ? data.providers : [];
    } catch (_) {
      state.importEnabled = false;
      state.importProviders = [];
    }
    syncImportUI(ctx);
  }

  function collectImportFeatures(ctx = {}) {
    const feats = [];
    if (ctx.importWatchlistCb && ctx.importWatchlistCb.checked && !ctx.importWatchlistCb.disabled) feats.push("watchlist");
    if (ctx.importHistoryCb && ctx.importHistoryCb.checked && !ctx.importHistoryCb.disabled) feats.push("history");
    if (ctx.importRatingsCb && ctx.importRatingsCb.checked && !ctx.importRatingsCb.disabled) feats.push("ratings");
    if (ctx.importProgressCb && ctx.importProgressCb.checked && !ctx.importProgressCb.disabled) feats.push("progress");
    return feats;
  }

  async function runStateImport(ctx = {}) {
    const state = stateOf(ctx);
    if (state.source !== "state") return;
    const provider = (ctx.importProviderSel ? ctx.importProviderSel.value : state.importProvider) || "";
    const features = collectImportFeatures(ctx);
    const mode = (ctx.importModeSel ? ctx.importModeSel.value : state.importMode) || "replace";

    if (!provider) {
      ctx.setStatusSticky?.("Pick a provider first", 3000);
      return;
    }
    if (!features.length) {
      ctx.setStatusSticky?.("Pick at least one dataset", 3000);
      return;
    }

    state.importProvider = provider;
    state.importMode = mode;
    state.importFeatures = {
      watchlist: features.includes("watchlist"),
      history: features.includes("history"),
      ratings: features.includes("ratings"),
      progress: features.includes("progress"),
    };

    try {
      const msg = `Importing ${features.join(", ")} from ${provider}...`;
      setImportBusy(true, msg, ctx);
      ctx.setTag?.("warn", "Importing...");
      ctx.setStatus?.(msg);

      const res = await ctx.fetchJSON("/api/editor/state/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, provider_instance: state.importProviderInstance || "default", features, mode }),
      });

      const featsOut = (res && res.features) ? res.features : {};
      const bits = [];
      let totalMs = 0;

      for (const k of Object.keys(featsOut)) {
        const r = featsOut[k] || {};
        if (r.skipped) continue;
        if (r.ok) bits.push(`${k}:${r.count}`);
        if (typeof r.elapsed_ms === "number") totalMs += r.elapsed_ms;
      }

      let done = "Imported " + (bits.length ? bits.join(" - ") : "done");
      if (totalMs) done += ` (${(totalMs / 1000).toFixed(1)}s)`;

      ctx.setTag?.("loaded", "Imported");
      ctx.setStatusSticky?.(done, 6000);
      if (window.cxToast) window.cxToast(done);

      state.snapshot = provider;
      state.instance = state.importProviderInstance || "default";
      ctx.persistUIState?.();
      await ctx.loadSnapshots?.();
      await ctx.loadState?.();
    } catch (e) {
      console.error(e);
      ctx.setTag?.("error", "Import failed");
      ctx.setStatus?.(String(e));
    } finally {
      setImportBusy(false, "", ctx);
      syncImportUI(ctx);
    }
  }

  Editor.Importers = {
    setImportBusy,
    syncImportUI,
    loadImportProviders,
    collectImportFeatures,
    runStateImport,
  };
  window.CrossWatchEditorImporters = Editor.Importers;
})();
