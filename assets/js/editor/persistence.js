/* assets/js/editor/persistence.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;

  function stateOf(ctx) {
    return (ctx && ctx.state) || {};
  }

  function findRowsMissingKey(state) {
    const missing = [];
    for (const row of state.rows || []) {
      if (row.deleted) continue;
      const key = (row.key || "").trim();
      if (key) continue;

      const hasOther =
        (row.title && row.title.trim()) ||
        (row.type && row.type.trim()) ||
        (row.year && String(row.year).trim()) ||
        ["imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist"].some(k => row[k] && String(row[k]).trim());

      if (hasOther) missing.push(row);
    }
    return missing;
  }

  function buildSaveData(ctx = {}) {
    const state = stateOf(ctx);
    const items = {};
    const blocks = ctx.isPolicySource?.() ? [...(state.preservedBlocks || [])] : [];
    const originals = {};
    const seenBlocks = new Set(blocks.map(key => key.toLowerCase()));

    for (const row of state.rows || []) {
      if (row.deleted) {
        if (ctx.isPolicySource?.() && row._origin === "baseline") {
          const k = (row.key || "").trim();
          if (k) {
            const kl = k.toLowerCase();
            if (!seenBlocks.has(kl)) {
              seenBlocks.add(kl);
              blocks.push(k);
            }
          }
        }
        continue;
      }

      if (ctx.isPolicySource?.() && row._origin === "baseline") continue;

      if (ctx.isPolicySource?.() && row._replacedKey) {
        const rk = String(row._replacedKey).trim();
        const rkl = rk.toLowerCase();
        if (rk && !seenBlocks.has(rkl)) {
          seenBlocks.add(rkl);
          blocks.push(rk);
        }
      }

      const key = (row.key || "").trim();
      if (!key) continue;

      const raw = row.raw || {};
      const ids = raw.ids || {};

      ["imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist"].forEach(k => {
        const v = String(row[k] || "").trim();
        if (v) ids[k] = v;
        else delete ids[k];
      });

      raw.ids = ids;
      raw.type = row.type || raw.type || null;
      raw.title = row.title ? row.title : raw.title || null;

      const y = (row.year || "").trim();
      const n = y ? parseInt(y, 10) : NaN;
      raw.year = Number.isFinite(n) ? n : null;

      items[key] = raw;
      originals[key] = String(row._replacedKey || key);
    }

    const payload = { kind: state.kind, source: state.source, items };
    if (ctx.isPolicySource?.()) payload.mapping_originals = originals;
    if (ctx.isProviderPickerSource?.()) {
      payload.provider = state.snapshot;
      payload.provider_instance = state.instance || "default";
      payload.pair_id = state.mappingPair || "";
      payload.blocks = blocks;
    }
    if (state.source === "playlist") {
      payload.endpoint = state.snapshot;
    }

    return { items, blocks, payload };
  }

  function confirmPlaylistRemovals(ctx, items) {
    const state = stateOf(ctx);
    if (state.source !== "playlist") return true;

    const currentKeys = new Set((state.playlistOriginalKeys || []).map(String));
    const nextKeys = new Set(Object.keys(items || {}));
    const removals = Array.from(currentKeys).filter(k => !nextKeys.has(k)).length;
    if (!removals || !state.playlistWarnings.length) return true;

    const text = state.playlistWarnings.join("\n");
    const confirmFn = typeof ctx.confirm === "function" ? ctx.confirm : window.confirm;
    return confirmFn(`${text}\n\n${T("editor.persist.removeItems", {count: removals})}`);
  }

  async function saveState(ctx = {}) {
    const state = stateOf(ctx);
    if (state.saving) return;

    const missing = findRowsMissingKey(state);
    if (missing.length) {
      ctx.setTag?.("error", T("editor.persist.missingKey"));
      ctx.setStatus?.(
        T("editor.persist.missingKeys", {count: missing.length})
      );
      if (window.cxToast) window.cxToast(T("editor.persist.fillKey"));
      return;
    }

    state.saving = true;
    ctx.setTag?.("warn", T("editor.persist.saving"));
    ctx.syncActionButtons?.();

    try {
      const { items, payload } = buildSaveData(ctx);

      if (!await confirmPlaylistRemovals(ctx, items)) {
        state.saving = false;
        ctx.setTag?.("warn", T("editor.persist.unsaved"));
        ctx.setStatus?.(T("editor.persist.cancelled"));
        ctx.syncActionButtons?.();
        return;
      }

      const res = await ctx.fetchJSON("/api/editor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      state.hasChanges = false;
      ctx.setTag?.("warn", T("editor.persist.savedTag"));
      if (state.source === "playlist") {
        const added = Number(res.added || 0);
        const removed = Number(res.removed || 0);
        const reordered = Number(res.reordered || 0);
        const unresolved = Number(res.unresolved_count || 0);
        const parts = [`+${added}`, `-${removed}`];
        if (reordered) parts.push(T("editor.persist.reordered", {n: reordered}));
        if (unresolved) parts.push(T("editor.persist.unresolved", {n: unresolved}));
        ctx.setStatus?.(T("editor.persist.applied", {parts: parts.join(", ")}));
        await ctx.loadState?.();
      } else {
        ctx.setStatus?.(T("editor.persist.saved", {count: Number(res.count || Object.keys(items).length)}));
        await ctx.loadSnapshots?.();
        await ctx.loadState?.();
      }
    } catch (e) {
      console.error(e);
      ctx.setTag?.("error", T("editor.persist.failed"));
      ctx.setStatus?.(String(e));
    } finally {
      state.saving = false;
      ctx.syncActionButtons?.();
    }
  }

  Editor.Persistence = {
    findRowsMissingKey,
    buildSaveData,
    confirmPlaylistRemovals,
    saveState,
  };
  window.CrossWatchEditorPersistence = Editor.Persistence;
})();
