/* assets/js/editor/persistence.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

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
    const blocks = [];
    const seenBlocks = new Set();

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
    }

    const payload = { kind: state.kind, source: state.source, items };
    if (ctx.isProviderPickerSource?.()) {
      payload.provider = state.snapshot;
      payload.provider_instance = state.instance || "default";
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
    return confirmFn(`${text}\n\nRemove ${removals} item${removals === 1 ? "" : "s"} from this playlist endpoint?`);
  }

  async function saveState(ctx = {}) {
    const state = stateOf(ctx);
    if (state.saving) return;

    const missing = findRowsMissingKey(state);
    if (missing.length) {
      ctx.setTag?.("error", "Missing key");
      ctx.setStatus?.(
        `Cannot save: ${missing.length} row${missing.length === 1 ? "" : "s"} have data but no Key. Fill the Key or remove the row.`
      );
      if (window.cxToast) window.cxToast("Fill Key for all rows with data before saving");
      return;
    }

    state.saving = true;
    ctx.setTag?.("warn", "Saving");
    ctx.syncActionButtons?.();

    try {
      const { items, payload } = buildSaveData(ctx);

      if (!confirmPlaylistRemovals(ctx, items)) {
        state.saving = false;
        ctx.setTag?.("warn", "Unsaved changes");
        ctx.setStatus?.("Save cancelled");
        ctx.syncActionButtons?.();
        return;
      }

      const res = await ctx.fetchJSON("/api/editor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      state.hasChanges = false;
      ctx.setTag?.("warn", "Saved");
      if (state.source === "playlist") {
        const added = Number(res.added || 0);
        const removed = Number(res.removed || 0);
        const reordered = Number(res.reordered || 0);
        const unresolved = Number(res.unresolved_count || 0);
        const parts = [`+${added}`, `-${removed}`];
        if (reordered) parts.push(`${reordered} reordered`);
        if (unresolved) parts.push(`${unresolved} unresolved`);
        ctx.setStatus?.(`Applied playlist changes: ${parts.join(", ")}`);
        await ctx.loadState?.();
      } else if (ctx.isManualSource?.()) {
        ctx.setStatus?.(`Saved ${res.count || Object.keys(items).length} overrides`);
        await ctx.loadState?.();
      } else {
        ctx.setStatus?.(`Saved ${res.count || Object.keys(items).length} items`);
        await ctx.loadSnapshots?.();
      }
    } catch (e) {
      console.error(e);
      ctx.setTag?.("error", "Save failed");
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
