/* assets/js/editor/load-controller.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});
  let deferredRefreshTimer = null;

  function renderMappingScope(ctx, data) {
    const parent = ctx.instanceSel?.parentElement;
    if (!parent) return;
    let label = document.getElementById("cw-mapping-scope-label");
    if (!label) {
      label = document.createElement("label");
      label.id = "cw-mapping-scope-label";
      label.textContent = "Mapping scope";
      const select = document.createElement("select");
      select.id = "cw-mapping-scope";
      select.className = "cw-select";
      select.setAttribute("aria-label", "Mapping scope");
      label.append(select);
      parent.append(label);
    }
    label.hidden = !ctx.isPolicySource();
    const select = label.querySelector("select");
    select.replaceChildren(new Option("All pairs using this provider instance", ""),
      ...(data.mapping_scopes || []).map(pair => new Option(pair.label, pair.id)));
    select.value = ctx.state.mappingPair || "";
    select.onchange = async () => {
      if (ctx.state.loading || ctx.state.saving || (ctx.state.hasChanges && !window.confirm("Discard unsaved Editor changes and switch mapping scope?"))) {
        select.value = ctx.state.mappingPair || "";
        return;
      }
      ctx.state.mappingPair = select.value;
      await loadState(ctx);
    };
  }

  function renderInstanceSharingNote(sharing) {
    const el = document.getElementById("cw-instance-shared");
    if (!el) return;
    const owners = Array.isArray(sharing?.owners) ? sharing.owners.filter(Boolean) : [];
    if (!sharing?.shared || owners.length < 2) {
      el.style.display = "none";
      el.textContent = "";
      return;
    }
    const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
    el.innerHTML =
      `<span class="material-symbols-rounded" aria-hidden="true">group</span>` +
      `<span>Shared profile. Blocks and manual entries here apply to every user of this instance: ` +
      `<strong>${owners.map(esc).join(", ")}</strong>.</span>`;
    el.style.display = "";
  }

  function editorIsVisible(ctx = {}) {
    const host = ctx.host;
    return !!host && !!document.getElementById("page-editor") && host.getClientRects().length > 0;
  }

  function syncSelectedScopeFromControls(ctx = {}) {
    const state = ctx.state;
    if (ctx.isProviderPickerSource()) {
      state.snapshot = (ctx.snapSel && ctx.snapSel.value) ? ctx.snapSel.value : state.snapshot || "";
      state.instance = (ctx.instanceSel && ctx.instanceSel.value) ? ctx.instanceSel.value : state.instance || "default";
      return;
    }
    if (state.source === "playlist") {
      state.snapshot = (ctx.snapSel && ctx.snapSel.value) ? ctx.snapSel.value : state.snapshot || "";
      state.instance = "default";
    }
  }

  async function loadState(ctx = {}) {
    const state = ctx.state;
    if (state.source === "playlist") {
      const endpointId = String(state.snapshot || "").trim();
      if (!endpointId) {
        state.items = {};
        state.rows = [];
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.hasChanges = false;
        state.page = 0;
        state.playlistResource = null;
        state.playlistWarnings = [];
        state.playlistOriginalKeys = [];
        ctx.renderRows();
        ctx.showStateHint("playlist");
        ctx.setTag("loaded", "No endpoint");
        ctx.setStatus("");
        ctx.syncActionButtons();
        return;
      }
    }
    state.source = ctx.normalizeSource(state.source);
    state.loading = true;
    state.loadError = null;
    ctx.host?.setAttribute("aria-busy", "true");
    ctx.syncActionButtons();
    if (!(state.rows || []).length) ctx.renderRows();
    ctx.setTag("warn", "Loading");
    try {
      const params = new URLSearchParams({ kind: state.kind, source: state.source });
      if (ctx.isProviderPickerSource() && state.snapshot) {
        params.set("provider", state.snapshot);
        params.set("provider_instance", state.instance || "default");
        if (state.mappingPair) params.set("pair_id", state.mappingPair);
      }
      if (state.source === "playlist" && state.snapshot) params.set("endpoint", state.snapshot);

      const data = await ctx.fetchJSON(`/api/editor?${params.toString()}`);
      if (data && data.ok === false) throw new Error(data.error || data.detail || "Load failed");
      renderMappingScope(ctx, data);

      if (state.source === "playlist") {
        state.playlistResource = data.resource || null;
        state.playlistWarnings = Array.isArray(data.resource && data.resource.warnings) ? data.resource.warnings.map(String) : [];
        state.playlistOriginalKeys = Array.isArray(data.original_keys) ? data.original_keys.map(String) : [];
        state.items = data.items || {};
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.rows = ctx.buildRows(state.items);
      } else if (ctx.isPolicySource()) {
        if (ctx.isProviderPickerSource() && data && typeof data.provider === "string" && data.provider.trim()) {
          state.snapshot = data.provider.trim();
          if (ctx.snapSel) {
            ctx.snapSel.value = state.snapshot;
            ctx.syncProviderIconSelect(ctx.snapSel, true);
          }
        }
        state.baselineItems = data.items || {};
        state.manualAdds = data.manual_adds || {};
        state.manualBlocks = Array.isArray(data.manual_blocks) ? data.manual_blocks : [];
        state.preservedBlocks = Array.isArray(data.preserved_blocks) ? data.preserved_blocks : [];

        if (ctx.isPolicySource() && data && typeof data.provider_instance === "string") {
          state.instance = data.provider_instance;
          if (ctx.instanceSel) {
            ctx.instanceSel.value = state.instance;
            ctx.syncProfileIconSelect(ctx.instanceSel, true);
          }
        }

        renderInstanceSharingNote(data && data.instance_sharing);

        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        const merged = Object.assign({}, state.baselineItems || {});
        const manualKeys = new Set();
        for (const [k, v] of Object.entries(state.manualAdds || {})) {
          const key = String(k || "").trim();
          if (!key) continue;
          const existingKey = Object.keys(merged).find(x => String(x || "").toLowerCase() === key.toLowerCase());
          const finalKey = existingKey || key;
          merged[finalKey] = v;
          manualKeys.add(finalKey.toLowerCase());
        }

        state.items = merged;
        state.rows = ctx.buildRows(state.items);

        const baselineKeys = new Set(Object.keys(state.baselineItems || {}));
        const blocked = new Set(
          (state.manualBlocks || []).map(x => String(x || "").trim()).filter(Boolean)
        );

        for (const row of state.rows) {
          const rowKey = String(row.key || "").toLowerCase();
          row._origin = manualKeys.has(rowKey) ? "manual" : baselineKeys.has(row.key) ? "baseline" : "manual";
          if (row._origin === "baseline") row.deleted = blocked.has(row.key);
        }
      }

      for (const row of state.rows || []) row._mappingScope = data.mapping_origins?.[row.key] || "";
      state.hasChanges = false;
      state.page = 0;
      state.loading = false;
      ctx.renderRows();

      if (ctx.isPolicySource()) {
        const hasBaseline = state.baselineItems && Object.keys(state.baselineItems).length > 0;
        const hasManual = state.manualAdds && Object.keys(state.manualAdds).length > 0;
        const hasBlocks = Array.isArray(state.manualBlocks) && state.manualBlocks.length > 0;
        ctx.showStateHint(hasBaseline || hasManual || hasBlocks ? null : "state");
      } else if (state.source === "playlist") {
        ctx.showStateHint(state.snapshot ? null : "playlist");
      }

      ctx.setTag("loaded", "Ready");
      if (state.source === "playlist" && state.playlistWarnings.length) {
        ctx.setStatus(state.playlistWarnings[0]);
      }
    } catch (e) {
      state.loadError = e;
      state.loading = false;
      console.error(e);
      const msg = String(e || "");

      if (
        state.source === "state" &&
        (msg.includes("404") || /state\.json/i.test(msg) || /missing state/i.test(msg))
      ) {
        ctx.showStateHint("state");
        state.items = {};
        state.rows = [];
        ctx.renderRows();
        ctx.setTag("warn", "Missing state");
        ctx.setStatus("");
      } else {
        if (!(state.rows || []).length) ctx.renderRows();
        ctx.setTag("error", "Load failed");
        ctx.setStatus(msg);
      }
    } finally {
      state.loading = false;
      ctx.host?.removeAttribute("aria-busy");
      ctx.syncActionButtons();
    }
  }

  async function refreshEditor(ctx = {}, options = {}) {
    const state = ctx.state;
    if (state.mappingEditing) return;
    const force = !!options.force;
    if (!force && (!editorIsVisible(ctx) || state.hasChanges || state.loading || state.saving)) return;
    syncSelectedScopeFromControls(ctx);
    state.page = 0;
    await ctx.loadSnapshots();
    await loadState(ctx);
    state.lastSyncAt = Date.now();
    ctx.syncHeaderPills();
  }

  function queueEditorRefresh(ctx = {}, delay = 250) {
    if (deferredRefreshTimer) window.clearTimeout(deferredRefreshTimer);
    deferredRefreshTimer = window.setTimeout(() => {
      deferredRefreshTimer = null;
      refreshEditor(ctx).catch(e => console.warn("[editor] refresh failed", e));
    }, delay);
  }

  Editor.LoadController = {
    editorIsVisible,
    syncSelectedScopeFromControls,
    loadState,
    refreshEditor,
    queueEditorRefresh,
  };
  window.CrossWatchEditorLoadController = Editor.LoadController;
})();
