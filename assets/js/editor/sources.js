/* assets/js/editor/sources.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  const SOURCES = ["state", "manual", "tracker", "playlist"];

  function stateOf(ctx) {
    return (ctx && ctx.state) || {};
  }

  function normalizeSource(value) {
    const s = String(value || "").trim();
    if (!SOURCES.includes(s)) return "state";
    return s;
  }

  function isTrackerSource(state) {
    return stateOf({ state }).source === "tracker";
  }

  function isManualSource(state) {
    return stateOf({ state }).source === "manual";
  }

  function isProviderPickerSource(state) {
    const s = stateOf({ state });
    return s.source === "state" || isManualSource(s);
  }

  function isPolicySource(state) {
    const s = stateOf({ state });
    return s.source === "state" || isManualSource(s) || s.source === "tracker";
  }

  function ensureTrackerOption(sourceSel) {
    if (!sourceSel) return;
    const manual = sourceSel.querySelector('option[value="manual"]');
    if (manual) {
      manual.textContent = "Manual Overrides";
    } else {
      const opt = document.createElement("option");
      opt.value = "manual";
      opt.textContent = "Manual Overrides";
      const trackerOpt = sourceSel.querySelector('option[value="tracker"]');
      if (trackerOpt) sourceSel.insertBefore(opt, trackerOpt);
      else sourceSel.appendChild(opt);
    }
    const existing = sourceSel.querySelector('option[value="tracker"]');
    if (existing) {
      existing.textContent = "Local Tracker";
      return;
    }
    const opt = document.createElement("option");
    opt.value = "tracker";
    opt.textContent = "Local Tracker";
    const playlistOpt = sourceSel.querySelector('option[value="playlist"]');
    if (playlistOpt) sourceSel.insertBefore(opt, playlistOpt);
    else sourceSel.appendChild(opt);
  }

  function currentTrackerWorkspace(state) {
    const s = stateOf({ state });
    const id = String(s.workspace || "").trim();
    const list = s.trackerWorkspaces || [];
    return list.find(w => String(w && w.id || "") === id) || list[0] || null;
  }

  function trackerKinds(state) {
    const ws = currentTrackerWorkspace(state);
    const feats = (ws && ws.features) || {};
    return ["watchlist", "history", "ratings", "progress"].filter(k => feats[k]);
  }

  function currentPlaylistEndpoint(state) {
    const s = stateOf({ state });
    const id = String(s.snapshot || "").trim();
    return (s.playlistEndpoints || []).find(ep => String(ep && ep.id || "") === id) || null;
  }

  function playlistEditable(state) {
    const s = stateOf({ state });
    if (s.source !== "playlist") return true;
    const r = s.playlistResource || {};
    return !!r && !r.smart && !!(r.can_add || r.can_remove || r.can_reorder);
  }

  function syncSnapshotControlVisibility(ctx = {}) {
    const state = stateOf(ctx);
    const show = !isTrackerSource(state);
    if (ctx.snapLabel) ctx.snapLabel.style.display = show ? "" : "none";
    if (ctx.snapSel) {
      ctx.snapSel.style.display = show ? "" : "none";
      const wrap = ctx.snapSel.nextElementSibling && ctx.snapSel.nextElementSibling.classList?.contains("cw-icon-select")
        ? ctx.snapSel.nextElementSibling
        : null;
      if (wrap) wrap.style.display = show ? "" : "none";
    }
  }

  function playlistEndpointLabel(ep) {
    if (!ep) return "Endpoint";
    const name = String(ep.name || ep.id || "Endpoint");
    const provider = String(ep.provider_label || ep.provider || "").trim();
    const playlist = String(ep.playlist_name || ep.playlist_id || "").trim();
    const type = String(ep.playlist_type || ep.resource_kind || ep.kind || "").trim();
    const parts = [name];
    if (provider) parts.push(provider);
    if (playlist && playlist !== name) parts.push(playlist);
    if (type) parts.push(type);
    return parts.join(" - ");
  }

  function rebuildSnapshots(ctx = {}) {
    const state = stateOf(ctx);
    const snapSel = ctx.snapSel;
    if (!snapSel) return;
    const providerPicker = isProviderPickerSource(state);
    const isTracker = isTrackerSource(state);
    const isPlaylist = state.source === "playlist";
    const esc = typeof ctx.escapeHtml === "function" ? ctx.escapeHtml : (s => String(s || ""));

    if (ctx.snapLabel) ctx.snapLabel.textContent = providerPicker ? "Provider" : "Endpoint";
    syncSnapshotControlVisibility(ctx);
    if (ctx.instanceLabel) ctx.instanceLabel.style.display = (providerPicker || isTracker) ? "" : "none";
    if (ctx.instanceSel) ctx.instanceSel.style.display = (providerPicker || isTracker) ? "" : "none";
    ctx.syncProfileIconSelect?.(ctx.instanceSel, providerPicker || isTracker);

    if (isTracker) {
      const list = Array.isArray(state.trackerWorkspaces) ? state.trackerWorkspaces : [];
      const options = list
        .map(w => `<option value="${esc(w && w.id)}">${esc((w && w.label) || "Local tracker")}</option>`)
        .join("");
      snapSel.innerHTML = options || `<option value="">No workspaces</option>`;
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.workspace) ? state.workspace : opts[0] || "";
      if (next !== state.workspace) state.workspace = next;
      snapSel.value = state.workspace || "";
      ctx.syncProviderIconSelect?.(snapSel, false);
      syncSnapshotControlVisibility(ctx);
      return;
    }

    if (isPlaylist) {
      const list = Array.isArray(state.playlistEndpoints) ? state.playlistEndpoints : [];
      const options = list
        .map(ep => `<option value="${esc(ep && ep.id)}">${esc(playlistEndpointLabel(ep))}</option>`)
        .join("");
      snapSel.innerHTML = options || `<option value="">No endpoints</option>`;
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.snapshot) ? state.snapshot : opts[0] || "";
      if (next !== state.snapshot) state.snapshot = next;
      snapSel.value = state.snapshot || "";
      ctx.syncProviderIconSelect?.(snapSel, false);
      syncSnapshotControlVisibility(ctx);
      return;
    }

    if (providerPicker) {
      const list = Array.isArray(state.snapshots) ? state.snapshots : [];
      const label = typeof ctx.providerLabel === "function" ? ctx.providerLabel : ((p, fallback) => fallback || p);
      snapSel.innerHTML = list.map(p => `<option value="${p}">${label(p, p)}</option>`).join("");
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.snapshot) ? state.snapshot : opts[0] || "";
      if (next !== state.snapshot) state.snapshot = next;
      snapSel.value = state.snapshot || "";
      ctx.syncProviderIconSelect?.(snapSel, true);
      syncSnapshotControlVisibility(ctx);
    }
  }

  function syncSourceUI(ctx = {}) {
    const state = stateOf(ctx);
    state.source = normalizeSource(state.source);
    if (ctx.sourceSel) {
      ctx.sourceSel.querySelector('option[value="pair"]')?.remove();
      if (!ctx.sourceSel.querySelector('option[value="playlist"]')) {
        ctx.sourceSel.insertAdjacentHTML("beforeend", '<option value="playlist">Playlist Endpoint</option>');
      }
      ensureTrackerOption(ctx.sourceSel);
    }
    const isManual = isManualSource(state);
    const providerPicker = isProviderPickerSource(state);
    const isTracker = isTrackerSource(state);
    const isPlaylist = state.source === "playlist";
    const policy = isPolicySource(state);
    if (ctx.sourceSel) ctx.sourceSel.value = state.source;
    if (ctx.pairLabel) ctx.pairLabel.style.display = "none";
    if (ctx.pairSel) ctx.pairSel.style.display = "none";
    if (ctx.snapLabel) ctx.snapLabel.textContent = providerPicker ? "Provider" : "Endpoint";
    syncSnapshotControlVisibility(ctx);
    if (ctx.kindSel) ctx.kindSel.disabled = isPlaylist;
    if (ctx.instanceLabel) ctx.instanceLabel.style.display = (providerPicker || isTracker) ? "" : "none";
    if (ctx.instanceSel) ctx.instanceSel.style.display = (providerPicker || isTracker) ? "" : "none";
    if (ctx.backupCard) ctx.backupCard.style.display = "none";
    if (ctx.stateBackupCard) ctx.stateBackupCard.style.display = policy ? "" : "none";
    if (ctx.blockedOnlyBtn) ctx.blockedOnlyBtn.style.display = policy ? "" : "none";
    if (ctx.trackerNotice) ctx.trackerNotice.style.display = isTracker ? "block" : "none";

    const sub = ctx.host?.querySelector(".cw-sub");
    if (sub) {
      sub.textContent = isManual
        ? "Edit the manual override policy applied during future syncs."
        : isTracker
          ? "Edit what CrossWatch sends from its local tracker. Connected provider accounts are not changed."
          : "Edit your current state or playlist endpoints";
    }

    if (isPlaylist) {
      state.kind = "watchlist";
      state.instance = "default";
    }
    ctx.syncKindUI?.();

    if (!policy && state.blockedOnly) {
      state.blockedOnly = false;
      ctx.syncTypeFilterUI?.();
      ctx.persistUIState?.();
    }
    ctx.syncStateBulkUI?.();
    ctx.syncImportUI?.();
    ctx.syncTypeFilterUI?.();
    ctx.syncActionButtons?.();
    ctx.syncHeaderPills?.();
  }

  function showStateHint(mode, ctx = {}) {
    const stateHint = ctx.stateHint;
    if (!stateHint) return;
    if (mode === "state") {
      stateHint.innerHTML =
        "<strong>No sync state found.</strong> Run a CrossWatch sync once to generate it. After that, your manual adds and blocks will show up here.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "playlist") {
      stateHint.innerHTML =
        "<strong>No playlist endpoints found.</strong> Create an endpoint on the Playlists page first. Then select it here to edit its items.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "tracker") {
      stateHint.innerHTML =
        "<strong>No Local Tracker data found.</strong> Run a sync pair that uses Local Tracker first.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "manual") {
      stateHint.innerHTML =
        "<strong>No manual overrides found.</strong> Add a row here or edit a baseline row in Current State to create an override.";
      stateHint.style.display = "block";
      return;
    }
    stateHint.style.display = "none";
  }

  async function loadTrackerWorkspaces(ctx = {}) {
    const state = stateOf(ctx);
    try {
      if (ctx.instanceSel) {
        const nextInst = await ctx.loadInstanceOptions?.("CROSSWATCH", ctx.instanceSel, state.instance || "default");
        if (nextInst !== state.instance) {
          state.instance = nextInst;
          ctx.persistUIState?.();
        }
      }
      const params = new URLSearchParams({ provider_instance: state.instance || "default" });
      const data = await ctx.fetchJSON(`/api/editor/tracker/workspaces?${params.toString()}`);
      const list = Array.isArray(data && data.workspaces) ? data.workspaces : [];
      state.trackerWorkspaces = list;
      state.trackerAvailable = list.length > 0;
      if (state.workspace && !list.some(w => String(w && w.id || "") === String(state.workspace))) {
        state.workspace = "";
        ctx.persistUIState?.();
      }
    } catch (_) {
      state.trackerWorkspaces = [];
      state.trackerAvailable = false;
    }
    ensureTrackerOption(ctx.sourceSel);
    return state.trackerWorkspaces;
  }

  async function loadSnapshots(ctx = {}) {
    const state = stateOf(ctx);
    try {
      if (isTrackerSource(state)) {
        await loadTrackerWorkspaces(ctx);
        rebuildSnapshots(ctx);
        ctx.syncKindUI?.();
        if (!state.trackerWorkspaces.length) showStateHint("tracker", ctx);
        else showStateHint(null, ctx);
        return;
      }
      if (state.source === "playlist") {
        const data = await ctx.fetchJSON("/api/editor/playlists/endpoints");
        state.playlistEndpoints = Array.isArray(data && data.endpoints) ? data.endpoints : [];
        state.snapshots = state.playlistEndpoints;
        rebuildSnapshots(ctx);
        if (!state.playlistEndpoints.length) showStateHint("playlist", ctx);
        else showStateHint(null, ctx);
        return;
      }
      if (isProviderPickerSource(state)) {
        const data = await ctx.fetchJSON(`/api/editor/state/providers`);
        state.snapshots = Array.isArray(data.providers) ? data.providers : [];
        rebuildSnapshots(ctx);

        const prov = state.snapshot || (ctx.snapSel ? (ctx.snapSel.value || "") : "");
        if (prov) {
          const nextInst = await ctx.loadInstanceOptions?.(prov, ctx.instanceSel, state.instance);
          if (prov !== state.snapshot || nextInst !== state.instance) {
            state.snapshot = prov;
            state.instance = nextInst;
            ctx.persistUIState?.();
          }
        } else {
          const nextInst = ctx.renderInstanceOptions?.(ctx.instanceSel, [{ id: "default", label: "Default" }], "default");
          if (state.instance !== nextInst) {
            state.instance = nextInst;
            ctx.persistUIState?.();
          }
        }

        if (!state.snapshots.length) showStateHint(isManualSource(state) ? "manual" : "state", ctx);
        else showStateHint(null, ctx);
        return;
      }
      state.source = "state";
      rebuildSnapshots(ctx);
    } catch (e) {
      console.error(e);
    }
  }

  Editor.Sources = {
    SOURCES,
    normalizeSource,
    isTrackerSource,
    isManualSource,
    isProviderPickerSource,
    isPolicySource,
    ensureTrackerOption,
    currentTrackerWorkspace,
    trackerKinds,
    currentPlaylistEndpoint,
    playlistEditable,
    syncSnapshotControlVisibility,
    playlistEndpointLabel,
    rebuildSnapshots,
    syncSourceUI,
    showStateHint,
    loadTrackerWorkspaces,
    loadSnapshots,
  };
  window.CrossWatchEditorSources = Editor.Sources;
})();
