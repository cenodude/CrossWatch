/* assets/js/editor/sources.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  const SOURCES = ["state", "manual", "playlist"];

  function stateOf(ctx) {
    return (ctx && ctx.state) || {};
  }

  function normalizeSource(value) {
    const s = String(value || "").trim();
    if (!SOURCES.includes(s)) return "state";
    return s;
  }

  function isTrackerSource(state) {
    return false;
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
    return s.source === "state" || isManualSource(s);
  }

  function hasPlaylistEndpoints(state) {
    const s = stateOf({ state });
    return Array.isArray(s.playlistEndpoints) && s.playlistEndpoints.length > 0;
  }

  function playlistEndpointsLoaded(state) {
    return stateOf({ state }).playlistEndpointsLoaded === true;
  }

  function ensureSourceOptions(sourceSel, state) {
    if (!sourceSel) return;
    sourceSel.querySelector('option[value="tracker"]')?.remove();
    const showPlaylist = hasPlaylistEndpoints(state);
    const playlistOpt = sourceSel.querySelector('option[value="playlist"]');
    if (!showPlaylist) {
      playlistOpt?.remove();
    } else if (playlistOpt) {
      playlistOpt.textContent = "Playlist Endpoint";
    }
    const manual = sourceSel.querySelector('option[value="manual"]');
    if (manual) {
      manual.textContent = "Manual Overrides";
    } else {
      const opt = document.createElement("option");
      opt.value = "manual";
      opt.textContent = "Manual Overrides";
      const currentPlaylistOpt = sourceSel.querySelector('option[value="playlist"]');
      if (currentPlaylistOpt) sourceSel.insertBefore(opt, currentPlaylistOpt);
      else sourceSel.appendChild(opt);
    }
    if (showPlaylist && !sourceSel.querySelector('option[value="playlist"]')) {
      const opt = document.createElement("option");
      opt.value = "playlist";
      opt.textContent = "Playlist Endpoint";
      sourceSel.appendChild(opt);
    }
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
    if (ctx.snapLabel) ctx.snapLabel.style.display = "";
    if (ctx.snapSel) {
      ctx.snapSel.style.display = "";
      const wrap = ctx.snapSel.nextElementSibling && ctx.snapSel.nextElementSibling.classList?.contains("cw-icon-select")
        ? ctx.snapSel.nextElementSibling
        : null;
      if (wrap) wrap.style.display = "";
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
    const isPlaylist = state.source === "playlist";
    const esc = typeof ctx.escapeHtml === "function" ? ctx.escapeHtml : (s => String(s || ""));

    if (ctx.snapLabel) ctx.snapLabel.textContent = providerPicker ? "Provider" : "Endpoint";
    syncSnapshotControlVisibility(ctx);
    if (ctx.instanceLabel) ctx.instanceLabel.style.display = providerPicker ? "" : "none";
    if (ctx.instanceSel) ctx.instanceSel.style.display = providerPicker ? "" : "none";
    ctx.syncProfileIconSelect?.(ctx.instanceSel, providerPicker);

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
    if (state.source === "playlist" && playlistEndpointsLoaded(state) && !hasPlaylistEndpoints(state)) {
      state.source = "state";
      state.snapshot = "";
    }
    if (ctx.sourceSel) {
      ctx.sourceSel.querySelector('option[value="pair"]')?.remove();
      ensureSourceOptions(ctx.sourceSel, state);
    }
    const isManual = isManualSource(state);
    const providerPicker = isProviderPickerSource(state);
    const isPlaylist = state.source === "playlist";
    const policy = isPolicySource(state);
    if (ctx.sourceSel) ctx.sourceSel.value = state.source;
    if (ctx.pairLabel) ctx.pairLabel.style.display = "none";
    if (ctx.pairSel) ctx.pairSel.style.display = "none";
    if (ctx.snapLabel) ctx.snapLabel.textContent = providerPicker ? "Provider" : "Endpoint";
    syncSnapshotControlVisibility(ctx);
    if (ctx.kindSel) ctx.kindSel.disabled = isPlaylist;
    if (ctx.instanceLabel) ctx.instanceLabel.style.display = providerPicker ? "" : "none";
    if (ctx.instanceSel) ctx.instanceSel.style.display = providerPicker ? "" : "none";
    if (ctx.backupCard) ctx.backupCard.style.display = "none";
    if (ctx.stateBackupCard) ctx.stateBackupCard.style.display = policy ? "" : "none";
    if (ctx.blockedOnlyBtn) ctx.blockedOnlyBtn.style.display = policy ? "" : "none";
    if (ctx.trackerNotice) ctx.trackerNotice.style.display = "none";

    const sub = ctx.host?.querySelector(".cw-sub");
    if (sub) {
      sub.textContent = isManual
        ? "Edit the manual override policy applied during future syncs."
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
    if (mode === "manual") {
      stateHint.innerHTML =
        "<strong>No manual overrides found.</strong> Add a row here or edit a baseline row in Current State to create an override.";
      stateHint.style.display = "block";
      return;
    }
    stateHint.style.display = "none";
  }

  async function loadSnapshots(ctx = {}) {
    const state = stateOf(ctx);
    try {
      const endpointData = await ctx.fetchJSON("/api/editor/playlists/endpoints").catch(() => null);
      state.playlistEndpoints = Array.isArray(endpointData && endpointData.endpoints) ? endpointData.endpoints : [];
      state.playlistEndpointsLoaded = true;
      if (state.source === "playlist" && !state.playlistEndpoints.length) {
        state.source = "state";
        state.snapshot = "";
        ctx.persistUIState?.();
      }
      ensureSourceOptions(ctx.sourceSel, state);
      if (ctx.sourceSel) ctx.sourceSel.value = state.source;
      if (state.source === "playlist") {
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
    ensureTrackerOption: ensureSourceOptions,
    ensureSourceOptions,
    currentPlaylistEndpoint,
    playlistEditable,
    syncSnapshotControlVisibility,
    playlistEndpointLabel,
    rebuildSnapshots,
    syncSourceUI,
    showStateHint,
    loadSnapshots,
  };
  window.CrossWatchEditorSources = Editor.Sources;
})();
