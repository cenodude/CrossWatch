/* assets/js/playlists.js */
/* Playlists page shell and components */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function () {
  "use strict";

  const BASE = "/api/playlists";
  const RULESET_DEFAULTS = {
    direction: "one_way",
    initial_sync: "source_authoritative",
    read_mode: "direct",
    write_mode: "direct",
    membership: "managed_only",
    order: "ignore",
    deduplicate: "canonical_id",
    allocation: "stable_first_fit",
    rebalance: "never",
    overflow: "block",
    per_endpoint_capacity: 250,
    aggregate_capacity: 1000,
    maximum_targets: 1,
    track_assignments: true,
  };
  const NAME_MAX = 10;
  const PLAYLIST_NAME_MAX = 20;
  const EMPTY_PROVIDER_RETRIES = 2;
  const EMPTY_PROVIDER_RETRY_MS = 300;
  const SAFE_NAME_CHARS = " _.'-&()";
  const PLAYLIST_COMPATIBLE_PROVIDERS = new Set(["PLEX", "TRAKT", "MDBLIST", "JELLYFIN", "EMBY", "PUBLICMETADB", "SIMKL", "CROSSWATCH"]);
  const SIMKL_PLAYLIST_WARNING = "SIMKL Custom Lists are not supported. These endpoints use SIMKL's built in status buckets, which are not true playlists. Changes may move or remove items from your SIMKL library. Use with caution.";
  const RULESET_PRESETS = {
    direct: {
      label: "Direct sync",
      description: "Keep one source playlist synced to one target playlist.",
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "direct", write_mode: "direct", membership: "managed_only", order: "ignore", maximum_targets: 1 },
    },
    mirror: {
      label: "Mirror source",
      description: "Make the target list match the source list as closely as possible.",
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "direct", write_mode: "direct", membership: "mirror", order: "preserve", maximum_targets: 1 },
    },
    split: {
      label: "Split large playlists",
      description: "Split a large source playlist across several target lists when capacity is reached.",
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "direct", write_mode: "partition", membership: "managed_only", order: "ignore", per_endpoint_capacity: 100, aggregate_capacity: 1000, maximum_targets: 5, overflow: "block", track_assignments: true },
    },
    merge: {
      label: "Merge playlists",
      description: "Read multiple target lists as one combined destination.",
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "aggregate", write_mode: "direct", membership: "managed_only", order: "ignore", aggregate_capacity: 1000, maximum_targets: 1 },
    },
    limited: {
      label: "Limited account sharing",
      description: "Use bidirectional aggregate and split behaviour for providers with list limits.",
      values: { ...RULESET_DEFAULTS, direction: "bidirectional", read_mode: "aggregate", write_mode: "partition", membership: "managed_only", order: "ignore", per_endpoint_capacity: 250, aggregate_capacity: 1000, maximum_targets: 5, overflow: "block", track_assignments: true },
    },
    custom: {
      label: "Custom",
      description: "Start from the current settings and adjust the details yourself.",
      values: { ...RULESET_DEFAULTS },
    },
  };
  const ENUMS = {
    direction: [["one_way", "One way"], ["bidirectional", "Bidirectional"]],
    initial_sync: [["source_authoritative", "Source authoritative"]],
    read_mode: [["direct", "Direct"], ["aggregate", "Aggregate"]],
    write_mode: [["direct", "Direct"], ["partition", "Partition"]],
    membership: [["add_only", "Add only"], ["managed_only", "Managed only"], ["mirror", "Mirror"]],
    order: [["ignore", "Ignore"], ["preserve", "Preserve"]],
    deduplicate: [["canonical_id", "Canonical ID"]],
    allocation: [["stable_first_fit", "Stable first fit"]],
    rebalance: [["never", "Never"]],
    overflow: [["block", "Block"]],
  };

  async function request(url, opt) {
    const res = await fetch(url, { cache: "no-store", headers: { "Content-Type": "application/json" }, ...(opt || {}) });
    let data = null;
    try { data = await res.json(); } catch { data = null; }
    if (!res.ok || (data && data.ok === false)) {
      const err = new Error((data && (data.error || data.detail)) || `${res.status} ${res.statusText}`);
      err.data = data;
      throw err;
    }
    return data || {};
  }

  const API = {
    providers: () => request(`${BASE}/providers`),
    resources: (provider, instance) => request(`${BASE}/resources?provider=${encodeURIComponent(provider)}&instance=${encodeURIComponent(instance || "default")}`),
    resourceCreate: (body) => request(`${BASE}/resources`, { method: "POST", body: JSON.stringify(body) }),
    resourceRename: (provider, instance, id, name) => request(`${BASE}/resources/${encodeURIComponent(id)}`, { method: "PATCH", body: JSON.stringify({ provider, instance: instance || "default", name }) }),
    resourceDelete: (provider, instance, id) => request(`${BASE}/resources/${encodeURIComponent(id)}?provider=${encodeURIComponent(provider)}&instance=${encodeURIComponent(instance || "default")}`, { method: "DELETE" }),
    overview: () => request(`${BASE}/overview`),
    activity: () => request(`${BASE}/activity`),
    activityClear: () => request(`${BASE}/activity`, { method: "DELETE" }),
    endpoints: () => request(`${BASE}/endpoints`),
    epUpsert: (body) => request(`${BASE}/endpoints`, { method: "POST", body: JSON.stringify(body) }),
    epDelete: (id) => request(`${BASE}/endpoints/${encodeURIComponent(id)}`, { method: "DELETE" }),
    epSync: (id) => request(`${BASE}/endpoints/${encodeURIComponent(id)}/sync`, { method: "POST" }),
    mappings: () => request(`${BASE}/mappings`),
    mapUpsert: (body) => request(`${BASE}/mappings`, { method: "POST", body: JSON.stringify(body) }),
    mapDelete: (id) => request(`${BASE}/mappings/${encodeURIComponent(id)}`, { method: "DELETE" }),
    run: (id) => request(`${BASE}/mappings/${encodeURIComponent(id)}/run`, { method: "POST" }),
    runPair: (id) => request("/api/run", { method: "POST", body: JSON.stringify({ pair_id: id }) }),
    runSummary: () => request("/api/run/summary"),
    pairMappings: (id) => request(`${BASE}/pairs/${encodeURIComponent(id)}/mappings`),
    rulesets: () => request(`${BASE}/rulesets`),
    rulesetUpsert: (body) => request(`${BASE}/rulesets`, { method: "POST", body: JSON.stringify(body) }),
    rulesetDelete: (id) => request(`${BASE}/rulesets/${encodeURIComponent(id)}`, { method: "DELETE" }),
  };

  const $ = (sel, root) => (root || document).querySelector(sel);
  const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));
  const esc = (v) => String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const val = (sel, root) => { const el = $(sel, root); return el ? String(el.value || "").trim() : ""; };
  const checked = (sel, root) => { const el = $(sel, root); return !!(el && el.checked); };
  const selectedValues = (sel, root) => {
    const el = $(sel, root);
    return el ? Array.from(el.selectedOptions || []).map((o) => o.value).filter(Boolean) : [];
  };
  const PM = () => (window.CW && window.CW.ProviderMeta) || null;
  const providerTone = (provider) => (PM() ? PM().tone(provider) : null) || { solid: "#7c5cff", rgb: "124,92,255" };
  const providerLogo = (provider) => (PM() ? PM().logoPath(provider) : "") || "";
  const providerLabel = (provider) => {
    const found = state.providers.find((p) => p.provider === String(provider || "").toUpperCase());
    if (found) return found.label || found.provider;
    return PM() ? PM().label(provider) : provider;
  };
  const ruleLabel = (key) => key === "one_way" ? "One way" : key === "bidirectional" ? "Bidirectional" : titleize(key);
  const titleize = (v) => String(v || "").replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const timeText = (ts) => ts ? new Date(Number(ts) * 1000).toLocaleString() : "Never";
  const compactTime = (ts) => ts ? new Date(Number(ts) * 1000).toLocaleString() : "-";
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));


  const state = {
    providers: [],
    endpoints: [],
    mappings: [],
    rulesets: [],
    overview: {},
    activity: [],
    runningEndpoints: new Set(),
    runningMappings: new Set(),
    syncSummary: null,
    syncPollTimer: 0,
    syncPollBusy: false,
    localSyncStartedAt: 0,
    syncObservedRunning: false,
    modal: null,
    loaded: false,
    loading: false,
    error: "",
  };


  function icon(provider) {
    const tone = providerTone(provider);
    const logo = providerLogo(provider);
    return `<span class="pl-provider-icon" style="--rgb:${esc(tone.rgb)}">${logo ? `<img src="${esc(logo)}" alt="">` : `<b>${esc(String(provider || "?").slice(0, 2))}</b>`}</span>`;
  }

  function endpointRef(item, fallback) {
    const provider = item && item.provider ? item.provider : "";
    const name = (item && (item.name || item.label || item.endpoint_id || item.id)) || fallback || "-";
    const sub = (item && (item.playlist_name || item.provider_label || item.provider)) || "";
    return `<div class="pl-provider">${icon(provider)}<div><div class="pl-main-text">${esc(name)}</div><div class="pl-muted">${esc(sub)}</div></div></div>`;
  }

  function mappingTargetRefs(mapping) {
    const ids = mapping.target_endpoints || [];
    const endpoints = ids.map((id) => endpointById(id)).filter(Boolean);
    const targets = endpoints.length ? endpoints : (Array.isArray(mapping.targets) ? mapping.targets.slice() : []);
    if (!targets.length && mapping.target) targets.push(mapping.target);
    if (!targets.length) return `<div class="pl-muted">-</div>`;
    return `<div class="pl-endpoint-stack">${targets.map((t, i) => endpointRef(t, ids[i] || "")).join("")}</div>`;
  }

  function actionButton(action, id, label, iconName, tone, extraClass, disabled = false) {
    return `<button class="pl-action-btn ${esc(tone || "")} ${esc(extraClass || "")}" data-action="${esc(action)}" data-id="${esc(id || "")}" title="${esc(label)}" aria-label="${esc(label)}" ${disabled ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">${esc(iconName)}</span></button>`;
  }

  function rulesetActionButton(action, id, label, iconName, tone = "", disabled = false) {
    return `<button class="pl-action-btn ${esc(tone)}" data-ruleset-action="${esc(action)}" data-id="${esc(id || "")}" title="${esc(label)}" aria-label="${esc(label)}" ${disabled ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">${esc(iconName)}</span></button>`;
  }

  function endpointById(id) {
    return state.endpoints.find((ep) => ep.id === id) || null;
  }

  function endpointIsDiscovery(endpoint) {
    const ep = endpoint || {};
    const typ = String(ep.playlist_type || ep.source_kind || ep.endpoint_type || "").toLowerCase();
    return !!ep.discovery || typ === "discovery";
  }

  function endpointWritable(endpoint) {
    const ep = endpoint || {};
    if (endpointIsDiscovery(ep) || ep.smart) return false;
    if (!ep.playlist_id && ep.pending_create) return true;
    if (!ep.playlist_id) return false;
    if ("can_add" in ep || "can_remove" in ep) return !!(ep.can_add || ep.can_remove);
    return true;
  }

  function rulesetById(id) {
    return state.rulesets.find((rs) => rs.id === id) || null;
  }

  function creatableEndpointTypes(provider) {
    const key = String(provider || "").toUpperCase();
    const found = state.providers.find((p) => String(p.provider || "").toUpperCase() === key);
    const types = (found && found.create_endpoint_types) || [];
    return types.length ? types : ["playlist"];
  }

  function configuredProviders() {
    const seen = new Set();
    return state.providers.filter((p) => {
      if (!p.configured || seen.has(p.provider)) return false;
      seen.add(p.provider);
      return true;
    });
  }

  function hasCompatiblePlaylistProvider() {
    return state.providers.some((p) => p && p.configured && PLAYLIST_COMPATIBLE_PROVIDERS.has(String(p.provider || "").toUpperCase()));
  }

  function renderBanners() {
    const errorGate = state.error ? `
      <div class="pl-banner warn">
        <span class="material-symbols-rounded" aria-hidden="true">warning</span>
        <span>${esc(state.error)}</span>
      </div>
    ` : "";
    const providerGate = state.loaded && !hasCompatiblePlaylistProvider() ? `
      <div class="pl-banner warn">
        <span class="material-symbols-rounded" aria-hidden="true">warning</span>
        <span>Playlists need at least one compatible provider (Plex, Trakt, MDBList, Jellyfin, Emby, PublicMetaDB, SIMKL or CrossWatch). Configure one in Connections to enable playlist endpoints and mappings.</span>
        <button type="button" class="pl-btn small" data-action="open-connections">Open Connections</button>
      </div>
    ` : "";
    if (!errorGate && !providerGate) return "";
    return `
      <div class="pl-banners">
        ${errorGate}
        ${providerGate}
      </div>
    `;
  }

  function instancesFor(provider) {
    return state.providers.filter((p) => p.configured && p.provider === String(provider || "").toUpperCase()).map((p) => p.instance || "default");
  }

  function mappingUsesEndpoint(mapping, endpointId) {
    return mapping.source_endpoint === endpointId || (mapping.target_endpoints || []).includes(endpointId);
  }

  function mappingsForRuleset(id) {
    return state.mappings.filter((m) => (m.ruleset_id || "") === id);
  }

  function targetNames(mapping) {
    const ids = mapping.target_endpoints || [];
    return ids.map((id) => {
      const ep = endpointById(id);
      return ep ? ep.name : id;
    }).join(", ");
  }

  function directionFor(mapping) {
    const rs = mapping.ruleset || rulesetById(mapping.ruleset_id || "");
    return rs ? ruleLabel(rs.direction) : "One way";
  }

  function syncSummaryRunning(summary = state.syncSummary) {
    const s = summary || {};
    const timeline = s.timeline && typeof s.timeline === "object" ? s.timeline : {};
    if (s.running === true || String(s.state || "").toLowerCase() === "running") return true;
    if ((timeline.start || timeline.started || s.started_at || s.raw_started_ts) && !(timeline.done || timeline.finished || timeline.complete || s.finished_at || s.exit_code != null)) return true;
    try { if (window.CW?.syncBar?.isRunning?.() || window.syncBar?.isRunning?.()) return true; } catch {}
    return false;
  }

  function syncSummaryPairScopeIds(summary = state.syncSummary) {
    const raw = summary && Array.isArray(summary.pair_scope_ids) ? summary.pair_scope_ids : [];
    return new Set(raw.map((value) => String(value || "").trim()).filter(Boolean));
  }

  function sharedSyncBusy() {
    return state.runningMappings.size > 0 || syncSummaryRunning();
  }

  function mappingIsRunning(mapping) {
    const id = String((mapping && mapping.id) || "");
    if (id && state.runningMappings.has(id)) return true;
    if (!syncSummaryRunning()) return false;
    const pair = String((mapping && mapping.assigned_pair) || "").trim();
    if (!pair) return false;
    const scope = syncSummaryPairScopeIds();
    return scope.size > 0 && scope.has(pair);
  }

  function reconcileSyncSummary() {
    if (syncSummaryRunning()) {
      state.syncObservedRunning = true;
      return;
    }
    if (!state.runningMappings.size) return;
    if (state.syncObservedRunning || Date.now() - state.localSyncStartedAt > 4000) {
      state.runningMappings.clear();
      state.syncObservedRunning = false;
      state.localSyncStartedAt = 0;
    }
  }

  function scheduleSyncSummaryPoll(delayMs) {
    if (state.syncPollTimer) clearTimeout(state.syncPollTimer);
    const root = $("#page-playlists");
    if (!root || !root.querySelector(".pl-page")) return;
    state.syncPollTimer = setTimeout(() => {
      state.syncPollTimer = 0;
      refreshSyncSummary().catch(() => scheduleSyncSummaryPoll(6000));
    }, delayMs);
  }

  async function refreshSyncSummary(renderOnly = true) {
    if (state.syncPollBusy) return state.syncSummary;
    state.syncPollBusy = true;
    const wasBusy = sharedSyncBusy();
    try {
      state.syncSummary = await API.runSummary();
      reconcileSyncSummary();
      const root = $("#page-playlists");
      if (root && root.querySelector(".pl-page")) {
        if (wasBusy && !sharedSyncBusy()) await refreshOverview(["endpoints", "mappings", "activity"]);
        else {
          updateMappingActions(root);
          refreshSection(root, "mappings");
        }
      }
      return state.syncSummary;
    } finally {
      state.syncPollBusy = false;
      if (renderOnly) scheduleSyncSummaryPoll(syncSummaryRunning() || state.runningMappings.size ? 1500 : 6000);
    }
  }

  function statusForResult(result) {
    if (!result) return `<span class="pl-pill off">No runs</span>`;
    const unresolved = Number(result.unresolved_count || result.unresolved || 0);
    const errors = Number(result.errors || result.error_count || 0);
    const warningBits = [];
    if (unresolved > 0) warningBits.push(`${unresolved} unresolved`);
    const warnings = Array.isArray(result.warnings) ? result.warnings.length : 0;
    if (warnings) warningBits.push(`${warnings} warning${warnings === 1 ? "" : "s"}`);
    if (result.capacity_error || errors > 0 || (result.ok === false && !warningBits.length)) return `<span class="pl-pill err">Failed</span>`;
    if (warningBits.length) return `<span class="pl-pill warn" title="${esc(warningBits.join(", "))}"><span class="material-symbols-rounded" aria-hidden="true">warning</span>${esc(warningBits[0])}</span>`;
    return `<span class="pl-pill ok">Success</span>`;
  }

  function endpointStatus(endpoint) {
    if (!endpoint.playlist_id) return `<span class="pl-state warn"><span class="material-symbols-rounded" aria-hidden="true">warning</span>Incomplete</span>`;
    return `<span class="pl-state ok"><span class="material-symbols-rounded" aria-hidden="true">check_circle</span>Connected</span>`;
  }

  function endpointIdentity(endpoint, sub) {
    const ep = endpoint || {};
    const tone = providerTone(ep.provider);
    return `
      <div class="pl-entity">
        <span class="pl-entity-icon" style="--rgb:${esc(tone.rgb)}"><span class="material-symbols-rounded" aria-hidden="true">share</span></span>
        <div><div class="pl-main-text">${esc(ep.name || ep.playlist_name || ep.id || "-")}</div><div class="pl-muted">${esc(sub || ep.id || "")}</div></div>
      </div>
    `;
  }

  function statusPill(kind, label, iconName = "radio_button_checked") {
    return `<span class="pl-pill ${esc(kind || "")}"><span class="material-symbols-rounded" aria-hidden="true">${esc(iconName)}</span>${esc(label)}</span>`;
  }

  function syncPairIndicator(pairId) {
    const id = String(pairId || "").trim();
    if (!id) {
      return `<span class="pl-icon-status warn" title="No sync pair assigned" aria-label="No sync pair assigned"><span class="material-symbols-rounded" aria-hidden="true">warning</span></span>`;
    }
    return `<span class="pl-icon-status ok" title="${esc(id)}" aria-label="Sync pair: ${esc(id)}"><span class="material-symbols-rounded" aria-hidden="true">check_circle</span></span>`;
  }

  function statCard(value, label, iconName, tone) {
    return `<div class="pl-stat ${esc(tone || "")}"><span class="pl-stat-icon material-symbols-rounded" aria-hidden="true">${esc(iconName)}</span><b>${esc(value)}</b><span>${esc(label)}</span></div>`;
  }

  function activityChangeChip(label, value, iconName, kind) {
    const count = Number(value || 0);
    return `<span class="pl-change-chip ${esc(kind || "")}" title="${esc(label)}: ${esc(count)}" aria-label="${esc(label)}: ${esc(count)}"><span class="material-symbols-rounded" aria-hidden="true">${esc(iconName)}</span>${esc(count)}</span>`;
  }

  function activityChangeSummary(row) {
    const counts = parseActivityCounts(row);
    const chips = [];
    if (counts.unresolved) chips.push(activityChangeChip("Unresolved", counts.unresolved, "help", "unresolved"));
    if (counts.added) chips.push(activityChangeChip("Added", counts.added, "add_circle", "added"));
    if (counts.updated) chips.push(activityChangeChip("Updated", counts.updated, "sync", "updated"));
    if (counts.removed) chips.push(activityChangeChip("Removed", counts.removed, "remove_circle", "removed"));
    if (counts.skipped) chips.push(activityChangeChip("Skipped", counts.skipped, "skip_next", "skipped"));
    return `<div class="pl-change-set">${chips.length ? chips.join("") : `<span class="pl-muted">No changes</span>`}</div>`;
  }

  function activityStatus(row) {
    const status = String(row.status || "").toLowerCase();
    if (status === "error" || status === "failed") return statusPill("err", titleize(status || "error"), "cancel");
    if (status === "warning" || parseActivityCounts(row).unresolved > 0) return statusPill("warn", "Warning", "warning");
    return statusPill("ok", titleize(row.status || "completed"), "check_circle");
  }

  function activityResultSummary(row) {
    const details = String(row.details || "");
    const rule = row.ruleset || (details.split(",")[0] || "direct");
    const rawTargets = row.target_count != null ? Number(row.target_count) : Number((details.match(/(\d+)\s+target/i) || [])[1] || 0);
    const targetText = rawTargets === 1 ? "1 target" : `${rawTargets} targets`;
    return `<div class="pl-main-text">${esc(titleize(rule || "direct"))}</div><div class="pl-muted">${esc(targetText)}</div>`;
  }

  function render(root) {
    const mappingDisabled = !state.loaded || state.endpoints.length < 2;
    const mappingTitle = !state.loaded ? "Playlist data is still loading." : mappingDisabled ? "Create at least two endpoints before adding a mapping." : "Create playlist mapping";
    root.innerHTML = `
      <div class="pl-page">
        <div class="pl-header cw-page-hero cw-page-hero-playlists" data-hero-icon="queue_music">
          <div class="cw-page-hero-copy">
            <div class="cw-page-hero-kicker">PLAYLISTS</div>
            <h2 class="pl-title cw-page-hero-title">Playlists</h2>
            <div class="pl-sub cw-page-hero-sub">Sync your playlists between services</div>
          </div>
          <div class="pl-header-actions cw-page-hero-actions">
            <button class="pl-btn" id="pl-new-endpoint"><span class="material-symbols-rounded" aria-hidden="true">add</span>New endpoint</button>
            <button class="pl-btn" id="pl-new-mapping" ${mappingDisabled ? "disabled" : ""} title="${esc(mappingTitle)}"><span class="material-symbols-rounded" aria-hidden="true">add</span>New mapping</button>
          </div>
        </div>
        ${renderBanners()}
        <main class="pl-grid">
          <section class="pl-section" id="pl-playlist-endpoints">
            <div class="pl-section-head">
              <div><div class="pl-section-title">Playlist endpoints</div><div class="pl-section-sub">Connect provider playlists to use in CrossWatch.</div></div>
              <button class="pl-btn small accent" data-action="endpoint-new"><span class="material-symbols-rounded" aria-hidden="true">add</span>Add endpoint</button>
            </div>
            <div class="pl-section-body">${renderEndpoints()}</div>
          </section>
          <section class="pl-section" id="pl-mappings-overview">
            <div class="pl-section-head">
              <div><div class="pl-section-title">Mappings</div><div class="pl-section-sub">Sync relationships between playlist endpoints.</div></div>
              <button class="pl-btn small accent" data-action="mapping-new" ${mappingDisabled ? "disabled" : ""} title="${esc(mappingTitle)}"><span class="material-symbols-rounded" aria-hidden="true">add</span>New mapping</button>
            </div>
            <div class="pl-section-body">${renderMappings()}</div>
          </section>
          <section class="pl-section" id="pl-activity-overview">
            <div class="pl-section-head">
              <div><div class="pl-section-title">Activity overview</div></div>
              <div class="pl-section-actions">
                <button class="pl-btn small" data-action="activity-all">View all activity</button>
                <button class="pl-btn small danger" data-action="activity-clear" title="Clear playlist activity"><span class="material-symbols-rounded" aria-hidden="true">delete_sweep</span>Clear</button>
              </div>
            </div>
            <div class="pl-section-body">${renderActivity()}</div>
          </section>
        </main>
      </div>
    `;
    wirePage(root);
  }

  function renderEndpoints() {
    if (state.loading && !state.loaded) return renderSkeleton("Loading playlist endpoints");
    if (!state.endpoints.length) {
      return `<div class="pl-empty"><strong>No playlist endpoints yet</strong><span>Add the first provider playlist before creating mappings.</span></div>`;
    }
    const rows = state.endpoints.map((ep) => {
      const playlistType = endpointIsDiscovery(ep) ? "discovery" : (ep.playlist_type || ep.endpoint_type || ep.kind || ep.media_type || "playlist");
      const usedBy = state.mappings.filter((m) => mappingUsesEndpoint(m, ep.id)).length;
      const refreshing = state.runningEndpoints.has(String(ep.id || ""));
      return `
        <tr>
          <td>${endpointIdentity(ep)}</td>
          <td><div class="pl-provider">${icon(ep.provider)}<div><div class="pl-main-text">${esc(ep.provider_label || providerLabel(ep.provider))}</div><div class="pl-muted">${esc(ep.provider || "")}</div></div></div></td>
          <td>${esc(ep.instance || "default")}</td>
          <td><div class="pl-main-text">${esc(ep.playlist_name || ep.playlist_id || "-")}</div></td>
          <td><span class="pl-pill ${endpointIsDiscovery(ep) ? "run" : "type"}">${esc(titleize(playlistType))}</span></td>
          <td>${endpointStatus(ep)}</td>
          <td><div class="pl-muted">${refreshing ? "Refreshing..." : ep.last_synced ? `Refreshed ${esc(timeText(ep.last_synced))}` : "Not refreshed yet"}${ep.item_count != null ? `<br>${esc(ep.item_count)} items` : ""}</div></td>
          <td>
            <div class="pl-actions">
              ${actionButton("endpoint-sync", ep.id, refreshing ? "Refresh running" : "Refresh endpoint", "refresh", "refresh", refreshing ? "running" : "", refreshing)}
              ${actionButton("endpoint-edit", ep.id, "Edit endpoint", "edit", "edit")}
              ${actionButton("endpoint-delete", ep.id, usedBy ? `Delete endpoint. ${usedBy} mapping(s) use this endpoint` : "Delete endpoint", "delete", "delete")}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap">
        <table class="pl-endpoints-table">
          <thead><tr><th>Endpoint</th><th>Provider</th><th>Profile</th><th>Selected playlist</th><th>Type</th><th>Status</th><th>Last refresh</th><th aria-label="Actions">Actions</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderMappings() {
    if (state.loading && !state.loaded) return renderSkeleton("Loading playlist mappings");
    if (!state.mappings.length) {
      const need = state.endpoints.length < 2;
      return `<div class="pl-empty"><strong>No mappings yet</strong><span>${need ? "At least two endpoints are required before a mapping can be created." : "Create a mapping to sync playlists between endpoints."}</span></div>`;
    }
    const rows = state.mappings.map((m) => {
      const src = endpointById(m.source_endpoint) || m.source || {};
      const rule = m.ruleset || rulesetById(m.ruleset_id || "");
      const res = m.last_result || null;
      const busy = sharedSyncBusy();
      const running = mappingIsRunning(m);
      const syncTitle = running ? "Sync running" : (busy ? "Synchronization is already running" : "Sync now");
      return `
        <tr class="${running ? "is-running" : ""}">
          <td>${endpointIdentity({ name: m.name || m.id, id: m.id, provider: (src && src.provider) || "" }, m.id)}</td>
          <td>${endpointRef(src, m.source_endpoint)}</td>
          <td><span class="pl-pill dir">${esc(directionFor(m))}<span class="material-symbols-rounded" aria-hidden="true">east</span></span></td>
          <td>${mappingTargetRefs(m)}</td>
          <td><span class="pl-pill">${esc(rule ? rule.name : "Direct")}</span></td>
          <td>${syncPairIndicator(m.assigned_pair)}</td>
          <td>${m.enabled ? statusPill("ok", "Enabled") : statusPill("off", "Disabled", "radio_button_unchecked")}</td>
          <td><div class="pl-result-cell">${running ? statusPill("run", "Running", "sync") : statusForResult(res)}<div class="pl-muted">${running ? "Running now..." : compactTime(res && res.finished_at)}</div></div></td>
          <td>
            <div class="pl-actions">
              ${actionButton("mapping-sync", m.id, syncTitle, "sync", "sync", running ? "running" : "", busy)}
              ${actionButton("mapping-edit", m.id, "Edit mapping", "edit", "edit")}
              ${actionButton("mapping-toggle", m.id, m.enabled ? "Disable mapping" : "Enable mapping", m.enabled ? "pause" : "play_arrow", "toggle", m.enabled ? "on" : "")}
              ${actionButton("mapping-delete", m.id, "Delete mapping", "delete", "delete")}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap">
        <table class="pl-mappings-table">
          <thead><tr><th>Mapping</th><th>Source</th><th>Direction</th><th>Destination</th><th>Ruleset</th><th>Sync pair</th><th>Status</th><th>Result</th><th aria-label="Actions">Actions</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderActivity() {
    if (state.loading && !state.loaded) return renderSkeleton("Loading playlist activity");
    const entries = state.activity || [];
    const stats = activityStats(entries);
    const latest = entries.filter((row) => String(row.type || "").toLowerCase() === "run").slice(0, 8);
    const table = latest.length ? `
      <div class="pl-table-wrap">
        <table class="pl-activity-table">
          <thead><tr><th>Time</th><th>Mapping</th><th>Result</th><th>Changes</th><th>Status</th></tr></thead>
          <tbody>${latest.map((row) => {
            return `<tr><td>${esc(compactTime(row.ts))}</td><td><div class="pl-main-text">${esc(row.label || "-")}</div></td><td>${activityResultSummary(row)}</td><td>${activityChangeSummary(row)}</td><td>${activityStatus(row)}</td></tr>`;
          }).join("")}</tbody>
        </table>
      </div>
    ` : `<div class="pl-empty"><strong>No playlist runs yet</strong><span>Run a mapping to see sync results here.</span></div>`;
    return `
      <div class="pl-activity">
        <div class="pl-stats">
          ${statCard(stats.total, "Total runs", "trending_up", "total")}
          ${statCard(stats.success, "Successful", "check_circle", "success")}
          ${statCard(stats.warning, "Warnings", "warning", "warning")}
          ${statCard(stats.failed, "Failed", "cancel", "failed")}
          ${statCard(stats.running, "Queued or running", "speed", "running")}
          ${statCard(stats.skipped, "Skipped", "sync_disabled", "skipped")}
        </div>
        ${table}
      </div>
    `;
  }

  function activityStats(rows) {
    const runRows = rows.filter((row) => String(row.type || "").toLowerCase() === "run");
    const stats = { total: runRows.length, success: 0, warning: 0, failed: 0, running: 0, skipped: 0 };
    runRows.forEach((row) => {
      const status = String(row.status || "").toLowerCase();
      if (status === "error" || status === "failed") stats.failed += 1;
      else if (status === "queued" || status === "running") stats.running += 1;
      else if (status === "skipped") stats.skipped += 1;
      else if (status === "warning") stats.warning += 1;
      else if (String(row.details || "").toLowerCase().includes("warning")) stats.warning += 1;
      else stats.success += 1;
    });
    return stats;
  }

  function parseActivityCounts(row) {
    const details = String(row.details || "");
    const add = details.match(/\+(\d+)/);
    const rem = details.match(/-(\d+)/);
    const unresolved = details.match(/(\d+)\s+unresolved/i);
    return {
      added: row.added != null ? Number(row.added) : (add ? Number(add[1]) : 0),
      updated: row.updated != null ? Number(row.updated) : 0,
      removed: row.removed != null ? Number(row.removed) : (rem ? Number(rem[1]) : 0),
      skipped: row.skipped != null ? Number(row.skipped) : 0,
      unresolved: row.unresolved != null ? Number(row.unresolved) : (unresolved ? Number(unresolved[1]) : 0),
    };
  }

  function renderSkeleton(label) {
    return `<div class="pl-skeleton" aria-label="${esc(label)}"><div class="pl-skeleton-row"></div><div class="pl-skeleton-row"></div><div class="pl-skeleton-row"></div></div>`;
  }

  function wirePage(root) {
    $("#pl-new-endpoint", root)?.addEventListener("click", (e) => openEndpointModal({ trigger: e.currentTarget }));
    $("#pl-new-mapping", root)?.addEventListener("click", (e) => openMappingModal({ trigger: e.currentTarget }));
    if (!root.__plActionWired) {
      root.addEventListener("click", onPageClick);
      root.__plActionWired = true;
    }
  }

  function onPageClick(e) {
    const btn = e.target.closest("[data-action]");
    if (!btn) return;
    const action = btn.dataset.action;
    const id = btn.dataset.id || "";
    if (action === "endpoint-new") openEndpointModal({ trigger: btn });
    if (action === "endpoint-edit") openEndpointModal({ endpoint: endpointById(id), trigger: btn });
    if (action === "endpoint-delete") openEndpointDelete(endpointById(id), btn);
    if (action === "endpoint-sync") syncEndpoint(endpointById(id), btn);
    if (action === "mapping-new") openMappingModal({ trigger: btn });
    if (action === "mapping-edit") openMappingModal({ mapping: state.mappings.find((m) => m.id === id), trigger: btn });
    if (action === "mapping-toggle") toggleMapping(state.mappings.find((m) => m.id === id), btn);
    if (action === "mapping-delete") openMappingDelete(state.mappings.find((m) => m.id === id), btn);
    if (action === "mapping-sync") syncMapping(state.mappings.find((m) => m.id === id), btn);
    if (action === "activity-all") openActivityModal(btn);
    if (action === "activity-clear") openActivityClear(btn);
    if (action === "open-connections") openConnections();
  }

  function openConnections() {
    if (typeof window.cwSettingsMenuSelect === "function") return window.cwSettingsMenuSelect("providers");
    window.showTab?.("settings");
    setTimeout(() => window.cwSettingsSelect?.("providers"), 0);
  }

  function openModal(opts) {
    closeModal(true);
    const host = $("#page-playlists") || document.body;
    const modal = document.createElement("div");
    modal.className = "pl-modal";
    modal.innerHTML = `
      <div class="pl-dialog" role="dialog" aria-modal="true" aria-labelledby="pl-dialog-title" style="--modal-width:${esc(opts.width || "880px")}">
        <div class="pl-dialog-head">
          <div><div class="pl-dialog-title" id="pl-dialog-title">${esc(opts.title || "")}</div><div class="pl-dialog-sub">${esc(opts.description || "")}</div></div>
          <button class="pl-btn icon" data-modal-close aria-label="Close">x</button>
        </div>
        <div class="pl-dialog-body">
          <div class="pl-dialog-error" data-modal-error></div>
          ${opts.body || ""}
        </div>
        <div class="pl-dialog-foot">
          <button class="pl-btn" data-modal-cancel>${esc(opts.cancelText || "Cancel")}</button>
          ${opts.primaryText ? `<button class="pl-btn primary" data-modal-primary>${esc(opts.primaryText)}</button>` : ""}
        </div>
      </div>
    `;
    host.appendChild(modal);
    document.body.classList.add("cx-modal-open");
    const ctx = { modal, opts, dirty: false, saving: false, initial: "" };
    state.modal = ctx;
    const formRoot = $(".pl-dialog-body", modal);
    ctx.initial = snapshot(formRoot);
    modal.addEventListener("input", () => { ctx.dirty = snapshot(formRoot) !== ctx.initial; });
    modal.addEventListener("change", () => { ctx.dirty = snapshot(formRoot) !== ctx.initial; });
    modal.addEventListener("click", (e) => {
      if (e.target.closest("[data-modal-close]") || e.target.closest("[data-modal-cancel]")) closeModal(false);
    });
    const primary = $("[data-modal-primary]", modal);
    if (primary && opts.onPrimary) {
      primary.addEventListener("click", async () => {
        if (ctx.saving) return;
        setModalError("");
        ctx.saving = true;
        primary.disabled = true;
        primary.textContent = opts.savingText || "Saving...";
        try {
          await opts.onPrimary(ctx);
        } catch (err) {
          setModalError(err && err.message ? err.message : String(err || "Save failed"));
          ctx.saving = false;
          primary.textContent = opts.primaryText;
          syncModalPrimary(ctx);
        }
      });
    }
    if (opts.onOpen) opts.onOpen(ctx);
    const focusable = $$("button,input,select,[tabindex]:not([tabindex='-1'])", modal).find((el) => !el.disabled);
    setTimeout(() => (focusable || modal).focus(), 0);
    document.addEventListener("keydown", onModalKey);
    return ctx;
  }

  function snapshot(root) {
    return $$("input,select", root).map((el) => {
      if (el.type === "checkbox") return `${el.id}:${el.checked}`;
      if (el.multiple) return `${el.id}:${selectedValues(`#${el.id}`, root).join("|")}`;
      return `${el.id}:${el.value}`;
    }).join(";");
  }

  function onModalKey(e) {
    if (e.key === "Escape") closeModal(false);
  }

  function closeModal(force) {
    const ctx = state.modal;
    if (!ctx) return true;
    if (!force && ctx.dirty && !confirm("Discard unsaved changes?")) return false;
    document.removeEventListener("keydown", onModalKey);
    ctx.modal.remove();
    document.body.classList.remove("cx-modal-open");
    state.modal = null;
    if (ctx.opts && ctx.opts.trigger && typeof ctx.opts.trigger.focus === "function") ctx.opts.trigger.focus();
    if (!force && ctx.opts && ctx.opts.onCancel) ctx.opts.onCancel();
    return true;
  }

  function setModalError(message) {
    const box = state.modal && $("[data-modal-error]", state.modal.modal);
    if (!box) return;
    box.style.display = message ? "block" : "none";
    box.textContent = message || "";
  }

  function selectOptions(items, selected, empty) {
    const set = new Set(Array.isArray(selected) ? selected : [selected]);
    return `${empty ? `<option value="">${esc(empty)}</option>` : ""}${items.map((item) => `<option value="${esc(item.value)}" ${set.has(item.value) ? "selected" : ""}>${esc(item.label)}</option>`).join("")}`;
  }

  function shortName(v) {
    return String(v || "").trim().slice(0, NAME_MAX);
  }

  function sequenceNumber(value, prefix) {
    const head = `${String(prefix || "").toUpperCase()}-`;
    const text = String(value || "").trim().toUpperCase();
    if (!head || !text.startsWith(head)) return 0;
    const tail = text.slice(head.length);
    if (!/^\d+$/.test(tail)) return 0;
    const number = Number(tail);
    return Number.isFinite(number) && number > 0 ? number : 0;
  }

  function nextSequenceName(items, prefix, offset = 0, ignoreId = "") {
    const used = new Set();
    const skippedId = String(ignoreId || "");
    (items || []).forEach((item) => {
      if (!item) return;
      if (skippedId && String(item.id || "") === skippedId) return;
      [item.id, item.name].forEach((value) => {
        const number = sequenceNumber(value, prefix);
        if (number) used.add(number);
      });
    });
    let remaining = Math.max(0, Number(offset || 0));
    let number = 1;
    while (number < 10000) {
      if (!used.has(number)) {
        if (remaining === 0) return `${String(prefix || "").toUpperCase()}-${String(number).padStart(2, "0")}`;
        remaining -= 1;
      }
      number += 1;
    }
    return `${String(prefix || "").toUpperCase()}-${Date.now().toString().slice(-4)}`;
  }

  function nextEndpointName(offset = 0, ignoreId = "") {
    return nextSequenceName(state.endpoints, "EP", offset, ignoreId);
  }

  function nextMappingName(offset = 0, ignoreId = "") {
    return nextSequenceName(state.mappings, "MAP", offset, ignoreId);
  }

  function isSafeNameChar(ch) {
    return /^[\p{L}\p{N}]$/u.test(ch) || SAFE_NAME_CHARS.includes(ch);
  }

  function safeNameError(name, label, max) {
    const clean = String(name || "").trim();
    if (!clean) return `${label} is required.`;
    if (clean.length > max) return `${label} must be ${max} characters or fewer.`;
    if (!/^[\p{L}\p{N}]$/u.test(Array.from(clean)[0] || "")) return `${label} must start with a letter or number.`;
    if (!Array.from(clean).every(isSafeNameChar)) return `${label} can only use letters, numbers, spaces, hyphens, underscores, periods, apostrophes, ampersands, or parentheses.`;
    return "";
  }

  function nameFieldError(name, label) {
    return safeNameError(name, label, NAME_MAX);
  }

  function playlistNameError(name) {
    return safeNameError(name, "New playlist name", PLAYLIST_NAME_MAX);
  }

  function applyFieldError(input, err, okText) {
    if (!input) return;
    const field = input.closest(".pl-field");
    const described = String(input.getAttribute("aria-describedby") || "").split(/\s+/).filter(Boolean)[0];
    const feedback = described ? $(`#${described}`, input.ownerDocument) : null;
    if (field) field.classList.toggle("invalid", !!err);
    if (feedback) feedback.textContent = err || okText || "";
  }

  function syncModalPrimary(ctx) {
    const primary = $("[data-modal-primary]", ctx.modal);
    if (!primary || ctx.saving) return "";
    const errors = (ctx.validators || []).map((fn) => fn()).filter(Boolean);
    const err = errors[0] || "";
    primary.disabled = !!err;
    primary.title = err;
    return err;
  }

  function addModalValidator(ctx, validate) {
    ctx.validators = ctx.validators || [];
    ctx.validators.push(validate);
    syncModalPrimary(ctx);
  }

  function bindNameValidation(ctx, selector, label) {
    const input = $(selector, ctx.modal);
    if (!input) return () => "";
    const update = () => {
      const err = nameFieldError(input.value, label);
      applyFieldError(input, err, `${String(input.value || "").trim().length}/${NAME_MAX} characters`);
      return err;
    };
    input.addEventListener("input", () => syncModalPrimary(ctx));
    input.addEventListener("change", () => syncModalPrimary(ctx));
    addModalValidator(ctx, update);
    return update;
  }

  function generatedEndpointName(value, fallback) {
    const raw = String(value || fallback || "Endpoint").replace(/\s*\([^)]*\)\s*$/g, "").trim() || "Endpoint";
    let clean = Array.from(raw).filter(isSafeNameChar).join("").replace(/\s+/g, " ").trim();
    if (!clean) clean = "Endpoint";
    if (!/^[\p{L}\p{N}]$/u.test(Array.from(clean)[0] || "")) clean = `List ${clean}`;
    return shortName(clean);
  }

  function selectedPlaylistOptions(root) {
    const select = $("#pl-ep-playlist", root);
    return select ? Array.from(select.selectedOptions || []).filter((o) => o.value) : [];
  }

  function selectedPlaylistName(root) {
    const opt = selectedPlaylistOptions(root)[0];
    return opt ? opt.dataset.name || opt.textContent || opt.value : "";
  }

  function selectedEndpointResources(root) {
    const selected = new Set(selectedValues("#pl-ep-playlist", root));
    return (root.__plEndpointResources || []).filter((r) => selected.has(String(r.id || "")));
  }

  function endpointCreateMode(root) {
    return checked("#pl-ep-create", root);
  }

  function endpointManageMode(root) {
    return String(root.dataset.epManageMode || "");
  }

  function setEndpointCreateMode(root, enabled) {
    const createCheck = $("#pl-ep-create", root);
    const panel = $("#pl-ep-create-panel", root);
    const resourceWrap = $("#pl-ep-resource-wrap", root);
    if (enabled) setEndpointManageMode(root, "");
    if (createCheck) createCheck.checked = !!enabled;
    if (panel) panel.classList.toggle("hidden", !enabled);
    if (resourceWrap) resourceWrap.classList.toggle("is-create-mode", !!enabled);
    updateEndpointGeneratedName(root);
  }

  function setEndpointManageMode(root, mode, resources = []) {
    const next = String(mode || "");
    root.dataset.epManageMode = next;
    const editPanel = $("#pl-ep-edit-panel", root);
    const deletePanel = $("#pl-ep-delete-panel", root);
    const editName = $("#pl-ep-edit-name", root);
    const deleteList = $("#pl-ep-delete-list", root);
    if (next) {
      const createCheck = $("#pl-ep-create", root);
      const createPanel = $("#pl-ep-create-panel", root);
      if (createCheck) createCheck.checked = false;
      if (createPanel) createPanel.classList.add("hidden");
    }
    if (editPanel) editPanel.classList.toggle("hidden", next !== "edit");
    if (deletePanel) deletePanel.classList.toggle("hidden", next !== "delete");
    if (next === "edit" && resources[0]) {
      root.dataset.epEditResourceId = String(resources[0].id || "");
      if (editName) {
        editName.value = resources[0].name || resources[0].id || "";
        applyFieldError(editName, "", `${String(editName.value || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
      }
    } else {
      root.dataset.epEditResourceId = "";
    }
    if (next === "delete") {
      root.__plDeleteResourceIds = resources.map((r) => String(r.id || "")).filter(Boolean);
      if (deleteList) {
        deleteList.innerHTML = resources.map((r) => `<span>${esc(r.name || r.id || "-")}</span>`).join("");
      }
    } else {
      root.__plDeleteResourceIds = [];
    }
    updateEndpointPlaylistActions(root);
    syncEndpointCreatePanel(root);
    syncEndpointManagePanel(root);
    syncModalPrimary(root.__plEndpointCtx || state.modal);
  }

  function updateEndpointGeneratedName(root, force = false) {
    const input = $("#pl-ep-name", root);
    if (!input) return;
    if (!force && root.dataset.epNameDirty === "1") return;
    input.value = nextEndpointName(0, root.dataset.epEditId || "");
  }

  function resourceCaps(resource) {
    const caps = [];
    const endpointType = resource.discovery ? "discovery" : ((resource.extra && resource.extra.endpoint_type) || resource.endpoint_type || resource.playlist_type || resource.kind || "");
    if (endpointType) caps.push(endpointType);
    if (resource.media_types && resource.media_types.length) caps.push(resource.media_types.join("/"));
    if (resource.discovery || !resource.writable) caps.push("read-only");
    if (resource.smart) caps.push("smart");
    return caps;
  }

  function resourceBadgeMeta(resource) {
    const endpointType = String(resource.discovery ? "discovery" : ((resource.extra && resource.extra.endpoint_type) || resource.endpoint_type || resource.playlist_type || resource.kind || "playlist")).toLowerCase();
    const kind = String(resource.kind || "").toLowerCase();
    const mediaTypes = Array.isArray(resource.media_types) ? resource.media_types : [];
    const raw = [kind || endpointType, endpointType, ...mediaTypes, resource.discovery || !resource.writable ? "read-only" : "", resource.smart ? "smart" : ""].filter(Boolean);
    const seen = new Set();
    const map = {
      playlist: ["queue_music", "Playlist"],
      regular: ["list_alt", "Regular list"],
      watchlist: ["bookmark", "Watchlist"],
      discovery: ["travel_explore", "Discovery source"],
      collection: ["collections_bookmark", "Collection"],
      movie: ["movie", "Movies"],
      movies: ["movie", "Movies"],
      show: ["tv", "Shows"],
      shows: ["tv", "Shows"],
      season: ["video_library", "Seasons"],
      seasons: ["video_library", "Seasons"],
      episode: ["live_tv", "Episodes"],
      episodes: ["live_tv", "Episodes"],
      "read-only": ["lock", "Read only"],
      smart: ["auto_awesome", "Smart list"],
    };
    return raw.map((value) => {
      const key = String(value || "").toLowerCase();
      if (seen.has(key)) return null;
      seen.add(key);
      const meta = map[key] || ["label", titleize(key)];
      return { key, icon: meta[0], title: meta[1] };
    }).filter(Boolean);
  }

  function resourceIconName(resource) {
    if (resource.discovery) return "travel_explore";
    if (resource.smart) return "auto_awesome";
    if (String(resource.kind || "").toLowerCase() === "watchlist") return "bookmark";
    return "queue_music";
  }

  function renderEndpointResourceList(root) {
    const list = $("#pl-ep-resource-list", root);
    const select = $("#pl-ep-playlist", root);
    const search = val("#pl-ep-playlist-search", root).toLowerCase();
    if (!list || !select) return;
    const singleSelect = root.dataset.epSingleSelect === "1";
    const resources = root.__plEndpointResources || [];
    const selected = new Set(selectedValues("#pl-ep-playlist", root));
    const visible = resources.filter((r) => {
      const text = `${r.name || ""} ${r.id || ""} ${resourceCaps(r).join(" ")}`.toLowerCase();
      return !search || text.includes(search);
    });
    if (!resources.length) {
      list.innerHTML = `<div class="pl-resource-empty">No playlists found.</div>`;
      return;
    }
    if (!visible.length) {
      list.innerHTML = `<div class="pl-resource-empty">No playlists match this search.</div>`;
      return;
    }
    list.innerHTML = visible.map((r) => {
      const badges = resourceBadgeMeta(r);
      const active = selected.has(String(r.id || ""));
      const controlIcon = active ? (singleSelect ? "radio_button_checked" : "check_box") : (singleSelect ? "radio_button_unchecked" : "check_box_outline_blank");
      return `
        <button type="button" class="pl-resource-row ${active ? "selected" : ""}" data-playlist-id="${esc(r.id || "")}" aria-pressed="${active ? "true" : "false"}">
          <span class="pl-resource-check material-symbols-rounded" aria-hidden="true">${controlIcon}</span>
          <span class="pl-resource-type material-symbols-rounded" aria-hidden="true">${esc(resourceIconName(r))}</span>
          <span class="pl-resource-main">
            <span class="pl-resource-name">${esc(r.name || r.id || "-")}</span>
          </span>
          <span class="pl-resource-badges" aria-label="Playlist properties">
            ${badges.map((badge) => `<span class="pl-resource-badge material-symbols-rounded" title="${esc(badge.title)}" aria-label="${esc(badge.title)}" data-kind="${esc(badge.key)}">${esc(badge.icon)}</span>`).join("")}
          </span>
        </button>
      `;
    }).join("");
  }

  function updateEndpointPlaylistActions(root) {
    const provider = val("#pl-ep-provider", root);
    const isSimkl = String(provider || "").toUpperCase() === "SIMKL";
    const canCreate = !isSimkl && creatableEndpointTypes(provider).length > 0;
    const createBtn = $("#pl-ep-list-create", root);
    const editBtn = $("#pl-ep-list-edit", root);
    const deleteBtn = $("#pl-ep-list-delete", root);
    const managing = !!endpointManageMode(root);
    const creating = endpointCreateMode(root);
    const selected = selectedEndpointResources(root);
    const editOk = !creating && !managing && selected.length === 1 && !!selected[0].can_rename;
    const deleteOk = !creating && !managing && selected.length > 0 && selected.every((r) => !!r.can_delete);
    if (createBtn) {
      createBtn.disabled = !canCreate || managing;
      createBtn.classList.toggle("active", creating);
      createBtn.title = managing ? "Finish or cancel the current provider playlist action first" : (canCreate ? "Create provider playlist" : "Creating provider playlists is not supported for this provider");
    }
    if (editBtn) {
      editBtn.disabled = !editOk;
      editBtn.title = creating || managing ? "Finish or cancel the current provider playlist action first"
        : (!selected.length ? "Select one provider playlist to rename"
          : (selected.length > 1 ? "Select exactly one playlist to rename" : (selected[0].can_rename ? "Rename selected provider playlist" : "Selected playlist cannot be renamed")));
    }
    if (deleteBtn) {
      deleteBtn.disabled = !deleteOk;
      deleteBtn.title = creating || managing ? "Finish or cancel the current provider playlist action first"
        : (!selected.length ? "Select one or more provider playlists to delete"
          : (deleteOk ? "Delete selected provider playlist(s)" : "One or more selected playlists cannot be deleted"));
    }
  }

  function refreshEndpointResourceSelection(root, ctx) {
    renderEndpointResourceList(root);
    updateEndpointPlaylistActions(root);
    syncEndpointCreatePanel(root);
    updateEndpointGeneratedName(root);
    if (ctx) syncModalPrimary(ctx);
  }

  function syncEndpointCreatePanel(root) {
    const submit = $("#pl-ep-create-submit", root);
    if (!submit) return;
    const err = endpointCreateMode(root) ? playlistNameError(val("#pl-ep-create-name", root)) : "";
    submit.disabled = !!err;
    submit.title = err || "Create this playlist with the selected provider";
  }

  function syncEndpointManagePanel(root) {
    const mode = endpointManageMode(root);
    const editName = $("#pl-ep-edit-name", root);
    const editSubmit = $("#pl-ep-edit-submit", root);
    const deleteSubmit = $("#pl-ep-delete-submit", root);
    if (editSubmit && editName) {
      const editValue = val("#pl-ep-edit-name", root);
      const err = mode === "edit" ? playlistNameError(editValue) : "";
      applyFieldError(editName, err, `${String(editValue || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
      editSubmit.disabled = !!err;
      editSubmit.title = err || "Rename this provider playlist";
    }
    if (deleteSubmit) {
      const ids = root.__plDeleteResourceIds || [];
      deleteSubmit.disabled = mode !== "delete" || !ids.length;
      deleteSubmit.title = ids.length ? "Delete selected provider playlist(s)" : "No playlists selected";
    }
  }

  async function createProviderPlaylistFromPanel(root, ctx, isEdit) {
    const submit = $("#pl-ep-create-submit", root);
    const nameInput = $("#pl-ep-create-name", root);
    const createName = val("#pl-ep-create-name", root);
    const err = playlistNameError(createName);
    applyFieldError(nameInput, err, `${String(createName || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
    if (err) {
      syncModalPrimary(ctx);
      return;
    }
    const provider = val("#pl-ep-provider", root);
    const instance = val("#pl-ep-instance", root) || "default";
    const mediaType = val("#pl-ep-create-type", root) || creatableEndpointTypes(provider)[0];
    if (submit) {
      submit.disabled = true;
      submit.dataset.originalText = submit.innerHTML;
      submit.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">sync</span>Creating...`;
    }
    try {
      const res = await API.resourceCreate({ provider, instance, name: createName, media_type: mediaType });
      const resource = res.resource || {};
      const search = $("#pl-ep-playlist-search", root);
      if (search) search.value = "";
      setEndpointCreateMode(root, false);
      await loadEndpointResources(root, resource.id || "", isEdit);
    } catch (error) {
      setModalError(error && error.message ? error.message : "Could not create provider playlist.");
    } finally {
      if (submit) {
        submit.innerHTML = submit.dataset.originalText || `<span class="material-symbols-rounded" aria-hidden="true">add</span>Create playlist`;
        delete submit.dataset.originalText;
      }
      syncEndpointCreatePanel(root);
      syncModalPrimary(ctx);
    }
  }

  function openEndpointResourceEdit(root, ctx) {
    const selected = selectedEndpointResources(root);
    if (selected.length !== 1 || !selected[0].can_rename) return;
    setEndpointManageMode(root, "edit", selected);
    $("#pl-ep-edit-name", root)?.focus();
    syncModalPrimary(ctx);
  }

  async function renameProviderPlaylistFromPanel(root, ctx, isEdit) {
    const submit = $("#pl-ep-edit-submit", root);
    const nameInput = $("#pl-ep-edit-name", root);
    const renameName = val("#pl-ep-edit-name", root);
    const err = playlistNameError(renameName);
    applyFieldError(nameInput, err, `${String(renameName || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
    if (err) {
      syncModalPrimary(ctx);
      return;
    }
    const playlistId = root.dataset.epEditResourceId || "";
    const provider = val("#pl-ep-provider", root);
    const instance = val("#pl-ep-instance", root) || "default";
    if (!playlistId) return;
    if (submit) {
      submit.disabled = true;
      submit.dataset.originalText = submit.innerHTML;
      submit.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">sync</span>Saving...`;
    }
    try {
      const res = await API.resourceRename(provider, instance, playlistId, renameName);
      const resource = res.resource || {};
      setEndpointManageMode(root, "");
      await loadEndpointResources(root, resource.id || playlistId, isEdit);
    } catch (error) {
      setModalError(error && error.message ? error.message : "Could not rename provider playlist.");
    } finally {
      if (submit) {
        submit.innerHTML = submit.dataset.originalText || `<span class="material-symbols-rounded" aria-hidden="true">check</span>Save rename`;
        delete submit.dataset.originalText;
      }
      syncEndpointManagePanel(root);
      syncModalPrimary(ctx);
    }
  }

  function openEndpointResourceDelete(root, ctx) {
    const selected = selectedEndpointResources(root);
    if (!selected.length || !selected.every((r) => !!r.can_delete)) return;
    setEndpointManageMode(root, "delete", selected);
    syncModalPrimary(ctx);
  }

  async function deleteProviderPlaylistsFromPanel(root, ctx, isEdit) {
    const submit = $("#pl-ep-delete-submit", root);
    const ids = Array.from(root.__plDeleteResourceIds || []).filter(Boolean);
    const provider = val("#pl-ep-provider", root);
    const instance = val("#pl-ep-instance", root) || "default";
    if (!ids.length) return;
    if (submit) {
      submit.disabled = true;
      submit.dataset.originalText = submit.innerHTML;
      submit.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">sync</span>Deleting...`;
    }
    try {
      for (const id of ids) await API.resourceDelete(provider, instance, id);
      setEndpointManageMode(root, "");
      await loadEndpointResources(root, "", isEdit);
    } catch (error) {
      setModalError(error && error.message ? error.message : "Could not delete provider playlist.");
    } finally {
      if (submit) {
        submit.innerHTML = submit.dataset.originalText || `<span class="material-symbols-rounded" aria-hidden="true">delete</span>Delete`;
        delete submit.dataset.originalText;
      }
      syncEndpointManagePanel(root);
      syncModalPrimary(ctx);
    }
  }

  function endpointOptionData(value, option) {
    const ep = endpointById(value) || {};
    const provider = ep.provider || option?.dataset?.provider || "";
    const label = ep.name || option?.dataset?.name || option?.textContent || value || "-";
    const playlist = ep.playlist_name || ep.playlist_id || "";
    const logo = providerLogo(provider);
    const rawType = String(ep.playlist_type || ep.endpoint_type || ep.kind || ep.source_kind || option?.dataset?.type || "").toLowerCase();
    const isWatchlist = rawType.includes("watchlist") || String(playlist || "").toLowerCase() === "watchlist";
    const typeBadge = endpointIsDiscovery(ep) ? "Discovery" : isWatchlist ? "Watchlist" : (!["", "regular", "playlist"].includes(rawType) ? titleize(rawType) : "");
    const note = playlist && playlist !== label && !isWatchlist ? playlist : "";
    return {
      label,
      selectedLabel: label,
      selectedShowNote: false,
      note,
      icons: [logo ? { src: logo, alt: "" } : { text: String(provider || "?").slice(0, 2) }],
      badges: [typeBadge].filter(Boolean),
    };
  }

  function mappingEndpointName(id) {
    const ep = endpointById(id) || {};
    return ep.name || ep.playlist_name || ep.playlist_id || id || "Endpoint";
  }

  function generatedMappingName(sourceId, targetId) {
    void sourceId;
    void targetId;
    return nextMappingName();
  }

  function updateMappingGeneratedName(root, force = false) {
    const input = $("#pl-map-name", root);
    if (!input) return;
    if (!force && root.dataset.mapNameDirty === "1") return;
    input.value = nextMappingName(0, root.dataset.mapEditId || "");
  }

  function enhanceMappingSelects(root) {
    ["#pl-map-source", "#pl-map-targets"].forEach((sel) => {
      const select = $(sel, root);
      if (select) window.CW?.IconSelect?.enhance?.(select, { className: "pl-endpoint-select", menuClassName: "pl-endpoint-select-menu", getOptionData: endpointOptionData });
    });
    ["#pl-map-direction", "#pl-map-ruleset", "#pl-map-membership", "#pl-map-order"].forEach((sel) => {
      const select = $(sel, root);
      if (select) window.CW?.IconSelect?.enhance?.(select, { className: "cw-plain-select" });
    });
  }

  function openEndpointModal({ endpoint = null, clone = false, trigger = null } = {}) {
    const isEdit = !!endpoint && !clone;
    const seed = endpoint || {};
    const providers = configuredProviders().map((p) => ({ value: p.provider, label: p.label || p.provider }));
    const provider = seed.provider || (providers[0] && providers[0].value) || "";
    const instances = instancesFor(provider);
    const instance = seed.instance || instances[0] || "default";
    const title = isEdit ? "Edit playlist endpoint" : "Create playlist endpoint";
    const description = isEdit ? "Update the provider playlist used by this endpoint." : "Connect one or more provider playlists as reusable endpoints.";
    const endpointName = isEdit ? (seed.name || seed.id || "") : nextEndpointName();
    const body = `
      <div class="pl-endpoint-wizard">
        <section class="pl-endpoint-step">
          <div class="pl-step-head"><span class="pl-step-index">1</span><div><b>Endpoint details</b><span>Name uses the next available endpoint number and can be edited.</span></div></div>
          <div class="pl-form">
            <div class="pl-field full">
              <label for="pl-ep-name">Endpoint name <span aria-hidden="true">*</span></label>
              <input id="pl-ep-name" maxlength="${NAME_MAX}" value="${esc(endpointName)}" placeholder="Enter endpoint name" aria-describedby="pl-ep-name-error">
              <div class="pl-field-error" id="pl-ep-name-error"></div>
            </div>
            <div class="pl-field full">
              <label for="pl-ep-provider">Provider <span aria-hidden="true">*</span></label>
              <select id="pl-ep-provider">${selectOptions(providers, provider, providers.length ? "" : "No providers configured")}</select>
            </div>
          </div>
        </section>
        <section class="pl-endpoint-step">
          <div class="pl-step-head"><span class="pl-step-index">2</span><div><b>Provider profile</b><span>Use the profile that owns the provider playlists.</span></div></div>
          <div class="pl-form">
            <div class="pl-field full">
              <label for="pl-ep-instance">Provider profile <span aria-hidden="true">*</span></label>
              <select id="pl-ep-instance">${selectOptions(instances.map((x) => ({ value: x, label: x })), instance)}</select>
            </div>
          </div>
        </section>
        <section class="pl-endpoint-step" id="pl-ep-resource-wrap">
          <div class="pl-step-head with-actions">
            <span class="pl-step-index">3</span>
            <div><b>Select provider playlists</b><span>${isEdit ? "Choose one playlist for this endpoint." : "Choose one or more playlists to create endpoints."}</span></div>
            <div class="pl-playlist-tools" aria-label="Provider playlist actions">
              <button type="button" class="pl-icon-tool" id="pl-ep-list-create" title="Create provider playlist" aria-label="Create provider playlist"><span class="material-symbols-rounded" aria-hidden="true">add</span></button>
              <button type="button" class="pl-icon-tool" id="pl-ep-list-edit" title="Edit provider playlist" aria-label="Edit provider playlist"><span class="material-symbols-rounded" aria-hidden="true">edit</span></button>
              <button type="button" class="pl-icon-tool danger" id="pl-ep-list-delete" title="Delete provider playlist" aria-label="Delete provider playlist"><span class="material-symbols-rounded" aria-hidden="true">delete</span></button>
            </div>
          </div>
          <input type="checkbox" id="pl-ep-create" hidden>
          <div class="pl-warning hidden" id="pl-ep-simkl-warning">${esc(SIMKL_PLAYLIST_WARNING)}</div>
          <div class="pl-create-panel hidden" id="pl-ep-create-panel">
            <div class="pl-field">
              <label for="pl-ep-create-name">New playlist name</label>
              <input id="pl-ep-create-name" maxlength="${PLAYLIST_NAME_MAX}" value="${esc(seed.playlist_name || seed.name || "")}" placeholder="New playlist name" aria-describedby="pl-ep-create-name-error">
              <div class="pl-field-error" id="pl-ep-create-name-error"></div>
            </div>
            <div class="pl-field" id="pl-ep-create-type-wrap">
              <label for="pl-ep-create-type">New list type</label>
              <select id="pl-ep-create-type"></select>
              <div class="pl-help" id="pl-ep-create-type-help">Collections group titles in the library, playlists keep their own order.</div>
            </div>
            <div class="pl-create-actions">
              <button type="button" class="pl-btn small" id="pl-ep-create-cancel">Cancel</button>
              <button type="button" class="pl-btn small primary" id="pl-ep-create-submit"><span class="material-symbols-rounded" aria-hidden="true">add</span>Create playlist</button>
            </div>
          </div>
          <div class="pl-create-panel pl-resource-manage-panel hidden" id="pl-ep-edit-panel">
            <div class="pl-field full">
              <label for="pl-ep-edit-name">Playlist name</label>
              <input id="pl-ep-edit-name" maxlength="${PLAYLIST_NAME_MAX}" value="" placeholder="Playlist name" aria-describedby="pl-ep-edit-name-error">
              <div class="pl-field-error" id="pl-ep-edit-name-error"></div>
            </div>
            <div class="pl-create-actions">
              <button type="button" class="pl-btn small" id="pl-ep-edit-cancel">Cancel</button>
              <button type="button" class="pl-btn small primary" id="pl-ep-edit-submit"><span class="material-symbols-rounded" aria-hidden="true">check</span>Save rename</button>
            </div>
          </div>
          <div class="pl-create-panel pl-resource-manage-panel danger hidden" id="pl-ep-delete-panel">
            <div class="pl-resource-delete-copy">
              <b>Delete provider playlist?</b>
              <span>This removes the playlist from the provider account. CrossWatch endpoints are not deleted.</span>
              <div class="pl-resource-delete-list" id="pl-ep-delete-list"></div>
            </div>
            <div class="pl-create-actions">
              <button type="button" class="pl-btn small" id="pl-ep-delete-cancel">Cancel</button>
              <button type="button" class="pl-btn small danger" id="pl-ep-delete-submit"><span class="material-symbols-rounded" aria-hidden="true">delete</span>Delete</button>
            </div>
          </div>
          <div class="pl-playlist-search">
            <span class="material-symbols-rounded" aria-hidden="true">search</span>
            <input id="pl-ep-playlist-search" type="search" placeholder="Search playlists..." autocomplete="off">
          </div>
          <select id="pl-ep-playlist" class="pl-native-playlist-select" ${isEdit ? "" : "multiple"} hidden><option value="">Loading...</option></select>
          <div class="pl-resource-list" id="pl-ep-resource-list" role="listbox" aria-multiselectable="${isEdit ? "false" : "true"}"><div class="pl-resource-empty">Loading playlists...</div></div>
          <div class="pl-help" id="pl-ep-playlist-help">${isEdit ? "Select one provider playlist." : "Select one or more provider playlists to create endpoints."}</div>
          <div class="pl-field-error" id="pl-ep-playlist-error"></div>
        </section>
      </div>
    `;
    openModal({
      title,
      description,
      body,
      trigger,
      width: "760px",
      primaryText: isEdit ? "Save endpoint" : "Create endpoint",
      savingText: "Saving endpoint...",
      onOpen: (ctx) => hydrateEndpointModal(ctx, seed, isEdit, provider, instance),
      onPrimary: async (ctx) => saveEndpointFromModal(ctx, seed, isEdit),
    });
  }

  function hydrateEndpointModal(ctx, seed, isEdit, provider, instance) {
    const root = ctx.modal;
    root.__plEndpointCtx = ctx;
    root.dataset.epEditId = isEdit ? (seed.id || "") : "";
    root.dataset.epSingleSelect = isEdit ? "1" : "";
    root.dataset.epNameDirty = isEdit ? "1" : "";
    bindNameValidation(ctx, "#pl-ep-name", "Endpoint name");
    const providerSelect = $("#pl-ep-provider", root);
    const instanceSelect = $("#pl-ep-instance", root);
    const createCheck = $("#pl-ep-create", root);
    const createName = $("#pl-ep-create-name", root);
    const search = $("#pl-ep-playlist-search", root);
    const simklWarning = $("#pl-ep-simkl-warning", root);
    const validateCreateName = () => {
      const err = createCheck.checked ? playlistNameError(createName.value) : "";
      applyFieldError(createName, err, `${String(createName.value || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
      return err;
    };
    addModalValidator(ctx, validateCreateName);
    addModalValidator(ctx, () => {
      const err = endpointCreateMode(root) || selectedValues("#pl-ep-playlist", root).length ? "" : "Select at least one provider playlist.";
      const list = $("#pl-ep-resource-list", root);
      const feedback = $("#pl-ep-playlist-error", root);
      if (list) list.classList.toggle("invalid", !!err);
      if (feedback) feedback.textContent = err;
      return err;
    });
    addModalValidator(ctx, () => endpointCreateMode(root) ? "Create or cancel the provider playlist before creating the endpoint." : "");
    addModalValidator(ctx, () => endpointManageMode(root) ? "Save or cancel the provider playlist action before creating the endpoint." : "");
    const updateInstances = () => {
      const list = instancesFor(providerSelect.value);
      instanceSelect.innerHTML = selectOptions(list.map((x) => ({ value: x, label: x })), list.includes(instanceSelect.value) ? instanceSelect.value : (list[0] || "default"));
      window.CW?.ProfileSelect?.enhanceProfile?.(instanceSelect);
    };
    const updateCreateTypes = () => {
      const wrap = $("#pl-ep-create-type-wrap", root);
      const select = $("#pl-ep-create-type", root);
      if (!wrap || !select) return;
      const types = creatableEndpointTypes(providerSelect.value);
      const current = select.value;
      select.innerHTML = selectOptions(types.map((t) => ({ value: t, label: titleize(t) })), types.includes(current) ? current : types[0]);
      wrap.classList.toggle("hidden", types.length <= 1);
      window.CW?.IconSelect?.enhance?.(select, { className: "cw-plain-select" });
    };
    const updateProviderRestrictions = () => {
      const isSimkl = String(providerSelect.value || "").toUpperCase() === "SIMKL";
      if (simklWarning) simklWarning.classList.toggle("hidden", !isSimkl);
      if (createCheck) createCheck.disabled = isSimkl;
      if (isSimkl) setEndpointCreateMode(root, false);
      updateCreateTypes();
      updateEndpointPlaylistActions(root);
      syncModalPrimary(ctx);
    };
    $("#pl-ep-name", root)?.addEventListener("input", () => { root.dataset.epNameDirty = "1"; });
    window.CW?.ProfileSelect?.enhanceProvider?.(providerSelect);
    window.CW?.ProfileSelect?.enhanceProfile?.(instanceSelect);
    providerSelect.addEventListener("change", () => {
      setEndpointManageMode(root, "");
      updateInstances();
      updateProviderRestrictions();
      loadEndpointResources(root, "", isEdit);
    });
    instanceSelect.addEventListener("change", () => {
      setEndpointManageMode(root, "");
      loadEndpointResources(root, "", isEdit);
    });
    $("#pl-ep-list-create", root)?.addEventListener("click", () => {
      if ($("#pl-ep-list-create", root).disabled) return;
      setEndpointCreateMode(root, !endpointCreateMode(root));
      updateEndpointPlaylistActions(root);
      syncModalPrimary(ctx);
    });
    $("#pl-ep-list-edit", root)?.addEventListener("click", () => {
      if ($("#pl-ep-list-edit", root).disabled) return;
      openEndpointResourceEdit(root, ctx);
    });
    $("#pl-ep-list-delete", root)?.addEventListener("click", () => {
      if ($("#pl-ep-list-delete", root).disabled) return;
      openEndpointResourceDelete(root, ctx);
    });
    $("#pl-ep-create-cancel", root)?.addEventListener("click", () => {
      setEndpointCreateMode(root, false);
      updateEndpointPlaylistActions(root);
      syncModalPrimary(ctx);
    });
    $("#pl-ep-create-submit", root)?.addEventListener("click", () => createProviderPlaylistFromPanel(root, ctx, isEdit));
    $("#pl-ep-edit-cancel", root)?.addEventListener("click", () => setEndpointManageMode(root, ""));
    $("#pl-ep-edit-submit", root)?.addEventListener("click", () => renameProviderPlaylistFromPanel(root, ctx, isEdit));
    $("#pl-ep-delete-cancel", root)?.addEventListener("click", () => setEndpointManageMode(root, ""));
    $("#pl-ep-delete-submit", root)?.addEventListener("click", () => deleteProviderPlaylistsFromPanel(root, ctx, isEdit));
    search?.addEventListener("input", () => renderEndpointResourceList(root));
    createName.addEventListener("input", () => { updateEndpointGeneratedName(root); syncEndpointCreatePanel(root); syncModalPrimary(ctx); });
    createName.addEventListener("change", () => { updateEndpointGeneratedName(root); syncEndpointCreatePanel(root); syncModalPrimary(ctx); });
    $("#pl-ep-edit-name", root)?.addEventListener("input", () => { syncEndpointManagePanel(root); syncModalPrimary(ctx); });
    $("#pl-ep-edit-name", root)?.addEventListener("change", () => { syncEndpointManagePanel(root); syncModalPrimary(ctx); });
    $("#pl-ep-resource-list", root)?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("[data-playlist-id]");
      const select = $("#pl-ep-playlist", root);
      if (!btn || !select) return;
      setEndpointCreateMode(root, false);
      setEndpointManageMode(root, "");
      const opt = Array.from(select.options || []).find((o) => o.value === btn.dataset.playlistId);
      if (!opt) return;
      if (isEdit) Array.from(select.options || []).forEach((o) => { o.selected = o === opt; });
      else opt.selected = !opt.selected;
      select.dispatchEvent(new Event("change", { bubbles: true }));
      refreshEndpointResourceSelection(root, ctx);
    });
    $("#pl-ep-playlist", root)?.addEventListener("change", () => refreshEndpointResourceSelection(root, ctx));
    updateInstances();
    updateProviderRestrictions();
    loadEndpointResources(root, seed.playlist_id || "", isEdit);
  }

  async function loadEndpointResources(root, selected, isEdit) {
    const select = $("#pl-ep-playlist", root);
    const help = $("#pl-ep-playlist-help", root);
    select.innerHTML = `<option value="">Loading...</option>`;
    root.__plEndpointResources = [];
    renderEndpointResourceList(root);
    try {
      const data = await API.resources(val("#pl-ep-provider", root), val("#pl-ep-instance", root));
      const resources = data.resources || [];
      root.__plEndpointResources = resources;
      if (!resources.length) {
        select.innerHTML = `<option value="">No playlists found</option>`;
        help.textContent = "No readable provider playlists were returned for this profile.";
        refreshEndpointResourceSelection(root, root.__plEndpointCtx);
        return;
      }
      select.innerHTML = resources.map((r) => {
        const caps = resourceCaps(r);
        const endpointType = r.discovery ? "discovery" : ((r.extra && r.extra.endpoint_type) || r.endpoint_type || r.playlist_type || r.kind || "");
        const shouldSelect = selected ? r.id === selected : (!isEdit && resources[0] && r.id === resources[0].id);
        return `<option value="${esc(r.id)}" data-name="${esc(r.name || r.id)}" data-type="${esc(endpointType || "playlist")}" data-media-types="${esc((r.media_types || []).join(","))}" data-kind="${esc(r.kind || "regular")}" ${shouldSelect ? "selected" : ""}>${esc(r.name || r.id)}${caps.length ? ` (${esc(caps.join("/"))})` : ""}</option>`;
      }).join("");
      help.textContent = isEdit ? "Select one provider playlist." : "Select one or more provider playlists to create endpoints.";
    } catch (err) {
      select.innerHTML = `<option value="">Could not load playlists</option>`;
      help.textContent = err && err.message ? err.message : "Provider playlist loading failed.";
      root.__plEndpointResources = [];
    }
    root.__plEndpointCtx = root.__plEndpointCtx || state.modal;
    refreshEndpointResourceSelection(root, root.__plEndpointCtx);
  }

  async function saveEndpointFromModal(ctx, seed, isEdit) {
    const root = ctx.modal;
    const create = checked("#pl-ep-create", root);
    const provider = val("#pl-ep-provider", root);
    const instance = val("#pl-ep-instance", root) || "default";
    const name = val("#pl-ep-name", root);
    const nameErr = nameFieldError(name, "Endpoint name");
    if (nameErr) throw new Error(nameErr);
    if (!provider) throw new Error("Provider is required.");
    if (create) {
      const createName = val("#pl-ep-create-name", root);
      const createNameErr = playlistNameError(createName);
      if (createNameErr) throw new Error(createNameErr);
      const createType = val("#pl-ep-create-type", root) || creatableEndpointTypes(provider)[0];
      await API.epUpsert({ id: isEdit ? seed.id : "", name: name || nextEndpointName(0, isEdit ? seed.id : ""), provider, instance, create: true, create_name: createName, media_type: createType });
    } else {
      const ids = selectedValues("#pl-ep-playlist", root);
      if (!ids.length) throw new Error("Select at least one playlist.");
      if (isEdit && ids.length !== 1) throw new Error("Edit mode can only save one selected playlist.");
      for (const [index, playlistId] of ids.entries()) {
        const opt = Array.from($("#pl-ep-playlist", root).options || []).find((o) => o.value === playlistId);
        const playlistName = opt ? opt.dataset.name || opt.textContent : playlistId;
        const playlistType = opt ? opt.dataset.type || "" : "";
        const mediaTypes = opt && opt.dataset.mediaTypes ? opt.dataset.mediaTypes.split(",").filter(Boolean) : [];
        const endpointName = root.dataset.epNameDirty === "1" || isEdit ? name : nextEndpointName(index, isEdit ? seed.id : "");
        await API.epUpsert({ id: isEdit ? seed.id : "", name: endpointName, provider, instance, playlist_id: playlistId, playlist_name: playlistName, playlist_type: playlistType, media_types: mediaTypes });
      }
    }
    closeModal(true);
    await refreshOverview();
  }

  async function syncEndpoint(endpoint, btn) {
    if (!endpoint) return;
    const id = String(endpoint.id || "");
    const root = $("#page-playlists");
    state.runningEndpoints.add(id);
    if (root) refreshSection(root, "endpoints");
    try {
      await API.epSync(id);
      await refreshOverview(["endpoints", "activity"]);
    } catch (err) {
      openNotice("Refresh failed", err && err.message ? err.message : "Could not refresh this endpoint.", btn);
    } finally {
      state.runningEndpoints.delete(id);
      const freshRoot = $("#page-playlists");
      if (freshRoot) refreshSection(freshRoot, "endpoints");
    }
  }

  function openEndpointDelete(endpoint, trigger) {
    if (!endpoint) return;
    const used = state.mappings.filter((m) => mappingUsesEndpoint(m, endpoint.id));
    const body = `
      <div class="pl-confirm-lines">
        <div><b>Endpoint:</b> ${esc(endpoint.name || endpoint.id)}</div>
        <div><b>Provider:</b> ${esc(endpoint.provider_label || providerLabel(endpoint.provider))}</div>
        <div><b>Selected playlist:</b> ${esc(endpoint.playlist_name || endpoint.playlist_id || "-")}</div>
        <div><b>Mappings using endpoint:</b> ${esc(used.length)}</div>
        ${used.length ? `<div class="pl-warning">This endpoint is used by ${esc(used.map((m) => m.name || m.id).join(", "))}. The backend will block deletion until those mappings are changed or removed.</div>` : `<div>Deleting the endpoint does not delete the provider playlist.</div>`}
      </div>
    `;
    openModal({
      title: "Delete playlist endpoint",
      description: "Confirm removal of this CrossWatch endpoint.",
      body,
      trigger,
      primaryText: "Delete endpoint",
      savingText: "Deleting...",
      onPrimary: async () => {
        await API.epDelete(endpoint.id);
        closeModal(true);
        await refreshOverview();
      },
    });
  }

  function openMappingModal({ mapping = null, clone = false, draft = null, trigger = null, onDone = null } = {}) {
    if (state.endpoints.length < 2) {
      openNotice("Create playlist mapping", "At least two endpoints are required before you can create a mapping.", trigger);
      return;
    }
    const seed = draft || mapping || {};
    const isEdit = !!mapping && !clone && !draft;
    const source = seed.source_endpoint || (state.endpoints[0] && state.endpoints[0].id) || "";
    const targets = seed.target_endpoints || (state.endpoints.find((e) => e.id !== source) ? [state.endpoints.find((e) => e.id !== source).id] : []);
    const target = targets.find((id) => id !== source) || targets[0] || "";
    const title = isEdit ? "Edit playlist mapping" : "Create playlist mapping";
    const endpointOpts = state.endpoints.map((ep) => ({ value: ep.id, label: `${ep.name || ep.id} (${ep.provider}${endpointIsDiscovery(ep) ? " discovery" : ""})` }));
    const rulesetOpts = [{ value: "", label: "Direct one way" }].concat(state.rulesets.map((rs) => ({ value: rs.id, label: `${rs.name}${rs.built_in ? " (built in)" : ""}` })));
    const keepSeedName = isEdit || !!draft;
    const mappingName = keepSeedName ? (seed.name || seed.id || "") : nextMappingName();
    const body = `
      <div class="pl-mapping-wizard">
        <section class="pl-mapping-step">
          <div class="pl-step-head"><span class="pl-step-index">1</span><div><b>Mapping details</b><span>Name uses the next available mapping number and can be edited.</span></div></div>
          <div class="pl-form">
            <div class="pl-field full">
              <label for="pl-map-name">Mapping name <span aria-hidden="true">*</span></label>
              <input id="pl-map-name" maxlength="${NAME_MAX}" value="${esc(mappingName)}" placeholder="Enter mapping name" aria-describedby="pl-map-name-error">
              <div class="pl-field-error" id="pl-map-name-error"></div>
            </div>
          </div>
        </section>
        <section class="pl-mapping-step">
          <div class="pl-step-head"><span class="pl-step-index">2</span><div><b>Mapping configuration</b><span>Choose a source endpoint and the destination that should receive updates.</span></div></div>
          <div class="pl-map-grid">
            <div class="pl-field">
              <label for="pl-map-source">Source endpoint <span aria-hidden="true">*</span></label>
              <select id="pl-map-source">${selectOptions(endpointOpts, source)}</select>
            </div>
            <div class="pl-field">
              <label for="pl-map-targets">Destination endpoint <span aria-hidden="true">*</span></label>
              <select id="pl-map-targets">${selectOptions(endpointOpts, target)}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-direction-field">
              <label for="pl-map-direction">Direction</label>
              <select id="pl-map-direction">${selectOptions(ENUMS.direction.map(([value, label]) => ({ value, label })), (rulesetById(seed.ruleset_id || "") || {}).direction || "one_way")}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-ruleset-field">
              <label for="pl-map-ruleset">Ruleset</label>
              <select id="pl-map-ruleset">${selectOptions(rulesetOpts, seed.ruleset_id || "")}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-membership-field">
              <label for="pl-map-membership">Sync mode</label>
              <select id="pl-map-membership">${selectOptions(ENUMS.membership.map(([value, label]) => ({ value, label })), seed.membership || "managed_only")}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-order-field">
              <label for="pl-map-order">Ordering</label>
              <select id="pl-map-order">${selectOptions(ENUMS.order.map(([value, label]) => ({ value, label })), seed.order || "ignore")}</select>
            </div>
          </div>
          <label class="pl-map-toggle"><input type="checkbox" id="pl-map-enabled" ${seed.enabled === false ? "" : "checked"}><span class="pl-map-toggle-box"><span class="material-symbols-rounded" aria-hidden="true">check</span></span><span><b>Enabled</b><small>This mapping is active and will be synchronized.</small></span></label>
          <div class="pl-warning hidden" id="pl-map-simkl-warning">${esc(SIMKL_PLAYLIST_WARNING)}</div>
          <div class="pl-map-ruleset-card pl-map-advanced" id="pl-map-ruleset-actions">
            <span class="material-symbols-rounded" aria-hidden="true">shield</span>
            <div><b>Rulesets</b><span>Rulesets define how items are matched and synchronized.</span></div>
            <button type="button" class="pl-btn small" id="pl-map-manage-rulesets">Manage rulesets <span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
          </div>
          <div class="pl-map-info"><span class="material-symbols-rounded" aria-hidden="true">info</span><div class="pl-help" id="pl-map-rule-help"></div></div>
          <div class="pl-map-info hidden" id="pl-map-discovery-help"><span class="material-symbols-rounded" aria-hidden="true">travel_explore</span><div class="pl-help">Discovery sources use direct mirror mappings so the destination matches the feed after each successful sync.</div></div>
        </section>
      </div>
    `;
    openModal({
      title,
      description: isEdit ? "Update the endpoints, ruleset and sync behavior for this mapping." : "Connect a source playlist endpoint to one or more destination endpoints.",
      body,
      trigger,
      width: "760px",
      primaryText: isEdit ? "Save mapping" : "Create mapping",
      savingText: "Saving mapping...",
      onCancel: () => { if (typeof onDone === "function") onDone("cancel"); },
      onOpen: (ctx) => hydrateMappingModal(ctx, seed, isEdit, keepSeedName),
      onPrimary: async (ctx) => {
        await saveMappingFromModal(ctx, isEdit ? mapping.id : "");
        if (typeof onDone === "function") onDone("save");
      },
    });
  }

  function hydrateMappingModal(ctx, seed = {}, isEdit = false, keepSeedName = false) {
    const root = ctx.modal;
    root.dataset.mapEditId = isEdit ? (seed.id || "") : "";
    root.dataset.mapNameDirty = keepSeedName && seed.name ? "1" : "";
    bindNameValidation(ctx, "#pl-map-name", "Mapping name");
    const refresh = () => {
      const source = val("#pl-map-source", root);
      $$("#pl-map-targets option", root).forEach((opt) => { opt.disabled = opt.value === source; });
      const targetSelect = $("#pl-map-targets", root);
      $$("#pl-map-targets option", root).forEach((opt) => {
        const ep = endpointById(opt.value);
        opt.disabled = opt.value === source || !endpointWritable(ep);
      });
      if (targetSelect.value === source || (targetSelect.selectedOptions[0] && targetSelect.selectedOptions[0].disabled)) {
        const next = Array.from(targetSelect.options || []).find((opt) => !opt.disabled);
        targetSelect.value = next ? next.value : "";
      }
      updateMappingGeneratedName(root);
      let rule = rulesetById(val("#pl-map-ruleset", root));
      let direction = val("#pl-map-direction", root);
      const help = $("#pl-map-rule-help", root);
      const sourceEp = endpointById(source);
      const targetEp = endpointById(targetSelect.value);
      const sourceDiscovery = endpointIsDiscovery(sourceEp);
      const simklWarning = $("#pl-map-simkl-warning", root);
      const usesSimkl = [sourceEp, targetEp].some((ep) => String((ep && ep.provider) || "").toUpperCase() === "SIMKL");
      if (simklWarning) simklWarning.classList.toggle("hidden", !usesSimkl);
      $$("#pl-map-direction-field,#pl-map-ruleset-field,#pl-map-membership-field,#pl-map-order-field,#pl-map-ruleset-actions", root).forEach((el) => el.classList.toggle("hidden", sourceDiscovery));
      const discoveryHelp = $("#pl-map-discovery-help", root);
      if (discoveryHelp) discoveryHelp.classList.toggle("hidden", !sourceDiscovery);
      ["#pl-map-direction", "#pl-map-ruleset", "#pl-map-membership", "#pl-map-order"].forEach((sel) => {
        const el = $(sel, root);
        if (el) el.disabled = sourceDiscovery;
      });
      if (sourceDiscovery) {
        $("#pl-map-direction", root).value = "one_way";
        $("#pl-map-ruleset", root).value = "";
        $("#pl-map-membership", root).value = "mirror";
        $("#pl-map-order", root).value = targetEp && targetEp.can_reorder === false ? "ignore" : "preserve";
        rule = null;
        direction = "one_way";
      }
      if (!rule) help.textContent = "Direct mappings run one way and require exactly one destination endpoint.";
      else help.textContent = `${rule.name} supports ${ruleLabel(rule.direction)} mappings, ${rule.write_mode} writes, and up to ${rule.maximum_targets} destination endpoint(s).`;
      if (targetEp && !endpointWritable(targetEp)) help.textContent = "Choose a writable destination endpoint.";
      if (rule && rule.direction !== direction) help.textContent += " Change the direction or choose another ruleset before saving.";
      enhanceMappingSelects(root);
      syncModalPrimary(ctx);
    };
    $("#pl-map-name", root)?.addEventListener("input", () => { root.dataset.mapNameDirty = "1"; });
    $("#pl-map-source", root).addEventListener("change", () => { refresh(); });
    $("#pl-map-targets", root).addEventListener("change", () => { refresh(); });
    $("#pl-map-ruleset", root).addEventListener("change", refresh);
    $("#pl-map-direction", root).addEventListener("change", refresh);
    $("#pl-map-manage-rulesets", root).addEventListener("click", (e) => {
      const draft = readMappingDraft(root);
      openRulesetManager({ trigger: e.currentTarget, fromMapping: true, mappingDraft: draft, mappingDone: ctx.opts.onDone });
    });
    enhanceMappingSelects(root);
    refresh();
  }

  function readMappingDraft(root) {
    return {
      name: val("#pl-map-name", root),
      source_endpoint: val("#pl-map-source", root),
      target_endpoints: val("#pl-map-targets", root) ? [val("#pl-map-targets", root)] : [],
      ruleset_id: val("#pl-map-ruleset", root),
      membership: val("#pl-map-membership", root),
      order: val("#pl-map-order", root),
      enabled: checked("#pl-map-enabled", root),
    };
  }

  async function saveMappingFromModal(ctx, id) {
    const root = ctx.modal;
    const draft = readMappingDraft(root);
    if (!draft.name) draft.name = nextMappingName(0, id);
    const direction = val("#pl-map-direction", root);
    const rule = rulesetById(draft.ruleset_id);
    const nameErr = nameFieldError(draft.name, "Mapping name");
    if (nameErr) throw new Error(nameErr);
    if (!draft.source_endpoint || !draft.target_endpoints.length) throw new Error("Source and destination endpoints are required.");
    if (draft.target_endpoints.includes(draft.source_endpoint)) throw new Error("Source and destination endpoints must be different.");
    if (!draft.ruleset_id && draft.target_endpoints.length !== 1) throw new Error("Direct mappings require exactly one destination endpoint.");
    if (rule && rule.direction !== direction) throw new Error(`The selected ruleset supports ${ruleLabel(rule.direction)}, not ${ruleLabel(direction)}.`);
    if (rule && draft.target_endpoints.length > Number(rule.maximum_targets || 1)) throw new Error("Too many destination endpoints for the selected ruleset.");
    if (endpointIsDiscovery(endpointById(draft.source_endpoint)) && (draft.ruleset_id || draft.membership !== "mirror")) throw new Error("Discovery sources use direct mirror mappings.");
    if (draft.target_endpoints.some((id) => !endpointWritable(endpointById(id)))) throw new Error("Destination endpoint must be writable.");
    const res = await API.mapUpsert({ id, ...draft });
    notifyPairsChanged({ source: "playlists", mapping_id: (res.mapping && res.mapping.id) || id, pair_id: res.pair_id || (res.mapping && res.mapping.assigned_pair) || "" });
    closeModal(true);
    await refreshOverview();
  }

  async function toggleMapping(mapping, btn) {
    if (!mapping) return;
    btn.disabled = true;
    try {
      await API.mapUpsert({ id: mapping.id, name: mapping.name, source_endpoint: mapping.source_endpoint, target_endpoints: mapping.target_endpoints || [], ruleset_id: mapping.ruleset_id || "", membership: mapping.membership || "managed_only", order: mapping.order || "ignore", enabled: !mapping.enabled });
      notifyPairsChanged({ source: "playlists", mapping_id: mapping.id, pair_id: mapping.assigned_pair || "" });
      await refreshOverview(["mappings", "activity"]);
    } finally {
      btn.disabled = false;
    }
  }

  async function syncMapping(mapping, btn) {
    if (!mapping) return;
    await refreshSyncSummary(false).catch(() => null);
    if (sharedSyncBusy()) {
      openNotice("Sync already running", "A synchronization is already running. Wait for it to finish before starting another mapping.", btn);
      return;
    }
    if (!mapping.assigned_pair) {
      openNotice("Sync pair missing", "Save the mapping once so CrossWatch can create its playlist sync pair.", btn);
      return;
    }
    const id = String(mapping.id || "");
    const root = $("#page-playlists");
    state.runningMappings.add(id);
    state.localSyncStartedAt = Date.now();
    state.syncObservedRunning = false;
    state.syncSummary = { ...(state.syncSummary || {}), running: true, pair_scope_ids: [String(mapping.assigned_pair || "")] };
    if (root) refreshSection(root, "mappings");
    try {
      const res = await API.runPair(mapping.assigned_pair);
      if (res && res.run_id) state.syncSummary = { ...(state.syncSummary || {}), running: true, run_id: res.run_id, pair_scope_ids: [String(mapping.assigned_pair || "")] };
      scheduleSyncSummaryPoll(800);
    } catch (err) {
      state.runningMappings.delete(id);
      state.localSyncStartedAt = 0;
      state.syncObservedRunning = false;
      openNotice("Sync failed", err && err.message ? err.message : "Could not run this mapping.", btn);
      const freshRoot = $("#page-playlists");
      if (freshRoot) refreshSection(freshRoot, "mappings");
    }
  }

  function openMappingDelete(mapping, trigger) {
    if (!mapping) return;
    const source = endpointById(mapping.source_endpoint);
    const rule = mapping.ruleset || rulesetById(mapping.ruleset_id || "");
    const body = `
      <div class="pl-confirm-lines">
        <div><b>Mapping:</b> ${esc(mapping.name || mapping.id)}</div>
        <div><b>Source endpoint:</b> ${esc(source ? source.name : mapping.source_endpoint)}</div>
        <div><b>Destination endpoint:</b> ${esc(targetNames(mapping) || "-")}</div>
        <div><b>Direction:</b> ${esc(directionFor(mapping))}</div>
        <div><b>Assigned ruleset:</b> ${esc(rule ? rule.name : "Direct")}</div>
        <div>Deleting the mapping does not delete endpoints or provider playlists.</div>
      </div>
    `;
    openModal({
      title: "Delete playlist mapping",
      description: "Confirm removal of this sync relationship.",
      body,
      trigger,
      primaryText: "Delete mapping",
      savingText: "Deleting...",
      onPrimary: async () => {
        await API.mapDelete(mapping.id);
        notifyPairsChanged({ source: "playlists", mapping_id: mapping.id, pair_id: mapping.assigned_pair || "" });
        closeModal(true);
        await refreshOverview();
      },
    });
  }

  function openRulesetManager({ trigger = null, fromMapping = false, mappingDraft = null, mappingDone = null } = {}) {
    const rows = state.rulesets.map((rs) => {
      const used = mappingsForRuleset(rs.id);
      return `
        <tr>
          <td><div class="pl-main-text">${esc(rs.name)}</div><div class="pl-muted">${esc(rs.id)}</div></td>
          <td><span class="pl-pill ${rs.built_in ? "warn" : "ok"}">${rs.built_in ? "Built in" : "Custom"}</span></td>
          <td>${esc(ruleLabel(rs.direction))}</td>
          <td>${esc(titleize(rs.read_mode))} read, ${esc(titleize(rs.write_mode))} write</td>
          <td>${esc(rs.per_endpoint_capacity)} per list, ${esc(rs.maximum_targets)} target(s)</td>
          <td>${esc(used.length)}</td>
          <td>
            <div class="pl-actions">
              ${rulesetActionButton("view", rs.id, "View ruleset", "visibility")}
              ${rulesetActionButton("clone", rs.id, "Clone ruleset", "content_copy", "sync")}
              ${rulesetActionButton("edit", rs.id, rs.built_in ? "Built in rulesets cannot be edited." : "Edit ruleset", "edit", "edit", !!rs.built_in)}
              ${rulesetActionButton("delete", rs.id, rs.built_in ? "Built in rulesets cannot be deleted." : "Delete ruleset", "delete", "delete", !!rs.built_in)}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    const body = `
      <div class="pl-table-wrap">
        <table class="pl-ruleset-table">
          <thead><tr><th>Ruleset name</th><th>Type</th><th>Direction</th><th>Strategy</th><th>Capacity behaviour</th><th>Mappings</th><th>Actions</th></tr></thead>
          <tbody>${rows || `<tr><td colspan="7"><div class="pl-empty"><strong>No rulesets</strong><span>Create a custom ruleset for advanced mapping behavior.</span></div></td></tr>`}</tbody>
        </table>
      </div>
    `;
    openModal({
      title: "Manage rulesets",
      description: "View built in rulesets and manage custom playlist sync rules.",
      body,
      trigger,
      width: "980px",
      primaryText: "Create new ruleset",
      onCancel: () => { if (fromMapping && mappingDraft) openMappingModal({ draft: mappingDraft, trigger, onDone: mappingDone }); },
      onOpen: (ctx) => {
        ctx.modal.addEventListener("click", (e) => {
          const btn = e.target.closest("[data-ruleset-action]");
          if (!btn) return;
          const rs = rulesetById(btn.dataset.id);
          if (btn.dataset.rulesetAction === "view") openRulesetForm({ mode: "view", ruleset: rs, trigger: btn, fromMapping, mappingDraft, mappingDone });
          if (btn.dataset.rulesetAction === "clone") openRulesetForm({ mode: "clone", ruleset: rs, trigger: btn, fromMapping, mappingDraft, mappingDone });
          if (btn.dataset.rulesetAction === "edit" && !rs.built_in) openRulesetForm({ mode: "edit", ruleset: rs, trigger: btn, fromMapping, mappingDraft, mappingDone });
          if (btn.dataset.rulesetAction === "delete" && !rs.built_in) openRulesetDelete(rs, btn, { fromMapping, mappingDraft, trigger, mappingDone });
        });
      },
      onPrimary: async () => openRulesetForm({ mode: "create", trigger, fromMapping, mappingDraft, mappingDone }),
    });
  }

  function openRulesetForm({ mode, ruleset = null, trigger = null, fromMapping = false, mappingDraft = null, mappingDone = null } = {}) {
    const readonly = mode === "view" || (ruleset && ruleset.built_in && mode === "edit");
    const clone = mode === "clone";
    const isEdit = mode === "edit";
    const seed = { ...RULESET_DEFAULTS, ...(ruleset || {}) };
    if (clone || mode === "create") {
      seed.id = "";
      seed.built_in = false;
      seed.name = clone ? "" : "";
    }
    const preset = mode === "create" && !clone ? "direct" : detectRulesetPreset(seed);
    const title = mode === "view" ? "View ruleset" : isEdit ? "Edit ruleset" : clone ? "Clone ruleset" : "Create new ruleset";
    const body = rulesetBuilderHtml(seed, preset, readonly, clone);
    openModal({
      title,
      description: readonly ? "Review this ruleset as readable behaviour. Built in rulesets can be cloned, but not edited." : "Choose a preset, adjust the rule behaviour, then review the generated ruleset before saving.",
      body,
      trigger,
      width: "920px",
      primaryText: readonly && ruleset && ruleset.built_in ? "Clone ruleset" : readonly ? "" : (isEdit ? "Save changes" : "Create ruleset"),
      savingText: "Saving ruleset...",
      onCancel: () => openRulesetManager({ trigger, fromMapping, mappingDraft, mappingDone }),
      onOpen: (ctx) => hydrateRulesetBuilder(ctx, seed, preset, readonly),
      onPrimary: async (ctx) => {
        if (readonly && ruleset && ruleset.built_in) {
          openRulesetForm({ mode: "clone", ruleset, trigger, fromMapping, mappingDraft, mappingDone });
          return;
        }
        const payload = readRulesetForm(ctx.modal, isEdit ? seed.id : "");
        const nameErr = nameFieldError(payload.name, "Ruleset name");
        if (nameErr) throw new Error(nameErr);
        const error = validateRulesetBuilder(payload);
        if (error) throw new Error(error);
        const res = await API.rulesetUpsert(payload);
        closeModal(true);
        await reloadData();
        if (fromMapping && mappingDraft) {
          mappingDraft.ruleset_id = (res.ruleset && res.ruleset.id) || payload.id || "";
          openMappingModal({ draft: mappingDraft, trigger, onDone: mappingDone });
        } else {
          render($("#page-playlists"));
          openRulesetManager({ trigger });
        }
      },
    });
  }

  function rulesetBuilderHtml(seed, preset, readonly, clone) {
    const current = { ...RULESET_DEFAULTS, ...seed };
    const presetCards = Object.entries(RULESET_PRESETS).map(([key, item]) => `
      <button type="button" class="pl-preset-card ${key === preset ? "active" : ""}" data-preset="${esc(key)}" ${readonly ? "disabled" : ""}>
        <b>${esc(item.label)}</b>
        <span>${esc(item.description)}</span>
      </button>
    `).join("");
    return `
      <div class="pl-builder">
        <section class="pl-builder-section" data-builder-section="basics">
          <div class="pl-builder-title"><div><b>Basics</b><span>Name the ruleset and choose a starting point.</span></div></div>
          <div class="pl-builder-grid">
            <div class="pl-field">
              <label for="pl-rs-name">Ruleset name</label>
              <input id="pl-rs-name" maxlength="${NAME_MAX}" value="${esc(current.name || "")}" placeholder="${clone ? "Custom" : "Ruleset"}" aria-describedby="pl-rs-name-error" ${readonly ? "disabled" : ""}>
              <div class="pl-field-error" id="pl-rs-name-error"></div>
            </div>
            <div class="pl-field">
              <label for="pl-rs-direction">Supported direction</label>
              <select id="pl-rs-direction" data-rs-field="direction" ${readonly ? "disabled" : ""}>${selectOptions(ENUMS.direction.map(([value, label]) => ({ value, label })), current.direction)}</select>
            </div>
            <div class="pl-field">
              <label for="pl-rs-description">Description</label>
              <input id="pl-rs-description" value="${esc(current.description || "")}" placeholder="Optional note for this ruleset" ${readonly ? "disabled" : ""}>
            </div>
            <div class="pl-field">
              <label for="pl-rs-preset">Preset</label>
              <select id="pl-rs-preset" ${readonly ? "disabled" : ""}>${selectOptions(Object.entries(RULESET_PRESETS).map(([value, item]) => ({ value, label: item.label })), preset)}</select>
            </div>
          </div>
          <div class="pl-preset-grid">${presetCards}</div>
          <div class="pl-help" id="pl-rs-preset-help">${esc(RULESET_PRESETS[preset].description)}</div>
        </section>

        <section class="pl-builder-section" data-builder-section="rules">
          <div class="pl-builder-title"><div><b>Rules</b><span>Describe the conditional behaviour using controlled sentence rows.</span></div></div>
          <div class="pl-field" id="pl-rs-combine-wrap">
            <label for="pl-rs-condition-mode">Combine conditions</label>
            <select id="pl-rs-condition-mode" ${readonly ? "disabled" : ""}><option value="all">ALL conditions must match</option><option value="any">ANY condition can match</option></select>
          </div>
          <div id="pl-rs-condition-rows"></div>
          <div id="pl-rs-action-rows"></div>
          <div class="pl-inline-actions">
            <button type="button" class="pl-btn small" id="pl-rs-add-condition" ${readonly ? "disabled" : ""}>Add condition</button>
            <button type="button" class="pl-btn small" id="pl-rs-add-action" ${readonly ? "disabled" : ""}>Add action</button>
            <button type="button" class="pl-btn small" id="pl-rs-add-else" ${readonly ? "disabled" : ""}>Add else action</button>
          </div>
        </section>

        <section class="pl-builder-section" data-builder-section="policies">
          <div class="pl-builder-title"><div><b>Policies</b><span>Common nonconditional behaviour.</span></div></div>
          <div class="pl-builder-grid">
            ${builderSelectField("initial_sync", "Initial sync", "Source is authoritative", current, readonly)}
            ${builderSelectField("membership", "Membership behaviour", "How CrossWatch handles additions and removals.", current, readonly)}
            ${builderSelectField("deduplicate", "Deduplication", "Match items using canonical IDs.", current, readonly)}
            ${builderSelectField("order", "Ordering", "Choose whether source order should be preserved.", current, readonly)}
            ${builderSelectField("allocation", "Allocation", "How items are assigned when splitting.", current, readonly)}
          </div>
          <details class="pl-advanced">
            <summary>Advanced policies</summary>
            <div class="pl-advanced-body pl-builder-grid">
              ${builderSelectField("read_mode", "Aggregate behaviour", "Read one list directly or aggregate lists.", current, readonly)}
              ${builderSelectField("write_mode", "Partition behaviour", "Write directly or split across target lists.", current, readonly)}
              ${builderSelectField("rebalance", "Rebalancing", "Whether existing item assignments move between lists.", current, readonly)}
              <label class="pl-check"><input type="checkbox" id="pl-rs-track_assignments" data-rs-field="track_assignments" ${current.track_assignments ? "checked" : ""} ${readonly ? "disabled" : ""}> Track assignments</label>
            </div>
          </details>
        </section>

        <section class="pl-builder-section" data-builder-section="limits">
          <div class="pl-builder-title"><div><b>Limits and overflow</b><span>Capacity controls appear when the chosen behaviour needs them.</span></div></div>
          <div class="pl-builder-grid">
            ${builderNumberField("per_endpoint_capacity", "Capacity per target list", current.per_endpoint_capacity, readonly, "pl-limit-partition")}
            ${builderNumberField("aggregate_capacity", "Aggregate capacity", current.aggregate_capacity, readonly, "pl-limit-aggregate")}
            ${builderNumberField("maximum_targets", "Maximum generated lists", current.maximum_targets, readonly, "pl-limit-partition")}
            ${builderSelectField("overflow", "Overflow behaviour", "What happens when capacity is exceeded.", current, readonly, "pl-limit-capacity")}
          </div>
        </section>

        <section class="pl-builder-section" data-builder-section="summary">
          <div class="pl-builder-title"><div><b>Readable summary</b><span>Generated from the structured ruleset state.</span></div></div>
          <div class="pl-summary-box" id="pl-rs-summary"></div>
        </section>

        <section class="pl-builder-section" data-builder-section="preview">
          <div class="pl-builder-title"><div><b>Preview</b><span>Local capacity simulation; no backend call is made while editing.</span></div></div>
          <div class="pl-preview-box">
            <div class="pl-field">
              <label for="pl-rs-preview-items">Source items</label>
              <input id="pl-rs-preview-items" type="number" min="0" value="284" ${readonly ? "disabled" : ""}>
            </div>
            <div class="pl-preview-result" id="pl-rs-preview"></div>
          </div>
        </section>
      </div>
    `;
  }

  function builderSelectField(key, label, help, seed, readonly, extraClass) {
    return `<div class="pl-field ${esc(extraClass || "")}"><label for="pl-rs-${esc(key)}">${esc(label)}</label><select id="pl-rs-${esc(key)}" data-rs-field="${esc(key)}" ${readonly ? "disabled" : ""}>${selectOptions(ENUMS[key].map(([value, text]) => ({ value, label: text })), seed[key])}</select><div class="pl-help">${esc(help || "")}</div></div>`;
  }

  function builderNumberField(key, label, value, readonly, extraClass) {
    return `<div class="pl-field ${esc(extraClass || "")}"><label for="pl-rs-${esc(key)}">${esc(label)}</label><input id="pl-rs-${esc(key)}" data-rs-field="${esc(key)}" type="number" min="1" value="${esc(value)}" ${readonly ? "disabled" : ""}></div>`;
  }

  function hydrateRulesetBuilder(ctx, seed, originalPreset, readonly) {
    const root = ctx.modal;
    root.dataset.rulesetReadonly = readonly ? "1" : "0";
    if (!readonly) bindNameValidation(ctx, "#pl-rs-name", "Ruleset name");
    const presetSelect = $("#pl-rs-preset", root);
    const applyPreset = (key) => {
      const preset = RULESET_PRESETS[key] || RULESET_PRESETS.custom;
      if (key !== "custom") {
        writeRulesetFields(root, { ...preset.values, name: val("#pl-rs-name", root), description: val("#pl-rs-description", root) });
      }
      presetSelect.value = key;
      updatePresetCards(root, key);
      $("#pl-rs-preset-help", root).textContent = preset.description;
      updateRulesetBuilder(root, true);
    };
    presetSelect.addEventListener("change", () => applyPreset(presetSelect.value));
    $$(".pl-preset-card", root).forEach((btn) => btn.addEventListener("click", () => applyPreset(btn.dataset.preset)));
    $("#pl-rs-add-condition", root).addEventListener("click", () => {
      writeRulesetFields(root, { write_mode: "partition", per_endpoint_capacity: Number(val("#pl-rs-per_endpoint_capacity", root) || 100), maximum_targets: Math.max(2, Number(val("#pl-rs-maximum_targets", root) || 5)) });
      updateRulesetBuilder(root);
    });
    $("#pl-rs-add-action", root).addEventListener("click", () => {
      writeRulesetFields(root, { write_mode: "partition", maximum_targets: Math.max(2, Number(val("#pl-rs-maximum_targets", root) || 5)) });
      updateRulesetBuilder(root);
    });
    $("#pl-rs-add-else", root).addEventListener("click", () => {
      writeRulesetFields(root, { write_mode: "partition", maximum_targets: Math.max(2, Number(val("#pl-rs-maximum_targets", root) || 5)) });
      updateRulesetBuilder(root);
    });
    root.addEventListener("click", (e) => {
      const btn = e.target.closest("[data-remove-rule]");
      if (!btn || readonly) return;
      writeRulesetFields(root, { write_mode: "direct", maximum_targets: 1 });
      updateRulesetBuilder(root);
    });
    $$("input,select", root).forEach((el) => {
      if (el.id === "pl-rs-preset") return;
      el.addEventListener("input", () => updateRulesetBuilder(root));
      el.addEventListener("change", () => updateRulesetBuilder(root));
    });
    writeRulesetFields(root, seed);
    presetSelect.value = originalPreset;
    updatePresetCards(root, originalPreset);
    updateRulesetBuilder(root, true);
  }

  function writeRulesetFields(root, values) {
    Object.entries(values || {}).forEach(([key, value]) => {
      const el = $(`#pl-rs-${key}`, root);
      if (!el) return;
      if (el.type === "checkbox") el.checked = !!value;
      else el.value = value;
    });
  }

  function updateRulesetBuilder(root, keepPreset) {
    const rs = readRulesetForm(root, "");
    const detected = detectRulesetPreset(rs);
    const preset = $("#pl-rs-preset", root);
    if (preset && !keepPreset) {
      preset.value = detected;
      updatePresetCards(root, detected);
      $("#pl-rs-preset-help", root).textContent = RULESET_PRESETS[detected].description;
    }
    renderVisualRuleRows(root, rs);
    updateRelevantLimitFields(root, rs);
    const summary = $("#pl-rs-summary", root);
    if (summary) summary.innerHTML = rulesetSummary(rs).map((line) => `<div>${esc(line)}</div>`).join("");
    const preview = $("#pl-rs-preview", root);
    if (preview) preview.innerHTML = rulesetPreview(rs, Number(val("#pl-rs-preview-items", root) || 0));
  }

  function updatePresetCards(root, preset) {
    $$(".pl-preset-card", root).forEach((btn) => btn.classList.toggle("active", btn.dataset.preset === preset));
  }

  function renderVisualRuleRows(root, rs) {
    const conditions = $("#pl-rs-condition-rows", root);
    const actions = $("#pl-rs-action-rows", root);
    const readonly = root.dataset.rulesetReadonly === "1";
    if (!conditions || !actions) return;
    if (rs.write_mode === "partition") {
      conditions.innerHTML = `
        <div class="pl-rule-row">
          <span class="pl-rule-word">When</span>
          <select disabled><option>Source item count</option></select>
          <select disabled><option>is greater than</option></select>
          <input type="number" min="1" value="${esc(rs.per_endpoint_capacity)}" data-rule-capacity ${readonly ? "disabled" : ""}>
          <button type="button" class="pl-btn small" data-remove-rule ${readonly ? "disabled" : ""}>Remove</button>
        </div>
      `;
      actions.innerHTML = `
        <div class="pl-rule-row then">
          <span class="pl-rule-word">Then</span>
          <select disabled><option>Split into target lists</option></select>
          <input type="number" min="1" value="${esc(rs.per_endpoint_capacity)}" data-rule-action-capacity ${readonly ? "disabled" : ""}>
          <span class="pl-help">items per list</span>
          <button type="button" class="pl-btn small" data-remove-rule ${readonly ? "disabled" : ""}>Remove</button>
        </div>
        <div class="pl-rule-row then">
          <span class="pl-rule-word">Else</span>
          <select disabled><option>Sync directly</option></select>
          <span></span>
          <span class="pl-help">one target list is enough</span>
          <span></span>
        </div>
      `;
      $$("[data-rule-capacity],[data-rule-action-capacity]", root).forEach((input) => input.addEventListener("change", () => {
        $("#pl-rs-per_endpoint_capacity", root).value = input.value;
        updateRulesetBuilder(root);
      }));
    } else {
      conditions.innerHTML = `<div class="pl-empty"><strong>No condition</strong><span>This ruleset syncs directly unless you add a split condition.</span></div>`;
      actions.innerHTML = `
        <div class="pl-rule-row then">
          <span class="pl-rule-word">Then</span>
          <select disabled><option>${rs.read_mode === "aggregate" ? "Merge source lists" : "Sync directly"}</option></select>
          <span></span>
          <span class="pl-help">${rs.read_mode === "aggregate" ? "read targets as one combined list" : "write to one target list"}</span>
          <span></span>
        </div>
      `;
    }
  }

  function updateRelevantLimitFields(root, rs) {
    $$(".pl-limit-partition", root).forEach((el) => el.classList.toggle("hidden", rs.write_mode !== "partition"));
    $$(".pl-limit-aggregate", root).forEach((el) => el.classList.toggle("hidden", rs.read_mode !== "aggregate"));
    $$(".pl-limit-capacity", root).forEach((el) => el.classList.toggle("hidden", rs.write_mode !== "partition" && rs.read_mode !== "aggregate"));
  }

  function detectRulesetPreset(rs) {
    const keys = ["direction", "initial_sync", "read_mode", "write_mode", "membership", "order", "deduplicate", "allocation", "rebalance", "overflow", "per_endpoint_capacity", "aggregate_capacity", "maximum_targets", "track_assignments"];
    for (const [name, preset] of Object.entries(RULESET_PRESETS)) {
      if (name === "custom") continue;
      const values = { ...RULESET_DEFAULTS, ...preset.values };
      if (keys.every((key) => String(rs[key]) === String(values[key]))) return name;
    }
    return "custom";
  }

  function readRulesetForm(root, id) {
    return {
      id: id || "",
      name: val("#pl-rs-name", root),
      description: val("#pl-rs-description", root),
      schema_version: 1,
      built_in: false,
      direction: val("#pl-rs-direction", root),
      initial_sync: val("#pl-rs-initial_sync", root),
      read_mode: val("#pl-rs-read_mode", root),
      write_mode: val("#pl-rs-write_mode", root),
      membership: val("#pl-rs-membership", root),
      order: val("#pl-rs-order", root),
      deduplicate: val("#pl-rs-deduplicate", root),
      allocation: val("#pl-rs-allocation", root),
      rebalance: val("#pl-rs-rebalance", root),
      overflow: val("#pl-rs-overflow", root),
      per_endpoint_capacity: Number(val("#pl-rs-per_endpoint_capacity", root) || RULESET_DEFAULTS.per_endpoint_capacity),
      aggregate_capacity: Number(val("#pl-rs-aggregate_capacity", root) || RULESET_DEFAULTS.aggregate_capacity),
      maximum_targets: Number(val("#pl-rs-maximum_targets", root) || RULESET_DEFAULTS.maximum_targets),
      track_assignments: checked("#pl-rs-track_assignments", root),
    };
  }

  function rulesetSummary(rs) {
    const lines = [];
    lines.push(`This ruleset performs a ${ruleLabel(rs.direction).toLowerCase()} sync.`);
    if (rs.write_mode === "partition") {
      lines.push(`When the source contains more than ${rs.per_endpoint_capacity} items, CrossWatch splits the content into target lists containing up to ${rs.per_endpoint_capacity} items each.`);
      lines.push(`CrossWatch may create up to ${rs.maximum_targets} target lists. Additional items are ${rs.overflow === "block" ? "blocked" : titleize(rs.overflow).toLowerCase()}.`);
    } else if (rs.read_mode === "aggregate") {
      lines.push(`CrossWatch reads multiple lists as one combined list with an aggregate capacity of ${rs.aggregate_capacity} items.`);
    } else {
      lines.push("CrossWatch syncs directly between the selected source and destination playlist.");
    }
    lines.push(`Initial sync treats the source as authoritative, membership uses ${titleize(rs.membership).toLowerCase()}, and items are deduplicated using canonical IDs.`);
    lines.push(rs.order === "preserve" ? "Items remain in source order where the target provider supports ordering." : "Source ordering is not enforced.");
    return lines;
  }

  function rulesetPreview(rs, sourceItems) {
    const count = Math.max(0, Number(sourceItems) || 0);
    if (rs.write_mode !== "partition") {
      const targetLists = rs.read_mode === "aggregate" ? Math.max(1, Number(rs.maximum_targets || 1)) : 1;
      return `Target lists: ${targetLists}<br>Distribution: ${esc(String(count))}<br>Overflow: 0`;
    }
    const cap = Math.max(1, Number(rs.per_endpoint_capacity || 1));
    const maxTargets = Math.max(1, Number(rs.maximum_targets || 1));
    const needed = Math.ceil(count / cap);
    const targetLists = Math.min(maxTargets, Math.max(1, needed));
    const distribution = [];
    let remaining = Math.min(count, cap * maxTargets);
    for (let i = 0; i < targetLists; i += 1) {
      const n = Math.min(cap, remaining);
      distribution.push(n);
      remaining -= n;
    }
    const overflow = Math.max(0, count - cap * maxTargets);
    return `Target lists: ${targetLists}<br>Distribution: ${esc(distribution.join(", ") || "0")}<br>Overflow: ${overflow}`;
  }

  function validateRulesetBuilder(payload) {
    const nameErr = nameFieldError(payload.name, "Ruleset name");
    if (nameErr) return nameErr;
    if (payload.write_mode === "partition") {
      if (payload.per_endpoint_capacity < 1) return "Capacity per target list must be at least 1.";
      if (payload.maximum_targets < 2) return "Splitting requires at least two generated lists.";
      if (payload.overflow !== "block") return "The current backend only supports blocking overflow.";
    }
    if (payload.read_mode === "aggregate" && payload.aggregate_capacity < 1) return "Aggregate capacity must be at least 1.";
    return "";
  }

  function openRulesetDelete(ruleset, trigger, context) {
    const used = mappingsForRuleset(ruleset.id);
    const body = `
      <div class="pl-confirm-lines">
        <div><b>Ruleset:</b> ${esc(ruleset.name)}</div>
        <div><b>Mappings:</b> ${esc(used.length)}</div>
        ${used.length ? `<div class="pl-warning">Deletion is blocked while mappings reference this ruleset: ${esc(used.map((m) => m.name || m.id).join(", "))}. Change those mappings first.</div>` : `<div>Deleting this custom ruleset will not delete endpoints or mappings.</div>`}
      </div>
    `;
    openModal({
      title: "Delete ruleset",
      description: "Confirm removal of this custom ruleset.",
      body,
      trigger,
      primaryText: "Delete ruleset",
      savingText: "Deleting...",
      onCancel: () => openRulesetManager({ trigger: context.trigger, fromMapping: context.fromMapping, mappingDraft: context.mappingDraft, mappingDone: context.mappingDone }),
      onPrimary: async () => {
        if (used.length) throw new Error("Ruleset is still used by mappings. Change those mappings first.");
        await API.rulesetDelete(ruleset.id);
        closeModal(true);
        await reloadData();
        render($("#page-playlists"));
        openRulesetManager({ trigger: context.trigger, fromMapping: context.fromMapping, mappingDraft: context.mappingDraft, mappingDone: context.mappingDone });
      },
    });
  }

  function openActivityClear(trigger) {
    const runCount = activityStats(state.activity || []).total;
    const body = `
      <div class="pl-confirm-lines">
        <div><b>Playlist runs:</b> ${esc(runCount)}</div>
        <div>This clears playlist activity timestamps and mapping results. Endpoints, mappings and provider playlists are not deleted.</div>
      </div>
    `;
    openModal({
      title: "Clear playlist activity",
      description: "Reset the playlist activity overview and counters.",
      body,
      trigger,
      primaryText: "Clear activity",
      savingText: "Clearing...",
      onPrimary: async () => {
        const cleared = await API.activityClear();
        state.activity = Array.isArray(cleared.activity) ? cleared.activity : [];
        if (cleared.overview) state.overview = cleared.overview;
        closeModal(true);
        await refreshOverview(["endpoints", "mappings", "activity"]);
      },
    });
  }

  function openActivityModal(trigger) {
    const body = state.activity.length ? `
      <div class="pl-table-wrap">
        <table>
          <thead><tr><th>Time</th><th>Type</th><th>Mapping</th><th>Details</th><th>Status</th></tr></thead>
          <tbody>${state.activity.map((row) => `<tr><td>${esc(compactTime(row.ts))}</td><td>${esc(row.type || "-")}</td><td>${esc(row.label || "-")}</td><td>${esc(row.details || "-")}</td><td><span class="pl-pill ${row.status === "error" ? "err" : "ok"}">${esc(titleize(row.status || "completed"))}</span></td></tr>`).join("")}</tbody>
        </table>
      </div>
    ` : `<div class="pl-empty"><strong>No activity yet</strong><span>Playlist activity will appear after refreshes or sync runs.</span></div>`;
    openModal({ title: "Playlist activity", description: "Full recent activity returned by the playlist API.", body, trigger, width: "980px" });
  }

  function openNotice(title, message, trigger) {
    openModal({ title, description: message, body: "", trigger });
  }

  function notifyPairsChanged(detail) {
    try {
      window.dispatchEvent(new CustomEvent("cx:pairs:changed", { detail: detail || { source: "playlists" } }));
    } catch {}
  }

  async function loadPlaylistDataAttempt() {
    const [providers, endpoints, mappings, rulesets, overview, activity, runSummary] = await Promise.all([
      API.providers(),
      API.endpoints(),
      API.mappings(),
      API.rulesets(),
      API.overview(),
      API.activity(),
      API.runSummary().catch(() => null),
    ]);
    return { providers, endpoints, mappings, rulesets, overview, activity, runSummary };
  }

  async function reloadData() {
    state.error = "";
    let data = null;
    for (let attempt = 0; attempt <= EMPTY_PROVIDER_RETRIES; attempt += 1) {
      data = await loadPlaylistDataAttempt();
      if ((data.providers.providers || []).length || attempt >= EMPTY_PROVIDER_RETRIES) break;
      await delay(EMPTY_PROVIDER_RETRY_MS * (attempt + 1));
    }
    const { providers, endpoints, mappings, rulesets, overview, activity, runSummary } = data;
    state.providers = providers.providers || [];
    state.endpoints = endpoints.endpoints || [];
    state.mappings = mappings.mappings || [];
    state.rulesets = rulesets.rulesets || [];
    state.overview = overview || {};
    state.activity = activity.activity || [];
    state.syncSummary = runSummary || null;
    reconcileSyncSummary();
    state.loaded = true;
  }

  function updateMappingActions(root) {
    const disabled = !state.loaded || state.endpoints.length < 2;
    $$("[data-action='mapping-new'], #pl-new-mapping", root).forEach((btn) => {
      btn.disabled = disabled;
      btn.title = !state.loaded ? "Playlist data is still loading." : disabled ? "Create at least two endpoints before adding a mapping." : "Create playlist mapping";
    });
  }

  function refreshSection(root, key) {
    const targets = {
      endpoints: ["#pl-playlist-endpoints .pl-section-body", renderEndpoints],
      mappings: ["#pl-mappings-overview .pl-section-body", renderMappings],
      activity: ["#pl-activity-overview .pl-section-body", renderActivity],
    };
    const spec = targets[key];
    if (!spec) return;
    const el = $(spec[0], root);
    if (el) el.innerHTML = spec[1]();
  }

  async function refreshOverview(sections = ["endpoints", "mappings", "activity"]) {
    const root = $("#page-playlists");
    const scrollX = window.scrollX;
    const scrollY = window.scrollY;
    try {
      await reloadData();
    } catch (err) {
      state.error = err && err.message ? err.message : "Could not refresh playlists.";
    }
    if (!root) return;
    if (!root.querySelector(".pl-page")) {
      render(root);
      return;
    }
    const banners = $(".pl-banners", root);
    if (banners) banners.outerHTML = renderBanners();
    updateMappingActions(root);
    sections.forEach((key) => refreshSection(root, key));
    scheduleSyncSummaryPoll(syncSummaryRunning() || state.runningMappings.size ? 1500 : 6000);
    window.scrollTo(scrollX, scrollY);
  }

  async function reload() {
    const root = $("#page-playlists");
    if (!root) return;
    state.loading = true;
    render(root);
    try {
      await reloadData();
    } catch (err) {
      state.error = err && err.message ? err.message : String(err || "Could not load playlists.");
    } finally {
      state.loading = false;
      render(root);
      scheduleSyncSummaryPoll(syncSummaryRunning() || state.runningMappings.size ? 1500 : 6000);
    }
  }

  function returnToSyncPairsOverview() {
    if (typeof window.showTab === "function") window.showTab("settings");
    setTimeout(() => {
      if (typeof window.cwSettingsSelect === "function") window.cwSettingsSelect("sync");
      const list = document.getElementById("pairs_list");
      if (list && typeof list.scrollIntoView === "function") list.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 0);
  }

  async function openMappingForPair(pairId, trigger, opts = {}) {
    const id = String(pairId || "").trim();
    if (!id) return;
    await refreshOverview();
    let mappings = state.mappings.filter((m) => String(m.assigned_pair || "") === id);
    try {
      const data = await API.pairMappings(id);
      if (Array.isArray(data.mappings)) mappings = data.mappings;
    } catch {}
    if (!mappings.length) {
      openNotice("Playlist mapping missing", "This sync pair does not have a managed playlist mapping assigned.", trigger);
      return;
    }
    const mapping = state.mappings.find((m) => m.id === mappings[0].id) || mappings[0];
    openMappingModal({ mapping, trigger, onDone: opts && opts.returnToSyncPairs ? returnToSyncPairsOverview : null });
  }

  async function init() {
    await reload();
  }

  window.initPlaylistsPage = init;
  window.Playlists = { mount: init, openMappingForPair };
  if ($("#page-playlists") && document.documentElement.dataset.tab === "playlists") init();
})();
