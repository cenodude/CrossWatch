/* assets/js/playlists.js */
/* Playlists page shell and components */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function () {
  "use strict";

  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const LOCALE = () => (window.CW?.I18n || window.__cwI18n)?.locale;

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
  const PLAYLIST_COMPATIBLE_PROVIDERS = new Set(["PLEX", "TRAKT", "MDBLIST", "JELLYFIN", "EMBY", "PUBLICMETADB", "SIMKL", "TMDB", "FLICKLIST", "SILO", "TOFA", "CROSSWATCH"]);
  const SIMKL_PLAYLIST_WARNING = T("playlists.simklWarning");
  const PRIVACY_LABELS = { public: T("playlists.privacy.public"), private: T("playlists.privacy.private"), unlisted: T("playlists.privacy.unlisted"), friends: T("playlists.privacy.friends"), friends_only: T("playlists.privacy.friendsOnly"), friends_of_friends: T("playlists.privacy.friendsOfFriends"), plex: T("playlists.privacy.plex"), link: T("playlists.privacy.link") };
  const CREATE_TYPE_LABELS = { movie: T("playlists.type.movies"), show: T("playlists.type.shows"), anime: T("playlists.type.anime") };
  const RULESET_PRESETS = {
    direct: {
      label: T("playlists.presets.direct"),
      description: T("playlists.presets.directHint"),
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "direct", write_mode: "direct", membership: "managed_only", order: "ignore", maximum_targets: 1 },
    },
    mirror: {
      label: T("playlists.presets.mirror"),
      description: T("playlists.presets.mirrorHint"),
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "direct", write_mode: "direct", membership: "mirror", order: "preserve", maximum_targets: 1 },
    },
    split: {
      label: T("playlists.presets.split"),
      description: T("playlists.presets.splitHint"),
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "direct", write_mode: "partition", membership: "managed_only", order: "ignore", per_endpoint_capacity: 100, aggregate_capacity: 1000, maximum_targets: 5, overflow: "block", track_assignments: true },
    },
    merge: {
      label: T("playlists.presets.merge"),
      description: T("playlists.presets.mergeHint"),
      values: { ...RULESET_DEFAULTS, direction: "one_way", read_mode: "aggregate", write_mode: "direct", membership: "managed_only", order: "ignore", aggregate_capacity: 1000, maximum_targets: 1 },
    },
    limited: {
      label: T("playlists.presets.limited"),
      description: T("playlists.presets.limitedHint"),
      values: { ...RULESET_DEFAULTS, direction: "bidirectional", read_mode: "aggregate", write_mode: "partition", membership: "managed_only", order: "ignore", per_endpoint_capacity: 250, aggregate_capacity: 1000, maximum_targets: 5, overflow: "block", track_assignments: true },
    },
    custom: {
      label: T("playlists.presets.custom"),
      description: T("playlists.presets.customHint"),
      values: { ...RULESET_DEFAULTS },
    },
  };
  const ENUMS = {
    direction: [["one_way", T("playlists.enums.oneWay")], ["bidirectional", T("playlists.enums.bidirectional")]],
    initial_sync: [["source_authoritative", T("playlists.enums.sourceAuthoritative")]],
    read_mode: [["direct", T("playlists.enums.direct")], ["aggregate", T("playlists.enums.aggregate")]],
    write_mode: [["direct", T("playlists.enums.direct")], ["partition", T("playlists.enums.partition")]],
    membership: [["add_only", T("playlists.enums.addOnly")], ["managed_only", T("playlists.enums.managedOnly")], ["mirror", T("playlists.enums.mirror")]],
    order: [["ignore", T("playlists.enums.ignore")], ["preserve", T("playlists.enums.preserve")]],
    deduplicate: [["canonical_id", T("playlists.enums.canonicalId")]],
    allocation: [["stable_first_fit", T("playlists.enums.stableFirstFit")]],
    rebalance: [["never", T("playlists.never")]],
    overflow: [["block", T("playlists.enums.block")]],
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
    resourceResolve: (provider, instance, ref) => request(`${BASE}/resources/resolve`, { method: "POST", body: JSON.stringify({ provider, instance: instance || "default", ref }) }),
    resourceRename: (provider, instance, id, name) => request(`${BASE}/resources/${encodeURIComponent(id)}`, { method: "PATCH", body: JSON.stringify({ provider, instance: instance || "default", name }) }),
    resourceUpdate: (provider, instance, id, changes) => request(`${BASE}/resources/${encodeURIComponent(id)}`, { method: "PATCH", body: JSON.stringify({ provider, instance: instance || "default", ...changes }) }),
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
    runSummary: () => request("/api/run/summary"),
    pairMappings: (id) => request(`${BASE}/pairs/${encodeURIComponent(id)}/mappings`),
    rulesets: () => request(`${BASE}/rulesets`),
    rulesetUpsert: (body) => request(`${BASE}/rulesets`, { method: "POST", body: JSON.stringify(body) }),
    rulesetDelete: (id) => request(`${BASE}/rulesets/${encodeURIComponent(id)}`, { method: "DELETE" }),
    published: () => request(`${BASE}/published`),
    publish: (instance, listId) => request(`${BASE}/published`, { method: "POST", body: JSON.stringify({ instance, list_id: listId }) }),
    publishKey: (id) => request(`${BASE}/published/${encodeURIComponent(id)}/key`, { method: "POST" }),
    unpublish: (id) => request(`${BASE}/published/${encodeURIComponent(id)}`, { method: "DELETE" }),
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
  const ruleLabel = (key) => key === "one_way" ? T("playlists.enums.oneWay") : key === "bidirectional" ? T("playlists.enums.bidirectional") : titleize(key);
  const titleize = (v) => String(v || "").replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const timeText = (ts) => ts ? new Date(Number(ts) * 1000).toLocaleString(LOCALE()) : T("playlists.never");
  const compactTime = (ts) => ts ? new Date(Number(ts) * 1000).toLocaleString(LOCALE()) : "-";
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));


  const state = {
    providers: [],
    endpoints: [],
    mappings: [],
    rulesets: [],
    overview: {},
    activity: [],
    advanced: (() => { try { return localStorage.getItem("cw.playlists.advanced") === "1"; } catch { return false; } })(),
    runningEndpoints: new Set(),
    syncSummary: null,
    syncPollTimer: 0,
    syncPollBusy: false,
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

  function createTypeLabel(type) {
    if (type === "cloud_list") return T("playlists.cloudList");
    return CREATE_TYPE_LABELS[type] || titleize(type);
  }

  function createPrivacyOptions(provider, endpointType) {
    const key = String(provider || "").toUpperCase();
    const found = state.providers.find((p) => String(p.provider || "").toUpperCase() === key);
    const raw = found && found.capabilities && found.capabilities.create_privacy;
    const values = raw && Array.isArray(raw.values) ? raw.values.map((v) => String(v || "").toLowerCase()).filter(Boolean) : [];
    if (!values.length) return null;
    const types = Array.isArray(raw.endpoint_types) ? raw.endpoint_types.map((t) => String(t || "").toLowerCase()) : [];
    if (types.length && endpointType && !types.includes(String(endpointType).toLowerCase())) return null;
    const def = String(raw.default || "").toLowerCase();
    return { values, default: values.includes(def) ? def : values[0] };
  }

  function fillPrivacySelect(select, provider, endpointType) {
    const opts = select ? createPrivacyOptions(provider, endpointType) : null;
    if (!select) return null;
    if (!opts) {
      select.innerHTML = "";
      return null;
    }
    const current = select.value;
    select.innerHTML = selectOptions(opts.values.map((v) => ({ value: v, label: PRIVACY_LABELS[v] || titleize(v) })), opts.values.includes(current) ? current : opts.default);
    window.CW?.IconSelect?.enhance?.(select, { className: "cw-plain-select" });
    return opts;
  }

  function privacyPayload(select, wrapHidden) {
    const value = !wrapHidden && select ? String(select.value || "").trim() : "";
    return value ? { privacy: value } : {};
  }

  function providerCanResolve(provider) {
    const key = String(provider || "").toUpperCase();
    const found = state.providers.find((p) => String(p.provider || "").toUpperCase() === key);
    return !!(found && found.capabilities && found.capabilities.resolve);
  }

  function providerLinkExample(provider) {
    const key = String(provider || "").toUpperCase();
    const found = state.providers.find((p) => String(p.provider || "").toUpperCase() === key);
    return String((found && found.capabilities && found.capabilities.link_example) || T("playlists.pasteLink"));
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
        <span>${TH("playlists.needsProvider")}</span>
        <button type="button" class="pl-btn small" data-action="open-connections">${TH("playlists.openConnections")}</button>
      </div>
    ` : "";
    const reconnect = state.loaded ? state.providers.filter((p) => p && !p.configured && p.reason === "reconnect_required") : [];
    const reconnectGate = reconnect.length ? `
      <div class="pl-banner warn">
        <span class="material-symbols-rounded" aria-hidden="true">link</span>
        <span>${TH("playlists.reconnectNeeded", {names: reconnect.map((p) => p.label || providerLabel(p.provider)).filter((v, i, a) => a.indexOf(v) === i).join(", ")})}</span>
        <button type="button" class="pl-btn small" data-action="open-connections">${TH("playlists.openConnections")}</button>
      </div>
    ` : "";
    if (!errorGate && !providerGate && !reconnectGate) return "";
    return `
      <div class="pl-banners">
        ${errorGate}
        ${providerGate}
        ${reconnectGate}
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
    return rs ? ruleLabel(rs.direction) : T("playlists.enums.oneWay");
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
    return syncSummaryRunning();
  }

  function mappingIsRunning(mapping) {
    if (!syncSummaryRunning()) return false;
    const pair = String((mapping && mapping.assigned_pair) || "").trim();
    if (!pair) return false;
    const scope = syncSummaryPairScopeIds();
    return scope.size > 0 && scope.has(pair);
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
      if (renderOnly) scheduleSyncSummaryPoll(syncSummaryRunning() ? 1500 : 6000);
    }
  }

  function statusForResult(result) {
    if (!result) return `<span class="pl-pill off">${TH("playlists.status.noRuns")}</span>`;
    const unresolved = Number(result.unresolved_count || result.unresolved || 0);
    const errors = Number(result.errors || result.error_count || 0);
    const warningBits = [];
    if (unresolved > 0) warningBits.push(T("playlists.count.unresolved", {count: unresolved}));
    const warnings = Array.isArray(result.warnings) ? result.warnings.length : 0;
    if (warnings) warningBits.push(T("playlists.count.warning", {count: warnings}));
    if (result.capacity_error || errors > 0 || (result.ok === false && !warningBits.length)) return `<span class="pl-pill err" title="${esc(result.error || "")}">${TH("playlists.status.failed")}</span>`;
    if (warningBits.length) return `<span class="pl-pill warn" title="${esc(warningBits.join(", "))}"><span class="material-symbols-rounded" aria-hidden="true">warning</span>${esc(warningBits[0])}</span>`;
    return `<span class="pl-pill ok">${TH("playlists.status.success")}</span>`;
  }

  function endpointStatus(endpoint) {
    if (!endpoint.playlist_id) return `<span class="pl-state warn"><span class="material-symbols-rounded" aria-hidden="true">warning</span>${TH("playlists.status.incomplete")}</span>`;
    return `<span class="pl-state ok"><span class="material-symbols-rounded" aria-hidden="true">check_circle</span>${TH("playlists.status.connected")}</span>`;
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
      return `<span class="pl-icon-status warn" title="${TH("playlists.noPair")}" aria-label="${TH("playlists.noPair")}"><span class="material-symbols-rounded" aria-hidden="true">warning</span></span>`;
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
    if (counts.unresolved) chips.push(activityChangeChip(T("playlists.changes.unresolved"), counts.unresolved, "help", "unresolved"));
    if (counts.added) chips.push(activityChangeChip(T("playlists.changes.added"), counts.added, "add_circle", "added"));
    if (counts.updated) chips.push(activityChangeChip(T("playlists.changes.updated"), counts.updated, "sync", "updated"));
    if (counts.removed) chips.push(activityChangeChip(T("playlists.changes.removed"), counts.removed, "remove_circle", "removed"));
    if (counts.skipped) chips.push(activityChangeChip(T("playlists.changes.skipped"), counts.skipped, "skip_next", "skipped"));
    return `<div class="pl-change-set">${chips.length ? chips.join("") : `<span class="pl-muted">${TH("playlists.changes.none")}</span>`}</div>`;
  }

  function activityStatus(row) {
    const status = String(row.status || "").toLowerCase();
    if (status === "error" || status === "failed") return statusPill("err", titleize(status || "error"), "cancel");
    if (status === "warning" || parseActivityCounts(row).unresolved > 0) return statusPill("warn", T("playlists.status.warning"), "warning");
    return statusPill("ok", titleize(row.status || "completed"), "check_circle");
  }

  function activityResultSummary(row) {
    const details = String(row.details || "");
    const rule = row.ruleset || (details.split(",")[0] || "direct");
    const rawTargets = row.target_count != null ? Number(row.target_count) : Number((details.match(/(\d+)\s+target/i) || [])[1] || 0);
    const targetText = T("playlists.count.target", {count: rawTargets});
    return `<div class="pl-main-text">${esc(titleize(rule || "direct"))}</div><div class="pl-muted">${esc(targetText)}</div>`;
  }

  function listTypeLabel(ep) {
    if (endpointIsDiscovery(ep)) return T("playlists.listType.discovery");
    const id = String(ep.playlist_id || "").toLowerCase();
    const raw = String(ep.playlist_type || ep.endpoint_type || ep.kind || "").toLowerCase();
    if (id.includes("watchlist") || raw === "watchlist") return T("playlists.listType.watchlist");
    if (raw === "custom_list") return T("playlists.listType.custom");
    if (raw === "cloud_list") return T("playlists.cloudList");
    if (raw === "liked_list") return T("playlists.listType.liked");
    if (raw === "followed_list") return T("playlists.listType.followed");
    if (raw === "public_list") return T("playlists.listType.public");
    if (raw === "collaboration") return T("playlists.listType.shared");
    if (raw === "status_bucket") return T("playlists.col.status");
    if (raw === "collection") return T("playlists.listType.collection");
    if (raw === "favorites") return T("playlists.listType.favorites");
    if (raw === "smart") return T("playlists.listType.smart");
    return T("playlists.listType.playlist");
  }

  function listRef(ep, fallback) {
    if (!ep || !ep.provider) return `<div class="pl-muted">${esc(fallback || "-")}</div>`;
    const profile = String(ep.instance || "default");
    const sub = [ep.provider_label || providerLabel(ep.provider), profile !== "default" ? profile : ""].filter(Boolean).join(" · ");
    return `<div class="pl-provider">${icon(ep.provider)}<div><div class="pl-main-text">${esc(ep.playlist_name || ep.playlist_id || ep.name || "-")}</div><div class="pl-muted">${esc(sub)}</div></div></div>`;
  }

  function syncModeLabel(mapping) {
    const rule = mapping.ruleset || rulesetById(mapping.ruleset_id || "");
    if (rule) return [rule.name, T("playlists.usesRuleset")];
    const modes = {
      add_only: [T("playlists.enums.addOnly"), T("playlists.modes.addOnlyHint")],
      managed_only: [T("playlists.modes.keepInSync"), T("playlists.modes.keepInSyncHint")],
      mirror: [T("playlists.modes.exactCopy"), T("playlists.modes.exactCopyHint")],
    };
    return modes[String(mapping.membership || "managed_only")] || modes.managed_only;
  }

  function renderListsSimple() {
    if (state.loading && !state.loaded) return renderSkeleton(T("playlists.loadingLists"));
    if (!state.endpoints.length) {
      return `<div class="pl-empty"><strong>${TH("playlists.noLists")}</strong><span>${TH("playlists.noListsHint")}</span></div>`;
    }
    const rows = state.endpoints.map((ep) => {
      const usedBy = state.mappings.filter((m) => mappingUsesEndpoint(m, ep.id)).length;
      const refreshing = state.runningEndpoints.has(String(ep.id || ""));
      return `
        <tr>
          <td>${listRef(ep)}</td>
          <td><span class="pl-pill type">${esc(listTypeLabel(ep))}</span>${ep.playlist_id ? "" : ` ${endpointStatus(ep)}`}</td>
          <td><div class="pl-muted">${ep.item_count != null ? `${esc(ep.item_count)} items<br>` : ""}${refreshing ? T("playlists.refreshing") : ep.last_synced ? TH("playlists.checkedAt", {time: timeText(ep.last_synced)}) : T("playlists.notChecked")}</div></td>
          <td>
            <div class="pl-actions">
              ${actionButton("endpoint-sync", ep.id, refreshing ? T("playlists.refreshRunning") : T("playlists.refreshList"), "refresh", "refresh", refreshing ? "running" : "", refreshing)}
              ${actionButton("endpoint-edit", ep.id, T("playlists.changeList"), "edit", "edit")}
              ${actionButton("endpoint-delete", ep.id, usedBy ? T("playlists.removeListUsed", {count: usedBy}) : T("playlists.removeList"), "delete", "delete")}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap cw-page-table">
        <table class="pl-lists-table">
          <thead><tr><th>${TH("playlists.col.list")}</th><th>${TH("playlists.col.type")}</th><th>${TH("playlists.col.contents")}</th><th aria-label="${TH("playlists.col.actions")}">${TH("playlists.col.actions")}</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderSyncsSimple() {
    if (state.loading && !state.loaded) return renderSkeleton(T("playlists.loadingSyncs"));
    if (!state.mappings.length) {
      const need = state.endpoints.length < 2;
      return `<div class="pl-empty"><strong>${TH("playlists.noSyncs")}</strong><span>${TH("playlists.noSyncsHint")}</span></div>`;
    }
    const rows = state.mappings.map((m) => {
      const src = endpointById(m.source_endpoint) || m.source || {};
      const targets = (m.target_endpoints || []).map((id) => endpointById(id)).filter(Boolean);
      const res = m.last_result || null;
      const busy = sharedSyncBusy();
      const running = mappingIsRunning(m);
      const mode = syncModeLabel(m);
      const result = running ? statusPill("run", T("playlists.running"), "sync") : !m.enabled ? statusPill("off", T("playlists.paused"), "pause_circle") : statusForResult(res);
      return `
        <tr class="${running ? "is-running" : ""}">
          <td>${listRef(src, m.source_endpoint)}</td>
          <td class="pl-sync-arrow"><span class="material-symbols-rounded" aria-hidden="true">${directionFor(m) === T("playlists.enums.bidirectional") ? "sync_alt" : "east"}</span></td>
          <td>${targets.length ? `<div class="pl-endpoint-stack">${targets.map((t) => listRef(t)).join("")}</div>` : mappingTargetRefs(m)}</td>
          <td><div class="pl-main-text">${esc(mode[0])}</div><div class="pl-muted">${esc(mode[1])}</div></td>
          <td><div class="pl-result-cell">${result}<div class="pl-muted">${running ? T("playlists.runningNow") : compactTime(res && res.finished_at)}</div></div></td>
          <td>
            <div class="pl-actions">
              <button class="pl-btn small" data-action="mapping-sync" data-id="${esc(m.id)}" ${busy ? "disabled" : ""} title="${esc(busy ? T("playlists.alreadyRunning") : T("playlists.reviewSyncHint"))}"><span class="material-symbols-rounded" aria-hidden="true">fact_check</span>${TH("playlists.reviewSync")}</button>
              ${actionButton("mapping-toggle", m.id, m.enabled ? T("playlists.pauseSync") : T("playlists.resumeSync"), m.enabled ? "pause" : "play_arrow", "toggle", m.enabled ? "on" : "")}
              ${actionButton("mapping-edit", m.id, T("playlists.editSync"), "edit", "edit")}
              ${actionButton("mapping-delete", m.id, T("playlists.deleteSync"), "delete", "delete")}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap cw-page-table">
        <table class="pl-syncs-table">
          <thead><tr><th>${TH("playlists.col.from")}</th><th aria-hidden="true"></th><th>${TH("playlists.col.to")}</th><th>${TH("playlists.col.how")}</th><th>${TH("playlists.col.lastResult")}</th><th aria-label="${TH("playlists.col.actions")}">${TH("playlists.col.actions")}</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function agoText(ts) {
    if (!ts) return T("playlists.neverLower");
    const mins = Math.floor(Math.max(0, Date.now() / 1000 - Number(ts)) / 60);
    if (mins < 1) return T("playlists.ago.justNow");
    if (mins < 60) return T("playlists.ago.minutes", {count: mins});
    const hours = Math.floor(mins / 60);
    if (hours < 24) return T("playlists.ago.hours", {count: hours});
    const days = Math.floor(hours / 24);
    return T("playlists.ago.days", {count: days});
  }

  function heroSummary() {
    const published = ((state.published && state.published.lists) || []).filter((row) => row.feed).length;
    const last = Math.max(0, ...state.mappings.map((m) => Number((m.last_result && m.last_result.finished_at) || 0)));
    const seg = (value, label, extra = "") => `<div class="cw-editor-hero-seg ${extra}"><strong>${esc(value)}</strong><span>${esc(label)}</span></div>`;
    return `
      ${seg(state.loaded ? state.mappings.length : "-", T("playlists.hero.syncs", {count: state.mappings.length}), "cw-editor-hero-count")}
      ${seg(state.loaded ? state.endpoints.length : "-", T("playlists.hero.lists", {count: state.endpoints.length}), "cw-editor-hero-count")}
      ${seg(state.loaded ? published : "-", T("playlists.hero.published"), "cw-editor-hero-count")}
      <div class="cw-editor-hero-seg cw-editor-hero-sync"><span>${TH("playlists.synced")}</span><strong>${esc(agoText(last))}</strong></div>
      <button class="cw-editor-refresh" id="pl-hero-refresh" type="button" data-action="hero-refresh" title="${TH("playlists.refreshPlaylists")}" aria-label="${TH("playlists.refreshPlaylists")}"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
      <button class="cw-editor-hero-seg pl-hero-toggle" id="pl-advanced-toggle" type="button" data-action="toggle-advanced" title="${state.advanced ? T("playlists.backToSimple") : T("playlists.showAdvanced")}"><span class="material-symbols-rounded" aria-hidden="true">${state.advanced ? "view_agenda" : "tune"}</span><span>${state.advanced ? T("playlists.simple") : T("playlists.advanced")}</span></button>
    `;
  }

  function refreshHero(root) {
    const el = $("#pl-hero-summary", root || $("#page-playlists"));
    if (el) el.innerHTML = heroSummary();
  }

  async function heroRefresh(btn) {
    btn.disabled = true;
    btn.classList.add("is-refreshing");
    try { await refreshOverview(); } finally { refreshHero(); }
  }

  function render(root) {
    const simple = !state.advanced;
    const mappingDisabled = !state.loaded || state.endpoints.length < 2;
    const mappingTitle = !state.loaded ? T("playlists.stillLoading") : mappingDisabled ? T("playlists.needTwoEndpoints") : T("playlists.createMapping");
    root.innerHTML = `
      <div class="pl-page${simple ? " pl-simple" : ""}">
        <div class="pl-header cw-page-hero cw-page-hero-playlists" data-hero-icon="queue_music">
          <div class="cw-page-hero-copy">
            <div class="cw-page-hero-kicker">${TH("playlists.kicker")}</div>
            <h2 class="pl-title cw-page-hero-title">${TH("playlists.title")}</h2>
            <div class="pl-sub cw-page-hero-sub">${TH("playlists.subtitle")}</div>
          </div>
          <div class="pl-header-actions cw-page-hero-actions">
            <div class="cw-editor-hero-summary" id="pl-hero-summary" aria-label="${TH("playlists.summaryLabel")}">${heroSummary()}</div>
          </div>
        </div>
        ${renderBanners()}
        <main class="pl-grid">
          <section class="pl-section cw-page-panel" id="pl-playlist-endpoints">
            <div class="pl-section-head">
              <div><div class="pl-section-title">${simple ? T("playlists.lists") : T("playlists.endpoints")}</div><div class="pl-section-sub">${simple ? T("playlists.listsHint") : T("playlists.endpointsHint")}</div></div>
              <button class="pl-btn small${simple ? "" : " accent"}" data-action="endpoint-new"><span class="material-symbols-rounded" aria-hidden="true">add</span>${simple ? T("playlists.addList") : T("playlists.addEndpoint")}</button>
            </div>
            <div class="pl-section-body">${renderEndpoints()}</div>
          </section>
          <section class="pl-section cw-page-panel" id="pl-mappings-overview">
            <div class="pl-section-head">
              <div><div class="pl-section-title">${simple ? T("playlists.listSyncs") : T("playlists.mappings")}</div><div class="pl-section-sub">${simple ? T("playlists.listSyncsHint") : T("playlists.mappingsHint")}</div></div>
              <div class="pl-section-actions">
                ${simple ? `<button class="pl-btn small" data-action="activity-all">${TH("playlists.history")}</button>` : ""}
                ${simple
                  ? `<button class="pl-btn small accent" data-action="sync-wizard" ${state.loaded ? "" : "disabled"}><span class="material-symbols-rounded" aria-hidden="true">add</span>${TH("playlists.newSync")}</button>`
                  : `<button class="pl-btn small accent" data-action="mapping-new" ${mappingDisabled ? "disabled" : ""} title="${esc(mappingTitle)}"><span class="material-symbols-rounded" aria-hidden="true">add</span>${TH("playlists.newMapping")}</button>`}
              </div>
            </div>
            <div class="pl-section-body">${renderMappings()}</div>
          </section>
          <section class="pl-section cw-page-panel" id="pl-published-lists">
            <div class="pl-section-head">
              <div><div class="pl-section-title">${TH("playlists.published.title")}</div><div class="pl-section-sub">${TH("playlists.published.hint")}</div></div>
              <button class="pl-btn small" data-action="sync-wizard-publish" ${state.loaded ? "" : "disabled"}><span class="material-symbols-rounded" aria-hidden="true">add</span>${TH("playlists.published.publish")}</button>
            </div>
            <div class="pl-section-body">${renderPublished()}</div>
          </section>
          <section class="pl-section cw-page-panel" id="pl-activity-overview">
            <div class="pl-section-head">
              <div><div class="pl-section-title">${TH("playlists.activity.title")}</div></div>
              <div class="pl-section-actions">
                <button class="pl-btn small" data-action="activity-all">${TH("playlists.activity.viewAll")}</button>
                <button class="pl-btn small danger" data-action="activity-clear" title="${TH("playlists.activity.clearLabel")}"><span class="material-symbols-rounded" aria-hidden="true">delete_sweep</span>${TH("playlists.clear")}</button>
              </div>
            </div>
            <div class="pl-section-body">${renderActivity()}</div>
          </section>
        </main>
      </div>
    `;
    labelMobileTables(root);
    wirePage(root);
  }

  function labelMobileTables(root) {
    root.querySelectorAll('.pl-table-wrap table').forEach(table => {
      const labels = Array.from(table.querySelectorAll('thead th'), th => th.textContent.trim());
      table.querySelectorAll('tbody tr').forEach(row => {
        Array.from(row.children).forEach((cell, index) => { cell.dataset.label = labels[index] || ''; });
      });
    });
  }

  function renderEndpoints() {
    if (!state.advanced) return renderListsSimple();
    if (state.loading && !state.loaded) return renderSkeleton(T("playlists.loadingEndpoints"));
    if (!state.endpoints.length) {
      return `<div class="pl-empty"><strong>${TH("playlists.noEndpoints")}</strong><span>${TH("playlists.noEndpointsHint")}</span></div>`;
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
          <td><div class="pl-muted">${refreshing ? T("playlists.refreshing") : ep.last_synced ? TH("playlists.refreshedAt", {time: timeText(ep.last_synced)}) : T("playlists.notRefreshed")}${ep.item_count != null ? `<br>${esc(ep.item_count)} items` : ""}</div></td>
          <td>
            <div class="pl-actions">
              ${actionButton("endpoint-sync", ep.id, refreshing ? T("playlists.refreshRunning") : T("playlists.refreshEndpoint"), "refresh", "refresh", refreshing ? "running" : "", refreshing)}
              ${actionButton("endpoint-edit", ep.id, T("playlists.editEndpoint"), "edit", "edit")}
              ${actionButton("endpoint-delete", ep.id, usedBy ? T("playlists.deleteEndpointUsed", {count: usedBy}) : T("playlists.deleteEndpoint"), "delete", "delete")}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap cw-page-table">
        <table class="pl-endpoints-table">
          <thead><tr><th>${TH("playlists.col.endpoint")}</th><th>${TH("playlists.col.provider")}</th><th>${TH("playlists.col.instance")}</th><th>${TH("playlists.col.selectedPlaylist")}</th><th>${TH("playlists.col.type")}</th><th>${TH("playlists.col.status")}</th><th>${TH("playlists.col.lastRefresh")}</th><th aria-label="${TH("playlists.col.actions")}">${TH("playlists.col.actions")}</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function publishedRow(key) {
    return ((state.published && state.published.lists) || []).find((row) => `${row.instance}|${row.list_id}` === key) || null;
  }

  function renderPublished() {
    if (state.loading && !state.loaded) return renderSkeleton(T("playlists.loadingPublished"));
    const lists = (state.published && state.published.lists) || [];
    if (!lists.length) {
      return `<div class="pl-empty"><strong>${TH("playlists.published.none")}</strong><span>${TH("playlists.published.noneHint")}</span></div>`;
    }
    const rows = lists.map((row) => {
      const key = `${row.instance}|${row.list_id}`;
      const feed = row.feed;
      const tool = { "kometa-movies.json": T("playlists.published.kometaMovies"), "kometa-shows.json": T("playlists.published.kometaShows"), "radarr.json": "Radarr", "sonarr.json": "Sonarr", "mdblist.json": "MDBList JSON" }[feed && feed.last_fetch_format] || "";
      const read = feed && feed.last_fetch_at ? `${timeText(feed.last_fetch_at)}<br>${esc(tool ? `by ${tool}` : "")}` : feed ? T("playlists.published.notRead") : "-";
      const used = Number(row.endpoint_count || 0);
      const deleteLabel = !row.can_delete ? T("playlists.published.cannotDelete") : used ? T("playlists.deleteListUsed", {count: used}) : T("playlists.deleteList");
      return `
        <tr>
          <td><div class="pl-provider">${icon("CROSSWATCH")}<div><div class="pl-main-text">${esc(row.name || row.list_id)}</div><div class="pl-muted">${[row.item_count != null ? `${row.item_count} items` : "", row.instance && row.instance !== "default" ? row.instance : ""].filter(Boolean).map(esc).join(" · ")}</div></div></div></td>
          <td>${feed ? `<span class="pl-pill ok">${TH("playlists.published.isPublished")}</span>` : `<span class="pl-pill off">${TH("playlists.published.notPublished")}</span>`}</td>
          <td><div class="pl-muted">${read}</div></td>
          <td>
            <div class="pl-actions">
              ${feed
                ? `<button class="pl-btn small" data-action="publish-show" data-id="${esc(key)}"><span class="material-symbols-rounded" aria-hidden="true">link</span>${TH("playlists.published.addresses")}</button>${actionButton("publish-key", key, T("playlists.ui.newKey"), "key", "refresh")}${actionButton("publish-off", key, T("playlists.ui.stopPublishing"), "link_off", "delete")}`
                : `<button class="pl-btn small" data-action="publish-on" data-id="${esc(key)}"><span class="material-symbols-rounded" aria-hidden="true">publish</span>${TH("playlists.published.publishBtn")}</button>`}
              ${actionButton("publish-delete", key, deleteLabel, "delete", "delete", "", !row.can_delete || used > 0)}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap cw-page-table">
        <table class="pl-published-table">
          <thead><tr><th>${TH("playlists.published.cwList")}</th><th>${TH("playlists.col.status")}</th><th>${TH("playlists.col.lastRead")}</th><th aria-label="${TH("playlists.col.actions")}">${TH("playlists.col.actions")}</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  async function reloadPublished() {
    try { state.published = await API.published(); } catch { state.published = { lists: [], formats: [] }; }
    const root = $("#page-playlists");
    if (root) refreshSection(root, "published");
    refreshHero(root);
  }

  function openPublishedModal(row, trigger) {
    if (!row || !row.feed) return;
    const urls = row.feed.urls || {};
    const name = row.name || T("playlists.published.cwList");
    const kometa = (url) => `collections:\n  ${name}:\n    text_file: ${url}\n    sync_mode: sync`;
    const block = (title, hint, text, area) => `
      <div class="pl-publish-block">
        <div class="pl-main-text">${esc(title)}</div>
        <div class="pl-muted">${esc(hint)}</div>
        <div class="pl-publish-line">
          ${area ? `<textarea readonly rows="4" wrap="off" spellcheck="false">${esc(text)}</textarea>` : `<input readonly spellcheck="false" value="${esc(text)}">`}
          <button class="pl-btn small" type="button" data-copy="${esc(text)}">${TH("playlists.copy")}</button>
        </div>
      </div>`;
    openModal({
      title: T("playlists.publishName", {name}),
      description: T("playlists.published.keyWarning"),
      width: "760px",
      cancelText: T("playlists.close"),
      trigger,
      body: `
        ${block(T("playlists.published.kometaMovieLib"), T("playlists.published.kometaMovieHint"), kometa(urls["kometa-movies.json"] || ""), true)}
        ${block(T("playlists.published.kometaShowLib"), T("playlists.published.kometaShowHint"), kometa(urls["kometa-shows.json"] || ""), true)}
        ${block(T("playlists.published.radarr"), T("playlists.published.radarrHint"), urls["radarr.json"] || "", false)}
        ${block(T("playlists.published.sonarr"), T("playlists.published.sonarrHint"), urls["sonarr.json"] || "", false)}
        ${block(T("playlists.published.mdblist"), T("playlists.published.mdblistHint"), urls["mdblist.json"] || "", false)}
        ${block("Kodi TMDBHelper", T("playlists.published.kodiHint"), urls["mdblist.json"] ? `plugin://plugin.video.themoviedb.helper/?info=mdblist_locallist&&${urls["mdblist.json"]}` : "", false)}
      `,
      onOpen: (ctx) => {
        ctx.modal.addEventListener("click", async (e) => {
          const btn = e.target.closest("[data-copy]");
          if (!btn) return;
          const text = btn.dataset.copy || "";
          if (!(await window.cwCopyText?.(text))) {
            window.cwSelectCopyField?.(btn.parentElement.querySelector("input,textarea"));
            return;
          }
          btn.textContent = T("playlists.copied");
          setTimeout(() => { btn.textContent = T("playlists.copy"); }, 1500);
        });
      },
    });
  }

  async function publishedAction(action, key, btn) {
    const row = publishedRow(key);
    if (!row) return;
    if (action === "publish-show") return openPublishedModal(row, btn);
    if (action === "publish-key" && !(await (window.CW?.MobileUI?.ask || window.confirm)(T("playlists.published.newKeyConfirm")))) return;
    if (action === "publish-off" && !(await (window.CW?.MobileUI?.ask || window.confirm)(T("playlists.published.stopConfirm")))) return;
    if (action === "publish-delete" && !(await (window.CW?.MobileUI?.ask || window.confirm)(T(row.feed ? "playlists.deletePublishedFeed" : "playlists.deletePublished", {name: row.name || T("playlists.thisList")})))) return;
    btn.disabled = true;
    try {
      if (action === "publish-on") await API.publish(row.instance, row.list_id);
      if (action === "publish-key") await API.publishKey(row.feed.id);
      if (action === "publish-off") await API.unpublish(row.feed.id);
      if (action === "publish-delete") await API.resourceDelete("CROSSWATCH", row.instance, row.list_id);
      await reloadPublished();
      if (action === "publish-on" || action === "publish-key") openPublishedModal(publishedRow(key), null);
    } catch (err) {
      window.cxToast?.(err && err.message ? err.message : T("playlists.published.updateFailed"));
      btn.disabled = false;
    }
  }

  function renderMappings() {
    if (!state.advanced) return renderSyncsSimple();
    if (state.loading && !state.loaded) return renderSkeleton(T("playlists.loadingMappings"));
    if (!state.mappings.length) {
      const need = state.endpoints.length < 2;
      return `<div class="pl-empty"><strong>${TH("playlists.noMappings")}</strong><span>${TH(need ? "playlists.ui.needTwo" : "playlists.ui.createMapping")}</span></div>`;
    }
    const rows = state.mappings.map((m) => {
      const src = endpointById(m.source_endpoint) || m.source || {};
      const rule = m.ruleset || rulesetById(m.ruleset_id || "");
      const res = m.last_result || null;
      const busy = sharedSyncBusy();
      const running = mappingIsRunning(m);
      const syncTitle = running ? T("playlists.syncRunning") : (busy ? T("playlists.alreadyRunning") : T("playlists.reviewSync"));
      return `
        <tr class="${running ? "is-running" : ""}">
          <td>${endpointIdentity({ name: m.name || m.id, id: m.id, provider: (src && src.provider) || "" }, m.id)}</td>
          <td>${endpointRef(src, m.source_endpoint)}</td>
          <td><span class="pl-pill dir">${esc(directionFor(m))}<span class="material-symbols-rounded" aria-hidden="true">east</span></span></td>
          <td>${mappingTargetRefs(m)}</td>
          <td><span class="pl-pill">${esc(rule ? rule.name : T("playlists.enums.direct"))}</span></td>
          <td>${syncPairIndicator(m.assigned_pair)}</td>
          <td>${m.enabled ? statusPill("ok", T("playlists.enabled")) : statusPill("off", T("playlists.disabled"), "radio_button_unchecked")}</td>
          <td><div class="pl-result-cell">${running ? statusPill("run", T("playlists.running"), "sync") : statusForResult(res)}<div class="pl-muted">${running ? T("playlists.runningNow") : compactTime(res && res.finished_at)}</div></div></td>
          <td>
            <div class="pl-actions">
              ${actionButton("mapping-sync", m.id, syncTitle, "sync", "sync", running ? "running" : "", busy)}
              ${actionButton("mapping-edit", m.id, T("playlists.editMapping"), "edit", "edit")}
              ${actionButton("mapping-toggle", m.id, m.enabled ? T("playlists.disableMapping") : T("playlists.enableMapping"), m.enabled ? "pause" : "play_arrow", "toggle", m.enabled ? "on" : "")}
              ${actionButton("mapping-delete", m.id, T("playlists.deleteMapping"), "delete", "delete")}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap cw-page-table">
        <table class="pl-mappings-table">
          <thead><tr><th>${TH("playlists.col.mapping")}</th><th>${TH("playlists.col.source")}</th><th>${TH("playlists.col.direction")}</th><th>${TH("playlists.col.destination")}</th><th>${TH("playlists.col.ruleset")}</th><th>${TH("playlists.col.syncPair")}</th><th>${TH("playlists.col.status")}</th><th>${TH("playlists.col.result")}</th><th aria-label="${TH("playlists.col.actions")}">${TH("playlists.col.actions")}</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderActivity() {
    if (state.loading && !state.loaded) return renderSkeleton(T("playlists.loadingActivity"));
    const entries = state.activity || [];
    const stats = activityStats(entries);
    const latest = entries.filter((row) => String(row.type || "").toLowerCase() === "run").slice(0, 8);
    const table = latest.length ? `
      <div class="pl-table-wrap cw-page-table">
        <table class="pl-activity-table">
          <thead><tr><th>${TH("playlists.col.time")}</th><th>${TH("playlists.col.mapping")}</th><th>${TH("playlists.col.result")}</th><th>${TH("playlists.col.changes")}</th><th>${TH("playlists.col.status")}</th></tr></thead>
          <tbody>${latest.map((row) => {
            return `<tr><td>${esc(compactTime(row.ts))}</td><td><div class="pl-main-text">${esc(row.label || "-")}</div></td><td>${activityResultSummary(row)}</td><td>${activityChangeSummary(row)}</td><td>${activityStatus(row)}</td></tr>`;
          }).join("")}</tbody>
        </table>
      </div>
    ` : `<div class="pl-empty"><strong>${TH("playlists.activity.noRuns")}</strong><span>${TH("playlists.activity.noRunsHint")}</span></div>`;
    return `
      <div class="pl-activity">
        <div class="pl-stats">
          ${statCard(stats.total, T("playlists.stats.total"), "trending_up", "total")}
          ${statCard(stats.success, T("playlists.stats.successful"), "check_circle", "success")}
          ${statCard(stats.warning, T("playlists.stats.warnings"), "warning", "warning")}
          ${statCard(stats.failed, T("playlists.status.failed"), "cancel", "failed")}
          ${statCard(stats.running, T("playlists.stats.queued"), "speed", "running")}
          ${statCard(stats.skipped, T("playlists.changes.skipped"), "sync_disabled", "skipped")}
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
    if (action === "sync-wizard") openSyncWizard({ trigger: btn });
    if (action === "sync-wizard-publish") openSyncWizard({ trigger: btn, publish: true });
    if (action === "mapping-edit") {
      const mapping = state.mappings.find((m) => m.id === id);
      if (state.advanced || (mapping && mapping.ruleset_id)) openMappingModal({ mapping, trigger: btn });
      else openSyncEdit(mapping, btn);
    }
    if (action === "mapping-toggle") toggleMapping(state.mappings.find((m) => m.id === id), btn);
    if (action === "mapping-delete") openMappingDelete(state.mappings.find((m) => m.id === id), btn);
    if (action === "mapping-sync") syncMapping(state.mappings.find((m) => m.id === id), btn);
    if (action === "activity-all") openActivityModal(btn);
    if (action === "activity-clear") openActivityClear(btn);
    if (action === "open-connections") openConnections();
    if (action === "hero-refresh") heroRefresh(btn);
    if (action === "toggle-advanced") {
      state.advanced = !state.advanced;
      try { localStorage.setItem("cw.playlists.advanced", state.advanced ? "1" : "0"); } catch {}
      render($("#page-playlists"));
    }
    if (action.startsWith("publish-")) publishedAction(action, id, btn);
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
          <button class="pl-btn icon" data-modal-close aria-label="${TH("playlists.close")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
        </div>
        <div class="pl-dialog-body">
          <div class="pl-dialog-error" data-modal-error></div>
          ${opts.body || ""}
        </div>
        <div class="pl-dialog-foot">
          <button class="pl-btn" data-modal-cancel>${esc(opts.cancelText || T("playlists.cancel"))}</button>
          ${opts.primaryText ? `<button class="pl-btn primary" data-modal-primary>${esc(opts.primaryText)}</button>` : ""}
        </div>
      </div>
    `;
    host.appendChild(modal);
    document.body.classList.add("cx-modal-open");
    const ctx = { modal, opts, dirty: false, saving: false, initial: "" };
    state.modal = ctx;
    window.CW?.DialogFocus?.activate(modal.querySelector('.pl-dialog'), () => closeModal(false));
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
        primary.textContent = opts.savingText || T("playlists.saving");
        try {
          await opts.onPrimary(ctx);
        } catch (err) {
          setModalError(err && err.message ? err.message : String(err || T("playlists.saveFailed")));
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
    if (e.key === "Escape" && !e.defaultPrevented && !document.querySelector('dialog[open]')) closeModal(false);
  }

  async function closeModal(force) {
    const ctx = state.modal;
    if (!ctx) return true;
    if (!force && (ctx.saving || ctx.confirming)) return false;
    if (!force && ctx.dirty) {
      ctx.confirming = true;
      const discard = await (window.CW?.MobileUI?.ask || window.confirm)(T("playlists.discardChanges"));
      ctx.confirming = false;
      if (!discard || state.modal !== ctx) return false;
    }
    const dismissed = window.CW?.DialogFocus?.deactivate(ctx.modal.querySelector('.pl-dialog'));
    document.removeEventListener("keydown", onModalKey);
    ctx.modal.remove();
    document.body.classList.remove("cx-modal-open");
    state.modal = null;
    await dismissed;
    if (!state.modal && ctx.opts && ctx.opts.trigger && typeof ctx.opts.trigger.focus === "function") ctx.opts.trigger.focus();
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
    return safeNameError(name, T("playlists.newPlaylistName"), PLAYLIST_NAME_MAX);
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
    const raw = String(value || fallback || T("playlists.col.endpoint")).replace(/\s*\([^)]*\)\s*$/g, "").trim() || T("playlists.col.endpoint");
    let clean = Array.from(raw).filter(isSafeNameChar).join("").replace(/\s+/g, " ").trim();
    if (!clean) clean = T("playlists.col.endpoint");
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

  function fillEditPrivacy(root, resource) {
    const wrap = $("#pl-ep-edit-privacy-wrap", root);
    const select = $("#pl-ep-edit-privacy", root);
    const extra = resource.extra || {};
    const opts = resource.can_edit_privacy ? createPrivacyOptions(val("#pl-ep-provider", root), resource.endpoint_type || extra.endpoint_type || "") : null;
    const current = opts && opts.values.includes(String(resource.privacy || "")) ? String(resource.privacy) : "";
    root.dataset.epEditPrivacy = current;
    if (wrap) wrap.classList.toggle("hidden", !opts);
    if (!select) return;
    if (!opts) {
      select.innerHTML = "";
      return;
    }
    select.innerHTML = selectOptions(opts.values.map((v) => ({ value: v, label: PRIVACY_LABELS[v] || titleize(v) })), current, current ? "" : T("playlists.keepCurrent"));
    window.CW?.IconSelect?.enhance?.(select, { className: "cw-plain-select" });
  }

  function editNameChanged(root) {
    if (!root.dataset.epEditCanRename) return false;
    return String(val("#pl-ep-edit-name", root) || "").trim() !== String(root.dataset.epEditName || "").trim();
  }

  function editPanelChanges(root) {
    const changes = {};
    if (root.dataset.epEditCanRename) {
      const name = String(val("#pl-ep-edit-name", root) || "").trim();
      if (name && name !== String(root.dataset.epEditName || "").trim()) changes.name = name;
    }
    const wrap = $("#pl-ep-edit-privacy-wrap", root);
    const privacy = wrap && !wrap.classList.contains("hidden") ? String(val("#pl-ep-edit-privacy", root) || "") : "";
    if (privacy && privacy !== String(root.dataset.epEditPrivacy || "")) changes.privacy = privacy;
    return changes;
  }

  function setEndpointManageMode(root, mode, resources = []) {
    const next = String(mode || "");
    root.dataset.epManageMode = next;
    const editPanel = $("#pl-ep-edit-panel", root);
    const deletePanel = $("#pl-ep-delete-panel", root);
    const linkPanel = $("#pl-ep-link-panel", root);
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
    if (linkPanel) linkPanel.classList.toggle("hidden", next !== "link");
    if (next !== "link") applyFieldError($("#pl-ep-link-ref", root), "", "");
    if (next === "edit" && resources[0]) {
      const resource = resources[0];
      root.dataset.epEditResourceId = String(resource.id || "");
      root.dataset.epEditName = String(resource.name || resource.id || "");
      root.dataset.epEditCanRename = resource.can_rename ? "1" : "";
      if (editName) {
        editName.value = resource.name || resource.id || "";
        applyFieldError(editName, "", `${String(editName.value || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
      }
      $("#pl-ep-edit-name-wrap", root)?.classList.toggle("hidden", !resource.can_rename);
      fillEditPrivacy(root, resource);
    } else {
      root.dataset.epEditResourceId = "";
      root.dataset.epEditName = "";
      root.dataset.epEditCanRename = "";
      root.dataset.epEditPrivacy = "";
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
      playlist: ["queue_music", T("playlists.listType.playlist")],
      regular: ["list_alt", T("playlists.res.regular")],
      watchlist: ["bookmark", T("playlists.listType.watchlist")],
      discovery: ["travel_explore", T("playlists.res.discoverySource")],
      liked_list: ["favorite", T("playlists.listType.liked")],
      followed_list: ["bookmark_added", T("playlists.listType.followed")],
      public_list: ["public", T("playlists.listType.public")],
      collaboration: ["group", T("playlists.listType.shared")],
      collection: ["collections_bookmark", T("playlists.listType.collection")],
      cloud_list: ["cloud", T("playlists.cloudList")],
      movie: ["movie", T("playlists.type.movies")],
      movies: ["movie", T("playlists.type.movies")],
      show: ["tv", T("playlists.type.shows")],
      shows: ["tv", T("playlists.type.shows")],
      season: ["video_library", T("playlists.res.seasons")],
      seasons: ["video_library", T("playlists.res.seasons")],
      episode: ["live_tv", T("playlists.res.episodes")],
      episodes: ["live_tv", T("playlists.res.episodes")],
      "read-only": ["lock", T("playlists.res.readOnly")],
      smart: ["auto_awesome", T("playlists.res.smart")],
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
      list.innerHTML = `<div class="pl-resource-empty">${TH("playlists.noPlaylistsFound")}</div>`;
      return;
    }
    if (!visible.length) {
      list.innerHTML = `<div class="pl-resource-empty">${TH("playlists.noPlaylistsMatch")}</div>`;
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
            <span class="pl-resource-name">${esc(r.name || r.id || "-")}${r.extra && r.extra.item_count != null ? ` <span class="pl-muted">· ${esc(r.extra.item_count)} items</span>` : ""}</span>
          </span>
          <span class="pl-resource-badges" aria-label="${TH("playlists.playlistProperties")}">
            ${badges.map((badge) => `<span class="pl-resource-badge material-symbols-rounded" title="${esc(badge.title)}" aria-label="${esc(badge.title)}" data-kind="${esc(badge.key)}">${esc(badge.icon)}</span>`).join("")}
          </span>
        </button>
      `;
    }).join("");
  }

  function updateEndpointPlaylistActions(root) {
    const provider = val("#pl-ep-provider", root);
    const canCreate = creatableEndpointTypes(provider).length > 0;
    const createBtn = $("#pl-ep-list-create", root);
    const editBtn = $("#pl-ep-list-edit", root);
    const deleteBtn = $("#pl-ep-list-delete", root);
    const linkBtn = $("#pl-ep-list-link", root);
    const managing = !!endpointManageMode(root);
    const creating = endpointCreateMode(root);
    if (linkBtn) {
      const linking = endpointManageMode(root) === "link";
      linkBtn.classList.toggle("hidden", !providerCanResolve(provider));
      const linkInput = $("#pl-ep-link-ref", root);
      if (linkInput) linkInput.placeholder = providerLinkExample(provider);
      linkBtn.disabled = creating || (managing && !linking);
      linkBtn.classList.toggle("active", linking);
      linkBtn.title = linkBtn.disabled ? T("playlists.manage.finishFirst") : T("playlists.manage.addFromLink");
    }
    const selected = selectedEndpointResources(root);
    const editOk = !creating && !managing && selected.length === 1 && !!(selected[0].can_rename || selected[0].can_edit_privacy);
    const deleteOk = !creating && !managing && selected.length > 0 && selected.every((r) => !!r.can_delete);
    if (createBtn) {
      createBtn.disabled = !canCreate || managing;
      createBtn.classList.toggle("active", creating);
      createBtn.title = managing ? T("playlists.manage.finishFirst") : (canCreate ? T("playlists.manage.create") : T("playlists.manage.createUnsupported"));
    }
    if (editBtn) {
      editBtn.disabled = !editOk;
      editBtn.title = creating || managing ? T("playlists.manage.finishFirst")
        : (!selected.length ? T("playlists.manage.selectOneRename")
          : (selected.length > 1 ? T("playlists.manage.selectExactlyOne") : (selected[0].can_rename ? T("playlists.manage.rename") : (selected[0].can_edit_privacy ? T("playlists.manage.changePrivacy") : T("playlists.manage.cannotRename")))));
    }
    if (deleteBtn) {
      deleteBtn.disabled = !deleteOk;
      deleteBtn.title = creating || managing ? T("playlists.manage.finishFirst")
        : (!selected.length ? T("playlists.manage.selectDelete")
          : (deleteOk ? T("playlists.manage.deleteSelected") : T("playlists.manage.cannotDelete")));
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
    submit.title = err || T("playlists.manage.createHint");
  }

  function syncEndpointManagePanel(root) {
    const mode = endpointManageMode(root);
    const editName = $("#pl-ep-edit-name", root);
    const editSubmit = $("#pl-ep-edit-submit", root);
    const deleteSubmit = $("#pl-ep-delete-submit", root);
    if (editSubmit && editName) {
      const editValue = val("#pl-ep-edit-name", root);
      const err = mode === "edit" && editNameChanged(root) ? playlistNameError(editValue) : "";
      applyFieldError(editName, err, `${String(editValue || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
      editSubmit.disabled = !!err;
      editSubmit.title = err || T("playlists.manage.saveHint");
    }
    if (deleteSubmit) {
      const ids = root.__plDeleteResourceIds || [];
      deleteSubmit.disabled = mode !== "delete" || !ids.length;
      deleteSubmit.title = ids.length ? T("playlists.manage.deleteSelected") : T("playlists.manage.noneSelected");
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
    const privacy = privacyPayload($("#pl-ep-create-privacy", root), $("#pl-ep-create-privacy-wrap", root)?.classList.contains("hidden"));
    if (submit) {
      submit.disabled = true;
      submit.dataset.originalText = submit.innerHTML;
      submit.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">sync</span>Creating...`;
    }
    try {
      const res = await API.resourceCreate({ provider, instance, name: createName, media_type: mediaType, ...privacy });
      const resource = res.resource || {};
      const search = $("#pl-ep-playlist-search", root);
      if (search) search.value = "";
      setEndpointCreateMode(root, false);
      await loadEndpointResources(root, resource.id || "", isEdit);
    } catch (error) {
      setModalError(error && error.message ? error.message : T("playlists.manage.createFailed"));
    } finally {
      if (submit) {
        submit.innerHTML = submit.dataset.originalText || `<span class="material-symbols-rounded" aria-hidden="true">add</span>${TH("playlists.ui.createPlaylist")}`;
        delete submit.dataset.originalText;
      }
      syncEndpointCreatePanel(root);
      syncModalPrimary(ctx);
    }
  }

  async function addEndpointResourceFromLink(root, ctx, isEdit) {
    const input = $("#pl-ep-link-ref", root);
    const submit = $("#pl-ep-link-submit", root);
    const ref = String(val("#pl-ep-link-ref", root) || "").trim();
    if (!ref) {
      applyFieldError(input, T("playlists.pasteLinkError"), "");
      return;
    }
    const provider = val("#pl-ep-provider", root);
    const instance = val("#pl-ep-instance", root) || "default";
    if (submit) submit.disabled = true;
    try {
      const res = await API.resourceResolve(provider, instance, ref);
      const resource = res.resource || {};
      const linked = (root.__plLinkedResources ||= {});
      const key = `${provider}|${instance}`;
      linked[key] = [...(linked[key] || []).filter((r) => String(r.id) !== String(resource.id)), resource];
      if (val("#pl-ep-provider", root) !== provider || (val("#pl-ep-instance", root) || "default") !== instance) return;
      if (input) input.value = "";
      const search = $("#pl-ep-playlist-search", root);
      if (search) search.value = "";
      setEndpointManageMode(root, "");
      await loadEndpointResources(root, resource.id || "", isEdit);
    } catch (error) {
      applyFieldError(input, error && error.message ? error.message : T("playlists.listNotFound"), "");
    } finally {
      if (submit) submit.disabled = false;
      syncModalPrimary(ctx);
    }
  }

  function openEndpointResourceEdit(root, ctx) {
    const selected = selectedEndpointResources(root);
    if (selected.length !== 1 || !(selected[0].can_rename || selected[0].can_edit_privacy)) return;
    setEndpointManageMode(root, "edit", selected);
    $(selected[0].can_rename ? "#pl-ep-edit-name" : "#pl-ep-edit-privacy", root)?.focus();
    syncModalPrimary(ctx);
  }

  async function renameProviderPlaylistFromPanel(root, ctx, isEdit) {
    const submit = $("#pl-ep-edit-submit", root);
    const nameInput = $("#pl-ep-edit-name", root);
    const renameName = val("#pl-ep-edit-name", root);
    const err = editNameChanged(root) ? playlistNameError(renameName) : "";
    applyFieldError(nameInput, err, `${String(renameName || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
    if (err) {
      syncModalPrimary(ctx);
      return;
    }
    const playlistId = root.dataset.epEditResourceId || "";
    const provider = val("#pl-ep-provider", root);
    const instance = val("#pl-ep-instance", root) || "default";
    if (!playlistId) return;
    const changes = editPanelChanges(root);
    if (!Object.keys(changes).length) {
      setEndpointManageMode(root, "");
      syncEndpointManagePanel(root);
      syncModalPrimary(ctx);
      return;
    }
    if (submit) {
      submit.disabled = true;
      submit.dataset.originalText = submit.innerHTML;
      submit.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">sync</span>Saving...`;
    }
    try {
      const res = await API.resourceUpdate(provider, instance, playlistId, changes);
      const resource = res.resource || {};
      setEndpointManageMode(root, "");
      await loadEndpointResources(root, resource.id || playlistId, isEdit);
    } catch (error) {
      setModalError(error && error.message ? error.message : T("playlists.manage.saveFailed"));
      if (changes.name && changes.privacy) {
        try {
          await loadEndpointResources(root, playlistId, isEdit);
          const fresh = (root.__plEndpointResources || []).find((r) => String(r.id || "") === playlistId);
          if (fresh) root.dataset.epEditName = String(fresh.name || fresh.id || "");
        } catch {}
      }
    } finally {
      if (submit) {
        submit.innerHTML = submit.dataset.originalText || `<span class="material-symbols-rounded" aria-hidden="true">check</span>${TH("playlists.ui.saveChanges")}`;
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
      setModalError(error && error.message ? error.message : T("playlists.manage.deleteFailed"));
    } finally {
      if (submit) {
        submit.innerHTML = submit.dataset.originalText || `<span class="material-symbols-rounded" aria-hidden="true">delete</span>${TH("playlists.ui.delete")}`;
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
    const typeBadge = endpointIsDiscovery(ep) ? T("playlists.listType.discovery") : isWatchlist ? T("playlists.listType.watchlist") : (!["", "regular", "playlist"].includes(rawType) ? titleize(rawType) : "");
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
    return ep.name || ep.playlist_name || ep.playlist_id || id || T("playlists.col.endpoint");
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
      if (select) window.CW?.IconSelect?.enhance?.(select, { className: "pl-endpoint-select", menuClassName: "pl-endpoint-select-menu", sortAlphabetically: true, getOptionData: endpointOptionData });
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
    const simple = !state.advanced;
    const title = simple ? (isEdit ? T("playlists.changeList") : T("playlists.ep.addList")) : isEdit ? T("playlists.ep.editTitle") : T("playlists.ep.createTitle");
    const description = simple
      ? (isEdit ? T("playlists.ep.pickOne") : T("playlists.ep.pickMany"))
      : isEdit ? T("playlists.ep.updateHint") : T("playlists.ep.connectHint");
    const endpointName = isEdit ? (seed.name || seed.id || "") : nextEndpointName();
    const body = `
      <div class="pl-endpoint-wizard">
        <section class="pl-endpoint-step">
          <div class="pl-step-head"><span class="pl-step-index">1</span><div>${simple ? "<b>" + TH("playlists.col.provider") + "</b><span>" + TH("playlists.ep.whereLives") + "</span>" : "<b>" + TH("playlists.ep.details") + "</b><span>" + TH("playlists.ep.nameHint") + "</span>"}</div></div>
          <div class="pl-form">
            <div class="pl-field full" ${simple ? "hidden" : ""}>
              <label for="pl-ep-name">${TH("playlists.ep.name")} <span aria-hidden="true">*</span></label>
              <input id="pl-ep-name" maxlength="${NAME_MAX}" value="${esc(endpointName)}" placeholder="${TH("playlists.ep.namePlaceholder")}" aria-describedby="pl-ep-name-error">
              <div class="pl-field-error" id="pl-ep-name-error"></div>
            </div>
            <div class="pl-field full">
              <label for="pl-ep-provider">${TH("playlists.col.provider")} <span aria-hidden="true">*</span></label>
              <select id="pl-ep-provider">${selectOptions(providers, provider, providers.length ? "" : T("playlists.ep.noProviders"))}</select>
            </div>
          </div>
        </section>
        <section class="pl-endpoint-step" id="pl-ep-instance-step">
          <div class="pl-step-head"><span class="pl-step-index">2</span><div>${simple ? "<b>" + TH("playlists.col.instance") + "</b><span>" + TH("playlists.ep.owner") + "</span>" : "<b>" + TH("playlists.ep.providerInstance") + "</b><span>" + TH("playlists.ep.instanceHint") + "</span>"}</div></div>
          <div class="pl-form">
            <div class="pl-field full">
              <label for="pl-ep-instance">${TH("playlists.ep.providerInstance")} <span aria-hidden="true">*</span></label>
              <select id="pl-ep-instance">${selectOptions(instances.map((x) => ({ value: x, label: x })), instance)}</select>
            </div>
          </div>
        </section>
        <section class="pl-endpoint-step" id="pl-ep-resource-wrap">
          <div class="pl-step-head with-actions">
            <span class="pl-step-index">3</span>
            <div>${simple ? `<b>${TH("playlists.col.list")}</b><span>${TH(isEdit ? "playlists.ui.chooseList" : "playlists.ui.chooseLists")}</span>` : `<b>${TH("playlists.ep.selectPlaylists")}</b><span>${TH(isEdit ? "playlists.ui.choosePlaylist" : "playlists.ui.choosePlaylists")}</span>`}</div>
            <div class="pl-playlist-tools" aria-label="${TH("playlists.ep.actions")}">
              <button type="button" class="pl-icon-tool hidden" id="pl-ep-list-link" title="${TH("playlists.manage.addFromLink")}" aria-label="${TH("playlists.manage.addFromLink")}"><span class="material-symbols-rounded" aria-hidden="true">link</span></button>
              <button type="button" class="pl-icon-tool" id="pl-ep-list-create" title="${TH("playlists.manage.create")}" aria-label="${TH("playlists.manage.create")}"><span class="material-symbols-rounded" aria-hidden="true">add</span></button>
              <button type="button" class="pl-icon-tool" id="pl-ep-list-edit" title="${TH("playlists.ep.edit")}" aria-label="${TH("playlists.ep.edit")}"><span class="material-symbols-rounded" aria-hidden="true">edit</span></button>
              <button type="button" class="pl-icon-tool danger" id="pl-ep-list-delete" title="${TH("playlists.ep.delete")}" aria-label="${TH("playlists.ep.delete")}"><span class="material-symbols-rounded" aria-hidden="true">delete</span></button>
            </div>
          </div>
          <input type="checkbox" id="pl-ep-create" hidden>
          <div class="pl-warning hidden" id="pl-ep-simkl-warning">${esc(SIMKL_PLAYLIST_WARNING)}</div>
          <div class="pl-create-panel hidden" id="pl-ep-create-panel">
            <div class="pl-field">
              <label for="pl-ep-create-name">${TH("playlists.newPlaylistName")}</label>
              <input id="pl-ep-create-name" maxlength="${PLAYLIST_NAME_MAX}" value="${esc(seed.playlist_name || seed.name || "")}" placeholder="${TH("playlists.newPlaylistName")}" aria-describedby="pl-ep-create-name-error">
              <div class="pl-field-error" id="pl-ep-create-name-error"></div>
            </div>
            <div class="pl-field" id="pl-ep-create-type-wrap">
              <label for="pl-ep-create-type">${TH("playlists.ep.newListType")}</label>
              <select id="pl-ep-create-type"></select>
              <div class="pl-help" id="pl-ep-create-type-help">${TH("playlists.ep.collectionsHint")}</div>
            </div>
            <div class="pl-field hidden" id="pl-ep-create-privacy-wrap">
              <label for="pl-ep-create-privacy">${TH("playlists.ep.privacy")}</label>
              <select id="pl-ep-create-privacy"></select>
            </div>
            <div class="pl-create-actions">
              <button type="button" class="pl-btn small" id="pl-ep-create-cancel">${TH("playlists.cancel")}</button>
              <button type="button" class="pl-btn small primary" id="pl-ep-create-submit"><span class="material-symbols-rounded" aria-hidden="true">add</span>${TH("playlists.ep.createPlaylist")}</button>
            </div>
          </div>
          <div class="pl-create-panel pl-resource-manage-panel hidden" id="pl-ep-edit-panel">
            <div class="pl-field full" id="pl-ep-edit-name-wrap">
              <label for="pl-ep-edit-name">${TH("playlists.ep.playlistName")}</label>
              <input id="pl-ep-edit-name" maxlength="${PLAYLIST_NAME_MAX}" value="" placeholder="${TH("playlists.ep.playlistName")}" aria-describedby="pl-ep-edit-name-error">
              <div class="pl-field-error" id="pl-ep-edit-name-error"></div>
            </div>
            <div class="pl-field full hidden" id="pl-ep-edit-privacy-wrap">
              <label for="pl-ep-edit-privacy">${TH("playlists.ep.privacy")}</label>
              <select id="pl-ep-edit-privacy"></select>
            </div>
            <div class="pl-create-actions">
              <button type="button" class="pl-btn small" id="pl-ep-edit-cancel">${TH("playlists.cancel")}</button>
              <button type="button" class="pl-btn small primary" id="pl-ep-edit-submit"><span class="material-symbols-rounded" aria-hidden="true">check</span>${TH("playlists.ep.saveChanges")}</button>
            </div>
          </div>
          <div class="pl-create-panel pl-resource-manage-panel hidden" id="pl-ep-link-panel">
            <div class="pl-field full">
              <label for="pl-ep-link-ref">${TH("playlists.ep.listLink")}</label>
              <input id="pl-ep-link-ref" placeholder="${TH("playlists.pasteLink")}" autocomplete="off" aria-describedby="pl-ep-link-ref-error">
              <div class="pl-field-error" id="pl-ep-link-ref-error"></div>
            </div>
            <div class="pl-create-actions">
              <button type="button" class="pl-btn small" id="pl-ep-link-cancel">${TH("playlists.cancel")}</button>
              <button type="button" class="pl-btn small primary" id="pl-ep-link-submit"><span class="material-symbols-rounded" aria-hidden="true">add_link</span>${TH("playlists.addList")}</button>
            </div>
          </div>
          <div class="pl-create-panel pl-resource-manage-panel danger hidden" id="pl-ep-delete-panel">
            <div class="pl-resource-delete-copy">
              <b>${TH("playlists.ep.deleteConfirm")}</b>
              <span>${TH("playlists.ep.deleteHint")}</span>
              <div class="pl-resource-delete-list" id="pl-ep-delete-list"></div>
            </div>
            <div class="pl-create-actions">
              <button type="button" class="pl-btn small" id="pl-ep-delete-cancel">${TH("playlists.cancel")}</button>
              <button type="button" class="pl-btn small danger" id="pl-ep-delete-submit"><span class="material-symbols-rounded" aria-hidden="true">delete</span>${TH("playlists.ui.delete")}</button>
            </div>
          </div>
          <div class="pl-playlist-search">
            <span class="material-symbols-rounded" aria-hidden="true">search</span>
            <input id="pl-ep-playlist-search" type="search" placeholder="${TH("playlists.ep.search")}" autocomplete="off">
          </div>
          <select id="pl-ep-playlist" class="pl-native-playlist-select" ${isEdit ? "" : "multiple"} hidden><option value="">${TH("playlists.loading")}</option></select>
          <div class="pl-resource-list" id="pl-ep-resource-list" role="listbox" aria-multiselectable="${isEdit ? "false" : "true"}"><div class="pl-resource-empty">${TH("playlists.ep.loadingPlaylists")}</div></div>
          <div class="pl-help" id="pl-ep-playlist-help">${isEdit ? T("playlists.ep.selectOne") : T("playlists.ep.selectMany")}</div>
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
      primaryText: simple ? (isEdit ? T("playlists.save") : T("playlists.addList")) : isEdit ? T("playlists.ep.save") : T("playlists.ep.create"),
      savingText: T("playlists.ep.saving"),
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
    bindNameValidation(ctx, "#pl-ep-name", T("playlists.ep.name"));
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
      const err = endpointCreateMode(root) || selectedValues("#pl-ep-playlist", root).length ? "" : T("playlists.ep.selectAtLeastOne");
      const list = $("#pl-ep-resource-list", root);
      const feedback = $("#pl-ep-playlist-error", root);
      if (list) list.classList.toggle("invalid", !!err);
      if (feedback) feedback.textContent = err;
      return err;
    });
    addModalValidator(ctx, () => endpointCreateMode(root) ? T("playlists.ep.createOrCancel") : "");
    addModalValidator(ctx, () => endpointManageMode(root) ? T("playlists.ep.saveOrCancel") : "");
    const updateInstances = () => {
      const list = instancesFor(providerSelect.value);
      instanceSelect.innerHTML = selectOptions(list.map((x) => ({ value: x, label: x })), list.includes(instanceSelect.value) ? instanceSelect.value : (list[0] || "default"));
      window.CW?.ProfileSelect?.enhanceProfile?.(instanceSelect);
      syncProfileStep(list);
    };
    const syncProfileStep = (list) => {
      const single = !state.advanced && list.length <= 1;
      const step = $("#pl-ep-instance-step", root);
      if (step) step.hidden = single;
      const index = $("#pl-ep-resource-wrap .pl-step-index", root);
      if (index) index.textContent = single ? "2" : "3";
    };
    const updateCreateTypes = () => {
      const wrap = $("#pl-ep-create-type-wrap", root);
      const select = $("#pl-ep-create-type", root);
      if (!wrap || !select) return;
      const types = creatableEndpointTypes(providerSelect.value);
      const current = select.value;
      select.innerHTML = selectOptions(types.map((t) => ({ value: t, label: createTypeLabel(t) })), types.includes(current) ? current : types[0]);
      wrap.classList.toggle("hidden", types.length <= 1);
      const help = $("#pl-ep-create-type-help", root);
      if (help) help.textContent = types.includes("cloud_list") ? T("playlists.ep.cloudHint") : (types.every((t) => CREATE_TYPE_LABELS[t]) ? T("playlists.ep.oneKind") : T("playlists.ep.collectionsHint"));
      window.CW?.IconSelect?.enhance?.(select, { className: "cw-plain-select" });
      updateCreatePrivacy();
    };
    const updateCreatePrivacy = () => {
      const wrap = $("#pl-ep-create-privacy-wrap", root);
      const opts = fillPrivacySelect($("#pl-ep-create-privacy", root), providerSelect.value, val("#pl-ep-create-type", root));
      if (wrap) wrap.classList.toggle("hidden", !opts);
    };
    const updateProviderRestrictions = () => {
      const isSimkl = String(providerSelect.value || "").toUpperCase() === "SIMKL";
      if (simklWarning) simklWarning.classList.toggle("hidden", !isSimkl);
      updateCreateTypes();
      updateEndpointPlaylistActions(root);
      syncModalPrimary(ctx);
    };
    $("#pl-ep-name", root)?.addEventListener("input", () => { root.dataset.epNameDirty = "1"; });
    window.CW?.ProfileSelect?.enhanceProvider?.(providerSelect);
    window.CW?.ProfileSelect?.enhanceProfile?.(instanceSelect);
    syncProfileStep(instancesFor(providerSelect.value));
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
    $("#pl-ep-create-type", root)?.addEventListener("change", updateCreatePrivacy);
    $("#pl-ep-edit-cancel", root)?.addEventListener("click", () => setEndpointManageMode(root, ""));
    $("#pl-ep-edit-submit", root)?.addEventListener("click", () => renameProviderPlaylistFromPanel(root, ctx, isEdit));
    $("#pl-ep-list-link", root)?.addEventListener("click", () => {
      if ($("#pl-ep-list-link", root).disabled) return;
      setEndpointManageMode(root, endpointManageMode(root) === "link" ? "" : "link");
      if (endpointManageMode(root) === "link") $("#pl-ep-link-ref", root)?.focus();
    });
    $("#pl-ep-link-cancel", root)?.addEventListener("click", () => setEndpointManageMode(root, ""));
    $("#pl-ep-link-submit", root)?.addEventListener("click", () => addEndpointResourceFromLink(root, ctx, isEdit));
    $("#pl-ep-link-ref", root)?.addEventListener("keydown", (ev) => {
      if (ev.key !== "Enter") return;
      ev.preventDefault();
      addEndpointResourceFromLink(root, ctx, isEdit);
    });
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
    select.innerHTML = `<option value="">${TH("playlists.loading")}</option>`;
    root.__plEndpointResources = [];
    renderEndpointResourceList(root);
    const provider = val("#pl-ep-provider", root);
    const instance = val("#pl-ep-instance", root) || "default";
    const seq = (root.__plEndpointLoadSeq = (root.__plEndpointLoadSeq || 0) + 1);
    const stale = () => seq !== root.__plEndpointLoadSeq || val("#pl-ep-provider", root) !== provider || (val("#pl-ep-instance", root) || "default") !== instance;
    try {
      const data = await API.resources(provider, instance);
      const resources = [...(data.resources || [])];
      const has = (id) => resources.some((r) => String(r.id) === String(id));
      ((root.__plLinkedResources || {})[`${provider}|${instance}`] || []).forEach((r) => { if (!has(r.id)) resources.push(r); });
      let resolveError = "";
      if (selected && !has(selected) && providerCanResolve(provider)) {
        try {
          const found = await API.resourceResolve(provider, instance, selected);
          if (found.resource && !has(found.resource.id)) resources.push(found.resource);
        } catch (err) {
          resolveError = err && err.message ? err.message : T("playlists.ep.savedNotFound");
        }
      }
      if (stale()) return;
      root.__plEndpointResources = resources;
      if (!resources.length) {
        select.innerHTML = `<option value="">${TH("playlists.ep.noneFound")}</option>`;
        help.textContent = T("playlists.ep.noneReadable");
        refreshEndpointResourceSelection(root, root.__plEndpointCtx);
        return;
      }
      select.innerHTML = [...resources].sort((a, b) => String(a.name || a.id).localeCompare(String(b.name || b.id), undefined, { sensitivity: "base", numeric: true })).map((r) => {
        const caps = resourceCaps(r);
        const endpointType = r.discovery ? "discovery" : ((r.extra && r.extra.endpoint_type) || r.endpoint_type || r.playlist_type || r.kind || "");
        const shouldSelect = selected ? r.id === selected : (!isEdit && resources[0] && r.id === resources[0].id);
        return `<option value="${esc(r.id)}" data-name="${esc(r.name || r.id)}" data-type="${esc(endpointType || "playlist")}" data-media-types="${esc((r.media_types || []).join(","))}" data-kind="${esc(r.kind || "regular")}" ${shouldSelect ? "selected" : ""}>${esc(r.name || r.id)}${caps.length ? ` (${esc(caps.join("/"))})` : ""}</option>`;
      }).join("");
      help.textContent = resolveError || (isEdit ? T("playlists.ep.selectOne") : T("playlists.ep.selectMany"));
    } catch (err) {
      if (stale()) return;
      select.innerHTML = `<option value="">${TH("playlists.ep.loadFailed")}</option>`;
      help.textContent = err && err.message ? err.message : T("playlists.ep.loadFailedDetail");
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
    const nameErr = nameFieldError(name, T("playlists.ep.name"));
    if (nameErr) throw new Error(nameErr);
    if (!provider) throw new Error(T("playlists.ep.providerRequired"));
    if (create) {
      const createName = val("#pl-ep-create-name", root);
      const createNameErr = playlistNameError(createName);
      if (createNameErr) throw new Error(createNameErr);
      const createType = val("#pl-ep-create-type", root) || creatableEndpointTypes(provider)[0];
      const privacy = privacyPayload($("#pl-ep-create-privacy", root), $("#pl-ep-create-privacy-wrap", root)?.classList.contains("hidden"));
      await API.epUpsert({ id: isEdit ? seed.id : "", name: name || nextEndpointName(0, isEdit ? seed.id : ""), provider, instance, create: true, create_name: createName, media_type: createType, ...privacy });
    } else {
      const ids = selectedValues("#pl-ep-playlist", root);
      if (!ids.length) throw new Error(T("playlists.ep.selectAtLeastOnePlaylist"));
      if (isEdit && ids.length !== 1) throw new Error(T("playlists.ep.editOnlyOne"));
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
      openNotice(T("playlists.ep.refreshFailed"), err && err.message ? err.message : T("playlists.ep.refreshFailedDetail"), btn);
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
        <div><b>${TH("playlists.del.endpoint")}</b> ${esc(endpoint.name || endpoint.id)}</div>
        <div><b>${TH("playlists.del.provider")}</b> ${esc(endpoint.provider_label || providerLabel(endpoint.provider))}</div>
        <div><b>${TH("playlists.del.selectedPlaylist")}</b> ${esc(endpoint.playlist_name || endpoint.playlist_id || "-")}</div>
        <div><b>${TH("playlists.del.mappingsUsing")}</b> ${esc(used.length)}</div>
        ${used.length ? `<div class="pl-warning">${TH("playlists.ui.usedBy", {names: used.map((m) => m.name || m.id).join(", ")})}</div>` : `<div>${TH("playlists.del.endpointNote")}</div>`}
      </div>
    `;
    openModal({
      title: T("playlists.del.endpointTitle"),
      description: T("playlists.del.endpointConfirm"),
      body,
      trigger,
      primaryText: T("playlists.deleteEndpoint"),
      savingText: T("playlists.deleting"),
      onPrimary: async () => {
        await API.epDelete(endpoint.id);
        closeModal(true);
        await refreshOverview();
      },
    });
  }

  function openMappingModal({ mapping = null, clone = false, draft = null, trigger = null, onDone = null } = {}) {
    if (state.endpoints.length < 2) {
      openNotice(T("playlists.createMapping"), T("playlists.map.needTwo"), trigger);
      return;
    }
    const seed = draft || mapping || {};
    const isEdit = !!mapping && !clone && !draft;
    const source = seed.source_endpoint || (state.endpoints[0] && state.endpoints[0].id) || "";
    const targets = seed.target_endpoints || (state.endpoints.find((e) => e.id !== source) ? [state.endpoints.find((e) => e.id !== source).id] : []);
    const target = targets.find((id) => id !== source) || targets[0] || "";
    const title = isEdit ? T("playlists.map.editTitle") : T("playlists.createMapping");
    const endpointOpts = state.endpoints.map((ep) => ({ value: ep.id, label: `${ep.name || ep.id} (${ep.provider}${endpointIsDiscovery(ep) ? ` ${T("playlists.discoveryLower")}` : ""})` }));
    const rulesetOpts = [{ value: "", label: T("playlists.map.directOneWay") }].concat(state.rulesets.map((rs) => ({ value: rs.id, label: `${rs.name}${rs.built_in ? ` ${T("playlists.builtInParen")}` : ""}` })));
    const keepSeedName = isEdit || !!draft;
    const mappingName = keepSeedName ? (seed.name || seed.id || "") : nextMappingName();
    const body = `
      <div class="pl-mapping-wizard">
        <section class="pl-mapping-step">
          <div class="pl-step-head"><span class="pl-step-index">1</span><div><b>${TH("playlists.map.details")}</b><span>${TH("playlists.map.nameHint")}</span></div></div>
          <div class="pl-form">
            <div class="pl-field full">
              <label for="pl-map-name">${TH("playlists.map.name")} <span aria-hidden="true">*</span></label>
              <input id="pl-map-name" maxlength="${NAME_MAX}" value="${esc(mappingName)}" placeholder="${TH("playlists.map.namePlaceholder")}" aria-describedby="pl-map-name-error">
              <div class="pl-field-error" id="pl-map-name-error"></div>
            </div>
          </div>
        </section>
        <section class="pl-mapping-step">
          <div class="pl-step-head"><span class="pl-step-index">2</span><div><b>${TH("playlists.map.config")}</b><span>${TH("playlists.map.configHint")}</span></div></div>
          <div class="pl-map-grid">
            <div class="pl-field">
              <label for="pl-map-source">${TH("playlists.map.source")} <span aria-hidden="true">*</span></label>
              <select id="pl-map-source">${selectOptions(endpointOpts, source)}</select>
            </div>
            <div class="pl-field">
              <label for="pl-map-targets">${TH("playlists.map.destination")} <span aria-hidden="true">*</span></label>
              <select id="pl-map-targets">${selectOptions(endpointOpts, target)}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-direction-field">
              <label for="pl-map-direction">${TH("playlists.col.direction")}</label>
              <select id="pl-map-direction">${selectOptions(ENUMS.direction.map(([value, label]) => ({ value, label })), (rulesetById(seed.ruleset_id || "") || {}).direction || "one_way")}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-ruleset-field">
              <label for="pl-map-ruleset">${TH("playlists.col.ruleset")}</label>
              <select id="pl-map-ruleset">${selectOptions(rulesetOpts, seed.ruleset_id || "")}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-membership-field">
              <label for="pl-map-membership">${TH("playlists.map.syncMode")}</label>
              <select id="pl-map-membership">${selectOptions(ENUMS.membership.map(([value, label]) => ({ value, label })), seed.membership || "managed_only")}</select>
            </div>
            <div class="pl-field pl-map-advanced" id="pl-map-order-field">
              <label for="pl-map-order">${TH("playlists.map.ordering")}</label>
              <select id="pl-map-order">${selectOptions(ENUMS.order.map(([value, label]) => ({ value, label })), seed.order || "ignore")}</select>
            </div>
          </div>
          <label class="pl-map-toggle"><input type="checkbox" id="pl-map-enabled" ${seed.enabled === false ? "" : "checked"}><span class="pl-map-toggle-box"><span class="material-symbols-rounded" aria-hidden="true">check</span></span><span><b>${TH("playlists.enabled")}</b><small>${TH("playlists.map.enabledHint")}</small></span></label>
          <div class="pl-warning hidden" id="pl-map-simkl-warning">${esc(SIMKL_PLAYLIST_WARNING)}</div>
          <div class="pl-map-ruleset-card pl-map-advanced" id="pl-map-ruleset-actions">
            <span class="material-symbols-rounded" aria-hidden="true">shield</span>
            <div><b>${TH("playlists.map.rulesets")}</b><span>${TH("playlists.map.rulesetsHint")}</span></div>
            <button type="button" class="pl-btn small" id="pl-map-manage-rulesets">${TH("playlists.map.manageRulesets")} <span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
          </div>
          <div class="pl-map-info"><span class="material-symbols-rounded" aria-hidden="true">info</span><div class="pl-help" id="pl-map-rule-help"></div></div>
          <div class="pl-map-info hidden" id="pl-map-discovery-help"><span class="material-symbols-rounded" aria-hidden="true">travel_explore</span><div class="pl-help">${TH("playlists.map.discoveryHint")}</div></div>
        </section>
      </div>
    `;
    openModal({
      title,
      description: isEdit ? T("playlists.map.updateHint") : T("playlists.map.connectHint"),
      body,
      trigger,
      width: "760px",
      primaryText: isEdit ? T("playlists.map.save") : T("playlists.map.create"),
      savingText: T("playlists.map.saving"),
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
    bindNameValidation(ctx, "#pl-map-name", T("playlists.map.name"));
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
      const sourceReadOnly = !sourceDiscovery && !!sourceEp && !endpointWritable(sourceEp);
      const directionSelect = $("#pl-map-direction", root);
      if (sourceReadOnly && directionSelect) {
        directionSelect.value = "one_way";
        directionSelect.disabled = true;
        direction = "one_way";
        if (rule && rule.direction === "bidirectional") {
          $("#pl-map-ruleset", root).value = "";
          rule = null;
        }
      }
      if (!rule) help.textContent = T("playlists.map.directHelp");
      else help.textContent = T("playlists.mapping.ruleHelp", {name: rule.name, direction: ruleLabel(rule.direction), mode: rule.write_mode, count: Number(rule.maximum_targets) || 0});
      if (targetEp && !endpointWritable(targetEp)) help.textContent = T("playlists.map.chooseWritable");
      if (rule && rule.direction !== direction) help.textContent += ` ${T("playlists.mapping.changeDirection")}`;
      if (sourceReadOnly) help.textContent += ` ${T("playlists.mapping.sourceReadOnly")}`;
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
    const nameErr = nameFieldError(draft.name, T("playlists.map.name"));
    if (nameErr) throw new Error(nameErr);
    if (!draft.source_endpoint || !draft.target_endpoints.length) throw new Error(T("playlists.map.endpointsRequired"));
    if (draft.target_endpoints.includes(draft.source_endpoint)) throw new Error(T("playlists.map.mustDiffer"));
    if (!draft.ruleset_id && draft.target_endpoints.length !== 1) throw new Error(T("playlists.map.directExactlyOne"));
    if (rule && rule.direction !== direction) throw new Error(T("playlists.mapping.rulesetSupports", {supported: ruleLabel(rule.direction), chosen: ruleLabel(direction)}));
    if (rule && draft.target_endpoints.length > Number(rule.maximum_targets || 1)) throw new Error(T("playlists.map.tooMany"));
    if (endpointIsDiscovery(endpointById(draft.source_endpoint)) && (draft.ruleset_id || draft.membership !== "mirror")) throw new Error(T("playlists.map.discoveryMirror"));
    if (draft.target_endpoints.some((id) => !endpointWritable(endpointById(id)))) throw new Error(T("playlists.map.mustBeWritable"));
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
      openNotice(T("playlists.run.alreadyRunning"), T("playlists.run.alreadyRunningHint"), btn);
      return;
    }
    if (!mapping.assigned_pair) {
      openNotice(T("playlists.run.pairMissing"), T("playlists.run.pairMissingHint"), btn);
      return;
    }
    location.hash = `interactive_sync?pair=${encodeURIComponent(mapping.assigned_pair)}&from=playlists`;
  }

  const SYNC_MODES = [
    ["add_only", T("playlists.enums.addOnly"), T("playlists.modes.addOnlyLong")],
    ["managed_only", T("playlists.modes.keepInSync"), T("playlists.modes.keepInSyncLong")],
    ["mirror", T("playlists.modes.exactCopy"), T("playlists.modes.exactCopyLong")],
  ];

  function wizardAccounts() {
    return state.providers
      .filter((p) => p && p.configured && PLAYLIST_COMPATIBLE_PROVIDERS.has(String(p.provider || "").toUpperCase()))
      .map((p) => ({ provider: String(p.provider).toUpperCase(), instance: p.instance || "default", label: p.label || providerLabel(p.provider) }))
      .sort((a, b) => Number(a.provider === "CROSSWATCH") - Number(b.provider === "CROSSWATCH"));
  }

  function wizardResourceType(resource) {
    return String(resource.discovery ? "discovery" : ((resource.extra && resource.extra.endpoint_type) || resource.endpoint_type || resource.playlist_type || resource.kind || "")).toLowerCase();
  }

  function wizardCreateType(resource, provider) {
    const types = creatableEndpointTypes(provider);
    const single = { movies: "movie", shows: "show", tv: "show" };
    const wanted = ((resource && resource.media_types) || []).map((t) => String(t || "").toLowerCase());
    return wanted.find((t) => types.includes(t)) || wanted.map((t) => single[t] || t).find((t) => types.includes(t)) || types[0];
  }

  function wizardResourceWritable(resource) {
    return !resource.discovery && !resource.smart && !!(resource.can_add || resource.can_remove);
  }

  async function wizardEnsureEndpoint(account, resource, offset) {
    const existing = state.endpoints.find((ep) => String(ep.provider || "").toUpperCase() === account.provider
      && String(ep.instance || "default") === account.instance && String(ep.playlist_id || "") === String(resource.id));
    if (existing) return existing;
    const res = await API.epUpsert({
      id: "", name: nextEndpointName(offset), provider: account.provider, instance: account.instance,
      playlist_id: resource.id, playlist_name: resource.name || resource.id, playlist_type: wizardResourceType(resource),
      media_types: resource.media_types || [],
    });
    return res.endpoint;
  }

  function openSyncWizard({ trigger = null, publish = false } = {}) {
    const accounts = wizardAccounts();
    if (!accounts.length) {
      openNotice(T("playlists.wizard.noProvider"), T("playlists.wizard.noProviderHint"), trigger);
      return;
    }
    const key = (a) => `${a.provider}|${a.instance}`;
    const accountOptions = (list) => [...list].sort((a, b) => String(a.label).localeCompare(String(b.label), undefined, { sensitivity: "base", numeric: true }) || String(a.instance).localeCompare(String(b.instance), undefined, { sensitivity: "base", numeric: true })).map((a) => `<option value="${esc(key(a))}">${esc(a.label)}${a.instance !== "default" ? ` · ${esc(a.instance)}` : ""}</option>`).join("");
    const cwAccounts = accounts.filter((a) => a.provider === "CROSSWATCH");
    const lists = new Map();
    const failed = new Set();
    const wizard = { publish: publish && cwAccounts.length > 0 };
    const body = `
      <div class="pl-form pl-wizard">
        <section class="pl-endpoint-step">
          <div class="pl-step-head"><span class="pl-step-index">1</span><div><b>${TH("playlists.col.from")}</b><span>${TH("playlists.wizard.listToRead")}</span></div></div>
          <div class="pl-wizard-row">
            <div class="pl-field"><label for="pl-wz-src-account">${TH("playlists.col.provider")}</label><select id="pl-wz-src-account"><option value="">${TH("playlists.wizard.chooseProvider")}</option>${accountOptions(accounts)}</select></div>
            <div class="pl-field"><label for="pl-wz-src-list">${TH("playlists.col.list")}</label><select id="pl-wz-src-list"></select></div>
          </div>
          <div id="pl-wz-src-link-toggle-row" hidden><button type="button" class="pl-link" id="pl-wz-src-link-toggle">${TH("playlists.wizard.useLink")}</button></div>
          <div class="pl-wizard-row" id="pl-wz-src-link-row" hidden>
            <div class="pl-field"><label for="pl-wz-src-link">${TH("playlists.ep.listLink")}</label><input id="pl-wz-src-link" placeholder="${TH("playlists.pasteLink")}" autocomplete="off" aria-describedby="pl-wz-src-link-error"><div class="pl-field-error" id="pl-wz-src-link-error"></div></div>
            <div class="pl-field"><label for="pl-wz-src-link-add">${TH("playlists.wizard.addAsSource")}</label><button type="button" class="pl-btn small primary" id="pl-wz-src-link-add">${TH("playlists.addList")}</button></div>
          </div>
        </section>
        <section class="pl-endpoint-step">
          <div class="pl-step-head"><span class="pl-step-index">2</span><div><b>${TH("playlists.col.to")}</b><span>${TH("playlists.wizard.whereItems")}</span></div></div>
          <div class="pl-wizard-choice" role="radiogroup" aria-label="${TH("playlists.col.destination")}">
            <label><input type="radio" name="pl-wz-dest" value="list" ${wizard.publish ? "" : "checked"}><span><b>${TH("playlists.wizard.onProvider")}</b><small>${TH("playlists.wizard.onProviderHint")}</small></span></label>
            <label class="${cwAccounts.length ? "" : "is-disabled"}"><input type="radio" name="pl-wz-dest" value="publish" ${wizard.publish ? "checked" : ""} ${cwAccounts.length ? "" : "disabled"}><span><b>${TH("playlists.wizard.tools")}</b><small>${cwAccounts.length ? T("playlists.wizard.toolsHint") : T("playlists.wizard.needsCw")}</small></span></label>
          </div>
          <div class="pl-wizard-row" id="pl-wz-dst-list-row">
            <div class="pl-field"><label for="pl-wz-dst-account">${TH("playlists.col.provider")}</label><select id="pl-wz-dst-account"><option value="">${TH("playlists.wizard.chooseProvider")}</option>${accountOptions(accounts)}</select></div>
            <div class="pl-field"><label for="pl-wz-dst-list">${TH("playlists.col.list")}</label><select id="pl-wz-dst-list"></select></div>
          </div>
          <div id="pl-wz-new-toggle-row"><button type="button" class="pl-link" id="pl-wz-new-toggle">${TH("playlists.wizard.createNew")}</button><span class="pl-muted" id="pl-wz-new-none"></span></div>
          <div class="pl-warning" id="pl-wz-simkl-warning" hidden>${esc(SIMKL_PLAYLIST_WARNING)}</div>
          <div class="pl-wizard-row" id="pl-wz-new-row">
            ${cwAccounts.length > 1 ? `<div class="pl-field" id="pl-wz-cw-wrap"><label for="pl-wz-cw-account">${TH("playlists.wizard.cwInstance")}</label><select id="pl-wz-cw-account">${accountOptions(cwAccounts)}</select></div>` : ""}
            <div class="pl-field"><label for="pl-wz-new-name">${TH("playlists.wizard.newName")}</label><input id="pl-wz-new-name" maxlength="${PLAYLIST_NAME_MAX}" autocomplete="off"><div class="pl-field-error"></div></div>
            <div class="pl-field" id="pl-wz-new-type-wrap" hidden><label for="pl-wz-new-type">${TH("playlists.wizard.listType")}</label><select id="pl-wz-new-type"></select></div>
            <div class="pl-field" id="pl-wz-new-privacy-wrap" hidden><label for="pl-wz-new-privacy">${TH("playlists.ep.privacy")}</label><select id="pl-wz-new-privacy"></select></div>
          </div>
        </section>
        <section class="pl-endpoint-step">
          <div class="pl-step-head"><span class="pl-step-index">3</span><div><b>${TH("playlists.col.how")}</b><span>${TH("playlists.wizard.whatMayChange")}</span></div></div>
          <div class="pl-wizard-choice" role="radiogroup" aria-label="${TH("playlists.map.syncMode")}">
            ${SYNC_MODES.map(([value, label, hint]) => `<label><input type="radio" name="pl-wz-mode" value="${value}" ${value === "managed_only" ? "checked" : ""}><span><b>${esc(label)}</b><small>${esc(hint)}</small></span></label>`).join("")}
          </div>
          <div class="pl-muted" id="pl-wz-note"></div>
        </section>
      </div>
    `;
    const ctx = openModal({
      title: T("playlists.wizard.title"),
      description: T("playlists.wizard.intro"),
      width: "720px",
      body,
      primaryText: T("playlists.wizard.create"),
      savingText: T("playlists.wizard.creating"),
      trigger,
      onPrimary: (modalCtx) => saveSyncWizard(modalCtx, lists),
    });
    const root = ctx.modal;
    const accountOf = (id) => accounts.find((a) => key(a) === val(id, root));
    const destination = () => (root.querySelector('input[name="pl-wz-dest"]:checked') || {}).value || "list";
    const sourceResource = () => (lists.get(val("#pl-wz-src-account", root)) || []).find((r) => String(r.id) === val("#pl-wz-src-list", root));

    async function fill(selectId, accountId, { writable = false } = {}) {
      const select = $(selectId, root);
      const account = accountOf(accountId);
      if (!select) return;
      if (!account) {
        select.innerHTML = `<option value="">${TH("playlists.wizard.chooseProviderFirst")}</option>`;
        select.disabled = true;
        if (writable) select.dataset.canCreate = "";
        sync();
        return;
      }
      const cacheKey = key(account);
      select.innerHTML = `<option value="">${TH("playlists.loading")}</option>`;
      select.disabled = true;
      if (!lists.has(cacheKey)) {
        try {
          const data = await API.resources(account.provider, account.instance);
          lists.set(cacheKey, data.resources || []);
          failed.delete(cacheKey);
        } catch (err) {
          failed.add(cacheKey);
        }
      }
      if (val(accountId, root) !== cacheKey) return;
      const broken = [val("#pl-wz-src-account", root), val("#pl-wz-dst-account", root)].filter((id) => failed.has(id)).map((id) => (accounts.find((a) => key(a) === id) || {}).label || id);
      setModalError(broken.length ? T("playlists.wizard.loadListsFailed", {names: [...new Set(broken)].join(` ${T("playlists.and")} `)}) : "");
      if (failed.has(cacheKey)) {
        select.innerHTML = `<option value="">${TH("playlists.wizard.loadFailed")}</option>`;
        select.disabled = true;
        sync();
        return;
      }
      const rows = (lists.get(cacheKey) || []).filter((r) => !writable || wizardResourceWritable(r));
      const canCreate = writable && creatableEndpointTypes(account.provider).length > 0;
      select.innerHTML = (canCreate ? `<option value="__new__">${TH("playlists.wizard.createNew")}</option>` : "")
        + [...rows].sort((a, b) => String(a.name || a.id).localeCompare(String(b.name || b.id), undefined, { sensitivity: "base", numeric: true })).map((r) => `<option value="${esc(r.id)}">${esc(r.name || r.id)}${wizardResourceType(r) === "watchlist" || String(r.id).toLowerCase().includes("watchlist") ? "" : ` (${esc(titleize(wizardResourceType(r) || "list"))})`}</option>`).join("")
        || `<option value="">${TH("playlists.wizard.noneFound")}</option>`;
      if (rows.length) select.value = String(rows[0].id);
      if (writable) select.dataset.canCreate = canCreate ? "1" : "";
      select.disabled = false;
      sync();
    }

    function syncNewListOptions(account) {
      const typeWrap = $("#pl-wz-new-type-wrap", root);
      const typeSelect = $("#pl-wz-new-type", root);
      const privacyWrap = $("#pl-wz-new-privacy-wrap", root);
      const types = account ? creatableEndpointTypes(account.provider) : [];
      if (typeSelect && types.length > 1) {
        const wanted = typeSelect.dataset.dirty && types.includes(typeSelect.value) ? typeSelect.value : wizardCreateType(sourceResource(), account.provider);
        typeSelect.innerHTML = selectOptions(types.map((t) => ({ value: t, label: createTypeLabel(t) })), wanted);
        window.CW?.IconSelect?.enhance?.(typeSelect, { className: "cw-plain-select" });
      }
      if (typeWrap) typeWrap.hidden = types.length <= 1;
      const type = types.length > 1 ? val("#pl-wz-new-type", root) : types[0];
      const opts = account ? fillPrivacySelect($("#pl-wz-new-privacy", root), account.provider, type) : null;
      if (privacyWrap) privacyWrap.hidden = !opts;
    }

    function sync() {
      const publishing = destination() === "publish";
      const creating = !publishing && val("#pl-wz-dst-list", root) === "__new__";
      $("#pl-wz-dst-list-row", root).hidden = publishing;
      $("#pl-wz-new-row", root).hidden = !(publishing || creating);
      const toggle = $("#pl-wz-new-toggle", root);
      const dstAccount = accountOf("#pl-wz-dst-account");
      const canCreate = !!$("#pl-wz-dst-list", root).dataset.canCreate;
      $("#pl-wz-new-toggle-row", root).hidden = publishing || !dstAccount;
      toggle.hidden = !canCreate;
      toggle.textContent = creating ? T("playlists.wizard.useExisting") : T("playlists.wizard.createNew");
      $("#pl-wz-new-none", root).textContent = canCreate || !dstAccount ? "" : T("playlists.wizard.cannotCreateOn", {name: dstAccount.label});
      $("#pl-wz-simkl-warning", root).hidden = publishing || !dstAccount || dstAccount.provider !== "SIMKL";
      const srcAccount = accountOf("#pl-wz-src-account");
      const canLink = !!srcAccount && providerCanResolve(srcAccount.provider);
      $("#pl-wz-src-link-toggle-row", root).hidden = !canLink;
      if (!canLink) $("#pl-wz-src-link-row", root).hidden = true;
      if (canLink) $("#pl-wz-src-link", root).placeholder = providerLinkExample(srcAccount.provider);
      const cwWrap = $("#pl-wz-cw-wrap", root);
      if (cwWrap) cwWrap.hidden = !publishing;
      syncNewListOptions(creating ? dstAccount : null);
      const nameInput = $("#pl-wz-new-name", root);
      const src = sourceResource();
      if (nameInput && !nameInput.dataset.dirty && src) nameInput.value = String(src.name || "").replace(/[^A-Za-z0-9 _.'&()-]/g, "").trim().slice(0, PLAYLIST_NAME_MAX);
      const discovery = !!(src && src.discovery);
      root.querySelectorAll('input[name="pl-wz-mode"]').forEach((input) => {
        input.disabled = discovery && input.value !== "mirror";
        if (discovery && input.value === "mirror") input.checked = true;
      });
      $("#pl-wz-note", root).textContent = discovery
        ? T("playlists.wizard.discoveryExact")
        : publishing ? T("playlists.wizard.afterCreate") : "";
      syncModalPrimary(ctx);
    }

    ctx.validators = [() => {
      if (!val("#pl-wz-src-list", root)) return T("playlists.wizard.chooseSource");
      const publishing = destination() === "publish";
      const target = val("#pl-wz-dst-list", root);
      if (!publishing && !target) return T("playlists.wizard.chooseDestination");
      if (publishing || target === "__new__") return playlistNameError(val("#pl-wz-new-name", root));
      if (val("#pl-wz-src-account", root) === val("#pl-wz-dst-account", root) && val("#pl-wz-src-list", root) === target) return T("playlists.wizard.mustDiffer");
      return "";
    }];
    root.addEventListener("change", (e) => {
      if (e.target.id === "pl-wz-src-account") fill("#pl-wz-src-list", "#pl-wz-src-account");
      else if (e.target.id === "pl-wz-dst-account") fill("#pl-wz-dst-list", "#pl-wz-dst-account", { writable: true });
      else sync();
    });
    $("#pl-wz-new-toggle", root).addEventListener("click", () => {
      const select = $("#pl-wz-dst-list", root);
      const existing = Array.from(select.options).find((o) => o.value && o.value !== "__new__");
      select.value = select.value === "__new__" ? (existing ? existing.value : "__new__") : "__new__";
      sync();
      if (select.value === "__new__") $("#pl-wz-new-name", root)?.focus();
    });
    $("#pl-wz-src-link-toggle", root).addEventListener("click", () => {
      const row = $("#pl-wz-src-link-row", root);
      row.hidden = !row.hidden;
      if (!row.hidden) $("#pl-wz-src-link", root)?.focus();
    });
    async function addSourceLink() {
      const input = $("#pl-wz-src-link", root);
      const button = $("#pl-wz-src-link-add", root);
      const account = accountOf("#pl-wz-src-account");
      const ref = String(val("#pl-wz-src-link", root) || "").trim();
      if (!account) return;
      if (!ref) {
        applyFieldError(input, T("playlists.pasteLinkError"), "");
        return;
      }
      button.disabled = true;
      try {
        const cacheKey = key(account);
        const res = await API.resourceResolve(account.provider, account.instance, ref);
        const resource = res.resource || {};
        const current = (lists.get(cacheKey) || []).filter((r) => String(r.id) !== String(resource.id));
        lists.set(cacheKey, [...current, resource]);
        if (val("#pl-wz-src-account", root) !== cacheKey) return;
        await fill("#pl-wz-src-list", "#pl-wz-src-account");
        if (val("#pl-wz-src-account", root) !== cacheKey) return;
        $("#pl-wz-src-list", root).value = String(resource.id);
        input.value = "";
        applyFieldError(input, "", "");
        $("#pl-wz-src-link-row", root).hidden = true;
        sync();
      } catch (error) {
        applyFieldError(input, error && error.message ? error.message : T("playlists.listNotFound"), "");
      } finally {
        button.disabled = false;
      }
    }
    $("#pl-wz-src-link-add", root).addEventListener("click", addSourceLink);
    $("#pl-wz-src-link", root).addEventListener("keydown", (e) => {
      if (e.key !== "Enter") return;
      e.preventDefault();
      addSourceLink();
    });
    root.addEventListener("input", (e) => {
      if (e.target.id === "pl-wz-new-name") e.target.dataset.dirty = "1";
      syncModalPrimary(ctx);
    });
    $("#pl-wz-new-type", root)?.addEventListener("change", (e) => {
      e.target.dataset.dirty = "1";
      sync();
    });
    fill("#pl-wz-src-list", "#pl-wz-src-account");
    fill("#pl-wz-dst-list", "#pl-wz-dst-account", { writable: true });
    ctx.initial = snapshot($(".pl-dialog-body", root));
    sync();
  }

  function openSyncEdit(mapping, trigger) {
    if (!mapping) return;
    const src = endpointById(mapping.source_endpoint) || mapping.source || {};
    const targets = (mapping.target_endpoints || []).map((id) => endpointById(id)).filter(Boolean);
    const discovery = endpointIsDiscovery(src);
    const current = discovery ? "mirror" : String(mapping.membership || "managed_only");
    const ctx = openModal({
      title: T("playlists.editSync"),
      description: T("playlists.wizard.editIntro"),
      width: "640px",
      primaryText: T("playlists.save"),
      trigger,
      body: `
        <div class="pl-form pl-wizard">
          <div class="pl-wizard-pair">${listRef(src, mapping.source_endpoint)}<span class="material-symbols-rounded" aria-hidden="true">east</span>${targets.map((t) => listRef(t)).join("") || mappingTargetRefs(mapping)}</div>
          <div class="pl-wizard-choice" role="radiogroup" aria-label="${TH("playlists.map.syncMode")}">
            ${SYNC_MODES.map(([value, label, hint]) => `<label><input type="radio" id="pl-se-mode-${value}" name="pl-se-mode" value="${value}" ${value === current ? "checked" : ""} ${discovery && value !== "mirror" ? "disabled" : ""}><span><b>${esc(label)}</b><small>${esc(hint)}</small></span></label>`).join("")}
          </div>
          ${discovery ? `<div class="pl-muted">${TH("playlists.wizard.discoveryExact")}</div>` : ""}
          <label class="pl-wizard-check"><input type="checkbox" id="pl-se-enabled" ${mapping.enabled ? "checked" : ""}><span>${TH("playlists.wizard.activeHint")}</span></label>
          <div><button type="button" class="pl-link" data-sync-advanced>${TH("playlists.wizard.moreSettings")}</button></div>
        </div>
      `,
      onPrimary: async (modalCtx) => {
        const root = modalCtx.modal;
        const mode = (root.querySelector('input[name="pl-se-mode"]:checked') || {}).value || current;
        const res = await API.mapUpsert({
          id: mapping.id, name: mapping.name, source_endpoint: mapping.source_endpoint,
          target_endpoints: mapping.target_endpoints || [], ruleset_id: "", membership: mode,
          order: mapping.order || "ignore", enabled: checked("#pl-se-enabled", root),
        });
        notifyPairsChanged({ source: "playlists", mapping_id: mapping.id, pair_id: res.pair_id || (res.mapping && res.mapping.assigned_pair) || "" });
        closeModal(true);
        await refreshOverview();
      },
    });
    $("[data-sync-advanced]", ctx.modal)?.addEventListener("click", async () => {
      if (await closeModal(false)) openMappingModal({ mapping, trigger });
    });
  }

  async function saveSyncWizard(ctx, lists) {
    const root = ctx.modal;
    const accounts = wizardAccounts();
    const byKey = (value) => accounts.find((a) => `${a.provider}|${a.instance}` === value);
    const srcAccount = byKey(val("#pl-wz-src-account", root));
    const srcResource = (lists.get(val("#pl-wz-src-account", root)) || []).find((r) => String(r.id) === val("#pl-wz-src-list", root));
    if (!srcAccount || !srcResource) throw new Error(T("playlists.wizard.chooseSource"));
    const publishing = ((root.querySelector('input[name="pl-wz-dest"]:checked') || {}).value || "list") === "publish";
    const mode = (root.querySelector('input[name="pl-wz-mode"]:checked') || {}).value || "managed_only";
    const newName = val("#pl-wz-new-name", root);
    const source = await wizardEnsureEndpoint(srcAccount, srcResource, 0);
    let target;
    if (publishing || val("#pl-wz-dst-list", root) === "__new__") {
      const account = publishing
        ? (byKey(val("#pl-wz-cw-account", root)) || accounts.find((a) => a.provider === "CROSSWATCH"))
        : byKey(val("#pl-wz-dst-account", root));
      if (!account) throw new Error(T("playlists.wizard.chooseWhereNew"));
      const typeWrap = $("#pl-wz-new-type-wrap", root);
      const mediaType = !publishing && typeWrap && !typeWrap.hidden && val("#pl-wz-new-type", root) ? val("#pl-wz-new-type", root) : wizardCreateType(srcResource, account.provider);
      const privacy = publishing ? {} : privacyPayload($("#pl-wz-new-privacy", root), $("#pl-wz-new-privacy-wrap", root)?.hidden);
      const res = await API.epUpsert({ id: "", name: nextEndpointName(1), provider: account.provider, instance: account.instance, create: true, create_name: newName, media_type: mediaType, ...privacy });
      target = res.endpoint;
    } else {
      const account = byKey(val("#pl-wz-dst-account", root));
      const resource = (lists.get(val("#pl-wz-dst-account", root)) || []).find((r) => String(r.id) === val("#pl-wz-dst-list", root));
      if (!account || !resource) throw new Error(T("playlists.wizard.chooseDestination"));
      target = await wizardEnsureEndpoint(account, resource, 1);
    }
    if (!source || !target || !source.id || !target.id) throw new Error(T("playlists.wizard.prepareFailed"));
    const res = await API.mapUpsert({ id: "", name: nextMappingName(0), source_endpoint: source.id, target_endpoints: [target.id], ruleset_id: "", membership: mode, order: "ignore", enabled: true });
    notifyPairsChanged({ source: "playlists", mapping_id: (res.mapping && res.mapping.id) || "", pair_id: res.pair_id || (res.mapping && res.mapping.assigned_pair) || "" });
    if (publishing && target.playlist_id) await API.publish(target.instance || "default", target.playlist_id).catch(() => null);
    closeModal(true);
    await refreshOverview();
    if (publishing) openPublishedModal(publishedRow(`${target.instance || "default"}|${target.playlist_id}`), null);
  }

  function openMappingDelete(mapping, trigger) {
    if (!mapping) return;
    const source = endpointById(mapping.source_endpoint);
    const rule = mapping.ruleset || rulesetById(mapping.ruleset_id || "");
    const body = `
      <div class="pl-confirm-lines">
        <div><b>${TH("playlists.del.mapping")}</b> ${esc(mapping.name || mapping.id)}</div>
        <div><b>${TH("playlists.del.source")}</b> ${esc(source ? source.name : mapping.source_endpoint)}</div>
        <div><b>${TH("playlists.del.destination")}</b> ${esc(targetNames(mapping) || "-")}</div>
        <div><b>${TH("playlists.del.direction")}</b> ${esc(directionFor(mapping))}</div>
        <div><b>${TH("playlists.del.ruleset")}</b> ${esc(rule ? rule.name : T("playlists.enums.direct"))}</div>
        <div>${TH("playlists.del.mappingNote")}</div>
      </div>
    `;
    openModal({
      title: T("playlists.del.mappingTitle"),
      description: T("playlists.del.mappingConfirm"),
      body,
      trigger,
      primaryText: T("playlists.deleteMapping"),
      savingText: T("playlists.deleting"),
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
          <td><span class="pl-pill ${rs.built_in ? "warn" : "ok"}">${rs.built_in ? T("playlists.rules.builtIn") : T("playlists.presets.custom")}</span></td>
          <td>${esc(ruleLabel(rs.direction))}</td>
          <td>${TH("playlists.rules.readWrite", {read: titleize(rs.read_mode), write: titleize(rs.write_mode)})}</td>
          <td>${TH("playlists.rules.capacityCell", {capacity: String(rs.per_endpoint_capacity), count: Number(rs.maximum_targets) || 0})}</td>
          <td>${esc(used.length)}</td>
          <td>
            <div class="pl-actions">
              ${rulesetActionButton("view", rs.id, T("playlists.rules.view"), "visibility")}
              ${rulesetActionButton("clone", rs.id, T("playlists.rules.clone"), "content_copy", "sync")}
              ${rulesetActionButton("edit", rs.id, rs.built_in ? T("playlists.rules.cannotEdit") : T("playlists.rules.edit"), "edit", "edit", !!rs.built_in)}
              ${rulesetActionButton("delete", rs.id, rs.built_in ? T("playlists.rules.cannotDelete") : T("playlists.rules.delete"), "delete", "delete", !!rs.built_in)}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    const body = `
      <div class="pl-table-wrap cw-page-table">
        <table class="pl-ruleset-table">
          <thead><tr><th>${TH("playlists.rules.name")}</th><th>${TH("playlists.col.type")}</th><th>${TH("playlists.col.direction")}</th><th>${TH("playlists.rules.strategy")}</th><th>${TH("playlists.rules.capacity")}</th><th>${TH("playlists.mappings")}</th><th>${TH("playlists.col.actions")}</th></tr></thead>
          <tbody>${rows || `<tr><td colspan="7"><div class="pl-empty"><strong>${TH("playlists.rules.none")}</strong><span>${TH("playlists.rules.noneHint")}</span></div></td></tr>`}</tbody>
        </table>
      </div>
    `;
    openModal({
      title: T("playlists.map.manageRulesets"),
      description: T("playlists.rules.manageHint"),
      body,
      trigger,
      width: "980px",
      primaryText: T("playlists.rules.createNew"),
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
    const title = mode === "view" ? T("playlists.rules.view") : isEdit ? T("playlists.rules.edit") : clone ? T("playlists.rules.clone") : T("playlists.rules.createNew");
    const body = rulesetBuilderHtml(seed, preset, readonly, clone);
    openModal({
      title,
      description: readonly ? T("playlists.rules.viewHint") : T("playlists.rules.editHint"),
      body,
      trigger,
      width: "920px",
      primaryText: readonly && ruleset && ruleset.built_in ? T("playlists.rules.clone") : readonly ? "" : (isEdit ? T("playlists.ep.saveChanges") : T("playlists.rules.create")),
      savingText: T("playlists.rules.saving"),
      onCancel: () => openRulesetManager({ trigger, fromMapping, mappingDraft, mappingDone }),
      onOpen: (ctx) => hydrateRulesetBuilder(ctx, seed, preset, readonly),
      onPrimary: async (ctx) => {
        if (readonly && ruleset && ruleset.built_in) {
          openRulesetForm({ mode: "clone", ruleset, trigger, fromMapping, mappingDraft, mappingDone });
          return;
        }
        const payload = readRulesetForm(ctx.modal, isEdit ? seed.id : "");
        const nameErr = nameFieldError(payload.name, T("playlists.rules.name"));
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
          <div class="pl-builder-title"><div><b>${TH("playlists.rules.basics")}</b><span>${TH("playlists.rules.basicsHint")}</span></div></div>
          <div class="pl-builder-grid">
            <div class="pl-field">
              <label for="pl-rs-name">${TH("playlists.rules.name")}</label>
              <input id="pl-rs-name" maxlength="${NAME_MAX}" value="${esc(current.name || "")}" placeholder="${clone ? T("playlists.presets.custom") : T("playlists.col.ruleset")}" aria-describedby="pl-rs-name-error" ${readonly ? "disabled" : ""}>
              <div class="pl-field-error" id="pl-rs-name-error"></div>
            </div>
            <div class="pl-field">
              <label for="pl-rs-direction">${TH("playlists.rules.supportedDirection")}</label>
              <select id="pl-rs-direction" data-rs-field="direction" ${readonly ? "disabled" : ""}>${selectOptions(ENUMS.direction.map(([value, label]) => ({ value, label })), current.direction)}</select>
            </div>
            <div class="pl-field">
              <label for="pl-rs-description">${TH("playlists.rules.description")}</label>
              <input id="pl-rs-description" value="${esc(current.description || "")}" placeholder="${TH("playlists.rules.descriptionPlaceholder")}" ${readonly ? "disabled" : ""}>
            </div>
            <div class="pl-field">
              <label for="pl-rs-preset">${TH("playlists.rules.preset")}</label>
              <select id="pl-rs-preset" ${readonly ? "disabled" : ""}>${selectOptions(Object.entries(RULESET_PRESETS).map(([value, item]) => ({ value, label: item.label })), preset)}</select>
            </div>
          </div>
          <div class="pl-preset-grid">${presetCards}</div>
          <div class="pl-help" id="pl-rs-preset-help">${esc(RULESET_PRESETS[preset].description)}</div>
        </section>

        <section class="pl-builder-section" data-builder-section="rules">
          <div class="pl-builder-title"><div><b>${TH("playlists.rules.rules")}</b><span>${TH("playlists.rules.rulesHint")}</span></div></div>
          <div class="pl-field" id="pl-rs-combine-wrap">
            <label for="pl-rs-condition-mode">${TH("playlists.rules.combine")}</label>
            <select id="pl-rs-condition-mode" ${readonly ? "disabled" : ""}><option value="all">${TH("playlists.rules.all")}</option><option value="any">${TH("playlists.rules.any")}</option></select>
          </div>
          <div id="pl-rs-condition-rows"></div>
          <div id="pl-rs-action-rows"></div>
          <div class="pl-inline-actions">
            <button type="button" class="pl-btn small" id="pl-rs-add-condition" ${readonly ? "disabled" : ""}>${TH("playlists.rules.addCondition")}</button>
            <button type="button" class="pl-btn small" id="pl-rs-add-action" ${readonly ? "disabled" : ""}>${TH("playlists.rules.addAction")}</button>
            <button type="button" class="pl-btn small" id="pl-rs-add-else" ${readonly ? "disabled" : ""}>${TH("playlists.rules.addElse")}</button>
          </div>
        </section>

        <section class="pl-builder-section" data-builder-section="policies">
          <div class="pl-builder-title"><div><b>${TH("playlists.rules.policies")}</b><span>${TH("playlists.rules.policiesHint")}</span></div></div>
          <div class="pl-builder-grid">
            ${builderSelectField("initial_sync", T("playlists.rules.initialSync"), T("playlists.rules.sourceIsAuth"), current, readonly)}
            ${builderSelectField("membership", T("playlists.rules.membership"), T("playlists.rules.membershipHint"), current, readonly)}
            ${builderSelectField("deduplicate", T("playlists.rules.dedup"), T("playlists.rules.dedupHint"), current, readonly)}
            ${builderSelectField("order", T("playlists.map.ordering"), T("playlists.rules.orderingHint"), current, readonly)}
            ${builderSelectField("allocation", T("playlists.rules.allocation"), T("playlists.rules.allocationHint"), current, readonly)}
          </div>
          <details class="pl-advanced">
            <summary>${TH("playlists.rules.advanced")}</summary>
            <div class="pl-advanced-body pl-builder-grid">
              ${builderSelectField("read_mode", T("playlists.rules.aggregateBehaviour"), T("playlists.rules.aggregateHint"), current, readonly)}
              ${builderSelectField("write_mode", T("playlists.rules.partitionBehaviour"), T("playlists.rules.partitionHint"), current, readonly)}
              ${builderSelectField("rebalance", T("playlists.rules.rebalancing"), T("playlists.rules.rebalancingHint"), current, readonly)}
              <label class="pl-check"><input type="checkbox" id="pl-rs-track_assignments" data-rs-field="track_assignments" ${current.track_assignments ? "checked" : ""} ${readonly ? "disabled" : ""}> ${TH("playlists.rules.trackAssignments")}</label>
            </div>
          </details>
        </section>

        <section class="pl-builder-section" data-builder-section="limits">
          <div class="pl-builder-title"><div><b>${TH("playlists.rules.limits")}</b><span>${TH("playlists.rules.limitsHint")}</span></div></div>
          <div class="pl-builder-grid">
            ${builderNumberField("per_endpoint_capacity", T("playlists.rules.perTarget"), current.per_endpoint_capacity, readonly, "pl-limit-partition")}
            ${builderNumberField("aggregate_capacity", T("playlists.rules.aggregateCapacity"), current.aggregate_capacity, readonly, "pl-limit-aggregate")}
            ${builderNumberField("maximum_targets", T("playlists.rules.maxLists"), current.maximum_targets, readonly, "pl-limit-partition")}
            ${builderSelectField("overflow", T("playlists.rules.overflow"), T("playlists.rules.overflowHint"), current, readonly, "pl-limit-capacity")}
          </div>
        </section>

        <section class="pl-builder-section" data-builder-section="summary">
          <div class="pl-builder-title"><div><b>${TH("playlists.rules.readable")}</b><span>${TH("playlists.rules.readableHint")}</span></div></div>
          <div class="pl-summary-box" id="pl-rs-summary"></div>
        </section>

        <section class="pl-builder-section" data-builder-section="preview">
          <div class="pl-builder-title"><div><b>${TH("playlists.rules.preview")}</b><span>${TH("playlists.rules.previewHint")}</span></div></div>
          <div class="pl-preview-box">
            <div class="pl-field">
              <label for="pl-rs-preview-items">${TH("playlists.rules.sourceItems")}</label>
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
    if (!readonly) bindNameValidation(ctx, "#pl-rs-name", T("playlists.rules.name"));
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
          <span class="pl-rule-word">${TH("playlists.rules.when")}</span>
          <select disabled><option>${TH("playlists.rules.sourceCount")}</option></select>
          <select disabled><option>${TH("playlists.rules.greater")}</option></select>
          <input type="number" min="1" value="${esc(rs.per_endpoint_capacity)}" data-rule-capacity ${readonly ? "disabled" : ""}>
          <button type="button" class="pl-btn small" data-remove-rule ${readonly ? "disabled" : ""}>${TH("playlists.rules.remove")}</button>
        </div>
      `;
      actions.innerHTML = `
        <div class="pl-rule-row then">
          <span class="pl-rule-word">${TH("playlists.rules.then")}</span>
          <select disabled><option>${TH("playlists.rules.splitInto")}</option></select>
          <input type="number" min="1" value="${esc(rs.per_endpoint_capacity)}" data-rule-action-capacity ${readonly ? "disabled" : ""}>
          <span class="pl-help">${TH("playlists.rules.perList")}</span>
          <button type="button" class="pl-btn small" data-remove-rule ${readonly ? "disabled" : ""}>${TH("playlists.rules.remove")}</button>
        </div>
        <div class="pl-rule-row then">
          <span class="pl-rule-word">${TH("playlists.rules.else")}</span>
          <select disabled><option>${TH("playlists.rules.syncDirectly")}</option></select>
          <span></span>
          <span class="pl-help">${TH("playlists.rules.oneEnough")}</span>
          <span></span>
        </div>
      `;
      $$("[data-rule-capacity],[data-rule-action-capacity]", root).forEach((input) => input.addEventListener("change", () => {
        $("#pl-rs-per_endpoint_capacity", root).value = input.value;
        updateRulesetBuilder(root);
      }));
    } else {
      conditions.innerHTML = `<div class="pl-empty"><strong>${TH("playlists.rules.noCondition")}</strong><span>${TH("playlists.rules.noConditionHint")}</span></div>`;
      actions.innerHTML = `
        <div class="pl-rule-row then">
          <span class="pl-rule-word">${TH("playlists.rules.then")}</span>
          <select disabled><option>${rs.read_mode === "aggregate" ? T("playlists.rules.mergeSources") : T("playlists.rules.syncDirectly")}</option></select>
          <span></span>
          <span class="pl-help">${rs.read_mode === "aggregate" ? T("playlists.rules.readCombined") : T("playlists.rules.writeOne")}</span>
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
    lines.push(T("playlists.summary.direction", {direction: ruleLabel(rs.direction).toLowerCase()}));
    if (rs.write_mode === "partition") {
      lines.push(T("playlists.summary.split", {capacity: String(rs.per_endpoint_capacity)}));
      lines.push(T("playlists.summary.maxTargets", {count: String(rs.maximum_targets), overflow: rs.overflow === "block" ? T("playlists.summary.blocked") : titleize(rs.overflow).toLowerCase()}));
    } else if (rs.read_mode === "aggregate") {
      lines.push(T("playlists.summary.aggregate", {capacity: String(rs.aggregate_capacity)}));
    } else {
      lines.push(T("playlists.summary.directSync"));
    }
    lines.push(T("playlists.summary.initial", {membership: titleize(rs.membership).toLowerCase()}));
    lines.push(rs.order === "preserve" ? T("playlists.summary.ordered") : T("playlists.summary.unordered"));
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
    const nameErr = nameFieldError(payload.name, T("playlists.rules.name"));
    if (nameErr) return nameErr;
    if (payload.write_mode === "partition") {
      if (payload.per_endpoint_capacity < 1) return T("playlists.rules.capacityMin");
      if (payload.maximum_targets < 2) return T("playlists.rules.splitMin");
      if (payload.overflow !== "block") return T("playlists.rules.blockOnly");
    }
    if (payload.read_mode === "aggregate" && payload.aggregate_capacity < 1) return T("playlists.rules.aggregateMin");
    return "";
  }

  function openRulesetDelete(ruleset, trigger, context) {
    const used = mappingsForRuleset(ruleset.id);
    const body = `
      <div class="pl-confirm-lines">
        <div><b>${TH("playlists.del.rulesetLabel")}</b> ${esc(ruleset.name)}</div>
        <div><b>${TH("playlists.del.mappingsLabel")}</b> ${esc(used.length)}</div>
        ${used.length ? `<div class="pl-warning">Deletion is blocked while mappings reference this ruleset: ${esc(used.map((m) => m.name || m.id).join(", "))}. Change those mappings first.</div>` : `<div>${TH("playlists.del.rulesetNote")}</div>`}
      </div>
    `;
    openModal({
      title: T("playlists.rules.delete"),
      description: T("playlists.del.rulesetConfirm"),
      body,
      trigger,
      primaryText: T("playlists.rules.delete"),
      savingText: T("playlists.deleting"),
      onCancel: () => openRulesetManager({ trigger: context.trigger, fromMapping: context.fromMapping, mappingDraft: context.mappingDraft, mappingDone: context.mappingDone }),
      onPrimary: async () => {
        if (used.length) throw new Error(T("playlists.del.rulesetInUse"));
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
        <div><b>${TH("playlists.activity.runs")}</b> ${esc(runCount)}</div>
        <div>${TH("playlists.activity.clearNote")}</div>
      </div>
    `;
    openModal({
      title: T("playlists.activity.clearLabel"),
      description: T("playlists.activity.clearHint"),
      body,
      trigger,
      primaryText: T("playlists.activity.clear"),
      savingText: T("playlists.activity.clearing"),
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
      <div class="pl-table-wrap cw-page-table">
        <table>
          <thead><tr><th>${TH("playlists.col.time")}</th><th>${TH("playlists.col.type")}</th><th>${TH("playlists.col.mapping")}</th><th>${TH("playlists.col.details")}</th><th>${TH("playlists.col.status")}</th></tr></thead>
          <tbody>${state.activity.map((row) => `<tr><td>${esc(compactTime(row.ts))}</td><td>${esc(row.type || "-")}</td><td>${esc(row.label || "-")}</td><td>${esc(row.details || "-")}</td><td><span class="pl-pill ${row.status === "error" ? "err" : "ok"}">${esc(titleize(row.status || "completed"))}</span></td></tr>`).join("")}</tbody>
        </table>
      </div>
    ` : `<div class="pl-empty"><strong>${TH("playlists.activity.none")}</strong><span>${TH("playlists.activity.noneHint")}</span></div>`;
    openModal({ title: T("playlists.activity.full"), description: T("playlists.activity.fullHint"), body, trigger, width: "980px" });
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
    state.published = await API.published().catch(() => ({ lists: [], formats: [] }));
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
    state.loaded = true;
  }

  function updateMappingActions(root) {
    const disabled = !state.loaded || state.endpoints.length < 2;
    $$("[data-action='mapping-new']", root).forEach((btn) => {
      btn.disabled = disabled;
      btn.title = !state.loaded ? T("playlists.stillLoading") : disabled ? T("playlists.needTwoEndpoints") : T("playlists.createMapping");
    });
  }

  function refreshSection(root, key) {
    const targets = {
      endpoints: ["#pl-playlist-endpoints .pl-section-body", renderEndpoints],
      mappings: ["#pl-mappings-overview .pl-section-body", renderMappings],
      activity: ["#pl-activity-overview .pl-section-body", renderActivity],
      published: ["#pl-published-lists .pl-section-body", renderPublished],
    };
    const spec = targets[key];
    if (!spec) return;
    const el = $(spec[0], root);
    if (el) {
      el.innerHTML = spec[1]();
      labelMobileTables(el);
    }
  }

  async function refreshOverview(sections = ["endpoints", "mappings", "activity"]) {
    const root = $("#page-playlists");
    const scrollX = window.scrollX;
    const scrollY = window.scrollY;
    try {
      await reloadData();
    } catch (err) {
      state.error = err && err.message ? err.message : T("playlists.loadFailed");
    }
    if (!root) return;
    if (!root.querySelector(".pl-page")) {
      render(root);
      return;
    }
    const banners = $(".pl-banners", root);
    if (banners) banners.outerHTML = renderBanners();
    updateMappingActions(root);
    [...sections, "published"].forEach((key) => refreshSection(root, key));
    refreshHero(root);
    scheduleSyncSummaryPoll(syncSummaryRunning() ? 1500 : 6000);
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
      state.error = err && err.message ? err.message : String(err || T("playlists.loadFailedInitial"));
    } finally {
      state.loading = false;
      render(root);
      scheduleSyncSummaryPoll(syncSummaryRunning() ? 1500 : 6000);
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
      openNotice(T("playlists.run.noMapping"), T("playlists.run.noMappingHint"), trigger);
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
