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
  const SAFE_NAME_CHARS = " _.'-&()";
  const PLAYLIST_COMPATIBLE_PROVIDERS = new Set(["PLEX", "TRAKT", "MDBLIST", "JELLYFIN", "EMBY", "PUBLICMETADB", "SIMKL"]);
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
    overview: () => request(`${BASE}/overview`),
    activity: () => request(`${BASE}/activity`),
    endpoints: () => request(`${BASE}/endpoints`),
    epUpsert: (body) => request(`${BASE}/endpoints`, { method: "POST", body: JSON.stringify(body) }),
    epDelete: (id) => request(`${BASE}/endpoints/${encodeURIComponent(id)}`, { method: "DELETE" }),
    epSync: (id) => request(`${BASE}/endpoints/${encodeURIComponent(id)}/sync`, { method: "POST" }),
    mappings: () => request(`${BASE}/mappings`),
    mapUpsert: (body) => request(`${BASE}/mappings`, { method: "POST", body: JSON.stringify(body) }),
    mapDelete: (id) => request(`${BASE}/mappings/${encodeURIComponent(id)}`, { method: "DELETE" }),
    run: (id) => request(`${BASE}/mappings/${encodeURIComponent(id)}/run`, { method: "POST" }),
    runPair: (id) => request("/api/run", { method: "POST", body: JSON.stringify({ pair_id: id }) }),
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


  const state = {
    providers: [],
    endpoints: [],
    mappings: [],
    rulesets: [],
    overview: {},
    activity: [],
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

  function actionButton(action, id, label, iconName, tone, extraClass) {
    return `<button class="pl-action-btn ${esc(tone || "")} ${esc(extraClass || "")}" data-action="${esc(action)}" data-id="${esc(id || "")}" title="${esc(label)}" aria-label="${esc(label)}"><span class="material-symbols-rounded" aria-hidden="true">${esc(iconName)}</span></button>`;
  }

  function endpointById(id) {
    return state.endpoints.find((ep) => ep.id === id) || null;
  }

  function rulesetById(id) {
    return state.rulesets.find((rs) => rs.id === id) || null;
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
        <span>Playlists need at least one compatible provider (Plex, Trakt, MDBList, Jellyfin, Emby, PublicMetaDB or SIMKL). Configure one in Connections to enable playlist endpoints and mappings.</span>
        <button type="button" class="pl-btn small" data-action="open-connections">Open Connections</button>
      </div>
    ` : "";
    return `
      <div class="pl-banners">
        ${errorGate}
        ${providerGate}
        <div class="pl-banner info">
          <span class="material-symbols-rounded" aria-hidden="true">info</span>
          <span>Playlists are highly experimental and cause issues. At this stage, the feature is intended for testing only. Do NOT use in production. You have been warned</span>
        </div>
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

  function statusForResult(result) {
    if (!result) return `<span class="pl-pill off">No runs</span>`;
    if (result.ok === false || result.capacity_error) return `<span class="pl-pill err">Failed</span>`;
    const warnings = Array.isArray(result.warnings) ? result.warnings.length : 0;
    if (warnings) return `<span class="pl-pill warn">Warning</span>`;
    return `<span class="pl-pill ok">Success</span>`;
  }

  function endpointStatus(endpoint) {
    if (!endpoint.playlist_id) return `<span class="pl-pill warn">Incomplete</span>`;
    return `<span class="pl-pill ok">Connected</span>`;
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
              <div><div class="pl-section-title">Playlist endpoints</div><div class="pl-section-sub">Connected provider playlists available for mappings.</div></div>
              <button class="pl-btn small" data-action="endpoint-new">+ Add endpoint</button>
            </div>
            <div class="pl-section-body">${renderEndpoints()}</div>
          </section>
          <section class="pl-section" id="pl-mappings-overview">
            <div class="pl-section-head">
              <div><div class="pl-section-title">Mappings</div><div class="pl-section-sub">Sync relationships between playlist endpoints.</div></div>
              <button class="pl-btn small" data-action="mapping-new" ${mappingDisabled ? "disabled" : ""} title="${esc(mappingTitle)}">+ New mapping</button>
            </div>
            <div class="pl-section-body">${renderMappings()}</div>
          </section>
          <section class="pl-section" id="pl-activity-overview">
            <div class="pl-section-head">
              <div><div class="pl-section-title">Activity overview</div><div class="pl-section-sub">Recent playlist sync and validation activity.</div></div>
              <button class="pl-btn small" data-action="activity-all">View all activity</button>
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
      return `<div class="pl-empty"><strong>No playlist endpoints yet</strong><span>Add the first provider playlist before creating mappings.</span><button class="pl-btn primary small" data-action="endpoint-new">+ New endpoint</button></div>`;
    }
    const rows = state.endpoints.map((ep) => {
      const playlistType = ep.playlist_type || ep.endpoint_type || ep.kind || ep.media_type || "playlist";
      const usedBy = state.mappings.filter((m) => mappingUsesEndpoint(m, ep.id)).length;
      return `
        <tr>
          <td><div class="pl-main-text">${esc(ep.name || ep.playlist_name || ep.id)}</div><div class="pl-muted">${esc(ep.id)}</div></td>
          <td><div class="pl-provider">${icon(ep.provider)}<div><div class="pl-main-text">${esc(ep.provider_label || providerLabel(ep.provider))}</div><div class="pl-muted">${esc(ep.provider || "")}</div></div></div></td>
          <td>${esc(ep.instance || "default")}</td>
          <td><div class="pl-main-text">${esc(ep.playlist_name || ep.playlist_id || "-")}</div></td>
          <td><span class="pl-pill">${esc(titleize(playlistType))}</span></td>
          <td>${endpointStatus(ep)}</td>
          <td><div class="pl-muted">${ep.last_synced ? `Refreshed ${esc(timeText(ep.last_synced))}` : "Not refreshed yet"}${ep.item_count != null ? `<br>${esc(ep.item_count)} items` : ""}</div></td>
          <td>
            <div class="pl-actions">
              ${actionButton("endpoint-edit", ep.id, "Edit endpoint", "edit", "edit")}
              ${actionButton("endpoint-delete", ep.id, usedBy ? `Delete endpoint. ${usedBy} mapping(s) use this endpoint` : "Delete endpoint", "delete", "delete")}
            </div>
          </td>
        </tr>
      `;
    }).join("");
    return `
      <div class="pl-table-wrap">
        <table>
          <thead><tr><th>Endpoint name</th><th>Provider</th><th>Profile</th><th>Selected playlist</th><th>Type</th><th>Status</th><th>Last refresh</th><th aria-label="Actions"></th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderMappings() {
    if (state.loading && !state.loaded) return renderSkeleton("Loading playlist mappings");
    if (!state.mappings.length) {
      const need = state.endpoints.length < 2;
      return `<div class="pl-empty"><strong>No mappings yet</strong><span>${need ? "At least two endpoints are required before a mapping can be created." : "Create a mapping to sync playlists between endpoints."}</span><button class="pl-btn primary small" data-action="mapping-new" ${need ? "disabled" : ""} title="${need ? "Create at least two endpoints first." : "Create playlist mapping"}">+ New mapping</button></div>`;
    }
    const rows = state.mappings.map((m) => {
      const src = endpointById(m.source_endpoint) || m.source || {};
      const rule = m.ruleset || rulesetById(m.ruleset_id || "");
      const res = m.last_result || null;
      return `
        <tr>
          <td><div class="pl-main-text">${esc(m.name || m.id)}</div><div class="pl-muted">${esc(m.id)}</div></td>
          <td>${endpointRef(src, m.source_endpoint)}</td>
          <td><span class="pl-pill">${esc(directionFor(m))}</span></td>
          <td>${mappingTargetRefs(m)}</td>
          <td><span class="pl-pill ${rule && rule.direction === "bidirectional" ? "warn" : ""}">${esc(rule ? ruleLabel(rule.direction) : "One way")}</span></td>
          <td><span class="pl-pill ${m.assigned_pair ? "ok" : "warn"}">${esc(m.assigned_pair || "No pair")}</span></td>
          <td><span class="pl-pill">${esc(rule ? rule.name : "Direct")}</span></td>
          <td><span class="pl-pill ${m.enabled ? "ok" : "off"}">${m.enabled ? "Enabled" : "Disabled"}</span></td>
          <td>${statusForResult(res)}</td>
          <td><div class="pl-muted">${compactTime(res && res.finished_at)}</div></td>
          <td>
            <div class="pl-actions">
              ${actionButton("mapping-sync", m.id, "Sync now", "sync", "sync")}
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
        <table>
          <thead><tr><th>Mapping name</th><th>Source endpoint</th><th>Direction</th><th>Destination endpoint</th><th>Mode</th><th>Sync pair</th><th>Ruleset</th><th>Status</th><th>Last result</th><th>Last sync</th><th aria-label="Actions"></th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function renderActivity() {
    if (state.loading && !state.loaded) return renderSkeleton("Loading playlist activity");
    const entries = state.activity || [];
    const stats = activityStats(entries);
    const latest = entries.slice(0, 8);
    const table = latest.length ? `
      <div class="pl-table-wrap">
        <table>
          <thead><tr><th>Time</th><th>Mapping</th><th>Direction</th><th>Result</th><th>Added</th><th>Updated</th><th>Removed</th><th>Skipped</th><th>Status</th></tr></thead>
          <tbody>${latest.map((row) => {
            const counts = parseActivityCounts(row);
            return `<tr><td>${esc(compactTime(row.ts))}</td><td>${esc(row.label || "-")}</td><td>${esc(row.type || "-")}</td><td>${esc(row.details || "-")}</td><td>${counts.added}</td><td>${counts.updated}</td><td>${counts.removed}</td><td>${counts.skipped}</td><td><span class="pl-pill ${row.status === "error" ? "err" : "ok"}">${esc(titleize(row.status || "completed"))}</span></td></tr>`;
          }).join("")}</tbody>
        </table>
      </div>
    ` : `<div class="pl-empty"><strong>No playlist activity yet</strong><span>Runs and endpoint refreshes will appear here.</span></div>`;
    return `
      <div class="pl-activity">
        <div class="pl-stats">
          <div class="pl-stat"><b>${stats.total}</b><span>Total runs</span></div>
          <div class="pl-stat"><b>${stats.success}</b><span>Successful</span></div>
          <div class="pl-stat"><b>${stats.warning}</b><span>Warnings</span></div>
          <div class="pl-stat"><b>${stats.failed}</b><span>Failed</span></div>
          <div class="pl-stat"><b>${stats.running}</b><span>Queued or running</span></div>
          <div class="pl-stat"><b>${stats.skipped}</b><span>Skipped</span></div>
        </div>
        ${table}
      </div>
    `;
  }

  function activityStats(rows) {
    const stats = { total: rows.length, success: 0, warning: 0, failed: 0, running: 0, skipped: 0 };
    rows.forEach((row) => {
      const status = String(row.status || "").toLowerCase();
      if (status === "error" || status === "failed") stats.failed += 1;
      else if (status === "queued" || status === "running") stats.running += 1;
      else if (status === "skipped") stats.skipped += 1;
      else if (String(row.details || "").toLowerCase().includes("warning")) stats.warning += 1;
      else stats.success += 1;
    });
    return stats;
  }

  function parseActivityCounts(row) {
    const details = String(row.details || "");
    const add = details.match(/\+(\d+)/);
    const rem = details.match(/-(\d+)/);
    return { added: add ? Number(add[1]) : 0, updated: 0, removed: rem ? Number(rem[1]) : 0, skipped: 0 };
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
    if (action === "mapping-new") openMappingModal({ trigger: btn });
    if (action === "mapping-edit") openMappingModal({ mapping: state.mappings.find((m) => m.id === id), trigger: btn });
    if (action === "mapping-toggle") toggleMapping(state.mappings.find((m) => m.id === id), btn);
    if (action === "mapping-delete") openMappingDelete(state.mappings.find((m) => m.id === id), btn);
    if (action === "mapping-sync") syncMapping(state.mappings.find((m) => m.id === id), btn);
    if (action === "activity-all") openActivityModal(btn);
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
      if (e.target === modal || e.target.closest("[data-modal-close]") || e.target.closest("[data-modal-cancel]")) closeModal(false);
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

  function openEndpointModal({ endpoint = null, clone = false, trigger = null } = {}) {
    const isEdit = !!endpoint && !clone;
    const seed = endpoint || {};
    const providers = configuredProviders().map((p) => ({ value: p.provider, label: p.label || p.provider }));
    const provider = seed.provider || (providers[0] && providers[0].value) || "";
    const instances = instancesFor(provider);
    const instance = seed.instance || instances[0] || "default";
    const title = isEdit ? "Edit playlist endpoint" : "Create playlist endpoint";
    const description = isEdit ? "Update the provider playlist used by this endpoint." : "Connect one or more provider playlists as reusable endpoints.";
    const body = `
      <div class="pl-form">
        <div class="pl-field">
          <label for="pl-ep-name">Endpoint name</label>
          <input id="pl-ep-name" maxlength="${NAME_MAX}" value="${esc(clone ? shortName(`${seed.name || seed.playlist_name || "Endpoint"} copy`) : seed.name || "")}" placeholder="Endpoint name" aria-describedby="pl-ep-name-error">
          <div class="pl-field-error" id="pl-ep-name-error"></div>
        </div>
        <div class="pl-field">
          <label for="pl-ep-provider">Provider</label>
          <select id="pl-ep-provider">${selectOptions(providers, provider, providers.length ? "" : "No providers configured")}</select>
        </div>
        <div class="pl-field">
          <label for="pl-ep-instance">Provider profile</label>
          <select id="pl-ep-instance">${selectOptions(instances.map((x) => ({ value: x, label: x })), instance)}</select>
        </div>
        <label class="pl-check full"><input type="checkbox" id="pl-ep-create"> Create a new list instead</label>
        <div class="pl-warning full hidden" id="pl-ep-simkl-warning">${esc(SIMKL_PLAYLIST_WARNING)}</div>
        <div class="pl-field full" id="pl-ep-existing-wrap">
          <label for="pl-ep-playlist">Existing playlist selector</label>
          <select id="pl-ep-playlist" ${isEdit ? "" : "multiple"} size="${isEdit ? "1" : "7"}"><option value="">Loading...</option></select>
          <div class="pl-help" id="pl-ep-playlist-help">${isEdit ? "Select one provider playlist." : "Select one or more provider playlists to create endpoints."}</div>
        </div>
        <div class="pl-field full" id="pl-ep-create-wrap" style="display:none">
          <label for="pl-ep-create-name">New playlist name</label>
          <input id="pl-ep-create-name" maxlength="${PLAYLIST_NAME_MAX}" value="${esc(seed.playlist_name || seed.name || "")}" placeholder="New playlist name" aria-describedby="pl-ep-create-name-error">
          <div class="pl-field-error" id="pl-ep-create-name-error"></div>
        </div>
      </div>
    `;
    openModal({
      title,
      description,
      body,
      trigger,
      primaryText: isEdit ? "Save endpoint" : "Create endpoint",
      savingText: "Saving endpoint...",
      onOpen: (ctx) => hydrateEndpointModal(ctx, seed, isEdit, provider, instance),
      onPrimary: async (ctx) => saveEndpointFromModal(ctx, seed, isEdit),
    });
  }

  function hydrateEndpointModal(ctx, seed, isEdit, provider, instance) {
    const root = ctx.modal;
    bindNameValidation(ctx, "#pl-ep-name", "Endpoint name");
    const providerSelect = $("#pl-ep-provider", root);
    const instanceSelect = $("#pl-ep-instance", root);
    const createCheck = $("#pl-ep-create", root);
    const createName = $("#pl-ep-create-name", root);
    const simklWarning = $("#pl-ep-simkl-warning", root);
    const validateCreateName = () => {
      const err = createCheck.checked ? playlistNameError(createName.value) : "";
      applyFieldError(createName, err, `${String(createName.value || "").trim().length}/${PLAYLIST_NAME_MAX} characters`);
      return err;
    };
    addModalValidator(ctx, validateCreateName);
    const updateInstances = () => {
      const list = instancesFor(providerSelect.value);
      instanceSelect.innerHTML = selectOptions(list.map((x) => ({ value: x, label: x })), list.includes(instanceSelect.value) ? instanceSelect.value : (list[0] || "default"));
    };
    const toggleCreate = () => {
      $("#pl-ep-existing-wrap", root).style.display = createCheck.checked ? "none" : "";
      $("#pl-ep-create-wrap", root).style.display = createCheck.checked ? "" : "none";
      syncModalPrimary(ctx);
    };
    const updateProviderRestrictions = () => {
      const isSimkl = String(providerSelect.value || "").toUpperCase() === "SIMKL";
      if (simklWarning) simklWarning.classList.toggle("hidden", !isSimkl);
      createCheck.disabled = isSimkl;
      if (isSimkl) createCheck.checked = false;
      toggleCreate();
    };
    providerSelect.addEventListener("change", () => {
      updateInstances();
      updateProviderRestrictions();
      loadEndpointResources(root, "", isEdit);
    });
    instanceSelect.addEventListener("change", () => loadEndpointResources(root, "", isEdit));
    createCheck.addEventListener("change", toggleCreate);
    createName.addEventListener("input", () => syncModalPrimary(ctx));
    createName.addEventListener("change", () => syncModalPrimary(ctx));
    updateInstances();
    updateProviderRestrictions();
    loadEndpointResources(root, seed.playlist_id || "", isEdit);
  }

  async function loadEndpointResources(root, selected, isEdit) {
    const select = $("#pl-ep-playlist", root);
    const help = $("#pl-ep-playlist-help", root);
    select.innerHTML = `<option value="">Loading...</option>`;
    try {
      const data = await API.resources(val("#pl-ep-provider", root), val("#pl-ep-instance", root));
      const resources = data.resources || [];
      if (!resources.length) {
        select.innerHTML = `<option value="">No playlists found</option>`;
        help.textContent = "No readable provider playlists were returned for this profile.";
        return;
      }
      select.innerHTML = resources.map((r) => {
        const caps = [];
        const endpointType = (r.extra && r.extra.endpoint_type) || r.endpoint_type || r.playlist_type || r.kind || "";
        if (endpointType) caps.push(endpointType);
        if (r.media_types && r.media_types.length) caps.push(r.media_types.join("/"));
        if (r.smart) caps.push("smart");
        return `<option value="${esc(r.id)}" data-name="${esc(r.name || r.id)}" data-type="${esc(endpointType || "playlist")}" data-media-types="${esc((r.media_types || []).join(","))}" data-kind="${esc(r.kind || "regular")}" ${r.id === selected ? "selected" : ""}>${esc(r.name || r.id)}${caps.length ? ` (${esc(caps.join("/"))})` : ""}</option>`;
      }).join("");
      help.textContent = isEdit ? "Select one provider playlist." : "Select one or more provider playlists to create endpoints.";
    } catch (err) {
      select.innerHTML = `<option value="">Could not load playlists</option>`;
      help.textContent = err && err.message ? err.message : "Provider playlist loading failed.";
    }
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
      await API.epUpsert({ id: isEdit ? seed.id : "", name: name || createName, provider, instance, create: true, create_name: createName });
    } else {
      const ids = selectedValues("#pl-ep-playlist", root);
      if (!ids.length) throw new Error("Select at least one playlist.");
      if (isEdit && ids.length !== 1) throw new Error("Edit mode can only save one selected playlist.");
      for (const playlistId of ids) {
        const opt = Array.from($("#pl-ep-playlist", root).options || []).find((o) => o.value === playlistId);
        const playlistName = opt ? opt.dataset.name || opt.textContent : playlistId;
        const playlistType = opt ? opt.dataset.type || "" : "";
        const mediaTypes = opt && opt.dataset.mediaTypes ? opt.dataset.mediaTypes.split(",").filter(Boolean) : [];
        await API.epUpsert({ id: isEdit ? seed.id : "", name: name || playlistName, provider, instance, playlist_id: playlistId, playlist_name: playlistName, playlist_type: playlistType, media_types: mediaTypes });
      }
    }
    closeModal(true);
    await refreshOverview();
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
    const endpointOpts = state.endpoints.map((ep) => ({ value: ep.id, label: `${ep.name || ep.id} (${ep.provider})` }));
    const rulesetOpts = [{ value: "", label: "Direct one way" }].concat(state.rulesets.map((rs) => ({ value: rs.id, label: `${rs.name}${rs.built_in ? " (built in)" : ""}` })));
    const body = `
      <div class="pl-form">
        <div class="pl-field full">
          <label for="pl-map-name">Mapping name</label>
          <input id="pl-map-name" maxlength="${NAME_MAX}" value="${esc(clone ? "" : seed.name || "")}" placeholder="Mapping name" aria-describedby="pl-map-name-error">
          <div class="pl-field-error" id="pl-map-name-error"></div>
        </div>
        <div class="pl-field">
          <label for="pl-map-source">Source endpoint</label>
          <select id="pl-map-source">${selectOptions(endpointOpts, source)}</select>
        </div>
        <div class="pl-field">
          <label for="pl-map-targets">Destination endpoint</label>
          <select id="pl-map-targets">${selectOptions(endpointOpts, target)}</select>
        </div>
        <div class="pl-field">
          <label for="pl-map-direction">Direction</label>
          <select id="pl-map-direction">${selectOptions(ENUMS.direction.map(([value, label]) => ({ value, label })), (rulesetById(seed.ruleset_id || "") || {}).direction || "one_way")}</select>
        </div>
        <div class="pl-field">
          <label for="pl-map-ruleset">Ruleset</label>
          <select id="pl-map-ruleset">${selectOptions(rulesetOpts, seed.ruleset_id || "")}</select>
        </div>
        <div class="pl-field">
          <label for="pl-map-membership">Sync mode</label>
          <select id="pl-map-membership">${selectOptions(ENUMS.membership.map(([value, label]) => ({ value, label })), seed.membership || "managed_only")}</select>
        </div>
        <div class="pl-field">
          <label for="pl-map-order">Ordering</label>
          <select id="pl-map-order">${selectOptions(ENUMS.order.map(([value, label]) => ({ value, label })), seed.order || "ignore")}</select>
        </div>
        <label class="pl-check"><input type="checkbox" id="pl-map-enabled" ${seed.enabled === false ? "" : "checked"}> Enabled</label>
        <div class="pl-warning full hidden" id="pl-map-simkl-warning">${esc(SIMKL_PLAYLIST_WARNING)}</div>
        <div class="pl-field full">
          <button type="button" class="pl-btn small" id="pl-map-manage-rulesets">Manage rulesets</button>
          <div class="pl-help" id="pl-map-rule-help"></div>
        </div>
      </div>
    `;
    openModal({
      title,
      description: isEdit ? "Update the endpoints, ruleset and sync behavior for this mapping." : "Connect a source playlist endpoint to one or more destination endpoints.",
      body,
      trigger,
      primaryText: isEdit ? "Save mapping" : "Create mapping",
      savingText: "Saving mapping...",
      onCancel: () => { if (typeof onDone === "function") onDone("cancel"); },
      onOpen: (ctx) => hydrateMappingModal(ctx),
      onPrimary: async (ctx) => {
        await saveMappingFromModal(ctx, isEdit ? mapping.id : "");
        if (typeof onDone === "function") onDone("save");
      },
    });
  }

  function hydrateMappingModal(ctx) {
    const root = ctx.modal;
    bindNameValidation(ctx, "#pl-map-name", "Mapping name");
    const refresh = () => {
      const source = val("#pl-map-source", root);
      $$("#pl-map-targets option", root).forEach((opt) => { opt.disabled = opt.value === source; });
      const targetSelect = $("#pl-map-targets", root);
      if (targetSelect.value === source) {
        const next = Array.from(targetSelect.options || []).find((opt) => !opt.disabled);
        targetSelect.value = next ? next.value : "";
      }
      const rule = rulesetById(val("#pl-map-ruleset", root));
      const direction = val("#pl-map-direction", root);
      const help = $("#pl-map-rule-help", root);
      const sourceEp = endpointById(source);
      const targetEp = endpointById(targetSelect.value);
      const simklWarning = $("#pl-map-simkl-warning", root);
      const usesSimkl = [sourceEp, targetEp].some((ep) => String((ep && ep.provider) || "").toUpperCase() === "SIMKL");
      if (simklWarning) simklWarning.classList.toggle("hidden", !usesSimkl);
      if (!rule) help.textContent = "Direct mappings run one way and require exactly one destination endpoint.";
      else help.textContent = `${rule.name} supports ${ruleLabel(rule.direction)} mappings, ${rule.write_mode} writes, and up to ${rule.maximum_targets} destination endpoint(s).`;
      if (rule && rule.direction !== direction) help.textContent += " Change the direction or choose another ruleset before saving.";
    };
    $("#pl-map-source", root).addEventListener("change", refresh);
    $("#pl-map-targets", root).addEventListener("change", refresh);
    $("#pl-map-ruleset", root).addEventListener("change", refresh);
    $("#pl-map-direction", root).addEventListener("change", refresh);
    $("#pl-map-manage-rulesets", root).addEventListener("click", (e) => {
      const draft = readMappingDraft(root);
      openRulesetManager({ trigger: e.currentTarget, fromMapping: true, mappingDraft: draft, mappingDone: ctx.opts.onDone });
    });
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
    const direction = val("#pl-map-direction", root);
    const rule = rulesetById(draft.ruleset_id);
    const nameErr = nameFieldError(draft.name, "Mapping name");
    if (nameErr) throw new Error(nameErr);
    if (!draft.source_endpoint || !draft.target_endpoints.length) throw new Error("Source and destination endpoints are required.");
    if (draft.target_endpoints.includes(draft.source_endpoint)) throw new Error("Source and destination endpoints must be different.");
    if (!draft.ruleset_id && draft.target_endpoints.length !== 1) throw new Error("Direct mappings require exactly one destination endpoint.");
    if (rule && rule.direction !== direction) throw new Error(`The selected ruleset supports ${ruleLabel(rule.direction)}, not ${ruleLabel(direction)}.`);
    if (rule && draft.target_endpoints.length > Number(rule.maximum_targets || 1)) throw new Error("Too many destination endpoints for the selected ruleset.");
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
    if (!mapping.assigned_pair) {
      openNotice("Sync pair missing", "Save the mapping once so CrossWatch can create its playlist sync pair.", btn);
      return;
    }
    btn.disabled = true;
    btn.textContent = "Syncing...";
    try {
      await API.runPair(mapping.assigned_pair);
      await refreshOverview(["mappings", "activity"]);
    } catch (err) {
      openNotice("Sync failed", err && err.message ? err.message : "Could not run this mapping.", btn);
    } finally {
      btn.disabled = false;
      btn.textContent = "Sync now";
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
              <button class="pl-btn small" data-ruleset-action="view" data-id="${esc(rs.id)}">View</button>
              <button class="pl-btn small" data-ruleset-action="clone" data-id="${esc(rs.id)}">Clone</button>
              <button class="pl-btn small" data-ruleset-action="edit" data-id="${esc(rs.id)}" ${rs.built_in ? "disabled title='Built in rulesets cannot be edited.'" : ""}>Edit</button>
              <button class="pl-btn small danger" data-ruleset-action="delete" data-id="${esc(rs.id)}" ${rs.built_in ? "disabled title='Built in rulesets cannot be deleted.'" : ""}>Delete</button>
            </div>
          </td>
        </tr>
      `;
    }).join("");
    const body = `
      <div class="pl-table-wrap">
        <table>
          <thead><tr><th>Ruleset name</th><th>Type</th><th>Direction support</th><th>Strategy</th><th>Capacity behaviour</th><th>Mappings using it</th><th>Actions</th></tr></thead>
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
        <div><b>Mappings using it:</b> ${esc(used.length)}</div>
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

  async function reloadData() {
    state.error = "";
    const [providers, endpoints, mappings, rulesets, overview, activity] = await Promise.all([
      API.providers(),
      API.endpoints(),
      API.mappings(),
      API.rulesets(),
      API.overview(),
      API.activity(),
    ]);
    state.providers = providers.providers || [];
    state.endpoints = endpoints.endpoints || [];
    state.mappings = mappings.mappings || [];
    state.rulesets = rulesets.rulesets || [];
    state.overview = overview || {};
    state.activity = activity.activity || [];
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
