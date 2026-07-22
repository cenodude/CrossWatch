/* assets/js/scrobbler.js */
/* Scrobbler configuration UI and logic. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const label = (v) => ({ plex: "Plex", jellyfin: "Jellyfin", emby: "Emby", trakt: "Trakt", simkl: "SIMKL", mdblist: "MDBList" }[String(v || "").toLowerCase()] || String(v || "").toUpperCase());
  const logo = (v) => ({ plex: "/assets/img/PLEX.svg", jellyfin: "/assets/img/JELLYFIN.svg", emby: "/assets/img/EMBY.svg", trakt: "/assets/img/TRAKT.svg", simkl: "/assets/img/SIMKL.svg", mdblist: "/assets/img/MDBLIST.svg" }[String(v || "").toLowerCase()] || "");
  const state = { root: null, overview: null, busy: false, panels: { watcherDefaults: false, ratings: false, webhookDefaults: false }, delBtn: null, delTimer: 0, regenBtn: null, regenTimer: 0, legacyConfirm: false, legacyTimer: 0 };

  async function j(url, options = {}) {
    if (w.cwIsAuthSetupPending?.() === true) throw new Error("auth setup pending");
    const res = await fetch(url, { cache: "no-store", credentials: "same-origin", ...options });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) {
      const err = new Error(data?.message || data?.error || `HTTP ${res.status}`);
      err.payload = data;
      throw err;
    }
    return data;
  }

  function chip(text, kind = "") {
    return `<span class="sc2-chip ${kind ? `sc2-chip-${esc(kind)}` : ""}">${esc(text)}</span>`;
  }

  function flashCopied(btn) {
    if (!btn) return;
    if (btn.__cwCopyTimer) clearTimeout(btn.__cwCopyTimer);
    if (!btn.dataset.copyIcon) btn.dataset.copyIcon = (btn.textContent || "content_copy").trim();
    btn.textContent = "check";
    btn.classList.add("is-copied");
    btn.__cwCopyTimer = setTimeout(() => {
      btn.textContent = btn.dataset.copyIcon || "content_copy";
      btn.classList.remove("is-copied");
      delete btn.dataset.copyIcon;
      btn.__cwCopyTimer = 0;
    }, 1400);
  }

  function providerLogo(provider) {
    const src = logo(provider);
    return `<span class="sc2-logo brand-${esc(provider)}">${src ? `<img src="${esc(src)}" alt="">` : `<span>${esc(label(provider).slice(0, 1))}</span>`}</span>`;
  }

  function fieldHelp(text) {
    const tip = esc(text);
    return `<span class="sc2-field-help material-symbols-rounded" tabindex="0" role="img" aria-label="Help: ${tip}" title="${tip}">help</span>`;
  }

  function webhookState(x) {
    if (x.enabled && x.source_configured) return ["Connected", "ok"];
    if (x.enabled) return ["Needs setup", "warn"];
    return ["Disabled", "off"];
  }

  function webhookProfileName(x) {
    return x.provider_instance === "default" ? "Default" : x.provider_instance;
  }

  function instanceName(v) {
    return String(v || "default") === "default" ? "Default" : String(v || "default");
  }

  function routeFilterSummary(filters = {}) {
    const parts = [];
    const users = Array.isArray(filters.username_whitelist) ? filters.username_whitelist : [];
    const allow = Array.isArray(filters.server_uuid_whitelist) ? filters.server_uuid_whitelist : [];
    const block = Array.isArray(filters.server_uuid_blacklist) ? filters.server_uuid_blacklist : [];
    if (users.length) parts.push(`${users.length} user${users.length === 1 ? "" : "s"}`);
    if (allow.length || filters.server_uuid) parts.push(`${allow.length || 1} UUID allow`);
    if (block.length) parts.push(`${block.length} UUID block`);
    if (filters.ignore_live_tv_dvr) parts.push("Live TV ignored");
    return parts.length ? parts.join(" - ") : "No filters";
  }

  function renderHeader(o) {
    return `
      <div class="cw-settings-pane-head sc2-pane-head">
        <div>
          <div class="cw-settings-pane-kicker">Scrobbler</div>
          <h3>Webhooks and Watcher</h3>
          <p>Receive real time scrobbles via Watcher routes or webhooks. <b>Watcher is recommended</b></p>
        </div>
        <div class="cw-settings-pane-head-actions">
          <div class="cw-settings-jumpbar" aria-label="Scrobbler actions">
            <button type="button" class="cw-settings-jump" data-action="add-webhook"><span class="material-symbols-rounded" aria-hidden="true">add</span>Add webhook</button>
            <button type="button" class="cw-settings-jump" data-action="add-route"><span class="material-symbols-rounded" aria-hidden="true">add</span>Add watcher</button>
          </div>
        </div>
      </div>
    `;
  }

  function renderSummary(o) {
    const s = o.summary || {};
    const r = o.watcher_runtime || {};
    const cards = [
      ["Webhooks", `${s.active_webhooks || 0}/${s.eligible_profiles || 0}`, "webhook"],
      ["Watcher routes", `${s.enabled_routes || 0}/${s.total_routes || 0}`, "bolt"],
      ["Watcher status", r.running ? "Running" : "Stopped", "monitor_heart"],
    ];
    return `<section class="sc2-summary">${cards.map(([a, b, icon, c]) => `<article class="sc2-summary-card"><span class="material-symbols-rounded sc2-summary-icon">${esc(icon)}</span><div><div class="sc2-k">${esc(a)}</div><div class="sc2-v">${esc(b)}</div><div class="sc2-muted">${esc(c)}</div></div></article>`).join("")}</section>`;
  }

  function renderWebhooks(o) {
    const rows = o.webhooks || [];
    return `
      <section class="sc2-section" id="sc-sec-webhook">
        <div class="sc2-section-head"><div><h4>Webhooks</h4><p>Manage media-server profile endpoints.</p></div></div>
        <div class="sc2-route-card-grid">
          ${rows.map((x) => {
            const [text] = webhookState(x);
            return `
            <article class="sc2-route sc2-route-card sc2-webhook-card ${x.enabled ? "is-enabled" : "is-disabled"} ${x.active ? "is-live" : "is-idle"}" data-action="edit-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="Edit webhook">
              <div class="sc2-route-flow-wrap">
                <div class="sc2-route-endpoint">${providerLogo(x.provider)}<div><strong>${esc(label(x.provider))}</strong><span>${esc(webhookProfileName(x))}</span></div></div>
                <div class="sc2-rt-conn"><span class="sc2-rt-conn-line"></span></div>
                <div class="sc2-route-endpoint">${providerLogo(x.sink)}<div><strong>${esc(label(x.sink))}</strong><span>${esc(instanceName(x.sink_instance))}</span></div></div>
              </div>
              <div class="sc2-route-meta">
                <span class="sc2-route-type"><span class="material-symbols-rounded">webhook</span>Webhook</span>
                <span class="sc2-route-id">${esc(text)}</span>
              </div>
              <div class="sc2-route-actions">
                <button type="button" class="btn small sc2-round-action sc2-route-toggle ${x.enabled ? "is-on" : "is-off"}" data-action="toggle-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="${esc(x.enabled ? "Disable source webhooks" : "Enable source webhooks")}" aria-label="${esc(x.enabled ? "Disable source webhooks" : "Enable source webhooks")}"><span class="material-symbols-rounded">power_settings_new</span><span>${x.enabled ? "On" : "Off"}</span></button>
                <button type="button" class="btn small sc2-round-action" data-action="edit-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="Edit webhook" aria-label="Edit webhook"><span class="material-symbols-rounded">edit</span><span>Edit</span></button>
                <button type="button" class="btn small danger sc2-round-action cw-danger-confirm" data-action="delete-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="Delete webhook" aria-label="Delete webhook"><span class="material-symbols-rounded">delete</span><span>Delete</span></button>
              </div>
            </article>`;
          }).join("")}
          <button type="button" class="sc2-route-card sc2-route-add-card" data-action="add-webhook">
            <span class="sc2-logo sc2-add-logo"><span class="material-symbols-rounded">add</span></span>
            <div class="sc2-route-add-copy">
              <strong>Add webhook</strong>
              <span>Create or enable a media-server endpoint.</span>
            </div>
            <span class="material-symbols-rounded sc2-route-add-arrow">arrow_forward</span>
          </button>
        </div>
      </section>
    `;
  }

  function renderRoutes(o) {
    const rows = o.routes || [];
    return `
      <section class="sc2-section" id="sc-sec-watch">
        <div class="sc2-section-head"><div><h4>Watcher routes</h4><p>Configure routes that send play events from media servers to trackers.</p></div></div>
        <div class="sc2-route-card-grid">
          ${rows.length ? rows.map((r) => `
            <article class="sc2-route sc2-route-card ${r.enabled ? "is-enabled" : "is-disabled"} ${r.runtime?.running ? "is-live" : "is-idle"}" data-action="edit-route" data-route="${esc(r.id)}" title="Edit route">
              <div class="sc2-route-flow-wrap">
                <div class="sc2-route-endpoint">${providerLogo(r.provider)}<div><strong>${esc(label(r.provider))}</strong><span>${esc(instanceName(r.provider_instance))}</span></div></div>
                <div class="sc2-rt-conn"><span class="sc2-rt-conn-line"></span></div>
                <div class="sc2-route-endpoint">${providerLogo(r.sink)}<div><strong>${esc(label(r.sink))}</strong><span>${esc(instanceName(r.sink_instance))}</span></div></div>
              </div>
              <div class="sc2-route-meta">
                <span class="sc2-route-type"><span class="material-symbols-rounded">sensors</span>Watcher</span>
                <span class="sc2-route-id">${esc(r.id || "Route")}</span>
                <span class="sc2-route-filters sc2-muted">${esc(routeFilterSummary(r.filters))}</span>
              </div>
              <div class="sc2-route-actions">
                <button type="button" class="btn small sc2-round-action sc2-route-toggle ${r.enabled ? "is-on" : "is-off"}" data-action="toggle-route" data-route="${esc(r.id)}" title="${esc(r.enabled ? "Disable route" : "Enable route")}" aria-label="${esc(r.enabled ? "Disable route" : "Enable route")}"><span class="material-symbols-rounded">power_settings_new</span><span>${esc(r.enabled ? "On" : "Off")}</span></button>
                <button type="button" class="btn small sc2-round-action" data-action="edit-route" data-route="${esc(r.id)}" title="Edit route" aria-label="Edit route"><span class="material-symbols-rounded">edit</span><span>Edit</span></button>
                <button type="button" class="btn small danger sc2-round-action cw-danger-confirm" data-action="delete-route" data-route="${esc(r.id)}" title="Delete route" aria-label="Delete route"><span class="material-symbols-rounded">delete</span><span>Delete</span></button>
              </div>
            </article>
          `).join("") : ""}
          <button type="button" class="sc2-route-card sc2-route-add-card" data-action="add-route">
            <span class="sc2-logo sc2-add-logo"><span class="material-symbols-rounded">add</span></span>
            <div class="sc2-route-add-copy">
              <strong>Add watcher route</strong>
              <span>Create a new source to tracker route.</span>
            </div>
            <span class="material-symbols-rounded sc2-route-add-arrow">arrow_forward</span>
          </button>
        </div>
      </section>
    `;
  }

  function renderRuntime(o) {
    const r = o.watcher_runtime || {};
    const routes = r.routes || [];
    const runningRoutes = routes.filter((x) => x.running).map((x) => x.id).join(", ") || "None";
    return `
      <section class="sc2-section sc2-runtime" id="sc-sec-runtime">
        <div class="sc2-section-head"><div><h4>Watcher status</h4><p>Control and monitor the Scrobbler watcher.</p></div></div>
        <div class="sc2-runtime-grid sc2-runtime-dashboard">
          <article class="sc2-runtime-tile is-groups"><span class="material-symbols-rounded">alt_route</span><div><small>Running routes</small><strong>${esc(runningRoutes)}</strong></div></article>
          <article class="sc2-runtime-tile is-routes"><span class="material-symbols-rounded">route</span><div><small>Routes</small><strong>${esc((r.running_route_count || 0) + " / " + (r.configured_route_count || 0))}</strong></div></article>
          <button type="button" class="sc2-runtime-tile is-autostart sc2-tile-toggle ${r.autostart ? "is-on" : "is-off"}" data-autostart="${r.autostart ? "1" : "0"}" title="${esc(r.autostart ? "Disable autostart" : "Enable autostart")}" aria-pressed="${r.autostart ? "true" : "false"}"><span class="material-symbols-rounded">power_settings_new</span><div><small>Autostart</small><strong>${esc(r.autostart ? "On" : "Off")}</strong></div><small class="sc2-tile-hint">Click to enable / disable</small></button>
          <article class="sc2-runtime-tile is-sources"><span class="material-symbols-rounded">dns</span><div><small>Active sources</small><strong>${esc((r.active_source_providers || []).map(label).join(", ") || "None")}</strong></div></article>
          <article class="sc2-runtime-tile is-sinks"><span class="material-symbols-rounded">database</span><div><small>Active sinks</small><strong>${esc((r.active_sinks || []).map(label).join(", ") || "None")}</strong></div></article>
          <article class="sc2-runtime-tile ${r.error ? "is-error" : "is-clean"}"><span class="material-symbols-rounded">${r.error ? "warning" : "check_circle"}</span><div><small>Runtime error</small><strong>${esc(r.error || "None")}</strong></div></article>
        </div>
        <div class="sc2-runtime-actions">
          <button type="button" class="btn small sc2-watch-btn sc2-watch-power ${r.running ? "sc2-watch-stop" : "sc2-watch-start"}" data-watch="${r.running ? "stop" : "start"}"><span class="material-symbols-rounded sc2-watch-ico">${r.running ? "stop" : "play_arrow"}</span><span class="sc2-watch-lbl">${r.running ? "Stop" : "Start"}</span></button>
          <button type="button" class="btn small sc2-watch-btn sc2-watch-reload" data-watch="refresh"><span class="material-symbols-rounded sc2-watch-ico">sync</span><span class="sc2-watch-lbl">Reload</span></button>
        </div>
      </section>
    `;
  }

  function renderWatcherDefaults(o) {
    const st = o.source_state || {};
    const wd = st.watch_defaults || {};
    const open = state.panels.watcherDefaults;
    const autoOn = !!st.global_auto_remove_watchlist;
    const summary = `Auto-remove ${autoOn ? "On" : "Off"} · Watched ${wd.watched_at ?? "default"} · Final-stop ${wd.force_stop_at ?? "default"} · Step ${wd.progress_step ?? "default"} · Pause ${wd.pause_debounce_seconds ?? "default"} · Suppress ${wd.suppress_start_at ?? "default"}`;
    return `
      <section class="sc2-section sc2-collapse ${open ? "is-open" : ""}" id="sc-sec-watch-defaults">
        <button type="button" class="sc2-collapse-head" data-collapse="watcherDefaults" aria-expanded="${open ? "true" : "false"}">
          <span class="sc2-collapse-ico material-symbols-rounded">tune</span>
          <div><h4>Watcher defaults</h4><p>${open ? "Global defaults for all Watcher routes." : esc(summary)}</p></div>
          <span class="sc2-collapse-chev material-symbols-rounded">expand_more</span>
        </button>
        <div class="sc2-collapse-body">
          <button type="button" class="sc2-setting-row ${autoOn ? "is-on" : "is-off"}" data-setting-toggle="auto-remove" title="${autoOn ? "Disable auto-remove" : "Enable auto-remove"}" aria-pressed="${autoOn ? "true" : "false"}"><span class="sc2-setting-ico material-symbols-rounded">bookmark_remove</span><span class="sc2-setting-copy"><strong>Auto-remove from Watchlists</strong><small>Remove watched items from watchlists automatically.</small></span><span class="sc2-setting-state">${autoOn ? "On" : "Off"}</span></button>
          <div class="sc2-inline-note"><span class="material-symbols-rounded">info</span><span>These settings apply to all routes unless overridden.<br><strong>Leave them at their defaults unless you know what you are doing.</strong></span></div>
          <div class="sc2-defaults-grid">
            <label class="sc2-field"><span class="sc2-label-text">Watched threshold (%)${fieldHelp("Progress percentage that counts an item as watched for completion and auto-remove. Route options can override it.")}</span><input class="input" type="number" min="0" max="100" data-setting-num="watch_watched_at" value="${esc(wd.watched_at ?? "")}" placeholder="Default"></label>
            <label class="sc2-field"><span class="sc2-label-text">Final-stop trust (%)${fieldHelp("At or above this progress, a final stop event is trusted even if the source is noisy near the end. Route options can override it.")}</span><input class="input" type="number" min="0" max="100" data-setting-num="watch_force_stop_at" value="${esc(wd.force_stop_at ?? "")}" placeholder="Default"></label>
            <label class="sc2-field"><span class="sc2-label-text">Progress step (%)${fieldHelp("Minimum progress change required before sending another watching update. Leave this at the default, as it significantly affects API usage.")}</span><input class="input" type="number" min="1" max="25" data-setting-num="watch_progress_step" value="${esc(wd.progress_step ?? "")}" placeholder="Default"></label>
            <label class="sc2-field"><span class="sc2-label-text">Pause debounce (s)${fieldHelp("Seconds to ignore tiny pause/start flaps just after playback starts.")}</span><input class="input" type="number" min="0" max="3600" data-setting-num="watch_pause_debounce_seconds" value="${esc(wd.pause_debounce_seconds ?? "")}" placeholder="Default"></label>
            <label class="sc2-field"><span class="sc2-label-text">Suppress start (%)${fieldHelp("Ignore new start events at or above this progress to avoid credits and near-end restarts.")}</span><input class="input" type="number" min="0" max="100" data-setting-num="watch_suppress_start_at" value="${esc(wd.suppress_start_at ?? "")}" placeholder="Default"></label>
          </div>
        </div>
      </section>
    `;
  }

  function renderRatingsWebhook(o) {
    const st = o.source_state || {};
    const r = st.global_plex_ratings || {};
    const open = state.panels.ratings;
    const url = r.endpoint_url || "";
    const on = ["simkl", "trakt", "mdblist"].filter((k) => r[k]);
    const summary = on.length ? `→ ${on.map(label).join(", ")}` : "No destinations selected";
    return `
      <section class="sc2-section sc2-collapse ${open ? "is-open" : ""}" id="sc-sec-ratings">
        <button type="button" class="sc2-collapse-head" data-collapse="ratings" aria-expanded="${open ? "true" : "false"}">
          <span class="sc2-collapse-ico material-symbols-rounded">star</span>
          <div><h4>Plex ratings webhook</h4><p>${open ? "Global Plex ratings destinations." : esc(summary)}</p></div>
          <span class="sc2-collapse-chev material-symbols-rounded">expand_more</span>
        </button>
        <div class="sc2-collapse-body">
          <div class="sc2-rating-targets">
            ${["simkl", "trakt", "mdblist"].map((k) => `<button type="button" class="sc2-rating-pill provider-${k} ${r[k] ? "is-on" : ""}" data-rating-target="${k}"><span class="material-symbols-rounded">${r[k] ? "check_circle" : "radio_button_unchecked"}</span>${esc(label(k))}</button>`).join("")}
          </div>
          <div class="sc2-endpoint">
            <code title="${esc(url)}">${esc(url || "Global Plex ratings URL is unavailable until a token exists")}</code>
            <button type="button" class="btn small sc2-icon-btn material-symbols-rounded" data-copy-url="${esc(url)}" title="Copy URL">content_copy</button>
            <button type="button" class="btn small danger sc2-icon-btn cw-danger-confirm" data-regen-global title="Regeneration is destructive for the old endpoint URL."><span class="material-symbols-rounded">sync_lock</span></button>
          </div>
        </div>
      </section>
    `;
  }

  function renderWebhookDefaults(o) {
    const st = o.source_state || {};
    const wd = st.webhook_defaults || {};
    const open = state.panels.webhookDefaults;
    const summary = `Pause ${wd.pause_debounce_seconds ?? "default"} · Suppress ${wd.suppress_start_at ?? "default"}`;
    return `
      <section class="sc2-section sc2-collapse ${open ? "is-open" : ""}" id="sc-sec-webhook-defaults">
        <button type="button" class="sc2-collapse-head" data-collapse="webhookDefaults" aria-expanded="${open ? "true" : "false"}">
          <span class="sc2-collapse-ico material-symbols-rounded">webhook</span>
          <div><h4>Webhook defaults</h4><p>${open ? "Global defaults for all webhook profiles." : esc(summary)}</p></div>
          <span class="sc2-collapse-chev material-symbols-rounded">expand_more</span>
        </button>
        <div class="sc2-collapse-body">
          <div class="sc2-inline-note"><span class="material-symbols-rounded">info</span><span>These apply to every webhook profile unless a profile sets its own value.</span></div>
          <div class="sc2-defaults-grid">
            <label class="sc2-field"><span>Pause debounce (s)</span><input class="input" type="number" min="0" max="3600" data-setting-num="webhook_pause_debounce_seconds" value="${esc(wd.pause_debounce_seconds ?? "")}" placeholder="Default"></label>
            <label class="sc2-field"><span>Suppress start (%)</span><input class="input" type="number" min="0" max="3600" data-setting-num="webhook_suppress_start_at" value="${esc(wd.suppress_start_at ?? "")}" placeholder="Default"></label>
          </div>
        </div>
      </section>
    `;
  }

  function renderProviderGate(o) {
    const srcOk = (o.eligible_sources || []).some((g) => (g.profiles || []).some((p) => p.configured));
    const sinkOk = (o.destination_availability || []).some((g) => (g.profiles || []).some((p) => p.configured));
    if (srcOk && sinkOk) return "";
    const needServer = "a compatible media server (Plex, Emby or Jellyfin)";
    const needTracker = "a compatible tracker (Trakt, SIMKL or MDBList)";
    const both = !srcOk && !sinkOk;
    const need = both ? `${needServer} and ${needTracker}` : (!srcOk ? needServer : needTracker);
    return `<div class="sc2-inline-note is-warn"><span class="material-symbols-rounded">warning</span><span>The Scrobbler needs ${esc(need)}. Configure ${both ? "them" : "it"} in Connections to enable webhook and watcher routing.</span><button type="button" class="btn small" data-action="open-connections">Open Connections</button></div>`;
  }

  function render() {
    const o = state.overview || {};
    const gate = renderProviderGate(o);
    const hybrid = o.hybrid_warning ? `<div class="sc2-inline-note is-warn"><span class="material-symbols-rounded">warning</span><span>Webhook and Watcher are both enabled. Do not route the same tracker through both sources.</span></div>` : "";
    const legacyList = o.legacy_webhooks || [];
    const legacy = legacyList.length ? `<button type="button" class="sc2-inline-note is-warn sc2-legacy-note" data-legacy-cleanup><span class="material-symbols-rounded">mop</span><span>${state.legacyConfirm ? "Click again to remove the old webhook endpoints from your config." : `Legacy webhook endpoints found (${esc(legacyList.join(", "))}). Click to clean them up but only after switching your media server to the new webhook URLs.`}</span></button>` : "";
    const routes = o.routes || [];
    const webhooks = o.webhooks || [];
    const hasEnabledRoute = routes.some((r) => r && r.enabled);
    const hasEnabledPlexRoute = routes.some((r) => r && r.enabled && String(r.provider || "").toLowerCase() === "plex");
    const hasEnabledWebhook = webhooks.some((x) => x && x.enabled);
    const watcherDefaults = hasEnabledRoute ? renderWatcherDefaults(o) : "";
    const ratingsWebhook = hasEnabledPlexRoute ? renderRatingsWebhook(o) : "";
    const collapseRow = (watcherDefaults || ratingsWebhook) ? `<div class="sc2-collapse-row">${watcherDefaults}${ratingsWebhook}</div>` : "";
    const webhookDefaults = hasEnabledWebhook ? renderWebhookDefaults(o) : "";
    state.root.innerHTML = `<div class="sc2-page">${renderHeader(o)}${gate}${renderSummary(o)}${hybrid}${legacy}${renderRoutes(o)}${collapseRow}${renderRuntime(o)}${renderWebhooks(o)}${webhookDefaults}</div>`;
  }

  async function loadOverview() {
    state.overview = await j("/api/scrobbler/overview");
    render();
  }

  function applyMutation(data) {
    if (data?.ok) {
      state.overview = data;
      render();
      return;
    }
    return loadOverview();
  }

  async function openModal(name, props) {
    const fn = { webhook: w.openScrobblerWebhookModal, route: w.openScrobblerRouteModal }[name];
    if (typeof fn !== "function") {
      const mod = await import(`/assets/js/modals.js?v=${encodeURIComponent(String(w.__CW_VERSION__ || Date.now()))}`);
      await mod.openModal(`scrobbler-${name}`, props);
      return;
    }
    await fn(props);
  }

  async function routeToggle(route, btn) {
    if (state.busy || !route?.id) return;
    state.busy = true;
    if (btn) btn.disabled = true;
    try {
      const data = await j(`/api/scrobbler/routes/${encodeURIComponent(route.id)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: !route.enabled }),
      });
      applyMutation(data);
    } finally {
      state.busy = false;
      if (btn) btn.disabled = false;
    }
  }

  function findWebhook(provider, instance, sink) {
    return (state.overview?.webhooks || []).find((x) => x.provider === provider && x.provider_instance === instance && (sink == null || x.sink === sink)) || null;
  }

  async function webhookToggle(wh, btn) {
    if (state.busy || !wh) return;
    state.busy = true;
    if (btn) btn.disabled = true;
    try {
      const url = wh.enabled ? "/api/scrobbler/webhooks/profile/disable" : "/api/scrobbler/webhooks/profile";
      const body = wh.enabled
        ? { provider: wh.provider, provider_instance: wh.provider_instance }
        : { provider: wh.provider, provider_instance: wh.provider_instance, enabled: true };
      const data = await j(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      applyMutation(data);
    } finally {
      state.busy = false;
      if (btn) btn.disabled = false;
    }
  }

  function resetInlineDelete() {
    if (state.delTimer) clearTimeout(state.delTimer);
    state.delTimer = 0;
    const btn = state.delBtn;
    state.delBtn = null;
    if (btn && btn.isConnected) {
      btn.classList.remove("is-confirming");
      const ico = btn.querySelector(".material-symbols-rounded");
      if (ico) ico.textContent = "delete";
    }
  }

  async function inlineDelete(btn, run) {
    if (!btn) return;
    if (state.delBtn !== btn) {
      resetInlineDelete();
      state.delBtn = btn;
      btn.classList.add("is-confirming");
      const ico = btn.querySelector(".material-symbols-rounded");
      if (ico) ico.textContent = "warning";
      state.delTimer = setTimeout(resetInlineDelete, 4200);
      return;
    }
    resetInlineDelete();
    if (state.busy) return;
    state.busy = true;
    try {
      const data = await run();
      if (data) applyMutation(data);
    } finally {
      state.busy = false;
    }
  }

  function resetRegenConfirm() {
    if (state.regenTimer) clearTimeout(state.regenTimer);
    state.regenTimer = 0;
    const btn = state.regenBtn;
    state.regenBtn = null;
    if (btn && btn.isConnected) {
      btn.classList.remove("is-confirming");
      const ico = btn.querySelector(".material-symbols-rounded");
      if (ico) ico.textContent = "sync_lock";
    }
  }

  async function webhookDelete(wh, btn) {
    if (!wh) return;
    await inlineDelete(btn, () => j("/api/scrobbler/webhooks/profile/disable", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ provider: wh.provider, provider_instance: wh.provider_instance, sink: wh.sink, remove: true }),
    }));
  }

  async function routeDelete(route, btn) {
    if (!route?.id) return;
    await inlineDelete(btn, () => j(`/api/scrobbler/routes/${encodeURIComponent(route.id)}`, { method: "DELETE" }));
  }

  function findRoute(id) {
    return (state.overview?.routes || []).find((x) => String(x.id) === String(id)) || null;
  }

  async function watchAction(action, btn) {
    if (state.busy) return;
    state.busy = true;
    if (btn) {
      btn.disabled = true;
      btn.classList.add("is-busy");
    }
    try {
      await j(`/api/watch/${action}`, { method: "POST" });
      await loadOverview();
    } finally {
      state.busy = false;
      if (btn) {
        btn.disabled = false;
        btn.classList.remove("is-busy");
      }
    }
  }

  async function autostartToggle(btn) {
    if (state.busy) return;
    state.busy = true;
    if (btn) btn.classList.add("is-busy");
    try {
      const on = btn?.getAttribute("data-autostart") === "1";
      const data = await j("/api/scrobbler/settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ watch_autostart: !on }),
      });
      applyMutation(data);
    } finally {
      state.busy = false;
      if (btn) btn.classList.remove("is-busy");
    }
  }

  async function saveSettings(body, btn) {
    if (state.busy) return;
    state.busy = true;
    if (btn) btn.classList.add("is-busy");
    try {
      const data = await j("/api/scrobbler/settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      applyMutation(data);
    } finally {
      state.busy = false;
      if (btn) btn.classList.remove("is-busy");
    }
  }

  function bind() {
    if (state.bound === state.root) return;
    state.bound = state.root;
    state.root.addEventListener("change", async (e) => {
      const num = e.target.closest("[data-setting-num]");
      if (!num || num.value === "") return;
      await saveSettings({ [num.getAttribute("data-setting-num")]: Number(num.value) });
    });
    state.root.addEventListener("click", async (e) => {
      const watch = e.target.closest("[data-watch]");
      if (watch) {
        e.preventDefault();
        await watchAction(watch.getAttribute("data-watch"), watch);
        return;
      }
      const auto = e.target.closest("[data-autostart]");
      if (auto) {
        e.preventDefault();
        await autostartToggle(auto);
        return;
      }
      const legacyBtn = e.target.closest("[data-legacy-cleanup]");
      if (legacyBtn) {
        e.preventDefault();
        if (!state.legacyConfirm) {
          state.legacyConfirm = true;
          if (state.legacyTimer) clearTimeout(state.legacyTimer);
          state.legacyTimer = setTimeout(() => { state.legacyConfirm = false; render(); }, 4200);
          render();
        } else {
          if (state.legacyTimer) clearTimeout(state.legacyTimer);
          state.legacyConfirm = false;
          if (state.busy) return;
          state.busy = true;
          try {
            const data = await j("/api/scrobbler/webhooks/cleanup-legacy", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
            applyMutation(data);
          } finally {
            state.busy = false;
          }
        }
        return;
      }
      const collapse = e.target.closest("[data-collapse]");
      if (collapse) {
        e.preventDefault();
        const key = collapse.getAttribute("data-collapse");
        state.panels[key] = !state.panels[key];
        if (!state.panels.ratings) resetRegenConfirm();
        render();
        return;
      }
      const setToggle = e.target.closest("[data-setting-toggle]");
      if (setToggle) {
        e.preventDefault();
        if (setToggle.getAttribute("data-setting-toggle") === "auto-remove") {
          await saveSettings({ global_auto_remove_watchlist: !setToggle.classList.contains("is-on") }, setToggle);
        }
        return;
      }
      const ratingTarget = e.target.closest("[data-rating-target]");
      if (ratingTarget) {
        e.preventDefault();
        const cur = state.overview?.source_state?.global_plex_ratings || {};
        const key = ratingTarget.getAttribute("data-rating-target");
        await saveSettings({ global_plex_ratings: { simkl: !!cur.simkl, trakt: !!cur.trakt, mdblist: !!cur.mdblist, [key]: !cur[key] } }, ratingTarget);
        return;
      }
      const copyUrl = e.target.closest("[data-copy-url]");
      if (copyUrl) {
        e.preventDefault();
        if (copyUrl.getAttribute("data-copy-url")) {
          await navigator.clipboard?.writeText(copyUrl.getAttribute("data-copy-url"));
          flashCopied(copyUrl);
        }
        return;
      }
      const regen = e.target.closest("[data-regen-global]");
      if (regen) {
        e.preventDefault();
        if (state.regenBtn !== regen) {
          resetRegenConfirm();
          state.regenBtn = regen;
          regen.classList.add("is-confirming");
          const ico = regen.querySelector(".material-symbols-rounded");
          if (ico) ico.textContent = "check";
          state.regenTimer = setTimeout(resetRegenConfirm, 4200);
        } else {
          resetRegenConfirm();
          await saveSettings({ regenerate_global_plex_ratings_webhook: true }, regen);
        }
        return;
      }
      const btn = e.target.closest("[data-action]");
      if (!btn) return;
      const action = btn.getAttribute("data-action");
      if (action === "open-connections") { try { w.cwSettingsSelect?.("providers"); } catch {} return; }
      if (action === "add-webhook") await openModal("webhook", { mode: "create", overview: state.overview, onSaved: applyMutation });
      if (action === "edit-webhook") await openModal("webhook", { mode: "edit", overview: state.overview, webhook: findWebhook(btn.dataset.provider, btn.dataset.instance, btn.dataset.sink), onSaved: applyMutation });
      if (action === "toggle-webhook") await webhookToggle(findWebhook(btn.dataset.provider, btn.dataset.instance, btn.dataset.sink), btn);
      if (action === "delete-webhook") await webhookDelete(findWebhook(btn.dataset.provider, btn.dataset.instance, btn.dataset.sink), btn);
      if (action === "add-route") await openModal("route", { mode: "create", overview: state.overview, onSaved: applyMutation });
      if (action === "toggle-route") await routeToggle(findRoute(btn.dataset.route), btn);
      if (action === "edit-route") await openModal("route", { mode: "edit", overview: state.overview, route: findRoute(btn.dataset.route), onSaved: applyMutation });
      if (action === "delete-route") await routeDelete(findRoute(btn.dataset.route), btn);
    });
  }

  async function init(opts = {}) {
    state.root = opts.mountId ? d.getElementById(opts.mountId) : d.getElementById("scrobble-mount");
    if (!state.root) return;
    state.panels = { watcherDefaults: false, ratings: false, webhookDefaults: false };
    state.legacyConfirm = false;
    if (state.legacyTimer) clearTimeout(state.legacyTimer);
    state.legacyTimer = 0;
    resetRegenConfirm();
    resetInlineDelete();
    if (!state.root.querySelector(".sc2-page")) {
      state.root.innerHTML = `<div class="sc2-page">${renderHeader({})}<div class="sc2-empty">Loading Scrobbler...</div></div>`;
    }
    bind();
    await loadOverview();
  }

  async function refresh() {
    if (!state.root) return;
    await loadOverview();
  }

  function mount(targetEl) {
    return init({ mountId: targetEl?.id });
  }

  w.ScrobUI = { state, refresh, applyMutation };
  w.Scrobbler = { init, mount, refresh, getConfig: (prev) => prev || {}, getRootPatch: () => ({}) };
  w.getScrobbleConfig = (prev) => prev || {};
  w.getRootPatch = () => ({});
})(window, document);
