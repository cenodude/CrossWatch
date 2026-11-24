/* assets/js/scrobbler.js */
/* Scrobbler configuration UI and logic. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const T = (key, vars) => (w.CW?.I18n || w.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (w.CW?.I18n || w.__cwI18n)?.html?.(key, vars) ?? key;
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const providerMeta = () => w.CW?.ProviderMeta || {};
  const label = (v) => providerMeta().label?.(v) || String(v || "").toUpperCase();
  const logo = (v) => providerMeta().logoPath?.(v) || "";
  const plexRatingSinks = ["kitsu", "myanimelist", "crosswatch", "trakt", "simkl", "mdblist", "floppy", "punchplay", "flicklist", "wetrakr", "anilist", "scrob", "silo"];
  const state = { root: null, overview: null, busy: false, panels: { watcherDefaults: false, ratings: false, webhookDefaults: false }, delBtn: null, delTimer: 0, regenBtn: null, regenTimer: 0, hybridDismissed: false };

  async function authSetupBlocked() {
    const boot = w.__cwAuthBootstrapState;
    if (boot && typeof boot.blocked === "boolean") return boot.blocked;
    const pending = w.__cwAuthBootstrapPromise;
    if (pending && typeof pending.then === "function") {
      const resolved = await pending.catch(() => null);
      if (resolved && typeof resolved.blocked === "boolean") return resolved.blocked;
    }
    return w.cwIsAuthSetupPending?.() === true;
  }

  function authPendingError() {
    const err = new Error("auth setup pending");
    err.code = "auth_setup_pending";
    return err;
  }

  function isAuthPendingError(err) {
    return err?.code === "auth_setup_pending" || String(err?.message || "").includes("auth setup pending");
  }

  async function j(url, options = {}) {
    if (await authSetupBlocked()) throw authPendingError();
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
    if (x.enabled && x.source_configured) return [T("scrobbler.page.connected"), "ok"];
    if (x.enabled) return [T("scrobbler.page.needsSetup"), "warn"];
    return [T("scrobbler.page.disabled"), "off"];
  }

  function webhookProfileName(x) {
    return String(x.profile_label || x.source_label || x.provider_instance || "default").trim() || T("scrobbler.page.default");
  }

  function instanceName(v, labelValue = "") {
    const labelText = String(labelValue || "").trim();
    if (labelText) return labelText;
    return String(v || "default") === "default" ? T("scrobbler.page.default") : String(v || "default");
  }

  function profileConnector(row) {
    const name = String(row?.user_profile_label || "").trim();
    if (!name) return `<div class="sc2-rt-conn"><span class="sc2-rt-conn-line"></span></div>`;
    return `<div class="sc2-rt-conn has-profile"><span class="sc2-rt-conn-line"></span>
      <span class="sc2-rt-profile" title="${esc(`Profile: ${name}`)}">
        <span class="material-symbols-rounded" aria-hidden="true">person</span>
        <span class="sc2-rt-profile-name">${esc(name)}</span>
      </span></div>`;
  }

  function linkedSummary(on, mapping) {
    if (!on) return "";
    const n = Object.keys(mapping && typeof mapping === "object" ? mapping : {}).length;
    return n ? T("scrobbler.page.peopleLinked", {count: n}) : T("scrobbler.page.noPeople");
  }

  function routeFilterSummary(filters = {}, linked = "", replaceUsers = false) {
    const parts = linked ? [linked] : [];
    const users = Array.isArray(filters.username_whitelist) ? filters.username_whitelist : [];
    const allow = Array.isArray(filters.server_uuid_whitelist) ? filters.server_uuid_whitelist : [];
    const block = Array.isArray(filters.server_uuid_blacklist) ? filters.server_uuid_blacklist : [];
    const paths = Array.isArray(filters.ignored_path_prefixes) ? filters.ignored_path_prefixes : [];
    const patterns = Array.isArray(filters.ignored_filename_patterns) ? filters.ignored_filename_patterns : [];
    const editions = Array.isArray(filters.ignored_editions) ? filters.ignored_editions : [];
    if (users.length && !replaceUsers) parts.push(T("scrobbler.page.users", {count: users.length}));
    if (allow.length || filters.server_uuid) parts.push(`${allow.length || 1} UUID allow`);
    if (block.length) parts.push(`${block.length} UUID block`);
    if (filters.ignore_agregarr_trailers) parts.push(T("scrobbler.page.agregarr"));
    if (filters.ignore_specials) parts.push(T("scrobbler.page.specials"));
    if (paths.length) parts.push(T("scrobbler.page.pathsIgnored", {count: paths.length}));
    if (patterns.length) parts.push(T("scrobbler.page.patternsIgnored", {count: patterns.length}));
    if (editions.length) parts.push(T("scrobbler.page.editionsIgnored", {count: editions.length}));
    if (filters.ignore_live_tv_dvr) parts.push(T("scrobbler.page.liveTv"));
    return parts.length ? parts.join(" - ") : T("scrobbler.page.noFilters");
  }

  function webhookFilterSummary(row = {}) {
    const provider = String(row.provider || "").toLowerCase();
    const key = provider === "plex" ? "filters_plex" : provider === "jellyfin" ? "filters_jellyfin" : "filters_emby";
    const settings = row.effective_settings || {};
    const sink = String(row.sink || "").toLowerCase();
    return routeFilterSummary(settings[key] || {}, linkedSummary(settings.match_users?.[sink] === true, settings.user_matches?.[sink]));
  }

  function renderHeader(o) {
    return `
      <div class="cw-settings-pane-head cw-settings-hero cw-settings-hero-scrobbler sc2-pane-head">
        <div>
          <div class="cw-settings-pane-kicker">${TH("scrobbler.page.kicker")}</div>
          <h3>${TH("scrobbler.page.title")}</h3>
          <p>${TH("scrobbler.page.heroHint")} <b>${TH("scrobbler.page.watcherRecommended")}</b></p>
        </div>
        <div class="cw-settings-pane-head-actions">
          <div class="cw-conn-hero-summary" aria-label="${TH("scrobbler.page.actions")}">
            <button type="button" class="cw-conn-seg" data-action="add-webhook"><span class="material-symbols-rounded" aria-hidden="true">add</span><span>${TH("scrobbler.page.addWebhook")}</span></button>
            <button type="button" class="cw-conn-seg" data-action="add-route"><span class="material-symbols-rounded" aria-hidden="true">add</span><span>${TH("scrobbler.page.addWatcher")}</span></button>
            <button type="button" class="cw-conn-refresh" data-action="refresh-scrobbler" title="${TH("scrobbler.page.refresh")}" aria-label="${TH("scrobbler.page.refresh")}"><span class="material-symbol" aria-hidden="true">refresh</span></button>
          </div>
        </div>
        <span class="material-symbols-rounded cw-settings-hero-shape" aria-hidden="true">sensors</span>
      </div>
    `;
  }

  function renderSummary(o) {
    const s = o.summary || {};
    const r = o.watcher_runtime || {};
    const webhooksOn = Number(s.active_webhooks || 0) > 0;
    const routesOn = Number(s.enabled_routes || 0) > 0;
    const cards = [
      [T("scrobbler.page.webhooks"), `${s.active_webhooks || 0}/${s.eligible_profiles || 0}`, "webhook", "webhooks", webhooksOn ? "" : "is-empty"],
      [T("scrobbler.page.routes"), `${s.enabled_routes || 0}/${s.total_routes || 0}`, "bolt", "watcher-routes", routesOn ? "" : "is-empty"],
      [T("scrobbler.page.status"), r.running ? T("scrobbler.page.running") : T("scrobbler.page.stopped"), "monitor_heart", "watcher-status", r.running ? "" : "is-stopped"],
    ];
    return `<section class="sc2-summary">${cards.map(([a, b, icon, stat, state]) => `<article class="sc2-summary-card ${state}" data-stat="${esc(stat)}"><span class="material-symbols-rounded sc2-summary-icon">${esc(icon)}</span><div><div class="sc2-k">${esc(a)}</div><div class="sc2-v">${esc(b)}</div><div class="sc2-muted"></div></div></article>`).join("")}</section>`;
  }

  function renderWebhooks(o) {
    const rows = o.webhooks || [];
    const sectionState = rows.some((x) => x.enabled) ? "" : " is-empty";
    return `
      <section class="sc2-section${sectionState}" id="sc-sec-webhook">
        <div class="sc2-section-head"><div><h4>${TH("scrobbler.page.webhooks")}</h4><p>${TH("scrobbler.page.webhooksHint")}</p></div></div>
        <div class="sc2-route-card-grid">
          ${rows.map((x) => {
            const [text] = webhookState(x);
            return `
            <article class="sc2-route sc2-route-card sc2-webhook-card ${x.enabled ? "is-enabled" : "is-disabled"} ${x.active ? "is-live" : "is-idle"} ${x.user_profile_label ? "has-user-profile" : ""}" data-action="edit-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="${TH("scrobbler.page.editWebhook")}">
              <div class="sc2-route-flow-wrap">
                <div class="sc2-route-endpoint">${providerLogo(x.provider)}<div><strong>${esc(label(x.provider))}</strong><span>${esc(webhookProfileName(x))}</span></div></div>
                ${profileConnector(x)}
                <div class="sc2-route-endpoint sc2-route-endpoint-sink"><div><strong>${esc(label(x.sink))}</strong><span>${esc(instanceName(x.sink_instance, x.sink_profile_label))}</span></div>${providerLogo(x.sink)}</div>
              </div>
              <div class="sc2-route-meta">
                <span class="sc2-route-type"><span class="material-symbols-rounded">webhook</span>${TH("scrobbler.page.webhook")}</span>
                <span class="sc2-route-id">${esc(text)}</span>
                <span class="sc2-route-filters sc2-muted">${esc(webhookFilterSummary(x))}</span>
              </div>
              <div class="sc2-route-actions">
                <button type="button" class="btn small sc2-round-action sc2-route-toggle ${x.enabled ? "is-on" : "is-off"}" data-action="toggle-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="${esc(x.enabled ? T("scrobbler.page.disableWebhooks") : T("scrobbler.page.enableWebhooks"))}" aria-label="${esc(x.enabled ? T("scrobbler.page.disableWebhooks") : T("scrobbler.page.enableWebhooks"))}"><span class="material-symbols-rounded">power_settings_new</span><span>${x.enabled ? T("scrobbler.page.on") : T("scrobbler.page.off")}</span></button>
                <button type="button" class="btn small sc2-round-action" data-action="edit-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="${TH("scrobbler.page.editWebhook")}" aria-label="${TH("scrobbler.page.editWebhook")}"><span class="material-symbols-rounded">edit</span><span>${TH("scrobbler.page.edit")}</span></button>
                <button type="button" class="btn small danger sc2-round-action cw-danger-confirm" data-action="delete-webhook" data-provider="${esc(x.provider)}" data-instance="${esc(x.provider_instance)}" data-sink="${esc(x.sink)}" title="${TH("scrobbler.page.deleteWebhook")}" aria-label="${TH("scrobbler.page.deleteWebhook")}"><span class="material-symbols-rounded">delete</span><span>${TH("scrobbler.page.delete")}</span></button>
              </div>
            </article>`;
          }).join("")}
          <button type="button" class="sc2-route-card sc2-route-add-card" data-action="add-webhook">
            <span class="sc2-logo sc2-add-logo"><span class="material-symbols-rounded">add</span></span>
            <div class="sc2-route-add-copy">
              <strong>${TH("scrobbler.page.addWebhook")}</strong>
              <span>${TH("scrobbler.page.addWebhookHint")}</span>
            </div>
            <span class="material-symbols-rounded sc2-route-add-arrow">arrow_forward</span>
          </button>
        </div>
      </section>
    `;
  }

  function renderRoutes(o) {
    const rows = o.routes || [];
    const sectionState = rows.some((r) => r.enabled) ? "" : " is-empty";
    return `
      <section class="sc2-section${sectionState}" id="sc-sec-watch">
        <div class="sc2-section-head"><div><h4>${TH("scrobbler.page.routes")}</h4><p>${TH("scrobbler.page.routesHint")}</p></div></div>
        <div class="sc2-route-card-grid">
          ${rows.length ? rows.map((r) => `
            <article class="sc2-route sc2-route-card ${r.enabled ? "is-enabled" : "is-disabled"} ${r.runtime?.running ? "is-live" : "is-idle"} ${r.user_profile_label ? "has-user-profile" : ""}" data-action="edit-route" data-route="${esc(r.id)}" title="${TH("scrobbler.page.editRoute")}">
              <div class="sc2-route-flow-wrap">
                <div class="sc2-route-endpoint">${providerLogo(r.provider)}<div><strong>${esc(label(r.provider))}</strong><span>${esc(instanceName(r.provider_instance, r.source_label))}</span></div></div>
                ${profileConnector(r)}
                <div class="sc2-route-endpoint sc2-route-endpoint-sink"><div><strong>${esc(label(r.sink))}</strong><span>${esc(instanceName(r.sink_instance, r.sink_label))}</span></div>${providerLogo(r.sink)}</div>
              </div>
              <div class="sc2-route-meta">
                <span class="sc2-route-type"><span class="material-symbols-rounded">sensors</span>${TH("scrobbler.page.watcher")}</span>
                <span class="sc2-route-id">${esc(r.id || T("scrobbler.page.route"))}</span>
                <span class="sc2-route-filters sc2-muted">${esc(routeFilterSummary(r.filters, linkedSummary(r.match_users === true, r.user_matches), r.match_users === true))}</span>
                ${r.needs_account_filter ? `<span class="sc2-route-warn" title="${TH("scrobbler.page.noAccountFilterHint")}"><span class="material-symbols-rounded">warning</span>${TH("scrobbler.page.noAccountFilter")}</span>` : ""}
              </div>
              <div class="sc2-route-actions">
                <button type="button" class="btn small sc2-round-action sc2-route-toggle ${r.enabled ? "is-on" : "is-off"}" data-action="toggle-route" data-route="${esc(r.id)}" title="${esc(r.enabled ? T("scrobbler.page.disableRoute") : T("scrobbler.page.enableRoute"))}" aria-label="${esc(r.enabled ? T("scrobbler.page.disableRoute") : T("scrobbler.page.enableRoute"))}"><span class="material-symbols-rounded">power_settings_new</span><span>${esc(r.enabled ? T("scrobbler.page.on") : T("scrobbler.page.off"))}</span></button>
                <button type="button" class="btn small sc2-round-action" data-action="edit-route" data-route="${esc(r.id)}" title="${TH("scrobbler.page.editRoute")}" aria-label="${TH("scrobbler.page.editRoute")}"><span class="material-symbols-rounded">edit</span><span>${TH("scrobbler.page.edit")}</span></button>
                <button type="button" class="btn small danger sc2-round-action cw-danger-confirm" data-action="delete-route" data-route="${esc(r.id)}" title="${TH("scrobbler.page.deleteRoute")}" aria-label="${TH("scrobbler.page.deleteRoute")}"><span class="material-symbols-rounded">delete</span><span>${TH("scrobbler.page.delete")}</span></button>
              </div>
            </article>
          `).join("") : ""}
          <button type="button" class="sc2-route-card sc2-route-add-card" data-action="add-route">
            <span class="sc2-logo sc2-add-logo"><span class="material-symbols-rounded">add</span></span>
            <div class="sc2-route-add-copy">
              <strong>${TH("scrobbler.page.addRoute")}</strong>
              <span>${TH("scrobbler.page.addRouteHint")}</span>
            </div>
            <span class="material-symbols-rounded sc2-route-add-arrow">arrow_forward</span>
          </button>
        </div>
      </section>
    `;
  }

  function renderRuntime(o) {
    const r = o.watcher_runtime || {};
    const sectionState = r.running ? "" : " is-stopped";
    const routes = r.routes || [];
    const runningRoutes = routes.filter((x) => x.running).map((x) => x.id).join(", ") || T("scrobbler.page.none");
    return `
      <section class="sc2-section sc2-runtime${sectionState}" id="sc-sec-runtime">
        <div class="sc2-section-head"><div><h4>${TH("scrobbler.page.status")}</h4><p>${TH("scrobbler.page.statusHint")}</p></div></div>
        <div class="sc2-runtime-grid sc2-runtime-dashboard">
          <article class="sc2-runtime-tile is-groups"><span class="material-symbols-rounded">alt_route</span><div><small>${TH("scrobbler.page.runningRoutes")}</small><strong>${esc(runningRoutes)}</strong></div></article>
          <article class="sc2-runtime-tile is-routes"><span class="material-symbols-rounded">route</span><div><small>${TH("scrobbler.page.routesLabel")}</small><strong>${esc((r.running_route_count || 0) + " / " + (r.configured_route_count || 0))}</strong></div></article>
          <button type="button" class="sc2-runtime-tile is-autostart sc2-tile-toggle ${r.autostart ? "is-on" : "is-off"}" data-autostart="${r.autostart ? "1" : "0"}" title="${esc(r.autostart ? T("scrobbler.page.disableAutostart") : T("scrobbler.page.enableAutostart"))}" aria-pressed="${r.autostart ? "true" : "false"}"><span class="material-symbols-rounded">power_settings_new</span><div><small>${TH("scrobbler.page.autostart")}</small><strong>${esc(r.autostart ? T("scrobbler.page.on") : T("scrobbler.page.off"))}</strong></div><small class="sc2-tile-hint">${TH("scrobbler.page.toggleHint")}</small></button>
          <article class="sc2-runtime-tile is-sources"><span class="material-symbols-rounded">dns</span><div><small>${TH("scrobbler.page.sources")}</small><strong>${esc((r.active_source_providers || []).map(label).join(", ") || T("scrobbler.page.none"))}</strong></div></article>
          <article class="sc2-runtime-tile is-sinks"><span class="material-symbols-rounded">database</span><div><small>${TH("scrobbler.page.sinks")}</small><strong>${esc((r.active_sinks || []).map(label).join(", ") || T("scrobbler.page.none"))}</strong></div></article>
          <article class="sc2-runtime-tile ${r.error ? "is-error" : "is-clean"}"><span class="material-symbols-rounded">${r.error ? "warning" : "check_circle"}</span><div><small>${TH("scrobbler.page.runtimeError")}</small><strong>${esc(r.error || T("scrobbler.page.none"))}</strong></div></article>
        </div>
        <div class="sc2-runtime-actions">
          <button type="button" class="btn small sc2-watch-btn sc2-watch-power ${r.running ? "sc2-watch-stop" : "sc2-watch-start"}" data-watch="${r.running ? "stop" : "start"}"><span class="material-symbols-rounded sc2-watch-ico">${r.running ? "stop" : "play_arrow"}</span><span class="sc2-watch-lbl">${r.running ? T("scrobbler.page.stop") : T("scrobbler.page.start")}</span></button>
          <button type="button" class="btn small sc2-watch-btn sc2-watch-reload" data-watch="refresh"><span class="material-symbols-rounded sc2-watch-ico">sync</span><span class="sc2-watch-lbl">${TH("scrobbler.page.reload")}</span></button>
        </div>
      </section>
    `;
  }

  function renderWatcherDefaults(o) {
    const st = o.source_state || {};
    const wd = st.watch_defaults || {};
    const open = state.panels.watcherDefaults;
    const autoOn = !!st.global_auto_remove_watchlist;
    const dflt = T("scrobbler.page.defaultLower");
    const summary = T("scrobbler.page.watcherSummary", {auto: autoOn ? T("scrobbler.page.on") : T("scrobbler.page.off"), watched: String(wd.watched_at ?? dflt), stop: String(wd.force_stop_at ?? dflt), step: String(wd.progress_step ?? dflt), pause: String(wd.pause_debounce_seconds ?? dflt), suppress: String(wd.suppress_start_at ?? dflt)});
    return `
      <section class="sc2-section sc2-collapse ${open ? "is-open" : ""}" id="sc-sec-watch-defaults">
        <button type="button" class="sc2-collapse-head" data-collapse="watcherDefaults" aria-expanded="${open ? "true" : "false"}">
          <span class="sc2-collapse-ico material-symbols-rounded">tune</span>
          <div><h4>${TH("scrobbler.page.watcherDefaults")}</h4><p>${open ? T("scrobbler.page.watcherDefaultsHint") : esc(summary)}</p></div>
          <span class="sc2-collapse-chev material-symbols-rounded">expand_more</span>
        </button>
        <div class="sc2-collapse-body">
          <button type="button" class="sc2-setting-row ${autoOn ? "is-on" : "is-off"}" data-setting-toggle="auto-remove" title="${autoOn ? T("scrobbler.page.disableAutoRemove") : T("scrobbler.page.enableAutoRemove")}" aria-pressed="${autoOn ? "true" : "false"}"><span class="sc2-setting-ico material-symbols-rounded">bookmark_remove</span><span class="sc2-setting-copy"><strong>${TH("scrobbler.page.autoRemove")}</strong><small>${TH("scrobbler.page.autoRemoveHint")}</small></span><span class="sc2-setting-state">${autoOn ? T("scrobbler.page.on") : T("scrobbler.page.off")}</span></button>
          <div class="sc2-inline-note"><span class="material-symbols-rounded">info</span><span>${TH("scrobbler.page.applyAll")}<br><strong>${TH("scrobbler.routeModal.leaveDefaults")}</strong></span></div>
          <div class="sc2-defaults-grid">
            <label class="sc2-field"><span class="sc2-label-text">${TH("scrobbler.routeModal.watched")}${fieldHelp(T("scrobbler.page.watchedHelp"))}</span><input class="input" type="number" min="0" max="100" data-setting-num="watch_watched_at" value="${esc(wd.watched_at ?? "")}" placeholder="${TH("scrobbler.page.default")}"></label>
            <label class="sc2-field"><span class="sc2-label-text">${TH("scrobbler.routeModal.finalStop")}${fieldHelp(T("scrobbler.page.finalStopHelp"))}</span><input class="input" type="number" min="0" max="100" data-setting-num="watch_force_stop_at" value="${esc(wd.force_stop_at ?? "")}" placeholder="${TH("scrobbler.page.default")}"></label>
            <label class="sc2-field"><span class="sc2-label-text">${TH("scrobbler.routeModal.progressStep")}${fieldHelp(T("scrobbler.routeModal.progressStepHint"))}</span><input class="input" type="number" min="15" max="50" data-setting-num="watch_progress_step" value="${esc(wd.progress_step ?? "")}" placeholder="${TH("scrobbler.page.default")}"></label>
            <label class="sc2-field"><span class="sc2-label-text">${TH("scrobbler.routeModal.pauseDebounce")}${fieldHelp(T("scrobbler.page.pauseHelp"))}</span><input class="input" type="number" min="0" max="3600" data-setting-num="watch_pause_debounce_seconds" value="${esc(wd.pause_debounce_seconds ?? "")}" placeholder="${TH("scrobbler.page.default")}"></label>
            <label class="sc2-field"><span class="sc2-label-text">${TH("scrobbler.routeModal.suppressStart")}${fieldHelp(T("scrobbler.page.suppressHelp"))}</span><input class="input" type="number" min="0" max="100" data-setting-num="watch_suppress_start_at" value="${esc(wd.suppress_start_at ?? "")}" placeholder="${TH("scrobbler.page.default")}"></label>
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
    const configured = new Set((o.destination_availability || [])
      .filter((g) => plexRatingSinks.includes(String(g?.provider || "").toLowerCase()))
      .filter((g) => (g?.profiles || []).some((p) => p?.configured))
      .map((g) => String(g.provider || "").toLowerCase()));
    const visibleSinks = plexRatingSinks.filter((k) => configured.has(k) || r[k]);
    const on = visibleSinks.filter((k) => r[k]);
    const summary = on.length ? `→ ${on.map(label).join(", ")}` : T("scrobbler.page.noDestinations");
    return `
      <section class="sc2-section sc2-collapse ${open ? "is-open" : ""}" id="sc-sec-ratings">
        <button type="button" class="sc2-collapse-head" data-collapse="ratings" aria-expanded="${open ? "true" : "false"}">
          <span class="sc2-collapse-ico material-symbols-rounded">star</span>
          <div><h4>${TH("scrobbler.page.plexRatings")}</h4><p>${open ? T("scrobbler.page.plexRatingsHint") : esc(summary)}</p></div>
          <span class="sc2-collapse-chev material-symbols-rounded">expand_more</span>
        </button>
        <div class="sc2-collapse-body">
          <div class="sc2-rating-targets">
            ${visibleSinks.map((k) => `<button type="button" class="sc2-rating-pill provider-${k} ${r[k] ? "is-on" : ""}" data-rating-target="${k}"><span class="material-symbols-rounded">${r[k] ? "check_circle" : "radio_button_unchecked"}</span>${esc(label(k))}</button>`).join("") || '<div class="sc2-muted">' + TH("scrobbler.page.configureRatings") + '</div>'}
          </div>
          <div class="sc2-endpoint">
            ${url ? `<input type="text" readonly value="${esc(url)}" title="${esc(url)}" aria-label="${TH("scrobbler.page.ratingsUrl")}" spellcheck="false" data-copy-field>` : "<code>" + TH("scrobbler.page.ratingsUrlUnavailable") + "</code>"}
            <button type="button" class="btn small sc2-icon-btn material-symbols-rounded" data-copy-url="${esc(url)}" title="${TH("scrobbler.page.copyUrl")}">content_copy</button>
            <button type="button" class="btn small danger sc2-icon-btn cw-danger-confirm" data-regen-global title="${TH("scrobbler.page.regenHint")}"><span class="material-symbols-rounded">sync_lock</span></button>
          </div>
        </div>
      </section>
    `;
  }

  function renderWebhookDefaults(o) {
    const st = o.source_state || {};
    const wd = st.webhook_defaults || {};
    const open = state.panels.webhookDefaults;
    const summary = T("scrobbler.page.webhookSummary", {pause: String(wd.pause_debounce_seconds ?? T("scrobbler.page.defaultLower")), suppress: String(wd.suppress_start_at ?? T("scrobbler.page.defaultLower"))});
    return `
      <section class="sc2-section sc2-collapse ${open ? "is-open" : ""}" id="sc-sec-webhook-defaults">
        <button type="button" class="sc2-collapse-head" data-collapse="webhookDefaults" aria-expanded="${open ? "true" : "false"}">
          <span class="sc2-collapse-ico material-symbols-rounded">webhook</span>
          <div><h4>${TH("scrobbler.page.webhookDefaults")}</h4><p>${open ? T("scrobbler.page.webhookDefaultsHint") : esc(summary)}</p></div>
          <span class="sc2-collapse-chev material-symbols-rounded">expand_more</span>
        </button>
        <div class="sc2-collapse-body">
          <div class="sc2-inline-note"><span class="material-symbols-rounded">info</span><span>${TH("scrobbler.page.webhookApply")}</span></div>
          <div class="sc2-defaults-grid">
            <label class="sc2-field"><span>${TH("scrobbler.page.pauseDebounce")}</span><input class="input" type="number" min="0" max="3600" data-setting-num="webhook_pause_debounce_seconds" value="${esc(wd.pause_debounce_seconds ?? "")}" placeholder="${TH("scrobbler.page.default")}"></label>
            <label class="sc2-field"><span>${TH("scrobbler.page.suppressStart")}</span><input class="input" type="number" min="0" max="3600" data-setting-num="webhook_suppress_start_at" value="${esc(wd.suppress_start_at ?? "")}" placeholder="${TH("scrobbler.page.default")}"></label>
          </div>
        </div>
      </section>
    `;
  }

  function render() {
    const o = state.overview || {};
    const hybrid = o.hybrid_warning && !state.hybridDismissed ? `<div class="sc2-inline-note is-warn sc2-dismissible-note"><span class="material-symbols-rounded">warning</span><span>${TH("scrobbler.page.hybrid")}</span><button type="button" class="sc2-note-dismiss" data-action="dismiss-hybrid-warning" aria-label="${TH("scrobbler.page.dismiss")}"><span class="material-symbols-rounded">close</span></button></div>` : "";
    const routes = o.routes || [];
    const webhooks = o.webhooks || [];
    const hasEnabledRoute = routes.some((r) => r && r.enabled);
    const hasEnabledPlexRoute = routes.some((r) => r && r.enabled && String(r.provider || "").toLowerCase() === "plex");
    const hasEnabledWebhook = webhooks.some((x) => x && x.enabled);
    const watcherDefaults = hasEnabledRoute ? renderWatcherDefaults(o) : "";
    const ratingsWebhook = hasEnabledPlexRoute ? renderRatingsWebhook(o) : "";
    const collapseRow = (watcherDefaults || ratingsWebhook) ? `<div class="sc2-collapse-row">${watcherDefaults}${ratingsWebhook}</div>` : "";
    const webhookDefaults = hasEnabledWebhook ? renderWebhookDefaults(o) : "";
    state.root.innerHTML = `<div class="sc2-page">${renderHeader(o)}${renderSummary(o)}${hybrid}${renderRoutes(o)}${collapseRow}${renderRuntime(o)}${renderWebhooks(o)}${webhookDefaults}</div>`;
  }

  function renderAuthPending() {
    if (!state.root) return;
    state.root.innerHTML = `<div class="sc2-page">${renderHeader(state.overview || {})}<div class="sc2-empty">${TH("scrobbler.page.finishSetup")}</div></div>`;
  }

  async function loadOverview() {
    try {
      state.overview = await j("/api/scrobbler/overview");
      render();
    } catch (err) {
      if (isAuthPendingError(err)) {
        renderAuthPending();
        return;
      }
      throw err;
    }
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
        await saveSettings({ global_plex_ratings: Object.fromEntries(plexRatingSinks.map((sink) => [sink, sink === key ? !cur[sink] : !!cur[sink]])) }, ratingTarget);
        return;
      }
      const copyUrl = e.target.closest("[data-copy-url]");
      if (copyUrl) {
        e.preventDefault();
        if (copyUrl.getAttribute("data-copy-url")) {
          if (await window.cwCopyText?.(copyUrl.getAttribute("data-copy-url"))) flashCopied(copyUrl);
          else window.cwSelectCopyField?.(copyUrl.parentElement?.querySelector("[data-copy-field]"));
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
      if (action === "dismiss-hybrid-warning") { state.hybridDismissed = true; btn.closest(".sc2-dismissible-note")?.remove(); return; }
      if (action === "refresh-scrobbler") {
        if (btn.classList.contains("loading")) return;
        btn.classList.add("loading", "spin");
        try { await refresh(); } catch {}
        btn.classList.remove("loading", "spin");
        return;
      }
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
    resetRegenConfirm();
    resetInlineDelete();
    if (!state.root.querySelector(".sc2-page")) {
      state.root.innerHTML = `<div class="sc2-page">${renderHeader({})}<div class="sc2-empty">${TH("scrobbler.page.loading")}</div></div>`;
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
