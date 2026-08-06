/* Playback Progress page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const ROOT_ID = "playback-progress-root";
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const api = async (url, opts = {}) => {
    const r = await fetch(url, { credentials: "same-origin", cache: "no-store", ...opts });
    const txt = await r.text();
    let data = {};
    try { data = txt ? JSON.parse(txt) : {}; } catch {}
    if (!r.ok) data.ok = false;
    return data;
  };
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
  const providerMeta = () => window.CW?.ProviderMeta || {};
  const providerLabel = (provider) => providerMeta().label?.(provider) || String(provider || "").trim().toUpperCase() || "Provider";
  const providerLogo = (provider) => providerMeta().logoPath?.(provider) || providerMeta().logLogoPath?.(provider) || `/assets/img/${String(provider || "").toUpperCase()}.svg`;
  const providerLogLogo = (provider) => providerMeta().logLogoPath?.(provider) || providerLogo(provider);
  const providerTone = (provider) => providerMeta().tone?.(provider)?.rgb || "124,92,255";
  const providerIcon = (provider) => {
    return `<img src="${esc(providerLogLogo(provider))}" alt="" onerror="this.remove()">`;
  };
  const PLAYBACK_PROVIDER_KEYS = ["crosswatch", "trakt", "simkl", "mdblist", "publicmetadb", "plex", "emby", "jellyfin", "nuvio", "kodi", "stremio", "floppy"];
  const DEFAULT_PROVIDER_TIMEOUT_SECONDS = 20;
  const state = {
    mounted: false,
    page: 1,
    pageSize: 30,
    total: 0,
    items: [],
    providers: [],
    errors: [],
    selected: new Map(),
    filters: { provider: "", media_type: "", progress: "", age: "", rating: "", search: "", sort: "last_updated" },
    busy: false,
    loaded: false,
    settings: null,
    lastSyncAt: null,
    lastRefreshFailed: false,
    syncClock: null
  };


  function root() {
    return document.getElementById(ROOT_ID);
  }

  function shell() {
    return `
      <div class="pp-head cw-page-hero cw-page-hero-playback" data-hero-icon="play_circle">
        <div class="cw-page-hero-copy"><div class="cw-page-hero-kicker">PLAYBACK</div><div class="pp-title cw-page-hero-title">Playback Progress</div><div class="pp-intro cw-page-hero-sub">Manage unfinished playback records across supported providers.</div></div>
        <div class="pp-hero-summary cw-page-hero-actions" id="pp-hero-summary" role="group" aria-label="Playback Progress actions">
          <button class="pp-page-control pp-view-control" id="pp-settings" type="button" title="Configure Playback Progress view" aria-label="Configure Playback Progress view">${icon("tune")}<strong>View</strong><span class="material-symbols-rounded pp-chevron" aria-hidden="true">keyboard_arrow_down</span></button>
          <div class="pp-hero-seg pp-sync-status" id="pp-sync-status" data-state="ready" aria-live="polite"><span>Synced</span><strong id="pp-sync-time">not yet</strong></div>
          <button class="pp-page-control pp-refresh-control" id="pp-refresh" type="button" title="Refresh Records" aria-label="Refresh Records">${icon("refresh")}</button>
        </div>
      </div>
      <div class="pp-status" id="pp-status"></div>
      <div class="pp-toolbar">
        <input class="pp-field" id="pp-search" type="search" placeholder="Search">
        <select class="pp-field" id="pp-provider"><option value="">Loading providers...</option></select>
        <select class="pp-field" id="pp-type"><option value="">All Types</option><option value="movie">Movies</option><option value="episode">TV Episodes</option><option value="anime_episode">Anime Episodes</option></select>
        <select class="pp-field" id="pp-progress"><option value="">All Progress</option><option value="0:24.99">Under 25 percent</option><option value="25:50">25 to 50 percent</option><option value="50:75">50 to 75 percent</option><option value="75:100">Over 75 percent</option><option value="90:100">Nearly Finished</option></select>
        <select class="pp-field" id="pp-age"><option value="">All Time</option><option value="today">Today</option><option value="7d">Last 7 Days</option><option value="30d">Last 30 Days</option><option value="older_30d">Older Than 30 Days</option></select>
        <select class="pp-field hidden" id="pp-rating"><option value="">All Ratings</option><option value="6">6 and Higher</option><option value="7">7 and Higher</option><option value="8">8 and Higher</option><option value="9">9 and Higher</option></select>
        <select class="pp-field" id="pp-sort"><option value="last_updated">Last Updated</option><option value="progress_high">Progress High</option><option value="progress_low">Progress Low</option><option value="remaining_time">Remaining Time</option><option value="rating_high">Rating High</option><option value="title">Title</option><option value="provider">Provider</option></select>
      </div>
      <div class="pp-errors hidden" id="pp-errors"></div>
      <div class="pp-loading-status" id="pp-loading-status" role="status" aria-live="polite"></div>
      <div class="pp-grid" id="pp-grid"></div>
      <div class="pp-pager" id="pp-pager"><button class="pp-btn" id="pp-prev">${icon("chevron_left")}</button><span id="pp-page-text"></span><button class="pp-btn" id="pp-next">${icon("chevron_right")}</button></div>
      <div class="pp-bulk hidden" id="pp-bulk"><div class="pp-bulk-summary"><span id="pp-selected-count" class="pp-selected-number">0</span><span class="pp-selected-copy"><strong>selected</strong><span>Select items to manage</span></span></div><div class="pp-bulk-buttons"><button class="pp-btn pp-bulk-choice" id="pp-select-visible">${icon("visibility")}<span>Select Visible</span></button><button class="pp-btn pp-bulk-choice" id="pp-select-all">${icon("format_list_bulleted")}<span>Select All Filtered Results</span></button><button class="pp-btn pp-bulk-choice pp-bulk-clear" id="pp-clear-selection">${icon("cancel")}<span>Clear Selection</span></button></div><div class="pp-bulk-actions"><button class="pp-btn pp-bulk-icon" id="pp-bulk-edit" title="Edit progress" aria-label="Edit progress">${icon("edit")}</button><button class="pp-btn pp-bulk-icon" id="pp-bulk-watch" title="Mark as watched" aria-label="Mark as watched">${icon("check_circle")}</button><button class="pp-btn pp-bulk-icon danger" id="pp-bulk-remove" title="Remove progress" aria-label="Remove progress">${icon("delete")}</button></div></div>
      <div class="pp-modal hidden" id="pp-progress-dialog" role="dialog" aria-modal="true" aria-labelledby="pp-progress-dialog-title">
        <div class="pp-dialog">
          <div><div class="pp-dialog-title" id="pp-progress-dialog-title">Edit Progress</div><div class="pp-dialog-sub" id="pp-progress-dialog-sub"></div></div>
          <div class="pp-progress-edit"><input id="pp-progress-range" type="range" min="2" max="99" step="1"><input id="pp-progress-value" type="number" min="2" max="99" step="0.01"></div>
          <div class="pp-dialog-error" id="pp-progress-error"></div>
          <div class="pp-dialog-actions"><button class="pp-btn" id="pp-progress-cancel">Cancel</button><button class="pp-btn" id="pp-progress-apply">Apply</button></div>
        </div>
      </div>
      <div class="pp-modal hidden" id="pp-settings-dialog" role="dialog" aria-modal="true" aria-labelledby="pp-settings-title">
        <div class="pp-dialog pp-settings-dialog cw-insight-set">
          <div class="cx-head">
            <div class="head-copy">
              <div class="head-title" id="pp-settings-title">Playback Progress Settings</div>
              <div class="head-sub">Choose which provider profiles appear on this screen.</div>
            </div>
            <div class="head-actions">
              <div class="head-chip"><span class="material-symbols-rounded" aria-hidden="true">stars</span><span id="pp-settings-head-chip">Preparing</span></div>
              <button class="close-btn" id="pp-settings-cancel" type="button">${icon("close")}<span>Close</span></button>
            </div>
          </div>
          <div class="body">
            <div class="layout">
              <section class="panel">
                <div class="panel-head">
                  <div>
                    <div class="panel-title">Playback Progress</div>
                    <div class="panel-sub">Refresh behavior</div>
                  </div>
                </div>
                <div class="panel-body">
                  <div class="settings-stack">
                    <label class="setting-card" for="pp-settings-timeout">
                      <span class="setting-icon">${icon("timer")}</span>
                      <span>
                        <span class="setting-name">Slow provider timeout</span>
                        <span class="setting-copy">Seconds</span>
                      </span>
                      <input id="pp-settings-timeout" type="number" min="3" max="60" step="1">
                    </label>
                  </div>
                </div>
              </section>
              <section class="panel providers-shell">
                <div class="panel-body">
                  <div class="pp-settings-list prov-grid" id="pp-settings-list"></div>
                </div>
              </section>
            </div>
          </div>
          <div class="pp-dialog-error" id="pp-settings-error"></div>
          <div class="actions">
            <div class="footer-note">${icon("info")}<span>These settings control which provider profiles appear on the playback progress screen.</span></div>
            <div class="action-row">
              <button class="btn danger" id="pp-settings-reset" type="button">${icon("restart_alt")}<span>Reset</span></button>
              <button class="btn good" id="pp-settings-save" type="button">${icon("check_circle")}<span>Apply</span></button>
            </div>
          </div>
        </div>
      </div>
      <div class="pp-toast hidden" id="pp-toast"></div>
    `;
  }

  const providerKey = (item) => `${item.provider}:${item.instance_id}`;
  const recordsOf = (item) => Array.isArray(item?.records) && item.records.length ? item.records : [item];
  const recordKey = (item) => item?.is_combined ? `combined:${item.remote_id || item.canonical_key || recordsOf(item).map((r) => `${providerKey(r)}:${r.remote_id}`).join("|")}` : `${providerKey(item)}:${item.remote_id}`;
  const fmtPct = (n) => Number.isFinite(Number(n)) ? `${Math.round(Number(n))}%` : "Unknown";
  const fmtRating = (n) => {
    const value = Number(n);
    if (!Number.isFinite(value) || value <= 0) return "";
    return Number.isInteger(value) ? String(value) : value.toFixed(1).replace(/\.0$/, "");
  };
  const fmtRemaining = (n) => {
    const s = Number(n);
    if (!Number.isFinite(s) || s <= 0) return "";
    const mins = Math.round(s / 60);
    return mins >= 60 ? `${Math.floor(mins / 60)}h ${mins % 60}m left` : `${mins}m left`;
  };
  const fmtDate = (v) => {
    const d = Date.parse(v || "");
    return Number.isFinite(d) ? new Date(d).toLocaleString() : "";
  };
  const fmtPaused = (v) => {
    const d = Date.parse(v || "");
    if (!Number.isFinite(d)) return "";
    const diff = Date.now() - d;
    if (diff >= 0) {
      const mins = Math.floor(diff / 60000);
      if (mins < 1) return "Paused just now";
      if (mins < 60) return `Paused ${mins} min ago`;
      const hours = Math.floor(mins / 60);
      if (hours < 24) return `Paused ${hours}h ago`;
      const days = Math.floor(hours / 24);
      if (days < 31) return `Paused ${days} day${days === 1 ? "" : "s"} ago`;
      const months = Math.floor(days / 30);
      if (months < 12) return `Paused ${months} month${months === 1 ? "" : "s"} ago`;
      const years = Math.floor(days / 365);
      return `Paused ${years} year${years === 1 ? "" : "s"} ago`;
    }
    return `Paused ${new Date(d).toLocaleDateString()}`;
  };
  const liveLabel = (it) => {
    const st = String(it?.live_state || "").toLowerCase();
    if (st === "playing") return "Playing now";
    if (st === "buffering") return "Buffering now";
    if (st === "paused") return fmtPaused((Number(it.live_updated) || 0) ? new Date(Number(it.live_updated) * 1000).toISOString() : "") || "Paused now";
    return "";
  };
  const titleOf = (it) => it.media_type === "movie" ? (it.title || "Untitled") : (it.series_title || it.title || "Untitled");
  const subOf = (it) => {
    if (it.media_type === "movie") return ["Movie", it.year].filter(Boolean).join(" - ");
    const ep = it.season != null && it.episode != null ? `S${String(it.season).padStart(2, "0")}E${String(it.episode).padStart(2, "0")}` : "";
    const type = it.media_type === "anime_episode" ? "Anime-Episode" : "TV-Episode";
    const label = [ep, it.episode_title].filter(Boolean).join(" - ");
    return label ? `${label} (${type})` : type;
  };
  const tmdbArtUrl = (it, size = "w342", kind = "poster") => {
    const media = String(it?.media_type || it?.type || "").toLowerCase();
    const meta = it?.provider_metadata && typeof it.provider_metadata === "object" ? it.provider_metadata : {};
    const showIds = meta.show_ids && typeof meta.show_ids === "object" ? meta.show_ids : {};
    const ids = it?.ids && typeof it.ids === "object" ? it.ids : {};
    const sharedTmdb = window.CW?.Meta?.tmdbId?.(it) || "";
    const tmdb = media === "movie"
      ? (it?.tmdb || it?.tmdb_id || ids?.tmdb || ids?.id || sharedTmdb)
      : (showIds?.tmdb || ids?.tmdb_show || it?.tmdb_show || ids?.show_tmdb || it?.show_tmdb || sharedTmdb || it?.tmdb);
    if (!tmdb) return "";
    const typ = media === "movie" ? "movie" : "tv";
    const evidenceTitle = media === "movie" ? it?.title : it?.series_title;
    const title = evidenceTitle ? `&title=${encodeURIComponent(String(evidenceTitle))}` : "";
    const year = media === "movie" && it?.year ? `&year=${encodeURIComponent(String(it.year))}` : "";
    return `/art/tmdb/${typ}/${encodeURIComponent(String(tmdb))}?kind=${encodeURIComponent(kind)}&size=${encodeURIComponent(size)}&locale=${encodeURIComponent(window.__CW_LOCALE || navigator.language || "en-US")}${title}${year}`;
  };
  const providerPills = (it) => {
    const providers = Array.isArray(it.providers) && it.providers.length
      ? it.providers
      : [{ provider: it.provider, provider_label: it.provider_label, instance_id: it.instance_id, instance_label: it.instance_label }];
    return providers.map((p) => {
      const label = compactProfileLabel(p);
      const title = [providerLabel(p.provider), label].filter(Boolean).join(" ");
      return `<span class="pp-provider-pill" title="${esc(title)}">${providerIcon(p.provider)}${label ? esc(label) : ""}</span>`;
    }).join("");
  };
  const profileLabel = (p) => {
    const provider = String(p.provider || "");
    const providerLabel = String(p.provider_label || provider);
    let label = String(p.instance_label || p.instance_id || "").trim();
    for (const prefix of [providerLabel, provider]) {
      if (prefix && label.toLowerCase().startsWith(prefix.toLowerCase())) {
        label = label.slice(prefix.length).trim();
      }
    }
    return label || (String(p.instance_id || "").trim() || "Default");
  };
  const compactProfileLabel = (p) => {
    const id = String(p?.instance_id || "default").trim() || "default";
    if (id.toLowerCase() === "default") return "";
    const label = profileLabel(p);
    for (const value of [label, id]) {
      const match = String(value || "").trim().match(/^(?:profile[\s_-]*)?p?0*(\d{1,2})$/i)
        || String(value || "").trim().match(/(?:^|[\s_-])(?:profile[\s_-]*)?p?0*(\d{1,2})$/i);
      if (match) return `P${String(match[1]).padStart(2, "0")}`;
    }
    return label;
  };
  const settingsProviderOrder = (provider) => {
    const p = String(provider || "").toLowerCase();
    const idx = PLAYBACK_PROVIDER_KEYS.indexOf(p);
    return idx >= 0 ? idx : PLAYBACK_PROVIDER_KEYS.length + p.charCodeAt(0);
  };
  const settingsProfileKey = (p) => `${String(p.provider || "").toLowerCase()}:${String(p.instance_id || "default")}`;
  const settingsProfileLabel = (p) => {
    const id = String(p.instance_id || "default");
    if (id === "default") return "Default";
    if (String(p.provider || "").toLowerCase() === "crosswatch" && /^CW-P\d+$/i.test(id)) return id.toUpperCase();
    return String(p.instance_label || id);
  };
  const settingsProviderCard = (provider, profiles) => {
    const key = String(provider || "").toLowerCase();
    const label = providerLabel(key);
    const rows = (Array.isArray(profiles) ? profiles : []).filter((p) => p.configured && p.read);
    if (!rows.length) return "";
    const selected = rows.filter((p) => p.included !== false).length;
    const logo = providerLogo(key);
    const titleBadge = rows.length > 1
      ? `<span class="prov-badge" data-badge>${selected}/${rows.length}</span><button class="mini" type="button" data-settings-all>All</button><button class="mini" type="button" data-settings-none>None</button>`
      : "";
    return `<section class="prov-card" data-provider="${esc(key)}" data-single="${rows.length === 1 ? 1 : 0}" style="--provider-rgb:${esc(providerTone(key))};--provider-wm:url(&quot;${esc(logo)}&quot;)">
      <div class="prov-top">
        <div class="prov-brand">
          <span class="prov-icon"><img src="${esc(logo)}" alt="${esc(label)} logo" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('span'),{className:'prov-icon-fallback',textContent:'${esc((label || key || "?").slice(0, 2).toUpperCase())}'}))"></span>
          <div class="prov-title">${esc(label)}</div>
        </div>
        <div class="prov-tools">${titleBadge}</div>
      </div>
      <div data-list>${rows.map((p) => {
        const checked = p.included !== false;
        const display = settingsProfileLabel(p);
        return `<label class="pill" for="pp-set-${esc(settingsProfileKey(p).replace(/[^a-z0-9_-]+/gi, "-"))}">
          <input type="checkbox" id="pp-set-${esc(settingsProfileKey(p).replace(/[^a-z0-9_-]+/gi, "-"))}" data-key="${esc(settingsProfileKey(p))}" data-provider="${esc(p.provider || key)}" data-instance="${esc(p.instance_id || "default")}" data-included="${p.included !== false ? "true" : "false"}"${checked ? " checked" : ""}>
          <span class="lab"><span>${esc(display)}</span><span class="material-symbols-rounded" aria-hidden="true">check</span></span>
        </label>`;
      }).join("")}</div>
    </section>`;
  };
  const updateSettingsCard = (card) => {
    if (!card) return;
    const checks = [...card.querySelectorAll("input[type=checkbox][data-provider]")];
    const badge = card.querySelector("[data-badge]");
    const selected = checks.filter((el) => !el.disabled && el.checked).length;
    if (badge) badge.textContent = `${selected}/${checks.length}`;
    card.dataset.empty = selected ? "0" : "1";
  };
  const renderSettingsProfiles = (data) => {
    const profiles = Array.isArray(data?.profiles) ? data.profiles : [];
    const byProvider = new Map();
    profiles.forEach((p) => {
      const provider = String(p.provider || "").toLowerCase();
      if (!provider) return;
      if (!byProvider.has(provider)) byProvider.set(provider, []);
      byProvider.get(provider).push(p);
    });
    const hidden = profiles
      .filter((p) => !(p.configured && p.read))
      .map((p) => `<input type="checkbox" hidden disabled data-provider="${esc(p.provider || "")}" data-instance="${esc(p.instance_id || "default")}" data-included="${p.included !== false ? "true" : "false"}">`)
      .join("");
    const cards = [...byProvider.entries()]
      .sort((a, b) => settingsProviderOrder(a[0]) - settingsProviderOrder(b[0]) || a[0].localeCompare(b[0]))
      .map(([provider, rows]) => settingsProviderCard(provider, rows))
      .filter(Boolean)
      .join("");
    return cards ? `${cards}${hidden}` : `<div class="loading">No connected playback progress providers were found.</div>${hidden}`;
  };
  const updateSettingsCount = () => {
    const cards = [...document.querySelectorAll("#pp-settings-list .prov-card")];
    const chip = document.getElementById("pp-settings-head-chip");
    if (chip) chip.textContent = `${cards.length} provider${cards.length === 1 ? "" : "s"}`;
    cards.forEach(updateSettingsCard);
  };
  const resetSettingsDraft = () => {
    const timeout = document.getElementById("pp-settings-timeout");
    if (timeout) timeout.value = String(DEFAULT_PROVIDER_TIMEOUT_SECONDS);
    document.querySelectorAll("#pp-settings-list input[type=checkbox][data-provider]").forEach((el) => {
      el.checked = !el.disabled;
      if (el.disabled) el.dataset.included = "false";
    });
    updateSettingsCount();
  };
  const actionRecords = (item, action) => recordsOf(item).filter((it) => {
    if (action === "mark_watched") return it.can_mark_watched;
    if (action === "update_progress") return it.can_update_progress;
    return it.can_remove_progress;
  });
  const actionPayloads = (items, action) => items.flatMap((it) => actionRecords(it, action).map((record) => ({
    provider: record.provider,
    instance_id: record.instance_id,
    remote_id: record.remote_id,
    canonical_key: record.canonical_key,
    record
  })));
  const editableMaxExclusive = (records) => {
    const values = records.map((it) => Number(it.editable_progress_max_exclusive || 100)).filter((v) => Number.isFinite(v) && v > 2);
    return values.length ? Math.min(...values, 100) : 100;
  };
  const progressSliderMax = (maxExclusive) => Math.max(2, Math.ceil(Math.min(Number(maxExclusive) || 100, 100)) - 1);
  const progressMaxLabel = (maxExclusive) => {
    const n = Number(maxExclusive);
    return Number.isFinite(n) ? String(Math.round(n * 100) / 100) : "100";
  };
  const avgProgress = (items, maxExclusive = 100) => {
    const values = items.flatMap((it) => recordsOf(it)).map((it) => Number(it.progress_percent)).filter(Number.isFinite);
    const sliderMax = progressSliderMax(maxExclusive);
    if (!values.length) return Math.min(25, sliderMax);
    return Math.max(2, Math.min(sliderMax, Math.round(values.reduce((a, b) => a + b, 0) / values.length)));
  };
  const actionTitle = (action) => action === "mark_watched" ? "Mark as Watched" : action === "update_progress" ? "Edit Progress" : "Remove Progress";

  function toast(msg) {
    const el = document.getElementById("pp-toast");
    if (!el) return;
    el.textContent = msg;
    el.classList.remove("hidden");
    clearTimeout(toast._t);
    toast._t = setTimeout(() => el.classList.add("hidden"), 2400);
  }

  function askProgress(defaultValue, count, maxExclusive = 100) {
    return new Promise((resolve) => {
      const dlg = document.getElementById("pp-progress-dialog");
      const range = document.getElementById("pp-progress-range");
      const value = document.getElementById("pp-progress-value");
      const sub = document.getElementById("pp-progress-dialog-sub");
      const err = document.getElementById("pp-progress-error");
      const apply = document.getElementById("pp-progress-apply");
      const cancel = document.getElementById("pp-progress-cancel");
      if (!dlg || !range || !value || !sub || !err || !apply || !cancel) return resolve(null);
      const upper = Math.max(3, Math.min(Number(maxExclusive) || 100, 100));
      const sliderMax = progressSliderMax(upper);
      const initial = Math.max(2, Math.min(sliderMax, Math.round(Number(defaultValue) || 25)));
      range.max = String(sliderMax);
      value.max = String(Math.round((upper - 0.01) * 100) / 100);
      range.value = String(initial);
      value.value = String(initial);
      sub.textContent = `${count || 1} provider record${count === 1 ? "" : "s"}`;
      err.textContent = "";
      dlg.classList.remove("hidden");
      value.focus();
      value.select?.();
      const syncFromRange = () => { value.value = range.value; err.textContent = ""; };
      const syncFromValue = () => { range.value = String(Math.max(2, Math.min(sliderMax, Math.round(Number(value.value) || initial)))); err.textContent = ""; };
      const done = (result) => {
        dlg.classList.add("hidden");
        range.removeEventListener("input", syncFromRange);
        value.removeEventListener("input", syncFromValue);
        apply.removeEventListener("click", onApply);
        cancel.removeEventListener("click", onCancel);
        dlg.removeEventListener("click", onBackdrop);
        dlg.removeEventListener("keydown", onKey);
        resolve(result);
      };
      const onApply = () => {
        const n = Number(value.value);
        if (!Number.isFinite(n) || n < 2 || n >= upper) {
          err.textContent = `Use at least 2% and below ${progressMaxLabel(upper)}%. Use Watched for completed items.`;
          return;
        }
        done(Math.round(n * 100) / 100);
      };
      const onCancel = () => done(null);
      const onBackdrop = (e) => { if (e.target === dlg) done(null); };
      const onKey = (e) => {
        if (e.key === "Escape") done(null);
        if (e.key === "Enter") onApply();
      };
      range.addEventListener("input", syncFromRange);
      value.addEventListener("input", syncFromValue);
      apply.addEventListener("click", onApply);
      cancel.addEventListener("click", onCancel);
      dlg.addEventListener("click", onBackdrop);
      dlg.addEventListener("keydown", onKey);
    });
  }

  async function openSettings() {
    const dlg = document.getElementById("pp-settings-dialog");
    const list = document.getElementById("pp-settings-list");
    const timeout = document.getElementById("pp-settings-timeout");
    const err = document.getElementById("pp-settings-error");
    if (!dlg || !list || !timeout || !err) return;
    err.textContent = "";
    list.innerHTML = `<div class="loading">Loading provider profiles...</div>`;
    dlg.classList.remove("hidden");
    const data = await api("/api/playback_progress/settings");
    state.settings = data;
    timeout.value = String(Math.round(Number(data.provider_timeout_seconds || DEFAULT_PROVIDER_TIMEOUT_SECONDS)));
    list.innerHTML = renderSettingsProfiles(data);
    updateSettingsCount();
  }

  function closeSettings() {
    document.getElementById("pp-settings-dialog")?.classList.add("hidden");
  }

  async function saveSettings() {
    const dlg = document.getElementById("pp-settings-dialog");
    const list = document.getElementById("pp-settings-list");
    const timeout = document.getElementById("pp-settings-timeout");
    const err = document.getElementById("pp-settings-error");
    if (!dlg || !list || !timeout || !err) return;
    const n = Number(timeout.value);
    if (!Number.isFinite(n) || n < 3 || n > 60) {
      err.textContent = "Use a timeout between 3 and 60 seconds.";
      return;
    }
    const profiles = [...list.querySelectorAll("input[type=checkbox][data-provider]")].map((el) => ({
      provider: el.dataset.provider,
      instance_id: el.dataset.instance,
      included: el.disabled ? el.dataset.included !== "false" : el.checked
    }));
    const res = await api("/api/playback_progress/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ profiles, provider_timeout_seconds: n })
    });
    if (!res.ok) {
      err.textContent = res.message || res.error || "Settings could not be saved.";
      return;
    }
    closeSettings();
    toast("Playback Progress settings saved");
    state.selected.clear();
    await load(true);
  }

  function providerOptions() {
    const readable = state.providers.filter((p) => p.read && p.configured && p.included !== false);
    const opts = ['<option value="">All Providers</option>'];
    readable.forEach((p) => {
      const label = compactProfileLabel(p);
      const provider = String(p.provider || "");
      const instance = String(p.instance_id || "default");
      opts.push(`<option value="${esc(provider)}:${esc(instance)}" data-provider="${esc(provider)}" data-profile-label="${esc(label)}">${esc(label || providerLabel(provider))}</option>`);
    });
    const select = document.getElementById("pp-provider");
    select.innerHTML = opts.join("");
    const cur = state.filters.provider;
    if ([...select.options].some((o) => o.value === cur)) select.value = cur;
    window.CW?.IconSelect?.enhance?.(select, {
      className: "cw-plain-select",
      getOptionData: (value, option) => {
        if (!value) return { label: "All Providers" };
        const provider = option?.dataset?.provider || String(value).split(":")[0];
        const label = option?.dataset?.profileLabel || "";
        return {
          label: label || "Default",
          icons: [{ src: providerLogLogo(provider), alt: providerLabel(provider) }]
        };
      }
    });
  }

  function loadingCard() {
    return `<article class="pp-card pp-loading-card" aria-hidden="true">
      <div class="pp-art pp-loading-art pp-loading-shape"></div>
      <div class="pp-body">
        <div class="pp-top"><div class="pp-card-head">
          <div class="pp-title-wrap pp-loading-copy"><span class="pp-loading-line title pp-loading-shape"></span><span class="pp-loading-line meta pp-loading-shape"></span></div>
          <div class="pp-card-side"><span class="pp-loading-chip pp-loading-shape"></span></div>
        </div></div>
        <div class="pp-progress pp-loading-progress"><div class="pp-progress-row"><span class="pp-loading-line pp-loading-shape"></span></div><span class="pp-loading-bar pp-loading-shape"></span></div>
        <div class="pp-actions pp-loading-actions"><span class="pp-loading-action pp-loading-shape"></span><span class="pp-loading-action pp-loading-shape"></span></div>
      </div>
    </article>`;
  }

  function renderInitialLoading() {
    const grid = document.getElementById("pp-grid");
    const status = document.getElementById("pp-loading-status");
    document.getElementById("pp-errors")?.classList.add("hidden");
    document.getElementById("pp-bulk")?.classList.add("hidden");
    if (status) status.textContent = "Refreshing Playback Progress from configured providers.";
    if (grid) {
      grid.classList.add("pp-loading-grid");
      grid.innerHTML = Array.from({ length: 6 }, loadingCard).join("");
    }
  }

  function fmtSyncTime(value) {
    if (!value) return "not yet";
    const diff = Math.max(0, Date.now() - Number(value));
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins} min ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.floor(hours / 24);
    return `${days} day${days === 1 ? "" : "s"} ago`;
  }

  function updateSyncStatus() {
    const wrap = document.getElementById("pp-sync-status");
    const time = document.getElementById("pp-sync-time");
    if (!wrap || !time) return;
    const status = state.busy ? "refreshing" : state.lastRefreshFailed ? "failed" : "ready";
    wrap.dataset.state = status;
    time.textContent = fmtSyncTime(state.lastSyncAt);
    time.title = state.lastSyncAt ? new Date(Number(state.lastSyncAt)).toLocaleString() : "";
    wrap.title = status === "failed" ? "Latest refresh failed" : "";
  }

  function startSyncClock() {
    if (state.syncClock) return;
    state.syncClock = setInterval(updateSyncStatus, 30000);
  }

  function setLoadingState(loading, initial = false) {
    const el = root();
    if (!el) return;
    el.classList.toggle("is-loading", loading);
    el.classList.toggle("is-initial-loading", loading && initial);
    loading ? el.setAttribute("aria-busy", "true") : el.removeAttribute("aria-busy");
    el.querySelectorAll(".pp-toolbar .pp-field").forEach((field) => { field.disabled = loading; });
    const refresh = document.getElementById("pp-refresh");
    if (refresh) refresh.disabled = loading;
    if (!loading) {
      const status = document.getElementById("pp-loading-status");
      if (status) status.textContent = "";
    }
    updateSyncStatus();
  }

  function renderStatus() {
    const wrap = document.getElementById("pp-status");
    const configured = state.providers.filter((p) => p.configured && p.read);
    const readable = configured.filter((p) => p.included !== false);
    if (readable.length) {
      wrap.classList.add("hidden");
      wrap.innerHTML = "";
      return;
    }
    wrap.innerHTML = `<div class="pp-status-message">${configured.length ? "Enable at least one provider profile in Playback Progress settings." : "Connect at least one compatible provider to view playback progress."}</div>`;
    wrap.classList.remove("hidden");
  }

  function errorProviderName(e) {
    const provider = String(e.provider || "").trim();
    const label = String(e.provider_label || providerLabel(provider)).trim();
    const instance = String(e.instance_label || e.instance_id || "").trim();
    if (!instance || instance.toLowerCase() === "default" || instance.toLowerCase() === label.toLowerCase()) return label || "Provider";
    return `${label || "Provider"} - ${instance}`;
  }

  function errorTitle(e) {
    const code = String(e.error_code || "").toLowerCase();
    if (code === "provider_timeout") return "Provider timed out";
    if (code === "provider_unavailable") return "Provider unavailable";
    if (code === "not_configured") return "Connection needs attention";
    if (code.startsWith("http:")) return `Remote service returned ${code.slice(5)}`;
    if (e.retryable) return "Provider could not refresh";
    return "Provider notice";
  }

  function errorItem(e) {
    const provider = String(e.provider || "").trim();
    const status = e.remote_status ? `HTTP ${e.remote_status}` : String(e.error_code || "").replace(/_/g, " ");
    const retry = e.retryable ? `<span class="pp-error-chip">Retryable</span>` : "";
    return `<article class="pp-error-item">
      <span class="pp-error-logo">${provider ? `<img src="${esc(providerLogLogo(provider))}" alt="" onerror="this.remove()">` : icon("warning")}</span>
      <span class="pp-error-main">
        <span class="pp-error-name">${esc(errorProviderName(e))}</span>
        <span class="pp-error-message"><strong>${esc(errorTitle(e))}.</strong> ${esc(e.message || e.error_code || "Playback progress could not be refreshed.")}</span>
        <span class="pp-error-meta">${status ? `<span class="pp-error-chip">${esc(status)}</span>` : ""}${retry}</span>
      </span>
    </article>`;
  }

  function renderErrors() {
    const el = document.getElementById("pp-errors");
    if (!state.errors.length) return el.classList.add("hidden");
    const count = state.errors.length;
    const partial = state.items.length > 0;
    el.innerHTML = `<div class="pp-error-head">
      <div><div class="pp-error-title">${icon(partial ? "sync_problem" : "error")}<span>${partial ? "Some providers could not refresh" : "Playback Progress could not refresh"}</span></div><div class="pp-error-copy">${partial ? "Showing available records from the providers that responded." : "Check the provider connection or try again in a moment."}</div></div>
      <div class="pp-error-actions"><button class="pp-btn" type="button" data-pp-error-action="settings">${icon("settings")}Settings</button><button class="pp-btn" type="button" data-pp-error-action="retry">${icon("refresh")}Retry</button></div>
    </div><div class="pp-error-list">${state.errors.slice(0, 6).map(errorItem).join("")}</div>${count > 6 ? `<div class="pp-error-copy">${esc(count - 6)} more provider notice${count - 6 === 1 ? "" : "s"} hidden.</div>` : ""}`;
    el.classList.remove("hidden");
  }

  function card(it) {
    const key = recordKey(it);
    const selected = state.selected.has(key) ? " selected" : "";
    const displayProgress = Number.isFinite(Number(it.live_progress_percent)) ? it.live_progress_percent : it.progress_percent;
    const pct = Math.max(0, Math.min(100, Number(displayProgress || 0)));
    const remaining = fmtRemaining(Number.isFinite(Number(it.live_remaining_seconds)) ? it.live_remaining_seconds : it.remaining_seconds);
    const directPoster = String(it.poster_url || "").trim();
    const metadataPoster = tmdbArtUrl(it, "w342");
    const posterImg = directPoster || metadataPoster || "/assets/img/placeholder_poster.svg";
    const posterFallback = directPoster && metadataPoster && directPoster !== metadataPoster ? metadataPoster : "";
    const posterFallbackAttr = posterFallback ? ` data-fallback-src="${esc(posterFallback)}"` : "";
    const posterKey = `${posterImg}|${posterFallback}`;
    const backdropImg = it.backdrop_url || tmdbArtUrl(it, "w780", "backdrop");
    const backdropStyle = backdropImg ? ` style="--pp-backdrop:url('${esc(backdropImg)}')"` : "";
    const live = liveLabel(it);
    const paused = live || fmtPaused(it.updated_at || it.progress_at);
    const ratingText = fmtRating(it.rating);
    const ratingChip = ratingText ? `<span class="pp-rating-chip" title="Rating ${esc(ratingText)}" aria-label="Rating ${esc(ratingText)}">${icon("star")}${esc(ratingText)}</span>` : "";
    const timing = [remaining ? `<span>${esc(remaining)}</span>` : "", paused ? `<span class="${live ? "pp-live" : "pp-paused"}">${esc(paused)}</span>` : ""].filter(Boolean).join("");
    const actionWatch = it.can_mark_watched ? `<button class="pp-btn pp-action-btn pp-action-watch" data-action="watch" data-key="${esc(key)}" title="Mark as watched" aria-label="Mark as watched">${icon("check_circle")}<span>Watched</span></button>` : "";
    const actionEdit = it.can_update_progress ? `<button class="pp-btn pp-action-btn pp-action-edit" data-action="edit" data-key="${esc(key)}" title="Edit progress" aria-label="Edit progress">${icon("edit")}<span>Edit</span></button>` : "";
    const actionRemove = it.can_remove_progress ? `<button class="pp-btn pp-action-btn pp-action-remove" data-action="remove" data-key="${esc(key)}" title="Remove progress" aria-label="Remove progress">${icon("delete")}<span>Remove</span></button>` : "";
    return `<article class="pp-card${selected}" data-key="${esc(key)}" role="checkbox" aria-checked="${state.selected.has(key) ? "true" : "false"}" tabindex="0"${backdropStyle}>
      <div class="pp-art"><img src="${esc(posterImg)}"${posterFallbackAttr} data-poster-key="${esc(posterKey)}" alt="" loading="lazy" decoding="async" onerror="const fallback=this.dataset.fallbackSrc;if(fallback){delete this.dataset.fallbackSrc;this.src=fallback}else{this.onerror=null;this.src='/assets/img/placeholder_poster.svg'}"></div>
      <div class="pp-body">
        <div class="pp-top">
          <div class="pp-card-head">
            <div class="pp-title-wrap"><div class="pp-card-title">${esc(titleOf(it))}</div><div class="pp-card-sub">${esc(subOf(it))}</div></div>
            <div class="pp-card-side"><div class="pp-provider-stack">${providerPills(it)}</div>${ratingChip}</div>
          </div>
        </div>
        <div class="pp-progress"><div class="pp-progress-row"><strong>${esc(fmtPct(displayProgress))} watched</strong><span class="pp-timing">${timing}</span></div><div class="pp-bar"><span style="width:${pct}%"></span></div></div>
        <div class="pp-actions">${actionEdit}${actionWatch}${actionRemove}</div>
      </div>
    </article>`;
  }

  function updateBulk() {
    const bulk = document.getElementById("pp-bulk");
    const count = state.selected.size;
    document.getElementById("pp-selected-count").textContent = String(count);
    bulk.classList.toggle("hidden", count === 0);
  }

  function renderItems(preserveArtwork = true) {
    const grid = document.getElementById("pp-grid");
    grid.classList.remove("pp-loading-grid");
    const ratingFilter = document.getElementById("pp-rating");
    ratingFilter.classList.toggle("hidden", !state.items.some((it) => Number(it.rating) > 0));
    const markup = state.items.length ? state.items.map(card).join("") : `<div class="pp-empty">No playback records found.</div>`;
    if (preserveArtwork && state.items.length && grid.children.length) {
      const existing = new Map(
        [...grid.querySelectorAll(".pp-card[data-key]")].map((cardEl) => [cardEl.dataset.key, cardEl])
      );
      const template = document.createElement("template");
      template.innerHTML = markup;
      template.content.querySelectorAll(".pp-card[data-key]").forEach((nextCard) => {
        const currentCard = existing.get(nextCard.dataset.key);
        const currentImg = currentCard?.querySelector(".pp-art img[data-poster-key]");
        const nextImg = nextCard.querySelector(".pp-art img[data-poster-key]");
        if (currentImg && nextImg && currentImg.dataset.posterKey === nextImg.dataset.posterKey) {
          nextImg.replaceWith(currentImg);
        }
      });
      grid.replaceChildren(template.content);
    } else {
      grid.innerHTML = markup;
    }
    const maxPage = Math.max(1, Math.ceil((state.total || 0) / state.pageSize));
    document.getElementById("pp-page-text").textContent = `${state.page} / ${maxPage} - ${state.total} total`;
    document.getElementById("pp-prev").disabled = state.page <= 1;
    document.getElementById("pp-next").disabled = state.page >= maxPage;
    updateBulk();
  }

  function query(force = false, all = false) {
    const params = new URLSearchParams();
    const [provider, instance] = String(state.filters.provider || "").split(":");
    if (provider) params.set("provider", provider);
    if (instance) params.set("instance_id", instance);
    if (state.filters.media_type) params.set("media_type", state.filters.media_type);
    if (state.filters.age) params.set("age", state.filters.age);
    if (state.filters.rating) params.set("rating_min", state.filters.rating);
    if (state.filters.search) params.set("search", state.filters.search);
    if (state.filters.sort) params.set("sort", state.filters.sort);
    if (state.filters.progress) {
      const [min, max] = state.filters.progress.split(":");
      if (min) params.set("progress_min", min);
      if (max) params.set("progress_max", max);
    }
    params.set("page", all ? "1" : String(state.page));
    params.set("page_size", all ? "250" : String(state.pageSize));
    if (force) params.set("force_refresh", "true");
    return params;
  }

  async function load(force = false) {
    if (state.busy) return;
    const initial = !state.loaded;
    state.busy = true;
    setLoadingState(true, initial);
    if (initial) renderInitialLoading();
    try {
      const data = await api(`/api/playback_progress/items?${query(force).toString()}`);
      state.items = Array.isArray(data.items) ? data.items : [];
      state.providers = Array.isArray(data.providers) ? data.providers : [];
      state.errors = Array.isArray(data.errors) ? data.errors : [];
      if (data.ok === false && !state.errors.length) {
        state.errors = [{ provider: "playback_progress", provider_label: "Playback Progress", message: data.message || data.error || "Playback Progress request failed.", retryable: true }];
      }
      state.lastRefreshFailed = data.ok === false;
      if (!state.lastRefreshFailed) state.lastSyncAt = Date.now();
      state.total = Number(data.total || 0);
      state.page = Number(data.page || 1);
      providerOptions();
      renderStatus();
      renderErrors();
      renderItems(!force);
      state.loaded = true;
    } catch (e) {
      state.items = [];
      state.errors = [{ provider: "Playback Progress", message: String(e?.message || e || "Request failed") }];
      state.lastRefreshFailed = true;
      state.total = 0;
      const status = document.getElementById("pp-status");
      if (status) {
        status.innerHTML = "";
        status.classList.add("hidden");
      }
      renderErrors();
      renderItems();
    } finally {
      state.busy = false;
      setLoadingState(false);
    }
  }

  async function act(action, item) {
    const bulkAction = action === "watch" ? "mark_watched" : action === "edit" ? "update_progress" : "remove_progress";
    const payloads = actionPayloads([item], bulkAction);
    let progressPercent = null;
    if (bulkAction === "update_progress") {
      if (!payloads.length) return toast("Edit Progress is unsupported for this record.");
      const editableRecords = payloads.map((payload) => payload.record);
      const maxExclusive = editableMaxExclusive(editableRecords);
      progressPercent = await askProgress(avgProgress(editableRecords, maxExclusive), payloads.length, maxExclusive);
      if (progressPercent == null) return;
    }
    if (payloads.length > 1 || item.is_combined) {
      if (!payloads.length) return toast("Action is unsupported for this record.");
      const res = await api("/api/playback_progress/actions/bulk", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: bulkAction, progress_percent: progressPercent, items: payloads }) });
      toast(`Successful ${res.successful || 0}, failed ${res.failed || 0}, unsupported ${res.unsupported || 0}`);
      if ((res.successful || 0) > 0) {
        state.selected.delete(recordKey(item));
        await load(true);
      }
      return;
    }
    const url = action === "watch" ? "/api/playback_progress/actions/mark_watched" : action === "edit" ? "/api/playback_progress/actions/update_progress" : "/api/playback_progress/actions/remove";
    const record = recordsOf(item)[0] || item;
    const body = { provider: record.provider, instance_id: record.instance_id, remote_id: record.remote_id, canonical_key: record.canonical_key, progress_percent: progressPercent, record };
    const res = await api(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    toast(res.message || (res.ok ? "Done" : "Action failed"));
    if (res.ok) {
      state.selected.delete(recordKey(item));
      await load(true);
    }
  }

  async function bulk(action) {
    const selected = [...state.selected.values()];
    if (!selected.length) return;
    const allRecords = selected.flatMap((it) => recordsOf(it));
    const payloads = actionPayloads(selected, action);
    const unsupported = allRecords.length - payloads.length;
    if (!payloads.length) return toast(`${actionTitle(action)} is unsupported for the selected records.`);
    let progressPercent = null;
    if (action === "update_progress") {
      const editableRecords = payloads.map((payload) => payload.record);
      const maxExclusive = editableMaxExclusive(editableRecords);
      progressPercent = await askProgress(avgProgress(editableRecords, maxExclusive), payloads.length, maxExclusive);
      if (progressPercent == null) return;
    } else if (!confirm(`${actionTitle(action)} for ${payloads.length} eligible provider record(s)? ${unsupported ? `${unsupported} unsupported provider record(s) will be skipped.` : ""}`)) return;
    const res = await api("/api/playback_progress/actions/bulk", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action, progress_percent: progressPercent, items: payloads }) });
    toast(`Successful ${res.successful || 0}, failed ${res.failed || 0}, unsupported ${res.unsupported || 0}`);
    state.selected.clear();
    await load(true);
  }

  function bind() {
    const r = root();
    const update = (key, val) => { state.filters[key] = val; state.page = 1; load(false); };
    r.addEventListener("change", (e) => {
      const t = e.target;
      if (!t) return;
      if (t.id === "pp-provider") update("provider", t.value);
      if (t.id === "pp-type") update("media_type", t.value);
      if (t.id === "pp-progress") update("progress", t.value);
      if (t.id === "pp-age") update("age", t.value);
      if (t.id === "pp-rating") update("rating", t.value);
      if (t.id === "pp-sort") update("sort", t.value);
      if (t.matches?.("#pp-settings-list input[type=checkbox][data-provider]")) updateSettingsCard(t.closest(".prov-card"));
    }, true);
    r.addEventListener("input", (e) => {
      if (e.target?.id !== "pp-search") return;
      clearTimeout(bind._search);
      bind._search = setTimeout(() => update("search", e.target.value), 180);
    }, true);
    r.addEventListener("click", async (e) => {
      const btn = e.target?.closest?.("button");
      if (btn) {
        if (btn.dataset?.ppErrorAction === "retry") return load(true);
        if (btn.dataset?.ppErrorAction === "settings") return openSettings();
        if (btn.id === "pp-settings") return openSettings();
        if (btn.id === "pp-settings-cancel") return closeSettings();
        if (btn.id === "pp-settings-reset") { resetSettingsDraft(); return; }
        if (btn.id === "pp-settings-save") return saveSettings();
        if (btn.hasAttribute("data-settings-all") || btn.hasAttribute("data-settings-none")) {
          const card = btn.closest(".prov-card");
          card?.querySelectorAll("input[type=checkbox][data-provider]:not(:disabled)").forEach((el) => { el.checked = btn.hasAttribute("data-settings-all"); });
          updateSettingsCard(card);
          return;
        }
        if (btn.id === "pp-refresh") return load(true);
        if (btn.id === "pp-prev" && state.page > 1) { state.page--; return load(false); }
        if (btn.id === "pp-next") { state.page++; return load(false); }
        if (btn.id === "pp-select-visible") { state.items.forEach((it) => state.selected.set(recordKey(it), it)); renderItems(); return; }
        if (btn.id === "pp-clear-selection") { state.selected.clear(); renderItems(); return; }
        if (btn.id === "pp-select-all") {
          const data = await api(`/api/playback_progress/items?${query(false, true).toString()}`);
          (data.items || []).forEach((it) => state.selected.set(recordKey(it), it));
          renderItems();
          return;
        }
        if (btn.id === "pp-bulk-watch") return bulk("mark_watched");
        if (btn.id === "pp-bulk-edit") return bulk("update_progress");
        if (btn.id === "pp-bulk-remove") return bulk("remove_progress");
        if (btn.dataset?.action && btn.dataset?.key) {
          const item = state.items.find((it) => recordKey(it) === btn.dataset.key);
          if (item) return act(btn.dataset.action, item);
        }
        return;
      }
      const settingsDialog = e.target?.closest?.("#pp-settings-dialog");
      if (settingsDialog && e.target === settingsDialog) {
        closeSettings();
        return;
      }
      const card = e.target?.closest?.(".pp-card[data-key]");
      if (card) {
        const item = state.items.find((it) => recordKey(it) === card.dataset.key);
        if (!item) return;
        if (state.selected.has(card.dataset.key)) state.selected.delete(card.dataset.key);
        else state.selected.set(card.dataset.key, item);
        renderItems();
      }
    }, true);
    r.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !document.getElementById("pp-settings-dialog")?.classList.contains("hidden")) {
        closeSettings();
        return;
      }
      if (e.key !== " " && e.key !== "Enter") return;
      const card = e.target?.closest?.(".pp-card[data-key]");
      if (!card) return;
      e.preventDefault();
      const item = state.items.find((it) => recordKey(it) === card.dataset.key);
      if (!item) return;
      if (state.selected.has(card.dataset.key)) state.selected.delete(card.dataset.key);
      else state.selected.set(card.dataset.key, item);
      renderItems();
    }, true);
  }

  function mount() {
    const el = root();
    if (!el) return;
    if (!state.mounted) {
      el.innerHTML = shell();
      bind();
      state.mounted = true;
    }
    startSyncClock();
    load(false);
  }

  window.PlaybackProgress = { mount, refresh: () => load(true) };
  document.addEventListener("tab-changed", (e) => {
    if ((e.detail?.id || e.detail?.tab) === "playback_progress") mount();
  });
  window.addEventListener("currently-watching-updated", () => {
    const page = document.getElementById("page-playback_progress");
    if (page && !page.classList.contains("hidden")) load(false);
  });
  const mountIfActive = () => {
    const page = document.getElementById("page-playback_progress");
    const tab = document.getElementById("tab-playback_progress");
    if (page && (!page.classList.contains("hidden") || tab?.classList.contains("active"))) mount();
  };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mountIfActive, { once: true });
  else mountIfActive();
})();
