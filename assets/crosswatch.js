/* assets/crosswatch.js */
/* CrossWatch main JavaScript file: UI utilities, tab handling, and bootstrap logic. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function () {
  'use strict';

  const CW = window.CW || {};
  const DOM = CW.DOM || {};
  const Events = CW.Events || {};
  const D = document;

  // Tabs
  function showTab(id) {
    try {
      D.querySelectorAll("#page-main, #page-settings, .tab-page")
        .forEach(el => el.classList.add("hidden"));

      const tgt = D.getElementById("page-" + id) || D.getElementById(id);
      if (tgt) tgt.classList.remove("hidden");

      ["main", "snapshots", "playlists", "editor", "settings"].forEach(n => {
        const th = D.getElementById("tab-" + n);
        if (th) th.classList.toggle("active", n === id);
      });

      D.dispatchEvent(new CustomEvent("tab-changed", { detail: { id } }));
      Events.emit?.("tab:changed", { id });
    } catch (e) {
      console.warn("[crosswatch] showTab failed", e);
    }
  }
  if (typeof window.showTab !== "function") window.showTab = showTab;

  function _cwUpdateHeaderHeight() {
    try {
      const header = D.querySelector("header");
      if (!header) return;
      const h = Math.ceil(header.getBoundingClientRect().height || 0);
      if (h > 0) document.documentElement.style.setProperty("--cw-header-h", h + "px");
    } catch {}
  }
  try {
    _cwUpdateHeaderHeight();
    window.addEventListener("resize", _cwUpdateHeaderHeight, { passive: true });
    const header = D.querySelector("header");
    if (header && window.ResizeObserver) {
      const ro = new ResizeObserver(() => _cwUpdateHeaderHeight());
      ro.observe(header);
    }
  } catch {}


  // Settings collectors
  const collectors = (window.__settingsCollectors ||= new Set());
  if (typeof window.registerSettingsCollector !== "function") {
    window.registerSettingsCollector = fn => { if (typeof fn === "function") collectors.add(fn); };
  }
  if (typeof window.__emitSettingsCollect !== "function") {
    window.__emitSettingsCollect = cfg => {
      try { D.dispatchEvent(new CustomEvent("settings-collect", { detail: { cfg } })); } catch {}
      for (const fn of collectors) { try { fn(cfg); } catch {} }
    };
  }


function _cwGetByPath(obj, path) {
  try {
    return String(path || "")
      .split(".")
      .filter(Boolean)
      .reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
  } catch {
    return undefined;
  }
}

function _cwHasConfiguredValue(v) {
  if (v == null) return false;
  if (typeof v === "string") return v.trim().length > 0;
  if (typeof v === "number") return Number.isFinite(v) && v > 0;
  if (typeof v === "boolean") return v === true;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return false;
}

  function _cwConfigLooksLikeFirstRun(cfg) {
  try {
    if (!cfg || typeof cfg !== "object") return false;

    const pairs = Array.isArray(cfg.pairs) ? cfg.pairs.length : 0;
    const advancedJobs = Array.isArray(cfg?.scheduling?.advanced?.jobs) ? cfg.scheduling.advanced.jobs.length : 0;

    const configuredPaths = [
      "plex.server_url",
      "plex.account_token",
      "plex.pms_token",
      "plex.client_id",
      "plex.machine_id",
      "plex.username",
      "plex.account_id",
      "plex.home_pin",
      "simkl.access_token",
      "simkl.refresh_token",
      "simkl.client_id",
      "simkl.client_secret",
      "anilist.client_id",
      "anilist.client_secret",
      "anilist.access_token",
      "mdblist.api_key",
      "mdblist.access_token",
      "tautulli.server_url",
      "tautulli.api_key",
      "tautulli.history.user_id",
      "tracearr.server_url",
      "tracearr.api_key",
      "tracearr.history.user_id",
      "trakt.client_id",
      "trakt.client_secret",
      "trakt.access_token",
      "trakt.refresh_token",
      "tmdb_sync.api_key",
      "tmdb_sync.session_id",
      "tmdb_sync.account_id",
      "tmdb.api_key",
      "jellyfin.server",
      "jellyfin.access_token",
      "jellyfin.user_id",
      "jellyfin.username",
      "jellyfin.user",
      "emby.server",
      "emby.access_token",
      "emby.user_id",
      "emby.username",
      "emby.user",
      "kodi.server",
      "kodi.connection_verified",
      "stremio.auth_key",
      "app_auth.username",
      "app_auth.password.hash",
    ];

    const hasConfiguredProviders = configuredPaths.some(path => _cwHasConfiguredValue(_cwGetByPath(cfg, path)));
    const hasRoutes = Array.isArray(cfg?.scrobble?.watch?.routes) && cfg.scrobble.watch.routes.length > 0;
    const hasWebhookFilters = Array.isArray(cfg?.scrobble?.webhook?.filters_plex?.username_whitelist)
      && cfg.scrobble.webhook.filters_plex.username_whitelist.length > 0;

    return !hasConfiguredProviders && !pairs && !advancedJobs && !hasRoutes && !hasWebhookFilters;
  } catch {
    return false;
  }
}

async function _cwShouldOpenSetupWizard(meta) {
  try {
    if (_cwForceSetupRequested()) return true;
    if ((!meta?.exists) || !!meta?.first_run || !!meta?.autogen) return true;
    if (!!meta?.needs_upgrade) return false;
    if (typeof meta?.setup_wizard_required === "boolean") return meta.setup_wizard_required;
    if (!!meta?.auth_reset_required) return true;
    if (!!meta?.auth_configured) return false;

    const r = await fetch('/api/config?ts=' + Date.now(), { cache: 'no-store' });
    if (!r.ok) return false;

    const cfg = await r.json();
    return _cwConfigLooksLikeFirstRun(cfg);
  } catch (e) {
    console.warn("[crosswatch] setup wizard config check failed", e);
    return false;
  }
}

function _cwForceSetupRequested() {
  try {
    const q = new URLSearchParams(window.location.search || "");
    const raw = String(q.get("setup") || q.get("first_setup") || q.get("auth_setup") || "").trim().toLowerCase();
    return raw === "1" || raw === "true" || raw === "yes";
  } catch {
    return false;
  }
}

async function _cwFetchBootstrapAuthState() {
  try {
    const [metaResp, statusResp] = await Promise.all([
      fetch('/api/config/meta?ts=' + Date.now(), { cache: 'no-store', credentials: 'same-origin' }),
      fetch('/api/app-auth/status?ts=' + Date.now(), { cache: 'no-store', credentials: 'same-origin' }),
    ]);
    const meta = metaResp.ok ? await metaResp.json() : null;
    const status = statusResp.ok ? await statusResp.json() : null;
    const blocked = !!(
      status
      && (status.setup_required || status.reset_required || !status.configured || meta?.auth_setup_required || meta?.auth_reset_required || meta?.first_run || meta?.autogen)
    );
    window.__cwAuthSetupPending = blocked;
    window.__cwAuthBootstrapState = { meta, status, blocked };
    return window.__cwAuthBootstrapState;
  } catch (e) {
    window.__cwAuthBootstrapState = { meta: null, status: null, blocked: false, error: String(e?.message || e || "") };
    window.__cwAuthSetupPending = false;
    return window.__cwAuthBootstrapState;
  }
}

if (typeof window.__cwAuthSetupPending === "undefined") window.__cwAuthSetupPending = true;
window.__cwAuthBootstrapPromise ||= _cwFetchBootstrapAuthState();
window.cwIsAuthSetupPending = () => window.__cwAuthSetupPending === true;

  // Bootstrap
  window.addEventListener("DOMContentLoaded", () => {
    try { DOM.fixFormLabels?.(); } catch {}

    // Setup 
    (async () => {
      try {
        const boot = await (window.__cwAuthBootstrapPromise || Promise.resolve(null));
        const forceSetup = _cwForceSetupRequested();
        const meta = boot?.meta || {};
        if (!boot?.meta && !forceSetup) return;

        async function ensureModals(required) {
          const ready = () => typeof window[required] === "function";
          if (ready()) return true;
          try {
            const v = encodeURIComponent(String(window.APP_VERSION || window.__CW_VERSION__ || Date.now()));
            await import(`/assets/js/modals.js?v=${v}`);
          } catch (e) {
            console.warn("[crosswatch] modals.js failed to load/execute", e);
          }
          for (let i = 0; i < 100 && !ready(); i++) {
            await new Promise(r => setTimeout(r, 50));
          }
          if (!ready()) console.warn(`[crosswatch] modals.js loaded but ${required} is unavailable`);
          return ready();
        }

        const firstRun = await _cwShouldOpenSetupWizard(meta);

        if (firstRun) {
          if (await ensureModals("openSetupWizard")) {
            try {
              await window.openSetupWizard({
                ...meta,
                auth_setup_required: true,
                setup_wizard_required: true,
                auth_reset_required: !!(meta?.auth_reset_required || boot?.status?.reset_required),
              });
            } catch (e) { console.warn(e); }
          }
          return;
        }

        if (meta.needs_upgrade) {
          if (await ensureModals("openUpgradeWarning")) { try { await window.openUpgradeWarning(meta); } catch (e) { console.warn(e); } }
        }
      } catch {}
    })();
  });
})();
