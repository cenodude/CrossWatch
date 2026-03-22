/* assets/crosswatch.js */
/* CrossWatch main JavaScript file: UI utilities, tab handling, PWA install prompt, and bootstrap logic. */
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
      D.querySelectorAll("#page-main, #page-watchlist, #page-settings, .tab-page")
        .forEach(el => el.classList.add("hidden"));

      const tgt = D.getElementById("page-" + id) || D.getElementById(id);
      if (tgt) tgt.classList.remove("hidden");

      ["main", "watchlist", "settings"].forEach(n => {
        const th = D.getElementById("tab-" + n);
        if (th) th.classList.toggle("active", n === id);
      });

      D.dispatchEvent(new CustomEvent("tab-changed", { detail: { id } }));
      Events.emit?.("tab:changed", { id });

      if (id === "watchlist") {
        try { window.Watchlist?.mount?.(D.getElementById("page-watchlist")); } catch {}
      }
    } catch (e) {
      console.warn("[crosswatch] showTab failed", e);
    }
  }
  if (typeof window.showTab !== "function") window.showTab = showTab;

    // UI mode (compact/full)
  const _cwGetUiMode = () => {
    try {
      const url = new URL(window.location.href);
      const q = url.searchParams;

      const ui = String(q.get("ui") || "").toLowerCase();
      if (ui === "compact" || q.get("compact") === "1") return "compact";
      if (ui === "full" || q.get("full") === "1") return "full";

      const saved = String(localStorage.getItem("cw_ui_mode") || "").toLowerCase();
      if (saved === "compact" || saved === "full") return saved;

      try {
        if (window.matchMedia?.("(max-width: 680px)")?.matches) return "compact";
      } catch {}
    } catch {}
    return "full";
  };

  const _cwApplyUiMode = (mode) => {
    const m = mode === "compact" ? "compact" : "full";
    document.documentElement.classList.toggle("cw-compact", m === "compact");
    return m;
  };

  if (typeof window.cwSetUiMode !== "function") {
    window.cwSetUiMode = (mode) => {
      try {
        const m = mode === "compact" ? "compact" : "full";
        try { localStorage.setItem("cw_ui_mode", m); } catch {}

        const url = new URL(window.location.href);
        const q = url.searchParams;
        q.delete("compact");
        q.delete("full");
        q.set("ui", m);
        url.search = q.toString() ? "?" + q.toString() : "";

        window.location.assign(url.toString());
      } catch (e) {
        console.warn("[crosswatch] cwSetUiMode failed", e);
      }
    };
  }

  try { _cwApplyUiMode(_cwGetUiMode()); } catch {}

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


    // PWA: install banner (Android prompt and fallback, iOS guidance)
function _cwIsMobile() {
  try {
    if (window.matchMedia?.("(max-width: 680px)")?.matches) return true;
  } catch {}
  return /Android|iPhone|iPad|iPod/i.test(navigator.userAgent || "");
}

function _cwIsStandalone() {
  try {
    if (window.matchMedia?.("(display-mode: standalone)")?.matches) return true;
  } catch {}
  // iOS Safari
  try {
    // @ts-ignore
    if (navigator.standalone) return true;
  } catch {}
  return false;
}

function _cwIsIOS() {
  return /iPhone|iPad|iPod/i.test(navigator.userAgent || "");
}

function _cwIsAndroid() {
  return /Android/i.test(navigator.userAgent || "");
}

function _cwInstallDismissedRecently() {
  try {
    const ts = Number(localStorage.getItem("cw_pwa_install_dismissed_at") || "0");
    if (!ts) return false;
    return (Date.now() - ts) < (7 * 24 * 60 * 60 * 1000);
  } catch {
    return false;
  }
}

function _cwMarkInstallDismissed() {
  try { localStorage.setItem("cw_pwa_install_dismissed_at", String(Date.now())); } catch {}
}

function _cwIsSecureEnough() {
  try {
    if (typeof window.isSecureContext === "boolean") return window.isSecureContext;
  } catch {}
  try {
    const h = String(location.hostname || "").toLowerCase();
    if (location.protocol === "https:") return true;
    if (h === "localhost" || h === "127.0.0.1" || h === "::1") return true;
  } catch {}
  return false;
}


window.addEventListener("appinstalled", () => {
  _cwMarkInstallDismissed();
  try { document.getElementById("cw-install")?.classList.remove("show"); } catch {}
});

function _cwEnsureInstallUi() {
  let wrap = D.getElementById("cw-install");
  if (wrap) return wrap;

  wrap = D.createElement("div");
  wrap.id = "cw-install";
  wrap.className = "cw-install";
  wrap.setAttribute("aria-live", "polite");
  wrap.innerHTML = `
    <div class="cw-install-card" role="dialog" aria-label="Install CrossWatch">
      <div class="cw-install-top">
        <div class="cw-install-icon" aria-hidden="true">CW</div>
        <div class="cw-install-copy">
          <div class="cw-install-title" id="cw-install-title">Install CrossWatch</div>
          <div class="cw-install-text" id="cw-install-text">Add CrossWatch to your Home Screen.</div>
          <div class="cw-install-hint hidden" id="cw-install-hint"></div>
        </div>
        <button type="button" class="cw-install-close" id="cw-install-close" aria-label="Dismiss">×</button>
      </div>
      <div class="cw-install-actions">
        <button type="button" class="cw-install-btn primary" id="cw-install-primary">Install</button>
        <button type="button" class="cw-install-btn" id="cw-install-secondary">Not now</button>
      </div>
    </div>
  `;

  try { D.body.appendChild(wrap); } catch {}
  return wrap;
}

function cwInitPwaInstall() {
  if (_cwIsStandalone()) return;
  if (!_cwIsSecureEnough()) return;

  try {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/sw.js", { scope: "/" }).catch(() => {});
    }
  } catch {}

  if (!_cwIsMobile() || !_cwIsIOS()) return;
  if (_cwInstallDismissedRecently()) return;

  const wrap = _cwEnsureInstallUi();
  if (!wrap) return;

  const titleEl = wrap.querySelector("#cw-install-title");
  const textEl = wrap.querySelector("#cw-install-text");
  const hintEl = wrap.querySelector("#cw-install-hint");
  const primaryBtn = wrap.querySelector("#cw-install-primary");
  const secondaryBtn = wrap.querySelector("#cw-install-secondary");
  const closeBtn = wrap.querySelector("#cw-install-close");

  const hide = () => wrap.classList.remove("show");
  const dismiss = () => { _cwMarkInstallDismissed(); hide(); };

  secondaryBtn?.addEventListener("click", dismiss, { passive: true });
  closeBtn?.addEventListener("click", dismiss, { passive: true });

  const setCopy = (title, text) => {
    if (titleEl && title) titleEl.textContent = title;
    if (textEl && text) textEl.textContent = text;
  };

  const setHint = (text) => {
    if (!hintEl) return;
    hintEl.textContent = text || "";
    hintEl.classList.toggle("hidden", !text);
  };

  if (primaryBtn) {
    primaryBtn.textContent = "Got it";
    primaryBtn.onclick = dismiss;
  }

  if (secondaryBtn) secondaryBtn.textContent = "Not now";

  setCopy("Install CrossWatch", "Add it to your Home Screen for the best experience.");
  setHint("Tap Share (⬆︎) → “Add to Home Screen”.");

  requestAnimationFrame(() => wrap.classList.add("show"));
}
window.cwPwaDiag = function () {
  try {
    return {
      secureContext: !!window.isSecureContext,
      protocol: location.protocol,
      host: location.hostname,
      mobile: _cwIsMobile(),
      standalone: _cwIsStandalone(),
      ios: _cwIsIOS(),
      android: _cwIsAndroid(),
      dismissedRecently: _cwInstallDismissedRecently(),
      hasInstallPrompt: false,
      swSupported: "serviceWorker" in navigator,
    };
  } catch {
    return { error: "diag_failed" };
  }
};


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
      "tautulli.server_url",
      "tautulli.api_key",
      "tautulli.history.user_id",
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
      && !status.authenticated
      && (status.reset_required || !status.configured || meta?.auth_reset_required || meta?.first_run || meta?.autogen)
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
    try { cwInitPwaInstall(); } catch {}

    // Setup 
    (async () => {
      try {
        const boot = await (window.__cwAuthBootstrapPromise || Promise.resolve(null));
        const meta = boot?.meta || null;
        if (!meta) return;

        async function ensureModals() {
          if (typeof window.openUpgradeWarning === "function" || typeof window.openSetupWizard === "function") return true;
          try {
            const v = encodeURIComponent(String(window.APP_VERSION || window.__CW_VERSION__ || Date.now()));
            await import(`/assets/js/modals.js?v=${v}`);
            return true;
          } catch (e) {
            console.warn("[crosswatch] modals.js failed to load/execute", e);
            return false;
          }
        }

        const firstRun = await _cwShouldOpenSetupWizard(meta);

        if (firstRun) {
          if (await ensureModals()) { try { await window.openSetupWizard?.(meta); } catch (e) { console.warn(e); } }
          return;
        }

        if (meta.needs_upgrade) {
          if (await ensureModals()) { try { await window.openUpgradeWarning?.(meta); } catch (e) { console.warn(e); } }
        }
      } catch {}
    })();
  });
})();
