/* assets/crosswatch.js */
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

// Capture install prompt (Chrome/Edge/Android).
let _cwDeferredInstallPrompt = null;

window.addEventListener("beforeinstallprompt", (e) => {
  try { e.preventDefault(); } catch {}
  _cwDeferredInstallPrompt = e;
  try {
    const wrap = document.getElementById("cw-install");
    if (wrap) wrap.setAttribute("data-cw-install-ready", "1");
  } catch {}
});

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
  if (!_cwIsMobile()) return;
  if (_cwIsStandalone()) return;
  if (_cwInstallDismissedRecently()) return;
  if (!_cwIsSecureEnough()) return;

  try {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/sw.js", { scope: "/" }).catch(() => {});
    }
  } catch {}

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

  let primaryHandler = null;
  const setPrimary = (label, handler) => {
    if (primaryBtn && label) primaryBtn.textContent = label;
    if (!primaryBtn) return;
    if (primaryHandler) primaryBtn.removeEventListener("click", primaryHandler);
    primaryHandler = handler;
    if (primaryHandler) primaryBtn.addEventListener("click", primaryHandler);
  };

  const setSecondary = (label) => {
    if (secondaryBtn && label) secondaryBtn.textContent = label;
  };

  const setCopy = (title, text) => {
    if (titleEl && title) titleEl.textContent = title;
    if (textEl && text) textEl.textContent = text;
  };

  const setHint = (text) => {
    if (!hintEl) return;
    hintEl.textContent = text || "";
    hintEl.classList.toggle("hidden", !text);
  };

  const show = () => {
    if (wrap.classList.contains("show")) return;
    requestAnimationFrame(() => wrap.classList.add("show"));
  };

  // iOS: always manual A2HS.
  if (_cwIsIOS()) {
    setCopy("Install CrossWatch", "Add it to your Home Screen for the best experience.");
    setHint("Tap Share (⬆︎) → “Add to Home Screen”.");
    setPrimary("Got it", dismiss);
    setSecondary("Not now");
    show();
    return;
  }

  // Android/others:
  const installAttempt = async () => {
    if (_cwDeferredInstallPrompt) {
      try {
        wrap.setAttribute("data-cw-install-ready", "1");
        _cwDeferredInstallPrompt.prompt();
        await _cwDeferredInstallPrompt.userChoice;
      } catch {}
      _cwDeferredInstallPrompt = null;
      dismiss();
      return;
    }

    // No prompt available 
    const tip = _cwIsAndroid()
      ? "Chrome: ⋮ menu|settings  “Install app” (or “Add to Home screen”)."
      : "Browser menu “Add to Home screen”.";
    setHint(tip);
    setPrimary("Got it", dismiss);
  };

  setCopy("Install CrossWatch", "Use it like a real app: full screen and faster access.");
  setHint("");
  setPrimary("Install", installAttempt);
  setSecondary("Not now");

  try {
    if (_cwDeferredInstallPrompt) wrap.setAttribute("data-cw-install-ready", "1");
  } catch {}

  show();
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
      hasInstallPrompt: !!_cwDeferredInstallPrompt,
      swSupported: "serviceWorker" in navigator,
    };
  } catch {
    return { error: "diag_failed" };
  }
};

  // Bootstrap
  window.addEventListener("DOMContentLoaded", () => {
    try { DOM.fixFormLabels?.(); } catch {}
    try { CW.Providers?.load?.(); } catch {}
    try { CW.Providers?.mountMetadataProviders?.(); } catch {}
    try { CW.Pairs?.list?.(); } catch {}
    try { CW.Scheduling?.load?.(); } catch {}
    try { CW.Insights?.loadLight?.(); } catch {}
    try { cwInitPwaInstall(); } catch {}

    // Setup 
    (async () => {
      try {
        const r = await fetch('/api/config/meta?ts=' + Date.now(), { cache: 'no-store' });
        if (!r.ok) return;
        const meta = await r.json();
        if (!meta) return;

        async function ensureModals() {
          if (typeof window.openUpgradeWarning === "function" || typeof window.openSetupWizard === "function") return true;
          try {
            const v = encodeURIComponent(String(window.APP_VERSION || window.__CW_VERSION__ || window.__CW_BUILD__ || Date.now()));
            await import(`/assets/js/modals.js?v=${v}`);
            return true;
          } catch (e) {
            console.warn("[crosswatch] modals.js failed to load/execute", e);
            return false;
          }
        }

        if (!meta.exists) {
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
