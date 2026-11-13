/* assets/helpers/pwa-install.js */
/* CrossWatch - Shared app installation for Main and My Media Hub */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const D = document;
  let _cwInstallPrompt = null;
  let _cwAppInstalled = false;

  function _cwIsMobile() {
    return /Android|iPhone|iPad|iPod/i.test(navigator.userAgent || "") || !!window.matchMedia?.("(pointer: coarse)")?.matches;
  }

  function _cwIsStandalone() {
    try {
      if (window.matchMedia?.("(display-mode: standalone)")?.matches) return true;
    } catch {}
    try {
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


  function _cwSyncInstallAction() {
    const menu = D.getElementById("cw-mobile-navigation");
    const existing = D.getElementById("cw-mobile-install");
    if (!_cwInstallPrompt || _cwAppInstalled || _cwIsStandalone() || !_cwIsSecureEnough()) {
      if (existing?.contains(D.activeElement)) D.getElementById("cw-mobile-more")?.focus({preventScroll:true});
      existing?.remove();
      const banner = D.getElementById("cw-install");
      if (banner?.dataset.nativeInstall) _cwHideInstallUi();
      return;
    }
    if (!_cwInstallDismissedRecently()) _cwShowNativeInstall();
    if (!menu || existing) return;
    const action = D.createElement("div");
    action.id = "cw-mobile-install";
    action.className = "cw-mobile-extra-destinations";
    action.innerHTML = '<button type="button" class="tab"><span class="material-symbols-rounded cw-mobile-destination-icon" aria-hidden="true">install_mobile</span><span>Install CrossWatch</span></button>';
    action.querySelector("button").addEventListener("click", _cwRequestInstall);
    menu.appendChild(action);
  }

  async function _cwRequestInstall() {
    const prompt = _cwInstallPrompt;
    if (!prompt || _cwAppInstalled || _cwIsStandalone()) return;
    _cwInstallPrompt = null;
    _cwSyncInstallAction();
    try {
      await prompt.prompt();
    } catch (error) {
      console.warn("[crosswatch] Install prompt failed", error);
    }
  }

  function _cwHideInstallUi() {
    const banner = D.getElementById("cw-install");
    if (!banner) return;
    banner.classList.remove("show");
    banner.hidden = true;
  }

  function _cwShowNativeInstall() {
    const banner = _cwEnsureInstallUi();
    banner.dataset.nativeInstall = "true";
    const dismiss = () => { _cwMarkInstallDismissed(); _cwHideInstallUi(); };
    banner.querySelector("#cw-install-primary").onclick = _cwRequestInstall;
    banner.querySelector("#cw-install-secondary").onclick = dismiss;
    banner.querySelector("#cw-install-close").onclick = dismiss;
    banner.onkeydown = event => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      event.stopPropagation();
      dismiss();
    };
    banner.hidden = false;
    requestAnimationFrame(() => { if (_cwInstallPrompt && !banner.hidden) banner.classList.add("show"); });
  }

  window.addEventListener("beforeinstallprompt", event => {
    if (!_cwIsMobile() || !_cwIsSecureEnough() || _cwAppInstalled || _cwIsStandalone()) return;
    event.preventDefault();
    _cwInstallPrompt = event;
    _cwSyncInstallAction();
    event.userChoice.then(choice => {
      if (_cwInstallPrompt === event) _cwInstallPrompt = null;
      if (choice.outcome === "accepted") _cwAppInstalled = true;
      else _cwMarkInstallDismissed();
      _cwSyncInstallAction();
    }).catch(() => {
      if (_cwInstallPrompt === event) _cwInstallPrompt = null;
      _cwSyncInstallAction();
    });
  });
  D.addEventListener("cw-mobile-navigation-ready", _cwSyncInstallAction);
  window.matchMedia?.("(display-mode: standalone)")?.addEventListener("change", _cwSyncInstallAction);

  window.addEventListener("appinstalled", () => {
    _cwAppInstalled = true;
    _cwInstallPrompt = null;
    _cwSyncInstallAction();
    _cwMarkInstallDismissed();
    _cwHideInstallUi();
  });

  function _cwEnsureInstallUi() {
    let wrap = D.getElementById("cw-install");
    if (wrap) return wrap;

    wrap = D.createElement("div");
    wrap.id = "cw-install";
    wrap.className = "cw-install";
    wrap.hidden = true;
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
    _cwSyncInstallAction();
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

    const hide = _cwHideInstallUi;
    const dismiss = () => { _cwMarkInstallDismissed(); hide(); };
    wrap.onkeydown = event => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      event.stopPropagation();
      dismiss();
    };

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

    wrap.hidden = false;
    requestAnimationFrame(() => { if (!wrap.hidden) wrap.classList.add("show"); });
  }
  window.cwPwaDiag = function () {
    try {
      return {
        secureContext: !!window.isSecureContext,
        protocol: location.protocol,
        host: location.hostname,
        mobile: _cwIsMobile(),
        handheld: _cwIsMobile(),
        standalone: _cwIsStandalone(),
        ios: _cwIsIOS(),
        android: _cwIsAndroid(),
        dismissedRecently: _cwInstallDismissedRecently(),
        hasInstallPrompt: !!_cwInstallPrompt && !_cwAppInstalled && !_cwIsStandalone(),
        swSupported: "serviceWorker" in navigator,
      };
    } catch {
      return { error: "diag_failed" };
    }
  };


  if (D.readyState === "loading") D.addEventListener("DOMContentLoaded", cwInitPwaInstall, {once:true});
  else cwInitPwaInstall();
})();
