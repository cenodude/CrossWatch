/* assets/js/modals/setup-wizard/index.js */
/* CrossWatch - setup wizard modal component */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */


const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || Date.now(); }
  catch { return window.__CW_VERSION__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(String(_cwV));

const {
  escapeHtml,
  saveRequiredAppAuth,
  setModalDismissible,
  setModalShellInline,
  syncAppAuthState,
  validateAppAuthState,
  wireLiveAppAuthValidation,
  renderAppAuthFields,
} = await import(_cwVer("../core/app-auth-setup.js"));

function _norm(v) {
  return String(v || "").replace(/^v/i, "").trim();
}

function _collapseByDefault() {
  const ids = ["sec-auth", "sec-scrobbler", "sc-sec-webhook", "sc-sec-watch"];
  for (const id of ids) {
    try { document.getElementById(id)?.classList.remove("open"); } catch {}
  }
}

function _openSettings() {
  try { window.showTab?.("settings"); } catch {}
  try { _collapseByDefault(); } catch {}
}

function _art(credentials) {
  return `<div class="sw-art" aria-hidden="true">
    <svg class="sw-monitor" viewBox="0 0 400 280" fill="none" focusable="false">
      <defs>
        <linearGradient id="sw-frame" x1="80" y1="50" x2="300" y2="250" gradientUnits="userSpaceOnUse"><stop stop-color="#7665e9"/><stop offset=".5" stop-color="#363565"/><stop offset="1" stop-color="#22273e"/></linearGradient>
        <linearGradient id="sw-screen" x1="120" y1="80" x2="280" y2="230" gradientUnits="userSpaceOnUse"><stop stop-color="#232743"/><stop offset="1" stop-color="#141822"/></linearGradient>
        <linearGradient id="sw-symbol" x1="180" y1="110" x2="250" y2="190" gradientUnits="userSpaceOnUse"><stop stop-color="#8c74ff"/><stop offset="1" stop-color="#4d418c"/></linearGradient>
        <linearGradient id="sw-arc" x1="40" y1="30" x2="340" y2="240" gradientUnits="userSpaceOnUse"><stop stop-color="#7762df" stop-opacity=".35"/><stop offset="1" stop-color="#7762df" stop-opacity="0"/></linearGradient>
      </defs>
      <path d="M35 230C55 40 188-20 315 48M2 273C65 125 212 92 384 107M65 277C136 180 271 160 398 184" stroke="url(#sw-arc)" stroke-width="1.5"/>
      <path d="M214 221L218 248H253L246 219" fill="#20253d" stroke="#51478d" stroke-opacity=".4"/>
      <path d="M189 249H272C278 249 282 252 282 256H179C179 252 183 249 189 249Z" fill="url(#sw-frame)"/>
      <path d="M129 75L301 53C312 52 317 59 317 69V218C317 228 312 232 302 232L127 237C117 237 112 231 112 221V93C112 83 116 77 129 75Z" fill="url(#sw-frame)" stroke="#8a7aef" stroke-opacity=".65"/>
      <path d="M134 89L294 69C300 68 302 71 302 77V203C302 207 299 210 295 210L133 216C128 216 125 213 125 208V101C125 94 127 91 134 89Z" fill="url(#sw-screen)" stroke="#7367bd" stroke-opacity=".5"/>
      ${credentials ? '<circle cx="216" cy="129" r="15" fill="url(#sw-symbol)" stroke="#9383fb" stroke-opacity=".6"/><path d="M189 176C189 144 243 141 243 174C243 182 189 185 189 176Z" fill="url(#sw-symbol)" stroke="#9383fb" stroke-opacity=".5"/>' : '<path d="M182 145L204 167L246 120" stroke="url(#sw-symbol)" stroke-width="12" stroke-linecap="round" stroke-linejoin="round"/>'}
      <path d="M120 257H326" stroke="#5b5293" stroke-opacity=".25"/>
    </svg>
    <p class="sw-motto">${credentials ? 'Secure<br>your access.<br>Keep your data<br>in your control.' : 'Your media.<br>Your data.<br>On your terms.'}<svg viewBox="0 0 120 20" focusable="false"><path d="M5 16Q58 0 114 5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg></p>
  </div>`;
}

function _hero(credentials, resetRequired = false) {
  const heading = resetRequired ? "Set up your sign-in again" : credentials ? "Create your sign-in credentials" : "Get CrossWatch ready";
  const copy = resetRequired ? "Authentication was reset at startup. Set a new username and password to continue." : credentials ? "You need a username and password before CrossWatch opens the rest of Settings." : "Create your sign-in first, then finish the rest in Settings.";
  return `<section class="sw-hero">
    ${_art(credentials)}
    ${resetRequired ? '<div class="sw-required">Recovery required</div>' : `<ol class="sw-steps" aria-label="Setup progress"><li aria-current="step"><span class="sw-step-number">1</span><div><span>Step 1 of 2</span><strong>Create sign-in</strong></div></li><li><span class="sw-step-number">2</span><div><strong>Finish setup</strong><span>In Settings</span></div></li></ol>`}
    <div class="sw-hero-copy"><h1 id="sw-heading" tabindex="-1">${heading}</h1><p>${copy}</p></div>
  </section>`;
}

function _chrome(ver, logo, body, foot, chrome = {}) {
  return `<div id="setup-host" class="sw-welcome" role="dialog" aria-modal="true" aria-labelledby="sw-heading">
    <div class="sw-header">
      <div class="sw-brand-icon" aria-hidden="true"><img src="${escapeHtml(logo)}" alt="" /></div>
      <div><div class="sw-brand-title">${escapeHtml(chrome.title || "Welcome to CrossWatch")}</div><div class="sw-subtitle">${escapeHtml(chrome.subtitle || "First run setup")}</div></div>
      <div class="sw-version">v${escapeHtml(ver)}</div>
    </div>
    <div class="sw-body">${body}</div>
    <div class="sw-footer">${foot}</div>
  </div>`;
}

export default {
  async mount(hostEl, props = {}) {
    if (!hostEl) return;

    const ver = _norm(props.current_version || window.__CW_VERSION__ || "0.0.0");
    const crossWatchLogo = window.CW?.ProviderMeta?.logoPath?.("crosswatch") || "/assets/img/CROSSWATCH.svg";
    const shell = hostEl.closest(".cx-modal-shell");
    const resetRequired = !!props.auth_reset_required;
    const state = {
      step: resetRequired ? "credentials" : "intro",
      username: "admin",
      password: "",
      password2: "",
      error: "",
      saving: false,
    };

    setModalDismissible(false);

    async function submitCredentials() {
      if (state.saving) return;
      syncAppAuthState(hostEl, state);
      state.error = validateAppAuthState(state);
      if (state.error) {
        render();
        return;
      }

      state.saving = true;
      render();

      try {
        await saveRequiredAppAuth({
          username: state.username,
          password: state.password,
        });
        state.saving = false;
        state.error = "";
        state.password = "";
        state.password2 = "";
        try { window.notify?.("Sign-in enabled."); } catch {}
        try {
          const boot = window.__cwAuthBootstrapState || {};
          window.__cwAuthBootstrapState = { ...boot, blocked: false };
        } catch {}
        try { window.cxCloseModal?.(); } catch {}
        setTimeout(() => {
          try { _openSettings(); } catch {}
          try { window.cwSettingsSelect?.("overview"); } catch {}
        }, 0);
        return;
      } catch (err) {
        state.error = String(err?.message || "Failed to save sign-in settings.");
        state.saving = false;
        render();
      }
    }

    function renderIntro() {
      const body = `
        ${_hero(false)}
        <section class="sw-signin-callout" aria-labelledby="sw-signin-title">
          <div class="sw-lock" aria-hidden="true"><span class="material-symbols-rounded">lock</span></div>
          <div class="sw-callout-copy"><span class="sw-required">Required</span><h2 id="sw-signin-title">Create sign-in credentials</h2><p>Protect access to CrossWatch before continuing.</p></div>
          <button class="sw-action" type="button" data-x="next">Set up sign-in <span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></button>
        </section>
        <section class="sw-after" aria-labelledby="sw-after-title"><h2 id="sw-after-title">After sign-in, you can configure</h2><p>These settings can be changed anytime in Settings.</p>
          <ul class="sw-features">
            <li><span class="sw-feature-icon material-symbols-rounded" aria-hidden="true">database</span><div><h3>Metadata provider</h3><p>Configure TMDb.</p></div></li>
            <li><span class="sw-feature-icon material-symbols-rounded" aria-hidden="true">key</span><div><h3>Connections</h3><p>Link one or more providers in Settings.</p></div></li>
            <li><span class="sw-feature-icon material-symbols-rounded" aria-hidden="true">sync_alt</span><div><h3>Synchronization and scrobbler</h3><p>Optionally configure sync pairs and/or Scrobbler.</p></div></li>
          </ul>
        </section>
        <a class="sw-docs" href="https://wiki.crosswatch.app/" target="_blank" rel="noopener noreferrer">
          <span class="sw-feature-icon material-symbols-rounded" aria-hidden="true">menu_book</span><div><span class="sw-eyebrow">Documentation</span><h2>Open the CrossWatch Wiki</h2><p>Setup guides, first-run help, and troubleshooting in one place.</p></div><span class="material-symbols-rounded sw-external" aria-hidden="true">open_in_new</span>
        </a>
        <details class="sw-disclaimer">
          <summary><span class="material-symbols-rounded" aria-hidden="true">gavel</span>Disclaimer</summary>
          <div class="sw-disclaimer-body">
            <p>This is an independent, community-maintained project and is not affiliated with, endorsed by, or sponsored by Plex, Emby, Jellyfin, Kodi, Nuvio, Stremio, Trakt, TMDb, SIMKL, Tautulli, AniList, MDBList, PublicMetaDB, Floppy, PunchPlay, BingeBase, FlickList, Scrob, or their owners. Use at your own risk.</p>
            <p>All product names, logos, and brands are property of their respective owners and used for identification only.</p>
            <p>Interacts with third-party services; you are responsible for complying with their Terms of Use and API rules.</p>
            <p>Provided "as is," without warranties or guarantees.</p>
          </div>
        </details>
      `;
      const foot = `<p class="sw-footer-note"><span class="material-symbols-rounded" aria-hidden="true">info</span>Sign-in is required before setup continues.</p>`;
      hostEl.innerHTML = _chrome(ver, crossWatchLogo, body, foot);
      hostEl.querySelector('[data-x="next"]')?.addEventListener("click", () => {
        state.step = "credentials";
        render();
        hostEl.querySelector("#sw-auth-user")?.focus({preventScroll: true});
      });
    }

    function renderCredentials() {
      const helper = resetRequired ? "Sign-in was reset and must be configured again before continuing." : "Sign-in is required before first use.";
      const body = `
        ${_hero(true, resetRequired)}
        <section class="sw-credentials" aria-labelledby="sw-credentials-title">
          <div class="sw-credentials-heading"><span class="sw-lock" aria-hidden="true"><span class="material-symbols-rounded">lock</span></span><div><h2 id="sw-credentials-title">Sign-in credentials</h2><p>${resetRequired ? "Restore access to your local CrossWatch account." : "This creates your local CrossWatch account."}</p></div></div>
          ${renderAppAuthFields({idPrefix: "sw-auth", state, errorId: "sw-auth-error", wrap: false})}
          ${resetRequired ? '<p class="sw-recovery-note">Background activity is paused until you finish setting the new sign-in credentials.</p>' : ""}
        </section>
      `;
      const foot = `
        <p class="sw-footer-note"><span class="material-symbols-rounded" aria-hidden="true">info</span>${escapeHtml(helper)}</p>
        <div class="sw-footer-actions">
          ${resetRequired ? "" : `<button class="sw-back" type="button" data-x="back"${state.saving ? " disabled" : ""}>Back</button>`}
          <button class="sw-action" type="button" data-x="save"${state.saving ? " disabled" : ""}>${state.saving ? "Saving..." : (resetRequired ? "Save new sign-in" : "Enable sign-in")}<span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></button>
        </div>
      `;
      hostEl.innerHTML = _chrome(
        ver,
        crossWatchLogo,
        body,
        foot,
        resetRequired
          ? { title: "CrossWatch authentication reset", subtitle: "Recovery setup" }
          : undefined,
      );
      if (!resetRequired) {
        hostEl.querySelector('[data-x="back"]')?.addEventListener("click", () => {
          if (state.saving) return;
          syncAppAuthState(hostEl, state);
          state.step = "intro";
          render();
          hostEl.querySelector('[data-x="next"]')?.focus({preventScroll: true});
        });
      }
      for (const [id, label] of [["sw-auth-pass", "password"], ["sw-auth-pass2", "confirm password"]]) {
        const input = hostEl.querySelector(`#${id}`);
        if (!input) continue;
        input.disabled = state.saving;
        const wrap = document.createElement("div");
        wrap.className = "sw-password";
        input.before(wrap);
        wrap.append(input);
        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "sw-password-toggle";
        toggle.setAttribute("aria-label", `Show ${label}`);
        toggle.setAttribute("aria-controls", id);
        toggle.setAttribute("aria-pressed", "false");
        toggle.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">visibility</span>';
        toggle.addEventListener("click", () => {
          const visible = input.type === "password";
          input.type = visible ? "text" : "password";
          toggle.setAttribute("aria-label", `${visible ? "Hide" : "Show"} ${label}`);
          toggle.setAttribute("aria-pressed", String(visible));
          toggle.firstElementChild.textContent = visible ? "visibility_off" : "visibility";
        });
        wrap.append(toggle);
      }
      const usernameInput = hostEl.querySelector("#sw-auth-user");
      if (usernameInput) usernameInput.disabled = state.saving;
      hostEl.querySelector("#sw-auth-error")?.setAttribute("role", "alert");
      const saveBtn = hostEl.querySelector('[data-x="save"]');
      const submitError = state.error;
      wireLiveAppAuthValidation(hostEl, state, "sw-auth-error", saveBtn);
      if (submitError) {
        state.error = submitError;
        const errorEl = hostEl.querySelector("#sw-auth-error");
        if (errorEl) {
          errorEl.textContent = submitError;
          errorEl.classList.add("show");
        }
      }
      saveBtn?.addEventListener("click", () => submitCredentials());
      hostEl.querySelector("#sw-auth-pass2")?.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !state.saving) submitCredentials();
      });
    }

    function render() {
      setModalShellInline(shell);
      setModalDismissible(false);
      if (state.step === "credentials") renderCredentials();
      else renderIntro();
    }

    render();
  },

  unmount() {}
};
