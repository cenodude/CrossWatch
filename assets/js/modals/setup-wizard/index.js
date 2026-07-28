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

function _chrome(ver, logo, body, foot, chrome = {}) {
  const title = escapeHtml(chrome.title || "Welcome to CrossWatch");
  const subtitle = escapeHtml(chrome.subtitle || "First run setup");
  const ariaLabel = escapeHtml(chrome.ariaLabel || "CrossWatch setup");
  return `

    <div id="setup-host" role="dialog" aria-label="${ariaLabel}">
      <div class="head">
        <div class="logoWrap" aria-hidden="true"><img class="logo" src="${logo}" alt="" /></div>
        <div>
          <div class="title">${title}</div>
          <div class="sub">${subtitle}</div>
        </div>
        <div class="v">v${ver}</div>
      </div>
      <div class="body">${body}</div>
      <div class="foot">${foot}</div>
    </div>
  `;
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

    async function submitCredentials(btn) {
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
        <div class="badge"><span class="dot" aria-hidden="true"></span><span style="font-weight:900">Quick setup</span></div>
        <div class="headline">Configure it once. Then forget it exists <span style="opacity:.70">(hopefully)</span></div>
        <div class="lede">CrossWatch requires you to create a sign-in username and password</div>

        <div class="grid" role="list">
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">lock</span></div>
            <div>
              <div><b>Sign-in protection</b></div>
              <div class="muted">This protects CrossWatch before the rest of setup.</div>
            </div>
          </div>
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">key</span></div>
            <div>
              <div><b>Connections</b></div>
              <div class="muted">Next: link one or more providers in Settings.</div>
            </div>
          </div>
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">database</span></div>
            <div>
              <div><b>Metadata provider</b></div>
              <div class="muted">Configure TMDb.</div>
            </div>
          </div>
          <div class="card" role="listitem">
            <div class="ico" aria-hidden="true"><span class="material-symbols-rounded">sync_alt</span></div>
            <div>
              <div><b>Synchronization and scrobbler</b></div>
              <div class="muted">Optional configure sync pairs and/or Scrobbler.</div>
            </div>
          </div>
        </div>

        <div class="tip">After sign-in is enabled, you will be taken to <b>Settings</b> to finish the rest of the setup.</div>

        <a class="helpLink" href="https://wiki.crosswatch.app/" target="_blank" rel="noopener noreferrer">
          <span class="helpCopy">
            <span class="helpEyebrow">Documentation</span>
            <span class="helpTitle">Open the CrossWatch Wiki</span>
            <span class="helpSub">Setup guides, first-run help, and troubleshooting in one place.</span>
          </span>
          <span class="helpIcon" aria-hidden="true"><span class="material-symbols-rounded">menu_book</span></span>
        </a>

        <details class="disc">
          <summary><span class="material-symbols-rounded" aria-hidden="true">gavel</span>Disclaimer</summary>
          <div class="discBody">
            <p>This is an independent, community-maintained project and is not affiliated with, endorsed by, or sponsored by Plex, Emby, Jellyfin, Trakt, TMDB, SIMKL, Tautulli, AniList or MDBList. Use at your own risk.</p>
            <p>All product names, logos, and brands are property of their respective owners and used for identification only.</p>
            <p>Interacts with third-party services; you are responsible for complying with their Terms of Use and API rules.</p>
            <p>Provided "as is," without warranties or guarantees.</p>
          </div>
        </details>
      `;
      const foot = `
        <div class="mini">Sign-in is required before setup continues.</div>
        <div class="btns"><button class="btn primary" type="button" data-x="next">Next</button></div>
      `;
      hostEl.innerHTML = _chrome(ver, crossWatchLogo, body, foot);
      hostEl.querySelector('[data-x="next"]')?.addEventListener("click", () => {
        state.step = "credentials";
        render();
      });
    }

    function renderCredentials() {
      const badge = resetRequired ? "Recovery required" : "Required security step";
      const headline = resetRequired ? "Set a new sign-in username and password" : "Create your sign-in credentials";
      const lede = resetRequired
        ? "Authentication was reset at startup. Set a new username and password to continue."
        : "You need a username and password before CrossWatch opens the rest of Settings.";
      const helper = resetRequired ? "Sign-in was reset and must be configured again before continuing." : "Sign-in is required before first use.";
      const pausedNote = resetRequired
        ? `<div class="card"><div class="h">Background activity is paused</div><div class="p">Sync summary and log streams stay paused until you finish setting the new sign-in credentials.</div></div>`
        : "";
      const body = `
        <div class="badge"><span class="dot" aria-hidden="true"></span><span style="font-weight:900">${escapeHtml(badge)}</span></div>
        <div class="headline">${escapeHtml(headline)}</div>
        <div class="lede">${escapeHtml(lede)}</div>
        ${renderAppAuthFields({
          idPrefix: "sw-auth",
          state,
          errorId: "sw-auth-error",
        })}
        ${pausedNote}
      `;
      const foot = `
        <div class="mini">${escapeHtml(helper)}</div>
        <div class="btns">
          ${resetRequired ? "" : '<button class="btn" type="button" data-x="back">Back</button>'}
          <button class="btn primary" type="button" data-x="save"${state.saving ? " disabled" : ""}>${state.saving ? "Saving..." : (resetRequired ? "Save New Sign-in" : "Enable Sign-in")}</button>
        </div>
      `;
      hostEl.innerHTML = _chrome(
        ver,
        crossWatchLogo,
        body,
        foot,
        resetRequired
          ? { title: "CrossWatch authentication reset", subtitle: "Recovery setup", ariaLabel: "CrossWatch authentication reset" }
          : undefined,
      );
      if (!resetRequired) {
        hostEl.querySelector('[data-x="back"]')?.addEventListener("click", () => {
          syncAppAuthState(hostEl, state);
          state.step = "intro";
          render();
        });
      }
      const saveBtn = hostEl.querySelector('[data-x="save"]');
      wireLiveAppAuthValidation(hostEl, state, "sw-auth-error", saveBtn);
      saveBtn?.addEventListener("click", (e) => submitCredentials(e.currentTarget));
      hostEl.querySelector("#sw-auth-pass2")?.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !state.saving) submitCredentials(hostEl.querySelector('[data-x="save"]'));
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
