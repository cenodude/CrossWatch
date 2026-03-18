/* assets/js/modals/setup-wizard/index.js */
/* CrossWatch - setup wizard modal component */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */


const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || Date.now(); }
  catch { return window.__CW_VERSION__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(String(_cwV));

const {
  appAuthFormCss,
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
  const ids = ["sec-auth", "sec-meta", "sec-sync", "sec-scrobbler", "sc-sec-webhook", "sc-sec-watch"];
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
    <style>
      #setup-host{--w:900px;position:relative;overflow:hidden;min-width:min(var(--w),94vw);max-width:94vw;color:#eaf0ff;border-radius:18px;
        border:1px solid rgba(255,255,255,.08);
        background:
          radial-gradient(900px circle at 18% 18%, rgba(150,70,255,.22), transparent 55%),
          radial-gradient(900px circle at 92% 10%, rgba(60,140,255,.18), transparent 55%),
          radial-gradient(800px circle at 55% 110%, rgba(60,255,215,.08), transparent 60%),
          rgba(7,8,11,.92);
        box-shadow:0 30px 90px rgba(0,0,0,.70), inset 0 1px 0 rgba(255,255,255,.04);
        backdrop-filter:saturate(135%) blur(10px)
      }
      #setup-host:before{content:"";position:absolute;inset:-120px;pointer-events:none;
        background:conic-gradient(from 180deg at 50% 50%, rgba(150,70,255,.0), rgba(150,70,255,.30), rgba(60,140,255,.24), rgba(60,255,215,.10), rgba(150,70,255,.0));
        filter:blur(90px);opacity:.35;transform:translate3d(0,0,0);
        animation:setupGlow 16s ease-in-out infinite alternate
      }
      @keyframes setupGlow{from{transform:translate(-16px,-10px) scale(1)}to{transform:translate(16px,12px) scale(1.03)}}
      @media (prefers-reduced-motion: reduce){#setup-host:before{animation:none}}

      #setup-host .head{position:relative;display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01))
      }
      #setup-host .logoWrap{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);
        box-shadow:0 12px 30px rgba(0,0,0,.40), inset 0 1px 0 rgba(255,255,255,.04)
      }
      #setup-host .logo{width:30px;height:30px;opacity:.95;filter:drop-shadow(0 10px 16px rgba(0,0,0,.45))}
      #setup-host .title{font-weight:950;letter-spacing:.2px;font-size:15px;line-height:1.1;text-transform:uppercase;opacity:.90}
      #setup-host .sub{opacity:.72;font-size:12px;margin-top:2px}
      #setup-host .v{margin-left:auto;opacity:.85;font-weight:900;font-size:12px;padding:6px 10px;border-radius:999px;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)
      }
      #setup-host .body{position:relative;padding:16px 16px 8px 16px;max-height:72vh;overflow:auto}
      #setup-host .badge{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border-radius:999px;
        background:rgba(0,0,0,.24);border:1px solid rgba(255,255,255,.08)}
      #setup-host .badge .dot{width:8px;height:8px;border-radius:999px;background:rgba(150,70,255,.90);box-shadow:0 0 0 4px rgba(150,70,255,.18)}
      #setup-host .headline{font-weight:950;font-size:20px;line-height:1.15;margin:10px 0 8px 0}
      #setup-host .lede{opacity:.84;max-width:72ch}
      #setup-host .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:14px 0 10px 0}
      @media (max-width:780px){#setup-host .grid{grid-template-columns:1fr}}
      #setup-host .card{display:flex;gap:10px;align-items:flex-start;padding:12px 12px;border-radius:14px;
        background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 30px rgba(0,0,0,.32)}
      #setup-host .ico{width:34px;height:34px;border-radius:12px;display:grid;place-items:center;flex:0 0 auto;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}
      #setup-host .card b{font-weight:950}
      #setup-host .muted{opacity:.78;font-size:12.5px;margin-top:2px}
      #setup-host .tip{margin:10px 0 10px 0;font-size:12.5px;opacity:.84}
      #setup-host code{opacity:.95}
      #setup-host details.disc{margin-top:10px;border-radius:14px;border:1px solid rgba(255,255,255,.08);
        background:rgba(255,255,255,.02);box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
      #setup-host details.disc>summary{cursor:pointer;list-style:none;padding:10px 12px;font-weight:950;display:flex;align-items:center;gap:8px}
      #setup-host details.disc>summary::-webkit-details-marker{display:none}
      #setup-host .discBody{padding:0 12px 12px 12px;opacity:.82;font-size:12.5px;line-height:1.38}
      #setup-host .discBody p{margin:8px 0 0 0}
      #setup-host .helpLink{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-top:12px;padding:14px 16px;border-radius:14px;text-decoration:none;color:#eef3ff;background:linear-gradient(135deg,rgba(150,70,255,.18),rgba(60,140,255,.14));border:1px solid rgba(150,70,255,.22);box-shadow:0 14px 34px rgba(0,0,0,.24), inset 0 1px 0 rgba(255,255,255,.04);transition:transform .16s ease, filter .16s ease, border-color .16s ease}
      #setup-host .helpLink:hover{transform:translateY(-1px);filter:brightness(1.06);border-color:rgba(150,70,255,.32)}
      #setup-host .helpCopy{display:grid;gap:4px}
      #setup-host .helpEyebrow{font-size:11px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;opacity:.72}
      #setup-host .helpTitle{font-size:16px;font-weight:950;line-height:1.15}
      #setup-host .helpSub{font-size:12.5px;line-height:1.4;opacity:.8}
      #setup-host .helpIcon{width:42px;height:42px;border-radius:12px;display:grid;place-items:center;flex:0 0 auto;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}
      #setup-host .helpIcon .material-symbols-rounded{font-size:22px}
      ${appAuthFormCss("#setup-host")}
      #setup-host .foot{position:relative;display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 16px;
        border-top:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01))}
      #setup-host .mini{opacity:.68;font-size:12px}
      #setup-host .btns{display:flex;gap:10px;align-items:center}
      #setup-host .btn{appearance:none;border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:10px 14px;font-weight:950;cursor:pointer;
        background:rgba(255,255,255,.04);color:#eaf0ff}
      #setup-host .btn:hover{filter:brightness(1.06)}
      #setup-host .btn[disabled]{opacity:.62;cursor:progress}
      #setup-host .btn.primary{border-color:rgba(150,70,255,.35);
        background:linear-gradient(135deg,rgba(150,70,255,.92),rgba(60,140,255,.82));box-shadow:0 16px 50px rgba(0,0,0,.48)}
    </style>
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
              <div><b>Authentication providers</b></div>
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
