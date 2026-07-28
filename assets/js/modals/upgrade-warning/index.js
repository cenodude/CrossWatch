/* assets/js/modals/upgrade-warning/index.js */
/* CrossWatch - upgrade warning modal component */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const NOTES_ENDPOINT = "/api/update";
const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || Date.now(); }
  catch { return window.__CW_VERSION__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(String(_cwV));

const { getJson, postJson } = await import(_cwVer("../core/net.js"));
const { renderNotesMarkup } = await import(_cwVer("./notes.js"));
const {
  escapeHtml,
  fetchAppAuthStatus,
  hasEnabledAppAuth,
  renderAppAuthFields,
  saveRequiredAppAuth,
  setModalDismissible,
  setModalShellInline,
  syncAppAuthState,
  validateAppAuthState,
  wireLiveAppAuthValidation,
} = await import(_cwVer("../core/app-auth-setup.js"));

function _norm(v) {
  return String(v || "").replace(/^v/i, "").trim();
}

function _cmp(a, b) {
  const pa = _norm(a).split(".").map((n) => parseInt(n, 10) || 0);
  const pb = _norm(b).split(".").map((n) => parseInt(n, 10) || 0);
  for (let i = 0; i < Math.max(pa.length, pb.length); i += 1) {
    const da = pa[i] || 0;
    const db = pb[i] || 0;
    if (da !== db) return da > db ? 1 : -1;
  }
  return 0;
}

async function _runConfigMigration() {
  return postJson("/api/config/migrate");
}

async function _runFullReset() {
  return postJson("/api/maintenance/reset-all-default", {
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ restart: true }),
  });
}

async function _waitForScheduledRestart() {
  try {
    window.cxCloseModal?.();
  } catch {}

  try {
    window.cwShowApplyOverlay?.("Restarting CrossWatch", "Restarting container / service...", 12);
  } catch {}

  setTimeout(() => {
    try { window.location.reload(); } catch {}
  }, 12000);
}

async function runCleanupAndRestart(btn) {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  try {
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
      btn.textContent = "Cleaning...";
    }
  } catch {}

  try {
    const res = await _runFullReset();
    if (res && res.ok === false) {
      throw new Error(String(res.error || (res.errors || []).join(", ") || "reset_failed"));
    }
    notify(res && res.backup
      ? `Cleanup completed. Config backup created: ${res.backup}`
      : "Cleanup completed. CrossWatch will restart now.");
    await _waitForScheduledRestart();
  } catch (e) {
    console.warn("[upgrade-warning] cleanup failed", e);
    notify("Cleanup failed. Check logs.");
    try {
      if (btn) {
        btn.disabled = false;
        btn.classList.remove("busy");
        btn.textContent = "Clean & Reboot";
      }
    } catch {}
  }
}

export default {
  async mount(hostEl, props = {}) {
    if (!hostEl) return;

    const notify = window.notify || ((m) => console.log("[notify]", m));
    const cur = _norm(props.current_version || window.__CW_VERSION__ || "0.0.0");
    const rawCfgVer = props.config_version;
    const hasCfgVer = rawCfgVer != null && String(rawCfgVer).trim() !== "";
    const cfg = hasCfgVer ? _norm(rawCfgVer) : "";
    const requiresCleanReset = !hasCfgVer || _cmp(cfg, "0.9.12") < 0;

    const shell = hostEl.closest(".cx-modal-shell");
    const state = {
      authReady: false,
      step: "intro",
      username: "admin",
      password: "",
      password2: "",
      error: "",
      saving: false,
      autoSaveStarted: false,
      autoSaveDone: false,
      autoSaveFailed: false,
      autoSaveMessage: "",
      notesLoaded: false,
      notesVisible: false,
      notesBody: "",
      notesMeta: "",
      notesUrl: "https://github.com/cenodude/CrossWatch/releases",
    };

    try {
      const authStatus = await fetchAppAuthStatus();
      state.authReady = !!(
        authStatus
        && !authStatus.reset_required
        && hasEnabledAppAuth(authStatus)
        && (requiresCleanReset ? authStatus.authenticated === true : true)
      );
    } catch {
      state.authReady = false;
    }
    if (requiresCleanReset) {
      state.step = state.authReady ? "cleanup" : "credentials";
    } else {
      state.step = state.authReady ? "migrate" : "intro";
    }

    async function ensureNotesLoaded() {
      if (state.step !== "migrate" || state.notesLoaded) return;
      state.notesLoaded = true;
      try {
        const j = await getJson(NOTES_ENDPOINT, { cache: "no-store" });
        const body = String(j.body || "").trim();
        state.notesUrl = String(j.html_url || j.url || state.notesUrl || "").trim() || state.notesUrl;
        if (!body) return;
        const latest = _norm(j.latest_version || j.latest || "");
        const published = String(j.published_at || "").trim();
        state.notesBody = renderNotesMarkup(body);
        state.notesMeta = `Latest${latest ? ` v${latest}` : ""}${published ? ` - ${published}` : ""}`;
        state.notesVisible = true;
        render();
      } catch {}
    }

    async function ensureAutoSaved() {
      if (requiresCleanReset || state.autoSaveStarted) return;
      state.autoSaveStarted = true;
      state.autoSaveFailed = false;
      state.autoSaveMessage = "Saving the updated config format in the background...";
      render();

      try {
        const res = await _runConfigMigration();
        if (res && res.ok === false) {
          throw new Error(String(res.error || "config_save_failed"));
        }
        state.autoSaveDone = true;
        state.autoSaveMessage = res && res.backup
          ? `Saved the updated config format. Backup created: ${res.backup}`
          : "Saved the updated config format.";
        notify("Upgrade settings saved.");
      } catch (e) {
        console.warn("[upgrade-warning] auto-save failed", e);
        state.autoSaveFailed = true;
        state.autoSaveMessage = "Automatic save failed. Check logs before continuing.";
        notify("Automatic upgrade save failed. Check logs.");
      } finally {
        render();
      }
    }

    async function submitCredentials() {
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
        state.authReady = true;
        state.saving = false;
        state.error = "";
        state.password = "";
        state.password2 = "";
        state.step = requiresCleanReset ? "cleanup" : "migrate";
        notify(requiresCleanReset
          ? "Sign-in saved. You can now run the clean reset."
          : "Sign-in saved. CrossWatch is updating the config in the background.");
        render();
        if (!requiresCleanReset) ensureAutoSaved();
        return;
      } catch (err) {
        state.saving = false;
        state.error = String(err?.message || "Failed to save sign-in settings.");
        render();
      }
    }

    function layout(body, foot) {
      return `

        <div id="upg-host">
          <div class="head">
            <div class="icon" aria-hidden="true"><span class="material-symbols-rounded">system_update</span></div>
            <div>
              <div class="t">${requiresCleanReset ? "Unsupported config detected" : "Config version notice"}</div>
              ${requiresCleanReset ? '<div class="sub">Pre-v0.9.12 requires a clean reset</div>' : ""}
            </div>
            <div class="pill">
              <span class="b">Engine v${cur}</span>
              <span class="b">${hasCfgVer ? `Config v${cfg}` : "Config: Legacy"}</span>
            </div>
          </div>
          <div class="body">${body}</div>
          <div class="foot">${foot}</div>
        </div>
      `;
    }

    function migrationBody() {
      return `
        <div class="card">
          <div class="h">Current status</div>
          <div class="p">${escapeHtml(state.autoSaveMessage || "Preparing the upgrade flow...")}</div>
          ${state.autoSaveFailed ? '<div class="p" style="color:#ffb3b3">Automatic save failed. Review logs before continuing.</div>' : ""}
        </div>
        <div class="card">
          <div class="h">Release notes</div>
          <div class="p" style="opacity:.72">${state.notesVisible ? escapeHtml(state.notesMeta) : "Open the full release notes if inline notes are unavailable."}</div>
          ${state.notesVisible
            ? `<div class="notes">${state.notesBody}</div>`
            : `<div class="p">Release notes could not be loaded in-app right now. <a href="${escapeHtml(state.notesUrl)}" target="_blank" rel="noopener noreferrer">Open release notes</a>.</div>`}
        </div>
        <div class="card">
          <div class="h">Need help?</div>
          <div class="p"><b>Tip:</b> After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
          <a class="helpLink" href="https://wiki.crosswatch.app/" target="_blank" rel="noopener noreferrer">
            <span class="helpCopy">
              <span class="helpEyebrow">Documentation</span>
              <span class="helpTitle">Open the CrossWatch Wiki</span>
              <span class="helpSub">Setup guides, upgrade notes, and troubleshooting in one place.</span>
            </span>
            <span class="helpIcon" aria-hidden="true"><span class="material-symbols-rounded">menu_book</span></span>
          </a>
        </div>
      `;
    }

    function cleanupBody() {
      return `
        <div class="card warn">
          <div class="h">Clean reset required</div>
          <div class="p">Configs older than <b>v0.9.12</b> are no longer supported. CrossWatch must clean everything using the maintenance reset flow, create a backup of <code>config.json</code>, and reboot.</div>
        </div>
        <div class="card">
          <div class="h">What will be cleaned</div>
          <div class="p">This matches the maintenance <b>Reset all to default</b> action: local state, provider cache, tracker files, reports, metadata cache, and TLS material are removed. Snapshots are kept.</div>
        </div>
        <div class="card">
          <div class="h">What happens next</div>
          <div class="p">Click <b>Clean &amp; Reboot</b> to start over with a fresh config baseline. This runs before any username/password upgrade checks.</div>
        </div>
      `;
    }

    function renderIntro() {
      setModalDismissible(false);
      hostEl.innerHTML = layout(`
        <div class="card warn">
          <div class="h">Migration now requires admin credentials</div>
          <div class="p">Before this supported upgrade can continue, CrossWatch needs a local admin username and password to be configured.</div>
        </div>
        <div class="card">
          <div class="h">What happens next</div>
          <div class="p">Click <b>Next</b>, create the admin credentials, and CrossWatch will save the updated config in the background.</div>
        </div>
      `, `<button class="btn primary" type="button" data-x="next">Next</button>`);
      setModalShellInline(shell);
      hostEl.querySelector('[data-x="next"]')?.addEventListener("click", () => {
        state.step = "credentials";
        render();
      });
    }

    function renderCredentials() {
      setModalDismissible(false);
      hostEl.innerHTML = layout(`
        <div class="card">
          <div class="h">Create admin credentials</div>
          <div class="p">${requiresCleanReset
            ? "You must finish this step before CrossWatch can run the clean reset."
            : "You must finish this step before the supported upgrade flow can continue."}</div>
        </div>
        <div class="card">
          <div class="h">${requiresCleanReset ? "Clean reset will unlock afterwards" : "Background save will start afterwards"}</div>
          <div class="p">${requiresCleanReset
            ? "As soon as sign-in is configured, the reset action uses your authenticated session instead of a public setup endpoint."
            : "As soon as sign-in is configured, CrossWatch saves the new config keys automatically."}</div>
        </div>
        <div class="card">
          ${renderAppAuthFields({
            idPrefix: "upg-auth",
            state,
            wrap: false,
          })}
        </div>
      `, `
        <button class="btn" type="button" data-x="back">Back</button>
        <button class="btn primary" type="button" data-x="save"${state.saving ? " disabled" : ""}>${state.saving ? "Saving..." : "Enable Sign-in"}</button>
      `);
      setModalShellInline(shell);
      hostEl.querySelector('[data-x="back"]')?.addEventListener("click", () => {
        syncAppAuthState(hostEl, state);
        state.step = "intro";
        render();
      });
      const saveBtn = hostEl.querySelector('[data-x="save"]');
      wireLiveAppAuthValidation(hostEl, state, "", saveBtn);
      saveBtn?.addEventListener("click", () => submitCredentials());
      hostEl.querySelector("#upg-auth-pass2")?.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !state.saving) submitCredentials();
      });
    }

    function renderCleanup() {
      setModalDismissible(false);
      hostEl.innerHTML = layout(cleanupBody(), `
        <button class="btn danger" type="button" data-x="cleanup">Clean &amp; Reboot</button>
      `);
      setModalShellInline(shell);
      hostEl.querySelector('[data-x="cleanup"]')?.addEventListener("click", (e) => runCleanupAndRestart(e.currentTarget));
    }

    function renderMigrate() {
      setModalDismissible(true);
      hostEl.innerHTML = layout(migrationBody(), `
        <button class="btn primary" type="button" data-x="ok"${state.autoSaveFailed ? " disabled" : ""}>OK</button>
      `);
      setModalShellInline(shell);
      hostEl.querySelector('[data-x="ok"]')?.addEventListener("click", () => {
        try { window.cxCloseModal?.(); } catch {}
      });
      ensureNotesLoaded();
    }

    function render() {
      if (state.step === "cleanup") return renderCleanup();
      if (state.step === "credentials") return renderCredentials();
      if (state.step === "migrate") return renderMigrate();
      return renderIntro();
    }

    render();
    if (!requiresCleanReset && state.authReady) ensureAutoSaved();
  },

  unmount() {
    setModalDismissible(true);
  }
};
