/* assets/js/modals/upgrade-warning/index.js */
/* CrossWatch - upgrade warning modal component */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const TR = (key, vars) => I18N()?.rich?.(key, vars) ?? key;
const NOTES_ENDPOINT = "/api/version/release-notes";
const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || Date.now(); }
  catch { return window.__CW_VERSION__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(String(_cwV));

const { getJson, postJson } = await import(_cwVer("../core/net.js"));
const { parseReleaseNotes, renderInlineMarkup, releaseNotesUrl } = await import(_cwVer("./notes.js"));
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
    window.cwShowApplyOverlay?.(T("modals.upgrade.restarting"), T("modals.upgrade.restartingDetail"), 12);
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
      btn.textContent = T("modals.upgrade.cleaning");
    }
  } catch {}

  try {
    const res = await _runFullReset();
    if (res && res.ok === false) {
      throw new Error(String(res.error || (res.errors || []).join(", ") || "reset_failed"));
    }
    notify(res && res.backup
      ? T("modals.upgrade.cleanupBackup", {path: res.backup})
      : T("modals.upgrade.cleanupRestart"));
    await _waitForScheduledRestart();
  } catch (e) {
    console.warn("[upgrade-warning] cleanup failed", e);
    notify(T("modals.upgrade.cleanupFailed"));
    try {
      if (btn) {
        btn.disabled = false;
        btn.classList.remove("busy");
        btn.textContent = T("modals.upgrade.cleanReboot");
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
      notesLoading: true,
      highlights: [],
      wikiLinks: [],
      upgradeNote: "",
      summary: "",
      showAllHighlights: false,
      backup: "",
      adjustedSettings: false,
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
        state.notesUrl = releaseNotesUrl(j.html_url, state.notesUrl);
        if (_norm(j.version) === cur) {
          const notes = parseReleaseNotes(j.body);
          state.highlights = notes.highlights;
          state.wikiLinks = notes.wikiLinks;
          state.upgradeNote = notes.upgradeNote;
          state.summary = notes.summary;
        }
      } catch {} finally {
        state.notesLoading = false;
        render();
      }
    }

    async function ensureAutoSaved() {
      if (requiresCleanReset || state.autoSaveStarted) return;
      state.autoSaveStarted = true;
      state.autoSaveFailed = false;
      state.autoSaveMessage = T("modals.upgrade.savingFormat");
      render();

      try {
        const res = await _runConfigMigration();
        if (res && res.ok === false) {
          throw new Error(String(res.error || "config_save_failed"));
        }
        state.autoSaveDone = true;
        state.backup = String(res.backup || "").split(/[\\/]/).pop() || "";
        state.adjustedSettings = [res.forced_paths, res.profile_cleanup_paths, res.obsolete_paths].some(paths => Array.isArray(paths) && paths.length);
        state.autoSaveMessage = res && res.backup
          ? T("modals.upgrade.savedBackup", {path: res.backup})
          : T("modals.upgrade.savedFormat");
        notify(T("modals.upgrade.settingsSaved"));
      } catch (e) {
        console.warn("[upgrade-warning] auto-save failed", e);
        state.autoSaveFailed = true;
        state.autoSaveMessage = T("modals.upgrade.autoSaveFailed");
        notify(T("modals.upgrade.autoSaveFailedShort"));
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
          ? T("modals.upgrade.signInSavedReset")
          : T("modals.upgrade.signInSavedUpdating"));
        render();
        if (!requiresCleanReset) ensureAutoSaved();
        return;
      } catch (err) {
        state.saving = false;
        state.error = String(err?.message || T("modals.upgrade.signInFailed"));
        render();
      }
    }

    function layout(body, foot) {
      return `

        <div id="upg-host" role="dialog" aria-modal="true" aria-labelledby="upg-title">
          <div class="head">
            <div class="icon" aria-hidden="true"><span class="material-symbols-rounded">${state.autoSaveDone ? "fact_check" : "system_update"}</span></div>
            <div>
              <h1 class="t" id="upg-title">${requiresCleanReset ? T("modals.upgrade.unsupported") : state.step !== "migrate" ? T("modals.upgrade.updateConfig") : state.autoSaveFailed ? T("modals.upgrade.updateFailed") : state.autoSaveDone ? T("modals.upgrade.updated") : T("modals.upgrade.updating")}</h1>
              <div class="sub">${requiresCleanReset ? T("modals.upgrade.preRequires") : state.step !== "migrate" ? T("modals.upgrade.setupSignIn") : state.autoSaveFailed ? T("modals.upgrade.notCompleted") : state.autoSaveDone ? TH(state.backup ? "modals.upgrade.migratedBackup" : "modals.upgrade.migrated") : T("modals.upgrade.beingUpdated")}</div>
            </div>
            <div class="pill">
              <span class="b">${hasCfgVer ? `${TH("modals.upgrade.configVersion", {version: cfg})}${state.autoSaveDone ? ` &rarr; v${escapeHtml(cur)}` : ""}` : TH("modals.upgrade.configLegacy")}</span>
              <span class="b">${TH("about.engine", {version: cur})}</span>
            </div>
          </div>
          <div class="body">${body}</div>
          <div class="foot">${foot}</div>
        </div>
      `;
    }

    function migrationBody() {
      const success = state.autoSaveDone;
      const icon = state.autoSaveFailed ? "error" : success ? "check_circle" : "sync";
      const highlights = state.showAllHighlights ? state.highlights : state.highlights.slice(0, 5);
      return `
        <section class="upg-result ${state.autoSaveFailed ? "upg-failed" : success ? "upg-success" : ""}" role="status" aria-live="polite">
          <span class="material-symbols-rounded upg-result-icon" aria-hidden="true">${icon}</span>
          <div><h2>${state.autoSaveFailed ? T("modals.upgrade.needsAttention") : success ? T("modals.upgrade.success") : T("modals.upgrade.updatingYour")}</h2>
          ${success ? `<ul class="upg-checks">
            <li><span class="material-symbols-rounded" aria-hidden="true">check_circle</span>${state.adjustedSettings ? T("modals.upgrade.migratedAdjusted") : T("modals.upgrade.retained")}</li>
            <li><span class="material-symbols-rounded" aria-hidden="true">${state.backup ? "check_circle" : "info"}</span>${state.backup ? `${TH("modals.upgrade.backupCreated")} <code>${escapeHtml(state.backup)}</code>` : T("modals.upgrade.noBackup")}</li>
            <li><span class="material-symbols-rounded" aria-hidden="true">${state.upgradeNote ? "info" : "check_circle"}</span>${state.upgradeNote ? T("modals.upgrade.reviewNote") : T("modals.upgrade.migrationComplete")}</li>
          </ul>` : `<p>${escapeHtml(state.autoSaveMessage || T("modals.upgrade.preparing"))}</p>`}</div>
        </section>
        ${state.upgradeNote ? `<section class="upg-upgrade-note" aria-label="${TH("modals.upgrade.upgradeNote")}"><h2><span class="material-symbols-rounded" aria-hidden="true">info</span>${TH("modals.upgrade.upgradeNote")}</h2><div>${state.upgradeNote.split(/\n\s*\n/).map(text => `<p>${renderInlineMarkup(text.replace(/\n/g, " "))}</p>`).join("")}</div></section>` : ""}
        <section class="upg-changes" aria-label="${TH("modals.upgrade.whatChanged")}"><h2>${TH("modals.upgrade.whatChanged")}</h2>
          ${state.summary ? `<div class="upg-summary">${state.summary.split(/\n\s*\n/).map(text => `<p>${renderInlineMarkup(text.replace(/\n/g, " "))}</p>`).join("")}</div>` : ""}
          ${highlights.length ? `<ul class="upg-highlights">${highlights.map((item, index) => `<li><span class="material-symbols-rounded" aria-hidden="true">${["auto_awesome", "tune", "sync_alt", "dashboard", "insights"][index % 5]}</span><div>${item.children.length ? `<details><summary>${renderInlineMarkup(item.text)}</summary><ul>${item.children.map(text => `<li>${renderInlineMarkup(text)}</li>`).join("")}</ul></details>` : renderInlineMarkup(item.text)}</div></li>`).join("")}</ul>` : `<p class="upg-muted">${state.notesLoading ? "Loading release highlights..." : "Highlights are unavailable for this build. You can check the full release notes below."}</p>`}
          ${state.highlights.length > 5 ? `<button class="upg-show-all" type="button" data-x="highlights" aria-expanded="${state.showAllHighlights}">${state.showAllHighlights ? TH("modals.upgrade.fewerHighlights") : TH("modals.upgrade.allHighlights", {count: state.highlights.length})}</button>` : ""}
          <a class="upg-release-link" href="${escapeHtml(state.notesUrl)}" target="_blank" rel="noopener noreferrer">${TH("modals.upgrade.viewNotes")} <span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></a>
        </section>
        ${state.wikiLinks.length ? `<section class="upg-wiki" aria-label="${TH("modals.upgrade.updatedWiki")}"><h2>${TH("modals.upgrade.updatedWiki")}</h2><ul>${state.wikiLinks.map(item => `<li><a href="${escapeHtml(item.url)}" title="${escapeHtml(item.url)}" target="_blank" rel="noopener noreferrer"><span class="material-symbols-rounded" aria-hidden="true">menu_book</span>${escapeHtml(item.title)}<span class="material-symbols-rounded" aria-hidden="true">open_in_new</span></a></li>`).join("")}</ul></section>` : ""}
        <aside class="upg-help"><span class="material-symbols-rounded" aria-hidden="true">info</span><p>${TR("modals.upgrade.refreshHelp")}</p><a href="https://wiki.crosswatch.app/" target="_blank" rel="noopener noreferrer"><span class="material-symbols-rounded" aria-hidden="true">menu_book</span>${TH("modals.upgrade.openDocs")}</a></aside>
      `;
    }

    function cleanupBody() {
      return `
        <div class="card warn">
          <div class="h">${TH("modals.upgrade.cleanRequired")}</div>
          <div class="p">${TR("modals.upgrade.olderUnsupported")}</div>
        </div>
        <div class="card">
          <div class="h">${TH("modals.upgrade.whatCleaned")}</div>
          <div class="p">${TR("modals.upgrade.cleanedDetail")}</div>
        </div>
        <div class="card">
          <div class="h">${TH("modals.upgrade.whatNext")}</div>
          <div class="p">${TR("modals.upgrade.cleanNext")}</div>
        </div>
      `;
    }

    function renderIntro() {
      setModalDismissible(false);
      hostEl.innerHTML = layout(`
        <div class="card warn">
          <div class="h">${TH("modals.upgrade.needsAdmin")}</div>
          <div class="p">${TH("modals.upgrade.needsAdminDetail")}</div>
        </div>
        <div class="card">
          <div class="h">${TH("modals.upgrade.whatNext")}</div>
          <div class="p">${TR("modals.upgrade.credsNext")}</div>
        </div>
      `, `<button class="btn primary" type="button" data-x="next">${TH("modals.upgrade.next")}</button>`);
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
          <div class="h">${TH("modals.upgrade.createAdmin")}</div>
          <div class="p">${requiresCleanReset
            ? T("modals.upgrade.finishReset")
            : T("modals.upgrade.finishUpgrade")}</div>
        </div>
        <div class="card">
          <div class="h">${requiresCleanReset ? T("modals.upgrade.resetUnlocks") : T("modals.upgrade.bgSaveStarts")}</div>
          <div class="p">${requiresCleanReset
            ? T("modals.upgrade.resetUsesSession")
            : T("modals.upgrade.savesKeys")}</div>
        </div>
        <div class="card">
          ${renderAppAuthFields({
            idPrefix: "upg-auth",
            state,
            wrap: false,
          })}
        </div>
      `, `
        <button class="btn" type="button" data-x="back">${TH("modals.upgrade.back")}</button>
        <button class="btn primary" type="button" data-x="save"${state.saving ? " disabled" : ""}>${state.saving ? T("modals.upgrade.saving") : T("modals.upgrade.enableSignIn")}</button>
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
        <button class="btn danger" type="button" data-x="cleanup">${TH("modals.upgrade.cleanReboot")}</button>
      `);
      setModalShellInline(shell);
      hostEl.querySelector('[data-x="cleanup"]')?.addEventListener("click", (e) => runCleanupAndRestart(e.currentTarget));
    }

    function renderMigrate() {
      const waiting = !state.autoSaveDone && !state.autoSaveFailed;
      setModalDismissible(!waiting);
      hostEl.innerHTML = layout(migrationBody(), `
        <button class="btn" type="button" data-x="close"${waiting ? " disabled" : ""}>${TH("modals.upgrade.close")}</button>
        <button class="btn primary" type="button" data-x="continue"${!state.autoSaveDone ? " disabled" : ""}>${TH("modals.upgrade.continue")} <span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></button>
      `);
      setModalShellInline(shell);
      hostEl.querySelector('[data-x="close"]')?.addEventListener("click", () => {
        window.cxCloseModal?.();
      });
      hostEl.querySelector('[data-x="continue"]')?.addEventListener("click", () => {
        window.cxCloseModal?.();
      });
      hostEl.querySelector('[data-x="highlights"]')?.addEventListener("click", () => {
        state.showAllHighlights = !state.showAllHighlights;
        render();
        hostEl.querySelector('[data-x="highlights"]')?.focus({preventScroll: true});
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
