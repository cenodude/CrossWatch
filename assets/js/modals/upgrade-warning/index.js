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
  appAuthFormCss,
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

async function _restartAfterMigration() {
  try {
    window.cxCloseModal?.();
  } catch {}

  setTimeout(() => {
    try {
      if (window.cwRestartCrossWatchWithOverlay) {
        window.cwRestartCrossWatchWithOverlay();
        return;
      }
    } catch {}

    fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" }).finally(() => {
      window.location.reload();
    });
  }, 150);
}

async function _pauseSchedulerOnce() {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  const KEY = "cw_stop_scheduler_pre_0911";

  try {
    if (window.__CW_STOP_SCHED_0911_DONE__ || window.__CW_STOP_SCHED_0911_INFLIGHT__) return;
  } catch {}

  try {
    if (localStorage.getItem(KEY) === "1") {
      try { window.__CW_STOP_SCHED_0911_DONE__ = true; } catch {}
      return;
    }
  } catch {}

  try { window.__CW_STOP_SCHED_0911_INFLIGHT__ = true; } catch {}

  try {
    await postJson("/api/scheduling/stop");
    notify("Scheduler stopped until you complete migration.");
    try {
      localStorage.setItem(KEY, "1");
      window.__CW_STOP_SCHED_0911_DONE__ = true;
    } catch {}
  } catch (e) {
    console.warn("[upgrade-warning] scheduler stop failed", e);
  } finally {
    try { window.__CW_STOP_SCHED_0911_INFLIGHT__ = false; } catch {}
  }
}

async function saveNow(btn) {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  try {
    if (btn && btn.dataset && btn.dataset.done === "1") return;
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
      btn.textContent = "Migrating...";
    }
  } catch {}

  try {
    const res = await _runConfigMigration();
    notify(res && res.backup
      ? `Migrated. Config backup created: ${res.backup}`
      : "Migrated. Config updated and backup completed.");

    try {
      if (btn) {
        btn.classList.remove("busy");
        btn.textContent = "MIGRATED";
        btn.disabled = true;
        btn.dataset.done = "1";
      }
    } catch {}

    await _restartAfterMigration();
  } catch (e) {
    console.warn("[upgrade-warning] save failed", e);
    notify("Save failed. Check logs.");
    try {
      if (btn && (!btn.dataset || btn.dataset.done !== "1")) {
        btn.disabled = false;
        btn.classList.remove("busy");
        btn.textContent = "MIGRATE";
      }
    } catch {}
  }
}

async function migrateNow(btn, fullClean = false) {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  try {
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
      btn.textContent = "Migrating...";
    }
  } catch {}

  try {
    const ops = [
      { url: "/api/maintenance/clear-state", opts: {} },
      { url: "/api/maintenance/clear-cache", opts: {} },
      { url: "/api/maintenance/clear-metadata-cache", opts: {} },
    ];

    if (fullClean) {
      ops.push(
        {
          url: "/api/maintenance/crosswatch-tracker/clear",
          opts: {
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ clear_state: true, clear_snapshots: true }),
          },
        },
        {
          url: "/api/maintenance/reset-stats",
          opts: {
            headers: { "Content-Type": "application/json" },
            body: "{}",
          },
        },
        { url: "/api/maintenance/reset-currently-watching", opts: {} },
      );
    }

    for (const op of ops) {
      await postJson(op.url, op.opts);
    }

    const res = await _runConfigMigration();
    notify(fullClean
      ? (res && res.backup
          ? `Migration completed. Legacy state/cache cleared. Config backup created: ${res.backup}`
          : "Migration completed. Legacy state/cache cleared. Config backup created.")
      : (res && res.backup
          ? `Migration completed. State/cache cleared. Config backup created: ${res.backup}`
          : "Migration completed. State/cache cleared. Config backup created."));

    try {
      if (btn) {
        btn.classList.remove("busy");
        btn.textContent = "MIGRATED";
        btn.disabled = true;
        btn.dataset.done = "1";
      }
    } catch {}

    await _restartAfterMigration();
  } catch (e) {
    console.warn("[upgrade-warning] migrate failed", e);
    notify("Migration failed. Check logs.");
    try {
      if (btn) {
        btn.disabled = false;
        btn.classList.remove("busy");
        btn.textContent = "MIGRATE";
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
    const legacy = !hasCfgVer || _cmp(cfg, "0.7.0") < 0;
    const needs0911Cleanup = !hasCfgVer || _cmp(cfg, "0.9.11") < 0;

    if (needs0911Cleanup) _pauseSchedulerOnce();

    const shell = hostEl.closest(".cx-modal-shell");
    const state = {
      authReady: false,
      step: "intro",
      username: "admin",
      password: "",
      password2: "",
      error: "",
      saving: false,
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
      );
      state.step = state.authReady ? "migrate" : "intro";
    } catch {
      state.authReady = false;
      state.step = "intro";
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
        state.step = "migrate";
        notify("Sign-in saved. Continue with migration.");
        render();
        return;
      } catch (err) {
        state.saving = false;
        state.error = String(err?.message || "Failed to save sign-in settings.");
        render();
      }
    }

    function layout(body, foot) {
      return `
        <style>
          #upg-host{--w:820px;position:relative;overflow:hidden;min-width:min(var(--w),94vw);max-width:94vw;color:#eaf0ff;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:radial-gradient(900px circle at 18% 18%, rgba(150,70,255,.22), transparent 55%),radial-gradient(900px circle at 92% 10%, rgba(60,140,255,.18), transparent 55%),radial-gradient(800px circle at 55% 110%, rgba(60,255,215,.08), transparent 60%),rgba(7,8,11,.92);box-shadow:0 30px 90px rgba(0,0,0,.70), inset 0 1px 0 rgba(255,255,255,.04);backdrop-filter:saturate(135%) blur(10px)}
          #upg-host .head{display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01))}
          #upg-host .icon{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}
          #upg-host .icon span{font-size:26px}
          #upg-host .t{font-weight:950;font-size:15px;line-height:1.1;text-transform:uppercase;opacity:.90}
          #upg-host .sub{opacity:.72;font-size:12px;margin-top:2px}
          #upg-host .pill{margin-left:auto;display:flex;gap:8px;align-items:center;font-weight:900;font-size:12px;opacity:.85}
          #upg-host .pill .b{padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}
          #upg-host .body{padding:16px 16px 8px 16px;max-height:72vh;overflow:auto}
          #upg-host .card{display:block;padding:12px;border-radius:14px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 30px rgba(0,0,0,.32);margin-bottom:10px}
          #upg-host .card .h{font-weight:950}
          #upg-host .card .p{opacity:.84;margin-top:6px;line-height:1.45}
          #upg-host .warn{border-color:rgba(255,120,120,.22);background:linear-gradient(180deg,rgba(255,77,79,.12),rgba(255,77,79,.05))}
          #upg-host .notes{margin-top:8px;max-height:320px;overflow:auto;padding:16px 18px;border-radius:16px;background:linear-gradient(180deg,rgba(7,9,16,.72),rgba(4,6,10,.88));border:1px solid rgba(255,255,255,.08);font:14px/1.65 "Segoe UI Variable","Avenir Next","Trebuchet MS",sans-serif;color:rgba(236,241,255,.94)}
          #upg-host .notes h2,#upg-host .notes h3,#upg-host .notes h4{margin:0 0 10px;line-height:1.15;letter-spacing:-.02em;color:#f5f7ff}
          #upg-host .notes h2{font-size:24px;font-weight:950}
          #upg-host .notes h3{margin-top:20px;font-size:18px;font-weight:900}
          #upg-host .notes h4{margin-top:16px;font-size:15px;font-weight:900}
          #upg-host .notes p{margin:0 0 12px;color:rgba(225,232,247,.82)}
          #upg-host .notes .notes-list{margin:0 0 14px;padding-left:20px;display:grid;gap:8px}
          #upg-host .notes li{color:rgba(231,237,250,.88)}
          #upg-host .notes li.indent-1{margin-left:14px;opacity:.92}
          #upg-host .notes li.indent-2{margin-left:28px;opacity:.88}
          #upg-host .notes strong{color:#f7f9ff}
          #upg-host .notes em{color:rgba(230,214,255,.92)}
          #upg-host .notes code{padding:2px 6px;border-radius:8px;background:rgba(140,109,255,.14);border:1px solid rgba(140,109,255,.18);font:12px/1.4 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;color:#f6ebff}
          #upg-host .notes a{color:#caa7ff;text-decoration:none;word-break:break-word}
          #upg-host .notes a:hover{text-decoration:underline}
          #upg-host .helpLink{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-top:10px;padding:14px 16px;border-radius:14px;text-decoration:none;color:#eef3ff;background:linear-gradient(135deg,rgba(150,70,255,.18),rgba(60,140,255,.14));border:1px solid rgba(150,70,255,.22);box-shadow:0 14px 34px rgba(0,0,0,.24), inset 0 1px 0 rgba(255,255,255,.04);transition:transform .16s ease, filter .16s ease, border-color .16s ease}
          #upg-host .helpLink:hover{transform:translateY(-1px);filter:brightness(1.06);border-color:rgba(150,70,255,.32)}
          #upg-host .helpCopy{display:grid;gap:4px}
          #upg-host .helpEyebrow{font-size:11px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;opacity:.72}
          #upg-host .helpTitle{font-size:16px;font-weight:950;line-height:1.15}
          #upg-host .helpSub{font-size:12.5px;line-height:1.4;opacity:.8}
          #upg-host .helpIcon{width:42px;height:42px;border-radius:12px;display:grid;place-items:center;flex:0 0 auto;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}
          #upg-host .helpIcon .material-symbols-rounded{font-size:22px}
          ${appAuthFormCss("#upg-host")}
          #upg-host .foot{display:flex;justify-content:flex-end;gap:10px;padding:12px 16px;border-top:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01))}
          #upg-host .btn{appearance:none;border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:10px 14px;font-weight:950;cursor:pointer;background:rgba(255,255,255,.04);color:#eaf0ff}
          #upg-host .btn:hover{filter:brightness(1.06)}
          #upg-host .btn[disabled]{opacity:.62;cursor:progress}
          #upg-host .btn.primary{border-color:rgba(150,70,255,.35);background:linear-gradient(135deg,rgba(150,70,255,.92),rgba(60,140,255,.82))}
          #upg-host .btn.danger{border-color:rgba(255,120,120,.28);background:linear-gradient(135deg,rgba(255,77,79,.92),rgba(255,122,122,.82))}
        </style>
        <div id="upg-host">
          <div class="head">
            <div class="icon" aria-hidden="true"><span class="material-symbols-rounded">system_update</span></div>
            <div>
              <div class="t">${needs0911Cleanup ? "Migration required" : (legacy ? "Legacy config detected" : "Config version notice")}</div>
              <div class="sub">${needs0911Cleanup ? "Pre-v0.9.11 data cleanup" : (legacy ? "This release introduced config versioning (0.7.0+)." : "Migrate to new save format.")}</div>
            </div>
            <div class="pill">
              <span class="b">Engine v${cur}</span>
              ${legacy ? `<span class="b">Config: Legacy</span>` : `<span class="b">Config v${cfg}</span>`}
            </div>
          </div>
          <div class="body">${body}</div>
          <div class="foot">${foot}</div>
        </div>
      `;
    }

    function migrationBody() {
      return `
        ${needs0911Cleanup ? `
        <div class="card warn">
          <div class="h">IMPORTANT</div>
          <div class="p">Starting with <b>v0.9.11</b>, CrossWatch switched the primary ID from <b>IMDb</b> to <b>TMDb</b>. Click <b>MIGRATE</b> to remove the old IMDb-based state and cache data.</div>
        </div>
        ` : ``}
        ${legacy ? `
        <div class="card warn">
          <div class="h">IMPORTANT</div>
          <div class="p">CrossWatch now separates <b>global orchestration state</b> from <b>pair-specific provider caches</b>.</div>
          <div class="p">For a smooth transition, the current caches need to be removed or migrated.</div>
        </div>
        ` : ``}
        <div class="card">
          <div class="h">What to do</div>
          <div class="p">${needs0911Cleanup ? "Click <b>MIGRATE</b> below. It runs the cleanup flow, backs up <code>config.json</code>, applies migration updates, and restarts CrossWatch." : (legacy ? "Click <b>MIGRATE</b> below. It clears legacy state/cache, backs up <code>config.json</code>, applies migration updates, and restarts CrossWatch." : "Nothing is broken. Click <b>MIGRATE</b> once so CrossWatch backs up your current config, applies the updated config structure, and restarts.")}</div>
        </div>
        <div class="card">
          <div class="h">Tip</div>
          <div class="p">After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
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

    function renderIntro() {
      setModalDismissible(false);
      hostEl.innerHTML = layout(`
        <div class="card warn">
          <div class="h">Migration now requires admin credentials</div>
          <div class="p">Before you migrate this installation, CrossWatch now requires a local admin username and password to be configured.</div>
        </div>
        <div class="card">
          <div class="h">What happens next</div>
          <div class="p">Click <b>Next</b>, create the admin credentials, and then continue with the normal migration flow.</div>
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
          <div class="p">You must finish this step before migration can continue.</div>
        </div>
        <div class="card">
          <div class="h">Background activity is paused</div>
          <div class="p">Sync summary and log streams stay paused until sign-in is configured.</div>
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

    function renderMigrate() {
      setModalDismissible(true);
      hostEl.innerHTML = layout(migrationBody(), `
        <button class="btn" type="button" data-x="close">Close</button>
        ${needs0911Cleanup || legacy
          ? `<button class="btn danger" type="button" data-x="migrate">MIGRATE</button>`
          : `<button class="btn primary" type="button" data-x="save">MIGRATE</button>`}
      `);
      setModalShellInline(shell);
      hostEl.querySelector('[data-x="close"]')?.addEventListener("click", () => {
        try { window.cxCloseModal?.(); } catch {}
      });
      if (needs0911Cleanup || legacy) {
        hostEl.querySelector('[data-x="migrate"]')?.addEventListener("click", (e) => migrateNow(e.currentTarget, needs0911Cleanup));
      } else {
        hostEl.querySelector('[data-x="save"]')?.addEventListener("click", (e) => saveNow(e.currentTarget));
      }
      ensureNotesLoaded();
    }

    function render() {
      if (state.step === "credentials") return renderCredentials();
      if (state.step === "migrate") return renderMigrate();
      return renderIntro();
    }

    render();
  },

  unmount() {
    setModalDismissible(true);
  }
};
