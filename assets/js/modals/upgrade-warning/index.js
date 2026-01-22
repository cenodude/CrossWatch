// assets/js/modals/upgrade-warning/index.js
function _norm(v) {
  return String(v || "").replace(/^v/i, "").trim();
}

function _cmp(a, b) {
  const pa = _norm(a).split(".").map((n) => parseInt(n, 10) || 0);
  const pb = _norm(b).split(".").map((n) => parseInt(n, 10) || 0);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const da = pa[i] || 0;
    const db = pb[i] || 0;
    if (da !== db) return da > db ? 1 : -1;
  }
  return 0;
}

async function _postJson(url, opts = {}) {
  const res = await fetch(url, { method: "POST", ...opts });
  let data = null;
  try {
    data = await res.json();
  } catch {}
  if (!res.ok || (data && data.ok === false)) {
    const msg = (data && (data.error || data.message)) || `HTTP ${res.status} ${res.statusText}`;
    throw new Error(`${url}: ${msg}`);
  }
  return data;
}

async function _saveConfigNoUi() {
  return _postJson("/api/config", {
    headers: { "Content-Type": "application/json" },
    body: "{}"
  });
}

async function saveNow(btn) {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  try {
    if (btn && btn.dataset && btn.dataset.done === "1") return;
  } catch {}
  try {
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
      btn.textContent = "Saving...";
    }
  } catch {}

  try {
    await _saveConfigNoUi();
    notify("Saved. After updates: hard refresh (Ctrl+F5) so the UI loads the new assets.");

    try {
      if (btn) {
        btn.classList.remove("busy");
        btn.textContent = "SAVED";
        btn.disabled = true;
        btn.dataset.done = "1";
      }
    } catch {}
  } catch (e) {
    console.warn("[upgrade-warning] save failed", e);
    notify("Save failed. Check logs.");
  } finally {
    try {
      if (btn) {
        if (!btn.dataset || btn.dataset.done !== "1") {
          btn.disabled = false;
          btn.classList.remove("busy");
          btn.textContent = "SAVE";
        }
      }
    } catch {}
  }
}

async function migrateNow(btn) {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  try {
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
      btn.textContent = "Migrating...";
    }
  } catch {}

  try {
    await _postJson("/api/maintenance/clear-state");
    await _postJson("/api/maintenance/clear-cache");
    await _postJson("/api/maintenance/clear-metadata-cache");

    await _saveConfigNoUi();

    notify("Migration completed. State/cache cleared and config saved.");

    try {
      if (btn) {
        btn.classList.remove("busy");
        btn.textContent = "MIGRATED";
        btn.disabled = true;
        btn.dataset.done = "1";
      }
    } catch {}
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

    const cur = _norm(props.current_version || window.__CW_VERSION__ || "0.0.0");

    const rawCfgVer = props.config_version;
    const hasCfgVer = rawCfgVer != null && String(rawCfgVer).trim() !== "";
    const cfg = hasCfgVer ? _norm(rawCfgVer) : "";

    // Legacy if config has no version, or version < 0.7.0
    const legacy = !hasCfgVer || _cmp(cfg, "0.7.0") < 0;

    hostEl.innerHTML = `
    <style>
      #upg-host{--w:820px;min-width:min(var(--w),94vw);max-width:94vw;color:#eef1ff;color:#eef1ff;position:relative;overflow:hidden;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:
        radial-gradient(1200px 600px at 0% 0%, rgba(124,58,237,.26), transparent 55%),
        radial-gradient(900px 520px at 100% 20%, rgba(59,130,246,.18), transparent 60%),
        linear-gradient(180deg, rgba(12,16,24,.94), rgba(9,12,18,.94));
        box-shadow:0 35px 110px rgba(0,0,0,.68), 0 0 0 1px rgba(124,58,237,.10), inset 0 1px 0 rgba(255,255,255,.04);
        backdrop-filter:saturate(140%) blur(10px);
      }
      #upg-host:before{content:'';position:absolute;inset:-2px;pointer-events:none;background:
        radial-gradient(600px 240px at 20% 10%, rgba(255,255,255,.10), transparent 60%),
        radial-gradient(520px 260px at 80% 0%, rgba(255,255,255,.06), transparent 58%);
        opacity:.55
      }
      #upg-host .topline{position:absolute;left:0;top:0;right:0;height:2px;background:linear-gradient(90deg,rgba(124,58,237,.0),rgba(124,58,237,.8),rgba(59,130,246,.8),rgba(59,130,246,.0));opacity:.9}
      #upg-host .head{position:relative;z-index:1;display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.01))}
      #upg-host .icon{width:34px;height:34px;border-radius:12px;display:grid;place-items:center;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);box-shadow:0 10px 28px rgba(0,0,0,.35)}
      #upg-host .t{font-weight:950;letter-spacing:.35px}
      #upg-host .sub{opacity:.72;font-size:12.5px;margin-top:2px}
      #upg-host .pill{margin-left:auto;display:flex;gap:8px;align-items:center;font-weight:900;font-size:12px;color:#dfe6ff}
      #upg-host .pill .b{padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12)}

      #upg-host .body{position:relative;z-index:1;padding:14px 16px;max-height:66vh;overflow:auto}
      #upg-host .card{background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018));border:1px solid rgba(255,255,255,.10);border-radius:14px;box-shadow:0 10px 32px rgba(0,0,0,.28);padding:12px 14px;margin-bottom:12px}
      #upg-host .card .h{font-weight:950}
      #upg-host .card .p{opacity:.86;margin-top:6px;line-height:1.45}
      #upg-host .warn{border-color:rgba(255,120,120,.28);background:linear-gradient(180deg,rgba(255,77,79,.14),rgba(255,77,79,.06))}
      #upg-host ul{margin:.6em 0 0 1.15em}
      #upg-host code{opacity:.95}

      #upg-host .btn{appearance:none;border:1px solid rgba(124,58,237,.35);border-radius:14px;padding:10px 14px;font-weight:950;background:linear-gradient(135deg,rgba(124,58,237,.95),rgba(59,130,246,.92));color:#fff;cursor:pointer;box-shadow:0 10px 26px rgba(0,0,0,.28)}
      #upg-host .btn:hover{filter:brightness(1.07)}
      #upg-host .btn:active{transform:translateY(1px)}
      #upg-host .btn.ghost{background:rgba(255,255,255,.06);border-color:rgba(255,255,255,.14);color:#eef1ff;box-shadow:none}
      #upg-host .btn.danger{background:linear-gradient(135deg,#ff4d4f,#ff7a7a);border-color:rgba(255,120,120,.35)}
      #upg-host .btn.busy{opacity:.82;cursor:progress}

      #upg-host .foot{position:relative;z-index:1;display:flex;justify-content:flex-end;gap:10px;padding:12px 16px;border-top:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.01),rgba(255,255,255,.00))}
    </style>

    <div id="upg-host">
      <div class="topline" aria-hidden="true"></div>

      <div class="head">
        <div class="icon" aria-hidden="true"><span class="material-symbols-rounded">system_update</span></div>
        <div>
          <div class="t">${legacy ? "Legacy config detected" : "Config version notice"}</div>
          <div class="sub">${legacy ? "This release introduced config versioning (0.7.0+)." : "One save updates the config format."}</div>
        </div>
        <div class="pill">
          <span class="b">Engine v${cur}</span>
          ${legacy ? `<span class="b">Config: Legacy</span>` : `<span class="b">Config v${cfg}</span>`}
        </div>
      </div>

      <div class="body">
        ${legacy ? `
        <div class="card warn">
          <div class="h">IMPORTANT</div>
          <div class="p">CrossWatch now clearly separates <b>global orchestration state</b> from <b>pair-specific provider caches</b>.</div>
          <ul>
            <li>Multiple pairs can run without overwriting each other’s cached snapshots/watermarks.</li>
            <li>Providers can safely reuse cached “present” indexes (when activities timestamps match) without risking cross-pair contamination.</li>
          </ul>
          <div class="p" style="margin-top:8px">For a smooth transition, the current caches need to be removed/migrated.</div>
        </div>

        <div class="card">
          <div class="h">What to do</div>
          <div class="p">Click <b>MIGRATE</b> below. It clears state/cache, then saves your config so it gets the new <code>version</code> field.</div>
        </div>

        <div class="card">
          <div class="h">Tip</div>
          <div class="p">After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
        </div>
        ` : `
        <div class="card">
          <div class="h">What this means</div>
          <div class="p">Nothing is broken. Click <b>SAVE</b> once so CrossWatch can apply the updated config structure.</div>
        </div>

        <div class="card">
          <div class="h">Tip</div>
          <div class="p">After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
        </div>
        `}
      </div>

      <div class="foot">
        <button class="btn ghost" type="button" data-x="close">Close</button>
        ${legacy
          ? `<button class="btn danger" type="button" data-x="migrate">MIGRATE</button>`
          : `<button class="btn" type="button" data-x="save">SAVE</button>`
        }
      </div>
    </div>
    `;

    const shell = hostEl.closest(".cx-modal-shell");
    if (shell) {
      shell.style.width = "auto";
      shell.style.maxWidth = "none";
      shell.style.height = "auto";
      shell.style.maxHeight = "none";
      shell.style.display = "inline-block";
    }

    hostEl.querySelector('[data-x="close"]')?.addEventListener("click", () => {
      try {
        window.cxCloseModal?.();
      } catch {}
    });

    if (legacy) {
      hostEl.querySelector('[data-x="migrate"]')?.addEventListener("click", (e) => migrateNow(e.currentTarget));
    } else {
      hostEl.querySelector('[data-x="save"]')?.addEventListener("click", (e) => saveNow(e.currentTarget));
    }
  },

  unmount() {}
};
