// assets/js/modals/upgrade-warning/index.js
const NOTES_ENDPOINT = "/api/update";
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

async function _getJson(url, opts = {}) {
  const res = await fetch(url, { method: "GET", ...opts });
  let data = null;
  try {
    data = await res.json();
  } catch {}
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status} ${res.statusText}`);
  return data || {};
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
      #upg-host{--w:820px;position:relative;overflow:hidden;min-width:min(var(--w),94vw);max-width:94vw;color:#eaf0ff;border-radius:18px;
        border:1px solid rgba(255,255,255,.08);
        background:
          radial-gradient(900px circle at 18% 18%, rgba(150,70,255,.22), transparent 55%),
          radial-gradient(900px circle at 92% 10%, rgba(60,140,255,.18), transparent 55%),
          radial-gradient(800px circle at 55% 110%, rgba(60,255,215,.08), transparent 60%),
          rgba(7,8,11,.92);
        box-shadow:0 30px 90px rgba(0,0,0,.70), inset 0 1px 0 rgba(255,255,255,.04);
        backdrop-filter:saturate(135%) blur(10px)
      }
      #upg-host:before{content:"";position:absolute;inset:-120px;pointer-events:none;
        background:conic-gradient(from 180deg at 50% 50%, rgba(150,70,255,.0), rgba(150,70,255,.30), rgba(60,140,255,.24), rgba(60,255,215,.10), rgba(150,70,255,.0));
        filter:blur(90px);opacity:.35;transform:translate3d(0,0,0);
        animation:upgGlow 16s ease-in-out infinite alternate
      }
      @keyframes upgGlow{from{transform:translate(-16px,-10px) scale(1)}to{transform:translate(16px,12px) scale(1.03)}}
      @media (prefers-reduced-motion: reduce){#upg-host:before{animation:none}}

      #upg-host .head{position:relative;display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01))
      }
      #upg-host .icon{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);
        box-shadow:0 12px 30px rgba(0,0,0,.40), inset 0 1px 0 rgba(255,255,255,.04)
      }
      #upg-host .icon span{font-size:26px;opacity:.95;filter:drop-shadow(0 10px 16px rgba(0,0,0,.45))}
      #upg-host .t{font-weight:950;letter-spacing:.2px;font-size:15px;line-height:1.1;text-transform:uppercase;opacity:.90}
      #upg-host .sub{opacity:.72;font-size:12px;margin-top:2px}
      #upg-host .pill{margin-left:auto;display:flex;gap:8px;align-items:center;font-weight:900;font-size:12px;opacity:.85}
      #upg-host .pill .b{padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}

      #upg-host .body{position:relative;padding:16px 16px 8px 16px;max-height:72vh;overflow:auto}
      #upg-host .card{display:block;padding:12px 12px;border-radius:14px;
        background:rgba(255,255,255,.03);
        border:1px solid rgba(255,255,255,.08);
        box-shadow:0 10px 30px rgba(0,0,0,.32);
        margin-bottom:10px
      }
      #upg-host .card .h{font-weight:950}
      #upg-host .card .p{opacity:.84;margin-top:6px;line-height:1.45}
      #upg-host .warn{border-color:rgba(255,120,120,.22);background:linear-gradient(180deg,rgba(255,77,79,.12),rgba(255,77,79,.05))}
      #upg-host ul{margin:.6em 0 0 1.15em}
      #upg-host code{opacity:.95}
      #upg-host .notes{margin-top:8px;white-space:pre-wrap;overflow:auto;max-height:280px;
        font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;
        padding:10px 11px;border-radius:12px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);opacity:.92}
      #upg-host .link{display:inline-block;margin-top:8px;opacity:.86}

      #upg-host .btn{appearance:none;border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:10px 14px;font-weight:950;cursor:pointer;
        background:rgba(255,255,255,.04);color:#eaf0ff
      }
      #upg-host .btn:hover{filter:brightness(1.06)}
      #upg-host .btn.primary{border-color:rgba(150,70,255,.35);
        background:linear-gradient(135deg,rgba(150,70,255,.92),rgba(60,140,255,.82));
        box-shadow:0 16px 50px rgba(0,0,0,.48)
      }
      #upg-host .btn.primary:active{transform:translateY(1px)}
      #upg-host .btn.ghost{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.10);box-shadow:none}
      #upg-host .btn.danger{border-color:rgba(255,120,120,.28);background:linear-gradient(135deg,rgba(255,77,79,.92),rgba(255,122,122,.82));color:#fff;box-shadow:0 16px 50px rgba(0,0,0,.48)}
      #upg-host .btn.busy{opacity:.82;cursor:progress}

      #upg-host .foot{position:relative;display:flex;justify-content:flex-end;gap:10px;padding:12px 16px;border-top:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01))
      }
    </style>

    <div id="upg-host">
      <div class="head">
        <div class="icon" aria-hidden="true"><span class="material-symbols-rounded">system_update</span></div>
        <div>
          <div class="t">${legacy ? "Legacy config detected" : "Config version notice"}</div>
          <div class="sub">${legacy ? "This release introduced config versioning (0.7.0+)." : "Migrate to new save format."}</div>
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

        <div class="card" id="upg-release-notes" style="display:none">
          <div class="h">Release notes</div>
          <div class="p" id="upg-release-notes-meta" style="opacity:.72">&nbsp;</div>
          <pre class="notes" id="upg-release-notes-body"></pre>
          <a class="link" id="upg-release-notes-link" href="" target="_blank" rel="noopener noreferrer">Open on GitHub</a>
        </div>
      </div>

      <div class="foot">
        <button class="btn ghost" type="button" data-x="close">Close</button>
        ${legacy
          ? `<button class="btn danger" type="button" data-x="migrate">MIGRATE</button>`
          : `<button class="btn primary" type="button" data-x="save">SAVE</button>`
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

    try {
      const j = await _getJson(NOTES_ENDPOINT, { cache: "no-store" });
      const body = String(j.body || "").trim();
      if (!body) return;
      const card = hostEl.querySelector("#upg-release-notes");
      const pre = hostEl.querySelector("#upg-release-notes-body");
      if (!card || !pre) return;

      pre.textContent = body;
      const lat = _norm(j.latest_version || j.latest || "");
      const pub = String(j.published_at || "").trim();
      const meta = hostEl.querySelector("#upg-release-notes-meta");
      if (meta) meta.textContent = `Latest${lat ? ` v${lat}` : ""}${pub ? ` • ${pub}` : ""}`;

      const href = String(j.html_url || j.url || "").trim();
      const a = hostEl.querySelector("#upg-release-notes-link");
      if (a) {
        if (href) a.setAttribute("href", href);
        else a.style.display = "none";
      }

      card.style.display = "block";
    } catch {
    }
  },

  unmount() {}
};
