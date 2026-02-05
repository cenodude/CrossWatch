// assets/js/modals/insight-settings/index.js


const PREF_KEY = "insights.settings.v1";
const FEATS = ["watchlist","ratings","history","playlists"];
const FEAT_LABEL = { watchlist:"Watchlist", ratings:"Ratings", history:"History", playlists:"Playlists" };

const fjson = async (url, opts = {}) => {
  const r = await fetch(url, { cache: "no-store", credentials: "same-origin", ...opts });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText || ""}`.trim());
  if (r.status === 204) return {};
  try { return await r.json(); } catch { return {}; }
};

const $ = (s, r = document) => r.querySelector(s);
const esc = s => (window.CSS?.escape ? CSS.escape(s) : String(s).replace(/[^\w-]/g, "\\$&"));

const loadPrefs = () => {
  try { return JSON.parse(localStorage.getItem(PREF_KEY) || "{}") || {}; }
  catch { return {}; }
};

const savePrefs = (p) => {
  try { localStorage.setItem(PREF_KEY, JSON.stringify(p || {})); } catch {}
};

function injectCSS() {
  if (document.getElementById("cw-insight-set-css")) return;
  const el = document.createElement("style");
  el.id = "cw-insight-set-css";
  el.textContent = `
  .cw-insight-set{position:relative;display:flex;flex-direction:column;height:100%}
  .cw-insight-set .cx-head{display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-bottom:1px solid rgba(255,255,255,.12);background:linear-gradient(90deg,#05070d,#05040b);box-shadow:0 0 24px rgba(0,0,0,.75)}
  .cw-insight-set .head-left{display:flex;align-items:center;gap:10px;min-width:0}
  .cw-insight-set .head-icon{width:32px;height:32px;border-radius:999px;display:flex;align-items:center;justify-content:center;background:radial-gradient(circle at 30% 0%, #7a6bff 0, #23d5ff 45%, #121227 100%);box-shadow:0 0 18px rgba(122,107,255,.55);flex-shrink:0}
  .cw-insight-set .head-icon span{font-weight:900}
  .cw-insight-set .head-text{display:flex;flex-direction:column;gap:2px;min-width:0}
  .cw-insight-set .head-title{font-weight:900;font-size:15px}
  .cw-insight-set .head-sub{font-size:12px;opacity:.78;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cw-insight-set .close-btn{border:1px solid rgba(255,255,255,.22);background:#171b2a;color:#fff;border-radius:999px;padding:6px 14px;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;cursor:pointer}
  .cw-insight-set .close-btn:hover{background:#20253d}
  .cw-insight-set .body{flex:1;min-height:0;overflow:auto;padding:14px 16px 12px;background:#05060c}
  .cw-insight-set .layout{display:grid;grid-template-columns:360px 1fr;gap:12px;align-items:start}
  @media (max-width: 980px){.cw-insight-set .layout{grid-template-columns:1fr}}
  .cw-insight-set .card{border:1px solid rgba(255,255,255,.12);background:radial-gradient(circle at 0 0, rgba(122,107,255,.15), transparent 55%), linear-gradient(135deg,#0b0f19,#0e1624);border-radius:16px;padding:12px 12px;box-shadow:0 0 20px rgba(0,0,0,.7)}
  .cw-insight-set .card h3{margin:0 0 8px 0;font-size:13px;letter-spacing:.04em;text-transform:uppercase;opacity:.9}
  .cw-insight-set #is-feat-grid{display:flex;flex-direction:column;gap:10px}
  .cw-insight-set .pill{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(0,0,0,.18);cursor:pointer;user-select:none}
  .cw-insight-set .pill:hover{background:rgba(255,255,255,.06)}
  .cw-insight-set .pill input{accent-color:auto}
  .cw-insight-set .pill .lab{font-weight:700;font-size:13px}
  .cw-insight-set .opt-row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.14);background:rgba(0,0,0,.18)}
  .cw-insight-set .opt-row:hover{background:rgba(255,255,255,.06)}
  .cw-insight-set .feat-name{font-weight:900;font-size:14px}
  .cw-insight-set .switch{--w:58px;--h:32px;--dot:24px;--pad:4px;--bw:1px;position:relative;display:inline-block;width:var(--w);height:var(--h)}
  .cw-insight-set .switch input{width:0;height:0;position:absolute;opacity:0}
  .cw-insight-set .switch .slider{position:absolute;inset:0;border-radius:999px;border:var(--bw) solid rgba(255,255,255,.16);background:rgba(0,0,0,.22);transition:.2s;box-sizing:border-box}
  .cw-insight-set .switch .slider::before{content:'';position:absolute;left:var(--pad);top:50%;width:var(--dot);height:var(--dot);transform:translateY(-50%);border-radius:999px;background:rgba(255,255,255,.85);transition:.2s;box-shadow:0 2px 10px rgba(0,0,0,.55)}
  .cw-insight-set .switch input:checked + .slider{background:rgba(35,213,255,.22);border-color:rgba(35,213,255,.55)}
  .cw-insight-set .switch input:checked + .slider::before{left:calc(100% - var(--dot) - var(--pad))}
  .cw-insight-set .muted{opacity:.72;font-size:12px}
  .cw-insight-set .prov{display:grid;grid-template-columns:1fr 1fr;gap:12px}
  @media (max-width: 980px){.cw-insight-set .prov{grid-template-columns:1fr}}
  .cw-insight-set .prov-head{display:flex;align-items:center;justify-content:space-between;gap:10px}
  .cw-insight-set .prov-name{font-weight:900;letter-spacing:.06em;text-transform:uppercase}
  .cw-insight-set .prov-badge{padding:3px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.18);font-size:11px;letter-spacing:.06em;text-transform:uppercase;opacity:.9}
  .cw-insight-set .prov-actions{display:flex;gap:8px;align-items:center}
  .cw-insight-set .mini{border:1px solid rgba(255,255,255,.14);background:transparent;color:#dbe8ff;border-radius:999px;padding:4px 10px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;cursor:pointer;opacity:.85}
  .cw-insight-set .mini:hover{opacity:1;background:rgba(255,255,255,.05)}
  .cw-insight-set .actions{padding:10px 16px;border-top:1px solid rgba(255,255,255,.12);display:flex;align-items:center;justify-content:space-between;gap:12px;background:#05060c}
  .cw-insight-set .btn{min-width:110px}
  .cw-insight-set .btn.good{background:var(--grad2);border-color:rgba(25,195,125,.45);box-shadow:0 0 14px var(--glow2);color:#fff}
  .cw-insight-set .btn.good:hover{filter:brightness(1.06)}
.cw-insight-set .toast{font-size:12px;opacity:.8}
  `;
  document.head.appendChild(el);
}

function normalizePrefs(p, instancesByProvider = {}) {
  const out = (p && typeof p === "object") ? JSON.parse(JSON.stringify(p)) : {};
  const f = out.features && typeof out.features === "object" ? out.features : {};
  out.features = {
    watchlist: f.watchlist !== false,
    ratings: f.ratings !== false,
    history: f.history !== false,
    playlists: f.playlists !== false,
  };
  out.instances = out.instances && typeof out.instances === "object" ? out.instances : {};
  out.known_instances = out.known_instances && typeof out.known_instances === "object" ? out.known_instances : {};

  for (const [prov, list] of Object.entries(instancesByProvider || {})) {
    const pkey = String(prov || "").toLowerCase();
    const all = Array.isArray(list) && list.length ? list.map(String) : ["default"];
    const prevKnown = Array.isArray(out.known_instances[pkey]) ? out.known_instances[pkey].map(String) : [];
    const prevSet = new Set(prevKnown);

    if (out.instances[pkey] !== undefined) {
      const cur = Array.isArray(out.instances[pkey]) ? out.instances[pkey].map(String) : [];
      const nowSet = new Set(all);
      const kept = cur.filter(x => nowSet.has(x));
      for (const x of all) {
        if (!prevSet.has(x) && !kept.includes(x)) kept.push(x);
      }
      out.instances[pkey] = kept;
    }

    out.known_instances[pkey] = all.slice();
  }

  if (!Object.values(out.features).some(Boolean)) out.features.watchlist = true;
  return out;
}

function providerLabel(p) {
  const up = String(p || "").toUpperCase();
  if (up === "JELLYFIN") return "Jellyfin";
  if (up === "MDBLIST") return "MDBList";
  if (up === "CROSSWATCH") return "CrossWatch";
  return up.slice(0, 1) + up.slice(1).toLowerCase();
}

export default {
  async mount(root) {
    injectCSS();
    root.classList.add("modal-root", "cw-insight-set");

    root.innerHTML = `
      <div class="cx-head">
        <div class="head-left">
          <div class="head-icon" aria-hidden="true"><span>⚙︎</span></div>
          <div class="head-text">
            <div class="head-title">Insights settings</div>
            <div class="head-sub">Choose which features and profiles are included</div>
          </div>
        </div>
        <button class="close-btn" id="is-close" type="button">Close</button>
      </div>
      <div class="body" id="is-body"><div class="layout">
        <div class="card" id="is-features">
          <h3>Features</h3>
          <div class="grid" id="is-feat-grid"></div>
          <div class="muted" style="margin-top:8px">Tip: hiding a feature removes it from the switcher.</div>
        </div>
        <div class="card" id="is-providers">
          <h3>Profiles</h3>
          <div class="muted" id="is-loading">Loading…</div>
          <div class="prov" id="is-prov-grid" style="display:none"></div>
        </div>
      </div></div>
      <div class="actions">
        <div class="toast" id="is-toast"></div>
        <div style="display:flex;gap:10px">
          <button class="btn danger" id="is-reset" type="button">Reset</button>
          <button class="btn good" id="is-apply" type="button">Apply</button>
        </div>
      </div>
    `;

    const toast = (msg) => {
      const el = $("#is-toast", root);
      if (el) el.textContent = msg || "";
    };

    $("#is-close", root)?.addEventListener("click", () => window.cxCloseModal?.());
    $("#is-reset", root)?.addEventListener("click", () => {
      try { localStorage.removeItem(PREF_KEY); } catch {}
      window.dispatchEvent(new CustomEvent("insights:settings-changed", { detail: { force: true } }));
      window.cxCloseModal?.();
    });

    let data = {};
    try {
      data = await fjson("/api/insights?limit_samples=0&history=0");
    } catch (e) {
      toast("Failed to load insights metadata.");
      console.error("[InsightsSettings] failed to load /api/insights", e);
      data = {};
    }

    let pairs = [];
    try {
      pairs = await fjson("/api/pairs");
    } catch (e) {
      pairs = [];
    }

    const usedProviders = new Set();
    for (const p of (Array.isArray(pairs) ? pairs : [])) {
      const a = p?.source;
      const b = p?.target;
      if (a) usedProviders.add(String(a).toLowerCase());
      if (b) usedProviders.add(String(b).toLowerCase());
    }

    if (!usedProviders.size) {
      for (const k of Object.keys(data.instances_by_provider || {})) {
        usedProviders.add(String(k).toLowerCase());
      }
    }

    let instApi = {};
    try {
      instApi = await fjson("/api/provider-instances");
    } catch (e) {
      instApi = {};
    }

    const labelsByProvider = {};
    const instancesByProvider = {};

    const findInstKey = (provKey) => {
      const want = String(provKey || "").toLowerCase();
      for (const k of Object.keys(instApi || {})) {
        if (String(k || "").toLowerCase() === want) return k;
      }
      return null;
    };

    for (const prov of Array.from(usedProviders).sort((a, b) => a.localeCompare(b))) {
      const k = findInstKey(prov);
      const rawList = k ? instApi[k] : null;
      const ids = [];
      const labs = {};
      if (Array.isArray(rawList) && rawList.length) {
        for (const it of rawList) {
          const id = typeof it === "string" ? it : String(it?.id || "").trim();
          if (!id) continue;
          if (!ids.includes(id)) ids.push(id);
          const label = typeof it === "object" && it ? String(it.label || "").trim() : "";
          if (label) labs[id] = label;
        }
      }

      if (!ids.includes("default")) ids.unshift("default");
      if (!labs["default"]) labs["default"] = "Default";

      instancesByProvider[prov] = ids;
      labelsByProvider[prov] = labs;
    }

    let prefs = normalizePrefs(loadPrefs(), instancesByProvider);

    const featGrid = $("#is-feat-grid", root);
    if (featGrid) {
      featGrid.innerHTML = FEATS.map(k => {
        const on = prefs.features?.[k] !== false;
        return `
          <div class="opt-row is-feat-row">
            <div class="feat-name">${FEAT_LABEL[k] || k}</div>
            <label class="switch" for="is-feat-${esc(k)}">
              <input type="checkbox" id="is-feat-${esc(k)}" data-feat="${esc(k)}" ${on ? "checked" : ""}>
              <span class="slider"></span>
            </label>
          </div>`;
      }).join("");
}

    const loading = $("#is-loading", root);
    const provGrid = $("#is-prov-grid", root);

    const provKeys = Object.keys(instancesByProvider || {}).sort((a,b) => String(a).localeCompare(String(b)));
    if (!provKeys.length) {
      if (loading) loading.textContent = "No profiles found.";
    } else {
      if (loading) loading.style.display = "none";
      if (provGrid) provGrid.style.display = "";

      for (const prov of provKeys) {
        const pkey = String(prov || "").toLowerCase();
        const all = Array.isArray(instancesByProvider[prov]) && instancesByProvider[prov].length
          ? instancesByProvider[prov].map(String)
          : ["default"];

        const cur = prefs.instances[pkey];
        const selected = cur === undefined ? all.slice() : (Array.isArray(cur) ? cur.map(String) : []);
        const selectedSet = new Set(selected);

        const section = document.createElement("div");
        section.className = "card";
        section.dataset.provider = pkey;

        const badge = `${selected.length}/${all.length}`;
        section.innerHTML = `
          <div class="prov-head">
            <div class="prov-name">${providerLabel(pkey)}</div>
            <div class="prov-actions">
              <span class="prov-badge" data-badge>${badge}</span>
              <button class="mini" type="button" data-all>All</button>
              <button class="mini" type="button" data-none>None</button>
            </div>
          </div>
          <div class="grid" style="margin-top:10px" data-list></div>
        `;

        const list = section.querySelector("[data-list]");
        if (list) {
          list.innerHTML = all.map(id => {
            const lab = (labelsByProvider[pkey] && labelsByProvider[pkey][id]) ? labelsByProvider[pkey][id] : (id === "default" ? "Default" : id);
            const on = selectedSet.has(id);
            return `
              <label class="pill" for="is-${esc(pkey)}-${esc(id)}">
                <input type="checkbox" id="is-${esc(pkey)}-${esc(id)}" data-provider="${esc(pkey)}" data-inst="${esc(id)}" ${on ? "checked" : ""}>
                <span class="lab">${lab}</span>
              </label>`;
          }).join("");
        }

        const updateBadge = () => {
          const checks = Array.from(section.querySelectorAll('input[type="checkbox"][data-inst]'));
          const on = checks.filter(c => c.checked).length;
          const b = section.querySelector("[data-badge]");
          if (b) b.textContent = `${on}/${checks.length}`;
        };

        section.querySelector("[data-all]")?.addEventListener("click", () => {
          section.querySelectorAll('input[type="checkbox"][data-inst]').forEach(c => (c.checked = true));
          updateBadge();
        });
        section.querySelector("[data-none]")?.addEventListener("click", () => {
          section.querySelectorAll('input[type="checkbox"][data-inst]').forEach(c => (c.checked = false));
          updateBadge();
        });
        section.addEventListener("change", (ev) => {
          const t = ev.target;
          if (t && t.matches && t.matches('input[type="checkbox"][data-inst]')) updateBadge();
        });

        provGrid?.appendChild(section);
      }
    }

    $("#is-apply", root)?.addEventListener("click", () => {
      const next = normalizePrefs(loadPrefs(), instancesByProvider);

      const featChecks = Array.from(root.querySelectorAll('input[type="checkbox"][data-feat]'));
      for (const c of featChecks) {
        const k = c.dataset.feat;
        if (!k) continue;
        next.features[k] = !!c.checked;
      }
      if (!Object.values(next.features).some(Boolean)) next.features.watchlist = true;

      next.instances = next.instances && typeof next.instances === "object" ? next.instances : {};
      const provSections = Array.from(root.querySelectorAll('[data-provider]'));
      for (const sec of provSections) {
        const prov = String(sec.dataset.provider || "").toLowerCase();
        const checks = Array.from(sec.querySelectorAll('input[type="checkbox"][data-inst]'));
        const selected = checks.filter(c => c.checked).map(c => String(c.dataset.inst || ""));
        const all = checks.map(c => String(c.dataset.inst || ""));

        if (selected.length === all.length) {
          delete next.instances[prov];
        } else {
          next.instances[prov] = selected;
        }
      }

      savePrefs(next);
      window.dispatchEvent(new CustomEvent("insights:settings-changed", { detail: { force: true } }));
      window.cxCloseModal?.();
    });
  }
};
