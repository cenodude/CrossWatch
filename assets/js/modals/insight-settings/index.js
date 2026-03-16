/* assets/js/modals/insight-settings/index.js */
/* refactor */
/* Modal for configuring which features and provider instances contribute to the insights statistics. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const PREF_KEY = "insights.settings.v1";
const FeatureMeta = () => window.CW?.FeatureMeta || {};
const ProviderMeta = () => window.CW?.ProviderMeta || {};
const FEAT_COPY = {
  watchlist: "Show watchlist tiles.",
  ratings: "Show ratings tiles.",
  history: "Show history tiles.",
  progress: "Show in-progress playback tiles.",
  playlists: "Not used.",
};
const FEATS = FeatureMeta().order || ["watchlist", "ratings", "history", "progress", "playlists"];
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const esc = (s) => window.CSS?.escape ? window.CSS.escape(String(s ?? "")) : String(s ?? "").replace(/[^\w-]/g, "\\$&");
const h = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const close = () => window.cxCloseModal?.();
const changed = () => window.dispatchEvent(new CustomEvent("insights:settings-changed", { detail: { force: true } }));
const loadPrefs = () => { try { return JSON.parse(localStorage.getItem(PREF_KEY) || "{}") || {}; } catch { return {}; } };
const savePrefs = (v) => { try { localStorage.setItem(PREF_KEY, JSON.stringify(v || {})); } catch {} };
const jget = async (url) => {
  try {
    const r = await fetch(url, { cache: "no-store", credentials: "same-origin" });
    if (!r.ok) throw 0;
    if (r.status === 204) return {};
    return await r.json().catch(() => ({}));
  } catch { return null; }
};

const canonProv = (v) => {
  const up = String(v || "").trim().toUpperCase();
  return !up ? "" : up === "TMDB_SYNC" ? "TMDB" : ["MDB", "MDB_LIST", "MDBLIST"].includes(up) ? "MDBLIST" : up;
};
const provKey = (v) => canonProv(v).toLowerCase();
const instKey = (v) => String(v || "default").trim() || "default";
const provLabel = (v) => ProviderMeta().label?.(canonProv(v)) || canonProv(v);

const STYLE = `.cx-modal-shell.cw-insight-set{background:linear-gradient(180deg,rgba(11,14,24,.98),rgba(7,10,18,.98))!important;border:1px solid rgba(120,135,255,.18)!important;box-shadow:0 32px 110px rgba(0,0,0,.58),0 0 0 1px rgba(255,255,255,.04) inset!important;overflow:hidden}.cx-modal-shell.cw-insight-set::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(50% 60% at 12% 0%,rgba(124,92,255,.22),transparent 70%),radial-gradient(42% 52% at 100% 100%,rgba(0,198,255,.12),transparent 72%);opacity:.95}.cw-insight-set{position:relative;display:grid!important;grid-template-rows:auto 1fr auto;height:100%}.cw-insight-set .cx-head{position:relative;display:flex;align-items:center;justify-content:space-between;gap:14px;padding:16px 18px!important;background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,0))!important;border-bottom:1px solid rgba(255,255,255,.08)!important;box-shadow:0 1px 0 rgba(255,255,255,.03) inset}.cw-insight-set .head-left,.cw-insight-set .head-actions,.cw-insight-set .action-row,.cw-insight-set .providers-shell{display:flex;align-items:center;gap:10px}.cw-insight-set .head-left{gap:14px;min-width:0}.cw-insight-set .head-actions{flex:0 0 auto}.cw-insight-set .head-icon{width:40px;height:40px;border-radius:14px;display:grid;place-items:center;flex:0 0 auto;background:linear-gradient(145deg,rgba(126,92,255,.34),rgba(30,205,255,.12));border:1px solid rgba(137,155,255,.28);box-shadow:0 12px 32px rgba(59,130,246,.16),inset 0 1px 0 rgba(255,255,255,.12)}.cw-insight-set .head-icon span{font-size:18px;font-weight:900;transform:translateY(-.5px)}.cw-insight-set .head-copy,.cw-insight-set .providers-shell{flex-direction:column;align-items:stretch}.cw-insight-set .head-copy{gap:4px;min-width:0}.cw-insight-set .head-eyebrow,.cw-insight-set .head-chip,.cw-insight-set .close-btn,.cw-insight-set .panel-title,.cw-insight-set .panel-chip,.cw-insight-set .providers-count,.cw-insight-set .prov-title,.cw-insight-set .prov-badge,.cw-insight-set .mini,.cw-insight-set .btn{font-weight:800;text-transform:uppercase}.cw-insight-set .head-eyebrow{display:flex;align-items:center;gap:8px;font-size:11px;letter-spacing:.14em;color:rgba(210,221,255,.72)}.cw-insight-set .dot,.cw-insight-set .providers-count .dot{width:8px;height:8px;border-radius:999px;background:linear-gradient(180deg,#7c5cff,#2de2ff)}.cw-insight-set .dot{box-shadow:0 0 12px rgba(124,92,255,.55)}.cw-insight-set .head-title{font-size:22px;font-weight:900;line-height:1.02;letter-spacing:.01em;color:#eef4ff}.cw-insight-set .head-sub,.cw-insight-set .panel-sub,.cw-insight-set .providers-note,.cw-insight-set .prov-copy,.cw-insight-set .toast,.cw-insight-set .loading,.cw-insight-set .feature-copy{font-size:12px;line-height:1.45;color:rgba(210,220,245,.64)}.cw-insight-set .head-sub{font-size:13px;color:rgba(220,229,255,.7);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cw-insight-set .head-chip,.cw-insight-set .close-btn{height:38px;display:inline-flex;align-items:center;justify-content:center;border-radius:999px;padding:0 14px;font-size:12px;letter-spacing:.08em}.cw-insight-set .head-chip{border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04);color:rgba(229,237,255,.78);box-shadow:inset 0 1px 0 rgba(255,255,255,.05)}.cw-insight-set .close-btn{border:1px solid rgba(136,155,255,.24);background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.04));color:#f6f8ff;cursor:pointer;transition:transform .14s ease,box-shadow .14s ease,border-color .14s ease}.cw-insight-set .close-btn:hover,.cw-insight-set .feature-row:hover,.cw-insight-set .prov-card:hover,.cw-insight-set .mini:hover,.cw-insight-set .pill:hover .lab,.cw-insight-set .btn:hover{transform:translateY(-1px)}.cw-insight-set .close-btn:hover,.cw-insight-set .btn:hover{box-shadow:0 10px 26px rgba(0,0,0,.28)}.cw-insight-set .body{position:relative;overflow:auto!important;padding:18px!important;background:linear-gradient(180deg,rgba(7,10,18,.14),rgba(7,10,18,.04))}.cw-insight-set .layout{display:grid;grid-template-columns:minmax(300px,340px) minmax(0,1fr);gap:14px;align-items:start}.cw-insight-set .panel{position:relative;border-radius:20px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.025));box-shadow:0 18px 46px rgba(0,0,0,.2),inset 0 1px 0 rgba(255,255,255,.04);overflow:hidden}.cw-insight-set .panel::before{content:"";position:absolute;inset:auto 0 0;height:1px;background:linear-gradient(90deg,transparent,rgba(123,146,255,.35),transparent);opacity:.5}.cw-insight-set .panel-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;padding:16px 16px 12px;border-bottom:1px solid rgba(255,255,255,.06)}.cw-insight-set .panel-title{font-size:14px;letter-spacing:.1em;color:#eef3ff}.cw-insight-set .panel-sub{margin-top:5px;color:rgba(213,222,246,.62)}.cw-insight-set .panel-chip,.cw-insight-set .providers-count,.cw-insight-set .prov-badge,.cw-insight-set .mini{height:28px;display:inline-flex;align-items:center;justify-content:center;border-radius:999px;font-size:11px;letter-spacing:.12em}.cw-insight-set .panel-chip{padding:0 10px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04);color:rgba(220,229,255,.7);white-space:nowrap}.cw-insight-set .panel-body{padding:14px 16px 16px}.cw-insight-set .feature-list{display:flex;flex-direction:column;gap:10px}.cw-insight-set .feature-row,.cw-insight-set .prov-card{display:grid;gap:12px;padding:14px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.025));transition:border-color .14s ease,transform .14s ease,background .14s ease}.cw-insight-set .feature-row{grid-template-columns:minmax(0,1fr) auto;align-items:center;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.02))}.cw-insight-set .feature-row:hover,.cw-insight-set .prov-card:hover{border-color:rgba(118,139,255,.22);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.03))}.cw-insight-set .feature-name{font-size:14px;font-weight:900;color:#f4f7ff}.cw-insight-set .switch{--w:56px;--h:32px;--dot:24px;--pad:4px;position:relative;display:inline-block;flex:0 0 auto;width:var(--w)!important;height:var(--h)!important}.cw-insight-set .switch input,.cw-insight-set .pill input{position:absolute;opacity:0;pointer-events:none}.cw-insight-set .switch .slider{position:absolute;inset:0;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(7,10,18,.72);box-shadow:inset 0 1px 0 rgba(255,255,255,.05);transition:background .2s ease,border-color .2s ease,box-shadow .2s ease}.cw-insight-set .switch .slider::before{content:"";position:absolute;left:var(--pad);top:50%;width:var(--dot);height:var(--dot);transform:translateY(-50%);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.96),rgba(223,231,255,.84));box-shadow:0 4px 14px rgba(0,0,0,.34);transition:left .2s ease}.cw-insight-set .switch input:checked+.slider{background:linear-gradient(135deg,rgba(124,92,255,.48),rgba(45,226,255,.28));border-color:rgba(124,92,255,.52);box-shadow:0 0 0 1px rgba(124,92,255,.2) inset,0 0 18px rgba(124,92,255,.14)}.cw-insight-set .switch input:checked+.slider::before{left:calc(100% - var(--dot) - var(--pad))}.cw-insight-set .providers-top,.cw-insight-set .prov-top,.cw-insight-set .prov-tools,.cw-insight-set .actions{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}.cw-insight-set .providers-count{gap:8px;padding:0 12px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.04);color:rgba(216,226,248,.72)}.cw-insight-set .providers-count .dot{box-shadow:0 0 12px rgba(124,92,255,.4)}.cw-insight-set .prov-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px}.cw-insight-set .prov-card{display:flex;flex-direction:column;box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}.cw-insight-set .prov-card[data-empty="1"]{opacity:.76}.cw-insight-set .prov-title{font-size:13px;letter-spacing:.12em;color:#f5f7ff}.cw-insight-set .prov-copy{margin-top:5px;color:rgba(209,219,243,.58)}.cw-insight-set .prov-badge{padding:0 10px;border:1px solid rgba(120,141,255,.22);background:linear-gradient(180deg,rgba(124,92,255,.18),rgba(45,226,255,.08));color:#edf1ff;box-shadow:inset 0 1px 0 rgba(255,255,255,.06)}.cw-insight-set .mini{padding:0 11px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.035);color:rgba(222,230,249,.76);cursor:pointer;transition:transform .14s ease,border-color .14s ease,background .14s ease}.cw-insight-set .mini:hover,.cw-insight-set .pill:hover .lab{border-color:rgba(118,139,255,.24);background:rgba(255,255,255,.07)}.cw-insight-set [data-list]{display:grid;grid-template-columns:repeat(auto-fit,minmax(132px,1fr));gap:9px}.cw-insight-set .pill{display:flex;min-height:42px;cursor:pointer;user-select:none}.cw-insight-set .pill .lab{display:flex;align-items:center;justify-content:center;width:100%;padding:0 12px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03);font-size:12px;font-weight:800;line-height:1.2;color:rgba(223,231,249,.76);transition:transform .14s ease,border-color .14s ease,background .14s ease,color .14s ease,box-shadow .14s ease}.cw-insight-set .pill input:checked+.lab{border-color:rgba(118,139,255,.34);background:linear-gradient(180deg,rgba(124,92,255,.24),rgba(45,226,255,.12));color:#f7f9ff;box-shadow:0 10px 24px rgba(20,28,56,.22),inset 0 1px 0 rgba(255,255,255,.06)}.cw-insight-set .loading{padding:12px 14px;border-radius:16px;border:1px dashed rgba(255,255,255,.12);background:rgba(255,255,255,.03);color:rgba(213,222,245,.66)}.cw-insight-set .actions{padding:14px 18px!important;border-top:1px solid rgba(255,255,255,.08)!important;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,0))!important}.cw-insight-set .toast{min-height:18px;color:rgba(209,219,242,.62)}.cw-insight-set .action-row{gap:10px}.cw-insight-set .btn{min-width:108px;height:40px;padding:0 16px;border-radius:14px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);color:#f6f8ff;font-size:12px;letter-spacing:.08em;cursor:pointer;transition:transform .14s ease,box-shadow .14s ease,border-color .14s ease,filter .14s ease}.cw-insight-set .btn.danger{background:rgba(255,255,255,.04)}.cw-insight-set .btn.good{background:linear-gradient(135deg,#7c5cff,#4c7dff 52%,#2de2ff);border-color:rgba(118,139,255,.46);box-shadow:0 16px 34px rgba(76,125,255,.24),0 0 18px rgba(118,139,255,.16)}.cw-insight-set .btn.good:hover{filter:brightness(1.05)}@media (max-width:960px){.cw-insight-set .layout{grid-template-columns:1fr}}@media (max-width:640px){.cw-insight-set .cx-head{align-items:flex-start;flex-direction:column}.cw-insight-set .head-actions{width:100%;justify-content:space-between}.cw-insight-set .actions{flex-direction:column;align-items:stretch}.cw-insight-set .action-row{width:100%;justify-content:stretch}.cw-insight-set .btn{flex:1 1 0}}`;
const HTML = `<div class="cx-head"><div class="head-left"><div class="head-icon" aria-hidden="true"><span>⚙︎</span></div><div class="head-copy"><div class="head-eyebrow"><span class="dot"></span><span>Insights</span></div><div class="head-title">Insights settings</div><div class="head-sub">Choose which features and profiles shape the statistics panel.</div></div></div><div class="head-actions"><div class="head-chip" id="is-head-chip">Preparing</div><button class="close-btn" id="is-close" type="button">Close</button></div></div><div class="body"><div class="layout"><section class="panel"><div class="panel-head"><div><div class="panel-title">Features</div></div><div class="panel-chip">View</div></div><div class="panel-body"><div class="feature-list" id="is-feat-grid"></div></div></section><section class="panel"><div class="panel-head"><div><div class="panel-title">Profiles</div><div class="panel-sub">Choose which provider instances contribute data.</div></div><div class="panel-chip">Sources</div></div><div class="panel-body providers-shell"><div class="providers-top"><div class="providers-note">Only configured providers and instances are listed here.</div><div class="providers-count"><span class="dot"></span><span id="is-providers-count">0 providers</span></div></div><div class="loading" id="is-loading">Loading configured providers…</div><div class="prov-grid" id="is-prov-grid" style="display:none"></div></div></section></div></div><div class="actions"><div class="toast" id="is-toast"></div><div class="action-row"><button class="btn danger" id="is-reset" type="button">Reset</button><button class="btn good" id="is-apply" type="button">Apply</button></div></div>`;

const injectCSS = () => {
  if ($("#cw-insight-set-css")) return;
  const el = document.createElement("style");
  el.id = "cw-insight-set-css";
  el.textContent = STYLE;
  document.head.appendChild(el);
};

const parseInstanceList = (raw) => {
  const out = { ids: [], labels: { default: "Default" } };
  for (const it of Array.isArray(raw) ? raw : []) {
    const id = typeof it === "string" ? it : String(it?.id || "").trim();
    if (!id || out.ids.includes(id)) continue;
    out.ids.push(id);
    const label = typeof it === "object" && it ? String(it.label || "").trim() : "";
    if (label) out.labels[id] = label;
  }
  if (!out.ids.includes("default")) out.ids.unshift("default");
  return out;
};

const normalizePrefs = (prefs, byProvider = {}) => {
  const out = prefs && typeof prefs === "object" ? JSON.parse(JSON.stringify(prefs)) : {};
  const f = out.features && typeof out.features === "object" ? out.features : {};
  out.features = { watchlist: f.watchlist !== false, ratings: f.ratings !== false, history: f.history !== false, progress: f.progress !== false, playlists: f.playlists === true };
  out.instances = out.instances && typeof out.instances === "object" ? out.instances : {};
  out.known_instances = out.known_instances && typeof out.known_instances === "object" ? out.known_instances : {};
  for (const [prov, list] of Object.entries(byProvider || {})) {
    const key = String(prov || "").toLowerCase();
    const all = Array.isArray(list) && list.length ? list.map(String) : ["default"];
    const cur = out.instances[key];
    out.instances[key] = cur === undefined ? [...all] : Array.isArray(cur) ? cur.map(String).filter((x) => all.includes(x)) : [];
    out.known_instances[key] = [...all];
  }
  return out;
};

const renderFeatures = (prefs) => FEATS.map((key) => {
  const label = FeatureMeta().label?.(key) || key;
  const copy = FEAT_COPY[key] || "";
  return `<div class="feature-row"><div><div class="feature-name">${h(label)}</div><div class="feature-copy">${h(copy)}</div></div><label class="switch" for="is-feat-${esc(key)}"><input type="checkbox" id="is-feat-${esc(key)}" data-feat="${h(key)}" ${prefs.features?.[key] !== false ? "checked" : ""}><span class="slider"></span></label></div>`;
}).join("");

const renderProviderCard = (provider, all, selected, labels) => {
  const key = String(provider || "").toLowerCase(), picked = new Set(selected), count = all.filter((id) => picked.has(id)).length;
  return `<section class="prov-card" data-provider="${h(key)}" data-empty="${count ? 0 : 1}"><div class="prov-top"><div><div class="prov-title">${h(provLabel(key))}</div><div class="prov-copy">Choose which profiles feed the insights tiles.</div></div><div class="prov-tools"><span class="prov-badge" data-badge>${count}/${all.length}</span><button class="mini" type="button" data-all>All</button><button class="mini" type="button" data-none>None</button></div></div><div data-list>${all.map((id) => `<label class="pill" for="is-${esc(key)}-${esc(id)}"><input type="checkbox" id="is-${esc(key)}-${esc(id)}" data-inst="${h(id)}" ${picked.has(id) ? "checked" : ""}><span class="lab">${h(labels?.[key]?.[id] || (id === "default" ? "Default" : id))}</span></label>`).join("")}</div></section>`;
};

const pathGet = (obj, path) => (path || []).reduce((acc, key) => (acc && typeof acc === "object" ? acc[key] : undefined), obj);
const hasValue = (v) => typeof v === "string" ? v.trim().length > 0 : !!v;
const hasAnyConfigValue = (root, keys = []) => {
  if (!root || typeof root !== "object") return false;
  if (keys.some((key) => hasValue(root[key]))) return true;
  const inst = root.instances;
  return !!(inst && typeof inst === "object" && Object.values(inst).some((row) => row && typeof row === "object" && keys.some((key) => hasValue(row[key]))));
};
const hasTmdbConfig = (root) => {
  const match = (block) => !!(block && typeof block === "object" && ((hasValue(block.api_key) && hasValue(block.session_id)) || hasValue(block.account_id)));
  if (match(root)) return true;
  const inst = root?.instances;
  return !!(inst && typeof inst === "object" && Object.values(inst).some(match));
};
const getAllowedProviders = (cfg = window._cfgCache || {}) => {
  try {
    if (typeof window.getConfiguredProviders === "function") return new Set(Array.from(window.getConfiguredProviders(cfg) || []).map(canonProv).filter(Boolean));
  } catch {}
  const set = new Set(), checks = [
    { key: "PLEX", paths: [["plex"]], keys: ["account_token", "token"] },
    { key: "SIMKL", paths: [["simkl"], ["auth", "simkl"]], keys: ["access_token"] },
    { key: "TRAKT", paths: [["trakt"], ["auth", "trakt"]], keys: ["access_token"] },
    { key: "ANILIST", paths: [["anilist"], ["auth", "anilist"]], keys: ["access_token", "token"] },
    { key: "JELLYFIN", paths: [["jellyfin"], ["auth", "jellyfin"]], keys: ["access_token"] },
    { key: "EMBY", paths: [["emby"], ["auth", "emby"]], keys: ["access_token", "api_key", "token"] },
    { key: "MDBLIST", paths: [["mdblist"], ["auth", "mdblist"]], keys: ["api_key"] },
  ];
  for (const def of checks) if (def.paths.some((path) => hasAnyConfigValue(pathGet(cfg, path), def.keys))) set.add(def.key);
  if ([cfg?.tmdb_sync, cfg?.tmdb, cfg?.auth?.tmdb_sync].some(hasTmdbConfig)) set.add("TMDB");
  if ([cfg?.tautulli, cfg?.auth?.tautulli].some((block) => hasAnyConfigValue(block, ["api_key", "server_url", "server"]))) set.add("TAUTULLI");
  if ((cfg?.crosswatch || cfg?.CrossWatch || {}).enabled !== false) set.add("CROSSWATCH");
  return set;
};

const buildProviders = async () => {
  const labels = {}, byProvider = {}, [instApi, cfg] = await Promise.all([jget(`/api/provider-instances?cb=${Date.now()}`), jget(`/api/config?cb=${Date.now()}`)]);
  const instMap = instApi || {}, allowed = getAllowedProviders(cfg || window._cfgCache || {}), relevant = new Set(["CROSSWATCH", "PLEX", "SIMKL", "TRAKT", "ANILIST", "MDBLIST", "JELLYFIN", "EMBY", "TAUTULLI", "TMDB"]);
  const getRaw = async (key) => {
    const up = canonProv(key), candidates = [up, key, up.toLowerCase(), ...(up === "TMDB" ? ["TMDB_SYNC", "tmdb_sync"] : [])];
    for (const k of candidates) if (k && Object.prototype.hasOwnProperty.call(instMap, k)) return instMap[k];
    return await jget(`/api/provider-instances/${encodeURIComponent(key)}?cb=${Date.now()}`);
  };
  for (const prov of Array.from(allowed).filter((key) => relevant.has(key)).map(provKey).filter(Boolean).sort((a, b) => a.localeCompare(b))) {
    const parsed = parseInstanceList(await getRaw(prov));
    if (!parsed.ids.length) continue;
    byProvider[prov] = parsed.ids;
    labels[prov] = parsed.labels;
  }
  return { byProvider, labels };
};

export default {
  async mount(root) {
    injectCSS();
    root.classList.add("modal-root", "cw-insight-set");
    root.style.setProperty("--cxModalMaxW", "1080px");
    root.style.setProperty("--cxModalMaxH", "86vh");
    root.style.setProperty("--cxModalW", "min(var(--cxModalMaxW,1080px),calc(100vw - 56px))");
    root.innerHTML = HTML;

    const toast = $("#is-toast", root), chip = $("#is-head-chip", root), count = $("#is-providers-count", root), loading = $("#is-loading", root), grid = $("#is-prov-grid", root);
    const setToast = (msg = "") => { if (toast) toast.textContent = msg; };
    const refreshStats = () => {
      const providers = $$(".prov-card", root).length;
      if (chip) chip.textContent = `${providers} providers`;
      if (count) count.textContent = `${providers} provider${providers === 1 ? "" : "s"}`;
    };
    const updateCard = (card) => {
      if (!card) return;
      const checks = $$('input[data-inst]', card), on = checks.filter((c) => c.checked).length;
      const badge = $('[data-badge]', card);
      if (badge) badge.textContent = `${on}/${checks.length}`;
      card.dataset.empty = on ? 0 : 1;
      refreshStats();
    };

    $("#is-close", root)?.addEventListener("click", close);
    $("#is-reset", root)?.addEventListener("click", () => { try { localStorage.removeItem(PREF_KEY); } catch {} changed(); close(); });

    try {
      const { byProvider, labels } = await buildProviders();
      const prefs = normalizePrefs(loadPrefs(), byProvider);
      const featGrid = $("#is-feat-grid", root);
      if (featGrid) featGrid.innerHTML = renderFeatures(prefs);

      const provKeys = Object.keys(byProvider).sort((a, b) => a.localeCompare(b));
      if (!provKeys.length) {
        if (loading) loading.textContent = "No configured providers yet.";
      } else {
        if (loading) loading.style.display = "none";
        if (grid) {
          grid.style.display = "grid";
          grid.innerHTML = provKeys.map((prov) => {
            const all = byProvider[prov]?.map(String) || ["default"];
            const picked = prefs.instances[prov] === undefined ? all : Array.isArray(prefs.instances[prov]) ? prefs.instances[prov].map(String) : [];
            return renderProviderCard(prov, all, picked, labels);
          }).join("");
          $$(".prov-card", grid).forEach(updateCard);
        }
      }
      refreshStats();

      root.addEventListener("click", (ev) => {
        const btn = ev.target?.closest?.("[data-all],[data-none]");
        if (!btn) return;
        const card = btn.closest(".prov-card");
        $$('input[data-inst]', card).forEach((c) => { c.checked = btn.hasAttribute("data-all"); });
        updateCard(card);
      });

      root.addEventListener("change", (ev) => {
        const t = ev.target;
        if (!(t instanceof Element)) return;
        if (t.matches('input[data-feat]')) return setToast(""), refreshStats();
        if (t.matches('input[data-inst]')) updateCard(t.closest('.prov-card'));
      });

      $("#is-apply", root)?.addEventListener("click", () => {
        const next = normalizePrefs(loadPrefs(), byProvider);
        for (const c of $$('input[data-feat]', root)) next.features[c.dataset.feat] = !!c.checked;
        if (!Object.values(next.features).some(Boolean)) {
          next.features.watchlist = true;
          const fallback = $("#is-feat-watchlist", root);
          if (fallback) fallback.checked = true;
          setToast("At least one feature stays enabled.");
        }
        next.instances = next.instances && typeof next.instances === "object" ? next.instances : {};
        for (const card of $$(".prov-card", root)) {
          const prov = String(card.dataset.provider || "").toLowerCase();
          const checks = $$('input[data-inst]', card), all = checks.map((c) => String(c.dataset.inst || "")), selected = checks.filter((c) => c.checked).map((c) => String(c.dataset.inst || ""));
          if (selected.length === all.length) delete next.instances[prov];
          else next.instances[prov] = selected;
        }
        savePrefs(next);
        changed();
        close();
      });
    } catch (e) {
      console.error("Insight settings mount failed:", e);
      if (chip) chip.textContent = "Error";
      if (loading) loading.textContent = `Failed to load providers: ${String(e?.message || e)}`;
      setToast("Failed to load insight settings. See console.");
    }
  }
};
