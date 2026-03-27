/* assets/js/insights.js */
/* refactored */
/* CrossWatch - Insight Module for watchlist, ratings, history, progress, playlists */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (w, d) {
  const authSetupPending = () => w.cwIsAuthSetupPending?.() === true;
  const featureMeta = w.CW?.FeatureMeta || {};
  const providerMeta = w.CW?.ProviderMeta || {};
  const FEAT_LABEL = featureMeta.labels || { watchlist:"Watchlist", ratings:"Ratings", history:"History", progress:"Progress", playlists:"Playlists" };
  const FEATS = featureMeta.order || Object.keys(FEAT_LABEL);
  const PREF_KEY = "insights.settings.v1";
  const $ = (s, r = d) => r.querySelector(s), $$ = (s, r = d) => [...r.querySelectorAll(s)], lc = s => String(s || "").toLowerCase();
  const featureLabel = v => featureMeta.label?.(v) || FEAT_LABEL[lc(v)] || String(v || "");
  const providerLabel = v => providerMeta.label?.(v) || String(v || "");
  const titleOf = x => x?.display_title || x?.title || x?.series_title || x?.name || (x?.type === "episode" && x?.series_title && Number.isInteger(x?.season) && Number.isInteger(x?.episode) ? `${x.series_title} S${String(x.season).padStart(2,"0")}E${String(x.episode).padStart(2,"0")}` : x?.key) || "item";
  const subtitleOf = x => x?.display_subtitle || "";
  const readJSON = (key, fallback = {}) => { try { return JSON.parse(localStorage.getItem(key) || "{}") || fallback; } catch { return fallback; } };
  const loadPrefs = () => readJSON(PREF_KEY, {});
  const savePrefs = p => { try { localStorage.setItem(PREF_KEY, JSON.stringify(p || {})); } catch {} };
  const rowTs = row => { const ts = new Date(row?.finished_at || row?.started_at || 0).getTime(); return Number.isFinite(ts) ? ts : null; };
  const totalsFor = (feat, row) => { const f = row?.features?.[feat] || {}; const a = f.added | 0, r = f.removed | 0, u = f.updated | 0; return { a, r, u, sum: a + r + u }; };
  const fmtWhen = row => { const dt = new Date(row?.finished_at || row?.started_at || 0); if (!Number.isFinite(+dt)) return "—"; return `${String(dt.getDate()).padStart(2,"0")}-${String(dt.getMonth()+1).padStart(2,"0")}-${String(dt.getFullYear()).slice(-2)} ${String(dt.getHours()).padStart(2,"0")}:${String(dt.getMinutes()).padStart(2,"0")}`; };
  const fmtDur = v => { if (v == null) return "—"; const n = parseFloat(String(v).replace(/[^\d.]/g, "")); return Number.isFinite(n) ? `${n.toFixed(1)}s` : "—"; };
  const asNum = (v, fb = 0) => Number.isFinite(+v) ? +v : fb;
  const tickUrl = url => `${url}${url.includes("?") ? "&" : "?"}_ts=${Date.now()}`;
  const fetchJSON = async url => {
    if (authSetupPending()) throw new Error("auth setup pending");
    const res = await fetch(tickUrl(url), { credentials: "same-origin", cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.json();
  };
  const postOK = async url => {
    const res = await fetch(url, { method: "POST", credentials: "same-origin" });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    const body = await res.json().catch(() => ({}));
    if (body?.ok === false) throw new Error(body.error || "Backend reported failure");
    return body;
  };
  const clone = v => v && typeof v === "object" ? JSON.parse(JSON.stringify(v)) : {};

  function normalizePrefs(p, instancesByProvider = {}) {
    const out = clone(p), f = out.features && typeof out.features === "object" ? out.features : {};
    out.features = { watchlist: f.watchlist !== false, ratings: f.ratings !== false, history: f.history !== false, progress: f.progress !== false, playlists: f.playlists === true };
    out.instances = out.instances && typeof out.instances === "object" ? out.instances : {};
    out.known_instances = out.known_instances && typeof out.known_instances === "object" ? out.known_instances : {};
    for (const [prov, list] of Object.entries(instancesByProvider || {})) {
      const key = lc(prov);
      if (!key) continue;
      const all = Array.isArray(list) && list.length ? list.map(String) : ["default"];
      const prev = new Set(Array.isArray(out.known_instances[key]) ? out.known_instances[key].map(String) : []);
      const cur = out.instances[key];
      if (cur !== undefined) {
        const keep = (Array.isArray(cur) ? cur : []).map(String).filter(x => all.includes(x));
        for (const x of all) if (!prev.has(x) && !keep.includes(x)) keep.push(x);
        out.instances[key] = keep;
      }
      out.known_instances[key] = all.slice();
    }
    if (!Object.values(out.features).some(Boolean)) out.features.watchlist = true;
    return out;
  }

  const visibleFeatures = p => {
    const out = FEATS.filter(k => (p?.features || {})[k] !== false);
    return out.length ? out : ["watchlist"];
  };

  function selectionDiffers(p, instancesByProvider = {}) {
    const inst = p?.instances || {};
    for (const [prov, list] of Object.entries(instancesByProvider || {})) {
      const all = Array.isArray(list) && list.length ? list.map(String) : ["default"], cur = inst[lc(prov)];
      if (cur === undefined) continue;
      if (!Array.isArray(cur) || cur.length !== all.length) return true;
      const want = new Set(cur.map(String));
      for (const x of all) if (!want.has(x)) return true;
    }
    return false;
  }

  let _prefs = loadPrefs(), _visibleFeats = visibleFeatures(_prefs);
  const clampFeature = name => _visibleFeats.includes(String(name)) ? name : (_visibleFeats[0] || "watchlist");
  let _feature = clampFeature(localStorage.getItem("insights.feature"));
  let _lastStatsFetch = 0, _cwSnapModal = null;

  function syncPrefs(instancesByProvider = {}) {
    const next = normalizePrefs(_prefs, instancesByProvider), changed = JSON.stringify(next) !== JSON.stringify(_prefs);
    _prefs = next;
    if (changed) savePrefs(_prefs);
    _visibleFeats = visibleFeatures(_prefs);
    const keep = clampFeature(_feature);
    if (keep !== _feature) {
      _feature = keep;
      localStorage.setItem("insights.feature", keep);
    }
  }

  function ensureStyles() {
    const id = "insights-provider-styles-v8";
    if (d.getElementById(id)) return;
    const s = d.createElement("style");
    s.id = id;
    s.textContent = `#insights-footer{position:absolute;left:12px;right:12px;bottom:12px;z-index:2}#insights-footer .ins-foot-wrap{position:relative;display:flex;flex-direction:column;gap:12px;padding:12px;border-radius:18px;background:linear-gradient(180deg,rgba(9,11,19,.82),rgba(8,10,18,.62));box-shadow:inset 0 1px 0 rgba(255,255,255,.06),inset 0 0 0 1px rgba(130,150,255,.10),0 18px 36px rgba(0,0,0,.32);backdrop-filter:blur(10px) saturate(118%);-webkit-backdrop-filter:blur(10px) saturate(118%)}#insights-footer .ins-foot-wrap::before{content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;background:linear-gradient(135deg,rgba(124,92,255,.10),transparent 34%,transparent 68%,rgba(45,161,255,.08));opacity:.9}@media(max-width:820px){#insights-footer{position:static;margin-top:10px}}#insights-switch{display:grid;grid-template-columns:minmax(0,1fr) 34px;align-items:center;gap:10px}#insights-switch .seg{min-width:0;display:grid;grid-template-columns:repeat(var(--ins-feat-count,4),minmax(0,1fr));gap:8px}#insights-switch .seg-btn{min-width:0;display:flex;align-items:center;justify-content:center;height:40px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;appearance:none;border:1px solid rgba(255,255,255,.08);cursor:pointer;font:inherit;font-weight:800;letter-spacing:.18px;font-size:.79rem;padding:0 14px;border-radius:999px;color:rgba(234,240,255,.82);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.02));box-shadow:inset 0 1px 0 rgba(255,255,255,.06);transition:transform .14s ease,box-shadow .16s ease,border-color .16s ease,color .16s ease,background .16s ease}#insights-switch .seg-btn:hover{transform:translateY(-1px);color:#fff;border-color:rgba(124,92,255,.22);box-shadow:inset 0 1px 0 rgba(255,255,255,.08),0 10px 18px rgba(0,0,0,.16)}#insights-switch .seg-btn.active{color:#f4f7ff;border-color:rgba(124,92,255,.34);background:linear-gradient(180deg,rgba(124,92,255,.18),rgba(45,161,255,.10));box-shadow:inset 0 1px 0 rgba(255,255,255,.10),0 0 0 1px rgba(124,92,255,.20),0 12px 22px rgba(13,19,38,.34)}#insights-switch .ins-gear{display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border:1px solid rgba(255,255,255,.08)!important;background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025))!important;border-radius:999px!important;padding:0!important;color:#fff!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.08);transition:transform .14s ease,box-shadow .16s ease,border-color .16s ease,color .16s ease}#insights-switch .ins-gear .material-symbols-rounded{display:block;font-size:22px;line-height:1;color:#fff;font-variation-settings:"FILL" 0,"wght" 500,"GRAD" 0,"opsz" 24}#insights-switch .ins-gear:hover{transform:translateY(-1px);color:#fff!important;border-color:rgba(124,92,255,.24)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.10),0 10px 18px rgba(0,0,0,.16)}#stats-card #stat-providers{--prov-cols:4;--tile-h:104px;display:grid!important;grid-template-columns:repeat(var(--prov-cols),minmax(0,1fr))!important;grid-auto-rows:var(--tile-h)!important;gap:12px!important;width:100%!important;align-items:stretch!important}#stats-card #stat-providers .tile{--brand:255,255,255;--wm:none;position:relative!important;display:block!important;height:var(--tile-h)!important;min-height:var(--tile-h)!important;max-height:var(--tile-h)!important;border-radius:18px!important;background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.02))!important;overflow:hidden!important;isolation:isolate!important;margin:0!important;padding:0!important;border:1px solid rgba(var(--brand),.18)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.08),0 16px 30px rgba(0,0,0,.26),0 0 0 1px rgba(var(--brand),.06);backdrop-filter:blur(4px) saturate(120%);-webkit-backdrop-filter:blur(4px) saturate(120%);transition:transform .16s ease,box-shadow .18s ease,border-color .18s ease}#stats-card #stat-providers .tile:hover{transform:translateY(-2px);border-color:rgba(var(--brand),.28)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.10),0 20px 36px rgba(0,0,0,.30),0 0 26px rgba(var(--brand),.14)}#stats-card #stat-providers .tile::before{content:"";position:absolute;inset:0;pointer-events:none;z-index:0;background:radial-gradient(88% 72% at 18% 14%,rgba(var(--brand),.20),transparent 52%),radial-gradient(72% 58% at 82% 76%,rgba(var(--brand),.10),transparent 66%),linear-gradient(180deg,rgba(255,255,255,.04),transparent 42%)}#stats-card #stat-providers .tile::after{content:"";position:absolute;left:50%;top:54%;transform:translate(-50%,-50%) rotate(-8deg);width:220%;height:220%;background-repeat:no-repeat;background-position:center;background-size:contain;background-image:var(--wm);mix-blend-mode:screen;opacity:.22;filter:saturate(1.4) brightness(1.18) contrast(1.02)}#stats-card #stat-providers .tile.inactive{border-color:rgba(var(--brand),.12)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.06),0 12px 20px rgba(0,0,0,.20),0 0 0 1px rgba(var(--brand),.04);filter:saturate(.88)}#stats-card #stat-providers .tile.inactive::after{opacity:.14;filter:saturate(.95) brightness(.96)}#stats-card #stat-providers .tile .tile-head{position:absolute;left:50%;top:8px;transform:translateX(-50%);display:flex;align-items:center;justify-content:center;gap:8px;z-index:2;pointer-events:none}#stats-card #stat-providers .tile .tile-k{display:none!important}#stats-card #stat-providers .tile .tile-state{display:inline-flex;align-items:center;gap:5px;padding:2px 6px;border-radius:999px;border:1px solid rgba(255,255,255,.06);background:rgba(6,10,18,.24);color:rgba(236,241,255,.58);font-size:10px;line-height:1;font-weight:800;letter-spacing:.08em;text-transform:uppercase;box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}#stats-card #stat-providers .tile .tile-state i{display:block;width:7px;height:7px;border-radius:999px;background:rgba(148,163,184,.55);box-shadow:0 0 0 1px rgba(255,255,255,.06)}#stats-card #stat-providers .tile .tile-state.on{color:rgba(216,255,232,.78);border-color:rgba(34,197,94,.18);background:rgba(8,26,18,.24)}#stats-card #stat-providers .tile .tile-state.on i{background:#21d07a;box-shadow:0 0 12px rgba(33,208,122,.42)}#stats-card #stat-providers .tile .n{position:absolute;top:53%;left:50%;transform:translate(-50%,-50%) scale(var(--ins-font-scale,1));transform-origin:center;margin:0;font-weight:900;letter-spacing:.25px;font-variant-numeric:tabular-nums;font-size:clamp(28px,calc(var(--tile-h)*.46),54px);line-height:1;color:rgba(255,255,255,.38);z-index:1}@supports(-webkit-background-clip:text){#stats-card #stat-providers .tile .n{background-image:linear-gradient(180deg,rgba(255,255,255,.88),rgba(232,236,245,.48) 52%,rgba(255,255,255,.16));-webkit-background-clip:text;-webkit-text-fill-color:transparent;color:transparent}}@supports(background-clip:text){#stats-card #stat-providers .tile .n{background-image:linear-gradient(180deg,rgba(255,255,255,.88),rgba(232,236,245,.48) 52%,rgba(255,255,255,.16));background-clip:text;color:transparent}}#stats-card #stat-providers [data-provider=crosswatch] .cw-snapshot{position:absolute;left:10px;right:10px;bottom:10px;padding:0 2px;font-size:11px;line-height:1.2;font-weight:800;text-align:center;color:rgba(255,255,255,.62);text-shadow:0 1px 2px rgba(0,0,0,.85);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;z-index:2}#stats-card #stat-providers .tile .mse{position:absolute;left:50%;bottom:4px;display:inline-flex!important;align-items:center!important;justify-content:center!important;gap:2px!important;width:max-content!important;max-width:none;padding:0;white-space:nowrap;text-align:center;transform:translateX(-50%) scale(var(--ins-mse-scale,1));transform-origin:center bottom;pointer-events:none;z-index:2}html.cw-compact #stats-card #stat-providers .tile .mse{display:inline-flex!important;align-items:center!important;justify-content:center!important;width:max-content!important}html.cw-compact #stats-card #stat-providers .tile .mse .mse-chip{flex:0 0 auto!important}#stats-card #stat-providers .tile .mse .mse-chip{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;gap:1px;min-width:0;padding:2px 5px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(6,10,18,.36);box-shadow:inset 0 1px 0 rgba(255,255,255,.06)}#stats-card #stat-providers .tile .mse .mse-chip .k{font-size:10px;line-height:1;letter-spacing:.08em;text-transform:uppercase;color:rgba(255,255,255,.48);font-weight:800}#stats-card #stat-providers .tile .mse .mse-chip .v{font-size:12px;line-height:1;color:rgba(255,255,255,.82);font-weight:900;font-variant-numeric:tabular-nums}.cw-snap-picker{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;padding:18px;background:rgba(4,8,16,.46);backdrop-filter:blur(10px) saturate(116%);-webkit-backdrop-filter:blur(10px) saturate(116%);z-index:1200;opacity:1;transition:opacity .16s ease}.cw-snap-picker.cw-snap-hidden{opacity:0;pointer-events:none}.cw-snap-dialog{width:min(760px,calc(100vw - 32px));display:grid;gap:14px;padding:16px;border-radius:22px;background:linear-gradient(180deg,rgba(10,13,22,.92),rgba(7,10,18,.84));border:1px solid rgba(124,92,255,.16);box-shadow:inset 0 1px 0 rgba(255,255,255,.08),0 28px 60px rgba(0,0,0,.42),0 0 0 1px rgba(59,130,246,.06)}.cw-snap-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}.cw-snap-title-wrap{min-width:0;display:grid;gap:4px}.cw-snap-kicker{font-size:10px;line-height:1;letter-spacing:.16em;text-transform:uppercase;color:rgba(190,198,214,.56);font-weight:800}.cw-snap-title{font-size:14px;line-height:1.25;font-weight:800;color:rgba(243,246,255,.92)}.cw-snap-title .hl{color:#d9e6ff}.snap-icon-close,.snap-close,.snap-btn{appearance:none;font:inherit}.snap-icon-close{flex:0 0 auto;display:inline-flex;align-items:center;justify-content:center;width:32px;height:32px;border-radius:12px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.04);color:rgba(240,244,255,.74);cursor:pointer;transition:transform .14s ease,border-color .16s ease,background .16s ease,color .16s ease}.snap-icon-close:hover{transform:translateY(-1px);border-color:rgba(124,92,255,.24);background:rgba(255,255,255,.06);color:#fff}.cw-snap-body{display:grid;gap:8px;grid-auto-rows:max-content;align-content:start;justify-items:center;max-height:min(56vh,392px);overflow:auto;padding-right:2px}.cw-snap-empty{padding:10px 2px;font-size:13px;line-height:1.45;color:rgba(197,205,220,.72)}.snap-btn{width:min(100%,640px);display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 12px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.022));color:rgba(240,244,255,.84);cursor:pointer;box-shadow:inset 0 1px 0 rgba(255,255,255,.05);transition:transform .14s ease,border-color .16s ease,box-shadow .18s ease,background .16s ease}.snap-btn:hover{transform:translateY(-1px);border-color:rgba(124,92,255,.24);background:linear-gradient(180deg,rgba(124,92,255,.10),rgba(45,161,255,.06));box-shadow:inset 0 1px 0 rgba(255,255,255,.08),0 14px 24px rgba(0,0,0,.20)}.snap-btn:active{transform:translateY(0)}.snap-btn .snap-name{min-width:0;font-size:13px;line-height:1.2;font-weight:800;letter-spacing:.01em;text-align:left;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.snap-btn .snap-meta{flex:0 0 auto;display:inline-flex;align-items:center;justify-content:center;min-width:56px;padding:4px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(8,12,22,.44);font-size:10px;line-height:1;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(201,210,226,.64)}.snap-btn.is-latest .snap-meta{color:rgba(216,255,232,.82);border-color:rgba(34,197,94,.20);background:rgba(8,26,18,.30)}.cw-snap-foot{display:flex;justify-content:flex-end}.snap-close{padding:8px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.04);color:rgba(236,241,255,.78);cursor:pointer;transition:transform .14s ease,border-color .16s ease,background .16s ease}.snap-close:hover{transform:translateY(-1px);border-color:rgba(124,92,255,.22);background:rgba(255,255,255,.06);color:#fff}#sync-history{display:grid;gap:8px}#sync-history .history-item{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:11px 12px;border-radius:14px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.018));border:1px solid rgba(255,255,255,.06);box-shadow:inset 0 1px 0 rgba(255,255,255,.04);transition:border-color .16s ease,transform .14s ease,box-shadow .18s ease}#sync-history .history-item:hover{transform:translateY(-1px);border-color:rgba(124,92,255,.18);box-shadow:inset 0 1px 0 rgba(255,255,255,.06),0 12px 22px rgba(0,0,0,.14)}#sync-history .history-item.ok{border-color:rgba(34,197,94,.12)}#sync-history .history-item.warn{border-color:rgba(245,158,11,.16)}#sync-history .history-item.err{border-color:rgba(239,68,68,.18)}#sync-history .history-main{min-width:0;display:grid;gap:4px}#sync-history .history-meta{display:flex;align-items:center;gap:8px;min-width:0;font-size:12px;opacity:1}#sync-history .history-time{min-width:0;color:rgba(227,233,245,.82);font-weight:700;letter-spacing:.01em}#sync-history .history-sub{font-size:11px;line-height:1.2;color:rgba(174,182,194,.72);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}#sync-history .history-badges{display:flex;align-items:center;gap:6px;flex-wrap:wrap;justify-content:flex-end}#sync-history .history-badges .badge,#sync-history .history-meta .badge{display:inline-flex;align-items:center;justify-content:center;padding:4px 8px;border-radius:999px;line-height:1.1;border:1px solid rgba(255,255,255,.10);background:rgba(8,10,18,.46);font-size:11px;font-weight:800}#sync-history .history-meta .badge.ok{background:rgba(25,195,125,.12);border-color:rgba(25,195,125,.30);color:#c8ffe6}#sync-history .history-meta .badge.warn{background:rgba(255,196,0,.10);border-color:rgba(255,196,0,.28);color:#ffe28a}#sync-history .history-meta .badge.err{background:rgba(255,77,79,.12);border-color:rgba(255,77,79,.30);color:#ffd2d2}#sync-history .history-badges .badge.add{background:rgba(25,195,125,.10);border-color:rgba(25,195,125,.26);color:#c8ffe6}#sync-history .history-badges .badge.del{background:rgba(255,77,79,.10);border-color:rgba(255,77,79,.26);color:#ffd2d2}#sync-history .history-badges .badge.micro{background:rgba(124,92,255,.10);border-color:rgba(124,92,255,.24);color:#ddd7ff}@media(max-width:720px){#insights-switch{grid-template-columns:minmax(0,1fr) 32px;gap:8px}#insights-switch .seg{gap:8px}#insights-switch .seg-btn{height:38px;padding:0 12px;font-size:.76rem}#insights-switch .ins-gear{width:32px;height:32px}}@media(max-width:560px){#insights-switch{grid-template-columns:minmax(0,1fr) 30px;gap:6px}#insights-switch .seg{gap:6px}#insights-switch .seg-btn{height:36px;padding:0 10px;font-size:.72rem}#insights-switch .ins-gear{width:30px;height:30px}#stats-card #stat-providers{grid-template-columns:repeat(2,minmax(0,1fr))!important}#stats-card #stat-providers .tile{border-radius:16px!important}#stats-card #stat-providers .tile .tile-state .txt{display:none}}@media(max-width:380px){#stats-card #stat-providers{grid-template-columns:repeat(1,minmax(0,1fr))!important}#sync-history .history-item{align-items:flex-start;flex-direction:column}#sync-history .history-badges{justify-content:flex-start}}`;
    d.head.appendChild(s);
    const s2 = d.createElement("style");
    s2.id = "insights-provider-styles-v8-mse";
    s2.textContent = `#stats-card #stat-providers .tile .mse{display:inline-flex!important;flex-direction:column!important;align-items:center!important;justify-content:center!important;gap:1px!important}html.cw-compact #stats-card #stat-providers .tile .mse{flex-direction:column!important}#stats-card #stat-providers .tile .mse .mse-row{display:inline-flex;align-items:center;justify-content:center;gap:2px;width:max-content}#stats-card #stat-providers .tile .mse .mse-row-anime{margin-top:-1px}#stats-card #stat-providers .tile .mse .mse-chip{gap:1px;padding:2px 5px}#stats-card #stat-providers .tile .mse .mse-chip .k{display:inline-flex;align-items:center;justify-content:center;min-width:1ch}#stats-card #stat-providers .tile .mse .mse-chip-anime{padding-inline:4px}`;
    d.head.appendChild(s2);
  }

  async function openInsightSettingsModal() {
    try {
      if (typeof w.openInsightSettingsModal === "function") return await w.openInsightSettingsModal({});
      const v = encodeURIComponent(String(w.__CW_VERSION__ || Date.now()));
      const mod = await import(`./modals.js?v=${v}`);
      if (typeof mod.openModal === "function") await mod.openModal("insight-settings", {});
    } catch (e) {
      console.error("[Insights] Failed to open settings modal", e);
    }
  }

  const getConfiguredProviders = async force => {
    try {
      let cfg = w._cfgCache;
      const needsConfigLoad = force || !cfg || typeof cfg !== "object" || !Object.keys(cfg).length;
      if (needsConfigLoad && typeof w.CW?.API?.Config?.load === "function") {
        try {
          cfg = await w.CW.API.Config.load(!!force);
          if (cfg && typeof cfg === "object") w._cfgCache = cfg;
        } catch {}
      }
      const out = typeof w.getConfiguredProviders === "function" ? await w.getConfiguredProviders(cfg || {}) : ["crosswatch"];
      return out instanceof Set ? out : new Set([...(out || [])]);
    } catch (e) {
      console.error("[Insights] Failed to resolve configured providers", e);
      return new Set(["crosswatch"]);
    }
  };

  function filterProviderTotals(block, instancesByProvider = {}) {
    const raw = block?.raw || {}, instCounts = raw.providers_instances, instMse = raw.providers_instances_mse;
    if (!selectionDiffers(_prefs, instancesByProvider) || !instCounts || typeof instCounts !== "object") {
      return { providers: block.providers || {}, mse: raw.providers_mse || null, now: block.now };
    }
    const out = {}, outMse = {}, selected = _prefs?.instances || {}, zero = () => ({ movies:0, shows:0, anime:0, episodes:0 });
    for (const [prov, byInst] of Object.entries(instCounts || {})) {
      const key = lc(prov), map = byInst && typeof byInst === "object" ? byInst : {}, keys = Object.keys(map), want = Array.isArray(selected[key]) ? selected[key].map(String) : selected[key] === undefined ? keys : [];
      out[key] = want.reduce((sum, id) => sum + (map[id] | 0), 0);
      const mseMap = instMse?.[key] && typeof instMse[key] === "object" ? instMse[key] : {}, agg = zero();
      for (const id of want) {
        const part = mseMap[id];
        if (!part || typeof part !== "object") continue;
        agg.movies += part.movies | 0; agg.shows += part.shows | 0; agg.anime += part.anime | 0; agg.episodes += part.episodes | 0;
      }
      outMse[key] = agg;
    }
    for (const [prov, v] of Object.entries(block.providers || {})) if (out[lc(prov)] === undefined) out[lc(prov)] = v | 0;
    for (const [prov, v] of Object.entries(raw.providers_mse || {})) if (outMse[lc(prov)] === undefined) outMse[lc(prov)] = v;
    const vals = Object.values(out).map(v => v | 0).filter(v => v > 0);
    return { providers: out, mse: outMse, now: vals.length ? Math.max(...vals) : block.now | 0 };
  }

  function pickBlock(data, feat) {
    const raw = data?.features?.[feat];
    if (!raw) throw new Error(`[Insights] Missing feature block for "${feat}"`);
    let { now, week, month, added, removed } = raw;
    const rows = (Array.isArray(data.history) ? data.history : []).map(r => ({ r, ts: rowTs(r) })).filter(x => x.ts != null).sort((a, b) => a.ts - b.ts);
    const sumSince = since => rows.reduce((acc, { r, ts }) => {
      if (ts < since) return acc;
      const t = totalsFor(feat, r);
      acc.A += t.a; acc.R += t.r; acc.S += t.sum;
      return acc;
    }, { A: 0, R: 0, S: 0 });
    const ms = Date.now();
    if (!Number.isFinite(+now)) now = rows.length ? totalsFor(feat, rows.at(-1).r).sum : 0;
    if (!Number.isFinite(+week)) week = sumSince(ms - 7 * 86400000).S;
    if (!Number.isFinite(+month)) month = sumSince(ms - 30 * 86400000).S;
    if (!Number.isFinite(+added) || !Number.isFinite(+removed)) {
      const m = sumSince(ms - 30 * 86400000);
      if (!Number.isFinite(+added)) added = m.A;
      if (!Number.isFinite(+removed)) removed = m.R;
    }
    return {
      series: Array.isArray(raw.series) ? raw.series : [],
      providers: raw.providers || {},
      active: raw.providers_active || data?.providers_active || {},
      now: asNum(now), week: asNum(week), month: asNum(month), added: asNum(added), removed: asNum(removed),
      raw
    };
  }

  function hydrateBlock(data) {
    syncPrefs(data?.instances_by_provider || {});
    const blk = pickBlock(data, _feature), filtered = filterProviderTotals(blk, data?.instances_by_provider || {});
    blk.providers = filtered.providers;
    blk.now = Number.isFinite(filtered.now) ? filtered.now : blk.now;
    if (blk.raw) blk.raw.providers_mse = filtered.mse || blk.raw.providers_mse;
    return blk;
  }

  function renderSparkline(id, points) {
    const el = d.getElementById(id);
    if (!el) return;
    if (!points?.length) return void (el.innerHTML = '<div class="muted">No data</div>');
    const wv = el.clientWidth || 260, hv = el.clientHeight || 64, pad = 4, xs = points.map(p => +p.ts || 0), ys = points.map(p => +p.count || 0);
    const minX = Math.min(...xs), maxX = Math.max(...xs), minY = Math.min(...ys), maxY = Math.max(...ys);
    const X = t => maxX === minX ? pad : pad + (wv - 2 * pad) * (t - minX) / (maxX - minX);
    const Y = v => maxY === minY ? hv / 2 : hv - pad - (hv - 2 * pad) * (v - minY) / (maxY - minY);
    el.innerHTML = `<svg viewBox="0 0 ${wv} ${hv}" preserveAspectRatio="none"><path class="line" d="${points.map((p, i) => `${i ? "L" : "M"}${X(p.ts)},${Y(p.count)}`).join(" ")}"></path>${points.map(p => `<circle class="dot" cx="${X(p.ts)}" cy="${Y(p.count)}"></circle>`).join("")}</svg>`;
  }

  const ease = t => t < .5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
  function fitProviderNumber(el) {
    const tile = el?.closest?.("#stat-providers .tile");
    if (!tile) return;
    el.style.setProperty("--ins-font-scale", "1");
    const tw = tile.getBoundingClientRect().width, nw = el.getBoundingClientRect().width;
    if (tw && nw) el.style.setProperty("--ins-font-scale", String(Math.min(1, tw * .78 / nw).toFixed(3)));
  }
  function fitProviderMSE(el) {
    const tile = el?.closest?.("#stat-providers .tile");
    if (!tile) return;
    el.style.setProperty("--ins-mse-scale", "1");
    const tw = tile.getBoundingClientRect().width, mw = el.getBoundingClientRect().width;
    if (!tw || !mw) return;
    const digits = [...el.querySelectorAll(".mse-chip .v")].map(n => String(n.textContent || "").trim().length).filter(Boolean), maxDigits = digits.length ? Math.max(...digits) : 0, totalDigits = digits.reduce((a, n) => a + n, 0);
    let minScale = .84;
    if (maxDigits <= 1 && totalDigits <= 3) minScale = .97;
    else if (maxDigits <= 2 && totalDigits <= 5) minScale = .93;
    else if (maxDigits <= 3 && totalDigits <= 8) minScale = .89;
    el.style.setProperty("--ins-mse-scale", String(Math.max(minScale, Math.min(1, (tw - 10) / mw)).toFixed(3)));
  }
  function animateNumber(el, to, duration = 650) {
    if (!el) return;
    const from = parseInt(el.dataset?.v || el.textContent || "0", 10) || 0, done = () => { el.textContent = String(to); el.dataset.v = String(to); fitProviderNumber(el); };
    if (from === to) return done();
    const start = performance.now(), dur = Math.max(180, duration);
    const step = now => {
      const p = Math.min(1, (now - start) / dur);
      el.textContent = String(Math.round(from + (to - from) * ease(p)));
      p < 1 ? requestAnimationFrame(step) : done();
    };
    requestAnimationFrame(step);
  }
  function animateChart(now, week, month) {
    const max = Math.max(1, now, week, month), scale = v => Math.max(.04, v / max);
    [[".bar.week", week], [".bar.month", month], [".bar.now", now]].forEach(([sel, val]) => { const el = $(sel); if (el) el.style.transform = `scaleY(${scale(val)})`; });
  }
  const refitProviderNumbers = () => { $$("#stat-providers .tile .n").forEach(fitProviderNumber); $$("#stat-providers .tile .mse").forEach(fitProviderMSE); };
  w.addEventListener("resize", refitProviderNumbers, { passive: true });

  const footWrap = (() => {
    let timer = 0;
    const ensure = () => {
      ensureStyles();
      let foot = $("#insights-footer");
      if (!foot) {
        foot = d.createElement("div");
        foot.id = "insights-footer";
        foot.className = "ins-footer";
        foot.innerHTML = '<div class="ins-foot-wrap"></div>';
        ($("#stats-card") || d.body).appendChild(foot);
      }
      return $(".ins-foot-wrap", foot) || foot;
    };
    ensure.reserve = () => {
      const card = $("#stats-card"), foot = $("#insights-footer");
      if (!card || !foot) return;
      clearTimeout(timer);
      timer = setTimeout(() => card.style.paddingBottom = `${(foot.getBoundingClientRect().height || foot.offsetHeight || 120) + 14}px`, 0);
    };
    w.addEventListener("resize", ensure.reserve, { passive: true });
    return ensure;
  })();

  function placeSwitchBeforeTiles() {
    const wrap = footWrap(), sw = $("#insights-switch"), grid = $("#stat-providers");
    if (!sw) return;
    if (!wrap.contains(sw)) wrap.appendChild(sw);
    const ref = grid?.parentNode === wrap ? grid : null;
    if (sw.nextSibling !== ref) try { wrap.insertBefore(sw, ref); } catch {}
  }

  function markActiveSwitcher() {
    $$("#insights-switch .seg-btn").forEach(btn => {
      const on = btn.dataset.key === _feature;
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
  }

  function switchFeature(name) {
    const want = clampFeature(name);
    if (want === _feature) return;
    _feature = want;
    localStorage.setItem("insights.feature", want);
    markActiveSwitcher();
    refreshInsights(true);
  }

  function ensureSwitch() {
    const wrap = footWrap();
    let host = $("#insights-switch");
    if (!host) {
      host = d.createElement("div");
      host.id = "insights-switch";
      host.className = "ins-switch";
      host.innerHTML = '<div class="seg" role="tablist" aria-label="Insights features"></div><button class="ins-gear" type="button" title="Insight settings" aria-label="Insight settings"><span class="material-symbols-rounded" aria-hidden="true">settings</span></button>';
      wrap.appendChild(host);
    } else if (host.parentNode !== wrap) wrap.appendChild(host);
    if (!host.dataset.bound) {
      host.addEventListener("click", ev => {
        const btn = ev.target.closest(".seg-btn");
        if (btn) return switchFeature(btn.dataset.key);
        if (ev.target.closest(".ins-gear")) openInsightSettingsModal();
      });
      host.dataset.bound = "1";
    }
    const seg = $(".seg", host), sig = _visibleFeats.join(",");
    host.style.setProperty("--ins-feat-count", String(Math.max(1, _visibleFeats.length)));
    if (host.dataset.feats !== sig || host.dataset.cur !== _feature) {
      seg.innerHTML = _visibleFeats.map(f => `<button class="seg-btn${_feature === f ? " active" : ""}" data-key="${f}" role="tab" aria-selected="${_feature === f}">${featureLabel(f)}</button>`).join("");
      host.dataset.feats = sig;
      host.dataset.cur = _feature;
    }
    placeSwitchBeforeTiles();
    markActiveSwitcher();
    footWrap.reserve();
  }

  const providerSelected = prov => {
    const cur = _prefs?.instances?.[lc(prov)];
    return cur === undefined || !Array.isArray(cur) ? cur !== false : cur.length > 0;
  };

  function renderProviderStats(provTotals = {}, provActive = {}, configuredSet = new Set(), breakdownMap = {}, instancesByProvider = {}) {
    const wrap = footWrap();
    let host = $("#stat-providers");
    if (!host) {
      host = d.createElement("div");
      host.id = "stat-providers";
      wrap.appendChild(host);
    } else if (host.parentNode !== wrap) wrap.appendChild(host);
    if (!host.dataset.bound) {
      host.addEventListener("click", ev => {
        const tile = ev.target.closest('.tile[data-provider="crosswatch"]');
        if (tile && _feature !== "playlists") openCrosswatchSnapshotPicker(_feature);
      });
      host.dataset.bound = "1";
    }

    const canonical = k => lc(k) === "crosswatch" ? "crosswatch" : String(k || "").toUpperCase();
    const totals = Object.fromEntries(Object.entries(provTotals).map(([k, v]) => [canonical(k), v]));
    const active = Object.fromEntries(Object.entries(provActive).map(([k, v]) => [canonical(k), v]));
    const conf = new Set([...configuredSet].map(canonical));
    const keys = [...new Set([...Object.keys(totals), ...Object.keys(active), ...conf])]
      .filter(k => conf.has(canonical(k)) && providerSelected(k, instancesByProvider))
      .sort((a, b) => a === "crosswatch" ? -1 : b === "crosswatch" ? 1 : a.localeCompare(b));

    if (!keys.length) {
      host.hidden = true;
      return footWrap.reserve();
    }

    host.hidden = false;
    host.style.setProperty("--prov-cols", Math.max(1, Math.min(keys.length, 4)));
    const seen = new Set();

    for (const key of keys) {
      const prov = lc(key), label = providerLabel(prov) || key, total = +totals[key] || 0, per = breakdownMap?.[prov] || null, live = !!active[key];
      let tile = $(`#tile-${key}`);
      if (!tile) {
        tile = d.createElement("div");
        tile.id = `tile-${key}`;
        tile.className = "tile provider";
        tile.dataset.provider = prov;
        tile.innerHTML = '<div class="tile-head"><span class="tile-k"></span><span class="tile-state"><i></i><span class="txt"></span></span></div><div class="n" data-v="0">0</div><div class="mse"></div>';
        host.appendChild(tile);
      } else if (tile.parentNode !== host) host.appendChild(tile);

      tile.style.cursor = prov === "crosswatch" ? "pointer" : "";
      tile.classList.toggle("inactive", !live);
      $(".tile-k", tile).textContent = label;
      $(".tile-state", tile).classList.toggle("on", live);
      $(".tile-state .txt", tile).textContent = live ? "Live" : "Idle";
      animateNumber($(".n", tile), total, 650);

      const info = $(".mse", tile);
      if (!per || prov === "crosswatch" || _feature === "playlists") {
        info.textContent = "";
        info.style.display = "none";
        tile.title = prov === "crosswatch" ? `${label} • ${total} • Click to switch snapshot` : `${label} • ${total}`;
      } else {
        const m = +per.movies || 0, s = +per.shows || 0, a = +per.anime || 0;
        const mainParts = [["M", m], ["S", s]].filter(([, v]) => v).map(([k, v]) => `<span class="mse-chip"><span class="k">${k}</span><span class="v">${v}</span></span>`);
        const animePart = a ? `<span class="mse-chip mse-chip-anime"><span class="k">A</span><span class="v">${a}</span></span>` : "";
        if (!mainParts.length && !animePart) {
          info.textContent = "";
          info.style.display = "none";
          tile.title = `${label} • ${total}`;
        } else {
          info.innerHTML = `<span class="mse-row mse-row-main">${mainParts.join("")}</span>${animePart ? `<span class="mse-row mse-row-anime">${animePart}</span>` : ""}`;
          info.style.display = "";
          fitProviderMSE(info);
          tile.title = `${label} • ${total} • Movies ${m} • Shows ${s} • Anime ${a}`;
        }
      }
      seen.add(tile.id);
    }

    [...host.querySelectorAll(".tile")].forEach(tile => !seen.has(tile.id) && tile.remove());
    placeSwitchBeforeTiles();
    footWrap.reserve();
  }

  function renderCrossWatchSnapshotHint(cwSnapshots) {
    const tile = d.querySelector('#stat-providers [data-provider="crosswatch"]');
    if (!tile) return;
    const old = $(".cw-snapshot", tile);
    if (_feature === "playlists") return void old?.remove();
    const info = cwSnapshots?.[_feature];
    if (!info?.has_snapshots || !info?.actual) return void old?.remove();
    const label = old || (() => { const el = d.createElement("div"); el.className = "cw-snapshot"; tile.appendChild(el); return el; })();
    label.textContent = String(info.selected || "latest") === "latest" ? "Latest" : (info.human || info.actual);
    label.title = info.actual;
  }

  function renderHistoryTabs(hist) {
    const wrap = $("#sync-history") || $("[data-role='sync-history']") || $(".sync-history");
    if (!wrap) return;
    if (!wrap.dataset.listInit) {
      wrap.innerHTML = '<div class="list"></div>';
      wrap.dataset.listInit = "1";
    }
    const list = $(".list", wrap);
    if (!list) return;
    const rows = (Array.isArray(hist) ? hist : []).slice().sort((a, b) => new Date(b.finished_at || b.started_at || 0) - new Date(a.finished_at || a.started_at || 0)).filter(row => row?.features_enabled?.[_feature] !== false).slice(0, +(localStorage.getItem("insights.history.limit") || 4));
    if (!rows.length) return void (list.innerHTML = '<div class="history-item"><div class="history-meta muted">No runs for this feature</div></div>');
    list.innerHTML = rows.map(row => {
      const t = totalsFor(_feature, row), cls = typeof row?.exit_code === "number" && row.exit_code !== 0 ? "err" : String(row?.result || "").toUpperCase() === "EQUAL" || t.sum === 0 ? "ok" : "warn";
      return `<div class="history-item ${cls}"><div class="history-main"><div class="history-meta"><span class="history-time">${fmtWhen(row)}</span><span class="badge ${cls}">${row?.result || "—"}${typeof row?.exit_code === "number" ? ` · ${row.exit_code}` : ""}</span></div><div class="history-sub">${featureLabel(_feature)} • ${fmtDur(row?.duration_sec)}</div></div><div class="history-badges"><span class="badge add">+${t.a}</span><span class="badge del">-${t.r}</span>${t.u ? `<span class="badge micro">~${t.u}</span>` : ""}</div></div>`;
    }).join("");
  }

  function renderTopStats({ now = 0, week = 0, month = 0, added = 0, removed = 0 }) {
    [["#stat-now", now], ["#stat-week", week], ["#stat-month", month], ["#stat-added", added], ["#stat-removed", removed]].forEach(([sel, val]) => animateNumber($(sel), val | 0));
    const fill = $("#stat-fill"), max = Math.max(1, now, week, month);
    if (fill) fill.style.width = `${Math.round(now / max * 100)}%`;
    animateChart(now, week, month);
    const lab = $("#stat-feature-label");
    if (lab) lab.textContent = featureLabel(_feature);
    const chip = $("#trend-week") || $("#stat-delta-chip");
    if (chip) {
      const diff = (now | 0) - (week | 0);
      chip.textContent = diff === 0 ? "no change" : `${diff > 0 ? "+" : ""}${diff} vs last week`;
      chip.classList.remove("up", "down", "flat", "muted");
      chip.classList.add(diff > 0 ? "up" : diff < 0 ? "down" : "flat");
      chip.classList.toggle("muted", diff === 0);
      chip.title = diff === 0 ? "No change versus last week" : `${Math.abs(diff)} ${diff > 0 ? "more" : "fewer"} than last week`;
    }
    $("#stat-breakdown")?.remove();
  }

  function renderWatchtime(wt) {
    const el = $("#watchtime"), note = $("#watchtime-note");
    if (el) el.innerHTML = `<div class="big">≈ ${wt.hours | 0}</div><div class="units">hrs <span style="opacity:.6">(${wt.days | 0} days)</span><br><span style="opacity:.8">${wt.movies | 0} movies • ${wt.shows | 0} shows</span></div>`;
    if (note) note.textContent = wt.method || "estimate";
  }

  async function renderFromData(data, statsOnly = false, forceConfigured = false) {
    const blk = hydrateBlock(data);
    footWrap();
    ensureSwitch();
    if (!statsOnly) {
      try { renderSparkline("sparkline", blk.series || []); } catch {}
      renderHistoryTabs(data?.history || []);
      if (data?.watchtime) renderWatchtime(data.watchtime);
    }
    renderTopStats(blk);
    const configured = await getConfiguredProviders(forceConfigured);
    renderProviderStats(blk.providers, blk.active, configured, blk.raw?.providers_mse || null, data?.instances_by_provider || {});
    renderCrossWatchSnapshotHint(data?.crosswatch_snapshots || null);
    footWrap.reserve();
    if (!statsOnly) setTimeout(footWrap.reserve, 0);
  }

  async function refreshInsights(force = false) {
    if (authSetupPending()) return;
    try { await renderFromData(await fetchJSON(`/api/insights?limit_samples=60&history=60${force ? `&t=${Date.now()}` : ""}`), false, force); }
    catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return;
      console.error("[Insights] Failed to load /api/insights", e);
    }
  }

  async function refreshStats(force = false) {
    if (authSetupPending()) return;
    const now = Date.now();
    if (!force && now - _lastStatsFetch < 900) return;
    _lastStatsFetch = now;
    try { await renderFromData(await fetchJSON("/api/insights?limit_samples=0&history=60"), true, force); }
    catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return;
      console.error("[Insights] Failed to load /api/insights (stats)", e);
    }
  }

  w.addEventListener("insights:settings-changed", () => { _prefs = loadPrefs(); _visibleFeats = visibleFeatures(_prefs); _feature = clampFeature(_feature); localStorage.setItem("insights.feature", _feature); refreshInsights(true); });

  w.Insights = Object.assign(w.Insights || {}, {
    renderSparkline, refreshInsights, refreshStats, fetchJSON, animateNumber, animateChart, titleOf, subtitleOf,
    switchFeature, get feature() { return _feature; }
  });
  w.renderSparkline = renderSparkline;
  w.refreshInsights = refreshInsights;
  w.refreshStats = refreshStats;
  w.fetchJSON = fetchJSON;
  w.animateNumber = w.animateNumber || animateNumber;
  w.titleOf = titleOf;
  w.subtitleOf = subtitleOf;
  w.scheduleInsights = function scheduleInsights(max) {
    if (authSetupPending()) return;
    let tries = 0, limit = max || 20;
    (function tick() {
      if (authSetupPending()) return;
      if ($("#sync-history") || $("#stat-now") || $("#sparkline")) return refreshInsights();
      if (++tries < limit) setTimeout(tick, 250);
    })();
  };

  d.addEventListener("DOMContentLoaded", () => { if (!authSetupPending()) w.scheduleInsights(); });
  d.addEventListener("tab-changed", ev => {
    if (authSetupPending()) return;
    const tab = String(ev?.detail?.id || ev?.detail?.tab || "").toLowerCase();
    if (tab === "main") refreshInsights(true);
  });

  const snapLabel = name => {
    const base = String(name || "").replace(/\.json$/, ""), stem = base.split("-", 1)[0], m = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(stem);
    return m ? `${m[1]}-${m[2]}-${m[3]} - ${m[4]}:${m[5]}` : base;
  };

  function ensureCrosswatchSnapshotModal() {
    if (_cwSnapModal?.parentNode) return _cwSnapModal;
    const modal = d.createElement("div");
    modal.className = "cw-snap-picker cw-snap-hidden";
    modal.innerHTML = '<div class="cw-snap-dialog" role="dialog" aria-modal="true" aria-labelledby="cw-snap-title"><div class="cw-snap-head"><div class="cw-snap-title-wrap"><div class="cw-snap-kicker">Snapshot selector</div><div class="cw-snap-title" id="cw-snap-title">Select snapshot for <span class="hl"></span></div></div><button class="snap-icon-close" type="button" aria-label="Close snapshot selector">×</button></div><div class="cw-snap-body"></div><div class="cw-snap-foot"><button class="snap-close" type="button">Cancel</button></div></div>';
    modal.addEventListener("click", async ev => {
      const close = ev.target === modal || ev.target.closest(".snap-close,.snap-icon-close");
      if (close) modal.classList.add("cw-snap-hidden");
      const btn = ev.target.closest(".snap-btn");
      if (!btn) return;
      const feature = modal.dataset.feature, name = btn.dataset.name;
      try { await postOK(`/api/crosswatch/select-snapshot?feature=${feature}&snapshot=${encodeURIComponent(name)}`); }
      catch (err) {
        console.error("[Insights] Failed to select snapshot", err);
        return void w.cxToast?.("Failed to set snapshot. Check server logs.");
      }
      modal.classList.add("cw-snap-hidden");
      w.cxToast?.(`Snapshot set: ${name === "latest" ? "latest" : snapLabel(name)}`);
      refreshInsights(true);
    });
    d.body.appendChild(modal);
    _cwSnapModal = modal;
    return modal;
  }

  async function openCrosswatchSnapshotPicker(feature) {
    const modal = ensureCrosswatchSnapshotModal(), body = $(".cw-snap-body", modal), head = $(".cw-snap-head .hl", modal);
    modal.dataset.feature = feature;
    if (head) head.textContent = featureLabel(feature);

    try {
      const data = await fetchJSON(`/api/files?path=${encodeURIComponent("/config/.cw_provider/snapshots")}`);
      const files = (Array.isArray(data?.files) ? data.files : data || []).filter(f => !f.is_dir && f.name?.endsWith(`-${feature}.json`)).sort((a, b) => a.name.localeCompare(b.name)).slice(-10).reverse();
      if (!files.length) body.innerHTML = '<div class="cw-snap-empty muted">No snapshots found for this feature yet.</div>';
      else body.innerHTML = [{ name: "latest", label: "Latest snapshot", meta: "Live" }, ...files.map(f => ({ name: f.name, label: snapLabel(f.name), meta: "Saved" }))].map(o => `<button class="snap-btn${o.name === "latest" ? " is-latest" : ""}" data-name="${o.name}" type="button"><span class="snap-name">${o.label}</span><span class="snap-meta">${o.meta}</span></button>`).join("");
    } catch (e) {
      console.error("[Insights] Failed to load snapshot files", e);
      body.innerHTML = '<div class="cw-snap-empty muted">Failed to load snapshots. Check server logs or configuration.</div>';
    }
    modal.classList.remove("cw-snap-hidden");
  }
})(window, document);
