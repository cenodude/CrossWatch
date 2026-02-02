/* assets/helpers/core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

/* Utilities */
const isTV = v => /^(tv|show|shows|series|season|episode|anime)$/i.test(String(v||""));

function _el(id) {
  return document.getElementById(id);
}
function _val(id, d = "") {
  const el = _el(id);
  return el && "value" in el ? el.value ?? d : d;
}
function _boolSel(id) {
  const v = _val(id, "false");
  return String(v).toLowerCase() === "true";
}
function _text(id, d = "") {
  const el = _el(id);
  return el ? el.textContent ?? d : d;
}

function _setVal(id, val) {
  const el = document.getElementById(id);
  if (el) el.value = val ?? "";
}
function _setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? "";
}
function _setChecked(id, on) {
  const el = document.getElementById(id);
  if (el) el.checked = !!on;
}

function setValIfExists(id, val) {
  const el = document.getElementById(id);
  if (el) el.value = val ?? "";
}

function stateAsBool(v) {
  if (v == null) return false;
  if (typeof v === "boolean") return v;
  if (typeof v === "object") {
    if ("connected"  in v) return !!v.connected;
    if ("ok"         in v) return !!v.ok;
    if ("authorized" in v) return !!v.authorized;
    if ("auth"       in v) return !!v.auth;
    if ("status"     in v) return /^(ok|connected|authorized|true|ready|valid)$/i.test(String(v.status));
  }
  return !!v;
}


function applyServerSecret(inputId, hasValue) {
  const el = document.getElementById(inputId);
  if (!el) return;
  el.value = hasValue ? "••••••••" : "";
  el.dataset.masked = hasValue ? "1" : "0";
  el.dataset.loaded = "1";
  el.dataset.touched = "";
  el.dataset.clear = "";
}
function startSecretLoad(inputId) {
  const el = document.getElementById(inputId);
  if (!el) return;
  el.dataset.loaded = "0";
  el.dataset.touched = "";
}
function finishSecretLoad(inputId, hasValue) {
  applyServerSecret(inputId, !!hasValue);
  try {
    if (String(inputId || "") === "tmdb_api_key" && typeof cwMetaSettingsHubUpdate === "function") cwMetaSettingsHubUpdate();
  } catch {}
}


function getConfiguredProviders(cfg = window._cfgCache || {}) {
  const S = new Set();
  const has = (v) => (typeof v === "string" ? v.trim().length > 0 : !!v);

  const hasInProvider = (obj, keys = []) => {
    if (!obj || typeof obj !== "object") return false;
    for (const k of keys) { if (has(obj[k])) return true; }
    const inst = obj.instances;
    if (inst && typeof inst === "object") {
      for (const v of Object.values(inst)) {
        if (!v || typeof v !== "object") continue;
        for (const k of keys) { if (has(v[k])) return true; }
      }
    }
    return false;
  };

  if (hasInProvider(cfg?.plex, ["account_token"])) S.add("PLEX");
  if (hasInProvider(cfg?.simkl, ["access_token"]) || has(cfg?.auth?.simkl?.access_token)) S.add("SIMKL");
  if (hasInProvider(cfg?.trakt, ["access_token"]) || has(cfg?.auth?.trakt?.access_token)) S.add("TRAKT");
  if (hasInProvider(cfg?.anilist, ["access_token"]) || has(cfg?.auth?.anilist?.access_token)) S.add("ANILIST");
  if (hasInProvider(cfg?.jellyfin, ["access_token"]) || has(cfg?.auth?.jellyfin?.access_token)) S.add("JELLYFIN");
  if (hasInProvider(cfg?.emby, ["access_token"]) || has(cfg?.auth?.emby?.access_token)) S.add("EMBY");
  if (hasInProvider(cfg?.mdblist, ["api_key"])) S.add("MDBLIST");

  const ts = cfg?.tmdb_sync || cfg?.auth?.tmdb_sync || {};
  const tmdbOk = (() => {
    if (!ts || typeof ts !== "object") return false;
    if ((has(ts.api_key) && has(ts.session_id)) || has(ts.account_id)) return true;
    const inst = ts.instances;
    if (inst && typeof inst === "object") {
      for (const v of Object.values(inst)) {
        if (!v || typeof v !== "object") continue;
        if ((has(v.api_key) && has(v.session_id)) || has(v.account_id)) return true;
      }
    }
    return false;
  })();
  if (tmdbOk) S.add("TMDB");

  const t = cfg?.tautulli || cfg?.auth?.tautulli || {};
  if (hasInProvider(t, ["api_key","server_url"])) S.add("TAUTULLI");

  const cw = cfg?.crosswatch || cfg?.CrossWatch || {};
  const cwEnabled =
    typeof cw.enabled === "boolean"
      ? cw.enabled
      : true;

  if (cwEnabled) S.add("CROSSWATCH");
  return S;
}


function resolveProviderKeyFromNode(node) {
  const attr = (node.getAttribute?.("data-sync-prov") || node.dataset?.syncProv || "").toUpperCase();
  if (attr) return attr;

  const img = node.querySelector?.('img[alt], .logo img[alt], [data-logo]');
  const alt = (img?.getAttribute?.('alt') || img?.dataset?.logo || "").toUpperCase();
  if (alt.includes("PLEX"))  return "PLEX";
  if (alt.includes("SIMKL")) return "SIMKL";
  if (alt.includes("TRAKT")) return "TRAKT";
  if (alt.includes("TMDB") || alt.includes("TMDBSYNC") || alt.includes("TMDB-SYNC")) return "TMDB";
  if (alt.includes("ANILIST")) return "ANILIST";
  if (alt.includes("JELLYFIN")) return "JELLYFIN";
  if (alt.includes("TAUTULLI")) return "TAUTULLI";
  if (alt.includes("MDBLIST")) return "MDBLIST";
  if (alt.includes("EMBY")) return "EMBY";
  if (alt.includes("CROSSWATCH")) return "CROSSWATCH";

  
  const tnode = node.querySelector?.(".title,.name,header,strong,h3,h4");
  const txt = (tnode?.textContent || node.textContent || "").toUpperCase();
  if (/\bPLEX\b/.test(txt))  return "PLEX";
  if (/\bSIMKL\b/.test(txt)) return "SIMKL";
  if (/\bTRAKT\b/.test(txt)) return "TRAKT";
  if (/\bTMDB\b/.test(txt) || /\bTMDB\s*SYNC\b/.test(txt) || /\bTMDB-SYNC\b/.test(txt)) return "TMDB";
  if (/\bANILIST\b/.test(txt)) return "ANILIST";
  if (/\bJELLYFIN\b/.test(txt)) return "JELLYFIN";
  if (/\bEMBY\b/.test(txt)) return "EMBY";
  if (/\bMDBLIST\b/.test(txt)) return "MDBLIST";
  if (/\bTAUTULLI\b/.test(txt)) return "TAUTULLI";
  if (/\bCROSSWATCH\b/.test(txt)) return "CROSSWATCH";

  return ""; 
}

function applySyncVisibility() {
  const allowed = getConfiguredProviders();
  const host = document.getElementById("providers_list");
  if (!host) return;

  let cards = host.querySelectorAll(".prov-card");
  if (!cards || cards.length === 0) {
  
    cards = host.querySelectorAll(":scope > .card, :scope > *");
  }

  cards.forEach((card) => {
    let key = (card.getAttribute?.("data-prov") || card.dataset?.prov || "").toUpperCase();

  
    if (!key) key = resolveProviderKeyFromNode(card);

    if (!key) return;
    card.dataset.syncProv = key; 
    card.style.display = allowed.has(key) ? "" : "none";
  });

  const LABEL = { PLEX: "Plex", SIMKL: "SIMKL", TRAKT: "Trakt", ANILIST: "AniList", TMDB: "TMDb", JELLYFIN: "Jellyfin", EMBY: "Emby", MDBLIST: "MDBList", TAUTULLI: "Tautulli", CROSSWATCH: "CrossWatch" };
  const PROVIDER_ORDER = ["CROSSWATCH","PLEX","SIMKL","TRAKT","ANILIST","TMDB","JELLYFIN","EMBY","MDBLIST","TAUTULLI"];
  ["source-provider", "target-provider"].forEach((id) => {
    const sel = document.getElementById(id);
    if (!sel) return;

    const hadPlaceholder = sel.options[0] && sel.options[0].value === "";
    const prev = (sel.value || "").toUpperCase();

    sel.innerHTML = "";
    if (hadPlaceholder) {
      const o0 = document.createElement("option");
      o0.value = ""; o0.textContent = "— select —";
      sel.appendChild(o0);
    }

    PROVIDER_ORDER.forEach((k) => {
      if (!allowed.has(k)) return;
      const o = document.createElement("option");
      o.value = k; o.textContent = LABEL[k] || k;
      sel.appendChild(o);
    });

    if (prev && allowed.has(prev)) sel.value = prev;
    else if (hadPlaceholder) sel.value = "";
  });

}

let __syncVisTick = 0;
function scheduleApplySyncVisibility() {
  if (__syncVisTick) return;
  const run = () => {
    __syncVisTick = 0;
    if (typeof applySyncVisibility === "function") {
      try { applySyncVisibility(); } catch (e) { console.warn("[sync-vis] apply failed", e); }
    }
  };
  const raf = window.requestAnimationFrame || ((f) => setTimeout(f, 0));
  __syncVisTick = raf(run);
}


function bindSyncVisibilityObservers() {
  const list = document.getElementById("providers_list");
  if (list && !list.__syncObs) {
    const obs = new MutationObserver(() => scheduleApplySyncVisibility());
    obs.observe(list, { childList: true, subtree: true });
    list.__syncObs = obs;
  }
  const footer = document.querySelector("#sec-sync .footer");
  if (footer && !footer.__syncObs) {
    const obs2 = new MutationObserver(() => scheduleApplySyncVisibility());
    obs2.observe(footer, { childList: true, subtree: true });
    footer.__syncObs = obs2;
  }
  if (!window.__syncVisEvt) {
    window.addEventListener("settings-changed", (e) => {
      if (e?.detail?.scope === "settings") scheduleApplySyncVisibility();
    });
    window.__syncVisEvt = true;
  }
  
  scheduleApplySyncVisibility();
}


const PAIRS_CACHE_KEY = "cw.pairs.v1";
const PAIRS_TTL_MS    = 15_000;

function _invalidatePairsCache(){ try { localStorage.removeItem(PAIRS_CACHE_KEY); } catch {} }

function _savePairsCache(pairs) {
  try { localStorage.setItem(PAIRS_CACHE_KEY, JSON.stringify({ pairs, t: Date.now() })); } catch {}
}
function _loadPairsCache() {
  try { return JSON.parse(localStorage.getItem(PAIRS_CACHE_KEY) || "null"); } catch { return null; }
}

async function _getPairsFresh() {
  try {
    const r = await fetch("/api/pairs", { cache: "no-store" });
    if (!r.ok) return null;
    const arr = await r.json();
    _savePairsCache(arr);
    return arr;
  } catch { return null; }
}

async function isWatchlistEnabledInPairs(){
  const freshWithin = (o) => o && (Date.now() - (o.t || 0) < PAIRS_TTL_MS);
  const anyWL = (arr) => Array.isArray(arr) && arr.some(p => !!(p?.features?.watchlist?.enable));
  const cached = _loadPairsCache();
  if (freshWithin(cached)) return anyWL(cached.pairs);
  const live = await _getPairsFresh();
  return anyWL(live);
}


const AUTO_STATUS = false; 
let lastStatusMs = 0;
const STATUS_MIN_INTERVAL = 24 * 60 * 60 * 1000; 

let busy = false,
  esDet = null,
  esSum = null,
  plexPoll = null,
  simklPoll = null,
  appDebug = false,
  currentSummary = null;
let detStickBottom = true; 
let wallLoaded = false,
  _lastSyncEpoch = null,
  _wasRunning = false;
let wallReqSeq = 0;   
window._ui = { status: null, summary: null };

const STATUS_CACHE_KEY = "cw.status.v1";


function normalizeProviders(input) {
  const pick = (o, k) => (o?.[k] ?? o?.[k.toLowerCase()] ?? o?.[k.toUpperCase()]);
  const normOne = (v) => {
    if (typeof v === "boolean") return { connected: v };
    if (v && typeof v === "object") {
      const c = v.connected ?? v.ok ?? v.online ?? v.status === "ok";
      return { connected: !!c };
    }
    return { connected: false };
  };
  const p = input || {};
  return {
    PLEX:    normOne(pick(p, "PLEX")    ?? p.plex_connected),
    SIMKL:   normOne(pick(p, "SIMKL")   ?? p.simkl_connected),
    TRAKT:   normOne(pick(p, "TRAKT")   ?? p.trakt_connected),
    ANILIST: normOne(pick(p, "ANILIST") ?? p.anilist_connected),
    TMDB:    normOne(pick(p, "TMDB")    ?? p.tmdb_connected),
    JELLYFIN:normOne(pick(p, "JELLYFIN")?? p.jellyfin_connected),
    EMBY:    normOne(pick(p, "EMBY")    ?? p.emby_connected),
    MDBLIST:  normOne(pick(p, "MDBLIST")  ?? p.mdblist_connected),
    TAUTULLI:  normOne(pick(p, "TAUTULLI")  ?? p.tautulli_connected),
    CROSSWATCH:normOne(pick(p, "CROSSWATCH")?? p.crosswatch_connected),
  };
}


function saveStatusCache(providers) {
  try {
    const normalized = normalizeProviders(providers);
    localStorage.setItem(
      STATUS_CACHE_KEY,
      JSON.stringify({ providers: normalized, updatedAt: Date.now(), v: 1 })
    );
  } catch {}
}

function loadStatusCache(maxAgeMs = 10 * 60 * 1000) {
  try {
    const obj = JSON.parse(localStorage.getItem(STATUS_CACHE_KEY) || "null");
    if (!obj || !obj.providers) return null;
    if (Date.now() - (obj.updatedAt || 0) > maxAgeMs) return null;
    return { providers: normalizeProviders(obj.providers), updatedAt: obj.updatedAt };
  } catch { return null; }
}

let _pairsFetchAt = 0;

async function refreshPairedProviders(throttleMs = 5000) {
  const now = Date.now();
  if (now - _pairsFetchAt < throttleMs && window._ui?.pairedProviders) {
    toggleProviderBadges(window._ui.pairedProviders);
    return window._ui.pairedProviders;
  }

  _pairsFetchAt = now;
  let pairs = [];
  try {
    const res = await fetch("/api/pairs", { cache: "no-store" });
    if (res.ok) pairs = await res.json();
  } catch (_) {}

  const active = { PLEX: false, SIMKL: false, TRAKT: false, ANILIST: false, TMDB: false, JELLYFIN: false, EMBY: false, MDBLIST: false, TAUTULLI: false, CROSSWATCH: false };
  for (const p of pairs || []) {
    if (p && p.enabled !== false) {
      const s = String(p.source || "").toUpperCase();
      const t = String(p.target || "").toUpperCase();
      if (s in active) active[s] = true;
      if (t in active) active[t] = true;
    }
  }

  
  window._ui = window._ui || {};
  window._ui.pairedProviders = active;

  toggleProviderBadges(active);
  return active;
}


function toggleProviderBadges(active){
  const map = { PLEX:"badge-plex", SIMKL:"badge-simkl", TRAKT:"badge-trakt", ANILIST:"badge-anilist", TMDB:"badge-tmdb", JELLYFIN:"badge-jellyfin", EMBY:"badge-emby", MDBLIST:"badge-mdblist", TAUTULLI:"badge-tautulli", CROSSWATCH:"badge-crosswatch" };
  for (const [prov,id] of Object.entries(map)){
    const el = document.getElementById(id);
    if (el) el.classList.toggle("hidden", !active?.[prov]);
  }
}


function connState(v) {
  if (v == null) return "unknown";

  if (v === true)  return "ok";
  if (v === false) return "no";

  if (typeof v === "number") {
    if (v === 1) return "ok";
    if (v === 0) return "no";
  }

  if (typeof v === "string") {
    const s = v.toLowerCase().trim();
    if (/^(ok|up|connected|ready|true|on|online|active)$/.test(s))   return "ok";
    if (/^(no|down|disconnected|false|off|disabled)$/.test(s))       return "no";
    if (/^(unknown|stale|n\/a|-|pending)$/.test(s))                  return "unknown";
    return "unknown";
  }

  
  if (typeof v === "object") {
    if (typeof v.connected === "boolean") return v.connected ? "ok" : "no";
    const b = v.ok ?? v.ready ?? v.active ?? v.online;
    if (typeof b === "boolean") return b ? "ok" : "no";

    const s = String(v.status ?? v.state ?? "").toLowerCase().trim();
    if (/^(ok|up|connected|ready|true|on|online|active)$/.test(s))   return "ok";
    if (/^(no|down|disconnected|false|off|disabled)$/.test(s))       return "no";
    if (/^(unknown|stale|n\/a|-|pending)$/.test(s))                  return "unknown";
  }

  return "unknown";
}


function pickCase(obj, k) {
  return obj?.[k] ?? obj?.[k.toLowerCase()] ?? obj?.[k.toUpperCase()];
}



function instancesTooltip(info) {
  const inst = info?.instances;
  const sum = info?.instances_summary;

  if (!inst || typeof inst !== "object") return "";

  const ok = Number(sum?.ok);
  const total = Number(sum?.total);
  const rep = String(sum?.rep || info?.rep_instance || "");

  const parts = [];

  if (Number.isFinite(ok) && Number.isFinite(total) && total > 1) {
    parts.push(`Instances: ${ok}/${total}`);
  }

  const entries = Object.entries(inst)
    .slice(0, 6)
    .map(([id, v]) => {
      const label = id === "default" ? "Default" : String(id);
      const c = !!(v && typeof v === "object" ? v.connected : v);
      return `${label}=${c ? "OK" : "NO"}`;
    });

  if (entries.length && (Number.isFinite(total) ? total > 1 : entries.length > 1)) {
    parts.push(entries.join(" · "));
  }

  if (rep && rep !== "default" && (Number.isFinite(total) ? total > 1 : true)) {
    parts.push(`Rep: ${rep}`);
  }

  return parts.filter(Boolean).join(" · ");
}

function svgCrown() {
  return '<svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M3 7l4 3 5-6 5 6 4-3v10H3zM5 15h14v2H5z"/></svg>';
}
function svgCheck() {
  return '<svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M9 16.2L5.5 12.7l1.4-1.4 2.1 2.1 6-6 1.4 1.4z"/></svg>';
}


function setBadge(id, providerName, state, stale, provKey, info) {
  const el = document.getElementById(id);
  if (!el) return;

  el.classList.remove("ok", "no", "unknown", "stale");
  el.classList.add(state);
  if (stale) el.classList.add("stale");
  el.classList.add("conn");

  
  let tag = "";
  if (provKey === "PLEX" && info && info.plexpass) {
    const plan = String(info?.subscription?.plan || "").toLowerCase();
    const label = plan === "lifetime" ? "Plex Pass • Lifetime" : "Plex Pass";
    tag = `<span class="tag plexpass" title="${label}">${svgCrown()}${label}</span>`;
  } else if (provKey === "TRAKT" && info && info.vip) {
    const t = String(info.vip_type || "vip").toLowerCase();
    const lbl = /plus|ep/.test(t) ? "VIP+" : "VIP";
    tag = `<span class="tag vip" title="Trakt ${lbl}">${svgCheck()}${lbl}</span>`;
  }

  
    const instTip = instancesTooltip(info);

if (provKey === "TRAKT" && info && typeof info === "object") {
    const lim = info.limits || {};
    const wl  = lim.watchlist  || {};
    const col = lim.collection || {};

    const bits = [];

    bits.push(info.vip ? "VIP account" : "Free account");

    const wlUsed = Number(wl.used);
    const wlMax  = Number(wl.item_count);
    if (Number.isFinite(wlUsed) && Number.isFinite(wlMax) && wlMax > 0) {
      bits.push(`Watchlist: ${wlUsed}/${wlMax}`);
    }

    const colUsed = Number(col.used);
    const colMax  = Number(col.item_count);
    if (Number.isFinite(colUsed) && Number.isFinite(colMax) && colMax > 0) {
      bits.push(`Collection: ${colUsed}/${colMax}`);
    }

    const last = info.last_limit_error;
    if (last && last.feature && last.ts) {
      bits.push(`Last limit: ${last.feature} @ ${last.ts}`);
    }

    if (instTip) bits.unshift(instTip);

    if (bits.length) {
      el.title = bits.join(" · ");
    }
  }

  if (provKey !== "TRAKT" && instTip) {
    el.title = instTip;
  }

  
  const labelState = state === "ok" ? "Connected" : state === "no" ? "Not connected" : "Unknown";
  el.innerHTML =

    `${tag}<span class="txt">` +
      `<span class="dot ${state}"></span>` +
      `<span class="name">${providerName}</span>` +
      `<span class="state">· ${labelState}</span>` +
    `</span>`;
}


function renderConnectorStatus(providers, { stale = false } = {}) {
  const p = providers || {};
  const plex    = pickCase(p, "PLEX");
  const simkl   = pickCase(p, "SIMKL");
  const trakt   = pickCase(p, "TRAKT");
  const anilist = pickCase(p, "ANILIST");
  const jelly   = pickCase(p, "JELLYFIN");
  const emby    = pickCase(p, "EMBY");
  const mdbl  = pickCase(p, "MDBLIST");
  const taut  = pickCase(p, "TAUTULLI");

  setBadge("badge-plex",     "Plex",     connState(plex  ?? false), stale, "PLEX",     plex);
  setBadge("badge-simkl",    "SIMKL",    connState(simkl ?? false), stale, "SIMKL",    simkl);
  setBadge("badge-trakt",    "Trakt",    connState(trakt ?? false), stale, "TRAKT",    trakt);
  setBadge("badge-anilist",  "AniList",   connState(anilist ?? false), stale, "ANILIST",  anilist);
  setBadge("badge-jellyfin", "Jellyfin", connState(jelly ?? false), stale, "JELLYFIN", jelly);
  setBadge("badge-emby",     "Emby",     connState(emby  ?? false), stale, "EMBY",     emby);
  setBadge("badge-mdblist",  "MDBlist",  connState(mdbl ?? false), stale, "MDBLIST", mdbl);
  setBadge("badge-tautulli", "Tautulli", connState(taut ?? false), stale, "TAUTULLI", taut);
}

function fetchWithTimeout(url, opts = {}, ms = 15000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort("timeout"), ms);
  return fetch(url, { cache: "no-store", ...opts, signal: ac.signal })
    .finally(() => clearTimeout(t));
}

/*! Status */
async function refreshStatus(force = false) {
  const now = Date.now();
  if (!force && typeof lastStatusMs !== "undefined" && typeof STATUS_MIN_INTERVAL !== "undefined" && (now - lastStatusMs < STATUS_MIN_INTERVAL)) return;
  if (typeof lastStatusMs !== "undefined") lastStatusMs = now;

  try {

        await refreshPairedProviders(force ? 0 : 5000);
    const res = await fetchWithTimeout("/api/status", {}, 15000);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const r = await res.json();

    if (typeof appDebug !== "undefined") appDebug = !!r.debug;

    const pick = (obj, k) => (obj?.[k] ?? obj?.[k.toLowerCase()] ?? obj?.[k.toUpperCase()]);
    const norm = (v, fb = false) => (typeof v === "boolean" ? { connected: v } : (v && typeof v === "object") ? v : { connected: !!fb });

    const pRaw = r.providers || {};
    const providers = {
      PLEX:     norm(pick(pRaw, "PLEX"),     (r.plex_connected    ?? r.plex)),
      SIMKL:    norm(pick(pRaw, "SIMKL"),    (r.simkl_connected   ?? r.simkl)),
      TRAKT:    norm(pick(pRaw, "TRAKT"),    (r.trakt_connected   ?? r.trakt)),
      ANILIST:  norm(pick(pRaw, "ANILIST"),  (r.anilist_connected ?? r.anilist)),
      JELLYFIN: norm(pick(pRaw, "JELLYFIN"), (r.jellyfin_connected?? r.jellyfin)),
      EMBY:     norm(pick(pRaw, "EMBY"),     (r.emby_connected    ?? r.emby)),
      MDBLIST:  norm(pick(pRaw, "MDBLIST"),  (r.mdblist_connected  ?? r.mdblist)),
      TAUTULLI: norm(pick(pRaw, "TAUTULLI"), (r.tautulli_connected ?? r.tautulli)),
    };

    renderConnectorStatus(providers, { stale: false });
    saveStatusCache?.(providers);

    window._ui = window._ui || {};
    window._ui.status = {
      can_run:            !!r.can_run,
      plex_connected:     !!(providers.PLEX?.connected     ?? providers.PLEX?.ok),
      simkl_connected:    !!(providers.SIMKL?.connected    ?? providers.SIMKL?.ok),
      trakt_connected:    !!(providers.TRAKT?.connected    ?? providers.TRAKT?.ok),
      anilist_connected:  !!(providers.ANILIST?.connected  ?? providers.ANILIST?.ok),
      jellyfin_connected: !!(providers.JELLYFIN?.connected ?? providers.JELLYFIN?.ok),
      emby_connected:     !!(providers.EMBY?.connected     ?? providers.EMBY?.ok),
      mdblist_connected:  !!(providers.MDBLIST?.connected  ?? providers.MDBLIST?.ok),
      tautulli_connected: !!(providers.TAUTULLI?.connected ?? providers.TAUTULLI?.ok),
    };

    if (typeof recomputeRunDisabled === "function") recomputeRunDisabled?.();

    const opsCard = document.getElementById("ops-card");
    const onMain = opsCard ? !opsCard.classList.contains("hidden") : true;
    const logPanel = document.getElementById("log-panel");
    const layout = document.getElementById("layout");
    const stats = document.getElementById("stats-card");
    const hasStatsVisible = !!(stats && !stats.classList.contains("hidden"));
    logPanel?.classList.toggle("hidden", !(appDebug && onMain));
    layout?.classList.toggle("full", onMain && !appDebug && !hasStatsVisible);
  } catch (e) {
    console.warn("refreshStatus failed", e);
  }
}


(function bootstrapStatusFromCache() {
  try {
    const cached = loadStatusCache();
    if (cached?.providers) {
      renderConnectorStatus(cached.providers, { stale: true });
    }
  } catch {}
  try { refreshPairedProviders?.(0); } catch {}
  try { refreshStatus(true); } catch {}
})();

async function manualRefreshStatus() {
  if (manualRefreshStatus._inFlight) return;
  manualRefreshStatus._inFlight = true;

  const btn = document.getElementById("btn-status-refresh");
  btn?.classList.add("spin");
  setRefreshBusy?.(true);

  try {
    await refreshPairedProviders(0);

    const cached = loadStatusCache?.();
    if (cached?.providers) {
      renderConnectorStatus(cached.providers, { stale: true });
    } else if (window._ui?.status) {
      const s = window._ui.status;
      renderConnectorStatus({
        PLEX:     { connected: !!s.plex_connected },
        SIMKL:    { connected: !!s.simkl_connected },
        TRAKT:    { connected: !!s.trakt_connected },
        ANILIST:  { connected: !!s.anilist_connected },
        JELLYFIN: { connected: !!s.jellyfin_connected },
        EMBY:     { connected: !!s.emby_connected },
        MDBLIST:  { connected: !!s.mdblist_connected },
        TAUTULLI: { connected: !!s.tautulli_connected },
      }, { stale: true });
    }

    try {
      await refreshStatus(true);
    } catch (e) {
      console.warn("Manual status refresh timed out; showing cached", e);
      const cached2 = loadStatusCache?.();
      if (cached2?.providers) renderConnectorStatus(cached2.providers, { stale: true });
      queueMicrotask(() => { try { refreshStatus(true); } catch {} });
    }
    } catch (e) {
    console.warn("Manual status refresh failed", e);
  } finally {
    setRefreshBusy?.(false);
    btn?.classList.remove("spin");
    manualRefreshStatus._inFlight = false;
  }
}

function toLocal(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  return d.toLocaleString(undefined, { hour12: false });
}

function computeRedirectURI() {
  return location.origin + "/callback";
}
function flashCopy(btn, ok, msg) {
  if (!btn) {
    if (!ok) alert(msg || "Copy failed");
    return;
  }
  const old = btn.textContent;
  btn.disabled = true;
  btn.textContent = ok ? "Copied ✓" : msg || "Copy failed";
  setTimeout(() => {
    btn.textContent = old;
    btn.disabled = false;
  }, 1200);
}


function recomputeRunDisabled() {
  const btn = document.getElementById("run");
  if (!btn) return;
  const busyNow = !!window.busy;
  const canRun = !window._ui?.status ? true : !!window._ui.status.can_run;
  const running = !!(window._ui?.summary && window._ui.summary.running);
  btn.disabled = busyNow || running || !canRun;
}


window.setTimeline = function setTimeline(tl){
  if (window.UX?.updateTimeline) window.UX.updateTimeline(tl || {});
  else window.dispatchEvent(new CustomEvent("ux:timeline", { detail: tl || {} }));
};

function setSyncHeader(status, msg) {
  const icon = document.getElementById("sync-icon");
  icon.classList.remove("sync-ok", "sync-warn", "sync-bad");
  icon.classList.add(status);
  document.getElementById("sync-status-text").textContent = msg;
}

function relTimeFromEpoch(epoch) {
  if (!epoch) return "";
  const secs = Math.max(1, Math.floor(Date.now() / 1000 - epoch));
  const units = [
    ["y", 31536000],
    ["mo", 2592000],
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ];
  for (const [label, span] of units) {
    if (secs >= span) return Math.floor(secs / span) + label + " ago";
  }

  return "just now";
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeAbout();
});


// Soft vs hard refresh helpers for Main
let __currentTab = "main";
let __softMainBusy = false;

// Force 2-col Main every time
function enforceMainLayout(){
  const layout = document.getElementById("layout");
  const stats  = document.getElementById("stats-card");
  if (!layout) return;
  layout.classList.remove("single","full");
  stats?.classList.remove("hidden");
}

async function softRefreshMain() {
  if (__softMainBusy) return;
  __softMainBusy = true;
  enforceMainLayout();
  try {
    const tasks = [
      (async () => { try { await refreshStatus(); } catch {} })(),
      (async () => { try { await refreshStats(); } catch {} })(),
      (async () => { try { window.refreshInsights?.(); } catch {} })(),
      (async () => { try { await updatePreviewVisibility?.(); } catch {} })(),
    ];
    await Promise.allSettled(tasks);
  } finally {
    __softMainBusy = false;
  }
}

async function hardRefreshMain({ layout, statsCard }) {
  enforceMainLayout();
  try { if (typeof lastStatusMs !== "undefined") lastStatusMs = 0; } catch {}
  await refreshStatus(true); 
  await refreshStats(true);
  window.refreshInsights?.(true);

  // Use the stream managed by main.js
  if (!window.esSum) window.openSummaryStream?.();
  if (!window.esLogs) window.openLogStream?.();

  window.wallLoaded = false;
  try { await updatePreviewVisibility(); } catch {}

  if (typeof window.refreshSchedulingBanner === "function") {
    window.refreshSchedulingBanner();
  } else {
    window.addEventListener("sched-banner-ready", () => { try { window.refreshSchedulingBanner?.(); } catch {} }, { once: true });
  }
}

// Tabs & Navigation
async function showTab(n) {
  document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id: n } }));

  const pageSettings  = document.getElementById("page-settings");
  const pageWatchlist = document.getElementById("page-watchlist");
  const pageSnapshots = document.getElementById("page-snapshots");
  const logPanel      = document.getElementById("log-panel");
  const layout        = document.getElementById("layout");
  const statsCard     = document.getElementById("stats-card");
  const ph            = document.getElementById("placeholder-card");

  // Tab header state
  document.getElementById("tab-main")?.classList.toggle("active", n === "main");
  document.getElementById("tab-watchlist")?.classList.toggle("active", n === "watchlist");
  document.getElementById("tab-snapshots")?.classList.toggle("active", n === "snapshots");
  document.getElementById("tab-settings")?.classList.toggle("active", n === "settings");

  // Cards visibility
  document.getElementById("ops-card")?.classList.toggle("hidden", n !== "main");
  statsCard?.classList.toggle("hidden", n !== "main");
  if (ph && n !== "main") ph.classList.add("hidden");

  // Pages
  pageWatchlist?.classList.toggle("hidden", n !== "watchlist");
  pageSnapshots?.classList.toggle("hidden", n !== "snapshots");
  pageSettings?.classList.toggle("hidden", n !== "settings");

  document.documentElement.dataset.tab = n;
  if (document.body) document.body.dataset.tab = n;

  // MAIN
  if (n === "main") {
    enforceMainLayout();
    if (__currentTab === "main") await softRefreshMain();
    else await hardRefreshMain({ layout, statsCard });
    logPanel?.classList.remove("hidden");

    queueMicrotask(() => {
      const hasPanel = document.getElementById("det-log");
      if (hasPanel && !window.esDet) { try { openDetailsLog(); } catch {} }
    });
    __currentTab = "main";
    return;
  }

  // WATCHLIST
  if (n === "watchlist") {
    layout?.classList.add("single");
    layout?.classList.remove("full");
    logPanel?.classList.add("hidden");

    try {
      const firstLoad = !window.__watchlistLoaded;
      if (firstLoad) {
        const base = new URL("/assets/js/watchlist.js", document.baseURI).href;
        const wlUrl = window.APP_VERSION ? `${base}?v=${encodeURIComponent(window.APP_VERSION)}` : base;

      const loadClassic = (src) => new Promise((resolve, reject) => {
        const s = document.createElement("script");
        s.src = src;
        s.async = true;
        s.onload = resolve;
        s.onerror = reject;
        document.head.appendChild(s);
      });

      const loadViaBlobModule = async (src) => {
        const res = await fetch(src, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status} loading ${src}`);
        const text = await res.text();

        const head = text.slice(0, 200).toLowerCase();
        if (head.includes("<!doctype") || head.includes("<html")) {
          const err = new Error("watchlist.js was served as HTML (reverse-proxy fallback?)");
          err.code = "WL_HTML_FALLBACK";
          throw err;
        }

        const blobUrl = URL.createObjectURL(new Blob([text], { type: "application/javascript" }));
        try {
          await import(/* @vite-ignore */ blobUrl);
        } finally {
          URL.revokeObjectURL(blobUrl);
        }
      };

      try {
        await import(/* @vite-ignore */ wlUrl);
      } catch (e1) {
        try {
          await loadViaBlobModule(wlUrl);
        } catch (e2) {
          await loadClassic(wlUrl);
        }
      }
        window.__watchlistLoaded = true;
      } else {
        if (window.Watchlist?.refresh) {
          await window.Watchlist.refresh();
        } else {
          window.dispatchEvent(new CustomEvent("watchlist:refresh"));
        }
      }
    } catch (e) {
      console.warn("Watchlist load/refresh failed:", e);
    }

    __currentTab = "watchlist";
    return;
  }

  // SNAPSHOTS
  if (n === "snapshots") {
    layout?.classList.add("single");
    layout?.classList.remove("full");
    logPanel?.classList.add("hidden");

    try {
      const firstLoad = !window.__snapshotsLoaded;
      const base = new URL("/assets/js/snapshots.js", document.baseURI).href;
      const ssUrl = window.APP_VERSION ? `${base}?v=${encodeURIComponent(window.APP_VERSION)}` : base;

      const loadClassic = (src) => new Promise((resolve, reject) => {
        const s = document.createElement("script");
        s.src = src;
        s.async = true;
        s.onload = resolve;
        s.onerror = reject;
        document.head.appendChild(s);
      });

      const loadViaBlobModule = async (src) => {
        const res = await fetch(src, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status} loading ${src}`);
        const text = await res.text();

        const head = text.slice(0, 200).toLowerCase();
        if (head.includes("<!doctype") || head.includes("<html")) {
          const err = new Error("snapshots.js was served as HTML (reverse-proxy fallback?)");
          err.code = "SS_HTML_FALLBACK";
          throw err;
        }

        const blobUrl = URL.createObjectURL(new Blob([text], { type: "application/javascript" }));
        try {
          await import(/* @vite-ignore */ blobUrl);
        } finally {
          URL.revokeObjectURL(blobUrl);
        }
      };

      if (firstLoad) {
        try {
          await import(/* @vite-ignore */ ssUrl);
        } catch (e1) {
          try {
            await loadViaBlobModule(ssUrl);
          } catch (e2) {
            await loadClassic(ssUrl);
          }
        }
        window.__snapshotsLoaded = true;
      } else {
        try {
          if (window.Snapshots?.refresh) await window.Snapshots.refresh(true);
        } catch {}
      }
    } catch (e) {
      console.warn("Snapshots load/refresh failed:", e);
    }

    __currentTab = "snapshots";
    return;
  }

  // SETTINGS
  if (n === "settings") {
    layout?.classList.add("single");
    layout?.classList.remove("full");
    logPanel?.classList.add("hidden");

    try { await window.mountAuthProviders?.(); } catch {}
    try { await window.loadConfig?.(); } catch {}
    try {
      if (typeof window.cwLoadAuth === "function") {
        await Promise.allSettled([window.cwLoadAuth("simkl"), window.cwLoadAuth("trakt")]);
      }
      try { window.cwAuth?.simkl?.init?.(); } catch {}
      try { window.cwAuth?.trakt?.init?.(); } catch {}
    } catch {}

    try { window.updateTmdbHint?.(); } catch {}
    try { window.updateSimklHint?.(); } catch {}
    try { window.updateSimklButtonState?.(); } catch {}
    try { window.updateTraktHint?.(); } catch {}
    try { window.startTraktTokenPoll?.(); } catch {}

    if (typeof window.loadScheduling === "function") {
      await window.loadScheduling();
    } else {
      window.addEventListener("sched-banner-ready", () => { try { window.loadScheduling?.(); } catch {} }, { once: true });
    }

    try { ensureScrobbler(); setTimeout(ensureScrobbler, 200); } catch {}
    __currentTab = "settings";
    return;
  }

  __currentTab = n || "main";
}

document.addEventListener("tab-changed", (e) => {
  const id = String(e?.detail?.id || "").toLowerCase();
  if (id !== "main") return;
  enforceMainLayout();
  setTimeout(() => {
    try { window.openSummaryStream?.(); } catch {}
    try { window.openLogStream?.(); } catch {}
    try { window.UX?.refresh?.(); } catch {}
    try { recomputeRunDisabled?.(); } catch {}
  }, 0);
});


// Scrobbler loader
let __scrobInit = false;
function ensureScrobbler() {
  if (__scrobInit) return;

  const mount = document.getElementById("scrobble-mount") || document.getElementById("scrobbler");
  if (!mount) return;

  const prov = (typeof getConfiguredProviders === "function") ? getConfiguredProviders() : new Set();
  const srcOk = prov.has("PLEX") || prov.has("EMBY") || prov.has("JELLYFIN");
  const sinkOk = prov.has("TRAKT") || prov.has("SIMKL") || prov.has("MDBLIST");
  if (!(srcOk && sinkOk)) return;

  const start = () => {
    if (__scrobInit) return;
    if (window.Scrobbler?.init) {
      window.Scrobbler.init({ mountId: mount.id });
    } else if (window.Scrobbler?.mount) {
      window.Scrobbler.mount(mount, window._cfgCache || {});
    } else {
      return;
    }
    __scrobInit = true;
  };

  if (window.Scrobbler) { start(); return; }
  let s = document.getElementById("scrobbler-js");
  if (!s) {
    s = document.createElement("script");
    s.id = "scrobbler-js";
    s.src = "/assets/js/scrobbler.js";
    s.defer = true;
    s.onload = start;
    s.onerror = () => console.warn("[scrobbler] script failed to load");
    document.head.appendChild(s);
  } else {
    s.onload = start;
  }
}

// Run and headers
function toggleSection(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle("open");
}
function setBusy(v) {
  busy = v;
  window.busy = v;
  recomputeRunDisabled();
}

// Run Sync
async function runSync(){
  if (busy) return;
  setBusy?.(true);

  const undoOptimisticSyncUI = () => {
    try{ window.SyncBar?.reset?.(); }catch{}
    try{
      const btn = document.getElementById("run");
      if (btn){
        btn.removeAttribute("disabled");
        btn.setAttribute("aria-busy", "false");
        btn.classList.remove("glass");
        btn.title = "Run synchronization";
      }
    }catch{}
  };

  try{ window.UX?.updateTimeline({ start:true, pre:false, post:false, done:false }); window.UX?.updateProgress({ pct:0 }); }catch{}

  try {
    const detLog = document.getElementById("det-log");
    if (detLog) detLog.textContent = "";
    try { window.esDet?.close(); } catch {}
    window.esDet = null;
  } catch {}

  try { typeof openDetailsLog === "function" && openDetailsLog(); } catch {}

  try{
    const resp = await fetch("/api/run", { method:"POST" });
    let j = null; try{ j = await resp.json(); }catch{}
    if (!resp.ok || !j || j.ok !== true){
      typeof setSyncHeader === "function" && setSyncHeader("sync-bad", `Failed to start${j?.error ? ` – ${j.error}` : ""}`);
      undoOptimisticSyncUI();
      try{ window.UX?.updateTimeline({ start:false, pre:false, post:false, done:false }); }catch{}
      return;
    }
    if (j?.skipped){
      const msg = (j.skipped === "no_pairs_configured")
        ? "No pairs configured — skipping sync"
        : `Sync skipped — ${j.skipped}`;
      typeof setSyncHeader === "function" && setSyncHeader("sync-warn", msg);
      undoOptimisticSyncUI();
      try{ window.UX?.updateTimeline({ start:false, pre:false, post:false, done:false }); }catch{}
      return;
    }
  }catch(_){
    typeof setSyncHeader === "function" && setSyncHeader("sync-bad", "Failed to reach server");
    undoOptimisticSyncUI();
    try{ window.UX?.updateTimeline({ start:false, pre:false, post:false, done:false }); }catch{}
  }finally{
    setBusy?.(false);
    typeof recomputeRunDisabled === "function" && recomputeRunDisabled();
    if (AUTO_STATUS) try{ refreshStatus(false); }catch{}
  }
}

const UPDATE_CHECK_INTERVAL_MS = 12 * 60 * 60 * 1000;
let _updInfo = null;


function setStatsExpanded(expanded) {
  const sc = document.getElementById("stats-card");
  if (!sc) return;
  sc.classList.toggle("collapsed", !expanded);
  sc.classList.toggle("expanded", !!expanded);
  if (expanded) {
    try {
      refreshInsights();
    } catch (e) {}
  }
}

function isElementOpen(el) {
  if (!el) return false;
  const c = el.classList || {};
  if (c.contains?.("open") || c.contains?.("expanded") || c.contains?.("show"))
    return true;
  const style = window.getComputedStyle(el);
  return !(
    style.display === "none" ||
    style.visibility === "hidden" ||
    el.offsetHeight === 0
  );
}

function findDetailsButton() {
  
  return (
    document.getElementById("btn-details") ||
    document.querySelector('[data-action="details"], .btn-details') ||
    Array.from(document.querySelectorAll("button")).find(
      (b) => (b.textContent || "").trim().toLowerCase() === "view details"
    )
  );
}

function findDetailsPanel() {
  
  return (
    document.getElementById("sync-output") ||
    document.getElementById("details") ||
    document.querySelector('#sync-log, .sync-output, [data-pane="details"]')
  );
}

function wireDetailsToStats() {
  const btn = findDetailsButton();
  const panel = findDetailsPanel();

  
  setStatsExpanded(isElementOpen(panel));
  if (btn) {
    btn.addEventListener("click", () => {
      
      setTimeout(() => setStatsExpanded(isElementOpen(panel)), 50);
    });
  }

  
  const syncBtn =
    document.getElementById("btn-sync") ||
    document.querySelector('[data-action="sync"], .btn-sync');
  if (syncBtn) {
    syncBtn.addEventListener("click", () => setStatsExpanded(false));
  }
}

document.addEventListener("DOMContentLoaded", wireDetailsToStats);
document.addEventListener("DOMContentLoaded", () => {
  try {
    scheduleInsights();
  } catch (_) {}
});

async function fetchJSON(){ if (window.Insights && window.Insights.fetchJSON) return window.Insights.fetchJSON.apply(this, arguments); return null; }

function scheduleInsights(){ if (window.Insights && window.Insights.scheduleInsights) return window.Insights.scheduleInsights.apply(this, arguments); }

// Insights refresh
async function refreshInsights(){ if (window.Insights && window.Insights.refreshInsights) return window.Insights.refreshInsights.apply(this, arguments); }

function renderSparkline(){ if (window.Insights && window.Insights.renderSparkline) return window.Insights.renderSparkline.apply(this, arguments); }
document.addEventListener("DOMContentLoaded", refreshInsights);

// Update check logic
(() => {
  const INTERVAL =
    typeof UPDATE_CHECK_INTERVAL_MS === "number"
      ? UPDATE_CHECK_INTERVAL_MS
      : 60 * 60 * 1000;

  // Prevent duplicate init if script loads twice
  if (window.__cwUpdateInitDone) return;
  window.__cwUpdateInitDone = true;

  const run = () => { try { checkForUpdate(); } catch (e) { console.debug("checkForUpdate failed:", e); } };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run, { once: true });
  } else {
    run();
  }

  // Periodic checks
  setInterval(run, INTERVAL);
})();


// Find the actions row and insert the pill right after it
function ensureMainUpdateSlot() {
  let slot = document.getElementById('st-main-update');
  if (slot) return slot;

  const syncBtn = [...document.querySelectorAll('button')].find(b => /synchroni[sz]e/i.test(b.textContent || ''));
  const actionsRow = syncBtn
    ? (syncBtn.closest('.sync-actions, .cx-sync-actions, .actions, .row, .toolbar') || syncBtn.parentElement)
    : (document.querySelector('.sync-actions, .cx-sync-actions, .actions, .row, .toolbar'));

  if (actionsRow && actionsRow.parentElement) {
    slot = document.createElement('div');
    slot.id = 'st-main-update';
    slot.className = 'hidden';
    actionsRow.insertAdjacentElement('afterend', slot);
    return slot;
  }

  // Fallbacks: 
  const previewHeader = [...document.querySelectorAll('h2, .section-title')].find(h => /watchlist\s*preview/i.test(h.textContent || ''));
  if (previewHeader && previewHeader.parentElement) {
    slot = document.createElement('div');
    slot.id = 'st-main-update';
    slot.className = 'hidden';
    previewHeader.insertAdjacentElement('beforebegin', slot);
    return slot;
  }

  const main = document.querySelector('#tab-main, [data-tab="main"], .page-main, main') || document.body;
  slot = document.createElement('div');
  slot.id = 'st-main-update';
  slot.className = 'hidden';
  main.insertBefore(slot, main.firstChild);
  return slot;
}

// Render pill content
function renderMainUpdatePill(hasUpdate, latest, url) {
  const host = ensureMainUpdateSlot();
  if (!host) return;

  if (hasUpdate && latest) {
    host.innerHTML = `
      <div class="pill">
        <span class="dot" aria-hidden="true"></span>
        <span>Update <strong>${latest}</strong> available · <a href="${url}" target="_blank" rel="noopener">Release notes</a></span>
      </div>`;
    host.classList.remove('hidden');
  } else {
    host.classList.add('hidden');
    host.textContent = '';
  }
}

// Hook into existing version check
async function checkForUpdate() {
  try {
    const r = await fetch('/api/version', { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();

    const cur = String(j.current ?? '0.0.0').trim();
    const latest = j.latest ? String(j.latest).trim() : null;
    const url = j.html_url || 'https://github.com/cenodude/CrossWatch/releases';
    const hasUpdate = !!j.update_available;

    // Header badge
    const vEl = document.getElementById('app-version');
    if (vEl) vEl.textContent = `Version ${cur}`;
    const updEl = document.getElementById('st-update');
    if (updEl) {
      if (hasUpdate && latest) {
        const changed = latest !== (updEl.dataset.lastLatest || '');
        updEl.classList.add('badge', 'upd');
        updEl.innerHTML = `<a href="${url}" target="_blank" rel="noopener" title="Open release page">Update ${latest} available</a>`;
        updEl.classList.remove('hidden');
        if (changed) {
          updEl.dataset.lastLatest = latest;
          updEl.classList.remove('reveal');
          void updEl.offsetWidth;
          updEl.classList.add('reveal');
        }
      } else {
        updEl.classList.add('hidden');
        updEl.classList.remove('reveal');
        updEl.textContent = '';
        updEl.removeAttribute('aria-label');
        delete updEl.dataset.lastLatest;
      }
    }

    // Main pill
    renderMainUpdatePill(hasUpdate, latest, url);
  } catch (err) {
    console.debug('Version check failed:', err);
  }
}

function renderSummary(sum) {
  currentSummary = sum;
  window._ui = window._ui || {};
  window._ui.summary = sum;

  const pp = sum.plex_post ?? sum.plex_pre;
  const sp = sum.simkl_post ?? sum.simkl_pre;

  // chips
  document.getElementById("chip-plex").textContent = pp ?? "–";
  document.getElementById("chip-simkl").textContent = sp ?? "–";
  document.getElementById("chip-dur").textContent =
    sum.duration_sec != null ? sum.duration_sec + "s" : "–";
  document.getElementById("chip-exit").textContent =
    sum.exit_code != null ? String(sum.exit_code) : "–";

  // headline
  if (sum.running) {
    setSyncHeader("sync-warn", "Running…");
  } else if (sum.exit_code === 0) {
    setSyncHeader(
      "sync-ok",
      (sum.result || "").toUpperCase() === "EQUAL" ? "In sync " : "Synced "
    );
  } else if (sum.exit_code != null) {
    setSyncHeader("sync-bad", "Attention needed ⚠️");
  } else {
    setSyncHeader("sync-warn", "Idle — run a sync to see results");
  }

  // details
  document.getElementById("det-cmd").textContent = sum.cmd || "–";
  document.getElementById("det-ver").textContent = sum.version || "–";
  document.getElementById("det-start").textContent  = toLocal(sum.started_at);
  document.getElementById("det-finish").textContent = toLocal(sum.finished_at);

}

(() => {
  const prev = window.renderSummary;
  window.renderSummary = function (sum) {
    try { prev?.(sum); } catch {}
    try { refreshStats(false); } catch {}
  };
})();

let _lastStatsFetch = 0;

function _ease(t) {
  return t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
}

function animateNumber(el, to) {
  const from = parseInt(el.dataset.v || "0", 10) || 0;
  if (from === to) {
    el.textContent = String(to);
    el.dataset.v = String(to);
    return;
  }
  const dur = 600, t0 = performance.now();
  function step(now) {
    const p = Math.min(1, (now - t0) / dur);
    const v = Math.round(from + (to - from) * _ease(p));
    el.textContent = String(v);
    if (p < 1) requestAnimationFrame(step);
    else el.dataset.v = String(to);
  }
  requestAnimationFrame(step);
}

function animateChart(now, week, month) {
  const bars = {
    now: document.querySelector(".bar.now"),
    week: document.querySelector(".bar.week"),
    month: document.querySelector(".bar.month"),
  };
  const max = Math.max(1, now, week, month);
  const h = (v) => Math.max(0.04, v / max);
  if (bars.week)  bars.week.style.transform  = `scaleY(${h(week)})`;
  if (bars.month) bars.month.style.transform = `scaleY(${h(month)})`;
  if (bars.now)   bars.now.style.transform   = `scaleY(${h(now)})`;
}


// Statistics dashboard refresh
async function refreshStats(force = false) {
  const nowT = Date.now();

  if (!force && nowT - _lastStatsFetch < 900) return;
  _lastStatsFetch = nowT;

  try {
    const j = await fetch("/api/stats", { cache: "no-store" }).then((r) =>
      r.json()
    );

    if (!j?.ok) return;
    const elNow = document.getElementById("stat-now");
    const elW = document.getElementById("stat-week");
    const elM = document.getElementById("stat-month");

    if (!elNow || !elW || !elM) return;
    const n = j.now | 0,
      w = j.week | 0,
      m = j.month | 0;

    animateNumber(elNow, n);
    animateNumber(elW, w);
    animateNumber(elM, m);

    
    const max = Math.max(1, n, w, m);
    const fill = document.getElementById("stat-fill");

    if (fill) fill.style.width = Math.round((n / max) * 100) + "%";

    
    const bumpOne = (delta, label) => {
      const t = document.getElementById("trend-week");
      if (!t) return;

      const cls = delta > 0 ? "up" : delta < 0 ? "down" : "flat";
      t.className = "chip trend " + cls;
      t.textContent =
        delta === 0
          ? "no change"
          : `${delta > 0 ? "+" : ""}${delta} vs ${label}`;

      if (cls === "up") {
        const c = document.getElementById("stats-card");
        c?.classList.remove("celebrate");
        void c?.offsetWidth;
        c?.classList.add("celebrate");
      }
    };

    bumpOne(n - w, "last week"); 

    
    const by = j.by_source || {};
    const totalAdd = Number.isFinite(j.added) ? j.added : null; 
    const totalRem = Number.isFinite(j.removed) ? j.removed : null;
    const lastAdd = Number.isFinite(j.new) ? j.new : null; 
    const lastRem = Number.isFinite(j.del) ? j.del : null;

    
    const setTxt = (id, val) => {
      const el = document.getElementById(id);

      if (el) el.textContent = String(val ?? 0);
    };

    setTxt("stat-added", totalAdd);
    setTxt("stat-removed", totalRem);

    
    const setTile = (tileId, numId, val) => {
      const t = document.getElementById(tileId),
        nEl = document.getElementById(numId);

      if (!t || !nEl) return;
      if (val == null) {
        t.hidden = true;
        return;
      }

      nEl.textContent = String(val);
      t.hidden = false;
    };

    setTile("tile-new", "stat-new", lastAdd);
    setTile("tile-del", "stat-del", lastRem);

    const plexVal = Number.isFinite(by.plex_total)
      ? by.plex_total
      : (by.plex ?? 0) + (by.both ?? 0);

    const simklVal = Number.isFinite(by.simkl_total)
      ? by.simkl_total
      : (by.simkl ?? 0) + (by.both ?? 0);

    const traktVal = Number.isFinite(by.trakt_total)
      ? by.trakt_total
      : (by.trakt ?? 0) + (by.both ?? 0);

    const elP = document.getElementById("stat-plex");
    const elS = document.getElementById("stat-simkl");
    const elT = document.getElementById("stat-trakt");

    const curP = Number(elP?.textContent || 0);
    const curS = Number(elS?.textContent || 0);
    const curT = Number(elT?.textContent || 0);

    const pop = (el) => {
      if (!el) return;
      el.classList.remove("bump");
      void el.offsetWidth;
      el.classList.add("bump");
    };

    if (elP) {
      if (plexVal !== curP) {
        animateNumber(elP, plexVal);
        pop(elP);
      } else {
        elP.textContent = String(plexVal);
      }
    }

    if (elS) {
      if (simklVal !== curS) {
        animateNumber(elS, simklVal);
        pop(elS);
      } else {
        elS.textContent = String(simklVal);
      }
    }

    if (elT) {
      if (traktVal !== curT) {
        animateNumber(elT, traktVal);
        pop(elT);
      } else {
        elT.textContent = String(traktVal);
      }
    }

    
    document.getElementById("tile-plex")?.removeAttribute("hidden");
    document.getElementById("tile-simkl")?.removeAttribute("hidden");
    document.getElementById("tile-trakt")?.removeAttribute("hidden");
  } catch (_) {}
}

function _setBarValues(n, w, m) {
  const bw = document.querySelector(".bar.week");
  const bm = document.querySelector(".bar.month");
  const bn = document.querySelector(".bar.now");

  if (bw) bw.dataset.v = String(w);
  if (bm) bm.dataset.v = String(m);
  if (bn) bn.dataset.v = String(n);
}

function _initStatsTooltip() {
  const chart = document.getElementById("stats-chart");
  const tip = document.getElementById("stats-tip");

  if (!chart || !tip) return;

  const map = [
    { el: document.querySelector(".bar.week"), label: "Last Week" },
    { el: document.querySelector(".bar.month"), label: "Last Month" },
    { el: document.querySelector(".bar.now"), label: "Now" },
  ];

  function show(e, label, value) {
    tip.textContent = `${label}: ${value} items`;
    tip.style.left = e.offsetX + "px";
    tip.style.top = e.offsetY + "px";
    tip.classList.add("show");
    tip.hidden = false;
  }

  function hide() {
    tip.classList.remove("show");
    tip.hidden = true;
  }

  map.forEach(({ el, label }) => {
    if (!el) return;

    el.addEventListener("mousemove", (ev) => {
      const rect = chart.getBoundingClientRect();

      const x = ev.clientX - rect.left,
        y = ev.clientY - rect.top;

      show({ offsetX: x, offsetY: y }, label, el.dataset.v || "0");
    });

    el.addEventListener("mouseleave", hide);

    el.addEventListener(
      "touchstart",
      (ev) => {
        const t = ev.touches[0];
        const rect = chart.getBoundingClientRect();

        show(
          { offsetX: t.clientX - rect.left, offsetY: t.clientY - rect.top },
          label,
          el.dataset.v || "0"
        );
      },
      { passive: true }
    );

    el.addEventListener(
      "touchend",
      () => {
        tip.classList.remove("show");
      },
      { passive: true }
    );
  });
}

document.addEventListener("DOMContentLoaded", _initStatsTooltip);


// Details Log (live stream)
if (typeof window.esDet === "undefined") window.esDet = null;
if (typeof window.esDetSummary === "undefined") window.esDetSummary = null;
if (typeof window._detStaleIV === "undefined") window._detStaleIV = null;
if (typeof window._detRetryTO === "undefined") window._detRetryTO = null;
if (typeof window._detVisibilityHandler === "undefined") window._detVisibilityHandler = null;
if (typeof window.detStickBottom === "undefined") window.detStickBottom = true;
if (typeof window.esWatch === "undefined") window.esWatch = null;
if (typeof window._watchRetryTO === "undefined") window._watchRetryTO = null;
if (typeof window._watchStaleIV === "undefined") window._watchStaleIV = null;
if (typeof window._watchVisibilityHandler === "undefined") window._watchVisibilityHandler = null;
if (typeof window._watchFlushIV === "undefined") window._watchFlushIV = null;
if (typeof window.watchStickBottom === "undefined") window.watchStickBottom = true;
if (typeof window.watchBuf === "undefined") window.watchBuf = [];
if (typeof window._detailsTabsWired === "undefined") window._detailsTabsWired = false;
if (typeof window._detailsTab === "undefined") window._detailsTab = "sync";
if (typeof window.DETAILS_MAX_LINES === "undefined") window.DETAILS_MAX_LINES = 2500;

function _activeDetailsLogEl() {
  return window._detailsTab === "watcher"
    ? document.getElementById("det-watch-log")
    : document.getElementById("det-log");
}

function _pruneDetailsLog(el) {
  const max = Number(window.DETAILS_MAX_LINES || 0) || 2500;
  while (el && el.childNodes && el.childNodes.length > max) el.removeChild(el.firstChild);
}

function setDetailsTab(tab) {
  const t = (tab === "watcher") ? "watcher" : "sync";
  window._detailsTab = t;

  const syncPanel  = document.getElementById("det-panel-sync");
  const watchPanel = document.getElementById("det-panel-watcher");
  const tabSync    = document.getElementById("det-tab-sync");
  const tabWatch   = document.getElementById("det-tab-watcher");
  if (!syncPanel || !watchPanel || !tabSync || !tabWatch) return;

  const isWatch = t === "watcher";
  syncPanel.classList.toggle("hidden", isWatch);
  watchPanel.classList.toggle("hidden", !isWatch);

  tabSync.classList.toggle("active", !isWatch);
  tabSync.setAttribute("aria-selected", String(!isWatch));
  tabWatch.classList.toggle("active", isWatch);
  tabWatch.setAttribute("aria-selected", String(isWatch));

  if (isWatch) { try { openWatcherLog(); } catch {} }
}

function initDetailsTabs() {
  if (window._detailsTabsWired) return;
  const tabSync  = document.getElementById("det-tab-sync");
  const tabWatch = document.getElementById("det-tab-watcher");
  if (!tabSync || !tabWatch) return;
  window._detailsTabsWired = true;

  tabSync.addEventListener("click", () => setDetailsTab("sync"));
  tabWatch.addEventListener("click", () => setDetailsTab("watcher"));

  const btnClear = document.getElementById("det-clear");
  if (btnClear) {
    btnClear.addEventListener("click", () => {
      const el = _activeDetailsLogEl();
      if (el) el.innerHTML = "";
      if (window._detailsTab === "watcher") window.watchBuf.length = 0;
    });
  }

  const btnFollow = document.getElementById("det-follow");
  if (btnFollow) {
    btnFollow.addEventListener("click", () => {
      if (window._detailsTab === "watcher") {
        window.watchStickBottom = !window.watchStickBottom;
        const el = document.getElementById("det-watch-log");
        if (window.watchStickBottom && el) el.scrollTop = el.scrollHeight;
      } else {
        window.detStickBottom = !window.detStickBottom;
        const el = document.getElementById("det-log");
        if (window.detStickBottom && el) el.scrollTop = el.scrollHeight;
      }
    });
  }
}

function closeWatcherLog() {
  try { window.esWatch?.close?.(); } catch {}
  window.esWatch = null;
  if (window._watchRetryTO) { clearTimeout(window._watchRetryTO); window._watchRetryTO = null; }
  if (window._watchStaleIV) { clearInterval(window._watchStaleIV); window._watchStaleIV = null; }
  if (window._watchFlushIV) { clearInterval(window._watchFlushIV); window._watchFlushIV = null; }
  if (window._watchVisibilityHandler) {
    document.removeEventListener("visibilitychange", window._watchVisibilityHandler);
    window._watchVisibilityHandler = null;
  }
  const tabWatch = document.getElementById("det-tab-watcher");
  tabWatch?.classList.remove("connected", "stale");
}

async function openWatcherLog() {
  const el = document.getElementById("det-watch-log");
  const details = document.getElementById("details");
  const tabWatch = document.getElementById("det-tab-watcher");
  if (!el || !details || details.classList.contains("hidden")) return;
  if (window.esWatch || window._watchOpening) return;
  window._watchOpening = true;

  try {
    let cfg = window._cfgCache;
    if (!cfg) {
      try {
        cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
        window._cfgCache = cfg;
      } catch {}
    }

    const sc = (cfg && typeof cfg === "object") ? (cfg.scrobble || {}) : {};
    let watchCfg = (sc && typeof sc === "object" && sc.watch && typeof sc.watch === "object") ? sc.watch : null;
    if (!watchCfg && cfg && typeof cfg.watch === "object") watchCfg = cfg.watch;
    watchCfg = watchCfg || {};

    const norm = (t) => {
      const s = String(t || "").trim().toUpperCase();
      if (!s) return "";
      if (s === "JFIN" || s === "JELLY") return "JELLYFIN";
      return s;
    };

    const provider = norm(watchCfg.provider || "plex");
    const sinksRaw = String(watchCfg.sink || "trakt");
    const sinks = sinksRaw.split(/[,&+]/g).map(norm).filter(Boolean);
    const tags = [provider, ...sinks].filter(Boolean);
    const uniq = [];
    for (const t of tags) if (!uniq.includes(t)) uniq.push(t);

    const url = new URL("/api/logs/watcher", document.baseURI);
    url.searchParams.set("tail", "200");
    if (uniq.length) url.searchParams.set("tags", uniq.join(","));

    if (!el.__cwScrollWired) {
      el.addEventListener("scroll", () => {
        const pad = 12;
        window.watchStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
      }, { passive: true });
      el.__cwScrollWired = true;
    }

    window.watchBuf.length = 0;
    el.innerHTML = "";
    window.watchStickBottom = true;

    const MAX_LINES = Number(window.DETAILS_MAX_LINES || 0) || 2500;
    let lastMsgAt = Date.now();

    const enqueue = (tag, html) => {
      if (!html) return;
      window.watchBuf.push({ tag, html });
      lastMsgAt = Date.now();
    };

    const flush = () => {
      if (!window.watchBuf.length) return;
      const frag = document.createDocumentFragment();
      const items = window.watchBuf.splice(0, window.watchBuf.length);
      for (const it of items) {
        const row = document.createElement("div");
        row.className = "wlog-line";

        const badge = document.createElement("span");
        badge.className = "wlog-tag";
        badge.textContent = it.tag;

        const msg = document.createElement("span");
        msg.className = "wlog-msg";
        msg.innerHTML = it.html;

        row.appendChild(badge);
        row.appendChild(msg);
        frag.appendChild(row);
      }
      el.appendChild(frag);

      while (el.childNodes.length > MAX_LINES) el.removeChild(el.firstChild);
      if (window.watchStickBottom) el.scrollTop = el.scrollHeight;
    };

    if (window._watchFlushIV) clearInterval(window._watchFlushIV);
    window._watchFlushIV = setInterval(flush, 120);

    const es = new EventSource(url.toString());
    window.esWatch = es;
    tabWatch?.classList.add("connected");
    tabWatch?.classList.remove("stale");

    for (const t of (uniq.length ? uniq : ["PLEX","JELLYFIN","EMBY","TRAKT","SIMKL","TMDB","MDBLIST","TRBL"])) {
      es.addEventListener(t, (ev) => enqueue(t, ev?.data));
    }

    es.addEventListener("ping", () => { lastMsgAt = Date.now(); });

    es.onerror = () => {
      tabWatch?.classList.remove("connected");
      try { window.esWatch?.close?.(); } catch {}
      window.esWatch = null;

      if (window._watchRetryTO) clearTimeout(window._watchRetryTO);
      window._watchRetryTO = setTimeout(() => {
        if (window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
      }, 1200);
    };

    if (window._watchStaleIV) clearInterval(window._watchStaleIV);
    window._watchStaleIV = setInterval(() => {
      const stale = (Date.now() - lastMsgAt) > 20000;
      tabWatch?.classList.toggle("stale", stale);
    }, 1000);

    if (window._watchVisibilityHandler) {
      document.removeEventListener("visibilitychange", window._watchVisibilityHandler);
    }
    window._watchVisibilityHandler = () => {
      if (document.visibilityState !== "visible") return;
      if (window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
    };
    document.addEventListener("visibilitychange", window._watchVisibilityHandler);
  } finally {
    window._watchOpening = false;
  }
}

async function openDetailsLog() {
  const el = document.getElementById("det-log");
  const slider = document.getElementById("det-scrub");
  if (!el) return;
  const tabSync = document.getElementById("det-tab-sync");
  try { initDetailsTabs(); } catch {}

  try {
    if (typeof window.appDebug === "undefined") {
      const cfg = window._cfgCache || await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
      window._cfgCache = cfg;
      window.appDebug = !!(cfg?.runtime?.debug || cfg?.runtime?.debug_mods);
    }
  } catch (_) {}

  el.innerHTML = "";
  el.classList?.add("cf-log");
  window.detStickBottom = true;

  try { window.esDet?.close(); } catch {}
  try { window.esDetSummary?.close(); } catch {}
  window.esDet = null;
  window.esDetSummary = null;
  if (window._detStaleIV) { clearInterval(window._detStaleIV); window._detStaleIV = null; }
  if (window._detRetryTO) { clearTimeout(window._detRetryTO); window._detRetryTO = null; }
  if (window._detVisibilityHandler) { document.removeEventListener("visibilitychange", window._detVisibilityHandler); window._detVisibilityHandler = null; }

  const updateSlider = () => {
    if (!slider) return;
    const max = el.scrollHeight - el.clientHeight;
    slider.value = max <= 0 ? 100 : Math.round((el.scrollTop / max) * 100);
  };

  const updateStick = () => {
    const pad = 6;
    window.detStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
  };

  el.addEventListener("scroll", () => { updateSlider(); updateStick(); }, { passive: true });

  if (slider) {
    slider.addEventListener("input", () => {
      const max = el.scrollHeight - el.clientHeight;
      el.scrollTop = Math.round((slider.value / 100) * max);
      window.detStickBottom = slider.value >= 99;
    });
  }

  const CF = window.ClientFormatter;
  const useFormatter = !window.appDebug && CF && CF.processChunk && CF.renderInto;

  const appendRaw = (s) => {
    const lines = String(s).replace(/\r\n/g, "\n").split("\n");
    for (const line of lines) {
      if (!line) continue;
      const div = document.createElement("div");
      div.className = "cf-line";
      div.textContent = line;
      el.appendChild(div);
    }
  };

  let detBuf = "";
  let lastMsgAt = Date.now();
  let retryMs = 1000;
  const STALE_MS = 20000;

  const connect = () => {
    try { window.esDet?.close(); } catch (_) {}
    window.esDet = new EventSource("/api/logs/stream?tag=SYNC");
    window.esDet.onopen = () => { tabSync?.classList.add("connected"); tabSync?.classList.remove("stale"); };

    window.esDet.onmessage = (ev) => {
      lastMsgAt = Date.now();
      tabSync?.classList.add("connected");
      tabSync?.classList.remove("stale");
      if (!ev?.data) return;

      if (ev.data === "::CLEAR::") {
        el.textContent = "";
        detBuf = "";
        updateSlider();
        return;
      }

      if (!useFormatter) {
        appendRaw(ev.data);
      } else {
        const { tokens, buf } = CF.processChunk(detBuf, ev.data);
        detBuf = buf;
        for (const tok of tokens) CF.renderInto(el, tok, false);
      }
      _pruneDetailsLog(el);
      if (window.detStickBottom) el.scrollTop = el.scrollHeight;
      updateSlider();
      retryMs = 1000;
    };

    window.esDet.onerror = () => {
        tabSync?.classList.remove("connected");
        tabSync?.classList.add("stale");
      try { window.esDet?.close(); } catch (_) {}
      window.esDet = null;

      if (useFormatter && detBuf && detBuf.trim()) {
        const { tokens } = CF.processChunk("", detBuf);
        detBuf = "";
        for (const tok of tokens) CF.renderInto(el, tok, false);
        if (window.detStickBottom) el.scrollTop = el.scrollHeight;
        updateSlider();
      }

      if (!window._detRetryTO) {
        window._detRetryTO = setTimeout(() => {
          window._detRetryTO = null;
          connect();
        }, retryMs);
        retryMs = Math.min(retryMs * 2, 15000);
      }
    };
  };

  connect();

  window._detStaleIV = setInterval(() => {
    tabSync?.classList.toggle("stale", (Date.now() - lastMsgAt) > STALE_MS);
    if (!window.esDet) return;
    if (document.visibilityState !== "visible") return;
    if (Date.now() - lastMsgAt > STALE_MS) {
      try { window.esDet.close(); } catch (_) {}
      window.esDet = null;
      connect();
    }
  }, STALE_MS);

  window._detVisibilityHandler = () => {
    if (document.visibilityState !== "visible") return;
    if (!window.esDet || (Date.now() - lastMsgAt > STALE_MS)) connect();
  };
  document.addEventListener("visibilitychange", window._detVisibilityHandler);

  if (!window.appDebug) {
    try { window.esDetSummary?.close(); } catch (_) {}
    window.esDetSummary = new EventSource("/api/run/summary/stream");
    window.esDetSummary.onmessage = (ev) => {
      try {
        if (!ev?.data) return;
        const obj = JSON.parse(ev.data);
        if (!obj || obj.event === "debug") return;
        const line = JSON.stringify(obj) + "\n";
        if (useFormatter) {
          const { tokens } = CF.processChunk("", line);
          for (const tok of tokens) CF.renderInto(el, tok, false);
        } else {
          appendRaw(line);
        }
        _pruneDetailsLog(el);
        if (window.detStickBottom) el.scrollTop = el.scrollHeight;
        updateSlider();
      } catch (_) {}
    };
    window.esDetSummary.onerror = () => {
      try { window.esDetSummary?.close(); } catch (_) {}
      window.esDetSummary = null;
    };
  }

  requestAnimationFrame(() => {
    el.scrollTop = el.scrollHeight;
    updateSlider();
  });
}

function closeDetailsLog() {
  try { window.esDet?.close(); } catch (_) {}
  try { window.esDetSummary?.close(); } catch (_) {}
  window.esDet = null;
  window.esDetSummary = null;
  if (window._detStaleIV) { clearInterval(window._detStaleIV); window._detStaleIV = null; }
  if (window._detRetryTO) { clearTimeout(window._detRetryTO); window._detRetryTO = null; }
  if (window._detVisibilityHandler) { document.removeEventListener("visibilitychange", window._detVisibilityHandler); window._detVisibilityHandler = null; }
  const tabSync = document.getElementById("det-tab-sync");
  tabSync?.classList.remove("connected", "stale");
  const tabWatch = document.getElementById("det-tab-watcher");
  tabWatch?.classList.remove("connected", "stale");
  try { closeWatcherLog(); } catch {}
}

function toggleDetails() {
  const d = document.getElementById("details");
  d.classList.toggle("hidden");
  if (!d.classList.contains("hidden")) {
    try { initDetailsTabs(); } catch {}
    try { setDetailsTab(window._detailsTab || "sync"); } catch {}
    openDetailsLog();
    if (window._detailsTab === "watcher") { try { openWatcherLog(); } catch {} }
  } else {
    closeDetailsLog();
    closeWatcherLog();
  }
}

window.addEventListener("beforeunload", () => {
  try { closeDetailsLog(); } catch {}
  try { closeWatcherLog(); } catch {}
});

function downloadSummary() {
  window.open("/api/run/summary/file", "_blank");
}

function setRefreshBusy(busy) {
  const btn = document.getElementById("btn-status-refresh");

  if (!btn) return;
  btn.disabled = !!busy;
  btn.classList.toggle("loading", !!busy);
}



window.openAbout = () => window.ModalRegistry.open('about');
window.cxEnsureCfgModal = window.cxEnsureCfgModal || function(){};
window.wireSecretTouch = window.wireSecretTouch || function wireSecretTouch(id) {
  const el = document.getElementById(id);
  if (!el || el.__wiredTouch) return;
  el.addEventListener("input", () => {
    el.dataset.touched = "1";
    el.dataset.masked = "0";
  });
  el.__wiredTouch = true;
};


window.maskSecret = function maskSecret(elOrId ) {
  const el = typeof elOrId === "string" ? document.getElementById(elOrId) : elOrId;
  if (!el) return;
  el.dataset.masked  = "0";
  el.dataset.loaded  = "1";
  el.dataset.touched = "";
  el.dataset.clear   = "";
};

function formatCwSnapshotLabel(name) {
  if (!name || typeof name !== "string") return name || "";
  const stem = name.replace(/\.json$/,"").split("-", 1)[0];
  if (!/^\d{8}T\d{6}Z$/.test(stem)) return name;

  const year  = stem.slice(0, 4);
  const month = stem.slice(4, 6);
  const day   = stem.slice(6, 8);
  const hour  = stem.slice(9, 11);
  const min   = stem.slice(11, 13);

  return `${year}-${month}-${day} - ${hour}:${min}`;
}

async function loadCrossWatchSnapshots(cfg) {
  const cw = (cfg && cfg.crosswatch) || {};
  const desired = {
    watchlist: (cw.restore_watchlist || "latest").trim() || "latest",
    history:   (cw.restore_history   || "latest").trim() || "latest",
    ratings:   (cw.restore_ratings   || "latest").trim() || "latest",
  };

  try {
    const res = await fetch("/api/files?path=/config/.cw_provider/snapshots");
    if (!res.ok) {
      console.warn("CrossWatch snapshot list HTTP", res.status);
      return;
    }

    const files = await res.json();
    const list = (Array.isArray(files) ? files : []).filter(
      (f) => f && typeof f.name === "string" && f.name.endsWith(".json")
    );

    
    const groups = {
      watchlist: [],
      history:   [],
      ratings:   [],
    };

    for (const f of list) {
      const name = f.name;
      if (name.endsWith("-watchlist.json")) groups.watchlist.push(name);
      else if (name.endsWith("-history.json")) groups.history.push(name);
      else if (name.endsWith("-ratings.json")) groups.ratings.push(name);
    }
    Object.keys(groups).forEach((k) => groups[k].sort());

    const idMap = {
      watchlist: "cw_restore_watchlist",
      history:   "cw_restore_history",
      ratings:   "cw_restore_ratings",
    };

    for (const key of ["watchlist", "history", "ratings"]) {
      const sel = document.getElementById(idMap[key]);
      if (!sel) continue;

      const names = groups[key];
      sel.innerHTML = "";

      
      const baseOpt = document.createElement("option");
      baseOpt.value = "latest";
      baseOpt.textContent = "Latest (default)";
      sel.appendChild(baseOpt);

      for (const name of names) {
        const o = document.createElement("option");
        o.value = name;
        o.textContent = formatCwSnapshotLabel(name);
        sel.appendChild(o);
      }

      const wanted = desired[key] || "latest";
      const hasWanted = names.includes(wanted);
      sel.value = hasWanted ? wanted : "latest";
    }
  } catch (e) {
    console.warn("CrossWatch snapshot list failed", e);
  }
}

/*! Settings */


/* Settings Hub: UI / Security / CW Tracker */

const UI_SETTINGS_TAB_KEY = "cw.ui.settings.tab.v1";

function _uiDaysLeftFromEpochSeconds(epochSeconds) {
  if (!epochSeconds || !Number.isFinite(epochSeconds)) return null;
  const ms = epochSeconds * 1000;
  const diffMs = ms - Date.now();
  if (diffMs <= 0) return 0;
  return Math.ceil(diffMs / (24 * 60 * 60 * 1000));
}

function cwUiSettingsSelect(tab, opts = {}) {
  const t = String(tab || "ui").toLowerCase();
  const persist = opts.persist !== false;

  const hub = document.getElementById("ui_settings_hub");
  const panels = document.getElementById("ui_settings_panels");
  if (!hub || !panels) return;

  const tiles = hub.querySelectorAll(".cw-hub-tile");
  tiles.forEach((btn) => {
    const k = String(btn.dataset.tab || "").toLowerCase();
    btn.classList.toggle("active", k === t);
    btn.setAttribute("aria-selected", k === t ? "true" : "false");
  });

  const ps = panels.querySelectorAll(".cw-settings-panel");
  ps.forEach((p) => {
    const k = String(p.dataset.tab || "").toLowerCase();
    p.classList.toggle("active", k === t);
  });

  if (persist) {
    try { localStorage.setItem(UI_SETTINGS_TAB_KEY, t); } catch {}
  }

  try { cwUiSettingsHubUpdate(); } catch {}
}

function cwUiSettingsHubUpdate() {
  const set = (id, text) => {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  };

  const wl = document.getElementById("ui_show_watchlist_preview");
  if (wl) set("hub_ui_watchlist", `Watchlist: ${wl.value === "false" ? "Hide" : "Show"}`);

  const pc = document.getElementById("ui_show_playingcard");
  if (pc) set("hub_ui_playing", `Playing: ${pc.value === "false" ? "Hide" : "Show"}`);

  const ai = document.getElementById("ui_show_AI");
  if (ai) set("hub_ui_askai", `ASK AI: ${ai.value === "false" ? "Hide" : "Show"}`);

  const proto = document.getElementById("ui_protocol");
  if (proto) set("hub_ui_proto", `Proto: ${String(proto.value || "http").toUpperCase()}`);

  const aaEnabled = (document.getElementById("app_auth_enabled")?.value || "").toString() === "true";
  const st = window._appAuthStatus || null;

  if (!aaEnabled) {
    set("hub_sec_auth", "Auth: Off");
    set("hub_sec_session", "Session: —");
  } else if (st && st.enabled && st.configured && st.authenticated) {
    set("hub_sec_auth", "Auth: On");
    const days = _uiDaysLeftFromEpochSeconds(st.session_expires_at);
    set("hub_sec_session", days == null ? "Session: active" : `Session: ${days}d`);
  } else if (st && st.enabled && !st.configured) {
    set("hub_sec_auth", "Auth: On");
    set("hub_sec_session", "Set password");
  } else {
    set("hub_sec_auth", "Auth: On");
    set("hub_sec_session", "Locked");
  }

  const cwEnabled = (document.getElementById("cw_enabled")?.value || "").toString() !== "false";
  set("hub_cw_enabled", `Tracker: ${cwEnabled ? "On" : "Off"}`);

  const retRaw = (document.getElementById("cw_retention_days")?.value || "").toString().trim();
  const ret = retRaw === "" ? null : parseInt(retRaw, 10);
  if (ret == null || Number.isNaN(ret)) set("hub_cw_retention", "Retention: —");
  else if (ret === 0) set("hub_cw_retention", "Retention: ∞");
  else set("hub_cw_retention", `Retention: ${ret}d`);

  const authFields = document.getElementById("app_auth_fields");
  if (authFields) authFields.classList.toggle("cw-disabled", !aaEnabled);

  const trackerFields = document.getElementById("cw_restore_fields");
  if (trackerFields) trackerFields.classList.toggle("cw-disabled", !cwEnabled);
}

function cwUiSettingsHubInit() {
  if (window.__cwUiSettingsHubInit) return;
  window.__cwUiSettingsHubInit = true;

  const ids = [
    "ui_show_watchlist_preview",
    "ui_show_playingcard",
    "ui_show_AI",
    "ui_protocol",
    "app_auth_enabled",
    "app_auth_username",
    "app_auth_password",
    "app_auth_password2",
    "cw_enabled",
    "cw_retention_days",
    "cw_auto_snapshot",
    "cw_max_snapshots",
    "cw_restore_watchlist",
    "cw_restore_history",
    "cw_restore_ratings"
  ];

  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (el.__hubWired) return;
    el.addEventListener("change", () => { try { cwUiSettingsHubUpdate(); } catch {} });
    el.addEventListener("input",  () => { try { cwUiSettingsHubUpdate(); } catch {} });
    el.__hubWired = true;
  });

  let tab = "ui";
  try {
    const saved = (localStorage.getItem(UI_SETTINGS_TAB_KEY) || "").toLowerCase();
    if (["ui","security","tracker"].includes(saved)) tab = saved;
  } catch {}

  cwUiSettingsSelect(tab, { persist: false });
  try { cwUiSettingsHubUpdate(); } catch {}
}

try {
  window.cwUiSettingsSelect = cwUiSettingsSelect;
  window.cwUiSettingsHubInit = cwUiSettingsHubInit;
  window.cwUiSettingsHubUpdate = cwUiSettingsHubUpdate;
} catch {}

/* Settings Hub: Scheduling */
const SCHED_SETTINGS_TAB_KEY = "cw.ui.scheduling.tab.v1";
const SCHED_PROVIDER_OPEN_KEY = "cw.ui.scheduling.open.v1";

let __cwSchedOpen = false;

function cwSchedProviderSelect(open, opts = {}) {
  const tilesHost = document.getElementById("sched_provider_tiles");
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;

  const wantOpen = (open == null) ? !__cwSchedOpen : !!open;
  __cwSchedOpen = wantOpen;

  if (tilesHost) {
    const tile = tilesHost.querySelector('[data-provider="scheduler"]');
    if (tile) {
      tile.classList.toggle("active", wantOpen);
      tile.setAttribute("aria-selected", wantOpen ? "true" : "false");
    }
  }

  panelHost.classList.toggle("hidden", !wantOpen);

  if (opts.persist !== false) {
    try { localStorage.setItem(SCHED_PROVIDER_OPEN_KEY, wantOpen ? "1" : "0"); } catch {}
  }
}

function cwSchedSettingsSelect(tab, opts = {}) {
  const panelHost = document.getElementById("sched-provider-panel");
  const panel = panelHost?.querySelector('.cw-meta-provider-panel[data-provider="scheduler"]');
  if (!panelHost || !panel) return;

  const t = (tab || "basic").toLowerCase();
  const want = ["basic", "advanced"].includes(t) ? t : "basic";

  panel.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
    btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
  });
  panel.querySelectorAll(".cw-subpanel[data-sub]").forEach((sp) => {
    sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === want);
  });

  if (opts.persist !== false) {
    try { localStorage.setItem(SCHED_SETTINGS_TAB_KEY, want); } catch {}
  }
  try { cwSchedSettingsHubUpdate(); } catch {}
}

function cwBuildSchedulerPanel() {
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;
  if (panelHost.querySelector('.cw-meta-provider-panel[data-provider="scheduler"]')) return;

  const wrap = document.createElement("div");
  wrap.className = "cw-meta-provider-panel active";
  wrap.dataset.provider = "scheduler";

  const head = document.createElement("div");
  head.className = "cw-panel-head";
  head.innerHTML = `
    <div>
      <div class="cw-panel-title">Scheduler</div>
      <div class="muted">Run sync automatically on a timer.</div>
    </div>
  `;

  const subTiles = document.createElement("div");
  subTiles.className = "cw-subtiles";
  subTiles.innerHTML = `
    <button type="button" class="cw-subtile active" data-sub="basic">Standard</button>
    <button type="button" class="cw-subtile" data-sub="advanced">Advanced</button>
  `;

  const subPanels = document.createElement("div");
  subPanels.className = "cw-subpanels";

  const pBasic = document.createElement("div");
  pBasic.className = "cw-subpanel active";
  pBasic.dataset.sub = "basic";

  const pAdv = document.createElement("div");
  pAdv.className = "cw-subpanel";
  pAdv.dataset.sub = "advanced";

  const detach = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    try { el.parentNode?.removeChild(el); } catch {}
    return el;
  };

  const mkField = (labelText, ctrl, noteText) => {
    if (!ctrl) return null;
    const f = document.createElement("div");
    f.className = "field";
    f.innerHTML = `<div class="muted" style="margin-bottom:6px;">${labelText}</div>`;
    f.appendChild(ctrl);
    if (noteText) {
      const n = document.createElement("div");
      n.className = "auth-card-notes";
      n.textContent = noteText;
      f.appendChild(n);
    }
    return f;
  };

  const enabledEl = detach("schEnabled");
  const modeEl = detach("schMode");
  const nEl = detach("schN");
  const timeEl = detach("schTime");

  const basicCard = document.createElement("div");
  basicCard.className = "auth-card";
  const basicFields = document.createElement("div");
  basicFields.className = "auth-card-fields";

  const f1 = mkField("Enable", enabledEl);
  const f2 = mkField("Frequency", modeEl, "Choose the timer mode.");
  const f3 = mkField("Every N hours", nEl, "Only used when Frequency = Every N hours.");
  const f4 = mkField("Time", timeEl, "Only used when Frequency = Daily at…");

  [f1, f2, f3, f4].forEach((x) => x && basicFields.appendChild(x));
  if (basicFields.childNodes.length) basicCard.appendChild(basicFields);
  pBasic.appendChild(basicCard);

  const advMount = detach("sched_advanced_mount") || (() => {
    const d = document.createElement("div");
    d.id = "sched_advanced_mount";
    return d;
  })();

  pAdv.appendChild(advMount);

  subPanels.appendChild(pBasic);
  subPanels.appendChild(pAdv);

  wrap.appendChild(head);
  wrap.appendChild(subTiles);
  wrap.appendChild(subPanels);

  panelHost.appendChild(wrap);

  try {
    const raw = document.getElementById("sched-provider-raw");
    if (raw) raw.classList.add("hidden");
  } catch {}

  subTiles.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
    btn.addEventListener("click", () => cwSchedSettingsSelect(btn.dataset.sub));
  });

  let lastSub = "basic";
  try { lastSub = (localStorage.getItem(SCHED_SETTINGS_TAB_KEY) || "basic").toLowerCase(); } catch {}
  cwSchedSettingsSelect((lastSub === "advanced") ? "advanced" : "basic", { persist: false });
}

function cwSchedProviderEnsure() {
  const tilesHost = document.getElementById("sched_provider_tiles");
  const panelHost = document.getElementById("sched-provider-panel");
  if (!panelHost) return;

  if (!panelHost.dataset.__cwSchedBuilt) {
    try { cwBuildSchedulerPanel(); } catch {}
    panelHost.dataset.__cwSchedBuilt = "1";
  }

  if (tilesHost) {
    tilesHost.querySelectorAll("[data-provider]").forEach((btn) => {
      if (btn.__cwSchedWired) return;
      btn.addEventListener("click", () => {
        const isOpen = !document.getElementById("sched-provider-panel")?.classList.contains("hidden");
        cwSchedProviderSelect(!isOpen);
      });
      btn.__cwSchedWired = true;
    });

    let open = "0";
    try { open = localStorage.getItem(SCHED_PROVIDER_OPEN_KEY) || "0"; } catch {}
    cwSchedProviderSelect(open === "1", { persist: false });
  } else {
    cwSchedProviderSelect(true, { persist: false });
  }

  try { cwSchedSettingsHubUpdate(); } catch {}
}

function cwSchedSettingsHubUpdate() {
  const set = (id, text) => {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  };

  let patch = null;
  try {
    patch = (typeof window.getSchedulingPatch === "function") ? window.getSchedulingPatch() : null;
  } catch {}

  if (!patch) {
    const enabled = (document.getElementById("schEnabled")?.value || "").toString().trim() === "true";
    const mode = document.getElementById("schMode")?.value || "hourly";
    const every_n_hours = parseInt(document.getElementById("schN")?.value || "2", 10);
    const daily_time = document.getElementById("schTime")?.value || "03:30";
    const advOn = !!document.getElementById("schAdvEnabled")?.checked;
    patch = { enabled, mode, every_n_hours, daily_time, advanced: { enabled: advOn, jobs: [] } };
  }

  set("hub_sch_enabled", `Status: ${patch.enabled ? "Enabled" : "Disabled"}`);

  let modeText = patch.mode || "hourly";
  if (patch.mode === "hourly") modeText = "Every hour";
  else if (patch.mode === "every_n_hours") modeText = `Every ${patch.every_n_hours || 2}h`;
  else if (patch.mode === "daily_time") modeText = `Daily ${patch.daily_time || "—"}`;
  set("hub_sch_mode", `Mode: ${modeText}`);

  const adv = patch.advanced || {};
  const jobs = Array.isArray(adv.jobs) ? adv.jobs : [];
  const active = jobs.filter(j => j && j.active !== false).length;
  const total = jobs.length;

  set("hub_sch_adv", `Plan: ${adv.enabled ? "On" : "Off"}`);
  set("hub_sch_steps", total ? `Steps: ${active}/${total}` : "Steps: —");
}

function cwSchedSettingsHubInit() {
  const first = !window.__cwSchedSettingsHubInit;
  if (first) window.__cwSchedSettingsHubInit = true;

  try { cwSchedProviderEnsure(); } catch {}

  const wire = (id) => {
    const el = document.getElementById(id);
    if (!el || el.__hubWired) return;
    el.addEventListener("change", () => { try { cwSchedSettingsHubUpdate(); } catch {} });
    el.addEventListener("input",  () => { try { cwSchedSettingsHubUpdate(); } catch {} });
    el.__hubWired = true;
  };

  ["schEnabled", "schMode", "schN", "schTime", "schAdvEnabled"].forEach(wire);

  const adv = document.getElementById("schAdv");
  if (adv && !adv.__hubWired) {
    adv.addEventListener("change", () => { try { cwSchedSettingsHubUpdate(); } catch {} }, true);
    adv.addEventListener("input",  () => { try { cwSchedSettingsHubUpdate(); } catch {} }, true);
    adv.__hubWired = true;
  }

  if (first) {
    let tab = "basic";
    try {
      const saved = (localStorage.getItem(SCHED_SETTINGS_TAB_KEY) || "").toLowerCase();
      if (["basic", "advanced"].includes(saved)) tab = saved;
    } catch {}
    cwSchedSettingsSelect(tab, { persist: false });
  }

  try { cwSchedSettingsHubUpdate(); } catch {}
}

try {
  window.cwSchedProviderSelect = cwSchedProviderSelect;
  window.cwSchedProviderEnsure = cwSchedProviderEnsure;
  window.cwSchedSettingsSelect = cwSchedSettingsSelect;
  window.cwSchedSettingsHubInit = cwSchedSettingsHubInit;
  window.cwSchedSettingsHubUpdate = cwSchedSettingsHubUpdate;
} catch {}


/* Settings Hub: Metadata Providers */
const META_SETTINGS_TAB_KEY = "cw.ui.metadata.tab.v1";
const META_PROVIDER_STATE_KEY = "cw.ui.meta.provider.v1";
const TMDB_META_SUBTAB_KEY = "cw.ui.meta.tmdb.sub.v1";

let activeMetaProvider = null;

function cwMetaProviderUpdateChips() {
  try { cwMetaSettingsHubUpdate?.(); } catch {}
}

function cwMetaProviderSelect(provider, opts = {}) {
  const want = provider ? String(provider).toLowerCase() : null;

  const tilesHost = document.getElementById("meta_provider_tiles");
  const panelHost = document.getElementById("meta-provider-panel");
  if (!tilesHost || !panelHost) return;

  const tiles = tilesHost.querySelectorAll("[data-provider]");
  tiles.forEach((btn) => {
    const k = String(btn.dataset.provider || "").toLowerCase();
    const on = !!(want && k === want);
    btn.classList.toggle("active", on);
    btn.setAttribute("aria-selected", on ? "true" : "false");
  });

  activeMetaProvider = want;
  panelHost.classList.toggle("hidden", !want);

  const panels = panelHost.querySelectorAll(".cw-meta-provider-panel");
  panels.forEach((p) => {
    const k = String(p.dataset.provider || "").toLowerCase();
    p.classList.toggle("active", !!(want && k === want));
  });

  if (opts.persist !== false) {
    try { localStorage.setItem(META_PROVIDER_STATE_KEY, want || ""); } catch {}
  }

  try { cwMetaProviderUpdateChips(); } catch {}
}

function cwMetaProviderSubSelect(provider, sub, opts = {}) {
  const p = (provider || "").toLowerCase();
  const s = (sub || "").toLowerCase();
  if (!p || !s) return;

  const panelHost = document.getElementById("meta-provider-panel");
  const panel = panelHost?.querySelector(`.cw-meta-provider-panel[data-provider="${p}"]`);
  if (!panel) return;

  const tiles = panel.querySelectorAll(".cw-subtile[data-sub]");
  tiles.forEach((b) => b.classList.toggle("active", (b.dataset.sub || "").toLowerCase() === s));

  const subs = panel.querySelectorAll(".cw-subpanel[data-sub]");
  subs.forEach((sp) => sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === s));

  if (opts.persist !== false) {
    try { localStorage.setItem(TMDB_META_SUBTAB_KEY, s); } catch {}
  }
}

function cwMetaProviderInit() {
  cwMetaProviderSelect(null, { persist: false });
}

function cwMetaProviderEnsure() {
  const tilesHost = document.getElementById("meta_provider_tiles");
  const panelHost = document.getElementById("meta-provider-panel");
  if (!tilesHost || !panelHost) return;

  tilesHost.querySelectorAll("[data-provider]").forEach((btn) => {
    if (btn.__cwMetaWired) return;
    btn.addEventListener("click", () => {
      const want = String(btn.dataset.provider || "").toLowerCase();
      if (want && activeMetaProvider === want) cwMetaProviderSelect(null);
      else cwMetaProviderSelect(btn.dataset.provider || null);
    });
    btn.__cwMetaWired = true;
  });

  if (!panelHost.dataset.__cwMetaBuilt) {
    try { cwBuildTmdbPanel(); } catch {}
    panelHost.dataset.__cwMetaBuilt = "1";
  }

  try {
    const keyEl = document.getElementById("tmdb_api_key");
    if (keyEl && !keyEl.__tmdbChipWired) {
      keyEl.addEventListener("input", () => { try { cwMetaProviderUpdateChips(); } catch {} });
      keyEl.__tmdbChipWired = true;
    }
  } catch {}

  try { cwMetaProviderInit(); } catch {}
  try { cwMetaProviderUpdateChips(); } catch {}
}

function cwBuildTmdbPanel() {
  const panelHost = document.getElementById("meta-provider-panel");
  if (!panelHost) return;

  if (panelHost.querySelector('.cw-meta-provider-panel[data-provider="tmdb"]')) return;

  const wrap = document.createElement("div");
  wrap.className = "cw-meta-provider-panel";
  wrap.dataset.provider = "tmdb";

  const head = document.createElement("div");
  head.className = "cw-panel-head";
  head.innerHTML = `
    <div>
      <div class="cw-panel-title">TMDb (The Movie Database)</div>
      <div class="muted">Metadata and images fetched from TMDb.</div>
    </div>
  `;

  const subTiles = document.createElement("div");
  subTiles.className = "cw-subtiles";
  subTiles.innerHTML = `
    <button type="button" class="cw-subtile active" data-sub="api">API key</button>
    <button type="button" class="cw-subtile" data-sub="advanced">Advanced</button>
  `;

  const subPanels = document.createElement("div");
  subPanels.className = "cw-subpanels";

  const pApi = document.createElement("div");
  pApi.className = "cw-subpanel active";
  pApi.dataset.sub = "api";

  const pAdv = document.createElement("div");
  pAdv.className = "cw-subpanel";
  pAdv.dataset.sub = "advanced";

  const detach = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    try { el.parentNode?.removeChild(el); } catch {}
    return el;
  };

  const keyInput = detach("tmdb_api_key") || (() => {
    const i = document.createElement("input");
    i.id = "tmdb_api_key";
    i.type = "text";
    i.placeholder = "TMDb API key";
    return i;
  })();

  const hint = detach("tmdb_hint") || (() => {
    const d = document.createElement("div");
    d.id = "tmdb_hint";
    d.className = "auth-card-notes";
    d.textContent = "Add a TMDb API key to enable metadata lookups.";
    return d;
  })();

  if (!hint.classList.contains("auth-card-notes")) hint.classList.add("auth-card-notes");

  const apiCard = document.createElement("div");
  apiCard.className = "auth-card";

  const apiFields = document.createElement("div");
  apiFields.className = "auth-card-fields";

  const apiField = document.createElement("div");
  apiField.className = "field";
  apiField.innerHTML = `<div class="muted" style="margin-bottom:6px;">API key</div>`;
  apiField.appendChild(keyInput);

  apiFields.appendChild(apiField);
  apiCard.appendChild(hint);
  apiCard.appendChild(apiFields);

  pApi.appendChild(apiCard);

  const localeEl = detach("metadata_locale");
  const ttlEl = detach("metadata_ttl_hours");

  const advCard = document.createElement("div");
  advCard.className = "auth-card";

  const advFields = document.createElement("div");
  advFields.className = "auth-card-fields";

  if (localeEl) {
    const f = document.createElement("div");
    f.className = "field";
    f.innerHTML = `<div class="muted" style="margin-bottom:6px;">Language / locale</div>`;
    advFields.appendChild(f);
    f.appendChild(localeEl);
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = "Optional. Example: en-US, nl-NL.";
    advFields.appendChild(note);
  }

  if (ttlEl) {
    const f = document.createElement("div");
    f.className = "field";
    f.innerHTML = `<div class="muted" style="margin-bottom:6px;">Cache TTL (hours)</div>`;
    f.appendChild(ttlEl);
    advFields.appendChild(f);
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = "How long metadata stays cached before re-fetching.";
    advFields.appendChild(note);
  }

  if (!localeEl && !ttlEl) {
    const note = document.createElement("div");
    note.className = "auth-card-notes";
    note.textContent = "No advanced options available yet.";
    advCard.appendChild(note);
  }

  if (advFields.childNodes.length) advCard.appendChild(advFields);
  pAdv.appendChild(advCard);

  subPanels.appendChild(pApi);
  subPanels.appendChild(pAdv);

  wrap.appendChild(head);
  wrap.appendChild(subTiles);
  wrap.appendChild(subPanels);

  panelHost.appendChild(wrap);

  subTiles.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
    btn.addEventListener("click", () => cwMetaProviderSubSelect("tmdb", btn.dataset.sub));
  });

  let lastSub = "api";
  try { lastSub = (localStorage.getItem(TMDB_META_SUBTAB_KEY) || "api").toLowerCase(); } catch {}
  cwMetaProviderSubSelect("tmdb", (lastSub === "advanced") ? "advanced" : "api", { persist: false });
}

try {
  window.cwMetaProviderSelect = cwMetaProviderSelect;
  window.cwMetaProviderEnsure = cwMetaProviderEnsure;
  window.cwMetaProviderSubSelect = cwMetaProviderSubSelect;
} catch {}


function cwMetaSettingsSelect(tab, opts = {}) {
  const hub = document.getElementById("meta_settings_hub");
  const panels = document.getElementById("meta_settings_panels");
  if (!hub || !panels) return;

  const t = (tab || "tmdb").toLowerCase();
  const want = ["tmdb"].includes(t) ? t : "tmdb";

  hub.querySelectorAll(".cw-hub-tile").forEach((btn) => {
    btn.classList.toggle("active", (btn.dataset.tab || "") === want);
  });
  panels.querySelectorAll(".cw-settings-panel").forEach((p) => {
    p.classList.toggle("active", (p.dataset.tab || "") === want);
  });

  if (opts.persist !== false) {
    try { localStorage.setItem(META_SETTINGS_TAB_KEY, want); } catch {}
  }
  try { cwMetaSettingsHubUpdate(); } catch {}
}

function cwMetaSettingsHubUpdate() {
  const chip = document.getElementById("hub_tmdb_key");
  if (!chip) return;

  const cfg = window._cfgCache || {};
  const cfgKey = String(cfg?.tmdb?.api_key || "").trim();
  const cfgMasked = cfgKey === "*****" || /^[•]+$/.test(cfgKey);
  const cfgHasKey = cfgKey.length > 0 || cfgMasked;

  const keyEl = document.getElementById("tmdb_api_key");
  let uiHasKey = false;
  let uiTouched = false;

  if (keyEl) {
    const v = String(keyEl.value || "").trim();
    uiTouched = keyEl.dataset?.touched === "1";
    const vMasked = v === "*****" || /^[•]+$/.test(v);
    const dsMasked = keyEl.dataset?.masked === "1";
    uiHasKey = v.length > 0 || vMasked || dsMasked;
    if (uiTouched) uiHasKey = v.length > 0 || vMasked;
  }

  const hasKeyNow = uiHasKey || (!uiTouched && cfgHasKey);
  chip.textContent = `API key: ${hasKeyNow ? "set" : "missing"}`;
}

function cwMetaSettingsHubInit() {
  let last = null;
  try { last = localStorage.getItem(META_SETTINGS_TAB_KEY); } catch {}
  cwMetaSettingsSelect(last || "tmdb", { persist: false });
  try { cwMetaSettingsHubUpdate(); } catch {}
}

function cwMetaSettingsHubEnsure() {
  const host = document.getElementById("metadata-providers");
  if (!host || host.dataset.metaHubified === "1") return;

  // New provider presents
  if (document.getElementById("meta_provider_tiles") || document.getElementById("meta-provider-panel")) {
    host.dataset.metaHubified = "1";
    return;
  }

  if (host.querySelector("#meta_settings_hub")) {
    host.dataset.metaHubified = "1";
    return;
  }

  const hub = document.createElement("div");
  hub.className = "cw-settings-hub cw-settings-hub--single";
  hub.id = "meta_settings_hub";

  const tile = document.createElement("button");
  tile.type = "button";
  tile.className = "cw-hub-tile tmdb active";
  tile.dataset.tab = "tmdb";
  tile.innerHTML = `
    <div class="cw-hub-dots" aria-hidden="true">
      <span class="cw-hub-dot dot-a"></span>
      <span class="cw-hub-dot dot-b"></span>
      <span class="cw-hub-dot dot-c"></span>
    </div>
    <div class="cw-hub-title-row">
      <img class="cw-hub-logo" src="/assets/img/TMDB.svg" alt="" loading="lazy">
      <div>
        <div class="cw-hub-title">TMDb</div>
        <div class="cw-hub-desc">The Movie Database</div>
      </div>
    </div>
    <div class="chips">
      <span class="chip" id="hub_tmdb_key">API key: —</span>
    </div>
  `;
  tile.addEventListener("click", () => cwMetaSettingsSelect("tmdb"));

  hub.appendChild(tile);

  const panels = document.createElement("div");
  panels.className = "cw-settings-panels";
  panels.id = "meta_settings_panels";

  const panel = document.createElement("div");
  panel.className = "cw-settings-panel active";
  panel.dataset.tab = "tmdb";

  while (host.firstChild) panel.appendChild(host.firstChild);

  panels.appendChild(panel);

  host.appendChild(hub);
  host.appendChild(panels);

  host.dataset.metaHubified = "1";

  const keyEl = document.getElementById("tmdb_api_key");
  if (keyEl && !keyEl.__tmdbChipWired) {
    keyEl.addEventListener("input", () => {
      try { cwMetaSettingsHubUpdate(); } catch {}
    });
    keyEl.__tmdbChipWired = true;
  }

  setTimeout(() => {
    try { cwMetaSettingsHubInit(); } catch {}
  }, 0);
}

try {
  window.cwMetaSettingsSelect = cwMetaSettingsSelect;
  window.cwMetaSettingsHubInit = cwMetaSettingsHubInit;
  window.cwMetaSettingsHubUpdate = cwMetaSettingsHubUpdate;
  window.cwMetaSettingsHubEnsure = cwMetaSettingsHubEnsure;
} catch {}

async function loadConfig() {
  const r = await fetch("/api/config", { cache: "no-store", credentials: "same-origin" });
  if (r.status === 401) {
    location.href = "/login";
    return;
  }
  if (!r.ok) throw new Error(`GET /api/config ${r.status}`);
  const cfg = await r.json();
  window._cfgCache = cfg;

  try { bindSyncVisibilityObservers?.(); } catch {}
  try {
    if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
    else applySyncVisibility?.();
  } catch {}

  _setVal("mode",   cfg.sync?.bidirectional?.mode || "two-way");
  _setVal("source", cfg.sync?.bidirectional?.source_of_truth || "plex");
  (function(){
    const rt = cfg.runtime || {};
    let mode = 'off';
    if (rt.debug) mode = (rt.debug_mods && rt.debug_http) ? 'full' : (rt.debug_mods ? 'mods' : 'on');
    _setVal("debug", mode);
  })();
  _setVal("metadata_locale", cfg.metadata?.locale || "");
  _setVal("metadata_ttl_hours", String(Number.isFinite(cfg.metadata?.ttl_hours) ? cfg.metadata.ttl_hours : 6));

  
  (function () {
    const ui = cfg.ui || cfg.user_interface || {};
    const cw = cfg.crosswatch || {};
    const aa = cfg.app_auth || {};

    
    const sel = document.getElementById("ui_show_watchlist_preview");
    if (sel) {
      const on = (typeof ui.show_watchlist_preview === "boolean")
        ? !!ui.show_watchlist_preview
        : true;
      sel.value = on ? "true" : "false";
    }

    
    const playSel = document.getElementById("ui_show_playingcard");
    if (playSel) {
      const on = (typeof ui.show_playingcard === "boolean")
        ? !!ui.show_playingcard
        : true;
      playSel.value = on ? "true" : "false";
    }

    const aiSel = document.getElementById("ui_show_AI");
    if (aiSel) {
      const on = (typeof ui.show_AI === "boolean")
        ? !!ui.show_AI
        : true;
      aiSel.value = on ? "true" : "false";
    }

    const protoSel = document.getElementById("ui_protocol");
    if (protoSel) {
      const p = String(ui.protocol || "http").trim().toLowerCase();
      protoSel.value = (p === "https") ? "https" : "http";
    }

    const aaEnabledEl = document.getElementById("app_auth_enabled");
    if (aaEnabledEl) aaEnabledEl.value = (aa.enabled === true) ? "true" : "false";
    const aaUserEl = document.getElementById("app_auth_username");
    if (aaUserEl) aaUserEl.value = (typeof aa.username === "string") ? aa.username : "";
    const aaP1 = document.getElementById("app_auth_password");
    if (aaP1) aaP1.value = "";
    const aaP2 = document.getElementById("app_auth_password2");
    if (aaP2) aaP2.value = "";

    
    const cwEnabledEl = document.getElementById("cw_enabled");
    if (cwEnabledEl) {
      const enabled = (cw.enabled === false) ? "false" : "true";
      cwEnabledEl.value = enabled;
    }
    const cwRetEl = document.getElementById("cw_retention_days");
    if (cwRetEl) {
      const v = Number.isFinite(cw.retention_days) ? cw.retention_days : 30;
      cwRetEl.value = String(v);
    }
    const cwAutoEl = document.getElementById("cw_auto_snapshot");
    if (cwAutoEl) {
      const on = (cw.auto_snapshot === false) ? "false" : "true";
      cwAutoEl.value = on;
    }
    const cwMaxEl = document.getElementById("cw_max_snapshots");
    if (cwMaxEl) {
      const v = Number.isFinite(cw.max_snapshots) ? cw.max_snapshots : 64;
      cwMaxEl.value = String(v);
    }
    const setVal = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.value = val || "latest";
    };
    setVal("cw_restore_watchlist", cw.restore_watchlist || "latest");
    setVal("cw_restore_history", cw.restore_history || "latest");
    setVal("cw_restore_ratings", cw.restore_ratings || "latest");
  })();

  try { cwUiSettingsHubInit?.(); } catch {}

  await loadCrossWatchSnapshots(cfg);
  window.appDebug = !!(cfg.runtime && cfg.runtime.debug);


(function hydrateSecretsRaw(cfg){
  const val = (x) => (typeof x === "string" ? x.trim() : "");
  const setRaw = (id, v) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.value = v || "";
    el.dataset.masked  = "0";
    el.dataset.loaded  = "1";
    el.dataset.touched = "";
    el.dataset.clear   = "";
    try { wireSecretTouch(id); } catch {}
  };

  
  setRaw("plex_token",    val(cfg.plex?.account_token));
  setRaw("plex_home_pin", val(cfg.plex?.home_pin));

  
  setRaw("simkl_client_id",     val(cfg.simkl?.client_id));
  setRaw("simkl_client_secret", val(cfg.simkl?.client_secret));
  setRaw("simkl_access_token",  val(cfg.simkl?.access_token) || val(cfg.auth?.simkl?.access_token));

  
  setRaw("anilist_client_id",     val(cfg.anilist?.client_id));
  setRaw("anilist_client_secret", val(cfg.anilist?.client_secret));
  setRaw("anilist_access_token",  val(cfg.anilist?.access_token) || val(cfg.auth?.anilist?.access_token));

  
  setRaw("tmdb_api_key",        val(cfg.tmdb?.api_key));

  
  setRaw("mdblist_key",         val(cfg.mdblist?.api_key));

  
  setRaw("trakt_client_id",     val(cfg.trakt?.client_id));
  setRaw("trakt_client_secret", val(cfg.trakt?.client_secret));
  setRaw("trakt_token",         val(cfg.trakt?.access_token) || val(cfg.auth?.trakt?.access_token));
})(cfg);

  try { cwMetaSettingsHubUpdate(); } catch {}

  const s = cfg.scheduling || {};
  _setVal("schEnabled", String(!!s.enabled));
  _setVal("schMode",    typeof s.mode === "string" && s.mode ? s.mode : "hourly");
  _setVal("schN",       Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "2");
  _setVal("schTime",    typeof s.daily_time === "string" && s.daily_time ? s.daily_time : "03:30");
  if (document.getElementById("schTz")) _setVal("schTz", s.timezone || "");

  try {
    const r = await fetch("/api/app-auth/status", { cache: "no-store", credentials: "same-origin" });
    const st = r.ok ? await r.json() : null;
    window._appAuthStatus = st;

    try {
      const aaEnabledEl = document.getElementById("app_auth_enabled");
      if (aaEnabledEl && st && typeof st.enabled === "boolean") aaEnabledEl.value = st.enabled ? "true" : "false";
      const aaUserEl = document.getElementById("app_auth_username");
      if (aaUserEl && st && st.enabled) aaUserEl.value = (st.username || "").toString();
    } catch {}

    const el = document.getElementById("app_auth_state");
    if (el) {
      if (!st) el.textContent = "—";
      else if (!st.enabled) el.textContent = "Auth: disabled";
      else if (!st.configured) el.textContent = "Auth: enabled (set password)";
      else if (st.authenticated) {
        const exp = (st.session_expires_at && st.session_expires_at > 0) ? new Date(st.session_expires_at * 1000) : null;
        el.textContent = exp ? `Auth: signed in (until ${exp.toISOString().replace('T',' ').slice(0,16)}Z)` : "Auth: signed in";
      } else el.textContent = "Auth: locked";
    }
    const btn = document.getElementById("btn-auth-logout");
    if (btn) btn.disabled = !(st && st.enabled && st.authenticated);
  } catch {}

  try { cwUiSettingsHubUpdate?.(); } catch {}

  try { window.updateSimklButtonState?.(); } catch {}
  try { window.updateSimklHint?.();      } catch {}
  try { window.updateTmdbHint?.();       } catch {}
  try {
    if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
    else applySyncVisibility?.();
  } catch {}
}

window.cwAppLogout = async function cwAppLogout() {
  try {
    await fetch("/api/app-auth/logout", { method: "POST", cache: "no-store", credentials: "same-origin" });
  } catch {}
  location.href = "/login";
};

function _getVal(id) {
  const el = document.getElementById(id);
  return (el && typeof el.value === "string" ? el.value : "").trim();
}

async function saveSettings() {
  let schedChanged = false;
  const fromFab = (() => {
    const ae = document.activeElement;
    return !!(ae && typeof ae.closest === "function" && ae.closest("#save-fab"));
  })();

  const _cwEnsureSaveToast = () => {
    let el = document.querySelector(".save-toast");
    if (!el) {
      const inline = document.getElementById("save_msg");
      if (inline && !inline.closest("#save-fab")) el = inline;
    }

    if (el) return el;
    try {
      el = document.createElement("div");
      el.className = "save-toast hide";
      el.setAttribute("aria-live", "polite");
      document.body.appendChild(el);
      if (!document.getElementById("cw-save-toast-style")) {
        const style = document.createElement("style");
        style.id = "cw-save-toast-style";
        style.textContent = `
          .save-toast{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);z-index:9999;max-width:calc(100vw - 24px);
            padding:10px 14px;border-radius:999px;backdrop-filter:blur(10px);background:rgba(20,20,30,.82);
            border:1px solid rgba(255,255,255,.14);color:#fff;font-size:13px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
          .save-toast.ok{border-color:rgba(80,220,140,.35)}
          .save-toast.error{border-color:rgba(255,120,120,.35)}
          .save-toast.hide{display:none}
        `;
        document.head.appendChild(style);
      }
    } catch {}
    return el;
  };

  const _cwEnsureInlineErrorStyle = () => {
    if (document.getElementById("cw-inline-error-style")) return;
    try {
      const style = document.createElement("style");
      style.id = "cw-inline-error-style";
      style.textContent = `
        .cw-inline-error{margin-top:10px;padding:8px 10px;border-radius:12px;background:rgba(255,80,80,.08);
          border:1px solid rgba(255,80,80,.18);color:rgba(255,220,220,.95);font-size:12px}
        .cw-inline-error.hidden{display:none}
        .cw-invalid{border-color:rgba(255,100,100,.55)!important;box-shadow:0 0 0 2px rgba(255,80,80,.12)!important}
      `;
      document.head.appendChild(style);
    } catch {}
  };

  const _cwEnsureAuthInlineError = () => {
    const host = document.getElementById("app_auth_fields");
    if (!host) return null;
    let el = document.getElementById("app_auth_error");
    if (el) return el;
    try {
      _cwEnsureInlineErrorStyle();
      el = document.createElement("div");
      el.id = "app_auth_error";
      el.className = "cw-inline-error hidden";
      el.setAttribute("role", "alert");
      host.appendChild(el);
      return el;
    } catch {
      return null;
    }
  };

  const setAuthError = (msg) => {
    const p1 = document.getElementById("app_auth_password");
    const p2 = document.getElementById("app_auth_password2");
    const has = !!(msg && String(msg).trim());
    try {
      if (p1) { p1.classList.toggle("cw-invalid", has); has ? p1.setAttribute("aria-invalid", "true") : p1.removeAttribute("aria-invalid"); }
      if (p2) { p2.classList.toggle("cw-invalid", has); has ? p2.setAttribute("aria-invalid", "true") : p2.removeAttribute("aria-invalid"); }
    } catch {}
    const el = _cwEnsureAuthInlineError();
    if (!el) return;
    if (!has) {
      el.textContent = "";
      el.classList.add("hidden");
      return;
    }
    el.textContent = String(msg);
    el.classList.remove("hidden");
  };

  const abortSave = (msg) => {
    const e = new Error(String(msg || "Save aborted"));
    // @ts-ignore
    e.__cwAbortSave = true;
    throw e;
  };

  const showToast = (text, ok = true) => {
    _cwEnsureSaveToast();
    try {
      const fn = window.CW?.DOM?.showToast || window.showToast;
      if (typeof fn === "function") return fn(String(text || ""), ok);
    } catch {}
    const el = _cwEnsureSaveToast();
    if (!el) return console.log(text);
    el.textContent = String(text || "");
    el.classList.remove("hide", "error", "ok");
    el.classList.add(ok ? "ok" : "error");
    el.classList.remove("hide");
    window.setTimeout(() => el.classList.add("hide"), 2000);
  };

  const norm = (s) => (s ?? "").trim();
  const readToggle = (id) => {
    const el = document.getElementById(id);
    if (!el) return false;
    const raw = norm(el.value || "");
    const s = raw.toLowerCase();
    return ["true","1","yes","on","enabled","enable"].includes(s);
  };

  ([
    "plex_token",
    "simkl_client_id",
    "simkl_client_secret",
    "trakt_client_id",
    "trakt_client_secret",
    "anilist_client_id",
    "anilist_client_secret",
    "tmdb_api_key",
    "mdblist_key"
  ]).forEach(id => {
    const el = document.getElementById(id);
    if (el && !el.__touchedWired) {
      el.addEventListener("input", () => { el.dataset.touched = "1"; });
      el.__touchedWired = true;
    }
  });

  function readSecretSafe(id, previousValue) {
    const el = document.getElementById(id);
    if (!el) return { changed: false };

    const raw = norm(el.value);
    const masked = el.dataset?.masked === "1" || raw.startsWith("•");
    const touched = el.dataset?.touched === "1";
    const explicitClear = el.dataset?.clear === "1";
    const loadedFlag = el.dataset?.loaded;

    if (explicitClear) return { changed: true, clear: true };
    if (loadedFlag === "0") return { changed: false };
    if (!touched || masked) return { changed: false };

    if (raw === "") {
      return previousValue ? { changed: true, clear: true } : { changed: false };
    }
    if (raw !== previousValue) return { changed: true, set: raw };
    return { changed: false };
  }

  try {
    const serverResp = await fetch("/api/config", { cache: "no-store" });
    if (!serverResp.ok) throw new Error(`GET /api/config ${serverResp.status}`);
    const serverCfg = await serverResp.json();
    const cfg = JSON.parse(JSON.stringify(serverCfg || {}));
    let changed = false;

    try { delete cfg.app_auth; } catch {}

    try {
      const wantEnabled = (document.getElementById("app_auth_enabled")?.value || "").toString() === "true";
      const wantUser = norm(document.getElementById("app_auth_username")?.value || "");
      const pass1 = (document.getElementById("app_auth_password")?.value || "").toString();
      const pass2 = (document.getElementById("app_auth_password2")?.value || "").toString();

      const prevEnabled = !!serverCfg?.app_auth?.enabled;
      const prevUser = norm(serverCfg?.app_auth?.username);

      let st = null;
      try {
        const r = await fetch("/api/app-auth/status", { cache: "no-store", credentials: "same-origin" });
        st = r.ok ? await r.json() : null;
      } catch {}

      const configured = !!(st && st.configured);
      const wantsPwd = norm(pass1) !== "" || norm(pass2) !== "";
      const needsCall = (wantEnabled !== prevEnabled) || (wantUser !== prevUser) || wantsPwd;

      try {
        const p1El = document.getElementById("app_auth_password");
        const p2El = document.getElementById("app_auth_password2");
        if (p1El && p2El && !p1El.__cwAuthPwWired) {
          const onInput = () => {
            const a = (p1El.value || "").toString();
            const b = (p2El.value || "").toString();
            if (!norm(a) && !norm(b)) { setAuthError(""); return; }
            if (a === b) setAuthError("");
          };
          p1El.addEventListener("input", onInput);
          p2El.addEventListener("input", onInput);
          p1El.__cwAuthPwWired = true;
        }
      } catch {}

      setAuthError("");
      try { document.getElementById("app_auth_username")?.classList.remove("cw-invalid"); } catch {}

      if (wantsPwd && pass1 !== pass2) {
        setAuthError("Passwords do not match");
        showToast("Password mismatch", false);
        try { document.getElementById("app_auth_password2")?.focus?.(); } catch {}
        abortSave("Password mismatch");
      }
      if (wantEnabled && !wantUser) {
        showToast("Auth username required", false);
        try { document.getElementById("app_auth_username")?.classList.add("cw-invalid"); } catch {}
        abortSave("Auth username required");
      }
      if (wantEnabled && !configured && !norm(pass1)) {
        setAuthError("Password required to enable auth");
        showToast("Set a password to enable auth", false);
        abortSave("Password required");
      }

      if (needsCall) {
        const resp = await fetch("/api/app-auth/credentials", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "same-origin",
          cache: "no-store",
          body: JSON.stringify({ enabled: wantEnabled, username: wantUser, password: pass1 || "" }),
        });
        const j = await resp.json().catch(() => null);
        if (!resp.ok || !j || !j.ok) {
          showToast((j && j.error) ? j.error : `Auth save failed (${resp.status})`, false);
          return;
        }
        try { document.getElementById("app_auth_password").value = ""; } catch {}
        try { document.getElementById("app_auth_password2").value = ""; } catch {}
        try { if (typeof loadConfig === "function") await loadConfig(); } catch {}
      }
    } catch (e) {
      console.warn("saveSettings: app_auth merge failed", e);
      // @ts-ignore
      if (e && e.__cwAbortSave) throw e;
    }

    const prevMode     = serverCfg?.sync?.bidirectional?.mode || "two-way";
    const prevSource   = serverCfg?.sync?.bidirectional?.source_of_truth || "plex";
    const prevDebug     = !!serverCfg?.runtime?.debug;
    const prevDebugMods = !!serverCfg?.runtime?.debug_mods;
    const prevDebugHttp = !!serverCfg?.runtime?.debug_http;
    const prevPlex     = norm(serverCfg?.plex?.account_token);
    const prevHomePin  = norm(serverCfg?.plex?.home_pin);
    const prevAniCid = norm(serverCfg?.anilist?.client_id);
    const prevAniSec = norm(serverCfg?.anilist?.client_secret);
    const prevCid      = norm(serverCfg?.simkl?.client_id);
    const prevSec      = norm(serverCfg?.simkl?.client_secret);
    const prevTmdb     = norm(serverCfg?.tmdb?.api_key);
    const prevTraktCid = norm(serverCfg?.trakt?.client_id);
    const prevTraktSec = norm(serverCfg?.trakt?.client_secret);
    const prevMdbl     = norm(serverCfg?.mdblist?.api_key);
    const prevMetaLocale = (serverCfg?.metadata?.locale ?? "").trim();
    const prevMetaTTL    = Number.isFinite(serverCfg?.metadata?.ttl_hours) ? Number(serverCfg.metadata.ttl_hours) : 6;
    const prevUiShow     = (typeof serverCfg?.ui?.show_watchlist_preview === "boolean") ? !!serverCfg.ui.show_watchlist_preview : true;
    const prevUiPlaying  = (typeof serverCfg?.ui?.show_playingcard === "boolean") ? !!serverCfg.ui.show_playingcard : true;
    const prevUiAskAi    = (typeof serverCfg?.ui?.show_AI === "boolean") ? !!serverCfg.ui.show_AI : true;
    const prevUiProtocol = String(serverCfg?.ui?.protocol || "http").trim().toLowerCase() === "https" ? "https" : "http";

    const prevCw = serverCfg?.crosswatch || {};

    const prevCwEnabled  = (prevCw.enabled === false) ? false : true;
    const prevCwRet      = Number.isFinite(prevCw.retention_days) ? Number(prevCw.retention_days) : 30;
    const prevCwAuto     = (prevCw.auto_snapshot === false) ? false : true;
    const prevCwMax      = Number.isFinite(prevCw.max_snapshots) ? Number(prevCw.max_snapshots) : 64;
    const prevCwRestoreWatch  = (prevCw.restore_watchlist || "latest").trim();
    const prevCwRestoreHist   = (prevCw.restore_history || "latest").trim();
    const prevCwRestoreRates  = (prevCw.restore_ratings || "latest").trim();

    const uiMode   = _getVal("mode");
    const uiSource = _getVal("source");
    const uiDebugMode = _getVal("debug"); 
    let wantDebug=false, wantMods=false, wantHttp=false;
    if (uiDebugMode==='on'){wantDebug=true;}
    else if (uiDebugMode==='mods'){wantDebug=true; wantMods=true;}
    else if (uiDebugMode==='full'){wantDebug=true; wantMods=true; wantHttp=true;}

    if (uiMode !== prevMode) {
      cfg.sync = cfg.sync || {};
      cfg.sync.bidirectional = cfg.sync.bidirectional || {};
      cfg.sync.bidirectional.mode = uiMode;
      changed = true;
    }
    if (uiSource !== prevSource) {
      cfg.sync = cfg.sync || {};
      cfg.sync.bidirectional = cfg.sync.bidirectional || {};
      cfg.sync.bidirectional.source_of_truth = uiSource;
      changed = true;
    }
    if (wantDebug!==prevDebug || wantMods!==prevDebugMods || wantHttp!==prevDebugHttp) {
      cfg.runtime = cfg.runtime || {};
      cfg.runtime.debug = wantDebug;
      cfg.runtime.debug_mods = wantMods;
      cfg.runtime.debug_http = wantHttp;
      changed = true;
    }

    
    const uiMetaLocale = (document.getElementById("metadata_locale")?.value || "").trim();
    const uiMetaTTLraw = (document.getElementById("metadata_ttl_hours")?.value || "").trim();
    const uiMetaTTL    = uiMetaTTLraw === "" ? null : parseInt(uiMetaTTLraw, 10);

    if (uiMetaLocale !== prevMetaLocale) {
      cfg.metadata = cfg.metadata || {};
      if (uiMetaLocale) cfg.metadata.locale = uiMetaLocale;
      else delete cfg.metadata.locale; 
      changed = true;
    }
    if (uiMetaTTL !== null && !Number.isNaN(uiMetaTTL) && uiMetaTTL !== prevMetaTTL) {
      cfg.metadata = cfg.metadata || {};
      cfg.metadata.ttl_hours = Math.max(1, uiMetaTTL);
      changed = true;
    }

    
    (function () {
      const norm = (s) => (s ?? "").trim();
      const truthy = (v) => ["true","1","yes","on","enabled","enable"].includes(String(v).toLowerCase());
      const intOr = (el, prev) => {
        if (!el) return prev;
        const n = parseInt(norm(el.value || ""), 10);
        return Number.isNaN(n) ? prev : Math.max(0, n);
      };

      
      const uiSel = document.getElementById("ui_show_watchlist_preview");
      if (uiSel) {
        const uiShow = !truthy(uiSel.value) ? (uiSel.value === "false" ? false : true) : truthy(uiSel.value);
        const finalUiShow = uiSel.value === "false" ? false : true;
        if (finalUiShow !== prevUiShow) {
          cfg.ui = cfg.ui || {};
          cfg.ui.show_watchlist_preview = finalUiShow;
          changed = true;
        }
      }

      
      const uiPlaySel = document.getElementById("ui_show_playingcard");
      if (uiPlaySel) {
        const finalUiPlay = uiPlaySel.value === "false" ? false : true;
        if (finalUiPlay !== prevUiPlaying) {
          cfg.ui = cfg.ui || {};
          cfg.ui.show_playingcard = finalUiPlay;
          changed = true;
        }
      }

      const uiAiSel = document.getElementById("ui_show_AI");
      if (uiAiSel) {
        const finalUiAi = uiAiSel.value === "false" ? false : true;
        if (finalUiAi !== prevUiAskAi) {
          cfg.ui = cfg.ui || {};
          cfg.ui.show_AI = finalUiAi;
          changed = true;
          try { window.__cwAskAiChanged = { from: prevUiAskAi, to: finalUiAi }; } catch {}
        }
      }

      const protoSel = document.getElementById("ui_protocol");
      if (protoSel) {
        const want = String(protoSel.value || "http").trim().toLowerCase() === "https" ? "https" : "http";
        if (want !== prevUiProtocol) {
          cfg.ui = cfg.ui || {};
          cfg.ui.protocol = want;
          changed = true;
          try { window.__cwProtoChanged = want; } catch {}
        }
      }

      const cw = cfg.crosswatch || {};
      let cwChanged = false;


      
      const enabledEl = document.getElementById("cw_enabled");
      const newEnabled = enabledEl ? truthy(enabledEl.value) : prevCwEnabled;
      if (newEnabled !== prevCwEnabled) {
        cw.enabled = newEnabled;
        cwChanged = true;
      }

      
      const newRet = intOr(document.getElementById("cw_retention_days"), prevCwRet);
      if (newRet !== prevCwRet) {
        cw.retention_days = newRet;
        cwChanged = true;
      }

      
      const autoEl = document.getElementById("cw_auto_snapshot");
      const newAuto = autoEl ? truthy(autoEl.value) : prevCwAuto;
      if (newAuto !== prevCwAuto) {
        cw.auto_snapshot = newAuto;
        cwChanged = true;
      }

      
      const newMax = intOr(document.getElementById("cw_max_snapshots"), prevCwMax);
      if (newMax !== prevCwMax) {
        cw.max_snapshots = newMax;
        cwChanged = true;
      }

      
      const prevMap = {
        watchlist: prevCwRestoreWatch,
        history:   prevCwRestoreHist,
        ratings:   prevCwRestoreRates,
      };
      for (const key of ["watchlist", "history", "ratings"]) {
        const el = document.getElementById(`cw_restore_${key}`);
        if (!el) continue;
        const val = norm(el.value || "") || "latest";
        if (val !== prevMap[key]) {
          cw[`restore_${key}`] = val;
          cwChanged = true;
        }
      }

      if (cwChanged) {
        cfg.crosswatch = cw;
        changed = true;
      }
    })();

    
    const sPlex     = readSecretSafe("plex_token", prevPlex);
    const sHomePin  = readSecretSafe("plex_home_pin", prevHomePin);
    const sCid      = readSecretSafe("simkl_client_id", prevCid);
    const sSec      = readSecretSafe("simkl_client_secret", prevSec);
    const sTmdb     = readSecretSafe("tmdb_api_key", prevTmdb);
    const sTrkCid   = readSecretSafe("trakt_client_id", prevTraktCid);
    const sTrkSec   = readSecretSafe("trakt_client_secret", prevTraktSec);
    const sMdbl     = readSecretSafe("mdblist_key", prevMdbl);
    const sAniCid   = readSecretSafe("anilist_client_id", prevAniCid);
    const sAniSec   = readSecretSafe("anilist_client_secret", prevAniSec);

    if (sMdbl.changed) {
      cfg.mdblist = cfg.mdblist || {};
      if (sMdbl.clear) delete cfg.mdblist.api_key; else cfg.mdblist.api_key = sMdbl.set;
      changed = true;
    }

    if (sPlex.changed) {
      cfg.plex = cfg.plex || {};
      if (sPlex.clear) delete cfg.plex.account_token; else cfg.plex.account_token = sPlex.set;
      changed = true;
    }
    if (sHomePin.changed) {
      cfg.plex = cfg.plex || {};
      if (sHomePin.clear) cfg.plex.home_pin = ""; else cfg.plex.home_pin = sHomePin.set;
      changed = true;
    }
    if (sCid.changed) {
      cfg.simkl = cfg.simkl || {};
      if (sCid.clear) delete cfg.simkl.client_id; else cfg.simkl.client_id = sCid.set;
      changed = true;
    }
    if (sSec.changed) {
      cfg.simkl = cfg.simkl || {};
      if (sSec.clear) delete cfg.simkl.client_secret; else cfg.simkl.client_secret = sSec.set;
      changed = true;
    }
    if (sTrkCid.changed) {
      cfg.trakt = cfg.trakt || {};
      if (sTrkCid.clear) delete cfg.trakt.client_id; else cfg.trakt.client_id = sTrkCid.set;
      changed = true;
    }
    if (sTrkSec.changed) {
      cfg.trakt = cfg.trakt || {};
      if (sTrkSec.clear) delete cfg.trakt.client_secret; else cfg.trakt.client_secret = sTrkSec.set;
      changed = true;
    }
    if (sTmdb.changed) {
      cfg.tmdb = cfg.tmdb || {};
      if (sTmdb.clear) delete cfg.tmdb.api_key; else cfg.tmdb.api_key = sTmdb.set;
      changed = true;
    }
    if (sAniCid.changed) {
      cfg.anilist = cfg.anilist || {};
      if (sAniCid.clear) delete cfg.anilist.client_id; else cfg.anilist.client_id = sAniCid.set;
      changed = true;
    }
    if (sAniSec.changed) {
      cfg.anilist = cfg.anilist || {};
      if (sAniSec.clear) delete cfg.anilist.client_secret; else cfg.anilist.client_secret = sAniSec.set;
      changed = true;
    }


    
    try {
      const norm = (s) => (s ?? "").trim();
      const first = (...ids) => {
        for (const id of ids) {
          const el = document.getElementById(id);
          const v = el && String(el.value || "").trim();
          if (v) return v;
        }
        return "";
      };

      const jfyInstRaw = norm(document.getElementById("jellyfin_instance")?.value || "");
      const jfyInst = (jfyInstRaw && jfyInstRaw.toLowerCase() !== "default") ? jfyInstRaw : "default";

      const jfyBaseSrv = (serverCfg?.jellyfin && typeof serverCfg.jellyfin === "object") ? serverCfg.jellyfin : {};
      const prevJfy = jfyInst === "default"
        ? jfyBaseSrv
        : ((jfyBaseSrv.instances && typeof jfyBaseSrv.instances === "object" && jfyBaseSrv.instances[jfyInst]) ? jfyBaseSrv.instances[jfyInst] : {});

      const jfyBaseCfg = (cfg.jellyfin && typeof cfg.jellyfin === "object") ? cfg.jellyfin : (cfg.jellyfin = {});
      const nextJfy = (() => {
        if (jfyInst === "default") return jfyBaseCfg;
        if (!jfyBaseCfg.instances || typeof jfyBaseCfg.instances !== "object") jfyBaseCfg.instances = {};
        if (!jfyBaseCfg.instances[jfyInst] || typeof jfyBaseCfg.instances[jfyInst] !== "object") jfyBaseCfg.instances[jfyInst] = {};
        return jfyBaseCfg.instances[jfyInst];
      })();

      
      const uiSrv    = first("jfy_server_url","jfy_server");
      const uiUser   = first("jfy_username","jfy_user");
      const uiUid    = first("jfy_user_id");
      const uiVerify = !!(document.getElementById("jfy_verify_ssl")?.checked ||
                          document.getElementById("jfy_verify_ssl_dup")?.checked);

      const prevSrv    = norm(prevJfy?.server);
      const prevUser   = norm(prevJfy?.username || prevJfy?.user);
      const prevUid    = norm(prevJfy?.user_id);
      const prevVerify = !!prevJfy?.verify_ssl;

      if (uiSrv && uiSrv !== prevSrv) { nextJfy.server = uiSrv; changed = true; }
      if (uiUser && uiUser !== prevUser) {
        nextJfy.username = uiUser;
        nextJfy.user = uiUser;
        changed = true;
      }
      if (uiUid && uiUid !== prevUid) { nextJfy.user_id = uiUid; changed = true; }
      if (uiVerify !== prevVerify)   { nextJfy.verify_ssl = uiVerify; changed = true; }

      const jfyHydrated =
        window.__jellyfinHydrated === true ||
        window.__jfyHydrated === true ||
        document.getElementById("sec-jellyfin")?.dataset?.hydrated === "1" ||
        document.querySelectorAll("#jfy_lib_matrix .lm-row").length > 0 ||
        document.querySelectorAll("#jfy_lib_whitelist .whrow").length > 0 ||
        !!document.querySelector("#jfy_lib_history option, #jfy_lib_ratings option, #jfy_lib_scrobble option");

      const readFromMatrix = () => {
        const rows = document.querySelectorAll("#jfy_lib_matrix .lm-row");
        if (!rows.length) return null;
        const H = [], R = [], S = [];
        rows.forEach(r => {
          const id = String(r.dataset.id || "").trim(); 
          if (!id) return;
          if (r.querySelector(".lm-dot.hist.on")) H.push(id);
          if (r.querySelector(".lm-dot.rate.on")) R.push(id);
          if (r.querySelector(".lm-dot.scr.on")) S.push(id);
        });
        return { H, R, S };
      };

      const readFromWhitelist = () => {
        const rows = document.querySelectorAll("#jfy_lib_whitelist .whrow");
        if (!rows.length) return null;  
        const H = [], R = [], S = [];
        rows.forEach(r => {
          const id = String(r.dataset.id || "").trim(); 
          if (!id) return;
          if (r.querySelector(".whtog.hist.on")) H.push(id);
          if (r.querySelector(".whtog.rate.on")) R.push(id);
          if (r.querySelector(".whtog.scr.on")) S.push(id);
        });
        return { H, R, S };
      };

      const readFromSelects = () => {
        const toStrs = (selector) => {
          const el = document.querySelector(selector);
          if (!el) return null;
          const opts = el.selectedOptions
            ? Array.from(el.selectedOptions)
            : Array.from(el.querySelectorAll("option:checked"));
          return opts
            .map(o => String(o.value || o.dataset.value || o.textContent).trim())
            .filter(Boolean);
        };
        return { H: toStrs("#jfy_lib_history"), R: toStrs("#jfy_lib_ratings"), S: toStrs("#jfy_lib_scrobble") };
      };

      const src = jfyHydrated ? (readFromMatrix() || readFromWhitelist() || readFromSelects()) : null;

      const same = (a, b) => {
        const A = (a || []).map(String).filter(Boolean).sort();
        const B = (b || []).map(String).filter(Boolean).sort();
        if (A.length !== B.length) return false;
        for (let i = 0; i < A.length; i++) if (A[i] !== B[i]) return false;
        return true;
      };

      if (src) {
        const prevH = (prevJfy?.history?.libraries || []).map(String);
        const prevR = (prevJfy?.ratings?.libraries || []).map(String);
        const prevS = (prevJfy?.scrobble?.libraries || []).map(String);
        if (!same(src.H, prevH)) {
          nextJfy.history = Object.assign({}, nextJfy.history || {}, { libraries: src.H || [] });
          changed = true;
        }
        if (!same(src.R, prevR)) {
          nextJfy.ratings = Object.assign({}, nextJfy.ratings || {}, { libraries: src.R || [] });
          changed = true;
        }
        if (!same(src.S, prevS)) {
          nextJfy.scrobble = Object.assign({}, nextJfy.scrobble || {}, { libraries: src.S || [] });
          changed = true;
        }
      }
    } catch (e) {
      console.warn("saveSettings: jellyfin merge failed", e);
    }

      
    try {
      const norm = (s) => (s ?? "").trim();

      const embyInstRaw = norm(document.getElementById("emby_instance")?.value || "");
      const embyInst = (embyInstRaw && embyInstRaw.toLowerCase() !== "default") ? embyInstRaw : "default";

      const embyBaseSrv = (serverCfg?.emby && typeof serverCfg.emby === "object") ? serverCfg.emby : {};
      const prevEmby = embyInst === "default"
        ? embyBaseSrv
        : ((embyBaseSrv.instances && typeof embyBaseSrv.instances === "object" && embyBaseSrv.instances[embyInst]) ? embyBaseSrv.instances[embyInst] : {});

      const embyBaseCfg = (cfg.emby && typeof cfg.emby === "object") ? cfg.emby : (cfg.emby = {});
      const nextEmby = (() => {
        if (embyInst === "default") return embyBaseCfg;
        if (!embyBaseCfg.instances || typeof embyBaseCfg.instances !== "object") embyBaseCfg.instances = {};
        if (!embyBaseCfg.instances[embyInst] || typeof embyBaseCfg.instances[embyInst] !== "object") embyBaseCfg.instances[embyInst] = {};
        return embyBaseCfg.instances[embyInst];
      })();

      const readFromMatrix = () => {
        const rows = document.querySelectorAll("#emby_lib_matrix .lm-row");
        if (!rows.length) return null;
        const H = [], R = [], S = [];
        rows.forEach((r) => {
          const id = String(r.dataset.id || "").trim(); 
          if (!id) return;
          if (r.querySelector(".lm-dot.hist.on")) H.push(id);
          if (r.querySelector(".lm-dot.rate.on")) R.push(id);
          if (r.querySelector(".lm-dot.scr.on")) S.push(id);
        });
        return { H, R, S };
      };

      const readFromWhitelist = () => {
        const rows = document.querySelectorAll("#emby_lib_whitelist .whrow");
        if (!rows.length) return null;
        const H = [], R = [], S = [];
        rows.forEach((r) => {
          const id = String(r.dataset.id || "").trim();
          if (!id) return;
          if (r.querySelector(".whtog.hist.on")) H.push(id);
          if (r.querySelector(".whtog.rate.on")) R.push(id);
          if (r.querySelector(".whtog.scr.on")) S.push(id);
        });
        return { H, R, S };
      };

      const readFromSelects = () => {
        const toStrs = (selector) => {
          const el = document.querySelector(selector);
          if (!el) return null;
          const opts = el.selectedOptions
            ? Array.from(el.selectedOptions)
            : Array.from(el.querySelectorAll("option:checked"));
          return opts
            .map((o) => String(o.value || o.dataset.value || o.textContent).trim())
            .filter(Boolean);
        };
        return {
          H: toStrs("#emby_lib_history"),
          R: toStrs("#emby_lib_ratings"),
          S: toStrs("#emby_lib_scrobble"),
        };
      };

      const embyHydrated =
        window.__embyHydrated === true ||
        document.getElementById("sec-emby")?.dataset?.hydrated === "1" ||
        document.querySelectorAll("#emby_lib_matrix .lm-row").length > 0 ||
        document.querySelectorAll("#emby_lib_whitelist .whrow").length > 0 ||
        !!document.querySelector("#emby_lib_history option, #emby_lib_ratings option, #emby_lib_scrobble option");

      const src = embyHydrated ? (readFromMatrix() || readFromWhitelist() || readFromSelects()) : null;

      const same = (a, b) => {
        const A = (a || []).map(String).filter(Boolean).sort();
        const B = (b || []).map(String).filter(Boolean).sort();
        if (A.length !== B.length) return false;
        for (let i = 0; i < A.length; i++) if (A[i] !== B[i]) return false;
        return true;
      };

      if (src) {
        const prevH = (prevEmby?.history?.libraries || []).map(String);
        const prevR = (prevEmby?.ratings?.libraries || []).map(String);
        const prevS = (prevEmby?.scrobble?.libraries || []).map(String);

        if (!same(src.H, prevH)) {
          nextEmby.history = Object.assign({}, nextEmby.history || {}, { libraries: src.H || [] });
          changed = true;
        }
        if (!same(src.R, prevR)) {
          nextEmby.ratings = Object.assign({}, nextEmby.ratings || {}, { libraries: src.R || [] });
          changed = true;
        }
        if (!same(src.S, prevS)) {
          nextEmby.scrobble = Object.assign({}, nextEmby.scrobble || {}, { libraries: src.S || [] });
          changed = true;
        }
      }
    } catch (e) {
      console.warn("saveSettings: emby merge failed", e);
    }
    try {
      const instRaw = norm(document.getElementById("plex_instance")?.value || "");
      const inst = (instRaw && instRaw.toLowerCase() !== "default") ? instRaw : "default";

      const baseSrv = (serverCfg?.plex && typeof serverCfg.plex === "object") ? serverCfg.plex : {};
      const prevPlex = inst === "default"
        ? baseSrv
        : ((baseSrv.instances && typeof baseSrv.instances === "object" && baseSrv.instances[inst]) ? baseSrv.instances[inst] : {});

      const baseCfg = (cfg.plex && typeof cfg.plex === "object") ? cfg.plex : (cfg.plex = {});
      const nextPlex = (() => {
        if (inst === "default") return baseCfg;
        if (!baseCfg.instances || typeof baseCfg.instances !== "object") baseCfg.instances = {};
        if (!baseCfg.instances[inst] || typeof baseCfg.instances[inst] !== "object") baseCfg.instances[inst] = {};
        return baseCfg.instances[inst];
      })();

      const uiUrl  = norm(document.getElementById("plex_server_url")?.value || "");
      const uiUser = norm(document.getElementById("plex_username")?.value   || "");
      const uiAidS = norm(document.getElementById("plex_account_id")?.value || "");

      let uiAid = null;
      if (uiAidS !== "") {
        const n = parseInt(uiAidS, 10);
        uiAid = Number.isFinite(n) && n > 0 ? n : 1;
      }

      const prevUrl    = norm(prevPlex?.server_url);
      const prevUser   = norm(prevPlex?.username);
      const prevAidRaw = prevPlex?.account_id;
      const prevAid    = Number.isFinite(prevAidRaw) && prevAidRaw > 0 ? prevAidRaw : 1;

      if (uiUrl && uiUrl !== prevUrl) {
        nextPlex.server_url = uiUrl;
        changed = true;
      }
      if (uiUser && uiUser !== prevUser) {
        nextPlex.username = uiUser;
        changed = true;
      }

      if (uiAid !== null) {
        if (uiAid !== prevAid) {
          nextPlex.account_id = uiAid;
          changed = true;
        }
      } else if (!Number.isFinite(prevAidRaw) || prevAidRaw <= 0) {
        nextPlex.account_id = 1;
        changed = true;
      }

      const uiVerify = !!document.getElementById("plex_verify_ssl")?.checked;
      const prevVerify = !!(prevPlex?.verify_ssl);
      if (uiVerify !== prevVerify) {
        nextPlex.verify_ssl = uiVerify;
        changed = true;
      }

      const plexHydrated =
        window.__plexHydrated === true ||
        document.getElementById("sec-plex")?.dataset?.hydrated === "1" ||
        document.querySelectorAll("#plex_lib_matrix .lm-row").length > 0 ||
        document.querySelectorAll("#plex_lib_whitelist .whrow").length > 0 ||
        !!document.querySelector("#plex_lib_history option, #plex_lib_ratings option, #plex_lib_scrobble option");

      if (plexHydrated) {
        const st = (window.__plexState || { hist: new Set(), rate: new Set(), scr: new Set() });
        const toNums = (xs) =>
          (Array.isArray(xs) ? xs : xs instanceof Set ? Array.from(xs) : [])
            .map(x => parseInt(String(x), 10))
            .filter(Number.isFinite);

        const hist = toNums(st.hist);
        const rate = toNums(st.rate);
        const scr  = toNums(st.scr);

        const _same = (a, b) => {
          const A = (a || []).map(Number).sort((x,y)=>x-y);
          const B = (b || []).map(Number).sort((x,y)=>x-y);
          if (A.length !== B.length) return false;
          for (let i=0;i<A.length;i++) if (A[i] !== B[i]) return false;
          return true;
        };

        const prevHist = (prevPlex?.history?.libraries || []).map(Number);
        const prevRate = (prevPlex?.ratings?.libraries || []).map(Number);
        const prevScr  = (prevPlex?.scrobble?.libraries || []).map(Number);

        if (!_same(hist, prevHist)) {
          nextPlex.history = Object.assign({}, nextPlex.history || {}, { libraries: hist });
          changed = true;
        }
        if (!_same(rate, prevRate)) {
          nextPlex.ratings = Object.assign({}, nextPlex.ratings || {}, { libraries: rate });
          changed = true;
        }
        if (!_same(scr, prevScr)) {
          nextPlex.scrobble = Object.assign({}, nextPlex.scrobble || {}, { libraries: scr });
          changed = true;
        }
      }
    } catch (e) {
      console.warn("saveSettings: plex merge failed", e);
    }
    
    try {
      if (typeof window.getScrobbleConfig === "function") {
        const prev = serverCfg?.scrobble || {};
        const next = window.getScrobbleConfig(prev) || {};
        if (JSON.stringify(next) !== JSON.stringify(prev)) {
          cfg.scrobble = next;
          changed = true;
        }
      }
    } catch (e) {
      console.warn("saveSettings: scrobbler merge failed", e);
    }

    
    try {
      let sched = {};
      if (typeof window.getSchedulingPatch === "function") {
        sched = window.getSchedulingPatch() || {};
      } else {
        sched = {
          enabled: readToggle("schEnabled"),
          mode: _getVal("schMode"),
          every_n_hours: parseInt((_getVal("schN") || "2"), 10),
          daily_time: _getVal("schTime") || "03:30",
          advanced: { enabled: false, jobs: [] }
        };
      }
      const prevSched = serverCfg?.scheduling || {};
      if (JSON.stringify(sched) !== JSON.stringify(prevSched)) {
        cfg.scheduling = sched;
        changed = true;
        schedChanged = true;
      }
    } catch (e) {
      console.warn("saveSettings: scheduling merge failed", e);
    }

    if (changed) {
      const postCfg = await fetch("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(cfg),
      });
      if (!postCfg.ok) throw new Error(`POST /api/config ${postCfg.status}`);

      try { if (typeof loadConfig === "function") await loadConfig(); } catch {}
      try { if (typeof _invalidatePairsCache === "function") _invalidatePairsCache(); } catch {}

      if (schedChanged) {
        try {
          await fetch("/api/scheduling", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(cfg.scheduling),
            cache: "no-store"
          });
        } catch (e) {
          console.warn("POST /api/scheduling failed", e);
        }
      } else {
        try { await fetch("/api/scheduling/replan_now", { method: "POST", cache: "no-store" }); } catch {}
      }
    }

    
    try {
      if (typeof refreshPairedProviders === "function") await refreshPairedProviders(0);
      const cached = (typeof loadStatusCache === "function") ? loadStatusCache() : null;
      if (cached?.providers && typeof renderConnectorStatus === "function") {
        renderConnectorStatus(cached.providers, { stale: true });
      }
      if (typeof refreshStatus === "function") await refreshStatus(true);
    } catch {}

    try { if (typeof updateTmdbHint === "function") updateTmdbHint(); } catch {}
    try { if (typeof updateSimklState === "function") updateSimklState(); } catch {}
    try { if (typeof updateJellyfinState === "function") updateJellyfinState(); } catch {}

    try {
      if (typeof window.loadScheduling === "function") {
        await window.loadScheduling();
      } else {
        document.dispatchEvent(new CustomEvent("config-saved", { detail: { section: "scheduling" } }));
        document.dispatchEvent(new Event("scheduling-status-refresh"));
      }
    } catch (e) {
      console.warn("loadScheduling failed:", e);
    }

    try { if (typeof updateTraktHint === "function") updateTraktHint(); } catch {}
    try { if (typeof updatePreviewVisibility === "function") updatePreviewVisibility(); } catch {}

    try {
      window.dispatchEvent(new CustomEvent("settings-changed", {
        detail: { scope: "settings", reason: "save" }
      }));
    } catch {}

    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}

    try { document.dispatchEvent(new CustomEvent("config-saved", { detail: { section: "scheduling" } })); } catch {}
    try { document.dispatchEvent(new Event("scheduling-status-refresh")); } catch {}

    try { if (typeof window.refreshSchedulingBanner === "function") await window.refreshSchedulingBanner(); } catch {}
    try { if (typeof window.refreshSettingsInsight === "function") window.refreshSettingsInsight(); } catch {}

    if (!fromFab) showToast("Settings saved", true);

    (function () {
      const reasons = [];
      let kind = "";
      let applyText = "Restart NOW";

      if (window.__cwProtoChanged) {
        const wantProto = String(window.__cwProtoChanged || "").trim().toLowerCase();
        try { delete window.__cwProtoChanged; } catch {}
        const url = cwBuildProtoUrl(wantProto);
        try { cwQueueProtocolApply(wantProto, url); } catch {}
        reasons.push("Protocol changed");
        kind = "protocol";
        applyText = "Apply NOW";
      }

      let askAiInfo = null;
      if (window.__cwAskAiChanged) {
        askAiInfo = window.__cwAskAiChanged;
        try { delete window.__cwAskAiChanged; } catch {}
        try {
          const to = !!(askAiInfo && askAiInfo.to);
          reasons.push(`ASK AI ${to ? "shown" : "hidden"}`);
        } catch {
          reasons.push("ASK AI changed");
        }
        if (!kind) kind = "restart";
      }

      if (!reasons.length) return;

      const msg = `${reasons.join(" + ")}: restart required`;
      try { cwShowRestartBanner(msg, { showApply: true, applyText, kind }); } catch {}
      showToast(msg, true);
    })();
  } catch (err) {
    console.error("saveSettings failed", err);
    showToast("Save failed — see console", false);
    throw err;
  }
}


async function clearState() {
  const btnText = "Clear State";
  try {
    const r = await fetch("/api/maintenance/reset-state", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "clear_both" }) 
    });
    const j = await r.json();
    const m = document.getElementById("tb_msg");
    m.classList.remove("hidden");
    m.textContent = j.ok ? btnText + " – started ✓" : btnText + " – failed";
    setTimeout(() => m.classList.add("hidden"), 1600);
    console.log("Reset:", j);
  } catch (_) {}
}

async function clearCache() {
  const btnText = "Clear Cache";
  try {
    const r = await fetch("/api/maintenance/clear-cache", { method: "POST" });
    const j = await r.json();
    const m = document.getElementById("tb_msg");

    m.classList.remove("hidden");
    m.textContent = j.ok ? btnText + " – done ✓" : btnText + " – failed";

    setTimeout(() => m.classList.add("hidden"), 1600);
  } catch (_) {}
}

async function resetStats() {
  const btnText = "Reset Statistics";

  try {
    const r = await fetch("/api/maintenance/reset-stats", { method: "POST" });
    const j = await r.json();
    const m = document.getElementById("tb_msg");

    m.classList.remove("hidden");
    m.textContent = j.ok
      ? btnText + " – done ✓"
      : btnText + " – failed" + (j.error ? ` (${j.error})` : "");

    setTimeout(() => m.classList.add("hidden"), 2200);

    if (j.ok && typeof refreshStats === "function") refreshStats(true);
  } catch (e) {
    const m = document.getElementById("tb_msg");

    m.classList.remove("hidden");
    m.textContent = btnText + " – failed (network)";

    setTimeout(() => m.classList.add("hidden"), 2200);
  }
}

async function resetCurrentlyPlaying() {
  const btnText = "Reset Currently Playing";
  try {
    const r = await fetch("/api/maintenance/reset-currently-watching", {
      method: "POST"
    });
    const j = await r.json();
    const m = document.getElementById("tb_msg");
    if (!m) return;

    m.classList.remove("hidden");
    m.textContent = j.ok
      ? btnText + " – done ✓"
      : btnText + " – failed" + (j.error ? ` (${j.error})` : "");
    setTimeout(() => m.classList.add("hidden"), 2200);
  } catch (_) {
    const m = document.getElementById("tb_msg");
    if (!m) return;
    m.classList.remove("hidden");
    m.textContent = btnText + " – failed (network)";
    setTimeout(() => m.classList.add("hidden"), 2200);
  }
}

async function restartCrossWatch() {
  if (typeof cwRestartCrossWatchWithOverlay === "function") return cwRestartCrossWatchWithOverlay();
  try { await fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" }); } catch {}
  try { window.location.reload(); } catch {}
}


async function updateTmdbHint() {
  const hint = document.getElementById("tmdb_hint");
  const input = document.getElementById("tmdb_api_key");

  if (!hint || !input) return;

  const settingsVisible = !document
    .getElementById("page-settings")
    ?.classList.contains("hidden");

  if (!settingsVisible) return;

  const v = (input.value || "").trim();

  if (document.activeElement === input) input.dataset.dirty = "1";

  if (input.dataset.dirty === "1") {
    hint.classList.toggle("hidden", !!v);
    return;
  }

  if (v) {
    hint.classList.add("hidden");
    return;
  }

  try {
    const cfg = await fetch("/api/config", { cache: "no-store" }).then((r) =>
      r.json()
    );

    const has = !!(cfg.tmdb?.api_key || "").trim();

    hint.classList.toggle("hidden", has);
  } catch {
    hint.classList.remove("hidden");
  }
}

function setTraktSuccess(show) {
  const el = document.getElementById("trakt_msg");
  if (el) el.classList.toggle("hidden", !show);
}

function isPlaceholder(v, ph) {
  return (v || "").trim().toUpperCase() === ph.toUpperCase();
}

function isSettingsVisible() {
  const el = document.getElementById("page-settings");
  return !!(el && !el.classList.contains("hidden"));
}
function setBtnBusy(id, busy) {
  const el = document.getElementById(id);
  if (!el) return;
  el.disabled = !!busy;
  el.classList.toggle("opacity-50", !!busy);
}

function flashBtnOK(btnEl) {
  if (!btnEl) return;
  btnEl.disabled = true;
  btnEl.classList.add("copied"); 
  setTimeout(() => {
    btnEl.classList.remove("copied");
    btnEl.disabled = false;
  }, 700);
}

document.addEventListener("DOMContentLoaded", () => {
  
  document
    .getElementById("btn-copy-plex-pin")
    ?.addEventListener("click", (e) =>
      copyInputValue("plex_pin", e.currentTarget)
    );

  document
    .getElementById("btn-copy-plex-token")
    ?.addEventListener("click", (e) =>
      copyInputValue("plex_token", e.currentTarget)
    );

  
  document
    .getElementById("btn-copy-trakt-pin")
    ?.addEventListener("click", (e) =>
      copyInputValue("trakt_pin", e.currentTarget)
    );

  document
    .getElementById("btn-copy-trakt-token")
    ?.addEventListener("click", (e) =>
      copyInputValue("trakt_token", e.currentTarget)
    );
});

function updateEdges() {
  const row = document.getElementById("poster-row");

  const L = document.getElementById("edgeL"),
    R = document.getElementById("edgeR");

  const max = row.scrollWidth - row.clientWidth - 1;

  L.classList.toggle("hide", row.scrollLeft <= 0);

  R.classList.toggle("hide", row.scrollLeft >= max);
}

function scrollWall(dir) {
  const row = document.getElementById("poster-row");

  const step = row.clientWidth;

  row.scrollBy({ left: dir * step, behavior: "smooth" });

  setTimeout(updateEdges, 350);
}

function initWallInteractions() {
  const row = document.getElementById("poster-row");

  row.addEventListener("scroll", updateEdges);

  row.addEventListener(
    "wheel",
    (e) => {
      if (Math.abs(e.deltaY) > Math.abs(e.deltaX)) {
        e.preventDefault();
        row.scrollBy({ left: e.deltaY, behavior: "auto" });
      }
    },
    { passive: false }
  );

  updateEdges();
}

function cxBrandInfo(name) {
  const key = String(name || "").toUpperCase();
  
  const map = {
    PLEX: { cls: "brand-plex", icon: "/assets/img/PLEX.svg" },
    SIMKL: { cls: "brand-simkl", icon: "/assets/img/SIMKL.svg" },
    TRAKT: { cls: "brand-trakt", icon: "/assets/img/TRAKT.svg" },
    ANILIST: { cls: "brand-anilist", icon: "/assets/img/ANILIST.svg" },
  };
  return map[key] || { cls: "", icon: "" };
}

function cxBrandLogo(providerName) {
  const key = (providerName || "").toUpperCase();
  const ICONS = {
    PLEX:  "/assets/img/PLEX.svg",
    SIMKL: "/assets/img/SIMKL.svg",
    TRAKT: "/assets/img/TRAKT.svg",
    ANILIST:"/assets/img/ANILIST.svg",
    TMDB:  "/assets/img/TMDB.svg",
    JELLYFIN: "/assets/img/JELLYFIN.svg",
    EMBY: "/assets/img/EMBY.svg",
  };
  const src = ICONS[key];
  return src
    ? `<img class="token-logo" src="${src}" alt="${key} logo" width="28" height="28" loading="lazy">`
    : `<span class="token-text">${providerName || ""}</span>`;
}

function updateFlowRailLogos() {
  const keyOf = id => (document.getElementById(id)?.value || '')
                      .trim()
                      .toUpperCase();

  const srcKey = keyOf('cx-src');
  const dstKey = keyOf('cx-dst');

  const rail = document.querySelector('.flow-rail.pretty');
  if (!rail) return;

  const tokens = rail.querySelectorAll('.token');
  if (!tokens.length) return;

  const setToken = (el, key) => {
    el.innerHTML = key
      ? `<img class="token-logo" src="/assets/img/${key}.svg" alt="${key}">`
      : '';
  };

  setToken(tokens[0], srcKey);
  if (tokens[1]) setToken(tokens[1], dstKey);
}

document.addEventListener('DOMContentLoaded', updateFlowRailLogos);
['cx-src', 'cx-dst'].forEach(id =>
  document.getElementById(id)?.addEventListener('change', updateFlowRailLogos)
);

function artUrl(item, size) {
  const typ = isTV(item.type || item.entity || item.media_type) ? "tv" : "movie";
  const tmdb = item.tmdb;
  if (!tmdb) return null;
  const cb = window._lastSyncEpoch || 0;
  return `/art/tmdb/${typ}/${tmdb}?size=${encodeURIComponent(
    size || "w342"
  )}&cb=${cb}`;
}

/*! Watchlist preview */
async function loadWall() {
  try {
    const card = document.getElementById("placeholder-card");
    const [wlEnabled, hasKey, uiAllowed] = await Promise.all([
      typeof isWatchlistEnabledInPairs === "function" ? isWatchlistEnabledInPairs() : true,
      typeof hasTmdbKey === "function" ? hasTmdbKey() : true,
      typeof isWatchlistPreviewAllowed === "function" ? isWatchlistPreviewAllowed() : true
    ]);
    if (!wlEnabled || !hasKey || !uiAllowed) {
      if (card) card.classList.add("hidden");
      return;
    }
    if (card) card.classList.remove("hidden");
  } catch (_) {}

  const myReq = ++wallReqSeq;
  const card = document.getElementById("placeholder-card");
  const msg  = document.getElementById("wall-msg");
  const row  = document.getElementById("poster-row");

  msg.textContent = "Loading…";
  row.innerHTML = "";
  row.classList.add("hidden");
  card.classList.remove("hidden");

  const hiddenMap = new Map(
    (JSON.parse(localStorage.getItem("wl_hidden") || "[]") || []).map(k => [k, true])
  );
  const isLocallyHidden = (k) => hiddenMap.has(k);

  const isDeleted = (item) => {
    if (isLocallyHidden(item.key) && item.status === "deleted") return true;
    if (isLocallyHidden(item.key) && item.status !== "deleted") {
      hiddenMap.delete(item.key);
      localStorage.setItem("wl_hidden", JSON.stringify([...hiddenMap.keys()]));
    }
    return (window._deletedKeys && window._deletedKeys.has(item.key)) || false;
  };

  
  function pillFor(status) {
    switch (String(status || "").toLowerCase()) {
      case "deleted":    return { text: "DELETED", cls: "p-del" };
      case "both":       return { text: "SYNCED",  cls: "p-syn" };
      case "plex_only":  return { text: "PLEX",    cls: "p-px" };
      case "simkl_only": return { text: "SIMKL",   cls: "p-sk" };
      case "trakt_only": return { text: "TRAKT",   cls: "p-tr" };
      case "anilist_only": return { text: "ANILIST", cls: "p-al" };
      case "jellyfin_only": return { text: "JELLYFIN", cls: "p-sk" };
      case "crosswatch_only": return { text: "CW", cls: "p-sk" };
      case "cw_only":         return { text: "CW", cls: "p-sk" };
      default:           return { text: "—",       cls: "p-sk" };
    }
  }

  try {
    const data = await fetch("/api/state/wall?both_only=0&active_only=1", { cache: "no-store" }).then(r => r.json());
    if (myReq !== wallReqSeq) return;

    if (data.missing_tmdb_key) { card.classList.add("hidden"); return; }
    if (!data.ok) { msg.textContent = data.error || "No state data found."; return; }

    let items = data.items || [];
    if (!items.length && Array.isArray(data.items)) {
      items = (data.items || []).filter(it => String(it.status || "").toLowerCase() === "both");
    }

    _lastSyncEpoch = data.last_sync_epoch || null;

    if (items.length === 0) { msg.textContent = "No items to show yet."; return; }

    msg.classList.add("hidden");
    row.classList.remove("hidden");

    
    const firstSeen = (() => {
      try { return JSON.parse(localStorage.getItem("wl_first_seen") || "{}"); }
      catch { return {}; }
    })();
    const getTs = (it) => {
      const s = it.added_epoch ?? it.added_ts ?? it.created_ts ?? it.created ?? it.epoch ?? null;
      return Number(s || firstSeen[it.key] || 0);
    };
    const now = Date.now();
    for (const it of items) if (!firstSeen[it.key]) firstSeen[it.key] = now;
    localStorage.setItem("wl_first_seen", JSON.stringify(firstSeen));

    
    items = items.slice().sort((a, b) => getTs(b) - getTs(a));

    
    const MAX = Number.isFinite(window.MAX_WALL_POSTERS) ? window.MAX_WALL_POSTERS : 20;
    items = items.slice(0, MAX);

    for (const it of items) {
      if (!it.tmdb) continue;

      const a = document.createElement("a");
      a.className = "poster";
      a.href = a.href = `https://www.themoviedb.org/${isTV(it.type) ? "tv" : "movie"}/${it.tmdb}`;
      a.target = "_blank";
      a.rel = "noopener";
      a.dataset.type = it.type;
      a.dataset.tmdb = String(it.tmdb);
      a.dataset.key  = it.key || "";

      const uiStatus = isDeleted(it) ? "deleted" : (it.status || "");
      a.dataset.source = uiStatus;

      const img = document.createElement("img");
      img.loading = "lazy";
      img.alt = `${it.title || ""} (${it.year || ""})`;
      img.src = artUrl(it, "w342");

      
      img.onerror = function () { this.onerror = null; this.src = "/assets/img/placeholder_poster.svg"; };

      a.appendChild(img);

      const ovr = document.createElement("div");
      ovr.className = "ovr";
      const pill = document.createElement("div");
      const p = pillFor(uiStatus);
      pill.className = "pill " + p.cls;
      pill.textContent = p.text;
      ovr.appendChild(pill);
      a.appendChild(ovr);

      const cap = document.createElement("div");
      cap.className = "cap";
      cap.textContent = `${it.title || ""} ${it.year ? "· " + it.year : ""}`;
      a.appendChild(cap);

      const hover = document.createElement("div");
      hover.className = "hover";
      hover.innerHTML = `
        <div class="titleline">${it.title || ""}</div>
        <div class="meta">
          <div class="chip time" id="time-${it.type}-${it.tmdb}">${_lastSyncEpoch ? "updated " + relTimeFromEpoch(_lastSyncEpoch) : ""}</div>
        </div>`;
      a.appendChild(hover);

      a.addEventListener("mouseenter", async () => {
        const descEl = document.getElementById(`desc-${it.type}-${it.tmdb}`);
        if (!descEl || descEl.dataset.loaded) return;
        try {
          const entity = isTV(it.type) ? "tv" : "movie";
          const res = await fetch("/api/metadata/resolve", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ entity, ids: { tmdb: String(it.tmdb) }, need: { overview: true } })
          });
          const j = await res.json();
          const meta = j?.ok ? j.result : null;
          descEl.textContent = meta?.overview || "—";
          descEl.dataset.loaded = "1";
        } catch {
          descEl.textContent = "—";
          descEl.dataset.loaded = "1";
        }
      }, { passive: true });

      row.appendChild(a);
    }

    initWallInteractions();

  } catch {
    msg.textContent = "Failed to load preview.";
  }
}

async function updateWatchlistPreview() {
  try {
    const [hasKey, wlEnabled, uiAllowed] = await Promise.all([
      hasTmdbKey?.(),
      isWatchlistEnabledInPairs?.(),
      isWatchlistPreviewAllowed?.()
    ]);
    const card = document.getElementById("placeholder-card");
    if (!hasKey || !wlEnabled || !uiAllowed) {
      if (card) card.classList.add("hidden");
      window.wallLoaded = false;
      return;
    }
    await loadWall();
    window.wallLoaded = true;
  } catch (e) {
    console.error("Failed to update watchlist preview:", e);
  }
}

async function hasTmdbKey(){
  try{
    if(window._cfgCache) return !!(window._cfgCache.tmdb?.api_key||"").trim();
    const cfg=await fetch("/api/config").then(r=>r.json());
    window._cfgCache=cfg;
    return !!(cfg.tmdb?.api_key||"").trim();
  }catch{ return false; }
}

function isOnMain(){
  var t = (document.documentElement.dataset.tab || "").toLowerCase();
  if (t) return t === "main";
  var th = document.getElementById("tab-main");
  return !!(th && th.classList.contains("active"));
}

async function isWatchlistPreviewAllowed(){
  try {
    if (window._cfgCache) {
      const ui = window._cfgCache.ui || window._cfgCache.user_interface || {};
      if (typeof ui.show_watchlist_preview === "boolean") return !!ui.show_watchlist_preview;
    }
    const cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
    window._cfgCache = cfg;
    const ui = cfg.ui || cfg.user_interface || {};
    if (typeof ui.show_watchlist_preview === "boolean") return !!ui.show_watchlist_preview;
  } catch (e) {
    console.warn("isWatchlistPreviewAllowed failed, falling back to true", e);
  }
  return true;
}


let __uPVBusy = false;
window.__wallLoading = window.__wallLoading || false;

async function updatePreviewVisibility() {
  if (__uPVBusy) return false;
  __uPVBusy = true;
  try {
    const card = document.getElementById("placeholder-card");
    const row  = document.getElementById("poster-row");
    const msg  = document.getElementById("wall-msg");
    if (!card) return false;

    const hideAll = () => {
      if (!card.classList.contains("hidden")) card.classList.add("hidden");
      if (row) { row.innerHTML = ""; if (!row.classList.contains("hidden")) row.classList.add("hidden"); }
      if (msg) msg.textContent = "";
      window.wallLoaded = false;
    };

    if (!isOnMain?.()) { hideAll(); return false; }

    let hasKey = false, wlEnabled = false, uiAllowed = true;
    try { hasKey = await hasTmdbKey?.(); } catch {}
    try { wlEnabled = await isWatchlistEnabledInPairs?.(); } catch {}
    try { uiAllowed = await isWatchlistPreviewAllowed?.(); } catch {}

    if (!hasKey || !wlEnabled || !uiAllowed) { hideAll(); return false; }
    if (card.classList.contains("hidden")) card.classList.remove("hidden");

    if (!window.wallLoaded && !window.__wallLoading) {
      window.__wallLoading = true;
      try { await loadWall?.(); window.wallLoaded = true; } catch {}
      finally { window.__wallLoading = false; }
    }
    return true;
  } finally {
    __uPVBusy = false;
  }
}

try { window.updatePreviewVisibility = updatePreviewVisibility; } catch (e) {}


showTab("main");

let _bootPreviewTriggered = false;
window.wallLoaded = false;

window.addEventListener("storage", (event) => {
  if (event.key === "wl_hidden") {
    updatePreviewVisibility();
    window.dispatchEvent(new CustomEvent("watchlist-hidden-changed"));
  }
});

async function resolvePosterUrl(entity, id, size = "w342") {
  
  if (!id) return null;
  if (window._cfgCache && !String(window._cfgCache?.tmdb?.api_key||"").trim()) return null;

  const typ = isTV(entity) ? "tv" : "movie";
  const cb = window._lastSyncEpoch || 0;

  try {
    const res = await fetch("/api/metadata/resolve", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        entity: typ,
        ids: { tmdb: String(id) },
        need: { poster: true }
      })
    });
    if (!res.ok) return null;

    const j = await res.json();
    const meta = j && j.ok ? j.result : null;
    if (!meta?.images?.poster?.length) return null;

    
    return `/art/tmdb/${typ}/${id}?size=${encodeURIComponent(size)}&cb=${cb}`;
  } catch {
    return null;
  }
}

async function mountAuthProviders() {
  try {
    const res = await fetch("/api/auth/providers/html", { cache: "no-store" });
    if (!res.ok) return;

    const html = await res.text();
    const slot = document.getElementById("auth-providers");
    if (slot) slot.innerHTML = html;

    window.initMDBListAuthUI?.();
    window.initTautulliAuthUI?.();
    window.initAniListAuthUI?.();

    try {
      const root = document.getElementById("auth-providers");
      if (root) {
        const want = ["media servers", "trackers", "others"];
        root.querySelectorAll(".section").forEach((sec) => {
          const t = (
            sec.querySelector(":scope > .head strong")?.textContent ||
            sec.querySelector(":scope > .head")?.textContent ||
            ""
          ).trim().toLowerCase();
          if (want.some((w) => t.startsWith(w))) {
            sec.classList.remove("open");
            const chev = sec.querySelector(":scope > .head .chev");
            if (chev) chev.textContent = "▶";
          }
        });
        root.querySelectorAll("details").forEach((det) => {
          const t = (det.querySelector(":scope > summary")?.textContent || "").trim().toLowerCase();
          if (want.some((w) => t.startsWith(w))) det.open = false;
        });
      }
    } catch (_) {}


    document.getElementById("btn-copy-plex-pin")
      ?.addEventListener("click", (e) => copyInputValue?.("plex_pin", e.currentTarget));
    document.getElementById("btn-copy-plex-token")
      ?.addEventListener("click", (e) => copyInputValue?.("plex_token", e.currentTarget));
    document.getElementById("btn-copy-trakt-pin")
      ?.addEventListener("click", (e) => copyInputValue?.("trakt_pin", e.currentTarget));
    document.getElementById("btn-copy-trakt-token")
      ?.addEventListener("click", (e) => copyInputValue?.("trakt_token", e.currentTarget));

    document.getElementById("trakt_client_id")
      ?.addEventListener("input", () => window.updateTraktHint?.());
    document.getElementById("trakt_client_secret")
      ?.addEventListener("input", () => window.updateTraktHint?.());

    await window.hydrateAuthFromConfig?.();
    window.updateTraktHint?.();
    window.startTraktTokenPoll?.();

    setTimeout(() => window.updateTraktHint?.(), 0);
    requestAnimationFrame(() => window.updateTraktHint?.());
  } catch (e) {
    console.warn("mountAuthProviders failed", e);
  }
}

async function mountMetadataProviders() {
  try {
    const res = await fetch("/api/metadata/providers/html");
    if (!res.ok) return;
    const html = await res.text();

    const raw = document.getElementById("meta-provider-raw");
    if (raw) raw.innerHTML = html;
    else {
      const slot = document.getElementById("metadata-providers");
      if (slot) slot.innerHTML = html;
    }

    try { cwMetaProviderEnsure?.(); } catch (_) {}

    try { updateTmdbHint?.(); } catch (_) {}
    try { cwMetaProviderUpdateChips?.(); } catch (_) {}
  } catch (e) {}
}


document.addEventListener("DOMContentLoaded", () => {
  try { mountMetadataProviders(); } catch (e) {}
});

try {
  const exportsObj = { showTab, renderConnections };
  if (typeof window.requestPlexPin === "function") {
    exportsObj.requestPlexPin = window.requestPlexPin;
  }
  Object.assign(window, exportsObj);
} catch (e) {
  console.warn("Global export failed", e);
}

if (typeof window.requestPlexPin !== "function") {
  window.requestPlexPin = function () {
    console.warn("requestPlexPin is not available yet — ensure auth.plex-simkl.js is loaded before crosswatch.js or call it later.");
  };
}


if (typeof updateSimklHint !== "function") {
  function updateSimklHint() {}
}



async function loadProviders() {
  const div = document.getElementById("providers_list");
  if (!div) return;

  let arr = [];
  try {
    arr = await fetch("/api/sync/providers", { cache: "no-store" })
      .then((r) => r.json())
      .catch(() => []);

    if (!Array.isArray(arr) || !arr.length) {
      div.innerHTML = '<div class="muted">No providers discovered.</div>';
      return;
    }

    
    const normKey = (s = "") => {
      s = String(s).toUpperCase();
      if (/\bPLEX\b/.test(s)) return "PLEX";
      if (/\bSIMKL\b/.test(s)) return "SIMKL";
      if (/\bTRAKT\b/.test(s)) return "TRAKT";
      if (/\bANILIST\b/.test(s)) return "ANILIST";
      if (/\bJELLYFIN\b/.test(s)) return "JELLYFIN";
      if (/\bEMBY\b/.test(s)) return "EMBY";
      return s;
    };

    const html = arr
      .map((p) => {
        const key = normKey(p.key || p.name || p.label);
        const caps = p.features || {};
        const chip = (t, on) =>
          `<span class="badge ${on ? "" : "feature-disabled"}" style="margin-left:6px">${t}</span>`;
        return `
          <div class="card prov-card" data-prov="${key}">
            <div style="padding:12px;display:flex;justify-content:space-between;align-items:center">
              <div class="title" style="font-weight:700">${p.label || p.name || key}</div>
              <div>
                ${chip("Watchlist", !!caps.watchlist)}
                ${chip("Ratings",   !!caps.ratings)}
                ${chip("History",   !!caps.history)}
                ${chip("Playlists", !!caps.playlists)}
              </div>
            </div>
          </div>`;
      })
      .join("");

    div.innerHTML = html;
    window.cx = window.cx || {};
    window.cx.providers = Array.isArray(arr) ? arr : [];

    try {
      if (typeof renderConnections === "function") renderConnections();
    } catch (e) {
      console.warn("renderConnections failed", e);
    }
  } catch (e) {
    div.innerHTML = '<div class="muted">Failed to load providers.</div>';
    console.warn("loadProviders error", e);
  } finally {

    try {
      if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
      else if (typeof applySyncVisibility === "function") applySyncVisibility();
    } catch {}
  }
}

(function () {
  try { window.addPair = addPair; } catch (e) {}
  try { window.savePairs = savePairs; } catch (e) {}
  try { window.deletePair = deletePair; } catch (e) {}
  try { window.loadPairs = loadPairs; } catch (e) {}

  try { window.addBatch = addBatch; } catch (e) {}
  try { window.saveBatches = saveBatches; } catch (e) {}
  try { window.loadBatches = loadBatches; } catch (e) {}
  try { window.runAllBatches = runAllBatches; } catch (e) {}

  try { window.loadProviders = loadProviders; } catch (e) {}
})();

try { window.showTab = showTab; } catch (e) {}
try { window.runSync = runSync; } catch (e) {}

window.cx = window.cx || {
  providers: [],
  pairs: [],
  connect: { source: null, target: null },
};

function _cap(obj, key) {
  try {
    return !!(obj && obj.features && obj.features[key]);
  } catch (_) {
    return false;
  }
}
function _byName(list, name) {
  name = String(name || "").toUpperCase();
  return (list || []).find((p) => String(p.name || "").toUpperCase() === name);
}
function _normWatchlistFeature(val) {
  if (val && typeof val === "object")
    return { add: !!val.add, remove: !!val.remove };
  return { add: !!val, remove: false };
}
function _pairFeatureObj(pair) {
  const f = (pair && pair.features) || {};
  return { watchlist: _normWatchlistFeature(f.watchlist) };
}

function renderConnections() {
  try { document.dispatchEvent(new Event("cx-state-change")); } catch(_) {}
}



/*! Connections (pairs API) */
async function loadPairs() {
  try {
    const res = await fetch("/api/pairs", { cache: "no-store" });
    const arr = await res.json().catch(() => []);
    window.cx = window.cx || {};
    window.cx.pairs = Array.isArray(arr) ? arr : [];
    try { renderConnections(); } catch (_) {}
    return window.cx.pairs;
  } catch (e) {
    console.warn("[cx] loadPairs failed", e);
    return [];
  }
}

async function deletePair(id) {
  if (!id) return false;
  try {
    const res = await fetch(`/api/pairs/${encodeURIComponent(id)}`, { method: "DELETE" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    await loadPairs();
    return true;
  } catch (e) {
    console.warn("[cx] deletePair failed", e);
    return false;
  }
}

async function cxSavePair(data, editingId = "") {
  try {
    const src = String(data?.source || "").trim();
    const dst = String(data?.target || "").trim();
    const mode = String(data?.mode || "one-way").toLowerCase() === "two-way" ? "two-way" : "one-way";
    const enabled = data?.enabled !== false;

    const normBasic = (v) => ({ enable: !!v?.enable, add: !!v?.add, remove: !!v?.remove });
    const featuresIn = data?.features || {};
    const payload = {
      source: src,
      target: dst,
      enabled,
      mode,
      features: {
        watchlist: normBasic(featuresIn.watchlist),
        ratings:   { ...normBasic(featuresIn.ratings), types: Array.isArray(featuresIn.ratings?.types) ? featuresIn.ratings.types : undefined, mode: featuresIn.ratings?.mode, from_date: featuresIn.ratings?.from_date },
        history:   normBasic(featuresIn.history),
        playlists: normBasic(featuresIn.playlists),
      },
    };

    const id = String(editingId || data?.id || "").trim();
    const url = id ? `/api/pairs/${encodeURIComponent(id)}` : "/api/pairs";
    const method = id ? "PUT" : "POST";

    const res = await fetch(url, { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);

    await loadPairs();
    return { ok: true };
  } catch (e) {
    console.warn("[cx] cxSavePair failed", e);
    return { ok: false, error: String(e?.message || e) };
  }
}

window.addEventListener('cx:open-modal', function(ev){
  try{
    var detail = ev.detail || {};
    if (typeof window.cxOpenModalFor === 'function') {
      window.cxOpenModalFor(detail);
    }
  }catch(e){ console.warn('cx modal bridge failed', e); }
});



/*! Accessibility */
function fixFormLabels(root = document) {
  const ctrls = new Set(["INPUT","SELECT","TEXTAREA"]);
  let uid = 0;
  root.querySelectorAll("label").forEach(lab => {
    if (lab.hasAttribute("for")) return;
    const owned = lab.querySelector("input,select,textarea");
    if (owned) return; 
    
    let ctrl = lab.nextElementSibling;
    while (ctrl && !ctrl.matches?.("input,select,textarea")) {
      ctrl = ctrl.nextElementSibling;
    }
    if (!ctrl) ctrl = lab.parentElement?.querySelector?.("input,select,textarea");
    if (!ctrl) return;
    if (!ctrl.id) ctrl.id = "auto_lbl_" + (++uid);
    lab.setAttribute("for", ctrl.id);
  });
}
document.addEventListener("DOMContentLoaded", () => { try { fixFormLabels(); } catch(_){} });


/*! Boot */
try {
  Object.assign(window, {
    showTab,
    runSync,
    toggleSection,
    toggleDetails,
    downloadSummary,
    copySummary,
    refreshStatus,
    refreshStats,
    refreshInsights,
    scrollWall,
    updatePreviewVisibility,
    loadProviders,
    loadPairs,
    deletePair,
    cxSavePair,
    renderConnections,
    fixFormLabels,
  });
} catch (_) {}

document.addEventListener("DOMContentLoaded", () => {
  try { loadProviders(); } catch (_) {}
  try { loadPairs(); } catch (_) {}
  try { mountAuthProviders(); } catch (_) {}
  try { cwSchedProviderEnsure?.(); } catch (_) {}
  try {
    if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
    else if (typeof applySyncVisibility === "function") applySyncVisibility();
  } catch (_) {}
  try { cwInitPendingProtoBanner(); } catch (_) {}
});
