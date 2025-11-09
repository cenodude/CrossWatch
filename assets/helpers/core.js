/* assets/crosswatch.js *
/* This file needs to be refactored and split up over time. - its garbage right now. */
/* Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

/* Global showTab bootstrap (runs first) */
(function(){
  if (typeof window.showTab !== "function") {
    window.showTab = function(id){
      try {
        var pages = document.querySelectorAll("#page-main, #page-watchlist, #page-settings, .tab-page");
        pages.forEach(el => el.classList.add("hidden"));
        var target = document.getElementById("page-" + id) || document.getElementById(id);
        if (target) target.classList.remove("hidden");
        ["main","watchlist","settings"].forEach(name => {
          var th = document.getElementById("tab-" + name);
          if (th) th.classList.toggle("active", name === id);
        });
        // Track current tab (used by preview guard)
        var t = String(id || "").toLowerCase();
        document.documentElement.dataset.tab = t;
        if (document.body) document.body.dataset.tab = t;

        document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id } }));
      } catch(e) { console.warn("showTab bootstrap failed:", e); }
    };
  }
})();

// --- Helpers for common DOM tasks --------------------------------------------
// Treat anything TV-ish as "tv"
const isTV = v => /^(tv|show|shows|series|season|episode)$/i.test(String(v||""));

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

// --- Secret-field helpers: mask tokens and track safe state
function applyServerSecret(inputId, hasValue) {
  const el = document.getElementById(inputId);
  if (!el) return;
  el.value = hasValue ? "••••••••" : "";
  el.dataset.masked = hasValue ? "1" : "0";
  el.dataset.loaded = "1";   // ready
  el.dataset.touched = "";   // untouched
  el.dataset.clear = "";     // not requested to clear
}
function startSecretLoad(inputId) {
  const el = document.getElementById(inputId);
  if (!el) return;
  el.dataset.loaded = "0";   // loading
  el.dataset.touched = "";   // ignore edits until finished (UI can disable)
}
function finishSecretLoad(inputId, hasValue) {
  // Call when an async token fetch finishes
  applyServerSecret(inputId, !!hasValue);
}

// --- Determine which authentication providers are configured
function getConfiguredProviders(cfg = window._cfgCache || {}) {
  const S = new Set();
  const has = (v) => (typeof v === "string" ? v.trim().length > 0 : !!v);

  if (has(cfg?.plex?.account_token)) S.add("PLEX");
  if (has(cfg?.simkl?.access_token || cfg?.auth?.simkl?.access_token)) S.add("SIMKL");
  if (has(cfg?.trakt?.access_token || cfg?.auth?.trakt?.access_token)) S.add("TRAKT");
  if (has(cfg?.jellyfin?.access_token || cfg?.auth?.jellyfin?.access_token)) S.add("JELLYFIN");
  if (has(cfg?.emby?.access_token || cfg?.auth?.emby?.access_token)) S.add("EMBY");
  if (has(cfg?.mdblist?.api_key)) S.add("MDBLIST");

  return S;
}

// Resolve a provider key from a dynamic card/row element
function resolveProviderKeyFromNode(node) {
  // Prefer an explicit attribute when available (e.g. <div data-sync-prov="PLEX">...)</div>
  const attr = (node.getAttribute?.("data-sync-prov") || node.dataset?.syncProv || "").toUpperCase();
  if (attr) return attr;

  // Detect provider via logo alt text or data-logo attributes
  const img = node.querySelector?.('img[alt], .logo img[alt], [data-logo]');
  const alt = (img?.getAttribute?.('alt') || img?.dataset?.logo || "").toUpperCase();
  if (alt.includes("PLEX"))  return "PLEX";
  if (alt.includes("SIMKL")) return "SIMKL";
  if (alt.includes("TRAKT")) return "TRAKT";
  if (alt.includes("JELLYFIN")) return "JELLYFIN";
  if (alt.includes("EMBY")) return "EMBY";

  // Fallback: inspect common title/name containers, then full text
  const tnode = node.querySelector?.(".title,.name,header,strong,h3,h4");
  const txt = (tnode?.textContent || node.textContent || "").toUpperCase();
  if (/\bPLEX\b/.test(txt))  return "PLEX";
  if (/\bSIMKL\b/.test(txt)) return "SIMKL";
  if (/\bTRAKT\b/.test(txt)) return "TRAKT";
  if (/\bJELLYFIN\b/.test(txt)) return "JELLYFIN";
  if (/\bEMBY\b/.test(txt)) return "EMBY";

  return ""; // unknown
}

function applySyncVisibility() {
  const allowed = getConfiguredProviders();
  const host = document.getElementById("providers_list");
  if (!host) return;

  // Prefer overlay cards when present: .prov-card[data-prov]
  let cards = host.querySelectorAll(".prov-card");
  if (!cards || cards.length === 0) {
  // Fallback: older renderer card children
    cards = host.querySelectorAll(":scope > .card, :scope > *");
  }

  cards.forEach((card) => {
  // 1) Use overlay path (fast and explicit)
    let key = (card.getAttribute?.("data-prov") || card.dataset?.prov || "").toUpperCase();

  // 2) Fallback to heuristics on inner content (legacy renderer)
    if (!key) key = resolveProviderKeyFromNode(card);

    if (!key) return; // unknown container: leave it alone
    card.dataset.syncProv = key; // remember for next runs
    card.style.display = allowed.has(key) ? "" : "none";
  });

  // Rebuild provider pair selectors using only allowed providers
  const LABEL = { PLEX: "Plex", SIMKL: "SIMKL", TRAKT: "Trakt", JELLYFIN: "Jellyfin", EMBY: "Emby" };
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

    ["PLEX", "SIMKL", "TRAKT", "JELLYFIN", "EMBY"].forEach((k) => {
      if (!allowed.has(k)) return;
      const o = document.createElement("option");
      o.value = k; o.textContent = LABEL[k] || k;
      sel.appendChild(o);
    });

    if (prev && allowed.has(prev)) sel.value = prev;
    else if (hadPlaceholder) sel.value = "";
  });

}

// Debounced applySyncVisibility using rAF or setTimeout
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

// Observe changes to the providers list and footer (where sync settings may be toggled)
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
  // Initial pass
  scheduleApplySyncVisibility();
}

// ---- BEGIN Watchlist Preview visibility based on /api/pairs ----
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

// Expose for other modules to call after settings or sync
window.updatePreviewVisibility = updatePreviewVisibility;

const AUTO_STATUS = false; // DISABLED by default -- can be enabled for debugging -- WATCH OUT FOR API LIMITS!
let lastStatusMs = 0;
const STATUS_MIN_INTERVAL = 24 * 60 * 60 * 1000; // 24 hours

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

// --- status normalizer (zet alles naar {PLEX|SIMKL|TRAKT|JELLYFIN: {connected:boolean}})
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
    JELLYFIN:normOne(pick(p, "JELLYFIN")?? p.jellyfin_connected),
    EMBY:    normOne(pick(p, "EMBY")    ?? p.emby_connected),
    MDBLIST:  normOne(pick(p, "MDBLIST")  ?? p.mdblist_connected),
  };
}

// cache helpers
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
    // still apply current visibility
    toggleProviderBadges(window._ui.pairedProviders);
    return window._ui.pairedProviders;
  }

  _pairsFetchAt = now;
  let pairs = [];
  try {
    const res = await fetch("/api/pairs", { cache: "no-store" });
    if (res.ok) pairs = await res.json();
  } catch (_) {}

  const active = { PLEX: false, SIMKL: false, TRAKT: false, JELLYFIN: false, EMBY: false };
  for (const p of pairs || []) {
    if (p && p.enabled !== false) {
      const s = String(p.source || "").toUpperCase();
      const t = String(p.target || "").toUpperCase();
      if (s in active) active[s] = true;
      if (t in active) active[t] = true;
    }
  }

  // Cache for reuse elsewhere
  window._ui = window._ui || {};
  window._ui.pairedProviders = active;

  toggleProviderBadges(active);
  return active;
}

// Hide/show badges by provider
function toggleProviderBadges(active){
  const map = { PLEX:"badge-plex", SIMKL:"badge-simkl", TRAKT:"badge-trakt", JELLYFIN:"badge-jellyfin", EMBY:"badge-emby", MDBLIST:"badge-mdblist" };
  for (const [prov,id] of Object.entries(map)){
    const el = document.getElementById(id);
    if (el) el.classList.toggle("hidden", !active?.[prov]);
  }
}

// Tri-state normalizer: "ok" | "no" | "unknown" (fallback)
function connState(v) {
  if (v == null) return "unknown";

  // Branch: boolean values
  if (v === true)  return "ok";
  if (v === false) return "no";

  // Branch: numeric values
  if (typeof v === "number") {
    if (v === 1) return "ok";
    if (v === 0) return "no";
  }

  // Branch: string values
  if (typeof v === "string") {
    const s = v.toLowerCase().trim();
    if (/^(ok|up|connected|ready|true|on|online|active)$/.test(s))   return "ok";
    if (/^(no|down|disconnected|false|off|disabled)$/.test(s))       return "no";
    if (/^(unknown|stale|n\/a|-|pending)$/.test(s))                  return "unknown";
    return "unknown";
  }

  // Branch: objects with common status keys (connected, ok, ready, etc.)
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

// Case-insensitive picker
function pickCase(obj, k) {
  return obj?.[k] ?? obj?.[k.toLowerCase()] ?? obj?.[k.toUpperCase()];
}

// --- tiny inline icons (inherit currentColor) --------------------------------
function svgCrown() {
  return '<svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M3 7l4 3 5-6 5 6 4-3v10H3zM5 15h14v2H5z"/></svg>';
}
function svgCheck() {
  return '<svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M9 16.2L5.5 12.7l1.4-1.4 2.1 2.1 6-6 1.4 1.4z"/></svg>';
}

/**
 * Render a connection badge. Adds a left "membership" tag for Plex Pass / Trakt VIP.
 * @param {string} id - Element id (e.g., 'badge-plex')
 * @param {string} providerName - Display name (e.g., 'Plex')
 * @param {'ok'|'no'|'unknown'} state - Connection state
 * @param {boolean} stale - When true, dim slightly
 * @param {string} [provKey] - 'PLEX' | 'TRAKT' | 'SIMKL' | 'JELLYFIN'
 * @param {object|boolean} [info] - Provider object as returned by /api/status.providers[provKey]
 */
function setBadge(id, providerName, state, stale, provKey, info) {
  const el = document.getElementById(id);
  if (!el) return;

  el.classList.remove("ok", "no", "unknown", "stale");
  el.classList.add(state);
  if (stale) el.classList.add("stale");
  // ensure new layout class is present
  el.classList.add("conn");

  // --- left capability tag (optional) ---------------------------------------
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

  // --- main text -------------------------------------------------------------
  const labelState = state === "ok" ? "Connected" : state === "no" ? "Not connected" : "Unknown";
  el.innerHTML =
    `${tag}<span class="txt">` +
      `<span class="dot ${state}"></span>` +
      `<span class="name">${providerName}</span>` +
      `<span class="state">· ${labelState}</span>` +
    `</span>`;
}

/**
 * Normalize providers and render all badges.
 * Accepts either booleans or objects: { connected: true, vip: ..., plexpass: ... }
 */
function renderConnectorStatus(providers, { stale = false } = {}) {
  const p = providers || {};
  const plex    = pickCase(p, "PLEX");   // boolean or {connected,...}
  const simkl   = pickCase(p, "SIMKL");
  const trakt   = pickCase(p, "TRAKT");
  const jelly   = pickCase(p, "JELLYFIN");
  const emby    = pickCase(p, "EMBY");
  const mdbl  = pickCase(p, "MDBLIST");

  setBadge("badge-plex",     "Plex",     connState(plex  ?? false), stale, "PLEX",     plex);
  setBadge("badge-simkl",    "SIMKL",    connState(simkl ?? false), stale, "SIMKL",    simkl);
  setBadge("badge-trakt",    "Trakt",    connState(trakt ?? false), stale, "TRAKT",    trakt);
  setBadge("badge-jellyfin", "Jellyfin", connState(jelly ?? false), stale, "JELLYFIN", jelly);
  setBadge("badge-emby",     "Emby",     connState(emby  ?? false), stale, "EMBY",     emby);
  setBadge("badge-mdblist",  "MDBlist",  connState(mdbl ?? false), stale, "MDBLIST", mdbl);
}

function fetchWithTimeout(url, opts = {}, ms = 15000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort("timeout"), ms);
  return fetch(url, { cache: "no-store", ...opts, signal: ac.signal })
    .finally(() => clearTimeout(t));
}

async function refreshStatus(force = false) {
  const now = Date.now();
  if (!force && typeof lastStatusMs !== "undefined" && typeof STATUS_MIN_INTERVAL !== "undefined" && (now - lastStatusMs < STATUS_MIN_INTERVAL)) return;
  if (typeof lastStatusMs !== "undefined") lastStatusMs = now;

  try {
    // 1) Update visibility first so badges hide/show instantly if pairs changed
    await refreshPairedProviders(force ? 0 : 5000);

    // 2) Fetch live status
    const r = await fetchWithTimeout("/api/status" + (force ? "?fresh=1" : ""), {}, 15000).then(r => r.json());
    if (typeof appDebug !== "undefined") appDebug = !!r.debug;

    const pick = (obj, k) => (obj?.[k] ?? obj?.[k.toLowerCase()] ?? obj?.[k.toUpperCase()]);
    const norm = (v, fb = false) => (typeof v === "boolean" ? { connected: v } : (v && typeof v === "object") ? v : { connected: !!fb });

    const pRaw = r.providers || {};
    const providers = {
      PLEX:     norm(pick(pRaw, "PLEX"),     (r.plex_connected    ?? r.plex)),
      SIMKL:    norm(pick(pRaw, "SIMKL"),    (r.simkl_connected   ?? r.simkl)),
      TRAKT:    norm(pick(pRaw, "TRAKT"),    (r.trakt_connected   ?? r.trakt)),
      JELLYFIN: norm(pick(pRaw, "JELLYFIN"), (r.jellyfin_connected?? r.jellyfin)),
      EMBY:     norm(pick(pRaw, "EMBY"),     (r.emby_connected    ?? r.emby)),
      MDBLIST:  norm(pick(pRaw, "MDBLIST"),  (r.mdblist_connected  ?? r.mdblist)),
    };

    renderConnectorStatus(providers, { stale: false });
    saveStatusCache?.(providers);

    window._ui = window._ui || {};
    window._ui.status = {
      can_run:            !!r.can_run,
      plex_connected:     !!(providers.PLEX?.connected     ?? providers.PLEX?.ok),
      simkl_connected:    !!(providers.SIMKL?.connected    ?? providers.SIMKL?.ok),
      trakt_connected:    !!(providers.TRAKT?.connected    ?? providers.TRAKT?.ok),
      jellyfin_connected: !!(providers.JELLYFIN?.connected ?? providers.JELLYFIN?.ok),
      emby_connected:     !!(providers.EMBY?.connected     ?? providers.EMBY?.ok),
      mdblist_connected:  !!(providers.MDBLIST?.connected  ?? providers.MDBLIST?.ok),
    };

    if (typeof recomputeRunDisabled === "function") recomputeRunDisabled?.();

    const onMain = !document.getElementById("ops-card").classList.contains("hidden");
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

// ---- bootstrap badges from cached status (runs once on load)
(function bootstrapStatusFromCache() {
  try {
    const cached = loadStatusCache();
    if (cached?.providers) {
      renderConnectorStatus(cached.providers, { stale: true });
    }
  } catch {}
  // Adjust badge visibility based on /api/pairs if that helper is available
  try { refreshPairedProviders?.(0); } catch {}
  // Then fetch live status to replace stale UI and refresh cache
  try { refreshStatus(true); } catch {}
})();

async function manualRefreshStatus() {
  if (manualRefreshStatus._inFlight) return;
  manualRefreshStatus._inFlight = true;

  const btn = document.getElementById("btn-status-refresh");
  btn?.classList.add("spin");
  setRefreshBusy?.(true);

  try {
    // Update visibility first so badges hide/show instantly if pairs changed
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
        JELLYFIN: { connected: !!s.jellyfin_connected },
        EMBY:     { connected: !!s.emby_connected },
        MDBLIST:  { connected: !!s.mdblist_connected },
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

// Bridge only; no UI here.
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
      (async () => { try { window.manualRefreshStatus?.(); } catch {} })(),
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
  try { await fetch("/api/debug/clear_probe_cache", { method: "POST", cache: "no-store" }); } catch {}
  try { if (typeof lastStatusMs !== "undefined") lastStatusMs = 0; } catch {}
  await refreshStatus(true);
  window.manualRefreshStatus?.();
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

/* Tabs & Navigation */
async function showTab(n) {
  document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id: n } }));

  const pageSettings  = document.getElementById("page-settings");
  const pageWatchlist = document.getElementById("page-watchlist");
  const logPanel      = document.getElementById("log-panel");
  const layout        = document.getElementById("layout");
  const statsCard     = document.getElementById("stats-card");
  const ph            = document.getElementById("placeholder-card");

  // Tab header state
  document.getElementById("tab-main")?.classList.toggle("active", n === "main");
  document.getElementById("tab-watchlist")?.classList.toggle("active", n === "watchlist");
  document.getElementById("tab-settings")?.classList.toggle("active", n === "settings");

  // Cards visibility
  document.getElementById("ops-card")?.classList.toggle("hidden", n !== "main");
  statsCard?.classList.toggle("hidden", n !== "main");
  if (ph && n !== "main") ph.classList.add("hidden");

  // Pages
  pageWatchlist?.classList.toggle("hidden", n !== "watchlist");
  pageSettings?.classList.toggle("hidden", n !== "settings");

  document.documentElement.dataset.tab = n;
  if (document.body) document.body.dataset.tab = n;

  // MAIN
  if (n === "main") {
    enforceMainLayout();
    if (__currentTab === "main") await softRefreshMain();
    else await hardRefreshMain({ layout, statsCard });
    logPanel?.classList.remove("hidden");
    // Reopen details log stream when returning to Main
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
        const base = new URL("./assets/js/watchlist.js", document.baseURI).href;
        const wlUrl = window.APP_VERSION ? `${base}?v=${encodeURIComponent(window.APP_VERSION)}` : base;

        try {
          await import(/* @vite-ignore */ wlUrl);
        } catch (_) {
          await new Promise((resolve, reject) => {
            const s = document.createElement("script");
            s.type = "module";
            s.src = wlUrl;
            s.onload = resolve;
            s.onerror = reject;
            document.head.appendChild(s);
          });
        }

        window.__watchlistLoaded = true;
      } else {
        // Subsequent visits
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


  // SETTINGS
  if (n === "settings") {
    layout?.classList.add("single");
    layout?.classList.remove("full");
    logPanel?.classList.add("hidden");

    try { await mountAuthProviders?.(); } catch {}
    try { await loadConfig(); } catch {}
    updateTmdbHint?.(); updateSimklHint?.(); updateSimklButtonState?.(); updateTraktHint?.(); startTraktTokenPoll?.();

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

// Keep main layout in sync for external triggers
document.addEventListener("tab-changed", e => {
  if (String(e?.detail?.id).toLowerCase() === "main") enforceMainLayout();
});


// --- Scrobbler UI mount: initialize once when the settings tab is shown and both PLEX + TRAKT are available
let __scrobInit = false;
function ensureScrobbler() {
  if (__scrobInit) return;

  const mount = document.getElementById("scrobble-mount") || document.getElementById("scrobbler");
  if (!mount) return;

  const prov = (typeof getConfiguredProviders === "function") ? getConfiguredProviders() : new Set();
  if (!(prov.has("PLEX") && prov.has("TRAKT"))) return;

  const start = () => {
    if (__scrobInit) return;
    if (window.Scrobbler?.init) {
      window.Scrobbler.init({ mountId: mount.id });
    } else if (window.Scrobbler?.mount) {
      window.Scrobbler.mount(mount, window._cfgCache || {});
    } else {
      return; // still not ready
    }
    __scrobInit = true;
  };

  if (window.Scrobbler) { start(); return; }

  // Otherwise, load the scrobbler script once and start onload
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

// ---- Run + Header progress UI helpers (drop-in) -----------------

function toggleSection(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle("open");
}
function setBusy(v) {
  busy = v;
  recomputeRunDisabled();
}

/* Run Sync (Trigger + bridge to UI) */
async function runSync(){
  if (busy) return;
  setBusy?.(true);

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
      try{ window.UX?.updateTimeline({ start:false, pre:false, post:false, done:false }); }catch{}
      return;
    }
  }catch(_){
    typeof setSyncHeader === "function" && setSyncHeader("sync-bad", "Failed to reach server");
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

/* Insights: Fetch & Render */
async function refreshInsights(){ if (window.Insights && window.Insights.refreshInsights) return window.Insights.refreshInsights.apply(this, arguments); }

function renderSparkline(){ if (window.Insights && window.Insights.renderSparkline) return window.Insights.renderSparkline.apply(this, arguments); }
document.addEventListener("DOMContentLoaded", refreshInsights);

// --- once-only bootstrap (no double timers) ---
(() => {
  // Use existing interval if defined; else 1 hour
  const INTERVAL =
    typeof UPDATE_CHECK_INTERVAL_MS === "number"
      ? UPDATE_CHECK_INTERVAL_MS
      : 60 * 60 * 1000;

  // Prevent duplicate init if script loads twice
  if (window.__cwUpdateInitDone) return;
  window.__cwUpdateInitDone = true;

  const run = () => { try { checkForUpdate(); } catch (e) { console.debug("checkForUpdate failed:", e); } };

  // Run on DOM ready (or immediately if already ready)
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

  // 1) Prefer the row containing the "Synchronize" button
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

  // 2) Fallback: place right before “Watchlist preview” header
  const previewHeader = [...document.querySelectorAll('h2, .section-title')].find(h => /watchlist\s*preview/i.test(h.textContent || ''));
  if (previewHeader && previewHeader.parentElement) {
    slot = document.createElement('div');
    slot.id = 'st-main-update';
    slot.className = 'hidden';
    previewHeader.insertAdjacentElement('beforebegin', slot);
    return slot;
  }

  // 3) Last fallback: top of main content
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

// Hook into your existing version check (keep your current header badge logic)
async function checkForUpdate() {
  try {
    const r = await fetch('/api/version', { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();

    const cur = String(j.current ?? '0.0.0').trim();
    const latest = j.latest ? String(j.latest).trim() : null;
    const url = j.html_url || 'https://github.com/cenodude/CrossWatch/releases';
    const hasUpdate = !!j.update_available;

    // Header badge (unchanged – your existing code)
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

    // Main pill (new placement)
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

  // Important: timeline/progress and run-button state are handled by main.js now.
  // No calls to setTimeline(), updateProgressFromTimeline(), or setRunProgress() here.
}

// After summary updates, keep stats view in step (lightweight hook)
(() => {
  const prev = window.renderSummary;
  window.renderSummary = function (sum) {
    try { prev?.(sum); } catch {}
    try { refreshStats(false); } catch {}
  };
})();

// (Removed) Summary Stream subscriber — main.js owns SSE and progress updates.
// function openSummaryStream() { ... }  ← intentionally deleted

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


/* Statistics Dashboard */
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

// Small buffer used to assemble Server-Sent Events (SSE) chunks
let detBuf = "";


function scanForEvents(chunk) {
  const lines = String(chunk).split('\n');
  for (const line of lines) {
    if (!line || line[0] !== '{') continue;
    try {
      const obj = JSON.parse(line);
      if (obj && obj.event) window.Progress?.onEvent(obj);
    } catch (_) { /* non-JSON line; ignore */ }
  }
}

// Progress mapper: SYNC events -> UI timeline/progress
window.Progress = (function () {
  let tl = { start: false, pre: false, post: false, done: false };
  const A = [0, 33, 66, 100]; // anchors used by main.js

  function emitTL() {
    (window.UX?.updateTimeline || window.setTimeline)?.(tl);
  }

  function setPhase(p) {
    tl = {
      start: true,
      pre: p !== "start",
      post: p === "post" || p === "done",
      done: p === "done",
    };
    emitTL();
  }

  function pushPct(done, total) {
    if (!total) return;
    const pct = Math.min(99, Math.floor(A[2] + (done / total) * (A[3] - A[2])));
    window.UX?.updateProgress?.({ pct });
  }

  function reset() {
    tl = { start: true, pre: false, post: false, done: false };
    emitTL();
    window.UX?.updateProgress?.({ pct: A[0] });
  }

  function onEvent(e) {
    if (!e || !e.event) return;

    switch (e.event) {
      // START
      case "run:start":
      case "run:pair":
      case "pair:start":
        reset();
        break;

      // DISCOVERING
      case "snapshot:start":
      case "plan":
        setPhase("pre");
        break;
      case "debug":
        if (e.msg && e.msg.startsWith("snapshot")) setPhase("pre");
        break;

      // SYNCING
      case "apply:start":
      case "apply:add:start":
      case "apply:remove:start":
      case "cascade:pre":
        setPhase("post");
        break;
      case "apply:add:progress":
      case "apply:remove:progress":
        setPhase("post");
        pushPct(+e.done || 0, +e.total || 0);
        break;
      case "apply:add:done":
      case "apply:remove:done":
      case "cascade:summary":
        setPhase("post");
        break;

      // DONE
      case "run:done":
        tl = { start: true, pre: true, post: true, done: true };
        emitTL();
        window.UX?.updateProgress?.({ pct: 100 });
        break;
    }
  }

  return { onEvent };
})();

// --- Details Log (live stream) -----------------
if (typeof window.esDet === "undefined") window.esDet = null;
if (typeof window.esDetSummary === "undefined") window.esDetSummary = null;
if (typeof window._detStaleIV === "undefined") window._detStaleIV = null;
if (typeof window._detRetryTO === "undefined") window._detRetryTO = null;
if (typeof window._detVisibilityHandler === "undefined") window._detVisibilityHandler = null;
if (typeof window.detStickBottom === "undefined") window.detStickBottom = true;

async function openDetailsLog() {
  const el = document.getElementById("det-log");
  const slider = document.getElementById("det-scrub");
  if (!el) return;

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

    window.esDet.onmessage = (ev) => {
      lastMsgAt = Date.now();
      if (!ev?.data) return;

      if (ev.data === "::CLEAR::") {
        el.textContent = "";
        detBuf = "";
        updateSlider();
        return;
      }

      try { scanForEvents(ev.data); } catch {}

      if (!useFormatter) {
        appendRaw(ev.data);
      } else {
        const { tokens, buf } = CF.processChunk(detBuf, ev.data);
        detBuf = buf;
        for (const tok of tokens) CF.renderInto(el, tok, false);
      }

      if (window.detStickBottom) el.scrollTop = el.scrollHeight;
      updateSlider();
      retryMs = 1000;
    };

    window.esDet.onerror = () => {
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
}

function toggleDetails() {
  const d = document.getElementById("details");
  d.classList.toggle("hidden");
  if (!d.classList.contains("hidden")) openDetailsLog();
  else closeDetailsLog();
}

window.addEventListener("beforeunload", closeDetailsLog);

function downloadSummary() {
  window.open("/api/run/summary/file", "_blank");
}

function setRefreshBusy(busy) {
  const btn = document.getElementById("btn-status-refresh");

  if (!btn) return;
  btn.disabled = !!busy;
  btn.classList.toggle("loading", !!busy);
}

//------------------------------------------------------------------

// --- Settings Page -----------------
window.openAbout = () => window.ModalRegistry.open('about');
window.cxEnsureCfgModal = window.cxEnsureCfgModal || function(){};

// Set value of input/select by ID (if visible)
window.wireSecretTouch = window.wireSecretTouch || function wireSecretTouch(id) {
  const el = document.getElementById(id);
  if (!el || el.__wiredTouch) return;
  el.addEventListener("input", () => {
    el.dataset.touched = "1";
    el.dataset.masked = "0";
  });
  el.__wiredTouch = true;
};

// Disable secret masking globally
window.maskSecret = function maskSecret(elOrId /*, hasValue */) {
  const el = typeof elOrId === "string" ? document.getElementById(elOrId) : elOrId;
  if (!el) return;
  // don't touch el.value at all
  el.dataset.masked  = "0";
  el.dataset.loaded  = "1";
  el.dataset.touched = "";
  el.dataset.clear   = "";
};

async function loadConfig() {
  // Fetch and cache config first; visibility helpers read from _cfgCache
  const cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
  window._cfgCache = cfg;

  // Bind observers once, then schedule initial visibility pass
  try { bindSyncVisibilityObservers?.(); } catch {}
  try {
    // Prefer debounced scheduler if present; fallback to direct apply
    if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
    else applySyncVisibility?.();
  } catch {}

  // --- Non-sensitive fields
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

  window.appDebug = !!(cfg.runtime && cfg.runtime.debug);

// --- Sensitive fields: inject RAW values from config (do not mark as touched)
(function hydrateSecretsRaw(cfg){
  const val = (x) => (typeof x === "string" ? x.trim() : "");
  const setRaw = (id, v) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.value = v || "";
    // make sure saveSettings() won't treat this as user-edit
    el.dataset.masked  = "0";
    el.dataset.loaded  = "1";
    el.dataset.touched = "";
    el.dataset.clear   = "";
    try { wireSecretTouch(id); } catch {}
  };

  // PLEX
  setRaw("plex_token", val(cfg.plex?.account_token));

  // SIMKL
  setRaw("simkl_client_id",     val(cfg.simkl?.client_id));
  setRaw("simkl_client_secret", val(cfg.simkl?.client_secret));
  setRaw("simkl_access_token",  val(cfg.simkl?.access_token) || val(cfg.auth?.simkl?.access_token));

  // TMDB
  setRaw("tmdb_api_key",        val(cfg.tmdb?.api_key));

  // MDBLIST
  setRaw("mdblist_key",         val(cfg.mdblist?.api_key));

  // TRAKT
  setRaw("trakt_client_id",     val(cfg.trakt?.client_id));
  setRaw("trakt_client_secret", val(cfg.trakt?.client_secret));
  setRaw("trakt_token",         val(cfg.trakt?.access_token) || val(cfg.auth?.trakt?.access_token));
})(cfg);

  // --- Legacy/basic scheduling (advanced UI can extend this)
  const s = cfg.scheduling || {};
  _setVal("schEnabled", String(!!s.enabled));
  _setVal("schMode",    typeof s.mode === "string" && s.mode ? s.mode : "hourly");
  _setVal("schN",       Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "2");
  _setVal("schTime",    typeof s.daily_time === "string" && s.daily_time ? s.daily_time : "03:30");
  if (document.getElementById("schTz")) _setVal("schTz", s.timezone || "");

  // UI helper hints
  try { updateSimklButtonState?.(); } catch {}
  try { updateSimklHint?.();      } catch {}
  try { updateTmdbHint?.();       } catch {}

  // Final visibility pass after the UI and fields re-render
  try {
    if (typeof scheduleApplySyncVisibility === "function") scheduleApplySyncVisibility();
    else applySyncVisibility?.();
  } catch {}
}

function _getVal(id) {
  const el = document.getElementById(id);
  return (el && typeof el.value === "string" ? el.value : "").trim();
}

async function saveSettings() {
  let schedChanged = false;
  const toast = document.getElementById("save_msg");
  const showToast = (text, ok = true) => {
    if (!toast) return;
    toast.classList.remove("hidden", "ok", "warn");
    toast.classList.add(ok ? "ok" : "warn");
    toast.textContent = text;
    setTimeout(() => toast.classList.add("hidden"), 2000);
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

    const prevMode     = serverCfg?.sync?.bidirectional?.mode || "two-way";
    const prevSource   = serverCfg?.sync?.bidirectional?.source_of_truth || "plex";
    const prevDebug     = !!serverCfg?.runtime?.debug;
    const prevDebugMods = !!serverCfg?.runtime?.debug_mods;
    const prevDebugHttp = !!serverCfg?.runtime?.debug_http;
    const prevPlex     = norm(serverCfg?.plex?.account_token);
    const prevCid      = norm(serverCfg?.simkl?.client_id);
    const prevSec      = norm(serverCfg?.simkl?.client_secret);
    const prevTmdb     = norm(serverCfg?.tmdb?.api_key);
    const prevTraktCid = norm(serverCfg?.trakt?.client_id);
    const prevTraktSec = norm(serverCfg?.trakt?.client_secret);
    const prevMdbl     = norm(serverCfg?.mdblist?.api_key);
    const prevMetaLocale = (serverCfg?.metadata?.locale ?? "").trim();
    const prevMetaTTL    = Number.isFinite(serverCfg?.metadata?.ttl_hours) ? Number(serverCfg.metadata.ttl_hours) : 6;


    const uiMode   = _getVal("mode");
    const uiSource = _getVal("source");
    const uiDebugMode = _getVal("debug"); // off|on|mods|full
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

    // Metadata (locale + TTL)
    const uiMetaLocale = (document.getElementById("metadata_locale")?.value || "").trim();
    const uiMetaTTLraw = (document.getElementById("metadata_ttl_hours")?.value || "").trim();
    const uiMetaTTL    = uiMetaTTLraw === "" ? null : parseInt(uiMetaTTLraw, 10);

    if (uiMetaLocale !== prevMetaLocale) {
      cfg.metadata = cfg.metadata || {};
      if (uiMetaLocale) cfg.metadata.locale = uiMetaLocale;
      else delete cfg.metadata.locale; // allow clearing
      changed = true;
    }
    if (uiMetaTTL !== null && !Number.isNaN(uiMetaTTL) && uiMetaTTL !== prevMetaTTL) {
      cfg.metadata = cfg.metadata || {};
      cfg.metadata.ttl_hours = Math.max(1, uiMetaTTL);
      changed = true;
    }

    // Secrets (tokens, keys, client ids/secrets)
    const sPlex   = readSecretSafe("plex_token", prevPlex);
    const sCid    = readSecretSafe("simkl_client_id", prevCid);
    const sSec    = readSecretSafe("simkl_client_secret", prevSec);
    const sTmdb   = readSecretSafe("tmdb_api_key", prevTmdb);
    const sTrkCid = readSecretSafe("trakt_client_id", prevTraktCid);
    const sTrkSec = readSecretSafe("trakt_client_secret", prevTraktSec);
    const sMdbl   = readSecretSafe("mdblist_key", prevMdbl);

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

    // Jellyfin patch
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

      // read basics from UI
      const uiSrv    = first("jfy_server_url","jfy_server");
      const uiUser   = first("jfy_username","jfy_user");
      const uiUid    = first("jfy_user_id");
      const uiVerify = !!(document.getElementById("jfy_verify_ssl")?.checked ||
                          document.getElementById("jfy_verify_ssl_dup")?.checked);

      const prevSrv    = norm(serverCfg?.jellyfin?.server);
      const prevUser   = norm(serverCfg?.jellyfin?.username || serverCfg?.jellyfin?.user);
      const prevUid    = norm(serverCfg?.jellyfin?.user_id);
      const prevVerify = !!serverCfg?.jellyfin?.verify_ssl;

      if (uiSrv && uiSrv !== prevSrv) { (cfg.jellyfin ||= {}).server = uiSrv; changed = true; }
      if (uiUser && uiUser !== prevUser) {
        (cfg.jellyfin ||= {}).username = uiUser;
        cfg.jellyfin.user = uiUser; // keep both for compatibility
        changed = true;
      }
      if (uiUid && uiUid !== prevUid) { (cfg.jellyfin ||= {}).user_id = uiUid; changed = true; }
      if (uiVerify !== prevVerify)   { (cfg.jellyfin ||= {}).verify_ssl = uiVerify; changed = true; }

      const readFromMatrix = () => {
        const rows = document.querySelectorAll("#jfy_lib_matrix .lm-row");
        if (!rows.length) return null;
        const H = [], R = [];
        rows.forEach(r => {
          const id = String(r.dataset.id || "").trim(); // GUID string
          if (!id) return;
          if (r.querySelector(".lm-dot.hist.on")) H.push(id);
          if (r.querySelector(".lm-dot.rate.on")) R.push(id);
        });
        return { H, R };
      };

      const readFromWhitelist = () => {
        const rows = document.querySelectorAll("#jfy_lib_whitelist .whrow");
        if (!rows.length) return null;
        const H = [], R = [];
        rows.forEach(r => {
          const id = String(r.dataset.id || "").trim(); // GUID string
          if (!id) return;
          if (r.querySelector(".whtog.hist.on")) H.push(id);
          if (r.querySelector(".whtog.rate.on")) R.push(id);
        });
        return { H, R };
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
        return { H: toStrs("#jfy_lib_history"), R: toStrs("#jfy_lib_ratings") };
      };

      const src = readFromMatrix() || readFromWhitelist() || readFromSelects();

      const same = (a, b) => {
        const A = (a || []).map(String).filter(Boolean).sort();
        const B = (b || []).map(String).filter(Boolean).sort();
        if (A.length !== B.length) return false;
        for (let i = 0; i < A.length; i++) if (A[i] !== B[i]) return false;
        return true;
      };

      if (src) {
        const prevH = (serverCfg?.jellyfin?.history?.libraries || []).map(String);
        const prevR = (serverCfg?.jellyfin?.ratings?.libraries || []).map(String);
        if (!same(src.H, prevH)) {
          (cfg.jellyfin ||= {}).history = Object.assign({}, cfg.jellyfin.history || {}, { libraries: src.H || [] });
          changed = true;
        }
        if (!same(src.R, prevR)) {
          (cfg.jellyfin ||= {}).ratings = Object.assign({}, cfg.jellyfin.ratings || {}, { libraries: src.R || [] });
          changed = true;
        }
      }
    } catch (e) {
      console.warn("saveSettings: jellyfin merge failed", e);
    }

    // Plex root patch + whitelist (driven by matrix state)
    try {
      const uiUrl  = norm(document.getElementById("plex_server_url")?.value || "");
      const uiUser = norm(document.getElementById("plex_username")?.value   || "");
      const uiAidS = norm(document.getElementById("plex_account_id")?.value || "");
      const uiAid  = uiAidS === "" ? null : parseInt(uiAidS, 10);

      const prevUrl  = norm(serverCfg?.plex?.server_url);
      const prevUser = norm(serverCfg?.plex?.username);
      const prevAid  = Number.isFinite(serverCfg?.plex?.account_id) ? serverCfg.plex.account_id : null;

      if (uiUrl && uiUrl !== prevUrl) {
        (cfg.plex ||= {}).server_url = uiUrl; changed = true;
      }
      if (uiUser && uiUser !== prevUser) {
        (cfg.plex ||= {}).username = uiUser; changed = true;
      }
      if (uiAid !== null && Number.isFinite(uiAid) && uiAid !== prevAid) {
        (cfg.plex ||= {}).account_id = uiAid; changed = true;
      }

      // ---- verify_ssl checkbox ---------------------------------
      const uiVerify   = !!document.getElementById("plex_verify_ssl")?.checked;
      const prevVerify = !!(serverCfg?.plex?.verify_ssl);
      if (uiVerify !== prevVerify) {
        (cfg.plex ||= {}).verify_ssl = uiVerify; changed = true;
      }
      // ----------------------------------------------------------------

      // --- read selections from the matrix' in-memory state -----------
      const st = (window.__plexState || { hist: new Set(), rate: new Set() });
      const toNums = (xs) =>
        (Array.isArray(xs) ? xs : xs instanceof Set ? Array.from(xs) : [])
          .map(x => parseInt(String(x), 10))
          .filter(Number.isFinite);

      const hist = toNums(st.hist);
      const rate = toNums(st.rate);

      const _same = (a, b) => {
        const A = (a || []).map(Number).sort((x,y)=>x-y);
        const B = (b || []).map(Number).sort((x,y)=>x-y);
        if (A.length !== B.length) return false;
        for (let i=0;i<A.length;i++) if (A[i] !== B[i]) return false;
        return true;
      };

      const prevHist = (serverCfg?.plex?.history?.libraries || []).map(Number);
      const prevRate = (serverCfg?.plex?.ratings?.libraries || []).map(Number);

      if (!_same(hist, prevHist)) {
        (cfg.plex ||= {}).history = Object.assign({}, cfg.plex.history || {}, { libraries: hist });
        changed = true;
      }
      if (!_same(rate, prevRate)) {
        (cfg.plex ||= {}).ratings = Object.assign({}, cfg.plex.ratings || {}, { libraries: rate });
        changed = true;
      }
    } catch (e) {
      console.warn("saveSettings: plex merge failed", e);
    }

    // Scrobbler merge
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

    // Scheduling merge
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

    // UI refreshes
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

    try { document.dispatchEvent(new CustomEvent("config-saved", { detail: { section: "scheduling" } })); } catch {}
    try { document.dispatchEvent(new Event("scheduling-status-refresh")); } catch {}

    try { if (typeof window.refreshSchedulingBanner === "function") await window.refreshSchedulingBanner(); } catch {}
    try { if (typeof window.refreshSettingsInsight === "function") window.refreshSettingsInsight(); } catch {}

    showToast("Settings saved", true);
  } catch (err) {
    console.error("saveSettings failed", err);
    showToast("Save failed — see console", false);
  }
}

// maintenance / troubleshooting actions
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

//----------------------------------------------------------------------

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
  };
  return map[key] || { cls: "", icon: "" };
}

function cxBrandLogo(providerName) {
  const key = (providerName || "").toUpperCase();
  const ICONS = {
    PLEX:  "/assets/img/PLEX.svg",
    SIMKL: "/assets/img/SIMKL.svg",
    TRAKT: "/assets/img/TRAKT.svg",
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

async function loadWall() {
  try {
    const card = document.getElementById("placeholder-card");
    const [wlEnabled, hasKey] = await Promise.all([
      typeof isWatchlistEnabledInPairs === "function" ? isWatchlistEnabledInPairs() : true,
      typeof hasTmdbKey === "function" ? hasTmdbKey() : true
    ]);
    if (!wlEnabled || !hasKey) { card?.classList.add("hidden"); return; }
    card?.classList.remove("hidden");
  } catch {}

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

  // status → pill mapping
  function pillFor(status) {
    switch (String(status || "").toLowerCase()) {
      case "deleted":    return { text: "DELETED", cls: "p-del" };
      case "both":       return { text: "SYNCED",  cls: "p-syn" };
      case "plex_only":  return { text: "PLEX",    cls: "p-px" };
      case "simkl_only": return { text: "SIMKL",   cls: "p-sk" };
      case "trakt_only": return { text: "TRAKT",   cls: "p-tr" };
      case "jellyfin_only": return { text: "JELLYFIN", cls: "p-sk" };
      default:           return { text: "—",       cls: "p-sk" };
    }
  }

  try {
    // filtered server call; falls back to client filter if needed
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

    // first-seen timestamps
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

    // newest first
    items = items.slice().sort((a, b) => getTs(b) - getTs(a));

    // cap preview size (Watchlist Preview only - currently on 20 max)
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

      // fallback poster
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
    const [hasKey, wlEnabled] = await Promise.all([
      hasTmdbKey?.(),
      isWatchlistEnabledInPairs?.()
    ]);
    if (!hasKey || !wlEnabled) return;
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

// Update preview visibility based on context
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

    let hasKey = false, wlEnabled = false;
    try { hasKey = await hasTmdbKey?.(); } catch {}
    try { wlEnabled = await isWatchlistEnabledInPairs?.(); } catch {}

    if (!hasKey || !wlEnabled) { hideAll(); return false; }

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
  // Guard
  if (!id) return null;
  if (window._cfgCache && !String(window._cfgCache?.tmdb?.api_key||"").trim()) return null;

  const typ = isTV(entity) ? "tv" : "movie";
  const cb = window._lastSyncEpoch || 0;

  try {
    // Ask the new resolver only for poster presence
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

    // Use the cached art proxy for the actual image
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
    const slot = document.getElementById("metadata-providers");
    if (slot) {
      slot.innerHTML = html;
    }

    
    try {
      updateTmdbHint?.();
    } catch (_) {}
  } catch (e) {}
}


document.addEventListener("DOMContentLoaded", () => {
  try { mountMetadataProviders(); } catch (e) {}
});

try {
  const exportsObj = { showTab, renderConnections };
  if (typeof window.requestPlexPin === "function") {
    exportsObj.requestPlexPin = window.requestPlexPin; // passthrough from the other file
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



try {
  window.addPair = addPair;
} catch (e) {}
try {
  window.savePairs = savePairs;
} catch (e) {}
try {
  window.deletePair = deletePair;
} catch (e) {}
try {
  window.loadPairs = loadPairs;
} catch (e) {}

try {
  window.addBatch = addBatch;
} catch (e) {}
try {
  window.saveBatches = saveBatches;
} catch (e) {}
try {
  window.loadBatches = loadBatches;
} catch (e) {}
try {
  window.runAllBatches = runAllBatches;
} catch (e) {}

try {
  window.loadProviders = loadProviders;
} catch (e) {}

window.addEventListener("DOMContentLoaded", () => {
  try {
    loadProviders();
  } catch (e) {}
  try {
    loadPairs();
  } catch (e) {}
  try {
    loadBatches();
  } catch (e) {}
});

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

    // Normalize to a stable provider key used by the visibility filter
    const normKey = (s = "") => {
      s = String(s).toUpperCase();
      if (/\bPLEX\b/.test(s)) return "PLEX";
      if (/\bSIMKL\b/.test(s)) return "SIMKL";
      if (/\bTRAKT\b/.test(s)) return "TRAKT";
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

    // Cache providers for other parts of the UI
    window.cx = window.cx || {};
    window.cx.providers = Array.isArray(arr) ? arr : [];

    // Re-render connections if available
    try {
      if (typeof renderConnections === "function") renderConnections();
    } catch (e) {
      console.warn("renderConnections failed", e);
    }
  } catch (e) {
    div.innerHTML = '<div class="muted">Failed to load providers.</div>';
    console.warn("loadProviders error", e);
  } finally {
    // Always apply visibility filter after (re)render
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

window.syncPairs = [];

window.addPair = function () {
  const source = _getVal("source-provider");
  const target = _getVal("target-provider");
  if (!source || !target) {
    logToSyncOutput("Source and Target must be selected.");
    return;
  }
  const pair = { source, target };
  window.syncPairs.push(pair);
  logToSyncOutput(`Added sync pair: ${source} → ${target}`);
  renderSyncPairs();
};

window.addBatch = function () {
  const batch = [
    { source: "PLEX", target: "SIMKL" },
    { source: "SIMKL", target: "PLEX" },
  ];
  for (const pair of batch) {
    window.syncPairs.push(pair);
    logToSyncOutput(`Added sync pair: ${pair.source} → ${pair.target}`);
  }
  renderSyncPairs();
};


function renderSyncPairs() {
  const table = _el("pair-table-body");
  if (!table) return;
  table.innerHTML = "";
  window.syncPairs.forEach((pair, idx) => {
    const row = document.createElement("tr");
    row.innerHTML = `<td>${pair.source}</td><td>${pair.target}</td><td><button onclick="removePair(${idx})">✕</button></td>`;
    table.appendChild(row);
  });
}

window.removePair = function (index) {
  if (index >= 0 && index < window.syncPairs.length) {
    const pair = window.syncPairs.splice(index, 1)[0];
    logToSyncOutput(`Removed sync pair: ${pair.source} → ${pair.target}`);
    renderSyncPairs();
  }
};

function logToSyncOutput(msg) {
  const el = document.getElementById("sync-output");
  if (el) {
    const timestamp = new Date().toLocaleTimeString();
    el.textContent += `[${timestamp}] ${msg}\n`;
    el.scrollTop = el.scrollHeight;
  } else {
    console.log("SYNC LOG:", msg);
  }
}

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


(function () {
  
  window.cxOpenModalFor = function (pair, editingId) {
  try { if (typeof window.cxEnsureCfgModal === "function") { window.cxEnsureCfgModal(); } else { _ensureCfgModal(); } } catch (_) {}

  function pick() {
    return {
      src: document.getElementById("cx-src"),
      dst: document.getElementById("cx-dst"),
      one: document.getElementById("cx-mode-one") || document.querySelector('input[name="cx-mode"][value="one-way"], input[name="cx-mode"][value="one"]'),
      two: document.getElementById("cx-mode-two") || document.querySelector('input[name="cx-mode"][value="two-way"], input[name="cx-mode"][value="two"]'),
      enabled: document.getElementById("cx-enabled"),
      wlAdd: document.getElementById("cx-wl-add"),
      wlRem: document.getElementById("cx-wl-remove"),
      wlNote: document.getElementById("cx-wl-note"),
    };
  }

  var ui = pick();

  if (!ui.src || !ui.dst || !ui.one || !ui.two || !ui.enabled) {
    try { if (typeof window.cxEnsureCfgModal === "function") { window.cxEnsureCfgModal(); } else { _ensureCfgModal(); } } catch (_) {}
    ui = pick();
    if (!ui.src || !ui.dst || !ui.one || !ui.two || !ui.enabled) {
      console.warn("cxOpenModalFor: modal inputs missing after ensure()");
      return;
    }
  }

  
  try {
    var _src = typeof _byName === "function" ? _byName(window.cx.providers, pair.source) : null;
    var _dst = typeof _byName === "function" ? _byName(window.cx.providers, pair.target) : null;
    var twoOk = !!(_src && _dst && _src.capabilities && _dst.capabilities && _src.capabilities.bidirectional && _dst.capabilities.bidirectional);
    if (ui.two) ui.two.disabled = !twoOk;
    var twoLabel = document.getElementById("cx-two-label") || (ui.two && ui.two.closest && ui.two.closest("label"));
    if (twoLabel) twoLabel.classList.toggle("muted", !twoOk);
  } catch (_) {}

  ui.src.value = pair.source; try { ui.src.dispatchEvent(new Event("change")); } catch(_){};
  ui.dst.value = pair.target; try { ui.dst.dispatchEvent(new Event("change")); } catch(_){};

  var mode = pair.mode || "one-way";
  if (mode === "one") mode = "one-way";
  if (mode === "two") mode = "two-way";
  ui.two.checked = mode === "two-way";
  ui.one.checked = !ui.two.checked;
  ui.enabled.checked = pair.enabled !== false;

  try {
    var wf = (pair.features && pair.features.watchlist) || { add: true, remove: false };
    var srcObj = typeof _byName === "function" ? _byName(window.cx.providers, pair.source) : null;
    var dstObj = typeof _byName === "function" ? _byName(window.cx.providers, pair.target) : null;
    var wlOk = !!(srcObj && dstObj ? true : true); 
    if (ui.wlAdd) { ui.wlAdd.checked = wlOk && !!wf.add; ui.wlAdd.disabled = !wlOk; }
    if (ui.wlRem) {
      var wlOn = document.getElementById("cx-wl-enable") ? document.getElementById("cx-wl-enable").checked : true;
      ui.wlRem.checked = wlOk && !!wf.remove;
      ui.wlRem.disabled = !(wlOk && wlOn);
    }
    if (ui.wlNote) ui.wlNote.textContent = wlOk ? "" : "Watchlist is not supported on one of the providers.";
  } catch (_) {}

  var modal = document.getElementById("cx-modal");
  if (modal) modal.classList.remove("hidden");
};
})();

(function () {
  
  window.cx = window.cx || {
    providers: [],
    pairs: [],
    connect: { source: null, target: null },
  };

  
  function _getModal() {
    var m = document.getElementById("cx-modal");
    if (!m && typeof window.cxEnsureCfgModal === "function") {
      try {
        window.cxEnsureCfgModal();
        m = document.getElementById("cx-modal");
      } catch (_) {}
    }
    return m;
  }

  
  async function loadPairs() {
    try {
      const res = await fetch("/api/pairs", { cache: "no-store" });
      const arr = await res.json().catch(() => []);
      window.cx.pairs = Array.isArray(arr) ? arr : [];
      if (typeof window.renderConnections === "function") {
        try {
          if (typeof window.renderConnections === "function") { renderConnections(); }
        } catch (_) {}
      }
    } catch (e) {
      console.warn("[cx] loadPairs failed", e);
    }
  }
  try {
    window.loadPairs = loadPairs;
  } catch (_) {}

  
  async function deletePair(id) {
    if (!id) return;
    try {
      await fetch(`/api/pairs/${encodeURIComponent(id)}`, { method: "DELETE" });
      await loadPairs();
    } catch (e) {
      console.warn("[cx] deletePair failed", e);
      alert("Failed to delete connection.");
    }
  }
  try {
    window.deletePair = deletePair;
  } catch (_) {}

  // --------------------------- Save Pair (create/update) ---------------------------
  async function cxSavePair(data) {
    try {
      const modal = (typeof _getModal === "function" ? _getModal() : document.getElementById("cx-modal")) || null;
      const editingId =
        modal && modal.dataset ? (modal.dataset.editingId || "").trim() : "";

      // ---- Normalize features ------------------------------------------------
      const F = (data && data.features) || {};
      const DEF = { enable: true, add: true, remove: false };

      // Basic feature: only enable/add/remove
      function normBasic(feat) {
        const v = Object.assign({}, DEF, feat || {});
        return {
          enable: !!v.enable,
          add: !!v.add,
          remove: !!v.remove,
        };
      }

      // Ratings: keep toggles + pass through types/mode/from_date (sanitized)
      function normRatings(feat) {
        const v = Object.assign({}, DEF, feat || {});
        const out = {
          enable: !!v.enable,
          add: !!v.add,
          remove: !!v.remove,
        };
        // Preserve scope if present
        if (Array.isArray(v.types)) out.types = v.types.map(String);
        if (typeof v.mode === "string") out.mode = v.mode;
        if (typeof v.from_date === "string") out.from_date = v.from_date.trim();
        return out;
      }

      const features = {};
      if (F.watchlist) features.watchlist = normBasic(F.watchlist);
      if (F.history) features.history = normBasic(F.history);
      if (F.playlists) features.playlists = normBasic(F.playlists);
      if (F.ratings) features.ratings = normRatings(F.ratings);

      // ---- Payload -----------------------------------------------------------
      const modeIn = String(data.mode || "one-way").toLowerCase();
      const mode =
        modeIn === "two" || modeIn === "two-way" ? "two-way" : "one-way";

      const payload = {
        source: data.source,
        target: data.target,
        mode,
        enabled: !!data.enabled,
        features,
      };

      // ---- Save via API ------------------------------------------------------
      let ok = false;
      let r;
      if (editingId) {
        r = await fetch(`/api/pairs/${encodeURIComponent(editingId)}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        ok = !!(r && r.ok);
      } else {
        r = await fetch("/api/pairs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        ok = !!(r && r.ok);
      }

      if (!ok) {
        let msg = "network";
        try {
          msg = r ? `${r.status} ${r.statusText}` : "network";
        } catch (_) {}
        console.warn("[cx] save failed:", msg);
        alert("Failed to save connection.");
        return;
      }

      // Reset modal state and refresh UI
      if (modal && modal.dataset) modal.dataset.editingId = "";
      try {
        window.cx = window.cx || {};
        window.cx.connect = { source: null, target: null };
      } catch (_) {}
      try {
        if (typeof window.cxCloseModal === "function") window.cxCloseModal();
      } catch (_) {}
      const close = document.getElementById("cx-modal");
      if (close) close.classList.add("hidden");

      await (typeof loadPairs === "function" ? loadPairs() : Promise.resolve());
    } catch (e) {
      console.warn("[cx] cxSavePair error", e);
      alert("Failed to save connection.");
    }
  }

  try {
    window.cxSavePair = cxSavePair;
  } catch (_) {}

  // ------------------------- Open Modal (prefill helpers) -------------------------
  const _olderOpen = window.cxOpenModalFor;
  window.cxOpenModalFor = async function (pair, editingId) {
    // Delegate to the original (which builds the full UI & advanced Ratings)
    if (typeof _olderOpen === "function") {
      try {
        await _olderOpen(pair, editingId);
      } catch (_) {}
    }

    // Ensure modal exists
    try {
      if (typeof cxEnsureCfgModal === "function") {
        await cxEnsureCfgModal();
      } else if (typeof _ensureCfgModal === "function") {
        _ensureCfgModal();
      }
    } catch (_) {}

    // Wait for provider selects to be populated
    const __wait = (pred, ms = 1500, step = 25) =>
      new Promise((res) => {
        const t0 = Date.now();
        (function loop() {
          if (pred() || Date.now() - t0 >= ms) return res();
          setTimeout(loop, step);
        })();
      });

    const m =
      document.getElementById("cx-modal") ||
      (typeof _getModal === "function" ? _getModal() : null);
    if (!m) return;
    if (m.dataset)
      m.dataset.editingId = String(editingId || (pair && pair.id) || "");

    const q = (sel) => m.querySelector(sel) || document.querySelector(sel);

    await __wait(() => {
      const s = q("#cx-src"),
        d = q("#cx-dst");
      return !!(
        s &&
        d &&
        s.querySelectorAll("option").length &&
        d.querySelectorAll("option").length
      );
    });

    try {
      const src = q("#cx-src");
      const dst = q("#cx-dst");
      const one =
        q("#cx-mode-one") ||
        q(
          'input[name="cx-mode"][value="one-way"], input[name="cx-mode"][value="one"]'
        );
      const two =
        q("#cx-mode-two") ||
        q(
          'input[name="cx-mode"][value="two-way"], input[name="cx-mode"][value="two"]'
        );
      const en = q("#cx-enabled");

      // Source/Target
      if (src) {
        src.value = (pair && pair.source) || "PLEX";
        try {
          src.dispatchEvent(new Event("change"));
        } catch (_) {}
      }
      if (dst) {
        dst.value = (pair && pair.target) || "SIMKL";
        try {
          dst.dispatchEvent(new Event("change"));
        } catch (_) {}
      }

      // Enabled
      if (en) en.checked = !(pair && pair.enabled === false);

      // Mode
      if (one && two) {
        let mval = (pair && pair.mode) || "one-way";
        if (mval === "one") mval = "one-way";
        if (mval === "two") mval = "two-way";
        two.checked = mval === "two-way";
        one.checked = !two.checked;
      }

      // Watchlist basics (Ratings & others are handled by original renderer)
      const f = (pair && pair.features && pair.features.watchlist) || {};
      const wlEnable = q("#cx-wl-enable");
      const wlAdd = q("#cx-wl-add");
      const wlRem = q("#cx-wl-remove");

      const wlOn = "enable" in f ? !!f.enable : true;
      if (wlEnable) {
        wlEnable.checked = wlOn;
        try {
          wlEnable.dispatchEvent(new Event("change"));
        } catch (_) {}
      }
      if (wlAdd) wlAdd.checked = !!f.add;
      if (wlRem) {
        wlRem.checked = !!f.remove;
        wlRem.disabled = !wlOn;
      }

      m.classList.remove("hidden");

      // Re-apply after a tick (ensures labels update)
      await new Promise((r) => setTimeout(r, 0));
      if (src && pair && pair.source) {
        src.value = pair.source;
        try {
          src.dispatchEvent(new Event("change"));
        } catch (_) {}
      }
      if (dst && pair && pair.target) {
        dst.value = pair.target;
        try {
          dst.dispatchEvent(new Event("change"));
        } catch (_) {}
      }
      if (wlEnable) {
        try {
          wlEnable.dispatchEvent(new Event("change"));
        } catch (_) {}
      }
      if (wlRem) wlRem.disabled = !(wlEnable ? wlEnable.checked : wlOn);
    } catch (_) {}
  };

  // ------------------------------- Boot --------------------------------------
  document.addEventListener("DOMContentLoaded", () => {
    try {
      if (typeof loadPairs === "function") loadPairs();
    } catch (_) {}
  });
})();

(function modalTweaks() {
  const $ = (s) => document.querySelector(s);

  
  const src = $("#cx-src"),
    dst = $("#cx-dst");
  function lockSrcDst() {
    if (src?.value && dst?.value) {
      src.disabled = true;
      dst.disabled = true;
      src.title = "Locked after selection";
      dst.title = "Locked after selection";
    }
  }
  src?.addEventListener("change", lockSrcDst);
  dst?.addEventListener("change", lockSrcDst);
  lockSrcDst();

  
  
  const L1 =
    document.querySelector('label[for="cx-mode-one"]') ||
    $("#cx-mode-one-label") ||
    $("#cx-one-label");
  const L2 =
    document.querySelector('label[for="cx-mode-two"]') ||
    $("#cx-mode-two-label") ||
    $("#cx-two-label");
  if (L1) L1.textContent = "Mirror";
  if (L2) L2.textContent = "Bidirectional";

  
  const en = $("#cx-enabled");
  if (en) {
    en.checked = true;
    (en.closest(".group,.row,fieldset,div") || en).style.display = "none";
  }

  
  function refreshWatchlistUI() {
    const wlOn = $("#cx-wl-enable")?.checked;
    const rem = $("#cx-wl-remove");
    if (rem) {
      rem.disabled = !wlOn;
      if (!wlOn) rem.checked = false;
    }
  }
  ["#cx-wl-enable", "#cx-mode-one", "#cx-mode-two"].forEach((sel) =>
    $(sel)?.addEventListener("change", refreshWatchlistUI)
  );
  refreshWatchlistUI();

  
  function updateDir() {
    const two = $("#cx-mode-two")?.checked;
    const el = $("#sum-dir");
    if (!el) return;
    el.className = "dir " + (two ? "bidi" : "one");
    el.textContent = two ? "⇄" : "→";
  }
  ["#cx-mode-one", "#cx-mode-two"].forEach((sel) =>
    $(sel)?.addEventListener("change", updateDir)
  );
  updateDir();
})();

async function populateSyncModes() {
  const res = await fetch("/api/sync/providers");
  const data = await res.json();
  const src = document.getElementById("src-provider")?.value?.toUpperCase();
  const dst = document.getElementById("dst-provider")?.value?.toUpperCase();
  const select = document.getElementById("sync-mode");
  if (!select || !src || !dst) return;

  const dir =
    data.directions.find((d) => d.source === src && d.target === dst) ||
    data.directions.find((d) => d.source === dst && d.target === src); 
  const modes = dir?.modes || [];
  select.innerHTML = "";
  modes.forEach((m) => {
    const opt = document.createElement("option");
    opt.value = m;
    opt.textContent = m === "two-way" ? "Two-way (bidirectional)" : "One-way";
    select.appendChild(opt);
  });
  if (modes.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "Not supported";
    select.appendChild(opt);
  }
}

window.populateSyncModes = populateSyncModes;

window.addEventListener('cx:open-modal', function(ev){
  try{
    var detail = ev.detail || {};
    if (typeof window.cxOpenModalFor === 'function') {
      window.cxOpenModalFor(detail);
    }
  }catch(e){ console.warn('cx modal bridge failed', e); }
});


// Accessibility: automatically associate labels with their nearest controls when missing
function fixFormLabels(root = document) {
  const ctrls = new Set(["INPUT","SELECT","TEXTAREA"]);
  let uid = 0;
  root.querySelectorAll("label").forEach(lab => {
    if (lab.hasAttribute("for")) return;
    const owned = lab.querySelector("input,select,textarea");
    if (owned) return; // label wraps its control → OK
    // find nearest control
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

/* Smoke-check: ensure essential APIs exist on window */
(function(){
  const need = ["openAbout","cxEnsureCfgModal","renderConnections","loadProviders"];
  need.forEach(n => { if (typeof window[n] !== "function") { console.warn("[crosswatch] missing", n); } });
  document.dispatchEvent(new Event("cx-state-change"));
})();

/* Global shim: showTab for legacy inline onclick= */
(function(){
  if (typeof window.showTab !== "function") {
    window._showTabBootstrap = function(id){
      try {
  // Prefer explicit page IDs when available: #page-main, #page-watchlist, #page-settings
        var pages = document.querySelectorAll("#page-main, #page-watchlist, #page-settings, .tab-page");
        pages.forEach(function(el){ el.classList.add("hidden"); });
        var target = document.getElementById("page-" + id) || document.getElementById(id);
        if (target) target.classList.remove("hidden");

  // Toggle tab header active state if tab headers exist
        ["main","watchlist","settings"].forEach(function(name){
          var th = document.getElementById("tab-" + name);
          if (th) th.classList.toggle("active", name === id);
        });

  // Mount the Watchlist UI dynamically when required
        if (id === "watchlist") {
          try { window.Watchlist?.mount?.(document.getElementById("page-watchlist")); } catch (e) { console.warn(e); }
        }

  // Call optional hook if provided
        document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id } }));
      } catch(e) {
        console.warn("showTab fallback failed:", e);
      }
    };
  }
})();

/* Ensure showTab is global at end */
window.showTab = showTab;