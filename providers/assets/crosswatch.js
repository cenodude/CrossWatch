/* Global showTab bootstrap (runs first) */
(function(){
  if (typeof window.showTab !== "function") {
    window.showTab = function(id){
      try {
        var pages = document.querySelectorAll("#page-main, #page-watchlist, #page-settings, .tab-page");
        pages.forEach(function(el){ el.classList.add("hidden"); });
        var target = document.getElementById("page-" + id) || document.getElementById(id);
        if (target) target.classList.remove("hidden");
        ["main","watchlist","settings"].forEach(function(name){
          var th = document.getElementById("tab-" + name);
          if (th) th.classList.toggle("active", name === id);
        });
        document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id: id } }));
      } catch(e) { console.warn("showTab bootstrap failed:", e); }
    };
  }
})();

/* ==== BEGIN crosswatch.core. ==== */

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

// --- secret field helpers (masking + safe flags)
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
  // Use when async token fetch completes
  applyServerSecret(inputId, !!hasValue);
}

// --- Determine which auth providers are configured
function getConfiguredProviders(cfg = window._cfgCache || {}) {
  const S = new Set();
  const has = (v) => (typeof v === "string" ? v.trim().length > 0 : !!v);

  if (has(cfg?.plex?.account_token)) S.add("PLEX");
  if (has(cfg?.simkl?.access_token || cfg?.auth?.simkl?.access_token)) S.add("SIMKL");
  if (has(cfg?.trakt?.access_token || cfg?.auth?.trakt?.access_token)) S.add("TRAKT");

  return S;
}

// Try to resolve provider key from a dynamic card/row
function resolveProviderKeyFromNode(node) {
  // Prefer an explicit attribute if you can add it in your renderer:
  // <div data-sync-prov="PLEX">...</div>
  const attr = (node.getAttribute?.("data-sync-prov") || node.dataset?.syncProv || "").toUpperCase();
  if (attr) return attr;

  // Logo-based detection (common patterns)
  const img = node.querySelector?.('img[alt], .logo img[alt], [data-logo]');
  const alt = (img?.getAttribute?.('alt') || img?.dataset?.logo || "").toUpperCase();
  if (alt.includes("PLEX"))  return "PLEX";
  if (alt.includes("SIMKL")) return "SIMKL";
  if (alt.includes("TRAKT")) return "TRAKT";

  // Text fallback: look at usual title/name containers, else full text
  const tnode = node.querySelector?.(".title,.name,header,strong,h3,h4");
  const txt = (tnode?.textContent || node.textContent || "").toUpperCase();
  if (/\bPLEX\b/.test(txt))  return "PLEX";
  if (/\bSIMKL\b/.test(txt)) return "SIMKL";
  if (/\bTRAKT\b/.test(txt)) return "TRAKT";

  return ""; // unknown
}

function applySyncVisibility() {
  const allowed = getConfiguredProviders();
  const host = document.getElementById("providers_list");
  if (!host) return;

  // Prefer overlay cards: .prov-card[data-prov]
  let cards = host.querySelectorAll(".prov-card");
  if (!cards || cards.length === 0) {
    // Fallback: oorspronkelijke “card” children
    cards = host.querySelectorAll(":scope > .card, :scope > *");
  }

  cards.forEach((card) => {
    // 1) overlay path (fast + explicit)
    let key = (card.getAttribute?.("data-prov") || card.dataset?.prov || "").toUpperCase();

    // 2) fallback: heuristics on inner content (older renderer)
    if (!key) key = resolveProviderKeyFromNode(card);

    if (!key) return; // unknown container: leave it alone
    card.dataset.syncProv = key; // remember for next runs
    card.style.display = allowed.has(key) ? "" : "none";
  });

  // Rebuild pair selectors strictly from allowed
  const LABEL = { PLEX: "Plex", SIMKL: "SIMKL", TRAKT: "Trakt" };
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

    ["PLEX", "SIMKL", "TRAKT"].forEach((k) => {
      if (!allowed.has(k)) return;
      const o = document.createElement("option");
      o.value = k; o.textContent = LABEL[k] || k;
      sel.appendChild(o);
    });

    if (prev && allowed.has(prev)) sel.value = prev;
    else if (hadPlaceholder) sel.value = "";
  });
}


// Debounced scheduler for applySyncVisibility (no thrash, safe if function not yet defined)
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

// Observe dynamic rendering and re-apply (debounced)
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
  // First pass
  scheduleApplySyncVisibility();
}


// ---- Watchlist Preview visibility based on /api/pairs ----
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
  const freshWithin = (obj) => obj && (Date.now() - (obj.t || 0) < PAIRS_TTL_MS);
  const anyWL = (arr) => Array.isArray(arr) && arr.some(p => !!(p?.features?.watchlist?.enable));

  // Try cache first
  const cached = _loadPairsCache();
  if (freshWithin(cached)) {
    const cachedEnabled = anyWL(cached.pairs);
    if (!cachedEnabled) return false;        // trust "false" immediately
    // cached says true → confirm with live before returning true
    const live = await _getPairsFresh();
    return anyWL(live);
  }

  // No fresh cache → go live
  const live = await _getPairsFresh();
  return anyWL(live);
}

// expose so other modules can call after settings/sync
window.updatePreviewVisibility = updatePreviewVisibility;

// run once on load
document.addEventListener("DOMContentLoaded", () => { updatePreviewVisibility(); });

// ---- END   Watchlist Preview visibility based on /api/pairs ----

// ========================== CONNECTION STATUS -- BEGIN   ============================================
const AUTO_STATUS = false; // DISABLE by default
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

// --- status normalizer (zet alles naar {PLEX|SIMKL|TRAKT: {connected:boolean}})
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
    PLEX:  normOne(pick(p, "PLEX")  ?? p.plex_connected),
    SIMKL: normOne(pick(p, "SIMKL") ?? p.simkl_connected),
    TRAKT: normOne(pick(p, "TRAKT") ?? p.trakt_connected),
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

  const active = { PLEX: false, SIMKL: false, TRAKT: false };
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
function toggleProviderBadges(active) {
  const map = { PLEX: "badge-plex", SIMKL: "badge-simkl", TRAKT: "badge-trakt" };
  for (const [prov, id] of Object.entries(map)) {
    const el = document.getElementById(id);
    if (!el) continue;
    el.classList.toggle("hidden", !(active && active[prov]));
  }
}

// Tri-state normalizer: "ok" | "no" | "unknown" (fallback)
function connState(v) {
  if (v == null) return "unknown";

  // booleans
  if (v === true)  return "ok";
  if (v === false) return "no";

  // numbers
  if (typeof v === "number") {
    if (v === 1) return "ok";
    if (v === 0) return "no";
  }

  // strings
  if (typeof v === "string") {
    const s = v.toLowerCase().trim();
    if (/^(ok|up|connected|ready|true|on|online|active)$/.test(s))   return "ok";
    if (/^(no|down|disconnected|false|off|disabled)$/.test(s))       return "no";
    if (/^(unknown|stale|n\/a|-|pending)$/.test(s))                  return "unknown";
    return "unknown";
  }

  // objects {connected|ok|ready|active|online|status|state}
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

function setBadge(id, providerName, state, stale) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove("ok", "no", "unknown", "stale");
  el.classList.add(state);
  if (stale) el.classList.add("stale");
  const label =
    providerName + ": " + (state === "ok" ? "Connected" : state === "no" ? "Not connected" : "Unknown");
  el.innerHTML = `<span class="dot ${state}"></span>${label}`;
}

function renderConnectorStatus(providers, { stale = false } = {}) {
  const p = providers || {};

  // Fallback to false (=> "no") if provider key is missing
  const plex  = pickCase(p, "PLEX");   // could be boolean or object
  const simkl = pickCase(p, "SIMKL");
  const trakt = pickCase(p, "TRAKT");

  setBadge("badge-plex",  "Plex",  connState(plex  ?? false), stale);
  setBadge("badge-simkl", "SIMKL", connState(simkl ?? false), stale);
  setBadge("badge-trakt", "Trakt", connState(trakt ?? false), stale);
}


async function refreshStatus(force = false) {
  const now = Date.now();
  if (!force && typeof lastStatusMs !== "undefined" && typeof STATUS_MIN_INTERVAL !== "undefined" && (now - lastStatusMs < STATUS_MIN_INTERVAL)) return;
  if (typeof lastStatusMs !== "undefined") lastStatusMs = now;

  try {
    // 1) Make sure we know which providers are used in pairs (throttled)
    await refreshPairedProviders(force ? 0 : 5000);

    // 2) Fetch live status
    const r = await fetch("/api/status" + (force ? "?fresh=1" : ""), { cache: "no-store" }).then(r => r.json());
    if (typeof appDebug !== "undefined") appDebug = !!r.debug;

    const pick = (obj, k) => (obj?.[k] ?? obj?.[k.toLowerCase()] ?? obj?.[k.toUpperCase()]);
    const norm = (v, fb = false) => (typeof v === "boolean" ? { connected: v } : (v && typeof v === "object") ? v : { connected: !!fb });

    const pRaw = r.providers || {};
    const providers = {
      PLEX:  norm(pick(pRaw, "PLEX"),  (r.plex_connected  ?? r.plex)),
      SIMKL: norm(pick(pRaw, "SIMKL"), (r.simkl_connected ?? r.simkl)),
      TRAKT: norm(pick(pRaw, "TRAKT"), (r.trakt_connected ?? r.trakt)),
    };

    renderConnectorStatus(providers, { stale: false });
    saveStatusCache?.(providers);

    window._ui = window._ui || {};
    window._ui.status = {
      can_run:          !!r.can_run,
      plex_connected:   !!(providers.PLEX?.connected  ?? providers.PLEX?.ok),
      simkl_connected:  !!(providers.SIMKL?.connected ?? providers.SIMKL?.ok),
      trakt_connected:  !!(providers.TRAKT?.connected ?? providers.TRAKT?.ok),
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
      renderConnectorStatus(cached.providers, { stale: true }); // voorkom rode flits
    }
  } catch {}
  // badge-visibility naar gelang /api/pairs (als je helper hebt)
  try { refreshPairedProviders?.(0); } catch {}
  // daarna live status halen (overschrijft stale weergave + her-cachen)
  try { refreshStatus(false); } catch {}
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
    if (cached?.providers) renderConnectorStatus(cached.providers, { stale: true });
    else if (window._ui?.status) {
      const s = window._ui.status;
      renderConnectorStatus({
        PLEX:  { connected: !!s.plex_connected },
        SIMKL: { connected: !!s.simkl_connected },
        TRAKT: { connected: !!s.trakt_connected },
      }, { stale: true });
    }

    await Promise.race([
      refreshStatus(true),
      new Promise((_, rej) => setTimeout(() => rej(new Error("status refresh timeout")), 8000))
    ]);
  } catch (e) {
    console.warn("Manual status refresh failed", e);
  } finally {
    setRefreshBusy?.(false);
    btn?.classList.remove("spin");
    manualRefreshStatus._inFlight = false;
  }
}

//  ====================== END CONNECTION status =========================

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

function setRunProgress(pct) {
  const btn = document.getElementById("run");
  if (!btn) return;
  const p = Math.max(0, Math.min(100, Math.floor(pct)));
  btn.style.setProperty("--prog", String(p));
}

function startRunVisuals(indeterminate = true) {
  const btn = document.getElementById("run");
  if (!btn) return;
  btn.classList.add("loading");
  btn.classList.toggle("indet", !!indeterminate);
  if (indeterminate) setRunProgress(8);
}

function stopRunVisuals() {
  const btn = document.getElementById("run");
  if (!btn) return;
  setRunProgress(100);
  btn.classList.remove("indet");
  setTimeout(() => {
    btn.classList.remove("loading");
    setRunProgress(0);
  }, 700);
}

function updateProgressFromTimeline(tl) {
  const order = ["start", "pre", "post", "done"];
  let done = 0;
  for (const k of order) if (tl && tl[k]) done++;
  let pct = (done / order.length) * 100;
  if (pct > 0 && pct < 15) pct = 15;

  if (typeof setRunProgress === "function") setRunProgress(pct);

  const payload = { pct, tl: tl || {} };
  window.dispatchEvent(new CustomEvent("ux:progress", { detail: payload }));
  if (window.UX && typeof window.UX.updateProgress === "function") {
    window.UX.updateProgress(payload);
  }
}

function recomputeRunDisabled() {
  const btn = document.getElementById("run");
  if (!btn) return;
  const busyNow = !!window.busy;
  const canRun = !window._ui?.status ? true : !!window._ui.status.can_run;
  const running = !!(window._ui?.summary && window._ui.summary.running);
  btn.disabled = busyNow || running || !canRun;
}

// Bridge-friendly + null-safe
function setTimeline(tl) {
  const keys = ["start", "pre", "post", "done"];
  for (const k of keys) {
    const el = document.getElementById("tl-" + k);
    if (el) el.classList.toggle("on", !!(tl && tl[k]));
  }
  window.dispatchEvent(new CustomEvent("ux:timeline", { detail: tl || {} }));
  if (window.UX && typeof window.UX.updateTimeline === "function") {
    window.UX.updateTimeline(tl || {});
  }
}

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


/* Tabs & Navigation */
async function showTab(n) {
  const pageSettings  = document.getElementById("page-settings");
  const pageWatchlist = document.getElementById("page-watchlist");
  const logPanel      = document.getElementById("log-panel");
  const layout        = document.getElementById("layout");
  const statsCard     = document.getElementById("stats-card");
  const ph            = document.getElementById("placeholder-card");

  // tabs
  document.getElementById("tab-main")?.classList.toggle("active", n === "main");
  document.getElementById("tab-watchlist")?.classList.toggle("active", n === "watchlist");
  document.getElementById("tab-settings")?.classList.toggle("active", n === "settings");

  // cards
  document.getElementById("ops-card")?.classList.toggle("hidden", n !== "main");
  statsCard?.classList.toggle("hidden", n !== "main");
  if (ph && n !== "main") ph.classList.add("hidden");

  // pages
  pageWatchlist?.classList.toggle("hidden", n !== "watchlist");
  pageSettings?.classList.toggle("hidden", n !== "settings");

  const hasStats = !!(statsCard && !statsCard.classList.contains("hidden"));

  if (n === "main") {
    layout.classList.remove("single");
    layout.classList.toggle("full", !appDebug && !hasStats);
    if (AUTO_STATUS) refreshStatus(false);
    if (!esSum) openSummaryStream();
    refreshStats(true);
    window.wallLoaded = false;
    try { await updatePreviewVisibility(); } catch (e) { console.warn(e); }

    if (typeof window.refreshSchedulingBanner === 'function') {
      window.refreshSchedulingBanner();
    } else {
      window.addEventListener('sched-banner-ready', () => {
        try { window.refreshSchedulingBanner?.(); } catch {}
      }, { once: true });
    }
  } else {
    layout.classList.add("single");
    layout.classList.remove("full");
    logPanel?.classList.add("hidden");
  }

  if (n === "watchlist") {
    try {
      const host = document.getElementById("watchlist-root") || pageWatchlist;

      const mountIt = async () => {
        const m = window.Watchlist || window.WatchlistPage || window.WatchlistUI || null;
        if (!m) return false;
        const mountFn = m.mount || m.init || m.render || (typeof m === "function" ? m : null);
        if (!mountFn) return false;
        if (!window.Watchlist || !window.Watchlist.mount) {
          window.Watchlist = {
            mount: (el) => mountFn.call(m, el),
            refresh: m.refresh || m.update || null
          };
        }
        if (!window._watchlistMounted) {
          await window.Watchlist.mount(host);
          window._watchlistMounted = true;
        } else {
          await window.Watchlist?.refresh?.();
        }
        return true;
      };

      let ok = await mountIt();
      if (!ok && !window.Watchlist) {
        try {
          const mod = await import("/assets/watchlist.js").catch(() => null);
          if (mod && !window.Watchlist) window.Watchlist = mod.Watchlist || mod.default || mod;
          ok = await mountIt();
        } catch (e) { console.error(e); }
      }
      if (!ok) console.warn("Watchlist not mounted");
    } catch (e) { console.warn(e); }
  }

  if (n === "settings") {
    try { await mountAuthProviders?.(); } catch (e) { console.warn(e); }
    try { await loadConfig(); } catch (e) { console.warn(e); }
    updateTmdbHint?.();
    updateSimklHint?.();
    updateSimklButtonState?.();
    updateTraktHint?.();
    startTraktTokenPoll?.();

    if (typeof window.loadScheduling === 'function') {
      await window.loadScheduling();
    } else {
      window.addEventListener('sched-banner-ready', () => {
        try { window.loadScheduling?.(); } catch {}
      }, { once: true });
    }

    ensureScrobbler();
    setTimeout(ensureScrobbler, 200);
  }
}


// --- Scrobbler UI mount (only once, when settings tab is shown and both PLEX+TRAKT are configured)
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

  // If already loaded by <script defer src="/assets/scrobbler.js">, just start
  if (window.Scrobbler) { start(); return; }

  // Otherwise load once
  let s = document.getElementById("scrobbler-js");
  if (!s) {
    s = document.createElement("script");
    s.id = "scrobbler-js";
    s.src = "/assets/scrobbler.js";
    s.defer = true;
    s.onload = start;
    s.onerror = () => console.warn("[scrobbler] script failed to load");
    document.head.appendChild(s);
  } else {
    s.onload = start;
  }
}


function toggleSection(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle("open");
}
function setBusy(v) {
  busy = v;
  recomputeRunDisabled();
}

/* Run Sync (Trigger + UI State) */
async function runSync() {
  if (busy) return;
  const btn = document.getElementById("run");
  setBusy(true);

  
  const detLog = document.getElementById("det-log");
  if (detLog) detLog.textContent = "";

  
  if (esDet) {
    try { esDet.close(); } catch (_) {}
    esDet = null;
  }
  openDetailsLog(); 

  try {
    btn?.classList.add("glass");
    const resp = await fetch("/api/run", { method: "POST" });
    const j = await resp.json();
    if (!resp.ok || !j || j.ok !== true) {
      setSyncHeader(
        "sync-bad",
        `Failed to start${j?.error ? ` – ${j.error}` : ""}`
      );
    } else {
    }
  } catch (e) {
    setSyncHeader("sync-bad", "Failed to reach server");
  } finally {
    setBusy(false);
    recomputeRunDisabled();
    if (AUTO_STATUS) refreshStatus(false);
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


/* Insights: Sparkline Chart */
/* #-------------PASCAL----BEGIN----- insights */
function renderSparkline(){ if (window.Insights && window.Insights.renderSparkline) return window.Insights.renderSparkline.apply(this, arguments); }
document.addEventListener("DOMContentLoaded", refreshInsights);

/* #-------------PASCAL----END----- insights */

/* Update Check (uses /api/version) */
async function checkForUpdate() {
  try {
    const r = await fetch("/api/version", { cache: "no-store" });
    if (!r.ok) throw new Error("HTTP " + r.status);

    const j = await r.json();
    const cur    = String(j.current ?? "0.0.0").trim();
    const latest = (j.latest ? String(j.latest).trim() : null) || null;
    const url    = j.html_url || "https://github.com/cenodude/CrossWatch/releases";
    const hasUpdate = !!j.update_available;

    // Header version label
    const vEl = document.getElementById("app-version");
    if (vEl) vEl.textContent = `Version ${cur}`;

    // Update badge
    const updEl = document.getElementById("st-update");
    if (!updEl) return;
    updEl.classList.add("badge", "upd");

    if (hasUpdate && latest) {
      const changed = latest !== (updEl.dataset.lastLatest || "");
      updEl.innerHTML = `<a href="${url}" target="_blank" rel="noopener" title="Open release page">Update ${latest} available</a>`;
      updEl.setAttribute("aria-label", `Update ${latest} available`);
      updEl.classList.remove("hidden");

      if (changed) {
        updEl.dataset.lastLatest = latest;
        updEl.classList.remove("reveal");
        void updEl.offsetWidth; // restart animation
        updEl.classList.add("reveal");
      }
    } else {
      updEl.classList.add("hidden");
      updEl.classList.remove("reveal");
      updEl.removeAttribute("aria-label");
      updEl.textContent = "";
      delete updEl.dataset.lastLatest;
    }
  } catch (err) {
    console.debug("Version check failed:", err);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  checkForUpdate();
});

checkForUpdate(true);
setInterval(() => checkForUpdate(false), UPDATE_CHECK_INTERVAL_MS);

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

// Init + hourly poll
document.addEventListener('DOMContentLoaded', () => { checkForUpdate(); });
setInterval(checkForUpdate, 60 * 60 * 1000);

/* END Update Check (uses /api/version) */

/* Summary Stream: Render */
/* #-------------PASCAL----BEGIN----- summary-stream */
function renderSummary(sum) {
  currentSummary = sum;
  window._ui = window._ui || {};
  window._ui.summary = sum;

  const pp = sum.plex_post ?? sum.plex_pre;
  const sp = sum.simkl_post ?? sum.simkl_pre;

  document.getElementById("chip-plex").textContent = pp ?? "–";
  document.getElementById("chip-simkl").textContent = sp ?? "–";
  document.getElementById("chip-dur").textContent =
    sum.duration_sec != null ? sum.duration_sec + "s" : "–";
  document.getElementById("chip-exit").textContent =
    sum.exit_code != null ? String(sum.exit_code) : "–";

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

  document.getElementById("det-cmd").textContent = sum.cmd || "–";
  document.getElementById("det-ver").textContent = sum.version || "–";
  document.getElementById("det-start").textContent  = toLocal(sum.started_at);
  document.getElementById("det-finish").textContent = toLocal(sum.finished_at);
  const tl = sum.timeline || {};
  setTimeline(tl);
  updateProgressFromTimeline?.(tl);
  const btn = document.getElementById("run");

  if (btn) {
    if (sum.running) {
      btn.classList.add("glass", "loading");

      if (tl.pre || tl.post || tl.done) btn.classList.remove("indet");
      else btn.classList.add("indet");

      if (!_wasRunning && !(tl.pre || tl.post || tl.done)) {
        setRunProgress?.(8);
      }
    } else {
      if (_wasRunning) {
        setRunProgress?.(100);
        btn.classList.remove("indet");
        setTimeout(() => {
          btn.classList.remove("loading", "glass");
          setRunProgress?.(0);
        }, 700);
      } else {
        btn.classList.remove("loading", "indet", "glass");
        setRunProgress?.(0);
      }
    }
  }

  if (typeof recomputeRunDisabled === "function") recomputeRunDisabled();

  if (_wasRunning && !sum.running) {
    window.wallLoaded = false;
    updatePreviewVisibility?.();
    refreshSchedulingBanner?.();
  }

  _wasRunning = !!sum.running;
}

// Keep stats in sync after summary renders
(() => {
  const prev = window.renderSummary;
  window.renderSummary = function(sum){
    try { prev?.(sum); } catch {}
    try { refreshStats(false); } catch {}
  };
})();


/* Summary Stream: Subscribe */
function openSummaryStream() {
  esSum = new EventSource("/api/run/summary/stream");

  esSum.onmessage = (ev) => {
    try {
      renderSummary(JSON.parse(ev.data));
    } catch (_) {}
  };

  fetch("/api/run/summary")
    .then((r) => r.json())
    .then(renderSummary)
    .catch(() => {});
}

let _lastStatsFetch = 0;

function _ease(t) {
  return t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
}

function animateNumber(
/* #-------------PASCAL----END----- summary-stream */
el, to) {
  const from = parseInt(el.dataset.v || "0", 10) || 0;

  if (from === to) {
    el.textContent = String(to);
    el.dataset.v = String(to);
    return;
  }

  const dur = 600,
    t0 = performance.now();

  function step(now) {
    const p = Math.min(1, (now - t0) / dur),
      v = Math.round(from + (to - from) * _ease(p));

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

  if (bars.week) bars.week.style.transform = `scaleY(${h(week)})`;
  if (bars.month) bars.month.style.transform = `scaleY(${h(month)})`;
  if (bars.now) bars.now.style.transform = `scaleY(${h(now)})`;
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

    const elP = document.getElementById("stat-plex");
    const elS = document.getElementById("stat-simkl");
    const curP = Number(elP?.textContent || 0);
    const curS = Number(elS?.textContent || 0);

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

    
    document.getElementById("tile-plex")?.removeAttribute("hidden");
    document.getElementById("tile-simkl")?.removeAttribute("hidden");
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

// tiny glue buffer for SSE chunks
let detBuf = "";

function openDetailsLog() {
  const el = document.getElementById("det-log");
  const slider = document.getElementById("det-scrub");
  if (!el) return;

  el.innerHTML = "";
  el.classList?.add("cf-log");
  detStickBottom = true;

  if (esDet) { try { esDet.close(); } catch (_) {} esDet = null; }

  const updateSlider = () => {
    if (!slider) return;
    const max = el.scrollHeight - el.clientHeight;
    slider.value = max <= 0 ? 100 : Math.round((el.scrollTop / max) * 100);
  };

  const updateStick = () => {
    const pad = 6;
    detStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
  };

  el.addEventListener("scroll", () => { updateSlider(); updateStick(); }, { passive: true });

  if (slider) {
    slider.addEventListener("input", () => {
      const max = el.scrollHeight - el.clientHeight;
      el.scrollTop = Math.round((slider.value / 100) * max);
      detStickBottom = slider.value >= 99;
    });
  }

  const CF = window.ClientFormatter;
  if (!CF || !CF.processChunk || !CF.renderInto) {
    console.warn("ClientFormatter not loaded");
    return;
  }

  esDet = new EventSource("/api/logs/stream?tag=SYNC");

  // helper: debug = true
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

  esDet.onmessage = (ev) => {
    if (!ev?.data) return;

    if (ev.data === "::CLEAR::") {
      el.textContent = "";
      detBuf = "";
      return;
    }

    // DEBUG: no formatting
    if (window.appDebug) {
      appendRaw(ev.data);
      if (detStickBottom) el.scrollTop = el.scrollHeight;
      updateSlider();
      return;
    }

    // NORMAL: thr. ClientFormatter (tokens + pretty)
    const { tokens, buf } = CF.processChunk(detBuf, ev.data);
    detBuf = buf;
    for (const tok of tokens) CF.renderInto(el, tok, false);

    if (detStickBottom) el.scrollTop = el.scrollHeight;
    updateSlider();
  };

  esDet.onerror = () => {
    try { esDet?.close(); } catch (_) {}
    esDet = null;


    if (!window.appDebug && detBuf && detBuf.trim()) {
      const { tokens } = CF.processChunk("", detBuf);
      detBuf = "";
      for (const tok of tokens) CF.renderInto(el, tok, false);
      if (detStickBottom) el.scrollTop = el.scrollHeight;
      updateSlider();
    }
  };

  requestAnimationFrame(() => {
    el.scrollTop = el.scrollHeight;
    updateSlider();
  });
  }

  function closeDetailsLog() {
    try { esDet?.close(); } catch (_) {}
    esDet = null;
    detBuf = "";
  }

function toggleDetails() {
  const d = document.getElementById("details");

  d.classList.toggle("hidden");

  if (!d.classList.contains("hidden")) openDetailsLog();
  else closeDetailsLog();
}

window.addEventListener("beforeunload", closeDetailsLog);

async function copySummary(btn) {
  if (!window.currentSummary) {
    try {
      window.currentSummary = await fetch("/api/run/summary").then((r) =>
        r.json()
      );
    } catch {
      flashCopy(btn, false, "No summary");
      return;
    }
  }

  const s = window.currentSummary;

  if (!s) {
    flashCopy(btn, false, "No summary");
    return;
  }

  const lines = [];
  lines.push(`CrossWatch ${s.version || ""}`.trim());

  if (s.started_at) lines.push(`Start:   ${s.started_at}`);
  if (s.finished_at) lines.push(`Finish:  ${s.finished_at}`);
  if (s.cmd) lines.push(`Cmd:     ${s.cmd}`);
  if (s.plex_pre != null && s.simkl_pre != null)
    lines.push(`Pre:     Plex=${s.plex_pre} vs SIMKL=${s.simkl_pre}`);
  if (s.plex_post != null && s.simkl_post != null)
    lines.push(
      `Post:    Plex=${s.plex_post} vs SIMKL=${s.simkl_post} -> ${
        s.result || "UNKNOWN"
      }`
    );

  if (s.duration_sec != null) lines.push(`Duration: ${s.duration_sec}s`);
  if (s.exit_code != null) lines.push(`Exit:     ${s.exit_code}`);

  const text = lines.join("\n");
  let ok = false;

  try {
    await navigator.clipboard.writeText(text);
    ok = true;
  } catch (e) {
    ok = false;
  }

  if (!ok) {
    try {
      const ta = document.createElement("textarea");

      ta.value = text;
      ta.setAttribute("readonly", "");

      ta.style.position = "fixed";
      ta.style.opacity = "0";

      document.body.appendChild(ta);
      ta.focus();
      ta.select();

      ok = document.execCommand("copy");
      document.body.removeChild(ta);
    } catch (e) {
      ok = false;
    }
  }

  flashCopy(btn, ok);
}

function downloadSummary() {
  window.open("/api/run/summary/file", "_blank");
}

function setRefreshBusy(busy) {
  const btn = document.getElementById("btn-status-refresh");

  if (!btn) return;
  btn.disabled = !!busy;
  btn.classList.toggle("loading", !!busy);
}

// --- bootstrap shims (keep console quiet if modals not loaded yet)
window.openAbout = window.openAbout || function(){};
window.cxEnsureCfgModal = window.cxEnsureCfgModal || function(){};

// --- secret field helpers (masking + touch flags)
// Ensures saveSettings only updates tokens when user actually changed them.
window.wireSecretTouch = window.wireSecretTouch || function wireSecretTouch(id) {
  const el = document.getElementById(id);
  if (!el || el.__wiredTouch) return;
  el.addEventListener("input", () => {
    el.dataset.touched = "1";
    // typing into a masked field means it's no longer a placeholder
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
  _setVal("debug",  String(!!cfg.runtime?.debug));
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

  // TRAKT
  setRaw("trakt_client_id",     val(cfg.trakt?.client_id));
  setRaw("trakt_client_secret", val(cfg.trakt?.client_secret));
  setRaw("trakt_token",         val(cfg.trakt?.access_token) || val(cfg.auth?.trakt?.access_token));
})(cfg);

  // --- Legacy/basic scheduling (advanced UI may extend this later)
  const s = cfg.scheduling || {};
  _setVal("schEnabled", String(!!s.enabled));
  _setVal("schMode",    typeof s.mode === "string" && s.mode ? s.mode : "hourly");
  _setVal("schN",       Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "2");
  _setVal("schTime",    typeof s.daily_time === "string" && s.daily_time ? s.daily_time : "03:30");
  if (document.getElementById("schTz")) _setVal("schTz", s.timezone || "");

  // UI side-hints
  try { updateSimklButtonState?.(); } catch {}
  try { updateSimklHint?.();      } catch {}
  try { updateTmdbHint?.();       } catch {}

  // Final visibility pass after fields/UI may have re-rendered
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
    const prevDebug    = !!serverCfg?.runtime?.debug;
    const prevPlex     = norm(serverCfg?.plex?.account_token);
    const prevCid      = norm(serverCfg?.simkl?.client_id);
    const prevSec      = norm(serverCfg?.simkl?.client_secret);
    const prevTmdb     = norm(serverCfg?.tmdb?.api_key);
    const prevTraktCid = norm(serverCfg?.trakt?.client_id);
    const prevTraktSec = norm(serverCfg?.trakt?.client_secret);

    const uiMode   = _getVal("mode");
    const uiSource = _getVal("source");
    const uiDebug  = _getVal("debug") === "true";

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
    if (uiDebug !== prevDebug) {
      cfg.runtime = cfg.runtime || {};
      cfg.runtime.debug = uiDebug;
      changed = true;
    }

    const sPlex   = readSecretSafe("plex_token", prevPlex);
    const sCid    = readSecretSafe("simkl_client_id", prevCid);
    const sSec    = readSecretSafe("simkl_client_secret", prevSec);
    const sTmdb   = readSecretSafe("tmdb_api_key", prevTmdb);
    const sTrkCid = readSecretSafe("trakt_client_id", prevTraktCid);
    const sTrkSec = readSecretSafe("trakt_client_secret", prevTraktSec);

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
      if (typeof window.getRootPatch === "function") {
        const rootPatch = window.getRootPatch() || {};
        const nextUrl = norm(rootPatch?.plex?.server_url);
        const prevUrl = norm(serverCfg?.plex?.server_url);
        cfg.plex = cfg.plex || {};
        cfg.plex.server_url = nextUrl;
        if (nextUrl !== prevUrl) changed = true;
      }
    } catch (e) {
      console.warn("saveSettings: plex.server_url merge failed", e);
    }

    // scheduling merge
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
        schedChanged = true; // NEW
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

      // refresh caches after save
      try { if (typeof loadConfig === "function") await loadConfig(); } catch {}
      try { if (typeof _invalidatePairsCache === "function") _invalidatePairsCache(); } catch {}

      // If scheduling changed, hit the scheduler endpoint (does real stop/start).
      // Otherwise just nudge a replan.
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

    // downstream UI refreshes (guard everything)
    try {
      if (typeof refreshPairedProviders === "function") await refreshPairedProviders(0);
      const cached = (typeof loadStatusCache === "function") ? loadStatusCache() : null;
      if (cached?.providers && typeof renderConnectorStatus === "function") {
        renderConnectorStatus(cached.providers, { stale: true });
      }
      if (typeof refreshStatus === "function") await refreshStatus(true);
    } catch {}

    try { if (typeof updatePreviewVisibility === "function") await updatePreviewVisibility(); } catch {}
    try { if (typeof updateTmdbHint === "function") updateTmdbHint(); } catch {}
    try { if (typeof updateSimklState === "function") updateSimklState(); } catch {}

    // loadScheduling may not exist → guard it, use the global
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

    try {
      window.dispatchEvent(new CustomEvent("settings-changed", {
        detail: { scope: "settings", reason: "save" }
      }));
    } catch {}

    // explicit broadcasts for scheduler consumers
    try { document.dispatchEvent(new CustomEvent("config-saved", { detail: { section: "scheduling" } })); } catch {}
    try { document.dispatchEvent(new Event("scheduling-status-refresh")); } catch {}

    // banners / side insight (cache-busted fetches happen inside)
    try { if (typeof window.refreshSchedulingBanner === "function") await window.refreshSchedulingBanner(); } catch {}
    try { if (typeof window.refreshSettingsInsight === "function") window.refreshSettingsInsight(); } catch {}

    showToast("Settings saved", true);
  } catch (err) {
    console.error("saveSettings failed", err);
    showToast("Save failed — see console", false);
  }
}

async function clearState() {
  const btnText = "Clear State";
  try {
    const r = await fetch("/api/troubleshoot/reset-state", {
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
    const r = await fetch("/api/troubleshoot/clear-cache", { method: "POST" });
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
    const r = await fetch("/api/troubleshoot/reset-stats", { method: "POST" });
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

// Wis tokens + pending info uit config en uit UI
async function flushTraktCreds() {
  try {
    const r = await fetch("/api/config", { cache: "no-store" });
    const cfg = await r.json();
    cfg.trakt = cfg.trakt || {};
    cfg.auth = cfg.auth || {};
    cfg.auth.trakt = cfg.auth.trakt || {};

    delete cfg.trakt.access_token;
    delete cfg.trakt.refresh_token;
    delete cfg.trakt.scope;
    delete cfg.trakt.token_type;
    delete cfg.trakt.expires_at;
    delete cfg.trakt._pending_device;

    delete cfg.auth.trakt.access_token;
    delete cfg.auth.trakt.refresh_token;
    delete cfg.auth.trakt.scope;
    delete cfg.auth.trakt.token_type;
    delete cfg.auth.trakt.expires_at;

    await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(cfg),
    });

    try { const t = document.getElementById("trakt_token"); if (t) t.value = ""; } catch(_) {}
    try { const p = document.getElementById("trakt_pin");   if (p) p.value = ""; } catch(_) {}
    try { setTraktSuccess(false); } catch(_) {}
    try { notify && notify("Trakt-gegevens gewist"); } catch(_) {}
  } catch (e) {
    console.warn("flushTraktCreds failed", e);
    try { notify && notify("Wissen mislukt"); } catch(_) {}
  }
}


function setPlexSuccess(show) {
  document.getElementById("plex_msg").classList.toggle("hidden", !show);
}
async function requestPlexPin() {
  try {
    setPlexSuccess && setPlexSuccess(false);
  } catch (_) {}
  let win = null;
  try {
    win = window.open("https://plex.tv/link", "_blank");
  } catch (_) {}
  let resp, data;
  try {
    resp = await fetch("/api/plex/pin/new", { method: "POST" });
  } catch (e) {
    console.warn("plex pin fetch failed", e);
    try {
      notify && notify("Failed to request PIN");
    } catch (_) {}

    return;
  }

  try {
    data = await resp.json();
  } catch (e) {
    console.warn("plex pin json parse failed", e);
    try {
      notify && notify("Invalid response");
    } catch (_) {}
    return;
  }

  if (!data || data.ok === false) {
    console.warn("plex pin error payload", data);

    try {
      notify && notify(data && data.error ? data.error : "PIN request failed");
    } catch (_) {}
    return;
  }

  try {
    const pin = data.code || data.pin || data.id || "";
    const pinEl = document.getElementById("plex_pin");

    if (pinEl) pinEl.value = pin;
    try {
      console.debug("Plex PIN received", data);
      document
        .querySelectorAll('#plex_pin, input[name="plex_pin"]')
        .forEach((el) => {
          try {
            el.value = pin;
          } catch (_) {}
        });

      const msg = document.getElementById("plex_msg");

      if (msg) {
        msg.textContent = pin ? "PIN: " + pin : "PIN request ok";
        msg.classList.remove("hidden");
      }
    } catch (_) {}

    if (pin) {
      try {
        await navigator.clipboard.writeText(pin);
      } catch (_) {}
    }
  } catch (e) {
    console.warn("pin ui update failed", e);
  }

  try {
    

    if (win && !win.closed) {
      win.focus();
    }
  } catch (_) {}

  try {
    

    if (typeof startPlexTokenPoll === "function") startPlexTokenPoll(data);
  } catch (e) {
    console.warn("startPlexTokenPoll error", e);
  }
}

function setSimklSuccess(show) {
  document.getElementById("simkl_msg").classList.toggle("hidden", !show);
}

function isPlaceholder(v, ph) {
  return (v || "").trim().toUpperCase() === ph.toUpperCase();
}

function updateSimklButtonState() {
  try {
    const cid  = (document.getElementById("simkl_client_id")?.value || "").trim();
    const sec  = (document.getElementById("simkl_client_secret")?.value || "").trim();
    const btn  = document.getElementById("simkl_start_btn");
    const hint = document.getElementById("simkl_hint");
    const rid  = document.getElementById("redirect_uri_preview");
    if (rid) rid.textContent = location.origin + "/callback";
    const ok = cid.length > 0 && sec.length > 0;
    if (btn)  btn.disabled = !ok;
    if (hint) hint.classList.toggle("hidden", ok);
  } catch (e) {
    console.warn("updateSimklButtonState failed", e);
  }
}

async function copyRedirect() {
  try {
    await navigator.clipboard.writeText(computeRedirectURI());
  } catch (_) {}
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

let simklPollTimer = null;
let simklCountdownTimer = null;

async function startSimkl() {
  try { setSimklSuccess && setSimklSuccess(false); } catch (_) {}

  // persist current Client ID/Secret first (no-op if unchanged)
  if (typeof saveSettings === "function") {
    try { await saveSettings(); } catch (_) {}
  }

  const origin = window.location.origin;
  const j = await fetch("/api/simkl/authorize", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ origin }),
    cache: "no-store",
  }).then(r => r.json()).catch(() => null);

  if (!j?.ok || !j.authorize_url) return;

  // open consent
  window.open(j.authorize_url, "_blank");

  // clear any previous poll
  if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }

  const MAX_MS = 120000;  // 2 minutes
  const deadline = Date.now() + MAX_MS;
  const backoff = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
  let i = 0;

  const poll = async () => {
    if (Date.now() >= deadline) { simklPoll = null; return; }

    const settingsVisible = !!(document.getElementById("page-settings") &&
                               !document.getElementById("page-settings").classList.contains("hidden"));
    if (document.hidden || !settingsVisible) {
      simklPoll = setTimeout(poll, 5000);
      return;
    }

    let cfg = null;
    try { cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json()); } catch (_){}

    const tok = (cfg?.simkl?.access_token || cfg?.auth?.simkl?.access_token || "").trim();
    if (tok) {
      try { const el = document.getElementById("simkl_access_token"); if (el) el.value = tok; } catch (_){}
      try { setSimklSuccess && setSimklSuccess(true); } catch (_){}
      simklPoll = null;
      return;
    }

    const delay = backoff[Math.min(i, backoff.length - 1)];
    i++;
    simklPoll = setTimeout(poll, delay);
  };

  simklPoll = setTimeout(poll, 1000);
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
    PLEX: { cls: "brand-plex", icon: "/assets/PLEX.svg" },
    SIMKL: { cls: "brand-simkl", icon: "/assets/SIMKL.svg" },
  };
  return map[key] || { cls: "", icon: "" };
}

function cxBrandLogo(providerName) {
  const key = (providerName || "").toUpperCase();
  const ICONS = {
    PLEX:  "/assets/PLEX.svg",
    SIMKL: "/assets/SIMKL.svg",
    TRAKT: "/assets/TRAKT.svg",
    TMDB:  "/assets/TMDB.svg",
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
      ? `<img class="token-logo" src="/assets/${key}.svg" alt="${key}">`
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
  const typ = item.type === "tv" || item.type === "show" ? "tv" : "movie";
  const tmdb = item.tmdb;
  if (!tmdb) return null;
  const cb = window._lastSyncEpoch || 0;
  return `/art/tmdb/${typ}/${tmdb}?size=${encodeURIComponent(
    size || "w342"
  )}&cb=${cb}`;
}

async function loadWall() {
  // Hide early if watchlist feature isn’t enabled in any pair
  try {
    const enabled = typeof isWatchlistEnabledInPairs === "function"
      ? await isWatchlistEnabledInPairs()
      : true; // fallback if helper isn’t present
    const card = document.getElementById("placeholder-card");
    if (!enabled) { card?.classList.add("hidden"); return; }
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
      case "trakt_only": return { text: "TRAKT",   cls: "p-tr" }; // ensure .p-tr exists in CSS
      default:           return { text: "—",       cls: "p-sk" };
    }
  }

  try {
    // filtered server call; falls back to client filter if needed
    const data = await fetch("/api/state/wall?both_only=1&active_only=1", { cache: "no-store" }).then(r => r.json());
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

    for (const it of items) {
      if (!it.tmdb) continue;

      const a = document.createElement("a");
      a.className = "poster";
      a.href = `https://www.themoviedb.org/${it.type}/${it.tmdb}`;
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
          const cb = window._lastSyncEpoch || 0;
          const meta = await fetch(`/api/tmdb/meta/${it.type}/${it.tmdb}?cb=${cb}`).then(r => r.json());
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
    await loadWall();
    window.wallLoaded = true;
  } catch (e) {
    console.error("Failed to update watchlist preview:", e);
  }
}

async function hasTmdbKey() {
  try {
    const cfg = await fetch("/api/config").then((r) => r.json());
    return !!(cfg.tmdb?.api_key || "").trim();
  } catch {
    return false;
  }
}

function isOnMain() {
  return !document.getElementById("ops-card").classList.contains("hidden");
}

async function updatePreviewVisibility() {
  const card = document.getElementById("placeholder-card");
  const row  = document.getElementById("poster-row");
  const msg  = document.getElementById("wall-msg");
  if (!card) return false;

  const hideAll = () => {
    card.classList.add("hidden");
    if (row) { row.innerHTML = ""; row.classList.add("hidden"); }
    if (msg) msg.textContent = "";
    window.wallLoaded = false;
  };

  if (!isOnMain?.()) { hideAll(); return false; }

  let hasKey = false, wlEnabled = false;
  try { hasKey = await hasTmdbKey?.(); } catch {}
  try { wlEnabled = await isWatchlistEnabledInPairs?.(); } catch {}

  if (!hasKey || !wlEnabled) { hideAll(); return false; }

  card.classList.remove("hidden");

  if (!window.wallLoaded) {
    try { await loadWall?.(); window.wallLoaded = true; } catch {}
  }
  return true;
}

showTab("main");

let _bootPreviewTriggered = false;

window.wallLoaded = false;

document.addEventListener("DOMContentLoaded", async () => {
  if (_bootPreviewTriggered) return;
  _bootPreviewTriggered = true;
  try { await updatePreviewVisibility(); } catch {}
});

window.addEventListener("storage", (event) => {
  if (event.key === "wl_hidden") {
    updatePreviewVisibility();
    window.dispatchEvent(new CustomEvent("watchlist-hidden-changed"));
  }
});

async function resolvePosterUrl(entity, id, size = "w342") {
  if (!id) return null;
  const typ = entity === "tv" || entity === "show" ? "tv" : "movie";
  const cb = window._lastSyncEpoch || 0;

  
  const res = await fetch(`/api/tmdb/meta/${typ}/${id}`);
  if (!res.ok) return null;

  const meta = await res.json();
  if (!meta.images || !meta.images.poster?.length) return null;

  
  return `/art/tmdb/${typ}/${id}?size=${encodeURIComponent(size)}&cb=${cb}`;
}

async function mountAuthProviders() {
  try {
    const res = await fetch("/api/auth/providers/html", { cache: "no-store" });
    if (!res.ok) return;

    const html = await res.text();
    const slot = document.getElementById("auth-providers");
    if (slot) slot.innerHTML = html;

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
  try {
    mountMetadataProviders();
  } catch (_) {}
});

try {
  Object.assign(window, { showTab, requestPlexPin, renderConnections });
} catch (e) {
  console.warn("Global export failed", e);
}


function copyTraktRedirect() {
  try {
    const uri = "urn:ietf:wg:oauth:2.0:oob"; 
    navigator.clipboard.writeText(uri);
    const codeEl = document.getElementById("trakt_redirect_uri_preview");
    if (codeEl) codeEl.textContent = uri;
    notify?.("Redirect URI copied ✓");
  } catch (e) {
    console.warn("copyTraktRedirect failed", e);
  }
}

if (typeof updateSimklHint !== "function") {
  function updateSimklHint() {}
}

function startPlexTokenPoll() {
  try { if (plexPoll) clearTimeout(plexPoll); } catch (_) {}
  const MAX_MS = 120000; 
  const deadline = Date.now() + MAX_MS;
  const backoff = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
  let i = 0;

  const poll = async () => {
    if (Date.now() >= deadline) { plexPoll = null; return; }

    
    const settingsVisible = !!(document.getElementById("page-settings") && !document.getElementById("page-settings").classList.contains("hidden"));
    if (document.hidden || !settingsVisible) {
      plexPoll = setTimeout(poll, 5000);
      return;
    }

    let cfg = null;
    try {
      cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
    } catch (_) {}

    const tok = cfg?.plex?.account_token || "";
    if (tok) {
      try {
        const el = document.getElementById("plex_token");
        if (el) el.value = tok;
      } catch (_) {}
      try { setPlexSuccess && setPlexSuccess(true); } catch (_) {}
      plexPoll = null;
      return;
    }

    const delay = backoff[Math.min(i, backoff.length - 1)];
    i++;
    plexPoll = setTimeout(poll, delay);
  };

  plexPoll = setTimeout(poll, 1000);
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
  
/* [moved to modals.js] */

/* #-------------PASCAL----END----- modal-template-_ensureCfgModal */
/* #-------------PASCAL----END----- modal-template-_ensureCfgModal */
/* #-------------PASCAL----END----- modal-template-_ensureCfgModal */

  
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

  
  async function cxSavePair(data) {
    try {
      const modal = _getModal();
      const editingId =
        modal && modal.dataset ? (modal.dataset.editingId || "").trim() : "";

      
      if (
        !editingId &&
        Array.isArray(window.cx.pairs) &&
        window.cx.pairs.some(
          (x) =>
            String(x.source || "").toUpperCase() ===
              String(data.source || "").toUpperCase() &&
            String(x.target || "").toUpperCase() ===
              String(data.target || "").toUpperCase()
        )
      ) {
        alert("This connection already exists.");
        return;
      }

      
      const F = (data && data.features) || {};
      const DEF = { enable: true, add: true, remove: false };
      function norm(feat) {
        const v = Object.assign({}, DEF, feat || {});
        return {
          enable: !!v.enable,
          add: !!v.add,
          remove: !!v.remove,
        };
      }

      const features = {
        watchlist: norm(F.watchlist),
      };
      if (F.ratings)   features.ratings   = norm(F.ratings);
      if (F.history)   features.history   = norm(F.history);
      if (F.playlists) features.playlists = norm(F.playlists);

      const payload = {
        source: data.source,
        target: data.target,
        mode: data.mode || "one-way",
        enabled: !!data.enabled,
        features,
      };

      let ok = false, r;
      if (editingId) {
        r = await fetch(`/api/pairs/${encodeURIComponent(editingId)}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        ok = r && r.ok;
      } else {
        r = await fetch("/api/pairs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        ok = r && r.ok;
      }

      if (!ok) {
        const msg = r ? `${r.status} ${r.statusText}` : "network";
        console.warn("[cx] save failed:", msg);
        alert("Failed to save connection.");
        return;
      }

      
      if (modal && modal.dataset) modal.dataset.editingId = "";
      try { window.cx.connect = { source: null, target: null }; } catch (_) {}
      try { if (typeof window.cxCloseModal === "function") window.cxCloseModal(); } catch (_) {}
      const close = document.getElementById("cx-modal");
      if (close) close.classList.add("hidden");

      await loadPairs();
    } catch (e) {
      console.warn("[cx] cxSavePair error", e);
      alert("Failed to save connection.");
    }
  }
  try { window.cxSavePair = cxSavePair; } catch (_) {}

  
  const _olderOpen = window.cxOpenModalFor;
  window.cxOpenModalFor = async function (pair, editingId) {
    
    if (typeof _olderOpen === "function") {
      try { await _olderOpen(pair, editingId); } catch (_) {}
    }

    
    try {
      if (typeof cxEnsureCfgModal === "function") {
        await cxEnsureCfgModal();
      } else if (typeof _ensureCfgModal === "function") {
        _ensureCfgModal();
      }
    } catch (_) {}

  
  const __wait = (pred, ms = 1500, step = 25) =>
    new Promise((res) => { const t0 = Date.now(); (function loop(){ if (pred() || Date.now() - t0 >= ms) return res(); setTimeout(loop, step); })(); });

  const m = document.getElementById("cx-modal") || (typeof _getModal === "function" ? _getModal() : null);
  if (!m) return;
  if (m.dataset) m.dataset.editingId = String(editingId || (pair && pair.id) || "");

  const q = (sel) => m.querySelector(sel) || document.querySelector(sel);

  await __wait(() => {
    const s = q("#cx-src"), d = q("#cx-dst");
    return !!(s && d && s.querySelectorAll("option").length && d.querySelectorAll("option").length);
  });

  try {
    const src = q("#cx-src");
    const dst = q("#cx-dst");
    const one = q("#cx-mode-one") || q('input[name="cx-mode"][value="one-way"], input[name="cx-mode"][value="one"]');
    const two = q("#cx-mode-two") || q('input[name="cx-mode"][value="two-way"], input[name="cx-mode"][value="two"]');
    const en  = q("#cx-enabled");

    
    if (src) { src.value = (pair && pair.source) || "PLEX"; try { src.dispatchEvent(new Event("change")); } catch(_) {} }
    if (dst) { dst.value = (pair && pair.target) || "SIMKL"; try { dst.dispatchEvent(new Event("change")); } catch(_) {} }

    
    if (en) en.checked = !(pair && pair.enabled === false);

    
    if (one && two) {
      let mval = (pair && pair.mode) || "one-way";
      if (mval === "one") mval = "one-way";
      if (mval === "two") mval = "two-way";
      two.checked = mval === "two-way";
      one.checked = !two.checked;
    }

    
    const f = (pair && pair.features && pair.features.watchlist) || {};
    const wlEnable = q("#cx-wl-enable");
    const wlAdd    = q("#cx-wl-add");
    const wlRem    = q("#cx-wl-remove");

    const wlOn = ("enable" in f) ? !!f.enable : true;
    if (wlEnable) {
      wlEnable.checked = wlOn;
      try { wlEnable.dispatchEvent(new Event("change")); } catch(_) {}
    }
    if (wlAdd) wlAdd.checked = !!f.add;
    if (wlRem) {
      wlRem.checked = !!f.remove;
      wlRem.disabled = !wlOn;      
    }

    m.classList.remove("hidden");

    
    await new Promise(r => setTimeout(r, 0));
    if (src && pair && pair.source) { src.value = pair.source; try { src.dispatchEvent(new Event("change")); } catch(_) {} }
    if (dst && pair && pair.target) { dst.value = pair.target; try { dst.dispatchEvent(new Event("change")); } catch(_) {} }

    
    if (wlEnable) { try { wlEnable.dispatchEvent(new Event("change")); } catch(_) {} }
    if (wlRem) wlRem.disabled = !(wlEnable ? wlEnable.checked : wlOn);

  } catch (_) {}
};

  
  document.addEventListener("DOMContentLoaded", () => {
    try {
      loadPairs();
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


// a11y: auto-associate labels with controls if missing
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

/* ==== END crosswatch.core.fixed4.js ==== */

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
        // Prefer explicit pages: #page-main/#page-watchlist/#page-settings
        var pages = document.querySelectorAll("#page-main, #page-watchlist, #page-settings, .tab-page");
        pages.forEach(function(el){ el.classList.add("hidden"); });
        var target = document.getElementById("page-" + id) || document.getElementById(id);
        if (target) target.classList.remove("hidden");

        // Toggle tab headers if present
        ["main","watchlist","settings"].forEach(function(name){
          var th = document.getElementById("tab-" + name);
          if (th) th.classList.toggle("active", name === id);
        });

        // Mount new Watchlist UI when needed
        if (id === "watchlist") {
          try { window.Watchlist?.mount?.(document.getElementById("page-watchlist")); } catch (e) { console.warn(e); }
        }

        // Fire optional hook
        document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id } }));
      } catch(e) {
        console.warn("showTab fallback failed:", e);
      }
    };
  }
})();

/* Ensure showTab is global at end */
window.showTab = showTab;

