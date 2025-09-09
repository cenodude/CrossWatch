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
// --- status fetch & render ---
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

// ==== CONNECTOR STATUS (drop-in) ============================================
const STATUS_CACHE_KEY = "cw.status.v1";

// cache helpers
function saveStatusCache(providers) {
  try { localStorage.setItem(STATUS_CACHE_KEY, JSON.stringify({ providers, updatedAt: Date.now() })); } catch {}
}
function loadStatusCache() {
  try { return JSON.parse(localStorage.getItem(STATUS_CACHE_KEY) || "null"); } catch { return null; }
}

// state: "ok" | "no" | "unknown"
function connState(v) {
  if (v == null) return "unknown";
  if (typeof v === "boolean") return v ? "ok" : "no";
  if (typeof v === "object") {
    if ("connected"  in v) return v.connected  ? "ok" : "no";
    if ("ok"         in v) return v.ok         ? "ok" : "no";
    if ("authorized" in v) return v.authorized ? "ok" : "no";
    if ("auth"       in v) return v.auth       ? "ok" : "no";
    if ("status"     in v) {
      const s = String(v.status).toLowerCase();
      if (/(ok|connected|authorized|active|ready|valid|true)/.test(s)) return "ok";
      if (/(no|not|disconnected|error|fail|expired|unauth|invalid|false)/.test(s)) return "no";
      return "unknown";
    }
  }
  return v ? "ok" : "no";
}

// raakt exact jouw markup (#badge-plex / #badge-simkl)
function setBadge(id, providerName, state, stale) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove("ok", "no", "unknown", "stale");
  el.classList.add(state);
  if (stale) el.classList.add("stale");

  const label = `${providerName}: ${state === "ok" ? "Connected" : state === "no" ? "Not connected" : "Unknown"}`;
  el.innerHTML = `<span class="dot ${state}"></span>${label}`;
}

function renderConnectorStatus(providers, { stale = false } = {}) {
  const p = providers || {};
  setBadge("badge-plex",  "Plex",  connState(p.PLEX),  stale);
  setBadge("badge-simkl", "SIMKL", connState(p.SIMKL), stale);
}

// NOTE: verwacht dat je elders deze bestaan hebt:
//   let lastStatusMs = 0;
//   const STATUS_MIN_INTERVAL = 1000;
//   let appDebug = false;
//   function recomputeRunDisabled(){}
//   function setRefreshBusy(b){}

async function refreshStatus(force = false) {
  const now = Date.now();
  if (!force && typeof lastStatusMs !== "undefined" && typeof STATUS_MIN_INTERVAL !== "undefined" && (now - lastStatusMs < STATUS_MIN_INTERVAL)) return;
  if (typeof lastStatusMs !== "undefined") lastStatusMs = now;

  try {
    const r = await fetch("/api/status" + (force ? "?fresh=1" : ""), { cache: "no-store" }).then(r => r.json());
    if (typeof appDebug !== "undefined") appDebug = !!r.debug;

    const providers = r.providers ?? {
      PLEX:  { connected: !!r.plex_connected },
      SIMKL: { connected: !!r.simkl_connected }
    };

    renderConnectorStatus(providers, { stale: false });
    saveStatusCache(providers);

    // booleans voor je UI flags
    window._ui = window._ui || {};
    window._ui.status = {
      can_run: !!r.can_run,
      plex_connected: !!(providers.PLEX  && (providers.PLEX.connected  ?? providers.PLEX.ok)),
      simkl_connected: !!(providers.SIMKL && (providers.SIMKL.connected ?? providers.SIMKL.ok)),
    };

    if (typeof recomputeRunDisabled === "function") recomputeRunDisabled?.();

    // bestaande layout toggles (optioneel)
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

async function manualRefreshStatus() {
  const btn = document.getElementById("btn-status-refresh");
  btn?.classList.add("spin");
  if (typeof setRefreshBusy === "function") setRefreshBusy(true);
  try {
    // toon cached status (stale) terwijl we live ophalen
    const cached = loadStatusCache();
    if (cached?.providers) renderConnectorStatus(cached.providers, { stale: true });
    await refreshStatus(true);
  } catch (e) {
    console.warn("Manual status refresh failed", e);
  } finally {
    if (typeof setRefreshBusy === "function") setRefreshBusy(false);
    btn?.classList.remove("spin");
  }
}

// expose voor onclick in HTML
window.manualRefreshStatus = manualRefreshStatus;
window.refreshStatus = refreshStatus;
window.renderConnectorStatus = renderConnectorStatus;

// hydrate bij pageload (cached → niet rood)
document.addEventListener("DOMContentLoaded", () => {
  const cached = loadStatusCache();
  if (cached?.providers) renderConnectorStatus(cached.providers, { stale: true });
});


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
  for (const k of order) {
    if (tl && tl[k]) done++;
  }
  let pct = (done / order.length) * 100;
  if (pct > 0 && pct < 15) pct = 15;
  setRunProgress(pct);
}

function recomputeRunDisabled() {
  const btn = document.getElementById("run");
  if (!btn) return;
  const busyNow = !!window.busy;
  const canRun = !window._ui?.status ? true : !!window._ui.status.can_run;
  const running = !!(window._ui?.summary && window._ui.summary.running);
  btn.disabled = busyNow || running || !canRun;
}

function setTimeline(tl) {
  ["start", "pre", "post", "done"].forEach((k) => {
    document.getElementById("tl-" + k).classList.toggle("on", !!(tl && tl[k]));
  });
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
  const pageSettings = document.getElementById("page-settings");
  const pageWatchlist = document.getElementById("page-watchlist");
  const logPanel = document.getElementById("log-panel");
  const layout = document.getElementById("layout");
  const statsCard = document.getElementById("stats-card");

  document.getElementById("tab-main")?.classList.toggle("active", n === "main");
  document
    .getElementById("tab-watchlist")
    ?.classList.toggle("active", n === "watchlist");
  document
    .getElementById("tab-settings")
    ?.classList.toggle("active", n === "settings");
  document.getElementById("ops-card")?.classList.toggle("hidden", n !== "main");
  document
    .getElementById("placeholder-card")
    ?.classList.toggle("hidden", n !== "main");
  statsCard?.classList.toggle("hidden", n !== "main");

  pageWatchlist?.classList.toggle("hidden", n !== "watchlist");
  pageSettings?.classList.toggle("hidden", n !== "settings");

  const hasStats = !!(statsCard && !statsCard.classList.contains("hidden"));
  if (n === "main") {
    layout.classList.remove("single");
    layout.classList.toggle("full", !appDebug && !hasStats);

    if (AUTO_STATUS) refreshStatus(false); // 👈 alleen nog bij AUTO_STATUS

    if (!esSum) openSummaryStream();
    refreshSchedulingBanner();
    refreshStats(true);
    window.wallLoaded = false;
    try {
      await updatePreviewVisibility();
    } catch (e) {
      console.warn("updatePreviewVisibility failed", e);
    }
  } else {
    layout.classList.add("single");
    layout.classList.remove("full");
  logPanel?.classList.add("hidden"); // ook null-safe maken
}

    if (n === "watchlist") {
      loadWatchlist();
    } else {
      
      try {
        await loadConfig();
      } catch (e) {
        console.warn("loadConfig failed", e);
      }
      updateTmdbHint?.();
      updateSimklHint?.();
      updateSimklButtonState?.();
      loadScheduling?.();
    }
  }

try { window.cxEnsureCfgModal = cxEnsureCfgModal; } catch(_) {}


function toggleSection(id) {
  document.getElementById(id).classList.toggle("open");
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


/* Update Check */
async function checkForUpdate(
/* #-------------PASCAL----END----- insights */
) {
  try {
    const r = await fetch("/api/version", { cache: "no-store" });
    if (!r.ok) throw new Error("HTTP " + r.status);

    const j = await r.json();
    const cur    = String(j.current ?? "0.0.0").trim();
    const latest = (String(j.latest ?? "") || "").trim() || null;
    const url    = j.html_url || "https://github.com/cenodude/plex-simkl-watchlist-sync/releases";
    const hasUpdate = !!j.update_available;

    
    const vEl = document.getElementById("app-version");
    if (vEl) vEl.textContent = `Version ${cur}`;

    
    const updEl = document.getElementById("st-update");
    if (!updEl) return;
    updEl.classList.add("badge", "upd");

    if (hasUpdate && latest) {
      const prev = updEl.dataset.lastLatest || "";
      const changed = latest !== prev;

      updEl.innerHTML = `<a href="${url}" target="_blank" rel="noopener" title="Open release page">Update ${latest} available</a>`;
      updEl.setAttribute("aria-label", `Update ${latest} available`);
      updEl.classList.remove("hidden");

      if (changed) {
        updEl.dataset.lastLatest = latest;
        updEl.classList.remove("reveal");
        void updEl.offsetWidth; 
        updEl.classList.add("reveal");
      }
    } else {
      updEl.classList.add("hidden");
      updEl.classList.remove("reveal");
      updEl.removeAttribute("aria-label");
      updEl.textContent = "";
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
    loadWatchlist?.();
    refreshSchedulingBanner?.();
  }

  _wasRunning = !!sum.running;
}


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

document.addEventListener("DOMContentLoaded", () => {
  refreshStats(true);
});

const _origRenderSummary =
  typeof renderSummary === "function" ? renderSummary : null;

window.renderSummary = function (sum) {
  if (_origRenderSummary) _origRenderSummary(sum);

  refreshStats(false);
};

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

  esDet.onmessage = (ev) => {
    if (!ev?.data) return;

    if (ev.data === "::CLEAR::") {
      el.textContent = "";
      detBuf = "";
      return;
    }

    const { tokens, buf } = CF.processChunk(detBuf, ev.data);
    detBuf = buf;

    for (const tok of tokens) CF.renderInto(el, tok, window.appDebug);

    if (detStickBottom) el.scrollTop = el.scrollHeight;
    updateSlider();
  };

  esDet.onerror = () => {
    try { esDet?.close(); } catch (_) {}
    esDet = null;

    if (detBuf && detBuf.trim()) {
      const { tokens } = CF.processChunk("", detBuf);
      detBuf = "";
      for (const tok of tokens) CF.renderInto(el, tok, window.appDebug);
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




async function loadConfig() {
  const cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());

  
  window._cfgCache = cfg;

  
  _setVal("mode",   cfg.sync?.bidirectional?.mode || "two-way");
  _setVal("source", cfg.sync?.bidirectional?.source_of_truth || "plex");

  
  _setVal("debug", String(!!cfg.runtime?.debug));

  
  setValIfExists("plex_token",           cfg.plex?.account_token   || "");
  setValIfExists("simkl_client_id",      cfg.simkl?.client_id      || "");
  setValIfExists("simkl_client_secret",  cfg.simkl?.client_secret  || "");
  setValIfExists("simkl_access_token",   cfg.simkl?.access_token   || "");
  setValIfExists("tmdb_api_key",         cfg.tmdb?.api_key         || "");
  setValIfExists("trakt_client_id",      cfg.trakt?.client_id      || "");
  setValIfExists("trakt_client_secret",  cfg.trakt?.client_secret  || "");
  setValIfExists("trakt_token",
    (cfg.auth?.trakt?.access_token) || (cfg.trakt?.access_token) || ""
  );

  
  const s = cfg.scheduling || {};
  _setVal("schEnabled", String(!!s.enabled));
  _setVal("schMode",    typeof s.mode === "string" && s.mode ? s.mode : "hourly");
  _setVal("schN",       Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "2");
  _setVal("schTime",    typeof s.daily_time === "string" && s.daily_time ? s.daily_time : "03:30");
  if (document.getElementById("schTz")) _setVal("schTz", s.timezone || "");

  
  try { updateSimklButtonState(); } catch {}
  try { updateSimklHint?.();      } catch {}
  try { updateTmdbHint?.();       } catch {}
}

function _getVal(id) {
  const el = document.getElementById(id);
  return (el && typeof el.value === "string" ? el.value : "").trim();
}

async function saveSettings() {
  const toast = document.getElementById("save_msg");
  const showToast = (text, ok = true) => {
    if (!toast) return;
    toast.classList.remove("hidden", "ok", "warn");
    toast.classList.add(ok ? "ok" : "warn");
    toast.textContent = text;
    setTimeout(() => toast.classList.add("hidden"), 2000);
  };

  try {
    const serverResp = await fetch("/api/config", { cache: "no-store" });
    if (!serverResp.ok) throw new Error(`GET /api/config ${serverResp.status}`);
    const serverCfg = await serverResp.json();
    const cfg =
      typeof structuredClone === "function"
        ? structuredClone(serverCfg)
        : JSON.parse(JSON.stringify(serverCfg || {}));

    const norm = (s) => (s ?? "").trim();
    let changed = false;

    // --- UI reads
    const uiMode    = _getVal("mode");
    const uiSource  = _getVal("source");
    const uiDebug   = _getVal("debug") === "true";
    const uiPlexTok = _getVal("plex_token");
    const uiCid     = _getVal("simkl_client_id");
    const uiSec     = _getVal("simkl_client_secret");
    const uiTmdb    = _getVal("tmdb_api_key");
    const uiTraktCid= _getVal("trakt_client_id");
    const uiTraktSec= _getVal("trakt_client_secret");

    // --- Prev values
    const prevMode    = serverCfg?.sync?.bidirectional?.mode || "two-way";
    const prevSource  = serverCfg?.sync?.bidirectional?.source_of_truth || "plex";
    const prevDebug   = !!serverCfg?.runtime?.debug;
    const prevPlex    = norm(serverCfg?.plex?.account_token);
    const prevCid     = norm(serverCfg?.simkl?.client_id);
    const prevSec     = norm(serverCfg?.simkl?.client_secret);
    const prevTmdb    = norm(serverCfg?.tmdb?.api_key);
    const prevTraktCid= norm(serverCfg?.trakt?.client_id);
    const prevTraktSec= norm(serverCfg?.trakt?.client_secret);

    // --- Apply changes into cfg
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

    if (norm(uiPlexTok) !== prevPlex) {
      cfg.plex = cfg.plex || {};
      if (norm(uiPlexTok)) cfg.plex.account_token = norm(uiPlexTok);
      else delete cfg.plex.account_token;
      changed = true;
    }

    if (norm(uiCid) !== prevCid) {
      cfg.simkl = cfg.simkl || {};
      if (norm(uiCid)) cfg.simkl.client_id = norm(uiCid);
      else delete cfg.simkl.client_id;
      changed = true;
    }
    if (norm(uiSec) !== prevSec) {
      cfg.simkl = cfg.simkl || {};
      if (norm(uiSec)) cfg.simkl.client_secret = norm(uiSec);
      else delete cfg.simkl.client_secret;
      changed = true;
    }

    if (norm(uiTraktCid) !== prevTraktCid) {
      cfg.trakt = cfg.trakt || {};
      if (norm(uiTraktCid)) cfg.trakt.client_id = norm(uiTraktCid);
      else delete cfg.trakt.client_id;
      changed = true;
    }
    if (norm(uiTraktSec) !== prevTraktSec) {
      cfg.trakt = cfg.trakt || {};
      if (norm(uiTraktSec)) cfg.trakt.client_secret = norm(uiTraktSec);
      else delete cfg.trakt.client_secret;
      changed = true;
    }

    if (norm(uiTmdb) !== prevTmdb) {
      cfg.tmdb = cfg.tmdb || {};
      if (norm(uiTmdb)) cfg.tmdb.api_key = norm(uiTmdb);
      else delete cfg.tmdb.api_key;
      changed = true;
    }

    // --- Persist config if changed
    if (changed) {
      const postCfg = await fetch("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(cfg),
      });
      if (!postCfg.ok) throw new Error(`POST /api/config ${postCfg.status}`);
      try { await loadConfig(); } catch {}
    }

    // --- Scheduling save (best effort)
    try {
      const schPayload = {
        enabled: _getVal("schEnabled") === "true",
        mode: _getVal("schMode"),
        every_n_hours: parseInt(_getVal("schN") || "2", 10),
        daily_time: _getVal("schTime") || "03:30",
        timezone: (_getVal("schTz") || "") || undefined,
      };
      const postSch = await fetch("/api/scheduling", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(schPayload),
      });
      if (!postSch.ok) throw new Error(`POST /api/scheduling ${postSch.status}`);
    } catch (e) {
      console.warn("saveSettings: scheduling failed", e);
    }

    // --- One-shot connector test 
    const authChanged =
      norm(uiPlexTok) !== prevPlex ||
      norm(uiCid)     !== prevCid  ||
      norm(uiSec)     !== prevSec  ||
      norm(uiTraktCid)!== prevTraktCid ||
      norm(uiTraktSec)!== prevTraktSec;

    if (authChanged) {
      try { await refreshStatus(true); } catch {}
    }

    // --- Misc UI updates
    try { updateTmdbHint?.(); } catch {}
    try { updateSimklState?.(); } catch {}
    try { await updateWatchlistTabVisibility?.(); } catch {}
    try { await loadScheduling?.(); } catch {}
    try { updateTraktHint?.(); } catch {}

    showToast("Settings saved ✓", true);
  } catch (err) {
    console.error("saveSettings failed", err);
    showToast("Save failed — see console", false);
  }
}


async function loadScheduling() {
  try {
    const res = await fetch("/api/scheduling", { cache: "no-store" });
    const s = await res.json();
    
    console.debug("[UI] /api/scheduling ->", s);
    const en = document.getElementById("schEnabled");
    const mo = document.getElementById("schMode");
    const nh = document.getElementById("schN");
    const ti = document.getElementById("schTime");

    if (!en || !mo || !nh || !ti) {
      console.warn("[UI] scheduling controls not found in DOM");
      return;
    }

    
    const valEnabled = s && s.enabled === true ? "true" : "false";
    const valMode =
      s && typeof s.mode === "string" && s.mode ? s.mode : "hourly";

    const valN =
      s && Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "2";

    const valTime =
      s && typeof s.daily_time === "string" && s.daily_time
        ? s.daily_time
        : "03:30";

    
    en.value = valEnabled;
    mo.value = valMode;
    nh.value = valN;
    ti.value = valTime;

    
    en.dispatchEvent(new Event("change"));
    mo.dispatchEvent(new Event("change"));
    nh.dispatchEvent(new Event("change"));
    ti.dispatchEvent(new Event("change"));
  } catch (e) {
    console.warn("Failed to load scheduling config", e);
  }

  refreshSchedulingBanner();
}

async function saveScheduling() {
  const payload = {
    enabled: document.getElementById("schEnabled").value === "true",

    mode: document.getElementById("schMode").value,

    every_n_hours: parseInt(document.getElementById("schN").value || "2", 10),

    daily_time: document.getElementById("schTime").value || "03:30",
  };

  const r = await fetch("/api/scheduling", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const j = await r.json().catch(() => ({}));

  const m = document.getElementById("schStatus");
      const _wl = (pair && pair.features && pair.features.watchlist) || {};
    const wlAdd = document.getElementById("cx-wl-add");
    const wlRem = document.getElementById("cx-wl-remove");
    if (wlAdd) wlAdd.checked = !!_wl.add;
    if (wlRem) wlRem.checked = !!_wl.remove;
    try { document.getElementById('cx-wl-enable')?.dispatchEvent(new Event('change')); } catch(_) {}
m.classList.remove("hidden");
      try { await new Promise(r=>setTimeout(r,0)); const wlAdd2=document.getElementById('cx-wl-add'); const wlRem2=document.getElementById('cx-wl-remove'); if(wlAdd2) wlAdd2.checked = !!_wl.add; if(wlRem2) wlRem2.checked = !!_wl.remove; } catch(_) {}
m.textContent = j.ok ? "Saved ✓" : "Error";

  setTimeout(() => m.classList.add("hidden"), 1500);

  refreshSchedulingBanner();
}

function refreshSchedulingBanner() {
  fetch("/api/scheduling/status")
    .then((r) => r.json())

    .then((j) => {
      const span = document.getElementById("sched-inline");

      if (!span) return;

      if (j && j.config && j.config.enabled) {
        const nextRun = j.next_run_at
          ? new Date(j.next_run_at * 1000).toLocaleString()
          : "—";

        span.textContent = `—   Scheduler running (next ${nextRun})`;

        span.style.display = "inline";
      } else {
        span.textContent = "";

        span.style.display = "none";
      }
    })

    .catch(() => {
      const span = document.getElementById("sched-inline");

      if (span) {
        span.textContent = "";
        span.style.display = "none";
      }
    });
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

async function requestTraktPin() {
  try { setTraktSuccess(false); } catch (_) {}

  let win = null;
  try {
    
    win = window.open("https://trakt.tv/activate", "_blank");
  } catch (_) {}

  let resp, data;
  try {
    resp = await fetch("/api/trakt/pin/new", { method: "POST" });
  } catch (e) {
    console.warn("trakt pin fetch failed", e);
    try { notify && notify("Failed to request Trakt code"); } catch (_) {}
    return;
  }

  try {
    data = await resp.json();
  } catch (e) {
    console.warn("trakt pin json parse failed", e);
    try { notify && notify("Invalid response"); } catch (_) {}
    return;
  }

  if (!data || data.ok === false) {
    console.warn("trakt pin error payload", data);
    try { notify && notify((data && data.error) ? data.error : "Trakt code request failed"); } catch (_) {}
    return;
  }

  const code = data.user_code || "";
  const url = data.verification_url || "https://trakt.tv/activate";

  
  try {
    const el = document.getElementById("trakt_pin");
    if (el) el.value = code;
    const msg = document.getElementById("trakt_msg");
    if (msg) {
      msg.textContent = code ? ("Code: " + code) : "Code issued";
      msg.classList.remove("hidden");
    }
  } catch (_) {}

  try { if (code) await navigator.clipboard.writeText(code); } catch (_) {}

  try {
    if (win && !win.closed) { win.location.href = url; win.focus(); }
  } catch (_) {}

  try { notify && notify("Enter the code on Trakt and wait for confirmation."); } catch (_) {}
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

  
  window.open(j.authorize_url, "_blank");

  
  if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }

  const MAX_MS = 120000; 
  const deadline = Date.now() + MAX_MS;
  const backoff = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
  let i = 0;

;

  
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
  const myReq = ++wallReqSeq;
  const card = document.getElementById("placeholder-card");
  const msg = document.getElementById("wall-msg");
  const row = document.getElementById("poster-row");

  msg.textContent = "Loading…";
  row.innerHTML = "";
  row.classList.add("hidden");
  card.classList.remove("hidden");

  const hiddenMap = new Map(
    (JSON.parse(localStorage.getItem("wl_hidden") || "[]") || []).map((k) => [
      k,
      true,
    ])
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

  try {
    const data = await fetch("/api/state/wall").then((r) => r.json());
    if (myReq !== wallReqSeq) return;

    if (data.missing_tmdb_key) {
      card.classList.add("hidden");
      return;
    }

    if (!data.ok) {
      msg.textContent = data.error || "No state data found.";
      return;
    }

    let items = data.items || [];

    _lastSyncEpoch = data.last_sync_epoch || null;

    if (items.length === 0) {
      msg.textContent = "No items to show yet.";
      return;
    }

    msg.classList.add("hidden");
    row.classList.remove("hidden");

    const firstSeen = (() => {
      try {
        return JSON.parse(localStorage.getItem("wl_first_seen") || "{}");
      } catch {
        return {};
      }
    })();

    const getTs = (it) => {
      const s =
        it.added_epoch ??
        it.added_ts ??
        it.created_ts ??
        it.created ??
        it.epoch ??
        null;

      return Number(s || firstSeen[it.key] || 0);
    };

    const now = Date.now();

    for (const it of items) {
      if (!firstSeen[it.key]) firstSeen[it.key] = now;
    }

    localStorage.setItem("wl_first_seen", JSON.stringify(firstSeen));

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
      a.dataset.key = it.key || "";

      const uiStatus = isDeleted(it) ? "deleted" : it.status;
      a.dataset.source = uiStatus;

      const img = document.createElement("img");

      img.loading = "lazy";
      img.alt = `${it.title || ""} (${it.year || ""})`;
      img.src = artUrl(it, "w342");
      a.appendChild(img);

      const ovr = document.createElement("div");
      ovr.className = "ovr";

      let pillText, pillClass;

      if (uiStatus === "deleted") {
        pillText = "DELETED";
        pillClass = "p-del";
      } else if (uiStatus === "both") {
        pillText = "SYNCED";
        pillClass = "p-syn";
      } else if (uiStatus === "plex_only") {
        pillText = "PLEX";
        pillClass = "p-px";
      } else {
        pillText = "SIMKL";
        pillClass = "p-sk";
      }

      const pill = document.createElement("div");
      pill.className = "pill " + pillClass;
      pill.textContent = pillText;
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
            <div class="chip time" id="time-${it.type}-${it.tmdb}">${
        _lastSyncEpoch ? "updated " + relTimeFromEpoch(_lastSyncEpoch) : ""
      }</div>
          </div>

        `;

      a.appendChild(hover);

      a.addEventListener(
        "mouseenter",
        async () => {
          const descEl = document.getElementById(`desc-${it.type}-${it.tmdb}`);

          if (!descEl || descEl.dataset.loaded) return;

          try {
            const cb = window._lastSyncEpoch || 0;

            const meta = await fetch(
              `/api/tmdb/meta/${it.type}/${it.tmdb}?cb=${cb}`
            ).then((r) => r.json());

            descEl.textContent = meta?.overview || "—";

            descEl.dataset.loaded = "1";
          } catch {
            descEl.textContent = "—";
            descEl.dataset.loaded = "1";
          }
        },
        { passive: true }
      );

      row.appendChild(a);
    }

    initWallInteractions();
  } catch {
    msg.textContent = "Failed to load preview.";
  }
}

async function loadWatchlist() {
  const grid = document.getElementById("wl-grid");
  const msg = document.getElementById("wl-msg");

  grid.innerHTML = "";
  grid.classList.add("hidden");
  msg.textContent = "Loading…";
  msg.classList.remove("hidden");

  try {
    const data = await fetch("/api/watchlist").then((r) => r.json());

    if (data.missing_tmdb_key) {
      msg.textContent = "Set a TMDb API key to see posters.";
      return;
    }

    if (!data.ok) {
      msg.textContent = data.error || "No state data found.";
      return;
    }

    const items = data.items || [];

    if (items.length === 0) {
      msg.textContent = "No items on your watchlist yet.";
      return;
    }

    msg.classList.add("hidden");
    grid.classList.remove("hidden");

    for (const it of items) {
      if (!it.tmdb) continue;

      const node = document.createElement("div");

      node.className = "wl-poster poster";

      node.dataset.key = it.key;

      node.dataset.type =
        it.type === "tv" || it.type === "show" ? "tv" : "movie";

      node.dataset.tmdb = String(it.tmdb || "");

      node.dataset.status = it.status;

      const pillText =
        it.status === "both"
          ? "SYNCED"
          : it.status === "plex_only"
          ? "PLEX"
          : "SIMKL";

      const pillClass =
        it.status === "both"
          ? "p-syn"
          : it.status === "plex_only"
          ? "p-px"
          : "p-sk";

      node.innerHTML = `

          <img alt="" src="${
            artUrl(it, "w342") || ""
          }" onerror="this.style.display='none'">

          <button class="wl-del icon-btn trash"
                  type="button"
                  title="Remove from Plex watchlist"
                  aria-label="Remove from Plex watchlist"
                  onclick="deletePoster(event, '${encodeURIComponent(
                    it.key
                  )}', this)">

            <svg class="ico" viewBox="0 0 24 24" aria-hidden="true">
              <path class="lid" d="M9 4h6l1 2H8l1-2z"/>
              <path d="M6 7h12l-1 13H7L6 7z"/>
              <path d="M10 11v6M14 11v6"/>
            </svg>
          </button>

          <div class="wl-ovr ovr"><span class="pill ${pillClass}">${pillText}</span></div>

          <div class="wl-cap cap">${(it.title || "").replace(/"/g, "&quot;")} ${
        it.year ? "· " + it.year : ""
      }</div>
      
          <div class="wl-hover hover">
            <div class="titleline">${it.title || ""}</div>

            <div class="meta">
              <div class="chip src">${
                it.status === "both"
                  ? "Source: Synced"
                  : it.status === "plex_only"
                  ? "Source: Plex"
                  : "Source: SIMKL"
              }</div>

              <div class="chip time">${relTimeFromEpoch(it.added_epoch)}</div>
            </div>

            <div class="desc" id="wldesc-${node.dataset.type}-${
        node.dataset.tmdb
      }">${it.tmdb ? "Fetching description…" : "—"}</div>

          </div>`;

      const hidden = new Set(
        JSON.parse(localStorage.getItem("wl_hidden") || "[]")
      );

      if (hidden.has(it.key)) {
        const pill = node.querySelector(".pill");

        pill.classList.add("p-del");
      }

      node.addEventListener(
        "mouseenter",
        async () => {
          const descEl = document.getElementById(
            `wldesc-${it.type}-${it.tmdb}`
          );

          if (!descEl || descEl.dataset.loaded) return;

          try {
            const cb = window._lastSyncEpoch || Date.now();

            const meta = await fetch(
              `/api/tmdb/meta/${it.type}/${it.tmdb}?cb=${cb}`
            ).then((r) => r.json());

            descEl.textContent = meta?.overview || "—";

            descEl.dataset.loaded = "1";
          } catch {
            descEl.textContent = "—";
            descEl.dataset.loaded = "1";
          }
        },
        { passive: true }
      );

      grid.appendChild(node);
    }
  } catch (error) {
    console.error("Error loading watchlist:", error);

    msg.textContent = "Failed to load preview.";
  }
}

async function deletePoster(ev, encKey, btnEl) {
  ev?.stopPropagation?.();
  const key = decodeURIComponent(encKey);
  const card = btnEl.closest(".wl-poster");

  
  btnEl.disabled = true;
  btnEl.classList.remove("done", "error");
  btnEl.classList.add("working");

  try {
    const res = await fetch("/api/watchlist/" + encodeURIComponent(key), {
      method: "DELETE",
    });
    if (!res.ok) throw new Error("HTTP " + res.status);

    
    if (card) {
      card.classList.add("wl-removing");

      setTimeout(() => {
        card.remove();
      }, 350);
    }

    
    const hidden = new Set(
      JSON.parse(localStorage.getItem("wl_hidden") || "[]")
    );

    hidden.add(key);

    localStorage.setItem("wl_hidden", JSON.stringify([...hidden]));
    window.dispatchEvent(new Event("storage"));
    btnEl.classList.remove("working");
    btnEl.classList.add("done");
  } catch (e) {
    console.warn("deletePoster error", e);
    btnEl.classList.remove("working");
    btnEl.classList.add("error");

    setTimeout(() => btnEl.classList.remove("error"), 1200);
  } finally {
    setTimeout(() => {
      btnEl.disabled = false;
    }, 600);
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

async function updateWatchlistTabVisibility() {
  try {
    const cfg = await fetch("/api/config").then((r) => r.json());
    const tmdbKey = (cfg.tmdb?.api_key || "").trim();
    document.getElementById("tab-watchlist").style.display = tmdbKey
      ? "block"
      : "none";
  } catch {
    document.getElementById("tab-watchlist").style.display = "none";
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
  const row = document.getElementById("poster-row");

  if (!isOnMain()) {
    card.classList.add("hidden");
    return false;
  }

  const show = await hasTmdbKey();
  if (!show) {
    card.classList.add("hidden");
    if (row) {
      row.innerHTML = "";
      row.classList.add("hidden");
    }
    window.wallLoaded = false;
    return false;
  } else {
    card.classList.remove("hidden");
    if (!window.wallLoaded) {
      await loadWall(); 
      window.wallLoaded = true;
    }
    return true;
  }
}

showTab("main");
updateWatchlistTabVisibility();

let _bootPreviewTriggered = false;

window.wallLoaded = false;

document.addEventListener("DOMContentLoaded", async () => {
  if (_bootPreviewTriggered) return;
  _bootPreviewTriggered = true;
  try { await updatePreviewVisibility(); } catch {}
});

window.addEventListener("storage", (event) => {
  if (event.key === "wl_hidden") {
    loadWatchlist();
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
    const res = await fetch("/api/auth/providers/html");
    if (!res.ok) return;
    const html = await res.text();
    const slot = document.getElementById("auth-providers");
    if (slot) {
      slot.innerHTML = html;
    }

    
    document.getElementById("btn-copy-plex-pin")
      ?.addEventListener("click", (e) => copyInputValue("plex_pin", e.currentTarget));
    document.getElementById("btn-copy-plex-token")
      ?.addEventListener("click", (e) => copyInputValue("plex_token", e.currentTarget));

    
    document.getElementById("btn-copy-trakt-pin")
      ?.addEventListener("click", (e) => copyInputValue("trakt_pin", e.currentTarget));
    document.getElementById("btn-copy-trakt-token")
      ?.addEventListener("click", (e) => copyInputValue("trakt_token", e.currentTarget));

    
    await hydrateAuthFromConfig();
    updateTraktHint();                 
    setTimeout(updateTraktHint, 0);    
    requestAnimationFrame(updateTraktHint);

  } catch (e) {
    console.warn("mountAuthProviders failed", e);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  try {
    
    Promise.resolve(mountAuthProviders()).then(() => {
      try { updateTraktHint?.(); } catch (_) {}
    });
  } catch (_) {}
});

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
  Object.assign(window, { showTab, requestPlexPin, requestTraktPin,
  renderConnections});
} catch (e) {
  console.warn("Global export failed", e);
}

if (typeof updateTraktHint !== "function") {

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

async function hydrateAuthFromConfig() {
  try {
    const r = await fetch("/api/config", { cache: "no-store" });
    if (!r.ok) return;
    const cfg = await r.json();

    
    setValIfExists("trakt_client_id",     (cfg.trakt?.client_id || "").trim());
    setValIfExists("trakt_client_secret", (cfg.trakt?.client_secret || "").trim());
    setValIfExists(
      "trakt_token",
      (cfg.auth?.trakt?.access_token) || (cfg.trakt?.access_token) || ""
    );

    
    updateTraktHint();
  } catch (_) {}
}

async function mountAuthProviders() {
  try {
    const res = await fetch("/api/auth/providers/html");
    if (!res.ok) return;
    const html = await res.text();
    const slot = document.getElementById("auth-providers");
    if (slot) slot.innerHTML = html;

    
    document.getElementById("btn-copy-plex-pin")
      ?.addEventListener("click", (e) => copyInputValue("plex_pin", e.currentTarget));
    document.getElementById("btn-copy-plex-token")
      ?.addEventListener("click", (e) => copyInputValue("plex_token", e.currentTarget));

    
    document.getElementById("btn-copy-trakt-pin")
      ?.addEventListener("click", (e) => copyInputValue("trakt_pin", e.currentTarget));
    document.getElementById("btn-copy-trakt-token")
      ?.addEventListener("click", (e) => copyInputValue("trakt_token", e.currentTarget));
    document.getElementById("trakt_client_id")
      ?.addEventListener("input", updateTraktHint);
    document.getElementById("trakt_client_secret")
      ?.addEventListener("input", updateTraktHint);

    
    await hydrateAuthFromConfig();
    updateTraktHint();
    setTimeout(updateTraktHint, 0);
    requestAnimationFrame(updateTraktHint);
  } catch (e) {
    console.warn("mountAuthProviders failed", e);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  try { mountAuthProviders(); } catch (_) {}
});

try {
  Object.assign(window, { requestTraktPin });
} catch (_) {}

async function requestTraktPin() {
  
  let win = null;
  try { win = window.open("https://trakt.tv/activate", "_blank"); } catch (_) {}

  let resp, data;
  try {
    resp = await fetch("/api/trakt/pin/new", { method: "POST" });
  } catch (e) {
    console.warn("trakt pin fetch failed", e);
    try { notify && notify("Failed to request code"); } catch (_) {}
    return;
  }
  try { data = await resp.json(); } catch (e) { data = null; }

  if (!data || data.ok === false) {
    console.warn("trakt pin error payload", data);
    try { notify && notify(data && data.error ? data.error : "Code request failed"); } catch (_) {}
    return;
  }

  const code = data.user_code || "";
  try {
    const pinEl = document.getElementById("trakt_pin");
    if (pinEl) pinEl.value = code;
    document.querySelectorAll('#trakt_pin, input[name="trakt_pin"]').forEach(el => { try { el.value = code; } catch(_){ } });

    const msg = document.getElementById("trakt_msg");
    if (msg) { msg.textContent = code ? "Code: " + code : "Code request ok"; msg.classList.remove("hidden"); }

    if (code) { try { await navigator.clipboard.writeText(code); } catch(_){} }
    if (win && !win.closed) { try { win.focus(); } catch(_){ } }
  } catch (e) {
    console.warn("trakt pin ui update failed", e);
  }
}

if (typeof saveSetting !== "function") {
  async function saveSetting(key, value) {
    try {
      await fetch("/api/settings/set", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key, value })
      });
    } catch (e) {
      console.warn("saveSetting failed", e);
    }
  }
}

if (typeof updateSimklHint !== "function") {
  function updateSimklHint() {}
}

function updateTraktHint() {
  try {
    const cid  = (document.getElementById("trakt_client_id")?.value || "").trim();
    const secr = (document.getElementById("trakt_client_secret")?.value || "").trim();
    const hint = document.getElementById("trakt_hint");
    if (!hint) return;

    const show = !(cid && secr);
    
    hint.classList.toggle("hidden", !show);
    
    hint.style.display = show ? "" : "none";
  } catch (_) {}
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
  try {
    const arr = await fetch("/api/sync/providers", { cache: "no-store" })
      .then((r) => r.json())
      .catch(() => []);
    if (!Array.isArray(arr) || !arr.length) {
      div.innerHTML = '<div class="muted">No providers discovered.</div>';
      return;
    }
    const html = arr
      .map((p) => {
        const caps = p.features || {};
        const cap = (k) => !!caps[k];
        const chip = (t, on) =>
          `<span class="badge ${
            on ? "" : "feature-disabled"
          }" style="margin-left:6px">${t}</span>`;
        return `<div class="card" style="padding:12px;display:flex;justify-content:space-between;align-items:center">
        <div style="font-weight:700">${p.label || p.name}</div>
        <div>${chip("Watchlist", cap("watchlist"))}${chip(
          "Ratings",
          cap("ratings")
        )}${chip("History", cap("history"))}${chip(
          "Playlists",
          cap("playlists")
        )}</div>
      </div>`;
      })
      .join("");
    div.innerHTML = html;
    try {
      window.cx.providers = Array.isArray(arr) ? arr : [];
    } catch (e) {
      window.cx.providers = [];
    }
    try {
      if (typeof renderConnections === "function") { renderConnections(); }
    } catch (e) {
      console.warn("renderConnections failed", e);
    }
  } catch (e) {
    div.innerHTML = '<div class="muted">Failed to load providers.</div>';
    console.warn("loadProviders error", e);
  }
}

(function () {
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
})();

try {
  window.showTab = showTab;
} catch (e) {}
try {
  window.runSync = runSync;
} catch (e) {}

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
    window.showTab = function(id){
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
        // Fire optional hook
        document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id } }));
      } catch(e) {
        console.warn("showTab fallback failed:", e);
      }
    };
  }
})();


/* Ensure showTab is global at end */
try{ window.showTab = window.showTab || showTab; }catch(_){}
