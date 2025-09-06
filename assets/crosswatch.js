// Crosswatch main JS

/* ====== Globals ====== */
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
function _getVal(id) {
  const el = document.getElementById(id);
  return el ? el.value : "";
}

function setValIfExists(id, val) {
  const el = document.getElementById(id);
  if (el) el.value = val ?? "";
}
let lastStatusMs = 0;
const STATUS_MIN_INTERVAL = 120000; // ms

let busy = false,
  esDet = null,
  esSum = null,
  plexPoll = null,
  simklPoll = null,
  appDebug = false,
  currentSummary = null;
let detStickBottom = true; // auto-stick to bottom voor details-log
let wallLoaded = false,
  _lastSyncEpoch = null,
  _wasRunning = false;
let wallReqSeq = 0;   // request sequence for loadWall
window._ui = { status: null, summary: null };

/* ====== Utilities ====== */
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

/* ====== About modal ====== */

async function openAbout() {
  try {
    // Bust any cache so we don't show an old payload

    const r = await fetch("/api/version?cb=" + Date.now(), {
      cache: "no-store",
    });

    const j = r.ok ? await r.json() : {};

    const cur = (j.current ?? "0.0.0").toString().trim();

    const latest = (j.latest ?? "").toString().trim() || null;

    const url =
      j.html_url ||
      "https://github.com/cenodude/plex-simkl-watchlist-sync/releases";

    const upd = !!j.update_available;

    // "Version x.y.z" — from CURRENT

    const verEl = document.getElementById("about-version");

    if (verEl) {
      verEl.textContent = `Version ${j.current}`;

      verEl.dataset.version = cur; // guard against later accidental overwrites
    }

    // If you also show a version elsewhere, keep it in sync (safe no-op if missing)

    const headerVer = document.getElementById("app-version");

    if (headerVer) {
      headerVer.textContent = `Version ${cur}`;

      headerVer.dataset.version = cur;
    }

    // Latest release link/label (separate from current)

    const relEl = document.getElementById("about-latest");

    if (relEl) {
      relEl.href = url;

      relEl.textContent = latest ? `v${latest}` : "Releases";

      relEl.setAttribute(
        "aria-label",
        latest ? `Latest release v${latest}` : "Releases"
      );
    }

    // Update badge

    const updEl = document.getElementById("about-update");

    if (updEl) {
      updEl.classList.add("badge", "upd");

      if (upd && latest) {
        updEl.textContent = `Update ${latest} available`;

        updEl.classList.remove("hidden", "reveal");

        void updEl.offsetWidth; // restart CSS animation

        updEl.classList.add("reveal");
      } else {
        updEl.textContent = "";

        updEl.classList.add("hidden");

        updEl.classList.remove("reveal");
      }
    }
  } catch (_) {}

  document.getElementById("about-backdrop")?.classList.remove("hidden");
}

function closeAbout(ev) {
  if (ev && ev.type === "click" && ev.currentTarget !== ev.target) return; // ignore clicks inside card

  document.getElementById("about-backdrop")?.classList.add("hidden");
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeAbout();
});

/* ====== Tabs ====== */

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
    refreshStatus();
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
    logPanel.classList.add("hidden");

    if (n === "watchlist") {
      loadWatchlist();
    } else {
      // document.getElementById("sec-auth")?.classList.add("open");
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
}

/* ====== Connector Model  cxEnsureCfgModal  ====== */
function cxEnsureCfgModal() {
  if (document.getElementById("cx-modal")) return;
  var wrap = document.createElement("div");
  wrap.id = "cx-modal";
  wrap.className = "modal-backdrop hidden";
  var h = "";
  h +=
    '<div id="cfg-conn" class="modal-card" role="dialog" aria-modal="true" aria-labelledby="cfg-title">';
  h +=
    '<div class="modal-header"><div id="cfg-title" class="title">Configure Connection</div><button type="button" class="btn-ghost" aria-label="Close" onclick="cxCloseModal()">✕</button></div>';
  h += '<div class="modal-body">';
  h += '<div class="form-grid">';
  h +=
    '<div class="field"><label>Source</label><select id="cx-src"><option>PLEX</option><option>SIMKL</option></select></div>';
  h +=
    '<div class="field"><label>Target</label><select id="cx-dst"><option>PLEX</option><option>SIMKL</option></select></div>';
  h += "</div>";
  h += '<div class="form-grid" style="margin-top:8px">';
  h += '<div class="field"><label>Mode</label><div class="seg">';
  h +=
    '<input id="cx-mode-one" type="radio" name="cx-mode" value="one-way" checked><label for="cx-mode-one">One-way</label>';
  h +=
    '<input id="cx-mode-two" type="radio" name="cx-mode" value="two-way"><label id="cx-two-label" for="cx-mode-two">Two-way</label>';
  h += "</div></div>";
  h +=
    '<div class="field"><label>Enabled</label><div class="row"><input id="cx-enabled" type="checkbox" checked><span class="fe-muted">Activate this connection</span></div></div>';
  h += "</div>";
  h += '<div class="features" style="margin-top:10px"><div class="fe-row">';
  h += '<div class="fe-name">Watchlist</div>';
  h +=
    '<label class="row"><input id="cx-wl-enable" type="checkbox" checked><span class="fe-muted">Enable</span></label>';
  h +=
    '<label class="row"><input id="cx-wl-add" type="checkbox" checked><span class="fe-muted">Add</span></label>';
  h +=
    '<label class="row"><input id="cx-wl-remove" type="checkbox" disabled><span class="fe-muted">Remove</span></label>';
  h += "</div></div>";
  h += '<div class="summary" style="margin-top:12px">';
  h +=
    '<div class="line"><div class="brand"><div class="chip" id="sum-src-chip">P</div><div id="sum-src">PLEX</div></div><div id="sum-dir" style="color:#fff;font-weight:800">→</div><div class="brand"><div class="chip" id="sum-dst-chip">S</div><div id="sum-dst">SIMKL</div></div></div>';
  h +=
    '<div class="line" style="margin-top:8px"><span>Watchlist</span><span id="sum-wl">Add</span></div>';
  h += "</div></div>";
  h +=
    '<div class="modal-footer actions"><button type="button" class="btn" onclick="cxCloseModal()">Cancel</button><button type="button" class="btn primary acc" id="cx-save">Save</button></div>';
  h += "</div>";
  wrap.innerHTML = h;
  document.body.appendChild(wrap);
  cxBindCfgEvents(); // events pas binden als DOM er is
}

function cxBindCfgEvents() {
  var ids = [
    "cx-src",
    "cx-dst",
    "cx-mode-one",
    "cx-mode-two",
    "cx-wl-enable",
    "cx-wl-add",
    "cx-wl-remove",
    "cx-enabled",
  ];
  ids.forEach(function (id) {
    var el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", function () {
      var two = document.getElementById("cx-mode-two").checked;
      var wlOn = document.getElementById("cx-wl-enable").checked;
      var rem = document.getElementById("cx-wl-remove");
      rem.disabled = !(two && wlOn);
      if (rem.disabled) rem.checked = false;
      cxUpdateSummary();
    });
  });
  var save = document.getElementById("cx-save");
  if (save) {
    save.addEventListener("click", function () {
      const data = {
        source: document.getElementById("cx-src").value,
        target: document.getElementById("cx-dst").value,
        enabled: true, // default ON; no "activate" toggle in modal
        mode: document.getElementById("cx-mode-two").checked
          ? "two-way"
          : "one-way",
        features: {
          watchlist: {
            enable: document.getElementById("cx-wl-enable").checked,
            add: document.getElementById("cx-wl-add").checked,
            remove: document.getElementById("cx-wl-remove").checked, // allowed for Mirror too
          },
        },
      };

      if (typeof window.cxSavePair === "function") {
        window.cxSavePair(data);
      } else {
        console.log("cxSavePair payload", data);
      }
      cxCloseModal();
    });
  }
}

function cxUpdateSummary() {
  var src = document.getElementById("cx-src"),
    dst = document.getElementById("cx-dst");
  if (!src || !dst) return;
  var two = document.getElementById("cx-mode-two").checked;
  var wlOn = document.getElementById("cx-wl-enable").checked;
  var wlAdd = document.getElementById("cx-wl-add").checked;
  var wlRem = document.getElementById("cx-wl-remove").checked;
  document.getElementById("sum-src").textContent = src.value;
  document.getElementById("sum-dst").textContent = dst.value;
  document.getElementById("sum-src-chip").textContent = src.value.slice(0, 1);
  document.getElementById("sum-dst-chip").textContent = dst.value.slice(0, 1);
  document.getElementById("sum-dir").textContent = two ? "↔" : "→";
  var wl = "Off";
  if (wlOn) {
    wl =
      wlAdd && wlRem ? "Add & Remove" : wlAdd ? "Add" : wlRem ? "Remove" : "On";
  }
  document.getElementById("sum-wl").textContent = wl;
}
function cxCloseModal() {
  var m = document.getElementById("cx-modal");
  if (m) m.classList.add("hidden");
}

function toggleSection(id) {
  document.getElementById(id).classList.toggle("open");
}

/* ====== Run (synchronize) ====== */

function setBusy(v) {
  busy = v;
  recomputeRunDisabled();
}
async function runSync() {
  if (busy) return;
  const btn = document.getElementById("run");
  setBusy(true);

  // clear UI log window
  const detLog = document.getElementById("det-log");
  if (detLog) detLog.textContent = "";

  // 🔑 restart EventSource so we always get fresh logs
  if (esDet) {
    try { esDet.close(); } catch (_) {}
    esDet = null;
  }
  openDetailsLog(); // reattach SSE listener

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
    refreshStatus();
  }
}

/* Version check + update notification */
const UPDATE_CHECK_INTERVAL_MS = 12 * 60 * 60 * 1000;
let _updInfo = null;
function openUpdateModal() {
  if (!_updInfo) return;
  document.getElementById("upd-modal").classList.remove("hidden");
  document.getElementById("upd-title").textContent = `v${_updInfo.latest}`;
  document.getElementById("upd-notes").textContent =
    _updInfo.notes || "(No release notes)";
  document.getElementById("upd-link").href = _updInfo.url || "#";
}

function closeUpdateModal() {
  document.getElementById("upd-modal").classList.add("hidden");
}
function dismissUpdate() {
  if (_updInfo?.latest) {
    localStorage.setItem("dismissed_version", _updInfo.latest);
  }
  document.getElementById("upd-pill").classList.add("hidden");
  closeUpdateModal();
}

/* expanded insight Statistics */
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
  // try common IDs/classes first
  return (
    document.getElementById("btn-details") ||
    document.querySelector('[data-action="details"], .btn-details') ||
    Array.from(document.querySelectorAll("button")).find(
      (b) => (b.textContent || "").trim().toLowerCase() === "view details"
    )
  );
}

function findDetailsPanel() {
  // adapt to your DOM: try several likely targets
  return (
    document.getElementById("sync-output") ||
    document.getElementById("details") ||
    document.querySelector('#sync-log, .sync-output, [data-pane="details"]')
  );
}

function wireDetailsToStats() {
  const btn = findDetailsButton();
  const panel = findDetailsPanel();

  // set initial state based on whether details are open at load
  setStatsExpanded(isElementOpen(panel));
  if (btn) {
    btn.addEventListener("click", () => {
      // wait a tick for the panel to toggle
      setTimeout(() => setStatsExpanded(isElementOpen(panel)), 50);
    });
  }

  // optional: collapse stats when starting a new sync
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

// BLOCK INSIGHT

async function fetchJSON(u) {
  const r = await fetch(u, { cache: "no-store" });
  return r.ok ? r.json() : null;
}

function scheduleInsights(max = 20) {
  let tries = 0;

  const tick = async () => {
    const hasTargets =
      document.getElementById("sync-history") ||
      document.getElementById("watchtime") ||
      document.getElementById("watchtime-note");

    if (hasTargets) {
      try {
        await refreshInsights();
      } catch (e) {
        console.warn("refreshInsights failed", e);
      }

      return;
    }

    tries++;

    if (tries < max) setTimeout(tick, 250);
  };

  setTimeout(tick, 0);
}

async function refreshInsights() {
  try {
    const data = await fetchJSON("/api/insights?limit_samples=60&history=3");

    if (!data) return;

    // 1) Sparkline

    try {
      renderSparkline("sparkline", data.series || []);
    } catch (_) {}

    // 2) History

    const hist = data.history || [];

    const hEl = document.getElementById("sync-history");

    if (hEl) {
      hEl.innerHTML =
        hist
          .map((row) => {
            const dur =
              row.duration_sec != null
                ? row.duration_sec.toFixed
                  ? row.duration_sec.toFixed(1)
                  : row.duration_sec
                : "—";

            const added = row.added ?? "—";
            const removed = row.removed ?? "—";
            const badgeClass =
              String(row.result || "").toUpperCase() === "EQUAL"
                ? "ok"
                : "warn";

            const when = toLocal(row.finished_at || row.started_at);
            return `<div class="history-item">

          <div class="history-meta">${when} • <span class="badge ${badgeClass}">${
              row.result || "—"
            }</span> • ${dur}s</div>

          <div class="history-badges">
            <span class="badge">+${added}</span>
            <span class="badge">-${removed}</span>
            <span class="badge">P:${row.plex_post ?? "—"}</span>
            <span class="badge">S:${row.simkl_post ?? "—"}</span>
          </div>
        </div>`;
          })
          .join("") ||
        `<div class="history-item"><div class="history-meta">No history yet</div></div>`;
    }

    // 3) Watchtime

    const wt = data.watchtime || {
      hours: 0,
      days: 0,
      minutes: 0,
      movies: 0,
      shows: 0,
      method: "fallback",
    };

    const wEl = document.getElementById("watchtime");

    const note = document.getElementById("watchtime-note");

    if (wEl) {
      wEl.innerHTML = `<div class="big">≈ ${wt.hours}</div>

      <div class="units">hrs <span style="opacity:.6">(${wt.days} days)</span><br>

        <span style="opacity:.8">${wt.movies} movies • ${wt.shows} shows</span>

      </div>`;
    }

    if (note) {
      note.textContent =
        wt.method === "mixed"
          ? "TMDb + SIMKL"
          : wt.method === "tmdb"
          ? "TMDb only"
          : wt.method === "simkl"
          ? "SIMKL only"
          : "estimate";
    }
  } catch (e) {
    console.warn("refreshInsights failed", e);
  }
}

// lightweight SVG sparkline with neon gradient

function renderSparkline(id, points) {
  const el = document.getElementById(id);

  if (!el) {
    return;
  }

  if (!points.length) {
    el.innerHTML = `<div class="muted">No data yet</div>`;
    return;
  }

  const w = el.clientWidth || 260,
    h = el.clientHeight || 64,
    pad = 4;

  const xs = points.map((p) => Number(p.ts) || 0);

  const ys = points.map((p) => Number(p.count) || 0);

  const minX = Math.min(...xs),
    maxX = Math.max(...xs);

  const minY = Math.min(...ys),
    maxY = Math.max(...ys);

  const x = (t) =>
    maxX === minX ? pad : pad + ((w - 2 * pad) * (t - minX)) / (maxX - minX);

  const y = (v) =>
    maxY === minY
      ? h / 2
      : h - pad - ((h - 2 * pad) * (v - minY)) / (maxY - minY);

  const d = points
    .map((p, i) => (i ? "L" : "M") + x(p.ts) + "," + y(p.count))
    .join(" ");

  const dots = points
    .map(
      (p) => `<circle class="dot" cx="${x(p.ts)}" cy="${y(p.count)}"></circle>`
    )
    .join("");

  el.innerHTML = `
    <svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
      <defs>
        <linearGradient id="spark-grad" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0" stop-color="var(--grad1,#7c5cff)"/>
          <stop offset="1" stop-color="var(--grad2,#2de2ff)"/>
        </linearGradient>
      </defs>
      <path class="line" d="${d}"></path>
      ${dots}
    </svg>`;
}
document.addEventListener("DOMContentLoaded", refreshInsights);

async function checkForUpdate() {
  try {
    const r = await fetch("/api/version", { cache: "no-store" });
    if (!r.ok) throw new Error("HTTP " + r.status);

    const j = await r.json();
    const cur    = String(j.current ?? "0.0.0").trim();
    const latest = (String(j.latest ?? "") || "").trim() || null;
    const url    = j.html_url || "https://github.com/cenodude/plex-simkl-watchlist-sync/releases";
    const hasUpdate = !!j.update_available;

    // Update version label
    const vEl = document.getElementById("app-version");
    if (vEl) vEl.textContent = `Version ${cur}`;

    // Badge
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
        void updEl.offsetWidth; // reflow
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

// Run once after DOM is ready
document.addEventListener("DOMContentLoaded", () => {
  checkForUpdate();
  // Optional: re-check when the tab becomes visible again
  // document.addEventListener('visibilitychange', () => {
  //   if (!document.hidden) checkForUpdate();
  // });
});

// tiny toast
function showToast(text, onClick) {
  const toast = document.createElement("div");
  toast.className = "msg ok";
  toast.textContent = text;
  toast.style.position = "fixed";
  toast.style.right = "16px";
  toast.style.bottom = "16px";
  toast.style.zIndex = 1000;
  toast.style.cursor = "pointer";
  toast.onclick = () => {
    onClick && onClick();
    toast.remove();
  };

  document.body.appendChild(toast);
  setTimeout(() => toast.classList.add("hidden"), 3000);
}

// call at boot, and on a timer
checkForUpdate(true);
setInterval(() => checkForUpdate(false), UPDATE_CHECK_INTERVAL_MS);

/* ====== Summary stream + details log ====== */
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

function animateNumber(el, to) {
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

    // meter
    const max = Math.max(1, n, w, m);
    const fill = document.getElementById("stat-fill");

    if (fill) fill.style.width = Math.round((n / max) * 100) + "%";

    // deltas
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

    bumpOne(n - w, "last week"); // or: bumpOne(n - m, 'last month')

    // optional API fields
    const by = j.by_source || {};
    const totalAdd = Number.isFinite(j.added) ? j.added : null; // all-time totals
    const totalRem = Number.isFinite(j.removed) ? j.removed : null;
    const lastAdd = Number.isFinite(j.new) ? j.new : null; // last run only
    const lastRem = Number.isFinite(j.del) ? j.del : null;

    // legend numbers (all-time)
    const setTxt = (id, val) => {
      const el = document.getElementById(id);

      if (el) el.textContent = String(val ?? 0);
    };

    setTxt("stat-added", totalAdd);
    setTxt("stat-removed", totalRem);

    // tiles (last run only, auto-hide when null)
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

    // brand totals for Plex / SIMKL (transparent tiles + subtle edge glow)
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

    // ensure tiles are visible
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

// Call once on boot
document.addEventListener("DOMContentLoaded", _initStatsTooltip);

// Call at boot
document.addEventListener("DOMContentLoaded", () => {
  refreshStats(true);
});

// Nudge the stats whenever the summary updates or a run finishes
const _origRenderSummary =
  typeof renderSummary === "function" ? renderSummary : null;

window.renderSummary = function (sum) {
  if (_origRenderSummary) _origRenderSummary(sum);

  refreshStats(false);
};

function openDetailsLog() {
  const el = document.getElementById("det-log");
  const slider = document.getElementById("det-scrub");

  if (!el) return;
  el.innerHTML = "";
  detStickBottom = true;

  if (esDet) {
    try {
      esDet.close();
    } catch (_) {}
    esDet = null;
  }

  const updateSlider = () => {
    if (!slider) return;
    const max = el.scrollHeight - el.clientHeight;
    slider.value = max <= 0 ? 100 : Math.round((el.scrollTop / max) * 100);
  };

  const updateStick = () => {
    const pad = 6; // tolerantierandje
    detStickBottom = el.scrollTop >= el.scrollHeight - el.clientHeight - pad;
  };

  el.addEventListener(
    "scroll",
    () => {
      updateSlider();
      updateStick();
    },
    { passive: true }
  );

  if (slider) {
    slider.addEventListener("input", () => {
      const max = el.scrollHeight - el.clientHeight;
      el.scrollTop = Math.round((slider.value / 100) * max);
      detStickBottom = slider.value >= 99;
    });
  }

  esDet = new EventSource("/api/logs/stream?tag=SYNC");
  esDet.onmessage = (ev) => {
    if (!ev?.data) return;
    const el = document.getElementById("det-log");
    if (!el) return;

    if (ev.data === "::CLEAR::") {
      el.textContent = "";   // wipe the sync output window
      return;
    }

  el.insertAdjacentHTML("beforeend", ev.data + "<br>");
  if (detStickBottom) el.scrollTop = el.scrollHeight;
  updateSlider();
};

  esDet.onerror = () => {
    try {
      esDet?.close();
    } catch (_) {}
    esDet = null;
  };

  requestAnimationFrame(() => {
    el.scrollTop = el.scrollHeight;
    updateSlider();
  });
}

function closeDetailsLog() {
  try {
    esDet?.close();
  } catch (_) {}
  esDet = null;
}

function toggleDetails() {
  const d = document.getElementById("details");

  d.classList.toggle("hidden");

  if (!d.classList.contains("hidden")) openDetailsLog();
  else closeDetailsLog();
}

window.addEventListener("beforeunload", closeDetailsLog);

/* ====== Summary copy / download ====== */
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

/* ====== Status refresh (Plex & SIMKL) ====== */
function setRefreshBusy(busy) {
  const btn = document.getElementById("btn-status-refresh");

  if (!btn) return;
  btn.disabled = !!busy;
  btn.classList.toggle("loading", !!busy);
}

async function manualRefreshStatus() {
  const btn = document.getElementById("btn-status-refresh");

  try {
    setRefreshBusy(true);

    btn.classList.add("spin");
    setTimeout(() => btn.classList.remove("spin"), 2000);
  } catch (e) {
    console?.warn("Manual status refresh failed", e);
  } finally {
    setRefreshBusy(false);
  }
}

async function refreshStatus(force = false) {
  const now = Date.now();

  if (!force && now - lastStatusMs < STATUS_MIN_INTERVAL) return;

  lastStatusMs = now;

  const r = await fetch("/api/status" + (force ? "?fresh=1" : "")).then((r) =>
    r.json()
  );

  appDebug = !!r.debug;

  // Auth Providers Connected or not connected
  const pb = document.getElementById("badge-plex");
  const sb = document.getElementById("badge-simkl");

  pb.className = "badge " + (r.plex_connected ? "ok" : "no");
  pb.innerHTML = `<span class="dot ${
    r.plex_connected ? "ok" : "no"
  }"></span>Plex: ${r.plex_connected ? "Connected" : "Not connected"}`;
  sb.className = "badge " + (r.simkl_connected ? "ok" : "no");
  sb.innerHTML = `<span class="dot ${
    r.simkl_connected ? "ok" : "no"
  }"></span>SIMKL: ${r.simkl_connected ? "Connected" : "Not connected"}`;

  window._ui.status = {
    can_run: !!r.can_run,
    plex_connected: !!r.plex_connected,
    simkl_connected: !!r.simkl_connected,
  };

  recomputeRunDisabled();

  const onMain = !document
    .getElementById("ops-card")
    .classList.contains("hidden");

  const logPanel = document.getElementById("log-panel");
  const layout = document.getElementById("layout");
  const stats = document.getElementById("stats-card");
  const hasStatsVisible = !!(stats && !stats.classList.contains("hidden"));
  logPanel.classList.toggle("hidden", !(appDebug && onMain));
  layout.classList.toggle("full", onMain && !appDebug && !hasStatsVisible);
}

/* ====== Config & Settings ====== */
async function loadConfig() {
  const cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());

  // cache once
  window._cfgCache = cfg;

  // ---- Sync Options
  _setVal("mode",   cfg.sync?.bidirectional?.mode || "two-way");
  _setVal("source", cfg.sync?.bidirectional?.source_of_truth || "plex");

  // ---- Troubleshoot / Runtime
  _setVal("debug", String(!!cfg.runtime?.debug));

  // ---- Auth / Keys (populate inputs FIRST)
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

  // ---- Scheduling (drive UI from same source)
  const s = cfg.scheduling || {};
  _setVal("schEnabled", String(!!s.enabled));
  _setVal("schMode",    typeof s.mode === "string" && s.mode ? s.mode : "hourly");
  _setVal("schN",       Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "2");
  _setVal("schTime",    typeof s.daily_time === "string" && s.daily_time ? s.daily_time : "03:30");
  if (document.getElementById("schTz")) _setVal("schTz", s.timezone || "");

  // ---- After inputs are set, update button/hints (read from inputs!)
  try { updateSimklButtonState(); } catch {}
  try { updateSimklHint?.();      } catch {}
  try { updateTmdbHint?.();       } catch {}
}

// Save settings back to server

// helper used below
function _getVal(id) {
  const el = document.getElementById(id);
  return (el && typeof el.value === "string" ? el.value : "").trim();
}

// saveSetting - Save Settings
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
    // 1) Pull current server config (fresh) and clone
    const serverResp = await fetch("/api/config", { cache: "no-store" });
    if (!serverResp.ok) throw new Error(`GET /api/config ${serverResp.status}`);
    const serverCfg = await serverResp.json();
    const cfg =
      typeof structuredClone === "function"
        ? structuredClone(serverCfg)
        : JSON.parse(JSON.stringify(serverCfg || {}));

    const norm = (s) => (s ?? "").trim();
    let changed = false;

    // --- SYNC ---
    const uiMode   = _getVal("mode");
    const uiSource = _getVal("source");
    const prevMode   = serverCfg?.sync?.bidirectional?.mode || "two-way";
    const prevSource = serverCfg?.sync?.bidirectional?.source_of_truth || "plex";

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

    // --- RUNTIME ---
    const uiDebug   = _getVal("debug") === "true";
    const prevDebug = !!serverCfg?.runtime?.debug;
    if (uiDebug !== prevDebug) {
      cfg.runtime = cfg.runtime || {};
      cfg.runtime.debug = uiDebug;
      changed = true;
    }

    // --- READ UI VALUES ---
    const uiPlexToken = _getVal("plex_token");
    const uiCid       = _getVal("simkl_client_id");
    const uiSec       = _getVal("simkl_client_secret");
    const uiTmdb      = _getVal("tmdb_api_key");

    // TRAKT
    const uiTraktCid  = _getVal("trakt_client_id");
    const uiTraktSec  = _getVal("trakt_client_secret");

    // --- PLEX (allow clearing) ---
    const prevPlex = norm(serverCfg?.plex?.account_token);
    const newPlex  = norm(uiPlexToken);
    if (newPlex !== prevPlex) {
      cfg.plex = cfg.plex || {};
      if (newPlex) cfg.plex.account_token = newPlex;
      else delete cfg.plex.account_token;
      changed = true;
    }

    // --- SIMKL (allow clearing) ---
    const prevCid = norm(serverCfg?.simkl?.client_id);
    const prevSec = norm(serverCfg?.simkl?.client_secret);
    const newCid  = norm(uiCid);
    const newSec  = norm(uiSec);

    if (newCid !== prevCid) {
      cfg.simkl = cfg.simkl || {};
      if (newCid) cfg.simkl.client_id = newCid;
      else delete cfg.simkl.client_id;
      changed = true;
    }
    if (newSec !== prevSec) {
      cfg.simkl = cfg.simkl || {};
      if (newSec) cfg.simkl.client_secret = newSec;
      else delete cfg.simkl.client_secret;
      changed = true;
    }

    // --- TRAKT (allow clearing) ---
    const prevTraktCid = norm(serverCfg?.trakt?.client_id);
    const prevTraktSec = norm(serverCfg?.trakt?.client_secret);
    const newTraktCid  = norm(uiTraktCid);
    const newTraktSec  = norm(uiTraktSec);

    if (newTraktCid !== prevTraktCid) {
      cfg.trakt = cfg.trakt || {};
      if (newTraktCid) cfg.trakt.client_id = newTraktCid;
      else delete cfg.trakt.client_id;
      changed = true;
    }
    if (newTraktSec !== prevTraktSec) {
      cfg.trakt = cfg.trakt || {};
      if (newTraktSec) cfg.trakt.client_secret = newTraktSec;
      else delete cfg.trakt.client_secret;
      changed = true;
    }

    // --- TMDb (allow clearing) ---
    const prevTmdb = norm(serverCfg?.tmdb?.api_key);
    const newTmdb  = norm(uiTmdb);
    if (newTmdb !== prevTmdb) {
      cfg.tmdb = cfg.tmdb || {};
      if (newTmdb) cfg.tmdb.api_key = newTmdb;
      else delete cfg.tmdb.api_key;
      changed = true;
    }

    // Save updated config (only if changed)
    if (changed) {
      const postCfg = await fetch("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(cfg),
      });
      if (!postCfg.ok) throw new Error(`POST /api/config ${postCfg.status}`);

      try { await loadConfig(); } catch {}
    }

    // 3) Save scheduling (best-effort)
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

    // 4) Refresh UI pieces (best-effort)
    try { await refreshStatus(true); } catch {}
    try { updateTmdbHint?.(); } catch {}
    try { updateSimklState?.(); } catch {}
    try { await updateWatchlistTabVisibility?.(); } catch {}
    try { await loadScheduling?.(); } catch {}
    try { updateTraktHint?.(); } catch {}

    // 5) Success toast
    showToast("Settings saved ✓", true);
  } catch (err) {
    console.error("saveSettings failed", err);
    showToast("Save failed — see console", false);
  }
}


// ---- Scheduling UI ----
async function loadScheduling() {
  try {
    const res = await fetch("/api/scheduling", { cache: "no-store" });
    const s = await res.json();
    // Debug log so we can verify what the UI received
    console.debug("[UI] /api/scheduling ->", s);
    const en = document.getElementById("schEnabled");
    const mo = document.getElementById("schMode");
    const nh = document.getElementById("schN");
    const ti = document.getElementById("schTime");

    if (!en || !mo || !nh || !ti) {
      console.warn("[UI] scheduling controls not found in DOM");
      return;
    }

    // Map JSON -> controls (strings for <select> values)
    const valEnabled = s && s.enabled === true ? "true" : "false";
    const valMode =
      s && typeof s.mode === "string" && s.mode ? s.mode : "hourly";

    const valN =
      s && Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : "2";

    const valTime =
      s && typeof s.daily_time === "string" && s.daily_time
        ? s.daily_time
        : "03:30";

    // Update controls
    en.value = valEnabled;
    mo.value = valMode;
    nh.value = valN;
    ti.value = valTime;

    // nudge browser to update UI state
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
  m.classList.remove("hidden");
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

/* Troubleshooting actions */
async function clearState() {
  const btnText = "Clear State";
  try {
    const r = await fetch("/api/troubleshoot/reset-state", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "clear_both" }) // state + tombstones
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

/* TMDb hint logic (Settings page only) */

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

/* Trakt auth (device/PIN) */
function setTraktSuccess(show) {
  const el = document.getElementById("trakt_msg");
  if (el) el.classList.toggle("hidden", !show);
}

async function requestTraktPin() {
  try { setTraktSuccess(false); } catch (_) {}

  let win = null;
  try {
    // Houd user-gesture aan voor popup blockers
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

  // UI invullen
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

/* Plex auth (PIN flow) */
function setPlexSuccess(show) {
  document.getElementById("plex_msg").classList.toggle("hidden", !show);
}
async function requestPlexPin() {
  try {
    setPlexSuccess && setPlexSuccess(false);
  } catch (_) {}
  let win = null;

  try {
    // Open first to keep user-gesture context; do NOT close automatically

    win = window.open("https://plex.tv/link", "_blank");
  } catch (_) {}
  let resp, data;

  try {
    resp = await fetch("/api/plex/pin/new", { method: "POST" });
  } catch (e) {
    console.warn("plex pin fetch failed", e);

    // Leave the plex tab open for the user; show a toast/message if available

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

    // Keep the tab open; user can still navigate manually

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
    // Optional: focus the plex tab if the browser allows it

    if (win && !win.closed) {
      win.focus();
    }
  } catch (_) {}

  try {
    // kick off polling if your app has it

    if (typeof startPlexTokenPoll === "function") startPlexTokenPoll(data);
  } catch (e) {
    console.warn("startPlexTokenPoll error", e);
  }
}

/* SIMKL auth */

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

// --- helpers (SIMKL flow)) ---
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

// Globale refs for timers
let simklPollTimer = null;
let simklCountdownTimer = null;

// --- SIMKL-connect flow (max 2 min) ---
async function startSimkl() {
  // mirror your old semantics
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

  // open consent tab
  window.open(j.authorize_url, "_blank");

  // clear any previous poll
  if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }

  const MAX_MS = 120000; // 2 minutes
  const deadline = Date.now() + MAX_MS;
  const backoff = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
  let i = 0;

  const poll = async () => {
    // hard stop on timeout
    if (Date.now() >= deadline) { simklPoll = null; return; }

    // be nice when not visible / settings closed
    const settingsVisible = !!(document.getElementById("page-settings") && !document.getElementById("page-settings").classList.contains("hidden"));
    if (document.hidden || !settingsVisible) {
      simklPoll = setTimeout(poll, 5000);
      return;
    }

    let cfg = null;
    try {
      cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json());
    } catch (_) {}

    const tok = cfg?.simkl?.access_token || "";
    if (tok) {
      try { setValIfExists && setValIfExists("simkl_access_token", tok); } catch (_) {}
      simklPoll = null;
      return;
    }

    const delay = backoff[Math.min(i, backoff.length - 1)];
    i++;
    simklPoll = setTimeout(poll, delay);
  };

  // first tick
  simklPoll = setTimeout(poll, 1000);
}

function flashBtnOK(btnEl) {
  if (!btnEl) return;
  btnEl.disabled = true;
  btnEl.classList.add("copied"); // optional style hook
  setTimeout(() => {
    btnEl.classList.remove("copied");
    btnEl.disabled = false;
  }, 700);
}

// Wire up buttons once the DOM is ready (no layout changes)
document.addEventListener("DOMContentLoaded", () => {
  // Plex
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

  // Trakt
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

/* ====== Poster carousel helpers ====== */
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

/* --- brand helpers ----------------------------------------------------------- */
function cxBrandInfo(name) {
  const key = String(name || "").toUpperCase();
  // uses your uploaded filenames in /assets
  const map = {
    PLEX: { cls: "brand-plex", icon: "/assets/PLEX.svg" },
    SIMKL: { cls: "brand-simkl", icon: "/assets/SIMKL.svg" },
  };
  return map[key] || { cls: "", icon: "" };
}

/* ====== Watchlist (grid) ====== */

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

// Delete a watchlist item (from Plex)

async function deletePoster(ev, encKey, btnEl) {
  ev?.stopPropagation?.();
  const key = decodeURIComponent(encKey);
  const card = btnEl.closest(".wl-poster");

  // visual state
  btnEl.disabled = true;
  btnEl.classList.remove("done", "error");
  btnEl.classList.add("working");

  try {
    const res = await fetch("/api/watchlist/" + encodeURIComponent(key), {
      method: "DELETE",
    });
    if (!res.ok) throw new Error("HTTP " + res.status);

    // fade out and remove
    if (card) {
      card.classList.add("wl-removing");

      setTimeout(() => {
        card.remove();
      }, 350);
    }

    // persist hidden key (your existing behavior)
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

/* ====== Watchlist preview visibility (fixed) ====== */
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
      await loadWall(); // fetch posters from /api/state/wall
      window.wallLoaded = true;
    }
    return true;
  }
}

/* ====== Boot ====== */

showTab("main");
updateWatchlistTabVisibility();

let _bootPreviewTriggered = false;

// make sure wall can load on first paint
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

// Metadata helper for posters
async function resolvePosterUrl(entity, id, size = "w342") {
  if (!id) return null;
  const typ = entity === "tv" || entity === "show" ? "tv" : "movie";
  const cb = window._lastSyncEpoch || 0;

  // Fetch metadata first (so we know poster paths exist)
  const res = await fetch(`/api/tmdb/meta/${typ}/${id}`);
  if (!res.ok) return null;

  const meta = await res.json();
  if (!meta.images || !meta.images.poster?.length) return null;

  // Just build the proxy URL, backend serves the file
  return `/art/tmdb/${typ}/${id}?size=${encodeURIComponent(size)}&cb=${cb}`;
}

// Dynamically load auth provider HTML
async function mountAuthProviders() {
  try {
    const res = await fetch("/api/auth/providers/html");
    if (!res.ok) return;
    const html = await res.text();
    const slot = document.getElementById("auth-providers");
    if (slot) {
      slot.innerHTML = html;
    }

    // Wire up copy buttons (Plex)
    document.getElementById("btn-copy-plex-pin")
      ?.addEventListener("click", (e) => copyInputValue("plex_pin", e.currentTarget));
    document.getElementById("btn-copy-plex-token")
      ?.addEventListener("click", (e) => copyInputValue("plex_token", e.currentTarget));

    // Wire up copy buttons (Trakt)
    document.getElementById("btn-copy-trakt-pin")
      ?.addEventListener("click", (e) => copyInputValue("trakt_pin", e.currentTarget));
    document.getElementById("btn-copy-trakt-token")
      ?.addEventListener("click", (e) => copyInputValue("trakt_token", e.currentTarget));

    // Hydrate + run Trakt hint updater
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
    // call, then update
    Promise.resolve(mountAuthProviders()).then(() => {
      try { updateTraktHint?.(); } catch (_) {}
    });
  } catch (_) {}
});

// Dynamically load metadata provider HTML
async function mountMetadataProviders() {
  try {
    const res = await fetch("/api/metadata/providers/html");
    if (!res.ok) return;
    const html = await res.text();
    const slot = document.getElementById("metadata-providers");
    if (slot) {
      slot.innerHTML = html;
    }

    // Post-mount: update hint visibility
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

// Ensure legacy inline onclick functions are available globally
try {
  Object.assign(window, { showTab, requestPlexPin, requestTraktPin });
} catch (e) {
  console.warn("Global export failed", e);
}

// Safe no-op for missing hint updaters
if (typeof updateTraktHint !== "function") {
  function updateTraktHint() {
    try {
      const cid  = document.getElementById("trakt_client_id")?.value?.trim();
      const secr = document.getElementById("trakt_client_secret")?.value?.trim();
      const hint = document.getElementById("trakt_hint");
      if (!hint) return;
      // Show warning when either field is missing
      hint.classList.toggle("hidden", !!(cid && secr));
    } catch (_) {}
  }
}

// ---------- Trakt helpers (UI) ----------
function updateTraktHint() {
  try {
    const cid = (document.getElementById("trakt_client_id")?.value || "").trim();
    const sec = (document.getElementById("trakt_client_secret")?.value || "").trim();
    const hint = document.getElementById("trakt_hint");
    if (!hint) return;
    const missing = !cid || !sec;
    hint.classList.toggle("hidden", !missing);
  } catch (_) {}
}

function copyTraktRedirect() {
  try {
    const uri = "urn:ietf:wg:oauth:2.0:oob"; // fixed redirect URI
    navigator.clipboard.writeText(uri);
    const codeEl = document.getElementById("trakt_redirect_uri_preview");
    if (codeEl) codeEl.textContent = uri;
    notify?.("Redirect URI copied ✓");
  } catch (e) {
    console.warn("copyTraktRedirect failed", e);
  }
}

// --  Trakt import config + toggle hint
async function hydrateAuthFromConfig() {
  try {
    const r = await fetch("/api/config", { cache: "no-store" });
    if (!r.ok) return;
    const cfg = await r.json();

    // Prefill TRKT
    setValIfExists("trakt_client_id",     (cfg.trakt?.client_id || "").trim());
    setValIfExists("trakt_client_secret", (cfg.trakt?.client_secret || "").trim());
    setValIfExists(
      "trakt_token",
      (cfg.auth?.trakt?.access_token) || (cfg.trakt?.access_token) || ""
    );

    // Hint meteen correct
    updateTraktHint();
  } catch (_) {}
}

// Wire up extra buttons after provider HTML mount
async function mountAuthProviders() {
  try {
    const res = await fetch("/api/auth/providers/html");
    if (!res.ok) return;
    const html = await res.text();
    const slot = document.getElementById("auth-providers");
    if (slot) slot.innerHTML = html;

    // existing Plex bindings…
    document.getElementById("btn-copy-plex-pin")
      ?.addEventListener("click", (e) => copyInputValue("plex_pin", e.currentTarget));
    document.getElementById("btn-copy-plex-token")
      ?.addEventListener("click", (e) => copyInputValue("plex_token", e.currentTarget));

    // TRAKT bindings
    document.getElementById("btn-copy-trakt-pin")
      ?.addEventListener("click", (e) => copyInputValue("trakt_pin", e.currentTarget));
    document.getElementById("btn-copy-trakt-token")
      ?.addEventListener("click", (e) => copyInputValue("trakt_token", e.currentTarget));
    document.getElementById("trakt_client_id")
      ?.addEventListener("input", updateTraktHint);
    document.getElementById("trakt_client_secret")
      ?.addEventListener("input", updateTraktHint);

    // Prefill vanuit config + meerdere veilige toggles i.v.m. render timing
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

// Exporteer legacy globals (voeg toe aan je bestaande export als nodig)
try {
  Object.assign(window, { requestTraktPin });
} catch (_) {}


// ---------- Request TRAKT PIN (device code) ----------
async function requestTraktPin() {
  // open tab first to keep user gesture
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

// Safe fallback for saving settings if not injected
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

// Safe no-op for missing hint updaters

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
    // class toggle
    hint.classList.toggle("hidden", !show);
    // style fallback (voor het geval .hidden css nog niet geladen is)
    hint.style.display = show ? "" : "none";
  } catch (_) {}
}

// Plex token poll
function startPlexTokenPoll() {
  try { if (plexPoll) clearTimeout(plexPoll); } catch (_) {}
  const MAX_MS = 120000; // 2 minutes
  const deadline = Date.now() + MAX_MS;
  const backoff = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
  let i = 0;

  const poll = async () => {
    if (Date.now() >= deadline) { plexPoll = null; return; }

    // be nice when hidden
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

// ---- Expose functions globally for inline onclick handlers ----
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

// Auto-boot lists after DOM is ready (and again when opening Settings)
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
      if (typeof renderConnections === "function") renderConnections();
    } catch (e) {
      console.warn("renderConnections failed", e);
    }
  } catch (e) {
    div.innerHTML = '<div class="muted">Failed to load providers.</div>';
    console.warn("loadProviders error", e);
  }
}

// ===== Global exports for inline onclicks =====
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

// ====== NEW: Sync Pair Manager and Logging ======

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

/* === Connections Builder: Global state ===
   Stores providers, pairs, and connect flow state. */
window.cx = window.cx || {
  providers: [],
  pairs: [],
  connect: { source: null, target: null },
};

/* === Connections Builder UI === */
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
  cxEnsureStyles();
  hideLegacyPairsUI();
  const host = document.getElementById("providers_list");
  if (!host) return;
  const provs = window.cx.providers || [];
  const pairs = window.cx.pairs || [];

  // Build provider cards with brand icon + glow + "Set as Source"
  const cards = provs
    .map((p) => {
      const name = p.label || p.name;
      const wl = _cap(p, "watchlist");
      const rat = _cap(p, "ratings");
      const hist = _cap(p, "history");
      const pl = _cap(p, "playlists");

      const brand = cxBrandInfo(p.name);
      const iconHtml = brand.icon
        ? `<img class="prov-brand" src="${brand.icon}" alt="${name}" />`
        : "";

      return `
      <div class="prov-card ${brand.cls}" data-prov="${p.name}">
        <div class="prov-head">
          <div class="prov-title">${name}</div>
          ${iconHtml}
        </div>

        <div class="prov-caps">
          <span class="dot ${wl ? "on" : "off"}" title="Watchlist"></span>
          <span class="dot ${rat ? "on" : "off"}" title="Ratings"></span>
          <span class="dot ${hist ? "on" : "off"}" title="History"></span>
          <span class="dot ${pl ? "on" : "off"}" title="Playlists"></span>
        </div>

        <button class="btn neon" onclick="cxToggleConnect('${
          p.name
        }')">Set as Source</button>
      </div>`;
    })
    .join("");

  // Build pairs board (with fast id lookup for editor)
  window._pairsById = Object.create(null);
  (pairs || []).forEach((p) => {
    window._pairsById[String(p.id)] = p;
  });

  const pairCards = (pairs || [])
    .map((pr) => {
      const f = _pairFeatureObj(pr);
      const wl = f.watchlist.add ? "Add" : "—";
      const enabled = pr.enabled === true; // default = OFF unless explicitly true
      const mode = (pr.mode || "one-way").toUpperCase();
      return `<div class="pair-card" draggable="true" data-id="${pr.id}">
        <div class="line"><span class="pill src">${
          pr.source
        }</span><span class="arrow">→</span><span class="pill dst">${
        pr.target
      }</span><span class="mode">${mode}</span></div>
        <div class="line small"><span class="feat">Watchlist: <strong>${wl}</strong></span></div>
        <div class="actions">
          <label class="switch"><input type="checkbox" ${
            enabled ? "checked" : ""
          } onchange="cxToggleEnable('${
        pr.id
      }', this.checked)"><span></span></label>
          <button class="btn" onclick="cxEditPair('${pr.id}')">Edit</button>
          <button class="btn danger" onclick="deletePair('${
            pr.id
          }')">Delete</button>
          <button class="btn ghost" title="Move first" onclick="movePair('${
            pr.id
          }','first')">⏮</button>
          <button class="btn ghost" title="Move last"  onclick="movePair('${
            pr.id
          }','last')">⏭</button>
        </div>
      </div>`;
    })
    .join("");

  const board = `<div class="cx-grid">${cards}</div>
    <div class="sep"></div>
    <div class="sub">Configured Connections</div>
    <div class="pairs-board">${
      pairCards || '<div class="muted">No connections configured yet.</div>'
    }</div>`;

  host.innerHTML = board;

  // kill old dock style if it was injected earlier
  document.getElementById("cx-dock-style")?.remove();

  /* ==== Horizontal layout + drag-to-reorder (no dock) ==== */
  (() => {
    // Compact CSS override (no extra UI)
    let s = document.getElementById("cx-align-style");
    if (!s) {
      s = document.createElement("style");
      s.id = "cx-align-style";
      document.head.appendChild(s);
    }
    s.textContent = `
      .pairs-board{display:flex;flex-direction:row;align-items:stretch;gap:12px;overflow-x:auto;padding:8px 2px}
      .pairs-board .pair-card{flex:0 0 280px;width:280px;min-width:280px;min-height:unset;padding:10px 12px;margin:0;cursor:grab}
      .pair-card.dragging{opacity:.6}
      .btn.ghost{background:transparent;border:1px solid rgba(255,255,255,.15);color:#cfd6ff;padding:6px 10px;border-radius:10px}
    `;

    const row = host.querySelector(".pairs-board");
    if (!row) return;

    let dragging = null;

    row.addEventListener("dragstart", (e) => {
      const card = e.target.closest(".pair-card");
      if (!card) return;
      dragging = card;
      card.classList.add("dragging");
      if (e.dataTransfer) e.dataTransfer.effectAllowed = "move";
    });

    row.addEventListener("dragend", () => {
      if (dragging) dragging.classList.remove("dragging");
      dragging = null;
    });

    row.addEventListener("dragover", (e) => {
      if (!dragging) return;
      e.preventDefault();
      const after = getAfter(row, e.clientX);
      if (!after) row.appendChild(dragging);
      else row.insertBefore(dragging, after);
    });

    row.addEventListener("drop", () => commitOrder(row));

    function getAfter(container, x) {
      const els = [...container.querySelectorAll(".pair-card:not(.dragging)")];
      let closest = null,
        closestOffset = Number.NEGATIVE_INFINITY;
      for (const el of els) {
        const r = el.getBoundingClientRect();
        const offset = x - (r.left + r.width / 2);
        if (offset < 0 && offset > closestOffset) {
          closestOffset = offset;
          closest = el;
        }
      }
      return closest;
    }

    function rebuildIndex() {
      window._pairsById = Object.create(null);
      (pairs || []).forEach((p) => {
        window._pairsById[String(p.id)] = p;
      });
    }

    function commitOrder(container) {
      const ids = [...container.querySelectorAll(".pair-card")].map((el) =>
        String(el.dataset.id)
      );
      pairs.sort(
        (a, b) => ids.indexOf(String(a.id)) - ids.indexOf(String(b.id))
      );
      rebuildIndex();
      try {
        typeof savePairs === "function" && savePairs(pairs);
      } catch (_) {}
      try {
        typeof renderConnections === "function" && renderConnections();
      } catch (_) {}
    }

    // click helpers remain
    window.movePair = function (id, where) {
      const i = pairs.findIndex((p) => String(p.id) === String(id));
      if (i < 0) return;
      const [item] = pairs.splice(i, 1);
      if (where === "first") pairs.unshift(item);
      else if (where === "last") pairs.push(item);
      rebuildIndex();
      try {
        typeof savePairs === "function" && savePairs(pairs);
      } catch (_) {}
      try {
        typeof renderConnections === "function" && renderConnections();
      } catch (_) {}
    };
  })();

  // Ensure modal is in DOM
  function cxCloseModal() {
    var m = document.getElementById("cx-modal");
    if (m) m.classList.add("hidden");
  }

  function cxUpdateSummary() {
    var src = document.getElementById("cx-src").value;
    var dst = document.getElementById("cx-dst").value;
    var two = document.getElementById("cx-mode-two").checked;
    var wlOn = document.getElementById("cx-wl-enable").checked;
    var wlAdd = document.getElementById("cx-wl-add").checked;
    var wlRem = document.getElementById("cx-wl-remove").checked;

    document.getElementById("sum-src").textContent = src;
    document.getElementById("sum-dst").textContent = dst;
    document.getElementById("sum-src-chip").textContent = src.slice(0, 1);
    document.getElementById("sum-dst-chip").textContent = dst.slice(0, 1);
    document.getElementById("sum-dir").textContent = two ? "↔" : "→";

    var wlText = "Off";
    if (wlOn) {
      wlText =
        wlAdd && wlRem
          ? "Add & Remove"
          : wlAdd
          ? "Add"
          : wlRem
          ? "Remove"
          : "On";
    }
    document.getElementById("sum-wl").textContent = wlText;
  }

  (function bindCfgEvents() {
    var ids = [
      "cx-src",
      "cx-dst",
      "cx-mode-one",
      "cx-mode-two",
      "cx-wl-enable",
      "cx-wl-add",
      "cx-wl-remove",
      "cx-enabled",
    ];
    ids.forEach(function (id) {
      var el = document.getElementById(id);
      if (el) {
        el.addEventListener("change", function () {
          // enable/disable Remove: alleen bij two-way en wl enabled
          var two = document.getElementById("cx-mode-two").checked;
          var wlOn = document.getElementById("cx-wl-enable").checked;
          var rem = document.getElementById("cx-wl-remove");
          rem.disabled = !(two && wlOn);
          if (rem.disabled) rem.checked = false;
          cxUpdateSummary();
        });
      }
    });
    var save = document.getElementById("cx-save");
    if (save) {
      save.addEventListener("click", function () {
        var data = {
          source: document.getElementById("cx-src").value,
          target: document.getElementById("cx-dst").value,
          enabled: document.getElementById("cx-enabled").checked,
          mode: document.getElementById("cx-mode-two").checked
            ? "two-way"
            : "one-way",
          features: {
            watchlist: {
              enable: document.getElementById("cx-wl-enable").checked,
              add: document.getElementById("cx-wl-add").checked,
              remove: document.getElementById("cx-wl-remove").checked,
            },
          },
        };
        if (typeof window.cxSavePair === "function") {
          window.cxSavePair(data);
        } else {
          console.log("cxSavePair payload", data);
        }
        cxCloseModal();
      });
    }
  })();

  /* === Connect flow handlers === */
  window.cxStartConnect = function (name) {
    window.cx.connect = { source: String(name), target: null };
    logToSyncOutput(`[ui] Select a target for ${name}`);
    // Visually hint could be added here
  };

  window.cxPickTarget = function (name) {
    if (!window.cx.connect || !window.cx.connect.source) return;
    window.cx.connect.target = String(name);
    cxOpenModalFor({
      source: window.cx.connect.source,
      target: window.cx.connect.target,
      mode: "one-way",
      enabled: true,
      features: { watchlist: { add: true, remove: false } },
    });
  };

  /* Ensure provider cards are clickable for target when in connect mode */
  document.addEventListener("click", (e) => {
    const el = e.target.closest && e.target.closest(".prov-card");
    if (!el) return;
    if (window.cx && window.cx.connect && window.cx.connect.source) {
      const name = el.getAttribute("data-prov");
      if (name && name !== window.cx.connect.source) {
        e.preventDefault();
        window.cxPickTarget(name);
      }
    }
  });

  function cxEditPair(id) {
    const pr = (window.cx.pairs || []).find((p) => p.id === id);
    if (!pr) return;
    cxOpenModalFor(pr, id);
  }

  function cxCloseModal() {
    const modal = document.getElementById("cx-modal");
    if (modal) modal.classList.add("hidden");
    window.cx.connect = { source: null, target: null };
  }

  // Compact enable/disable toggle with strict error handling
  async function cxToggleEnable(id, on) {
    try {
      const r = await fetch(`/api/pairs/${encodeURIComponent(id)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: !!on }),
      });
      if (!r.ok) {
        console.warn("toggle enable failed:", r.status, r.statusText);
        alert("Failed to update. Please try again.");
      }
    } catch (e) {
      console.warn("toggle enable failed", e);
      alert("Network error. Please try again.");
    } finally {
      await loadPairs();
    }
  }


  /* Toggle connect: if no source selected -> set source, else pick as target (if different) */
  window.cxToggleConnect = function (name) {
    name = String(name || "");
    window.cx = window.cx || {};
    window.cx.connect = window.cx.connect || { source: null, target: null };
    const sel = window.cx.connect;

    if (!sel.source) {
      if (typeof cxStartConnect === "function") cxStartConnect(name);
      else window.cx.connect = { source: name, target: null };
      try {
        logToSyncOutput(`[ui] Source selected: ${name}. Now click a target.`);
      } catch (_) {}
      try {
        window.renderConnections && window.renderConnections();
      } catch (_) {}
      return;
    }

    if (sel.source && sel.source !== name) {
      if (typeof cxPickTarget === "function") cxPickTarget(name);
      else window.cx.connect.target = name;
      return;
    }

    window.cx.connect = { source: null, target: null };
    try {
      logToSyncOutput(`[ui] Selection cleared.`);
    } catch (_) {}
    try {
      window.renderConnections && window.renderConnections();
    } catch (_) {}
  };

  /* Ensure minimal neon styles for provider/pair cards without touching existing theme */
  function cxEnsureStyles() {
    if (document.getElementById("cx-style")) return;
    const css = `
  .cx-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px;margin-top:6px}
  .prov-card{position:relative;padding:14px;border-radius:14px;background:rgba(255,255,255,0.03);border:1px solid rgba(125,125,255,0.2);box-shadow:0 0 0 1px rgba(125,125,255,0.05) inset, 0 12px 30px rgba(0,0,0,0.2)}
  .prov-card:hover{box-shadow:0 0 0 1px rgba(125,125,255,0.3) inset, 0 18px 40px rgba(0,0,0,0.35)}
  .prov-title{font-weight:600;margin-bottom:8px}
  .prov-caps{display:flex;gap:6px;margin-bottom:10px}
  .prov-caps .dot{width:8px;height:8px;border-radius:50%;display:inline-block;opacity:.55}
  .prov-caps .dot.on{opacity:1}
  .prov-caps .dot.off{opacity:.25;filter:saturate(.2)}
  .btn.neon{background:linear-gradient(90deg,#2de2ff,#7c5cff,#ff7ae0);-webkit-background-clip:text;background-clip:text;color:transparent;border:1px solid rgba(124,92,255,.5)}
  .btn.neon:hover{filter:brightness(1.1)}
  .pair-card{padding:12px;border-radius:12px;background:rgba(255,255,255,0.03);border:1px dashed rgba(255,255,255,.12);margin-top:8px}
  .pair-card .line{display:flex;align-items:center;gap:8px}
  .pair-card .line.small{opacity:.8;font-size:.9em;margin-top:4px}
  .pair-card .pill{padding:3px 8px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12)}
  .pair-card .arrow{opacity:.7}
  .pair-card .mode{margin-left:auto;font-size:.75em;opacity:.8}
  .pairs-board{margin-top:8px}
  /* Legacy pairs UI hidden when connections builder is active */
  #pairs_list, .pair-selectors { display:none !important; }
  #sec-sync .sub { } .prov-card.selected{outline:1px solid rgba(124,92,255,.6); box-shadow:0 0 22px rgba(124,92,255,.25)}
  `;
    const style = document.createElement("style");
    style.id = "cx-style";
    style.textContent = css;
    document.head.appendChild(style);
  }

  function hideLegacyPairsUI() {
    // Hide the "Pairs" header and its explanatory text
    const subs = Array.from(document.querySelectorAll("#sec-sync .sub"));
    subs.forEach((el) => {
      if (el.textContent && el.textContent.trim().toLowerCase() === "pairs") {
        el.style.display = "none";
        // hide the next sibling muted text if present
        let sib = el.nextElementSibling;
        if (sib && sib.classList.contains("muted")) sib.style.display = "none";
      }
    });
    // Hide legacy containers (also covered by CSS)
    const legacy = document.querySelectorAll("#pairs_list, .pair-selectors");
    legacy.forEach((el) => el && (el.style.display = "none"));
  }

  /* Drag & Drop: drag a source card onto a target card to create a connection */
  document.addEventListener("dragstart", (e) => {
    const card = e.target.closest && e.target.closest(".prov-card");
    if (!card) return;
    const name = card.getAttribute("data-prov");
    if (!name) return;
    // set this card as source
    window.cx.connect = { source: name, target: null };
    try {
      e.dataTransfer.setData("text/plain", name);
    } catch (_) {}
  });

  document.addEventListener("dragover", (e) => {
    const card = e.target.closest && e.target.closest(".prov-card");
    if (card) e.preventDefault();
  });

  document.addEventListener("drop", (e) => {
    const card = e.target.closest && e.target.closest(".prov-card");
    if (!card) return;
    e.preventDefault();
    const target = card.getAttribute("data-prov");
    const src =
      (window.cx.connect && window.cx.connect.source) ||
      (e.dataTransfer && e.dataTransfer.getData("text/plain"));
    if (src && target && src !== target) {
      cxPickTarget(target);
    }
  });

  // ===== Fallbacks to avoid ReferenceErrors (in case legacy lists are absent) =====
  if (typeof window.loadPairs !== "function") {
    async function loadPairs() {
      try {
        const arr = await fetch("/api/pairs", { cache: "no-store" }).then((r) =>
          r.json()
        );
        window.cx = window.cx || {};
        window.cx.pairs = Array.isArray(arr) ? arr : [];
        try {
          if (typeof renderConnections === "function") renderConnections();
        } catch (_) {}
      } catch (e) {
        console.warn("[fallback] loadPairs failed", e);
      }
    }
    try {
      window.loadPairs = loadPairs;
    } catch (_) {}
  }

  if (typeof window.deletePair !== "function") {
    async function deletePair(id) {
      if (!id) {
        console.warn("deletePair: missing id");
        return;
      }
      try {
        await fetch(`/api/pairs/${id}`, { method: "DELETE" });
        try {
          await loadPairs();
        } catch (_) {}
      } catch (e) {
        console.warn("[fallback] deletePair failed", e);
      }
    }
    try {
      window.deletePair = deletePair;
    } catch (_) {}
  }

  function _hideLegacyPairsUIHard() {
    try {
      // Pairs header + selectors
      const ids = ["pairs_list", "pair-table", "pair-table-body"];
      ids.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.style.display = "none";
      });
      // Any button(s) that look like "Add pair" or "Add batch"
      Array.from(document.querySelectorAll("button, .btn")).forEach((b) => {
        const t = (b.textContent || "").trim().toLowerCase();
        if (
          t === "add pair" ||
          t === "+ add pair" ||
          t === "add batch" ||
          t === "+ add batch"
        ) {
          b.style.display = "none";
        }
      });
      // Batch section containers — try several likely IDs/classes
      const guess = ["batches_list", "batches", "sec-batches", "batch-list"];
      guess.forEach((sel) => {
        const el =
          document.getElementById(sel) ||
          document.querySelector("#" + sel) ||
          document.querySelector("." + sel);
        if (el) el.style.display = "none";
      });
    } catch (_) {}
  }
  document.addEventListener("DOMContentLoaded", _hideLegacyPairsUIHard);

  /* horizontale layout voor pairs-board */
  (() => {
    const css = `
    .pairs-board{
      display:flex !important;
      flex-direction:row !important;
      align-items:stretch !important;
      gap:12px !important;
      overflow-x:auto !important;
      padding:8px 2px !important;
    }
    .pairs-board .pair-card{
      flex:0 0 280px !important;   /* vaste kaartbreedte */
      width:280px !important;
      min-width:280px !important;
      height:auto !important;
      min-height:unset !important; /* voorkomt die huge hoogte */
      padding:10px 12px !important;
      margin:0 !important;
      cursor:grab;
    }
    .pairs-board .actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    .btn.ghost{background:transparent;border:1px solid rgba(255,255,255,.15);color:#cfd6ff;padding:6px 10px;border-radius:10px}
    `;
    let s = document.getElementById("cx-align-style");
    if (!s) {
      s = document.createElement("style");
      s.id = "cx-align-style";
      document.head.appendChild(s);
    }
    s.textContent = css;
  })();

  // --- ensure cxEditPair exists (global, works with module too)
  (function ensureCxEditPair() {
    if (typeof window.cxEditPair === "function") return;

    function editPairById(id) {
      id = String(id);
      // try fast index → state → DOM data
      const pairs =
        window.cx && Array.isArray(window.cx.pairs) ? window.cx.pairs : [];
      const byId =
        (window._pairsById && window._pairsById[id]) ||
        pairs.find((p) => String(p.id) === id);

      const pair =
        byId ||
        (function () {
          // fallback: try to read from the card dataset if present
          const el = document.querySelector(`.pair-card[data-id="${id}"]`);
          if (!el) return null;
          return {
            id,
            source: el.dataset.source,
            target: el.dataset.target,
            mode: el.dataset.mode || "one-way",
          };
        })();

      if (!pair) {
        console.warn("[cxEditPair] pair not found:", id);
        return;
      }

      if (typeof openPairModal === "function") openPairModal(pair);
      else if (typeof cxOpenModalFor === "function") cxOpenModalFor(pair);
      else alert("Pair editor not wired.");
    }

    // expose
    window.cxEditPair = editPairById;
  })();

  function _hideLegacyPairsUIHard() {
    try {
      // Hide legacy Pairs containers if present
      const ids = ["pairs_list", "pair-table", "pair-table-body"];
      ids.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.style.display = "none";
      });
      // Hide buttons with these labels
      Array.from(document.querySelectorAll("button, .btn")).forEach((b) => {
        const t = (b.textContent || "").trim().toLowerCase();
        if (
          t === "add pair" ||
          t === "+ add pair" ||
          t === "add batch" ||
          t === "+ add batch"
        ) {
          b.style.display = "none";
        }
      });
      // Hide batch sections by common IDs/classes
      const guess = ["batches_list", "batches", "sec-batches", "batch-list"];
      guess.forEach((sel) => {
        const el =
          document.getElementById(sel) ||
          document.querySelector("#" + sel) ||
          document.querySelector("." + sel);
        if (el) el.style.display = "none";
      });
    } catch (_) {}
  }
  document.addEventListener("DOMContentLoaded", _hideLegacyPairsUIHard);
}

/* === SAFETY OVERRIDE: ensure modal exists before populating === */
(function () {
  function _ensureCfgModal() {
    if (document.getElementById("cx-modal")) return true;
    if (typeof cxEnsureCfgModal === "function") {
      cxEnsureCfgModal();
      return !!document.getElementById("cx-modal");
    }
    // Minimal fallback (shouldn't be needed if cxEnsureCfgModal exists)
    var wrap = document.createElement("div");
    wrap.id = "cx-modal";
    wrap.className = "modal-backdrop hidden";
    wrap.innerHTML =
      '<div class="modal-card"><div class="modal-header"><div class="title">Configure</div><button class="btn-ghost" onclick="cxCloseModal()">✕</button></div>' +
      '<div class="modal-body"><div class="form-grid">' +
      '<div class="field"><label>Source</label><select id="cx-src"><option>PLEX</option><option>SIMKL</option></select></div>' +
      '<div class="field"><label>Target</label><select id="cx-dst"><option>PLEX</option><option>SIMKL</option></select></div>' +
      '</div><div class="form-grid" style="margin-top:8px">' +
      '<div class="field"><label>Mode</label><div class="seg">' +
      '<input id="cx-mode-one" type="radio" name="cx-mode" value="one-way" checked><label for="cx-mode-one">One-way</label>' +
      '<input id="cx-mode-two" type="radio" name="cx-mode" value="two-way"><label id="cx-two-label" for="cx-mode-two">Two-way</label>' +
      "</div></div>" +
      '<div class="field"><label>Enabled</label><div class="row"><input id="cx-enabled" type="checkbox" checked></div></div>' +
      "</div>" +
      '<div class="features"><div class="fe-row">' +
      '<div class="fe-name">Watchlist</div>' +
      '<label class="row"><input id="cx-wl-add" type="checkbox" checked><span>Add</span></label>' +
      '<label class="row"><input id="cx-wl-remove" type="checkbox" disabled><span>Remove</span></label>' +
      '<div id="cx-wl-note" class="micro-note"></div>' +
      "</div></div></div>" +
      '<div class="modal-footer"><button class="btn acc" id="cx-save">Save</button><button class="btn" onclick="cxCloseModal()">Cancel</button></div></div>';
    document.body.appendChild(wrap);
    return true;
  }

  // Last-write-wins override to guard all callers
  window.cxOpenModalFor = function (pair, editingId) {
    try {
      _ensureCfgModal();
    } catch (_) {}
    var src = document.getElementById("cx-src");
    var dst = document.getElementById("cx-dst");
    var twoInput = document.querySelector(
      'input[name="cx-mode"][value="two-way"]'
    );
    var oneInput = document.querySelector(
      'input[name="cx-mode"][value="one-way"]'
    );
    var enabled = document.getElementById("cx-enabled");

    if (!src || !dst || !twoInput || !oneInput || !enabled) {
      console.warn("cxOpenModalFor: modal inputs missing after ensure()");
      return;
    }

    // Capability checks (safe if helpers exist)
    var twoLabel =
      document.getElementById("cx-two-label") ||
      (twoInput && twoInput.closest && twoInput.closest("label"));
    try {
      var _src =
        typeof _byName === "function"
          ? _byName(window.cx.providers, pair.source)
          : null;
      var _dst =
        typeof _byName === "function"
          ? _byName(window.cx.providers, pair.target)
          : null;
      var twoWayOk = !!(
        _src &&
        _dst &&
        _src.capabilities &&
        _dst.capabilities &&
        _src.capabilities.bidirectional &&
        _dst.capabilities.bidirectional
      );
      if (twoInput) twoInput.disabled = !twoWayOk;
      if (twoLabel) twoLabel.classList.toggle("muted", !twoWayOk);
    } catch (_) {}

    src.value = pair.source;
    dst.value = pair.target;
    oneInput.checked = (pair.mode || "one-way") !== "two-way";
    twoInput.checked = (pair.mode || "one-way") === "two-way";
    enabled.checked = pair.enabled !== false;

    var wlAdd = document.getElementById("cx-wl-add");
    var wlRem = document.getElementById("cx-wl-remove");
    var wlNote = document.getElementById("cx-wl-note");
    try {
      var wf = (pair.features && pair.features.watchlist) || {
        add: true,
        remove: false,
      };
      // If capability helpers exist, respect them
      var srcObj =
        typeof _byName === "function"
          ? _byName(window.cx.providers, pair.source)
          : null;
      var dstObj =
        typeof _byName === "function"
          ? _byName(window.cx.providers, pair.target)
          : null;
      var wlOk =
        typeof _cap === "function"
          ? _cap(srcObj, "watchlist") && _cap(dstObj, "watchlist")
          : true;
      wlAdd.checked = wlOk && !!wf.add;
      wlAdd.disabled = !wlOk;
      wlRem.checked = false;
      wlRem.disabled = true;
      if (wlNote)
        wlNote.textContent = wlOk
          ? ""
          : "Watchlist is not supported on one of the providers.";
    } catch (_) {}

    var modal = document.getElementById("cx-modal");
    if (modal) modal.classList.remove("hidden");
  };
})();

(function () {
  // Ensure global namespace
  window.cx = window.cx || {
    providers: [],
    pairs: [],
    connect: { source: null, target: null },
  };

  // Helper: get modal and stash/read editing id
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

  // Expose: loadPairs -> refresh list from server and re-render
  async function loadPairs() {
    try {
      const res = await fetch("/api/pairs", { cache: "no-store" });
      const arr = await res.json().catch(() => []);
      window.cx.pairs = Array.isArray(arr) ? arr : [];
      if (typeof window.renderConnections === "function") {
        try {
          window.renderConnections();
        } catch (_) {}
      }
    } catch (e) {
      console.warn("[cx] loadPairs failed", e);
    }
  }
  try {
    window.loadPairs = loadPairs;
  } catch (_) {}

  // Expose: deletePair(id) -> DELETE then refresh
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

  // Expose: cxSavePair(data) -> POST/PUT and refresh
  async function cxSavePair(data) {
    try {
      const modal = _getModal();
      const editingId =
        modal && modal.dataset ? (modal.dataset.editingId || "").trim() : "";

      // Gentle client-side dupe guard (for creates only)
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

      // Normalize payload
      const wl = (data && data.features && data.features.watchlist) || {};
      const payload = {
        source: data.source,
        target: data.target,
        mode: data.mode || "one-way",
        enabled: !!data.enabled,
        features: { watchlist: { add: !!wl.add, remove: !!wl.remove } },
      };

      let ok = false,
        r;
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

      // Clear editing id & close
      if (modal && modal.dataset) modal.dataset.editingId = "";
      try {
        window.cx.connect = { source: null, target: null };
      } catch (_) {}
      try {
        if (typeof window.cxCloseModal === "function") window.cxCloseModal();
      } catch (_) {}
      const close = document.getElementById("cx-modal");
      if (close) close.classList.add("hidden");

      await loadPairs();
    } catch (e) {
      console.warn("[cx] cxSavePair error", e);
      alert("Failed to save connection.");
    }
  }
  try {
    window.cxSavePair = cxSavePair;
  } catch (_) {}

  // Override: cxOpenModalFor -> sets dataset.editingId and pre-fills fields
  const _olderOpen = window.cxOpenModalFor;
  window.cxOpenModalFor = function (pair, editingId) {
    // Try existing behavior first to keep UI in sync
    if (typeof _olderOpen === "function") {
      try {
        _olderOpen(pair, editingId);
      } catch (_) {}
    } else {
      // Minimal fallback prefill (in case earlier versions differ)
      const m = _getModal();
      if (!m) return;
      try {
        var src = document.getElementById("cx-src");
        var dst = document.getElementById("cx-dst");
        var one = document.querySelector(
          'input[name="cx-mode"][value="one-way"]'
        );
        var two = document.querySelector(
          'input[name="cx-mode"][value="two-way"]'
        );
        var en = document.getElementById("cx-enabled");
        if (src) src.value = (pair && pair.source) || "PLEX";
        if (dst) dst.value = (pair && pair.target) || "SIMKL";
        if (en) en.checked = !(pair && pair.enabled === false);
        if (one && two) {
          const mval = (pair && pair.mode) || "one-way";
          two.checked = mval === "two-way";
          one.checked = !two.checked;
        }
        const f = (pair && pair.features && pair.features.watchlist) || {
          add: true,
          remove: false,
        };
        const wlAdd = document.getElementById("cx-wl-add");
        const wlRem = document.getElementById("cx-wl-remove");
        if (wlAdd) wlAdd.checked = !!f.add;
        if (wlRem) wlRem.checked = !!f.remove;
        m.classList.remove("hidden");
      } catch (_) {}
    }

    // Store editing id on the modal so the Save handler can detect PUT vs POST
    const modal = _getModal();
    if (modal && modal.dataset)
      modal.dataset.editingId = editingId ? String(editingId) : "";

    // Ensure Save button wires to our save function (existing listeners will still call us)
    const saveBtn = document.getElementById("cx-save");
    if (saveBtn && !saveBtn.dataset.cxBound) {
      saveBtn.dataset.cxBound = "1";
      saveBtn.addEventListener(
        "click",
        function () {
          // Read from DOM fresh to avoid stale 'data' when using our fallback path
          const data = {
            source: (document.getElementById("cx-src") || {}).value,
            target: (document.getElementById("cx-dst") || {}).value,
            enabled: !!(document.getElementById("cx-enabled") || {}).checked,
            mode:
              (document.querySelector('input[name="cx-mode"]:checked') || {})
                .value || "one-way",
            features: {
              watchlist: {
                add: !!(document.getElementById("cx-wl-add") || {}).checked,
                remove: !!(document.getElementById("cx-wl-remove") || {})
                  .checked,
              },
            },
          };
          // Intentionally do nothing here; existing listeners already call window.cxSavePair(data)
          // and our global function above will execute.
        },
        { capture: false }
      );
    }
  };

  // Boot: make sure pairs are loaded once DOM is ready
  document.addEventListener("DOMContentLoaded", () => {
    try {
      loadPairs();
    } catch (_) {}
  });
})();

/* === Modal tweaks: labels, lock src/dst, remove 'activate', arrow, WL rules === */
(function modalTweaks() {
  const $ = (s) => document.querySelector(s);

  // 1) Source/Target vastzetten zodra beide gekozen zijn
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

  // 2+3) UI-labels: Two-way -> Bidirectional, One-way -> Mirror (waarden blijven gelijk)
  // Probeer zowel label[for=] als tekstknoppen te raken
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

  // 4) "Activate this connection" verbergen; default aan
  const en = $("#cx-enabled");
  if (en) {
    en.checked = true;
    (en.closest(".group,.row,fieldset,div") || en).style.display = "none";
  }

  // 6) Watchlist Remove mag óók in Mirror; alleen blokkeren als WL disabled
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

  // 5) Richtingspijl in de samenvatting (groot + animatie; ⇄ voor bidi)
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

/**
 * Minimal UI helper: given the API payload from /api/sync/providers,
 * populate a <select id="sync-mode"> with available modes for current pair.
 * Expects two selects with ids #src-provider and #dst-provider.
 */
async function populateSyncModes() {
  const res = await fetch("/api/sync/providers");
  const data = await res.json();
  const src = document.getElementById("src-provider")?.value?.toUpperCase();
  const dst = document.getElementById("dst-provider")?.value?.toUpperCase();
  const select = document.getElementById("sync-mode");
  if (!select || !src || !dst) return;

  const dir =
    data.directions.find((d) => d.source === src && d.target === dst) ||
    data.directions.find((d) => d.source === dst && d.target === src); // fallback
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

// expose globally
window.populateSyncModes = populateSyncModes;
