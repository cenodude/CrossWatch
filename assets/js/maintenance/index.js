/* assets/js/maintenance/index.js */
/* CrossWatch - Maintenance tools page */
import {pageBackLink} from "../page-return.js";
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const REQUEST_TIMEOUT_MS = 45_000;
const fjson = async (url, opts = {}) => {
  const controller = new AbortController();
  const externalSignal = opts.signal;
  const forwardAbort = () => controller.abort();
  let timedOut = false;
  if (externalSignal?.aborted) controller.abort();
  else externalSignal?.addEventListener("abort", forwardAbort, { once: true });
  const timeout = window.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, REQUEST_TIMEOUT_MS);

  try {
    const r = await fetch(url, { cache: "no-store", ...opts, signal: controller.signal });
    if (!r.ok) {
      const msg = `${r.status} ${r.statusText || ""}`.trim();
      throw new Error(msg || "Request failed");
    }
    if (r.status === 204) return {};
    try {
      return await r.json();
    } catch {
      return {};
    }
  } catch (error) {
    if (timedOut) throw new Error("Request timed out after 45 seconds");
    throw error;
  } finally {
    window.clearTimeout(timeout);
    externalSignal?.removeEventListener("abort", forwardAbort);
  }
};

const $ = (sel, root = document) => root.querySelector(sel);
const escapeHtml = (value) => String(value ?? "").replace(/[&<>"]/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[ch]));
const post = (url, body) =>
  fjson(url, body === undefined ? { method: "POST" } : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
const listParts = (data, defs) => defs.flatMap(([k, label]) => data && data[k] != null ? [`${data[k]} ${label}${data[k] === 1 ? "" : "s"}`] : []);
const notifyTrackerStateChanged = (result = {}) => {
  try {
    if (window.CW?.Maintenance?.applySyncStateReset) {
      window.CW.Maintenance.applySyncStateReset({ ...(result || {}), source: "tracker" });
      return;
    }
  } catch {}
  try {
    localStorage.removeItem("cw.dashboardWidgets.data.v1");
    localStorage.removeItem("cw.profileWidgets.data.v1");
  } catch {}
  try {
    window.dispatchEvent(new CustomEvent("cw:sync-state-cleared", {
      detail: { source: "tracker", result },
    }));
  } catch {}
  try { window.CW?.DashboardWidgets?.refresh?.({ force: true, forceConfig: true, preserve: false }); } catch {}
};
const SIMPLE_OPS = {
  state: "/api/maintenance/clear-state",
  cache: "/api/maintenance/clear-cache",
  metadata: "/api/maintenance/clear-metadata-cache",
  scrobbles: "/api/maintenance/clear-recent-scrobbles",
  stats: "/api/maintenance/reset-stats",
  playing: "/api/maintenance/reset-currently-watching",
  captures: "/api/snapshots/clear",
  "database-health": "/api/maintenance/database-health",
  "events-health": "/api/maintenance/events-health",
  "events-optimize": "/api/maintenance/events-optimize",
  "events-rebuild": "/api/maintenance/events-rebuild",
  "events-purge": "/api/maintenance/events-purge",
  "state-file": "/api/maintenance/state-file/compact",
  "state-file-prune": "/api/maintenance/state-file/prune",
};
const OPS = [
  {
    key: "state",
    kind: "state",
    icon: "deployed_code_history",
    title: "Rebuild sync state",
    desc: "Starts every sync pair from fresh provider baselines.",
  },
  {
    key: "cache",
    kind: "cache",
    icon: "network_node",
    title: "Retry provider items",
    desc: "Clears temporary retry and health data so items are tried again.",
  },
  {
    key: "database-health",
    kind: "database-health",
    icon: "database",
    title: "Database health",
    desc: "Checks the local CrossWatch database integrity and row consistency.",
  },
  {
    key: "state-file",
    kind: "state-file",
    icon: "data_object",
    title: "Compact sync state",
    desc: "Creates an app-state backup, then rewrites the sync state database.",
  },
  {
    key: "state-file-prune",
    kind: "state-file-prune",
    icon: "cleaning_services",
    title: "Prune stale state",
    desc: "Creates an app-state backup, then removes state baselines for pairs or routes that no longer exist.",
  },
  {
    key: "meta",
    kind: "metadata",
    icon: "gallery_thumbnail",
    title: "Refresh artwork & metadata",
    desc: 'Removes cached artwork and metadata so fresh copies are fetched when needed.',
  },
  {
    key: "tracker",
    kind: "tracker",
    icon: "deployed_code",
    title: "CW tracker archive",
    desc: "Manage local tracker state files, snapshots, exports and imports.",
    extra: `
      <div class="action-options tracker-archive-options">
        <label class="tracker-profile-control">
          <span>Profile</span>
          <select id="cxm-cw-profile" class="input"><option value="default">Default</option></select>
        </label>
        <label class="tracker-archive-check"><input type="checkbox" id="cxm-cw-state" checked><span>Tracker state files</span></label>
        <label class="tracker-archive-check"><input type="checkbox" id="cxm-cw-snaps"><span>All snapshots</span></label>
      </div>
    `,
    sideActions: `
      <div class="archive-actions">
        <button type="button" class="archive-btn secondary" id="cxm-cw-export" data-label="Download tracker archive" title="Download tracker archive" aria-label="Download tracker archive">
          <span class="material-symbols-rounded" aria-hidden="true">download</span>
          Download archive
        </button>
        <button type="button" class="archive-btn secondary" id="cxm-cw-import" data-label="Import tracker archive" title="Import tracker archive" aria-label="Import tracker archive">
          <span class="material-symbols-rounded" aria-hidden="true">upload_file</span>
          Import archive
        </button>
        <input type="file" id="cxm-cw-import-file" accept=".zip,.json" hidden>
      </div>
    `,
  },
  {
    key: "scrobbles",
    kind: "scrobbles",
    icon: "podcasts",
    title: "Clear Recent Scrobbles",
    desc: "Clears only the local Recent Scrobble list while keeping other Recent Activity entries.",
  },
  {
    key: "stats",
    kind: "stats",
    icon: "monitoring",
    title: "Rebuild statistics",
    desc: "Rebuilds Statistics, Reports and Insights from clean local data.",
  },
  {
    key: "playing",
    kind: "playing",
    icon: "live_tv",
    title: "Clear currently playing",
    desc: 'Removes stuck items from the local Currently Playing list.',
  },
  {
    key: "events-health",
    kind: "events-health",
    icon: "health_and_safety",
    title: "Health check",
    desc: "Checks database integrity, archive size and event totals.",
  },
  {
    key: "events-optimize",
    kind: "events-optimize",
    icon: "tune",
    title: "Optimize archive",
    desc: "Compacts and re-indexes the event archive to reclaim space.",
  },
  {
    key: "events-purge",
    kind: "events-purge",
    icon: "delete_sweep",
    title: "Clear event data",
    desc: "Empties every event category and the sync activity calendar. Leaves the rest of the local database alone.",
  },
  {
    key: "events-rebuild",
    kind: "events-rebuild",
    icon: "database",
    title: "Rebuild archive",
    desc: "Rebuilds the event archive from current runtime state. Older events may be lost.",
  },
  {
    key: "captures",
    kind: "captures",
    icon: "photo_library",
    title: "Clear all captures",
    desc: "Deletes every saved provider capture from local storage.",
  },
  {
    key: "defaults",
    kind: "defaults",
    icon: "release_alert",
    title: "Factory reset",
    desc: "Returns CrossWatch to a clean install and backs up config.json. Snapshots are kept.",
  },
];
const GROUPS = [
  {
    id: "sync",
    icon: "sync",
    title: "Sync",
    desc: "Keep sync state healthy and up to date.",
    keys: ["state", "cache"],
  },
  {
    id: "playback",
    icon: "play_circle",
    title: "Playback",
    desc: "Manage playback lists and state.",
    keys: ["playing", "scrobbles"],
  },
  {
    id: "reports",
    icon: "bar_chart",
    title: "Reports & Metadata",
    desc: "Rebuild reports and refresh cached metadata.",
    keys: ["stats", "meta"],
  },
  {
    id: "events",
    icon: "history",
    title: "Events",
    desc: "Check, optimize and rebuild the event history archive.",
    keys: ["events-health", "events-optimize", "events-purge", "events-rebuild"],
  },
  {
    id: "state-file",
    icon: "data_object",
    title: "Sync State",
    desc: "Inspect, compact and prune the sync state database.",
    keys: ["database-health", "state-file", "state-file-prune"],
  },
  {
    id: "archive",
    icon: "inventory_2",
    title: "Archive & Recovery",
    desc: "Manage CW Tracker state, snapshots and archive files.",
    keys: ["tracker"],
  },
  {
    id: "captures",
    icon: "photo_library",
    title: "Captures",
    desc: "Manage saved provider captures.",
    keys: ["captures"],
  },
  {
    id: "danger",
    icon: "warning",
    title: "Danger zone",
    desc: "Irreversible actions. Proceed with caution.",
    keys: ["defaults"],
  },
];

const OPS_BY_KEY = Object.fromEntries(OPS.map((op) => [op.key, op]));
const DEFAULT_RECOMMENDED = ["cache", "database-health", "playing"];
const SAFE_ACTIONS = new Set([...DEFAULT_RECOMMENDED, "events-health", "events-optimize", "meta"]);
const DESTRUCTIVE_ACTIONS = new Set(["state", "scrobbles", "stats", "events-purge", "events-rebuild", "captures", "defaults"]);
const riskFor = op => SAFE_ACTIONS.has(op.key) ? "safe" : DESTRUCTIVE_ACTIONS.has(op.key) ? "destructive" : "review";
const historyKey = auth => `cw.maintenance.history.v1:${encodeURIComponent(String(auth?.user?.id || auth?.user?.username || auth?.profileId || "admin"))}`;
function cleanHistory(value) {
  return (Array.isArray(value) ? value : []).filter(entry => entry && Object.hasOwn(OPS_BY_KEY, entry.key)
    && ["success", "issues", "error"].includes(entry.status) && Number.isFinite(entry.at) && entry.at > 0 && Number.isFinite(new Date(entry.at).getTime()))
    .map(({key, status, at, batch}) => ({key, status, at, batch:batch === true}))
    .sort((a,b) => b.at - a.at).slice(0,100);
}
function recommendedKeys(entries) {
  const counts = new Map();
  for (const entry of entries) if (entry.status === "success" && !entry.batch && SAFE_ACTIONS.has(entry.key)) counts.set(entry.key, (counts.get(entry.key) || 0) + 1);
  const candidates = [...SAFE_ACTIONS];
  return candidates.sort((a,b) => ((counts.get(b) || 0) + (DEFAULT_RECOMMENDED.includes(b) ? 2 : 0))
    - ((counts.get(a) || 0) + (DEFAULT_RECOMMENDED.includes(a) ? 2 : 0))).slice(0,3);
}
const groupFor = op => GROUPS.find(group => group.keys.includes(op.key));
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const renderActionRow = op => `<article class="maint-task" data-op="${op.key}" data-kind="${op.kind}" aria-label="${op.title}">
  <div class="task-copy"><strong>${op.title}</strong><p>${op.desc}</p><div class="task-meta"><span class="risk-pill ${riskFor(op)}">${({safe:"Safe",review:"Review first",destructive:"Destructive"})[riskFor(op)]}</span><span class="task-status" data-status="idle">Not run</span><span data-last-run></span></div></div>
  <div class="task-actions">${op.kind === "tracker"
    ? `<button type="button" class="archive-configure" aria-controls="cxm-archive-panel" aria-expanded="false">${icon("tune")}Configure</button>`
    : `<button type="button" class="run-btn action-run-btn" data-label="${op.title}" data-idle-label="Run">${icon("play_arrow")}Run</button>`}<button type="button" class="details-btn" aria-label="Details for ${op.title}" title="Task details">${icon("info")}</button></div>
</article>`;
const renderCategory = group => `<details class="maint-group ${group.id === "danger" ? "danger-group" : ""}" data-group="${group.id}"><summary><span class="action-icon">${icon(group.icon)}</span><span class="group-copy"><strong>${group.title}</strong><span>${group.desc}</span></span><span class="group-count"></span><button type="button" class="run-btn group-run-btn" data-run-group="${group.id}" aria-label="${group.id === "archive" ? "Configure" : "Run all tasks in"} ${group.title}">${icon(group.id === "archive" ? "tune" : "play_arrow")}<span>${group.id === "archive" ? "Configure" : "Run all"}</span></button>${icon("expand_more")}</summary><div class="group-tasks">${group.keys.map(key => renderActionRow(OPS_BY_KEY[key])).join("")}</div></details>`;
let activeRoot, syncRoute;

function injectCSS() {
  const existing = document.getElementById("cw-maint-css");
  if (existing?.tagName === "LINK") return Promise.resolve();
  existing?.remove();
  const link = document.createElement("link");
  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  link.id = "cw-maint-css";
  link.rel = "stylesheet";
  link.href = cssUrl.href;
  return new Promise((resolve) => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
    document.head.appendChild(link);
  });
}

const MaintenancePage = {
  async mount(root) {
    if (!root) return;
    if (activeRoot === root && root.querySelector(".cw-maint")) { syncRoute?.(); return; }
    await injectCSS();

    root.innerHTML = `
      <div class="cw-maint">
        <nav class="maint-breadcrumb" aria-label="Breadcrumb"><a id="cxm-back" href="#main">${icon("arrow_back")}Main</a><span aria-hidden="true">/</span><span>Maintenance</span></nav>
        <div class="maint-heading"><div><h1>Maintenance tools</h1><p>These tools repair or clean local CrossWatch data without touching provider accounts.</p></div><div class="heading-actions"><button type="button" class="run-btn primary" id="cxm-run-recommended">${icon("play_arrow")}Run safe recommended</button><button type="button" id="cxm-history-toggle" aria-controls="cxm-history" aria-expanded="false">${icon("history")}View history</button></div></div>
        <div class="maint-overview">
          <div class="overview-card"><span class="overview-icon good">${icon("check_circle")}</span><div><strong>3 recommended</strong><p>Safe to run now</p></div></div>
          <button type="button" class="overview-card" id="cxm-attention"><span class="overview-icon warning">${icon("warning")}</span><span><strong id="cxm-attention-count">0 need attention</strong><span class="overview-note">Results that need your review</span></span></button>
          <div class="overview-card last-maintenance"><span class="overview-icon">${icon("schedule")}</span><div><strong>Last maintenance</strong><p id="cxm-last-maintenance">No runs recorded yet</p></div><span class="health-note" id="cxm-health-note">Local data only</span></div>
        </div>
        <section class="maint-history" id="cxm-history" aria-labelledby="cxm-history-title" hidden><div class="section-heading"><div><h2 id="cxm-history-title" tabindex="-1">Maintenance history</h2><p>Last 100 runs for this account in this browser.</p></div><button type="button" id="cxm-history-close" aria-label="Close maintenance history">${icon("close")}</button></div><ol id="cxm-history-list"></ol></section>
        <section class="maint-recommendations" aria-labelledby="cxm-recommended-title"><div class="section-heading"><span class="section-icon">${icon("auto_awesome")}</span><div><h2 id="cxm-recommended-title">Recommended actions</h2><p>Your frequent safe tasks. Suggestions adapt as you use them.</p></div></div><div class="recommended-grid" id="cxm-recommended-cards"></div></section>
        <div class="maint-filters">
          <label class="maint-search">${icon("search")}<input type="search" id="cxm-search" placeholder="Search maintenance tasks..." aria-label="Search maintenance tasks"></label>
          <select id="cxm-category" aria-label="Category" data-cw-native-select="true"><option value="all">All categories</option>${GROUPS.map(group => `<option value="${group.id}">${group.title}</option>`).join("")}</select>
          <select id="cxm-risk" aria-label="Risk level" data-cw-native-select="true"><option value="all">All risk levels</option><option value="safe">Safe</option><option value="review">Review first</option><option value="destructive">Destructive</option></select>
          <select id="cxm-filter-status" aria-label="Tasks" data-cw-native-select="true"><option value="all">All tasks</option><option value="attention">Needs attention</option><option value="idle">Not run</option><option value="success">Success</option></select>
        </div>
        <div id="cxm-status" class="status-message" role="status" hidden></div>
        <div class="maint-categories">${[GROUPS.slice(0,4), GROUPS.slice(4,-1)].map(groups => `<div class="maint-category-column">${groups.map(renderCategory).join("")}</div>`).join("")}${renderCategory(GROUPS.at(-1))}</div>
        <p id="cxm-empty" hidden>No maintenance tasks match these filters.</p><p id="cxm-range" class="maint-range"></p>
        <section id="cxm-archive-panel" class="archive-panel" aria-labelledby="cxm-archive-title" hidden>
          <div class="archive-panel-heading"><span class="action-icon">${icon("inventory_2")}</span><div><h2 id="cxm-archive-title" tabindex="-1">CW tracker archive</h2><p>Choose a profile to manage its local tracker data.</p></div><button type="button" id="cxm-archive-close" aria-label="Close archive options">${icon("close")}</button></div>
          ${OPS_BY_KEY.tracker.extra}
          <div class="archive-panel-footer">${OPS_BY_KEY.tracker.sideActions}<button type="button" class="run-btn archive-clear" id="cxm-cw-clear" data-kind="tracker" data-label="CW tracker archive" data-idle-label="Clear selected">${icon("delete_sweep")}Clear selected</button></div>
          <div id="cxm-archive-status" class="status-message" role="status" hidden></div>
        </section>
        <section id="cxm-action-insight" class="action-insight" aria-live="polite" hidden><div class="insight-head"><span class="material-symbols-rounded insight-icon" aria-hidden="true"></span><h2 class="insight-title"></h2><button type="button" id="cxm-insight-close" aria-label="Close task details">${icon("close")}</button></div><div class="insight-metrics"></div><p class="insight-note"></p></section>
        <details class="storage-details" id="cxm-overview-status"><summary>Storage details</summary><div class="status-lines"><span id="cxm-tracker-count">Loading tracker status...</span><span id="cxm-cache-count">Loading cache status...</span></div><dl><dt>Tracker</dt><dd><code id="cxm-tracker-root"></code></dd><dt>Provider cache</dt><dd><code id="cxm-cache-root"></code></dd></dl></details>
      </div>`;
    activeRoot = root;
    function syncFilterSelects() {
      for (const select of root.querySelectorAll(".maint-filters select")) {
        const wrap = window.CW?.IconSelect?.enhance(select, { className: "cw-plain-select maint-filter-select", menuMinWidth: 240 });
        const label = select.getAttribute("aria-label");
        wrap?.querySelector("button")?.setAttribute("aria-label", `${label}: ${select.selectedOptions[0]?.textContent || ""}`);
        wrap?.__cwMenu?.setAttribute("aria-label", label);
      }
    }
    const currentAuth = window.CW?.AuthState?.read?.();
    const auth = currentAuth?.status ? currentAuth : await window.CW?.AuthState?.refresh?.() || currentAuth;
    const storageKey = historyKey(auth);
    let entries = [];
    try { entries = cleanHistory(JSON.parse(localStorage.getItem(storageKey) || "[]")); } catch {}
    const results = new Map();
    for (const entry of entries) if (!results.has(OPS_BY_KEY[entry.key].kind)) results.set(OPS_BY_KEY[entry.key].kind, entry);
    let recommended = recommendedKeys(entries);
    let batchRunning = false;
    function saveResult(key, status) {
      entries = cleanHistory([{key, status, at:Date.now(), batch:batchRunning}, ...entries]);
      try { localStorage.setItem(storageKey, JSON.stringify(entries)); } catch {}
    }
    function updateOverview() {
      const attention = [...results.values()].filter(result => ["issues","error"].includes(result.status)).length;
      $("#cxm-attention-count", root).textContent = `${attention} need attention`;
      $("#cxm-last-maintenance", root).textContent = entries.length ? new Date(entries[0].at).toLocaleString() : "No runs recorded yet";
      $("#cxm-health-note", root).textContent = attention ? "Review recent results" : entries.length ? "No issues recorded" : "Local data only";
      $("#cxm-history-list", root).innerHTML = entries.length ? entries.map(entry => `<li><strong>${OPS_BY_KEY[entry.key].title}</strong><time datetime="${new Date(entry.at).toISOString()}">${new Date(entry.at).toLocaleString()}</time><span class="task-status" data-status="${entry.status}">${({success:"Success",issues:"Issues found",error:"Failed"})[entry.status]}</span>${entry.batch ? '<span class="history-source">Batch</span>' : ""}</li>`).join("") : '<li>No maintenance runs recorded yet.</li>';
      if (!operationBusy) recommended = recommendedKeys(entries);
      const cards = $("#cxm-recommended-cards", root);
      if (cards.dataset.keys !== recommended.join(",")) {
        cards.dataset.keys = recommended.join(",");
        cards.innerHTML = recommended.map(key => {
          const op = OPS_BY_KEY[key];
          return `<article class="recommended-card" data-recommended="${key}"><span class="action-icon">${icon(op.icon)}</span><div class="recommended-copy"><h3>${op.title}</h3><p>${op.desc}</p></div><span class="recommended-badge">${icon("check_circle")}Recommended</span><div class="recommended-footer"><span class="recommended-last">${icon("schedule")}<span data-recommended-last></span></span><button type="button" class="run-btn primary" data-run-recommended="${key}" aria-label="Run ${op.title}">${icon("play_arrow")}Run</button></div></article>`;
        }).join("");
      }
      cards.querySelectorAll("[data-recommended]").forEach(card => {
        const result = results.get(OPS_BY_KEY[card.dataset.recommended].kind);
        card.querySelector("[data-recommended-last]").textContent = result?.status === "running" ? "Running..." : result ? `Last run: ${new Date(result.at).toLocaleString()}` : "Last run: Never";
        card.querySelector("button").disabled = operationBusy;
      });
    }
    const rows = [...root.querySelectorAll(".maint-task")];
    const archivePanel = $("#cxm-archive-panel", root);
    const archiveConfigure = $(".archive-configure", root);
    function setArchiveOpen(open) {
      archivePanel.hidden = !open;
      archiveConfigure.setAttribute("aria-expanded", String(open));
      if (open) {
        showOverviewStatus();
        $("#cxm-archive-status", root).hidden = true;
        archivePanel.scrollIntoView({behavior:"smooth", block:"nearest"});
        $("#cxm-archive-title", root).focus({preventScroll:true});
      }
    }
    archiveConfigure.addEventListener("click", () => setArchiveOpen(archivePanel.hidden));
    $("#cxm-archive-close", root).addEventListener("click", () => { setArchiveOpen(false); archiveConfigure.focus(); });
    $("#cxm-cw-clear", root).addEventListener("click", event => runOp("tracker", event.currentTarget));
    function updateRows() {
      const query = $("#cxm-search", root).value.trim().toLowerCase();
      const category = $("#cxm-category", root).value;
      const risk = $("#cxm-risk", root).value;
      const status = $("#cxm-filter-status", root).value;
      let visible = 0;
      for (const row of rows) {
        const op = OPS_BY_KEY[row.dataset.op];
        const result = results.get(op.kind);
        const statusMatch = status === "all" || (status === "attention" ? ["issues","error"].includes(result?.status) : status === (result?.status || "idle"));
        row.hidden = !((risk === "all" || riskFor(op) === risk) && (category === "all" || groupFor(op).id === category)
          && statusMatch && `${op.title} ${op.desc} ${groupFor(op).title}`.toLowerCase().includes(query));
        if (!row.hidden) visible++;
        const statusEl = $(".task-status", row);
        statusEl.dataset.status = result?.status || "idle";
        statusEl.textContent = ({idle:"Not run",running:"Running",success:"Success",issues:"Issues found",error:"Failed"})[statusEl.dataset.status];
        $("[data-last-run]", row).textContent = result?.at ? new Date(result.at).toLocaleString() : "\u2014";
      }
      $("#cxm-empty", root).hidden = visible > 0;
      if (selectedInsightKind && rows.find(row => row.dataset.kind === selectedInsightKind)?.hidden) showOverviewStatus();
      if (!operationBusy && rows.find(row => row.dataset.kind === "tracker")?.hidden) setArchiveOpen(false);
      $("#cxm-range", root).textContent = `${visible} of ${OPS.length} tasks`;
      root.querySelectorAll("[data-group]").forEach(group => {
        const count = [...group.querySelectorAll(".maint-task")].filter(row => !row.hidden).length;
        group.hidden = !count;
        group.querySelector(".group-count").textContent = `${count} ${count === 1 ? "task" : "tasks"}`;
        if (count && (query || category !== "all" || risk !== "all" || status !== "all")) group.open = true;
      });
      const columns = [...root.querySelectorAll(".maint-category-column")];
      for (const column of columns) column.hidden = !column.querySelector(".maint-group:not([hidden])");
      $(".maint-categories", root).classList.toggle("is-single-column", columns.filter(column => !column.hidden).length === 1);
      updateOverview();
      $("#cxm-run-recommended", root).disabled = operationBusy;
    }
    function updateRoute() {
      const params = new URLSearchParams(location.hash.split("?")[1] || "");
      const group = $("#cxm-category", root).value;
      if (group === "all") params.delete("group"); else params.set("group", group);
      history.replaceState(history.state, "", "#maintenance" + (params.size ? `?${params}` : ""));
    }
    syncRoute = () => {
      const params = new URLSearchParams(location.hash.split("?")[1] || "");
      const category = params.get("group");
      $("#cxm-category", root).value = GROUPS.some(group => group.id === category) ? category : "all";
      const back = pageBackLink(params.get("returnTo") || "#main", location.href, "maintenance");
      $("#cxm-back", root).href = back.href;
      $("#cxm-back", root).innerHTML = icon("arrow_back") + escapeHtml(back.label === "Maintenance" ? "Settings" : back.label);
      syncFilterSelects();
      updateRows();
    };
    for (const input of root.querySelectorAll(".maint-filters input, .maint-filters select")) {
      input.addEventListener(input.tagName === "INPUT" ? "input" : "change", () => {
        if (input.tagName === "SELECT") syncFilterSelects();
        updateRows(); updateRoute();
      });
    }
    $("#cxm-recommended-cards", root).addEventListener("click", event => {
      const button = event.target.closest("[data-run-recommended]");
      if (!button || operationBusy) return;
      const row = rows.find(row => row.dataset.op === button.dataset.runRecommended);
      if (row) runOp(row.dataset.kind, row.querySelector(".action-run-btn"));
    });
    function toggleHistory(open) {
      $("#cxm-history", root).hidden = !open;
      $("#cxm-history-toggle", root).setAttribute("aria-expanded", String(open));
      if (open) { $("#cxm-history", root).scrollIntoView({block:"nearest",behavior:"smooth"}); $("#cxm-history-title", root).focus({preventScroll:true}); }
      else $("#cxm-history-toggle", root).focus();
    }
    $("#cxm-history-toggle", root).addEventListener("click", () => toggleHistory($("#cxm-history", root).hidden));
    $("#cxm-history-close", root).addEventListener("click", () => toggleHistory(false));
    $("#cxm-attention", root).addEventListener("click", () => {
      $("#cxm-search", root).value = "";
      $("#cxm-category", root).value = "all";
      $("#cxm-risk", root).value = "all";
      $("#cxm-filter-status", root).value = "attention";
      syncFilterSelects(); updateRows(); updateRoute();
    });
    const statusEl = $("#cxm-status", root);
    const returnToCaller = () => { $("#cxm-back", root).click(); };
    let statusFadeTimer, statusHideTimer;
    const setStatus = (msg, kind = "") => {
      if (!statusEl) return;
      window.clearTimeout(statusFadeTimer);
      window.clearTimeout(statusHideTimer);
      statusEl.textContent = msg;
      statusEl.className = "status-message" + (kind ? " " + kind : "");
      statusEl.hidden = !msg;
      const archiveStatus = $("#cxm-archive-status", root);
      if (!archivePanel.hidden) {
        archiveStatus.textContent = msg;
        archiveStatus.className = statusEl.className;
        archiveStatus.hidden = !msg;
      }
      if (msg && kind === "ok") {
        statusFadeTimer = window.setTimeout(() => {
          statusEl.classList.add("is-fading");
          archiveStatus.classList.add("is-fading");
          const duration = window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 700;
          statusHideTimer = window.setTimeout(() => {
            statusEl.hidden = true;
            archiveStatus.hidden = true;
          }, duration);
        }, 5000);
      }
    };
    const trackerProfile = () => String($("#cxm-cw-profile", root)?.value || "default").trim() || "default";
    const trackerProfileQuery = () => `provider_instance=${encodeURIComponent(trackerProfile())}`;
    const loadTrackerProfiles = async () => {
      const sel = $("#cxm-cw-profile", root);
      if (!sel) return;
      const current = sel.value || "default";
      try {
        const data = await fjson("/api/provider-instances/CROSSWATCH");
        const list = Array.isArray(data) && data.length ? data : [{ id: "default", label: "Default" }];
        sel.innerHTML = list
          .map(p => `<option value="${escapeHtml(String(p?.id || "default"))}">${escapeHtml(String(p?.label || p?.id || "Default"))}</option>`)
          .join("");
      } catch {
        sel.innerHTML = '<option value="default">Default</option>';
      }
      const values = Array.from(sel.options).map(o => o.value);
      sel.value = values.includes(current) ? current : "default";
      window.CW?.ProfileSelect?.enhanceProfile?.(sel, {
        className: "cxm-tracker-profile-select",
        menuClassName: "cxm-tracker-profile-menu",
        menuMinWidth: 220,
      });
    };

    const downloadTrackerArchive = () => {
      const a = document.createElement("a");
      a.href = `/api/maintenance/crosswatch-tracker/export?${trackerProfileQuery()}`;
      a.download = "crosswatch-tracker.zip";
      document.body.appendChild(a);
      a.click();
      a.remove();
      setStatus("Downloading tracker archive...", "ok");
    };

    const importTrackerArchive = async (file) => {
      if (!file || operationBusy) return;
      const form = new FormData();
      form.append("file", file);
      setOperationBusy(true);
      setStatus("Importing tracker archive...", "busy");
      try {
        const data = await fjson(`/api/maintenance/crosswatch-tracker/import?${trackerProfileQuery()}`, {
          method: "POST",
          body: form,
        });
        if (data?.ok === false) throw new Error(data.error || "Import failed");
        const parts = listParts(data, [["files", "file"], ["states", "state file"], ["snapshots", "snapshot"]]);
        setStatus("Imported " + (parts.length ? parts.join(", ") : "tracker archive") + ".", "ok");
        if (Number(data?.states || 0) > 0) notifyTrackerStateChanged(data);
        await refreshSummary();
        if (selectedInsightKind === "tracker") await loadActionInsight("tracker");
      } catch (e) {
        setStatus(`Import failed: ${e.message || String(e)}`, "err");
      } finally {
        setOperationBusy(false);
      }
    };

    let selectedInsightKind = null;
    let insightRequestId = 0;
    let operationBusy = false;

    function setOperationBusy(busy) {
      if (operationBusy === busy) return;
      operationBusy = busy;
      const controls = root.querySelectorAll(".run-btn, .archive-btn, .archive-configure, #cxm-cw-profile, #cxm-cw-state, #cxm-cw-snaps, #cxm-archive-close");
      controls.forEach((control) => {
        if (busy) {
          control.dataset.cwWasDisabled = control.disabled ? "1" : "0";
          control.disabled = true;
        } else {
          control.disabled = control.dataset.cwWasDisabled === "1";
          delete control.dataset.cwWasDisabled;
        }
      });
      $(".cw-maint", root)?.toggleAttribute("aria-busy", busy);
      updateRows();
    }

    const formatBytes = (raw) => {
      const bytes = Number(raw || 0);
      if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
      const units = ["B", "KB", "MB", "GB", "TB"];
      const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
      const value = bytes / (1024 ** index);
      const digits = index === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2;
      return `${value.toFixed(digits)} ${units[index]}`;
    };

    const resultSummary = (result) => result?.summary || result?.result?.summary || null;
    const combineSummaries = (results) => {
      const summaries = results.map(resultSummary).filter(Boolean);
      if (!summaries.length) return null;
      return summaries.reduce((total, item) => ({
        removed_files: total.removed_files + Number(item.removed_files || 0),
        removed_items: total.removed_items + Number(item.removed_items || 0),
        freed_bytes: total.freed_bytes + Number(item.freed_bytes || 0),
      }), { removed_files: 0, removed_items: 0, freed_bytes: 0 });
    };
    const plural = (count, one, many = `${one}s`) => `${new Intl.NumberFormat().format(count)} ${count === 1 ? one : many}`;
    const completionReceipt = (label, results, extra = []) => {
      const list = Array.isArray(results) ? results : [results];
      const summary = combineSummaries(list);
      const details = [...extra];
      if (summary) {
        if (summary.removed_items > 0) details.push(plural(summary.removed_items, "item"));
        if (summary.removed_files > 0) details.push(plural(summary.removed_files, "file"));
        if (summary.freed_bytes > 0) details.push(`${formatBytes(summary.freed_bytes)} cleared`);
        if (!summary.removed_items && !summary.removed_files && !summary.freed_bytes) details.push("nothing to remove");
      }
      details.push(new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
      return `${label} completed · ${details.join(" · ")}.`;
    };

    const eventsReceipt = (kind, res) => {
      if (!res || typeof res !== "object") return null;
      if (kind === "events-health") {
        const bits = [
          `integrity ${res.integrity || "?"}`,
          `${new Intl.NumberFormat().format(res.events || 0)} events`,
          `${new Intl.NumberFormat().format(res.acknowledged || 0)} acknowledged`,
          formatBytes(res.size_bytes || 0),
        ];
        if (res.wal_size_bytes) bits.push(`WAL ${formatBytes(res.wal_size_bytes)}`);
        if (res.exists === false) bits.push("file missing");
        bits.push(`schema v${res.schema_version ?? "?"}`);
        return `Health check · ${res.healthy ? "healthy" : "issues found"} · ${bits.join(" · ")}.`;
      }
      if (kind === "events-optimize") {
        return `Optimize archive · freed ${formatBytes(res.reclaimed_bytes || 0)} · ${formatBytes(res.before_bytes || 0)} → ${formatBytes(res.after_bytes || 0)} · ${res.duration_ms ?? 0} ms.`;
      }
      if (kind === "events-rebuild") {
        return `Rebuild archive · ${new Intl.NumberFormat().format(res.events || 0)} events rebuilt from runtime state.`;
      }
      return null;
    };

    const databaseReceipt = (kind, res) => {
      if (!res || typeof res !== "object" || kind !== "database-health") return null;
      const tables = res.table_counts || {};
      const orphanCounts = res.orphan_counts || {};
      const orphanRows = Object.values(orphanCounts).reduce((total, value) => total + Number(value || 0), 0);
      const bits = [
        `integrity ${res.integrity || "?"}`,
        `schema v${res.schema_version ?? "?"}`,
        `${new Intl.NumberFormat().format(tables.provider_feature_state || 0)} baselines`,
        `${new Intl.NumberFormat().format(tables.baseline_items || 0)} items`,
        `${new Intl.NumberFormat().format(orphanRows)} orphan rows`,
        formatBytes(res.size_bytes || 0),
      ];
      if (res.wal_size_bytes) bits.push(`WAL ${formatBytes(res.wal_size_bytes)}`);
      return `Database health · ${res.healthy ? "healthy" : "issues found"} · ${bits.join(" · ")}.`;
    };

    const stateFileReceipt = (res) => {
      if (!res || typeof res !== "object") return null;
      const backupPath = res.backup && res.backup.path ? String(res.backup.path) : "";
      const before = formatBytes(res.before_bytes || 0);
      const after = formatBytes(res.after_bytes || 0);
      const freed = formatBytes((res.summary && res.summary.freed_bytes) || 0);
      const bits = [`${before} → ${after}`, `${freed} reclaimed`];
      if (backupPath) bits.push(`backup ${backupPath}`);
      return `Compact sync state completed · ${bits.join(" · ")}.`;
    };

    const statePruneReceipt = (res) => {
      if (!res || typeof res !== "object") return null;
      const removed = res.removed || {};
      const baselines = Number(removed.removed_baselines || 0);
      const instances = Number(removed.removed_instances || 0);
      const providers = Number(removed.removed_providers || 0);
      const items = Number(removed.removed_items || 0);
      const freed = formatBytes((res.summary && res.summary.freed_bytes) || 0);
      const backupPath = res.backup && res.backup.path ? String(res.backup.path) : "";
      const bits = [`${providers} providers`, `${instances} instances`, `${baselines} baselines`, `${items} items`, `${freed} reclaimed`];
      if (backupPath) bits.push(`backup ${backupPath}`);
      return `Prune sync state completed · ${bits.join(" · ")}.`;
    };

    const formatMetric = ({ value, format }) => {
      if (format === "bytes") return formatBytes(value);
      if (format === "datetime") {
        if (!value) return "Never";
        const date = new Date(Number(value) * 1000);
        return Number.isNaN(date.getTime()) ? "Unknown" : date.toLocaleString();
      }
      if (typeof value === "number") return new Intl.NumberFormat().format(value);
      return value ?? "-";
    };

    function showOverviewStatus() {
      selectedInsightKind = null;
      insightRequestId += 1;
      root.querySelectorAll(".maint-task.is-inspected").forEach((row) => row.classList.remove("is-inspected"));
      const overview = $("#cxm-overview-status", root);
      const insight = $("#cxm-action-insight", root);
      if (overview) overview.hidden = false;
      if (insight) {
        insight.hidden = true;
        insight.classList.remove("loading", "load-error");
      }
    }

    function renderActionInsight(op, payload = null, state = "ready") {
      const insight = $("#cxm-action-insight", root);
      if (!insight) return;
      insight.hidden = false;
      insight.classList.toggle("loading", state === "loading");
      insight.classList.toggle("load-error", state === "error");

      $(".insight-icon", insight).textContent = op.icon;
      $(".insight-title", insight).textContent = payload?.title || op.title;
      const metricsRoot = $(".insight-metrics", insight);
      metricsRoot.replaceChildren();

      if (state === "loading") {
        for (let i = 0; i < 3; i += 1) {
          const skeleton = document.createElement("div");
          skeleton.className = "insight-metric skeleton";
          metricsRoot.appendChild(skeleton);
        }
      } else {
        (payload?.metrics || []).forEach((metric) => {
          const item = document.createElement("div");
          item.className = "insight-metric";
          item.dataset.format = metric.format || "number";
          const value = document.createElement("div");
          value.className = "insight-value";
          value.textContent = formatMetric(metric);
          const label = document.createElement("div");
          label.className = "insight-label";
          label.textContent = metric.label || "Value";
          item.append(value, label);
          metricsRoot.appendChild(item);
        });
      }

      const note = $(".insight-note", insight);
      note.textContent = state === "loading"
        ? "Reading current local data..."
        : state === "error"
          ? "Status data could not be loaded. The maintenance action is still available."
          : payload?.note || "";
    }

    async function loadActionInsight(kind) {
      const op = OPS.find((item) => item.kind === kind);
      if (!op) return;
      if (!operationBusy) setStatus("");
      selectedInsightKind = kind;
      const requestId = ++insightRequestId;
      root.querySelectorAll(".maint-task").forEach((row) => {
        row.classList.toggle("is-inspected", row.dataset.kind === kind);
      });
      const overview = $("#cxm-overview-status", root);
      if (overview) overview.hidden = true;
      renderActionInsight(op, null, "loading");
      $("#cxm-action-insight", root)?.scrollIntoView({behavior:"smooth", block:"nearest"});

      try {
        const payload = await fjson(`/api/maintenance/action-status/${encodeURIComponent(kind)}`);
        if (requestId !== insightRequestId || selectedInsightKind !== kind) return;
        if (payload?.ok === false) throw new Error(payload.error || "Status unavailable");
        renderActionInsight(op, payload, "ready");
      } catch {
        if (requestId !== insightRequestId || selectedInsightKind !== kind) return;
        renderActionInsight(op, null, "error");
      }
    }

    $("#cxm-insight-close", root).addEventListener("click", showOverviewStatus);

    async function refreshSummary() {
      try {
        const [tracker, cache] = await Promise.all([
          fjson(`/api/maintenance/crosswatch-tracker?${trackerProfileQuery()}`).catch(() => null),
          fjson("/api/maintenance/provider-cache").catch(() => null),
        ]);

        const trackerRoot = tracker?.root || "/config/.cw_provider";
        const cacheRoot = cache?.root || "/config/.cw_state";

        $("#cxm-tracker-root", root).textContent = trackerRoot;
        $("#cxm-cache-root", root).textContent = cacheRoot;

        const tCounts = tracker?.counts || {};
        const tState = tCounts.state_files ?? "-";
        const tSnap = tCounts.snapshots ?? "-";
        $("#cxm-tracker-count", root).textContent =
          `Tracker ${tState} state · ${tSnap} snapshots`;

        const cCount = cache?.count ?? "-";
        $("#cxm-cache-count", root).textContent =
          `Provider cache ${cCount} file${cCount === 1 ? "" : "s"}`;
      } catch {

      }
    }

    function actionRow(btn) {
      return btn.closest(".maint-task") || (btn.dataset.kind === "tracker" ? root.querySelector('.maint-task[data-kind="tracker"]') : null);
    }

    function resetActionFeedback(btn) {
      if (!btn) return;
      if (btn._cwResultTimer) window.clearTimeout(btn._cwResultTimer);
      btn._cwResultTimer = null;
      btn.classList.remove("busy", "result-success", "result-error");
      btn.removeAttribute("aria-busy");
      btn.innerHTML = icon("play_arrow") + escapeHtml(btn.dataset.idleLabel || "Run");
      actionRow(btn)?.classList.remove("is-running", "run-success", "run-error");
    }

    function startActionFeedback(btn) {
      if (!btn) return;
      btn.dataset.idleLabel ||= btn.textContent.trim() || "Run";
      resetActionFeedback(btn);
      btn.classList.add("busy");
      btn.setAttribute("aria-busy", "true");
      actionRow(btn)?.classList.add("is-running");
      const kind = actionRow(btn)?.dataset.kind;
      if (kind) { results.set(kind, {status:"running",at:Date.now(),previous:results.get(kind)}); updateRows(); }
    }

    function finishActionFeedback(btn, result) {
      if (!btn) return;
      const row = actionRow(btn);
      row?.classList.remove("is-running");
      btn.classList.remove("busy");
      btn.removeAttribute("aria-busy");
      if (result === "cancel") {
        const kind = row?.dataset.kind;
        if (kind) { const previous = results.get(kind)?.previous; if (previous) results.set(kind,previous); else results.delete(kind); updateRows(); }
        resetActionFeedback(btn);
        return;
      }

      if (row) { results.set(row.dataset.kind, {status:result,at:Date.now()}); saveResult(row.dataset.op, result); updateRows(); }
      const ok = result === "success";
      row?.classList.add(ok ? "run-success" : "run-error");
      btn.classList.add(ok ? "result-success" : "result-error");
      btn.textContent = ok ? "Done" : result === "issues" ? "Issues" : "Failed";
      btn._cwResultTimer = window.setTimeout(() => resetActionFeedback(btn), 1400);
    }

    async function runOp(kind, btn, options = {}) {
      const {
        manageLock = true,
      } = options;
      if (manageLock && operationBusy) return false;

      if (kind === "captures" && !confirm("Delete all saved captures? This cannot be undone.")) {
        setStatus("Cancelled.", "");
        return false;
      }

      if (kind === "events-purge" && !confirm("Clear all event data?\n\nThis empties every event category (sync, scrobble, audits) and the sync activity calendar.\n\nNothing else in the local database is touched. This cannot be undone.")) {
        setStatus("Cancelled.", "");
        return false;
      }

      if (kind === "events-rebuild" && !confirm("This removes the current event archive and rebuilds it from current runtime state.\n\nHistorical events that only exist in the event database may be lost.\n\nThis does not change CrossWatch configuration or provider runtime state.")) {
        setStatus("Cancelled.", "");
        return false;
      }

      if (kind === "state-file" && !confirm("Create an app-state backup and compact the sync state database?")) {
        setStatus("Cancelled.", "");
        return false;
      }

      if (kind === "state-file-prune" && !confirm("Create an app-state backup and prune stale sync state baselines?\n\nThis removes provider or instance baselines that are no longer referenced by configured sync pairs or scrobbler routes.")) {
        setStatus("Cancelled.", "");
        return false;
      }

      if (manageLock) setOperationBusy(true);
      startActionFeedback(btn);
      const label = btn?.dataset?.label || OPS.find((item) => item.kind === kind)?.title || kind;
      setStatus(`Running ${label.toLowerCase()}...`, "busy");

      try {
        let res = null;
        let trackerStateChanged = false;
        if (SIMPLE_OPS[kind]) {
          const body = kind === "stats" ? {
            recalc: false,
            purge_file: true,
            purge_state: false,
            purge_reports: true,
            purge_insights: true,
          } : (kind === "events-rebuild" || kind === "events-purge") ? { confirm: true } : undefined;
          res = await post(SIMPLE_OPS[kind], body);
        } else if (kind === "tracker") {
          const chkState = $("#cxm-cw-state", root);
          const chkSnaps = $("#cxm-cw-snaps", root);
          const clearState = !!(chkState && chkState.checked);
          const clearSnaps = !!(chkSnaps && chkSnaps.checked);

          if (!clearState && !clearSnaps) {
            setStatus("Select at least one option for tracker cleanup.", "err");
            finishActionFeedback(btn, "error");
            return false;
          }

          res = await post("/api/maintenance/crosswatch-tracker/clear", {
            clear_state: clearState,
            clear_snapshots: clearSnaps,
            provider_instance: trackerProfile(),
          });
          trackerStateChanged = clearState;
        } else if (kind === "defaults") {
          const warn = [
            "WARNING Reset all to default",
            "",
            "This will delete local state, provider cache, tracker files, reports, metadata cache and TLS material.",
            "It will also move /config/config.json to a timestamped backup file.",
            "",
            "Snapshots are NOT deleted ( /config/snapshots ).",
            "",
            "Are you absolutely sure you want to continue?"
          ].join("\n");

          if (!confirm(warn)) {
            setStatus("Cancelled.", "");
            finishActionFeedback(btn, "cancel");
            return false;
          }

          {
            const typed = prompt("Type RESET to continue");
            if (String(typed || "").trim().toUpperCase() !== "RESET") {
              setStatus("Cancelled.", "");
              finishActionFeedback(btn, "cancel");
              return false;
            }
          }
          res = await post("/api/maintenance/reset-all-default", {});
        }

        if (res?.ok === false) {
          setStatus(`Failed: ${res.error || "Unknown error"}`, "err");
          finishActionFeedback(btn, "error");
          return false;
        }

        if (kind === "scrobbles") {
          try { window.dispatchEvent(new CustomEvent("activity-log-cleared")); } catch {}
        }
        if (kind === "state") {
          try { window.CW?.Maintenance?.applySyncStateReset?.(res || { ok: true }); } catch {}
          await refreshSummary();
        }
        if (kind === "tracker" && trackerStateChanged) {
          notifyTrackerStateChanged(res || { ok: true });
        }
        if (kind === "cache" || kind === "tracker") await refreshSummary();

        if (kind === "defaults") {
          finishActionFeedback(btn, "success");
          returnToCaller();
          setTimeout(() => {
            if (window.cwRestartCrossWatchWithOverlay) {
              window.cwRestartCrossWatchWithOverlay();
            } else {
              fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" }).finally(() => {
                window.location.reload();
              });
            }
          }, 150);
          return res || { ok: true };
        }

        if (selectedInsightKind === kind) await loadActionInsight(kind);
        const evReceipt = eventsReceipt(kind, res);
        const dbReceipt = databaseReceipt(kind, res);
        const stateReceipt = kind === "state-file" ? stateFileReceipt(res) : null;
        const statePrune = kind === "state-file-prune" ? statePruneReceipt(res) : null;
        setStatus(evReceipt || dbReceipt || stateReceipt || statePrune || completionReceipt(label, res), res?.healthy === false ? "err" : "ok");
        finishActionFeedback(btn, res?.healthy === false ? "issues" : "success");
        return res || { ok: true };
      } catch (e) {
        setStatus(`Error: ${e.message || String(e)}`, "err");
        finishActionFeedback(btn, "error");
        return false;
      } finally {
        if (manageLock) setOperationBusy(false);
      }
    }

    async function runCategory(groupId, button) {
      if (operationBusy) return;
      const group = GROUPS.find(item => item.id === groupId);
      if (!group) return;
      if (group.id === "archive") {
        setArchiveOpen(true);
        return;
      }
      const tasks = group.keys.map(key => rows.find(row => row.dataset.op === key))
        .filter(row => row && !row.hidden && row.querySelector(".action-run-btn"));
      if (!tasks.length) return;
      const taskList = tasks.map(row => {
        const op = OPS_BY_KEY[row.dataset.op];
        return `${op.title}: ${op.desc}`;
      }).join("\n\n");
      if (!confirm(`Run ${tasks.length} ${tasks.length === 1 ? "task" : "tasks"} in ${group.title}?\n\n${taskList}`)) return;
      const container = button.closest("details");
      if (container) container.open = true;
      const label = button.querySelector("span:last-child");
      batchRunning = true;
      setOperationBusy(true);
      button.setAttribute("aria-busy", "true");
      try {
        const completed = [];
        for (const row of tasks) {
          if (!root.isConnected) return;
          label.textContent = `${completed.length + 1}/${tasks.length}`;
          const result = await runOp(row.dataset.kind, row.querySelector(".action-run-btn"), {manageLock:false});
          if (!result || result.healthy === false) return;
          completed.push(result);
        }
        setStatus(completionReceipt(group.title, completed, [plural(completed.length, "task")]), "ok");
      } finally {
        label.textContent = "Run all";
        button.removeAttribute("aria-busy");
        batchRunning = false;
        setOperationBusy(false);
      }
    }

    OPS.forEach(({ key, kind }) => {
      const row = root.querySelector(`.maint-task[data-op="${key}"]`);
      const btn = row?.querySelector(".action-run-btn");
      if (btn) btn.addEventListener("click", () => runOp(kind, btn));
      row?.querySelector(".details-btn")?.addEventListener("click", () => loadActionInsight(kind));
      row?.addEventListener("click", (event) => {
        if (event.target.closest("button, input, label, a, summary, .cw-icon-select")) return;
        loadActionInsight(kind);
      });
      row?.addEventListener("keydown", (event) => {
        if (event.target !== row || !["Enter", " "].includes(event.key)) return;
        event.preventDefault();
        loadActionInsight(kind);
      });
    });

    root.querySelectorAll("[data-run-group]").forEach(button => {
      button.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        void runCategory(button.dataset.runGroup, button);
      });
    });

    root.querySelector("#cxm-cw-export")?.addEventListener("click", downloadTrackerArchive);
    root.querySelector("#cxm-cw-profile")?.addEventListener("change", async () => {
      await refreshSummary();
      if (selectedInsightKind === "tracker") await loadActionInsight("tracker");
    });
    const archiveInput = root.querySelector("#cxm-cw-import-file");
    root.querySelector("#cxm-cw-import")?.addEventListener("click", () => archiveInput?.click());
    archiveInput?.addEventListener("change", async () => {
      const file = archiveInput.files && archiveInput.files[0];
      try {
        await importTrackerArchive(file);
      } finally {
        try { archiveInput.value = ""; } catch {}
      }
    });

    $("#cxm-run-recommended", root).addEventListener("click", async () => {
      if (operationBusy) return;
      const tasks = recommended.filter(key => SAFE_ACTIONS.has(key)).map(key => rows.find(row => row.dataset.op === key)).filter(Boolean);
      if (!tasks.length) return;
      batchRunning = true;
      setOperationBusy(true);
      try {
        const completed = [];
        for (const row of tasks) {
          const result = await runOp(row.dataset.kind, row.querySelector(".action-run-btn"), {manageLock:false});
          if (!result || result.healthy === false) return;
          completed.push(result);
        }
        setStatus(completionReceipt("Recommended tasks", completed, [plural(completed.length, "tool")]), "ok");
      } finally { batchRunning = false; setOperationBusy(false); }
    });
    syncRoute();
    showOverviewStatus();
    const loadMaintenanceBootStatus = async () => {
      await loadTrackerProfiles();
      await refreshSummary();
      if (root.isConnected && !operationBusy && statusEl.textContent === "Loading maintenance status...") setStatus("");
    };
    setStatus("Loading maintenance status...", "busy");
    void loadMaintenanceBootStatus();
  },
};
window.MaintenancePage = MaintenancePage;
export default MaintenancePage;
