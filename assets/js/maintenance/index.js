/* assets/js/maintenance/index.js */
/* CrossWatch - Maintenance tools page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import {pageBackLink} from "../page-return.js";

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
    } catch (error) {
      if (error instanceof SyntaxError) throw new Error("Server returned an invalid response.");
      throw error;
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
  "database-rebuild": "/api/maintenance/database-rebuild",
};
const JOB_ACTIONS = new Set(["events-optimize", "events-health", "database-health", "state-file", "state-file-prune", "events-rebuild", "events-purge", "state", "database-rebuild"]);

const OPS = [
  {
    key: "state",
    kind: "state",
    icon: "deployed_code_history",
    title: "Reset all sync comparisons",
    desc: "Clears saved comparisons so every pair starts from fresh provider data.",
    note: "The next sync works like a first sync. Two-way pairs can add back items removed on one side.",
  },
  {
    key: "pair-state",
    kind: "pair-state",
    icon: "restart_alt",
    title: "Reset one sync pair",
    desc: "Start fresh comparisons for this pair. Other pairs are unchanged.",
    note: "Recent deletions will not carry over. Two-way sync may add missing items back.",
  },
  {
    key: "cache",
    kind: "cache",
    icon: "network_node",
    title: "Let skipped items try again",
    desc: "Clears temporary failure and retry records so items can be tried again.",
    note: "This does not start a sync or fix a provider that is still unavailable.",
  },
  {
    key: "database-health",
    kind: "database-health",
    icon: "database",
    title: "Check for database problems",
    desc: "Checks the local database for damage and inconsistent records, then reports the result.",
  },
  {
    key: "state-file",
    kind: "state-file",
    icon: "data_object",
    title: "Reorganize saved sync data",
    desc: "Makes a backup, then rewrites the saved sync data in a compact form.",
  },
  {
    key: "state-file-prune",
    kind: "state-file-prune",
    icon: "cleaning_services",
    title: "Remove unused sync records",
    desc: "Makes a backup, then removes obsolete pair records, unused provider comparisons and leftover retry files.",
  },
  {
    key: "database-rebuild",
    kind: "database-rebuild",
    icon: "settings_backup_restore",
    title: "Replace a damaged local database",
    desc: "Backs up the database files, then starts a fresh local database.",
    note: "Use for recovery, not routine cleanup. Local sync comparisons, mappings, events and reports start empty.",
  },
  {
    key: "meta",
    kind: "metadata",
    icon: "gallery_thumbnail",
    title: "Refresh pictures and title details",
    desc: "Clears saved artwork and metadata. Fresh copies are fetched as needed.",
    note: "Pictures may take longer to appear while they are fetched again.",
  },
  {
    key: "tracker",
    kind: "tracker",
    icon: "deployed_code",
    title: "CW Tracker",
    desc: "Clean up the tracked lists or saved restore points for a local Tracker instance.",
    sideActions: `
      <div class="archive-actions">
        <button type="button" class="archive-btn secondary" id="cxm-cw-export" data-label="Download Tracker backup" title="Download Tracker backup" aria-label="Download Tracker backup">
          <span class="material-symbols-rounded" aria-hidden="true">download</span>
          Download backup
        </button>
        <button type="button" class="archive-btn secondary" id="cxm-cw-import" data-label="Import Tracker backup" title="Import Tracker backup" aria-label="Import Tracker backup">
          <span class="material-symbols-rounded" aria-hidden="true">upload_file</span>
          Import backup
        </button>
        <input type="file" id="cxm-cw-import-file" accept=".zip,.json" hidden>
      </div>
    `,
  },
  {
    key: "scrobbles",
    kind: "scrobbles",
    icon: "podcasts",
    title: "Clear the Recent Scrobble list",
    desc: "Removes local Recent Scrobble entries while keeping other Recent Activity entries.",
    note: "Provider watch history is kept.",
  },
  {
    key: "stats",
    kind: "stats",
    icon: "monitoring",
    title: "Rebuild statistics and reports",
    desc: "Resets local statistics, Reports and Insights so they can be rebuilt.",
  },
  {
    key: "playing",
    kind: "playing",
    icon: "live_tv",
    title: "Clear the Currently Playing list",
    desc: "Removes stuck items from the local Currently Playing list.",
  },
  {
    key: "events-health",
    kind: "events-health",
    icon: "health_and_safety",
    title: "Check the event archive",
    desc: "Checks the event database and reports its integrity, size and event totals.",
  },
  {
    key: "events-optimize",
    kind: "events-optimize",
    icon: "tune",
    title: "Reclaim unused database space",
    desc: "Compacts the shared runtime and event database to reclaim unused disk space.",
  },
  {
    key: "events-purge",
    kind: "events-purge",
    icon: "delete_sweep",
    title: "Delete all event history",
    desc: "Clears every event category and the sync activity calendar.",
    note: "Existing event history is removed.",
  },
  {
    key: "events-rebuild",
    kind: "events-rebuild",
    icon: "database",
    title: "Rebuild the event archive",
    desc: "Starts an empty event archive while keeping current runtime data.",
    note: "Historical events will be removed.",
  },
  {
    key: "captures",
    kind: "captures",
    icon: "photo_library",
    title: "Delete all saved captures",
    desc: "Deletes every saved provider capture from local storage.",
  },
  {
    key: "defaults",
    kind: "defaults",
    icon: "release_alert",
    title: "Reset CrossWatch to a clean install",
    desc: "Clears local data and returns CrossWatch to a fresh setup.",
    note: "A configuration backup and provider snapshots are kept. Review the full reset confirmation before continuing.",
  },
];
const GROUPS = [
  {id:"sync", title:"Sync issues", keys:["cache", "pair-state", "state"]},
  {id:"archive", title:"CW Tracker", keys:["tracker"]},
  {id:"reports", title:"Refresh artwork and statistics", desc:"Update pictures, title details and report data.", keys:["meta", "stats"]},
  {id:"playback", title:"Manage activity lists and captures", desc:"Clear local scrobble entries, playing items or saved captures.", keys:["playing", "scrobbles", "captures"]},
  {id:"state-file", title:"Check data or free up space", desc:"Check for problems and clean up stored data.", keys:["database-health", "events-health", "events-optimize", "state-file", "state-file-prune"]},
  {id:"danger", title:"Repair or start over", desc:"Recovery tools that can replace or delete local data.", keys:["database-rebuild", "events-rebuild", "events-purge", "defaults"]},
];
const QUICK_TASKS = ["meta", "playing", "database-health"];
const SECTIONS = [["quick", "Quick fixes"], ["sync", "Sync issues"], ["tracker", "CW Tracker"], ["advanced", "Advanced"]];
const OPS_BY_KEY = Object.fromEntries(OPS.map(op => [op.key, op]));
const historyKey = auth => `cw.maintenance.history.v1:${encodeURIComponent(String(auth?.user?.id || auth?.user?.username || auth?.profileId || "admin"))}`;
function cleanHistory(value) {
  return (Array.isArray(value) ? value : []).filter(entry => entry && Object.hasOwn(OPS_BY_KEY, entry.key)
    && ["success", "issues", "error"].includes(entry.status) && Number.isFinite(entry.at) && entry.at > 0 && Number.isFinite(new Date(entry.at).getTime()))
    .map(({key, status, at, batch, jobId}) => ({key, status, at, batch:batch === true,
      ...(typeof jobId === "string" && jobId.length > 0 && jobId.length <= 128 ? {jobId} : {})}))
    .sort((a,b) => b.at - a.at).slice(0,100);
}
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const renderActionRow = op => `<button type="button" class="maint-task task-open" data-op="${op.key}" data-kind="${op.kind}" aria-controls="cxm-task-panel">
  <span class="action-icon">${icon(op.icon)}</span><span class="task-copy"><strong>${op.title}</strong><span class="task-description">${op.desc}</span><span class="task-status" hidden></span></span>${icon("chevron_right")}
</button>`;
const renderCategory = group => `<details class="maint-group ${group.id === "danger" ? "danger-group" : ""}" data-group="${group.id}"><summary><span class="group-copy"><strong>${group.title}</strong><span>${group.desc}</span></span>${icon("expand_more")}</summary><div class="group-tasks">${group.keys.map(key => renderActionRow(OPS_BY_KEY[key])).join("")}</div></details>`;
let activeRoot, syncRoute;


const MaintenancePage = {
  async mount(root) {
    if (!root) return;
    if (activeRoot === root && root.querySelector(".cw-maint")) { syncRoute?.(); return; }

    root.innerHTML = `
      <div class="cw-maint cw-page-maint-tools">
        <nav class="maint-breadcrumb" aria-label="Breadcrumb"><a id="cxm-back" href="#main">${icon("arrow_back")}Main</a><span aria-hidden="true">/</span><span>Maintenance</span></nav>
        <div class="maint-heading"><div><h1>Maintenance</h1><p>Fix common issues and manage local data.</p></div><button type="button" id="cxm-history-toggle" aria-controls="cxm-history" aria-expanded="false">${icon("history")}History</button></div>
        <p class="maint-last" id="cxm-last-maintenance">No maintenance runs recorded yet</p>
        <nav class="maint-tabs" aria-label="Maintenance sections">${SECTIONS.map(([id,label]) => `<button type="button" data-section="${id}" aria-controls="cxm-section-${id}"${id === "quick" ? ' aria-current="page"' : ""}>${label}</button>`).join("")}</nav>
        <div id="cxm-status" class="status-message" role="status" hidden></div>
        <section class="maint-section" id="cxm-section-quick" data-pane="quick" aria-labelledby="cxm-quick-title"><h2 id="cxm-quick-title">What needs fixing?</h2><div class="maint-task-list">${QUICK_TASKS.map(key => renderActionRow(OPS_BY_KEY[key])).join("")}</div></section>
        <section class="maint-section" id="cxm-section-sync" data-pane="sync" aria-labelledby="cxm-sync-title" hidden><h2 id="cxm-sync-title">Get syncing again</h2><p class="section-description">Choose the problem you want to resolve.</p>${["cache", "pair-state"].map(key => renderActionRow(OPS_BY_KEY[key])).join("")}<div class="sync-reset-all"><h3>Reset every pair</h3><p>This affects all sync pairs. For a problem with one pair, use the option above.</p>${renderActionRow(OPS_BY_KEY.state)}</div></section>
        <section class="maint-section" id="cxm-section-tracker" data-pane="tracker" aria-labelledby="cxm-archive-title" hidden>
          <div id="cxm-archive-panel" data-op="tracker" data-kind="tracker">
            <h2 id="cxm-archive-title">Clean up a CW Tracker instance</h2><p class="section-description">Choose the local Tracker you want to clean, then select what to clear.</p>
            <div class="tracker-instance-row"><label class="tracker-profile-control"><span>Tracker instance</span><select id="cxm-cw-profile" class="input" disabled><option value="">Loading instances…</option></select></label><button type="button" id="cxm-tracker-retry" hidden>Retry</button></div>
            <h3>What do you want to clear?</h3>
            <label class="tracker-cleanup-choice"><input type="checkbox" id="cxm-cw-state"><span><strong>Tracked lists and activity</strong><small>Empties this instance’s watchlist, history, ratings, progress and collection, and clears its change and review records.</small></span></label>
            <label class="tracker-cleanup-choice"><input type="checkbox" id="cxm-cw-snaps"><span><strong>Saved restore points</strong><small>Deletes this instance’s saved snapshots. You will no longer be able to restore from those copies.</small></span></label>
            <div class="tracker-cleanup-footer"><p id="cxm-cleanup-scope"></p><button type="button" class="primary" id="cxm-cw-review" disabled>Review cleanup</button></div>
            <div class="tracker-cleanup-review" id="cxm-cleanup-review" hidden><h3 id="cxm-cleanup-title"></h3><p id="cxm-cleanup-copy"></p><div class="task-panel-actions"><button type="button" id="cxm-cleanup-cancel">Cancel</button><button type="button" class="run-btn archive-clear" id="cxm-cw-clear" data-kind="tracker" data-label="CW Tracker cleanup" data-idle-label="Clear data">${icon("delete_sweep")}Clear data</button></div></div>
            <details class="tracker-backup"><summary>Backup &amp; import</summary><p>Save a copy or import an existing backup for this instance.</p>${OPS_BY_KEY.tracker.sideActions}</details>
            <div id="cxm-archive-status" class="status-message" role="status" hidden></div>
          </div>
        </section>
        <section class="maint-section" id="cxm-section-advanced" data-pane="advanced" aria-labelledby="cxm-advanced-title" hidden><h2 id="cxm-advanced-title">What do you want to do?</h2><p class="section-description">Each tool explains what it changes. Opening one does not run it.</p>${GROUPS.slice(2).map(renderCategory).join("")}
          <details class="storage-details" id="cxm-overview-status"><summary>Storage details</summary><div class="status-lines"><span id="cxm-tracker-count">Tracker status not loaded</span><span id="cxm-cache-count">Cache status not loaded</span></div><dl><dt>Tracker</dt><dd><code id="cxm-tracker-root"></code></dd><dt>Provider cache</dt><dd><code id="cxm-cache-root"></code></dd></dl></details>
        </section>
        <section class="maint-history maint-section" id="cxm-history" data-pane="history" aria-labelledby="cxm-history-title" hidden><div class="section-heading"><div><h2 id="cxm-history-title" tabindex="-1">Maintenance history</h2><p>Last 100 runs for this account in this browser.</p></div><button type="button" id="cxm-history-close" aria-label="Close maintenance history">${icon("close")}</button></div><ol id="cxm-history-list"></ol></section>
        <section id="cxm-task-panel" class="maint-task-panel" hidden><button type="button" class="task-back" id="cxm-task-back">${icon("arrow_back")}Back</button><div class="task-panel-body"><h2 id="cxm-task-title" tabindex="-1"></h2><p id="cxm-task-description"></p>
          <div id="cxm-pair-panel" hidden><div class="pair-reset-options"><label class="tracker-profile-control"><span>Sync pair</span><select id="cxm-pair-select" class="input" data-cw-native-select="true"><option value="">Loading sync pairs...</option></select></label><fieldset><legend>Reset for</legend><div class="pair-reset-features" id="cxm-pair-features"></div></fieldset><label class="tracker-archive-check"><input type="checkbox" id="cxm-pair-retry" checked><span>Also clear retry and blocked items for this pair</span></label></div></div>
          <p class="task-impact" id="cxm-task-note" hidden></p><div class="task-panel-actions"><button type="button" id="cxm-task-cancel">Cancel</button><button type="button" class="run-btn primary" id="cxm-run-action">Run</button><button type="button" class="run-btn primary" id="cxm-pair-reset" data-kind="pair-state" data-label="Reset one sync pair" data-idle-label="Reset pair" disabled hidden>${icon("restart_alt")}Reset pair</button></div>
          <div id="cxm-task-status" class="status-message" role="status" hidden></div>
          <details id="cxm-technical-details"><summary>Technical details</summary><section id="cxm-action-insight" class="action-insight" aria-live="polite" hidden><div class="insight-head"><span class="material-symbols-rounded insight-icon" aria-hidden="true"></span><h3 class="insight-title"></h3></div><div class="insight-metrics"></div><p class="insight-note"></p></section></details>
        </div></section>
      </div>`;
    activeRoot = root;
    const currentAuth = window.CW?.AuthState?.read?.();
    const auth = currentAuth?.status ? currentAuth : await window.CW?.AuthState?.refresh?.() || currentAuth;
    const storageKey = historyKey(auth);
    let entries = [];
    try { entries = cleanHistory(JSON.parse(localStorage.getItem(storageKey) || "[]")); } catch {}
    const results = new Map();
    for (const entry of entries) if (!results.has(OPS_BY_KEY[entry.key].kind)) results.set(OPS_BY_KEY[entry.key].kind, entry);
    const rows = [...root.querySelectorAll(".maint-task")];
    const archivePanel = $("#cxm-archive-panel", root);
    const pairPanel = $("#cxm-pair-panel", root);
    const taskPanel = $("#cxm-task-panel", root);
    let section = "quick", selectedTask = null, taskTrigger = null, historyReturn = "quick";
    let pairList = [];
    let pairRequestId = 0;
    const selectedPair = () => pairList.find(pair => pair.id === $("#cxm-pair-select", root).value);
    function saveResult(key, status, {jobId, at} = {}) {
      try { entries = cleanHistory(JSON.parse(localStorage.getItem(storageKey) || "[]")); } catch {}
      let previous = jobId && entries.find(entry => entry.jobId === jobId);
      if (jobId && !previous && Number.isFinite(at)) {
        const legacy = entries.filter(item => !item.jobId && !item.batch && item.key === key && item.status === status
          && item.at >= Math.floor(at) && item.at <= at + 5000);
        if (legacy.length === 1) previous = legacy[0];
      }
      const entry = {key, status, at:at ?? previous?.at ?? Date.now(), batch:false, ...(jobId ? {jobId} : {})};
      entries = cleanHistory([entry, ...entries.filter(item => item !== previous && (!jobId || item.jobId !== jobId))]);
      try { localStorage.setItem(storageKey, JSON.stringify(entries)); } catch {}
      const latest = entries.find(item => item.key === key);
      if (latest) results.set(OPS_BY_KEY[key].kind, latest);
      return entry;
    }
    function updateRows() {
      for (const row of rows) {
        const result = results.get(row.dataset.kind);
        const status = row.querySelector(".task-status");
        status.hidden = !result;
        status.dataset.status = result?.status || "idle";
        const label = ({success:"Completed",issues:"Issues found",error:"Failed"})[result?.status];
        status.textContent = result?.status === "running" ? "Running…" : label ? `Last run: ${label} · ${new Date(result.at).toLocaleString()}` : "";
        row.disabled = operationBusy;
      }
      const last = entries[0];
      $("#cxm-last-maintenance", root).textContent = last ? `Last maintenance: ${new Date(last.at).toLocaleString()} · ${last.status === "success" ? "Completed" : last.status === "issues" ? "Issues found" : "Failed"}` : "No maintenance runs recorded yet";
      $("#cxm-history-list", root).innerHTML = entries.length ? entries.map(entry => `<li><strong>${OPS_BY_KEY[entry.key].title}</strong><time datetime="${new Date(entry.at).toISOString()}">${new Date(entry.at).toLocaleString()}</time><span class="task-status" data-status="${entry.status}">${({success:"Completed",issues:"Issues found",error:"Failed"})[entry.status]}</span></li>`).join("") : '<li>No maintenance runs recorded yet.</li>';
    }
    function writeRoute() {
      if (!location.hash.startsWith("#maintenance")) return;
      const params = new URLSearchParams(location.hash.split("?")[1] || "");
      params.delete("group");
      if (section === "quick") params.delete("section"); else params.set("section", section);
      history.replaceState(history.state, "", "#maintenance" + (params.size ? `?${params}` : ""));
    }
    function setSection(next, route = true) {
      setStatus("");
      selectedTask = null;
      taskPanel.hidden = true;
      showOverviewStatus();
      section = next;
      root.querySelectorAll("[data-pane]").forEach(pane => { pane.hidden = pane.dataset.pane !== next; });
      root.querySelectorAll("[data-section]").forEach(button => {
        if (button.dataset.section === next) button.setAttribute("aria-current", "page");
        else button.removeAttribute("aria-current");
      });
      $("#cxm-history-toggle", root).setAttribute("aria-expanded", String(next === "history"));
      if (route) writeRoute();
      if (next === "tracker") void ensureTrackerProfiles();
    }
    function closeTask() {
      if (operationBusy) return;
      setSection(section, false);
      taskTrigger?.focus({preventScroll:true});
    }
    function openTask(key, trigger) {
      if (operationBusy) return;
      const op = OPS_BY_KEY[key];
      setStatus("");
      resetActionFeedback($("#cxm-pair-reset", root));
      selectedTask = op;
      taskTrigger = trigger;
      showOverviewStatus();
      root.querySelectorAll("[data-pane]").forEach(pane => { pane.hidden = true; });
      taskPanel.hidden = false;
      taskPanel.dataset.op = key;
      taskPanel.dataset.kind = op.kind;
      $("#cxm-task-title", root).textContent = op.title;
      $("#cxm-task-description", root).textContent = op.desc;
      $("#cxm-task-note", root).textContent = op.note || "";
      $("#cxm-task-note", root).hidden = !op.note;
      $("#cxm-task-status", root).hidden = true;
      $("#cxm-technical-details", root).open = false;
      pairPanel.hidden = key !== "pair-state";
      $("#cxm-pair-reset", root).hidden = key !== "pair-state";
      const run = $("#cxm-run-action", root);
      run.hidden = key === "pair-state";
      run.dataset.kind = op.kind;
      run.dataset.label = op.title;
      run.dataset.idleLabel = key.endsWith("health") ? "Run check" : ({meta:"Refresh metadata", cache:"Clear retry data", state:"Reset comparisons", playing:"Clear list", scrobbles:"Clear list", stats:"Rebuild reports", captures:"Delete captures", "state-file":"Compact data", "state-file-prune":"Prune records", "events-optimize":"Reclaim space", "events-purge":"Delete events", "events-rebuild":"Rebuild archive", "database-rebuild":"Replace database", defaults:"Reset CrossWatch"})[key] || "Continue";
      resetActionFeedback(run);
      if (key === "pair-state") void loadPairs();
      $("#cxm-task-title", root).focus({preventScroll:true});
    }
    function updatePairButton() {
      $("#cxm-pair-reset", root).disabled = operationBusy || !selectedPair() || !root.querySelector("#cxm-pair-features input:checked");
    }
    function renderPairFeatures() {
      const pair = selectedPair();
      $("#cxm-pair-features", root).innerHTML = (pair?.features || []).map(feature => `<label class="tracker-archive-check"><input type="checkbox" value="${escapeHtml(feature)}" checked><span>${escapeHtml(feature.charAt(0).toUpperCase() + feature.slice(1))}</span></label>`).join("");
      updatePairButton();
    }
    async function loadPairs() {
      const request = ++pairRequestId;
      const select = $("#cxm-pair-select", root);
      select.disabled = true;
      $("#cxm-pair-reset", root).disabled = true;
      select.innerHTML = '<option value="">Loading sync pairs…</option>';
      $("#cxm-pair-features", root).replaceChildren();
      try {
        const data = await fjson("/api/maintenance/support/scopes");
        if (request !== pairRequestId || !root.isConnected) return;
        if (data?.ok === false || !Array.isArray(data?.pairs)) throw new Error("Pair inventory unavailable");
        pairList = (Array.isArray(data?.pairs) ? data.pairs : []).filter(pair => pair?.id).map((pair, index) => ({...pair, id:String(pair.id), order:index + 1, label:`${index + 1} · ${pair.label || pair.id}${pair.enabled === false ? " (disabled)" : ""}`, features:Array.isArray(pair.features) ? pair.features.map(String) : []}));
        select.innerHTML = `<option value="">${pairList.length ? "Choose a pair" : "No sync pairs configured"}</option>` + pairList.map(pair => `<option value="${escapeHtml(pair.id)}">${escapeHtml(pair.label)}</option>`).join("");
      } catch {
        if (request !== pairRequestId) return;
        pairList = [];
        select.innerHTML = '<option value="">Could not load sync pairs. Reopen to retry.</option>';
      }
      select.disabled = operationBusy;
      window.CW?.IconSelect?.enhance(select, {
        className:"cw-plain-select cxm-pair-select", menuMinWidth:260,
        getOptionData:value => {
          const pair = pairList.find(pair => pair.id === value);
          return pair ? window.CW?.syncPairOptionData?.(pair, {tags:pair.enabled === false ? ["Disabled"] : []}) || null : null;
        },
      });
      renderPairFeatures();
    }
    function updateTrackerSelection() {
      $("#cxm-cleanup-review", root).hidden = true;
      const chosen = $("#cxm-cw-state", root).checked || $("#cxm-cw-snaps", root).checked;
      $("#cxm-cw-review", root).disabled = operationBusy || !chosen || !trackerProfilesLoaded;
      $("#cxm-cleanup-scope", root).textContent = `${chosen ? "Cleanup applies to" : "Choose what to clear in"} ${$("#cxm-cw-profile", root).selectedOptions[0]?.textContent || "this instance"}.`;
    }
    root.querySelectorAll("[data-section]").forEach(button => button.addEventListener("click", () => { if (!operationBusy) setSection(button.dataset.section); }));
    rows.forEach(row => row.addEventListener("click", () => openTask(row.dataset.op, row)));
    $("#cxm-task-back", root).addEventListener("click", closeTask);
    $("#cxm-task-cancel", root).addEventListener("click", closeTask);
    $("#cxm-run-action", root).addEventListener("click", event => { if (selectedTask) void runOp(selectedTask.kind, event.currentTarget); });
    $("#cxm-pair-reset", root).addEventListener("click", event => runOp("pair-state", event.currentTarget));
    $("#cxm-pair-select", root).addEventListener("change", renderPairFeatures);
    $("#cxm-pair-features", root).addEventListener("change", updatePairButton);
    for (const id of ["#cxm-cw-state", "#cxm-cw-snaps"]) $(id, root).addEventListener("change", updateTrackerSelection);
    $("#cxm-cw-review", root).addEventListener("click", () => {
      if (operationBusy || !trackerProfilesLoaded) return;
      const selected = [$("#cxm-cw-state", root).checked ? "tracked lists and activity" : null, $("#cxm-cw-snaps", root).checked ? "saved restore points" : null].filter(Boolean);
      if (!selected.length) return;
      $("#cxm-cleanup-title", root).textContent = `Clear ${selected.join(" and ")} from ${$("#cxm-cw-profile", root).selectedOptions[0].textContent}?`;
      $("#cxm-cleanup-copy", root).textContent = $("#cxm-cw-snaps", root).checked ? "The selected data will be removed, including saved restore points for this instance." : "The selected lists and activity records will be emptied. Saved restore points are kept.";
      $("#cxm-cleanup-review", root).hidden = false;
      $("#cxm-cleanup-review", root).scrollIntoView({block:"nearest"});
      $("#cxm-cleanup-cancel", root).focus({preventScroll:true});
    });
    $("#cxm-cleanup-cancel", root).addEventListener("click", () => { $("#cxm-cleanup-review", root).hidden = true; $("#cxm-cw-review", root).focus({preventScroll:true}); });
    $("#cxm-cw-clear", root).addEventListener("click", event => runOp("tracker", event.currentTarget));
    $("#cxm-history-toggle", root).addEventListener("click", () => {
      if (operationBusy) return;
      if (section === "history") setSection(historyReturn);
      else { historyReturn = section; setSection("history"); }
    });
    $("#cxm-history-close", root).addEventListener("click", () => { setSection(historyReturn); $("#cxm-history-toggle", root).focus({preventScroll:true}); });
    syncRoute = () => {
      if (operationBusy) return;
      const params = new URLSearchParams(location.hash.split("?")[1] || "");
      const legacy = {events:"state-file", captures:"playback"};
      const group = legacy[params.get("group")] || params.get("group");
      const next = group === "sync" ? "sync" : group === "archive" ? "tracker" : GROUPS.some(item => item.id === group) ? "advanced" : params.get("section");
      setSection([...SECTIONS.map(([id]) => id), "history"].includes(next) ? next : "quick", false);
      const target = [...root.querySelectorAll("[data-group]")].find(item => item.dataset.group === group);
      if (target) target.open = true;
      const back = pageBackLink(params.get("returnTo") || "#main", location.href, "maintenance");
      $("#cxm-back", root).href = back.href;
      $("#cxm-back", root).innerHTML = icon("arrow_back") + escapeHtml(back.label === "Maintenance" ? "Settings" : back.label);
      updateRows();
    };
    const statusEl = $("#cxm-status", root);
    const returnToCaller = () => { $("#cxm-back", root).click(); };
    let statusFadeTimer, statusHideTimer;
    const setStatus = (msg, kind = "") => {
      if (!statusEl) return;
      window.clearTimeout(statusFadeTimer);
      window.clearTimeout(statusHideTimer);
      statusEl.textContent = msg;
      statusEl.className = "status-message" + (kind ? " " + kind : "");
      statusEl.hidden = !msg || !taskPanel.hidden || section === "tracker";
      const panelStatuses = [[ $("#cxm-section-tracker", root), $("#cxm-archive-status", root)], [taskPanel, $("#cxm-task-status", root)]].map(([panel, el]) => {
        if (!panel.hidden) {
          el.textContent = msg;
          el.className = statusEl.className;
          el.hidden = !msg;
        }
        return el;
      });
      if (msg && kind === "ok") {
        statusFadeTimer = window.setTimeout(() => {
          statusEl.classList.add("is-fading");
          panelStatuses.forEach(el => el.classList.add("is-fading"));
          const duration = window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 700;
          statusHideTimer = window.setTimeout(() => {
            statusEl.hidden = true;
            panelStatuses.forEach(el => { el.hidden = true; });
          }, duration);
        }, 5000);
      }
    };
    const trackerProfile = () => String($("#cxm-cw-profile", root)?.value || "default").trim() || "default";
    const trackerProfileQuery = () => `provider_instance=${encodeURIComponent(trackerProfile())}`;
    let trackerProfilesLoaded = false;
    let trackerProfilesPromise = null;
    const loadTrackerProfiles = async () => {
      const sel = $("#cxm-cw-profile", root);
      if (!sel) return;
      const current = sel.value || "default";
      sel.disabled = true;
      $("#cxm-tracker-retry", root).hidden = true;
      try {
        const data = await fjson("/api/provider-instances/CROSSWATCH");
        if (!Array.isArray(data)) throw new Error("Invalid instance list");
        const list = data.filter(item => item?.id);
        sel.innerHTML = list.length ? list
          .map(p => `<option value="${escapeHtml(String(p?.id || "default"))}">${escapeHtml(String(p?.label || p?.id || "Default"))}</option>`)
          .join("") : '<option value="">No Tracker instances configured</option>';
        trackerProfilesLoaded = list.length > 0;
      } catch {
        trackerProfilesLoaded = false;
        sel.innerHTML = '<option value="">Could not load Tracker instances</option>';
        $("#cxm-tracker-retry", root).hidden = false;
      }
      const values = Array.from(sel.options).map(o => o.value);
      sel.value = values.includes(current) ? current : values[0] || "";
      sel.disabled = operationBusy || !trackerProfilesLoaded;
      window.CW?.ProfileSelect?.enhanceProfile?.(sel, {
        className: "cxm-tracker-profile-select",
        menuClassName: "cxm-tracker-profile-menu",
        menuMinWidth: 220,
      });
      updateTrackerSelection();
      for (const button of archivePanel.querySelectorAll(".archive-btn")) button.disabled = operationBusy || !trackerProfilesLoaded;
      if (trackerProfilesLoaded && trackerProfile() !== current && summaryRequestId > 0) void refreshSummary();
    };
    async function ensureTrackerProfiles() {
      if (trackerProfilesLoaded) return;
      if (!trackerProfilesPromise) trackerProfilesPromise = loadTrackerProfiles().finally(() => { trackerProfilesPromise = null; });
      await trackerProfilesPromise;
    }
    $("#cxm-tracker-retry", root).addEventListener("click", () => { void ensureTrackerProfiles(); });

    const downloadTrackerArchive = () => {
      if (operationBusy || !trackerProfilesLoaded) return;
      const a = document.createElement("a");
      a.href = `/api/maintenance/crosswatch-tracker/export?${trackerProfileQuery()}`;
      a.download = "crosswatch-tracker.zip";
      document.body.appendChild(a);
      a.click();
      a.remove();
      setStatus("Downloading tracker archive...", "ok");
    };

    const importTrackerArchive = async (file) => {
      if (!file || operationBusy || !trackerProfilesLoaded) return;
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
        void refreshSummary();
        if (selectedInsightKind === "tracker") void loadActionInsight("tracker");
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
      const controls = root.querySelectorAll(".run-btn, .archive-btn, .task-open, [data-section], #cxm-history-toggle, #cxm-history-close, #cxm-cw-profile, #cxm-cw-state, #cxm-cw-snaps, #cxm-cw-review, #cxm-cleanup-cancel, #cxm-tracker-retry, #cxm-task-back, #cxm-task-cancel, #cxm-pair-select, #cxm-pair-panel input");
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
      updatePairButton();
      if (!busy) {
        updateTrackerSelection();
        $("#cxm-cw-profile", root).disabled = !trackerProfilesLoaded;
        for (const button of archivePanel.querySelectorAll(".archive-btn")) button.disabled = !trackerProfilesLoaded;
      }
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
        return `Optimize database · freed ${formatBytes(res.reclaimed_bytes || 0)} · database ${formatBytes(res.database_bytes || 0)} · WAL ${formatBytes(res.wal_bytes || 0)} · ${res.duration_ms ?? 0} ms.${res.checkpoint_pending ? " Checkpoint pending: another connection is holding the WAL." : ""}`;
      }
      if (kind === "events-rebuild") {
        return `Rebuild archive · ${new Intl.NumberFormat().format(res.events || 0)} events · runtime data preserved.`;
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
      bits.push(`unused ${formatBytes(res.unused_bytes || 0)}`);
      if (res.pair_scopes) bits.push(`${res.pair_scopes.active_scopes} active pair scopes`, `${res.pair_scopes.scopes.length} obsolete scopes (${res.pair_scopes.items} items)`);
      if (res.pair_scopes?.deferred) bits.push(res.pair_scopes.reason === "config_unavailable" ? "Scope cleanup deferred: configuration unavailable" : `Scope cleanup deferred for ${res.pair_scopes.deferred_pairs.length} pair(s) with unresolved users`);
      if (res.table_storage?.length) bits.push(...res.table_storage.slice(0, 5).map(row => `${row.name}: ${formatBytes(row.bytes)}`));
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
      const orphans = Number(res.orphaned_state_files || 0);
      const bits = [`${providers} providers`, `${instances} instances`, `${baselines} baselines`, `${items} items`, `${orphans} orphaned files`, `${freed} reclaimed`];
      if (res.pair_scopes?.deferred) bits.push(`Preserved scopes for ${res.pair_scopes.deferred_pairs.length} pair(s) with unresolved users; unowned historical scopes were also preserved`);
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
      selectedInsightKind = kind;
      const requestId = ++insightRequestId;
      renderActionInsight(op, null, "loading");
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

    $("#cxm-technical-details", root).addEventListener("toggle", () => {
      if ($("#cxm-technical-details", root).open && selectedTask) void loadActionInsight(selectedTask.kind);
      else showOverviewStatus();
    });

    let summaryRequestId = 0;
    async function refreshSummary() {
      const request = ++summaryRequestId;
      try {
        const [tracker, cache] = await Promise.all([
          fjson(`/api/maintenance/crosswatch-tracker?${trackerProfileQuery()}`).catch(() => null),
          fjson("/api/maintenance/provider-cache").catch(() => null),
        ]);

        if (request !== summaryRequestId || !root.isConnected) return;
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
      return btn.closest("[data-op]") || (btn.dataset.kind ? root.querySelector(`[data-op][data-kind="${btn.dataset.kind}"]`) : null);
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

    function finishActionFeedback(btn, result, jobId) {
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

      if (row) { saveResult(row.dataset.op, result, {jobId}); updateRows(); }
      const ok = result === "success";
      row?.classList.add(ok ? "run-success" : "run-error");
      btn.classList.add(ok ? "result-success" : "result-error");
      btn.textContent = ok ? "Done" : result === "issues" ? "Issues" : "Failed";
      btn._cwResultTimer = window.setTimeout(() => resetActionFeedback(btn), 1400);
    }

    function recordJobResult(job) {
      const op = OPS.find(item => item.kind === job.action);
      if (!op || typeof job.id !== "string" || !job.id || !["completed", "failed", "interrupted"].includes(job.state)) return null;
      const status = job.state !== "completed" || job.result?.ok !== true ? "error" : job.result?.healthy === false ? "issues" : "success";
      const ended = Number(job.started_at) * 1000 + Math.max(0, Number(job.duration_ms) || 0);
      const at = Number.isFinite(ended) && ended > 0 && Number.isFinite(new Date(ended).getTime()) ? ended : undefined;
      const entry = saveResult(op.key, status, {jobId:job.id, at});
      updateRows();
      return entry;
    }

    async function followJob(initial) {
      let job = initial;
      while (job.state === "queued" || job.state === "running") {
        setStatus(`Running ${job.action}: ${job.step || job.state}…`, "busy");
        await new Promise(resolve => window.setTimeout(resolve, 1000));
        const next = await fjson("/api/maintenance/jobs/current");
        if (next.id !== initial.id) throw new Error("Maintenance status changed. Check the latest job before retrying.");
        job = next;
      }
      recordJobResult(job);
      window.dispatchEvent?.(new CustomEvent("cw:maintenance-job-changed"));
      return job.result || { ok: false, error: job.state || "missing_job_result" };
    }

    async function resumeJob() {
      setOperationBusy(true);
      try {
        const job = await fjson("/api/maintenance/jobs/current");
        if (!job.id || job.state === "idle") return;
        if (!["queued", "running"].includes(job.state)) {
          const entry = recordJobResult(job);
          if (entry && entry.status !== "success") {
            const outcome = job.state === "interrupted" ? "was interrupted" : entry.status === "issues" ? "found issues" : "failed";
            setStatus(`Previous maintenance ${outcome}: ${OPS_BY_KEY[entry.key].title} · ${new Date(entry.at).toLocaleString()}${job.result?.error ? ` (${job.result.error})` : ""}`, "err");
          }
          return;
        }
        const result = await followJob(job);
        setStatus(result.ok ? (eventsReceipt(job.action, result) || databaseReceipt(job.action, result) || "Maintenance completed.") : `Maintenance stopped: ${result.error}`, result.ok && result.healthy !== false ? "ok" : "err");
      } catch (error) {
        setStatus(`Cannot retrieve maintenance status: ${error.message}`, "err");
      } finally {
        setOperationBusy(false);
      }
    }

    async function runOp(kind, btn) {
      if (operationBusy) return false;

      if (kind === "tracker" && (!trackerProfilesLoaded || $("#cxm-cleanup-review", root).hidden)) return false;
      let jobId;
      setOperationBusy(true);
      try {
        if (kind === "state" && !(await (window.CW?.MobileUI?.ask || window.confirm)("Reset all sync baselines, including obsolete pair scopes?\n\nThe next sync starts fresh. Previous deletions will not propagate, and two-way pairs can add missing items back. Create a backup first."))) {
          setStatus("Cancelled.", "");
          return false;
        }

        if (kind === "captures" && !(await (window.CW?.MobileUI?.ask || window.confirm)("Delete all saved captures? This cannot be undone."))) {
          setStatus("Cancelled.", "");
          return false;
        }

        if (kind === "events-purge" && !(await (window.CW?.MobileUI?.ask || window.confirm)("Clear all event data?\n\nThis empties every event category (sync, scrobble, audits) and the sync activity calendar.\n\nNothing else in the local database is touched. This cannot be undone."))) {
          setStatus("Cancelled.", "");
          return false;
        }

        if (kind === "events-rebuild" && !(await (window.CW?.MobileUI?.ask || window.confirm)("Clear the event archive and start it empty?\n\nHistorical events will be removed. Runtime baselines, configuration and other database tables are preserved."))) {
          setStatus("Cancelled.", "");
          return false;
        }

        if (kind === "state-file" && !(await (window.CW?.MobileUI?.ask || window.confirm)("Create an app-state backup and compact the sync state database?"))) {
          setStatus("Cancelled.", "");
          return false;
        }

        if (kind === "state-file-prune" && !(await (window.CW?.MobileUI?.ask || window.confirm)("Create an app-state backup and prune stale sync state?\n\nThis removes provider or instance baselines that are no longer referenced by configured sync pairs or scrobbler routes, plus unresolved, blackbox and flap files left behind by removed pairs."))) {
          setStatus("Cancelled.", "");
          return false;
        }

        if (kind === "database-rebuild" && !(await (window.CW?.MobileUI?.ask || window.confirm)("Rebuild the local database?\n\nThe current database files are moved to .cw_databases/backups and a fresh database is created.\n\nSync baselines, Editor mappings and blocks, Events, statistics, sync reports and hidden watchlist items start empty. config.json and provider logins stay.\n\nThe next sync works like a first sync: nothing is deleted, but a two-way pair can add back items removed on one side."))) {
          setStatus("Cancelled.", "");
          return false;
        }

        startActionFeedback(btn);
        const label = btn?.dataset?.label || OPS.find((item) => item.kind === kind)?.title || kind;
        setStatus(`Running ${label.toLowerCase()}...`, "busy");
        let res = null;
        let trackerStateChanged = false;
        if (SIMPLE_OPS[kind]) {
          const body = kind === "stats" ? {
            recalc: false,
            purge_file: true,
            purge_state: false,
            purge_reports: true,
            purge_insights: true,
          } : (kind === "events-rebuild" || kind === "events-purge" || kind === "database-rebuild" || kind === "state") ? { confirm: true } : undefined;
          if (JOB_ACTIONS.has(kind)) {
            const job = await post("/api/maintenance/jobs", { action: kind, ...(body || {}) });
            jobId = job.id;
            window.dispatchEvent?.(new CustomEvent("cw:maintenance-job-changed"));
            res = job.ok === false ? job : await followJob(job);
          } else {
            res = await post(SIMPLE_OPS[kind], body);
          }
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
        } else if (kind === "pair-state") {
          const pair = selectedPair();
          const features = [...root.querySelectorAll("#cxm-pair-features input:checked")].map(input => input.value);
          if (!pair || !features.length) {
            setStatus("Select a sync pair and at least one feature.", "err");
            finishActionFeedback(btn, "error");
            return false;
          }
          if (!(await (window.CW?.MobileUI?.ask || window.confirm)(`Reset sync state for pair ${pair.label}?\n\nFeatures: ${features.join(", ")}\n\nThe next sync reads fresh provider data for this pair. Deletions made since the last sync are not propagated, and a two-way pair adds items back that are missing on one side.\n\nOther pairs keep their state.`))) {
            setStatus("Cancelled.", "");
            finishActionFeedback(btn, "cancel");
            return false;
          }
          res = await post("/api/maintenance/clear-pair-state", {
            pair_id: pair.id,
            features,
            clear_retry: !!$("#cxm-pair-retry", root)?.checked,
          });
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

          if (!(await (window.CW?.MobileUI?.ask || window.confirm)(warn))) {
            setStatus("Cancelled.", "");
            finishActionFeedback(btn, "cancel");
            return false;
          }

          {
            const typed = (await (window.CW?.MobileUI?.prompt || window.prompt)("Type RESET to continue"));
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
          finishActionFeedback(btn, "error", jobId);
          return false;
        }

        if (kind === "scrobbles") {
          try { window.dispatchEvent(new CustomEvent("activity-log-cleared")); } catch {}
        }
        if (kind === "state" || kind === "database-rebuild") {
          try { window.CW?.Maintenance?.applySyncStateReset?.(res || { ok: true }); } catch {}
          void refreshSummary();
        }
        if (kind === "tracker" && trackerStateChanged) {
          notifyTrackerStateChanged(res || { ok: true });
        }
        if (kind === "cache" || kind === "tracker") void refreshSummary();

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

        if (selectedInsightKind === kind) void loadActionInsight(kind);
        const evReceipt = eventsReceipt(kind, res);
        const dbReceipt = databaseReceipt(kind, res);
        const stateReceipt = kind === "state-file" ? stateFileReceipt(res) : null;
        const statePrune = kind === "state-file-prune" ? statePruneReceipt(res) : null;
        const rebuildReceipt = kind === "database-rebuild" ? `Rebuild local database completed · ${res?.backup ? `backup ${res.backup}` : "no previous files"}.` : null;
        setStatus(evReceipt || dbReceipt || stateReceipt || statePrune || rebuildReceipt || completionReceipt(label, res), res?.healthy === false ? "err" : "ok");
        finishActionFeedback(btn, res?.healthy === false ? "issues" : "success", jobId);
        return res || { ok: true };
      } catch (e) {
        setStatus(`Error: ${e.message || String(e)}`, "err");
        finishActionFeedback(btn, "error", jobId);
        return false;
      } finally {
        setOperationBusy(false);
      }
    }

    root.querySelector("#cxm-cw-export")?.addEventListener("click", downloadTrackerArchive);
    root.querySelector("#cxm-cw-profile")?.addEventListener("change", async () => {
      $("#cxm-cw-state", root).checked = false;
      $("#cxm-cw-snaps", root).checked = false;
      setStatus("");
      updateTrackerSelection();
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

    syncRoute();
    showOverviewStatus();
    const loadMaintenanceBootStatus = async () => {
      await resumeJob();
      if (section === "tracker") await ensureTrackerProfiles();
      await refreshSummary();
    };
    void loadMaintenanceBootStatus();
  },
};
window.MaintenancePage = MaintenancePage;
export default MaintenancePage;
