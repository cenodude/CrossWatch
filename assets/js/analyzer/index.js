/* assets/js/analyzer/index.js */
/* CrossWatch - Sync Analyzer Page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const FEATURES = ["history", "watchlist", "ratings", "progress", "collection"];
const VIEWS = {
  missing: { title: "Missing items", icon: "compare_arrows", description: "Items present in one saved snapshot and missing from a destination. These are findings, not proposed sync changes." },
  pending: { title: "Pending retries", icon: "pending_actions", description: "Items a previous sync could not match or write. Review the reason before running the pair again." },
  system: { title: "System health", icon: "monitor_heart", description: "Provider, orchestrator and saved-state diagnostics across CrossWatch, plus findings for the selected pairs." },
  blocked: { title: "Blocked items", icon: "block", description: "Items held back by saved blocking rules or automatic protections." },
  all: { title: "All items", icon: "dataset", description: "Browse saved items for the selected pairs. Rating and progress values may differ even when an item exists on both sides." }
};
const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const array = value => Array.isArray(value) ? value : [];
const human = value => String(value || "").replace(/[_-]+/g, " ").replace(/^./, c => c.toUpperCase());
const severity = row => row.severity === "error" ? "error" : ["warn", "warning"].includes(row.severity) ? "warn" : "info";
const identity = row => `${row.provider || ""}::${row.feature || ""}::${row.key || ""}`;
const label = row => {
  const item = row.item || row;
  const title = row.title || row.item_title || item.title || row.series_title || item.series_title || row.message || row.key || array(row.keys)[0] || "Untitled item";
  const season = row.season ?? item.season;
  const episode = row.episode ?? item.episode;
  if ((row.item_type || item.type) === "episode" && season != null && episode != null) return `${row.series_title || item.series_title || title} · S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`;
  return title;
};
const reasonText = hint => hint?.message || [hint?.provider, array(hint?.reasons).join(", ") || hint?.reason || hint?.kind].filter(Boolean).join(": ");
const unique = rows => [...new Map(rows.map(row => [JSON.stringify(row), row])).values()];
const affectedLabels = row => [
  ...array(row.affected_items).map(value => [value.label || value.key, value.reason, value.attempts != null && `Attempts: ${value.attempts}`].filter(Boolean).join(" · ")),
  ...array(row.extra_source_titles || row.extra_source).map(value => `Only at source: ${value}`),
  ...array(row.extra_target_titles || row.extra_target).map(value => `Only at destination: ${value}`)
];
export function createResultFilter(providerName = String) {
  const cache = new WeakMap();
  const describe = row => {
    let entry = cache.get(row);
    if (!entry) {
      const ids = Object.entries(row.ids || row.item?.ids || {}).map(([key, value]) => `${key}:${value}`);
      const title = String(label(row)).toLowerCase();
      entry = {
        text: [title, row.provider, providerName(row.provider), row.feature, row.key, row.year, row.message, row.reason, row.error, row.type, row.path, row.module, ...ids].join(" ").toLowerCase(),
        title, provider: String(row.provider || "").toLowerCase(), feature: String(row.feature || "").toLowerCase()
      };
      cache.set(row, entry);
    }
    return entry;
  };
  return (rows, { query = "", feature = "", level = "", sort = "title", diagnostics = false } = {}) => {
    const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
    const filtered = rows.filter(row => (!feature || row.feature === feature) && (!level || severity(row) === level) && terms.every(term => describe(row).text.includes(term)));
    const key = sort.startsWith("title") ? "title" : sort;
    const rank = { error: 0, warn: 1, info: 2 };
    return filtered.sort((a, b) => {
      const delta = diagnostics ? rank[severity(a)] - rank[severity(b)] : 0;
      if (delta) return delta;
      const left = describe(a)[key] || "", right = describe(b)[key] || "";
      return (left < right ? -1 : left > right ? 1 : 0) * (sort === "title-desc" ? -1 : 1);
    });
  };
}
const requestJSON = async (url, signal) => {
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  const timer = setTimeout(abort, 120000);
  try {
    const response = await fetch(url, { signal: controller.signal, cache: "no-store" });
    if (!response.ok) throw new Error(`Request failed (${response.status}).`);
    return await response.json();
  } catch (error) {
    if (controller.signal.aborted && !signal.aborted) throw new Error("The request timed out. Please try again.");
    throw error;
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", abort);
  }
};
let cssReady;
function injectAnalyzerCSS() {
  if (cssReady) return cssReady;
  if (document.getElementById("cw-analyzer-page-css")) return Promise.resolve();
  const link = document.createElement("link");
  const url = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) url.searchParams.set("v", version);
  link.id = "cw-analyzer-page-css";
  link.rel = "stylesheet";
  link.href = url.href;
  return cssReady = new Promise(resolve => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
    document.head.appendChild(link);
  });
}
let activeCleanup;
let mountGeneration = 0;
const Analyzer = {
  async mount(host) {
    activeCleanup?.();
    const generation = ++mountGeneration;
    const root = document.createElement("div");
    root.className = "an-page";
    host.replaceChildren(root);
    await injectAnalyzerCSS();
    if (generation !== mountGeneration || host.classList.contains("hidden")) return;
    root.innerHTML = `
      <a class="an-back" href="#main">${icon("arrow_back")}Main</a>
      <header class="an-header">
        <div><div class="an-eyebrow">SYNC INSIGHTS</div><h1>Sync Analyzer</h1><p>Understand missing items, failed attempts and the health of your sync setup.</p></div>
        <button class="an-button an-primary" id="an-run" type="button">${icon("refresh")}<span>Analyze</span></button>
      </header>
      <div id="an-progress" class="an-progress" role="status" aria-live="polite" hidden></div>
      <div id="an-alerts"></div>
      <section class="an-scope-bar" aria-label="Analysis scope">
        <label for="an-pair">Sync pair<select id="an-pair" disabled><option>Loading pairs…</option></select></label>
        <div class="an-snapshot">${icon("schedule")}<div><strong>Saved snapshots</strong><span id="an-snapshot-time">Run a sync to update provider data.</span></div></div>
      </section>
      <details id="an-scope-notes" class="an-scope-notes" hidden></details>
      <nav class="an-tabs" aria-label="Analyzer views">${Object.entries(VIEWS).map(([key, view]) => `<button type="button" class="an-tab" data-view="${key}" aria-pressed="${key === "missing"}">${icon(view.icon)}<span>${view.title}</span><span class="an-count" data-count="${key}">—</span></button>`).join("")}</nav>
      <section class="an-results" aria-labelledby="an-view-title">
        <div class="an-results-heading"><div><h2 id="an-view-title"></h2><p id="an-view-description"></p></div><span id="an-checked" class="an-muted"></span></div>
        <div class="an-toolbar">
          <label class="an-search">${icon("search")}<input id="an-search" type="search" maxlength="300" aria-label="Search Analyzer results" placeholder="Search titles, IDs, providers or reasons"></label>
          <label class="an-filter">Feature<select id="an-feature"><option value="">All features</option>${FEATURES.map(f => `<option value="${f}">${human(f)}</option>`).join("")}</select></label>
          <label class="an-filter" id="an-severity-wrap" hidden>Severity<select id="an-severity"><option value="">All severities</option><option value="error">Errors</option><option value="warn">Warnings</option><option value="info">Information</option></select></label>
          <label class="an-filter">Sort<select id="an-sort"><option value="title">Title A–Z</option><option value="title-desc">Title Z–A</option><option value="provider">Provider</option><option value="feature">Feature</option></select></label>
          <button type="button" class="an-button an-clear" id="an-clear" hidden>Clear filters</button>
        </div>
        <div class="an-list-meta"><span id="an-result-count" role="status" aria-live="polite"></span><label>Per page<select id="an-page-size" aria-label="Items per page"><option>25</option><option selected>50</option><option>100</option><option>250</option></select></label></div>
        <div id="an-workspace" class="an-workspace">
          <div class="an-list-column"><div id="an-list"></div><nav id="an-pagination" class="an-pagination" aria-label="Result pages"></nav></div>
          <aside id="an-detail" class="an-detail" aria-label="Item details" hidden></aside>
        </div>
      </section>`;
    const $ = selector => root.querySelector(selector);
    const lifetime = new AbortController();
    let analysisController, systemController, pageController, detailController, searchTimer;
    let pairs = [], problems = [], pending = [], system = [], exclusions = [];
    let PROFILE_LABELS = {}, activity = {}, status = {};
    let view = "missing", analysisState = "loading", systemState = "loading";
    let analysisError = "", systemError = "", checked = "", pageError = "";
    let analysisStarted = Date.now(), systemStarted = Date.now();
    let allTotal = null, selected = null;
    let total = 0, offset = 0, pageSize = 50;
    let currentRows = [], filteredRows = [];
    let pageLoading = false;
    let missingIndex = new Map(), blockedIndex = new Map();
    const detailCache = new Map();
    const cleanup = () => {
      lifetime.abort();
      analysisController?.abort();
      systemController?.abort();
      pageController?.abort();
      detailController?.abort();
      clearTimeout(searchTimer);
      clearInterval(progressTimer);
      root.remove();
      if (activeCleanup === cleanup) activeCleanup = null;
    };
    activeCleanup = cleanup;
    const scoped = path => {
      const url = new URL(path, location.origin);
      const ids = $("#an-pair").value ? [$("#an-pair").value] : pairs.map(p => p.id);
      if (ids.length) url.searchParams.set("pairs", ids.join(","));
      return url.pathname + url.search;
    };
    function profileLabel(provider, instance) {
      const inst = String(instance || "").trim();
      if (!inst || inst.toLowerCase() === "default") return "";
      const prov = String(provider || "").toUpperCase();
      return (PROFILE_LABELS[prov] && PROFILE_LABELS[prov][inst]) || inst;
    }
    const providerName = token => {
      const [provider, instance] = String(token || "").split("@");
      const name = window.CWProviderMeta?.label?.(provider) || provider;
      const profile = profileLabel(provider, instance);
      return profile ? `${name} · ${profile}` : name;
    };
    const pairName = pair => {
      const twoWay = ["two-way", "bi", "both", "mirror", "two", "two_way", "two way"].includes(String(pair.mode).toLowerCase());
      const endpoint = (provider, instance) => providerName(`${provider}${instance ? `@${instance}` : ""}`);
      return `${endpoint(pair.source, pair.source_instance)} ${twoWay ? "↔" : "→"} ${endpoint(pair.target, pair.target_instance)}`;
    };
    const filterResults = createResultFilter(providerName);
    let groups = { missing: [], blocked: [], system: [], errors: 0, warnings: 0 };
    function indexResultGroups() {
      const systemFindings = unique([...problems.filter(p => !["missing_peer", "blocked_manual", "cw_state_blackbox_active", "cw_state_unresolved_backlog"].includes(p.type)), ...system]);
      groups = {
        missing: [...missingIndex.values()],
        blocked: problems.filter(p => ["blocked_manual", "cw_state_blackbox_active"].includes(p.type)),
        system: systemFindings,
        errors: systemFindings.filter(p => severity(p) === "error").length,
        warnings: systemFindings.filter(p => severity(p) === "warn").length
      };
    }
    const sourceRows = () => view === "pending" ? pending : groups[view] || [];
    const isDiagnostic = () => ["system", "blocked"].includes(view);
    function updateProgress() {
      const tasks = [];
      const elapsed = started => `${Math.floor((Date.now() - started) / 60000)}m ${Math.floor((Date.now() - started) / 1000) % 60}s`;
      if (analysisState === "loading") tasks.push(`Checking saved items · ${elapsed(analysisStarted)}`);
      if (systemState === "loading") tasks.push(`Checking system health · ${elapsed(systemStarted)}`);
      $("#an-progress").hidden = !tasks.length;
      $("#an-progress").innerHTML = `${icon("progress_activity")}<span>${tasks.map(esc).join('<span class="an-progress-sep">/</span>')}</span>`;
      $("#an-run").disabled = !!tasks.length;
      $("#an-run span:last-child").textContent = tasks.length ? "Analyzing…" : "Analyze";
    }
    const progressTimer = setInterval(updateProgress, 1000);
    function renderStatus() {
      const findings = groups.system;
      const { errors, warnings } = groups;
      const counts = { missing: analysisState === "ready" ? missingIndex.size : null, pending: analysisState === "ready" ? pending.length : null, blocked: analysisState === "ready" ? groups.blocked.length : null, system: ["ready", "restricted"].includes(systemState) ? findings.length : null, all: allTotal };
      for (const [key, count] of Object.entries(counts)) {
        $(`[data-count="${key}"]`).textContent = count == null ? "—" : count.toLocaleString();
        $(`[data-view="${key}"]`).setAttribute("aria-pressed", String(view === key));
      }
      $('[data-view="system"]').classList.toggle("has-errors", errors > 0 || systemState === "error");
      const alerts = [];
      if (analysisState === "error") alerts.push(`<div class="an-notice an-error" role="alert">${icon("error")}<div><strong>Item analysis could not finish</strong><p>${esc(analysisError)}</p></div><button type="button" class="an-button" data-retry="analysis">Retry</button></div>`);
      if (systemState === "error") alerts.push(`<div class="an-notice an-error" role="alert">${icon("error")}<div><strong>System health could not be checked</strong><p>${esc(systemError)} The result is unknown.</p></div><button type="button" class="an-button" data-retry="system">Retry</button></div>`);
      if (errors || warnings) alerts.push(`<div class="an-notice ${errors ? "an-error" : "an-warning"}">${icon(errors ? "error" : "warning")}<div><strong>${errors ? `${errors} system failure${errors === 1 ? "" : "s"}` : `${warnings} system warning${warnings === 1 ? "" : "s"}`} need${(errors || warnings) === 1 ? "s" : ""} attention</strong><p>${errors && warnings ? `${warnings} warning${warnings === 1 ? "" : "s"} also found. ` : ""}Review provider and saved-state diagnostics.</p></div><button type="button" class="an-button" data-open-system>View findings</button></div>`);
      $("#an-alerts").innerHTML = alerts.join("");
      $("#an-checked").textContent = checked ? `Analyzed ${checked}` : "";
      updateProgress();
    }
    function updateSnapshot() {
      const ids = $("#an-pair").value ? [$("#an-pair").value] : pairs.map(p => p.id);
      const times = ids.map(id => Number(activity[id] || 0) / 1000000).filter(Boolean);
      $("#an-snapshot-time").textContent = times.length ? `Last sync ${new Date(Math.max(...times)).toLocaleString()}` : "No sync time available. Run a sync to update provider data.";
    }
    function renderView() {
      $("#an-view-title").textContent = VIEWS[view].title;
      $("#an-view-description").textContent = VIEWS[view].description;
      $("#an-severity-wrap").hidden = view !== "system";
      $("#an-workspace").classList.toggle("an-diagnostics", isDiagnostic());
      $("#an-clear").hidden = !($("#an-search").value || $("#an-feature").value || $("#an-severity").value);
      const excluded = exclusions.reduce((sum, row) => sum + Number(row.excluded_total || 0), 0);
      $("#an-scope-notes").hidden = !excluded;
      $("#an-scope-notes").innerHTML = `<summary>${excluded.toLocaleString()} items excluded by the pair?s sync rules</summary>${exclusions.map(row => `<p>${esc(providerName(row.source))} ? ${esc(providerName(row.target))} ? ${esc(human(row.feature))}: ${Number(row.excluded_total || 0).toLocaleString()} excluded${row.excluded_types ? ` ? Types: ${esc(Object.keys(row.excluded_types).join(", "))}` : ""}${row.excluded_libraries ? ` ? Libraries: ${esc(Object.keys(row.excluded_libraries).join(", "))}` : ""}</p>`).join("")}`;
      renderStatus();
    }
    function emptyState(title, message, symbol = "search_off") {
      return `<div class="an-empty">${icon(symbol)}<h3>${esc(title)}</h3><p>${esc(message)}</p></div>`;
    }
    function pagination() {
      const pages = Math.max(1, Math.ceil(total / pageSize));
      const current = Math.floor(offset / pageSize) + 1;
      $("#an-result-count").textContent = pageLoading ? "Loading results…" : total ? `${(offset + 1).toLocaleString()}–${Math.min(offset + currentRows.length, total).toLocaleString()} of ${total.toLocaleString()} ${isDiagnostic() ? (total === 1 ? "finding" : "findings") : (total === 1 ? "item" : "items")}` : "0 results";
      $("#an-pagination").innerHTML = `<button type="button" class="an-button" data-page="0" ${pageLoading || current === 1 ? "disabled" : ""}>${icon("first_page")}<span>First</span></button><button type="button" class="an-button" data-page="${offset - pageSize}" ${pageLoading || current === 1 ? "disabled" : ""}>${icon("chevron_left")}<span>Previous</span></button><label>Page <input type="number" id="an-page-number" aria-label="Page number" min="1" max="${pages}" value="${current}" ${pageLoading ? "disabled" : ""}> of ${pages.toLocaleString()}</label><button type="button" class="an-button" data-page="${offset + pageSize}" ${pageLoading || current >= pages ? "disabled" : ""}><span>Next</span>${icon("chevron_right")}</button><button type="button" class="an-button" data-page="${(pages - 1) * pageSize}" ${pageLoading || current >= pages ? "disabled" : ""}><span>Last</span>${icon("last_page")}</button>`;
    }
    function renderList() {
      renderView();
      pagination();
      $("#an-detail").hidden = isDiagnostic() || !selected || !currentRows.length;
      $("#an-workspace").classList.toggle("has-detail", !$("#an-detail").hidden);
      if (pageLoading) {
        $("#an-list").innerHTML = emptyState("Loading items", "Reading the selected page of saved data…", "progress_activity");
        return;
      }
      if (pageError) {
        $("#an-list").innerHTML = emptyState("Could not load results", pageError, "error") + '<button class="an-button an-list-retry" type="button" data-retry="page">Retry this page</button>';
        return;
      }
      if (!currentRows.length) {
        let title = "No matching results", message = "Try another search or clear the filters.", symbol = "search_off";
        if (!$("#an-search").value && !$("#an-feature").value && !$("#an-severity").value) {
          const loading = view === "system" ? systemState === "loading" : analysisState === "loading";
          const failed = view === "system" ? systemState === "error" : analysisState === "error";
          title = loading ? "Analysis in progress" : failed ? "Results unavailable" : ({ missing: "No missing items found", pending: "No pending retries", blocked: "No blocked items", system: "No system findings", all: "No saved items" })[view];
          message = loading ? "Results will appear here as the checks finish." : failed ? "Retry the failed check using the message above." : view === "system" && systemState === "restricted" ? "Global system diagnostics are available to administrators. Pair findings remain visible here." : view === "missing" ? "No presence differences were found in the saved snapshots for these pairs." : "There is nothing to review in this view for the current scope.";
          symbol = loading ? "progress_activity" : failed ? "error" : "check_circle";
          if (!loading && !failed && view !== "system" && !pairs.length) {
            title = "No active sync pairs";
            message = "Create or enable a pair in Synchronization, then run it to create saved snapshots.";
            symbol = "compare_arrows";
          }
        }
        $("#an-list").innerHTML = emptyState(title, message, symbol);
        return;
      }
      if (isDiagnostic()) {
        $("#an-list").innerHTML = `<div class="an-findings">${currentRows.map((row, index) => renderFinding(row, index)).join("")}</div>`;
        return;
      }
      $("#an-list").innerHTML = `<div class="an-table-scroll"><table class="an-table"><thead><tr><th scope="col">Item</th><th scope="col">Provider</th><th scope="col">Feature</th><th scope="col">Finding</th></tr></thead><tbody>${currentRows.map((row, index) => {
        const mismatch = missingIndex.get(identity(row));
        const finding = view === "pending" ? "Pending retry" : row.type === "missing_peer" || mismatch ? `Missing at ${array((mismatch || row).targets).map(providerName).join(", ") || "destination"}` : blockedIndex.has(identity(row)) ? "Blocked" : "No presence issue";
        return `<tr class="${selected === row ? "is-selected" : ""}"><td><button type="button" class="an-item-button" data-row="${index}" aria-pressed="${selected === row}"><strong>${esc(label(row))}</strong><span>${esc([human(row.item_type || row.item?.type || (row.type === "missing_peer" ? "" : row.type)), row.year || row.item?.year].filter(Boolean).join(" · "))}</span></button></td><td>${esc(providerName(row.provider))}</td><td><span class="an-feature-badge">${esc(human(row.feature))}</span></td><td><span class="an-result-badge ${mismatch || row.type === "missing_peer" ? "an-warn-text" : ""}">${esc(finding)}</span></td></tr>`;
      }).join("")}</tbody></table></div>`;
    }
    function renderFinding(row, index) {
      const sev = severity(row);
      const title = row.title || row.message || human(row.type) || "Diagnostic finding";
      const context = [providerName(row.provider || row.source), row.feature && human(row.feature), row.category && human(row.category)].filter(Boolean).join(" · ");
      const detail = [row.message && row.message !== title ? row.message : "", row.item_title, row.key && `Key: ${row.key}`, row.module && `Module: ${row.module}`, row.path && `Path: ${row.path}`, row.error, row.reason, row.count != null && `Items affected: ${row.count}`, row.source && row.target && `${row.source} → ${row.target}`, row.show_delta && `Shows: ${row.show_delta.source} at source / ${row.show_delta.target} at destination`, row.show_gap && `Source-only shows: ${row.show_gap.source_only}; destination-only shows: ${row.show_gap.target_only}`, row.missing?.length && `Missing IDs: ${row.missing.join(", ")}`, row.id_name && `ID field: ${row.id_name} (${row.id_value ?? ""})`, row.watermark_key && `Watermark: ${row.watermark_key}`, row.value != null && `Value: ${row.value}`].filter(Boolean);
      const items = affectedLabels(row);
      return `<article class="an-finding an-severity-${sev}"><div class="an-finding-icon">${icon(sev === "error" ? "error" : sev === "warn" ? "warning" : "info")}</div><div class="an-finding-content"><div class="an-finding-top"><span class="an-severity-badge">${sev === "error" ? "Error" : sev === "warn" ? "Warning" : "Information"}</span><span>${esc(context)}</span></div><h3>${esc(title)}</h3>${detail.map(value => `<p>${esc(value)}</p>`).join("")}${items.length ? `<details class="an-affected"><summary>Affected items (${items.length.toLocaleString()})</summary><ul>${items.slice(0, 25).map(value => `<li>${esc(value)}</li>`).join("")}</ul>${items.length > 25 ? `<button type="button" class="an-button" data-more="${index}" data-shown="25">Show more</button>` : ""}</details>` : ""}<span class="an-finding-code">${esc(row.type)}</span></div></article>`;
    }
    function filterRows() {
      return filterResults(sourceRows(), {
        query: $("#an-search").value,
        feature: $("#an-feature").value,
        level: view === "system" ? $("#an-severity").value : "",
        sort: $("#an-sort").value,
        diagnostics: view === "system"
      });
    }
    function showLocalPage(start = 0) {
      total = filteredRows.length;
      offset = Math.max(0, Math.min(start, Math.max(0, Math.ceil(total / pageSize) - 1) * pageSize));
      currentRows = filteredRows.slice(offset, offset + pageSize);
      selectVisible();
    }
    function selectVisible() {
      detailController?.abort();
      selected = currentRows.find(row => selected && identity(row) === identity(selected)) || currentRows[0] || null;
      renderList();
      if (selected && !isDiagnostic()) showDetail(selected);
    }
    async function loadPage(start = 0) {
      pageController?.abort();
      const controller = pageController = new AbortController();
      pageLoading = true;
      pageError = "";
      selected = null;
      renderList();
      try {
        const sort = $("#an-sort").value;
        const params = new URLSearchParams({ offset: String(Math.max(0, start)), limit: String(pageSize), q: $("#an-search").value, feature: $("#an-feature").value, sort: sort.split("-")[0], direction: sort.endsWith("-desc") ? "desc" : "asc" });
        const data = await requestJSON(scoped(`/api/analyzer/state?${params}`), controller.signal);
        if (controller.signal.aborted || lifetime.signal.aborted || view !== "all") return;
        total = Number(data.total || 0);
        offset = Number(data.offset || 0);
        currentRows = array(data.items);
        if (!currentRows.length && offset && total) return loadPage(Math.floor((total - 1) / pageSize) * pageSize);
        if (!currentRows.length) offset = 0;
        pageLoading = false;
        selectVisible();
      } catch (error) {
        if (controller.signal.aborted || lifetime.signal.aborted) return;
        pageError = error.message;
        pageLoading = false;
        currentRows = [];
        total = 0;
        renderList();
      }
    }
    function refreshResults() {
      clearTimeout(searchTimer);
      pageError = "";
      pageLoading = false;
      if (view === "all") {
        if (pairs.length) return loadPage();
        currentRows = [];
        total = 0;
        offset = 0;
        renderList();
        return;
      }
      filteredRows = filterRows();
      showLocalPage();
    }
    function changeView(next) {
      if (!VIEWS[next]) return;
      pageController?.abort();
      detailController?.abort();
      view = next;
      selected = null;
      $("#an-severity").value = "";
      $("#an-search").value = "";
      $("#an-feature").value = "";
      refreshResults();
    }
    async function showDetail(row) {
      detailController?.abort();
      const controller = detailController = new AbortController();
      const mismatch = missingIndex.get(identity(row));
      const render = (detail = {}, error = "") => {
        if (controller.signal.aborted || lifetime.signal.aborted || selected !== row) return;
        const reasons = [...new Set([row.message, row.reason, ...array(detail.hints).map(reasonText), ...array(detail.target_show_info).map(reasonText)].filter(Boolean))];
        const targets = array(detail.targets || mismatch?.targets || row.targets);
        const ids = Object.entries(row.ids || row.item?.ids || {});
        const provider = providerName(row.provider);
        const presence = view === "pending" ? "Waiting for a successful retry" : blockedIndex.has(identity(row)) ? "Blocked by a saved rule" : mismatch ? `Missing at ${targets.map(providerName).join(", ") || "destination"}` : "Present in the saved snapshot";
        const limitNotes = targets.map(target => {
          const provider = status.providers?.[String(target).split("@")[0]];
          const key = row.feature === "collection" ? "collection" : "watchlist";
          const limit = ["watchlist", "collection"].includes(row.feature) ? provider?.limits?.[key] : null;
          return limit && limit.item_count > 0 && limit.used >= limit.item_count ? `${providerName(target)} ${human(key)} limit reached (${limit.used}/${limit.item_count}). Free up space or review the provider account limit.` : "";
        }).filter(Boolean);
        $("#an-detail").innerHTML = `<div class="an-detail-heading"><span class="an-eyebrow">ITEM DETAILS</span><button type="button" class="an-icon-button" id="an-close-detail" aria-label="Close item details">${icon("close")}</button></div><h3>${esc(label(row))}</h3><p class="an-detail-meta">${esc([provider, human(row.feature), row.year || row.item?.year].filter(Boolean).join(" · "))}</p><div class="an-detail-status">${icon(mismatch || view === "pending" ? "info" : "check_circle")}<strong>${esc(presence)}</strong></div>${reasons.length || limitNotes.length ? `<div class="an-detail-reasons"><h4>What happened</h4>${[...limitNotes, ...reasons].map(reason => `<p>${esc(reason)}</p>`).join("")}</div>` : mismatch ? '<p class="an-muted">CrossWatch found this item in the source snapshot but could not confirm it at the destination.</p>' : ""}${error ? `<p class="an-detail-error" role="alert">${esc(error)}</p><button type="button" class="an-button" id="an-retry-detail">Retry details</button>` : ""}${mismatch || view === "pending" ? '<div class="an-next-step"><h4>Next step</h4><p>Check the pair’s sync rules and the provider. If an item needs a mapping correction, use Editor or Anime ID mappings, then run the pair again.</p></div>' : ""}${ids.length ? `<details class="an-identifiers"><summary>Item IDs (${ids.length})</summary><dl>${ids.map(([key, value]) => `<div><dt>${esc(key)}</dt><dd>${esc(value)}</dd></div>`).join("")}</dl></details>` : ""}${row.key ? `<p class="an-item-key">${esc(row.key)}</p>` : ""}`;
      };
      render(detailCache.get(identity(row)));
      if (!mismatch || detailCache.has(identity(row))) return;
      try {
        const params = new URLSearchParams({ provider: row.provider, feature: row.feature, key: row.key });
        const detail = await requestJSON(scoped(`/api/analyzer/detail?${params}`), controller.signal);
        if (controller.signal.aborted || lifetime.signal.aborted) return;
        if (detailCache.size >= 500) detailCache.clear();
        detailCache.set(identity(row), detail);
        render(detail);
      } catch (error) {
        if (!controller.signal.aborted) render({}, `Could not load the explanation: ${error.message}`);
      }
    }
    async function analyzeItems() {
      analysisController?.abort();
      pageController?.abort();
      detailController?.abort();
      const controller = analysisController = new AbortController();
      analysisState = "loading";
      analysisStarted = Date.now();
      analysisError = "";
      problems = [];
      pending = [];
      exclusions = [];
      missingIndex.clear();
      blockedIndex.clear();
      indexResultGroups();
      detailCache.clear();
      allTotal = null;
      checked = "";
      selected = null;
      currentRows = [];
      total = 0;
      pageLoading = false;
      pageError = "";
      if (view === "system") refreshResults(); else renderList();
      try {
        if (!pairs.length) {
          analysisState = "ready";
          allTotal = 0;
          refreshResults();
          return;
        }
        const [meta, snapshot, liveStatus] = await Promise.all([
          requestJSON(scoped("/api/analyzer/problems"), controller.signal),
          requestJSON(scoped("/api/analyzer/state?limit=0"), controller.signal),
          requestJSON("/api/status", controller.signal).catch(() => ({}))
        ]);
        if (controller.signal.aborted || lifetime.signal.aborted) return;
        problems = array(meta.problems);
        pending = array(meta.attention?.rows).filter(row => row.unresolved).map(row => ({ ...row.item, ...row, key: row.key || array(row.keys)[0] }));
        exclusions = array(meta.pair_exclusions);
        missingIndex = new Map(problems.filter(p => p.type === "missing_peer").map(p => [identity(p), p]));
        blockedIndex = new Map(problems.filter(p => p.type === "blocked_manual").map(p => [identity(p), p]));
        indexResultGroups();
        status = liveStatus || {};
        allTotal = Number(snapshot.total || 0);
        analysisState = "ready";
        checked = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        refreshResults();
      } catch (error) {
        if (controller.signal.aborted || lifetime.signal.aborted) return;
        analysisState = "error";
        analysisError = error.message;
        refreshResults();
      } finally {
        if (!controller.signal.aborted && !lifetime.signal.aborted) renderStatus();
      }
    }
    async function analyzeSystem(refresh = false) {
      systemController?.abort();
      const controller = systemController = new AbortController();
      systemState = "loading";
      systemStarted = Date.now();
      systemError = "";
      system = [];
      indexResultGroups();
      renderStatus();
      if (view === "system") refreshResults();
      try {
        const data = await requestJSON(`/api/analyzer/system${refresh ? "?refresh=true" : ""}`, controller.signal);
        if (controller.signal.aborted || lifetime.signal.aborted) return;
        system = array(data.problems);
        systemState = data.available === false ? "restricted" : "ready";
      } catch (error) {
        if (controller.signal.aborted || lifetime.signal.aborted) return;
        systemState = "error";
        systemError = error.message;
      }
      indexResultGroups();
      renderStatus();
      if (view === "system") refreshResults();
    }
    async function loadScope() {
      try {
        const [rawPairs, instances, runs] = await Promise.all([
          requestJSON("/api/pairs", lifetime.signal),
          requestJSON("/api/provider-instances", lifetime.signal).catch(() => ({})),
          requestJSON("/api/analyzer/pair-activity", lifetime.signal).catch(() => ({}))
        ]);
        if (lifetime.signal.aborted) return;
        pairs = array(rawPairs).filter(pair => pair.enabled !== false && pair.source && pair.target);
        for (const [provider, entries] of Object.entries(instances || {})) {
          PROFILE_LABELS[provider.toUpperCase()] = {};
          array(entries).forEach(row => {
            if (row && typeof row === "object" && row.id) PROFILE_LABELS[provider.toUpperCase()][row.id] = row.label || row.id;
          });
        }
        activity = Object.fromEntries(array(runs.pairs).map(pair => [pair.id, pair.last_run_ns]));
        let saved = [];
        try { saved = JSON.parse(localStorage.getItem("an.pairs") || "[]"); } catch {}
        const previous = array(saved).length === 1 && pairs.find(pair => pair.id === saved[0]);
        const recent = [...pairs].sort((a, b) => (activity[b.id] || 0) - (activity[a.id] || 0))[0];
        $("#an-pair").innerHTML = `<option value="">All sync pairs (${pairs.length})</option>${pairs.map(pair => `<option value="${esc(pair.id)}">${esc(pairName(pair))}</option>`).join("")}`;
        $("#an-pair").value = array(saved).length > 1 ? "" : previous?.id || recent?.id || "";
        $("#an-pair").disabled = !pairs.length;
        window.CW?.IconSelect?.enhance?.($("#an-pair"), { className: "cw-plain-select", menuClassName: "an-pair-menu" });
        updateSnapshot();
        await analyzeItems();
      } catch (error) {
        if (lifetime.signal.aborted) return;
        analysisState = "error";
        analysisError = `Could not load sync pairs. ${error.message}`;
        renderList();
      }
    }
    root.addEventListener("click", event => {
      const button = event.target.closest("button");
      if (!button) return;
      if (button.dataset.view) changeView(button.dataset.view);
      else if (button.hasAttribute("data-open-system")) changeView("system");
      else if (button.dataset.row != null) {
        selected = currentRows[Number(button.dataset.row)];
        renderList();
        showDetail(selected);
      } else if (button.dataset.page != null) {
        const start = Number(button.dataset.page);
        if (view === "all") loadPage(start); else showLocalPage(start);
      } else if (button.dataset.retry === "system") analyzeSystem(true);
      else if (button.dataset.retry === "analysis") pairs.length ? analyzeItems() : loadScope();
      else if (button.dataset.retry === "page") loadPage(offset);
      else if (button.id === "an-run") { analyzeItems(); analyzeSystem(true); }
      else if (button.id === "an-retry-detail" && selected) showDetail(selected);
      else if (button.id === "an-close-detail") { detailController?.abort(); selected = null; renderList(); }
      else if (button.id === "an-clear") {
        $("#an-search").value = "";
        $("#an-feature").value = "";
        $("#an-severity").value = "";
        refreshResults();
      } else if (button.dataset.more != null) {
        const values = affectedLabels(currentRows[Number(button.dataset.more)]);
        const shown = Number(button.dataset.shown);
        button.previousElementSibling.insertAdjacentHTML("beforeend", values.slice(shown, shown + 50).map(value => `<li>${esc(value)}</li>`).join(""));
        button.dataset.shown = String(shown + 50);
        if (shown + 50 >= values.length) button.remove();
      }
    });
    $("#an-pair").addEventListener("change", () => {
      try { localStorage.setItem("an.pairs", JSON.stringify($("#an-pair").value ? [$("#an-pair").value] : pairs.map(pair => pair.id))); } catch {}
      clearTimeout(searchTimer);
      updateSnapshot();
      analyzeItems();
    });
    $("#an-search").addEventListener("input", () => {
      pageController?.abort();
      clearTimeout(searchTimer);
      searchTimer = setTimeout(refreshResults, 250);
    });
    for (const selector of ["#an-feature", "#an-severity", "#an-sort"]) $(selector).addEventListener("change", refreshResults);
    $("#an-page-size").addEventListener("change", () => { pageSize = Number($("#an-page-size").value); refreshResults(); });
    $("#an-pagination").addEventListener("change", event => {
      if (event.target.id !== "an-page-number") return;
      const page = Math.min(Math.max(1, Number(event.target.value) || 1), Math.max(1, Math.ceil(total / pageSize)));
      if (view === "all") loadPage((page - 1) * pageSize); else showLocalPage((page - 1) * pageSize);
    });
    renderList();
    await Promise.all([loadScope(), analyzeSystem()]);
  },
  unmount() {
    mountGeneration += 1;
    activeCleanup?.();
  }
};
window.Analyzer = Analyzer;
document.addEventListener("tab-changed", event => {
  if (event.detail?.tab !== "analyzer") Analyzer.unmount();
});
export default Analyzer;
