/* assets/js/analyzer/index.js */
/* CrossWatch - Sync Analyzer Page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const FEATURES = ["history", "watchlist", "ratings", "progress", "collection"];
const VIEWS = {
  missing: { title: T("analyzer.views.missing"), icon: "compare_arrows", description: T("analyzer.views.missingHint") },
  pending: { title: T("analyzer.views.pending"), icon: "pending_actions", description: T("analyzer.views.pendingHint") },
  system: { title: T("analyzer.views.system"), icon: "monitor_heart", description: T("analyzer.views.systemHint") },
  blocked: { title: T("analyzer.views.blocked"), icon: "block", description: T("analyzer.views.blockedHint") },
  all: { title: T("analyzer.views.all"), icon: "dataset", description: T("analyzer.views.allHint") }
};
const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const icon = name => `<span class="material-symbols-rounded${name === "progress_activity" ? " an-spinner" : ""}" aria-hidden="true">${name}</span>`;
const aiAsk = question => `<button type="button" class="an-button an-ai-ask" data-ai-analyze data-ai-question="${esc(question)}" title="${TH("analyzer.askAi")}">${icon("auto_awesome")}<span>${TH("analyzer.explainAi")}</span></button>`;
const array = value => Array.isArray(value) ? value : [];
const human = value => String(value || "").replace(/[_-]+/g, " ").replace(/^./, c => c.toUpperCase());
const severity = row => row.severity === "error" ? "error" : ["warn", "warning"].includes(row.severity) ? "warn" : "info";
const identity = row => `${row.pair_id || ""}::${row.provider || ""}::${row.feature || ""}::${row.key || ""}`;
const label = row => {
  const item = row.item || row;
  const title = row.title || row.item_title || item.title || row.series_title || item.series_title || row.message || row.key || array(row.keys)[0] || T("analyzer.untitled");
  const season = row.season ?? item.season;
  const episode = row.episode ?? item.episode;
  if ((row.item_type || item.type) === "episode" && season != null && episode != null) return `${row.series_title || item.series_title || title} · S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`;
  return title;
};
const reasonText = hint => hint?.message || [hint?.provider, array(hint?.reasons).join(", ") || hint?.reason || hint?.kind].filter(Boolean).join(": ");
const unique = rows => [...new Map(rows.map(row => [JSON.stringify(row), row])).values()];
export function matchDetailsHTML(row, hints = []) {
  const entries = unique([row, ...array(hints)].filter(entry => array(entry?.match_details?.candidates).length).map(entry => ({ provider: String(entry.provider || "").trim().toUpperCase().replace(/@DEFAULT$/, ""), details: entry.match_details })));
  const ids = item => ["ids", "show_ids"].flatMap(field => Object.entries(item?.[field] || {}).map(([key, value]) => `${field === "show_ids" ? "show " : ""}${key}:${value}`)).join(" / ");
  return entries.map(({ provider, details }) => `<details class="an-match-details"><summary>${TH("analyzer.prevMatches")}${provider ? ` (${esc(provider)})` : ""}: ${esc(details.candidate_count)}</summary><ul>${array(details.candidates).slice(0, 10).map(item => `<li>${esc([item.type, item.local_id, item.title, item.year].filter(Boolean).join(" / "))}<p>${esc(ids(item))}${item.season != null && item.episode != null ? ` / S${esc(item.season)}E${esc(item.episode)}` : ""}</p></li>`).join("")}</ul>${details.truncated || details.candidates.length > 10 ? "<p>" + TH("analyzer.firstCandidates") + "</p>" : ""}</details>`).join("");
}

export function mappingRowActions(row, index) {
  const rules = array(row.saved_mappings);
  const canEdit = row.key && FEATURES.includes(row.feature) && !array(row.watch_time_differences).length && !array(row.episode_groups).length && (row.type !== "missing_peer" || array(row.targets).length === 1);
  const action = rules.length ? T("analyzer.editMapping") : T("analyzer.addMapping");
  const edit = canEdit ? `<button type="button" class="an-icon-button" data-edit-mapping="${index}" title="${action}" aria-label="${TH("analyzer.actionFor", {action, name: label(row)})}">${icon(rules.length ? "edit" : "add_link")}</button>` : "";
  return edit + rules.map((rule, ruleIndex) => {
    const scope = rule.pair_id ? "this pair" : "all pairs using this source instance";
    const name = rule.corrected?.title || rule.key;
    return `<button type="button" class="an-icon-button" data-remove-mapping="${index}" data-mapping-index="${ruleIndex}" title="Remove mapping: ${esc(name)} (${scope})" aria-label="Remove mapping: ${esc(name)} (${scope})">${icon("link_off")}</button>`;
  }).join("");
}

export function savedMappingText(row) {
  return array(row.saved_mappings).map(rule => {
    const item = rule.corrected || {};
    const ids = Object.entries(item.show_ids || item.ids || {}).map(([key, value]) => key.toUpperCase() + " " + value).join(" · ");
    return `${T("analyzer.mappingSaved")} → ` + [item.series_title || item.title || rule.key, ids].filter(Boolean).join(" · ") + ` ${T(rule.scope === "pair" ? "analyzer.thisPairParen" : "analyzer.sharedParen")}`;
  }).join("; ");
}

export function episodeGroupLabel(row) {
  const groups = array(row.episode_groups);
  if (!groups.length) return "";
  if (groups.some(group => group.status === "held")) return T("analyzer.groupHeld");
  if (groups.some(group => group.status === "waiting")) return T("analyzer.groupWaiting");
  if (groups.some(group => group.status === "pending")) return T("analyzer.groupMissing");
  return T("analyzer.groupMatched");
}
export function watchDifferenceLabel(row, providerName = String) {
  const differences = array(row.watch_time_differences);
  const timeTargets = new Set(differences.map(entry => entry.target));
  const groupTargets = new Set(array(row.episode_groups).map(group => group.target));
  const cloudTargets = new Set(array(row.cloud_confirmed_targets));
  const missing = array(row.targets).filter(target => !timeTargets.has(target) && !groupTargets.has(target) && !cloudTargets.has(target));
  return [cloudTargets.size && T("analyzer.cloudRecent", {providers: [...cloudTargets].map(providerName).join(", ")}), episodeGroupLabel(row), differences.length && T("analyzer.timeDiffersAt", {providers: [...timeTargets].map(providerName).join(", ")}),
    missing.length && T("analyzer.missingAt", {providers: missing.map(providerName).join(", ")})].filter(Boolean).join("; ") || T("analyzer.missingDestination");
}
export function retryLabel(row) {
  return row.retry_blocked ? T("analyzer.retryBlocked") : T("analyzer.pendingRetry");
}
export function nextStepText(row) {
  const blocked = row.retry_blocked ? T("analyzer.blockedHint") : "";
  if (array(row.episode_groups).length) return blocked + T("analyzer.nextGroup");
  if (array(row.watch_time_differences).length) return blocked + T("analyzer.nextTime");
  return blocked + T("analyzer.nextDefault");
}
export function watchDifferenceDetails(differences, providerName = String) {
  return array(differences).map(entry => {
    const seconds = Math.abs(Number(entry.difference_seconds));
    const duration = `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    return `<div class="an-detail-reasons"><h4>${TH("analyzer.timeDiffers")}</h4><p>${esc(providerName(entry.source))}: ${esc(entry.source_watched_at)}</p><p>${esc(providerName(entry.target))} ${TH("analyzer.closest")}: ${esc(entry.target_watched_at)}</p><p>${TH("analyzer.difference", {duration})}</p><p>${esc(entry.message)}</p></div>`;
  }).join("");
}
const affectedLabels = row => [
  ...array(row.affected_items).map(value => [value.label || value.key, value.reason, value.attempts != null && T("analyzer.attempts", {count: String(value.attempts)})].filter(Boolean).join(" · ")),
  ...array(row.extra_source_titles || row.extra_source).map(value => T("analyzer.onlySource", {value})),
  ...array(row.extra_target_titles || row.extra_target).map(value => T("analyzer.onlyDestination", {value}))
];
export function groupSnapshotGaps(gaps) {
  const groups = new Map();
  for (const row of array(gaps)) {
    const missing = [...new Set(array(row.missing))];
    const key = `${[row.source, row.target].sort().join(">")}|${[...missing].sort().join(",")}`;
    const group = groups.get(key) || { source: row.source, target: row.target, missing, features: [] };
    if (!group.features.includes(row.feature)) group.features.push(row.feature);
    groups.set(key, group);
  }
  return [...groups.values()];
}

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
    if (!response.ok) throw new Error(T("analyzer.requestFailed", {status: String(response.status)}));
    return await response.json();
  } catch (error) {
    if (controller.signal.aborted && !signal.aborted) throw new Error(T("analyzer.timeout"));
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
let mountedHost = null;
let mountedContext = "";
const contextKey = () => {
  const auth = window.CW?.AuthState?.read?.();
  return JSON.stringify([auth?.user?.id, auth?.user?.username, auth?.isManaged, auth?.permissions, window.CW?.OverviewProfile?.id || ""]);
};
const authReady = async () => {
  try { await window.__cwAuthBootstrapPromise; } catch {}
};
const profileReady = () => {
  const ready = window.CW?.OverviewProfile?.ready;
  if (typeof ready?.then !== "function") return Promise.resolve();
  return Promise.race([ready.catch(() => {}), new Promise(resolve => setTimeout(resolve, 3000))]);
};
const Analyzer = {
  async mount(host) {
    activeCleanup?.();
    mountedHost = null;
    const generation = ++mountGeneration;
    const root = document.createElement("div");
    root.className = "an-page";
    host.replaceChildren(root);
    await Promise.all([injectAnalyzerCSS(), authReady(), profileReady()]);
    if (generation !== mountGeneration || host.classList.contains("hidden")) return;
    mountedHost = host;
    mountedContext = contextKey();
    root.innerHTML = `
      <a class="an-back" href="#main">${icon("arrow_back")}${TH("navigation.tabs.main")}</a>
      <header class="an-header">
        <div><div class="an-eyebrow">${TH("analyzer.kicker")}</div><h1>${TH("analyzer.title")}</h1></div>
        <button class="an-button an-primary" id="an-run" type="button">${icon("refresh")}<span>${TH("analyzer.analyze")}</span></button>
      </header>
      <div id="an-progress" class="an-progress" role="status" aria-live="polite" hidden></div>
      <div id="an-alerts"></div>
      <p id="an-mapping-notice" class="an-mapping-notice" role="status" hidden></p>
      <section class="an-scope-bar" aria-label="${TH("analyzer.scope")}">
        <label for="an-pair">${TH("analyzer.syncPair")}<select id="an-pair" disabled><option>${TH("analyzer.loadingPairs")}</option></select></label>
        <div class="an-snapshot">${icon("schedule")}<div><strong>${TH("analyzer.savedSnapshots")}</strong><span id="an-snapshot-time">${TH("analyzer.runSync")}</span></div></div>
      </section>
      <details id="an-scope-notes" class="an-scope-notes" hidden></details>
      <nav class="an-tabs" aria-label="${TH("analyzer.views.label")}">${Object.entries(VIEWS).map(([key, view]) => `<button type="button" class="an-tab" data-view="${key}" aria-pressed="${key === "missing"}">${icon(view.icon)}<span>${view.title}</span><span class="an-count" data-count="${key}">—</span></button>`).join("")}</nav>
      <label class="cw-page-mobile-view" for="an-mobile-view"><span>${TH("analyzer.view")}</span><select id="an-mobile-view">${Object.entries(VIEWS).map(([key, view]) => `<option value="${key}">${view.title}</option>`).join("")}</select></label>
      <section class="an-results" aria-labelledby="an-view-title">
        <div class="an-results-heading"><div><h2 id="an-view-title"></h2><p id="an-view-description"></p></div><span id="an-checked" class="an-muted"></span></div>
        <div class="an-toolbar">
          <label class="an-search">${icon("search")}<input id="an-search" type="search" maxlength="300" aria-label="${TH("analyzer.searchLabel")}" placeholder="${TH("analyzer.searchPlaceholder")}"></label>
          <button type="button" class="an-button an-mobile-filters" id="an-mobile-filters" aria-label="${TH("analyzer.filtersSorting")}" aria-expanded="false" aria-controls="an-filter-options">${icon("tune")}</button>
          <div id="an-filter-options" class="an-filter-options">
          <label class="an-filter">${TH("analyzer.feature")}<select id="an-feature"><option value="">${TH("analyzer.allFeatures")}</option>${FEATURES.map(f => `<option value="${f}">${human(f)}</option>`).join("")}</select></label>
          <label class="an-filter" id="an-severity-wrap" hidden>${TH("analyzer.severity")}<select id="an-severity"><option value="">${TH("analyzer.allSeverities")}</option><option value="error">${TH("analyzer.errors")}</option><option value="warn">${TH("analyzer.warnings")}</option><option value="info">${TH("analyzer.information")}</option></select></label>
          <label class="an-filter">${TH("analyzer.sort")}<select id="an-sort"><option value="title">${TH("analyzer.titleAz")}</option><option value="title-desc">${TH("analyzer.titleZa")}</option><option value="provider">${TH("analyzer.provider")}</option><option value="feature">${TH("analyzer.feature")}</option></select></label>
          <button type="button" class="an-button an-clear" id="an-clear" hidden>${TH("analyzer.clearFilters")}</button>
          </div>
        </div>
        <div class="an-list-meta"><span id="an-result-count" role="status" aria-live="polite"></span><label>${TH("analyzer.perPage")}<select id="an-page-size" aria-label="${TH("analyzer.itemsPerPage")}"><option>25</option><option selected>50</option><option>100</option><option>250</option></select></label></div>
        <div id="an-workspace" class="an-workspace">
          <div class="an-list-column"><div id="an-list"></div><nav id="an-pagination" class="an-pagination" aria-label="${TH("analyzer.resultPages")}"></nav></div>
          <aside id="an-detail" class="an-detail" aria-label="${TH("analyzer.itemDetails")}" tabindex="-1" hidden></aside>
        </div>
      </section>`;
    const $ = selector => root.querySelector(selector);
    const lifetime = new AbortController();
    $("#an-mobile-filters").addEventListener("click", event => {
      const button = event.currentTarget;
      if (window.CW?.MobileUI?.mobile()) {
        window.CW.MobileUI.sheet($("#an-filter-options"), {trigger:button, title:T("analyzer.filtersSorting")});
        return;
      }
      button.setAttribute("aria-expanded", String(button.getAttribute("aria-expanded") !== "true"));
    });
    let analysisController, systemController, pageController, detailController, searchTimer;
    let pairs = [], problems = [], pending = [], system = [], exclusions = [], snapshotGaps = [];
    let PROFILE_LABELS = {}, activity = {}, status = {};
    let view = "missing", analysisState = "loading", systemState = "loading";
    let analysisError = "", systemError = "", checked = "", pageError = "";
    let analysisStarted = Date.now(), systemStarted = Date.now();
    let allTotal = null, selected = null;
    let listScroll = 0, detailIndex = 0;
    let detailLayer = null;
    const closeMobileDetail = () => {
      detailController?.abort();
      selected = null;
      const layer = detailLayer;
      detailLayer = null;
      void layer?.dismiss();
      renderList();
      if (mobile()) {
        window.scrollTo({top:listScroll, behavior:'instant'});
        $(`.an-item-button[data-row="${detailIndex}"]`)?.focus({preventScroll:true});
      }
    };
    const mobile = () => window.matchMedia("(max-width:760px)").matches;
    let mappingWorkspace = null, mappingController = null, savedRow = null, releasing = false;
    let scopeSequence = 0;
    let total = 0, offset = 0, pageSize = 50;
    let currentRows = [], filteredRows = [];
    let pageLoading = false;
    let missingIndex = new Map(), blockedIndex = new Map();
    const detailCache = new Map();
    const cleanup = () => {
      root.querySelector('dialog.cw-page-panel-sheet')?.close();
      const layer = detailLayer;
      detailLayer = null;
      void layer?.dismiss();
      lifetime.abort();
      mappingController?.abort();
      mappingWorkspace?.destroy?.();
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
    const scoped = (path, pairId = "") => {
      const url = new URL(path, location.origin);
      const ids = pairId ? [pairId] : $("#an-pair").value ? [$("#an-pair").value] : pairs.map(p => p.id);
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
      const name = window.CW?.ProviderMeta?.label?.(provider) || provider;
      const profile = profileLabel(provider, instance);
      return profile ? `${name} · ${profile}` : name;
    };
    const pairName = pair => {
      const twoWay = ["two-way", "bi", "both", "mirror", "two", "two_way", "two way"].includes(String(pair.mode).toLowerCase());
      const endpoint = (provider, instance) => providerName(`${provider}${instance ? `@${instance}` : ""}`);
      return `${endpoint(pair.source, pair.source_instance)} ${twoWay ? "↔" : "→"} ${endpoint(pair.target, pair.target_instance)}`;
    };
    const pairOptionData = value => {
      const pair = pairs.find(pair => pair.id === value);
      return pair ? window.CW?.syncPairOptionData?.(pair) || null : null;
    };
    const endpointToken = (provider, instance) => {
      const profile = String(instance || "").trim();
      return `${String(provider || "").toUpperCase()}${profile && profile.toLowerCase() !== "default" ? `@${profile}` : ""}`;
    };
    const endpointPill = token => {
      const [provider, instance] = String(token || "").split("@");
      return window.CW?.syncProfilePillHTML?.(provider, instance) || esc(providerName(token));
    };
    const routeHTML = row => {
      const runs = (pair, source, target) => endpointToken(pair.source, pair.source_instance) === source && endpointToken(pair.target, pair.target_instance) === target;
      const pair = pairs.find(pair => runs(pair, row.source, row.target)) || pairs.find(pair => runs(pair, row.target, row.source));
      const [source, source_instance] = String(row.source || "").split("@");
      const [target, target_instance] = String(row.target || "").split("@");
      return window.CW?.syncProfileRouteHTML?.(pair || { source, source_instance, target, target_instance }) || `${esc(providerName(row.source))} → ${esc(providerName(row.target))}`;
    };
    const gapNote = row => `${esc(row.features.map(human).join(", "))}: ${row.missing.length > 1 ? TH("analyzer.noSnapshotEither") : `${TH("analyzer.noSnapshotFor")} ${endpointPill(row.missing[0])}`}`;
    const rowPairName = row => {
      const pair = pairs.find(pair => pair.id === row.pair_id);
      return pair ? pairName(pair) : row.pair_id || "";
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
      if (analysisState === "loading") tasks.push(`${T("analyzer.checkingItems")} · ${elapsed(analysisStarted)}`);
      if (systemState === "loading") tasks.push(`${T("analyzer.checkingSystem")} · ${elapsed(systemStarted)}`);
      $("#an-progress").hidden = !tasks.length;
      $("#an-progress").innerHTML = `${icon("progress_activity")}<span>${tasks.map(esc).join('<span class="an-progress-sep">/</span>')}</span>`;
      $("#an-run").disabled = !!tasks.length;
      $("#an-run span:last-child").textContent = tasks.length ? T("analyzer.analyzing") : T("analyzer.analyze");
    }
    const progressTimer = setInterval(updateProgress, 1000);
    function renderStatus() {
      const findings = groups.system;
      const { errors, warnings } = groups;
      const counts = { missing: analysisState === "ready" ? missingIndex.size : null, pending: analysisState === "ready" ? pending.length : null, blocked: analysisState === "ready" ? groups.blocked.length : null, system: ["ready", "restricted"].includes(systemState) ? findings.length : null, all: allTotal };
      for (const [key, count] of Object.entries(counts)) {
        $(`[data-count="${key}"]`).textContent = count == null ? "—" : count.toLocaleString(I18N()?.locale);
        $(`[data-view="${key}"]`).setAttribute("aria-pressed", String(view === key));
        $(`#an-mobile-view option[value="${key}"]`).textContent = `${VIEWS[key].title}${count == null ? "" : ` (${count.toLocaleString(I18N()?.locale)})`}`;
      }
      $("#an-mobile-view").value = view;
      $('[data-view="system"]').classList.toggle("has-errors", errors > 0 || systemState === "error");
      const alerts = [];
      const gaps = groupSnapshotGaps(snapshotGaps);
      if (analysisState === "ready" && gaps.length) alerts.push(`<div class="an-notice" role="status">${icon("info")}<div><strong>${TH("analyzer.gapsTitle")}</strong><p>${TH("analyzer.gapsHint")}</p>${gaps.slice(0, 5).map(row => `<p class="an-route-note">${routeHTML(row)}<span>${gapNote(row)}</span></p>`).join("")}${gaps.length > 5 ? `<p>${TH("analyzer.moreGaps", {count: gaps.length - 5})}</p>` : ""}</div></div>`);
      if (analysisState === "error") alerts.push(`<div class="an-notice an-error" role="alert">${icon("error")}<div><strong>${TH("analyzer.analysisFailed")}</strong><p>${esc(analysisError)}</p></div><button type="button" class="an-button" data-retry="analysis">${TH("analyzer.retry")}</button></div>`);
      if (systemState === "error") alerts.push(`<div class="an-notice an-error" role="alert">${icon("error")}<div><strong>${TH("analyzer.systemFailed")}</strong><p>${esc(systemError)} ${TH("analyzer.resultUnknown")}</p></div><button type="button" class="an-button" data-retry="system">${TH("analyzer.retry")}</button></div>`);
      if (errors || warnings) alerts.push(`<div class="an-notice ${errors ? "an-error" : "an-warning"}">${icon(errors ? "error" : "warning")}<div><strong>${TH(errors ? "analyzer.failuresAttention" : "analyzer.warningsAttention", {count: errors || warnings})}</strong><p>${errors && warnings ? `${TH("analyzer.alsoWarnings", {count: warnings})} ` : ""}${TH("analyzer.reviewDiagnostics")}</p></div><button type="button" class="an-button" data-open-system>${TH("analyzer.viewFindings")}</button></div>`);
      $("#an-alerts").innerHTML = alerts.join("");
      $("#an-checked").textContent = checked ? T("analyzer.analyzedAt", {time: checked}) : "";
      updateProgress();
    }
    function updateSnapshot() {
      const ids = $("#an-pair").value ? [$("#an-pair").value] : pairs.map(p => p.id);
      const times = ids.map(id => Number(activity[id] || 0) / 1000000).filter(Boolean);
      $("#an-snapshot-time").textContent = times.length ? T("analyzer.lastSync", {time: new Date(Math.max(...times)).toLocaleString(I18N()?.locale)}) : T("analyzer.noSyncTime");
    }
    function renderView() {
      $("#an-view-title").textContent = VIEWS[view].title;
      $("#an-view-description").textContent = VIEWS[view].description;
      $("#an-severity-wrap").hidden = view !== "system";
      $("#an-workspace").classList.toggle("an-diagnostics", isDiagnostic());
      $("#an-clear").hidden = !($("#an-search").value || $("#an-feature").value || $("#an-severity").value);
      const excluded = exclusions.reduce((sum, row) => sum + Number(row.excluded_total || 0), 0);
      $("#an-scope-notes").hidden = !excluded;
      $("#an-scope-notes").innerHTML = `<summary>${TH("analyzer.excludedSummary", {count: excluded})}</summary>${exclusions.map(row => `<p class="an-route-note">${routeHTML(row)}<span>${esc(human(row.feature))}: ${TH("analyzer.ex.excluded", {count: Number(row.excluded_total || 0)})}${row.excluded_types ? ` · ${TH("analyzer.ex.types")}: ${esc(Object.keys(row.excluded_types).join(", "))}` : ""}${row.excluded_libraries ? ` · ${TH("analyzer.ex.libraries")}: ${esc(Object.keys(row.excluded_libraries).join(", "))}` : ""}${row.excluded_anime_only ? ` · ${TH("analyzer.ex.animeOnly")}: ${Number(row.excluded_anime_only).toLocaleString(I18N()?.locale)}` : ""}${row.excluded_dropped ? ` · ${TH("analyzer.ex.dropped")}: ${Number(row.excluded_dropped).toLocaleString(I18N()?.locale)}` : ""}${row.excluded_rewatch_reset ? ` · ${TH("analyzer.ex.rewatchReset")}: ${Number(row.excluded_rewatch_reset).toLocaleString(I18N()?.locale)}` : ""}</span></p>`).join("")}`;
      renderStatus();
    }
    function emptyState(title, message, symbol = "search_off") {
      return `<div class="an-empty">${icon(symbol)}<h3>${esc(title)}</h3><p>${esc(message)}</p></div>`;
    }
    function pagination() {
      const pages = Math.max(1, Math.ceil(total / pageSize));
      const current = Math.floor(offset / pageSize) + 1;
      $("#an-result-count").textContent = pageLoading ? T("analyzer.loadingResults") : total ? T(isDiagnostic() ? "analyzer.rangeFindings" : "analyzer.rangeItems", {start: offset + 1, end: Math.min(offset + currentRows.length, total), count: total}) : T("analyzer.zeroResults");
      $("#an-pagination").innerHTML = `<button type="button" class="an-button" data-page="0" ${pageLoading || current === 1 ? "disabled" : ""}>${icon("first_page")}<span>${TH("analyzer.first")}</span></button><button type="button" class="an-button" aria-label="${TH("analyzer.previousPage")}" data-page="${offset - pageSize}" ${pageLoading || current === 1 ? "disabled" : ""}>${icon("chevron_left")}<span>${TH("analyzer.previous")}</span></button><label>${TH("analyzer.page")} <input type="number" id="an-page-number" aria-label="${TH("analyzer.pageNumber")}" min="1" max="${pages}" value="${current}" ${pageLoading ? "disabled" : ""}> ${TH("analyzer.ofPages", {pages})}</label><button type="button" class="an-button" aria-label="${TH("analyzer.nextPage")}" data-page="${offset + pageSize}" ${pageLoading || current >= pages ? "disabled" : ""}><span>${TH("analyzer.next")}</span>${icon("chevron_right")}</button><button type="button" class="an-button" data-page="${(pages - 1) * pageSize}" ${pageLoading || current >= pages ? "disabled" : ""}><span>${TH("analyzer.last")}</span>${icon("last_page")}</button>`;
    }
    function renderList() {
      renderView();
      pagination();
      $("#an-detail").hidden = isDiagnostic() || !selected || !currentRows.length;
      if ($("#an-detail").hidden && detailLayer) {
        const layer = detailLayer;
        detailLayer = null;
        void layer.dismiss();
      }
      $("#an-workspace").classList.toggle("has-detail", !$("#an-detail").hidden);
      if (pageLoading) {
        $("#an-list").innerHTML = emptyState(T("analyzer.loadingItems"), T("analyzer.readingPage"), "progress_activity");
        return;
      }
      if (pageError) {
        $("#an-list").innerHTML = emptyState(T("analyzer.resultsFailed"), pageError, "error") + '<button class="an-button an-list-retry" type="button" data-retry="page">' + TH("analyzer.retryPage") + '</button>';
        return;
      }
      if (!currentRows.length) {
        let title = T("analyzer.noMatching"), message = T("analyzer.tryAnother"), symbol = "search_off";
        if (!$("#an-search").value && !$("#an-feature").value && !$("#an-severity").value) {
          const loading = view === "system" ? systemState === "loading" : analysisState === "loading";
          const failed = view === "system" ? systemState === "error" : analysisState === "error";
          title = loading ? T("analyzer.inProgress") : failed ? T("analyzer.unavailable") : ({ missing: T("analyzer.empty.missing"), pending: T("analyzer.empty.pending"), blocked: T("analyzer.empty.blocked"), system: T("analyzer.empty.system"), all: T("analyzer.empty.all") })[view];
          message = loading ? T("analyzer.resultsAppear") : failed ? T("analyzer.retryFailed") : view === "system" && systemState === "restricted" ? T("analyzer.adminOnly") : view === "missing" ? T("analyzer.noneMissing") : T("analyzer.nothing");
          symbol = loading ? "progress_activity" : failed ? "error" : "check_circle";
          if (!loading && !failed && view === "missing" && snapshotGaps.length) {
            title = T("analyzer.incomplete");
            message = T("analyzer.incompleteHint");
            symbol = "info";
          }
          if (!loading && !failed && view !== "system" && !pairs.length) {
            title = T("analyzer.noPairs");
            message = T("analyzer.noPairsHint");
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
      const editable = ["pending", "missing"].includes(view) && window.CW?.AuthState?.read?.().permissions?.write !== false;
      $("#an-list").innerHTML = `<div class="an-table-scroll"><table class="an-table" role="table"><thead><tr><th scope="col">${TH("analyzer.item")}</th><th scope="col">${TH("analyzer.provider")}</th><th scope="col">${TH("analyzer.feature")}</th><th scope="col">${TH("analyzer.finding")}</th>${editable ? '<th scope="col" class="an-mapping-column" aria-label="' + TH("analyzer.editMapping") + '"></th>' : ''}</tr></thead><tbody>${currentRows.map((row, index) => {
        const mismatch = missingIndex.get(identity(row));
        const finding = view === "pending" ? retryLabel(row) : row.type === "missing_peer" || mismatch ? watchDifferenceLabel(mismatch || row, providerName) : episodeGroupLabel(row) || (blockedIndex.has(identity(row)) ? T("analyzer.blocked") : T("analyzer.noPresenceIssue"));
        return `<tr data-row="${index}" class="${selected === row ? "is-selected" : ""}"><td><button type="button" class="an-item-button" data-row="${index}" aria-pressed="${selected === row}"><strong>${esc(label(row))}</strong><span>${esc([human(row.item_type || row.item?.type || (row.type === "missing_peer" ? "" : row.type)), row.year || row.item?.year].filter(Boolean).join(" · "))}</span></button></td><td data-label="Provider">${esc(providerName(row.provider))}${rowPairName(row) ? `<span class="an-pair-label">${esc(rowPairName(row))}</span>` : ""}</td><td data-label="Feature"><span class="an-feature-badge">${esc(human(row.feature))}</span></td><td data-label="Finding"><span class="an-result-badge ${mismatch || row.type === "missing_peer" ? "an-warn-text" : ""}">${esc(finding)}</span>${savedMappingText(row) ? `<span class="an-pair-label">${esc(savedMappingText(row))}</span>` : ""}</td>${editable ? `<td class="an-mapping-column">${mappingRowActions(row, index)}${row.retry_blocked && row.key ? `<button type="button" class="an-icon-button" data-release="${index}" title="${TH("analyzer.releaseRetryBlock")}" aria-label="Release retry block for ${esc(label(row))}">${icon("lock_open")}</button>` : ''}</td>` : ''}</tr>`;
      }).join("")}</tbody></table></div>`;
    }
    function renderFinding(row, index) {
      const sev = severity(row);
      const title = row.title || row.message || human(row.type) || T("analyzer.diagnostic");
      const context = [providerName(row.provider || row.source), row.feature && human(row.feature), row.category && human(row.category)].filter(Boolean).join(" · ");
      const detail = [rowPairName(row) && `Sync pair: ${rowPairName(row)}`, row.message && row.message !== title ? row.message : "", row.item_title, row.key && `Key: ${row.key}`, row.module && `Module: ${row.module}`, row.path && `Path: ${row.path}`, row.error, row.reason, row.count != null && `Items affected: ${row.count}`, row.source && row.target && `${row.source} → ${row.target}`, row.show_delta && `Shows: ${row.show_delta.source} at source / ${row.show_delta.target} at destination`, row.show_gap && `Source-only shows: ${row.show_gap.source_only}; destination-only shows: ${row.show_gap.target_only}`, row.missing?.length && `Missing IDs: ${row.missing.join(", ")}`, row.id_name && `ID field: ${row.id_name} (${row.id_value ?? ""})`, row.watermark_key && `Watermark: ${row.watermark_key}`, row.value != null && `Value: ${row.value}`].filter(Boolean);
      const items = affectedLabels(row);
      return `<article class="an-finding an-severity-${sev}"><div class="an-finding-icon">${icon(sev === "error" ? "error" : sev === "warn" ? "warning" : "info")}</div><div class="an-finding-content"><div class="an-finding-top"><span class="an-severity-badge">${TH(sev === "error" ? "analyzer.sev.error" : sev === "warn" ? "analyzer.sev.warning" : "analyzer.sev.info")}</span><span>${esc(context)}</span></div><h3>${esc(title)}</h3>${detail.map(value => `<p>${esc(value)}</p>`).join("")}${items.length ? `<details class="an-affected"><summary>${Number(row.count) > items.length ? TH("analyzer.affectedOf", {shown: items.length, total: Number(row.count)}) : TH("analyzer.affected", {count: items.length})}</summary><ul>${items.slice(0, 25).map(value => `<li>${esc(value)}</li>`).join("")}</ul>${items.length > 25 ? `<button type="button" class="an-button" data-more="${index}" data-shown="25">${TH("analyzer.showMore")}</button>` : ""}</details>` : ""}${row.type === "history_episode_identity_conflict" && window.CW?.AuthState?.read?.().permissions?.write !== false ? array(row.shows).map((show, at) => `<button type="button" class="an-button" data-fix-numbering="${index}" data-show="${at}">${TH("analyzer.fixMatching")}${show.title ? ` · ${esc(show.title)}` : ""}</button>`).join("") : ""}${row.type === "blocked_manual" && row.block && window.CW?.AuthState?.read?.().permissions?.write !== false ? `<button type="button" class="an-button" data-release="${index}">${icon(row.block.mapping_key ? "link_off" : "lock_open")}<span>${TH(row.block.mapping_key ? "analyzer.removeMapping" : "analyzer.releaseBlock")}</span></button>` : ""}${sev === "error" || sev === "warn" ? aiAsk(`Explain this CrossWatch system health finding: ${title}.${context ? ` ${context}.` : ""}${detail.filter(Boolean).slice(0, 4).map(value => ` ${value}.`).join("")} Why is this happening and how do I fix it?`) : ""}<span class="an-finding-code">${esc(row.type)}</span></div></article>`;
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
      selected = currentRows.find(row => selected && identity(row) === identity(selected)) || (mobile() ? null : currentRows[0]) || null;
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
    function redrawFilters() {
      for (const selector of ["#an-severity", "#an-feature"]) window.CW?.IconSelect?.enhance?.($(selector), $(selector).__cwIconSelectCfg || { className: "cw-plain-select" });
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
      redrawFilters();
      refreshResults();
    }
    async function showDetail(row) {
      detailController?.abort();
      const controller = detailController = new AbortController();
      const mismatch = missingIndex.get(identity(row));
      const render = (detail = {}, error = "") => {
        if (controller.signal.aborted || lifetime.signal.aborted || selected !== row) return;
        const reasons = [...new Set([row.message, row.reason_message || row.reason, row.hint_message, ...array(detail.hints).map(hint => hint.hint_message), ...array(detail.episode_groups || mismatch?.episode_groups || row.episode_groups).map(reasonText), ...array(detail.hints).map(reasonText), ...array(detail.target_show_info).map(reasonText)].filter(Boolean))];
        const targets = array(detail.targets || mismatch?.targets || row.targets);
        const ids = Object.entries(row.ids || row.item?.ids || {});
        const provider = providerName(row.provider);
        const diagnosis = { ...row, ...mismatch, ...detail, retry_blocked: row.retry_blocked || mismatch?.retry_blocked || array(detail.hints).some(hint => hint.kind === "blackbox") };
        const presence = view === "pending" ? retryLabel(row) : blockedIndex.has(identity(row)) ? T("analyzer.blockedRule") : mismatch ? watchDifferenceLabel(diagnosis, providerName) : episodeGroupLabel(diagnosis) || T("analyzer.presentSnapshot");
        const limitNotes = targets.map(target => {
          const provider = status.providers?.[String(target).split("@")[0]];
          const key = row.feature === "collection" ? "collection" : "watchlist";
          const limit = ["watchlist", "collection"].includes(row.feature) ? provider?.limits?.[key] : null;
          return limit && limit.item_count > 0 && limit.used >= limit.item_count ? T("analyzer.limitReached", {provider: providerName(target), feature: human(key), used: String(limit.used), total: String(limit.item_count)}) : "";
        }).filter(Boolean);
        const ask = view !== "all" || mismatch ? aiAsk(`Explain this analyzer finding for ${label(row)} (${[human(row.feature), provider, rowPairName(row) && `sync pair ${rowPairName(row)}`, row.key && `key ${row.key}`].filter(Boolean).join(", ")}): ${presence}.${reasons.slice(0, 3).map(value => ` ${value}.`).join("")} Why is this happening and what should I do?`) : "";
        $("#an-detail").innerHTML = `<div class="an-detail-heading"><span class="an-eyebrow">${TH("analyzer.detailsKicker")}</span><button type="button" class="an-icon-button" id="an-close-detail" aria-label="${TH("analyzer.closeDetails")}">${icon("close")}<span class="an-mobile-back">${TH("analyzer.backResults")}</span></button></div><h3>${esc(label(row))}</h3><p class="an-detail-meta">${esc([provider, human(row.feature), row.year || row.item?.year].filter(Boolean).join(" · "))}</p>${rowPairName(row) ? `<p class="an-detail-meta">${TH("analyzer.syncPairValue", {name: rowPairName(row)})}</p>` : ""}<div class="an-detail-status">${icon(mismatch || view === "pending" ? "info" : "check_circle")}<strong>${esc(presence)}</strong></div>${reasons.length || limitNotes.length ? `<div class="an-detail-reasons"><h4>${TH("analyzer.whatHappened")}</h4>${[...limitNotes, ...reasons].map(reason => `<p>${esc(reason)}</p>`).join("")}</div>` : mismatch ? '<p class="an-muted">' + TH("analyzer.notConfirmed") + '</p>' : ""}${matchDetailsHTML(row, detail.hints)}${savedMappingText(row) ? `<div class="an-detail-reasons"><h4>${TH("analyzer.savedMapping")}</h4><p>${esc(savedMappingText(row))}</p><p>${TH("analyzer.savedMappingHint")}</p></div>` : ""}${watchDifferenceDetails(diagnosis.watch_time_differences, providerName)}${error ? `<p class="an-detail-error" role="alert">${esc(error)}</p><button type="button" class="an-button" id="an-retry-detail">${TH("analyzer.retryDetails")}</button>` : ""}${mismatch || view === "pending" ? `<div class="an-next-step"><h4>${TH("analyzer.nextStep")}</h4><p>${esc(nextStepText(diagnosis))}</p></div>` : ""}${ids.length ? `<details class="an-identifiers"><summary>${TH("analyzer.itemIds", {count: ids.length})}</summary><dl>${ids.map(([key, value]) => `<div><dt>${esc(key)}</dt><dd>${esc(value)}</dd></div>`).join("")}</dl></details>` : ""}${row.key ? `<p class="an-item-key">${esc(row.key)}</p>` : ""}${ask}`;
      };
      render(detailCache.get(identity(row)));
      if (!mismatch || detailCache.has(identity(row))) return;
      try {
        const params = new URLSearchParams({ provider: row.provider, feature: row.feature, key: row.key });
        const detail = await requestJSON(scoped(`/api/analyzer/detail?${params}`, row.pair_id), controller.signal);
        if (controller.signal.aborted || lifetime.signal.aborted) return;
        if (detailCache.size >= 500) detailCache.clear();
        detailCache.set(identity(row), detail);
        render(detail);
      } catch (error) {
        if (!controller.signal.aborted) render({}, T("analyzer.explainFailed", {error: error.message}));
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
      snapshotGaps = [];
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
        snapshotGaps = array(meta.snapshot_gaps);
        missingIndex = new Map(problems.filter(p => p.type === "missing_peer").map(p => [identity(p), p]));
        blockedIndex = new Map(problems.filter(p => p.type === "blocked_manual").map(p => [identity(p), p]));
        indexResultGroups();
        status = liveStatus || {};
        allTotal = Number(snapshot.total || 0);
        analysisState = "ready";
        checked = new Date().toLocaleTimeString(I18N()?.locale || [], { hour: "2-digit", minute: "2-digit" });
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
    async function fixNumbering(row, at, button) {
      const show = array(row?.shows)[at];
      if (!show || !row.pair_id) return;
      const {openEpisodeAlignment} = await import(`/assets/js/editor/episode-alignment.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
      await openEpisodeAlignment(button, {pairId:row.pair_id, showIds:show.show_ids, onSaved:() => analyzeItems()});
    }
    async function editMapping(row, button) {
      if (!row || mappingWorkspace || mappingController) return;
      const pairId = row.pair_id || $("#an-pair").value;
      const notice = $("#an-mapping-notice");
      notice.hidden = false;
      if (!pairId) { notice.textContent = T("analyzer.selectPairEdit"); return; }
      notice.textContent = T("analyzer.openingMapping");
      const controller = mappingController = new AbortController();
      button.disabled = true;
      const finish = () => {
        mappingWorkspace = null;
        mappingController = null;
        controller.abort();
        if (button.isConnected) { button.disabled = false; button.focus(); }
      };
      try {
        const {openAnalyzerMapping} = await import(`./mapping.js?v=${encodeURIComponent(window.APP_VERSION || '1')}`);
        if (controller.signal.aborted) return;
        mappingWorkspace = await openAnalyzerMapping({
          reference:{pair_id:pairId, provider:row.type === "missing_peer" ? row.targets[0] : row.provider, feature:row.feature, key:row.key},
          signal:controller.signal,
          onClose: () => { notice.hidden = true; finish(); },
          onGroupsChanged: () => { if (!lifetime.signal.aborted) analyzeItems(); },
          onRemoved: () => {
            finish();
            if (!lifetime.signal.aborted) {
              analyzeItems();
              notice.hidden = false;
              notice.textContent = T("analyzer.mappingRemoved");
            }
          },
          onSaved: () => {
            finish();
            if (!lifetime.signal.aborted) analyzeItems();
            if (!lifetime.signal.aborted) {
              notice.hidden = false;
              notice.textContent = T("analyzer.mappingSavedRun");
              if (row.retry_blocked) {
                savedRow = { ...row, pair_id: pairId };
                notice.innerHTML = `${TH("analyzer.savedStillBlocked")} <button type="button" class="an-button" data-release-saved>${icon("lock_open")}<span>${TH("analyzer.releaseRetry")}</span></button>`;
              }
            }
          },
        });
        if (!controller.signal.aborted) notice.hidden = true;
      } catch (error) {
        if (!controller.signal.aborted) notice.textContent = error.message || T("analyzer.openMappingFailed");
        finish();
      }
    }
    async function removeMapping(row, button) {
      if (!row || releasing || mappingWorkspace || mappingController) return;
      const rule = array(row.saved_mappings)[Number(button.dataset.mappingIndex)];
      if (!rule) return;
      const scope = rule.pair_id ? "this pair" : "all pairs using this source instance";
      const name = rule.corrected?.title || rule.key;
      if (!(await (window.CW?.MobileUI?.ask || window.confirm)(T("analyzer.removeConfirm", {name, scope})))) return;
      const notice = $("#an-mapping-notice");
      releasing = true; button.disabled = true;
      try {
        const identity = {...Object.fromEntries(["provider", "instance", "feature", "key", "pair_id"].map(key => [key, rule[key]])), entry_type:"mapping"};
        const response = await fetch("/api/editor/mappings/delete", {method:"POST", credentials:"same-origin", headers:{"Content-Type":"application/json"}, body:JSON.stringify(identity)});
        const data = await response.json();
        if (!response.ok || data.ok === false) throw new Error(typeof data.detail === "string" ? data.detail : T("analyzer.removeFailed"));
        if (!lifetime.signal.aborted) {
          await analyzeItems();
          notice.hidden = false;
          notice.textContent = T("analyzer.mappingRemoved");
        }
      } catch (error) {
        if (!lifetime.signal.aborted) { notice.hidden = false; notice.textContent = error.message; }
      } finally { releasing = false; if (button.isConnected) button.disabled = false; }
    }
    async function releaseRow(row, button) {
      if (!row || releasing) return;
      const rule = row.type === "blocked_manual";
      const pairId = row.pair_id || $("#an-pair").value;
      const notice = $("#an-mapping-notice");
      notice.hidden = false;
      if (!pairId) { notice.textContent = T("analyzer.selectPairRelease"); return; }
      if (rule && row.block?.mapping_key && !(await (window.CW?.MobileUI?.ask || window.confirm)(T("analyzer.removeItemConfirm")))) { notice.hidden = true; return; }
      releasing = true;
      button.disabled = true;
      notice.textContent = T("analyzer.releasing");
      try {
        const response = await fetch("/api/analyzer/release", {
          method: "POST", credentials: "same-origin", cache: "no-store",
          headers: { "Content-Type": "application/json" },
          signal: lifetime.signal,
          body: JSON.stringify({ pair_id: pairId, provider: row.provider, feature: row.feature, key: row.key, kind: rule ? "rule" : "retry" }),
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || data.ok === false) throw new Error(data.detail || data.error || T("analyzer.releaseFailed"));
        savedRow = null;
        notice.textContent = data.released === "mapping" ? T("analyzer.releasedMapping")
          : data.released === "rule" ? T("analyzer.releasedRule")
          : T("analyzer.releasedRetry");
        await analyzeItems();
      } catch (error) {
        if (!lifetime.signal.aborted) notice.textContent = error.message || T("analyzer.releaseFailedRetry");
        if (button.isConnected) button.disabled = false;
      } finally {
        releasing = false;
      }
    }
    async function loadScope() {
      const sequence = ++scopeSequence;
      try {
        const [rawPairs, instances, runs] = await Promise.all([
          requestJSON("/api/pairs", lifetime.signal),
          requestJSON("/api/provider-instances", lifetime.signal).catch(() => ({})),
          requestJSON("/api/analyzer/pair-activity", lifetime.signal).catch(() => ({}))
        ]);
        if (lifetime.signal.aborted || sequence !== scopeSequence) return;
        pairs = array(rawPairs).map((pair, index) => ({ ...pair, order: index + 1 })).filter(pair => pair.enabled !== false && pair.source && pair.target);
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
        window.CW?.IconSelect?.enhance?.($("#an-pair"), { className: "cw-plain-select", menuClassName: "an-pair-menu", getOptionData: pairOptionData });
        updateSnapshot();
        await analyzeItems();
      } catch (error) {
        if (lifetime.signal.aborted || sequence !== scopeSequence) return;
        analysisState = "error";
        analysisError = T("analyzer.pairsFailed", {error: error.message});
        renderList();
      }
    }
    root.addEventListener("click", event => {
      const button = event.target.closest("button, tr[data-row]");
      if (!button) return;
      if (button.hasAttribute("data-edit-mapping")) editMapping(currentRows[Number(button.dataset.editMapping)], button);
      else if (button.hasAttribute("data-release-saved")) releaseRow(savedRow, button);
      else if (button.hasAttribute("data-release")) releaseRow(currentRows[Number(button.dataset.release)], button);
      else if (button.hasAttribute("data-remove-mapping")) removeMapping(currentRows[Number(button.dataset.removeMapping)], button);
      else if (button.hasAttribute("data-fix-numbering")) fixNumbering(currentRows[Number(button.dataset.fixNumbering)], Number(button.dataset.show), button);
      else if (button.dataset.view) changeView(button.dataset.view);
      else if (button.hasAttribute("data-open-system")) changeView("system");
      else if (button.dataset.row != null) {
        listScroll = window.scrollY;
        detailIndex = Number(button.dataset.row);
        selected = currentRows[Number(button.dataset.row)];
        renderList();
        showDetail(selected);
        if (mobile()) {
          detailLayer ||= window.CW?.MobileUI?.layer(closeMobileDetail);
          $("#an-detail").scrollIntoView({block:"start", behavior:"instant"});
          $("#an-detail").focus({preventScroll:true});
        }
      } else if (button.dataset.page != null) {
        const start = Number(button.dataset.page);
        if (view === "all") loadPage(start); else showLocalPage(start);
      } else if (button.dataset.retry === "system") analyzeSystem(true);
      else if (button.dataset.retry === "analysis") pairs.length ? analyzeItems() : loadScope();
      else if (button.dataset.retry === "page") loadPage(offset);
      else if (button.id === "an-run") { pairs.length ? analyzeItems() : loadScope(); analyzeSystem(true); }
      else if (button.id === "an-retry-detail" && selected) showDetail(selected);
      else if (button.id === "an-close-detail") {
        closeMobileDetail();
      }
      else if (button.id === "an-clear") {
        $("#an-search").value = "";
        $("#an-feature").value = "";
        $("#an-severity").value = "";
        redrawFilters();
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
    $("#an-mobile-view").addEventListener("change", event => changeView(event.target.value));
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
    mountedHost = null;
    activeCleanup?.();
  }
};
window.Analyzer = Analyzer;
for (const name of ["cw:overview-profile-changed", "cw:auth-state-changed"]) {
  window.addEventListener?.(name, () => {
    const host = mountedHost;
    if (!host || mountedContext === contextKey()) return;
    Analyzer.unmount();
    if (document.documentElement.dataset.tab !== "analyzer") return;
    Analyzer.mount(host).catch(() => {
      host.innerHTML = `<div class="cw-page-load-error">${TH("analyzer.pageLoadFailed")}</div>`;
    });
  });
}
document.addEventListener("tab-changed", event => {
  if (event.detail?.tab !== "analyzer") Analyzer.unmount();
});
export default Analyzer;
