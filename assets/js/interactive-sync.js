/* assets/js/interactive-sync.js */
/* CrossWatch - Interactive Sync Review Page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const API = "/api/interactive-sync";
  const host = document.getElementById("page-interactive_sync");
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  const ORIGINS = { settings: ["settings/sync", T("sync.review.origin.sync")], playlists: ["playlists", T("sync.review.origin.playlists")] };
  const origin = () => {
    if (session?.tracker_import) return [session.report ? `tracker?instance=${encodeURIComponent(session.pair.target_instance || "default")}` : "tracker", T("sync.review.origin.tracker")];
    try { return ORIGINS[sessionStorage.getItem("cw.interactive_sync.origin")] || ORIGINS.settings; } catch { return ORIGINS.settings; }
  };
  const labels = { add: T("sync.review.op.add"), remove: T("sync.review.op.remove"), update: T("sync.review.op.update"), tag_dropped: T("sync.review.op.markDropped"), rewatch_reset: T("sync.review.op.markUnwatched"), hide_continue_watching: T("sync.review.op.hideContinue"), unresolved: T("sync.review.needsAttention"), blocked: T("sync.review.blocked"), conflict: T("sync.review.conflict"), skipped: T("sync.review.skipped"), not_in_library: T("sync.review.notInLibrary") };
  let session = null, busy = false, timer = null, page = 0, route = "", generation = 0;
  let feature = "", result = "", query = "", mappingDirty = false, mappingWorkspace = null;
  let pageData = { items: [], total: 0 }, pageLoading = false, pageRequest = 0, pageController = null, searchTimer = null;
  let progressTimer = null, progressReceived = Date.now(), pollFailures = 0;
  let downloadOpen = false, downloadScope = "all", downloadFormat = "csv", downloading = false, noticesExpanded = null;
  const PAGE_SIZE = 75;
  const css = document.createElement("link");
  css.rel = "stylesheet";
  css.href = `/assets/css/interactive-sync.css?v=${encodeURIComponent(window.APP_VERSION || "1")}`;
  document.head.append(css);

  async function json(url, options = {}) {
    const response = await fetch(url, { credentials: "same-origin", cache: "no-store", ...options });
    const data = await response.json();
    if (!response.ok || data.ok === false) {
      const error = new Error(data.detail || data.error || T("sync.review.requestFailed"));
      error.status = response.status;
      throw error;
    }
    return data;
  }
  const post = (url, body) => json(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const active = () => !host.classList.contains("hidden");
  const pending = () => busy || ["reading", "applying"].includes(session?.status);
  const title = item => item?.series_title && item?.episode != null ? `${item.series_title} · S${String(item.season || 0).padStart(2, "0")}E${String(item.episode).padStart(2, "0")}` : item?.title || item?.name || T("sync.review.untitled");
  let syncUserLabels = null;
  const syncUserName = (provider, instance) => {
    const inst = String(instance || "");
    const user = syncUserLabels?.[String(provider || "").toUpperCase()]?.[inst];
    if (user === undefined || !inst.includes("~")) return inst;
    const parent = inst.split("~")[0];
    return `${parent === "default" ? "" : `${parent} · `}${user || inst.split("~").slice(1).join("~")}`;
  };
  const endpoint = (provider, instance) => `${provider || T("sync.review.providerWord")}${instance && instance !== "default" ? ` · ${syncUserName(provider, instance)}` : ""}`;
  async function loadSyncUserLabels() {
    if (syncUserLabels) return;
    await window.CW?.loadSyncUserNames?.();
    try {
      const response = await fetch("/api/sync-user-labels", { credentials: "same-origin", cache: "no-store" });
      syncUserLabels = response.ok ? await response.json() : {};
    } catch {
      syncUserLabels = {};
    }
  }
  function value(item, kind) {
    if (!item) return "—";
    if (item._cw_status_mapping) return ({watching:T("sync.review.status.watching"),on_hold:T("sync.review.status.onHold"),dropped:T("sync.review.status.dropped"),completed:T("sync.review.status.completed")})[item.watch_status] || T("sync.review.status.none");
    if (kind === "ratings") return item.rating == null ? T("sync.review.unrated") : `${item.rating}/10`;
    if (kind === "progress") return item.progress_percent != null ? `${item.progress_percent}%` : `${Math.round(Number(item.progress_ms || 0) / 60000)} min`;
    if (kind === "history") return item.watched_at || T("sync.review.watched");
    if (kind === "collection") return item.collected_at || T("sync.review.collected");
    if (item.target_order_count != null) return T("sync.review.sourceOrder", {count: item.target_order_count});
    return T("sync.review.present");
  }
  function statusMappingHTML(rows = []) {
    if (!rows.length) return "";
    return `<section class="is-notices" aria-label="${TH("sync.review.statusMapping")}"><strong>${TH("sync.review.statusMapping")}</strong>${rows.map(row => {
      const state = row.state === "skipped" ? T("sync.review.state.notRun") : row.state === "checking" ? T("sync.review.state.checkIncomplete") : row.state === "attention" ? T("sync.review.state.needsReview") : row.preview || row.dry_run ? T("sync.review.state.previewChecked") : T("sync.review.state.checked");
      const counts = row.state === "skipped" ? "" : ["matched", "proposed", "updated", "unchanged", "excluded", "unmatched", "recovered", "not_selected", "unresolved", "errors"].map(key => [key, T(`sync.review.mapCount.${key}`)])
        .filter(([key]) => row[key] || key === "matched" || key === (row.preview || row.dry_run ? "proposed" : "updated"))
        .map(([key, label]) => key === "unresolved" && (session?.notices || []).length ? `<button class="is-link" data-action="show-notices">${number(row[key])} ${label}</button>` : `${number(row[key])} ${label}`).join(" · ");
      return `<p><strong>${esc(endpoint(row.src, row.src_instance))} ${row.mode === "two-way" ? "↔" : "→"} ${esc(endpoint(row.dst, row.dst_instance))}: ${state}</strong>${row.reason ? `<br>${esc(row.reason)}` : ""}${counts ? `<br>${counts}` : ""}</p>`;
    }).join("")}</section>`;
  }
  const duration = seconds => {
    const value = Math.max(0, Math.floor(seconds || 0));
    const h = Math.floor(value / 3600), m = Math.floor(value % 3600 / 60), s = value % 60;
    return h ? `${h}h ${String(m).padStart(2, "0")}m ${String(s).padStart(2, "0")}s` : `${m}m ${String(s).padStart(2, "0")}s`;
  };
  const number = value => Number(value || 0).toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale);
  const noticeSubject = n => n.provider ? `${n.provider}${n.side ? ` (${n.side})` : ""}` : n.route || "";
  const noticeReason = n => { const reason = String(n.reason || n.event || T("sync.review.providerNotice")); return reason.includes(" ") ? reason : reason.replaceAll("_", " ").replaceAll(":", ": "); };
  function noticesHTML(failedPlan, partialReady) {
    const notices = session?.notices || [];
    if (!notices.length) return "";
    const all = noticesExpanded ?? (!!failedPlan || !!partialReady);
    const heading = failedPlan ? T("sync.review.failureNotices") : T("sync.review.protections");
    const line = n => `<p><strong>${esc([n.feature, noticeSubject(n)].filter(Boolean).join(" / "))}</strong> ${esc(noticeReason(n))}${n.detail ? ` - ${esc(n.detail)}` : ""}${n.error ? `<br><strong>${TH("sync.review.error")}</strong> ${esc(n.error)}` : ""}</p>`;
    const toggle = `<button class="is-btn is-small" data-action="toggle-notices" aria-expanded="${all}">${all ? TH("sync.review.hideNotices") : TH("sync.review.showNotices", {count: notices.length})}</button>`;
    return `<section id="is-notices" class="is-notices" aria-label="${heading}"><div class="cw-page-notice-heading"><strong>${heading} (${number(notices.length)})</strong>${toggle}</div>${(all ? notices : []).map(line).join("")}</section>`;
  }
  function aiQuestion() {
    const pair = session?.pair || {};
    const route = `${endpoint(pair.source, pair.source_instance)} ${pair.mode === "two-way" ? "↔" : "→"} ${endpoint(pair.target, pair.target_instance)}`;
    const report = session?.report;
    if (report) {
      const t = report.totals || {};
      if (report.outcome === "success" && ![t.unresolved, t.errors, t.blocked, report.not_reached].some(n => Number(n) > 0)) return "";
      const notices = (report.notices || []).slice(0, 6).map(n => [n.feature, n.provider, n.reason, n.detail].filter(Boolean).join(" ")).join("; ");
      return `Explain this interactive sync report for ${route} (run ${report.run_id}). Outcome: ${report.outcome}. Added ${number(t.added)}, updated ${number(t.updated)}, removed ${number(t.removed)}, unresolved ${number(t.unresolved)}, provider errors ${number(t.errors)}, blocked ${number(t.blocked)}, not reached ${number(report.not_reached)}.${notices ? ` Notices: ${notices}.` : ""} What went wrong and what should I do?`;
    }
    const c = session?.counts || {};
    if (session?.status !== "review" || !(c.removals || c.conflicts || c.attention)) return "";
    const removing = (pageData.items || []).filter(row => row.result === "remove").slice(0, 8).map(row => title(row.item)).join(", ");
    return `I am reviewing an interactive sync for ${route} before applying it. It proposes ${number(c.changes)} changes: ${number(c.removals)} removals, ${number(c.conflicts)} conflicts and ${number(c.attention)} that need attention.${removing ? ` Removals include: ${removing}.` : ""} Explain why CrossWatch proposes these changes, especially any removals, and whether anything looks risky to apply.`;
  }
  function aiButton() {
    const question = aiQuestion();
    return question ? `<button class="is-btn" type="button" data-ai-analyze data-ai-question="${esc(question)}" title="${TH(session?.report ? "sync.review.askReport" : "sync.review.askReview")}"><span class="material-symbols-rounded" aria-hidden="true">auto_awesome</span>${TH("sync.review.explainAi")}</button>` : "";
  }
  function requestBreakdown(counts) {
    if (!counts) return "";
    const pair = session?.pair || {};
    const providers = new Map([pair.source, pair.target].filter(Boolean).map(provider => [provider, 0]));
    for (const [provider, count] of Object.entries(counts)) providers.set(provider, count);
    return [...providers].map(([provider, count]) => `${esc(provider)}: ${number(count)}`).join(" · ");
  }
  function progressHTML() {
    const p = session?.progress || {};
    const running = ["reading", "applying"].includes(session?.status);
    const recheck = session?.apply_review;
    if (!running) return `<div class="is-status ${recheck ? "is-recheck" : ""}" role="status"><span class="material-symbols-rounded" aria-hidden="true">${session?.status === "error" ? "error" : recheck ? "manage_search" : "fact_check"}</span><div><strong>${esc(session?.message || T("sync.review.preparing"))}</strong>${recheck ? `<small>${TH("sync.review.recheckSummary", {requested: Number(recheck.requested || 0), retained: Number(recheck.retained || 0), review: Number(recheck.needs_review || 0)})}</small><small>${TH("sync.review.changedUnchecked")}</small>` : ""}${p.elapsed_seconds ? `<small>${TH("sync.review.elapsedValue", {time: duration(p.elapsed_seconds)})}</small>` : ""}</div></div>`;
    const applying = p.operation === "apply";
    const stages = applying ? [["verifying", T("sync.review.stage.recheck")], ["applying", T("sync.review.stage.apply")], ["finalizing", T("sync.review.stage.save")]] : [["connecting", T("sync.review.stage.connect")], ["reading", T("sync.review.stage.read")], ["planning", T("sync.review.stage.build")], ["finalizing", T("sync.review.stage.prepare")]];
    const index = Math.max(0, stages.findIndex(([key]) => key === p.stage));
    const determinate = p.percent != null && p.total > 0;
    const detailsOpen = host.querySelector(".is-progress-details")?.open;
    const recent = (p.recent || []).slice().reverse();
    const clock = row => esc(new Date(row.at * 1000).toLocaleTimeString((window.CW?.I18n || window.__cwI18n)?.locale));
    return `<section class="is-run-progress" aria-label="${TH("sync.review.progressLabel")}">
      <div class="is-progress-top"><div><div class="is-eyebrow">${applying ? T("sync.review.applying") : T("sync.review.preparingCaps")}</div><h2>${esc(p.label || T("sync.review.checking"))}</h2></div><span class="is-live"><i></i><span data-progress-connection>${TH("sync.review.connected")}</span></span></div>
      <ol class="is-progress-stages">${stages.map(([key, label], i) => `<li class="${i === index ? "current" : i < index ? "finished" : ""}" ${i === index ? 'aria-current="step"' : ""}><span>${i < index ? '<span class="material-symbols-rounded" aria-hidden="true">check</span>' : i + 1}</span>${label}</li>`).join("")}</ol>
      <div class="is-progress-caption"><strong>${determinate ? TH("sync.review.doneOf", {done: Number(p.done || 0), total: Number(p.total || 0), unit: p.unit || ""}) : p.done ? `${number(p.done)} ${esc(p.unit)}` : ["reading", "verifying"].includes(p.stage) ? T("sync.review.waitingTotals") : T("sync.review.workingStage")}</strong><span>${determinate ? `${p.percent}%` : T("sync.review.inProgress")}</span></div>
      <div class="is-progress-track ${determinate ? "" : "indeterminate"}" role="progressbar" aria-label="${esc(p.label || T("sync.review.currentActivityLabel"))}" aria-valuemin="0" aria-valuemax="100" ${determinate ? `aria-valuenow="${p.percent}"` : `aria-valuetext="${TH("sync.review.inProgressNoTotal")}"`}><span style="width:${determinate ? p.percent : 30}%"></span></div>
      <div class="is-progress-stats"><div><span>${TH("sync.review.elapsed")}</span><strong data-progress-elapsed>${duration(p.elapsed_seconds)}</strong></div><div><span>${TH("sync.review.currentActivity")}</span><strong data-progress-stage>${duration(p.stage_seconds)}</strong></div><div><span>${TH("sync.review.itemsRead")}</span><strong>${number(p.items_read)}</strong></div><div><span>${TH("sync.review.apiRequests")}</span><strong>${number(p.requests)}</strong><small>${requestBreakdown(p.requests_by_provider)}</small></div></div>
      <div class="is-progress-note"><span class="material-symbols-rounded" aria-hidden="true">info</span><p><span data-progress-quiet>${TH("sync.review.waitingUpdate")}</span><br>${TH("sync.review.continuesServer")}</p></div>
      ${recent.length ? `<details class="is-progress-details" ${detailsOpen ? "open" : ""}><summary aria-label="${TH("sync.review.recentActivity")}"><span class="is-latest-label">${TH("sync.review.latest")}</span><time>${clock(recent[0])}</time><span class="is-latest-text">${esc(recent[0].text)}</span><span class="is-latest-toggle">${recent.length > 1 ? TH("sync.review.allCount", {count: recent.length}) : T("sync.review.details")}<span class="material-symbols-rounded" aria-hidden="true">expand_more</span></span></summary><ol>${recent.map(row => `<li><time>${clock(row)}</time><span>${esc(row.text)}</span></li>`).join("")}</ol></details>` : ""}
    </section>`;
  }
  function updateProgressClock() {
    if (!active()) return;
    const p = session?.progress || {};
    const extra = p.running ? Math.floor((Date.now() - progressReceived) / 1000) : 0;
    const put = (selector, value) => { const node = host.querySelector(selector); if (node && node.textContent !== value) node.textContent = value; };
    put("[data-progress-elapsed]", duration((p.elapsed_seconds || 0) + extra));
    put("[data-progress-stage]", duration((p.stage_seconds || 0) + extra));
    put("[data-progress-connection]", pollFailures ? T("sync.review.reconnecting") : T("sync.review.connected"));
    host.querySelector(".is-live")?.classList.toggle("reconnecting", pollFailures > 0);
    const quiet = (p.quiet_seconds || 0) + extra;
    put("[data-progress-quiet]", pollFailures ? T("sync.review.interrupted") : quiet >= 60 ? T("sync.review.lastActivity", {time: duration(quiet)}) : T("sync.review.autoUpdates"));
  }
  function accept(data, forcePage = false) {
    const changed = !session || session.id !== data.id || session.revision !== data.revision || session.selection_version !== data.selection_version || session.status !== data.status;
    if (data.report && (!session?.report || session.id !== data.id)) {
      page = 0; feature = ""; result = ""; query = ""; pageData = {items: [], total: 0};
    }
    session = data;
    progressReceived = Date.now();
    pollFailures = 0;
    if (!changed && ["reading", "applying"].includes(data.status) && host.querySelector(".is-progress-host")) host.querySelector(".is-progress-host").innerHTML = progressHTML();
    else render();
    updateProgressClock();
    schedule();
    if (mappingDirty && data.status === "review") { mappingDirty = false; action("refresh", { reread: false }); }
    else if (active() && !pending() && data.revision && (changed || forcePage)) loadPage();
  }
  function invalidatePage() {
    pageRequest += 1;
    pageController?.abort();
    clearTimeout(searchTimer);
  }
  async function loadPage() {
    if (!session?.revision || pending() || !active()) return;
    if (session.report && !session.report.issue_count) return;
    invalidatePage();
    const request = pageRequest, current = generation, sid = session.id;
    pageController = new AbortController();
    pageLoading = true;
    render();
    const params = new URLSearchParams({ revision: session.revision, offset: page * PAGE_SIZE, limit: PAGE_SIZE, feature, result, q: query });
    try {
      const data = await json(`${API}/${sid}/${session.report ? "report-issues" : "rows"}?${params}`, { signal: pageController.signal });
      if (request !== pageRequest || current !== generation) return;
      pageData = data;
      page = Math.floor(data.offset / PAGE_SIZE);
      if (!session.report) {
        session.counts = data.counts;
        session.selection_version = data.selection_version;
      }
      pageLoading = false;
      render();
    } catch (error) {
      if (error.name === "AbortError" || request !== pageRequest || current !== generation) return;
      pageLoading = false;
      render();
      if (error.status === 409) poll(); else showError(error);
    }
  }
  function schedule() {
    clearTimeout(timer);
    clearInterval(progressTimer);
    if (active() && ["reading", "applying"].includes(session?.status)) {
      timer = setTimeout(poll, Math.min(15000, 1200 * 2 ** Math.min(pollFailures, 4)));
      progressTimer = setInterval(updateProgressClock, 1000);
    }
  }
  async function poll() {
    if (!session || !active()) return;
    const current = generation;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const data = await json(`${API}/${session.id}`, { signal: controller.signal });
      if (current === generation) accept(data, true);
    } catch (error) {
      if (current === generation) {
        if (error.status === 401 || error.status === 403 || error.status === 404) {
          session.status = "error"; session.message = T("sync.review.progressUnavailable"); render(); showError(error); clearInterval(progressTimer);
        } else {
          pollFailures += 1;
          updateProgressClock();
          schedule();
        }
      }
    } finally { clearTimeout(timeout); }
  }
  function showError(error) {
    const box = host.querySelector(".is-error");
    if (box) { box.textContent = error.message || String(error); box.hidden = false; }
  }
  async function downloadReview() {
    if (!session || pending() || pageLoading || downloading || session.status !== "review") return;
    const sid = session.id, revision = session.revision, current = generation;
    const format = downloadFormat, scope = downloadScope;
    const params = new URLSearchParams({ revision, selection_version: session.selection_version, format, scope });
    if (scope === "filtered") {
      params.set("feature", feature); params.set("result", result); params.set("q", query);
    }
    downloading = true;
    render();
    try {
      const response = await fetch(`${API}/${sid}/export?${params}`, { credentials: "same-origin", cache: "no-store" });
      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || T("sync.review.downloadFailed"));
      }
      const blob = await response.blob();
      if (current !== generation) return;
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `crosswatch-review-${sid}-r${revision}-${scope}.${format}`;
      document.body.append(link); link.click(); link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (error) {
      if (current === generation) {
        downloading = false;
        render();
        showError(error);
      }
      return;
    } finally {
      downloading = false;
    }
    if (current === generation) {
      render();
      host.querySelector('[data-action="download-review"]')?.focus();
    }
  }
  function matchDetailsHTML(row) {
    const details = row.match_details;
    if (!details || !Array.isArray(details.candidates) || !details.candidate_count) return "";
    const ids = item => ["ids", "show_ids"].flatMap(field => Object.entries(item?.[field] || {}).map(([key, value]) => `${field === "show_ids" ? "show " : ""}${key}:${value}`)).join(" / ");
    return `<div><strong>${TH(row.reason_origin === "previous_attempt" ? "sync.review.matchesPrevious" : "sync.review.matches", {count: String(details.candidate_count)})}</strong><p>${TH("sync.review.sourceIds", {value: `${row.item?.type || ""} ${ids(row.item)}`})}</p><ul>${details.candidates.slice(0, 10).map(item => `<li>${esc([item.type, item.local_id, item.title, item.year].filter(Boolean).join(" / "))}<small>${esc(ids(item))}${item.season != null && item.episode != null ? ` / S${esc(item.season)}E${esc(item.episode)}` : ""}</small></li>`).join("")}</ul>${details.truncated ? "<p>" + TH("sync.review.firstCandidates") + "</p>" : ""}</div>`;
  }
  function previousAttemptHTML(row) {
    const ts = row.previous_attempt?.ts;
    if (row.reason_origin !== "previous_attempt" || typeof ts !== "number") return "";
    const date = new Date(ts * 1000);
    return Number.isFinite(date.getTime()) ? `<small>${TH("sync.review.previousAttempt", {time: date.toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale)})}</small>` : "";
  }
  function reasonDetailsHTML(row) {
    const message = row.explanation || row.reason || "";
    const label = row.reason_label || message;
    const summary = label.length <= 80 ? label : T("sync.review.needsAttention");
    const previous = row.reason_origin === "previous_attempt";
    const codes = previous ? row.previous_attempt : row.explanation || row.reason_label ? row : null;
    const candidates = matchDetailsHTML(row);
    const timestamp = previousAttemptHTML(row);
    if (!message && !candidates) return "";
    const detail = message !== summary || codes?.hint || candidates || timestamp;
    return `<small class="is-reason">${esc(summary)}</small>${detail ? `<details><summary>${TH("sync.review.details")}</summary><p>${esc(message)}</p>${codes?.reason ? `<small>${esc(codes.reason)}</small>` : ""}${codes?.hint ? `<small>${esc(codes.hint)}</small>` : ""}${timestamp}${candidates}</details>` : ""}`;
  }
  function reportIssuesHTML() {
    const report = session.report;
    if (!report.issue_count && !report.issue_details_omitted) return "";
    const pages = Math.max(1, Math.ceil(pageData.total / PAGE_SIZE));
    return `<section class="is-report-section"><h2>${TH("sync.review.itemsAttention", {count: Number(report.issue_count || 0)})}</h2><p>${TH("sync.review.itemDetailsHint")}</p>
      ${report.issue_details_omitted ? `<p class="is-report-warning">${TH("sync.review.omitted", {count: Number(report.issue_details_omitted || 0)})}</p>` : ""}
      <div class="is-toolbar"><label class="is-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input data-filter="query" type="search" maxlength="256" placeholder="${TH("sync.review.searchReport")}" aria-label="${TH("sync.review.searchReportLabel")}" value="${esc(query)}"></label><label>${TH("sync.review.feature")}<select data-filter="feature"><option value="">${TH("sync.review.allFeatures")}</option>${session.report.features.map(row => `<option value="${esc(row.feature)}" ${feature === row.feature ? "selected" : ""}>${esc(row.feature)}</option>`).join("")}</select></label><label>${TH("sync.review.result")}<select data-filter="result"><option value="">${TH("sync.review.allResults")}</option>${Object.entries({failed:T("sync.review.notConfirmed"),unresolved:T("sync.review.unresolved"),blocked:T("sync.review.blocked")}).map(([key,label]) => `<option value="${key}" ${result === key ? "selected" : ""}>${label}</option>`).join("")}</select></label></div>
      <p class="is-report-item-range" role="status">${pageLoading ? T("sync.review.loading") : TH("sync.review.itemRange", {start: pageData.total ? page * PAGE_SIZE + 1 : 0, end: Math.min((page + 1) * PAGE_SIZE, pageData.total), total: Number(pageData.total || 0)})}</p><div class="is-table-wrap"><table class="is-table is-report-items"><thead><tr><th>${TH("sync.review.item")}</th><th>${TH("sync.review.featureDestination")}</th><th>${TH("sync.review.result")}</th><th>${TH("sync.review.reason")}</th></tr></thead><tbody>${pageLoading ? `<tr><td colspan="4">${TH("sync.review.loadingItems")}</td></tr>` : pageData.items.map(row => `<tr><td><strong>${esc(title(row.item))}</strong><small>${esc(row.key || Object.entries(row.item?.ids || {}).map(([key,value]) => `${key}:${value}`).join(" · "))}</small></td><td>${esc(row.feature)}<small>${esc(endpoint(row.provider,row.instance))} · ${esc(labels[row.operation] || row.operation)}</small></td><td><span class="is-badge ${row.result === "blocked" ? "blocked" : "unresolved"}">${row.result === "blocked" ? T("sync.review.blocked") : row.result === "unresolved" ? T("sync.review.unresolved") : T("sync.review.notConfirmed")}</span></td><td class="is-report-reason">${reasonDetailsHTML(row)}</td></tr>`).join("") || `<tr><td colspan="4">${TH("sync.review.noMatching")}</td></tr>`}</tbody></table></div>
      <nav class="is-pagination" aria-label="${TH("sync.review.reportPages")}"><button class="is-btn is-small" data-action="first" ${pageLoading || !page ? "disabled" : ""}>${TH("sync.review.first")}</button><button class="is-btn is-small" aria-label="${TH("sync.review.previousPage")}" data-action="prev" ${pageLoading || !page ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span><span>${TH("sync.review.previous")}</span></button><label>${TH("sync.review.page")} <input type="number" data-page min="1" max="${pages}" value="${page + 1}" ${pageLoading ? "disabled" : ""}> ${TH("sync.review.ofPages", {pages})}</label><button class="is-btn is-small" aria-label="${TH("sync.review.nextPage")}" data-action="next" ${pageLoading || page + 1 >= pages ? "disabled" : ""}><span>${TH("sync.review.next")}</span><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button><button class="is-btn is-small" data-action="last" ${pageLoading || page + 1 >= pages ? "disabled" : ""}>${TH("sync.review.last")}</button></nav></section>`;
  }
  function closeButtonHTML() {
    return `<button class="is-btn" data-action="discard" ${pending() || !session ? "disabled" : ""}>${session?.report ? T("sync.review.closeReport") : T("sync.review.closeReview")}</button>`;
  }
  function reportHTML() {
    const report = session.report, totals = report.totals;
    const outcomes = {
      success: ["check_circle", session?.tracker_import ? T("sync.review.importCompleted") : T("sync.review.syncCompleted"), report.requested === 0 ? T("sync.review.noChangesApplied") : T("sync.review.engineFinished")],
      attention: ["warning", T("sync.review.finishedAttention"), T("sync.review.someAttention")],
      cancelled: ["pause_circle", T("sync.review.cancelled"), T("sync.review.keptApplied")],
      incomplete: ["error", T("sync.review.didNotFinish"), T("sync.review.availableBefore")]
    };
    const [icon, heading, description] = outcomes[report.outcome] || outcomes.incomplete;
    const changes = totals.added + totals.updated + totals.removed;
    const columns = [["added", T("sync.review.added")], ["updated", T("sync.review.updated")], ["removed", T("sync.review.removed")], ["skipped", T("sync.review.skipped")], ["unresolved", T("sync.review.unresolved")], ["blocked", T("sync.review.blocked")], ["errors", T("sync.review.errors")]];
    const cells = row => columns.map(([key]) => `<td>${row[key] == null ? "—" : number(row[key])}</td>`).join("");
    const date = value => value ? esc(new Date(value).toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale)) : "—";
    return `<section class="is-report" aria-label="${TH("sync.review.completionReport")}">
      <div class="is-report-outcome ${esc(report.outcome)}" role="status"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span><div><h2>${heading}</h2><p>${description}</p></div><span class="is-report-duration">${duration(report.duration_seconds)}</span></div>
      <div class="is-metrics is-report-metrics">${columns.slice(0, 4).map(([key, label]) => `<div><strong>${number(totals[key])}</strong><span>${label}</span></div>`).join("")}</div>
      ${statusMappingHTML(report.status_mappings)}
      ${changes ? `<div class="is-report-distribution" role="img" aria-label="${TH("sync.review.distribution", {added: Number(totals.added || 0), updated: Number(totals.updated || 0), removed: Number(totals.removed || 0)})}">${["added", "updated", "removed"].map(key => `<span class="${key}" style="width:${totals[key] / changes * 100}%"></span>`).join("")}</div><div class="is-report-legend">${columns.slice(0, 3).map(([key, label]) => `<span><i class="${key}"></i>${label}</span>`).join("")}</div>` : ""}
      <dl class="is-report-facts"><div><dt>${TH("sync.review.selectedForRun")}</dt><dd>${TH("sync.review.ofProposals", {requested: Number(report.requested || 0), proposed: Number(report.proposed || 0)})}</dd></div><div><dt>${TH("sync.review.notSelected")}</dt><dd>${number(report.not_selected)}</dd></div><div><dt>${TH("sync.review.started")}</dt><dd>${date(report.started_at)}</dd></div><div><dt>${TH("sync.review.finished")}</dt><dd>${date(report.finished_at)}</dd></div><div><dt>${TH("sync.review.apiRequests")}</dt><dd>${number(report.requests)}<br><small>${requestBreakdown(report.requests_by_provider)}</small></dd></div><div><dt>${TH("sync.review.conflictDecisions")}</dt><dd>${number(report.conflicts_reviewed)}</dd></div></dl>
      ${(report.cloud_results || []).length ? `<section class="is-report-section"><h2>${TH("sync.review.plexFallback")}</h2><p>${TH("sync.review.cloudHint")}</p>${report.cloud_results.length > 50 ? `<p>${TH("sync.review.cloudFirst50", {count: report.cloud_results.length})}</p>` : ""}<div class="is-table-wrap"><table class="is-table is-cloud-results"><thead><tr><th scope="col">${TH("sync.review.item")}</th><th scope="col">${TH("sync.review.account")}</th><th scope="col">${TH("sync.review.result")}</th></tr></thead><tbody>${report.cloud_results.slice(0, 50).map(row => `<tr><td>${esc(row.title)}<small>${esc(row.key)}</small></td><td>${esc(endpoint(row.provider, row.instance))}</td><td>${esc(({dated:T("sync.review.cloud.dated"),present:T("sync.review.cloud.present"),cached:T("sync.review.cloud.cached"),plex_discover_date_pending:T("sync.review.cloud.pending")})[row.outcome] || row.reason || T("sync.review.cloudOutcome", {outcome: row.outcome}))}</td></tr>`).join("")}</tbody></table></div></section>` : ""}
      ${report.accepted_unverified ? `<p>${TH("sync.review.unverified", {count: Number(report.accepted_unverified || 0)})}</p>` : ""}
      <section class="is-report-section"><h2>${TH("sync.review.byFeature")}</h2><p>${TH("sync.review.byFeatureHint")}</p><div class="is-table-wrap"><table class="is-table is-report-table"><thead><tr><th scope="col">${TH("sync.review.featureDestination")}</th>${columns.map(([, label]) => `<th scope="col">${label}</th>`).join("")}</tr></thead><tbody>${report.features.map(row => `<tr class="is-report-feature"><th scope="row">${esc(row.feature.charAt(0).toUpperCase() + row.feature.slice(1))}</th>${cells(row)}</tr>${row.destinations.map(dst => `<tr><th scope="row" class="is-report-destination">${esc(endpoint(dst.provider, dst.instance))}</th>${cells(dst)}</tr>`).join("")}`).join("") || `<tr><td colspan="8">${TH("sync.review.noFeatureResults")}</td></tr>`}</tbody></table></div></section>
      ${report.manual_excluded ? `<section class="is-report-section" aria-label="${TH("sync.review.manualExclusions")}"><h2>${TH("sync.review.manualExclusions")}</h2><p>${TH("sync.review.manualExcluded", {count: Number(report.manual_excluded || 0)})}</p><dl class="is-report-facts">${report.features.filter(row => row.manual_excluded).map(row => `<div><dt>${esc(row.feature.charAt(0).toUpperCase() + row.feature.slice(1))}</dt><dd>${TH("sync.review.excludedCount", {count: Number(row.manual_excluded || 0)})}</dd></div>`).join("")}</dl></section>` : ""}
      <section class="is-report-section is-report-attention"><h2>${TH("sync.review.attention")}</h2><div class="is-report-checks">${[[T("sync.review.unresolvedItems"), totals.unresolved], [T("sync.review.providerErrors"), totals.errors], [T("sync.review.blockedRules"), totals.blocked], [T("sync.review.notReached"), report.not_reached]].map(([label, n]) => `<div class="${n ? "needs-attention" : ""}"><span>${label}</span><strong>${number(n)}</strong></div>`).join("")}</div>${report.not_reached ? `<p>${TH("sync.review.notReachedHint")}</p>` : ""}${report.incomplete_note ? `<p class="is-report-warning">${esc(report.incomplete_note)}</p>` : ""}${report.outcome !== "success" ? `<p>${TH(report.issue_count ? "sync.review.reviewBelow" : "sync.review.noItemDetails")} ${TH("sync.review.startNew")}</p>` : ""}</section>
      ${(report.notices || []).length ? `<details class="is-notices" open><summary>${TH("sync.review.executionNotices", {count: report.notices.length})}</summary>${report.notices.map(n => `<p><strong>${esc([n.feature, noticeSubject(n)].filter(Boolean).join(" · "))}</strong> ${esc(n.reason)}${n.detail ? ` ${esc(n.detail)}` : ""}${n.occurrences > 1 ? ` ${TH("sync.review.occurrences", {count: Number(n.occurrences)})}` : ""}</p>`).join("")}${report.notice_overflow ? `<p>${TH("sync.review.moreInEvents")}</p>` : ""}</details>` : ""}
      ${(report.review_notices || []).length ? `<details class="is-notices"><summary>${TH("sync.review.previewNotices", {count: report.review_notices.length})}</summary>${report.review_notices.map(n => `<p>${esc([n.feature, noticeSubject(n)].filter(Boolean).join(" / "))} · ${esc(n.reason)}${n.detail ? ` · ${esc(n.detail)}` : ""}</p>`).join("")}</details>` : ""}
      ${reportIssuesHTML()}
      <p class="is-report-accounting">${esc(report.accounting_note)}</p>
      <footer class="is-footer is-report-footer"><div><strong>${TH("sync.review.keepCopy")}</strong><small>${TH("sync.review.keepCopyHint")}</small></div><div class="is-header-actions is-footer-actions">${closeButtonHTML()}<button class="is-btn" data-action="download-report"><span class="material-symbols-rounded" aria-hidden="true">download</span>${TH("sync.review.downloadJson")}</button><a class="is-btn is-primary" href="#${origin()[0]}">${TH("sync.review.backTo", {name: origin()[1]})}</a></div></footer>
    </section>`;
  }
  function render() {
    if (!host) return;
    const pair = session?.pair || {};
    const pages = Math.max(1, Math.ceil(pageData.total / PAGE_SIZE));
    const rows = pageData.items;
    const count = session?.counts?.changes || 0;
    const selectedCount = session?.counts?.selected || 0;
    const selectableCount = pageData.selectable || 0;
    const filteredSelected = pageData.selected || 0;
    const hiddenCount = session?.counts?.not_in_library || 0;
    const done = !!session?.report || session?.status === "complete";
    const partialReady = session?.status === "review" && (session?.failed_features || []).length > 0;
    const failedPlan = session?.status === "error" && !done;
    const dryRun = !!session?.dry_run && !done && !failedPlan;
    const failedFeatures = [...new Set((session?.notices || []).filter(n => n.event === "feature:error").map(n => [n.feature, noticeSubject(n)].filter(Boolean).join(" from ")).filter(Boolean))];
    const locked = pending() || pageLoading || done || session?.status !== "review";
    const summary = session?.summary || {};
    const focused = host.contains(document.activeElement) && document.activeElement?.dataset?.filter === "query";
    const cursor = focused ? document.activeElement.selectionStart : null;
    host.innerHTML = `<div class="is-page">
      <a class="is-back" href="#${origin()[0]}"><span class="material-symbols-rounded" aria-hidden="true">arrow_back</span>${origin()[1]}</a>
      <header class="is-header"><div><div class="is-eyebrow">${session?.tracker_import ? T("sync.review.trackerImport") : T("sync.review.interactiveSync")}</div><h1>${session?.tracker_import ? (session?.report ? T("sync.review.importReport") : T("sync.review.reviewImport")) : session?.report ? T("sync.review.syncReport") : failedPlan ? T("sync.review.partialPlan") : T("sync.review.reviewSync")}</h1></div><div class="is-header-actions">${aiButton()}${[pair.source,pair.target].includes("PLEX") ? `<button class="is-btn" data-action="recover-plex" title="${TH("sync.review.recoverHint")}" ${pending() ? "disabled" : ""}>${TH("sync.review.recoverPlex")}</button>` : ""}${session?.report ? "" : `<button class="is-btn" data-action="refresh" ${pending() || done || !session ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">refresh</span>${TH("sync.review.refreshPlan")}</button>`}${session?.report ? "" : `<button class="is-btn" data-action="toggle-download" aria-expanded="${downloadOpen}" aria-controls="is-download-options" ${locked ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">download</span>${TH("sync.review.downloadReview")}</button>`}${closeButtonHTML()}</div></header>
      ${downloadOpen && !done ? `<section id="is-download-options" class="is-notices" aria-label="${TH("sync.review.downloadReview")}"><div class="is-toolbar"><label>${TH("sync.review.scope")}<select data-download="scope" ${downloading ? "disabled" : ""}><option value="all" ${downloadScope === "all" ? "selected" : ""}>${TH("sync.review.entire")}</option><option value="filtered" ${downloadScope === "filtered" ? "selected" : ""}>${TH("sync.review.filtered")}</option></select></label><label>${TH("sync.review.format")}<select data-download="format" ${downloading ? "disabled" : ""}><option value="csv" ${downloadFormat === "csv" ? "selected" : ""}>CSV</option><option value="json" ${downloadFormat === "json" ? "selected" : ""}>JSON</option></select></label><button class="is-btn" data-action="download-review" ${locked || downloading ? "disabled" : ""}>${downloading ? T("sync.review.downloading") : T("sync.review.download")}</button></div><p>${TH("sync.review.downloadHint")}</p></section>` : ""}
      <div class="is-route"><span class="material-symbols-rounded" aria-hidden="true">sync_alt</span>${window.CW?.syncProfileRouteHTML?.(pair) || `<strong>${esc(endpoint(pair.source, pair.source_instance))}</strong><span>${pair.mode === "two-way" ? "↔" : "→"}</span><strong>${esc(endpoint(pair.target, pair.target_instance))}</strong>`}<span class="is-route-mode">${pair.mode === "two-way" ? T("sync.review.twoWay") : T("sync.review.oneWay")}</span></div>
      ${session?.report ? "" : `<div class="is-progress-host">${progressHTML()}</div>`}
      <div class="is-error" role="alert" hidden></div>
      ${session?.report ? reportHTML() : `<div class="is-review-content" ${["reading", "applying"].includes(session?.status) ? "hidden" : ""}>
      ${statusMappingHTML(session?.status_mappings)}
      ${partialReady ? `<section class="is-notices" role="alert"><h2>${TH("sync.review.someFailed")}</h2><p>${TH("sync.review.partialReady", {features: session.failed_features.join(", ")})}</p></section>` : ""}
      ${failedPlan ? `<section class="is-notices" role="alert" aria-label="${TH("sync.review.incompletePlanLabel")}"><h2>${TH("sync.review.partialTitle")}</h2><p>${failedFeatures.length ? TH("sync.review.planningFailedFor", {features: failedFeatures.join(", ")}) : TH("sync.review.collectedBefore")} ${TH("sync.review.notComplete")}</p><p>${TH("sync.review.applyDisabled")}</p>${(session.notices || []).length ? "" : "<p>" + TH("sync.review.noFailureNotice") + "</p>"}</section>` : ""}
      ${done ? `<div class="is-metrics">${[[T("sync.review.added"), summary.added], [T("sync.review.updated"), summary.updated], [T("sync.review.removed"), summary.removed], [T("sync.review.unresolved"), summary.unresolved], [T("sync.review.errors"), summary.errors], [T("sync.review.changedSince"), summary.not_applied]].map(([label, n]) => `<div><strong>${Number(n || 0)}</strong><span>${label}</span></div>`).join("")}</div>` : `<div class="is-metrics"><div><strong>${count}</strong><span>${failedPlan ? T("sync.review.partialProposals") : T("sync.review.proposedChanges")}</span></div><div><strong>${selectedCount}</strong><span>${TH("sync.review.selected")}</span></div><div><strong>${session?.counts?.conflicts || 0}</strong><span>${failedPlan ? T("sync.review.conflictsSoFar") : T("sync.review.conflictsToReview")}</span></div><div><strong>${failedPlan && !session?.counts?.attention ? T("sync.review.unknown") : session?.counts?.attention || 0}</strong><span>${failedPlan ? T("sync.review.checksIncomplete") : T("sync.review.needAttention")}</span></div></div>`}
      ${noticesHTML(failedPlan, partialReady)}
      <div class="is-toolbar"><label class="is-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input data-filter="query" type="search" maxlength="256" placeholder="${TH("sync.review.searchTitles")}" aria-label="${TH("sync.review.searchProposed")}" value="${esc(query)}"></label><label>${TH("sync.review.feature")}<select data-filter="feature"><option value="">${TH("sync.review.allFeatures")}</option>${(session?.features || []).map(f => `<option value="${esc(f)}" ${feature === f ? "selected" : ""}>${esc(f.charAt(0).toUpperCase() + f.slice(1))}</option>`).join("")}</select></label><label>${TH("sync.review.result")}<select data-filter="result"><option value="">${TH("sync.review.allResults")}</option>${Object.entries(labels).map(([k, v]) => `<option value="${k}" ${result === k ? "selected" : ""}>${v}</option>`).join("")}</select></label></div>
      <div class="is-list-actions"><span>${pageLoading ? T("sync.review.loadingPage") : TH("sync.review.matching", {count: Number(pageData.total || 0)})}</span>${hiddenCount ? `<button class="is-link" data-action="toggle-library" title="${TH("sync.review.notInLibraryHint")}">${result === "not_in_library" ? T("sync.review.backToProposed") : TH("sync.review.hiddenNotInLibrary", {count: hiddenCount})}</button>` : ""}${session?.tracker_import ? "" : `<button class="is-btn is-small" data-action="map-selected" ${locked || !selectedCount ? "disabled" : ""} title="${TH("sync.review.editCheckedHint")}">${TH("sync.review.editSelected")}</button>`}<div class="is-header-actions"><button class="is-btn is-small is-primary" data-action="select" title="${TH("sync.review.selectAllHint")}" ${locked || !selectableCount || filteredSelected === selectableCount ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">select_all</span>${TH(feature || result || query ? "sync.review.selectAllFiltered" : "sync.review.selectAll")}</button><button class="is-btn is-small" data-action="deselect" title="${TH("sync.review.deselectAllHint")}" ${locked || !filteredSelected ? "disabled" : ""}>${TH(feature || result || query ? "sync.review.deselectAllFiltered" : "sync.review.deselectAll")}</button></div></div>
      <div class="is-table-wrap" aria-busy="${pageLoading}"><table class="is-table"><thead><tr><th scope="col"><input type="checkbox" data-select-all aria-label="${TH("sync.review.selectAllMatching")}" title="${TH("sync.review.selectToggleHint")}" ${selectableCount > 0 && filteredSelected === selectableCount ? "checked" : ""} ${locked || !selectableCount ? "disabled" : ""}></th><th scope="col">${TH("sync.review.item")}</th><th scope="col">${TH("sync.review.feature")}</th><th scope="col">${TH("sync.review.proposedChange")}</th><th scope="col">${TH("sync.review.review")}</th></tr></thead><tbody>${rows.map(row => {
        if (row.result === "conflict") return `<tr class="is-conflict"><td><span class="material-symbols-rounded" aria-hidden="true">compare_arrows</span></td><td><strong>${esc(title(row.left))}</strong><small>${row.left?._cw_status_mapping ? T("sync.review.titleStatus") : esc(row.key)}</small></td><td>${esc(row.feature)}</td><td><span class="is-badge conflict">${TH("sync.review.conflict")}</span><small>${esc(row.source)}: ${esc(value(row.left, row.feature))} · ${esc(row.target)}: ${esc(value(row.right, row.feature))}</small></td><td><label class="is-sr-label" for="choice-${row.id}">${TH("sync.review.useValueFrom")}</label><select id="choice-${row.id}" data-choice="${row.id}" ${locked ? "disabled" : ""}>${row.left?._cw_status_mapping ? `<option value="" ${!row.winner ? "selected" : ""}>${TH("sync.review.leaveUnchanged")}</option>` : ""}${[row.source, row.target].map(p => `<option value="${esc(p)}" ${p === row.winner ? "selected" : ""}>${TH("sync.review.useProvider", {provider: p})}</option>`).join("")}</select></td></tr>`;
        return `<tr><td><input type="checkbox" data-select="${row.id}" aria-label="${TH("sync.review.selectItem", {title: title(row.item)})}" ${row.selected ? "checked" : ""} ${locked || !row.selectable ? "disabled" : ""}></td><td><strong>${esc(title(row.item))}</strong><small>${esc(row.item.type || "")} ${esc(row.item.year || "")} · ${esc(row.key)}</small>${reasonDetailsHTML(row)}</td><td>${esc(row.feature)}</td><td><span class="is-badge ${esc(row.result)}">${row.reason_origin === "previous_attempt" ? T("sync.review.previousFailure") : labels[row.result] || esc(row.result)}</span><span class="is-destination">${esc(endpoint(row.provider, row.instance))}</span>${row.destination_label ? `<small>${esc(row.destination_label)}</small>` : ""}<small>${esc(value(row.before, row.feature))} → ${row.operation === "remove" ? T("sync.review.removed") : row.operation === "tag_dropped" ? T("sync.review.status.dropped") : row.operation === "rewatch_reset" ? T("sync.review.unwatchedReset") : row.operation === "hide_continue_watching" ? T("sync.review.hiddenContinue") :esc(value(row.item, row.feature))}</small></td><td>${!session?.tracker_import && ["add", "update"].includes(row.operation) && row.feature !== "playlists" && !row.item?._cw_status_mapping ? `<button class="is-btn is-small" data-map="${row.id}" ${locked ? "disabled" : ""}>${TH("sync.review.mapping")}</button>` : "—"}</td></tr>`;
      }).join("") || `<tr><td colspan="5"><div class="is-empty"><span class="material-symbols-rounded" aria-hidden="true">${pending() ? "sync" : failedPlan ? "warning" : "done_all"}</span><strong>${pending() ? T("sync.review.readingProviders") : failedPlan ? (count || session?.counts?.conflicts ? T("sync.review.noPartialMatch") : T("sync.review.noPartial")) : count || session?.counts?.conflicts ? T("sync.review.noItemsMatch") : T("sync.review.noChanges")}</strong><p>${pending() ? T("sync.review.rulesChecked") : T("sync.review.refreshAfter")}</p></div></td></tr>`}</tbody></table></div>
      <div class="is-pagination"><button class="is-btn is-small" data-action="first" ${pageLoading || page === 0 ? "disabled" : ""}>${TH("sync.review.first")}</button><button class="is-btn is-small" aria-label="${TH("sync.review.previousPage")}" data-action="prev" ${pageLoading || page === 0 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span><span>${TH("sync.review.previous")}</span></button><label>${TH("sync.review.page")} <input data-page type="number" min="1" max="${pages}" value="${page + 1}" aria-label="${TH("sync.review.pageNumber")}" ${pageLoading ? "disabled" : ""}> ${TH("sync.review.ofPages", {pages})}</label><button class="is-btn is-small" aria-label="${TH("sync.review.nextPage")}" data-action="next" ${pageLoading || page + 1 >= pages ? "disabled" : ""}><span>${TH("sync.review.next")}</span><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button><button class="is-btn is-small" data-action="last" ${pageLoading || page + 1 >= pages ? "disabled" : ""}>${TH("sync.review.last")}</button></div>
      <footer class="is-footer"><div><strong>${done ? T("sync.review.opFinished") : failedPlan ? T("sync.review.incompletePlan") : dryRun ? T("sync.review.dryRunOn") : TH("sync.review.changesSelected", {count: selectedCount})}</strong><small>${done ? T("sync.review.resultsInEvents") : failedPlan ? T("sync.review.applyNeedsComplete") : dryRun ? T("sync.review.dryRunHint") : session?.tracker_import ? T("sync.review.importOnly") : T("sync.review.mappingsSaved")}</small></div><div class="is-footer-actions">${closeButtonHTML()}${done ? `<a class="is-btn is-primary" href="#${origin()[0]}">${TH("sync.review.backTo", {name: origin()[1]})}</a>` : `<button class="is-btn is-primary" data-action="apply" ${locked || !selectedCount ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">check</span>${TH("sync.review.applySelected", {count: selectedCount})}</button>`}</div></footer>
      </div>`}
    </div>`;
    const selectAll = host.querySelector("[data-select-all]");
    if (selectAll) selectAll.indeterminate = filteredSelected > 0 && filteredSelected < selectableCount;
    if (focused) {
      const input = host.querySelector('[data-filter="query"]');
      input.focus(); input.setSelectionRange(cursor, cursor);
    }
  }
  async function action(name, body = {}) {
    if (!session || pending() || (pageLoading && ["selection", "apply", "mapping"].includes(name))) return;
    if (name === "apply" && session.dry_run) {
      const count = Number(session.counts?.selected || 0);
      if (!count || !(await (window.CW?.MobileUI?.ask || window.confirm)(T("sync.review.dryRunConfirm", {count})))) return;
      body = { ...body, confirm_dry_run: true };
    }
    const current = generation, sid = session.id;
    const previousSelection = new Map();
    if (name === "selection" && body.ids) {
      const ids = new Set(body.ids);
      for (const row of pageData.items) if (ids.has(row.id)) {
        previousSelection.set(row.id, row.selected);
        row.selected = body.selected;
      }
    }
    invalidatePage();
    pageLoading = false;
    busy = true;
    render();
    try {
      const data = await post(`${API}/${sid}/${name}`, { revision: session.revision, selection_version: session.selection_version, ...body });
      if (current !== generation) return;
      busy = false;
      accept(data);
    } catch (error) {
      if (current !== generation) return;
      for (const row of pageData.items) if (previousSelection.has(row.id)) row.selected = previousSelection.get(row.id);
      busy = false; render(); showError(error);
      if (error.status === 409) poll();
    }
  }
  async function mapping(row) {
    if (session?.tracker_import || mappingWorkspace || pending() || pageLoading) return;
    const sid = session.id, revision = session.revision, selectionVersion = session.selection_version;
    mappingWorkspace = {};
    try {
      const {openMappingWorkspace} = await import(`/assets/js/interactive-sync-mapping.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
      const data = row ? {items:[row], total:1} : await json(`${API}/${sid}/rows?${new URLSearchParams({revision, limit:200, selected_only:true, editable_only:true})}`);
      if (sid !== session?.id || revision !== session.revision) throw new Error(T("sync.review.planChanged"));
      if (!row && (data.selection_version !== selectionVersion || session.selection_version !== selectionVersion)) throw new Error(T("sync.review.selectionChanged"));
      const editable = data.items.filter(item => ["add", "update"].includes(item.operation) && item.feature !== "playlists" && !item.item?._cw_status_mapping);
      if (!editable.length) throw new Error(T("sync.review.checkFirst"));
      const {episodeGroupAction} = await import(`/assets/js/editor/episode-groups.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
      let groupsChanged = false;
      mappingWorkspace = openMappingWorkspace({rows:editable, total:data.total, session:{...session}, json, post,
        onEpisodeGroup: editable.length === 1 ? episodeGroupAction(editable[0], session.pair_id, () => { groupsChanged = true; }) : undefined,
        onClose: () => {
          mappingWorkspace = null;
          if (!groupsChanged || session?.id !== sid) return;
          if (active() && !pending()) action("refresh", { reread: false }); else mappingDirty = true;
        },
        onRemoved: () => {
          mappingWorkspace = null;
          if (session?.id !== sid) return;
          if (active() && !pending()) action("refresh", { reread: false }); else mappingDirty = true;
        },
        onSaved: data => { mappingWorkspace = null; if (session?.id === sid) accept(data, true); },
      });
    } catch (error) { mappingWorkspace = null; showError(error); }
  }
  host?.addEventListener("change", event => {
    const el = event.target;
    if (el.dataset.download === "scope") downloadScope = el.value;
    if (el.dataset.download === "format") downloadFormat = el.value;
    if (el.hasAttribute("data-select-all")) action("selection", { selected: el.checked, feature, result, q: query });
    else if (el.dataset.select) action("selection", { selected: el.checked, ids: [el.dataset.select] });
    if (el.dataset.filter === "feature") { feature = el.value; page = 0; loadPage(); }
    if (el.dataset.filter === "result") { result = el.value; page = 0; loadPage(); }
    if (el.hasAttribute("data-page")) {
      page = Math.max(0, Math.min(Math.ceil(pageData.total / PAGE_SIZE) - 1, Math.trunc(Number(el.value) || 1) - 1));
      loadPage();
    }
    if (el.dataset.choice) action("refresh", { choices: { [el.dataset.choice]: el.value } });
  });
  host?.addEventListener("input", event => {
    if (event.target.dataset.filter !== "query") return;
    query = event.target.value;
    invalidatePage();
    pageLoading = true;
    page = 0; render();
    searchTimer = setTimeout(loadPage, 300);
  });
  host?.addEventListener("keydown", event => {
    if (event.key === "Escape" && downloadOpen) {
      downloadOpen = false;
      render();
      host.querySelector('[data-action="toggle-download"]')?.focus();
    }
    if (event.key === "Enter" && event.target.hasAttribute("data-page")) { event.preventDefault(); event.target.blur(); }
  });
  host?.addEventListener("click", async event => {
    const button = event.target.closest("button");
    if (!button || button.disabled) return;
    if (button.dataset.map) return mapping(pageData.items.find(row => row.id === button.dataset.map));
    const name = button.dataset.action;
    if (name === "toggle-download") {
      downloadOpen = !downloadOpen;
      render();
      host.querySelector(downloadOpen ? '[data-download="scope"]' : '[data-action="toggle-download"]')?.focus();
      return;
    }
    if (name === "toggle-notices") {
      noticesExpanded = button.getAttribute("aria-expanded") !== "true";
      render();
      host.querySelector('[data-action="toggle-notices"]')?.focus();
      return;
    }
    if (name === "show-notices") {
      noticesExpanded = true;
      render();
      host.querySelector("#is-notices")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    if (name === "download-review") return downloadReview();
    if (name === "map-selected") return mapping();
    if (name === "download-report" && session.report) {
      const report = session.report, sid = session.id, issues = [];
      button.disabled = true;
      try {
        while (issues.length < (report.issue_count || 0)) {
          const data = await json(`${API}/${sid}/report-issues?offset=${issues.length}&limit=200`);
          if (!data.items.length) throw new Error(T("sync.review.readAllFailed"));
          issues.push(...data.items);
        }
      } catch (error) { button.disabled = false; showError(error); return; }
      const url = URL.createObjectURL(new Blob([JSON.stringify({...report, issues}, null, 2)], { type: "application/json" }));
      const link = document.createElement("a");
      link.href = url;
      link.download = `crosswatch-sync-${sid}.json`;
      document.body.append(link); link.click(); link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      button.disabled = false;
      return;
    }
    if (name === "recover-plex") {
      const pair = session?.pair || {};
      (window.CW ||= {}).pendingPlexRecovery = {source_instance:pair.source === "PLEX" ? pair.source_instance || "default" : pair.target_instance || "default"};
      window.openExporter({recovery:true});
      return;
    }
    if (name === "refresh") return action("refresh");
    if (name === "apply") return action("apply");
    if (name === "toggle-library") { result = result === "not_in_library" ? "" : "not_in_library"; page = 0; return loadPage(); }
    if (name === "select" || name === "deselect") return action("selection", { selected: name === "select", feature, result, q: query });
    if (name === "prev" || name === "next") { page += name === "prev" ? -1 : 1; loadPage(); }
    if (name === "first" || name === "last") { page = name === "first" ? 0 : Math.max(0, Math.ceil(pageData.total / PAGE_SIZE) - 1); loadPage(); }
    if (name === "discard") {
      try { await json(`${API}/${session.id}`, { method: "DELETE" }); location.hash = origin()[0]; }
      catch (error) { showError(error); }
    }
  });
  window.addEventListener("cw:anime-mappings-changed", () => {
    if (!session || session.report || session.status === "complete") return;
    if (active() && !pending()) action("refresh", { reread: false }); else mappingDirty = true;
  });
  document.addEventListener("tab-changed", event => {
    if (event.detail.tab !== "interactive_sync") { clearTimeout(timer); clearInterval(progressTimer); invalidatePage(); pageLoading = false; }
  });
  async function refresh() {
    if (!host || !active()) return;
    await loadSyncUserLabels();
    const next = location.hash;
    if (next === route && session) {
      route = `#interactive_sync?session=${encodeURIComponent(session.id)}`;
      history.replaceState(null, "", route);
      if (mappingDirty && !pending()) { mappingDirty = false; action("refresh", { reread: false }); }
      else if (!busy) poll();
      return;
    }
    if (next === route && busy) return;
    route = next;
    const current = ++generation;
    downloadOpen = false; downloadScope = "all"; downloadFormat = "csv"; noticesExpanded = null;
    clearTimeout(timer); clearInterval(progressTimer); pollFailures = 0;
    const params = new URLSearchParams(next.split("?")[1] || "");
    if (params.has("pair")) {
      try { sessionStorage.setItem("cw.interactive_sync.origin", ORIGINS[params.get("from")] ? params.get("from") : "settings"); } catch {}
    }
    invalidatePage();
    session = null; pageData = { items: [], total: 0 }; pageLoading = false; page = 0; feature = ""; result = ""; query = ""; busy = true;
    render();
    try {
      await window.__cwAuthBootstrapPromise;
      if (window.cwIsAuthSetupPending?.()) throw new Error(T("sync.review.setupFirst"));
      const sid = params.get("session");
      const pairId = params.get("pair");
      if (!sid && !pairId) throw new Error(T("sync.review.chooseRun"));
      const data = sid ? await json(`${API}/${encodeURIComponent(sid)}`) : await post(API, { pair_id: pairId });
      if (current !== generation) return;
      busy = false;
      if (active() && location.hash === next) {
        route = `#interactive_sync?session=${encodeURIComponent(data.id)}`;
        history.replaceState(null, "", route);
      }
      accept(data);
    } catch (error) { if (current === generation) { busy = false; render(); showError(error); } }
  }
  window.InteractiveSync = { refresh };
  refresh();
})();
