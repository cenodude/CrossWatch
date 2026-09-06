/* assets/js/interactive-sync.js */
/* CrossWatch - Interactive Sync Review Page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const API = "/api/interactive-sync";
  const host = document.getElementById("page-interactive_sync");
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  const labels = { add: "Add", remove: "Remove", update: "Update", unresolved: "Needs mapping", blocked: "Blocked", conflict: "Conflict" };
  let session = null, busy = false, timer = null, page = 0, route = "", generation = 0;
  let feature = "", result = "", query = "", mappingDirty = false;
  let pageData = { items: [], total: 0 }, pageLoading = false, pageRequest = 0, pageController = null, searchTimer = null;
  let progressTimer = null, progressReceived = Date.now(), pollFailures = 0;
  const PAGE_SIZE = 75;
  const css = document.createElement("link");
  css.rel = "stylesheet";
  css.href = `/assets/css/interactive-sync.css?v=${encodeURIComponent(window.APP_VERSION || "1")}`;
  document.head.append(css);

  async function json(url, options = {}) {
    const response = await fetch(url, { credentials: "same-origin", cache: "no-store", ...options });
    const data = await response.json();
    if (!response.ok || data.ok === false) {
      const error = new Error(data.detail || data.error || "Request failed");
      error.status = response.status;
      throw error;
    }
    return data;
  }
  const post = (url, body) => json(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const active = () => !host.classList.contains("hidden");
  const pending = () => busy || ["reading", "applying"].includes(session?.status);
  const title = item => item?.series_title && item?.episode != null ? `${item.series_title} · S${String(item.season || 0).padStart(2, "0")}E${String(item.episode).padStart(2, "0")}` : item?.title || item?.name || "Untitled item";
  const endpoint = (provider, instance) => `${provider || "Provider"}${instance && instance !== "default" ? ` · ${instance}` : ""}`;
  function value(item, kind) {
    if (!item) return "—";
    if (kind === "ratings") return item.rating == null ? "Unrated" : `${item.rating}/10`;
    if (kind === "progress") return item.progress_percent != null ? `${item.progress_percent}%` : `${Math.round(Number(item.progress_ms || 0) / 60000)} min`;
    if (kind === "history") return item.watched_at || "Watched";
    if (kind === "collection") return item.collected_at || "Collected";
    if (item.target_order_count != null) return `${item.target_order_count} items in source order`;
    return "Present";
  }
  const duration = seconds => {
    const value = Math.max(0, Math.floor(seconds || 0));
    const h = Math.floor(value / 3600), m = Math.floor(value % 3600 / 60), s = value % 60;
    return h ? `${h}h ${String(m).padStart(2, "0")}m ${String(s).padStart(2, "0")}s` : `${m}m ${String(s).padStart(2, "0")}s`;
  };
  const number = value => Number(value || 0).toLocaleString();
  function progressHTML() {
    const p = session?.progress || {};
    const running = ["reading", "applying"].includes(session?.status);
    const recheck = session?.apply_review;
    if (!running) return `<div class="is-status ${recheck ? "is-recheck" : ""}" role="status"><span class="material-symbols-rounded" aria-hidden="true">${session?.status === "error" ? "error" : recheck ? "manage_search" : "fact_check"}</span><div><strong>${esc(session?.message || "Preparing review...")}</strong>${recheck ? `<small>${number(recheck.requested)} requested · 0 applied · ${number(recheck.retained)} unchanged selections kept · ${number(recheck.needs_review)} changed or unavailable</small><small>Changed and new proposals are unchecked. Review them before selecting again.</small>` : ""}${p.elapsed_seconds ? `<small>${duration(p.elapsed_seconds)} elapsed</small>` : ""}</div></div>`;
    const applying = p.operation === "apply";
    const stages = applying ? [["verifying", "Recheck providers"], ["applying", "Apply selections"], ["finalizing", "Save results"]] : [["connecting", "Connect"], ["reading", "Read providers"], ["planning", "Build plan"], ["finalizing", "Prepare review"]];
    const index = Math.max(0, stages.findIndex(([key]) => key === p.stage));
    const determinate = p.percent != null && p.total > 0;
    const detailsOpen = host.querySelector(".is-progress-details")?.open;
    return `<section class="is-run-progress" aria-label="Sync progress">
      <div class="is-progress-top"><div><div class="is-eyebrow">${applying ? "APPLYING YOUR SYNC" : "PREPARING YOUR SYNC"}</div><h2>${esc(p.label || "Checking provider connections")}</h2></div><span class="is-live"><i></i><span data-progress-connection>Connected</span></span></div>
      <ol class="is-progress-stages">${stages.map(([key, label], i) => `<li class="${i === index ? "current" : i < index ? "finished" : ""}" ${i === index ? 'aria-current="step"' : ""}><span>${i < index ? '<span class="material-symbols-rounded" aria-hidden="true">check</span>' : i + 1}</span>${label}</li>`).join("")}</ol>
      <div class="is-progress-caption"><strong>${determinate ? `${number(p.done)} of ${number(p.total)} ${esc(p.unit)}` : p.done ? `${number(p.done)} ${esc(p.unit)}` : ["reading", "verifying"].includes(p.stage) ? "Waiting for provider totals" : "Working on this stage"}</strong><span>${determinate ? `${p.percent}%` : "In progress"}</span></div>
      <div class="is-progress-track ${determinate ? "" : "indeterminate"}" role="progressbar" aria-label="${esc(p.label || "Current sync activity")}" aria-valuemin="0" aria-valuemax="100" ${determinate ? `aria-valuenow="${p.percent}"` : 'aria-valuetext="In progress; total not yet available"'}><span style="width:${determinate ? p.percent : 30}%"></span></div>
      <div class="is-progress-stats"><div><span>Elapsed</span><strong data-progress-elapsed>${duration(p.elapsed_seconds)}</strong></div><div><span>Current activity</span><strong data-progress-stage>${duration(p.stage_seconds)}</strong></div><div><span>Items read</span><strong>${number(p.items_read)}</strong></div><div><span>API requests</span><strong>${number(p.requests)}</strong></div></div>
      <div class="is-progress-note"><span class="material-symbols-rounded" aria-hidden="true">info</span><p><span data-progress-quiet>Waiting for the next provider update.</span><br>You can leave this page and return. This operation continues on the server.</p></div>
      ${(p.recent || []).length ? `<details class="is-progress-details" ${detailsOpen ? "open" : ""}><summary>Recent activity</summary><ol>${p.recent.slice().reverse().map(row => `<li><time>${esc(new Date(row.at * 1000).toLocaleTimeString())}</time><span>${esc(row.text)}</span></li>`).join("")}</ol></details>` : ""}
    </section>`;
  }
  function updateProgressClock() {
    if (!active()) return;
    const p = session?.progress || {};
    const extra = p.running ? Math.floor((Date.now() - progressReceived) / 1000) : 0;
    const put = (selector, value) => { const node = host.querySelector(selector); if (node && node.textContent !== value) node.textContent = value; };
    put("[data-progress-elapsed]", duration((p.elapsed_seconds || 0) + extra));
    put("[data-progress-stage]", duration((p.stage_seconds || 0) + extra));
    put("[data-progress-connection]", pollFailures ? "Reconnecting..." : "Connected");
    host.querySelector(".is-live")?.classList.toggle("reconnecting", pollFailures > 0);
    const quiet = (p.quiet_seconds || 0) + extra;
    put("[data-progress-quiet]", pollFailures ? "Connection interrupted. Retrying automatically; the server may still be processing." : quiet >= 60 ? `Last provider activity ${duration(quiet)} ago. Waiting for the next update.` : "Progress updates automatically as providers report activity.");
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
    if (mappingDirty && data.status === "review") { mappingDirty = false; action("refresh"); }
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
          session.status = "error"; session.message = "Progress is unavailable. Reopen the review to reconnect."; render(); showError(error); clearInterval(progressTimer);
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
  function reportIssuesHTML() {
    const report = session.report;
    if (!report.issue_count && !report.issue_details_omitted) return "";
    const pages = Math.max(1, Math.ceil(pageData.total / PAGE_SIZE));
    return `<section class="is-report-section"><h2>Items that need attention (${number(report.issue_count)})</h2><p>Item details and reasons recorded by the sync engine during this run.</p>
      ${report.issue_details_omitted ? `<p class="is-report-warning">${number(report.issue_details_omitted)} additional items were omitted from the engine's detailed events. The totals above include them.</p>` : ""}
      <div class="is-toolbar"><label class="is-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input data-filter="query" type="search" maxlength="256" placeholder="Search titles, IDs or reasons" aria-label="Search report items" value="${esc(query)}"></label><label>Feature<select data-filter="feature"><option value="">All features</option>${session.report.features.map(row => `<option value="${esc(row.feature)}" ${feature === row.feature ? "selected" : ""}>${esc(row.feature)}</option>`).join("")}</select></label><label>Result<select data-filter="result"><option value="">All results</option>${Object.entries({failed:"Not confirmed",unresolved:"Unresolved",blocked:"Blocked"}).map(([key,label]) => `<option value="${key}" ${result === key ? "selected" : ""}>${label}</option>`).join("")}</select></label></div>
      <p class="is-report-item-range" role="status">${pageLoading ? "Loading..." : `${number(pageData.total ? page * PAGE_SIZE + 1 : 0)}&ndash;${number(Math.min((page + 1) * PAGE_SIZE, pageData.total))} of ${number(pageData.total)} matching items`}</p><div class="is-table-wrap"><table class="is-table is-report-items"><thead><tr><th>Item</th><th>Feature / destination</th><th>Result</th><th>Reason</th></tr></thead><tbody>${pageLoading ? `<tr><td colspan="4">Loading item details…</td></tr>` : pageData.items.map(row => `<tr><td><strong>${esc(title(row.item))}</strong><small>${esc(row.key || Object.entries(row.item?.ids || {}).map(([key,value]) => `${key}:${value}`).join(" · "))}</small></td><td>${esc(row.feature)}<small>${esc(endpoint(row.provider,row.instance))} · ${esc(labels[row.operation] || row.operation)}</small></td><td><span class="is-badge ${row.result === "blocked" ? "blocked" : "unresolved"}">${row.result === "blocked" ? "Blocked" : row.result === "unresolved" ? "Unresolved" : "Not confirmed"}</span></td><td class="is-report-reason">${esc(row.explanation || row.reason)}${row.explanation ? `<small>${esc(row.reason)}</small>` : ""}</td></tr>`).join("") || `<tr><td colspan="4">No matching items.</td></tr>`}</tbody></table></div>
      <nav class="is-pagination" aria-label="Report item pages"><button class="is-btn is-small" data-action="first" ${pageLoading || !page ? "disabled" : ""}>First</button><button class="is-btn is-small" data-action="prev" ${pageLoading || !page ? "disabled" : ""}>Previous</button><label>Page <input type="number" data-page min="1" max="${pages}" value="${page + 1}" ${pageLoading ? "disabled" : ""}> of ${number(pages)}</label><button class="is-btn is-small" data-action="next" ${pageLoading || page + 1 >= pages ? "disabled" : ""}>Next</button><button class="is-btn is-small" data-action="last" ${pageLoading || page + 1 >= pages ? "disabled" : ""}>Last</button></nav></section>`;
  }
  function reportHTML() {
    const report = session.report, totals = report.totals;
    const outcomes = {
      success: ["check_circle", "Sync completed", "The sync engine finished processing your selected changes."],
      attention: ["warning", "Finished with items to review", "Some changes need attention. Review the results and notices below."],
      cancelled: ["pause_circle", "Sync cancelled", "Changes already applied were kept."],
      incomplete: ["error", "Sync did not finish", "The report contains the results available before the operation stopped."]
    };
    const [icon, heading, description] = outcomes[report.outcome] || outcomes.incomplete;
    const changes = totals.added + totals.updated + totals.removed;
    const columns = [["added", "Added"], ["updated", "Updated"], ["removed", "Removed"], ["skipped", "Skipped"], ["unresolved", "Unresolved"], ["blocked", "Blocked"], ["errors", "Errors"]];
    const cells = row => columns.map(([key]) => `<td>${row[key] == null ? "—" : number(row[key])}</td>`).join("");
    const date = value => value ? esc(new Date(value).toLocaleString()) : "—";
    return `<section class="is-report" aria-label="Sync completion report">
      <div class="is-report-outcome ${esc(report.outcome)}" role="status"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span><div><h2>${heading}</h2><p>${description}</p></div><span class="is-report-duration">${duration(report.duration_seconds)}</span></div>
      <div class="is-metrics is-report-metrics">${columns.slice(0, 4).map(([key, label]) => `<div><strong>${number(totals[key])}</strong><span>${label}</span></div>`).join("")}</div>
      ${changes ? `<div class="is-report-distribution" role="img" aria-label="${number(totals.added)} added, ${number(totals.updated)} updated, ${number(totals.removed)} removed">${["added", "updated", "removed"].map(key => `<span class="${key}" style="width:${totals[key] / changes * 100}%"></span>`).join("")}</div><div class="is-report-legend">${columns.slice(0, 3).map(([key, label]) => `<span><i class="${key}"></i>${label}</span>`).join("")}</div>` : ""}
      <dl class="is-report-facts"><div><dt>Selected for this run</dt><dd>${number(report.requested)} of ${number(report.proposed)} proposals</dd></div><div><dt>Not selected</dt><dd>${number(report.not_selected)}</dd></div><div><dt>Started</dt><dd>${date(report.started_at)}</dd></div><div><dt>Finished</dt><dd>${date(report.finished_at)}</dd></div><div><dt>API requests</dt><dd>${number(report.requests)}</dd></div><div><dt>Conflict decisions</dt><dd>${number(report.conflicts_reviewed)}</dd></div></dl>
      <section class="is-report-section"><h2>Results by feature and destination</h2><p>Counts reported by the sync engine for this operation.</p><div class="is-table-wrap"><table class="is-table is-report-table"><thead><tr><th scope="col">Feature / destination</th>${columns.map(([, label]) => `<th scope="col">${label}</th>`).join("")}</tr></thead><tbody>${report.features.map(row => `<tr class="is-report-feature"><th scope="row">${esc(row.feature.charAt(0).toUpperCase() + row.feature.slice(1))}</th>${cells(row)}</tr>${row.destinations.map(dst => `<tr><th scope="row" class="is-report-destination">${esc(endpoint(dst.provider, dst.instance))}</th>${cells(dst)}</tr>`).join("")}`).join("") || `<tr><td colspan="8">No completed feature results were reported.</td></tr>`}</tbody></table></div></section>
      <section class="is-report-section is-report-attention"><h2>Attention and follow-up</h2><div class="is-report-checks">${[["Unresolved items", totals.unresolved], ["Provider errors", totals.errors], ["Blocked by sync rules", totals.blocked], ["Selected proposals not reached", report.not_reached]].map(([label, n]) => `<div class="${n ? "needs-attention" : ""}"><span>${label}</span><strong>${number(n)}</strong></div>`).join("")}</div>${report.not_reached ? `<p>These selected proposals did not reach execution. Data may have changed, a protection may have stopped them, or the run may have ended early.</p>` : ""}${report.incomplete_note ? `<p class="is-report-warning">${esc(report.incomplete_note)}</p>` : ""}${report.outcome !== "success" ? `<p>${report.issue_count ? "Review the affected items below." : "The engine did not include item-level details. Check Events for the available diagnostics."} Start a new review to see what still needs syncing before retrying.</p>` : ""}</section>
      ${(report.notices || []).length ? `<details class="is-notices" open><summary>Execution notices (${number(report.notices.length)})</summary>${report.notices.map(n => `<p><strong>${esc([n.feature, n.provider].filter(Boolean).join(" · "))}</strong> ${esc(n.reason)}${n.occurrences > 1 ? ` (${number(n.occurrences)} occurrences)` : ""}</p>`).join("")}${report.notice_overflow ? `<p>Additional notices are available in Events.</p>` : ""}</details>` : ""}
      ${(report.review_notices || []).length ? `<details class="is-notices"><summary>Protections and notices from the preview (${number(report.review_notices.length)})</summary>${report.review_notices.map(n => `<p>${esc(n.feature)} ${esc(n.provider)} · ${esc(n.reason)}</p>`).join("")}</details>` : ""}
      ${reportIssuesHTML()}
      <p class="is-report-accounting">${esc(report.accounting_note)}</p>
      <footer class="is-report-footer"><div><strong>Keep a copy of this report</strong><small>Available while this review session is retained. Download it for your records.</small></div><div class="is-header-actions"><button class="is-btn" data-action="download-report"><span class="material-symbols-rounded" aria-hidden="true">download</span>Download JSON</button><a class="is-btn is-primary" href="#settings/sync">Back to Synchronization</a></div></footer>
    </section>`;
  }
  function render() {
    if (!host) return;
    const pair = session?.pair || {};
    const pages = Math.max(1, Math.ceil(pageData.total / PAGE_SIZE));
    const rows = pageData.items;
    const count = session?.counts?.changes || 0;
    const selectedCount = session?.counts?.selected || 0;
    const done = !!session?.report || session?.status === "complete";
    const locked = pending() || pageLoading || done || session?.status !== "review";
    const summary = session?.summary || {};
    const focused = host.contains(document.activeElement) && document.activeElement?.dataset?.filter === "query";
    const cursor = focused ? document.activeElement.selectionStart : null;
    host.innerHTML = `<div class="is-page">
      <a class="is-back" href="#settings/sync"><span class="material-symbols-rounded" aria-hidden="true">arrow_back</span>Synchronization</a>
      <header class="is-header"><div><div class="is-eyebrow">INTERACTIVE SYNC</div><h1>${session?.report ? "Sync report" : "Review your sync"}</h1></div><div class="is-header-actions">${session?.report ? "" : `<button class="is-btn" data-action="refresh" ${pending() || done || !session ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">refresh</span>Refresh plan</button>`}<button class="is-btn" data-action="discard" ${pending() || !session ? "disabled" : ""}>${session?.report ? "Close report" : "Close review"}</button></div></header>
      <div class="is-route"><span class="material-symbols-rounded" aria-hidden="true">sync_alt</span><strong>${esc(endpoint(pair.source, pair.source_instance))}</strong><span>${pair.mode === "two-way" ? "↔" : "→"}</span><strong>${esc(endpoint(pair.target, pair.target_instance))}</strong><span class="is-route-mode">${pair.mode === "two-way" ? "Two-way" : "One-way"}</span></div>
      ${session?.report ? "" : `<div class="is-progress-host">${progressHTML()}</div>`}
      <div class="is-error" role="alert" hidden></div>
      ${session?.report ? reportHTML() : `<div class="is-review-content" ${["reading", "applying"].includes(session?.status) ? "hidden" : ""}>
      ${done ? `<div class="is-metrics">${[["Added", summary.added], ["Updated", summary.updated], ["Removed", summary.removed], ["Unresolved", summary.unresolved], ["Errors", summary.errors], ["Changed since review", summary.not_applied]].map(([label, n]) => `<div><strong>${Number(n || 0)}</strong><span>${label}</span></div>`).join("")}</div>` : `<div class="is-metrics"><div><strong>${count}</strong><span>Proposed changes</span></div><div><strong>${selectedCount}</strong><span>Selected</span></div><div><strong>${session?.counts?.conflicts || 0}</strong><span>Conflicts to review</span></div><div><strong>${session?.counts?.attention || 0}</strong><span>Need attention</span></div></div>`}
      ${(session?.notices || []).length ? `<details class="is-notices"><summary>Sync protections and provider notices (${session.notices.length})</summary>${session.notices.map(n => `<p>${esc(n.feature)} ${esc(n.provider)} · ${esc(n.reason.replaceAll("_", " ").replaceAll(":", ": "))}</p>`).join("")}</details>` : ""}
      <div class="is-toolbar"><label class="is-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input data-filter="query" type="search" maxlength="256" placeholder="Search titles or IDs" aria-label="Search proposed changes" value="${esc(query)}"></label><label>Feature<select data-filter="feature"><option value="">All features</option>${(session?.features || []).map(f => `<option value="${esc(f)}" ${feature === f ? "selected" : ""}>${esc(f.charAt(0).toUpperCase() + f.slice(1))}</option>`).join("")}</select></label><label>Result<select data-filter="result"><option value="">All results</option>${Object.entries(labels).map(([k, v]) => `<option value="${k}" ${result === k ? "selected" : ""}>${v}</option>`).join("")}</select></label></div>
      <div class="is-list-actions"><span>${pageLoading ? "Loading page..." : `${pageData.total} matching items`}</span><button class="is-link" data-action="select" ${locked ? "disabled" : ""}>Select filtered</button><button class="is-link" data-action="deselect" ${locked ? "disabled" : ""}>Deselect filtered</button></div>
      <div class="is-table-wrap" aria-busy="${pageLoading}"><table class="is-table"><thead><tr><th scope="col"><span class="sr-only">Selected</span></th><th scope="col">Item</th><th scope="col">Feature</th><th scope="col">Proposed change</th><th scope="col">Review</th></tr></thead><tbody>${rows.map(row => {
        if (row.result === "conflict") return `<tr class="is-conflict"><td><span class="material-symbols-rounded" aria-hidden="true">compare_arrows</span></td><td><strong>${esc(title(row.left))}</strong><small>${esc(row.key)}</small></td><td>${esc(row.feature)}</td><td><span class="is-badge conflict">Conflict</span><small>${esc(row.source)}: ${esc(value(row.left, row.feature))} · ${esc(row.target)}: ${esc(value(row.right, row.feature))}</small></td><td><label class="is-sr-label" for="choice-${row.id}">Use value from</label><select id="choice-${row.id}" data-choice="${row.id}" ${locked ? "disabled" : ""}>${[row.source, row.target].map(p => `<option value="${esc(p)}" ${p === row.winner ? "selected" : ""}>Use ${esc(p)}</option>`).join("")}</select></td></tr>`;
        return `<tr><td><input type="checkbox" data-select="${row.id}" aria-label="Select ${esc(title(row.item))}" ${row.selected ? "checked" : ""} ${locked || !row.selectable ? "disabled" : ""}></td><td><strong>${esc(title(row.item))}</strong><small>${esc(row.item.type || "")} ${esc(row.item.year || "")} · ${esc(row.key)}</small>${row.reason ? `<small class="is-reason">${esc(row.reason)}</small>` : ""}</td><td>${esc(row.feature)}</td><td><span class="is-badge ${esc(row.result)}">${labels[row.result] || esc(row.result)}</span><span class="is-destination">${esc(endpoint(row.provider, row.instance))}</span>${row.destination_label ? `<small>${esc(row.destination_label)}</small>` : ""}<small>${esc(value(row.before, row.feature))} → ${row.operation === "remove" ? "Removed" : esc(value(row.item, row.feature))}</small></td><td>${["add", "update"].includes(row.operation) && row.feature !== "playlists" ? `<button class="is-btn is-small" data-map="${row.id}" ${locked ? "disabled" : ""}>Mapping</button>` : "—"}</td></tr>`;
      }).join("") || `<tr><td colspan="5"><div class="is-empty"><span class="material-symbols-rounded" aria-hidden="true">${pending() ? "sync" : "done_all"}</span><strong>${pending() ? "Reading your providers" : count || session?.counts?.conflicts ? "No items match these filters" : "No changes proposed"}</strong><p>${pending() ? "Your sync rules and mappings are being checked." : "Refresh the plan after changing sync settings or mappings."}</p></div></td></tr>`}</tbody></table></div>
      <div class="is-pagination"><button class="is-btn is-small" data-action="first" ${pageLoading || page === 0 ? "disabled" : ""}>First</button><button class="is-btn is-small" data-action="prev" ${pageLoading || page === 0 ? "disabled" : ""}>Previous</button><label>Page <input data-page type="number" min="1" max="${pages}" value="${page + 1}" aria-label="Page number" ${pageLoading ? "disabled" : ""}> of ${pages}</label><button class="is-btn is-small" data-action="next" ${pageLoading || page + 1 >= pages ? "disabled" : ""}>Next</button><button class="is-btn is-small" data-action="last" ${pageLoading || page + 1 >= pages ? "disabled" : ""}>Last</button></div>
      <footer class="is-footer"><div><strong>${done ? "Operation finished" : `${selectedCount} changes selected`}</strong><small>${done ? "Provider results are also available in Events." : "Mappings are saved permanently. Only selected sync changes will be applied."}</small></div>${done ? `<a class="is-btn is-primary" href="#settings/sync">Back to Synchronization</a>` : `<button class="is-btn is-primary" data-action="apply" ${locked || !selectedCount ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">check</span>Apply selected (${selectedCount})</button>`}</footer>
      </div>`}
      <div class="is-mapping-host"></div>
    </div>`;
    if (focused) {
      const input = host.querySelector('[data-filter="query"]');
      input.focus(); input.setSelectionRange(cursor, cursor);
    }
  }
  async function action(name, body = {}) {
    if (!session || pending() || (pageLoading && ["selection", "apply", "mapping"].includes(name))) return;
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
    const root = host.querySelector(".is-mapping-host");
    root.innerHTML = `<section class="is-mapping" aria-label="Resolve mapping"><div class="is-mapping-head"><div><h2>Resolve mapping</h2><p>${esc(title(row.item))}</p></div><button class="is-btn" data-close-map>Close</button></div><p>Save a persistent correction using the same overrides as Editor. The plan will be recalculated.</p><div class="is-map-tools"><button class="is-btn" data-search-map>Search metadata</button>${document.documentElement.dataset.cwRole !== "user" ? `<button class="is-btn" data-anime-map>Anime ID mappings</button>` : ""}</div><form class="is-map-form"><div class="is-map-fields">${["imdb", "tmdb", "tvdb", "trakt", "simkl", "anilist", "mal", "anidb"].map(k => `<label>${esc(k.toUpperCase())}<input name="${k}" value="${esc(row.item.ids?.[k] || "")}" autocomplete="off"></label>`).join("")}</div><button type="submit" class="is-btn is-primary">Save mapping and recalculate</button></form><div class="is-map-search"></div></section>`;
    root.scrollIntoView({ behavior: "smooth", block: "center" });
    root.querySelector("input")?.focus();
    root.querySelector("[data-close-map]").onclick = () => { root.innerHTML = ""; };
    root.querySelector("form").onsubmit = event => {
      event.preventDefault();
      const ids = { ...row.item.ids };
      for (const [k, v] of new FormData(event.target)) { if (String(v).trim()) ids[k] = String(v).trim(); else delete ids[k]; }
      action("mapping", { row_id: row.id, item: { ...row.item, ids } });
    };
    root.querySelector("[data-anime-map]")?.addEventListener("click", async () => {
      if (!window.openAnimeOverridesModal) await import(`/assets/js/modals.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
      window.openAnimeOverridesModal?.();
    });
    root.querySelector("[data-search-map]").onclick = async event => {
      try {
        for (const name of ["datetime", "search", "row-editor", "metadata-replacer"]) await import(`/assets/js/editor/${name}.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
        const editorRow = { key: row.key, type: row.item.type, title: title(row.item), year: row.item.year, raw: structuredClone(row.item) };
        const searchRoot = root.querySelector(".is-map-search");
        window.CW.Editor.MetadataReplacer.openItemReplacer(editorRow, event.target, {
          isPolicySource: () => true,
          fetchJSON: json,
          formatEpisodeVisualTitle: r => title(r.raw),
          updateTypeDisplay: window.CW.Editor.RowEditor.updateTypeDisplay,
          openPopup: (_, build) => { searchRoot.innerHTML = ""; build(searchRoot, () => { searchRoot.innerHTML = ""; }); },
          appendPopupTitle: (el, text) => { const h = document.createElement("h3"); h.textContent = text; el.append(h); },
          appendPopupActions: (el, buttons) => buttons.forEach(b => { const btn = document.createElement("button"); btn.type = "button"; btn.className = "is-btn"; btn.textContent = b.label; btn.onclick = b.onClick; el.append(btn); }),
          setStatusSticky: text => showError(new Error(text)),
          commitReplacement: (_, corrected) => { action("mapping", { row_id: row.id, item: corrected }); return ""; },
        });
      } catch (error) { showError(error); }
    };
  }
  host?.addEventListener("change", event => {
    const el = event.target;
    if (el.dataset.select) action("selection", { selected: el.checked, ids: [el.dataset.select] });
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
    if (event.key === "Enter" && event.target.hasAttribute("data-page")) { event.preventDefault(); event.target.blur(); }
  });
  host?.addEventListener("click", async event => {
    const button = event.target.closest("button");
    if (!button || button.disabled) return;
    if (button.dataset.map) return mapping(pageData.items.find(row => row.id === button.dataset.map));
    const name = button.dataset.action;
    if (name === "download-report" && session.report) {
      const report = session.report, sid = session.id, issues = [];
      button.disabled = true;
      try {
        while (issues.length < (report.issue_count || 0)) {
          const data = await json(`${API}/${sid}/report-issues?offset=${issues.length}&limit=200`);
          if (!data.items.length) throw new Error("Could not read all report items. Try downloading again.");
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
    if (name === "refresh") return action("refresh");
    if (name === "apply") return action("apply");
    if (name === "select" || name === "deselect") return action("selection", { selected: name === "select", feature, result, q: query });
    if (name === "prev" || name === "next") { page += name === "prev" ? -1 : 1; loadPage(); }
    if (name === "first" || name === "last") { page = name === "first" ? 0 : Math.max(0, Math.ceil(pageData.total / PAGE_SIZE) - 1); loadPage(); }
    if (name === "discard") {
      try { await json(`${API}/${session.id}`, { method: "DELETE" }); location.hash = "settings/sync"; }
      catch (error) { showError(error); }
    }
  });
  window.addEventListener("cw:anime-mappings-changed", () => {
    if (!session || session.report || session.status === "complete") return;
    if (active() && !pending()) action("refresh"); else mappingDirty = true;
  });
  document.addEventListener("tab-changed", event => {
    if (event.detail.tab !== "interactive_sync") { clearTimeout(timer); clearInterval(progressTimer); invalidatePage(); pageLoading = false; }
  });
  async function refresh() {
    if (!host || !active()) return;
    const next = location.hash;
    if (next === route && session) {
      route = `#interactive_sync?session=${encodeURIComponent(session.id)}`;
      history.replaceState(null, "", route);
      if (mappingDirty && !pending()) { mappingDirty = false; action("refresh"); }
      else if (!busy) poll();
      return;
    }
    if (next === route && busy) return;
    route = next;
    const current = ++generation;
    clearTimeout(timer); clearInterval(progressTimer); pollFailures = 0;
    const params = new URLSearchParams(next.split("?")[1] || "");
    invalidatePage();
    session = null; pageData = { items: [], total: 0 }; pageLoading = false; page = 0; feature = ""; result = ""; query = ""; busy = true;
    render();
    try {
      await window.__cwAuthBootstrapPromise;
      if (window.cwIsAuthSetupPending?.()) throw new Error("Complete application setup before starting a review.");
      const sid = params.get("session");
      const pairId = params.get("pair");
      if (!sid && !pairId) throw new Error("Choose Run on a sync pair to start a review.");
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
