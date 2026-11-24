/* assets/js/import-export/index.js */
/* CrossWatch - Import and Export Page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { mountPlexRecovery } from "./plex-recovery.js";
import { pageBackLink } from "../page-return.js";
import { NEW_TRACKER, createTracker, trackerOptions } from "./tracker-instance.js";

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const human = value => String(value || "").replace(/_/g, " ").replace(/^./, char => char.toUpperCase());
const number = value => Number(value || 0).toLocaleString(I18N()?.locale);
const statuses = { all: T("importExport.page.st.all"), ready: T("importExport.page.st.ready"), exists: T("importExport.page.st.exists"), duplicate: T("importExport.page.st.duplicate"), missing_identity: T("importExport.page.st.missing"), invalid: T("importExport.page.st.invalid"), unsupported: T("importExport.page.st.unsupported") };
const media = { movie: T("importExport.page.media.movies"), show: T("importExport.page.media.shows"), season: T("importExport.page.media.seasons"), episode: T("importExport.page.media.episodes") };
const option = (value, label) => `<option value="${esc(value)}">${esc(label)}</option>`;
const select = (id, label, choices = "") => `<label class="ie-field" for="${id}"><span>${label}</span><select id="${id}" data-cw-native-select="true">${choices}</select></label>`;
const checks = (group, values) => `<div class="ie-checks">${Object.entries(values).map(([value, label]) => `<label><input type="checkbox" data-group="${group}" value="${value}" checked><span>${label}</span></label>`).join("")}</div>`;

async function responseError(response) {
  const data = await response.json().catch(() => ({}));
  const detail = data.detail;
  return new Error(typeof detail === "string" ? detail : detail?.detail || detail?.code || T("importExport.page.requestFailed", {status: String(response.status)}));
}

async function json(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) throw await responseError(response);
  return response.json();
}

const template = `
<div class="ie-page" hidden>
  <a class="ie-back" href="#main">${icon("arrow_back")}${TH("navigation.tabs.main")}</a>
  <header class="ie-header"><div><span class="ie-eyebrow">${TH("importExport.page.kicker")}</span><h1>${TH("importExport.page.title")}</h1></div></header>
  <nav class="ie-tabs" aria-label="${TH("importExport.page.direction")}"><button type="button" data-mode="import" aria-pressed="true">${icon("upload_file")}${TH("importExport.page.importFile")}</button><button type="button" data-mode="export" aria-pressed="false">${icon("download")}${TH("importExport.page.export")}</button><button type="button" data-mode="recovery" aria-pressed="false">${icon("history")}${TH("importExport.page.recover")}</button></nav>
  <label class="ie-mobile-view ie-field"><span>${TH("importExport.page.action")}</span><select id="ie-mobile-mode" data-cw-native-select="true"><option value="import">${TH("importExport.page.importFile")}</option><option value="export">${TH("importExport.page.export")}</option><option value="recovery">${TH("importExport.page.recover")}</option></select></label>
  <div id="ie-notice" class="ie-notice" role="status" hidden></div>
  <div id="ie-progress" class="ie-progress" role="status" hidden><div class="ie-progress-copy"><strong id="ie-activity"></strong><span id="ie-elapsed"></span></div><div class="ie-progress-track" role="progressbar" aria-label="${TH("importExport.page.inProgress")}"><span></span></div><p id="ie-progress-note"></p></div>
  <div id="ie-recovery" hidden></div>
  <fieldset id="ie-controls">
    <section id="ie-import-setup" class="ie-setup">
      <div id="ie-drop" class="ie-drop">
        <div class="ie-drop-icon">${icon("drive_folder_upload")}</div><h2>${TH("importExport.page.drop")}</h2><p>ZIP, JSON, CSV ${TH("importExport.page.or")} TXT · <span id="ie-limit">25 MB</span> ${TH("importExport.page.max")}</p>
        <button type="button" class="ie-button ie-primary" id="ie-browse">${icon("add")}${TH("importExport.page.chooseFile")}</button><input id="ie-file" type="file" accept=".zip,.json,.csv,.txt" hidden>
        <span class="ie-drop-hint">${TH("importExport.page.dropHint")}</span>
        <div id="ie-file-info" class="ie-file-info" hidden></div>
      </div>
      <div class="ie-panel ie-destination"><span class="ie-eyebrow">${TH("importExport.page.destKicker")}</span><h2>${TH("importExport.page.destTitle")}</h2><p>${TH("importExport.page.destHint")}</p>
        <div class="ie-fields">${select("ie-target", T("importExport.page.target"))}${select("ie-source", T("importExport.page.fileSource"))}<label class="ie-field" for="ie-new-name" id="ie-new-name-wrap" hidden><span>${TH("importExport.page.newName")}</span><input id="ie-new-name" type="text" maxlength="24" autocomplete="off" placeholder="${TH("importExport.page.fileImport")}"></label></div>
        <details class="ie-guide"><summary>${TH("importExport.page.howExport")}</summary><div id="ie-guide"></div></details>
      </div>
    </section>
    <section id="ie-export-setup" class="ie-panel" hidden><div class="ie-section-heading"><div><h2>${TH("importExport.page.chooseExport")}</h2><p>${TH("importExport.page.chooseExportHint")}</p></div><button type="button" id="ie-download" class="ie-button ie-primary" aria-label="${TH("importExport.page.downloadSelected")}" title="${TH("importExport.page.downloadSelected")}" disabled>${icon("download")}</button></div>
      <div class="ie-fields ie-export-fields">${select("ie-provider", T("importExport.page.provider"))}${select("ie-instance", T("importExport.page.instance"))}${select("ie-feature", T("importExport.page.feature"), ["watchlist", "history", "ratings", "combined"].map(value => option(value, value === "combined" ? T("importExport.page.historyRatings") : human(value))).join(""))}${select("ie-format", T("importExport.page.format"))}</div>
      <div class="ie-export-extras"><label id="ie-date-wrap"><input id="ie-date" type="checkbox" checked>${TH("importExport.page.dates")}</label><label id="ie-rewatch-wrap"><input id="ie-rewatch" type="checkbox" checked>${TH("importExport.page.repeats")}</label></div>
    </section>
    <button type="button" id="ie-options" class="ie-button ie-mobile-options" aria-expanded="false" aria-controls="ie-inclusions">${icon("tune")}${TH("importExport.page.featuresMedia")}</button>
    <section id="ie-inclusions" class="ie-panel ie-inclusions"><div id="ie-features-wrap"><h3>${TH("importExport.page.features")}</h3>${checks("features", {history:T("importExport.page.history"),ratings:T("importExport.page.ratings"),watchlist:T("importExport.page.watchlist")})}</div><div><h3>${TH("importExport.page.mediaTypes")}</h3>${checks("media", media)}</div></section>
    <section id="ie-review" hidden>
      <div class="ie-review-heading"><div><span class="ie-eyebrow">${TH("importExport.page.reviewKicker")}</span><h2 id="ie-review-title">${TH("importExport.page.filePreview")}</h2></div><button type="button" class="ie-button" id="ie-refresh">${icon("refresh")}${TH("importExport.page.refreshPreview")}</button></div>
      <div id="ie-metrics" class="ie-metrics"></div>
      <div id="ie-warnings" class="ie-warning" hidden></div>
      <div class="ie-filters"><label class="ie-search">${icon("search")}<input id="ie-search" type="search" maxlength="1000" placeholder="${TH("importExport.page.searchPlaceholder")}" aria-label="${TH("importExport.page.searchLabel")}"></label>${select("ie-status", T("importExport.page.result"), Object.entries(statuses).map(([value,label]) => option(value,label)).join(""))}</div>
      <div class="ie-list-bar"><span id="ie-range"></span><button type="button" id="ie-selection-options" class="ie-button ie-mobile-options" aria-expanded="false" aria-controls="ie-selection-actions">${icon("checklist")}${TH("importExport.page.selection")}</button><div id="ie-selection-actions" class="ie-list-actions"><button type="button" class="ie-link" id="ie-select-all">${TH("importExport.page.selectAll")}</button><button type="button" class="ie-link" id="ie-select-none">${TH("importExport.page.clearSelection")}</button><label id="ie-existing-wrap"><input type="checkbox" id="ie-existing">${TH("importExport.page.includeExisting")}</label>${select("ie-page-size", T("importExport.page.perPage"), [25,50,100,200].map(value => option(value,value)).join(""))}</div></div>
      <div class="ie-table-wrap"><table><thead><tr><th class="ie-check-cell"><span class="ie-sr">${TH("importExport.page.selectedCol")}</span></th><th>${TH("importExport.page.item")}</th><th id="ie-column-feature">${TH("importExport.page.feature")}</th><th>${TH("importExport.page.ids")}</th><th id="ie-column-status">${TH("importExport.page.result")}</th></tr></thead><tbody id="ie-rows"></tbody></table></div>
      <nav id="ie-pagination" class="ie-pagination" aria-label="${TH("importExport.page.previewPages")}"></nav>
      <div class="ie-apply"><div><strong id="ie-selected"></strong><p id="ie-apply-note"></p></div><button type="button" id="ie-apply" class="ie-button ie-primary" disabled></button></div>
    </section>
  </fieldset>
</div>`;

let stylesReady;
function loadStyles() {
  if (stylesReady) return stylesReady;
  stylesReady = new Promise(resolve => {
    const link = document.createElement("link");
    link.id = "ie-styles";
    link.rel = "stylesheet";
    link.href = new URL(`./styles.css?v=${encodeURIComponent(window.__CW_VERSION__ || "1")}`, import.meta.url).href;
    link.onload = () => resolve(true);
    link.onerror = () => { link.remove(); stylesReady = null; resolve(false); };
    document.head.appendChild(link);
  });
  return stylesReady;
}

function updateBackLink(host) {
  const route = new URLSearchParams(window.location.hash.split("?")[1] || "");
  const back = pageBackLink(route.get("returnTo"), window.location.href, "import_export");
  const link = host.querySelector(".ie-back");
  link.setAttribute("href", back.href);
  link.innerHTML = `${icon("arrow_back")}${esc(back.label)}`;
}

let active;
const ImportExport = {
  async mount(host) {
    if (!host) return;
    if (new URLSearchParams(window.location.hash.split("?")[1] || "").has("recovery")) (window.CW ||= {}).pendingPlexRecovery ||= {};
    if (active?.host === host) {
      updateBackLink(host);
      if (window.CW?.pendingPlexRecovery) await active.openRecovery();
      return;
    }
    this.unmount();
    const stylesheet = loadStyles();
    host.innerHTML = '<div id="ie-loading" role="status">' + TH("importExport.page.loading") + '</div>' + template;
    updateBackLink(host);
    const $ = selector => host.querySelector(selector);
    const $$ = selector => [...host.querySelectorAll(selector)];
    const lifetime = new AbortController();
    let recoveryCleanup = null;
    let mode = "import", busy = false, loading = false, file = null, preview = null, scopeChanged = false;
    let importOptions = null, exportOptions = null, data = null, rows = [], offset = 0, pageSize = 50;
    let all = true, selected = new Set(), excluded = new Set(), pageController, pageSequence = 0, searchTimer, progressTimer;
    let exportMedia = ["movie"], importMedia = Object.keys(media);
    const on = (element, event, fn) => element.addEventListener(event, fn, {signal:lifetime.signal});
    active = { host, async openRecovery() {
      await setMode("recovery");
      window.dispatchEvent(new CustomEvent("cw:open-plex-recovery"));
    }, cleanup() {
      recoveryCleanup?.();
      lifetime.abort(); pageController?.abort(); clearTimeout(searchTimer); clearInterval(progressTimer);
      window.CW?.IconSelect?.closeAll();
      $$("select").forEach(input => { input.__cwOptionsObserver?.disconnect(); input.nextElementSibling?.__cwMenu?.remove(); });
    } };
    $("#ie-page-size").value = "50";
    function refreshSelects() {
      $$("select").forEach(input => {
        if (input.id === "ie-mobile-mode") return;
        const wrap = window.CW?.IconSelect?.enhance(input, {className:"ie-select",menuClassName:"ie-select-menu",sortAlphabetically:["ie-source","ie-target","ie-provider","ie-instance"].includes(input.id),sortFirstValues:["all",NEW_TRACKER]});
        const label = input.closest("label").querySelector("span");
        label.id = `${input.id}-label`;
        wrap?.querySelector("button")?.setAttribute("aria-labelledby", label.id);
        wrap?.__cwMenu?.setAttribute("aria-labelledby", label.id);
      });
    }
    const checked = group => $$(`input[data-group="${group}"]:checked`).filter(input => !input.disabled).map(input => input.value);
    const isImport = () => mode === "import";
    const status = () => isImport() ? $("#ie-status").value : "all";
    const eligible = row => !isImport() || row.status === "ready" || (row.status === "exists" && $("#ie-existing").checked);
    const rowKey = row => String(isImport() ? row.id : row.key);
    const isSelected = row => eligible(row) && (all ? !excluded.has(rowKey(row)) : selected.has(rowKey(row)));
    const total = () => Number(isImport() ? data?.filtered_total : data?.total) || 0;
    function selectableTotal() {
      if (!isImport()) return total();
      const counts = data?.summary?.by_status || {};
      return (status() === "all" || status() === "ready" ? counts.ready || 0 : 0) +
        ($("#ie-existing").checked && (status() === "all" || status() === "exists") ? counts.exists || 0 : 0);
    }
    const selectedCount = () => Math.max(0, all ? selectableTotal() - excluded.size : selected.size);
    const resetSelection = () => { all = true; selected.clear(); excluded.clear(); };
    function notice(message = "", kind = "info", detail = "") {
      const node = $("#ie-notice");
      node.hidden = !message;
      node.dataset.kind = kind;
      node.innerHTML = `${icon(kind === "error" ? "error" : kind === "success" ? "check_circle" : "info")}<div><strong>${esc(message)}</strong>${detail ? `<p>${esc(detail)}</p>` : ""}</div>`;
    }
    function progress(title, note) {
      window.CW?.IconSelect?.closeAll();
      busy = true;
      $("#ie-mobile-mode").disabled = true;
      $("#ie-controls").disabled = true;
      $$("[data-mode]").forEach(button => button.disabled = true);
      $("#ie-progress").hidden = false;
      $("#ie-activity").textContent = title;
      $("#ie-progress-note").textContent = note;
      const started = Date.now();
      const tick = () => {
        const seconds = Math.floor((Date.now() - started) / 1000);
        $("#ie-elapsed").textContent = `${Math.floor(seconds / 60)}m ${seconds % 60}s elapsed`;
      };
      tick();
      progressTimer = setInterval(tick, 1000);
      updateSelection();
    }
    function finishProgress() {
      clearInterval(progressTimer);
      busy = false;
      $("#ie-mobile-mode").disabled = false;
      $("#ie-controls").disabled = false;
      $$("[data-mode]").forEach(button => button.disabled = false);
      $("#ie-progress").hidden = true;
      updateSelection();
      if (scopeChanged) { ImportExport.unmount(); ImportExport.mount(host); }
    }
    function parameters() {
      if (isImport()) return { features:checked("features").join(","),media_types:checked("media").join(","),status:status(),q:$("#ie-search").value.trim() };
      return { provider:$("#ie-provider").value,provider_instance:$("#ie-instance").value,feature:$("#ie-feature").value,format:$("#ie-format").value,
        media_types:checked("media").join(","),include_watched_date:$("#ie-date").checked,include_rewatches:$("#ie-rewatch").checked,q:$("#ie-search").value.trim() };
    }
    function updateSelection() {
      const count = selectedCount();
      $("#ie-selected").textContent = T("importExport.page.selected", {count});
      $("#ie-apply").innerHTML = `${icon(isImport() ? "upload" : "download")}${TH(isImport() ? "importExport.page.importCount" : "importExport.page.downloadCount", {count})}`;
      $("#ie-apply").disabled = busy || loading || !data || !count;
      $("#ie-download").disabled = mode !== "export" || $("#ie-apply").disabled;
      $("#ie-apply-note").textContent = isImport() ? T("importExport.page.into", {name: $("#ie-target").selectedOptions[0]?.textContent || "CrossWatch"}) : T("importExport.page.downloadNote");
      $("#ie-select-all").disabled = loading || !selectableTotal();
      $("#ie-select-none").disabled = loading || !count;
    }
    function render() {
      const importing = isImport();
      const counts = data?.summary || {};
      const metrics = importing ? [[counts.total,T("importExport.page.itemsInPreview")],[counts.ready,T("importExport.page.st.ready")],[counts.exists,T("importExport.page.st.exists")],[(counts.total || 0)-(counts.ready || 0)-(counts.exists || 0),T("importExport.page.notImportable")]] : [[data?.matched_total,T("importExport.page.matching")],[data?.total,T("importExport.page.readyExport")],[data?.dropped_total,T("importExport.page.notExportable")]];
      $("#ie-metrics").innerHTML = metrics.map(([value,label]) => `<div><strong>${number(value)}</strong><span>${label}</span></div>`).join("");
      $("#ie-range").textContent = loading ? T("importExport.page.loadingPreview") : T("importExport.page.range", {start: total() ? offset + 1 : 0, end: Math.min(offset + pageSize,total()), count: total()});
      $("#ie-rows").innerHTML = loading ? `<tr><td colspan="5" class="ie-empty">${TH("importExport.page.loadingPreview")}</td></tr>` : rows.map((row,index) => {
        const ids = Object.entries(row.ids || {}).filter(([,value]) => value != null && value !== "");
        const name = `${row.title || T("importExport.page.untitled")}${row.season != null && row.episode != null ? ` · S${String(row.season).padStart(2,"0")}E${String(row.episode).padStart(2,"0")}` : ""}`;
        const result = importing ? statuses[row.status] || human(row.status) : [row.watched_at, row.rating != null ? T("importExport.page.rating", {value: String(row.rating)}) : ""].filter(Boolean).join(" · ") || T("importExport.page.ready");
        return `<tr${isSelected(row) ? ' class="ie-row-selected"' : ""}><td class="ie-check-cell"><input type="checkbox" data-row="${index}" aria-label="${TH("importExport.plexRecovery.selectTitle", {title: name})}" ${isSelected(row) ? "checked" : ""} ${eligible(row) ? "" : "disabled"}></td><td><strong>${esc(name)}</strong><small>${esc([human(row.media_type || row.type),row.year].filter(Boolean).join(" · "))}</small></td><td>${esc(human(row.feature || $("#ie-feature").value))}</td><td class="ie-ids">${ids.slice(0,3).map(([key,value]) => `<span>${esc(key)}:${esc(value)}</span>`).join("")}${ids.length > 3 ? `<details><summary>${TH("importExport.page.more", {count: ids.length-3})}</summary>${ids.slice(3).map(([key,value]) => `<span>${esc(key)}:${esc(value)}</span>`).join("")}</details>` : ""}</td><td><span class="ie-status" data-status="${esc(row.status || "ready")}">${esc(result)}</span>${importing && row.reason ? `<small>${esc(human(row.reason))}</small>` : ""}</td></tr>`;
      }).join("") || `<tr><td colspan="5" class="ie-empty">${TH(data ? "importExport.page.noMatch" : "importExport.page.previewUnavailable")}</td></tr>`;
      const pages = Math.max(1,Math.ceil(total()/pageSize)), current = Math.floor(offset/pageSize)+1;
      $("#ie-pagination").innerHTML = `${[[0,"keyboard_double_arrow_left",T("importExport.page.first")],[Math.max(0,offset-pageSize),"chevron_left",T("importExport.page.previous")]].map(([start,ico,label]) => `<button type="button" class="ie-button" data-page="${start}" ${loading || current===1 ? "disabled" : ""}>${icon(ico)}${label}</button>`).join("")}<label>${TH("importExport.page.page")} <input id="ie-page" type="number" min="1" max="${pages}" value="${current}" ${loading ? "disabled" : ""}> ${TH("importExport.page.ofPages", {pages})}</label>${[[offset+pageSize,"chevron_right",T("importExport.page.next")],[(pages-1)*pageSize,"keyboard_double_arrow_right",T("importExport.page.last")]].map(([start,ico,label]) => `<button type="button" class="ie-button" data-page="${start}" ${loading || current>=pages ? "disabled" : ""}>${label}${icon(ico)}</button>`).join("")}`;
      const warnings = !importing ? data?.warnings || [] : [];
      $("#ie-warnings").hidden = !warnings.length;
      $("#ie-warnings").textContent = warnings.map(warning => typeof warning === "string" ? warning : warning.message || human(warning.code)).join(" ");
      updateSelection();
    }
    function cancelRead() {
      clearTimeout(searchTimer);
      pageController?.abort();
      pageSequence++;
    }
    async function loadPage(start = 0) {
      cancelRead();
      const sequence = pageSequence;
      offset = start;
      if ((isImport() && !preview) || (!isImport() && !$("#ie-provider").value)) {
        data = null; rows = []; loading = false; render(); return;
      }
      $("#ie-review").hidden = false;
      const params = parameters();
      if (!params.media_types || (isImport() && !params.features)) {
        data = {total:0,filtered_total:0}; rows=[]; loading=false; render(); return;
      }
      pageController = new AbortController();
      loading = true;
      render();
      try {
        const url = isImport() ? `/api/import/preview/${encodeURIComponent(preview.import_id)}` : "/api/export/sample";
        const result = await json(`${url}?${new URLSearchParams({...params,limit:pageSize,offset})}`, {signal:pageController.signal});
        if (sequence !== pageSequence || lifetime.signal.aborted) return;
        data=result; rows=isImport() ? result.rows || [] : result.items || [];
        if (offset && offset >= total()) { await loadPage(Math.max(0,Math.ceil(total()/pageSize)-1)*pageSize); return; }
      } catch (error) {
        if (sequence !== pageSequence || lifetime.signal.aborted) return;
        data=null; rows=[];
        notice(T("importExport.page.previewFailed"),"error",error.message);
      } finally {
        if (sequence === pageSequence && !lifetime.signal.aborted) { loading=false; render(); }
      }
    }
    function refresh() { resetSelection(); notice(); loadPage(); }
    function guide() {
      const source = $("#ie-source").value;
      const spec = importOptions?.source_guides?.[source];
      $("#ie-guide").innerHTML = spec ? `<strong>${esc(spec.title)}</strong><ol>${(spec.steps || []).map(step => `<li>${esc(step)}</li>`).join("")}</ol>` : T("importExport.page.guideDefault");
    }
    function invalidatePreview() {
      cancelRead(); preview=null; data=null; rows=[]; loading=false; resetSelection(); $("#ie-review").hidden=true;
      $("#ie-file-info").innerHTML = file ? `${icon("description")}<span>${esc(file.name)}<small>${number(Math.ceil(file.size/1024))} KB</small></span><button type="button" class="ie-button" id="ie-preview-file">${TH("importExport.page.previewFile")}</button>` : "";
      $("#ie-file-info").hidden = !file;
      updateSelection();
    }
    async function previewFile(chosen = file) {
      if (busy || !chosen) return;
      const max = importOptions?.limits?.upload_bytes || 25*1024*1024;
      if (!/\.(zip|json|csv|txt)$/i.test(chosen.name)) { notice(T("importExport.page.chooseType"),"error"); return; }
      if (chosen.size > max) { notice(T("importExport.page.tooLarge", {size: number(max/1024/1024)}),"error"); return; }
      file=chosen;
      invalidatePreview();
      if ($("#ie-target").value === NEW_TRACKER) {
        try {
          const made = await createTracker($("#ie-new-name").value.trim() || T("importExport.page.fileImport"));
          importOptions.targets.push(made);
          $("#ie-target").innerHTML = trackerOptions(importOptions.targets, option);
          $("#ie-target").value = made.id;
          $("#ie-new-name").value = "";
          $("#ie-new-name-wrap").hidden = true;
        } catch (error) {
          notice(T("importExport.page.createFailed"),"error",error.message); return;
        }
      }
      if (!importOptions?.targets?.find(target => target.id === $("#ie-target").value)?.connected) {
        notice(T("importExport.page.connectTracker"),"error",T("importExport.page.connectTrackerHint")); return;
      }
      notice();
      progress(T("importExport.page.reading"), T("importExport.page.readingHint"));
      const body = new FormData();
      body.append("file",file); body.append("source",$("#ie-source").value); body.append("target_instance",$("#ie-target").value);
      try {
        preview = await json("/api/import/preview",{method:"POST",body,signal:lifetime.signal});
        $("#ie-review-title").textContent = file.name;
        $("#ie-file-info").innerHTML = `${icon("task")}<span>${esc(file.name)}<small>${esc(human(preview.source))} · ${TH("importExport.page.itemsFound", {count: Number(preview.total || 0)})}</small></span>`;
        notice(T("importExport.page.fileReady"),"success",T("importExport.page.fileReadyHint"));
        await loadPage();
      } catch (error) {
        if (!lifetime.signal.aborted) notice(T("importExport.page.fileFailed"),"error",error.message);
      } finally {
        if (!lifetime.signal.aborted) {
          finishProgress();
          if (preview && data) $("#ie-review").scrollIntoView({behavior:"smooth",block:"start"});
        }
      }
    }
    function exportConfiguration(changed = "") {
      if (!exportOptions) return;
      if (!changed || changed === "ie-provider") {
        const instances=exportOptions.instances[$("#ie-provider").value] || [];
        $("#ie-instance").innerHTML = option("all",T("importExport.page.allInstances"))+instances.map(instance => option(instance.id,instance.label)).join("");
      }
      if (!changed || changed === "ie-feature") {
        const previous=$("#ie-format").value;
        const formats=exportOptions.formats[$("#ie-feature").value] || [];
        $("#ie-format").innerHTML=formats.map(format => option(format,exportOptions.labels[format] || human(format))).join("");
        if (formats.includes(previous)) $("#ie-format").value=previous;
      }
      const supported=exportOptions.capabilities[$("#ie-format").value]?.media_types || [];
      $$('[data-group="media"]').forEach(input => { input.disabled=!supported.includes(input.value); input.checked=exportMedia.includes(input.value) && !input.disabled; });
      $("#ie-date-wrap").hidden=!($("#ie-format").value==="letterboxd" && ["history","combined"].includes($("#ie-feature").value));
      $("#ie-rewatch-wrap").hidden=!(["history","combined"].includes($("#ie-feature").value) && exportOptions.rewatches[$("#ie-provider").value]);
    }
    async function setMode(next) {
      if (busy || mode===next) return;
      window.CW?.IconSelect?.closeAll();
      cancelRead();
      if (isImport()) importMedia=checked("media"); else if (mode === "export") exportMedia=checked("media");
      mode=next; data=null; rows=[]; resetSelection(); notice();
      $("#ie-mobile-mode").value = mode;
      $("#ie-controls").hidden=mode === "recovery";
      $("#ie-recovery").hidden=mode !== "recovery";
      if (mode === "recovery") {
        $$("[data-mode]").forEach(button => button.setAttribute("aria-pressed",String(button.dataset.mode===mode)));
        recoveryCleanup ||= mountPlexRecovery($("#ie-recovery"), { NEW_TRACKER, createTracker, trackerOptions });
        return;
      }
      $("#ie-search").value="";
      $("#ie-import-setup").hidden=!isImport();
      $("#ie-export-setup").hidden=isImport();
      $("#ie-features-wrap").hidden=!isImport();
      $("#ie-existing-wrap").hidden=!isImport();
      $("#ie-status").closest("label").hidden=!isImport();
      $("#ie-review-title").textContent=isImport() ? file?.name || T("importExport.page.filePreview") : T("importExport.page.exportPreview");
      $("#ie-column-status").textContent=isImport() ? T("importExport.page.result") : T("importExport.page.watchedRating");
      $$("[data-mode]").forEach(button => button.setAttribute("aria-pressed",String(button.dataset.mode===mode)));
      if (isImport()) $$('[data-group="media"]').forEach(input => { input.disabled=false; input.checked=importMedia.includes(input.value); });
      else exportConfiguration();
      $("#ie-review").hidden=isImport() && !preview;
      await loadPage();
      if (!isImport() && !exportOptions?.providers?.length) notice(T("importExport.page.noData"),"info",T("importExport.page.noDataHint"));
    }
    async function apply() {
      if (busy || loading || !data || !selectedCount()) return;
      const count=selectedCount(), importing=isImport(), params=parameters();
      const selection={mode:all ? (importing ? "ready" : "all") : "selected",row_ids:[...selected],excluded_row_ids:[...excluded]};
      cancelRead(); notice();
      progress(importing ? T("importExport.page.importing", {count}) : T("importExport.page.preparing"), importing ? T("importExport.page.writing") : T("importExport.page.building"));
      try {
        if (importing) {
          const result=await json("/api/import/commit",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({...params,...selection,import_id:preview.import_id,target_instance:$("#ie-target").value,features:checked("features"),media_types:checked("media"),include_existing:$("#ie-existing").checked})});
          const details=Object.entries(result.results || {}).map(([feature,value]) => `${human(feature)}: ${T("importExport.page.applied", {count: Number(value.count || 0)})}${value.ok===false ? ` ${T("importExport.page.needsAttention")}` : ""}`).join(" · ");
          notice(result.ok ? T("importExport.page.importComplete") : T("importExport.page.importIssues"),result.ok ? "success" : "error",`${T("importExport.page.resultSummary", {selected: count, applied: Number(result.applied || 0), skipped: Math.max(0,count-result.applied)})} ${details}${result.ok ? ` ${T("importExport.page.runPairs")}` : ` ${T("importExport.page.reviewTracker")}`}`);
          invalidatePreview();
        } else {
          const response=await fetch("/api/export/file",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({...params,...selection})});
          if (!response.ok) throw await responseError(response);
          const blob=await response.blob();
          const disposition=response.headers.get("Content-Disposition") || "";
          const name=disposition.match(/filename="?([^";]+)"?/i)?.[1] || `crosswatch-export.${blob.type.includes("zip") ? "zip" : "csv"}`;
          const url=URL.createObjectURL(blob), link=document.createElement("a");
          link.href=url; link.download=name; document.body.appendChild(link); link.click(); link.remove(); setTimeout(()=>URL.revokeObjectURL(url),4000);
          notice(T("importExport.page.downloadReadyTitle"),"success",T("importExport.page.downloadReady", {name, size: number(Math.ceil(blob.size/1024))}));
        }
      } catch (error) {
        if (importing) invalidatePreview();
        notice(importing ? T("importExport.page.importUnconfirmed") : T("importExport.page.exportFailed"),"error",`${error.message}${importing ? ` ${T("importExport.page.checkTracker")}` : ""}`);
      } finally { finishProgress(); $("#ie-notice").scrollIntoView({behavior:"smooth",block:"center"}); }
    }
    on(host,"click",event=> {
      const button=event.target.closest("button");
      if (!button || button.disabled || busy) return;
      if (["ie-options", "ie-selection-options"].includes(button.id)) {
        button.setAttribute("aria-expanded", String(button.getAttribute("aria-expanded") !== "true"));
        return;
      }
      if (button.dataset.mode) setMode(button.dataset.mode);
      else if (button.dataset.page != null) loadPage(Number(button.dataset.page));
      else if (button.id==="ie-browse") $("#ie-file").click();
      else if (button.id==="ie-preview-file") previewFile();
      else if (button.id==="ie-refresh") isImport() ? previewFile() : refresh();
      else if (button.id==="ie-select-all" || button.id==="ie-select-none") { all=button.id==="ie-select-all"; selected.clear(); excluded.clear(); render(); }
      else if (button.id==="ie-apply" || (button.id==="ie-download" && mode==="export")) apply();
    });
    on(host,"change",event=> {
      const input=event.target;
      if (input.id === "ie-mobile-mode") { setMode(input.value); return; }
      if (busy || mode === "recovery" || input.id === "ie-search" || input.closest("#ie-recovery")) return;
      if (input.dataset.row != null) {
        if (loading) return;
        const row=rows[Number(input.dataset.row)];
        if (!row || !eligible(row)) return;
        const key=rowKey(row);
        if (all) input.checked ? excluded.delete(key) : excluded.add(key);
        else input.checked ? selected.add(key) : selected.delete(key);
        input.closest("tr").classList.toggle("ie-row-selected",input.checked); updateSelection();
      } else if (input.id==="ie-file") { previewFile(input.files[0]); input.value=""; }
      else if (["ie-target","ie-source"].includes(input.id)) { $("#ie-new-name-wrap").hidden = $("#ie-target").value !== NEW_TRACKER; invalidatePreview(); notice(); guide(); }
      else if (input.id==="ie-page") loadPage((Math.min(Math.max(1,Number(input.value)||1),Math.max(1,Math.ceil(total()/pageSize)))-1)*pageSize);
      else if (input.id==="ie-page-size") { pageSize=Number(input.value); loadPage(); }
      else {
        if (input.dataset.group==="media" && !isImport()) exportMedia=checked("media");
        if (!isImport() && ["ie-provider","ie-feature","ie-format"].includes(input.id)) exportConfiguration(input.id);
        refresh();
      }
    });
    on($("#ie-search"),"input",()=> {
      cancelRead(); loading=true; data=null; resetSelection(); render();
      searchTimer=setTimeout(()=>loadPage(),250);
    });
    let dragDepth=0;
    on($("#ie-drop"),"dragenter",event=> { event.preventDefault(); if (!busy) { dragDepth++; $("#ie-drop").classList.add("ie-dragging"); } });
    on($("#ie-drop"),"dragover",event=> { event.preventDefault(); if (event.dataTransfer) event.dataTransfer.dropEffect=busy ? "none" : "copy"; });
    on($("#ie-drop"),"dragleave",event=> { event.preventDefault(); if (--dragDepth<=0) $("#ie-drop").classList.remove("ie-dragging"); });
    on($("#ie-drop"),"drop",event=> {
      event.preventDefault(); dragDepth=0; $("#ie-drop").classList.remove("ie-dragging");
      if (busy) return;
      const files=event.dataTransfer?.files;
      if (files?.length!==1) { notice(T("importExport.page.oneFile"),"error",T("importExport.page.oneFileHint")); return; }
      previewFile(files[0]);
    });
    on(host,"dragover",event=>event.preventDefault());
    on(host,"drop",event=>event.preventDefault());
    on(window,"cw:overview-profile-changed",()=> {
      if (busy) scopeChanged=true;
      else { ImportExport.unmount(); ImportExport.mount(host); }
    });
    progress(T("importExport.page.loadingOptions"), T("importExport.page.loadingOptionsHint"));
    try {
      [importOptions,exportOptions]=await Promise.all([json("/api/import/options",{signal:lifetime.signal}),json("/api/export/options",{signal:lifetime.signal})]);
      if (lifetime.signal.aborted) return;
      $("#ie-source").innerHTML=importOptions.sources.map(source=>option(source.id,source.id === "auto" ? T("importExport.page.autoDetect") : source.label)).join("");
      $("#ie-target").innerHTML=trackerOptions(importOptions.targets, option);
      const wanted=new URLSearchParams(window.location.hash.split("?")[1] || "").get("target");
      const connected=importOptions.targets.find(target=>target.id===wanted) || importOptions.targets.find(target=>target.connected);
      if (connected) $("#ie-target").value=connected.id;
      $("#ie-provider").innerHTML=exportOptions.providers.map(provider=>option(provider,provider)).join("");
      $("#ie-limit").textContent=`${number(importOptions.limits.upload_bytes/1024/1024)} MB`;
      guide();
      refreshSelects();
    } catch (error) {
      if (!lifetime.signal.aborted) { notice(T("importExport.page.optionsFailed"),"error",`${error.message} ${T("importExport.page.refreshRetry")}`); $("#ie-controls").dataset.unavailable="true"; }
    } finally {
      if (!lifetime.signal.aborted) { finishProgress(); if ($("#ie-controls").dataset.unavailable) $("#ie-controls").disabled=true; }
      if (!lifetime.signal.aborted && (window.CW?.pendingPlexRecovery || new URLSearchParams(window.location.hash.split("?")[1] || "").has("recovery"))) await setMode("recovery");
      const styled = await stylesheet;
      if (!lifetime.signal.aborted) {
        if (styled) {
          $("#ie-loading").remove();
          $(".ie-page").hidden = false;
        } else {
          $("#ie-loading").textContent = T("importExport.page.stylesFailed");
        }
      }
    }
  },
  unmount() { active?.cleanup(); active=null; }
};
window.ImportExport=ImportExport;
export default ImportExport;
