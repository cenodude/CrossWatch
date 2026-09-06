/* assets/js/import-export/index.js */
/* CrossWatch - Import and Export Page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const human = value => String(value || "").replace(/_/g, " ").replace(/^./, char => char.toUpperCase());
const number = value => Number(value || 0).toLocaleString();
const statuses = { all: "All results", ready: "Ready to import", exists: "Already in tracker", duplicate: "Duplicate", missing_identity: "Missing IDs", invalid: "Invalid data", unsupported: "Unsupported" };
const media = { movie: "Movies", show: "Shows", season: "Seasons", episode: "Episodes" };
const option = (value, label) => `<option value="${esc(value)}">${esc(label)}</option>`;
const select = (id, label, choices = "") => `<label class="ie-field" for="${id}"><span>${label}</span><select id="${id}" data-cw-native-select="true">${choices}</select></label>`;
const checks = (group, values) => `<div class="ie-checks">${Object.entries(values).map(([value, label]) => `<label><input type="checkbox" data-group="${group}" value="${value}" checked><span>${label}</span></label>`).join("")}</div>`;

async function responseError(response) {
  const data = await response.json().catch(() => ({}));
  const detail = data.detail;
  return new Error(typeof detail === "string" ? detail : detail?.detail || detail?.code || `Request failed (${response.status}).`);
}

async function json(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) throw await responseError(response);
  return response.json();
}

const template = `
<div class="ie-page">
  <a class="ie-back" href="#main">${icon("arrow_back")}Main</a>
  <header class="ie-header"><div><span class="ie-eyebrow">YOUR DATA</span><h1>Import and Export</h1></div>${icon("import_export")}</header>
  <nav class="ie-tabs" aria-label="Transfer direction"><button type="button" data-mode="import" aria-pressed="true">${icon("upload_file")}Import a file</button><button type="button" data-mode="export" aria-pressed="false">${icon("download")}Export your data</button></nav>
  <div id="ie-notice" class="ie-notice" role="status" hidden></div>
  <div id="ie-progress" class="ie-progress" role="status" hidden><div class="ie-progress-copy"><strong id="ie-activity"></strong><span id="ie-elapsed"></span></div><div class="ie-progress-track" role="progressbar" aria-label="Transfer in progress"><span></span></div><p id="ie-progress-note"></p></div>
  <fieldset id="ie-controls">
    <section id="ie-import-setup" class="ie-setup">
      <div id="ie-drop" class="ie-drop">
        <div class="ie-drop-icon">${icon("drive_folder_upload")}</div><h2>Drop your export file here</h2><p>ZIP, JSON, CSV or TXT · <span id="ie-limit">25 MB</span> max</p>
        <button type="button" class="ie-button ie-primary" id="ie-browse">${icon("add")}Choose file</button><input id="ie-file" type="file" accept=".zip,.json,.csv,.txt" hidden>
        <span class="ie-drop-hint">Trakt, Letterboxd, SIMKL, IMDb, TV Time and more</span>
        <div id="ie-file-info" class="ie-file-info" hidden></div>
      </div>
      <div class="ie-panel ie-destination"><span class="ie-eyebrow">IMPORT DESTINATION</span><h2>Your CrossWatch tracker</h2><p>Preview the file, choose what to keep, then import. Your sync pairs can send these items to other providers afterward.</p>
        <div class="ie-fields">${select("ie-target", "Target profile")}${select("ie-source", "File source")}</div>
        <details class="ie-guide"><summary>How do I get an export file?</summary><div id="ie-guide"></div></details>
      </div>
    </section>
    <section id="ie-export-setup" class="ie-panel" hidden><div class="ie-section-heading"><div><h2>Choose what to export</h2><p>Download from saved provider data. Run a sync first if you need a fresh snapshot.</p></div>${icon("download")}</div>
      <div class="ie-fields ie-export-fields">${select("ie-provider", "Provider")}${select("ie-instance", "Instance")}${select("ie-feature", "Feature", ["watchlist", "history", "ratings", "combined"].map(value => option(value, value === "combined" ? "History & ratings" : human(value))).join(""))}${select("ie-format", "Export format")}</div>
      <div class="ie-export-extras"><label id="ie-date-wrap"><input id="ie-date" type="checkbox" checked>Include watched dates</label><label id="ie-rewatch-wrap"><input id="ie-rewatch" type="checkbox" checked>Keep repeat watches</label></div>
    </section>
    <section class="ie-panel ie-inclusions"><div id="ie-features-wrap"><h3>Include features</h3>${checks("features", {history:"History",ratings:"Ratings",watchlist:"Watchlist"})}</div><div><h3>Media types</h3>${checks("media", media)}</div></section>
    <section id="ie-review" hidden>
      <div class="ie-review-heading"><div><span class="ie-eyebrow">REVIEW YOUR DATA</span><h2 id="ie-review-title">File preview</h2></div><button type="button" class="ie-button" id="ie-refresh">${icon("refresh")}Refresh preview</button></div>
      <div id="ie-metrics" class="ie-metrics"></div>
      <div id="ie-warnings" class="ie-warning" hidden></div>
      <div class="ie-filters"><label class="ie-search">${icon("search")}<input id="ie-search" type="search" maxlength="1000" placeholder="Search titles, years or IDs" aria-label="Search preview"></label>${select("ie-status", "Result", Object.entries(statuses).map(([value,label]) => option(value,label)).join(""))}</div>
      <div class="ie-list-bar"><span id="ie-range"></span><div class="ie-list-actions"><button type="button" class="ie-link" id="ie-select-all">Select all matching</button><button type="button" class="ie-link" id="ie-select-none">Clear selection</button><label id="ie-existing-wrap"><input type="checkbox" id="ie-existing">Include existing items</label>${select("ie-page-size", "Per page", [25,50,100,200].map(value => option(value,value)).join(""))}</div></div>
      <div class="ie-table-wrap"><table><thead><tr><th class="ie-check-cell"><span class="ie-sr">Selected</span></th><th>Item</th><th id="ie-column-feature">Feature</th><th>IDs</th><th id="ie-column-status">Result</th></tr></thead><tbody id="ie-rows"></tbody></table></div>
      <nav id="ie-pagination" class="ie-pagination" aria-label="Preview pages"></nav>
      <div class="ie-apply"><div><strong id="ie-selected"></strong><p id="ie-apply-note"></p></div><button type="button" id="ie-apply" class="ie-button ie-primary" disabled></button></div>
    </section>
  </fieldset>
</div>`;

let active;
const ImportExport = {
  async mount(host) {
    if (!host || active?.host === host) return;
    this.unmount();
    if (!document.getElementById("ie-styles")) {
      const link = document.createElement("link");
      link.id = "ie-styles";
      link.rel = "stylesheet";
      link.href = new URL(`./styles.css?v=${encodeURIComponent(window.__CW_VERSION__ || "1")}`, import.meta.url).href;
      document.head.appendChild(link);
    }
    host.innerHTML = template;
    const $ = selector => host.querySelector(selector);
    const $$ = selector => [...host.querySelectorAll(selector)];
    const lifetime = new AbortController();
    let mode = "import", busy = false, loading = false, file = null, preview = null, scopeChanged = false;
    let importOptions = null, exportOptions = null, data = null, rows = [], offset = 0, pageSize = 50;
    let all = true, selected = new Set(), excluded = new Set(), pageController, pageSequence = 0, searchTimer, progressTimer;
    let exportMedia = ["movie"], importMedia = Object.keys(media);
    const on = (element, event, fn) => element.addEventListener(event, fn, {signal:lifetime.signal});
    active = { host, cleanup() {
      lifetime.abort(); pageController?.abort(); clearTimeout(searchTimer); clearInterval(progressTimer);
      window.CW?.IconSelect?.closeAll();
      $$("select").forEach(input => { input.__cwOptionsObserver?.disconnect(); input.nextElementSibling?.__cwMenu?.remove(); });
    } };
    $("#ie-page-size").value = "50";
    function refreshSelects() {
      $$("select").forEach(input => {
        const wrap = window.CW?.IconSelect?.enhance(input, {className:"ie-select",menuClassName:"ie-select-menu"});
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
      $("#ie-selected").textContent = `${number(count)} ${count === 1 ? "item" : "items"} selected`;
      $("#ie-apply").innerHTML = `${icon(isImport() ? "upload" : "download")}${isImport() ? "Import" : "Download"} ${number(count)} ${count === 1 ? "item" : "items"}`;
      $("#ie-apply").disabled = busy || loading || !data || !count;
      $("#ie-apply-note").textContent = isImport() ? `Into ${$("#ie-target").selectedOptions[0]?.textContent || "CrossWatch"}. Only selected, importable items will be added.` : "Your selection is downloaded as a file. Provider data stays unchanged.";
      $("#ie-select-all").disabled = loading || !selectableTotal();
      $("#ie-select-none").disabled = loading || !count;
    }
    function render() {
      const importing = isImport();
      const counts = data?.summary || {};
      const metrics = importing ? [[counts.total,"Items in preview"],[counts.ready,"Ready to import"],[counts.exists,"Already in tracker"],[(counts.total || 0)-(counts.ready || 0)-(counts.exists || 0),"Not importable"]] : [[data?.matched_total,"Matching items"],[data?.total,"Ready to export"],[data?.dropped_total,"Not exportable"]];
      $("#ie-metrics").innerHTML = metrics.map(([value,label]) => `<div><strong>${number(value)}</strong><span>${label}</span></div>`).join("");
      $("#ie-range").textContent = loading ? "Loading preview…" : `${number(total() ? offset + 1 : 0)}–${number(Math.min(offset + pageSize,total()))} of ${number(total())} items`;
      $("#ie-rows").innerHTML = loading ? `<tr><td colspan="5" class="ie-empty">Loading preview…</td></tr>` : rows.map((row,index) => {
        const ids = Object.entries(row.ids || {}).filter(([,value]) => value != null && value !== "");
        const name = `${row.title || "Untitled item"}${row.season != null && row.episode != null ? ` · S${String(row.season).padStart(2,"0")}E${String(row.episode).padStart(2,"0")}` : ""}`;
        const result = importing ? statuses[row.status] || human(row.status) : [row.watched_at, row.rating != null ? `Rating: ${row.rating}` : ""].filter(Boolean).join(" · ") || "Ready";
        return `<tr${isSelected(row) ? ' class="ie-row-selected"' : ""}><td class="ie-check-cell"><input type="checkbox" data-row="${index}" aria-label="Select ${esc(name)}" ${isSelected(row) ? "checked" : ""} ${eligible(row) ? "" : "disabled"}></td><td><strong>${esc(name)}</strong><small>${esc([human(row.media_type || row.type),row.year].filter(Boolean).join(" · "))}</small></td><td>${esc(human(row.feature || $("#ie-feature").value))}</td><td class="ie-ids">${ids.slice(0,3).map(([key,value]) => `<span>${esc(key)}:${esc(value)}</span>`).join("")}${ids.length > 3 ? `<details><summary>+${ids.length-3} more</summary>${ids.slice(3).map(([key,value]) => `<span>${esc(key)}:${esc(value)}</span>`).join("")}</details>` : ""}</td><td><span class="ie-status" data-status="${esc(row.status || "ready")}">${esc(result)}</span>${importing && row.reason ? `<small>${esc(human(row.reason))}</small>` : ""}</td></tr>`;
      }).join("") || `<tr><td colspan="5" class="ie-empty">${data ? "No items match these filters." : "Preview unavailable. Try refreshing."}</td></tr>`;
      const pages = Math.max(1,Math.ceil(total()/pageSize)), current = Math.floor(offset/pageSize)+1;
      $("#ie-pagination").innerHTML = `${[[0,"keyboard_double_arrow_left","First"],[Math.max(0,offset-pageSize),"chevron_left","Previous"]].map(([start,ico,label]) => `<button type="button" class="ie-button" data-page="${start}" ${loading || current===1 ? "disabled" : ""}>${icon(ico)}${label}</button>`).join("")}<label>Page <input id="ie-page" type="number" min="1" max="${pages}" value="${current}" ${loading ? "disabled" : ""}> of ${number(pages)}</label>${[[offset+pageSize,"chevron_right","Next"],[(pages-1)*pageSize,"keyboard_double_arrow_right","Last"]].map(([start,ico,label]) => `<button type="button" class="ie-button" data-page="${start}" ${loading || current>=pages ? "disabled" : ""}>${label}${icon(ico)}</button>`).join("")}`;
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
        notice("Could not load the preview.","error",error.message);
      } finally {
        if (sequence === pageSequence && !lifetime.signal.aborted) { loading=false; render(); }
      }
    }
    function refresh() { resetSelection(); notice(); loadPage(); }
    function guide() {
      const source = $("#ie-source").value;
      const spec = importOptions?.source_guides?.[source];
      $("#ie-guide").innerHTML = spec ? `<strong>${esc(spec.title)}</strong><ol>${(spec.steps || []).map(step => `<li>${esc(step)}</li>`).join("")}</ol>` : "Download a data export or backup from your tracker, then drop it here. ZIP archives can stay zipped. Choose a file source above for specific instructions.";
    }
    function invalidatePreview() {
      cancelRead(); preview=null; data=null; rows=[]; loading=false; resetSelection(); $("#ie-review").hidden=true;
      $("#ie-file-info").innerHTML = file ? `${icon("description")}<span>${esc(file.name)}<small>${number(Math.ceil(file.size/1024))} KB</small></span><button type="button" class="ie-button" id="ie-preview-file">Preview file</button>` : "";
      $("#ie-file-info").hidden = !file;
      updateSelection();
    }
    async function previewFile(chosen = file) {
      if (busy || !chosen) return;
      const max = importOptions?.limits?.upload_bytes || 25*1024*1024;
      if (!/\.(zip|json|csv|txt)$/i.test(chosen.name)) { notice("Choose a ZIP, JSON, CSV or TXT export file.","error"); return; }
      if (chosen.size > max) { notice(`This file exceeds the ${number(max/1024/1024)} MB upload limit.`,"error"); return; }
      file=chosen;
      invalidatePreview();
      if (!importOptions?.targets?.find(target => target.id === $("#ie-target").value)?.connected) {
        notice("Connect a CrossWatch tracker before importing.","error","Choose a connected target profile, or connect one in Settings → Connections."); return;
      }
      notice();
      progress("Reading and checking your file", "Uploading, detecting the format and checking for existing items. Large files can take a little longer. Nothing is imported during preview.");
      const body = new FormData();
      body.append("file",file); body.append("source",$("#ie-source").value); body.append("target_instance",$("#ie-target").value);
      try {
        preview = await json("/api/import/preview",{method:"POST",body,signal:lifetime.signal});
        $("#ie-review-title").textContent = file.name;
        $("#ie-file-info").innerHTML = `${icon("task")}<span>${esc(file.name)}<small>${esc(human(preview.source))} · ${number(preview.total)} items found</small></span>`;
        notice("Your file is ready to review.","success","Nothing has been imported. Choose the items below, then import your selection.");
        await loadPage();
      } catch (error) {
        if (!lifetime.signal.aborted) notice("Could not preview this file.","error",error.message);
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
        $("#ie-instance").innerHTML = option("all","All instances")+instances.map(instance => option(instance.id,instance.label)).join("");
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
      if (isImport()) importMedia=checked("media"); else exportMedia=checked("media");
      mode=next; data=null; rows=[]; resetSelection(); notice();
      $("#ie-search").value="";
      $("#ie-import-setup").hidden=!isImport();
      $("#ie-export-setup").hidden=isImport();
      $("#ie-features-wrap").hidden=!isImport();
      $("#ie-existing-wrap").hidden=!isImport();
      $("#ie-status").closest("label").hidden=!isImport();
      $("#ie-review-title").textContent=isImport() ? file?.name || "File preview" : "Export preview";
      $("#ie-column-status").textContent=isImport() ? "Result" : "Watched / rating";
      $$("[data-mode]").forEach(button => button.setAttribute("aria-pressed",String(button.dataset.mode===mode)));
      if (isImport()) $$('[data-group="media"]').forEach(input => { input.disabled=false; input.checked=importMedia.includes(input.value); });
      else exportConfiguration();
      $("#ie-review").hidden=isImport() && !preview;
      await loadPage();
      if (!isImport() && !exportOptions?.providers?.length) notice("No saved provider data to export yet.","info","Run a sync, then return here to download a copy.");
    }
    async function apply() {
      if (busy || loading || !data || !selectedCount()) return;
      const count=selectedCount(), importing=isImport(), params=parameters();
      const selection={mode:all ? (importing ? "ready" : "all") : "selected",row_ids:[...selected],excluded_row_ids:[...excluded]};
      cancelRead(); notice();
      progress(importing ? `Importing ${number(count)} items` : "Preparing your download", importing ? "Writing your selection to the CrossWatch tracker. Keep this tab open until the result appears." : "Building your export file. Keep this tab open until the download starts.");
      try {
        if (importing) {
          const result=await json("/api/import/commit",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({...params,...selection,import_id:preview.import_id,target_instance:$("#ie-target").value,features:checked("features"),media_types:checked("media"),include_existing:$("#ie-existing").checked})});
          const details=Object.entries(result.results || {}).map(([feature,value]) => `${human(feature)}: ${number(value.count)} applied${value.ok===false ? " (needs attention)" : ""}`).join(" · ");
          notice(result.ok ? "Import complete" : "Import finished with issues",result.ok ? "success" : "error",`${number(count)} selected · ${number(result.applied)} applied · ${number(Math.max(0,count-result.applied))} not applied. ${details}${result.ok ? " Run your sync pairs when you are ready to send these items to other providers." : " Review the tracker before retrying the file."}`);
          invalidatePreview();
        } else {
          const response=await fetch("/api/export/file",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({...params,...selection})});
          if (!response.ok) throw await responseError(response);
          const blob=await response.blob();
          const disposition=response.headers.get("Content-Disposition") || "";
          const name=disposition.match(/filename="?([^";]+)"?/i)?.[1] || `crosswatch-export.${blob.type.includes("zip") ? "zip" : "csv"}`;
          const url=URL.createObjectURL(blob), link=document.createElement("a");
          link.href=url; link.download=name; document.body.appendChild(link); link.click(); link.remove(); setTimeout(()=>URL.revokeObjectURL(url),4000);
          notice("Your download is ready.","success",`${name} · ${number(Math.ceil(blob.size/1024))} KB. Check your browser downloads.`);
        }
      } catch (error) {
        if (importing) invalidatePreview();
        notice(importing ? "The import could not be confirmed." : "Could not create the export.","error",`${error.message}${importing ? " Check your tracker, then preview the file again before retrying." : ""}`);
      } finally { finishProgress(); $("#ie-notice").scrollIntoView({behavior:"smooth",block:"center"}); }
    }
    on(host,"click",event=> {
      const button=event.target.closest("button");
      if (!button || button.disabled || busy) return;
      if (button.dataset.mode) setMode(button.dataset.mode);
      else if (button.dataset.page != null) loadPage(Number(button.dataset.page));
      else if (button.id==="ie-browse") $("#ie-file").click();
      else if (button.id==="ie-preview-file") previewFile();
      else if (button.id==="ie-refresh") isImport() ? previewFile() : refresh();
      else if (button.id==="ie-select-all" || button.id==="ie-select-none") { all=button.id==="ie-select-all"; selected.clear(); excluded.clear(); render(); }
      else if (button.id==="ie-apply") apply();
    });
    on(host,"change",event=> {
      const input=event.target;
      if (busy || input.id === "ie-search") return;
      if (input.dataset.row != null) {
        if (loading) return;
        const row=rows[Number(input.dataset.row)];
        if (!row || !eligible(row)) return;
        const key=rowKey(row);
        if (all) input.checked ? excluded.delete(key) : excluded.add(key);
        else input.checked ? selected.add(key) : selected.delete(key);
        input.closest("tr").classList.toggle("ie-row-selected",input.checked); updateSelection();
      } else if (input.id==="ie-file") { previewFile(input.files[0]); input.value=""; }
      else if (["ie-target","ie-source"].includes(input.id)) { invalidatePreview(); notice(); guide(); }
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
      if (files?.length!==1) { notice("Drop one export file at a time.","error","For exports containing several files, upload the original ZIP."); return; }
      previewFile(files[0]);
    });
    on(host,"dragover",event=>event.preventDefault());
    on(host,"drop",event=>event.preventDefault());
    on(window,"cw:overview-profile-changed",()=> {
      if (busy) scopeChanged=true;
      else { ImportExport.unmount(); ImportExport.mount(host); }
    });
    progress("Loading import and export options", "Checking available profiles and saved provider data.");
    try {
      [importOptions,exportOptions]=await Promise.all([json("/api/import/options",{signal:lifetime.signal}),json("/api/export/options",{signal:lifetime.signal})]);
      $("#ie-source").innerHTML=importOptions.sources.map(source=>option(source.id,source.label)).join("");
      $("#ie-target").innerHTML=importOptions.targets.map(target=>option(target.id,`${target.label}${target.connected ? "" : " (not connected)"}`)).join("");
      const connected=importOptions.targets.find(target=>target.connected);
      if (connected) $("#ie-target").value=connected.id;
      $("#ie-provider").innerHTML=exportOptions.providers.map(provider=>option(provider,provider)).join("");
      $("#ie-limit").textContent=`${number(importOptions.limits.upload_bytes/1024/1024)} MB`;
      guide();
      refreshSelects();
    } catch (error) {
      if (!lifetime.signal.aborted) { notice("Could not load import/export options.","error",`${error.message} Refresh the page to try again.`); $("#ie-controls").dataset.unavailable="true"; }
    } finally {
      if (!lifetime.signal.aborted) { finishProgress(); if ($("#ie-controls").dataset.unavailable) $("#ie-controls").disabled=true; }
    }
  },
  unmount() { active?.cleanup(); active=null; }
};
window.ImportExport=ImportExport;
export default ImportExport;
