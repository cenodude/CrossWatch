/* assets/js/import-export/plex-recovery.js */
/* CrossWatch - Plex History Recovery */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[char]);
const number = value => Number(value || 0).toLocaleString(I18N()?.locale);
const labels = {ready:T("importExport.plexRecovery.status.ready"),needs_review:T("importExport.plexRecovery.status.review"),exists:T("importExport.plexRecovery.status.exists"),missing_identity:T("importExport.plexRecovery.status.missing"),invalid:T("importExport.plexRecovery.status.invalid"),duplicate:T("importExport.plexRecovery.status.duplicate"),unsupported:T("importExport.plexRecovery.status.unsupported")};
const selectable = row => ["ready","needs_review"].includes(row.status);
const option = (value, label) => `<option value="${esc(value)}">${esc(label)}</option>`;
const field = (id, title) => `<label class="ie-field" for="${id}"><span id="${id}-label">${title}</span><select id="${id}" data-cw-native-select="true"></select></label>`;
const watchDate = value => value ? new Date(value).toLocaleString(I18N()?.locale) : T("importExport.plexRecovery.missingDate");
function watchDetails(row) {
  if (row.watch_count == null) return "";
  const counts = [T("importExport.plexRecovery.watches", {count: Number(row.watch_count) || 0}), T("importExport.plexRecovery.newCount", {count: Number(row.pending_watch_count) || 0})];
  if (row.existing_watch_count) counts.push(T("importExport.plexRecovery.existingCount", {count: Number(row.existing_watch_count)}));
  if (row.missing_date_count) counts.push(T("importExport.plexRecovery.missingDateCount", {count: Number(row.missing_date_count)}));
  const details = (row.watch_details || []).map(watch => `<small>${esc(watchDate(watch.watched_at))} · ${esc(!watch.watched_at ? T("importExport.plexRecovery.skippedDate") : labels[watch.status] || watch.status)}</small>`).join("");
  return `<small>${counts.join(" · ")}</small>${details ? `<details><summary>${TH("importExport.plexRecovery.watchDetails")}</summary>${details}</details>` : ""}`;
}

export function mountPlexRecovery(host, { NEW_TRACKER, createTracker, trackerOptions }) {
  host.innerHTML = `<section class="ie-panel cw-page-panel">
    <div class="ie-section-heading"><div><h2>${TH("importExport.plexRecovery.title")}</h2><p>${TH("importExport.plexRecovery.intro")}</p></div></div>
    <div class="ie-fields">${field("pr-kind",T("importExport.plexRecovery.source"))}${field("pr-db",T("importExport.plexRecovery.database"))}${field("pr-tautulli",T("importExport.plexRecovery.tautulli"))}${field("pr-tracearr",T("importExport.plexRecovery.tracearr"))}${field("pr-source",T("importExport.plexRecovery.plexInstance"))}${field("pr-db-account",T("importExport.plexRecovery.dbAccount"))}${field("pr-target",T("importExport.plexRecovery.target"))}<label class="ie-field" for="pr-new-name" id="pr-new-name-wrap" hidden><span>${TH("importExport.plexRecovery.newName")}</span><input id="pr-new-name" type="text" maxlength="24" autocomplete="off" placeholder="${TH("importExport.plexRecovery.defaultName")}"></label></div>
    <div class="ie-export-extras"><label id="pr-live-wrap" hidden><input id="pr-live" type="checkbox" checked>${TH("importExport.plexRecovery.ignoreLive")}</label></div>
    <p id="pr-access">${TH("importExport.plexRecovery.ownerRequired")}</p>
    <div class="pr-actions"><button class="ie-button" id="pr-browse" hidden>${TH("importExport.plexRecovery.chooseFile")}</button><input id="pr-file" type="file" hidden><button class="ie-button ie-primary" id="pr-start">${TH("importExport.plexRecovery.scan")}</button><button class="ie-button" id="pr-cancel" hidden>${TH("importExport.plexRecovery.cancelScan")}</button><button class="ie-button" id="pr-close" hidden>${TH("importExport.plexRecovery.close")}</button></div>
    <details class="ie-guide"><summary>${TH("importExport.plexRecovery.how")}</summary>
      <p id="pr-scope"></p>
      <ol>
        <li>${TH("importExport.plexRecovery.how1")}</li>
        <li>${TH("importExport.plexRecovery.how2")}</li>
        <li>${TH("importExport.plexRecovery.how3")}</li>
      </ol>
      <p>${TH("importExport.plexRecovery.shared")}</p>
      <p>${TH("importExport.plexRecovery.expiry")}</p>
    </details>
    <div id="pr-message" class="ie-notice" role="status" hidden></div>
    <div id="pr-progress" class="ie-progress" role="status" hidden>
      <div class="ie-progress-copy"><strong id="pr-stage"></strong></div><div id="pr-bar" class="ie-progress-track" role="progressbar" aria-label="${TH("importExport.plexRecovery.progress")}" aria-valuemin="0" aria-valuemax="100"><span></span></div><p id="pr-count"></p>
      <div id="pr-activity" hidden><p><strong id="pr-current"></strong></p><p id="pr-live-counts"></p><p>${TH("importExport.plexRecovery.candidates")}</p>
        <div id="pr-recent-wrap" class="ie-table-wrap" hidden><table><thead><tr><th>${TH("importExport.plexRecovery.recentItem")}</th><th>${TH("importExport.plexRecovery.result")}</th></tr></thead><tbody id="pr-recent"></tbody></table></div>
      </div>
    </div>
  </section>
  <section id="pr-review" class="ie-panel cw-page-panel" hidden>
    <div class="ie-review-heading"><div><h2>${TH("importExport.plexRecovery.recovered")}</h2><p>${TH("importExport.plexRecovery.recoveredHint")}</p></div><button class="ie-button" id="pr-editor" hidden>${TH("importExport.plexRecovery.openEditor")}</button></div>
    <div id="pr-metrics" class="ie-metrics"></div>
    <div class="ie-filters"><label class="ie-search"><input id="pr-search" type="search" maxlength="1000" placeholder="${TH("importExport.plexRecovery.search")}" aria-label="${TH("importExport.plexRecovery.search")}"></label>${field("pr-result",T("importExport.plexRecovery.result"))}</div>
    <div class="ie-list-bar"><span id="pr-range"></span><div class="ie-list-actions"><button class="ie-button" id="pr-bulk-match">${TH("importExport.plexRecovery.matchUnresolved")}</button><button class="ie-button" id="pr-accept-titles" title="${TH("importExport.plexRecovery.acceptHint")}" hidden>${TH("importExport.plexRecovery.acceptAll")}</button><button class="ie-link" id="pr-all">${TH("importExport.plexRecovery.selectAll")}</button><button class="ie-link" id="pr-none">${TH("importExport.plexRecovery.clearSelection")}</button></div></div>
    <div class="ie-table-wrap"><table><thead><tr><th>${TH("importExport.plexRecovery.select")}</th><th>${TH("importExport.plexRecovery.historyMatch")}</th><th>${TH("importExport.plexRecovery.watched")}</th><th>${TH("importExport.plexRecovery.result")}</th><th>${TH("importExport.plexRecovery.match")}</th></tr></thead><tbody id="pr-rows"></tbody></table></div>
    <div class="ie-pagination"><button class="ie-button" id="pr-prev">${TH("importExport.plexRecovery.previous")}</button><span id="pr-page"></span><button class="ie-button" id="pr-next">${TH("importExport.plexRecovery.next")}</button></div>
    <div class="ie-apply"><div><strong id="pr-selected"></strong><p>${TH("importExport.plexRecovery.onlySelected")}</p></div><button class="ie-button ie-primary" id="pr-import">${TH("importExport.plexRecovery.import")}</button></div>
  </section>
  <dialog id="pr-match" class="cw-page-panel pr-match"><form method="dialog">
    <h2>${TH("importExport.plexRecovery.matchTitle")}</h2><p id="pr-match-original"></p><p>${TH("importExport.plexRecovery.matchHint")}</p>
    <div class="ie-fields"><label class="ie-field"><span id="pr-match-type-label">${TH("importExport.plexRecovery.mediaType")}</span><select id="pr-match-type" name="media_type" data-cw-native-select="true"><option value="movie">${TH("importExport.plexRecovery.movie")}</option><option value="episode">${TH("importExport.plexRecovery.episode")}</option></select></label><label class="ie-field"><span>${TH("importExport.plexRecovery.titleSeries")}</span><input name="title" required maxlength="512"></label>
    <label class="ie-field"><span id="pr-match-catalog-label">${TH("importExport.plexRecovery.searchProvider")}</span><select id="pr-match-catalog" name="catalog" data-cw-native-select="true"></select></label></div>
    <div class="pr-actions"><button type="button" class="ie-button" id="pr-match-search">${TH("importExport.plexRecovery.searchTitles")}</button></div><div id="pr-match-results" class="pr-actions" aria-live="polite"></div>
    <div class="ie-fields">
    ${["tmdb","tvdb","imdb","season","episode"].map(key => `<label class="ie-field"><span>${({tmdb:"TMDb ID",tvdb:"TVDB ID",imdb:"IMDb ID",season:T("importExport.plexRecovery.season"),episode:T("importExport.plexRecovery.episode")})[key]}</span><input name="${key}" maxlength="20" ${["season","episode"].includes(key) ? 'type="number" min="0"' : ''}></label>`).join("")}</div>
    <p id="pr-match-error" role="alert"></p><div class="pr-actions"><button class="ie-button" value="cancel" formnovalidate>${TH("importExport.plexRecovery.cancel")}</button><button class="ie-button ie-primary" id="pr-match-save" value="save">${TH("importExport.plexRecovery.saveMatch")}</button></div>
  </form></dialog>`;
  const $ = selector => host.querySelector(selector);
  const lifetime = new AbortController();
  const on = (el, type, fn) => el.addEventListener(type, fn, {signal:lifetime.signal});
  const api = "/api/import/plex-recovery";
  let job = null, rows = [], data = null, sources = [], options = null, offset = 0, upload = null;
  let otherRecovery = false;
  let busy = false, reading = false, loading = false, all = false, chosen = new Set(), excluded = new Set();
  let timer, searchTimer, sequence = 0, editing = null, receipt = null, matchingWorkspace = null;
  let pollSequence = 0, stoppingRecovery = false, matchSequence = 0, matchDialogSequence = 0, matchRevision = null, matchQuery = "", matchResults = [];
  async function request(path, body, method = body ? "POST" : "GET", signal = lifetime.signal) {
    const response = await fetch(api + path, {method, signal,
      ...(body ? {headers:{"Content-Type":"application/json"},body:JSON.stringify(body)} : {})});
    const result = await response.json();
    if (!response.ok) {
      if (response.status === 404 && result.detail === "Recovery expired or not found. Start another scan."
          && job && path.split("/")[1]?.split("?")[0] === job.id && !lifetime.signal.aborted) {
        resetRecovery();
        message(result.detail,true);
      }
      throw Object.assign(new Error(typeof result.detail === "string" ? result.detail : result.detail?.detail || T("importExport.plexRecovery.requestFailed")), {status:response.status});
    }
    return result;
  }
  function resetRecovery() {
    clearTimeout(timer);clearTimeout(searchTimer);++sequence;++pollSequence;
    job=null;data=null;rows=[];receipt=null;editing=null;offset=0;
    busy=false;reading=false;loading=false;otherRecovery=false;stoppingRecovery=false;
    all=false;chosen.clear();excluded.clear();
    matchingWorkspace?.destroy();matchingWorkspace=null;
    $("#pr-match").close();
    $("#pr-progress").hidden=true;
    $("#pr-search").value="";$("#pr-result").value="all";
    window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
    render();
  }
  function message(text, error = false) {
    $("#pr-message").hidden = !text;
    $("#pr-message").dataset.kind = error ? "error" : "info";
    $("#pr-message").textContent = text;
  }
  function filters() { return {q:$("#pr-search").value.trim(),status:$("#pr-result").value}; }
  function count() {
    const ready = ["all","ready"].includes(filters().status) ? data?.summary?.ready || 0 : 0;
    return all ? Math.max(0,ready-excluded.size) + chosen.size : chosen.size;
  }
  function controls() {
    const locked = busy || reading || loading;
    if (busy || reading) window.CW?.IconSelect?.closeAll();
    host.querySelectorAll("button,input,select").forEach(el => { if (!el.closest("dialog")) el.disabled = busy || reading || (loading && !el.closest(".ie-filters")); });
    $("#pr-cancel").disabled = false;
    $("#pr-cancel").hidden = !reading;
    $("#pr-start").disabled = locked || !!job || otherRecovery || !$("#pr-source").value || ($("#pr-kind").value === "tautulli" && !(options?.tautulli?.find(t=>t.id === $("#pr-tautulli").value)?.user)) || ($("#pr-kind").value === "tracearr" && !(options?.tracearr?.find(t=>t.id === $("#pr-tracearr").value)?.user)) || ($("#pr-kind").value === "plex_db" && $("#pr-db").value === "upload" && !(upload && $("#pr-db-account").value)) || !($("#pr-target").value === NEW_TRACKER || options?.targets?.find(t => t.id === $("#pr-target").value)?.connected);
    $("#pr-close").hidden = !job || reading;
    for (const id of ["pr-kind","pr-db","pr-tautulli","pr-tracearr","pr-source","pr-db-account","pr-target","pr-new-name","pr-browse","pr-live"]) $(`#${id}`).disabled = locked || !!job;
    $("#pr-new-name-wrap").hidden = !!job || $("#pr-target").value !== NEW_TRACKER;
    $("#pr-import").disabled = locked || job?.auto_match?.status === "running" || !count();
    $("#pr-accept-titles").hidden = !data?.summary?.by_status?.needs_review || !["all","needs_review"].includes(filters().status);
    $("#pr-accept-titles").disabled = locked || job?.auto_match?.status === "running";
    $("#pr-bulk-match").textContent = job?.auto_match?.status === "running" ? T("importExport.plexRecovery.viewAuto") : job?.auto_match?.id ? T("importExport.plexRecovery.reviewAuto") : T("importExport.plexRecovery.matchUnresolved");
    $("#pr-prev").disabled = locked || offset === 0;
    $("#pr-next").disabled = locked || offset + 50 >= (data?.filtered_total || 0);
    $("#pr-selected").textContent = T("importExport.plexRecovery.itemsSelected", {count: count()});
    $("#pr-editor").hidden = !receipt?.keys?.length;
    rows.forEach(row => { const el = $(`[data-select="${row.id}"]`); if (el) el.disabled = locked || !selectable(row); });
  }
  function render() {
    const stats = data?.summary || {};
    $("#pr-review").hidden = !data;
    $("#pr-metrics").innerHTML = [[stats.total,T("importExport.plexRecovery.items")],[stats.ready,T("importExport.plexRecovery.status.ready")],[stats.exists,T("importExport.plexRecovery.status.exists")],[(stats.by_status?.missing_identity || 0)+(stats.by_status?.needs_review || 0),T("importExport.plexRecovery.needReview")]].map(([n,label]) => `<div><strong>${number(n)}</strong><span>${label}</span></div>`).join("");
    $("#pr-rows").innerHTML = rows.map(row => {
      const selected = selectable(row) && (all && row.status === "ready" ? !excluded.has(row.id) : chosen.has(row.id));
      const ids = row.media_type === "episode" ? row.show_ids : row.ids;
      const title = row.media_type === "episode" ? row.title.replace(/ - (S\d+E\d+)$/, " | $1") : row.title;
      return `<tr${selected ? ' class="ie-row-selected"' : ''}><td><input type="checkbox" data-select="${esc(row.id)}" aria-label="${TH(row.status === "needs_review" ? "importExport.plexRecovery.confirmSelect" : "importExport.plexRecovery.selectTitle", {title: row.title})}" ${selected ? "checked" : ""} ${!selectable(row) ? "disabled" : ""}></td>
      <td><strong>${esc(title)}</strong><small>Original: ${esc(row.original_title)}${row.media_type === "episode" ? ` · S${row.season ?? "?"} E${row.episode ?? "?"}` : ""}</small><small>${esc(row.recovery_match?.replaceAll(" — ", " | "))}</small><small>${esc(Object.entries(ids || {}).filter(([k]) => k !== "plex").map(([k,v])=>`${k}:${v}`).join(" · "))}</small></td>
      <td>${esc(watchDate(row.watched_at))}${watchDetails(row)}</td><td><span class="ie-status" data-status="${esc(row.status)}">${esc(labels[row.status] || row.status)}</span>${row.status === "duplicate" ? "<small>" + TH("importExport.plexRecovery.duplicateHint") + "</small>" : ""}</td>
      <td><button class="ie-button" data-match="${esc(row.id)}">${TH("importExport.plexRecovery.editMatch")}</button></td></tr>`;
    }).join("") || '<tr><td colspan="5" class="ie-empty">' + TH("importExport.plexRecovery.noMatches") + '</td></tr>';
    const total = data?.filtered_total || 0;
    $("#pr-range").textContent = `${number(total ? offset+1 : 0)}–${number(Math.min(offset+50,total))} of ${number(total)}`;
    $("#pr-page").textContent = T("importExport.plexRecovery.pageOf", {page: Math.floor(offset/50)+1, pages: Math.max(1,Math.ceil(total/50))});
    controls();
  }
  async function page(start = 0) {
    if (!job) return;
    clearTimeout(searchTimer);
    const ticket = ++sequence, revision = data?.revision ?? job.revision;
    loading = true; controls(); offset = start;
    try {
      const result = await request(`/${job.id}/rows?${new URLSearchParams({...filters(),offset,limit:50})}`);
      if (ticket !== sequence) return;
      if (revision != null && result.revision != null && revision !== result.revision) {
        all=false;chosen.clear();excluded.clear();
      }
      data=result; job={...job,...result}; rows=result.rows || [];
    } catch (error) {
      if (ticket === sequence && !lifetime.signal.aborted) {
        data={summary:{},filtered_total:0};rows=[];all=false;chosen.clear();excluded.clear();message(error.message,true);
      }
    } finally { if (ticket === sequence) {loading=false;render();} }
  }
  async function poll() {
    if(!job)return;
    clearTimeout(timer);
    const ticket=++pollSequence, id=job.id;
    try {
      const current=await request(`/${id}`);
      if(ticket !== pollSequence || lifetime.signal.aborted)return;
      if(data?.revision != null && current.revision != null && data.revision !== current.revision) {
        all=false;chosen.clear();excluded.clear();
      }
      job=current;
      const changed=!data || data.import_id !== job.id || data.status !== job.status || data.revision !== job.revision;
      reading = job.status === "reading";
      $("#pr-progress").hidden = !reading;
      $("#pr-stage").textContent = job.stage === "title_matching" ? `${T("importExport.plexRecovery.scanComplete")} · ${job.message}` : job.message;
      $("#pr-count").textContent = job.total ? `${number(job.done)} / ${number(job.total)}` : job.stage === "review" ? T("importExport.plexRecovery.checkingDuplicates") : job.source === "tautulli" ? T("importExport.plexRecovery.waitTautulli") : job.source === "tracearr" ? T("importExport.plexRecovery.waitTracearr") : T("importExport.plexRecovery.waitPlex");
      if(job.stage === "title_matching") $("#pr-count").textContent += " candidates checked";
      const bar=$("#pr-bar"), fill=bar.querySelector("span");
      const percent=job.total ? Math.min(100,Math.max(0,job.done/job.total*100)) : null;
      if (percent !== null) bar.setAttribute("aria-valuenow",String(percent)); else bar.removeAttribute("aria-valuenow");
      bar.setAttribute("aria-valuetext",$("#pr-count").textContent);
      fill.style.width=percent !== null ? `${percent}%` : "";
      fill.style.animation=percent !== null ? "none" : "";
      renderActivity();
      controls();
      if (reading) timer = setTimeout(poll,1000);
      else {
        const auto=job.auto_match;
        message(auto?.id ? `${auto.message} ${T("importExport.plexRecovery.autoChecked", {done: Number(auto.done || 0), total: Number(auto.total || 0), action: auto.status === "running" ? T("importExport.plexRecovery.viewAuto") : T("importExport.plexRecovery.reviewAuto")})}` : job.imported ? T("importExport.plexRecovery.importedWatches", {count: Number(job.imported)}) : job.message,job.status === "error");
        window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
        if (job.status === "review" && changed) {
          const imported=await request(`/${id}/receipt`);
          if(ticket !== pollSequence || lifetime.signal.aborted)return;
          receipt=imported;
          await page(offset);
        }
        if(ticket === pollSequence && auto?.status === "running") timer=setTimeout(poll,3000);
      }
    } catch (error) {
      if (ticket === pollSequence && !lifetime.signal.aborted) {
        if ([403,404].includes(error.status)) resetRecovery();
        else if (reading || !error.status || error.status === 429 || error.status >= 500) timer=setTimeout(poll,5000);
        message(error.message,true);controls();
      }
    }
  }
  function renderActivity() {
    const activity=job?.activity || {}, recent=job?.recent || [];
    $("#pr-activity").hidden=activity.checked == null;
    $("#pr-current").textContent=activity.current_item ? `${activity.current_action || T("importExport.plexRecovery.lastChecked")}: ${activity.current_item}` : "";
    const checkedKey=job?.stage === "title_matching" ? "importExport.plexRecovery.liveRows" : "importExport.plexRecovery.liveChecked";
    $("#pr-live-counts").textContent=T(checkedKey,{checked:Number(activity.checked||0),found:Number(activity.found||0),skipped:Number(activity.skipped||0)});
    $("#pr-recent-wrap").hidden=!recent.length;
    $("#pr-recent").innerHTML=[...recent].reverse().map(item=>`<tr><td>${esc(item.title)}</td><td>${esc(item.result)}</td></tr>`).join("");
  }
  async function start() {
    busy=true; controls(); message();
    try {
      if ($("#pr-target").value === NEW_TRACKER) {
        const made = await createTracker($("#pr-new-name").value.trim() || T("importExport.plexRecovery.defaultName"));
        options.targets.push(made);
        $("#pr-target").innerHTML = trackerOptions(options.targets, option);
        $("#pr-target").value = made.id;
        $("#pr-new-name").value = "";
      }
      const sourceKind=$("#pr-kind").value, fromFile=sourceKind === "plex_db" && $("#pr-db").value === "upload";
      job = await request("",{source_instance:$("#pr-source").value,target_instance:$("#pr-target").value,ignore_live_tv:$("#pr-live").checked,
        ...(sourceKind === "tautulli" ? {source:"tautulli",tautulli_instance:$("#pr-tautulli").value} : {}),
        ...(sourceKind === "tracearr" ? {source:"tracearr",tracearr_instance:$("#pr-tracearr").value} : {}),
        ...(sourceKind === "plex_db" ? {source:"plex_db",...(fromFile ? {upload_id:upload.upload_id,db_account:Number($("#pr-db-account").value)} : {})} : {})});
      if (fromFile) upload=null;
      window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
      data=null; rows=[]; receipt=null; all=false; chosen.clear(); excluded.clear();
      busy=false; reading=true; render(); await poll();
    } catch (error) {
      busy=false;message(error.message,true);
      if(error.status === 409) { try {await resume();} catch(resumeError) {message(resumeError.message,true);} }
      controls();
    }
  }
  async function resume() {
    const current=await request("/active");
    if (!current.job && job) resetRecovery();
    otherRecovery=current.busy && !current.job;
    if (!current.job) {
      if(current.stopping) {
        stoppingRecovery=true;
        message(T("importExport.plexRecovery.closedWaiting"));
        clearTimeout(timer);
        timer=setTimeout(()=>resume().catch(error=>{if(!lifetime.signal.aborted)message(error.message,true);}),1000);
      } else if(otherRecovery) message(T("importExport.plexRecovery.otherOpen"));
      else if(stoppingRecovery) {stoppingRecovery=false;message(T("importExport.plexRecovery.closedNew"));}
      controls();
      return;
    }
    job=current.job;
    if(job.unavailable) {
      reading=false;data=null;rows=[];receipt=null;all=false;chosen.clear();excluded.clear();
      $("#pr-progress").hidden=true;
      render();message(job.message,true);
      return;
    }
    $("#pr-kind").value=job.source || "plex";
    if(job.database) $("#pr-db").value=job.database;
    if(job.tautulli_instance) $("#pr-tautulli").value=job.tautulli_instance;
    if(job.tracearr_instance) $("#pr-tracearr").value=job.tracearr_instance;
    $("#pr-source").value=job.source_instance;
    $("#pr-target").value=job.target_instance;
    $("#pr-live").checked=job.ignore_live_tv !== false;
    updateScope();
    await poll();
  }
  async function uploadDatabase(file) {
    busy=true;controls();message(T("importExport.plexRecovery.uploading", {name: file.name}));
    try {
      const body=new FormData();body.append("file",file);
      const response=await fetch(`${api}/database`,{method:"POST",body,signal:lifetime.signal});
      const result=await response.json().catch(()=>({}));
      if(!response.ok) throw new Error(typeof result.detail === "string" ? result.detail : T("importExport.plexRecovery.dbUnreadable"));
      upload=result;
      $("#pr-db-account").innerHTML=result.accounts.map(a=>`<option value="${esc(a.id)}">${esc(a.name)} · ${TH("importExport.plexRecovery.plays", {count: Number(a.plays || 0)})}</option>`).join("");
      const user=(options?.lookups || []).find(s=>s.id === $("#pr-source").value)?.user;
      const own=result.accounts.find(a=>user && a.name.toLowerCase() === String(user).toLowerCase());
      if(own) $("#pr-db-account").value=String(own.id);
      message(result.accounts.length ? T("importExport.plexRecovery.fileReady", {name: result.filename}) : T("importExport.plexRecovery.fileEmpty", {name: result.filename}),!result.accounts.length);
    } catch(error) {if(!lifetime.signal.aborted){upload=null;$("#pr-db-account").innerHTML="";message(error.message,true);}}
    finally {busy=false;$("#pr-file").value="";if(!lifetime.signal.aborted){updateScope();controls();}}
  }
  async function closeRecovery() {
    const id=job.id;
    let released=false;
    busy=true;clearTimeout(timer);++pollSequence;controls();
    try {
      await request(`/${job.id}`,null,"DELETE");
      if(lifetime.signal.aborted || job?.id !== id)return;
      released=true;
      resetRecovery();busy=true;controls();
      message(T("importExport.plexRecovery.closed"));
      await resume();
    } catch(error) {if(!lifetime.signal.aborted && (released || job?.id === id))message(error.message,true);}
    finally {if(!lifetime.signal.aborted && (released || job?.id === id)){busy=false;render();}}
  }
  async function commit() {
    const id=job.id;
    busy=true;controls();message(T("importExport.plexRecovery.importing"));
    try {
      const result=await request(`/${job.id}/commit`,{...filters(),import_id:job.id,revision:data?.revision ?? job.revision,target_instance:job.target_instance,
        mode:all ? "ready" : "selected",row_ids:[...chosen],excluded_row_ids:[...excluded],features:["history"],media_types:["movie","episode"]},"POST",null);
      window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
      if(lifetime.signal.aborted || job?.id !== id || result.revision < job.revision)return;
      job.revision=result.revision;
      receipt={provider:"CROSSWATCH",instance:job.target_instance,feature:"history",keys:result.imported_keys};
      message(T(result.ok ? "importExport.plexRecovery.importDone" : "importExport.plexRecovery.importIssues", {count: Number(result.applied || 0)}),!result.ok);
      chosen.clear();excluded.clear();all=false;await page();
    } catch(error) {
      if(!lifetime.signal.aborted && job?.id === id) {
        if(job && (!error.status || [409,429].includes(error.status) || error.status >= 500)) await poll();
        if(!lifetime.signal.aborted && job?.id === id)message(`${error.message} ${T("importExport.plexRecovery.checkTracker")}`,true);
      }
    }
    finally {if(!lifetime.signal.aborted && job?.id === id){busy=false;controls();}}
  }
  async function acceptTitleMatches() {
    const id=job.id;
    const total=data?.summary?.by_status?.needs_review || 0;
    if (!total || !(await (window.CW?.MobileUI?.ask || window.confirm)(T("importExport.plexRecovery.acceptConfirm", {count: total})))) return;
    busy=true;controls();
    try {
      const result=await request(`/${job.id}/accept-title-matches`,{revision:data?.revision ?? job.revision,q:filters().q});
      if (lifetime.signal.aborted || job?.id !== id || result.revision < job.revision) return;
      job={...job,...result};all=false;chosen.clear();excluded.clear();
      if (filters().status === "needs_review") {
        $("#pr-result").value="ready";
        $("#pr-result").dispatchEvent(new Event("change"));
      }
      await page();
      if(lifetime.signal.aborted || job?.id !== id)return;
      message(T("importExport.plexRecovery.accepted", {count: Number(result.accepted || 0)}));
      window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
    } catch(error) {if(!lifetime.signal.aborted && job?.id === id)message(error.message,true);}
    finally {if(!lifetime.signal.aborted && job?.id === id){busy=false;controls();}}
  }
  async function bulkMatch() {
    const id=job.id;
    busy=true;controls();
    try {
      const context=await request(`/${job.id}/match-groups?${new URLSearchParams({revision:job.revision,q:filters().q})}`);
      if(lifetime.signal.aborted || job?.id !== id)return;
      if (!context.rows.length) {message(T("importExport.plexRecovery.noUnresolved"));busy=false;controls();return;}
      const version=encodeURIComponent(window.APP_VERSION || "1");
      if (!document.querySelector('link[href*="/assets/css/interactive-sync.css"]')) {
        const style=document.createElement("link");style.rel="stylesheet";style.href=`/assets/css/interactive-sync.css?v=${version}`;document.head.append(style);
      }
      const {openMappingWorkspace}=await import(`/assets/js/interactive-sync-mapping.js?v=${version}`);
      if (lifetime.signal.aborted || job?.id !== id) return;
      const revision=context.revision;
      matchingWorkspace=openMappingWorkspace({rows:context.rows,total:context.total,recovery:true,
        scope:"recovery",scopes:[{id:"recovery",label:T("importExport.plexRecovery.thisRecovery")}],
        onClose:()=>{if(!lifetime.signal.aborted && job?.id === id){matchingWorkspace=null;busy=false;controls();}},
        onSaved:async result=>{
          if(lifetime.signal.aborted || job?.id !== id || result.revision < job.revision)return;
          matchingWorkspace=null;job={...job,...result};all=false;chosen.clear();excluded.clear();busy=false;
          message(T("importExport.plexRecovery.eventsUpdated", {count: Number(result.matched_events || 0)}));
          await page();
        },
        mappingApi:{
          catalogs:async()=>context,
          autoStart:async(group_ids,catalog,language)=>{
            const result=await request(`/${id}/auto-match`,{revision,group_ids,catalog,language});
            if(lifetime.signal.aborted || job?.id !== id)return result;
            job.auto_match=result.auto_match;
            window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
            clearTimeout(timer);timer=setTimeout(poll,3000);
            return result;
          },
          autoStatus:()=>request(`/${id}/auto-match`),
          autoStop:()=>request(`/${id}/auto-match`,null,"DELETE"),
          search:(row,q,catalog,options)=>request(`/${id}/match-search?${new URLSearchParams({revision,group_id:row.id,q,catalog,language:options?.language || "en-US",media_type:row.item.type === "movie" ? "movie" : "episode"})}`,null,"GET",options?.signal),
          save:edits=>request(`/${id}/match-groups`,{revision,edits:edits.map(edit=>({group_id:edit.row_id,title:edit.item.title,
            media_type:edit.item.type === "movie" ? "movie" : "episode",...edit.matchSearch,
            ids:Object.fromEntries(Object.entries(edit.item.ids || {}).filter(([key])=>["tmdb","tvdb","imdb"].includes(key)).map(([key,value])=>[key,String(value)]))}))})
        }
      });
    } catch(error) {if(!lifetime.signal.aborted && job?.id === id){message(error.message,true);busy=false;controls();}}
  }
  function matchTypeChanged(clear = true) {
    const form = $("#pr-match form"), episode = form.elements.media_type.value === "episode";
    ++matchSequence;matchQuery="";matchResults=[];$("#pr-match-results").innerHTML="";
    $("#pr-match-error").textContent="";
    $("#pr-match-search").disabled=!form.elements.catalog.value;
    for (const key of ["season","episode"]) {
      form.elements[key].closest("label").hidden=!episode;
      form.elements[key].required=episode;
    }
    if (clear) for (const key of ["tmdb","tvdb","imdb"]) form.elements[key].value="";
  }
  async function edit(row) {
    editing=row;matchRevision=data?.revision ?? job.revision;
    const dialogTicket=++matchDialogSequence, id=job.id;
    const current=()=>!lifetime.signal.aborted && job?.id === id && matchDialogSequence === dialogTicket && $("#pr-match").open;
    const form=$("#pr-match form"), ids=row.media_type === "episode" ? row.show_ids : row.ids;
    form.elements.media_type.value=row.media_type;
    form.elements.media_type.dispatchEvent(new Event("change"));
    form.elements.catalog.innerHTML="";
    for(const key of ["title","tmdb","tvdb","imdb","season","episode"]) form.elements[key].value = key === "title" ? row.original_title || row.title : ["season","episode"].includes(key) ? row[key] ?? "" : ids?.[key] || "";
    matchTypeChanged(false);
    $("#pr-match-save").disabled=false;
    $("#pr-match-original").textContent=T("importExport.plexRecovery.original", {title: row.original_title});
    $("#pr-match-error").textContent="";window.CW?.MobileUI?.openDialog($("#pr-match")) || $("#pr-match").showModal();
    try {
      const context=await request(`/${job.id}/match-groups?${new URLSearchParams({revision:matchRevision})}`);
      if(!current())return;
      form.elements.catalog.innerHTML=context.catalogs.map(c=>option(c.id,c.label)).join("");
      $("#pr-match-search").disabled=!context.catalogs.length;
      if(!context.catalogs.length)$("#pr-match-error").textContent=T("importExport.plexRecovery.configureSearch");
    } catch(error) {if(current())$("#pr-match-error").textContent=error.message;}
  }
  on($("#pr-match form").elements.media_type,"change",()=>matchTypeChanged());
  on($("#pr-match form").elements.catalog,"change",()=>matchTypeChanged(false));
  on($("#pr-match"),"close",()=>{++matchSequence;++matchDialogSequence;});
  on($("#pr-match-search"),"click",async()=>{
    const form=$("#pr-match form"), query=form.elements.title.value.trim(), ticket=++matchSequence;
    matchQuery="";matchResults=[];$("#pr-match-results").innerHTML="";
    if(!query){$("#pr-match-error").textContent=T("importExport.plexRecovery.enterTitle");return;}
    if(query.length>200){$("#pr-match-error").textContent=T("importExport.plexRecovery.titleTooLong");return;}
    $("#pr-match-search").disabled=true;$("#pr-match-error").textContent=T("importExport.plexRecovery.searching");
    try {
      const result=await request(`/${job.id}/match-search?${new URLSearchParams({revision:matchRevision,row_id:editing.id,
        q:query,catalog:form.elements.catalog.value,media_type:form.elements.media_type.value})}`);
      if(ticket!==matchSequence)return;
      matchQuery=query;matchResults=result.results;
      $("#pr-match-results").innerHTML=matchResults.map((item,index)=>`<button type="button" class="ie-button" data-match-result="${index}" ${item.mapping_unavailable ? "disabled" : ""}>${esc(item.title)}${item.year ? ` (${esc(item.year)})` : ""}${item.mapping_unavailable ? ` · ${esc(item.mapping_unavailable)}` : ""}</button>`).join("");
      $("#pr-match-error").textContent=matchResults.length ? T("importExport.plexRecovery.selectResult") : T("importExport.plexRecovery.noResults");
    } catch(error) {if(ticket===matchSequence)$("#pr-match-error").textContent=error.message;}
    finally {if(ticket===matchSequence)$("#pr-match-search").disabled=false;}
  });
  on($("#pr-match-results"),"click",event=>{
    const button=event.target.closest("[data-match-result]");
    if(!button || button.disabled)return;
    const item=matchResults[Number(button.dataset.matchResult)], form=$("#pr-match form");
    if(!item || item.mapping_unavailable)return;
    form.elements.title.value=item.title;
    for(const key of ["tmdb","tvdb","imdb"])form.elements[key].value=item.ids?.[key] || "";
    $("#pr-match-results").innerHTML="";$("#pr-match-error").textContent=T("importExport.plexRecovery.matchSelected");
  });
  on($("#pr-match form"),"submit",async event=>{
    if (event.submitter?.value !== "save") return;
    event.preventDefault(); const form=event.currentTarget;
    const id=job.id, target=editing, revision=matchRevision, ticket=matchSequence, dialogTicket=matchDialogSequence;
    const sameDialog=()=>!lifetime.signal.aborted && job?.id === id && matchDialogSequence === dialogTicket;
    const current=()=>sameDialog() && matchSequence === ticket;
    $("#pr-match-save").disabled=true;
    try {
      const values=Object.fromEntries(new FormData(form));
      values.season=values.season === "" ? null : Number(values.season);values.episode=values.episode === "" ? null : Number(values.episode);
      if(values.media_type==="movie")values.season=values.episode=null;
      const result=await request(`/${id}/match`,{...values,match_query:matchQuery,row_id:target.id,revision});
      if(lifetime.signal.aborted || job?.id !== id || result.revision < job.revision)return;
      job=result;
      if(sameDialog())matchRevision=result.revision;
      if(current())$("#pr-match").close();
      all=false;chosen.clear();excluded.clear();await page(offset);
    } catch(error) {if(current())$("#pr-match-error").textContent=error.message;}
    finally {if(sameDialog())$("#pr-match-save").disabled=false;}
  });
  on(host,"click",async event=>{
    const button=event.target.closest("button");if(!button || button.disabled || button.closest("dialog")) return;
    try {
      if(button.id === "pr-cancel") {await request(`/${job.id}`,null,"DELETE");message(T("importExport.plexRecovery.cancelling"));return;}
      if(busy || reading || loading) return;
      if(button.id === "pr-start") return start();
      if(button.id === "pr-browse") return $("#pr-file").click();
      if(button.id === "pr-close") return closeRecovery();
      if(button.id === "pr-import") return commit();
      if(button.id === "pr-bulk-match") return bulkMatch();
      if(button.id === "pr-accept-titles") return acceptTitleMatches();
      if(button.id === "pr-prev") return page(Math.max(0,offset-50));
      if(button.id === "pr-next") return page(offset+50);
      if(button.id === "pr-all" || button.id === "pr-none") {all=button.id === "pr-all";chosen.clear();excluded.clear();render();}
      if(button.dataset.match) edit(rows.find(r=>r.id===button.dataset.match));
      if(button.id === "pr-editor" && receipt) {
        (window.CW ||= {}).pendingRecovery=receipt;
        window.location.hash="#editor";
        window.dispatchEvent(new CustomEvent("cw:open-recovery"));
      }
    } catch(error) {message(error.message,true);}
  });
  on(host,"change",event=>{
    if(event.target.dataset.select) {
      const key=event.target.dataset.select, row=rows.find(r=>r.id === key);
      if (!row || !selectable(row) || busy || reading || loading) return;
      if(all && row.status === "ready") event.target.checked ? excluded.delete(key) : excluded.add(key);
      else event.target.checked ? chosen.add(key) : chosen.delete(key);
      render();
    }
    if(event.target.id === "pr-result") {all=false;chosen.clear();excluded.clear();page();}
    if(event.target.id === "pr-file" && event.target.files?.[0] && !job && !busy && !reading) return uploadDatabase(event.target.files[0]);
    if(["pr-kind","pr-db","pr-tautulli","pr-tracearr","pr-source","pr-db-account","pr-target"].includes(event.target.id)) {
      if(job || busy || reading)return;
      clearTimeout(searchTimer);++sequence;loading=false;
      data=null;rows=[];receipt=null;chosen.clear();excluded.clear();all=false;render();
      updateScope();
    }
  });
  on($("#pr-search"),"input",()=>{
    clearTimeout(searchTimer);++sequence;
    all=false;chosen.clear();excluded.clear();loading=!!job;controls();
    searchTimer=setTimeout(()=>page(),250);
  });
  $("#pr-result").innerHTML='<option value="all">' + TH("importExport.plexRecovery.allResults") + '</option>'+Object.entries(labels).map(([key,label])=>`<option value="${key}">${label}</option>`).join("");
  function openRequestedSource() {
    const pending=window.CW?.pendingPlexRecovery;
    if (!pending || !options) return;
    if (job) { window.CW.pendingPlexRecovery=null;return; }
    if (busy || reading || loading) return;
    if (sources.some(s=>s.id === pending.source_instance)) $("#pr-source").value=pending.source_instance;
    window.CW.pendingPlexRecovery=null;
    $("#pr-source").dispatchEvent(new Event("change",{bubbles:true}));
  }
  function kind() { return ["tautulli","tracearr","plex_db"].includes($("#pr-kind").value) ? $("#pr-kind").value : "plex"; }
  function accounts() { return [...sources, ...(options?.lookups || []).filter(l=>!sources.some(s=>s.id === l.id))]; }
  function uploading() { return kind() === "plex_db" && $("#pr-db").value === "upload"; }
  let filledKind=null;
  function fillSources() {
    const mode=`${kind()}:${uploading()}`;
    $("#pr-db").closest(".ie-field").hidden=kind() !== "plex_db";
    $("#pr-db-account").closest(".ie-field").hidden=!uploading() || !upload;
    $("#pr-browse").hidden=!uploading();
    $("#pr-live-wrap").hidden=["plex","tracearr"].includes(kind());
    $("#pr-browse").textContent=upload ? T("importExport.plexRecovery.chooseAnother") : T("importExport.plexRecovery.chooseFile");
    if(!options || filledKind === mode) return;
    filledKind=mode;
    const lookup=kind() === "tautulli" || uploading(), list=(kind() === "tracearr" ? accounts() : lookup ? options?.lookups : sources) || [], current=job?.source_instance || $("#pr-source").value;
    $("#pr-source").innerHTML=list.map(s=>`<option value="${esc(s.id)}">${esc(s.label)} · ${esc(s.user)}</option>`).join("");
    if(list.some(s=>s.id === current)) $("#pr-source").value=current;
    $("#pr-source-label").textContent=kind() === "tracearr" ? T("importExport.plexRecovery.plexAccount") : lookup ? T("importExport.plexRecovery.plexLookup") : T("importExport.plexRecovery.plexInstance");
    $("#pr-tautulli").closest(".ie-field").hidden=kind() !== "tautulli";
    $("#pr-tracearr").closest(".ie-field").hidden=kind() !== "tracearr";
    $("#pr-access").textContent=kind() === "tracearr" ? T("importExport.plexRecovery.tracearrAccess")
      : kind() === "tautulli" ? T("importExport.plexRecovery.tautulliAccess")
      : uploading() ? T("importExport.plexRecovery.uploadAccess")
      : kind() === "plex_db" ? T("importExport.plexRecovery.dbAccess")
      : T("importExport.plexRecovery.ownerRequired");
  }
  function updateScope() {
    fillSources();
    if(uploading()) {
      $("#pr-scope").textContent=upload ? T("importExport.plexRecovery.scopeFile", {name: upload.filename}) : T("importExport.plexRecovery.chooseDbFile");
      return;
    }
    if(kind() === "plex_db") {
      const source=sources.find(s=>s.id === $("#pr-source").value);
      $("#pr-scope").textContent=source ? `${T("importExport.plexRecovery.scopeDb", {user: source.user})}${source.libraries.length ? ` ${T("importExport.plexRecovery.scopeDbLibraries")}` : ""}` : "";
      return;
    }
    if(kind() === "tautulli") {
      const tautulli=options?.tautulli?.find(t=>t.id === $("#pr-tautulli").value);
      $("#pr-scope").textContent=tautulli ? `${tautulli.user ? T("importExport.plexRecovery.tautulliUser", {user: tautulli.user}) : T("importExport.plexRecovery.tautulliChoose")} ${T("importExport.plexRecovery.tautulliChecks")}` : "";
      return;
    }
    if(kind() === "tracearr") {
      const tracearr=options?.tracearr?.find(t=>t.id === $("#pr-tracearr").value);
      $("#pr-scope").textContent=tracearr ? (tracearr.user ? T("importExport.plexRecovery.tracearrUser", {user: tracearr.user}) : T("importExport.plexRecovery.tracearrChoose")) : "";
      return;
    }
    const source=sources.find(s=>s.id === $("#pr-source").value);
    $("#pr-scope").textContent=source ? `${T("importExport.plexRecovery.plexUser", {user: source.user})} ${T(source.libraries.length ? "importExport.plexRecovery.usesSelection" : "importExport.plexRecovery.allLibraries")}` : "";
  }
  on(window,"cw:open-plex-recovery",openRequestedSource);
  host.querySelectorAll("select").forEach(input=>{
    const wrap=window.CW?.IconSelect?.enhance(input,{className:"ie-select",menuClassName:"ie-select-menu",sortAlphabetically:["pr-source","pr-target","pr-tautulli","pr-tracearr"].includes(input.id),sortFirstValues:[NEW_TRACKER]});
    wrap?.querySelector("button")?.setAttribute("aria-labelledby",`${input.id}-label`);
    wrap?.__cwMenu?.setAttribute("aria-labelledby",`${input.id}-label`);
  });
  for(const id of ["pr-kind","pr-db","pr-tautulli","pr-tracearr","pr-db-account"]) $(`#${id}`).closest(".ie-field").hidden=true;
  busy=true;controls();
  request("/options").then(async result=>{
    options=result;sources=result.sources;
    $("#pr-kind").innerHTML='<option value="plex">Plex</option><option value="plex_db">' + TH("importExport.plexRecovery.plexDatabase") + '</option>'+((result.tautulli || []).length ? '<option value="tautulli">Tautulli</option>' : "")+((result.tracearr || []).length ? '<option value="tracearr">Tracearr</option>' : "");
    $("#pr-db").innerHTML='<option value="server">' + TH("importExport.plexRecovery.fromServer") + '</option><option value="upload">' + TH("importExport.plexRecovery.uploadFile") + '</option>';
    $("#pr-tautulli").innerHTML=(result.tautulli || []).map(t=>`<option value="${esc(t.id)}">${esc(t.label)}</option>`).join("");
    $("#pr-tracearr").innerHTML=(result.tracearr || []).map(t=>`<option value="${esc(t.id)}">${esc(t.label)}</option>`).join("");
    $("#pr-kind").closest(".ie-field").hidden=false;
    $("#pr-target").innerHTML=trackerOptions(result.targets, option);
    const connected=result.targets.find(t=>t.connected);if(connected) $("#pr-target").value=connected.id;
    if(!sources.length && !(result.lookups || []).length) message(T("importExport.plexRecovery.connectPlex"));
    updateScope();
    await resume();
  }).catch(error=>{if(!lifetime.signal.aborted) message(error.message,true);}).finally(()=>{busy=false;openRequestedSource();controls();});
  return () => {
    matchingWorkspace?.destroy();
    ++sequence;++pollSequence;clearTimeout(timer);clearTimeout(searchTimer);lifetime.abort();$("#pr-match").close();
    window.CW?.IconSelect?.closeAll();
    host.querySelectorAll("select").forEach(input=>{input.__cwOptionsObserver?.disconnect();input.nextElementSibling?.__cwMenu?.remove();});
  };
}
