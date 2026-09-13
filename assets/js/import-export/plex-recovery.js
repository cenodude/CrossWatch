/* assets/js/import-export/plex-recovery.js */
/* CrossWatch - Plex History Recovery */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[char]);
const number = value => Number(value || 0).toLocaleString();
const labels = {ready:"Ready to import",needs_review:"Review title match",exists:"Already in tracker",missing_identity:"Needs a match",invalid:"Invalid history",duplicate:"Duplicate in scan",unsupported:"Unsupported"};
const selectable = row => ["ready","needs_review"].includes(row.status);
const field = (id, title) => `<label class="ie-field" for="${id}"><span id="${id}-label">${title}</span><select id="${id}" data-cw-native-select="true"></select></label>`;

export function mountPlexRecovery(host) {
  host.innerHTML = `<section class="ie-panel cw-page-panel">
    <div class="ie-section-heading"><div><h2>Recover Plex history</h2><p>Recover watched movies and episodes removed from Plex into your CW tracker.</p></div></div>
    <div class="ie-fields">${field("pr-source","Plex instance")}${field("pr-target","CrossWatch tracker profile")}</div>
    <p>Plex server owner access required.</p>
    <div class="pr-actions"><button class="ie-button ie-primary" id="pr-start">Scan older history</button><button class="ie-button" id="pr-cancel" hidden>Cancel scan</button><button class="ie-button" id="pr-close" hidden>Close recovery</button></div>
    <details class="ie-guide"><summary>How recovery works</summary>
      <p id="pr-scope"></p>
      <ol>
        <li>Scan in the background and return through the notification bell. Only one recovery can be open at a time.</li>
        <li>Review matches before importing. Other search providers can identify titles, but cannot retrieve history Plex no longer exposes.</li>
        <li>Import your selection into the CW tracker. Scheduled pairs using that tracker can sync it afterward.</li>
      </ol>
      <p>Shared or managed-user connections may lack access to the server's full play history.</p>
      <p>Recovery results expire after 24 hours of inactivity, when you choose Close recovery, or when CrossWatch restarts. Leaving this page or closing the matching dialog keeps your recovery available. Imported history stays in your tracker.</p>
    </details>
    <div id="pr-message" class="ie-notice" role="status" hidden></div>
    <div id="pr-progress" class="ie-progress" role="status" hidden>
      <div class="ie-progress-copy"><strong id="pr-stage"></strong></div><div id="pr-bar" class="ie-progress-track" role="progressbar" aria-label="Recovery progress" aria-valuemin="0" aria-valuemax="100"><span></span></div><p id="pr-count"></p>
      <div id="pr-activity" hidden><p><strong id="pr-current"></strong></p><p id="pr-live-counts"></p><p>Found records are candidates for review. Repeated records are combined using the most recent watch date. Nothing is imported during the scan.</p>
        <div id="pr-recent-wrap" class="ie-table-wrap" hidden><table><thead><tr><th>Recent item</th><th>Result</th></tr></thead><tbody id="pr-recent"></tbody></table></div>
      </div>
    </div>
  </section>
  <section id="pr-review" class="ie-panel cw-page-panel" hidden>
    <div class="ie-review-heading"><div><h2>Recovered movies and episodes</h2><p>Review title guesses individually or use Accept all title matches. Select all matching skips unaccepted guesses. Repeated records are combined into one item using the most recent watch date.</p></div><button class="ie-button" id="pr-editor" hidden>Open imported items in Editor</button></div>
    <div id="pr-metrics" class="ie-metrics"></div>
    <div class="ie-filters"><label class="ie-search"><input id="pr-search" type="search" maxlength="1000" placeholder="Search recovered history" aria-label="Search recovered history"></label>${field("pr-result","Result")}</div>
    <div class="ie-list-bar"><span id="pr-range"></span><div class="ie-list-actions"><button class="ie-button" id="pr-bulk-match">Match unresolved items</button><button class="ie-button" id="pr-accept-titles" title="Accept suggested title identities across all pages of the current search. This does not import history." hidden>Accept all title matches</button><button class="ie-link" id="pr-all">Select all matching</button><button class="ie-link" id="pr-none">Clear selection</button></div></div>
    <div class="ie-table-wrap"><table><thead><tr><th>Select</th><th>Plex history / match</th><th>Watched</th><th>Result</th><th>Match</th></tr></thead><tbody id="pr-rows"></tbody></table></div>
    <div class="ie-pagination"><button class="ie-button" id="pr-prev">Previous</button><span id="pr-page"></span><button class="ie-button" id="pr-next">Next</button></div>
    <div class="ie-apply"><div><strong id="pr-selected"></strong><p>Only selected, valid watch events are imported. Events already in the tracker are skipped.</p></div><button class="ie-button ie-primary" id="pr-import">Import selected history</button></div>
  </section>
  <dialog id="pr-match" class="cw-page-panel pr-match"><form method="dialog">
    <h2>Match recovered history</h2><p id="pr-match-original"></p><p>For episodes, enter the show's IDs and the season and episode numbers.</p>
    <div class="ie-fields"><label class="ie-field"><span>Title / series</span><input name="title" required maxlength="512"></label>
    ${["tmdb","tvdb","imdb","season","episode"].map(key => `<label class="ie-field"><span>${({tmdb:"TMDb ID",tvdb:"TVDB ID",imdb:"IMDb ID",season:"Season",episode:"Episode"})[key]}</span><input name="${key}" maxlength="20" ${["season","episode"].includes(key) ? 'type="number" min="0"' : ''}></label>`).join("")}</div>
    <p id="pr-match-error" role="alert"></p><div class="pr-actions"><button class="ie-button" value="cancel">Cancel</button><button class="ie-button ie-primary" id="pr-match-save" value="save">Save match</button></div>
  </form></dialog>`;
  const $ = selector => host.querySelector(selector);
  const lifetime = new AbortController();
  const on = (el, type, fn) => el.addEventListener(type, fn, {signal:lifetime.signal});
  const api = "/api/import/plex-recovery";
  let job = null, rows = [], data = null, sources = [], options = null, offset = 0;
  let otherRecovery = false;
  let busy = false, reading = false, loading = false, all = false, chosen = new Set(), excluded = new Set();
  let timer, searchTimer, sequence = 0, editing = null, receipt = null, matchingWorkspace = null;
  let pollSequence = 0, stoppingRecovery = false;
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
      throw Object.assign(new Error(typeof result.detail === "string" ? result.detail : result.detail?.detail || "Recovery request failed."), {status:response.status});
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
    $("#pr-start").disabled = locked || !!job || otherRecovery || !$("#pr-source").value || !options?.targets?.find(t => t.id === $("#pr-target").value)?.connected;
    $("#pr-close").hidden = !job || reading;
    for (const id of ["pr-source","pr-target"]) $(`#${id}`).disabled = locked || !!job;
    $("#pr-import").disabled = locked || job?.auto_match?.status === "running" || !count();
    $("#pr-accept-titles").hidden = !data?.summary?.by_status?.needs_review || !["all","needs_review"].includes(filters().status);
    $("#pr-accept-titles").disabled = locked || job?.auto_match?.status === "running";
    $("#pr-bulk-match").textContent = job?.auto_match?.status === "running" ? "View Auto match progress" : job?.auto_match?.id ? "Review Auto matches" : "Match unresolved items";
    $("#pr-prev").disabled = locked || offset === 0;
    $("#pr-next").disabled = locked || offset + 50 >= (data?.filtered_total || 0);
    $("#pr-selected").textContent = `${number(count())} watch ${count() === 1 ? "event" : "events"} selected`;
    $("#pr-editor").hidden = !receipt?.keys?.length;
    rows.forEach(row => { const el = $(`[data-select="${row.id}"]`); if (el) el.disabled = locked || !selectable(row); });
  }
  function render() {
    const stats = data?.summary || {};
    $("#pr-review").hidden = !data;
    $("#pr-metrics").innerHTML = [[stats.total,"Items"],[stats.ready,"Ready to import"],[stats.exists,"Already in tracker"],[(stats.by_status?.missing_identity || 0)+(stats.by_status?.needs_review || 0),"Need review / a match"]].map(([n,label]) => `<div><strong>${number(n)}</strong><span>${label}</span></div>`).join("");
    $("#pr-rows").innerHTML = rows.map(row => {
      const selected = selectable(row) && (all && row.status === "ready" ? !excluded.has(row.id) : chosen.has(row.id));
      const ids = row.media_type === "episode" ? row.show_ids : row.ids;
      const title = row.media_type === "episode" ? row.title.replace(/ - (S\d+E\d+)$/, " | $1") : row.title;
      return `<tr${selected ? ' class="ie-row-selected"' : ''}><td><input type="checkbox" data-select="${esc(row.id)}" aria-label="${row.status === "needs_review" ? "Confirm title match and select" : "Select"} ${esc(row.title)}" ${selected ? "checked" : ""} ${!selectable(row) ? "disabled" : ""}></td>
      <td><strong>${esc(title)}</strong><small>Original: ${esc(row.original_title)}${row.media_type === "episode" ? ` · S${row.season ?? "?"} E${row.episode ?? "?"}` : ""}</small><small>${esc(row.recovery_match?.replaceAll(" — ", " | "))}</small><small>${esc(Object.entries(ids || {}).filter(([k]) => k !== "plex").map(([k,v])=>`${k}:${v}`).join(" · "))}</small></td>
      <td>${esc(row.watched_at ? new Date(row.watched_at).toLocaleString() : "Missing watch date")}</td><td><span class="ie-status" data-status="${esc(row.status)}">${esc(labels[row.status] || row.status)}</span>${row.status === "duplicate" ? "<small>Same matched item and watch time as another event in this scan.</small>" : ""}</td>
      <td><button class="ie-button" data-match="${esc(row.id)}">Edit match</button></td></tr>`;
    }).join("") || '<tr><td colspan="5" class="ie-empty">No recovered history matches this view.</td></tr>';
    const total = data?.filtered_total || 0;
    $("#pr-range").textContent = `${number(total ? offset+1 : 0)}–${number(Math.min(offset+50,total))} of ${number(total)}`;
    $("#pr-page").textContent = `Page ${Math.floor(offset/50)+1} of ${Math.max(1,Math.ceil(total/50))}`;
    controls();
  }
  async function page(start = 0) {
    if (!job) return;
    clearTimeout(searchTimer);
    const ticket = ++sequence;
    loading = true; controls(); offset = start;
    try {
      const result = await request(`/${job.id}/rows?${new URLSearchParams({...filters(),offset,limit:50})}`);
      if (ticket !== sequence) return;
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
      job=current;
      const changed=!data || data.import_id !== job.id || data.status !== job.status || data.revision !== job.revision;
      reading = job.status === "reading";
      $("#pr-progress").hidden = !reading;
      $("#pr-stage").textContent = job.message;
      $("#pr-count").textContent = job.total ? `${number(job.done)} / ${number(job.total)}` : job.stage === "review" ? "Checking duplicates and existing watch events…" : "Waiting for Plex…";
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
        message(auto?.id ? `${auto.message} ${number(auto.done)} / ${number(auto.total)} titles checked. Use ${auto.status === "running" ? "View Auto match progress" : "Review Auto matches"} to return to matching.` : job.imported ? `${number(job.imported)} ${job.imported === 1 ? "item" : "items"} imported into CrossWatch. Review the remaining items or open the imported items in Editor.` : job.message,job.status === "error");
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
        else if (reading) timer=setTimeout(poll,5000);
        message(error.message,true);controls();
      }
    }
  }
  function renderActivity() {
    const activity=job?.activity || {}, recent=job?.recent || [];
    $("#pr-activity").hidden=activity.checked == null;
    $("#pr-current").textContent=activity.current_item ? `${activity.current_action || "Last checked"}: ${activity.current_item}` : "";
    $("#pr-live-counts").textContent=`${number(activity.checked)} checked · ${number(activity.found)} found for review · ${number(activity.skipped)} skipped`;
    $("#pr-recent-wrap").hidden=!recent.length;
    $("#pr-recent").innerHTML=[...recent].reverse().map(item=>`<tr><td>${esc(item.title)}</td><td>${esc(item.result)}</td></tr>`).join("");
  }
  async function start() {
    busy=true; controls(); message();
    try {
      job = await request("",{source_instance:$("#pr-source").value,target_instance:$("#pr-target").value});
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
        message("Recovery closed. Waiting for its last provider request to stop. Imported history stays in your CW tracker.");
        clearTimeout(timer);
        timer=setTimeout(()=>resume().catch(error=>{if(!lifetime.signal.aborted)message(error.message,true);}),1000);
      } else if(otherRecovery) message("Another user has a Plex recovery open. Wait until it is closed or expires.");
      else if(stoppingRecovery) {stoppingRecovery=false;message("Recovery closed. You can start a new scan. Imported history stays in your CW tracker.");}
      controls();
      return;
    }
    job=current.job;
    $("#pr-source").value=job.source_instance;
    $("#pr-target").value=job.target_instance;
    updateScope();
    await poll();
  }
  async function closeRecovery() {
    busy=true;clearTimeout(timer);++pollSequence;controls();
    try {
      await request(`/${job.id}`,null,"DELETE");
      resetRecovery();busy=true;controls();
      message("Recovery closed. Imported history stays in your CW tracker.");
      await resume();
    } catch(error) {message(error.message,true);}
    finally {busy=false;render();}
  }
  async function commit() {
    busy=true;controls();message("Importing selected watch events into CrossWatch…");
    try {
      const result=await request(`/${job.id}/commit`,{...filters(),import_id:job.id,revision:job.revision,target_instance:job.target_instance,
        mode:all ? "ready" : "selected",row_ids:[...chosen],excluded_row_ids:[...excluded],features:["history"],media_types:["movie","episode"]},"POST",null);
      window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
      if(lifetime.signal.aborted)return;
      receipt = await request(`/${job.id}/receipt`);
      message(`${result.ok ? "Import complete" : "Import finished with issues"}. ${number(result.applied)} watch ${result.applied === 1 ? "event" : "events"} applied. Open imported items in Editor to send just this recovery to another provider.`,!result.ok);
      chosen.clear();excluded.clear();all=false;await page();
    } catch(error) {if(!lifetime.signal.aborted)message(`${error.message} Check the tracker before retrying.`,true);}
    finally {busy=false;if(!lifetime.signal.aborted)controls();}
  }
  async function acceptTitleMatches() {
    const total=data?.summary?.by_status?.needs_review || 0;
    if (!total || !window.confirm(`Accept all ${number(total)} suggested title matches across all pages of this search? Title guesses can be incorrect. Nothing will be imported until you select items and import them.`)) return;
    busy=true;controls();
    try {
      const result=await request(`/${job.id}/accept-title-matches`,{revision:job.revision,q:filters().q});
      if (lifetime.signal.aborted) return;
      job={...job,...result};all=false;chosen.clear();excluded.clear();
      if (filters().status === "needs_review") {
        $("#pr-result").value="ready";
        $("#pr-result").dispatchEvent(new Event("change"));
      }
      await page();
      message(`${number(result.accepted)} title matches accepted. Select the items you want to import. Nothing has been imported.`);
      window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
    } catch(error) {if(!lifetime.signal.aborted)message(error.message,true);}
    finally {busy=false;if(!lifetime.signal.aborted)controls();}
  }
  async function bulkMatch() {
    busy=true;controls();
    try {
      const context=await request(`/${job.id}/match-groups?${new URLSearchParams({revision:job.revision,q:filters().q})}`);
      if (!context.rows.length) {message("No unresolved movies or series match this search.");busy=false;controls();return;}
      const version=encodeURIComponent(window.APP_VERSION || "1");
      if (!document.querySelector('link[href*="/assets/css/interactive-sync.css"]')) {
        const style=document.createElement("link");style.rel="stylesheet";style.href=`/assets/css/interactive-sync.css?v=${version}`;document.head.append(style);
      }
      const {openMappingWorkspace}=await import(`/assets/js/interactive-sync-mapping.js?v=${version}`);
      if (lifetime.signal.aborted) return;
      const revision=context.revision, id=job.id;
      matchingWorkspace=openMappingWorkspace({rows:context.rows,total:context.total,recovery:true,
        scope:"recovery",scopes:[{id:"recovery",label:"This recovery"}],
        onClose:()=>{matchingWorkspace=null;busy=false;controls();},
        onSaved:async result=>{
          if(lifetime.signal.aborted)return;
          matchingWorkspace=null;job={...job,...result};all=false;chosen.clear();excluded.clear();busy=false;
          message(`${number(result.matched_events)} watch ${result.matched_events === 1 ? "event" : "events"} updated. Review the results before importing.`);
          await page();
        },
        mappingApi:{
          catalogs:async()=>context,
          autoStart:async(group_ids,catalog,language)=>{
            const result=await request(`/${id}/auto-match`,{revision,group_ids,catalog,language});
            job.auto_match=result.auto_match;
            window.dispatchEvent(new CustomEvent("cw:plex-recovery-changed"));
            clearTimeout(timer);timer=setTimeout(poll,3000);
            return result;
          },
          autoStatus:()=>request(`/${id}/auto-match`),
          autoStop:()=>request(`/${id}/auto-match`,null,"DELETE"),
          search:(row,q,catalog,options)=>request(`/${id}/match-search?${new URLSearchParams({revision,group_id:row.id,q,catalog,language:options?.language || "en-US"})}`,null,"GET",options?.signal),
          save:edits=>request(`/${id}/match-groups`,{revision,edits:edits.map(edit=>({group_id:edit.row_id,title:edit.item.title,
            ids:Object.fromEntries(Object.entries(edit.item.ids || {}).filter(([key])=>["tmdb","tvdb","imdb"].includes(key)).map(([key,value])=>[key,String(value)]))}))})
        }
      });
    } catch(error) {if(!lifetime.signal.aborted)message(error.message,true);busy=false;controls();}
  }
  function edit(row) {
    editing=row; const form=$("#pr-match form"), ids=row.media_type === "episode" ? row.show_ids : row.ids;
    for(const key of ["title","tmdb","tvdb","imdb","season","episode"]) form.elements[key].value = key === "title" ? row.original_title || row.title : ["season","episode"].includes(key) ? row[key] ?? "" : ids?.[key] || "";
    for(const key of ["season","episode"]) form.elements[key].closest("label").hidden=row.media_type !== "episode";
    $("#pr-match-original").textContent=`Original Plex record: ${row.original_title}`;
    $("#pr-match-error").textContent="";$("#pr-match").showModal();
  }
  on($("#pr-match form"),"submit",async event=>{
    if (event.submitter?.value !== "save") return;
    event.preventDefault(); const form=event.currentTarget;
    $("#pr-match-save").disabled=true;
    try {
      const values=Object.fromEntries(new FormData(form));
      values.season=values.season === "" ? null : Number(values.season);values.episode=values.episode === "" ? null : Number(values.episode);
      job=await request(`/${job.id}/match`,{...values,row_id:editing.id,revision:job.revision});
      $("#pr-match").close();all=false;chosen.clear();excluded.clear();await page(offset);
    } catch(error) {$("#pr-match-error").textContent=error.message;}
    finally {$("#pr-match-save").disabled=false;}
  });
  on(host,"click",async event=>{
    const button=event.target.closest("button");if(!button || button.disabled || button.closest("dialog")) return;
    try {
      if(button.id === "pr-cancel") {await request(`/${job.id}`,null,"DELETE");message("Cancelling after the current Plex request…");return;}
      if(busy || reading || loading) return;
      if(button.id === "pr-start") return start();
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
    if(["pr-source","pr-target"].includes(event.target.id)) {
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
  $("#pr-result").innerHTML='<option value="all">All results</option>'+Object.entries(labels).map(([key,label])=>`<option value="${key}">${label}</option>`).join("");
  function openRequestedSource() {
    const pending=window.CW?.pendingPlexRecovery;
    if (!pending || !options) return;
    if (job) { window.CW.pendingPlexRecovery=null;return; }
    if (busy || reading || loading) return;
    if (sources.some(s=>s.id === pending.source_instance)) $("#pr-source").value=pending.source_instance;
    window.CW.pendingPlexRecovery=null;
    $("#pr-source").dispatchEvent(new Event("change",{bubbles:true}));
  }
  function updateScope() {
    const source=sources.find(s=>s.id === $("#pr-source").value);
    $("#pr-scope").textContent=source ? `Plex user: ${source.user}. ${source.libraries.length ? "Uses the configured history library selection." : "Checks all accessible movie and TV libraries."}` : "";
  }
  on(window,"cw:open-plex-recovery",openRequestedSource);
  host.querySelectorAll("select").forEach(input=>{
    const wrap=window.CW?.IconSelect?.enhance(input,{className:"ie-select",menuClassName:"ie-select-menu"});
    wrap?.querySelector("button")?.setAttribute("aria-labelledby",`${input.id}-label`);
    wrap?.__cwMenu?.setAttribute("aria-labelledby",`${input.id}-label`);
  });
  busy=true;controls();
  request("/options").then(async result=>{
    options=result;sources=result.sources;
    $("#pr-source").innerHTML=sources.map(s=>`<option value="${esc(s.id)}">${esc(s.label)} · ${esc(s.user)}</option>`).join("");
    $("#pr-target").innerHTML=result.targets.map(t=>`<option value="${esc(t.id)}">${esc(t.label)}${t.connected ? "" : " (not connected)"}</option>`).join("");
    const connected=result.targets.find(t=>t.connected);if(connected) $("#pr-target").value=connected.id;
    if(!sources.length) message("Connect a Plex server in Settings to recover its history.");
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
