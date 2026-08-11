/* assets/js/modals/exporter/index.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const fjson=async(u,o)=>{const r=await fetch(u,o);if(!r.ok){let d=null;try{d=await r.json()}catch{}const e=new Error(d?.detail?.code||`${r.status} ${r.statusText}`);e.payload=d;throw e}return r.json()};
const $=(s,r=document)=>r.querySelector(s),$$=(s,r=document)=>Array.from(r.querySelectorAll(s));
const esc=s=>String(s??"").replace(/[&<>"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m]));
const LS={get:(k,d)=>{try{return JSON.parse(localStorage.getItem(k))??d}catch{return d}},set:(k,v)=>{try{localStorage.setItem(k,JSON.stringify(v))}catch{}}};

async function injectExporterStyles(){
  if(document.querySelector('link[data-cw-exporter-styles]')) return;
  await new Promise(resolve=>{
    const link=document.createElement("link");
    link.rel="stylesheet";
    link.href=new URL("./styles.css",import.meta.url).href;
    link.dataset.cwExporterStyles="";
    link.onload=link.onerror=resolve;
    document.head.appendChild(link);
  });
}

const closeModal=()=>window.cxCloseModal?window.cxCloseModal():document.querySelector(".cx-modal-shell")?.dispatchEvent(new CustomEvent("cw-modal-close",{bubbles:true}));
async function downloadFile(u){const r=await fetch(u);if(!r.ok)throw new Error(`Download failed: ${r.status}`);const blob=await r.blob(),cd=r.headers.get("Content-Disposition")||"",m=/filename="([^"]+)"/i.exec(cd),a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=m?.[1]||"export.csv";a.click();setTimeout(()=>URL.revokeObjectURL(a.href),4000)}

function enableColumnResize(table,key){
  try{
    if(!table?.isConnected) return;
    const ths=$$("thead th",table); if(!ths.length) return;
    const cg=table.querySelector("colgroup")||table.insertBefore(document.createElement("colgroup"),table.firstChild);
    while(cg.children.length<ths.length) cg.appendChild(document.createElement("col"));
    while(cg.children.length>ths.length) cg.lastElementChild.remove();
    const cols=[...cg.children], saved=LS.get(key,{}), cv=document.createElement("canvas"), ctx=cv.getContext("2d");
    const tw=(txt,ref)=>{const cs=getComputedStyle(ref);ctx.font=`${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`.replace(/\s{2,}/g," ");return Math.ceil(ctx.measureText(txt||"").width)};
    const setW=(i,w)=>{const th=ths[i], col=cols[i]; if(!th||!col) return; col.style.width=th.style.width=`${w}px`; saved[th.dataset.col||`c${i}`]=Math.round(w)};
    ths.forEach((th,i)=>setW(i,saved[th.dataset.col||`c${i}`]||parseInt(th.style.width,10)||Math.max(80,Math.round(th.getBoundingClientRect().width))));
    const autofit=i=>{const th=ths[i]; if(!th) return; const cells=$$(`tbody tr td:nth-child(${i+1})`,table).slice(0,250); let w=tw(th.innerText.trim(),th)+24; for(const td of cells) w=Math.max(w,tw(td.innerText?.trim?.()||td.textContent||"",td)+24); setW(i,Math.max(80,Math.min(1000,w))); LS.set(key,saved)};
    let drag; const move=e=>drag&&setW(drag.i,Math.max(80,drag.w+e.clientX-drag.x)), up=()=>{if(!drag) return; drag=null; document.body.style.userSelect=""; document.removeEventListener("mousemove",move); document.removeEventListener("mouseup",up); LS.set(key,saved)};
    ths.forEach((th,i)=>{const h=th.querySelector(".resizer")||th.appendChild(Object.assign(document.createElement("div"),{className:"resizer"}));h.onmousedown=e=>{drag={i,x:e.clientX,w:parseInt(cols[i].style.width||th.offsetWidth,10)||120};document.body.style.userSelect="none";document.addEventListener("mousemove",move);document.addEventListener("mouseup",up);e.preventDefault();e.stopPropagation()};h.ondblclick=e=>{e.stopPropagation();autofit(i)}});
  }catch(err){console.warn("Column resize init failed:",err)}
}

const mediaLabel=t=>({movie:"Movies",show:"Shows",season:"Seasons",episode:"Episodes"}[t]||t);
const featureLabel=f=>({history:"History",ratings:"Ratings",watchlist:"Watchlist",combined:"History & Ratings"}[f]||f);
const reasonLabel=r=>({
  already_exists:"Already in tracker",
  duplicate_in_file:"Duplicate in file",
  rating_update:"Update rating",
  missing_identity:"Missing usable ID",
  missing_watched_at:"Missing watched date",
  missing_rating:"Missing rating",
  unsupported_feature:"Unsupported feature",
  unsupported_media_type:"Unsupported type",
  import_source_mismatch:"Wrong export type",
  import_no_rows:"No importable rows",
  import_file_too_large:"File too large",
  import_unsupported_file_type:"Unsupported file type",
  import_zip_too_large:"ZIP too large",
  import_zip_too_many_files:"Too many ZIP files",
  import_zip_encrypted:"Encrypted ZIP not supported",
  import_zip_unsupported_member:"No supported files in ZIP",
  import_parse_failed:"Could not parse export",
  import_target_unavailable:"CrossWatch tracker not connected"
}[r]||String(r||"Ready"));
const helpIcon=(tip,label="Help")=>`<button type="button" class="cx-help ex-help material-symbols-rounded" title="${esc(tip)}" aria-label="${esc(label)}">help</button>`;

const TEMPLATE=`<div class="cw-exporter">
  <div class="cx-head">
    <div class="cx-left"><div class="head-mark material-symbols-rounded">import_export</div><div class="head-copy"><div class="cx-title">Import / Export</div><div class="cx-sub">Bring data into CrossWatch or export it out.</div></div></div>
    <div class="ex-actions"><div class="ex-tabs" role="tablist"><button type="button" class="ex-tab active" data-tab="import">Import</button><button type="button" class="ex-tab" data-tab="export">Export</button></div><button class="close-btn" id="ex-close">Close</button></div>
  </div>
  <div class="ex-progress hidden" id="ex-progress">
    <div class="ex-progress-head"><div><strong id="ex-progress-title">Working</strong><span id="ex-progress-sub">Preparing...</span></div><b id="ex-progress-percent">0%</b></div>
    <div class="ex-progress-bar"><span id="ex-progress-fill"></span></div>
    <div class="ex-progress-grid"><div><span>Stage</span><b id="ex-progress-stage">Starting</b></div><div><span>Elapsed</span><b id="ex-progress-elapsed">0s</b></div><div><span>Rows</span><b id="ex-progress-rows">-</b></div></div>
  </div>
  <div class="im-body">
    <div class="im-setup">
      <div class="im-panel im-panel-main">
        <div class="im-panel-head"><span class="material-symbols-rounded">source_environment</span><b>Import setup</b></div>
        <div class="im-fields">
          <div class="field"><label for="im-source">Source ${helpIcon("Provider or export format to import. Auto detect uses filenames and common columns.", "Source help")}</label><select id="im-source" class="input"><option value="auto">Auto detect</option><option value="trakt">Trakt export ZIP</option><option value="letterboxd">Letterboxd ZIP</option><option value="simkl">Simkl backup JSON</option><option value="imdb">IMDb CSV</option><option value="tvtime">TV Time export</option><option value="yamtrack">Yamtrack CSV</option><option value="generic">Generic CSV/JSON</option></select></div>
          <div class="field"><label for="im-target">Target profile ${helpIcon("Target profile from CrossWatch Tracker", "Target profile help")}</label><select id="im-target" class="input"><option value="default">Default</option></select></div>
        </div>
        <div class="field expect-field"><label>How to export ${helpIcon("Shows where to create the export and which files CrossWatch expects.", "How to export help")}</label><div class="hint import-expect" id="im-expect">Pick a source to see the export steps.</div></div>
      </div>
      <div class="im-panel im-panel-file">
        <div class="im-panel-head"><span class="material-symbols-rounded">upload_file</span><b>Export file</b>${helpIcon("Upload the provider export ZIP, JSON or CSV. Files are previewed before anything is imported.", "Export file help")}</div>
        <input id="im-file" class="file-native" type="file" accept=".zip,.json,.csv,.txt,application/zip,application/json,text/csv">
        <button type="button" class="file-pick-btn" id="im-file-btn"><span class="material-symbols-rounded">upload_file</span><span id="im-file-name">Choose export file</span></button>
        <button class="btn" id="im-upload">Preview file</button>
      </div>
      <div class="im-panel im-panel-options">
        <div class="im-panel-head"><span class="material-symbols-rounded">tune</span><b>Include</b>${helpIcon("Choose which data types and media types are included in the preview and import.", "Include help")}</div>
        <div class="media-picks im-pick-group">
          <label class="media-pick"><input type="checkbox" data-im-feature="history" checked><span>History</span></label>
          <label class="media-pick"><input type="checkbox" data-im-feature="ratings" checked><span>Ratings</span></label>
          <label class="media-pick"><input type="checkbox" data-im-feature="watchlist" checked><span>Watchlist</span></label>
        </div>
        <div class="media-picks im-pick-group">
          <label class="media-pick"><input type="checkbox" data-im-media="movie" checked><span>Movies</span></label>
          <label class="media-pick"><input type="checkbox" data-im-media="show" checked><span>Shows</span></label>
          <label class="media-pick"><input type="checkbox" data-im-media="season" checked><span>Seasons</span></label>
          <label class="media-pick"><input type="checkbox" data-im-media="episode" checked><span>Episodes</span></label>
        </div>
      </div>
    </div>
    <div class="im-summary" id="im-summary"></div>
    <div class="im-preview-tools">
      <div class="field search"><label for="im-q">Search preview ${helpIcon("Filter preview rows by title, year, provider ID, status, or media type.", "Search preview help")}</label><input id="im-q" class="input" type="text" placeholder="Title, year or id..."></div>
      <div class="im-status-tabs" id="im-status-tabs" role="group" aria-label="Preview status filter"><button type="button" class="im-filter active" data-im-status="all" title="Show all preview rows">All</button><button type="button" class="im-filter" data-im-status="ready" title="Rows that are not in the target profile yet">Ready</button><button type="button" class="im-filter" data-im-status="exists" title="Rows the target profile already holds. Turn on Include existing to import them anyway">In tracker</button><button type="button" class="im-filter" data-im-status="duplicate" title="Rows that appear more than once inside the export file">Duplicate</button><button type="button" class="im-filter" data-im-status="missing_identity" title="Rows without a usable provider ID or title/year match">Missing IDs</button><button type="button" class="im-filter" data-im-status="invalid" title="Rows missing required data like watched date or rating">Invalid</button><button type="button" class="im-filter" data-im-status="unsupported" title="Rows CrossWatch cannot import yet">Unsupported</button></div>
      <div class="im-import-actions"><div class="hint count-chip" id="im-count">No preview</div><div class="im-page-actions"><button type="button" class="btn icon-btn im-page-btn" id="im-prev" disabled title="Previous preview page" aria-label="Previous preview page"><span class="material-symbols-rounded">chevron_left</span></button><div class="hint count-chip" id="im-page">Rows 0-0 of 0</div><button type="button" class="btn icon-btn im-page-btn" id="im-next" disabled title="Next preview page" aria-label="Next preview page"><span class="material-symbols-rounded">chevron_right</span></button></div><label class="toggle" title="Also import rows the target profile already holds. Use this to restore or merge a backup"><input id="im-include-existing" type="checkbox"><span class="toggle-track"><span class="toggle-knob"></span></span><span class="toggle-label">Include existing</span></label><label class="toggle" title="Import all importable rows matching the filters"><input id="im-all-ready" type="checkbox" checked><span class="toggle-track"><span class="toggle-knob"></span></span><span class="toggle-label">All ready</span></label><button class="btn primary" id="im-import" disabled title="Import selected rows into the target CrossWatch Tracker profile">Import</button></div>
    </div>
    <div class="ex-grid-wrap"><div class="ex-grid"><table id="im-table"><colgroup></colgroup><thead><tr><th data-col="sel" style="width:34px"></th><th data-col="title" style="width:220px">Title</th><th data-col="feature" style="width:92px">Feature</th><th data-col="type" style="width:88px">Type</th><th data-col="ids">IDs</th><th data-col="status" style="width:160px">Status</th></tr></thead><tbody id="im-tbody"><tr><td colspan="6" class="hint">Choose an export file to preview it here.</td></tr></tbody></table></div></div>
  </div>
  <div class="ex-body hidden">
    <div class="row">
      <div class="field provider-field"><label for="ex-prov-btn">Provider</label><select id="ex-prov" name="ex-prov" class="prov-native" data-cw-icon-select="off" aria-hidden="true" tabindex="-1"></select><div class="prov-dd"><button type="button" id="ex-prov-btn" class="prov-btn" aria-haspopup="listbox" aria-expanded="false"><span class="prov-val" id="ex-prov-val"></span><span class="prov-chev">v</span></button><div id="ex-prov-menu" class="prov-menu" role="listbox"></div></div></div>
      <div class="field"><label for="ex-inst">Instance</label><select id="ex-inst" name="ex-inst" class="input"></select></div>
      <div class="field"><label for="ex-feat">Feature</label><select id="ex-feat" name="ex-feat" class="input"><option value="watchlist">Watchlist</option><option value="history">History</option><option value="ratings">Ratings</option><option value="combined">History &amp; Ratings</option></select></div>
      <div class="field"><label for="ex-fmt">Format</label><select id="ex-fmt" name="ex-fmt" class="input"></select></div>
      <div class="field search"><label for="ex-q">Search</label><input id="ex-q" name="ex-q" class="input" type="text" placeholder="Title, year or id..."></div>
      <div class="action-row"><div class="action-left"><div class="media-field"><label>Media types</label><div class="media-picks"><span id="ex-media"></span><label class="media-pick watched-date-wrap" id="ex-watched-date-wrap" title="Include WatchedDate in Letterboxd exports"><input id="ex-watched-date" type="checkbox" checked><span>WatchedDate</span></label><label class="media-pick rewatch-wrap" id="ex-rewatch-wrap" title="Keep separate watched events when the source supports rewatches"><input id="ex-rewatches" type="checkbox" checked><span>Rewatches</span></label></div></div></div><div class="row-right"><div class="hint count-chip" id="ex-count">-</div><label class="toggle" title="Export all filtered results (live)"><input id="ex-all" type="checkbox" checked><span class="toggle-track"><span class="toggle-knob"></span></span><span class="toggle-label">All filtered</span></label><button class="btn" id="ex-preview">Preview</button><button class="btn primary" id="ex-export">Export</button></div></div>
    </div>
    <div class="ex-grid-wrap"><div class="ex-grid"><table id="ex-table"><colgroup></colgroup><thead><tr><th data-col="sel" style="width:34px"></th><th data-col="title" style="width:220px">Title</th><th data-col="year" style="width:82px">Year</th><th data-col="type" style="width:92px">Type</th><th data-col="ids">IDs</th><th data-col="extra" style="width:142px">Watched / Rating</th></tr></thead><tbody id="ex-tbody"><tr><td colspan="6" class="hint">Loading...</td></tr></tbody></table></div></div>
  </div>
</div><div class="wait-overlay hidden" id="ex-wait"><div class="wait-card" role="status" aria-live="assertive"><div class="wait-ring"></div><div class="wait-text" id="ex-wait-text">Loading...</div></div></div>`;

export default {
  async mount(root){
    await injectExporterStyles();
    const shell=root.closest(".cx-modal-shell");
    shell?.classList.add("cw-exporter-modal");
    root.classList.add("cw-exporter-modal");
    root.style.setProperty("--cxModalMaxW","1200px");
    root.style.setProperty("--cxModalMaxH","84vh");
    root.innerHTML=TEMPLATE;
    root.addEventListener("click",e=>{if(e.target.closest(".ex-help")){e.preventDefault();e.stopPropagation()}},true);
    root.addEventListener("mousedown",e=>{if(e.target.closest(".ex-help")) e.stopPropagation()},true);

    const el=n=>$(n,root), PM=window.CW?.ProviderMeta;
    const wait=el("#ex-wait"), waitText=el("#ex-wait-text");
    const prog={box:el("#ex-progress"),title:el("#ex-progress-title"),sub:el("#ex-progress-sub"),pct:el("#ex-progress-percent"),fill:el("#ex-progress-fill"),stage:el("#ex-progress-stage"),elapsed:el("#ex-progress-elapsed"),rows:el("#ex-progress-rows")};
    let waitTimer, shownAt=0, progressTimer, progressStart=0, progressPct=0, progressOk=true;
    const setWait=t=>waitText.textContent=t;
    const setProgress=(pct,stage,sub,rows="-")=>{progressPct=Math.max(0,Math.min(100,Math.round(pct||0)));prog.pct.textContent=`${progressPct}%`;prog.fill.style.width=`${progressPct}%`;prog.stage.textContent=stage||"Working";prog.sub.textContent=sub||stage||"Working...";prog.rows.textContent=rows};
    const showProgress=(title,stage)=>{clearInterval(progressTimer);progressOk=true;progressStart=performance.now();progressPct=8;prog.title.textContent=title;setProgress(progressPct,stage,stage);prog.box.classList.remove("hidden","done","error");prog.box.classList.add("active");progressTimer=setInterval(()=>{const elapsed=Math.max(0,Math.round((performance.now()-progressStart)/1000));prog.elapsed.textContent=`${elapsed}s`;if(progressPct<88)setProgress(progressPct+Math.max(1,Math.round((88-progressPct)*.08)),prog.stage.textContent,prog.sub.textContent,prog.rows.textContent)},400)};
    const finishProgress=(ok=true,sub="Done")=>{progressOk=ok;clearInterval(progressTimer);prog.box.classList.remove("active");prog.box.classList.toggle("done",ok);prog.box.classList.toggle("error",!ok);setProgress(100,ok?"Done":"Error",sub,prog.rows.textContent);setTimeout(()=>prog.box.classList.add("hidden"),ok?850:1800)};
    const showWait=(t="Loading...")=>{setWait(t);wait.classList.add("hidden");shownAt=performance.now();showProgress(t,t);clearTimeout(waitTimer);waitTimer=setTimeout(()=>{prog.sub.textContent=`${t} (still working...)`},3000)};
    const hideWait=()=>{clearTimeout(waitTimer);const ms=250-(performance.now()-shownAt);setTimeout(()=>{if(progressOk)finishProgress(true)},Math.max(0,ms))};
    const debounce=(fn,ms=250)=>{let t; return (...a)=>{clearTimeout(t); t=setTimeout(()=>fn(...a),ms)}};

    const tabs=$$(".ex-tab",root), imBody=el(".im-body"), exBody=el(".ex-body");
    const setTab=tab=>{tabs.forEach(b=>b.classList.toggle("active",b.dataset.tab===tab));imBody.classList.toggle("hidden",tab!=="import");exBody.classList.toggle("hidden",tab!=="export")};

    const im={
      source:el("#im-source"), target:el("#im-target"), file:el("#im-file"), fileBtn:el("#im-file-btn"), fileName:el("#im-file-name"), q:el("#im-q"),
      count:el("#im-count"), page:el("#im-page"), prev:el("#im-prev"), next:el("#im-next"), summary:el("#im-summary"), tbody:el("#im-tbody"), table:el("#im-table"),
      allReady:el("#im-all-ready"), includeExisting:el("#im-include-existing"), upload:el("#im-upload"), commit:el("#im-import"), expect:el("#im-expect"), expectField:el(".expect-field"),
      state:{opts:null,importId:"",selected:new Set(),mode:"ready",status:"all",file:null,total:0,ready:0,exists:0,summary:null,filteredTotal:0,offset:0,pageSize:200}
    };
    const imFeatures=()=>$$("[data-im-feature]",root).filter(x=>x.checked).map(x=>x.dataset.imFeature);
    const imMedia=()=>$$("[data-im-media]",root).filter(x=>x.checked).map(x=>x.dataset.imMedia);
    const imIncludeExisting=()=>!!im.includeExisting?.checked;
    const imTargetConnected=()=>{const id=im.target.value||"default";const t=(im.state.opts?.targets||[]).find(x=>String(x.id)===String(id));return !t||t.connected!==false};
    const syncTargetState=()=>{const ok=imTargetConnected();im.upload.disabled=!ok;im.fileBtn.classList.toggle("disabled",!ok);if(!ok){im.expectField?.classList.remove("hidden");im.expect.innerHTML=`<b>CrossWatch tracker not connected</b><span>Connect the CrossWatch Tracker under Connections before importing. Nothing can be written until then.</span>`}return ok};
    const imImportable=row=>row.status==="ready"||(imIncludeExisting()&&row.status==="exists");
    const imStatusClass=s=>`im-status status-${String(s||"").replace(/[^a-z_]+/g,"")}`;
    const setImportStatus=status=>{im.state.status=status||"all";$$("[data-im-status]",root).forEach(x=>x.classList.toggle("active",x.dataset.imStatus===im.state.status))};
    const renderImportExpect=()=>{
      if(!imTargetConnected()) return;
      const src=im.source.value, spec=im.state.opts?.source_expectations?.[src], files=spec?.files||[];
      const guide=im.state.opts?.source_guides?.[src];
      if(src==="auto"){
        im.expect.innerHTML=`<b>Auto detect</b><span>Upload a Trakt ZIP, Letterboxd ZIP, Simkl backup JSON, or matching ZIP.</span>`;
        return;
      }
      const steps=(guide?.steps||[]).map((step,i)=>`<li><b>${i+1}</b><span>${esc(step)}</span></li>`).join("");
      const expected=files.length?`<small>Expected: ${esc(files.join(", "))}</small>`:"";
      im.expect.innerHTML=`<b>${esc(guide?.title||spec?.label||"How to export")}</b><ol>${steps}</ol>${expected}`;
    };
    const setImportGuideVisible=visible=>im.expectField?.classList.toggle("hidden",!visible);
    const resetImportPreview=(message="Choose an export file to preview it here.")=>{
      im.state.importId=""; im.state.selected.clear(); im.state.total=0; im.state.ready=0; im.state.exists=0; im.state.filteredTotal=0; im.state.offset=0; im.state.mode="ready"; im.allReady.checked=true;
      im.summary.innerHTML="";
      im.tbody.innerHTML=`<tr><td colspan="6" class="hint">${esc(message)}</td></tr>`;
      setImportGuideVisible(true);
      refreshImportCount();
    };
    const imRowHTML=row=>{
      const ids=Object.entries(row.ids||{}).map(([k,v])=>`<span class="mono">${esc(k)}:${esc(v)}</span>`).join(" ");
      const allowed=imImportable(row);
      const checked=im.state.mode==="ready"?allowed:im.state.selected.has(row.id);
      const disabled=!allowed;
      return `<tr data-row-id="${esc(row.id)}"><td><input type="checkbox" name="import-row" class="glow-check im-row-check" aria-label="Include ${esc(row.title||row.key||"row")}" ${checked?"checked":""} ${disabled?"disabled":""}></td><td class="td-wrap">${esc(row.title||"")}</td><td>${esc(featureLabel(row.feature))}</td><td>${esc(row.media_type||"")}</td><td class="ids">${ids}</td><td><span class="${imStatusClass(row.status)}">${esc(row.status==="ready"?(row.reason?reasonLabel(row.reason):"Ready"):reasonLabel(row.reason))}</span></td></tr>`;
    };
    const sumCard=(n,label,cls,tip)=>`<div class="sum-card${cls?` ${cls}`:""}" title="${esc(tip||"")}"><b>${esc(n)}</b><span>${esc(label)}</span></div>`;
    const renderImportSummary=s=>{
      im.state.summary=s||null;
      if(!s){im.summary.innerHTML="";return}
      const inc=imIncludeExisting(), st=s.by_status||{}, ready=s.by_feature_ready||{}, held=s.by_feature_exists||{};
      const feat=f=>(ready[f]||0)+(inc?(held[f]||0):0);
      const total=(s.ready||0)+(inc?(s.exists||0):0);
      const exists=st.exists||0, dupes=st.duplicate||0, issues=(st.invalid||0)+(st.missing_identity||0)+(st.unsupported||0);
      im.summary.innerHTML=
        sumCard(total,"Will import","primary",`Rows written when you press Import${inc?" (including rows already in the tracker)":""}. The three cards after this one add up to it.`)
        +sumCard(feat("history"),"History","",`History rows included in the ${total} above`)
        +sumCard(feat("ratings"),"Ratings","",`Rating rows included in the ${total} above`)
        +sumCard(feat("watchlist"),"Watchlist","",`Watchlist rows included in the ${total} above`)
        +(exists?sumCard(exists,inc?"In tracker (included)":"In tracker, skipped",inc?"":"muted",inc?"Already in the target profile and being re-written because Include existing is on":"Already in the target profile under the same key. Turn on Include existing to import them anyway"):"")
        +(dupes?sumCard(dupes,"Duplicate in file","muted","The export lists these rows more than once. Never imported."):"")
        +(issues?sumCard(issues,"Needs review","warn","Rows with no usable ID, no watched date, or no rating"):"");
    };
    const importCount=()=>im.state.mode==="ready"?((im.state.ready||0)+(imIncludeExisting()?(im.state.exists||0):0)):im.state.selected.size;
    const syncUploadCta=()=>im.upload.classList.toggle("primary",!!(im.state.file||im.file.files?.[0])&&!im.state.importId);
    const refreshImportCount=()=>{
      const n=importCount(), total=im.state.filteredTotal||0;
      im.count.textContent=im.state.importId?`Importing ${n} of ${im.state.total}`:"No preview";
      if(im.page){
        const start=im.state.importId&&total?im.state.offset+1:0, end=im.state.importId?Math.min(im.state.offset+im.state.pageSize,total):0;
        im.page.textContent=`Rows ${start}-${end} of ${total}`;
      }
      syncUploadCta();
      if(im.prev) im.prev.disabled=!im.state.importId||im.state.offset<=0;
      if(im.next) im.next.disabled=!im.state.importId||im.state.offset+im.state.pageSize>=total;
      im.commit.disabled=!im.state.importId||n<=0;
    };
    const loadImportRows=async()=>{
      if(!im.state.importId) return;
      try{
        const url=`/api/import/preview/${encodeURIComponent(im.state.importId)}?features=${encodeURIComponent(imFeatures().join(","))}&media_types=${encodeURIComponent(imMedia().join(","))}&status=${encodeURIComponent(im.state.status)}&q=${encodeURIComponent(im.q.value||"")}&limit=${encodeURIComponent(im.state.pageSize)}&offset=${encodeURIComponent(im.state.offset)}`;
        const data=await fjson(url);
        im.state.total=data.total||0; im.state.ready=data.summary?.ready||0; im.state.exists=data.summary?.exists||0; im.state.filteredTotal=data.filtered_total||0;
        im.tbody.innerHTML=(data.rows||[]).map(imRowHTML).join("")||`<tr><td colspan="6" class="hint">No rows match the filters.</td></tr>`;
        renderImportSummary(data.summary); refreshImportCount();
      }catch(e){
        im.tbody.innerHTML=`<tr><td colspan="6" class="hint">${esc(e.message||"Preview failed")}</td></tr>`;
        im.state.total=0; im.state.ready=0; im.state.exists=0; im.state.filteredTotal=0; im.state.offset=0; refreshImportCount();
      }
    };
    const uploadImport=async()=>{
      const file=im.state.file||im.file.files?.[0];
      if(!file){im.tbody.innerHTML=`<tr><td colspan="6" class="hint">Choose a ZIP, JSON or CSV file first.</td></tr>`;return}
      showWait("Parsing import...");
      im.upload.disabled=true;
      try{
        const body=new FormData();
        body.append("file",file);
        body.append("source",im.source.value);
        body.append("target_instance",im.target.value||"default");
        const data=await fjson("/api/import/preview",{method:"POST",body});
        im.state.importId=data.import_id||""; im.state.mode="ready"; im.state.selected.clear(); im.state.offset=0; im.allReady.checked=true;
        im.state.total=data.total||0; im.state.ready=data.summary?.ready||0; im.state.exists=data.summary?.exists||0; im.state.filteredTotal=data.filtered_total||0;
        prog.rows.textContent=`${im.state.total}`;
        setImportStatus("ready");
        im.tbody.innerHTML=(data.rows||[]).map(imRowHTML).join("")||`<tr><td colspan="6" class="hint">No ready rows to import.</td></tr>`;
        setImportGuideVisible(false);
        renderImportSummary(data.summary); refreshImportCount();
      }catch(e){
        const code=e?.payload?.detail?.code||e.message||"import_failed";
        im.state.importId=""; im.state.selected.clear(); im.state.total=0; im.state.ready=0; im.state.exists=0; im.state.filteredTotal=0; im.state.offset=0;
        im.summary.innerHTML="";
        im.tbody.innerHTML=`<tr><td colspan="6" class="hint">${esc(reasonLabel(code))}</td></tr>`;
        setImportGuideVisible(true);
        refreshImportCount();
        finishProgress(false,reasonLabel(code));
      }finally{im.upload.disabled=false;hideWait()}
    };
    const commitImport=async()=>{
      if(!im.state.importId) return;
      if(!syncTargetState()){im.tbody.innerHTML=`<tr><td colspan="6" class="hint">Connect the CrossWatch tracker before importing.</td></tr>`;return}
      const rows=importCount();
      im.commit.disabled=true;
      const oldLabel=im.commit.textContent;
      im.commit.textContent="Importing...";
      showWait(`Importing ${rows} row(s)...`);
      prog.rows.textContent=`${rows}`;
      try{
        const payload={import_id:im.state.importId,target_instance:im.target.value||"default",mode:im.state.mode,row_ids:[...im.state.selected],features:imFeatures(),media_types:imMedia(),include_existing:imIncludeExisting()};
        const data=await fjson("/api/import/commit",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
        prog.rows.textContent=`${data.applied||0}`;
        im.tbody.innerHTML=`<tr><td colspan="6" class="hint">Imported ${esc(data.applied||0)} item(s) into CrossWatch.</td></tr>`;
        im.summary.innerHTML=`<div class="sum-card primary"><b>${esc(data.applied||0)}</b><span>Imported</span></div><div class="sum-card"><b>${esc(data.skipped||0)}</b><span>Skipped</span></div>`;
        im.state.importId=""; im.state.selected.clear(); im.state.total=0; im.state.ready=0; im.state.exists=0; im.state.filteredTotal=0; im.state.offset=0;
        im.state.file=null; im.file.value=""; setImportFileName();
        refreshImportCount();
        setImportGuideVisible(false);
      }catch(e){
        im.tbody.innerHTML=`<tr><td colspan="6" class="hint">${esc(reasonLabel(e?.payload?.detail?.code||e.message||"Import failed"))}</td></tr>`;
        refreshImportCount();
        finishProgress(false,reasonLabel(e?.payload?.detail?.code||e.message||"Import failed"));
      }finally{im.commit.textContent=oldLabel;hideWait()}
    };

    const ex={
      count:el("#ex-count"), provSel:el("#ex-prov"), provBtn:el("#ex-prov-btn"), provVal:el("#ex-prov-val"), provMenu:el("#ex-prov-menu"), instSel:el("#ex-inst"),
      featSel:el("#ex-feat"), fmtSel:el("#ex-fmt"), mediaWrap:el("#ex-media"), watchedDateWrap:el("#ex-watched-date-wrap"), watchedDateChk:el("#ex-watched-date"), rewatchWrap:el("#ex-rewatch-wrap"), rewatchChk:el("#ex-rewatches"),
      qInput:el("#ex-q"), allChk:el("#ex-all"), btnPrev:el("#ex-preview"), btnExp:el("#ex-export"), tbody:el("#ex-tbody"), table:el("#ex-table"),
      state:{opts:null,total:0,matchedTotal:0,droppedTotal:0,lastQuery:"",selected:new Set(),mode:"all"}
    };
    const PREFS_KEY="cw.exporter.prefs", prefs=LS.get(PREFS_KEY,{});
    const selectedMediaTypes=()=>$$('input[type="checkbox"][data-media]',ex.mediaWrap).filter(x=>x.checked&&!x.disabled).map(x=>x.dataset.media);
    const savePrefs=()=>LS.set(PREFS_KEY,{provider:ex.provSel.value,instance:ex.instSel.value,feature:ex.featSel.value,format:ex.fmtSel.value,media_types:selectedMediaTypes(),include_watched_date:ex.watchedDateChk.checked,include_rewatches:ex.rewatchChk.checked,q:ex.qInput.value,all:ex.allChk.checked});
    const logoHtml=(p,cls="badge-logo")=>{const src=PM?.logLogoPath?.(p)||PM?.logoPath?.(p)||"", label=PM?.label?.(p)||String(p||""); return src?`<img class="${cls}" src="${src}" alt="${esc(label)}">`:`<span class="prov-fallback">${esc(label.slice(0,2).toUpperCase())}</span>`};
    const provText=p=>esc(PM?.label?.(p)||p);
    const provOption=p=>`<button type="button" class="prov-opt${ex.provSel.value===p?" active":""}" data-provider="${esc(p)}" role="option" aria-selected="${ex.provSel.value===p}">${logoHtml(p,"prov-logo")}<span>${provText(p)}</span></button>`;
    const renderProv=()=>{ex.provVal.innerHTML=`${logoHtml(ex.provSel.value,"prov-logo")}<span>${provText(ex.provSel.value)}</span>`; ex.provMenu.innerHTML=[...ex.provSel.options].map(o=>provOption(o.value)).join("")};
    const closeProv=()=>{ex.provMenu.classList.remove("open"); ex.provBtn.setAttribute("aria-expanded","false")};
    const openProv=()=>{ex.provMenu.classList.add("open"); ex.provBtn.setAttribute("aria-expanded","true")};
    const refreshCounts=()=>{const sel=ex.state.mode==="all"?ex.state.total:ex.state.selected.size; ex.count.textContent=`Selected: ${sel} of ${ex.state.total}`};
    const rowHTML=it=>`<tr data-key="${esc(it.key)}"><td><input type="checkbox" name="export-row" class="glow-check row-check" aria-label="Select ${esc(it.title||it.key||"row")}" ${ex.state.mode==="all"||ex.state.selected.has(it.key)?"checked":""}></td><td class="td-wrap">${esc(it.title||"")}</td><td>${esc(it.year||"")}</td><td>${esc(it.type||"")}</td><td class="ids">${Object.entries(it.ids||{}).map(([k,v])=>`<span class="mono">${esc(k)}:${esc(v)}</span>`).join(" ")}</td><td>${esc(it.rating||it.watched_at||"")}</td></tr>`;
    const setMediaTypes=types=>{$$('input[type="checkbox"][data-media]',ex.mediaWrap).forEach(cb=>{cb.checked=(types||[]).includes(cb.dataset.media)})};
    const syncInstances=()=>{const prov=ex.provSel.value,list=ex.state.opts?.instances?.[prov]||[{id:"default",label:"Default"}];ex.instSel.innerHTML=[`<option value="all">All</option>`,...list.map(x=>`<option value="${esc(x.id)}">${esc(x.label||x.id)}</option>`)].join("");const want=prefs.instance;if(want&&(want==="all"||list.some(x=>x.id===want))) ex.instSel.value=want; if(!ex.instSel.value) ex.instSel.value="all"; renderProv()};
    const syncFormats=()=>{const list=ex.state.opts?.formats?.[ex.featSel.value]||[], labels=ex.state.opts?.labels||{};const prev=ex.fmtSel.value;ex.fmtSel.innerHTML=list.map(x=>`<option value="${esc(x)}">${esc(labels[x]||x.toUpperCase())}</option>`).join(""); if(list.includes(prev)) ex.fmtSel.value=prev; else if(prefs.format&&list.includes(prefs.format)) ex.fmtSel.value=prefs.format};
    const syncWatchedDateOption=()=>{ex.watchedDateWrap.hidden=!(ex.fmtSel.value==="letterboxd"&&["history","combined"].includes(ex.featSel.value))};
    const syncCapabilities=()=>{const allowed=new Set(ex.state.opts?.capabilities?.[ex.fmtSel.value]?.media_types||ex.state.opts?.media_types||[]), fmtLabel=ex.state.opts?.labels?.[ex.fmtSel.value]||ex.fmtSel.value||"Selected format";$$('input[type="checkbox"][data-media]',ex.mediaWrap).forEach(cb=>{cb.disabled=!!allowed.size&&!allowed.has(cb.dataset.media);const pill=cb.closest(".media-pick");pill?.classList.toggle("disabled",cb.disabled);if(pill) pill.title=cb.disabled?`${fmtLabel} supports ${[...allowed].map(mediaLabel).join(", ")} only.`:"";if(cb.disabled) cb.checked=false});if(!selectedMediaTypes().length){const fallback=(ex.state.opts?.default_media_types||["movie"]).find(t=>!$$('input[type="checkbox"][data-media]',ex.mediaWrap).find(cb=>cb.dataset.media===t)?.disabled);if(fallback) setMediaTypes([fallback])}};
    const sourceRewatches=()=>!!ex.state.opts?.rewatches?.[ex.provSel.value];
    const syncRewatchOption=()=>{const show=sourceRewatches()&&["history","combined"].includes(ex.featSel.value);ex.rewatchWrap.hidden=!show;if(!show)ex.rewatchChk.checked=false;else if(prefs.include_rewatches!==false)ex.rewatchChk.checked=true};
    async function renderPreview(auto=false){if(!ex.state.opts?.providers?.length){ex.tbody.innerHTML=`<tr><td colspan="6" class="hint">No state loaded. Nothing to show.</td></tr>`;ex.state.total=0;ex.state.selected.clear();ex.btnExp.disabled=true;return refreshCounts()}ex.tbody.innerHTML=`<tr><td colspan="6" class="hint">Loading...</td></tr>`;try{ex.state.lastQuery=ex.qInput.value||"";const media=selectedMediaTypes().join(","),data=await fjson(`/api/export/sample?provider=${encodeURIComponent(ex.provSel.value)}&provider_instance=${encodeURIComponent(ex.instSel.value)}&feature=${encodeURIComponent(ex.featSel.value)}&format=${encodeURIComponent(ex.fmtSel.value)}&media_types=${encodeURIComponent(media)}&include_watched_date=${encodeURIComponent(ex.watchedDateChk.checked)}&include_rewatches=${encodeURIComponent(ex.rewatchChk.checked)}&limit=50&q=${encodeURIComponent(ex.state.lastQuery)}`);ex.state.total=data.total||0;ex.state.matchedTotal=data.matched_total||0;ex.state.droppedTotal=data.dropped_total||0;if(ex.state.mode==="all") ex.state.selected.clear();ex.tbody.innerHTML=(data.items||[]).map(rowHTML).join("")||`<tr><td colspan="6" class="hint">No items.</td></tr>`;ex.btnExp.disabled=!ex.state.total&&!ex.state.selected.size;refreshCounts()}catch{ex.tbody.innerHTML=`<tr><td colspan="6" class="hint">No data.</td></tr>`;ex.state.total=0;ex.state.selected.clear();ex.btnExp.disabled=true;refreshCounts()}}
    async function doExport(){const label=ex.btnExp.textContent;ex.btnExp.disabled=true;ex.btnExp.textContent="Preparing...";try{const ids=ex.state.mode==="manual"&&ex.state.selected.size?`&ids=${encodeURIComponent([...ex.state.selected].join(","))}`:"";await downloadFile(`/api/export/file?provider=${encodeURIComponent(ex.provSel.value)}&provider_instance=${encodeURIComponent(ex.instSel.value)}&feature=${encodeURIComponent(ex.featSel.value)}&format=${encodeURIComponent(ex.fmtSel.value)}&media_types=${encodeURIComponent(selectedMediaTypes().join(","))}&include_watched_date=${encodeURIComponent(ex.watchedDateChk.checked)}&include_rewatches=${encodeURIComponent(ex.rewatchChk.checked)}&q=${encodeURIComponent(ex.state.lastQuery)}${ids}`)}catch(e){window.CW?.DOM?.showToast?.(`Export failed: ${e.message||e}`,false)}finally{ex.btnExp.disabled=false;ex.btnExp.textContent=label}}
    let exportPreviewLoaded=false;
    tabs.forEach(btn=>btn.addEventListener("click",()=>{const tab=btn.dataset.tab;setTab(tab);if(tab==="export"&&!exportPreviewLoaded){exportPreviewLoaded=true;renderPreview(false)}}));

    try{
      const [exOpts, imOpts]=await Promise.all([
        fjson("/api/export/options").catch(()=>({providers:[],counts:{},formats:{watchlist:["letterboxd","imdb","justwatch","yamtrack","tmdb"],history:["letterboxd","justwatch","yamtrack"],ratings:["letterboxd","tmdb"],combined:["letterboxd","yamtrack"]},labels:{letterboxd:"Letterboxd",imdb:"IMDb (list)",justwatch:"JustWatch",yamtrack:"Yamtrack",tmdb:"TMDB (Auto: IMDb/Trakt/SIMKL)"},capabilities:{letterboxd:{media_types:["movie"]},imdb:{media_types:["movie","show","season","episode"]},justwatch:{media_types:["movie","show","season","episode"]},yamtrack:{media_types:["movie","show","season","episode"]},tmdb:{media_types:["movie","show","season","episode"]}},media_types:["movie","show","season","episode"],default_media_types:["movie"]})),
        fjson("/api/import/options").catch(()=>({targets:[{id:"default",label:"Default"}]}))
      ]);
      ex.state.opts=exOpts; im.state.opts=imOpts;
      if(imOpts.sources?.length) im.source.innerHTML=imOpts.sources.map(x=>`<option value="${esc(x.id)}">${esc(x.label||x.id)}</option>`).join("");
      im.target.innerHTML=(imOpts.targets||[{id:"default",label:"Default"}]).map(x=>`<option value="${esc(x.id)}">${esc(x.label||x.id)}${x.connected===false?" (not connected)":""}</option>`).join("");
      syncTargetState();
      renderImportExpect();
      if(ex.state.opts.providers?.length){ex.provSel.innerHTML=ex.state.opts.providers.map(p=>`<option value="${esc(p)}">${esc(PM?.label?.(p)||p)}</option>`).join("")}else{ex.provSel.innerHTML='<option value="" disabled>(no providers)</option>';ex.provSel.disabled=ex.instSel.disabled=true;ex.instSel.innerHTML='<option value="all">All</option>'}
      ex.mediaWrap.innerHTML=(ex.state.opts.media_types||["movie","show","season","episode"]).map(t=>`<label class="media-pick"><input type="checkbox" data-media="${esc(t)}"><span>${esc(mediaLabel(t))}</span></label>`).join("");
      if(ex.state.opts.providers?.includes(prefs.provider)) ex.provSel.value=prefs.provider;
      if(["watchlist","history","ratings","combined"].includes(prefs.feature)) ex.featSel.value=prefs.feature;
      ex.qInput.value=prefs.q||"";ex.allChk.checked=prefs.all!==false;ex.watchedDateChk.checked=prefs.include_watched_date!==false;ex.rewatchChk.checked=prefs.include_rewatches!==false;syncInstances();syncFormats();setMediaTypes(prefs.media_types||ex.state.opts.default_media_types||["movie"]);syncCapabilities();syncWatchedDateOption();syncRewatchOption();enableColumnResize(ex.table,"cw.exporter.cols.v2");enableColumnResize(im.table,"cw.importer.cols.v1");
    }catch(e){
      console.warn("Import/export options failed:",e);
    }

    const autoRefresh=debounce(()=>renderPreview(true),200), reset=cb=>()=>{ex.state.selected.clear();ex.state.mode="all";ex.allChk.checked=true;cb?.();savePrefs();autoRefresh()};
    ex.provBtn.addEventListener("click",e=>{e.stopPropagation();ex.provMenu.classList.contains("open")?closeProv():openProv()});
    ex.provMenu.addEventListener("click",e=>{const btn=e.target.closest(".prov-opt");if(!btn) return;ex.provSel.value=btn.dataset.provider;closeProv();reset(()=>{syncInstances();syncRewatchOption()})()});
    document.addEventListener("click",e=>{if(!e.target.closest(".provider-field")) closeProv()});
    ex.instSel.addEventListener("change",reset());
    ex.featSel.addEventListener("change",reset(()=>{syncFormats();syncCapabilities();syncWatchedDateOption();syncRewatchOption()}));
    ex.fmtSel.addEventListener("change",()=>{syncCapabilities();syncWatchedDateOption();savePrefs();autoRefresh()});
    ex.watchedDateChk.addEventListener("change",()=>{savePrefs();autoRefresh()});
    ex.rewatchChk.addEventListener("change",()=>{savePrefs();autoRefresh()});
    ex.mediaWrap.addEventListener("change",e=>{if(!e.target.closest('input[type="checkbox"][data-media]')) return;if(!selectedMediaTypes().length){e.target.checked=true}savePrefs();autoRefresh()});
    ex.qInput.addEventListener("input",()=>{savePrefs();autoRefresh()});
    ex.allChk.addEventListener("change",()=>{ex.state.mode=ex.allChk.checked?"all":"manual";if(ex.state.mode==="all") ex.state.selected.clear();savePrefs();autoRefresh()});
    ex.btnPrev.addEventListener("click",()=>renderPreview(false));
    ex.btnExp.addEventListener("click",doExport);
    ex.tbody.addEventListener("change",e=>{const cb=e.target.closest(".row-check");if(!cb) return;const key=cb.closest("tr")?.dataset.key;if(!key) return;if(ex.state.mode==="all"){ex.state.mode="manual";ex.allChk.checked=false}cb.checked?ex.state.selected.add(key):ex.state.selected.delete(key);refreshCounts()});
    ex.tbody.addEventListener("click",e=>{const tr=e.target.closest("tr[data-key]");if(!tr||e.target.closest("input,button,select,.resizer")) return;const cb=$(".row-check",tr);if(cb){cb.checked=!cb.checked;cb.dispatchEvent(new Event("change",{bubbles:true}))}});

    const loadImportFirstPage=()=>{im.state.offset=0;loadImportRows()};
    const refreshImport=debounce(loadImportFirstPage,200);
    im.source.addEventListener("change",()=>{renderImportExpect();resetImportPreview()});
    const setImportFileName=()=>{const name=(im.state.file||im.file.files?.[0])?.name||"Choose export file";im.fileName.textContent=name;im.fileBtn.title=name;syncUploadCta()};
    im.fileBtn.addEventListener("click",()=>im.file.click());
    im.file.addEventListener("change",()=>{im.state.file=im.file.files?.[0]||null;setImportFileName();resetImportPreview()});
    ["dragenter","dragover"].forEach(type=>im.fileBtn.addEventListener(type,e=>{e.preventDefault();im.fileBtn.classList.add("dragging")}));
    ["dragleave","drop"].forEach(type=>im.fileBtn.addEventListener(type,e=>{e.preventDefault();im.fileBtn.classList.remove("dragging")}));
    im.fileBtn.addEventListener("drop",e=>{const file=e.dataTransfer?.files?.[0];if(file){im.state.file=file;setImportFileName();resetImportPreview()}});
    im.upload.addEventListener("click",uploadImport);
    im.commit.addEventListener("click",commitImport);
    im.q.addEventListener("input",refreshImport);
    im.prev.addEventListener("click",()=>{im.state.offset=Math.max(0,im.state.offset-im.state.pageSize);loadImportRows()});
    im.next.addEventListener("click",()=>{if(im.state.offset+im.state.pageSize<im.state.filteredTotal){im.state.offset+=im.state.pageSize;loadImportRows()}});
    el("#im-status-tabs").addEventListener("click",e=>{const btn=e.target.closest("[data-im-status]");if(!btn) return;setImportStatus(btn.dataset.imStatus||"all");loadImportFirstPage()});
    im.target.addEventListener("change",()=>{if(!syncTargetState()) return;if(im.state.file||im.file.files?.[0]) uploadImport()});
    $$("[data-im-feature],[data-im-media]",root).forEach(cb=>cb.addEventListener("change",()=>{if(!imFeatures().length) cb.checked=true;if(!imMedia().length) cb.checked=true;loadImportFirstPage()}));
    im.allReady.addEventListener("change",()=>{im.state.mode=im.allReady.checked?"ready":"selected";if(im.state.mode==="ready") im.state.selected.clear();refreshImportCount();loadImportRows()});
    im.includeExisting.addEventListener("change",()=>{renderImportSummary(im.state.summary);refreshImportCount();loadImportRows()});
    im.tbody.addEventListener("change",e=>{const cb=e.target.closest(".im-row-check");if(!cb) return;const id=cb.closest("tr")?.dataset.rowId;if(!id) return;if(im.state.mode==="ready"){im.state.mode="selected";im.allReady.checked=false}cb.checked?im.state.selected.add(id):im.state.selected.delete(id);refreshImportCount()});
    im.tbody.addEventListener("click",e=>{const tr=e.target.closest("tr[data-row-id]");if(!tr||e.target.closest("input,button,select,.resizer")) return;const cb=$(".im-row-check",tr);if(cb&&!cb.disabled){cb.checked=!cb.checked;cb.dispatchEvent(new Event("change",{bubbles:true}))}});
    el("#ex-close").addEventListener("click",closeModal);
  },
  unmount(){}
};
