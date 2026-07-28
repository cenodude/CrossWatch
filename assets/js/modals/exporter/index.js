/* assets/js/modals/exporter/index.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const fjson=async(u,o)=>{const r=await fetch(u,o);if(!r.ok)throw new Error(`${r.status} ${r.statusText}`);return r.json()};
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

function enableColumnResize(table,key="cw.exporter.cols.v2"){
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

export default {
  async mount(root){
    await injectExporterStyles();
    const shell=root.closest(".cx-modal-shell");
    shell?.classList.add("cw-exporter-modal");
    root.classList.add("cw-exporter-modal");
    root.style.setProperty("--cxModalMaxW","1200px");
    root.style.setProperty("--cxModalMaxH","84vh");
    root.innerHTML=`<div class="cw-exporter"><div class="cx-head"><div class="cx-left"><div class="head-mark">⇩</div><div class="head-copy"><div class="cx-title">Exporter</div><div class="cx-sub">Filter, preview and export source data.</div></div></div><div class="ex-actions"><button class="close-btn" id="ex-close">Close</button></div></div><div class="ex-intro"><div class="ex-intro-copy"><div class="ex-intro-title">Build a scoped export before downloading</div><div class="ex-intro-sub" id="ex-summary-copy">Choose a provider, narrow the preview, then export exactly the rows you want.</div><div class="ex-warnings" id="ex-warnings"></div></div><div class="ex-intro-meta" id="ex-summary-meta"><span class="mini">Mode all</span><span class="mini">Selected 0</span><span class="mini">Total 0</span></div></div><div class="ex-body"><div class="row"><div class="field provider-field"><label for="ex-prov-btn">Provider</label><select id="ex-prov" name="ex-prov" class="prov-native" data-cw-icon-select="off" aria-hidden="true" tabindex="-1"></select><div class="prov-dd"><button type="button" id="ex-prov-btn" class="prov-btn" aria-haspopup="listbox" aria-expanded="false"><span class="prov-val" id="ex-prov-val"></span><span class="prov-chev">▾</span></button><div id="ex-prov-menu" class="prov-menu" role="listbox"></div></div></div><div class="field"><label for="ex-inst">Instance</label><select id="ex-inst" name="ex-inst" class="input"></select></div><div class="field"><label for="ex-feat">Feature</label><select id="ex-feat" name="ex-feat" class="input"><option value="watchlist">Watchlist</option><option value="history">History</option><option value="ratings">Ratings</option><option value="combined">History &amp; Ratings</option></select></div><div class="field"><label for="ex-fmt">Format</label><select id="ex-fmt" name="ex-fmt" class="input"></select></div><div class="field search"><label for="ex-q">Search</label><input id="ex-q" name="ex-q" class="input" type="text" placeholder="Title, year or id..."></div><div class="action-row"><div class="action-left"><div class="media-field"><label>Media types</label><div class="media-picks"><span id="ex-media"></span><label class="media-pick watched-date-wrap" id="ex-watched-date-wrap" title="Include WatchedDate in Letterboxd exports"><input id="ex-watched-date" type="checkbox" checked><span>WatchedDate</span></label></div></div></div><div class="row-right"><div class="hint count-chip" id="ex-count">-</div><label class="toggle" title="Export all filtered results (live)"><input id="ex-all" type="checkbox" checked><span class="toggle-track"><span class="toggle-knob"></span></span><span class="toggle-label">All filtered</span></label><button class="btn" id="ex-preview">Preview</button><button class="btn primary" id="ex-export">Export</button></div></div></div><div class="ex-grid-wrap"><div class="ex-grid"><table id="ex-table"><colgroup></colgroup><thead><tr><th data-col="sel" style="width:34px"></th><th data-col="title" style="width:220px">Title</th><th data-col="year" style="width:82px">Year</th><th data-col="type" style="width:92px">Type</th><th data-col="ids">IDs</th><th data-col="extra" style="width:142px">Watched / Rating</th></tr></thead><tbody id="ex-tbody"><tr><td colspan="6" class="hint">Loading...</td></tr></tbody></table></div></div></div></div><div class="wait-overlay hidden" id="ex-wait"><div class="wait-card" role="status" aria-live="assertive"><div class="wait-ring"></div><div class="wait-text" id="ex-wait-text">Loading...</div></div></div>`;

    const el=n=>$(n,root), PM=window.CW?.ProviderMeta, countEl=el("#ex-count"), summaryCopy=el("#ex-summary-copy"), summaryMeta=el("#ex-summary-meta"), warnEl=el("#ex-warnings"), provSel=el("#ex-prov"), provBtn=el("#ex-prov-btn"), provVal=el("#ex-prov-val"), provMenu=el("#ex-prov-menu"), instSel=el("#ex-inst"), featSel=el("#ex-feat"), fmtSel=el("#ex-fmt"), mediaWrap=el("#ex-media"), watchedDateWrap=el("#ex-watched-date-wrap"), watchedDateChk=el("#ex-watched-date"), qInput=el("#ex-q"), allChk=el("#ex-all"), btnPrev=el("#ex-preview"), btnExp=el("#ex-export"), tbody=el("#ex-tbody"), table=el("#ex-table"), wait=el("#ex-wait"), waitText=el("#ex-wait-text");
    const state={opts:null,total:0,matchedTotal:0,droppedTotal:0,lastQuery:"",selected:new Set(),mode:"all"};
    const PREFS_KEY="cw.exporter.prefs", prefs=LS.get(PREFS_KEY,{}), savePrefs=()=>LS.set(PREFS_KEY,{provider:provSel.value,instance:instSel.value,feature:featSel.value,format:fmtSel.value,media_types:selectedMediaTypes(),include_watched_date:watchedDateChk.checked,q:qInput.value,all:allChk.checked});
    let waitTimer, shownAt=0;
    const logoHtml=(p,cls="badge-logo")=>{const src=PM?.logLogoPath?.(p)||PM?.logoPath?.(p)||"", label=PM?.label?.(p)||String(p||""); return src?`<img class="${cls}" src="${src}" alt="${esc(label)}">`:`<span class="prov-fallback">${esc(label.slice(0,2).toUpperCase())}</span>`};
    const provText=p=>esc(PM?.label?.(p)||p);
    const provOption=p=>`<button type="button" class="prov-opt${provSel.value===p?" active":""}" data-provider="${esc(p)}" role="option" aria-selected="${provSel.value===p}">${logoHtml(p,"prov-logo")}<span>${provText(p)}</span></button>`;
    const renderProv=()=>{provVal.innerHTML=`${logoHtml(provSel.value,"prov-logo")}<span>${provText(provSel.value)}</span>`; provMenu.innerHTML=[...provSel.options].map(o=>provOption(o.value)).join("")};
    const closeProv=()=>{provMenu.classList.remove("open"); provBtn.setAttribute("aria-expanded","false")};
    const openProv=()=>{provMenu.classList.add("open"); provBtn.setAttribute("aria-expanded","true")};
    const setWait=t=>waitText.textContent=t, showWait=(t="Loading...")=>{setWait(t);wait.classList.remove("hidden");shownAt=performance.now();clearTimeout(waitTimer);waitTimer=setTimeout(()=>setWait(`${t} (still working...)`),3000)}, hideWait=()=>{clearTimeout(waitTimer);const ms=250-(performance.now()-shownAt);setTimeout(()=>wait.classList.add("hidden"),Math.max(0,ms))};
    const refreshCounts=()=>{const sel=state.mode==="all"?state.total:state.selected.size; countEl.textContent=`Selected: ${sel} of ${state.total}`; const dropped=state.droppedTotal?` ${state.droppedTotal} row(s) skipped for this target.`:""; summaryCopy.textContent=(state.mode==="all"?"Export is currently scoped to all filtered rows in the preview.":"Export is currently scoped to the manually selected preview rows.")+dropped; summaryMeta.innerHTML=`<span class="mini">Mode ${state.mode}</span><span class="mini">Selected ${sel}</span><span class="mini">Matched ${state.matchedTotal||state.total}</span><span class="mini">Exportable ${state.total}</span>`};
    const rowHTML=it=>`<tr data-key="${esc(it.key)}"><td><input type="checkbox" name="export-row" class="glow-check row-check" aria-label="Select ${esc(it.title||it.key||"row")}" ${state.mode==="all"||state.selected.has(it.key)?"checked":""}></td><td class="td-wrap">${esc(it.title||"")}</td><td>${esc(it.year||"")}</td><td>${esc(it.type||"")}</td><td class="ids">${Object.entries(it.ids||{}).map(([k,v])=>`<span class="mono">${esc(k)}:${esc(v)}</span>`).join(" ")}</td><td>${esc(it.rating||it.watched_at||"")}</td></tr>`;
    const selectedMediaTypes=()=>$$('input[type="checkbox"][data-media]',mediaWrap).filter(x=>x.checked&&!x.disabled).map(x=>x.dataset.media);
    const mediaLabel=t=>({movie:"Movies",show:"Shows",season:"Seasons",episode:"Episodes"}[t]||t);
    const setMediaTypes=types=>{$$('input[type="checkbox"][data-media]',mediaWrap).forEach(cb=>{cb.checked=(types||[]).includes(cb.dataset.media)})};
    const renderWarnings=warnings=>{warnEl.innerHTML=(warnings||[]).map(w=>`<span class="warn">${esc(w)}</span>`).join("")};
    const syncInstances=()=>{const prov=provSel.value,list=state.opts?.instances?.[prov]||[{id:"default",label:"Default"}];instSel.innerHTML=[`<option value="all">All</option>`,...list.map(x=>`<option value="${esc(x.id)}">${esc(x.label||x.id)}</option>`)].join("");const want=prefs.instance;if(want&&(want==="all"||list.some(x=>x.id===want))) instSel.value=want; if(!instSel.value) instSel.value="all"; renderProv()};
    const syncFormats=()=>{const list=state.opts?.formats?.[featSel.value]||[], labels=state.opts?.labels||{};const prev=fmtSel.value;fmtSel.innerHTML=list.map(x=>`<option value="${esc(x)}">${esc(labels[x]||x.toUpperCase())}</option>`).join(""); if(list.includes(prev)) fmtSel.value=prev; else if(prefs.format&&list.includes(prefs.format)) fmtSel.value=prefs.format};
    const syncWatchedDateOption=()=>{
      watchedDateWrap.hidden=!(fmtSel.value==="letterboxd"&&["history","combined"].includes(featSel.value));
    };
    const syncCapabilities=()=>{
      const allowed=new Set(state.opts?.capabilities?.[fmtSel.value]?.media_types||state.opts?.media_types||[]);
      const fmtLabel=state.opts?.labels?.[fmtSel.value]||fmtSel.value||"Selected format";
      $$('input[type="checkbox"][data-media]',mediaWrap).forEach(cb=>{
        cb.disabled=!!allowed.size&&!allowed.has(cb.dataset.media);
        const pill=cb.closest(".media-pick");
        pill?.classList.toggle("disabled",cb.disabled);
        if(pill) pill.title=cb.disabled?`${fmtLabel} supports ${[...allowed].map(mediaLabel).join(", ")} only.`:"";
        if(cb.disabled) cb.checked=false;
      });
      if(!selectedMediaTypes().length){
        const fallback=(state.opts?.default_media_types||["movie"]).find(t=>!$$('input[type="checkbox"][data-media]',mediaWrap).find(cb=>cb.dataset.media===t)?.disabled);
        if(fallback) setMediaTypes([fallback]);
      }
    };

    async function renderPreview(auto=false){
      if(!state.opts?.providers?.length){tbody.innerHTML=`<tr><td colspan="6" class="hint">No state loaded. Nothing to show.</td></tr>`; state.total=0; state.selected.clear(); btnExp.disabled=true; return refreshCounts()}
      tbody.innerHTML=`<tr><td colspan="6" class="hint">Loading...</td></tr>`; showWait(auto?"Refreshing...":"Generating preview...");
      try{
        state.lastQuery=qInput.value||"";
        const media=selectedMediaTypes().join(",");
        const data=await fjson(`/api/export/sample?provider=${encodeURIComponent(provSel.value)}&provider_instance=${encodeURIComponent(instSel.value)}&feature=${encodeURIComponent(featSel.value)}&format=${encodeURIComponent(fmtSel.value)}&media_types=${encodeURIComponent(media)}&include_watched_date=${encodeURIComponent(watchedDateChk.checked)}&limit=50&q=${encodeURIComponent(state.lastQuery)}`);
        state.total=data.total||0; state.matchedTotal=data.matched_total||0; state.droppedTotal=data.dropped_total||0; if(state.mode==="all") state.selected.clear();
        tbody.innerHTML=(data.items||[]).map(rowHTML).join("")||`<tr><td colspan="6" class="hint">No items.</td></tr>`;
        renderWarnings(data.warnings||[]); btnExp.disabled=!state.total&&!state.selected.size; refreshCounts();
      }catch{
        tbody.innerHTML=`<tr><td colspan="6" class="hint">No data.</td></tr>`; state.total=0; state.matchedTotal=0; state.droppedTotal=0; state.selected.clear(); renderWarnings([]); btnExp.disabled=true; refreshCounts();
      }finally{hideWait()}
    }

    async function doExport(){
      const label=btnExp.textContent; btnExp.disabled=true; btnExp.textContent="Preparing..."; showWait("Preparing file...");
      try{
        const ids=state.mode==="manual"&&state.selected.size?`&ids=${encodeURIComponent([...state.selected].join(","))}`:"";
        await downloadFile(`/api/export/file?provider=${encodeURIComponent(provSel.value)}&provider_instance=${encodeURIComponent(instSel.value)}&feature=${encodeURIComponent(featSel.value)}&format=${encodeURIComponent(fmtSel.value)}&media_types=${encodeURIComponent(selectedMediaTypes().join(","))}&include_watched_date=${encodeURIComponent(watchedDateChk.checked)}&q=${encodeURIComponent(state.lastQuery)}${ids}`);
      }finally{btnExp.disabled=false; btnExp.textContent=label; hideWait()}
    }

    showWait("Loading options...");
    try{
      state.opts=await fjson("/api/export/options").catch(()=>({providers:[],counts:{},formats:{watchlist:["letterboxd","imdb","justwatch","yamtrack","tmdb"],history:["letterboxd","justwatch","yamtrack"],ratings:["letterboxd","tmdb"],combined:["letterboxd","yamtrack"]},labels:{letterboxd:"Letterboxd",imdb:"IMDb (list)",justwatch:"JustWatch",yamtrack:"Yamtrack",tmdb:"TMDB (Auto: IMDb/Trakt/SIMKL)"},capabilities:{letterboxd:{media_types:["movie"]},imdb:{media_types:["movie","show","season","episode"]},justwatch:{media_types:["movie","show","season","episode"]},yamtrack:{media_types:["movie","show","season","episode"]},tmdb:{media_types:["movie","show","season","episode"]}},media_types:["movie","show","season","episode"],default_media_types:["movie"] }));
      if(state.opts.providers?.length){provSel.innerHTML=state.opts.providers.map(p=>`<option value="${esc(p)}">${esc(PM?.label?.(p)||p)}</option>`).join("")}else{provSel.innerHTML='<option value="" disabled>(no providers)</option>'; provSel.disabled=instSel.disabled=true; instSel.innerHTML='<option value="all">All</option>'}
      mediaWrap.innerHTML=(state.opts.media_types||["movie","show","season","episode"]).map(t=>`<label class="media-pick"><input type="checkbox" data-media="${esc(t)}"><span>${esc(mediaLabel(t))}</span></label>`).join("");
      if(state.opts.providers?.includes(prefs.provider)) provSel.value=prefs.provider;
      if(["watchlist","history","ratings","combined"].includes(prefs.feature)) featSel.value=prefs.feature;
      qInput.value=prefs.q||""; allChk.checked=prefs.all!==false; watchedDateChk.checked=prefs.include_watched_date!==false; syncInstances(); syncFormats();
      setMediaTypes(prefs.media_types||state.opts.default_media_types||["movie"]);
      syncCapabilities(); syncWatchedDateOption(); enableColumnResize(table);
    }finally{hideWait()}

    const debounce=(fn,ms=250)=>{let t; return (...a)=>{clearTimeout(t); t=setTimeout(()=>fn(...a),ms)}};
    const autoRefresh=debounce(()=>renderPreview(true),200), reset=cb=>()=>{state.selected.clear(); state.mode="all"; allChk.checked=true; cb?.(); savePrefs(); autoRefresh()};
    provBtn.addEventListener("click",e=>{e.stopPropagation(); provMenu.classList.contains("open")?closeProv():openProv()});
    provMenu.addEventListener("click",e=>{const btn=e.target.closest(".prov-opt"); if(!btn) return; provSel.value=btn.dataset.provider; closeProv(); reset(syncInstances)()});
    document.addEventListener("click",e=>{if(!e.target.closest(".provider-field")) closeProv()});
    instSel.addEventListener("change",reset());
    featSel.addEventListener("change",reset(()=>{syncFormats(); syncCapabilities(); syncWatchedDateOption()}));
    fmtSel.addEventListener("change",()=>{syncCapabilities(); syncWatchedDateOption(); savePrefs(); autoRefresh()});
    watchedDateChk.addEventListener("change",()=>{savePrefs(); autoRefresh()});
    mediaWrap.addEventListener("change",e=>{if(!e.target.closest('input[type="checkbox"][data-media]')) return; if(!selectedMediaTypes().length){e.target.checked=true} savePrefs(); autoRefresh()});
    qInput.addEventListener("input",()=>{savePrefs(); autoRefresh()});
    allChk.addEventListener("change",()=>{state.mode=allChk.checked?"all":"manual"; if(state.mode==="all") state.selected.clear(); savePrefs(); autoRefresh()});
    btnPrev.addEventListener("click",()=>renderPreview(false));
    btnExp.addEventListener("click",doExport);
    el("#ex-close").addEventListener("click",closeModal);
    tbody.addEventListener("change",e=>{const cb=e.target.closest(".row-check"); if(!cb) return; const key=cb.closest("tr")?.dataset.key; if(!key) return; if(state.mode==="all"){state.mode="manual"; allChk.checked=false} cb.checked?state.selected.add(key):state.selected.delete(key); refreshCounts()});
    tbody.addEventListener("click",e=>{const tr=e.target.closest("tr[data-key]"); if(!tr||e.target.closest("input,button,select,.resizer")) return; const cb=$(".row-check",tr); if(cb){cb.checked=!cb.checked; cb.dispatchEvent(new Event("change",{bubbles:true}))}});
    await renderPreview(false);
  },
  unmount(){}
};
