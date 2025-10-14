// assets/js/modals/pair-config/index.js

// Helpers
const ID=(x,r=document)=>(r.getElementById?r.getElementById(x):r.querySelector("#"+x));
const Q=(s,r=document)=>r.querySelector(s);
const QA=(s,r=document)=>Array.from(r.querySelectorAll(s));
const G=typeof window!=="undefined"?window:globalThis;
const jclone=(o)=>JSON.parse(JSON.stringify(o||{}));

// Provider helpers
const same=(a,b)=>String(a||"").trim().toLowerCase()===String(b||"").trim().toLowerCase();
const isSimkl=(v)=>same(v,"simkl");
const isJelly=(v)=>same(v,"jellyfin");
function hasSimkl(state){return isSimkl(state?.src)||isSimkl(state?.dst)}
function hasJelly(state){return isJelly(state?.src)||isJelly(state?.dst)}
function iconPath(n){const key=String(n||"").trim().toUpperCase();return `/assets/img/${key}.svg`}
function logoHTML(n,l){const src=iconPath(n),alt=(l||n||"Provider")+" logo";return `<span class="prov-wrap"><img class="prov-logo" src="${src}" alt="${alt}" width="36" height="36" onerror="this.style.display='none'; this.nextElementSibling.style.display='inline-block'"/><span class="prov-fallback" style="display:none">${l||n||"—"}</span></span>`}

// Flow anim CSS
function flowAnimCSSOnce(){if(ID("cx-flow-anim-css"))return;const st=document.createElement("style");st.id="cx-flow-anim-css";st.textContent=`@keyframes cx-flow-one{0%{left:0;opacity:.2}50%{opacity:1}100%{left:calc(100% - 8px);opacity:.2}}
@keyframes cx-flow-two-a{0%{left:0;opacity:.2}50%{opacity:1}100%{left:calc(100% - 8px);opacity:.2}}
@keyframes cx-flow-two-b{0%{left:calc(100% - 8px);opacity:.2}50%{opacity:1}100%{left:0;opacity:.2}}
.flow-rail.pretty.anim-one .dot.flow.a{animation:cx-flow-one 1.2s ease-in-out infinite}
.flow-rail.pretty.anim-one .dot.flow.b{animation:cx-flow-one 1.2s ease-in-out .6s infinite}
.flow-rail.pretty.anim-two .dot.flow.a{animation:cx-flow-two-a 1.2s ease-in-out infinite}
.flow-rail.pretty.anim-two .dot.flow.b{animation:cx-flow-two-b 1.2s ease-in-out infinite}`;document.head.appendChild(st)}

// Inline footer
function ensureInlineFoot(modal){if(!modal)return;const card=Q(".cx-card",modal)||modal;let bar=card.querySelector(":scope > .cx-actions");if(!bar){bar=document.createElement("div");bar.className="cx-actions";const cancel=document.createElement("button");cancel.className="cx-btn";cancel.textContent="Cancel";cancel.addEventListener("click",()=>G.cxCloseModal?.());const save=document.createElement("button");save.className="cx-btn primary";save.id="cx-inline-save";save.textContent="Save";save.addEventListener("click",async()=>{const b=ID("cx-inline-save");if(!b)return;const old=b.textContent;b.disabled=true;b.textContent="Saving…";try{await modal.__doSave?.()}finally{b.disabled=false;b.textContent=old}});bar.append(cancel,save);card.appendChild(bar)}}

// Ratings summary
function updateRtSummary(){
  const m=ID("cx-modal"),st=m?.__state,rt=st?.options?.ratings||{},det=ID("cx-rt-adv"),sum=det?.querySelector("summary");
  if(!sum)return;
  const types=Array.isArray(rt.types)&&rt.types.length?rt.types.join(", "):"movies, shows, episodes";
  const mode=String(rt.mode||"all")==="from_date"?(rt.from_date?`From ${rt.from_date}`:"From a date"):"All";
  sum.innerHTML=`<span class="pill">Scope: ${types}</span><span class="summary-gap">•</span><span class="pill">Mode: ${mode}</span>`;
  sum.setAttribute("aria-expanded",det.open?"true":"false");
}

// Template
const tpl=()=>`
  <div id="cx-modal" class="cx-card">
    <div class="cx-head">
      <div class="title-wrap">
        <span class="material-symbols-rounded app-logo" aria-hidden="true">sync_alt</span>
        <div><div class="app-name">Configure Connection</div><div class="app-sub">Choose source → target and what to sync</div></div>
      </div>
      <label class="switch big head-toggle" title="Enable/Disable connection">
        <input type="checkbox" id="cx-enabled" checked><span class="slider" aria-hidden="true"></span>
        <span class="lab on" aria-hidden="true">Enabled</span><span class="lab off" aria-hidden="true">Disabled</span>
      </label>
    </div>
    <div class="cx-body">
      <div class="cx-top grid2">
        <div class="top-left">
          <div class="cx-row cx-st-row">
            <div class="field"><label>Source</label><div id="cx-src-display" class="input static" data-value=""></div><select id="cx-src" class="input hidden"></select></div>
            <div class="field"><label>Target</label><div id="cx-dst-display" class="input static" data-value=""></div><select id="cx-dst" class="input hidden"></select></div>
          </div>
          <div class="cx-row cx-mode-row">
            <div class="seg">
              <input type="radio" name="cx-mode" id="cx-mode-one" value="one"/><label for="cx-mode-one">One-way</label>
              <input type="radio" name="cx-mode" id="cx-mode-two" value="two"/><label for="cx-mode-two">Two-way</label>
            </div>
          </div>
          <div class="cx-row"><div id="cx-feat-tabs" class="feature-tabs"></div></div>
        </div>
        <div class="top-right">
          <div class="flow-card">
            <div class="flow-title">Sync flow: <span id="cx-flow-title">One-way</span></div>
            <div class="flow-rail pretty" id="cx-flow-rail">
              <span class="token" id="cx-flow-src"></span>
              <span class="arrow"><span class="dot flow a"></span><span class="dot flow b"></span></span>
              <span class="token" id="cx-flow-dst"></span>
            </div>
          </div>
          <div id="cx-flow-warn" class="flow-warn-area" aria-live="polite"></div>
        </div>
      </div>
      <div class="cx-main grid2">
        <div class="left"><div class="panel" id="cx-feat-panel"></div></div>
        <div class="right"><div class="panel" id="cx-adv-panel"></div></div>
      </div>
    </div>
  </div>
`;

// State
function defaultState(){
  return {
    providers:[],src:null,dst:null,feature:"globals",mode:"one-way",enabled:true,
    options:{
      watchlist:{enable:false,add:false,remove:false},
      ratings:{enable:false,add:false,remove:false,types:["movies","shows","episodes"],mode:"all",from_date:""},
      history:{enable:false,add:false,remove:false},
      playlists:{enable:false,add:true,remove:false}
    },
    jellyfin:{watchlist:{mode:"favorites",playlist_name:"Watchlist"}},
    globals:{
      dry_run:false,verify_after_write:false,drop_guard:false,allow_mass_delete:true,
      tombstone_ttl_days:30,include_observed_deletes:true,
      blackbox:{enabled:true,promote_after:1,unresolved_days:0,cooldown_days:30,pair_scoped:true,block_adds:true,block_removes:true}
    },
    cfgRaw:null,
    visited:new Set()
  }
}

// Data
async function getJSON(url){try{const r=await fetch(url,{cache:"no-store"});return r.ok?await r.json():null}catch{return null}}
async function loadProviders(state){
  const list=await getJSON("/api/sync/providers?cb="+Date.now());
  state.providers=Array.isArray(list)?list:[
    {name:"PLEX",label:"Plex",features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true},version:"1.0.0"},
    {name:"SIMKL",label:"Simkl",features:{watchlist:true,ratings:true,history:true,playlists:false},capabilities:{bidirectional:true},version:"1.0.0"},
    {name:"TRAKT",label:"Trakt",features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true},version:"1.0.0"},
    {name:"JELLYFIN",label:"Jellyfin",features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true},version:"1.2.1"}
  ]
}
async function loadConfigBits(state){
  const cfg=(await getJSON("/api/config?cb="+Date.now()))||{},s=cfg?.sync||{};
  state.cfgRaw=cfg||{};
  state.globals={
    dry_run:!!s.dry_run,
    verify_after_write:!!s.verify_after_write,
    drop_guard:!!s.drop_guard,
    allow_mass_delete:!!s.allow_mass_delete,
    tombstone_ttl_days:Number.isFinite(s.tombstone_ttl_days)?s.tombstone_ttl_days:30,
    include_observed_deletes:!!s.include_observed_deletes,
    blackbox:Object.assign(
      {enabled:true,promote_after:1,unresolved_days:0,cooldown_days:30,pair_scoped:true,block_adds:true,block_removes:true},
      s.blackbox||{}
    )
  };
  const jf=cfg?.jellyfin?.watchlist||{};
  const mode=(jf.mode==="playlist"||jf.mode==="favorites"||jf.mode==="collection"||jf.mode==="collections")?jf.mode:"favorites";
  state.jellyfin.watchlist.mode=(mode==="collections")?"collection":mode;
  state.jellyfin.watchlist.playlist_name=jf.playlist_name||"Watchlist";
}
async function loadPairById(id){
  if(!id)return null;const arr=await getJSON("/api/pairs?cb="+Date.now());if(Array.isArray(arr))return arr.find(p=>String(p.id)===String(id))||null;return null
}

// UI utils
const byName=(state,n)=>state.providers.find(p=>p.name===n);
const commonFeatures=(state)=>!state.src||!state.dst?[]:["watchlist","ratings","history","playlists"].filter(k=>byName(state,state.src)?.features?.[k]&&byName(state,state.dst)?.features?.[k]);
const defaultFor=(k)=>k==="watchlist"?{enable:false,add:false,remove:false}:k==="playlists"?{enable:false,add:true,remove:false}:{enable:false,add:false,remove:false};
function getOpts(state,key){
  if(!state.visited.has(key)){
    if(key==="ratings") state.options.ratings=Object.assign({enable:false,add:false,remove:false,types:["movies","shows","episodes"],mode:"all",from_date:""},state.options.ratings||{});
    else state.options[key]=state.options[key]??defaultFor(key);
    state.visited.add(key);
  }
  return state.options[key];
}

function restartFlowAnimation(mode){
  const rail=ID("cx-flow-rail");if(!rail)return;
  const arrow=rail.querySelector(".arrow"),dots=[...rail.querySelectorAll(".dot.flow")];
  ["anim-one","anim-two"].forEach(c=>{rail.classList.remove(c);arrow?.classList.remove(c);dots.forEach(d=>d.classList.remove(c))});
  void rail.offsetWidth;const cls=mode==="two"?"anim-two":"anim-one";[rail,arrow,...dots].forEach(n=>n?.classList.add(cls))
}

function renderWarnings(state){
  const flowBox=ID("cx-flow-warn"),main=Q(".cx-main");
  const HIDE=new Set(["globals","providers"]);
  const BOTTOM=new Set(["watchlist","ratings","history","playlists"]);
  if(flowBox) flowBox.innerHTML="";
  ID("cx-feat-warn")?.remove();
  if(HIDE.has(state.feature)) return;
  const src=byName(state,state.src),dst=byName(state,state.dst);
  const isExp=v=>(parseInt(String(v||"0").split(".")[0],10)||0)<1;
  const html=[src,dst].reduce((a,p)=>a+(p&&isExp(p.version)?`<div class="module-alert experimental-alert"><div class="title"><span class="ic">⚠</span> Experimental Module: ${p.label||p.name||"Provider"}</div><div class="body"><div class="mini">Not stable yet. Limited functionality. Prefer Dry run and verify results.</div></div></div>`:""),"");
  if(!html) return;
  if(BOTTOM.has(state.feature)){
    const host=document.createElement("div");
    host.id="cx-feat-warn";host.className="cx-bottom-warn";host.innerHTML=html;
    main?.appendChild(host);
  }else{
    if(flowBox) flowBox.innerHTML=html;
  }
}

function renderProviderSelects(state){
  const srcSel=ID("cx-src"),dstSel=ID("cx-dst"),srcLab=ID("cx-src-display"),dstLab=ID("cx-dst-display");
  const opts=state.providers.map(p=>`<option value="${p.name}">${p.label}</option>`).join("");
  srcSel.innerHTML=`<option value="">Select…</option>${opts}`;dstSel.innerHTML=`<option value="">Select…</option>${opts}`;
  if(state.src) srcSel.value=state.src;if(state.dst) dstSel.value=state.dst;
  const upd=()=>{const s=byName(state,srcSel.value),d=byName(state,dstSel.value);if(srcLab){srcLab.textContent=s?.label||"—";srcLab.dataset.value=srcSel.value||""}if(dstLab){dstLab.textContent=d?.label||"—";dstLab.dataset.value=dstSel.value||""}};
  srcSel.onchange=()=>{state.src=srcSel.value||null;upd();updateFlow(state,true);refreshTabs(state);renderWarnings(state)};
  dstSel.onchange=()=>{state.dst=dstSel.value||null;upd();updateFlow(state,true);refreshTabs(state);renderWarnings(state)};
  upd();ID("cx-mode-two").checked=state.mode==="two-way";ID("cx-mode-one").checked=!ID("cx-mode-two").checked;ID("cx-enabled").checked=!!state.enabled;
}

function updateFlow(state,animate=false){
  const s=byName(state,state.src),d=byName(state,state.dst);
  Q("#cx-flow-src").innerHTML=s?logoHTML(s.name,s.label):"";Q("#cx-flow-dst").innerHTML=d?logoHTML(d.name,d.label):"";
  const two=ID("cx-mode-two"),ok=byName(state,state.src)?.capabilities?.bidirectional&&byName(state,state.dst)?.capabilities?.bidirectional;
  two.disabled=!ok;if(!ok&&two.checked)ID("cx-mode-one").checked=true;two.nextElementSibling?.classList.toggle("disabled",!ok);
  const t=ID("cx-flow-title");if(t)t.textContent=ID("cx-mode-two")?.checked?"Two-way (bidirectional)":"One-way";
  updateFlowClasses(state);if(animate)restartFlowAnimation(ID("cx-mode-two")?.checked?"two":"one");renderWarnings(state)
}
function updateFlowClasses(state){
  const rail=ID("cx-flow-rail"); if(!rail) return;
  const two=ID("cx-mode-two")?.checked;
  const enabled=!!ID("cx-enabled")?.checked;
  const wl=state.options.watchlist||{enable:false,add:false,remove:false};

  rail.className="flow-rail pretty";
  rail.classList.toggle("mode-two",!!two);
  rail.classList.toggle("mode-one",!two);

  const flowOn=enabled&&wl.enable&&(two?(wl.add||wl.remove):(wl.add||wl.remove));
  rail.classList.toggle("off",!flowOn);
  if(two) rail.classList.toggle("active",flowOn);
  else{
    rail.classList.toggle("dir-add",flowOn&&wl.add);
    rail.classList.toggle("dir-remove",flowOn&&!wl.add&&wl.remove);
  }

  const need=two?"anim-two":"anim-one";
  const parts=[rail,rail.querySelector(".arrow"),...rail.querySelectorAll(".dot.flow")];
  parts.forEach(n=>{if(!n)return;if(!n.classList.contains(need)){n.classList.remove("anim-one","anim-two");n.classList.add(need)}});
}

// Fold toggles (works with draggable modals)
function bindFoldToggles(root){
  const isSummary=(el)=>el && el.tagName==="SUMMARY";
  root.addEventListener("click",(e)=>{
    const sum=e.target.closest?.("summary.fold-head, .fold > summary");
    if(!sum)return;
    const det=sum.closest("details"); if(!det)return;
    e.preventDefault(); e.stopPropagation();
    det.open=!det.open;
    det.classList.toggle("open", det.open);   // ← sync class
  });
  root.addEventListener("keydown",(e)=>{
    const sum=e.target.closest?.("summary.fold-head, .fold > summary");
    if(!sum)return;
    if(e.key===" "||e.key==="Enter"){
      const det=sum.closest("details"); if(!det)return;
      e.preventDefault(); e.stopPropagation();
      det.open=!det.open;
      det.classList.toggle("open", det.open); // ← sync class
    }
  });

}

function applySubDisable(feature){
  const map={
    watchlist:["#cx-wl-add","#cx-wl-remove","#cx-jf-wl-mode-fav","#cx-jf-wl-mode-pl","#cx-jf-wl-mode-col","#cx-jf-wl-pl-name"],
    ratings:["#cx-rt-add","#cx-rt-remove","#cx-rt-type-all","#cx-rt-type-movies","#cx-rt-type-shows","#cx-rt-type-episodes","#cx-rt-mode","#cx-rt-from-date"],
    history:["#cx-hs-add"],
    playlists:["#cx-pl-add","#cx-pl-remove"]
  };
  const on=ID(feature==="ratings"?"cx-rt-enable":feature==="watchlist"?"cx-wl-enable":feature==="history"?"cx-hs-enable":"cx-pl-enable")?.checked;
  (map[feature]||[]).forEach(sel=>{const n=Q(sel);if(n){n.disabled=!on;n.closest?.(".opt-row")?.classList.toggle("muted",!on)}});
}

function renderFeaturePanel(state){
  if(state.feature!=="providers"){ ID("cx-prov-warn")?.remove(); }
  const left=ID("cx-feat-panel"),right=ID("cx-adv-panel");if(!left||!right)return;

  if(state.feature==="providers"){
    const cfg=state.cfgRaw||{};
    const plex=cfg.plex||{};
    const jf=cfg.jellyfin||{};
    const tr=cfg.trakt||{};
    const sim=cfg.simkl||{};
    left.innerHTML=`<div class="panel-title"><span class="material-symbols-rounded" style="vertical-align:-3px;margin-right:6px;">dns</span>Media Servers</div>
      <details class="mods fold" id="prov-plex"><summary class="fold-head"><span>Plex</span><span class="chev">expand_more</span></summary><div class="fold-body">
        <div class="grid2 compact" style="padding:8px 0 2px">
          <div class="opt-row"><label for="plx-rating-workers">Rating workers</label><input id="plx-rating-workers" class="input small" type="number" min="1" max="64" step="1" value="${plex.rating_workers??12}"></div>
          <div class="opt-row"><label for="plx-history-workers">History workers</label><input id="plx-history-workers" class="input small" type="number" min="1" max="64" step="1" value="${plex.history_workers??12}"></div>
          <div class="opt-row"><label for="plx-wl-pms">Allow PMS fallback</label><label class="switch"><input id="plx-wl-pms" type="checkbox" ${plex.watchlist_allow_pms_fallback?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row"><label for="plx-wl-limit">Discover query limit</label><input id="plx-wl-limit" class="input small" type="number" min="5" max="50" step="1" value="${plex.watchlist_query_limit??25}"></div>
          <div class="opt-row"><label for="plx-wl-delay">Write delay (ms)</label><input id="plx-wl-delay" class="input small" type="number" min="0" max="5000" step="10" value="${plex.watchlist_write_delay_ms??0}"></div>
          <div class="opt-row"><label for="plx-fallback-guid">Fallback GUID</label><label class="switch"><input id="plx-fallback-guid" type="checkbox" ${plex.fallback_GUID?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row" style="grid-column:1/-1"><label for="plx-wl-guid">GUID priority</label><input id="plx-wl-guid" class="input" type="text" value="${(plex.watchlist_guid_priority||["tmdb","imdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"]).join(", ")}"></div>
            </div>
      </div></details>

      <details class="mods fold" id="prov-jelly">
        <summary class="fold-head"><span>Jellyfin</span><span class="chev">expand_more</span></summary>
        <div class="fold-body">
          <div class="grid2 compact" style="padding:8px 0 2px">
            <div class="opt-row">
              <label for="jf-ssl">Verify SSL</label>
              <label class="switch"><input id="jf-ssl" type="checkbox" ${jf.verify_ssl?"checked":""}><span class="slider"></span></label>
            </div>
            <div class="opt-row"></div>

            <!-- Watchlist -->
            <div class="prov-box" style="grid-column:1/-1">
              <div class="panel-title small">Watchlist</div>
              <div class="grid2 compact">
                <div class="opt-row">
                  <label for="jf-wl-limit">Query limit</label>
                  <input id="jf-wl-limit" class="input small" type="number" min="5" max="1000" value="${jf.watchlist?.watchlist_query_limit??25}">
                </div>
                <div class="opt-row">
                  <label for="jf-wl-delay">Write delay (ms)</label>
                  <input id="jf-wl-delay" class="input small" type="number" min="0" max="5000" value="${jf.watchlist?.watchlist_write_delay_ms??0}">
                </div>
              </div>
            </div>

            <!-- History -->
            <div class="prov-box" style="grid-column:1/-1">
              <div class="panel-title small">History</div>
              <div class="grid2 compact">
                <div class="opt-row">
                  <label for="jf-hs-limit">Query limit</label>
                  <input id="jf-hs-limit" class="input small" type="number" min="5" max="1000" value="${jf.history?.history_query_limit??25}">
                </div>
                <div class="opt-row">
                  <label for="jf-hs-delay">Write delay (ms)</label>
                  <input id="jf-hs-delay" class="input small" type="number" min="0" max="5000" value="${jf.history?.history_write_delay_ms??0}">
                </div>
              </div>
            </div>

            <!-- Ratings -->
            <div class="prov-box" style="grid-column:1/-1">
              <div class="panel-title small">Ratings</div>
              <div class="grid2 compact">
                <div class="opt-row">
                  <label for="jf-rt-limit">Ratings query limit</label>
                  <input id="jf-rt-limit" class="input small" type="number" min="100" max="10000" value="${jf.ratings?.ratings_query_limit??2000}">
                </div>
              </div>
            </div>

          </div>
        </div>
      </details>


    `;
    right.innerHTML=`<div class="panel-title"><span class="material-symbols-rounded" style="vertical-align:-3px;margin-right:6px;">flag_circle</span>Trackers</div>
      <details class="mods fold" id="prov-simkl"><summary class="fold-head"><span>Simkl</span><span class="chev">expand_more</span></summary><div class="fold-body">
        <div class="muted" style="padding:10px 12px">No additional settings.</div>
      </div></details>

      <details class="mods fold" id="prov-trakt"><summary class="fold-head"><span>Trakt</span><span class="chev">expand_more</span></summary><div class="fold-body">
        <div class="panel-title small" style="margin:6px 0 4px">Watchlist</div>
        <div class="grid2 compact">
          <div class="opt-row"><label for="tr-wl-etag">Use ETag</label><label class="switch"><input id="tr-wl-etag" type="checkbox" ${tr.watchlist_use_etag!==false?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row"><label for="tr-wl-batch">Batch size</label><input id="tr-wl-batch" class="input small" type="number" min="10" max="500" value="${tr.watchlist_batch_size??100}"></div>
          <div class="opt-row"><label for="tr-wl-log">Log rate limits</label><label class="switch"><input id="tr-wl-log" type="checkbox" ${tr.watchlist_log_rate_limits?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row"><label for="tr-wl-freeze">Freeze details</label><label class="switch"><input id="tr-wl-freeze" type="checkbox" ${tr.watchlist_freeze_details!==false?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row" style="grid-column:1/-1"><label for="tr-wl-ttl">Shadow TTL (hours)</label><input id="tr-wl-ttl" class="input small" type="number" min="1" max="9999" value="${tr.watchlist_shadow_ttl_hours??168}"></div>
        </div>

        <div class="panel-title small" style="margin:8px 0 4px">Ratings</div>
        <div class="grid2 compact">
          <div class="opt-row"><label for="tr-rt-page">Per page</label><input id="tr-rt-page" class="input small" type="number" min="10" max="100" value="${tr.ratings_per_page??100}"></div>
          <div class="opt-row"><label for="tr-rt-pages">Max pages</label><input id="tr-rt-pages" class="input small" type="number" min="1" max="100000" value="${tr.ratings_max_pages??50}"></div>
          <div class="opt-row"><label for="tr-rt-chunk">Chunk size</label><input id="tr-rt-chunk" class="input small" type="number" min="10" max="100" value="${tr.ratings_chunk_size??100}"></div>
        </div>

        <div class="panel-title small" style="margin:8px 0 4px">History</div>
        <div class="grid2 compact">
          <div class="opt-row"><label for="tr-hs-page">Per page</label><input id="tr-hs-page" class="input small" type="number" min="10" max="100" value="${tr.history_per_page??100}"></div>
          <div class="opt-row"><label for="tr-hs-pages">Max pages</label><input id="tr-hs-pages" class="input small" type="number" min="1" max="100000" value="${tr.history_max_pages??100000}"></div>
          <div class="opt-row" style="grid-column:1/-1"><label for="tr-hs-unres">Unresolved freeze</label><label class="switch"><input id="tr-hs-unres" type="checkbox" ${tr.history_unresolved?"checked":""}><span class="slider"></span></label></div>
        </div>
      </div></details>
    `;
      {
      const grid = left.querySelector('#prov-plex .fold-body .grid2');
      if (grid && !ID('plx-fallback-guid')) {
        const row = document.createElement('div');
        row.className = 'opt-row';
        row.innerHTML =
          '<label for="plx-fallback-guid">Fallback GUID</label>' +
          '<label class="switch"><input id="plx-fallback-guid" type="checkbox" ' +
          ((cfg.plex || {}).fallback_GUID ? 'checked' : '') +
          '><span class="slider"></span></label>';
        const before = ID('plx-wl-guid')?.closest('.opt-row');
        grid.insertBefore(row, before || null); 
      }
    }
    // Add neon warning spanning both columns
    const main = Q(".cx-main");
    let warn = ID("cx-prov-warn");
    if (!warn) {
      warn = document.createElement("div");
      warn.id = "cx-prov-warn";
      warn.className = "prov-warning";
      main.appendChild(warn);
    }
    warn.innerHTML = `
      <span class="material-symbols-rounded" aria-hidden="true">warning</span>
      Advanced provider settings — do not change unless you know what you're doing!
    `;
    QA(".fold").forEach(f=>{f.classList.remove("open")});
  
    return;
  }

  if(state.feature==="globals"){
    const g=state.globals||{},bb=g.blackbox||{};
    left.innerHTML=`<div class="panel-title"><span class="material-symbols-rounded" style="vertical-align:-3px;margin-right:6px;">tune</span>Globals</div>
      <div class="opt-row"><label for="gl-dry">Dry run</label><label class="switch"><input id="gl-dry" type="checkbox" ${g.dry_run?"checked":""}><span class="slider"></span></label></div><div class="muted">Simulate changes only; no writes.</div>
      <div class="opt-row"><label for="gl-verify">Verify after write</label><label class="switch"><input id="gl-verify" type="checkbox" ${g.verify_after_write?"checked":""}><span class="slider"></span></label></div><div class="muted">Re-check a small sample after writes.</div>
      <div class="opt-row"><label for="gl-drop">Drop guard</label><label class="switch"><input id="gl-drop" type="checkbox" ${g.drop_guard?"checked":""}><span class="slider"></span></label></div><div class="muted">Protect against empty source snapshots.</div>
      <div class="opt-row"><label for="gl-mass">Allow mass delete</label><label class="switch"><input id="gl-mass" type="checkbox" ${g.allow_mass_delete?"checked":""}><span class="slider"></span></label></div><div class="muted">Permit bulk removals when required.</div>`;
    right.innerHTML=`<div class="panel-title">Advanced</div>
      <div class="opt-row"><label for="gl-ttl">Tombstone TTL (days)</label><input id="gl-ttl" class="input" type="number" min="0" step="1" value="${g.tombstone_ttl_days??30}"></div><div class="muted">Keep delete markers to avoid re-adding.</div>
      <div class="opt-row"><label for="gl-observed">Include observed deletes</label><label class="switch"><input id="gl-observed" type="checkbox" ${g.include_observed_deletes?"checked":""}><span class="slider"></span></label></div><div class="muted"></div>
      <div class="panel-title small" style="margin-top:10px">Blackbox</div>
      <div class="grid2 compact">
        <div class="opt-row"><label for="gl-bb-enable">Enabled</label><label class="switch"><input id="gl-bb-enable" type="checkbox" ${bb.enabled?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="gl-bb-pair">Pair scoped</label><label class="switch"><input id="gl-bb-pair" type="checkbox" ${bb.pair_scoped?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="gl-bb-promote">Promote after (days)</label><input id="gl-bb-promote" class="input small" type="number" min="0" max="365" step="1" value="${bb.promote_after??1}"></div>
        <div class="opt-row"><label for="gl-bb-unresolved">Unresolved days</label><input id="gl-bb-unresolved" class="input small" type="number" min="0" max="365" step="1" value="${bb.unresolved_days??0}"></div>
        <div class="opt-row"><label for="gl-bb-cooldown">Cooldown days</label><input id="gl-bb-cooldown" class="input small" type="number" min="0" max="365" step="1" value="${bb.cooldown_days??30}"></div>
      </div>
      <div class="muted"></div>`;
    return;
  }

  if(state.feature==="watchlist"){
    const wl=getOpts(state,"watchlist");
    left.innerHTML=`<div class="panel-title">Watchlist — basics</div>
      <div class="opt-row"><label for="cx-wl-enable">Enable</label><label class="switch"><input id="cx-wl-enable" type="checkbox" ${wl.enable?"checked":""}><span class="slider"></span></label></div>
      <div class="grid2"><div class="opt-row"><label for="cx-wl-add">Add</label><label class="switch"><input id="cx-wl-add" type="checkbox" ${wl.add?"checked":""}><span class="slider"></span></label></div>
      <div class="opt-row"><label for="cx-wl-remove">Remove</label><label class="switch"><input id="cx-wl-remove" type="checkbox" ${wl.remove?"checked":""}><span class="slider"></span></label></div></div>`;
    if(hasJelly(state)){
      const jfw=state.jellyfin?.watchlist||{mode:"favorites",playlist_name:"Watchlist"};
      right.innerHTML=`<div class="panel-title">Advanced</div>
        <div class="panel-title small" style="margin-top:6px">Jellyfin specifics</div>
        <details id="cx-jf-wl" open>
          <summary class="muted" style="margin-bottom:10px;">Favorites / Playlist / Collections</summary>
          <div class="grid2 compact">
            <div class="opt-row" style="grid-column:1/-1">
              <label>Mode</label>
              <div class="seg">
                <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-fav" value="favorites" ${jfw.mode==="favorites"?"checked":""}/><label for="cx-jf-wl-mode-fav">Favorites</label>
                <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-pl" value="playlist" ${jfw.mode==="playlist"?"checked":""}/><label for="cx-jf-wl-mode-pl">Playlist</label>
                <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-col" value="collection" ${(jfw.mode==="collection"||jfw.mode==="collections")?"checked":""}/><label for="cx-jf-wl-mode-col">Collections</label>
              </div>
            </div>
            <div class="opt-row" style="grid-column:1/-1"><label for="cx-jf-wl-pl-name">Name</label>
              <input id="cx-jf-wl-pl-name" class="input" type="text" value="${jfw.playlist_name||"Watchlist"}" placeholder="Watchlist"></div>
              </div>
          <div class="hint" style="text-align:center;">Jellyfin doesn’t support a Watchlist. Simulate one with <b>Favorites</b>, <b>Playlist</b>, or <b>Collections</b>. Tip: prefer <b>Favorites</b> or <b>Collections</b></div>
        </details>`;
    }else{
      right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">More controls coming later.</div>`;
    }
    applySubDisable("watchlist");
    return;
  }

  if(state.feature==="ratings"){
    const rt=getOpts(state,"ratings"),hasType=t=>Array.isArray(rt.types)&&rt.types.includes(t);
    left.innerHTML=`<div class="panel-title">Ratings — basics</div>
      <div class="opt-row"><label for="cx-rt-enable">Enable</label><label class="switch"><input id="cx-rt-enable" type="checkbox" ${rt.enable?"checked":""}><span class="slider"></span></label></div>
      <div class="grid2"><div class="opt-row"><label for="cx-rt-add">Add / Update</label><label class="switch"><input id="cx-rt-add" type="checkbox" ${rt.add?"checked":""}><span class="slider"></span></label></div>
      <div class="opt-row"><label for="cx-rt-remove">Remove (clear)</label><label class="switch"><input id="cx-rt-remove" type="checkbox" ${rt.remove?"checked":""}><span class="slider"></span></label></div></div>
      <div class="panel-title small">Scope</div>
      <div class="grid2 compact">
        <div class="opt-row"><label for="cx-rt-type-all">All</label><label class="switch"><input id="cx-rt-type-all" type="checkbox" ${(hasType("movies")&&hasType("shows")&&hasType("episodes"))?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-rt-type-movies">Movies</label><label class="switch"><input id="cx-rt-type-movies" type="checkbox" ${hasType("movies")?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-rt-type-shows">Shows</label><label class="switch"><input id="cx-rt-type-shows" type="checkbox" ${hasType("shows")?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-rt-type-episodes">Episodes</label><label class="switch"><input id="cx-rt-type-episodes" type="checkbox" ${hasType("episodes")?"checked":""}><span class="slider"></span></label></div>
      </div>`;
    const simkl=hasSimkl(state);
    right.innerHTML=`<div class="panel-title">Advanced</div>
      <details id="cx-rt-adv" open>
        <summary class="muted" style="margin-bottom:10px;"></summary>
        <div class="panel-title small">History window</div>
        <div class="grid2">
          <div class="opt-row"><label for="cx-rt-mode">Mode</label>
            <select id="cx-rt-mode" class="input">
              <option value="all" ${rt.mode==="all"?"selected":""}>All</option>
              <option value="from_date" ${rt.mode==="from_date"?"selected":""}>From a date…</option>
            </select>
          </div>
          <div class="opt-row"><label for="cx-rt-from-date">From date</label><input id="cx-rt-from-date" class="input small" type="date" value="${rt.from_date||""}" ${rt.mode==="from_date"?"":"disabled"}></div>
        </div>
        <div class="hint">All is everything or “From a date”.</div>
      </details>
      ${simkl?`<div class="simkl-alert" role="note" aria-live="polite"><div class="title"><span class="ic">⚠</span> Simkl heads-up for Ratings</div><div class="body"><ul class="bul"><li><b>Movies:</b> Rating auto-marks as <i>Completed</i> on Simkl.</li><li>Can appear under <i>Recently watched</i> and <i>My List</i>.</li></ul><div class="mini">Tip: Prefer small windows when backfilling.</div></div></div>`:""}`;
    try{updateRtSummary()}catch{}
    applySubDisable("ratings");
    return;
  }

  if(state.feature==="history"){
    const hs=getOpts(state,"history");
    left.innerHTML=`<div class="panel-title">History — basics</div>
      <div class="opt-row"><label for="cx-hs-enable">Enable</label><label class="switch"><input id="cx-hs-enable" type="checkbox" ${hs.enable?"checked":""}><span class="slider"></span></label></div>
      <div class="grid2">
        <div class="opt-row"><label for="cx-hs-add">Add</label><label class="switch"><input id="cx-hs-add" type="checkbox" ${hs.add?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label class="muted">Remove (disabled)</label>
          <label class="switch" style="opacity:.5;pointer-events:none">
            <input id="cx-hs-remove" type="checkbox" disabled>
            <span class="slider"></span>
          </label>
        </div>
      </div>
      <div class="muted">Synchronize plays between providers. Deletions are disabled.</div>`;
    right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">More controls coming later.</div>`;
    applySubDisable("history");
    return;
  }

  const pl=getOpts(state,"playlists");
  left.innerHTML=`<div class="panel-title">Playlists</div>
    <div class="grid2"><div class="opt-row"><label for="cx-pl-enable">Enable</label><label class="switch"><input id="cx-pl-enable" type="checkbox" ${pl.enable?"checked":""}><span class="slider"></span></label></div>
    <div class="opt-row"><label for="cx-pl-add">Add</label><label class="switch"><input id="cx-pl-add" type="checkbox" ${pl.add?"checked":""}><span class="slider"></span></label></div>
    <div class="opt-row"><label for="cx-pl-remove">Remove</label><label class="switch"><input id="cx-pl-remove" type="checkbox" ${pl.remove?"checked":""}><span class="slider"></span></label></div></div>`;
  right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">Experimental.</div>`;
  applySubDisable("playlists");
}

// Tabs
function refreshTabs(state){
  const tabs = ID('cx-feat-tabs'); if(!tabs) return;
  const LABELS = {globals:'Globals',providers:'Providers',watchlist:'Watchlist',ratings:'Ratings',history:'History',playlists:'Playlists'};
  const ORDER  = ['globals','providers','watchlist','ratings','history','playlists'];
  const COMMON = new Set(commonFeatures(state));
  const isValid = k => k==='globals' || k==='providers' || (ORDER.includes(k) && COMMON.has(k));
  if(!isValid(state.feature)) state.feature = 'globals';

  tabs.innerHTML = '';
  ORDER.forEach(k=>{
    if(!['globals','providers'].includes(k) && !COMMON.has(k)) return;
    const b = document.createElement('button');
    b.className = 'ftab'; b.dataset.key = k;
    const icon = k==='globals' ? 'tune' : k==='providers' ? 'dns' : '';
    b.innerHTML = icon
      ? `<span class="material-symbols-rounded" style="font-size:16px;vertical-align:-3px;margin-right:6px;">${icon}</span>${LABELS[k]||k}`
      : (LABELS[k]||k);
    b.onclick = ()=>{
      state.feature = k;
      renderFeaturePanel(state);
      renderWarnings(state);
      [...tabs.children].forEach(c=>c.classList.toggle('active', c.dataset.key===k));
      restartFlowAnimation(ID("cx-mode-two")?.checked ? "two" : "one");
    };
    if(state.feature===k) b.classList.add('active');
    tabs.appendChild(b);
  });

  // initial render + warnings
  renderFeaturePanel(state);
  renderWarnings(state);

  // second pass
  queueMicrotask(()=>{
    renderFeaturePanel(state);
    renderWarnings(state);
    restartFlowAnimation(ID("cx-mode-two")?.checked ? "two" : "one");
  });
}

function bindChangeHandlers(state,root){
  root.addEventListener("change",(e)=>{
    const id=e.target.id,map={"cx-wl-enable":"cx-wl-remove","cx-rt-enable":"cx-rt-remove","cx-pl-enable":"cx-pl-remove"};
    if(map[id]){const rm=ID(map[id]);if(rm){rm.disabled=!e.target.checked;if(!e.target.checked)rm.checked=false}}
    if(id==="cx-wl-enable"||id==="cx-rt-enable"||id==="cx-hs-enable"||id==="cx-pl-enable"){applySubDisable(id.startsWith("cx-wl")?"watchlist":id.startsWith("cx-rt")?"ratings":id.startsWith("cx-hs")?"history":"playlists")}

    if(id.startsWith("cx-wl-")){
      state.options.watchlist={enable:!!ID("cx-wl-enable")?.checked,add:!!ID("cx-wl-add")?.checked,remove:!!ID("cx-wl-remove")?.checked};
      state.visited.add("watchlist");
    }

    if(id.startsWith("cx-rt-")){
      if(id==="cx-rt-enable"&&ID("cx-rt-enable")?.checked){
        ID("cx-rt-add").checked=true;ID("cx-rt-remove").checked=false;
        ["movies","shows","episodes"].forEach(t=>{const cb=ID("cx-rt-type-"+t);if(cb)cb.checked=true});
        const all=ID("cx-rt-type-all");if(all)all.checked=true;
        const modeSel=ID("cx-rt-mode");if(modeSel)modeSel.value="all";
        const fd=ID("cx-rt-from-date");if(fd){fd.value="";fd.disabled=true}
      }
      if(id==="cx-rt-type-all"){
        const on=!!ID("cx-rt-type-all")?.checked;["movies","shows","episodes"].forEach(t=>{const cb=ID("cx-rt-type-"+t);if(cb)cb.checked=on});
      }else if(/^cx-rt-type-(movies|shows|episodes)$/.test(id)){
        const allOn=["movies","shows","episodes"].every(t=>ID("cx-rt-type-"+t)?.checked);const allCb=ID("cx-rt-type-all");if(allCb)allCb.checked=!!allOn;
      }
      if(id==="cx-rt-mode"){
        const md=ID("cx-rt-mode")?.value||"all",fd=ID("cx-rt-from-date");
        if(fd){fd.disabled=md!=="from_date";if(md!=="from_date")fd.value=""}
      }
      const rt=state.options.ratings||{};
      const types=ID("cx-rt-type-all")?.checked?["movies","shows","episodes"]:["movies","shows","episodes"].filter(t=>ID("cx-rt-type-"+t)?.checked);
      state.options.ratings=Object.assign({},rt,{
        enable:!!ID("cx-rt-enable")?.checked,
        add:!!ID("cx-rt-add")?.checked,
        remove:!!ID("cx-rt-remove")?.checked,
        types,mode:ID("cx-rt-mode")?.value||"all",
        from_date:(ID("cx-rt-from-date")?.value||"").trim()
      });
      state.visited.add("ratings");
      try{updateRtSummary()}catch{}
    }

    if(id.startsWith("cx-hs-")){
      state.options.history={enable:!!ID("cx-hs-enable")?.checked,add:!!ID("cx-hs-add")?.checked,remove:false};
      state.visited.add("history");
    }

    if(id.startsWith("cx-pl-")){
      state.options.playlists={enable:!!ID("cx-pl-enable")?.checked,add:!!ID("cx-pl-add")?.checked,remove:!!ID("cx-pl-remove")?.checked};
      state.visited.add("playlists");
    }

    if(id.startsWith("gl-")){
      const g=state.globals||{};
      const bb={
        enabled:!!Q("#gl-bb-enable")?.checked,
        pair_scoped:!!Q("#gl-bb-pair")?.checked,
        promote_after:Math.min(365,Math.max(0,parseInt(Q("#gl-bb-promote")?.value||"0",10)||0)),
        unresolved_days:Math.min(365,Math.max(0,parseInt(Q("#gl-bb-unresolved")?.value||"0",10)||0)),
        cooldown_days:Math.min(365,Math.max(0,parseInt(Q("#gl-bb-cooldown")?.value||"0",10)||0))
      };
      bb.block_adds = bb.enabled;
      bb.block_removes = bb.enabled;
      state.globals={
        dry_run:!!Q("#gl-dry")?.checked,
        verify_after_write:!!Q("#gl-verify")?.checked,
        drop_guard:!!Q("#gl-drop")?.checked,
        allow_mass_delete:!!Q("#gl-mass")?.checked,
        tombstone_ttl_days:parseInt(Q("#gl-ttl")?.value||"0",10)||0,
        include_observed_deletes:!!Q("#gl-observed")?.checked,
        blackbox:bb
      };
    }

    if(id==="cx-jf-wl-mode-fav"||id==="cx-jf-wl-mode-pl"||id==="cx-jf-wl-mode-col"||id==="cx-jf-wl-pl-name"||id==="cx-wl-q"||id==="cx-wl-delay"||id==="cx-wl-guid"){
      const jf=state.jellyfin||(state.jellyfin={});
      const mode=ID("cx-jf-wl-mode-pl")?.checked?"playlist":ID("cx-jf-wl-mode-col")?.checked?"collection":"favorites";
      const name=(ID("cx-jf-wl-pl-name")?.value||"").trim()||"Watchlist";
      const q=parseInt(ID("cx-wl-q")?.value||"25",10)||25;
      const d=parseInt(ID("cx-wl-delay")?.value||"0",10)||0;
      const gp=(ID("cx-wl-guid")?.value||"").split(",").map(s=>s.trim()).filter(Boolean);
      jf.watchlist={mode,playlist_name:name,watchlist_query_limit:q,watchlist_write_delay_ms:d,watchlist_guid_priority:gp.length?gp:undefined};
    }

    if(id==="cx-enabled"||id==="cx-mode-one"||id==="cx-mode-two") updateFlow(state,true);
    updateFlowClasses(state);renderWarnings(state);
  });
}

// Save config bits
async function saveConfigBits(state){
  try{
    const cur=await fetch("/api/config",{cache:"no-store"}).then(r=>r.ok?r.json():{});
    const cfg=typeof structuredClone==="function"?structuredClone(cur||{}):jclone(cur||{});

    if(ID("gl-dry")){
      const s={
        dry_run:!!ID("gl-dry")?.checked,
        verify_after_write:!!ID("gl-verify")?.checked,
        drop_guard:!!ID("gl-drop")?.checked,
        allow_mass_delete:!!ID("gl-mass")?.checked,
        tombstone_ttl_days:Math.max(0,parseInt(ID("gl-ttl")?.value||"0",10)||0),
        include_observed_deletes:!!ID("gl-observed")?.checked
      };
      const bb={
        enabled:!!ID("gl-bb-enable")?.checked,
        pair_scoped:!!ID("gl-bb-pair")?.checked,
        promote_after:Math.min(365,Math.max(0,parseInt(ID("gl-bb-promote")?.value||"0",10)||0)),
        unresolved_days:Math.min(365,Math.max(0,parseInt(ID("gl-bb-unresolved")?.value||"0",10)||0)),
        cooldown_days:Math.min(365,Math.max(0,parseInt(ID("gl-bb-cooldown")?.value||"0",10)||0))
      };
      bb.block_adds = bb.enabled; bb.block_removes = bb.enabled;
      cfg.sync = Object.assign({}, cfg.sync || {}, s, { blackbox: Object.assign({}, cfg.sync?.blackbox||{}, bb) });
    }

    // Providers panel → persist
    if(ID("plx-rating-workers")){
      cfg.plex = Object.assign({}, cfg.plex||{}, {
        rating_workers: Math.max(1, parseInt(ID("plx-rating-workers").value||"12",10)||12),
        history_workers: Math.max(1, parseInt(ID("plx-history-workers").value||"12",10)||12),
        watchlist_allow_pms_fallback: !!ID("plx-wl-pms")?.checked,
        watchlist_query_limit: Math.max(1, parseInt(ID("plx-wl-limit").value||"25",10)||25),
        watchlist_write_delay_ms: Math.max(0, parseInt(ID("plx-wl-delay").value||"0",10)||0),
        watchlist_guid_priority: (ID("plx-wl-guid").value||"").split(",").map(s=>s.trim()).filter(Boolean), // ← comma here
        fallback_GUID: !!ID("plx-fallback-guid")?.checked
      });
    }

    if(ID("jf-ssl")){
      const jf=Object.assign({},cfg.jellyfin||{});
      jf.verify_ssl = !!ID("jf-ssl")?.checked;
      const mode = ID("jf-wl-pl")?.checked?"playlist":ID("jf-wl-col")?.checked?"collection":"favorites";
      const name = (ID("jf-wl-name")?.value||"Watchlist").trim()||"Watchlist";
      const wlLimit = Math.max(1, parseInt(ID("jf-wl-limit")?.value||"25",10)||25);
      const wlDelay = Math.max(0, parseInt(ID("jf-wl-delay")?.value||"0",10)||0);
      const wlPri = (ID("jf-wl-guid")?.value||"").split(",").map(s=>s.trim()).filter(Boolean);
      const hsLimit = Math.max(1, parseInt(ID("jf-hs-limit")?.value||"25",10)||25);
      const hsDelay = Math.max(0, parseInt(ID("jf-hs-delay")?.value||"0",10)||0);
      const hsPri = (ID("jf-hs-guid")?.value||"").split(",").map(s=>s.trim()).filter(Boolean);
      const rtLimit = Math.max(100, parseInt(ID("jf-rt-limit")?.value||"2000",10)||2000);
      jf.watchlist = Object.assign({}, jf.watchlist||{}, { mode, playlist_name:name, watchlist_query_limit:wlLimit, watchlist_write_delay_ms:wlDelay, watchlist_guid_priority:wlPri });
      jf.history = Object.assign({}, jf.history||{}, { history_query_limit:hsLimit, history_write_delay_ms:hsDelay, history_guid_priority:hsPri });
      jf.ratings = Object.assign({}, jf.ratings||{}, { ratings_query_limit:rtLimit });
      cfg.jellyfin=jf;
    }

    if(ID("tr-wl-etag")){
      cfg.trakt = Object.assign({}, cfg.trakt||{}, {
        watchlist_use_etag: !!ID("tr-wl-etag")?.checked,
        watchlist_shadow_ttl_hours: Math.max(1, parseInt(ID("tr-wl-ttl")?.value||"168",10)||168),
        watchlist_batch_size: Math.max(1, parseInt(ID("tr-wl-batch")?.value||"100",10)||100),
        watchlist_log_rate_limits: !!ID("tr-wl-log")?.checked,
        watchlist_freeze_details: !!ID("tr-wl-freeze")?.checked,
        ratings_per_page: Math.max(10, parseInt(ID("tr-rt-page")?.value||"100",10)||100),
        ratings_max_pages: Math.max(1, parseInt(ID("tr-rt-pages")?.value||"50",10)||50),
        ratings_chunk_size: Math.max(10, parseInt(ID("tr-rt-chunk")?.value||"100",10)||100),
        history_per_page: Math.max(10, parseInt(ID("tr-hs-page")?.value||"100",10)||100),
        history_max_pages: Math.max(1, parseInt(ID("tr-hs-pages")?.value||"100000",10)||100000),
        history_unresolved: !!ID("tr-hs-unres")?.checked
      });
    }

    const hasJF=String(state.src||"").toUpperCase()==="JELLYFIN"||String(state.dst||"").toUpperCase()==="JELLYFIN";
    if(hasJF){
      const jf=Object.assign({},cfg.jellyfin||{});
      const mode=ID("cx-jf-wl-mode-pl")?.checked?"playlist":ID("cx-jf-wl-mode-col")?.checked?"collection":ID("cx-jf-wl-mode-fav")?.checked?"favorites":(state.jellyfin?.watchlist?.mode||"favorites");
      const name=(ID("cx-jf-wl-pl-name")?.value||state.jellyfin?.watchlist?.playlist_name||"Watchlist").trim()||"Watchlist";
      const q= parseInt(ID("cx-wl-q")?.value||"25",10)||undefined;
      const d= parseInt(ID("cx-wl-delay")?.value||"0",10)||undefined;
      const gp=(ID("cx-wl-guid")?.value||"").split(",").map(s=>s.trim()).filter(Boolean);
      jf.watchlist=Object.assign({},jf.watchlist||{},{mode,playlist_name:name});
      if(Number.isFinite(q)) jf.watchlist.watchlist_query_limit=q;
      if(Number.isFinite(d)) jf.watchlist.watchlist_write_delay_ms=d;
      if(gp.length) jf.watchlist.watchlist_guid_priority=gp;
      cfg.jellyfin=jf;
    }

    const res=await fetch("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)});
    if(!res.ok)throw new Error("POST /api/config "+res.status);
  }catch(e){console.warn("[cx] saving config bits failed",e)}
}

function buildPayload(state,wrap){
  const src=state.src||ID("cx-src")?.value||ID("cx-src-display")?.dataset.value||"";
  const dst=state.dst||ID("cx-dst")?.value||ID("cx-dst-display")?.dataset.value||"";
  const modeTwo=!!ID("cx-mode-two")?.checked;const enabled=!!ID("cx-enabled")?.checked;
  const get=k=>Object.assign({enable:false,add:false,remove:false},(state.options||{})[k]||{});
  const payload={source:src,target:dst,enabled,mode:modeTwo?"two-way":"one-way",features:{watchlist:get("watchlist"),ratings:get("ratings"),history:get("history"),playlists:get("playlists")}};
  const eid=wrap.dataset&&wrap.dataset.editingId?String(wrap.dataset.editingId||""):"";if(eid)payload.id=eid;return payload;
}

// Save: REST-first (PUT id → POST), then legacy fallback
async function savePair(payload){
  try{if(payload?.id){const r=await fetch(`/api/pairs/${encodeURIComponent(payload.id)}`,{method:"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});if(r.ok)return{ok:true}}}catch{}
  try{const r=await fetch("/api/pairs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});if(r&&r.ok)return{ok:true}}catch{}
  if(typeof window.cxSavePair==="function"){try{const res=await Promise.resolve(window.cxSavePair(payload,payload.id||""));const ok=typeof res==="object"?res?.ok!==false&&!res?.error:res!==false;return{ok:!!ok}}catch(e){return{ok:false}}}
  return{ok:false}
}

export default{
  async mount(hostEl,props){
    hostEl.innerHTML=tpl(); flowAnimCSSOnce();
    const wrap=ID("cx-modal",hostEl);
    const state=defaultState();
    state.feature="globals";
    wrap.__state=state;

    // hydrate
    let pair=null;
    if(props?.pairOrId && typeof props.pairOrId==="object") pair=props.pairOrId;
    else if(props?.pairOrId) pair=await loadPairById(String(props.pairOrId));

    if(pair && typeof pair==="object"){
      const up=x=>String(x||"").toUpperCase();
      state.src=up(pair.source||pair.src||state.src);
      state.dst=up(pair.target||pair.dst||state.dst);
      state.mode=pair.mode||state.mode;
      state.enabled=typeof pair.enabled==="boolean"?pair.enabled:true;
      const f=pair.features||{}, safe=(v,d)=>Object.assign({},d,v||{});
      state.options.watchlist=safe(f.watchlist,state.options.watchlist);
      state.options.history=safe(f.history,state.options.history);
      state.options.playlists=safe(f.playlists,state.options.playlists);
      const r0=state.options.ratings, rI=f.ratings||{};
      state.options.ratings=Object.assign({},r0,rI,{types:Array.isArray(rI.types)&&rI.types.length?rI.types:r0.types,mode:rI.mode||r0.mode,from_date:rI.from_date||r0.from_date||""});
      wrap.dataset.editingId=pair?.id?String(pair.id):"";
    }

    await loadConfigBits(state);
    await loadProviders(state);

    state.feature="globals";
    renderProviderSelects(state);
    refreshTabs(state);
    updateFlow(state,true);
    renderWarnings(state);
    bindFoldToggles(wrap);
    Q(".cx-body")?.scrollTo?.({top:0,behavior:"instant"});
    restartFlowAnimation(ID("cx-mode-two")?.checked ? "two" : "one");

    ID("cx-enabled").addEventListener("change",()=>updateFlow(state,true));
    QA('input[name="cx-mode"]').forEach(el=>el.addEventListener("change",()=>updateFlow(state,true)));
    bindChangeHandlers(state,wrap);

    ensureInlineFoot(hostEl);
    hostEl.__doSave=async()=>{
      await saveConfigBits(state);
      const payload=buildPayload(state,wrap);
      const res=await savePair(payload);
      if(!res.ok){ alert("Save failed"); return; }

      try{
        if(typeof window.loadPairs==="function"){await window.loadPairs()}
        else{
          const r=await fetch("/api/pairs",{cache:"no-store"});
          const arr=r.ok?await r.json():[]; window.cx=window.cx||{}; window.cx.pairs=Array.isArray(arr)?arr:[];
        }
        document.dispatchEvent(new Event("cx-state-change"));
        window.cxRenderPairsOverlay?.();
        window.renderConnections?.();
        window.updatePreviewVisibility?.();
        window.dispatchEvent?.(new CustomEvent("cx:pairs:changed",{detail:payload}));
        window.cxAfterPairSave?.(payload);
      }catch(e){console.warn("[pair save] refresh failed",e)}
      window.cxCloseModal?.();
    };
  },
  unmount(){}
};
