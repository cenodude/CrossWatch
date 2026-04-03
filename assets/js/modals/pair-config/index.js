/* assets/js/modals/pair-config/index.js */
/* Modal for configuring sync pairs, including provider selection, features, and advanced options. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { createFlowController } from "./flow.js";
import { injectHelpIcons as applyHelpIcons } from "./help.js";
import { createLibraryController } from "./libraries.js";
import { providerLogoHTML, providerToneRgb, sharedFeatureOrder, sharedFeatureLabel } from "./meta.js";
import { ensurePairConfigStyles } from "./styles.js";
import { createTabsController } from "./tabs.js";

const TWO_WAY_WARNING =
  "Two-way sync means both sides can write to each other. It keeps both providers aligned, but it also heavily increases the risk of conflicts, duplicates, overwrites, and deletions. Use with extreme caution.";

// Helpers
const ID=(x,r=document)=>(r.getElementById?r.getElementById(x):r.querySelector("#"+x));
const Q=(s,r=document)=>r.querySelector(s);
const QA=(s,r=document)=>Array.from(r.querySelectorAll(s));
const G=typeof window!=="undefined"?window:globalThis;
const jclone=(o)=>JSON.parse(JSON.stringify(o||{}));

const isEmby = v => same(v, "emby");
function hasEmby(state){ return isEmby(state?.src) || isEmby(state?.dst) }

// Provider helpers
const same=(a,b)=>String(a||"").trim().toLowerCase()===String(b||"").trim().toLowerCase();
const isSimkl=(v)=>same(v,"simkl");
const isJelly=(v)=>same(v,"jellyfin");
const isTrakt=(v)=>same(v,"trakt");
const isMDBList=(v)=>same(v,"mdblist");
const isPlex = (v) => same(v, "plex");
const isCrossWatch = (v) => same(v, "crosswatch");
function hasPlex(state){ return isPlex(state?.src) || isPlex(state?.dst) }
const isMedia = (v) => isPlex(v) || isEmby(v) || isJelly(v);
function providerSupportsProgress(state, name){ return !!(byName(state, name)?.features || {}).progress }
function isProgressPair(state){ return providerSupportsProgress(state, state?.src) && providerSupportsProgress(state, state?.dst) }
function hasSimkl(state){return isSimkl(state?.src)||isSimkl(state?.dst)}
function hasJelly(state){return isJelly(state?.src)||isJelly(state?.dst)}
function hasTrakt(state){return isTrakt(state?.src)||isTrakt(state?.dst)}
function hasMDBList(state){return isMDBList(state?.src)||isMDBList(state?.dst)}

const RATINGS_TYPE_RULES={SIMKL:{disable:["seasons","episodes"]},TMDB:{disable:["seasons"]}};
function ratingsDisabledFor(state){
  const names=[state?.src,state?.dst].map(x=>String(x||"").trim().toUpperCase());
  const out=new Set();
  names.forEach(n=>{const r=RATINGS_TYPE_RULES[n];if(r&&Array.isArray(r.disable))r.disable.forEach(t=>out.add(t))});
  return out;
}
function applyRatingsTypeRules(state){
  const rt=state.options?.ratings||{};
  const dis=ratingsDisabledFor(state);
  const all=["movies","shows","seasons","episodes"];
  all.forEach(t=>{
    const cb=ID("cx-rt-type-"+t);
    if(!cb)return;
    const row=cb.closest(".opt-row");
    if(dis.has(t)){
      cb.checked=false;cb.disabled=true;if(row)row.classList.add("muted");
    }else{
      cb.disabled=false;if(row)row.classList.remove("muted");
    }
  });
  const checked=all.filter(t=>{const cb=ID("cx-rt-type-"+t);return cb&&cb.checked&&!dis.has(t)});
  state.options.ratings=Object.assign({},rt,{types:checked});
  const allOn=all.filter(t=>!dis.has(t)).every(t=>ID("cx-rt-type-"+t)?.checked);
  const allCb=ID("cx-rt-type-all");if(allCb)allCb.checked=!!allOn;
  try{updateRtSummary()}catch{}
}

// Inline footer
function ensureInlineFoot(modal){if(!modal)return;const card=Q(".cx-card",modal)||modal;let bar=card.querySelector(":scope > .cx-actions");if(!bar){bar=document.createElement("div");bar.className="cx-actions";const cancel=document.createElement("button");cancel.className="cx-btn";cancel.textContent="Cancel";cancel.addEventListener("click",()=>G.cxCloseModal?.());const save=document.createElement("button");save.className="cx-btn primary";save.id="cx-inline-save";save.textContent="Save";save.addEventListener("click",async()=>{const b=ID("cx-inline-save");if(!b)return;const old=b.textContent;b.disabled=true;b.textContent="Saving…";try{await modal.__doSave?.()}finally{b.disabled=false;b.textContent=old}});bar.append(cancel,save);card.appendChild(bar)}}

// Ratings summary
function updateRtSummary(){
  const m=ID("cx-modal"),st=m?.__state,rt=st?.options?.ratings||{},det=ID("cx-rt-adv"),sum=det?.querySelector("summary");
  if(!sum)return;
  const types=Array.isArray(rt.types)&&rt.types.length?rt.types.join(", "):"movies, shows, seasons, episodes";
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
            <div class="field endpoint-field endpoint-source">
              <div class="endpoint-card" id="cx-src-card">
                <div class="endpoint-top" id="cx-src-trigger" role="button" tabindex="0" aria-label="Choose source provider">
                  <div class="endpoint-identity">
                    <span class="endpoint-logo" id="cx-src-logo"></span>
                    <div class="endpoint-copy">
                      <div class="endpoint-name" id="cx-src-name">Select source</div>
                      <div class="endpoint-meta" id="cx-src-meta">Choose the system CrossWatch reads from.</div>
                    </div>
                  </div>
                  <div class="endpoint-role">From</div>
                </div>
                <select id="cx-src-inst" name="cx-src-inst" class="input endpoint-profile-select"></select>
              </div>
              <select id="cx-src" name="cx-src" class="input hidden" data-cw-icon-select="off" aria-hidden="true" tabindex="-1"></select>
            </div>
            <div class="field endpoint-field endpoint-target">
              <div class="endpoint-card" id="cx-dst-card">
                <div class="endpoint-top" id="cx-dst-trigger" role="button" tabindex="0" aria-label="Choose target provider">
                  <div class="endpoint-identity">
                    <span class="endpoint-logo" id="cx-dst-logo"></span>
                    <div class="endpoint-copy">
                      <div class="endpoint-name" id="cx-dst-name">Select target</div>
                      <div class="endpoint-meta" id="cx-dst-meta">Choose the system CrossWatch writes to.</div>
                    </div>
                  </div>
                  <div class="endpoint-role">To</div>
                </div>
                <select id="cx-dst-inst" name="cx-dst-inst" class="input endpoint-profile-select"></select>
              </div>
              <select id="cx-dst" name="cx-dst" class="input hidden" data-cw-icon-select="off" aria-hidden="true" tabindex="-1"></select>
            </div>
          </div>
        </div>
        <div class="top-right">
          <div class="flow-card">
            <div class="flow-head">
              <div class="flow-copy">
                <div class="flow-title">Sync flow: <span id="cx-flow-title">One-way</span></div>
                <div id="cx-flow-features" class="flow-feature-dots" aria-label="Enabled connection features"></div>
              </div>
              <div class="flow-mode-inline">
                <div class="seg">
                  <input type="radio" name="cx-mode" id="cx-mode-one" value="one"/><label for="cx-mode-one">One-way</label>
                  <input type="radio" name="cx-mode" id="cx-mode-two" value="two"/><label for="cx-mode-two" title="${TWO_WAY_WARNING}">Two-way</label>
                </div>
              </div>
            </div>
            <div class="flow-rail pretty" id="cx-flow-rail">
              <span class="token" id="cx-flow-src"></span>
              <span class="arrow"><span class="dot flow a"></span><span class="dot flow b"></span></span>
              <span class="token" id="cx-flow-dst"></span>
            </div>
          </div>
          <div id="cx-flow-warn" class="flow-warn-area" aria-live="polite"></div>
        </div>
      </div>
      <div class="cx-tabsrow grid2">
        <div id="cx-feat-tabs" class="feature-tabs" role="tablist" aria-label="Connection sections"></div>
      </div>
      <div class="cx-main grid2">
        <div class="left"><div class="panel" id="cx-feat-panel" role="tabpanel" aria-live="polite"></div></div>
        <div class="right"><div class="panel" id="cx-adv-panel"></div></div>
      </div>
    </div>
  </div>
`;

// State
function defaultState(){
  return {
    providers:[],src:null,dst:null,src_instance:"default",dst_instance:"default",instanceMap:{},feature:"globals",mode:"one-way",enabled:true,
    options:{
      watchlist:{enable:false,add:false,remove:false},
      ratings:{enable:false,add:false,remove:false,types:["movies","shows","seasons","episodes"],mode:"all",from_date:""},
      history:{enable:false,add:false,remove:false},
      playlists:{enable:false,add:true,remove:false},
      progress:{enable:false,add:true,remove:false,min_seconds:60,delta_seconds:30,max_percent:95,propagate_timestamp_updates:false}
    },
    pairProviders:{},
    jellyfin:{watchlist:{mode:"favorites",playlist_name:"Watchlist"}},
    emby:{watchlist:{mode:"favorites",playlist_name:"Watchlist"}},
    globals:{
      dry_run:false,verify_after_write:false,drop_guard:false,allow_mass_delete:true,one_way_remove_mode:"source_deletes",
      tombstone_ttl_days:30,include_observed_deletes:true,
      blackbox:{enabled:true,promote_after:1,unresolved_days:0,cooldown_days:30,pair_scoped:true,block_adds:true,block_removes:true}
    },
    cfgRaw:null,
    visited:new Set()
  }
}

function normalizePairProviders(p){
  const out={};
  if(!p||typeof p!=="object")return out;
  for(const k of Object.keys(p)){
    const key=String(k||"").trim().toLowerCase();
    if(!key)continue;
    const v=p[k];
    if(typeof v==="boolean"){out[key]={strict_id_matching:!!v};continue;}
    if(v&&typeof v==="object"){
      const blk=Object.assign({},v);
      if("strict_id_matching" in blk) blk.strict_id_matching=!!blk.strict_id_matching;
      if(key==="trakt"){
        if("history_collection" in blk) blk.history_collection=!!blk.history_collection;
        if("history_ignore_dropped_shows" in blk) blk.history_ignore_dropped_shows=!!blk.history_ignore_dropped_shows;
        const raw=blk.history_collection_types;
        let types=[];
        if(typeof raw==="string") types=raw.split(",").map(s=>s.trim().toLowerCase()).filter(Boolean);
        else if(Array.isArray(raw)) types=raw.map(x=>String(x).trim().toLowerCase()).filter(Boolean);
        types=types.filter(x=>x==="movies"||x==="shows");
        if(blk.history_collection && !types.length) types=["movies"];
        if(types.length) blk.history_collection_types=types;
        else delete blk.history_collection_types;
      }
      if(key==="mdblist"){
        if("history_ignore_dropped_shows" in blk) blk.history_ignore_dropped_shows=!!blk.history_ignore_dropped_shows;
      }
      if(key==="simkl"){
        if("history_ignore_dropped_shows" in blk) blk.history_ignore_dropped_shows=!!blk.history_ignore_dropped_shows;
      }
      out[key]=blk;
    }
  }
  return out;
}

function defaultStrictIdForProvider(providerKey){
  const key=String(providerKey||"").trim().toLowerCase();
  return key==="plex" || key==="jellyfin" || key==="emby";
}

function getPairProviderStrictValue(state, providerKey){
  const key=String(providerKey||"").trim().toLowerCase();
  const blk=state?.pairProviders?.[key];
  if(blk && typeof blk==="object" && "strict_id_matching" in blk) return !!blk.strict_id_matching;
  return defaultStrictIdForProvider(key);
}

// Data
async function getJSON(url){try{const r=await fetch(url,{cache:"no-store"});return r.ok?await r.json():null}catch{return null}}

async function loadPairById(id){
  try{
    if(!id) return null;

    if(typeof window!=="undefined" && typeof window.loadPairById==="function"){
      try{
        const p=await Promise.resolve(window.loadPairById(id));
        if(p) return p;
      }catch{}
    }

    if(typeof window!=="undefined" && window.cx && Array.isArray(window.cx.pairs)){
      const p=window.cx.pairs.find(x=>String(x?.id||"")===String(id));
      if(p) return p;
    }

    const direct=await getJSON(`/api/pairs/${encodeURIComponent(id)}?cb=${Date.now()}`);
    if(direct && typeof direct==="object" && (direct.id||direct.source||direct.target)) return direct;

    const list=await getJSON(`/api/pairs?cb=${Date.now()}`);
    if(Array.isArray(list)){
      const p=list.find(x=>String(x?.id||"")===String(id));
      if(p) return p;
    }
  }catch{}
  return null;
}

async function loadProviderInstances(state){
  try{
    const r=await fetch("/api/provider-instances",{cache:"no-store"});
    const j=r.ok?await r.json():{};
    state.instanceMap=(j&&typeof j==="object")?j:{};
  }catch{state.instanceMap={}}
}

async function loadProviders(state){
  const list=await getJSON("/api/sync/providers?cb="+Date.now());
  state.providers=Array.isArray(list)?list:[
    {name:"PLEX",label:"Plex",features:{watchlist:true,ratings:true,history:true,progress:true,playlists:true},capabilities:{bidirectional:true},version:"1.0.0"},
    {name:"CROSSWATCH",label:"CW Tracker",features:{watchlist:true,ratings:true,history:true,progress:true,playlists:false},capabilities:{bidirectional:true},version:"1.0.0"},
    {name:"SIMKL",label:"Simkl",features:{watchlist:true,ratings:true,history:true,progress:false,playlists:false},capabilities:{bidirectional:true},version:"1.0.0"},
    {name:"TRAKT",label:"Trakt",features:{watchlist:true,ratings:true,history:true,progress:false,playlists:true},capabilities:{bidirectional:true},version:"1.0.0"},
    {name:"JELLYFIN",label:"Jellyfin",features:{watchlist:true,ratings:false,history:true,progress:true,playlists:true},capabilities:{bidirectional:true},version:"1.2.1"},
    {name:"EMBY",label:"Emby",features:{watchlist:true,ratings:false,history:true,progress:true,playlists:true},capabilities:{bidirectional:true},version:"1.0.0"} 
  ]
}

async function loadConfigBits(state){
  const cfg=(await getJSON("/api/config?cb="+Date.now()))||{}, s=cfg?.sync||{};
  state.cfgRaw=cfg||{};
  const dropOn=!!s.drop_guard;
  const massOn=!!s.allow_mass_delete && !dropOn;
  state.globals={
    dry_run:!!s.dry_run,
    verify_after_write:!!s.verify_after_write,
    drop_guard:dropOn,
    allow_mass_delete:massOn,
    one_way_remove_mode:(String(s.one_way_remove_mode||"source_deletes").trim().toLowerCase()==="mirror"?"mirror":"source_deletes"),
    tombstone_ttl_days:Number.isFinite(s.tombstone_ttl_days)?s.tombstone_ttl_days:30,
    include_observed_deletes:!!s.include_observed_deletes,
    blackbox:Object.assign(
      {enabled:true,promote_after:1,unresolved_days:0,cooldown_days:30,pair_scoped:true,block_adds:true,block_removes:true},
      s.blackbox||{}
    ),
    runtime:Object.assign(
      {suspect_min_prev:20,suspect_shrink_ratio:0.1},
      s.runtime||{}
    )
  };

  const jf = cfg?.jellyfin?.watchlist || {};
  const em = cfg?.emby?.watchlist || {};

  const modeJF = (jf.mode==="playlist"||jf.mode==="favorites"||jf.mode==="collection"||jf.mode==="collections") ? jf.mode : "favorites";
  const modeEM = (em.mode==="playlist"||em.mode==="favorites"||em.mode==="collection"||em.mode==="collections") ? em.mode : "favorites";

  state.jellyfin.watchlist.mode = (modeJF==="collections") ? "collection" : modeJF;
  state.jellyfin.watchlist.playlist_name = jf.playlist_name || "Watchlist";

  state.emby.watchlist.mode = (modeEM==="collections") ? "collection" : modeEM;
  state.emby.watchlist.playlist_name = em.playlist_name || "Watchlist";
}

// UI utils
const byName=(state,n)=>state.providers.find(p=>p.name===n);
const commonFeatures=(state)=>{
  if(!state.src||!state.dst) return [];
  const a=byName(state,state.src)?.features||{};
  const b=byName(state,state.dst)?.features||{};
  const keys=["watchlist","ratings","history","progress","playlists"];
  return keys.filter(k=>{
    if(k==="progress") return isProgressPair(state) && !!a.progress && !!b.progress;
    return !!a[k] && !!b[k];
  });
};
const defaultFor=(k)=>
  k==="watchlist"?{enable:false,add:false,remove:false}:
  k==="playlists"?{enable:false,add:true,remove:false}:
  k==="progress"?{enable:false,add:true,remove:false,min_seconds:60,delta_seconds:30,max_percent:95,propagate_timestamp_updates:false}:
  {enable:false,add:false,remove:false};
function getOpts(state,key){
  if(!state.visited.has(key)){
    if(key==="ratings") state.options.ratings=Object.assign({enable:false,add:false,remove:false,types:["movies","shows","seasons","episodes"],mode:"all",from_date:""},state.options.ratings||{});
    else if(key==="progress") state.options.progress=Object.assign({enable:false,add:true,remove:false,min_seconds:60,delta_seconds:30,max_percent:95,propagate_timestamp_updates:false},state.options.progress||{});
    else state.options[key]=state.options[key]??defaultFor(key);
    state.visited.add(key);
  }
  return state.options[key];
}

 

/* Legacy library UI moved to ./libraries.js
function initPairLibraryUI(state){
  if(!state._libsAutoload) state._libsAutoload = {};
  const hasPL=hasPlex(state);
  const hasJF=hasJelly(state);
  const hasEM=hasEmby(state);
  const plBox=ID("plx-pair-libs");
  const jfBox=ID("jf-pair-libs");
  const emBox=ID("em-pair-libs");
  if(plBox) plBox.style.display=hasPL?"":"none";
  if(jfBox) jfBox.style.display=hasJF?"":"none";
  if(emBox) emBox.style.display=hasEM?"":"none";

  if(hasPL){
    const btn=ID("plx-libs-load");
    const load=()=>{
      if(btn){btn.disabled=true;btn.textContent="Loading…";}
      Promise.all([
        fetchPairLibraries("PLEX","history").then(libs=>{
          renderPairLibChips(state,"PLEX","history",libs);
        }),
        fetchPairLibraries("PLEX","ratings").then(libs=>{
          renderPairLibChips(state,"PLEX","ratings",libs);
        })
      ]).finally(()=>{
        if(btn){btn.disabled=false;btn.textContent="Load libraries";}
      });
    };
    
    if(btn&&!btn.__wired){
      btn.__wired=true;
      btn.addEventListener("click",load);
    }
    if(!state._libsAutoload.PLEX){ state._libsAutoload.PLEX=true; load(); }
  }else{
    const btn=ID("plx-libs-load");
    if(btn) btn.disabled=true;
  }

  if(hasJF){
    const btn=ID("jf-libs-load");
    const load=()=>{
      if(btn){btn.disabled=true;btn.textContent="Loading…";}
      Promise.all([
        fetchPairLibraries("JELLYFIN","history").then(libs=>{
          renderPairLibChips(state,"JELLYFIN","history",libs);
        }),
        fetchPairLibraries("JELLYFIN","ratings").then(libs=>{
          renderPairLibChips(state,"JELLYFIN","ratings",libs);
        })
      ]).finally(()=>{
        if(btn){btn.disabled=false;btn.textContent="Load libraries";}
      });
    };

    if(btn&&!btn.__wired){
      btn.__wired=true;
      btn.addEventListener("click",load);
    }
    if(!state._libsAutoload.JELLYFIN){ state._libsAutoload.JELLYFIN=true; load(); }
  }else{
    const btn=ID("jf-libs-load");
    if(btn) btn.disabled=true;
  }

  if(hasEM){
    const btn=ID("em-libs-load");
    const load=()=>{
      if(btn){btn.disabled=true;btn.textContent="Loading…";}
      Promise.all([
        fetchPairLibraries("EMBY","history").then(libs=>{
          renderPairLibChips(state,"EMBY","history",libs);
        }),
        fetchPairLibraries("EMBY","ratings").then(libs=>{
          renderPairLibChips(state,"EMBY","ratings",libs);
        })
      ]).finally(()=>{
        if(btn){btn.disabled=false;btn.textContent="Load libraries";}
      });
    };

    if(btn&&!btn.__wired){
      btn.__wired=true;
      btn.addEventListener("click",load);
    }
    if(!state._libsAutoload.EMBY){ state._libsAutoload.EMBY=true; load(); }
  }else{
    const btn=ID("em-libs-load");
    if(btn) btn.disabled=true;
  }
}
*/

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

const {
  restartFlowAnimation,
  applyFlowTheme,
  updateFlow,
  updateFlowClasses,
} = createFlowController({ ID, Q, byName, renderWarnings });

const { refreshTabs } = createTabsController({
  ID,
  sharedFeatureOrder,
  sharedFeatureLabel,
  commonFeatures,
  renderFeaturePanel,
  applyFlowTheme,
  renderWarnings,
  restartFlowAnimation,
  injectHelpIcons: (root) => applyHelpIcons(root, { QA }),
});

const {
  initPairLibraryUI: initPairLibraryUIController,
  invalidatePairServerCfg: invalidatePairServerCfgController,
} = createLibraryController({
  ID,
  hasPlex,
  hasJelly,
  hasEmby,
  getOpts,
  onLibrariesChanged: (state) => refreshProviderCardSummaries(state),
});

function renderProviderSelects(state){
  const srcSel=ID("cx-src"),dstSel=ID("cx-dst");
  const opts=state.providers.map(p=>`<option value="${p.name}">${p.label}</option>`).join("");
  srcSel.innerHTML=`<option value="">Select…</option>${opts}`;dstSel.innerHTML=`<option value="">Select…</option>${opts}`;
  if(state.src) srcSel.value=state.src;if(state.dst) dstSel.value=state.dst;
  const providerKind=(name)=>{
    if(isMedia(name)) return "Media server";
    if(isCrossWatch(name)) return "Tracker";
    if(isTrakt(name)||isSimkl(name)||same(name,"mdblist")||same(name,"tautulli")) return "Tracker";
    if(same(name,"tmdb")||same(name,"anilist")) return "Metadata";
    return "Provider";
  };
  const openPicker=(sel)=>{
    if(!sel) return;
    try{
      sel.focus({preventScroll:true});
      if(typeof sel.showPicker==="function") sel.showPicker();
      else{
        try{sel.dispatchEvent(new MouseEvent("mousedown",{bubbles:true}))}catch{}
        sel.click();
      }
    }catch{}
  };
  const bindCardTrigger=(trigger,sel)=>{
    if(!trigger||!sel) return;
    trigger.onclick=(e)=>{e.preventDefault();openPicker(sel)};
    trigger.onkeydown=(e)=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();openPicker(sel)}};
  };
  const setEndpointCard=(side,prov)=>{
    const card=ID(`cx-${side}-card`),logo=ID(`cx-${side}-logo`),name=ID(`cx-${side}-name`),meta=ID(`cx-${side}-meta`),trigger=ID(`cx-${side}-trigger`);
    if(!card)return;
    const key=String(prov?.name||"").trim().toUpperCase();
    const rgb=providerToneRgb(key,side==="src"?"87,160,255":"167,139,250");
    card.style.setProperty("--endpoint-rgb",rgb);
    card.style.setProperty("--endpoint-watermark", prov ? `url("${window.CW?.ProviderMeta?.logoPath?.(key) || `/assets/img/${key}.svg`}")` : "none");
    card.classList.toggle("is-selected",!!prov);
    if(logo) logo.innerHTML=prov?providerLogoHTML(prov.name,prov.label):`<span class="prov-wrap"><span class="prov-fallback">?</span></span>`;
    if(name) name.textContent=prov?.label||(side==="src"?"Select source":"Select target");
    if(meta) meta.innerHTML=prov?`<span class="endpoint-meta-type">${providerKind(prov.name)}</span><span class="endpoint-meta-cap">${prov.capabilities?.bidirectional?"Bidirectional capable":"Single direction"}</span>`:(side==="src"?"Choose the system CrossWatch reads from.":"Choose the system CrossWatch writes to.");
    if(trigger) trigger.setAttribute("aria-label", `${side==="src"?"Choose source":"Choose target"} provider${prov?.label?`: ${prov.label}`:""}`);
  };
  const upd=()=>{
    const s=byName(state,srcSel.value),d=byName(state,dstSel.value);
    setEndpointCard("src",s);
    setEndpointCard("dst",d);
  };
  bindCardTrigger(ID("cx-src-trigger"),srcSel);
  bindCardTrigger(ID("cx-dst-trigger"),dstSel);
  srcSel.onchange=()=>{state.src=srcSel.value||null;upd();renderInstanceSelects(state);updateFlow(state,true);refreshTabs(state);renderWarnings(state)};
  dstSel.onchange=()=>{state.dst=dstSel.value||null;upd();renderInstanceSelects(state);updateFlow(state,true);refreshTabs(state);renderWarnings(state)};
  upd();renderInstanceSelects(state);ID("cx-mode-two").checked=state.mode==="two-way";ID("cx-mode-one").checked=!ID("cx-mode-two").checked;ID("cx-enabled").checked=!!state.enabled;
}

function renderInstanceSelects(state){
  const srcInstSel=ID("cx-src-inst"),dstInstSel=ID("cx-dst-inst");
  if(!srcInstSel||!dstInstSel) return;
  const map=state.instanceMap||{};
  const norm=(v)=>{const s=String(v||"default").trim();return (!s||s.toLowerCase()==="default")?"default":s};
  const optsFor=(prov)=>{
    const key=String(prov||"").toUpperCase();
    const raw=map[key]||map[String(prov||"").toLowerCase()]||[];
    const arr=Array.isArray(raw)?raw:[];
    const ids=[];
    for(const x of arr){
      if(typeof x==="string") ids.push(x);
      else if(x&&typeof x==="object"&&x.id) ids.push(String(x.id));
    }
    const uniq=["default",...ids.map(norm).filter(x=>x&&x!=="default")];
    return [...new Set(uniq)];
  };
  const fill=(sel, prov, cur)=>{
    const ids=optsFor(prov);
    sel.innerHTML=ids.map(id=>`<option value="${id}">${id==="default"?"Default":id}</option>`).join("");
    const want=norm(cur);
    sel.value=ids.includes(want)?want:"default";
    sel.disabled=!prov;
  };

  fill(srcInstSel, state.src, state.src_instance);
  fill(dstInstSel, state.dst, state.dst_instance);
  state.src_instance=norm(srcInstSel.value);
  state.dst_instance=norm(dstInstSel.value);

  srcInstSel.onchange=()=>{state.src_instance=norm(srcInstSel.value)};
  dstInstSel.onchange=()=>{state.dst_instance=norm(dstInstSel.value)};
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
    det.classList.toggle("open", det.open);
  });
  root.addEventListener("keydown",(e)=>{
    const sum=e.target.closest?.("summary.fold-head, .fold > summary");
    if(!sum)return;
    if(e.key===" "||e.key==="Enter"){
      const det=sum.closest("details"); if(!det)return;
      e.preventDefault(); e.stopPropagation();
      det.open=!det.open;
      det.classList.toggle("open", det.open);
    }
  });

}

function applySubDisable(feature){
  const map={
    watchlist: [
      "#cx-wl-add","#cx-wl-remove",
      "#plx-wl-pms","#plx-wl-limit","#plx-wl-delay","#plx-wl-title","#plx-wl-meta","#plx-wl-guid",
      "#cx-jf-wl-mode-fav","#cx-jf-wl-mode-pl","#cx-jf-wl-mode-col","#cx-jf-wl-pl-name",
      "#cx-em-wl-mode-fav","#cx-em-wl-mode-pl","#cx-em-wl-mode-col","#cx-em-wl-pl-name",
      "#cx-wl-q","#cx-wl-delay","#cx-wl-guid",
      "#tr-wl-etag","#tr-wl-ttl","#tr-wl-batch","#tr-wl-log","#tr-wl-freeze"
    ],
    ratings: [
      "#cx-rt-add","#cx-rt-remove","#cx-rt-type-all","#cx-rt-type-movies","#cx-rt-type-shows","#cx-rt-type-seasons","#cx-rt-type-episodes","#cx-rt-mode","#cx-rt-from-date",
      "#tr-rt-perpage","#tr-rt-maxpages","#tr-rt-chunk"
    ],
    history: ["#cx-hs-add", "#cx-hs-remove", "#cx-tr-hs-numfb", "#cx-tr-hs-col", "#cx-tr-hs-col-movies", "#cx-tr-hs-col-shows", "#cx-tr-hs-ignore-dropped", "#cx-md-hs-ignore-dropped", "#cx-sm-hs-ignore-dropped", "#cx-tr-hs-unres"],
    playlists:["#cx-pl-add","#cx-pl-remove"],
    progress:["#cx-pr-add","#cx-pr-remove","#cx-pr-min","#cx-pr-delta","#cx-pr-maxp"]
  };
  const on=ID(feature==="ratings"?"cx-rt-enable":feature==="watchlist"?"cx-wl-enable":feature==="history"?"cx-hs-enable":feature==="progress"?"cx-pr-enable":"cx-pl-enable")?.checked;
  (map[feature]||[]).forEach(sel=>{const n=Q(sel);if(n){n.disabled=!on;n.closest?.(".opt-row")?.classList.toggle("muted",!on)}});
}

function countProviderLibraries(state, providerName){
  const historyLibs = state.options?.history?.libraries?.[providerName];
  const ratingsLibs = state.options?.ratings?.libraries?.[providerName];
  const historyCount = Array.isArray(historyLibs) ? historyLibs.length : 0;
  const ratingsCount = Array.isArray(ratingsLibs) ? ratingsLibs.length : 0;
  return historyCount + ratingsCount;
}

function getProviderOverrideCount(state, providerKey){
  const cfg = state.cfgRaw || {};
  const pp = state.pairProviders || {};
  const num = (id, fallback, min = 0) => {
    const el = ID(id);
    if (!el) return fallback;
    const v = parseInt(String(el.value || "").trim(), 10);
    return Number.isFinite(v) ? Math.max(min, v) : fallback;
  };
  const checked = (id, fallback = false) => {
    const el = ID(id);
    return el ? !!el.checked : fallback;
  };

  if (providerKey === "plex") {
    const plex = cfg.plex || {};
    const hist = plex.history || {};
    const strictDefault = defaultStrictIdForProvider("plex");
    let count = 0;
    if (num("plx-rating-workers", plex.rating_workers ?? 12, 1) !== 12) count++;
    if (num("plx-history-workers", plex.history_workers ?? 12, 1) !== 12) count++;
    if (num("plx-timeout", Number.isFinite(plex.timeout) ? plex.timeout : 10, 1) !== 10) count++;
    if (num("plx-retries", Number.isFinite(plex.max_retries) ? plex.max_retries : 3, 0) !== 3) count++;
    if (checked("plx-fallback-guid", !!plex.fallback_GUID)) count++;
    if (checked("plx-strict-ids", getPairProviderStrictValue(state, "plex")) !== strictDefault) count++;
    if (checked("plx-marked-watched", hist.include_marked_watched ?? true) !== true) count++;
    return count + countProviderLibraries(state, "PLEX");
  }
  if (providerKey === "jellyfin") {
    const jf = cfg.jellyfin || {};
    const strictDefault = defaultStrictIdForProvider("jellyfin");
    let count = 0;
    if (num("jf-timeout", Number.isFinite(jf.timeout) ? jf.timeout : 15, 1) !== 15) count++;
    if (num("jf-retries", Number.isFinite(jf.max_retries) ? jf.max_retries : 3, 0) !== 3) count++;
    if (checked("jf-strict-ids", getPairProviderStrictValue(state, "jellyfin")) !== strictDefault) count++;
    return count + countProviderLibraries(state, "JELLYFIN");
  }
  if (providerKey === "emby") {
    const em = cfg.emby || {};
    const strictDefault = defaultStrictIdForProvider("emby");
    let count = 0;
    if (num("em-timeout", Number.isFinite(em.timeout) ? em.timeout : 15, 1) !== 15) count++;
    if (num("em-retries", Number.isFinite(em.max_retries) ? em.max_retries : 3, 0) !== 3) count++;
    if (checked("em-strict-ids", getPairProviderStrictValue(state, "emby")) !== strictDefault) count++;
    return count + countProviderLibraries(state, "EMBY");
  }
  return 0;
}

function getProviderSummaryText(state, providerKey){
  const overrides = getProviderOverrideCount(state, providerKey);
  return overrides
    ? `${overrides} custom ${overrides === 1 ? "override" : "overrides"} active`
    : "Defaults are good for most users";
}

function refreshProviderCardSummaries(state){
  [["plex","prov-plex-summary"],["jellyfin","prov-jelly-summary"],["emby","prov-emby-summary"]].forEach(([key, id]) => {
    const el = ID(id);
    if (el) el.textContent = getProviderSummaryText(state, key);
  });
}

function renderFeaturePanel(state){
  if(state.feature!=="providers"){ ID("cx-prov-warn")?.remove(); }
  const left=ID("cx-feat-panel"),right=ID("cx-adv-panel");if(!left||!right)return;

  const leftWrap = Q(".cx-main .left");
  const rightWrap = Q(".cx-main .right");
  if (leftWrap) leftWrap.style.gridColumn = "";
  if (rightWrap) rightWrap.style.display = "";

  if(state.feature==="providers"){
    const cfg=state.cfgRaw||{};
    const plex=cfg.plex||{};
    const hist=plex.history||{};
    const jf=cfg.jellyfin||{};
    const em=cfg.emby||{};
    if (leftWrap) leftWrap.style.gridColumn = "1 / -1";
    if (rightWrap) rightWrap.style.display = "none";

    const providerHintText = (providerKey) => {
      if (providerKey === "plex") return "Tune Plex matching, retries, workers, and pair library scope";
      if (providerKey === "jellyfin") return "Tune Jellyfin matching, longer timeouts and pair library scope.";
      return "Tune Emby matching, longer timeouts and pair-level library scope.";
    };

    const providerCards = [];
    if (hasPlex(state)) providerCards.push(`
      <details class="mods fold provider-card provider-plex" id="prov-plex">
        <summary class="fold-head provider-card-head">
          <span class="provider-card-main">
            <span class="provider-card-badge">Plex</span>
            <span class="provider-card-copy">
              <span class="provider-card-title">Plex</span>
              <span class="provider-card-sub" id="prov-plex-summary">${getProviderSummaryText(state, "plex")}</span>
            </span>
          </span>
          <span class="provider-card-meta">
            <span class="provider-card-hint">${providerHintText("plex")}</span>
            <span class="chev">expand_more</span>
          </span>
        </summary>
        <div class="fold-body provider-card-body">
          <div class="grid2 compact" style="padding:8px 0 2px">
            <div class="opt-row"><label for="plx-rating-workers">Rating workers</label><input id="plx-rating-workers" class="input small" type="number" min="1" max="64" step="1" value="${plex.rating_workers??12}"></div>
            <div class="opt-row"><label for="plx-history-workers">History workers</label><input id="plx-history-workers" class="input small" type="number" min="1" max="64" step="1" value="${plex.history_workers??12}"></div>
            <div class="opt-row"><label for="plx-timeout">Timeout (s)</label><input id="plx-timeout" class="input small" type="number" min="1" max="120" step="1" value="${Number.isFinite(plex.timeout)?plex.timeout:10}"></div>
            <div class="opt-row"><label for="plx-retries">Max retries</label><input id="plx-retries" class="input small" type="number" min="0" max="10" step="1" value="${Number.isFinite(plex.max_retries)?plex.max_retries:3}"></div>
            <div class="opt-row"><label for="plx-fallback-guid">Fallback GUID</label><label class="switch"><input id="plx-fallback-guid" type="checkbox" ${plex.fallback_GUID?"checked":""}><span class="slider"></span></label></div><div class="opt-row"><label for="plx-strict-ids">Strict ID matching</label><label class="switch"><input id="plx-strict-ids" type="checkbox" ${getPairProviderStrictValue(state, "plex")?"checked":""}><span class="slider"></span></label></div><div class="opt-row"><label for="plx-marked-watched">Marked Watched</label><label class="switch"><input id="plx-marked-watched" type="checkbox" ${(hist.include_marked_watched??false)?"checked":""}><span class="slider"></span></label></div>
          </div>
          <div class="prov-box" id="plx-pair-libs">
            <div class="panel-title small">Pair library whitelist</div>
            <div class="muted">Empty = use server-level whitelist.</div>
            <div class="opt-row">
              <div class="field-label">History</div>
              <div class="chip-row" id="plx-hist-libs"></div>
            </div>
            <div class="opt-row">
              <div class="field-label">Ratings</div>
              <div class="chip-row" id="plx-rate-libs"></div>
            </div>
            <button type="button" class="cx-btn small" id="plx-libs-load">Load libraries</button>
          </div>
        </div>
      </details>`);

    if (hasJelly(state)) providerCards.push(`
      <details class="mods fold provider-card provider-jellyfin" id="prov-jelly">
        <summary class="fold-head provider-card-head">
          <span class="provider-card-main">
            <span class="provider-card-badge">Jellyfin</span>
            <span class="provider-card-copy">
              <span class="provider-card-title">Jellyfin</span>
              <span class="provider-card-sub" id="prov-jelly-summary">${getProviderSummaryText(state, "jellyfin")}</span>
            </span>
          </span>
          <span class="provider-card-meta">
            <span class="provider-card-hint">${providerHintText("jellyfin")}</span>
            <span class="chev">expand_more</span>
          </span>
        </summary>
        <div class="fold-body provider-card-body">
          <div class="grid2 compact" style="padding:8px 0 2px">
            <div class="opt-row"><label for="jf-timeout">Timeout (s)</label><input id="jf-timeout" class="input small" type="number" min="1" max="120" step="1" value="${Number.isFinite(jf.timeout)?jf.timeout:15}"></div>
            <div class="opt-row"><label for="jf-retries">Max retries</label><input id="jf-retries" class="input small" type="number" min="0" max="10" step="1" value="${Number.isFinite(jf.max_retries)?jf.max_retries:3}"></div>
            <div class="opt-row"><label for="jf-strict-ids">Strict ID matching</label><label class="switch"><input id="jf-strict-ids" type="checkbox" ${getPairProviderStrictValue(state, "jellyfin")?"checked":""}><span class="slider"></span></label></div>
          </div>
          <div class="prov-box" id="jf-pair-libs">
            <div class="panel-title small">Pair library whitelist</div>
            <div class="muted">Empty = use server-level whitelist.</div>
            <div class="opt-row">
              <div class="field-label">History</div>
              <div class="chip-row" id="jf-hist-libs"></div>
            </div>
            <div class="opt-row">
              <div class="field-label">Ratings</div>
              <div class="chip-row" id="jf-rate-libs"></div>
            </div>
            <button type="button" class="cx-btn small" id="jf-libs-load">Load libraries</button>
          </div>
        </div>
      </details>`);

    if (hasEmby(state)) providerCards.push(`
      <details class="mods fold provider-card provider-emby" id="prov-emby">
        <summary class="fold-head provider-card-head">
          <span class="provider-card-main">
            <span class="provider-card-badge">Emby</span>
            <span class="provider-card-copy">
              <span class="provider-card-title">Emby</span>
              <span class="provider-card-sub" id="prov-emby-summary">${getProviderSummaryText(state, "emby")}</span>
            </span>
          </span>
          <span class="provider-card-meta">
            <span class="provider-card-hint">${providerHintText("emby")}</span>
            <span class="chev">expand_more</span>
          </span>
        </summary>
        <div class="fold-body provider-card-body">
          <div class="grid2 compact" style="padding:8px 0 2px">
            <div class="opt-row"><label for="em-timeout">Timeout (s)</label><input id="em-timeout" class="input small" type="number" min="1" max="120" step="1" value="${Number.isFinite(em.timeout)?em.timeout:15}"></div>
            <div class="opt-row"><label for="em-retries">Max retries</label><input id="em-retries" class="input small" type="number" min="0" max="10" step="1" value="${Number.isFinite(em.max_retries)?em.max_retries:3}"></div>
            <div class="opt-row"><label for="em-strict-ids">Strict ID matching</label><label class="switch"><input id="em-strict-ids" type="checkbox" ${getPairProviderStrictValue(state, "emby")?"checked":""}><span class="slider"></span></label></div>
          </div>
          <div class="prov-box" id="em-pair-libs">
            <div class="panel-title small">Pair library whitelist</div>
            <div class="muted">Empty = use server-level whitelist.</div>
            <div class="opt-row">
              <div class="field-label">History</div>
              <div class="chip-row" id="em-hist-libs"></div>
            </div>
            <div class="opt-row">
              <div class="field-label">Ratings</div>
              <div class="chip-row" id="em-rate-libs"></div>
            </div>
            <button type="button" class="cx-btn small" id="em-libs-load">Load libraries</button>
          </div>
        </div>
      </details>`);

    left.innerHTML=`<div class="panel-title"><span class="material-symbols-rounded" style="vertical-align:-3px;margin-right:6px;">dns</span>Media Servers</div>
      <div class="providers-intro">
        <div class="providers-intro-copy">
          <div class="providers-intro-title">Advanced provider tuning</div>
          <div class="providers-intro-sub">Open a provider only if you want stricter matching, different retry behavior, or pair-specific library scope.</div>
        </div>
        <div class="providers-intro-badge">${providerCards.length} ${providerCards.length === 1 ? "provider" : "providers"} in this pair</div>
      </div>
      <div class="provider-card-list">${providerCards.length ? providerCards.join("") : `<div class="providers-empty">This connection does not include a media server with provider-specific controls.</div>`}</div>
      <div class="providers-note" role="note" aria-live="polite">
        <div class="providers-note-title"><span class="material-symbols-rounded">info</span>Optional advanced controls</div>
        <div class="providers-note-body">Library whitelists only appear for media servers that are actually part of this pair.</div>
      </div>`;

    right.innerHTML = "";
    initPairLibraryUIController(state);
    QA(".fold").forEach(f=>{f.classList.remove("open")});
    return;
  }

  if(state.feature==="globals"){
    const g=state.globals||{},bb=g.blackbox||{},rt=g.runtime||{suspect_min_prev:20,suspect_shrink_ratio:0.1};
    const pct = Math.round((Number.isFinite(rt.suspect_shrink_ratio)?rt.suspect_shrink_ratio:0.1)*100);
    const minPrevVal = Number.isFinite(rt.suspect_min_prev)?rt.suspect_min_prev:20;

    left.innerHTML=`<div class="panel-title"><span class="material-symbols-rounded" style="vertical-align:-3px;margin-right:6px;">tune</span>Globals <button type="button" class="cx-help material-symbols-rounded" data-tip-id="gl-section-main">help</button></div>
      <div class="opt-row"><label for="gl-dry">Dry run</label><label class="switch"><input id="gl-dry" type="checkbox" ${g.dry_run?"checked":""}><span class="slider"></span></label></div>
      <div class="opt-row"><label for="gl-verify">Verify after write</label><label class="switch"><input id="gl-verify" type="checkbox" ${g.verify_after_write?"checked":""}><span class="slider"></span></label></div>
      <div class="opt-row"><label for="gl-drop">Drop guard</label><label class="switch"><input id="gl-drop" type="checkbox" ${g.drop_guard?"checked":""}><span class="slider"></span></label></div>
      <div id="gl-drop-adv" class="prov-box" style="margin:8px 0 4px; ${g.drop_guard?"":"opacity:.5;pointer-events:none;"}">
        <div class="panel-title small">Suspect guard (shrinking inventories)</div>
        <div class="grid2 compact">
          <div class="opt-row">
            <label for="gl-sus-min">Min</label>
            <div style="position:relative;width:100%">
              <input id="gl-sus-min" type="range" min="0" max="200" step="1" value="${minPrevVal}" style="width:100%">
              <span id="gl-sus-min-val" style="position:absolute;right:6px;top:-6px;font-size:12px;opacity:.8;">${minPrevVal}</span>
            </div>
          </div>
          <div class="opt-row">
            <label for="gl-sus-pct-range">Shrink(%)</label>
            <div style="position:relative;width:100%">
              <input id="gl-sus-pct-range" type="range" min="1" max="50" step="1" value="${pct}" style="width:100%">
              <span id="gl-sus-pct-val" style="position:absolute;right:6px;top:-6px;font-size:12px;opacity:.8;">${pct}</span>
            </div>
          </div>
        </div>
      </div>
      <div class="grid2 compact">
        <div class="opt-row"><label for="gl-mass">Allow mass delete</label><label class="switch"><input id="gl-mass" type="checkbox" ${g.allow_mass_delete?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="gl-oneway-remove">One-Way Remove mode Source</label><label class="switch"><input id="gl-oneway-remove" type="checkbox" ${((String(g.one_way_remove_mode||"source_deletes").trim().toLowerCase()==="mirror")?"":"checked")}><span class="slider"></span></label></div>
      </div>`;
    right.innerHTML=`<div class="panel-title">Advanced <button type="button" class="cx-help material-symbols-rounded" data-tip-id="gl-section-advanced">help</button></div>
      <div class="opt-row"><label for="gl-ttl">Tombstone TTL (days)</label><input id="gl-ttl" class="input" type="number" min="0" step="1" value="${g.tombstone_ttl_days??30}"></div><div class="muted">Keep delete markers to avoid re-adding.</div>
      <div class="opt-row"><label for="gl-observed">Include observed deletes</label><label class="switch"><input id="gl-observed" type="checkbox" ${g.include_observed_deletes?"checked":""}><span class="slider"></span></label></div>
      <div class="panel-title small" style="margin-top:10px">Blackbox</div>
      <div class="grid2 compact">
        <div class="opt-row"><label for="gl-bb-enable">Enabled</label><label class="switch"><input id="gl-bb-enable" type="checkbox" ${bb.enabled?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="gl-bb-pair">Pair scoped</label><label class="switch"><input id="gl-bb-pair" type="checkbox" ${bb.pair_scoped?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="gl-bb-promote">Promote after (days)</label><input id="gl-bb-promote" class="input small" type="number" min="0" max="365" step="1" value="${bb.promote_after??1}"></div>
        <div class="opt-row"><label for="gl-bb-unresolved">Unresolved days</label><input id="gl-bb-unresolved" class="input small" type="number" min="0" max="365" step="1" value="${bb.unresolved_days??0}"></div>
        <div class="opt-row"><label for="gl-bb-cooldown">Cooldown days</label><input id="gl-bb-cooldown" class="input small" type="number" min="0" max="365" step="1" value="${bb.cooldown_days??30}"></div>
      </div>`;
    return;
  }

  if (state.feature === "watchlist") {
    const wl = getOpts(state, "watchlist");
    const emw = state.emby?.watchlist || { mode: "favorites", playlist_name: "Watchlist" };
    const jfw = state.jellyfin?.watchlist || { mode: "favorites", playlist_name: "Watchlist" };
    const trPair = (state.pairProviders?.trakt) || {};

    left.innerHTML = `
      <div class="panel-title">Watchlist | Basics</div>
      <div class="opt-row">
        <label for="cx-wl-enable">Enable</label>
        <label class="switch"><input id="cx-wl-enable" type="checkbox" ${wl.enable?"checked":""}><span class="slider"></span></label>
      </div>
      <div class="grid2">
        <div class="opt-row"><label for="cx-wl-add">Add</label><label class="switch"><input id="cx-wl-add" type="checkbox" ${wl.add?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-wl-remove">Remove</label><label class="switch"><input id="cx-wl-remove" type="checkbox" ${wl.remove?"checked":""}><span class="slider"></span></label></div>
      </div>

      ${hasJelly(state)?`
        <div class="panel-title small" style="margin-top:6px">Jellyfin specifics</div>
        <div class="muted" style="margin:-2px 0 10px;display:flex;align-items:center;justify-content:space-between;gap:8px;">
          <span>Favorites / Playlist / Collections</span>
          <button type="button" class="cx-help material-symbols-rounded" data-tip-id="cx-jf-wl-mode">help</button>
        </div>
        <div class="grid2 compact">
          <div class="opt-row" style="grid-column:1/-1">
            <div class="field-label">Mode</div>
            <div class="seg">
              <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-fav" value="favorites" ${jfw.mode==="favorites"?"checked":""}/><label for="cx-jf-wl-mode-fav">Favorites</label>
              <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-pl"  value="playlist"  ${jfw.mode==="playlist"?"checked":""}/><label for="cx-jf-wl-mode-pl">Playlist</label>
              <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-col" value="collection" ${(jfw.mode==="collection")?"checked":""}/><label for="cx-jf-wl-mode-col">Collections</label>
            </div>
          </div>
          <div class="opt-row" style="grid-column:1/-1">
            <label for="cx-jf-wl-pl-name">Name</label>
            <input id="cx-jf-wl-pl-name" class="input" type="text" value="${jfw.playlist_name||"Watchlist"}" placeholder="Watchlist">
          </div>
        </div>
      `:""}

      ${hasEmby(state)?`
        <div class="panel-title small" style="margin-top:6px">Emby specifics</div>
        <div class="muted" style="margin:-2px 0 10px;display:flex;align-items:center;justify-content:space-between;gap:8px;">
          <span>Favorites / Playlist / Collections</span>
          <button type="button" class="cx-help material-symbols-rounded" data-tip-id="cx-em-wl-mode">help</button>
        </div>
        <div class="grid2 compact">
          <div class="opt-row" style="grid-column:1/-1">
            <div class="field-label">Mode</div>
            <div class="seg">
              <input type="radio" name="cx-em-wl-mode" id="cx-em-wl-mode-fav" value="favorites" ${emw.mode==="favorites"?"checked":""}/><label for="cx-em-wl-mode-fav">Favorites</label>
              <input type="radio" name="cx-em-wl-mode" id="cx-em-wl-mode-pl"  value="playlist"  ${emw.mode==="playlist"?"checked":""}/><label for="cx-em-wl-mode-pl">Playlist</label>
              <input type="radio" name="cx-em-wl-mode" id="cx-em-wl-mode-col" value="collection" ${(emw.mode==="collection")?"checked":""}/><label for="cx-em-wl-mode-col">Collections</label>
            </div>
          </div>
          <div class="opt-row" style="grid-column:1/-1">
            <label for="cx-em-wl-pl-name">Name</label>
            <input id="cx-em-wl-pl-name" class="input" type="text" value="${emw.playlist_name||"Watchlist"}" placeholder="Watchlist">
          </div>
        </div>
      `:""}
    `;

    const parts = [`<div class="panel-title">Advanced</div>`];
    
    if (hasPlex(state)) {
      const plex = (state.cfgRaw?.plex) || {};
      const defPri = ["tmdb","imdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"];
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">Plex</div>
        <details id="cx-plx-wl">
          <summary class="muted" style="margin-bottom:10px;">Plex watchlist controls</summary>
          <div class="grid2 compact">
            <div class="opt-row">
              <label for="plx-wl-pms">Allow PMS fallback</label>
              <label class="switch"><input id="plx-wl-pms" type="checkbox" ${plex.watchlist_allow_pms_fallback ? "checked" : ""}><span class="slider"></span></label>
            </div>
            <div class="opt-row">
              <label for="plx-wl-limit">Query limit</label>
              <input id="plx-wl-limit" class="input small" type="number" min="1" max="1000" value="${Number.isFinite(plex.watchlist_query_limit)?plex.watchlist_query_limit:25}">
            </div>
            <div class="opt-row">
              <label for="plx-wl-delay">Write delay (ms)</label>
              <input id="plx-wl-delay" class="input small" type="number" min="0" max="5000" value="${Number.isFinite(plex.watchlist_write_delay_ms)?plex.watchlist_write_delay_ms:0}">
            </div>
            <div class="opt-row">
              <label for="plx-wl-title">Title text search</label>
              <label class="switch"><input id="plx-wl-title" type="checkbox" ${plex.watchlist_title_query !== false ? "checked" : ""}><span class="slider"></span></label>
            </div>
            <div class="opt-row">
              <label for="plx-wl-meta">Use METADATA.matches</label>
              <label class="switch"><input id="plx-wl-meta" type="checkbox" ${plex.watchlist_use_metadata_match !== false ? "checked" : ""}><span class="slider"></span></label>
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="plx-wl-guid">GUID priority</label>
              <input id="plx-wl-guid" class="input" type="text" value="${(Array.isArray(plex.watchlist_guid_priority)&&plex.watchlist_guid_priority.length?plex.watchlist_guid_priority:defPri).join(", ")}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasEmby(state)) {
      const emAdv = (state.cfgRaw?.emby?.watchlist) || {};
      const defPri = ["tmdb","imdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"];
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">Emby</div>
        <details id="cx-em-wl">
          <summary class="muted" style="margin-bottom:10px;">Emby watchlist controls</summary>
          <div class="grid2 compact">
            <div class="opt-row">
              <label for="cx-wl-q">Query limit</label>
              <input id="cx-wl-q" class="input small" type="number" min="5" max="1000" value="${emAdv.watchlist_query_limit ?? 25}">
            </div>
            <div class="opt-row">
              <label for="cx-wl-delay">Write delay (ms)</label>
              <input id="cx-wl-delay" class="input small" type="number" min="0" max="5000" value="${emAdv.watchlist_write_delay_ms ?? 0}">
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="cx-wl-guid">GUID priority</label>
              <input id="cx-wl-guid" class="input" type="text" value="${(emAdv.watchlist_guid_priority || defPri).join(", ")}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasTrakt(state)) {
      const tr = (state.cfgRaw?.trakt) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">Trakt</div>
        <details id="cx-tr-wl">
          <summary class="muted" style="margin-bottom:10px;">Trakt watchlist controls</summary>
          <div class="grid2 compact">
            <div class="opt-row"><label for="tr-wl-etag">Use ETag</label>
              <label class="switch"><input id="tr-wl-etag" type="checkbox" ${tr.watchlist_use_etag!==false?"checked":""}><span class="slider"></span></label>
            </div>
            <div class="opt-row"><label for="tr-wl-batch">Batch size</label>
              <input id="tr-wl-batch" class="input small" type="number" min="10" max="500" value="${tr.watchlist_batch_size ?? 100}">
            </div>
            <div class="opt-row"><label for="tr-wl-log">Log rate limits</label>
              <label class="switch"><input id="tr-wl-log" type="checkbox" ${tr.watchlist_log_rate_limits?"checked":""}><span class="slider"></span></label>
            </div>
            <div class="opt-row"><label for="tr-wl-freeze">Freeze details</label>
              <label class="switch"><input id="tr-wl-freeze" type="checkbox" ${tr.watchlist_freeze_details!==false?"checked":""}><span class="slider"></span></label>
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="tr-wl-ttl">Shadow TTL (hours)</label>
              <input id="tr-wl-ttl" class="input small" type="number" min="1" max="9999" value="${tr.watchlist_shadow_ttl_hours ?? 168}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasSimkl(state)) {
      const sm = (state.cfgRaw?.simkl) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">SIMKL</div>
        <details id="cx-sm-wl">
          <summary class="muted" style="margin-bottom:10px;">SIMKL watchlist controls</summary>
          <div class="grid2 compact">
            <div class="opt-row" style="grid-column:1/-1">
              <label for="sm-wl-batch">Batch size</label>
              <input id="sm-wl-batch" class="input small" type="number" min="1" max="1000" value="${sm.watchlist_batch_size ?? 500}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasMDBList(state)) {
      const md = (state.cfgRaw?.mdblist) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">MDBList</div>
        <details id="cx-md-wl">
          <summary class="muted" style="margin-bottom:10px;">MDBList watchlist controls</summary>
          <div class="grid2 compact">
            <div class="opt-row" style="grid-column:1/-1">
              <label for="md-wl-batch">Batch size</label>
              <input id="md-wl-batch" class="input small" type="number" min="1" max="1000" value="${md.watchlist_batch_size ?? 500}">
            </div>
          </div>
        </details>
      `);
    }

    right.innerHTML = parts.join("");
    applySubDisable("watchlist");
    return;
  }

  if(state.feature==="ratings"){
    const rt=getOpts(state,"ratings"),hasType=t=>Array.isArray(rt.types)&&rt.types.includes(t);

    left.innerHTML=`<div class="panel-title">Ratings | Basics</div>
      <div class="opt-row"><label for="cx-rt-enable">Enable</label><label class="switch"><input id="cx-rt-enable" type="checkbox" ${rt.enable?"checked":""}><span class="slider"></span></label></div>
      <div class="grid2"><div class="opt-row"><label for="cx-rt-add">Add / Update</label><label class="switch"><input id="cx-rt-add" type="checkbox" ${rt.add?"checked":""}><span class="slider"></span></label></div>
      <div class="opt-row"><label for="cx-rt-remove">Remove (clear)</label><label class="switch"><input id="cx-rt-remove" type="checkbox" ${rt.remove?"checked":""}><span class="slider"></span></label></div></div>
      <div class="panel-title small">Scope</div>
      <div class="grid2 compact">
        <div class="opt-row"><label for="cx-rt-type-all">All</label><label class="switch"><input id="cx-rt-type-all" type="checkbox" ${(hasType("movies")&&hasType("shows")&&hasType("seasons")&&hasType("episodes"))?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-rt-type-movies">Movies</label><label class="switch"><input id="cx-rt-type-movies" type="checkbox" ${hasType("movies")?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-rt-type-shows">Shows</label><label class="switch"><input id="cx-rt-type-shows" type="checkbox" ${hasType("shows")?"checked":""}><span class="slider"></span></label></div><div class="opt-row"><label for="cx-rt-type-seasons">Seasons</label><label class="switch"><input id="cx-rt-type-seasons" type="checkbox" ${hasType("seasons")?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-rt-type-episodes">Episodes</label><label class="switch"><input id="cx-rt-type-episodes" type="checkbox" ${hasType("episodes")?"checked":""}><span class="slider"></span></label></div>
      </div>`;

    const parts = [`<div class="panel-title">Advanced</div>
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
      </details>`];

    if (hasTrakt(state)) {
      const tr = (state.cfgRaw?.trakt) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">Trakt</div>
        <details id="cx-tr-rt">
          <summary class="muted" style="margin-bottom:10px;">Trakt ratings controls</summary>
          <div class="grid2 compact">
            <div class="opt-row">
              <label for="tr-rt-perpage">Items per page</label>
              <input id="tr-rt-perpage" class="input small" type="number" min="1" max="500" value="${tr.ratings_per_page ?? 100}">
            </div>
            <div class="opt-row">
              <label for="tr-rt-maxpages">Max pages</label>
              <input id="tr-rt-maxpages" class="input small" type="number" min="1" max="1000" value="${tr.ratings_max_pages ?? 50}">
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="tr-rt-chunk">Write chunk size</label>
              <input id="tr-rt-chunk" class="input small" type="number" min="1" max="1000" value="${tr.ratings_chunk_size ?? 100}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasSimkl(state)) {
      const sm = (state.cfgRaw?.simkl) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">SIMKL</div>
        <details id="cx-sm-rt">
          <summary class="muted" style="margin-bottom:10px;">SIMKL ratings controls</summary>
          <div class="grid2 compact">
            <div class="opt-row" style="grid-column:1/-1">
              <label for="sm-rt-chunk">Write chunk size</label>
              <input id="sm-rt-chunk" class="input small" type="number" min="1" max="1000" value="${sm.ratings_chunk_size ?? 500}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasMDBList(state)) {
      const md = (state.cfgRaw?.mdblist) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">MDBList</div>
        <details id="cx-md-rt">
          <summary class="muted" style="margin-bottom:10px;">MDBList ratings controls</summary>
          <div class="grid2 compact">
            <div class="opt-row">
              <label for="md-rt-perpage">Items per page</label>
              <input id="md-rt-perpage" class="input small" type="number" min="1" max="5000" value="${md.ratings_per_page ?? 200}">
            </div>
            <div class="opt-row">
              <label for="md-rt-maxpages">Max pages</label>
              <input id="md-rt-maxpages" class="input small" type="number" min="1" max="2000" value="${md.ratings_max_pages ?? 50}">
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="md-rt-chunk">Write chunk size</label>
              <input id="md-rt-chunk" class="input small" type="number" min="1" max="1000" value="${md.ratings_chunk_size ?? 500}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasSimkl(state)) {
      parts.push(`<div class="simkl-alert" role="note" aria-live="polite"><div class="title"><span class="ic">⚠</span> Simkl heads-up for Ratings</div><div class="body"><ul class="bul"><li><b>Movies:</b> Rating auto-marks as <i>Completed</i> on Simkl.</li><li>Can appear under <i>Recently watched</i> and <i>My List</i>.</li></ul><div class="mini">Tip: Prefer small windows when backfilling.</div></div></div>`);
    }

    right.innerHTML = parts.join("");
    try{updateRtSummary()}catch{}
    applySubDisable("ratings");
    applyRatingsTypeRules(state);
    return;
  }

  if (state.feature === "history") {
    const hs = getOpts(state, "history");
    const trCfg = (state.cfgRaw?.trakt) || {};
    const emCfg = (state.cfgRaw?.emby?.history) || {};

    const trPair = (state.pairProviders?.trakt) || {};
    const mdPair = (state.pairProviders?.mdblist) || {};
    const smPair = (state.pairProviders?.simkl) || {};
    const trColOn = !!trPair.history_collection;
    const trIgnoreDropped = !!trPair.history_ignore_dropped_shows;
    const mdIgnoreDropped = !!mdPair.history_ignore_dropped_shows;
    const smIgnoreDropped = !!smPair.history_ignore_dropped_shows;
    const trColTypesRaw = trPair.history_collection_types;
    let trColTypes = [];
    if (typeof trColTypesRaw === "string") trColTypes = trColTypesRaw.split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
    else if (Array.isArray(trColTypesRaw)) trColTypes = trColTypesRaw.map(x => String(x).trim().toLowerCase()).filter(Boolean);
    trColTypes = trColTypes.filter(x => x === "movies" || x === "shows");
    if (trColOn && !trColTypes.length) trColTypes = ["movies"];
    const colMovies = trColTypes.includes("movies");
    const colShows = trColTypes.includes("shows");

    const trColRow = hasTrakt(state)
      ? `<div class="opt-row" style="grid-column:1/-1">
          <label for="cx-tr-hs-col"><span class="cx-provider-pill provider-trakt">TRAKT</span> Add to collection</label>
          <label class="switch">
            <input id="cx-tr-hs-col" type="checkbox" ${trColOn ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="grid2 compact" id="cx-tr-hs-col-types" style="grid-column:1/-1; padding-left:8px; margin-top:-6px; ${trColOn ? "" : "display:none;"}">
          <div class="opt-row">
            <label for="cx-tr-hs-col-movies">Movies</label>
            <label class="switch">
              <input id="cx-tr-hs-col-movies" type="checkbox" ${colMovies ? "checked" : ""}>
              <span class="slider"></span>
            </label>
          </div>
          <div class="opt-row">
            <label for="cx-tr-hs-col-shows">Shows</label>
            <label class="switch">
              <input id="cx-tr-hs-col-shows" type="checkbox" ${colShows ? "checked" : ""}>
              <span class="slider"></span>
            </label>
          </div>
        </div>
        <div class="opt-row" style="grid-column:1/-1">
          <label for="cx-tr-hs-ignore-dropped"><span class="cx-provider-pill provider-trakt">TRAKT</span> Ignore dropped shows</label>
          <label class="switch">
            <input id="cx-tr-hs-ignore-dropped" type="checkbox" ${trIgnoreDropped ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>`
      : "";
    const mdDroppedRow = hasMDBList(state)
      ? `<div class="opt-row" style="grid-column:1/-1">
          <label for="cx-md-hs-ignore-dropped"><span class="cx-provider-pill provider-mdblist">MDBLIST</span> Ignore dropped shows</label>
          <label class="switch">
            <input id="cx-md-hs-ignore-dropped" type="checkbox" ${mdIgnoreDropped ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>`
      : "";
    const smDroppedRow = hasSimkl(state)
      ? `<div class="opt-row" style="grid-column:1/-1">
          <label for="cx-sm-hs-ignore-dropped"><span class="cx-provider-pill provider-simkl">SIMKL</span> Ignore dropped shows</label>
          <label class="switch">
            <input id="cx-sm-hs-ignore-dropped" type="checkbox" ${smIgnoreDropped ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>`
      : "";
left.innerHTML = `
      <div class="panel-title">History | Basics</div>
      <div class="opt-row">
        <label for="cx-hs-enable">Enable</label>
        <label class="switch">
          <input id="cx-hs-enable" type="checkbox" ${hs.enable ? "checked" : ""}>
          <span class="slider"></span>
        </label>
      </div>
      <div class="grid2">
        <div class="opt-row">
          <label for="cx-hs-add">Add</label>
          <label class="switch">
            <input id="cx-hs-add" type="checkbox" ${hs.add ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="opt-row">
          <label for="cx-hs-remove">Remove</label>
          <label class="switch">
            <input id="cx-hs-remove" type="checkbox" ${hs.remove ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        ${trColRow}
        ${mdDroppedRow}
        ${smDroppedRow}
      </div>
      <div class="muted">Synchronize plays between providers. “Remove” is not recommended and should only be enabled for specific cases like mirroring.</div>
    `;

    const parts = [`<div class="panel-title">Advanced</div>`];

    if (hasTrakt(state)) {
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">Trakt</div>
        <details id="cx-tr-hs">
          <summary class="muted" style="margin-bottom:10px;">Trakt history controls</summary>
          <div class="grid2 compact">
            <div class="opt-row">
              <label for="cx-tr-hs-numfb">Number Fallback</label>
              <label class="switch"><input id="cx-tr-hs-numfb" type="checkbox" ${trCfg.history_number_fallback ? "checked" : ""}><span class="slider"></span></label>
            </div>
            <div class="opt-row">
              <label for="cx-tr-hs-unres">Unresolved Freeze</label>
              <label class="switch"><input id="cx-tr-hs-unres" type="checkbox" ${trCfg.history_unresolved ? "checked" : ""}><span class="slider"></span></label>
            </div>
            <div class="opt-row">
              <label for="tr-hs-perpage">Items per page</label>
              <input id="tr-hs-perpage" class="input small" type="number" min="1" max="500" value="${trCfg.history_per_page ?? 100}">
            </div>
            <div class="opt-row">
              <label for="tr-hs-maxpages">Max pages</label>
              <input id="tr-hs-maxpages" class="input small" type="number" min="1" max="100000" value="${trCfg.history_max_pages ?? 10000}">
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="tr-hs-chunk">Write chunk size</label>
              <input id="tr-hs-chunk" class="input small" type="number" min="1" max="1000" value="${trCfg.history_chunk_size ?? 100}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasSimkl(state)) {
      const sm = (state.cfgRaw?.simkl) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">SIMKL</div>
        <details id="cx-sm-hs">
          <summary class="muted" style="margin-bottom:10px;">SIMKL history controls</summary>
          <div class="grid2 compact">
            <div class="opt-row" style="grid-column:1/-1">
              <label for="sm-hs-chunk">Write chunk size</label>
              <input id="sm-hs-chunk" class="input small" type="number" min="1" max="1000" value="${sm.history_chunk_size ?? 500}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasMDBList(state)) {
      const md = (state.cfgRaw?.mdblist) || {};
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">MDBList</div>
        <details id="cx-md-hs">
          <summary class="muted" style="margin-bottom:10px;">MDBList history controls</summary>
          <div class="grid2 compact">
            <div class="opt-row">
              <label for="md-hs-perpage">Items per page</label>
              <input id="md-hs-perpage" class="input small" type="number" min="1" max="5000" value="${md.history_per_page ?? 1000}">
            </div>
            <div class="opt-row">
              <label for="md-hs-maxpages">Max pages</label>
              <input id="md-hs-maxpages" class="input small" type="number" min="1" max="2000" value="${md.history_max_pages ?? 250}">
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="md-hs-chunk">Write chunk size</label>
              <input id="md-hs-chunk" class="input small" type="number" min="1" max="1000" value="${md.history_chunk_size ?? 500}">
            </div>
          </div>
        </details>
      `);
    }

    if (hasEmby(state)) {
      const defPri = ["tmdb","imdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"];
      parts.push(`
        <div class="panel-title small" style="margin-top:6px">Emby</div>
        <details id="cx-em-hs">
          <summary class="muted" style="margin-bottom:10px;">Emby history controls</summary>
          <div class="grid2 compact">
            <div class="opt-row">
              <label for="cx-em-hs-limit">Query limit</label>
              <input id="cx-em-hs-limit" class="input small" type="number" min="1" max="1000" value="${Number.isFinite(emCfg.history_query_limit)?emCfg.history_query_limit:25}">
            </div>
            <div class="opt-row">
              <label for="cx-em-hs-delay">Write delay (ms)</label>
              <input id="cx-em-hs-delay" class="input small" type="number" min="0" max="5000" value="${Number.isFinite(emCfg.history_write_delay_ms)?emCfg.history_write_delay_ms:0}">
            </div>
            <div class="opt-row" style="grid-column:1/-1">
              <label for="cx-em-hs-guid">GUID priority</label>
              <input id="cx-em-hs-guid" class="input" type="text" value="${(Array.isArray(emCfg.history_guid_priority)&&emCfg.history_guid_priority.length?emCfg.history_guid_priority:defPri).join(", ")}">
            </div>
          </div>
        </details>
      `);
    }

    if (parts.length === 1) {
      parts.push(`<div class="muted">More controls coming later.</div>`);
    }

    right.innerHTML = parts.join("");
    applySubDisable("history");
    return;
  }

  if (state.feature === "playlists") {
    const pl=getOpts(state,"playlists");
    left.innerHTML=`<div class="panel-title">Playlists</div>
      <div class="grid2"><div class="opt-row"><label for="cx-pl-enable">Enable</label><label class="switch"><input id="cx-pl-enable" type="checkbox" ${pl.enable?"checked":""}><span class="slider"></span></label></div>
      <div class="opt-row"><label for="cx-pl-add">Add</label><label class="switch"><input id="cx-pl-add" type="checkbox" ${pl.add?"checked":""}><span class="slider"></span></label></div>
      <div class="opt-row"><label for="cx-pl-remove">Remove</label><label class="switch"><input id="cx-pl-remove" type="checkbox" ${pl.remove?"checked":""}><span class="slider"></span></label></div></div>`;
    right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">Experimental.</div>`;
    applySubDisable("playlists");
    return;
  }

  if (state.feature === "progress") {
    const pr = getOpts(state, "progress") || {};
    const minS = Number.isFinite(pr.min_seconds) ? pr.min_seconds : 60;
    const deltaS = Number.isFinite(pr.delta_seconds) ? pr.delta_seconds : 30;
    const maxP = Number.isFinite(pr.max_percent) ? pr.max_percent : 80;

    left.innerHTML = `<div class="panel-title">Progress | Basics</div>
      <div class="grid2">
        <div class="opt-row"><label for="cx-pr-enable" data-tip-id="cx-pr-enable">Enable</label>
          <label class="switch"><input id="cx-pr-enable" type="checkbox" ${pr.enable ? "checked" : ""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-pr-add" data-tip-id="cx-pr-add">Add / Update</label>
          <label class="switch"><input id="cx-pr-add" type="checkbox" ${pr.add ? "checked" : ""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-pr-remove" data-tip-id="cx-pr-remove">Remove</label>
          <label class="switch"><input id="cx-pr-remove" type="checkbox" ${pr.remove ? "checked" : ""}><span class="slider"></span></label></div>
      </div>`;

    right.innerHTML = `<div class="panel-title">Advanced</div>
      <div class="grid2 compact">
        <div class="opt-row"><label for="cx-pr-min" data-tip-id="cx-pr-min">Minimum seconds</label>
          <input id="cx-pr-min" class="input small" type="number" min="0" max="36000" value="${minS}"></div>
        <div class="opt-row"><label for="cx-pr-delta" data-tip-id="cx-pr-delta">Change threshold (s)</label>
          <input id="cx-pr-delta" class="input small" type="number" min="0" max="36000" value="${deltaS}"></div>
        <div class="opt-row"><label for="cx-pr-maxp" data-tip-id="cx-pr-maxp">Ignore near complete (%)</label>
          <input id="cx-pr-maxp" class="input small" type="number" min="0" max="100" step="1" value="${maxP}"></div>
      </div>
      <div class="muted" style="margin-top:10px">Tip: Progress does not infer clears from absence. It only syncs resume positions.</div>`;

    applySubDisable("progress");
    return;
  }
}

function bindChangeHandlers(state,root){
  const syncGlobalsUI = () => {
    const drop = ID("gl-drop");
    const mass = ID("gl-mass");
    if (!drop || !mass) return;

    if (drop.checked && mass.checked) mass.checked = false;
    if (mass.checked) drop.checked = false;

    const dropOn = !!drop.checked;
    const massOn = !!mass.checked;

    mass.disabled = dropOn;
    drop.disabled = massOn;

    mass.closest?.(".opt-row")?.classList.toggle("muted", mass.disabled);
    drop.closest?.(".opt-row")?.classList.toggle("muted", drop.disabled);

    const adv = ID("gl-drop-adv");
    if (adv) {
      adv.style.opacity = dropOn ? "" : "0.5";
      adv.style.pointerEvents = dropOn ? "auto" : "none";
    }
  };
  root.addEventListener("input",(e)=>{
    const id=e.target.id;
    if(id==="gl-sus-pct-range"||id==="gl-sus-pct"||id==="gl-sus-min"){
      let pct=parseInt((id==="gl-sus-min"? (Q("#gl-sus-pct")?.value||Q("#gl-sus-pct-range")?.value) : e.target.value)||"10",10);
      if(!Number.isFinite(pct)) pct=10;
      pct=Math.min(50,Math.max(1,pct));
      const r=Q("#gl-sus-pct-range"), n=Q("#gl-sus-pct");
      if(id==="gl-sus-pct-range"&&n) n.value=String(pct);
      if(id==="gl-sus-pct"&&r) r.value=String(pct);

      const minPrev=Math.max(0,parseInt(Q("#gl-sus-min")?.value||"20",10)||20);
      const dropOn=!!Q("#gl-drop")?.checked;
      const bb=state.globals?.blackbox||{};

      const vp=ID("gl-sus-pct-val"); if(vp) vp.textContent=String(pct);
      const vm=ID("gl-sus-min-val"); if(vm) vm.textContent=String(minPrev);

      state.globals=Object.assign({},state.globals,{
        dry_run:!!Q("#gl-dry")?.checked,
        verify_after_write:!!Q("#gl-verify")?.checked,
        drop_guard:dropOn,
        allow_mass_delete:!!Q("#gl-mass")?.checked && !dropOn,
        tombstone_ttl_days:parseInt(Q("#gl-ttl")?.value||"0",10)||0,
        include_observed_deletes:!!Q("#gl-observed")?.checked,
        runtime:{suspect_min_prev:minPrev,suspect_shrink_ratio:pct/100},
        blackbox:bb
      });
      const adv=ID("gl-drop-adv");
      if(adv){adv.style.opacity=dropOn?"":"0.5";adv.style.pointerEvents=dropOn?"auto":"none"}
    }
  });

  root.addEventListener("change",(e)=>{
    const id=e.target.id, el=e.target;

    if(id==="plx-strict-ids"||id==="jf-strict-ids"||id==="em-strict-ids"){
      state.pairProviders=state.pairProviders||{};
      if(id==="plx-strict-ids") state.pairProviders.plex=Object.assign({},state.pairProviders.plex||{}, {strict_id_matching:!!el.checked});
      if(id==="jf-strict-ids") state.pairProviders.jellyfin=Object.assign({},state.pairProviders.jellyfin||{}, {strict_id_matching:!!el.checked});
      if(id==="em-strict-ids") state.pairProviders.emby=Object.assign({},state.pairProviders.emby||{}, {strict_id_matching:!!el.checked});
      return;
    }
    if(id==="cx-tr-hs-col"||id==="cx-tr-hs-col-movies"||id==="cx-tr-hs-col-shows"||id==="cx-tr-hs-ignore-dropped"){
      state.pairProviders=state.pairProviders||{};
      const tr=Object.assign({},state.pairProviders.trakt||{});
      const on=!!ID("cx-tr-hs-col")?.checked;
      tr.history_collection=on;
      tr.history_ignore_dropped_shows=!!ID("cx-tr-hs-ignore-dropped")?.checked;

      const box=ID("cx-tr-hs-col-types");
      if(box) box.style.display=on?"":"none";

      let mOn=!!ID("cx-tr-hs-col-movies")?.checked;
      let sOn=!!ID("cx-tr-hs-col-shows")?.checked;

      if(on && !mOn && !sOn){
        mOn=true;
        const m=ID("cx-tr-hs-col-movies"); if(m) m.checked=true;
      }

      if(on){
        const types=[];
        if(mOn) types.push("movies");
        if(sOn) types.push("shows");
        tr.history_collection_types=types;
      }else{
        delete tr.history_collection_types;
      }

      state.pairProviders.trakt=tr;
      return;
    }
    if(id==="cx-md-hs-ignore-dropped"){
      state.pairProviders=state.pairProviders||{};
      const md=Object.assign({},state.pairProviders.mdblist||{});
      md.history_ignore_dropped_shows=!!ID("cx-md-hs-ignore-dropped")?.checked;
      state.pairProviders.mdblist=md;
      return;
    }
    if(id==="cx-sm-hs-ignore-dropped"){
      state.pairProviders=state.pairProviders||{};
      const sm=Object.assign({},state.pairProviders.simkl||{});
      sm.history_ignore_dropped_shows=!!ID("cx-sm-hs-ignore-dropped")?.checked;
      state.pairProviders.simkl=sm;
      return;
    }

    const map={
            "cx-wl-enable":"cx-wl-remove",
            "cx-rt-enable":"cx-rt-remove",
            "cx-hs-enable":"cx-hs-remove",
            "cx-pl-enable":"cx-pl-remove",
            "cx-pr-enable":"cx-pr-remove"
          };

    if(map[id]){
      const rm=ID(map[id]);
      if(rm){
        rm.disabled=!e.target.checked;
        if(!e.target.checked) rm.checked=false;
      }
    }

    if (id === "gl-drop" || id === "gl-mass") {
      if (id === "gl-drop" && !!ID("gl-drop")?.checked) {
        const m = ID("gl-mass");
        if (m) m.checked = false;
      }
      if (id === "gl-mass" && !!ID("gl-mass")?.checked) {
        const d = ID("gl-drop");
        if (d) d.checked = false;
      }
      syncGlobalsUI();
    }

    if (id === "cx-wl-enable") {
      if (!!ID("cx-wl-enable")?.checked) {
        const add = ID("cx-wl-add");
        if (add) add.checked = true;
      }
      applySubDisable("watchlist");
    }

    if (id === "cx-rt-enable") {
      applySubDisable("ratings");
    }

    if (id === "cx-hs-enable") {
      if (!!ID("cx-hs-enable")?.checked) {
        const add = ID("cx-hs-add");
        if (add) add.checked = true;
      }
      applySubDisable("history");
    }

    if (id === "cx-pl-enable") {
      applySubDisable("playlists");
    }

    if (id === "cx-pr-enable") {
      if (!!ID("cx-pr-enable")?.checked) {
        const add = ID("cx-pr-add");
        if (add) add.checked = true;
      }
      applySubDisable("progress");
    }

    if(id.startsWith("cx-wl-")){
      const prev=state.options.watchlist||{};
      state.options.watchlist=Object.assign({},prev,{
        enable:!!ID("cx-wl-enable")?.checked,
        add:!!ID("cx-wl-add")?.checked,
        remove:!!ID("cx-wl-remove")?.checked
      });
      state.visited.add("watchlist");
    }

    if(id.startsWith("cx-rt-")){
      if(id==="cx-rt-enable"&&ID("cx-rt-enable")?.checked){
        ID("cx-rt-add").checked=true;ID("cx-rt-remove").checked=false;
        const dis=ratingsDisabledFor(state);
        ["movies","shows","seasons","episodes"].forEach(t=>{const cb=ID("cx-rt-type-"+t);if(cb)cb.checked=!dis.has(t)});
        const all=ID("cx-rt-type-all");if(all)all.checked=true;
        const modeSel=ID("cx-rt-mode");if(modeSel)modeSel.value="all";
        const fd=ID("cx-rt-from-date");if(fd){fd.value="";fd.disabled=true}
      }
      if(id==="cx-rt-type-all"){
        const on=!!ID("cx-rt-type-all")?.checked;const dis=ratingsDisabledFor(state);
        ["movies","shows","seasons","episodes"].forEach(t=>{const cb=ID("cx-rt-type-"+t);if(cb&&!dis.has(t))cb.checked=on});
      }else if(/^cx-rt-type-(movies|shows|seasons|episodes)$/.test(id)){
        const dis=ratingsDisabledFor(state);
        const allOn=["movies","shows","seasons","episodes"].filter(t=>!dis.has(t)).every(t=>ID("cx-rt-type-"+t)?.checked);
        const allCb=ID("cx-rt-type-all");if(allCb)allCb.checked=!!allOn;
      }
      if(id==="cx-rt-mode"){
        const md=ID("cx-rt-mode")?.value||"all",fd=ID("cx-rt-from-date");
        if(fd){fd.disabled=md!=="from_date";if(md!=="from_date")fd.value=""}
      }
      const rt=state.options.ratings||{};
      const dis=ratingsDisabledFor(state);
      const enabledTypes=["movies","shows","seasons","episodes"].filter(t=>!dis.has(t));
      const types=ID("cx-rt-type-all")?.checked?enabledTypes:enabledTypes.filter(t=>ID("cx-rt-type-"+t)?.checked);
      state.options.ratings=Object.assign({},rt,{
        enable:!!ID("cx-rt-enable")?.checked,
        add:!!ID("cx-rt-add")?.checked,
        remove:!!ID("cx-rt-remove")?.checked,
        types,
        mode:ID("cx-rt-mode")?.value||"all",
        from_date:(ID("cx-rt-from-date")?.value||"").trim()
      });
      state.visited.add("ratings");
      try{updateRtSummary()}catch{}
      applyRatingsTypeRules(state);
    }

    if (id.startsWith("cx-hs-")) {
      const prev = state.options.history || {};
      state.options.history = Object.assign({}, prev, {
        enable: !!ID("cx-hs-enable")?.checked,
        add:    !!ID("cx-hs-add")?.checked,
        remove: !!ID("cx-hs-remove")?.checked,
      });
      state.visited.add("history");
    }

    if(id.startsWith("cx-pl-")){
      const prev=state.options.playlists||{};
      state.options.playlists=Object.assign({},prev,{
        enable:!!ID("cx-pl-enable")?.checked,
        add:!!ID("cx-pl-add")?.checked,
        remove:!!ID("cx-pl-remove")?.checked
      });
      state.visited.add("playlists");
    }

    if(id.startsWith("cx-pr-")){
      const prev=state.options.progress||{};
      const minS=parseInt(ID("cx-pr-min")?.value||"60",10);
      const delS=parseInt(ID("cx-pr-delta")?.value||"30",10);
      const maxP=parseFloat(ID("cx-pr-maxp")?.value||"95");
      state.options.progress=Object.assign({},prev,{
        enable:!!ID("cx-pr-enable")?.checked,
        add:!!ID("cx-pr-add")?.checked,
        remove:!!ID("cx-pr-remove")?.checked,
        min_seconds: Number.isFinite(minS)?Math.max(0,minS):60,
        delta_seconds: Number.isFinite(delS)?Math.max(0,delS):30,
        max_percent: Number.isFinite(maxP)?Math.min(100,Math.max(0,maxP)):80
      });
      state.visited.add("progress");
    }

    if(id.startsWith("gl-")){
      const bb={
        enabled:!!Q("#gl-bb-enable")?.checked,
        pair_scoped:!!Q("#gl-bb-pair")?.checked,
        promote_after:Math.min(365,Math.max(0,parseInt(Q("#gl-bb-promote")?.value||"0",10)||0)),
        unresolved_days:Math.min(365,Math.max(0,parseInt(Q("#gl-bb-unresolved")?.value||"0",10)||0)),
        cooldown_days:Math.min(365,Math.max(0,parseInt(Q("#gl-bb-cooldown")?.value||"0",10)||0))
      };
      bb.block_adds = bb.enabled;
      bb.block_removes = bb.enabled;

      let pct=parseInt(Q("#gl-sus-pct")?.value||Q("#gl-sus-pct-range")?.value||"10",10);
      if(!Number.isFinite(pct)) pct=10;
      pct=Math.min(50,Math.max(1,pct));
      const rangeEl=Q("#gl-sus-pct-range");
      const numEl=Q("#gl-sus-pct");
      if(rangeEl) rangeEl.value=String(pct);
      if(numEl) numEl.value=String(pct);

      const minPrev=Math.max(0,parseInt(Q("#gl-sus-min")?.value||"20",10)||20);
      syncGlobalsUI();
      const dropOn=!!Q("#gl-drop")?.checked;
      const adv=ID("gl-drop-adv");
      if(adv){adv.style.opacity=dropOn?"":"0.5";adv.style.pointerEvents=dropOn?"auto":"none"}

      state.globals={
        dry_run:!!Q("#gl-dry")?.checked,
        verify_after_write:!!Q("#gl-verify")?.checked,
        drop_guard:dropOn,
        allow_mass_delete:!!Q("#gl-mass")?.checked && !dropOn,
        tombstone_ttl_days:parseInt(Q("#gl-ttl")?.value||"0",10)||0,
        include_observed_deletes:!!Q("#gl-observed")?.checked,
        runtime:{suspect_min_prev:minPrev,suspect_shrink_ratio:pct/100},
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

    if(id==="cx-em-wl-mode-fav"||id==="cx-em-wl-mode-pl"||id==="cx-em-wl-mode-col"||id==="cx-em-wl-pl-name"||id==="cx-wl-q"||id==="cx-wl-delay"||id==="cx-wl-guid"){
      const em=state.emby||(state.emby={});
      const mode=ID("cx-em-wl-mode-pl")?.checked?"playlist":ID("cx-em-wl-mode-col")?.checked?"collection":"favorites";
      const name=(ID("cx-em-wl-pl-name")?.value||"").trim()||"Watchlist";
      const q=parseInt(ID("cx-wl-q")?.value||"25",10)||25;
      const d=parseInt(ID("cx-wl-delay")?.value||"0",10)||0;
      const gp=(ID("cx-wl-guid")?.value||"").split(",").map(s=>s.trim()).filter(Boolean);
      em.watchlist={mode,playlist_name:name,watchlist_query_limit:q,watchlist_write_delay_ms:d,watchlist_guid_priority:gp.length?gp:undefined};
    }

    if (/^(plx-|jf-|em-)/.test(id)) {
      refreshProviderCardSummaries(state);
    }

    const flowRelevant=/^(cx-(wl|rt|hs|pr|pl)-|cx-enabled|cx-mode-one|cx-mode-two)/.test(id);
    if(flowRelevant) updateFlow(state,id==="cx-enabled"||id==="cx-mode-one"||id==="cx-mode-two");
    else{ updateFlowClasses(state); renderWarnings(state); }
  });
}

// Save config bits
async function saveConfigBits(state){
  try{
    const cur=await fetch("/api/config",{cache:"no-store"}).then(r=>r.ok?r.json():{});
    const cfg=typeof structuredClone==="function"?structuredClone(cur||{}):jclone(cur||{});

    if(ID("gl-dry")){
      const dropOn=!!ID("gl-drop")?.checked;
      const massOn=!!ID("gl-mass")?.checked && !dropOn;
      const s={
        dry_run:!!ID("gl-dry")?.checked,
        verify_after_write:!!ID("gl-verify")?.checked,
        drop_guard:dropOn,
        allow_mass_delete:massOn,
        one_way_remove_mode:!!ID("gl-oneway-remove")?.checked ? "source_deletes" : "mirror",
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

      let pct = parseInt(ID("gl-sus-pct")?.value||ID("gl-sus-pct-range")?.value||"10",10);
      if(!Number.isFinite(pct)) pct=10;
      pct=Math.min(50,Math.max(1,pct));
      const runtime={
        suspect_min_prev: Math.max(0, parseInt(ID("gl-sus-min")?.value||"20",10)||20),
        suspect_shrink_ratio: pct/100
      };

      cfg.sync = Object.assign(
        {},
        cfg.sync || {},
        s,
        { runtime: Object.assign({}, cfg.sync?.runtime||{}, runtime) },
        { blackbox: Object.assign({}, cfg.sync?.blackbox||{}, bb) }
      );
    }

    {
      const plex = Object.assign({}, cfg.plex || {});
      const n = (id) => parseInt((ID(id)?.value || "").trim(), 10);
      const num = (id, min, def) => {
        const v = n(id);
        return Number.isFinite(v) ? Math.max(min, v) : def;
      };

      if (ID("plx-rating-workers"))  plex.rating_workers  = num("plx-rating-workers", 1, plex.rating_workers ?? 12);
      if (ID("plx-history-workers")) plex.history_workers = num("plx-history-workers", 1, plex.history_workers ?? 12);
      if (ID("plx-timeout"))         plex.timeout         = num("plx-timeout", 1, plex.timeout ?? 10);
      if (ID("plx-retries")) {
        const rv = n("plx-retries");
        plex.max_retries = Number.isFinite(rv) ? Math.max(0, rv) : (plex.max_retries ?? 3);
      }
      if (ID("plx-fallback-guid"))   plex.fallback_GUID   = !!ID("plx-fallback-guid").checked;
      if (ID("plx-marked-watched")) plex.history = Object.assign({}, plex.history || {}, { include_marked_watched: !!ID("plx-marked-watched").checked });
      if (ID("plx-wl-pms"))          plex.watchlist_allow_pms_fallback = !!ID("plx-wl-pms").checked;
      if (ID("plx-wl-limit"))        plex.watchlist_query_limit        = num("plx-wl-limit", 1, plex.watchlist_query_limit ?? 25);
      if (ID("plx-wl-delay")) {
        const dv = n("plx-wl-delay");
        plex.watchlist_write_delay_ms = Number.isFinite(dv) ? Math.max(0, dv) : (plex.watchlist_write_delay_ms ?? 0);
      }
      if (ID("plx-wl-guid")) {
        plex.watchlist_guid_priority = (ID("plx-wl-guid").value || "")
          .split(",").map(s=>s.trim()).filter(Boolean);
      }
      if (ID("plx-wl-title"))        plex.watchlist_title_query        = !!ID("plx-wl-title").checked;
      if (ID("plx-wl-meta"))         plex.watchlist_use_metadata_match = !!ID("plx-wl-meta").checked;

      cfg.plex = plex;
    }

    if(ID("jf-timeout")){
      const jf=Object.assign({},cfg.jellyfin||{});
      jf.timeout = Math.max(1, parseInt(ID("jf-timeout").value||"15",10)||15);
      jf.max_retries = Math.max(0, parseInt(ID("jf-retries").value||"3",10)||3);
      cfg.jellyfin=jf;
    }

    if(ID("em-timeout")){
      const em=Object.assign({},cfg.emby||{});
      em.timeout = Math.max(1, parseInt(ID("em-timeout").value||"15",10)||15);
      em.max_retries = Math.max(0, parseInt(ID("em-retries").value||"3",10)||3);
      cfg.emby=em;
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
        watchlist_freeze_details: !!ID("tr-wl-freeze")?.checked
      });
    }

    if(ID("sm-wl-batch")){
      cfg.simkl = Object.assign({}, cfg.simkl||{}, {
        watchlist_batch_size: Math.max(1, parseInt(ID("sm-wl-batch")?.value||"500",10)||500)
      });
    }

    if(ID("md-wl-batch")){
      cfg.mdblist = Object.assign({}, cfg.mdblist||{}, {
        watchlist_batch_size: Math.max(1, parseInt(ID("md-wl-batch")?.value||"500",10)||500)
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

    const hasEM = String(state.src||"").toUpperCase()==="EMBY" || String(state.dst||"").toUpperCase()==="EMBY";
    if(hasEM){
      const em = Object.assign({}, cfg.emby || {});
      const mode = ID("cx-em-wl-mode-pl")?.checked ? "playlist" : ID("cx-em-wl-mode-col")?.checked ? "collection" : ID("cx-em-wl-mode-fav")?.checked ? "favorites" : (state.emby?.watchlist?.mode || "favorites");
      const name = (ID("cx-em-wl-pl-name")?.value || state.emby?.watchlist?.playlist_name || "Watchlist").trim() || "Watchlist";
      const q = parseInt(ID("cx-wl-q")?.value||"25",10)||undefined;
      const d = parseInt(ID("cx-wl-delay")?.value||"0",10)||undefined;
      const gp = (ID("cx-wl-guid")?.value||"").split(",").map(s=>s.trim()).filter(Boolean);
      em.watchlist = Object.assign({}, em.watchlist||{}, { mode, playlist_name:name });
      if(Number.isFinite(q)) em.watchlist.watchlist_query_limit = q;
      if(Number.isFinite(d)) em.watchlist.watchlist_write_delay_ms = d;
      if(gp.length) em.watchlist.watchlist_guid_priority = gp;

      const hq = parseInt(ID("cx-em-hs-limit")?.value||"",10);
      const hd = parseInt(ID("cx-em-hs-delay")?.value||"",10);
      const hg = (ID("cx-em-hs-guid")?.value||"").split(",").map(s=>s.trim()).filter(Boolean);
      em.history = Object.assign({}, em.history || {});
      if (Number.isFinite(hq)) em.history.history_query_limit = Math.max(1, hq);
      if (Number.isFinite(hd)) em.history.history_write_delay_ms = Math.max(0, hd);
      if (hg.length) em.history.history_guid_priority = hg;

      cfg.emby = em;
    }

    const hasTR = String(state.src||"").toUpperCase()==="TRAKT" || String(state.dst||"").toUpperCase()==="TRAKT";
    if(hasTR){
      const tr=Object.assign({},cfg.trakt||{});
      const numfb=ID("cx-tr-hs-numfb");
      const unres=ID("cx-tr-hs-unres");
      if(numfb) tr.history_number_fallback = !!numfb.checked; 
      if(unres) tr.history_unresolved       = !!unres.checked;

      const n = x => parseInt((ID(x)?.value||"").trim(), 10);
      const clamp = (v,min,max)=>Math.min(max,Math.max(min,Number.isFinite(v)?v:min));

      const perEl   = ID("tr-rt-perpage") || ID("tr-rt-page");
      const pagesEl = ID("tr-rt-maxpages")|| ID("tr-rt-pages");
      const chunkEl = ID("tr-rt-chunk");
      const hsPerEl = ID("tr-hs-perpage");
      const hsPagesEl = ID("tr-hs-maxpages");
      const hsChunkEl = ID("tr-hs-chunk");

      if (perEl)   tr.ratings_per_page   = clamp(n(perEl.id), 10, 500);
      if (pagesEl) tr.ratings_max_pages  = clamp(n(pagesEl.id), 1, 1000);
      if (chunkEl) tr.ratings_chunk_size = clamp(n(chunkEl.id), 10, 1000);
      if (hsPerEl)   tr.history_per_page   = clamp(n(hsPerEl.id), 1, 500);
      if (hsPagesEl) tr.history_max_pages  = clamp(n(hsPagesEl.id), 1, 100000);
      if (hsChunkEl) tr.history_chunk_size = clamp(n(hsChunkEl.id), 1, 1000);

      cfg.trakt=tr;
    }

    const hasSM = String(state.src||"").toUpperCase()==="SIMKL" || String(state.dst||"").toUpperCase()==="SIMKL";
    if(hasSM){
      const sm = Object.assign({}, cfg.simkl||{});
      const wlBatchEl = ID("sm-wl-batch");
      const rtChunkEl = ID("sm-rt-chunk");
      const hsChunkEl = ID("sm-hs-chunk");
      if (wlBatchEl) sm.watchlist_batch_size = Math.max(1, parseInt(wlBatchEl.value||"500", 10) || 500);
      if (rtChunkEl) sm.ratings_chunk_size = Math.max(1, parseInt(rtChunkEl.value||"500", 10) || 500);
      if (hsChunkEl) sm.history_chunk_size = Math.max(1, parseInt(hsChunkEl.value||"500", 10) || 500);
      cfg.simkl = sm;
    }

    const hasMD = String(state.src||"").toUpperCase()==="MDBLIST" || String(state.dst||"").toUpperCase()==="MDBLIST";
    if(hasMD){
      const md = Object.assign({}, cfg.mdblist||{});
      const n = x => parseInt((ID(x)?.value||"").trim(), 10);
      const clamp = (v,min,max)=>Math.min(max,Math.max(min,Number.isFinite(v)?v:min));
      const wlBatchEl = ID("md-wl-batch");
      const rtPerEl = ID("md-rt-perpage");
      const rtPagesEl = ID("md-rt-maxpages");
      const rtChunkEl = ID("md-rt-chunk");
      const hsPerEl = ID("md-hs-perpage");
      const hsPagesEl = ID("md-hs-maxpages");
      const hsChunkEl = ID("md-hs-chunk");
      if (wlBatchEl) md.watchlist_batch_size = clamp(n(wlBatchEl.id), 1, 1000);
      if (rtPerEl) md.ratings_per_page = clamp(n(rtPerEl.id), 1, 5000);
      if (rtPagesEl) md.ratings_max_pages = clamp(n(rtPagesEl.id), 1, 2000);
      if (rtChunkEl) md.ratings_chunk_size = clamp(n(rtChunkEl.id), 1, 1000);
      if (hsPerEl) md.history_per_page = clamp(n(hsPerEl.id), 1, 5000);
      if (hsPagesEl) md.history_max_pages = clamp(n(hsPagesEl.id), 1, 2000);
      if (hsChunkEl) md.history_chunk_size = clamp(n(hsChunkEl.id), 1, 1000);
      cfg.mdblist = md;
    }

    const res=await fetch("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)});
    if(!res.ok)throw new Error("POST /api/config "+res.status);
  }catch(e){console.warn("[cx] saving config bits failed",e)}
}

function buildPayload(state,wrap){
  const src=state.src||ID("cx-src")?.value||ID("cx-src-display")?.dataset.value||"";
  const dst=state.dst||ID("cx-dst")?.value||ID("cx-dst-display")?.dataset.value||"";
  const srcInst=state.src_instance||ID("cx-src-inst")?.value||"default";
  const dstInst=state.dst_instance||ID("cx-dst-inst")?.value||"default";
  const modeTwo=!!ID("cx-mode-two")?.checked;const enabled=!!ID("cx-enabled")?.checked;
  const get=k=>Object.assign(defaultFor(k), (state.options||{})[k]||{});
  const watchlist=get("watchlist");
  const ratings=get("ratings");
  const progress=get("progress");
  const dis=ratingsDisabledFor({src,dst});
  if(ratings&&Array.isArray(ratings.types)&&dis.size)ratings.types=ratings.types.filter(t=>!dis.has(String(t)));
  const payload={source:src,target:dst,source_instance:String(srcInst||"default"),target_instance:String(dstInst||"default"),enabled,mode:modeTwo?"two-way":"one-way",features:{watchlist,ratings,history:get("history"),progress,playlists:get("playlists")}};
  const prov={};
  const pp=state.pairProviders||{};
  const usePlex=(String(src).toUpperCase()==="PLEX"||String(dst).toUpperCase()==="PLEX");
  const useJf=(String(src).toUpperCase()==="JELLYFIN"||String(dst).toUpperCase()==="JELLYFIN");
  const useEm=(String(src).toUpperCase()==="EMBY"||String(dst).toUpperCase()==="EMBY");
  const useTr=(String(src).toUpperCase()==="TRAKT"||String(dst).toUpperCase()==="TRAKT");
  if(usePlex) prov.plex={strict_id_matching:getPairProviderStrictValue(state, "plex")};
  if(useJf) prov.jellyfin={strict_id_matching:getPairProviderStrictValue(state, "jellyfin")};
  if(useEm) prov.emby={strict_id_matching:getPairProviderStrictValue(state, "emby")};
  if(useTr){
    const trSrc=pp.trakt||{};
    const colOn=!!trSrc.history_collection;
    const histIgnoreDropped=!!trSrc.history_ignore_dropped_shows;
    let types=Array.isArray(trSrc.history_collection_types)?trSrc.history_collection_types.map(x=>String(x).trim().toLowerCase()):[];
    types=types.filter(x=>x==="movies"||x==="shows");
    if(colOn && !types.length) types=["movies"];
    if(colOn || types.length || histIgnoreDropped){
      prov.trakt={};
      if(colOn || types.length){
        prov.trakt.history_collection=colOn;
        if(types.length) prov.trakt.history_collection_types=types;
      }
      if(histIgnoreDropped) prov.trakt.history_ignore_dropped_shows=true;
    }
  }
  if(isMDBList(src)||isMDBList(dst)){
    const mdSrc=pp.mdblist||{};
    const histIgnoreDropped=!!mdSrc.history_ignore_dropped_shows;
    if(histIgnoreDropped){
      prov.mdblist={history_ignore_dropped_shows:true};
    }
  }
  if(isSimkl(src)||isSimkl(dst)){
    const smSrc=pp.simkl||{};
    const histIgnoreDropped=!!smSrc.history_ignore_dropped_shows;
    if(histIgnoreDropped){
      prov.simkl={history_ignore_dropped_shows:true};
    }
  }
  if(Object.keys(prov).length) payload.providers=prov;
  const eid=wrap.dataset&&wrap.dataset.editingId?String(wrap.dataset.editingId||""):"";if(eid)payload.id=eid;return payload;
}

// Save:
async function savePair(payload){
  try{if(payload?.id){const r=await fetch(`/api/pairs/${encodeURIComponent(payload.id)}`,{method:"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});if(r.ok)return{ok:true}}}catch{}
  try{const r=await fetch("/api/pairs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});if(r&&r.ok)return{ok:true}}catch{}
  if(typeof window.cxSavePair==="function"){try{const res=await Promise.resolve(window.cxSavePair(payload,payload.id||""));const ok=typeof res==="object"?res?.ok!==false&&!res?.error:res!==false;return{ok:!!ok}}catch(e){return{ok:false}}}
  return{ok:false}
}

export default{
  async mount(hostEl,props){
    ensurePairConfigStyles();
    hostEl.classList.add("pair-config-modal");
    hostEl.style.setProperty("--cxModalMaxW", "1280px");
    hostEl.style.setProperty("--cxModalMaxH", "92vh");
    hostEl.style.setProperty("--cxModalW", "min(var(--cxModalMaxW,1280px),calc(100vw - 64px))");
    hostEl.innerHTML=tpl();
    const wrap=ID("cx-modal",hostEl);
    const state=defaultState();
    state.feature="globals";
    wrap.__state=state;

    let pair=null;
    if(props?.pairOrId && typeof props.pairOrId==="object") pair=props.pairOrId;
    else if(props?.pairOrId) pair=await loadPairById(String(props.pairOrId));

    if(pair && typeof pair==="object"){
      const up=x=>String(x||"").toUpperCase();
      state.src=up(pair.source||pair.src||state.src);
      state.dst=up(pair.target||pair.dst||state.dst);
      state.src_instance=String(pair.source_instance||"default");
      state.dst_instance=String(pair.target_instance||"default");
      state.mode=pair.mode||state.mode;
      state.enabled=typeof pair.enabled==="boolean"?pair.enabled:true;
      const f=pair.features||{}, safe=(v,d)=>Object.assign({},d,v||{});
      state.options.watchlist=safe(f.watchlist,state.options.watchlist);
      state.options.history=safe(f.history,state.options.history);
      state.options.progress=safe(f.progress,state.options.progress);
      state.options.playlists=safe(f.playlists,state.options.playlists);
      const r0=state.options.ratings, rI=f.ratings||{};
      state.options.ratings=Object.assign({},r0,rI,{types:Array.isArray(rI.types)&&rI.types.length?rI.types:r0.types,mode:rI.mode||r0.mode,from_date:rI.from_date||r0.from_date||""});
      state.pairProviders = normalizePairProviders(pair.providers);
      wrap.dataset.editingId=pair?.id?String(pair.id):"";
    }

    await loadConfigBits(state);
    await loadProviders(state);
    await loadProviderInstances(state);

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
    queueMicrotask(() => applyHelpIcons(wrap, { QA }));
    ensureInlineFoot(hostEl);
    hostEl.__doSave=async()=>{
      await saveConfigBits(state);
      const payload=buildPayload(state,wrap);

      const feats=payload.features||{};
      const enabledKeys=Object.keys(feats).filter(k=>feats[k]?.enable);
      if(!enabledKeys.length){
        const ok=window.confirm("This pair has no enabled features.\nIt will not transfer any data.\nSave anyway?");
        if(!ok)return;
      }

      const res=await savePair(payload);
      if(!res.ok){alert("Save failed");return;}

      try{
        if(typeof window.loadPairs==="function"){await window.loadPairs(true)}
        else{
          const r=await fetch("/api/pairs",{cache:"no-store"});
          const arr=r.ok?await r.json():[];
          window.cx=window.cx||{};
          window.cx.pairs=Array.isArray(arr)?arr:[];
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
