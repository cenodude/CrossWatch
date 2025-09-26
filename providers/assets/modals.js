// modals.js — Configure Connection modal UI + helpers
(function(){
"use strict";

/* utils */
const G=(typeof window!=="undefined"?window:globalThis),ID=(x)=>document.getElementById(x),Q=(s,r=document)=>r.querySelector(s),QA=(s,r=document)=>Array.from(r.querySelectorAll(s)),log=(...a)=>{try{console.debug("[cx:modals]",...a)}catch(_){}},jclone=(o)=>JSON.parse(JSON.stringify(o||{}));
function _cxState(){const el=ID("cx-modal");return(el&&el.__state)||G.__cxState||null}


/* open/close */
async function openPairModal(pairOrId){
  try{
    let pair=(pairOrId&&typeof pairOrId==="object")?pairOrId:null;
    if(!pair){
      const id=String(pairOrId||"");
      try{const r=await fetch("/api/pairs?cb="+Date.now(),{cache:"no-store"});const arr=r.ok?(await r.json()):[];pair=Array.isArray(arr)?arr.find(p=>String(p.id)===id):null}catch(_){}
      if(!pair&&Array.isArray(G.cx?.pairs)) pair=G.cx.pairs.find(p=>String(p.id)===id)||null;
    }
    const wrap=await cxEnsureCfgModal(pair||null);
    try{wrap.dataset.editingId=pair?.id?String(pair.id):"";if(wrap.__saving)wrap.__saving=false}catch(_){}
    if(typeof G.cxOpenModalFor==="function") await G.cxOpenModalFor(pair||null,pair?.id||null);
    wrap.classList.remove("hidden"); document.body.classList.add("cx-modal-open");
    try{ensureInlineFootNoBar(wrap)}catch(_){}
    try{blockForeignSaveBars(wrap)}catch(_){}
    return true;
  }catch(e){console.warn("[cx] openPairModal failed",e);alert("Could not open the editor.");return false}
}
function closePairModal(persist){
  try{
    const m=ID("cx-modal"); if(!m) return;
    try{if(persist&&m.__persistCurrentFeature) m.__persistCurrentFeature()}catch(_){}
    m.classList.add("hidden"); document.body.classList.remove("cx-modal-open");
    try{if(m.__unblockForeign) m.__unblockForeign()}catch(_){}
    try{m.dataset.editingId=""; if(m.__state) m.__state.visited=new Set()}catch(_){}
  }catch(e){console.warn("[cx] closePairModal",e)}
}
G.openPairModal=openPairModal; G.closePairModal=closePairModal; G.cxCloseModal=closePairModal;
if(typeof G.cxEditPair!=="function") G.cxEditPair=(id)=>openPairModal(id);

/* provider helpers */
const _is=(v,name)=>String(v||"").trim().toLowerCase()===name,__isSimkl=(v)=>_is(v,"simkl"),__isJellyfin=(v)=>_is(v,"jellyfin");
function __pairHasSimkl(state){try{if(__isSimkl(state?.src)||__isSimkl(state?.dst))return true;const src=ID("cx-src")?.value||ID("cx-src-display")?.dataset.value||"",dst=ID("cx-dst")?.value||ID("cx-dst-display")?.dataset.value||"";if(__isSimkl(src)||__isSimkl(dst))return true;const sTxt=ID("cx-src-display")?.textContent||"",dTxt=ID("cx-dst-display")?.textContent||"";return /simkl/i.test(sTxt)||/simkl/i.test(dTxt)}catch(_){return false}}
function __pairHasJellyfin(state){try{if(__isJellyfin(state?.src)||__isJellyfin(state?.dst))return true;const src=ID("cx-src")?.value||ID("cx-src-display")?.dataset.value||"",dst=ID("cx-dst")?.value||ID("cx-dst-display")?.dataset.value||"";if(__isJellyfin(src)||__isJellyfin(dst))return true;const sTxt=ID("cx-src-display")?.textContent||"",dTxt=ID("cx-dst-display")?.textContent||"";return /jellyfin/i.test(sTxt)||/jellyfin/i.test(dTxt)}catch(_){return false}}
function _provIconPath(name){const key=String(name||"").trim().toUpperCase(),map={PLEX:"PLEX",JELLYFIN:"JELLYFIN",SIMKL:"SIMKL",TRAKT:"TRAKT"},file=map[key]||key;return `/assets/${file}.svg`}
function _provLogoHTML(name,label){const src=_provIconPath(name),alt=(label||name||"Provider")+" logo";return `<span class="prov-wrap"><img class="prov-logo" src="${src}" alt="${alt}" width="36" height="36" onerror="this.style.display='none'; this.nextElementSibling.style.display='inline-block'"/><span class="prov-fallback" style="display:none">${label||name||"—"}</span></span>`}

/* CSS */
(function(){
  if (document.getElementById("cx-modal-core-css")) return;
  const st = document.createElement("style"); st.id = "cx-modal-core-css";
  st.textContent = `
#cx-modal .modal-shell{width:min(960px,calc(100vw - 56px));max-height:calc(100vh - 56px);overflow:hidden;border-radius:18px;box-shadow:0 30px 80px rgba(0,0,0,.55);display:flex;flex-direction:column}
#cx-modal .cx-card{display:flex;flex-direction:column;height:100%;background:rgba(20,22,28,.96)}
#cx-modal .cx-body{padding:14px 16px 0;overflow:auto;padding-bottom:var(--cxFooterH,96px)}
#cx-modal .cx-head{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;border-bottom:1px solid rgba(255,255,255,.08)}
#cx-modal .title-wrap{display:flex;align-items:center;gap:10px}
#cx-modal .app-logo{display:inline-flex;align-items:center;justify-content:center;font-size:24px;line-height:1;color:#fff;width:28px;height:28px;border-radius:6px;background:linear-gradient(90deg,#7c4dff,#00d4ff)}
#cx-modal .app-name{font-weight:800;letter-spacing:.2px}
#cx-modal .app-sub{opacity:.7;font-size:12px}
body.cx-modal-open #save-fab,body.cx-modal-open #save-frost,body.cx-modal-open .savebar,body.cx-modal-open .actions.sticky{display:none!important}
#cx-modal.cx-draggable .modal-shell{position:fixed;left:50%;top:50%;transform:translate(-50%,-50%)}
#cx-modal[data-dragging=true] .modal-shell{transform:none;transition:none}
#cx-modal .cx-head{cursor:move;user-select:none}
#cx-modal .cx-head .switch,#cx-modal .cx-head input,#cx-modal .cx-head button{cursor:auto}
#cx-modal .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px 16px}
#cx-modal .grid2.compact{grid-template-columns:1fr 1fr;gap:8px 10px}
#cx-modal .cx-top{align-items:start}
#cx-modal .cx-row{margin:6px 0}
#cx-modal .field label{display:block;opacity:.8;font-size:12px;margin-bottom:6px}
#cx-modal .input{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:10px;padding:10px 12px;height:36px}
#cx-modal .input.static{display:flex;align-items:center;height:36px}
#cx-modal .cx-st-row{align-items:end}
#cx-modal .flow-card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:10px 12px}
#cx-modal .flow-title{font-weight:700;margin-bottom:6px;opacity:.9}
.flow-rail.pretty{display:flex;align-items:center;gap:10px;--flowColor:currentColor}
.flow-rail.pretty .token{min-width:40px;text-align:center;opacity:.9;display:flex;align-items:center;justify-content:center}
.flow-rail.pretty .arrow{position:relative;display:block;flex:1;min-width:120px;height:12px}
.flow-rail.pretty .arrow::before{content:"";position:absolute;left:0;right:0;top:50%;height:2px;background:var(--flowColor);opacity:.25;transform:translateY(-50%);border-radius:2px}
.flow-rail.pretty .dot.flow{position:absolute;top:50%;transform:translateY(-50%);width:8px;height:8px;border-radius:50%;background:var(--flowColor);opacity:.6}
.prov-wrap{display:inline-flex;align-items:center;justify-content:center;width:40px;height:40px}
.prov-logo{width:36px;height:36px;object-fit:contain;display:block;filter:drop-shadow(0 2px 6px rgba(0,0,0,.35))}
.prov-fallback{font-size:12px;opacity:.8}
#cx-modal .cx-actions{position:sticky;bottom:0;z-index:3;display:flex;justify-content:flex-end;align-items:center;gap:12px;padding:14px 16px;border-top:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(20,22,28,.85),rgba(20,22,28,.98));backdrop-filter:blur(2px);min-height:72px}
#cx-modal .cx-btn{appearance:none;cursor:pointer;user-select:none;font-weight:800;text-transform:uppercase;letter-spacing:.02em;border-radius:14px;padding:12px 18px;line-height:1.1;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.07);color:#fff;transition:transform .12s,box-shadow .12s,opacity .12s}
#cx-modal .cx-btn:hover{transform:translateY(-1px);box-shadow:0 8px 20px rgba(0,0,0,.35)}
#cx-modal .cx-btn:active{transform:none}
#cx-modal .cx-btn.primary{background:linear-gradient(135deg,#4c7dff,#6b9bff 40%,#8ab0ff 100%);border-color:#7aa0ff66;color:#fff;box-shadow:0 10px 28px rgba(43,88,255,.35),0 0 14px rgba(122,160,255,.28);position:relative;overflow:hidden}
#cx-modal .cx-btn[disabled]{opacity:.6;cursor:default}
#cx-modal .simkl-alert{margin-top:12px;padding:12px 14px;border-radius:14px;background:linear-gradient(180deg,rgba(255,170,54,.12),rgba(255,170,54,.05));border:1px solid rgba(255,170,54,.55);box-shadow:0 8px 28px rgba(255,170,54,.1),0 0 0 1px rgba(255,170,54,.25) inset}
#cx-modal .simkl-alert .title{display:flex;align-items:center;gap:8px;font-weight:700;letter-spacing:.2px;margin:0 0 6px;color:#ffd79a}
#cx-modal .simkl-alert .title .ic{font-size:14px;line-height:1;transform:translateY(-1px)}
#cx-modal .simkl-alert .body{color:#fff;opacity:.92}
#cx-modal .simkl-alert .mini{margin-top:8px;font-size:12.5px;opacity:.85}
#cx-modal .cx-top{margin-bottom:0}
#cx-modal .feature-tabs{margin:6px 0 0}
#cx-modal .cx-main{margin-top:6px}
#cx-modal .cx-main .left .panel{margin-top:0;padding-top:8px}
#cx-modal .cx-mode-row{margin:6px 0 4px}
#cx-modal .feature-tabs + .panel{border-top-left-radius:12px;border-top-right-radius:12px}
.material-symbols-rounded{font-variation-settings:'FILL' 1,'wght' 600,'GRAD' 0,'opsz' 24}
#cx-modal,#cx-modal .cx-card{color-scheme:dark}
#cx-modal select.input{background:rgba(255,255,255,.06);color:#fff;border:1px solid rgba(255,255,255,.12)}
#cx-modal select.input:focus{outline:2px solid rgba(124,77,255,.55)}
#cx-modal select.input option{background:#14161c;color:#fff}
#cx-modal input[type="date"].input{color-scheme:dark}
`;
  document.head.appendChild(st);
})();


/* footer */
function ensureInlineFootNoBar(modal){
  if(!modal) return; const card=Q(".cx-card",modal)||modal; let bar=Q(".cx-actions",card);
  if(!bar){
    bar=document.createElement("div"); bar.className="cx-actions";
    const btnCancel=document.createElement("button"); btnCancel.className="cx-btn"; btnCancel.textContent="Cancel"; btnCancel.addEventListener("click",()=>closePairModal(false));
    const btnSave=document.createElement("button"); btnSave.className="cx-btn primary"; btnSave.id="cx-inline-save"; btnSave.textContent="Save";
    btnSave.addEventListener("click",async()=>{const b=ID("cx-inline-save"); if(!b)return; const old=b.textContent; b.disabled=true;b.textContent="Saving…"; try{await modal.__doSave?.()}finally{b.disabled=false;b.textContent=old}});
    bar.append(btnCancel,btnSave); card.appendChild(bar);
    const body=Q(".cx-body",card),apply=()=>{const h=Math.max(56,Math.round(bar.getBoundingClientRect().height||72)); if(body) body.style.setProperty("--cxFooterH",(h+20)+"px")};
    apply(); const ro=new ResizeObserver(apply); ro.observe(bar); addEventListener("resize",apply,{passive:true}); card.__footerPaddingRO=ro;
  }
}
/* block page save bars */
function blockForeignSaveBars(wrap){
  try{
    if(!wrap) return; if(wrap.__foreignBlocked===true){document.body.classList.add("cx-modal-open");return}
    document.body.classList.add("cx-modal-open"); const kills=[],ids=["save-fab","save-frost","savebar"];
    for(const id of ids){const n=document.getElementById(id); if(n&&n.parentNode){const anchor=document.createComment("cx-anchor-"+id); n.parentNode.insertBefore(anchor,n); n.parentNode.removeChild(n); kills.push({node:n,anchor})}}
    wrap.__foreignBlocked=true; wrap.__foreignKills=kills;
    wrap.__unblockForeign=()=>{try{for(const {node,anchor} of (wrap.__foreignKills||[])){if(anchor&&anchor.parentNode){anchor.replaceWith(node)}}}finally{wrap.__foreignKills=null;wrap.__foreignBlocked=false;document.body.classList.remove("cx-modal-open");wrap.__unblockForeign=null}};
  }catch(_){}
}

/* drag */
function installModalDrag(wrap){
  if(!wrap||wrap.__dragInstalled) return; wrap.__dragInstalled=true; wrap.classList.add("cx-draggable");
  const shell=Q(".modal-shell",wrap),head=Q(".cx-head",wrap); if(!shell||!head) return;
  const pos=wrap.__dragPos||null; if(pos&&Number.isFinite(pos.left)&&Number.isFinite(pos.top)){shell.style.position="fixed";shell.style.left=pos.left+"px";shell.style.top=pos.top+"px";shell.style.transform="none"}
  let dragging=false,sx=0,sy=0,startL=0,startT=0,pid=null; const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
  const bounds=()=>{const vw=Math.max(320,innerWidth||0),vh=Math.max(240,innerHeight||0),r=shell.getBoundingClientRect();return{minL:24,maxL:Math.max(24,vw-r.width-24),minT:24,maxT:Math.max(24,vh-r.height-24)}};
  function start(e){if(e.button!==0)return; if(e.target.closest("input,select,textarea,button,.switch"))return; dragging=true; pid=e.pointerId??null; head.setPointerCapture?.(pid); const r=shell.getBoundingClientRect(); startL=r.left; startT=r.top; sx=e.clientX; sy=e.clientY; shell.style.position="fixed"; shell.style.left=startL+"px"; shell.style.top=startT+"px"; shell.style.transform="none"; wrap.dataset.dragging="true"; e.preventDefault()}
  function move(e){if(!dragging)return; const dx=e.clientX-sx,dy=e.clientY-sy,lim=bounds(); shell.style.left=clamp(startL+dx,lim.minL,lim.maxL)+"px"; shell.style.top=clamp(startT+dy,lim.minT,lim.maxT)+"px"}
  function end(){if(!dragging)return; dragging=false; wrap.dataset.dragging="false"; head.releasePointerCapture?.(pid); pid=null; const r=shell.getBoundingClientRect(); wrap.__dragPos={left:Math.round(r.left),top:Math.round(r.top)}}
  head.addEventListener("pointerdown",start,true); head.addEventListener("pointermove",move,true); head.addEventListener("pointerup",end,true); head.addEventListener("pointercancel",end,true);
  addEventListener("resize",()=>{const lim=bounds(),r=shell.getBoundingClientRect(),nx=Math.max(lim.minL,Math.min(lim.maxL,r.left)),ny=Math.max(lim.minT,Math.min(lim.maxT,r.top)); shell.style.left=nx+"px"; shell.style.top=ny+"px"; wrap.__dragPos={left:nx,top:ny}},{passive:true});
}

/* ratings summary pills */
function updateRtCompactSummary(){
  const m=ID("cx-modal"),st=m?.__state,rt=st?.options?.ratings||{},det=ID("cx-rt-adv"),sum=det?.querySelector("summary"); if(!sum)return;
  const types=Array.isArray(rt.types)&&rt.types.length?rt.types.join(", "):"movies, shows",modeMap={only_new:"New since last sync",from_date:"From a date",all:"Everything"},mode=modeMap[String(rt.mode||"only_new")]||"New since last sync",from=(rt.mode==="from_date"&&rt.from_date)?`From ${rt.from_date}`:"";
  sum.innerHTML=`<span class="pill">Types: ${types}</span><span class="summary-gap">•</span><span class="pill">Mode: ${mode}${from?` — ${from}`:""}</span>`;
  sum.setAttribute("aria-expanded",det.open?"true":"false");
}

/* event micro-binder */
function cxBindCfgEvents(){
  try{const _m1=ID("cx-mode-one"),_m2=ID("cx-mode-two"),_b=(el)=>{if(!el)return;el.addEventListener("change",()=>{try{ID("cx-modal").__updateFlow?.(true)}catch(_){}})};_b(_m1);_b(_m2)}catch(_){}
  const ids=["cx-src","cx-dst","cx-mode-one","cx-mode-two","cx-wl-enable","cx-wl-add","cx-wl-remove","cx-rt-enable","cx-rt-add","cx-rt-remove","cx-rt-type-all","cx-rt-type-movies","cx-rt-type-shows","cx-rt-type-seasons","cx-rt-type-episodes","cx-rt-mode","cx-rt-from-date","cx-hs-enable","cx-hs-add","cx-hs-remove","cx-pl-enable","cx-pl-add","cx-pl-remove","cx-enabled","cx-jf-wl-mode-fav","cx-jf-wl-mode-pl","cx-jf-wl-pl-name"];
  ids.forEach((id)=>{const el=ID(id); if(!el)return; el.addEventListener("change",(ev)=>{const id=ev.target?.id||"",map={"cx-wl-enable":"cx-wl-remove","cx-rt-enable":"cx-rt-remove","cx-hs-enable":"cx-hs-remove","cx-pl-enable":"cx-pl-remove"}; if(map[id]){const rm=ID(map[id]); if(rm){rm.disabled=!ev.target.checked; if(!ev.target.checked) rm.checked=false}} if(id==="cx-enabled") ID("cx-modal")?.__updateFlowClasses?.(); try{updateRtCompactSummary()}catch(_){}
    if(id==="cx-jf-wl-mode-fav"||id==="cx-jf-wl-mode-pl"){const usePl=ID("cx-jf-wl-mode-pl")?.checked,inp=ID("cx-jf-wl-pl-name"); if(inp){inp.disabled=!usePl; if(!usePl&&!inp.value) inp.value="Watchlist"}}})});
}
G.cxBindCfgEvents=G.cxBindCfgEvents||cxBindCfgEvents;

/* About overlay */
async function openAbout(){
  try{
    const r=await fetch("/api/version?cb="+Date.now(),{cache:"no-store"}),j=r.ok?await r.json():{},cur=(j.current??"0.0.0").toString().trim(),latest=(j.latest??"").toString().trim()||null,url=j.html_url||"https://github.com/cenodude/crosswatch/releases",upd=!!j.update_available;
    const verEl=ID("about-version"); if(verEl){verEl.textContent=`Version ${j.current}`; verEl.dataset.version=cur}
    const headerVer=ID("app-version"); if(headerVer){headerVer.textContent=`Version ${cur}`; headerVer.dataset.version=cur}
    const relEl=ID("about-latest"); if(relEl){relEl.href=url; relEl.textContent=latest?`v${latest}`:"Releases"; relEl.setAttribute("aria-label",latest?`Latest release v${latest}`:"Releases")}
    const updEl=ID("about-update"); if(updEl){updEl.classList.add("badge","upd"); if(upd&&latest){updEl.textContent=`Update ${latest} available`; updEl.classList.remove("hidden","reveal"); void updEl.offsetWidth; updEl.classList.add("reveal")}else{updEl.textContent=""; updEl.classList.add("hidden"); updEl.classList.remove("reveal")}}
    try{
      const mr=await fetch("/api/modules/versions?cb="+Date.now(),{cache:"no-store"}); if(!mr.ok) throw new Error(String(mr.status)); const mv=await mr.json();
      const body=document.querySelector('#about-backdrop .modal-card .modal-body'),firstGrid=body?.querySelector('.about-grid'); if(!body||!firstGrid) throw new Error("about body/grid missing");
      body.querySelector("#about-mods")?.remove();

      // CSS (once)
      if(!ID("about-mods-style")){
        const style=document.createElement("style"); style.id="about-mods-style";
        style.textContent=[
          `.mods-card{margin-top:16px;border:1px solid rgba(255,255,255,.12);border-radius:12px;background:rgba(255,255,255,.03)}`,
          `.fold-head{width:100%;display:flex;align-items:center;justify-content:space-between;padding:10px 12px;background:rgba(255,255,255,.02);border:0;border-bottom:1px solid rgba(255,255,255,.06);border-radius:12px 12px 0 0;color:#fff;font-weight:700;cursor:pointer}`,
          `.fold-head .chev{font-family:"Material Symbols Rounded";transition:transform .18s ease}`,
          `.fold.open .fold-head .chev{transform:rotate(180deg)}`,
          `.fold-body{overflow:hidden;opacity:0;transform:translateY(-2px);height:0}`,
          `.fold.open .fold-body{opacity:1;transform:none}`,
          `.mods-section{border-top:1px solid rgba(255,255,255,.06)}`,
          `.mods-section:first-child{border-top:0}`,
          `.fold-sub{width:100%;display:flex;align-items:center;justify-content:space-between;background:transparent;border:0;color:#fff;font-weight:600;padding:10px 12px;cursor:pointer;opacity:.9}`,
          `.mods-rows{display:grid;grid-template-columns:minmax(120px,160px) 1fr auto;gap:8px 12px;padding:0 12px 12px}`,
          `.mods-name{opacity:.9}`, `.mods-key{opacity:.7}`, `.mods-ver{justify-self:end;font-variant-numeric:tabular-nums;opacity:.95}`,
          `@media (max-width:520px){.mods-rows{grid-template-columns:1fr auto}.mods-key{display:none}}`,
          `@media (prefers-reduced-motion:reduce){.fold-body{transition:none !important}}`
        ].join("");
        document.head.appendChild(style);
      }

      // Helpers
      const animFold=(wrap,open)=>{
        const b=wrap.querySelector(".fold-body"); if(!b) return;
        b.hidden=false;
        const to=open?b.scrollHeight:0;
        b.style.transition="height 200ms ease,opacity 200ms ease,transform 200ms ease";
        requestAnimationFrame(()=>{ b.style.height=to+"px"; b.style.opacity=open?"1":"0"; b.style.transform=open?"none":"translateY(-2px)"; });
        const end=()=>{ b.style.transition=""; if(open){ b.style.height="auto"; } else { b.style.height="0"; b.hidden=true; } b.removeEventListener("transitionend",end); };
        b.addEventListener("transitionend",end);
        wrap.classList.toggle("open",open);
        const hd=wrap.querySelector(".fold-head,.fold-sub"); if(hd) hd.setAttribute("aria-expanded",String(open));
      };
      const makeSection=(label)=>{
        const sec=document.createElement("section"); sec.className="mods-section fold";
        const h=document.createElement("button"); h.type="button"; h.className="fold-sub"; h.innerHTML=`<span>${label}</span><span class="chev">expand_more</span>`; h.setAttribute("aria-expanded","false");
        const b=document.createElement("div"); b.className="fold-body"; b.hidden=true;
        const rows=document.createElement("div"); rows.className="mods-rows"; b.appendChild(rows);
        h.addEventListener("click",()=>animFold(sec,!sec.classList.contains("open")));
        sec.append(h,b);
        return {sec,rows};
      };
      const addRow=(rows,label,key,ver)=>{
        const n=document.createElement("div"); n.className="mods-name"; n.textContent=label.replace(/^_+/,"");
        const k=document.createElement("div"); k.className="mods-key"; k.textContent=key;
        const v=document.createElement("div"); v.className="mods-ver"; v.textContent=ver?`v${ver}`:"v0.0.0";
        rows.append(n,k,v);
      };

      // Build card (default collapsed)
      const card=document.createElement("div"); card.id="about-mods"; card.className="mods-card fold";
      const head=document.createElement("button"); head.type="button"; head.className="fold-head"; head.setAttribute("aria-expanded","false"); head.innerHTML=`<span>Modules</span><span class="chev">expand_more</span>`;
      const bodyFold=document.createElement("div"); bodyFold.className="fold-body"; bodyFold.hidden=true;

      // Sections (default collapsed)
      const auth=makeSection("Authentication Providers");
      const sync=makeSection("Synchronization Providers");
      const groups=mv?.groups||{};
      Object.entries(groups.AUTH||{}).forEach(([n,v])=>addRow(auth.rows,n,n,v));
      Object.entries(groups.SYNC||{}).forEach(([n,v])=>addRow(sync.rows,n,n,v));

      bodyFold.append(auth.sec,sync.sec);
      card.append(head,bodyFold);
      head.addEventListener("click",()=>animFold(card,!card.classList.contains("open")));

      firstGrid.insertAdjacentElement("afterend",card);
    }catch(e){console.warn("[about] modules render failed",e)}
  }catch(e){console.warn("[about] openAbout failed",e)}
  const bb=ID("about-backdrop"); if(bb){bb.classList.remove("hidden"); document.body.classList.add("cx-modal-open"); document.body.dataset.aboutOpen="1";}
}
function closeAbout(ev){
  if(ev&&ev.type==="click"&&ev.currentTarget!==ev.target) return;
  ID("about-backdrop")?.classList.add("hidden");
  delete document.body.dataset.aboutOpen;
  const stillOpen=document.querySelector('#cx-modal:not(.hidden),#about-backdrop:not(.hidden),#upd-modal:not(.hidden),.modal.open,[data-modal-open="1"]');
  if(!stillOpen) document.body.classList.remove("cx-modal-open");
}

/* expose to header FastAPI */
G.openAbout=openAbout;
G.closeAbout=closeAbout;


/* ensure modal UI */
async function cxEnsureCfgModal(pairInit=null){
  QA("#cx-modal").slice(1).forEach(n=>{try{n.remove()}catch(_){}});

  let wrap=ID("cx-modal"); const firstCreate=!wrap;
  if(!wrap){
    wrap=document.createElement("div"); wrap.id="cx-modal"; wrap.className="modal-backdrop cx-wide hidden";
    wrap.innerHTML=`
      <div class="modal-shell"><div class="cx-card">
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
              <div class="cx-row grid2 cx-st-row">
                <div class="field"><label>Source</label><div id="cx-src-display" class="input static"></div><select id="cx-src" class="hidden"></select></div>
                <div class="field"><label>Target</label><div id="cx-dst-display" class="input static"></div><select id="cx-dst" class="hidden"></select></div>
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
      </div></div>`;
    document.body.appendChild(wrap); try{installModalDrag(wrap)}catch(_){}
    wrap.__state={providers:[],src:null,dst:null,feature:"globals",mode:"one-way",enabled:true,
      options:{watchlist:{enable:true,add:true,remove:false},ratings:{enable:false,add:false,remove:false,types:["movies","shows"],mode:"only_new",from_date:""},history:{enable:false,add:false,remove:false},playlists:{enable:false,add:true,remove:false}},
      jellyfin:{watchlist:{mode:"favorites",playlist_name:"Watchlist"}},globals:null,visited:new Set()};
    G.__cxState=wrap.__state; if(typeof G.cxBindCfgEvents==="function") G.cxBindCfgEvents();
  }
  const state=wrap.__state;

  /* hydrate from pair */
  if(pairInit&&typeof pairInit==="object"){
    const up=(x)=>String(x||"").toUpperCase();
    state.src=up(pairInit.source||pairInit.src||state.src);
    state.dst=up(pairInit.target||pairInit.dst||state.dst);
    state.mode=(pairInit.mode||state.mode||"one-way"); state.enabled=(typeof pairInit.enabled==="boolean")?pairInit.enabled:true;
    const f=pairInit.features||{},safe=(v,d)=>Object.assign({},d,v||{});
    state.options.watchlist=safe(f.watchlist,state.options.watchlist);
    state.options.history=safe(f.history,state.options.history);
    state.options.playlists=safe(f.playlists,state.options.playlists);
    const r0=state.options.ratings,rI=f.ratings||{};
    state.options.ratings=Object.assign({},r0,rI,{types:Array.isArray(rI.types)&&rI.types.length?rI.types:r0.types,mode:rI.mode||r0.mode,from_date:(rI.from_date||r0.from_date||"")});
  }

  /* persist/snapshot helpers */
  wrap.__persistCurrentFeature=function(){
    const st=wrap.__state||{},f=st.feature,pick=(on,add,rem)=>({enable:!!on?.checked,add:!!add?.checked,remove:!!rem?.checked});
    if(f==="watchlist") st.options.watchlist=pick(ID("cx-wl-enable"),ID("cx-wl-add"),ID("cx-wl-remove"));
    if(f==="ratings"){const base=pick(ID("cx-rt-enable"),ID("cx-rt-add"),ID("cx-rt-remove")),allOn=!!ID("cx-rt-type-all")?.checked,types=allOn?["movies","shows","seasons","episodes"]:["movies","shows","seasons","episodes"].filter(t=>ID(`cx-rt-type-${t}`)?.checked),mode=(ID("cx-rt-mode")?.value||"only_new").toString(),from_date=(ID("cx-rt-from-date")?.value||"").trim(); st.options.ratings=Object.assign({},st.options.ratings||{},base,{types,mode,from_date})}
    if(f==="history")   st.options.history  =pick(ID("cx-hs-enable"),ID("cx-hs-add"),ID("cx-hs-remove"));
    if(f==="playlists") st.options.playlists=pick(ID("cx-pl-enable"),ID("cx-pl-add"),ID("cx-pl-remove"));
    try{if(__pairHasJellyfin(st)){const modePl=!!ID("cx-jf-wl-mode-pl")?.checked,name=(ID("cx-jf-wl-pl-name")?.value||"").trim()||"Watchlist"; st.jellyfin=st.jellyfin||{}; st.jellyfin.watchlist={mode:(modePl?"playlist":"favorites"),playlist_name:name}}}catch(_){}
    st.visited.add(f);
  };
  wrap.__buildPairPayload=function(){
    const st=_cxState()||{},src=st.src||ID("cx-src")?.value||ID("cx-src-display")?.dataset.value||"",dst=st.dst||ID("cx-dst")?.value||ID("cx-dst-display")?.dataset.value||"",modeTwo=!!ID("cx-mode-two")?.checked,enabled=!!ID("cx-enabled")?.checked;
    const get=(k)=>Object.assign({enable:false,add:false,remove:false},(st.options||{})[k]||{});
    const payload={source:src,target:dst,enabled,mode:modeTwo?"two-way":"one-way",features:{watchlist:get("watchlist"),ratings:get("ratings"),history:get("history"),playlists:get("playlists")}};
    const eid=(wrap.dataset&&wrap.dataset.editingId)?String(wrap.dataset.editingId||""):""; if(eid) payload.id=eid; return payload;
  };
  wrap.__saveConfigBits=async function(){
    try{
      const cur=await fetch("/api/config",{cache:"no-store"}).then(r=>r.ok?r.json():{}),cfg=(typeof structuredClone==="function")?structuredClone(cur||{}):jclone(cur||{});
      if(ID("gl-dry")){const s={dry_run:!!ID("gl-dry")?.checked,verify_after_write:!!ID("gl-verify")?.checked,drop_guard:!!ID("gl-drop")?.checked,allow_mass_delete:!!ID("gl-mass")?.checked,tombstone_ttl_days:Math.max(0,parseInt(ID("gl-ttl")?.value||"0",10)||0),include_observed_deletes:!!ID("gl-observed")?.checked}; cfg.sync=Object.assign({},cfg.sync||{},s)}
      const st=wrap.__state||{},hasJF=String(st.src||"").toUpperCase()==="JELLYFIN"||String(st.dst||"").toUpperCase()==="JELLYFIN";
      if(hasJF){const jf=Object.assign({},cfg.jellyfin||{}),mode=ID("cx-jf-wl-mode-pl")?.checked?"playlist":ID("cx-jf-wl-mode-fav")?.checked?"favorites":(st.jellyfin?.watchlist?.mode||"favorites"),name=(ID("cx-jf-wl-pl-name")?.value||st.jellyfin?.watchlist?.playlist_name||"Watchlist").trim()||"Watchlist"; jf.watchlist=Object.assign({},jf.watchlist||{},{mode,playlist_name:name}); cfg.jellyfin=jf}
      const res=await fetch("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)}); if(!res.ok) throw new Error(`POST /api/config ${res.status}`);
    }catch(e){console.warn("[cx] saving config bits failed",e)}
  };
  wrap.__snapshotAll=function(){
    const st=wrap.__state||{},pick=(on,add,rem)=>({enable:!!on?.checked,add:!!add?.checked,remove:!!rem?.checked});
    const wl_on=ID("cx-wl-enable"),wl_add=ID("cx-wl-add"),wl_rem=ID("cx-wl-remove"); if(wl_on||wl_add||wl_rem) st.options.watchlist=pick(wl_on,wl_add,wl_rem);
    const rt_on=ID("cx-rt-enable"),rt_add=ID("cx-rt-add"),rt_rem=ID("cx-rt-remove"); if(rt_on||rt_add||rt_rem){const base=pick(rt_on,rt_add,rt_rem),allOn=!!ID("cx-rt-type-all")?.checked,types=allOn?["movies","shows","seasons","episodes"]:["movies","shows","seasons","episodes"].filter(t=>ID(`cx-rt-type-${t}`)?.checked),mode=(ID("cx-rt-mode")?.value||"only_new").toString(),from_date=(ID("cx-rt-from-date")?.value||"").trim(); st.options.ratings=Object.assign({},st.options.ratings||{},base,{types,mode,from_date})}
    const hs_on=ID("cx-hs-enable"),hs_add=ID("cx-hs-add"),hs_rem=ID("cx-hs-remove"); if(hs_on||hs_add||hs_rem) st.options.history=pick(hs_on,hs_add,hs_rem);
    const pl_on=ID("cx-pl-enable"),pl_add=ID("cx-pl-add"),pl_rem=ID("cx-pl-remove"); if(pl_on||pl_add||pl_rem) st.options.playlists=pick(pl_on,pl_add,pl_rem);
    if(__pairHasJellyfin(st)){const mode=ID("cx-jf-wl-mode-pl")?.checked?"playlist":"favorites",name=(ID("cx-jf-wl-pl-name")?.value||"").trim()||"Watchlist"; st.jellyfin=st.jellyfin||{}; st.jellyfin.watchlist={mode,playlist_name:name}}
  };
  wrap.__doSave=async function(){
    if(wrap.__saving) return; wrap.__saving=true;
    const finish=(persist)=>{try{persist&&wrap.__persistCurrentFeature?.()}catch(_){}
      try{wrap.classList.add("hidden")}catch(_){}
      try{document.body.classList.remove("cx-modal-open")}catch(_){}
      try{wrap.dataset.editingId=""; if(wrap.__state) wrap.__state.visited=new Set()}catch(_){}
      try{wrap.__unblockForeign&&wrap.__unblockForeign()}catch(_){}
      wrap.__saving=false};
    try{
      wrap.__persistCurrentFeature?.(); wrap.__snapshotAll?.(); await wrap.__saveConfigBits?.();
      const payload=wrap.__buildPairPayload?.(); if(!payload){console.warn("[cx] no payload to save"); finish(false); return}
      if(typeof G.cxSavePair==="function"){const res=await Promise.resolve(G.cxSavePair(payload,payload.id||"")); const ok=(typeof res==="object")?(res?.ok!==false&&!res?.error):(res!==false); if(ok) finish(true); else console.warn("[cx] cxSavePair failed; modal stays open")}else{console.warn("[cx] cxSavePair missing; not writing pairs to config.json")}
    }catch(e){console.warn("[cx] save failed:",e)}finally{wrap.__saving=false}
  };

  /* config bits */
  const DEFAULT_GLOBALS={dry_run:false,verify_after_write:false,drop_guard:false,allow_mass_delete:true,tombstone_ttl_days:30,include_observed_deletes:true}; state.globals={...DEFAULT_GLOBALS};
  try{
    const cfg=await fetch("/api/config?cb="+Date.now(),{cache:"no-store"}).then(r=>r.ok?r.json():{}),s=cfg?.sync||{};
    state.globals={dry_run:!!s.dry_run,verify_after_write:!!s.verify_after_write,drop_guard:!!s.drop_guard,allow_mass_delete:!!s.allow_mass_delete,tombstone_ttl_days:Number.isFinite(s.tombstone_ttl_days)?s.tombstone_ttl_days:DEFAULT_GLOBALS.tombstone_ttl_days,include_observed_deletes:!!s.include_observed_deletes};
    const jf=cfg?.jellyfin||{},wl=jf.watchlist||{}; state.jellyfin={watchlist:{mode:(wl.mode==="playlist"||wl.mode==="favorites")?wl.mode:"favorites",playlist_name:(wl.playlist_name||"Watchlist")}};
  }catch(_){}

  /* providers & tabs */
  const byName=(n)=>state.providers.find(p=>p.name===n);
  const commonFeatures=()=>!state.src||!state.dst?[]:["watchlist","ratings","history","playlists"].filter(k=>byName(state.src)?.features?.[k]&&byName(state.dst)?.features?.[k]);
  const defaultForFeature=(k)=>k==="watchlist"?{enable:true,add:true,remove:false}:k==="playlists"?{enable:false,add:true,remove:false}:{enable:false,add:false,remove:false};
  function getOpts(key){if(!state.visited.has(key)){if(key==="ratings"){state.options.ratings=Object.assign({enable:false,add:false,remove:false,types:["movies","shows"],mode:"only_new",from_date:""},state.options.ratings||{})}else{state.options[key]=state.options[key]??defaultForFeature(key)} state.visited.add(key)} return state.options[key]}

  async function loadProviders(){
    try{const list=await fetch("/api/sync/providers?cb="+Date.now(),{cache:"no-store"}).then(r=>r.json()); state.providers=Array.isArray(list)?list:[]}
    catch(e){log("providers fetch failed; fallback"); state.providers=[
      {name:"PLEX",label:"Plex",features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true},version:"1.0.0"},
      {name:"SIMKL",label:"Simkl",features:{watchlist:true,ratings:true,history:true,playlists:false},capabilities:{bidirectional:true},version:"1.0.0"},
      {name:"TRAKT",label:"Trakt",features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true},version:"1.0.0"},
      {name:"JELLYFIN",label:"Jellyfin",features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true},version:"1.2.1"}]}
  }

  function renderProviderSelects(){
    const srcSel=Q("#cx-src",wrap),dstSel=Q("#cx-dst",wrap),srcLab=Q("#cx-src-display",wrap),dstLab=Q("#cx-dst-display",wrap);
    const opts=state.providers.map(p=>`<option value="${p.name}">${p.label}</option>`).join("");
    srcSel.innerHTML=`<option value="">Select…</option>${opts}`; dstSel.innerHTML=`<option value="">Select…</option>${opts}`;
    if(state.src) srcSel.value=state.src; if(state.dst) dstSel.value=state.dst;
    const updateLabels=()=>{const s=byName(srcSel.value),d=byName(dstSel.value); if(srcLab){srcLab.textContent=s?.label||"—"; srcLab.dataset.value=srcSel.value||""} if(dstLab){dstLab.textContent=d?.label||"—"; dstLab.dataset.value=dstSel.value||""}};
    srcSel.onchange=()=>{state.src=srcSel.value||null; updateLabels(); updateFlow(true); refreshTabs(); renderFlowWarnings()};
    dstSel.onchange=()=>{state.dst=dstSel.value||null; updateLabels(); updateFlow(true); refreshTabs(); renderFlowWarnings()};
    updateLabels();
    const two=ID("cx-mode-two"),one=ID("cx-mode-one"); if(state.mode==="two-way"){two.checked=true}else{one.checked=true}
    const en=ID("cx-enabled"); if(en) en.checked=!!state.enabled;
  }

  function restartFlowAnimation(mode){const rail=ID("cx-flow-rail"); if(!rail)return; const arrow=rail.querySelector(".arrow"),dots=[...rail.querySelectorAll(".dot.flow")]; rail.classList.remove("anim-one","anim-two"); arrow?.classList.remove("anim-one","anim-two"); dots.forEach(d=>d.classList.remove("anim-one","anim-two")); void rail.offsetWidth; const cls=(mode==="two"?"anim-two":"anim-one"); rail.classList.add(cls); arrow?.classList.add(cls); dots.forEach(d=>d.classList.add(cls))}
  function updateFlow(animate=false){
    const s=byName(state.src),d=byName(state.dst);
    Q("#cx-flow-src",wrap).innerHTML=s?_provLogoHTML(s.name,s.label):"";
    Q("#cx-flow-dst",wrap).innerHTML=d?_provLogoHTML(d.name,d.label):"";
    const two=ID("cx-mode-two"),ok=(byName(state.src)?.capabilities?.bidirectional)&&(byName(state.dst)?.capabilities?.bidirectional);
    two.disabled=!ok; if(!ok&&two.checked) ID("cx-mode-one").checked=true; two.nextElementSibling?.classList.toggle("disabled",!ok);
    const t=ID("cx-flow-title"); if(t) t.textContent=(ID("cx-mode-two")?.checked?"Two-way (bidirectional)":"One-way");
    updateFlowClasses(); if(animate) restartFlowAnimation(ID("cx-mode-two")?.checked?"two":"one"); renderFlowWarnings();
  }
  wrap.__updateFlow=updateFlow;

  function updateFlowClasses(){
    const rail=ID("cx-flow-rail"); if(!rail)return; const two=ID("cx-mode-two")?.checked,enabled=!!ID("cx-enabled")?.checked,wl=state.options.watchlist||defaultForFeature("watchlist");
    rail.className="flow-rail pretty"; rail.classList.toggle("mode-two",!!two); rail.classList.toggle("mode-one",!two);
    const flowOn=enabled&&wl.enable&&(two?(wl.add||wl.remove):(wl.add||wl.remove));
    rail.classList.toggle("off",!flowOn); if(two) rail.classList.toggle("active",flowOn); else{rail.classList.toggle("dir-add",flowOn&&wl.add); rail.classList.toggle("dir-remove",flowOn&&!wl.add&&wl.remove)}
  }
  wrap.__updateFlowClasses=updateFlowClasses;

  /* panels */
  function renderFeaturePanel(){
    const left=ID("cx-feat-panel"),right=ID("cx-adv-panel"); if(!left||!right) return;
    if(state.feature==="about"){
      left.innerHTML=`<div class="panel-title">About</div>
        <div class="rules">
          <div class="r"><div class="t">App</div><div class="s">Crosswatch — Configure Connection</div></div>
          <div class="r"><div class="t">Version</div><div class="s">${(G.cx&&G.cx.version)||"—"}</div></div>
          <div class="r"><div class="t">Endpoints</div><div class="s">/api/config • /api/pairs • /api/sync/providers</div></div>
        </div>`;
      right.innerHTML=`<div class="panel-title">Tips</div><div class="muted">Use Dry run to preview changes. Two-way requires both providers to support bidirectional sync.</div>`;
      return;
    }
    if(state.feature==="globals"){
      const g=state.globals||{};
      left.innerHTML=`<div class="panel-title">Global sync options</div>
        <div class="opt-row"><label for="gl-dry">Dry run</label><label class="switch"><input id="gl-dry" type="checkbox" ${g.dry_run?"checked":""}><span class="slider"></span></label></div><div class="muted">Simulate changes only; no writes.</div>
        <div class="opt-row"><label for="gl-verify">Verify after write</label><label class="switch"><input id="gl-verify" type="checkbox" ${g.verify_after_write?"checked":""}><span class="slider"></span></label></div><div class="muted">Re-check a small sample after writes.</div>
        <div class="opt-row"><label for="gl-drop">Drop guard</label><label class="switch"><input id="gl-drop" type="checkbox" ${g.drop_guard?"checked":""}><span class="slider"></span></label></div><div class="muted">Protect against empty source snapshots.</div>
        <div class="opt-row"><label for="gl-mass">Allow mass delete</label><label class="switch"><input id="gl-mass" type="checkbox" ${g.allow_mass_delete?"checked":""}><span class="slider"></span></label></div><div class="muted">Permit bulk removals when required.</div>`;
      right.innerHTML=`<div class="panel-title">Advanced</div>
        <div class="opt-row"><label for="gl-ttl">Tombstone TTL (days)</label><input id="gl-ttl" class="input" type="number" min="0" step="1" value="${g.tombstone_ttl_days??30}"></div><div class="muted">Keep delete markers to avoid re-adding.</div>
        <div class="opt-row"><label for="gl-observed">Include observed deletes</label><label class="switch"><input id="gl-observed" type="checkbox" ${g.include_observed_deletes?"checked":""}><span class="slider"></span></label></div><div class="muted">Apply deletions detected from activity.</div>`;
      return;
    }
    if(state.feature==="watchlist"){
      const wl=getOpts("watchlist");
      left.innerHTML=`<div class="panel-title">Watchlist — basics</div>
        <div class="opt-row"><label for="cx-wl-enable">Enable</label><label class="switch"><input id="cx-wl-enable" type="checkbox" ${wl.enable?"checked":""}><span class="slider"></span></label></div>
        <div class="grid2"><div class="opt-row"><label for="cx-wl-add">Add</label><label class="switch"><input id="cx-wl-add" type="checkbox" ${wl.add?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-wl-remove">Remove</label><label class="switch"><input id="cx-wl-remove" type="checkbox" ${wl.remove?"checked":""}><span class="slider"></span></label></div></div>`;
      if(__pairHasJellyfin(state)){
        const jfw=state.jellyfin?.watchlist||{mode:"favorites",playlist_name:"Watchlist"};
        right.innerHTML=`<div class="panel-title">Advanced</div>
          <div class="panel-title small" style="margin-top:6px">Jellyfin specifics</div>
          <details id="cx-jf-wl" open>
            <summary class="muted" style="margin-bottom:10px;">Favorites vs. Playlist watchlist</summary>
            <div class="grid2 compact">
              <div class="opt-row">
                <label>Mode</label>
                <div class="seg">
                  <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-fav" value="favorites" ${jfw.mode==='favorites'?'checked':''}/><label for="cx-jf-wl-mode-fav">Favorites</label>
                  <input type="radio" name="cx-jf-wl-mode" id="cx-jf-wl-mode-pl" value="playlist" ${jfw.mode==='playlist'?'checked':''}/><label for="cx-jf-wl-mode-pl">Playlist</label>
                </div>
              </div>
              <div class="opt-row"><label for="cx-jf-wl-pl-name">Playlist name</label>
                <input id="cx-jf-wl-pl-name" class="input small" type="text" value="${(jfw.playlist_name||"Watchlist")}" ${jfw.mode==='playlist'?'':'disabled'} placeholder="Watchlist"></div>
            </div>
            <div class="hint" style="text-align:center;">Use <b>Favorites</b> or a <b>Playlist</b> to act as your watchlist.</div>
          </details>`;
      }else{ right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">No Jellyfin in this pair.</div>`}
      const wlRem=ID("cx-wl-remove"); if(wlRem){wlRem.disabled=!wl.enable; if(!wl.enable&&wlRem.checked) wlRem.checked=false} return;
    }
    if(state.feature==="ratings"){
      const rt=getOpts("ratings"),hasType=(t)=>Array.isArray(rt.types)&&rt.types.includes(t);
      left.innerHTML=`<div class="panel-title">Ratings — basics</div>
        <div class="opt-row"><label for="cx-rt-enable">Enable</label><label class="switch"><input id="cx-rt-enable" type="checkbox" ${rt.enable?"checked":""}><span class="slider"></span></label></div>
        <div class="grid2"><div class="opt-row"><label for="cx-rt-add">Add / Update</label><label class="switch"><input id="cx-rt-add" type="checkbox" ${rt.add?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-rt-remove">Remove (clear)</label><label class="switch"><input id="cx-rt-remove" type="checkbox" ${rt.remove?"checked":""}><span class="slider"></span></label></div></div>
        <div class="panel-title small">Scope</div>
        <div class="grid2 compact">
          <div class="opt-row"><label for="cx-rt-type-all">All</label><label class="switch"><input id="cx-rt-type-all" type="checkbox" ${(hasType("movies")&&hasType("shows")&&hasType("seasons")&&hasType("episodes"))?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row"><label for="cx-rt-type-movies">Movies</label><label class="switch"><input id="cx-rt-type-movies" type="checkbox" ${hasType("movies")?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row"><label for="cx-rt-type-shows">Shows</label><label class="switch"><input id="cx-rt-type-shows" type="checkbox" ${hasType("shows")?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row"><label for="cx-rt-type-seasons">Seasons</label><label class="switch"><input id="cx-rt-type-seasons" type="checkbox" ${hasType("seasons")?"checked":""}><span class="slider"></span></label></div>
          <div class="opt-row"><label for="cx-rt-type-episodes">Episodes</label><label class="switch"><input id="cx-rt-type-episodes" type="checkbox" ${hasType("episodes")?"checked":""}><span class="slider"></span></label></div>
        </div>`;
      const simkl=__pairHasSimkl(state);
      right.innerHTML=`<div class="panel-title">Advanced</div>
        <details id="cx-rt-adv" open>
          <summary class="muted" style="margin-bottom:10px;"></summary>
          <div class="panel-title small">History window</div>
          <div class="grid2">
            <div class="opt-row"><label for="cx-rt-mode">Mode</label>
              <select id="cx-rt-mode" class="input">
                <option value="only_new" ${rt.mode==='only_new'?'selected':''}>New since last sync</option>
                <option value="from_date" ${rt.mode==='from_date'?'selected':''}>From a date…</option>
                <option value="all" ${rt.mode==='all'?'selected':''}>Everything (advanced)</option>
              </select>
            </div>
            <div class="opt-row"><label for="cx-rt-from-date">From date</label><input id="cx-rt-from-date" class="input small" type="date" value="${(rt.from_date||"")}" ${rt.mode==='from_date'?'':'disabled'}></div>
          </div>
          <div class="hint">“New since last sync” is safe. Use “From a date” for a slice, or “Everything” for full backfill.</div>
        </details>
        ${simkl?`<div class="simkl-alert" role="note" aria-live="polite"><div class="title"><span class="ic">⚠</span> Simkl heads-up for Ratings</div><div class="body"><ul class="bul"><li><b>Movies:</b> Rating auto-marks as <i>Completed</i> on Simkl.</li><li>Can appear under <i>Recently watched</i> and <i>My List</i>.</li></ul><div class="mini">Tip: Prefer “New since last sync”.</div></div></div>`:""}`;
      const rm=ID("cx-rt-remove"); if(rm){rm.disabled=!rt.enable; if(!rt.enable&&rm.checked) rm.checked=false} try{updateRtCompactSummary()}catch(_){}
      return;
    }
    if(state.feature==="history"){
      const hs=getOpts("history");
      left.innerHTML=`<div class="panel-title">History — basics</div>
        <div class="opt-row"><label for="cx-hs-enable">Enable</label><label class="switch"><input id="cx-hs-enable" type="checkbox" ${hs.enable?"checked":""}><span class="slider"></span></label></div>
        <div class="grid2"><div class="opt-row"><label for="cx-hs-add">Add</label><label class="switch"><input id="cx-hs-add" type="checkbox" ${hs.add?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-hs-remove">Remove</label><label class="switch"><input id="cx-hs-remove" type="checkbox" ${hs.remove?"checked":""}><span class="slider"></span></label></div></div>
        <div class="muted">Synchronize plays between providers. “Remove” mirrors unscrobbles where supported.</div>`;
      right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">More controls coming later.</div>`;
      const rm=ID("cx-hs-remove"); if(rm){rm.disabled=!hs.enable; if(!hs.enable&&rm.checked) rm.checked=false} return;
    }
    left.innerHTML=`<div class="panel-title">${state.feature[0].toUpperCase()+state.feature.slice(1)} options</div><div class="muted" style="padding:18px 0;">Coming soon.</div>`;
    right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">Nothing to configure yet.</div>`;
  }
  wrap.__renderFeaturePanel=renderFeaturePanel;

  /* TABS — About always visible, Playlists disabled */
  const __TEMP_DISABLED=new Set(["playlists"]);
  function refreshTabs(){
    const tabs=ID("cx-feat-tabs"); if(!tabs) return;
    const LABELS={globals:"Globals",watchlist:"Watchlist",ratings:"Ratings",history:"History",playlists:"Playlists"};
    const TEMP_OFF=new Set(["playlists"]);
    const COMMON=new Set(commonFeatures());
    const ORDER=["globals","watchlist","ratings","history","playlists"];

    const isValid=k=>k==="globals" || (ORDER.includes(k) && !TEMP_OFF.has(k) && COMMON.has(k));
    if(!isValid(state.feature)) state.feature="globals";

    tabs.innerHTML="";
    ORDER.forEach(k=>{
      if(k!=="globals" && k!=="playlists" && !COMMON.has(k)) return;
      const b=document.createElement("button");
      b.className="ftab"; b.dataset.key=k; b.textContent=LABELS[k]||k;
      if(TEMP_OFF.has(k)){ b.classList.add("disabled"); b.ariaDisabled="true"; b.title="Coming soon"; }
      else{
        b.onclick=()=>{ wrap.__persistCurrentFeature?.(); state.feature=k; renderFeaturePanel();
          [...tabs.children].forEach(c=>c.classList.toggle("active", c.dataset.key===k)); };
      }
      if(state.feature===k) b.classList.add("active");
      tabs.appendChild(b);
    });

    renderFeaturePanel();
    queueMicrotask(()=>renderFeaturePanel());
  }
  wrap.__refreshTabs=refreshTabs;

  /* warnings */
  const _v=(v)=>String(v||"").split(".").map(x=>parseInt(x,10)||0); function isExperimental(ver){const[maj]=_v(ver); return maj<1}
  function renderFlowWarnings(){
    const box=ID("cx-flow-warn"); if(!box) return; const st=wrap.__state||{},src=st.src&&st.providers.find(p=>p.name===st.src),dst=st.dst&&st.providers.find(p=>p.name===st.dst),warns=[];
    const push=(prov)=>{if(!prov)return; if(isExperimental(prov.version)){const label=prov.label||prov.name||"Provider"; warns.push(`<div class="simkl-alert experimental-alert"><div class="title"><span class="ic">⚠</span> Experimental Module: ${label}</div><div class="body"><div class="mini">Not stable yet. Limited functionalities. Use Dry Run and verify results before syncing/removing.</div></div></div>`) }};
    push(src); push(dst); box.innerHTML=warns.join("")||"";
  }
  wrap.__renderFlowWarnings=renderFlowWarnings;

  /* first render */
  try{await loadProviders(); renderProviderSelects(); refreshTabs(); updateFlow(true); renderFlowWarnings()}catch(_){}

  /* state sync */
  wrap.addEventListener("change",(e)=>{
    const id=e.target.id,map={"cx-wl-enable":"cx-wl-remove","cx-rt-enable":"cx-rt-remove","cx-hs-enable":"cx-hs-remove","cx-pl-enable":"cx-pl-remove"};
    if(map[id]){const rm=ID(map[id]); if(rm){rm.disabled=!e.target.checked; if(!e.target.checked) rm.checked=false}}
    if(id.startsWith("cx-wl-")){state.options.watchlist={enable:!!ID("cx-wl-enable")?.checked,add:!!ID("cx-wl-add")?.checked,remove:!!ID("cx-wl-remove")?.checked}; state.visited.add("watchlist")}
    if(id.startsWith("cx-rt-")){
      if(id==="cx-rt-type-all"){const on=!!ID("cx-rt-type-all")?.checked; ["movies","shows","seasons","episodes"].forEach(t=>{const cb=ID(`cx-rt-type-${t}`); if(cb) cb.checked=on})}
      else if(/^cx-rt-type-(movies|shows|seasons|episodes)$/.test(id)){const allOn=["movies","shows","seasons","episodes"].every(t=>ID(`cx-rt-type-${t}`)?.checked),allCb=ID("cx-rt-type-all"); if(allCb) allCb.checked=!!allOn}
      if(id==="cx-rt-mode"){const md=(ID("cx-rt-mode")?.value||"only_new"),fd=ID("cx-rt-from-date"); if(fd){fd.disabled=(md!=="from_date"); if(md!=="from_date"){fd.value=""}}}
      const rt=state.options.ratings||{},types=ID("cx-rt-type-all")?.checked?["movies","shows","seasons","episodes"]:["movies","shows","seasons","episodes"].filter(t=>ID(`cx-rt-type-${t}`)?.checked);
      state.options.ratings=Object.assign({},rt,{enable:!!ID("cx-rt-enable")?.checked,add:!!ID("cx-rt-add")?.checked,remove:!!ID("cx-rt-remove")?.checked,types,mode:(ID("cx-rt-mode")?.value||rt.mode||"only_new"),from_date:(ID("cx-rt-from-date")?.value||"").trim()});
      state.visited.add("ratings");
    }
    if(id.startsWith("cx-hs-")){state.options.history={enable:!!ID("cx-hs-enable")?.checked,add:!!ID("cx-hs-add")?.checked,remove:!!ID("cx-hs-remove")?.checked}; state.visited.add("history")}
    if(id.startsWith("cx-pl-")){state.options.playlists={enable:!!ID("cx-pl-enable")?.checked,add:!!ID("cx-pl-add")?.checked,remove:!!ID("cx-pl-remove")?.checked}; state.visited.add("playlists")}
    if(id.startsWith("gl-")){state.globals={dry_run:!!Q("#gl-dry",wrap)?.checked,verify_after_write:!!Q("#gl-verify",wrap)?.checked,drop_guard:!!Q("#gl-drop",wrap)?.checked,allow_mass_delete:!!Q("#gl-mass",wrap)?.checked,tombstone_ttl_days:parseInt(Q("#gl-ttl",wrap)?.value||"0",10)||0,include_observed_deletes:!!Q("#gl-observed",wrap)?.checked}}
    if(id==="cx-jf-wl-mode-fav"||id==="cx-jf-wl-mode-pl"||id==="cx-jf-wl-pl-name"){const jf=state.jellyfin||(state.jellyfin={}),mode=ID("cx-jf-wl-mode-pl")?.checked?"playlist":"favorites",name=(ID("cx-jf-wl-pl-name")?.value||"").trim()||"Watchlist"; jf.watchlist={mode,playlist_name:name}}
    if(id==="cx-enabled"||id==="cx-mode-one"||id==="cx-mode-two") updateFlow(true);
    try{updateRtCompactSummary()}catch(_){}
    updateFlowClasses(); renderFlowWarnings();
  });

  ensureInlineFootNoBar(wrap); blockForeignSaveBars(wrap);
  return wrap;
}
G.cxEnsureCfgModal=cxEnsureCfgModal;

/* flow anim CSS */
(function(){if(ID("cx-flow-anim-css"))return; const st=document.createElement("style"); st.id="cx-flow-anim-css"; st.textContent=`@keyframes cx-flow-one{0%{left:0;opacity:.2}50%{opacity:1}100%{left:calc(100% - 8px);opacity:.2}}@keyframes cx-flow-two-a{0%{left:0;opacity:.2}50%{opacity:1}100%{left:calc(100% - 8px);opacity:.2}}@keyframes cx-flow-two-b{0%{left:calc(100% - 8px);opacity:.2}50%{opacity:1}100%{left:0;opacity:.2}}.flow-rail.pretty.anim-one .dot.flow.a{animation:cx-flow-one 1.2s ease-in-out infinite}.flow-rail.pretty.anim-one .dot.flow.b{animation:cx-flow-one 1.2s ease-in-out .6s infinite}.flow-rail.pretty.anim-two .dot.flow.a{animation:cx-flow-two-a 1.2s ease-in-out infinite}.flow-rail.pretty.anim-two .dot.flow.b{animation:cx-flow-two-b 1.2s ease-in-out infinite}`; document.head.appendChild(st)})();

/* debug */
Object.assign(window,{__cxGetState:()=> (ID("cx-modal")?.__state||window.__cxState||null)});

})(); // END
