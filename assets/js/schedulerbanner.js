/* assets/js/schedulerbanner.js */
/* refactored */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(()=>{
  if (window.__SCHED_BANNER_INIT__) return;
  window.__SCHED_BANNER_INIT__ = 1;

  const $=(s,r=document)=>r.querySelector(s),
    CW=()=>window.CW||{}, API=()=>CW().API||null, Cache=()=>CW().Cache||null,
    blank=()=>({enabled:false,title:"",state:null,streams:0}),
    clear=n=>S.timers[n]&&(clearTimeout(S.timers[n]),S.timers[n]=null),
    scrobMode=()=>String(S.cfg?.scrobble?.mode||"webhook").toLowerCase(),
    schedulingOn=c=>!!((c?.scheduling||c||{}).enabled||(c?.scheduling||c||{})?.advanced?.enabled),
    activeCaptureJobs=c=>(((c?.scheduling||c||{})?.advanced?.capture_jobs)||((c?.scheduling||c||{})?.advanced?.captureJobs)||[])
      .filter(r=>r&&typeof r==="object"&&r.active!==false&&String(r?.provider||"").trim()&&String(r?.feature||"").trim()&&String(r?.at||"").trim()).length,
    activeEventRules=c=>(((c?.scheduling||c||{})?.advanced?.event_rules)||((c?.scheduling||c||{})?.advanced?.eventRules)||[])
      .filter(r=>r&&typeof r==="object"&&r.active!==false&&String(r?.action?.kind||"sync_pair")==="sync_pair"&&String(r?.action?.pair_id||r?.action?.pairId||r?.pair_id||"").trim()&&String(r?.filters?.route_id||r?.filters?.routeId||"").trim()).length,
    chipText={sched:"Scheduler",watch:"Watcher",hook:"Webhook"},
    S={cfg:null,sched:{enabled:false,running:false,next:0,advanced:false,captures:0},evt:{enabled:false,count:0},watch:{...blank(),alive:false},hook:blank(),timers:{sched:null,scrob:null,wait:null},debounce:null,last:{watcher:"",webhook:""}};

  if (!$("#sched-banner-css")) {
    const st=document.createElement("style");
    st.id="sched-banner-css";
    st.textContent=`#ops-card .action-row{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;flex-wrap:wrap}#ops-card .action-buttons{display:flex;gap:10px;flex:1 1 640px;min-width:min(100%,540px);align-items:center;flex-wrap:wrap}#ops-card .cw-status-dock{display:flex;flex:1 1 360px;min-width:min(100%,320px);justify-content:flex-end;align-items:center;margin-left:auto}#sched-inline-log{position:static;z-index:2;pointer-events:none;display:flex;gap:10px;align-items:center;justify-content:flex-end;flex-wrap:wrap;max-width:100%}#sched-inline-log .sched,#sched-inline-log .sched *,#sched-inline-log .sched *::before,#sched-inline-log .sched *::after{box-sizing:border-box}#sched-inline-log .sched{--chip-accent:#ef4444;position:relative;display:inline-flex;align-items:center;gap:10px;min-height:34px;max-width:min(420px,100%);padding:5px 12px;border-radius:999px;background:linear-gradient(180deg,rgba(17,21,29,.96),rgba(10,13,20,.98));backdrop-filter:blur(6px);border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 8px 18px rgba(0,0,0,.24);overflow:hidden;pointer-events:auto;white-space:nowrap}#sched-inline-log .sched.ok,#sched-inline-log .sched.live{--chip-accent:#22c55e}#sched-inline-log .sched.idle{--chip-accent:#ef4444;background:linear-gradient(180deg,rgba(17,21,29,.94),rgba(10,13,20,.96));border-color:rgba(255,255,255,.07)}#sched-inline-log .sched.warn{--chip-accent:#ef4444;background:linear-gradient(180deg,rgba(30,16,18,.96),rgba(18,10,12,.98));border-color:rgba(211,106,106,.18)}#sched-inline-log .ic{position:relative;display:inline-flex!important;align-items:center!important;justify-content:center!important;align-self:center!important;flex:0 0 auto;z-index:1}#sched-inline-log .ic.dot{width:10px;height:10px;border-radius:50%;background:var(--chip-accent);box-shadow:0 0 0 1px rgba(0,0,0,.48),0 0 10px color-mix(in srgb,var(--chip-accent) 40%,transparent)}#sched-inline-log .sched.live .ic.dot::after{content:"";position:absolute;inset:-4px;border-radius:50%;border:1px solid color-mix(in srgb,var(--chip-accent) 42%,transparent);opacity:.7;animation:ringPulse 1.7s ease-out infinite}#sched-inline-log .copy{position:relative;z-index:1;display:inline-flex!important;align-items:center!important;align-self:center!important;gap:7px;min-width:0;max-width:100%;height:20px;line-height:20px;overflow:hidden}#sched-inline-log .label{display:inline-flex!important;align-items:center!important;align-self:center!important;vertical-align:middle!important;height:20px;line-height:20px;margin:0!important;padding:0!important;position:relative;top:0!important;bottom:auto!important;transform:none!important;font-size:10px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(181,190,208,.76);flex:0 0 auto}#sched-inline-log .value,#sched-inline-log .meta,#sched-inline-log .badges{position:relative;top:-1.5px!important}#sched-inline-log .value,#sched-inline-log .meta{display:inline-flex!important;align-items:center!important;align-self:center!important;vertical-align:middle!important;height:20px;line-height:20px;margin:0!important;padding:0!important;bottom:auto!important;transform:none!important}#sched-inline-log .value{font-size:12px;font-weight:800;letter-spacing:.02em;color:#eef3ff;flex:0 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis}#sched-inline-log .meta{min-width:0;overflow:hidden;text-overflow:ellipsis;font-size:12px;font-weight:700;color:rgba(188,197,214,.68)}#sched-inline-log .badges{display:inline-flex!important;align-items:center!important;align-self:center!important;gap:6px;flex:0 0 auto;height:20px}#sched-inline-log .badge{display:inline-flex;align-items:center;justify-content:center;min-width:24px;height:18px;padding:0 6px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.05);color:rgba(232,238,250,.82);font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;line-height:18px}#sched-inline-log .sched[data-tip]{cursor:help;transition:transform .16s ease,box-shadow .16s ease,border-color .16s ease,background .16s ease}#sched-inline-log .sched[data-tip]:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(20,24,32,.98),rgba(12,15,22,.99));box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 10px 22px rgba(0,0,0,.28)}#sched-inline-log .sched.warn[data-tip]:hover{background:linear-gradient(180deg,rgba(34,18,21,.98),rgba(22,11,14,.99))}#sched-inline-log .sched[data-tip]::after{content:attr(data-tip);position:absolute;left:50%;bottom:calc(100% + 8px);transform:translateX(-50%) translateY(4px);opacity:0;pointer-events:none;min-width:180px;max-width:min(320px,72vw);padding:8px 10px;border-radius:12px;background:rgba(10,13,20,.98);border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 30px rgba(0,0,0,.34);color:#edf4ff;font-size:11px;font-weight:700;line-height:1.35;white-space:normal;text-align:left;transition:opacity .16s ease,transform .16s ease}#sched-inline-log .sched[data-tip]:hover::after{opacity:1;transform:translateX(-50%) translateY(0)}@keyframes ringPulse{0%{transform:scale(.72);opacity:.8}80%{transform:scale(1.28);opacity:0}100%{transform:scale(1.28);opacity:0}}@media (max-width:1100px){#ops-card .cw-status-dock{flex-basis:100%;justify-content:flex-start;margin-left:0}#sched-inline-log{justify-content:flex-start}}@media (max-width:640px){#sched-inline-log{gap:8px}#sched-inline-log .sched{min-height:32px;padding:4px 10px;max-width:100%}#sched-inline-log .copy{gap:6px;height:18px;line-height:18px}#sched-inline-log .label,#sched-inline-log .value,#sched-inline-log .meta{height:18px;line-height:18px}#sched-inline-log .label{font-size:9px}#sched-inline-log .value,#sched-inline-log .meta{font-size:11px}#sched-inline-log .value,#sched-inline-log .meta,#sched-inline-log .badges{top:-1px!important}#sched-inline-log .badges{height:18px}#sched-inline-log .badge{height:17px;min-width:22px;padding:0 5px;font-size:9px;line-height:17px}}`;
    document.head.appendChild(st);
  }

  const findBox=()=>$("#ops-card .cw-status-dock")||(()=>{
    const row=$("#ops-card .action-row");
    if (row) return row.appendChild(Object.assign(document.createElement("div"),{className:"cw-status-dock"}));
    for (const s of ["#ops-card","#ops-out","#ops_log","#sync-output",".sync-output","#ops"]) { const n=$(s); if (n) return n; }
    const h=[...document.querySelectorAll("h2,h3,h4,div.head,.head")].find(x=>(x.textContent||"").trim().toUpperCase()==="SYNC OUTPUT");
    return h?.parentElement?.querySelector("pre,textarea,.box,.card,div")||null;
  })();

  function ensureBanner(){
    const host=findBox();
    if (!host) return null;
    if (host.id!=="sched-inline-log" && getComputedStyle(host).position==="static") host.style.position="relative";
    let wrap=host.id==="sched-inline-log"?host:$("#sched-inline-log",host);
    if (!wrap) {
      wrap=document.createElement("div");
      wrap.id="sched-inline-log";
      wrap.innerHTML=["sched","watch","hook"].map(k=>`<div class="sched idle" id="chip-${k}"><span class="ic dot"></span><span class="copy"><span class="label">${chipText[k]}</span><span class="value" id="${k}-value">-</span><span class="meta" id="${k}-meta"></span><span class="badges" id="${k}-badges"></span></span></div>`).join("");
      host.appendChild(wrap);
    }
    const evtChip=$("#chip-evt", wrap);
    if (evtChip) evtChip.remove();
    return wrap;
  }

  function clock(v,day=false){
    if (!v) return "-";
    const n=+v, dt=new Date(Number.isFinite(n)&&n>0?(n<1e10?n*1000:n):v);
    if (isNaN(+dt)) return "-";
    const time=dt.toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"}), now=new Date();
    if (!day || dt.toDateString()===now.toDateString()) return time;
    const tom=new Date(now); tom.setDate(now.getDate()+1);
    return dt.toDateString()===tom.toDateString()?`tomorrow ${time}`:`${dt.toLocaleDateString([],{weekday:"short"})} ${time}`;
  }

  function emit(source,detail){
    const payload={source,...detail}, key=JSON.stringify(payload);
    if (S.last[source]===key) return;
    S.last[source]=key;
    try { window.dispatchEvent(new CustomEvent("currently-watching-updated",{detail:payload})); } catch {}
  }

  function paint(el,{show=true,value="-",meta="",badges=[],live=false,ok=true,idle=false,tip=""}={}){
    if (!el?.chip) return;
    el.chip.style.display=show?"inline-flex":"none";
    if (!show) {
      if (el.badges) {
        el.badges.innerHTML="";
        el.badges.style.display="none";
      }
      return el.chip.removeAttribute("data-tip"), el.chip.removeAttribute("title");
    }
    const healthy=!!ok&&!idle;
    el.chip.classList.toggle("ok",healthy);
    el.chip.classList.toggle("live",!!live);
    el.chip.classList.toggle("idle",!!idle);
    el.chip.classList.toggle("warn",!idle&&!ok);
    el.value.textContent=value||"-";
    el.meta.textContent=meta||"";
    el.meta.style.display=meta?"inline":"none";
    if (el.badges) {
      const badgeHtml=(Array.isArray(badges)?badges:[]).filter(Boolean).map(x=>`<span class="badge">${String(x)}</span>`).join("");
      el.badges.innerHTML=badgeHtml;
      el.badges.style.display=badgeHtml?"inline-flex":"none";
    }
    tip=String(tip||"").trim();
    tip?(el.chip.dataset.tip=tip,el.chip.title=tip):(el.chip.removeAttribute("data-tip"),el.chip.removeAttribute("title"));
  }

  function render(){
    const host=ensureBanner();
    if (!host) return;
    const E=k=>({chip:$(`#chip-${k}`,host),value:$(`#${k}-value`,host),meta:$(`#${k}-meta`,host),badges:$(`#${k}-badges`,host)}),
      sched=E("sched"), watch=E("watch"), hook=E("hook"),
      watchLive=!!(S.watch.alive&&S.watch.state==="playing"&&S.watch.title),
      hookLive=!!(S.hook.state==="playing"&&S.hook.title),
      watchStreams=watchLive&&S.watch.streams>1?`${S.watch.streams} streams`:"",
      hookStreams=hookLive&&S.hook.streams>1?`${S.hook.streams} streams`:"",
      schedBadges=[S.sched.captures?`C${S.sched.captures}`:"",S.evt.enabled&&S.evt.count?`E${S.evt.count}`:""].filter(Boolean);

    paint(sched,!S.sched.enabled?{show:false}:{
      value:S.sched.running?"running":(S.sched.advanced?"advanced":"scheduled"),
      meta:S.sched.next?`next ${clock(S.sched.next,true)}`:"",
      badges:schedBadges,
      live:S.sched.running,
      tip:`Scheduler ${S.sched.running?"running":(S.sched.advanced?"advanced":"scheduled")}${S.sched.next?` • next ${clock(S.sched.next,true)}`:""}${S.sched.advanced?" • advanced mode":""}${S.sched.captures?` • ${S.sched.captures} capture schedule${S.sched.captures===1?"":"s"}`:""}${S.evt.enabled&&S.evt.count?` • ${S.evt.count} event trigger${S.evt.count===1?"":"s"}`:""}`
    });

    paint(watch,!S.watch.enabled?{show:false}:{
      value:watchLive?S.watch.title:(S.watch.alive?"running":"idle"), meta:watchStreams,
      live:watchLive, ok:S.watch.alive, idle:!S.watch.alive,
      tip:watchLive?`Watcher playing • ${S.watch.title}${watchStreams?` • ${watchStreams}`:""}`:`Watcher ${S.watch.alive?"running":"idle"}${S.watch.state?` • state ${S.watch.state}`:""}`
    });
    emit("watcher",watchLive?{title:S.watch.title,progress:0,state:S.watch.state||"playing",_streams_count:S.watch.streams}:{state:"stopped"});

    paint(hook,!S.hook.enabled?{show:false}:{
      value:hookLive?S.hook.title:"enabled", meta:hookStreams,
      live:hookLive, idle:!hookLive,
      tip:hookLive?`Webhook playing • ${S.hook.title}${hookStreams?` • ${hookStreams}`:""}`:"Webhook enabled"
    });
    emit("webhook",hookLive?{title:S.hook.title,progress:0,state:S.hook.state||"playing",_streams_count:S.hook.streams}:{state:"stopped"});

    host.style.display=S.sched.enabled||S.evt.enabled||S.watch.enabled||S.hook.enabled?"flex":"none";
  }

  async function readConfig(force=false){
    try { return await API()?.Config.load(force)||{}; } catch { return Cache()?.getCfg()||{}; }
  }

  async function pollSched(force=false){
    clear("sched");
    if (document.hidden) return scheduleSched();
    try {
      const st=await API().Scheduling.status(force), sc=st?.config||S.cfg?.scheduling||{};
      S.sched={enabled:schedulingOn(sc),advanced:!!sc?.advanced?.enabled,running:!!st?.running,next:+(st?.next_run_at||st?.next_run||0)||0,captures:activeCaptureJobs(sc)};
      S.evt={enabled:!!sc?.advanced?.enabled&&activeEventRules(sc)>0,count:activeEventRules(sc)};
    } catch { S.sched={enabled:false,running:false,next:0,advanced:false,captures:0}; S.evt={enabled:false,count:0}; }
    render();
    scheduleSched();
  }

  async function pollScrob(force=false){
    clear("scrob");
    const sc=S.cfg?.scrobble||{}, mode=scrobMode(), enabled=!!sc.enabled;
    S.watch={...blank(),enabled:enabled&&mode==="watch",alive:false};
    S.hook={...blank(),enabled:enabled&&mode==="webhook"};
    if (!enabled) return render();
    if (document.hidden) return scheduleScrob();
    try {
      if (S.watch.enabled) S.watch.alive=!!(await API().Watch.status(force))?.alive;
      const cw=await API().Watch.currentlyWatching(force).catch(()=>null), cur=cw&&(cw.currently_watching||cw), streams=+cw?.streams_count||0;
      if (cur?.state && cur.state!=="stopped") {
        const src=String(cur.source||"").toLowerCase(), target=src.includes("webhook")&&S.hook.enabled?S.hook:(src.includes("watch")||src.includes("watcher"))&&S.watch.enabled?S.watch:(S.watch.enabled?S.watch:S.hook);
        Object.assign(target,{title:cur.title||"",state:cur.state||null,streams});
      }
    } catch {}
    render();
    scheduleScrob();
  }

  const scheduleSched=()=>S.sched.enabled&&(S.timers.sched=setTimeout(()=>pollSched(false),30000)),
    scheduleScrob=()=>S.cfg?.scrobble?.enabled&&(S.timers.scrob=setTimeout(()=>pollScrob(false),scrobMode()==="watch"?15000:60000)),
    stop=()=>["sched","scrob"].forEach(clear);

  async function refresh(forceCfg=false){
    stop();
    S.cfg=await readConfig(forceCfg);
    if (!schedulingOn(S.cfg?.scheduling) && !S.cfg?.scrobble?.enabled) {
      S.sched={enabled:false,running:false,next:0,advanced:false,captures:0};
      S.evt={enabled:false,count:0};
      S.watch={...blank(),alive:false};
      S.hook=blank();
      return render();
    }
    schedulingOn(S.cfg?.scheduling)?await pollSched(true):(S.sched={enabled:false,running:false,next:0,advanced:false,captures:0},S.evt={enabled:false,count:0});
    S.cfg?.scrobble?.enabled?await pollScrob(true):(S.watch.enabled=S.hook.enabled=false,render());
  }

  function queueRefresh(forceCfg=false){
    clearTimeout(S.debounce);
    S.debounce=setTimeout(()=>{
      if (forceCfg) try { Cache()?.invalidate("config"); } catch {}
      refresh(forceCfg);
    },150);
  }

  function boot(){
    if (window.__SCHED_BANNER_STARTED__) return;
    window.__SCHED_BANNER_STARTED__=true;
    queueRefresh(true);
    document.addEventListener("visibilitychange",()=>!document.hidden&&queueRefresh(false),{passive:true});
    document.addEventListener("config-saved",()=>queueRefresh(true));
    window.addEventListener("auth-changed",()=>queueRefresh(true));
    document.addEventListener("scheduling-status-refresh",()=>pollSched(true));
    document.addEventListener("watcher-status-refresh",()=>pollScrob(true));
    document.addEventListener("tab-changed",e=>["main","settings"].includes(e?.detail?.id)&&queueRefresh(false));
    window.addEventListener("focus",()=>queueRefresh(false));
    window.refreshSchedulingBanner=queueRefresh;
  }

  document.addEventListener("DOMContentLoaded",()=>{
    clear("wait");
    S.timers.wait=setInterval(()=>findBox()&&(clear("wait"),boot()),300);
  });
})();
