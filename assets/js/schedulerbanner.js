/* assets/js/schedulerbanner.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(()=>{
  if (window.__SCHED_BANNER_INIT__) return;
  window.__SCHED_BANNER_INIT__ = 1;

  const $=(s,r=document)=>r.querySelector(s),
    CW=()=>window.CW||{}, API=()=>CW().API||null, Cache=()=>CW().Cache||null,
    blank=()=>({enabled:false,title:"",state:null,streams:0,items:[],index:0}),
    clear=n=>S.timers[n]&&(clearTimeout(S.timers[n]),S.timers[n]=null),
    scrobMode=()=>String(S.cfg?.scrobble?.mode||"webhook").toLowerCase(),
    scrobSources=()=>{
      const sc=S.cfg?.scrobble||{}, mode=scrobMode(), src=sc?.sources&&typeof sc.sources==="object"?sc.sources:null;
      return src?{webhook:!!src.webhook,watcher:!!(src.watcher??src.watch)}:{webhook:mode==="webhook",watcher:mode==="watch"};
    },
    schedulingOn=c=>!!((c?.scheduling||c||{}).enabled||(c?.scheduling||c||{})?.advanced?.enabled),
    advancedOn=c=>!!((c?.scheduling||c||{})?.advanced?.enabled),
    activeCaptureJobs=c=>(((c?.scheduling||c||{})?.advanced?.capture_jobs)||((c?.scheduling||c||{})?.advanced?.captureJobs)||[])
      .filter(r=>r&&typeof r==="object"&&r.active!==false&&String(r?.provider||"").trim()&&String(r?.feature||"").trim()&&String(r?.at||"").trim()).length,
    activeEventRules=c=>(((c?.scheduling||c||{})?.advanced?.event_rules)||((c?.scheduling||c||{})?.advanced?.eventRules)||[])
      .filter(r=>r&&typeof r==="object"&&r.active!==false&&String(r?.action?.kind||"sync_pair")==="sync_pair"&&String(r?.action?.pair_id||r?.action?.pairId||r?.pair_id||"").trim()&&String(r?.filters?.route_id||r?.filters?.routeId||"").trim()).length,
    webhookState=c=>{
      const wh=(c?.scheduling||c||{})?.webhooks||{};
      if (!wh||typeof wh!=="object"||wh.enabled!==true) return {active:false,label:""};
      const format=String(wh.payload_format||wh.payloadFormat||wh.format||"crosswatch").trim().toLowerCase().replace("-","_"),
        label=format==="notifiarr"||format==="notifiarr_passthrough"?"Notifiarr Passthrough":"CrossWatch JSON",
        url=String(wh.url||wh.default_url||wh.defaultUrl||"").trim(),
        urls=[url,wh.base_url,wh.healthchecks_base_url,wh.start_url,wh.success_url,wh.failure_url].map(v=>String(v||"").trim()).filter(Boolean);
      return {active:label==="Notifiarr Passthrough"?!!url:urls.length>0,label};
    },
    chipText={pairs:"Sync pairs",sched:"Scheduler",watch:"Watcher",hook:"Webhook",health:"CrossWatch health",update:"Updates"},
    chipIcon={pairs:"sync_alt",sched:"calendar_month",watch:"visibility",hook:"webhook",health:"arrow_upward",update:"notifications"},
    chipNav={pairs:{target:"pairs",label:"Open sync pair settings"},sched:{target:"scheduling",label:"Open scheduler settings"},watch:{target:"watcher",label:"Open watcher settings"},hook:{target:"webhook",label:"Open webhook settings"},health:{target:"maintenance",label:"Open Maintenance tools"},update:{target:"refresh-update",label:"Re-check for updates"}},
    S={cfg:null,pairs:{total:0,active:0},sched:{enabled:false,running:false,next:0,advanced:false,captures:0,webhooks:false,webhookLabel:"",warning:false,warnings:[]},evt:{enabled:false,count:0},watch:{...blank(),alive:false},hook:blank(),system:{health:{known:false,ok:false,status:"checking"},update:{known:false,available:false,current:"",latest:"",url:""}},timers:{sched:null,scrob:null,health:null,wait:null,rotate:null},debounce:null,last:{watcher:"",webhook:""}};
  const SHARED_WATCH_KEY="__CW_CURRENT_WATCHING_SHARED__",SHARED_WATCH_TTL_MS=3000,WATCHER_UNAVAILABLE_GRACE_MS=35000;
  let scrobPollSeq=0;
  const isManaged=()=>document.documentElement?.dataset?.cwRole==="user";

  if (!$("#sched-banner-css")) {
    const st=document.createElement("style");
    st.id="sched-banner-css";
    st.textContent=`#ops-card .action-row{display:flex;align-items:center;justify-content:flex-start;gap:12px;flex-wrap:nowrap}#ops-card .action-buttons{display:flex;gap:10px;flex:0 0 auto;min-width:0;align-items:center;flex-wrap:nowrap;order:1}#ops-card .cw-status-dock{display:flex;flex:1 1 auto;width:auto;min-width:0;justify-content:flex-end;align-items:center;margin-left:auto;order:2}#ops-card .cw-status-dock:not(:has(.sched)){display:none}#sched-inline-log{position:static;z-index:2;pointer-events:none;display:flex;gap:10px;align-items:center;justify-content:flex-end;flex-wrap:wrap;max-width:100%;width:100%}#sched-inline-log .sched,#sched-inline-log .sched *,#sched-inline-log .sched *::before,#sched-inline-log .sched *::after{box-sizing:border-box}#sched-inline-log .sched{--chip-accent:#ef4444;position:relative;display:inline-flex;align-items:center;gap:10px;min-height:34px;max-width:min(420px,100%);padding:5px 12px;border-radius:999px;background:linear-gradient(180deg,rgba(17,21,29,.96),rgba(10,13,20,.98));backdrop-filter:blur(6px);border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 8px 18px rgba(0,0,0,.24);overflow:hidden;pointer-events:auto;white-space:nowrap}#sched-inline-log .sched.ok,#sched-inline-log .sched.live{--chip-accent:#22c55e}#sched-inline-log .sched.idle{--chip-accent:#ef4444;background:linear-gradient(180deg,rgba(17,21,29,.94),rgba(10,13,20,.96));border-color:rgba(255,255,255,.07)}#sched-inline-log .sched.warn{--chip-accent:#ef4444;background:linear-gradient(180deg,rgba(30,16,18,.96),rgba(18,10,12,.98));border-color:rgba(211,106,106,.18)}#sched-inline-log .ic{position:relative;display:inline-flex!important;align-items:center!important;justify-content:center!important;align-self:center!important;flex:0 0 auto;z-index:1}#sched-inline-log .ic.dot{width:10px;height:10px;border-radius:50%;background:var(--chip-accent);box-shadow:0 0 0 1px rgba(0,0,0,.48),0 0 10px color-mix(in srgb,var(--chip-accent) 40%,transparent)}#sched-inline-log .sched.live .ic.dot::after{content:"";position:absolute;inset:-4px;border-radius:50%;border:1px solid color-mix(in srgb,var(--chip-accent) 42%,transparent);opacity:.7;animation:ringPulse 1.7s ease-out infinite}#sched-inline-log .copy{position:relative;z-index:1;display:inline-flex!important;align-items:center!important;align-self:center!important;gap:7px;min-width:0;max-width:100%;height:20px;line-height:20px;overflow:hidden}#sched-inline-log .label{display:inline-flex!important;align-items:center!important;align-self:center!important;vertical-align:middle!important;height:20px;line-height:20px;margin:0!important;padding:0!important;position:relative;top:0!important;bottom:auto!important;transform:none!important;font-size:10px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(181,190,208,.76);flex:0 0 auto}#sched-inline-log .value,#sched-inline-log .meta,#sched-inline-log .badges{position:relative;top:-1.5px!important}#sched-inline-log .value,#sched-inline-log .meta{display:inline-flex!important;align-items:center!important;align-self:center!important;vertical-align:middle!important;height:20px;line-height:20px;margin:0!important;padding:0!important;bottom:auto!important;transform:none!important}#sched-inline-log .value{font-size:12px;font-weight:800;letter-spacing:.02em;color:#eef3ff;flex:0 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis}#sched-inline-log .meta{min-width:0;overflow:hidden;text-overflow:ellipsis;font-size:12px;font-weight:700;color:rgba(188,197,214,.68)}#sched-inline-log .badges{display:inline-flex!important;align-items:center!important;align-self:center!important;gap:6px;flex:0 0 auto;height:20px}#sched-inline-log .badge{display:inline-flex;align-items:center;justify-content:center;min-width:24px;height:18px;padding:0 6px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.05);color:rgba(232,238,250,.82);font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;line-height:18px}#sched-inline-log .sched[data-tip]{cursor:help;transition:transform .16s ease,box-shadow .16s ease,border-color .16s ease,background .16s ease}#sched-inline-log .sched[data-tip]:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(20,24,32,.98),rgba(12,15,22,.99));box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 10px 22px rgba(0,0,0,.28)}#sched-inline-log .sched.warn[data-tip]:hover{background:linear-gradient(180deg,rgba(34,18,21,.98),rgba(22,11,14,.99))}#sched-inline-log .sched[data-tip]::after{content:attr(data-tip);position:absolute;left:50%;bottom:calc(100% + 8px);transform:translateX(-50%) translateY(4px);opacity:0;pointer-events:none;min-width:180px;max-width:min(320px,72vw);padding:8px 10px;border-radius:12px;background:rgba(10,13,20,.98);border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 30px rgba(0,0,0,.34);color:#edf4ff;font-size:11px;font-weight:700;line-height:1.35;white-space:pre-line;text-align:left;transition:opacity .16s ease,transform .16s ease}#sched-inline-log .sched[data-tip]:hover::after{opacity:1;transform:translateX(-50%) translateY(0)}@keyframes ringPulse{0%{transform:scale(.72);opacity:.8}80%{transform:scale(1.28);opacity:0}100%{transform:scale(1.28);opacity:0}}@media (max-width:640px){#ops-card .action-row{flex-wrap:wrap}#ops-card .action-buttons{width:100%;flex-wrap:wrap}#ops-card .cw-status-dock{width:100%;margin-left:0}#sched-inline-log{gap:8px}#sched-inline-log .sched{min-height:32px;padding:4px 10px;max-width:100%}#sched-inline-log .copy{gap:6px;height:18px;line-height:18px}#sched-inline-log .label,#sched-inline-log .value,#sched-inline-log .meta{height:18px;line-height:18px}#sched-inline-log .label{font-size:9px}#sched-inline-log .value,#sched-inline-log .meta{font-size:11px}#sched-inline-log .value,#sched-inline-log .meta,#sched-inline-log .badges{top:-1px!important}#sched-inline-log .badges{height:18px}#sched-inline-log .badge{height:17px;min-width:22px;padding:0 5px;font-size:9px;line-height:17px}}`;
    st.textContent+=`
#ops-card{--hub-service-good:#57b58a;--hub-service-bad:#e06470;--hub-service-disabled:#788291;--hub-service-update:#8b5cf6;--hub-action-bg:rgba(255,255,255,.035);--hub-action-hover:rgba(255,255,255,.065);--hub-card-bg:linear-gradient(180deg,rgba(20,24,34,.96),rgba(12,15,23,.98));--hub-card-border:rgba(255,255,255,.09);--hub-text:#eef3ff;--hub-muted:rgba(188,198,217,.68);}
#ops-card .action-row{display:flex!important;align-items:center!important;justify-content:flex-start!important;flex-wrap:nowrap!important;gap:14px!important;margin-top:14px!important;padding:14px!important;}
#ops-card .action-buttons{display:flex!important;flex:0 0 auto!important;min-width:0!important;width:auto!important;align-items:center!important;justify-content:flex-start!important;gap:10px!important;order:1!important;flex-wrap:nowrap!important;}
#ops-card .action-buttons .cw-hub-action{display:inline-flex!important;align-items:center!important;justify-content:center!important;gap:9px!important;flex:0 0 auto!important;min-width:132px!important;min-height:46px!important;height:46px!important;padding:0 16px!important;border-radius:14px!important;background:var(--hub-action-bg)!important;border:1px solid var(--hub-card-border)!important;color:var(--hub-text)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.035)!important;white-space:nowrap!important;line-height:1!important;}
#ops-card .action-buttons .cw-hub-action:hover{background:var(--hub-action-hover)!important;border-color:rgba(255,255,255,.16)!important;transform:translateY(-1px)!important;}
#ops-card .action-buttons .cw-action-icon{font-size:21px!important;line-height:1!important;font-variation-settings:"FILL" 0,"wght" 450,"GRAD" 0,"opsz" 24;}
#ops-card .action-buttons .cw-hub-action .cw-action-icon{color:var(--hub-text)!important;}
#ops-card .action-buttons .cw-split-run .cw-sync-action-icon{color:var(--hub-service-good)!important;}
#ops-card .cw-split-run{flex:0 0 auto!important;align-items:stretch!important;min-height:46px!important;height:46px!important;}
#ops-card .cw-split-run .btn{min-height:44px!important;height:44px!important;}
#ops-card .cw-split-run .cw-split-main{display:inline-flex!important;align-items:center!important;justify-content:center!important;gap:9px!important;min-width:146px!important;white-space:nowrap!important;line-height:1!important;}
#ops-card .action-buttons :is(.cw-hub-action,.cw-split-main,.cw-split-edge){box-sizing:border-box!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;height:46px!important;min-height:46px!important;line-height:1!important;vertical-align:middle!important;font-size:14px!important;font-weight:850!important;}
#ops-card .action-buttons .cw-split-edge{width:46px!important;min-width:46px!important;padding:0!important;}
#ops-card .action-buttons :is(.cw-hub-action,.cw-split-main,.cw-split-edge)>span{display:inline-flex!important;align-items:center!important;justify-content:center!important;line-height:1!important;margin:0!important;position:static!important;transform:none!important;}
#ops-card .action-buttons .material-symbols-rounded{width:24px!important;min-width:24px!important;height:24px!important;font-size:22px!important;line-height:24px!important;text-align:center!important;overflow:hidden!important;font-variation-settings:"FILL" 0,"wght" 450,"GRAD" 0,"opsz" 24!important;}
#ops-card .action-buttons :is(.cw-hub-action,.cw-split-main)>span:not(.material-symbols-rounded){height:22px!important;line-height:22px!important;}
#ops-card #run:is(.loading,.glass) .cw-sync-action-icon{animation:cwHubSyncSpin .8s linear infinite!important;}
#ops-card .cw-status-dock{display:flex!important;flex:0 0 auto!important;box-sizing:border-box!important;width:100%!important;min-width:0!important;align-items:center!important;justify-content:flex-start!important;margin:10px 0 0!important;padding-top:12px!important;border-top:1px solid rgba(255,255,255,.08)!important;order:initial!important;overflow:visible!important;}
#ops-card .cw-status-dock:not(:has(.sched)){display:none!important;}
#sched-inline-log{display:flex!important;width:100%!important;max-width:100%!important;align-items:center!important;justify-content:flex-start!important;gap:18px!important;flex-wrap:nowrap!important;overflow:visible!important;}
#ops-card #sched-inline-log .hub-status-group{position:relative;display:block;min-width:0;overflow:visible;}
#ops-card #sched-inline-log .hub-status-group.is-hidden{display:none;}
#ops-card #sched-inline-log .hub-group-system{margin-left:auto;}
#ops-card #sched-inline-log .hub-group-items{display:flex;align-items:center;gap:4px;padding:3px 5px;border:1px solid var(--hub-card-border);border-radius:12px;background:var(--hub-action-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.025);overflow:visible;}
#ops-card #sched-inline-log .sched{--service-state:var(--hub-service-bad);position:relative!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;flex:0 0 64px!important;width:64px!important;min-width:64px!important;max-width:64px!important;min-height:39px!important;height:39px!important;padding:0!important;border-radius:10px!important;background:var(--hub-action-bg)!important;border:1px solid var(--hub-card-border)!important;color:var(--hub-text)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)!important;overflow:visible!important;white-space:nowrap!important;isolation:isolate!important;}
#ops-card #sched-inline-log .sched.is-hidden{display:none!important;}
#ops-card #sched-inline-log .hub-group-items .sched{background:transparent!important;border:0!important;box-shadow:none!important;}
#ops-card #sched-inline-log .hub-group-items .sched:hover,#ops-card #sched-inline-log .hub-group-items .sched:focus-visible{background:var(--hub-action-hover)!important;}
#ops-card #sched-inline-log .hub-group-items .sched:hover,#ops-card #sched-inline-log .hub-group-items .sched:focus-visible{z-index:50!important;}
#ops-card #sched-inline-log #chip-sched{flex-basis:64px!important;}
#ops-card #sched-inline-log .sched.ok,#ops-card #sched-inline-log .sched.live{--service-state:var(--hub-service-good);}
#ops-card #sched-inline-log .sched:is(.ok,.live) .service-icon{opacity:.72!important;}
#ops-card #sched-inline-log .sched:is(.ok,.live)::before{opacity:.68!important;box-shadow:0 0 7px color-mix(in srgb,var(--service-state) 24%,transparent)!important;}
#ops-card #sched-inline-log .sched:is(.ok,.live) .watch-progress{opacity:.82;filter:saturate(.82);}
#ops-card #sched-inline-log .sched.disabled{--service-state:var(--hub-service-disabled);border-color:color-mix(in srgb,var(--hub-service-disabled) 26%,var(--hub-card-border))!important;}
#ops-card #sched-inline-log .sched::before{content:""!important;position:absolute!important;z-index:2!important;left:0!important;top:5px!important;right:auto!important;bottom:5px!important;width:3px!important;height:auto!important;border:0!important;border-radius:0 999px 999px 0!important;background:var(--service-state)!important;box-shadow:0 0 10px color-mix(in srgb,var(--service-state) 48%,transparent)!important;transform:none!important;opacity:1!important;pointer-events:none!important;}
#ops-card #sched-inline-log .service-icon{display:inline-flex!important;align-items:center!important;justify-content:center!important;width:28px!important;height:28px!important;font-size:23px!important;line-height:1!important;color:var(--service-state)!important;background:transparent!important;border:0!important;border-radius:0!important;box-shadow:none!important;font-variation-settings:"FILL" 0,"wght" 430,"GRAD" 0,"opsz" 24;}
#ops-card #sched-inline-log .watch-progress{--watch-progress-angle:0deg;position:relative;display:none;place-items:center;flex:0 0 28px;width:28px;height:28px;margin-left:1px;border-radius:50%;background:conic-gradient(var(--service-state) 0 var(--watch-progress-angle),color-mix(in srgb,var(--hub-muted) 20%,transparent) var(--watch-progress-angle) 360deg);box-shadow:inset 0 0 0 1px color-mix(in srgb,var(--hub-muted) 18%,transparent);overflow:hidden;}
#ops-card #sched-inline-log .watch-progress::before{content:"";position:absolute;inset:4px;border-radius:50%;background:var(--hub-card-bg);box-shadow:inset 0 0 0 1px var(--hub-card-border);}
#ops-card #sched-inline-log .watch-progress-value{position:relative;z-index:1;font-size:8px;font-weight:900;letter-spacing:-.02em;line-height:1;color:var(--hub-text);}
#ops-card #sched-inline-log :is(#chip-watch,#chip-hook).has-progress .watch-progress{display:grid;}
#ops-card #sched-inline-log :is(#chip-watch,#chip-hook).has-progress{min-width:190px!important;}
#ops-card #sched-inline-log .sched.disabled .service-icon{opacity:.52!important;}
#ops-card #sched-inline-log .sched.disabled::before{opacity:.48!important;box-shadow:none!important;}
#ops-card #sched-inline-log .copy{position:absolute!important;width:1px!important;height:1px!important;padding:0!important;margin:-1px!important;overflow:hidden!important;clip:rect(0,0,0,0)!important;white-space:nowrap!important;border:0!important;}
#ops-card #sched-inline-log .sched.has-copy{flex:0 1 auto!important;width:auto!important;max-width:280px!important;justify-content:flex-start!important;gap:9px!important;padding:0 12px 0 9px!important;}
#ops-card #sched-inline-log #chip-sched.has-copy{min-width:166px!important;}
#ops-card #sched-inline-log #chip-watch.has-copy,#ops-card #sched-inline-log #chip-hook.has-copy{min-width:150px!important;}
#ops-card #sched-inline-log .sched.has-copy .copy{position:static!important;display:grid!important;grid-template-columns:auto auto auto!important;grid-template-rows:auto auto!important;align-items:center!important;column-gap:7px!important;row-gap:2px!important;width:auto!important;height:auto!important;padding:0!important;margin:0!important;overflow:visible!important;clip:auto!important;white-space:nowrap!important;border:0!important;}
#ops-card #sched-inline-log .sched.has-copy .label,#ops-card #sched-inline-log .sched.has-copy .value,#ops-card #sched-inline-log .sched.has-copy .meta,#ops-card #sched-inline-log .sched.has-copy .badges{position:static!important;top:auto!important;transform:none!important;margin:0!important;padding:0!important;height:auto!important;line-height:1.15!important;}
#ops-card #sched-inline-log .sched.has-copy .label{grid-column:1!important;grid-row:1!important;font-size:10px!important;font-weight:850!important;letter-spacing:.11em!important;color:var(--hub-muted)!important;}
#ops-card #sched-inline-log .sched.has-copy .value{grid-column:2!important;grid-row:1!important;font-size:11px!important;font-weight:850!important;color:var(--service-state)!important;}
#ops-card #sched-inline-log #chip-sched .cw-sched-warning-icon{display:inline-flex!important;align-items:center!important;justify-content:center!important;font-size:15px!important;line-height:1!important;font-variation-settings:"FILL" 1,"wght" 500,"GRAD" 0,"opsz" 20;}
#ops-card #sched-inline-log .sched.has-copy .value:empty{display:none!important;}
#ops-card #sched-inline-log .sched.has-copy .meta{grid-column:1 / -1!important;grid-row:2!important;font-size:10px!important;font-weight:650!important;color:var(--hub-muted)!important;overflow:hidden!important;text-overflow:ellipsis!important;}
#ops-card #sched-inline-log .sched.has-copy .badges{grid-column:3!important;grid-row:1!important;display:inline-flex!important;gap:4px!important;}
#ops-card #sched-inline-log .sched.has-copy .meta:empty,#ops-card #sched-inline-log .sched.has-copy .badges:empty{display:none!important;}
#ops-card #sched-inline-log .sched[data-tip]{cursor:help!important;}
#ops-card #sched-inline-log .sched[data-nav]{cursor:pointer!important;}
#ops-card #sched-inline-log .sched[data-tip]:hover,#ops-card #sched-inline-log .sched[data-tip]:focus-visible{background:var(--hub-action-hover)!important;border-color:color-mix(in srgb,var(--service-state) 42%,var(--hub-card-border))!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 8px 20px rgba(0,0,0,.18)!important;transform:translateY(-1px)!important;outline:none!important;}
#ops-card #sched-inline-log .sched[data-tip]::after{z-index:40!important;bottom:calc(100% + 11px)!important;width:max-content!important;min-width:280px!important;max-width:min(760px,calc(100vw - 32px))!important;padding:10px 12px!important;border-radius:10px!important;background:var(--hub-card-bg)!important;border:1px solid color-mix(in srgb,var(--service-state) 34%,var(--hub-card-border))!important;box-shadow:0 14px 32px rgba(0,0,0,.34)!important;color:var(--hub-text)!important;font-size:11px!important;line-height:1.5!important;letter-spacing:.01em!important;white-space:normal!important;}
#ops-card #sched-inline-log .sched[data-tip]:hover::after,#ops-card #sched-inline-log .sched[data-tip]:focus-visible::after{opacity:1!important;transform:translateX(-50%) translateY(0)!important;}
#ops-card #sched-inline-log .hub-status-group:first-child .sched:first-child[data-tip]::after{left:0!important;transform:translateX(0) translateY(4px)!important;}
#ops-card #sched-inline-log .hub-status-group:first-child .sched:first-child[data-tip]:hover::after,#ops-card #sched-inline-log .hub-status-group:first-child .sched:first-child[data-tip]:focus-visible::after{transform:translateX(0) translateY(0)!important;}
#ops-card #sched-inline-log .hub-group-system .sched:last-child[data-tip]::after{left:auto!important;right:0!important;transform:translateX(0) translateY(4px)!important;}
#ops-card #sched-inline-log .hub-group-system .sched:last-child[data-tip]:hover::after,#ops-card #sched-inline-log .hub-group-system .sched:last-child[data-tip]:focus-visible::after{transform:translateX(0) translateY(0)!important;}
#ops-card #sched-inline-log .sched[data-tip]::after{display:none!important;}
#ops-card #sched-inline-log .cw-hub-tip{position:absolute;z-index:40;left:50%;bottom:calc(100% + 11px);display:grid;gap:2px;width:max-content;min-width:280px;max-width:min(760px,calc(100vw - 32px));padding:10px 12px;border-radius:10px;background:var(--hub-card-bg);border:1px solid color-mix(in srgb,var(--service-state) 34%,var(--hub-card-border));box-shadow:0 14px 32px rgba(0,0,0,.34);color:var(--hub-text);font-size:11px;line-height:1.5;letter-spacing:.01em;text-align:left;white-space:normal;opacity:0;pointer-events:none;transform:translateX(-50%) translateY(4px);transition:opacity .16s ease,transform .16s ease;}
#ops-card #sched-inline-log .sched[data-tip]:hover>.cw-hub-tip,#ops-card #sched-inline-log .sched[data-tip]:focus-visible>.cw-hub-tip{opacity:1;transform:translateX(-50%) translateY(0);}
#ops-card #sched-inline-log .cw-hub-tip-line{display:block;font-weight:750;overflow:visible;text-overflow:clip;white-space:normal;}
#ops-card #sched-inline-log .cw-hub-tip-action{display:block;margin-top:5px;padding-top:6px;border-top:1px solid color-mix(in srgb,var(--hub-muted) 24%,transparent);color:color-mix(in srgb,var(--service-state) 72%,var(--hub-text));font-weight:500;}
#ops-card #sched-inline-log .hub-status-group:first-child .sched:first-child .cw-hub-tip{left:0;transform:translateX(0) translateY(4px);}
#ops-card #sched-inline-log .hub-status-group:first-child .sched:first-child[data-tip]:hover>.cw-hub-tip,#ops-card #sched-inline-log .hub-status-group:first-child .sched:first-child[data-tip]:focus-visible>.cw-hub-tip{transform:translateX(0) translateY(0);}
#ops-card #sched-inline-log :is(#chip-watch,#chip-hook) .cw-hub-tip{left:0;right:auto;width:min(360px,calc(100vw - 32px));min-width:0;max-width:calc(100vw - 32px);transform:translateX(0) translateY(4px);}
#ops-card #sched-inline-log :is(#chip-watch,#chip-hook)[data-tip]:hover>.cw-hub-tip,#ops-card #sched-inline-log :is(#chip-watch,#chip-hook)[data-tip]:focus-visible>.cw-hub-tip{transform:translateX(0) translateY(0);}
#ops-card #sched-inline-log .hub-group-system .sched:last-child .cw-hub-tip{left:auto;right:0;transform:translateX(0) translateY(4px);}
#ops-card #sched-inline-log .hub-group-system .sched:last-child[data-tip]:hover>.cw-hub-tip,#ops-card #sched-inline-log .hub-group-system .sched:last-child[data-tip]:focus-visible>.cw-hub-tip{transform:translateX(0) translateY(0);}
html[data-cw-theme="flat-dark"] #ops-card{--hub-service-good:#57b58a;--hub-service-bad:#d86672;--hub-service-disabled:#7c8491;--hub-service-update:#a78bfa;--hub-action-bg:#20242d;--hub-action-hover:#272c36;--hub-card-bg:#20242d;--hub-card-border:rgba(255,255,255,.13);--hub-text:#eef1f6;--hub-muted:#a9b0bd;}
html[data-cw-theme="flat-light"] #ops-card{--hub-service-good:#276348;--hub-service-bad:#a93f4d;--hub-service-disabled:#98a2b3;--hub-service-update:#6d3fd1;--hub-action-bg:#fff;--hub-action-hover:#eef2f7;--hub-card-bg:#fff;--hub-card-border:rgba(16,24,40,.16);--hub-text:#172033;--hub-muted:#667085;}
html.cw-theme-original #ops-card{--hub-service-good:#57b58a;--hub-service-bad:#e06470;--hub-service-disabled:#77808f;--hub-service-update:#8b5cf6;--hub-action-bg:rgba(255,255,255,.035);--hub-action-hover:rgba(255,255,255,.065);--hub-card-bg:linear-gradient(180deg,rgba(20,24,34,.96),rgba(12,15,23,.98));--hub-card-border:rgba(255,255,255,.09);--hub-text:#eef3ff;--hub-muted:rgba(188,198,217,.68);}
#ops-card #sched-inline-log #chip-update.update-available{--service-state:var(--hub-service-update,#8b5cf6)!important;}
#ops-card #sched-inline-log #chip-update.update-available .service-icon{opacity:1!important;color:var(--service-state)!important;font-variation-settings:"FILL" 1,"wght" 500,"GRAD" 0,"opsz" 24!important;transform-origin:50% 5px;animation:cwHubBellRing 3.6s ease-in-out infinite;}
#ops-card #sched-inline-log #chip-update.update-available::before{opacity:1!important;box-shadow:0 0 10px color-mix(in srgb,var(--service-state) 55%,transparent)!important;animation:cwHubBellBar 3.6s ease-in-out infinite;}
#ops-card #sched-inline-log #chip-update.update-available::after{content:""!important;display:block!important;position:absolute!important;z-index:3!important;top:5px!important;right:9px!important;left:auto!important;bottom:auto!important;width:7px!important;height:7px!important;min-width:0!important;max-width:none!important;margin:0!important;padding:0!important;border:2px solid var(--hub-action-bg)!important;border-radius:50%!important;background:var(--service-state)!important;opacity:1!important;transform:none!important;pointer-events:none!important;animation:cwHubBellDot 2.4s ease-out infinite;}
@keyframes cwHubBellRing{0%,60%,100%{transform:rotate(0)}64%{transform:rotate(14deg)}68%{transform:rotate(-12deg)}72%{transform:rotate(9deg)}76%{transform:rotate(-7deg)}80%{transform:rotate(4deg)}84%{transform:rotate(-2deg)}88%{transform:rotate(0)}}
@keyframes cwHubBellBar{0%,60%,100%{opacity:1}70%{opacity:.45}80%{opacity:1}}
@keyframes cwHubBellDot{0%{box-shadow:0 0 0 0 color-mix(in srgb,var(--service-state) 60%,transparent)}70%{box-shadow:0 0 0 7px color-mix(in srgb,var(--service-state) 0%,transparent)}100%{box-shadow:0 0 0 0 color-mix(in srgb,var(--service-state) 0%,transparent)}}
@keyframes cwHubSyncSpin{to{transform:rotate(360deg)}}
@media(prefers-reduced-motion:reduce){#ops-card #sched-inline-log #chip-update.update-available .service-icon,#ops-card #sched-inline-log #chip-update.update-available::before,#ops-card #sched-inline-log #chip-update.update-available::after{animation:none!important;}}
@media(max-width:760px){#ops-card .action-buttons{width:100%!important;flex-wrap:wrap!important;}#ops-card .action-buttons>.cw-split-run,#ops-card .action-buttons>.cw-hub-action{flex:1 1 150px!important;}#sched-inline-log{flex-wrap:wrap!important;}#ops-card #sched-inline-log .hub-group-system{margin-left:0;}}
@media(max-width:480px){#ops-card .action-buttons>.cw-split-run,#ops-card .action-buttons>.cw-hub-action{flex:1 1 100%!important;}#ops-card #sched-inline-log .sched[data-tip]::after{left:0!important;transform:translateX(0) translateY(4px)!important;}#ops-card #sched-inline-log .sched[data-tip]:hover::after,#ops-card #sched-inline-log .sched[data-tip]:focus-visible::after{transform:translateX(0) translateY(0)!important;}}
`;
    document.head.appendChild(st);
  }

  const findBox=()=>$("#ops-card>.cw-status-dock")||(()=>{
    const card=$("#ops-card"), row=$("#ops-card .action-row"), existing=$("#ops-card .cw-status-dock");
    if (card&&row) {
      const dock=existing||Object.assign(document.createElement("div"),{className:"cw-status-dock"});
      if (dock.parentElement!==card||dock.previousElementSibling!==row) row.insertAdjacentElement("afterend",dock);
      return dock;
    }
    for (const s of ["#ops-card","#ops-out","#ops_log","#sync-output",".sync-output","#ops"]) { const n=$(s); if (n) return n; }
    const h=[...document.querySelectorAll("h2,h3,h4,div.head,.head")].find(x=>(x.textContent||"").trim().toUpperCase()==="SYNC OUTPUT");
    return h?.parentElement?.querySelector("pre,textarea,.box,.card,div")||null;
  })();

  function activateStatusTarget(target){
    if (target==="maintenance") return window.openMaintenanceModal?.();
    if (target==="refresh-update") return CW().checkForUpdate?.();
    window.showTab?.("settings");
    setTimeout(()=>{
      if (target==="scheduling") return window.cwSettingsSelect?.("scheduling");
      if (target==="watcher") return window.cwScrobblerJump?.("sc-sec-watch");
      if (target==="webhook") return window.cwScrobblerJump?.("sc-sec-webhook");
      if (target==="pairs") return window.cwSyncJump?.();
    },0);
  }

  function handleStatusNavigation(event){
    if (event.type==="keydown"&&!['Enter',' '].includes(event.key)) return;
    const chip=event.target?.closest?.(".sched[data-nav]");
    if (!chip||!event.currentTarget.contains(chip)) return;
    event.preventDefault();
    activateStatusTarget(chip.dataset.nav);
  }

  function ensureBanner(){
    const host=findBox();
    if (!host) return null;
    if (host.id!=="sched-inline-log" && getComputedStyle(host).position==="static") host.style.position="relative";
    let wrap=host.id==="sched-inline-log"?host:$("#sched-inline-log",host);
    if (!wrap) {
      wrap=document.createElement("div");
      wrap.id="sched-inline-log";
      const statusTile=k=>{const nav=!isManaged()?chipNav[k]:null;return `<div class="sched idle${["sched","watch","hook"].includes(k)?" has-copy":""}" id="chip-${k}" role="${nav?"button":"status"}"${nav?` tabindex="0" data-nav="${nav.target}" data-nav-label="${nav.label}"`:""}><span class="ic service-icon material-symbols-rounded" aria-hidden="true">${chipIcon[k]}</span><span class="copy"><span class="label">${chipText[k]}</span><span class="value" id="${k}-value">-</span><span class="meta" id="${k}-meta"></span><span class="badges" id="${k}-badges"></span></span>${["watch","hook"].includes(k)?`<span class="watch-progress" aria-hidden="true"><span class="watch-progress-value" id="${k}-progress-value"></span></span>`:""}<span class="cw-hub-tip" aria-hidden="true"></span></div>`};
      wrap.innerHTML=`
        <div class="hub-status-group hub-group-monitoring">
          <div class="hub-group-items">${(isManaged()?["watch","hook"]:["sched","watch","hook"]).map(statusTile).join("")}</div>
        </div>
        <div class="hub-status-group hub-group-system">
          <div class="hub-group-items">${["pairs","health","update"].map(statusTile).join("")}</div>
        </div>`;
      host.appendChild(wrap);
    }
    if (!wrap.dataset.navigationBound) {
      wrap.dataset.navigationBound="1";
      wrap.addEventListener("click",handleStatusNavigation);
      wrap.addEventListener("keydown",handleStatusNavigation);
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

  const sourceLabel=(src)=>{
    const s=String(src||"").toLowerCase();
    const pm=CW()?.ProviderMeta;
    const base=s.replace(/trakt$/,"");
    const viaMeta=typeof pm?.label==="function"?pm.label(base):"";
    if (viaMeta && viaMeta !== "?") return s.endsWith("trakt") ? `${viaMeta} webhook` : viaMeta;
    if (s==="plex") return "Plex";
    if (s==="emby") return "Emby";
    if (s==="jellyfin") return "Jellyfin";
    if (s==="plextrakt") return "Plex webhook";
    if (s==="embytrakt") return "Emby webhook";
    if (s==="jellyfintrakt") return "Jellyfin webhook";
    return String(src||"");
  };
  const instanceLabel=(value)=>{
    const raw=String(value||"").trim();
    if (!raw || raw.toLowerCase()==="default") return "Default";
    return raw;
  };
  const streamTitle=(item)=>{
    const title=String(item?.title||"").trim()||"Untitled";
    const season=item?.season==null||item.season===""?null:Number(item.season);
    const episode=item?.episode==null||item.episode===""?null:Number(item.episode);
    if (!Number.isInteger(season)||season<0||!Number.isInteger(episode)||episode<0) return title;
    return `${title} - S${String(season).padStart(2,"0")}E${String(episode).padStart(2,"0")}`;
  };
  const visualProgress=(item,nowMs=Date.now())=>{
    const base=Math.max(0,Math.min(100,Number(item?.progress)||0));
    if (String(item?.state||"").toLowerCase()!=="playing") return base;
    const durationMs=Number(item?.duration_ms)||0,updatedSec=Number(item?.updated)||0;
    if (!(durationMs>0)||!(updatedSec>0)) return base;
    const serverTs=Number(item?._server_ts)||0,receivedAt=Number(item?._received_at_ms)||0;
    const nowSec=serverTs&&receivedAt?serverTs+Math.max(0,nowMs-receivedAt)/1000:nowMs/1000;
    return Math.max(base,Math.min(100,base+(Math.max(0,nowSec-updatedSec)*100000/durationMs)));
  };
  const streamSummary=(item)=>{
    if (!item || typeof item!=="object") return "";
    const src=sourceLabel(item.source);
    const inst=instanceLabel(item.provider_instance);
    const title=streamTitle(item);
    const pct=visualProgress(item);
    const sourceLine=[src,inst].filter(Boolean).join(" \u2022 ");
    const mediaLine=[title,Number.isFinite(pct)?`${Math.round(pct)}%`:""].filter(Boolean).join(" \u2022 ");
    return [sourceLine,mediaLine].filter(Boolean).join("\n");
  };
  const currentItem=(bucket)=>{
    const items=Array.isArray(bucket?.items)?bucket.items:[];
    if (!items.length) return null;
    const idx=((Number(bucket?.index)||0)%items.length+items.length)%items.length;
    return items[idx]||items[0]||null;
  };
  const isWebhookStream=(item)=>{
    const source=String(item?.source||"").trim().toLowerCase();
    return source.includes("webhook")||["plextrakt","embytrakt","jellyfintrakt"].includes(source);
  };
  const tooltipFor=(label,items)=>[label,...(Array.isArray(items)?items.map(streamSummary).filter(Boolean):[])].join("\n");

  const renderTip=(el,tip,action)=>{
    if (!el?.tip) return;
    el.tip.replaceChildren();
    String(tip||"").split("\n").map(line=>line.trim()).filter(Boolean).forEach(line=>{
      const row=document.createElement("span");
      row.className="cw-hub-tip-line";
      row.textContent=line;
      el.tip.appendChild(row);
    });
    if (action) {
      const row=document.createElement("span");
      row.className="cw-hub-tip-action";
      row.textContent=action;
      el.tip.appendChild(row);
    }
  };

  function emit(source,detail){
    const payload={source,...detail}, key=JSON.stringify(payload);
    if (S.last[source]===key) return;
    let previous=null;
    try { previous=S.last[source]?JSON.parse(S.last[source]):null; } catch {}
    S.last[source]=key;
    try { window.dispatchEvent(new CustomEvent("currently-watching-updated",{detail:payload})); } catch {}
    const wasActive=previous&&String(previous.state||"").toLowerCase()!=="stopped";
    const isStopped=String(payload.state||"").toLowerCase()==="stopped";
    if (["watcher","webhook"].includes(source)&&wasActive&&isStopped) {
      try { window.dispatchEvent(new CustomEvent("cw:scrobble-stopped",{detail:{source,previous,stopped_at:Date.now()}})); } catch {}
    }
  }

  function paint(el,{show=true,value="-",meta="",badges=[],live=false,ok=true,idle=false,disabled=false,progress=null,tip=""}={}){
    if (!el?.chip) return;
    el.chip.classList.toggle("is-hidden",!show);
    if (!show) {
      if (el.badges) {
        el.badges.innerHTML="";
        el.badges.style.display="none";
      }
      el.tip?.replaceChildren();
      return el.chip.removeAttribute("data-tip"), el.chip.removeAttribute("title");
    }
    const healthy=!!ok&&!idle;
    el.chip.classList.toggle("ok",healthy);
    el.chip.classList.toggle("live",!!live);
    el.chip.classList.toggle("idle",!!idle);
    el.chip.classList.toggle("disabled",!!disabled);
    el.chip.classList.toggle("warn",!idle&&!ok&&!disabled);
    if (el.progress) {
      const n=Number(progress), hasProgress=live&&progress!==null&&progress!==""&&Number.isFinite(n);
      const pct=hasProgress?Math.max(0,Math.min(100,n)):0;
      el.chip.classList.toggle("has-progress",hasProgress);
      el.progress.style.setProperty("--watch-progress-angle",`${pct*3.6}deg`);
      if (el.progressValue) el.progressValue.textContent=hasProgress?`${Math.round(pct)}%`:"";
    }
    el.value.textContent=value==null?"-":String(value);
    const schedWarnings=(Array.isArray(S.sched?.warnings)?S.sched.warnings:[]).map(x=>String(x||"").trim()).filter(Boolean);
    const schedHasWarning=el.chip?.id==="chip-sched"&&(!!S.sched?.warning||schedWarnings.length>0);
    if (schedHasWarning) {
      el.value.innerHTML=`<span class="material-symbols-rounded cw-sched-warning-icon" aria-hidden="true">warning</span>`;
      el.value.setAttribute("aria-label","Scheduler issue");
    } else {
      el.value.removeAttribute("aria-label");
    }
    el.meta.textContent=meta||"";
    el.meta.style.display=meta?"inline":"none";
    if (el.badges) {
      const badgeHtml=(Array.isArray(badges)?badges:[]).filter(Boolean).map(x=>`<span class="badge">${String(x)}</span>`).join("");
      el.badges.innerHTML=badgeHtml;
      el.badges.style.display=badgeHtml?"inline-flex":"none";
    }
    tip=String(tip||"").trim().replace(/\s*\u2022\s*/,"\n");
    if (el.chip?.id==="chip-sched"&&schedWarnings.length) tip=[tip,...schedWarnings.map(text=>`Warning: ${text}`)].filter(Boolean).join("\n");
    const action=el.chip.dataset.navLabel||"";
    const aria=[tip,action].filter(Boolean).join("\n");
    renderTip(el,tip,action);
    el.chip.removeAttribute("title");
    aria?(el.chip.dataset.tip=aria,el.chip.setAttribute("aria-label",aria)):(el.chip.removeAttribute("data-tip"),el.chip.removeAttribute("aria-label"));
  }

  function render(){
    const host=ensureBanner();
    if (!host) return;
    const E=k=>{const chip=$(`#chip-${k}`,host);return {chip,icon:chip?$(".service-icon",chip):null,value:$(`#${k}-value`,host),meta:$(`#${k}-meta`,host),badges:$(`#${k}-badges`,host),tip:chip?$(".cw-hub-tip",chip):null,progress:["watch","hook"].includes(k)&&chip?$(".watch-progress",chip):null,progressValue:["watch","hook"].includes(k)?$(`#${k}-progress-value`,host):null};},
      pairs=E("pairs"), sched=E("sched"), watch=E("watch"), hook=E("hook"), health=E("health"), update=E("update"),
      managed=isManaged(),
      watchItem=currentItem(S.watch),
      hookItem=currentItem(S.hook),
      watchLive=!!(watchItem&&S.watch.alive),
      hookLive=!!hookItem,
      schedBadges=[S.sched.captures?`C${S.sched.captures}`:"",S.evt.enabled&&S.evt.count?`E${S.evt.count}`:""].filter(Boolean),
      schedWarnings=(Array.isArray(S.sched.warnings)?S.sched.warnings:[]).map(x=>String(x||"").trim()).filter(Boolean),
      schedWarning=!!S.sched.warning||schedWarnings.length>0,
      schedWebhookTip=S.sched.webhooks?` • scheduler webhooks active${S.sched.webhookLabel?` (${S.sched.webhookLabel})`:""}`:"";

    const pairTotal=Number(S.pairs?.total)||0, pairActive=Number(S.pairs?.active)||0;
    paint(pairs,pairTotal===0?{
      value:"none", ok:false, idle:true,
      tip:managed?"Sync pairs\nStatus: none assigned":"Sync pairs\nStatus: none configured\nCreate a sync pair to enable synchronization"
    }:pairActive===0?{
      value:"available", ok:false, disabled:true,
      tip:`Sync pairs\nStatus: available but disabled\nConfigured: ${pairTotal}\nEnabled: 0`
    }:{
      value:"active", ok:true,
      tip:`Sync pairs\nStatus: active\nEnabled: ${pairActive} of ${pairTotal}`
    });

    paint(sched,managed||!S.sched.enabled?{
      show:false
    }:{
      value:schedWarning?"":(S.sched.running?"":(S.sched.advanced?"advanced":"scheduled")),
      meta:S.sched.next?`next ${clock(S.sched.next,true)}`:"",
      badges:schedBadges,
      live:S.sched.running&&!schedWarning,
      ok:!schedWarning,
      tip:`Scheduler ${S.sched.running?"running":(S.sched.advanced?"advanced":"scheduled")}${S.sched.next?` • next ${clock(S.sched.next,true)}`:""}${S.sched.advanced?" • advanced mode":""}${S.sched.captures?` • ${S.sched.captures} capture schedule${S.sched.captures===1?"":"s"}`:""}${S.evt.enabled&&S.evt.count?` • ${S.evt.count} event trigger${S.evt.count===1?"":"s"}`:""}${schedWebhookTip}`
    });

    paint(watch,!S.watch.enabled?{
      show:false
    }:{
      value:S.watch.alive?"":"unavailable",
      meta:watchLive?streamTitle(watchItem):"",
      badges:S.watch.streams>1?[S.watch.streams]:[],
      progress:watchLive?visualProgress(watchItem):null,
      live:watchLive, ok:S.watch.alive, idle:!S.watch.alive,
      tip:watchLive?tooltipFor(`Watcher • ${S.watch.streams} active stream${S.watch.streams===1?"":"s"}`,S.watch.items):`Watcher ${S.watch.alive?"running":"idle"}${S.watch.state?` • state ${S.watch.state}`:""}`
    });
    emit("watcher",watchLive?{title:watchItem.title,progress:Number(watchItem.progress)||0,state:watchItem.state||"playing",_streams_count:S.watch.streams}:{state:"stopped"});

    paint(hook,!S.hook.enabled?{
      show:false
    }:{
      value:"",
      meta:hookLive?streamTitle(hookItem):"",
      badges:S.hook.streams>1?[S.hook.streams]:[],
      progress:hookLive?visualProgress(hookItem):null,
      live:hookLive, ok:true, idle:false,
      tip:hookLive?tooltipFor(`Webhook • ${S.hook.streams} active stream${S.hook.streams===1?"":"s"}`,S.hook.items):"Webhook listening"
    });
    emit("webhook",hookLive?{title:hookItem.title,progress:Number(hookItem.progress)||0,state:hookItem.state||"playing",_streams_count:S.hook.streams}:{state:"stopped"});
    $(".hub-group-monitoring",host)?.classList.toggle("is-hidden",[(managed?null:sched),watch,hook].filter(Boolean).every(item=>item.chip?.classList.contains("is-hidden")));

    const healthState=S.system.health||{};
    if (health.icon) health.icon.textContent=healthState.known&&!healthState.ok?"arrow_downward":"arrow_upward";
    paint(health,!healthState.known?{
      value:"checking", ok:false, disabled:true,
      tip:"CrossWatch health\nStatus: checking"
    }:healthState.ok?{
      value:"up", ok:true,
      tip:"CrossWatch health\nStatus: up"
    }:{
      value:"down", ok:false,
      tip:["CrossWatch health","Status: down",healthState.status&&healthState.status!=="down"?`Response: ${healthState.status}`:""].filter(Boolean).join("\n")
    });

    const updateState=S.system.update||{};
    paint(update,managed?{
      value:"admin", ok:false, disabled:true,
      tip:"Updates\nManaged by administrator"
    }:!updateState.known?{
      value:"checking", ok:false, disabled:true,
      tip:"Updates\nStatus: checking"
    }:updateState.available?{
      value:"available", ok:false, disabled:true,
      tip:["Update available",updateState.latest?`Latest: ${updateState.latest}`:"",updateState.current?`Installed: ${updateState.current}`:""].filter(Boolean).join("\n")
    }:updateState.ahead?{
      value:"ahead", ok:true,
      tip:["Updates","Status: newer than latest release",updateState.current?`Installed: ${updateState.current}`:"",updateState.latest?`Latest release: ${updateState.latest}`:""].filter(Boolean).join("\n")
    }:updateState.unavailable?{
      value:"unknown", ok:false, disabled:true,
      tip:["Updates","Status: GitHub release check unavailable",updateState.current?`Installed: ${updateState.current}`:""].filter(Boolean).join("\n")
    }:{
      value:"current", ok:true,
      tip:["Updates","Status: up to date",updateState.current?`Installed: ${updateState.current}`:""].filter(Boolean).join("\n")
    });
    update.chip?.classList.toggle("update-available",!!updateState.available);

    host.style.display="flex";
  }

  async function readConfig(force=false){
    try { return await API()?.Config.load(force)||{}; } catch { return Cache()?.getCfg()||{}; }
  }

  function applyUpdateStatus(detail=window.__CW_UPDATE_STATUS__){
    if (!detail||typeof detail!=="object") return;
    const publicVersion=value=>{
      const raw=String(value||"").trim(),match=raw.match(/(?:^|[^0-9])(\d+\.\d+\.\d+)(?:[^0-9]|$)/);
      return match?`v${match[1]}`:raw;
    };
    const versionParts=value=>String(value||"").match(/\d+/g)?.map(Number)||[];
    const compareVersions=(left,right)=>{
      const a=versionParts(left),b=versionParts(right),length=Math.max(a.length,b.length);
      for(let index=0;index<length;index+=1){
        const diff=(a[index]||0)-(b[index]||0);
        if(diff) return diff>0?1:-1;
      }
      return 0;
    };
    const current=publicVersion(detail.current||detail.current_version||"");
    const latest=publicVersion(detail.latest||detail.latest_version||"");
    S.system.update={
      known:detail.known!==false,
      available:!!(detail.available??detail.update_available),
      ahead:!!(current&&latest&&compareVersions(current,latest)>0),
      current,
      latest,
      unavailable:detail.unavailable===true||!latest,
      url:String(detail.url||detail.html_url||"")
    };
  }

  async function pollPairs(force=false){
    let list=Array.isArray(window.cx?.pairs)?window.cx.pairs:[];
    try {
      const fresh=await API()?.Pairs?.list(force);
      if (Array.isArray(fresh)) list=fresh;
    } catch {}
    S.pairs={
      total:list.length,
      active:list.filter(pair=>pair&&pair.enabled!==false).length
    };
  }

  async function pollHealth(){
    clear("health");
    if (document.hidden) return scheduleHealth();
    try {
      const response=await fetch("/api/health",{cache:"no-store",headers:{Accept:"application/json"}});
      const data=await response.json().catch(()=>({}));
      const status=String(data?.status||"").trim().toLowerCase();
      const ok=response.ok&&data?.ok===true&&status==="ok";
      S.system.health={known:true,ok,status:status||response.statusText||"unknown"};
    } catch {
      S.system.health={known:true,ok:false,status:"unreachable"};
    }
    render();
    scheduleHealth();
  }

  async function pollSched(force=false){
    clear("sched");
    if (document.hidden) return scheduleSched();
    try {
      const st=await API().Scheduling.status(force), sc=st?.config||S.cfg?.scheduling||{};
      const adv=advancedOn(sc), captures=adv?activeCaptureJobs(sc):0, events=adv?activeEventRules(sc):0, wh=webhookState(sc);
      const warnings=(Array.isArray(st?.scheduling_warnings)?st.scheduling_warnings:(Array.isArray(st?.warnings)?st.warnings:[])).map(x=>String(x||"").trim()).filter(Boolean);
      S.sched={enabled:schedulingOn(sc),advanced:adv,running:!!st?.running,next:+(st?.next_run_at||st?.next_run||0)||0,captures:captures,webhooks:!!wh.active,webhookLabel:wh.label||"",warning:!!st?.warning||warnings.length>0,warnings};
      S.evt={enabled:adv&&events>0,count:events};
    } catch { S.sched={enabled:false,running:false,next:0,advanced:false,captures:0,webhooks:false,webhookLabel:"",warning:false,warnings:[]}; S.evt={enabled:false,count:0}; }
    render();
    scheduleSched();
  }

  async function pollScrob(force=false){
    clear("scrob");
    const pollSeq=++scrobPollSeq;
    const sc=S.cfg?.scrobble||{}, sources=scrobSources(), enabled=!!sc.enabled;
    const previousWatch=S.watch||{}, previousHook=S.hook||{};
    const watchEnabled=enabled&&sources.watcher, hookEnabled=enabled&&sources.webhook;
    const nextWatch={
      ...blank(),
      enabled:watchEnabled,
      alive:!!previousWatch.alive,
      lastHealthyAt:Number(previousWatch.lastHealthyAt)||0,
      items:watchEnabled&&Array.isArray(previousWatch.items)?previousWatch.items:[],
      streams:watchEnabled?(Number(previousWatch.streams)||0):0,
      title:watchEnabled?String(previousWatch.title||""):"",
      state:watchEnabled?(previousWatch.state||null):null,
      index:watchEnabled?(Number(previousWatch.index)||0):0
    };
    const nextHook={
      ...blank(),
      enabled:hookEnabled,
      items:hookEnabled&&Array.isArray(previousHook.items)?previousHook.items:[],
      streams:hookEnabled?(Number(previousHook.streams)||0):0,
      title:hookEnabled?String(previousHook.title||""):"",
      state:hookEnabled?(previousHook.state||null):null,
      index:hookEnabled?(Number(previousHook.index)||0):0
    };
    if (!enabled) {
      S.watch=nextWatch;
      S.hook=nextHook;
      clear("rotate");
      return render();
    }
    if (document.hidden) return scheduleScrob();
    const now=Date.now();
    let watcherReportedAlive=false;
    try {
      if (nextWatch.enabled) watcherReportedAlive=!!(await API().Watch.status(force))?.alive;
      if (watcherReportedAlive) nextWatch.lastHealthyAt=now;
      const shared=!force?window[SHARED_WATCH_KEY]:null;
      const sharedFresh=shared&&typeof shared==="object"&&(now-(Number(shared.at)||0))<SHARED_WATCH_TTL_MS;
      const cw=sharedFresh
        ? (shared.payload||null)
        : await API().Watch.currentlyWatching(force).catch(()=>null);
      if (cw&&typeof cw==="object") {
        if (!sharedFresh) window[SHARED_WATCH_KEY]={at:now,payload:cw};
        const all=Array.isArray(cw.streams)?cw.streams.filter(x=>x&&typeof x==="object"):[];
        const serverTs=Number(cw.ts)||0,receivedAt=Date.now();
        all.forEach(item=>{if(serverTs)item._server_ts=serverTs;item._received_at_ms=receivedAt;});
        const watchItems=all.filter(it=>!isWebhookStream(it));
        const hookItems=all.filter(isWebhookStream);
        Object.assign(nextWatch,{items:watchItems,streams:watchItems.length,title:String(watchItems[0]?.title||""),state:watchItems[0]?.state||null,index:Math.min(Number(nextWatch.index)||0,Math.max(0,watchItems.length-1))});
        Object.assign(nextHook,{items:hookItems,streams:hookItems.length,title:String(hookItems[0]?.title||""),state:hookItems[0]?.state||null,index:Math.min(Number(nextHook.index)||0,Math.max(0,hookItems.length-1))});
      }
    } catch {}
    if (pollSeq!==scrobPollSeq) return;
    if (nextWatch.enabled) nextWatch.alive=watcherReportedAlive||!!(nextWatch.lastHealthyAt&&now-nextWatch.lastHealthyAt<=WATCHER_UNAVAILABLE_GRACE_MS);
    S.watch=nextWatch;
    S.hook=nextHook;
    render();
    scheduleRotate();
    scheduleScrob();
  }

  const scheduleHealth=()=>S.timers.health=setTimeout(()=>pollHealth(),30000),
    scheduleSched=()=>S.sched.enabled&&!isManaged()&&(S.timers.sched=setTimeout(()=>pollSched(false),30000)),
    scheduleScrob=()=>S.cfg?.scrobble?.enabled&&(S.timers.scrob=setTimeout(()=>pollScrob(false),scrobSources().webhook?5000:15000)),
    stop=()=>["sched","scrob","health","rotate"].forEach(clear);

  function scheduleRotate(){
    clear("rotate");
    const canRotate=(S.watch.streams>1)||(S.hook.streams>1);
    if (!canRotate) return;
    S.timers.rotate=setTimeout(()=>{
      if (S.watch.streams>1) S.watch.index=(Number(S.watch.index)||0)+1;
      if (S.hook.streams>1) S.hook.index=(Number(S.hook.index)||0)+1;
      render();
      scheduleRotate();
    },30000);
  }

  async function refresh(forceCfg=false){
    stop();
    [S.cfg]=await Promise.all([readConfig(forceCfg),pollPairs(forceCfg),pollHealth()]);
    applyUpdateStatus();
    if ((isManaged()||!schedulingOn(S.cfg?.scheduling)) && !S.cfg?.scrobble?.enabled) {
      S.sched={enabled:false,running:false,next:0,advanced:false,captures:0,webhooks:false,webhookLabel:"",warning:false,warnings:[]};
      S.evt={enabled:false,count:0};
      S.watch={...blank(),alive:false};
      S.hook=blank();
      clear("rotate");
      return render();
    }
    !isManaged()&&schedulingOn(S.cfg?.scheduling)?await pollSched(true):(S.sched={enabled:false,running:false,next:0,advanced:false,captures:0,webhooks:false,webhookLabel:"",warning:false,warnings:[]},S.evt={enabled:false,count:0});
    S.cfg?.scrobble?.enabled?await pollScrob(true):(S.watch.enabled=S.hook.enabled=false,render());
  }

  function queueRefresh(forceCfg=false){
    clearTimeout(S.debounce);
    S.debounce=setTimeout(()=>{
      if (forceCfg) try { Cache()?.invalidate("config"); } catch {}
      refresh(forceCfg);
    },150);
  }

  const refreshPairs=()=>pollPairs(true).then(render).catch(()=>{});

  function boot(){
    if (window.__SCHED_BANNER_STARTED__) return;
    window.__SCHED_BANNER_STARTED__=true;
    queueRefresh(true);
    document.addEventListener("visibilitychange",()=>!document.hidden&&queueRefresh(false),{passive:true});
    document.addEventListener("config-saved",()=>queueRefresh(true));
    window.addEventListener("cx:pairs:changed",refreshPairs);
    document.addEventListener("cx-state-change",refreshPairs);
    document.addEventListener("cw-update-status",event=>{applyUpdateStatus(event?.detail);render();});
    window.addEventListener("auth-changed",()=>queueRefresh(true));
    document.addEventListener("scheduling-status-refresh",()=>!isManaged()&&pollSched(true));
    document.addEventListener("watcher-status-refresh",()=>pollScrob(true));
    document.addEventListener("tab-changed",e=>["main","settings"].includes(e?.detail?.id)&&queueRefresh(false));
    window.addEventListener("focus",()=>queueRefresh(false));
    window.refreshSchedulingBanner=queueRefresh;
    S.timers.visual=setInterval(()=>{
      if (document.hidden) return;
      const items=[currentItem(S.watch),currentItem(S.hook)].filter(Boolean);
      if (items.some(item=>String(item?.state||"").toLowerCase()==="playing"&&Number(item?.duration_ms)>0)) render();
    },1000);
  }

  document.addEventListener("DOMContentLoaded",()=>{
    clear("wait");
    S.timers.wait=setInterval(()=>findBox()&&(clear("wait"),boot()),300);
  });
})();
