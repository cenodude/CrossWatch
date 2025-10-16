// Main UI logic
(()=> {
  const FEATS=[ {key:"watchlist",icon:"movie",label:"Watchlist"},
                {key:"ratings",icon:"star",label:"Ratings"},
                {key:"history",icon:"play_arrow",label:"History"},
                {key:"playlists",icon:"queue_music",label:"Playlists"} ];

  // DOM
  const elProgress=document.getElementById("ux-progress");
  const elLanes=document.getElementById("ux-lanes");
  const elSpot=document.getElementById("ux-spotlight");
  if(!elProgress||!elLanes||!elSpot) return;

  // Styles (lanes only)
  (document.getElementById("lanes-css")||{}).remove?.();
  document.head.appendChild(Object.assign(document.createElement("style"),{id:"lanes-css",textContent:`
#ux-lanes{margin-top:12px}
.lanes{display:grid;grid-template-columns:1fr;gap:10px}
@media (min-width:900px){.lanes{grid-template-columns:1fr 1fr}}
.lane{border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:10px 12px;background:rgba(255,255,255,.02);transition:transform .15s ease}
.lane.disabled{opacity:.45;filter:saturate(.5) brightness(.95)}
.lane.shake{animation:laneShake .42s cubic-bezier(.36,.07,.19,.97)}
@keyframes laneShake{10%,90%{transform:translateX(-1px)}20%,80%{transform:translateX(2px)}30%,50%,70%{transform:translateX(-4px)}40%,60%{transform:translateX(4px)}}
.lane-h{display:flex;align-items:center;gap:10px}
.lane-ico{font-size:18px;line-height:1}
.lane-title{font-weight:600;font-size:13px;opacity:.95}
.lane-badges{margin-left:auto;display:flex;gap:6px;align-items:center}
.chip{font-size:11px;padding:2px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.12);opacity:.9}
.chip.ok{border-color:rgba(0,220,130,.45);color:#4be3a6}
.chip.run{border-color:rgba(0,180,255,.45);color:#4dd6ff}
.chip.skip{border-color:rgba(255,255,255,.18);color:rgba(255,255,255,.7)}
.chip.err{border-color:rgba(255,80,80,.5);color:#ff7b7b}
.delta{font-size:11px;display:inline-flex;gap:6px;align-items:center;opacity:.9}
.delta b{font-weight:600}
.lane-body{margin-top:8px;display:grid;grid-template-columns:1fr;gap:6px}
.spot{font-size:12px;opacity:.95;display:flex;gap:8px;align-items:baseline}
.spot .tag{font-size:10px;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);opacity:.85}
.spot .t-add{color:#7cffc4;border-color:rgba(124,255,196,.25)}
.spot .t-rem{color:#ff9aa2;border-color:rgba(255,154,162,.25)}
.spot .t-upd{color:#9ecbff;border-color:rgba(158,203,255,.25)}
.muted{opacity:.7}
.small{font-size:11px}
#run[disabled]{pointer-events:none;opacity:.6;filter:saturate(.7);cursor:not-allowed}
#run.glass{position:relative}
#run.glass::after{content:"";position:absolute;inset:6px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .9s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
`}));

  // State
  const sync = new window.SyncBar({
    el: elProgress,
    onStart: ()=>startRunVisualsSafe(true),
    onStop: ()=>stopRunVisualsSafe()
  });

  let summary=null, enabledFromPairs=null, lastPairsAt=0;
  let _finishedForRun = null;
  let _prevRunKey = null;
  const runKeyOf = (s) => s?.run_id || s?.run_uuid || s?.raw_started_ts || (s?.started_at ? Date.parse(s.started_at) : null);
  const hydratedLanes=Object.create(null), lastCounts=Object.create(null), lastLaneTs={watchlist:0,ratings:0,history:0,playlists:0};

  // Safe hooks
  const startRunVisualsSafe=(...a)=>window.startRunVisuals?.(...a);
  const stopRunVisualsSafe=(...a)=>window.stopRunVisuals?.(...a);

  // IO
  const fetchJSON = async (url, fallback=null) => {
    try{ const r=await fetch(url+(url.includes("?")?"&":"?")+"_ts="+Date.now(),{credentials:"same-origin",cache:"no-store"}); return r.ok? r.json(): fallback; }
    catch{ return fallback; }
  };
  const fetchFirstJSON = async (urls, fallback=null) => { for(const u of urls){ const j=await fetchJSON(u,null); if(j) return j; } return fallback; };

  // Lanes helpers
  const titleOf=x=>typeof x==="string"?x:(x?.title||x?.name||x?.key||"item");
  const synthSpots=(items,key)=>{ const a=[],r=[],u=[]; for(const it of items||[]){ const t=titleOf(it); const act=(it?.action||it?.op||it?.change||"").toLowerCase(); let tag="upd";
    if(key==="history"&&(it?.watched||it?.watched_at||act.includes("watch")||act.includes("scrobble"))) tag="add";
    else if(key==="ratings"&&(act.includes("rate")||("rating"in(it||{})))) tag="add";
    else if(key==="playlists"&&(act.includes("add")||act.includes("playlist"))) tag="add";
    else if(act.includes("add")) tag="add"; else if(act.includes("rem")||act.includes("del")||act.includes("unwatch")) tag="rem";
    if(tag==="add"&&a.length<3) a.push(t); else if(tag==="rem"&&r.length<3) r.push(t); else if(u.length<3) u.push(t);
    if(a.length+r.length+u.length>=3) break; } return {a,r,u}; };

  const defaultEnabledMap=()=>({watchlist:true,ratings:true,history:true,playlists:true});
  const getEnabledMap=()=>enabledFromPairs ?? (summary?.enabled||defaultEnabledMap());

  const guardLaneOverwrite = (key, payload, ts) => {
    const sum = (+payload.added||0)+(+payload.removed||0)+(+payload.updated||0);
    const prev = hydratedLanes[key];
    const prevSum = (prev? ((+prev.added||0)+(+prev.removed||0)+(+prev.updated||0)) : 0);
    if (sync.isRunning() && sum===0 && prevSum>0) return false; // ignore zero-wipes mid-run
    if ((ts||0) < (lastLaneTs[key]||0)) return false;           // drop stale
    lastLaneTs[key] = ts||Date.now();
    return true;
  };

  const getLaneStats=(sum,key)=>{
    const f=(sum?.features?.[key])||sum?.[key]||{};
    const added=(f.added??0)|0, removed=(f.removed??0)|0, updated=(f.updated??0)|0;
    const items=Array.isArray(f.items)?f.items:[];
    let spotAdd=Array.isArray(f.spotlight_add)?f.spotlight_add:[],
        spotRem=Array.isArray(f.spotlight_remove)?f.spotlight_remove:[],
        spotUpd=Array.isArray(f.spotlight_update)?f.spotlight_update:[];
    if ((added||removed||updated)===0 && hydratedLanes[key]) return { ...hydratedLanes[key] };
    if(!spotAdd.length && !spotRem.length && !spotUpd.length && items.length){
      const s=synthSpots(items,key); spotAdd=s.a; spotRem=s.r; spotUpd=s.u;
    }
    const out={added,removed,updated,items,spotAdd,spotRem,spotUpd};
    if(guardLaneOverwrite(key,out,Date.now())) hydratedLanes[key]=out;
    return out;
  };

  const laneState=(key)=>{
    const err=(summary?.exit_code!=null&&summary.exit_code!==0);
    const enabled=!!getEnabledMap()[key];
    if(!enabled) return "skip";
    if(err) return "err";
    return sync.isRunning()? "run" : (sync.state().timeline.done ? "ok" : "skip");
  };
  const fmtDelta=(a,r,u)=>`+${a||0} / -${r||0} / ~${u||0}`;

  function renderLanes(){
    elLanes.innerHTML="";
    const wrap=document.createElement("div"); wrap.className="lanes";
    const running=sync.isRunning();

    for(const f of FEATS){
      const isEnabled=!!getEnabledMap()[f.key];
      const {added,removed,updated,items,spotAdd,spotRem,spotUpd}=getLaneStats(summary||{},f.key);
      const st=laneState(f.key);

      const lane=document.createElement("div"); lane.className="lane"; if(!isEnabled) lane.classList.add("disabled");

      const total=(added||0)+(removed||0)+(updated||0);
      const prev=lastCounts[f.key]??0;
      if(running&&total>prev&&isEnabled){ lane.classList.add("shake"); setTimeout(()=>lane.classList.remove("shake"),450); }
      lastCounts[f.key]=total;

      const h=document.createElement("div"); h.className="lane-h";
      const ico=document.createElement("div"); ico.className="lane-ico"; ico.innerHTML=`<span class="material-symbols-outlined material-symbol material-icons">${f.icon}</span>`;
      const ttl=document.createElement("div"); ttl.className="lane-title"; ttl.textContent=f.label;
      const badges=document.createElement("div"); badges.className="lane-badges";
      const delta=document.createElement("span"); delta.className="delta"; delta.innerHTML=`<b>${fmtDelta(added,removed,updated)}</b>`;
      const chip=document.createElement("span");
      chip.className="chip "+(st==="ok"?"ok":st==="run"?"run":st==="err"?"err":"skip");
      chip.textContent=!isEnabled?"Disabled":st==="err"?"Failed":st==="ok"?"Synced":st==="run"?"Running":"Skipped";
      badges.append(delta,chip);
      h.append(ico,ttl,badges); lane.appendChild(h);

      const body=document.createElement("div"); body.className="lane-body";
      const spots=[]; for(const x of (spotAdd||[]).slice(0,2)) spots.push({t:"add",text:titleOf(x)});
      for(const x of (spotRem||[]).slice(0,2)) spots.push({t:"rem",text:titleOf(x)});
      for(const x of (spotUpd||[]).slice(0,2)) spots.push({t:"upd",text:titleOf(x)});
      if(!spots.length && items?.length){ const s=synthSpots(items,f.key); for(const x of s.a.slice(0,2)) spots.push({t:"add",text:x}); for(const x of s.r.slice(0,2)) spots.push({t:"rem",text:x}); for(const x of s.u.slice(0,2)) spots.push({t:"upd",text:x}); }

      if(!isEnabled){
        body.appendChild(Object.assign(document.createElement("div"),{className:"spot muted small",textContent:"Feature not configured"}));
      }else if(!spots.length){
        body.appendChild(Object.assign(document.createElement("div"),{className:"spot muted small",textContent:sync.state().timeline.done?"No changes":"Awaiting results…"}));
      }else{
        for(const s of spots.slice(0,3)){
          const row=document.createElement("div"); row.className="spot";
          const tag=document.createElement("span"); tag.className="tag "+(s.t==="add"?"t-add":s.t==="rem"?"t-rem":"t-upd"); tag.textContent=s.t==="add"?"Added":s.t==="rem"?"Removed":"Updated";
          row.append(tag,Object.assign(document.createElement("span"),{textContent:s.text})); body.appendChild(row);
        }
      }
      lane.appendChild(body); wrap.appendChild(lane);
    }
    elLanes.appendChild(wrap);
  }

  const renderSpotlightSummary=()=>{ elSpot.innerHTML=""; };
  const renderAll=()=>{ renderLanes(); renderSpotlightSummary(); };

  // Pairs → enablement
  async function pullPairs(){
    const arr=await fetchJSON("/api/pairs",null);
    if(!Array.isArray(arr)) return;
    if(!arr.length){ enabledFromPairs={watchlist:false,ratings:false,history:false,playlists:false}; return; }
    const enabled={watchlist:false,ratings:false,history:false,playlists:false};
    for(const p of arr){ const feats=p?.features||{}; for(const f of FEATS){ const cfg=feats[f.key]; if(cfg&&(cfg.enable===true||cfg.enabled===true)) enabled[f.key]=true; } }
    enabledFromPairs=enabled;
  }

  // Insights hydration (unchanged behavior)
  let _insightsTried=false;
  async function hydrateFromInsights(startTsEpoch){
    const src=await fetchFirstJSON(["/api/insights"],null);
    const events=src?.events;
    if(!Array.isArray(events)||!events.length) return false;
    const since=Math.floor(startTsEpoch||0);

    const tallies={watchlist:mk(),ratings:mk(),history:mk(),playlists:mk()};
    function mk(){ return {added:0,removed:0,updated:0,spotAdd:[],spotRem:[],spotUpd:[]}; }
    const mapFeature=(e)=>{ const f=String(e.feature||e.lane||e.kind||"").toLowerCase(); if(f) return f;
      const act=String(e.action||"").toLowerCase();
      if(act.includes("watch")||act.includes("scrobble")) return "history";
      if(act.includes("rate")||"rating" in (e||{})) return "ratings";
      if(act.includes("playlist")) return "playlists";
      return "watchlist";
    };

    for(const e of events){
      if((e.ts||0)<since) continue;
      const k=mapFeature(e);
      const L=tallies[k]||(tallies[k]=mk());
      const title=titleOf(e);
      if(e.action==="add"){ L.added++; if(L.spotAdd.length<3) L.spotAdd.push(title); }
      if(e.action==="remove"){ L.removed++; if(L.spotRem.length<3) L.spotRem.push(title); }
      if(e.action==="update"){ L.updated++; if(L.spotUpd.length<3) L.spotUpd.push(title); }
    }

    summary ||= {}; summary.features ||= {};
    for(const [k,L] of Object.entries(tallies)){
      if((L.added+L.removed+L.updated)===0 && !L.spotAdd.length && !L.spotRem.length && !L.spotUpd.length) continue;
      const prev=summary.features[k]||{};
      const merged = {
        added:   Math.max(prev.added   || 0, L.added),
        removed: Math.max(prev.removed || 0, L.removed),
        updated: Math.max(prev.updated || 0, L.updated),
        spotlight_add:    prev.spotlight_add?.length    ? prev.spotlight_add    : L.spotAdd,
        spotlight_remove: prev.spotlight_remove?.length ? prev.spotlight_remove : L.spotRem,
        spotlight_update: prev.spotlight_update?.length ? prev.spotlight_update : L.spotUpd,
      };
      summary.features[k]=merged;
      hydratedLanes[k]={added:merged.added,removed:merged.removed,updated:merged.updated,items:[],spotAdd:merged.spotlight_add||[],spotRem:merged.spotlight_remove||[],spotUpd:merged.spotlight_update||[]};
    }
    summary.enabled=Object.assign(defaultEnabledMap(),summary.enabled||{});
    renderAll();
    return true;
  }

  function hydrateFromLog(){
    const det=document.getElementById("det-log"); if(!det) return false;
    const txt=det.innerText||det.textContent||""; if(!txt) return false;

    const lines=txt.split(/\n+/).slice(-800);
    const tallies=Object.create(null);
    const ensureLane=k=>(tallies[k] ||= {added:0,removed:0,updated:0,spotAdd:[],spotRem:[],spotUpd:[]});
    const mapFeat=s=>{ const f=String(s||"").trim().toLowerCase(); if(!f) return ""; if(f==="watch"||f==="watched") return "history"; return f; };
    let lastFeatHint="";

    for(const raw of lines){
      const i=raw.indexOf("{");
      if(i<0){ const m=raw.match(/feature["']?\s*:\s*"?(\w+)"?/i); if(m) lastFeatHint=mapFeat(m[1])||lastFeatHint; continue; }
      let obj; try{ obj=JSON.parse(raw.slice(i)); }catch{ continue; }
      if(!obj||!obj.event) continue;
      const feat=mapFeat(obj.feature); if(feat) lastFeatHint=feat;

      if(obj.event==="snapshot:progress"||obj.event==="progress:snapshot"){ sync.snap({done:obj.done,total:obj.total,final:!!obj.final,dst:obj.dst,feature:obj.feature}); }
      if(/^apply:/.test(obj.event||"")){
        const isStart=/:start$/.test(obj.event);
        const isProg=/:progress$/.test(obj.event);
        const isDone=/:done$/.test(obj.event);
        if(isStart){ sync.applyStart({feature:lastFeatHint,total:obj.total}); }
        if(isProg){ sync.applyProg({feature:lastFeatHint,done:obj.done,total:obj.total}); }
        if(isDone){ sync.applyDone({feature:lastFeatHint,count:obj.result?.count||obj.count}); }
      }

      const laneKey=feat||lastFeatHint;
      if(obj.event==="two:done"&&laneKey){ const res=obj.res||{}; const L=ensureLane(laneKey); L.added+=+res.adds||0; L.removed+=+res.removes||0; L.updated+=+res.updates||0; continue; }
      if(obj.event==="plan"&&laneKey){ const L=ensureLane(laneKey); L.added+=+obj.add||0; L.removed+=+obj.rem||0; continue; }
      if(obj.event==="two:plan"&&laneKey){ const L=ensureLane(laneKey); const addA=+obj.add_to_A||0, addB=+obj.add_to_B||0, remA=+obj.rem_from_A||0, remB=+obj.rem_from_B||0; L.added+=Math.max(addA,addB); L.removed+=Math.max(remA,remB); continue; }
      if(obj.event==="spotlight"&&(feat||obj.feature)&&obj.action&&obj.title){
        const L=ensureLane(laneKey||obj.feature); const act=String(obj.action).toLowerCase();
        if(act==="add"&&L.spotAdd.length<3) L.spotAdd.push(obj.title);
        if(act==="remove"&&L.spotRem.length<3) L.spotRem.push(obj.title);
        if(act==="update"&&L.spotUpd.length<3) L.spotUpd.push(obj.title);
        continue;
      }
    }

    if(!Object.keys(tallies).length) return false;
    summary ||= {}; summary.features ||= {};
    for(const [feat,lane] of Object.entries(tallies)){
      const prev=summary.features[feat]||{};
      const merged={ added: Math.max(prev.added||0, lane.added||0),
                     removed: Math.max(prev.removed||0, lane.removed||0),
                     updated: Math.max(prev.updated||0, lane.updated||0),
                     spotlight_add:    (prev.spotlight_add?.length?prev.spotlight_add:lane.spotAdd).slice(-3),
                     spotlight_remove: (prev.spotlight_remove?.length?prev.spotlight_remove:lane.spotRem).slice(-3),
                     spotlight_update: (prev.spotlight_update?.length?prev.spotlight_update:lane.spotUpd).slice(-3) };
      if(guardLaneOverwrite(feat,merged,Date.now())){
        summary.features[feat]=merged;
        hydratedLanes[feat]={added:merged.added,removed:merged.removed,updated:merged.updated,items:[],spotAdd:merged.spotlight_add||[],spotRem:merged.spotlight_remove||[],spotUpd:merged.spotlight_update||[]};
      }
    }
    summary.enabled=Object.assign(defaultEnabledMap(),summary.enabled||{});
    renderAll();
    return true;
  }

  function setRunButtonState(running){
    const btn=document.getElementById("run"); if(!btn) return;
    btn.toggleAttribute("disabled",!!running);
    btn.setAttribute("aria-busy",running?"true":"false");
    btn.classList.toggle("glass",!!running);
    btn.title=running?"Synchronization running…":"Run synchronization";
  }

  function wireRunButton(){
    const btn=document.getElementById("run"); if(!btn||wireRunButton._done) return;
    wireRunButton._done=true;
    btn.addEventListener("click",async ()=>{
      if(btn.disabled||btn.classList.contains("glass")) return;
      setRunButtonState(true);
      sync.markInit();
    },{capture:true});
  }

  // Summary pull
  async function pullSummary(){
    const s = await fetchJSON("/api/run/summary", summary);
    if (!s) return;

    const runKey = runKeyOf(s);
    if (runKey && runKey !== _prevRunKey) {
      _finishedForRun = null;
      _prevRunKey = runKey;
    }

    const { running, justStarted, justFinished } = sync.fromSummary(s);
    summary = s;
    setRunButtonState(running);

    if (justFinished && runKey && _finishedForRun !== runKey) {
      _finishedForRun = runKey;
      optimistic = false;

      try { window.updatePreviewVisibility?.(); window.refreshSchedulingBanner?.(); } catch {}
      try { (window.Insights?.refreshInsights || window.refreshInsights)?.(); } catch {}

      try {
        const startTs = s?.raw_started_ts || (s?.started_at ? Date.parse(s.started_at)/1000 : 0);
        await hydrateFromInsights(startTs);
      } catch {}

      try {
        window.wallLoaded = false;
        if (typeof window.updateWatchlistPreview === "function") {
          await window.updateWatchlistPreview();
        } else if (typeof window.updatePreviewVisibility === "function") {
          await window.updatePreviewVisibility();
        } else if (typeof window.loadWatchlist === "function") {
          await window.loadWatchlist();
        }
      } catch {}

      try {
        window.dispatchEvent(new CustomEvent("sync-complete", { detail: { at: Date.now(), summary: s } }));
      } catch {}
    }

    if (!summary.enabled) summary.enabled = defaultEnabledMap();
    renderAll();

    const hasFeatures = summary?.features && Object.values(summary.features).some(v =>
      (v?.added||v?.removed||v?.updated||0)>0 ||
      (v?.spotlight_add?.length||v?.spotlight_remove?.length||v?.spotlight_update?.length)
    );

    if (!hasFeatures && sync.state().timeline.done && !_insightsTried) {
      _insightsTried = true;
      const startTs = summary?.raw_started_ts || (summary?.started_at ? Date.parse(summary.started_at)/1000 : 0);
      const got = await hydrateFromInsights(startTs);
      if (!got) setTimeout(() => { if (!hasFeatures) hydrateFromLog(); }, 300);
    } else {
      const missing = FEATS.some(f => {
        const lane = summary?.features?.[f.key];
        return !(lane?.spotlight_add?.length || lane?.spotlight_remove?.length || lane?.spotlight_update?.length ||
                (lane?.added || lane?.removed || lane?.updated));
      });
      if (missing) hydrateFromLog();
    }
  }

  // Streams
  let esSummary=null, esLogs=null;
  window.openSummaryStream = function openSummaryStream(){
    try{
      try{ esSummary?.close?.(); }catch{}
      esSummary = new EventSource("/api/run/summary/stream");
      window.esSum = esSummary;

      esSummary.onopen = () => { window.esSum = esSummary; };
      esSummary.onmessage = (ev) => { try{
        const incoming = JSON.parse(ev.data || "{}");

        if (incoming && incoming.running === true && !sync.state().timeline.start) {
          sync.markInit();
        }

        if (incoming && (incoming.running === false || incoming.exit_code != null)) {
          if (!sync.state().timeline.start) {
            sync.reset(); setRunButtonState(false); renderAll();
          } else {
            sync.done(); setRunButtonState(false);
          }
        }
      } catch{} };

      const markInit = () => { try { sync.markInit(); } catch {} };
      ["run:start","run:pair","feature:start"].forEach(n => esSummary.addEventListener(n, markInit));

      const onSnap = (ev)=>{ try{ sync.snap(JSON.parse(ev.data||"{}")); }catch{} };
      esSummary.addEventListener("progress:snapshot", onSnap);
      esSummary.addEventListener("snapshot:progress", onSnap);

      esSummary.addEventListener("progress:apply", ev => { try{
        const d=JSON.parse(ev.data||"{}");
        sync.prog({feature:"__global__",done:d.done,total:d.total});
      }catch{} });

      ["apply:add:start","apply:remove:start"].forEach(name=> esSummary.addEventListener(name, ev=>{
        try{ sync.applyStart(JSON.parse(ev.data||"{}")); }catch{}
      }));
      ["apply:add:done","apply:remove:done"].forEach(name=> esSummary.addEventListener(name, ev=>{
        try{ sync.applyDone(JSON.parse(ev.data||"{}")); }catch{}
      }));

      esSummary.addEventListener("run:done", ()=>{ try{ sync.done(); setRunButtonState(false); }catch{} });
      ["run:error","run:aborted"].forEach(name=> esSummary.addEventListener(name, ()=>{ try{ sync.error(); setRunButtonState(false); }catch{} }));

      esSummary.onerror = () => {
        try{ esSummary.close(); }catch{}
        window.esSum = null;
        setTimeout(openSummaryStream, 2000);
      };
    }catch{}
  };

  window.openLogStream = function openLogStream(){
    try{
      try{ esLogs?.close?.(); }catch{}
      esLogs = new EventSource("/api/logs/stream");
      window.esLogs = esLogs; // expose for guards

      esLogs.onopen = ()=> { window.esLogs = esLogs; };

      esLogs.onmessage = (ev) => {
        try{
          const txt = String(ev.data || "");

          if (!sync.isStreamArmed?.() && /(?:\bSYNC\b.*\bstart\b|\brun\b.*\bstart|\bstarting\b.*\bsync|\brunning pairs\b)/i.test(txt)) {
            sync.markInit();
          }

          const m = txt.match(/\[SYNC\]\s*exit\s*code\s*:\s*(\d+)/i);
          if (m) {
            if (!sync.state().timeline.start) {
              sync.reset(); setRunButtonState(false); renderAll();
            } else {
              sync.done();  setRunButtonState(false);
            }
          }
        } catch{}
      };

      const onSnap = (ev)=>{ try{ sync.snap(JSON.parse(ev.data||"{}")); }catch{} };
      esLogs.addEventListener("snapshot:progress", onSnap);
      esLogs.addEventListener("progress:snapshot", onSnap);

      ["apply:add:start","apply:remove:start"].forEach(name=> esLogs.addEventListener(name, ev=>{
        try{ sync.applyStart(JSON.parse(ev.data||"{}")); }catch{}
      }));
      ["apply:add:done","apply:remove:done"].forEach(name=> esLogs.addEventListener(name, ev=>{
        try{ sync.applyDone(JSON.parse(ev.data||"{}")); }catch{}
      }));
      esLogs.addEventListener("progress:apply", ev => {
        try{ sync.applyProg(JSON.parse(ev.data||"{}")); }catch{}
      });

      esLogs.addEventListener("run:done", ()=>{ try{ sync.done(); setRunButtonState(false); }catch{} });
      ["run:error","run:aborted"].forEach(name=> esLogs.addEventListener(name, ()=>{ try{ sync.error(); setRunButtonState(false); }catch{} }));

      esLogs.onerror = () => {
        try{ esLogs.close(); }catch{}
        window.esLogs = null;
        setTimeout(openLogStream, 2000);
      };
    }catch{}
  };

  // Public UX hooks compatible with old calls
  window.UX={
    updateTimeline:(tl)=> sync.updateTimeline(tl||{}),
    updateProgress:(payload)=> payload?.pct!=null && sync.updatePct(payload.pct),
    refresh:()=> pullSummary().then(()=>renderAll())
  };

  // Visibility reconnect
  window.addEventListener("visibilitychange",()=>{ if(document.visibilityState==="visible"){ openSummaryStream(); openLogStream(); } });

  // Periodic: pull summary + pairs; auto-reconnect if quiet
  function tick(){
    const running=sync.isRunning();
    pullSummary();
    if((Date.now()-lastPairsAt)>10000){ pullPairs().finally(()=>{ lastPairsAt=Date.now(); renderLanes(); }); }
    if(running && !sync.state().timeline.pre && !sync.state().timeline.post){ // gentle drift only at start
      if(Date.now()-sync._lastPhaseAt>900){ sync.updatePct(Math.min((sync._pctMemo||0)+2,24)); }
    }
    if(Date.now()-sync.lastEvent()>20000){ openSummaryStream(); openLogStream(); }
    clearTimeout(tick._t); tick._t=setTimeout(tick,running?1500:5000);
  }

  // Boot
  renderAll();
  wireRunButton();
  openSummaryStream();
  openLogStream();
  pullPairs();
  tick();
})();

// Hard lock: preview only on Main
(() => {
  // CSS guard
  (document.getElementById("preview-guard-css")||{}).remove?.();
  document.head.appendChild(Object.assign(document.createElement("style"), {
    id: "preview-guard-css",
    textContent: `html[data-tab!="main"] #placeholder-card { display: none !important; }`
  }));

  const DOC = document.documentElement;
  DOC.dataset.tab ||= "main";

  const _showTab = window.showTab;
  window.showTab = function(name){
    const ret = _showTab ? _showTab.apply(this, arguments) : undefined;
    try {
      DOC.dataset.tab = name || "main";
      document.dispatchEvent(new CustomEvent("tab-changed", { detail: { tab: name } }));
    } catch {}
    return ret;
  };

  const isMain = () => DOC.dataset.tab === "main";
  const hidePreview = () => document.getElementById("placeholder-card")?.classList.add("hidden");

  const guard = (fn) => {
    const orig = window[fn];
    if (typeof orig !== "function") return;
    window[fn] = async function(...args){
      if (!isMain()) { hidePreview(); return; }
      return orig.apply(this, args);
    };
  };
  ["updateWatchlistPreview","updatePreviewVisibility","loadWatchlist"].forEach(guard);

  document.addEventListener("tab-changed", () => { if (!isMain()) hidePreview(); });
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && !isMain()) hidePreview();
  });
})();
