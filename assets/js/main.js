// UX panel: monotone bar + live SSE
(()=>{
  // Lanes
  const FEATS=[
    {key:"watchlist",icon:"movie",label:"Watchlist"},
    {key:"ratings",icon:"star",label:"Ratings"},
    {key:"history",icon:"play_arrow",label:"History"},
    {key:"playlists",icon:"queue_music",label:"Playlists"},
  ];

  // DOM
  const elProgress=document.getElementById("ux-progress");
  const elLanes=document.getElementById("ux-lanes");
  const elSpot=document.getElementById("ux-spotlight");
  if(!elProgress||!elLanes||!elSpot) return;

  // Styles
  (document.getElementById("ux-styles")||{}).remove?.();
  document.head.appendChild(Object.assign(document.createElement("style"),{id:"ux-styles",textContent:`
#ux-progress,#ux-lanes{margin-top:12px}
.ux-rail{position:relative;height:10px;border-radius:999px;background:#1f1f26;overflow:hidden}
.ux-rail.error{background:linear-gradient(90deg,#311,#401818)}
.ux-bead{position:absolute;inset:0 auto 0 0;width:0%;height:100%;border-radius:inherit;background:linear-gradient(90deg,#7c4dff,#00d4ff);box-shadow:inset 0 0 14px rgba(124,77,255,.35);transition:width .28s ease}
.ux-bead.indet{background-size:200% 100%;animation:fillShift 1.2s ease-in-out infinite}
@keyframes fillShift{0%{background-position:0% 50%}100%{background-position:100% 50%}}
.ux-rail.running .ux-bead{background-size:200% 100%}
.ux-rail.running.indet::after{content:"";position:absolute;inset:0;background:linear-gradient(120deg,transparent 0%,rgba(255,255,255,.09) 20%,transparent 40%);transform:translateX(-100%);animation:shimmer 1.4s linear infinite;pointer-events:none}
.ux-rail.apply.indet::after{animation-duration:1.0s}
.ux-rail.starting .ux-bead{animation:pulse .9s ease-in-out infinite alternate}
.ux-rail.finishing .ux-bead{filter:saturate(1.2) brightness(1.05)}
@keyframes shimmer{to{transform:translateX(100%)}}
@keyframes pulse{from{opacity:.9}to{opacity:.75}}
@media (prefers-reduced-motion:reduce){
  .ux-rail.running.indet::after,.ux-bead.indet{animation:none}
  .ux-rail.starting .ux-bead{animation:none}
}
.ux-rail-steps{display:flex;justify-content:space-between;font-size:11px;margin-top:6px;opacity:.8}
.ux-rail-steps span{white-space:nowrap}
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

  // Anchors
  const Anch=Object.freeze({start0:0, preStart:25, preEnd:50, postEnd:95, done:100});

  // State
  let timeline={start:false,pre:false,post:false,done:false};
  let progressPct=0, status=null, summary=null;
  let holdAtTen=false, pctMemo=0, phaseMemo=-1;
  let lastPairsAt=0, optimistic=false, lastPhaseAt=Date.now();
  let lastRunStartedAt=0;
  let runKey=null, lastEventTs=Date.now();

  // Safe hooks (avoid ReferenceError)
  const startRunVisualsSafe=(...a)=>window.startRunVisuals?.(...a);
  const stopRunVisualsSafe =(...a)=>window.stopRunVisuals?.(...a);

  // Run identity
  const runKeyOf=s=> s?.run_id||s?.run_uuid||s?.raw_started_ts||(s?.started_at?Date.parse(s.started_at):null)||null;

  // Phase aggregators
  const PhaseAgg={ snap:{done:0,total:0,started:false,finished:false}, apply:{done:0,total:0,started:false,finished:false} };

  const SnapAgg={
    buckets:Object.create(null),
    reset(){ this.buckets=Object.create(null); },
    has(){ return Object.keys(this.buckets).length>0; },
    update(d){
      const k=`${(d.dst||"ALL").toUpperCase()}:${(d.feature||"all").toLowerCase()}`;
      this.buckets[k]={done:+(d.done||0),total:+(d.total||0),final:!!d.final};
      let tot=0,don=0,allFinal=true;
      for(const v of Object.values(this.buckets)){
        const dn=Math.min(+v.done||0,+v.total||0);
        don+=dn; tot+= (+v.total||0);
        allFinal=allFinal && (!!v.final || dn>=(+v.total||0));
      }
      PhaseAgg.snap.total=tot;
      PhaseAgg.snap.done=don;
      PhaseAgg.snap.started=tot>0;
      PhaseAgg.snap.finished=allFinal && tot>0 && don>=tot;
      lastEventTs=Date.now();
    }
  };

  const ApplyAgg={
    buckets:Object.create(null),
    reset(){
      this.buckets=Object.create(null);
      PhaseAgg.apply={done:0,total:0,started:false,finished:false};
    },
    key:(d)=>`${(d.feature||"all").toLowerCase()}`,
    ensure(d){
      const k=this.key(d);
      this.buckets[k] ||= {done:0,total:0,final:false};
      return k;
    },
    start(d){
      const k=this.ensure(d);
      this.buckets[k].total=Number(d.count||d.total||0);
      this._recalc(); lastEventTs=Date.now();
    },
    prog(d){
      const k=this.ensure(d);
      if(typeof d.done==="number") this.buckets[k].done=Number(d.done||0);
      if(typeof d.total==="number") this.buckets[k].total=Number(d.total||0);
      this._recalc(); lastEventTs=Date.now();
    },
    done(d){
      const k=this.ensure(d);
      const c = Number( (d.result?.count ?? d.count ?? this.buckets[k].total ?? 0) );
      this.buckets[k].done=c;
      this.buckets[k].total=Math.max(this.buckets[k].total,c);
      this.buckets[k].final=true;
      this._recalc(); lastEventTs=Date.now();
    },
    _recalc(){
      let tot=0,don=0,allFinal=true,any=false;
      for(const v of Object.values(this.buckets)){
        any=true;
        tot+=Number(v.total||0);
        don+=Math.min(Number(v.done||0),Number(v.total||0));
        allFinal=allFinal && (!!v.final || (Number(v.total||0)>0 && Number(v.done||0)>=Number(v.total||0)));
      }
      PhaseAgg.apply.total=tot;
      PhaseAgg.apply.done=don;
      PhaseAgg.apply.started=any && tot>0;
      PhaseAgg.apply.finished=any && allFinal && tot>0 && don>=tot;
    }
  };

  // Math helpers
  const phaseIdx=tl=>tl?.done?3:tl?.post?2:tl?.pre?1:tl?.start?0:-1;
  const clampPct=(n,lo=0,hi=100)=>Math.max(lo,Math.min(hi,Math.round(n)));
  const clampTimelineForward=next=>(phaseIdx(next)<phaseIdx(timeline))?timeline:next;
  const monotonePct=(_tl,pct)=>{ const idx=phaseIdx(_tl); if(idx<phaseMemo) return pctMemo; if(idx>phaseMemo) phaseMemo=idx; return (pctMemo=Math.max(pctMemo,clampPct(pct))); };
  const asPctFromTimeline=tl=>tl?.done?Anch.done:tl?.post?Anch.preEnd:tl?.pre?Anch.preStart:tl?.start?Anch.start0:0;
  const lerp=(a,b,t)=>a+(b-a)*t;

  // Progress model (apply is ignored until all snapshots are finished → no early jump)
  const pctFromPhaseAgg=()=>{
    const sTot=PhaseAgg.snap.total|0, sDone=PhaseAgg.snap.done|0;
    const aTot=PhaseAgg.apply.total|0, aDone=PhaseAgg.apply.done|0;
    const snapPct = sTot>0 ? lerp(Anch.preStart, Anch.preEnd, Math.max(0,Math.min(1,sDone/sTot))) : null;
    const appPct  = (PhaseAgg.snap.finished && aTot>0)
      ? lerp(Anch.preEnd, Anch.postEnd, Math.max(0,Math.min(1,aDone/aTot)))
      : null;
    return appPct!=null?clampPct(appPct):snapPct!=null?clampPct(snapPct):null;
  };

  // IO
  const fetchJSON = async (url, fallback=null) => {
    try{
      const r=await fetch(url+(url.includes("?")?"&":"?")+"_ts="+Date.now(),{credentials:"same-origin",cache:"no-store"});
      return r.ok? r.json(): fallback;
    }catch{ return fallback; }
  };
  const fetchFirstJSON = async (urls, fallback=null) => {
    for(const u of urls){ const j=await fetchJSON(u,null); if(j) return j; }
    return fallback;
  };

  // Feature toggles
  const defaultEnabledMap=()=>({watchlist:true,ratings:true,history:true,playlists:true});
  let enabledFromPairs=null;

  // Progress rail
  const resetProgress=()=>{
    pctMemo=0; phaseMemo=-1; holdAtTen=false;
    PhaseAgg.snap={done:0,total:0,started:false,finished:false};
    PhaseAgg.apply={done:0,total:0,started:false,finished:false};
    SnapAgg.reset(); ApplyAgg.reset();
  };

  function renderProgress(){
    elProgress.innerHTML="";
    const rail=Object.assign(document.createElement("div"),{className:"ux-rail"});
    const fill=Object.assign(document.createElement("div"),{className:"ux-bead"});

    const byPhases=pctFromPhaseAgg();
    let base=(byPhases!=null?byPhases:asPctFromTimeline(timeline));
    if(holdAtTen && !PhaseAgg.snap.started) base=Math.max(base,10);

    const logicalDone = (PhaseAgg.snap.finished && (PhaseAgg.apply.finished || PhaseAgg.apply.total===0));
    const hardDone = !!timeline.done || logicalDone;
    if(!hardDone) base=Math.min(base,Anch.postEnd);

    const pct=monotonePct(timeline,base);
    fill.style.width=pct+"%";

    const isRunning = (summary?.running===true)||(timeline.start&&!timeline.done);
    rail.classList.toggle("running",isRunning && !timeline.done);
    rail.classList.toggle("indet",isRunning && !timeline.done && byPhases==null);
    rail.classList.toggle("apply",PhaseAgg.apply.started && !PhaseAgg.apply.finished);
    rail.classList.toggle("starting",isRunning && !(timeline.pre||timeline.post));
    rail.classList.toggle("finishing",!isRunning && !timeline.done && logicalDone);

    if(summary?.exit_code!=null && summary.exit_code!==0) rail.classList.add("error");

    const steps=Object.assign(document.createElement("div"),{className:"ux-rail-steps muted"});
    [["Start","start"],["Discovering","discovering"],["Syncing","syncing"],["Done","done"]]
      .forEach(([txt,key])=>{ const s=document.createElement("span"); s.textContent=txt; s.dataset.step=key; steps.appendChild(s); });

    rail.appendChild(fill);
    elProgress.append(rail,steps);
  }

  // Lanes
  const titleOf=x=>typeof x==="string"?x:(x?.title||x?.name||x?.key||"item");
  const hydratedLanes=Object.create(null), lastCounts=Object.create(null);
  const synthSpots=(items,key)=>{
    const a=[],r=[],u=[];
    for(const it of items||[]){
      const t=titleOf(it);
      const act=(it?.action||it?.op||it?.change||"").toLowerCase();
      let tag="upd";
      if(key==="history"&&(it?.watched||it?.watched_at||act.includes("watch")||act.includes("scrobble"))) tag="add";
      else if(key==="ratings"&&(act.includes("rate")||("rating"in(it||{})))) tag="add";
      else if(key==="playlists"&&(act.includes("add")||act.includes("playlist"))) tag="add";
      else if(act.includes("add")) tag="add";
      else if(act.includes("rem")||act.includes("del")||act.includes("unwatch")) tag="rem";
      if(tag==="add"&&a.length<3) a.push(t);
      else if(tag==="rem"&&r.length<3) r.push(t);
      else if(u.length<3) u.push(t);
      if(a.length+r.length+u.length>=3) break;
    }
    return {a,r,u};
  };
  const getEnabledMap=()=>enabledFromPairs ?? (summary?.enabled||defaultEnabledMap());
  const getLaneStats=(sum,key)=>{
    const f=(sum?.features?.[key])||sum?.[key]||{};
    const added=(f.added??f.add??f.adds??f.plus??0)|0;
    const removed=(f.removed??f.del??f.deletes??f.minus??0)|0;
    const updated=(f.updated??f.upd??f.changed??0)|0;
    const items=Array.isArray(f.items)?f.items:[];
    let spotAdd=Array.isArray(f.spotlight_add)?f.spotlight_add:[],
        spotRem=Array.isArray(f.spotlight_remove)?f.spotlight_remove:[],
        spotUpd=Array.isArray(f.spotlight_update)?f.spotlight_update:[];
    if((added||removed||updated)===0 && hydratedLanes[key]) return {...hydratedLanes[key]};
    if(!spotAdd.length && !spotRem.length && !spotUpd.length && items.length){
      const s=synthSpots(items,key); spotAdd=s.a; spotRem=s.r; spotUpd=s.u;
    }
    const out={added,removed,updated,items,spotAdd,spotRem,spotUpd};
    if((added+removed+updated)>0||spotAdd.length||spotRem.length||spotUpd.length) hydratedLanes[key]=out;
    return out;
  };
  const laneState=(sum,key,enabled)=>{
    const err=(summary?.exit_code!=null&&summary.exit_code!==0);
    if(!enabled) return "skip";
    if(err) return "err";
    if(timeline?.done) return "ok";
    if(timeline?.start&&!timeline?.done) return "run";
    return "skip";
  };
  const fmtDelta=(a,r,u)=>`+${a||0} / -${r||0} / ~${u||0}`;
  function renderLanes(){
    elLanes.innerHTML="";
    const wrap=document.createElement("div"); wrap.className="lanes";
    const enabledMap=getEnabledMap();
    const running=summary?.running===true||(timeline.start&&!timeline.done);

    for(const f of FEATS){
      const isEnabled=!!enabledMap[f.key];
      const {added,removed,updated,items,spotAdd,spotRem,spotUpd}=getLaneStats(summary||{},f.key);
      const st=laneState(summary||{},f.key,isEnabled);

      const lane=document.createElement("div"); lane.className="lane";
      if(!isEnabled) lane.classList.add("disabled");

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
      const spots=[];
      for(const x of (spotAdd||[]).slice(0,2)) spots.push({t:"add",text:titleOf(x)});
      for(const x of (spotRem||[]).slice(0,2)) spots.push({t:"rem",text:titleOf(x)});
      for(const x of (spotUpd||[]).slice(0,2)) spots.push({t:"upd",text:titleOf(x)});

      if(!spots.length && items?.length){
        const s=synthSpots(items,f.key);
        for(const x of s.a.slice(0,2)) spots.push({t:"add",text:x});
        for(const x of s.r.slice(0,2)) spots.push({t:"rem",text:x});
        for(const x of s.u.slice(0,2)) spots.push({t:"upd",text:x});
      }

      if(!isEnabled){
        body.appendChild(Object.assign(document.createElement("div"),{className:"spot muted small",textContent:"Feature not configured"}));
      }else if(!spots.length){
        body.appendChild(Object.assign(document.createElement("div"),{className:"spot muted small",textContent:timeline?.done?"No changes":"Awaiting results…"}));
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

  // Spotlight (placeholder)
  const renderSpotlightSummary=()=>{ elSpot.innerHTML=""; };

  // Pairs → lane enablement
  async function pullPairs(){
    const arr=await fetchJSON("/api/pairs",null);
    if(!Array.isArray(arr)) return;
    if(!arr.length){ enabledFromPairs={watchlist:false,ratings:false,history:false,playlists:false}; return; }
    const enabled={watchlist:false,ratings:false,history:false,playlists:false};
    for(const p of arr){
      const feats=p?.features||{};
      for(const f of FEATS){
        const cfg=feats[f.key];
        if(cfg&&(cfg.enable===true||cfg.enabled===true)) enabled[f.key]=true;
      }
    }
    enabledFromPairs=enabled;
  }

  // Insights hydration
  let _insightsTried=false;
  async function hydrateFromInsights(startTsEpoch){
    const src=await fetchFirstJSON(["/api/insights"],null);
    const events=src?.events;
    if(!Array.isArray(events)||!events.length) return false;
    const since=Math.floor(startTsEpoch||0);

    const tallies={watchlist:mk(),ratings:mk(),history:mk(),playlists:mk()};
    function mk(){ return {added:0,removed:0,updated:0,spotAdd:[],spotRem:[],spotUpd:[]}; }
    const mapFeature=(e)=>{
      const f=String(e.feature||e.lane||e.kind||"").toLowerCase();
      if(f) return f;
      const act=String(e.action||"").toLowerCase();
      if(act.includes("watch")||act.includes("scrobble")) return "history";
      if(act.includes("rate")||"rating" in (e||{})) return "ratings";
      if(act.includes("playlist")) return "playlists";
      return "watchlist";
    };

    for(const e of events){
      if((e.ts||0)<since) continue;
      const k=mapFeature(e);
      if(!tallies[k]) tallies[k]=mk();
      const L=tallies[k]; const title=titleOf(e);
      if(e.action==="add"){ L.added++; if(L.spotAdd.length<3) L.spotAdd.push(title); }
      if(e.action==="remove"){ L.removed++; if(L.spotRem.length<3) L.spotRem.push(title); }
      if(e.action==="update"){ L.updated++; if(L.spotUpd.length<3) L.spotUpd.push(title); }
    }

    summary ||= {}; summary.features ||= {};
    for(const [k,L] of Object.entries(tallies)){
      if((L.added+L.removed+L.updated)===0 && !L.spotAdd.length && !L.spotRem.length && !L.spotUpd.length) continue;
      const prev=summary.features[k]||{};
      const merged={
        added:(prev.added||0)+L.added,
        removed:(prev.removed||0)+L.removed,
        updated:(prev.updated||0)+L.updated,
        spotlight_add:    prev.spotlight_add&&prev.spotlight_add.length?prev.spotlight_add:L.spotAdd,
        spotlight_remove: prev.spotlight_remove&&prev.spotlight_remove.length?prev.spotlight_remove:L.spotRem,
        spotlight_update: prev.spotlight_update&&prev.spotlight_update.length?prev.spotUpd:L.spotUpd,
      };
      summary.features[k]=merged;
      hydratedLanes[k]={added:merged.added,removed:merged.removed,updated:merged.updated,items:[],spotAdd:merged.spotlight_add||[],spotRem:merged.spotlight_remove||[],spotUpd:merged.spotlight_update||[]};
    }
    summary.enabled=Object.assign(defaultEnabledMap(),summary.enabled||{});
    renderAll();
    return true;
  }

  // Log hydration (fallback)
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
      if(i<0){
        const m=raw.match(/feature["']?\s*:\s*"?(\w+)"?/i);
        if(m) lastFeatHint=mapFeat(m[1])||lastFeatHint;
        continue;
      }
      let obj; try{ obj=JSON.parse(raw.slice(i)); }catch{ continue; }
      if(!obj||!obj.event) continue;

      const feat=mapFeat(obj.feature); if(feat) lastFeatHint=feat;

      if(obj.event==="snapshot:progress"||obj.event==="progress:snapshot"){
        SnapAgg.update({done:obj.done,total:obj.total,final:!!obj.final,dst:obj.dst,feature:obj.feature});
      }
      if(/^apply:/.test(obj.event||"")){
        const isStart=/:start$/.test(obj.event);
        const isProg=/:progress$/.test(obj.event);
        const isDone=/:done$/.test(obj.event);
        if(isStart){ ApplyAgg.start({feature:lastFeatHint,total:obj.total,count:obj.count}); }
        if(isProg){ ApplyAgg.prog({feature:lastFeatHint,done:obj.done,total:obj.total}); }
        if(isDone){ ApplyAgg.done({feature:lastFeatHint,count:obj.result?.count||obj.count}); }
      }

      const laneKey=feat||lastFeatHint;
      if(obj.event==="two:done"&&laneKey){
        const res=obj.res||{}; const lane=ensureLane(laneKey);
        lane.added+=+res.adds||0; lane.removed+=+res.removes||0; lane.updated+=+res.updates||0; continue;
      }
      if(obj.event==="plan"&&laneKey){
        const lane=ensureLane(laneKey);
        lane.added+=+obj.add||0; lane.removed+=+obj.rem||0; continue;
      }
      if(obj.event==="two:plan"&&laneKey){
        const lane=ensureLane(laneKey);
        const addA=+obj.add_to_A||0, addB=+obj.add_to_B||0, remA=+obj.rem_from_A||0, remB=+obj.rem_from_B||0;
        lane.added+=Math.max(addA,addB); lane.removed+=Math.max(remA,remB); continue;
      }
      if(obj.event==="spotlight"&&(feat||obj.feature)&&obj.action&&obj.title){
        const lane=ensureLane(laneKey||obj.feature); const act=String(obj.action).toLowerCase();
        if(act==="add"&&lane.spotAdd.length<3) lane.spotAdd.push(obj.title);
        if(act==="remove"&&lane.spotRem.length<3) lane.spotRem.push(obj.title);
        if(act==="update"&&lane.spotUpd.length<3) lane.spotUpd.push(obj.title);
        continue;
      }
    }

    if(!Object.keys(tallies).length) return false;

    summary ||= {}; summary.features ||= {};
    for(const [feat,lane] of Object.entries(tallies)){
      const prev=summary.features[feat]||{};
      const merged={
        added:(prev.added||0)+(lane.added||0),
        removed:(prev.removed||0)+(lane.removed||0),
        updated:(prev.updated||0)+(lane.updated||0),
        spotlight_add:    (prev.spotlight_add&&prev.spotlight_add.length?prev.spotlight_add:lane.spotAdd).slice(-3),
        spotlight_remove: (prev.spotlight_remove&&prev.spotlight_remove.length?prev.spotlight_remove:lane.spotRem).slice(-3),
        spotlight_update: (prev.spotlight_update&&prev.spotlight_update.length?prev.spotlight_update:lane.spotUpd).slice(-3),
      };
      summary.features[feat]=merged;
      hydratedLanes[feat]={added:merged.added,removed:merged.removed,updated:merged.updated,items:[],spotAdd:merged.spotlight_add||[],spotRem:merged.spotlight_remove||[],spotUpd:merged.spotlight_update||[]};
    }
    summary.enabled=Object.assign(defaultEnabledMap(),summary.enabled||{});
    renderAll();
    return true;
  }

  // Status pull (for UI info only)
  const pullStatus=async()=>{ status=await fetchJSON("/api/status",status); try{ window._ui ||= {}; window._ui.status=status; }catch{} };

  // Summary → phase totals
  function hydratePhaseAggFromSummary(sum){
    const ph=sum?._phase||{};
    if(ph.snapshot && !SnapAgg.has()){
      PhaseAgg.snap.total=Number(ph.snapshot.total||0);
      PhaseAgg.snap.done=Number(ph.snapshot.done||0);
      PhaseAgg.snap.started=PhaseAgg.snap.total>0;
      PhaseAgg.snap.finished=!!ph.snapshot.final||(PhaseAgg.snap.total>0&&PhaseAgg.snap.done>=PhaseAgg.snap.total);
    }
    if(ph.apply){
      PhaseAgg.apply.total=Number(ph.apply.total||0);
      PhaseAgg.apply.done=Number(ph.apply.done||0);
      PhaseAgg.apply.started=PhaseAgg.apply.total>0;
      PhaseAgg.apply.finished=!!ph.apply.final||(PhaseAgg.apply.total>0&&PhaseAgg.apply.done>=PhaseAgg.apply.total);
    }
  }

  // Pull summary (stabilized)
  let _prevTL={start:false,pre:false,post:false,done:false}, _prevRunning=false;
  async function pullSummary(){
    const s = await fetchJSON("/api/run/summary", summary); if(!s) return;

    if (s && s.exit_code != null && (s.running === false || s.state === "idle") && !timeline.start) {
      optimistic = false; holdAtTen = false;
      resetProgress();
      timeline = { start:false, pre:false, post:false, done:false };
      setRunButtonState(false);
      renderProgress(); renderAll();
      return;
    }

    const prevTL=_prevTL, prevRunning=_prevRunning, prevRunKey=runKey;
    summary = s; hydratePhaseAggFromSummary(summary);

    runKey = runKeyOf(summary);
    if (runKey && runKey!==prevRunKey){
      lastRunStartedAt = Math.floor(Date.now()/1000);
      resetProgress();
      timeline = { start:true, pre:false, post:false, done:false };
      holdAtTen = true; optimistic = true;
    }

    const running = s?.running===true || s?.state==="running";
    setRunButtonState(running);

    let mapped={
      start:!!(s?.timeline?.start||s?.timeline?.started||s?.timeline?.[0]||s?.started),
      pre:  !!(s?.timeline?.pre||s?.timeline?.discovery||s?.timeline?.discovering||s?.timeline?.[1]),
      post: !!(s?.timeline?.post||s?.timeline?.syncing||s?.timeline?.apply||s?.timeline?.[2]),
      done: !!(s?.timeline?.done||s?.timeline?.finished||s?.timeline?.complete||s?.timeline?.[3]),
    };
    if(s?.phase){
      const p=String(s.phase).toLowerCase();
      if(p==="snapshot") mapped.pre=true;
      if(p==="apply"||p==="sync"||p==="syncing") mapped.post=true;
    }
    if(!mapped.done && !running && (s?.exit_code===0 || s?.finished || s?.end)){
      mapped={start:true,pre:true,post:true,done:true};
    }

    if(running && !prevRunning) resetProgress();

    mapped=clampTimelineForward(mapped);
    if(mapped.start!==prevTL.start||mapped.pre!==prevTL.pre||mapped.post!==prevTL.post||mapped.done!==prevTL.done) lastPhaseAt=Date.now();
    timeline=mapped;

    const baseline=(pctFromPhaseAgg() ?? asPctFromTimeline(timeline));
    progressPct=monotonePct(timeline,baseline);

    try{
      const nowRunning=!!running, justStarted=nowRunning&&!prevRunning, justStopped=!nowRunning&&prevRunning;
      if(justStarted){ _insightsTried=false; startRunVisualsSafe(!(timeline.pre||timeline.post||timeline.done)); }
      if(justStopped){ stopRunVisualsSafe(); }
      setRunButtonState(nowRunning);
    }catch{}

    const wasInProgress=prevRunning || (prevTL.start && !prevTL.done) || optimistic;
    const nowInProgress=running || (timeline.start && !timeline.done);
    const justFinished=wasInProgress && !nowInProgress && (timeline.done || (PhaseAgg.snap.finished && (PhaseAgg.apply.finished || PhaseAgg.apply.total===0)));

    if(justFinished){
      optimistic=false;
      try{ window.wallLoaded=false; window.updatePreviewVisibility?.(); window.loadWatchlist?.(); window.refreshSchedulingBanner?.(); }catch{}
      try{ (window.Insights?.refreshInsights || window.refreshInsights)?.(); }catch{}
      try{
        await hydrateFromInsights(summary?.raw_started_ts || (summary?.started_at?Date.parse(summary.started_at)/1000:lastRunStartedAt));
      }catch{}
      try{ window.dispatchEvent(new CustomEvent("sync-complete",{detail:{at:Date.now(),summary}})); }catch{}
    }

    _prevTL={...timeline}; _prevRunning=!!running;
    if(!summary.enabled) summary.enabled=defaultEnabledMap();
    lastEventTs=Date.now();
    renderAll();

    const hasFeatures=summary?.features && Object.keys(summary.features).length>0 &&
      Object.values(summary.features).some(v => (v?.added||v?.removed||v?.updated||0)>0 || (v?.spotlight_add?.length||v?.spotlight_remove?.length||v?.spotlight_update?.length));
    if(!hasFeatures && timeline.done && !_insightsTried){
      _insightsTried=true;
      const startTs=summary?.raw_started_ts || (summary?.started_at?Date.parse(summary.started_at)/1000:0);
      const got=await hydrateFromInsights(startTs);
      if(!got) setTimeout(()=>{ if(!hasFeatureData()) hydrateFromLog(); },300);
    }else{
      const missing=FEATS.some(f=>{
        const lane=summary?.features?.[f.key];
        return !(lane?.spotlight_add?.length||lane?.spotlight_remove?.length||lane?.spotlight_update?.length||(lane?.added||lane?.removed||lane?.updated));
      });
      if(missing) hydrateFromLog();
    }
  }

  // Render all
  const renderAll=()=>{ renderProgress(); renderLanes(); renderSpotlightSummary(); };

  // Helper used after summary fetch when we might backfill from insights/log
  const hasFeatureData=()=>
    summary?.features && Object.values(summary.features).some(v =>
      (v?.added||v?.removed||v?.updated||0)>0 ||
      (v?.spotlight_add?.length||v?.spotlight_remove?.length||v?.spotlight_update?.length)
    );

  // Button
  function setRunButtonState(running){
    const btn=document.getElementById("run"); if(!btn) return;
    btn.toggleAttribute("disabled",!!running);
    btn.setAttribute("aria-busy",running?"true":"false");
    btn.classList.toggle("glass",!!running);
    btn.title=running?"Synchronization running…":"Run synchronization";
  }

  // Wire the manual run button
  function wireRunButton(){
    const btn=document.getElementById("run"); if(!btn||wireRunButton._done) return;
    wireRunButton._done=true;
    btn.addEventListener("click",async ()=>{
      if(btn.disabled||btn.classList.contains("glass")) return;
      setRunButtonState(true);
      resetProgress(); optimistic=true; holdAtTen=true;
      lastRunStartedAt=Math.floor(Date.now()/1000);
      startRunVisualsSafe(true);
      timeline={start:true,pre:false,post:false,done:false};
      window.UX.updateProgress?.({pct:10});
      renderProgress();
    },{capture:true});
  }

  // SSE streams (kept singletons so we can reconnect cleanly)
  let esSummary=null, esLogs=null;

  window.openSummaryStream = function openSummaryStream(){
    try{
      try{ esSummary?.close?.(); }catch{}
      esSummary = new EventSource("/api/run/summary/stream");

      // Some backends also push a compact final summary as default message
      esSummary.onmessage = (ev) => { try{
        const incoming = JSON.parse(ev.data || "{}");
        if (incoming && (incoming.running===false || incoming.exit_code!=null)) {
          summary = incoming;
          hydratePhaseAggFromSummary(summary);
          if (!timeline.start && summary.exit_code!=null) {
            // idle finalized summary for previous run → keep UI idle
            optimistic=false; holdAtTen=false;
            resetProgress();
            timeline={start:false,pre:false,post:false,done:false};
            setRunButtonState(false);
            renderProgress(); renderAll();
            return;
          }
        }
        lastEventTs=Date.now();
      }catch{} };

      const markInit = () => {
        resetProgress();
        optimistic = true;
        holdAtTen = true;
        lastRunStartedAt = Math.floor(Date.now()/1000);
        timeline = { start:true, pre:false, post:false, done:false };
        window.UX.updateTimeline?.(timeline);
        window.UX.updateProgress?.({ pct:10 });
        renderProgress();
        lastEventTs = Date.now();
      };
      ["health","api:hit","run:start","run:pair","feature:start"].forEach(n=>{
        esSummary.addEventListener(n, markInit);
      });

      const onSnap = (ev) => { try{
        const d = JSON.parse(ev.data || "{}");
        holdAtTen = false;
        SnapAgg.update(d);
        window.UX.updateTimeline?.({ start:true, pre:true, post:PhaseAgg.apply.started||false, done:false });
        renderProgress(); lastEventTs = Date.now();
      }catch{} };
      esSummary.addEventListener("progress:snapshot", onSnap);
      esSummary.addEventListener("snapshot:progress", onSnap);

      // Apply progress (we record it immediately; visual uses it only after all snapshots finish)
      esSummary.addEventListener("progress:apply", ev => { try{
        const d = JSON.parse(ev.data || "{}");
        ApplyAgg.prog({ feature:"__global__", done:d.done, total:d.total });
        window.UX.updateTimeline?.({ start:true, pre:true, post:true, done:false });
        renderProgress(); lastEventTs = Date.now();
      }catch{} });

      // Optional per-feature markers if backend emits them on summary stream
      ["apply:add:start","apply:remove:start"].forEach(name=>{
        esSummary.addEventListener(name, ev => { try{
          const d = JSON.parse(ev.data || "{}");
          ApplyAgg.start(d);
          window.UX.updateTimeline?.({ start:true, pre:true, post:true, done:false });
          renderProgress(); lastEventTs = Date.now();
        }catch{} });
      });
      ["apply:add:done","apply:remove:done"].forEach(name=>{
        esSummary.addEventListener(name, ev => { try{
          const d = JSON.parse(ev.data || "{}");
          ApplyAgg.done(d);
          const hardDone=(PhaseAgg.apply.total>0 && PhaseAgg.apply.done>=PhaseAgg.apply.total);
          window.UX.updateTimeline?.({ start:true, pre:true, post:true, done:hardDone });
          renderProgress(); lastEventTs = Date.now();
        }catch{} });
      });

      esSummary.addEventListener("run:done", () => { try{
        timeline = { start:true, pre:true, post:true, done:true };
        renderProgress(); setRunButtonState(false); lastEventTs = Date.now();
        stopRunVisualsSafe();
      }catch{} });

      ["run:error","run:aborted"].forEach(name=>{
        esSummary.addEventListener(name, () => { try{
          timeline.done = true; setRunButtonState(false); renderProgress(); lastEventTs = Date.now();
          stopRunVisualsSafe();
        }catch{} });
      });

      esSummary.onerror = () => {
        try{ esSummary.close(); }catch{}
        setTimeout(openSummaryStream, 2000);
      };
    }catch{}
  };

  window.openLogStream = function openLogStream(){
    try{
      try{ esLogs?.close?.(); }catch{}
      esLogs = new EventSource("/api/logs/stream");

      esLogs.onmessage = (ev) => {
        lastEventTs = Date.now();
        const txt = String(ev.data || "");
        const m = txt.match(/\[SYNC\]\s*exit\s*code\s*:\s*(\d+)/i);
        if (m) {
          const code = +m[1];
          // Only apply if there is NO active run in UI; otherwise this is a previous run's trailer
          if (!(summary?.running || timeline.start)) {
            optimistic = false; holdAtTen = false;
            resetProgress();
            timeline = { start:false, pre:false, post:false, done:false };
            setRunButtonState(false);
            summary = { ...(summary||{}), running:false, exit_code:code, timeline:{ start:false, pre:false, post:false, done:true } };
            renderProgress(); renderAll();
          }
        }
      };

      const markInit = () => {
        resetProgress(); optimistic = true; holdAtTen = true;
        lastRunStartedAt = Math.floor(Date.now()/1000);
        timeline = { start:true, pre:false, post:false, done:false };
        window.UX.updateTimeline?.(timeline);
        window.UX.updateProgress?.({ pct:10 });
        renderProgress();
        lastEventTs = Date.now();
      };
      esLogs.addEventListener("run:start", markInit);

      const onSnap = (ev) => { try{
        const d = JSON.parse(ev.data || "{}");
        holdAtTen = false; SnapAgg.update(d);
        window.UX.updateTimeline?.({ start:true, pre:true, post:PhaseAgg.apply.started||false, done:false });
        renderProgress(); lastEventTs = Date.now();
      }catch{} };
      esLogs.addEventListener("snapshot:progress", onSnap);
      esLogs.addEventListener("progress:snapshot", onSnap);

      ["apply:add:start","apply:remove:start"].forEach(name=>{
        esLogs.addEventListener(name, ev => { try{
          const d = JSON.parse(ev.data || "{}");
          ApplyAgg.start(d);
          window.UX.updateTimeline?.({ start:true, pre:true, post:true, done:false });
          renderProgress(); lastEventTs = Date.now();
        }catch{} });
      });

      ["apply:add:done","apply:remove:done"].forEach(name=>{
        esLogs.addEventListener(name, ev => { try{
          const d = JSON.parse(ev.data || "{}");
          ApplyAgg.done(d);
          const hardDone = (PhaseAgg.apply.total>0 && PhaseAgg.apply.done>=PhaseAgg.apply.total);
          window.UX.updateTimeline?.({ start:true, pre:true, post:true, done:hardDone });
          renderProgress(); lastEventTs = Date.now();
        }catch{} });
      });

      esLogs.addEventListener("progress:apply", ev => { try{
        const d = JSON.parse(ev.data || "{}");
        ApplyAgg.prog(d);
        window.UX.updateTimeline?.({ start:true, pre:true, post:true, done:false });
        renderProgress(); lastEventTs = Date.now();
      }catch{} });

      esLogs.addEventListener("run:done", () => { try{
        timeline = { start:true, pre:true, post:true, done:true };
        renderProgress(); setRunButtonState(false); lastEventTs = Date.now();
        stopRunVisualsSafe();
      }catch{} });

      ["run:error","run:aborted"].forEach(name=>{
        esLogs.addEventListener(name, () => { try{
          setRunButtonState(false);
          timeline.done = true; renderProgress(); lastEventTs = Date.now();
          stopRunVisualsSafe();
        }catch{} });
      });

      esLogs.onerror = () => {
        try{ esLogs.close(); }catch{}
        setTimeout(openLogStream, 2000);
      };
    }catch{}
  };

  // Public UX hooks
  window.UX={
    updateTimeline:(tl)=>window.dispatchEvent(new CustomEvent("ux:timeline",{detail:tl||{}})),
    updateProgress:(payload)=>payload && window.dispatchEvent(new CustomEvent("ux:progress",{detail:payload})),
    refresh:()=>pullSummary().then(()=>renderAll())
  };

  // Events from the rest of the UI
  window.addEventListener("ux:timeline",(e)=>{
    const tl=e.detail||{};
    if(phaseIdx(timeline)===3 && tl?.start && !tl?.done) resetProgress();
    timeline=clampTimelineForward({start:!!tl.start,pre:!!tl.pre,post:!!tl.post,done:!!tl.done});
    const base=(pctFromPhaseAgg() ?? asPctFromTimeline(timeline));
    progressPct=monotonePct(timeline,base);
    renderProgress();
  });

  window.addEventListener("ux:progress",(e)=>{
    const p=e.detail?.pct;
    if(typeof p==="number"){ progressPct=monotonePct(timeline,p); renderProgress(); }
  });

  // If tab comes back after sleep, just reconnect SSE pipes
  window.addEventListener("visibilitychange",()=>{ if(document.visibilityState==="visible"){ openSummaryStream(); openLogStream(); } });

  // Periodic work: fetch summary + pairs, smooth starting phase
  function tick(){
    const running=(summary?.running===true)||(timeline.start&&!timeline.done);
    pullSummary();

    if((Date.now()-lastPairsAt)>10000){
      pullPairs().finally(()=>{ lastPairsAt=Date.now(); renderLanes(); });
    }

    // Gentle drift only in "starting" stage before snapshot numbers arrive
    if(running && optimistic && !holdAtTen && !(timeline.pre||timeline.post||timeline.done)){
      const since=Date.now()-lastPhaseAt;
      if(since>900){
        progressPct=clampPct((progressPct||0)+2,0,24); // cap well under preStart(25)
        renderProgress();
      }
    }

    // If SSE goes quiet for too long, nudge reconnection
    const staleFor=Date.now()-lastEventTs;
    if(staleFor>20000){ openSummaryStream(); openLogStream(); lastEventTs=Date.now(); }

    clearTimeout(tick._t);
    tick._t=setTimeout(tick,running?1000:2500);
  }

  // Boot
  pullPairs();
  renderAll();
  wireRunButton();
  openSummaryStream();
  openLogStream();
  tick();
})();
