// UX panel
//* Refactoring project: main.js (v0.2) */
//*-------------------------------------*/

(() => {
  const FEATS = [
    { key: "watchlist", icon: "movie",       label: "Watchlist" },
    { key: "ratings",   icon: "star",        label: "Ratings"   },
    { key: "history",   icon: "play_arrow",  label: "History"   },
    { key: "playlists", icon: "queue_music", label: "Playlists" },
  ];

  const elProgress = document.getElementById("ux-progress");
  const elLanes    = document.getElementById("ux-lanes");
  const elSpot     = document.getElementById("ux-spotlight");
  if (!elProgress || !elLanes || !elSpot) return;

  // styles
  (document.getElementById("ux-styles") || {}).remove?.();
  document.head.appendChild(Object.assign(document.createElement("style"), {
    id: "ux-styles",
    textContent: `
#ux-progress,#ux-lanes{margin-top:12px}
.ux-rail{position:relative;height:10px;border-radius:999px;background:#1f1f26;overflow:hidden}
.ux-rail.error{background:linear-gradient(90deg,#311,#401818)}
.ux-bead{position:absolute;inset:0 auto 0 0;width:0%;height:100%;border-radius:inherit;background:linear-gradient(90deg,#7c4dff,#00d4ff);box-shadow:inset 0 0 14px rgba(124,77,255,.35);transition:width .35s ease}
.ux-bead.indet{background-size:200% 100%;animation:fillShift 1.1s ease-in-out infinite}
@keyframes fillShift{0%{background-position:0% 50%}100%{background-position:100% 50%}}
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
#run[disabled]{pointer-events:none;opacity:.6;filter:saturate(.7)}
#run.glass{position:relative}
#run.glass::after{content:"";position:absolute;inset:6px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .9s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
`
  }));

  // state
  let timeline = { start:false, pre:false, post:false, done:false };
  let progressPct = 0, status = null, summary = null;
  let _prevTL = { start:false, pre:false, post:false, done:false }, _prevRunning = false;
  let optimistic = false, enabledFromPairs = null, lastPairsAt = 0, lastPhaseAt = 0, _insightsTried = false;
  const lastCounts = Object.create(null), hydratedLanes = Object.create(null);

  // forward-only progress
  let pctMemo = 0, phaseMemo = -1;
  const phaseIdx = tl => tl?.done?3 : tl?.post?2 : tl?.pre?1 : tl?.start?0 : -1;
  const ANCHOR = [0,33,66,100];
  const clampPct = (n, lo=0, hi=100) => Math.max(lo, Math.min(hi, Math.round(n)));
  const resetProgress = () => { pctMemo = 0; phaseMemo = -1; lastPhaseAt = Date.now(); };
  const clampTimelineForward = next => (phaseIdx(next) < phaseIdx(timeline)) ? timeline : next;

  function monotonePct(tl, pct){
    const idx = phaseIdx(tl);
    let next = typeof pct === "number" ? pct : (idx >= 0 ? ANCHOR[idx] : 0);
    if (tl?.done) next = 100;
    if (idx < phaseMemo) next = pctMemo; else if (idx > phaseMemo) phaseMemo = idx;
    return (pctMemo = Math.max(pctMemo, clampPct(next)));
  }

  function asPctFromTimeline(tl){
    if (!tl) return 0;
    if (tl.done) return 100;
    const idx = tl.post ? 2 : tl.pre ? 1 : 0;
    const anchors = [12, 33, 66, 100]; // tiny head-start for “Start”
    return anchors[idx];
  }

  // net helpers
  const fetchJSON = async (url, fallback=null) => {
    try{ const r=await fetch(url,{credentials:"same-origin"}); return r.ok ? r.json() : fallback; }catch{ return fallback; }
  };
  const fetchFirstJSON = async (urls, fallback=null) => { for (const u of urls){ const j=await fetchJSON(u,null); if (j) return j; } return fallback; };

  const defaultEnabledMap = () => ({ watchlist:true, ratings:true, history:true, playlists:true });
  const getEnabledMap = () => enabledFromPairs ?? (summary?.enabled || defaultEnabledMap());

  // rail metrics
  const Rail = (() => {
    let cache = null;
    const _labels = (root) => {
      const steps = (root||document).querySelector(".ux-rail-steps"); if(!steps) return null;
      const pick = (name)=> steps.querySelector(`[data-step="${name}"]`)
        || [...steps.querySelectorAll("*")].find(el => el.childElementCount===0 && (el.textContent||"").trim().toLowerCase()===name);
      const start=pick("start"), disc=pick("discovering")||pick("discover")||pick("pre"), sync=pick("syncing")||pick("sync")||pick("post"), done=pick("done");
      return (start && disc && sync && done) ? { steps,start,disc,sync,done } : null;
    };
    const _measure = (root) => {
      if (cache) return cache;
      const L=_labels(root); if(!L) return null;
      const hb=L.steps.getBoundingClientRect(), rb=el=>el.getBoundingClientRect();
      const s=rb(L.start), d=rb(L.disc), y=rb(L.sync), z=rb(L.done);
      const x0=s.left-hb.left, x1=d.left+d.width/2-hb.left, x2=y.left+y.width/2-hb.left, x3=z.right-hb.left;
      const span=Math.max(1,x3-x0);
      cache={host:L.steps,x0,x1,x2,x3,span,ml:x0,mr:(hb.width-x3)}; return cache;
    };
    const align = (root) => { const m=_measure(root); if(!m) return; const rail=(root||document).querySelector(".ux-rail"); if(!rail) return; rail.style.marginLeft=Math.round(m.ml)+"px"; rail.style.marginRight=Math.round(m.mr)+"px"; };
    const pctFromTimeline = (tl,root) => { const m=_measure(root); if(!m) return null; let x=m.x0; if(tl?.done)x=m.x3; else if(tl?.post)x=m.x2; else if(tl?.pre)x=m.x1; return clampPct(Math.round(((x-m.x0)/m.span)*100),0,100); };
    window.addEventListener("resize", () => { cache=null; requestAnimationFrame(()=>align(document)); });
    return { align, pct:pctFromTimeline, _invalidate(){ cache=null; } };
  })();

  // run button state
  function setRunButtonState(running){
  const btn=document.getElementById("run"); if(!btn) return;
  btn.toggleAttribute("disabled", !!running);
  btn.setAttribute("aria-busy", running?"true":"false");
  btn.classList.toggle("glass", !!running);
}

  // progress rail
  function renderProgress(){
    elProgress.innerHTML = "";
    const rail = Object.assign(document.createElement("div"), { className: "ux-rail" });
    const fill = Object.assign(document.createElement("div"), { className: "ux-bead" });

    const base = (Rail.pct?.(timeline, elProgress) ?? asPctFromTimeline(timeline));
    fill.style.width = monotonePct(timeline, base) + "%";

    if (optimistic && !(timeline?.pre || timeline?.post || timeline?.done)) fill.classList.add("indet");
    if (summary?.exit_code != null && summary.exit_code !== 0) rail.classList.add("error");

    const steps = Object.assign(document.createElement("div"), { className: "ux-rail-steps muted" });
    [["Start","start"],["Discovering","discovering"],["Syncing","syncing"],["Done","done"]]
      .forEach(([txt,key]) => { const s=document.createElement("span"); s.textContent=txt; s.dataset.step=key; steps.appendChild(s); });

    rail.appendChild(fill);
    elProgress.append(rail, steps);
    Rail._invalidate?.(); Rail.align?.(elProgress);
  }

  // lanes
  const synthSpots = (items,key) => {
    const a=[],r=[],u=[], titleOf = x => typeof x==="string"?x:(x?.title||x?.name||x?.key||"item");
    for (const it of items||[]){
      const t=titleOf(it), act=(it?.action||it?.op||it?.change||"").toLowerCase();
      let tag="upd";
      if (key==="history" && (it?.watched||it?.watched_at)) tag="add";
      else if (act.includes("add")||act.includes("watch")||act.includes("scrobble")) tag="add";
      else if (act.includes("rem")||act.includes("del")||act.includes("unwatch")) tag="rem";
      if (tag==="add"&&a.length<3)a.push(t); else if(tag==="rem"&&r.length<3)r.push(t); else if(u.length<3)u.push(t);
      if (a.length+r.length+u.length>=3) break;
    }
    return { a,r,u };
  };

  const getLaneStats = (sum,key) => {
    const f = (sum?.features?.[key]) || sum?.[key] || {};
    const added = f.added ?? f.add ?? f.adds ?? f.plus ?? 0;
    const removed = f.removed ?? f.del ?? f.deletes ?? f.minus ?? 0;
    const updated = f.updated ?? f.upd ?? f.changed ?? 0;
    const items = Array.isArray(f.items)?f.items:[];
    let spotAdd = Array.isArray(f.spotlight_add)?f.spotlight_add:[],
        spotRem = Array.isArray(f.spotlight_remove)?f.spotlight_remove:[],
        spotUpd = Array.isArray(f.spotlight_update)?f.spotlight_update:[];
    if ((added||removed||updated)===0 && hydratedLanes[key]) return { ...hydratedLanes[key] };
    if (!spotAdd.length && !spotRem.length && !spotUpd.length && items.length){
      const s = synthSpots(items,key); spotAdd=s.a; spotRem=s.r; spotUpd=s.u;
    }
    const out = { added,removed,updated,items,spotAdd,spotRem,spotUpd };
    if ((added+removed+updated)>0 || spotAdd.length || spotRem.length || spotUpd.length) hydratedLanes[key]=out;
    return out;
  };

  const laneState = (sum,key,enabled) => {
    const err = (sum?.exit_code != null && sum.exit_code !== 0);
    if (!enabled) return "skip";
    if (err) return "err";
    if (timeline?.done) return "ok";
    if (timeline?.start && !timeline?.done) return "run";
    return "skip";
  };

  const fmtDelta = (a,r,u)=>`+${a||0} / -${r||0} / ~${u||0}`;

  function renderLanes(){
    elLanes.innerHTML="";
    const wrap=document.createElement("div"); wrap.className="lanes";
    const enabledMap=getEnabledMap();
    const running = summary?.running===true || (timeline.start && !timeline.done);

    for (const f of FEATS){
      const isEnabled=!!enabledMap[f.key];
      const { added,removed,updated,items,spotAdd,spotRem,spotUpd } = getLaneStats(summary||{}, f.key);
      const st = laneState(summary||{}, f.key, isEnabled);

      const lane=document.createElement("div"); lane.className="lane";
      if (!isEnabled) lane.classList.add("disabled");

      const total=(added||0)+(removed||0)+(updated||0);
      const prev = lastCounts[f.key] ?? 0;
      if (running && total>prev && isEnabled){ lane.classList.add("shake"); setTimeout(()=>lane.classList.remove("shake"),450); }
      lastCounts[f.key]=total;

      const h=document.createElement("div"); h.className="lane-h";
      const ico=document.createElement("div"); ico.className="lane-ico"; ico.innerHTML=`<span class="material-symbols-outlined material-symbol material-icons">${f.icon}</span>`;
      const ttl=document.createElement("div"); ttl.className="lane-title"; ttl.textContent=f.label;

      const badges=document.createElement("div"); badges.className="lane-badges";
      const delta=document.createElement("span"); delta.className="delta"; delta.innerHTML=`<b>${fmtDelta(added,removed,updated)}</b>`;
      const chip=document.createElement("span");
      chip.className="chip " + (st==="ok"?"ok":st==="run"?"run":st==="err"?"err":"skip");
      chip.textContent = !isEnabled?"Disabled":st==="err"?"Failed":st==="ok"?"Synced":st==="run"?"Running":"Skipped";
      badges.append(delta,chip);

      h.append(ico,ttl,badges); lane.appendChild(h);

      const body=document.createElement("div"); body.className="lane-body";
      const spots=[];
      for (const x of (spotAdd||[]).slice(0,2)) spots.push({ t:"add", text:x?.title||x });
      for (const x of (spotRem||[]).slice(0,2)) spots.push({ t:"rem", text:x?.title||x });
      for (const x of (spotUpd||[]).slice(0,2)) spots.push({ t:"upd", text:x?.title||x });

      if (!spots.length && items?.length){
        const s=synthSpots(items,f.key);
        for (const x of s.a.slice(0,2)) spots.push({ t:"add", text:x });
        for (const x of s.r.slice(0,2)) spots.push({ t:"rem", text:x });
        for (const x of s.u.slice(0,2)) spots.push({ t:"upd", text:x });
      }

      if (!isEnabled){
        body.appendChild(Object.assign(document.createElement("div"), { className: "spot muted small", textContent: "Feature not configured" }));
      } else if (!spots.length){
        body.appendChild(Object.assign(document.createElement("div"), { className: "spot muted small", textContent: timeline?.done ? "No changes" : "Awaiting results…" }));
      } else {
        for (const s of spots.slice(0,3)){
          const row=document.createElement("div"); row.className="spot";
          const tag=document.createElement("span"); tag.className="tag "+(s.t==="add"?"t-add":s.t==="rem"?"t-rem":"t-upd"); tag.textContent=s.t==="add"?"Added":s.t==="rem"?"Removed":"Updated";
          row.append(tag, Object.assign(document.createElement("span"), { textContent: s.text })); body.appendChild(row);
        }
      }

      lane.appendChild(body); wrap.appendChild(lane);
    }
    elLanes.appendChild(wrap);
  }

  const renderSpotlightSummary = () => { elSpot.innerHTML=""; };

  // pairs
  async function pullPairs(){
    const arr = await fetchJSON("/api/pairs", null);
    if (!Array.isArray(arr)) return;
    if (!arr.length){ enabledFromPairs = { watchlist:false, ratings:false, history:false, playlists:false }; return; }
    const enabled = { watchlist:false, ratings:false, history:false, playlists:false };
    for (const p of arr){
      const feats=p?.features||{};
      for (const f of FEATS){
        const cfg=feats[f.key];
        if (cfg && (cfg.enable===true || cfg.enabled===true)) enabled[f.key]=true;
      }
    }
    enabledFromPairs=enabled;
  }

  // insights
  async function hydrateFromInsights(startTsEpoch){
    const src = await fetchFirstJSON(
      ["/api/insights", "/api/statistics", "/statistics.json", "/data/statistics.json"], null
    );
    const events = src?.events;
    if (!Array.isArray(events) || !events.length) return false;
    const since = Math.floor(startTsEpoch || 0);
    const recent = events.filter(e => (e.ts || 0) >= since);
    let added = 0, removed = 0; const spotAdd = [], spotRem = [];
    for (const e of recent){
      const title = e.title || e.key || "item";
      if (e.action === "add")   { added++;   if (spotAdd.length<3) spotAdd.push(title); }
      if (e.action === "remove"){ removed++; if (spotRem.length<3) spotRem.push(title); }
    }
    summary ||= {}; summary.features ||= {};
    const lane = summary.features.watchlist || {};
    lane.added = added || lane.added || 0; lane.removed = removed || lane.removed || 0; lane.updated = lane.updated || 0;
    if (!lane.spotlight_add?.length && spotAdd.length) lane.spotlight_add = spotAdd;
    if (!lane.spotlight_remove?.length && spotRem.length) lane.spotlight_remove = spotRem;
    summary.features.watchlist = lane;
    hydratedLanes.watchlist = { added: lane.added, removed: lane.removed, updated: lane.updated, items: [], spotAdd: lane.spotlight_add || [], spotRem: lane.spotlight_remove || [], spotUpd: [] };
    summary.enabled = Object.assign(defaultEnabledMap(), summary.enabled || {});
    renderAll();
    return (added>0 || removed>0 || spotAdd.length || spotRem.length);
  }

  function hydrateFromLog(){
    const det=document.getElementById("det-log"); if(!det) return false;
    const txt=det.innerText||det.textContent||""; if(!txt) return false;

    const lines=txt.split(/\n+/).slice(-500);
    const tallies=Object.create(null);
    const ensureLane=(k)=>(tallies[k] ||= { added:0, removed:0, updated:0, spotAdd:[], spotRem:[], spotUpd:[] });

    const mapFeat=(s)=>{ const f=String(s||"").trim().toLowerCase(); if(!f) return ""; if (f==="watch"||f==="watched") return "history"; return f; };
    let lastFeatHint="";

    for (const raw of lines){
      const i=raw.indexOf("{");
      if (i<0){ const m=raw.match(/feature["']?\s*:\s*"?(\w+)"?/i); if (m) lastFeatHint=mapFeat(m[1])||lastFeatHint; continue; }
      let obj; try{ obj=JSON.parse(raw.slice(i)); }catch{ continue; }
      if (!obj || !obj.event) continue;

      const feat=mapFeat(obj.feature); if (feat) lastFeatHint=feat;

      if (obj.event==="two:done"){
        if (!feat) continue; const res=obj.res||{}; const lane=ensureLane(feat);
        lane.added += +res.adds||0; lane.removed += +res.removes||0; lane.updated += +res.updates||0; continue;
      }
      if (obj.event==="plan"){
        if (!feat) continue; const lane=ensureLane(feat);
        lane.added += +obj.add||0; lane.removed += +obj.rem||0; continue;
      }
      if (obj.event==="two:plan"){
        if (!feat) continue; const lane=ensureLane(feat);
        const addA=+obj.add_to_A||0, addB=+obj.add_to_B||0, remA=+obj.rem_from_A||0, remB=+obj.rem_from_B||0;
        lane.added += Math.max(addA,addB); lane.removed += Math.max(remA,remB); continue;
      }
      if (obj.event==="spotlight" && obj.feature && obj.action && obj.title){
        const lane=ensureLane(feat||obj.feature); const act=String(obj.action).toLowerCase();
        if (act==="add"   && lane.spotAdd.length<3) lane.spotAdd.push(obj.title);
        if (act==="remove"&& lane.spotRem.length<3) lane.spotRem.push(obj.title);
        if (act==="update"&& lane.spotUpd.length<3) lane.spotUpd.push(obj.title);
        continue;
      }
    }

    if (!Object.keys(tallies).length){
      let added=0, removed=0;
      for (let i=lines.length-1;i>=0;i--){ const m=lines[i].match(/Sync complete·\+(\d+)\s*\/\s*-(\d+)/i); if (m){ added=+m[1]||0; removed=+m[2]||0; break; } }
      if (added===0 && removed===0){
        for (let i=lines.length-1;i>=0;i--){ const m=lines[i].match(/Plan·add A=(\d+),\s*add B=(\d+),\s*remove A=(\d+),\s*remove B=(\d+)/i);
          if (m){ added=Math.max(+m[1]||0,+m[2]||0); removed=Math.max(+m[3]||0,+m[4]||0); break; } }
      }
      const feat=lastFeatHint || "watchlist";
      summary ||= {}; summary.features ||= {};
      const lane = Object.assign({ added:0, removed:0, updated:0 }, summary.features[feat] || {});
      lane.added ||= added; lane.removed ||= removed;
      summary.features[feat]=lane;
      hydratedLanes[feat]={ added:lane.added, removed:lane.removed, updated:lane.updated, items:[], spotAdd:[], spotRem:[], spotUpd:[] };
      summary.enabled = Object.assign(defaultEnabledMap(), summary.enabled || {});
      renderAll();
      return (added>0 || removed>0);
    }

    summary ||= {}; summary.features ||= {};
    for (const [feat,lane] of Object.entries(tallies)){
      const prev=summary.features[feat] || {};
      const merged = {
        added:(prev.added||0)+(lane.added||0),
        removed:(prev.removed||0)+(lane.removed||0),
        updated:(prev.updated||0)+(lane.updated||0),
        spotlight_add:    (prev.spotlight_add    && prev.spotlight_add.length    ? prev.spotlight_add    : lane.spotAdd).slice(-3),
        spotlight_remove: (prev.spotlight_remove && prev.spotlight_remove.length ? prev.spotlight_remove : lane.spotRem).slice(-3),
        spotlight_update: (prev.spotlight_update && prev.spotlight_update.length ? prev.spotlight_update : lane.spotUpd).slice(-3),
      };
      summary.features[feat]=merged;
      hydratedLanes[feat]={ added:merged.added, removed:merged.removed, updated:merged.updated, items:[], spotAdd:merged.spotlight_add||[], spotRem:merged.spotlight_remove||[], spotUpd:merged.spotlight_update||[] };
    }
    summary.enabled = Object.assign(defaultEnabledMap(), summary.enabled || {});
    renderAll();
    return true;
  }

  const hasFeatureData = () =>
    summary?.features && Object.values(summary.features).some(v =>
      (v?.added||v?.removed||v?.updated||0)>0 ||
      (v?.spotlight_add?.length||v?.spotlight_remove?.length||v?.spotlight_update?.length)
    );

  const pullStatus = async () => { status = await fetchJSON("/api/status", status); try{ window._ui ||= {}; window._ui.status = status; }catch{} };

  // summary + timeline gating
  async function pullSummary(){
    const s = await fetchJSON("/api/run/summary", summary); if (!s) return;
    const prevTL=_prevTL, prevRunning=_prevRunning;
    summary = s;

    const tl = s?.timeline || s?.tl || null;
    const running = s?.running===true || s?.state==="running";
    setRunButtonState(running);
    const exitedOk = (s?.exit_code===0) || (s?.exit===0) || (s?.status==="ok");

    let mapped = {
      start: !!(tl?.start || tl?.started || tl?.[0] || s?.started),
      pre:   !!(tl?.pre   || tl?.discovery || tl?.discovering || tl?.[1]),
      post:  !!(tl?.post  || tl?.syncing   || tl?.apply       || tl?.[2]),
      done:  !!(tl?.done  || tl?.finished  || tl?.complete    || tl?.[3]),
    };
    if (!mapped.done && !running && (exitedOk || s?.finished || s?.end)) mapped = { start:true, pre:true, post:true, done:true };

    // new run? reset memos
    if (running && !prevRunning) resetProgress();

    // never go backwards
    mapped = clampTimelineForward(mapped);
    const changedPhase = (mapped.start!==prevTL.start || mapped.pre!==prevTL.pre || mapped.post!==prevTL.post || mapped.done!==prevTL.done);
    if (changedPhase) lastPhaseAt = Date.now();
    timeline = mapped;

    const baseline = (Rail.pct(timeline, elProgress) ?? asPctFromTimeline(timeline));
    progressPct = monotonePct(timeline, baseline);

    try {
      updateProgressFromTimeline?.(timeline);

      const nowRunning  = !!running;
      const justStarted =  nowRunning && !prevRunning;
      const justStopped = !nowRunning &&  prevRunning;

      if (justStarted) {
        _insightsTried = false;
        const indet = !(timeline.pre || timeline.post || timeline.done);
        startRunVisuals?.(indet);
      }
      if (justStopped) {
        stopRunVisuals?.();
      }

      setRunButtonState(nowRunning);
      recomputeRunDisabled?.();
    } catch {}

    const wasInProgress = prevRunning || (prevTL.start && !prevTL.done) || optimistic;
    const nowInProgress = running || (timeline.start && !timeline.done);
    const justFinished  = wasInProgress && !nowInProgress && timeline.done;

    if (justFinished){
      optimistic=false;
      try{
        window.wallLoaded=false;
        window.updatePreviewVisibility?.();
        window.loadWatchlist?.();
        window.refreshSchedulingBanner?.();
      }catch{}
      try{ (window.Insights?.refreshInsights || window.refreshInsights)?.(); }catch{}
      try{ window.dispatchEvent(new CustomEvent("sync-complete",{ detail:{ at:Date.now(), summary } })); }catch{}
    }

    _prevTL = { ...timeline }; _prevRunning = !!running;
    if (!summary.enabled) summary.enabled = defaultEnabledMap();
    renderAll();

    const hasFeatures = summary?.features && Object.keys(summary.features).length>0 &&
      Object.values(summary.features).some(v => (v?.added||v?.removed||v?.updated||0)>0 || (v?.spotlight_add?.length||v?.spotlight_remove?.length||v?.spotlight_update?.length));
    if (!hasFeatures && timeline.done && !_insightsTried){
      _insightsTried = true;
      const startTs = summary?.raw_started_ts || (summary?.started_at ? Date.parse(summary.started_at)/1000 : 0);
      const got = await hydrateFromInsights(startTs);
      if (!got) setTimeout(() => { if (!hasFeatureData()) hydrateFromLog(); }, 300);
    }
  }

  const renderAll = () => { renderProgress(); renderLanes(); renderSpotlightSummary(); };

  // main tick
  function tick(){
    const running = (summary?.running===true) || (timeline.start && !timeline.done);
    pullSummary();
    if ((Date.now()-lastPairsAt)>10000) pullPairs().finally(()=>{ lastPairsAt=Date.now(); renderLanes(); });
    if (running && optimistic && !(timeline.pre||timeline.post||timeline.done)){
      const since = Date.now() - (lastPhaseAt||0);
      if (since>900){
        const floor = Math.max((Rail.pct({start:true},elProgress) ?? 12),12);
        const cap   = (Rail.pct({pre:true},elProgress) ?? 60);
        progressPct = clampPct((progressPct || floor) + 2, floor, cap - 1);
        renderProgress();
      }
    }
    clearTimeout(tick._t);
    tick._t = setTimeout(tick, running ? 1000 : 2500);
  }

  // run button
  function wireRunButton(){
    const btn=document.getElementById("run"); if (!btn || wireRunButton._done) return;
    wireRunButton._done = true;
    btn.addEventListener("click", () => {
      if (btn.disabled || btn.classList.contains("glass")) return;
      setRunButtonState(true);
      resetProgress(); optimistic=true;
      window.startRunVisuals?.(true); window.recomputeRunDisabled?.();
      timeline={ start:true, pre:false, post:false, done:false };
      progressPct = Math.max(progressPct, (Rail.pct({start:true},elProgress) ?? 12));
      renderProgress();
    }, { capture:true });
  }

  // event bridge (never regress)
  window.addEventListener("ux:timeline", (e) => {
    const tl=e.detail||{};
    if (phaseIdx(timeline)===3 && tl?.start && !tl?.done) resetProgress(); // brand-new run
    timeline = clampTimelineForward({ start:!!tl.start, pre:!!tl.pre, post:!!tl.post, done:!!tl.done });
    const base=(Rail.pct(timeline, elProgress) ?? asPctFromTimeline(timeline));
    progressPct = monotonePct(timeline, base);
    lastPhaseAt=Date.now();
    renderProgress();
  });

  window.addEventListener("ux:progress", (e) => {
    const p=e.detail?.pct;
    if (typeof p==="number"){ progressPct = monotonePct(timeline, p); renderProgress(); }
  });

  // UX API
  window.UX = {
    updateTimeline: (tl)=>window.dispatchEvent(new CustomEvent("ux:timeline",{ detail: tl || {} })),
    updateProgress: (payload)=>payload && window.dispatchEvent(new CustomEvent("ux:progress",{ detail: payload })),
    refresh: ()=>{ Rail._invalidate(); return pullSummary().then(renderAll); }
  };

  // SSE adapter
  window.openSummaryStream = function openSummaryStream(){
    try{
      const es = new EventSource("/api/run/summary/stream");
      es.onmessage = (ev)=>{ try{ summary = JSON.parse(ev.data||"{}") || summary; renderAll(); }catch{} };
      es.addEventListener("timeline", ev => { try{ UX.updateTimeline(JSON.parse(ev.data||"{}")); }catch{} });
      es.addEventListener("progress", ev => { try{ UX.updateProgress(JSON.parse(ev.data||"{}")); }catch{} });
      es.onerror = ()=>{ try{es.close();}catch{}; setTimeout(openSummaryStream, 3000); };
    }catch{}
  };

  // boot
  pullPairs(); renderAll(); wireRunButton(); tick();
})();
