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
  document.head.appendChild(Object.assign(document.createElement("style"),{
    id:"lanes-css",
    textContent:`
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
  .tag{font-size:10px;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);opacity:.85;white-space:nowrap;flex:0 0 auto;display:inline-flex;align-items:center;gap:4px}
  .t-add{color:#7cffc4;border-color:rgba(124,255,196,.25)}
  .t-rem{color:#ff9aa2;border-color:rgba(255,154,162,.25)}
  .t-upd{color:#9ecbff;border-color:rgba(158,203,255,.25)}
  .muted{opacity:.7}
  .small{font-size:11px}
  #run[disabled]{pointer-events:none;opacity:.6;filter:saturate(.7);cursor:not-allowed}
  #run.glass{position:relative}
  #run.glass::after{content:"";position:absolute;inset:6px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .9s linear infinite}
  @keyframes spin{to{transform:rotate(360deg)}}
  .chip.more{cursor:pointer;border-color:rgba(255,255,255,.22);font-size:10px;padding:1px 6px;line-height:1.2;width:auto}
  .chip.more:hover{background:rgba(255,255,255,.06)}
  .ux-spots-modal{position:fixed;inset:0;z-index:9999}
  .ux-spots-modal.hidden{display:none}
  .ux-spots-backdrop{position:absolute;inset:0;background:rgba(0,0,0,.6);backdrop-filter:blur(2px)}
  .ux-title{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .ux-date{margin-left:auto;font-size:10px;opacity:.7;white-space:nowrap}
  .ux-spots-card{position:relative;max-width:640px;margin:6vh auto;background:rgba(12,12,14,.88);border:1px solid rgba(255,255,255,.14);border-radius:16px;padding:10px 12px;box-shadow:0 0 0 1px rgba(255,255,255,.06),0 0 22px rgba(120,80,255,.18);backdrop-filter:blur(6px)}
  .ux-spots-h{display:flex;align-items:center;gap:8px;margin-bottom:6px}
  .ux-spots-title{font-weight:700;font-size:13px;letter-spacing:.2px}
  .ux-spots-close{margin-left:auto;border:none;background:transparent;color:inherit;font-size:18px;cursor:pointer;opacity:.9}
  .ux-spots-close:hover{opacity:1;filter:drop-shadow(0 0 6px currentColor)}
  .ux-spots-body{display:grid;grid-template-columns:1fr 1fr;gap:8px;max-height:55vh;overflow:auto;padding-right:4px}
  .ux-spots-body.single{grid-template-columns:1fr}
  .ux-col{display:flex;flex-direction:column;gap:4px}
  .ux-col-full{width:100%;display:block}
  .ux-sec-row{font-size:11px;display:flex;gap:8px;align-items:baseline;padding:2px 3px;border-radius:8px;background:rgba(255,255,255,.02)}
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
  const titleOf=x=>typeof x==="string"?x:(x?.title||x?.series_title||x?.name||((x?.type==="episode"&&x?.series_title&&Number.isInteger(x?.season)&&Number.isInteger(x?.episode))?`${x.series_title} S${String(x.season).padStart(2,"0")}E${String(x.episode).padStart(2,"0")}`:x?.key)||"item");
  const synthSpots = (items, key) => {
    const arr = Array.isArray(items) ? [...items] : [];

    const tsOf = (it) => {
      const v = it?.ts ?? it?.seen_ts ?? it?.sync_ts ?? it?.ingested_ts ?? it?.watched_at ?? it?.rated_at ?? 0;
      const n = (typeof v === "number") ? v : Date.parse(v);
      return Number.isFinite(n) ? n : 0;
    };

    arr.sort((x, y) => tsOf(y) - tsOf(x)); // newest first

    const a=[], r=[], u=[];
    for (const it of arr) {
      const t = titleOf(it);
      const act = (it?.action||it?.op||it?.change||"").toLowerCase();
      let tag="upd";

      if(key==="history"&&(it?.watched||it?.watched_at||act.includes("watch")||act.includes("scrobble"))) tag="add";
      else if(key==="ratings"&&(act.includes("rate")||("rating" in (it||{})))) tag="add";
      else if(key==="playlists"&&(act.includes("add")||act.includes("playlist"))) tag="add";
      else if(act.includes("add")) tag="add";
      else if(act.includes("rem")||act.includes("del")||act.includes("unwatch")) tag="rem";

      if(tag==="add" && a.length<3) a.push(t);
      else if(tag==="rem" && r.length<3) r.push(t);
      else if(u.length<3) u.push(t);

      if(a.length+r.length+u.length>=3) break;
    }
    return {a,r,u};
  };

  const defaultEnabledMap=()=>({watchlist:true,ratings:true,history:true,playlists:true});
  const getEnabledMap=()=>enabledFromPairs ?? (summary?.enabled||defaultEnabledMap());

  const guardLaneOverwrite = (key, payload, ts) => {
    const sum = (+payload.added||0)+(+payload.removed||0)+(+payload.updated||0);
    const prev = hydratedLanes[key];
    const prevSum = (prev? ((+prev.added||0)+(+prev.removed||0)+(+prev.updated||0)) : 0);
    if (sync.isRunning() && sum===0 && prevSum>0) return false;
    if ((ts||0) < (lastLaneTs[key]||0)) return false;
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

    if((added||removed||updated)===0 && hydratedLanes[key] && sync.isRunning())
      return {...hydratedLanes[key]};

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

  // Spotlight "more" modal (shows last 25 per bucket)
  let _spotsModal=null;
  const esc = (s)=>String(s??"").replace(/[&<>"']/g,c=>({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));

  const ensureSpotsModal = () => {
    if (_spotsModal) return _spotsModal;
    const m=document.createElement("div");
    m.id="ux-spots-modal";
    m.className="ux-spots-modal hidden";
    m.innerHTML=`
      <div class="ux-spots-backdrop"></div>
      <div class="ux-spots-card">
        <div class="ux-spots-h">
          <div class="ux-spots-title"></div>
          <button class="ux-spots-close" aria-label="Close">✕</button>
        </div>
        <div class="ux-spots-body"></div>
      </div>`;
    document.body.appendChild(m);

    m.querySelector(".ux-spots-close").onclick=()=>closeSpotsModal();
    m.querySelector(".ux-spots-backdrop").onclick=()=>closeSpotsModal();
    document.addEventListener("keydown",(e)=>{ if(e.key==="Escape") closeSpotsModal(); });

    _spotsModal=m;
    return m;
  };

  const closeSpotsModal = () => {
    if (_spotsModal) _spotsModal.classList.add("hidden");
  };

  const openSpotsModal = (key, label, buckets) => {
    const m = ensureSpotsModal();
    m.querySelector(".ux-spots-title").textContent = `${label} — last 25`;
    const body = m.querySelector(".ux-spots-body");

    const tsOf = (it) => {
      if (!it || typeof it === "string") return 0;
      const v =
        it.added_at ?? it.listed_at ?? it.watched_at ?? it.rated_at ??
        it.last_watched_at ?? it.user_rated_at ??
        it.ts ?? it.seen_ts ?? it.ingested_ts ?? it.sync_ts ?? 0;

      let n = (typeof v === "number") ? v : Date.parse(v);
      if (!Number.isFinite(n)) return 0;
      if (n < 1e12) n *= 1000; // seconds -> ms
      return n;
    };

    const fmtDate = (ts) => ts ? new Date(ts).toLocaleDateString() : "";
    const tagMeta = (kind) => {
      if (kind === "rem")
        return { cls: "t-rem", text: "Removed", icon: "mdi mdi-delete-outline" };
      if (kind === "upd")
        return { cls: "t-upd", text: "Updated", icon: "mdi mdi-sync" };
      return { cls: "t-add", text: "Added", icon: "mdi mdi-plus" };
    };

    const markKind = (arr, kind) =>
      (arr || []).map(it => (typeof it === "object" && it !== null)
        ? Object.assign({}, it, { __kind: kind })
        : { title: it, __kind: kind });

    const all = [
      ...markKind(buckets.add || [], "add"),
      ...markKind(buckets.rem || [], "rem"),
      ...markKind(buckets.upd || [], "upd"),
    ];
    all.sort((a, b) => tsOf(b) - tsOf(a));
    const last25 = all.slice(0, 25);

    const hasRem    = (buckets.rem || []).length > 0;
    const leftItems = last25.filter(it => it.__kind !== "rem");
    const rightItems = hasRem ? last25.filter(it => it.__kind === "rem") : [];

    const mkCol = (items) => {
      if (!items.length) return `<div class="muted small">No items.</div>`;
      return items.map(it => {
        const t = esc(titleOf(it));
        const d = fmtDate(tsOf(it));
        const { cls, text, icon } = tagMeta(it.__kind);
        return `
          <div class="ux-sec-row">
            <span class="tag ${cls}">
              <i class="${icon}"></i> ${text}
            </span>
            <span class="ux-title">${t}</span>
            ${d ? `<span class="ux-date">${d}</span>` : ``}
          </div>`;
      }).join("");
    };

    body.classList.toggle("single", !hasRem);
    body.innerHTML = hasRem
      ? `
        <div class="ux-col ux-col-add">${mkCol(leftItems)}</div>
        <div class="ux-col ux-col-rem">${mkCol(rightItems)}</div>`
      : `
        <div class="ux-col ux-col-full">${mkCol(leftItems)}</div>`;

    m.classList.remove("hidden");
  };

  // Renderers
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

      // Cap at 25 overall
      const rawTotal     = (spotAdd?.length || 0) + (spotRem?.length || 0) + (spotUpd?.length || 0);
      const logicalTotal = rawTotal || (items?.length || 0) || spots.length;
      const cappedTotal  = Math.min(25, logicalTotal);

      const shownSpots = Math.min(3, spots.length);
      const moreCount  = Math.max(0, cappedTotal - shownSpots);

      if(!isEnabled){
        body.appendChild(Object.assign(document.createElement("div"),{className:"spot muted small",textContent:"Feature not configured"}));
      }else if(!spots.length){
        body.appendChild(Object.assign(document.createElement("div"),{className:"spot muted small",textContent:sync.state().timeline.done?"No changes":"Awaiting results…"}));
      }else{
        let lastRow=null;

        for(const s of spots.slice(0,3)){
          const row=document.createElement("div"); row.className="spot";
          const tag=document.createElement("span");
          tag.className="tag "+(s.t==="add"?"t-add":s.t==="rem"?"t-rem":"t-upd");
          tag.textContent=s.t==="add"?"Added":s.t==="rem"?"Removed":"Updated";
          row.append(tag,Object.assign(document.createElement("span"),{textContent:s.text}));
          body.appendChild(row);
          lastRow=row;
        }

        if(moreCount>0 && lastRow){
          const moreChip=document.createElement("span");
          moreChip.className="chip more";
          moreChip.textContent = `+${moreCount} more`;
          moreChip.title="Show recent items";
          moreChip.style.marginLeft="auto"; // keep on same line, right-aligned
          moreChip.addEventListener("click",(ev)=>{
            ev.stopPropagation();
            openSpotsModal(f.key,f.label,{add:spotAdd,rem:spotRem,upd:spotUpd});
          });
          lastRow.appendChild(moreChip);
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

  // Insights hydration
  let _insightsTried = false;
  async function hydrateFromInsights(startTsEpoch) {
    const src = await fetchFirstJSON(["/api/insights"], null);
    const events = src?.events;
    if (!Array.isArray(events) || !events.length) return false;

    const since = Math.floor(startTsEpoch || 0);

    const tallies = { watchlist: mk(), ratings: mk(), history: mk(), playlists: mk() };
    function mk() {
      return { added: 0, removed: 0, updated: 0, spotAdd: [], spotRem: [], spotUpd: [] };
    }

    const mapFeature = (e) => {
      const f = String(e.feature || e.lane || e.kind || "").toLowerCase();
      if (f) return f;
      const act = String(e.action || "").toLowerCase();
      if (act.includes("watch") || act.includes("scrobble")) return "history";
      if (act.includes("rate") || "rating" in (e || {})) return "ratings";
      if (act.includes("playlist")) return "playlists";
      return "watchlist";
    };

    for (const e of events) {
      if ((e.ts || 0) < since) continue;
      const k = mapFeature(e);
      const L = tallies[k] || (tallies[k] = mk());
      const title = titleOf(e);
      const act = String(e.action || "").toLowerCase();

      if (act === "add") {
        L.added++;
        if (L.spotAdd.length < 25) L.spotAdd.push({ title });
      } else if (act === "remove") {
        L.removed++;
        if (L.spotRem.length < 25) L.spotRem.push({ title });
      } else {
        L.updated++;
        if (L.spotUpd.length < 25) L.spotUpd.push({ title });
      }
    }

    summary ||= {};
    summary.features ||= {};

    const nowTs = Date.now();

    for (const [feat, L] of Object.entries(tallies)) {
      if (
        (L.added + L.removed + L.updated) === 0 &&
        !L.spotAdd.length && !L.spotRem.length && !L.spotUpd.length
      ) continue;

      const prev = summary.features[feat] || {};
      const prevAdded   = (+prev.added   || 0);
      const prevRemoved = (+prev.removed || 0);
      const prevUpdated = (+prev.updated || 0);
      const hasPrevCounts = (prevAdded + prevRemoved + prevUpdated) > 0;

      const merged = {
        // If summary already has counts for this lane, keep them.
        added:   hasPrevCounts ? prevAdded   : (L.added   || 0),
        removed: hasPrevCounts ? prevRemoved : (L.removed || 0),
        updated: hasPrevCounts ? prevUpdated : (L.updated || 0),

        spotlight_add:    (prev.spotlight_add?.length    ? prev.spotlight_add    : L.spotAdd)  || [],
        spotlight_remove: (prev.spotlight_remove?.length ? prev.spotlight_remove : L.spotRem) || [],
        spotlight_update: (prev.spotlight_update?.length ? prev.spotlight_update : L.spotUpd) || [],
      };

      if (guardLaneOverwrite(feat, merged, nowTs)) {
        summary.features[feat] = merged;
        hydratedLanes[feat] = {
          added: merged.added,
          removed: merged.removed,
          updated: merged.updated,
          spotAdd: merged.spotlight_add,
          spotRem: merged.spotlight_remove,
          spotUpd: merged.spotlight_update,
        };
      }
    }

    summary.enabled = Object.assign(defaultEnabledMap(), summary.enabled || {});
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
        if(act==="add"&&L.spotAdd.length<25) L.spotAdd.push(obj.title);
        if(act==="remove"&&L.spotRem.length<25) L.spotRem.push(obj.title);
        if(act==="update"&&L.spotUpd.length<25) L.spotUpd.push(obj.title);
        continue;
      }
    }

    if(!Object.keys(tallies).length) return false;

    summary ||= {};
    summary.features ||= {};

    for(const [feat,lane] of Object.entries(tallies)){
      const prev=summary.features[feat]||{};

      const saPrev = prev.spotlight_add?.length;
      const srPrev = prev.spotlight_remove?.length;
      const suPrev = prev.spotlight_update?.length;

      const sa = saPrev ? prev.spotlight_add : lane.spotAdd;
      const sr = srPrev ? prev.spotlight_remove : lane.spotRem;
      const su = suPrev ? prev.spotlight_update : lane.spotUpd;

      const merged = {
        added:   Math.max(prev.added   || 0, lane.added   || 0),
        removed: Math.max(prev.removed || 0, lane.removed || 0),
        updated: Math.max(prev.updated || 0, lane.updated || 0),
        spotlight_add:    saPrev ? sa.slice(0,25) : sa.slice(-25).reverse(),
        spotlight_remove: srPrev ? sr.slice(0,25) : sr.slice(-25).reverse(),
        spotlight_update: suPrev ? su.slice(0,25) : su.slice(-25).reverse()
      };

      if(guardLaneOverwrite(feat,merged,Date.now())){
        summary.features[feat]=merged;
        hydratedLanes[feat]={
          added:merged.added,removed:merged.removed,updated:merged.updated,items:[],
          spotAdd:merged.spotlight_add||[],spotRem:merged.spotlight_remove||[],spotUpd:merged.spotlight_update||[]
        };
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
       try { sync._optimistic = false; } catch {}

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

        if (incoming && incoming.exit_code != null) {
          const code = Number(incoming.exit_code);
          if (code === 0 && typeof sync?.success === "function") sync.success();
          else if (typeof sync?.fail === "function") sync.fail(code);
          else if (code === 0) sync.done(); else sync.error();
          setRunButtonState(false);
          return;
        }

        if (incoming && incoming.running === false) {
          if (!sync.state().timeline.start) {
            sync.reset(); setRunButtonState(false); renderAll();
          } else {
            sync.done(); setRunButtonState(false); // stays at 75
          }
        }

      } catch{} };

      const markInit = () => { try { sync.markInit(); } catch {} };
      ["run:start","run:pair","feature:start"].forEach(n => esSummary.addEventListener(n, markInit));

      const onSnap = (ev)=>{ try{ sync.snap(JSON.parse(ev.data||"{}")); }catch{} };
      esSummary.addEventListener("progress:snapshot", onSnap);
      esSummary.addEventListener("snapshot:progress", onSnap);

      const onApplyProg = (ev) => { try {
        const d = JSON.parse(ev.data || "{}");
        sync.applyProg(d);
      } catch {} };

      esSummary.addEventListener("progress:apply", onApplyProg);
      esSummary.addEventListener("apply:add:progress", onApplyProg);
      esSummary.addEventListener("apply:remove:progress", onApplyProg);

      ["apply:add:start","apply:remove:start"].forEach(name=> esSummary.addEventListener(name, ev=>{
        try{ sync.applyStart(JSON.parse(ev.data||"{}")); }catch{}
      }));
      ["apply:add:done","apply:remove:done"].forEach(name=> esSummary.addEventListener(name, ev=>{
        try{ sync.applyDone(JSON.parse(ev.data||"{}")); }catch{}
      }));

      // esSummary.addEventListener("run:done", ()=>{ try{ setRunButtonState(false); }catch{} });
      ["run:error","run:aborted"].forEach(name=> esSummary.addEventListener(name, ()=>{ try{ sync.error(); setRunButtonState(false); }catch{} }));

      esSummary.onerror = () => {
        try{ esSummary.close(); }catch{}
        window.esSum = null;
        setTimeout(openSummaryStream, 2000);
      };
    }catch{}
  };

  window.openLogStream = function openLogStream() {
    try {
      try { esLogs?.close?.(); } catch {}
      esLogs = new EventSource("/api/logs/stream");
      window.esLogs = esLogs;
      esLogs.onopen = () => { window.esLogs = esLogs; };
      esLogs.onmessage = (ev) => {
        try {
          const txt = String(ev.data || "");
          const m = txt.match(/\[SYNC\]\s*exit\s*code\s*:\s*(\d+)/i);
          if (!m) return;
          if (!sync?.isRunning?.() && !sync?.state?.().timeline.start) return;

          const code = parseInt(m[1], 10);
          if (code === 0 && typeof sync?.success === "function") sync.success();
          else if (typeof sync?.fail === "function") sync.fail(code);
          else if (code === 0) sync.done(); else sync.error();

          setRunButtonState(false);
        } catch {}
      };
      esLogs.onerror = () => {
        try { esLogs.close(); } catch {}
        window.esLogs = null;
        setTimeout(openLogStream, 2000);
      };
    } catch {}
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
