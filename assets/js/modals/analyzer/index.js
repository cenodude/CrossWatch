// assets/js/modals/analyzer/index.js
const fjson = async (u,o)=>{const r=await fetch(u,o); if(!r.ok) throw new Error(r.status); return r.json();};
const Q=(s,r=document)=>r.querySelector(s); const QA=(s,r=document)=>Array.from(r.querySelectorAll(s));
const esc=s=> (window.CSS?.escape?CSS.escape(s):String(s).replace(/[^\w-]/g,"\\$&"));
const tagOf=(p,f,k)=>`${p}::${f}::${k}`;
const chips=(ids)=>Object.entries(ids||{}).map(([k,v])=>`<span class="chip mono">${k}:${v}</span>`).join("");
const fmtCounts=(c)=>Object.entries(c||{}).map(([p,v])=>`${p}: ${v.total} (H:${v.history}/W:${v.watchlist}/R:${v.ratings||0})`).join(" • ");
const FIXABLE = new Set(["missing_peer","missing_ids","key_missing_ids","key_ids_mismatch","invalid_id_format"]);

function css(){
  if (Q("#an-css")) return;
  const el=document.createElement("style"); el.id="an-css"; el.textContent = `
  .modal-root{position:relative;display:flex;flex-direction:column;height:100%}
  .cx-head{display:flex;align-items:center;gap:10px;justify-content:space-between;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.12)}
  .cx-left{display:flex;align-items:center;gap:12px;flex:1}
  .cx-title{font-weight:800}
  .an-actions{display:flex;gap:8px;align-items:center}
  .pill{border:1px solid rgba(255,255,255,.12);background:#0e1320;color:#dbe8ff;border-radius:16px;padding:6px 12px;font-weight:700}
  .pill.ghost{background:#0b0f19;color:#c8d3ff}
  #an-toggle-ids{white-space:nowrap;min-width:140px;padding:6px 16px}
  .badge{padding:3px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:#0b0f19}
  .close-btn{border:1px solid rgba(255,255,255,.2);background:#171b2a;color:#fff;border-radius:10px;padding:6px 10px}
  .an-wrap{flex:1;min-height:0;display:grid;grid-template-rows:minmax(220px,1fr) 8px minmax(180px,1fr);overflow:hidden}
  .an-split{height:8px;background:linear-gradient(90deg,#27214b,#2b5cff);box-shadow:0 0 10px #6a5cff88 inset;cursor:row-resize}
  .an-grid{overflow:auto;border-bottom:1px solid rgba(255,255,255,.08)}
  .an-issues{overflow:auto;padding:8px}
  .row{display:grid;gap:8px;padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.06);align-items:center}
  .head{position:sticky;top:0;background:#0b0f19;z-index:2}
  .row.sel{outline:1px solid #6aa3ff}
  .chip{display:inline-block;border:1px solid rgba(255,255,255,.16);border-radius:999px;padding:2px 6px;margin:2px}
  .mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
  .ids{opacity:.9}
  .an-grid.show-ids .ids{display:block}
  .an-grid .ids{display:none}
  .issue{padding:10px;border-bottom:1px solid rgba(255,255,255,.06)}
  .issue .h{font-weight:800;margin-bottom:6px}
  .col-head{position:relative;user-select:none}
  .resizer{position:absolute;right:-4px;top:0;width:8px;height:100%;cursor:col-resize}
  .an-footer{position:sticky;bottom:0;display:flex;justify-content:center;padding:8px 0;border-top:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(11,15,25,.9),rgba(11,15,25,.98))}
  .an-footer .stats{font-weight:700;color:#dbe8ff}
  input[type=search]{background:#0b0f19;border:1px solid rgba(255,255,255,.12);color:#dbe8ff;border-radius:12px;padding:6px 10px}
  #an-search{flex:1 1 720px;min-width:420px;width:auto}
  /* Neon scrollbars */
  .an-grid, .an-issues{scrollbar-width:thin; scrollbar-color:#7a6bff #0b0f19;}
  .an-grid::-webkit-scrollbar, .an-issues::-webkit-scrollbar{height:12px;width:12px}
  .an-grid::-webkit-scrollbar-track, .an-issues::-webkit-scrollbar-track{background:#0b0f19}
  .an-grid::-webkit-scrollbar-thumb, .an-issues::-webkit-scrollbar-thumb{
    background:linear-gradient(180deg,#7a6bff,#23a8ff);
    border-radius:10px; border:2px solid #0b0f19; box-shadow:0 0 12px #7a6bff88 inset;
  }
  `; document.head.appendChild(el);
}

function gridTemplateFrom(widths){ return widths.map(w=>`${w}px`).join(" "); }
function fixHint(provider, feature){
  const where = (p)=> p==="PLEX" ? "Plex" : (p==="JELLYFIN" ? "Jellyfin" : "your media server");
  if (provider==="SIMKL" || provider==="TRAKT"){
    return `Don’t edit on ${provider}. Fix the match in ${where("PLEX")} (use “Match” / correct IDs), then run a sync.`;
  }
  return `Open ${where(provider)} → item → “Match” (or edit metadata) → ensure IMDb/TMDB/TVDB IDs and year/title are correct. Then sync to SIMKL.`;
}

export default {
  async mount(root){
    css();
    root.classList.add("modal-root");
    root.innerHTML = `
      <div class="cx-head">
        <div class="cx-left">
          <div class="cx-title">Analyzer</div>
          <button class="pill ghost" id="an-toggle-ids">IDs: hidden</button>
          <input id="an-search" type="search" placeholder="title, year, provider, feature…">
        </div>
        <div class="an-actions">
          <button class="pill" id="an-run" type="button">Analyze</button>
          <button class="close-btn" id="an-close">Close</button>
        </div>
      </div>
      <div class="an-wrap" id="an-wrap">
        <div class="an-grid" id="an-grid"></div>
        <div class="an-split" id="an-split" title="drag to resize"></div>
        <div class="an-issues" id="an-issues"></div>
      </div>
      <div class="an-footer"><span class="mono" id="an-issues-count">Issues: 0</span>&nbsp;•&nbsp;<span class="stats mono" id="an-stats">—</span></div>
    `;

    const wrap=Q("#an-wrap",root), grid=Q("#an-grid",root), issues=Q("#an-issues",root);
    const stats=Q("#an-stats",root), issuesCount=Q("#an-issues-count",root), search=Q("#an-search",root);
    const btnRun=Q("#an-run",root), btnToggleIDs=Q("#an-toggle-ids",root), btnClose=Q("#an-close",root);
    const split=Q("#an-split",root);

    let COLS = (JSON.parse(localStorage.getItem("an.cols")||"null")) || [110, 110, 430, 80, 100];
    let ITEMS=[], VIEW=[], SORT_KEY="title", SORT_DIR="asc", SHOW_IDS=false;
    let PROB={all:[],fix:[]};
    let SELECTED=null;

    function applySplit(top,total){const bar=8,min=140,clamped=Math.max(min,Math.min(total-bar-min,top)); wrap.style.gridTemplateRows=`${clamped}px 8px 1fr`; localStorage.setItem("an.split.r",(clamped/total).toFixed(4));}
    function restoreSplit(){const r=parseFloat(localStorage.getItem("an.split.r")||"0.62");const tot=wrap.getBoundingClientRect().height;applySplit(Math.round(r*tot),tot);}
    function dragY(){const rect=wrap.getBoundingClientRect();const tot=rect.height;const mv=e=>{const y=(e.touches?e.touches[0].clientY:e.clientY);applySplit((y-rect.top),tot)}; const up=()=>{window.removeEventListener("mousemove",mv);window.removeEventListener("mouseup",up);window.removeEventListener("touchmove",mv);window.removeEventListener("touchend",up)}; window.addEventListener("mousemove",mv);window.addEventListener("mouseup",up);window.addEventListener("touchmove",mv,{passive:false});window.addEventListener("touchend",up);}
    split.addEventListener("mousedown",dragY); split.addEventListener("touchstart",(e)=>{dragY();e.preventDefault()},{passive:false});

    function setCols(){
      grid.style.setProperty("--col-template", gridTemplateFrom(COLS));
      grid.querySelectorAll(".row").forEach(r=> r.style.gridTemplateColumns = gridTemplateFrom(COLS));
    }

    const val=(r,k)=>k==="provider"?r.provider||"":k==="feature"?r.feature||"":k==="title"?(r.title||"").toLowerCase():k==="year"?(+r.year||0):k==="type"?r.type||"":(r.title||"").toLowerCase();
    const sortRows=(r)=>r.sort((a,b)=>{const d=SORT_DIR==="asc"?1:-1,va=val(a,SORT_KEY),vb=val(b,SORT_KEY);return va<vb?-1*d:va>vb?1*d:0});

    function renderHeader(){
      const res = (i)=>`<div class="resizer" data-i="${i}"></div>`;
      const head = `
      <div class="row head" style="grid-template-columns:${gridTemplateFrom(COLS)}">
        <div class="col-head sort" data-k="provider"><b>Provider</b>${res(0)}</div>
        <div class="col-head sort" data-k="feature"><b>Feature</b>${res(1)}</div>
        <div class="col-head sort" data-k="title"><b>Title</b>${res(2)}</div>
        <div class="col-head sort" data-k="year"><b>Year</b>${res(3)}</div>
        <div class="col-head sort" data-k="type"><b>Type</b>${res(4)}</div>
      </div>`;
      return head;
    }

    function renderBody(rows){
      return rows.map(r=>`
        <div class="row ${SELECTED===tagOf(r.provider,r.feature,r.key)?'sel':''}" data-tag="${esc(tagOf(r.provider,r.feature,r.key))}" style="grid-template-columns:${gridTemplateFrom(COLS)}">
          <div>${r.provider}</div>
          <div>${r.feature}</div>
          <div><div>${r.title||'Untitled'}</div><div class="ids mono">${chips(r.ids)}</div></div>
          <div>${r.year||''}</div>
          <div>${r.type||''}</div>
        </div>`).join("");
    }

    function bindHeader(){
      QA(".head .sort", grid).forEach(h=> h.addEventListener("click", () => {
        const k=h.dataset.k; SORT_DIR = (SORT_KEY===k && SORT_DIR==="asc")?"desc":"asc"; SORT_KEY=k; draw();
      }));
      QA(".resizer", grid).forEach(el=>{
        let i=+el.dataset.i, startX=0, startW=0;
        const down=(e)=>{startX=(e.touches?e.touches[0].clientX:e.clientX); startW=COLS[i]; document.addEventListener("mousemove",move); document.addEventListener("mouseup",up); document.addEventListener("touchmove",move,{passive:false}); document.addEventListener("touchend",up); e.stopPropagation(); e.preventDefault();}
        const move=(e)=>{const x=(e.touches?e.touches[0].clientX:e.clientX); const dx=x-startX; COLS[i]=Math.max(70,startW+dx); setCols();}
        const up=()=>{document.removeEventListener("mousemove",move); document.removeEventListener("mouseup",up); document.removeEventListener("touchmove",move); document.removeEventListener("touchend",up); localStorage.setItem("an.cols",JSON.stringify(COLS));}
        el.addEventListener("mousedown",down); el.addEventListener("touchstart",down,{passive:false});
      });
    }

    function draw(){
      grid.innerHTML = renderHeader() + renderBody(sortRows(VIEW.slice()));
      bindHeader(); setCols();
      QA(".row:not(.head)",grid).forEach(r=> r.addEventListener("click", ()=> select(r.getAttribute("data-tag")) ));
    }

    function filter(q){
      q=(q||"").toLowerCase().trim();
      if(!q){ VIEW=ITEMS.slice(); draw(); return; }
      const W=q.split(/\s+/g);
      VIEW=ITEMS.filter(r=>{
        const hay=[r.provider,r.feature,r.title,String(r.year||""),r.type].join(" ").toLowerCase();
        return W.every(w=>hay.includes(w));
      });
      draw();
    }

    function closeModal(){
      window.cxCloseModal?.();
    }

    function suggestionCard(it, s){
      const hdr = `Suggestion for: <b>${it.title||"Untitled"}</b>${it.year?` <span class="mono">(${it.year})</span>`:""}`;
      const meta = `${s.reason} ${(s.confidence?`<span class="mono">(${(s.confidence*100|0)}%)</span>`:"")}`;
      return `
        <div class="issue">
          <div class="h">${hdr}</div>
          <div>${meta}</div>
          <div class="mono ids" style="margin-top:6px">${chips(s.ids)}</div>
        </div>`;
    }

    async function select(tag){
      SELECTED=tag; draw();
      const [provider,feature,key]=tag.split("::");
      const it = ITEMS.find(r=> tagOf(r.provider,r.feature,r.key)===tag);
      if (!it) { issues.innerHTML = "<div class='issue'><div class='h'>No selection</div></div>"; return; }
      const meta = await fjson("/api/analyzer/suggest",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({provider,feature,key})}).catch(()=>({suggestions:[]}));
      const sugs = meta.suggestions||[];
      const guidance = `<div class="issue">
        <div class="h">${it.title||"Untitled"} ${it.year?`(${it.year})`:""} • <span class="mono">${provider}/${feature}</span></div>
        <div>${fixHint(provider, feature)}</div>
      </div>`;
      const lines = sugs.slice(0,5).map(s=>suggestionCard(it,s)).join("") || `<div class="issue"><div class="h">No suggestions</div><div class="mono">Match it manually in Plex/Jellyfin and sync.</div></div>`;
      issues.innerHTML = guidance + lines;
      issues.scrollTop = 0;
    }

    // Fetch active pairs and build a map of allowed provider-feature to target providers
    async function getActivePairMap() {
      try {
        const arr = await fjson("/api/pairs", { cache: "no-store" });
        const map = new Map();
        const on = (feat) => feat && (typeof feat.enable === "boolean" ? feat.enable : !!feat);
        const add = (src, feat, dst) => {
          const k = `${String(src||"").toUpperCase()}::${feat}`;
          if (!map.has(k)) map.set(k, new Set());
          map.get(k).add(String(dst||"").toUpperCase());
        };
        for (const p of (arr || [])) {
          if (!p?.enabled) continue;
          const src = (p.source || "").toUpperCase();
          const dst = (p.target || "").toUpperCase();
          const mode = String(p.mode || "one-way").toLowerCase();
          const F = p.features || {};
          for (const feat of ["history","watchlist","ratings"]) {
            if (!on(F[feat])) continue;
            add(src, feat, dst);
            if (["two-way","bi","both","mirror"].includes(mode)) add(dst, feat, src);
          }
        }
        return map;
      } catch { return new Map(); }
    }

    async function load(){
      restoreSplit();
      const s = await fjson("/api/analyzer/state");
      ITEMS = s.items || []; VIEW = ITEMS.slice();
      stats.textContent = fmtCounts(s.counts) || "—";
      issuesCount.textContent = 'Issues: 0';
      draw();
      await analyze();
    }

    async function analyze(){
      const [pairMap, meta] = await Promise.all([
        getActivePairMap(),
        fjson("/api/analyzer/problems").catch(()=>({problems:[]})),
      ]);
      const all = meta.problems || [];
      const seen = new Set();
      const per = { history:0, watchlist:0, ratings:0 };
      const keep = [];

      for (const p of all) {
        if (p.type !== "missing_peer") continue;
        const key = `${String(p.provider||"").toUpperCase()}::${String(p.feature||"").toLowerCase()}`;
        const allowed = pairMap.get(key);
        if (!allowed) continue;
        const tgts = (p.targets||[]).map(t => String(t||"").toUpperCase());
        if (!tgts.some(t => allowed.has(t))) continue;
        const sig = `${p.provider}::${p.feature}::${p.key}`;
        if (seen.has(sig)) continue;
        seen.add(sig);
        per[p.feature] = (per[p.feature]||0) + 1;
        keep.push(p);
      }

      PROB.all = all;
      PROB.fix = keep;

      const parts = [`Issues: ${keep.length}`];
      if (per.history)   parts.push(`H:${per.history}`);
      if (per.watchlist) parts.push(`W:${per.watchlist}`);
      if (per.ratings)   parts.push(`R:${per.ratings}`);
      issuesCount.textContent = parts.join(" • ");

      if (!keep.length){
        issues.innerHTML = `<div class="issue"><div class="h">No issues detected</div><div>All good.</div></div>`;
        return;
      }
      const first = keep[0];
      const tag = tagOf(first.provider, first.feature, first.key);
      select(tag);
      SELECTED = tag; draw();
    }

    btnRun.addEventListener('click', async (e) => {
      e.preventDefault(); e.stopPropagation();
      if (btnRun.disabled) return;
      const prev = btnRun.textContent;
      btnRun.disabled = true; btnRun.textContent = 'Analyzing…';
      try { await analyze(); }
      finally { btnRun.disabled = false; btnRun.textContent = prev; }
    });

    btnToggleIDs.onclick = () => { SHOW_IDS = !SHOW_IDS; btnToggleIDs.textContent = `IDs: ${SHOW_IDS?'shown':'hidden'}`; grid.classList.toggle("show-ids", SHOW_IDS); };
    search.addEventListener("input", (e)=>filter(e.target.value));
    btnClose.addEventListener("click", closeModal);

    await load();
    },
    unmount(){ /* noop */ }
    };
