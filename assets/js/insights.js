/* Insights module: multi-feature stats (watchlist/ratings/history/playlists). */

(function (w, d) {
  // ────────────────────────────────────────────────────────────────────────────
  // Helpers & constants
  // ────────────────────────────────────────────────────────────────────────────
  const FEATS = ["watchlist","ratings","history","playlists"];
  const FEAT_LABEL = { watchlist:"Watchlist", ratings:"Ratings", history:"History", playlists:"Playlists" };
  const $  = (s,r)=> (r||d).querySelector(s);
  const $$ = (s,r)=> Array.from((r||d).querySelectorAll(s));
  const txt= (el,v)=> el && (el.textContent = v==null ? "—" : String(v));
  const clampFeature = n => FEATS.includes(n) ? n : "watchlist";
  let _feature = clampFeature(localStorage.getItem("insights.feature") || "watchlist");

  const fetchJSON=async(url,fallback=null)=>{
    try{
      const r=await fetch(url+(url.includes("?")?"&":"?")+"_ts="+Date.now(),{
        credentials:"same-origin",
        cache:"no-store"
      });
      return r.ok ? r.json() : fallback;
    }catch{ return fallback; }
  };
  const fetchFirstJSON=async(urls,fallback=null)=>{
    for(const u of urls){ const j=await fetchJSON(u,null); if(j) return j; }
    return fallback;
  };

  // Configured providers cache (used to gate provider tiles)
  const _lc = s=>String(s||"").toLowerCase();
  let _cfgSet = null, _cfgAt = 0; const CFG_TTL=60_000;
  async function getConfiguredProviders(force=false){
    if (!force && _cfgSet && (Date.now()-_cfgAt<CFG_TTL)) return _cfgSet;
    const cfg = await fetchJSON(`/api/config?no_secrets=1&t=${Date.now()}`) || {};
    const has = v => typeof v==="string" ? v.trim().length>0 : !!v;
    const S = new Set();
    if (has(cfg?.plex?.account_token)) S.add("plex");
    if (has(cfg?.trakt?.access_token)) S.add("trakt");
    if (has(cfg?.simkl?.access_token)) S.add("simkl");
    if (has(cfg?.jellyfin?.access_token)) S.add("jellyfin");
    _cfgSet=S; _cfgAt=Date.now(); return S;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Micro charts: sparkline + animated counters/bars
  // ────────────────────────────────────────────────────────────────────────────
  function renderSparkline(id, points) {
    const el = d.getElementById(id); if (!el) return;
    if (!points?.length) { el.innerHTML = '<div class="muted">No data</div>'; return; }
    const wv=el.clientWidth||260, hv=el.clientHeight||64, pad=4;
    const xs=points.map(p=>+p.ts||0), ys=points.map(p=>+p.count||0);
    const minX=Math.min(...xs), maxX=Math.max(...xs), minY=Math.min(...ys), maxY=Math.max(...ys);
    const X=t=> maxX===minX? pad : pad + ((wv-2*pad)*(t-minX))/(maxX-minX);
    const Y=v=> maxY===minY? hv/2: hv - pad - ((hv-2*pad)*(v-minY))/(maxY-minY);
    const dStr=points.map((p,i)=>(i?"L":"M")+X(p.ts)+","+Y(p.count)).join(" ");
    const dots=points.map(p=>`<circle class="dot" cx="${X(p.ts)}" cy="${Y(p.count)}"></circle>`).join("");
    el.innerHTML = `<svg viewBox="0 0 ${wv} ${hv}" preserveAspectRatio="none"><path class="line" d="${dStr}"></path>${dots}</svg>`;
  }

  const _ease = t => t<.5 ? 2*t*t : -1 + (4-2*t)*t;
  function animateNumber(el, to, duration=650) {
    if (!el) return;
    const from = parseInt(el.dataset?.v || el.textContent || "0",10)||0;
    if (from===to) { el.textContent=String(to); el.dataset.v=String(to); return; }
    const t0 = performance.now(), dur = Math.max(180,duration);
    const step = now => { const p=Math.min(1,(now-t0)/dur), v=Math.round(from+(to-from)*_ease(p));
      el.textContent=String(v); p<1? requestAnimationFrame(step) : (el.dataset.v=String(to)); };
    requestAnimationFrame(step);
  }
  function animateChart(now,week,month){
    const bars = { now:$('.bar.now'), week:$('.bar.week'), month:$('.bar.month') };
    const max = Math.max(1, now, week, month), h=v=> Math.max(.04, v/max);
    bars.week && (bars.week.style.transform = `scaleY(${h(week)})`);
    bars.month&& (bars.month.style.transform= `scaleY(${h(month)})`);
    bars.now  && (bars.now.style.transform  = `scaleY(${h(now)})`);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Footer host (provides switcher + provider tiles area)
  // ────────────────────────────────────────────────────────────────────────────
  const footWrap = (()=>{ let _padTimer=0;
    function ensureFooter(){
      let foot = d.getElementById("insights-footer");
      if (!foot) {
        foot = d.createElement("div"); foot.id="insights-footer"; foot.className="ins-footer";
        foot.innerHTML = '<div class="ins-foot-wrap"></div>'; (d.getElementById("stats-card")||d.body).appendChild(foot);
      }
      return foot.querySelector(".ins-foot-wrap")||foot;
    }
    function reserve(){ const card=$("#stats-card"), foot=$("#insights-footer"); if(!card||!foot) return;
      clearTimeout(_padTimer); _padTimer=setTimeout(()=>{ const h=(foot.getBoundingClientRect().height||foot.offsetHeight||120)+14; card.style.paddingBottom=h+"px"; },0);
    }
    w.addEventListener("resize", reserve, { passive:true });
    return Object.assign(ensureFooter, { reserve });
  })();

  // ────────────────────────────────────────────────────────────────────────────
  // Feature switcher (tabs for watchlist/ratings/history/playlists)
  // ────────────────────────────────────────────────────────────────────────────
  function ensureSwitch() {
    const wrap = footWrap();
    let host = d.getElementById("insights-switch");
    if (!host) {
      host = d.createElement("div");
      host.id = "insights-switch"; host.className = "ins-switch";
      host.innerHTML = '<div class="seg" role="tablist" aria-label="Insights features"></div>';
      wrap.appendChild(host);
    } else if (host.parentNode !== wrap) {
      wrap.appendChild(host);
    }
    const seg = host.querySelector(".seg");
    if (!host.dataset.init || !seg.querySelector(".seg-btn")) {
      seg.innerHTML = FEATS.map(f=>{
        const on = _feature===f;
        return `<button class="seg-btn${on?' active':''}" data-key="${f}" role="tab" aria-selected="${on}">${FEAT_LABEL[f]}</button>`;
      }).join("");
      seg.addEventListener("click", ev=>{
        const b = ev.target.closest(".seg-btn"); if (!b) return; switchFeature(b.dataset.key);
      });
      host.dataset.init="1";
    }
    placeSwitchBeforeTiles(); markActiveSwitcher(); footWrap.reserve();
    return host;
  }
  function placeSwitchBeforeTiles(){
    const wrap = footWrap(), sw=$("#insights-switch"), grid=$("#stat-providers"); if (!wrap||!sw) return;
    if (!wrap.contains(sw)) wrap.appendChild(sw);
    const ref = (grid && grid.parentNode === wrap) ? grid : null;
    if (sw.nextSibling !== ref) { try { wrap.insertBefore(sw, ref); } catch {} }
  }
  function markActiveSwitcher(){
    $$("#insights-switch .seg .seg-btn").forEach(b=>{
      const on=b.dataset.key===_feature; b.classList.toggle("active",on); b.setAttribute("aria-selected", on?"true":"false");
    });
  }
  function switchFeature(name){
    const want = clampFeature(name); if (want===_feature) return;
    _feature=want; localStorage.setItem("insights.feature", want); markActiveSwitcher(); refreshInsights(true);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Provider tiles (only show for configured providers)
  // ────────────────────────────────────────────────────────────────────────────
  function renderProviderStats(provTotals, provActive, configuredSet) {
    const wrap = footWrap();
    const host = d.getElementById("stat-providers") || (()=>{ const c=d.createElement("div"); c.id="stat-providers"; wrap.appendChild(c); return c; })();
    if (host.parentNode !== wrap) wrap.appendChild(host);

    const totals = provTotals || {};
    const active = Object.assign({}, provActive || {});
    const conf   = configuredSet || _cfgSet || new Set();
    let keys = Array.from(new Set([
      ...Object.keys(Object.assign({}, totals, active)),
      ...Array.from(conf)
    ])).filter(k=> conf.has(_lc(k))).sort();

    // If nothing is configured, hide the grid.
    if (!keys.length) {
      host.hidden = true;
      footWrap.reserve();
      return;
    }

    host.hidden = false;
    host.style.setProperty("--prov-cols", Math.max(1, Math.min(keys.length, 4)));

    const seen = new Set();
    keys.forEach(k=>{
      const id=`tile-${k}`, valId=`stat-${k}`;
      let tile=d.getElementById(id);
      if (!tile) {
        tile=d.createElement("div"); tile.id=id; tile.dataset.provider=_lc(k); tile.className="tile provider";
        tile.innerHTML=`<div class="n" id="${valId}" data-v="0">0</div>`; host.appendChild(tile);
      } else if (tile.parentNode !== host) host.appendChild(tile);

      let valEl=d.getElementById(valId);
      if (!valEl) { valEl=d.createElement("div"); valEl.className="n"; valEl.id=valId; valEl.dataset.v="0"; valEl.textContent="0"; tile.appendChild(valEl); }
      animateNumber(valEl, (+totals[k]||0), 650);
      tile.classList.toggle("inactive", !active[k]);
      seen.add(id);
    });

    Array.from(host.querySelectorAll(".tile")).forEach(t=>{ if(!seen.has(t.id)) t.remove(); });
    placeSwitchBeforeTiles(); footWrap.reserve();
  }

  // ────────────────────────────────────────────────────────────────────────────
  // History tabs (recent runs per feature)
  // ────────────────────────────────────────────────────────────────────────────
  function renderHistoryTabs(hist){
    const LIMIT_HISTORY = +(localStorage.getItem("insights.history.limit") || 4);
    const wrap = $("#sync-history") || $("[data-role='sync-history']") || $(".sync-history");
    if (!wrap) return;

    // One-time scaffold
    if (!wrap.dataset.tabsInit){
      wrap.innerHTML =
        '<div class="sync-tabs" role="tablist" aria-label="Recent syncs">' +
          FEATS.map((f,i)=>`<button class="tab ${i?'':'active'}" data-tab="${f}" role="tab" aria-selected="${i?'false':'true'}">${FEAT_LABEL[f]}</button>`).join("") +
        '</div><div class="sync-tabpanes">' +
          FEATS.map((f,i)=>`<div class="pane ${i?'':'active'}" data-pane="${f}" role="tabpanel"${i?' hidden':''}><div class="list"></div></div>`).join("") +
        '</div>';
      wrap.dataset.tabsInit = "1";
      wrap.addEventListener("click", ev => {
        const btn = ev.target.closest(".tab"); if (!btn) return;
        const name = btn.dataset.tab;
        $$(".sync-tabs .tab", wrap).forEach(b => {
          const on = b.dataset.tab === name;
          b.classList.toggle("active", on);
          b.setAttribute("aria-selected", on ? "true" : "false");
        });
        $$(".sync-tabpanes .pane", wrap).forEach(p => {
          const on = p.dataset.pane === name;
          p.classList.toggle("active", on);
          p.hidden = !on;
        });
      });
    }
    // Fill in recent sync data
    const emptyMsg = '<div class="history-item"><div class="history-meta muted">No runs for this feature</div></div>';
    const when = row => { const t=row?.finished_at||row?.started_at; if(!t) return "—"; const dt=new Date(t); if(isNaN(+dt)) return "—"; const dd=String(dt.getDate()).padStart(2,"0"), mm=String(dt.getMonth()+1).padStart(2,"0"), yy=String(dt.getFullYear()).slice(-2), hh=String(dt.getHours()).padStart(2,"0"), mi=String(dt.getMinutes()).padStart(2,"0"); return `${dd}-${mm}-${yy} ${hh}:${mi}`; };
    const dur  = v => { if(v==null) return "—"; const n=parseFloat(String(v).replace(/[^\d.]/g,"")); return Number.isFinite(n)? n.toFixed(1)+'s':'—'; };
    const totalsFor = (row, feat) => {
      const f=(row?.features?.[feat])||{};
      const a=+((f.added??f.adds)||0), r=+((f.removed??f.removes)||0), u=+((f.updated??f.updates)||0);
      return { a:a|0, r:r|0, u:u|0, sum:(a|0)+(r|0)+(u|0) };
    };

    const badgeCls = (row,t) => {
      const exit = (typeof row?.exit_code==="number") ? row.exit_code : null;
      const res  = (row?.result?String(row.result):"");
      if (exit!=null && exit!==0) return "err";
      if (res.toUpperCase()==="EQUAL" || t.sum===0) return "ok";
      return "warn";
    };

    // Compute latest finished timestamp
    const all = Array.isArray(hist) ? hist.slice() : [];
    const latestTs = all.reduce((mx,row)=>{
      const t = Date.parse(row?.finished_at || row?.started_at || "");
      return Number.isFinite(t) ? Math.max(mx, t) : mx;
    }, 0);

    function renderPane(list, feat){
      const paneList = wrap.querySelector(`.pane[data-pane="${feat}"] .list`);
      if (!paneList) return;

      const sorted = (list || [])
        .slice()
        .sort((a,b)=> new Date(b.finished_at||b.started_at||0) - new Date(a.finished_at||a.started_at||0));

      const rows = [];
      for (const row of sorted) {
        const en = row?.features_enabled;
        if (en && en[feat] === false) continue;
        rows.push(row);
        if (rows.length >= LIMIT_HISTORY) break;
      }

      if (!rows.length){ paneList.innerHTML = emptyMsg; return; }

      paneList.innerHTML = rows.map(row=>{
        const t = totalsFor(row, feat);
        const b = badgeCls(row, t);
        const upd = t.u ? ` <span class="badge micro">~${t.u}</span>` : "";
        return `<div class="history-item">
          <div class="history-meta">${when(row)} • <span class="badge ${b}">${(row?.result)||"—"}${(typeof row?.exit_code==="number")?(' · '+row.exit_code):''}</span> • ${dur(row?.duration_sec)}</div>
          <div class="history-badges"><span class="badge">+${t.a|0}</span><span class="badge">-${t.r|0}</span>${upd}</div>
        </div>`;
      }).join("");
    }

    // Clear panes first
    FEATS.forEach(n=>{
      const pane = wrap.querySelector(`.pane[data-pane="${n}"] .list`);
      if (pane) pane.innerHTML = emptyMsg;
    });

    if (!all.length) return;
    FEATS.forEach(n => renderPane(all, n));
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Top counters (now / week / month / added / removed)
  // ────────────────────────────────────────────────────────────────────────────
  function renderTopStats(s) {
    const now=+(s?.now||0), week=+(s?.week||0), month=+(s?.month||0), added=+(s?.added||0), removed=+(s?.removed||0);
    const elNow=$("#stat-now"), elW=$("#stat-week"), elM=$("#stat-month"), elA=$("#stat-added"), elR=$("#stat-removed");
    elNow ? animateNumber(elNow, now|0) : txt(elNow, now|0);
    elW   ? animateNumber(elW,   week|0): txt(elW,   week|0);
    elM   ? animateNumber(elM,   month|0):txt(elM,   month|0);
    elA   ? animateNumber(elA,   added|0):txt(elA,   added|0);
    elR   ? animateNumber(elR, removed|0):txt(elR, removed|0);
    const fill=$("#stat-fill"); if (fill){ const max=Math.max(1,now,week,month); fill.style.width=Math.round((now/max)*100)+"%"; }
    animateChart(now,week,month);
    const lab=$("#stat-feature-label"); if (lab) lab.textContent = FEAT_LABEL[_feature] || _feature;
    const chip=$("#trend-week")||$("#stat-delta-chip"); if (chip){ const diff=(now|0)-(week|0); chip.textContent = diff===0 ? "no change" : (diff>0?`+${diff} vs last week`:`${diff} vs last week`); chip.classList.toggle("muted", diff===0); }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Fetch & render pipeline
  // ────────────────────────────────────────────────────────────────────────────
  async function refreshInsights(force=false) {
    const data = await fetchJSON(`/api/insights?limit_samples=60&history=60${force ? "&t="+Date.now() : ""}`); if (!data) return;
    footWrap(); ensureSwitch(); const blk = pickBlock(data, _feature);
    try{ renderSparkline("sparkline", blk.series||[]); }catch{}
    renderHistoryTabs(data.history||[]);
    renderTopStats({ now:blk.now, week:blk.week, month:blk.month, added:blk.added, removed:blk.removed });
    const configured = await getConfiguredProviders();
    renderProviderStats(blk.providers, blk.active, configured);
    const wt = data.watchtime||null;
    if (wt) {
      const wEl=$("#watchtime"); wEl && (wEl.innerHTML = `<div class="big">≈ ${wt.hours|0}</div><div class="units">hrs <span style="opacity:.6">(${wt.days|0} days)</span><br><span style="opacity:.8">${wt.movies|0} movies • ${wt.shows|0} shows</span></div>`);
      const note=$("#watchtime-note"); note && (note.textContent = wt.method || "estimate");
    }
    footWrap.reserve(); setTimeout(footWrap.reserve, 0);
  }

  let _lastStatsFetch = 0;
  async function refreshStats(force=false) {
    const nowT=Date.now(); if(!force && nowT - _lastStatsFetch < 900) return; _lastStatsFetch=nowT;
    const data = await fetchJSON("/api/insights?limit_samples=0&history=60");if (!data) return;
    const blk = pickBlock(data, _feature);
    renderTopStats({ now:blk.now, week:blk.week, month:blk.month, added:blk.added, removed:blk.removed });
    const configured = await getConfiguredProviders();
    renderProviderStats(blk.providers, blk.active, configured);
    footWrap.reserve();
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Shape per feature (normalizes API payloads)
  // ────────────────────────────────────────────────────────────────────────────
  function pickBlock(data, feat) {
    const featureBlock = (data?.features?.[feat] || data?.stats?.[feat] || data?.[feat]) || null;
    const block  = featureBlock || data || {};
    const history = Array.isArray(data?.history) ? data.history : [];
    const n = (v, fb=0)=> Number.isFinite(+v) ? +v : fb;

    function pickProviderTotals(src, whichFeat){
      if(!src) return null;
      if (src.providers_by_feature?.[whichFeat]) return src.providers_by_feature[whichFeat];
      return src.providers || src.provider_stats || src.providers_totals || null;
    }
    const series    = block.series_by_feature?.[feat] || data?.series_by_feature?.[feat] || block.series || [];
    const providers = pickProviderTotals(block, feat) || pickProviderTotals(data, feat) || {};
    const active    = (block.providers_active || data.providers_active || {});

    let { now, week, month, added, removed } = featureBlock ? block : { now: undefined, week: undefined, month: undefined, added: undefined, removed: undefined };

    const unionNow = Math.max(0, ...Object.values(providers || {}).map(v => +v || 0));
    if (!Number.isFinite(+now) || (+now === 0 && unionNow > 0)) now = unionNow;

    const MS = { w:7*86400000, m:30*86400000 }, nowMs=Date.now();
    const rowTs = r => { const t=r?.finished_at||r?.started_at; const ts=t? new Date(t).getTime():NaN; return Number.isFinite(ts)? ts:null; };
    const totalsFor = r => {
      const f=(r?.features?.[feat])||{};
      const a=+((f.added??f.adds)||0), rr=+((f.removed??f.removes)||0), u=+((f.updated??f.updates)||0);
      return { a:a|0, r:rr|0, u:u|0, sum:(a|0)+(rr|0)+(u|0) };
    };

    const rowsAll = history.map(r=>({r,ts:rowTs(r)})).filter(x=>x.ts!=null).sort((a,b)=>a.ts-b.ts);

    if (!Number.isFinite(+now)) now = rowsAll.length ? totalsFor(rowsAll.at(-1).r).sum : 0;

    const sumSince = since =>
      rowsAll.reduce((acc,{r,ts})=>{
        if (ts < since) return acc;
        const t = totalsFor(r);
        acc.A += t.a; acc.R += t.r; acc.S += t.sum; return acc;
      }, {A:0,R:0,S:0});

    const needRange = v => !Number.isFinite(+v) || (+v===0 && feat!=="watchlist");
    if (needRange(week))  week  = sumSince(nowMs - MS.w).S;
    if (needRange(month)) month = sumSince(nowMs - MS.m).S;

    const needAR = v => !Number.isFinite(+v) || (+v===0 && feat!=="watchlist");
    if (needAR(added) || needAR(removed)) {
      const m = sumSince(nowMs - MS.m);
      if (needAR(added))   added   = m.A;
      if (needAR(removed)) removed = m.R;

      if ((+added===0 && +removed===0)) {
        const lastNZ = rowsAll.slice().reverse().find(({r}) => { const t=totalsFor(r); return t.a||t.r||t.u; });
        if (lastNZ) { const t = totalsFor(lastNZ.r); added = t.a; removed = t.r; }
      }
    }

    return { series, providers, active, now:n(now), week:n(week), month:n(month), added:n(added), removed:n(removed), raw:block };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Public API & bootstrapping
  // ────────────────────────────────────────────────────────────────────────────
  w.Insights = Object.assign(w.Insights||{}, {
    renderSparkline, refreshInsights, refreshStats, fetchJSON, animateNumber, animateChart,
    switchFeature, get feature(){ return _feature; }
  });
  w.renderSparkline = renderSparkline; w.refreshInsights = refreshInsights; w.refreshStats = refreshStats;
  w.scheduleInsights = function scheduleInsights(max){ let tries=0, limit=max||20; (function tick(){ if ($("#sync-history")||$("#stat-now")||$("#sparkline")){ refreshInsights(); return; } if (++tries<limit) setTimeout(tick,250); })(); };
  w.fetchJSON = fetchJSON; w.animateNumber = w.animateNumber || animateNumber;

  d.addEventListener("DOMContentLoaded", ()=>{ w.scheduleInsights(); });
  d.addEventListener("tab-changed", ev=>{ if (ev?.detail?.id === "main") refreshInsights(true); });

  // IIFE closer
})(window, document);


/* ---- History tabs layout -------------------------------------------------- */
// Small CSS shim for the history tabs layout.
(function(){
  const id='insights-tabs-layout-fix'; if (document.getElementById(id)) return;
  const s=document.createElement('style'); s.id=id; s.textContent = `
  .sync-tabs{display:flex;justify-content:center;gap:.5rem;margin:.25rem 0 .6rem;flex-wrap:wrap}
  .sync-tabpanes{margin-top:.25rem}.sync-tabpanes .pane{display:none;width:100%}.sync-tabpanes .pane.active{display:block}
  .sync-tabpanes .pane .list{display:flex;flex-direction:column;gap:.5rem}.sync-tabpanes .pane .history-item{width:100%}`;
  document.head.appendChild(s);
})();

/* glassy tabs for Recent syncs */
// Visual-only: keeps existing markup, just adds style.
(() => {
  const id = 'insights-tabs-style-v2';
  if (document.getElementById(id)) return;
  const s = document.createElement('style'); s.id = id;
  s.textContent = `
  #sync-history .sync-tabs{ gap:.35rem; margin:.1rem 0 .4rem; flex-wrap:wrap }
  #sync-history .sync-tabs .tab{
    appearance:none; border:1px solid rgba(255,255,255,.12);
    background:rgba(255,255,255,.06);
    -webkit-backdrop-filter:blur(8px) saturate(110%);
    backdrop-filter:blur(8px) saturate(110%);
    border-radius:12px;
    padding:.34rem .68rem;
    min-height:32px;
    line-height:1; font-weight:700; letter-spacing:.2px;
    color:#e6e8ee;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.05), 0 6px 16px rgba(0,0,0,.22);
    transition:background .16s, border-color .16s, box-shadow .16s, color .16s;
  }
  #sync-history .sync-tabs .tab:hover{ background:rgba(255,255,255,.09); border-color:rgba(255,255,255,.18); }
  #sync-history .sync-tabs .tab.active{
    background:linear-gradient(180deg, rgba(255,255,255,.14), rgba(255,255,255,.06));
    border-color:rgba(120,150,255,.45); color:#fff;
    box-shadow:0 0 0 1px rgba(120,150,255,.5), inset 0 10px 24px rgba(80,130,255,.16), 0 8px 18px rgba(64,128,255,.2);
  }
  #sync-history .sync-tabs .tab:focus-visible{ outline:2px solid rgba(120,150,255,.7); outline-offset:2px; }`;
  document.head.appendChild(s);
})();

/* provider tiles + switcher */
// Visual-only style block for provider tiles and the feature switcher.
(()=>{const old=document.getElementById("insights-provider-styles");if(old)old.remove();
const id="insights-provider-styles-v6";if(document.getElementById(id))return;
const s=document.createElement("style");s.id=id;s.textContent=`
#insights-footer{position:absolute;left:12px;right:12px;bottom:12px;z-index:2}
#insights-footer .ins-foot-wrap{display:flex;flex-direction:column;gap:10px;padding:10px 12px;border-radius:14px;background:linear-gradient(180deg,rgba(8,8,14,.28),rgba(8,8,14,.48));box-shadow:inset 0 0 0 1px rgba(255,255,255,.06),0 8px 22px rgba(0,0,0,.28);backdrop-filter:blur(6px) saturate(110%);-webkit-backdrop-filter:blur(6px) saturate(110%)}
@media(max-width:820px){#insights-footer{position:static;margin-top:10px}}

#insights-switch{display:flex;justify-content:center}
#insights-switch .seg{display:flex;gap:.4rem;flex-wrap:wrap;justify-content:center}
#insights-switch .seg-btn{appearance:none;border:0;cursor:pointer;font:inherit;font-weight:700;letter-spacing:.2px;padding:.38rem .72rem;border-radius:.8rem;color:rgba(255,255,255,.85);background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 0 0 1px rgba(255,255,255,.04);transition:transform .12s,box-shadow .12s,background .12s,border-color .12s;opacity:.95}
#insights-switch .seg-btn:hover{transform:translateY(-1px);opacity:1}
#insights-switch .seg-btn.active{background:linear-gradient(180deg,rgba(22,22,30,.24),rgba(130,150,255,.10));border-color:rgba(128,140,255,.30);box-shadow:0 0 0 1px rgba(128,140,255,.35),0 8px 22px rgba(0,0,0,.18)}

#stats-card #stat-providers{--prov-cols:4;--tile-h:92px;display:grid!important;grid-template-columns:repeat(var(--prov-cols),minmax(0,1fr))!important;grid-auto-rows:var(--tile-h)!important;gap:12px!important;width:100%!important;align-items:stretch!important}
#stats-card #stat-providers .tile{--brand:255,255,255;--wm:none;position:relative!important;display:block!important;height:var(--tile-h)!important;min-height:var(--tile-h)!important;max-height:var(--tile-h)!important;border-radius:12px!important;background:rgba(255,255,255,.045)!important;overflow:hidden!important;isolation:isolate!important;margin:0!important;padding:0!important;border:0!important;box-shadow:inset 0 0 0 1px rgba(255,255,255,.06)}
#stats-card #stat-providers .tile .k{display:none!important}
#stats-card #stat-providers .tile::before{content:"";position:absolute;inset:0;pointer-events:none;z-index:0;background:
  radial-gradient(80% 60% at 35% 40%,rgba(var(--brand),.24),transparent 60%),
  radial-gradient(80% 60% at 55% 75%,rgba(var(--brand),.12),transparent 70%)}
#stats-card #stat-providers .tile::after{content:"";position:absolute;left:50%;top:50%;transform:translate(-50%,-50%) rotate(-8deg);width:220%;height:220%;background-repeat:no-repeat;background-position:center;background-size:contain;background-image:var(--wm);mix-blend-mode:screen;opacity:.28;filter:saturate(1.5) brightness(1.22) contrast(1.05)}
#stats-card #stat-providers .tile{box-shadow:inset 0 0 0 1px rgba(var(--brand),.25),0 0 24px rgba(var(--brand),.16)}
#stats-card #stat-providers .tile.inactive{box-shadow:inset 0 0 0 1px rgba(var(--brand),.18),0 0 16px rgba(var(--brand),.10)}
#stats-card #stat-providers .tile.inactive::after{opacity:.18;filter:saturate(1.1) brightness(1)}
#stats-card #stat-providers .tile .n{position:absolute;left:50%;bottom:8px;transform:translateX(-50%);margin:0;font-weight:800;letter-spacing:.25px;font-size:clamp(18px,2.6vw,28px);line-height:1;color:rgba(255,255,255,.36)}
@supports(-webkit-background-clip:text){#stats-card #stat-providers .tile .n{background-image:linear-gradient(180deg,rgba(255,255,255,.82),rgba(224,224,224,.40) 52%,rgba(255,255,255,.18));-webkit-background-clip:text;-webkit-text-fill-color:transparent;color:transparent}}
@supports(background-clip:text){#stats-card #stat-providers .tile .n{background-image:linear-gradient(180deg,rgba(255,255,255,.82),rgba(224,224,224,.40) 52%,rgba(255,255,255,.18));background-clip:text;color:transparent}}
#stats-card #stat-providers [data-provider=plex]{--brand:229,160,13;--wm:url("/assets/img/PLEX.svg")}
#stats-card #stat-providers [data-provider=simkl]{--brand:0,183,235;--wm:url("/assets/img/SIMKL.svg")}
#stats-card #stat-providers [data-provider=trakt]{--brand:237,28,36;--wm:url("/assets/img/TRAKT.svg")}
#stats-card #stat-providers [data-provider=jellyfin]{--brand:150,84,244;--wm:url("/assets/img/JELLYFIN.svg")}
#stats-card #stat-providers{ --prov-cols:4; --tile-h:96px; }
#stats-card #stat-providers .tile .n{
  position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
  margin:0; font-weight:900; letter-spacing:.25px; font-variant-numeric:tabular-nums;
  font-size:clamp(26px, calc(var(--tile-h)*.48), 56px); line-height:1; color:rgba(255,255,255,.36);
}

#stats-card #stat-providers .provider-empty{
  grid-column:1/-1; display:flex; align-items:center; justify-content:center;
  min-height:80px; padding:12px; border-radius:12px;
  background:rgba(255,255,255,.04); border:1px dashed rgba(255,255,255,.18);
  color:rgba(255,255,255,.75); font-weight:700; letter-spacing:.2px;
}

}
@media(max-width:560px){#stats-card #stat-providers{grid-template-columns:repeat(2,minmax(0,1fr))!important}}
@media(max-width:380px){#stats-card #stat-providers{grid-template-columns:repeat(1,minmax(0,1fr))!important}}
`;document.head.appendChild(s)})();