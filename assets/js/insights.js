/* assets/js/insights.js */
/* CrossWatch - Insight Module for watchlist, ratings, history, playlists */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (w, d) {
  // Helpers
  const FEATS = ["watchlist","ratings","history","playlists"];
  const FEAT_LABEL = { watchlist:"Watchlist", ratings:"Ratings", history:"History", playlists:"Playlists" };
  const clampFeature = (name) => FEATS.includes(String(name)) ? name : "watchlist";
  let _feature = clampFeature(localStorage.getItem("insights.feature"));

  const titleOf = x =>
    (x?.display_title
    || x?.title
    || x?.series_title
    || x?.name
    || ((x?.type==="episode" && x?.series_title && Number.isInteger(x?.season) && Number.isInteger(x?.episode))
          ? `${x.series_title} S${String(x.season).padStart(2,"0")}E${String(x.episode).padStart(2,"0")}`
          : x?.key)
    || "item");

  const subtitleOf = x => (x?.display_subtitle || "");
  const $  = (s,r)=> (r||d).querySelector(s);
  const $$ = (s,r)=> Array.from((r||d).querySelectorAll(s));

  const fetchJSON = async (url) => {
    const full = url + (url.includes("?") ? "&" : "?") + "_ts=" + Date.now();
    const res = await fetch(full, {
      credentials: "same-origin",
      cache: "no-store",
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} for ${url}`);
    }
    return res.json();
  };

  // Configured providers cache
  const _lc = s => String(s || "").toLowerCase();
  let _cfgSet = null, _cfgAt = 0;
  const CFG_TTL = 60_000;

  async function getConfiguredProviders(force = false) {
    if (!force && _cfgSet && (Date.now() - _cfgAt < CFG_TTL)) return _cfgSet;

    let cfg = {};
    try {
      cfg = await fetchJSON("/api/config?no_secrets=1");
    } catch (e) {
      console.error("[Insights] Failed to load /api/config", e);
      cfg = {};
    }

    const has = v => typeof v === "string" ? v.trim().length > 0 : !!v;
    const S = new Set();
    if (has(cfg?.plex?.account_token)) S.add("plex");
    if (has(cfg?.trakt?.access_token)) S.add("trakt");
    if (has(cfg?.simkl?.access_token)) S.add("simkl");
    if (has(cfg?.anilist?.access_token) || has(cfg?.anilist?.token)) S.add("anilist");
    if (has(cfg?.jellyfin?.access_token)) S.add("jellyfin");
    if (has(cfg?.emby?.access_token) || has(cfg?.emby?.api_key) || has(cfg?.emby?.token)) S.add("emby");
    if (has(cfg?.mdblist?.api_key)) S.add("mdblist");

    const t = cfg?.tautulli || cfg?.auth?.tautulli || {};
    if (has(t?.server_url || t?.server)) S.add("tautulli");

    S.add("crosswatch");

    _cfgSet = S;
    _cfgAt = Date.now();
    return S;
  }

  // Sparkline and animated counters/bars
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

  function fitProviderNumber(el){
    if (!el || !el.closest) return;
    const tile = el.closest("#stat-providers .tile");
    if (!tile) return;
    el.style.setProperty("--ins-font-scale", "1");
    const tb = tile.getBoundingClientRect();
    const nb = el.getBoundingClientRect();
    if (!tb.width || !nb.width) return;
    const maxW = tb.width * 0.78;
    const scale = Math.min(1, maxW / nb.width);
    el.style.setProperty("--ins-font-scale", String(scale.toFixed(3)));
  }

  function fitProviderMSE(el){
    if (!el || !el.closest) return;
    const tile = el.closest("#stat-providers .tile");
    if (!tile) return;
    el.style.setProperty("--ins-mse-scale", "1");
    const tb = tile.getBoundingClientRect();
    const mb = el.getBoundingClientRect();
    if (!tb.width || !mb.width) return;
    const maxW = tb.width - 16;
    const scale = Math.min(1, maxW / mb.width);
    el.style.setProperty("--ins-mse-scale", String(scale.toFixed(3)));
  }

  function animateNumber(el, to, duration = 650){
    if (!el) return;
    const from = parseInt(el.dataset?.v || el.textContent || "0", 10) || 0;
    const finish = () => {
      el.textContent = String(to);
      el.dataset.v = String(to);
      fitProviderNumber(el);
    };
    if (from === to){ finish(); return; }
    const t0 = performance.now();
    const dur = Math.max(180, duration);
    const step = now => {
      const p = Math.min(1, (now - t0) / dur);
      const v = Math.round(from + (to - from) * _ease(p));
      el.textContent = String(v);
      if (p < 1) requestAnimationFrame(step);
      else finish();
    };
    requestAnimationFrame(step);
  }

  function refitProviderNumbers(){
    $$("#stat-providers .tile .n").forEach(fitProviderNumber);
    $$("#stat-providers .tile .mse").forEach(fitProviderMSE);
  }

  window.addEventListener("resize", refitProviderNumbers, { passive:true });

  function animateChart(now,week,month){
    const bars = { now:$('.bar.now'), week:$('.bar.week'), month:$('.bar.month') };
    const max = Math.max(1, now, week, month), h=v=> Math.max(.04, v/max);
    bars.week && (bars.week.style.transform = `scaleY(${h(week)})`);
    bars.month&& (bars.month.style.transform= `scaleY(${h(month)})`);
    bars.now  && (bars.now.style.transform  = `scaleY(${h(now)})`);
  }

  // Footer host
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

  // Feature switcher
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

  // Provider tiles 
  function renderProviderStats(provTotals, provActive, configuredSet, breakdownMap) {
    const wrap = footWrap();
    const host = d.getElementById("stat-providers") || (()=>{ const c=d.createElement("div"); c.id="stat-providers"; wrap.appendChild(c); return c; })();
    if (host.parentNode !== wrap) wrap.appendChild(host);

    const totals = provTotals || {};
    const active = Object.assign({}, provActive || {});
    const conf   = configuredSet || _cfgSet || new Set();
    const breakdown = breakdownMap || {};

    let keys = Array.from(new Set([
      ...Object.keys(Object.assign({}, totals, active)),
      ...Array.from(conf)
    ])).filter(k=> conf.has(_lc(k))).sort();

    // Hide if no providers
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
      const kLc = _lc(k);

      let tile=d.getElementById(id);
      if (!tile) {
        tile=d.createElement("div"); tile.id=id; tile.dataset.provider=kLc; tile.className="tile provider";
        tile.innerHTML=`<div class="n" id="${valId}" data-v="0">0</div>`;
        host.appendChild(tile);
      } else if (tile.parentNode !== host) host.appendChild(tile);

      if (kLc === "crosswatch") {
        tile.style.cursor = "pointer";

        if (!tile.dataset.cwSnapBound) {
          tile.addEventListener("click", () => {
            if (_feature === "playlists") return;
            openCrosswatchSnapshotPicker(_feature);
          });
          tile.dataset.cwSnapBound = "1";
        }
      }

      let valEl=d.getElementById(valId);
      if (!valEl) {
        valEl=d.createElement("div");
        valEl.className="n";
        valEl.id=valId;
        valEl.dataset.v="0";
        valEl.textContent="0";
        tile.appendChild(valEl);
      }
      animateNumber(valEl, (+totals[k]||0), 650);
      tile.classList.toggle("inactive", !active[k]);

      // Per-provider movie/show/anime line
      let infoEl = tile.querySelector(".mse");
      if (!infoEl) {
        infoEl = d.createElement("div");
        infoEl.className = "mse";
        tile.appendChild(infoEl);
      }

      const per = breakdown[kLc] || null;
      if (!per || kLc === "crosswatch" || _feature === "playlists") {
        infoEl.textContent = "";
        infoEl.style.display = "none";
      } else {
        const m = +(per.movies || 0);
        const s = +(per.shows  || 0);
        const a = +(per.anime  || 0);

        const parts = [];
        if (m) parts.push(`M:${m}`);
        if (s) parts.push(`S:${s}`);
        if (a) parts.push(`A:${a}`);

        if (!parts.length) {
          infoEl.textContent = "";
          infoEl.style.display = "none";
        } else {
          infoEl.textContent = parts.join(" ");
          infoEl.style.display = "";
          fitProviderMSE(infoEl); 
        }
      }
      seen.add(id);
    });

    Array.from(host.querySelectorAll(".tile")).forEach(t=>{ if(!seen.has(t.id)) t.remove(); });
    placeSwitchBeforeTiles(); footWrap.reserve();
  }
    function renderCrossWatchSnapshotHint(cwSnapshots) {
    const tile = d.querySelector('#stat-providers [data-provider="crosswatch"]');
    if (!tile) return;

    // Currently No playlists supported
    if (_feature === "playlists") {
      const old = tile.querySelector(".cw-snapshot");
      if (old) old.remove();
      return;
    }

    const key = _feature;
    const info = cwSnapshots && cwSnapshots[key] || null;
    let label = tile.querySelector(".cw-snapshot");

    if (!info || !info.has_snapshots || !info.actual) {
      if (label) label.remove();
      return;
    }
    if (!label) {
      label = d.createElement("div");
      label.className = "cw-snapshot";
      tile.appendChild(label);
    }
    const sel   = String(info.selected || "latest");
    const human = info.human || info.actual;

    let text;
    if (sel === "latest") {
      text = "Latest";
    } else {
      text = human;
    }
    label.textContent = text;
    label.title = info.actual;
  }

  // History tabs
  function renderHistoryTabs(hist){
    const LIMIT_HISTORY = +(localStorage.getItem("insights.history.limit") || 4);
    const wrap = $("#sync-history") || $("[data-role='sync-history']") || $(".sync-history");
    if (!wrap) return;

    if (!wrap.dataset.listInit){
      wrap.innerHTML = '<div class="list"></div>';
      wrap.dataset.listInit = "1";
    }

    const listEl = wrap.querySelector(".list");
    if (!listEl) return;

    const emptyMsg = '<div class="history-item"><div class="history-meta muted">No runs for this feature</div></div>';
    const when = row => { const t=row?.finished_at||row?.started_at; if(!t) return "—"; const dt=new Date(t); if(isNaN(+dt)) return "—"; const dd=String(dt.getDate()).padStart(2,"0"), mm=String(dt.getMonth()+1).padStart(2,"0"), yy=String(dt.getFullYear()).slice(-2), hh=String(dt.getHours()).padStart(2,"0"), mi=String(dt.getMinutes()).padStart(2,"0"); return `${dd}-${mm}-${yy} ${hh}:${mi}`; };
    const dur  = v => { if(v==null) return "—"; const n=parseFloat(String(v).replace(/[^\d.]/g,"")); return Number.isFinite(n)? n.toFixed(1)+'s':'—'; };

    const feat = _feature;

    const totalsFor = (row) => {
      const f = (row?.features?.[feat]) || {};
      const a = f.added   | 0;
      const r = f.removed | 0;
      const u = f.updated | 0;
      return { a, r, u, sum: a + r + u };
    };

    const badgeCls = (row,t) => {
      const exit = (typeof row?.exit_code==="number") ? row.exit_code : null;
      const res  = (row?.result?String(row.result):"");
      if (exit!=null && exit!==0) return "err";
      if (res.toUpperCase()==="EQUAL" || t.sum===0) return "ok";
      return "warn";
    };

    const all = Array.isArray(hist) ? hist.slice() : [];
    const sorted = all
      .slice()
      .sort((a,b)=> new Date(b.finished_at||b.started_at||0) - new Date(a.finished_at||a.started_at||0));

    const rows = [];
    for (const row of sorted) {
      const en = row?.features_enabled;
      if (en && en[feat] === false) continue;
      rows.push(row);
      if (rows.length >= LIMIT_HISTORY) break;
    }

    if (!rows.length){ listEl.innerHTML = emptyMsg; return; }

    listEl.innerHTML = rows.map(row=>{
      const t = totalsFor(row);
      const b = badgeCls(row, t);
      const upd = t.u ? ` <span class="badge micro">~${t.u}</span>` : "";
      return `<div class="history-item">
        <div class="history-meta">${when(row)} • <span class="badge ${b}">${(row?.result)||"—"}${(typeof row?.exit_code==="number")?(' · '+row.exit_code):''}</span> • ${dur(row?.duration_sec)}</div>
        <div class="history-badges"><span class="badge">+${t.a|0}</span><span class="badge">-${t.r|0}</span>${upd}</div>
      </div>`;
    }).join("");
  }

  // Top counters
  function renderTopStats(s) {
    const now    = +(s?.now || 0);
    const week   = +(s?.week || 0);
    const month  = +(s?.month || 0);
    const added  = +(s?.added || 0);
    const removed = +(s?.removed || 0);

    const elNow = $("#stat-now");
    const elW   = $("#stat-week");
    const elM   = $("#stat-month");
    const elA   = $("#stat-added");
    const elR   = $("#stat-removed");

    if (elNow) animateNumber(elNow, now | 0);
    if (elW)   animateNumber(elW,   week | 0);
    if (elM)   animateNumber(elM,   month | 0);
    if (elA)   animateNumber(elA,   added | 0);
    if (elR)   animateNumber(elR,   removed | 0);

    const fill = $("#stat-fill");
    if (fill) {
      const max = Math.max(1, now, week, month);
      fill.style.width = Math.round((now / max) * 100) + "%";
    }

    animateChart(now, week, month);

    const lab = $("#stat-feature-label");
    if (lab) lab.textContent = FEAT_LABEL[_feature] || _feature;

    const chip = $("#trend-week") || $("#stat-delta-chip");
    if (chip) {
      const diff = (now | 0) - (week | 0);
      chip.textContent = diff === 0
        ? "no change"
        : (diff > 0 ? `+${diff} vs last week` : `${diff} vs last week`);
      chip.classList.toggle("muted", diff === 0);
    }

    const bdEl = $("#stat-breakdown");
    if (bdEl && bdEl.parentElement) bdEl.parentElement.removeChild(bdEl);
  }

  // Rendering
  async function refreshInsights(force = false) {
    let data;
    try {
      data = await fetchJSON(`/api/insights?limit_samples=60&history=60${force ? "&t=" + Date.now() : ""}`);
    } catch (e) {
      console.error("[Insights] Failed to load /api/insights", e);
      return;
    }

    let blk;
    try {
      blk = pickBlock(data, _feature);
    } catch (e) {
      console.error("[Insights] Failed to resolve feature block", e);
      return;
    }

    footWrap();
    ensureSwitch();

    try {
      renderSparkline("sparkline", blk.series || []);
    } catch {
    }

    renderHistoryTabs(data.history || []);
    renderTopStats({
      now: blk.now,
      week: blk.week,
      month: blk.month,
      added: blk.added,
      removed: blk.removed,
      feature: _feature,
      breakdown: blk.raw && blk.raw.breakdown ? blk.raw.breakdown : null,
    });

    const configured = await getConfiguredProviders();
    renderProviderStats(
      blk.providers,
      blk.active,
      configured,
      blk.raw && blk.raw.providers_mse ? blk.raw.providers_mse : null,
    );

    renderCrossWatchSnapshotHint(data.crosswatch_snapshots || null);

    const wt = data.watchtime || null;
    if (wt) {
      const wEl = $("#watchtime");
      if (wEl) {
        wEl.innerHTML = `<div class="big">≈ ${wt.hours | 0}</div><div class="units">hrs <span style="opacity:.6">(${wt.days | 0} days)</span><br><span style="opacity:.8">${wt.movies | 0} movies • ${wt.shows | 0} shows</span></div>`;
      }
      const note = $("#watchtime-note");
      if (note) note.textContent = wt.method || "estimate";
    }

    footWrap.reserve();
    setTimeout(footWrap.reserve, 0);
  }

  let _lastStatsFetch = 0;
  async function refreshStats(force = false) {
    const nowT = Date.now();
    if (!force && nowT - _lastStatsFetch < 900) return;
    _lastStatsFetch = nowT;

    let data;
    try {
      data = await fetchJSON("/api/insights?limit_samples=0&history=60");
    } catch (e) {
      console.error("[Insights] Failed to load /api/insights (stats)", e);
      return;
    }

    let blk;
    try {
      blk = pickBlock(data, _feature);
    } catch (e) {
      console.error("[Insights] Failed to resolve feature block (stats)", e);
      return;
    }

    renderTopStats({
      now: blk.now,
      week: blk.week,
      month: blk.month,
      added: blk.added,
      removed: blk.removed,
      feature: _feature,
      breakdown: blk.raw && blk.raw.breakdown ? blk.raw.breakdown : null,
    });

    const configured = await getConfiguredProviders();
    renderProviderStats(
      blk.providers,
      blk.active,
      configured,
      blk.raw && blk.raw.providers_mse ? blk.raw.providers_mse : null,
    )
    renderCrossWatchSnapshotHint(data.crosswatch_snapshots || null);
    footWrap.reserve();
  }

  // Data picker
  function pickBlock(data, feat) {
    if (!data || !data.features || !data.features[feat]) {
      throw new Error(`[Insights] Missing feature block for "${feat}"`);
    }

    const featureBlock = data.features[feat];
    const history = Array.isArray(data.history) ? data.history : [];
    const n = (v, fb = 0) => Number.isFinite(+v) ? +v : fb;

    const series = Array.isArray(featureBlock.series) ? featureBlock.series : [];
    const providers = featureBlock.providers || {};

    const active =
      featureBlock.providers_active ||
      data.providers_active ||
      {};

    let { now, week, month, added, removed } = featureBlock;
    now    = n(now);
    week   = n(week);
    month  = n(month);
    added  = n(added);
    removed = n(removed);

    const MS = { w: 7 * 86400000, m: 30 * 86400000 };
    const nowMs = Date.now();
    const rowTs = row => {
      const t = row?.finished_at || row?.started_at;
      const ts = t ? new Date(t).getTime() : NaN;
      return Number.isFinite(ts) ? ts : null;
    };

    const totalsFor = row => {
      const f = (row?.features?.[feat]) || {};
      const a = f.added   | 0;
      const r = f.removed | 0;
      const u = f.updated | 0;
      return {
        a,
        r,
        u,
        sum: a + r + u,
      };
    };

    const rowsAll = history
      .map(r => ({ r, ts: rowTs(r) }))
      .filter(x => x.ts != null)
      .sort((a, b) => a.ts - b.ts);

    if (!Number.isFinite(now)) {
      now = rowsAll.length ? totalsFor(rowsAll[rowsAll.length - 1].r).sum : 0;
    }

    const sumSince = since =>
      rowsAll.reduce((acc, { r, ts }) => {
        if (ts < since) return acc;
        const t = totalsFor(r);
        acc.A += t.a;
        acc.R += t.r;
        acc.S += t.sum;
        return acc;
      }, { A: 0, R: 0, S: 0 });

    if (!Number.isFinite(week)) {
      week = sumSince(nowMs - MS.w).S;
    }
    if (!Number.isFinite(month)) {
      month = sumSince(nowMs - MS.m).S;
    }

    if (!Number.isFinite(added) || !Number.isFinite(removed)) {
      const m = sumSince(nowMs - MS.m);
      if (!Number.isFinite(added)) added = m.A;
      if (!Number.isFinite(removed)) removed = m.R;
    }

    return {
      series,
      providers,
      active,
      now: n(now),
      week: n(week),
      month: n(month),
      added: n(added),
      removed: n(removed),
      raw: featureBlock,
    };
  }

  // Public API
  w.Insights = Object.assign(w.Insights||{}, {
    renderSparkline, refreshInsights, refreshStats, fetchJSON, animateNumber, animateChart,
    titleOf, subtitleOf,
    switchFeature, get feature(){ return _feature; }
  });
  w.renderSparkline  = renderSparkline;
  w.refreshInsights  = refreshInsights;
  w.refreshStats     = refreshStats;
  w.scheduleInsights = function scheduleInsights(max){
    let tries=0, limit=max||20;
    (function tick(){
      if ($("#sync-history")||$("#stat-now")||$("#sparkline")){ refreshInsights(); return; }
      if (++tries<limit) setTimeout(tick,250);
    })();
  };
  w.fetchJSON      = fetchJSON;
  w.animateNumber  = w.animateNumber || animateNumber;
  w.titleOf        = titleOf;
  w.subtitleOf     = subtitleOf;

  d.addEventListener("DOMContentLoaded", ()=>{ w.scheduleInsights(); });
  d.addEventListener("tab-changed", ev=>{ if (ev?.detail?.id === "main") refreshInsights(true); });

  // Snapshot Picker
  let _cwSnapModal = null;

  function ensureCrosswatchSnapshotModal() {
    if (_cwSnapModal && _cwSnapModal.parentNode) return _cwSnapModal;

    const modal = d.createElement("div");
    modal.className = "cw-snap-picker cw-snap-hidden";
    modal.innerHTML = `
      <div class="cw-snap-dialog">
        <div class="cw-snap-head">
          Select snapshot for <span class="hl"></span>
        </div>
        <div class="cw-snap-body"></div>
        <div class="cw-snap-foot">
          <button class="snap-close">Cancel</button>
        </div>
      </div>
    `;
    d.body.appendChild(modal);

    // Close
    modal.querySelector(".snap-close").addEventListener("click", () => {
      modal.classList.add("cw-snap-hidden");
    });

    modal.addEventListener("click", ev => {
      if (ev.target === modal) {
        modal.classList.add("cw-snap-hidden");
      }
    });

    _cwSnapModal = modal;
    return modal;
  }

  async function openCrosswatchSnapshotPicker(feature) {
    const rootDir = "/config/.cw_provider";
    const snapRoot = `${rootDir}/snapshots`;
    const formatSnapshotLabel = name => {
      const base = String(name || "").replace(/\.json$/,"");
      const stem = base.split("-", 1)[0];
      const m = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(stem);
      if (!m) return base;
      const [, Y, M, D, h, m2] = m;
      return `${Y}-${M}-${D} - ${h}:${m2}`;
    };

    let filesRaw = null;
    let loadError = null;

    try {
      const res = await fetch(
        `/api/files?path=${encodeURIComponent(snapRoot)}`,
        { credentials: "same-origin", cache: "no-store" }
      );
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} for /api/files`);
      }
      filesRaw = await res.json();
    } catch (e) {
      console.error("[Insights] Failed to load snapshot files", e);
      loadError = e;
    }

    const files = Array.isArray(filesRaw?.files) ? filesRaw.files : (filesRaw || []);
    const filtered = files
      .filter(f => !f.is_dir && f.name && f.name.endsWith(`-${feature}.json`))
      .sort((a, b) => a.name.localeCompare(b.name));

    if (loadError) {
      const modal = ensureCrosswatchSnapshotModal();
      const headSpan = modal.querySelector(".cw-snap-head .hl");
      const body = modal.querySelector(".cw-snap-body");

      if (headSpan) headSpan.textContent = feature;
      body.innerHTML = `<div class="muted">Failed to load snapshots. Check server logs or configuration.</div>`;
      modal.classList.remove("cw-snap-hidden");
      return;
    }
    const latestList = filtered.slice(-10).reverse();

    const modal = ensureCrosswatchSnapshotModal();
    const headSpan = modal.querySelector(".cw-snap-head .hl");
    const body = modal.querySelector(".cw-snap-body");

    if (headSpan) headSpan.textContent = feature;

    if (!latestList.length) {
      body.innerHTML = `<div class="muted">No snapshots found for this feature yet.</div>`;
      modal.classList.remove("cw-snap-hidden");
      return;
    }

    const options = [
      { name: "latest", label: "🟢 Latest" },
      ...latestList.map(f => ({
        name: f.name,
        label: formatSnapshotLabel(f.name),
      })),
    ];

    body.innerHTML = options.map(o => `
      <button class="snap-btn" data-name="${o.name}">
        ${o.label}
      </button>
    `).join("");

    body.querySelectorAll(".snap-btn").forEach(btn => {
      btn.addEventListener("click", async e => {
        const name = e.currentTarget.dataset.name;

        try {
          const res = await fetch(
            `/api/crosswatch/select-snapshot?feature=${feature}&snapshot=${encodeURIComponent(name)}`,
            { method: "POST" }
          );
          if (!res.ok) {
            throw new Error(`HTTP ${res.status} for /api/crosswatch/select-snapshot`);
          }
          const bodyJson = await res.json().catch(() => ({}));
          if (bodyJson && bodyJson.ok === false) {
            throw new Error(bodyJson.error || "Backend reported failure");
          }
        } catch (err) {
          console.error("[Insights] Failed to select snapshot", err);
          if (window.cxToast) {
            window.cxToast("Failed to set snapshot. Check server logs.");
          }
          return;
        }

        modal.classList.add("cw-snap-hidden");
        if (window.cxToast) {
          const label = name === "latest" ? "latest" : formatSnapshotLabel(name);
          window.cxToast(`Snapshot set: ${label}`);
        }
        refreshInsights(true);
      });
    });

    modal.classList.remove("cw-snap-hidden");
  }

})(window, document);

(() => {
  const id = 'insights-provider-styles-v6';
  if (document.getElementById(id)) return;

  const s = document.createElement('style');
  s.id = id;
  s.textContent = `
  #insights-footer {
    position:absolute;left:12px;right:12px;bottom:12px;z-index:2;
  }
  #insights-footer .ins-foot-wrap {
    display:flex;flex-direction:column;gap:10px;padding:10px 12px;border-radius:14px;
    background:linear-gradient(180deg,rgba(8,8,14,.28),rgba(8,8,14,.48));
    box-shadow:inset 0 0 0 1px rgba(255,255,255,.06),0 8px 22px rgba(0,0,0,.28);
    backdrop-filter:blur(6px) saturate(110%);-webkit-backdrop-filter:blur(6px) saturate(110%);
  }
  @media(max-width:820px){
    #insights-footer{position:static;margin-top:10px;}
  }

  #insights-switch{display:flex;justify-content:center;}
  #insights-switch .seg{display:flex;gap:.4rem;flex-wrap:wrap;justify-content:center;}
  #insights-switch .seg-btn{
    appearance:none;border:0;cursor:pointer;font:inherit;font-weight:700;letter-spacing:.2px;
    padding:.38rem .72rem;border-radius:.8rem;color:rgba(255,255,255,.85);
    background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.02));
    border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 0 0 1px rgba(255,255,255,.04);
    transition:transform .12s,box-shadow .12s,background .12s,border-color .12s;opacity:.95;
  }
  #insights-switch .seg-btn:hover{
    transform:translateY(-1px);opacity:1;
  }
  #insights-switch .seg-btn.active{
    background:linear-gradient(180deg,rgba(22,22,30,.24),rgba(130,150,255,.10));
    border-color:rgba(128,140,255,.30);
    box-shadow:0 0 0 1px rgba(128,140,255,.35),0 8px 22px rgba(0,0,0,.18);
  }

  #stats-card #stat-providers{
    --prov-cols:4;--tile-h:96px;
    display:grid!important;grid-template-columns:repeat(var(--prov-cols),minmax(0,1fr))!important;
    grid-auto-rows:var(--tile-h)!important;gap:12px!important;width:100%!important;align-items:stretch!important;
  }
  #stats-card #stat-providers .tile{
    --brand:255,255,255;--wm:none;
    position:relative!important;display:block!important;
    height:var(--tile-h)!important;min-height:var(--tile-h)!important;max-height:var(--tile-h)!important;
    border-radius:12px!important;background:rgba(255,255,255,.045)!important;overflow:hidden!important;isolation:isolate!important;
    margin:0!important;padding:0!important;border:0!important;
    box-shadow:inset 0 0 0 1px rgba(var(--brand),.25),0 0 24px rgba(var(--brand),.16);
  }
  #stats-card #stat-providers .tile::before{
    content:"";position:absolute;inset:0;pointer-events:none;z-index:0;
    background:
      radial-gradient(80% 60% at 35% 40%,rgba(var(--brand),.24),transparent 60%),
      radial-gradient(80% 60% at 55% 75%,rgba(var(--brand),.12),transparent 70%);
  }
  #stats-card #stat-providers .tile::after{
    content:"";position:absolute;left:50%;top:50%;transform:translate(-50%,-50%) rotate(-8deg);
    width:220%;height:220%;background-repeat:no-repeat;background-position:center;background-size:contain;
    background-image:var(--wm);mix-blend-mode:screen;opacity:.28;
    filter:saturate(1.5) brightness(1.22) contrast(1.05);
  }
  #stats-card #stat-providers .tile.inactive{
    box-shadow:inset 0 0 0 1px rgba(var(--brand),.18),0 0 16px rgba(var(--brand),.10);
  }
  #stats-card #stat-providers .tile.inactive::after{
    opacity:.18;filter:saturate(1.1) brightness(1);
  }

  #stats-card #stat-providers .tile .n{
    position:absolute;top:50%;left:50%;
    transform:translate(-50%,-50%) scale(var(--ins-font-scale,1));
    transform-origin:center;margin:0;font-weight:900;letter-spacing:.25px;font-variant-numeric:tabular-nums;
    font-size:clamp(26px,calc(var(--tile-h)*.48),56px);line-height:1;color:rgba(255,255,255,.36);
  }

  @supports(-webkit-background-clip:text){
    #stats-card #stat-providers .tile .n{
      background-image:linear-gradient(180deg,rgba(255,255,255,.82),rgba(224,224,224,.40) 52%,rgba(255,255,255,.18));
      -webkit-background-clip:text;-webkit-text-fill-color:transparent;color:transparent;
    }
  }
  @supports(background-clip:text){
    #stats-card #stat-providers .tile .n{
      background-image:linear-gradient(180deg,rgba(255,255,255,.82),rgba(224,224,224,.40) 52%,rgba(255,255,255,.18));
      background-clip:text;color:transparent;
    }
  }
  #stats-card #stat-providers [data-provider=crosswatch] .cw-snapshot{
    position:absolute;left:0;right:0;bottom:6px;padding:0 8px;
    font-size:11px;line-height:1.2;font-weight:700;text-align:center;
    color:rgba(255,255,255,.60);text-shadow:0 1px 2px rgba(0,0,0,.85);
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  }

  #stats-card #stat-providers .tile .mse{
    position:absolute;left:50%;bottom:6px;transform-origin:center bottom;
    padding:0 6px;font-size:11px;line-height:1.2;font-weight:700;font-variant-numeric:tabular-nums;
    white-space:nowrap;text-align:center;
    transform:translateX(-50%) scale(var(--ins-mse-scale,1));
    color:rgba(255,255,255,.60);
    text-shadow:0 1px 2px rgba(0,0,0,.85);
    pointer-events:none;
  }

  #stats-card #stat-providers .tile .mse .mse-group{
    display:inline-flex;align-items:center;gap:3px;margin:0 4px;
  }
  #stats-card #stat-providers .tile .mse .mse-icon{
    font-family:"Material Symbols Rounded";font-weight:400;font-style:normal;
    font-variation-settings:"FILL" 1,"wght" 400,"GRAD" 0,"opsz" 20;
    font-size:1.1em;line-height:1;
  }

  @media(max-width:560px){
    #stats-card #stat-providers{
      grid-template-columns:repeat(2,minmax(0,1fr))!important;
    }
  }
  @media(max-width:380px){
    #stats-card #stat-providers{
      grid-template-columns:repeat(1,minmax(0,1fr))!important;
    }
  }
  `;
  document.head.appendChild(s);
})();