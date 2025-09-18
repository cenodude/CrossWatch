/* Insights module: provider-agnostic stats, history, and sparkline. */
(function (w, d) {
  // --- Small utility helpers ---
  function $(sel, root) { return (root || d).querySelector(sel); }
  function txt(el, v) { if (el) el.textContent = v == null ? "—" : String(v); }
  function toLocal(iso) { if (!iso) return "—"; var t = new Date(iso); return isNaN(t) ? "—" : t.toLocaleString(undefined, { hour12: false }); }

  // --- Simple HTTP helper ---
  async function fetchJSON(url) {
    try { const res = await fetch(url, { cache: "no-store" }); return res.ok ? res.json() : null; }
    catch (_) { return null; }
  }

  // --- Sparkline renderer (compact SVG) ---
  function renderSparkline(id, points) {
    var el = d.getElementById(id);
    if (!el) return;
    if (!points || !points.length) { el.innerHTML = '<div class="muted">No data</div>'; return; }
    var wv = el.clientWidth || 260, hv = el.clientHeight || 64, pad = 4;
    var xs = points.map(p => +p.ts || 0), ys = points.map(p => +p.count || 0);
    var minX = Math.min.apply(null, xs), maxX = Math.max.apply(null, xs);
    var minY = Math.min.apply(null, ys), maxY = Math.max.apply(null, ys);
    var X = t => maxX === minX ? pad : pad + ((wv - 2 * pad) * (t - minX)) / (maxX - minX);
    var Y = v => maxY === minY ? hv / 2 : hv - pad - ((hv - 2 * pad) * (v - minY)) / (maxY - minY);
    var dStr = points.map((p, i) => (i ? "L" : "M") + X(p.ts) + "," + Y(p.count)).join(" ");
    var dots = points.map(p => '<circle class="dot" cx="'+X(p.ts)+'" cy="'+Y(p.count)+'"></circle>').join("");
    el.innerHTML = '<svg viewBox="0 0 '+wv+' '+hv+'" preserveAspectRatio="none"><path class="line" d="'+dStr+'"></path>'+dots+'</svg>';
  }

  // --- Number + bar animations ---
  function _ease(t) { return t < 0.5 ? 2*t*t : -1 + (4 - 2*t)*t; }
  function animateNumber(el, to, duration) {
    if (!el) return;
    const from = parseInt(el.dataset?.v || el.textContent || "0", 10) || 0;
    if (from === to) { el.textContent = String(to); el.dataset.v = String(to); return; }
    const dur = Math.max(180, duration || 650), t0 = performance.now();
    function step(now) {
      const p = Math.min(1, (now - t0) / dur);
      const v = Math.round(from + (to - from) * _ease(p));
      el.textContent = String(v);
      if (p < 1) requestAnimationFrame(step); else el.dataset.v = String(to);
    }
    requestAnimationFrame(step);
  }
  function animateChart(now, week, month) {
    const bars = { now: d.querySelector(".bar.now"), week: d.querySelector(".bar.week"), month: d.querySelector(".bar.month") };
    const max = Math.max(1, now, week, month);
    const h = (v) => Math.max(0.04, v / max);
    if (bars.week)  bars.week.style.transform  = `scaleY(${h(week)})`;
    if (bars.month) bars.month.style.transform = `scaleY(${h(month)})`;
    if (bars.now)   bars.now.style.transform   = `scaleY(${h(now)})`;
  }

  // --- Provider tiles (Plex, SIMKL, Trakt) ---
  function ensureProviderTiles() {
    var container =
      d.getElementById("stat-providers") ||
      d.querySelector("[data-role='stat-providers']") ||
      d.body;

    ["plex","simkl","trakt"].forEach(function(name){
      var tileId = "tile-" + name;
      var valId  = "stat-" + name;

      var tile = d.getElementById(tileId);
      if (!tile && container) {
        tile = d.createElement("div");
        tile.id = tileId;
        tile.className = "tile provider brand-" + name;
        tile.dataset.provider = name;
        tile.innerHTML =
          '<div class="k">'+name.toUpperCase()+'</div>' +
          '<div class="n" id="'+valId+'">0</div>';
        container.appendChild(tile);
      } else if (tile) {
        tile.classList.add("provider","brand-" + name);
        tile.dataset.provider = name;
        if (!d.getElementById(valId)) {
          var n = d.createElement("div");
          n.className = "n";
          n.id = valId;
          n.textContent = "0";
          tile.appendChild(n);
        }
      }
    });

    return {
      plex:  d.getElementById("stat-plex"),
      simkl: d.getElementById("stat-simkl"),
      trakt: d.getElementById("stat-trakt")
    };
  }

  // Pulse on change
  function pulseTile(tile) {
    tile.classList.remove("pulse-brand");
    // restart animation
    // eslint-disable-next-line no-unused-expressions
    tile.offsetWidth;
    tile.classList.add("pulse-brand");
  }

  // --- Derive totals and active flags ---
  function deriveProviderTotals(data) {
    var by = data && (data.providers || data.provider_stats);
    if (by) return by;

    var cur = data && data.current;
    if (cur && typeof cur === "object") {
      var out = { plex: 0, simkl: 0, trakt: 0 };
      Object.keys(cur).forEach(function (k) {
        var x = cur[k] || {};
        if (x.p) out.plex++;
        if (x.s) out.simkl++;
        if (x.t) out.trakt++;
      });
      return out;
    }
    return null;
  }
  function deriveProviderActive(data, totals) {
    if (data && data.providers_active) return data.providers_active;
    totals = totals || {};
    return { plex: !!(totals.plex||0), simkl: !!(totals.simkl||0), trakt: !!(totals.trakt||0) };
  }

  // --- Render provider totals and active state ---
  function renderProviderStats(provTotals, provActive) {
    var by = provTotals || {};
    var n = (x) => (+x || 0);

    function pickTotal(key) {
      if (by && typeof by === "object") {
        if (Number.isFinite(+by[key + "_total"])) return n(by[key + "_total"]);
        var maybe = by[key];
        if (maybe && typeof maybe === "object" && "total" in maybe) return n(maybe.total);
        return n(maybe) + n(by.both);
      }
      return 0;
    }

    var totals = { plex: pickTotal("plex"), simkl: pickTotal("simkl"), trakt: pickTotal("trakt") };
    var active = Object.assign({ plex:false, simkl:false, trakt:false }, provActive || {});

    ensureProviderTiles();

    [["plex","stat-plex","tile-plex"],
     ["simkl","stat-simkl","tile-simkl"],
     ["trakt","stat-trakt","tile-trakt"]].forEach(([k, valId, tileId])=>{
      var vEl = d.getElementById(valId);
      var tEl = d.getElementById(tileId);
      if (!vEl || !tEl) return;

      var prev = parseInt(vEl.dataset.v || vEl.textContent || 0, 10) || 0;
      animateNumber(vEl, totals[k], 650);
      if (prev !== totals[k]) pulseTile(tEl);

      tEl.classList.toggle("inactive", !active[k]);
      tEl.removeAttribute("hidden");
    });
  }

  // --- Render recent sync history ---
  function renderHistory(hist) {
    var wrap = d.getElementById("sync-history") || d.querySelector("[data-role='sync-history']") || d.querySelector(".sync-history");
    if (!wrap) return;
    if (!hist || !hist.length) { wrap.innerHTML = '<div class="history-item"><div class="history-meta">No history yet</div></div>'; return; }

    function sumFromFeatures(row) {
      var feats = row && row.features || {};
      var en = row && row.features_enabled || {};
      var keys = ["watchlist","ratings","history","playlists"];
      var a=0,r=0,u=0;
      for (var i=0;i<keys.length;i++) {
        var k = keys[i];
        if (en && en[k] === false) continue;
        var f = feats[k] || {};
        a += +((f.added)||0);
        r += +((f.removed)||0);
        u += +((f.updated)||0);
      }
      return {a:rNaN(a), r:rNaN(r), u:rNaN(u)};
      function rNaN(x){ return isFinite(x)?x:0; }
    }

    var labelMap = { watchlist:"WL", ratings:"RT", history:"HC", playlists:"PL" };

    wrap.innerHTML = hist.map(function(row){
      var when = toLocal((row && (row.finished_at || row.started_at)) || null);
      var dur = row && row.duration_sec != null ? (+row.duration_sec).toFixed(1) : "—";
      var totals = { a: row && row.added, r: row && row.removed };
      if (totals.a == null || totals.r == null) {
        var t = sumFromFeatures(row);
        if (totals.a == null) totals.a = t.a;
        if (totals.r == null) totals.r = t.r;
      }
      var result = (row && row.result) ? String(row.result) : "—";
      var exit = (row && typeof row.exit_code === "number") ? row.exit_code : null;

      var badgeClass = "warn";
      if (exit != null && exit !== 0) badgeClass = "err";
      else if (String(result).toUpperCase() === "EQUAL" || ((totals.a|0)===0 && (totals.r|0)===0)) badgeClass = "ok";

      var feats = row && row.features || {};
      var en = row && row.features_enabled || {};
      var chips = [];
      Object.keys(labelMap).forEach(function(k){
        if (k === "watchlist") return;  // hide WL chip
        if (en && en[k] === false) return;
        var f = feats[k] || {};
        var a = +f.added || 0, r = +f.removed || 0, u = +f.updated || 0;
        if (a || r || u) {
          var txt = labelMap[k] + " +" + a + "/-" + r + (u ? "/~" + u : "");
          chips.push('<span class="badge micro">'+txt+'</span>');
        }
      });

      return '<div class="history-item">'
           +   '<div class="history-meta">'+when+' • <span class="badge '+badgeClass+'">'+result+(exit!=null?(' · '+exit):'')+'</span> • '+dur+'s</div>'
           +   '<div class="history-badges">'
           +     '<span class="badge">+'+(totals.a|0)+'</span>'
           +     '<span class="badge">-'+(totals.r|0)+'</span>'
           +     (chips.length ? ('<span class="sep"></span>'+chips.join('')) : '')
           +   '</div>'
           + '</div>';
    }).join("");
  }

  // --- Top-level counters ---
  function renderTopStats(s) {
    var now = +((s && s.now) || 0),
        week = +((s && s.week) || 0),
        month = +((s && s.month) || 0);

    var added = +((s && s.added) || 0),
        removed = +((s && s.removed) || 0);

    var elNow = d.getElementById("stat-now");
    var elW   = d.getElementById("stat-week");
    var elM   = d.getElementById("stat-month");
    var elA   = d.getElementById("stat-added");
    var elR   = d.getElementById("stat-removed");

    if (elNow) animateNumber(elNow, now|0); else txt(elNow, now|0);
    if (elW)   animateNumber(elW,   week|0); else txt(elW,   week|0);
    if (elM)   animateNumber(elM,   month|0); else txt(elM,   month|0);

    if (elA)   animateNumber(elA,   added|0); else txt(elA,   added|0);
    if (elR)   animateNumber(elR,   removed|0); else txt(elR, removed|0);

    var fill = d.getElementById("stat-fill");
    if (fill) { var max = Math.max(1, now, week, month); fill.style.width = Math.round((now / max) * 100) + "%"; }

    animateChart(now, week, month);
  }

  // --- Fetch and render full insights ---
  async function refreshInsights() {
    var data = await fetchJSON("/api/insights?limit_samples=60&history=3");
    if (!data) return;

    try { renderSparkline("sparkline", data.series || []); } catch (_) {}
    renderHistory(data.history || []);
    renderTopStats({
      now: data.now, week: data.week, month: data.month,
      added: data.added, removed: data.removed, new: data.new, del: data.del
    });

    var provTotals = deriveProviderTotals(data);
    var provActive = deriveProviderActive(data, provTotals);
    renderProviderStats(provTotals, provActive);

    var wt = data.watchtime || null;
    if (wt) {
      var wEl = d.getElementById("watchtime");
      if (wEl) wEl.innerHTML = '<div class="big">≈ ' + (wt.hours|0) + '</div><div class="units">hrs <span style="opacity:.6">('+(wt.days|0)+' days)</span><br><span style="opacity:.8">'+(wt.movies|0)+' movies • '+(wt.shows|0)+' shows</span></div>';
      var note = d.getElementById("watchtime-note");
      if (note) note.textContent = wt.method || "estimate";
    }
  }

  // --- Lightweight stats-only refresh (legacy callers) ---
  var _lastStatsFetch = 0;
  async function refreshStats(force=false) {
    var nowT = Date.now();
    if (!force && nowT - _lastStatsFetch < 900) return; // debounce
    _lastStatsFetch = nowT;

    var data = await fetchJSON("/api/insights?limit_samples=0&history=0");
    if (!data) return;

    renderTopStats({ now: data.now, week: data.week, month: data.month });

    var provTotals = deriveProviderTotals(data);
    var provActive = deriveProviderActive(data, provTotals);
    renderProviderStats(provTotals, provActive);
  }

  // --- Mount scheduler UI (if provided) ---
  function scheduleInsights(max) {
    var tries = 0, limit = max || 20;
    (function tick(){
      var need = d.getElementById("sync-history") || d.getElementById("stat-now") || d.getElementById("sparkline");
      if (need) { refreshInsights(); return; }
      tries++; if (tries < limit) setTimeout(tick, 250);
    })();
  }

  // --- Expose API ---
  w.Insights = Object.assign(w.Insights || {}, {
    renderSparkline, refreshInsights, refreshStats, scheduleInsights, fetchJSON,
    animateNumber, animateChart
  });
  w.renderSparkline = renderSparkline;
  w.refreshInsights = refreshInsights;
  w.refreshStats = refreshStats;
  w.scheduleInsights = scheduleInsights;
  w.fetchJSON = fetchJSON;

  // Back-compat
  w.animateNumber = w.animateNumber || animateNumber;

  // --- Boot ---
  d.addEventListener("DOMContentLoaded", function(){ scheduleInsights(); });
  d.addEventListener("tab-changed", function(ev){ if (ev && ev.detail && ev.detail.id === "main") refreshInsights(); });
})(window, document);

/* Inject provider layout styles + subtle brand treatment + animations */
(function injectInsightStyles() {
  var id = "insights-provider-styles";
  if (document.getElementById(id)) return;
  var css = `
  /* Grid */
  #stat-providers{
    display:grid !important;
    grid-template-columns:repeat(3,minmax(0,1fr)) !important;
    gap:.5rem !important; width:100% !important; margin-top:.5rem;
  }
  .stat-tiles{ grid-template-columns: unset; }

  /* Tile */
  #stat-providers .tile{
    display:flex; align-items:center; justify-content:center;
    padding:.5rem .75rem; min-height:76px;
    border-radius:.8rem; background:rgba(255,255,255,.045);
    border:0; box-shadow:none; position:relative; overflow:hidden; isolation:isolate;
    animation: tile-in .6s cubic-bezier(.2,.7,.2,1) both;
  }

  /* Hide labels */
  #stat-providers .tile .k{ display:none !important; }

  /* Watermark: centered, straight, subtle */
  #stat-providers .tile::after{
    content:""; position:absolute; left:50%; top:50%;
    width:90%; height:90%; transform:translate(-50%,-50%);
    background-repeat:no-repeat; background-position:center; background-size:contain;
    opacity:.08; mix-blend-mode:soft-light; filter:blur(.2px);
    pointer-events:none; z-index:0; transition:opacity .25s ease;
  }
  #stat-providers .tile:hover::after{ opacity:.10; }
  #stat-providers .tile.inactive::after{ opacity:.05; }

  /* Feather-light brand bloom */
  #stat-providers .tile::before{
    content:""; position:absolute; left:50%; top:50%;
    width:120%; height:120%; transform:translate(-50%,-50%);
    background:radial-gradient(50% 50% at 50% 50%,
              rgb(var(--glow,255,255,255)/.07),
              rgb(var(--glow,255,255,255)/0) 62%);
    filter:blur(8px); z-index:0; pointer-events:none;
  }

  /* Numbers: LARGE, neutral white/grey gradient (dim) */
  #stat-providers .tile .n{
    position: relative; z-index: 1;
    margin: 0; padding: 0;
    font-weight: 800; letter-spacing: .25px;
    font-size: clamp(36px, 4vw, 64px);
    line-height: 1;
    color: rgba(255,255,255,.36); /* visible fallback if gradients unsupported */
    text-shadow:
      0 1px 0 rgba(0,0,0,.08),
      0 0 4px rgba(255,255,255,.04);
  }
  @supports (-webkit-background-clip:text){
    #stat-providers .tile .n{
      background-image: linear-gradient(
        180deg,
        rgba(255,255,255,.76) 0%,
        rgba(224,224,224,.38) 52%,
        rgba(255,255,255,.16) 100%
      );
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      color: transparent;
    }
  }
  @supports (background-clip:text){
    #stat-providers .tile .n{
      background-image: linear-gradient(
        180deg,
        rgba(255,255,255,.76) 0%,
        rgba(224,224,224,.38) 52%,
        rgba(255,255,255,.16) 100%
      );
      background-clip: text;
      color: transparent;
    }
  }

  /* Hover shimmer (subtle) */
  #stat-providers .tile .n::after{
    content:""; position:absolute; inset:0;
    background: linear-gradient(100deg,
      transparent 0%,
      rgba(255,255,255,.06) 46%,
      rgba(255,255,255,.12) 50%,
      rgba(255,255,255,.06) 54%,
      transparent 100%);
    transform: translateX(-120%);
    opacity:0; pointer-events:none; z-index:2;
  }
  #stat-providers .tile:hover .n::after{
    animation: shimmer 1.05s ease-out forwards;
    opacity:1;
  }

  /* Brand color vars (used for glow/watermark only) */
  #tile-plex,  [data-provider="plex"]  { --glow: 255,194,0;   }
  #tile-simkl, [data-provider="simkl"] { --glow:  24,196,255; }
  #tile-trakt, [data-provider="trakt"] { --glow: 142, 78,255; }

  /* Watermark assets */
  #tile-plex::after,  [data-provider="plex"]::after  { background-image:url("/assets/PLEX.svg");  }
  #tile-simkl::after, [data-provider="simkl"]::after { background-image:url("/assets/SIMKL.svg"); }
  #tile-trakt::after, [data-provider="trakt"]::after { background-image:url("/assets/TRAKT.svg"); }

  /* Update pulse (brand-tinted) */
  #stat-providers .tile.pulse-brand{ animation: brand-pulse .55s ease-out 1; }

  /* Dim inactive */
  #stat-providers .tile.inactive{ opacity:.6; filter:saturate(.85); }

  /* Light themes */
  @media (prefers-color-scheme: light) {
    #stat-providers .tile::after { mix-blend-mode:multiply; opacity:.12; }
  }

  /* Responsive */
  @media (max-width:560px){ #stat-providers{ grid-template-columns:repeat(2,1fr) !important; } }
  @media (max-width:380px){ #stat-providers{ grid-template-columns:1fr !important; } }

  /* --- Animations --- */
  @keyframes tile-in{
    from{ opacity:0; transform: translateY(6px) scale(.98); }
    to  { opacity:1; transform: translateY(0)   scale(1); }
  }
  @keyframes brand-pulse{
    0%  { box-shadow: 0 0 0 0 rgba(var(--glow), .22); }
    100%{ box-shadow: 0 0 0 14px rgba(var(--glow), 0); }
  }
  @keyframes shimmer{
    from{ transform: translateX(-120%); }
    to  { transform: translateX(120%); }
  }`;
  var style = document.createElement("style");
  style.id = id;
  style.textContent = css;
  document.head.appendChild(style);
})();
