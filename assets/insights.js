// Insights module: provider-agnostic statistics, sync history, and sparkline rendering
(function (w, d) {
  function $(sel, root) { return (root || d).querySelector(sel); }
  function txt(el, v) { if (el) el.textContent = v == null ? "—" : String(v); }
  function toLocal(iso) { if (!iso) return "—"; var t = new Date(iso); return isNaN(t) ? "—" : t.toLocaleString(undefined, { hour12: false }); }

  // Fetch JSON data from a URL
  async function fetchJSON(url) {
    try { const res = await fetch(url, { cache: "no-store" }); return res.ok ? res.json() : null; }
    catch (_) { return null; }
  }

  // Render a compact SVG sparkline for time series data
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

  // Ensure provider tiles (Plex, Simkl, Trakt) exist in the DOM; create if missing
  function ensureProviderTiles() {
    var container = d.getElementById("stat-providers") || d.querySelector("[data-role='stat-providers']") || d.body;
    ["plex","simkl","trakt"].forEach(function(name){
      var id = "stat-" + name;
      if (!d.getElementById(id) && container) {
        var el = d.createElement("div");
        el.id = id;
        el.className = "prov-tile";
        el.innerHTML = '<div class="title">'+name.toUpperCase()+'</div><div class="big">0</div>';
        container.appendChild(el);
      }
    });
    return {
      plex:  d.getElementById("stat-plex"),
      simkl: d.getElementById("stat-simkl"),
      trakt: d.getElementById("stat-trakt")
    };
  }

  // Render provider totals and update active/inactive state
  function renderProviderStats(provTotals, provActive) {
    var totals = { plex:0, simkl:0, trakt:0 };
    if (provTotals && typeof provTotals === "object") {
      totals.plex  = +(provTotals.plex?.total ?? provTotals.plex ?? 0) || 0;
      totals.simkl = +(provTotals.simkl?.total ?? provTotals.simkl ?? 0) || 0;
      totals.trakt = +(provTotals.trakt?.total ?? provTotals.trakt ?? 0) || 0;
    }
    var active = Object.assign({ plex:false, simkl:false, trakt:false }, provActive || {});

    [["plex","stat-plex","tile-plex"],
     ["simkl","stat-simkl","tile-simkl"],
     ["trakt","stat-trakt","tile-trakt"]].forEach(([k, valId, tileId])=>{
      var v = document.getElementById(valId);
      var t = document.getElementById(tileId);
      if (v) v.textContent = totals[k] | 0;
      if (t) t.classList.toggle("inactive", !active[k]);
    });
  }

  // Render recent sync history items
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

  // Render micro badges for each feature lane except watchlist
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

  // Render top-level counters for now, week, and month
  function renderTopStats(s) {
    var now = +((s && s.now) || 0), week = +((s && s.week) || 0), month = +((s && s.month) || 0);
    txt(d.getElementById("stat-now"), now | 0);
    txt(d.getElementById("stat-week"), week | 0);
    txt(d.getElementById("stat-month"), month | 0);
    var fill = d.getElementById("stat-fill");
    if (fill) { var max = Math.max(1, now, week, month); fill.style.width = Math.round((now / max) * 100) + "%"; }
  }

  // Fetch insights data and render all statistics and history
  async function refreshInsights() {
    var data = await fetchJSON("/api/insights?limit_samples=60&history=3");
    if (!data) return;

    try { renderSparkline("sparkline", data.series || []); } catch (_) {}
    renderHistory(data.history || []);
    renderTopStats({ now: data.now, week: data.week, month: data.month });

    renderProviderStats(
      data.providers || data.provider_stats || null,
      data.providers_active || null
    );

    var wt = data.watchtime || null;
    if (wt) {
      var wEl = d.getElementById("watchtime");
      if (wEl) wEl.innerHTML = '<div class="big">≈ ' + (wt.hours|0) + '</div><div class="units">hrs <span style="opacity:.6">('+(wt.days|0)+' days)</span><br><span style="opacity:.8">'+(wt.movies|0)+' movies • '+(wt.shows|0)+' shows</span></div>';
      var note = d.getElementById("watchtime-note");
      if (note) note.textContent = wt.method || "estimate";
    }
  }

  // Schedule insights rendering, retrying until required elements are present
  function scheduleInsights(max) {
    var tries = 0, limit = max || 20;
    (function tick(){
      var need = d.getElementById("sync-history") || d.getElementById("stat-now") || d.getElementById("sparkline");
      if (need) { refreshInsights(); return; }
      tries++; if (tries < limit) setTimeout(tick, 250);
    })();
  }

  // Expose public API for Insights module
  w.Insights = Object.assign(w.Insights || {}, { renderSparkline, refreshInsights, scheduleInsights, fetchJSON });
  w.renderSparkline = renderSparkline;
  w.refreshInsights = refreshInsights;
  w.scheduleInsights = scheduleInsights;
  w.fetchJSON = fetchJSON;

  // Initialize insights rendering on DOMContentLoaded and tab change
  d.addEventListener("DOMContentLoaded", function(){ scheduleInsights(); });
  d.addEventListener("tab-changed", function(ev){ if (ev && ev.detail && ev.detail.id === "main") refreshInsights(); });
})(window, document);


// Inject compact provider layout styles (only once)
(function injectInsightStyles() {
  var id = "insights-provider-styles";
  if (document.getElementById(id)) return; // avoid duplicates
  var css = `
  // Provider tiles: force a single 3-column row
  #stat-providers {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: .5rem;
    margin-top: .5rem;
  }
  // Neutralize legacy 2-column rules
  .stat-tiles { grid-template-columns: unset; }

  #stat-providers .tile {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: .5rem .75rem;
    min-height: 64px;
    border-radius: .6rem;
    background: rgba(255,255,255,0.05);
    border: 0;
    box-shadow: none;
  }
  #stat-providers .tile .k {
    font-size: .75rem;
    font-weight: 600;
    opacity: .8;
    margin-bottom: .15rem;
  }
  #stat-providers .tile .n {
    font-size: 1rem;
    font-weight: 700;
    line-height: 1;
  }
  // Dim provider tiles that are not part of any current sync pair
  #stat-providers .tile.inactive {
    opacity: .55;
    filter: saturate(.7);
  }
  // Responsive fallback for smaller screens
  @media (max-width: 560px) {
    #stat-providers { grid-template-columns: repeat(2, minmax(0,1fr)); }
  }
  @media (max-width: 380px) {
    #stat-providers { grid-template-columns: 1fr; }
  }`;
  var style = document.createElement("style");
  style.id = id;
  style.textContent = css;
  document.head.appendChild(style);
})();

// Inject CSS for provider tiles (fixed 3-column layout)
(function addProviderCss() {
  var id = "insights-provider-css";
  if (document.getElementById(id)) return;
  var css = `
  #stat-providers {
    display: grid !important;
    grid-template-columns: repeat(3, 1fr) !important;
    gap: .5rem !important;
    width: 100% !important;
    margin-top: .5rem;
  }
  #stat-providers .tile {
    float: none !important;
    flex: none !important;
    width: auto !important;
    min-width: 0 !important;
    max-width: none !important;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    padding: .5rem .75rem;
    min-height: 64px;
    border-radius: .6rem;
    background: rgba(255,255,255,0.05);
    border: 0;
    box-shadow: none;
  }
  #stat-providers .tile .k {
    font-size: .75rem;
    font-weight: 600;
    opacity: .8;
    margin-bottom: .15rem;
  }
  #stat-providers .tile .n {
    font-size: 1rem;
    font-weight: 700;
    line-height: 1;
  }
  #stat-providers .tile.inactive {
    opacity: .55;
    filter: saturate(.7);
  }

  @media (max-width: 560px) {
    #stat-providers { grid-template-columns: repeat(2, 1fr) !important; }
  }
  @media (max-width: 380px) {
    #stat-providers { grid-template-columns: 1fr !important; }
  }`;
  var style = document.createElement("style");
  style.id = id;
  style.textContent = css;
  document.head.appendChild(style);
})();
