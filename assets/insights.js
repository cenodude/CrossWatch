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

  // --- Number and bar animations (from crosswatch.js) ---
  function _ease(t) { return t < 0.5 ? 2*t*t : -1 + (4 - 2*t)*t; }
  function animateNumber(el, to) {
    if (!el) return;
    const from = parseInt(el.dataset?.v || "0", 10) || 0;
    if (from === to) { el.textContent = String(to); el.dataset.v = String(to); return; }
    const dur = 600, t0 = performance.now();
    function step(now) {
      const p = Math.min(1, (now - t0) / dur);
      const v = Math.round(from + (to - from) * _ease(p));
      el.textContent = String(v);
      if (p < 1) requestAnimationFrame(step); else el.dataset.v = String(to);
    }
    requestAnimationFrame(step);
  }
  function animateChart(now, week, month) {
    const bars = {
      now: d.querySelector(".bar.now"),
      week: d.querySelector(".bar.week"),
      month: d.querySelector(".bar.month"),
    };
    const max = Math.max(1, now, week, month);
    const h = (v) => Math.max(0.04, v / max);
    if (bars.week)  bars.week.style.transform  = `scaleY(${h(week)})`;
    if (bars.month) bars.month.style.transform = `scaleY(${h(month)})`;
    if (bars.now)   bars.now.style.transform   = `scaleY(${h(now)})`;
  }

  // --- Provider tiles (Plex, SIMKL, Trakt): ensure wrapper and value elements exist ---
  function ensureProviderTiles() {
    var container =
      d.getElementById("stat-providers") ||
      d.querySelector("[data-role='stat-providers']") ||
      d.body;

    ["plex","simkl","trakt"].forEach(function(name){
      var tileId = "tile-" + name;
      var valId  = "stat-" + name;

  // Create a tile wrapper if it's missing
      var tile = d.getElementById(tileId);
      if (!tile && container) {
        tile = d.createElement("div");
        tile.id = tileId;
        tile.className = "tile";
        tile.innerHTML =
          '<div class="k">'+name.toUpperCase()+'</div>' +
          '<div class="n" id="'+valId+'">0</div>';
        container.appendChild(tile);
      } else if (tile && !d.getElementById(valId)) {
  // Ensure the inner value node exists
        var n = d.createElement("div");
        n.className = "n";
        n.id = valId;
        tile.appendChild(n);
      }
    });

    return {
      plex:  d.getElementById("stat-plex"),
      simkl: d.getElementById("stat-simkl"),
      trakt: d.getElementById("stat-trakt")
    };
  }

  // --- Derive totals and active flags (supports multiple API response shapes) ---
  function deriveProviderTotals(data) {
  // Prefer server-provided aggregate totals when available
    var by = data && (data.providers || data.provider_stats);
    if (by) return by;

  // Fallback: derive counts from current entries using p/s/t flags
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
    return {
      plex:  !!(totals.plex  || 0),
      simkl: !!(totals.simkl || 0),
      trakt: !!(totals.trakt || 0),
    };
  }

  // --- Render provider totals and active state ---
  function renderProviderStats(provTotals, provActive) {
  // Supported shapes: {plex_total,...,both}, {plex:{total:...}}, plain numbers, or derived counts
    var by = provTotals || {};
    var n = (x) => (+x || 0);

    function pickTotal(key) {
      if (by && typeof by === "object") {
        if (Number.isFinite(+by[key + "_total"])) return n(by[key + "_total"]);
        var maybe = by[key];
        if (maybe && typeof maybe === "object" && "total" in maybe) return n(maybe.total);
  // Last resort: use direct value plus shared 'both' count
        return n(maybe) + n(by.both);
      }
      return 0;
    }

    var totals = {
      plex:  pickTotal("plex"),
      simkl: pickTotal("simkl"),
      trakt: pickTotal("trakt"),
    };

    var active = Object.assign({ plex:false, simkl:false, trakt:false }, provActive || {});

  // Ensure provider tiles are present in the DOM
    ensureProviderTiles();

    [["plex","stat-plex","tile-plex"],
     ["simkl","stat-simkl","tile-simkl"],
     ["trakt","stat-trakt","tile-trakt"]].forEach(([k, valId, tileId])=>{
      var v = d.getElementById(valId);
      var t = d.getElementById(tileId);
      if (v) v.textContent = (totals[k] | 0);
      if (t) t.classList.toggle("inactive", !active[k]);
      t?.removeAttribute("hidden");
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

  // Per-lane micro chips for added/removed/updated counts
      var feats = row && row.features || {};
      var en = row && row.features_enabled || {};
      var chips = [];
      Object.keys(labelMap).forEach(function(k){
  if (k === "watchlist") return;  // Hide watchlist chip
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
    var now = +((s && s.now) || 0), week = +((s && s.week) || 0), month = +((s && s.month) || 0);
    var elNow = d.getElementById("stat-now");
    var elW   = d.getElementById("stat-week");
    var elM   = d.getElementById("stat-month");
    if (elNow) animateNumber(elNow, now | 0); else txt(elNow, now | 0);
    if (elW)   animateNumber(elW,   week | 0); else txt(elW,   week | 0);
    if (elM)   animateNumber(elM,   month| 0); else txt(elM,   month| 0);

    var fill = d.getElementById("stat-fill");
    if (fill) { var max = Math.max(1, now, week, month); fill.style.width = Math.round((now / max) * 100) + "%"; }

  // Optional: animate mini bars if present
    animateChart(now, week, month);
  }

  // --- Fetch and render full insights ---
  async function refreshInsights() {
    var data = await fetchJSON("/api/insights?limit_samples=60&history=3");
    if (!data) return;

    try { renderSparkline("sparkline", data.series || []); } catch (_) {}
    renderHistory(data.history || []);
    renderTopStats({ now: data.now, week: data.week, month: data.month });

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
  if (!force && nowT - _lastStatsFetch < 900) return; // Debounce: skip if recently fetched
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

  // --- Expose API to global scope ---
  w.Insights = Object.assign(w.Insights || {}, {
    renderSparkline, refreshInsights, refreshStats, scheduleInsights, fetchJSON,
    animateNumber, animateChart
  });
  w.renderSparkline = renderSparkline;
  w.refreshInsights = refreshInsights;
  w.refreshStats = refreshStats;          // keep global for other modules
  w.scheduleInsights = scheduleInsights;
  w.fetchJSON = fetchJSON;

  // Compatibility shim for older callers
  w.animateNumber = w.animateNumber || animateNumber;

  // --- Boot: initial load ---
  d.addEventListener("DOMContentLoaded", function(){ scheduleInsights(); });
  d.addEventListener("tab-changed", function(ev){ if (ev && ev.detail && ev.detail.id === "main") refreshInsights(); });
})(window, document);


/* Inject compact provider layout styles (once, no duplicates) */
(function injectInsightStyles() {
  var id = "insights-provider-styles";
  if (document.getElementById(id)) return;
  var css = `
  /* Provider tiles: force a single 3-column row */
  #stat-providers {
    display: grid !important;
    grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
    gap: .5rem !important;
    width: 100% !important;
    margin-top: .5rem;
  }
  /* Neutralize legacy 2-col rules */
  .stat-tiles { grid-template-columns: unset; }

  #stat-providers .tile {
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    padding: .5rem .75rem; min-height: 64px;
    border-radius: .6rem; background: rgba(255,255,255,0.05);
    border: 0; box-shadow: none;
    float: none; flex: none; width: auto; min-width: 0; max-width: none; box-sizing: border-box;
  }
  #stat-providers .tile .k { font-size: .75rem; font-weight: 600; opacity: .8; margin-bottom: .15rem; }
  #stat-providers .tile .n { font-size: 1rem; font-weight: 700; line-height: 1; }
  /* Dim providers not part of any current pair */
  #stat-providers .tile.inactive { opacity: .55; filter: saturate(.7); }

  /* Responsive fallback */
  @media (max-width: 560px) { #stat-providers { grid-template-columns: repeat(2, 1fr) !important; } }
  @media (max-width: 380px) { #stat-providers { grid-template-columns: 1fr !important; } }`;
  var style = document.createElement("style");
  style.id = id;
  style.textContent = css;
  document.head.appendChild(style);
})();
