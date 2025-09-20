/* Insights module: multi-feature stats (watchlist/ratings/history/playlists). */
(function (w, d) {
  // --- Small helpers ---------------------------------------------------------
  const FEATS = ["watchlist","ratings","history","playlists"];
  const FEAT_LABEL = { watchlist:"Watchlist", ratings:"Ratings", history:"History", playlists:"Playlists" };

  function $(sel, root) { return (root || d).querySelector(sel); }
  function $all(sel, root){ return Array.prototype.slice.call((root || d).querySelectorAll(sel)); }
  function txt(el, v) { if (el) el.textContent = v == null ? "—" : String(v); }
  function toLocal(iso) { if (!iso) return "—"; const t = new Date(iso); return isNaN(+t) ? "—" : t.toLocaleString(undefined, { hour12: false }); }
  function clampFeature(name){ return FEATS.includes(name) ? name : "watchlist"; }

  // Persisted selection
  let _feature = clampFeature(localStorage.getItem("insights.feature") || "watchlist");

  // --- HTTP ------------------------------------------------------------------
  async function fetchJSON(url) {
    try { const res = await fetch(url, { cache: "no-store" }); return res.ok ? res.json() : null; }
    catch (_) { return null; }
  }

  // --- Sparkline -------------------------------------------------------------
  function renderSparkline(id, points) {
    const el = d.getElementById(id);
    if (!el) return;
    if (!points || !points.length) { el.innerHTML = '<div class="muted">No data</div>'; return; }
    const wv = el.clientWidth || 260, hv = el.clientHeight || 64, pad = 4;
    const xs = points.map(p => +p.ts || 0), ys = points.map(p => +p.count || 0);
    const minX = Math.min(...xs), maxX = Math.max(...xs);
    const minY = Math.min(...ys), maxY = Math.max(...ys);
    const X = t => maxX === minX ? pad : pad + ((wv - 2 * pad) * (t - minX)) / (maxX - minX);
    const Y = v => maxY === minY ? hv / 2 : hv - pad - ((hv - 2 * pad) * (v - minY)) / (maxY - minY);
    const dStr = points.map((p, i) => (i ? "L" : "M") + X(p.ts) + "," + Y(p.count)).join(" ");
    const dots = points.map(p => '<circle class="dot" cx="'+X(p.ts)+'" cy="'+Y(p.count)+'"></circle>').join("");
    el.innerHTML = '<svg viewBox="0 0 '+wv+' '+hv+'" preserveAspectRatio="none"><path class="line" d="'+dStr+'"></path>'+dots+'</svg>';
  }

  // --- Number + bar animations ----------------------------------------------
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

  // --- Footer host inside #stats-card (switcher + tiles live here) ----------
  function ensureInsightsFooter() {
    let foot = d.getElementById("insights-footer");
    if (foot) return foot;

    const card = d.getElementById("stats-card") || d.body;
    foot = d.createElement("div");
    foot.id = "insights-footer";
    foot.className = "ins-footer";
    foot.innerHTML = '<div class="ins-foot-wrap"></div>';
    card.appendChild(foot);
    return foot;
  }
  function footWrap() {
    const foot = ensureInsightsFooter();
    return foot.querySelector(".ins-foot-wrap") || foot;
  }

  // Reserve space so the absolute footer never overlaps the stats above
  let _padTimer = 0;
  function reserveSpaceForFooter() {
    const card = d.getElementById("stats-card");
    const foot = d.getElementById("insights-footer");
    if (!card || !foot) return;
    clearTimeout(_padTimer);
    _padTimer = setTimeout(() => {
      const h = (foot.getBoundingClientRect().height || foot.offsetHeight || 120) + 14;
      card.style.paddingBottom = h + "px";
    }, 0);
  }
  w.addEventListener("resize", () => reserveSpaceForFooter(), { passive: true });

  // --- Provider tiles (Plex, SIMKL, Trakt) ----------------------------------
  function ensureProviderTiles() {
    let container =
      d.getElementById("stat-providers") ||
      d.querySelector("[data-role='stat-providers']");

    if (!container) {
      container = d.createElement("div");
      container.id = "stat-providers";
      footWrap().appendChild(container);
    } else if (container.parentElement?.id !== "insights-footer" && container.parentElement?.className !== "ins-foot-wrap") {
      footWrap().appendChild(container);
    }

    ["plex","simkl","trakt"].forEach((name)=>{
      const tileId = "tile-" + name;
      const valId  = "stat-" + name;

      let tile = d.getElementById(tileId);
      if (!tile) {
        tile = d.createElement("div");
        tile.id = tileId;
        tile.className = "tile provider brand-" + name;
        tile.dataset.provider = name;
        tile.innerHTML =
          '<div class="k">'+name.toUpperCase()+'</div>' +
          '<div class="n" id="'+valId+'" data-v="0">0</div>';
        container.appendChild(tile);
      } else {
        tile.classList.add("provider","brand-" + name);
        tile.dataset.provider = name;
        if (!d.getElementById(valId)) {
          const n = d.createElement("div");
          n.className = "n";
          n.id = valId;
          n.dataset.v = "0";
          n.textContent = "0";
          tile.appendChild(n);
        }
        if (tile.parentElement !== container) container.appendChild(tile);
      }
    });

    return {
      plex:  d.getElementById("stat-plex"),
      simkl: d.getElementById("stat-simkl"),
      trakt: d.getElementById("stat-trakt")
    };
  }

  function pulseTile(tile) {
    if (!tile) return;
    tile.classList.remove("pulse-brand");
    // restart animation
    // eslint-disable-next-line no-unused-expressions
    tile.offsetWidth;
    tile.classList.add("pulse-brand");
  }

  // --- Feature switcher (segment + arrows) ----------------------------------
  function ensureFeatureSwitcher() {
    let host = d.getElementById("insights-switch");
    if (!host) {
      host = d.createElement("div");
      host.id = "insights-switch";
      host.className = "ins-switch";
      host.innerHTML =
        '<button class="nav prev" aria-label="Previous feature" title="Previous"></button>' +
        '<div class="seg" role="tablist" aria-label="Insights features"></div>' +
        '<button class="nav next" aria-label="Next feature" title="Next"></button>';
      footWrap().appendChild(host);

      const seg = host.querySelector(".seg");
      FEATS.forEach((key)=>{
        const b = d.createElement("button");
        b.className = "seg-btn";
        b.type = "button";
        b.dataset.key = key;
        b.setAttribute("role", "tab");
        b.textContent = FEAT_LABEL[key] || key;
        if (key === _feature) { b.classList.add("active"); b.setAttribute("aria-selected","true"); }
        seg.appendChild(b);
      });

      host.addEventListener("click", (ev)=>{
        const btn = ev.target.closest(".seg-btn");
        if (!btn) return;
        switchFeature(btn.dataset.key);
      });
      host.querySelector(".prev").addEventListener("click", ()=> {
        const idx = FEATS.indexOf(_feature);
        const next = FEATS[(idx - 1 + FEATS.length) % FEATS.length];
        switchFeature(next);
      });
      host.querySelector(".next").addEventListener("click", ()=> {
        const idx = FEATS.indexOf(_feature);
        const next = FEATS[(idx + 1) % FEATS.length];
        switchFeature(next);
      });
    } else if (host.parentElement?.id !== "insights-footer" && host.parentElement?.className !== "ins-foot-wrap") {
      footWrap().appendChild(host);
    }
    placeFeatureSwitcher();
    reserveSpaceForFooter();
    return host;
  }

  // Keep the switcher before the tiles inside the footer
  function placeFeatureSwitcher() {
    const host = d.getElementById("insights-switch");
    ensureProviderTiles();
    const prov = d.getElementById("stat-providers");
    const wrap = footWrap();
    if (!host || !prov || !wrap) return;
    if (host.nextElementSibling !== prov || host.parentElement !== wrap) {
      wrap.appendChild(host);
      wrap.appendChild(prov);
    }
  }

  function markActiveSwitcher() {
    const seg = d.querySelector("#insights-switch .seg");
    if (!seg) return;
    $all(".seg-btn", seg).forEach(b=>{
      const on = b.dataset.key === _feature;
      b.classList.toggle("active", on);
      b.setAttribute("aria-selected", on ? "true":"false");
    });
  }

  function switchFeature(name){
    const want = clampFeature(name);
    if (want === _feature) return;
    _feature = want;
    localStorage.setItem("insights.feature", want);
    markActiveSwitcher();
    refreshInsights(true);
  }

  // --- Data shaping per feature (UPDATED: avoid mixing features) ------------
  function pickBlock(data, feat) {
    const fromFeatures = data?.features?.[feat] || data?.stats?.[feat] || data?.[feat];
    const block = fromFeatures || data || {};
    const history = Array.isArray(data?.history) ? data.history : [];

    const n = (v, fb = 0) => {
      const x = Number(v);
      return Number.isFinite(x) ? x : fb;
    };

    // Provider totals
    function pickProviderTotals(src, whichFeat) {
      if (!src) return null;
      if (src.providers_by_feature && src.providers_by_feature[whichFeat]) {
        return src.providers_by_feature[whichFeat];
      }
      const by = src.providers || src.provider_stats || src.providers_totals;
      if (by) return by;

      const cur = src.current;
      if (cur && typeof cur === "object") {
        const out = { plex:0, simkl:0, trakt:0 };
        Object.keys(cur).forEach((k)=>{
          const x = cur[k] || {};
          if (x.p) out.plex++;
          if (x.s) out.simkl++;
          if (x.t) out.trakt++;
        });
        return out;
      }
      return null;
    }
    function pickActive(src, totals) {
      if (!src) return { plex:false, simkl:false, trakt:false };
      if (src.providers_active) return src.providers_active;
      totals = totals || {};
      return { plex: !!(totals.plex||0), simkl: !!(totals.simkl||0), trakt: !!(totals.trakt||0) };
    }

    const series =
      block.series_by_feature?.[feat] ||
      data?.series_by_feature?.[feat] ||
      block.series ||
      [];

    const providers = pickProviderTotals(block, feat) || pickProviderTotals(data, feat) || {};
    const active = pickActive(block, providers);

    // Prefer feature-scoped counters if present; otherwise we'll derive them from history.
    let now   = Number.isFinite(+block.now)   ? n(block.now)   : NaN;
    let week  = Number.isFinite(+block.week)  ? n(block.week)  : NaN;
    let month = Number.isFinite(+block.month) ? n(block.month) : NaN;

    // ADDED / REMOVED: only trust explicit per-feature fields
    let added   = NaN;
    let removed = NaN;

    // Optionals (server may expose any of these):
    if (block.added_by_feature && block.added_by_feature[feat] != null)
      added = n(block.added_by_feature[feat]);
    if (block.removed_by_feature && block.removed_by_feature[feat] != null)
      removed = n(block.removed_by_feature[feat]);

    if (Number.isNaN(added) && block.totals_by_feature && block.totals_by_feature[feat]?.added != null)
      added = n(block.totals_by_feature[feat].added);
    if (Number.isNaN(removed) && block.totals_by_feature && block.totals_by_feature[feat]?.removed != null)
      removed = n(block.totals_by_feature[feat].removed);

    // Also check top-level maps if present
    if (Number.isNaN(added) && data?.added_by_feature?.[feat] != null)
      added = n(data.added_by_feature[feat]);
    if (Number.isNaN(removed) && data?.removed_by_feature?.[feat] != null)
      removed = n(data.removed_by_feature[feat]);

    // Derive counters from history if still missing (feature-scoped)
    const MS = { w: 7*86400000, m: 30*86400000 };
    const nowMs = Date.now();

    function rowTs(row) {
      const t = row?.finished_at || row?.started_at;
      const ts = t ? new Date(t).getTime() : NaN;
      return Number.isFinite(ts) ? ts : null;
    }
    function totalsFor(row) {
      const f = (row && row.features && row.features[feat]) || {};
      const a = +((f.added ?? f.adds) || 0);
      const r = +((f.removed ?? f.removes) || 0);
      const u = +((f.updated ?? f.updates) || 0);
      return { a, r, u, sum: (a|0)+(r|0)+(u|0) };
    }
    function rowHasFeat(row) {
      if (!row) return false;
      const explicit = (row.feature || row.run_feature || row.target_feature || row.meta?.feature || "");
      if (explicit) return String(explicit).toLowerCase() === feat;
      const listed = (row.feature_list || row.features_list || row.meta?.features || "");
      if (listed) return (","+String(listed).toLowerCase()+",").includes(","+feat+",");
      const en = row.features_enabled || row.enabled || row.featuresEnabled || null;
      if (en && Object.prototype.hasOwnProperty.call(en, feat)) return !!en[feat];
      const t = totalsFor(row); return t.sum > 0;
    }

    const rows = history
      .map(r => ({ r, ts: rowTs(r) }))
      .filter(x => x.ts != null && rowHasFeat(x.r))
      .sort((a,b)=>a.ts-b.ts);

    if (Number.isNaN(now)) {
      const last = rows.length ? rows[rows.length-1].r : null;
      now = last ? totalsFor(last).sum : 0;
    }
    if (Number.isNaN(week) || Number.isNaN(month) || Number.isNaN(added) || Number.isNaN(removed)) {
      const sumSince = (since) => {
        let A=0,R=0,U=0,S=0;
        rows.forEach(({r, ts})=>{
          if (ts < since) return;
          const t = totalsFor(r); A+=t.a; R+=t.r; U+=t.u; S+=t.sum;
        });
        return {A,R,U,S};
      };
      if (Number.isNaN(week))  week  = sumSince(nowMs - MS.w).S;
      if (Number.isNaN(month)) month = sumSince(nowMs - MS.m).S;
      if (Number.isNaN(added) || Number.isNaN(removed)) {
        const m = sumSince(nowMs - MS.m);
        if (Number.isNaN(added))   added   = m.A;
        if (Number.isNaN(removed)) removed = m.R;
      }
    }

    return {
      series,
      providers,
      active,
      now: n(now, 0),
      week: n(week, 0),
      month: n(month, 0),
      added: n(added, 0),
      removed: n(removed, 0),
      raw:block
    };
  }

  // --- Render provider totals ------------------------------------------------
  function renderProviderStats(provTotals, provActive) {
    const by = provTotals || {};
    const n = (x) => (+x || 0);

    function pickTotal(key) {
      if (by && typeof by === "object") {
        if (Number.isFinite(+by[key + "_total"])) return n(by[key + "_total"]);
        const maybe = by[key];
        if (maybe && typeof maybe === "object" && "total" in maybe) return n(maybe.total);
        return n(maybe) + n(by.both); // tolerate legacy shapes
      }
      return 0;
    }

    const totals = { plex: pickTotal("plex"), simkl: pickTotal("simkl"), trakt: pickTotal("trakt") };
    const active = Object.assign({ plex:false, simkl:false, trakt:false }, provActive || {});
    ensureProviderTiles();

    [["plex","stat-plex","tile-plex"],
     ["simkl","stat-simkl","tile-simkl"],
     ["trakt","stat-trakt","tile-trakt"]].forEach(([k, valId, tileId])=>{
      const vEl = d.getElementById(valId);
      const tEl = d.getElementById(tileId);
      if (!vEl || !tEl) return;

      const prev = parseInt(vEl.dataset.v || vEl.textContent || 0, 10) || 0;
      animateNumber(vEl, totals[k], 650);
      if (prev !== totals[k]) pulseTile(tEl);

      tEl.classList.toggle("inactive", !active[k]);
      tEl.removeAttribute("hidden");
    });

    placeFeatureSwitcher();
    reserveSpaceForFooter();
  }

  // --- Recent syncs: TABBED (shared across features) -------------------------
  function renderHistoryTabs(hist) {
    const wrap =
      document.getElementById("sync-history") ||
      document.querySelector("[data-role='sync-history']") ||
      document.querySelector(".sync-history");
    if (!wrap) return;

    if (!wrap.dataset.tabsInit) {
      wrap.innerHTML =
        '<div class="sync-tabs" role="tablist" aria-label="Recent syncs">' +
          '<button class="tab active" data-tab="watchlist" role="tab" aria-selected="true">Watchlist</button>' +
          '<button class="tab" data-tab="ratings" role="tab" aria-selected="false">Ratings</button>' +
          '<button class="tab" data-tab="history" role="tab" aria-selected="false">History</button>' +
          '<button class="tab" data-tab="playlists" role="tab" aria-selected="false">Playlists</button>' +
        '</div>' +
        '<div class="sync-tabpanes">' +
          '<div class="pane active" data-pane="watchlist" role="tabpanel"><div class="list"></div></div>' +
          '<div class="pane" data-pane="ratings" role="tabpanel" hidden><div class="list"></div></div>' +
          '<div class="pane" data-pane="history" role="tabpanel" hidden><div class="list"></div></div>' +
          '<div class="pane" data-pane="playlists" role="tabpanel" hidden><div class="list"></div></div>' +
        '</div>';
      wrap.dataset.tabsInit = "1";
      wrap.addEventListener("click", (ev) => {
        const btn = ev.target.closest(".tab"); if (!btn) return;
        const name = btn.dataset.tab;
        wrap.querySelectorAll(".sync-tabs .tab").forEach(b=>{
          const on = b.dataset.tab === name;
          b.classList.toggle("active", on);
          b.setAttribute("aria-selected", on ? "true" : "false");
        });
        wrap.querySelectorAll(".sync-tabpanes .pane").forEach(p=>{
          const on = p.dataset.pane === name;
          p.classList.toggle("active", on); p.hidden = !on;
        });
      });
    }

    const emptyMsg = '<div class="history-item"><div class="history-meta muted">No runs for this feature</div></div>';
    const SHOW_ZERO = { watchlist:false, ratings:false, history:false, playlists:false };

    const toWhen  = (row) => {
      const t = (row && (row.finished_at || row.started_at)) || null;
      if (!t) return "—";
      const dt = new Date(t); return isNaN(+dt) ? "—" : dt.toLocaleString(undefined, { hour12:false });
    };
    const safeDur = (v) => {
      if (v == null) return "—";
      const n = parseFloat(String(v).replace(/[^\d.]/g,""));
      return Number.isFinite(n) ? n.toFixed(1)+'s' : '—';
    };
    const totalsFor = (row, feat) => {
      const f = (row && row.features && row.features[feat]) || {};
      const a = +((f.added ?? f.adds) || 0);
      const r = +((f.removed ?? f.removes) || 0);
      const u = +((f.updated ?? f.updates) || 0);
      return { a, r, u, sum: (a|0)+(r|0)+(u|0) };
    };

    // UPDATED: consider enabled maps from orchestrator insights
    function hasFeature(row, feat) {
      if (!row) return false;

      // Explicit single-feature markers win
      const explicit = (row && (row.feature || row.run_feature || row.target_feature || row.meta?.feature || "")).toLowerCase?.() || "";
      if (explicit) return explicit === feat;

      // Explicit list markers (e.g., "watchlist,ratings,history")
      const listed = (row.feature_list || row.features_list || row.meta?.features || "");
      if (listed) {
        const s = String(listed).toLowerCase();
        if ((","+s+",").includes(","+feat+",")) return true;
      }

      // NEW: treat runs as relevant when the feature was enabled (even if 0 changes)
      const enMap = row.features_enabled || row.enabled || row.featuresEnabled || null;
      if (enMap && Object.prototype.hasOwnProperty.call(enMap, feat)) {
        return !!enMap[feat];
      }

      // Fallback: only show if that feature had activity
      const t = totalsFor(row, feat);
      return t.sum > 0;
    }

    const badgeCls = (row, t) => {
      const exit = (row && typeof row.exit_code === "number") ? row.exit_code : null;
      const res  = (row && row.result) ? String(row.result) : "";
      if (exit != null && exit !== 0) return "err";
      if (res.toUpperCase()==="EQUAL" || t.sum===0) return "ok";
      return "warn";
    };

    function renderPane(list, featName) {
      const paneList = wrap.querySelector('.pane[data-pane="'+featName+'"] .list');
      if (!paneList) return;

      const html = (list || [])
        .filter(row => {
          if (!hasFeature(row, featName)) return false;
          const t = totalsFor(row, featName);
          return (t.sum > 0) || SHOW_ZERO[featName] || true; // show enabled runs even with 0
        })
        .map(row => {
          const t = totalsFor(row, featName);
          const b = badgeCls(row, t);
          const upd = t.u ? (' <span class="badge micro">~'+t.u+'</span>') : '';
          return (
            '<div class="history-item">' +
              '<div class="history-meta">' +
                toWhen(row) + ' • ' +
                '<span class="badge '+b+'">'+((row && row.result) || "—")+
                  (typeof row.exit_code === "number" ? (' · '+row.exit_code) : '') +
                '</span> • ' + safeDur(row && row.duration_sec) +
              '</div>' +
              '<div class="history-badges">' +
                '<span class="badge">+'+(t.a|0)+'</span>' +
                '<span class="badge">-'+(t.r|0)+'</span>' +
                upd +
              '</div>' +
            '</div>'
          );
        })
        .join("");

      paneList.innerHTML = html || emptyMsg;
    }

    ["watchlist","ratings","history","playlists"].forEach(n=>{
      const pane = wrap.querySelector('.pane[data-pane="'+n+'"] .list');
      if (pane) pane.innerHTML = emptyMsg; // default
    });
    if (!hist || !hist.length) return;

    renderPane(hist, "watchlist");
    renderPane(hist, "ratings");
    renderPane(hist, "history");
    renderPane(hist, "playlists");
  }



  // --- Top-level counters (per selected feature) -----------------------------
  function renderTopStats(s) {
    const now   = +((s && s.now) || 0);
    const week  = +((s && s.week) || 0);
    const month = +((s && s.month) || 0);
    const added   = +((s && s.added) || 0);
    const removed = +((s && s.removed) || 0);

    const elNow = d.getElementById("stat-now");
    const elW   = d.getElementById("stat-week");
    const elM   = d.getElementById("stat-month");
    const elA   = d.getElementById("stat-added");
    const elR   = d.getElementById("stat-removed");

    if (elNow) animateNumber(elNow, now|0); else txt(elNow, now|0);
    if (elW)   animateNumber(elW,   week|0); else txt(elW,   week|0);
    if (elM)   animateNumber(elM,   month|0); else txt(elM,   month|0);

    if (elA)   animateNumber(elA,   added|0); else txt(elA,   added|0);
    if (elR)   animateNumber(elR,   removed|0); else txt(elR, removed|0);

    const fill = d.getElementById("stat-fill");
    if (fill) { const max = Math.max(1, now, week, month); fill.style.width = Math.round((now / max) * 100) + "%"; }

    animateChart(now, week, month);

    const lab = d.getElementById("stat-feature-label");
    if (lab) lab.textContent = FEAT_LABEL[_feature] || _feature;

    // OPTIONAL: dynamic chip/label (“no change” or delta vs last week)
    const chip = d.getElementById("stat-delta-chip") || d.querySelector(".stat-delta-chip");
    if (chip) {
      const diff = (now|0) - (week|0);
      const txtVal = diff === 0 ? "no change" : (diff > 0 ? `+${diff} vs last week` : `${diff} vs last week`);
      chip.textContent = txtVal;
      chip.classList.toggle("muted", diff === 0);
    }
  }

  // --- Fetch and render ------------------------------------------------------
  async function refreshInsights(force=false) {
    const data = await fetchJSON(`/api/insights?limit_samples=60&history=3${force ? "&t="+Date.now() : ""}`);
    if (!data) return;

    ensureInsightsFooter();
    ensureFeatureSwitcher();
    markActiveSwitcher();

    const blk = pickBlock(data, _feature);

    try { renderSparkline("sparkline", blk.series || []); } catch (_) {}
    renderHistoryTabs(data.history || []);
    renderTopStats({
      now: blk.now, week: blk.week, month: blk.month,
      added: blk.added, removed: blk.removed
    });

    renderProviderStats(blk.providers, blk.active);

    const wt = data.watchtime || null;
    if (wt) {
      const wEl = d.getElementById("watchtime");
      if (wEl) wEl.innerHTML = '<div class="big">≈ ' + (wt.hours|0) + '</div><div class="units">hrs <span style="opacity:.6">('+(wt.days|0)+' days)</span><br><span style="opacity:.8">'+(wt.movies|0)+' movies • '+(wt.shows|0)+' shows</span></div>';
      const note = d.getElementById("watchtime-note");
      if (note) note.textContent = wt.method || "estimate";
    }

    reserveSpaceForFooter();
    setTimeout(reserveSpaceForFooter, 0);
  }

  // Lightweight stats-only refresh (legacy callers)
  let _lastStatsFetch = 0;
  async function refreshStats(force=false) {
    const nowT = Date.now();
    if (!force && nowT - _lastStatsFetch < 900) return;
    _lastStatsFetch = nowT;

    const data = await fetchJSON("/api/insights?limit_samples=0&history=0");
    if (!data) return;

    const blk = pickBlock(data, _feature);
    renderTopStats({ now: blk.now, week: blk.week, month: blk.month, added: blk.added, removed: blk.removed });
    renderProviderStats(blk.providers, blk.active);
    reserveSpaceForFooter();
  }

  // Mount scheduler UI (when dashboard becomes visible)
  function scheduleInsights(max) {
    let tries = 0, limit = max || 20;
    (function tick(){
      const need = d.getElementById("sync-history") || d.getElementById("stat-now") || d.getElementById("sparkline");
      if (need) { refreshInsights(); return; }
      tries++; if (tries < limit) setTimeout(tick, 250);
    })();
  }

  // --- Public API ------------------------------------------------------------
  w.Insights = Object.assign(w.Insights || {}, {
    renderSparkline, refreshInsights, refreshStats, scheduleInsights, fetchJSON,
    animateNumber, animateChart,
    switchFeature, get feature(){ return _feature; }
  });
  w.renderSparkline = renderSparkline;
  w.refreshInsights = refreshInsights;
  w.refreshStats = refreshStats;
  w.scheduleInsights = scheduleInsights;
  w.fetchJSON = fetchJSON;
  w.animateNumber = w.animateNumber || animateNumber; // back-compat

  // --- Boot ------------------------------------------------------------------
  d.addEventListener("DOMContentLoaded", function(){ scheduleInsights(); });
  d.addEventListener("tab-changed", function(ev){ if (ev && ev.detail && ev.detail.id === "main") refreshInsights(true); });
})(window, document);
-
// --------- Layout polish for history tabs (centered, correct visibility) -----
(function patchTabLayoutCss(){
  const id = 'insights-tabs-layout-fix';
  if (document.getElementById(id)) return;

  const css = `
  /* Tabs row centered; panes toggle only via .active */
  .sync-tabs{
    display:flex; justify-content:center; gap:.5rem;
    margin:.25rem 0 .6rem; flex-wrap:wrap;
  }
  .sync-tabpanes{ margin-top:.25rem; }
  .sync-tabpanes .pane{ display:none; width:100%; column-count:1; }
  .sync-tabpanes .pane.active{ display:block; }
  .sync-tabpanes .pane .list{ display:flex; flex-direction:column; gap:.5rem; }
  .sync-tabpanes .pane .history-item{ width:100%; }
  `;

  const s = document.createElement('style');
  s.id = id;
  s.textContent = css;
  document.head.appendChild(s);
})();

// -------- Styles: footer + provider tiles + feature switcher (size preserved) -
(function injectInsightStyles() {
  const id = "insights-provider-styles";
  if (document.getElementById(id)) return;

  const css = `
  /* Footer host inside #stats-card */
  #insights-footer{
    position:absolute; left:12px; right:12px; bottom:12px; z-index:2;
    pointer-events:auto;
  }
  #insights-footer .ins-foot-wrap{
    display:flex; flex-direction:column; gap:8px;
    padding:10px 12px; border-radius:14px;
    background:linear-gradient(180deg, rgba(8,8,14,.32), rgba(8,8,14,.52));
    box-shadow: inset 0 0 0 1px rgba(255,255,255,.06), 0 8px 22px rgba(0,0,0,.28);
    backdrop-filter: blur(6px) saturate(110%);
    -webkit-backdrop-filter: blur(6px) saturate(110%);
  }
  @media (max-width: 820px){
    #insights-footer{ position:static; margin-top:10px; }
  }

  /* Feature switcher — centered line, arrows hidden */
  #insights-switch{
    display:flex; align-items:center; justify-content:center; gap:.5rem; flex-wrap:nowrap;
  }
  #insights-switch .nav{ display:none !important; }
  #insights-switch .seg{
    display:flex; gap:.35rem; flex:1 1 auto; min-width:0; max-width:100%;
    justify-content:center; flex-wrap:nowrap; overflow-x:auto; overflow-y:hidden;
    -webkit-overflow-scrolling:touch; scrollbar-width:none; margin-inline:auto;
  }
  #insights-switch .seg::-webkit-scrollbar{ display:none; }
  #insights-switch .seg-btn{
    appearance:none; border:0; outline:0; cursor:pointer; font:inherit; font-weight:600; letter-spacing:.2px;
    padding:.32rem .66rem; border-radius:.6rem;
    background:linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.03));
    color:rgba(255,255,255,.9); opacity:.9;
    box-shadow: inset 0 0 0 1px rgba(255,255,255,.06);
    transition:all .12s ease; white-space:nowrap;
  }
  #insights-switch .seg-btn:hover{ opacity:1; transform:translateY(-1px); }
  #insights-switch .seg-btn.active{
    background:linear-gradient(180deg, rgba(24,24,24,.28), rgba(255,255,255,.06));
    box-shadow: inset 0 0 0 1px rgba(255,255,255,.14), 0 4px 18px rgba(0,0,0,.18);
  }

  /* — Recent syncs: subtle glassy tabs — */
  .sync-tabs .tab{
    appearance:none; border:0; outline:0; cursor:pointer;
    font:inherit; font-weight:600; letter-spacing:.2px;
    padding:.48rem .9rem; border-radius:.8rem;
    color:rgba(255,255,255,.88);
    background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.02));
    border:1px solid rgba(255,255,255,.08);
    box-shadow: inset 0 0 0 1px rgba(255,255,255,.04), 0 6px 18px rgba(0,0,0,.22);
    backdrop-filter: blur(8px) saturate(115%);
    -webkit-backdrop-filter: blur(8px) saturate(115%);
    transition:transform .12s ease, box-shadow .12s ease, background .12s ease, border-color .12s ease;
  }
  .sync-tabs .tab:hover{
    transform:translateY(-1px);
    border-color:rgba(255,255,255,.12);
    box-shadow: inset 0 0 0 1px rgba(255,255,255,.05), 0 8px 22px rgba(0,0,0,.26);
  }
  .sync-tabs .tab.active{
    background:linear-gradient(180deg, rgba(24,24,40,.28), rgba(120,140,255,.10));
    box-shadow: 0 0 0 1.5px rgba(128,140,255,.45), 0 8px 22px rgba(0,0,0,.26), inset 0 0 0 1px rgba(255,255,255,.08);
    border-color:rgba(128,140,255,.35);
  }
  .sync-tabs .tab:focus-visible{
    outline:2px solid rgba(128,140,255,.55);
    outline-offset:2px;
  }

  /* Provider tiles grid (sizes preserved) */
  #stat-providers{
    display:grid !important;
    grid-template-columns:repeat(3,minmax(0,1fr)) !important;
    gap:.5rem !important; width:100% !important;
  }
  #stat-providers .tile{
    display:flex; align-items:center; justify-content:center;
    padding:.5rem .75rem; min-height:76px;
    border-radius:.8rem; background:rgba(255,255,255,.045);
    border:0; box-shadow:none; position:relative; overflow:hidden; isolation:isolate;
    animation: tile-in .6s cubic-bezier(.2,.7,.2,1) both;
  }
  #stat-providers .tile .k{ display:none !important; }

  /* Watermark */
  #stat-providers .tile::after{
    content:""; position:absolute; left:50%; top:50%;
    width:90%; height:90%; transform:translate(-50%,-50%);
    background-repeat:no-repeat; background-position:center; background-size:contain;
    opacity:.08; mix-blend-mode:soft-light; filter:blur(.2px);
    pointer-events:none; z-index:0; transition:opacity .25s ease;
  }
  #stat-providers .tile:hover::after{ opacity:.10; }
  #stat-providers .tile.inactive::after{ opacity:.05; }

  /* Brand bloom */
  #stat-providers .tile::before{
    content:""; position:absolute; left:50%; top:50%;
    width:120%; height:120%; transform:translate(-50%,-50%);
    background:radial-gradient(50% 50% at 50% 50%, rgb(var(--glow,255,255,255)/.07), rgb(var(--glow,255,255,255)/0) 62%);
    filter:blur(8px); z-index:0; pointer-events:none;
  }

  /* Big numbers (unchanged size) */
  #stat-providers .tile .n{
    position:relative; z-index:1; margin:0; padding:0;
    font-weight:800; letter-spacing:.25px;
    font-size:clamp(36px, 4vw, 64px); line-height:1;
    color:rgba(255,255,255,.36);
    text-shadow:0 1px 0 rgba(0,0,0,.08), 0 0 4px rgba(255,255,255,.04);
  }
  @supports (-webkit-background-clip:text){
    #stat-providers .tile .n{
      background-image:linear-gradient(180deg, rgba(255,255,255,.76) 0%, rgba(224,224,224,.38) 52%, rgba(255,255,255,.16) 100%);
      -webkit-background-clip:text; -webkit-text-fill-color:transparent; color:transparent;
    }
  }
  @supports (background-clip:text){
    #stat-providers .tile .n{
      background-image:linear-gradient(180deg, rgba(255,255,255,.76) 0%, rgba(224,224,224,.38) 52%, rgba(255,255,255,.16) 100%);
      background-clip:text; color:transparent;
    }
  }
  #stat-providers .tile .n::after{
    content:""; position:absolute; inset:0;
    background:linear-gradient(100deg, transparent 0%, rgba(255,255,255,.06) 46%, rgba(255,255,255,.12) 50%, rgba(255,255,255,.06) 54%, transparent 100%);
    transform:translateX(-120%); opacity:0; pointer-events:none; z-index:2;
  }
  #stat-providers .tile:hover .n::after{ animation:shimmer 1.05s ease-out forwards; opacity:1; }

  /* Brand color vars + watermarks */
  #tile-plex,  [data-provider="plex"]  { --glow:255,194,0; }
  #tile-simkl, [data-provider="simkl"] { --glow:24,196,255; }
  #tile-trakt, [data-provider="trakt"] { --glow:142,78,255; }

  #tile-plex::after,  [data-provider="plex"]::after  { background-image:url("/assets/PLEX.svg"); }
  #tile-simkl::after, [data-provider="simkl"]::after { background-image:url("/assets/SIMKL.svg"); }
  #tile-trakt::after, [data-provider="trakt"]::after { background-image:url("/assets/TRAKT.svg"); }

  #stat-providers .tile.pulse-brand{ animation:brand-pulse .55s ease-out 1; }
  #stat-providers .tile.inactive{ opacity:.6; filter:saturate(.85); }

  @media (prefers-color-scheme: light) {
    #stat-providers .tile::after { mix-blend-mode:multiply; opacity:.12; }
  }
  @media (max-width:560px){ #stat-providers{ grid-template-columns:repeat(2,1fr) !important; } }
  @media (max-width:380px){ #stat-providers{ grid-template-columns:1fr !important; } }

  /* History rows */
  .history-item{ padding:.35rem 0; border-bottom:1px dashed rgba(255,255,255,.08); }
  .history-item:last-child{ border-bottom:0; }
  .history-meta{ font-size:.86rem; opacity:.9; }
  .history-badges{ display:flex; align-items:center; gap:.35rem; margin-top:.25rem; }
  .badge{ display:inline-flex; align-items:center; gap:.25rem; padding:.12rem .4rem; border-radius:.45rem; background:rgba(255,255,255,.08); font-size:.78rem; }
  .badge.micro{ font-size:.72rem; opacity:.8; }
  .badge.ok{ background:rgba(80,200,120,.18); }
  .badge.warn{ background:rgba(255,255,255,.12); }
  .badge.err{ background:rgba(255,80,80,.20); }

  /* Neutralize external pushes and keep footer clean */
  #tile-plex, #tile-simkl, #tile-trakt { margin-top:0 !important; }

  /* Animations */
  @keyframes tile-in{ from{ opacity:0; transform:translateY(6px) scale(.98);} to{ opacity:1; transform:translateY(0) scale(1);} }
  @keyframes brand-pulse{ 0%{ box-shadow:0 0 0 0 rgba(var(--glow), .22);} 100%{ box-shadow:0 0 0 14px rgba(var(--glow), 0);} }
  @keyframes shimmer{ from{ transform:translateX(-120%);} to{ transform:translateX(120%);} }
  `;

  const style = document.createElement("style");
  style.id = id;
  style.textContent = css;
  document.head.appendChild(style);
})();
