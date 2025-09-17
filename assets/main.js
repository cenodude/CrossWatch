(() => {
  const FEATS = [
    { key: "watchlist", icon: "movie",       label: "Watchlist" },
    { key: "ratings",   icon: "star",        label: "Ratings"   },
    { key: "history",   icon: "play_arrow",  label: "History"   },
    { key: "playlists", icon: "queue_music", label: "Playlists" },
  ];

  const elProgress = document.getElementById("ux-progress");
  const elLanes    = document.getElementById("ux-lanes");
  const elSpot     = document.getElementById("ux-spotlight"); // intentionally unused (placeholder)
  if (!elProgress || !elLanes || !elSpot) return;

  // ---------- Inline styles for UX controls ----------
  const css = `
  #ux-progress, #ux-lanes { margin-top: 12px; }

  .ux-rail { position: relative; height: 10px; border-radius: 999px; background: #1f1f26; overflow: hidden; }
  .ux-rail-fill { height: 100%; width: 0%; background: linear-gradient(90deg,#7c4dff,#00d4ff); transition: width .35s ease; }
  .ux-rail.error .ux-rail-fill { background: linear-gradient(90deg,#ff6b6b,#ff9f43); }
  .ux-rail-fill.indet { background-size: 200% 100%; animation: uxSweep 1.1s linear infinite; }
  @keyframes uxSweep { 0%{background-position:0% 0} 100%{background-position:200% 0} }
  .ux-rail-steps { display:flex; justify-content:space-between; font-size:11px; margin-top:6px; opacity:.8; }
  .ux-rail-steps span { white-space:nowrap; }

  .lanes { display:grid; grid-template-columns:1fr; gap:10px; }
  @media (min-width: 900px) { .lanes { grid-template-columns:1fr 1fr; } }
  .lane { border:1px solid rgba(255,255,255,.08); border-radius:14px; padding:10px 12px; background:rgba(255,255,255,.02); transition: transform .15s ease; }
  .lane.disabled { opacity:.45; filter:saturate(.5) brightness(.95); }
  .lane.shake { animation: laneShake .42s cubic-bezier(.36,.07,.19,.97); }
  @keyframes laneShake {
    10%, 90% { transform: translateX(-1px) }
    20%, 80% { transform: translateX( 2px) }
    30%, 50%, 70% { transform: translateX(-4px) }
    40%, 60% { transform: translateX( 4px) }
  }
  .lane-h { display:flex; align-items:center; gap:10px; }
  .lane-ico { font-size:18px; line-height:1; }
  .lane-title { font-weight:600; font-size:13px; opacity:.95; }
  .lane-badges { margin-left:auto; display:flex; gap:6px; align-items:center; }
  .chip { font-size:11px; padding:2px 8px; border-radius:999px; border:1px solid rgba(255,255,255,.12); opacity:.9; }
  .chip.ok  { border-color: rgba(0,220,130,.45); color:#4be3a6; }
  .chip.run { border-color: rgba(0,180,255,.45); color:#4dd6ff; }
  .chip.skip{ border-color: rgba(255,255,255,.18); color:rgba(255,255,255,.7); }
  .chip.err { border-color: rgba(255,80,80,.5); color:#ff7b7b; }
  .delta { font-size:11px; display:inline-flex; gap:6px; align-items:center; opacity:.9; }
  .delta b { font-weight:600; }
  .lane-body { margin-top:8px; display:grid; grid-template-columns:1fr; gap:6px; }
  .spot { font-size:12px; opacity:.95; display:flex; gap:8px; align-items:baseline; }
  .spot .tag { font-size:10px; padding:2px 6px; border-radius:6px; border:1px solid rgba(255,255,255,.12); opacity:.85; }
  .spot .t-add { color:#7cffc4; border-color:rgba(124,255,196,.25); }
  .spot .t-rem { color:#ff9aa2; border-color:rgba(255,154,162,.25); }
  .spot .t-upd { color:#9ecbff; border-color:rgba(158,203,255,.25); }
  .muted { opacity:.7; }
  .small { font-size:11px; }
  `;
  const styleEl = document.createElement("style");
  styleEl.id = "ux-styles";
  styleEl.textContent = css;
  document.head.appendChild(styleEl);

  // ---------- Component state ----------
  let timeline = { start:false, pre:false, post:false, done:false };
  let progressPct = 0;
  let status = null;
  let summary = null;
  let _prevTL = { start:false, done:false };
  let _prevRunning = false;

  // Optimistic UI state (assume progress until server confirms)
  let optimistic = false;
  let lastServerUpdate = 0;

  // Map of enabled lanes (derived from /api/pairs)
  let enabledFromPairs = null;
  let lastPairsAt = 0;

  // Counters used for shake detection on updates
  const lastCounts = Object.create(null);

  // ---------- Utility functions ----------
  const clamp = (n,a,b)=>Math.max(a,Math.min(b,n));
  function asPctFromTimeline(tl) {
    if (!tl) return 0;
    if (tl.done) return 100;
    const order = ["start","pre","post","done"];
    let k = 0; for (const x of order) if (tl[x]) k++;
    let pct = (k / order.length) * 100;
    if (pct > 0 && pct < 15) pct = 15;
    return pct;
  }
  function pick(obj, path, dflt) {
    try { const parts = Array.isArray(path)?path:path.split("."); let v=obj; for (const p of parts) v=v?.[p]; return v ?? dflt; }
    catch { return dflt; }
  }
  async function fetchJSON(url, fallback=null) {
    try { const r = await fetch(url, { credentials:"same-origin" }); if (!r.ok) return fallback; return await r.json(); }
    catch { return fallback; }
  }
  async function fetchFirstJSON(urls, fallback=null) {
    for (const u of urls) { const j = await fetchJSON(u, null); if (j) return j; } return fallback;
  }

  function defaultEnabledMap() {
    return { watchlist:true, ratings:true, history:true, playlists:true };
  }
  function getEnabledMap() {
    if (enabledFromPairs) return enabledFromPairs;
    return summary?.enabled || defaultEnabledMap();
  }

  // ---------- Rendering helpers ----------
  function renderProgress() {
    elProgress.innerHTML = "";
    const rail = document.createElement("div");
    rail.className = "ux-rail";
    const fill = document.createElement("div");
    fill.className = "ux-rail-fill";

    const pct = timeline?.done ? 100 : Math.min(95, clamp(progressPct, 0, 100));
    fill.style.width = pct + "%";

    const indet = (optimistic && !timeline.pre && !timeline.post && !timeline.done);
    if (indet) fill.classList.add("indet");

    const error = (summary?.exit_code != null && summary.exit_code !== 0);
    if (error) rail.classList.add("error"); else rail.classList.remove("error");

    rail.appendChild(fill);

    const steps = document.createElement("div");
    steps.className = "ux-rail-steps muted";
    ["Start","Discovering","Syncing","Done"].forEach(t => {
      const s = document.createElement("span"); s.textContent = t; steps.appendChild(s);
    });

    elProgress.appendChild(rail);
    elProgress.appendChild(steps);
  }

  function getLaneStats(sum, key) {
    const f = (sum?.features?.[key]) || sum?.[key] || {};
    const added   = f.added   ?? f.add    ?? f.adds   ?? f.plus     ?? 0;
    const removed = f.removed ?? f.del    ?? f.deletes?? f.minus    ?? 0;
    const updated = f.updated ?? f.upd    ?? f.changed?? 0;
    const items   = Array.isArray(f.items) ? f.items : [];
    const spotAdd = Array.isArray(f.spotlight_add)    ? f.spotlight_add    : [];
    const spotRem = Array.isArray(f.spotlight_remove) ? f.spotlight_remove : [];
    const spotUpd = Array.isArray(f.spotlight_update) ? f.spotlight_update : [];
    return { added, removed, updated, items, spotAdd, spotRem, spotUpd };
  }

  function laneState(sum, key, enabled) {
    const err = (sum?.exit_code != null && sum.exit_code !== 0);
    if (!enabled) return "skip";
    if (err) return "err";
    if (timeline?.done) return "ok";
    if (timeline?.start && !timeline?.done) return "run";
    return "skip";
  }

  function fmtDelta(a, r, u) {
    return `+${a||0} / -${r||0} / ~${u||0}`;
  }

  function renderLanes() {
    elLanes.innerHTML = "";
    const wrap = document.createElement("div");
    wrap.className = "lanes";

    const enabledMap = getEnabledMap();
    const running = summary?.running === true || (!timeline.done && timeline.start);

    for (const f of FEATS) {
      const isEnabled = !!enabledMap[f.key];
      const { added, removed, updated, spotAdd, spotRem, spotUpd } = getLaneStats(summary || {}, f.key);
      const st = laneState(summary || {}, f.key, isEnabled);

      const lane = document.createElement("div"); lane.className = "lane";
      if (!isEnabled) lane.classList.add("disabled");

  // Add a temporary 'shake' animation when totals increase during a run
      const total = (added||0) + (removed||0) + (updated||0);
      const prev  = lastCounts[f.key] ?? 0;
      if (running && total > prev && isEnabled) {
        lane.classList.add("shake");
        setTimeout(() => lane.classList.remove("shake"), 450);
      }
      lastCounts[f.key] = total;

      const h = document.createElement("div"); h.className = "lane-h";
      const ico = document.createElement("div"); ico.className = "lane-ico"; ico.innerHTML = `<span class="material-symbol">${f.icon}</span>`;

      const ttl = document.createElement("div"); ttl.className = "lane-title"; ttl.textContent = f.label;

      const badges = document.createElement("div"); badges.className = "lane-badges";
      const delta = document.createElement("span"); delta.className = "delta"; delta.innerHTML = `<b>${fmtDelta(added, removed, updated)}</b>`;
      const chip  = document.createElement("span");
      chip.className = "chip " + (st === "ok" ? "ok" : st === "run" ? "run" : st === "err" ? "err" : "skip");
      chip.textContent = !isEnabled ? "Disabled" : st === "err" ? "Failed" : st === "ok" ? "Synced" : st === "run" ? "Running" : "Skipped";
      badges.appendChild(delta); badges.appendChild(chip);

      h.appendChild(ico); h.appendChild(ttl); h.appendChild(badges); lane.appendChild(h);

      const body = document.createElement("div"); body.className = "lane-body";
      const spots = [];
      for (const x of spotAdd.slice(0,2)) spots.push({ t:"add", text: x?.title || x });
      for (const x of spotRem.slice(0,2)) spots.push({ t:"rem", text: x?.title || x });
      for (const x of spotUpd.slice(0,2)) spots.push({ t:"upd", text: x?.title || x });

      if (!isEnabled) {
        const note = document.createElement("div");
        note.className = "spot muted small";
        note.textContent = "Feature not configured";
        body.appendChild(note);
      } else if (spots.length === 0) {
        const none = document.createElement("div");
        none.className = "spot muted small";
        none.textContent = timeline?.done ? "No changes" : "Awaiting results…";
        body.appendChild(none);
      } else {
        for (const s of spots.slice(0,3)) {
          const row = document.createElement("div"); row.className = "spot";
          const tag = document.createElement("span");
          tag.className = "tag " + (s.t === "add" ? "t-add" : s.t === "rem" ? "t-rem" : "t-upd");
          tag.textContent = s.t === "add" ? "Added" : s.t === "rem" ? "Removed" : "Updated";
          const txt = document.createElement("span"); txt.textContent = s.text;
          row.appendChild(tag); row.appendChild(txt); body.appendChild(row);
        }
      }

      lane.appendChild(body);
      wrap.appendChild(lane);
    }

    elLanes.appendChild(wrap);
  }

  function renderSpotlightSummary() {
  elSpot.innerHTML = ""; // intentionally left blank (no "Sync complete" message)
  }

  // ---------- Pairs → derive enabled lanes ----------
  async function pullPairs() {
    const arr = await fetchJSON("/api/pairs", null);
    if (!Array.isArray(arr)) return;

  // If pairs exist, compute the union of enabled features; otherwise leave null to fall back to summary/defaults
    if (arr.length === 0) { enabledFromPairs = null; return; }

    const enabled = { watchlist:false, ratings:false, history:false, playlists:false };
    for (const p of arr) {
      const feats = p?.features || {};
      for (const f of FEATS) {
        const cfg = feats[f.key];
        if (cfg && (cfg.enable === true || cfg.enabled === true)) enabled[f.key] = true;
      }
    }
    enabledFromPairs = enabled;
  }

  // ---------- Fallback helpers ----------
  async function hydrateFromInsights(startTsEpoch) {
    const src = await fetchFirstJSON(
      ["/api/insights", "/api/statistics", "/statistics.json", "/data/statistics.json"],
      null
    );
    const events = src?.events;
    if (!Array.isArray(events) || !events.length) return false;

    const since = Math.floor(startTsEpoch || 0);
    const recent = events.filter(e => (e.ts || 0) >= since);

    let added = 0, removed = 0;
    const spotAdd = [], spotRem = [];
    for (const e of recent) {
      const title = e.title || e.key || "item";
      if (e.action === "add")    { added++;   if (spotAdd.length < 3) spotAdd.push(title); }
      if (e.action === "remove") { removed++; if (spotRem.length < 3) spotRem.push(title); }
    }

    summary = summary || {};
    summary.features = summary.features || {};
    const lane = summary.features.watchlist || {};
    lane.added = added || lane.added || 0;
    lane.removed = removed || lane.removed || 0;
    lane.updated = lane.updated || 0;
    if (!lane.spotlight_add?.length && spotAdd.length) lane.spotlight_add = spotAdd;
    if (!lane.spotlight_remove?.length && spotRem.length) lane.spotlight_remove = spotRem;
    summary.features.watchlist = lane;

    summary.enabled = Object.assign(defaultEnabledMap(), summary.enabled || {});

    renderAll();
    return (added > 0 || removed > 0 || spotAdd.length || spotRem.length);
  }

  function hydrateFromLog() {
    const det = document.getElementById("det-log");
    if (!det) return false;
    const txt = det.innerText || det.textContent || "";
    if (!txt) return false;

    const lines = txt.split(/\n+/).slice(-80);
    let added = 0, removed = 0;

    for (let i = lines.length - 1; i >= 0; i--) {
      const m = lines[i].match(/Sync complete·\+(\d+)\s*\/\s*-(\d+)/);
      if (m) { added = parseInt(m[1],10)||0; removed = parseInt(m[2],10)||0; break; }
    }
    if (added === 0 && removed === 0) {
      for (let i = lines.length - 1; i >= 0; i--) {
        const m = lines[i].match(/Plan·add A=(\d+),\s*add B=(\d+),\s*remove A=(\d+),\s*remove B=(\d+)/i);
        if (m) {
          added = (parseInt(m[1],10)||0) + (parseInt(m[2],10)||0);
          removed = (parseInt(m[3],10)||0) + (parseInt(m[4],10)||0);
          break;
        }
      }
    }

    const changed = (added > 0 || removed > 0);

    summary = summary || {};
    summary.features = summary.features || {};
    const lane = summary.features.watchlist || {};
    lane.added = added || lane.added || 0;
    lane.removed = removed || lane.removed || 0;
    lane.updated = lane.updated || 0;
    summary.features.watchlist = lane;

    summary.enabled = Object.assign(defaultEnabledMap(), summary.enabled || {});
    renderAll();
    return changed;
  }

  function hasFeatureData() {
    return summary?.features && Object.values(summary.features).some(v =>
      (v?.added||v?.removed||v?.updated||0) > 0 ||
      (v?.spotlight_add?.length||v?.spotlight_remove?.length||v?.spotlight_update?.length)
    );
  }

  // ---------- Data fetch routines ----------
  async function pullStatus() {
    status = await fetchJSON("/api/status", status);
    try { window._ui = window._ui || {}; window._ui.status = status; } catch (e) {}
  }

async function pullSummary() {
  const s = await fetchJSON("/api/run/summary", summary);
  if (!s) return;

  // Remember previous state before updating UI
  const prevTL = _prevTL;
  const prevRunning = _prevRunning;

  summary = s;
  lastServerUpdate = Date.now();

  const tl = s?.timeline || s?.tl || null;
  const running = s?.running === true || s?.state === "running";
  const exitedOk = (s?.exit_code === 0) || (s?.exit === 0) || (s?.status === "ok");

  let mapped = {
    start: !!(tl?.start || tl?.started || tl?.[0] || s?.started),
    pre:   !!(tl?.pre   || tl?.discovery || tl?.discovering || tl?.[1]),
    post:  !!(tl?.post  || tl?.syncing   || tl?.apply       || tl?.[2]),
    done:  !!(tl?.done  || tl?.finished  || tl?.complete    || tl?.[3]),
  };
  if (!mapped.done && !running && (exitedOk || s?.finished || s?.end)) {
    mapped = { start:true, pre:true, post:true, done:true };
  }

  timeline = mapped;
  progressPct = asPctFromTimeline(timeline);

  // ---- Legacy compatibility bridges (do not alter) -------------------------
  try {
    if (typeof updateProgressFromTimeline === "function") updateProgressFromTimeline(timeline);
    const btn = document.getElementById("run");
    if (typeof startRunVisuals === "function" && typeof stopRunVisuals === "function") {
      if (running && !_prevRunning) {
        const indeterminate = !(timeline.pre || timeline.post || timeline.done);
        startRunVisuals(indeterminate);
        btn?.classList.add("glass");
      }
      if (!running && _prevRunning) {
        stopRunVisuals();
        btn?.classList.remove("glass");
      }
    }
    if (typeof recomputeRunDisabled === "function") recomputeRunDisabled();
  } catch (e) {}

  // ---- Robust detection of phase transitions --------------------------------
  const wasInProgress = prevRunning || (prevTL.start && !prevTL.done) || optimistic;
  const nowInProgress = running || (timeline.start && !timeline.done);
  const justFinished  = wasInProgress && !nowInProgress && timeline.done;

  if (justFinished) {
  optimistic = false; // Clear optimistic mode once confirmed

  // Refresh left-side preview elements
    try {
      window.wallLoaded = false;
      if (typeof updatePreviewVisibility === "function") updatePreviewVisibility();
      if (typeof loadWatchlist === "function") loadWatchlist();
      if (typeof refreshSchedulingBanner === "function") refreshSchedulingBanner();
    } catch (e) {}

  // Refresh right-side statistics panel
    try { (window.Insights?.refreshInsights || window.refreshInsights)?.(); } catch (e) {}

  // Dispatch an event for other listeners
    try {
      window.dispatchEvent(new CustomEvent("sync-complete", { detail: { at: Date.now(), summary } }));
    } catch (e) {}
  }

  // Preserve previous markers for the next update tick
  _prevTL = { start: timeline.start, done: timeline.done };
  _prevRunning = !!running;

  // Ensure enabled map is set and re-render lanes
  if (!summary.enabled) summary.enabled = defaultEnabledMap();
  renderAll();

  // Feature hydration fallback (use defaults when server data missing)
  const hasFeatures =
    summary?.features && Object.keys(summary.features).length > 0 &&
    Object.values(summary.features).some(v =>
      (v?.added||v?.removed||v?.updated||0) > 0 ||
      (v?.spotlight_add?.length||v?.spotlight_remove?.length||v?.spotlight_update?.length)
    );
  if (!hasFeatures && timeline.done) {
    const startTs = summary?.raw_started_ts || (summary?.started_at ? Date.parse(summary.started_at)/1000 : 0);
    const ok = await hydrateFromInsights(startTs);
    if (!ok) setTimeout(() => { if (!hasFeatureData()) hydrateFromLog(); }, 300);
  }
}

  function renderAll() {
    renderProgress();
    renderLanes();
    renderSpotlightSummary();
  }

  // ---------- Polling and optimistic auto-bump ----------
  function tick() {
    const running = !timeline?.done || (summary?.running === true);
    pullSummary();

  // Periodically refresh pairs so enabled lanes remain synchronized
    if ((Date.now() - lastPairsAt) > 10000) { pullPairs().finally(() => { lastPairsAt = Date.now(); renderLanes(); }); }

    if ((tick._lastStatusAt || 0) + 5000 < Date.now()) { pullStatus(); tick._lastStatusAt = Date.now(); }

    if (running) {
      const noUpdateMs = Date.now() - (lastServerUpdate || 0);
      if (optimistic && noUpdateMs > 700 && !(timeline.pre || timeline.post || timeline.done)) {
        const floor = 12;
        const cap   = 60;
        progressPct = clamp((progressPct || floor) + 2, floor, cap);
        renderProgress();
      }
    }

    clearTimeout(tick._t);
    tick._t = setTimeout(tick, running ? 1000 : 2500);
  }

  // ---------- Optimistic start behavior when user clicks Start ----------
  function wireRunButton() {
    const btn = document.getElementById("run");
    if (!btn || wireRunButton._done) return;
    wireRunButton._done = true;

    btn.addEventListener("click", () => {
      optimistic = true;
      lastServerUpdate = 0;
      try {
        if (typeof startRunVisuals === "function") startRunVisuals(true);
        if (typeof recomputeRunDisabled === "function") recomputeRunDisabled();
      } catch (e) {}
      timeline = { start:true, pre:false, post:false, done:false };
      progressPct = Math.max(progressPct, 12);
      renderProgress();
    }, { capture:true });
  }

  // ---------- Legacy bridges (compatibility) ----------
  window.addEventListener("ux:timeline", (e) => {
    const tl = e.detail || {};
    timeline = { start: !!tl.start, pre: !!tl.pre, post: !!tl.post, done: !!tl.done };
    progressPct = asPctFromTimeline(timeline);
    renderProgress();
  });
  window.addEventListener("ux:progress", (e) => {
    const p = e.detail?.pct;
    if (typeof p === "number") { progressPct = clamp(p, 0, 100); renderProgress(); }
  });

  window.UX = {
    updateTimeline: (tl) => window.dispatchEvent(new CustomEvent("ux:timeline", { detail: tl || {} })),
    updateProgress: (payload) => payload && window.dispatchEvent(new CustomEvent("ux:progress", { detail: payload })),
    refresh: () => pullSummary().then(renderAll)
  };

  // ---------- Boot / initialization ----------
  pullPairs(); // initialize enabled lanes
  renderAll();
  wireRunButton();
  tick();
})();
