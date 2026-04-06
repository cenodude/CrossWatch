/* assets/js/main.js */
/* refactored */
/* CrossWatch - Main UI Module */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const cfg = window._cfgCache;
  const key = cfg?.tmdb?.api_key;
  const hasTmdb = typeof key === "string" ? key.trim().length > 0 : !!key;
  if (cfg && (cfg.ui?.show_playingcard === false || !hasTmdb)) {
    document.head.insertAdjacentHTML("beforeend", `<style>#playing-card{display:none!important}</style>`);
  }
})();

(() => {
  const featureMeta = window.CW?.FeatureMeta || {};
  const featureLabel = (key) => featureMeta.label?.(key) || String(key || "");
  const FEATS = [
    ["watchlist", "movie"],
    ["ratings", "star"],
    ["history", "play_arrow"],
    ["progress", "timelapse"],
    ["playlists", "queue_music"]
  ].map(([key, icon]) => ({ key, icon, label: featureLabel(key) }));
  const FEAT_KEYS = FEATS.map((f) => f.key);
  const FEAT_BY_KEY = Object.fromEntries(FEATS.map((f) => [f.key, f]));
  const DEFAULT_ENABLED = { watchlist: true, ratings: true, history: true, progress: false, playlists: true };
  const EMPTY_ENABLED = () => Object.fromEntries(FEAT_KEYS.map((k) => [k, false]));
  const mkLane = () => ({ added: 0, removed: 0, updated: 0, spotAdd: [], spotRem: [], spotUpd: [] });
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const safe = (fn, ...args) => {
    try { return fn?.(...args); } catch {}
  };
  const nowTs = () => Date.now();
  const parseTs = (v) => {
    let n = typeof v === "number" ? v : Date.parse(v || 0);
    if (!Number.isFinite(n)) return 0;
    return n < 1e12 ? n * 1000 : n;
  };
  const last25 = (arr) => arr.slice(-25).reverse();
  const laneHasCounts = (lane) => (+lane?.added || 0) + (+lane?.removed || 0) + (+lane?.updated || 0) > 0;
  const on = (target, names, fn) => names.forEach((name) => target.addEventListener(name, fn));

  const elProgress = document.getElementById("ux-progress");
  const elLanes = document.getElementById("ux-lanes");
  const elSpot = document.getElementById("ux-spotlight");
  if (!elProgress || !elLanes || !elSpot) return;

  const sync = new window.SyncBar({
    el: elProgress,
    onStart: () => safe(window.startRunVisuals, true),
    onStop: () => safe(window.stopRunVisuals)
  });
  window.syncBar = sync;
  (window.CW ||= {}).syncBar = sync;

  let summary = null;
  let enabledFromPairs = null;
  let lastPairsAt = 0;
  let lastSummaryEventAt = 0;
  let renderTO = null;
  let lastRenderAt = 0;
  let finishedForRun = null;
  let prevRunKey = null;
  let sumBusy = false;
  let sumAbort = null;
  let logHydratedForRun = null;
  let pairsRefreshTO = null;
  let spotsModal = null;
  let esSummary = null;
  let esLogs = null;
  let runButtonWired = false;
  const hydratedLanes = Object.create(null);
  const lastCounts = Object.create(null);
  const lastLaneTs = Object.fromEntries(FEAT_KEYS.map((k) => [k, 0]));

  const runKeyOf = (s) => s?.run_id || s?.run_uuid || s?.raw_started_ts || (s?.started_at ? Date.parse(s.started_at) : null);
  const defaultEnabledMap = () => ({ ...DEFAULT_ENABLED });
  const getEnabledMap = () => enabledFromPairs ?? (summary?.enabled || defaultEnabledMap());
  const getDisplayFeats = () => {
    const enabled = getEnabledMap() || defaultEnabledMap();
    const keys = ["watchlist", "ratings", "history"];
    keys.push(enabled.progress ? "progress" : "playlists");
    return keys.map((key) => FEAT_BY_KEY[key]).filter(Boolean);
  };
  const fmtDelta = (a, r, u) => `+${a || 0} / -${r || 0} / ~${u || 0}`;

  const fetchJSON = async (url, fallback = null, signal) => {
    try {
      const r = await fetch(`${url}${url.includes("?") ? "&" : "?"}_ts=${nowTs()}`, { credentials: "same-origin", cache: "no-store", signal });
      return r.ok ? await r.json() : fallback;
    } catch {
      return fallback;
    }
  };

  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  const ensureOpsStatusDock = () => {
    const row = document.querySelector("#ops-card .action-row");
    if (!row || row.querySelector(".cw-status-dock")) return;
    row.appendChild(Object.assign(document.createElement("div"), { className: "cw-status-dock" }));
  };
  ensureOpsStatusDock();
  window.addEventListener("cw:ops-layout-refresh", ensureOpsStatusDock);

  const toInt = (v) => Number.isInteger(v) ? v : typeof v === "number" && Number.isFinite(v) && Math.floor(v) === v ? v : typeof v === "string" && /^\d+$/.test(v.trim()) ? parseInt(v.trim(), 10) : null;
  const titleOf = (x) => {
    if (typeof x === "string") return x;
    if (!x || typeof x !== "object") return "item";
    const key = String(x.key || "");
    const show = String(x.series_title || x.show_title || "").trim();
    const rawTitle = String(x.title || "").trim();
    const looksLikeIdLabel = (v) => {
      const s = String(v || "").trim().toLowerCase();
      if (!s) return false;
      if (key && s === key.trim().toLowerCase()) return true;
      return /^(tmdb|imdb|tvdb|trakt|slug|mdblist|kitsu):/.test(s);
    };
    const mKey = key.match(/#s(\d{1,3})e(\d{1,3})/i);
    const mRaw = rawTitle.match(/^s(\d{1,3})e(\d{1,3})$/i);
    let season = toInt(x.season), episode = toInt(x.episode), type = String(x.type || "").toLowerCase();
    if (mKey) [season, episode] = [season ?? +mKey[1], episode ?? +mKey[2]];
    if (mRaw) [season, episode] = [season ?? +mRaw[1], episode ?? +mRaw[2]];
    if (type === "episode" || mKey || mRaw || (show && season != null && episode != null)) {
      if (show && season != null && episode != null) return `${show} - S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`;
      if (typeof x.display_title === "string" && /S\d{2}E\d{2}/i.test(x.display_title)) return x.display_title.trim();
      if (show) return show;
    }
    if (typeof x.display_title === "string" && x.display_title.trim()) return x.display_title.trim();
    if (type === "season") {
      const seasonLabel = rawTitle || (toInt(x.season) != null ? `Season ${toInt(x.season)}` : "");
      return show && seasonLabel ? `${show} - ${seasonLabel}` : show || seasonLabel || "item";
    }
    if ((type === "show" || type === "anime") && show && looksLikeIdLabel(rawTitle)) return show;
    const title = String(x.title || x.name || "").trim();
    if (show && looksLikeIdLabel(title)) return show;
    return show && title && show.toLowerCase() === title.toLowerCase() ? show : title || x.series_title || x.name || x.key || "item";
  };

  const synthSpots = (items, key) => {
    const arr = Array.isArray(items) ? [...items] : [];
    const tsOf = (it) => {
      const v = it?.ts ?? it?.seen_ts ?? it?.sync_ts ?? it?.ingested_ts ?? it?.watched_at ?? it?.rated_at ?? 0;
      const n = typeof v === "number" ? v : Date.parse(v);
      return Number.isFinite(n) ? n : 0;
    };
    arr.sort((a, b) => tsOf(b) - tsOf(a));
    const out = { a: [], r: [], u: [] };
    for (const it of arr) {
      const act = String(it?.action || it?.op || it?.change || "").toLowerCase();
      const tag = key === "history" && (it?.watched || it?.watched_at || act.includes("watch") || act.includes("scrobble")) ? "a"
        : key === "ratings" && (act.includes("rate") || "rating" in (it || {})) ? "a"
        : key === "progress" && ["progress", "resume", "position", "offset"].some((s) => act.includes(s)) ? "u"
        : key === "playlists" && (act.includes("add") || act.includes("playlist")) ? "a"
        : act.includes("add") ? "a"
        : ["rem", "del", "unwatch"].some((s) => act.includes(s)) ? "r"
        : "u";
      const bucket = out[tag];
      if (bucket.length < 3) bucket.push(titleOf(it));
      if (out.a.length + out.r.length + out.u.length >= 3) break;
    }
    return out;
  };

  const guardLaneOverwrite = (key, payload, ts = nowTs()) => {
    const sum = (+payload.added || 0) + (+payload.removed || 0) + (+payload.updated || 0);
    const prev = hydratedLanes[key];
    const prevSum = prev ? (+prev.added || 0) + (+prev.removed || 0) + (+prev.updated || 0) : 0;
    if (sync.isRunning() && sum === 0 && prevSum > 0) return false;
    if (ts < (lastLaneTs[key] || 0)) return false;
    lastLaneTs[key] = ts;
    return true;
  };

  const getLaneStats = (sum, key) => {
    const f = sum?.features?.[key] || sum?.[key] || {};
    const out = {
      added: (f.added ?? 0) | 0,
      removed: (f.removed ?? 0) | 0,
      updated: (f.updated ?? 0) | 0,
      items: Array.isArray(f.items) ? f.items : [],
      spotAdd: Array.isArray(f.spotlight_add) ? f.spotlight_add : [],
      spotRem: Array.isArray(f.spotlight_remove) ? f.spotlight_remove : [],
      spotUpd: Array.isArray(f.spotlight_update) ? f.spotlight_update : []
    };
    const prev = hydratedLanes[key];
    const prevHasSpots = !!((prev?.spotAdd?.length || 0) + (prev?.spotRem?.length || 0) + (prev?.spotUpd?.length || 0));
    const outHasSpots = !!(out.spotAdd.length + out.spotRem.length + out.spotUpd.length);
    if (!out.added && !out.removed && !out.updated && hydratedLanes[key] && sync.isRunning()) return { ...hydratedLanes[key] };
    if (!outHasSpots && prevHasSpots) {
      out.spotAdd = Array.isArray(prev?.spotAdd) ? prev.spotAdd : out.spotAdd;
      out.spotRem = Array.isArray(prev?.spotRem) ? prev.spotRem : out.spotRem;
      out.spotUpd = Array.isArray(prev?.spotUpd) ? prev.spotUpd : out.spotUpd;
    }
    if (!out.spotAdd.length && !out.spotRem.length && !out.spotUpd.length && out.items.length) {
      const s = synthSpots(out.items, key);
      out.spotAdd = s.a; out.spotRem = s.r; out.spotUpd = s.u;
    }
    if (guardLaneOverwrite(key, out)) hydratedLanes[key] = out;
    return out;
  };

  const laneState = (key) => {
    if (!getEnabledMap()[key]) return "skip";
    if (summary?.exit_code != null && summary.exit_code !== 0) return "err";
    return sync.isRunning() ? "run" : sync.state().timeline.done ? "ok" : "skip";
  };

  const ensureSpotsModal = () => {
    if (spotsModal) return spotsModal;
    spotsModal = Object.assign(document.createElement("div"), { id: "ux-spots-modal", className: "ux-spots-modal hidden" });
    spotsModal.innerHTML = `<div class="ux-spots-backdrop"></div><div class="ux-spots-card"><div class="ux-spots-h"><div class="ux-spots-title"></div><button class="ux-spots-close" aria-label="Close">✕</button></div><div class="ux-spots-body"></div></div>`;
    document.body.appendChild(spotsModal);
    const close = () => spotsModal?.classList.add("hidden");
    spotsModal.querySelector(".ux-spots-close").onclick = close;
    spotsModal.querySelector(".ux-spots-backdrop").onclick = close;
    document.addEventListener("keydown", (e) => e.key === "Escape" && close());
    return spotsModal;
  };

  const openSpotsModal = (label, buckets) => {
    const modal = ensureSpotsModal();
    const body = modal.querySelector(".ux-spots-body");
    const meta = { add: ["t-add", "Added", "mdi mdi-plus"], rem: ["t-rem", "Removed", "mdi mdi-delete-outline"], upd: ["t-upd", "Updated", "mdi mdi-sync"] };
    const stampOf = (it) => !it || typeof it === "string" ? 0 : parseTs(it.added_at ?? it.listed_at ?? it.watched_at ?? it.rated_at ?? it.last_watched_at ?? it.user_rated_at ?? it.ts ?? it.seen_ts ?? it.ingested_ts ?? it.sync_ts ?? 0);
    const all = ["add", "rem", "upd"].flatMap((kind) => (buckets[kind] || []).map((it) => typeof it === "object" && it ? { ...it, __kind: kind } : { title: it, __kind: kind }));
    const rows = all.sort((a, b) => stampOf(b) - stampOf(a)).slice(0, 25);
    const hasRem = (buckets.rem || []).length > 0;
    const columns = hasRem ? [rows.filter((it) => it.__kind !== "rem"), rows.filter((it) => it.__kind === "rem")] : [rows.filter((it) => it.__kind !== "rem")];
    const mkCol = (items) => !items.length ? `<div class="muted small">No items.</div>` : items.map((it) => {
      const [cls, text, icon] = meta[it.__kind] || meta.add;
      const d = stampOf(it) ? new Date(stampOf(it)).toLocaleDateString() : "";
      return `<div class="ux-sec-row"><span class="tag ${cls}"><i class="${icon}"></i> ${text}</span><span class="ux-title">${esc(titleOf(it))}</span>${d ? `<span class="ux-date">${d}</span>` : ""}</div>`;
    }).join("");
    modal.querySelector(".ux-spots-title").textContent = `${label} - last 25`;
    body.classList.toggle("single", !hasRem);
    body.innerHTML = columns.map((items, i) => `<div class="ux-col ${hasRem ? i ? "ux-col-rem" : "ux-col-add" : "ux-col-full"}">${mkCol(items)}</div>`).join("");
    modal.classList.remove("hidden");
  };

  const createSpotRow = (kind, text) => {
    const row = Object.assign(document.createElement("div"), { className: "spot" });
    row.innerHTML = `<span class="tag ${kind === "add" ? "t-add" : kind === "rem" ? "t-rem" : "t-upd"}">${kind === "add" ? "Added" : kind === "rem" ? "Removed" : "Updated"}</span><span></span>`;
    row.lastChild.textContent = text;
    return row;
  };

  function renderLanes() {
    const wrap = Object.assign(document.createElement("div"), { className: "lanes" });
    const running = sync.isRunning();
    for (const feat of getDisplayFeats()) {
      const enabled = !!getEnabledMap()[feat.key];
      const { added, removed, updated, items, spotAdd, spotRem, spotUpd } = getLaneStats(summary || {}, feat.key);
      const lane = Object.assign(document.createElement("div"), { className: `lane${enabled ? "" : " disabled"}` });
      const total = (added || 0) + (removed || 0) + (updated || 0);
      if (running && enabled && total > (lastCounts[feat.key] ?? 0)) {
        lane.classList.add("shake");
        setTimeout(() => lane.classList.remove("shake"), 450);
      }
      lastCounts[feat.key] = total;

      const chipState = laneState(feat.key);
      const header = Object.assign(document.createElement("div"), { className: "lane-h" });
      header.innerHTML = `<div class="lane-ico"><span class="material-symbols-outlined material-symbol material-icons">${feat.icon}</span></div><div class="lane-title">${feat.label}</div><div class="lane-badges"><span class="delta"><b>${fmtDelta(added, removed, updated)}</b></span><span class="chip ${chipState}">${!enabled ? "Disabled" : chipState === "err" ? "Failed" : chipState === "ok" ? "Synced" : chipState === "run" ? "Running" : "Skipped"}</span></div>`;
      lane.appendChild(header);

      const body = Object.assign(document.createElement("div"), { className: "lane-body" });
      const spots = [...(spotAdd || []).slice(0, 2).map((x) => ["add", titleOf(x)]), ...(spotRem || []).slice(0, 2).map((x) => ["rem", titleOf(x)]), ...(spotUpd || []).slice(0, 2).map((x) => ["upd", titleOf(x)])];
      if (!spots.length && items?.length) {
        const s = synthSpots(items, feat.key);
        spots.push(...s.a.slice(0, 2).map((x) => ["add", x]), ...s.r.slice(0, 2).map((x) => ["rem", x]), ...s.u.slice(0, 2).map((x) => ["upd", x]));
      }

      const cappedTotal = Math.min(25, total);
      if (!enabled || !cappedTotal || !spots.length) {
        let emptyText = "Awaiting results…";
        if (!enabled) emptyText = "Feature not configured";
        else if (cappedTotal > 0) emptyText = updated && !added && !removed ? `${updated} updated` : `${cappedTotal} change${cappedTotal === 1 ? "" : "s"}`;
        else if (sync.state().timeline.done) emptyText = "No changes";
        body.appendChild(Object.assign(document.createElement("div"), { className: "spot muted small", textContent: emptyText }));
      } else {
        let lastRow = null;
        for (const [kind, text] of spots.slice(0, Math.min(3, cappedTotal))) {
          lastRow = createSpotRow(kind, text);
          body.appendChild(lastRow);
        }
        const moreCount = Math.max(0, cappedTotal - Math.min(Math.min(3, cappedTotal), spots.length));
        if (moreCount > 0 && lastRow) {
          const more = Object.assign(document.createElement("span"), { className: "chip more", textContent: `+${moreCount} more`, title: "Show recent items" });
          more.style.marginLeft = "auto";
          more.addEventListener("click", (ev) => {
            ev.stopPropagation();
            openSpotsModal(feat.label, { add: spotAdd, rem: spotRem, upd: spotUpd });
          });
          lastRow.appendChild(more);
        }
      }

      lane.appendChild(body);
      wrap.appendChild(lane);
    }
    elLanes.replaceChildren(wrap);
  }

  const renderAll = () => { renderLanes(); elSpot.textContent = ""; };

  async function pullPairs() {
    const arr = await fetchJSON("/api/pairs", null);
    if (!Array.isArray(arr)) return;
    if (!arr.length) return void (enabledFromPairs = EMPTY_ENABLED());
    const enabled = EMPTY_ENABLED();
    for (const pair of arr) {
      const feats = pair?.features || {};
      for (const key of FEAT_KEYS) if (feats[key] && (feats[key].enable === true || feats[key].enabled === true)) enabled[key] = true;
    }
    enabledFromPairs = enabled;
  }

  const queuePairsRefresh = () => {
    clearTimeout(pairsRefreshTO);
    pairsRefreshTO = setTimeout(() => {
      pullPairs().finally(() => {
        lastPairsAt = nowTs();
        renderLanes();
      });
    }, 250);
  };
  window.addEventListener("cx:pairs:changed", queuePairsRefresh);
  document.addEventListener("config-saved", queuePairsRefresh);
  window.addEventListener("sync-complete", queuePairsRefresh);

  const needsSpotlights = (s) => {
    const feats = s?.features;
    if (!feats) return false;
    return getDisplayFeats().some(({ key }) => {
      const lane = feats[key] || {};
      return laneHasCounts(lane) && !((lane.spotlight_add?.length || 0) + (lane.spotlight_remove?.length || 0) + (lane.spotlight_update?.length || 0));
    });
  };

  const queueLogHydration = (runKey) => {
    if (!runKey) return;
    setTimeout(() => {
      if (logHydratedForRun !== runKey) {
        logHydratedForRun = runKey;
        hydrateFromLog();
      }
    }, 300);
  };

  function hydrateFromLog() {
    const txt = document.getElementById("det-log")?.innerText || document.getElementById("det-log")?.textContent || "";
    if (!txt) return false;
    const tallies = Object.create(null);
    const ensureLane = (k) => (tallies[k] ||= mkLane());
    const mapFeat = (s) => {
      const f = String(s || "").trim().toLowerCase();
      return !f ? "" : f === "watch" || f === "watched" ? "history" : f;
    };

    let lastFeatHint = "";
    for (const raw of txt.split(/\n+/).slice(-800)) {
      const idx = raw.indexOf("{");
      if (idx < 0) {
        const m = raw.match(/feature["']?\s*:\s*"?(\w+)"?/i);
        if (m) lastFeatHint = mapFeat(m[1]) || lastFeatHint;
        continue;
      }
      let obj;
      try { obj = JSON.parse(raw.slice(idx)); } catch { continue; }
      if (!obj?.event) continue;

      const feat = mapFeat(obj.feature);
      if (feat) lastFeatHint = feat;
      if (obj.event === "snapshot:progress" || obj.event === "progress:snapshot") sync.snap({ done: obj.done, total: obj.total, final: !!obj.final, dst: obj.dst, feature: obj.feature });
      if (/^apply:/.test(obj.event || "")) {
        if (/:start$/.test(obj.event)) sync.applyStart({ feature: lastFeatHint, total: obj.total });
        if (/:progress$/.test(obj.event)) sync.applyProg({ feature: lastFeatHint, done: obj.done, total: obj.total });
        if (/:done$/.test(obj.event)) sync.applyDone({ feature: lastFeatHint, count: obj.result?.count || obj.count });
        const lane = ensureLane(lastFeatHint || feat || obj.feature);
        const cnt = +(obj.result?.count || obj.count || 0);
        let bucket = null;
        if (/^apply:add:done$/.test(obj.event)) { lane.added += cnt; bucket = lane.spotAdd; }
        else if (/^apply:remove:done$/.test(obj.event)) { lane.removed += cnt; bucket = lane.spotRem; }
        else if (/^apply:update:done$/.test(obj.event)) { lane.updated += cnt; bucket = lane.spotUpd; }
        if (bucket && Array.isArray(obj.spotlight)) {
          const seen = new Set(bucket.map((it) => JSON.stringify(it)));
          for (const item of obj.spotlight) {
            const sig = JSON.stringify(item);
            if (seen.has(sig)) continue;
            bucket.push(item);
            seen.add(sig);
            if (bucket.length >= 25) break;
          }
        }
      }
      if (obj.event === "spotlight" && (feat || obj.feature) && obj.action && obj.title) {
        const lane = ensureLane(feat || lastFeatHint || obj.feature);
        const act = String(obj.action).toLowerCase();
        if (act === "add" && lane.spotAdd.length < 25) lane.spotAdd.push(obj.title);
        if (act === "remove" && lane.spotRem.length < 25) lane.spotRem.push(obj.title);
        if (act === "update" && lane.spotUpd.length < 25) lane.spotUpd.push(obj.title);
      }
    }
    if (!Object.keys(tallies).length) return false;

    summary ||= {}; summary.features ||= {};
    for (const [feat, lane] of Object.entries(tallies)) {
      const prev = summary.features[feat] || {};
      const sa = prev.spotlight_add?.length ? prev.spotlight_add.slice(0, 25) : last25(lane.spotAdd);
      const sr = prev.spotlight_remove?.length ? prev.spotlight_remove.slice(0, 25) : last25(lane.spotRem);
      const su = prev.spotlight_update?.length ? prev.spotlight_update.slice(0, 25) : last25(lane.spotUpd);
      const merged = {
        added: Math.max(prev.added || 0, lane.added || 0),
        removed: Math.max(prev.removed || 0, lane.removed || 0),
        updated: Math.max(prev.updated || 0, lane.updated || 0),
        spotlight_add: sa,
        spotlight_remove: sr,
        spotlight_update: su
      };
      if (!guardLaneOverwrite(feat, merged)) continue;
      summary.features[feat] = merged;
      hydratedLanes[feat] = { added: merged.added, removed: merged.removed, updated: merged.updated, items: [], spotAdd: merged.spotlight_add || [], spotRem: merged.spotlight_remove || [], spotUpd: merged.spotlight_update || [] };
    }
    summary.enabled = { ...defaultEnabledMap(), ...(summary.enabled || {}) };
    renderAll();
    return true;
  }

  const runBtn = () => document.getElementById("run");
  const setRunButtonState = (running) => {
    const btn = runBtn();
    if (!btn) return;
    btn.toggleAttribute("disabled", !!running);
    if (!running) safe(window.recomputeRunDisabled);
    btn.setAttribute("aria-busy", running ? "true" : "false");
    btn.classList.toggle("glass", !!running);
    btn.title = running ? "Synchronization running…" : "Run synchronization";
  };

  const wireRunButton = () => {
    const btn = runBtn();
    if (!btn || runButtonWired) return;
    runButtonWired = true;
    btn.addEventListener("click", () => {
      if (!btn.disabled && !btn.classList.contains("glass")) setRunButtonState(true);
    }, { capture: true });
  };

  const scheduleRender = () => {
    const now = nowTs();
    if (now - lastRenderAt < 200) {
      clearTimeout(renderTO);
      return void (renderTO = setTimeout(() => {
        lastRenderAt = nowTs();
        renderAll();
      }, 200));
    }
    lastRenderAt = now;
    renderAll();
  };

  function applySummarySnapshot(s, source) {
    if (!s) return;
    lastSummaryEventAt = nowTs();
    const runKey = runKeyOf(s) || "_";
    if (runKey !== prevRunKey) {
      finishedForRun = null;
      prevRunKey = runKey;
      logHydratedForRun = null;
    }

    const { running, justFinished } = sync.fromSummary(s);
    summary = s;
    setRunButtonState(running);

    if (justFinished && finishedForRun !== runKey) {
      finishedForRun = runKey;
      if ("_optimistic" in sync) sync._optimistic = false;
      safe(window.updatePreviewVisibility);
      safe(window.refreshSchedulingBanner);
      safe(window.Insights?.refreshInsights || window.refreshInsights);
      try {
        if (needsSpotlights(s)) queueLogHydration(runKey);
      } catch {}
      try {
        window.wallLoaded = false;
        if (typeof window.updateWatchlistPreview === "function") window.updateWatchlistPreview();
        else if (typeof window.updatePreviewVisibility === "function") window.updatePreviewVisibility();
        else if (typeof window.loadWatchlist === "function") window.loadWatchlist();
      } catch {}
      try {
        window.dispatchEvent(new CustomEvent("sync-complete", { detail: { at: nowTs(), summary: s, source: source || "?" } }));
      } catch {}
    }

    if (!summary.enabled) summary.enabled = defaultEnabledMap();
    scheduleRender();

    if (!sync.state().timeline.done) return;
    const hasFeatures = summary?.features && Object.values(summary.features).some((v) => laneHasCounts(v) || v?.spotlight_add?.length || v?.spotlight_remove?.length || v?.spotlight_update?.length);
    if (needsSpotlights(summary)) {
      queueLogHydration(runKey);
    } else if (!hasFeatures) {
      queueLogHydration(runKey);
    }
  }

  const pullSummary = async () => {
    if (authSetupPending()) return;
    if (sumBusy) return;
    sumBusy = true;
    try {
      safe(sumAbort?.abort?.bind(sumAbort));
      sumAbort = new AbortController();
      const snap = await fetchJSON("/api/run/summary", null, sumAbort.signal);
      if (snap) applySummarySnapshot(snap, "poll");
    } finally {
      sumBusy = false;
    }
  };

  window.openSummaryStream = function openSummaryStream() {
    try {
      if (authSetupPending()) {
        safe(esSummary?.close?.bind(esSummary));
        esSummary = null;
        window.esSum = null;
        return;
      }
      safe(esSummary?.close?.bind(esSummary));
      const url = new URL("/api/run/summary/stream", document.baseURI);
      url.searchParams.set("_ts", String(nowTs()));
      esSummary = new EventSource(url.toString());
      window.esSum = esSummary;
      esSummary.onmessage = (ev) => {
        try { applySummarySnapshot(JSON.parse(ev.data || "{}"), "sse"); } catch {}
      };
      on(esSummary, ["run:start", "run:pair", "feature:start"], () => safe(sync.markInit?.bind(sync)));
      on(esSummary, ["one:plan", "two:plan"], (e) => {
        try { sync.setPair(JSON.parse(e.data || "{}")); } catch {}
      });
      on(esSummary, ["progress:snapshot", "snapshot:progress"], (e) => {
        try { sync.snap(JSON.parse(e.data || "{}")); } catch {}
      });
      on(esSummary, ["progress:apply", "apply:add:progress", "apply:update:progress", "apply:remove:progress"], (e) => {
        try { sync.applyProg(JSON.parse(e.data || "{}")); } catch {}
      });
      on(esSummary, ["apply:add:start", "apply:update:start", "apply:remove:start"], (e) => {
        try { sync.applyStart(JSON.parse(e.data || "{}")); } catch {}
      });
      on(esSummary, ["apply:add:done", "apply:update:done", "apply:remove:done"], (e) => {
        try { sync.applyDone(JSON.parse(e.data || "{}")); } catch {}
      });
      on(esSummary, ["run:error", "run:aborted"], () => {
        try { sync.error(); setRunButtonState(false); } catch {}
      });
      esSummary.onerror = () => {
        safe(esSummary?.close?.bind(esSummary));
        esSummary = null;
        window.esSum = null;
        if (authSetupPending()) return;
        setTimeout(openSummaryStream, 2000);
      };
    } catch {}
  };

  window.openLogStream = function openLogStream() {
    try {
      safe(esLogs?.close?.bind(esLogs));
      esLogs = null;
      window.esLogs = null;
    } catch {}
  };

  window.UX = {
    updateTimeline: (tl) => sync.updateTimeline(tl || {}),
    updateProgress: (payload) => payload?.pct != null && sync.updatePct(payload.pct),
    refresh: () => pullSummary().then(renderAll)
  };

  window.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      if (authSetupPending()) return;
      openSummaryStream();
      openLogStream();
    }
  });

  window.addEventListener("auth-changed", () => {
    if (authSetupPending()) return;
    openSummaryStream();
    openLogStream();
    pullSummary().then(renderAll);
  });

  window.addEventListener("cw-auth-setup-pending", (ev) => {
    if (ev?.detail?.pending) {
      try { esSummary?.close?.(); } catch {}
      esSummary = null;
      window.esSum = null;
    }
  });

  async function tick() {
    if (authSetupPending()) {
      try { esSummary?.close?.(); } catch {}
      esSummary = null;
      window.esSum = null;
      clearTimeout(tick._t);
      tick._t = setTimeout(tick, 4000);
      return;
    }
    const now = nowTs();
    const running = sync.isRunning();
    const sseUp = esSummary?.readyState === 1;
    if (!sseUp || !summary || (running && now - lastSummaryEventAt > 15000)) await pullSummary();
    if (now - lastPairsAt > 120000) queuePairsRefresh();
    if (running && !sync.state().timeline.pre && !sync.state().timeline.post && now - sync._lastPhaseAt > 900) {
      sync.updatePct(Math.min((sync._pctMemo || 0) + 2, 24));
    }
    if (now - sync.lastEvent() > 20000) {
      openSummaryStream();
      openLogStream();
    }
    clearTimeout(tick._t);
    tick._t = setTimeout(tick, running ? 2000 : 6000);
  }

  renderAll();
  wireRunButton();
  openSummaryStream();
  openLogStream();
  queuePairsRefresh();
  tick();
})();

(() => {
  document.getElementById("preview-guard-css")?.remove();
  document.head.appendChild(Object.assign(document.createElement("style"), { id: "preview-guard-css", textContent: `html[data-tab!="main"] #placeholder-card{display:none!important;}` }));

  const DOC = document.documentElement;
  DOC.dataset.tab ||= "main";
  const isMain = () => DOC.dataset.tab === "main";
  const hidePreview = () => document.getElementById("placeholder-card")?.classList.add("hidden");
  const guard = (name) => {
    const orig = window[name];
    if (typeof orig !== "function") return;
    window[name] = function (...args) {
      if (!isMain()) return hidePreview();
      return orig.apply(this, args);
    };
  };

  const showTab = window.showTab;
  window.showTab = function (name) {
    const ret = typeof showTab === "function" ? showTab.apply(this, arguments) : undefined;
    try {
      DOC.dataset.tab = name || "main";
    } catch {}
    return ret;
  };

  ["updateWatchlistPreview", "updatePreviewVisibility", "loadWatchlist"].forEach(guard);
  document.addEventListener("tab-changed", () => !isMain() && hidePreview());
  document.addEventListener("visibilitychange", () => document.visibilityState === "visible" && !isMain() && hidePreview());
})();

(() => {
  const ROOT_ID = "cw-quick-add";
  const STYLE_ID = "cw-quick-add-style";
  const SESSION_KEY = "cw.quick_add.desktop_peek.v1";
  const DESKTOP_POS_KEY = "cw.quick_add.desktop_top.v1";
  const MOBILE_POS_KEY = "cw.quick_add.mobile_bottom.v1";
  const DOC = document.documentElement;
  let root = null;
  let closeTimer = 0;
  let peekTimer = 0;
  let dragState = null;

  const currentTab = () => String(DOC.dataset.tab || document.body?.dataset?.tab || "main").trim().toLowerCase();
  const onMainTab = () => currentTab() === "main";
  const uiCfg = () => (window._cfgCache && typeof window._cfgCache === "object" ? window._cfgCache.ui || {} : {});
  const desktopEnabled = () => uiCfg().show_quick_add_desktop !== false;
  const mobileEnabled = () => uiCfg().show_quick_add_mobile !== false;
  const canOpen = () => typeof window.openManualWatchedModal === "function";
  const hasTmdbMetadata = () => {
    const cfg = window._cfgCache && typeof window._cfgCache === "object" ? window._cfgCache : {};
    const fromBlock = (blk) => {
      if (!blk || typeof blk !== "object") return false;
      if (String(blk.api_key || "").trim()) return true;
      const insts = blk.instances;
      if (!insts || typeof insts !== "object") return false;
      return Object.values(insts).some((value) => value && typeof value === "object" && String(value.api_key || "").trim());
    };
    return fromBlock(cfg.tmdb);
  };
  const clamp = (n, min, max) => Math.min(max, Math.max(min, n));
  const readStoredNumber = (key, fallback) => {
    try {
      const raw = window.localStorage.getItem(key);
      const val = Number.parseFloat(raw || "");
      return Number.isFinite(val) ? val : fallback;
    } catch {
      return fallback;
    }
  };
  const writeStoredNumber = (key, value) => {
    try { window.localStorage.setItem(key, String(value)); } catch {}
  };
  const isMobileLayout = () => {
    try {
      if (window.matchMedia?.("(max-width: 860px)")?.matches) return true;
      if (window.matchMedia?.("(pointer: coarse)")?.matches) return true;
    } catch {}
    return false;
  };

  const ensureStyle = () => {
    if (document.getElementById(STYLE_ID)) return;
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = `
#${ROOT_ID}{position:fixed;z-index:70;pointer-events:none}
#${ROOT_ID}.hidden{display:none!important}
#${ROOT_ID} .cw-qa-shell{pointer-events:auto}
#${ROOT_ID} .cw-qa-desktop{position:fixed;right:0;top:var(--cw-qa-desktop-top,66%);transform:translateY(-50%);display:flex;align-items:center;justify-content:flex-end;filter:drop-shadow(0 14px 26px rgba(0,0,0,.28))}
#${ROOT_ID} .cw-qa-tab{width:28px;height:60px;padding:0 14px;border-radius:18px 0 0 18px;border:1px solid rgba(255,255,255,.08);border-right:0;background:linear-gradient(180deg,rgba(20,24,34,.98),rgba(7,9,14,.99));color:#eef2ff;display:flex;align-items:center;justify-content:flex-end;gap:12px;cursor:pointer;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.05),0 8px 20px rgba(0,0,0,.16);transition:width .2s ease,background .18s ease,box-shadow .18s ease,border-color .18s ease}
#${ROOT_ID} .cw-qa-tab:hover,#${ROOT_ID} .cw-qa-tab:focus-visible{background:linear-gradient(180deg,rgba(24,28,40,.985),rgba(10,12,18,.995));border-color:rgba(255,255,255,.12);box-shadow:inset 0 1px 0 rgba(255,255,255,.06),0 10px 24px rgba(0,0,0,.2);outline:none}
#${ROOT_ID} .cw-qa-tab .cw-qa-grip{font-size:17px;opacity:.38;margin-left:2px;color:rgba(214,221,240,.72)}
#${ROOT_ID} .cw-qa-tab-text{font-size:14px;font-weight:850;letter-spacing:.015em;white-space:nowrap;color:rgba(241,244,252,.94);opacity:0;transform:translateX(-6px);transition:opacity .16s ease,transform .18s ease}
#${ROOT_ID}.is-open .cw-qa-tab,#${ROOT_ID}.is-peek .cw-qa-tab{width:156px}
#${ROOT_ID}.is-open .cw-qa-tab-text,#${ROOT_ID}.is-peek .cw-qa-tab-text{opacity:1;transform:translateX(0)}
#${ROOT_ID} .cw-qa-fab{position:fixed;right:18px;bottom:var(--cw-qa-mobile-bottom,22px);min-width:0;height:50px;padding:0 16px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(18,22,30,.98),rgba(8,10,16,.995));color:#f3f6ff;display:inline-flex;align-items:center;gap:9px;font-size:14px;font-weight:850;box-shadow:0 14px 28px rgba(0,0,0,.28),inset 0 1px 0 rgba(255,255,255,.07);cursor:pointer;pointer-events:auto}
#${ROOT_ID} .cw-qa-fab:hover,#${ROOT_ID} .cw-qa-fab:focus-visible{background:linear-gradient(180deg,rgba(22,26,36,.99),rgba(9,11,17,.998));border-color:rgba(255,255,255,.12);outline:none}
#${ROOT_ID} .cw-qa-fab .material-symbols-rounded{font-size:20px;line-height:1;color:rgba(226,232,246,.9)}
#${ROOT_ID}:not(.is-desktop) .cw-qa-desktop{display:none}
#${ROOT_ID}:not(.is-mobile) .cw-qa-fab{display:none}
#${ROOT_ID}.is-dragging .cw-qa-tab,#${ROOT_ID}.is-dragging .cw-qa-fab{cursor:grabbing;transition:none}
@media (prefers-reduced-motion:reduce){
  #${ROOT_ID} .cw-qa-tab,#${ROOT_ID} .cw-qa-tab-text{transition:none}
}
    `;
    document.head.appendChild(style);
  };

  const ensureRoot = () => {
    if (root && root.isConnected) return root;
    ensureStyle();
    root = document.createElement("div");
    root.id = ROOT_ID;
    root.className = "hidden";
    root.innerHTML = `
      <div class="cw-qa-shell cw-qa-desktop" aria-hidden="true">
        <button type="button" class="cw-qa-tab" aria-label="Open quick add item" title="Click to Quick Add, drag to move">
          <span class="cw-qa-tab-text">Quick Add</span>
          <span class="material-symbols-rounded cw-qa-grip" aria-hidden="true">drag_indicator</span>
        </button>
      </div>
      <button type="button" class="cw-qa-shell cw-qa-fab" aria-label="Quick Add item" title="Click to Quick Add, drag to move">
        <span class="material-symbols-rounded" aria-hidden="true">add</span>
        <span>Quick Add</span>
      </button>
    `;
    document.body.appendChild(root);

    const desktop = root.querySelector(".cw-qa-desktop");
    const tabBtn = root.querySelector(".cw-qa-tab");
    const fabBtn = root.querySelector(".cw-qa-fab");
    const gripBtn = root.querySelector(".cw-qa-grip");
    let suppressClickUntil = 0;
    const applyStoredPosition = () => {
      root.style.setProperty("--cw-qa-desktop-top", `${clamp(readStoredNumber(DESKTOP_POS_KEY, 66), 18, 88)}%`);
      root.style.setProperty("--cw-qa-mobile-bottom", `${clamp(readStoredNumber(MOBILE_POS_KEY, 22), 16, 120)}px`);
    };
    const openModal = () => {
      if (typeof window.openManualWatchedModal === "function") window.openManualWatchedModal();
    };
    const openDrawer = () => {
      clearTimeout(closeTimer);
      root.classList.add("is-open");
      root.classList.remove("is-peek");
    };
    const queueClose = () => {
      clearTimeout(closeTimer);
      closeTimer = window.setTimeout(() => {
        if (desktop?.contains(document.activeElement)) return;
        root.classList.remove("is-open");
      }, 180);
    };
    const clearDrag = () => {
      dragState = null;
      root.classList.remove("is-dragging");
    };
    const beginDrag = (event, kind) => {
      if (!event.isPrimary) return;
      const target = kind === "desktop" ? desktop : fabBtn;
      if (!target) return;
      dragState = { kind, startX: event.clientX, startY: event.clientY, moved: false, pointerId: event.pointerId };
      target.setPointerCapture?.(event.pointerId);
      clearTimeout(closeTimer);
    };
    const moveDrag = (event) => {
      if (!dragState || event.pointerId !== dragState.pointerId) return;
      const dy = event.clientY - dragState.startY;
      const dx = event.clientX - dragState.startX;
      if (!dragState.moved && Math.abs(dy) < 4 && Math.abs(dx) < 4) return;
      if (!dragState.moved) root.classList.add("is-dragging");
      dragState.moved = true;
      if (dragState.kind === "desktop") {
        const pct = clamp((event.clientY / Math.max(window.innerHeight || 1, 1)) * 100, 18, 88);
        root.style.setProperty("--cw-qa-desktop-top", `${pct}%`);
      } else {
        const bottom = clamp((window.innerHeight || 0) - event.clientY - 28, 16, 120);
        root.style.setProperty("--cw-qa-mobile-bottom", `${bottom}px`);
      }
    };
    const endDrag = (event) => {
      if (!dragState || event.pointerId !== dragState.pointerId) return;
      const { kind, moved } = dragState;
      desktop?.releasePointerCapture?.(event.pointerId);
      fabBtn?.releasePointerCapture?.(event.pointerId);
      clearDrag();
      if (moved) {
        suppressClickUntil = Date.now() + 250;
        if (kind === "desktop") writeStoredNumber(DESKTOP_POS_KEY, parseFloat(root.style.getPropertyValue("--cw-qa-desktop-top")) || 66);
        else writeStoredNumber(MOBILE_POS_KEY, parseFloat(root.style.getPropertyValue("--cw-qa-mobile-bottom")) || 22);
        root.classList.remove("is-open", "is-peek");
      }
    };
    const onOpenClick = (event) => {
      if (Date.now() < suppressClickUntil) {
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      openModal();
    };

    applyStoredPosition();
    desktop?.addEventListener("mouseenter", openDrawer);
    desktop?.addEventListener("mouseleave", queueClose);
    desktop?.addEventListener("focusin", openDrawer);
    desktop?.addEventListener("focusout", () => window.setTimeout(() => {
      if (desktop?.contains(document.activeElement)) return;
      queueClose();
    }, 0));
    tabBtn?.addEventListener("click", onOpenClick);
    gripBtn?.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      event.stopPropagation();
      beginDrag(event, "desktop");
    });
    window.addEventListener("pointermove", moveDrag);
    window.addEventListener("pointerup", endDrag);
    window.addEventListener("pointercancel", endDrag);
    fabBtn?.addEventListener("pointerdown", (event) => beginDrag(event, "mobile"));
    fabBtn?.addEventListener("click", onOpenClick);
    window.addEventListener("blur", clearDrag);
    return root;
  };

  const maybePeek = () => {
    if (!root || !root.classList.contains("is-desktop") || root.classList.contains("hidden")) return;
    try {
      if (sessionStorage.getItem(SESSION_KEY) === "1") return;
      sessionStorage.setItem(SESSION_KEY, "1");
    } catch {}
    clearTimeout(peekTimer);
    root.classList.add("is-peek");
    peekTimer = window.setTimeout(() => root?.classList.remove("is-peek"), 2200);
  };

  const syncVisibility = () => {
    const el = ensureRoot();
    const mobile = isMobileLayout();
    const tmdbReady = hasTmdbMetadata();
    const showDesktop = !mobile && onMainTab() && desktopEnabled() && tmdbReady;
    const showMobile = mobile && onMainTab() && mobileEnabled() && tmdbReady;
    const showAny = showDesktop || showMobile;
    el.classList.toggle("hidden", !showAny);
    el.classList.toggle("is-desktop", showDesktop);
    el.classList.toggle("is-mobile", showMobile);
    el.classList.toggle("is-ready", canOpen());
    if (!showDesktop) el.classList.remove("is-open", "is-peek");
    if (showDesktop) maybePeek();
  };

  const refreshSoon = () => syncVisibility();
  window.addEventListener("resize", syncVisibility, { passive: true });
  document.addEventListener("visibilitychange", () => document.visibilityState === "visible" && syncVisibility());
  document.addEventListener("tab-changed", syncVisibility);
  document.addEventListener("config-saved", syncVisibility);
  window.addEventListener("load", syncVisibility, { once: true });
  window.setInterval(syncVisibility, 1500);
  refreshSoon();
})();
