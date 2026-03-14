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
  let insightsTriedForRun = null;
  let logHydratedForRun = null;
  let pairsRefreshTO = null;
  let spotsModal = null;
  let esSummary = null;
  let esLogs = null;
  let runButtonWired = false;
  const hydratedLanes = Object.create(null);
  const lastCounts = Object.create(null);
  const lastLaneTs = Object.fromEntries(FEAT_KEYS.map((k) => [k, 0]));
  const insightsRetry = { runKey: null, tries: 0, t: null };
  const INSIGHTS_RETRY_DELAYS = [0, 800, 2000, 5000];

  const runKeyOf = (s) => s?.run_id || s?.run_uuid || s?.raw_started_ts || (s?.started_at ? Date.parse(s.started_at) : null);
  const defaultEnabledMap = () => ({ ...DEFAULT_ENABLED });
  const getEnabledMap = () => enabledFromPairs ?? (summary?.enabled || defaultEnabledMap());
  const getDisplayFeats = () => (enabledFromPairs?.progress ? [FEAT_BY_KEY.watchlist, FEAT_BY_KEY.ratings, FEAT_BY_KEY.history, FEAT_BY_KEY.progress] : [FEAT_BY_KEY.watchlist, FEAT_BY_KEY.ratings, FEAT_BY_KEY.history, FEAT_BY_KEY.playlists]);
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
    const title = String(x.title || x.name || "").trim();
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
    if (!out.added && !out.removed && !out.updated && hydratedLanes[key] && sync.isRunning()) return { ...hydratedLanes[key] };
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
        body.appendChild(Object.assign(document.createElement("div"), { className: "spot muted small", textContent: !enabled ? "Feature not configured" : sync.state().timeline.done ? "No changes" : "Awaiting results…" }));
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

  const inferInsightFeature = (e) => {
    const f = String(e.feature || e.lane || e.kind || "").toLowerCase();
    if (f) return f;
    const act = String(e.action || "").toLowerCase();
    if (act.includes("watch") || act.includes("scrobble")) return "history";
    if (act.includes("rate") || "rating" in (e || {})) return "ratings";
    if (["progress", "resume", "position", "offset"].some((s) => act.includes(s)) || "progress_ms" in (e || {}) || "playback_position" in (e || {})) return "progress";
    if (act.includes("playlist")) return "playlists";
    return "watchlist";
  };

  async function hydrateFromInsights(startTsEpoch) {
    const events = (await fetchJSON("/api/insights", null))?.events;
    if (!Array.isArray(events) || !events.length) return false;
    const since = Math.floor(startTsEpoch || 0);
    const tallies = Object.fromEntries(FEAT_KEYS.map((k) => [k, mkLane()]));

    for (const e of events) {
      if ((e.ts || 0) < since) continue;
      const key = inferInsightFeature(e);
      const lane = tallies[key] || (tallies[key] = mkLane());
      const act = String(e.action || "").toLowerCase();
      const spot = { title: titleOf(e), key: e.key, type: e.type, ts: e.ts, source: e.source || e.provider || e.side, series_title: e.series_title || e.show_title, season: e.season, episode: e.episode, display_title: e.display_title };
      if (act === "add") { lane.added++; if (lane.spotAdd.length < 25) lane.spotAdd.push(spot); }
      else if (act === "remove") { lane.removed++; if (lane.spotRem.length < 25) lane.spotRem.push(spot); }
      else { lane.updated++; if (lane.spotUpd.length < 25) lane.spotUpd.push(spot); }
    }

    summary ||= {}; summary.features ||= {};
    let appliedAny = false;
    for (const [feat, lane] of Object.entries(tallies)) {
      if (!laneHasCounts(lane) && !lane.spotAdd.length && !lane.spotRem.length && !lane.spotUpd.length) continue;
      const prev = summary.features[feat] || {};
      const prevCounts = (+prev.added || 0) + (+prev.removed || 0) + (+prev.updated || 0);
      if (!prevCounts) continue;
      const merged = {
        added: +prev.added || 0,
        removed: +prev.removed || 0,
        updated: +prev.updated || 0,
        spotlight_add: prev.spotlight_add?.length ? prev.spotlight_add : lane.spotAdd,
        spotlight_remove: prev.spotlight_remove?.length ? prev.spotlight_remove : lane.spotRem,
        spotlight_update: prev.spotlight_update?.length ? prev.spotlight_update : lane.spotUpd
      };
      if (!guardLaneOverwrite(feat, merged)) continue;
      appliedAny = true;
      summary.features[feat] = merged;
      hydratedLanes[feat] = { added: merged.added, removed: merged.removed, updated: merged.updated, spotAdd: merged.spotlight_add, spotRem: merged.spotlight_remove, spotUpd: merged.spotlight_update };
    }
    if (!appliedAny) return false;
    summary.enabled = { ...defaultEnabledMap(), ...(summary.enabled || {}) };
    renderAll();
    return true;
  }

  const needsSpotlights = (s) => {
    const feats = s?.features;
    if (!feats) return false;
    return getDisplayFeats().some(({ key }) => {
      const lane = feats[key] || {};
      return laneHasCounts(lane) && !((lane.spotlight_add?.length || 0) + (lane.spotlight_remove?.length || 0) + (lane.spotlight_update?.length || 0));
    });
  };

  const queueInsightsHydration = (runKey, startTs) => {
    if (!runKey) return;
    if (insightsRetry.runKey !== runKey) Object.assign(insightsRetry, { runKey, tries: 0 });
    clearTimeout(insightsRetry.t);
    const delay = INSIGHTS_RETRY_DELAYS[Math.min(insightsRetry.tries, INSIGHTS_RETRY_DELAYS.length - 1)] || 0;
    insightsRetry.t = setTimeout(async () => {
      const got = await hydrateFromInsights(startTs).catch(() => false);
      if (got || insightsRetry.runKey !== runKey || !needsSpotlights(summary)) return;
      if (insightsRetry.tries >= INSIGHTS_RETRY_DELAYS.length - 1) {
        if (logHydratedForRun !== runKey) {
          logHydratedForRun = runKey;
          hydrateFromLog();
        }
        return;
      }
      insightsRetry.tries++;
      queueInsightsHydration(runKey, startTs);
    }, delay);
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
        if (/^apply:add:done$/.test(obj.event)) lane.added += cnt;
        else if (/^apply:remove:done$/.test(obj.event)) lane.removed += cnt;
        else if (/^apply:update:done$/.test(obj.event)) lane.updated += cnt;
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
      insightsTriedForRun = null;
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
      if (insightsTriedForRun !== runKey) insightsTriedForRun = runKey;
      try {
        const startTs = s?.raw_started_ts || (s?.started_at ? Date.parse(s.started_at) / 1000 : 0);
        if (needsSpotlights(s)) queueInsightsHydration(runKey, startTs);
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
    const startTs = summary?.raw_started_ts || (summary?.started_at ? Date.parse(summary.started_at) / 1000 : 0);
    const hasFeatures = summary?.features && Object.values(summary.features).some((v) => laneHasCounts(v) || v?.spotlight_add?.length || v?.spotlight_remove?.length || v?.spotlight_update?.length);
    if (needsSpotlights(summary)) {
      if (insightsTriedForRun !== runKey) insightsTriedForRun = runKey;
      queueInsightsHydration(runKey, startTs);
    } else if (!hasFeatures) {
      setTimeout(() => {
        if (logHydratedForRun !== runKey) {
          logHydratedForRun = runKey;
          hydrateFromLog();
        }
      }, 300);
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
      esSummary = new EventSource("/api/run/summary/stream");
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
      on(esSummary, ["progress:apply", "apply:add:progress", "apply:remove:progress"], (e) => {
        try { sync.applyProg(JSON.parse(e.data || "{}")); } catch {}
      });
      on(esSummary, ["apply:add:start", "apply:remove:start"], (e) => {
        try { sync.applyStart(JSON.parse(e.data || "{}")); } catch {}
      });
      on(esSummary, ["apply:add:done", "apply:remove:done"], (e) => {
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
      document.dispatchEvent(new CustomEvent("tab-changed", { detail: { tab: name } }));
    } catch {}
    return ret;
  };

  ["updateWatchlistPreview", "updatePreviewVisibility", "loadWatchlist"].forEach(guard);
  document.addEventListener("tab-changed", () => !isMain() && hidePreview());
  document.addEventListener("visibilitychange", () => document.visibilityState === "visible" && !isMain() && hidePreview());
})();
