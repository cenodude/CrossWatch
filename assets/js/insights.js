/* assets/js/insights.js */
/* CrossWatch - Insight Module for watchlist, ratings, history, progress, playlists */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (w, d) {
  const authSetupPending = () => w.cwIsAuthSetupPending?.() === true;
  const featureMeta = w.CW?.FeatureMeta || {};
  const providerMeta = w.CW?.ProviderMeta || {};
  const FEAT_LABEL = featureMeta.labels || { watchlist:"Watchlist", ratings:"Ratings", history:"History", progress:"Progress", playlists:"Playlists" };
const FEAT_ICON = { watchlist:"movie", ratings:"star", history:"play_arrow", progress:"timelapse", playlists:"queue_music" };
  const FEATS = featureMeta.order || Object.keys(FEAT_LABEL);
  const DEFAULT_RECENT_SYNCS_LIMIT = 3;
  const PREF_KEY = "insights.settings.v1";
  const CW_SNAPSHOT_PROFILE_KEY = "insights.crosswatch.snapshotProfile";
  const $ = (s, r = d) => r.querySelector(s), $$ = (s, r = d) => [...r.querySelectorAll(s)], lc = s => String(s || "").toLowerCase();
  const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c]));
  const featureLabel = v => featureMeta.label?.(v) || FEAT_LABEL[lc(v)] || String(v || "");
  const providerLabel = v => providerMeta.label?.(v) || String(v || "");
  const providerLogo = v => providerMeta.logoPath?.(v) || "";
  const overviewProfile = () => w.CW?.OverviewProfile || null;
  const overviewProfileId = () => String(overviewProfile()?.id || "").trim();
  const overviewFilter = () => overviewProfile()?.filter || {};
  const overviewInstancesFor = prov => {
    const filter = overviewFilter();
    const up = String(prov || "").trim().toUpperCase();
    const low = lc(prov);
    return Array.isArray(filter[up]) ? filter[up].map(String) : Array.isArray(filter[low]) ? filter[low].map(String) : null;
  };
  const overviewHasScope = () => !!Object.keys(overviewFilter() || {}).length;
  const titleOf = x => x?.display_title || x?.title || x?.series_title || x?.name || (x?.type === "episode" && x?.series_title && Number.isInteger(x?.season) && Number.isInteger(x?.episode) ? `${x.series_title} S${String(x.season).padStart(2,"0")}E${String(x.episode).padStart(2,"0")}` : x?.key) || "item";
  const subtitleOf = x => x?.display_subtitle || "";
  const readJSON = (key, fallback = {}) => { try { return JSON.parse(localStorage.getItem(key) || "{}") || fallback; } catch { return fallback; } };
  const loadPrefs = () => readJSON(PREF_KEY, {});
  const savePrefs = p => { try { localStorage.setItem(PREF_KEY, JSON.stringify(p || {})); } catch {} };
  const rowTs = row => { const ts = new Date(row?.finished_at || row?.started_at || 0).getTime(); return Number.isFinite(ts) ? ts : null; };
  const totalsFor = (feat, row) => { const f = row?.features?.[feat] || {}; const a = f.added | 0, r = f.removed | 0, u = f.updated | 0; return { a, r, u, sum: a + r + u }; };
  const fmtWhen = row => { const dt = new Date(row?.finished_at || row?.started_at || 0); if (!Number.isFinite(+dt)) return "—"; return `${String(dt.getDate()).padStart(2,"0")}-${String(dt.getMonth()+1).padStart(2,"0")}-${String(dt.getFullYear()).slice(-2)} ${String(dt.getHours()).padStart(2,"0")}:${String(dt.getMinutes()).padStart(2,"0")}`; };
  const fmtDur = v => { if (v == null) return "—"; const n = parseFloat(String(v).replace(/[^\d.]/g, "")); return Number.isFinite(n) ? `${n.toFixed(1)}s` : "—"; };
  const asNum = (v, fb = 0) => Number.isFinite(+v) ? +v : fb;
  const tickUrl = url => `${url}${url.includes("?") ? "&" : "?"}_ts=${Date.now()}`;
  const fetchJSON = async url => {
    if (authSetupPending()) throw new Error("auth setup pending");
    const res = await fetch(tickUrl(url), { credentials: "same-origin", cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.json();
  };
  const postOK = async url => {
    const res = await fetch(url, { method: "POST", credentials: "same-origin" });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    const body = await res.json().catch(() => ({}));
    if (body?.ok === false) throw new Error(body.error || "Backend reported failure");
    return body;
  };
  const clone = v => v && typeof v === "object" ? JSON.parse(JSON.stringify(v)) : {};

  function normalizePrefs(p, instancesByProvider = {}) {
    const out = clone(p), f = out.features && typeof out.features === "object" ? out.features : {};
    out.features = { watchlist: f.watchlist !== false, ratings: f.ratings !== false, history: f.history !== false, progress: f.progress !== false, playlists: f.playlists !== false };
    out.instances = out.instances && typeof out.instances === "object" ? out.instances : {};
    out.known_instances = out.known_instances && typeof out.known_instances === "object" ? out.known_instances : {};
    for (const [prov, list] of Object.entries(instancesByProvider || {})) {
      const key = lc(prov);
      if (!key) continue;
      const all = Array.isArray(list) && list.length ? list.map(String) : ["default"];
      const prev = new Set(Array.isArray(out.known_instances[key]) ? out.known_instances[key].map(String) : []);
      const cur = out.instances[key];
      if (cur !== undefined) {
        const keep = (Array.isArray(cur) ? cur : []).map(String).filter(x => all.includes(x));
        for (const x of all) if (!prev.has(x) && !keep.includes(x)) keep.push(x);
        out.instances[key] = keep;
      }
      out.known_instances[key] = all.slice();
    }
    if (!Object.values(out.features).some(Boolean)) out.features.watchlist = true;
    return out;
  }

  const visibleFeatures = p => {
    const out = FEATS.filter(k => (p?.features || {})[k] !== false);
    return out.length ? out : ["watchlist"];
  };

  function recentSyncsDisplay() {
    const ui = w._cfgCache && typeof w._cfgCache.ui === "object" ? w._cfgCache.ui : {};
    const raw = String(ui.recent_syncs_display || "").trim().toLowerCase();
    const countMatch = /^count:(3|4|5)$/.exec(raw);
    if (countMatch) return { mode: "count", limit: Number(countMatch[1]), hours: 0, sinceMs: 0 };
    const hoursMatch = /^hours:(24|48|72)$/.exec(raw);
    if (hoursMatch) {
      const hours = Number(hoursMatch[1]);
      return { mode: "hours", limit: 5, hours, sinceMs: Date.now() - (hours * 3600 * 1000) };
    }
    const rawLimit = Number(ui.recent_syncs_limit);
    return { mode: "count", limit: Math.max(3, Math.min(5, Number.isFinite(rawLimit) ? rawLimit : DEFAULT_RECENT_SYNCS_LIMIT)), hours: 0, sinceMs: 0 };
  }

  function selectionDiffers(p, instancesByProvider = {}) {
    const inst = p?.instances || {};
    for (const [prov, list] of Object.entries(instancesByProvider || {})) {
      const all = Array.isArray(list) && list.length ? list.map(String) : ["default"], cur = inst[lc(prov)];
      if (cur === undefined) continue;
      if (!Array.isArray(cur) || cur.length !== all.length) return true;
      const want = new Set(cur.map(String));
      for (const x of all) if (!want.has(x)) return true;
    }
    return false;
  }

  let _prefs = loadPrefs(), _visibleFeats = visibleFeatures(_prefs);
  const clampFeature = name => _visibleFeats.includes(String(name)) ? name : (_visibleFeats[0] || "watchlist");
  let _feature = clampFeature(localStorage.getItem("insights.feature"));
  let _lastStatsFetch = 0, _cwSnapModal = null, _lastInsightsData = null, _fullInsightsTimer = 0, _configuredProvidersCache = null, _tilesFeature = null;

  function syncPrefs(instancesByProvider = {}) {
    const next = normalizePrefs(_prefs, instancesByProvider), changed = JSON.stringify(next) !== JSON.stringify(_prefs);
    _prefs = next;
    if (changed) savePrefs(_prefs);
    _visibleFeats = visibleFeatures(_prefs);
    const keep = clampFeature(_feature);
    if (keep !== _feature) {
      _feature = keep;
      localStorage.setItem("insights.feature", keep);
    }
  }


  async function openInsightSettingsModal() {
    try {
      if (typeof w.openInsightSettingsModal === "function") return await w.openInsightSettingsModal({});
      const v = encodeURIComponent(String(w.__CW_VERSION__ || Date.now()));
      const mod = await import(`./modals.js?v=${v}`);
      if (typeof mod.openModal === "function") await mod.openModal("insight-settings", {});
    } catch (e) {
      console.error("[Insights] Failed to open settings modal", e);
    }
  }

  const getConfiguredProviders = async force => {
    try {
      if (document.documentElement?.dataset?.cwRole === "user") {
        const keys = Object.keys(overviewFilter() || {}).map(k => String(k || "").trim()).filter(Boolean);
        if (keys.length) return new Set(keys);
      }
      let cfg = w._cfgCache;
      const needsConfigLoad = force || !cfg || typeof cfg !== "object" || !Object.keys(cfg).length;
      if (needsConfigLoad && typeof w.CW?.API?.Config?.load === "function") {
        try {
          cfg = await w.CW.API.Config.load(!!force);
          if (cfg && typeof cfg === "object") w._cfgCache = cfg;
        } catch {}
      }
      const out = typeof w.getConfiguredProviders === "function" ? await w.getConfiguredProviders(cfg || {}) : ["crosswatch"];
      const set = out instanceof Set ? out : new Set([...(out || [])]);
      _configuredProvidersCache = new Set([...set]);
      return set;
    } catch (e) {
      console.error("[Insights] Failed to resolve configured providers", e);
      return new Set(["crosswatch"]);
    }
  };

  function configuredProvidersSnapshot(active = {}) {
    if (_configuredProvidersCache?.size) return new Set([..._configuredProvidersCache]);
    try {
      if (typeof w.getConfiguredProviders === "function") {
        const cfg = w._cfgCache;
        if (cfg && typeof cfg === "object" && Object.keys(cfg).length) {
          const out = w.getConfiguredProviders(cfg);
          const set = out instanceof Set ? out : new Set([...(out || [])]);
          if (set.size) {
            _configuredProvidersCache = new Set([...set]);
            return set;
          }
        }
      }
    } catch {}
    return new Set(Object.entries(active || {}).filter(([, live]) => !!live).map(([key]) => key));
  }

  function filterProviderTotals(block, instancesByProvider = {}) {
    const raw = block?.raw || {}, instCounts = raw.providers_instances, instMse = raw.providers_instances_mse;
    const activeOverviewFilter = overviewFilter();
    const hasOverviewFilter = !!Object.keys(activeOverviewFilter || {}).length;
    if (!hasOverviewFilter && (!selectionDiffers(_prefs, instancesByProvider) || !instCounts || typeof instCounts !== "object")) {
      return { providers: block.providers || {}, mse: raw.providers_mse || null, now: block.now };
    }
    const out = {}, outMse = {}, selected = _prefs?.instances || {}, zero = () => ({ movies:0, shows:0, anime:0, episodes:0 });
    for (const [prov, byInst] of Object.entries(instCounts || {})) {
      const key = lc(prov), map = byInst && typeof byInst === "object" ? byInst : {}, keys = Object.keys(map), want = Array.isArray(selected[key]) ? selected[key].map(String) : selected[key] === undefined ? keys : [];
      const overviewWant = activeOverviewFilter[String(prov || "").toUpperCase()] || activeOverviewFilter[String(prov || "").toLowerCase()];
      const finalWant = hasOverviewFilter ? want.filter(id => (overviewWant || []).map(String).includes(String(id))) : want;
      out[key] = finalWant.reduce((sum, id) => sum + (map[id] | 0), 0);
      const mseMap = instMse?.[key] && typeof instMse[key] === "object" ? instMse[key] : {}, agg = zero();
      for (const id of finalWant) {
        const part = mseMap[id];
        if (!part || typeof part !== "object") continue;
        agg.movies += part.movies | 0; agg.shows += part.shows | 0; agg.anime += part.anime | 0; agg.episodes += part.episodes | 0;
      }
      outMse[key] = agg;
    }
    if (!hasOverviewFilter) {
      for (const [prov, v] of Object.entries(block.providers || {})) if (out[lc(prov)] === undefined) out[lc(prov)] = v | 0;
      for (const [prov, v] of Object.entries(raw.providers_mse || {})) if (outMse[lc(prov)] === undefined) outMse[lc(prov)] = v;
    }
    const vals = Object.values(out).map(v => v | 0).filter(v => v > 0);
    return { providers: out, mse: outMse, now: vals.length ? Math.max(...vals) : hasOverviewFilter ? 0 : block.now | 0 };
  }

  function pickBlock(data, feat) {
    const raw = data?.features?.[feat];
    if (!raw) throw new Error(`[Insights] Missing feature block for "${feat}"`);
    let { now, week, month, added, removed } = raw;
    const rows = (Array.isArray(data.history) ? data.history : []).map(r => ({ r, ts: rowTs(r) })).filter(x => x.ts != null).sort((a, b) => a.ts - b.ts);
    const sumSince = since => rows.reduce((acc, { r, ts }) => {
      if (ts < since) return acc;
      const t = totalsFor(feat, r);
      acc.A += t.a; acc.R += t.r; acc.S += t.sum;
      return acc;
    }, { A: 0, R: 0, S: 0 });
    const ms = Date.now();
    if (!Number.isFinite(+now)) now = rows.length ? totalsFor(feat, rows.at(-1).r).sum : 0;
    if (!Number.isFinite(+week)) week = sumSince(ms - 7 * 86400000).S;
    if (!Number.isFinite(+month)) month = sumSince(ms - 30 * 86400000).S;
    if (!Number.isFinite(+added) || !Number.isFinite(+removed)) {
      const m = sumSince(ms - 30 * 86400000);
      if (!Number.isFinite(+added)) added = m.A;
      if (!Number.isFinite(+removed)) removed = m.R;
    }
    return {
      series: Array.isArray(raw.series) ? raw.series : [],
      providers: raw.providers || {},
      active: raw.providers_active || data?.providers_active || {},
      now: asNum(now), week: asNum(week), month: asNum(month), added: asNum(added), removed: asNum(removed),
      raw
    };
  }

  function hydrateBlock(data) {
    syncPrefs(data?.instances_by_provider || {});
    const blk = pickBlock(data, _feature), filtered = filterProviderTotals(blk, data?.instances_by_provider || {});
    blk.providers = filtered.providers;
    blk.now = Number.isFinite(filtered.now) ? filtered.now : blk.now;
    if (blk.raw) blk.raw.providers_mse = filtered.mse || blk.raw.providers_mse;
    return blk;
  }

  function renderSparkline(id, points) {
    const el = d.getElementById(id);
    if (!el) return;
    if (!points?.length) return void (el.innerHTML = '<div class="muted">No data</div>');
    const wv = el.clientWidth || 260, hv = el.clientHeight || 64, pad = 4, xs = points.map(p => +p.ts || 0), ys = points.map(p => +p.count || 0);
    const minX = Math.min(...xs), maxX = Math.max(...xs), minY = Math.min(...ys), maxY = Math.max(...ys);
    const X = t => maxX === minX ? pad : pad + (wv - 2 * pad) * (t - minX) / (maxX - minX);
    const Y = v => maxY === minY ? hv / 2 : hv - pad - (hv - 2 * pad) * (v - minY) / (maxY - minY);
    el.innerHTML = `<svg viewBox="0 0 ${wv} ${hv}" preserveAspectRatio="none"><path class="line" d="${points.map((p, i) => `${i ? "L" : "M"}${X(p.ts)},${Y(p.count)}`).join(" ")}"></path>${points.map(p => `<circle class="dot" cx="${X(p.ts)}" cy="${Y(p.count)}"></circle>`).join("")}</svg>`;
  }

  const ease = t => t < .5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
  function fitProviderNumber(el) {
    const tile = el?.closest?.("#stat-providers .tile");
    if (!tile) return;
    el.style.setProperty("--ins-font-scale", "1");
    const tw = tile.getBoundingClientRect().width, nw = el.getBoundingClientRect().width;
    if (tw && nw) el.style.setProperty("--ins-font-scale", String(Math.min(1, tw * .78 / nw).toFixed(3)));
  }
  function fitProviderMSE(el) {
    const tile = el?.closest?.("#stat-providers .tile");
    if (!tile) return;
    el.style.setProperty("--ins-mse-scale", "1");
    const digits = [...el.querySelectorAll(".mse-chip .v")].map(n => String(n.textContent || "").trim().length).filter(Boolean);
    const maxDigits = digits.length ? Math.max(...digits) : 0, totalDigits = digits.reduce((a, n) => a + n, 0);
    el.classList.remove("is-wrap");
    const row = el.querySelector(".mse-row") || el;
    const tw = tile.getBoundingClientRect().width;
    let mw = row.scrollWidth || row.getBoundingClientRect().width;
    if (!tw || !mw) return;
    if (digits.length > 2 && mw > tw - 8) {
      el.classList.add("is-wrap");
      mw = row.scrollWidth || row.getBoundingClientRect().width;
    }
    let minScale = .84;
    if (el.classList.contains("is-wrap")) minScale = .96;
    else if (maxDigits <= 1 && totalDigits <= 3) minScale = .97;
    else if (maxDigits <= 2 && totalDigits <= 5) minScale = .93;
    else if (maxDigits <= 3 && totalDigits <= 8) minScale = .89;
    el.style.setProperty("--ins-mse-scale", String(Math.max(minScale, Math.min(1, (tw - 10) / mw)).toFixed(3)));
  }
  function animateNumber(el, to, duration = 650, animate = true) {
    if (!el) return;
    const from = parseInt(el.dataset?.v || el.textContent || "0", 10) || 0, done = () => { el.textContent = String(to); el.dataset.v = String(to); fitProviderNumber(el); };
    if (!animate || from === to) return done();
    const start = performance.now(), dur = Math.max(180, duration);
    const step = now => {
      const p = Math.min(1, (now - start) / dur);
      el.textContent = String(Math.round(from + (to - from) * ease(p)));
      p < 1 ? requestAnimationFrame(step) : done();
    };
    requestAnimationFrame(step);
  }
  function animateChart(now, week, month) {
    const max = Math.max(1, now, week, month), scale = v => Math.max(.04, v / max);
    [[".bar.week", week], [".bar.month", month], [".bar.now", now]].forEach(([sel, val]) => { const el = $(sel); if (el) el.style.transform = `scaleY(${scale(val)})`; });
  }
  const refitProviderNumbers = () => { $$("#stat-providers .tile .n").forEach(fitProviderNumber); $$("#stat-providers .tile .mse").forEach(fitProviderMSE); };
  w.addEventListener("resize", refitProviderNumbers, { passive: true });

  const footWrap = () => {
    const stats = $("#stats-card");
    if (!stats) {
      $("#insights-footer")?.remove();
      return null;
    }
    let foot = $("#insights-footer");
    if (!foot) {
      foot = d.createElement("div");
      foot.id = "insights-footer";
      foot.className = "ins-footer";
      foot.innerHTML = '<div class="ins-foot-wrap"></div>';
      stats.appendChild(foot);
    }
    return $(".ins-foot-wrap", foot) || foot;
  };

  function placeSwitchBeforeTiles() {
    const wrap = footWrap(), sw = $("#insights-switch"), grid = $("#stat-providers");
    if (!wrap || !sw) return;
    if (!wrap.contains(sw)) wrap.appendChild(sw);
    const ref = grid?.parentNode === wrap ? grid : null;
    if (sw.nextSibling !== ref) try { wrap.insertBefore(sw, ref); } catch {}
  }

  function markActiveSwitcher() {
    $$("#insights-switch .seg-btn").forEach(btn => {
      const on = btn.dataset.key === _feature;
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
  }

  function switchFeature(name) {
    const want = clampFeature(name);
    if (want === _feature) return;
    _feature = want;
    localStorage.setItem("insights.feature", want);
    markActiveSwitcher();
    refreshInsightsFastThenFull(true);
  }

  function ensureSwitch() {
    const wrap = footWrap();
    if (!wrap) return;
    let host = $("#insights-switch");
    if (!host) {
      host = d.createElement("div");
      host.id = "insights-switch";
      host.className = "ins-switch";
      host.innerHTML = '<div class="seg" role="tablist" aria-label="Insights features"></div>';
      wrap.appendChild(host);
    } else if (host.parentNode !== wrap) wrap.appendChild(host);
    if (!host.dataset.bound) {
      host.addEventListener("click", ev => {
        const btn = ev.target.closest(".seg-btn");
        if (btn) return switchFeature(btn.dataset.key);
        if (ev.target.closest(".ins-gear")) openInsightSettingsModal();
      });
      host.dataset.bound = "1";
    }
    const seg = $(".seg", host), sig = _visibleFeats.join(",");
    const featCount = Math.max(1, _visibleFeats.length);
    host.style.setProperty("--ins-feat-count", String(featCount));
    host.dataset.labels = featCount >= 4 ? "icon" : "text";
    if (seg) seg.dataset.count = String(featCount);
    host.querySelector(":scope > .ins-gear")?.remove();
    const needsGear = seg && !seg.querySelector(".ins-gear");
    if (host.dataset.feats !== sig || host.dataset.cur !== _feature || needsGear) {
      const settingsButton = '<button class="ins-gear" type="button" title="Insight settings" aria-label="Insight settings"><span class="material-symbols-rounded" aria-hidden="true">settings</span></button>';
      seg.innerHTML = _visibleFeats.map(f => `<button class="seg-btn${_feature === f ? " active" : ""}" data-key="${f}" role="tab" aria-selected="${_feature === f}" title="${featureLabel(f)}" aria-label="${featureLabel(f)}"><span class="material-symbols-rounded" aria-hidden="true">${FEAT_ICON[lc(f)] || "insights"}</span><span>${featureLabel(f)}</span></button>`).join("") + settingsButton;
      host.dataset.feats = sig;
      host.dataset.cur = _feature;
    }
    placeSwitchBeforeTiles();
    markActiveSwitcher();
  }

  const providerSelected = (prov, instancesByProvider = {}) => {
    const scoped = overviewInstancesFor(prov);
    if (overviewHasScope() && !Array.isArray(scoped)) return false;
    const cur = _prefs?.instances?.[lc(prov)];
    const selected = cur === undefined || !Array.isArray(cur)
      ? (cur !== false ? (instancesByProvider?.[lc(prov)] || instancesByProvider?.[String(prov || "").toUpperCase()] || []) : [])
      : cur.map(String);
    if (!overviewHasScope()) return cur === undefined || !Array.isArray(cur) ? cur !== false : cur.length > 0;
    if (!Array.isArray(scoped)) return false;
    if (!selected.length) return scoped.length > 0;
    const allowed = new Set(scoped.map(String));
    return selected.some(id => allowed.has(String(id)));
  };

  function renderProviderStats(provTotals = {}, provActive = {}, configuredSet = new Set(), breakdownMap = {}, instancesByProvider = {}, animate = true) {
    const wrap = footWrap();
    if (!wrap) return;
    let host = $("#stat-providers");
    if (!host) {
      host = d.createElement("div");
      host.id = "stat-providers";
      wrap.appendChild(host);
    } else if (host.parentNode !== wrap) wrap.appendChild(host);
    if (!host.dataset.bound) {
      host.addEventListener("click", ev => {
        if (document.documentElement?.dataset?.cwRole === "user") return;
        const tile = ev.target.closest('.tile[data-provider="crosswatch"]');
        if (tile && _feature !== "playlists") openCrosswatchSnapshotPicker(_feature);
      });
      host.dataset.bound = "1";
    }

    const canonical = k => lc(k) === "crosswatch" ? "crosswatch" : String(k || "").toUpperCase();
    const totals = Object.fromEntries(Object.entries(provTotals).map(([k, v]) => [canonical(k), v]));
    const active = Object.fromEntries(Object.entries(provActive).map(([k, v]) => [canonical(k), v]));
    const conf = new Set([...configuredSet].map(canonical));
    const keys = [...new Set([...Object.keys(totals), ...Object.keys(active), ...conf])]
      .filter(k => conf.has(canonical(k)) && providerSelected(k, instancesByProvider))
      .sort((a, b) => a === "crosswatch" ? -1 : b === "crosswatch" ? 1 : a.localeCompare(b));

    if (!keys.length) {
      host.hidden = true;
      return;
    }

    host.hidden = false;
    host.style.setProperty("--prov-cols", Math.max(1, Math.min(keys.length, 4)));
    const seen = new Set();

    for (const key of keys) {
      const prov = lc(key), label = providerLabel(prov) || key, total = +totals[key] || 0, per = breakdownMap?.[prov] || null, live = !!active[key];
      let tile = $(`#tile-${key}`);
      if (!tile) {
        tile = d.createElement("div");
        tile.id = `tile-${key}`;
        tile.className = "tile provider";
        tile.dataset.provider = prov;
        tile.innerHTML = '<div class="tile-head"><span class="tile-k"></span><span class="tile-state"><i></i><span class="txt"></span></span></div><div class="n" data-v="0">0</div><div class="mse"></div>';
        host.appendChild(tile);
      } else if (tile.parentNode !== host) host.appendChild(tile);

      tile.style.cursor = prov === "crosswatch" ? "pointer" : "";
      const watermark = providerLogo(prov);
      if (watermark) tile.style.setProperty("--wm", `url("${watermark}")`);
      else tile.style.removeProperty("--wm");
      tile.classList.toggle("inactive", !live);
      $(".tile-k", tile).textContent = label;
      $(".tile-state", tile).classList.toggle("on", live);
      $(".tile-state .txt", tile).textContent = live ? "Live" : "Idle";
      animateNumber($(".n", tile), total, 650, animate);

      const info = $(".mse", tile);
      if (!per || prov === "crosswatch" || _feature === "playlists") {
        info.textContent = "";
        info.style.display = "none";
        tile.title = prov === "crosswatch" ? `${label} • ${total} • Click to switch snapshot` : `${label} • ${total}`;
      } else {
        const m = +per.movies || 0, s = +per.shows || 0, a = +per.anime || 0;
        const parts = [["M", m, ""], ["S", s, ""], ["A", a, " mse-chip-anime"]]
          .filter(([, v]) => v)
          .map(([k, v, cls]) => `<span class="mse-chip${cls}"><span class="k">${k}</span><span class="v">${v}</span></span>`);
        if (!parts.length) {
          info.textContent = "";
          info.style.display = "none";
          tile.title = `${label} • ${total}`;
        } else {
          info.innerHTML = `<span class="mse-row">${parts.join("")}</span>`;
          info.style.display = "";
          fitProviderMSE(info);
          tile.title = `${label} • ${total} • Movies ${m} • Shows ${s} • Anime ${a}`;
        }
      }
      seen.add(tile.id);
    }

    [...host.querySelectorAll(".tile")].forEach(tile => !seen.has(tile.id) && tile.remove());
    placeSwitchBeforeTiles();
  }

  function crosswatchSnapshotProfiles(cwSnapshots) {
    const profiles = Array.isArray(cwSnapshots?._profiles) ? cwSnapshots._profiles : [];
    return profiles.length ? profiles : [{ id: "default", label: "Default", root_dir: "/config/.cw_provider" }];
  }

  function crosswatchSnapshotProfileId(cwSnapshots, preferred = "") {
    const profiles = crosswatchSnapshotProfiles(cwSnapshots);
    const ids = new Set(profiles.map(p => String(p.id || "default")));
    const wanted = String(preferred || localStorage.getItem(CW_SNAPSHOT_PROFILE_KEY) || "default");
    if (ids.has(wanted)) return wanted;
    return ids.has("default") ? "default" : String(profiles[0]?.id || "default");
  }

  function crosswatchSnapshotProfileInfo(cwSnapshots, profileId, feature) {
    const inst = crosswatchSnapshotProfileId(cwSnapshots, profileId);
    return cwSnapshots?._by_profile?.[inst]?.[feature] || (inst === "default" ? cwSnapshots?.[feature] : null) || null;
  }

  function crosswatchSnapshotProfileLabel(cwSnapshots, profileId) {
    const inst = crosswatchSnapshotProfileId(cwSnapshots, profileId);
    return crosswatchSnapshotProfiles(cwSnapshots).find(p => String(p.id || "default") === inst)?.label || inst;
  }

  function renderCrossWatchSnapshotHint(cwSnapshots) {
    const tile = d.querySelector('#stat-providers [data-provider="crosswatch"]');
    if (!tile) return;
    const old = $(".cw-snapshot", tile);
    if (_feature === "playlists") return void old?.remove();
    const inst = crosswatchSnapshotProfileId(cwSnapshots);
    const info = crosswatchSnapshotProfileInfo(cwSnapshots, inst, _feature);
    if (!info?.has_snapshots || !info?.actual) return void old?.remove();
    const label = old || (() => { const el = d.createElement("div"); el.className = "cw-snapshot"; tile.appendChild(el); return el; })();
    const selected = String(info.selected || "latest") === "latest" ? "Latest" : (info.human || info.actual);
    label.textContent = inst === "default" ? selected : `${inst}: ${selected}`;
    label.title = `${crosswatchSnapshotProfileLabel(cwSnapshots, inst)} - ${info.actual}`;
  }

  function renderHistoryTabs(hist) {
    const wrap = $("#sync-history") || $("[data-role='sync-history']") || $(".sync-history");
    if (!wrap) return;
    if (!wrap.dataset.listInit) {
      wrap.innerHTML = '<div class="list"></div>';
      wrap.dataset.listInit = "1";
    }
    const list = $(".list", wrap);
    if (!list) return;
    const display = recentSyncsDisplay();
    const profile = window.CW?.OverviewProfile;
    const rows = (Array.isArray(hist) ? hist : []).slice()
      .sort((a, b) => new Date(b.finished_at || b.started_at || 0) - new Date(a.finished_at || a.started_at || 0))
      .filter(row => row?.features_enabled?.[_feature] !== false)
      .filter(row => !profile?.filter || !Object.keys(profile.filter || {}).length || profile.matchesEndpoint(row?.source, row?.source_instance) || profile.matchesEndpoint(row?.target, row?.target_instance))
      .filter(row => display.mode !== "hours" || ((rowTs(row) || 0) >= display.sinceMs))
      .slice(0, display.limit);
    if (!rows.length) return void (list.innerHTML = `<div class="history-item"><div class="history-meta muted">${display.mode === "hours" ? `No runs in the last ${display.hours} hours` : "No runs for this feature"}</div></div>`);
    list.innerHTML = rows.map(row => {
      const t = totalsFor(_feature, row), cls = typeof row?.exit_code === "number" && row.exit_code !== 0 ? "err" : String(row?.result || "").toUpperCase() === "EQUAL" || t.sum === 0 ? "ok" : "warn";
      return `<div class="history-item ${cls}"><div class="history-main"><div class="history-meta"><span class="history-time">${fmtWhen(row)}</span><span class="badge ${cls}">${row?.result || "—"}${typeof row?.exit_code === "number" ? ` · ${row.exit_code}` : ""}</span></div><div class="history-sub">${featureLabel(_feature)} • ${fmtDur(row?.duration_sec)}</div></div><div class="history-badges"><span class="badge add">+${t.a}</span><span class="badge del">-${t.r}</span>${t.u ? `<span class="badge micro">~${t.u}</span>` : ""}</div></div>`;
    }).join("");
  }

  function renderTopStats({ now = 0, week = 0, month = 0, added = 0, removed = 0 }) {
    [["#stat-now", now], ["#stat-week", week], ["#stat-month", month], ["#stat-added", added], ["#stat-removed", removed]].forEach(([sel, val]) => animateNumber($(sel), val | 0));
    const fill = $("#stat-fill"), max = Math.max(1, now, week, month);
    if (fill) fill.style.width = `${Math.round(now / max * 100)}%`;
    animateChart(now, week, month);
    const lab = $("#stat-feature-label");
    if (lab) lab.textContent = featureLabel(_feature);
    const chip = $("#trend-week") || $("#stat-delta-chip");
    if (chip) {
      const diff = (now | 0) - (week | 0);
      chip.textContent = diff === 0 ? "no change" : `${diff > 0 ? "+" : ""}${diff} vs last week`;
      chip.classList.remove("up", "down", "flat", "muted");
      chip.classList.add(diff > 0 ? "up" : diff < 0 ? "down" : "flat");
      chip.classList.toggle("muted", diff === 0);
      chip.title = diff === 0 ? "No change versus last week" : `${Math.abs(diff)} ${diff > 0 ? "more" : "fewer"} than last week`;
    }
    $("#stat-breakdown")?.remove();
  }

  function renderWatchtime(wt) {
    const el = $("#watchtime"), note = $("#watchtime-note");
    if (el) el.innerHTML = `<div class="big">≈ ${wt.hours | 0}</div><div class="units">hrs <span style="opacity:.6">(${wt.days | 0} days)</span><br><span style="opacity:.8">${wt.movies | 0} movies • ${wt.shows | 0} shows</span></div>`;
    if (note) note.textContent = wt.method || "estimate";
  }

  async function renderFromData(data, statsOnly = false, forceConfigured = false) {
    _lastInsightsData = data || {};
    const blk = hydrateBlock(data);
    footWrap();
    ensureSwitch();
    if (!statsOnly) {
      try { renderSparkline("sparkline", blk.series || []); } catch {}
      renderHistoryTabs(data?.history || []);
      if (data?.watchtime) renderWatchtime(data.watchtime);
    }
    renderTopStats(blk);
    const animateTiles = _tilesFeature === _feature;
    _tilesFeature = _feature;
    const optimisticConfigured = configuredProvidersSnapshot(blk.active);
    renderProviderStats(blk.providers, blk.active, optimisticConfigured, blk.raw?.providers_mse || null, data?.instances_by_provider || {}, animateTiles);
    renderCrossWatchSnapshotHint(data?.crosswatch_snapshots || null);
    getConfiguredProviders(forceConfigured)
      .then((configured) => {
        renderProviderStats(blk.providers, blk.active, configured, blk.raw?.providers_mse || null, data?.instances_by_provider || {}, animateTiles);
        renderCrossWatchSnapshotHint(data?.crosswatch_snapshots || null);
      })
      .catch((e) => console.error("[Insights] Failed to resolve configured providers", e));
  }

  async function refreshInsights(force = false) {
    if (authSetupPending()) return;
    try {
      const profileParam = overviewProfileId() ? `&user_profile=${encodeURIComponent(overviewProfileId())}` : "";
      const [data] = await Promise.all([
        fetchJSON(`/api/insights?limit_samples=60&history=60${profileParam}${force ? `&t=${Date.now()}` : ""}`),
        getConfiguredProviders(force).catch(() => null),
      ]);
      await renderFromData(data, false, force);
    }
    catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return;
      console.error("[Insights] Failed to load /api/insights", e);
    }
  }

  async function refreshStats(force = false) {
    if (authSetupPending()) return;
    const now = Date.now();
    if (!force && now - _lastStatsFetch < 900) return;
    _lastStatsFetch = now;
    try {
      const profileParam = overviewProfileId() ? `&user_profile=${encodeURIComponent(overviewProfileId())}` : "";
      const [data] = await Promise.all([
        fetchJSON(`/api/insights?limit_samples=0&history=0&include_events=0${profileParam}${force ? `&t=${Date.now()}` : ""}`),
        getConfiguredProviders(force).catch(() => null),
      ]);
      await renderFromData(data, true, force);
    }
    catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return;
      console.error("[Insights] Failed to load /api/insights (stats)", e);
    }
  }

  function refreshInsightsFastThenFull(force = false) {
    if (authSetupPending()) return;
    try { refreshStats(force); } catch {}
    clearTimeout(_fullInsightsTimer);
    _fullInsightsTimer = setTimeout(() => {
      _fullInsightsTimer = 0;
      refreshInsights(force);
    }, 180);
  }

  w.addEventListener("insights:settings-changed", () => { _prefs = loadPrefs(); _visibleFeats = visibleFeatures(_prefs); _feature = clampFeature(_feature); localStorage.setItem("insights.feature", _feature); refreshInsightsFastThenFull(true); });
  w.addEventListener("cw:overview-profile-changed", () => refreshInsightsFastThenFull(true));

  w.Insights = Object.assign(w.Insights || {}, {
    renderSparkline, refreshInsights, refreshStats, fetchJSON, animateNumber, animateChart, titleOf, subtitleOf,
    switchFeature, refreshInsightsFastThenFull, get feature() { return _feature; }
  });
  w.renderSparkline = renderSparkline;
  w.refreshInsights = refreshInsights;
  w.refreshStats = refreshStats;
  w.fetchJSON = fetchJSON;
  w.animateNumber = w.animateNumber || animateNumber;
  w.titleOf = titleOf;
  w.subtitleOf = subtitleOf;
  w.scheduleInsights = function scheduleInsights(max) {
    if (authSetupPending()) return;
    let tries = 0, limit = max || 20;
    (function tick() {
      if (authSetupPending()) return;
      if ($("#sync-history") || $("#stat-now") || $("#sparkline")) return refreshInsightsFastThenFull();
      if (++tries < limit) setTimeout(tick, 250);
    })();
  };

  d.addEventListener("DOMContentLoaded", () => { if (!authSetupPending()) w.scheduleInsights(); });
  d.addEventListener("tab-changed", ev => {
    if (authSetupPending()) return;
    const tab = String(ev?.detail?.id || ev?.detail?.tab || "").toLowerCase();
    if (tab === "main") refreshInsightsFastThenFull(true);
  });

  const snapLabel = name => {
    const base = String(name || "").replace(/\.json$/, ""), stem = base.split("-", 1)[0], m = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(stem);
    return m ? `${m[1]}-${m[2]}-${m[3]} - ${m[4]}:${m[5]}` : base;
  };

  function ensureCrosswatchSnapshotModal() {
    if (_cwSnapModal?.parentNode) return _cwSnapModal;
    const modal = d.createElement("div");
    modal.className = "cw-snap-picker cw-snap-hidden";
    modal.innerHTML = '<div class="cw-snap-dialog" role="dialog" aria-modal="true" aria-labelledby="cw-snap-title"><div class="cw-snap-head"><div class="cw-snap-title-wrap"><div class="cw-snap-kicker">Snapshot selector</div><div class="cw-snap-title" id="cw-snap-title">Select snapshot for <span class="hl"></span></div></div><button class="snap-icon-close" type="button" aria-label="Close snapshot selector">×</button></div><div class="cw-snap-body"></div><div class="cw-snap-foot"><button class="snap-close" type="button">Cancel</button></div></div>';
    modal.addEventListener("click", async ev => {
      const close = ev.target === modal || ev.target.closest(".snap-close,.snap-icon-close");
      if (close) modal.classList.add("cw-snap-hidden");
      const btn = ev.target.closest(".snap-btn");
      if (!btn) return;
      const feature = modal.dataset.feature, name = btn.dataset.name, profile = btn.dataset.profile || modal.dataset.providerInstance || "default";
      try { await postOK(`/api/crosswatch/select-snapshot?feature=${encodeURIComponent(feature)}&snapshot=${encodeURIComponent(name)}&provider_instance=${encodeURIComponent(profile)}`); }
      catch (err) {
        console.error("[Insights] Failed to select snapshot", err);
        return void w.cxToast?.("Failed to set snapshot. Check server logs.");
      }
      try { localStorage.setItem(CW_SNAPSHOT_PROFILE_KEY, profile); } catch {}
      modal.classList.add("cw-snap-hidden");
      w.cxToast?.(`Snapshot set for ${profile}: ${name === "latest" ? "latest" : snapLabel(name)}`);
      refreshInsights(true);
    });
    modal.addEventListener("change", ev => {
      const sel = ev.target.closest(".cw-snap-profile-select");
      if (sel) loadCrosswatchSnapshotChoices(modal, modal.dataset.feature, sel.value);
    });
    d.body.appendChild(modal);
    _cwSnapModal = modal;
    return modal;
  }

  async function loadCrosswatchSnapshotChoices(modal, feature, profileId) {
    const body = $(".cw-snap-body", modal);
    const cwSnapshots = _lastInsightsData?.crosswatch_snapshots || {};
    const profiles = crosswatchSnapshotProfiles(cwSnapshots);
    const inst = crosswatchSnapshotProfileId(cwSnapshots, profileId);
    const profile = profiles.find(p => String(p.id || "default") === inst) || profiles[0] || { id: "default", label: "Default", root_dir: "/config/.cw_provider" };
    const info = crosswatchSnapshotProfileInfo(cwSnapshots, inst, feature);
    const rootDir = String(info?.root_dir || profile.root_dir || "/config/.cw_provider").replace(/[\\/]+$/, "");
    const selected = String(info?.selected || "latest");
    modal.dataset.providerInstance = inst;
    try { localStorage.setItem(CW_SNAPSHOT_PROFILE_KEY, inst); } catch {}

    body.innerHTML = `${profiles.length > 1
      ? `<label class="cw-snap-profile"><span>Profile</span><select class="cw-snap-profile-select">${profiles.map(p => `<option value="${esc(p.id || "default")}"${String(p.id || "default") === inst ? " selected" : ""}>${esc(p.label || p.id || "Default")}</option>`).join("")}</select></label>`
      : `<div class="cw-snap-profile is-single"><span>Profile</span><strong>${esc(profile.label || inst)}</strong></div>`}<div class="cw-snap-list"><div class="cw-snap-empty muted">Loading snapshots...</div></div>`;
    const list = $(".cw-snap-list", body);

    try {
      const data = await fetchJSON(`/api/files?path=${encodeURIComponent(`${rootDir}/snapshots`)}`);
      const files = (Array.isArray(data?.files) ? data.files : data || []).filter(f => !f.is_dir && f.name?.endsWith(`-${feature}.json`)).sort((a, b) => a.name.localeCompare(b.name)).slice(-10).reverse();
      if (!files.length) list.innerHTML = '<div class="cw-snap-empty muted">No snapshots found for this feature yet.</div>';
      else list.innerHTML = [{ name: "latest", label: "Latest snapshot", meta: selected === "latest" ? "Active" : "Live" }, ...files.map(f => ({ name: f.name, label: snapLabel(f.name), meta: f.name === selected ? "Active" : "Saved" }))].map(o => `<button class="snap-btn${o.name === "latest" ? " is-latest" : ""}${o.meta === "Active" ? " is-active" : ""}" data-name="${esc(o.name)}" data-profile="${esc(inst)}" type="button"><span class="snap-name">${esc(o.label)}</span><span class="snap-meta">${esc(o.meta)}</span></button>`).join("");
    } catch (e) {
      console.error("[Insights] Failed to load snapshot files", e);
      list.innerHTML = '<div class="cw-snap-empty muted">Failed to load snapshots. Check server logs or configuration.</div>';
    }
  }

  async function openCrosswatchSnapshotPicker(feature, profileId = "") {
    const modal = ensureCrosswatchSnapshotModal(), head = $(".cw-snap-head .hl", modal);
    modal.dataset.feature = feature;
    if (head) head.textContent = featureLabel(feature);
    await loadCrosswatchSnapshotChoices(modal, feature, profileId);
    modal.classList.remove("cw-snap-hidden");
  }
})(window, document);
