/* assets/js/playingcard.js */
/* CrossWatch - Now Playing card driver (scrobble variant of the shared Playing Card) */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  if (window.__PLAYING_CARD_INIT__) return;
  window.__PLAYING_CARD_INIT__ = 1;

  const PM = window.CW?.ProviderMeta || null;
  const FMT = window.CW?.PlayingCard?.fmt || null;

  const getCfg = () => {
    try { return window._cfgCache || null; } catch { return null; }
  };

  const hasTmdbKey = (cfg) => {
    const key = cfg?.tmdb?.api_key;
    return typeof key === "string" ? key.trim().length > 0 : !!key;
  };

  const isUiEnabled = () => {
    const cfg = getCfg();
    const ui = cfg?.ui || {};
    return !!cfg && ui.show_playingcard !== false && hasTmdbKey(cfg);
  };

  const isActiveState = (s) => ["playing", "paused", "buffering"].includes(String(s || "").toLowerCase());

  const visualProgress = (item, nowMs = Date.now()) => {
    const base = Math.max(0, Math.min(100, Number(item?.progress) || 0));
    if (String(item?.state || item?.status || "").toLowerCase() !== "playing") return base;
    const durationMs = Number(item?.duration_ms) || 0;
    const updatedSec = Number(item?.updated) || 0;
    if (!(durationMs > 0) || !(updatedSec > 0)) return base;
    const serverTs = Number(item?._server_ts) || 0;
    const receivedAt = Number(item?._received_at_ms) || 0;
    const nowSec = serverTs && receivedAt ? serverTs + Math.max(0, nowMs - receivedAt) / 1000 : nowMs / 1000;
    return Math.max(base, Math.min(100, base + (Math.max(0, nowSec - updatedSec) * 100000 / durationMs)));
  };

  const keyOf = (p) => {
    const k = String(p?._key || "").trim();
    if (k) return k;

    const sk = String(p?.session_key || "").trim();
    const inst = String(p?.provider_instance || "").trim();
    if (p?.source && sk) return inst ? `${p.source}:${inst}:${sk}` : `${p.source}:${sk}`;
    return [p?.source || "", inst, p?.media_type || p?.type || "", p?.title || "", p?.year || "", p?.season || "", p?.episode || ""].join("|");
  };

  const tmdbIdOf = (p) => window.CW?.Meta?.tmdbId(p) || "";

  const buildTmdbUrl = (p) => {
    const id = tmdbIdOf(p);
    if (!id) return "";
    const resolved = window.CW?.Meta?.peek(p)?.resolved_type;
    const type = String(resolved || p?.media_type || p?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `https://www.themoviedb.org/${type}/${encodeURIComponent(String(id))}`;
  };

  const artTypeOf = (p) => {
    const type = p?.media_type || p?.type || "";
    return String(type).toLowerCase() === "movie" ? "movie" : "tv";
  };

  const artEvidenceOf = (p) => {
    if (window.CW?.Meta?.isEpisode(p)) return "";
    const t = p?.title ? `&title=${encodeURIComponent(String(p.title))}` : "";
    const y = p?.year ? `&year=${encodeURIComponent(String(p.year))}` : "";
    return t + y;
  };

  const buildArtUrl = (p) => {
    if (p?.cover) return p.cover;
    const id = tmdbIdOf(p);
    if (!id) return "/assets/img/placeholder_poster.svg";
    return `/art/tmdb/${artTypeOf(p)}/${encodeURIComponent(String(id))}?size=w342${artEvidenceOf(p)}`;
  };

  const runtimeLabel = (mins) => FMT?.runtimeLabel(mins) || "";
  const formatTime = (ms) => FMT?.formatTime(ms) || "";

  const firstPositive = (...vals) => {
    for (const v of vals) {
      const n = Array.isArray(v) ? Number(v[0]) : Number(v);
      if (Number.isFinite(n) && n > 0) return n;
    }
    return 0;
  };

  const runtimeMinsFor = (p, meta) => {
    if (!meta) return 0;
    const det = meta.detail || {};
    const mt = String(p?.media_type || p?.type || "").toLowerCase();
    if (mt === "episode") {
      return firstPositive(det.episode_run_time, meta.episode_run_time, det.runtime, meta.runtime, meta.runtime_minutes, det.runtime_minutes);
    }
    return firstPositive(meta.runtime_minutes, det.runtime_minutes, meta.runtime, det.runtime);
  };

  const sourceLabel = (src) => {
    const s = String(src || "").toLowerCase();
    if (!s) return "";
    if (PM?.label) {
      const base = s.replace(/trakt$/, "");
      const label = PM.label(base);
      if (label && label !== "?") return s.endsWith("trakt") ? `${label} webhook` : label;
    }
    if (s === "plex") return "PLEX";
    if (s === "emby") return "EMBY";
    if (s === "jellyfin") return "Jellyfin";
    if (s === "plextrakt") return "PLEX webhook";
    if (s === "embytrakt") return "EMBY webhook";
    if (s === "jellyfintrakt") return "Jellyfin webhook";
    return String(src || "");
  };

  const sourceChipClass = (src) => {
    const raw = String(src || "").toLowerCase();
    if (raw.includes("plex")) return "pc-chip-source-plex";
    if (raw.includes("emby")) return "pc-chip-source-emby";
    if (raw.includes("jellyfin")) return "pc-chip-source-jellyfin";
    return "";
  };

  const instanceLabel = (value) => {
    const raw = String(value || "").trim();
    if (!raw || raw.toLowerCase() === "default") return "Default";
    return raw;
  };

  const mediaTypeLabel = (p) => {
    const mediaType = String(p?.media_type || p?.type || "").toLowerCase();
    return mediaType === "movie" ? "Movie" : mediaType === "episode" ? "Episode" : mediaType ? mediaType.toUpperCase() : "TV";
  };

  const streamCount = (p, counts) => Number(p?._streams_count ?? counts.get(keyOf(p)) ?? 0) || 0;

  const statusText = (state, since = "") => {
    const st = String(state || "").toLowerCase();
    let label = st === "paused" ? "Paused" : st === "buffering" ? "Buffering..." : st === "stopped" ? "Stopped" : "Now Playing";
    if (since) label += `\n${since}`;
    return label;
  };

  const sinceLabel = (nowSec, startedSec) => {
    const now = Number(nowSec) || 0;
    const started = Number(startedSec) || 0;
    if (!now || !started) return "";
    const diff = Math.max(0, now - started);
    if (diff < 60) return `${diff}s ago`;
    const min = Math.floor(diff / 60);
    if (min < 60) return `${min}m ago`;
    const hr = Math.floor(min / 60);
    return hr < 24 ? `${hr}h ago` : `${Math.floor(hr / 24)}d ago`;
  };

  const backdropFromMeta = (meta) => {
    const id = meta?.ids?.tmdb;
    if (!id) return "";
    const type = String(meta?.resolved_type || meta?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `/art/tmdb/${type}/${encodeURIComponent(String(id))}?kind=backdrop&size=w1280`;
  };

  const SHARED_WATCH_KEY = "__CW_CURRENT_WATCHING_SHARED__";
  const getMetaFor = (p) => window.CW?.Meta?.get(p, "detail", { seriesInfo: true }) || Promise.resolve(null);

  const countByKey = new Map();
  const CACHE_TTL_MS = 10000;
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  const streamPriority = (p) => {
    const st = String(p?.state || p?.status || "").toLowerCase();
    if (st === "playing") return 0;
    if (st === "buffering") return 1;
    if (st === "paused") return 2;
    return 3;
  };

  const sortStreams = (items) => items.sort((a, b) => {
    const pa = streamPriority(a);
    const pb = streamPriority(b);
    if (pa !== pb) return pa - pb;
    return (Number(b?.updated) || 0) - (Number(a?.updated) || 0);
  });

  const activeStreamsFromPayload = (payload) => {
    const raw = Array.isArray(payload?.streams) ? payload.streams : [];
    const items = raw.filter((x) => x && typeof x === "object" && isActiveState(x.state || x.status));
    return sortStreams(items.slice());
  };

  const fetchCurrentlyWatchingData = async (force = false) => {
    if (authSetupPending()) return { streams: [], primary: null, ts: 0 };
    const now = Date.now();
    const shared = window[SHARED_WATCH_KEY];
    if (!force && shared && typeof shared === "object" && (now - (Number(shared.at) || 0)) < CACHE_TTL_MS) {
      return shared.payload || { streams: [], primary: null, ts: 0 };
    }
    if (!force && CARD.cachePayload && (now - CARD.cacheAt) < CACHE_TTL_MS) return CARD.cachePayload;
    if (CARD.cacheBusy) return CARD.cacheBusy;
    CARD.cacheBusy = (async () => {
      try {
        const r = await fetch("/api/watch/currently_watching", { cache: "no-store" });
        if (!r.ok) return { streams: [], primary: null, ts: 0 };
        const j = await r.json();
        const streams = activeStreamsFromPayload(j);
        const ts = Number(j?.ts) || 0;
        countByKey.clear();
        const receivedAt = Date.now();
        streams.forEach((item) => {
          const k = keyOf(item);
          if (k) countByKey.set(k, streams.length);
          item._streams_count = streams.length;
          if (ts) item._server_ts = ts;
          item._received_at_ms = receivedAt;
        });
        CARD.serverTs = ts || CARD.serverTs || 0;
        const payload = { streams, primary: streams[0] || null, ts };
        CARD.cachePayload = payload;
        CARD.cacheAt = Date.now();
        window[SHARED_WATCH_KEY] = { at: CARD.cacheAt, payload };
        return payload;
      } catch {
        return { streams: [], primary: null, ts: 0 };
      } finally {
        CARD.cacheBusy = null;
      }
    })();
    return CARD.cacheBusy;
  };

  const CARD = {
    selectedKey: "",
    streams: [],
    dismissed: false,
    poll: null,
    tick: null,
    cacheBusy: null,
    cacheAt: 0,
    cachePayload: null,
    serverTs: 0,
    durationByKey: new Map(),
  };

  const card = window.CW.PlayingCard.mount({
    id: "playing-detail",
    variant: "scrobble",
    tabScope: "main",
    label: "Now playing",
    onClose: () => {
      CARD.dismissed = true;
      hide(false);
    },
    onPrev: () => applySelectionOffset(-1),
    onNext: () => applySelectionOffset(1),
  });

  const stopStatusPoll = () => {
    try { if (CARD.poll) clearInterval(CARD.poll); } catch {}
    try { if (CARD.tick) clearInterval(CARD.tick); } catch {}
    CARD.poll = null;
    CARD.tick = null;
  };

  const hide = (resetSelection = false) => {
    card.hide();
    stopStatusPoll();
    if (resetSelection) CARD.selectedKey = "";
  };

  const baseChips = (p, releaseLabel = "") => {
    const chips = [];
    chips.push({ text: sourceLabel(p?.source), cls: sourceChipClass(p?.source) });
    if (p?.provider_instance) chips.push({ text: instanceLabel(p.provider_instance) });
    chips.push({ text: mediaTypeLabel(p) });
    const sc = streamCount(p, countByKey);
    if (sc > 1) chips.push({ text: `${sc} streams`, cls: "pc-chip-streams" });
    if (String(p?.media_type || p?.type || "").toLowerCase() === "episode" && p?.season && p?.episode) {
      chips.push({ text: `S${String(p.season).padStart(2, "0")}E${String(p.episode).padStart(2, "0")}` });
      return chips;
    }
    const yearLabel = p?.year ? String(p.year) : "";
    if (yearLabel && releaseLabel && releaseLabel.startsWith(yearLabel)) {
      chips.push({ text: releaseLabel });
      return chips;
    }
    chips.push({ text: yearLabel });
    if (releaseLabel && releaseLabel !== yearLabel) chips.push({ text: releaseLabel });
    return chips;
  };

  const progressModel = (p) => {
    const key = keyOf(p);
    let totalMs = Number(p?.duration_ms) || 0;
    if (totalMs > 0) {
      CARD.durationByKey.set(key, totalMs);
    } else {
      totalMs = Number(CARD.durationByKey.get(key)) || 0;
    }
    const itemForPct = (totalMs > 0 && !(Number(p?.duration_ms) > 0)) ? { ...p, duration_ms: totalMs } : p;
    const pct = visualProgress(itemForPct);
    let remaining = "";
    if (totalMs > 0) {
      const remainingStr = formatTime(Math.max(0, totalMs - totalMs * (pct / 100)));
      remaining = remainingStr ? `${remainingStr} left` : "";
    }
    return { pct, remaining };
  };

  const statusModel = (p, st) => {
    const updatedSec = Number(p.updated) || 0;
    const startedSec = Number(p.started) || 0;
    const serverTs = Number(p._server_ts) || CARD.serverTs || 0;
    const nowSec = serverTs || Math.floor(Date.now() / 1000);
    const since = updatedSec ? sinceLabel(nowSec, updatedSec) : startedSec ? sinceLabel(nowSec, startedSec) : "";
    return {
      text: statusText(st, since),
      icon: st === "paused" ? "pause" : "play_arrow",
      title: `source=${p.source || ""}, instance=${p.provider_instance || ""}, state=${p.state || p.status || ""}, started=${startedSec || ""}, updated=${updatedSec || ""}`,
    };
  };

  const selectedIndex = () => Math.max(0, CARD.streams.findIndex((item) => keyOf(item) === CARD.selectedKey));
  const selectedStream = () => CARD.streams.find((item) => keyOf(item) === CARD.selectedKey) || CARD.streams[0] || null;

  const ensureSelection = (preferredKey = "") => {
    const keys = new Set(CARD.streams.map((item) => keyOf(item)));
    if (preferredKey && keys.has(preferredKey)) {
      CARD.selectedKey = preferredKey;
      return;
    }
    if (CARD.selectedKey && keys.has(CARD.selectedKey)) return;
    CARD.selectedKey = keyOf(CARD.streams[0] || {});
  };

  function applySelectionOffset(delta) {
    const total = CARD.streams.length;
    if (total <= 1) return;
    const current = selectedIndex();
    const next = (current + delta + total) % total;
    CARD.selectedKey = keyOf(CARD.streams[next]);
    CARD.dismissed = false;
    renderSelectedStream().catch(() => {});
  }

  const startStatusPoll = () => {
    if (!CARD.poll) {
      CARD.poll = setInterval(() => {
        if (!card.isVisible() || document.hidden) return;
        refreshCard(CARD.selectedKey, false).catch(() => {});
      }, 15000);
    }
    if (!CARD.tick) {
      CARD.tick = setInterval(() => {
        if (!card.isVisible() || document.hidden) return;
        const item = selectedStream();
        if (item) card.renderProgress(progressModel(item));
      }, 1000);
    }
  };

  const modelFor = (p, meta) => {
    const isMovie = String(p?.media_type || p?.type || "").toLowerCase() === "movie";
    const st = String(p.state || p.status || "playing").toLowerCase();
    const det = meta?.detail || {};

    let releaseLabel = "";
    if (meta) {
      const releaseRaw = meta.release?.date || det.release_date || det.first_air_date || "";
      if (releaseRaw) releaseLabel = String(releaseRaw).trim().split("T")[0];
    }

    const chips = baseChips(p, releaseLabel);
    if (meta) {
      const runtimeMin = meta.runtime_minutes ?? det.runtime_minutes ?? meta.runtime ?? det.runtime;
      if (runtimeMin) chips.push({ text: runtimeLabel(runtimeMin) });
      const metaRuntimeMin = runtimeMinsFor(p, meta);
      if (metaRuntimeMin > 0) {
        const durKey = keyOf(p);
        if (!(Number(CARD.durationByKey.get(durKey)) > 0)) {
          CARD.durationByKey.set(durKey, metaRuntimeMin * 60 * 1000);
        }
      }
    }

    let rating = null;
    if (meta) {
      const rawScore = Number(meta.score ?? meta.vote_average);
      const rawRating = Number(meta.vote_average ?? det.vote_average);
      const value = Number.isFinite(rawRating) ? rawRating : Number.isFinite(rawScore) ? rawScore / 10 : null;
      rating = { value, votes: meta.vote_count ?? det.vote_count };
    }

    const overview = p?.overview || (meta ? (meta.overview || det.overview || det.tagline || "") : "");
    const tmdbUrl = meta
      ? buildTmdbUrl(Object.assign({}, p, { tmdb: tmdbIdOf(p) || (meta.ids && (meta.ids.tmdb || meta.ids.id)) }))
      : buildTmdbUrl(p);

    return {
      title: p.title || "Now Playing",
      year: p.year || "",
      isMovie,
      chips,
      overview,
      poster: buildArtUrl(p),
      posterHref: tmdbUrl,
      backdrop: meta ? backdropFromMeta(meta) : "",
      information: meta ? FMT.informationFor(meta, isMovie) : "loading",
      rating,
      progress: progressModel(p),
      nav: { index: selectedIndex(), total: CARD.streams.length },
      status: statusModel(p, st),
    };
  };

  async function renderSelectedStream() {
    const p = selectedStream();
    if (!p) {
      hide(true);
      return;
    }

    const state = p.state || p.status || "playing";
    const st = String(state || "").toLowerCase();
    if (!p.title || !isActiveState(st)) {
      hide(true);
      return;
    }

    const updatedSec = Number(p.updated) || 0;
    if (updatedSec) {
      const ageMs = Date.now() - updatedSec * 1000;
      const totalMs = Number(p.duration_ms) || 0;
      const baseProgress = Math.max(0, Math.min(100, Number(p.progress) || 0));
      const expectedRemainingMs = totalMs > 0 ? totalMs * (100 - baseProgress) / 100 : 0;
      const maxAgeMs = st === "paused"
        ? 4 * 60 * 60 * 1000
        : st === "playing" && expectedRemainingMs > 0
          ? Math.max(10 * 60 * 1000, expectedRemainingMs + 10 * 60 * 1000)
          : 10 * 60 * 1000;
      if (ageMs > maxAgeMs) {
        hide(true);
        return;
      }
    }

    const serverTs = Number(p._server_ts) || CARD.serverTs || 0;
    if (serverTs) CARD.serverTs = serverTs;

    card.render(modelFor(p, null));
    card.show();
    startStatusPoll();

    const renderKey = keyOf(p);
    const meta = await getMetaFor(p);
    if (!meta || CARD.selectedKey !== renderKey) return;
    card.render(modelFor(p, meta));
  }

  async function refreshCard(preferredKey = "", force = false) {
    if (!isUiEnabled()) {
      hide(true);
      return;
    }

    const data = await fetchCurrentlyWatchingData(force);
    CARD.streams = Array.isArray(data?.streams) ? data.streams.slice() : [];

    if (!CARD.streams.length) {
      CARD.dismissed = false;
      hide(true);
      return;
    }

    ensureSelection(preferredKey);
    if (CARD.dismissed) {
      hide(false);
      return;
    }
    await renderSelectedStream();
  }

  window.addEventListener("currently-watching-updated", () => {
    refreshCard(CARD.selectedKey, false).catch(() => {});
  });

  window.updatePlayingCard = (payload) => refreshCard(keyOf(payload || {}), false);
  refreshCard("", false).catch(() => {});
})();
