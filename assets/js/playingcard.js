/* assets/js/playingcard.js */
/* CrossWatch - Now Playing Card UI */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  if (window.__PLAYING_CARD_INIT__) return;
  window.__PLAYING_CARD_INIT__ = 1;

  const PM = window.CW?.ProviderMeta || null;

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

  const tmdbIdOf = (p) => {
    const mt = String(p?.media_type || p?.type || "").toLowerCase();
    const ids = p?.ids || {};
    return mt === "episode"
      ? ids.tmdb_show || p?.tmdb_show || p?.tmdb || p?.tmdb_id || ids.tmdb || ids.id
      : p?.tmdb || p?.tmdb_id || ids.tmdb || ids.tmdb_show || ids.id;
  };

  const buildTmdbUrl = (p) => {
    const id = tmdbIdOf(p);
    if (!id) return "";
    const type = String(p?.media_type || p?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `https://www.themoviedb.org/${type}/${encodeURIComponent(String(id))}`;
  };

  const buildArtUrl = (p) => {
    if (p?.cover) return p.cover;
    const id = tmdbIdOf(p);
    if (!id) return "/assets/img/placeholder_poster.svg";
    const type = String(p?.media_type || p?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `/art/tmdb/${type}/${encodeURIComponent(String(id))}?size=w342`;
  };

  const buildBackdropUrl = (p) => {
    const id = tmdbIdOf(p);
    if (!id) return "";
    const type = String(p?.media_type || p?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `/art/tmdb/${type}/${encodeURIComponent(String(id))}?kind=backdrop&size=w1280`;
  };

  const runtimeLabel = (mins) => {
    const m = Number(mins) || 0;
    if (!m) return "";
    const h = Math.floor(m / 60);
    const mm = m % 60;
    return h ? `${h}h ${mm ? `${mm}m` : ""}` : `${mm}m`;
  };

  const formatTime = (ms) => {
    const totalMs = Number(ms) || 0;
    if (!totalMs) return "";
    const totalSec = Math.floor(totalMs / 1000);
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}` : `${m}:${String(s).padStart(2, "0")}`;
  };

  const metaKey = (p) => `${String(p?.media_type || p?.type || "").toLowerCase()}:${String(tmdbIdOf(p) || "")}`;
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
    const type = String(meta?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `/art/tmdb/${type}/${encodeURIComponent(String(id))}?kind=backdrop&size=w1280`;
  };

  const metaCache = new Map();
  const metaInflight = new Map();
  const SHARED_WATCH_KEY = "__CW_CURRENT_WATCHING_SHARED__";
  const getMetaFor = async (p) => {
    const k = metaKey(p);
    const hit = metaCache.get(k);
    if (hit) return hit;
    const inflight = metaInflight.get(k);
    if (inflight) return inflight;

    const tmdb = String(tmdbIdOf(p) || "");
    if (!tmdb) return null;
    const mt = String(p?.media_type || p?.type || "").toLowerCase();
    const type = mt === "movie" ? "movie" : "show";
    const need = { overview: 1, runtime_minutes: 1, ids: 1, videos: 1, genres: 1, certification: 1, score: 1, vote_count: 1, release: 1, backdrop: 1 };
    if (type === "show") need.series_info = 1;

    const req = (async () => {
      try {
      const r = await fetch("/api/metadata/bulk?overview=full", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: [{ type, tmdb }],
          need,
          concurrency: 1
        })
      });
      if (!r.ok) return null;
      const data = await r.json();
      const first = Object.values(data?.results || {})[0];
      const meta = first?.ok ? (first.meta || null) : null;
      if (meta) metaCache.set(k, meta);
      return meta;
      } catch {
        return null;
      } finally {
        metaInflight.delete(k);
      }
    })();
    metaInflight.set(k, req);
    return req;
  };

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

  const css = `
  #playing-detail{position:fixed;left:50%;bottom:20px;transform:translate(-50%,calc(100% + 30px));width:min(720px,calc(100vw - 420px));background:radial-gradient(120% 140% at 0% 0%,rgba(91,73,197,.18) 0%,rgba(91,73,197,0) 38%),radial-gradient(100% 140% at 100% 100%,rgba(36,118,215,.12) 0%,rgba(36,118,215,0) 42%),linear-gradient(180deg,rgba(7,10,18,.985),rgba(3,5,10,.992));border:1px solid rgba(255,255,255,.06);border-radius:20px;box-shadow:0 24px 64px rgba(0,0,0,.56),inset 0 1px 0 rgba(255,255,255,.03);color:#fff;opacity:0;transition:transform .35s cubic-bezier(.22,.7,.25,1),opacity .25s ease-out,box-shadow .25s ease-out;z-index:10000;overflow:hidden;isolation:isolate}
  #playing-detail.show{transform:translate(-50%,0);opacity:1}
  #playing-detail.show:hover{transform:translate(-50%,-3px);box-shadow:0 28px 72px rgba(0,0,0,.62),inset 0 1px 0 rgba(255,255,255,.035)}
  html[data-tab!="main"] #playing-detail{display:none!important}
  #playing-detail::before{content:"";position:absolute;inset:0;border-radius:inherit;background-image:linear-gradient(90deg,rgba(25,29,38,.92) 0%,rgba(25,29,38,.76) 42%,rgba(25,29,38,.88) 100%),var(--pc-backdrop,none);background-size:100% 100%,cover;background-position:center center,center center;background-repeat:no-repeat,no-repeat;pointer-events:none;z-index:0}
  #playing-detail .pc-body{position:relative;display:flex;flex-direction:column;justify-content:flex-start;padding-bottom:0}
  #playing-detail .pc-inner{position:relative;z-index:1;display:grid;grid-template-columns:104px 1fr 170px;gap:14px;align-items:stretch;padding:14px}
  #playing-detail .pc-poster-link{display:block;width:104px;border-radius:14px;overflow:hidden;background:#05070d;text-decoration:none}
  #playing-detail .pc-poster-link[href]{cursor:pointer}
  #playing-detail .pc-poster{width:104px;border:1px solid rgba(255,255,255,.06);border-radius:14px;object-fit:cover;box-shadow:0 14px 30px rgba(0,0,0,.44),inset 0 1px 0 rgba(255,255,255,.04);background:#05070d}
  #playing-detail .pc-title-row{display:flex;align-items:flex-start;gap:8px}
  #playing-detail .pc-title{font-weight:700;font-size:17px;letter-spacing:.005em;line-height:1.2}
  #playing-detail .pc-title-actions{margin-left:auto;display:flex;align-items:center;justify-content:flex-end;gap:8px}
  #playing-detail .pc-nav{display:inline-flex;align-items:center;gap:6px;padding:3px 6px;border:1px solid rgba(255,255,255,.08);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018))}
  #playing-detail .pc-nav[hidden]{display:none!important}
  #playing-detail .pc-nav-btn{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border:0;border-radius:999px;background:rgba(255,255,255,.04);color:#eef3ff;cursor:pointer;transition:background .18s ease,transform .18s ease,opacity .18s ease}
  #playing-detail .pc-nav-btn .material-symbols-rounded{font-size:18px;line-height:1}
  #playing-detail .pc-nav-btn:hover{background:rgba(255,255,255,.09);transform:translateY(-1px)}
  #playing-detail .pc-nav-btn:disabled{opacity:.35;cursor:default;transform:none}
  #playing-detail .pc-nav-count{min-width:40px;text-align:center;font-size:11px;font-weight:800;letter-spacing:.08em;color:rgba(236,241,251,.88)}
  #playing-detail .pc-close{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;width:32px;height:32px;min-width:32px;margin:0;padding:0;border:1px solid rgba(255,255,255,.08);border-radius:50%;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018));color:rgba(232,238,252,.78);cursor:pointer;line-height:1;transition:background .18s ease,border-color .18s ease,color .18s ease,transform .18s ease}
  #playing-detail .pc-close .material-symbols-rounded{font-size:18px;line-height:1;font-variation-settings:"FILL" 0,"wght" 400,"GRAD" 0,"opsz" 20}
  #playing-detail .pc-close:hover{color:#fff;border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(255,255,255,.075),rgba(255,255,255,.03));transform:translateY(-1px)}
  #playing-detail .pc-meta{display:flex;flex-wrap:wrap;gap:5px;margin-top:6px}
  #playing-detail .pc-chip{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 9px;border:1px solid rgba(255,255,255,.07);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.022));box-shadow:inset 0 1px 0 rgba(255,255,255,.03);font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:rgba(236,241,251,.86);line-height:1}
  .pc-chip-streams{background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.032))}
  #playing-detail .pc-overview{margin-top:8px;font-size:12px;line-height:1.45;color:rgba(211,219,234,.7);max-height:3.2em;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
  #playing-detail .pc-overview-wrap{position:static;min-width:0;min-height:0}
  #playing-detail .pc-overview-more{position:absolute;right:2px;bottom:2px;z-index:2;padding:0;border:0;background:transparent;color:rgba(236,241,251,.72);font:inherit;font-size:10px;font-weight:700;line-height:1;cursor:pointer}
  #playing-detail .pc-overview-more:hover{color:#fff}
  #playing-detail .pc-overview-more[hidden]{display:none}
  #playing-detail .pc-overview-wrap.is-expanded .pc-overview{display:block;overflow-x:hidden;overflow-y:auto;text-overflow:clip;-webkit-line-clamp:unset;scrollbar-width:thin;scrollbar-color:rgba(126,226,184,.55) rgba(255,255,255,.06)}
  #playing-detail .pc-progress-wrap{margin-top:auto;position:relative;width:100%;max-width:100%;box-sizing:border-box}
  #playing-detail .pc-progress-bg{position:relative;width:100%;height:22px;border:1px solid rgba(255,255,255,.06);border-radius:999px;background:linear-gradient(180deg,rgba(12,16,27,.96),rgba(8,11,19,.98));overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
  #playing-detail .pc-progress{width:0;height:100%;background:linear-gradient(90deg,#5fb6ff,#7ee2b8);transition:width .4s cubic-bezier(.22,.7,.25,1)}
  #playing-detail .pc-progress::after{content:"";position:absolute;inset:0;border-radius:999px;box-shadow:0 0 18px rgba(84,124,255,.22);pointer-events:none}
  #playing-detail .pc-progress-labels{position:absolute;inset:0;display:flex;align-items:center;justify-content:space-between;padding:0 10px;pointer-events:none;font-size:11px;font-weight:700;color:rgba(236,241,251,.92);text-shadow:0 1px 2px rgba(0,0,0,.8);box-sizing:border-box}
  #playing-detail .pc-info-block{display:grid;align-content:center;gap:4px;min-width:0;min-height:62px;padding:8px 10px;border:1px solid rgba(255,255,255,.14);border-radius:10px;background:rgba(20,26,36,.5);box-sizing:border-box}
  #playing-detail .pc-info-label{display:flex;align-items:center;gap:5px;min-width:0;font-size:8px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:rgba(200,209,226,.62);white-space:nowrap}
  #playing-detail .pc-info-value{display:flex;align-items:center;gap:6px;min-width:0;font-size:17px;font-weight:800;line-height:1}
  #playing-detail .pc-info-note{font-size:8px;line-height:1.1;color:rgba(200,209,226,.5);white-space:nowrap}
  #playing-detail .pc-info-icon{font-size:21px;line-height:1;font-variation-settings:"FILL" 1,"wght" 550,"GRAD" 0,"opsz" 22}
  #playing-detail .pc-information-block{align-content:start;gap:6px;min-height:80px}
  #playing-detail .pc-information-block.is-series{min-height:100px}
  #playing-detail .pc-information-rows{display:grid;gap:5px;min-width:0}
  #playing-detail .pc-information-row{display:grid;grid-template-columns:15px minmax(0,1fr);align-items:start;gap:6px;min-width:0;color:rgba(222,229,241,.78)}
  #playing-detail .pc-information-row-icon{font-size:15px;line-height:1.1;color:#22c55e;font-variation-settings:"FILL" 1,"wght" 500,"GRAD" 0,"opsz" 18}
  #playing-detail .pc-information-copy{min-width:0;font-size:9px;line-height:1.2}
  #playing-detail .pc-information-main{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  #playing-detail .pc-information-sub{display:block;margin-top:2px;color:rgba(200,209,226,.48);font-size:8px;white-space:nowrap}
  #playing-detail .pc-rating-block{--pc-rating-color:#8b93a7}
  #playing-detail .pc-rating-block .pc-info-value{color:var(--pc-rating-color)}
  #playing-detail .pc-rating-block.rating-low{--pc-rating-color:#ef4444}
  #playing-detail .pc-rating-block.rating-mid{--pc-rating-color:#f59e0b}
  #playing-detail .pc-rating-block.rating-high{--pc-rating-color:#22c55e}
  #playing-detail .pc-status{position:static;font-size:10px;font-weight:700;line-height:1.45;letter-spacing:.08em;text-transform:uppercase;color:rgba(236,241,251,.88);opacity:.96;white-space:pre-line;text-align:left}
  #playing-detail,
  #playing-detail .pc-nav,
  #playing-detail .pc-nav-btn,
  #playing-detail .pc-close,
  #playing-detail .pc-chip,
  #playing-detail .pc-progress-bg,
  #playing-detail .pc-info-block,
  #playing-detail .pc-status{
    background:#20242d!important;
    border-color:rgba(255,255,255,.14)!important;
    box-shadow:none!important;
    text-shadow:none!important;
    filter:none!important;
  }
  #playing-detail .pc-progress::after{
    content:none!important;
    display:none!important;
    background:none!important;
    box-shadow:none!important;
  }
  #playing-detail::before{
    content:""!important;
    display:block!important;
    background-image:linear-gradient(90deg,rgba(32,36,45,.92) 0%,rgba(32,36,45,.76) 42%,rgba(32,36,45,.88) 100%),var(--pc-backdrop,none)!important;
    background-size:100% 100%,cover!important;
    background-position:center center,center center!important;
    background-repeat:no-repeat,no-repeat!important;
    opacity:1!important;
    filter:none!important;
    box-shadow:none!important;
  }
  #playing-detail.show:hover,
  #playing-detail .pc-nav-btn:hover,
  #playing-detail .pc-close:hover{
    background:#2b313d!important;
    border-color:rgba(255,255,255,.19)!important;
    box-shadow:none!important;
    filter:none!important;
    transform:translate(-50%,0)!important;
  }
  #playing-detail .pc-nav-btn:hover,
  #playing-detail .pc-close:hover{
    transform:none!important;
  }
  #playing-detail .pc-poster{
    box-shadow:none!important;
    filter:none!important;
    text-shadow:none!important;
  }
  #playing-detail .pc-progress{
    background:linear-gradient(90deg,#5fb6ff,#7ee2b8)!important;
    box-shadow:none!important;
  }
  #playing-detail .pc-info-block{
    background:linear-gradient(145deg,rgba(21,28,39,.62),rgba(14,20,30,.42))!important;
    border-color:rgba(255,255,255,.16)!important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.055),0 10px 28px rgba(0,0,0,.12)!important;
    -webkit-backdrop-filter:blur(12px) saturate(115%);
    backdrop-filter:blur(12px) saturate(115%);
  }
  html[data-cw-theme="flat-light"] #playing-detail,
  html[data-cw-theme="flat-light"] #playing-detail .pc-nav,
  html[data-cw-theme="flat-light"] #playing-detail .pc-nav-btn,
  html[data-cw-theme="flat-light"] #playing-detail .pc-close,
  html[data-cw-theme="flat-light"] #playing-detail .pc-chip,
  html[data-cw-theme="flat-light"] #playing-detail .pc-progress-bg,
  html[data-cw-theme="flat-light"] #playing-detail .pc-info-block,
  html[data-cw-theme="flat-light"] #playing-detail .pc-status{
    background:#ffffff!important;
    border-color:rgba(21,31,48,.14)!important;
    color:#172033!important;
  }
  html[data-cw-theme="flat-light"] #playing-detail.show:hover,
  html[data-cw-theme="flat-light"] #playing-detail .pc-nav-btn:hover,
  html[data-cw-theme="flat-light"] #playing-detail .pc-close:hover{
    background:#eef2f7!important;
    border-color:rgba(21,31,48,.20)!important;
  }
  html[data-cw-theme="flat-light"] #playing-detail .pc-progress{
    background:linear-gradient(90deg,#5fb6ff,#7ee2b8)!important;
  }
  html[data-cw-theme="flat-light"] #playing-detail .pc-info-block{
    background:linear-gradient(145deg,rgba(255,255,255,.72),rgba(244,247,251,.58))!important;
    border-color:rgba(21,31,48,.14)!important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.72),0 10px 26px rgba(31,41,55,.08)!important;
  }
  html[data-cw-theme="flat-light"] #playing-detail .pc-overview-more{color:rgba(23,32,51,.68)}
  html[data-cw-theme="flat-light"] #playing-detail .pc-overview-more:hover{color:#172033}
  html[data-cw-theme="flat-light"] #playing-detail::before{
    background-image:linear-gradient(90deg,rgba(255,255,255,.94) 0%,rgba(255,255,255,.80) 42%,rgba(255,255,255,.92) 100%),var(--pc-backdrop,none)!important;
  }
  /* Split Info Layout: preserve the current styling while reducing card height. */
  #playing-detail{width:min(780px,calc(100vw - 60px))}
  #playing-detail .pc-inner{grid-template-columns:126px minmax(235px,1fr) minmax(320px,350px);gap:14px;align-items:stretch;padding:0 14px 0 0;min-height:190px;box-sizing:border-box}
  #playing-detail .pc-poster-link{width:126px;height:190px;align-self:stretch;border-right:1px solid rgba(255,255,255,.06);border-radius:19px 0 0 19px;box-sizing:border-box}
  #playing-detail .pc-poster{display:block;width:100%;height:100%;border:0;border-radius:inherit;box-shadow:none;box-sizing:border-box}
  #playing-detail .pc-body{min-width:0;padding:14px 0;justify-content:flex-start}
  #playing-detail .pc-title{font-size:18px;line-height:1.15}
  #playing-detail .pc-overview{margin-top:10px;max-height:7.25em;line-height:1.45;-webkit-line-clamp:5}
  #playing-detail .pc-close{position:static;width:24px;height:24px;min-width:24px}
  #playing-detail .pc-stats{min-width:0;display:grid;grid-template-rows:auto auto;align-content:center;gap:9px;padding:14px 6px 14px 14px;border-left:1px solid rgba(255,255,255,.08)}
  #playing-detail .pc-progress-wrap{margin:0;align-self:start}
  #playing-detail .pc-progress-labels{position:static;display:flex;align-items:center;justify-content:space-between;min-height:24px;margin-bottom:5px;padding:0;pointer-events:auto;font-size:11px;text-shadow:none}
  #playing-detail .pc-progress-end{display:flex;align-items:center;justify-content:flex-end;gap:8px;min-width:0}
  #playing-detail .pc-progress-bg{height:8px}
  #playing-detail .pc-stats-bottom{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));align-items:start;gap:8px;min-width:0}
  #playing-detail .pc-info-block{min-height:62px}
  #playing-detail .pc-rating-stack{display:flex;flex-direction:column;gap:7px;min-width:0}
  #playing-detail .pc-status-head{display:flex;align-items:center;justify-content:center;gap:7px;min-width:0;padding:2px 2px 0}
  #playing-detail .pc-status-icon{flex:0 0 auto;margin-top:-1px;color:#22c55e;font-size:21px;line-height:1;font-variation-settings:"FILL" 1,"wght" 650,"GRAD" 0,"opsz" 22;filter:drop-shadow(0 0 6px rgba(34,197,94,.28))}
  #playing-detail .pc-status{min-width:0;border:0!important;background:transparent!important;text-align:center}
  @media (max-width:1024px){#playing-detail{width:min(780px,calc(100vw - 40px))}#playing-detail .pc-inner{grid-template-columns:112px minmax(220px,1fr) minmax(230px,250px)}#playing-detail .pc-poster-link{width:112px;height:190px}}
  @media (max-width:820px){#playing-detail .pc-inner{grid-template-columns:82px minmax(0,1fr) minmax(210px,240px);min-height:0;padding:12px}#playing-detail .pc-poster-link{width:82px;height:123px;grid-row:1 / span 2;align-self:center;border:1px solid rgba(255,255,255,.06);border-radius:11px}#playing-detail .pc-body{padding:4px 0}}
  @media (max-width:680px){#playing-detail{bottom:max(10px,env(safe-area-inset-bottom));width:calc(100vw - 20px);border-radius:18px}#playing-detail .pc-inner{grid-template-columns:64px minmax(0,1fr);gap:12px;padding:12px}#playing-detail .pc-poster-link{width:64px;height:96px;grid-row:auto;border-radius:10px}#playing-detail .pc-body{padding:2px 0}#playing-detail .pc-title{font-size:15px;line-height:1.15}#playing-detail .pc-title-actions{gap:6px}#playing-detail .pc-nav{padding:2px 4px}#playing-detail .pc-nav-count{min-width:32px;font-size:10px}#playing-detail .pc-nav-btn{width:24px;height:24px}#playing-detail .pc-meta{gap:4px;margin-top:4px}#playing-detail .pc-chip{min-height:21px;font-size:9px;padding:0 7px}#playing-detail .pc-overview{margin-top:6px;max-height:4.35em;font-size:11px;-webkit-line-clamp:3}#playing-detail .pc-overview-more{font-size:9px}#playing-detail .pc-stats{grid-column:1 / -1;grid-template-rows:auto auto;padding:10px 0 0;border-left:0;border-top:1px solid rgba(255,255,255,.08)}#playing-detail .pc-info-block{min-height:58px}}
  @media (max-width:460px){#playing-detail .pc-stats-bottom{grid-template-columns:1fr}#playing-detail .pc-overview{-webkit-line-clamp:1;max-height:1.5em}}
  @media (hover:none){#playing-detail.show:hover{transform:translate(-50%,0);box-shadow:0 20px 48px rgba(0,0,0,.6)}}
  `;

  const style = document.createElement("style");
  style.textContent = css;
  document.head.appendChild(style);

  const detail = document.createElement("div");
  detail.id = "playing-detail";
  detail.setAttribute("aria-live", "polite");
  detail.innerHTML = `
    <div class="pc-inner">
      <a id="pc-poster-link" class="pc-poster-link" target="_blank" rel="noopener noreferrer">
        <img id="pc-poster" class="pc-poster" src="/assets/img/placeholder_poster.svg" alt="">
      </a>
      <div class="pc-body">
        <div class="pc-title-row">
          <div id="pc-title" class="pc-title">Now Playing</div>
          <div class="pc-title-actions">
            <div id="pc-nav" class="pc-nav" hidden>
              <button id="pc-prev" class="pc-nav-btn" type="button" aria-label="Previous stream"><span class="material-symbols-rounded">chevron_left</span></button>
              <span id="pc-nav-count" class="pc-nav-count">1 / 1</span>
              <button id="pc-next" class="pc-nav-btn" type="button" aria-label="Next stream"><span class="material-symbols-rounded">chevron_right</span></button>
            </div>
          </div>
        </div>
        <div id="pc-meta" class="pc-meta"></div>
        <div id="pc-overview-wrap" class="pc-overview-wrap">
          <div id="pc-overview" class="pc-overview"></div>
          <button id="pc-overview-more" class="pc-overview-more" type="button" aria-expanded="false" hidden>More</button>
        </div>
      </div>
      <div class="pc-stats">
        <div class="pc-progress-wrap">
          <div class="pc-progress-labels">
            <span id="pc-progress-pct"></span>
            <span class="pc-progress-end">
              <span id="pc-progress-time"></span>
              <button id="pc-close" class="pc-close" type="button" title="Hide" aria-label="Hide Playing card"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
            </span>
          </div>
          <div class="pc-progress-bg"><div id="pc-progress" class="pc-progress"></div></div>
        </div>
        <div class="pc-stats-bottom">
          <div id="pc-information-block" class="pc-info-block pc-information-block">
            <div class="pc-info-label">Information</div>
            <div id="pc-information-rows" class="pc-information-rows"></div>
          </div>
          <div class="pc-rating-stack">
            <div id="pc-rating-block" class="pc-info-block pc-rating-block">
              <div class="pc-info-label">TMDB Rating</div>
              <div class="pc-info-value"><span class="material-symbols-rounded pc-info-icon" aria-hidden="true">star</span><span id="pc-rating">--</span></div>
              <div id="pc-rating-votes" class="pc-info-note">Rating unavailable</div>
            </div>
            <div class="pc-status-head"><span id="pc-status-icon" class="material-symbols-rounded pc-status-icon" aria-hidden="true">play_arrow</span><div id="pc-status" class="pc-status">Now Playing</div></div>
          </div>
        </div>
      </div>
    </div>

  `;
  document.body.appendChild(detail);

  const posterEl = detail.querySelector("#pc-poster");
  const posterLinkEl = detail.querySelector("#pc-poster-link");
  const titleEl = detail.querySelector("#pc-title");
  const metaEl = detail.querySelector("#pc-meta");
  const overviewEl = detail.querySelector("#pc-overview");
  const overviewWrapEl = detail.querySelector("#pc-overview-wrap");
  const overviewMoreBtn = detail.querySelector("#pc-overview-more");
  const progEl = detail.querySelector("#pc-progress");
  const progPctEl = detail.querySelector("#pc-progress-pct");
  const progTimeEl = detail.querySelector("#pc-progress-time");
  const informationBlockEl = detail.querySelector("#pc-information-block");
  const informationRowsEl = detail.querySelector("#pc-information-rows");
  const ratingBlockEl = detail.querySelector("#pc-rating-block");
  const ratingEl = detail.querySelector("#pc-rating");
  const ratingVotesEl = detail.querySelector("#pc-rating-votes");
  const statusEl = detail.querySelector("#pc-status");
  const statusIconEl = detail.querySelector("#pc-status-icon");
  const closeBtn = detail.querySelector("#pc-close");
  const navWrap = detail.querySelector("#pc-nav");
  const prevBtn = detail.querySelector("#pc-prev");
  const nextBtn = detail.querySelector("#pc-next");
  const navCountEl = detail.querySelector("#pc-nav-count");

  posterEl.onerror = () => {
    posterEl.onerror = null;
    posterEl.src = "/assets/img/placeholder_poster.svg";
  };

  let overviewMeasureFrame = 0;
  const updateOverviewMore = () => {
    if (overviewMeasureFrame) cancelAnimationFrame(overviewMeasureFrame);
    overviewMeasureFrame = requestAnimationFrame(() => {
      overviewMeasureFrame = 0;
      if (overviewWrapEl.classList.contains("is-expanded")) return;
      const hasOverflow = !!overviewEl.textContent.trim() && overviewEl.scrollHeight > overviewEl.clientHeight + 1;
      overviewWrapEl.classList.toggle("has-overflow", hasOverflow);
      overviewMoreBtn.hidden = !hasOverflow;
    });
  };

  const setOverview = (value) => {
    overviewWrapEl.classList.remove("is-expanded");
    overviewEl.scrollTop = 0;
    overviewEl.textContent = String(value || "");
    overviewMoreBtn.textContent = "More";
    overviewMoreBtn.setAttribute("aria-expanded", "false");
    overviewMoreBtn.hidden = true;
    updateOverviewMore();
  };

  overviewMoreBtn.addEventListener("click", () => {
    const expanded = overviewWrapEl.classList.toggle("is-expanded");
    overviewEl.scrollTop = 0;
    overviewMoreBtn.textContent = expanded ? "Less" : "More";
    overviewMoreBtn.setAttribute("aria-expanded", expanded ? "true" : "false");
    if (!expanded) updateOverviewMore();
  });

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
  };

  const stopStatusPoll = () => {
    try { if (CARD.poll) clearInterval(CARD.poll); } catch {}
    try { if (CARD.tick) clearInterval(CARD.tick); } catch {}
    CARD.poll = null;
    CARD.tick = null;
  };
  const hide = (resetSelection = false) => {
    detail.classList.remove("show");
    stopStatusPoll();
    if (resetSelection) CARD.selectedKey = "";
  };

  const addChip = (txt, extraClass = "") => {
    const t = String(txt || "").trim();
    if (!t) return;
    const span = document.createElement("span");
    span.className = "pc-chip";
    if (extraClass) span.classList.add(extraClass);
    span.textContent = t;
    metaEl.appendChild(span);
  };

  const renderBaseMeta = (p, releaseLabel = "") => {
    metaEl.innerHTML = "";
    addChip(sourceLabel(p?.source), sourceChipClass(p?.source));
    if (p?.provider_instance) addChip(instanceLabel(p.provider_instance));
    addChip(mediaTypeLabel(p));
    const sc = streamCount(p, countByKey);
    if (sc > 1) addChip(`${sc} streams`, "pc-chip-streams");
    if (String(p?.media_type || p?.type || "").toLowerCase() === "episode" && p?.season && p?.episode) {
      addChip(`S${String(p.season).padStart(2, "0")}E${String(p.episode).padStart(2, "0")}`);
      return;
    }
    const yearLabel = p?.year ? String(p.year) : "";
    if (yearLabel && releaseLabel && releaseLabel.startsWith(yearLabel)) {
      addChip(releaseLabel);
      return;
    }
    addChip(yearLabel);
    if (releaseLabel && releaseLabel !== yearLabel) addChip(releaseLabel);
  };

  const formatDateLabel = (raw) => {
    const value = String(raw || "").trim().split("T")[0];
    const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) return value;
    const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
    return date.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
  };

  const informationRow = (icon, main, sub = "") => {
    const row = document.createElement("div");
    row.className = "pc-information-row";
    const iconEl = document.createElement("span");
    iconEl.className = "material-symbols-rounded pc-information-row-icon";
    iconEl.setAttribute("aria-hidden", "true");
    iconEl.textContent = icon;
    const copy = document.createElement("span");
    copy.className = "pc-information-copy";
    const mainEl = document.createElement("span");
    mainEl.className = "pc-information-main";
    mainEl.textContent = main;
    mainEl.title = main;
    copy.appendChild(mainEl);
    if (sub) {
      const subEl = document.createElement("span");
      subEl.className = "pc-information-sub";
      subEl.textContent = sub;
      copy.appendChild(subEl);
    }
    row.append(iconEl, copy);
    return row;
  };

  const genreLabel = (meta, det) => {
    const raw = meta?.genres || det?.genres || [];
    return (Array.isArray(raw) ? raw : [])
      .map((genre) => typeof genre === "string" ? genre : genre?.name)
      .map((genre) => String(genre || "").trim())
      .filter(Boolean)
      .join(", ") || "Genres unavailable";
  };

  const nextEpisodeLabels = (nextEpisode) => {
    if (!nextEpisode || typeof nextEpisode !== "object") return ["No upcoming episode", ""];
    const season = Number(nextEpisode.season_number);
    const episode = Number(nextEpisode.episode_number);
    const code = Number.isInteger(season) && Number.isInteger(episode)
      ? `S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`
      : "Next episode";
    const airDate = String(nextEpisode.air_date || "").trim();
    let timing = "";
    const match = airDate.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (match) {
      const target = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
      const now = new Date();
      const today = Date.UTC(now.getFullYear(), now.getMonth(), now.getDate());
      const days = Math.round((target - today) / 86400000);
      timing = days === 0 ? "Airs today" : days === 1 ? "Airs tomorrow" : days > 1 ? `Airs in ${days} days` : "Previously aired";
    }
    return [[code, timing].filter(Boolean).join(" · "), formatDateLabel(airDate)];
  };

  const renderInformation = (p, meta, det, runtimeMin, releaseRaw) => {
    const isMovie = String(p?.media_type || p?.type || "").toLowerCase() === "movie";
    informationBlockEl.classList.toggle("is-series", !isMovie);
    informationRowsEl.replaceChildren();
    informationRowsEl.appendChild(informationRow("sell", genreLabel(meta, det)));
    if (isMovie) {
      informationRowsEl.appendChild(informationRow("calendar_month", formatDateLabel(releaseRaw) || "Release date unavailable"));
      informationRowsEl.appendChild(informationRow("schedule", runtimeLabel(runtimeMin) || "Runtime unavailable"));
      return;
    }

    informationRowsEl.appendChild(informationRow("tv", String(det?.status || "Status unavailable")));
    const seasons = Number(det?.number_of_seasons);
    const episodes = Number(det?.number_of_episodes);
    const totals = [
      Number.isFinite(seasons) && seasons > 0 ? `${seasons} Season${seasons === 1 ? "" : "s"}` : "",
      Number.isFinite(episodes) && episodes > 0 ? `${episodes} Episode${episodes === 1 ? "" : "s"}` : "",
    ].filter(Boolean).join(" · ") || "Series totals unavailable";
    informationRowsEl.appendChild(informationRow("layers", totals));
    const [nextMain, nextSub] = nextEpisodeLabels(det?.next_episode_to_air);
    informationRowsEl.appendChild(informationRow("arrow_forward", nextMain, nextSub));
  };

  const setInformationLoading = (p) => {
    const isMovie = String(p?.media_type || p?.type || "").toLowerCase() === "movie";
    informationBlockEl.classList.toggle("is-series", !isMovie);
    informationRowsEl.replaceChildren(informationRow("hourglass_empty", "Loading information..."));
  };

  const setRatingState = (rawRating, rawVoteCount) => {
    const rating = Number(rawRating);
    const available = Number.isFinite(rating) && rating >= 1 && rating <= 10;
    ratingBlockEl.classList.toggle("rating-low", available && rating < 5);
    ratingBlockEl.classList.toggle("rating-mid", available && rating >= 5 && rating < 7);
    ratingBlockEl.classList.toggle("rating-high", available && rating >= 7);
    ratingEl.textContent = available ? rating.toFixed(1) : "--";

    const votes = Number(rawVoteCount);
    ratingVotesEl.textContent = !available
      ? "Rating unavailable"
      : Number.isFinite(votes) && votes > 0
        ? `${votes.toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 })} votes`
        : "0 votes";
  };

  const setPosterLink = (href, title = "") => {
    if (!href) {
      posterLinkEl.removeAttribute("href");
      posterLinkEl.removeAttribute("aria-label");
      posterLinkEl.removeAttribute("title");
      posterLinkEl.setAttribute("aria-disabled", "true");
      return;
    }
    posterLinkEl.href = href;
    posterLinkEl.setAttribute("aria-label", `Open ${title || "title"} on TMDb`);
    posterLinkEl.title = `Open ${title || "title"} on TMDb`;
    posterLinkEl.removeAttribute("aria-disabled");
  };

  const applyMeta = (p, meta) => {
    if (!meta) return;
    const backdrop = backdropFromMeta(meta);
    const det = meta.detail || {};
    let releaseRaw = meta.release?.date || det.release_date || det.first_air_date || "";
    let releaseLabel = "";

    if (releaseRaw) {
      let s = String(releaseRaw).trim();
      if (s.includes("T")) s = s.split("T")[0];
      releaseLabel = s;
    }
    renderBaseMeta(p, releaseLabel);
    const runtimeMin = meta.runtime_minutes ?? det.runtime_minutes ?? meta.runtime ?? det.runtime;
    if (runtimeMin) addChip(runtimeLabel(runtimeMin));
    if (!p?.duration_ms && runtimeMin) {
      const pct = Math.max(0, Math.min(100, Number(p?.progress) || 0));
      if (pct > 0) {
        const totalMs = Number(runtimeMin) * 60 * 1000;
        if (totalMs > 0) {
          const remainingMs = Math.max(0, totalMs - totalMs * (pct / 100));
          const remainingStr = formatTime(remainingMs);
          if (remainingStr && !progTimeEl.textContent) progTimeEl.textContent = `${remainingStr} left`;
        }
      }
    }
    if (!p?.overview) {
      const ov = meta.overview || det.overview || det.tagline;
      if (ov) setOverview(ov);
    }
    const rawScore = Number(meta.score ?? meta.vote_average);
    const rawRating = Number(meta.vote_average ?? det.vote_average);
    const rating = Number.isFinite(rawRating)
      ? rawRating
      : Number.isFinite(rawScore) ? rawScore / 10 : null;
    setRatingState(rating, meta.vote_count ?? det.vote_count);
    renderInformation(p, meta, det, runtimeMin, releaseLabel || releaseRaw);
    const tmdbUrl = buildTmdbUrl(Object.assign({}, p, { tmdb: tmdbIdOf(p) || (meta.ids && (meta.ids.tmdb || meta.ids.id)) }));
    setPosterLink(tmdbUrl, p?.title);
    detail.style.setProperty("--pc-backdrop", backdrop ? `url("${backdrop}")` : "none");
  };

  const startStatusPoll = () => {
    if (!CARD.poll) {
      CARD.poll = setInterval(() => {
        if (!detail.classList.contains("show") || document.hidden) return;
        refreshCard(CARD.selectedKey, false).catch(() => {});
      }, 15000);
    }
    if (!CARD.tick) {
      CARD.tick = setInterval(() => {
        if (!detail.classList.contains("show") || document.hidden) return;
        const item = selectedStream();
        if (item) renderProgress(item);
      }, 1000);
    }
  };
  closeBtn.addEventListener("click", () => {
    CARD.dismissed = true;
    hide(false);
  }, true);
  const selectedIndex = () => Math.max(0, CARD.streams.findIndex((item) => keyOf(item) === CARD.selectedKey));
  const selectedStream = () => CARD.streams.find((item) => keyOf(item) === CARD.selectedKey) || CARD.streams[0] || null;
  const updateNav = () => {
    const total = CARD.streams.length;
    if (navWrap) navWrap.hidden = total <= 1;
    if (navCountEl) navCountEl.textContent = total > 0 ? `${selectedIndex() + 1} / ${total}` : "0 / 0";
    if (prevBtn) prevBtn.disabled = total <= 1;
    if (nextBtn) nextBtn.disabled = total <= 1;
  };
  const ensureSelection = (preferredKey = "") => {
    const keys = new Set(CARD.streams.map((item) => keyOf(item)));
    if (preferredKey && keys.has(preferredKey)) {
      CARD.selectedKey = preferredKey;
      return;
    }
    if (CARD.selectedKey && keys.has(CARD.selectedKey)) return;
    CARD.selectedKey = keyOf(CARD.streams[0] || {});
  };
  const applySelectionOffset = (delta) => {
    const total = CARD.streams.length;
    if (total <= 1) return;
    const current = selectedIndex();
    const next = (current + delta + total) % total;
    CARD.selectedKey = keyOf(CARD.streams[next]);
    CARD.dismissed = false;
    renderSelectedStream().catch(() => {});
  };
  prevBtn?.addEventListener("click", () => applySelectionOffset(-1), true);
  nextBtn?.addEventListener("click", () => applySelectionOffset(1), true);

  const renderProgress = (p) => {
    const pct = visualProgress(p);
    progEl.style.width = `${pct}%`;
    progPctEl.textContent = `${Math.round(pct)}% watched`;
    let timeLabel = "";
    const totalMs = Number(p?.duration_ms) || 0;
    if (totalMs > 0) {
      const remainingStr = formatTime(Math.max(0, totalMs - totalMs * (pct / 100)));
      if (remainingStr) timeLabel = `${remainingStr} left`;
    }
    progTimeEl.textContent = timeLabel;
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

    const startedSec = Number(p.started) || 0;
    const serverTs = Number(p._server_ts) || CARD.serverTs || 0;
    if (serverTs) CARD.serverTs = serverTs;
    const nowSec = serverTs || Math.floor(Date.now() / 1000);
    const since = updatedSec ? sinceLabel(nowSec, updatedSec) : startedSec ? sinceLabel(nowSec, startedSec) : "";

    statusEl.textContent = statusText(st, since);
    statusIconEl.textContent = st === "paused" ? "pause" : "play_arrow";
    statusEl.title = `source=${p.source || ""}, instance=${p.provider_instance || ""}, state=${state || ""}, started=${startedSec || ""}, updated=${updatedSec || ""}`;

    titleEl.textContent = p.year ? `${p.title} ${p.year}` : (p.title || "Now Playing");
    setOverview(p.overview || "");
    renderBaseMeta(p);
    posterEl.src = buildArtUrl(p);
    posterEl.alt = p.title || "Poster";
    setPosterLink(buildTmdbUrl(p), p.title);
    renderProgress(p);
    setInformationLoading(p);
    setRatingState(null, null);
    detail.style.setProperty("--pc-backdrop", "none");
    updateNav();
    detail.classList.add("show");
    startStatusPoll();

    const renderKey = keyOf(p);
    const meta = await getMetaFor(p);
    if (!meta || CARD.selectedKey !== renderKey) return;
    applyMeta(p, meta);
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
