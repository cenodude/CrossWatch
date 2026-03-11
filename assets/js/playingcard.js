/* assets/js/playingcard.js */
/* refactored */
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

  const isSmallScreen = () => {
    try { return !!window.matchMedia?.("(max-width: 680px)")?.matches; } catch { return false; }
  };


  const isActiveState = (s) => ["playing", "paused", "buffering"].includes(String(s || "").toLowerCase());
  const keyOf = (p) => {
    const k = String(p?._key || "").trim();
    if (k) return k;
    
    const sk = String(p?.session_key || "").trim();
    if (p?.source && sk) return `${p.source}:${sk}`;
    return [p?.source || "", p?.media_type || p?.type || "", p?.title || "", p?.year || "", p?.season || "", p?.episode || ""].join("|");
  };

  const tmdbIdOf = (p) => {
    const mt = String(p?.media_type || p?.type || "").toLowerCase();
    const ids = p?.ids || {};
    return mt === "episode"
      ? ids.tmdb_show || p?.tmdb_show || p?.tmdb || p?.tmdb_id || ids.tmdb || ids.id
      : p?.tmdb || p?.tmdb_id || ids.tmdb || ids.tmdb_show || ids.id;
  };

  const imdbIdOf = (p, meta) => {
    const ids = Object.assign({}, p?.ids || {}, meta?.ids || {});
    return String(p?.media_type || p?.type || "").toLowerCase() === "episode"
      ? ids.imdb_show || ids.imdb || ids.imdb_id
      : ids.imdb || ids.imdb_show || ids.imdb_id;
  };

  const buildTmdbUrl = (p) => {
    const id = tmdbIdOf(p);
    if (!id) return "";
    const type = String(p?.media_type || p?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `https://www.themoviedb.org/${type}/${encodeURIComponent(String(id))}`;
  };

  const buildImdbUrl = (p, meta) => {
    const id = imdbIdOf(p, meta);
    if (!id) return "";
    const clean = String(id).startsWith("tt") ? String(id) : `tt${id}`;
    return `https://www.imdb.com/title/${clean}`;
  };

  const buildArtUrl = (p) => {
    if (p?.cover) return p.cover;
    const id = tmdbIdOf(p);
    if (!id) return "/assets/img/placeholder_poster.svg";
    const type = String(p?.media_type || p?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `/art/tmdb/${type}/${encodeURIComponent(String(id))}?size=w342`;
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

  const mediaTypeLabel = (p) => {
    const mediaType = String(p?.media_type || p?.type || "").toLowerCase();
    return mediaType === "movie" ? "Movie" : mediaType === "episode" ? "Episode" : mediaType ? mediaType.toUpperCase() : "TV";
  };

  const streamCount = (p, counts) => Number(p?._streams_count ?? counts.get(keyOf(p)) ?? 0) || 0;
  const statusText = (state, since = "") => {
    const st = String(state || "").toLowerCase();
    let label = st === "paused" ? "Paused" : st === "buffering" ? "Buffering..." : st === "stopped" ? "Stopped" : "Now Playing";
    if (since) label += ` | ${since}`;
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
    if (!meta) return "";
    const images = meta.images || {};
    let arr = images.backdrop || images.backdrops || [];
    if (!arr) return "";
    if (!Array.isArray(arr)) arr = [arr];
    const first = arr[0];
    if (!first) return "";
    if (typeof first === "string") return first;
    if (first.url) return first.url;
    if (first.path) return `https://image.tmdb.org/t/p/w1280${first.path}`;
    if (first.file_path) return `https://image.tmdb.org/t/p/w1280${first.file_path}`;
    return "";
  };

  const metaCache = new Map();
  const getMetaFor = async (p) => {
    const k = metaKey(p);
    const hit = metaCache.get(k);
    if (hit) return hit;

    const tmdb = String(tmdbIdOf(p) || "");
    if (!tmdb) return null;
    const mt = String(p?.media_type || p?.type || "").toLowerCase();
    const type = mt === "movie" ? "movie" : "show";

    try {
      const r = await fetch("/api/metadata/bulk?overview=full", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: [{ type, tmdb }],
          need: { overview: 1, tagline: 1, runtime_minutes: 1, poster: 1, ids: 1, videos: 1, genres: 1, certification: 1, score: 1, release: 1, backdrop: 1 },
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
    }
  };

  let busy = null;
  let cacheKey = "";
  let cacheAt = 0;
  let cacheVal = null;
  const countByKey = new Map();
  const CACHE_TTL_MS = 30000;

  const fetchCurrentlyWatching = async (wantKey) => {
    const now = Date.now();
    const wk = String(wantKey || "");
    if (wk && cacheVal && cacheKey === wk && now - cacheAt < CACHE_TTL_MS) return cacheVal;
    if (busy) return busy;
    busy = (async () => {
      try {
        const r = await fetch("/api/watch/currently_watching", { cache: "no-store" });
        if (!r.ok) return null;
        const j = await r.json();
        let v = j?.currently_watching || null;
        const sc = Number(j?.streams_count) || (Array.isArray(j?.streams) ? j.streams.length : 0);
        const ts = Number(j?.ts) || 0;


        if (v && v.streams && !v.title) {
          const items = Object.values(v.streams || {}).filter(x => x && typeof x === "object");
          items.sort((a, b) => (Number(b.updated) || 0) - (Number(a.updated) || 0));
          v = items.find(it => isActiveState(it.state)) || items[0] || null;
        }

        if (v) {
          const k = wk || keyOf(v) || "";
          cacheVal = v;
          cacheKey = k;
          cacheAt = Date.now();
          if (sc > 1) countByKey.set(k, sc); else countByKey.delete(k);
          if (typeof v === "object") {
            v._streams_count = sc;
            if (ts) v._server_ts = ts;
          }
        }
        return v;
      } catch {
        return null;
      } finally {
        busy = null;
      }
    })();
    return busy;
  };

  const css = `
  #playing-detail{position:fixed;left:50%;bottom:20px;transform:translate(-50%,calc(100% + 30px));width:min(720px,calc(100vw - 420px));background:radial-gradient(120% 140% at 0% 0%,rgba(91,73,197,.18) 0%,rgba(91,73,197,0) 38%),radial-gradient(100% 140% at 100% 100%,rgba(36,118,215,.12) 0%,rgba(36,118,215,0) 42%),linear-gradient(180deg,rgba(7,10,18,.985),rgba(3,5,10,.992));border:1px solid rgba(255,255,255,.06);border-radius:20px;box-shadow:0 24px 64px rgba(0,0,0,.56),inset 0 1px 0 rgba(255,255,255,.03);color:#fff;opacity:0;transition:transform .35s cubic-bezier(.22,.7,.25,1),opacity .25s ease-out,box-shadow .25s ease-out;z-index:10000;overflow:hidden}
  #playing-detail.show{transform:translate(-50%,0);opacity:1}
  #playing-detail.show:hover{transform:translate(-50%,-3px);box-shadow:0 28px 72px rgba(0,0,0,.62),inset 0 1px 0 rgba(255,255,255,.035)}
  html[data-tab!="main"] #playing-detail{display:none!important}
  #playing-detail::before{content:"";position:absolute;inset:4px;border-radius:16px;background-image:radial-gradient(90% 120% at 0% 0%,rgba(94,77,204,.12) 0%,rgba(94,77,204,0) 45%),linear-gradient(90deg,rgba(5,8,15,.94) 0%,rgba(5,8,15,.92) 28%,rgba(5,8,15,.88) 62%,rgba(5,8,15,.78) 100%),var(--pc-backdrop,none);background-size:100% 100%,100% 100%,cover;background-position:center center,center center,right center;background-repeat:no-repeat,no-repeat,no-repeat;pointer-events:none;z-index:0}
  #playing-detail .pc-body{position:relative;display:flex;flex-direction:column;justify-content:flex-start;padding-bottom:0}
  #playing-detail .pc-inner{position:relative;z-index:1;display:grid;grid-template-columns:104px 1fr 170px;gap:14px;align-items:stretch;padding:14px}
  #playing-detail .pc-poster{width:104px;border:1px solid rgba(255,255,255,.06);border-radius:14px;object-fit:cover;box-shadow:0 14px 30px rgba(0,0,0,.44),inset 0 1px 0 rgba(255,255,255,.04);background:#05070d}
  #playing-detail .pc-title-row{display:flex;align-items:flex-start;gap:8px}
  #playing-detail .pc-title{font-weight:700;font-size:17px;letter-spacing:.005em;line-height:1.2}
  #playing-detail .pc-close{margin-left:auto;display:flex;align-items:center;justify-content:center;gap:4px;padding:4px 9px;border:1px solid rgba(255,255,255,.08);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018));color:rgba(232,238,252,.78);cursor:pointer;font-size:12px;line-height:1;text-transform:uppercase;letter-spacing:.08em;transition:background .18s ease,border-color .18s ease,color .18s ease,transform .18s ease}
  #playing-detail .pc-close span{font-size:16px;line-height:1}
  #playing-detail .pc-close:hover{color:#fff;border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(255,255,255,.075),rgba(255,255,255,.03));transform:translateY(-1px)}
  #playing-detail .pc-meta{display:flex;flex-wrap:wrap;gap:5px;margin-top:6px}
  #playing-detail .pc-chip{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 9px;border:1px solid rgba(255,255,255,.07);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.022));box-shadow:inset 0 1px 0 rgba(255,255,255,.03);font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:rgba(236,241,251,.86);line-height:1}
  .pc-chip-streams{background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.032))}
  #playing-detail .pc-overview{margin-top:8px;font-size:12px;line-height:1.45;color:rgba(211,219,234,.7);max-height:3.2em;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
  #playing-detail .pc-progress-wrap{margin-top:auto;position:relative;width:100%;max-width:100%;box-sizing:border-box}
  #playing-detail .pc-progress-bg{position:relative;width:100%;height:22px;border:1px solid rgba(255,255,255,.06);border-radius:999px;background:linear-gradient(180deg,rgba(12,16,27,.96),rgba(8,11,19,.98));overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
  #playing-detail .pc-progress{width:0;height:100%;background:linear-gradient(90deg,rgba(106,97,255,.84),rgba(75,157,255,.88));transition:width .4s cubic-bezier(.22,.7,.25,1)}
  #playing-detail .pc-progress::after{content:"";position:absolute;inset:0;border-radius:999px;box-shadow:0 0 18px rgba(84,124,255,.22);pointer-events:none}
  #playing-detail .pc-progress-labels{position:absolute;inset:0;display:flex;align-items:center;justify-content:space-between;padding:0 10px;pointer-events:none;font-size:11px;font-weight:700;color:rgba(236,241,251,.92);text-shadow:0 1px 2px rgba(0,0,0,.8);box-sizing:border-box}
  #playing-detail .pc-right{display:flex;flex-direction:column;align-items:stretch;justify-content:flex-start;gap:8px}
  #playing-detail .pc-score-circle{--pc-score-deg:0deg;--pc-score-color:#16a34a;--pc-score-track:#111827;position:relative;align-self:flex-end;display:flex;align-items:center;justify-content:center;width:58px;height:58px;border-radius:50%;background:conic-gradient(var(--pc-score-color) var(--pc-score-deg),var(--pc-score-track) var(--pc-score-deg));color:#fff;font-weight:700;box-shadow:0 14px 28px rgba(0,0,0,.34)}
  #playing-detail .pc-score-circle::before{content:"";position:absolute;inset:4px;border-radius:50%;background:linear-gradient(180deg,rgba(8,11,20,.98),rgba(4,7,14,.98))}
  #playing-detail .pc-score-circle.is-empty{background:conic-gradient(#374151 0deg,#111827 0deg)}
  #playing-detail #pc-score{position:relative;font-size:18px}
  #playing-detail .pc-score-label{font-size:11px;color:rgba(200,209,226,.56);text-align:right}
  #playing-detail .pc-link{display:inline-flex;align-items:center;justify-content:center;min-height:28px;padding:0 10px;border:1px solid rgba(255,255,255,.07);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018));color:#dbe8ff;font-size:11px;font-weight:700;text-decoration:none;text-align:center;transition:background .18s ease,border-color .18s ease,color .18s ease,transform .18s ease}
  #playing-detail .pc-link + .pc-link{margin-top:0}
  #playing-detail .pc-link:hover{border-color:rgba(122,138,255,.24);background:linear-gradient(180deg,rgba(96,104,242,.18),rgba(255,255,255,.03));color:#f3f7ff;transform:translateY(-1px)}
  #playing-detail .pc-status{position:static;display:inline-flex;align-items:center;justify-content:center;align-self:stretch;min-height:28px;margin-top:auto;padding:0 10px;border:1px solid rgba(255,255,255,.07);border-radius:999px;background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.018));font-size:10px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:rgba(236,241,251,.88);opacity:.96;white-space:nowrap;text-align:center}
  @media (max-width:1024px){#playing-detail{width:calc(100vw - 40px)}}
  @media (max-width:768px){#playing-detail .pc-inner{grid-template-columns:80px 1fr;grid-template-rows:auto auto}#playing-detail .pc-right{grid-column:span 2;flex-direction:row;justify-content:space-between}}
  @media (max-width:680px){#playing-detail{bottom:max(10px,env(safe-area-inset-bottom));width:calc(100vw - 20px);border-radius:18px}#playing-detail .pc-inner{grid-template-columns:64px 1fr;gap:12px;padding:12px}#playing-detail .pc-poster{width:64px;height:96px;border-radius:10px}#playing-detail .pc-title{font-size:15px;line-height:1.15}#playing-detail .pc-close{padding:2px 4px}#playing-detail .pc-close span:nth-child(2){display:none}#playing-detail .pc-meta{gap:4px;margin-top:4px}#playing-detail .pc-chip{font-size:10px;padding:3px 7px}#playing-detail .pc-overview{display:none}#playing-detail .pc-progress-bg{height:18px}#playing-detail .pc-progress-labels{font-size:11px;padding:0 8px}#playing-detail .pc-right{grid-column:1 / -1;flex-direction:row;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:8px}#playing-detail .pc-score-circle{width:46px;height:46px}#playing-detail .pc-score-label{display:none}#playing-detail .pc-link{font-size:11px;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);text-decoration:none}#playing-detail .pc-status{margin-top:0;flex:1 1 auto;text-align:right;white-space:normal;max-width:60%}}
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
      <img id="pc-poster" class="pc-poster" src="/assets/img/placeholder_poster.svg" alt="">
      <div class="pc-body">
        <div class="pc-title-row">
          <div id="pc-title" class="pc-title">Now Playing</div>
          <button id="pc-close" class="pc-close" title="Hide">
            <span>x</span><span>Hide</span>
          </button>
        </div>
        <div id="pc-meta" class="pc-meta"></div>
        <div id="pc-overview" class="pc-overview"></div>
        <div class="pc-progress-wrap">
          <div class="pc-progress-bg"><div id="pc-progress" class="pc-progress"></div></div>
          <div class="pc-progress-labels"><span id="pc-progress-pct"></span><span id="pc-progress-time"></span></div>
        </div>
      </div>
      <div class="pc-right">
        <div id="pc-score-circle" class="pc-score-circle is-empty"><span id="pc-score">--</span></div>
        <div class="pc-score-label">User Score</div>
        <a id="pc-tmdb" class="pc-link" href="#" target="_blank" rel="noopener noreferrer"></a>
        <a id="pc-imdb" class="pc-link" href="#" target="_blank" rel="noopener noreferrer"></a>
        <div id="pc-status" class="pc-status">Now Playing</div>
      </div>
    </div>

  `;
  document.body.appendChild(detail);

  const posterEl = detail.querySelector("#pc-poster");
  const titleEl = detail.querySelector("#pc-title");
  const metaEl = detail.querySelector("#pc-meta");
  const overviewEl = detail.querySelector("#pc-overview");
  const progEl = detail.querySelector("#pc-progress");
  const progPctEl = detail.querySelector("#pc-progress-pct");
  const progTimeEl = detail.querySelector("#pc-progress-time");
  const scoreCircleEl = detail.querySelector("#pc-score-circle");
  const scoreEl = detail.querySelector("#pc-score");
  const tmdbEl = detail.querySelector("#pc-tmdb");
  const imdbEl = detail.querySelector("#pc-imdb");
  const statusEl = detail.querySelector("#pc-status");
  const closeBtn = detail.querySelector("#pc-close");

  posterEl.onerror = () => {
    posterEl.onerror = null;
    posterEl.src = "/assets/img/placeholder_poster.svg";
  };

  let lastKey = null;
  let dismissedKey = null;
  let lastUpdatedSec = 0;
  let lastStartedSec = 0;
  let lastServerTsSec = 0;
  let statusPoll = null;

  const stopStatusPoll = () => {
    if (!statusPoll) return;
    try { clearInterval(statusPoll); } catch {}
    statusPoll = null;
  };
  const hide = () => {
    detail.classList.remove("show");
    stopStatusPoll();
    lastKey = null;
    lastUpdatedSec = 0;
    lastStartedSec = 0;
    lastServerTsSec = 0;
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

  const setScoreState = (score100) => {
    if (score100 != null) {
      const value = Math.max(0, Math.min(100, Math.round(score100)));
      const deg = value * 3.6;
      const color = value <= 49 ? "#ef4444" : value <= 74 ? "#f59e0b" : "#22c55e";
      scoreEl.textContent = `${value}%`;
      scoreCircleEl.classList.remove("is-empty");
      scoreCircleEl.style.setProperty("--pc-score-deg", `${deg}deg`);
      scoreCircleEl.style.setProperty("--pc-score-color", color);
      return;
    }
    scoreEl.textContent = "--";
    scoreCircleEl.classList.add("is-empty");
    scoreCircleEl.style.setProperty("--pc-score-deg", "0deg");
    scoreCircleEl.style.setProperty("--pc-score-color", "#374151");
  };

  const setLinkState = (el, href, shortLabel, longLabel) => {
    if (!href) {
      el.style.display = "none";
      el.textContent = "";
      el.removeAttribute("href");
      return;
    }
    el.href = href;
    el.textContent = isSmallScreen() ? shortLabel : longLabel;
    el.style.display = "";
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
      if (ov) overviewEl.textContent = ov;
    }
    setScoreState(Number.isFinite(meta.score) ? meta.score : null);
    const tmdbUrl = buildTmdbUrl(Object.assign({}, p, { tmdb: tmdbIdOf(p) || (meta.ids && (meta.ids.tmdb || meta.ids.id)) }));
    setLinkState(tmdbEl, tmdbUrl, "TMDb ->", "View on TMDb ->");
    const imdbUrl = buildImdbUrl(p, meta);
    setLinkState(imdbEl, imdbUrl, "IMDb ->", "View on IMDb ->");
    detail.style.setProperty("--pc-backdrop", backdrop ? `url("${backdrop}")` : "none");
  };

  const startStatusPoll = () => {
    if (statusPoll) return;
    statusPoll = setInterval(async () => {
      try {
        if (!detail.classList.contains("show") || document.hidden) return;
        const api = await fetchCurrentlyWatching(lastKey || "");
        if (!api) return;
        const k = keyOf(api);
        if (!k || k !== lastKey) return;
        const st = String(api.state || api.status || "").toLowerCase();
        if (!isActiveState(st)) {
          hide();
          dismissedKey = null;
          return;
        }
        const startedSec = Number(api.started) || lastStartedSec || 0;
        if (startedSec) lastStartedSec = startedSec;
        const serverTs = Number(api._server_ts) || 0;
        if (serverTs) lastServerTsSec = serverTs;
        const nowSec = lastServerTsSec || Math.floor(Date.now() / 1000);
        const since = lastStartedSec ? sinceLabel(nowSec, lastStartedSec) : "";
        if (!since) return;
        statusEl.textContent = statusText(st, since);
      } catch {}
    }, 60000);
  };
  closeBtn.addEventListener("click", () => {
    if (lastKey) dismissedKey = lastKey;
    hide();
  }, true);

  async function render(payload) {
    if (!isUiEnabled()) {
      hide();
      return;
    }

    let p = payload || {};
    const eventState = p.state || p.status || "playing";

    if (!p.title && !isActiveState(eventState)) {
      hide();
      return;
    }

    if (!tmdbIdOf(p) || !p.started) {
      const wantKey = keyOf(p);
      const api = await fetchCurrentlyWatching(wantKey);
      if (api) {
        const ak = keyOf(api);
        if (!wantKey || wantKey === ak || !p.title) p = Object.assign({}, p, api);
      }
    }

    const state = p.state || p.status || eventState;
    if (!p.title || !isActiveState(state)) {
      hide();
      return;
    }

    const k = keyOf(p);
    if (dismissedKey && dismissedKey === k) return;


    if (k !== lastKey) {
      lastKey = k;
      lastUpdatedSec = 0;
    }


    const st = String(state || "").toLowerCase();


    let updatedSec = Number(p.updated) || 0;
    if (!updatedSec && lastUpdatedSec && lastKey === k) updatedSec = lastUpdatedSec;
    if (updatedSec) {
      lastUpdatedSec = updatedSec;
      const ageMs = Date.now() - updatedSec * 1000;
      const maxAgeMs = st === "paused" ? 4 * 60 * 60 * 1000 : 10 * 60 * 1000;
      if (ageMs > maxAgeMs) {
        hide();
        return;
      }
    }
    const pct = Math.round(Math.max(0, Math.min(100, Number(p.progress) || 0)));
    let startedSec = Number(p.started) || 0;
    if (!startedSec && lastStartedSec && lastKey === k) startedSec = lastStartedSec;

    const serverTs = Number(p._server_ts) || 0;
    if (serverTs) lastServerTsSec = serverTs;

    const nowSec = lastServerTsSec || Math.floor(Date.now() / 1000);
    const since = startedSec ? sinceLabel(nowSec, startedSec) : "";

    if (startedSec) lastStartedSec = startedSec;


    statusEl.textContent = statusText(st, since);
    statusEl.title = `source=${p.source || ""}, state=${state || ""}, started=${startedSec || ""}, updated=${updatedSec || ""}`;
    
    titleEl.textContent = p.year ? `${p.title} ${p.year}` : (p.title || "Now Playing");
    overviewEl.textContent = p.overview || "";
    renderBaseMeta(p);
    posterEl.src = buildArtUrl(p);
    posterEl.alt = p.title || "Poster";

    progEl.style.width = `${pct}%`;

    let timeLabel = "";

    if (p.duration_ms && pct > 0) {
      const totalMs = Number(p.duration_ms) || 0;
      if (totalMs > 0) {
        const remainingMs = Math.max(0, totalMs - totalMs * (pct / 100));
        const remainingStr = formatTime(remainingMs);
        if (remainingStr) timeLabel = `${remainingStr} left`;
      }
    }
    progPctEl.textContent = `${pct}% watched`;
    progTimeEl.textContent = timeLabel;
    setScoreState(null);
    setLinkState(tmdbEl, "", "TMDb ->", "View on TMDb ->");
    setLinkState(imdbEl, "", "IMDb ->", "View on IMDb ->");
    detail.style.setProperty("--pc-backdrop", "none");
    detail.classList.add("show");
    startStatusPoll();

    const meta = await getMetaFor(p);
    if (!meta || k !== lastKey) return;
    applyMeta(p, meta);
  }

  window.addEventListener("currently-watching-updated", (ev) => {
    (async () => {
      try {
        const d = ev.detail || {};
        const st = String(d?.state || d?.status || "").toLowerCase();
        const wantKey = keyOf(d) || lastKey || "";
        const api = await fetchCurrentlyWatching(wantKey);
        if (api && isActiveState(api.state || api.status)) {
          await render(api);
          return;
        }
        if (!d || st === "stopped") {
          hide();
          dismissedKey = null;
          return;
        }
        await render(d);
      } catch {}
    })();
  });

  window.updatePlayingCard = render;
})();
