/* assets/helpers/watchlist-preview.js */
/* Extracted watchlist preview/wall UI from core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  const isTV = window.isTV || ((v) => /^(tv|show|shows|series|season|episode|anime)$/i.test(String(v || "")));
  let wallReqSeq = 0;
  let previewBusy = false;

  window.wallLoaded = window.wallLoaded || false;
  window.__wallLoading = window.__wallLoading || false;
  window.__cwWallPreviewDirty = typeof window.__cwWallPreviewDirty === "boolean" ? window.__cwWallPreviewDirty : true;
  window.__cwWallPreviewDirtyVersion = Number(window.__cwWallPreviewDirtyVersion || 1);
  window.__cwWallPreviewLoadedAt = Number(window.__cwWallPreviewLoadedAt || 0);
  window._lastSyncEpoch = window._lastSyncEpoch || null;
  window.__wallRenderSignature = window.__wallRenderSignature || "";
  const WALL_PREVIEW_CACHE_KEY = "cw.wall.preview.v2";
  const WALL_PREVIEW_LEGACY_CACHE_KEY = `cw.wall.preview.${window.APP_VERSION || "v1"}`;
  const WALL_PREVIEW_REFRESH_TTL_MS = 60 * 1000;
  const DEFAULT_INSTANCE = "default";

  const json = async (url, opt) => {
    if (authSetupPending()) throw new Error("auth setup pending");
    if (window.CW?.API?.j && !opt) return window.CW.API.j(url);
    const res = await fetch(url, { cache: "no-store", ...(opt || {}) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  };

  const getConfig = async () => {
    if (window.CW?.API?.Config?.load) return window.CW.API.Config.load(false);
    if (window._cfgCache) return window._cfgCache;
    const cfg = await json("/api/config");
    window._cfgCache = cfg;
    return cfg;
  };

  const providerMeta = () => window.CW?.ProviderMeta || {};
  const providerKey = (value) => providerMeta().keyOf?.(value) || String(value || "").trim().toUpperCase();
  const providerLabel = (value) => providerMeta().label?.(value) || providerKey(value) || String(value || "");
  const providerShortLabel = (value) => providerMeta().shortLabel?.(value) || providerLabel(value);
  const providerInstanceLabel = (provider, instance) => {
    const label = providerLabel(provider);
    const inst = String(instance || DEFAULT_INSTANCE).trim() || DEFAULT_INSTANCE;
    return inst.toLowerCase() === DEFAULT_INSTANCE ? label : `${label} (${inst})`;
  };
  const providerFromStatus = (status) => {
    const raw = String(status || "").toLowerCase().trim();
    if (!raw || raw === "both" || raw === "deleted") return "";
    if (raw === "crosswatch_only" || raw === "cw_only") return "CROSSWATCH";
    if (raw.endsWith("_only")) return providerKey(raw.slice(0, -5));
    return "";
  };
  const PILL_CLASS_BY_PROVIDER = {
    PLEX: "p-px",
    SIMKL: "p-sk",
    TRAKT: "p-tr",
    ANILIST: "p-al",
    JELLYFIN: "p-sk",
    CROSSWATCH: "p-sk",
  };

  const readHidden = () => {
    try { return new Set(JSON.parse(localStorage.getItem("wl_hidden") || "[]") || []); }
    catch { return new Set(); }
  };

  const writeHidden = (set) => {
    try { localStorage.setItem("wl_hidden", JSON.stringify([...set])); } catch {}
  };

  const readWallCache = () => {
    try {
      let raw = localStorage.getItem(WALL_PREVIEW_CACHE_KEY)
        || localStorage.getItem(WALL_PREVIEW_LEGACY_CACHE_KEY);
      if (!raw) {
        for (let i = 0; i < localStorage.length; i++) {
          const key = localStorage.key(i) || "";
          if (!key.startsWith("cw.wall.preview.") || key === WALL_PREVIEW_CACHE_KEY) continue;
          raw = localStorage.getItem(key);
          if (raw) break;
        }
      }
      const data = JSON.parse(raw);
      return Array.isArray(data?.items) ? data : null;
    } catch {
      return null;
    }
  };

  const writeWallCache = (items, lastSyncEpoch, total = null) => {
    try {
      localStorage.setItem(WALL_PREVIEW_CACHE_KEY, JSON.stringify({ items, last_sync_epoch: lastSyncEpoch || 0, total }));
    } catch {}
  };

  const hasRenderedWall = (row = document.getElementById("poster-row")) => !!(row?.childElementCount && !row.classList.contains("hidden"));
  const previewNeedsRefresh = () => window.__cwWallPreviewDirty
    || !window.__cwWallPreviewLoadedAt
    || (Date.now() - window.__cwWallPreviewLoadedAt) >= WALL_PREVIEW_REFRESH_TTL_MS;
  const markPreviewClean = (refreshVersion) => {
    if (window.__cwWallPreviewDirtyVersion === refreshVersion) window.__cwWallPreviewDirty = false;
    window.__cwWallPreviewLoadedAt = Date.now();
  };

  const firstSeenMap = () => {
    try { return JSON.parse(localStorage.getItem("wl_first_seen") || "{}"); }
    catch { return {}; }
  };

  function updateEdges() {
    const row = document.getElementById("poster-row");
    const left = document.getElementById("edgeL");
    const right = document.getElementById("edgeR");
    if (!row || !left || !right) return;
    const max = row.scrollWidth - row.clientWidth - 1;
    left.classList.toggle("hide", row.scrollLeft <= 0);
    right.classList.toggle("hide", row.scrollLeft >= max);
  }

  function scrollWall(dir) {
    const row = document.getElementById("poster-row");
    if (!row) return;
    row.scrollBy({ left: dir * row.clientWidth, behavior: "smooth" });
    setTimeout(updateEdges, 350);
  }

  function initWallInteractions() {
    const row = document.getElementById("poster-row");
    if (!row || row.__cwWallWired) return;
    row.addEventListener("scroll", updateEdges, { passive: true });
    row.addEventListener("click", (event) => {
      const link = event.target?.closest?.("a.poster");
      if (!link || !row.contains(link)) return;
      if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      const key = link.dataset.previewKey || link.dataset.key || "";
      const item = window.__cwWallPreviewItems?.get?.(key);
      if (!item) return;
      event.preventDefault();
      void openPreviewDrawer(item);
    }, true);
    row.addEventListener("wheel", (e) => {
      if (Math.abs(e.deltaY) <= Math.abs(e.deltaX)) return;
      e.preventDefault();
      row.scrollBy({ left: e.deltaY, behavior: "auto" });
    }, { passive: false });
    row.__cwWallWired = true;
    updateEdges();
  }

  function artUrl(item, size = "w342") {
    const tmdb = item?.tmdb;
    if (!tmdb) return null;
    return `/art/tmdb/${isTV(item.type || item.entity || item.media_type) ? "tv" : "movie"}/${tmdb}?size=${encodeURIComponent(size)}`;
  }

  const providerLogoPath = (name) => window.CW?.ProviderMeta?.logoPath?.(name) || "";
  const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
  const previewMetaCache = new Map();
  const previewMetaInflight = new Map();
  let activePreviewDrawerKey = "";

  function countLabel(total, noun) {
    const n = Number(total || 0);
    const label = n === 1 ? noun : `${noun}s`;
    return `${Number.isFinite(n) ? n : 0} ${label}`;
  }

  function setWatchlistCount(total) {
    const chip = document.getElementById("watchlist-count-chip");
    if (!chip) return;
    const count = Number(total || 0);
    chip.textContent = String(Number.isFinite(count) ? count : 0);
    chip.setAttribute("aria-label", countLabel(total, "item"));
    chip.classList.remove("hidden");
  }

  function hideWatchlistCount() {
    document.getElementById("watchlist-count-chip")?.classList.add("hidden");
  }

  function mediaTypeOf(item) {
    const raw = String(item?.type || item?.entity || item?.media_type || "").toLowerCase();
    return raw === "movie" ? "movie" : "show";
  }

  function artTypeOf(item) {
    return mediaTypeOf(item) === "movie" ? "movie" : "tv";
  }

  function mediaLabelOf(item) {
    const raw = String(item?.type || item?.entity || item?.media_type || "").toLowerCase();
    if (raw === "movie") return "Movie";
    if (raw === "anime") return "Anime";
    return "Show";
  }

  function tmdbIdOf(item, meta = null) {
    return item?.tmdb || item?.tmdb_id || item?.ids?.tmdb || item?.ids?.tmdb_show || meta?.ids?.tmdb || "";
  }

  function imdbIdOf(item, meta = null) {
    const ids = { ...(item?.ids || {}), ...(meta?.ids || {}) };
    return ids.imdb || ids.imdb_id || ids.imdb_show || "";
  }

  function previewItemKey(item) {
    return item?.key || `${mediaTypeOf(item)}:${tmdbIdOf(item)}:${item?.title || ""}:${item?.year || ""}`;
  }

  function previewMetaKey(item) {
    const tmdb = tmdbIdOf(item);
    return tmdb ? `${mediaTypeOf(item)}:${tmdb}` : "";
  }

  function backdropUrl(item, meta = null) {
    const tmdb = tmdbIdOf(item, meta);
    if (!tmdb) return "";
    return `/art/tmdb/${artTypeOf(item)}/${encodeURIComponent(String(tmdb))}?kind=backdrop&size=w1280&locale=${encodeURIComponent(window.__CW_LOCALE || navigator.language || "en-US")}`;
  }

  function runtimeLabel(mins) {
    const total = Number(mins) || 0;
    if (!total) return "";
    const h = Math.floor(total / 60);
    const m = total % 60;
    return h ? `${h}h${m ? ` ${m}m` : ""}` : `${m}m`;
  }

  function yearFromIso(value) {
    const raw = String(value || "");
    return /^\d{4}/.test(raw) ? raw.slice(0, 4) : "";
  }

  function dateLabel(value) {
    const raw = String(value || "");
    if (!/^\d{4}-\d{2}-\d{2}/.test(raw)) return "";
    const dt = new Date(`${raw.slice(0, 10)}T00:00:00Z`);
    if (Number.isNaN(dt.getTime())) return "";
    try {
      return new Intl.DateTimeFormat(navigator.language || "en-US", { day: "2-digit", month: "short", year: "numeric", timeZone: "UTC" }).format(dt);
    } catch {
      return raw.slice(0, 10);
    }
  }

  function scoreValue(meta) {
    const raw = Number(meta?.score ?? meta?.vote_average);
    if (!Number.isFinite(raw) || raw <= 0) return null;
    return raw <= 10 ? Math.round(raw * 10) : Math.round(raw);
  }

  function tmdbUrl(item, meta = null) {
    const tmdb = tmdbIdOf(item, meta);
    if (!tmdb) return "";
    return `https://www.themoviedb.org/${mediaTypeOf(item) === "movie" ? "movie" : "tv"}/${encodeURIComponent(String(tmdb))}`;
  }

  function imdbUrl(item, meta = null) {
    const imdb = String(imdbIdOf(item, meta) || "").trim();
    if (!imdb) return "";
    const clean = imdb.startsWith("tt") ? imdb : `tt${imdb}`;
    return `https://www.imdb.com/title/${encodeURIComponent(clean)}`;
  }

  async function getPreviewMeta(item) {
    const cacheKey = previewMetaKey(item);
    if (!cacheKey) return null;
    if (previewMetaCache.has(cacheKey)) return previewMetaCache.get(cacheKey);
    if (previewMetaInflight.has(cacheKey)) return previewMetaInflight.get(cacheKey);

    const tmdb = tmdbIdOf(item);
    const type = mediaTypeOf(item);
    const req = (async () => {
      try {
        const res = await fetch("/api/metadata/bulk?overview=full", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            items: [{ type, tmdb }],
            need: { overview: 1, runtime_minutes: 1, ids: 1, videos: 1, genres: 1, certification: 1, score: 1, release: 1, backdrop: 1 },
            concurrency: 1,
          }),
        });
        if (!res.ok) return null;
        const data = await res.json();
        const first = Object.values(data?.results || {})[0];
        const meta = first?.ok ? (first.meta || null) : null;
        if (meta) previewMetaCache.set(cacheKey, meta);
        return meta;
      } catch {
        return null;
      } finally {
        previewMetaInflight.delete(cacheKey);
      }
    })();
    previewMetaInflight.set(cacheKey, req);
    return req;
  }

  function ensurePreviewDrawerStyles() {
    if (document.getElementById("cw-wall-preview-detail-style")) return;
    const style = document.createElement("style");
    style.id = "cw-wall-preview-detail-style";
    style.textContent = `
      #cw-wall-preview-detail{
        --wpd-bg:rgba(24,29,39,.94);
        --wpd-bg-strong:rgba(11,14,21,.92);
        --wpd-border:rgba(148,163,184,.22);
        --wpd-border-strong:rgba(148,163,184,.30);
        --wpd-text:#f4f7ff;
        --wpd-muted:rgba(207,216,232,.72);
        --wpd-chip:rgba(255,255,255,.07);
        --wpd-shadow:0 24px 68px rgba(0,0,0,.48);
        position:fixed;
        left:50%;
        bottom:max(18px,env(safe-area-inset-bottom));
        width:min(860px,calc(100vw - 32px));
        color:var(--wpd-text);
        border:1px solid var(--wpd-border);
        border-radius:24px;
        background:var(--wpd-bg);
        box-shadow:var(--wpd-shadow);
        overflow:hidden;
        isolation:isolate;
        transform:translate(-50%,calc(100% + 28px)) scale(.985);
        opacity:0;
        pointer-events:none;
        transition:transform .28s cubic-bezier(.2,.8,.2,1),opacity .22s ease,border-color .18s ease;
        z-index:10025;
      }
      #cw-wall-preview-detail.show{
        transform:translate(-50%,0) scale(1);
        opacity:1;
        pointer-events:auto;
      }
      html:not([data-tab="main"]) #cw-wall-preview-detail{display:none!important}
      #cw-wall-preview-detail::before{
        content:"";
        position:absolute;
        inset:0;
        z-index:0;
        pointer-events:none;
        background:
          linear-gradient(90deg,rgba(18,23,33,.98) 0%,rgba(18,23,33,.92) 48%,rgba(18,23,33,.80) 100%),
          var(--wpd-backdrop,none);
        background-size:100% 100%,cover;
        background-position:center center,center center;
        background-repeat:no-repeat,no-repeat;
      }
      #cw-wall-preview-detail .wpd-inner{
        position:relative;
        z-index:1;
        display:grid;
        grid-template-columns:118px minmax(0,1fr) 150px;
        gap:16px;
        align-items:stretch;
        padding:16px;
      }
      #cw-wall-preview-detail .wpd-poster{
        width:118px;
        aspect-ratio:2/3;
        border-radius:16px;
        object-fit:cover;
        border:1px solid var(--wpd-border);
        background:var(--wpd-bg-strong);
        box-shadow:0 18px 34px rgba(0,0,0,.28);
      }
      #cw-wall-preview-detail .wpd-main{min-width:0;display:flex;flex-direction:column}
      #cw-wall-preview-detail .wpd-title-row{display:flex;gap:12px;align-items:flex-start}
      #cw-wall-preview-detail .wpd-title{
        flex:1;
        min-width:0;
        font-size:22px;
        font-weight:850;
        line-height:1.12;
        letter-spacing:0;
      }
      #cw-wall-preview-detail .wpd-year{color:var(--wpd-muted);font-weight:760}
      #cw-wall-preview-detail .wpd-close{
        display:inline-flex;
        align-items:center;
        justify-content:center;
        width:40px;
        height:40px;
        border-radius:999px;
        border:1px solid var(--wpd-border);
        background:var(--wpd-chip);
        color:var(--wpd-text);
        cursor:pointer;
      }
      #cw-wall-preview-detail .wpd-close:hover{border-color:var(--wpd-border-strong);background:rgba(255,255,255,.10)}
      #cw-wall-preview-detail .wpd-close .material-symbol{font-size:22px}
      #cw-wall-preview-detail .wpd-meta,
      #cw-wall-preview-detail .wpd-sources{
        display:flex;
        align-items:center;
        flex-wrap:wrap;
        gap:7px;
      }
      #cw-wall-preview-detail .wpd-meta{margin-top:10px}
      #cw-wall-preview-detail .wpd-sources{margin-top:auto;padding-top:12px}
      #cw-wall-preview-detail .wpd-chip,
      #cw-wall-preview-detail .wpd-source{
        display:inline-flex;
        align-items:center;
        justify-content:center;
        min-height:27px;
        padding:0 10px;
        border-radius:999px;
        border:1px solid var(--wpd-border);
        background:var(--wpd-chip);
        color:var(--wpd-text);
        font-size:11px;
        font-weight:820;
        letter-spacing:.04em;
        text-transform:uppercase;
      }
      #cw-wall-preview-detail .wpd-source{width:34px;padding:0}
      #cw-wall-preview-detail .wpd-source img{display:block;height:16px;max-width:23px;object-fit:contain}
      #cw-wall-preview-detail .wpd-overview{
        margin-top:12px;
        color:var(--wpd-muted);
        font-size:14px;
        line-height:1.48;
        max-height:4.5em;
        overflow:hidden;
        display:-webkit-box;
        -webkit-line-clamp:3;
        -webkit-box-orient:vertical;
      }
      #cw-wall-preview-detail .wpd-side{
        display:flex;
        flex-direction:column;
        align-items:stretch;
        justify-content:flex-start;
        gap:9px;
      }
      #cw-wall-preview-detail .wpd-score{
        --wpd-score:0deg;
        --wpd-score-color:#49d391;
        position:relative;
        align-self:flex-end;
        display:flex;
        align-items:center;
        justify-content:center;
        width:64px;
        height:64px;
        border-radius:50%;
        background:conic-gradient(var(--wpd-score-color) var(--wpd-score),rgba(148,163,184,.18) 0);
        font-size:18px;
        font-weight:880;
      }
      #cw-wall-preview-detail .wpd-score::before{
        content:"";
        position:absolute;
        inset:5px;
        border-radius:50%;
        background:var(--wpd-bg-strong);
      }
      #cw-wall-preview-detail .wpd-score span{position:relative}
      #cw-wall-preview-detail .wpd-score-label{
        align-self:flex-end;
        margin-top:-3px;
        color:var(--wpd-muted);
        font-size:11px;
        font-weight:720;
      }
      #cw-wall-preview-detail .wpd-link{
        display:flex;
        align-items:center;
        justify-content:center;
        min-height:36px;
        padding:0 12px;
        border-radius:999px;
        border:1px solid var(--wpd-border);
        background:var(--wpd-chip);
        color:var(--wpd-text);
        text-decoration:none;
        font-size:12px;
        font-weight:820;
      }
      #cw-wall-preview-detail .wpd-link:hover{border-color:var(--wpd-border-strong);background:rgba(255,255,255,.10)}
      #placeholder-card .poster{cursor:pointer}
      html[data-cw-theme="flat-light"] #cw-wall-preview-detail{
        --wpd-bg:rgba(255,255,255,.96);
        --wpd-bg-strong:rgba(246,248,252,.96);
        --wpd-border:rgba(15,23,42,.16);
        --wpd-border-strong:rgba(15,23,42,.26);
        --wpd-text:#172033;
        --wpd-muted:rgba(51,65,85,.78);
        --wpd-chip:rgba(241,245,249,.86);
        --wpd-shadow:0 24px 60px rgba(15,23,42,.18);
      }
      html[data-cw-theme="flat-light"] #cw-wall-preview-detail::before{
        background:
          linear-gradient(90deg,rgba(255,255,255,.98) 0%,rgba(255,255,255,.93) 50%,rgba(255,255,255,.78) 100%),
          var(--wpd-backdrop,none);
      }
      @media (max-width:720px){
        #cw-wall-preview-detail{width:calc(100vw - 18px);bottom:max(10px,env(safe-area-inset-bottom));border-radius:20px}
        #cw-wall-preview-detail .wpd-inner{grid-template-columns:78px minmax(0,1fr);gap:12px;padding:12px}
        #cw-wall-preview-detail .wpd-poster{width:78px;border-radius:12px}
        #cw-wall-preview-detail .wpd-title{font-size:17px}
        #cw-wall-preview-detail .wpd-side{grid-column:1 / -1;flex-direction:row;align-items:center;flex-wrap:wrap}
        #cw-wall-preview-detail .wpd-score{width:48px;height:48px;font-size:14px;align-self:center}
        #cw-wall-preview-detail .wpd-score-label{display:none}
        #cw-wall-preview-detail .wpd-overview{-webkit-line-clamp:2;font-size:13px}
        #cw-wall-preview-detail .wpd-link{flex:1;min-width:120px}
      }
    `;
    document.head.appendChild(style);
  }

  function ensurePreviewDrawer() {
    ensurePreviewDrawerStyles();
    let drawer = document.getElementById("cw-wall-preview-detail");
    if (drawer) return drawer;

    drawer = document.createElement("aside");
    drawer.id = "cw-wall-preview-detail";
    drawer.setAttribute("aria-live", "polite");
    drawer.setAttribute("aria-label", "Watchlist preview details");
    document.body.appendChild(drawer);
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") closePreviewDrawer();
    }, true);
    return drawer;
  }

  function closePreviewDrawer() {
    activePreviewDrawerKey = "";
    document.getElementById("cw-wall-preview-detail")?.classList.remove("show");
  }

  function sourceMarkup(item) {
    return sourceRowsForItem(item).slice(0, 6).map(({ provider, instance }) => {
      const name = provider;
      const src = providerLogoPath(name);
      const label = providerInstanceLabel(name, instance);
      return src
        ? `<span class="wpd-source"><img src="${esc(src)}" alt="${esc(label)} logo"></span>`
        : `<span class="wpd-source" aria-label="${esc(label)}">${esc(providerShortLabel(name).slice(0, 2))}</span>`;
    }).join("");
  }

  function renderPreviewDrawer(item, meta = null, loading = false) {
    const drawer = ensurePreviewDrawer();
    const poster = artUrl(item, "w342") || "/assets/img/placeholder_poster.svg";
    const backdrop = backdropUrl(item, meta);
    const releaseIso = mediaTypeOf(item) === "movie"
      ? (meta?.detail?.release_date || meta?.release?.date || item?.release_date || "")
      : (meta?.detail?.first_air_date || meta?.release?.date || item?.first_air_date || "");
    const title = item?.title || meta?.title || "Unknown title";
    const year = String(item?.year || meta?.year || yearFromIso(releaseIso) || "").trim();
    const genres = (Array.isArray(meta?.genres) ? meta.genres : Array.isArray(meta?.detail?.genres) ? meta.detail.genres : Array.isArray(item?.genres) ? item.genres : []).slice(0, 3);
    const chips = [
      mediaLabelOf(item),
      year,
      runtimeLabel(meta?.runtime_minutes),
      dateLabel(releaseIso),
      meta?.certification || meta?.release?.cert || meta?.detail?.certification || "",
      ...genres,
    ].filter(Boolean);
    const overview = meta?.overview || meta?.detail?.overview || meta?.detail?.tagline || (loading ? "Loading details..." : "No description available.");
    const score = scoreValue(meta);
    const scoreColor = score == null ? "#94a3b8" : score >= 70 ? "#49d391" : score >= 45 ? "#e4b85a" : "#e66b7a";
    const tmdb = tmdbUrl(item, meta);
    const imdb = imdbUrl(item, meta);
    const sourcesTitle = sourceRouteTitle(item);

    drawer.style.setProperty("--wpd-backdrop", backdrop ? `url("${backdrop}")` : "none");
    drawer.innerHTML = `
      <div class="wpd-inner">
        <img class="wpd-poster" src="${poster}" alt="" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        <div class="wpd-main">
          <div class="wpd-title-row">
            <div class="wpd-title">${esc(title)}${year ? ` <span class="wpd-year">${esc(year)}</span>` : ""}</div>
            <button type="button" class="wpd-close" aria-label="Close details"><span class="material-symbol">close</span></button>
          </div>
          <div class="wpd-meta">${chips.map((chip) => `<span class="wpd-chip">${esc(chip)}</span>`).join("")}</div>
          <div class="wpd-overview">${esc(overview)}</div>
          <div class="wpd-sources" title="${esc(sourcesTitle)}" aria-label="${esc(sourcesTitle || "Providers")}">${sourceMarkup(item)}</div>
        </div>
        <div class="wpd-side">
          ${score == null ? "" : `<div class="wpd-score" style="--wpd-score:${Math.max(0, Math.min(100, score)) * 3.6}deg;--wpd-score-color:${scoreColor}"><span>${score}%</span></div><div class="wpd-score-label">User Score</div>`}
          ${tmdb ? `<a class="wpd-link" href="${tmdb}" target="_blank" rel="noopener">View on TMDb</a>` : ""}
          ${imdb ? `<a class="wpd-link" href="${imdb}" target="_blank" rel="noopener">View on IMDb</a>` : ""}
        </div>
      </div>
    `;
    drawer.querySelector(".wpd-close")?.addEventListener("click", closePreviewDrawer, true);
    drawer.classList.add("show");
  }

  async function openPreviewDrawer(item) {
    if (!item) return;
    const activeKey = previewItemKey(item);
    activePreviewDrawerKey = activeKey;
    renderPreviewDrawer(item, null, true);
    const meta = await getPreviewMeta(item);
    const drawer = document.getElementById("cw-wall-preview-detail");
    if (!drawer?.classList.contains("show") || activePreviewDrawerKey !== activeKey) return;
    renderPreviewDrawer(item, meta || null, false);
  }
  const previewGate = async () => {
    const [wlEnabled, hasKey, uiAllowed] = await Promise.all([
      Promise.resolve(window.isWatchlistEnabledInPairs?.() ?? true).catch(() => false),
      hasTmdbKey().catch(() => false),
      isWatchlistPreviewAllowed().catch(() => true),
    ]);
    return { wlEnabled, hasKey, uiAllowed, allowed: !!(wlEnabled && hasKey && uiAllowed) };
  };
  const hidePreviewCard = (card, row, msg, { preserve = false } = {}) => {
    card?.classList.add("hidden");
    if (preserve) return;
    hideWatchlistCount();
    if (row) {
      row.innerHTML = "";
      row.classList.add("hidden");
      row.closest(".wall-wrap")?.classList.remove("is-empty");
    }
    if (msg) {
      msg.textContent = "";
      msg.classList.remove("is-empty");
    }
    window.wallLoaded = false;
  };
  const setWallEmpty = (row, msg, text) => {
    setWatchlistCount(0);
    window.__wallRenderSignature = "";
    row.replaceChildren();
    row.classList.add("hidden");
    row.closest(".wall-wrap")?.classList.add("is-empty");
    msg.textContent = text;
    msg.classList.add("is-empty");
    msg.classList.remove("hidden");
  };

  function pillFor(status) {
    const raw = String(status || "").toLowerCase().trim();
    if (raw === "deleted") return { text: "DELETED", cls: "p-del" };
    if (raw === "both") return { text: "SYNCED", cls: "p-syn" };
    const provider = providerFromStatus(raw);
    if (provider) return { text: providerShortLabel(provider).toUpperCase(), cls: PILL_CLASS_BY_PROVIDER[provider] || "p-sk" };
    return { text: "-", cls: "p-sk" };
  }

  function sourceRowsForItem(item) {
    const rows = [];
    const seen = new Set();
    const push = (provider, instance = DEFAULT_INSTANCE) => {
      const keyProvider = providerKey(provider);
      const inst = String(instance || DEFAULT_INSTANCE).trim() || DEFAULT_INSTANCE;
      const key = `${keyProvider}:${inst}`;
      if (!keyProvider || seen.has(key)) return;
      seen.add(key);
      rows.push({ provider: keyProvider, instance: inst });
    };

    const sbp = item?.sources_by_provider || item?.sourcesByProvider || {};
    if (sbp && typeof sbp === "object") {
      for (const [provider, instances] of Object.entries(sbp)) {
        if (Array.isArray(instances) && instances.length) {
          for (const instance of instances) push(provider, instance);
        } else {
          push(provider);
        }
      }
    }
    if (rows.length) return rows;

    const direct = Array.isArray(item?.sources) ? item.sources : [];
    for (const source of direct) {
      if (source && typeof source === "object") push(source.provider, source.instance);
      else push(source);
    }
    if (rows.length) return rows;

    const provider = providerFromStatus(item?.status);
    if (provider) push(provider);
    return rows;
  }

  function providersForItem(item) {
    return [...new Set(sourceRowsForItem(item).map((row) => row.provider))];
  }

  function sourceRouteTitle(item) {
    const labels = sourceRowsForItem(item).slice(0, 8).map((row) => providerInstanceLabel(row.provider, row.instance));
    if (!labels.length) return "";
    return labels.length > 1 ? `Sources: ${labels.join(" -> ")}` : `Source: ${labels[0]}`;
  }

  function providerIconMarkup(name) {
    const src = providerLogoPath(name);
    const label = providerLabel(name);
    const shortLabel = providerShortLabel(name);
    const shell = `display:inline-flex;align-items:center;justify-content:center;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:rgba(7,11,18,.38);box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 8px 20px rgba(0,0,0,.18);backdrop-filter:blur(10px) saturate(120%);-webkit-backdrop-filter:blur(10px) saturate(120%);`;
    return src
      ? `<span style="${shell}width:28px;height:28px;padding:0 5px;"><img src="${esc(src)}" alt="${esc(label)} logo" style="display:block;width:auto;height:16px;max-width:20px;object-fit:contain;filter:brightness(1.05)"></span>`
      : `<span aria-label="${esc(label)}" style="${shell}min-width:28px;height:28px;padding:0 7px;font-size:11px;font-weight:800;line-height:1;color:rgba(245,248,255,.88);">${esc(shortLabel)}</span>`;
  }

  function wallSignature(items, epoch) {
    return JSON.stringify({
      epoch: Number(epoch || 0),
      items: (Array.isArray(items) ? items : []).map((item) => [
        item?.key || "",
        item?.status || "",
        item?.tmdb || "",
        item?.type || "",
        item?.year || "",
      ]),
    });
  }

  function renderWall(row, msg, items, lastSyncEpoch, { preserveIfSame = false, total = null } = {}) {
    let wallItems = Array.isArray(items) ? items.slice() : [];
    if (!wallItems.length) {
      setWallEmpty(row, msg, "No items to show yet.");
      return false;
    }

    const signature = wallSignature(wallItems, lastSyncEpoch);
    const hasRenderedWall = row.childElementCount > 0 && !row.classList.contains("hidden");
    if (preserveIfSame && signature === window.__wallRenderSignature && hasRenderedWall) {
      msg.classList.add("hidden");
      row.classList.remove("hidden");
      return true;
    }

    const hidden = readHidden();
    const isDeleted = (item) => {
      if (hidden.has(item.key) && String(item.status || "").toLowerCase() === "deleted") return true;
      if (hidden.has(item.key) && String(item.status || "").toLowerCase() !== "deleted") {
        hidden.delete(item.key);
        writeHidden(hidden);
      }
      return !!(window._deletedKeys && window._deletedKeys.has(item.key));
    };

    const firstSeen = firstSeenMap();
    const now = Date.now();
    for (const item of wallItems) if (item?.key && !firstSeen[item.key]) firstSeen[item.key] = now;
    try { localStorage.setItem("wl_first_seen", JSON.stringify(firstSeen)); } catch {}

    const getTs = (it) => Number(it?.added_epoch ?? it?.added_ts ?? it?.created_ts ?? it?.created ?? it?.epoch ?? firstSeen[it?.key] ?? 0);
    wallItems.sort((a, b) => getTs(b) - getTs(a));
    setWatchlistCount(total ?? wallItems.length);
    wallItems = wallItems.slice(0, Number.isFinite(window.MAX_WALL_POSTERS) ? window.MAX_WALL_POSTERS : 20);

    const frag = document.createDocumentFragment();
    let renderedCount = 0;
    const itemMap = new Map();

    for (const item of wallItems) {
      if (!item?.tmdb) continue;
      const link = document.createElement("a");
      const source = isDeleted(item) ? "deleted" : (item.status || "");
      const pill = pillFor(source);
      const itemKey = previewItemKey(item);
      itemMap.set(itemKey, item);

      link.className = "poster";
      link.href = `https://www.themoviedb.org/${isTV(item.type) ? "tv" : "movie"}/${item.tmdb}`;
      link.target = "_blank";
      link.rel = "noopener";
      link.style.cursor = "pointer";
      link.title = `Show details for ${item.title || "this item"}`;
      link.setAttribute("aria-label", `Show details for ${item.title || "this item"}`);
      link.dataset.type = item.type || "";
      link.dataset.tmdb = String(item.tmdb);
      link.dataset.key = item.key || "";
      link.dataset.previewKey = itemKey;
      link.dataset.source = source;

      const img = document.createElement("img");
      img.loading = renderedCount < 4 ? "eager" : "lazy";
      if (renderedCount < 4) img.fetchPriority = "high";
      img.alt = `${item.title || ""} (${item.year || ""})`;
      img.src = artUrl(item, "w342") || "/assets/img/placeholder_poster.svg";
      img.onerror = function () { this.onerror = null; this.src = "/assets/img/placeholder_poster.svg"; };
      link.appendChild(img);

      const overlay = document.createElement("div");
      const currentProviders = providersForItem(item).slice(0, 5);
      const routeTitle = sourceRouteTitle(item);
      const synced = String(source).toLowerCase() === "both";
      overlay.className = "ovr";
      if (routeTitle) {
        overlay.title = routeTitle;
        overlay.setAttribute("aria-label", routeTitle);
      }
      overlay.style.left = "8px";
      overlay.style.right = synced ? "8px" : "auto";
      overlay.style.justifyContent = synced ? "center" : "flex-start";
      overlay.style.width = synced ? "calc(100% - 16px)" : "auto";
      overlay.innerHTML = synced
        ? `<div class="pill ${pill.cls}">${pill.text}</div>`
        : currentProviders.map(providerIconMarkup).join("");
      link.appendChild(overlay);

      const cap = document.createElement("div");
      cap.className = "cap";
      cap.textContent = `${item.title || ""}${item.year ? ` - ${item.year}` : ""}`;
      link.appendChild(cap);

      const hover = document.createElement("div");
      hover.className = "hover";
      hover.innerHTML = `
        <div class="titleline">${item.title || ""}</div>
        <div class="meta">
          <div class="chip time">${lastSyncEpoch ? `updated ${window.relTimeFromEpoch?.(lastSyncEpoch) || ""}` : ""}</div>
        </div>`;
      link.appendChild(hover);

      frag.appendChild(link);
      renderedCount++;
    }

    if (!renderedCount) {
      setWallEmpty(row, msg, "No items to show yet.");
      return false;
    }

    window._lastSyncEpoch = lastSyncEpoch || null;
    window.__cwWallPreviewItems = itemMap;
    row.replaceChildren(frag);
    row.closest(".wall-wrap")?.classList.remove("is-empty");
    row.classList.remove("hidden");
    msg.classList.remove("is-empty");
    msg.classList.add("hidden");
    window.__wallRenderSignature = wallSignature(wallItems, lastSyncEpoch);
    initWallInteractions();
    return true;
  }

  async function loadWall() {
    const card = document.getElementById("placeholder-card");
    const msg = document.getElementById("wall-msg");
    const row = document.getElementById("poster-row");
    if (!card || !msg || !row) return;
    if (!isOnMain()) { hidePreviewCard(card, row, msg, { preserve: true }); return; }

    const myReq = ++wallReqSeq;
    const refreshVersion = window.__cwWallPreviewDirtyVersion;
    let renderedWall = hasRenderedWall(row);
    if (!renderedWall) {
      msg.textContent = "Loading...";
      msg.classList.remove("is-empty");
      msg.classList.remove("hidden");
      row.closest(".wall-wrap")?.classList.remove("is-empty");
      row.classList.add("hidden");
      const cached = readWallCache();
      if (cached) {
        renderedWall = renderWall(row, msg, cached.items, cached.last_sync_epoch || 0, { total: cached.total ?? null });
      }
    }

    const limit = Number.isFinite(window.MAX_WALL_POSTERS) ? Math.max(1, Number(window.MAX_WALL_POSTERS)) : 20;
    const wallDataPromise = json(`/api/state/wall?both_only=0&active_only=1&limit=${encodeURIComponent(limit)}`)
      .then((data) => ({ data }), (error) => ({ error }));

    try {
      const gate = await previewGate();
      if (myReq !== wallReqSeq) return false;
      if (!isOnMain()) { hidePreviewCard(card, row, msg, { preserve: true }); return false; }
      if (!gate.allowed) {
        hidePreviewCard(card, row, msg);
        return false;
      }
      card.classList.remove("hidden");

      const wallResult = await wallDataPromise;
      if (wallResult.error) throw wallResult.error;
      const data = wallResult.data;
      if (myReq !== wallReqSeq) return false;
      if (!isOnMain()) { hidePreviewCard(card, row, msg, { preserve: true }); return false; }
      if (data?.missing_tmdb_key) { hidePreviewCard(card, row, msg); return false; }
      if (!data?.ok) { msg.textContent = data?.error || "No state data found."; return false; }

      let items = Array.isArray(data.items) ? data.items.slice() : [];
      if (!items.length) items = (data.items || []).filter((it) => String(it?.status || "").toLowerCase() === "both");
      window._lastSyncEpoch = data.last_sync_epoch || null;
      if (!items.length) {
        setWallEmpty(row, msg, "No items to show yet.");
        markPreviewClean(refreshVersion);
        return true;
      }
      writeWallCache(items, data.last_sync_epoch || 0, data?.total ?? items.length);
      renderWall(row, msg, items, data.last_sync_epoch || 0, { preserveIfSame: true, total: data?.total ?? items.length });
      markPreviewClean(refreshVersion);
      return true;
    } catch {
      if (!renderedWall) {
        row.classList.add("hidden");
        msg.classList.remove("hidden");
      }
      msg.textContent = "Failed to load preview.";
      return renderedWall;
    }
  }

  async function hasTmdbKey() {
    const pick = (cfg) => {
      const fromBlock = (blk) => {
        if (!blk || typeof blk !== "object") return "";
        const direct = String(blk.api_key || "").trim();
        if (direct) return direct;
        const insts = blk.instances;
        if (!insts || typeof insts !== "object") return "";
        for (const value of Object.values(insts)) {
          const key = value && typeof value === "object" ? String(value.api_key || "").trim() : "";
          if (key) return key;
        }
        return "";
      };
      return fromBlock(cfg?.tmdb);
    };

    try {
      return !!pick(await getConfig());
    } catch {
      return false;
    }
  }

  function isOnMain() {
    const tab = String(document.documentElement.dataset.tab || "").toLowerCase();
    if (tab) return tab === "main";
    return !!document.getElementById("tab-main")?.classList.contains("active");
  }

  async function isWatchlistPreviewAllowed() {
    try {
      const cfg = await getConfig();
      const ui = cfg?.ui || cfg?.user_interface || {};
      return typeof ui.show_watchlist_preview === "boolean" ? !!ui.show_watchlist_preview : true;
    } catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return true;
      console.warn("isWatchlistPreviewAllowed failed, falling back to true", e);
      return true;
    }
  }

  async function updateWatchlistPreview({ force = true } = {}) {
    try {
      const card = document.getElementById("placeholder-card");
      const row = document.getElementById("poster-row");
      const msg = document.getElementById("wall-msg");
      if (!isOnMain()) {
        hidePreviewCard(card, row, msg, { preserve: true });
        return false;
      }
      card?.classList.remove("hidden");
      const rendered = hasRenderedWall(row);
      if (!force && !previewNeedsRefresh() && (window.wallLoaded || rendered)) return true;
      if (window.__wallLoading) return !!(window.wallLoaded || rendered);
      const refreshVersion = window.__cwWallPreviewDirtyVersion;
      window.__wallLoading = true;
      try { window.wallLoaded = !!(await loadWall()); }
      finally {
        window.__wallLoading = false;
        if (window.__cwWallPreviewDirtyVersion !== refreshVersion && isOnMain()) {
          window.setTimeout(() => updateWatchlistPreview({ force: true }), 0);
        }
      }
      if (!isOnMain()) {
        hidePreviewCard(card, row, msg, { preserve: true });
        return false;
      }
      return !!(window.wallLoaded || hasRenderedWall(row));
    } catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return;
      console.error("Failed to update watchlist preview:", e);
    }
  }

  async function updatePreviewVisibility() {
    if (previewBusy) return !!(window.wallLoaded || hasRenderedWall());
    previewBusy = true;
    try {
      const card = document.getElementById("placeholder-card");
      const row = document.getElementById("poster-row");
      const msg = document.getElementById("wall-msg");
      if (!card) return false;

      if (!isOnMain()) { hidePreviewCard(card, row, msg, { preserve: true }); return false; }
      card.classList.remove("hidden");
      const rendered = hasRenderedWall(row);
      if (msg && !window.wallLoaded && !rendered) {
        msg.textContent = "Loading...";
        msg.classList.remove("hidden");
      }

      if ((!window.wallLoaded || previewNeedsRefresh()) && !window.__wallLoading) {
        const pending = updateWatchlistPreview({ force: true });
        if (rendered) void pending;
        else await pending;
      }
      return !!(window.wallLoaded || rendered);
    } finally {
      previewBusy = false;
    }
  }

  function markWatchlistPreviewDirty() {
    window.__cwWallPreviewDirty = true;
    window.__cwWallPreviewDirtyVersion += 1;
    if (!isOnMain()) return false;
    void updateWatchlistPreview({ force: true });
    return true;
  }

  window.addEventListener("storage", (event) => {
    if (event.key !== "wl_hidden") return;
    updatePreviewVisibility();
    window.dispatchEvent(new CustomEvent("watchlist-hidden-changed"));
  });
  window.addEventListener("sync-complete", markWatchlistPreviewDirty);
  window.addEventListener("watchlist:refresh", markWatchlistPreviewDirty);

  const WatchlistPreview = {
    updateEdges,
    scrollWall,
    initWallInteractions,
    artUrl,
    loadWall,
    updateWatchlistPreview,
    hasTmdbKey,
    isOnMain,
    isWatchlistPreviewAllowed,
    updatePreviewVisibility,
    markWatchlistPreviewDirty,
  };

  (window.CW ||= {}).WatchlistPreview = WatchlistPreview;
  Object.assign(window, WatchlistPreview);
})();
