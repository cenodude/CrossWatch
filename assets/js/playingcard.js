(() => {
  if (window.__PLAYING_CARD_INIT__) return;
  window.__PLAYING_CARD_INIT__ = 1;

  const getCfg = () => {
    try {
      return window._cfgCache || null;
    } catch {
      return null;
    }
  };

  const hasTmdbKey = (cfg) => {
    const key = cfg?.tmdb?.api_key;
    if (typeof key === "string") return key.trim().length > 0;
    return !!key;
  };

  const isUiEnabled = () => {
    const cfg = getCfg();
    if (!cfg) return false;
    const ui = cfg.ui || {};
    if (ui.show_playingcard === false) return false;
    if (!hasTmdbKey(cfg)) return false;
    return true;
  };

  const isActiveState = (s) => {
    const v = String(s || "").toLowerCase();
    return v === "playing" || v === "paused" || v === "buffering";
  };

  const keyOf = (p) => [
    p.media_type || p.type || "",
    p.title || "",
    p.year || "",
    p.season || "",
    p.episode || ""
  ].join("|");

  const tmdbIdOf = (p) => {
    const mt = String(p.media_type || p.type || "").toLowerCase();
    const ids = p.ids || {};
    if (mt === "episode") {
      return ids.tmdb_show || p.tmdb_show || p.tmdb || p.tmdb_id || ids.tmdb || ids.id;
    }
    return p.tmdb || p.tmdb_id || ids.tmdb || ids.tmdb_show || ids.id;
  };

  const imdbIdOf = (p, meta) => {
    const ids = Object.assign({}, p.ids || {}, meta?.ids || {});
    const mt = String(p.media_type || p.type || "").toLowerCase();
    if (mt === "episode") return ids.imdb_show || ids.imdb || ids.imdb_id;
    return ids.imdb || ids.imdb_show || ids.imdb_id;
  };

  const buildTmdbUrl = (p) => {
    const id = tmdbIdOf(p);
    if (!id) return "";
    const t = (p.media_type || p.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `https://www.themoviedb.org/${t}/${encodeURIComponent(String(id))}`;
  };

  const buildImdbUrl = (p, meta) => {
    const id = imdbIdOf(p, meta);
    if (!id) return "";
    const clean = String(id).startsWith("tt") ? String(id) : `tt${id}`;
    return `https://www.imdb.com/title/${clean}`;
  };

  const buildArtUrl = (p) => {
    if (p.cover) return p.cover;
    const id = tmdbIdOf(p);
    if (!id) return "/assets/img/placeholder_poster.svg";
    const t = (p.media_type || p.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `/art/tmdb/${t}/${encodeURIComponent(String(id))}?size=w342`;
  };

  const runtimeLabel = (mins) => {
    const m = Number(mins) || 0;
    if (!m) return "";
    const h = Math.floor(m / 60);
    const mm = m % 60;
    return h ? `${h}h ${mm ? mm + "m" : ""}` : `${mm}m`;
  };

  const formatTime = (ms) => {
    const totalMs = Number(ms) || 0;
    if (!totalMs) return "";
    const totalSec = Math.floor(totalMs / 1000);
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
    return `${m}:${String(s).padStart(2, "0")}`;
  };

  const metaKey = (p) =>
    `${String(p.media_type || p.type || "").toLowerCase()}:${String(tmdbIdOf(p) || "")}`;

  const metaCache = new Map();
  const sourceLabel = (src) => {
    const s = String(src || "").toLowerCase();
    if (!s) return "";
    if (s === "plex") return "PLEX · watcher";
    if (s === "emby") return "EMBY · watcher";
    if (s === "jellyfin") return "Jellyfin";
    if (s === "plextrakt") return "PLEX · webhook";
    if (s === "embytrakt") return "EMBY · webhook";
    if (s === "jellyfintrakt") return "JF · webhook";
    return src;
  };

  const agoLabel = (updatedSec) => {
    const ts = Number(updatedSec);
    if (!ts) return "";
    const diffMs = Date.now() - ts * 1000;
    if (diffMs < 0) return "";
    const sec = Math.floor(diffMs / 1000);
    if (sec < 60) return "just now";
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min} min ago`;
    const hr = Math.floor(min / 60);
    if (hr < 24) return `${hr}h ago`;
    const d = Math.floor(hr / 24);
    return `${d}d ago`;
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

  const getMetaFor = async (p) => {
    const k = metaKey(p);
    const hit = metaCache.get(k);
    if (hit) return hit;

    const tmdb = String(tmdbIdOf(p) || "");
    if (!tmdb) return null;

    const mt = String(p.media_type || p.type || "").toLowerCase();
    const type = mt === "movie" ? "movie" : "show";

    try {
      const r = await fetch("/api/metadata/bulk?overview=full", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: [{ type, tmdb }],
          need: {
            overview: 1,
            tagline: 1,
            runtime_minutes: 1,
            poster: 1,
            ids: 1,
            videos: 1,
            genres: 1,
            certification: 1,
            score: 1,
            release: 1,
            backdrop: 1
          },
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

  const fetchCurrentlyWatching = async () => {
    try {
      const r = await fetch("/api/watch/currently_watching", { cache: "no-store" });
      if (!r.ok) return null;
      const j = await r.json();
      return j?.currently_watching || null;
    } catch {
      return null;
    }
  };

  const css = `
  #playing-detail{
    position:fixed;
    left:50%;
    bottom:20px;
    transform:translate(-50%,calc(100% + 30px));
    width:min(720px,calc(100vw - 420px));
    background:#05060b;
    border-radius:16px;
    box-shadow:0 20px 48px rgba(0,0,0,.6);
    border:1px solid rgba(255,255,255,.1);
    color:#fff;

    opacity:0;
    transition:
      transform .35s cubic-bezier(0.22,0.7,0.25,1),
      opacity  .25s ease-out,
      box-shadow .25s ease-out;

    z-index:10000;
    overflow:hidden;
  }
  #playing-detail.show{
    transform:translate(-50%,0);
    opacity:1;
  }

  #playing-detail.show:hover{
    transform:translate(-50%,-3px);
    box-shadow:0 24px 60px rgba(0,0,0,.8);
  }
  html[data-tab!="main"] #playing-detail{display:none!important;}

  #playing-detail::before{
    content:"";
    position:absolute;
    inset:4px;
    border-radius:12px;
    background-image:
      linear-gradient(
        90deg,
        rgba(4,6,12,0.94) 0%,
        rgba(4,6,12,0.93) 30%,
        rgba(4,6,12,0.90) 65%,
        rgba(4,6,12,0.86) 100%
      ),
      var(--pc-backdrop, none);
    background-size:100% 100%, cover;
    background-position:center center, right center;
    background-repeat:no-repeat,no-repeat;
    pointer-events:none;
    z-index:0;
  }

  #playing-detail .pc-body{
    position:relative;
    display:flex;
    flex-direction:column;
    justify-content:flex-start;
    padding-bottom:0px;
  }

  #playing-detail .pc-inner{
    position:relative;
    z-index:1;
    display:grid;
    grid-template-columns:100px 1fr 160px;
    gap:16px;
    align-items:stretch;
    padding:14px 16px;
  }

  #playing-detail .pc-poster{
    width:100px;
    border-radius:12px;
    object-fit:cover;
    box-shadow:0 8px 20px rgba(0,0,0,.5);
    background:#000;
  }

  #playing-detail .pc-title-row{
    display:flex;
    align-items:flex-start;
    gap:8px;
  }

  #playing-detail .pc-title{
    font-weight:600;
    font-size:17px;
    letter-spacing:.01em;
  }

  #playing-detail .pc-close{
    margin-left:auto;
    border:none;
    background:transparent;
    color:#ddd;
    cursor:pointer;
    font-size:12px;
    line-height:1;
    padding:2px 6px;
    display:flex;
    align-items:center;
    gap:4px;
    text-transform:uppercase;
    letter-spacing:.08em;
  }
  #playing-detail .pc-close span{font-size:16px;line-height:1;}
  #playing-detail .pc-close:hover{color:#fff;}

  #playing-detail .pc-meta{
    display:flex;
    flex-wrap:wrap;
    gap:6px;
    margin-top:6px;
  }

  #playing-detail .pc-chip{
    display:inline-flex;
    align-items:center;
    padding:3px 8px;
    border-radius:999px;
    background:rgba(255,255,255,.06);
    font-size:11px;
    letter-spacing:.02em;
    text-transform:uppercase;
  }

  #playing-detail .pc-chip-source-plex{
    background:rgba(229,160,13,0.18);
    color:#ffd36a;
    border:1px solid rgba(229,160,13,0.45);
  }

  #playing-detail .pc-chip-source-emby{
    background:rgba(76,175,80,0.18);
    color:#bbf1bf;
    border:1px solid rgba(76,175,80,0.45);
  }

  #playing-detail .pc-chip-source-jellyfin{
    background:rgba(170,92,195,0.18);
    color:#e8cff8;
    border:1px solid rgba(170,92,195,0.45);
  }

  #playing-detail .pc-overview{
    margin-top:8px;
    font-size:13px;
    color:#ccc;
    max-height:3.2em;
    overflow:hidden;
    text-overflow:ellipsis;
    display:-webkit-box;
    -webkit-line-clamp:2;
    -webkit-box-orient:vertical;
  }

  #playing-detail .pc-progress-wrap{
    margin-top:auto;
    position:relative;
    width:calc(100% - 25px);
    max-width:100%;
    box-sizing:border-box;
  }

  #playing-detail .pc-progress-bg{
    position:relative;
    width:100%;
    height:22px;
    background:rgba(27,28,43,.9);
    border-radius:999px;
    overflow:hidden;
  }

  #playing-detail .pc-progress{
    width:0;
    height:100%;
    background:linear-gradient(90deg,rgba(4,120,87,.9),rgba(34,197,94,.9));
    transition:width .4s cubic-bezier(0.22,0.7,0.25,1);
  }

  #playing-detail .pc-progress::after{
    content:"";
    position:absolute;
    inset:0;
    border-radius:999px;
    box-shadow:0 0 14px rgba(34,197,94,.25);
    pointer-events:none;
  }

  #playing-detail .pc-progress-labels {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 10px;
    pointer-events: none;
    font-size: 12px;
    font-weight: 600;
    color: #e5e7eb;
    text-shadow: 0 1px 2px rgba(0,0,0,.8);
    width: 100%;
    height: 100%;
    box-sizing: border-box;
  }

  #playing-detail .pc-right{
    display:flex;
    flex-direction:column;
    align-items:flex-end;
    justify-content:flex-start;
    gap:4px;
  }

  #playing-detail .pc-score-circle{
    --pc-score-deg:0deg;
    --pc-score-color:#16a34a;
    --pc-score-track:#111827;
    position:relative;
    width:56px;
    height:56px;
    border-radius:50%;
    background:conic-gradient(var(--pc-score-color) var(--pc-score-deg),var(--pc-score-track) var(--pc-score-deg));
    display:flex;
    align-items:center;
    justify-content:center;
    color:#fff;
    font-weight:700;
  }
  #playing-detail .pc-score-circle::before{
    content:"";
    position:absolute;
    inset:4px;
    border-radius:50%;
    background:#020713;
  }
  #playing-detail .pc-score-circle.is-empty{
    background:conic-gradient(#374151 0deg,#111827 0deg);
  }
  #playing-detail #pc-score{
    position:relative;
    font-size:18px;
  }

  #playing-detail .pc-score-label{
    font-size:12px;
    color:#aaa;
  }

  #playing-detail .pc-link{
    color:#8ab4ff;
    font-size:12px;
    text-decoration:none;
  }
  #playing-detail .pc-link + .pc-link{margin-top:2px;}
  #playing-detail .pc-link:hover{text-decoration:underline;}


  #playing-detail .pc-status{
    position:static;
    margin-top:auto;
    font-size:12px;
    font-weight:600;
    text-transform:uppercase;
    letter-spacing:.08em;
    color:#e5e7eb;
    opacity:.9;
    white-space:nowrap;
  }

  @media (max-width:1024px){
    #playing-detail{width:calc(100vw - 40px);}
  }

  @media (max-width:768px){
    #playing-detail .pc-inner{
      grid-template-columns:80px 1fr;
      grid-template-rows:auto auto;
    }
    #playing-detail .pc-right{
      grid-column:span 2;
      flex-direction:row;
      justify-content:space-between;
    }
  }
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
            <span>×</span><span>Hide</span>
          </button>
        </div>
        <div id="pc-meta" class="pc-meta"></div>
        <div id="pc-overview" class="pc-overview"></div>
        <div class="pc-progress-wrap">
          <div class="pc-progress-bg">
            <div id="pc-progress" class="pc-progress"></div>
          </div>
          <div class="pc-progress-labels">
            <span id="pc-progress-pct"></span>
            <span id="pc-progress-time"></span>
          </div>
        </div>
      </div>
      <div class="pc-right">
        <div id="pc-score-circle" class="pc-score-circle is-empty">
          <span id="pc-score">--</span>
        </div>
        <div class="pc-score-label">User Score</div>
        <a id="pc-tmdb" class="pc-link" href="#" target="_blank" rel="noopener noreferrer"></a>
        <a id="pc-imdb" class="pc-link" href="#" target="_blank" rel="noopener noreferrer"></a>
        <div id="pc-status" class="pc-status">Now Playing</div>
      </div>
    </div>

  `;
  document.body.appendChild(detail);


  const posterEl      = detail.querySelector("#pc-poster");
  const titleEl       = detail.querySelector("#pc-title");
  const metaEl        = detail.querySelector("#pc-meta");
  const overviewEl    = detail.querySelector("#pc-overview");
  const progEl        = detail.querySelector("#pc-progress");
  const progPctEl     = detail.querySelector("#pc-progress-pct");
  const progTimeEl    = detail.querySelector("#pc-progress-time");
  const scoreCircleEl = detail.querySelector("#pc-score-circle");
  const scoreEl       = detail.querySelector("#pc-score");
  const tmdbEl        = detail.querySelector("#pc-tmdb");
  const imdbEl        = detail.querySelector("#pc-imdb");
  const statusEl      = detail.querySelector("#pc-status");
  const closeBtn      = detail.querySelector("#pc-close");

  posterEl.onerror = () => {
    posterEl.onerror = null;
    posterEl.src = "/assets/img/placeholder_poster.svg";
  };

  let lastKey = null;
  let dismissedKey = null;
  let lastUpdatedSec = 0;
  let lastStatusLabel = "";

  const hide = () => {
    detail.classList.remove("show");
    lastKey = null;
    lastUpdatedSec = 0;
    lastStatusLabel = "";
  };

  closeBtn.addEventListener("click", () => {
    if (lastKey) dismissedKey = lastKey;
    hide();
  }, true);

  const addChip = (txt, extraClass) => {
    const t = (txt || "").trim();
    if (!t) return;
    const span = document.createElement("span");
    span.className = "pc-chip";
    if (extraClass) span.classList.add(extraClass);
    span.textContent = t;
    metaEl.appendChild(span);
  };

  const applyMeta = (p, meta) => {
    if (!meta) return;

    const mediaType = String(p.media_type || p.type || "").toLowerCase();
    const typeLabel =
      mediaType === "movie" ? "Movie" :
      mediaType === "episode" ? "Episode" :
      mediaType ? mediaType.toUpperCase() : "TV";

    metaEl.innerHTML = "";

    const rawSrc = String(p.source || "").toLowerCase();
    const srcLabel = sourceLabel(p.source);
    let srcClass = "";
    if (rawSrc.includes("plex")) srcClass = "pc-chip-source-plex";
    else if (rawSrc.includes("emby")) srcClass = "pc-chip-source-emby";
    else if (rawSrc.includes("jellyfin")) srcClass = "pc-chip-source-jellyfin";
    if (srcLabel) addChip(srcLabel, srcClass);
    if (typeLabel) addChip(typeLabel);

    let yearLabel = "";

    if (mediaType === "episode" && p.season && p.episode) {
      addChip(`S${String(p.season).padStart(2,"0")}E${String(p.episode).padStart(2,"0")}`);
    } else if (p.year) {
      yearLabel = String(p.year);
    }

    const det = meta.detail || {};
    let releaseRaw = meta.release?.date || det.release_date || det.first_air_date || "";
    let releaseLabel = "";

    if (releaseRaw) {
      let s = String(releaseRaw).trim();
      if (s.includes("T")) s = s.split("T")[0]; // drop time
      releaseLabel = s;
    }

    if (yearLabel && releaseLabel && releaseLabel.startsWith(yearLabel)) {
      addChip(releaseLabel);          // e.g. "2025-09-05"
    } else {
      if (yearLabel) addChip(yearLabel);          // just the year
      if (releaseLabel && releaseLabel !== yearLabel) addChip(releaseLabel);
    }

    const runtimeMin =
      meta.runtime_minutes ??
      det.runtime_minutes ??
      meta.runtime ??
      det.runtime;

    if (runtimeMin) addChip(runtimeLabel(runtimeMin));

    if (!p.duration_ms && runtimeMin) {
      const pct = Math.max(0, Math.min(100, Number(p.progress) || 0));
      if (pct > 0) {
        const totalMs = Number(runtimeMin) * 60 * 1000; // minutes → ms
        if (totalMs > 0) {
          const remainingMs = Math.max(0, totalMs - totalMs * (pct / 100));
          const remainingStr = formatTime(remainingMs);
    
          if (remainingStr && !progTimeEl.textContent) {
            progTimeEl.textContent = `${remainingStr} left`;
          }
        }
      }
    }

    const gs = (Array.isArray(meta.genres) ? meta.genres : Array.isArray(det.genres) ? det.genres : [])
      .map(g => typeof g === "string" ? g : (g?.name || g?.title || ""))
      .filter(Boolean);
    // Disable genre chips for now
    // if (gs.length) addChip(gs.slice(0,2).join(", "));

    if (!p.overview) {
      const ov = meta.overview || det.overview || det.tagline;
      if (ov) overviewEl.textContent = ov;
    }

    const score100 = Number.isFinite(meta.score) ? Math.round(meta.score) : null;
    if (score100 != null) {
      const value = Math.max(0, Math.min(100, score100));
      const deg = value * 3.6;

      let color = "#22c55e";
      if (value <= 49) color = "#ef4444";
      else if (value <= 74) color = "#f59e0b";

      scoreEl.textContent = `${value}%`;
      scoreCircleEl.classList.remove("is-empty");
      scoreCircleEl.style.setProperty("--pc-score-deg", `${deg}deg`);
      scoreCircleEl.style.setProperty("--pc-score-color", color);
    } else {
      scoreEl.textContent = "--";
      scoreCircleEl.classList.add("is-empty");
      scoreCircleEl.style.setProperty("--pc-score-deg", "0deg");
      scoreCircleEl.style.setProperty("--pc-score-color", "#374151");
    }

    const tmdbUrl = buildTmdbUrl(Object.assign({}, p, {
      tmdb: tmdbIdOf(p) || (meta.ids && (meta.ids.tmdb || meta.ids.id))
    }));
    if (tmdbUrl) {
      tmdbEl.href = tmdbUrl;
      tmdbEl.textContent = "View on TMDb ↗";
      tmdbEl.style.display = "";
    } else {
      tmdbEl.style.display = "none";
      tmdbEl.textContent = "";
    }

    const imdbUrl = buildImdbUrl(p, meta);
    if (imdbUrl) {
      imdbEl.href = imdbUrl;
      imdbEl.textContent = "View on IMDb ↗";
      imdbEl.style.display = "";
    } else {
      imdbEl.style.display = "none";
      imdbEl.textContent = "";
    }

    const bd = backdropFromMeta(meta);
    if (bd) {
      detail.style.setProperty("--pc-backdrop", `url("${bd}")`);
    } else {
      detail.style.setProperty("--pc-backdrop", "none");
    }
  };

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

    if (!tmdbIdOf(p)) {
      const api = await fetchCurrentlyWatching();
      if (api) p = api;
    }

    const state = p.state || p.status || eventState;
    if (!p.title || !isActiveState(state)) {
      hide();
      return;
    }

    const k = keyOf(p);
    if (dismissedKey && dismissedKey === k) return;

    // New item: reset cached updated timestamp
    if (k !== lastKey) {
      lastKey = k;
      lastUpdatedSec = 0;
    }

    const src = String(p.source || "").toLowerCase();
    const st = String(state || "").toLowerCase();

    // Normalize updated timestamp, fall back to last known value for this item
    let updatedSec = Number(p.updated) || 0;
    if (!updatedSec && lastUpdatedSec && lastKey === k) {
      updatedSec = lastUpdatedSec;
    }

    if (updatedSec) {
      lastUpdatedSec = updatedSec;

      const now = Date.now();
      const ageMs = now - updatedSec * 1000;
      const maxAgeMs =
        st === "paused" ? 4 * 60 * 60 * 1000 :
        10 * 60 * 1000;

      if (ageMs > maxAgeMs) {
        hide();
        return;
      }
    }

    const numericProgress = Number(p.progress) || 0;

    let statusLabel =
      st === "paused" ? "Paused" :
      st === "buffering" ? "Buffering..." :
      st === "stopped" ? "Stopped" :
      "Now Playing";

    const ago = updatedSec ? agoLabel(updatedSec) : "";
    if (ago) {
      statusLabel += ` • ${ago}`;
    } else if (lastStatusLabel && lastStatusLabel.startsWith("Now Playing •")) {
      statusLabel = lastStatusLabel;
    }

    statusEl.textContent = statusLabel;
    statusEl.title = `source=${p.source || ""}, state=${state || ""}, updated=${updatedSec || ""}`;
    lastStatusLabel = statusLabel;  // remember what we actually showed

    const mediaType = String(p.media_type || p.type || "").toLowerCase();
    const typeLabel =
      mediaType === "movie" ? "Movie" :
      mediaType === "episode" ? "Episode" :
      mediaType ? mediaType.toUpperCase() : "TV";

    const pct = Math.round(Math.max(0, Math.min(100, numericProgress)));

    titleEl.textContent = p.year ? `${p.title} ${p.year}` : (p.title || "Now Playing");
    overviewEl.textContent = p.overview || "";

    metaEl.innerHTML = "";
    const rawSrc = String(p.source || "").toLowerCase();
    const baseSrcLabel = sourceLabel(p.source);
    let baseSrcClass = "";
    if (rawSrc.includes("plex")) baseSrcClass = "pc-chip-source-plex";
    else if (rawSrc.includes("emby")) baseSrcClass = "pc-chip-source-emby";
    else if (rawSrc.includes("jellyfin")) baseSrcClass = "pc-chip-source-jellyfin";
    if (baseSrcLabel) addChip(baseSrcLabel, baseSrcClass);
    if (typeLabel) addChip(typeLabel);
    if (mediaType === "episode" && p.season && p.episode) {
      addChip(`S${String(p.season).padStart(2,"0")}E${String(p.episode).padStart(2,"0")}`);
    } else if (p.year) {
      addChip(String(p.year));
    }

    const art = buildArtUrl(p);
    posterEl.src = art;
    posterEl.alt = p.title || "Poster";

    progEl.style.width = `${pct}%`;
    let progText = `${pct}% watched`;
    let timeLabel = "";

    if (p.duration_ms && pct > 0) {
      const totalMs = Number(p.duration_ms) || 0;
      if (totalMs > 0) {
        const remainingMs = Math.max(0, totalMs - totalMs * (pct / 100));
        const remainingStr = formatTime(remainingMs);
        if (remainingStr) {
          timeLabel = `${remainingStr} left`;
        }
      }
    }

    progPctEl.textContent = progText;
    progTimeEl.textContent = timeLabel;

    // reset score
    scoreEl.textContent = "--";
    scoreCircleEl.classList.add("is-empty");
    scoreCircleEl.style.setProperty("--pc-score-deg", "0deg");
    scoreCircleEl.style.setProperty("--pc-score-color", "#374151");

    tmdbEl.style.display = "none";
    tmdbEl.textContent = "";
    imdbEl.style.display = "none";
    imdbEl.textContent = "";

    detail.style.setProperty("--pc-backdrop", "none");
    detail.classList.add("show");

    const meta = await getMetaFor(p);
    if (!meta) return;
    if (k !== lastKey) return;
    applyMeta(p, meta);
  }

  window.addEventListener("currently-watching-updated", (ev) => {
    try {
      const d = ev.detail || {};
      if (!d || d.state === "stopped") {
        hide();
        dismissedKey = null;
        return;
      }
      render(d);
    } catch {}
  });

  window.updatePlayingCard = render;
})();
