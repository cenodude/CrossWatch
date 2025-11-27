// watchlist.js - client-side watchlist management (patched)

(function () {

  /* ========= styles ========= */
  const css = `
  .wl-wrap{display:grid;grid-template-columns:minmax(0,1fr) 360px;gap:16px}
  .wl-controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px}
  .wl-input{background:#15151c;border:1px solid rgba(255,255,255,.12);border-radius:8px;padding:8px 10px;color:#fff;width:100%}
  .wl-btn{background:#1d1d26;border:1px solid rgba(255,255,255,.15);border-radius:8px;color:#fff;padding:8px 10px;cursor:pointer}
  .wl-btn.danger{background:#2a1113;border-color:#57252a}
  .wl-chip{display:inline-flex;align-items:center;gap:6px;border-radius:16px;padding:6px 10px;background:#171720;border:1px solid rgba(255,255,255,.1);white-space:nowrap}
  .wl-muted{opacity:.7}
  .wl-empty{padding:24px;border:1px dashed rgba(255,255,255,.12);border-radius:12px;text-align:center}

  /* Posters */
  .wl-grid{--wl-min:150px;display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(var(--wl-min),1fr))}
  .wl-card{position:relative;border-radius:12px;overflow:hidden;background:#0f0f13;border:1px solid rgba(255,255,255,.08);transition:box-shadow .15s,border-color .15s;aspect-ratio:2/3}
  .wl-card img{width:100%;height:100%;object-fit:cover;display:block}
  .wl-card .wl-tags{position:absolute;left:8px;top:8px;display:flex;gap:6px;flex-wrap:wrap;z-index:2}
  .wl-tag{font-size:11px;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.35)}
  .wl-card.selected{box-shadow:0 0 0 3px #6f6cff,0 0 0 5px rgba(111,108,255,.35)}

  /* List */
  .wl-table-wrap{border:1px solid rgba(255,255,255,.12);border-radius:10px;overflow:auto}
  .wl-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed}
  .wl-table col.c-sel{width:44px}
  .wl-table th,.wl-table td{padding:6px 8px;border-bottom:1px solid rgba(255,255,255,.08);white-space:nowrap;text-align:left}
  .wl-table th{position:sticky;top:0;background:#101018;font-weight:600;z-index:1}
  .wl-table tr:last-child td{border-bottom:none}
  .wl-table .wl-title{white-space:normal; text-transform:none; letter-spacing:normal; font-weight:inherit}
  .wl-table td.rel{white-space:normal;overflow:hidden;text-overflow:ellipsis}
  .wl-table td.genre{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .wl-table th.sortable{cursor:pointer;user-select:none}
  .wl-table th.sortable::after{content:"";margin-left:6px;opacity:.6}
  .wl-table th.sort-asc::after{content:"▲"} /* patched */
  .wl-table th.sort-desc::after{content:"▼"} /* patched */

  /* Poster thumb in list */
  .wl-mini{width:36px;height:54px;border-radius:4px;object-fit:cover;background:#0f0f13;border:1px solid rgba(255,255,255,.08)}

  /* Sync matrix */
  .wl-matrix{display:flex;gap:10px;align-items:center}
  .wl-mat{display:flex;align-items:center;gap:6px;padding:4px 6px;border:1px solid rgba(255,255,255,.12);border-radius:8px;background:#14141c}
  .wl-mat img{height:14px}.wl-mat .material-symbol{font-size:16px}
  .wl-mat.ok{border-color:rgba(120,255,180,.35)} .wl-mat.miss{opacity:.6}

  /* Sidebar */
  .wl-side{display:flex;flex-direction:column;gap:6px}
  .ins-card{background:linear-gradient(180deg,rgba(20,20,28,.95),rgba(16,16,24,.95));border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:10px 12px}
  .ins-row{display:flex;align-items:center;gap:12px;padding:8px 6px;border-top:1px solid rgba(255,255,255,.06)}
  .ins-row:first-child{border-top:none;padding-top:2px}
  .ins-icon{width:32px;height:32px;border-radius:10px;display:flex;align-items:center;justify-content:center;background:#13131b;border:1px solid rgba(255,255,255,.06)}
  .ins-title{font-weight:700}
  .ins-kv{display:grid;grid-template-columns:110px 1fr;gap:10px;align-items:center}
  .ins-kv label{opacity:.85}
  .ins-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}
  .metric{position:relative;display:flex;align-items:center;gap:8px;background:#12121a;border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:10px}
  .metric .material-symbol{font-size:18px;opacity:.9}.metric .m-val{font-weight:700}.metric .m-lbl{font-size:12px;opacity:.75}

  /* Snackbar */
  .wl-snack{position:fixed;left:50%;transform:translateX(-50%);bottom:20px;background:#1a1a22;border:1px solid rgba(255,255,255,.15);border-radius:10px;padding:10px 12px;display:flex;gap:10px;align-items:center;z-index:9999}
  .wl-hidden{display:none!important}

  /* Trailer modal */
  .wl-modal{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,.6);z-index:10050}
  .wl-modal.show{display:flex}
  .wl-modal .box{position:relative;width:min(90vw,960px);aspect-ratio:16/9;background:#000;border:1px solid rgba(255,255,255,.12);border-radius:12px;overflow:hidden;box-shadow:0 10px 40px rgba(0,0,0,.6)}
  .wl-modal .box iframe{width:100%;height:100%}
  .wl-modal .box .x{position:absolute;top:8px;right:8px}

  /* hide poster overlays when toggled */
  .wl-hide-overlays .wl-tags{display:none!important}

  /* score colors */
  .wl-detail .actions .score.good{ color:#2ecc71 }
  .wl-detail .actions .score.mid { color:#f0ad4e }
  .wl-detail .actions .score.bad { color:#e74c3c }

  /* detail bar */
  .wl-detail{
    position:fixed;left:50%;bottom:12px;
    transform:translate(-50%, calc(100% + 12px));
    width:min(640px, calc(100vw - 420px));
    background:#101018;border:1px solid rgba(255,255,255,.12);
    border-radius:14px;box-shadow:0 18px 48px rgba(0,0,0,.55);
    z-index:10000;transition:transform .3s ease;
  }
  .wl-detail.show{transform:translate(-50%,0)}

  /* Resizers */
  .wl-resize{position:absolute;right:0;top:0;height:100%;width:6px;cursor:col-resize;opacity:.25}
  .wl-resize:hover{opacity:.55}
  `;

  // style inject
  const ensureStyle=(id,txt)=>{const s=document.getElementById(id)||Object.assign(document.createElement("style"),{id});s.textContent=txt;if(!s.parentNode)document.head.appendChild(s);};
  ensureStyle("watchlist-styles", css);
  ensureStyle("watchlist-refresh-css", `.wl-refresh-btn{margin-left:auto;display:inline-flex;align-items:center;justify-content:center;width:32px;height:32px;border-radius:9999px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);cursor:pointer;transition:background .15s,opacity .15s}.wl-refresh-btn:hover{background:rgba(255,255,255,.10)}.wl-refresh-btn.loading{opacity:.6;pointer-events:none}.wl-refresh-btn .material-symbol{font-size:18px;line-height:1;color:#fff;-webkit-text-fill-color:#fff;font-variation-settings:'FILL' 1,'wght' 500,'GRAD' 0,'opsz' 24;display:inline-block;will-change:transform}.wl-refresh-btn.spin .material-symbol,.wl-refresh-btn.loading .material-symbol,.wl-refresh-btn[disabled] .material-symbol{animation:wlrot .5s linear infinite!important}@keyframes wlrot{to{transform:rotate(360deg)}}`);
  ensureStyle("watchlist-title-css", `.wl-table td.title{white-space:normal;text-transform:none!important;letter-spacing:normal!important;font:inherit;color:inherit;-webkit-text-fill-color:currentColor}.wl-table td.title a{color:inherit;text-decoration:none;font:inherit;-webkit-text-fill-color:currentColor}.wl-table td.title a:visited{color:inherit}`);

  /* ========= layout ========= */
  const host=document.getElementById("page-watchlist"); if(!host) return;
  const readPrefs=()=>{try{return JSON.parse(localStorage.getItem("wl.prefs")||"{}")}catch{return{}}};
  const writePrefs=p=>{try{localStorage.setItem("wl.prefs",JSON.stringify(p))}catch{}};
  const prefs=Object.assign({posterMin:150,view:"posters",released:"both",overlays:"yes",genre:"",sortKey:"title",sortDir:"asc",moreOpen:false,cols:{}},readPrefs());
  host.innerHTML=`
    <div class="title" style="display:flex;align-items:center;justify-content:space-between;gap:12px"><span>Watchlist</span></div>
    <div class="wl-wrap" id="watchlist-root">
      <div>
        <div class="wl-controls">
          <label class="wl-chip wl-selectall"><input id="wl-select-all" type="checkbox"><span>Select all</span></label>
          <span id="wl-count" class="wl-muted">0 selected</span>
        </div>

        <div id="wl-posters" class="wl-grid" style="display:none"></div>

        <div id="wl-list" class="wl-table-wrap" style="display:none">
          <table class="wl-table">
            <colgroup><col class="c-sel"><col class="c-title"><col class="c-rel"><col class="c-genre"><col class="c-type"><col class="c-sync"><col class="c-poster"></colgroup>
            <thead><tr>
              <th style="text-align:center"><input id="wl-list-select-all" type="checkbox"></th>
              <th class="sortable" data-sort="title" data-col="title" style="position:relative">Title<span class="wl-resize"></span></th>
              <th class="sortable" data-sort="release" data-col="rel" style="position:relative">Release<span class="wl-resize"></span></th>
              <th class="sortable" data-sort="genre" data-col="genre" style="position:relative">Genre<span class="wl-resize"></span></th>
              <th class="sortable" data-sort="type" data-col="type" style="position:relative">Type<span class="wl-resize"></span></th>
              <th class="sortable" data-sort="sync" data-col="sync" style="position:relative">Sync<span class="wl-resize"></span></th>
              <th class="sortable" data-sort="poster" data-col="poster" style="position:relative">Poster<span class="wl-resize"></span></th>
            </tr></thead>
            <tbody id="wl-tbody"></tbody>
          </table>
        </div>

        <div id="wl-empty" class="wl-empty wl-muted" style="display:none">No items</div>
      </div>

      <aside class="wl-side">
        <div class="ins-card">
          <div class="ins-row wl-ref-row" style="align-items:center">
            <div class="ins-icon"><span class="material-symbol">tune</span></div>
            <div class="ins-title" style="margin-right:auto">Filters</div>
            <button id="wl-refresh" class="wl-refresh-btn" title="Refresh watchlist" aria-label="Refresh watchlist"><span class="material-symbol">refresh</span></button>
          </div>
          <div class="ins-row"><div class="ins-kv" style="width:100%">
            <label>View</label>
            <select id="wl-view" class="wl-input" style="width:auto;padding:6px 10px"><option value="posters">Posters</option><option value="list">List</option></select>

            <label>Search</label>
            <input id="wl-q" class="wl-input" placeholder="Search title...">

            <label>Type</label>
            <select id="wl-type" class="wl-input"><option value="">All types</option><option value="movie">Movies</option><option value="tv">Shows</option></select>

            <label>Provider</label>
            <select id="wl-provider" class="wl-input">
              <option value="">All</option>
              <option value="PLEX">PLEX</option>
              <option value="SIMKL">SIMKL</option>
              <option value="TRAKT">TRAKT</option>
              <option value="JELLYFIN">JELLYFIN</option>
              <option value="EMBY">EMBY</option>
              <option value="MDBLIST">MDBLIST</option>
            </select>

            <label id="wl-size-label">Size</label>
            <input id="wl-size" type="range" min="120" max="320" step="10" class="wl-input" style="padding:0">
          </div></div>

          <div class="ins-row" id="wl-more-panel" style="display:none"><div class="ins-kv" style="width:100%">
            <label>Released</label>
            <select id="wl-released" class="wl-input"><option value="both">Both</option><option value="released">Yes</option><option value="unreleased">No</option></select>

            <label id="wl-overlays-label">Show overlays</label>
            <select id="wl-overlays" class="wl-input"><option value="yes">Yes</option><option value="no">No</option></select>

            <label>Genre</label>
            <select id="wl-genre" class="wl-input"><option value="">All</option></select>
          </div></div>

          <div class="ins-row" style="justify-content:flex-end;gap:8px">
            <button id="wl-more" class="wl-btn" aria-expanded="false">More...</button>
            <button id="wl-clear" class="wl-btn">Reset</button>
          </div>
        </div>

        <div class="ins-card">
          <div class="ins-row"><div class="ins-icon"><span class="material-symbol">flash_on</span></div><div class="ins-title">Actions</div></div>
          <div class="ins-row"><div class="ins-kv" style="width:100%">
            <label>Delete</label>
            <div class="wl-actions" style="display:flex;gap:10px">
              <select id="wl-delete-provider" class="wl-input" style="flex:1">
                <option value="ALL">ALL (default)</option>
                <option value="PLEX">PLEX</option>
                <option value="SIMKL">SIMKL</option>
                <option value="TRAKT">TRAKT</option>
                <option value="JELLYFIN">JELLYFIN</option>
                <option value="EMBY">EMBY</option>
                <option value="MDBLIST">MDBLIST</option>
              </select>
              <button id="wl-delete" class="wl-btn danger" disabled>Delete</button>
            </div>

            <label>Visibility</label>
            <div class="wl-actions" style="display:flex;gap:10px"><button id="wl-hide" class="wl-btn" disabled>Hide (local)</button><button id="wl-unhide" class="wl-btn">Unhide all</button></div>
          </div></div>
        </div>

        <div class="ins-card">
          <div class="ins-row"><div class="ins-icon"><span class="material-symbol">insights</span></div><div class="ins-title">List Insight</div></div>
          <div class="ins-row"><div id="wl-metrics" class="ins-metrics" style="width:100%"></div></div>
        </div>
      </aside>
    </div>

    <div id="wl-snack" class="wl-snack wl-hidden" role="status" aria-live="polite"></div>
    <div id="wl-detail" class="wl-detail" aria-live="polite"></div>
    <div id="wl-trailer" class="wl-modal" aria-modal="true" role="dialog"><div class="box"><button class="x" id="wl-trailer-close" title="Close"><span class="material-symbol">close</span></button></div></div>
  `;

  /* ========= refs ========= */
  const $ = id => document.getElementById(id);

  const postersEl   = $("wl-posters");
  const listWrapEl  = $("wl-list");
  const listBodyEl  = $("wl-tbody");
  const listSelectAll = $("wl-list-select-all");
  const empty       = $("wl-empty");
  const selAll      = $("wl-select-all");
  const selCount    = $("wl-count");
  const qEl         = $("wl-q");
  const tEl         = $("wl-type");
  const providerSel = $("wl-provider");
  const sizeInput   = $("wl-size");
  const sizeLabel   = $("wl-size-label");
  const delProv     = $("wl-delete-provider");
  const clearBtn    = $("wl-clear");
  const viewSel     = $("wl-view");
  const snack       = $("wl-snack");
  const metricsEl   = $("wl-metrics");
  const detailEl    = $("wl-detail");
  const sideEl      = document.querySelector(".wl-side");
  const moreBtn     = $("wl-more");
  const morePanel   = $("wl-more-panel");
  const releasedSel = $("wl-released");
  const overlaysSel = $("wl-overlays");
  const overlaysLabel = $("wl-overlays-label");
  const genreSel    = $("wl-genre");
  const trailerModal= $("wl-trailer");
  const trailerClose= $("wl-trailer-close");

  /* ========= column widths ========= */
  const colSel = { title: ".c-title", rel: ".c-rel", genre: ".c-genre", type: ".c-type", sync: ".c-sync", poster: ".c-poster" };
  const minPx  = { title: 120, rel: 90, genre: 140, type: 70, sync: 140, poster: 48 };

  function applyCols(init=false){
    const cg=document.querySelector(".wl-table colgroup"); if(!cg) return;
    prefs.cols=prefs.cols||{};
    for(const [k,sel] of Object.entries(colSel)){
      const col=cg.querySelector(sel); if(!col) continue;
      let w=prefs.cols[k];
      if(!w && init){
        const th=document.querySelector(`.wl-table thead th[data-col="${k}"]`);
        const base=(th?getComputedStyle(th).width:getComputedStyle(col).width)||"";
        prefs.cols[k]=w=`${parseInt(base,10)||minPx[k]}px`; writePrefs(prefs);
      }
      if(w) col.style.width=w;
    }
  }

  /* ========= column resizing ========= */
  function attachResizers() {
    const cg = document.querySelector(".wl-table colgroup");
    if (!cg) return;

    const getCol = k => cg.querySelector(colSel[k]);
    const px = el => parseInt((el?.style.width || getComputedStyle(el).width), 10) || 0;
    const selColW = () => (document.querySelector(".wl-table col.c-sel") ? px(document.querySelector(".wl-table col.c-sel")) : 44);

    document.querySelectorAll(".wl-table thead th[data-col]").forEach(th => {
      const k = th.dataset.col, h = th.querySelector(".wl-resize"), c = getCol(k);
      if (!h || !c) return;

      const onDown = e => {
        e.preventDefault(); e.stopPropagation();
        const startX = e.clientX, base = px(c);

        const sumOther = () =>
          selColW() + Object.keys(colSel).reduce((s, kk) => kk === k ? s : s + (px(getCol(kk)) || minPx[kk]), 0);

        const maxW = () => Math.max(minPx[k], (listWrapEl.clientWidth - 2) - sumOther());

        const onMove = ev => {
          const w = Math.min(maxW(), Math.max(minPx[k], base + (ev.clientX - startX)));
          c.style.width = w + "px";
        };
        const onUp = () => {
          prefs.cols[k] = c.style.width; writePrefs(prefs);
          window.removeEventListener("pointermove", onMove, true);
          window.removeEventListener("pointerup", onUp, true);
          window.removeEventListener("pointercancel", onUp, true);
        };

        window.addEventListener("pointermove", onMove, true);
        window.addEventListener("pointerup", onUp, true);
        window.addEventListener("pointercancel", onUp, true);
      };

      const onDbl = e => {
        e.stopPropagation();
        delete (prefs.cols || {})[k];
        writePrefs(prefs);
        applyCols(true);
      };

      h.addEventListener("pointerdown", onDown, true);
      h.addEventListener("dblclick", onDbl, true);
    });
  }

  applyCols(true);
  attachResizers();

  /* ======== data & state ======== */
  let [items, filtered] = [[], []];
  const selected = new Set();
  const hiddenSet = (() => { try { return new Set(JSON.parse(localStorage.getItem("wl.hidden") || "[]")); } catch { return new Set(); } })();
  const saveHidden = () => { try { localStorage.setItem("wl.hidden", JSON.stringify([...hiddenSet])); } catch {} };
  const hideBtn = document.getElementById("wl-hide"), unhideBtn = document.getElementById("wl-unhide");

  let viewMode = prefs.view === "list" ? "list" : "posters";
  let sortKey = prefs.sortKey || "title", sortDir = prefs.sortDir || "asc";

  const metaCache = new Map();
  const derivedCache = new Map();
  let activeProviders = new Set();

  let TMDB_OK = true;

  /* ========= utils ========= */
  const esc = s => String(s).replace(/[&<>"]/g, m => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;" }[m]));
  const toLocale = () => navigator.language || "en-US";
  const cmp = (a, b) => a < b ? -1 : a > b ? 1 : 0;
  const cmpDir = v => (sortDir === "asc" ? v : -v);
  const normKey = it => it.key || it.guid || it.id || (it.ids?.imdb && `imdb:${it.ids.imdb}`) || (it.ids?.tmdb && `tmdb:${it.ids.tmdb}`) || (it.ids?.tvdb && `tvdb:${it.ids.tvdb}`) || "";
  const artUrl=(it,size)=>(!TMDB_OK||!(it?.tmdb||it?.ids?.tmdb))?"":`/art/tmdb/${(((it?.type||it?.media_type||"")+"").toLowerCase()==="movie"?"movie":"tv")}/${encodeURIComponent(String(it?.tmdb||it?.ids?.tmdb))}?size=${encodeURIComponent(size||"w342")}`;
  const parseReleaseDate = s => { if (typeof s !== "string" || !(s = s.trim())) return null; let y, m, d; if (/^\d{4}-\d{2}-\d{2}$/.test(s)) ([y, m, d] = s.split("-").map(Number)); else if (/^\d{2}-\d{2}-\d{4}$/.test(s)) { const a = s.split("-").map(Number); d = a[0]; m = a[1]; y = a[2]; } else return null; const t = Date.UTC(y, (m || 1) - 1, d || 1), dt = new Date(t); return Number.isFinite(dt.getTime()) ? dt : null; };
  const fmtDateSmart = (raw, loc) => { const dt = parseReleaseDate(raw); if (!dt) return ""; try { return new Intl.DateTimeFormat(loc || toLocale(), { day:"2-digit", month:"2-digit", year:"numeric", timeZone:"UTC" }).format(dt); } catch { return ""; } };
  const providersOf = it => Array.isArray(it.sources) ? it.sources.map(s => String(s).toUpperCase()) : [];
  const metaKey = it => `${(String(it.type || "").toLowerCase() === "movie" ? "movie" : "tv")}:${it.tmdb || it.ids?.tmdb || ""}`;
  const getReleaseIso = it => {
    const tv = /^(tv|show)$/i.test(String(it.type || ""));
    let iso = tv ? (it.first_air_date || it.firstAired || it.aired) : (it.release_date || it.released);
    iso = iso || it?.release?.date || "";
    if (!iso) {
      const m = metaCache.get(metaKey(it)) || {};
      iso = tv ? (m.detail?.first_air_date || m.release?.date || m.first_air_date || "")
              : (m.detail?.release_date || m.release?.date || "");
    }
    return typeof iso === "string" ? iso.trim() : "";
  };

  /* ========= hide/show snackbar ========= */
  hideBtn?.addEventListener("click", () => {
    if (!selected.size) return;
    selected.forEach(k => hiddenSet.add(k));
    saveHidden(); selected.clear(); applyFilters(); updateSelCount(); snackbar("Hidden locally");
  }, true);

  unhideBtn?.addEventListener("click", () => {
    hiddenSet.clear(); saveHidden(); applyFilters(); updateSelCount(); snackbar("All unhidden");
  }, true);

  /* ========= hydrate rows (list view) ========= */
  const _hydrating = new Set();
  const setText = (el, t) => {
    if (!el) return;
    const next = (t || "").trim();
    if (!next) return;
    el.textContent = next;
    el.title = next;
  };

  async function hydrateRow(it, tr){
    const k=normKey(it); if(_hydrating.has(k)) return; _hydrating.add(k);
    try{
      const canTMDB = (typeof TMDB_OK==="undefined") ? true : !!TMDB_OK;
      const movie = /^movie$/i.test(it.type||"");
      const m = canTMDB ? (await getMetaFor(it)) : null;

      const isoMeta = m ? (movie ? (m.detail?.release_date||m.release?.date||"") : (m.detail?.first_air_date||m.release?.date||"")) : "";
      const gs = m ? (Array.isArray(m.genres||m.detail?.genres) ? (m.genres||m.detail?.genres) : []) : [];
      const genresMeta = gs.map(g=>typeof g==="string"?g:(g?.name||g?.title||"")).filter(Boolean).slice(0,3).join(", ");

      const prev = derivedCache.get(k)||{};
      const isoBase = (prev.iso||getReleaseIso(it)||"").trim();
      const iso = (isoMeta||isoBase).trim();

      const genresBase = (prev.genresText||extractGenres(it).slice(0,3).join(", ")).trim();
      const genresText = (genresMeta||genresBase).trim();

      const relFmtBase = (prev.relFmt||fmtDateSmart(isoBase,toLocale())||"").trim();
      const relFmt = (fmtDateSmart(iso,toLocale())||relFmtBase).trim();

      derivedCache.set(k,{iso,relFmt,genresText});
      if(tr?.isConnected){ setText(tr.querySelector("td.rel"),relFmt); setText(tr.querySelector("td.genre"),genresText); }
    } finally { _hydrating.delete(k); }
  }

  /* ========= API ========= */
  const fetchWatchlist = async () => {
    const r = await fetch("/api/watchlist?limit=5000", { cache: "no-store" });
    if (!r.ok) throw new Error("watchlist fetch failed");
    const j = await r.json();
    TMDB_OK = !Boolean(j?.missing_tmdb_key);
    return Array.isArray(j?.items) ? j.items : [];
  };

  const fetchConfig = async () => {
    try { const r = await fetch("/api/config", { cache: "no-store" }); return r.ok ? await r.json() : {}; }
    catch { return {}; }
  };

  const getMetaFor = async it => {
    if (!TMDB_OK) return null;
    const k = metaKey(it), hit = metaCache.get(k);
    if (hit) return hit;
    const tmdb = String(it.tmdb || it.ids?.tmdb || "");
    if (!tmdb) return null;

    try {
      const r = await fetch("/api/metadata/bulk?overview=full", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: [{ type: k.startsWith("movie") ? "movie" : "show", tmdb }],
          need: { overview:1, tagline:1, runtime_minutes:1, poster:1, ids:1, videos:1, genres:1, certification:1, score:1, release:1, backdrop:1 },
          concurrency: 1
        })
      });
      const first = Object.values((await r.json())?.results || {})[0];
      const meta = first?.ok ? (first.meta || null) : null;
      if (meta) metaCache.set(k, meta);
      return meta;
    } catch { return null; }
  };

  /* ========= genre index ========= */
  const extractGenres = it => {
    const meta = metaCache.get(metaKey(it));
    const srcs = [it.genres, it.genre, it.detail?.genres, it.meta?.genres, it.meta?.detail?.genres, meta?.genres, meta?.detail?.genres].filter(Boolean);
    return srcs.flatMap(s => Array.isArray(s)
      ? s.map(g => typeof g === "string" ? g : (g?.name || g?.title || g?.slug || ""))
      : String(s).split(/[|,\/]/)
    ).map(v => String(v || "").trim()).filter(Boolean);
  };

  function getDerived(it){
    const k = normKey(it);
    let d = derivedCache.get(k);
    if (d) return d;

    const iso = getReleaseIso(it);
    const relFmt = fmtDateSmart(iso, toLocale());
    const genresText = extractGenres(it).slice(0,3).join(", ");

    d = { iso, relFmt, genresText };
    derivedCache.set(k, d);
    return d;
  }

  const buildGenreIndex = list => {
    const m = new Map();
    for (const g of list.flatMap(extractGenres)) { const k = g.toLowerCase(); if (!m.has(k)) m.set(k, g); }
    return [...m.values()].sort((a, b) => a.localeCompare(b));
  };

  const populateGenreOptions = genres => {
    const mk = (v, l = v) => Object.assign(document.createElement("option"), { value: v, textContent: l });
    genreSel.replaceChildren(mk("", "All"), ...genres.map(g => mk(g)));
    genreSel.value = prefs.genre || "";
  };

  /* ========= providers & metrics ========= */
  const SRC_LOGOS = {
    PLEX:"/assets/img/PLEX.svg",
    SIMKL:"/assets/img/SIMKL.svg",
    TRAKT:"/assets/img/TRAKT.svg",
    JELLYFIN:"/assets/img/JELLYFIN.svg",
    EMBY:"/assets/img/EMBY.svg",
    MDBLIST:"/assets/img/MDBLIST.svg",
    CROSSWATCH:"/assets/img/CROSSWATCH.svg"
  };
  const providerChip = (name, ok) => {
    const src = SRC_LOGOS[name], icon = ok ? "check_circle" : "cancel", cls = ok ? "ok" : "miss";
    return `<span class="wl-mat ${cls}" title="${name}${ok ? " present" : " missing"}">${src ? `<img src="${src}" alt="${name}">` : `<span class="wl-badge">${name}</span>`}<span class="material-symbol">${icon}</span></span>`;
  };
  let providerActive = (p, have) => providerChip(p, have); // replaced after config load

  const mapProvidersByKey = list => new Map(list.map(it => [normKey(it), new Set(providersOf(it))]).filter(([k]) => !!k));

  function updateMetrics() {
    const ICON  = { PLEX:"movie_filter", SIMKL:"playlist_add", TRAKT:"featured_play_list", JELLYFIN:"bookmark_added", EMBY:"library_add", MDBLIST:"grading", CROSSWATCH: "save" };
    const ORDER = ["PLEX","SIMKL","TRAKT","MDBLIST","JELLYFIN","EMBY","CROSSWATCH"];
    const counts = ORDER.reduce((acc, p) => (acc[p] = filtered.reduce((n, it) => n + (providersOf(it).includes(p) ? 1 : 0), 0), acc), {});
    metricsEl.innerHTML = ORDER.filter(p => activeProviders.has(p)).map(p =>
      `<div class="metric" data-w="${p}"><span class="material-symbol">${ICON[p]}</span><div><div class="m-val">${counts[p]}</div><div class="m-lbl">${p}</div></div></div>`
    ).join("");
  }

  /* ========= sorting ========= */
  const _t = it => String(it.title || "").toLowerCase();
  const _type = it => ((it.type || "").toLowerCase() === "show" ? "tv" : String(it.type || "").toLowerCase());

  function sortFilteredForList(arr) {
    const byTitle = (a, b) => cmp(_t(a), _t(b));
    const sorters = {
      title: (a, b) => cmpDir(byTitle(a, b)),
      type: (a, b) => cmpDir(cmp(_type(a), _type(b))),
      release: (a, b) => {
        const unk = sortDir === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
        const ta = parseReleaseDate(getReleaseIso(a)), tb = parseReleaseDate(getReleaseIso(b));
        const va = ta ? ta.getTime() : unk, vb = tb ? tb.getTime() : unk;
        const diff = va - vb || byTitle(a, b);
        return cmpDir(diff);
      },
      genre: (a, b) => {
        const ga = (extractGenres(a)[0] || "").toLowerCase();
        const gb = (extractGenres(b)[0] || "").toLowerCase();
        const sentinel = "\uFFFF"; /* patched */
        const va = ga || (sortDir === "asc" ? sentinel : "");
        const vb = gb || (sortDir === "asc" ? sentinel : "");
        const diff = cmp(va, vb) || byTitle(a, b);
        return cmpDir(diff);
      },
      sync: (a, b) => {
        const ca = providersOf(a).length, cb = providersOf(b).length;
        return cmpDir((ca === cb ? cmp(String(a.title || ""), String(b.title || "")) : (ca - cb)));
      },
      poster: (a, b) => {
        const pa = !!artUrl(a, "w92"), pb = !!artUrl(b, "w92");
        return cmpDir((pa === pb ? cmp(String(a.title || ""), String(b.title || "")) : (pa ? 1 : -1)));
      }
    };
    const fn = sorters[sortKey] || (() => 0);
    return arr.slice().sort(fn);
  }

  function updateSortHeaderUI() {
    document.querySelectorAll(".wl-table th.sortable").forEach(th => {
      th.classList.toggle("sort-asc", th.dataset.sort === sortKey && sortDir === "asc");
      th.classList.toggle("sort-desc", th.dataset.sort === sortKey && sortDir === "desc");
    });
  }

  function setSort(k) {
    sortKey === k ? (sortDir = sortDir === "asc" ? "desc" : "asc") : (sortKey = k, sortDir = "asc");
    prefs.sortKey = sortKey; prefs.sortDir = sortDir; writePrefs(prefs);
    render(); updateSortHeaderUI();
  }

  function wireSortableHeaders() {
    document.querySelectorAll(".wl-table th.sortable").forEach(th =>
      th.addEventListener("click", e => { if (!e.target.closest(".wl-resize")) setSort(th.dataset.sort); }, true)
    );
    updateSortHeaderUI();
  }

  /* ========= filters ========= */
  const applyOverlayPrefUI = () => {
    postersEl.classList.toggle("wl-hide-overlays", prefs.overlays === "no");
    const show = viewMode === "posters";
    [overlaysLabel, overlaysSel].forEach(el => el.style.display = show ? "" : "none");
  };

  const normReleased = v => (v === "yes" ? "released" : v === "no" ? "unreleased" : "both");

  function applyFilters() {
    const q = (qEl.value || "").toLowerCase().trim();
    const ty = (tEl.value || "").trim(); // "", "movie", "tv"
    const provider = (providerSel.value || "").toUpperCase();
    const releasedPref = normReleased(releasedSel?.value || prefs.released || "both");
    const genrePref = (genreSel?.value || prefs.genre || "").trim().toLowerCase();
    const todayUTC = Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate());

    filtered = items.filter(it => {
      const key = normKey(it);
      if (hiddenSet.has(key) && !document.getElementById("wl-show-hidden")) return false;

      const title = String(it.title || "").toLowerCase();
      const t = ((it.type || "").toLowerCase() === "show") ? "tv" : (it.type || "").toLowerCase();

      if (q && !title.includes(q)) return false;
      if (ty && t !== ty) return false;
      if (provider && !providersOf(it).includes(provider)) return false;

      if (releasedPref !== "both") {
        const dt = parseReleaseDate(getReleaseIso(it));
        const isRel = !!dt && dt.getTime() <= todayUTC;
        if ((releasedPref === "released" && !isRel) || (releasedPref === "unreleased" && isRel)) return false;
      }

      if (genrePref && !extractGenres(it).some(g => String(g).toLowerCase() === genrePref)) return false;
      return true;
    });

    render();
    updateMetrics();
  }

  /* ========= trailer & detail ========= */
  function pickTrailer(meta) {
    const flat = [meta?.videos, meta?.videos?.results, meta?.detail?.videos, meta?.detail?.videos?.results].flatMap(v => Array.isArray(v) ? v : []);
    const scored = flat.map(v => {
      const site0 = String(v.site || v.host || "").toLowerCase();
      const site = /youtube/.test(site0) ? "youtube" : /vimeo/.test(site0) ? "vimeo" : site0;
      const type = String(v.type || "").toLowerCase();
      const rank = (type.includes("trailer") ? 100 : type.includes("teaser") ? 60 : type.includes("clip") ? 40 : 10)
                + (v.official ? 30 : 0) + (site === "youtube" ? 5 : 0) + (v.published_at || v.created_at ? 1 : 0);
      return { site, key: v.key || v.id || "", name: v.name || "Trailer", rank };
    }).filter(v => v.site && v.key);
    const v = scored.sort((a,b)=>b.rank-a.rank)[0];
    if (!v) return null;
    if (v.site === "youtube") return { url: `https://www.youtube-nocookie.com/embed/${encodeURIComponent(v.key)}?autoplay=1&rel=0&modestbranding=1&playsinline=1`, title: v.name };
    if (v.site === "vimeo")   return { url: `https://player.vimeo.com/video/${encodeURIComponent(v.key)}?autoplay=1`, title: v.name };
    return null;
  }

  function openTrailerWithUrl(url, title="Trailer") {
    const box = trailerModal.querySelector(".box");
    box.querySelector("iframe")?.remove();
    const ifr = document.createElement("iframe");
    Object.assign(ifr, { title, src: url, loading: "lazy" });
    ifr.setAttribute("allow", "autoplay; fullscreen; encrypted-media; picture-in-picture");
    ifr.setAttribute("referrerpolicy", "strict-origin-when-cross-origin");
    box.appendChild(ifr);
    trailerModal.classList.add("show");
    trailerClose?.focus();
  }
  function closeTrailer() {
    trailerModal.classList.remove("show");
    const ifr = trailerModal.querySelector("iframe");
    if (ifr) { try { ifr.src = "about:blank"; } catch {} ifr.remove(); }
  }
  trailerClose?.addEventListener("click", e => (e.preventDefault(), closeTrailer()), true);
  document.addEventListener("keydown", e => { if (e.key === "Escape" && trailerModal?.classList.contains("show")) closeTrailer(); }, true);
  trailerModal?.addEventListener("click", e => { if (e.target === trailerModal) closeTrailer(); }, true);

  function createScoreSVG(score0to100) {
    const v = Math.max(0, Math.min(100, Number(score0to100) || 0));
    const r = 26, c = 2 * Math.PI * r, off = c * (1 - v / 100);
    return `<svg viewBox="0 0 60 60" class="score" aria-label="User score ${v}%">
      <circle cx="30" cy="30" r="${r}" fill="none" stroke="rgba(255,255,255,.12)" stroke-width="6"/>
      <circle cx="30" cy="30" r="${r}" fill="none" stroke="currentColor" stroke-width="6" stroke-linecap="round" stroke-dasharray="${c.toFixed(2)}" stroke-dashoffset="${off.toFixed(2)}"/>
      <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" font-size="14" font-weight="700" fill="#fff">${v}%</text>
    </svg>`;
  }

  function renderDetail(it, meta) {
    const isMovie = String(it.type || "").toLowerCase() === "movie";
    const poster = artUrl(it, "w154") || "/assets/img/placeholder_poster.svg";
    const year = it.year || meta?.year ? `<span class="year">${it.year || meta?.year}</span>` : "";
    const runtime = (() => { const m = meta?.runtime_minutes|0; if (!m) return ""; const h = (m/60)|0, mm = m%60; return h ? `${h}h ${mm?mm+'m':''}` : `${mm}m`; })();
    const genresText = (Array.isArray(meta?.genres) ? meta.genres : Array.isArray(it?.genres) ? it.genres : []).slice(0,3).join(", ");
    const relIso = isMovie ? (meta?.detail?.release_date || meta?.release?.date || it?.release_date) : (meta?.detail?.first_air_date || it?.first_air_date);
    const metaLine = [isMovie ? "Movie" : "TV", runtime, fmtDateSmart(relIso, toLocale()), meta?.certification || meta?.release?.cert || meta?.detail?.certification, genresText]
      .filter(Boolean).map((p,i)=> i? `<span class="dot">&bull;</span><span class="chip">${esc(p)}</span>` : `<span class="chip">${esc(p)}</span>`).join("");

    const score100 = Number.isFinite(meta?.score) ? Math.round(meta.score) : (Number.isFinite(meta?.vote_average) ? Math.round(meta.vote_average*10) : null);
    const scoreCls = score100 == null ? "" : score100 >= 70 ? "good" : score100 >= 40 ? "mid" : "bad";
    const scoreHtml = score100 != null ? `<div style="text-align:center">${createScoreSVG(score100).replace('<svg', `<svg class="score ${scoreCls}"`)}<span class="score-label">User Score</span></div>` : "";

    const srcs = providersOf(it).map(s => SRC_LOGOS[s] ? `<span class="wl-src" title="${s}"><img src="${SRC_LOGOS[s]}" alt="${s} logo" style="height:16px"></span>` : `<span class="wl-badge">${s}</span>`).join("");
    const hasTrailer = !!pickTrailer(meta);
    const overview = meta?.overview ? `<div class="overview" id="wl-overview">${esc(meta.overview)}</div>` : `<div class="overview wl-muted">No description available</div>`;

    detailEl.innerHTML = `
      <div class="inner" style="position:relative;z-index:1;max-width:unset;margin:0 auto;padding:10px 14px 12px;display:grid;grid-template-columns:80px 1fr 96px;gap:12px;align-items:start">
        <div><img class="poster" src="${poster}" alt="" style="width:76px;border-radius:12px;box-shadow:0 8px 24px rgba(0,0,0,.6)" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'" /></div>
        <div>
          <div style="display:flex;align-items:center;gap:10px">
            <div class="title" style="font-weight:800;font-size:18px;flex:1">${esc(it.title || meta?.title || "Unknown")} ${year}</div>
            <button class="wl-btn" id="wl-detail-close" title="Close"><span class="material-symbol">close</span></button>
          </div>
          <div class="meta" style="display:flex;flex-wrap:wrap;gap:8px;opacity:.95;margin-top:2px">${metaLine}</div>
          ${overview}
        </div>
        <div class="actions" style="display:flex;flex-direction:column;align-items:center;gap:6px;align-self:start;justify-self:end">
          ${scoreHtml || ""}
          <button class="wl-btn" id="wl-play-trailer" ${hasTrailer ? "" : "data-fallback=1"}>Watch Trailer</button>
          <div class="wl-srcs" style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-top:6px">${srcs}</div>
        </div>
      </div>`;
    detailEl.classList.add("show");

    document.getElementById("wl-detail-close")?.addEventListener("click", () => detailEl.classList.remove("show"), true);
    document.getElementById("wl-play-trailer")?.addEventListener("click", () => {
      const pick = pickTrailer(meta);
      if (pick) openTrailerWithUrl(pick.url, pick.title);
      else window.open(`https://www.youtube.com/results?search_query=${encodeURIComponent(`${it?.title || meta?.title || ""} ${(it?.year || meta?.year || "")} trailer`.trim())}`,"_blank","noopener,noreferrer");
    }, true);
  }

  /* preview & detail */
  let activePreviewKey = null;
  function forceHideDetail(){ if(!detailEl) return; detailEl.classList.remove("show"); activePreviewKey=null; }
  function showPreview(it){ const k=normKey(it); activePreviewKey=k; getMetaFor(it).then(m=>{ if(activePreviewKey===k) renderDetail(it,m||{}); }); }
  function hidePreview(it){ const k=normKey(it); if(!selected.has(k)&&activePreviewKey===k){ detailEl.classList.remove("show"); activePreviewKey=null; } }

  /* ========= render ========= */
  const _show = (el, on) => el && (el.style.display = on ? "" : "none");

  function render() {
    const posters = viewMode === "posters";
    _show(postersEl, posters); _show(listWrapEl, !posters); _show(sizeInput, posters); _show(sizeLabel, posters);
    applyOverlayPrefUI();

    if (!filtered.length) {
      empty.style.display = ""; selAll.checked = false; listSelectAll.checked = false;
      postersEl.innerHTML = ""; listBodyEl.innerHTML = ""; selCount.textContent = "0 selected"; metricsEl.innerHTML = ""; return;
    }

    empty.style.display = "none";
    posters ? renderPosters() : renderList();
    selCount.textContent = `${selected.size} selected`;
  }

  function renderPosters(){
    postersEl.replaceChildren();
    const frag=document.createDocumentFragment();
    const canTMDB=(typeof TMDB_OK==="undefined")?true:!!TMDB_OK;

    filtered.forEach((it,i)=>{
      const key=normKey(it);
      const imgUrl=canTMDB ? artUrl(it,"w342") : "";
      const src=imgUrl || "/assets/img/placeholder_poster.svg";
      const card=document.createElement("div");
      card.className=`wl-card ${selected.has(key)?"selected":""}`;

      const provHtml=providersOf(it).map(p=>`<span class="wl-tag">${p}</span>`).join("");
      const eager=i<24?`loading="eager" fetchpriority="high"`:`loading="lazy"`;
      card.innerHTML=`<div class="wl-tags">${provHtml}</div><img ${eager} decoding="async" src="${src}" alt="" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"/>`;

      card.addEventListener("click",()=>{
        selected.has(key)?selected.delete(key):selected.add(key);
        card.classList.toggle("selected"); updateSelCount();
        if(canTMDB) getMetaFor(it).then(m=>renderDetail(it,m||{})); else renderDetail(it,{});
      },true);

      card.addEventListener("mouseenter",()=>{ if(canTMDB) showPreview(it); },true);
      card.addEventListener("mouseleave",()=>hidePreview(it),true);

      frag.appendChild(card);
    });

    postersEl.appendChild(frag);
  }

  function renderList() {
    listBodyEl.replaceChildren();
    const frag = document.createDocumentFragment();
    const rows = sortFilteredForList(filtered);

    rows.forEach(it => {
      const key = normKey(it), tr = document.createElement("tr");
      const t = ((it.type || "").toLowerCase() === "show") ? "tv" : (it.type || "").toLowerCase();
      const typeLabel = t === "movie" ? "Movie" : "Show";
      const thumb = artUrl(it, "w92") || "/assets/img/placeholder_poster.svg";
      const p = providersOf(it);
      const have = {
        PLEX:p.includes("PLEX"),
        SIMKL:p.includes("SIMKL"),
        TRAKT:p.includes("TRAKT"),
        JELLYFIN:p.includes("JELLYFIN"),
        EMBY:p.includes("EMBY"),
        MDBLIST:p.includes("MDBLIST")
      };
      const matrix = `<div class="wl-matrix">${providerActive("PLEX",have.PLEX)}${providerActive("SIMKL",have.SIMKL)}${providerActive("TRAKT",have.TRAKT)}${providerActive("MDBLIST",have.MDBLIST)}${providerActive("JELLYFIN",have.JELLYFIN)}${providerActive("EMBY",have.EMBY)}</div>`;
      const d = getDerived(it);

      tr.innerHTML = `
        <td style="text-align:center"><input type="checkbox" data-k="${key}" ${selected.has(key) ? "checked" : ""}></td>
        <td class="title"><div>${esc(it.title || "")}</div></td>
        <td class="rel">${esc(d.relFmt)}</td>
        <td class="genre" title="${esc(d.genresText)}">${esc(d.genresText)}</td>
        <td>${esc(typeLabel)}</td>
        <td>${matrix}</td>
        <td><img class="wl-mini" src="${thumb}" alt="" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"/></td>
      `;

      if (!d.relFmt || !d.genresText) setTimeout(() => hydrateRow(it, tr), 0);

      tr.querySelector('input[type=checkbox]')?.addEventListener("change", e => { e.target.checked ? selected.add(key) : selected.delete(key); updateSelCount(); }, true);
      tr.addEventListener("click", ev => { if (!ev.target.closest("input")) getMetaFor(it).then(m => renderDetail(it, m || {})); }, true);
      tr.addEventListener("mouseenter", () => showPreview(it), true);
      tr.addEventListener("mouseleave", () => hidePreview(it), true);

      const relEmpty = !tr.querySelector(".rel")?.textContent?.trim();
      const genreEmpty = !tr.querySelector(".genre")?.textContent?.trim();
      if (relEmpty || genreEmpty) setTimeout(() => hydrateRow(it, tr), 0);

      frag.appendChild(tr);
    });

    listBodyEl.appendChild(frag);
    listSelectAll.checked = rows.length > 0 && rows.every(x => selected.has(normKey(x)));
    updateSortHeaderUI();
  }

  /* ========= snackbar & selection/delete ========= */
  let snackTimer = null;
  function snackbar(html){
    clearTimeout(snackTimer); snackTimer = null;
    snack.textContent = ""; snack.innerHTML = html;
    snack.classList.remove("wl-hidden");
    snackTimer = setTimeout(() => (snack.classList.add("wl-hidden"), snackTimer = null), 1800);
  }

  function rebuildDeleteProviderOptions(){
    const byKey = mapProvidersByKey(items), union = new Set(), prev = delProv.value;
    for (const k of selected) byKey.get(k)?.forEach?.(p => union.add(p));
    const ALL = ["PLEX","SIMKL","TRAKT","MDBLIST","JELLYFIN","EMBY"];
    delProv.innerHTML = `<option value="ALL">ALL (default)</option>${ALL.filter(p=>union.has(p)).map(p=>`<option value="${p}">${p}</option>`).join("")}`;
    if ([...delProv.options].some(o => o.value === prev)) delProv.value = prev;
  }

  function updateSelCount(){
    selCount.textContent = `${selected.size} selected`;
    rebuildDeleteProviderOptions();
    document.getElementById("wl-delete").disabled = !(delProv.value && selected.size);
    document.getElementById("wl-hide").disabled = selected.size === 0;
  }

  async function postDelete(keys, provider){
    const send = async prov => {
      const r = await fetch("/api/watchlist/delete", {
        method:"POST", headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ keys, provider: prov })
      });
      const txt = await r.text(); let j=null; try{ j = txt ? JSON.parse(txt) : null }catch{}
      const okCount =
        typeof j?.deleted_ok === "number" ? j.deleted_ok :
        Array.isArray(j?.results) ? j.results.filter(x=>x && (x.ok===true || x.status==="ok")).length :
        (r.ok ? keys.length : 0);
      return { okCount, ok: r.ok || okCount>0 };
    };
    const p = (provider||"ALL");
    let res = await send(p.toUpperCase());
    if (!res.ok) res = await send(p.toLowerCase());
    return res;
  }

  const delBtn = document.getElementById("wl-delete");
  delBtn?.addEventListener("click", async () => {
    forceHideDetail();
    if (!selected.size) return snackbar("Nothing selected");
    const provider = (delProv?.value || "ALL");
    const PROV_UP = provider.toUpperCase();
    const keys = [...selected];
    const total = keys.length, CHUNK = 50;

    delBtn.disabled = delProv.disabled = true;
    const progress = d => { snack.innerHTML = `Deleting <b>${d}/${total}</b> ${PROV_UP==="ALL"?"across providers":"from "+PROV_UP}...`; snack.classList.remove("wl-hidden"); };
    progress(0);

    let done = 0, ok = 0;
    for (let i = 0; i < keys.length; i += CHUNK) {
      const res = await postDelete(keys.slice(i, i + CHUNK), provider);
      ok += res.okCount || 0; done = Math.min(total, i + CHUNK); progress(done);
    }
    snack.classList.add("wl-hidden");
    delBtn.disabled = delProv.disabled = false;

    const byProv = mapProvidersByKey(items);
    for (const k of keys){
      const s = byProv.get(k) || new Set();
      PROV_UP==="ALL" ? s.clear() : s.delete(PROV_UP);
      if (!s.size){ const idx = items.findIndex(it => normKey(it) === k); if (idx > -1) items.splice(idx,1); }
    }
    selected.clear(); applyFilters(); updateSelCount();
    forceHideDetail();
    hardReloadWatchlist().catch(()=>{});

    snackbar(ok>0 ? (PROV_UP==="ALL" ? `Deleted on available providers for ${ok}/${total}` : `Deleted ${ok}/${total} on ${PROV_UP}`) : "Delete completed with no visible changes");
  }, true);

  /* ========= events & refresh/init ========= */
  const on = (els, evts, fn, cap=true) => evts.forEach(e => els.forEach(el => el?.addEventListener(e, fn, cap)));
  const setPosterMin = px => postersEl.style.setProperty("--wl-min", `${px}px`);

  ["pointerenter","pointerdown","focusin","mouseenter","touchstart"].forEach(ev =>
    sideEl?.addEventListener(ev, forceHideDetail, true)
  );

  qEl.addEventListener("input", applyFilters, true);
  on([tEl, providerSel], ["change","input"], applyFilters);

  moreBtn.addEventListener("click", () => {
    const open = morePanel.style.display !== "none";
    morePanel.style.display = open ? "none" : "";
    prefs.moreOpen = !open; writePrefs(prefs);
  }, true);

  on([releasedSel], ["change","input"], () => { prefs.released = normReleased(releasedSel.value); writePrefs(prefs); applyFilters(); });
  on([overlaysSel], ["change","input"], () => { prefs.overlays = overlaysSel.value || "yes"; writePrefs(prefs); applyOverlayPrefUI(); });
  on([genreSel], ["change","input"], () => { prefs.genre = genreSel.value || ""; writePrefs(prefs); applyFilters(); });

  const selectAll = chk => { selected.clear(); if (chk.checked) filtered.forEach(it => { const k = normKey(it); if (k) selected.add(k); }); };
  selAll.addEventListener("change", () => { selectAll(selAll); (viewMode === "posters" ? renderPosters : renderList)(); updateSelCount(); }, true);
  listSelectAll.addEventListener("change", () => { selectAll(listSelectAll); renderList(); selAll.checked = listSelectAll.checked; updateSelCount(); }, true);

  clearBtn.addEventListener("click", () => {
    qEl.value = ""; tEl.value = ""; providerSel.value = "";
    releasedSel.value = "both"; overlaysSel.value = "yes"; genreSel.value = "";
    Object.assign(prefs, { released:"both", overlays:"yes", genre:"" }); writePrefs(prefs);
    applyOverlayPrefUI(); applyFilters();
  }, true);

  delProv.addEventListener("change", updateSelCount, true);

  sizeInput.addEventListener("input", () => {
    const px = Math.max(120, Math.min(320, Number(sizeInput.value) || 150));
    setPosterMin(px); prefs.posterMin = px; writePrefs(prefs);
  }, true);

  document.addEventListener("keydown", e => {
    if (e.key === "Delete" && !document.getElementById("wl-delete").disabled) document.getElementById("wl-delete").click();
    if (e.key === "Escape") trailerModal.classList.contains("show") ? closeTrailer() : detailEl.classList.remove("show");
  }, true);

  viewSel.addEventListener("change", () => { viewMode = viewSel.value === "list" ? "list" : "posters"; prefs.view = viewMode; writePrefs(prefs); render(); });

  /* ========= refresh ========= */
  async function hardReloadWatchlist(){
    try{ items=await fetchWatchlist(); populateGenreOptions(buildGenreIndex(items)); applyFilters(); rebuildDeleteProviderOptions(); }
    catch(e){ console.warn("watchlist reload failed:", e); }
  }
  function _wlBusy(on){ const b=document.getElementById("wl-refresh"); if(!b)return; b.disabled=!!on; b.classList.toggle("loading",!!on); b.classList.toggle("spin",!!on); }
  document.getElementById("wl-refresh")?.addEventListener("click", async()=>{ if(hardReloadWatchlist._busy)return; hardReloadWatchlist._busy=true; _wlBusy(true); try{ await hardReloadWatchlist(); } finally{ _wlBusy(false); hardReloadWatchlist._busy=false; } }, {passive:true});
  window.Watchlist=Object.assign(window.Watchlist||{}, { refresh: hardReloadWatchlist });
  window.addEventListener("watchlist:refresh", hardReloadWatchlist);

  /* ========= init ========= */
  (async function init(){
    viewSel.value = viewMode;
    sizeInput.value = String(prefs.posterMin); setPosterMin(prefs.posterMin);
    releasedSel.value = prefs.released; overlaysSel.value = prefs.overlays; morePanel.style.display = prefs.moreOpen ? "" : "none";

    const cfg = await fetchConfig();
    const active = new Set();
    if (cfg?.plex?.account_token) active.add("PLEX");
    if (cfg?.simkl?.access_token) active.add("SIMKL");
    if (cfg?.trakt?.access_token) active.add("TRAKT");
    if (cfg?.jellyfin?.access_token) active.add("JELLYFIN");
    if (cfg?.emby?.access_token || cfg?.emby?.api_key || cfg?.emby?.token) active.add("EMBY");
    if (cfg?.mdblist?.api_key) active.add("MDBLIST");

    activeProviders = active;
    providerActive = (p, have) => (activeProviders.has(p) ? providerChip(p, have) : "");

    items = await fetchWatchlist();
    populateGenreOptions(buildGenreIndex(items));
    applyOverlayPrefUI(); applyFilters(); rebuildDeleteProviderOptions(); wireSortableHeaders();

    window.dispatchEvent(new CustomEvent("watchlist-ready"));
  })();
})(); // close outer IIFE
