(function(){
  // ---------- styles ----------
  const css = `
  .wl-wrap{display:grid;grid-template-columns:minmax(0,1fr) 360px;gap:16px}
  .wl-controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
  .wl-input{background:#15151c;border:1px solid rgba(255,255,255,.12);border-radius:8px;padding:8px 10px;color:#fff;width:100%}
  .wl-btn{background:#1d1d26;border:1px solid rgba(255,255,255,.15);border-radius:8px;color:#fff;padding:8px 10px;cursor:pointer}
  .wl-btn.danger{background:#2a1113;border-color:#57252a}
  .wl-chip{display:inline-flex;align-items:center;gap:6px;border-radius:16px;padding:6px 10px;background:#171720;border:1px solid rgba(255,255,255,.1);white-space:nowrap}
  .wl-muted{opacity:.7}
  .wl-row{display:flex;gap:10px}
  .wl-actions{display:flex;gap:10px}
  .wl-empty{padding:24px;border:1px dashed rgba(255,255,255,.12);border-radius:12px;text-align:center}

  /* Posters view */
  .wl-grid{--wl-min:150px;display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(var(--wl-min),1fr));min-height:240px}
  .wl-card{position:relative;border-radius:12px;overflow:hidden;background:#0f0f13;border:1px solid rgba(255,255,255,.08);transition:box-shadow .15s ease,border-color .15s ease;aspect-ratio:2/3}
  .wl-card img{width:100%;height:100%;object-fit:cover;display:block}
  .wl-card .wl-tags{position:absolute;left:8px;top:8px;display:flex;gap:6px;flex-wrap:wrap;z-index:2}
  .wl-tag{font-size:11px;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.35);backdrop-filter:blur(4px)}
  .wl-card.selected{box-shadow:0 0 0 3px #6f6cff,0 0 0 5px rgba(111,108,255,.35)}

  /* List view */
  .wl-table-wrap{border:1px solid rgba(255,255,255,.12);border-radius:10px;overflow:auto}
  .wl-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed}
  .wl-table col.c-sel{width:44px}
  .wl-table col.c-title{width:20%}        /* was 28% → minder lucht naast Title */
  .wl-table col.c-type{width:84px}        /* was 90px */
  .wl-table col.c-sync{width:200px}       /* was 220px */
  .wl-table col.c-poster{width:56px}      /* was 60px */
  .wl-table th,.wl-table td{padding:6px 8px;border-bottom:1px solid rgba(255,255,255,.08);white-space:nowrap;text-align:left}
  .wl-table th{position:sticky;top:0;background:#101018;font-weight:600;z-index:1;text-transform:none;letter-spacing:0}
  .wl-table tr:last-child td{border-bottom:none}
  .wl-table .title{white-space:normal}
  .wl-table input[type=checkbox]{width:18px;height:18px}

  .wl-table col.c-rel{width:110px}     /* Release date (was 120px) */
  .wl-table col.c-genre{width:220px}   /* Genre list (was 240px) */
  .wl-table td.genre{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden;white-space:normal;line-height:1.2}
  /* Clamp long genre text to 2 lines
  .wl-table td.genre{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden;white-space:normal;line-height:1.2}

  /* Submeta line under title in list view */
  .wl-submeta{font-size:12px;opacity:.75;margin-top:4px;white-space:normal}

  /* Sortable headers */
  .wl-table th.sortable{cursor:pointer;user-select:none}
  .wl-table th.sortable::after{content:"";margin-left:6px;opacity:.6}
  .wl-table th.sort-asc::after{content:"▲"}
  .wl-table th.sort-desc::after{content:"▼"}

  /* Poster column */
  .wl-table{width:100%;table-layout:fixed;border-collapse:separate;border-spacing:0}
  .wl-table col.c-poster{width:56px}                 /* a bit narrower = sits left */
  .wl-table th.poster,.wl-table td.poster{padding-left:0;padding-right:4px;text-align:right}
  .wl-table td.poster .wl-mini{width:48px;height:72px;display:block;margin-left:auto;object-fit:cover;border-radius:6px}

  /* Release + Genre cells should wrap */
  .wl-table td.rel,
  .wl-table td.genre{
    white-space:normal;       /* allow wrapping */
    overflow:hidden;
    text-overflow:ellipsis;
  }

  /* Sources */
  .wl-srcs{display:flex;gap:10px;align-items:center}
  .wl-src{display:inline-flex;align-items:center;justify-content:center;height:20px}
  .wl-src img{height:16px;display:block;opacity:.95}
  .wl-badge{padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.12);font-size:11px}

  /* Sync matrix */
  .wl-matrix{display:flex;gap:10px;align-items:center}
  .wl-mat{display:flex;align-items:center;gap:6px;padding:4px 6px;border:1px solid rgba(255,255,255,.12);border-radius:8px;background:#14141c}
  .wl-mat img{height:14px}
  .wl-mat .material-symbol{font-size:16px}
  .wl-mat.ok{border-color:rgba(120,255,180,.35)}
  .wl-mat.miss{opacity:.6}

  .wl-mini{width:36px;height:54px;border-radius:4px;object-fit:cover;background:#0f0f13;border:1px solid rgba(255,255,255,.08)}

  /* Sidebar cards (compact spacing) */
  .wl-side{display:flex;flex-direction:column;gap:6px}
  .ins-card{background:linear-gradient(180deg,rgba(20,20,28,.95),rgba(16,16,24,.95));border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:10px 12px}
  .ins-row{display:flex;align-items:center;gap:12px;padding:8px 6px;border-top:1px solid rgba(255,255,255,.06)}
  .ins-row:first-child{border-top:none;padding-top:2px}
  .ins-icon{width:32px;height:32px;border-radius:10px;display:flex;align-items:center;justify-content:center;background:#13131b;border:1px solid rgba(255,255,255,.06)}
  .ins-title{font-weight:700}
  .ins-kv{display:grid;grid-template-columns:110px 1fr;gap:10px;align-items:center}
  .ins-kv label{opacity:.85}

  .ins-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}
  .metric{position:relative;overflow:hidden;display:flex;align-items:center;gap:8px;background:#12121a;border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:10px}
  .metric .material-symbol{font-size:18px;opacity:.9}
  .metric .m-val{font-weight:700}
  .metric .m-lbl{font-size:12px;opacity:.75}
  .metric::after{
    content:"";position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
    width:min(75%,120px);aspect-ratio:1/1;opacity:.10;background-repeat:no-repeat;background-position:center;background-size:contain;pointer-events:none;
  }
  .metric[data-w="PLEX"]::after{background-image:url('/assets/PLEX.svg')}
  .metric[data-w="SIMKL"]::after{background-image:url('/assets/SIMKL.svg')}
  .metric[data-w="TRAKT"]::after{background-image:url('/assets/TRAKT.svg')}
  .metric[data-w="JELLYFIN"]::after{background-image:url('/assets/JELLYFIN.svg')}

  .wl-snack{position:fixed;left:50%;transform:translateX(-50%);bottom:20px;background:#1a1a22;border:1px solid rgba(255,255,255,.15);border-radius:10px;padding:10px 12px;display:flex;gap:10px;align-items:center;z-index:9999}
  .wl-snack .wl-btn{padding:6px 10px}

  /* Busy button state */
  .wl-btn.is-busy{ position:relative; pointer-events:none; opacity:.85 }
  .wl-btn.is-busy::after{
    content:""; position:absolute; top:50%; left:50%; width:14px; height:14px; margin:-7px 0 0 -7px;
    border-radius:50%; border:2px solid currentColor; border-top-color:transparent;
    animation: wl-spin .9s linear infinite;
  }
  .wl-btn.is-busy::before{
    content:""; position:absolute; left:0; bottom:0; height:2px;
    width:var(--p,0%); background:currentColor; opacity:.7; transition:width .2s ease;
  }
  @keyframes wl-spin { to { transform: rotate(360deg); } }

  /* Hide poster overlays when requested */
  .wl-hide-overlays .wl-tags{ display:none !important; }

  /* --- Detail bar: compact, centered, and clear of the right sidebar --- */
  .wl-detail{
    position:fixed;
    left:50%;
    bottom:12px;
    /* Keep it narrow and never collide with the filters/actions panel */
    width:min(640px, calc(100vw - 420px));
    transform:translate(-50%, calc(100% + 12px));
    opacity:1;

    /* Own rounded box; no edge-to-edge look */
    border:1px solid rgba(255,255,255,.12);
    border-bottom:none;
    border-radius:14px 14px 0 0;
    overflow:hidden;

    background:transparent; /* Backdrop sits on ::before */
    box-shadow:0 18px 48px rgba(0,0,0,.55);
    transition:transform .35s cubic-bezier(.2,.8,.2,1), opacity .35s;
    z-index:10000;
  }
  .wl-detail.show{ transform:translate(-50%, 0); }

  .wl-detail::before{
    content:"";
    position:absolute; inset:0; z-index:0;
    background-image: var(--wl-bg, none); /* Set via JS */
    background-size:cover; background-position:center;
    filter: blur(22px) saturate(130%) brightness(0.8);
    transform: scale(1.06);
  }
  .wl-detail::after{
    content:""; position:absolute; inset:0; z-index:0; pointer-events:none;
    /* Subtle scrim and vignette; keeps text readable on any artwork */
    background:
      radial-gradient(120% 90% at 50% 110%, rgba(0,0,0,.65) 40%, rgba(0,0,0,.9) 100%),
      linear-gradient(180deg, rgba(0,0,0,.15) 0%, rgba(0,0,0,.45) 100%),
      linear-gradient(180deg, rgba(var(--wl-tint,20,28,44), .22), rgba(0,0,0,.25));
  }

  /* Tight three-column layout (Poster • Content • Actions) */
  .wl-detail .inner{
    position:relative; z-index:1;
    max-width:unset;
    margin:0 auto;
    padding:10px 14px 12px;
    display:grid;
    grid-template-columns:80px 1fr 96px;
    gap:12px;
    align-items:start;
  }

  /* Left column (poster) */
  .wl-detail .poster{
    width:76px; height:auto;
    border-radius:12px;
    box-shadow:0 8px 24px rgba(0,0,0,.6);
  }

  /* Title row with close button on the right */
  .wl-detail .title-row{ display:flex; align-items:center; gap:10px; }
  .wl-detail .title{ font-weight:800; font-size:18px; display:flex; gap:8px; align-items:baseline; flex:1 }
  .wl-detail .year{ opacity:.8; font-weight:600 }
  .wl-detail .close{
    margin-left:auto;
    background:rgba(0,0,0,.35);
    border:1px solid rgba(255,255,255,.18);
    border-radius:999px; padding:4px 8px;
    cursor:pointer;
  }

  /* Compact meta line */
  .wl-detail .meta{ display:flex; flex-wrap:wrap; gap:8px; opacity:.95; margin-top:2px }
  .wl-detail .meta .chip{ font-size:12px; padding:2px 8px; border-radius:999px; background:rgba(0,0,0,.35); border:1px solid rgba(255,255,255,.16) }
  .wl-detail .meta .dot{ opacity:.7; padding:0 2px }

  /* Text blocks */
  .wl-detail .tagline{ font-style:italic; opacity:.95; margin:4px 0 6px; }
  .wl-detail .overview{
    display:-webkit-box; -webkit-box-orient:vertical; overflow:hidden;
    -webkit-line-clamp:3; line-clamp:3;
    line-height:1.25; max-height:4.6em;
  }
  .wl-detail .overview.expanded{ -webkit-line-clamp:unset; line-clamp:unset; max-height:none; }
  .wl-detail .more{
    margin-top:6px; padding:4px 10px; font-size:12px;
    background:rgba(0,0,0,.35); border:1px solid rgba(255,255,255,.16);
    border-radius:999px;
  }
  .wl-detail .wl-srcs img{ height:16px }
  .wl-detail .release-chip{
    font-size:12px; padding:2px 8px; border-radius:999px;
    background:rgba(255,255,255,.08); border:1px solid rgba(255,255,255,.12);
    margin-left:6px;
  }

  /* Right column (actions): score at the very top, trailer link below */
  .wl-detail .actions{
    display:flex; flex-direction:column; align-items:center; gap:6px;
    align-self:start; justify-self:end; padding-top:0; opacity:.98;
  }
  .wl-detail .actions .score{ width:56px; height:56px; display:block; }
  .wl-detail .actions .score-label{ display:block; text-align:center; font-size:12px; opacity:.85; font-weight:700; margin-top:4px; }
  .wl-detail .actions .score.good{ color:#2ecc71 }
  .wl-detail .actions .score.mid { color:#f0ad4e }
  .wl-detail .actions .score.bad { color:#e74c3c }

  /* Small “Watch Trailer” button under the score */
  .wl-detail .actions #wl-play-trailer{
    font-size:12px; line-height:1.1;
    padding:4px 10px; margin-top:2px;
  }

  /* Keep the standalone score style consistent (legacy selector) */
  .wl-detail .score{ width:56px; height:56px; display:inline-block }

  /* Provider badges under the trailer button in the Actions column */
  .wl-detail .actions .wl-srcs{
    display:flex;
    gap:8px;
    justify-content:center;
    align-items:center;
    flex-wrap:wrap;
    margin-top:6px;
  }
  .wl-detail .actions .wl-src img{ height:14px; } /* a bit smaller to fit the narrow column */

  /* Trailer modal (hidden by default) */
  .wl-modal{
    position:fixed;
    inset:0;
    display:none;               /* hide when not .show */
    align-items:center;
    justify-content:center;
    background:rgba(0,0,0,.6);
    z-index:10050;
  }
  .wl-modal.show{ display:flex; }

  .wl-modal .box{
    position:relative;
    width:min(90vw, 960px);
    aspect-ratio:16/9;
    background:#000;
    border:1px solid rgba(255,255,255,.12);
    border-radius:12px;
    overflow:hidden;
    box-shadow:0 10px 40px rgba(0,0,0,.6);
  }
  .wl-modal .box iframe{ width:100%; height:100%; display:block; }
  .wl-modal .box .x{ position:absolute; top:8px; right:8px; }

  /* Make the artwork pop more */
  .wl-detail::before{
    /* softer blur and a touch more brightness/contrast */
    filter: blur(14px) saturate(120%) brightness(1.05) contrast(1.05);
  }

  /* Use a lighter overlay when we actually have a backdrop */
  .wl-detail.has-bg::after{
    background:
      radial-gradient(120% 90% at 50% 110%, rgba(0,0,0,.35) 35%, rgba(0,0,0,.6) 100%),
      linear-gradient(180deg, rgba(0,0,0,.08) 0%, rgba(0,0,0,.22) 100%),
      linear-gradient(180deg, rgba(var(--wl-tint,20,28,44), .18), rgba(0,0,0,.18));
  }

  /* Keep a stronger scrim only when there is no backdrop (readability) */
  .wl-detail:not(.has-bg)::after{
    background:
      radial-gradient(120% 90% at 50% 110%, rgba(0,0,0,.55) 40%, rgba(0,0,0,.85) 100%),
      linear-gradient(180deg, rgba(0,0,0,.12) 0%, rgba(0,0,0,.35) 100%),
      linear-gradient(180deg, rgba(var(--wl-tint,20,28,44), .22), rgba(0,0,0,.25));
  }

  /* Auto-hide helper */
  .wl-hidden{display:none !important}
  `;
  const st = document.createElement("style"); st.id = "watchlist-styles"; st.textContent = css;
  document.head.appendChild(st);

  // ---------- elements ----------
  const host = document.getElementById("page-watchlist");
  if (!host) return;

  host.innerHTML = `
    <div class="title" style="display:flex;align-items:center;justify-content:space-between;gap:12px">
      <span>Watchlist</span>
    </div>

    <div class="wl-wrap" id="watchlist-root">
      <div>
        <div class="wl-controls">
          <label class="wl-chip wl-selectall"><input id="wl-select-all" type="checkbox"> <span>Select all</span></label>
          <span id="wl-count" class="wl-muted">0 selected</span>
        </div>

        <div id="wl-posters" class="wl-grid" style="display:none"></div>
        <div id="wl-list" class="wl-table-wrap" style="display:none">
          <table class="wl-table">
            <colgroup>
              <col class="c-sel">
              <col class="c-title">
              <col class="c-rel">
              <col class="c-genre">
              <col class="c-type">
              <col class="c-sync">
              <col class="c-poster">
            </colgroup>
            <thead>
              <tr>
                <th style="text-align:center"><input id="wl-list-select-all" type="checkbox"></th>
                <th class="sortable" data-sort="title">Title</th>
                <th class="sortable" data-sort="release">Release</th>
                <th class="sortable" data-sort="genre">Genre</th>
                <th class="sortable" data-sort="type">Type</th>
                <th class="sortable" data-sort="sync">Sync</th>
                <th class="sortable" data-sort="poster">Poster</th>
              </tr>
            </thead>
            <tbody id="wl-tbody"></tbody>
          </table>
        </div>

        <div id="wl-empty" class="wl-empty wl-muted" style="display:none">No items</div>
      </div>

      <aside class="wl-side">
        <!-- Filters -->
        <div class="ins-card">
          <div class="ins-row">
            <div class="ins-icon"><span class="material-symbol">tune</span></div>
            <div class="ins-title">Filters</div>
          </div>
          <div class="ins-row">
            <div class="ins-kv" style="width:100%">
              <label>View</label>
              <select id="wl-view" class="wl-input" style="width:auto;padding:6px 10px">
                <option value="posters" selected>Posters</option>
                <option value="list">List</option>
              </select>

              <label>Search</label>
              <input id="wl-q" class="wl-input" placeholder="Search title…">

              <label>Type</label>
              <select id="wl-type" class="wl-input">
                <option value="">All types</option>
                <option value="movie">Movies</option>
                <option value="tv">Shows</option>
              </select>

              <label>Provider</label>
              <select id="wl-provider" class="wl-input">
                <option value="">All</option>
                <option value="PLEX">PLEX</option>
                <option value="SIMKL">SIMKL</option>
                <option value="TRAKT">TRAKT</option>
                <option value="JELLYFIN">JELLYFIN</option>
              </select>

              <!-- Posters-only control -->
              <label id="wl-size-label">Size</label>
              <input id="wl-size" type="range" min="120" max="320" step="10" class="wl-input" style="padding:0 0" />
            </div>
          </div>

          <!-- Expanded filter panel (hidden by default) -->
          <div class="ins-row" id="wl-more-panel" style="display:none">
            <div class="ins-kv" style="width:100%">
              <label>Released</label>
              <select id="wl-released" class="wl-input">
                <option value="both" selected>Both</option>
                <option value="released">Yes</option>
                <option value="unreleased">No</option>
              </select>


              <label id="wl-overlays-label">Show overlays</label>
              <select id="wl-overlays" class="wl-input">
                <option value="yes" selected>Yes</option>
                <option value="no">No</option>
              </select>

              <label>Genre</label>
              <select id="wl-genre" class="wl-input">
                <option value="">All</option>
                <!-- options populated dynamically -->
              </select>
            </div>
          </div>

          <!-- Footer actions with "More…" before Reset -->
          <div class="ins-row" style="justify-content:flex-end; gap:8px">
            <button id="wl-more" class="wl-btn" aria-expanded="false">More…</button>
            <button id="wl-clear" class="wl-btn">Reset</button>
          </div>
        </div>

        <!-- Actions -->
        <div class="ins-card">
          <div class="ins-row">
            <div class="ins-icon"><span class="material-symbol">flash_on</span></div>
            <div class="ins-title">Actions</div>
          </div>
          <div class="ins-row">
            <div class="ins-kv" style="width:100%">
              <label>Delete</label>
              <div class="wl-actions">
                <select id="wl-delete-provider" class="wl-input" style="flex:1">
                  <option value="ALL" selected>ALL (default)</option>
                  <option value="PLEX">PLEX</option>
                  <option value="SIMKL">SIMKL</option>
                  <option value="TRAKT">TRAKT</option>
                  <option value="JELLYFIN">JELLYFIN</option>
                </select>
                <button id="wl-delete" class="wl-btn danger" disabled>Delete</button>
              </div>

              <label>Visibility</label>
              <div class="wl-actions">
                <button id="wl-hide" class="wl-btn" disabled>Hide (local)</button>
                <button id="wl-unhide" class="wl-btn">Unhide all</button>
              </div>
            </div>
          </div>
        </div>

        <!-- List Insight -->
        <div class="ins-card">
          <div class="ins-row">
            <div class="ins-icon"><span class="material-symbol">insights</span></div>
            <div class="ins-title">List Insight</div>
          </div>
          <div class="ins-row">
            <div id="wl-metrics" class="ins-metrics" style="width:100%"></div>
          </div>
        </div>
      </aside>
    </div>

    <div id="wl-snack" class="wl-snack wl-hidden" role="status" aria-live="polite"></div>
    <div id="wl-detail" class="wl-detail" aria-live="polite"></div>
    <div id="wl-trailer" class="wl-modal" aria-modal="true" role="dialog">
      <div class="box">
        <button class="x" id="wl-trailer-close" title="Close"><span class="material-symbol">close</span></button>
        <!-- iframe injected on open -->
      </div>
    </div>
  `;

  // ---------- refs ----------
  const trailerModal = document.getElementById("wl-trailer");
  const trailerClose = document.getElementById("wl-trailer-close");

  const postersEl = document.getElementById("wl-posters");
  const listWrapEl = document.getElementById("wl-list");
  const listBodyEl = document.getElementById("wl-tbody");
  const listSelectAll = document.getElementById("wl-list-select-all");
  const empty = document.getElementById("wl-empty");
  const selAll = document.getElementById("wl-select-all");
  const selCount = document.getElementById("wl-count");

  const qEl = document.getElementById("wl-q");
  const tEl = document.getElementById("wl-type");
  const providerSel = document.getElementById("wl-provider");
  const sizeInput = document.getElementById("wl-size");
  const sizeLabel = document.getElementById("wl-size-label");
  const delProv = document.getElementById("wl-delete-provider");
  const clearBtn = document.getElementById("wl-clear");
  const hideBtn = document.getElementById("wl-hide");
  const unhideBtn = document.getElementById("wl-unhide");
  const viewSel = document.getElementById("wl-view");
  const snack = document.getElementById("wl-snack");
  const metricsEl = document.getElementById("wl-metrics");
  const detailEl = document.getElementById("wl-detail");

  // More… filters
  const moreBtn = document.getElementById("wl-more");
  const morePanel = document.getElementById("wl-more-panel");
  const releasedSel = document.getElementById("wl-released");
  const overlaysSel = document.getElementById("wl-overlays");
  const overlaysLabel = document.getElementById("wl-overlays-label");
  const genreSel = document.getElementById("wl-genre");

  // Sort headers
  const sortableHeaders = () => Array.from(document.querySelectorAll('.wl-table th.sortable'));

  // --- trailer modal wiring ---
  trailerClose?.addEventListener("click", (e)=>{
    e.preventDefault();
    e.stopPropagation();
    closeTrailer();
  });
  document.addEventListener("keydown", (e)=>{
    if (e.key === "Escape" && trailerModal?.classList.contains("show")) closeTrailer();
  }, true);
  trailerModal?.addEventListener("click", (e)=>{
    if (e.target === trailerModal) closeTrailer();
  }, true);

  // ---------- state ----------
  let items = [];
  let filtered = [];
  const selected = new Set();
  const hiddenSet = loadHidden();
  let viewMode = "posters"; // "posters" | "list"
  let snackTimer = null;    // auto-hide timer

  // Detail bar state
  const metaCache = new Map();   // key -> meta
  let previewTimer = null;       // debounce hover
  let activePreviewKey = null;   // currently rendered key
  let pinnedKey = null;          // stick detail until closed

  // Sorting state (list view)
  let sortKey = "title"; // 'title' | 'type' | 'sync' | 'poster'
  let sortDir = "asc";   // 'asc' | 'desc'

  // ---------- prefs ----------
  function loadPrefs(){ try { return JSON.parse(localStorage.getItem("wl.prefs")||"{}"); } catch { return {}; } }
  function savePrefs(){ try { localStorage.setItem("wl.prefs", JSON.stringify(prefs)); } catch {} }
  const prefs = loadPrefs();

  // Defaults (non-destructive)
  if (typeof prefs.posterMin !== "number") prefs.posterMin = 150;
  if (!prefs.view) prefs.view = "posters";
  if (!prefs.released) prefs.released = "both";           // 'both' | 'released' | 'unreleased'
  if (!prefs.overlays) prefs.overlays = "yes";            // 'yes' | 'no' (posters-only)
  if (typeof prefs.genre !== "string") prefs.genre = "";  // '' = all
  if (!prefs.sortKey) prefs.sortKey = "title";
  if (!prefs.sortDir) prefs.sortDir = "asc";
  if (typeof prefs.moreOpen !== "boolean") prefs.moreOpen = false;

  // ---------- utils ----------
  function loadHidden(){ try { return new Set(JSON.parse(localStorage.getItem("wl.hidden")||"[]")); } catch { return new Set(); } }
  function persistHidden(){ try { localStorage.setItem("wl.hidden", JSON.stringify([...hiddenSet])); } catch {} }

  // artwork URL helper (TMDb-based)
  const artUrl = (it, size) => {
    const tmdb = it?.tmdb || it?.ids?.tmdb;
    if (!tmdb) return "";
    const raw = String(it?.type || it?.media_type || "").toLowerCase();
    const typ = (raw === "movie") ? "movie" : "tv";    // default everything else to TV
    const id  = encodeURIComponent(String(tmdb).trim());
    return `/art/tmdb/${typ}/${id}?size=${encodeURIComponent(size || "w342")}`;
  };

  const normKey = (it) =>
    it.key || it.guid || it.id ||
    (it.ids?.imdb && `imdb:${it.ids.imdb}`) ||
    (it.ids?.tmdb && `tmdb:${it.ids.tmdb}`) ||
    (it.ids?.tvdb && `tvdb:${it.ids.tvdb}`) || "";

  const metaKey = (it) => {
    const typ = String(it.type||"").toLowerCase() === "tv" ? "tv" : "movie";
    const tmdb = it.tmdb || it.ids?.tmdb || "";
    return `${typ}:${tmdb}`;
  };

  const fmtRuntime = (mins) => {
    if (!mins || mins<=0) return "";
    const h = Math.floor(mins/60), m = mins%60;
    return h ? `${h}h ${m?m+'m':''}` : `${m}m`;
  };

  const toLocale = () => (navigator.language || "en-US");
  const fmtDate = (iso, loc) => {
    if (!iso) return "";
    try {
      const d = new Date(iso + "T00:00:00Z");
      return new Intl.DateTimeFormat(loc||toLocale(), { day:"2-digit", month:"2-digit", year:"numeric" }).format(d);
    } catch { return iso; }
  };
  const daysBetweenNow = (iso) => {
    if (!iso) return null;
    try{
      const d = new Date(iso + "T00:00:00Z").getTime();
      const now = Date.now();
      const ms = d - now;
      return Math.round(ms / 86400000); // >0 future, <0 past, 0 today
    }catch{ return null; }
  };

  /* --- Release helpers (single source of truth) --- */
  function getReleaseIso(it){
    const t = String(it.type || "").toLowerCase();
    const isTV = (t === "tv" || t === "show");

    // Primary: direct fields
    let iso =
      (isTV ? (it.first_air_date || it.firstAired || it.aired) : (it.release_date || it.released)) ||
      (it.release?.date) ||
      "";

    // Fallback: consult metadata cache (filled by getMetaFor / previews)
    if (!iso) {
      try {
        const mk = metaKey(it);
        const meta = metaCache.get(mk);
        if (meta) {
          iso = isTV
            ? (meta?.detail?.first_air_date || meta?.release?.date || meta?.first_air_date || "")
            : (meta?.detail?.release_date || meta?.release?.date || "");
        }
      } catch {}
    }
    return (typeof iso === "string" ? iso.trim() : "") || "";
  }

  async function warmGenresForFilter(limit=200){
    // Build a unique list of (type, tmdb) not yet in metaCache
    const wants = [];
    const seen = new Set();
    for (const it of items){
      const k = metaKey(it);
      if (metaCache.has(k)) continue;
      const tmdb = String(it.tmdb || it.ids?.tmdb || "");
      if (!tmdb) continue;
      const typ = k.startsWith("movie") ? "movie" : "tv";
      const sig = `${typ}:${tmdb}`;
      if (seen.has(sig)) continue;
      seen.add(sig);
      wants.push({ type: typ, tmdb });
      if (wants.length >= limit) break;
    }
    if (!wants.length) return 0;

    try{
      const r = await fetch(`/api/metadata/bulk`, {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ items: wants, need: { genres:true, detail:true, videos:true }, concurrency: 2 })
      });
      const j = await r.json();
      const results = j?.results || {};
      let cached = 0;
      for (const key in results){
        const m = results[key]?.ok && results[key]?.meta ? results[key].meta : null;
        if (m){ metaCache.set(key, m); cached++; }
      }
      return cached;
    } catch { return 0; }
  }

  // If Released filter is active but many items have unknown dates,
  let _warmingReleases = false;
  async function warmReleaseDatesForFilter(limit = 60){
    if (_warmingReleases) return;
    _warmingReleases = true;
    try{
      const need = [];
      for (const it of items){
        if (!getReleaseIso(it)) {
          // Only fetch if we can resolve by TMDb
          const tmdb = it.tmdb || it.ids?.tmdb;
          if (tmdb) need.push(it);
          if (need.length >= limit) break;
        }
      }
      if (need.length){
        await Promise.all(need.map(it => getMetaFor(it)));
      }
    } finally {
      _warmingReleases = false;
    }
  }

  // Accepts "YYYY-MM-DD" or "DD-MM-YYYY" and returns a UTC Date or null
  function parseReleaseDate(raw) {
    if (!raw || typeof raw !== "string") return null;
    const s = raw.trim();
    let y, m, d;

    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {            // ISO
      [y, m, d] = s.split("-").map(Number);
    } else if (/^\d{2}-\d{2}-\d{4}$/.test(s)) {     // EU
      const [dd, mm, yy] = s.split("-").map(Number);
      y = yy; m = mm; d = dd;
    } else {
      return null;
    }

    const t = Date.UTC(y, (m || 1) - 1, d || 1);     // normalize to UTC midnight
    const dt = new Date(t);
    return Number.isFinite(dt.getTime()) ? dt : null;
  }

  // Returns true (released), false (future), or null (unknown date)
  function isReleasedNow(it) {
    const dt = parseReleaseDate(getReleaseIso(it));
    if (!dt) return null;
    const todayUTC = new Date();
    const today = Date.UTC(
      todayUTC.getUTCFullYear(),
      todayUTC.getUTCMonth(),
      todayUTC.getUTCDate()
    );
    return dt.getTime() <= today;
  }

  // Format with the robust parser above
  function fmtDateSmart(raw, loc) {
    const dt = parseReleaseDate(raw);
    if (!dt) return "";
    try {
      return new Intl.DateTimeFormat(loc || (navigator.language || "en-US"), {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        timeZone: "UTC"
      }).format(dt);
    } catch {
      return "";
    }
  }

  const providersOf = (it) => (Array.isArray(it.sources) ? it.sources.map(s=>String(s).toUpperCase()) : []);

  const releaseIso = (it) => {
    const t = String(it.type||"").toLowerCase();
    return t === "tv" ? (it.first_air_date || it.release_date || null)
                      : (it.release_date || it.first_air_date || null);
  };

  const fetchWatchlist = async () => {
    const r = await fetch("/api/watchlist?limit=5000", {cache:"no-store"});
    if (!r.ok) throw new Error("watchlist fetch failed");
    const j = await r.json();
    return Array.isArray(j?.items) ? j.items : [];
  };

  const SRC_LOGOS = {
    PLEX: "/assets/PLEX.svg",
    SIMKL: "/assets/SIMKL.svg",
    TRAKT: "/assets/TRAKT.svg",
    JELLYFIN: "/assets/JELLYFIN.svg",
  };

  function mapProvidersByKey(list){
    const m = new Map();
    for (const it of list){
      const k = normKey(it);
      if (!k) continue;
      m.set(k, new Set(providersOf(it)));
    }
    return m;
  }

  // Extract genres from item and cached metadata
  function extractGenres(it){
    const out = [];
    const push = v => { const s = String(v||"").trim(); if (s) out.push(s); };

    const meta = metaCache.get(metaKey(it)); // may be null
    const sources = [
      it.genres,
      it.genre,
      it.detail?.genres,
      it.meta?.genres,
      it.meta?.detail?.genres,
      meta?.genres,
      meta?.detail?.genres
    ];

    for (const src of sources){
      if (!src) continue;
      if (Array.isArray(src)){
        for (const g of src){
          if (typeof g === "string") push(g);
          else if (g && typeof g === "object") push(g.name || g.title || g.slug);
        }
      } else if (typeof src === "string"){
        src.split(/[|,\/]/).forEach(push);
      }
    }
    return out;
  }

  function buildGenreIndex(list){
    const seenLower = new Set();
    const uniq = [];
    for (const it of list){
      for (const g of extractGenres(it)){
        const k = g.toLowerCase();
        if (!seenLower.has(k)){ seenLower.add(k); uniq.push(g); }
      }
    }
    return uniq.sort((a,b)=>a.localeCompare(b));
  }

  function populateGenreOptions(genres){
    while (genreSel.firstChild) genreSel.removeChild(genreSel.firstChild);
    const mk = (val, label) => { const opt = document.createElement("option"); opt.value = val; opt.textContent = label; return opt; };
    genreSel.appendChild(mk("", "All"));
    genres.forEach(g => genreSel.appendChild(mk(g, g)));
    genreSel.value = prefs.genre || "";
  }

  // ---------- provider dropdown, sizing ----------
  function rebuildDeleteProviderOptions(){
    const map = mapProvidersByKey(items);
    const union = new Set();
    for (const k of selected){
      const s = map.get(k);
      if (!s) continue;
      for (const p of s) union.add(p);
    }
    const prev = delProv.value;
    while (delProv.firstChild) delProv.removeChild(delProv.firstChild);
    const mk = (val, label) => { const opt = document.createElement("option"); opt.value = val; opt.textContent = label; return opt; };
    delProv.appendChild(mk("ALL", "ALL (default)"));
    ["PLEX","SIMKL","TRAKT","JELLYFIN"].forEach(p=>{ if (union.has(p)) delProv.appendChild(mk(p, p)); });
    const allowed = new Set([...delProv.options].map(o=>o.value));
    delProv.value = allowed.has(prev) ? prev : "ALL";
  }
  function applyPosterSize(px){ postersEl.style.setProperty("--wl-min", `${px}px`); }

  // Posters overlays toggle (posters-only)
  function applyOverlayPrefUI(){
    const hide = (prefs.overlays === "no");
    postersEl.classList.toggle("wl-hide-overlays", hide);
    // Hide the overlays control itself in list view
    const showOverlaysControl = (viewMode === "posters");
    overlaysLabel.style.display = showOverlaysControl ? "" : "none";
    overlaysSel.style.display = showOverlaysControl ? "" : "none";
  }

  // ---------- filtering & metrics ----------
  function applyFilters(){
    const q = (qEl.value || "").toLowerCase().trim();

    // Normalize type (merge "show" into "tv")
    const ty = (tEl.value || "").trim();                 // "", "movie", "tv"
    const provider = (providerSel.value || "").toUpperCase();

    // "More…" filters
    const rawRel = (releasedSel?.value || prefs.released || "both").toLowerCase();
    const releasedPref = (rawRel === "yes" ? "released" : rawRel === "no" ? "unreleased" : rawRel); // both|released|unreleased
    const genrePref = (genreSel?.value || prefs.genre || "").trim();
    const genrePrefLc = genrePref.toLowerCase();

    // Close any open detail to avoid stale overlay
    pinnedKey = null;
    hideDetail?.();

    let unknownReleaseSeen = false;

    filtered = items.filter(it=>{
      const key = normKey(it);
      if (!document.getElementById("wl-show-hidden")?.checked && hiddenSet.has(key)) return false;

      if (q && !String(it.title||"").toLowerCase().includes(q)) return false;

      const tRaw = String(it.type||"").toLowerCase();
      const normType = (tRaw === "show" ? "tv" : tRaw);
      if (ty && normType !== ty) return false;

      if (provider){
        const have = providersOf(it);
        if (!have.includes(provider)) return false;
      }

      // Released filter
      if (releasedPref !== "both"){
        const iso = getReleaseIso(it);
        if (!iso){
          unknownReleaseSeen = true; // warm later
          return false;
        }
        const rel = isReleasedNow(it); // true | false
        if (releasedPref === "released") return rel === true;
        return rel === false;
      }

      // Genre filter (use enriched sources + case-insensitive)
      if (genrePref){
        const gs = extractGenres(it).map(g => String(g).toLowerCase());
        if (!gs.includes(genrePrefLc)) return false;
      }

      return true;
    });

    render();
    updateMetrics();

    // Warm release dates once if needed
    if (releasedPref !== "both" && unknownReleaseSeen && !applyFilters._rerunPending) {
      applyFilters._rerunPending = true;
      Promise.resolve(warmReleaseDatesForFilter?.()).finally(()=>{
        applyFilters._rerunPending = false;
        const againRaw = (releasedSel?.value || prefs.released || "both").toLowerCase();
        const again = (againRaw === "yes" ? "released" : againRaw === "no" ? "unreleased" : againRaw);
        if (again !== "both") applyFilters();
      });
    }
  }

  // Update the metrics sidebar (counts of filtered by provider)
  function updateMetrics(){
    const counts = {
      PLEX:     filtered.filter(it => providersOf(it).includes("PLEX")).length,
      SIMKL:    filtered.filter(it => providersOf(it).includes("SIMKL")).length,
      TRAKT:    filtered.filter(it => providersOf(it).includes("TRAKT")).length,
      JELLYFIN: filtered.filter(it => providersOf(it).includes("JELLYFIN")).length,
    };
    const ICON = {
      PLEX: "movie_filter",
      SIMKL: "playlist_add",
      TRAKT: "featured_play_list",
      JELLYFIN: "bookmark_added",
    };
    const ORDER = ["PLEX","SIMKL","TRAKT","JELLYFIN"];

    metricsEl.innerHTML = ORDER
      .filter(p => activeProviders.has(p))
      .map(p => metric(ICON[p], p, counts[p], p))
      .join("");
  }

  function metric(icon, label, val, w){
    return `<div class="metric" data-w="${w}">
      <span class="material-symbol">${icon}</span>
      <div>
        <div class="m-val">${val}</div>
        <div class="m-lbl">${label}</div>
      </div>
    </div>`;
  }


  // ---------- sorting (list view only) ----------
  function cmp(a,b){
    return a < b ? -1 : (a > b ? 1 : 0);
  }
  function cmpDir(v){
    return sortDir === "asc" ? v : -v;
  }
  function sortFilteredForList(arr){
    const key = sortKey;
    return arr.slice().sort((a,b)=>{
      if (key === "title"){
        return cmpDir(cmp(String(a.title||"").toLowerCase(), String(b.title||"").toLowerCase()));
      }
      if (key === "type"){
        const ta = (String(a.type||"").toLowerCase()==="show" ? "tv" : String(a.type||"").toLowerCase());
        const tb = (String(b.type||"").toLowerCase()==="show" ? "tv" : String(b.type||"").toLowerCase());
        return cmpDir(cmp(ta, tb));
      }
      if (key === "release"){
        // Unknown dates sorted last
        const unk = sortDir === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
        const ta = parseReleaseDate(getReleaseIso(a));
        const tb = parseReleaseDate(getReleaseIso(b));
        const va = ta ? ta.getTime() : unk;
        const vb = tb ? tb.getTime() : unk;
        const diff = va - vb;
        return cmpDir(diff !== 0 ? diff : cmp(String(a.title||"").toLowerCase(), String(b.title||"").toLowerCase()));
      }
      if (key === "genre"){
        // Sort by first genre (case-insensitive); unknown last
        const ga = (extractGenres(a)[0] || "").toLowerCase();
        const gb = (extractGenres(b)[0] || "").toLowerCase();
        const va = ga || (sortDir === "asc" ? "\uffff" : ""); // push empties last
        const vb = gb || (sortDir === "asc" ? "\uffff" : "");
        const diff = cmp(va, vb);
        return cmpDir(diff !== 0 ? diff : cmp(String(a.title||"").toLowerCase(), String(b.title||"").toLowerCase()));
      }
      if (key === "sync"){
        const ca = providersOf(a).length;
        const cb = providersOf(b).length;
        const v = (ca === cb) ? cmp(String(a.title||""), String(b.title||"")) : (ca - cb);
        return cmpDir(v);
      }
      if (key === "poster"){
        const pa = !!artUrl(a, "w92");
        const pb = !!artUrl(b, "w92");
        const v = (pa === pb) ? cmp(String(a.title||""), String(b.title||"")) : (pa ? 1 : -1);
        return cmpDir(v);
      }
      return 0;
    });
  }
  function updateSortHeaderUI(){
    sortableHeaders().forEach(th=>{
      th.classList.remove("sort-asc","sort-desc");
      if (th.dataset.sort === sortKey){
        th.classList.add(sortDir === "asc" ? "sort-asc" : "sort-desc");
      }
    });
  }
  function setSort(k){
    if (sortKey === k){
      sortDir = (sortDir === "asc" ? "desc" : "asc");
    }else{
      sortKey = k;
      sortDir = "asc";
    }
    prefs.sortKey = sortKey;
    prefs.sortDir = sortDir;
    savePrefs();
    render(); // will call renderList() when in list view
    updateSortHeaderUI();
  }
  function wireSortableHeaders(){
    sortableHeaders().forEach(th=>{
      th.addEventListener("click", (e)=>{
        e.preventDefault();
        setSort(th.dataset.sort);
      }, true);
    });
    updateSortHeaderUI();
  }

  // ---------- core render switch ----------
  function render(){
    postersEl.style.display = (viewMode === "posters") ? "" : "none";
    listWrapEl.style.display = (viewMode === "list") ? "" : "none";

    sizeInput.style.display = (viewMode === "posters") ? "" : "none";
    sizeLabel.style.display = (viewMode === "posters") ? "" : "none";

    applyOverlayPrefUI();

    if (!filtered.length){
      empty.style.display = "";
      selAll.checked = false;
      listSelectAll.checked = false;
      postersEl.innerHTML = "";
      listBodyEl.innerHTML = "";
      updateSelCount();
      return;
    }
    empty.style.display = "none";

    if (viewMode === "posters") renderPosters();
    else renderList();

    updateSelCount();
  }

  // ---------- metadata fetch (bulk API; single item) ----------
  async function getMetaFor(it){
    const key = metaKey(it);
    if (metaCache.has(key)) return metaCache.get(key);

    const typ = key.startsWith("movie") ? "movie" : "show";
    const tmdb = (it.tmdb || it.ids?.tmdb || "") + "";
    if (!tmdb) { metaCache.set(key, null); return null; }

    const body = {
      items: [{ type: typ, tmdb }],
      need: {
        overview: true, tagline: true, runtime_minutes: true, poster: true, ids: true,
        videos: true, genres: true, certification: true, score: true, release: true,
        backdrop: true
      },
      concurrency: 1
    };

    try{
      const r = await fetch(`/api/metadata/bulk?overview=full`, {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify(body)
      });
      const j = await r.json();
      const results = j?.results || {};
      const firstKey = Object.keys(results)[0];
      const meta = (firstKey && results[firstKey]?.ok && results[firstKey]?.meta) ? results[firstKey].meta : null;
      metaCache.set(key, meta || null);
      return meta || null;
    }catch{
      metaCache.set(key, null);
      return null;
    }
  }

  // ---------- trailer helpers ----------
  function pickTrailer(meta){
    const pools = [
      meta?.videos,
      meta?.videos?.results,
      meta?.detail?.videos,
      meta?.detail?.videos?.results
    ].filter(Array.isArray);

    const vids = (pools.length ? pools.flat() : []).map(v => {
      const siteRaw = String(v.site || v.host || v.platform || "").toLowerCase();
      const site =
        siteRaw.includes("youtube") ? "youtube" :
        siteRaw.includes("vimeo")   ? "vimeo"   : siteRaw;

      const key = v.key || v.id || v.videoId || v.video_id || "";
      const type = String(v.type || v.category || "").toLowerCase();
      const name = v.name || v.title || "Trailer";
      const official = !!v.official;
      const published = Date.parse(v.published_at || v.publishedAt || v.created_at || v.added_at || "") || 0;

      const rank =
        (type.includes("trailer") ? 100 :
        type.includes("teaser")  ?  60 :
        type.includes("clip")    ?  40 : 10) +
        (official ? 30 : 0) +
        (site === "youtube" ? 5 : 0) +
        (published ? 1 : 0);

      return { site, key, type, name, official, published, rank };
    }).filter(v => v.site && v.key);

    vids.sort((a,b) => b.rank - a.rank);
    const v = vids[0];
    if (!v) return null;

    if (v.site === "youtube"){
      return {
        url: `https://www.youtube-nocookie.com/embed/${encodeURIComponent(v.key)}?autoplay=1&rel=0&modestbranding=1&playsinline=1`,
        site: "YouTube",
        title: v.name
      };
    }
    if (v.site === "vimeo"){
      return {
        url: `https://player.vimeo.com/video/${encodeURIComponent(v.key)}?autoplay=1`,
        site: "Vimeo",
        title: v.name
      };
    }
    return null;
  }

  let _prevFocus = null;
  function trapFocus(modal){
    const focusables = modal.querySelectorAll(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
    );
    const list = Array.from(focusables).filter(el => !el.hasAttribute('disabled'));
    if (!list.length) return;
    const first = list[0], last = list[list.length - 1];
    function onKey(e){
      if (e.key !== 'Tab') return;
      if (e.shiftKey && document.activeElement === first){ last.focus(); e.preventDefault(); }
      else if (!e.shiftKey && document.activeElement === last){ first.focus(); e.preventDefault(); }
    }
    modal._trapHandler = onKey;
    modal.addEventListener('keydown', onKey, true);
  }
  function releaseTrap(modal){
    if (modal?._trapHandler){
      modal.removeEventListener('keydown', modal._trapHandler, true);
      delete modal._trapHandler;
    }
  }

  function openTrailer(meta){
    const pick = pickTrailer(meta);
    if (pick) return openTrailerWithUrl(pick.url, pick.title);

    // Fallback: keep modal UX with a YouTube search embed
    const title = meta?.title || meta?.detail?.title || "";
    const year  = meta?.year || (meta?.detail?.release_date||"").slice(0,4) || "";
    const q = `${title} ${year} trailer`.trim();
    return openTrailerWithUrl(
      `https://www.youtube-nocookie.com/embed?listType=search&list=${encodeURIComponent(q)}&autoplay=1`,
      "Trailer"
    );
  }

  function openTrailerWithUrl(url, title="Trailer"){
    if (!trailerModal) { console.warn("Trailer modal element not found"); return; }
    const box = trailerModal.querySelector(".box");
    box.querySelector("iframe")?.remove();

    const iframe = document.createElement("iframe");
    // NOTE: no 'allowfullscreen' attr -> removes the console warning.
    iframe.setAttribute("allow", "autoplay; fullscreen; encrypted-media; picture-in-picture");
    iframe.setAttribute("loading", "lazy");
    iframe.setAttribute("referrerpolicy", "strict-origin-when-cross-origin");
    iframe.title = title;
    iframe.src = url;

    box.appendChild(iframe);
    _prevFocus = document.activeElement;
    trailerModal.classList.add("show");
    trapFocus(trailerModal);
    trailerClose?.focus();
  }

  function closeTrailer(){
    releaseTrap(trailerModal);
    trailerModal.classList.remove("show");
    const box = trailerModal.querySelector(".box");
    const iframe = box.querySelector("iframe");
    if (iframe) { try { iframe.src = "about:blank"; } catch {} iframe.remove(); } // stop playback
    if (_prevFocus && document.contains(_prevFocus)) { try { _prevFocus.focus(); } catch {} }
    _prevFocus = null;
  }

  // ---------- score ring ----------
  function createScoreSVG(score0to100){
    const v = Math.max(0, Math.min(100, Number(score0to100)||0));
    const r = 26;            // radius
    const c = 2 * Math.PI * r;
    const off = c * (1 - v/100);

    return `
      <svg viewBox="0 0 60 60" class="score" aria-label="User score ${v}%">
        <circle cx="30" cy="30" r="${r}" fill="none" stroke="rgba(255,255,255,.12)" stroke-width="6"/>
        <circle cx="30" cy="30" r="${r}" fill="none" stroke="currentColor" stroke-width="6"
                stroke-linecap="round" stroke-dasharray="${c.toFixed(2)}" stroke-dashoffset="${off.toFixed(2)}"/>
        <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" font-size="14" font-weight="700" fill="#fff">${v}%</text>
      </svg>
    `;
  }

  // ---------- small UI helpers ----------
  function providerChip(name, ok){
    const src = SRC_LOGOS[name];
    const icon = ok ? 'check_circle' : 'cancel';
    const cls = ok ? 'ok' : 'miss';
    return `<span class="wl-mat ${cls}" title="${name}${ok?' present':' missing'}">
      ${src ? `<img src="${src}" alt="${name}">` : `<span class="wl-badge">${name}</span>`}
      <span class="material-symbol">${icon}</span>
    </span>`;
  }
  function esc(s){ return String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

    /* --- Only show Active providers --- */
    let activeProviders = new Set();
    async function loadActiveProviders(){
      const r = await fetch("/api/config",{cache:"no-store"});
      if(!r.ok) return;
      const cfg = await r.json();
      const act = new Set();
      if(cfg?.plex?.account_token)     act.add("PLEX");
      if(cfg?.simkl?.access_token)     act.add("SIMKL");
      if(cfg?.trakt?.access_token)     act.add("TRAKT");
      if(cfg?.jellyfin?.access_token)  act.add("JELLYFIN");
      activeProviders = act;
    }
    const providerChipIfActive = (p, have) => activeProviders.has(p) ? providerChip(p, have) : "";

  // ---------- dominant color from image (for tint) ----------
  async function dominantRGB(url){
    return new Promise((resolve) => {
      try{
        const img = new Image();
        img.crossOrigin = "anonymous"; // same-origin paths are fine
        img.decoding = "async";
        img.onload = () => {
          try{
            const w = 40, h = 22;
            const c = document.createElement("canvas");
            c.width = w; c.height = h;
            const ctx = c.getContext("2d");
            ctx.drawImage(img, 0, 0, w, h);
            const data = ctx.getImageData(0,0,w,h).data;
            let r=0,g=0,b=0,n=0;
            for (let i=0;i<data.length;i+=4){
              const R=data[i], G=data[i+1], B=data[i+2], A=data[i+3];
              if (A<200) continue;
              r+=R; g+=G; b+=B; n++;
            }
            if (!n) return resolve("20,28,44");
            r=Math.round(r/n); g=Math.round(g/n); b=Math.round(b/n);
            resolve(`${r},${g},${b}`);
          }catch{ resolve("20,28,44"); }
        };
        img.onerror = () => resolve("20,28,44");
        img.src = url;
      }catch{ resolve("20,28,44"); }
    });
  }

  // ---------- detail bar (render, show/hide, hover/pin) ----------
  function releaseCountdownChip(isMovie, meta, it){
    const loc = toLocale();
    const relIso = isMovie ? (meta?.detail?.release_date || meta?.release?.date || it?.release_date)
                           : (meta?.detail?.first_air_date || it?.first_air_date);
    const d = daysBetweenNow(relIso);
    if (d == null) return "";
    const txt = d > 0 ? `Releases in ${d} day${d===1?"":"s"}` :
                d < 0 ? `Released ${Math.abs(d)} day${Math.abs(d)===1?"":"s"} ago` :
                        "Releases today";
    return `<span class="release-chip" title="${esc(fmtDate(relIso, toLocale()))}">${esc(txt)}</span>`;
  }

  function metaLine({isMovie, runtime, relFmt, cert, genresText}){
    // Build one compact line: Type • Runtime • Release date • Age • Genres
    const parts = [
      isMovie ? "Movie" : "TV",
      runtime || "",
      relFmt || "",
      cert || "",
      genresText || ""
    ].filter(Boolean);
    return parts.map((p,i)=> i===0 ? `<span class="chip">${esc(p)}</span>` : `<span class="dot">•</span><span class="chip">${esc(p)}</span>`).join("");
  }

  async function setBackdrop(meta, it){
    const b =
      meta?.images?.backdrop?.[0]?.url ||
      meta?.images?.backdrops?.[0]?.url ||
      artUrl(it, "w780") ||
      "";

    if (b){
      detailEl.style.setProperty("--wl-bg", `url("${b}")`);
      detailEl.classList.add("has-bg");       // <-- NEW
      try{
        const rgb = await dominantRGB(b);
        detailEl.style.setProperty("--wl-tint", rgb);
      }catch{}
    }else{
      detailEl.style.removeProperty("--wl-bg");
      detailEl.style.setProperty("--wl-tint","20,28,44");
      detailEl.classList.remove("has-bg");    // <-- NEW
    }
  }

  function renderDetail(it, meta){
    // Identify & cache the active preview
    const key = metaKey(it);
    activePreviewKey = key;

    // Basic shape
    const isMovie = key.startsWith("movie");
    const poster = artUrl(it, "w154");

    // Title line bits
    const year = (it.year || meta?.year) ? `<span class="year">${it.year || meta?.year}</span>` : "";

    // Meta line (Type • Runtime • Release • Age • Genres)
    const runtime = fmtRuntime(meta?.runtime_minutes);
    const genres = Array.isArray(meta?.genres) ? meta.genres : (Array.isArray(it?.genres) ? it.genres : []);
    const genresText = (genres || []).slice(0, 3).join(", ");
    const loc = toLocale();
    const relIso = isMovie
      ? (meta?.detail?.release_date || meta?.release?.date || it?.release_date)
      : (meta?.detail?.first_air_date || it?.first_air_date);
    const relFmt = fmtDate(relIso, loc);
    const cert = meta?.certification || meta?.release?.cert || meta?.detail?.certification;

    // Compose compact meta line and release chip
    const metaCompact = metaLine({ isMovie, runtime, relFmt, cert, genresText });
    const releaseChip = releaseCountdownChip(isMovie, meta, it);

    // Score (0..100) + classification color
    const score100 = typeof meta?.score === "number"
      ? Math.round(meta.score)
      : (typeof meta?.vote_average === "number" ? Math.round(meta.vote_average * 10) : null);
    const scoreCls = (score100 == null) ? "" : (score100 >= 70 ? "good" : (score100 >= 40 ? "mid" : "bad"));

    // Single declaration to avoid collisions
    const scoreHtml = (score100 != null) ? `
      <div style="text-align:center">
        ${createScoreSVG(score100).replace('<svg', `<svg class="score ${scoreCls}"`)}
        <span class="score-label">User Score</span>
      </div>` : "";

    // Tagline + overview with “More/Less”
    const tagline = meta?.tagline ? `<div class="tagline">${esc(meta.tagline)}</div>` : "";
    const overviewText = meta?.overview || "";
    const overview = overviewText
      ? `<div class="overview" id="wl-overview">${esc(overviewText)}</div><button class="more wl-btn" id="wl-overview-more">More</button>`
      : `<div class="overview wl-muted">No description available</div>`;

    // Source logos
    const srcs = providersOf(it);
    const logos = srcs.map(s=>{
      const src = SRC_LOGOS[s];
      return src
        ? `<span class="wl-src" title="${s}"><img src="${src}" alt="${s} logo"></span>`
        : `<span class="wl-badge">${s}</span>`;
    }).join("");

    // Trailer availability (but we render the button regardless; fallback will search YouTube)
    const hasTrailer = !!pickTrailer(meta);

    // Render
    detailEl.innerHTML = `
      <div class="inner" data-key="${key}">
        <div>${poster ? `<img class="poster" src="${poster}" alt="" onerror="this.onerror=null;this.src='/assets/placeholder_poster.svg'">` : ""}</div>

        <div>
          <div class="title-row">
            <div class="title">${esc(it.title || meta?.title || "Unknown")} ${year}</div>
            <button class="close wl-btn" id="wl-detail-close" title="Close"><span class="material-symbol">close</span></button>
          </div>
          <div class="meta">${metaCompact}${releaseChip || ""}</div>
          ${tagline}
          ${overview}
          <!-- logos moved to the Actions column -->
        </div>

        <div class="actions">
          ${scoreHtml || ""}
          <button class="wl-btn trailer" id="wl-play-trailer" title="${hasTrailer ? "Watch Trailer" : "Search trailer"}" ${hasTrailer ? "" : "data-fallback='1'"}>Watch Trailer</button>
          <div class="wl-srcs">${logos}</div>   <!-- now under Watch Trailer -->
        </div>
      </div>
    `;

    // Backdrop + tint (async; avoids flicker)
    setBackdrop(meta || {}, it || {});

    // Wiring: close, trailer, and overview toggle
    const closeBtn = document.getElementById("wl-detail-close");
    if (closeBtn) closeBtn.onclick = () => { pinnedKey = null; hideDetail(); };

    const playBtn = document.getElementById("wl-play-trailer");
    if (playBtn) {
      playBtn.onclick = () => {
        const pick = pickTrailer(meta);
        if (pick) {
          openTrailer(meta);
        } else {
          const title = it?.title || meta?.title || "";
          const yr = it?.year || meta?.year || "";
          const q = `${title} ${yr} trailer`.trim();
          window.open(`https://www.youtube.com/results?search_query=${encodeURIComponent(q)}`, "_blank", "noopener,noreferrer");
        }
      };
    }

    const moreBtn = document.getElementById("wl-overview-more");
    const ov = document.getElementById("wl-overview");
    if (moreBtn && ov){
      moreBtn.addEventListener("click", ()=>{
        const expanded = ov.classList.toggle("expanded");
        moreBtn.textContent = expanded ? "Less" : "More";
      }, true);
    }

    detailEl.classList.add("show");
  }

  function hideDetail(){
    if (!detailEl.classList.contains("show")) return;
    detailEl.classList.remove("show");
    activePreviewKey = null;
  }

  // Hover debounce + pin logic
  function schedulePreview(it){
    if (pinnedKey) return; // do not override pinned detail
    if (previewTimer) { clearTimeout(previewTimer); previewTimer = null; }
    previewTimer = setTimeout(async ()=>{
      const meta = await getMetaFor(it);
      renderDetail(it, meta||{});
    }, 120);
  }
  function cancelPreview(){
    if (previewTimer) { clearTimeout(previewTimer); previewTimer = null; }
    if (!pinnedKey) hideDetail();
  }

  // ---------- posters & list renders ----------
  function kickLazy(){
    postersEl.querySelectorAll('img[loading="lazy"]').forEach(img => { img.src = img.src; });
  }

  function renderPosters(){
    postersEl.innerHTML = "";
    const frag = document.createDocumentFragment();

    filtered.forEach((it, idx)=>{
      const imgUrl = artUrl(it, "w342");
      const provHtml = providersOf(it).map(p=>`<span class="wl-tag">${p}</span>`).join("");
      const key = normKey(it);
      const card = document.createElement("div");
      card.className = `wl-card ${selected.has(key) ? "selected": ""}`;

      const eager = idx < 24; // first screenful
      const imgTag = imgUrl
        ? `<img ${eager ? 'loading="eager" fetchpriority="high"' : 'loading="lazy"'}
                decoding="async"
                src="${imgUrl}" alt=""
                width="342" height="513"
                onerror="this.onerror=null;this.src='/assets/placeholder_poster.svg'">`
        : `<div style="height:100%"></div>`;


      // overlays (tags) are hidden via CSS class when prefs.overlays === 'no'
      card.innerHTML = `<div class="wl-tags">${provHtml}</div>${imgTag}`;

      // Selection toggle & pin detail
      card.addEventListener("click", ()=>{
        if (selected.has(key)) selected.delete(key); else selected.add(key);
        card.classList.toggle("selected");
        updateSelCount();

        pinnedKey = metaKey(it);
        getMetaFor(it).then(meta => renderDetail(it, meta||{}));
      });

      // Hover preview (debounced)
      card.addEventListener("mouseenter", ()=> schedulePreview(it));
      card.addEventListener("mouseleave", cancelPreview);

      frag.appendChild(card);
    });

    postersEl.appendChild(frag);
    requestAnimationFrame(kickLazy);
  }

  function renderList(){
    listBodyEl.innerHTML = "";
    const frag = document.createDocumentFragment();

    // Respect current sort
    const rows = sortFilteredForList(filtered);

    rows.forEach(it=>{
      const key = normKey(it);
      const tr = document.createElement("tr");

      const typeRaw = String(it.type || "").toLowerCase();
      const type = (typeRaw === "show" ? "tv" : typeRaw);
      const typeLabel = type === "movie" ? "Movie" : (type === "tv" ? "Show" : "");

      const thumb = artUrl(it, "w92");

      const have = {
        PLEX:     providersOf(it).includes("PLEX"),
        SIMKL:    providersOf(it).includes("SIMKL"),
        TRAKT:    providersOf(it).includes("TRAKT"),
        JELLYFIN: providersOf(it).includes("JELLYFIN"),
      };

      const matrix = `
        <div class="wl-matrix">
          ${providerChipIfActive('PLEX',    have.PLEX)}
          ${providerChipIfActive('SIMKL',   have.SIMKL)}
          ${providerChipIfActive('TRAKT',   have.TRAKT)}
          ${providerChipIfActive('JELLYFIN',have.JELLYFIN)}
        </div>`;

      // Release + Genre (separate columns)
      const rel = fmtDateSmart(getReleaseIso(it), toLocale());
      const genresList = extractGenres(it).slice(0, 3).join(", ");

      tr.innerHTML = `
        <td style="text-align:center"><input type="checkbox" data-k="${key}" ${selected.has(key) ? "checked" : ""}></td>
        <td class="title"><div>${esc(it.title || "")}</div></td>
        <td class="rel">${esc(rel)}</td>
        <td class="genre" title="${esc(genresList)}">${esc(genresList)}</td>
        <td>${esc(typeLabel)}</td>
        <td>${matrix}</td>
        <td>${thumb ? `<img class="wl-mini" src="${thumb}" alt="" onerror="this.onerror=null;this.src='/assets/placeholder_poster.svg'">` : ""}</td>
      `;
      tr.querySelector('input[type=checkbox]').addEventListener("change", (e)=>{
        if (e.target.checked) selected.add(key); else selected.delete(key);
        updateSelCount();
      });

      // Click row = pin detail
      tr.addEventListener("click", (ev)=>{
        // Don't hijack checkbox clicks
        if (ev.target && (ev.target.tagName === "INPUT" || ev.target.closest("input"))) return;
        pinnedKey = metaKey(it);
        getMetaFor(it).then(meta => renderDetail(it, meta||{}));
      });

      frag.appendChild(tr);
    });

    listBodyEl.appendChild(frag);

    const allKeys = filtered.map(normKey);
    listSelectAll.checked = allKeys.length>0 && allKeys.every(k => selected.has(k));

    // ensure header sort chevrons are correct after render
    updateSortHeaderUI();
  }

  // ---------- selection UI ----------
  function updateSelCount(){
    selCount.textContent = `${selected.size} selected`;
    rebuildDeleteProviderOptions();
    const provider = delProv.value || "";
    document.getElementById("wl-delete").disabled = !(provider && selected.size);
    document.getElementById("wl-hide").disabled = selected.size === 0;
  }

  // ---------- snackbar / helpers ----------
  function snackbar(html, actions=[]) {
    if (snackTimer) { clearTimeout(snackTimer); snackTimer = null; }
    snack.innerHTML = html + actions.map(a=>` <button class="wl-btn" data-k="${a.key}">${a.label}</button>`).join("");
    snack.classList.remove("wl-hidden");

    const handler = (e)=>{
      const k = e.target?.dataset?.k;
      if (!k) return;
      actions.find(a=>a.key===k)?.onClick?.();
      snack.classList.add("wl-hidden");
      snack.removeEventListener("click", handler, true);
      if (snackTimer) { clearTimeout(snackTimer); snackTimer = null; }
    };
    snack.addEventListener("click", handler, true);

    snackTimer = setTimeout(()=>{
      snack.classList.add("wl-hidden");
      snack.removeEventListener("click", handler, true);
      snackTimer = null;
    }, 2000);
  }

  const sleep = (ms) => new Promise(res => setTimeout(res, ms));
  // ---------- refresh / network ----------
  async function hardReloadWatchlist(){
    try {
      const list = await fetchWatchlist();
      items = list;

      // Rebuild dynamic genre options (warm once if empty)
      populateGenreOptions(buildGenreIndex(items));
      if (genreSel.options.length <= 1) {
        try {
          await warmGenresForFilter();
          populateGenreOptions(buildGenreIndex(items));
        } catch {}
      }

      // Restore selection + re-apply filters (does render + metrics)
      genreSel.value = prefs.genre || "";
      applyFilters();
      rebuildDeleteProviderOptions();
    } catch (e) {
      console.warn("Watchlist hard reload failed:", e);
    }
  }

  // Delete response parsing (PLEX/SIMKL/TRAKT/JELLYFIN/ALL)
  async function postDelete(keys, provider) {
    let status = 0;
    try {
      const prov = (provider || "ALL").toUpperCase();
      const r = await fetch("/api/watchlist/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ keys, provider: prov })
      });

      status = r.status;

      // Tolerate empty/non-JSON bodies
      let txt = "";
      try { txt = await r.text(); } catch {}
      let data = null;
      try { data = txt ? JSON.parse(txt) : null; } catch {}

      // Universal fast-path: any 2xx = success (backend already enforced auth/logic)
      if (r.ok) {
        let okCount = 0;
        if (data && typeof data.deleted_ok === "number") {
          okCount = data.deleted_ok;
        } else if (Array.isArray(data?.results)) {
          okCount = data.results.filter(x => x && (x.ok === true || x.status === "ok")).length;
        } else {
          okCount = keys.length; // assume batch-level OK
        }
        return { okCount, anySuccess: true, status, networkError: false, raw: data };
      }

      // Non-2xx fallback: try to salvage partial success
      let okCount = 0;
      if (data && typeof data.deleted_ok === "number") {
        okCount = data.deleted_ok;
      } else if (Array.isArray(data?.results)) {
        okCount = data.results.filter(x => x && (x.ok === true || x.status === "ok")).length;
      }
      const anySuccess = (okCount > 0) || (data?.ok === true);
      return { okCount, anySuccess, status, networkError: false, raw: data };
    } catch {
      return { okCount: 0, anySuccess: false, status: status || 0, networkError: true, raw: null };
    }
  }

  function computeDelta(keys, provider, beforeProv, afterProv){
    let deltaOk = 0;
    for (const k of keys) {
      const before = beforeProv.get(k);
      const after = afterProv.get(k);
      if (!before) continue;
      if (!after) { deltaOk++; continue; } // fully removed
      if (provider === "ALL") {
        if (after.size < before.size) deltaOk++;
      } else {
        if (before.has(provider) && !after.has(provider)) deltaOk++;
      }
    }
    return deltaOk;
  }

  // ---------- events: filters & selection ----------
  const normReleased = v => (v === "yes" ? "released" : v === "no" ? "unreleased" : (v || "both"));

  // Basic filters
  qEl.addEventListener("input", () => { applyFilters(); }, true);
  ["change","input"].forEach(ev => {
    tEl.addEventListener(ev, () => { applyFilters(); }, true);
    providerSel.addEventListener(ev, () => { applyFilters(); }, true);
  });

  // “More…” panel toggle + persistence
  moreBtn.addEventListener("click", () => {
    const open = morePanel.style.display !== "none";
    morePanel.style.display = open ? "none" : "";
    prefs.moreOpen = !open;
    savePrefs();
  }, true);

  // Released (persist normalized value)
  ["change","input"].forEach(ev => {
    releasedSel.addEventListener(ev, () => {
      prefs.released = normReleased(releasedSel.value);
      savePrefs();
      applyFilters();
    }, true);
  });

  // Overlays (UI-only; no filter re-run needed)
  ["change","input"].forEach(ev => {
    overlaysSel.addEventListener(ev, () => {
      prefs.overlays = overlaysSel.value || "yes";
      savePrefs();
      applyOverlayPrefUI();
    }, true);
  });

  // Genre
  ["change","input"].forEach(ev => {
    genreSel.addEventListener(ev, () => {
      prefs.genre = genreSel.value || "";
      savePrefs();
      applyFilters();
    }, true);
  });

  // Select all (posters vs list)
  selAll.addEventListener("change", () => {
    selected.clear();
    if (selAll.checked) filtered.forEach(it => { const key = normKey(it); if (key) selected.add(key); });
    if (viewMode === "posters") renderPosters(); else renderList();
    updateSelCount();
  }, true);

  listSelectAll.addEventListener("change", () => {
    selected.clear();
    if (listSelectAll.checked) filtered.forEach(it => { const key = normKey(it); if (key) selected.add(key); });
    renderList();
    selAll.checked = listSelectAll.checked;
    updateSelCount();
  }, true);

  // Reset button (keep view + poster size)
  clearBtn.addEventListener("click", () => {
    qEl.value = "";
    tEl.value = "";
    providerSel.value = "";

    // advanced filters
    releasedSel.value = "both";          // normalized key
    overlaysSel.value = "yes";
    genreSel.value = "";

    // persist advanced defaults
    prefs.released = "both";
    prefs.overlays = "yes";
    prefs.genre = "";
    savePrefs();

    applyOverlayPrefUI();
    applyFilters();
  }, true);

  // Delete provider dropdown affects button enablement
  delProv.addEventListener("change", updateSelCount, true);

  // Poster size slider
  sizeInput.addEventListener("input", () => {
    const px = Math.max(120, Math.min(320, Number(sizeInput.value) || 150));
    applyPosterSize(px);
    prefs.posterMin = px;
    savePrefs();
  }, true);

  // ---------- busy button helpers ----------
  (() => {
    if (window.beginBusy) return;

    const style = document.createElement("style");
    style.textContent = `
      .btn-busy{position:relative;opacity:.85;pointer-events:none}
      .btn-busy .btn-spinner{
        position:absolute;left:8px;top:50%;transform:translateY(-50%);
        width:14px;height:14px;border-radius:50%;
        border:2px solid currentColor;border-right-color:transparent;
        animation:btnspin .8s linear infinite
      }
      .btn-busy .btn-progress{
        position:absolute;left:0;bottom:0;height:2px;width:0;
        background:currentColor;opacity:.6;transition:width .2s ease
      }
      @keyframes btnspin{to{transform:translateY(-50%) rotate(360deg)}}
    `;
    document.head.appendChild(style);

    window.beginBusy = function(btn, label){
      if (!btn || btn.dataset.busy === "1") return;
      btn.dataset.busy = "1";
      btn.dataset.origHTML = btn.innerHTML;
      const text = label || btn.textContent.trim() || "Working…";
      btn.innerHTML =
        `<span class="btn-spinner"></span>` +
        `<span class="btn-label" style="margin-left:22px">${text}</span>` +
        `<span class="btn-progress"></span>`;
      btn.classList.add("btn-busy");
      btn.disabled = true;
    };

    window.updateBusy = function(btn, pct){
      if (!btn || btn.dataset.busy !== "1") return;
      const bar = btn.querySelector(".btn-progress");
      if (bar && isFinite(pct)) {
        const v = Math.max(0, Math.min(100, Number(pct)));
        bar.style.width = v + "%";
      }
    };

    window.endBusy = function(btn){
      if (!btn || btn.dataset.busy !== "1") return;
      btn.classList.remove("btn-busy");
      btn.innerHTML = btn.dataset.origHTML || btn.textContent;
      btn.disabled = false;
      delete btn.dataset.busy;
      delete btn.dataset.origHTML;
    };
  })();

  // ---------- delete flow ----------
  // ---------- delete flow ----------
  document.getElementById("wl-delete")?.addEventListener("click", async () => {
    const provider = (delProv?.value || "ALL").toUpperCase();
    if (!selected?.size) { snackbar?.("Nothing selected"); return; }

    const keys = [...selected];
    const btn = document.getElementById("wl-delete");
    beginBusy?.(btn, `Deleting ${keys.length}…`);

    const beforeProv = mapProvidersByKey(items);
    let totalOk = 0;
    let attemptCount = 0;
    let anyHttpAttempt = false;
    let sawNetworkError = false;
    let any2xx = false;
    let lastStatus = 0;

    try {
      const CHUNK = 50;

      // Build run plan
      const runPlan = [];
      if (provider === "ALL") {
        runPlan.push({ prov: "ALL", list: keys });
      } else {
        const map = mapProvidersByKey(items);
        const subset = keys.filter(k => (map.get(k) || new Set()).has(provider));
        runPlan.push({ prov: provider, list: subset.length ? subset : keys });
      }

      // Execute deletes in chunks
      for (const { prov, list } of runPlan) {
        for (let i = 0; i < list.length; i += CHUNK) {
          const part = list.slice(i, i + CHUNK);
          const res = await postDelete(part, prov);
          attemptCount++;
          anyHttpAttempt ||= (res.status > 0);
          sawNetworkError ||= !!res.networkError;
          lastStatus = res.status || lastStatus;
          totalOk += (res.okCount || 0);
          any2xx ||= (res.status >= 200 && res.status < 300) || !!res.anySuccess;
          updateBusy?.(btn, Math.round(((i + part.length) / list.length) * 100));
        }
      }

    // ----- Optimistic UI: remove immediately from view -----
    if (typeof applyOptimisticDeletion === "function") {
      applyOptimisticDeletion(keys, provider);
    } else {
      // Fallback: mutate local model (keeps UI responsive if helper missing)
      const byProv = mapProvidersByKey(items);
      for (const k of keys) {
        const provs = byProv.get(k) || new Set();
        if (provider === "ALL") provs.clear();
        else provs.delete(provider);
        if (provs.size === 0) {
          const idx = items.findIndex(it => it.key === k);
          if (idx >= 0) items.splice(idx, 1);
        }
      }
    }
    selected.clear();
    applyFilters?.();
    updateSelCount?.();
    updateMetrics?.();

    // ----- Background refresh to pull server state -----
    Promise.resolve().then(async () => {
      try {
        await (hardReloadWatchlist?.());
        if (provider === "JELLYFIN") {
          // JF can lag a bit; do a late re-poll
          setTimeout(() => { try { hardReloadWatchlist?.(); } catch {} }, 700);
        }
      } catch {}
    });

    // ----- Messaging -----
    const ok = any2xx || (totalOk > 0);
    const n = totalOk || (any2xx ? keys.length : 0);
    if (ok) {
      snackbar?.(
        provider === "ALL"
          ? `Deleted on available providers for ${n}/${keys.length} item(s)`
          : `Deleted ${n}/${keys.length} from <b>${provider}</b>`
      );
    } else {
      if (attemptCount === 0) {
        snackbar?.("No delete attempted");
      } else if (!anyHttpAttempt) {
        snackbar?.("No delete attempted (network) — check connectivity");
      } else if (sawNetworkError || (lastStatus >= 400 || lastStatus === 0)) {
        snackbar?.(`Delete failed${lastStatus ? ` (HTTP ${lastStatus})` : ""} — check credentials/providers`);
      } else {
        snackbar?.("Delete completed with no visible changes");
      }
    }
  } catch (err) {
    console.error("[WL] delete flow error:", err);
    snackbar?.("Delete failed");
  } finally {
    endBusy?.(btn);
  }
});


  // ---------- keyboard ----------
  document.addEventListener("keydown", (e)=>{
    if (e.key === "Delete" && !document.getElementById("wl-delete").disabled) {
      document.getElementById("wl-delete").click();
    }
    if (e.key === "Escape") {
      if (trailerModal.classList.contains("show")) closeTrailer();
      else { pinnedKey = null; hideDetail(); }
    }
  }, true);

  // ---------- view toggle ----------
  viewSel.addEventListener("change", ()=>{
    viewMode = viewSel.value === "list" ? "list" : "posters";
    prefs.view = viewMode; savePrefs();
    render();
  });

  // ---------- route hooks ----------
  const navCandidates = [
    '#nav-watchlist',
    '[data-nav="watchlist"]',
    'a[href="#watchlist"]'
  ];
  for (const sel of navCandidates) {
    const el = document.querySelector(sel);
    if (el) el.addEventListener('click', async () => { await hardReloadWatchlist(); }, true);
  }
  window.addEventListener('hashchange', async () => {
    if ((location.hash || '').toLowerCase().includes('watchlist')) {
      await hardReloadWatchlist();
    }
  });

  // ---------- init ----------
  (async function init(){
    // base prefs
    viewMode = (prefs.view === "list") ? "list" : "posters";
    viewSel.value = viewMode;
    sizeInput.value = String(prefs.posterMin);
    applyPosterSize(prefs.posterMin);

    // advanced filters (More…)
    releasedSel.value = prefs.released || "both";
    overlaysSel.value = prefs.overlays || "yes";
    // show overlays control only in posters view
    applyOverlayPrefUI();

    // panel open/closed state
    morePanel.style.display = prefs.moreOpen ? "" : "none";

    // load active providers 
    await loadActiveProviders();

    // fetch + render
    items = await fetchWatchlist();

    // build dynamic genres before applying filters
    populateGenreOptions(buildGenreIndex(items));

    if (genreSel.options.length <= 1) {
      // Warm once, then repopulate + re-apply selection and filters
      warmGenresForFilter().then(() => {
        populateGenreOptions(buildGenreIndex(items));
        genreSel.value = prefs.genre || "";
        applyFilters();
      });
    }

    // Initial selection + filter pass (in case warming isn't needed)
    genreSel.value = prefs.genre || "";
    applyFilters();
    rebuildDeleteProviderOptions();

    // wire sorting once DOM is ready
    wireSortableHeaders();

  })();

  // ---------- auto refresh ----------
  const AUTO_REFRESH_MS = 60000;
  let _refreshBusy = false;
  let _prevSig = "";

  setInterval(async () => {
    if (_refreshBusy) return;
    if (document.visibilityState !== "visible") return;

    _refreshBusy = true;
    try {
      const list = await fetchWatchlist();

      // Cheap signature to detect changes (add/remove/reorder)
      const sig = (() => {
        try {
          const keys = list.map(normKey).filter(Boolean);
          return `${keys.length}:${keys.slice(0, 500).join(",")}`;
        } catch { return String(list?.length || 0); }
      })();

      if (sig !== _prevSig) {
        _prevSig = sig;
        items = list;

        // Genres can change when catalog changes
        populateGenreOptions(buildGenreIndex(items));
        if (genreSel.options.length <= 1) {
          await warmGenresForFilter();
          populateGenreOptions(buildGenreIndex(items));
        }

        // Restore selection + re-apply filters
        genreSel.value = prefs.genre || "";
        applyFilters();
        rebuildDeleteProviderOptions();
      }
    } catch (e) {
      // Optional: console.warn('auto refresh failed', e);
    } finally {
      _refreshBusy = false;
    }
  }, AUTO_REFRESH_MS);

  // ---------- legacy API ----------
  window.Watchlist = {
    async mount(_host) {},
    async refresh() {
      try {
        const list = await fetchWatchlist();
        items = list;

        // Update genre options (and warm once if empty)
        populateGenreOptions(buildGenreIndex(items));
        if (genreSel.options.length <= 1) {
          try {
            await warmGenresForFilter();
            populateGenreOptions(buildGenreIndex(items));
          } catch {}
        }

        // Restore selection + re-apply filters
        genreSel.value = prefs.genre || "";
        applyFilters();
        rebuildDeleteProviderOptions();
      } catch {}
    }
  };

  window.dispatchEvent(new CustomEvent("watchlist-ready"));
})();
