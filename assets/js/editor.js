(function () {
  const PAGE_SIZE = 50;
  const STORAGE_KEY = "cw-editor-ui";

  const css = `
  .cw-root{display:flex;flex-direction:column;gap:10px}
  .cw-topline{margin-bottom:4px}
  .cw-wrap{display:grid;grid-template-columns:minmax(0,1fr) 360px;gap:16px;align-items:flex-start}
  .cw-main{display:flex;flex-direction:column;gap:8px}
  .cw-side{display:flex;flex-direction:column;gap:6px}

  .cw-controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px}
  .cw-controls .cw-input{flex:1 1 260px;max-width:420px}
  .cw-controls-spacer{flex:1 1 auto}
  .cw-status-text{font-size:12px;opacity:.8}
  .cw-input,.cw-select,.cw-btn{
    background:#15151c;
    border:1px solid rgba(255,255,255,.12);
    border-radius:8px;
    color:#fff;
    font-size:13px;
    padding:8px 10px;
  }
  .cw-input{width:100%}
  .cw-select{min-height:34px}
  .cw-btn{
    background:#1d1d26;
    border-color:rgba(255,255,255,.15);
    cursor:pointer;
    display:inline-flex;
    align-items:center;
    gap:6px;
    white-space:nowrap;
  }
  .cw-btn.primary{background:#2154ff;border-color:#2154ff}
  .cw-btn.danger{background:#2a1113;border-color:#57252a}
  .cw-btn-del{
    padding:3px 6px;
    font-size:11px;
    min-width:26px;
    width:26px;
    height:26px;
    justify-content:center;
    border-radius:10px;
  }
  .cw-btn-del .material-symbol{font-size:14px;line-height:1}
  .cw-side .cw-select,.cw-side .cw-input{width:100%}
  .cw-backup-actions{display:flex;flex-wrap:wrap;gap:6px}

  .cw-table-wrap{
    border:1px solid rgba(255,255,255,.12);
    border-radius:10px;
    overflow:auto;
    max-height:70vh;
  }
  .cw-table{
    width:100%;
    border-collapse:separate;
    border-spacing:0;
    table-layout:fixed;
    font-size:12px;
  }
  .cw-table th,.cw-table td{
    padding:6px 8px;
    border-bottom:1px solid rgba(255,255,255,.08);
    white-space:nowrap;
    text-align:left;
  }
  .cw-table th{
    position:sticky;
    top:0;
    background:#101018;
    font-weight:600;
    z-index:1;
  }
  .cw-table tr:last-child td{border-bottom:none}
  .cw-table input{
    width:100%;
    background:#111119;
    border:1px solid rgba(255,255,255,.12);
    border-radius:6px;
    padding:3px 5px;
    font-size:12px;
    color:#fff;
  }
  .cw-table input:focus{
    outline:none;
    border-color:#2154ff;
    box-shadow:0 0 0 1px rgba(33,84,255,.5);
  }
  .cw-table .cw-key{font-family:monospace;font-size:11px}
  .cw-row-episode{background:rgba(108,92,231,.05)}
  .cw-row-deleted td{opacity:.4;text-decoration:line-through}

  .cw-table th.sortable{cursor:pointer;user-select:none}
  .cw-table th.sortable::after{content:"";margin-left:6px;opacity:.6;font-size:10px}
  .cw-table th.sort-asc::after{content:"▲"}
  .cw-table th.sort-desc::after{content:"▼"}

  .cw-empty{
    padding:24px;
    border:1px dashed rgba(255,255,255,.12);
    border-radius:12px;
    text-align:center;
    font-size:13px;
    opacity:.7;
  }
  .cw-pager{
    display:flex;
    align-items:center;
    justify-content:flex-end;
    gap:8px;
    margin:6px 0;
    font-size:12px;
  }
  .cw-pager .cw-page-info{opacity:.8}
  .cw-pager .cw-btn{min-width:80px;padding:6px 10px;font-size:12px}

  .ins-card{
    background:linear-gradient(180deg,rgba(20,20,28,.95),rgba(16,16,24,.95));
    border:1px solid rgba(255,255,255,.08);
    border-radius:16px;
    padding:10px 12px;
  }
  .ins-row{
    display:flex;
    align-items:center;
    gap:12px;
    padding:8px 6px;
    border-top:1px solid rgba(255,255,255,.06);
  }
  .ins-row:first-child{border-top:none;padding-top:2px}
  .ins-icon{
    width:32px;
    height:32px;
    border-radius:10px;
    display:flex;
    align-items:center;
    justify-content:center;
    background:#13131b;
    border:1px solid rgba(255,255,255,.06);
  }
  .ins-title{font-weight:700}
  .ins-kv{
    display:grid;
    grid-template-columns:110px 1fr;
    gap:10px;
    align-items:center;
  }
  .ins-kv label{opacity:.85}

  .ins-metrics{
    display:flex;
    flex-direction:column;
    gap:6px;
    width:100%;
  }
  .metric-row{
    display:grid;
    grid-template-columns:repeat(auto-fit,minmax(0,1fr));
    gap:8px;
  }
  .metric-divider{
    height:1px;
    background:rgba(148,163,184,.28);
    margin:2px 0;
  }
  .metric{
    position:relative;
    display:flex;
    align-items:center;
    gap:8px;
    background:#12121a;
    border:1px solid rgba(255,255,255,.08);
    border-radius:12px;
    padding:10px;
  }
  .metric .material-symbol{font-size:18px;opacity:.9}
  .metric .m-val{font-weight:700}
  .metric .m-lbl{font-size:12px;opacity:.75}

.cw-tag{
  position:relative;
  display:inline-flex;
  align-items:center;
  gap:6px;
  font-size:11px;
  padding:4px 12px;
  border-radius:999px;
  background:radial-gradient(circle at 0 50%,rgba(52,211,153,.28),rgba(15,23,42,.96));
  border:1px solid rgba(52,211,153,.85);
  box-shadow:0 0 0 1px rgba(15,23,42,1),0 0 18px rgba(52,211,153,.45);
  color:#e5e7eb;
  letter-spacing:.02em;
  transition:background .18s ease,border-color .18s ease,box-shadow .18s ease,color .18s ease;
}
.cw-tag::before{
  content:"";
  position:absolute;
  inset:-2px;
  border-radius:inherit;
  background:radial-gradient(circle at 0 50%,rgba(52,211,153,.45),transparent 55%);
  opacity:.85;
  filter:blur(8px);
  z-index:-1;
}
.cw-tag-dot{
  width:8px;
  height:8px;
  border-radius:999px;
  background:linear-gradient(135deg,#6ee7b7,#22c55e);
  box-shadow:0 0 8px rgba(52,211,153,.9),0 0 14px rgba(52,211,153,.75);
  animation:cw-status-pulse 1.4s ease-in-out infinite;
}

.cw-tag.warn{
  background:radial-gradient(circle at 0 50%,rgba(248,187,109,.3),rgba(24,16,4,.96));
  border-color:rgba(250,204,21,.9);
  box-shadow:0 0 0 1px rgba(15,23,42,1),0 0 18px rgba(251,191,36,.5);
}
.cw-tag.warn .cw-tag-dot{
  background:linear-gradient(135deg,#fbbf24,#f97316);
  box-shadow:0 0 10px rgba(251,191,36,1),0 0 20px rgba(249,115,22,.95);
}

.cw-tag.error{
  background:radial-gradient(circle at 0 50%,rgba(248,113,113,.35),rgba(24,6,7,.96));
  border-color:rgba(248,113,113,.9);
  box-shadow:0 0 0 1px rgba(15,23,42,1),0 0 18px rgba(248,113,113,.55);
}
.cw-tag.error .cw-tag-dot{
  background:linear-gradient(135deg,#fb7185,#ef4444);
  box-shadow:0 0 10px rgba(248,113,113,1),0 0 20px rgba(248,113,113,.9);
}

@keyframes cw-status-pulse{
  0%{
    transform:scale(.9);
    opacity:.7;
    box-shadow:0 0 6px rgba(52,211,153,.8),0 0 12px rgba(52,211,153,.6);
  }
  100%{
    transform:scale(1.18);
    opacity:1;
    box-shadow:0 0 12px rgba(52,211,153,1),0 0 22px rgba(52,211,153,.95);
  }
}
  .cw-extra-display{
    width:100%;
    background:#111119;
    border-radius:6px;
    border:1px solid rgba(129,140,248,.45);
    padding:4px 8px;
    font-size:12px;
    color:#e5e7ff;
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
    cursor:pointer;
    box-shadow:0 0 0 1px rgba(15,23,42,.6);
    transition:border-color .15s,box-shadow .15s,background .15s;
  }
  .cw-extra-display:hover{
    border-color:#818cf8;
    box-shadow:0 0 0 1px rgba(129,140,248,.7),0 0 18px rgba(129,140,248,.35);
    background:#151528;
  }
  .cw-extra-display-label{
    flex:1;
    overflow:hidden;
    text-overflow:ellipsis;
    white-space:nowrap;
  }
  .cw-extra-display-placeholder{
    opacity:.55;
    font-style:italic;
  }
  .cw-extra-display-value{
    color:#e5e7ff;
    font-weight:400;
  }
  .cw-extra-display-icon{
    font-size:14px;
    opacity:.7;
  }

  .cw-pop{
    position:fixed;
    z-index:10060;
    background:radial-gradient(circle at top,#1e1b4b,#020617 60%);
    border-radius:12px;
    border:1px solid rgba(129,140,248,.9);
    box-shadow:0 18px 45px rgba(0,0,0,.8);
    padding:10px 12px;
    min-width:220px;
    color:#e5e7ff;
  }
  .cw-pop-title{
    font-size:13px;
    font-weight:600;
    margin-bottom:6px;
  }
  .cw-pop-actions{
    display:flex;
    justify-content:flex-end;
    gap:8px;
    margin-top:8px;
  }
  .cw-pop-btn{
    border-radius:999px;
    border:1px solid rgba(148,163,184,.8);
    background:rgba(15,23,42,.9);
    padding:4px 10px;
    font-size:12px;
    color:#e5e7eb;
    cursor:pointer;
  }
  .cw-pop-btn.primary{
    border-color:#4f46e5;
    background:linear-gradient(90deg,#4f46e5,#22c1c3);
    color:#f9fafb;
  }
  .cw-pop-btn.ghost{
    background:transparent;
  }

  .cw-datetime-grid{
    display:grid;
    grid-template-columns:repeat(2,minmax(0,1fr));
    gap:8px;
    margin-top:6px;
  }
  .cw-pop input[type="date"],
  .cw-pop input[type="time"]{
    width:100%;
    background:#020617;
    border-radius:8px;
    border:1px solid rgba(148,163,184,.7);
    color:#e5e7eb;
    font-size:12px;
    padding:6px 8px;
  }

  .cw-rating-grid{
    display:grid;
    grid-template-columns:repeat(5,minmax(0,1fr));
    gap:6px;
    margin-top:6px;
  }
  .cw-rating-pill{
    border-radius:999px;
    border:1px solid rgba(148,163,184,.7);
    background:#020617;
    color:#e5e7eb;
    font-size:12px;
    padding:4px 0;
    text-align:center;
    cursor:pointer;
    transition:border-color .15s,background .15s,box-shadow .15s;
  }
  .cw-rating-pill:hover{
    border-color:#a5b4fc;
    box-shadow:0 0 12px rgba(129,140,248,.55);
  }
  .cw-rating-pill.active{
    background:linear-gradient(135deg,#4f46e5,#22c1c3);
    border-color:#c4b5fd;
    color:#f9fafb;
  }

  .cw-type-grid{
    display:grid;
    grid-template-columns:repeat(3,minmax(0,1fr));
    gap:6px;
    margin-top:6px;
  }
  .cw-type-pill{
    border-radius:999px;
    border:1px solid rgba(148,163,184,.7);
    background:#020617;
    color:#e5e7eb;
    font-size:12px;
    padding:4px 0;
    text-align:center;
    cursor:pointer;
    transition:border-color .15s,background .15s,box-shadow .15s;
  }
  .cw-type-pill:hover{
    border-color:#a5b4fc;
    box-shadow:0 0 12px rgba(129,140,248,.55);
  }
  .cw-type-pill.active{
    background:linear-gradient(135deg,#4f46e5,#22c1c3);
    border-color:#c4b5fd;
    color:#f9fafb;
  }

  .cw-type-filter{
    display:flex;
    flex-wrap:wrap;
    gap:6px;
  }
  .cw-type-chip{
    border-radius:999px;
    border:1px solid rgba(148,163,184,.7);
    background:#020617;
    color:#e5e7eb;
    font-size:11px;
    padding:4px 10px;
    cursor:pointer;
    transition:border-color .15s,background .15s,box-shadow .15s;
  }
  .cw-type-chip.active{
    background:linear-gradient(135deg,#4f46e5,#22c1c3);
    border-color:#c4b5fd;
    color:#f9fafb;
  }

  .cw-state-hint{
    margin-top:6px;
    font-size:11px;
    line-height:1.4;
    background:rgba(15,23,42,.96);
    border-radius:10px;
    border:1px dashed rgba(148,163,184,.65);
    padding:8px 10px;
    color:#e5e7eb;
  }
  .cw-state-hint strong{color:#a5b4fc}

  @media (max-width:1100px){
    .cw-wrap{grid-template-columns:minmax(0,1fr)}
  }
  `;

  const ensureStyle = (id, txt) => {
    let s = document.getElementById(id);
    if (!s) { s = document.createElement("style"); s.id = id; }
    s.textContent = txt;
    if (!s.parentNode) document.head.appendChild(s);
  };
  ensureStyle("editor-styles", css);

  const host = document.getElementById("page-editor");
  if (!host) return;

  const state = {
    kind: "watchlist",
    snapshot: "",
    items: {},
    rows: [],
    filter: "",
    loading: false,
    saving: false,
    snapshots: [],
    hasChanges: false,
    page: 0,
    typeFilter: { movie: true, show: true, episode: true },
    sortKey: "title",
    sortDir: "asc",
  };

  function restoreUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const saved = JSON.parse(raw);
      const kinds = ["watchlist", "history", "ratings"];
      if (saved.kind && kinds.includes(saved.kind)) state.kind = saved.kind;
      if (typeof saved.snapshot === "string") state.snapshot = saved.snapshot;
      if (typeof saved.filter === "string") state.filter = saved.filter;
      if (saved.typeFilter && typeof saved.typeFilter === "object") {
        ["movie", "show", "episode"].forEach(t => {
          if (typeof saved.typeFilter[t] === "boolean") state.typeFilter[t] = saved.typeFilter[t];
        });
      }
      const sortKeys = ["title", "type", "key", "extra"];
      if (saved.sortKey && sortKeys.includes(saved.sortKey)) state.sortKey = saved.sortKey;
      if (saved.sortDir === "asc" || saved.sortDir === "desc") state.sortDir = saved.sortDir;
    } catch (_) {}
  }

  restoreUIState();

  host.innerHTML = `
    <div class="cw-root">
      <div class="cw-topline">
        <div class="title">CrossWatch tracker editor</div>
      </div>

      <div class="cw-wrap">
        <div class="cw-main">
          <div class="cw-controls">
            <input id="cw-filter" class="cw-input" placeholder="Filter by key / title / id...">
            <span class="cw-status-text" id="cw-status"></span>
            <div class="cw-controls-spacer"></div>
            <button id="cw-reload" class="cw-btn" type="button">Reload</button>
            <button id="cw-add" class="cw-btn" type="button">Add row</button>
            <button id="cw-save" class="cw-btn primary" type="button">Save changes</button>
          </div>

          <div class="cw-table-wrap" id="cw-table-wrap">
            <table class="cw-table">
                <thead>
                <tr>
                    <th style="width:30px"></th>
                    <th style="width:12%" data-sort="key" class="sortable">Key</th>
                    <th style="width:10%" data-sort="type" class="sortable">Type</th>
                    <th style="width:24%" data-sort="title" class="sortable">Title</th>
                    <th style="width:6%">Year</th>
                    <th style="width:10%">IMDb</th>
                    <th style="width:10%">TMDB</th>
                    <th style="width:10%">Trakt</th>
                    <th style="width:16%" data-sort="extra" class="sortable">Extra</th>
                </tr>
                </thead>
              <tbody id="cw-tbody"></tbody>
            </table>
          </div>

          <div class="cw-pager" id="cw-pager" style="display:none">
            <button id="cw-prev" class="cw-btn" type="button">Previous</button>
            <span id="cw-page-info" class="cw-page-info"></span>
            <button id="cw-next" class="cw-btn" type="button">Next</button>
          </div>

          <div class="cw-empty" id="cw-empty" style="display:none">No items</div>
        </div>

        <aside class="cw-side">
          <div class="ins-card">
            <div class="ins-row">
              <div class="ins-icon"><span class="material-symbol">tune</span></div>
              <div class="ins-title">Editor filters</div>
            </div>
            <div class="ins-row">
              <div class="ins-kv" style="width:100%">
                <label>Kind</label>
                <select id="cw-kind" class="cw-select">
                  <option value="watchlist">Watchlist</option>
                  <option value="history">History</option>
                  <option value="ratings">Ratings</option>
                </select>

                <label>Snapshot</label>
                <select id="cw-snapshot" class="cw-select">
                  <option value="">Latest</option>
                </select>
              </div>
            </div>
            <div class="ins-row">
              <div class="ins-kv" style="width:100%">
                <label>Types</label>
                <div id="cw-type-filter" class="cw-type-filter">
                  <button type="button" data-type="movie" class="cw-type-chip active">Movies</button>
                  <button type="button" data-type="show" class="cw-type-chip active">Shows</button>
                  <button type="button" data-type="episode" class="cw-type-chip active">Episodes</button>
                </div>
              </div>
            </div>
          </div>

          <div class="ins-card">
            <div class="ins-row" style="align-items:center">
              <div class="ins-icon"><span class="material-symbol">insights</span></div>
              <div class="ins-title" style="margin-right:auto">State</div>
              <span class="cw-tag" id="cw-tag-status">
                <span class="cw-tag-dot"></span>
                <span id="cw-tag-label">Idle</span>
              </span>
            </div>
            <div class="ins-row">
              <div class="ins-metrics">
                <div class="metric-row">
                  <div class="metric">
                    <span class="material-symbol">view_list</span>
                    <div>
                      <div class="m-val" id="cw-summary-total">0</div>
                      <div class="m-lbl">Total rows</div>
                    </div>
                  </div>
                  <div class="metric">
                    <span class="material-symbol">visibility</span>
                    <div>
                      <div class="m-val" id="cw-summary-visible">0</div>
                      <div class="m-lbl">Rows visible</div>
                    </div>
                  </div>
                </div>
                <div class="metric-divider"></div>
                <div class="metric-row">
                  <div class="metric">
                    <span class="material-symbol">movie</span>
                    <div>
                      <div class="m-val" id="cw-summary-movies">0</div>
                      <div class="m-lbl">Movies</div>
                    </div>
                  </div>
                  <div class="metric">
                    <span class="material-symbol">monitoring</span>
                    <div>
                      <div class="m-val" id="cw-summary-shows">0</div>
                      <div class="m-lbl">Shows</div>
                    </div>
                  </div>
                  <div class="metric">
                    <span class="material-symbol">live_tv</span>
                    <div>
                      <div class="m-val" id="cw-summary-episodes">0</div>
                      <div class="m-lbl">Episodes</div>
                    </div>
                  </div>
                </div>
                <div class="metric-divider"></div>
                <div class="metric-row">
                  <div class="metric">
                    <span class="material-symbol">description</span>
                    <div>
                      <div class="m-val" id="cw-summary-state-files">0</div>
                      <div class="m-lbl">State files</div>
                    </div>
                  </div>
                  <div class="metric">
                    <span class="material-symbol">folder_copy</span>
                    <div>
                      <div class="m-val" id="cw-summary-snapshots">0</div>
                      <div class="m-lbl">Snapshots</div>
                    </div>
                  </div>
                </div>
                <div id="cw-state-hint" class="cw-state-hint" style="display:none">
                  <strong>No tracker data found.</strong> Run a CrossWatch sync with the tracker enabled once. After that, tracker state files and snapshots will appear here and you can edit them.
                </div>
              </div>
            </div>
          </div>

          <div class="ins-card">
            <div class="ins-row">
              <div class="ins-icon"><span class="material-symbol">backup</span></div>
              <div class="ins-title">Backup</div>
            </div>
            <div class="ins-row">
              <div class="ins-kv" style="width:100%">
                <label>Export / Import</label>
                <div class="cw-backup-actions">
                    <button id="cw-download" class="cw-btn" type="button">Download ZIP</button>
                    <button id="cw-upload" class="cw-btn" type="button">Import file</button>
                    <input id="cw-upload-input" type="file" accept=".zip,.json" style="display:none">
              </div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  `;

  const $ = id => document.getElementById(id);
  const kindSel = $("cw-kind");
  const snapSel = $("cw-snapshot");
  const filterInput = $("cw-filter");
  const reloadBtn = $("cw-reload");
  const addBtn = $("cw-add");
  const saveBtn = $("cw-save");
  const tbody = $("cw-tbody");
  const empty = $("cw-empty");
  const statusEl = $("cw-status");
  const tag = $("cw-tag-status");
  const tagLabel = $("cw-tag-label");
  const summaryVisible = $("cw-summary-visible");
  const summaryTotal = $("cw-summary-total");
  const summaryMovies = $("cw-summary-movies");
  const summaryShows = $("cw-summary-shows");
  const summaryEpisodes = $("cw-summary-episodes");
  const summaryStateFiles = $("cw-summary-state-files");
  const summarySnapshots = $("cw-summary-snapshots");
  const stateHint = $("cw-state-hint");
  const pager = $("cw-pager");
  const prevBtn = $("cw-prev");
  const nextBtn = $("cw-next");
  const pageInfo = $("cw-page-info");
  const typeFilterWrap = $("cw-type-filter");
  const downloadBtn = $("cw-download");
  const uploadBtn = $("cw-upload");
  const uploadInput = $("cw-upload-input");
  const sortHeaders = Array.from(host.querySelectorAll(".cw-table th[data-sort]"));

  let statusStickyUntil = 0;

  function setStatus(message) {
    if (!statusEl) return;
    statusEl.textContent = message || "";
  }

  function setStatusSticky(message, ms = 4000) {
    statusStickyUntil = Date.now() + ms;
    setStatus(message);
  }

  function setRowsStatus(message) {
    if (Date.now() < statusStickyUntil) return;
    setStatus(message);
  }

  if (filterInput && state.filter) {
    filterInput.value = state.filter;
  }

  function syncKindUI() {
    if (!kindSel) return;
    const allowed = ["watchlist", "history", "ratings"];
    if (!allowed.includes(state.kind)) state.kind = "watchlist";
    if (!allowed.includes(kindSel.value)) kindSel.value = state.kind;
  }

  function syncTypeFilterUI() {
    if (!typeFilterWrap) return;
    const buttons = typeFilterWrap.querySelectorAll("button[data-type]");
    buttons.forEach(btn => {
      const t = btn.dataset.type;
      const on = state.typeFilter[t] !== false;
      btn.classList.toggle("active", on);
    });
  }

  syncKindUI();
  syncTypeFilterUI();

  function persistUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const data = {
        kind: state.kind,
        snapshot: state.snapshot,
        filter: state.filter,
        typeFilter: state.typeFilter,
        sortKey: state.sortKey,
        sortDir: state.sortDir,
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch (_) {}
  }

  function setTag(mode, label) {
    tag.classList.remove("warn", "error");
    if (mode === "warn") tag.classList.add("warn");
    if (mode === "error") tag.classList.add("error");
    tagLabel.textContent = label;
  }

  function markChanged() {
    state.hasChanges = true;
    setTag("warn", "Unsaved changes");
  }

  let activePopup = null;

  function closePopup() {
    if (!activePopup) return;
    document.removeEventListener("mousedown", activePopup.onDoc);
    document.removeEventListener("keydown", activePopup.onKey);
    if (activePopup.node && activePopup.node.parentNode) {
      activePopup.node.parentNode.removeChild(activePopup.node);
    }
    activePopup = null;
  }

  function positionPopup(pop, anchor) {
    const rect = anchor.getBoundingClientRect();
    const margin = 8;
    const viewportWidth = document.documentElement.clientWidth;
    const viewportHeight = document.documentElement.clientHeight;
    let left = rect.left + window.scrollX;
    let top = rect.bottom + margin + window.scrollY;
    const width = pop.offsetWidth;
    const height = pop.offsetHeight;
    if (left + width + margin > window.scrollX + viewportWidth) {
      left = window.scrollX + viewportWidth - width - margin;
    }
    if (top + height + margin > window.scrollY + viewportHeight) {
      top = rect.top + window.scrollY - height - margin;
    }
    if (left < margin) left = margin;
    if (top < margin) top = margin;
    pop.style.left = left + "px";
    pop.style.top = top + "px";
  }

  function openPopup(anchor, builder) {
    closePopup();
    const pop = document.createElement("div");
    pop.className = "cw-pop";
    document.body.appendChild(pop);

    function doClose() {
      closePopup();
    }

    builder(pop, doClose);
    positionPopup(pop, anchor);

    const onDoc = ev => {
      if (pop.contains(ev.target) || anchor.contains(ev.target)) return;
      closePopup();
    };
    const onKey = ev => {
      if (ev.key === "Escape") closePopup();
    };
    activePopup = { node: pop, onDoc, onKey };
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
  }

  function formatHistoryLabel(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    const pad = n => String(n).padStart(2, "0");
    return (
      d.getFullYear() +
      "-" +
      pad(d.getMonth() + 1) +
      "-" +
      pad(d.getDate()) +
      " " +
      pad(d.getHours()) +
      ":" +
      pad(d.getMinutes())
    );
  }

  function updateExtraDisplay(row, el) {
    let label = "";
    let placeholder = "";
    let icon = "";
    if (state.kind === "ratings") {
      icon = "star";
      const r = row.raw && row.raw.rating;
      if (r == null || r === "") {
        placeholder = "Set rating";
      } else {
        label = String(r) + "/10";
      }
    } else if (state.kind === "history") {
      icon = "schedule";
      const w = row.raw && row.raw.watched_at;
      if (!w) {
        placeholder = "Set time";
      } else {
        label = formatHistoryLabel(w);
      }
    } else {
      placeholder = "";
    }
    el.innerHTML = "";
    const text = document.createElement("span");
    text.className = "cw-extra-display-label";
    if (label) {
      text.textContent = label;
      text.classList.add("cw-extra-display-value");
    } else {
      text.textContent = placeholder || "";
      text.classList.add("cw-extra-display-placeholder");
    }
    el.appendChild(text);
    if (icon) {
      const iconEl = document.createElement("span");
      iconEl.className = "material-symbol cw-extra-display-icon";
      iconEl.textContent = icon;
      el.appendChild(iconEl);
    }
  }

  function updateTypeDisplay(row, el) {
    let label = "";
    let icon = "category";
    const t = (row.type || "").toLowerCase();
    if (t === "movie") {
      label = "Movie";
      icon = "movie";
    } else if (t === "show") {
      label = "Show";
      icon = "monitoring";
    } else if (t === "episode") {
      label = "Episode";
      icon = "live_tv";
    }
    el.innerHTML = "";
    const text = document.createElement("span");
    text.className = "cw-extra-display-label";
    if (label) {
      text.textContent = label;
      text.classList.add("cw-extra-display-value");
    } else {
      text.textContent = "Set type";
      text.classList.add("cw-extra-display-placeholder");
    }
    el.appendChild(text);
    const iconEl = document.createElement("span");
    iconEl.className = "material-symbol cw-extra-display-icon";
    iconEl.textContent = icon;
    el.appendChild(iconEl);
  }

  function buildRows(items) {
    const rows = [];
    for (const [key, raw] of Object.entries(items || {})) {
      const ids = raw.ids || {};
      const showIds = raw.show_ids || {};
      const type = raw.type || "";
      const isEpisode = type === "episode";
      const baseTitle = raw.title || raw.series_title || "";
      rows.push({
        key,
        type,
        title: baseTitle,
        year: raw.year != null ? String(raw.year || "") : "",
        imdb: ids.imdb || "",
        tmdb: ids.tmdb || showIds.tmdb || "",
        trakt: ids.trakt || showIds.trakt || "",
        raw: JSON.parse(JSON.stringify(raw)),
        deleted: false,
        episode: isEpisode,
      });
    }
    rows.sort((a, b) => (a.title || "").localeCompare(b.title || ""));
    return rows;
  }

  function applyFilter(rows) {
    const q = (state.filter || "").trim().toLowerCase();
    const filters = state.typeFilter || {};
    const hasTypeFilter = filters.movie || filters.show || filters.episode;
    return rows.filter(r => {
      if (hasTypeFilter) {
        const t = (r.type || "").toLowerCase();
        const known = t === "movie" || t === "show" || t === "episode";
        let allowed = true;
        if (known) {
          if (t === "movie") allowed = !!filters.movie;
          else if (t === "show") allowed = !!filters.show;
          else if (t === "episode") allowed = !!filters.episode;
        }
        if (!allowed) return false;
      }
      if (!q) return true;
      const parts = [
        r.key,
        r.title,
        r.type,
        r.year,
        r.imdb,
        r.tmdb,
        r.trakt,
        r.raw && r.raw.series_title ? r.raw.series_title : "",
      ]
        .join(" ")
        .toLowerCase();
      return parts.includes(q);
    });
  }

  function openHistoryEditor(row, anchor, displayEl) {
    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Watched at";
      pop.appendChild(title);

      const grid = document.createElement("div");
      grid.className = "cw-datetime-grid";

      const dateInput = document.createElement("input");
      dateInput.type = "date";

      const timeInput = document.createElement("input");
      timeInput.type = "time";
      timeInput.step = 60;

      const current = row.raw && row.raw.watched_at;
      if (current) {
        const d = new Date(current);
        if (!Number.isNaN(d.getTime())) {
          const iso = d.toISOString();
          dateInput.value = iso.slice(0, 10);
          timeInput.value = iso.slice(11, 16);
        }
      }

      grid.appendChild(dateInput);
      grid.appendChild(timeInput);
      pop.appendChild(grid);

      const actions = document.createElement("div");
      actions.className = "cw-pop-actions";

      const clearBtn = document.createElement("button");
      clearBtn.type = "button";
      clearBtn.className = "cw-pop-btn ghost";
      clearBtn.textContent = "Clear";
      clearBtn.onclick = () => {
        row.raw.watched_at = null;
        updateExtraDisplay(row, displayEl);
        markChanged();
        close();
      };

      const saveBtn = document.createElement("button");
      saveBtn.type = "button";
      saveBtn.className = "cw-pop-btn primary";
      saveBtn.textContent = "Save";
      saveBtn.onclick = () => {
        const dv = dateInput.value;
        const tv = timeInput.value;
        if (!dv) {
          row.raw.watched_at = null;
        } else {
          const parts = dv.split("-");
          const y = parseInt(parts[0], 10);
          const m = parseInt(parts[1], 10);
          const dDay = parseInt(parts[2], 10);
          let hh = 0;
          let mm = 0;
          if (tv) {
            const tparts = tv.split(":");
            hh = parseInt(tparts[0], 10) || 0;
            mm = parseInt(tparts[1], 10) || 0;
          }
          const dt = new Date(Date.UTC(y, m - 1, dDay, hh, mm, 0));
          let iso = dt.toISOString();
          iso = iso.replace(/\.\d{3}Z$/, ".000Z");
          row.raw.watched_at = iso;
        }
        updateExtraDisplay(row, displayEl);
        markChanged();
        close();
      };

      actions.appendChild(clearBtn);
      actions.appendChild(saveBtn);
      pop.appendChild(actions);

      dateInput.focus();
    });
  }

  function openRatingEditor(row, anchor, displayEl) {
    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Rating";
      pop.appendChild(title);

      const grid = document.createElement("div");
      grid.className = "cw-rating-grid";
      const current = row.raw && row.raw.rating != null ? Number(row.raw.rating) : null;

      for (let i = 1; i <= 10; i += 1) {
        const pill = document.createElement("button");
        pill.type = "button";
        pill.className = "cw-rating-pill" + (current === i ? " active" : "");
        pill.textContent = String(i);
        pill.onclick = () => {
          row.raw.rating = i;
          updateExtraDisplay(row, displayEl);
          markChanged();
          close();
        };
        grid.appendChild(pill);
      }

      pop.appendChild(grid);

      const actions = document.createElement("div");
      actions.className = "cw-pop-actions";

      const clearBtn = document.createElement("button");
      clearBtn.type = "button";
      clearBtn.className = "cw-pop-btn ghost";
      clearBtn.textContent = "Clear";
      clearBtn.onclick = () => {
        row.raw.rating = null;
        updateExtraDisplay(row, displayEl);
        markChanged();
        close();
      };

      actions.appendChild(clearBtn);
      pop.appendChild(actions);
    });
  }

  function openTypeEditor(row, anchor) {
    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Type";
      pop.appendChild(title);

      const grid = document.createElement("div");
      grid.className = "cw-type-grid";
      const current = (row.type || "").toLowerCase();
      const options = [
        { key: "movie", label: "Movie" },
        { key: "show", label: "Show" },
        { key: "episode", label: "Episode" },
      ];

      options.forEach(opt => {
        const pill = document.createElement("button");
        pill.type = "button";
        pill.className = "cw-type-pill" + (current === opt.key ? " active" : "");
        pill.textContent = opt.label;
        pill.onclick = () => {
          row.type = opt.key;
          row.raw.type = opt.key;
          row.episode = opt.key === "episode";
          markChanged();
          close();
          renderRows();
        };
        grid.appendChild(pill);
      });

      pop.appendChild(grid);

      const actions = document.createElement("div");
      actions.className = "cw-pop-actions";

      const clearBtn = document.createElement("button");
      clearBtn.type = "button";
      clearBtn.className = "cw-pop-btn ghost";
      clearBtn.textContent = "Clear";
      clearBtn.onclick = () => {
        row.type = "";
        row.raw.type = null;
        row.episode = false;
        markChanged();
        close();
        renderRows();
      };

      actions.appendChild(clearBtn);
      pop.appendChild(actions);
    });
  }

  function compareValues(aVal, bVal) {
    if (typeof aVal === "number" && typeof bVal === "number") {
      if (aVal < bVal) return -1;
      if (aVal > bVal) return 1;
      return 0;
    }
    const aStr = aVal == null ? "" : String(aVal).toLowerCase();
    const bStr = bVal == null ? "" : String(bVal).toLowerCase();
    if (aStr < bStr) return -1;
    if (aStr > bStr) return 1;
    return 0;
  }

  function sortRows(rows) {
    const key = state.sortKey;
    const dir = state.sortDir === "desc" ? -1 : 1;
    if (!key) return rows;
    const sorted = rows.slice().sort((a, b) => {
      let av;
      let bv;
      if (key === "title") {
        av = a.title || "";
        bv = b.title || "";
      } else if (key === "type") {
        av = a.type || "";
        bv = b.type || "";
      } else if (key === "key") {
        av = a.key || "";
        bv = b.key || "";
      } else if (key === "extra") {
        if (state.kind === "ratings") {
          av = a.raw && a.raw.rating != null ? Number(a.raw.rating) : -Infinity;
          bv = b.raw && b.raw.rating != null ? Number(b.raw.rating) : -Infinity;
        } else if (state.kind === "history") {
          const aw = a.raw && a.raw.watched_at;
          const bw = b.raw && b.raw.watched_at;
          const at = aw ? Date.parse(aw) || 0 : 0;
          const bt = bw ? Date.parse(bw) || 0 : 0;
          av = at;
          bv = bt;
        } else {
          av = "";
          bv = "";
        }
      } else {
        av = "";
        bv = "";
      }
      return compareValues(av, bv) * dir;
    });
    return sorted;
  }

  function updateSortUI() {
    sortHeaders.forEach(th => {
      const k = th.dataset.sort;
      th.classList.remove("sort-asc", "sort-desc");
      if (k === state.sortKey) {
        th.classList.add(state.sortDir === "desc" ? "sort-desc" : "sort-asc");
      }
    });
  }

  function renderRows() {
    closePopup();
    updateSortUI();
    let filtered = applyFilter(state.rows);
    const totalFiltered = filtered.length;
    const totalAll = state.rows.length;

    filtered = sortRows(filtered);

    let movies = 0;
    let shows = 0;
    let episodes = 0;
    for (const row of state.rows) {
      const t = (row.type || "").toLowerCase();
      if (t === "movie") movies += 1;
      else if (t === "show") shows += 1;
      else if (t === "episode") episodes += 1;
    }
    if (summaryMovies) summaryMovies.textContent = String(movies);
    if (summaryShows) summaryShows.textContent = String(shows);
    if (summaryEpisodes) summaryEpisodes.textContent = String(episodes);

    tbody.innerHTML = "";

    if (!totalFiltered) {
      empty.style.display = "block";
      if (pager) pager.style.display = "none";
      if (summaryVisible) summaryVisible.textContent = "0";
      if (summaryTotal) summaryTotal.textContent = String(totalAll || 0);
      setStatus("0 rows visible");
      if (pageInfo) pageInfo.textContent = "";
      return;
    }

    empty.style.display = "none";

    const pageCount = Math.max(1, Math.ceil(totalFiltered / PAGE_SIZE));
    if (state.page >= pageCount) state.page = pageCount - 1;
    if (state.page < 0) state.page = 0;

    const start = state.page * PAGE_SIZE;
    const end = start + PAGE_SIZE;
    const rows = filtered.slice(start, end);

    const frag = document.createDocumentFragment();
    rows.forEach(row => {
      const tr = document.createElement("tr");
      if (row.episode) tr.classList.add("cw-row-episode");
      if (row.deleted) tr.classList.add("cw-row-deleted");

      const cell = inner => {
        const td = document.createElement("td");
        td.appendChild(inner);
        return td;
      };

      const delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "cw-btn cw-btn-del danger";
      delBtn.innerHTML = '<span class="material-symbol">delete</span>';
      delBtn.title = "Delete row";
      delBtn.onclick = () => {
        row.deleted = !row.deleted;
        markChanged();
        renderRows();
      };
      tr.appendChild(cell(delBtn));

      const keyIn = document.createElement("input");
      keyIn.value = row.key || "";
      keyIn.className = "cw-key";
      keyIn.oninput = e => {
        row.key = e.target.value;
        markChanged();
      };
      tr.appendChild(cell(keyIn));

      const typeBtn = document.createElement("button");
      typeBtn.type = "button";
      typeBtn.className = "cw-extra-display";
      updateTypeDisplay(row, typeBtn);
      typeBtn.onclick = () => {
        openTypeEditor(row, typeBtn);
      };
      tr.appendChild(cell(typeBtn));

      const titleIn = document.createElement("input");
      titleIn.value = row.title || "";
      titleIn.oninput = e => {
        row.title = e.target.value;
        row.raw.title = e.target.value || null;
        markChanged();
      };
      tr.appendChild(cell(titleIn));

      const yearIn = document.createElement("input");
      yearIn.value = row.year || "";
      yearIn.oninput = e => {
        row.year = e.target.value;
        const v = e.target.value.trim();
        row.raw.year = v ? parseInt(v, 10) || null : null;
        markChanged();
      };
      tr.appendChild(cell(yearIn));

      const imdbIn = document.createElement("input");
      imdbIn.value = row.imdb || "";
      imdbIn.oninput = e => {
        row.imdb = e.target.value;
        row.raw.ids = row.raw.ids || {};
        row.raw.ids.imdb = e.target.value || undefined;
        markChanged();
      };
      tr.appendChild(cell(imdbIn));

      const tmdbIn = document.createElement("input");
      tmdbIn.value = row.tmdb || "";
      tmdbIn.oninput = e => {
        row.tmdb = e.target.value;
        row.raw.ids = row.raw.ids || {};
        row.raw.ids.tmdb = e.target.value || undefined;
        markChanged();
      };
      tr.appendChild(cell(tmdbIn));

      const traktIn = document.createElement("input");
      traktIn.value = row.trakt || "";
      traktIn.oninput = e => {
        row.trakt = e.target.value;
        row.raw.ids = row.raw.ids || {};
        row.raw.ids.trakt = e.target.value || undefined;
        markChanged();
      };
      tr.appendChild(cell(traktIn));

      const extraBtn = document.createElement("button");
      extraBtn.type = "button";
      extraBtn.className = "cw-extra-display";
      updateExtraDisplay(row, extraBtn);
      if (state.kind === "ratings") {
        extraBtn.onclick = () => {
          openRatingEditor(row, extraBtn, extraBtn);
        };
      } else if (state.kind === "history") {
        extraBtn.onclick = () => {
          openHistoryEditor(row, extraBtn, extraBtn);
        };
      } else {
        extraBtn.disabled = true;
        extraBtn.style.opacity = "0.6";
        extraBtn.style.cursor = "default";
      }
      tr.appendChild(cell(extraBtn));

      frag.appendChild(tr);
    });
    tbody.appendChild(frag);

    const vis = rows.length;
    const first = start + 1;
    const last = start + vis;

    if (summaryVisible) summaryVisible.textContent = String(vis);
    if (summaryTotal) summaryTotal.textContent = String(totalAll);

    if (pageInfo) {
      pageInfo.textContent = `Page ${state.page + 1} of ${pageCount} • Rows ${first}-${last} of ${totalFiltered}`;
    }

    if (pager) {
      pager.style.display = pageCount > 1 ? "flex" : "none";
    }
    if (prevBtn) prevBtn.disabled = state.page <= 0;
    if (nextBtn) nextBtn.disabled = state.page >= pageCount - 1;

    if (totalFiltered > vis) {
      setRowsStatus(
        `${vis} rows visible (rows ${first}-${last} of ${totalFiltered} filtered, ${totalAll} total)`
      );
    } else {
      setRowsStatus(`${vis} rows visible, ${totalAll} total`);
    }
}

  function formatSnapshotLabel(s) {
    if (s && typeof s.ts === "number" && s.ts > 0) {
      const d = new Date(s.ts * 1000);
      const pad = n => String(n).padStart(2, "0");
      return (
        d.getFullYear() +
        "-" +
        pad(d.getMonth() + 1) +
        "-" +
        pad(d.getDate()) +
        " - " +
        pad(d.getHours()) +
        ":" +
        pad(d.getMinutes())
      );
    }
    if (s && s.name) return s.name;
    return "Snapshot";
  }

  function rebuildSnapshots() {
    const options = (state.snapshots || [])
      .map(s => {
        const label = formatSnapshotLabel(s);
        return `<option value="${s.name}">${label}</option>`;
      })
      .join("");
    snapSel.innerHTML = `<option value="">Latest</option>` + options;
    snapSel.value = state.snapshot || "";
  }

  async function fetchJSON(url, opts) {
    const res = await fetch(url, Object.assign({ cache: "no-store" }, opts || {}));
    if (!res.ok) throw new Error(`Request failed: ${res.status}`);
    return await res.json();
  }

  async function loadSnapshots() {
    try {
      const data = await fetchJSON(`/api/editor/snapshots?kind=${encodeURIComponent(state.kind)}`);
      state.snapshots = Array.isArray(data.snapshots) ? data.snapshots : [];
      rebuildSnapshots();
    } catch (e) {
      console.error(e);
    }
  }

  async function loadTrackerCounts() {
    try {
      const data = await fetchJSON("/api/maintenance/crosswatch-tracker");
      const counts = data && data.counts ? data.counts : {};
      const stateFiles = counts.state_files != null ? counts.state_files : 0;
      const snaps = counts.snapshots != null ? counts.snapshots : 0;
      if (summaryStateFiles) summaryStateFiles.textContent = String(stateFiles);
      if (summarySnapshots) summarySnapshots.textContent = String(snaps);
      if (stateHint) {
        if (stateFiles === 0 && snaps === 0) {
          stateHint.style.display = "block";
        } else {
          stateHint.style.display = "none";
        }
      }
    } catch (e) {
      console.error(e);
    }
  }

  async function loadState() {
    state.loading = true;
    setTag("warn", "Loading…");
    try {
      const params = new URLSearchParams({ kind: state.kind });
      if (state.snapshot) params.set("snapshot", state.snapshot);
      const data = await fetchJSON(`/api/editor?${params.toString()}`);
      state.items = data.items || {};
      state.rows = buildRows(state.items);
      state.hasChanges = false;
      state.page = 0;
      renderRows();
      setTag("warn", "Loaded");
    } catch (e) {
      console.error(e);
      setTag("error", "Load failed");
      setStatus(String(e));
    } finally {
      state.loading = false;
    }
  }

  function findRowsMissingKey() {
    const missing = [];
    for (const row of state.rows) {
      if (row.deleted) continue;
      const key = (row.key || "").trim();
      if (key) continue;
      const hasOther =
        (row.title && row.title.trim()) ||
        (row.type && row.type.trim()) ||
        (row.year && String(row.year).trim()) ||
        (row.imdb && row.imdb.trim()) ||
        (row.tmdb && row.tmdb.trim()) ||
        (row.trakt && row.trakt.trim());
      if (hasOther) missing.push(row);
    }
    return missing;
  }

  async function saveState() {
    if (state.saving) return;

    const missing = findRowsMissingKey();
    if (missing.length) {
      setTag("error", "Missing key");
      setStatus(`Cannot save: ${missing.length} row${missing.length === 1 ? "" : "s"} have data but no Key. Fill the Key or delete the row.`);
      if (window.cxToast) window.cxToast("Fill Key for all rows with data before saving");
      return;
    }

    state.saving = true;
    setTag("warn", "Saving…");
    saveBtn.disabled = true;
    try {
      const items = {};
      for (const row of state.rows) {
        if (row.deleted) continue;
        const key = (row.key || "").trim();
        if (!key) continue;
        const raw = Object.assign({}, row.raw);
        const ids = Object.assign({}, raw.ids || {});
        if (row.imdb) ids.imdb = row.imdb;
        if (row.tmdb) ids.tmdb = row.tmdb;
        if (row.trakt) ids.trakt = row.trakt;
        raw.ids = ids;
        raw.type = row.type || raw.type || null;
        if (row.title) raw.title = row.title;
        const y = (row.year || "").trim();
        raw.year = y ? parseInt(y, 10) || null : null;
        items[key] = raw;
      }
      const payload = { kind: state.kind, items };
      const res = await fetchJSON("/api/editor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      state.hasChanges = false;
      setTag("warn", "Saved");
      setStatus(`Saved ${res.count || Object.keys(items).length} items`);
      await loadSnapshots();
    } catch (e) {
      console.error(e);
      setTag("error", "Save failed");
      setStatus(String(e));
    } finally {
      state.saving = false;
      saveBtn.disabled = false;
    }
  }

  function addRow() {
    const raw = { ids: {}, type: "movie", title: "", year: null };
    state.rows.unshift({
      key: "",
      type: raw.type,
      title: "",
      year: "",
      imdb: "",
      tmdb: "",
      trakt: "",
      raw,
      deleted: false,
      episode: false,
    });
    state.page = 0;
    markChanged();
    renderRows();
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", () => {
      if (state.page <= 0) return;
      state.page -= 1;
      renderRows();
    });
  }
  if (nextBtn) {
    nextBtn.addEventListener("click", () => {
      const filteredCount = applyFilter(state.rows).length;
      const pageCount = Math.max(1, Math.ceil(filteredCount / PAGE_SIZE));
      if (state.page >= pageCount - 1) return;
      state.page += 1;
      renderRows();
    });
  }

  sortHeaders.forEach(th => {
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (!key) return;
      if (state.sortKey === key) {
        state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = key;
        state.sortDir = "asc";
      }
      persistUIState();
      renderRows();
    });
  });

  if (typeFilterWrap) {
    typeFilterWrap.addEventListener("click", e => {
      const btn = e.target.closest("button[data-type]");
      if (!btn) return;
      const t = btn.dataset.type;
      const current = !!state.typeFilter[t];
      if (current) {
        const enabledCount = Object.values(state.typeFilter).filter(Boolean).length;
        if (enabledCount <= 1) return;
      }
      state.typeFilter[t] = !current;
      syncTypeFilterUI();
      state.page = 0;
      persistUIState();
      renderRows();
    });
  }

  if (downloadBtn) {
    downloadBtn.addEventListener("click", async () => {
      try {
        setTag("warn", "Preparing download…");
        const res = await fetch("/api/editor/export", { cache: "no-store" });
        if (!res.ok) throw new Error(`Download failed: ${res.status}`);
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "crosswatch-tracker.zip";
        document.body.appendChild(a);
        a.click();
        setTimeout(() => {
          URL.revokeObjectURL(url);
          a.remove();
        }, 0);
        setTag("warn", "Loaded");
        if (window.cxToast) window.cxToast("Tracker export downloaded");
      } catch (e) {
        console.error(e);
        setTag("error", "Download failed");
        setStatus(String(e));
      }
    });
  }

  if (uploadBtn && uploadInput) {
    uploadBtn.addEventListener("click", () => {
      uploadInput.click();
    });

    uploadInput.addEventListener("change", async () => {
      const file = uploadInput.files && uploadInput.files[0];
      if (!file) return;

      try {
        const fd = new FormData();
        fd.append("file", file);
        setTag("warn", "Importing…");
        setStatus("");
        const res = await fetch("/api/editor/import", { method: "POST", body: fd });
        if (!res.ok) {
          let msg = `Import failed: ${res.status}`;
          try {
            const err = await res.json();
            if (err && err.detail) msg += ` – ${err.detail}`;
          } catch (_) {}
          throw new Error(msg);
        }

        const data = await res.json();

        const parts = [];
        if (data.files != null) {
          parts.push(`${data.files} file${data.files === 1 ? "" : "s"}`);
        }
        if (data.states != null) {
          parts.push(`${data.states} state file${data.states === 1 ? "" : "s"}`);
        }
        if (data.snapshots != null) {
          parts.push(`${data.snapshots} snapshot${data.snapshots === 1 ? "" : "s"}`);
        }

        let msg = "Imported " + (parts.length ? parts.join(", ") : "tracker data");
        if (data.overwritten) {
          msg += ` (${data.overwritten} overwritten)`;
        }

        setTag("warn", "Loaded");
        setStatusSticky(msg, 5000);
        if (window.cxToast) {
        window.cxToast(msg);
        }

        await loadTrackerCounts();
        await loadSnapshots();
        await loadState();
      } catch (e) {
        console.error(e);
        setTag("error", "Import failed");
        setStatus(String(e));
      } finally {
        uploadInput.value = "";
      }
    });
  }

  kindSel.addEventListener("change", async () => {

    state.kind = (kindSel.value || "watchlist").trim();
    state.snapshot = "";
    state.page = 0;
    persistUIState();
    await loadSnapshots();
    renderRows();
    await loadState();
  });

  snapSel.addEventListener("change", async () => {
    state.snapshot = snapSel.value || "";
    state.page = 0;
    persistUIState();
    await loadState();
  });

  filterInput.addEventListener("input", () => {
    state.filter = filterInput.value || "";
    state.page = 0;
    persistUIState();
    renderRows();
  });

  reloadBtn.addEventListener("click", () => {
    state.snapshot = snapSel.value || "";
    state.page = 0;
    loadState();
  });

  addBtn.addEventListener("click", addRow);
  saveBtn.addEventListener("click", saveState);

  window.addEventListener("beforeunload", e => {
    if (!state.hasChanges) return;
    e.preventDefault();
    e.returnValue = "";
  });

  setStatus("Loading CrossWatch tracker state…");
  loadTrackerCounts();
  loadSnapshots().then(loadState);
})();