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

.cw-title-cell{display:flex;flex-direction:column;align-items:stretch;gap:4px}
.cw-title-row{display:flex;align-items:center;gap:4px}
.cw-title-sub{font-size:12px;opacity:.75;line-height:1.1;padding-left:2px}
.cw-title-cell input{flex:1 1 auto}
.cw-title-search-btn{
  flex:0 0 auto;
  width:26px;
  height:26px;
  border-radius:999px;
  border:1px solid rgba(148,163,184,.7);
  background:#020617;
  color:#e5e7eb;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  cursor:pointer;
  padding:0;
  font-size:15px;
}
.cw-title-search-btn .material-symbol{font-size:16px}
/* search popup + results */
.cw-pop{
  position:fixed;
  z-index:10060;
  background:linear-gradient(180deg,#05060c,#0a0d16);
  border-radius:16px;
  border:1px solid rgba(255,255,255,.10);
  box-shadow:0 22px 60px rgba(0,0,0,.75),inset 0 0 0 1px rgba(255,255,255,.04);
  padding:10px 12px 12px;
  color:#e5e7eb;

  width:min(560px,calc(100vw - 28px));
  max-height:calc(100vh - 120px);
  overflow:hidden;

  display:flex;
  flex-direction:column;
}

.cw-pop-title{
  font-size:11px;
  font-weight:600;
  margin-bottom:4px;
  letter-spacing:.08em;
  text-transform:uppercase;
  opacity:.8;
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
  background:radial-gradient(circle at 0 0,rgba(15,23,42,.9),rgba(15,23,42,.98));
  padding:4px 10px;
  font-size:12px;
  color:#e5e7eb;
  cursor:pointer;
}
.cw-pop-btn.primary{
  border-color:#4f46e5;
  background:linear-gradient(135deg,#4f46e5,#22c1c3);
  color:#f9fafb;
  box-shadow:0 0 14px rgba(79,70,229,.7);
}
.cw-pop-btn.ghost{
  background:transparent;
}

.cw-search-bar{
  display:grid;
  grid-template-columns:minmax(0,1fr) 70px 90px;
  gap:6px;
  margin-top:2px;
}
.cw-search-bar input,
.cw-search-bar select,
.cw-pop input[type="date"],
.cw-pop input[type="time"]{
  width:100%;
  background:#020617;
  border-radius:9px;
  border:1px solid rgba(129,140,248,.75);
  color:#e5e7eb;
  font-size:12px;
  padding:4px 7px;
}
.cw-search-bar input:focus,
.cw-search-bar select:focus,
.cw-pop input[type="date"]:focus,
.cw-pop input[type="time"]:focus{
  outline:none;
  border-color:#818cf8;
  box-shadow:0 0 0 1px rgba(129,140,248,.75);
}

.cw-search-results{
  margin-top:6px;
  border-radius:12px;
  border:1px solid rgba(255,255,255,.08);
  background:#05060c;
  box-shadow:0 18px 50px rgba(0,0,0,.55);

  flex:0 1 auto;
  max-height:min(360px, calc(100vh - 260px));
  overflow:auto;
}

.cw-search-item{
  width:100%;
  text-align:left;
  padding:8px 10px;
  border:none;
  border-bottom:1px solid rgba(255,255,255,.06);
  cursor:pointer;
  display:flex;
  align-items:flex-start;
  gap:10px;
  background:transparent;
}
.cw-search-item:last-child{border-bottom:none}
.cw-search-item:hover{
  background:rgba(255,255,255,.03);
}

.cw-search-poster{
  flex:0 0 auto;
  width:40px;
  height:60px;
  border-radius:8px;
  overflow:hidden;
  background:radial-gradient(circle at 30% 0,rgba(79,70,229,.7),rgba(15,23,42,1));
  border:1px solid rgba(148,163,184,.6);
  box-shadow:0 0 0 1px rgba(15,23,42,.9),0 0 10px rgba(79,70,229,.55);
}
.cw-search-poster img{
  width:100%;
  height:100%;
  object-fit:cover;
  display:block;
}
.cw-search-poster-placeholder{
  width:100%;
  height:100%;
  display:flex;
  align-items:center;
  justify-content:center;
  font-size:9px;
  letter-spacing:.06em;
  text-transform:uppercase;
  opacity:.8;
  color:#e5e7ff;
}

.cw-search-content{
  flex:1 1 auto;
  min-width:0;
  display:flex;
  flex-direction:column;
  gap:1px;
}

.cw-search-title-line{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:6px;
}
.cw-search-title{
  font-size:12px;
  font-weight:600;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
  color:#f9fafb;
}
.cw-search-tag{
  flex:0 0 auto;
  font-size:9px;
  text-transform:uppercase;
  padding:2px 7px;
  border-radius:999px;
  background:rgba(255,255,255,.08);
  border:1px solid rgba(255,255,255,.10);
  color:#e5e7eb;
  letter-spacing:.06em;
}

.cw-search-meta{
  font-size:10px;
  opacity:.9;
  color:#cbd5e1;
  white-space:normal;
  overflow:visible;
  text-overflow:clip;
  overflow-wrap:anywhere;
}

.cw-search-overview{
  font-size:10px;
  opacity:.9;
  color:#e5e9ff;

  white-space:normal;
  overflow:hidden;
  display:-webkit-box;
  -webkit-line-clamp:2;
  -webkit-box-orient:vertical;
  word-break:break-word;
}

.cw-search-empty{
  font-size:11px;
  opacity:.9;
  color:#e5e7ff;
  padding:6px 9px;
}
.cw-search-status{
  margin-top:4px;
  font-size:10px;
  opacity:.85;
  color:#e5e7ff;
}

/* datetime editor */
.cw-datetime-grid{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:8px;
  margin-top:6px;
}

/* rating + type pills */
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

.cw-type-filter{display:flex;flex-wrap:wrap;gap:6px}
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

/* sort + empty + pager */
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

/* sidebar cards */
#page-editor .ins-card{
  background:linear-gradient(180deg,rgba(20,20,28,.95),rgba(16,16,24,.95));
  border:1px solid rgba(255,255,255,.08);
  border-radius:16px;
  padding:10px 12px;
}
#page-editor .ins-row{
  display:flex;
  align-items:center;
  gap:12px;
  padding:8px 6px;
  border-top:1px solid rgba(255,255,255,.06);
}
#page-editor .ins-row:first-child{
  border-top:none;
  padding-top:2px;
}
#page-editor .ins-icon{
  width:32px;
  height:32px;
  border-radius:10px;
  display:flex;
  align-items:center;
  justify-content:center;
  background:#13131b;
  border:1px solid rgba(255,255,255,.06);
}
#page-editor .ins-title{font-weight:700}
#page-editor .ins-kv{
  display:grid;
  grid-template-columns:110px 1fr;
  gap:10px;
  align-items:center;
}
#page-editor .ins-kv label{opacity:.85}

#page-editor .ins-metrics{
  display:flex;
  flex-direction:column;
  gap:6px;
  width:100%;
}
#page-editor .metric-row{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(0,1fr));
  gap:8px;
}
#page-editor .metric-divider{
  height:1px;
  background:rgba(148,163,184,.28);
  margin:2px 0;
}
#page-editor .metric{
  position:relative;
  display:flex;
  align-items:center;
  gap:8px;
  background:#12121a;
  border:1px solid rgba(255,255,255,.08);
  border-radius:12px;
  padding:10px;
}
#page-editor .metric .material-symbol{font-size:18px;opacity:.9}
#page-editor .metric .m-val{font-weight:700}
#page-editor .metric .m-lbl{font-size:12px;opacity:.75}

/* status tag */
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
.cw-tag.loaded{
  background:radial-gradient(circle at 0 50%,rgba(147,197,253,.25),rgba(15,23,42,.96));
  border-color:rgba(96,165,250,.9);
  box-shadow:0 0 0 1px rgba(15,23,42,1),0 0 18px rgba(96,165,250,.5);
}
.cw-tag.loaded .cw-tag-dot{
  background:linear-gradient(135deg,#93c5fd,#3b82f6);
  box-shadow:0 0 10px rgba(147,197,253,1),0 0 20px rgba(59,130,246,.9);
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

/* extra-display pill */
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
.cw-extra-display-placeholder{opacity:.55;font-style:italic}
.cw-extra-display-value{color:#e5e7ff;font-weight:400}
.cw-extra-display-icon{font-size:14px;opacity:.7}

/* hint + responsive */
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

/* bulk selection */
.cw-checkbox{width:16px;height:16px;cursor:pointer;accent-color:#4f46e5}
.cw-bulk{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.cw-bulk-count{font-size:12px;opacity:.85}

.cw-btn.sm{padding:6px 10px;font-size:12px;min-height:30px}
.cw-progress{width:100%;height:8px;border-radius:999px;overflow:hidden;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.10)}
.cw-progress>span{display:block;height:100%;width:40%;background:linear-gradient(90deg,rgba(129,140,248,.15),rgba(129,140,248,.85),rgba(34,193,195,.85),rgba(129,140,248,.15));animation:cw-progress-move 1.2s linear infinite}
@keyframes cw-progress-move{0%{transform:translateX(-100%)}100%{transform:translateX(250%)}}

@media (max-width:1100px){
  .cw-wrap{grid-template-columns:minmax(0,1fr)}
}
`;

  const ensureStyle = (id, txt) => {
    let s = document.getElementById(id);
    if (!s) {
      s = document.createElement("style");
      s.id = id;
    }
    s.textContent = txt;
    if (!s.parentNode) document.head.appendChild(s);
  };
  ensureStyle("editor-styles", css);

  let cwEditorBooted = false;

  function bootEditor() {
    if (cwEditorBooted) return;
    const host = document.getElementById("page-editor");
    if (!host) return;
    cwEditorBooted = true;

  const state = {
    source: "state",
    kind: "watchlist",
    snapshot: "",
    pair: "",
    pairs: [],
    baselineItems: {},
    manualAdds: {},
    manualBlocks: [],
    items: {},
    rows: [],
    selected: new Set(),
    pageRids: [],
    ridSeq: 1,
    filter: "",
    loading: false,
    saving: false,
    snapshots: [],
    importEnabled: false,
    importProviders: [],
    importProvider: "",
    importMode: "replace",
    importFeatures: { watchlist: true, history: true, ratings: true },
    hasChanges: false,
    page: 0,
    blockedOnly: false,
    typeFilter: { movie: true, show: true, anime: true, season: true, episode: true },
    sortKey: "title",
    sortDir: "asc",
  };

  function restoreUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const saved = JSON.parse(raw);

      const sources = ["tracker", "pair", "state"];
      if (saved.source && sources.includes(saved.source)) state.source = saved.source;

      if (typeof saved.blockedOnly === "boolean") state.blockedOnly = saved.blockedOnly;

      const kinds = ["watchlist", "history", "ratings"];
      if (saved.kind && kinds.includes(saved.kind)) state.kind = saved.kind;

      if (typeof saved.snapshot === "string") state.snapshot = saved.snapshot;

      if (typeof saved.pair === "string") state.pair = saved.pair;
      if (typeof saved.filter === "string") state.filter = saved.filter;

      if (saved.typeFilter && typeof saved.typeFilter === "object") {
        ["movie", "show", "anime", "season", "episode"].forEach(t => {
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
            <div class="cw-bulk" id="cw-bulk" style="display:none">
              <span class="cw-bulk-count" id="cw-bulk-count"></span>
              <button id="cw-bulk-remove" class="cw-btn danger" type="button"></button>
              <button id="cw-bulk-restore" class="cw-btn" type="button"></button>
              <button id="cw-bulk-clear" class="cw-btn" type="button">Clear</button>
            </div>
            <button id="cw-reload" class="cw-btn" type="button">Reload</button>
            <button id="cw-add" class="cw-btn" type="button">Add row</button>
            <button id="cw-save" class="cw-btn primary" type="button">Save changes</button>
          </div>

          <div class="cw-table-wrap" id="cw-table-wrap">
            <table class="cw-table">
              <thead>
                <tr>
                  <th style="width:34px"><input id="cw-select-page" class="cw-checkbox" type="checkbox" title="Select page"></th>
                  <th style="width:30px"></th>
                  <th style="width:12%" data-sort="key" class="sortable">Key</th>
                  <th style="width:10%" data-sort="type" class="sortable">Type</th>
                  <th style="width:24%" data-sort="title" class="sortable">Title</th>
                  <th style="width:6%">Year</th>
                  <th style="width:10%">IMDb</th>
                  <th style="width:10%" id="cw-col-id-a">TMDB</th>
                  <th style="width:10%" id="cw-col-id-b">Trakt</th>
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
                <label>Data</label>
                <select id="cw-source" class="cw-select">
                  <option value="tracker">CW Tracker</option>
                  <option value="pair">Pair Cache</option>
                  <option value="state">Current State</option>
                </select>

                <label>Kind</label>
                <select id="cw-kind" class="cw-select">
                  <option value="watchlist">Watchlist</option>
                  <option value="history">History</option>
                  <option value="ratings">Ratings</option>
                </select>


                <label id="cw-pair-label" style="display:none">Pair</label>
                <select id="cw-pair" class="cw-select" style="display:none"></select>

                <label id="cw-snapshot-label">Snapshot</label>
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
                  <button type="button" data-type="anime" class="cw-type-chip active">Anime</button>
                  <button type="button" data-type="season" class="cw-type-chip active">S</button>
                  <button type="button" data-type="episode" class="cw-type-chip active">EP</button>
                  <button type="button" id="cw-blocked-only" class="cw-type-chip">Blocked only</button>
                </div>
              </div>
            </div>

            <div class="ins-row" id="cw-state-bulk" style="display:none">
              <details class="cw-collapse" id="cw-bulk-details" style="width:100%">
                <summary style="cursor:pointer;font-weight:700;user-select:none">Bulk policy</summary>
                <div style="display:flex;flex-direction:column;gap:8px;width:100%;margin-top:10px">
                  <select id="cw-bulk-type" class="cw-select" style="width:100%"></select>
                  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
                    <button id="cw-bulk-block-type" class="cw-btn danger" type="button" style="flex:1 1 0;min-width:120px">Block all</button>
                    <button id="cw-bulk-unblock-type" class="cw-btn" type="button" style="flex:1 1 0;min-width:120px">Unblock all</button>
                  </div>
                  <div class="cw-status-text">Current State only • affects baseline items</div>
                </div>
              </details>
            </div>

            <div class="ins-row" id="cw-import-row" style="display:none">
              <details class="cw-collapse" id="cw-import-details" style="width:100%">
                <summary style="cursor:pointer;font-weight:700;user-select:none">Import datasets</summary>

                <div style="display:flex;flex-direction:column;gap:10px;width:100%;margin-top:10px">
                  <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
                    <select id="cw-import-provider" class="cw-select" style="flex:1;min-width:200px"></select>
                    <select id="cw-import-mode" class="cw-select" style="min-width:180px">
                      <option value="replace">Replace baseline</option>
                      <option value="merge">Merge (keep old)</option>
                    </select>
                  </div>

                  <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center">
                    <label id="cw-import-watchlist-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0">
                      <input id="cw-import-watchlist" class="cw-checkbox" type="checkbox" checked>Watchlist
                    </label>
                    <label id="cw-import-history-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0">
                      <input id="cw-import-history" class="cw-checkbox" type="checkbox" checked>History
                    </label>
                    <label id="cw-import-ratings-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0">
                      <input id="cw-import-ratings" class="cw-checkbox" type="checkbox" checked>Ratings
                    </label>

                    <span style="flex:1 1 auto"></span>
                    <button id="cw-import-run" class="cw-btn sm" type="button">Import</button>
                  </div>

                  <div id="cw-import-progress" style="display:none">
                    <div class="cw-progress"><span></span></div>
                    <div class="cw-status-text" id="cw-import-progress-text" style="margin-top:6px"></div>
                  </div>
                </div>
              </details>
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
                    <span class="material-symbol">layers</span>
                    <div>
                      <div class="m-val" id="cw-summary-seasons">0</div>
                      <div class="m-lbl">S</div>
                    </div>
                  </div>
                  <div class="metric">
                    <span class="material-symbol">live_tv</span>
                    <div>
                      <div class="m-val" id="cw-summary-episodes">0</div>
                      <div class="m-lbl">EP</div>
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

          <div class="ins-card" id="cw-backup-card">
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
          </div>

          <div class="ins-card" id="cw-state-backup-card">
            <div class="ins-row">
              <div class="ins-icon"><span class="material-symbol">backup</span></div>
              <div class="ins-title">Policy Backup</div>
            </div>
            <div class="ins-row">
              <div class="ins-kv" style="width:100%">
                <label>Export / Import</label>
                <div class="cw-backup-actions">
                  <button id="cw-state-download" class="cw-btn" type="button">Download JSON</button>
                  <button id="cw-state-upload" class="cw-btn" type="button">Import file</button>
                  <input id="cw-state-upload-input" type="file" accept=".json" style="display:none">
                </div>
              </div>
            </div>
          </div>

        </aside>
      </div>
    </div>
  `;

  const $ = id => document.getElementById(id);
  const sourceSel = $("cw-source");
  const kindSel = $("cw-kind");
  const pairLabel = $("cw-pair-label");
  const pairSel = $("cw-pair");
  const snapLabel = $("cw-snapshot-label");
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
  const summarySeasons = $("cw-summary-seasons");
  const summaryEpisodes = $("cw-summary-episodes");
  const summaryStateFiles = $("cw-summary-state-files");
  const summarySnapshots = $("cw-summary-snapshots");
  const stateHint = $("cw-state-hint");
  const pager = $("cw-pager");
  const prevBtn = $("cw-prev");
  const nextBtn = $("cw-next");
  const pageInfo = $("cw-page-info");
  const typeFilterWrap = $("cw-type-filter");
  const backupCard = $("cw-backup-card");
  const blockedOnlyBtn = $("cw-blocked-only");
  const downloadBtn = $("cw-download");
  const uploadBtn = $("cw-upload");
  const uploadInput = $("cw-upload-input");
  const stateBackupCard = $("cw-state-backup-card");
  const stateDownloadBtn = $("cw-state-download");
  const stateUploadBtn = $("cw-state-upload");
  const stateUploadInput = $("cw-state-upload-input");

  const importRow = $("cw-import-row");
  const importProviderSel = $("cw-import-provider");
  const importWatchlistCb = $("cw-import-watchlist");
  const importHistoryCb = $("cw-import-history");
  const importRatingsCb = $("cw-import-ratings");
  const importModeSel = $("cw-import-mode");
  const importRunBtn = $("cw-import-run");
  const importWatchlistWrap = $("cw-import-watchlist-wrap");
  const importHistoryWrap = $("cw-import-history-wrap");
  const importRatingsWrap = $("cw-import-ratings-wrap");
  const importProgressWrap = $("cw-import-progress");
  const importProgressText = $("cw-import-progress-text");
  const sortHeaders = Array.from(host.querySelectorAll(".cw-table th[data-sort]"));

  const selectPage = $("cw-select-page");
  const bulkWrap = $("cw-bulk");
  const bulkCount = $("cw-bulk-count");
  const bulkRemoveBtn = $("cw-bulk-remove");
  const bulkRestoreBtn = $("cw-bulk-restore");
  const bulkClearBtn = $("cw-bulk-clear");
  const stateBulkRow = $("cw-state-bulk");
  const bulkTypeSel = $("cw-bulk-type");
  const bulkBlockTypeBtn = $("cw-bulk-block-type");
  const bulkUnblockTypeBtn = $("cw-bulk-unblock-type");

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

  if (filterInput && state.filter) filterInput.value = state.filter;

  function syncKindUI() {
    if (!kindSel) return;
    const allowed = ["watchlist", "history", "ratings"];
    if (!allowed.includes(state.kind)) state.kind = "watchlist";
    kindSel.value = state.kind;
  }

  function allowedTypesForKind(kind) {
    return kind === "watchlist"
      ? ["movie", "show", "anime"]
      : ["movie", "show", "anime", "season", "episode"];
  }
  function isAnilistMode() {
    return state.source === "state" && String(state.snapshot || "").trim().toUpperCase() === "ANILIST";
  }

  function syncIdColumnHeaders() {
    const a = $("cw-col-id-a");
    const b = $("cw-col-id-b");
    if (!a || !b) return;

    if (isAnilistMode()) {
      a.textContent = "MAL";
      b.textContent = "AniList";
    } else {
      a.textContent = "TMDB";
      b.textContent = "Trakt";
    }
  }

  function enforceKindTypeRules() {
    const allowed = allowedTypesForKind(state.kind);
    for (const t of ["movie", "show", "anime", "season", "episode"]) {
      if (!allowed.includes(t)) state.typeFilter[t] = false;
      else if (typeof state.typeFilter[t] !== "boolean") state.typeFilter[t] = true;
    }
  }

  function syncTypeFilterUI() {
    if (!typeFilterWrap) return;
    enforceKindTypeRules();
    const allowed = allowedTypesForKind(state.kind);
    const buttons = typeFilterWrap.querySelectorAll("button[data-type]");
    buttons.forEach(btn => {
      const t = btn.dataset.type;
      const visible = allowed.includes(t);
      btn.style.display = visible ? "" : "none";
      const on = state.typeFilter[t] !== false;
      btn.classList.toggle("active", on);
    });
    if (blockedOnlyBtn) blockedOnlyBtn.classList.toggle("active", !!state.blockedOnly);
  }

  function syncStateBulkUI() {
    if (!stateBulkRow || !bulkTypeSel || !bulkBlockTypeBtn || !bulkUnblockTypeBtn) return;
    const show = state.source === "state" && state.kind !== "watchlist";
    stateBulkRow.style.display = show ? "" : "none";
    if (!show) return;

    const allowed = allowedTypesForKind(state.kind);
    const opts = allowed.map(t => ({ v: t, l: t.charAt(0).toUpperCase() + t.slice(1) }));
    const current = bulkTypeSel.value;
    bulkTypeSel.innerHTML = opts.map(o => `<option value="${o.v}">${o.l}</option>`).join("");
    if (opts.some(o => o.v === current)) bulkTypeSel.value = current;
    else bulkTypeSel.value = opts[0] ? opts[0].v : "movie";
  }

  function setImportBusy(on, message) {
    if (importProgressWrap) importProgressWrap.style.display = on ? "" : "none";
    if (importProgressText) importProgressText.textContent = message || "";
    const disabled = !!on;
    if (importRunBtn) importRunBtn.disabled = disabled;
    if (importProviderSel) importProviderSel.disabled = disabled;
    if (importModeSel) importModeSel.disabled = disabled;
    if (importWatchlistCb) importWatchlistCb.disabled = disabled || importWatchlistCb.disabled;
    if (importHistoryCb) importHistoryCb.disabled = disabled || importHistoryCb.disabled;
    if (importRatingsCb) importRatingsCb.disabled = disabled || importRatingsCb.disabled;
  }

  function syncImportUI() {
    if (!importRow) return;
    const show = state.source === "state" && state.importEnabled;
    importRow.style.display = show ? "" : "none";
    if (!show) return;

    if (importModeSel) importModeSel.value = state.importMode || "replace";

    const all = Array.isArray(state.importProviders) ? state.importProviders : [];
    const list = all.filter(p => p && p.configured && p.name);

    if (importProviderSel) {
      const current = importProviderSel.value || state.importProvider || "";
      const opts = list
        .map(p => {
          const name = p && p.name ? String(p.name) : "";
          const label = p && p.label ? String(p.label) : name;
          return `<option value="${name}">${label}</option>`;
        })
        .join("");

      importProviderSel.innerHTML = opts || `<option value="">No configured providers</option>`;

      const names = list.map(p => String(p.name));
      let next = current;

      if (!next || !names.includes(next)) {
        next = state.snapshot && names.includes(state.snapshot) ? state.snapshot : "";
      }
      if (!next) next = names[0] || "";

      state.importProvider = next;
      importProviderSel.value = next;
      importProviderSel.disabled = !names.length;
    }

    const sel = state.importProvider || (importProviderSel ? importProviderSel.value : "");
    const p = list.find(x => String((x || {}).name || "") === String(sel || ""));
    const feats = (p && p.features) ? p.features : {};

    const setCb = (wrap, cb, key) => {
      const supported = !!feats[key];
      if (wrap) wrap.style.display = supported ? "" : "none";
      if (!cb) return;
      cb.disabled = !supported;
      if (!supported) cb.checked = false;
      else if (state.importFeatures && typeof state.importFeatures[key] === "boolean") cb.checked = !!state.importFeatures[key];
    };

    setCb(importWatchlistWrap, importWatchlistCb, "watchlist");
    setCb(importHistoryWrap, importHistoryCb, "history");
    setCb(importRatingsWrap, importRatingsCb, "ratings");

    if (importRunBtn) importRunBtn.disabled = !state.importProvider;
  }

  async function loadImportProviders() {
    state.importEnabled = false;
    state.importProviders = [];
    if (!importRow) return;
    try {
      const data = await fetchJSON("/api/editor/state/import/providers");
      state.importEnabled = !!(data && data.enabled);
      state.importProviders = Array.isArray(data && data.providers) ? data.providers : [];
    } catch (e) {
      state.importEnabled = false;
      state.importProviders = [];
    }
    syncImportUI();
  }

  function _collectImportFeatures() {
    const feats = [];
    if (importWatchlistCb && importWatchlistCb.checked && !importWatchlistCb.disabled) feats.push("watchlist");
    if (importHistoryCb && importHistoryCb.checked && !importHistoryCb.disabled) feats.push("history");
    if (importRatingsCb && importRatingsCb.checked && !importRatingsCb.disabled) feats.push("ratings");
    return feats;
  }

  async function runStateImport() {
    if (state.source !== "state") return;
    const provider = (importProviderSel ? importProviderSel.value : state.importProvider) || "";
    const features = _collectImportFeatures();
    const mode = (importModeSel ? importModeSel.value : state.importMode) || "replace";

    if (!provider) {
      setStatusSticky("Pick a provider first", 3000);
      return;
    }
    if (!features.length) {
      setStatusSticky("Pick at least one dataset", 3000);
      return;
    }

    state.importProvider = provider;
    state.importMode = mode;
    state.importFeatures = {
      watchlist: features.includes("watchlist"),
      history: features.includes("history"),
      ratings: features.includes("ratings"),
    };

    try {
      const msg = `Importing ${features.join(", ")} from ${provider}…`;
      setImportBusy(true, msg);
      setTag("warn", "Importing…");
      setStatus(msg);

      const res = await fetchJSON("/api/editor/state/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, features, mode }),
      });

      const featsOut = (res && res.features) ? res.features : {};
      const bits = [];
      let totalMs = 0;

      for (const k of Object.keys(featsOut)) {
        const r = featsOut[k] || {};
        if (r.skipped) continue;
        if (r.ok) bits.push(`${k}:${r.count}`);
        if (typeof r.elapsed_ms === "number") totalMs += r.elapsed_ms;
      }

      let done = "Imported " + (bits.length ? bits.join(" • ") : "done");
      if (totalMs) done += ` (${(totalMs / 1000).toFixed(1)}s)`;

      setTag("loaded", "Imported");
      setStatusSticky(done, 6000);
      if (window.cxToast) window.cxToast(done);

      state.snapshot = provider;
      persistUIState();
      await loadSnapshots();
      await loadState();
    } catch (e) {
      console.error(e);
      setTag("error", "Import failed");
      setStatus(String(e));
    } finally {
      setImportBusy(false, "");
      syncImportUI();
    }
  }


  syncKindUI();
  syncTypeFilterUI();
  syncStateBulkUI();

  function persistUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const data = {
        source: state.source,
        kind: state.kind,
        snapshot: state.snapshot,
        pair: state.pair,
        filter: state.filter,
        typeFilter: state.typeFilter,
        blockedOnly: state.blockedOnly,
        sortKey: state.sortKey,
        sortDir: state.sortDir,
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch (_) {}
  }

  function syncBulkBar() {
    if (!bulkWrap || !bulkCount || !bulkRemoveBtn || !bulkRestoreBtn || !bulkClearBtn) return;
    const n = state.selected ? state.selected.size : 0;
    bulkWrap.style.display = n ? "flex" : "none";
    if (!n) return;
    bulkCount.textContent = `${n} selected`;
    if (state.source === "state") {
      bulkRemoveBtn.textContent = "Block selected";
      bulkRestoreBtn.textContent = "Unblock selected";
    } else {
      bulkRemoveBtn.textContent = "Delete selected";
      bulkRestoreBtn.textContent = "Restore selected";
    }
  }

  function clearSelection() {
    if (!state.selected) state.selected = new Set();
    state.selected.clear();
    syncBulkBar();
  }

  function syncSelectPageCheckbox() {
    if (!selectPage) return;
    const rids = Array.isArray(state.pageRids) ? state.pageRids : [];
    if (!rids.length) {
      selectPage.checked = false;
      selectPage.indeterminate = false;
      return;
    }
    const sel = state.selected || new Set();
    const all = rids.every(r => sel.has(r));
    const any = rids.some(r => sel.has(r));
    selectPage.checked = all;
    selectPage.indeterminate = any && !all;
  }

  function bulkSetDeletedForSelected(flag) {
    const sel = state.selected || new Set();
    if (!sel.size) return;
    let changed = 0;
    for (const row of state.rows || []) {
      if (!sel.has(row._rid)) continue;
      if (row.deleted !== flag) {
        row.deleted = flag;
        changed += 1;
      }
    }
    clearSelection();
    if (changed) {
      markChanged();
      renderRows();
      const verb = flag
        ? state.source === "state"
          ? "Blocked"
          : "Deleted"
        : state.source === "state"
          ? "Unblocked"
          : "Restored";
      setStatusSticky(`${verb} ${changed} item${changed === 1 ? "" : "s"}`, 3000);
    }
  }

  function bulkSetBlocksByType(type, flag) {
    if (state.source !== "state") return;
    const t = String(type || "").toLowerCase();
    if (!t) return;
    let changed = 0;
    for (const row of state.rows || []) {
      if (row._origin !== "baseline") continue;
      if (((row.type || "") + "").toLowerCase() !== t) continue;
      if (row.deleted !== flag) {
        row.deleted = flag;
        changed += 1;
      }
    }
    clearSelection();
    if (changed) {
      markChanged();
      renderRows();
      setStatusSticky(
        `${flag ? "Blocked" : "Unblocked"} ${changed} ${t} item${changed === 1 ? "" : "s"}`,
        3500
      );
    }
  }

  function syncSourceUI() {
    const isState = state.source === "state";
    const isPair = state.source === "pair";
    if (sourceSel) sourceSel.value = state.source;
    if (pairLabel) pairLabel.style.display = isPair ? "" : "none";
    if (pairSel) pairSel.style.display = isPair ? "" : "none";
    if (snapLabel) snapLabel.textContent = isState ? "Provider" : isPair ? "Dataset" : "Snapshot";
    if (backupCard) backupCard.style.display = isState ? "none" : "";
    if (stateBackupCard) stateBackupCard.style.display = isState ? "" : "none";
    if (blockedOnlyBtn) blockedOnlyBtn.style.display = isState ? "" : "none";

    if (!isState && state.blockedOnly) {
      state.blockedOnly = false;
      syncTypeFilterUI();
      persistUIState();
    }
    syncStateBulkUI();
    syncImportUI();
  }

  function showStateHint(mode) {
    if (!stateHint) return;
    if (mode === "tracker") {
      stateHint.innerHTML =
        "<strong>No tracker data found.</strong> Run a CrossWatch sync with the tracker enabled once. After that, tracker state files and snapshots will appear here and you can edit them.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "pair") {
      stateHint.innerHTML =
        "<strong>No pair cache found.</strong> Run a CrossWatch sync once to generate .cw_state pair indexes. Then select a Pair and Dataset here.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "state") {
      stateHint.innerHTML =
        "<strong>No state.json found.</strong> Run a CrossWatch sync once to generate it. After that, your manual adds and blocks will show up here.";
      stateHint.style.display = "block";
      return;
    }
    stateHint.style.display = "none";
  }

  function setTag(mode, label) {
    if (!tag || !tagLabel) return;
    tag.classList.remove("warn", "error", "loaded");
    if (mode === "warn") tag.classList.add("warn");
    else if (mode === "error") tag.classList.add("error");
    else if (mode === "loaded") tag.classList.add("loaded");
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
  function formatSxxEyy(season, episode) {
    const s = season == null ? NaN : parseInt(String(season), 10);
    if (!Number.isFinite(s)) return "";
    const pad = n => String(n).padStart(2, "0");
    const e = episode == null ? NaN : parseInt(String(episode), 10);
    if (Number.isFinite(e)) return `S${pad(s)}E${pad(e)}`;
    return `S${pad(s)}`;
  }


  function updateExtraDisplay(row, el) {
    let label = "";
    let placeholder = "";
    let icon = "";
    if (state.kind === "ratings") {
      icon = "star";
      const r = row.raw && row.raw.rating;
      if (r == null || r === "") placeholder = "Set rating";
      else label = String(r) + "/10";
    } else if (state.kind === "history") {
      icon = "schedule";
      const w = row.raw && row.raw.watched_at;
      if (!w) placeholder = "Set time";
      else label = formatHistoryLabel(w);
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
    } else if (t === "anime") {
      label = "Anime";
      icon = "auto_awesome";
    } else if (t === "season") {
      label = "Season";
      icon = "layers";
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

  function imdbFromKey(key) {
    const s = (key || "") + "";
    if (!s.startsWith("imdb:")) return "";
    return s.slice(5).split("#")[0];
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
        _rid: state.ridSeq++,
        key,
        type,
        title: baseTitle,
        year: raw.year != null ? String(raw.year) : "",
        imdb: ids.imdb || (type === "season" ? showIds.imdb || imdbFromKey(key) : ""),
        tmdb: ids.tmdb || showIds.tmdb || "",
        trakt: ids.trakt || showIds.trakt || "",
        mal: ids.mal || "",
        anilist: ids.anilist || "",
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
    const hasTypeFilter = filters.movie || filters.show || filters.anime || filters.season || filters.episode;

    return rows.filter(r => {
      if (hasTypeFilter) {
        const t = (r.type || "").toLowerCase();
        const known = t === "movie" || t === "show" || t === "anime" || t === "season" || t === "episode";
        let allowed = true;
        if (known) {
          if (t === "movie") allowed = !!filters.movie;
          else if (t === "show") allowed = !!filters.show;
          else if (t === "anime") allowed = !!filters.anime;
          else if (t === "season") allowed = !!filters.season;
          else if (t === "episode") allowed = !!filters.episode;
        }
        if (!allowed) return false;
      }

      if (state.blockedOnly && state.source === "state") {
        if (!(r.deleted && r._origin === "baseline")) return false;
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
        r.mal,
        r.anilist,
        r.raw && r.raw.series_title ? r.raw.series_title : "",
      ]
        .join(" ")
        .toLowerCase();

      return parts.includes(q);
    });
  }

  function openHistoryEditor(row, anchor, displayEl) {
    const locked = false;

    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Watched at";
      pop.appendChild(title);

      if (locked) {
        const status = document.createElement("div");
        status.className = "cw-search-status";
        status.textContent = "Baseline rows are read-only. Block the row to exclude it.";
        pop.appendChild(status);

        const actions = document.createElement("div");
        actions.className = "cw-pop-actions";
        const closeBtn = document.createElement("button");
        closeBtn.type = "button";
        closeBtn.className = "cw-pop-btn primary";
        closeBtn.textContent = "Close";
        closeBtn.onclick = close;
        actions.appendChild(closeBtn);
        pop.appendChild(actions);
        return;
      }

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

      const saveBtn2 = document.createElement("button");
      saveBtn2.type = "button";
      saveBtn2.className = "cw-pop-btn primary";
      saveBtn2.textContent = "Save";
      saveBtn2.onclick = () => {
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
      actions.appendChild(saveBtn2);
      pop.appendChild(actions);

      dateInput.focus();
    });
  }

  function openRatingEditor(row, anchor, displayEl) {
    const locked = false;

    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Rating";
      pop.appendChild(title);

      if (locked) {
        const status = document.createElement("div");
        status.className = "cw-search-status";
        status.textContent = "Baseline rows are read-only. Block the row to exclude it.";
        pop.appendChild(status);

        const actions = document.createElement("div");
        actions.className = "cw-pop-actions";
        const closeBtn = document.createElement("button");
        closeBtn.type = "button";
        closeBtn.className = "cw-pop-btn primary";
        closeBtn.textContent = "Close";
        closeBtn.onclick = close;
        actions.appendChild(closeBtn);
        pop.appendChild(actions);
        return;
      }

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

  function openTitleSearchEditor(row, anchor, refs) {
    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Search metadata";
      pop.appendChild(title);

      const bar = document.createElement("div");
      bar.className = "cw-search-bar";

      const qInput = document.createElement("input");
      qInput.type = "text";
      qInput.placeholder = "Title…";
      qInput.value = row.title || "";
      bar.appendChild(qInput);

      const yearInput = document.createElement("input");
      yearInput.type = "number";
      yearInput.placeholder = "Year";
      if (row.year) yearInput.value = row.year;
      bar.appendChild(yearInput);

      const typeSelect = document.createElement("select");
      [["movie", "Movie"], ["show", "Show"], ["anime", "Anime"]].forEach(([val, label]) => {
        const opt = document.createElement("option");
        opt.value = val;
        opt.textContent = label;
        typeSelect.appendChild(opt);
      });
      typeSelect.value = row.type === "anime" ? "anime" : row.type === "show" || row.type === "episode" ? "show" : "movie";
      bar.appendChild(typeSelect);

      pop.appendChild(bar);

      const actions = document.createElement("div");
      actions.className = "cw-pop-actions";

      const searchBtn = document.createElement("button");
      searchBtn.type = "button";
      searchBtn.className = "cw-pop-btn primary";
      searchBtn.textContent = "Search";
      actions.appendChild(searchBtn);

      const closeBtn = document.createElement("button");
      closeBtn.type = "button";
      closeBtn.className = "cw-pop-btn ghost";
      closeBtn.textContent = "Close";
      closeBtn.onclick = close;
      actions.appendChild(closeBtn);

      pop.appendChild(actions);

      const status = document.createElement("div");
      status.className = "cw-search-status";
      pop.appendChild(status);

      const resultsBox = document.createElement("div");
      resultsBox.className = "cw-search-results";
      pop.appendChild(resultsBox);

      async function doSearch() {
        const q = (qInput.value || "").trim();
        const yearVal = parseInt(yearInput.value || "", 10);
        if (q.length < 2) {
          status.textContent = "Type at least 2 characters.";
          resultsBox.innerHTML = "";
          return;
        }
        const typ = String(typeSelect.value || "").toLowerCase();
        const makeUrl = t => {
          let u = `/api/metadata/search?q=${encodeURIComponent(q)}&typ=${encodeURIComponent(t)}`;
          if (!Number.isNaN(yearVal)) u += `&year=${yearVal}`;
          return u;
        };

        status.textContent = "Searching…";
        resultsBox.innerHTML = "";
        try {
          let items = [];
          if (typ === "anime") {
            const [showRes, movieRes] = await Promise.all([fetchJSON(makeUrl("show")), fetchJSON(makeUrl("movie"))]);

            const showOk = !!(showRes && showRes.ok !== false);
            const movieOk = !!(movieRes && movieRes.ok !== false);

            if (!showOk && !movieOk) {
              const msg = (showRes && showRes.error) || (movieRes && movieRes.error) || "Search failed.";
              status.textContent = msg;
              return;
            }

            const a = showOk && Array.isArray(showRes.results) ? showRes.results : [];
            const b = movieOk && Array.isArray(movieRes.results) ? movieRes.results : [];

            items = [...a.map(x => ({ ...x, _resolve_entity: "show" })), ...b.map(x => ({ ...x, _resolve_entity: "movie" }))];

            const seen = new Set();
            items = items.filter(it => {
              const k = `${String(it.tmdb || "")}:${String(it.type || "")}`;
              if (!k || seen.has(k)) return false;
              seen.add(k);
              return true;
            });
          } else {
            const data = await fetchJSON(makeUrl(typ));
            if (!data || data.ok === false) {
              status.textContent = data && data.error ? data.error : "Search failed.";
              return;
            }
            items = Array.isArray(data.results) ? data.results : [];
          }
          if (!items.length) {
            resultsBox.innerHTML = '<div class="cw-search-empty">No results.</div>';
            status.textContent = "";
            return;
          }

          resultsBox.innerHTML = "";
          items.forEach(item => {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "cw-search-item";

            const posterWrap = document.createElement("div");
            posterWrap.className = "cw-search-poster";

            if (item.poster_path) {
              const img = document.createElement("img");
              img.src = "https://image.tmdb.org/t/p/w92" + item.poster_path;
              img.alt = "";
              posterWrap.appendChild(img);
            } else {
              const ph = document.createElement("div");
              ph.className = "cw-search-poster-placeholder";
              ph.textContent = item.type === "show" ? "TV" : "MOV";
              posterWrap.appendChild(ph);
            }

            btn.appendChild(posterWrap);

            const content = document.createElement("div");
            content.className = "cw-search-content";

            const titleLine = document.createElement("div");
            titleLine.className = "cw-search-title-line";

            const t = document.createElement("div");
            t.className = "cw-search-title";
            const yearTxt = item.year ? ` (${item.year})` : "";
            t.textContent = (item.title || "") + yearTxt;
            titleLine.appendChild(t);

            const tag2 = document.createElement("span");
            tag2.className = "cw-search-tag";
            tag2.textContent = item.type === "show" ? "Show" : "Movie";
            titleLine.appendChild(tag2);

            content.appendChild(titleLine);

            const meta = document.createElement("div");
            meta.className = "cw-search-meta";
            const bits = [];
            if (item.year) bits.push(String(item.year));
            bits.push(item.type === "show" ? "TV" : "Movie");
            if (item.tmdb) bits.push(`TMDb ${item.tmdb}`);
            meta.textContent = bits.join(" • ");
            content.appendChild(meta);

            if (item.overview) {
              const ov = document.createElement("div");
              ov.className = "cw-search-overview";
              ov.textContent = item.overview;
              content.appendChild(ov);
            }

            btn.appendChild(content);

            btn.onclick = async () => {
              const picked = item;
              const newTitle = picked.title || row.title || "";
              row.title = newTitle;
              row.raw.title = newTitle || null;
              refs.titleIn.value = newTitle;

              if (picked.year) {
                row.year = String(picked.year);
                row.raw.year = picked.year;
                refs.yearIn.value = row.year;
              }

              const wantsAnime = String(typeSelect.value || "").toLowerCase() === "anime";
              const pickedType = String(picked.type || "movie").toLowerCase();

              const newType = wantsAnime ? "anime" : pickedType;
              const resolveEntity = wantsAnime ? picked._resolve_entity || pickedType || "movie" : newType;

              row.type = newType;
              row.raw.type = newType;
              row.episode = false;
              updateTypeDisplay(row, refs.typeBtn);

              const tmdbId = picked.tmdb;
              if (tmdbId != null) {
                const tmdbStr = String(tmdbId);
                row.tmdb = tmdbStr;
                row.raw.ids = row.raw.ids || {};
                row.raw.ids.tmdb = tmdbId;
                if (refs.tmdbIn) refs.tmdbIn.value = tmdbStr;
              }

              if (tmdbId != null) {
                try {
                  const metaRes = await fetchJSON("/api/metadata/resolve", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ entity: resolveEntity, ids: { tmdb: tmdbId } }),
                  });

                  if (metaRes && metaRes.ok && metaRes.result && metaRes.result.ids) {
                    const ids = metaRes.result.ids || {};
                    row.raw.ids = row.raw.ids || {};

                    if (ids.imdb) {
                      row.imdb = ids.imdb;
                      row.raw.ids.imdb = ids.imdb;
                      refs.imdbIn.value = ids.imdb;
                      const imdbKey = `imdb:${ids.imdb}`;
                      const prevKey = (row.key || "").trim();
                      if (!prevKey || /^imdb:/i.test(prevKey)) {
                        row.key = imdbKey;
                        if (refs.keyIn) refs.keyIn.value = imdbKey;
                      }
                    }
                    if (ids.tmdb) {
                      const tVal = String(ids.tmdb);
                      row.tmdb = tVal;
                      row.raw.ids.tmdb = ids.tmdb;
                      if (refs.tmdbIn) refs.tmdbIn.value = tVal;
                    }
                    if (ids.trakt) {
                      const trVal = String(ids.trakt);
                      row.trakt = trVal;
                      row.raw.ids.trakt = ids.trakt;
                      if (refs.traktIn) refs.traktIn.value = trVal;
                    }
                  }
                } catch (err) {
                  console.error("metadata resolve failed", err);
                }
              }

              markChanged();
              setStatusSticky("Row updated from metadata", 2500);
              close();
              renderRows();
            };

            resultsBox.appendChild(btn);
          });

          status.textContent = `${items.length} result${items.length === 1 ? "" : "s"} found.`;
        } catch (err) {
          console.error("search failed", err);
          status.textContent = "Search failed.";
        }
      }

      searchBtn.onclick = () => doSearch();

      qInput.addEventListener("keydown", ev => {
        if (ev.key === "Enter") {
          ev.preventDefault();
          doSearch();
        }
      });

      if ((row.title || "").trim().length >= 3) doSearch();
      else status.textContent = "Enter a title and press Enter or Search.";
    });
  }

  function openTypeEditor(row, anchor) {
    const locked = false;

    openPopup(anchor, (pop, close) => {
      const title = document.createElement("div");
      title.className = "cw-pop-title";
      title.textContent = "Type";
      pop.appendChild(title);

      if (locked) {
        const status = document.createElement("div");
        status.className = "cw-search-status";
        status.textContent = "Baseline rows are read-only. Block the row to exclude it.";
        pop.appendChild(status);

        const actions = document.createElement("div");
        actions.className = "cw-pop-actions";
        const closeBtn = document.createElement("button");
        closeBtn.type = "button";
        closeBtn.className = "cw-pop-btn primary";
        closeBtn.textContent = "Close";
        closeBtn.onclick = close;
        actions.appendChild(closeBtn);
        pop.appendChild(actions);
        return;
      }

      const grid = document.createElement("div");
      grid.className = "cw-type-grid";
      const current = (row.type || "").toLowerCase();
      const allowed = allowedTypesForKind(state.kind);
      const options = [
        { key: "movie", label: "Movie" },
        { key: "show", label: "Show" },
        { key: "anime", label: "Anime" },
        { key: "season", label: "Season" },
        { key: "episode", label: "Episode" },
      ].filter(o => allowed.includes(o.key));

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
    return rows.slice().sort((a, b) => {
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
          av = aw ? Date.parse(aw) || 0 : 0;
          bv = bw ? Date.parse(bw) || 0 : 0;
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
  }

  function updateSortUI() {
    sortHeaders.forEach(th => {
      const k = th.dataset.sort;
      th.classList.remove("sort-asc", "sort-desc");
      if (k === state.sortKey) th.classList.add(state.sortDir === "desc" ? "sort-desc" : "sort-asc");
    });
  }

  function renderRows() {
    closePopup();
    updateSortUI();
    syncIdColumnHeaders();

    let filtered = applyFilter(state.rows);
    const totalFiltered = filtered.length;
    const totalAll = state.rows.length;

    filtered = sortRows(filtered);

    let movies = 0;
    let shows = 0;
    let seasons = 0;
    let episodes = 0;
    for (const row of state.rows) {
      const t = (row.type || "").toLowerCase();
      if (t === "movie") movies += 1;
      else if (t === "show") shows += 1;
      else if (t === "season") seasons += 1;
      else if (t === "episode") episodes += 1;
    }
    if (summaryMovies) summaryMovies.textContent = String(movies);
    if (summaryShows) summaryShows.textContent = String(shows);
    if (summarySeasons) summarySeasons.textContent = String(seasons);
    if (summaryEpisodes) summaryEpisodes.textContent = String(episodes);

    if (tbody) tbody.innerHTML = "";

    if (!totalFiltered) {
      if (empty) empty.style.display = "block";
      if (pager) pager.style.display = "none";
      if (summaryVisible) summaryVisible.textContent = "0";
      if (summaryTotal) summaryTotal.textContent = String(totalAll || 0);
      setStatus("0 rows visible");
      state.pageRids = [];
      syncSelectPageCheckbox();
      clearSelection();
      if (pageInfo) pageInfo.textContent = "";
      return;
    }

    if (empty) empty.style.display = "none";

    const pageCount = Math.max(1, Math.ceil(totalFiltered / PAGE_SIZE));
    if (state.page >= pageCount) state.page = pageCount - 1;
    if (state.page < 0) state.page = 0;

    const start = state.page * PAGE_SIZE;
    const end = start + PAGE_SIZE;
    const rows = filtered.slice(start, end);

    state.pageRids = rows.map(r => r._rid);
    syncSelectPageCheckbox();
    syncBulkBar();

    const frag = document.createDocumentFragment();
    const anilistMode = isAnilistMode();
    rows.forEach(row => {
      const tr = document.createElement("tr");
      const locked = false;
      if (row.episode) tr.classList.add("cw-row-episode");
      if (row.deleted) tr.classList.add("cw-row-deleted");

      const cell = inner => {
        const td = document.createElement("td");
        td.appendChild(inner);
        return td;
      };

      const selCb = document.createElement("input");
      selCb.type = "checkbox";
      selCb.className = "cw-checkbox";
      selCb.checked = (state.selected || new Set()).has(row._rid);
      selCb.onchange = () => {
        if (!state.selected) state.selected = new Set();
        if (selCb.checked) state.selected.add(row._rid);
        else state.selected.delete(row._rid);
        syncBulkBar();
        syncSelectPageCheckbox();
      };
      tr.appendChild(cell(selCb));

      const delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "cw-btn cw-btn-del danger";
      delBtn.innerHTML = '<span class="material-symbol">delete</span>';
      delBtn.title = locked ? (row.deleted ? "Unblock row" : "Block row") : "Delete row";
      delBtn.onclick = () => {
        row.deleted = !row.deleted;
        markChanged();
        renderRows();
      };
      tr.appendChild(cell(delBtn));

      const keyIn = document.createElement("input");
      keyIn.value = row.key || "";
      keyIn.className = "cw-key";
      keyIn.disabled = locked;
      keyIn.oninput = e => {
        row.key = e.target.value;
        markChanged();
      };
      tr.appendChild(cell(keyIn));

      const typeBtn = document.createElement("button");
      typeBtn.type = "button";
      typeBtn.className = "cw-extra-display";
      typeBtn.disabled = locked;
      if (locked) {
        typeBtn.style.opacity = "0.6";
        typeBtn.style.cursor = "not-allowed";
      }
      updateTypeDisplay(row, typeBtn);
      typeBtn.onclick = () => {
        if (typeBtn.disabled) return;
        openTypeEditor(row, typeBtn);
      };
      tr.appendChild(cell(typeBtn));

      const titleCell = document.createElement("div");
      titleCell.className = "cw-title-cell";

      const titleRow = document.createElement("div");
      titleRow.className = "cw-title-row";
      titleCell.appendChild(titleRow);

      const titleIn = document.createElement("input");
      titleIn.value = row.title || "";
      titleIn.disabled = locked;
      titleIn.oninput = e => {
        row.title = e.target.value;
        row.raw.title = e.target.value || null;
        markChanged();
      };
      titleRow.appendChild(titleIn);

      const yearIn = document.createElement("input");
      yearIn.value = row.year || "";
      yearIn.disabled = locked;
      yearIn.oninput = e => {
        row.year = e.target.value;
        const v = e.target.value.trim();
        const n = v ? parseInt(v, 10) : NaN;
        row.raw.year = Number.isFinite(n) ? n : null;
        markChanged();
      };

      const imdbIn = document.createElement("input");
      imdbIn.value = row.imdb || "";
      imdbIn.disabled = locked;
      imdbIn.oninput = e => {
        row.imdb = e.target.value;
        row.raw.ids = row.raw.ids || {};
        if (e.target.value) row.raw.ids.imdb = e.target.value;
        else delete row.raw.ids.imdb;
        markChanged();
      };
      const idAIn = document.createElement("input");
      idAIn.value = anilistMode ? (row.mal || "") : (row.tmdb || "");
      idAIn.placeholder = anilistMode ? "MAL…" : "TMDB…";
      idAIn.disabled = locked;
      idAIn.oninput = e => {
        const v = e.target.value;
        row.raw.ids = row.raw.ids || {};
        if (anilistMode) {
          row.mal = v;
          if (v) row.raw.ids.mal = v;
          else delete row.raw.ids.mal;
        } else {
          row.tmdb = v;
          if (v) row.raw.ids.tmdb = v;
          else delete row.raw.ids.tmdb;
        }
        markChanged();
      };

      const idBIn = document.createElement("input");
      idBIn.value = anilistMode ? (row.anilist || "") : (row.trakt || "");
      idBIn.placeholder = anilistMode ? "AniList…" : "Trakt…";
      idBIn.disabled = locked;
      idBIn.oninput = e => {
        const v = e.target.value;
        row.raw.ids = row.raw.ids || {};
        if (anilistMode) {
          row.anilist = v;
          if (v) row.raw.ids.anilist = v;
          else delete row.raw.ids.anilist;
        } else {
          row.trakt = v;
          if (v) row.raw.ids.trakt = v;
          else delete row.raw.ids.trakt;
        }
        markChanged();
      };

      const searchBtn = document.createElement("button");
      searchBtn.type = "button";
      searchBtn.className = "cw-title-search-btn";
      searchBtn.innerHTML = '<span class="material-symbol">search</span>';
      searchBtn.title = "Search and fill IDs";
      searchBtn.disabled = locked;
      if (locked) {
        searchBtn.style.opacity = "0.6";
        searchBtn.style.cursor = "not-allowed";
      }
      searchBtn.onclick = () => {
        if (searchBtn.disabled) return;
        openTitleSearchEditor(row, searchBtn, {
          keyIn,
          titleIn,
          yearIn,
          imdbIn,
          tmdbIn: anilistMode ? null : idAIn,
          traktIn: anilistMode ? null : idBIn,
          typeBtn,
        });
      };
      titleRow.appendChild(searchBtn);

      const subType = (((row.raw && row.raw.type) || row.type || "") + "").toLowerCase();
      if ((subType === "episode" || subType === "season") && row.raw && row.raw.series_title) {
        const sub = document.createElement("div");
        sub.className = "cw-title-sub";
        let label = row.raw.series_title;
        const code = subType === "episode" ? formatSxxEyy(row.raw.season, row.raw.episode) : formatSxxEyy(row.raw.season, null);
        if (code) label += " - " + code;
        sub.textContent = label;
        titleCell.appendChild(sub);
      }
      tr.appendChild(cell(titleCell));

      tr.appendChild(cell(yearIn));
      tr.appendChild(cell(imdbIn));
      tr.appendChild(cell(idAIn));
      tr.appendChild(cell(idBIn));

      const extraBtn = document.createElement("button");
      extraBtn.type = "button";
      extraBtn.className = "cw-extra-display";
      updateExtraDisplay(row, extraBtn);

      const extraEditable = !locked && (state.kind === "ratings" || state.kind === "history");
      if (!extraEditable) {
        extraBtn.disabled = true;
        extraBtn.style.opacity = "0.6";
        extraBtn.style.cursor = locked ? "not-allowed" : "default";
      } else if (state.kind === "ratings") {
        extraBtn.onclick = () => openRatingEditor(row, extraBtn, extraBtn);
      } else if (state.kind === "history") {
        extraBtn.onclick = () => openHistoryEditor(row, extraBtn, extraBtn);
      }

      tr.appendChild(cell(extraBtn));

      frag.appendChild(tr);
    });

    if (tbody) tbody.appendChild(frag);

    const vis = rows.length;
    const first = start + 1;
    const last = start + vis;

    if (summaryVisible) summaryVisible.textContent = String(vis);
    if (summaryTotal) summaryTotal.textContent = String(totalAll);

    if (pageInfo) pageInfo.textContent = `Page ${state.page + 1} of ${pageCount} • Rows ${first}-${last} of ${totalFiltered}`;
    if (pager) pager.style.display = pageCount > 1 ? "flex" : "none";
    if (prevBtn) prevBtn.disabled = state.page <= 0;
    if (nextBtn) nextBtn.disabled = state.page >= pageCount - 1;

    if (totalFiltered > vis) {
      setRowsStatus(`${vis} rows visible (rows ${first}-${last} of ${totalFiltered} filtered, ${totalAll} total)`);
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
    if (!snapSel) return;
    const isState = state.source === "state";
    const isPair = state.source === "pair";
    if (snapLabel) snapLabel.textContent = isState ? "Provider" : isPair ? "Dataset" : "Snapshot";

    if (isState || isPair) {
      const list = Array.isArray(state.snapshots) ? state.snapshots : [];
      const options = list.map(p => `<option value="${p}">${p}</option>`).join("");
      snapSel.innerHTML = options;
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.snapshot) ? state.snapshot : opts[0] || "";
      if (next !== state.snapshot) state.snapshot = next;
      snapSel.value = state.snapshot || "";
      return;
    }

    const options = (state.snapshots || [])
      .map(s => {
        const label = formatSnapshotLabel(s);
        return `<option value="${s.name}">${label}</option>`;
      })
      .join("");

    snapSel.innerHTML = `<option value="">Latest</option>` + options;
    snapSel.value = state.snapshot || "";
  }


  function rebuildPairs() {
    if (!pairSel) return;
    const isPair = state.source === "pair";
    if (!isPair) return;
    const list = Array.isArray(state.pairs) ? state.pairs : [];
    const esc = s => String(s || "").replace(/[&<>\"\']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "\'": "&#39;" }[c] || c));
    const options = list
      .map(p => {
        const scope = p && p.scope ? String(p.scope) : "";
        const label = p && p.label ? String(p.label) : scope;
        return `<option value="${esc(scope)}">${esc(label)}</option>`;
      })
      .join("");
    pairSel.innerHTML = options || `<option value="">No pairs</option>`;
    const opts = Array.from(pairSel.options).map(o => o.value);
    const next = opts.includes(state.pair) ? state.pair : opts[0] || "";
    if (next !== state.pair) state.pair = next;
    pairSel.value = state.pair || "";
  }

  async function loadPairs() {
    try {
      const data = await fetchJSON("/api/editor/pairs");
      state.pairs = Array.isArray(data && data.pairs) ? data.pairs : [];
      if (!state.pair) state.pair = (data && data.default) ? String(data.default) : "";
      rebuildPairs();
    } catch (e) {
      console.error(e);
      state.pairs = [];
      rebuildPairs();
    }
  }

  async function fetchJSON(url, opts) {
    const res = await fetch(url, Object.assign({ cache: "no-store" }, opts || {}));
    if (!res.ok) throw new Error(`Request failed: ${res.status}`);
    return await res.json();
  }

  async function loadSnapshots() {
    try {
      if (state.source === "pair") {
        if (!state.pair || !Array.isArray(state.pairs) || !state.pairs.length) await loadPairs();
        rebuildPairs();
        if (!state.pair) {
          state.snapshots = [];
          rebuildSnapshots();
          showStateHint("pair");
          return;
        }
        const data = await fetchJSON(`/api/editor/pairs/datasets?kind=${encodeURIComponent(state.kind)}&pair=${encodeURIComponent(state.pair)}`);
        const dsets = Array.isArray(data && data.datasets) ? data.datasets : [];
        state.snapshots = dsets.map(d => (d && d.name ? String(d.name) : "")).filter(Boolean);
        const defDs = data && data.default_dataset ? String(data.default_dataset) : "";
        rebuildSnapshots();
        const opts = state.snapshots;
        const next = opts.includes(state.snapshot) ? state.snapshot : (defDs && opts.includes(defDs) ? defDs : (opts[0] || ""));
        if (next !== state.snapshot) state.snapshot = next;
        if (snapSel) snapSel.value = state.snapshot || "";
        if (!state.snapshots.length) showStateHint("pair");
        else showStateHint(null);
        return;
      }
      if (state.source === "state") {
        const data = await fetchJSON(`/api/editor/state/providers`);
        state.snapshots = Array.isArray(data.providers) ? data.providers : [];
        rebuildSnapshots();
        if (!state.snapshots.length) showStateHint("state");
        else showStateHint(null);
        return;
      }
      const data = await fetchJSON(`/api/editor/snapshots?kind=${encodeURIComponent(state.kind)}`);
      state.snapshots = Array.isArray(data.snapshots) ? data.snapshots : [];
      rebuildSnapshots();
    } catch (e) {
      console.error(e);
    }
  }

  async function resolveRowIds(row) {
    const t = ((row.type || "") + "").toLowerCase();
    if (t === "season") return;
    if (!row.tmdb) return;

    try {
      const payload = {
        entity: row.type || "movie",
        ids: { tmdb: row.tmdb },
        locale: null,
        need: { ids: true, titles: true, year: true },
      };

      const data = await fetchJSON("/api/metadata/resolve", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!data || data.ok === false || !data.result) return;

      const result = data.result;
      const ids = result.ids || {};

      row.imdb = ids.imdb || row.imdb || "";
      row.tmdb = ids.tmdb || row.tmdb || "";
      row.trakt = ids.trakt || row.trakt || "";

      row.raw.ids = Object.assign({}, row.raw.ids || {}, {
        imdb: row.imdb || undefined,
        tmdb: row.tmdb || undefined,
        trakt: row.trakt || undefined,
      });

      if (row.imdb) {
        const imdbKey = `imdb:${row.imdb}`;
        const prevKey = (row.key || "").trim();
        if (!prevKey || /^imdb:tt\d+$/i.test(prevKey)) row.key = imdbKey;
      }
      if (result.title) {
        row.title = result.title;
        row.raw.title = result.title;
      }
      if (result.year) {
        row.year = String(result.year);
        row.raw.year = result.year;
      }
    } catch (err) {
      console.error("Metadata resolve failed", err);
    }
  }

  async function loadTrackerCounts() {
    try {
      const data = await fetchJSON("/api/maintenance/crosswatch-tracker");
      const counts = data && data.counts ? data.counts : {};

      let stateFiles = counts.state_files != null ? counts.state_files : 0;
      let snaps = counts.snapshots != null ? counts.snapshots : 0;

      if (stateFiles === 0 && snaps === 0) {
        for (let i = 0; i < 3; i += 1) {
          await new Promise(r => setTimeout(r, 400));
          const d2 = await fetchJSON("/api/maintenance/crosswatch-tracker");
          const c2 = d2 && d2.counts ? d2.counts : {};
          stateFiles = c2.state_files != null ? c2.state_files : stateFiles;
          snaps = c2.snapshots != null ? c2.snapshots : snaps;
          if (stateFiles || snaps) break;
        }
      }

      if (summaryStateFiles) summaryStateFiles.textContent = String(stateFiles);
      if (summarySnapshots) summarySnapshots.textContent = String(snaps);

      if (stateFiles === 0 && snaps === 0) showStateHint("tracker");
      else showStateHint(null);
    } catch (e) {
      console.error(e);
    }
  }

  async function loadState() {
    state.loading = true;
    setTag("warn", "Loading…");
    try {
      const params = new URLSearchParams({ kind: state.kind, source: state.source });
      if (state.source === "tracker" && state.snapshot) params.set("snapshot", state.snapshot);
      if (state.source === "state" && state.snapshot) params.set("provider", state.snapshot);
      if (state.source === "pair") {
        if (state.pair) params.set("pair", state.pair);
        if (state.snapshot) params.set("dataset", state.snapshot);
      }

      const data = await fetchJSON(`/api/editor?${params.toString()}`);
      if (data && data.ok === false) throw new Error(data.error || data.detail || "Load failed");

      if (state.source === "state") {
        state.baselineItems = data.items || {};
        state.manualAdds = data.manual_adds || {};
        state.manualBlocks = Array.isArray(data.manual_blocks) ? data.manual_blocks : [];

        const merged = Object.assign({}, state.baselineItems || {});
        for (const [k, v] of Object.entries(state.manualAdds || {})) {
          if (!(k in merged)) merged[k] = v;
        }

        state.items = merged;
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.rows = buildRows(state.items);

        const baselineKeys = new Set(Object.keys(state.baselineItems || {}));
        const blocked = new Set(
          (state.manualBlocks || []).map(x => String(x || "").trim()).filter(Boolean)
        );

        for (const row of state.rows) {
          row._origin = baselineKeys.has(row.key) ? "baseline" : "manual";
          if (row._origin === "baseline") row.deleted = blocked.has(row.key);
        }
      } else {
        state.items = data.items || {};
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.rows = buildRows(state.items);
      }

      state.hasChanges = false;
      state.page = 0;
      renderRows();

      if (state.source === "state") {
        const hasBaseline = state.baselineItems && Object.keys(state.baselineItems).length > 0;
        const hasManual = state.manualAdds && Object.keys(state.manualAdds).length > 0;
        const hasBlocks = Array.isArray(state.manualBlocks) && state.manualBlocks.length > 0;
        showStateHint(hasBaseline || hasManual || hasBlocks ? null : "state");
      } else {
        showStateHint(null);
      }

      setTag("loaded", "Loaded");
    } catch (e) {
      console.error(e);
      const msg = String(e || "");

      if (
        state.source === "state" &&
        (msg.includes("404") || /state\.json/i.test(msg) || /missing state/i.test(msg))
      ) {
        showStateHint("state");
        state.items = {};
        state.rows = [];
        renderRows();
        setTag("warn", "Missing state");
        setStatus("");
      } else {
        setTag("error", "Load failed");
        setStatus(msg);
      }
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
      setStatus(
        `Cannot save: ${missing.length} row${missing.length === 1 ? "" : "s"} have data but no Key. Fill the Key or delete the row.`
      );
      if (window.cxToast) window.cxToast("Fill Key for all rows with data before saving");
      return;
    }

    state.saving = true;
    setTag("warn", "Saving…");
    if (saveBtn) saveBtn.disabled = true;

    try {
      const items = {};
      const blocks = [];
      const seenBlocks = new Set();

      for (const row of state.rows) {
        if (row.deleted) {
          if (state.source === "state" && row._origin === "baseline") {
            const k = (row.key || "").trim();
            if (k) {
              const kl = k.toLowerCase();
              if (!seenBlocks.has(kl)) {
                seenBlocks.add(kl);
                blocks.push(k);
              }
            }
          }
          continue;
        }

        if (state.source === "state" && row._origin === "baseline") continue;

        const key = (row.key || "").trim();
        if (!key) continue;

        const raw = row.raw || {};
        const ids = raw.ids || {};

        if (row.imdb) ids.imdb = row.imdb;
        else delete ids.imdb;

        if (row.tmdb) ids.tmdb = row.tmdb;
        else delete ids.tmdb;

        if (row.trakt) ids.trakt = row.trakt;
        else delete ids.trakt;

        raw.ids = ids;
        raw.type = row.type || raw.type || null;
        raw.title = row.title ? row.title : raw.title || null;

        const y = (row.year || "").trim();
        const n = y ? parseInt(y, 10) : NaN;
        raw.year = Number.isFinite(n) ? n : null;

        items[key] = raw;
      }

      const payload = { kind: state.kind, source: state.source, items };
      if (state.source === "pair") {
        payload.pair = state.pair;
        payload.dataset = state.snapshot;
      }
      if (state.source === "state") {
        payload.provider = state.snapshot;
        payload.blocks = blocks;
      }

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
      if (saveBtn) saveBtn.disabled = false;
    }
  }

  function addRow() {
    const raw = { ids: {}, type: "movie", title: "", year: null };
    state.rows.unshift({
      _rid: state.ridSeq++,
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
      _origin: state.source === "state" ? "manual" : "tracker",
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
      if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      else {
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

  if (blockedOnlyBtn) {
    blockedOnlyBtn.addEventListener("click", () => {
      state.blockedOnly = !state.blockedOnly;
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
        setTag("loaded", "Loaded");
        if (window.cxToast) window.cxToast("Tracker export downloaded");
      } catch (e) {
        console.error(e);
        setTag("error", "Download failed");
        setStatus(String(e));
      }
    });
  }

  if (uploadBtn && uploadInput) {
    uploadBtn.addEventListener("click", () => uploadInput.click());

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
        if (data.files != null) parts.push(`${data.files} file${data.files === 1 ? "" : "s"}`);
        if (data.states != null) parts.push(`${data.states} state file${data.states === 1 ? "" : "s"}`);
        if (data.snapshots != null) parts.push(`${data.snapshots} snapshot${data.snapshots === 1 ? "" : "s"}`);

        let msg = "Imported " + (parts.length ? parts.join(", ") : "tracker data");
        if (data.overwritten) msg += ` (${data.overwritten} overwritten)`;

        setTag("loaded", "Loaded");
        setStatusSticky(msg, 5000);
        if (window.cxToast) window.cxToast(msg);

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

  if (sourceSel) {
    sourceSel.addEventListener("change", async () => {
      state.source = (sourceSel.value || "tracker").trim();
      state.snapshot = "";
      state.page = 0;
      persistUIState();
      syncSourceUI();
      clearSelection();
      if (state.source === "state") await loadImportProviders();
      else if (importRow) syncImportUI();
      if (state.source === "pair") await loadPairs();
      if (state.source === "tracker") await loadTrackerCounts();
      await loadSnapshots();
      await loadState();
    });
  }

  if (kindSel) {
    kindSel.addEventListener("change", async () => {
      const prevKind = state.kind;
      state.kind = (kindSel.value || "watchlist").trim();
      if (prevKind === "watchlist" && state.kind !== "watchlist") {
        state.typeFilter.season = true;
        state.typeFilter.episode = true;
      }
      syncKindUI();
      syncTypeFilterUI();
      syncStateBulkUI();
      clearSelection();
      if (state.source !== "state") state.snapshot = "";
      state.page = 0;
      persistUIState();
      await loadSnapshots();
      renderRows();
      await loadState();
    });
  }

  if (snapSel) {
    snapSel.addEventListener("change", async () => {
      state.snapshot = snapSel.value || "";
      state.page = 0;
      persistUIState();
      await loadState();
    });
  }

  if (importProviderSel) {
    importProviderSel.addEventListener("change", () => {
      state.importProvider = importProviderSel.value || "";
      syncImportUI();
    });
  }

  if (importModeSel) {
    importModeSel.addEventListener("change", () => {
      state.importMode = importModeSel.value || "replace";
    });
  }

  if (importWatchlistCb) {
    importWatchlistCb.addEventListener("change", () => {
      state.importFeatures.watchlist = !!importWatchlistCb.checked;
    });
  }

  if (importHistoryCb) {
    importHistoryCb.addEventListener("change", () => {
      state.importFeatures.history = !!importHistoryCb.checked;
    });
  }

  if (importRatingsCb) {
    importRatingsCb.addEventListener("change", () => {
      state.importFeatures.ratings = !!importRatingsCb.checked;
    });
  }

  if (importRunBtn) {
    importRunBtn.addEventListener("click", async () => {
      await runStateImport();
    });
  }


  if (filterInput) {
    filterInput.addEventListener("input", () => {
      state.filter = filterInput.value || "";
      state.page = 0;
      clearSelection();
      persistUIState();
      renderRows();
    });
  }

  if (pairSel) {
    pairSel.addEventListener("change", async () => {
      state.pair = pairSel.value || "";
      state.snapshot = "";
      state.page = 0;
      persistUIState();
      await loadSnapshots();
      await loadState();
    });
  }

  if (reloadBtn) {
    reloadBtn.addEventListener("click", async () => {
      state.snapshot = (snapSel && snapSel.value) ? snapSel.value : "";
      state.page = 0;
      if (state.source === "pair") await loadPairs();
      if (state.source !== "state") await loadTrackerCounts();
      await loadSnapshots();
      await loadState();
    });
  }

  if (selectPage) {
    selectPage.addEventListener("change", () => {
      if (!state.selected) state.selected = new Set();
      const on = !!selectPage.checked;
      for (const rid of state.pageRids || []) {
        if (on) state.selected.add(rid);
        else state.selected.delete(rid);
      }
      syncBulkBar();
      syncSelectPageCheckbox();
      renderRows();
    });
  }

  if (bulkRemoveBtn) bulkRemoveBtn.addEventListener("click", () => bulkSetDeletedForSelected(true));
  if (bulkRestoreBtn) bulkRestoreBtn.addEventListener("click", () => bulkSetDeletedForSelected(false));
  if (bulkClearBtn) bulkClearBtn.addEventListener("click", () => { clearSelection(); renderRows(); });

  if (bulkBlockTypeBtn) bulkBlockTypeBtn.addEventListener("click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, true));
  if (bulkUnblockTypeBtn) bulkUnblockTypeBtn.addEventListener("click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, false));

  if (addBtn) addBtn.addEventListener("click", addRow);
  if (saveBtn) saveBtn.addEventListener("click", saveState);

  window.addEventListener("beforeunload", e => {
    if (!state.hasChanges) return;
    e.preventDefault();
    e.returnValue = "";
  });

  if (stateDownloadBtn) {
    stateDownloadBtn.addEventListener("click", async () => {
      try {
        setTag("warn", "Preparing download…");
        const res = await fetch("/api/editor/state/manual/export", { cache: "no-store" });
        if (!res.ok) throw new Error(`Download failed: ${res.status}`);
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "crosswatch-state-policy.json";
        document.body.appendChild(a);
        a.click();
        setTimeout(() => {
          URL.revokeObjectURL(url);
          a.remove();
        }, 0);
        setTag("loaded", "Loaded");
        if (window.cxToast) window.cxToast("Policy export downloaded");
      } catch (e) {
        console.error(e);
        setTag("error", "Download failed");
        setStatus(String(e));
      }
    });
  }

  if (stateUploadBtn && stateUploadInput) {
    stateUploadBtn.addEventListener("click", () => stateUploadInput.click());

    stateUploadInput.addEventListener("change", async () => {
      const file = stateUploadInput.files && stateUploadInput.files[0];
      if (!file) return;

      try {
        const fd = new FormData();
        fd.append("file", file);
        setTag("warn", "Importing…");
        setStatus("");

        const res = await fetch("/api/editor/state/manual/import?mode=merge", { method: "POST", body: fd });
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
        if (data.providers != null) parts.push(`${data.providers} provider${data.providers === 1 ? "" : "s"}`);
        if (data.blocks != null) parts.push(`${data.blocks} block${data.blocks === 1 ? "" : "s"}`);
        if (data.adds != null) parts.push(`${data.adds} add${data.adds === 1 ? "" : "s"}`);

        const msg = "Imported " + (parts.length ? parts.join(", ") : "policy");
        if (window.cxToast) window.cxToast(msg);
        setTag("warn", "Imported");
        await loadSnapshots();
        await loadState();
      } catch (e) {
        console.error(e);
        setTag("error", "Import failed");
        setStatus(String(e));
      } finally {
        try { stateUploadInput.value = ""; } catch (_) {}
      }
    });
  }

  (async () => {
    syncSourceUI();
    await loadImportProviders();
    setTag("warn", state.source === "state" ? "Loading current state…" : state.source === "pair" ? "Loading pair cache…" : "Loading tracker state…");
    if (state.source === "pair") await loadPairs();
    if (state.source === "tracker") await loadTrackerCounts();
    await loadSnapshots();
    await loadState();
  })();
  }

  function bootWhenReady() {
    if (cwEditorBooted) return;
    if (document.getElementById("page-editor")) {
      bootEditor();
      return;
    }
    const obs = new MutationObserver(() => {
      if (!document.getElementById("page-editor")) return;
      obs.disconnect();
      bootEditor();
    });
    obs.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootWhenReady, { once: true });
  } else {
    bootWhenReady();
  }

})();
