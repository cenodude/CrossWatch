/* assets/js/editor.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const PAGE_SIZE_OPTIONS = [50, 100, 150, 200];
  const DEFAULT_PAGE_SIZE = 50;
  const COLUMN_KEYS = ["key", "type", "title", "year", "id", "imdb", "tvdb", "trakt", "simkl", "anilist", "extra"];
  const COLUMN_LAYOUT_VERSION = 4;
  const DEFAULT_COLUMN_ORDER = COLUMN_KEYS.slice();
  const DEFAULT_COLUMN_VISIBILITY = { key: true, type: true, title: true, year: false, id: true, imdb: false, tvdb: false, trakt: false, simkl: false, anilist: false, extra: true };
  const DEFAULT_COLUMN_WIDTHS = { key: 132, type: 126, title: 360, year: 92, id: 132, imdb: 150, tvdb: 120, trakt: 120, simkl: 120, anilist: 120, extra: 220 };
  const MIN_COLUMN_WIDTHS = { key: 96, type: 116, title: 220, year: 84, id: 120, imdb: 130, tvdb: 110, trakt: 110, simkl: 110, anilist: 110, extra: 180 };
  const MAX_COLUMN_WIDTHS = { key: 340, type: 240, title: 760, year: 180, id: 280, imdb: 280, tvdb: 240, trakt: 240, simkl: 240, anilist: 240, extra: 560 };
  const COLUMN_META = {
    key: { icon: "key", label: "Key" },
    type: { icon: "category", label: "Type" },
    title: { icon: "title", label: "Title", required: true },
    year: { icon: "event", label: "Year" },
    id: { icon: "fingerprint", label: "IDs" },
    imdb: { icon: "tag", label: "IMDb" },
    tvdb: { icon: "dns", label: "TVDB" },
    trakt: { icon: "confirmation_number", label: "Trakt" },
    simkl: { icon: "hub", label: "SIMKL" },
    anilist: { icon: "animation", label: "AniList" },
    extra: { icon: "tune", label: "Extra" },
  };
  const STORAGE_KEY = "cw-editor-ui";
  let cwEditorBooted = false;
  let cwEditorBootRetryWired = false;

  function bootEditor() {
    if (cwEditorBooted) return;
    const host = document.getElementById("page-editor");
    if (!host) return;
    cwEditorBooted = true;

  const state = {
    source: "state",
    kind: "watchlist",
    snapshot: "",
    instance: "default",
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
    lastSyncAt: null,
    saving: false,
    snapshots: [],
    instance: "default",
    playlistEndpoints: [],
    playlistEndpointsLoaded: false,
    playlistResource: null,
    playlistWarnings: [],
    playlistOriginalKeys: [],
    importEnabled: false,
    importProviders: [],
    importProvider: "",
    importProviderInstance: "default",
    importMode: "replace",
    importFeatures: { watchlist: true, history: true, ratings: true, progress: true },
    hasChanges: false,
    page: 0,
    pageSize: DEFAULT_PAGE_SIZE,
    blockedOnly: false,
    typeFilter: { movie: true, show: true, anime: true, season: true, episode: true },
    columnVisibility: { ...DEFAULT_COLUMN_VISIBILITY },
    columnOrder: DEFAULT_COLUMN_ORDER.slice(),
    columnWidths: { ...DEFAULT_COLUMN_WIDTHS },
    wideView: false,
    sortKey: "title",
    sortDir: "asc",
  };
  const editorModules = window.CW && window.CW.Editor;
  if (!editorModules) throw new Error("Editor module registry missing");
  const requireEditorModule = name => {
    const mod = editorModules[name];
    if (!mod) throw new Error(`Editor module missing: ${name}`);
    return mod;
  };
  const editorDateTime = requireEditorModule("DateTime");
  const editorSearch = requireEditorModule("Search");
  const editorRows = requireEditorModule("Rows");
  const editorSources = requireEditorModule("Sources");
  const editorImporters = requireEditorModule("Importers");
  const editorPersistence = requireEditorModule("Persistence");
  const editorTable = requireEditorModule("Table");
  const editorChrome = requireEditorModule("Chrome");
  const editorRowEditor = requireEditorModule("RowEditor");
  const editorTableController = requireEditorModule("TableController");
  const editorFileUtils = requireEditorModule("FileUtils");
  const editorLoadController = requireEditorModule("LoadController");
  const editorExtraEditors = requireEditorModule("ExtraEditors");
  const editorMetadataReplacer = requireEditorModule("MetadataReplacer");
  const editorSendModal = requireEditorModule("SendModal");
  const rowBuilderOptions = () => ({
    nextRid: () => state.ridSeq++,
    sort: state.source !== "playlist",
  });

  function isRequiredColumn(column) {
    return !!(COLUMN_META[column] && COLUMN_META[column].required);
  }

  function normalizeColumnOrder(order) {
    const seen = new Set();
    const out = [];
    (Array.isArray(order) ? order : []).forEach(column => {
      if (!COLUMN_KEYS.includes(column) || seen.has(column)) return;
      seen.add(column);
      out.push(column);
    });
    COLUMN_KEYS.forEach(column => {
      if (!seen.has(column)) out.push(column);
    });
    return out;
  }

  function clampColumnWidth(column, value) {
    const fallback = DEFAULT_COLUMN_WIDTHS[column] || 160;
    const n = Number(value);
    const min = MIN_COLUMN_WIDTHS[column] || 80;
    const max = MAX_COLUMN_WIDTHS[column] || 720;
    return Math.max(min, Math.min(max, Math.round(Number.isFinite(n) ? n : fallback)));
  }

  function restoreUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const saved = JSON.parse(raw);

      const sources = ["state", "manual", "playlist"];
      if (saved.source && sources.includes(saved.source)) state.source = saved.source;

      if (typeof saved.blockedOnly === "boolean") state.blockedOnly = saved.blockedOnly;

      const kinds = ["watchlist", "history", "ratings", "progress"];
      if (saved.kind && kinds.includes(saved.kind)) state.kind = saved.kind;

      if (typeof saved.snapshot === "string") state.snapshot = saved.snapshot;
      if (typeof saved.instance === "string" && saved.instance.trim()) state.instance = saved.instance;

      if (typeof saved.filter === "string") state.filter = saved.filter;
      if (PAGE_SIZE_OPTIONS.includes(Number(saved.pageSize))) state.pageSize = Number(saved.pageSize);
      if (typeof saved.wideView === "boolean") state.wideView = saved.wideView;

      if (saved.typeFilter && typeof saved.typeFilter === "object") {
        ["movie", "show", "anime", "season", "episode"].forEach(t => {
          if (typeof saved.typeFilter[t] === "boolean") state.typeFilter[t] = saved.typeFilter[t];
        });
      }
      const restoreColumnLayout = saved.columnLayoutVersion === COLUMN_LAYOUT_VERSION;
      if (restoreColumnLayout && saved.columnVisibility && typeof saved.columnVisibility === "object") {
        COLUMN_KEYS.forEach(k => {
          if (typeof saved.columnVisibility[k] === "boolean") state.columnVisibility[k] = saved.columnVisibility[k];
        });
      }
      if (restoreColumnLayout && Array.isArray(saved.columnOrder)) state.columnOrder = normalizeColumnOrder(saved.columnOrder);
      if (restoreColumnLayout && saved.columnWidths && typeof saved.columnWidths === "object") {
        COLUMN_KEYS.forEach(k => {
          if (saved.columnWidths[k] != null) state.columnWidths[k] = clampColumnWidth(k, saved.columnWidths[k]);
        });
      }

      const sortKeys = COLUMN_KEYS;
      if (saved.sortKey && sortKeys.includes(saved.sortKey)) state.sortKey = saved.sortKey;
      if (saved.sortDir === "asc" || saved.sortDir === "desc") state.sortDir = saved.sortDir;
    } catch (_) {}
  }
  restoreUIState();

  host.innerHTML = `<div class="cw-root"><div class="cw-topline cw-page-hero cw-page-hero-editor" data-hero-icon="edit_note"><div class="cw-head-copy cw-page-hero-copy"><div class="cw-page-hero-kicker">EDITOR</div><div class="cw-title-row"><div><div class="cw-title cw-page-hero-title">Editor</div><div class="cw-sub cw-page-hero-sub">Edit your current state or playlist endpoints</div></div></div></div><div class="cw-editor-hero-summary cw-page-hero-actions" id="cw-hero-summary" aria-label="Editor summary"><div class="cw-editor-hero-seg"><strong id="cw-pill-source">Current state</strong><span>source</span></div><div class="cw-editor-hero-seg"><strong id="cw-pill-kind">Watchlist</strong><span>view</span></div><div class="cw-editor-hero-seg cw-editor-hero-count"><strong id="cw-pill-count">0</strong><span>rows</span></div><div class="cw-editor-hero-seg cw-editor-hero-sync"><span>Synced</span><strong id="cw-pill-sync">never</strong></div><button id="cw-reload" class="cw-editor-refresh" type="button" title="Refresh editor data" aria-label="Refresh editor data"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button></div></div><div class="cw-wrap"><div class="cw-main"><div class="cw-controls"><input id="cw-filter" class="cw-input" placeholder="Filter by key / title / id..."><span class="cw-status-text" id="cw-status"></span><label class="cw-page-size-control" for="cw-page-size"><span>Rows</span><select id="cw-page-size" class="cw-select"><option value="50">50</option><option value="100">100</option><option value="150">150</option><option value="200">200</option></select></label><div class="cw-controls-spacer"></div><div class="cw-bulk" id="cw-bulk" style="display:none"><span class="cw-bulk-count" id="cw-bulk-count"></span><button id="cw-bulk-remove" class="cw-btn danger" type="button"></button><button id="cw-bulk-restore" class="cw-btn" type="button"></button><button id="cw-bulk-clear" class="cw-btn" type="button">Clear</button></div><button id="cw-add" class="cw-btn" type="button">Add row</button><button id="cw-save" class="cw-btn primary" type="button">Save changes</button></div><div class="cw-table-wrap" id="cw-table-wrap"><div class="cw-table-scroll"><table class="cw-table"><thead><tr><th style="width:34px"><input id="cw-select-page" class="cw-checkbox" type="checkbox" title="Select page"></th><th class="cw-action-head" style="width:46px"></th><th class="cw-col-key sortable" style="width:12%" data-sort="key">Key</th><th class="cw-col-type sortable" style="width:13%" data-sort="type">Type</th><th class="cw-col-title sortable" style="width:33%" data-sort="title">Title</th><th class="cw-col-year" style="width:84px">Year</th><th class="cw-col-id cw-col-id-a" style="width:12%" id="cw-col-id-a">TMDB</th><th class="cw-col-extra sortable" style="width:21%" data-sort="extra">Extra</th></tr></thead><tbody id="cw-tbody"></tbody></table></div></div><div class="cw-pager" id="cw-pager" style="display:none"><button id="cw-prev" class="cw-btn" type="button">Previous</button><span id="cw-page-info" class="cw-page-info"></span><button id="cw-next" class="cw-btn" type="button">Next</button></div><div class="cw-empty" id="cw-empty" role="status" style="display:none"><span class="material-symbols-rounded cw-empty-icon" aria-hidden="true">table_rows</span><div class="cw-empty-text">No rows match this view.</div></div></div><aside class="cw-side"><div class="ins-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">tune</span></div><div class="ins-title">Workspace</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Source</label><select id="cw-source" class="cw-select"><option value="state">Current State</option><option value="manual">Manual Overrides</option></select><label>Kind</label><select id="cw-kind" class="cw-select"><option value="watchlist">Watchlist</option><option value="history">History</option><option value="ratings">Ratings</option><option value="progress">Progress</option></select><label id="cw-pair-label" style="display:none">Pair</label><select id="cw-pair" class="cw-select" style="display:none"></select><label id="cw-snapshot-label">Snapshot</label><select id="cw-snapshot" class="cw-select"><option value="">Latest</option></select><label id="cw-instance-label" style="display:none">Profile</label><select id="cw-instance" class="cw-select" style="display:none"><option value="default">Default</option></select><div id="cw-instance-shared" class="cw-shared-note" role="note" style="display:none"></div></div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><div class="field-label">Types</div><div id="cw-type-filter" class="cw-type-filter"><button type="button" data-type="movie" class="cw-type-chip active">Movies</button><button type="button" data-type="show" class="cw-type-chip active">Shows</button><button type="button" data-type="anime" class="cw-type-chip active">Anime</button><button type="button" data-type="season" class="cw-type-chip active">Seasons</button><button type="button" data-type="episode" class="cw-type-chip active">Episodes</button><button type="button" id="cw-blocked-only" class="cw-type-chip">Blocked</button></div></div></div><div class="ins-row" id="cw-state-bulk" style="display:none"><details class="cw-collapse" id="cw-bulk-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">Block rules</summary><div style="display:flex;flex-direction:column;gap:8px;width:100%;margin-top:10px"><select id="cw-bulk-type" class="cw-select" style="width:100%"></select><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button id="cw-bulk-block-type" class="cw-btn danger" type="button" style="flex:1 1 0;min-width:120px">Block all</button><button id="cw-bulk-unblock-type" class="cw-btn" type="button" style="flex:1 1 0;min-width:120px">Unblock all</button></div><div class="cw-status-text">Current State only â€¢ affects baseline items</div></div></details></div><div class="ins-row" id="cw-import-row" style="display:none"><details class="cw-collapse" id="cw-import-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">Import provider state</summary><div style="display:flex;flex-direction:column;gap:10px;width:100%;margin-top:10px"><div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center"><select id="cw-import-provider" class="cw-select" style="flex:1;min-width:200px"></select><select id="cw-import-instance" class="cw-select" style="min-width:180px"></select><select id="cw-import-mode" class="cw-select" style="min-width:180px"><option value="replace">Replace baseline</option><option value="merge">Merge (keep old)</option></select></div><div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center"><label id="cw-import-watchlist-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-watchlist" class="cw-checkbox" type="checkbox" checked>Watchlist </label><label id="cw-import-history-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-history" class="cw-checkbox" type="checkbox" checked>History </label><label id="cw-import-ratings-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-ratings" class="cw-checkbox" type="checkbox" checked>Ratings </label><label id="cw-import-progress-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-progress-cb" class="cw-checkbox" type="checkbox" checked>Progress </label><span style="flex:1 1 auto"></span><button id="cw-import-run" class="cw-btn sm" type="button">Import</button></div><div id="cw-import-progress" style="display:none"><div class="cw-progress"><span></span></div><div class="cw-status-text" id="cw-import-progress-text" style="margin-top:6px"></div></div></div></details></div></div><div class="ins-card"><div class="ins-row" style="align-items:center"><div class="ins-icon"><span class="material-symbol">insights</span></div><div class="ins-title" style="margin-right:auto">Pulse</div><span class="cw-tag" id="cw-tag-status"><span class="cw-tag-dot"></span><span id="cw-tag-label">Idle</span></span></div><div class="ins-row"><div class="ins-metrics"><div class="metric-row"><div class="metric"><span class="material-symbol">view_list</span><div><div class="m-val" id="cw-summary-total">0</div><div class="m-lbl">Total rows</div></div></div><div class="metric"><span class="material-symbol">visibility</span><div><div class="m-val" id="cw-summary-visible">0</div><div class="m-lbl">Rows visible</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">movie</span><div><div class="m-val" id="cw-summary-movies">0</div><div class="m-lbl">Movies</div></div></div><div class="metric"><span class="material-symbol">monitoring</span><div><div class="m-val" id="cw-summary-shows">0</div><div class="m-lbl">Shows</div></div></div><div class="metric"><span class="material-symbol">layers</span><div><div class="m-val" id="cw-summary-seasons">0</div><div class="m-lbl">Seasons</div></div></div><div class="metric"><span class="material-symbol">live_tv</span><div><div class="m-val" id="cw-summary-episodes">0</div><div class="m-lbl">Episodes</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">description</span><div><div class="m-val" id="cw-summary-state-files">0</div><div class="m-lbl">State files</div></div></div><div class="metric"><span class="material-symbol">folder_copy</span><div><div class="m-val" id="cw-summary-snapshots">0</div><div class="m-lbl">Snapshots</div></div></div></div><div id="cw-state-hint" class="cw-state-hint" style="display:none"><strong>No sync state found.</strong> Run a CrossWatch sync once to generate it.</div></div></div></div><div class="ins-card" id="cw-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">Archive</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Export / Import</label><div class="cw-backup-actions"><button id="cw-download" class="cw-btn" type="button">Download ZIP</button><button id="cw-upload" class="cw-btn" type="button">Import file</button><input id="cw-upload-input" type="file" accept=".zip,.json" style="display:none"></div></div></div></div><div class="ins-card" id="cw-state-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">Policy backup</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Export / Import</label><div class="cw-backup-actions"><button id="cw-state-download" class="cw-btn" type="button">Download JSON</button><button id="cw-state-upload" class="cw-btn" type="button">Import file</button><input id="cw-state-upload-input" type="file" accept=".json" style="display:none"></div></div></div></div></aside></div></div>`;

  editorChrome.wireStaticLabels(host);
  editorChrome.prepareSourceOptions(host);
  editorChrome.addTrackerNotice(host);
  editorChrome.ensureFieldNames(host);

  const $ = id => document.getElementById(id);
  const pickEls = spec => Object.fromEntries(Object.entries(spec).map(([key, id]) => [key, $(id)]));
  const {
    sourceSel, kindSel, pairLabel, pairSel, snapLabel, snapSel, instanceLabel, instanceSel,
    filterInput, pageSizeSel, reloadBtn, addBtn, saveBtn, tbody, empty, statusEl, tag, tagLabel,
    summaryVisible, summaryTotal, summaryMovies, summaryShows, summarySeasons, summaryEpisodes,
    summaryStateFiles, summarySnapshots, stateHint, pager, prevBtn, nextBtn, pageInfo,
    typeFilterWrap, backupCard, blockedOnlyBtn, downloadBtn, uploadBtn, uploadInput,
    stateBackupCard, stateDownloadBtn, stateUploadBtn, stateUploadInput,
    pillSource, pillKind, pillCount, pillSync,
    importRow, importProviderSel, importInstanceSel, importWatchlistCb, importHistoryCb,
    importRatingsCb, importProgressCb, importModeSel, importRunBtn, importWatchlistWrap,
    importHistoryWrap, importRatingsWrap, importProgressFeatWrap, importProgressWrap,
    importProgressText,
    selectPage, bulkWrap, bulkCount, bulkRemoveBtn, bulkRestoreBtn, bulkClearBtn,
    stateBulkRow, bulkTypeSel, bulkBlockTypeBtn, bulkUnblockTypeBtn, trackerNotice,
  } = pickEls({
    trackerNotice: "cw-tracker-notice",
    sourceSel: "cw-source",
    kindSel: "cw-kind",
    pairLabel: "cw-pair-label",
    pairSel: "cw-pair",
    snapLabel: "cw-snapshot-label",
    snapSel: "cw-snapshot",
    instanceLabel: "cw-instance-label",
    instanceSel: "cw-instance",
    filterInput: "cw-filter",
    pageSizeSel: "cw-page-size",
    reloadBtn: "cw-reload",
    addBtn: "cw-add",
    saveBtn: "cw-save",
    tbody: "cw-tbody",
    empty: "cw-empty",
    statusEl: "cw-status",
    tag: "cw-tag-status",
    tagLabel: "cw-tag-label",
    summaryVisible: "cw-summary-visible",
    summaryTotal: "cw-summary-total",
    summaryMovies: "cw-summary-movies",
    summaryShows: "cw-summary-shows",
    summarySeasons: "cw-summary-seasons",
    summaryEpisodes: "cw-summary-episodes",
    summaryStateFiles: "cw-summary-state-files",
    summarySnapshots: "cw-summary-snapshots",
    stateHint: "cw-state-hint",
    pager: "cw-pager",
    prevBtn: "cw-prev",
    nextBtn: "cw-next",
    pageInfo: "cw-page-info",
    typeFilterWrap: "cw-type-filter",
    backupCard: "cw-backup-card",
    blockedOnlyBtn: "cw-blocked-only",
    downloadBtn: "cw-download",
    uploadBtn: "cw-upload",
    uploadInput: "cw-upload-input",
    stateBackupCard: "cw-state-backup-card",
    stateDownloadBtn: "cw-state-download",
    stateUploadBtn: "cw-state-upload",
    stateUploadInput: "cw-state-upload-input",
    pillSource: "cw-pill-source",
    pillKind: "cw-pill-kind",
    pillCount: "cw-pill-count",
    pillSync: "cw-pill-sync",
    importRow: "cw-import-row",
    importProviderSel: "cw-import-provider",
    importInstanceSel: "cw-import-instance",
    importWatchlistCb: "cw-import-watchlist",
    importHistoryCb: "cw-import-history",
    importRatingsCb: "cw-import-ratings",
    importProgressCb: "cw-import-progress-cb",
    importModeSel: "cw-import-mode",
    importRunBtn: "cw-import-run",
    importWatchlistWrap: "cw-import-watchlist-wrap",
    importHistoryWrap: "cw-import-history-wrap",
    importRatingsWrap: "cw-import-ratings-wrap",
    importProgressFeatWrap: "cw-import-progress-wrap",
    importProgressWrap: "cw-import-progress",
    importProgressText: "cw-import-progress-text",
    selectPage: "cw-select-page",
    bulkWrap: "cw-bulk",
    bulkCount: "cw-bulk-count",
    bulkRemoveBtn: "cw-bulk-remove",
    bulkRestoreBtn: "cw-bulk-restore",
    bulkClearBtn: "cw-bulk-clear",
    stateBulkRow: "cw-state-bulk",
    bulkTypeSel: "cw-bulk-type",
    bulkBlockTypeBtn: "cw-bulk-block-type",
    bulkUnblockTypeBtn: "cw-bulk-unblock-type",
  });

  const columnsBtn = document.createElement("button");
  columnsBtn.id = "cw-columns-btn";
  columnsBtn.className = "cw-btn cw-columns-btn";
  columnsBtn.type = "button";
  columnsBtn.title = "Columns";
  columnsBtn.setAttribute("aria-label", "Columns");
  columnsBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">filter_alt</span>`;
  pageSizeSel?.closest(".cw-page-size-control")?.insertAdjacentElement("afterend", columnsBtn);

  const wideBtn = document.createElement("button");
  wideBtn.id = "cw-wide-btn";
  wideBtn.className = "cw-btn cw-wide-btn";
  wideBtn.type = "button";
  wideBtn.title = "Wide view";
  wideBtn.setAttribute("aria-label", "Wide view");
  wideBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">fullscreen</span>`;
  columnsBtn.insertAdjacentElement("afterend", wideBtn);

  const bulkSendBtn = document.createElement("button");
  bulkSendBtn.id = "cw-bulk-send";
  bulkSendBtn.className = "cw-btn";
  bulkSendBtn.type = "button";
  bulkWrap?.insertBefore(bulkSendBtn, bulkRemoveBtn || bulkClearBtn || null);

  if (backupCard) backupCard.remove();
  [summaryStateFiles, summarySnapshots].forEach(el => el?.closest(".metric")?.remove());

  editorChrome.decorateImportPanel({
    importProviderSel,
    importInstanceSel,
    importModeSel,
    importWatchlistWrap,
    importHistoryWrap,
    importRatingsWrap,
    importProgressFeatWrap,
    importRunBtn,
    importProgressWrap,
  });
  editorChrome.decoratePolicyBackupPanel({
    stateBackupCard,
    importRow,
    stateDownloadBtn,
    stateUploadBtn,
    stateUploadInput,
  });
  editorChrome.decorateEditorChrome({
    addBtn,
    saveBtn,
    typeFilterWrap,
    state,
    syncTypeFilterUI,
    persistUIState,
    renderRows,
  });
  let sortHeaders = Array.from(host.querySelectorAll(".cw-table th[data-sort]"));
  let columnLayoutResizeTimer = 0;
  const providerMeta = window.CW?.ProviderMeta || {};
  const providerKey = (name) => String(name || "").trim().toUpperCase();
  const providerLabel = (name, fallback = "") => {
    const key = providerKey(name);
    return providerMeta.label?.(key) || providerMeta.label?.(name) || fallback || String(name || "");
  };
  function syncProviderIconSelect(selectEl, show) {
    if (!selectEl) return;
    const helper = window.CW?.ProfileSelect?.enhanceProvider;
    const wrap = selectEl.nextElementSibling && selectEl.nextElementSibling.classList?.contains("cw-icon-select")
      ? selectEl.nextElementSibling
      : null;
    if (!show || typeof helper !== "function") {
      selectEl.classList.remove("cw-icon-select-native");
      if (wrap) wrap.style.display = "none";
      return;
    }
    selectEl.classList.add("cw-icon-select-native");
    helper(selectEl, {
      className: "cw-editor-icon-select",
      getOptionData: (value, option) => {
        const key = providerKey(value);
        const label = providerLabel(value, option?.textContent || value || "Select");
        const icon = providerMeta.logLogoPath?.(key) || providerMeta.logoPath?.(key) || providerMeta.logLogoPath?.(value) || providerMeta.logoPath?.(value) || "";
        return {
          label,
          icons: icon && value ? [{ src: icon, alt: label }] : [],
          disabled: !!option?.disabled,
        };
      },
    });
    const nextWrap = selectEl.nextElementSibling && selectEl.nextElementSibling.classList?.contains("cw-icon-select")
      ? selectEl.nextElementSibling
      : null;
    if (nextWrap) nextWrap.style.display = "";
  }

  function syncProfileIconSelect(selectEl, show) {
    if (!selectEl) return;
    const helper = window.CW?.ProfileSelect?.enhanceProfile;
    const wrap = selectEl.nextElementSibling && selectEl.nextElementSibling.classList?.contains("cw-icon-select")
      ? selectEl.nextElementSibling
      : null;
    if (!show || typeof helper !== "function") {
      selectEl.classList.remove("cw-icon-select-native");
      if (wrap) wrap.style.display = "none";
      return;
    }
    helper(selectEl, {
      className: "cw-editor-icon-select",
    });
    const nextWrap = selectEl.nextElementSibling && selectEl.nextElementSibling.classList?.contains("cw-icon-select")
      ? selectEl.nextElementSibling
      : null;
    if (nextWrap) nextWrap.style.display = "";
  }

  function syncSnapshotControlVisibility() {
    return editorSources.syncSnapshotControlVisibility(sourceContext());
  }

  function sourceContext() {
    return {
      state,
      host,
      sourceSel,
      pairLabel,
      pairSel,
      snapLabel,
      snapSel,
      kindSel,
      instanceLabel,
      instanceSel,
      backupCard,
      stateBackupCard,
      blockedOnlyBtn,
      trackerNotice,
      stateHint,
      escapeHtml: _escapeHtml,
      providerLabel,
      fetchJSON,
      syncProviderIconSelect,
      syncProfileIconSelect,
      renderInstanceOptions,
      loadInstanceOptions,
      persistUIState,
      syncKindUI,
      syncTypeFilterUI,
      syncStateBulkUI,
      syncImportUI,
      syncActionButtons,
      syncHeaderPills,
    };
  }

  function importContext() {
    return {
      state,
      importRow,
      importProviderSel,
      importInstanceSel,
      importWatchlistCb,
      importHistoryCb,
      importRatingsCb,
      importProgressCb,
      importModeSel,
      importRunBtn,
      importWatchlistWrap,
      importHistoryWrap,
      importRatingsWrap,
      importProgressFeatWrap,
      importProgressWrap,
      importProgressText,
      providerLabel,
      fetchJSON,
      syncProviderIconSelect,
      syncProfileIconSelect,
      renderInstanceOptions,
      persistUIState,
      setTag,
      setStatus,
      setStatusSticky,
      loadSnapshots,
      loadState,
    };
  }

  function persistenceContext() {
    return {
      state,
      fetchJSON,
      setTag,
      setStatus,
      syncActionButtons,
      loadState,
      loadSnapshots,
      isPolicySource,
      isProviderPickerSource,
      isTrackerSource,
      isManualSource,
      confirm: message => window.confirm(message),
    };
  }

  let statusStickyUntil = 0;

  function isTrackerSource() {
    return editorSources.isTrackerSource(state);
  }

  function isManualSource() {
    return editorSources.isManualSource(state);
  }

  function isProviderPickerSource() {
    return editorSources.isProviderPickerSource(state);
  }

  function isPolicySource() {
    return editorSources.isPolicySource(state);
  }

  function normalizeSource(value) {
    return editorSources.normalizeSource(value);
  }

  function ensureTrackerOption() {
    return editorSources.ensureTrackerOption(sourceSel);
  }

  function formatEpisodeVisualTitle(row) {
    const raw = row && row.raw ? row.raw : {};
    const t = String(raw.type || row?.type || "").toLowerCase();
    if (t !== "episode") return "";
    const series = String(raw.series_title || "").trim();
    const code = formatSxxEyy(raw.season, raw.episode);
    if (!series || !code) return "";
    return `${series} - ${code}`;
  }

  function syncHeaderPills(visible, total) {
    const srcMap = { state: "Current state", manual: "Manual overrides", playlist: "Playlist endpoint" };
    const kindMap = { watchlist: "Watchlist", history: "History", ratings: "Ratings", progress: "Progress", playlist: "Playlist" };
    if (pillSource) pillSource.textContent = srcMap[state.source] || "Source";
    if (pillKind) pillKind.textContent = state.source === "playlist" ? "Playlist" : (kindMap[state.kind] || "Kind");
    const all = typeof total === "number" ? total : ((state.rows && state.rows.length) || 0);
    const vis = typeof visible === "number" ? visible : all;
    if (pillCount) pillCount.textContent = all ? (vis !== all ? `${vis}/${all}` : `${all}`) : "0";
    if (pillSync) pillSync.textContent = fmtSyncTime(state.lastSyncAt);
  }

  function fmtSyncTime(value) {
    if (!value) return "not yet";
    const diff = Math.max(0, Date.now() - Number(value));
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins} min ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.floor(hours / 24);
    return `${days} day${days === 1 ? "" : "s"} ago`;
  }

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
    if (!statusEl) return;
    statusEl.title = message || "";
    if (/rows visible/i.test(String(statusEl.textContent || ""))) statusEl.textContent = "";
  }

  if (filterInput) {
    filterInput.placeholder = "Filter by title / S01 / S01E02 / id...";
    if (state.filter) filterInput.value = state.filter;
  }

  const KIND_LABELS = { watchlist: "Watchlist", history: "History", ratings: "Ratings", progress: "Progress" };

  function syncKindUI() {
    if (!kindSel) return;
    let allowed = ["watchlist", "history", "ratings", "progress"];
    if (!allowed.includes(state.kind)) state.kind = allowed[0] || "watchlist";
    const current = Array.from(kindSel.options).map(o => o.value);
    if (current.join("|") !== allowed.join("|")) {
      kindSel.innerHTML = allowed
        .map(k => `<option value="${k}">${_escapeHtml(KIND_LABELS[k] || k)}</option>`)
        .join("");
    }
    kindSel.value = state.kind;
  }

  function currentPlaylistEndpoint() {
    return editorSources.currentPlaylistEndpoint(state);
  }

  function playlistEditable() {
    return editorSources.playlistEditable(state);
  }

  function syncActionButtons() {
    const r = state.playlistResource || {};
    const playlist = state.source === "playlist";
    if (reloadBtn) reloadBtn.disabled = state.loading || state.saving;
    if (addBtn) addBtn.disabled = state.loading || state.saving || (playlist && (!r.can_add || r.smart));
    if (saveBtn) saveBtn.disabled = state.saving || state.loading || (playlist && !playlistEditable());
  }

  function allowedTypesForKind(kind) {
    if (state.source === "playlist") {
      const ep = currentPlaylistEndpoint();
      const values = (state.playlistResource && state.playlistResource.media_types) || (ep && ep.media_types) || [];
      const allowed = values.map(x => String(x || "").toLowerCase()).filter(x => ["movie", "show", "anime", "season", "episode"].includes(x));
      return allowed.length ? allowed : ["movie", "show", "anime"];
    }
    return kind === "watchlist"
      ? ["movie", "show", "anime"]
      : ["movie", "show", "anime", "season", "episode"];
  }
  function isAnilistMode() {
    return isProviderPickerSource() && String(state.snapshot || "").trim().toUpperCase() === "ANILIST";
  }

  function normalizeColumnState() {
    state.columnOrder = normalizeColumnOrder(state.columnOrder);
    COLUMN_KEYS.forEach(column => {
      if (isRequiredColumn(column)) state.columnVisibility[column] = true;
      else if (typeof state.columnVisibility[column] !== "boolean") state.columnVisibility[column] = DEFAULT_COLUMN_VISIBILITY[column] !== false;
      state.columnWidths[column] = clampColumnWidth(column, state.columnWidths[column]);
    });
  }

  function columnWidth(column) {
    return clampColumnWidth(column, state.columnWidths[column]);
  }

  function orderedColumns() {
    normalizeColumnState();
    return state.columnOrder;
  }

  function columnSortKey(column) {
    return COLUMN_KEYS.includes(column) ? column : "";
  }

  function columnLabel(column) {
    if (column === "id") return isAnilistMode() ? "MAL" : "TMDB";
    return (COLUMN_META[column] && COLUMN_META[column].label) || column;
  }

  function actionColumnWidth() {
    return isPolicySource() ? 84 : 46;
  }

  function syncIdColumnHeaders() {
    const a = $("cw-col-id-a");
    if (!a) return;
    const label = a.querySelector(".cw-th-label");
    if (label) label.textContent = columnLabel("id");
    else a.textContent = columnLabel("id");
  }

  function ensureColumnHeader(headRow, column) {
    let th = headRow.querySelector(`th.cw-col-${column}`);
    if (!th) {
      th = document.createElement("th");
      th.className = `cw-col-${column}`;
      if (column === "id") th.id = "cw-col-id-a";
    }
    th.classList.add("cw-data-col", "cw-resizable");
    th.dataset.column = column;
    const sortKey = columnSortKey(column);
    if (sortKey) {
      th.classList.add("sortable");
      th.dataset.sort = sortKey;
    } else {
      th.classList.remove("sortable", "sort-asc", "sort-desc");
      delete th.dataset.sort;
    }
    let label = th.querySelector(".cw-th-label");
    if (!label) {
      th.textContent = "";
      const inner = document.createElement("span");
      inner.className = "cw-th-inner";
      label = document.createElement("span");
      label.className = "cw-th-label";
      inner.appendChild(label);
      th.appendChild(inner);
    }
    label.textContent = columnLabel(column);
    wireColumnResize(th, column);
    return th;
  }

  function applyColumnWidths() {
    const table = host.querySelector(".cw-table");
    if (!table) return;
    const scrollEl = table.parentElement;
    const selectHead = table.querySelector("thead th:first-child");
    const actionHead = table.querySelector(".cw-action-head");
    const visibleColumns = orderedColumns().filter(column => state.columnVisibility[column] !== false);
    const widths = Object.fromEntries(COLUMN_KEYS.map(column => [column, columnWidth(column)]));
    let total = 34 + actionColumnWidth();
    visibleColumns.forEach(column => { total += widths[column]; });
    const containerWidth = Math.floor((scrollEl && scrollEl.clientWidth) || 0);
    const availableWidth = Math.max(0, containerWidth - 1);
    const hasHorizontalOverflow = containerWidth > 0 && total > availableWidth + 1;
    const renderedTotal = hasHorizontalOverflow ? total : Math.max(total, availableWidth || total);
    const titleExtra = !hasHorizontalOverflow && renderedTotal > total && visibleColumns.includes("title") ? renderedTotal - total : 0;
    scrollEl?.classList.toggle("cw-table-overflow-x", hasHorizontalOverflow);
    syncColumnGroup(table, visibleColumns, widths, titleExtra);
    if (selectHead) {
      selectHead.style.width = "34px";
      selectHead.style.minWidth = "34px";
    }
    if (actionHead) {
      const width = `${actionColumnWidth()}px`;
      actionHead.style.width = width;
      actionHead.style.minWidth = width;
    }
    COLUMN_KEYS.forEach(column => {
      const th = table.querySelector(`thead th.cw-col-${column}`);
      const width = widths[column] + (column === "title" ? titleExtra : 0);
      if (th) {
        th.style.width = `${width}px`;
        th.style.minWidth = `${width}px`;
      }
    });
    table.style.width = hasHorizontalOverflow ? `${total}px` : "100%";
    table.style.minWidth = hasHorizontalOverflow ? `${total}px` : "0";
  }

  function syncColumnGroup(table, visibleColumns, widths, titleExtra) {
    let group = Array.from(table.children).find(el => el.tagName === "COLGROUP");
    if (!group) {
      group = document.createElement("colgroup");
      table.insertBefore(group, table.firstChild);
    }
    group.textContent = "";
    [
      ["select", 34],
      ["actions", actionColumnWidth()],
      ...visibleColumns.map(column => [column, widths[column] + (column === "title" ? titleExtra : 0)]),
    ].forEach(([column, width]) => {
      const col = document.createElement("col");
      col.dataset.column = column;
      col.style.width = `${width}px`;
      group.appendChild(col);
    });
  }

  function syncWideViewUI() {
    const active = !!state.wideView;
    host.classList.toggle("cw-editor-wide", active);
    wideBtn.classList.toggle("active", active);
    wideBtn.title = active ? "Exit wide view" : "Wide view";
    wideBtn.setAttribute("aria-label", wideBtn.title);
    const icon = wideBtn.querySelector(".material-symbols-rounded");
    if (icon) icon.textContent = active ? "fullscreen_exit" : "fullscreen";
    applyColumnWidths();
  }

  function applyRenderedRowColumnOrder() {
    const order = orderedColumns();
    host.querySelectorAll(".cw-table tbody tr").forEach(tr => {
      const selectCell = tr.children[0] || null;
      const actionCell = tr.querySelector(".cw-action-cell") || tr.children[1] || null;
      const cells = {};
      COLUMN_KEYS.forEach(column => {
        const td = tr.querySelector(`td.cw-col-${column}`);
        if (!td) return;
        td.dataset.column = column;
        cells[column] = td;
      });
      const frag = document.createDocumentFragment();
      if (selectCell) frag.appendChild(selectCell);
      if (actionCell && actionCell !== selectCell) frag.appendChild(actionCell);
      order.forEach(column => {
        if (cells[column]) frag.appendChild(cells[column]);
      });
      tr.appendChild(frag);
    });
  }

  function applyColumnLayout() {
    normalizeColumnState();
    const table = host.querySelector(".cw-table");
    const headRow = table && table.tHead && table.tHead.rows[0];
    if (!table || !headRow) return;
    const selectHead = headRow.children[0];
    const actionHead = headRow.querySelector(".cw-action-head") || headRow.children[1];
    const headers = {};
    COLUMN_KEYS.forEach(column => {
      headers[column] = ensureColumnHeader(headRow, column);
    });
    const frag = document.createDocumentFragment();
    if (selectHead) frag.appendChild(selectHead);
    if (actionHead && actionHead !== selectHead) frag.appendChild(actionHead);
    orderedColumns().forEach(column => frag.appendChild(headers[column]));
    headRow.appendChild(frag);
    sortHeaders = Array.from(headRow.querySelectorAll("th[data-sort]"));
    applyColumnWidths();
    applyRenderedRowColumnOrder();
    updateSortUI();
  }

  function wireColumnResize(th, column) {
    let handle = Array.from(th.children).find(child => child.classList && child.classList.contains("cw-col-resize"));
    if (!handle) {
      handle = document.createElement("span");
      handle.className = "cw-col-resize";
      handle.setAttribute("role", "separator");
      handle.setAttribute("aria-orientation", "vertical");
      th.appendChild(handle);
    }
    handle.dataset.column = column;
    handle.title = `Resize ${columnLabel(column)}`;
    handle.onpointerdown = ev => startColumnResize(ev, column, th);
    handle.ondblclick = ev => {
      ev.preventDefault();
      ev.stopPropagation();
      state.columnWidths[column] = DEFAULT_COLUMN_WIDTHS[column];
      applyColumnWidths();
      persistUIState();
    };
  }

  function startColumnResize(ev, column, th) {
    if (!COLUMN_KEYS.includes(column)) return;
    if (ev.button != null && ev.button !== 0) return;
    ev.preventDefault();
    ev.stopPropagation();
    const startX = ev.clientX;
    const startWidth = columnWidth(column);
    document.body.classList.add("cw-column-resizing");
    th.classList.add("cw-resizing");
    const onMove = moveEv => {
      state.columnWidths[column] = clampColumnWidth(column, startWidth + moveEv.clientX - startX);
      applyColumnWidths();
    };
    const finish = () => {
      document.removeEventListener("pointermove", onMove);
      document.removeEventListener("pointerup", finish);
      document.removeEventListener("pointercancel", finish);
      document.body.classList.remove("cw-column-resizing");
      th.classList.remove("cw-resizing");
      persistUIState();
    };
    document.addEventListener("pointermove", onMove);
    document.addEventListener("pointerup", finish);
    document.addEventListener("pointercancel", finish);
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

  function syncPageSizeUI() {
    const value = PAGE_SIZE_OPTIONS.includes(Number(state.pageSize)) ? Number(state.pageSize) : DEFAULT_PAGE_SIZE;
    state.pageSize = value;
    if (pageSizeSel) pageSizeSel.value = String(value);
  }

  function syncColumnVisibilityUI() {
    const table = host.querySelector(".cw-table");
    normalizeColumnState();
    if (COLUMN_KEYS.includes(state.sortKey) && state.columnVisibility[state.sortKey] === false) {
      state.sortKey = "title";
      state.sortDir = "asc";
    }
    const hiddenCount = COLUMN_KEYS.filter(k => !isRequiredColumn(k) && state.columnVisibility[k] === false).length;
    const customVisibility = COLUMN_KEYS.some(k => state.columnVisibility[k] !== DEFAULT_COLUMN_VISIBILITY[k]);
    const customOrder = orderedColumns().join("|") !== DEFAULT_COLUMN_ORDER.join("|");
    const customWidths = COLUMN_KEYS.some(k => columnWidth(k) !== DEFAULT_COLUMN_WIDTHS[k]);
    columnsBtn.classList.toggle("active", customVisibility || customOrder || customWidths);
    columnsBtn.title = hiddenCount > 0 ? `Columns (${hiddenCount} hidden)` : customOrder || customWidths ? "Columns (custom layout)" : "Columns";
    COLUMN_KEYS.forEach(k => {
      if (isRequiredColumn(k)) state.columnVisibility[k] = true;
      else if (typeof state.columnVisibility[k] !== "boolean") state.columnVisibility[k] = DEFAULT_COLUMN_VISIBILITY[k] !== false;
      const visible = state.columnVisibility[k] !== false;
      table?.classList.toggle(`cw-hide-col-${k}`, !visible);
      document.querySelectorAll(`.cw-columns-pop .cw-column-toggle[data-column="${k}"]`).forEach(btn => {
        btn.classList.toggle("active", visible);
        btn.setAttribute("aria-pressed", visible ? "true" : "false");
        btn.disabled = isRequiredColumn(k);
      });
    });
    applyColumnLayout();
  }

  function syncStateBulkUI() {
    if (!stateBulkRow || !bulkTypeSel || !bulkBlockTypeBtn || !bulkUnblockTypeBtn) return;
    const show = isPolicySource() && state.kind !== "watchlist";
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
    return editorImporters.setImportBusy(on, message, importContext());
  }

  
  function _escapeHtml(s) {
    return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function renderInstanceOptions(selectEl, instances, current) {
    if (!selectEl) return "default";
    const list = Array.isArray(instances) ? instances : [];
    const norm = list
      .map(x => ({
        id: String((x && x.id) ? x.id : ""),
        label: String((x && x.label) ? x.label : (x && x.id) ? x.id : ""),
      }))
      .filter(x => x.id);

    if (!norm.length) norm.push({ id: "default", label: "Default" });

    const ids = norm.map(x => x.id);
    let next = String(current || "");
    if (!next || !ids.includes(next)) next = ids.includes("default") ? "default" : ids[0];
    const opts = norm.map(x => `<option value="${_escapeHtml(x.id)}">${_escapeHtml(x.label || x.id)}</option>`).join("");
    selectEl.innerHTML = opts;
    selectEl.value = next;
    selectEl.disabled = !ids.length;
    syncProfileIconSelect(selectEl, true);
    return next;
  }

  async function loadInstanceOptions(provider, selectEl, current) {
    if (!selectEl) return "default";
    if (!provider) {
      return renderInstanceOptions(selectEl, [{ id: "default", label: "Default" }], current);
    }
    try {
      const data = await fetchJSON(`/api/provider-instances/${encodeURIComponent(provider)}`);
      return renderInstanceOptions(selectEl, Array.isArray(data) ? data : [], current);
    } catch (_) {
      return renderInstanceOptions(selectEl, [{ id: "default", label: "Default" }], current);
    }
  }


  function syncImportUI() {
    return editorImporters.syncImportUI(importContext());
  }

  async function loadImportProviders() {
    return editorImporters.loadImportProviders(importContext());
  }

  function _collectImportFeatures() {
    return editorImporters.collectImportFeatures(importContext());
  }

  async function runStateImport() {
    return editorImporters.runStateImport(importContext());
  }

  syncKindUI();
  syncTypeFilterUI();
  syncPageSizeUI();
  syncWideViewUI();
  syncColumnVisibilityUI();
  syncStateBulkUI();

  function persistUIState() {
    try {
      if (typeof localStorage === "undefined") return;
      const data = {
        source: state.source,
        kind: state.kind,
        snapshot: state.snapshot,
        instance: state.instance,
        filter: state.filter,
        pageSize: state.pageSize,
        wideView: state.wideView,
        typeFilter: state.typeFilter,
        columnLayoutVersion: COLUMN_LAYOUT_VERSION,
        columnVisibility: state.columnVisibility,
        columnOrder: state.columnOrder,
        columnWidths: state.columnWidths,
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
    const setIconOnly = (btn, icon, label) => {
      btn.classList.add("cw-icon-only");
      btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">${icon}</span>`;
      btn.title = label;
      btn.setAttribute("aria-label", label);
    };
    setIconOnly(bulkSendBtn, "send", "Send to...");
    if (isPolicySource()) {
      setIconOnly(bulkRemoveBtn, "block", "Block selected");
      setIconOnly(bulkRestoreBtn, "undo", "Unblock selected");
    } else {
      setIconOnly(bulkRemoveBtn, "delete", "Delete selected");
      setIconOnly(bulkRestoreBtn, "restore_from_trash", "Restore selected");
    }
    setIconOnly(bulkClearBtn, "close", "Clear selection");
  }

  function selectedRowsForSend() {
    const sel = state.selected || new Set();
    return (state.rows || []).filter(row => sel.has(row._rid) && !row.deleted);
  }

  function rowToSendItem(row) {
    const raw = JSON.parse(JSON.stringify(row?.raw || {}));
    const ids = raw.ids && typeof raw.ids === "object" ? raw.ids : {};
    if (row.imdb) ids.imdb = row.imdb;
    if (row.tmdb) ids.tmdb = row.tmdb;
    if (row.tvdb) ids.tvdb = row.tvdb;
    if (row.trakt) ids.trakt = row.trakt;
    if (row.simkl) ids.simkl = row.simkl;
    if (row.mal) ids.mal = row.mal;
    if (row.anilist) ids.anilist = row.anilist;
    raw.ids = ids;
    raw.key = String(row.key || "").trim();
    raw.type = row.type || raw.type || null;
    raw.title = row.title || raw.title || raw.series_title || null;
    const y = String(row.year || "").trim();
    if (y) {
      const n = parseInt(y, 10);
      raw.year = Number.isFinite(n) ? n : y;
    }
    return raw;
  }

  async function openEditorSendModal() {
    return editorSendModal.open({
      state,
      selectedRowsForSend,
      rowToSendItem,
      fetchJSON,
      escapeHtml: _escapeHtml,
      currentPlaylistEndpoint,
      isProviderPickerSource,
      isTrackerSource,
      setStatusSticky,
      clearSelection,
      loadState,
    });
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
        ? isManualSource()
          ? "Removed"
          : isPolicySource()
            ? "Blocked"
            : "Deleted"
        : isManualSource()
          ? "Restored"
          : isPolicySource()
            ? "Unblocked"
            : "Restored";
      setStatusSticky(`${verb} ${changed} item${changed === 1 ? "" : "s"}`, 3000);
    }
  }

  function bulkSetBlocksByType(type, flag) {
    if (!isPolicySource()) return;
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
    return editorSources.syncSourceUI(sourceContext());
  }

  function showStateHint(mode) {
    return editorSources.showStateHint(mode, sourceContext());
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
    return editorDateTime.formatHistoryLabel(iso);
  }

  function formatSxxEyy(season, episode) {
    return editorDateTime.formatSxxEyy(season, episode);
  }



  function formatMs(ms) {
    return editorDateTime.formatMs(ms);
  }

  const clampProgressPercent = value => editorDateTime.clampProgressPercent(value);

  function progressPercentValue(raw) {
    return editorDateTime.progressPercentValue(raw);
  }

  function formatProgressPercent(value) {
    return editorDateTime.formatProgressPercent(value);
  }

  function parseProgressPercent(v) {
    return editorDateTime.parseProgressPercent(v);
  }

  function parseTimeToMs(v) {
    return editorDateTime.parseTimeToMs(v);
  }

  function appendPopupTitle(pop, text, marginTop = "") {
    const title = document.createElement("div");
    title.className = "cw-pop-title";
    title.textContent = text;
    if (marginTop) title.style.marginTop = marginTop;
    pop.appendChild(title);
  }

  function appendPopupActions(pop, defs) {
    const actions = document.createElement("div");
    actions.className = "cw-pop-actions";
    defs.forEach(def => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = `cw-pop-btn${def.kind ? ` ${def.kind}` : ""}`;
      btn.textContent = def.label;
      btn.onclick = def.onClick;
      actions.appendChild(btn);
    });
    pop.appendChild(actions);
  }

  function toggleColumnVisibility(column) {
    if (!COLUMN_KEYS.includes(column)) return;
    if (isRequiredColumn(column)) return;
    state.columnVisibility[column] = state.columnVisibility[column] === false;
    const sortChanged = state.columnVisibility[column] === false && state.sortKey === column;
    if (sortChanged) {
      state.sortKey = "title";
      state.sortDir = "asc";
    }
    syncColumnVisibilityUI();
    persistUIState();
    if (sortChanged) renderRows();
    else updateSortUI();
  }

  function moveColumn(column, delta) {
    const order = orderedColumns().slice();
    const idx = order.indexOf(column);
    const next = idx + delta;
    if (idx < 0 || next < 0 || next >= order.length) return;
    [order[idx], order[next]] = [order[next], order[idx]];
    state.columnOrder = order;
    syncColumnVisibilityUI();
    persistUIState();
    renderColumnPopupContents(activePopup && activePopup.node);
  }

  function resetColumnLayout(pop) {
    state.columnOrder = DEFAULT_COLUMN_ORDER.slice();
    state.columnWidths = { ...DEFAULT_COLUMN_WIDTHS };
    COLUMN_KEYS.forEach(column => {
      state.columnVisibility[column] = DEFAULT_COLUMN_VISIBILITY[column] !== false;
    });
    syncColumnVisibilityUI();
    persistUIState();
    renderColumnPopupContents(pop);
  }

  function renderColumnPopupContents(pop) {
    if (!pop || !pop.classList || !pop.classList.contains("cw-columns-pop")) return;
    pop.textContent = "";
    appendPopupTitle(pop, "Columns");
    const list = document.createElement("div");
    list.className = "cw-column-popup-list";
    const order = orderedColumns();
    order.forEach((column, idx) => {
      const meta = COLUMN_META[column] || { icon: "view_column", label: column };
      const row = document.createElement("div");
      row.className = "cw-column-row";
      row.dataset.column = column;

      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "cw-type-chip cw-column-toggle";
      toggle.dataset.column = column;
      toggle.disabled = isRequiredColumn(column);
      toggle.title = isRequiredColumn(column) ? `${columnLabel(column)} is always shown` : `Show ${columnLabel(column)}`;
      toggle.innerHTML = `<span class="material-symbol cw-type-icon" aria-hidden="true">${meta.icon}</span><span>${columnLabel(column)}</span>`;
      toggle.addEventListener("click", () => toggleColumnVisibility(column));

      const left = document.createElement("button");
      left.type = "button";
      left.className = "cw-column-move";
      left.disabled = idx === 0;
      left.title = `Move ${columnLabel(column)} left`;
      left.setAttribute("aria-label", left.title);
      left.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">keyboard_arrow_left</span>`;
      left.addEventListener("click", ev => {
        ev.stopPropagation();
        moveColumn(column, -1);
      });

      const right = document.createElement("button");
      right.type = "button";
      right.className = "cw-column-move";
      right.disabled = idx === order.length - 1;
      right.title = `Move ${columnLabel(column)} right`;
      right.setAttribute("aria-label", right.title);
      right.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">keyboard_arrow_right</span>`;
      right.addEventListener("click", ev => {
        ev.stopPropagation();
        moveColumn(column, 1);
      });

      row.appendChild(toggle);
      row.appendChild(left);
      row.appendChild(right);
      list.appendChild(row);
    });
    pop.appendChild(list);
    appendPopupActions(pop, [
      { label: "Reset", onClick: () => resetColumnLayout(pop) },
      { label: "Close", kind: "primary", onClick: closePopup },
    ]);
    syncColumnVisibilityUI();
    positionPopup(pop, columnsBtn);
  }

  function openColumnPopup(anchor) {
    openPopup(anchor, pop => {
      pop.classList.add("cw-columns-pop");
      renderColumnPopupContents(pop);
    });
  }

  function isRowLocked(row) {
    return isPolicySource() && !!row && row._origin === "baseline";
  }

  function isExtraKindEditable() {
    return state.kind === "ratings" || state.kind === "history" || state.kind === "progress";
  }

  function manualCorrectionKindLabel(kind = state.kind) {
    const k = String(kind || "").toLowerCase();
    if (k === "history") return "history date";
    if (k === "ratings") return "rating";
    if (k === "progress") return "progress";
    return "extra field";
  }

  function extraCorrectionStatus(row, promoted) {
    if (!isPolicySource() || !row || !isExtraKindEditable()) return "";
    const label = manualCorrectionKindLabel();
    if (promoted) return `A local ${label} correction was created. Save changes to use it on the next sync.`;
    if (row._origin === "manual") return `Updated local ${label} correction. Save changes to use it on the next sync.`;
    return "";
  }

  function promoteBaselineEdit(row) {
    if (!isPolicySource() || !row || row._origin !== "baseline") return false;
    row._origin = "manual";
    row.deleted = false;
    return true;
  }

  function rowType(row) {
    return String((row && row.type) || "").toLowerCase();
  }

  function canReplaceRow(row) {
    return !!editorMetadataReplacer.canReplaceRow(row, { isPolicySource });
  }

  function usesCoordinateReplacer(row) {
    return !!editorMetadataReplacer.usesCoordinateReplacer(row);
  }

  function commitReplacement(row, corrected, key, sameMessage) {
    const clash = (state.rows || []).some(
      r => r !== row && !r.deleted && String(r.key || "").toLowerCase() === key.toLowerCase()
    );
    if (clash) return "A row for that item already exists.";

    if (row._origin === "baseline") {
      row.deleted = true;
      state.rows.unshift(buildManualRow(corrected, key, row.key));
      setStatusSticky("The original item was blocked and a corrected local item was added.", 6000);
    } else {
      applyManualRow(row, corrected, key);
      setStatusSticky(sameMessage, 5000);
    }
    state.page = 0;
    markChanged();
    renderRows();
    return "";
  }

  function metadataReplacerContext() {
    return {
      isPolicySource,
      openPopup,
      appendPopupTitle,
      appendPopupActions,
      fetchJSON,
      updateTypeDisplay,
      formatEpisodeVisualTitle,
      setStatusSticky,
      markChanged,
      renderRows,
      commitReplacement,
    };
  }

  function openItemReplacer(row, anchor) {
    return editorMetadataReplacer.openItemReplacer(row, anchor, metadataReplacerContext());
  }

  function applyManualRow(row, item, key) {
    return editorRows.applyManualRow(row, item, key);
  }

  function buildManualRow(item, key, replacedKey) {
    return editorRows.buildManualRow(item, key, replacedKey, rowBuilderOptions());
  }

  function renderLockedPopup(pop, close) {
    const status = document.createElement("div");
    status.className = "cw-search-status";
    status.textContent = "Baseline title and ID fields are read-only. Use Extra for a local value correction, or block the row to exclude it.";
    pop.appendChild(status);
    appendPopupActions(pop, [{ label: "Close", kind: "primary", onClick: close }]);
  }

  function fillDateTimeInputs(iso, dateInput, timeInput) {
    editorDateTime.fillDateTimeInputs(iso, dateInput, timeInput);
  }

  function dateTimeInputsToIso(dateValue, timeValue) {
    return editorDateTime.dateTimeInputsToIso(dateValue, timeValue);
  }

  function appendUtcHint(pop) {
    editorDateTime.appendUtcHint(pop);
  }

  function finishExtraChange(row, displayEl, close) {
    const promoted = promoteBaselineEdit(row);
    updateExtraDisplay(row, displayEl);
    markChanged();
    const status = extraCorrectionStatus(row, promoted);
    if (status) setStatusSticky(status, 6000);
    close();
    if (promoted) renderRows();
  }

  function finishPopupChange(close, rerender = false) {
    markChanged();
    close();
    if (rerender) renderRows();
  }

  function extraEditorContext() {
    return {
      state,
      openPopup,
      appendPopupTitle,
      appendPopupActions,
      finishExtraChange,
    };
  }

  function updateExtraDisplay(row, el) {
    return editorExtraEditors.updateExtraDisplay(row, el, extraEditorContext());
  }

  function rowEditorContext() {
    return {
      state,
      openPopup,
      appendPopupTitle,
      appendPopupActions,
      allowedTypesForKind,
      isRowLocked,
      renderLockedPopup,
      finishPopupChange,
      isPolicySource,
      markChanged,
      renderRows,
    };
  }

  function updateTypeDisplay(row, el) {
    return editorRowEditor.updateTypeDisplay(row, el);
  }

  function imdbFromKey(key) {
    return editorRows.imdbFromKey(key);
  }

  function buildRows(items) {
    return editorRows.buildRows(items, rowBuilderOptions());
  }

  function buildManualOverrideRows(items, blocks) {
    return editorRows.buildManualOverrideRows(items, blocks, rowBuilderOptions());
  }

  function normalizeSearchText(value) {
    return editorSearch.normalizeSearchText(value);
  }

  function parseSeasonEpisodeText(value) {
    return editorSearch.parseSeasonEpisodeText(value);
  }

  function rowSeasonEpisode(row) {
    return editorSearch.rowSeasonEpisode(row);
  }

  function rowSearchHaystack(row) {
    return editorSearch.rowSearchHaystack(row, { formatEpisodeVisualTitle, formatSxxEyy });
  }
  function tableControllerContext() {
    return {
      state,
      host,
      pageSize: state.pageSize || DEFAULT_PAGE_SIZE,
      sortHeaders,
      tbody,
      empty,
      pager,
      prevBtn,
      nextBtn,
      pageInfo,
      summaryVisible,
      summaryTotal,
      summaryMovies,
      summaryShows,
      summarySeasons,
      summaryEpisodes,
      editorTable,
      normalizeSearchText,
      parseSeasonEpisodeText,
      rowSeasonEpisode,
      rowSearchHaystack,
      closePopup,
      syncIdColumnHeaders,
      syncColumnVisibilityUI,
      applyColumnWidths,
      syncHeaderPills,
      isPolicySource,
      isTrackerSource,
      isAnilistMode,
      isRowLocked,
      isExtraKindEditable,
      canReplaceRow,
      usesCoordinateReplacer,
      rowType,
      markChanged,
      renderRows,
      syncBulkBar,
      syncSelectPageCheckbox,
      updateTypeDisplay,
      updateExtraDisplay,
      formatEpisodeVisualTitle,
      formatSxxEyy,
      openTypeEditor,
      openTitleSearchEditor,
      openItemReplacer,
      openRawFieldsModal,
      openRatingEditor,
      openHistoryEditor,
      openProgressEditor,
      setStatus,
      setRowsStatus,
    };
  }

  function applyFilter(rows) {
    return editorTableController.applyFilter(rows, tableControllerContext());
  }

  function openHistoryEditor(row, anchor, displayEl) {
    return editorExtraEditors.openHistoryEditor(row, anchor, displayEl, extraEditorContext());
  }


  function openProgressEditor(row, anchor, displayEl) {
    return editorExtraEditors.openProgressEditor(row, anchor, displayEl, extraEditorContext());
  }

  function openRatingEditor(row, anchor, displayEl) {
    return editorExtraEditors.openRatingEditor(row, anchor, displayEl, extraEditorContext());
  }

  function openTitleSearchEditor(row, anchor, refs) {
    return editorMetadataReplacer.openTitleSearchEditor(row, anchor, refs, metadataReplacerContext());
  }

  function openTypeEditor(row, anchor) {
    return editorRowEditor.openTypeEditor(row, anchor, rowEditorContext());
  }

  async function openRawFieldsModal(row) {
    if (!isPolicySource() || !row) return;
    const props = {
      source: state.source,
      kind: state.kind,
      key: row.key || "",
      title: formatEpisodeVisualTitle(row) || row.title || row.key || "",
      origin: row._origin || "",
      item: JSON.parse(JSON.stringify(row.raw || {})),
    };
    try {
      if (typeof window.openEditorRawModal === "function") {
        await window.openEditorRawModal(props);
        return;
      }
      const version = encodeURIComponent(String(window.__CW_VERSION__ || Date.now()));
      const mod = await import(`/assets/js/modals.js?v=${version}`);
      if (typeof mod.openModal === "function") await mod.openModal("editor-raw", props);
    } catch (err) {
      console.error("editor raw modal failed", err);
      setStatusSticky("Could not open record details.", 3500);
    }
  }

  function sortRows(rows) {
    return editorTableController.sortRows(rows, tableControllerContext());
  }

  function updateSortUI() {
    return editorTableController.updateSortUI(tableControllerContext());
  }

  function renderRows() {
    return editorTableController.renderRows(tableControllerContext());
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

  function playlistEndpointLabel(ep) {
    return editorSources.playlistEndpointLabel(ep);
  }

  function rebuildSnapshots() {
    return editorSources.rebuildSnapshots(sourceContext());
  }

const on = (el, ev, fn) => el && el.addEventListener(ev, fn);

async function fetchJSON(url, opts) {
  return editorFileUtils.fetchJSON(url, opts);
}

async function downloadFile(url, filename, toast) {
  return editorFileUtils.downloadFile(url, filename, toast, { setTag, setStatus });
}

const listParts = (data, defs) => editorFileUtils.listParts(data, defs);

function bindFileImport(btn, input, url, done) {
  return editorFileUtils.bindFileImport(btn, input, url, done, { on, setTag, setStatus });
}

  async function loadSnapshots() {
    return editorSources.loadSnapshots(sourceContext());
  }

  function loadControllerContext() {
    return {
      state,
      host,
      snapSel,
      instanceSel,
      isTrackerSource,
      isProviderPickerSource,
      isPolicySource,
      isManualSource,
      normalizeSource,
      fetchJSON,
      buildRows,
      buildManualOverrideRows,
      loadSnapshots,
      renderRows,
      showStateHint,
      setTag,
      setStatus,
      syncActionButtons,
      syncHeaderPills,
      syncProviderIconSelect,
      syncProfileIconSelect,
    };
  }

  async function settleStateView(maxAttempts = 3, delayMs = 300) {
    return editorLoadController.settleStateView(loadControllerContext(), maxAttempts, delayMs);
  }

  async function loadState() {
    return editorLoadController.loadState(loadControllerContext());
  }

  function findRowsMissingKey() {
    return editorPersistence.findRowsMissingKey(state);
  }

  async function saveState() {
    return editorPersistence.saveState(persistenceContext());
  }

  function addRow() {
    return editorRowEditor.addRow(rowEditorContext());
  }

  on(prevBtn, "click", () => {
    if (state.page <= 0) return;
    state.page -= 1;
    renderRows();
  });

  on(nextBtn, "click", () => {
    const pageCount = Math.max(1, Math.ceil(applyFilter(state.rows).length / (state.pageSize || DEFAULT_PAGE_SIZE)));
    if (state.page >= pageCount - 1) return;
    state.page += 1;
    renderRows();
  });

  on(host.querySelector(".cw-table thead"), "click", e => {
    if (e.target.closest(".cw-col-resize") || e.target.closest(".cw-checkbox")) return;
    const th = e.target.closest("th[data-sort]");
    if (!th || !host.contains(th)) return;
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

  on(pageSizeSel, "change", () => {
    const next = Number(pageSizeSel.value);
    state.pageSize = PAGE_SIZE_OPTIONS.includes(next) ? next : DEFAULT_PAGE_SIZE;
    state.page = 0;
    syncPageSizeUI();
    persistUIState();
    renderRows();
  });

  on(columnsBtn, "click", () => openColumnPopup(columnsBtn));
  on(wideBtn, "click", () => {
    state.wideView = !state.wideView;
    closePopup();
    syncWideViewUI();
    persistUIState();
  });

  on(window, "resize", () => {
    window.clearTimeout(columnLayoutResizeTimer);
    columnLayoutResizeTimer = window.setTimeout(applyColumnWidths, 80);
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

  if (sourceSel) {
    sourceSel.addEventListener("change", async () => {
      state.source = normalizeSource(sourceSel.value);
      state.snapshot = "";
      state.page = 0;
      if (state.source === "playlist") {
        state.kind = "watchlist";
        state.instance = "default";
      }
      persistUIState();
      syncSourceUI();
      clearSelection();
      if (state.source === "state") await loadImportProviders();
      else if (importRow) syncImportUI();
      await loadSnapshots();
      await loadState();
      await settleStateView();
    });
  }

  if (kindSel) {
    kindSel.addEventListener("change", async () => {
      if (state.source === "playlist") {
        state.kind = "watchlist";
        syncKindUI();
        return;
      }
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
      if (!isProviderPickerSource()) state.snapshot = "";
      state.page = 0;
      persistUIState();
      await loadSnapshots();
      renderRows();
      await loadState();
      await settleStateView();
    });
  }

  if (snapSel) {
    snapSel.addEventListener("change", async () => {
      state.snapshot = snapSel.value || "";
      if (isProviderPickerSource()) syncProviderIconSelect(snapSel, true);
      if (isProviderPickerSource()) {
        state.instance = await loadInstanceOptions(state.snapshot, instanceSel, state.instance);
        persistUIState();
      }
      state.page = 0;
      if (!isProviderPickerSource()) persistUIState();
      await loadState();
    });
  }

  
  if (instanceSel) {
    instanceSel.addEventListener("change", async () => {
      state.instance = instanceSel.value || "default";
      state.page = 0;
      persistUIState();
      await loadState();
    });
  }

if (importProviderSel) {
    importProviderSel.addEventListener("change", () => {
      state.importProvider = importProviderSel.value || "";
      state.importProviderInstance = "default";
      persistUIState();
      syncImportUI();
    });
  }

  if (importInstanceSel) {
    importInstanceSel.addEventListener("change", () => {
      state.importProviderInstance = importInstanceSel.value || "default";
      persistUIState();
    });
  }

  if (importModeSel) {
    importModeSel.addEventListener("change", () => {
      state.importMode = importModeSel.value || "replace";
    });
  }

  [[importWatchlistCb, "watchlist"], [importHistoryCb, "history"], [importRatingsCb, "ratings"], [importProgressCb, "progress"]]
    .forEach(([el, key]) => on(el, "change", () => { state.importFeatures[key] = !!el.checked; }));

  on(importRunBtn, "click", runStateImport);


  if (filterInput) {
    filterInput.addEventListener("input", () => {
      state.filter = filterInput.value || "";
      state.page = 0;
      persistUIState();
      renderRows();
    });
  }

  function editorIsVisible() {
    return editorLoadController.editorIsVisible(loadControllerContext());
  }

  function syncSelectedScopeFromControls() {
    return editorLoadController.syncSelectedScopeFromControls(loadControllerContext());
  }

  async function refreshEditor({ force = false } = {}) {
    return editorLoadController.refreshEditor(loadControllerContext(), { force });
  }

  function queueEditorRefresh(delay = 250) {
    return editorLoadController.queueEditorRefresh(loadControllerContext(), delay);
  }

  if (reloadBtn) {
    reloadBtn.addEventListener("click", async () => {
      reloadBtn.disabled = true;
      reloadBtn.classList.add("is-refreshing");
      reloadBtn.setAttribute("aria-busy", "true");
      try {
        await refreshEditor({ force: true });
      } catch (e) {
        console.warn("[editor] refresh failed", e);
      } finally {
        reloadBtn.classList.remove("is-refreshing");
        reloadBtn.removeAttribute("aria-busy");
        syncActionButtons();
      }
    });
  }

  window.addEventListener("sync-complete", () => queueEditorRefresh(350));

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

  on(bulkRemoveBtn, "click", () => bulkSetDeletedForSelected(true));
  on(bulkRestoreBtn, "click", () => bulkSetDeletedForSelected(false));
  on(bulkClearBtn, "click", () => { clearSelection(); renderRows(); });
  on(bulkSendBtn, "click", () => openEditorSendModal());
  on(bulkBlockTypeBtn, "click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, true));
  on(bulkUnblockTypeBtn, "click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, false));

  on(addBtn, "click", addRow);
  on(saveBtn, "click", saveState);

  window.addEventListener("beforeunload", e => {
    if (!state.hasChanges) return;
    e.preventDefault();
    e.returnValue = "";
  });

  on(stateDownloadBtn, "click", () => downloadFile("/api/editor/state/manual/export", "crosswatch-state-policy.json", "Policy export downloaded"));

  bindFileImport(stateUploadBtn, stateUploadInput, "/api/editor/state/manual/import?mode=merge", async data => {
    const msg = "Imported " + (listParts(data, [["providers", "provider"], ["blocks", "block"], ["adds", "add"]]).join(", ") || "policy");
    if (window.cxToast) window.cxToast(msg);
    setTag("warn", "Imported");
    await loadSnapshots();
    await loadState();
  });

  (async () => {
    const wanted = state.source;
    state.source = normalizeSource(wanted);
    if (state.source !== wanted) {
      state.snapshot = "";
      state.instance = "default";
      persistUIState();
    }
    syncSourceUI();
    await loadImportProviders();
    setTag(
      "warn",
      state.source === "state"
        ? "Loading current state..."
        : "Loading playlist endpoint..."
    );
    await loadSnapshots();
    await loadState();
    await settleStateView();
    state.lastSyncAt = Date.now();
    syncHeaderPills();
    window.setInterval(() => syncHeaderPills(), 30000);
  })();
  }

  function bootWhenReady() {
    if (cwEditorBooted) return;
    if (window.cwIsAuthSetupPending?.() === true) {
      if (!cwEditorBootRetryWired) {
        cwEditorBootRetryWired = true;
        Promise.resolve(window.__cwAuthBootstrapPromise)
          .catch(() => null)
          .finally(() => {
            cwEditorBootRetryWired = false;
            if (window.cwIsAuthSetupPending?.() === true) return;
            bootWhenReady();
          });
      }
      return;
    }
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
