/* assets/js/editor.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const PAGE_SIZE_OPTIONS = [50, 100, 150, 200];
  const DEFAULT_PAGE_SIZE = 50;
  const MOBILE_PAGE_SIZE_OPTIONS = [10, 20, 50];
  const mobilePaging = window.matchMedia("(max-width:760px)");
  const COLUMN_KEYS = ["key", "type", "title", "year", "id", "imdb", "tvdb", "trakt", "simkl", "anilist", "extra", "status"];
  const COLUMN_LAYOUT_VERSION = 4;
  const DEFAULT_COLUMN_ORDER = COLUMN_KEYS.slice();
  const DEFAULT_COLUMN_VISIBILITY = { key: true, type: true, title: true, year: false, id: true, imdb: false, tvdb: false, trakt: false, simkl: false, anilist: false, extra: true, status: false };
  const DEFAULT_COLUMN_WIDTHS = { key: 132, type: 126, title: 360, year: 92, id: 132, imdb: 150, tvdb: 120, trakt: 120, simkl: 120, anilist: 120, extra: 190, status: 120 };
  const MIN_COLUMN_WIDTHS = { key: 96, type: 116, title: 220, year: 84, id: 120, imdb: 130, tvdb: 110, trakt: 110, simkl: 110, anilist: 110, extra: 180 };
  const MAX_COLUMN_WIDTHS = { key: 340, type: 240, title: 760, year: 180, id: 280, imdb: 280, tvdb: 240, trakt: 240, simkl: 240, anilist: 240, extra: 560 };
  const COLUMN_META = {
    key: { icon: "key", label: T("editor.col.key") },
    type: { icon: "category", label: T("editor.col.type") },
    title: { icon: "title", label: T("editor.col.title"), required: true },
    year: { icon: "event", label: T("editor.col.year") },
    id: { icon: "fingerprint", label: T("editor.col.ids") },
    imdb: { icon: "tag", label: "IMDb" },
    tvdb: { icon: "dns", label: "TVDB" },
    trakt: { icon: "confirmation_number", label: "Trakt" },
    simkl: { icon: "hub", label: "SIMKL" },
    anilist: { icon: "animation", label: "AniList" },
    extra: { icon: "tune", label: T("editor.col.extra") },
    status: { icon: "flag", label: T("editor.col.status") },
  };
  const SIMPLE_COLUMNS = ["type", "title", "year", "extra"];
  const ALL_PROVIDERS = "__all__";
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
    expandedViewings: new Set(),
    pageRids: [],
    ridSeq: 1,
    filter: "",
    loading: true,
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
    importFeatures: { watchlist: true, history: true, ratings: true, progress: true, collection: true },
    hasChanges: false,
    page: 0,
    desktopPageSize: DEFAULT_PAGE_SIZE,
    mobilePageSize: 20,
    get pageSize() { return mobilePaging.matches ? this.mobilePageSize : this.desktopPageSize; },
    set pageSize(value) { if (mobilePaging.matches) this.mobilePageSize = value; else this.desktopPageSize = value; },
    blockedOnly: false,
    typeFilter: { movie: true, show: true, anime: true, season: true, episode: true },
    columnVisibility: { ...DEFAULT_COLUMN_VISIBILITY },
    columnOrder: DEFAULT_COLUMN_ORDER.slice(),
    columnWidths: { ...DEFAULT_COLUMN_WIDTHS },
    wideView: false,
    advanced: false,
    sortKey: "title",
    sortDir: "asc",
  };
  const editorModules = window.CW && window.CW.Editor;
  window.cwEditorAssistantContext = () => {
    const picked = state.rows.filter(row => state.selected.has(row._rid));
    return { page: "editor", pair_id: state.mappingPair || "", feature: state.kind,
      provider: state.snapshot, instance: state.instance,
      item_key: picked.length === 1 ? picked[0].key : "" };
  };
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

      const sources = ["state", "playlist"];
      if (saved.source && sources.includes(saved.source)) state.source = saved.source;

      if (typeof saved.blockedOnly === "boolean") state.blockedOnly = saved.blockedOnly;

      const kinds = ["watchlist", "history", "ratings", "progress", "collection"];
      if (saved.kind && kinds.includes(saved.kind)) state.kind = saved.kind;

      if (typeof saved.snapshot === "string") state.snapshot = saved.snapshot;
      if (typeof saved.instance === "string" && saved.instance.trim()) state.instance = saved.instance;

      if (typeof saved.filter === "string") state.filter = saved.filter;
      if (PAGE_SIZE_OPTIONS.includes(Number(saved.pageSize))) state.desktopPageSize = Number(saved.pageSize);
      if (MOBILE_PAGE_SIZE_OPTIONS.includes(Number(saved.mobilePageSize))) state.mobilePageSize = Number(saved.mobilePageSize);
      if (typeof saved.wideView === "boolean") state.wideView = saved.wideView;
      if (typeof saved.advanced === "boolean") state.advanced = saved.advanced;

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
          if (saved.columnWidths[k] != null) state.columnWidths[k] = clampColumnWidth(k, k === "extra" && Number(saved.columnWidths[k]) === 220 ? DEFAULT_COLUMN_WIDTHS.extra : saved.columnWidths[k]);
        });
      }

      const sortKeys = COLUMN_KEYS;
      if (saved.sortKey && sortKeys.includes(saved.sortKey)) state.sortKey = saved.sortKey;
      if (saved.sortDir === "asc" || saved.sortDir === "desc") state.sortDir = saved.sortDir;
    } catch (_) {}
  }
  restoreUIState();

  host.innerHTML = `<div class="cw-root"><div class="cw-topline cw-page-hero cw-page-hero-editor" data-hero-icon="edit_note"><div class="cw-head-copy cw-page-hero-copy"><div class="cw-page-hero-kicker">${TH("editor.kicker")}</div><div class="cw-title-row"><div><div class="cw-title cw-page-hero-title">${TH("editor.title")}</div><div class="cw-sub cw-page-hero-sub">${TH("editor.subtitle")}</div></div></div></div><div class="cw-editor-hero-summary cw-page-hero-actions" id="cw-hero-summary" aria-label="${TH("editor.summaryLabel")}"><div class="cw-editor-hero-seg"><strong id="cw-pill-source">${TH("editor.hero.current")}</strong><span>${TH("editor.hero.source")}</span></div><div class="cw-editor-hero-seg"><strong id="cw-pill-kind">${TH("editor.kind.watchlist")}</strong><span>${TH("editor.hero.view")}</span></div><div class="cw-editor-hero-seg cw-editor-hero-count"><strong id="cw-pill-count">0</strong><span>${TH("editor.hero.rows")}</span></div><div class="cw-editor-hero-seg cw-editor-hero-sync"><span>${TH("editor.synced")}</span><strong id="cw-pill-sync">${TH("playlists.neverLower")}</strong></div><button id="cw-reload" class="cw-editor-refresh" type="button" title="${TH("editor.refresh")}" aria-label="${TH("editor.refresh")}"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button></div></div><div class="cw-wrap"><div class="cw-main"><div class="cw-controls cw-page-toolbar"><input id="cw-filter" class="cw-input" placeholder="${TH("editor.filterPlaceholder")}"><span class="cw-status-text" id="cw-status"></span><label class="cw-page-size-control" for="cw-page-size"><span>${TH("editor.rows")}</span><select id="cw-page-size" class="cw-select"><option value="50">50</option><option value="100">100</option><option value="150">150</option><option value="200">200</option></select></label><div class="cw-controls-spacer"></div><div class="cw-selection-bar" id="cw-bulk" role="group" aria-label="${TH("editor.selectedRows")}" hidden><span class="cw-selection-count" id="cw-bulk-count" aria-live="polite"></span><button id="cw-bulk-remove" class="cw-selection-button is-danger" type="button"></button><button id="cw-bulk-restore" class="cw-selection-button is-good" type="button"></button><button id="cw-bulk-clear" class="cw-selection-button" type="button">${TH("editor.clear")}</button></div><button id="cw-add" class="cw-btn" type="button">${TH("editor.addRow")}</button><button id="cw-save" class="cw-btn primary" type="button">${TH("editor.saveChanges")}</button></div><div class="cw-table-wrap cw-page-table" id="cw-table-wrap"><div class="cw-table-scroll"><table class="cw-table"><thead><tr><th style="width:34px"><input id="cw-select-page" class="cw-checkbox" type="checkbox" title="${TH("editor.selectPage")}"></th><th class="cw-action-head" style="width:46px"></th><th class="cw-col-key sortable" style="width:12%" data-sort="key">${TH("editor.col.key")}</th><th class="cw-col-type sortable" style="width:13%" data-sort="type">${TH("editor.col.type")}</th><th class="cw-col-title sortable" style="width:33%" data-sort="title">${TH("editor.col.title")}</th><th class="cw-col-year" style="width:84px">${TH("editor.col.year")}</th><th class="cw-col-id cw-col-id-a" style="width:12%" id="cw-col-id-a">TMDB</th><th class="cw-col-extra sortable" style="width:260px" data-sort="extra">${TH("editor.col.extra")}</th></tr></thead><tbody id="cw-tbody"></tbody></table></div></div><div class="cw-pager" id="cw-pager" style="display:none"><button id="cw-prev" class="cw-btn" type="button">${TH("editor.previous")}</button><span id="cw-page-info" class="cw-page-info"></span><button id="cw-next" class="cw-btn" type="button">${TH("editor.next")}</button></div><div class="cw-empty" id="cw-empty" role="status" style="display:none"><span class="material-symbols-rounded cw-empty-icon" aria-hidden="true">table_rows</span><div class="cw-empty-text">${TH("editor.emptyHint")}</div></div></div><aside class="cw-side"><div class="ins-card cw-page-panel"><div class="ins-row"><div class="ins-kv" style="width:100%"><label>${TH("editor.source")}</label><select id="cw-source" class="cw-select"><option value="state">${TH("editor.currentState")}</option></select><label>${TH("editor.list")}</label><select id="cw-kind" class="cw-select"><option value="watchlist">${TH("editor.kind.watchlist")}</option><option value="history">${TH("editor.kind.history")}</option><option value="ratings">${TH("editor.kind.ratings")}</option><option value="progress">${TH("editor.kind.progress")}</option></select><label id="cw-pair-label" style="display:none">${TH("editor.pair")}</label><select id="cw-pair" class="cw-select" style="display:none"></select><label id="cw-snapshot-label">${TH("editor.snapshot")}</label><select id="cw-snapshot" class="cw-select"><option value="">${TH("editor.latest")}</option></select><label id="cw-instance-label" style="display:none">${TH("editor.instance")}</label><select id="cw-instance" class="cw-select" style="display:none"><option value="default">${TH("editor.default")}</option></select><div id="cw-instance-shared" class="cw-shared-note" role="note" style="display:none"></div></div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><div class="field-label">${TH("editor.types")}</div><div id="cw-type-filter" class="cw-type-filter"><button type="button" data-type="movie" class="cw-type-chip active">${TH("editor.type.movies")}</button><button type="button" data-type="show" class="cw-type-chip active">${TH("editor.type.shows")}</button><button type="button" data-type="anime" class="cw-type-chip active">${TH("editor.type.anime")}</button><button type="button" data-type="season" class="cw-type-chip active">${TH("editor.type.seasons")}</button><button type="button" data-type="episode" class="cw-type-chip active">${TH("editor.type.episodes")}</button><button type="button" id="cw-blocked-only" class="cw-type-chip">${TH("editor.blocked")}</button></div></div></div><div class="ins-row" id="cw-state-bulk" style="display:none"><details class="cw-collapse" id="cw-bulk-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">${TH("editor.blockRules")}</summary><div style="display:flex;flex-direction:column;gap:8px;width:100%;margin-top:10px"><select id="cw-bulk-type" class="cw-select" style="width:100%"></select><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button id="cw-bulk-block-type" class="cw-btn danger" type="button" style="flex:1 1 0;min-width:120px">${TH("editor.blockAll")}</button><button id="cw-bulk-unblock-type" class="cw-btn" type="button" style="flex:1 1 0;min-width:120px">${TH("editor.unblockAll")}</button></div><div class="cw-status-text">${TH("editor.stateOnlyHint")}</div></div></details></div><div class="ins-row" id="cw-import-row" style="display:none"><details class="cw-collapse" id="cw-import-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">${TH("editor.debug")}</summary><div style="display:flex;flex-direction:column;gap:10px;width:100%;margin-top:10px"><div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center"><select id="cw-import-provider" class="cw-select" style="flex:1;min-width:200px"></select><select id="cw-import-instance" class="cw-select" style="min-width:180px"></select><select id="cw-import-mode" class="cw-select" style="min-width:180px"><option value="replace">${TH("editor.replaceBaseline")}</option><option value="merge">${TH("editor.mergeKeep")}</option></select></div><div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center"><label id="cw-import-watchlist-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-watchlist" class="cw-checkbox" type="checkbox" checked>${TH("editor.kind.watchlist")} </label><label id="cw-import-history-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-history" class="cw-checkbox" type="checkbox" checked>${TH("editor.kind.history")} </label><label id="cw-import-ratings-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-ratings" class="cw-checkbox" type="checkbox" checked>${TH("editor.kind.ratings")} </label><label id="cw-import-progress-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-progress-cb" class="cw-checkbox" type="checkbox" checked>${TH("editor.kind.progress")} </label><span style="flex:1 1 auto"></span><button id="cw-import-run" class="cw-btn sm" type="button">${TH("editor.refreshBaseline")}</button></div><div id="cw-import-progress" style="display:none"><div class="cw-progress"><span></span></div><div class="cw-status-text" id="cw-import-progress-text" style="margin-top:6px"></div></div></div></details></div></div><div class="ins-card cw-page-panel"><div class="ins-row" style="align-items:center"><div class="ins-icon"><span class="material-symbol">insights</span></div><div class="ins-title" style="margin-right:auto">${TH("editor.pulse")}</div><span class="cw-tag" id="cw-tag-status"><span class="cw-tag-dot"></span><span id="cw-tag-label">${TH("editor.idle")}</span></span></div><div class="ins-row"><div class="ins-metrics"><div class="metric-row"><div class="metric"><span class="material-symbol">view_list</span><div><div class="m-val" id="cw-summary-total">0</div><div class="m-lbl">${TH("editor.totalRows")}</div></div></div><div class="metric"><span class="material-symbol">visibility</span><div><div class="m-val" id="cw-summary-visible">0</div><div class="m-lbl">${TH("editor.rowsVisible")}</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">movie</span><div><div class="m-val" id="cw-summary-movies">0</div><div class="m-lbl">${TH("editor.type.movies")}</div></div></div><div class="metric"><span class="material-symbol">monitoring</span><div><div class="m-val" id="cw-summary-shows">0</div><div class="m-lbl">${TH("editor.type.shows")}</div></div></div><div class="metric"><span class="material-symbol">layers</span><div><div class="m-val" id="cw-summary-seasons">0</div><div class="m-lbl">${TH("editor.type.seasons")}</div></div></div><div class="metric"><span class="material-symbol">live_tv</span><div><div class="m-val" id="cw-summary-episodes">0</div><div class="m-lbl">${TH("editor.type.episodes")}</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">description</span><div><div class="m-val" id="cw-summary-state-files">0</div><div class="m-lbl">${TH("editor.stateFiles")}</div></div></div><div class="metric"><span class="material-symbol">folder_copy</span><div><div class="m-val" id="cw-summary-snapshots">0</div><div class="m-lbl">${TH("editor.snapshots")}</div></div></div></div><div id="cw-state-hint" class="cw-state-hint" style="display:none"><strong>${TH("editor.noState")}</strong> ${TH("editor.noStateHint")}</div></div></div></div><div class="ins-card cw-page-panel" id="cw-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">${TH("editor.archive")}</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>${TH("editor.exportImport")}</label><div class="cw-backup-actions"><button id="cw-download" class="cw-btn" type="button">${TH("editor.downloadZip")}</button><button id="cw-upload" class="cw-btn" type="button">${TH("editor.importFile")}</button><input id="cw-upload-input" type="file" accept=".zip,.json" style="display:none"></div></div></div></div><div class="ins-card cw-page-panel" id="cw-state-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">${TH("editor.policyBackup")}</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>${TH("editor.exportImport")}</label><div class="cw-backup-actions"><button id="cw-state-download" class="cw-btn" type="button">${TH("editor.downloadJson")}</button><button id="cw-state-upload" class="cw-btn" type="button">${TH("editor.importFile")}</button><input id="cw-state-upload-input" type="file" accept=".json" style="display:none"></div></div></div></div></aside></div></div>`;

  editorChrome.wireStaticLabels(host);
  editorChrome.prepareMobileFilters(host);
  editorChrome.prepareSourceOptions(host);
  editorChrome.addTrackerNotice(host);
  editorChrome.ensureFieldNames(host);
  const initialKindSel = host.querySelector("#cw-kind");
  if (initialKindSel && !initialKindSel.querySelector('option[value="collection"]')) {
    initialKindSel.appendChild(new Option(T("editor.collections"), "collection"));
  }
  const initialImportProgressWrap = host.querySelector("#cw-import-progress-wrap");
  if (initialImportProgressWrap && !host.querySelector("#cw-import-collection-wrap")) {
    const label = document.createElement("label");
    label.id = "cw-import-collection-wrap";
    label.style.cssText = initialImportProgressWrap.getAttribute("style") || "";
    const input = document.createElement("input");
    input.id = "cw-import-collection";
    input.className = "cw-checkbox";
    input.type = "checkbox";
    input.checked = true;
    label.append(input, document.createTextNode(`${T("editor.collections")} `));
    initialImportProgressWrap.insertAdjacentElement("afterend", label);
  }

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
    importRatingsCb, importProgressCb, importCollectionCb, importModeSel, importRunBtn, importWatchlistWrap,
    importHistoryWrap, importRatingsWrap, importProgressFeatWrap, importCollectionWrap, importProgressWrap,
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
    importCollectionCb: "cw-import-collection",
    importModeSel: "cw-import-mode",
    importRunBtn: "cw-import-run",
    importWatchlistWrap: "cw-import-watchlist-wrap",
    importHistoryWrap: "cw-import-history-wrap",
    importRatingsWrap: "cw-import-ratings-wrap",
    importProgressFeatWrap: "cw-import-progress-wrap",
    importCollectionWrap: "cw-import-collection-wrap",
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
  columnsBtn.title = T("editor.columns");
  columnsBtn.setAttribute("aria-label", T("editor.columns"));
  columnsBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">view_column</span>`;
  pageSizeSel?.closest(".cw-page-size-control")?.insertAdjacentElement("afterend", columnsBtn);

  const wideBtn = document.createElement("button");
  wideBtn.id = "cw-wide-btn";
  wideBtn.className = "cw-btn cw-wide-btn";
  wideBtn.type = "button";
  wideBtn.title = T("editor.wide");
  wideBtn.setAttribute("aria-label", T("editor.wide"));
  wideBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">fullscreen</span>`;
  columnsBtn.insertAdjacentElement("afterend", wideBtn);

  const bulkSendBtn = document.createElement("button");
  bulkSendBtn.id = "cw-bulk-send";
  bulkSendBtn.className = "cw-selection-button";
  bulkSendBtn.type = "button";

  const bulkVisibleBtn = document.createElement("button");
  bulkVisibleBtn.className = "cw-selection-button";
  bulkVisibleBtn.type = "button";
  bulkVisibleBtn.id = "cw-bulk-visible";
  bulkVisibleBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">visibility</span><span>${TH("editor.visible")}</span>`;
  const bulkAllBtn = document.createElement("button");
  bulkAllBtn.className = "cw-selection-button";
  bulkAllBtn.type = "button";
  bulkAllBtn.id = "cw-bulk-all";
  bulkAllBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">checklist</span><span>${TH("editor.allResults")}</span>`;

  const advancedBtn = document.createElement("button");
  advancedBtn.id = "cw-advanced-toggle";
  advancedBtn.className = "cw-btn cw-advanced-toggle";
  advancedBtn.type = "button";
  advancedBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">tune</span><span>${TH("editor.advanced")}</span>`;
  wideBtn.insertAdjacentElement("afterend", advancedBtn);

  const quickAddBtn = document.createElement("button");
  quickAddBtn.id = "cw-editor-quick-add";
  quickAddBtn.className = "cw-btn primary";
  quickAddBtn.type = "button";
  quickAddBtn.setAttribute('aria-label', T("editor.quickAdd"));
  quickAddBtn.title = T("editor.quickAddHint");
  quickAddBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">add_circle</span><span>${TH("editor.quickAdd")}</span>`;
  (bulkWrap || addBtn)?.insertAdjacentElement(bulkWrap ? "afterend" : "beforebegin", quickAddBtn);

  const changeBar = document.createElement("div");
  changeBar.id = "cw-change-bar";
  changeBar.className = "cw-change-bar";
  changeBar.setAttribute("role", "status");
  changeBar.hidden = true;
  changeBar.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">edit_note</span><div class="cw-change-copy"><strong>${TH("editor.unsaved")}</strong><span id="cw-change-note"></span></div><button id="cw-discard" class="cw-btn" type="button">${TH("editor.discard")}</button>`;
  const discardBtn = changeBar.querySelector("#cw-discard");
  const changeNote = changeBar.querySelector("#cw-change-note");
  if (saveBtn) changeBar.appendChild(saveBtn);
  host.querySelector(".cw-main")?.appendChild(changeBar);
  if (stateHint) host.querySelector(".cw-controls")?.insertAdjacentElement("afterend", stateHint);

  let pendingChanges = false;
  Object.defineProperty(state, "hasChanges", {
    enumerable: true,
    get: () => pendingChanges,
    set: value => {
      pendingChanges = !!value;
      syncChangeBar();
    },
  });

  if (backupCard) backupCard.remove();
  [summaryStateFiles, summarySnapshots].forEach(el => el?.closest(".metric")?.remove());
  [columnsBtn, addBtn, stateBulkRow, importRow, stateBackupCard, blockedOnlyBtn]
    .forEach(el => el?.classList.add("cw-advanced-only"));

  editorChrome.decorateImportPanel({
    importProviderSel,
    importInstanceSel,
    importModeSel,
    importWatchlistWrap,
    importHistoryWrap,
    importRatingsWrap,
    importProgressFeatWrap,
    importCollectionWrap,
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
    setStatus,
    mappingEditor: {
      state, loadSnapshots, loadState, syncSourceUI, syncTypeFilterUI, persistUIState, renderRows, sortRows, applyFilter,
      clearFilter() { state.filter = ""; if (filterInput) filterInput.value = ""; },
      restoreUI() {
        syncSourceUI(); rebuildSnapshots();
        if (instanceSel) instanceSel.value = state.instance;
        if (filterInput) filterInput.value = state.filter;
        syncProfileIconSelect(instanceSel, true); syncTypeFilterUI(); renderRows();
      },
      focusMapping(row) {
        const input = tbody?.querySelector(`[name="cw-row-${row._rid}-title"]`);
        input?.closest("tr")?.scrollIntoView({ block: "center" });
        input?.focus();
        return openItemReplacer(row, input);
      },
    },
  });
  stateBackupCard?.classList.add("cw-advanced-only");
  editorChrome.prepareMobileTools(host);
  const typeAllBtn = document.createElement("button");
  typeAllBtn.id = "cw-type-all";
  typeAllBtn.type = "button";
  typeAllBtn.textContent = T("editor.all");
  if (typeFilterWrap) {
    const typeBar = document.createElement("div");
    typeBar.className = "cw-type-bar";
    typeBar.setAttribute("aria-label", T("editor.filterType"));
    const typeRow = typeFilterWrap.closest(".ins-row");
    host.querySelector(".cw-controls")?.appendChild(typeBar);
    typeFilterWrap.className = "cw-type-filter";
    typeFilterWrap.prepend(typeAllBtn);
    typeFilterWrap.querySelectorAll("button").forEach(btn => { btn.className = `cw-filter-chip${btn.classList.contains("cw-advanced-only") ? " cw-advanced-only" : ""}`; });
    typeBar.appendChild(typeFilterWrap);
    typeRow?.remove();
  }
  if (bulkWrap) {
    const actions = document.createElement("span");
    actions.className = "cw-selection-actions";
    actions.append(bulkSendBtn, bulkRemoveBtn, bulkRestoreBtn);
    bulkWrap.replaceChildren(bulkCount, bulkVisibleBtn, bulkAllBtn, bulkClearBtn, actions);
    host.querySelector(".cw-root")?.appendChild(bulkWrap);
    new ResizeObserver(() => host.style.setProperty("--cw-selection-height", `${bulkWrap.offsetHeight}px`)).observe(bulkWrap);
  }
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
      sortFirstValues: [ALL_PROVIDERS],
      className: "cw-editor-icon-select",
      getOptionData: (value, option) => {
        if (value === ALL_PROVIDERS) return { label: T("editor.allProviders"), icons: [], disabled: false };
        const key = providerKey(value);
        const label = providerLabel(value, option?.textContent || value || T("editor.select"));
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
    selectEl.classList.add("cw-icon-select-native");
    helper(selectEl, {
      className: "cw-editor-icon-select",
    });
    const nextWrap = selectEl.nextElementSibling && selectEl.nextElementSibling.classList?.contains("cw-icon-select")
      ? selectEl.nextElementSibling
      : null;
    if (nextWrap) nextWrap.style.display = "";
  }

  function syncPlaylistEndpointIconSelect(selectEl, show) {
    if (!selectEl) return;
    const helper = window.CW?.IconSelect?.enhance;
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
      menuClassName: "cw-editor-icon-select-menu",
      sortAlphabetically: true,
      getOptionData: (value, option) => {
        const ep = (state.playlistEndpoints || []).find(x => String(x?.id || "") === String(value || "")) || {};
        const provider = ep.provider || option?.dataset?.provider || "";
        const providerText = providerLabel(provider, provider || "");
        const playlist = ep.playlist_name || ep.playlist_id || "";
        const label = ep.name || ep.id || option?.textContent || value || T("editor.endpoint");
        const note = [providerText, playlist && playlist !== label ? playlist : ""].filter(Boolean).join(" - ");
        const icon = providerMeta.logLogoPath?.(provider) || providerMeta.logoPath?.(provider) || "";
        return {
          label,
          note,
          selectedLabel: label,
          selectedShowNote: false,
          icons: [icon ? { src: icon, alt: providerText } : { text: String(provider || "?").slice(0, 2) }],
          disabled: !!option?.disabled,
        };
      },
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
      allProviders: ALL_PROVIDERS,
      providerLabel,
      fetchJSON,
      syncProviderIconSelect,
      syncProfileIconSelect,
      syncPlaylistEndpointIconSelect,
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
      importCollectionCb,
      importModeSel,
      importRunBtn,
      importWatchlistWrap,
      importHistoryWrap,
      importRatingsWrap,
      importProgressFeatWrap,
      importCollectionWrap,
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
      confirm: async message => (await (window.CW?.MobileUI?.ask || window.confirm)(message)),
    };
  }

  let statusStickyUntil = 0;

  function isProviderPickerSource() {
    return editorSources.isProviderPickerSource(state);
  }

  function isPolicySource() {
    return editorSources.isPolicySource(state);
  }

  function normalizeSource(value) {
    return editorSources.normalizeSource(value);
  }

  function isMergedView() {
    return state.source === "state" && state.snapshot === ALL_PROVIDERS;
  }

  function syncMergedScope(data) {
    state.mergedTargets = Array.isArray(data.targets) ? data.targets : [];
    const profiles = Array.isArray(data.profiles) ? data.profiles : [];
    const show = profiles.length > 0;
    if (instanceLabel) instanceLabel.style.display = show ? "" : "none";
    if (instanceSel) instanceSel.style.display = show ? "" : "none";
    if (show) {
      state.instance = renderInstanceOptions(instanceSel, [{ id: "default", label: T("editor.defaultAccounts") }, ...profiles], data.profile || "default");
    } else {
      syncProfileIconSelect(instanceSel, false);
    }
  }

  function mergedProfile() {
    return isMergedView() && state.instance && state.instance !== "default" ? state.instance : "";
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
    const srcMap = { state: T("editor.hero.current"), playlist: T("editor.playlist") };
    const kindMap = { watchlist: T("editor.kind.watchlist"), history: T("editor.kind.history"), ratings: T("editor.kind.ratings"), progress: T("editor.kind.progress"), collection: T("editor.collections"), playlist: T("editor.playlist") };
    if (pillSource) pillSource.textContent = srcMap[state.source] || T("editor.source");
    if (pillKind) pillKind.textContent = state.source === "playlist" ? T("editor.playlist") : (kindMap[state.kind] || T("editor.kindFallback"));
    const all = typeof total === "number" ? total : ((state.rows && state.rows.length) || 0);
    const vis = typeof visible === "number" ? visible : all;
    if (pillCount) pillCount.textContent = all ? (vis !== all ? `${vis}/${all}` : `${all}`) : "0";
    if (pillSync) pillSync.textContent = fmtSyncTime(state.lastSyncAt);
  }

  function fmtSyncTime(value) {
    if (!value) return T("snapshots.notYet");
    const diff = Math.max(0, Date.now() - Number(value));
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return T("playlists.ago.justNow");
    if (mins < 60) return T("playlists.ago.minutes", {count: mins});
    const hours = Math.floor(mins / 60);
    if (hours < 24) return T("playlists.ago.hours", {count: hours});
    const days = Math.floor(hours / 24);
    return T("playlists.ago.days", {count: days});
  }

  function setStatus(message) {
    if (state.hasChanges && message) changeNote.textContent = message;
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
    filterInput.placeholder = T("editor.filterEpisodes");
    if (state.filter) filterInput.value = state.filter;
  }

  const KIND_LABELS = { watchlist: T("editor.kind.watchlist"), history: T("editor.kind.history"), ratings: T("editor.kind.ratings"), progress: T("editor.kind.progress"), collection: T("editor.collections") };

  function syncKindUI() {
    if (!kindSel) return;
    let allowed = ["watchlist", "history", "ratings", "progress", "collection"];
    if (!allowed.includes(state.kind)) state.kind = allowed[0] || "watchlist";
    const current = Array.from(kindSel.options).map(o => o.value);
    if (current.join("|") !== allowed.join("|")) {
      kindSel.innerHTML = allowed
        .map(k => `<option value="${k}">${_escapeHtml(KIND_LABELS[k] || k)}</option>`)
        .join("");
    }
    kindSel.value = state.kind;
    window.CW?.IconSelect?.enhance(kindSel, {className:"cw-plain-select"});
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
    if (addBtn) addBtn.disabled = state.loading || state.saving || isMergedView() || (playlist && (!r.can_add || r.smart));
    if (saveBtn) saveBtn.disabled = state.saving || state.loading || (playlist && !playlistEditable());
    if (discardBtn) discardBtn.disabled = state.saving || state.loading;
    host.classList.toggle("cw-editor-single-source", (sourceSel?.options.length || 0) < 2);
  }

  function syncChangeBar() {
    changeBar.hidden = !state.hasChanges;
    changeNote.textContent = state.source === "playlist"
      ? T("editor.savePlaylist")
      : T("editor.savedNextSync");
  }

  function columnShown(column) {
    return state.advanced ? state.columnVisibility[column] !== false : SIMPLE_COLUMNS.includes(column);
  }

  function syncAdvancedUI() {
    host.classList.toggle("cw-editor-simple", !state.advanced);
    advancedBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">${state.advanced ? "view_agenda" : "tune"}</span><span>${state.advanced ? TH("editor.simple") : T("editor.advanced")}</span>`;
    advancedBtn.title = state.advanced ? T("editor.backSimple") : T("editor.showKeys");
  }

  function sendRow(row, target) {
    state.selected = new Set([row._rid]);
    syncBulkBar();
    renderRows();
    openEditorSendModal(target ? `${target.provider}:${target.instance}` : "");
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
    const width = clampColumnWidth(column, state.columnWidths[column]);
    return column === "extra" && ["history", "collection"].includes(state.kind) ? Math.max(210, width) : width;
  }

  function orderedColumns() {
    normalizeColumnState();
    return state.columnOrder;
  }

  function columnSortKey(column) {
    return column !== "status" && COLUMN_KEYS.includes(column) ? column : "";
  }

  function columnLabel(column) {
    if (column === "id") return isAnilistMode() ? "MAL" : "TMDB";
    if (column === "extra") {
      const labels = { history: T("editor.watchedOn"), ratings: T("editor.rating"), progress: T("editor.kind.progress"), collection: T("editor.collectedOn") };
      return (state.source !== "playlist" && labels[state.kind]) || T("editor.details");
    }
    return (COLUMN_META[column] && COLUMN_META[column].label) || column;
  }

  function actionColumnWidth() {
    return isPolicySource() ? (isMergedView() ? 84 : 120) : 46;
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
    table.style.setProperty("--cw-action-width", `${actionColumnWidth()}px`);
    const scrollEl = table.parentElement;
    const selectHead = table.querySelector("thead th:first-child");
    const actionHead = table.querySelector(".cw-action-head");
    const visibleColumns = orderedColumns().filter(columnShown);
    const widths = Object.fromEntries(COLUMN_KEYS.map(column => [column, columnWidth(column)]));
    let total = 34 + actionColumnWidth();
    visibleColumns.forEach(column => { total += widths[column]; });
    const containerWidth = Math.floor((scrollEl && scrollEl.clientWidth) || 0);
    const availableWidth = Math.max(0, containerWidth - 1);
    if (containerWidth > 0 && total > availableWidth && visibleColumns.includes("title")) {
      const shrink = Math.min(total - availableWidth, Math.max(0, widths.title - MIN_COLUMN_WIDTHS.title));
      widths.title -= shrink;
      total -= shrink;
    }
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
    wideBtn.title = active ? T("editor.exitWide") : T("editor.wide");
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
    handle.title = T("editor.cols.resize", {name: columnLabel(column)});
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
    const allOn = allowed.every(t => state.typeFilter[t] !== false);
    buttons.forEach(btn => {
      const t = btn.dataset.type;
      const visible = allowed.includes(t);
      btn.style.display = visible ? "" : "none";
      const on = !allOn && state.typeFilter[t] !== false;
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    });
    typeAllBtn.classList.toggle("active", allOn && !state.blockedOnly);
    typeAllBtn.setAttribute("aria-pressed", allOn && !state.blockedOnly ? "true" : "false");
    if (blockedOnlyBtn) blockedOnlyBtn.classList.toggle("active", !!state.blockedOnly);
    syncTypeChipVisibility();
  }

  function syncTypeChipVisibility() {
    if (!typeFilterWrap) return;
    const allowed = allowedTypesForKind(state.kind);
    const allOn = allowed.every(t => state.typeFilter[t] !== false);
    const present = new Set((state.rows || []).map(row => String(row.type || "").toLowerCase()));
    typeFilterWrap.querySelectorAll("button[data-type]").forEach(btn => {
      const t = btn.dataset.type;
      const used = present.has(t) || (!allOn && state.typeFilter[t] !== false);
      btn.style.display = allowed.includes(t) && (state.loading || !present.size || used) ? "" : "none";
    });
  }

  function syncPageSizeUI() {
    const options = mobilePaging.matches ? MOBILE_PAGE_SIZE_OPTIONS : PAGE_SIZE_OPTIONS;
    if (pageSizeSel) {
      pageSizeSel.innerHTML = options.map(value => `<option value="${value}">${value}</option>`).join("");
      pageSizeSel.value = String(state.pageSize);
    }
  }

  function syncColumnVisibilityUI() {
    const table = host.querySelector(".cw-table");
    normalizeColumnState();
    if (COLUMN_KEYS.includes(state.sortKey) && !columnShown(state.sortKey)) {
      state.sortKey = "title";
      state.sortDir = "asc";
    }
    const hiddenCount = COLUMN_KEYS.filter(k => !isRequiredColumn(k) && state.columnVisibility[k] === false).length;
    const customVisibility = COLUMN_KEYS.some(k => state.columnVisibility[k] !== DEFAULT_COLUMN_VISIBILITY[k]);
    const customOrder = orderedColumns().join("|") !== DEFAULT_COLUMN_ORDER.join("|");
    const customWidths = COLUMN_KEYS.some(k => columnWidth(k) !== DEFAULT_COLUMN_WIDTHS[k]);
    columnsBtn.classList.toggle("active", customVisibility || customOrder || customWidths);
    columnsBtn.title = hiddenCount > 0 ? T("editor.cols.hidden", {count: hiddenCount}) : customOrder || customWidths ? T("editor.cols.custom") : T("editor.columns");
    COLUMN_KEYS.forEach(k => {
      if (isRequiredColumn(k)) state.columnVisibility[k] = true;
      else if (typeof state.columnVisibility[k] !== "boolean") state.columnVisibility[k] = DEFAULT_COLUMN_VISIBILITY[k] !== false;
      const visible = state.columnVisibility[k] !== false;
      table?.classList.toggle(`cw-hide-col-${k}`, !columnShown(k));
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
    const show = isPolicySource() && !isMergedView() && state.kind !== "watchlist";
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

    if (!norm.length) norm.push({ id: "default", label: T("editor.default") });

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
      return renderInstanceOptions(selectEl, [{ id: "default", label: T("editor.default") }], current);
    }
    try {
      const data = await fetchJSON(`/api/provider-instances/${encodeURIComponent(provider)}`);
      return renderInstanceOptions(selectEl, Array.isArray(data) ? data : [], current);
    } catch (_) {
      return renderInstanceOptions(selectEl, [{ id: "default", label: T("editor.default") }], current);
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
  syncAdvancedUI();
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
        pageSize: state.desktopPageSize,
        mobilePageSize: state.mobilePageSize,
        wideView: state.wideView,
        advanced: state.advanced,
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
    bulkWrap.hidden = !n;
    if (!n) {
      window.CW?.SelectionBar?.update(bulkWrap, 0);
      return;
    }
    bulkCount.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">check_box_outline_blank</span><strong>${n}</strong><span>selected</span>`;
    window.CW?.SelectionBar?.update(bulkWrap, n);
    const setAction = (btn, icon, label) => {
      btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">${icon}</span><span>${label}</span>`;
      btn.title = label;
      btn.setAttribute("aria-label", label);
    };
    setAction(bulkSendBtn, "send", T("editor.send"));
    const picked = (state.rows || []).filter(row => state.selected.has(row._rid));
    bulkRemoveBtn.style.display = !isMergedView() && picked.some(row => !row.deleted) ? "" : "none";
    bulkRestoreBtn.style.display = !isMergedView() && picked.some(row => row.deleted) ? "" : "none";
    if (isPolicySource()) {
      setAction(bulkRemoveBtn, "block", T("editor.block"));
      setAction(bulkRestoreBtn, "undo", T("editor.unblock"));
    } else {
      setAction(bulkRemoveBtn, "delete", T("editor.deleteAction"));
      setAction(bulkRestoreBtn, "restore_from_trash", T("editor.restore"));
    }
    setAction(bulkClearBtn, "close", T("editor.clear"));
    bulkVisibleBtn.disabled = !state.pageRids?.length;
    bulkAllBtn.disabled = !applyFilter(state.rows || []).length;
  }

  function selectedRowsForSend(operation = "add") {
    const sel = state.selected || new Set();
    const picked = (state.rows || []).filter(row => sel.has(row._rid) &&
      (!row.deleted || (operation === "remove" && row._origin === "baseline")));
    if (operation !== "remove" || state.kind !== "history") return picked;
    const groups = new Map();
    for (const row of state.rows || []) {
      const base = editorRows.viewingBaseKey(row.key);
      if (!base) continue;
      if (!groups.has(base)) groups.set(base, []);
      groups.get(base).push(row);
    }
    const out = [];
    const titles = new Set();
    for (const row of picked) {
      const base = editorRows.viewingBaseKey(row.key);
      const members = base ? groups.get(base) : null;
      if (!members || !members.every(member => sel.has(member._rid))) {
        out.push(row);
        continue;
      }
      if (titles.has(base)) continue;
      titles.add(base);
      const raw = JSON.parse(JSON.stringify(row.raw || {}));
      delete raw._cw_event_key;
      delete raw._cw_rewatch_sync;
      delete raw.watched_at;
      out.push({ ...row, key: base, raw });
    }
    return out;
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

  async function openEditorSendModal(preselect = "") {
    const merged = isMergedView();
    const profile = mergedProfile();
    return editorSendModal.open({
      state,
      preselect,
      sendQuery: profile ? `user_profile=${encodeURIComponent(profile)}` : "",
      sendNotes: Object.fromEntries((merged ? state.mergedTargets || [] : [])
        .filter(target => (target.shared_with || []).length)
        .map(target => [`${target.provider}:${target.instance}`, T("editor.sharedBy", {names: target.shared_with.join(", ")})])),
      sendScope: merged && profile ? new Set((state.mergedTargets || []).filter(target => target.can_send).map(target => `${target.provider}:${target.instance}`)) : null,
      selectedRowsForSend,
      rowToSendItem,
      fetchJSON,
      escapeHtml: _escapeHtml,
      currentPlaylistEndpoint,
      isProviderPickerSource,
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
        ? isPolicySource() ? T("editor.blocked") : T("editor.deleted")
        : isPolicySource() ? T("editor.status.unblocked") : T("editor.restored");
      setStatusSticky(T("editor.verbItems", {verb, count: changed}), 3000);
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
        T("editor.verbTypeItems", {verb: flag ? T("editor.status.blocked") : T("editor.status.unblocked"), count: changed, type: t}),
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
    setTag("warn", T("editor.unsaved"));
  }

  let activePopup = null;

  function closePopup() {
    if (!activePopup) return;
    document.removeEventListener("mousedown", activePopup.onDoc);
    document.removeEventListener("keydown", activePopup.onKey);
    if (activePopup.onScroll) document.removeEventListener("scroll", activePopup.onScroll, true);
    if (activePopup.onResize) window.removeEventListener("resize", activePopup.onResize);
    if (activePopup.repositionFrame) cancelAnimationFrame(activePopup.repositionFrame);
    if (activePopup.node && activePopup.node.parentNode) {
      if (activePopup.node instanceof HTMLDialogElement) activePopup.node.close();
      activePopup.node.parentNode.removeChild(activePopup.node);
    }
    activePopup = null;
  }

  function positionPopup(pop, anchor) {
    if (pop instanceof HTMLDialogElement && window.CW?.MobileUI?.mobile()) return;
    const rect = anchor.getBoundingClientRect();
    const margin = 8;
    const viewportWidth = document.documentElement.clientWidth;
    const viewportHeight = document.documentElement.clientHeight;
    let left = rect.left;
    let top = rect.bottom + margin;
    const width = pop.offsetWidth;
    const height = pop.offsetHeight;
    if (left + width + margin > viewportWidth) {
      left = viewportWidth - width - margin;
    }
    if (top + height + margin > viewportHeight) {
      top = rect.top - height - margin;
    }
    if (left < margin) left = margin;
    if (top < margin) top = margin;
    pop.style.left = left + "px";
    pop.style.top = top + "px";
  }

  function openPopup(anchor, builder, opts = {}) {
    closePopup();
    const isMobile = window.CW?.MobileUI?.mobile();
    if (isMobile && !anchor.getClientRects().length) anchor = host.querySelector('.cw-editor-tools-toggle') || anchor;
    const pop = document.createElement(isMobile ? "dialog" : "div");
    pop.className = "cw-pop";
    document.body.appendChild(pop);

    function doClose() {
      closePopup();
    }

    builder(pop, doClose);
    if (!isMobile) positionPopup(pop, anchor);

    const onDoc = ev => {
      if (pop.contains(ev.target) || anchor.contains(ev.target)) return;
      closePopup();
    };
    const onKey = ev => {
      if (ev.key === "Escape") closePopup();
    };
    const popupState = { node: pop, onDoc, onKey, onScroll: null, onResize: null, repositionFrame: 0 };
    const onReposition = () => {
      if (popupState.repositionFrame) return;
      popupState.repositionFrame = requestAnimationFrame(() => {
        popupState.repositionFrame = 0;
        if (!activePopup || activePopup.node !== pop) return;
        if (!document.body.contains(anchor)) {
          closePopup();
          return;
        }
        positionPopup(pop, anchor);
      });
    };
    if (!isMobile && opts.trackAnchorOnScroll === true) {
      popupState.onScroll = onReposition;
      popupState.onResize = onReposition;
    }
    activePopup = popupState;
    if (isMobile) {
      const done = document.createElement('button');
      done.type = 'button';
      done.className = 'cw-btn';
      done.textContent = T("editor.done");
      done.addEventListener('click', doClose);
      if (![...pop.querySelectorAll('button')].some(button => /^(Close|Done)$/.test(button.textContent.trim()))) pop.append(done);
      pop.addEventListener('close', () => { if (activePopup === popupState) closePopup(); anchor.focus({preventScroll:true}); }, {once:true});
      window.CW.MobileUI.openDialog(pop);
    }
    if (opts.closeOnOutside !== false) document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    if (popupState.onScroll) document.addEventListener("scroll", popupState.onScroll, true);
    if (popupState.onResize) window.addEventListener("resize", popupState.onResize);
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
    if (column === "status") {
      syncStatusColumn();
      updateStatusCells();
    }
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
    appendPopupTitle(pop, T("editor.columns"));
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
      toggle.title = isRequiredColumn(column) ? T("editor.cols.alwaysShown", {name: columnLabel(column)}) : T("editor.cols.show", {name: columnLabel(column)});
      toggle.innerHTML = `<span class="material-symbol cw-type-icon" aria-hidden="true">${meta.icon}</span><span>${columnLabel(column)}</span>`;
      toggle.addEventListener("click", () => toggleColumnVisibility(column));

      const left = document.createElement("button");
      left.type = "button";
      left.className = "cw-column-move";
      left.disabled = idx === 0;
      left.title = T("editor.cols.moveLeft", {name: columnLabel(column)});
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
      right.title = T("editor.cols.moveRight", {name: columnLabel(column)});
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
      { label: T("editor.reset"), onClick: () => resetColumnLayout(pop) },
      { label: T("editor.close"), kind: "primary", onClick: closePopup },
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
    if (isMergedView()) return false;
    return state.kind === "ratings" || state.kind === "history" || state.kind === "progress" || state.kind === "collection";
  }

  function manualCorrectionKindLabel(kind = state.kind) {
    const k = String(kind || "").toLowerCase();
    if (k === "history") return "history date";
    if (k === "ratings") return "rating";
    if (k === "progress") return "progress";
    if (k === "collection") return "collection date";
    return "extra field";
  }

  function extraCorrectionStatus(row, promoted) {
    if (!isPolicySource() || !row || !isExtraKindEditable()) return "";
    const label = manualCorrectionKindLabel();
    if (promoted) return T("editor.correction.created", {kind: label});
    if (row._origin === "manual") return T("editor.correction.updated", {kind: label});
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
    const groupPart = r => r._origin === "baseline" && !!state.mappingPair
      && (r._episodeGroups || []).some(group => group.pair_id === state.mappingPair);
    const clash = (state.rows || []).some(
      r => r !== row && !r.deleted && String(r.key || "").toLowerCase() === key.toLowerCase() && !groupPart(r)
    );
    if (clash) return T("editor.exists");

    if (row._origin === "baseline") {
      row.deleted = true;
      state.rows.unshift(buildManualRow(corrected, key, row.key));
      setStatusSticky(T("editor.originalBlocked"), 6000);
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
      openMapping: async row => {
        if (state.loading || state.saving || state.mappingEditing) return;
        state.mappingEditing = true;
        try {
          const {openEditorMapping} = await import(`/assets/js/editor/mapping.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
          await openEditorMapping(row, {state, fetchJSON, commitReplacement, saveChanges: saveMappingFix, loadState});
        } catch (error) { state.mappingEditing = false; setStatusSticky(error.message, 6000); }
      },
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

  async function saveMappingFix() {
    await saveState();
    if (!state.hasChanges) window.cxToast?.(T("editor.fixSaved"));
  }

  async function openMappingProvider(row, target) {
    if (state.loading || state.saving || state.mappingEditing) return;
    if (state.hasChanges) {
      setStatusSticky(T("editor.saveBeforeMapping"), 6000);
      return;
    }
    const previous = {...state, typeFilter:{...state.typeFilter}};
    try {
      state.snapshot = target.provider;
      state.instance = target.instance || "default";
      state.mappingPair = "";
      state.blockedOnly = false;
      state.filter = row.title || row.raw?.series_title || row.key;
      for (const type of Object.keys(state.typeFilter)) state.typeFilter[type] = true;
      syncSourceUI();
      await loadSnapshots();
      if (state.snapshot !== target.provider || state.instance !== (target.instance || "default")) {
        throw new Error(T("editor.accountGone"));
      }
      await loadState();
      if (state.loadError) throw new Error(T("editor.loadProviderFailed"));
      if (filterInput) filterInput.value = state.filter;
      syncTypeFilterUI();
      persistUIState();
      renderRows();
      setStatusSticky(T("editor.chooseMappingIcon"), 6000);
    } catch (error) {
      Object.assign(state, previous);
      syncSourceUI(); rebuildSnapshots();
      if (instanceSel) instanceSel.value = state.instance;
      if (filterInput) filterInput.value = state.filter;
      syncProfileIconSelect(instanceSel, true); syncTypeFilterUI(); renderRows();
      setStatusSticky(error.message, 6000);
    }
  }

  function openItemReplacer(row, anchor) {
    if (isMergedView()) {
      if (state.loading || state.saving || state.mappingEditing) return;
      const targets = (row.presence || []).map(entry => state.mergedTargets?.[entry[0]])
        .filter(target => target && target.verified !== false);
      openPopup(anchor, (pop, close) => {
        appendPopupTitle(pop, T("editor.chooseMappingProvider"));
        const note = document.createElement("p");
        note.textContent = targets.length
          ? T("editor.openProviderHint")
          : T("editor.noConfirmedProvider");
        pop.appendChild(note);
        const select = document.createElement("select");
        select.className = "cw-select";
        select.setAttribute("aria-label", T("editor.mappingAccount"));
        targets.forEach((target, index) => {
          const option = document.createElement("option");
          option.value = String(index);
          option.textContent = target.display || target.label || target.provider;
          select.appendChild(option);
        });
        if (targets.length) pop.appendChild(select);
        appendPopupActions(pop, [
          {label:T("editor.cancel"), onClick:close},
          ...(targets.length ? [{label:T("editor.openProvider"), kind:"primary", onClick:() => {
            const target = targets[Number(select.value)];
            close(); openMappingProvider(row, target);
          }}] : []),
        ]);
      });
      return;
    }
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
    status.textContent = T("editor.readOnlyHint");
    pop.appendChild(status);
    appendPopupActions(pop, [{ label: T("editor.close"), kind: "primary", onClick: close }]);
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
      isMergedView,
      isAnilistMode,
      isRowLocked,
      isExtraKindEditable,
      canReplaceRow,
      usesCoordinateReplacer,
      rowType,
      markChanged,
      renderRows,
      loadState,
      syncBulkBar,
      syncSelectPageCheckbox,
      updateTypeDisplay,
      updateExtraDisplay,
      createDateTimeEditor: row => editorExtraEditors.createDateTimeEditor(row, extraEditorContext()),
      statusForRow: row => editorRows.statusForRow(row, state.statusSnapshot),
      updateStatusCells,
      formatEpisodeVisualTitle,
      formatSxxEyy,
      openTypeEditor,
      openTitleSearchEditor,
      openItemReplacer,
      canTraceOrigin,
      openItemOrigin,
      openRawFieldsModal,
      sendRow,
      columnLabel,
      openRatingEditor,
      openHistoryEditor,
      openCollectionEditor,
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

  function openCollectionEditor(row, anchor, displayEl) {
    return editorExtraEditors.openCollectionEditor(row, anchor, displayEl, extraEditorContext());
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

  function canTraceOrigin(row) {
    return !!row && isProviderPickerSource() && !isMergedView() && !!state.snapshot;
  }

  async function openItemOrigin(row, anchor) {
    if (!canTraceOrigin(row)) return;
    try {
      const {openItemOrigin: open} = await import(`/assets/js/editor/origin.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
      const replacer = metadataReplacerContext();
      open(row, anchor, {state, fetchJSON, openPopup, positionPopup, appendPopupTitle, appendPopupActions,
        canReplaceRow, openMapping: replacer.openMapping});
    } catch (error) {
      setStatusSticky(error.message, 6000);
    }
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
      setStatusSticky(T("editor.openDetailsFailed"), 3500);
    }
  }

  function sortRows(rows) {
    return editorTableController.sortRows(rows, tableControllerContext());
  }

  function updateSortUI() {
    return editorTableController.updateSortUI(tableControllerContext());
  }

  function updateStatusCells(changedRow = null) {
    const rows = new Map((changedRow ? [changedRow] : state.rows).map(row => [String(row._rid), row]));
    host.querySelectorAll("[data-status-row]").forEach(cell => {
      const row = rows.get(cell.dataset.statusRow);
      if (!row) return;
      const status = editorRows.statusForRow(row, state.statusSnapshot);
      cell.textContent = status.label;
      cell.title = status.hint;
    });
  }

  function syncStatusColumn() {
    if (!columnShown("status") || state.loading || !state.rows.length || !isProviderPickerSource() || isMergedView() || !state.snapshot || state.snapshot === ALL_PROVIDERS) {
      state.statusSnapshot = null;
      return;
    }
    const scope = JSON.stringify([state.source, state.snapshot, state.instance]);
    if (state.statusSnapshot?.scope === scope && state.statusSnapshot.rows === state.rows) return;
    const snapshot = state.statusSnapshot = {scope, rows: state.rows, loading: true};
    const params = new URLSearchParams({provider: state.snapshot, provider_instance: state.instance || "default"});
    fetchJSON(`/api/editor/statuses?${params}`).then(data => {
      snapshot.data = data;
    }).catch(() => {
      snapshot.error = true;
    }).finally(() => {
      snapshot.loading = false;
      if (state.statusSnapshot === snapshot) updateStatusCells();
    });
  }

  function renderRows() {
    syncStatusColumn();
    const result = editorTableController.renderRows(tableControllerContext());
    syncTypeChipVisibility();
    return result;
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
    return T("editor.snapshot");
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
      isProviderPickerSource,
      isPolicySource,
      isMergedView,
      mergedProfile,
      syncMergedScope,
      normalizeSource,
      fetchJSON,
      buildRows,
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
    const options = mobilePaging.matches ? MOBILE_PAGE_SIZE_OPTIONS : PAGE_SIZE_OPTIONS;
    state.pageSize = options.includes(next) ? next : mobilePaging.matches ? 20 : DEFAULT_PAGE_SIZE;
    state.page = 0;
    syncPageSizeUI();
    persistUIState();
    renderRows();
  });

  mobilePaging.addEventListener("change", () => {
    state.page = 0;
    syncPageSizeUI();
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
      const allowed = allowedTypesForKind(state.kind);
      if (allowed.every(x => state.typeFilter[x] !== false)) {
        allowed.forEach(x => { state.typeFilter[x] = x === t; });
      } else {
        state.typeFilter[t] = !state.typeFilter[t];
        if (!allowed.some(x => state.typeFilter[x])) allowed.forEach(x => { state.typeFilter[x] = true; });
      }
      syncTypeFilterUI();
      state.page = 0;
      persistUIState();
      renderRows();
    });
  }

  typeAllBtn.addEventListener("click", () => {
    allowedTypesForKind(state.kind).forEach(t => { state.typeFilter[t] = true; });
    state.blockedOnly = false;
    syncTypeFilterUI();
    state.page = 0;
    persistUIState();
    renderRows();
  });

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
      state.mappingPair = "";
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
      state.mappingPair = "";
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
      await loadState();
    });
  }

  if (snapSel) {
    snapSel.addEventListener("change", async () => {
      state.snapshot = snapSel.value || "";
      state.mappingPair = "";
      if (isProviderPickerSource()) syncProviderIconSelect(snapSel, true);
      else if (state.source === "playlist") syncPlaylistEndpointIconSelect(snapSel, true);
      if (isMergedView()) {
        state.instance = "default";
        persistUIState();
      } else if (isProviderPickerSource()) {
        if (instanceLabel) instanceLabel.style.display = "";
        if (instanceSel) instanceSel.style.display = "";
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
      state.mappingPair = "";
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

  [[importWatchlistCb, "watchlist"], [importHistoryCb, "history"], [importRatingsCb, "ratings"], [importProgressCb, "progress"], [importCollectionCb, "collection"]]
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
  on(bulkClearBtn, "click", () => { clearSelection(); renderRows(); selectPage?.focus(); });
  on(bulkVisibleBtn, "click", () => {
    for (const rid of state.pageRids || []) state.selected.add(rid);
    renderRows();
  });
  on(bulkAllBtn, "click", () => {
    for (const row of applyFilter(state.rows || [])) state.selected.add(row._rid);
    renderRows();
  });
  on(bulkSendBtn, "click", () => openEditorSendModal());
  on(bulkBlockTypeBtn, "click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, true));
  on(bulkUnblockTypeBtn, "click", () => bulkSetBlocksByType(bulkTypeSel && bulkTypeSel.value, false));

  on(addBtn, "click", addRow);
  on(saveBtn, "click", saveState);
  on(discardBtn, "click", async () => {
    if (!(await (window.CW?.MobileUI?.ask || window.confirm)(T("editor.discardAll")))) return;
    state.hasChanges = false;
    clearSelection();
    await loadState();
  });
  on(advancedBtn, "click", async () => {
    const resetScope = state.advanced && !!state.mappingPair;
    if (resetScope && state.hasChanges) {
      window.cxToast?.(T("editor.leaveAdvanced"));
      return;
    }
    state.advanced = !state.advanced;
    closePopup();
    syncAdvancedUI();
    persistUIState();
    if (resetScope) {
      state.mappingPair = "";
      await loadState();
      return;
    }
    renderRows();
  });
  on(quickAddBtn, "click", () => {
    if (typeof window.openManualWatchedModal !== "function") return;
    const action = { history: "history", watchlist: "watchlist", ratings: "rating", collection: "collection" }[state.kind];
    window.openManualWatchedModal(action ? { action } : {});
  });
  window.addEventListener("cw:manual-watched-saved", () => {
    if (editorIsVisible()) window.cxToast?.(T("editor.sentProviders"));
  });

  window.addEventListener("beforeunload", e => {
    if (!state.hasChanges) return;
    e.preventDefault();
    e.returnValue = "";
  });

  on(stateDownloadBtn, "click", () => downloadFile("/api/editor/state/manual/export", "crosswatch-state-policy.json", T("editor.policyDownloaded")));

  bindFileImport(stateUploadBtn, stateUploadInput, "/api/editor/state/manual/import?mode=merge", async data => {
    const msg = T("editor.imported", {what: listParts(data, [["providers", "provider"], ["blocks", "block"], ["adds", "add"]]).join(", ") || T("editor.policy")});
    if (window.cxToast) window.cxToast(msg);
    setTag("warn", T("editor.importedTag"));
    await loadSnapshots();
    await loadState();
  });

  let recoveryReady = false;
  async function openRecoveredHistory() {
    const receipt = window.CW?.pendingRecovery;
    if (!recoveryReady || !receipt || state.loading || state.saving) return;
    if (state.hasChanges && !(await (window.CW?.MobileUI?.ask || window.confirm)(T("editor.discardRecovered")))) return;
    window.CW.pendingRecovery = null;
    try {
      state.source = "state"; state.snapshot = "CROSSWATCH"; state.instance = receipt.instance;
      state.kind = "history"; state.mappingPair = ""; state.filter = ""; state.blockedOnly = false;
      Object.keys(state.typeFilter).forEach(key => { state.typeFilter[key] = true; });
      if (filterInput) filterInput.value = "";
      syncSourceUI(); syncTypeFilterUI();
      await loadSnapshots(); await loadState();
      if (state.loadError) throw new Error(state.loadError);
      if (state.source !== "state" || state.snapshot !== "CROSSWATCH" || state.instance !== receipt.instance || state.kind !== "history") {
        throw new Error(T("editor.trackerUnavailable"));
      }
      const keys = new Set(receipt.keys);
      state.selected = new Set(state.rows.filter(row => keys.has(row.key) && !row.deleted).map(row => row._rid));
      state.page = 0;
      renderRows(); syncBulkBar();
      const count = state.selected.size;
      setStatus(T("editor.recoveredSelected", {count}));
    } catch (error) { setStatus(error.message || T("editor.openRecoveredFailed")); }
  }
  window.addEventListener("cw:open-recovery", openRecoveredHistory);

  (async () => {
    const wanted = state.source;
    state.source = normalizeSource(wanted);
    if (state.source !== wanted) {
      state.snapshot = "";
      state.instance = "default";
      persistUIState();
    }
    syncSourceUI();
    host.setAttribute("aria-busy", "true");
    renderRows();
    loadImportProviders().catch(error => console.warn("Editor import setup failed:", error));
    setTag(
      "warn",
      state.source === "state"
        ? T("editor.loadingState")
        : T("editor.loadingPlaylist")
    );
    await loadSnapshots();
    await loadState();
    state.lastSyncAt = Date.now();
    recoveryReady = true;
    await openRecoveredHistory();
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
