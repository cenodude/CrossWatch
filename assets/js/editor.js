/* assets/js/editor.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const PAGE_SIZE = 100;
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
    playlistResource: null,
    playlistWarnings: [],
    playlistOriginalKeys: [],
    workspace: "",
    trackerWorkspaces: [],
    trackerAvailable: false,
    importEnabled: false,
    importProviders: [],
    importProvider: "",
    importProviderInstance: "default",
    importMode: "replace",
    importFeatures: { watchlist: true, history: true, ratings: true, progress: true },
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

      const sources = ["state", "tracker", "playlist"];
      if (saved.source && sources.includes(saved.source)) state.source = saved.source;

      if (typeof saved.workspace === "string") state.workspace = saved.workspace;

      if (typeof saved.blockedOnly === "boolean") state.blockedOnly = saved.blockedOnly;

      const kinds = ["watchlist", "history", "ratings", "progress"];
      if (saved.kind && kinds.includes(saved.kind)) state.kind = saved.kind;

      if (typeof saved.snapshot === "string") state.snapshot = saved.snapshot;
      if (typeof saved.instance === "string" && saved.instance.trim()) state.instance = saved.instance;

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

  function wireStaticLabels(root) {
    if (!root) return;

    const bindPrevLabel = (fieldId) => {
      const field = root.querySelector(`#${fieldId}`);
      const label = field?.previousElementSibling;
      if (label?.tagName === "LABEL") label.htmlFor = fieldId;
    };

    bindPrevLabel("cw-source");
    bindPrevLabel("cw-kind");
    bindPrevLabel("cw-pair");
    bindPrevLabel("cw-snapshot");
    bindPrevLabel("cw-instance");

    const convertGroupLabel = (cardId) => {
      const field = root.querySelector(`#${cardId} .ins-kv`);
      const label = field?.firstElementChild;
      if (!field || label?.tagName !== "LABEL") return;
      const title = document.createElement("div");
      title.className = "field-label";
      title.textContent = label.textContent || "";
      label.replaceWith(title);
    };

    convertGroupLabel("cw-backup-card");
    convertGroupLabel("cw-state-backup-card");
  }

  host.innerHTML = `<div class="cw-root"><div class="cw-topline cw-page-hero cw-page-hero-editor" data-hero-icon="edit_note"><div class="cw-head-copy cw-page-hero-copy"><div class="cw-page-hero-kicker">EDITOR</div><div class="cw-title-row"><div><div class="cw-title cw-page-hero-title">Editor</div><div class="cw-sub cw-page-hero-sub">Edit your current state, tracker or cache</div></div></div></div><div class="cw-editor-hero-summary cw-page-hero-actions" id="cw-hero-summary" aria-label="Editor summary"><div class="cw-editor-hero-seg"><strong id="cw-pill-source">Current state</strong><span>source</span></div><div class="cw-editor-hero-seg"><strong id="cw-pill-kind">Watchlist</strong><span>view</span></div><div class="cw-editor-hero-seg cw-editor-hero-count"><strong id="cw-pill-count">0</strong><span>rows</span></div><div class="cw-editor-hero-seg cw-editor-hero-sync"><span>Synced</span><strong id="cw-pill-sync">never</strong></div><button id="cw-reload" class="cw-editor-refresh" type="button" title="Refresh editor data" aria-label="Refresh editor data"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button></div></div><div class="cw-wrap"><div class="cw-main"><div class="cw-controls"><input id="cw-filter" class="cw-input" placeholder="Filter by key / title / id..."><span class="cw-status-text" id="cw-status"></span><div class="cw-controls-spacer"></div><div class="cw-bulk" id="cw-bulk" style="display:none"><span class="cw-bulk-count" id="cw-bulk-count"></span><button id="cw-bulk-remove" class="cw-btn danger" type="button"></button><button id="cw-bulk-restore" class="cw-btn" type="button"></button><button id="cw-bulk-clear" class="cw-btn" type="button">Clear</button></div><button id="cw-add" class="cw-btn" type="button">Add row</button><button id="cw-save" class="cw-btn primary" type="button">Save changes</button></div><div class="cw-table-wrap" id="cw-table-wrap"><div class="cw-table-scroll"><table class="cw-table"><thead><tr><th style="width:34px"><input id="cw-select-page" class="cw-checkbox" type="checkbox" title="Select page"></th><th class="cw-action-head" style="width:46px"></th><th style="width:12%" data-sort="key" class="sortable">Key</th><th style="width:13%" data-sort="type" class="sortable">Type</th><th style="width:33%" data-sort="title" class="sortable">Title</th><th style="width:84px">Year</th><th style="width:12%" id="cw-col-id-a">TMDB</th><th style="width:21%" data-sort="extra" class="sortable">Extra</th></tr></thead><tbody id="cw-tbody"></tbody></table></div></div><div class="cw-pager" id="cw-pager" style="display:none"><button id="cw-prev" class="cw-btn" type="button">Previous</button><span id="cw-page-info" class="cw-page-info"></span><button id="cw-next" class="cw-btn" type="button">Next</button></div><div class="cw-empty" id="cw-empty" role="status" style="display:none"><span class="material-symbols-rounded cw-empty-icon" aria-hidden="true">table_rows</span><div class="cw-empty-text">No rows match this view.</div></div></div><aside class="cw-side"><div class="ins-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">tune</span></div><div class="ins-title">Workspace</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Source</label><select id="cw-source" class="cw-select"><option value="state">Current State</option><option value="pair">Pair Cache</option><option value="tracker">Local Tracker</option></select><label>Kind</label><select id="cw-kind" class="cw-select"><option value="watchlist">Watchlist</option><option value="history">History</option><option value="ratings">Ratings</option><option value="progress">Progress</option></select><label id="cw-pair-label" style="display:none">Pair</label><select id="cw-pair" class="cw-select" style="display:none"></select><label id="cw-snapshot-label">Snapshot</label><select id="cw-snapshot" class="cw-select"><option value="">Latest</option></select><label id="cw-instance-label" style="display:none">Profile</label><select id="cw-instance" class="cw-select" style="display:none"><option value="default">Default</option></select></div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><div class="field-label">Types</div><div id="cw-type-filter" class="cw-type-filter"><button type="button" data-type="movie" class="cw-type-chip active">Movies</button><button type="button" data-type="show" class="cw-type-chip active">Shows</button><button type="button" data-type="anime" class="cw-type-chip active">Anime</button><button type="button" data-type="season" class="cw-type-chip active">Seasons</button><button type="button" data-type="episode" class="cw-type-chip active">Episodes</button><button type="button" id="cw-blocked-only" class="cw-type-chip">Blocked</button></div></div></div><div class="ins-row" id="cw-state-bulk" style="display:none"><details class="cw-collapse" id="cw-bulk-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">Block rules</summary><div style="display:flex;flex-direction:column;gap:8px;width:100%;margin-top:10px"><select id="cw-bulk-type" class="cw-select" style="width:100%"></select><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button id="cw-bulk-block-type" class="cw-btn danger" type="button" style="flex:1 1 0;min-width:120px">Block all</button><button id="cw-bulk-unblock-type" class="cw-btn" type="button" style="flex:1 1 0;min-width:120px">Unblock all</button></div><div class="cw-status-text">Current State only • affects baseline items</div></div></details></div><div class="ins-row" id="cw-import-row" style="display:none"><details class="cw-collapse" id="cw-import-details" style="width:100%"><summary style="cursor:pointer;font-weight:700;user-select:none">Import provider state</summary><div style="display:flex;flex-direction:column;gap:10px;width:100%;margin-top:10px"><div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center"><select id="cw-import-provider" class="cw-select" style="flex:1;min-width:200px"></select><select id="cw-import-instance" class="cw-select" style="min-width:180px"></select><select id="cw-import-mode" class="cw-select" style="min-width:180px"><option value="replace">Replace baseline</option><option value="merge">Merge (keep old)</option></select></div><div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center"><label id="cw-import-watchlist-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-watchlist" class="cw-checkbox" type="checkbox" checked>Watchlist </label><label id="cw-import-history-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-history" class="cw-checkbox" type="checkbox" checked>History </label><label id="cw-import-ratings-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-ratings" class="cw-checkbox" type="checkbox" checked>Ratings </label><label id="cw-import-progress-wrap" style="display:flex;gap:6px;align-items:center;font-size:12px;width:auto;margin:0"><input id="cw-import-progress-cb" class="cw-checkbox" type="checkbox" checked>Progress </label><span style="flex:1 1 auto"></span><button id="cw-import-run" class="cw-btn sm" type="button">Import</button></div><div id="cw-import-progress" style="display:none"><div class="cw-progress"><span></span></div><div class="cw-status-text" id="cw-import-progress-text" style="margin-top:6px"></div></div></div></details></div></div><div class="ins-card"><div class="ins-row" style="align-items:center"><div class="ins-icon"><span class="material-symbol">insights</span></div><div class="ins-title" style="margin-right:auto">Pulse</div><span class="cw-tag" id="cw-tag-status"><span class="cw-tag-dot"></span><span id="cw-tag-label">Idle</span></span></div><div class="ins-row"><div class="ins-metrics"><div class="metric-row"><div class="metric"><span class="material-symbol">view_list</span><div><div class="m-val" id="cw-summary-total">0</div><div class="m-lbl">Total rows</div></div></div><div class="metric"><span class="material-symbol">visibility</span><div><div class="m-val" id="cw-summary-visible">0</div><div class="m-lbl">Rows visible</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">movie</span><div><div class="m-val" id="cw-summary-movies">0</div><div class="m-lbl">Movies</div></div></div><div class="metric"><span class="material-symbol">monitoring</span><div><div class="m-val" id="cw-summary-shows">0</div><div class="m-lbl">Shows</div></div></div><div class="metric"><span class="material-symbol">layers</span><div><div class="m-val" id="cw-summary-seasons">0</div><div class="m-lbl">Seasons</div></div></div><div class="metric"><span class="material-symbol">live_tv</span><div><div class="m-val" id="cw-summary-episodes">0</div><div class="m-lbl">Episodes</div></div></div></div><div class="metric-divider"></div><div class="metric-row"><div class="metric"><span class="material-symbol">description</span><div><div class="m-val" id="cw-summary-state-files">0</div><div class="m-lbl">State files</div></div></div><div class="metric"><span class="material-symbol">folder_copy</span><div><div class="m-val" id="cw-summary-snapshots">0</div><div class="m-lbl">Snapshots</div></div></div></div><div id="cw-state-hint" class="cw-state-hint" style="display:none"><strong>No tracker data found.</strong> Run a CrossWatch sync with the tracker enabled once. After that, tracker state files and snapshots will appear here and you can edit them. </div></div></div></div><div class="ins-card" id="cw-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">Archive</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Export / Import</label><div class="cw-backup-actions"><button id="cw-download" class="cw-btn" type="button">Download ZIP</button><button id="cw-upload" class="cw-btn" type="button">Import file</button><input id="cw-upload-input" type="file" accept=".zip,.json" style="display:none"></div></div></div></div><div class="ins-card" id="cw-state-backup-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">backup</span></div><div class="ins-title">Policy backup</div></div><div class="ins-row"><div class="ins-kv" style="width:100%"><label>Export / Import</label><div class="cw-backup-actions"><button id="cw-state-download" class="cw-btn" type="button">Download JSON</button><button id="cw-state-upload" class="cw-btn" type="button">Import file</button><input id="cw-state-upload-input" type="file" accept=".json" style="display:none"></div></div></div></div></aside></div></div>`;

  wireStaticLabels(host);

  const sourceSelectBoot = document.getElementById("cw-source");
  if (sourceSelectBoot) {
    sourceSelectBoot.querySelector('option[value="pair"]')?.remove();
    if (!sourceSelectBoot.querySelector('option[value="tracker"]')) {
      const trackerOpt = document.createElement("option");
      trackerOpt.value = "tracker";
      trackerOpt.textContent = "Local Tracker";
      sourceSelectBoot.appendChild(trackerOpt);
    } else {
      sourceSelectBoot.querySelector('option[value="tracker"]').textContent = "Local Tracker";
    }
    if (!sourceSelectBoot.querySelector('option[value="playlist"]')) {
      const opt = document.createElement("option");
      opt.value = "playlist";
      opt.textContent = "Playlist Endpoint";
      sourceSelectBoot.appendChild(opt);
    }
  }
  const subBoot = host.querySelector(".cw-sub");
  if (subBoot) subBoot.textContent = "Edit your current state or playlist endpoints";

  const trackerNoticeBoot = document.createElement("div");
  trackerNoticeBoot.id = "cw-tracker-notice";
  trackerNoticeBoot.className = "cw-state-hint";
  trackerNoticeBoot.style.display = "none";
  trackerNoticeBoot.innerHTML =
    "<strong>Local Tracker</strong> stores data inside CrossWatch. Changes here affect future syncs from Local Tracker but only for one-way syncs.";
  host.querySelector(".cw-controls")?.insertAdjacentElement("afterend", trackerNoticeBoot);

  host.querySelectorAll("input,select,textarea").forEach((field, idx) => {
    if (!field.name) field.name = field.id || `cw-field-${idx + 1}`;
  });

  const $ = id => document.getElementById(id);
  const pickEls = spec => Object.fromEntries(Object.entries(spec).map(([key, id]) => [key, $(id)]));
  const {
    sourceSel, kindSel, pairLabel, pairSel, snapLabel, snapSel, instanceLabel, instanceSel,
    filterInput, reloadBtn, addBtn, saveBtn, tbody, empty, statusEl, tag, tagLabel,
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

  if (backupCard) backupCard.remove();
  [summaryStateFiles, summarySnapshots].forEach(el => el?.closest(".metric")?.remove());

  function decorateImportPanel() {
    const details = document.getElementById("cw-import-details");
    if (!details || details.dataset.decorated === "1") return;
    details.dataset.decorated = "1";
    details.classList.add("cw-import-panel");

    const summary = details.querySelector("summary");
    if (summary) {
      summary.classList.add("cw-import-summary");
      summary.innerHTML =
        '<span class="cw-import-title">Import provider state</span>' +
        '<span class="cw-import-help material-symbol" title="Imports selected Watchlist, History, Ratings, or Progress data from a configured provider profile into Current State. Replace baseline refreshes those datasets from the provider; Merge keeps existing baseline rows and adds or updates provider rows. Provider accounts are not changed." aria-label="Import provider state help">help</span>';
    }

    const body = summary ? summary.nextElementSibling : null;
    if (body instanceof HTMLElement) {
      body.classList.add("cw-import-body");
      body.style.cssText = "";
      const rows = Array.from(body.children).filter(el => el instanceof HTMLElement);
      const fields = rows[0];
      const actions = rows[1];
      if (fields instanceof HTMLElement) {
        fields.classList.add("cw-import-fields");
        fields.style.cssText = "";
      }
      if (actions instanceof HTMLElement) {
        actions.classList.add("cw-import-actions");
        actions.style.cssText = "";
      }
    }

    const wrapField = (el, label) => {
      if (!(el instanceof HTMLElement) || el.parentElement?.classList.contains("cw-import-field")) return;
      const field = document.createElement("label");
      field.className = "cw-import-field";
      const text = document.createElement("span");
      text.className = "cw-import-field-label";
      text.textContent = label;
      el.parentNode.insertBefore(field, el);
      field.append(text, el);
    };

    wrapField(importProviderSel, "Provider");
    wrapField(importInstanceSel, "Profile");
    wrapField(importModeSel, "Mode");

    const featureWrap = document.createElement("div");
    featureWrap.className = "cw-import-features";
    [importWatchlistWrap, importHistoryWrap, importRatingsWrap, importProgressFeatWrap].forEach(wrap => {
      if (!(wrap instanceof HTMLElement)) return;
      wrap.classList.add("cw-import-feature");
      wrap.style.cssText = "";
      featureWrap.append(wrap);
    });

    const runRow = document.createElement("div");
    runRow.className = "cw-import-run-row";
    if (importRunBtn) runRow.append(importRunBtn);

    const actions = details.querySelector(".cw-import-actions");
    if (actions) {
      actions.textContent = "";
      actions.append(featureWrap, runRow);
    }

    if (importProgressWrap) importProgressWrap.classList.add("cw-import-progress-row");
  }

  decorateImportPanel();

  function decoratePolicyBackupPanel() {
    if (!stateBackupCard || stateBackupCard.dataset.decorated === "1") return;
    stateBackupCard.dataset.decorated = "1";
    stateBackupCard.className = "ins-row cw-policy-row";
    if (importRow && importRow.parentNode && stateBackupCard.parentNode !== importRow.parentNode) {
      importRow.insertAdjacentElement("afterend", stateBackupCard);
    }

    const details = document.createElement("details");
    details.id = "cw-policy-details";
    details.className = "cw-collapse cw-policy-panel";
    details.style.width = "100%";

    const summary = document.createElement("summary");
    summary.className = "cw-import-summary";
    summary.innerHTML =
      '<span class="cw-import-title">Policy backup</span>' +
      '<span class="cw-import-help material-symbol" title="Exports or imports the local Current State policy JSON used by manual baseline edits and block rules. It does not change provider accounts." aria-label="Policy backup help">help</span>';

    const body = document.createElement("div");
    body.className = "cw-policy-body";

    const copy = document.createElement("div");
    copy.className = "cw-policy-copy";
    copy.textContent = "Export or import the local Current State policy as JSON.";

    const actions = document.createElement("div");
    actions.className = "cw-policy-actions";

    const exportAction = document.createElement("div");
    exportAction.className = "cw-policy-action";
    exportAction.innerHTML = "<span>Export</span>";
    if (stateDownloadBtn) exportAction.append(stateDownloadBtn);

    const importAction = document.createElement("div");
    importAction.className = "cw-policy-action";
    importAction.innerHTML = "<span>Import</span>";
    if (stateUploadBtn) importAction.append(stateUploadBtn);
    if (stateUploadInput) importAction.append(stateUploadInput);

    actions.append(exportAction, importAction);
    body.append(copy, actions);
    details.append(summary, body);
    stateBackupCard.textContent = "";
    stateBackupCard.append(details);
  }

  decoratePolicyBackupPanel();

  function decorateEditorChrome() {
    const typeIcons = {
      movie: "movie",
      show: "tv",
      anime: "theater_comedy",
      season: "layers",
      episode: "play_circle",
      blocked: "block",
    };
    const setButtonIcon = (btn, icon, label) => {
      if (!btn) return;
      btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">${icon}</span><span>${label}</span>`;
      btn.setAttribute("aria-label", label);
    };

    setButtonIcon(addBtn, "add", "Add row");
    setButtonIcon(saveBtn, "check", "Save changes");

    if (typeFilterWrap && typeFilterWrap.dataset.decorated !== "1") {
      typeFilterWrap.dataset.decorated = "1";
      const field = typeFilterWrap.closest(".ins-kv");
      const label = field?.querySelector(".field-label");
      if (field && label) {
        const shell = document.createElement("div");
        shell.className = "cw-type-filter-shell";
        const head = document.createElement("div");
        head.className = "cw-type-filter-head";
        const title = document.createElement("div");
        title.className = "field-label";
        title.textContent = label.textContent || "Types";
        const clear = document.createElement("button");
        clear.type = "button";
        clear.className = "cw-type-filter-clear";
        clear.innerHTML = 'Clear <span class="material-symbol" aria-hidden="true">filter_alt</span>';
        clear.setAttribute("aria-label", "Clear type filters");
        clear.addEventListener("click", () => {
          ["movie", "show", "anime", "season", "episode"].forEach(t => {
            state.typeFilter[t] = true;
          });
          state.blockedOnly = false;
          syncTypeFilterUI();
          state.page = 0;
          persistUIState();
          renderRows();
        });
        head.append(title, clear);
        label.replaceWith(shell);
        shell.append(head, typeFilterWrap);
      }

      typeFilterWrap.querySelectorAll("button").forEach(btn => {
        const type = btn.dataset.type || (btn.id === "cw-blocked-only" ? "blocked" : "");
        const text = (btn.textContent || "").trim();
        btn.innerHTML =
          `<span class="material-symbol cw-type-icon" aria-hidden="true">${typeIcons[type] || "category"}</span>` +
          `<span>${_escapeHtml(text)}</span>`;
      });
    }
  }

  decorateEditorChrome();
  const sortHeaders = Array.from(host.querySelectorAll(".cw-table th[data-sort]"));
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
    const show = !isTrackerSource();
    if (snapLabel) snapLabel.style.display = show ? "" : "none";
    if (snapSel) {
      snapSel.style.display = show ? "" : "none";
      const wrap = snapSel.nextElementSibling && snapSel.nextElementSibling.classList?.contains("cw-icon-select")
        ? snapSel.nextElementSibling
        : null;
      if (wrap) wrap.style.display = show ? "" : "none";
    }
  }

  let statusStickyUntil = 0;

  const SOURCES = ["state", "tracker", "playlist"];

  function isTrackerSource() {
    return state.source === "tracker";
  }

  function isPolicySource() {
    return state.source === "state" || state.source === "tracker";
  }

  function normalizeSource(value) {
    const s = String(value || "").trim();
    if (!SOURCES.includes(s)) return "state";
    return s;
  }

  function ensureTrackerOption() {
    if (!sourceSel) return;
    const existing = sourceSel.querySelector('option[value="tracker"]');
    if (existing) {
      existing.textContent = "Local Tracker";
      return;
    }
    const opt = document.createElement("option");
    opt.value = "tracker";
    opt.textContent = "Local Tracker";
    const playlistOpt = sourceSel.querySelector('option[value="playlist"]');
    if (playlistOpt) sourceSel.insertBefore(opt, playlistOpt);
    else sourceSel.appendChild(opt);
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

  function currentTrackerWorkspace() {
    const id = String(state.workspace || "").trim();
    const list = state.trackerWorkspaces || [];
    return list.find(w => String(w && w.id || "") === id) || list[0] || null;
  }

  function trackerKinds() {
    const ws = currentTrackerWorkspace();
    const feats = (ws && ws.features) || {};
    return ["watchlist", "history", "ratings", "progress"].filter(k => feats[k]);
  }

  function syncHeaderPills(visible, total) {
    const srcMap = { state: "Current state", tracker: "Local tracker", playlist: "Playlist endpoint" };
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

  if (filterInput && state.filter) filterInput.value = state.filter;

  const KIND_LABELS = { watchlist: "Watchlist", history: "History", ratings: "Ratings", progress: "Progress" };

  function syncKindUI() {
    if (!kindSel) return;
    let allowed = ["watchlist", "history", "ratings", "progress"];
    if (isTrackerSource()) {
      const supported = trackerKinds();
      if (supported.length) allowed = supported;
    }
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
    const id = String(state.snapshot || "").trim();
    return (state.playlistEndpoints || []).find(ep => String(ep && ep.id || "") === id) || null;
  }

  function playlistEditable() {
    if (state.source !== "playlist") return true;
    const r = state.playlistResource || {};
    return !!r && !r.smart && !!(r.can_add || r.can_remove || r.can_reorder);
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
    return state.source === "state" && String(state.snapshot || "").trim().toUpperCase() === "ANILIST";
  }

  function syncIdColumnHeaders() {
    const a = $("cw-col-id-a");
    if (!a) return;
    a.textContent = isAnilistMode() ? "MAL" : "TMDB";
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
    if (importProgressWrap) importProgressWrap.style.display = on ? "" : "none";
    if (importProgressText) importProgressText.textContent = message || "";
    const disabled = !!on;
    if (importRunBtn) importRunBtn.disabled = disabled;
    if (importProviderSel) importProviderSel.disabled = disabled;
    if (importModeSel) importModeSel.disabled = disabled;
    if (importWatchlistCb) importWatchlistCb.disabled = disabled || importWatchlistCb.disabled;
    if (importHistoryCb) importHistoryCb.disabled = disabled || importHistoryCb.disabled;
    if (importRatingsCb) importRatingsCb.disabled = disabled || importRatingsCb.disabled;
    if (importProgressCb) importProgressCb.disabled = disabled || importProgressCb.disabled;
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

    if (!norm.some(x => x.id === "default")) norm.unshift({ id: "default", label: "Default" });

    const ids = norm.map(x => x.id);
    let next = String(current || "");
    if (!next || !ids.includes(next)) next = "default";
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
          const label = providerLabel(name, p && p.label ? String(p.label) : name);
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
      syncProviderIconSelect(importProviderSel, true);
    }

    const sel = state.importProvider || (importProviderSel ? importProviderSel.value : "");
    const p = list.find(x => String((x || {}).name || "") === String(sel || ""));
    const feats = (p && p.features) ? p.features : {};

    if (importInstanceSel) {
      const ids = (p && Array.isArray(p.instances)) ? p.instances : ["default"];
      const instObjs = ids.map(x => ({ id: String(x), label: String(x) }));
      const nextInst = renderInstanceOptions(importInstanceSel, instObjs, state.importProviderInstance);
      if (nextInst !== state.importProviderInstance) {
        state.importProviderInstance = nextInst;
        persistUIState();
      }
      const instanceField = importInstanceSel.closest(".cw-import-field");
      if (instanceField) instanceField.style.display = state.importProvider ? "" : "none";
      else importInstanceSel.style.display = state.importProvider ? "" : "none";
      syncProfileIconSelect(importInstanceSel, !!state.importProvider);
    }

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
    setCb(importProgressFeatWrap, importProgressCb, "progress");

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
    if (importProgressCb && importProgressCb.checked && !importProgressCb.disabled) feats.push("progress");
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
      progress: features.includes("progress"),
    };

    try {
      const msg = `Importing ${features.join(", ")} from ${provider}…`;
      setImportBusy(true, msg);
      setTag("warn", "Importing…");
      setStatus(msg);

      const res = await fetchJSON("/api/editor/state/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, provider_instance: state.importProviderInstance || "default", features, mode }),
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
      state.instance = state.importProviderInstance || "default";
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
        workspace: state.workspace,
        instance: state.instance,
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
    const setIconOnly = (btn, icon, label) => {
      btn.classList.add("cw-icon-only");
      btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">${icon}</span>`;
      btn.title = label;
      btn.setAttribute("aria-label", label);
    };
    if (isPolicySource()) {
      setIconOnly(bulkRemoveBtn, "block", "Block selected");
      setIconOnly(bulkRestoreBtn, "undo", "Unblock selected");
    } else {
      setIconOnly(bulkRemoveBtn, "delete", "Delete selected");
      setIconOnly(bulkRestoreBtn, "restore_from_trash", "Restore selected");
    }
    setIconOnly(bulkClearBtn, "close", "Clear selection");
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
        ? isPolicySource()
          ? "Blocked"
          : "Deleted"
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
    state.source = normalizeSource(state.source);
    if (sourceSel) {
      sourceSel.querySelector('option[value="pair"]')?.remove();
      if (!sourceSel.querySelector('option[value="playlist"]')) {
        sourceSel.insertAdjacentHTML("beforeend", '<option value="playlist">Playlist Endpoint</option>');
      }
      ensureTrackerOption();
    }
    const isState = state.source === "state";
    const isTracker = isTrackerSource();
    const isPlaylist = state.source === "playlist";
    const policy = isPolicySource();
    if (sourceSel) sourceSel.value = state.source;
    if (pairLabel) pairLabel.style.display = "none";
    if (pairSel) pairSel.style.display = "none";
    if (snapLabel) snapLabel.textContent = isState ? "Provider" : "Endpoint";
    syncSnapshotControlVisibility();
    if (kindSel) kindSel.disabled = isPlaylist;
    if (instanceLabel) instanceLabel.style.display = (isState || isTracker) ? "" : "none";
    if (instanceSel) instanceSel.style.display = (isState || isTracker) ? "" : "none";
    if (backupCard) backupCard.style.display = "none";
    if (stateBackupCard) stateBackupCard.style.display = policy ? "" : "none";
    if (blockedOnlyBtn) blockedOnlyBtn.style.display = policy ? "" : "none";
    if (trackerNotice) trackerNotice.style.display = isTracker ? "block" : "none";

    const sub = host.querySelector(".cw-sub");
    if (sub) {
      sub.textContent = isTracker
        ? "Edit what CrossWatch sends from its local tracker. Connected provider accounts are not changed."
        : "Edit your current state or playlist endpoints";
    }

    if (isPlaylist) {
      state.kind = "watchlist";
      state.instance = "default";
    }
    syncKindUI();

    if (!policy && state.blockedOnly) {
      state.blockedOnly = false;
      syncTypeFilterUI();
      persistUIState();
    }
    syncStateBulkUI();
    syncImportUI();
    syncTypeFilterUI();
    syncActionButtons();
    syncHeaderPills();
  }

  function showStateHint(mode) {
    if (!stateHint) return;
    if (mode === "state") {
      stateHint.innerHTML =
        "<strong>No state.json found.</strong> Run a CrossWatch sync once to generate it. After that, your manual adds and blocks will show up here.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "playlist") {
      stateHint.innerHTML =
        "<strong>No playlist endpoints found.</strong> Create an endpoint on the Playlists page first. Then select it here to edit its items.";
      stateHint.style.display = "block";
      return;
    }
    if (mode === "tracker") {
      stateHint.innerHTML =
        "<strong>No Local Tracker data found.</strong> Run a sync pair that uses Local Tracker first.";
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



  function formatMs(ms) {
    const n = ms == null ? NaN : Number(ms);
    if (!Number.isFinite(n) || n <= 0) return "";
    const total = Math.floor(n / 1000);
    const pad = x => String(x).padStart(2, "0");
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    if (h > 0) return `${h}:${pad(m)}:${pad(s)}`;
    return `${m}:${pad(s)}`;
  }

  const PROGRESS_PERCENT_KEYS = ["progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"];
  const clampProgressPercent = value => {
    const n = value == null || value === "" ? NaN : Number(value);
    return Number.isFinite(n) ? Math.max(0, Math.min(100, n)) : null;
  };

  function progressPercentValue(raw) {
    if (!raw) return null;
    const pm = Number(raw.progress_ms);
    const dm = Number(raw.duration_ms);
    if (Number.isFinite(pm) && pm > 0 && Number.isFinite(dm) && dm > 0) return clampProgressPercent((pm / dm) * 100);
    for (const key of PROGRESS_PERCENT_KEYS) {
      const n = clampProgressPercent(raw[key]);
      if (n != null) return n;
    }
    return null;
  }

  function formatProgressPercent(value) {
    const n = clampProgressPercent(value);
    if (n == null) return "";
    const rounded = Math.round(n * 10) / 10;
    return `${Number.isInteger(rounded) ? Math.trunc(rounded) : rounded}%`;
  }

  function parseProgressPercent(v) {
    const s = (v == null ? "" : String(v)).trim().replace(/%$/, "").trim();
    return s ? clampProgressPercent(s) : null;
  }

  function parseTimeToMs(v) {
    const s = (v == null ? "" : String(v)).trim();
    if (!s) return null;

    const lower = s.toLowerCase();
    if (lower.endsWith("ms")) {
      const num = parseFloat(lower.slice(0, -2));
      return Number.isFinite(num) ? Math.max(0, Math.floor(num)) : null;
    }

    if (s.includes(":")) {
      const parts = s.split(":").map(p => p.trim()).filter(Boolean);
      if (!parts.length) return null;
      const nums = parts.map(x => parseInt(x, 10));
      if (nums.some(n => !Number.isFinite(n))) return null;

      let sec = 0;
      if (nums.length === 3) sec = nums[0] * 3600 + nums[1] * 60 + nums[2];
      else if (nums.length === 2) sec = nums[0] * 60 + nums[1];
      else sec = nums[0];
      return Math.max(0, sec * 1000);
    }

    const num = parseFloat(s);
    if (!Number.isFinite(num)) return null;
    // Heuristic: large numbers are probably milliseconds.
    if (num >= 100000) return Math.max(0, Math.floor(num));
    return Math.max(0, Math.floor(num * 1000));
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

  function isRowLocked(row) {
    return isPolicySource() && !!row && row._origin === "baseline";
  }

  function isExtraKindEditable() {
    return state.kind === "ratings" || state.kind === "history" || state.kind === "progress";
  }

  function promoteBaselineEdit(row) {
    if (!isPolicySource() || !row || row._origin !== "baseline") return false;
    row._origin = "manual";
    row.deleted = false;
    return true;
  }

  const REPLACEABLE_TYPES = ["movie", "show", "anime", "season", "episode"];

  function rowType(row) {
    return String((row && row.type) || "").toLowerCase();
  }

  function canReplaceRow(row) {
    if (!isPolicySource()) return false;
    return REPLACEABLE_TYPES.includes(rowType(row));
  }

  function usesCoordinateReplacer(row) {
    const t = rowType(row);
    return t === "episode" || t === "season";
  }

  const EPISODE_ID_FIELDS = [
    { key: "tvdb", label: "TVDB episode ID" },
    { key: "tmdb", label: "TMDB episode ID" },
    { key: "simkl", label: "SIMKL episode ID" },
  ];

  const PROVIDER_HISTORY_ID_FIELDS = [
    "_trakt_history_id",
    "history_id",
    "_simkl_history_id",
    "_plex_history_id",
    "watched_id",
    "play_id",
  ];

  function coordinateKeyFor(row, season, episode) {
    const base = String((row && row.key) || "").split("#")[0].trim();
    if (!base) return "";
    if (rowType(row) === "season") return `${base}#season:${season}`;
    const s = String(Math.max(0, season)).padStart(2, "0");
    const e = String(Math.max(0, episode)).padStart(2, "0");
    return `${base}#s${s}e${e}`;
  }

  function carryOverFields(row, drop) {
    const src = (row && row.raw) || {};
    const out = {};
    for (const [k, v] of Object.entries(src)) {
      if (drop.includes(k) || PROVIDER_HISTORY_ID_FIELDS.includes(k)) continue;
      out[k] = JSON.parse(JSON.stringify(v === undefined ? null : v));
    }
    return out;
  }

  function correctedEpisodeItem(row, season, episode, watchedAt, extraIds) {
    const isSeason = rowType(row) === "season";
    const out = carryOverFields(row, ["ids", "season", "episode", "title"]);
    out.type = isSeason ? "season" : "episode";
    out.season = season;
    if (isSeason) {
      out.title = `Season ${season}`;
    } else {
      out.episode = episode;
      out.title = `S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}`;
      out.watched_at = watchedAt || null;
    }
    const ids = {};
    for (const [k, v] of Object.entries(extraIds || {})) {
      const val = String(v || "").trim();
      if (val) ids[k] = val;
    }
    out.ids = ids;
    return out;
  }

  function correctedMetadataItem(row, draft) {
    const out = carryOverFields(row, [
      "ids", "title", "year", "type",
      "season", "episode", "series_title", "series_year", "show_ids",
    ]);
    const picked = (draft && draft.raw) || {};
    out.type = picked.type || draft.type || "movie";
    out.title = picked.title || draft.title || null;
    out.year = picked.year != null ? picked.year : null;
    out.ids = { ...(picked.ids || {}) };
    return out;
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

  function openItemReplacer(row, anchor) {
    if (usesCoordinateReplacer(row)) return openEpisodeReplacer(row, anchor);
    return openMetadataReplacer(row, anchor);
  }

  function openMetadataReplacer(row, anchor) {
    const draft = {
      _rid: -1,
      key: String(row.key || ""),
      type: rowType(row),
      title: String(row.title || ""),
      year: String(row.year || ""),
      imdb: "",
      tmdb: "",
      trakt: "",
      mal: "",
      anilist: "",
      raw: JSON.parse(JSON.stringify(row.raw || {})),
      deleted: false,
      episode: false,
    };
    const stub = () => document.createElement("input");
    const refs = {
      keyIn: stub(),
      titleIn: stub(),
      yearIn: stub(),
      imdbIn: stub(),
      tmdbIn: stub(),
      traktIn: stub(),
      typeBtn: document.createElement("button"),
      onApplied: applied => {
        const key = String(applied.key || "").trim();
        if (!key) {
          setStatusSticky("That result has no usable identifier.", 5000);
          return;
        }
        if (key.toLowerCase() === String(row.key || "").trim().toLowerCase()) {
          setStatusSticky("The corrected item is the same as the current one.", 5000);
          return;
        }
        const corrected = correctedMetadataItem(row, applied);
        const err = commitReplacement(row, corrected, key, "The corrected local item was updated.");
        if (err) setStatusSticky(err, 5000);
      },
    };
    openTitleSearchEditor(draft, anchor, refs);
  }

  function openEpisodeReplacer(row, anchor) {
    const isSeason = rowType(row) === "season";
    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, isSeason ? "Replace season" : "Replace episode");

      const raw = (row && row.raw) || {};
      const curSeason = Number(raw.season || 0);
      const curEpisode = Number(raw.episode || 0);
      const seriesTitle = String(raw.series_title || row.title || "").trim() || "Unknown series";

      const info = document.createElement("div");
      info.className = "cw-search-meta";
      info.textContent = isSeason
        ? `${seriesTitle} - currently season ${curSeason}`
        : `${seriesTitle} - currently S${String(curSeason).padStart(2, "0")}E${String(curEpisode).padStart(2, "0")}`;
      pop.appendChild(info);

      appendPopupTitle(pop, isSeason ? "Corrected season" : "Corrected episode", "10px");

      const grid = document.createElement("div");
      grid.className = "cw-datetime-grid";
      grid.style.gridTemplateColumns = isSeason ? "minmax(0,1fr)" : "minmax(0,1fr) minmax(0,1fr)";

      const seasonIn = document.createElement("input");
      seasonIn.type = "number";
      seasonIn.min = "0";
      seasonIn.placeholder = "Season";
      seasonIn.value = String(curSeason);

      const episodeIn = document.createElement("input");
      episodeIn.type = "number";
      episodeIn.min = "0";
      episodeIn.placeholder = "Episode";
      episodeIn.value = String(curEpisode);

      grid.appendChild(seasonIn);
      if (!isSeason) grid.appendChild(episodeIn);
      pop.appendChild(grid);

      const dateInput = document.createElement("input");
      dateInput.type = "date";
      const timeInput = document.createElement("input");
      timeInput.type = "time";
      timeInput.step = 60;

      if (!isSeason) {
        appendPopupTitle(pop, "Watched at", "10px");
        const whenGrid = document.createElement("div");
        whenGrid.className = "cw-datetime-grid";
        fillDateTimeInputs(raw.watched_at, dateInput, timeInput);
        whenGrid.appendChild(dateInput);
        whenGrid.appendChild(timeInput);
        pop.appendChild(whenGrid);
      }

      const advanced = document.createElement("details");
      advanced.className = "cw-collapse";
      advanced.style.marginTop = "10px";
      const summary = document.createElement("summary");
      summary.style.cursor = "pointer";
      summary.style.fontWeight = "700";
      summary.style.userSelect = "none";
      summary.textContent = "Advanced";
      advanced.appendChild(summary);

      const advGrid = document.createElement("div");
      advGrid.className = "cw-datetime-grid";
      advGrid.style.marginTop = "8px";
      const idInputs = {};
      EPISODE_ID_FIELDS.forEach(field => {
        const input = document.createElement("input");
        input.type = "text";
        input.placeholder = field.label;
        idInputs[field.key] = input;
        advGrid.appendChild(input);
      });
      advanced.appendChild(advGrid);
      pop.appendChild(advanced);

      const status = document.createElement("div");
      status.className = "cw-search-status";
      status.style.marginTop = "8px";
      pop.appendChild(status);

      const apply = () => {
        const season = parseInt(seasonIn.value, 10);
        const episode = isSeason ? 0 : parseInt(episodeIn.value, 10);
        const seasonOk = Number.isFinite(season) && season >= 0;
        const episodeOk = isSeason || (Number.isFinite(episode) && episode > 0);
        if (!seasonOk || !episodeOk) {
          status.textContent = isSeason
            ? "Enter a valid corrected season."
            : "Enter a valid corrected season and episode.";
          return;
        }
        if (season === curSeason && (isSeason || episode === curEpisode)) {
          status.textContent = isSeason
            ? "The corrected season is the same as the current one."
            : "The corrected episode is the same as the current one.";
          return;
        }
        const key = coordinateKeyFor(row, season, episode);
        if (!key) {
          status.textContent = "This row has no usable series identifier.";
          return;
        }

        const watchedAt = dateTimeInputsToIso(dateInput.value, timeInput.value) || raw.watched_at || null;
        const extraIds = {};
        EPISODE_ID_FIELDS.forEach(field => {
          extraIds[field.key] = idInputs[field.key].value;
        });
        const corrected = correctedEpisodeItem(row, season, episode, watchedAt, extraIds);
        const err = commitReplacement(
          row,
          corrected,
          key,
          isSeason ? "The corrected local season was updated." : "The corrected local episode was updated."
        );
        if (err) {
          status.textContent = err;
          return;
        }
        close();
      };

      appendPopupActions(pop, [
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Replace episode", kind: "primary", onClick: apply },
      ]);

      seasonIn.focus();
    });
  }

  function applyManualRow(row, item, key) {
    row.key = key;
    row.raw = item;
    row.type = "episode";
    row.episode = true;
    row.title = String(item.series_title || row.title || "");
    row.year = item.series_year != null ? String(item.series_year) : row.year || "";
    row.imdb = "";
    row.tmdb = item.ids && item.ids.tmdb ? String(item.ids.tmdb) : "";
    row.trakt = "";
    row.deleted = false;
    row._origin = "manual";
    return row;
  }

  function buildManualRow(item, key, replacedKey) {
    const row = applyManualRow(
      { _rid: state.ridSeq++, mal: "", anilist: "" },
      item,
      key
    );
    if (replacedKey) row._replacedKey = replacedKey;
    return row;
  }

  function renderLockedPopup(pop, close) {
    const status = document.createElement("div");
    status.className = "cw-search-status";
    status.textContent = "Baseline title and ID fields are read-only. Use Extra for a local value correction, or block the row to exclude it.";
    pop.appendChild(status);
    appendPopupActions(pop, [{ label: "Close", kind: "primary", onClick: close }]);
  }

  function fillDateTimeInputs(iso, dateInput, timeInput) {
    if (!iso) return;
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return;
    const pad = n => String(n).padStart(2, "0");
    dateInput.value = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    timeInput.value = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function dateTimeInputsToIso(dateValue, timeValue) {
    if (!dateValue) return null;
    const parts = dateValue.split("-");
    const y = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10);
    const dDay = parseInt(parts[2], 10);
    const [hhRaw, mmRaw] = (timeValue || "").split(":");
    const hh = parseInt(hhRaw, 10) || 0;
    const mm = parseInt(mmRaw, 10) || 0;
    return new Date(y, m - 1, dDay, hh, mm, 0).toISOString().replace(/\.\d{3}Z$/, ".000Z");
  }

  function finishExtraChange(row, displayEl, close) {
    const promoted = promoteBaselineEdit(row);
    updateExtraDisplay(row, displayEl);
    markChanged();
    if (promoted) setStatusSticky("A local correction was created for this row.", 4500);
    close();
    if (promoted) renderRows();
  }

  function finishPopupChange(close, rerender = false) {
    markChanged();
    close();
    if (rerender) renderRows();
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
    } else if (state.kind === "progress") {
      icon = "play_circle";
      const p = row.raw && row.raw.progress_ms;
      const d = row.raw && row.raw.duration_ms;
      const percent = progressPercentValue(row.raw);
      const pm = p == null ? NaN : Number(p);
      const dm = d == null ? NaN : Number(d);
      if (!Number.isFinite(pm) || pm <= 0) {
        if (percent != null) label = formatProgressPercent(percent);
        else placeholder = "Set progress";
      }
      else {
        const left = formatMs(pm);
        const right = Number.isFinite(dm) && dm > 0 ? formatMs(dm) : "";
        const pct = percent != null ? ` (${formatProgressPercent(percent)})` : "";
        label = right ? `${left} / ${right}${pct}` : left;
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
    } else if (t === "anime") {
      label = "Anime";
      icon = "auto_awesome";
    } else if (t === "season") {
      label = "Season";
      icon = "layers";
    } else if (t === "episode") {
      label = "Episode";
      icon = "open_in_new";
    }

    el.innerHTML = "";
    ["type-movie", "type-show", "type-anime", "type-season", "type-episode"].forEach(cls => el.classList.remove(cls));
    if (t) el.classList.add(`type-${t}`);
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
    if (state.source !== "playlist") rows.sort((a, b) => (a.title || "").localeCompare(b.title || ""));
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

      if (state.blockedOnly && isPolicySource()) {
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
    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Watched at");

      const grid = document.createElement("div");
      grid.className = "cw-datetime-grid";

      const dateInput = document.createElement("input");
      dateInput.type = "date";

      const timeInput = document.createElement("input");
      timeInput.type = "time";
      timeInput.step = 60;

      fillDateTimeInputs(row.raw && row.raw.watched_at, dateInput, timeInput);

      grid.appendChild(dateInput);
      grid.appendChild(timeInput);
      pop.appendChild(grid);

      appendPopupActions(pop, [
        { label: "Clear", kind: "ghost", onClick: () => { row.raw.watched_at = null; finishExtraChange(row, displayEl, close); } },
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Save", kind: "primary", onClick: () => { row.raw.watched_at = dateTimeInputsToIso(dateInput.value, timeInput.value); finishExtraChange(row, displayEl, close); } },
      ]);

      dateInput.focus();
    });
  }


  function openProgressEditor(row, anchor, displayEl) {
    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Progress");

      const makeInput = (type, props = {}) => Object.assign(document.createElement("input"), { type, ...props });
      const makeField = (label, child, extraClass = "") => {
        const field = document.createElement("label");
        field.className = `cw-progress-edit-field ${extraClass}`.trim();
        const labelEl = Object.assign(document.createElement("span"), { className: "cw-progress-edit-label", textContent: label });
        field.appendChild(labelEl);
        field.appendChild(child);
        return field;
      };

      const grid = document.createElement("div");
      grid.className = "cw-progress-edit-grid";

      const curPos = row.raw && row.raw.progress_ms;
      const curDur = row.raw && row.raw.duration_ms;
      const curPercent = progressPercentValue(row.raw);
      const posInput = makeInput("text", { placeholder: "mm:ss", value: curPos != null ? formatMs(curPos) : "" });
      const durInput = makeInput("text", { placeholder: "mm:ss", value: curDur != null ? formatMs(curDur) : "" });

      grid.appendChild(makeField("Position", posInput));
      grid.appendChild(makeField("Duration", durInput));
      pop.appendChild(grid);

      const percentInput = makeInput("number", {
        min: "0",
        max: "100",
        step: "0.1",
        placeholder: "0-100",
        value: curPercent != null ? String(Math.round(curPercent * 10) / 10) : "",
      });
      const percentWrap = document.createElement("div");
      percentWrap.className = "cw-progress-percent-wrap";
      percentWrap.append(percentInput, Object.assign(document.createElement("span"), { className: "cw-progress-percent-suffix", textContent: "%" }));
      pop.appendChild(makeField("Percent", percentWrap, "cw-progress-percent-field"));

      appendPopupTitle(pop, "Updated at", "10px");

      const whenGrid = document.createElement("div");
      whenGrid.className = "cw-datetime-grid";

      const dateInput = makeInput("date");
      const timeInput = makeInput("time", { step: 60 });

      fillDateTimeInputs(row.raw && row.raw.progress_at, dateInput, timeInput);

      whenGrid.appendChild(dateInput);
      whenGrid.appendChild(timeInput);
      pop.appendChild(whenGrid);

      const saveProgress = () => {
        const posMs = parseTimeToMs(posInput.value);
        const durMs = parseTimeToMs(durInput.value);
        const percent = parseProgressPercent(percentInput.value);

        row.raw.progress_ms = posMs != null && posMs > 0 ? posMs : null;
        row.raw.duration_ms = durMs != null && durMs > 0 ? durMs : null;
        row.raw.progress_percent = row.raw.progress_ms != null && row.raw.duration_ms != null
          ? Math.round((row.raw.progress_ms / row.raw.duration_ms) * 1000) / 10
          : percent;
        row.raw.progress_at = dateTimeInputsToIso(dateInput.value, timeInput.value);
        if (!row.raw.progress_at && (row.raw.progress_ms != null || row.raw.progress_percent != null)) row.raw.progress_at = new Date().toISOString().replace(/\.\d{3}Z$/, ".000Z");
        finishExtraChange(row, displayEl, close);
      };

      appendPopupActions(pop, [
        {
          label: "Clear",
          kind: "ghost",
          onClick: () => {
            for (const key of ["progress_ms", "duration_ms", "progress_percent", "progress_at"]) row.raw[key] = null;
            finishExtraChange(row, displayEl, close);
          }
        },
        { label: "Close", kind: "ghost", onClick: close },
        { label: "Save", kind: "primary", onClick: saveProgress },
      ]);

      posInput.focus();
    });
  }

  function openRatingEditor(row, anchor, displayEl) {
    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Rating");

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
          finishExtraChange(row, displayEl, close);
        };
        grid.appendChild(pill);
      }

      pop.appendChild(grid);
      appendPopupActions(pop, [
        { label: "Clear", kind: "ghost", onClick: () => { row.raw.rating = null; finishExtraChange(row, displayEl, close); } },
        { label: "Close", kind: "ghost", onClick: close },
      ]);
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
      qInput.id = "cw_meta_search_title";
      qInput.name = qInput.id;
      qInput.placeholder = "Title...";
      qInput.value = row.title || "";
      bar.appendChild(qInput);

      const yearInput = document.createElement("input");
      yearInput.type = "number";
      yearInput.id = "cw_meta_search_year";
      yearInput.name = yearInput.id;
      yearInput.placeholder = "Year";
      if (row.year) yearInput.value = row.year;
      bar.appendChild(yearInput);

      const typeSelect = document.createElement("select");
      typeSelect.id = "cw_meta_search_type";
      typeSelect.name = typeSelect.id;
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

        status.textContent = "Searching...";
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
              img.src = `/art/tmdb/${item.type === "show" ? "tv" : "movie"}/${encodeURIComponent(String(item.tmdb))}?size=w92`;
              img.alt = "";
              img.onerror = () => {
                img.remove();
                const ph = document.createElement("div");
                ph.className = "cw-search-poster-placeholder";
                ph.textContent = item.type === "show" ? "TV" : "MOV";
                posterWrap.appendChild(ph);
              };
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
            meta.textContent = bits.join(" - ");
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
              refs.titleIn.value = formatEpisodeVisualTitle(row) || newTitle;

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
                const prevKey = (row.key || "").trim();
                if (!prevKey || /^(tmdb|imdb|trakt|tvdb|slug):/i.test(prevKey)) {
                  row.key = `tmdb:${tmdbStr}`;
                  if (refs.keyIn) refs.keyIn.value = row.key;
                }
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
                    }
                    if (ids.tmdb) {
                      const tVal = String(ids.tmdb);
                      row.tmdb = tVal;
                      row.raw.ids.tmdb = ids.tmdb;
                      if (refs.tmdbIn) refs.tmdbIn.value = tVal;
                      const prevKey = (row.key || "").trim();
                      if (!prevKey || /^(tmdb|imdb|trakt|tvdb|slug):/i.test(prevKey)) {
                        row.key = `tmdb:${tVal}`;
                        if (refs.keyIn) refs.keyIn.value = row.key;
                      }
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

              if (typeof refs.onApplied === "function") {
                close();
                refs.onApplied(row);
                return;
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
    const locked = isRowLocked(row);

    openPopup(anchor, (pop, close) => {
      appendPopupTitle(pop, "Type");
      if (locked) return renderLockedPopup(pop, close);

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
          finishPopupChange(close, true);
        };
        grid.appendChild(pill);
      });

      pop.appendChild(grid);
      appendPopupActions(pop, [
        {
          label: "Clear",
          kind: "ghost",
          onClick: () => {
            row.type = "";
            row.raw.type = null;
            row.episode = false;
            finishPopupChange(close, true);
          }
        },
        { label: "Close", kind: "ghost", onClick: close },
      ]);
    });
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
      setStatusSticky("Could not open raw fields.", 3500);
    }
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
    syncHeaderPills(totalFiltered, totalAll);

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

    const actionHead = host.querySelector(".cw-action-head");
    const wideActions = isPolicySource();
    if (actionHead) {
      actionHead.classList.toggle("cw-action-wide", wideActions);
      actionHead.style.width = wideActions ? "132px" : "46px";
    }

    if (!totalFiltered) {
      if (empty) empty.style.display = "block";
      empty?.closest(".cw-main")?.classList.add("cw-main-empty");
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
    empty?.closest(".cw-main")?.classList.remove("cw-main-empty");

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
      const locked = isRowLocked(row);
      const fieldName = suffix => `cw-row-${row._rid || "new"}-${suffix}`;
      if (row.episode) tr.classList.add("cw-row-episode");
      if (row.deleted) tr.classList.add("cw-row-deleted");

      const cell = inner => {
        const td = document.createElement("td");
        td.appendChild(inner);
        return td;
      };

      const selCb = document.createElement("input");
      selCb.type = "checkbox";
      selCb.name = fieldName("selected");
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

      const blockMode = isPolicySource();
      const baselineRow = blockMode && row._origin === "baseline";
      const delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "cw-btn cw-btn-del danger";
      delBtn.innerHTML = `<span class="material-symbol">${blockMode ? "block" : "delete"}</span>`;
      delBtn.title = blockMode
        ? (baselineRow
            ? (row.deleted ? "Restore for future syncs" : "Block from future syncs")
            : (row.deleted ? "Restore row" : "Remove manual correction"))
        : (row.deleted ? "Restore row" : "Delete row");
      delBtn.onclick = () => {
        row.deleted = !row.deleted;
        markChanged();
        renderRows();
      };
      const delTd = cell(delBtn);
      delTd.className = "cw-action-cell";
      if (wideActions) delTd.classList.add("cw-action-wide");
      if (canReplaceRow(row)) {
        const t = rowType(row);
        const repBtn = document.createElement("button");
        repBtn.type = "button";
        repBtn.className = "cw-btn cw-btn-del";
        repBtn.innerHTML = '<span class="material-symbol">published_with_changes</span>';
        repBtn.title = t === "episode" ? "Replace episode" : t === "season" ? "Replace season" : "Replace item";
        repBtn.style.marginLeft = "4px";
        repBtn.onclick = () => openItemReplacer(row, repBtn);
        delTd.appendChild(repBtn);
      }
      if (isPolicySource()) {
        const rawBtn = document.createElement("button");
        rawBtn.type = "button";
        rawBtn.className = "cw-btn cw-btn-del";
        rawBtn.innerHTML = '<span class="material-symbol">data_object</span>';
        rawBtn.title = "Advanced fields";
        rawBtn.setAttribute("aria-label", "Advanced fields");
        rawBtn.style.marginLeft = "4px";
        rawBtn.onclick = () => openRawFieldsModal(row);
        delTd.appendChild(rawBtn);
      }
      tr.appendChild(delTd);

      const keyIn = document.createElement("input");
      keyIn.name = fieldName("key");
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
      typeBtn.className = "cw-extra-display cw-type-display";
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
      titleIn.name = fieldName("title");
      titleIn.value = formatEpisodeVisualTitle(row) || row.title || "";
      titleIn.disabled = locked;
      titleIn.onfocus = () => {
        const visual = formatEpisodeVisualTitle(row);
        if (visual) titleIn.value = row.title || "";
      };
      titleIn.oninput = e => {
        row.title = e.target.value;
        row.raw.title = e.target.value || null;
        markChanged();
      };
      titleIn.onblur = () => {
        const visual = formatEpisodeVisualTitle(row);
        if (visual) titleIn.value = visual;
      };
      titleRow.appendChild(titleIn);

      const yearIn = document.createElement("input");
      yearIn.name = fieldName("year");
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
      imdbIn.name = fieldName("imdb");
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
      idAIn.name = fieldName(anilistMode ? "mal" : "tmdb");
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
      idBIn.name = fieldName(anilistMode ? "anilist" : "trakt");
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
      const searchUsesCorrection = locked && canReplaceRow(row);
      searchBtn.title = searchUsesCorrection
        ? (usesCoordinateReplacer(row) ? "Replace episode" : "Search and add correction")
        : "Search and fill IDs";
      searchBtn.disabled = locked && !searchUsesCorrection;
      if (searchBtn.disabled) {
        searchBtn.style.opacity = "0.6";
        searchBtn.style.cursor = "not-allowed";
      }
      searchBtn.onclick = () => {
        if (searchBtn.disabled) return;
        if (searchUsesCorrection) {
          openItemReplacer(row, searchBtn);
          return;
        }
        openTitleSearchEditor(row, searchBtn, {
          keyIn,
          titleIn,
          yearIn,
          imdbIn,
          tmdbIn: anilistMode ? null : idAIn,
          traktIn: null,
          typeBtn,
        });
      };
      titleRow.appendChild(searchBtn);

      const subType = (((row.raw && row.raw.type) || row.type || "") + "").toLowerCase();
      if (subType === "season" && row.raw && row.raw.series_title) {
        const sub = document.createElement("div");
        sub.className = "cw-title-sub";
        let label = row.raw.series_title;
        const code = subType === "episode" ? formatSxxEyy(row.raw.season, row.raw.episode) : formatSxxEyy(row.raw.season, null);
        if (code) label += " - " + code;
        sub.textContent = label;
        titleCell.appendChild(sub);
      }
      if (isTrackerSource() && row._origin !== "baseline") {
        const origin = document.createElement("div");
        origin.className = "cw-title-sub";
        origin.textContent = "Manual correction";
        titleCell.appendChild(origin);
      }
      tr.appendChild(cell(titleCell));

      const yearTd = cell(yearIn);
      yearTd.className = "cw-col-year";
      tr.appendChild(yearTd);
      tr.appendChild(cell(idAIn));

      const extraBtn = document.createElement("button");
      extraBtn.type = "button";
      extraBtn.className = "cw-extra-display";
      updateExtraDisplay(row, extraBtn);

      const extraEditable = isExtraKindEditable();
      if (!extraEditable) {
        extraBtn.disabled = true;
        extraBtn.style.opacity = "0.6";
        extraBtn.style.cursor = "default";
      } else if (state.kind === "ratings") {
        extraBtn.onclick = () => openRatingEditor(row, extraBtn, extraBtn);
      } else if (state.kind === "history") {
        extraBtn.onclick = () => openHistoryEditor(row, extraBtn, extraBtn);
      } else if (state.kind === "progress") {
        extraBtn.onclick = () => openProgressEditor(row, extraBtn, extraBtn);
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

  function playlistEndpointLabel(ep) {
    if (!ep) return "Endpoint";
    const name = String(ep.name || ep.id || "Endpoint");
    const provider = String(ep.provider_label || ep.provider || "").trim();
    const playlist = String(ep.playlist_name || ep.playlist_id || "").trim();
    const type = String(ep.playlist_type || ep.resource_kind || ep.kind || "").trim();
    const parts = [name];
    if (provider) parts.push(provider);
    if (playlist && playlist !== name) parts.push(playlist);
    if (type) parts.push(type);
    return parts.join(" - ");
  }

  function rebuildSnapshots() {
    if (!snapSel) return;
    const isState = state.source === "state";
    const isTracker = isTrackerSource();
    const isPlaylist = state.source === "playlist";
    if (snapLabel) snapLabel.textContent = isState ? "Provider" : "Endpoint";
    syncSnapshotControlVisibility();
    if (instanceLabel) instanceLabel.style.display = (isState || isTracker) ? "" : "none";
    if (instanceSel) instanceSel.style.display = (isState || isTracker) ? "" : "none";
    syncProfileIconSelect(instanceSel, isState || isTracker);

    if (isTracker) {
      const list = Array.isArray(state.trackerWorkspaces) ? state.trackerWorkspaces : [];
      const options = list
        .map(w => `<option value="${_escapeHtml(w && w.id)}">${_escapeHtml((w && w.label) || "Local tracker")}</option>`)
        .join("");
      snapSel.innerHTML = options || `<option value="">No workspaces</option>`;
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.workspace) ? state.workspace : opts[0] || "";
      if (next !== state.workspace) state.workspace = next;
      snapSel.value = state.workspace || "";
      syncProviderIconSelect(snapSel, false);
      syncSnapshotControlVisibility();
      return;
    }

    if (isPlaylist) {
      const list = Array.isArray(state.playlistEndpoints) ? state.playlistEndpoints : [];
      const options = list
        .map(ep => `<option value="${_escapeHtml(ep && ep.id)}">${_escapeHtml(playlistEndpointLabel(ep))}</option>`)
        .join("");
      snapSel.innerHTML = options || `<option value="">No endpoints</option>`;
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.snapshot) ? state.snapshot : opts[0] || "";
      if (next !== state.snapshot) state.snapshot = next;
      snapSel.value = state.snapshot || "";
      syncProviderIconSelect(snapSel, false);
      syncSnapshotControlVisibility();
      return;
    }

    if (isState) {
      const list = Array.isArray(state.snapshots) ? state.snapshots : [];
      const options = list.map(p => `<option value="${p}">${providerLabel(p, p)}</option>`).join("");
      snapSel.innerHTML = options;
      const opts = Array.from(snapSel.options).map(o => o.value);
      const next = opts.includes(state.snapshot) ? state.snapshot : opts[0] || "";
      if (next !== state.snapshot) state.snapshot = next;
      snapSel.value = state.snapshot || "";
      syncProviderIconSelect(snapSel, isState);
      syncSnapshotControlVisibility();
      return;
    }
  }

const on = (el, ev, fn) => el && el.addEventListener(ev, fn);

async function fetchJSON(url, opts) {
  if (window.cwIsAuthSetupPending?.() === true) throw new Error("auth setup pending");
  const res = await fetch(url, Object.assign({ cache: "no-store" }, opts || {}));
  if (!res.ok) {
    let detail = "";
    try {
      const data = await res.json();
      detail = data && (data.detail || data.error || data.message) ? String(data.detail || data.error || data.message) : "";
    } catch (_) {}
    throw new Error(detail || `Request failed: ${res.status}`);
  }
  return await res.json();
}

async function fetchBlob(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Download failed: ${res.status}`);
  return await res.blob();
}

function saveBlob(blob, filename) {
  const href = URL.createObjectURL(blob);
  const a = Object.assign(document.createElement("a"), { href, download: filename });
  document.body.appendChild(a);
  a.click();
  setTimeout(() => {
    URL.revokeObjectURL(href);
    a.remove();
  }, 0);
}

async function downloadFile(url, filename, toast) {
  try {
    setTag("warn", "Preparing download…");
    saveBlob(await fetchBlob(url), filename);
    setTag("loaded", "Ready");
    if (toast && window.cxToast) window.cxToast(toast);
  } catch (e) {
    console.error(e);
    setTag("error", "Download failed");
    setStatus(String(e));
  }
}

async function uploadJSON(url, file) {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(url, { method: "POST", body: fd });
  if (!res.ok) {
    let msg = `Import failed: ${res.status}`;
    try {
      const err = await res.json();
      if (err && err.detail) msg += ` – ${err.detail}`;
    } catch (_) {}
    throw new Error(msg);
  }
  return await res.json();
}

const listParts = (data, defs) => defs.flatMap(([k, label]) => data && data[k] != null ? [`${data[k]} ${label}${data[k] === 1 ? "" : "s"}`] : []);

function bindFileImport(btn, input, url, done) {
  if (!btn || !input) return;
  on(btn, "click", () => input.click());
  on(input, "change", async () => {
    const file = input.files && input.files[0];
    if (!file) return;
    try {
      setTag("warn", "Importing…");
      setStatus("");
      await done(await uploadJSON(url, file));
    } catch (e) {
      console.error(e);
      setTag("error", "Import failed");
      setStatus(String(e));
    } finally {
      try { input.value = ""; } catch (_) {}
    }
  });
}

  async function loadTrackerWorkspaces() {
    try {
      if (instanceSel) {
        const nextInst = await loadInstanceOptions("CROSSWATCH", instanceSel, state.instance || "default");
        if (nextInst !== state.instance) {
          state.instance = nextInst;
          persistUIState();
        }
      }
      const params = new URLSearchParams({ provider_instance: state.instance || "default" });
      const data = await fetchJSON(`/api/editor/tracker/workspaces?${params.toString()}`);
      const list = Array.isArray(data && data.workspaces) ? data.workspaces : [];
      state.trackerWorkspaces = list;
      state.trackerAvailable = list.length > 0;
      if (state.workspace && !list.some(w => String(w && w.id || "") === String(state.workspace))) {
        state.workspace = "";
        persistUIState();
      }
    } catch (_) {
      state.trackerWorkspaces = [];
      state.trackerAvailable = false;
    }
    ensureTrackerOption();
    return state.trackerWorkspaces;
  }

  async function loadSnapshots() {
    try {
      if (isTrackerSource()) {
        await loadTrackerWorkspaces();
        rebuildSnapshots();
        syncKindUI();
        if (!state.trackerWorkspaces.length) showStateHint("tracker");
        else showStateHint(null);
        return;
      }
      if (state.source === "playlist") {
        const data = await fetchJSON("/api/editor/playlists/endpoints");
        state.playlistEndpoints = Array.isArray(data && data.endpoints) ? data.endpoints : [];
        state.snapshots = state.playlistEndpoints;
        rebuildSnapshots();
        if (!state.playlistEndpoints.length) showStateHint("playlist");
        else showStateHint(null);
        return;
      }
      if (state.source === "state") {
        const data = await fetchJSON(`/api/editor/state/providers`);
        state.snapshots = Array.isArray(data.providers) ? data.providers : [];
        rebuildSnapshots();

        const prov = state.snapshot || (snapSel ? (snapSel.value || "") : "");
        if (prov) {
          const nextInst = await loadInstanceOptions(prov, instanceSel, state.instance);
          if (prov !== state.snapshot || nextInst !== state.instance) {
            state.snapshot = prov;
            state.instance = nextInst;
            persistUIState();
          }
        } else {
          const nextInst = renderInstanceOptions(instanceSel, [{ id: "default", label: "Default" }], "default");
          if (state.instance !== nextInst) {
            state.instance = nextInst;
            persistUIState();
          }
        }

        if (!state.snapshots.length) showStateHint("state");
        else showStateHint(null);
        return;
      }
      state.source = "state";
      rebuildSnapshots();
      return;
    } catch (e) {
      console.error(e);
    }
  }

  async function settleStateView(maxAttempts = 3, delayMs = 300) {
    if (state.source !== "state") return;
    for (let i = 0; i < maxAttempts; i += 1) {
      const missingProvider = !String(state.snapshot || "").trim();
      const hasRows = Array.isArray(state.rows) && state.rows.length > 0;
      const hasSnapshots = Array.isArray(state.snapshots) && state.snapshots.length > 0;
      if (!missingProvider && (hasRows || !hasSnapshots)) return;
      await new Promise(resolve => setTimeout(resolve, delayMs));
      await loadSnapshots();
      await loadState();
    }
  }

  async function loadState() {
    if (state.source === "playlist") {
      const endpointId = String(state.snapshot || "").trim();
      if (!endpointId) {
        state.items = {};
        state.rows = [];
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.hasChanges = false;
        state.page = 0;
        state.playlistResource = null;
        state.playlistWarnings = [];
        state.playlistOriginalKeys = [];
        renderRows();
        showStateHint("playlist");
        setTag("loaded", "No endpoint");
        setStatus("");
        syncActionButtons();
        return;
      }
    }
    if (isTrackerSource() && !String(state.workspace || "").trim()) {
      state.baselineItems = {};
      state.manualAdds = {};
      state.manualBlocks = [];
      state.items = {};
      state.rows = [];
      state.selected = new Set();
      state.pageRids = [];
      state.ridSeq = 1;
      state.hasChanges = false;
      state.page = 0;
      renderRows();
      showStateHint("tracker");
      setTag("loaded", "No workspace");
      setStatus("");
      syncActionButtons();
      return;
    }
    state.source = normalizeSource(state.source);
    state.loading = true;
    setTag("warn", "Loading");
    try {
      const params = new URLSearchParams({ kind: state.kind, source: state.source });
      if (state.source === "state" && state.snapshot) {
        params.set("provider", state.snapshot);
        params.set("provider_instance", state.instance || "default");
      }
      if (isTrackerSource()) {
        params.set("workspace", state.workspace);
        params.set("provider_instance", state.instance || "default");
      }
      if (state.source === "playlist" && state.snapshot) params.set("endpoint", state.snapshot);

      const data = await fetchJSON(`/api/editor?${params.toString()}`);
      if (data && data.ok === false) throw new Error(data.error || data.detail || "Load failed");

      if (state.source === "playlist") {
        state.playlistResource = data.resource || null;
        state.playlistWarnings = Array.isArray(data.resource && data.resource.warnings) ? data.resource.warnings.map(String) : [];
        state.playlistOriginalKeys = Array.isArray(data.original_keys) ? data.original_keys.map(String) : [];
        state.items = data.items || {};
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.rows = buildRows(state.items);
      } else if (isPolicySource()) {
        if (state.source === "state" && data && typeof data.provider === "string" && data.provider.trim()) {
          state.snapshot = data.provider.trim();
          if (snapSel) {
            snapSel.value = state.snapshot;
            syncProviderIconSelect(snapSel, true);
          }
        }
        if (isTrackerSource() && data && typeof data.workspace === "string" && data.workspace.trim()) {
          state.workspace = data.workspace.trim();
          if (snapSel) snapSel.value = state.workspace;
        }
        state.baselineItems = data.items || {};
        state.manualAdds = data.manual_adds || {};
        state.manualBlocks = Array.isArray(data.manual_blocks) ? data.manual_blocks : [];

        if (isPolicySource() && data && typeof data.provider_instance === "string") {
          state.instance = data.provider_instance;
          if (instanceSel) {
            instanceSel.value = state.instance;
            syncProfileIconSelect(instanceSel, true);
          }
        }

        const merged = Object.assign({}, state.baselineItems || {});
        const manualKeys = new Set();
        for (const [k, v] of Object.entries(state.manualAdds || {})) {
          const key = String(k || "").trim();
          if (!key) continue;
          const existingKey = Object.keys(merged).find(x => String(x || "").toLowerCase() === key.toLowerCase());
          const finalKey = existingKey || key;
          merged[finalKey] = v;
          manualKeys.add(finalKey.toLowerCase());
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
          const rowKey = String(row.key || "").toLowerCase();
          row._origin = manualKeys.has(rowKey) ? "manual" : baselineKeys.has(row.key) ? "baseline" : "manual";
          if (row._origin === "baseline") row.deleted = blocked.has(row.key);
        }
      }

      state.hasChanges = false;
      state.page = 0;
      renderRows();

      if (isPolicySource()) {
        const hasBaseline = state.baselineItems && Object.keys(state.baselineItems).length > 0;
        const hasManual = state.manualAdds && Object.keys(state.manualAdds).length > 0;
        const hasBlocks = Array.isArray(state.manualBlocks) && state.manualBlocks.length > 0;
        const emptyMode = isTrackerSource() ? "tracker" : "state";
        showStateHint(hasBaseline || hasManual || hasBlocks ? null : emptyMode);
      } else if (state.source === "playlist") {
        showStateHint(state.snapshot ? null : "playlist");
      }

      setTag("loaded", "Ready");
      if (state.source === "playlist" && state.playlistWarnings.length) {
        setStatus(state.playlistWarnings[0]);
      }
    } catch (e) {
      console.error(e);
      const msg = String(e || "");

      if (
        isTrackerSource() &&
        (msg.includes("404") || /local tracker/i.test(msg) || /workspace/i.test(msg))
      ) {
        showStateHint("tracker");
        state.baselineItems = {};
        state.manualAdds = {};
        state.manualBlocks = [];
        state.items = {};
        state.rows = [];
        state.selected = new Set();
        state.pageRids = [];
        state.ridSeq = 1;
        state.workspace = "";
        renderRows();
        setTag("warn", "No tracker data");
        setStatus("");
      } else if (
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
      syncActionButtons();
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
        `Cannot save: ${missing.length} row${missing.length === 1 ? "" : "s"} have data but no Key. Fill the Key or remove the row.`
      );
      if (window.cxToast) window.cxToast("Fill Key for all rows with data before saving");
      return;
    }

    state.saving = true;
    setTag("warn", "Saving");
    syncActionButtons();

    try {
      const items = {};
      const blocks = [];
      const seenBlocks = new Set();

      for (const row of state.rows) {
        if (row.deleted) {
          if (isPolicySource() && row._origin === "baseline") {
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

        if (isPolicySource() && row._origin === "baseline") continue;

        if (isPolicySource() && row._replacedKey) {
          const rk = String(row._replacedKey).trim();
          const rkl = rk.toLowerCase();
          if (rk && !seenBlocks.has(rkl)) {
            seenBlocks.add(rkl);
            blocks.push(rk);
          }
        }

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
      if (state.source === "state") {
        payload.provider = state.snapshot;
        payload.provider_instance = state.instance || "default";
        payload.blocks = blocks;
      }
      if (isTrackerSource()) {
        payload.workspace = state.workspace;
        payload.provider_instance = state.instance || "default";
        payload.blocks = blocks;
      }
      if (state.source === "playlist") {
        payload.endpoint = state.snapshot;
        const currentKeys = new Set((state.playlistOriginalKeys || []).map(String));
        const nextKeys = new Set(Object.keys(items));
        const removals = Array.from(currentKeys).filter(k => !nextKeys.has(k)).length;
        if (removals && state.playlistWarnings.length) {
          const text = state.playlistWarnings.join("\n");
          if (!window.confirm(`${text}\n\nRemove ${removals} item${removals === 1 ? "" : "s"} from this playlist endpoint?`)) {
            state.saving = false;
            setTag("warn", "Unsaved changes");
            setStatus("Save cancelled");
            syncActionButtons();
            return;
          }
        }
      }

      const res = await fetchJSON("/api/editor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      state.hasChanges = false;
      setTag("warn", "Saved");
      if (state.source === "playlist") {
        const added = Number(res.added || 0);
        const removed = Number(res.removed || 0);
        const reordered = Number(res.reordered || 0);
        const unresolved = Number(res.unresolved_count || 0);
        const parts = [`+${added}`, `-${removed}`];
        if (reordered) parts.push(`${reordered} reordered`);
        if (unresolved) parts.push(`${unresolved} unresolved`);
        setStatus(`Applied playlist changes: ${parts.join(", ")}`);
        await loadState();
      } else if (isTrackerSource()) {
        setStatus(`Saved ${res.count || Object.keys(items).length} corrections`);
        await loadState();
      } else {
        setStatus(`Saved ${res.count || Object.keys(items).length} items`);
        await loadSnapshots();
      }
    } catch (e) {
      console.error(e);
      setTag("error", "Save failed");
      setStatus(String(e));
    } finally {
      state.saving = false;
      syncActionButtons();
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
      _origin: isPolicySource() ? "manual" : "playlist",
    });
    state.page = 0;
    markChanged();
    renderRows();
  }

  on(prevBtn, "click", () => {
    if (state.page <= 0) return;
    state.page -= 1;
    renderRows();
  });

  on(nextBtn, "click", () => {
    const pageCount = Math.max(1, Math.ceil(applyFilter(state.rows).length / PAGE_SIZE));
    if (state.page >= pageCount - 1) return;
    state.page += 1;
    renderRows();
  });

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

  if (sourceSel) {
    sourceSel.addEventListener("change", async () => {
      state.source = normalizeSource(sourceSel.value);
      state.snapshot = "";
      state.page = 0;
      if (state.source === "playlist") {
        state.kind = "watchlist";
        state.instance = "default";
      }
      if (isTrackerSource()) {
        state.workspace = "";
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
      if (state.source !== "state" && !isTrackerSource()) state.snapshot = "";
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
      if (isTrackerSource()) {
        state.workspace = snapSel.value || "";
        state.page = 0;
        clearSelection();
        syncKindUI();
        syncTypeFilterUI();
        syncStateBulkUI();
        persistUIState();
        await loadState();
        return;
      }
      state.snapshot = snapSel.value || "";
      if (state.source === "state") syncProviderIconSelect(snapSel, true);
      if (state.source === "state") {
        state.instance = await loadInstanceOptions(state.snapshot, instanceSel, state.instance);
        persistUIState();
      }
      state.page = 0;
      if (state.source !== "state") persistUIState();
      await loadState();
    });
  }

  
  if (instanceSel) {
    instanceSel.addEventListener("change", async () => {
      state.instance = instanceSel.value || "default";
      state.page = 0;
      persistUIState();
      if (isTrackerSource()) {
        state.workspace = "";
        clearSelection();
        await loadSnapshots();
      }
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
      clearSelection();
      persistUIState();
      renderRows();
    });
  }

  function editorIsVisible() {
    return !!host && !!document.getElementById("page-editor") && host.getClientRects().length > 0;
  }

  function syncSelectedScopeFromControls() {
    if (isTrackerSource()) {
      state.workspace = (snapSel && snapSel.value) ? snapSel.value : state.workspace || "";
      state.snapshot = "";
      state.instance = (instanceSel && instanceSel.value) ? instanceSel.value : state.instance || "default";
      return;
    }
    if (state.source === "state") {
      state.snapshot = (snapSel && snapSel.value) ? snapSel.value : state.snapshot || "";
      state.instance = (instanceSel && instanceSel.value) ? instanceSel.value : state.instance || "default";
      return;
    }
    if (state.source === "playlist") {
      state.snapshot = (snapSel && snapSel.value) ? snapSel.value : state.snapshot || "";
      state.instance = "default";
    }
  }

  async function refreshEditor({ force = false } = {}) {
    if (!force && (!editorIsVisible() || state.hasChanges || state.loading || state.saving)) return;
    syncSelectedScopeFromControls();
    state.page = 0;
    await loadSnapshots();
    await loadState();
    await settleStateView();
    state.lastSyncAt = Date.now();
    syncHeaderPills();
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

  let deferredRefreshTimer = null;
  function queueEditorRefresh(delay = 250) {
    if (deferredRefreshTimer) window.clearTimeout(deferredRefreshTimer);
    deferredRefreshTimer = window.setTimeout(() => {
      deferredRefreshTimer = null;
      refreshEditor().catch(e => console.warn("[editor] refresh failed", e));
    }, delay);
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
    await loadTrackerWorkspaces();
    const wanted = state.source;
    state.source = normalizeSource(wanted);
    if (state.source !== wanted) {
      state.snapshot = "";
      state.workspace = "";
      state.instance = "default";
      persistUIState();
    }
    syncSourceUI();
    await loadImportProviders();
    setTag(
      "warn",
      isTrackerSource()
        ? "Loading Local Tracker…"
        : state.source === "state"
          ? "Loading current state…"
          : "Loading playlist endpoint…"
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
