/* assets/js/watchlist.js */
/* Watchlist page shell and components */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  const PAGE_SIZE_OPTIONS = [50, 100, 150, 200];
  const DEFAULT_PAGE_SIZE = 50;
  const COLUMN_KEYS = ["poster", "title", "year", "rel", "genre", "type", "tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist", "mal", "sync", "added", "key"];
  const COLUMN_LAYOUT_VERSION = 1;
  const DEFAULT_COLUMN_ORDER = COLUMN_KEYS.slice();
  const DEFAULT_COLUMN_VISIBILITY = { poster:true, title:true, year:false, rel:true, genre:true, type:true, tmdb:false, imdb:false, tvdb:false, trakt:false, simkl:false, anilist:false, mal:false, sync:true, added:false, key:false };
  const DEFAULT_COLUMN_WIDTHS = { poster:70, title:340, year:92, rel:120, genre:180, type:104, tmdb:130, imdb:150, tvdb:120, trakt:120, simkl:120, anilist:120, mal:120, sync:172, added:150, key:220 };
  const MIN_COLUMN_WIDTHS = { poster:56, title:120, year:72, rel:92, genre:96, type:82, tmdb:88, imdb:104, tvdb:88, trakt:88, simkl:88, anilist:96, mal:82, sync:106, added:112, key:120 };
  const MAX_COLUMN_WIDTHS = { poster:110, title:760, year:160, rel:220, genre:360, type:180, tmdb:260, imdb:280, tvdb:240, trakt:240, simkl:240, anilist:240, mal:240, sync:380, added:260, key:380 };
  const COLUMN_META = {
    poster: { icon:"image", label:"Poster" },
    title: { icon:"title", label:"Title", required:true },
    year: { icon:"event", label:"Year" },
    rel: { icon:"calendar_month", label:"Release" },
    genre: { icon:"theater_comedy", label:"Genre" },
    type: { icon:"category", label:"Type" },
    tmdb: { icon:"fingerprint", label:"TMDB" },
    imdb: { icon:"tag", label:"IMDb" },
    tvdb: { icon:"dns", label:"TVDB" },
    trakt: { icon:"confirmation_number", label:"Trakt" },
    simkl: { icon:"hub", label:"SIMKL" },
    anilist: { icon:"animation", label:"AniList" },
    mal: { icon:"link", label:"MAL" },
    sync: { icon:"sync", label:"Sync" },
    added: { icon:"schedule", label:"Added" },
    key: { icon:"key", label:"Key" },
  };


  /* Layout */
  const host=document.getElementById("page-watchlist"); if(!host) return;
  const readPrefs=()=>{try{return JSON.parse(localStorage.getItem("wl.prefs")||"{}")}catch{return{}}};
  const writePrefs=p=>{try{localStorage.setItem("wl.prefs",JSON.stringify(p))}catch{}};
  const prefs=Object.assign({posterMin:150,view:"posters",released:"both",overlays:"yes",genre:"",showHidden:false,sortKey:"title",sortDir:"asc",moreOpen:false,pageSize:DEFAULT_PAGE_SIZE,wideView:false,cols:{},colUser:{},colVis:{},columnOrder:DEFAULT_COLUMN_ORDER.slice(),columnLayoutVersion:COLUMN_LAYOUT_VERSION},readPrefs());
  const providerMeta = () => window.CW?.ProviderMeta || {};
  const providerKey = (value) => providerMeta().keyOf?.(value) || String(value || "").trim().toUpperCase();
  const providerLabel = (value) => providerMeta().label?.(value) || providerKey(value) || String(value || "");
  const providerShortLabel = (value) => providerMeta().shortLabel?.(value) || providerLabel(value);
  const escOpt = s => String(s == null ? "" : s).replace(/[&<>"]/g, m => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;" }[m]));
  let activeProviders = new Set();
  let crosswatchProfiles = [{ id: "default", label: "Default" }];
  const watchlistProviderKeys = () => {
    const keys = providerMeta().watchlistProviders?.();
    return Array.isArray(keys) && keys.length
      ? keys
      : ["CROSSWATCH","PLEX","JELLYFIN","EMBY","SIMKL","TRAKT","ANILIST","TMDB","MDBLIST","PUBLICMETADB","PUNCHPLAY","FLOPPY","SCROB","NUVIO","STREMIO"];
  };
  const PROVIDERS = watchlistProviderKeys();
  const visibleProviders = () => PROVIDERS.filter((p) => p !== "CROSSWATCH" || activeProviders.has("CROSSWATCH"));
  const providerOptions=(empty="All")=>`<option value="">${empty}</option>${visibleProviders().map(p=>`<option value="${p}">${providerLabel(p)}</option>`).join("")}`;
  const deleteProviderOptions=pick=>`<option value="ALL">ALL (default)</option>${(pick ? PROVIDERS.filter(p=>pick.has(p)) : visibleProviders()).map(p=>`<option value="${p}">${providerLabel(p)}</option>`).join("")}`;
  const cwProfileOptions=()=>crosswatchProfiles.map(p=>`<option value="${escOpt(p.id)}">${escOpt(p.label || p.id)}</option>`).join("");
  const isProfileUser = () => {
    const doc = document.documentElement;
    return doc?.dataset?.cwRole === "user" && doc?.dataset?.cwPermWrite !== "on";
  };
  const hasFloppyConfig = root => {
    const match = block => block && typeof block === "object" && (block.server_url || block.server) && (block.api_token || block.token);
    return !!(match(root) || (root?.instances && Object.values(root.instances).some(match)));
  };
  host.innerHTML=`<div class="wl-topline cw-page-hero cw-page-hero-watchlist" data-hero-icon="bookmark_added"><div class="wl-title-stack cw-page-hero-copy"><div class="cw-page-hero-kicker">WATCHLIST</div><div class="wl-title cw-page-hero-title">Watchlist</div><div class="wl-sub cw-page-hero-sub">Browse and manage your unified watchlist</div></div><div class="wl-hero-summary cw-page-hero-actions" id="wl-hero-summary" aria-label="Watchlist summary"><div class="wl-hero-seg"><strong id="wl-stat-total">0</strong><span>items</span></div><div class="wl-hero-seg"><strong id="wl-stat-visible">0</strong><span>visible</span></div><div class="wl-hero-seg wl-hero-sync"><span>Synced</span><strong id="wl-stat-sync">never</strong></div><button id="wl-refresh" class="wl-hero-refresh" title="Sync watchlist" aria-label="Sync watchlist"><span class="material-symbol ss-refresh-icon">refresh</span></button></div></div><div class="wl-wrap" id="watchlist-root"><div class="wl-main-shell"><div class="wl-toolbar"><div class="wl-toolbar-left"><label class="wl-chip wl-selectall"><input id="wl-select-all" type="checkbox"><span>Select all</span></label><span id="wl-count" class="wl-chip is-filter">0 selected</span></div><div class="wl-toolbar-right"><span id="wl-filter-state" class="wl-chip is-filter">All items</span></div></div><div id="wl-posters" class="wl-grid" style="display:none"></div><div id="wl-list" class="wl-table-wrap" style="display:none"><table class="wl-table"><colgroup><col class="c-sel"><col class="c-poster"><col class="c-title"><col class="c-rel"><col class="c-genre"><col class="c-type"><col class="c-sync"></colgroup><thead><tr><th style="text-align:center"><input id="wl-list-select-all" type="checkbox"></th><th class="sortable" data-sort="poster" data-col="poster" style="position:relative">Poster<span class="wl-resize"></span></th><th class="sortable" data-sort="title" data-col="title" style="position:relative">Title<span class="wl-resize"></span></th><th class="sortable" data-sort="release" data-col="rel" style="position:relative">Release<span class="wl-resize"></span></th><th class="sortable" data-sort="genre" data-col="genre" style="position:relative">Genre<span class="wl-resize"></span></th><th class="sortable" data-sort="type" data-col="type" style="position:relative">Type<span class="wl-resize"></span></th><th class="sortable" data-sort="sync" data-col="sync" style="position:relative">Sync<span class="wl-resize"></span></th></tr></thead><tbody id="wl-tbody"></tbody></table></div><div id="wl-pagination" class="wl-pagination" style="display:none"><button id="wl-page-prev" class="wl-btn">Previous</button><span id="wl-page-label" class="wl-muted">Page 1 of 1 • Rows 0–0 of 0</span><button id="wl-page-next" class="wl-btn">Next</button></div><div id="wl-empty" class="wl-empty wl-muted" style="display:none">No items match the current filters.</div></div><aside class="wl-side"><div class="ins-card"><div class="ins-row wl-ref-row" style="align-items:center"><div class="ins-icon"><span class="material-symbol">tune</span></div><div class="ins-title" style="margin-right:auto">Filters</div></div><div class="ins-row"><div class="ins-kv"><label for="wl-view">View</label><select id="wl-view" name="wl-view" class="wl-input" style="width:auto;padding:6px 10px"><option value="posters">Posters</option><option value="list">List</option></select><label for="wl-q">Search</label><input id="wl-q" name="wl-q" class="wl-input" placeholder="Search title..."><label for="wl-type">Type</label><select id="wl-type" name="wl-type" class="wl-input"><option value="">All types</option><option value="movie">Movies</option><option value="tv">Shows</option><option value="anime">Anime</option></select><label for="wl-provider">Provider</label><select id="wl-provider" name="wl-provider" class="wl-input">${providerOptions()}</select><label id="wl-size-label" for="wl-size">Size</label><input id="wl-size" name="wl-size" type="range" min="120" max="320" step="10" class="wl-input" style="padding:0"></div></div><div class="ins-row" id="wl-more-panel" style="display:none"><div class="ins-kv"><label for="wl-released">Released</label><select id="wl-released" name="wl-released" class="wl-input"><option value="both">Both</option><option value="released">Released</option><option value="unreleased">Upcoming</option></select><label id="wl-overlays-label" for="wl-overlays">Overlays</label><select id="wl-overlays" name="wl-overlays" class="wl-input"><option value="yes">On</option><option value="no">Off</option></select><label for="wl-genre">Genre</label><select id="wl-genre" name="wl-genre" class="wl-input"><option value="">All</option></select><label for="wl-show-hidden">Hidden</label><label class="wl-chip" style="justify-content:flex-start"><input id="wl-show-hidden" type="checkbox"><span>Include local hidden</span></label><div id="wl-cols-label" class="field-label">Columns</div><div id="wl-cols" class="wl-cols"><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="poster">Poster</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="rel">Release</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="genre">Genre</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="type">Type</label><label class="wl-colchip"><input type="checkbox" name="wl-col" data-col="sync">Sync</label></div></div></div><div class="ins-row" style="justify-content:flex-end;gap:8px"><button id="wl-more" class="wl-btn" aria-expanded="false">More</button><button id="wl-clear" class="wl-btn">Reset</button></div></div><div class="ins-card"><div class="ins-row wl-action-head"><div class="wl-action-title"><div class="ins-icon"><span class="material-symbol">flash_on</span></div><div class="ins-title">Actions</div></div><button type="button" class="wl-action-help" aria-label="Watchlist actions help" title="Delete selected items from the chosen provider, or from all providers. Hide local only removes selected items from the local CrossWatch view. Unhide all restores locally hidden items."><span class="material-symbol">help</span></button></div><div class="ins-row"><div class="wl-actions-panel"><div class="wl-action-row"><div class="wl-action-copy"><div class="wl-action-label">Delete from</div><div class="wl-action-hint">Selected items only</div></div><div class="wl-action-control is-select"><select id="wl-delete-provider" name="wl-delete-provider" class="wl-input">${deleteProviderOptions()}</select><button id="wl-delete" class="wl-btn danger" disabled>Delete</button></div></div><div class="wl-action-row"><div class="wl-action-copy"><div class="wl-action-label">Visibility</div><div class="wl-action-hint">Local view only</div></div><div class="wl-action-control"><button id="wl-hide" class="wl-btn" disabled>Hide local</button><button id="wl-unhide" class="wl-btn">Unhide all</button></div></div></div></div></div><div class="ins-card"><div class="ins-row"><div class="ins-icon"><span class="material-symbol">monitoring</span></div><div class="ins-title">Coverage Pulse</div></div><div class="ins-row"><div id="wl-metrics" class="ins-metrics"></div></div></div></aside></div><div id="wl-snack" class="wl-snack wl-hidden" role="status" aria-live="polite"></div>`;
  host.querySelector(".wl-actions-panel")?.closest(".ins-card")?.classList.add("wl-action-card");
  host.classList.toggle("wl-readonly", isProfileUser());

  /* References to elements */
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
  const sideEl      = document.querySelector(".wl-side");
  const moreBtn     = $("wl-more");
  const morePanel   = $("wl-more-panel");
  const releasedSel = $("wl-released");
  const overlaysSel = $("wl-overlays");
  const overlaysLabel = $("wl-overlays-label");
  const genreSel    = $("wl-genre");
  const colsLabel  = $("wl-cols-label");
  const colsBox    = $("wl-cols");
  const pagerEl     = $("wl-pagination");
  const pagerPrev   = $("wl-page-prev");
  const pagerNext   = $("wl-page-next");
  const pagerLabel  = $("wl-page-label");
  const topTotalEl  = $("wl-stat-total");
  const topVisibleEl= $("wl-stat-visible");
  const topSyncEl   = $("wl-stat-sync");
  const filterStateEl = $("wl-filter-state");
  const showHiddenChk = $("wl-show-hidden");
  let pageSizeSel = null;
  let pageSizeValueEl = null;
  let viewValueEl = null;
  let columnsBtn = null;
  let wideBtn = null;
  let cwProfileLabel = null;
  let cwProfileSel = null;
  let delProfile = null;
  function decorateToolbar() {
    const toolbar = host.querySelector(".wl-toolbar");
    const left = host.querySelector(".wl-toolbar-left");
    const right = host.querySelector(".wl-toolbar-right");
    if (!left || !right || !qEl || !viewSel) return;
    host.querySelector('label[for="wl-q"]')?.remove();
    host.querySelector('label[for="wl-view"]')?.remove();
    colsLabel?.remove();
    colsBox?.remove();
    qEl.placeholder = "Filter by title / id / provider...";
    toolbar?.classList.add("cw-controls");
    qEl.classList.add("cw-input", "wl-toolbar-search");

    const viewField = document.createElement("div");
    viewField.className = "cw-page-size-control wl-toolbar-field wl-view-field";
    viewField.innerHTML = `<span class="wl-toolbar-field-label">View</span>`;
    const viewWrap = document.createElement("button");
    viewWrap.id = "wl-view-menu";
    viewWrap.className = "cw-btn wl-btn wl-toolbar-menu wl-view-control";
    viewWrap.type = "button";
    viewWrap.setAttribute("aria-label", "View");
    viewWrap.innerHTML = `<span class="wl-toolbar-menu-value"></span><span class="material-symbols-rounded" aria-hidden="true">expand_more</span>`;
    viewSel.style.display = "none";
    viewValueEl = viewWrap.querySelector(".wl-toolbar-menu-value");
    viewField.appendChild(viewWrap);

    const pageField = document.createElement("div");
    pageField.className = "cw-page-size-control wl-toolbar-field wl-page-field";
    pageField.innerHTML = `<span class="wl-toolbar-field-label">Rows</span>`;
    const pageWrap = document.createElement("button");
    pageWrap.id = "wl-page-size";
    pageWrap.className = "cw-btn wl-btn wl-toolbar-menu wl-page-size-control";
    pageWrap.type = "button";
    pageWrap.setAttribute("aria-label", "Rows");
    pageWrap.innerHTML = `<span class="wl-toolbar-menu-value"></span><span class="material-symbols-rounded" aria-hidden="true">expand_more</span>`;
    pageSizeValueEl = pageWrap.querySelector(".wl-toolbar-menu-value");
    pageSizeSel = pageWrap;
    pageField.appendChild(pageWrap);

    columnsBtn = document.createElement("button");
    columnsBtn.id = "wl-columns-btn";
    columnsBtn.className = "cw-btn wl-btn cw-columns-btn wl-columns-btn";
    columnsBtn.type = "button";
    columnsBtn.title = "Columns";
    columnsBtn.setAttribute("aria-label", "Columns");
    columnsBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">filter_alt</span>`;

    wideBtn = document.createElement("button");
    wideBtn.id = "wl-wide-btn";
    wideBtn.className = "cw-btn wl-btn cw-wide-btn wl-wide-btn";
    wideBtn.type = "button";
    wideBtn.title = "Wide view";
    wideBtn.setAttribute("aria-label", "Wide view");
    wideBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">fullscreen</span>`;

    const spacer = document.createElement("div");
    spacer.className = "cw-controls-spacer wl-controls-spacer";
    selAll?.closest(".wl-selectall")?.remove();
    left.replaceChildren(qEl, viewField, pageField, columnsBtn, wideBtn, spacer);
    selCount?.classList.add("cw-chip");
    filterStateEl?.classList.add("cw-chip");
    if (selCount) right.insertBefore(selCount, filterStateEl || null);
  }
  decorateToolbar();
  const enhancedControlWrap = el => {
    const wrap = el?.nextElementSibling;
    return wrap?.classList?.contains("cw-icon-select") && wrap.__cwNativeSelect === el ? wrap : null;
  };
  const setControlVisible = (el, on) => {
    if (!el) return;
    el.hidden = !on;
    el.style.display = on ? "" : "none";
    el.classList.toggle("wl-hidden-control", !on);
    const wrap = enhancedControlWrap(el);
    if (wrap) {
      wrap.hidden = !on;
      wrap.style.display = on ? "" : "none";
      wrap.classList.toggle("wl-hidden-control", !on);
    }
  };
  const syncProfileSelectOptions = (selectEl, current = "default") => {
    if (!selectEl) return "default";
    const opts = cwProfileOptions();
    selectEl.innerHTML = opts || '<option value="default">Default</option>';
    const values = Array.from(selectEl.options).map(o => o.value);
    const next = values.includes(current) ? current : "default";
    selectEl.value = next;
    return next;
  };
  function ensureCrosswatchProfileControls() {
    if (providerSel && !cwProfileSel) {
      cwProfileLabel = document.createElement("label");
      cwProfileLabel.id = "wl-cw-profile-label";
      cwProfileLabel.setAttribute("for", "wl-cw-profile");
      cwProfileLabel.textContent = "Profile";
      cwProfileSel = document.createElement("select");
      cwProfileSel.id = "wl-cw-profile";
      cwProfileSel.name = "wl-cw-profile";
      cwProfileSel.className = "wl-input";
      providerSel.insertAdjacentElement("afterend", cwProfileSel);
      providerSel.insertAdjacentElement("afterend", cwProfileLabel);
      syncProfileSelectOptions(cwProfileSel, "default");
    }
    if (delProv && !delProfile) {
      delProfile = document.createElement("select");
      delProfile.id = "wl-delete-profile";
      delProfile.name = "wl-delete-profile";
      delProfile.className = "wl-input";
      delProv.insertAdjacentElement("afterend", delProfile);
      syncProfileSelectOptions(delProfile, "default");
    }
  }
  ensureCrosswatchProfileControls();

  /* Column sizing */
  const isRequiredColumn = column => !!(COLUMN_META[column] && COLUMN_META[column].required);
  const isColVisible = column => isRequiredColumn(column) || prefs.colVis?.[column] !== false;
  const pageSizeValue = () => PAGE_SIZE_OPTIONS.includes(Number(prefs.pageSize)) ? Number(prefs.pageSize) : DEFAULT_PAGE_SIZE;
  const columnHeaders = {};
  let activePopup = null;
  let columnLayoutResizeTimer = 0;

  function normalizeColumnOrder(order) {
    const seen = new Set();
    const out = [];
    (Array.isArray(order) ? order : []).forEach(column => {
      const key = column === "release" ? "rel" : column;
      if (!COLUMN_KEYS.includes(key) || seen.has(key)) return;
      seen.add(key);
      out.push(key);
    });
    COLUMN_KEYS.forEach(column => {
      if (!seen.has(column)) out.push(column);
    });
    return out;
  }

  function clampColumnWidth(column, value) {
    const fallback = DEFAULT_COLUMN_WIDTHS[column] || 160;
    const raw = typeof value === "string" ? parseInt(value, 10) : Number(value);
    const n = Number.isFinite(raw) ? raw : fallback;
    return Math.max(MIN_COLUMN_WIDTHS[column] || 80, Math.min(MAX_COLUMN_WIDTHS[column] || 720, n));
  }

  function normalizeColumnState() {
    const legacyVisibility = prefs.colVis && typeof prefs.colVis === "object" ? prefs.colVis : {};
    prefs.colVis = Object.assign({}, DEFAULT_COLUMN_VISIBILITY, legacyVisibility);
    if (typeof prefs.colVis.release === "boolean") {
      prefs.colVis.rel = prefs.colVis.release;
      delete prefs.colVis.release;
    }
    prefs.colVis.title = true;
    prefs.columnOrder = normalizeColumnOrder(prefs.columnOrder);
    prefs.cols = prefs.cols && typeof prefs.cols === "object" ? prefs.cols : {};
    prefs.colUser = prefs.colUser && typeof prefs.colUser === "object" ? prefs.colUser : {};
    COLUMN_KEYS.forEach(column => {
      prefs.cols[column] = clampColumnWidth(column, prefs.cols[column]);
      prefs.colUser[column] = !!prefs.colUser[column];
    });
    Object.keys(prefs.colUser).forEach(column => {
      if (!COLUMN_KEYS.includes(column)) delete prefs.colUser[column];
    });
    prefs.pageSize = pageSizeValue();
    prefs.columnLayoutVersion = COLUMN_LAYOUT_VERSION;
  }

  function orderedColumns() {
    normalizeColumnState();
    return prefs.columnOrder;
  }

  function visibleColumns() {
    return orderedColumns().filter(isColVisible);
  }

  function columnLabel(column) {
    return COLUMN_META[column]?.label || column;
  }

  function columnWidth(column) {
    normalizeColumnState();
    return clampColumnWidth(column, prefs.cols[column]);
  }

  function sortForColumn(column) {
    return column === "rel" ? "rel" : column;
  }

  function ensureColumnHeader(column) {
    let th = columnHeaders[column] || host.querySelector(`.wl-table th[data-col="${column}"]`);
    if (!th) th = document.createElement("th");
    columnHeaders[column] = th;
    th.className = `sortable wl-col-${column} wl-resizable`;
    th.dataset.col = column;
    th.dataset.sort = sortForColumn(column);
    th.style.position = "relative";
    th.innerHTML = `<span class="wl-th-inner"><span class="wl-th-label">${escOpt(columnLabel(column))}</span></span>`;
    let handle = th.querySelector(".wl-resize");
    if (!handle) {
      handle = document.createElement("span");
      handle.className = "wl-resize";
      handle.setAttribute("role", "separator");
      handle.setAttribute("aria-orientation", "vertical");
      th.appendChild(handle);
    }
    handle.title = `Resize ${columnLabel(column)}`;
    handle.onpointerdown = ev => startColumnResize(ev, column, th);
    handle.ondblclick = ev => {
      ev.preventDefault();
      ev.stopPropagation();
      prefs.cols[column] = DEFAULT_COLUMN_WIDTHS[column];
      if (prefs.colUser) prefs.colUser[column] = false;
      applyColumnLayout();
      writePrefs(prefs);
    };
    th.onclick = ev => {
      if (ev.target?.closest?.(".wl-resize")) return;
      setSort(th.dataset.sort);
    };
    return th;
  }

  function syncColumnGroup(table, columns, widths, titleExtra, fillerWidth = 0) {
    let group = table.querySelector("colgroup");
    if (!group) {
      group = document.createElement("colgroup");
      table.insertBefore(group, table.firstChild);
    }
    group.replaceChildren();
    [["select", 46], ...columns.map(column => [column, widths[column] + (column === "title" ? titleExtra : 0)])].forEach(([column, width]) => {
      const col = document.createElement("col");
      col.dataset.column = column;
      col.className = column === "select" ? "c-sel" : `c-${column}`;
      col.style.width = `${width}px`;
      group.appendChild(col);
    });
    if (fillerWidth > 0) {
      const col = document.createElement("col");
      col.dataset.column = "_fill";
      col.className = "c-fill";
      col.style.width = `${fillerWidth}px`;
      group.appendChild(col);
    }
  }

  function applyRenderedRowColumnOrder() {
    const columns = visibleColumns();
    const table = host.querySelector(".wl-table");
    const needsFiller = table?.dataset?.hasFiller === "1";
    host.querySelectorAll(".wl-table tbody tr").forEach(tr => {
      const selectCell = tr.children[0] || null;
      const cells = {};
      COLUMN_KEYS.forEach(column => {
        const td = tr.querySelector(`td[data-col="${column}"]`);
        if (td) cells[column] = td;
      });
      const frag = document.createDocumentFragment();
      if (selectCell) frag.appendChild(selectCell);
      columns.forEach(column => {
        if (cells[column]) frag.appendChild(cells[column]);
      });
      let filler = tr.querySelector('td[data-col="_fill"]');
      if (!needsFiller) {
        filler?.remove();
      } else {
        if (!filler) {
          filler = document.createElement("td");
          filler.dataset.col = "_fill";
          filler.className = "wl-fill-cell";
          filler.setAttribute("aria-hidden", "true");
        }
        frag.appendChild(filler);
      }
      tr.appendChild(frag);
    });
  }

  function applyColumnLayout() {
    const table = host.querySelector(".wl-table");
    const headRow = table?.tHead?.rows?.[0];
    if (!table || !headRow) return;
    const selectHead = headRow.querySelector("th:first-child") || document.createElement("th");
    const columns = visibleColumns();
    const widths = Object.fromEntries(COLUMN_KEYS.map(column => [column, columnWidth(column)]));
    let total = 46;
    columns.forEach(column => { total += widths[column]; });
    const containerWidth = Math.floor(listWrapEl?.clientWidth || 0);
    const availableWidth = Math.max(0, containerWidth - 1);
    const hasOverflow = availableWidth > 0 && total > availableWidth + 1;
    const layoutWidths = { ...widths };
    let layoutTotal = total;
    if (!hasOverflow && availableWidth > total) {
      let remaining = availableWidth - total;
      const weights = { title:4, genre:2, sync:2, rel:1 };
      const growTargets = columns.filter(column => Object.prototype.hasOwnProperty.call(weights, column) && !prefs.colUser?.[column]);
      while (remaining > 0 && growTargets.length) {
        const weightSum = growTargets.reduce((sum, column) => sum + weights[column], 0);
        let consumed = 0;
        for (let i = growTargets.length - 1; i >= 0; i -= 1) {
          const column = growTargets[i];
          const cap = (MAX_COLUMN_WIDTHS[column] || 720) - layoutWidths[column];
          if (cap <= 0) {
            growTargets.splice(i, 1);
            continue;
          }
          const add = Math.min(cap, remaining, Math.max(1, Math.round((remaining * weights[column]) / weightSum)));
          layoutWidths[column] += add;
          remaining -= add;
          consumed += add;
        }
        if (!consumed) break;
      }
      layoutTotal = availableWidth - remaining;
    }
    const fillerWidth = !hasOverflow && availableWidth > layoutTotal ? availableWidth - layoutTotal : 0;
    const fillerHead = document.createElement("th");
    fillerHead.dataset.col = "_fill";
    fillerHead.className = "wl-fill-col";
    fillerHead.setAttribute("aria-hidden", "true");

    selectHead.style.width = "46px";
    selectHead.style.minWidth = "46px";
    headRow.replaceChildren(selectHead, ...columns.map(ensureColumnHeader), ...(fillerWidth > 0 ? [fillerHead] : []));
    syncColumnGroup(table, columns, layoutWidths, 0, fillerWidth);
    columns.forEach(column => {
      const th = columnHeaders[column];
      const width = layoutWidths[column];
      th.style.width = `${width}px`;
      th.style.minWidth = `${width}px`;
    });
    table.dataset.hasFiller = fillerWidth > 0 ? "1" : "0";
    table.style.width = hasOverflow ? `${total}px` : `${layoutTotal + fillerWidth}px`;
    table.style.minWidth = hasOverflow ? `${total}px` : "0";
    listWrapEl?.classList.toggle("wl-table-overflow-x", hasOverflow);
    applyRenderedRowColumnOrder();
    updateSortHeaderUI();
  }

  function applyResizeWidths(column, startWidths, rawWidth) {
    const columns = visibleColumns();
    const widths = Object.fromEntries(columns.map(c => [c, clampColumnWidth(c, startWidths[c])]));
    if (!columns.includes(column)) return;
    widths[column] = clampColumnWidth(column, rawWidth);

    const availableWidth = Math.max(0, Math.floor(listWrapEl?.clientWidth || 0) - 1);
    const minTotal = 46 + columns.reduce((sum, c) => sum + (MIN_COLUMN_WIDTHS[c] || 80), 0);
    if (availableWidth > minTotal) {
      let overflow = 46 + columns.reduce((sum, c) => sum + widths[c], 0) - availableWidth;
      const idx = columns.indexOf(column);
      const candidates = [...columns.slice(idx + 1), ...columns.slice(0, idx).reverse()].filter(c => c !== column);
      for (const c of candidates) {
        if (overflow <= 0) break;
        const min = MIN_COLUMN_WIDTHS[c] || 80;
        const take = Math.min(overflow, Math.max(0, widths[c] - min));
        widths[c] -= take;
        overflow -= take;
      }
      if (overflow > 0) {
        widths[column] = Math.max(MIN_COLUMN_WIDTHS[column] || 80, widths[column] - overflow);
      }
    }

    columns.forEach(c => {
      prefs.cols[c] = clampColumnWidth(c, widths[c]);
    });
    prefs.colUser = prefs.colUser && typeof prefs.colUser === "object" ? prefs.colUser : {};
    prefs.colUser[column] = true;
  }

  function startColumnResize(ev, column, th) {
    if (!COLUMN_KEYS.includes(column)) return;
    if (ev.button != null && ev.button !== 0) return;
    ev.preventDefault();
    ev.stopPropagation();
    const columns = visibleColumns();
    const startWidths = Object.fromEntries(columns.map(c => [c, columnWidth(c)]));
    const startX = ev.clientX;
    const startWidth = startWidths[column] || columnWidth(column);
    document.body.classList.add("cw-column-resizing");
    th.classList.add("wl-resizing");
    const onMove = moveEv => {
      applyResizeWidths(column, startWidths, startWidth + moveEv.clientX - startX);
      applyColumnLayout();
    };
    const finish = () => {
      document.removeEventListener("pointermove", onMove);
      document.removeEventListener("pointerup", finish);
      document.removeEventListener("pointercancel", finish);
      document.body.classList.remove("cw-column-resizing");
      th.classList.remove("wl-resizing");
      writePrefs(prefs);
    };
    document.addEventListener("pointermove", onMove);
    document.addEventListener("pointerup", finish);
    document.addEventListener("pointercancel", finish);
  }

  function syncWideViewUI() {
    const active = !!prefs.wideView;
    host.classList.toggle("wl-wide", active);
    wideBtn?.classList.toggle("active", active);
    if (wideBtn) {
      wideBtn.title = active ? "Exit wide view" : "Wide view";
      wideBtn.setAttribute("aria-label", wideBtn.title);
      const icon = wideBtn.querySelector(".material-symbols-rounded");
      if (icon) icon.textContent = active ? "fullscreen_exit" : "fullscreen";
    }
    window.setTimeout(applyColumnLayout, 0);
  }

  function syncPageSizeUI() {
    prefs.pageSize = pageSizeValue();
    if (pageSizeValueEl) pageSizeValueEl.textContent = String(prefs.pageSize);
    pageSizeSel?.setAttribute("aria-label", `Rows: ${prefs.pageSize}`);
  }

  function syncViewModeUI() {
    if (viewSel) viewSel.value = viewMode;
    if (viewValueEl) viewValueEl.textContent = viewMode === "list" ? "List" : "Posters";
    host.querySelector("#wl-view-menu")?.setAttribute("aria-label", `View: ${viewMode === "list" ? "List" : "Posters"}`);
  }

  function syncColumnVisibilityUI() {
    normalizeColumnState();
    if (COLUMN_KEYS.includes(sortKey) && prefs.colVis[sortKey] === false) {
      sortKey = "title";
      sortDir = "asc";
      prefs.sortKey = sortKey;
      prefs.sortDir = sortDir;
    }
    const hiddenCount = COLUMN_KEYS.filter(k => !isRequiredColumn(k) && !isColVisible(k)).length;
    const customVisibility = COLUMN_KEYS.some(k => prefs.colVis[k] !== DEFAULT_COLUMN_VISIBILITY[k]);
    const customOrder = orderedColumns().join("|") !== DEFAULT_COLUMN_ORDER.join("|");
    const customWidths = COLUMN_KEYS.some(k => columnWidth(k) !== DEFAULT_COLUMN_WIDTHS[k]);
    columnsBtn?.classList.toggle("active", customVisibility || customOrder || customWidths);
    if (columnsBtn) columnsBtn.title = hiddenCount > 0 ? `Columns (${hiddenCount} hidden)` : customOrder || customWidths ? "Columns (custom layout)" : "Columns";
    document.querySelectorAll(".cw-columns-pop .cw-column-toggle").forEach(btn => {
      const column = btn.dataset.column;
      const visible = isColVisible(column);
      btn.classList.toggle("active", visible);
      btn.setAttribute("aria-pressed", visible ? "true" : "false");
      btn.disabled = isRequiredColumn(column);
    });
    applyColumnLayout();
  }

  function closePopup() {
    if (!activePopup) return;
    document.removeEventListener("mousedown", activePopup.onDoc);
    document.removeEventListener("keydown", activePopup.onKey);
    activePopup.node?.remove?.();
    activePopup = null;
  }

  function positionPopup(pop, anchor) {
    if (!pop || !anchor) return;
    const rect = anchor.getBoundingClientRect();
    const margin = 8;
    const viewportWidth = document.documentElement.clientWidth;
    const viewportHeight = document.documentElement.clientHeight;
    let left = rect.left + window.scrollX;
    let top = rect.bottom + margin + window.scrollY;
    const width = pop.offsetWidth;
    const height = pop.offsetHeight;
    if (left + width + margin > window.scrollX + viewportWidth) left = window.scrollX + viewportWidth - width - margin;
    if (top + height + margin > window.scrollY + viewportHeight) top = rect.top + window.scrollY - height - margin;
    if (left < margin) left = margin;
    if (top < margin) top = margin;
    pop.style.left = `${left}px`;
    pop.style.top = `${top}px`;
  }

  function openPopup(anchor, builder) {
    closePopup();
    const pop = document.createElement("div");
    pop.className = "cw-pop";
    document.body.appendChild(pop);
    builder(pop);
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

  function appendPopupTitle(pop, text) {
    const title = document.createElement("div");
    title.className = "cw-pop-title";
    title.textContent = text;
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
    if (!COLUMN_KEYS.includes(column) || isRequiredColumn(column)) return;
    prefs.colVis[column] = !isColVisible(column);
    syncColumnVisibilityUI();
    writePrefs(prefs);
    render();
  }

  function moveColumn(column, delta) {
    const order = orderedColumns().slice();
    const idx = order.indexOf(column);
    const next = idx + delta;
    if (idx < 0 || next < 0 || next >= order.length) return;
    [order[idx], order[next]] = [order[next], order[idx]];
    prefs.columnOrder = order;
    syncColumnVisibilityUI();
    writePrefs(prefs);
    renderColumnPopupContents(activePopup?.node);
    render();
  }

  function resetColumnLayout(pop) {
    prefs.columnOrder = DEFAULT_COLUMN_ORDER.slice();
    prefs.colVis = { ...DEFAULT_COLUMN_VISIBILITY };
    prefs.cols = { ...DEFAULT_COLUMN_WIDTHS };
    syncColumnVisibilityUI();
    writePrefs(prefs);
    renderColumnPopupContents(pop);
    render();
  }

  function renderColumnPopupContents(pop) {
    if (!pop || !pop.classList?.contains("cw-columns-pop")) return;
    pop.textContent = "";
    appendPopupTitle(pop, "Columns");
    const list = document.createElement("div");
    list.className = "cw-column-popup-list wl-column-popup-list";
    const order = orderedColumns();
    order.forEach((column, idx) => {
      const meta = COLUMN_META[column] || { icon:"view_column", label:column };
      const row = document.createElement("div");
      row.className = "cw-column-row";
      row.dataset.column = column;

      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "cw-type-chip cw-column-toggle";
      toggle.dataset.column = column;
      toggle.disabled = isRequiredColumn(column);
      toggle.title = isRequiredColumn(column) ? `${columnLabel(column)} is always shown` : `Show ${columnLabel(column)}`;
      const toggleIcon = document.createElement("span");
      toggleIcon.className = "material-symbol cw-type-icon";
      toggleIcon.setAttribute("aria-hidden", "true");
      toggleIcon.textContent = meta.icon;
      const toggleLabel = document.createElement("span");
      toggleLabel.textContent = columnLabel(column);
      toggle.replaceChildren(toggleIcon, toggleLabel);
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
      { label:"Reset", onClick:() => resetColumnLayout(pop) },
      { label:"Close", kind:"primary", onClick:closePopup },
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

  function openChoicePopup(anchor, options, currentValue, onPick) {
    openPopup(anchor, pop => {
      pop.classList.add("wl-choice-pop");
      const list = document.createElement("div");
      list.className = "wl-choice-list";
      options.forEach(opt => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "wl-choice-btn";
        btn.dataset.value = String(opt.value);
        btn.classList.toggle("active", String(opt.value) === String(currentValue));
        btn.innerHTML = `<span>${esc(opt.label)}</span>${String(opt.value) === String(currentValue) ? `<span class="material-symbols-rounded" aria-hidden="true">check</span>` : ""}`;
        btn.addEventListener("click", () => {
          onPick(opt.value);
          closePopup();
        });
        list.appendChild(btn);
      });
      pop.appendChild(list);
    });
  }

  normalizeColumnState();

  /* Date formatting */
  let [items, filtered] = [[], []];
  const selected = new Set();
  const hiddenSet = (() => { try { return new Set(JSON.parse(localStorage.getItem("wl.hidden") || "[]")); } catch { return new Set(); } })();
  const saveHidden = () => { try { localStorage.setItem("wl.hidden", JSON.stringify([...hiddenSet])); } catch {} };
  const hideBtn = document.getElementById("wl-hide"), unhideBtn = document.getElementById("wl-unhide");

  let viewMode = prefs.view === "list" ? "list" : "posters";
  let sortKey = prefs.sortKey === "release" ? "rel" : (COLUMN_KEYS.includes(prefs.sortKey) ? prefs.sortKey : "title");
  let sortDir = prefs.sortDir === "desc" ? "desc" : "asc";

  const derivedCache = new Map();
  const renderedRowRefs = new Map();
  let rowHydrationSeq = 0;
  let lastRowHydrationKey = "";

  const sharedMeta = () => window.CW?.Meta || null;
  const peekMeta = (it) => sharedMeta()?.peek(it) || null;
  const hasMeta = (it, profile) => !!sharedMeta()?.has(it, profile);
  const requestMetaBatch = async (items, profile = "row") => {
    if (!TMDB_OK) return false;
    return !!(await sharedMeta()?.batch(items, profile));
  };
  const getMetaFor = async (it, profile = "detail") => {
    if (!TMDB_OK) return null;
    return (await sharedMeta()?.get(it, profile)) || null;
  };
  const instancesOfProvider = (it, p) => {
    const sbp = it?.sources_by_provider || it?.sourcesByProvider || {};
    const arr = sbp?.[String(p || "").toLowerCase()] || sbp?.[String(p || "").toUpperCase()];
    return Array.isArray(arr) ? arr.map(x => String(x || "").trim()).filter(Boolean) : [];
  };

  function rebuildProviderOptions() {
    if (providerSel) {
      const current = String(providerSel.value || "");
      providerSel.innerHTML = providerOptions();
      providerSel.value = Array.from(providerSel.options).some((o) => o.value === current) ? current : "";
      enhanceProviderFilterSelect();
    }
    if (delProv) {
      const current = String(delProv.value || "ALL");
      delProv.innerHTML = deleteProviderOptions();
      delProv.value = Array.from(delProv.options).some((o) => o.value === current) ? current : "ALL";
      enhanceDeleteProviderSelect();
    }
    syncCrosswatchProfileControls();
  }

  function syncCrosswatchProfileControls() {
    ensureCrosswatchProfileControls();
    const filterOn = String(providerSel?.value || "").toUpperCase() === "CROSSWATCH";
    const deleteOn = String(delProv?.value || "").toUpperCase() === "CROSSWATCH";
    const currentFilter = cwProfileSel?.value || "default";
    const currentDelete = delProfile?.value || currentFilter || "default";
    syncProfileSelectOptions(cwProfileSel, currentFilter);
    syncProfileSelectOptions(delProfile, currentDelete);
    setControlVisible(cwProfileLabel, filterOn);
    setControlVisible(cwProfileSel, filterOn);
    setControlVisible(delProfile, deleteOn);
  }

  async function loadCrosswatchProfiles() {
    try {
      const data = await fetch("/api/provider-instances/CROSSWATCH", { cache: "no-store" }).then(r => r.ok ? r.json() : []);
      const list = Array.isArray(data) ? data : [];
      const norm = list
        .map(x => ({ id: String(x?.id || "").trim(), label: String(x?.label || x?.id || "").trim() }))
        .filter(x => x.id);
      crosswatchProfiles = norm.length ? norm : [{ id: "default", label: "Default" }];
    } catch (_) {
      crosswatchProfiles = [{ id: "default", label: "Default" }];
    }
    syncCrosswatchProfileControls();
  }

  let TMDB_OK = true;

  let currentPage = 1;
  let pageInfo = { start:0, end:0, total:0, pageCount:1 };
  let watchlistMeta = { lastSyncEpoch: 0 };

  /* utils */
  const esc = s => String(s).replace(/[&<>"]/g, m => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;" }[m]));
  const toLocale = () => navigator.language || "en-US";
  const cmp = (a, b) => a < b ? -1 : a > b ? 1 : 0;
  const cmpDir = v => (sortDir === "asc" ? v : -v);
  const normKey = it => it.key || it.guid || it.id || (it.ids?.tmdb && `tmdb:${it.ids.tmdb}`) || (it.ids?.imdb && `imdb:${it.ids.imdb}`) || (it.ids?.tvdb && `tvdb:${it.ids.tvdb}`) || "";
  const artType=it=>(((it?.type||it?.media_type||"")+"").toLowerCase()==="movie"?"movie":"tv");
  const artEvidence=it=>{const t=it?.title?`&title=${encodeURIComponent(String(it.title))}`:"";const y=it?.year?`&year=${encodeURIComponent(String(it.year))}`:"";return t+y;};
  const tmdbIdForArt = it => String(sharedMeta()?.tmdbId?.(it) || it?.tmdb || it?.tmdb_id || it?.ids?.tmdb || it?.ids?.tmdb_show || "").trim();
  const artUrl=(it,size,kind="poster")=>{const tmdbId=tmdbIdForArt(it);return(!TMDB_OK||!tmdbId)?"":`/art/tmdb/${artType(it)}/${encodeURIComponent(tmdbId)}?kind=${encodeURIComponent(kind)}&size=${encodeURIComponent(size||"w342")}&locale=${encodeURIComponent(window.__CW_LOCALE||navigator.language||"en-US")}${artEvidence(it)}`;};
  const parseReleaseDate = s => { if (typeof s !== "string" || !(s = s.trim())) return null; let y, m, d; if (/^\d{4}-\d{2}-\d{2}$/.test(s)) ([y, m, d] = s.split("-").map(Number)); else if (/^\d{2}-\d{2}-\d{4}$/.test(s)) { const a = s.split("-").map(Number); d = a[0]; m = a[1]; y = a[2]; } else return null; const t = Date.UTC(y, (m || 1) - 1, d || 1), dt = new Date(t); return Number.isFinite(dt.getTime()) ? dt : null; };
  const fmtDateSmart = (raw, loc) => { const dt = parseReleaseDate(raw); if (!dt) return ""; try { return new Intl.DateTimeFormat(loc || toLocale(), { day:"2-digit", month:"2-digit", year:"numeric", timeZone:"UTC" }).format(dt); } catch { return ""; } };
  const providersOf = it => Array.isArray(it.sources) ? it.sources.map(providerKey).filter(Boolean) : [];
  const addActiveProvidersFromItems = list => {
    for (const it of (Array.isArray(list) ? list : [])) {
      for (const provider of providersOf(it)) activeProviders.add(provider);
    }
  };
    const getReleaseIso = it => {
    const tv = /^(tv|show|anime)$/i.test(String(it.type || ""));
    let iso = tv ? (it.first_air_date || it.firstAired || it.aired) : (it.release_date || it.released);
    iso = iso || it?.release?.date || "";
    if (!iso) {
      const m = peekMeta(it) || {};
      iso = tv ? (m.detail?.first_air_date || m.release?.date || m.first_air_date || "")
              : (m.detail?.release_date || m.release?.date || "");
    }
    return typeof iso === "string" ? iso.trim() : "";
  };

  const resolvedTypeFor = it => String(peekMeta(it)?.resolved_type || "").toLowerCase();
  const typeLabelFor = it => {
    const resolved = resolvedTypeFor(it);
    if (resolved === "movie") return "Movie";
    const raw = String(it?.type || "").toLowerCase();
    if (raw === "movie") return "Movie";
    if (raw === "anime") return resolved === "show" ? "Show" : "Anime";
    return "Show";
  };
  const posterTypeLabelFor = it => {
    const resolved = resolvedTypeFor(it);
    if (resolved === "movie") return "M";
    const raw = String(it?.type || "").toLowerCase();
    if (raw === "movie") return "M";
    if (raw === "anime") return resolved === "show" ? "S" : "A";
    return "S";
  };
  const yearFromIso = iso => (typeof iso === "string" && /^\d{4}/.test(iso) ? iso.slice(0,4) : "");
  const formatRelativeSync = epoch => {
    const n = Number(epoch) || 0;
    if (!n) return "never";
    const now = Math.floor(Date.now() / 1000);
    const diff = Math.max(0, now - n);
    if (diff < 45) return "just now";
    if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
    return `${Math.round(diff / 86400)}d ago`;
  };
  const describeFilters = () => {
    const bits = [];
    const q = (qEl?.value || "").trim();
    const ty = (tEl?.value || "").trim();
    const provider = (providerSel?.value || "").trim();
    const rel = normReleased(releasedSel?.value || prefs.released || "both");
    const genre = (genreSel?.value || prefs.genre || "").trim();
    if (q) bits.push(`Search: ${q}`);
    if (ty) bits.push(typeLabelFor({ type: ty }));
    if (provider) bits.push(providerLabel(provider));
    if (provider.toUpperCase() === "CROSSWATCH" && cwProfileSel?.value) {
      const row = crosswatchProfiles.find(p => p.id === cwProfileSel.value);
      bits.push(row?.label || cwProfileSel.value);
    }
    if (rel === "released") bits.push("Released only");
    if (rel === "unreleased") bits.push("Upcoming only");
    if (genre) bits.push(genre);
    if (showHiddenChk?.checked) bits.push("Hidden included");
    return bits.length ? bits.join(" • ") : "All items";
  };
  const updateHeaderSummary = () => {
    if (topTotalEl) topTotalEl.textContent = String(items.length);
    if (topVisibleEl) topVisibleEl.textContent = String(filtered.length);
    if (topSyncEl) {
      topSyncEl.textContent = formatRelativeSync(watchlistMeta.lastSyncEpoch);
      topSyncEl.title = watchlistMeta.lastSyncEpoch ? new Date(watchlistMeta.lastSyncEpoch * 1000).toLocaleString() : "";
    }
    if (filterStateEl) {
      filterStateEl.textContent = describeFilters();
      filterStateEl.title = filterStateEl.textContent;
    }
  };
  window.setInterval(updateHeaderSummary, 30000);

  function computePageInfo() {
    const pageSize = pageSizeValue();
    const total = filtered.length;
    const pageCount = total ? Math.ceil(total / pageSize) : 1;
    if (currentPage < 1) currentPage = 1;
    if (currentPage > pageCount) currentPage = pageCount;
    const start = total ? (currentPage - 1) * pageSize : 0;
    const end = total ? Math.min(start + pageSize, total) : 0;
    pageInfo = { start, end, total, pageCount };
  }

  function updatePaginationUI() {
    if (!pagerEl) return;
    const total = pageInfo.total;
    if (!total) {
      pagerEl.style.display = "none";
      return;
    }
    const start = pageInfo.start;
    const end = pageInfo.end;
    const pageCount = pageInfo.pageCount;
    pagerEl.style.display = "";
    pagerLabel.textContent = `Page ${currentPage} of ${pageCount} • Rows ${start + 1}\u2013${end} of ${total}`;
    pagerPrev.disabled = currentPage <= 1;
    pagerNext.disabled = currentPage >= pageCount;
  }

  /* Hide/Unhide buttons */
  hideBtn?.addEventListener("click", () => {
    if (!selected.size) return;
    selected.forEach(k => hiddenSet.add(k));
    saveHidden(); selected.clear(); applyFilters(); updateSelCount(); snackbar("Hidden locally");
  }, true);

  unhideBtn?.addEventListener("click", () => {
    hiddenSet.clear(); saveHidden(); applyFilters(); updateSelCount(); snackbar("All unhidden");
  }, true);

  /* Hydration listing */
  const _hydrating = new Set();
  const setText = (el, t) => {
    if (!el) return;
    const next = (t || "").trim();
    if (!next) return;
    el.textContent = next;
    el.title = next;
  };

  function hydrateRow(it, tr){
    const k=normKey(it); if(_hydrating.has(k)) return; _hydrating.add(k);
    try{
      const movie = /^movie$/i.test(it.type||"");
      const m = peekMeta(it) || null;

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

  /* API fetches */
  const fetchWatchlist = async () => {
    const r = await fetch("/api/watchlist/?limit=5000", { cache: "no-store" });
    if (!r.ok) throw new Error("watchlist fetch failed");
    const j = await r.json();
    TMDB_OK = !Boolean(j?.missing_tmdb_key);
    watchlistMeta.lastSyncEpoch = Number(j?.last_sync_epoch) || 0;
    return Array.isArray(j?.items) ? j.items : [];
  };

  const fetchConfig = async () => {
    if (authSetupPending()) return {};
    try {
      const managed = document.documentElement?.dataset?.cwRole === "user";
      const r = await fetch(managed ? "/api/config/meta" : "/api/config", { cache: "no-store" });
      if (!r.ok) return {};
      const cfg = await r.json();
      if (!managed) return cfg || {};
      return {
        providers: cfg?.providers || {},
        provider_instances: cfg?.provider_instances || {},
        user_profiles: cfg?.user_profiles || {},
        features: cfg?.features || {},
        ui: cfg?.ui || {},
        auth: { setup_required: false },
      };
    }
    catch { return {}; }
  };

  const metadataAffectsListState = () => {
    const releasedPref = normReleased(releasedSel?.value || prefs.released || "both");
    const genrePref = (genreSel?.value || prefs.genre || "").trim();
    return releasedPref !== "both" || !!genrePref || sortKey === "rel" || sortKey === "genre";
  };

  const visibleItemsForHydration = () => {
    if (!filtered.length) return [];
    if (metadataAffectsListState()) {
      return filtered.slice(0, Math.min(filtered.length, 250));
    }
    if (viewMode === "list") {
      const sorted = sortFilteredForList(filtered);
      return sorted.slice(pageInfo.start, pageInfo.end);
    }
    return filtered.slice(pageInfo.start, pageInfo.end);
  };

  const rowHydrationKeyFor = targets => targets.map(it => sharedMeta()?.key(it) || "").filter(Boolean).join("|");

  async function hydrateVisibleMetadata() {
    if (!TMDB_OK) return;
    const targets = visibleItemsForHydration().filter((it) => tmdbIdForArt(it));
    if (!targets.length) return;
    const batchKey = rowHydrationKeyFor(targets);
    const missingRowProfile = targets.some((it) => !hasMeta(it, "row"));
    if (!missingRowProfile && batchKey && batchKey === lastRowHydrationKey) return;
    lastRowHydrationKey = batchKey;
    const seq = ++rowHydrationSeq;
    const changed = await requestMetaBatch(targets, "row");
    if (!changed || seq !== rowHydrationSeq) return;
    populateGenreOptions(buildGenreIndex(items));
    if (metadataAffectsListState()) {
      applyFilters();
      return;
    }
    if (viewMode === "posters") {
      renderPosters();
      return;
    }
    if (viewMode === "list") {
      targets.forEach((it) => {
        const tr = renderedRowRefs.get(normKey(it));
        if (!tr) return;
        hydrateRow(it, tr);
        const img = tr.querySelector(".wl-mini");
        const src = artUrl(it, "w92") || "/assets/img/placeholder_poster.svg";
        if (img && img.getAttribute("src") !== src) img.setAttribute("src", src);
      });
    }
  }

  /*  Genre extraction  */
  const extractGenres = it => {
    const meta = peekMeta(it);
    const srcs = [it.genres, it.genre, it.detail?.genres, it.meta?.genres, it.meta?.detail?.genres, meta?.genres, meta?.detail?.genres].filter(Boolean);
    return srcs.flatMap(s => Array.isArray(s)
      ? s.map(g => typeof g === "string" ? g : (g?.name || g?.title || g?.slug || ""))
      : String(s).split(/[|,\/]/)
    ).map(v => String(v || "").trim()).filter(Boolean);
  };

  function getDerived(it){
    const k = normKey(it);
    let d = derivedCache.get(k);
    if (d && !peekMeta(it)) return d;

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

  /* Provider chips */
  const providerLogoPath = name => window.CW?.ProviderMeta?.logoPath?.(name) || "";

  function providerSelectOptionData(value, option, allLabel = "All", showAllIcon = true) {
    const raw = String(value || "").trim();
    const isAll = !raw || raw.toUpperCase() === "ALL";
    const label = isAll ? allLabel : providerLabel(raw);
    const iconSrc = isAll ? "" : providerLogoPath(raw);
    const icons = iconSrc
      ? [{ src: iconSrc, alt: `${label} logo` }]
      : (isAll && !showAllIcon ? [] : [{ text: isAll ? "ALL" : providerShortLabel(raw) }]);
    return {
      label: String(option?.textContent || label).trim() || label,
      icons,
    };
  }

  function enhanceProviderFilterSelect() {
    const enhance = window.CW?.IconSelect?.enhance;
    if (!enhance || !providerSel) return;
    enhance(providerSel, {
      className: "wl-provider-filter-select",
      getOptionData: (value, option) => providerSelectOptionData(value, option, "All", false),
    });
  }

  function syncDeleteProviderDisabled() {
    const wrap = enhancedControlWrap(delProv);
    const btn = wrap?.querySelector?.(".cw-icon-select-btn");
    if (!btn) return;
    btn.disabled = !!delProv?.disabled;
    wrap.classList.toggle("is-disabled", !!delProv?.disabled);
  }

  function enhanceDeleteProviderSelect() {
    const enhance = window.CW?.IconSelect?.enhance;
    if (!enhance || !delProv) return;
    enhance(delProv, {
      className: "wl-action-provider-select",
      getOptionData: (value, option) => providerSelectOptionData(value, option, "ALL (default)", false),
    });
    syncDeleteProviderDisabled();
  }
  enhanceProviderFilterSelect();
  enhanceDeleteProviderSelect();

  const providerChip = (name, state = "ok") => {
    const label = providerLabel(name);
    const shortLabel = providerShortLabel(name);
    const src = providerLogoPath(name), icon = state === "ok" ? "check_small" : "remove";
    return `<span class="wl-mat ${state}" title="${esc(label)} ${state === "ok" ? "present" : "missing"}">${src ? `<img src="${src}" alt="${esc(label)}">` : `<span class="wl-badge">${esc(shortLabel)}</span>`}<span class="material-symbol">${icon}</span></span>`;
  };
  const posterProviderIcon = name => {
    const label = providerLabel(name);
    const shortLabel = providerShortLabel(name);
    const src = providerLogoPath(name);
    return src
      ? `<span class="wl-provider-icon" title="${esc(label)}"><img src="${src}" alt="${esc(label)} logo"></span>`
      : `<span class="wl-provider-icon" title="${esc(label)}"><span class="wl-badge">${esc(shortLabel)}</span></span>`;
  };
  const providerMatrix = have => `<div class="wl-matrix">${PROVIDERS.map(p => activeProviders.has(p) ? providerChip(p, have.has(p) ? "ok" : "miss") : "").join("")}</div>`;
  const mapProvidersByKey = list => new Map(list.map(it => [normKey(it), new Set(providersOf(it))]).filter(([k]) => !!k));
  function updateMetrics() {
    const ORDER = PROVIDERS;

    const instsOf = (it, p) => {
      const sbp = it?.sources_by_provider || it?.sourcesByProvider || {};
      const arr = sbp?.[String(p || "").toLowerCase()];
      return Array.isArray(arr) ? arr.map(x => String(x || "").trim()).filter(Boolean) : [];
    };

    const visible = filtered.length;
    const hiddenLocal = items.reduce((n, it) => n + (hiddenSet.has(normKey(it)) ? 1 : 0), 0);
    const movies = filtered.filter(it => /^movie$/i.test(String(it?.type || ""))).length;
    const anime = filtered.filter(it => /^anime$/i.test(String(it?.type || ""))).length;
    const series = Math.max(0, visible - movies - anime);
    const active = ORDER.filter(p => activeProviders.has(p));
    const providerSlots = Math.max(active.length, 1);
    const syncDensity = visible
      ? Math.round(filtered.reduce((sum, it) => sum + providersOf(it).filter(p => activeProviders.has(p)).length, 0) / (visible * providerSlots) * 100)
      : 0;

    const stat = (label, value) => `<div class="wl-insight-stat"><span class="k">${label}</span><span class="v">${value}</span></div>`;

    const cards = active.map(p => {
      const count = filtered.reduce((n, it) => n + (providersOf(it).includes(p) ? 1 : 0), 0);
      const pct = visible ? Math.round((count / visible) * 100) : 0;
      const instSet = new Set();
      for (const it of filtered) {
        if (!providersOf(it).includes(p)) continue;
        for (const inst of instsOf(it, p)) instSet.add(inst);
      }
      const insts = [...instSet].filter(Boolean);
      insts.sort((a, b) => (a !== "default") - (b !== "default") || a.localeCompare(b));
      const hint = insts.length ? ` • ${esc(insts.slice(0, 2).join(", "))}${insts.length > 2 ? ` +${insts.length - 2}` : ""}` : "";
      const src = providerLogoPath(p);
      const label = providerLabel(p);
      const shortLabel = providerShortLabel(p);
      const brand = src
        ? `<span class="wl-provider-brand"><img src="${src}" alt="${esc(label)} logo"></span>`
        : `<span class="wl-provider-brand"><span class="wl-badge">${esc(shortLabel)}</span></span>`;
      const providerClass = `provider-${String(p || "").toLowerCase().replace(/[^a-z0-9_-]+/g, "-")}`;
      return `<div class="wl-provider-card ${providerClass} ${count ? "is-live" : "is-idle"}" data-provider="${esc(p)}" title="${esc(`${label}: ${count}/${visible || 0}`)}">
        <div class="wl-provider-top">${brand}<strong>${count}</strong></div>
        <div class="wl-provider-name">${esc(label)}</div>
        <div class="wl-provider-sub">${pct}% coverage${hint}</div>
      </div>`;
    }).join("");

    metricsEl.innerHTML = `<div class="wl-insight">
      <div class="wl-insight-hero">
        <div class="wl-insight-score"><strong>${syncDensity}%</strong><span>Sync density</span></div>
        <div class="wl-insight-stats">
          ${stat("Visible", visible)}
          ${stat("Movies", movies)}
          ${stat("Series", series + anime)}
          ${stat("Hidden", hiddenLocal)}
        </div>
      </div>
      <div class="wl-provider-grid">${cards || '<div class="wl-provider-card is-idle"><div class="wl-provider-name">No active sources</div><div class="wl-provider-sub">Connect a source to see coverage.</div></div>'}</div>
    </div>`;
  }

  /* Sorting */
  const _t = it => String(it.title || "").toLowerCase();
  const _type = it => ((it.type || "").toLowerCase() === "show" ? "tv" : String(it.type || "").toLowerCase());
  const idValue = (it, name) => String(name === "tmdb" ? (it?.tmdb || it?.ids?.tmdb || "") : (it?.ids?.[name] || "")).trim();
  const addedValue = it => Number(it?.added_epoch || 0) || (Date.parse(String(it?.added_when || "")) || 0);

  function sortFilteredForList(arr) {
    const byTitle = (a, b) => cmp(_t(a), _t(b));
    const byText = key => (a, b) => cmpDir(cmp(idValue(a, key).toLowerCase(), idValue(b, key).toLowerCase()) || byTitle(a, b));
    const sorters = {
      key: (a, b) => cmpDir(cmp(normKey(a).toLowerCase(), normKey(b).toLowerCase()) || byTitle(a, b)),
      title: (a, b) => cmpDir(byTitle(a, b)),
      type: (a, b) => cmpDir(cmp(_type(a), _type(b))),
      year: (a, b) => cmpDir((Number(a?.year || yearFromIso(getReleaseIso(a)) || 0) - Number(b?.year || yearFromIso(getReleaseIso(b)) || 0)) || byTitle(a, b)),
      rel: (a, b) => {
        const unk = sortDir === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
        const ta = parseReleaseDate(getReleaseIso(a)), tb = parseReleaseDate(getReleaseIso(b));
        const va = ta ? ta.getTime() : unk, vb = tb ? tb.getTime() : unk;
        const diff = va - vb || byTitle(a, b);
        return cmpDir(diff);
      },
      genre: (a, b) => {
        const ga = (extractGenres(a)[0] || "").toLowerCase();
        const gb = (extractGenres(b)[0] || "").toLowerCase();
        const sentinel = "\uFFFF";
        const va = ga || (sortDir === "asc" ? sentinel : "");
        const vb = gb || (sortDir === "asc" ? sentinel : "");
        const diff = cmp(va, vb) || byTitle(a, b);
        return cmpDir(diff);
      },
      sync: (a, b) => {
        const ca = providersOf(a).length, cb = providersOf(b).length;
        return cmpDir((ca === cb ? cmp(String(a.title || ""), String(b.title || "")) : (ca - cb)));
      },
      tmdb: byText("tmdb"),
      imdb: byText("imdb"),
      tvdb: byText("tvdb"),
      trakt: byText("trakt"),
      simkl: byText("simkl"),
      anilist: byText("anilist"),
      mal: byText("mal"),
      added: (a, b) => cmpDir((addedValue(a) - addedValue(b)) || byTitle(a, b)),
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
    updateSortHeaderUI();
  }

  /* Filtering */
  const applyOverlayPrefUI = () => {
    const off = prefs.overlays === "no";
    postersEl.classList.toggle("wl-hide-overlays", off);
    if (off) forceHideDetail();
    const show = viewMode === "posters";
    if (overlaysLabel) overlaysLabel.style.display = show ? "" : "none";
    setControlVisible(overlaysSel, show);
  };


const normReleased = v => (v === "yes" ? "released" : v === "no" ? "unreleased" : "both");

  function applyFilters() {
    currentPage = 1;
    const q = (qEl.value || "").toLowerCase().trim();
    const ty = (tEl.value || "").trim();
    const provider = (providerSel.value || "").toUpperCase();
    const releasedPref = normReleased(releasedSel?.value || prefs.released || "both");
    const genrePref = (genreSel?.value || prefs.genre || "").trim().toLowerCase();
    const todayUTC = Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate());

    filtered = items.filter(it => {
      const key = normKey(it);
      if (hiddenSet.has(key) && !showHiddenChk?.checked) return false;

      const title = String(it.title || "").toLowerCase();
      const haystack = [
        title,
        normKey(it),
        it.year,
        typeLabelFor(it),
        providersOf(it).map(p => `${p} ${providerLabel(p)}`).join(" "),
        Object.entries(it.ids || {}).map(([k, v]) => `${k}:${v}`).join(" "),
      ].join(" ").toLowerCase();
      const rawType = String(it.type || "").toLowerCase();
      const t = (rawType === "show" || rawType === "shows" || rawType === "series") ? "tv" : rawType;

      if (q && !haystack.includes(q)) return false;
      if (ty && t !== ty) return false;
      if (provider && !providersOf(it).includes(provider)) return false;
      if (provider === "CROSSWATCH") {
        const inst = String(cwProfileSel?.value || "default").trim() || "default";
        const insts = instancesOfProvider(it, "CROSSWATCH");
        if (insts.length ? !insts.includes(inst) : inst !== "default") return false;
      }

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

  function backdropFromMeta(it, meta){
  const tmdb = it?.tmdb || it?.ids?.tmdb || meta?.ids?.tmdb;
  if (!tmdb) return "";
  const type = artType(it);
  return `/art/tmdb/${type}/${encodeURIComponent(String(tmdb))}?kind=backdrop&size=w1280&locale=${encodeURIComponent(window.__CW_LOCALE||navigator.language||"en-US")}${artEvidence(it)}`;
}

  let detailCard = null;
  let detailItem = null;
  let detailMeta = null;

  function ensureDetailCard() {
    if (detailCard) return detailCard;
    detailCard = window.CW.PlayingCard.mount({
      id: "wl-detail-card",
      variant: "watchlist",
      tabScope: "watchlist",
      label: "Watchlist item details",
      width: "min(720px,calc(100vw - 396px))",
      onClose: forceHideDetail,
      onTrailer: () => { void window.CW?.Trailer?.openFor(detailItem, detailMeta); },
    });
    return detailCard;
  }

  const detailTmdbType = (it, meta) => {
    const resolved = String(meta?.resolved_type || resolvedTypeFor(it) || "").toLowerCase();
    return resolved ? (resolved === "movie" ? "movie" : "tv") : artType(it);
  };

  const detailTmdbUrl = (it, meta) => {
    const id = it?.tmdb || it?.ids?.tmdb || meta?.ids?.tmdb || "";
    return id ? `https://www.themoviedb.org/${detailTmdbType(it, meta)}/${encodeURIComponent(String(id))}` : "";
  };

  const detailImdbUrl = (it, meta) => {
    const raw = String(it?.ids?.imdb || meta?.ids?.imdb || "").trim();
    if (!raw) return "";
    const clean = raw.startsWith("tt") ? raw : `tt${raw}`;
    return `https://www.imdb.com/title/${encodeURIComponent(clean)}`;
  };

  function detailModel(it, meta) {
    const resolvedType = String(meta?.resolved_type || resolvedTypeFor(it) || "").toLowerCase();
    const isMovie = resolvedType ? resolvedType === "movie" : String(it.type || "").toLowerCase() === "movie";
    const year = String(it.year || meta?.year || yearFromIso(meta?.detail?.release_date || meta?.detail?.first_air_date || "") || "").trim();
    const chips = [
      { text: typeLabelFor(it) },
      { text: year },
      { text: window.CW.PlayingCard.fmt.runtimeLabel(meta?.runtime_minutes) },
      { text: meta?.certification || meta?.release?.cert || meta?.detail?.certification || "" },
    ];
    const rawScore = Number(meta?.score);
    const rawRating = Number(meta?.vote_average ?? meta?.detail?.vote_average);
    const ratingValue = Number.isFinite(rawRating)
      ? rawRating
      : Number.isFinite(rawScore) ? rawScore / 10 : null;

    return {
      title: it.title || meta?.title || "Unknown",
      year: "",
      isMovie,
      chips,
      overview: meta?.overview || meta?.detail?.overview || meta?.detail?.tagline || "No description available",
      poster: artUrl(it, "w342") || "/assets/img/placeholder_poster.svg",
      posterHref: detailTmdbUrl(it, meta),
      backdrop: backdropFromMeta(it, meta),
      information: meta && Object.keys(meta).length ? window.CW.PlayingCard.fmt.informationFor(meta, isMovie) : "loading",
      rating: { value: ratingValue, votes: meta?.vote_count ?? meta?.detail?.vote_count },
      sources: providersOf(it).map((p) => ({
        label: providerLabel(p),
        short: providerShortLabel(p),
        logo: providerLogoPath(p),
      })),
      links: [
        { href: detailTmdbUrl(it, meta), text: "TMDb" },
        { href: detailImdbUrl(it, meta), text: "IMDb" },
      ],
    };
  }

  function renderDetail(it, meta) {
    if (viewMode !== "posters" && viewMode !== "list") { forceHideDetail(); return; }
    const card = ensureDetailCard();
    detailItem = it;
    detailMeta = meta;
    card.render(detailModel(it, meta));
    card.show();
  }

  /* preview on hover */
  let activePreviewKey = null, previewTimer = 0;
  function forceHideDetail(){ detailCard?.hide(); activePreviewKey=null; }
  function showPreview(it, mode = viewMode){
    if (mode === "posters" && prefs.overlays === "no") return;
    if (viewMode !== mode) return;
    const k=normKey(it); activePreviewKey=k;
    clearTimeout(previewTimer);
    previewTimer = window.setTimeout(() => {
      getMetaFor(it, "detail").then(m=>{ if(activePreviewKey===k && viewMode === mode) renderDetail(it,m||{}); });
    }, 140);
  }
  function hidePreview(it, mode = viewMode){
    if (viewMode !== mode) return;
    clearTimeout(previewTimer);
    const k=normKey(it);
    if(!selected.has(k)&&activePreviewKey===k){ detailCard?.hide(); activePreviewKey=null; }
  }


  const _show = (el, on) => setControlVisible(el, on);

  function render() {
    const posters = viewMode === "posters";
    _show(postersEl, posters); _show(listWrapEl, !posters); _show(sizeInput, posters); _show(sizeLabel, posters);
    applyOverlayPrefUI();
    syncViewModeUI();
    syncPageSizeUI();
    syncColumnVisibilityUI();

    computePageInfo();

    if (!filtered.length) {
      empty.style.display = "";
      if (selAll) selAll.checked = false;
      if (listSelectAll) listSelectAll.checked = false;
      postersEl.innerHTML = ""; listBodyEl.innerHTML = ""; selCount.textContent = "0 selected"; metricsEl.innerHTML = "";
      if (pagerEl) pagerEl.style.display = "none";
      updateHeaderSummary();
      return;
    }

    empty.style.display = "none";
    posters ? renderPosters() : renderList();
    void hydrateVisibleMetadata();
    if (TMDB_OK) {
      void Promise.all(visibleItemsForHydration().slice(0, 2).map(it => getMetaFor(it, "detail").catch(() => null)));
    }
    selCount.textContent = `${selected.size} selected`;
    updatePaginationUI();
    updateHeaderSummary();
  }

  function renderPosters(){
    postersEl.replaceChildren();
    const frag=document.createDocumentFragment();
    const canTMDB=(typeof TMDB_OK==="undefined")?true:!!TMDB_OK;
    const readOnly = isProfileUser();

    const start = pageInfo.start;
    const end = pageInfo.end;
    const pageItems = filtered.slice(start, end);

    pageItems.forEach((it,i)=>{
      const key=normKey(it);
      const imgUrl=canTMDB ? artUrl(it,"w342") : "";
      const src=imgUrl || "/assets/img/placeholder_poster.svg";
      const d = getDerived(it);
      const typeLabel = typeLabelFor(it);
      const posterTypeLabel = posterTypeLabelFor(it);
      const relYear = String(it.year || yearFromIso(d.iso) || "").trim();
      const providerCount = providersOf(it).length;
      const card=document.createElement("div");
      card.className=`wl-card ${!readOnly && selected.has(key)?"selected":""}`;

      const provHtml = providersOf(it).map(p => posterProviderIcon(p)).join("");
      const eager=i<24?`loading="eager" fetchpriority="high"`:`loading="lazy"`;
      card.innerHTML=`
        <div class="wl-card-top">
          <div class="wl-provider-icons">${provHtml}</div>
          ${posterTypeLabel ? `<span class="wl-type-corner">${esc(posterTypeLabel)}</span>` : ""}
        </div>
        <img ${eager} decoding="async" src="${src}" alt="" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"/>
        <div class="wl-card-meta">
          <div class="wl-card-title">${esc(it.title || "Unknown")}</div>
          <div class="wl-card-sub">
            <span>${esc(relYear || typeLabel || "Queued")}</span>
            <span>${providerCount} sync</span>
          </div>
        </div>`;

      card.addEventListener("click",()=>{
        if (!isProfileUser()) {
          selected.has(key)?selected.delete(key):selected.add(key);
          card.classList.toggle("selected"); updateSelCount();
        }
        if(canTMDB) getMetaFor(it, "detail").then(m=>renderDetail(it,m||{})); else renderDetail(it,{});
      },true);

      card.addEventListener("mouseenter",()=>{ if(canTMDB) showPreview(it); },true);
      card.addEventListener("mouseleave",()=>hidePreview(it),true);

      frag.appendChild(card);
    });

    postersEl.appendChild(frag);
  }

  function renderList() {
    listBodyEl.replaceChildren();
    renderedRowRefs.clear();
    applyColumnLayout();
    const frag = document.createDocumentFragment();
    const sorted = sortFilteredForList(filtered);
    const canTMDB=(typeof TMDB_OK==="undefined")?true:!!TMDB_OK;
    const readOnly = isProfileUser();
    const start = pageInfo.start;
    const end = pageInfo.end;
    const rows = sorted.slice(start, end);
    const columns = visibleColumns();
    const addedLabel = it => {
      const epoch = Number(it?.added_epoch || 0);
      if (epoch > 0) return new Date(epoch * 1000).toISOString().slice(0, 10);
      const raw = String(it?.added_when || "").trim();
      if (!raw) return "";
      const ms = Date.parse(raw);
      return Number.isFinite(ms) ? new Date(ms).toISOString().slice(0, 10) : raw;
    };
    const textCell = (column, value, cls = "") => `<td class="wl-col-${column}${cls ? ` ${cls}` : ""}" data-col="${column}" title="${esc(value || "")}"><span class="wl-cell-text">${esc(value || "")}</span></td>`;

    rows.forEach(it => {
      const key = normKey(it), tr = document.createElement("tr");
      tr.dataset.key = key;
      renderedRowRefs.set(key, tr);
      const typeLabel = typeLabelFor(it);
      const thumb = artUrl(it, "w92") || "/assets/img/placeholder_poster.svg";
      const have = new Set(providersOf(it));
      const matrix = providerMatrix(have);
      const d = getDerived(it);

      const yearHint = String(it.year || yearFromIso(d.iso) || "").trim();
      const titleSub = [
        !isColVisible("year") && yearHint ? `<span class="wl-inline-pill">${esc(yearHint)}</span>` : "",
        !isColVisible("type") ? `<span class="wl-inline-pill">${esc(typeLabel)}</span>` : "",
      ].filter(Boolean).join("");
      const cellHtml = {
        poster: `<td class="wl-col-poster wl-poster-cell" data-col="poster" style="text-align:center"><img class="wl-mini" src="${thumb}" alt="" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"/></td>`,
        title: `<td class="wl-col-title title" data-col="title"><div class="wl-title-cell"><div class="wl-title-main">${esc(it.title || "")}</div>${titleSub ? `<div class="wl-title-sub">${titleSub}</div>` : ""}</div></td>`,
        year: textCell("year", yearHint),
        rel: textCell("rel", d.relFmt, "rel"),
        genre: textCell("genre", d.genresText, "genre"),
        type: `<td class="wl-col-type" data-col="type"><span class="wl-inline-pill">${esc(typeLabel)}</span></td>`,
        tmdb: textCell("tmdb", idValue(it, "tmdb"), "wl-id-cell"),
        imdb: textCell("imdb", idValue(it, "imdb"), "wl-id-cell"),
        tvdb: textCell("tvdb", idValue(it, "tvdb"), "wl-id-cell"),
        trakt: textCell("trakt", idValue(it, "trakt"), "wl-id-cell"),
        simkl: textCell("simkl", idValue(it, "simkl"), "wl-id-cell"),
        anilist: textCell("anilist", idValue(it, "anilist"), "wl-id-cell"),
        mal: textCell("mal", idValue(it, "mal"), "wl-id-cell"),
        sync: `<td class="wl-col-sync sync" data-col="sync">${matrix}</td>`,
        added: textCell("added", addedLabel(it)),
        key: textCell("key", key, "wl-key-cell"),
      };
      tr.innerHTML = `
        <td style="text-align:center"><input type="checkbox" name="wl-select" data-k="${key}" ${selected.has(key) ? "checked" : ""}></td>
        ${columns.map(column => cellHtml[column] || "").join("")}
      `;
      tr.classList.toggle("selected", !readOnly && selected.has(key));

      if (d.relFmt || d.genresText) hydrateRow(it, tr);
      const posterCell = tr.querySelector(".wl-poster-cell");
      const showFromCover = () => {
        if (canTMDB) showPreview(it, "list");
        else renderDetail(it, {});
      };
      const hideFromCover = e => {
        if (posterCell?.contains?.(e?.relatedTarget)) return;
        hidePreview(it, "list");
      };
      posterCell?.addEventListener("mouseenter", showFromCover, true);
      posterCell?.addEventListener("mouseleave", hideFromCover, true);
      posterCell?.addEventListener("focusin", showFromCover, true);
      posterCell?.addEventListener("focusout", hideFromCover, true);
      const rowCheckbox = tr.querySelector('input[type=checkbox]');
      const setRowSelected = checked => {
        if (isProfileUser()) return;
        checked ? selected.add(key) : selected.delete(key);
        tr.classList.toggle("selected", checked);
        if (rowCheckbox) rowCheckbox.checked = checked;
        if (listSelectAll) listSelectAll.checked = filtered.length > 0 && filtered.every(x => selected.has(normKey(x)));
        updateSelCount();
      };
      if (readOnly && rowCheckbox) rowCheckbox.disabled = true;
      rowCheckbox?.addEventListener("click", e => e.stopPropagation(), true);
      rowCheckbox?.addEventListener("change", e => setRowSelected(!!e.target.checked), true);
      tr.addEventListener("click", e => {
        if (isProfileUser()) return;
        if (e.target?.closest?.("input,button,a,select,textarea,label,.wl-resize")) return;
        setRowSelected(!selected.has(key));
      }, true);
      frag.appendChild(tr);
    });

    listBodyEl.appendChild(frag);
    if (listSelectAll) listSelectAll.checked = filtered.length > 0 && filtered.every(x => selected.has(normKey(x)));
    updateSortHeaderUI();
    applyColumnLayout();
  }

  let snackTimer = null;
  function setSnackContent(parts){
    snack.replaceChildren(...parts.map(part =>
      typeof part === "string" ? document.createTextNode(part) : part
    ));
  }

  function snackbar(message){
    clearTimeout(snackTimer); snackTimer = null;
    setSnackContent([String(message ?? "")]);
    snack.classList.remove("wl-hidden");
    snackTimer = setTimeout(() => (snack.classList.add("wl-hidden"), snackTimer = null), 1800);
  }

  function rebuildDeleteProviderOptions(){
    const byKey = mapProvidersByKey(items), union = new Set(), prev = delProv.value;
    for (const k of selected) byKey.get(k)?.forEach?.(p => union.add(p));
    delProv.innerHTML = deleteProviderOptions(union);
    if ([...delProv.options].some(o => o.value === prev)) delProv.value = prev;
    enhanceDeleteProviderSelect();
    syncCrosswatchProfileControls();
  }

  function updateSelCount(){
    if (isProfileUser()) selected.clear();
    selCount.textContent = `${selected.size} selected`;
    selCount.classList.toggle("is-accent", selected.size > 0);
    rebuildDeleteProviderOptions();
    document.getElementById("wl-delete").disabled = isProfileUser() || !(delProv.value && selected.size);
    document.getElementById("wl-hide").disabled = isProfileUser() || selected.size === 0;
    updateHeaderSummary();
  }

  const deleteSpecsForKeys = keys => keys.map(key => {
    const aliases = items.find(it => normKey(it) === key)?.aliases;
    return Array.isArray(aliases) && aliases.some(Boolean)
      ? { key, aliases: aliases.filter(Boolean) }
      : { key };
  });

  async function postDelete(keys, provider, providerInstance){
    const send = async prov => {
      const body = { keys: deleteSpecsForKeys(keys), provider: prov };
      if (providerInstance) body.provider_instance = providerInstance;
      const r = await fetch("/api/watchlist/delete", {
        method:"POST", headers:{ "Content-Type":"application/json" },
        body: JSON.stringify(body)
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
    const providerInstance = PROV_UP === "CROSSWATCH"
      ? String(delProfile?.value || cwProfileSel?.value || "default").trim() || "default"
      : "";
    const keys = [...selected];
    const total = keys.length, CHUNK = 50;

    delBtn.disabled = delProv.disabled = true;
    syncDeleteProviderDisabled();
    const progress = d => {
      const count = document.createElement("b");
      count.textContent = `${d}/${total}`;
      setSnackContent([
        "Deleting ",
        count,
        ` ${PROV_UP==="ALL" ? "across providers" : "from " + PROV_UP}...`,
      ]);
      snack.classList.remove("wl-hidden");
    };
    progress(0);

    let done = 0, ok = 0;
    for (let i = 0; i < keys.length; i += CHUNK) {
      const res = await postDelete(keys.slice(i, i + CHUNK), provider, providerInstance);
      ok += res.okCount || 0; done = Math.min(total, i + CHUNK); progress(done);
    }
    snack.classList.add("wl-hidden");
    delBtn.disabled = delProv.disabled = false;
    syncDeleteProviderDisabled();

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

  /* reference: event wiring */
  const on = (els, evts, fn, cap=true) => evts.forEach(e => els.forEach(el => el?.addEventListener(e, fn, cap)));
  const setPosterMin = px => {
    const min = Math.max(120, Math.min(320, Number(px) || 160));
    const badge = Math.max(20, Math.min(28, Math.round(min * 0.165)));
    const iconH = Math.max(11, Math.min(15, badge - 12));
    const typeH = Math.max(20, Math.min(24, Math.round(badge * 0.92)));
    const typeMin = Math.max(22, Math.min(28, Math.round(badge * 0.96)));
    postersEl.style.setProperty("--wl-min", `${min}px`);
    postersEl.style.setProperty("--wl-provider-badge", `${badge}px`);
    postersEl.style.setProperty("--wl-provider-icon-h", `${iconH}px`);
    postersEl.style.setProperty("--wl-type-pill-h", `${typeH}px`);
    postersEl.style.setProperty("--wl-type-pill-min", `${typeMin}px`);
  };

  ["pointerenter","pointerdown","focusin","mouseenter","touchstart"].forEach(ev =>
    sideEl?.addEventListener(ev, forceHideDetail, true)
  );

  qEl.addEventListener("input", applyFilters, true);
  on([tEl], ["change","input"], applyFilters);
  on([providerSel], ["change","input"], () => { syncCrosswatchProfileControls(); applyFilters(); });
  cwProfileSel?.addEventListener("change", applyFilters, true);
  cwProfileSel?.addEventListener("input", applyFilters, true);
  delProfile?.addEventListener("change", updateSelCount, true);

  moreBtn.addEventListener("click", () => {
    const open = morePanel.style.display !== "none";
    morePanel.style.display = open ? "none" : "";
    moreBtn.setAttribute("aria-expanded", String(!open));
    prefs.moreOpen = !open; writePrefs(prefs);
  }, true);

  on([releasedSel], ["change","input"], () => { prefs.released = normReleased(releasedSel.value); writePrefs(prefs); applyFilters(); });
  on([overlaysSel], ["change","input"], () => { prefs.overlays = overlaysSel.value || "yes"; writePrefs(prefs); applyOverlayPrefUI(); });
  on([genreSel], ["change","input"], () => { prefs.genre = genreSel.value || ""; writePrefs(prefs); applyFilters(); });
  showHiddenChk?.addEventListener("change", () => { prefs.showHidden = !!showHiddenChk.checked; writePrefs(prefs); applyFilters(); }, true);

  const selectAll = chk => { selected.clear(); if (!isProfileUser() && chk.checked) filtered.forEach(it => { const k = normKey(it); if (k) selected.add(k); }); };
  selAll?.addEventListener("change", () => { selectAll(selAll); (viewMode === "posters" ? renderPosters : renderList)(); updateSelCount(); }, true);
  listSelectAll?.addEventListener("change", () => { selectAll(listSelectAll); renderList(); updateSelCount(); }, true);

  window.addEventListener("cw:overview-profile-changed", () => {
    host.classList.toggle("wl-readonly", isProfileUser());
    if (isProfileUser()) selected.clear();
    render();
    updateSelCount();
  });

  clearBtn.addEventListener("click", () => {
    qEl.value = ""; tEl.value = ""; providerSel.value = "";
    if (cwProfileSel) cwProfileSel.value = "default";
    releasedSel.value = "both"; overlaysSel.value = "yes"; genreSel.value = "";
    if (showHiddenChk) showHiddenChk.checked = false;
    Object.assign(prefs, { released:"both", overlays:"yes", genre:"", showHidden:false }); writePrefs(prefs);
    applyOverlayPrefUI(); applyFilters();
  }, true);

  delProv.addEventListener("change", () => { syncCrosswatchProfileControls(); updateSelCount(); }, true);

  sizeInput.addEventListener("input", () => {
    const px = Math.max(120, Math.min(320, Number(sizeInput.value) || 150));
    setPosterMin(px); prefs.posterMin = px; writePrefs(prefs);
  }, true);

  pageSizeSel?.addEventListener("click", e => {
    e.preventDefault();
    openChoicePopup(
      pageSizeSel,
      PAGE_SIZE_OPTIONS.map(n => ({ value:n, label:String(n) })),
      pageSizeValue(),
      value => {
        const next = Number(value);
        prefs.pageSize = PAGE_SIZE_OPTIONS.includes(next) ? next : DEFAULT_PAGE_SIZE;
        currentPage = 1;
        syncPageSizeUI();
        writePrefs(prefs);
        render();
      }
    );
  }, true);

  host.querySelector("#wl-view-menu")?.addEventListener("click", e => {
    e.preventDefault();
    openChoicePopup(
      e.currentTarget,
      [{ value:"posters", label:"Posters" }, { value:"list", label:"List" }],
      viewMode,
      value => {
        const next = value === "list" ? "list" : "posters";
        if (viewMode === next) return;
        viewSel.value = next;
        viewSel.dispatchEvent(new Event("change", { bubbles: true }));
      }
    );
  }, true);

  columnsBtn?.addEventListener("click", () => openColumnPopup(columnsBtn), true);
  wideBtn?.addEventListener("click", () => {
    prefs.wideView = !prefs.wideView;
    closePopup();
    syncWideViewUI();
    writePrefs(prefs);
  }, true);

  window.addEventListener("resize", () => {
    window.clearTimeout(columnLayoutResizeTimer);
    columnLayoutResizeTimer = window.setTimeout(applyColumnLayout, 80);
  });

  document.addEventListener("keydown", e => {
    if (e.key === "Delete" && !isProfileUser() && !document.getElementById("wl-delete").disabled) document.getElementById("wl-delete").click();
    if (e.key === "Escape" && !document.getElementById("cw-trailer")?.classList.contains("show")) forceHideDetail();
  }, true);

  viewSel.addEventListener("change", () => {
    viewMode = viewSel.value === "list" ? "list" : "posters";
    prefs.view = viewMode; writePrefs(prefs);
    forceHideDetail();
    closePopup();
    syncViewModeUI();
    render();
  });

  pagerPrev?.addEventListener("click", () => {
    if (currentPage > 1) {
      currentPage--;
      render();
    }
  }, true);

  pagerNext?.addEventListener("click", () => {
    const total = filtered.length;
    if (!total) return;
    const maxPage = Math.ceil(total / pageSizeValue());
    if (currentPage < maxPage) {
      currentPage++;
      render();
    }
  }, true);

  async function hardReloadWatchlist(){
    try{ items=await fetchWatchlist(); addActiveProvidersFromItems(items); rebuildProviderOptions(); populateGenreOptions(buildGenreIndex(items)); applyFilters(); rebuildDeleteProviderOptions(); }
    catch(e){ console.warn("watchlist reload failed:", e); }
  }
  function _wlBusy(on){ const b=document.getElementById("wl-refresh"); if(!b)return; b.disabled=!!on; b.classList.toggle("loading",!!on); b.classList.toggle("spin",!!on); }
  document.getElementById("wl-refresh")?.addEventListener("click", async()=>{ if(hardReloadWatchlist._busy)return; hardReloadWatchlist._busy=true; _wlBusy(true); try{ await hardReloadWatchlist(); } finally{ _wlBusy(false); hardReloadWatchlist._busy=false; } }, {passive:true});
  window.Watchlist=Object.assign(window.Watchlist||{}, { refresh: hardReloadWatchlist });
  window.addEventListener("watchlist:refresh", hardReloadWatchlist);

  let authRetryWired = false;
  let watchlistInitStarted = false;

  function retryInitAfterAuth() {
    if (authRetryWired) return;
    authRetryWired = true;
    const retry = () => {
      if (authSetupPending()) return;
      authRetryWired = false;
      window.removeEventListener("cw-auth-setup-pending", onAuth);
      initWatchlist();
    };
    const onAuth = e => { if (e?.detail?.pending === false) retry(); };
    window.addEventListener("cw-auth-setup-pending", onAuth);
    Promise.resolve(window.__cwAuthBootstrapPromise).catch(() => null).finally(retry);
  }

  async function initWatchlist(){
    if (watchlistInitStarted) return;
    if (authSetupPending()) return retryInitAfterAuth();
    watchlistInitStarted = true;
    viewSel.value = viewMode;
    syncViewModeUI();
    syncPageSizeUI();
    syncWideViewUI();
    syncColumnVisibilityUI();
    sizeInput.value = String(prefs.posterMin); setPosterMin(prefs.posterMin);
    releasedSel.value = prefs.released; overlaysSel.value = prefs.overlays; morePanel.style.display = prefs.moreOpen ? "" : "none";
    moreBtn.setAttribute("aria-expanded", String(!!prefs.moreOpen));
    if (showHiddenChk) showHiddenChk.checked = !!prefs.showHidden;

    const cfg = await fetchConfig();
    window.__CW_LOCALE = (cfg?.metadata?.locale || cfg?.ui?.locale || window.__CW_LOCALE || navigator.language || "en-US");
    const active = new Set();
    try {
      if (typeof window.getConfiguredProviders === "function") {
        for (const key of window.getConfiguredProviders(cfg || {})) active.add(providerKey(key));
      } else {
        if ((cfg?.crosswatch || cfg?.CrossWatch || {}).enabled !== false) active.add("CROSSWATCH");
        if (cfg?.plex?.account_token) active.add("PLEX");
        if (cfg?.simkl?.access_token) active.add("SIMKL");
        const anTok = cfg?.anilist?.access_token || cfg?.anilist?.token || cfg?.auth?.anilist?.access_token || cfg?.auth?.anilist?.token;
        if (anTok) active.add("ANILIST");
        if (cfg?.trakt?.access_token) active.add("TRAKT");
        if (cfg?.tmdb_sync?.api_key && cfg?.tmdb_sync?.session_id) active.add("TMDB");
        if (cfg?.jellyfin?.access_token) active.add("JELLYFIN");
        if (cfg?.emby?.access_token || cfg?.emby?.api_key || cfg?.emby?.token) active.add("EMBY");
        if (cfg?.mdblist?.api_key || cfg?.mdblist?.access_token) active.add("MDBLIST");
        if (cfg?.publicmetadb?.api_key) active.add("PUBLICMETADB");
        if (cfg?.nuvio?.access_token || cfg?.auth?.nuvio?.access_token || cfg?.nuvio?.instances) active.add("NUVIO");
        if (cfg?.stremio?.auth_key || cfg?.stremio?.authKey || cfg?.auth?.stremio?.auth_key || cfg?.auth?.stremio?.authKey) active.add("STREMIO");
        if ([cfg?.floppy, cfg?.auth?.floppy].some(hasFloppyConfig)) active.add("FLOPPY");
      }
    } catch {}

    activeProviders = active;
    await loadCrosswatchProfiles();
    rebuildProviderOptions();
    items = await fetchWatchlist();
    addActiveProvidersFromItems(items);
    rebuildProviderOptions();
    populateGenreOptions(buildGenreIndex(items));
    applyOverlayPrefUI(); applyFilters(); rebuildDeleteProviderOptions(); wireSortableHeaders(); updateHeaderSummary();

    window.dispatchEvent(new CustomEvent("watchlist-ready"));
  }

  initWatchlist();
})();
