/* assets/js/tracker.js */
/* CrossWatch - Local tracker */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const FEATURES = [
    { id: "history", label: T("profile.history.title"), icon: "history", date: T("tracker.dates.watched") },
    { id: "watchlist", label: T("common.media.watchlist"), icon: "bookmark", date: "" },
    { id: "ratings", label: T("profile.ratings.title"), icon: "star", date: T("tracker.dates.rated") },
    { id: "progress", label: T("profile.playback.title"), icon: "play_circle", date: T("tracker.dates.lastPlayed") },
    { id: "collection", label: T("profile.tabs.collection"), icon: "video_library", date: T("tracker.dates.collected") },
  ];
  const VIEWS = [
    { id: "overview", label: T("profile.tabs.overview"), icon: "space_dashboard" },
    { id: "watchlist", label: T("common.media.watchlist"), icon: "bookmark" },
    { id: "history", label: T("profile.history.title"), icon: "history" },
    { id: "ratings", label: T("profile.ratings.title"), icon: "star" },
    { id: "progress", label: T("profile.playback.title"), icon: "play_circle" },
    { id: "collection", label: T("profile.tabs.collection"), icon: "video_library" },
    { id: "shows", label: T("common.media.shows"), icon: "tv" },
    { id: "movies", label: T("common.media.movies"), icon: "movie" },
    { id: "watched", label: T("tracker.views.historyByTitle"), icon: "history" },
    { id: "titles", label: T("tracker.views.myTitles"), icon: "bookmarks" },
    { id: "cleanup", label: T("tracker.views.cleanUp"), icon: "health_and_safety" },
  ];
  const MEDIA = [
    { id: "", label: T("profile.types.all"), icon: "" },
    { id: "movie", label: T("common.media.movies"), icon: "movie" },
    { id: "show", label: "TV", icon: "tv" },
  ];
  const HEALTH = [
    { id: "attention", label: T("tracker.health.needAttention"), icon: "report" },
    { id: "unchecked", label: T("tracker.health.notChecked"), icon: "help" },
    { id: "ok", label: T("tracker.health.matchTmdb"), icon: "verified" },
  ];
  const STATUSES = [
    { id: "watching", label: T("tracker.statuses.watching"), icon: "play_circle" },
    { id: "planned", label: T("tracker.statuses.planned"), icon: "schedule" },
    { id: "completed", label: T("tracker.statuses.completed"), icon: "task_alt" },
    { id: "on_hold", label: T("tracker.statuses.onHold"), icon: "pause_circle" },
    { id: "dropped", label: T("tracker.statuses.dropped"), icon: "do_not_disturb_on" },
  ];
  const FIELD_TYPES = [
    { id: "text", label: T("tracker.fieldTypes.text") },
    { id: "number", label: T("tracker.fieldTypes.number") },
    { id: "date", label: T("tracker.fieldTypes.date") },
    { id: "bool", label: T("tracker.fieldTypes.bool") },
    { id: "choice", label: T("tracker.fieldTypes.choice") },
  ];
  const NOTE_LABELS = { status: "status", status_control: "automatic status preference", started: "start date", finished: "finish date", notes: "notes", tags: "tags", fields: "your fields", title: "title", year: "year", tmdb: "match" };
  const NOTES_VIEW = { label: T("tracker.views.titleDetails"), icon: "edit_note" };
  const MAX_TAGS = 30;
  const MAX_FIELDS = 20;
  const PAGE_SIZES = [50, 100, 150, 200];
  const MOBILE_PAGE_SIZES = [10, 20, 50];
  const TIMELINE_PAGE = 200;
  const ID_FIELDS = [
    { id: "imdb", label: "IMDb" },
    { id: "tmdb", label: "TMDB" },
    { id: "tvdb", label: "TVDB" },
    { id: "trakt", label: "Trakt" },
    { id: "simkl", label: "SIMKL" },
    { id: "mal", label: "MAL" },
    { id: "anilist", label: "AniList" },
    { id: "kitsu", label: "Kitsu" },
    { id: "anidb", label: "AniDB" },
  ];
  const BASE_IDS = ["imdb", "tmdb", "tvdb", "trakt"];
  const FIELD_LABELS = {
    watched_at: "watched date", rated_at: "rated date", progress_at: "played date", collected_at: "collected date",
    rating: "rating", progress_percent: "progress", progress_ms: "progress", ids: T("tracker.fieldNames.ids"), show_ids: T("tracker.fieldNames.showIds"),
    title: "title", series_title: "title", year: "year", series_year: "year", type: "type", season: "season", episode: "episode",
  };
  const ERRORS = {
    sync_running: T("tracker.errors.syncRunning"),
    changed_since: T("tracker.errors.changedSince"),
    already_undone: T("tracker.errors.alreadyUndone"),
    not_undoable: T("tracker.errors.notUndoable"),
    key_exists: T("tracker.errors.keyExists"),
    not_found: T("tracker.errors.notFound"),
    invalid_date: T("tracker.errors.invalidDate"),
    invalid_rating: T("tracker.errors.invalidRating"),
    invalid_progress: T("tracker.errors.invalidProgress"),
    invalid_ids: T("tracker.errors.invalidIds"),
    invalid_title: T("tracker.errors.invalidTitle"),
    invalid_year: T("tracker.errors.invalidYear"),
    invalid_type: T("tracker.errors.invalidType"),
    invalid_episode: T("tracker.errors.invalidEpisode"),
    no_changes: T("tracker.errors.noChanges"),
    already_there: T("tracker.errors.alreadyThere"),
    too_many: T("tracker.errors.tooMany"),
    tracker_unreadable: T("tracker.errors.unreadable"),
    check_running: T("tracker.errors.checkRunning"),
    tracker_migrating: T("tracker.errors.migrating"),
    pair_scoped: T("tracker.errors.pairScoped"),
    tmdb_key_missing: T("tracker.errors.tmdbKeyMissing"),
    tmdb_key_invalid: T("tracker.errors.tmdbKeyInvalid"),
    tmdb_unreachable: T("tracker.errors.tmdbUnreachable"),
    import_running: T("tracker.errors.importRunning"),
    profile_scope_denied: T("tracker.errors.profileScope"),
    unknown_source: T("tracker.errors.unknownSource"),
    unknown_instance: T("tracker.errors.unknownInstance"),
    same_instance: T("tracker.errors.sameInstance"),
    unsupported_feature: T("tracker.errors.unsupportedFeature"),
    profile_limit_reached: T("tracker.errors.profileLimit"),
    invalid_status: T("tracker.errors.invalidStatus"),
    invalid_note_date: T("tracker.errors.invalidNoteDate"),
    finished_before_started: T("tracker.errors.finishedBeforeStarted"),
    invalid_tags: T("tracker.errors.invalidTags"),
    too_many_tags: T("tracker.errors.tooManyTags", { tags: MAX_TAGS }),
    notes_too_long: T("tracker.errors.notesTooLong"),
    invalid_field_value: T("tracker.errors.invalidFieldValue"),
    invalid_field: T("tracker.errors.invalidField"),
    too_many_fields: T("tracker.errors.tooManyFields", { fields: MAX_FIELDS }),
    invalid_note_key: T("tracker.errors.invalidNoteKey"),
  };
  const IMPORT_SEEN_KEY = "cw.tracker.import.seen";
  const STORE_KEY = "cw.tracker.view.v3";
  const NARROW = "(max-width: 1320px)";

  const state = {
    root: null,
    instance: "default",
    instances: [],
    counts: {},
    health: {},
    effects: {},
    stats: null,
    view: "overview",
    monthlyReview: false,
    viewSelectedAt: 0,
    overview: null,
    listHero: null,
    overviewLimits: { continue: 1, recent: 1 },
    overviewRecentStep: 10,
    overviewBusy: "",
    layouts: {},
    historyGroup: "watch",
    historyMedia: "",
    historyTitleKeys: {},
    cleanFeature: "history",
    media: "",
    byDay: false,
    hideDropped: false,
    showStatusLabels: true,
    statusMenu: false,
    month: "",
    months: [],
    zoom: 0,
    healthFilter: "attention",
    sort: "recent",
    q: "",
    pageSize: 50,
    mobilePageSize: 20,
    offset: 0,
    items: [],
    shows: [],
    noteRows: [],
    notes: { total: 0, statuses: {}, tags: [] },
    noteFields: [],
    duplicates: false,
    filtersOpen: false,
    noteStatus: "",
    noteTag: "",
    total: 0,
    loading: false,
    error: "",
    request: 0,
    wired: false,
    picked: new Set(),
    open: new Set(),
    progress: {},
    ref: null,
    title: null,
    titleRequest: 0,
    seasonOpen: new Set(),
    seasonsExpanded: false,
    playsExpanded: false,
    check: null,
    imp: null,
    tmdbKey: true,
    fromLink: false,
    moving: false,
    migration: null,
  };
  const sheet = { el: null, mode: "", item: null, feature: "history", busy: false, error: "", keys: [], changes: [], changesTotal: 0, changesAll: 0, changeKind: "", changeQ: "", lastFocus: null, token: 0, log: null };

  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const symbol = (name, cls = "") => `<span class="material-symbol${cls ? ` ${cls}` : ""}" aria-hidden="true">${esc(name)}</span>`;
  const featureOf = (id) => FEATURES.find((f) => f.id === id) || FEATURES[0];
  const pad = (n) => String(n).padStart(2, "0");
  const num = (n) => Number(n || 0).toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale);
  const tabCountFormat = new Intl.NumberFormat((window.CW?.I18n || window.__cwI18n)?.locale || "en", {notation:"compact", maximumFractionDigits:1});
  const PLURAL_KEYS = { change: "change", copy: "copy", day: "day", episode: "episode", error: "error", minute: "minute", "more entry": "moreEntry", "more play": "morePlay", "more season": "moreSeason", "episode play": "episodePlay", movie: "movie", other: "other", play: "play", "record needs": "recordNeeds", "record was": "recordWas", record: "record", season: "season", show: "show", "synced copy": "syncedCopy", title: "title", watch: "watch" };
  const plural = (n, one, many) => PLURAL_KEYS[one] ? T(`tracker.count.${PLURAL_KEYS[one]}`, { count: Number(n) || 0 }) : `${num(n)} ${Number(n) === 1 ? one : many}`;
  const providerName = (p) => window.CW?.ProviderMeta?.label?.(p) || String(p || "");
  const mobile = () => !!window.matchMedia?.("(max-width:760px)")?.matches;
  const pageSize = () => mobile() ? state.mobilePageSize : state.pageSize;
  const overviewStep = kind => kind === "recent" ? state.overviewRecentStep : mobile() ? 3 : 4;
  const narrow = () => !!window.matchMedia?.(NARROW)?.matches;
  const listFeature = () => (state.view === "cleanup" ? state.cleanFeature : FEATURES.some((f) => f.id === state.view) ? state.view : "history");
  const se = (s, e) => `S${pad(s)}E${pad(e)}`;
  const inst = () => `provider_instance=${encodeURIComponent(state.instance)}`;
  const historyView = () => ["history", "movies", "shows", "watched"].includes(state.view);
  const groupedHistory = () => ["movies", "shows", "watched"].includes(state.view);
  const layoutView = () => groupedHistory() ? "watched" : state.view;
  const preferredHistoryView = () => state.historyGroup === "title" ? state.historyMedia === "movie" ? "movies" : state.historyMedia === "show" ? "shows" : "watched" : "history";
  const timelineOn = () => state.view === "history" && state.byDay;

  function loadView() {
    try {
      const saved = JSON.parse(localStorage.getItem(STORE_KEY) || "{}");
      if (VIEWS.some((v) => v.id === saved.view)) state.view = saved.view;
      state.historyGroup = ["movies", "shows", "watched"].includes(saved.view) || saved.historyGroup === "title" ? "title" : "watch";
      state.historyMedia = ["", "movie", "show"].includes(saved.historyMedia) ? saved.historyMedia : saved.view === "movies" ? "movie" : saved.view === "shows" ? "show" : saved.view === "history" && MEDIA.some(m => m.id === saved.media) ? saved.media : "";
      state.monthlyReview = saved.monthlyReview === true;
      state.viewSelectedAt = saved.viewSelectedAt;
      if (MEDIA.some((m) => m.id === saved.media)) state.media = saved.media;
      if (PAGE_SIZES.includes(saved.pageSize)) state.pageSize = saved.pageSize;
      if (MOBILE_PAGE_SIZES.includes(saved.mobilePageSize)) state.mobilePageSize = saved.mobilePageSize;
      state.layouts = Object.fromEntries(VIEWS.map(({ id }) => [id, saved.layouts?.[id] === "grid" || (id === "collection" && ["covers", "shelf"].includes(saved.layouts?.[id])) ? saved.layouts[id] : "list"]));
      if (!saved.layouts?.watched && ["movies", "shows"].includes(saved.view)) state.layouts.watched = saved.layouts?.[saved.view] === "grid" ? "grid" : "list";
      if (!saved.layouts && saved.layout === "grid") state.layouts[layoutView()] = "grid";
      state.byDay = saved.byDay === true;
      state.hideDropped = saved.hideDropped === true;
      state.showStatusLabels = saved.showStatusLabels !== false;
      if (Number.isInteger(saved.zoom)) state.zoom = saved.zoom;
      if (typeof saved.instance === "string" && saved.instance) state.instance = saved.instance;
      if (typeof saved.q === "string") state.q = saved.q.slice(0, 200);
      if (typeof saved.noteStatus === "string") state.noteStatus = saved.noteStatus.slice(0, 200);
      if (typeof saved.noteTag === "string") state.noteTag = saved.noteTag.slice(0, 200);
      if (typeof saved.cleanFeature === "string") state.cleanFeature = saved.cleanFeature.slice(0, 200);
      if (typeof saved.healthFilter === "string") state.healthFilter = saved.healthFilter.slice(0, 200);
      if (["recent", "oldest", "title"].includes(saved.sort)) state.sort = saved.sort;
      state.duplicates = saved.duplicates === true;
      state.offset = Number.isInteger(saved.offset) && saved.offset >= 0 ? saved.offset : 0;
      state.scrollTop = Number.isFinite(saved.scrollTop) && saved.scrollTop > 0 ? saved.scrollTop : 0;
      if (saved.ref && typeof saved.ref === "object" && ["note", "show", "key"].some(key => typeof saved.ref[key] === "string")) state.ref = saved.ref;
    } catch {}
    expireView();
  }

  function rememberView() {
    if (["history", "movies", "shows", "watched"].includes(state.view)) {
      state.historyGroup = state.view === "history" ? "watch" : "title";
      state.historyMedia = state.view === "movies" ? "movie" : state.view === "shows" ? "show" : state.view === "watched" ? "" : state.media;
    }
    state.viewSelectedAt = Date.now();
    saveView();
  }

  function expireView() {
    const age = Date.now() - state.viewSelectedAt;
    if (Number.isFinite(state.viewSelectedAt) && state.viewSelectedAt > 0 && age >= 0 && age < 60 * 60 * 1000) return false;
    state.view = 'overview';
    state.monthlyReview = false;
    state.viewSelectedAt = 0;
    state.ref = null;
    state.title = null;
    state.offset = 0;
    state.scrollTop = 0;
    state.picked.clear();
    return true;
  }

  function saveView() {
    try {
      localStorage.setItem(STORE_KEY, JSON.stringify({ view: state.view, historyGroup: state.historyGroup, historyMedia: state.historyMedia, viewSelectedAt: state.viewSelectedAt, monthlyReview: state.monthlyReview, layouts: state.layouts, media: state.media, byDay: state.byDay, hideDropped: state.hideDropped, showStatusLabels: state.showStatusLabels, zoom: state.zoom, pageSize: state.pageSize, mobilePageSize: state.mobilePageSize, instance: state.instance, q: state.q, sort: state.sort, noteStatus: state.noteStatus, noteTag: state.noteTag, cleanFeature: state.cleanFeature, healthFilter: state.healthFilter, duplicates: state.duplicates, offset: state.offset, ref: state.ref, scrollTop: state.scrollTop }));
    } catch {}
  }

  const mobileViewFields = ['q', 'media', 'sort', 'noteStatus', 'noteTag', 'healthFilter', 'cleanFeature', 'duplicates', 'offset', 'scrollTop', 'month'];
  let mobileHistoryView = null;
  function changeMobileView(wanted, back = false) {
    const ui = window.CW?.MobileUI;
    if (!ui?.mobile()) return false;
    clearTimeout(searchTimer);
    const search = state.root?.querySelector('[data-trk-input="q"]');
    if (search) state.q = search.value;
    const key = `tracker-sections.${overviewCacheKey()}`;
    const saved = ui.recall(key) || {};
    saved[state.view] = Object.fromEntries(mobileViewFields.map(field => [field, field === 'scrollTop' ? window.scrollY : state[field]]));
    ui.remember(key, saved);
    if (!back) {
      history.replaceState({...history.state, cwTrackerView:state.view}, '', location.href);
      history.pushState({...history.state, cwTrackerView:wanted}, '', location.href);
    }
    state.view = wanted;
    mobileHistoryView = wanted;
    Object.assign(state, {q:'', media:wanted === 'history' ? state.historyMedia : '', sort:'recent', noteStatus:'', noteTag:'', healthFilter:'attention', cleanFeature:'history', duplicates:false, offset:0, scrollTop:0, month:''});
    for (const field of mobileViewFields) if (typeof saved[wanted]?.[field] === typeof state[field]) state[field] = saved[wanted][field];
    state.filtersOpen = false;
    state.picked.clear();
    state.ref = null;
    state.title = null;
    rememberView();
    const top = state.scrollTop;
    const instance = state.instance;
    loadMonths();
    void loadMain();
    ui.restoreScroll(top, () => state.view === wanted && state.instance === instance && trackerVisible());
    return true;
  }

  async function getJSON(url, body, signal) {
    const init = body === undefined
      ? { cache: "no-store", credentials: "same-origin" }
      : { method: "POST", cache: "no-store", credentials: "same-origin", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) };
    const res = await fetch(url, {...init, signal});
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) {
      const detail = typeof data?.detail === "string" ? data.detail : "";
      const err = new Error(data?.error || detail || `HTTP ${res.status}`);
      err.code = data?.error || detail;
      throw err;
    }
    const path = new URL(url, location.origin).pathname;
    const mutation = body !== undefined && (path.startsWith("/api/tracker/") || path.startsWith("/api/provider-instances/"))
      && !path.endsWith("/preview") && (path !== "/api/tracker/titles/remove" || !!body.token);
    if (mutation) {
      window.CW?.ViewCache?.discard();
      try { sessionStorage.removeItem("cw.tracker.lists.v1"); sessionStorage.removeItem("cw.hub.lists.v1"); } catch {}
    }
    return data;
  }

  const trackerVisible = () => !document.hidden && !!state.root?.getClientRects().length;
  function getViewJSON(url, body, signal, options = {}) {
    if (!window.CW?.ViewCache || body !== undefined) return getJSON(url, body, signal);
    const instance = state.instance, view = state.view;
    const summary = /\/api\/tracker\/(summary|stats)\?/.test(url);
    return window.CW.ViewCache.read(url, next => getJSON(next, undefined, signal), {
      interval:300000, maxAge:300000, cacheScope:overviewCacheKey(),
      trackRevision:timelineOn() && url.startsWith("/api/tracker/items?"), ...options, signal, active:() => trackerVisible() && state.instance === instance && (summary || state.view === view),
    });
  }

  const errorText = (err) => ERRORS[err?.code] || T("tracker.errors.loadFailed");
  const writeError = (err) => ERRORS[err?.code] || err?.message || T("tracker.errors.saveFailed");

  function dateText(value) {
    if (!value) return "";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, { year: "numeric", month: "short", day: "numeric" });
  }

  function whenText(value) {
    if (!value) return "";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale, { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  }

  function timeText(value) {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? "" : d.toLocaleTimeString((window.CW?.I18n || window.__cwI18n)?.locale, { hour: "2-digit", minute: "2-digit" });
  }

  function dayLabel(value) {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return T("profile.records.unknownDate");
    const today = new Date();
    const diff = Math.round((new Date(today.getFullYear(), today.getMonth(), today.getDate()) - new Date(d.getFullYear(), d.getMonth(), d.getDate())) / 86400000);
    if (diff === 0) return T("common.time.today");
    if (diff === 1) return T("common.time.yesterday");
    return d.toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, { weekday: "long", day: "numeric", month: "long", year: d.getFullYear() === today.getFullYear() ? undefined : "numeric" });
  }

  function agoText(value) {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "";
    const mins = Math.round((Date.now() - d.getTime()) / 60000);
    if (mins < 1) return T("common.time.justNow");
    if (mins < 60) return T("tracker.ago.minutes", { count: mins });
    if (mins < 60 * 24) return T("tracker.ago.hours", { count: Math.round(mins / 60) });
    if (mins < 60 * 24 * 14) return T("tracker.ago.days", { count: Math.round(mins / 1440) });
    return dateText(value);
  }

  function localInput(value) {
    const d = new Date(value || "");
    if (!value || Number.isNaN(d.getTime())) return "";
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }

  function artUrl(item, size = "w342") {
    const art = item?.art;
    if (!art?.tmdb) return "";
    const title = item.series_title || item.title;
    const year = item.type === "movie" ? item.year : item.series_year || item.year;
    return `/art/tmdb/${art.kind}/${encodeURIComponent(art.tmdb)}?fallback=2&size=${size}${title ? `&title=${encodeURIComponent(title)}` : ""}${year ? `&year=${encodeURIComponent(year)}` : ""}${window.CW?.ArtChoice?.param?.(art.kind, art.tmdb) || ""}`;
  }

  const isChild = (item) => item?.type === "episode" || item?.type === "season";

  function cardTitle(item) {
    return isChild(item) ? item.series_title || item.title || item.key : item.title || item.series_title || item.key;
  }

  function listIdentity(item, content, date = "") {
    const image = artUrl(item, "w185");
    const meta = [episodeCode(item) || typeMeta(item).label, isChild(item) ? item.series_year : item.year, date].filter(Boolean).join(" · ");
    return `<div class="cw-title-cell trk-list-identity"><span class="trk-list-art" aria-hidden="true">${symbol(typeMeta(item).icon)}${image ? `<img src="${esc(image)}" alt="" loading="lazy" decoding="async">` : ""}</span><div class="trk-list-copy">${content}<span class="trk-list-meta">${esc(meta)}</span></div></div>`;
  }

  function episodeLabel(item) {
    if (item.type === "episode" && item.season != null && item.episode != null) return `S${pad(item.season)} · E${pad(item.episode)}`;
    if (item.type === "season" && item.season != null) return T("common.media.seasonN", { n: String(item.season) });
    return "";
  }

  function episodeCode(item) {
    if (item.type === "episode" && item.season != null && item.episode != null) return se(item.season, item.episode);
    return episodeLabel(item);
  }

  function cardSub(item) {
    const year = isChild(item) ? item.series_year : item.year;
    return [episodeLabel(item), year ? String(year) : ""].filter(Boolean).join(" · ");
  }

  function typeMeta(item) {
    const t = String(item?.type || "").toLowerCase();
    if (t === "movie") return { label: T("common.media.movie"), icon: "movie" };
    if (t === "episode") return { label: T("common.media.episode"), icon: "play_circle" };
    if (t === "season") return { label: T("common.media.season"), icon: "layers" };
    if (t === "anime") return { label: T("common.media.anime"), icon: "theater_comedy" };
    return { label: T("common.media.show"), icon: "tv" };
  }

  function typePill(item) {
    const t = typeMeta(item);
    return `<span class="cw-extra-display cw-type-display type-${esc(String(item?.type || "show").toLowerCase())}"><span class="cw-extra-display-label cw-extra-display-value">${esc(t.label)}</span>${symbol(t.icon, "cw-extra-display-icon")}</span>`;
  }

  const statusOf = (id) => STATUSES.find((s) => s.id === id) || null;

  function statusPill(id) {
    const s = statusOf(id);
    return s ? `<span class="trk-status" data-status="${esc(s.id)}">${symbol(s.icon)}<span>${esc(s.label)}</span></span>` : "";
  }

  function tagChips(tags, limit = MAX_TAGS) {
    const list = Array.isArray(tags) ? tags : [];
    if (!list.length) return "";
    const more = list.length > limit ? `<span class="trk-label is-more">+${esc(list.length - limit)}</span>` : "";
    return `<span class="trk-tags">${list.slice(0, limit).map((t) => `<span class="trk-label">${esc(t)}</span>`).join("")}${more}</span>`;
  }

  function dayText(value) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value || ""));
    if (!m) return dateText(value);
    return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])).toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, { year: "numeric", month: "short", day: "numeric" });
  }

  function today() {
    const d = new Date();
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  }

  function fieldText(field, value) {
    if (field.type === "bool") return value ? T("common.labels.yes") : T("common.labels.no");
    if (field.type === "date") return dayText(value);
    if (field.type === "number") return Number(value).toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale);
    return String(value);
  }

  function fieldOk(field, value) {
    if (value === null || value === undefined || value === "") return false;
    if (field.type === "choice") return (field.choices || []).includes(value);
    if (field.type === "number") return Number.isFinite(Number(value));
    if (field.type === "bool") return typeof value === "boolean";
    return true;
  }

  function showKeyOf(item) {
    const tmdb = item?.show_ids?.tmdb;
    return tmdb ? `tmdb:${tmdb}` : `title:${String(item?.series_title || "").toLowerCase()}`;
  }

  function checkedText(check) {
    if (!check?.checked_at) return "";
    const parts = [T("tracker.check.checkedAt", { time: whenText(check.checked_at) })];
    if (check.source === "cache" && check.fetched_at) parts.push(T("tracker.check.dataFrom", { date: dateText(check.fetched_at) }));
    return parts.join(" · ");
  }

  function checkInfo(item) {
    const c = item?.check;
    if (!c) return { state: "unchecked", icon: "help", title: T("tracker.check.notCheckedYet"), text: T("tracker.check.runCheckAll"), fix: [] };
    const tm = c.title ? `"${c.title}"${c.year ? ` (${c.year})` : ""}` : T("tracker.check.thisTitle");
    const what = c.kind === "movie" ? T("tracker.check.whatMovie") : T("tracker.check.whatShow");
    const spot = c.episode != null ? se(c.season, c.episode) : c.season != null ? T("tracker.check.seasonSpot", { n: String(c.season) }) : T("tracker.check.thisEpisode");
    switch (c.code) {
      case "no_tmdb":
        return { state: "attention", icon: "link_off", title: T("tracker.check.noMatchTitle"), text: T("tracker.check.noMatchText"), fix: ["map"] };
      case "not_found":
        return { state: "attention", icon: "search_off", title: T("tracker.check.notFoundTitle"), text: T("tracker.check.notFoundText", { what, id: String(c.tmdb) }), fix: ["map"] };
      case "mismatch":
        return { state: "attention", icon: "compare_arrows", title: T("tracker.check.mismatchTitle"), text: T("tracker.check.mismatchText", { what, id: String(c.tmdb), title: tm }), fix: ["map", "tmdb"] };
      case "incomplete":
        return { state: "attention", icon: "edit_note", title: T("tracker.check.incompleteTitle"), text: T("tracker.check.incompleteText", { title: tm }), fix: ["tmdb"] };
      case "episode_missing":
        return { state: "attention", icon: "playlist_remove", title: T("tracker.check.episodeMissingTitle"), text: T("tracker.check.episodeMissingText", { spot }), fix: ["map"] };
      case "episode_unverified":
        return { state: "attention", icon: "help", title: T("tracker.check.unverifiedTitle"), text: T("tracker.check.unverifiedText"), fix: ["map"] };
      case "ok":
        return { state: "ok", icon: "verified", title: T("tracker.check.matches"), text: "", fix: [] };
      default:
        return { state: "ok", icon: "do_not_disturb_on", title: T("tracker.check.notCheckedType"), text: T("tracker.check.nothingToCompare"), fix: [] };
    }
  }

  function healthIcon(item, always = false) {
    const info = checkInfo(item);
    if (!always && info.state !== "attention") return "";
    const label = [info.title, info.text, checkedText(item.check)].filter(Boolean).join(" · ");
    return `<span class="trk-health is-${info.state}" title="${esc(label)}" aria-label="${esc(info.title)}">${symbol(info.state === "attention" ? "report" : info.state === "ok" ? "verified" : "help")}</span>`;
  }

  function healthOf(feature = listFeature()) {
    return state.health?.[feature] || { ok: 0, attention: 0, unchecked: Number(state.counts[feature] || 0) };
  }

  function attentionTotal() {
    return FEATURES.reduce((sum, f) => sum + Number(state.health?.[f.id]?.attention || 0), 0);
  }

  function renderHero() {
    const s = state.stats || {};
    const hours = s.timed ? num(Math.round((s.minutes || 0) / 60)) : "–";
    return `
      <div class="cw-editor-hero-seg cw-editor-hero-count"><strong>${esc(num(s.week))}</strong><span>last 7 days</span></div>
      <div class="cw-editor-hero-seg cw-editor-hero-count"><strong>${esc(num(s.month))}</strong><span>last 30 days</span></div>
      <div class="cw-editor-hero-seg cw-editor-hero-count"><strong>${esc(num(s.year))}</strong><span>this year</span></div>
      <div class="cw-editor-hero-seg cw-editor-hero-count" title="${esc(s.timed ? T("tracker.ui.runtimeKnown", {known: num(s.timed), total: num(s.total)}) : T("tracker.ui.runtimeHint"))}"><strong>${esc(hours)}</strong><span>hours</span></div>
      <div class="cw-editor-hero-seg cw-editor-hero-count"><strong>${esc(num(s.streak))}</strong><span>day streak</span></div>
      <button class="cw-editor-refresh" type="button" data-trk-action="refresh" title="${TH("tracker.hero.refresh")}" aria-label="${TH("tracker.hero.refresh")}">${icon("refresh")}</button>`;
  }

  let profileScope = null;
  const currentProfileScope = () => String(window.CW?.OverviewProfile?.id || "");

  function renderProfilePicker() {
    const identity = state.root?.querySelector('[data-trk-slot="identity"]');
    const user = window.CW?.AuthState?.read?.()?.user;
    const name = String(user?.display_name || user?.label || user?.username || "");
    if (identity) {
      identity.hidden = !name;
      const signature = JSON.stringify([name, user?.avatar_url]);
      if (identity.dataset.signature !== signature) {
        identity.dataset.signature = signature;
        identity.innerHTML = `<a class="cw-media-hero-avatar" href="/profile" aria-label="${TH("profile.hero.openUserSettings")}" title="${TH("profile.hero.userSettings")}">${symbol("person")}${user?.avatar_url ? `<img src="${esc(user.avatar_url)}" alt="" onerror="this.hidden=true">` : ""}</a><span>${esc(name)}</span>`;
      }
    }
  }

  function viewCount(view) {
    if (view === "cleanup") return attentionTotal();
    if (view === state.view && !state.loading && ["history", "shows", "movies", "watched", "titles"].includes(view)) return state.total;
    if (view === "titles") return state.notes.total || null;
    if (["shows", "movies", "watched"].includes(view)) return null;
    return state.counts[view] ?? null;
  }

  function statusText() {
    if (state.loading && !state.items.length && !state.shows.length && !state.noteRows.length) return T("common.status.loading");
    if (state.view === "shows") return plural(state.total, "show", "shows");
    if (state.view === "movies") return plural(state.total, "movie", "movies");
    if (state.view === "watched") return plural(state.total, "title", "titles");
    if (state.view === "titles") return plural(state.total, "title", "titles");
    if (state.view === "history") return plural(state.total, "play", "plays");
    return plural(state.total, "record", "records");
  }

  function chip(attrs, label, { active = false, iconName = "", count = 0, title = "" } = {}) {
    return `<button type="button" class="cw-filter-chip${active ? " active" : ""}" ${attrs} aria-pressed="${active}"${title ? ` title="${esc(title)}"` : ""}>${iconName ? symbol(iconName, "cw-type-icon") : ""}<span>${esc(label)}</span>${count ? `<b class="trk-chip-count">${esc(num(count))}</b>` : ""}</button>`;
  }

  function navigationViews() {
    return VIEWS.filter(v => !["cleanup", "movies", "shows", "watched"].includes(v.id));
  }

  function renderTabs() {
    return navigationViews().map((v) => {
      const active = state.view === v.id || v.id === "history" && groupedHistory();
      const count = active ? viewCount(state.view) : 0;
      const badge = count ? `<small class="trk-tab-count" title="${esc(num(count))}" aria-label="${esc(num(count))}">${esc(tabCountFormat.format(count))}</small>` : "";
      return `<button type="button" role="tab" class="trk-tab${active ? " active" : ""}" data-trk-view="${v.id}" aria-selected="${active}" tabindex="${active || state.view === "cleanup" && v.id === "overview" ? 0 : -1}">${symbol(v.icon)}<span>${esc(v.label)}</span>${badge}</button>`;
    }).join("");
  }

  function renderNavigation() {
    const overflow = navigationViews().map(v => `<button type="button" class="cw-btn" data-trk-view="${v.id}" hidden>${symbol(v.icon)}<span>${esc(v.label)}</span>${viewCount(v.id) ? `<small>${esc(num(viewCount(v.id)))}</small>` : ""}</button>`).join("");
    const attention = attentionTotal();
    return `<div class="trk-navigation cw-page-navigation"><nav class="trk-tabs" role="tablist" aria-label="${TH("tracker.tabs.label")}">${renderTabs()}</nav><details class="trk-others trk-nav-menu${state.view === "cleanup" ? " is-active" : ""}"><summary aria-label="${TH("tracker.tabs.more")}" class="trk-tab${state.view === "cleanup" ? " active" : ""}">${symbol("more_horiz")}<span>${TH("dashboard.mobile.more")}</span>${attention ? `<small aria-label="${esc(plural(attention, "record needs", "records need"))} attention">${esc(num(attention))}</small>` : ""}${symbol("expand_more")}</summary><div class="trk-others-menu cw-page-panel"><div class="trk-overflow" hidden>${overflow}</div>${state.trackerOptions?.month_review !== false ? `<button type="button" class="cw-btn" data-trk-month-open>${symbol("auto_awesome")}<span>${TH("tracker.month.open")}</span></button>` : ""}${state.trackerOptions?.wrapped !== false ? `<button type="button" class="cw-btn" data-trk-wrapped-open>${symbol("celebration")}<span>${TH("tracker.wrapped.open")}</span></button>` : ""}${state.trackerOptions?.automatic_status !== false ? `<button type="button" class="cw-btn" data-trk-action="automation">${symbol("autorenew")}<span>${TH("tracker.automation.title")}</span></button>` : ""}<button type="button" class="cw-btn" data-trk-view="cleanup"${state.view === "cleanup" ? ' aria-current="page"' : ""}>${symbol("health_and_safety")}<span>${TH("tracker.views.cleanUp")}</span>${attention ? `<small>${esc(num(attention))}</small>` : ""}</button></div></details></div>`;
  }

  function fitNavigation() {
    const navigation = state.root?.querySelector(".trk-navigation");
    const controls = state.root?.querySelector('[data-trk-slot="controls"]');
    if (navigation && controls) {
      state.root.querySelector(".cw-media-hero").after(navigation);
    }
    const source = state.root?.querySelector('[data-trk-slot="source"]');
    const sourceHost = mobile() ? state.root?.querySelector('.trk-hero-topbar .cw-media-hero-brand') : navigation;
    if (source && sourceHost && source.parentNode !== sourceHost) sourceHost.append(source);
    source?.querySelector('summary')?.setAttribute('aria-haspopup', mobile() ? 'dialog' : 'menu');
    const tools = state.root?.querySelector('.trk-secondary-actions');
    if (tools) {
      if (mobile()) navigation?.querySelector('.trk-others-menu')?.append(tools);
      else state.root.querySelector('.trk-log-btn')?.before(tools);
    }
    const list = navigation?.querySelector(".trk-tabs");
    if (!list?.clientWidth) return;
    const tabs = [...list.querySelectorAll("[data-trk-view]")];
    const focused = document.activeElement;
    tabs.forEach(tab => {
      tab.hidden = false;
      tab.tabIndex = tab.getAttribute('aria-selected') === 'true' || state.view === 'cleanup' && tab.dataset.trkView === 'overview' ? 0 : -1;
    });
    if (mobile()) {
      tabs.forEach(tab => { tab.hidden = tab.dataset.trkView === 'titles'; });
      navigation.querySelector(".trk-overflow").hidden = true;
      navigation.querySelector('.trk-others').open = false;
      const active = tabs.find(tab => !tab.hidden && tab.getAttribute('aria-selected') === 'true');
      if (!active) tabs[0].tabIndex = 0;
      if (active) list.scrollLeft = Math.max(0, active.offsetLeft - list.offsetLeft - 12);
      return;
    }
    const widths = new Map(tabs.map(tab => [tab, tab.getBoundingClientRect().width]));
    const gap = parseFloat(getComputedStyle(list).columnGap) || 0;
    const active = tabs.find(tab => tab.getAttribute("aria-selected") === "true");
    const priority = active ? [active, ...tabs.filter(tab => tab !== active)] : tabs;
    const visible = new Set();
    let remaining = list.getBoundingClientRect().width + 1;
    for (const tab of priority) {
      const width = widths.get(tab) + (visible.size ? gap : 0);
      if (width <= remaining || tab === active) {
        visible.add(tab.dataset.trkView);
        remaining -= width;
      }
    }
    tabs.forEach(tab => { tab.hidden = !visible.has(tab.dataset.trkView); });
    const overflow = navigation.querySelector(".trk-overflow");
    overflow.hidden = visible.size === tabs.length;
    overflow.querySelectorAll("[data-trk-view]").forEach(button => { button.hidden = visible.has(button.dataset.trkView); });
    if (tabs.includes(focused) && focused.hidden || overflow.contains(focused) && (overflow.hidden || focused.hidden)) navigation.querySelector("summary").focus({preventScroll:true});
  }

  function overviewRows(kind) {
    const data = state.overview;
    if (!data) return [];
    if (kind !== "continue") return data[kind] || [];
    const partial = new Set(data.continue.map(row => `${row.art?.kind}:${row.art?.tmdb || row.key}`));
    return [...data.continue, ...data.next.filter(row => !partial.has(`${row.art?.kind}:${row.art?.tmdb || row.key}`))].sort((a, b) => String(b.date || "").localeCompare(String(a.date || "")));
  }

  function overviewCard(row, kind, index) {
    const title = cardTitle(row);
    const art = row.art;
    const episode = row.has_still && row.season != null && row.episode != null;
    const url = art?.tmdb ? `/art/tmdb/${art.kind === "movie" ? "movie" : "tv"}/${encodeURIComponent(art.tmdb)}?fallback=2&av=${document.documentElement.dataset.cwArt || "t"}&size=w500&kind=${episode ? "still" : "backdrop"}${episode ? `&season=${encodeURIComponent(row.season)}&episode=${encodeURIComponent(row.episode)}` : window.CW?.ArtChoice?.param?.(art.kind, art.tmdb) || ""}` : "";
    const percent = Number.isFinite(row.progress_percent) && row.progress_percent > 0 && row.progress_percent < 100 ? row.progress_percent : null;
    const watched = percent == null ? "" : percent < 1 ? T("tracker.overview.lessThan1") : T("profile.playback.percentWatched", { percent: `${Math.min(99, Math.round(percent))}%` });
    const remaining = percent != null && Number.isFinite(row.remaining_minutes) && row.remaining_minutes > 0 ? row.remaining_minutes : null;
    const left = Number.isInteger(row.aired_episodes_left) && row.aired_episodes_left > 0 ? row.aired_episodes_left : null;
    const badge = remaining != null ? T("tracker.overview.minLeft", { count: remaining }) : watched || (left != null ? T("tracker.overview.airedLeft", { count: left }) : T("tracker.overview.upNext"));
    const detail = [episodeLabel(row), row.episode_title || (remaining != null ? watched : "")].filter(Boolean).join(" · ");
    return `<article class="trk-overview-card" data-trk-motion-key="${esc(`${row.key || title}:${row.season || 0}:${row.episode || 0}`)}"><button type="button" class="trk-overview-open" data-trk-overview-open="${kind}:${index}" aria-label="${TH("tracker.overview.openItem", { item: [title, detail, badge].filter(Boolean).join(", ") })}"><span class="trk-overview-art">${symbol(art?.kind === "movie" || row.type === "movie" ? "movie" : "tv")}${url ? `<img src="${esc(url)}" alt="" loading="lazy" decoding="async">` : ""}<span class="trk-overview-badge">${esc(badge)}</span><span class="trk-overview-copy"><strong>${esc(title)}</strong>${detail ? `<span class="trk-overview-meta">${esc(detail)}</span>` : ""}</span><span class="trk-overview-arrow" aria-hidden="true">${icon("arrow_forward")}</span>${percent != null ? `<span class="trk-overview-progress" aria-hidden="true"><i style="width:${percent}%"></i></span>` : ""}</span></button>${kind === "continue" ? `<button type="button" class="cw-card-actions trk-overview-menu" data-trk-overview-menu="${index}" title="${TH("profile.cards.actions")}" aria-label="${TH("profile.cards.actionsFor", { title })}" aria-haspopup="menu" aria-expanded="false"${state.overviewBusy ? " disabled" : ""}>${icon("more_horiz")}</button>` : ""}</article>`;
  }

  function renderAiringTimeline(rows, data, message) {
    const byDay = new Map();
    rows.forEach((row, index) => {
      if (!byDay.has(row.air_date)) byDay.set(row.air_date, []);
      byDay.get(row.air_date).push({row, index});
    });
    const dates = Array.from({length:30}, (_, offset) => {
      const date = new Date(`${today()}T12:00:00`);
      date.setDate(date.getDate() + offset);
      const iso = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
      return {date, iso};
    });
    const densities = new Map([0, 14, 16].map(start => {
      const counts = dates.slice(start, start + 14).map(({iso}) => byDay.get(iso)?.length || 0);
      return [start, {releases:Math.max(1, counts.reduce((sum, count) => sum + count, 0)), empty:counts.filter(count => !count).length}];
    }));
    const days = dates.map(({date, iso}, offset) => {
      const density = densities.get(Math.min(16, Math.floor(offset / 14) * 14));
      const weekday = date.toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, {weekday:"short"});
      const releases = (byDay.get(iso) || []).sort((a, b) => cardTitle(a.row).localeCompare(cardTitle(b.row)) || a.row.season - b.row.season || a.row.episode - b.row.episode);
      const day = `<time datetime="${iso}"><span>${esc(weekday)}</span><strong>${date.getDate()}</strong></time>`;
      const cards = releases.map(({row, index}) => {
        const title = cardTitle(row), art = row.art;
        const url = art?.tmdb ? `/art/tmdb/tv/${encodeURIComponent(art.tmdb)}?fallback=2&av=${document.documentElement.dataset.cwArt || "t"}&size=w780&kind=${row.has_still ? `still&season=${encodeURIComponent(row.season)}&episode=${encodeURIComponent(row.episode)}` : `backdrop${window.CW?.ArtChoice?.param?.("tv", art.tmdb) || ""}`}` : "";
        const label = offset === 0 ? T("common.time.today") : row.episode === 1 ? T("profile.airing.premiere") : offset === 1 ? T("common.time.tomorrow") : T("profile.airing.inDays", { count: offset });
        return window.CW.AiringCard.render(row, {image:url, dateHtml:day, label, title, openAttributes:`data-trk-overview-open="airing:${index}"`, ariaLabel:`${dayText(iso)}: ${title}, ${episodeLabel(row)}`});
      }).join("");
      return `<div class="trk-calendar-day${releases.length ? " has-releases" : ""}${offset === 0 ? " is-today" : ""}" data-calendar-date="${iso}"${releases.length ? ` style="--calendar-releases:${density.releases};--calendar-empty-days:${density.empty}"` : ""}>${cards || `${day}${offset === 0 ? '<span class="trk-calendar-today">' + TH("common.time.today") + '</span>' : ""}`}</div>`;
    }).join("");
    const notice = !data.metadata_available || !rows.length ? message : data.checking ? `${T("tracker.ui.checkingShows")}${data.total ? ` · ${T("tracker.ui.nOfTotal", {n: num(data.checked), total: num(data.total)})}` : ""}…` : "";
    const problem = data.calendar_error ? T("tracker.overview.calendarError") : data.unavailable ? T("tracker.overview.datesUnavailable", { shows: plural(data.unavailable, "show", "shows") }) : "";
    return `<section class="trk-overview-section" aria-labelledby="trk-overview-airing"><div class="trk-overview-section-head"><div><h2 id="trk-overview-airing">${TH("profile.overview.airingSoon")}</h2></div><div class="trk-calendar-actions"><button type="button" class="cw-btn" data-trk-calendar="today">${TH("common.time.today")}</button><button type="button" class="cw-btn" data-trk-calendar="previous" aria-label="${TH("profile.overview.previousDays")}">${icon("chevron_left")}</button><button type="button" class="cw-btn" data-trk-calendar="next" aria-label="${TH("profile.overview.nextDays")}">${icon("chevron_right")}</button></div></div><div class="trk-calendar" role="region" aria-label="${TH("profile.overview.upcoming30")}" tabindex="0">${days}</div>${notice ? rows.length ? `<p class="trk-overview-notice" role="status">${esc(notice)}</p>` : emptyState(notice, "event", true) : ""}${problem ? `<p class="trk-overview-notice" role="status">${esc(problem)}</p>` : ""}</section>`;
  }

  function renderOverview(kinds = ["continue", "airing", "recent"]) {
    const data = state.overview;
    if (!data) return emptyState(state.loading ? T("tracker.overview.loading") : T("tracker.overview.unavailable"), state.loading ? "hourglass_empty" : "info");
    const playback = data.continue.length > 0;
    const sections = [["continue", T("profile.overview.continueWatching"), playback ? "progress" : "shows", playback ? T("dashboard.activity.playbackProgress") : T("tracker.overview.viewShows")], ["airing", T("profile.overview.airingSoon"), "", ""], ["recent", T("tracker.overview.recentlyWatched"), "history", T("tracker.overview.viewHistory")]];
    const reviewMonth = data.review_month || "";
    const reviewName = /^\d{4}-\d{2}$/.test(reviewMonth) ? monthName(reviewMonth) : "";
    const review = kinds.includes("recent") && state.trackerOptions?.month_review !== false && reviewName && reviewMonth > dismissedReview() ? `<div class="trk-month-banner"><button type="button" class="trk-month-entry" data-trk-month="${esc(reviewMonth)}"><span class="trk-month-decoration" aria-hidden="true"></span><span class="trk-month-entry-art" aria-hidden="true">${(data.recent || []).filter(row => artUrl(row)).slice(0, 3).map(row => `<img src="${esc(artUrl(row))}" alt="" loading="lazy" decoding="async">`).join("")}</span><span class="trk-month-entry-copy"><small>${TH("tracker.ui.monthInReview")}</small><strong>${esc(reviewName)}</strong><span>${TH("tracker.ui.exploreMonth")}${icon("arrow_forward")}</span></span>${symbol("auto_awesome", "trk-month-entry-spark")}</button><button type="button" class="trk-month-dismiss" data-trk-month-dismiss="${esc(reviewMonth)}" title="Dismiss this month's banner" aria-label="Dismiss this month's banner">${icon("close")}</button></div>` : "";
    const wrappedYear = data.wrapped?.year || "";
    const wrapped = kinds.includes("recent") && state.trackerOptions?.wrapped !== false && /^\d{4}$/.test(wrappedYear) && wrappedYear > dismissedReview(true) ? `<div class="trk-wrapped-banner"><button type="button" class="trk-wrapped-entry" data-trk-wrapped="${esc(wrappedYear)}"><span class="trk-wrapped-art" aria-hidden="true">${(data.wrapped.posters || []).filter(url=>/^https:\/\/image\.tmdb\.org\/t\/p\//.test(url)).slice(0,3).map(url=>`<img src="${esc(url)}" alt="" loading="lazy" decoding="async">`).join("")}</span><span class="trk-wrapped-copy"><small>${TH("tracker.ui.presents")}</small><strong>${TH("tracker.ui.yourYear", {year: wrappedYear})} <em>${TH("tracker.ui.wrapped")}</em></strong><span>${TH("tracker.ui.unwrapYear")} ${icon("arrow_forward")}</span></span>${symbol("celebration", "trk-wrapped-spark")}</button><button type="button" class="trk-month-dismiss" data-trk-wrapped-dismiss="${esc(wrappedYear)}" aria-label="Dismiss this year's Wrapped banner">${icon("close")}</button></div>` : "";
    return `<div class="trk-overview">${wrapped || review}${sections.filter(([kind]) => kinds.includes(kind)).map(([kind, title, view, link]) => {
      const rows = overviewRows(kind), step = overviewStep(kind), limit = state.overviewLimits[kind] * step;
      const message = kind === "airing" ? !data.metadata_available ? T("profile.airing.needsTmdb") : data.checking ? T("tracker.overview.lookingUpcoming") : data.unavailable ? T("tracker.overview.noUpcoming") : T("profile.airing.none") : kind === "continue" ? data.checking ? T("tracker.overview.findingNext") : T("tracker.overview.caughtUp") : T("tracker.overview.recentEmpty");
      if (kind === "airing") return renderAiringTimeline(rows, data, message);
      const more = rows.length > step ? `<button type="button" class="cw-btn trk-overview-more" data-trk-overview-more="${kind}" aria-expanded="${limit > step}">${rows.length > limit ? mobile() ? TH("tracker.overview.showNMore", { count: Math.min(step, rows.length - limit) }) : TH("tracker.overview.showMore") : TH("tracker.overview.showLess")}</button>` : "";
      return `<section class="trk-overview-section" aria-labelledby="trk-overview-${kind}"><div class="trk-overview-section-head"><div class="trk-overview-heading"><h2 id="trk-overview-${kind}">${title}</h2>${kind === "recent" ? `<button type="button" class="trk-overview-log" data-trk-action="log">${icon("add")}<span>${TH("tracker.actions.logWatch")}</span></button>` : ""}</div>${view ? `<button type="button" class="cw-btn" data-trk-view="${view}">${link}${icon("arrow_forward")}</button>` : ""}</div>${rows.length ? kind === "recent" ? renderGrid(rows.slice(0, limit)) : `<div class="trk-overview-grid">${rows.slice(0, limit).map((row, index) => overviewCard(row, kind, index)).join("")}</div>` : `${emptyState(message, kind === "continue" ? "play_circle" : "history", true)}`}${more}</section>`;
    }).join("")}</div>`;
  }

  let overviewMotionObserver = null;
  let overviewMotionScope = "";
  const overviewRevealed = new Set();

  function animateOverviewCards() {
    overviewMotionObserver?.disconnect();
    if (window.matchMedia?.("(prefers-reduced-motion:reduce)")?.matches || !window.IntersectionObserver) return;
    if (overviewMotionScope !== state.instance) {
      overviewMotionScope = state.instance;
      overviewRevealed.clear();
    }
    overviewMotionObserver ||= new IntersectionObserver(entries => {
      entries.forEach(entry => {
        if (!entry.isIntersecting) return;
        const card = entry.target;
        overviewMotionObserver.unobserve(card);
        if (overviewRevealed.has(card.dataset.trkMotionKey)) return;
        overviewRevealed.add(card.dataset.trkMotionKey);
        card.classList.add("is-revealing");
        card.addEventListener("animationend", () => card.classList.remove("is-revealing"), {once:true});
      });
    }, {threshold:0.15});
    state.root?.querySelectorAll(".trk-overview-card").forEach(card => {
      if (!overviewRevealed.has(card.dataset.trkMotionKey)) overviewMotionObserver.observe(card);
    });
  }

  const overviewMarkup = new WeakMap();
  const mainMarkup = new WeakMap();

  function rememberOverviewMarkup(host, html) {
    mainMarkup.set(host, {html, node:host.firstChild});
    if (!host.querySelector(".trk-overview")) return;
    const template = document.createElement("template");
    template.innerHTML = html;
    const selector = ".trk-overview-section,.trk-calendar-day";
    const nodes = host.querySelectorAll(selector);
    template.content.querySelectorAll(selector).forEach((node, index) => {
      if (nodes[index]) overviewMarkup.set(nodes[index], node.outerHTML);
    });
  }

  function renderOverviewSlot(metadata = false) {
    if (state.view !== "overview") return;
    const airingCards = window.CW.AiringCard?.capture(state.root) || [];
    const calendarScroll = state.root.querySelector(".trk-calendar")?.scrollLeft || 0;
    const continueScroll = state.root.querySelector(".trk-overview-grid")?.scrollLeft || 0;
    const active = document.activeElement;
    const attr = ["data-trk-overview-more", "data-trk-overview-open", "data-trk-overview-menu"].find(name => active?.hasAttribute(name));
    const value = attr ? active.getAttribute(attr) : "";
    if (metadata && state.root.querySelector(".trk-overview")) {
      const template = document.createElement("template");
      template.innerHTML = renderOverview(["continue", "airing"]);
      for (const next of template.content.querySelectorAll(".trk-overview-section")) {
        const current = state.root.querySelector(`[aria-labelledby="${next.getAttribute("aria-labelledby")}"]`);
        if (current && overviewMarkup.get(current) === next.outerHTML) continue;
        const dayMarkup = new Map([...next.querySelectorAll(".trk-calendar-day")].map(day => [day.dataset.calendarDate, day.outerHTML]));
        const missing = new Set([...current?.querySelectorAll("img[hidden]") || []].map(img => img.getAttribute("src")));
        next.querySelectorAll("img").forEach(img => { if (missing.has(img.getAttribute("src"))) img.hidden = true; });
        if (!current || current.isEqualNode(next)) continue;
        const timeline = current.querySelector(".trk-calendar");
        if (!timeline) { current.replaceWith(next); continue; }
        const dates = new Map([...timeline.children].map(day => [day.dataset.calendarDate, day]));
        for (const day of next.querySelector(".trk-calendar").children) {
          const old = dates.get(day.dataset.calendarDate);
          if (old && old.getAttribute("style") !== day.getAttribute("style")) {
            if (day.hasAttribute("style")) old.setAttribute("style", day.getAttribute("style"));
            else old.removeAttribute("style");
          }
          if (old && overviewMarkup.get(old) !== dayMarkup.get(day.dataset.calendarDate) && !old.isEqualNode(day)) old.replaceWith(day.cloneNode(true));
        }
        current.querySelectorAll(":scope > .trk-overview-notice").forEach(el => el.remove());
        current.append(...next.querySelectorAll(":scope > .trk-overview-notice"));
      }
      rememberOverviewMarkup(state.root.querySelector('[data-trk-slot="main"]'), renderMain());
    } else setSlot("main", renderMain());
    const calendar = state.root.querySelector(".trk-calendar");
    if (calendar) calendar.scrollLeft = calendarScroll;
    window.CW.AiringCard?.restore(state.root, airingCards);
    const continuing = state.root.querySelector(".trk-overview-grid");
    if (continuing) continuing.scrollLeft = continueScroll;
    animateOverviewCards();
    if (attr) state.root.querySelector(`[${attr}="${value}"]`)?.focus({preventScroll:true});
  }

  let overviewController = null;
  const overviewCache = new Map();
  const reviewDismissals = new Map();
  let monthThemeModule;
  function paintMonthlyBanner() {
    const banner = state.root?.querySelector(".trk-month-banner"), entry = banner?.querySelector("[data-trk-month]");
    if (!entry) return;
    const instance = state.instance;
    monthThemeModule ||= import("/assets/js/tracker-month-themes.js").catch(() => {monthThemeModule = null; return null;});
    monthThemeModule.then(module => {if (module && banner.isConnected && instance === state.instance) module.paintTheme(banner,entry.dataset.trkMonth,instance,true);});
  }
  const reviewDismissKey = (yearly = false) => {
    const auth = window.CW?.AuthState?.read?.(), view = window.CW?.OverviewProfile;
    return `cw-${yearly ? "wrapped" : "month"}-dismissed:${JSON.stringify([auth?.user?.id || auth?.user?.username || auth?.role || "local",auth?.profileId || "",auth?.isAdmin ? view?.id || "" : "",state.instance])}`;
  };
  function dismissedReview(yearly = false) {
    const key = reviewDismissKey(yearly);
    try {return localStorage.getItem(key) || reviewDismissals.get(key) || "";} catch {return reviewDismissals.get(key) || "";}
  }
  let monthlyReviewPage = null, wrappedPage = null, reviewOpening = 0;
  function openYearlyReview(button, year = "") {
    button.disabled = true;
    const opening = ++reviewOpening, scope = overviewCacheKey() + currentProfileScope();
    state.root.querySelector(".trk-others")?.removeAttribute("open");
    import(`/assets/js/tracker-wrapped.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`).then(({openWrapped}) => {
      if (opening !== reviewOpening || scope !== overviewCacheKey() + currentProfileScope() || state.root.hidden || state.root.classList.contains("hidden")) return;
      monthlyReviewPage?.close(false);
      wrappedPage = openWrapped({root:state.root, instance:state.instance, label:instanceLabel(), year,
        onClose: () => {wrappedPage = null;}, openMedia:row => selectRef(row.ref,{openMedia:true})});
      state.monthlyReview = false;
      rememberView();
    }).catch(() => toast.show(T("tracker.wrapped.openFailed"),{tone:"bad"})).finally(() => {button.disabled = false;});
  }
  function openMonthlyReview(button, month = "", remember = true) {
    button.disabled = true;
    const opening = ++reviewOpening, scope = overviewCacheKey() + currentProfileScope();
    state.root.querySelector(".trk-others")?.removeAttribute("open");
    import(`/assets/js/tracker-month.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`).then(({openMonthReview}) => {
      if (opening !== reviewOpening || scope !== overviewCacheKey() + currentProfileScope() || state.root.hidden || state.root.classList.contains('hidden')) return;
      wrappedPage?.close(false);
      monthlyReviewPage = openMonthReview({root:state.root, instance:state.instance, label:instanceLabel(), month,
        onClose: restore => { monthlyReviewPage = null; if (restore) { state.monthlyReview = false; rememberView(); } },
        returnLabel:VIEWS.find(view => view.id === state.view)?.label || T("profile.tabs.overview"), openMedia:row => selectRef(row.ref, {openMedia:true})});
      state.monthlyReview = true;
      if (remember) rememberView();
    }).catch(() => toast.show(T("tracker.month.openFailed"), {tone:"bad"})).finally(() => {button.disabled = false;});
  }
  const overviewCacheKey = () => {
    const auth = window.CW?.AuthState?.read?.();
    return JSON.stringify([auth?.user?.id || auth?.user?.username, auth?.profileId, auth?.role, auth?.permissions, currentProfileScope(), window.CW?.OverviewProfile?.filter, state.instance]);
  };

  const OVERVIEW_STORE_KEY = "cw.tracker.overview.v1";
  const OVERVIEW_CACHE_TTL = 24 * 60 * 60 * 1000;
  const listCacheKey = () => JSON.stringify([overviewCacheKey(), state.view, state.q, state.media, state.sort, state.noteStatus, state.noteTag, state.hideDropped, state.duplicates, state.cleanFeature, state.healthFilter, state.offset, pageSize(), timelineOn(), state.month]);

  function savedLists() {
    try {
      const entries = JSON.parse(sessionStorage.getItem("cw.tracker.lists.v1") || "[]");
      return Array.isArray(entries) ? entries.filter(entry => entry?.data && Date.now() - entry.t < OVERVIEW_CACHE_TTL).slice(-16) : [];
    } catch { return []; }
  }

  function restoreList() {
    if (["overview", "cleanup"].includes(state.view)) return false;
    const saved = savedLists().find(entry => entry.key === listCacheKey());
    if (!saved) return false;
    Object.assign(state, saved.data);
    return true;
  }

  function rememberList(key) {
    if (["overview", "cleanup"].includes(state.view) || key !== listCacheKey() || state.error || Math.max(state.items.length, state.shows.length, state.noteRows.length) > 250) return;
    try {
      const entries = savedLists().filter(entry => entry.key !== key);
      entries.push({key, t:Date.now(), data:{items:state.items, shows:state.shows, noteRows:state.noteRows, total:state.total}});
      sessionStorage.setItem("cw.tracker.lists.v1", JSON.stringify(entries.slice(-16)));
    } catch {}
  }

  function savedOverviews() {
    try {
      const entries = JSON.parse(sessionStorage.getItem(OVERVIEW_STORE_KEY) || "[]");
      return Array.isArray(entries) ? entries.filter(entry => entry?.payload?.day === today() && Date.now() - Number(entry.t || 0) < OVERVIEW_CACHE_TTL).slice(-16) : [];
    } catch { return []; }
  }

  function restoreOverview() {
    const key = overviewCacheKey();
    const saved = savedOverviews().find(entry => entry.key === key);
    if (!saved || saved.payload.scope !== key) return;
    state.overview = saved.payload;
    state.stats = saved.stats || state.stats;
    overviewCache.set(key, state.overview);
  }

  function rememberOverview() {
    if (!state.overview || state.overview.checking || state.overview.calendar_error) return;
    const key = overviewCacheKey();
    if (state.overview.scope !== key) return;
    overviewCache.delete(key);
    overviewCache.set(key, state.overview);
    while (overviewCache.size > 16) overviewCache.delete(overviewCache.keys().next().value);
    try {
      const entries = savedOverviews().filter(entry => entry.key !== key);
      entries.push({key, t:Date.now(), payload:state.overview, stats:state.stats});
      sessionStorage.setItem(OVERVIEW_STORE_KEY, JSON.stringify(entries.slice(-16)));
    } catch {}
  }

  async function loadOverviewHero(data) {
    const head = data.hero || data.continue?.[0] || data.recent?.[0];
    if (!head) return;
    data.hero = head;
    updateHeroSelection();
    updateHeroBackdrop();
    const item = cardItem(head, head.art?.kind === "tv" || isChild(head) || head.type === "show");
    if (!item) return;
    try {
      data.heroMeta = await window.CW?.Meta?.get?.(item, "detail", {seriesInfo:true});
      if (state.overview === data && state.view === "overview") { updateHeroSelection(); rememberOverview(); }
    } catch {}
  }

  function stopOverview() {
    overviewController?.abort();
    overviewController = null;
  }

  function applyOverviewShows(page) {
    const data = state.overview;
    if (!data || page?._view?.ready === false) return;
    data.next = page.continue || [];
    data.airing = page.airing || [];
    data.unavailable = page.unavailable || 0;
    data.total = page.total || 0;
    data.checked = page.checked ?? data.total;
    data.metadata_available = page.metadata_available !== false;
    data.checking = false;
    data.calendar_error = !!page._view?.error;
    renderOverviewSlot(true);
    rememberOverview();
  }

  async function loadOverviewShows(id, instance, controller) {
    const data = state.overview;
    const active = () => !controller.signal.aborted && id === state.request && state.overview === data && state.view === "overview" && instance === state.instance;
    try {
      const page = await getViewJSON(`/api/tracker/overview/shows?provider_instance=${encodeURIComponent(instance)}&day=${today()}`, undefined, controller.signal);
      if (active()) applyOverviewShows(page);
    } catch (error) {
      if (!active() || error.name === "AbortError") return;
      data.checking = false;
      data.calendar_error = true;
      renderOverviewSlot(true);
    }
  }

  async function overviewOpen(kind, index) {
    const row = overviewRows(kind)[index];
    if (!row) return;
    const item = cardItem({...row, title:cardTitle(row)}, row.art?.kind === "tv" || isChild(row));
    if (canCard(item)) selectRef(row.ref, {openMedia:true});
    else {
      state.view = row.ref.show ? "shows" : row.ref.feature || "history";
      rememberView();
      state.q = "";
      reload();
      selectRef(row.ref);
    }
  }

  async function updateWatchLabel(menu, row, ref) {
    if (!ref || ref.feature === "history" || (row.episode == null && row.type !== "movie")) return;
    const same = state.ref && ["note", "show", "feature", "key"].every(key => state.ref[key] === ref[key]);
    try {
      const data = same && state.title ? state.title : await getJSON(`/api/tracker/title?${inst()}&${new URLSearchParams(ref)}`);
      const watched = (data.plays || []).some(play => row.type === "movie" || Number(play.season) === Number(row.season) && Number(play.episode) === Number(row.episode));
      const button = menu.querySelector("[data-trk-watch-label]");
      if (!button || !menu.matches(":popover-open")) return;
      button.lastElementChild.textContent = watched ? T("tracker.actions.logRewatch") : T("tracker.actions.logWatch");
      button.firstElementChild.textContent = watched ? "replay" : "check";
    } catch {}
  }

  function openOverviewActions(button) {
    const row = overviewRows("continue")[Number(button.dataset.trkOverviewMenu)];
    if (!row) return;
    const disabled = !!state.overviewBusy || window.CW?.AuthState?.read?.()?.permissions?.write === false;
    const entries = [{icon:"check", label:T("tracker.actions.logWatch"), attrs:' data-trk-watch-label', disabled, run:() => overviewWatch(row)},
      {icon:"do_not_disturb_on", label:row.type === "movie" ? T("tracker.actions.dropTitle") : T("tracker.actions.dropShow"), disabled:disabled || row.status === "dropped", run:() => overviewDrop(row)}];
    const menu = window.CW.CardActions.open(button, entries, {title:cardTitle(row),subtitle:[episodeLabel(row),row.episode_title].filter(Boolean).join(" · ")});
    void updateWatchLabel(menu, row, row.ref);
  }

  async function overviewWatch(row) {
    if (!row || state.overviewBusy || window.CW?.AuthState?.read?.()?.permissions?.write === false) return;
    const instance = state.instance;
    const item = row.type === "episode" ? {type:"episode", series_title:cardTitle(row), series_year:row.series_year || row.year, season:row.season, episode:row.episode, ids:row.ids || {}, show_ids:row.show_ids || {tmdb:String(row.art?.tmdb || "")}} : {type:"movie", title:cardTitle(row), year:row.year, ids:row.ids || {tmdb:String(row.art?.tmdb || "")}};
    state.overviewBusy = row.key;
    renderOverviewSlot();
    try {
      const result = await getJSON("/api/tracker/add", {feature:"history", provider_instance:instance, items:[{...item, watched_at:new Date().toISOString()}]});
      toast.show(T("tracker.toast.markedWatched", { title: cardTitle(row) }), {undo:result.batch ? () => undo(result.batch, instance) : null});
      if (instance === state.instance) { state.progress = {}; await refresh(); }
    } catch (err) { toast.show(writeError(err), {tone:"bad"}); }
    finally { state.overviewBusy = ""; renderOverviewSlot(); }
  }

  async function overviewDrop(row) {
    if (!row || state.overviewBusy || window.CW?.AuthState?.read?.()?.permissions?.write === false) return;
    const instance = state.instance, ref = row.ref;
    state.overviewBusy = row.key;
    renderOverviewSlot();
    try {
      const result = await getJSON("/api/tracker/titles/status", {
        provider_instance:instance, keys:[ref.show || ref.key], status:"dropped",
        view:ref.show ? "shows" : ref.feature, feature:ref.show ? "history" : ref.feature,
      });
      toast.show(T("tracker.toast.dropped", { title: cardTitle(row) }), {undo:result.batch ? () => undo(result.batch, instance) : null});
      if (instance === state.instance) { state.progress = {}; await refresh(); }
    } catch (err) { toast.show(writeError(err), {tone:"bad"}); }
    finally { state.overviewBusy = ""; renderOverviewSlot(); }
  }

  function renderNoteFilters() {
    const counts = state.view === "titles" ? state.notes.statuses || {} : {};
    const statuses = [{ id: "", label: T("tracker.filters.allStatuses"), icon: "", count: state.view === "titles" ? state.notes.total : 0 }, ...STATUSES.map((s) => ({ ...s, count: counts[s.id] })), {id:"none",label:T("tracker.filters.noStatus")}]
      .map((s) => `<option value="${esc(s.id)}"${state.noteStatus === s.id ? " selected" : ""}>${esc(s.label)}${s.count ? ` (${esc(num(s.count))})` : ""}</option>`)
      .join("");
    const tags = [...(state.notes.tags || [])];
    const picked = state.noteTag.toLowerCase();
    if (picked && !tags.some(t => t.tag.toLowerCase() === picked)) tags.push({tag:state.noteTag,count:0});
    const tagSelect = `<label class="trk-status-filter"><span>${TH("tracker.filters.tag")}</span><select class="cw-select trk-tag-filter" data-trk-input="note-tag" aria-label="${TH("tracker.filters.byTag")}"><option value="">${TH("tracker.filters.everyTag")}</option>${tags.map((t) => `<option value="${esc(t.tag)}"${t.tag.toLowerCase() === picked ? " selected" : ""}>${esc(t.tag)} · ${esc(num(t.count))}</option>`).join("")}</select></label>`;
    return `<label class="trk-status-filter"><span>${TH("tracker.filters.status")}</span><select class="cw-select" data-trk-input="note-status" aria-label="${TH("tracker.filters.byStatus")}">${statuses}</select></label>${state.view === "titles" && !tags.length ? "" : tagSelect}`;
  }

  function renderFilters() {
    if (state.view === "cleanup") {
      const lists = FEATURES.map((f) => `<option value="${f.id}"${f.id === state.cleanFeature ? " selected" : ""}>${esc(f.label)}</option>`).join("");
      const health = HEALTH.map((h) => `<button type="button" class="trk-clean-health" data-trk-health="${h.id}" aria-pressed="${state.healthFilter === h.id}">${symbol(h.icon)}<span>${esc(h.label)}</span><b>${esc(num(healthOf()[h.id]))}</b></button>`).join("");
      return `<select class="cw-select trk-clean-list" data-trk-input="clean-feature" aria-label="${TH("tracker.filters.cleanList")}">${lists}</select><div class="trk-clean-health-options" role="group" aria-label="TMDB health">${health}</div>`;
    }
    if (state.view === "titles" || historyView()) return "";
    let out = `<div class="trk-seg" role="group" aria-label="${TH("tracker.filters.type")}">${MEDIA.map((m) => chip(`data-trk-media="${m.id}"`, m.label, { active: state.media === m.id, iconName: m.icon })).join("")}</div>`;
    if (timelineOn() && state.month) {
      out += `<button type="button" class="cw-filter-chip active" data-trk-action="month-clear" title="${TH("tracker.filters.backToLatest")}">${symbol("event", "cw-type-icon")}<span>${esc(monthName(state.month))} and older</span>${symbol("close", "cw-type-icon")}</button>`;
    }
    return out;
  }

  function renderExtraFilters() {
    const type = historyView() ? `<div class="trk-filter-type"><span>${TH("tracker.filters.type")}</span><div class="trk-seg" role="group" aria-label="${TH("tracker.filters.historyType")}">${[["", T("tracker.ui.all"), ""], ["movie", T("tracker.ui.movies"), "movie"], ["show", T("tracker.ui.shows"), "tv"]].map(([id, label, glyph]) => chip(`data-trk-history-media="${id}"`, label, {active:state.historyMedia === id, iconName:glyph})).join("")}</div></div><label class="trk-status-filter"><span>${TH("tracker.filters.groupBy")}</span><select class="cw-select" data-trk-input="history-group" aria-label="${TH("tracker.filters.groupHistory")}"><option value="watch"${!groupedHistory() ? " selected" : ""}>${TH("tracker.filters.groupWatch")}</option><option value="title"${groupedHistory() ? " selected" : ""}>${TH("tracker.filters.groupTitle")}</option></select></label>` : "";
    const dropped = state.view === "shows" ? `<label class="trk-filter-check"><input type="checkbox" class="cw-checkbox" data-trk-input="hide-dropped"${state.hideDropped ? " checked" : ""}><span>${TH("tracker.filters.hideDropped")}</span></label>` : "";
    const labels = `<label class="trk-filter-check"><input type="checkbox" class="cw-checkbox" data-trk-input="status-labels"${state.showStatusLabels !== false ? " checked" : ""}><span>${TH("tracker.filters.statusLabels")}</span></label>`;
    const sort = ["shows", "cleanup", "titles"].includes(state.view) ? "" : `<label class="trk-status-filter"><span>${TH("common.labels.sort")}</span><select class="cw-select" data-trk-input="sort" aria-label="${TH("tracker.filters.sortResults")}">${[["recent",T("tracker.ui.newest")],["oldest",T("tracker.ui.oldest")],["title","Title A-Z"]].map(([value,label]) => `<option value="${value}"${state.sort === value ? " selected" : ""}>${label}</option>`).join("")}</select></label>`;
    const options = renderFilters();
    return `<div id="trk-extra-filters" class="trk-extra-filters"${state.filtersOpen ? "" : " hidden"} role="group" aria-label="${TH("common.labels.filters")}"><div class="trk-filter-fields">${type}${options ? `<div class="trk-filter-options">${options}</div>` : ""}${renderNoteFilters()}${sort}</div><div class="trk-filter-footer"><div class="trk-filter-checks">${state.view === "history" ? `<label class="trk-filter-check"><input type="checkbox" class="cw-checkbox" data-trk-input="duplicates"${state.duplicates ? " checked" : ""}><span>${TH("tracker.filters.duplicates")}</span></label>` : ""}${dropped}${labels}</div><button type="button" class="cw-btn" data-trk-action="filters-clear">${TH("tracker.filters.clear")}</button></div>${state.view === "history" && state.duplicates ? `<small>${TH("tracker.filters.duplicatesHint")}</small>` : ""}</div>`;
  }

  function renderControls() {
    if (state.view === "overview") return renderNavigation();
    const placeholder = state.view === "shows" ? T("tracker.search.shows") : state.view === "titles" ? T("tracker.search.titles") : T("tracker.search.default");
    const count = Number(state.view === "history" && !!state.duplicates) + Number(!!state.noteStatus) + Number(!!state.noteTag)
      + Number(state.sort !== "recent" && !["shows", "cleanup", "titles"].includes(state.view))
      + Number(!historyView() && !["titles", "cleanup"].includes(state.view) && !!state.media)
      + Number(state.view === "shows" && state.hideDropped) + Number(timelineOn() && !!state.month)
      + Number(state.view === "cleanup" && !!state.healthFilter)
      + Number(historyView() && !!state.historyMedia) + Number(groupedHistory()) + Number(state.showStatusLabels === false);
    const filterLabel = `${T("tracker.ui.filters")}${count ? ` (${T("tracker.ui.activeN", {n: count})})` : ""}`;
    return `
      ${renderNavigation()}
      <div class="trk-toolbar${state.view === "cleanup" ? " trk-toolbar-cleanup" : ""}">
        <div class="trk-search-wrap"><input class="cw-input trk-search" data-trk-input="q" value="${esc(state.q)}" placeholder="${esc(placeholder)}" aria-label="${esc(placeholder)}"><button type="button" class="trk-filter-toggle${count || state.filtersOpen ? " active" : ""}" data-trk-action="filters" aria-label="${filterLabel}" title="${filterLabel}" aria-expanded="${state.filtersOpen}" aria-controls="trk-extra-filters">${icon("filter_list")}${count ? `<span>${count}</span>` : ""}</button></div>
        <div class="trk-actions">
          ${state.view === "history" ? `<button type="button" class="cw-filter-chip trk-icon-btn${state.byDay ? " active" : ""}" data-trk-action="by-day" aria-pressed="${state.byDay}" aria-label="${TH("tracker.layout.groupByDay")}" title="${TH("tracker.layout.byDay")}">${symbol("calendar_view_day", "cw-type-icon")}</button>` : ""}
          <div class="cw-type-filter trk-layout" role="group" aria-label="${TH("tracker.layout.label")}">${[['list', 'view_list', T("tracker.layout.list")], ['grid', 'grid_view', T("tracker.layout.grid")], ...(state.view === "collection" ? [['covers', 'photo_library', T("tracker.layout.covers")], ['shelf', 'shelves', T("tracker.layout.shelf")]] : [])].map(([value, glyph, label]) => `<button type="button" class="cw-filter-chip trk-icon-btn${(state.layouts[layoutView()] || "list") === value ? " active" : ""}" data-trk-layout="${value}" aria-pressed="${(state.layouts[layoutView()] || "list") === value}" aria-label="${label} view" title="${label} view">${symbol(glyph, "cw-type-icon")}</button>`).join("")}</div>
          <div class="trk-secondary-actions">${[['views','bookmarks',T("tracker.secondary.savedViews")],['changes','manage_history',T("tracker.secondary.changes")],['import','download',T("tracker.secondary.import")]].map(([action,glyph,label]) => `<button type="button" class="cw-btn trk-icon-btn" data-trk-action="${action}" title="${label}" aria-label="${label}">${icon(glyph)}<span class="trk-tool-label">${label}</span></button>`).join('')}</div>
          <button type="button" class="cw-btn primary trk-log-btn" data-trk-action="quick-add" aria-label="${TH("tracker.add.quickAdd")}">${icon("add_circle")}<span>${TH("tracker.add.quickAdd")}</span></button>
        </div>
      </div>${window.CW?.MobileUI?.mobile() ? '' : renderExtraFilters()}`;
  }

  function pager() {
    const shown = state.view === "shows" ? state.shows.length : state.view === "titles" ? state.noteRows.length : state.items.length;
    if (!shown) return "";
    const sizes = (mobile() ? MOBILE_PAGE_SIZES : PAGE_SIZES).map((n) => `<option value="${n}"${n === pageSize() ? " selected" : ""}>${n}</option>`).join("");
    const start = state.offset + 1;
    const end = state.offset + shown;
    const nav = state.total > pageSize()
      ? `<div class="trk-pager-nav">
          <button type="button" class="cw-btn" data-trk-action="prev"${state.offset > 0 && !state.loading ? "" : " disabled"}>${TH("common.paging.previousShort")}</button>
          <span class="cw-page-info">${esc(num(start))}–${esc(num(end))} of ${esc(num(state.total))}</span>
          <button type="button" class="cw-btn" data-trk-action="next"${end < state.total && !state.loading ? "" : " disabled"}>${TH("common.paging.nextShort")}</button>
        </div>`
      : "";
    return `<div class="cw-pager trk-pager">
        <span class="cw-status-text">${esc(statusText())}</span>
        <label class="cw-page-size-control"><span>${TH("tracker.paging.rows")}</span><select class="cw-select" data-trk-input="size" aria-label="${TH("tracker.paging.rowsPerPage")}">${sizes}</select></label>
        ${nav}
      </div>`;
  }

  function renderSelection() {
    return `<span class="cw-selection-count" aria-live="polite">${icon("check_box_outline_blank")}<strong>${esc(num(state.picked.size))}</strong><span>selected</span></span>
      <button type="button" class="cw-selection-button" data-trk-action="pick-visible"${state.loading || state.picking ? " disabled" : ""}>${icon("visibility")}<span>${TH("common.selection.visible")}</span></button>
      <button type="button" class="cw-selection-button" data-trk-action="pick-all"${state.loading || state.picking ? " disabled" : ""} title="${TH("tracker.selection.allMatching")}">${icon("checklist")}<span>${state.picking ? TH("tracker.selection.selecting") : TH("common.selection.allResults")}</span></button>
      <button type="button" class="cw-selection-button" data-trk-action="clear-picked">${icon("close")}<span>${TH("common.actions.clear")}</span></button>
      <span class="cw-selection-actions"><button type="button" class="cw-selection-button" data-trk-action="status-picked" popovertarget="trk-bulk-status-menu" aria-haspopup="menu" aria-expanded="false"${state.picking || state.bulkStatusBusy ? " disabled" : ""}>${icon("flag")}<span>${state.bulkStatusBusy ? TH("tracker.selection.saving") : TH("tracker.selection.setStatus")}</span></button><button type="button" class="cw-selection-button is-danger" data-trk-action="delete-picked"${state.picking ? " disabled" : ""}>${icon("delete")}<span>${state.view === "titles" ? TH("tracker.selection.removeFromTitles") : groupedHistory() ? TH("tracker.selection.removeEllipsis") : TH("common.actions.delete")}</span></button></span>`;
  }

  function createBulkStatusMenu() {
    const menu = document.createElement("div");
    menu.id = "trk-bulk-status-menu";
    menu.className = "trk-status-menu trk-bulk-status-menu";
    menu.setAttribute("popover", "auto");
    menu.setAttribute("role", "menu");
    menu.setAttribute("aria-label", T("tracker.selection.setStatusFor"));
    menu.innerHTML = [...STATUSES, {id:"",label:T("tracker.filters.noStatus"),icon:"remove"}].map(s => `<button type="button" class="trk-status-item" role="menuitem" tabindex="-1" data-trk-bulk-set-status="${s.id}" data-status="${s.id}">${symbol(s.icon)}<span>${esc(s.label)}</span></button>`).join("");
    const trigger = () => state.root?.querySelector('[data-trk-action="status-picked"]');
    const position = () => {
      const rect = trigger()?.getBoundingClientRect();
      if (!rect) return;
      const width = Math.min(220, window.innerWidth - 24);
      menu.style.width = `${width}px`;
      menu.style.left = `${Math.max(12, Math.min(rect.left, window.innerWidth - width - 12))}px`;
      menu.style.bottom = `${window.innerHeight - rect.top + 8}px`;
      menu.style.maxHeight = `${Math.max(0, rect.top - 20)}px`;
    };
    menu.addEventListener("beforetoggle", event => {
      const open = event.newState === "open";
      trigger()?.setAttribute("aria-expanded", String(open));
      if (open) position();
    });
    menu.addEventListener("toggle", () => {
      if (menu.matches(":popover-open")) menu.querySelector("button")?.focus({preventScroll:true});
    });
    menu.addEventListener("keydown", event => {
      const buttons = Array.from(menu.querySelectorAll("button"));
      const index = buttons.indexOf(document.activeElement);
      if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
        event.preventDefault();
        const next = event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1 : (index + (event.key === "ArrowUp" ? -1 : 1) + buttons.length) % buttons.length;
        buttons[next]?.focus();
      } else if (event.key === "Escape" || event.key === "Tab") {
        event.preventDefault();
        event.stopPropagation();
        menu.hidePopover();
        trigger()?.focus({preventScroll:true});
      }
    });
    menu.addEventListener("click", event => {
      const item = event.target.closest("[data-trk-bulk-set-status]");
      if (!item) return;
      menu.hidePopover();
      trigger()?.focus({preventScroll:true});
      void applyBulkStatus(item.dataset.trkBulkSetStatus);
    });
    window.addEventListener("resize", () => { if (menu.isConnected && menu.matches(":popover-open")) position(); });
    state.root.querySelector(".cw-root").appendChild(menu);
  }

  function emptyState(text, symbolName = "table_rows", compact = false, extra = "") {
    if (symbolName === "hourglass_empty") return `<div class="cw-empty cw-page-loading" role="status"><div class="cw-empty-text">${esc(text)}</div></div>`;
    return `<div class="cw-page-empty${compact ? " is-compact" : ""}" role="status"><span class="cw-page-empty-art" aria-hidden="true">${icon(symbolName)}</span><span class="cw-page-empty-text">${esc(text)}</span>${extra}</div>`;
  }

  function tableWrap(head, body, cls, cols) {
    const colgroup = `<colgroup>${cols.map((c) => `<col class="trk-c-${c}">`).join("")}</colgroup>`;
    return `<div class="cw-table-wrap cw-page-table"><div class="cw-table-scroll"><table class="cw-table trk-table ${cls}">${colgroup}<thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div></div>`;
  }

  const th = (label, cls = "") => `<th class="${cls}"><span class="cw-th-inner"><span class="cw-th-label">${esc(label)}</span></span></th>`;

  function timelineGroups() {
    const days = [];
    for (const item of state.items) {
      const day = dayLabel(item.date);
      let bucket = days[days.length - 1];
      if (!bucket || bucket.label !== day) {
        bucket = { label: day, plays: 0, rows: [], shows: new Map() };
        days.push(bucket);
      }
      bucket.plays += 1;
      if (isChild(item)) {
        const key = showKeyOf(item);
        const group = bucket.shows.get(key);
        if (group) {
          group.items.push(item);
          continue;
        }
        const fresh = { kind: "show", key, items: [item] };
        bucket.shows.set(key, fresh);
        bucket.rows.push(fresh);
      } else {
        bucket.rows.push({ kind: "movie", key: item.key, items: [item] });
      }
    }
    return days;
  }

  function episodeSummary(items) {
    const eps = items.filter((i) => i.season != null && i.episode != null).sort((a, b) => a.season - b.season || a.episode - b.episode);
    if (!eps.length) return plural(items.length, "episode", "episodes");
    if (eps.length === 1) return se(eps[0].season, eps[0].episode);
    const seasons = new Set(eps.map((e) => e.season));
    if (seasons.size === 1) {
      const nums = eps.map((e) => e.episode);
      const contiguous = nums.every((n, i) => i === 0 || n === nums[i - 1] + 1);
      return contiguous ? `${se(eps[0].season, nums[0])}–E${pad(nums[nums.length - 1])}` : `S${pad(eps[0].season)} · ${plural(eps.length, "episode", "episodes")}`;
    }
    return plural(eps.length, "episode", "episodes");
  }

  function renderTimeline() {
    if (!state.items.length) {
      return emptyState(state.loading ? T("tracker.empty.loadingHistory") : state.q || state.media || state.noteStatus || state.noteTag || state.duplicates ? T("tracker.empty.noPlays") : T("tracker.empty.nothingWatched"), state.loading ? "hourglass_empty" : "history");
    }
    const body = timelineGroups().map((day) => {
      const header = `<tr class="trk-day"><td colspan="4"><span>${esc(day.label)}</span><small>${esc(plural(day.plays, "play", "plays"))}</small></td></tr>`;
      const rows = day.rows.map((group) => {
        const lead = group.items[0];
        const many = group.items.length > 1;
        const selected = state.ref && (group.kind === "show" ? state.ref.show === group.key : state.ref.key === lead.key);
        const ref = group.kind === "show" ? `data-trk-ref-show="${esc(group.key)}"` : `data-trk-ref-key="${esc(lead.key)}"`;
        const flagged = group.items.find((i) => checkInfo(i).state === "attention");
        const inline = group.kind === "show"
          ? `<span class="trk-ep">${esc(episodeSummary(group.items))}</span>${many ? `<span class="trk-inline-sub">${esc(plural(group.items.length, "episode", "episodes"))}</span>` : ""}`
          : lead.year ? `<span class="trk-inline-sub trk-list-desktop-meta">${esc(lead.year)}</span>` : "";
        const openKey = `${day.label}|${group.key}`;
        const expanded = many && state.open.has(openKey);
        const toggle = many ? `<button type="button" class="trk-fold" data-trk-fold="${esc(openKey)}" aria-expanded="${expanded}" aria-label="${TH(expanded ? "tracker.ui.hideEpisodes" : "tracker.ui.showEpisodes")}">${symbol(expanded ? "expand_less" : "expand_more")}</button>` : "";
        const row = `<tr class="trk-play${selected ? " is-selected" : ""}" ${ref} data-trk-day="${esc(day.label)}" tabindex="0" aria-selected="${!!selected}">
            <td class="trk-col-time">${esc(timeText(lead.date))}</td>
            <td class="cw-col-type">${typePill(group.kind === "show" ? { type: "show" } : lead)}</td>
            <td class="cw-col-title">${listIdentity(group.kind === "show" ? {...lead, type:"show", year:lead.series_year || lead.year} : lead, `<div class="cw-title-row"><strong class="cw-merged-title">${esc(cardTitle(lead))}</strong>${inline}${toggle}</div>`)}</td>
            <td class="trk-col-health">${flagged ? healthIcon(flagged) : ""}</td>
          </tr>`;
        const children = expanded ? group.items.map((i) => `<tr class="trk-play trk-child" data-trk-ref-key="${esc(i.key)}" tabindex="0"><td class="trk-col-time">${esc(timeText(i.date))}</td><td></td><td class="cw-col-title"><span class="trk-child-title">${esc(episodeLabel(i) || cardTitle(i))}</span></td><td class="trk-col-health">${healthIcon(i)}</td></tr>`).join("") : "";
        return row + children;
      }).join("");
      return header + rows;
    }).join("");
    const more = state.items.length < state.total
      ? `<div class="cw-pager"><button type="button" class="cw-btn" data-trk-action="more"${state.loading ? " disabled" : ""}>${icon("expand_more")}<span>${TH("tracker.list.showOlder")}</span></button><span class="cw-page-info">${esc(num(state.items.length))} of ${esc(num(state.total))}</span></div>`
      : "";
    return tableWrap(`${th(T("tracker.ui.time"), "trk-col-time")}${th(T("tracker.list.colType"), "cw-col-type")}${th(T("tracker.list.colTitle"), "cw-col-title")}${th("", "trk-col-health")}`, body, "trk-timeline", ["time", "type", "title", "health"]) + more;
  }

  function progressCell(show, part = "all") {
    const tmdb = show.art?.tmdb;
    const p = tmdb ? state.progress[tmdb] : null;
    if (!p || !p.available) return part === "bar" ? "" : `<span class="trk-sub">${tmdb && !p ? "…" : esc(plural(show.episodes, "episode", "episodes"))}</span>`;
    const pct = p.total ? Math.min(100, Math.round((p.watched / p.total) * 100)) : 0;
    const bar = `<span class="trk-progress" role="progressbar" aria-label="${TH("tracker.list.episodesWatched")}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}" aria-valuetext="${esc(num(p.watched))} of ${esc(num(p.total))} episodes watched"><i style="width:${pct}%"></i></span>`;
    const count = `<span class="trk-progress-text">${esc(num(p.watched))}/${esc(num(p.total))}</span>`;
    return part === "bar" ? bar : part === "count" ? count : bar + count;
  }

  function nextCell(show) {
    if (show.status === "dropped") return `<span class="trk-sub">${TH("tracker.statuses.dropped")}</span>`;
    const p = show.art?.tmdb ? state.progress[show.art.tmdb] : null;
    if (!p || !p.available) return "";
    if (p.next) return `<span class="trk-next">${esc(se(p.next.season, p.next.episode))}</span>`;
    if (p.completed) return `<span class="trk-sub">${TH("tracker.statuses.completed")}</span>`;
    if (p.next_air) return `<span class="trk-sub">${TH("tracker.ui.airs", {date: dateText(p.next_air.date)})}</span>`;
    return `<span class="trk-sub">${TH("tracker.list.upToDate")}</span>`;
  }

  function renderShows() {
    if (!state.shows.length) return emptyState(state.loading ? T("tracker.empty.loadingShows") : state.q || state.noteStatus || state.noteTag || state.hideDropped ? T("tracker.empty.noShows") : T("tracker.empty.noEpisodes"), state.loading ? "hourglass_empty" : "tv");
    const body = state.shows.map((show, index) => {
      const selected = state.ref?.show === show.key;
      const last = show.last_season != null ? se(show.last_season, show.last_episode) : "";
      return `<tr class="trk-play${selected ? " is-selected" : ""}${state.picked.has(show.key) ? " is-picked" : ""}" data-trk-ref-show="${esc(show.key)}" tabindex="0" aria-selected="${selected}">
          <td class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="${index}"${state.picked.has(show.key) ? " checked" : ""} aria-label="Select ${esc(show.title || show.key)}"></td>
          <td class="cw-col-title">${listIdentity(show, `<div class="cw-title-row"><strong class="cw-merged-title">${esc(show.title || show.key)}</strong>${statusPill(show.status)}</div>${show.year ? `<span class="trk-sub trk-list-desktop-meta">${esc(show.year)}</span>` : ""}`, last ? T("tracker.list.lastWatched", { time: last }) : "")}</td>
          <td class="trk-col-progress">${progressCell(show)}</td>
          <td class="trk-col-next">${nextCell(show)}</td>
          <td class="trk-col-last-episode">${esc(last)}</td>
          <td class="trk-col-date">${esc(agoText(show.last_watched))}</td>
        </tr>`;
    }).join("");
    return tableWrap(`<th class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="page"${state.shows.every(s => state.picked.has(s.key)) ? " checked" : ""} aria-label="${TH("tracker.list.selectPage")}"></th>${th(T("tracker.ui.show"), "cw-col-title")}${th(T("tracker.ui.progress"), "trk-col-progress")}${th(T("tracker.ui.nextUp"), "trk-col-next")}${th(T("tracker.ui.lastEpisode"), "trk-col-last-episode")}${th(T("tracker.ui.lastWatched"), "trk-col-date")}`, body, "trk-shows", ["pick", "title", "progress", "next", "last-episode", "date"]) + pager();
  }

  function renderGroupedHistory() {
    const label = state.view === "movies" ? "movies" : "titles";
    if (!state.items.length) return emptyState(state.loading ? T("tracker.empty.loadingNamed", { name: label }) : state.q || state.noteStatus || state.noteTag ? T("tracker.empty.noNamedMatch", { name: label }) : state.view === "movies" ? T("tracker.empty.noMovies") : T("tracker.empty.noWatches"), state.loading ? "hourglass_empty" : "movie");
    const body = state.items.map((item, index) => {
      const selected = state.ref?.key === item.key && state.ref?.feature === "history";
      return `<tr class="trk-play${selected ? " is-selected" : ""}${state.picked.has(item.key) ? " is-picked" : ""}" data-trk-ref-key="${esc(item.key)}" tabindex="0" aria-selected="${selected}">
        <td class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="${index}"${state.picked.has(item.key) ? " checked" : ""} aria-label="Select ${esc(item.title || item.key)}"></td>
        <td class="cw-col-title">${listIdentity(item, `<div class="cw-title-row"><strong class="cw-merged-title">${esc(item.title || item.key)}</strong>${statusPill(item.status)}</div>${item.year ? `<span class="trk-sub trk-list-desktop-meta">${esc(item.year)}</span>` : ""}`, plural(item.plays, "watch", "watches"))}</td>
        <td class="trk-col-plays">${esc(plural(item.plays, "watch", "watches"))}</td>
        <td class="trk-col-extra">${extraCell(item, "history")}</td>
        <td class="trk-col-date">${esc(item.date ? agoText(item.date) : T("common.status.unknown"))}</td>
      </tr>`;
    }).join("");
    const head = `<th class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="page"${state.items.every(item => state.picked.has(item.key)) ? " checked" : ""} aria-label="${TH("tracker.list.selectPage")}"></th>`;
    return tableWrap(`${head}${th(T(state.view === "movies" ? "tracker.ui.movie" : "tracker.ui.title"), "cw-col-title")}${th(T("tracker.ui.watches"), "trk-col-plays")}${th(T("tracker.ui.rating"), "trk-col-extra")}${th(T("tracker.ui.lastWatched"), "trk-col-date")}`, body, "trk-movies", ["pick", "title", "plays", "extra", "date"]) + pager();
  }

  function renderTitles() {
    if (!state.noteRows.length) {
      const filtered = state.q || state.noteStatus || state.noteTag;
      const text = state.loading ? T("common.status.loading") : filtered
        ? T("tracker.empty.noTitles")
        : T("tracker.empty.titlesHint");
      return emptyState(text, state.loading ? "hourglass_empty" : "bookmarks");
    }
    const allPicked = state.noteRows.every((n) => state.picked.has(n.key));
    const body = state.noteRows.map((n, index) => {
      const selected = state.ref?.note === n.key;
      const dates = [n.started ? T("tracker.notes.startedOn", { date: dayText(n.started) }) : "", n.finished ? T("tracker.notes.finishedOn", { date: dayText(n.finished) }) : ""].filter(Boolean).join(" · ");
      return `<tr class="trk-play${selected ? " is-selected" : ""}${state.picked.has(n.key) ? " is-picked" : ""}" data-trk-ref-note="${esc(n.key)}" tabindex="0" aria-selected="${selected}">
          <td class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="${index}"${state.picked.has(n.key) ? " checked" : ""} aria-label="Select ${esc(n.title || n.key)}"></td>
          <td class="cw-col-type">${typePill({ type: n.type })}</td>
          <td class="cw-col-title">${listIdentity(n, `<div class="cw-title-row"><strong class="cw-merged-title">${esc(n.title || n.key)}</strong>${n.year ? `<span class="trk-inline-sub trk-list-desktop-meta">${esc(n.year)}</span>` : ""}</div>${dates ? `<span class="trk-sub">${esc(dates)}</span>` : ""}${tagChips(n.tags, 5)}`, agoText(n.updated))}</td>
          <td class="trk-col-status">${statusPill(n.status)}</td>
          <td class="trk-col-date">${esc(agoText(n.updated))}</td>
        </tr>`;
    }).join("");
    const head = `<th class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="page"${allPicked ? " checked" : ""} aria-label="${TH("tracker.list.selectPage")}"></th>`;
    return tableWrap(`${head}${th(T("tracker.list.colType"), "cw-col-type")}${th(T("tracker.list.colTitle"), "cw-col-title")}${th(T("tracker.ui.status"), "trk-col-status")}${th(T("tracker.ui.updated"), "trk-col-date")}`, body, "trk-titles", ["pick", "type", "title", "status", "date"]) + pager();
  }

  function extraColumn(feature) {
    if (feature !== "progress") return T("common.labels.rating");
    if (feature === "progress") return T("profile.playback.title");
    return "";
  }

  function extraCell(item, feature, part = "all") {
    if (feature !== "progress") {
      const rating = feature === "ratings" ? item.rating : item.user_rating;
      return rating != null ? `<span class="trk-rating" aria-label="Your rating: ${esc(rating)} out of 10">${symbol("star")}${esc(rating)}</span>` : "";
    }
    if (feature === "progress") {
      const pct = Number.isFinite(item.progress_percent) ? Math.max(0, Math.min(100, item.progress_percent)) : null;
      if (pct == null) return "";
      return `${part !== "count" ? `<span class="trk-progress" aria-hidden="true"><i style="width:${pct}%"></i></span>` : ""}${part !== "bar" ? `<span class="trk-progress-text">${esc(Math.round(pct))}%</span>` : ""}`;
    }
    return "";
  }

  function renderGrid(recentRows = null) {
    const recent = Array.isArray(recentRows);
    const shows = !recent && state.view === "shows", notes = !recent && state.view === "titles";
    const rows = recent ? recentRows : shows ? state.shows : notes ? state.noteRows : state.items;
    if (!rows.length) return shows ? renderShows() : notes ? renderTitles() : ["movies", "watched"].includes(state.view) ? renderGroupedHistory() : renderList();
    const feature = recent ? "history" : listFeature();
    const coverMode = feature === "collection" && !recent && !shows && !notes && state.layouts.collection === "covers";
    let previousDay = "";
    const cards = rows.map((item, index) => {
      const refName = shows ? "show" : notes ? "note" : "key";
      const selected = !recent && state.ref?.[refName] === item.key && (shows || notes || state.ref?.feature === feature);
      const title = shows || notes ? item.title || item.key : cardTitle(item);
      const card = canCard(cardItem(item, shows || isChild(item) || item.type === "show"));
      const art = item.art || (item.ids?.tmdb ? {kind: item.type === "movie" ? "movie" : "tv", tmdb: item.ids.tmdb} : null);
      const posterCard = coverMode && !isChild(item);
      const url = artUrl({...item, art}, posterCard ? "w342" : "w500");
      const copies = Array.isArray(item.copies) ? item.copies : [];
      const mediaPill = feature === "collection" && !shows && !notes ? window.CW?.Shelf?.pill?.({ format: copies[0]?.format || item.format || "", media: item.media_info || {}, copies: copies.length, service: copies[0]?.service }) || "" : "";
      const stillSeason = art?.season ?? item.season;
      const stillEpisode = art?.episode ?? item.episode;
      const image = url && posterCard ? url : url ? `${url}&kind=${isChild(item) && stillSeason != null && stillEpisode != null ? `still&season=${encodeURIComponent(stillSeason)}&episode=${encodeURIComponent(stillEpisode)}` : "backdrop"}` : "";
      const rating = feature === "ratings" && !shows && !notes ? item.rating : item.user_rating;
      const date = shows ? item.last_watched : notes ? item.updated : item.date;
      const status = state.showStatusLabels !== false ? statusOf(item.status) : null;
      const prominentStatus = status && ["dropped", "on_hold"].includes(status.id) ? statusPill(status.id) : "";
      const statusIcon = status && !prominentStatus ? `<span class="trk-grid-status-icon" data-status="${esc(status.id)}" tabindex="0" role="img" aria-label="${esc(status.label)}">${symbol(status.icon)}</span>` : "";
      const episode = shows ? item.last_season != null ? T("tracker.list.lastEpisode", { code: se(item.last_season, item.last_episode) }) : "" : episodeCode(item);
      const meta = [isChild(item) ? item.series_year : item.year, shows ? T("common.media.show") : typeMeta(item).label, episode, !recent && ["movies", "watched"].includes(state.view) ? plural(item.plays, "watch", "watches") : "", !recent && ["movies", "watched"].includes(state.view) && !date ? T("profile.records.unknownDate") : ""].filter(Boolean).join(" · ");
      let heading = "";
      if (!recent && timelineOn()) {
        const day = dayLabel(date);
        if (day !== previousDay) { heading = `<h3 class="trk-grid-day">${esc(day)}</h3>`; previousDay = day; }
      }
      return `${heading}<article class="trk-play trk-grid-card${posterCard ? " is-poster" : ""}${selected ? " is-selected" : ""}${state.picked.has(item.key) ? " is-picked" : ""}" data-trk-ref-${refName}="${esc(item.key)}"${recent ? ` data-trk-overview-open="recent:${index}"` : ""} tabindex="0" aria-label="${esc(title)}" aria-selected="${selected}">
        <div class="trk-grid-art"><span class="trk-grid-placeholder" aria-hidden="true">${symbol(shows ? "tv" : typeMeta(item).icon)}</span>${image ? `<img src="${esc(image)}" alt="" loading="lazy" decoding="async">` : ""}${mediaPill ? `<span class="trk-grid-pill">${mediaPill}</span>` : ""}
          ${!recent ? `<label class="trk-grid-select"><input type="checkbox" class="cw-checkbox trk-grid-pick" data-trk-pick="${index}"${state.picked.has(item.key) ? " checked" : ""} aria-label="Select ${esc(title)}"></label>` : ""}
          ${rating != null ? `<span class="cw-media-card-score" aria-label="Your rating: ${esc(rating)} out of 10"><span>${esc(rating)}</span></span>` : ""}
          ${date || prominentStatus ? `<div class="trk-grid-labels">${date ? `<span class="trk-grid-date" title="${esc(whenText(date))}">${esc(!recent && timelineOn() ? timeText(date) : whenText(date))}</span>` : ""}${prominentStatus ? `<span class="trk-grid-status">${prominentStatus}</span>` : ""}</div>` : ""}
          ${shows ? progressCell(item, "bar") : !notes && feature === "progress" ? extraCell(item, feature, "bar") : ""}
          ${card ? `<button type="button" class="trk-grid-open" data-trk-card-open data-trk-card-index="${index}" data-trk-card-recent="${recent}" aria-label="Open details for ${esc(title)}"></button>` : ""}
        </div>
        <div class="trk-grid-copy"><strong title="${esc(title)}">${esc(title)}</strong><small>${statusIcon}<span>${esc(meta)}</span></small>${notes ? tagChips(item.tags, 3) : !shows && feature === "progress" ? extraCell(item, feature, "count") : ""}
          <div class="trk-grid-tools">${shows ? progressCell(item, "count") : !recent && ["movies", "watched"].includes(state.view) ? "" : !notes ? healthIcon(item, state.view === "cleanup") : ""}<button type="button" class="cw-card-actions" data-trk-card-menu data-trk-card-index="${index}" data-trk-card-recent="${recent}" title="${TH("profile.cards.actions")}" aria-label="${TH("profile.cards.actionsFor", { title })}" aria-haspopup="menu" aria-expanded="false">${icon("more_horiz")}</button></div>
        </div></article>`;
    }).join("");
    const more = recent ? "" : timelineOn() ? state.items.length < state.total ? `<div class="cw-pager"><button type="button" class="cw-btn" data-trk-action="more"${state.loading ? " disabled" : ""}>${TH("tracker.list.showOlder")}</button></div>` : "" : pager();
    return `<div class="trk-cover-grid${coverMode ? " is-posters" : ""}">${cards}</div>${more}`;
  }

  function shelfRef(item) {
    const child = isChild(item);
    const art = item?.art || {};
    const tmdb = art.tmdb || (child ? item?.show_ids?.tmdb : item?.ids?.tmdb) || "";
    if (!tmdb) return null;
    return { type: !child && (art.kind === "movie" || item?.type === "movie") ? "movie" : "tv", id: String(tmdb) };
  }

  function shelfInfo(item) {
    const ref = shelfRef(item);
    return ref ? window.CW?.Shelf?.peek?.(ref.type, ref.id) || null : null;
  }

  function trackerShelfItem(item, index) {
    const shelf = window.CW?.Shelf || {};
    const info = shelfInfo(item) || {};
    const format = (info.formats || [])[0] || "";
    const art = item.art || (item.ids?.tmdb ? {kind: item.type === "movie" ? "movie" : "tv", tmdb: item.ids.tmdb} : null);
    const poster = artUrl({...item, art}, "w342") || "/assets/img/placeholder_poster.svg?v=2";
    const ref = shelfRef(item);
    const disc = ref ? shelf.discUrl?.(ref.type, ref.id, format) || "" : "";
    const kind = shelf.caseKind?.(format, info.packaging || "") || "plain";
    const cover = shelf.caseHtml?.({format, packaging: info.packaging || "", poster}) || `<span class="cw-case" data-case="plain"><img class="cw-case-art" src="${esc(poster)}" alt=""></span>`;
    const title = cardTitle(item);
    const statusLabel = info.status && info.status !== "owned" ? shelf.statuses?.[info.status] || info.status : "";
    const pill = shelf.pill?.({format, media: item.media_info || {}, service: info.service}) || "";
    const count = Number(info.copies) || 0;
    const note = [isChild(item) ? item.series_year : item.year, info.edition, count > 1 ? T("profile.shelf.copies", { count }) : "", info.lent ? T("profile.shelf.lentOut") : ""].filter(Boolean).join(" · ");
    return `<div class="cw-shelfv-item trk-play${disc ? " has-disc" : ""}${info.status === "wishlist" ? " is-wishlist" : ""}" data-case="${esc(kind)}" data-trk-shelf-pop="${index}" data-trk-ref-key="${esc(item.key)}">
      <button type="button" class="cw-shelfv-art" data-trk-card-open data-trk-card-index="${index}" data-trk-card-recent="false" aria-label="${esc([title, statusLabel].filter(Boolean).join(", "))}">
        ${shelf.sceneHtml?.({ cover, disc, reflection: true }) || cover}
        ${statusLabel ? `<span class="cw-shelfv-status" data-status="${esc(info.status)}">${esc(statusLabel)}</span>` : ""}
      </button>
      <span class="cw-shelfv-base" aria-hidden="true"></span>
      <span class="cw-shelfv-cap"><strong title="${esc(title)}">${esc(title)}</strong>${note ? `<small>${esc(note)}</small>` : ""}${pill ? `<span>${pill}</span>` : ""}</span>
    </div>`;
  }

  function trackerShelfPop(item) {
    const year = isChild(item) ? item.series_year : item.year;
    const sub = [year, item.type === "movie" ? T("common.media.movie") : T("common.media.show")].filter(Boolean).join(" · ");
    return window.CW?.Shelf?.popHtml?.({title: cardTitle(item), sub, info: shelfInfo(item) || {}, media: item.media_info || {}}) || "";
  }

  function renderShelf() {
    if (!state.items.length) return emptyState(state.q ? T("tracker.shelf.noSearchMatch") : T("tracker.shelf.empty"), "shelves");
    const groups = new Map();
    state.items.forEach((item, index) => {
      const name = String(shelfInfo(item)?.shelves?.[0] || "").trim();
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name).push([item, index]);
    });
    const ordered = [...groups.entries()].sort(([a], [b]) => (!a ? 1 : !b ? -1 : a.localeCompare(b, undefined, {numeric: true, sensitivity: "base"})));
    const rows = ordered.map(([name, list], rowIndex) => {
      const copies = list.reduce((sum, [item]) => sum + (Number(shelfInfo(item)?.copies) || 0), 0);
      return `<section class="cw-shelfv-row">
        <div class="cw-shelfv-head">${icon("shelves")}<h3>${esc(name || T("profile.shelf.noShelf"))}</h3><span>${esc([plural(list.length, "title", "titles"), plural(copies, "copy", "copies")].join(" · "))}</span>${rowIndex === 0 ? window.CW?.Shelf?.casesSwitch?.() || "" : ""}</div>
        <div class="cw-shelfv-rack">${list.map(([item, index]) => trackerShelfItem(item, index)).join("")}</div>
      </section>`;
    }).join("");
    void window.CW?.Shelf?.load?.();
    return `<div class="cw-shelfv trk-shelfv">${rows}</div>${pager()}`;
  }

  const shelfLayoutOn = () => state.root && state.view === "collection" && state.layouts.collection === "shelf";

  window.addEventListener("cw:shelf-cases", () => {
    if (shelfLayoutOn()) setSlot("main", renderMain());
  });

  window.addEventListener("cw:shelf-changed", (event) => {
    if (event.detail?.currency !== undefined) return;
    if (state.root && state.title) setSlot("title", renderTitle());
    if (event.detail?.loaded) {
      if (shelfLayoutOn()) setSlot("main", renderMain());
      return;
    }
    window.CW?.ViewCache?.discard();
    if (shelfLayoutOn()) void reload();
  });

  function wireTrackerShelfPop(host) {
    if (!host || host.dataset.trkShelfPop) return;
    host.dataset.trkShelfPop = "1";
    const show = (node) => {
      const item = state.items[Number(node.dataset.trkShelfPop)];
      if (item) window.CW?.Shelf?.popShow?.(node, trackerShelfPop(item), node.querySelector(".cw-shelfv-art") || node);
    };
    host.addEventListener("pointerover", (event) => {
      if (event.pointerType && event.pointerType !== "mouse") return;
      const node = event.target?.closest?.(".cw-shelfv-item[data-trk-shelf-pop]");
      if (node) show(node);
    });
    host.addEventListener("pointerout", (event) => {
      const node = event.target?.closest?.(".cw-shelfv-item[data-trk-shelf-pop]");
      if (node && !node.contains(event.relatedTarget)) window.CW?.Shelf?.popHide?.();
    });
    host.addEventListener("focusin", (event) => {
      const node = event.target?.closest?.(".cw-shelfv-item[data-trk-shelf-pop]");
      if (node) show(node);
    });
    host.addEventListener("focusout", () => window.CW?.Shelf?.popHide?.());
    host.addEventListener("click", () => window.CW?.Shelf?.popHide?.());
  }

  function gridCardContext(button) {
    const recent = button.dataset.trkCardRecent === "true";
    const rows = recent ? overviewRows("recent") : state.view === "shows" ? state.shows : state.view === "titles" ? state.noteRows : state.items;
    const row = rows[Number(button.dataset.trkCardIndex)];
    return {row, recent, ref:row ? recent ? row.ref : rowRef(button.closest(".trk-play")) : null};
  }

  function openGridActions(button) {
    const {row, recent, ref} = gridCardContext(button);
    if (!row) return;
    const feature = recent ? "history" : listFeature();
    const disabled = window.CW?.AuthState?.read?.()?.permissions?.write === false || !!state.overviewBusy;
    const media = canCard(cardItem(row, !!ref.show || isChild(row) || row.type === "show"));
    const entries = [{icon:"info", label:T("tracker.actions.viewDetails"), run:() => selectRef(ref, {openMedia:media})}];
    if (ref.show || ref.note || !recent && state.view === "watched" && row.type === "show") {
      if (ref.show || row.type === "show") entries.push({icon:"edit", label:T("tracker.actions.editTitle"), disabled, run:() => selectRef(ref, {action:"edit-title"})});
      entries.push({icon:"notes", label:T("tracker.actions.myNotes"), disabled, run:() => selectRef(ref, {action:"notes"})});
    } else {
      const label = !recent && ["movies", "watched"].includes(state.view) ? T("tracker.actions.editLatestWatch") : feature === "history" ? T("tracker.actions.editWatch") : feature === "progress" ? T("profile.playback.edit") : feature === "ratings" ? T("tracker.actions.editRating") : T("tracker.actions.editItem");
      entries.push({icon:"edit", label, disabled, run:() => openSheet("edit", {item:row, feature})});
    }
    if (feature !== "ratings") entries.push({icon:"star", label:T("tracker.actions.rate"), disabled, run:() => selectRef(ref, {action:"rate"})});
    if (!ref.show && !ref.note && feature === "history") entries.push({icon:"album", label:T("tracker.actions.addCollection"), disabled, run:() => addRowToCollection(row)});
    if (!ref.show && !ref.note && feature === "collection" && shelfRef(row)) entries.push({icon:"shelves", label:shelfMenuLabel(row), disabled, run:() => openMyShelf(row)});
    entries.push({icon:ref.feature === "history" ? "replay" : "check", label:ref.feature === "history" ? T("tracker.actions.logRewatch") : T("tracker.actions.logWatch"), attrs:' data-trk-watch-label', disabled, run:() => cardQuickAction(button, "log", row, ref)});
    entries.push({icon:"do_not_disturb_on", label:T("tracker.actions.markDropped"), disabled:disabled || row.status === "dropped", run:() => cardQuickAction(button, "drop", row, ref)});
    const menu = window.CW.CardActions.open(button, entries, {title:cardTitle(row),subtitle:[episodeLabel(row),row.episode_title].filter(Boolean).join(" · ")});
    void updateWatchLabel(menu, row, ref);
  }

  function shelfMenuLabel(row) {
    return shelfInfo(row) ? T("tracker.actions.editShelf") : T("tracker.actions.addShelf");
  }

  function loadShelfEditor() {
    if (window.CW?.ShelfEditor) return Promise.resolve(window.CW.ShelfEditor);
    shelfEditorLoad ||= new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = `/assets/js/shelf-editor.js${trackerAssetVersion ? `?v=${encodeURIComponent(trackerAssetVersion)}` : ""}`;
      script.onload = () => (window.CW?.ShelfEditor ? resolve(window.CW.ShelfEditor) : reject(new Error("missing")));
      script.onerror = () => { shelfEditorLoad = null; reject(new Error("load failed")); };
      document.head.appendChild(script);
    });
    return shelfEditorLoad;
  }

  async function openMyShelf(row) {
    const ref = shelfRef(row);
    if (!ref) return;
    let editor = null;
    try { editor = await loadShelfEditor(); } catch { editor = null; }
    if (!editor) return;
    const child = isChild(row);
    const synced = Array.isArray(row.copies) && row.copies[0]?.format ? {format: row.copies[0].format, media_info: row.media_info || {}} : {format: row.format || "", media_info: row.media_info || {}};
    editor.open({
      type: ref.type,
      id: ref.id,
      title: child ? row.series_title || row.title || "" : row.title || "",
      year: child ? row.series_year || "" : row.year || "",
      ids: {...((child ? row.show_ids : row.ids) || {}), tmdb: ref.id},
      canEdit: window.CW?.AuthState?.read?.()?.permissions?.write !== false,
      synced,
    });
  }

  async function addRowToCollection(row) {
    const child = isChild(row);
    const art = row.art || {};
    const ids = { ...((child ? row.show_ids : row.ids) || {}) };
    if (art.tmdb) ids.tmdb = art.tmdb;
    const item = { type: child || row.type === "show" ? "show" : "movie", title: child ? row.series_title || row.title : row.title, year: child ? row.series_year : row.year, ids };
    try {
      const data = await getJSON("/api/tracker/add", { feature: "collection", provider_instance: state.instance, item });
      toast.show(T("tracker.toast.addedCollection", { title: item.title }), { undo: data?.batch ? () => undo(data.batch) : null });
      await refresh();
    } catch (err) {
      if (err?.code === "already_there") toast.show(`${item.title} is already in your collection`);
      else toast.show(writeError(err), { tone: "bad" });
    }
  }

  async function cardQuickAction(button, action, row, ref) {
    if (button.disabled || state.overviewBusy || window.CW?.AuthState?.read?.()?.permissions?.write === false) return;
    if (action === "log") { selectRef(ref, {action:"log", watchItem:row}); return; }
    const instance = state.instance;
    state.overviewBusy = row.key;
    button.disabled = true;
    try {
      const result = await getJSON("/api/tracker/titles/status", {provider_instance:instance, keys:[ref.show || ref.note || ref.key], status:"dropped", view:ref.show ? "shows" : ref.note ? "titles" : ref.feature, feature:ref.feature || "history"});
      toast.show(T("tracker.toast.dropped", { title: cardTitle(row) }), {undo:result.batch ? () => undo(result.batch, instance) : null});
      if (instance === state.instance) await refresh();
    } catch (err) { toast.show(writeError(err), {tone:"bad"}); }
    finally { state.overviewBusy = ""; button.disabled = false; renderSlots(false); }
  }

  function renderList() {
    const feature = listFeature();
    const f = featureOf(feature);
    const clean = state.view === "cleanup";
    if (!state.items.length) {
      const text = state.loading ? T("common.status.loading") : state.noteStatus || state.noteTag || (state.view === "history" && state.duplicates) ? T("tracker.empty.noRecordsFilters") : clean ? (state.healthFilter === "attention" ? T("tracker.empty.nothingAttention", { feature: f.label.toLowerCase() }) : T("tracker.empty.noRecordsFilter")) : state.q || state.media ? T("tracker.empty.noRecordsFilters") : T("tracker.empty.featureEmpty", { feature: f.label.toLowerCase() });
      return emptyState(text, state.loading ? "hourglass_empty" : clean ? "verified" : f.icon);
    }
    const allPicked = state.items.every((i) => state.picked.has(i.key));
    const body = state.items.map((item, index) => {
      const ep = episodeCode(item);
      const year = isChild(item) ? item.series_year : item.year;
      const selected = state.ref?.key === item.key && state.ref?.feature === feature;
      return `<tr class="trk-play${selected ? " is-selected" : ""}" data-trk-ref-key="${esc(item.key)}" tabindex="0" aria-selected="${selected}">
          <td class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="${index}"${state.picked.has(item.key) ? " checked" : ""} aria-label="Select ${esc(cardTitle(item))}"></td>
          <td class="cw-col-type">${typePill(item)}</td>
          <td class="cw-col-title">${listIdentity(item, `<div class="cw-title-row"><strong class="cw-merged-title">${esc(cardTitle(item))}</strong>${ep ? `<span class="trk-ep">${esc(ep)}</span>` : ""}</div>`, f.date ? feature === "history" ? whenText(item.date) : dateText(item.date) : "")}</td>
          <td class="cw-col-year"><span class="cw-merged-year">${esc(year || "")}</span></td>
          ${f.date ? `<td class="trk-col-date">${esc(feature === "history" ? whenText(item.date) : dateText(item.date))}</td>` : ""}
          ${extraColumn(feature) ? `<td class="trk-col-extra${feature !== "progress" ? " trk-col-rating" : ""}">${extraCell(item, feature)}</td>` : ""}
          <td class="trk-col-health"><span class="trk-row-tools"><button type="button" class="trk-icon trk-row-edit" data-trk-row-edit="${index}" title="${TH("common.actions.edit")}" aria-label="${TH("tracker.actions.editNamed", { title: cardTitle(item) })}">${icon("edit")}</button>${healthIcon(item, clean)}</span></td>
        </tr>`;
    }).join("");
    const head = `<th class="trk-col-pick"><input type="checkbox" class="cw-checkbox" data-trk-pick="page"${allPicked ? " checked" : ""} aria-label="${TH("tracker.list.selectPage")}"></th>`
      + th(T("tracker.list.colType"), "cw-col-type") + th(T("tracker.list.colTitle"), "cw-col-title") + th(T("tracker.list.colYear"), "cw-col-year")
      + (f.date ? th(f.date, "trk-col-date") : "") + (extraColumn(feature) ? th(extraColumn(feature), feature === "progress" ? "trk-col-extra" : "trk-col-extra trk-col-rating") : "")
      + th(clean ? "TMDB" : "", "trk-col-health");
    const cols = ["pick", "type", "title", "year", ...(f.date ? ["date"] : []), ...(extraColumn(feature) ? [feature === "progress" ? "extra" : "rating"] : []), "health"];
    return tableWrap(head, body, "trk-list", cols) + pager();
  }

  function importSource(job) {
    return `${job.source_label || providerName(job.source)}${job.source_instance && job.source_instance !== "default" ? ` (${job.source_instance_label || job.source_instance})` : ""}`;
  }

  function importSeen() {
    try {
      return localStorage.getItem(IMPORT_SEEN_KEY) || "";
    } catch {
      return "";
    }
  }

  function renderImportBanner() {
    const job = state.imp;
    if (!job?.id || job.state === "idle" || (job.state !== "running" && importSeen() === job.id)) return "";
    const from = importSource(job);
    if (job.state === "running") {
      const writing = job.phase === "writing" && job.total;
      const step = Number(job.feature_count) > 1 ? ` · ${T("tracker.import.listOf", { index: Number(job.feature_index) || 0, count: Number(job.feature_count) || 0 })}` : "";
      const text = job.cancel_requested ? T("tracker.import.stopping")
        : writing ? T("tracker.import.writing", { done: Number(job.done) || 0, total: Number(job.total) || 0, step })
        : job.phase === "reading" ? T("tracker.import.reading", { from, step })
        : T("tracker.import.starting");
      const pct = writing ? Math.min(100, Math.round(Number(job.progress || 0) * 100)) : 0;
      return `<div class="trk-detail-health trk-import is-running" role="status">${symbol("download")}<div><strong>${TH("tracker.import.importingFrom", { from })}</strong><span>${esc(text)}</span><div class="trk-meter${writing ? "" : " is-busy"}" aria-hidden="true"><i${writing ? ` style="width:${pct}%"` : ""}></i></div></div><button type="button" class="cw-btn" data-trk-action="import-cancel"${job.cancel_requested ? " disabled" : ""}>${icon("stop_circle")}<span>${TH("tracker.import.stop")}</span></button></div>`;
    }
    const added = Number(job.added || 0);
    const extra = [Number(job.unresolved) ? T("tracker.import.unmatched", { count: Number(job.unresolved) }) : "", Number(job.errors) ? `${plural(job.errors, "error", "errors")}` : ""].filter(Boolean).join(" · ");
    let title, text, tone = "is-ok", mark = "check_circle";
    if (job.state === "completed") {
      title = T("tracker.import.finished", { from });
      text = added ? (extra ? T("tracker.import.addedExtra", { records: plural(added, "record", "records"), extra }) : T("tracker.import.added", { records: plural(added, "record", "records") })) : extra ? T("tracker.import.nothingNewExtra", { extra }) : T("tracker.import.nothingNew");
    } else if (job.state === "cancelled") {
      title = T("tracker.import.stopped", { from });
      text = T("tracker.import.stoppedText", { records: plural(added, "record was", "records were") });
      tone = "is-attention";
      mark = "block";
    } else {
      title = T("tracker.import.notFinished", { from });
      text = job.state === "interrupted" ? T("tracker.import.restarted") : T("tracker.import.errorText");
      tone = "is-attention";
      mark = "error_outline";
    }
    const check = added && state.tmdbKey ? `<button type="button" class="cw-btn" data-trk-action="import-check">${icon("health_and_safety")}<span>${TH("tracker.import.checkTmdb")}</span></button>` : "";
    return `<div class="trk-detail-health trk-import ${tone}" role="status">${symbol(mark)}<div><strong>${esc(title)}</strong><span>${esc(text)}</span></div>${check}<button type="button" class="trk-icon" data-trk-action="import-dismiss" title="${TH("tracker.import.dismiss")}" aria-label="${TH("tracker.import.dismiss")}">${icon("close")}</button></div>`;
  }

  function renderMain() {
    if (state.moving) {
      const move = state.migration;
      const total = Number(move?.files_total || 0);
      const pct = total ? Math.round((Number(move.files_done || 0) / total) * 100) : 0;
      return emptyState(state.error, "database", false, total ? `<div class="trk-meter" aria-hidden="true"><i style="width:${pct}%"></i></div><div class="cw-status-text">${esc(num(move.files_done))} of ${esc(num(total))} files</div>` : "");
    }
    if (state.error) return emptyState(state.error, "error");
    if (state.view === "overview") return renderOverview();
    if (state.layouts[layoutView()] === "shelf" && state.view === "collection") return renderShelf();
    if (state.layouts[layoutView()] === "grid" || (state.layouts[layoutView()] === "covers" && state.view === "collection")) return renderGrid();
    if (timelineOn()) return renderTimeline();
    if (state.view === "shows") return renderShows();
    if (["movies", "watched"].includes(state.view)) return renderGroupedHistory();
    if (state.view === "titles") return renderTitles();
    return renderList();
  }

  function renderSource() {
    if (!state.instances.length) return "";
    const selected = state.instances.find(i => i.id === state.instance) || state.instances[0];
    const instances = state.instances.map((i) => `<option value="${esc(i.id)}"${i.id === state.instance ? " selected" : ""}>${esc(i.label)}</option>`).join("");
    const choices = state.instances.map(i => `<button type="button" class="cw-btn" role="menuitemradio" aria-checked="${i.id === state.instance}" data-trk-instance="${esc(i.id)}">${symbol("badge", "trk-source-badge")}<span>${esc(i.label)}</span>${i.id === state.instance ? symbol("check", "trk-source-check") : ""}</button>`).join("");
    return `<select id="trk-instance" aria-label="${TH("tracker.instance.label")}" data-trk-input="instance" hidden>${instances}</select><details class="trk-source trk-nav-menu"><summary class="trk-tab" aria-label="Tracker: ${esc(selected.label)}" aria-haspopup="menu" title="${esc(selected.label)}">${symbol("badge", "trk-source-badge")}<span class="trk-source-label">${esc(selected.label)}</span>${symbol("expand_more")}</summary><div class="trk-source-options trk-others-menu cw-page-panel" role="menu" aria-label="${TH("tracker.instance.choose")}">${choices}</div></details>`;
  }

  function selectInstance(value) {
    const select = state.root?.querySelector('#trk-instance');
    if (select && select.value !== value && state.instances.some(instance => instance.id === value)) {
      select.value = value;
      select.dispatchEvent(new Event('change', {bubbles:true}));
    }
  }

  function playbackView() {
    return state.view === "overview" || (state.view === "progress" && !state.ref);
  }

  function heroContext() {
    if (playbackView() && heroPlayback) return {data:heroPlayback, record:heroPlayback.title};
    if (state.view === "overview") return {data:state.overview?.hero ? {title:state.overview.hero, heroMeta:state.overview.heroMeta} : null, record:state.overview?.hero};
    if (!state.ref) {
      const data = state.listHero?.view === state.view && state.listHero.instance === state.instance ? state.listHero : null;
      return {data, record:data?.title};
    }
    const record = state.ref.key ? findRecord(state.ref.key, state.ref.feature) || state.items.find(item => item.key === state.ref.key)
      : state.ref.show ? state.shows.find(item => item.key === state.ref.show) : state.noteRows.find(item => item.key === state.ref.note);
    return {data:state.title, record};
  }

  let heroPlayback = null;
  let heroPlaybackRequest = null;
  let dismissedPlayback = null;

  function updateHeroPlayback() {
    const host = state.root?.querySelector('.trk-hero-playback');
    const stats = state.root?.querySelector('[data-trk-slot="hero"]');
    if (!host || !stats) return;
    const available = playbackView() && heroPlayback;
    const live = available && dismissedPlayback !== heroPlayback.session;
    const restore = state.root.querySelector('[data-trk-action="show-playback"]');
    if (restore) restore.hidden = !available || !!live;
    host.hidden = !live;
    stats.hidden = !!live;
    if (!live) return;
    const item = available.stream;
    const status = String(item.state || item.status).toLowerCase();
    host.querySelector('[data-trk-live="state"]').textContent = status === 'paused' ? T("profile.playback.paused") : status === 'buffering' ? T("profile.playback.buffering") : T("tracker.live.nowPlaying");
    host.querySelector('[data-trk-live="title"]').textContent = cardTitle(available.title);
    const provider = item.source?.provider || item.provider || (typeof item.source === 'string' ? item.source : '');
    host.querySelector('[data-trk-live="meta"]').textContent = [window.CW?.ProviderMeta?.label?.(provider) || provider, episodeCode(available.title)].filter(Boolean).join(' · ');
    const progress = window.CW?.PlayingCard?.progress?.({...item, duration_ms:Number(item.duration_ms) || Number(available.heroMeta?.runtime_minutes) * 60000});
    const pct = progress?.pct ?? Math.max(0, Math.min(100, Number(item.progress) || 0));
    const track = host.querySelector('[role="progressbar"]');
    track.setAttribute('aria-valuenow', String(Math.round(pct)));
    track.firstElementChild.style.width = `${pct}%`;
    host.querySelector('[data-trk-live="percent"]').textContent = `${Math.round(pct)}%`;
    host.querySelector('[data-trk-live="remaining"]').textContent = progress?.remaining || '';
    const image = host.querySelector('img');
    const url = artUrl(available.title, 'w185');
    if (image.dataset.url !== url) {
      image.dataset.url = url;
      image.hidden = !url;
      image.onerror = () => { image.hidden = true; };
      if (url) image.src = url;
      else image.removeAttribute('src');
    }
  }

  async function refreshHeroPlayback() {
    if (!state.root?.getClientRects().length || document.hidden) return;
    const read = window.CW?.PlayingCard?.currentlyWatching;
    if (!read || heroPlaybackRequest) return heroPlaybackRequest;
    const scope = currentProfileScope();
    heroPlaybackRequest = (async () => {
      try {
        const payload = await read();
        if (scope !== currentProfileScope()) return;
        const stream = payload?.primary || payload?.streams?.[0];
        if (!stream) heroPlayback = null;
        else {
          const show = window.CW?.Meta?.isEpisode?.(stream) || ['episode','show','tv'].includes(stream.media_type || stream.type) || stream.season != null;
          const tmdb = window.CW?.Meta?.tmdbId?.(stream) || (show ? stream.ids?.tmdb_show || stream.show_ids?.tmdb : '') || stream.ids?.tmdb || stream.tmdb;
          const head = {...stream, type:show ? 'episode' : 'movie', art:{kind:show ? 'tv' : 'movie',tmdb}};
          const previous = heroPlayback;
          const same = previous?.title.art.tmdb === tmdb && previous?.show === !!show;
          const session = JSON.stringify([scope, stream.source?.provider || stream.source || stream.provider, stream.provider_instance || stream.instance_id || "default", stream.session_key || "", stream.started || "", tmdb || stream.title, stream.season, stream.episode]);
          heroPlayback = {stream,session,title:head,show:!!show,heroMeta:same ? previous.heroMeta : window.CW?.Meta?.peek?.(stream)};
          if (!heroPlayback.heroMeta && !same) {
            const live = heroPlayback;
            void window.CW?.Meta?.get?.(stream,'detail',{seriesInfo:true})?.then(meta => {
              if (heroPlayback?.title.art.tmdb !== tmdb || heroPlayback?.show !== live.show || scope !== currentProfileScope()) return;
              heroPlayback.heroMeta = meta;
              updateHeroSelection();
              updateHeroPlayback();
            }).catch(() => {});
          }
        }
        updateHeroSelection();
        updateHeroBackdrop();
        updateHeroPlayback();
      } catch {} finally { heroPlaybackRequest = null; }
    })();
    return heroPlaybackRequest;
  }

  async function loadListHero() {
    if (state.view === "overview") return;
    const rows = state.view === "shows" ? state.shows : state.view === "titles" ? state.noteRows : state.items;
    const date = row => Date.parse(row.last_watched || row.date || row.updated || "") || 0;
    const head = rows.reduce((latest, row) => !latest || date(row) > date(latest) ? row : latest, null);
    if (!head) { state.listHero = null; return; }
    const show = state.view === "shows" || isChild(head) || head.type === "show";
    const item = cardItem(head, show);
    const data = state.listHero = {title:head, show, view:state.view, instance:state.instance, heroMeta:item ? window.CW?.Meta?.peek?.(item) : null};
    if (state.ref || !item || data.heroMeta) return;
    try {
      data.heroMeta = await window.CW?.Meta?.get?.(item, "detail", {seriesInfo:true});
      if (state.listHero === data && !state.ref && state.view === data.view && state.instance === data.instance) updateHeroSelection();
    } catch {}
  }

  let heroSelectionRequest = 0;
  function updateHeroSelection() {
    const {data, record} = heroContext();
    const head = data?.title || record;
    const title = head ? cardTitle(head) : "";
    const selection = state.root?.querySelector('[data-trk-slot="selection"]');
    state.root?.querySelector(".cw-page-hero")?.classList.toggle("has-selection", !!title);
    if (!title) {
      heroSelectionRequest++;
      if (selection) {
        delete selection.dataset.logoSelection;
        delete selection.dataset.heroHeading;
      }
      setSlot("selection", T("tracker.hero.selection"));
      return;
    }
    const show = data?.show || isChild(head) || head.type === "show";
    const item = cardItem(head, show);
    const meta = data?.heroMeta || (item ? window.CW?.Meta?.peek?.(item) : null);
    const seasons = Number(meta?.detail?.number_of_seasons) || 0;
    const runtime = Number(meta?.runtime_minutes) || 0;
    const summary = [meta?.year || head.year, show ? T("profile.hero.tvSeries") : T("common.media.movie")].filter(Boolean).join(" · ");
    const detail = [show && record ? episodeLabel(record) : "",
      show ? seasons ? plural(seasons, "season", "seasons") : "" : runtime ? `${runtime} min` : "",
      show ? meta?.detail?.status : "", ...(Array.isArray(meta?.genres) ? meta.genres.slice(0, 2) : [])].filter(Boolean).join(" · ");
    const status = statusPill(data?.note ? data.note.status : head.status);
    const episode = record?.type === "episode" && record.title && record.title !== title && !/^S\d+\s*[·-]?\s*E\d+$/i.test(record.title) ? record.title : "";
    const art = head?.art;
    const logoUrl = art?.tmdb ? `/art/tmdb/${art.kind === "movie" ? "movie" : "tv"}/${encodeURIComponent(art.tmdb)}?kind=logo&av=${document.documentElement.dataset.cwArt || "t"}&size=w500${window.CW?.ArtChoice?.param?.(art.kind, art.tmdb) || ""}` : "";
    const details = `<span class="trk-hero-meta">${esc(summary)}${detail ? `<span class="trk-hero-meta-extra"> · ${esc(detail)}</span>` : ""}</span>${episode ? `<small title="${esc(episode)}">${esc(episode)}</small>` : ""}${status}`;
    const heading = JSON.stringify([title, logoUrl]);
    const signature = JSON.stringify([heading, details]);
    if (selection?.dataset.logoSelection === signature) return;
    const shownDetails = selection?.dataset.heroHeading === heading ? selection.querySelector(".trk-hero-details") : null;
    if (shownDetails) {
      heroSelectionRequest++;
      shownDetails.innerHTML = details;
      selection.dataset.logoSelection = signature;
      return;
    }
    const request = ++heroSelectionRequest;
    const swap = window.CW?.HeroSwap;
    Promise.resolve(swap ? swap.preload(logoUrl) : "slow").then((result) => {
      if (request !== heroSelectionRequest) return;
      const apply = () => {
        const ready = result === "ready";
        setSlot("selection", `<div class="trk-hero-selection"><div class="trk-hero-title-line"><span class="trk-hero-heading"><strong title="${esc(title)}"${ready ? " hidden" : ""}>${esc(title)}</strong>${logoUrl ? `<img class="trk-hero-logo" alt="${esc(title)}"${ready ? "" : " hidden"}>` : ""}</span><div class="trk-hero-details">${details}</div></div></div>`);
        if (!selection) return;
        selection.dataset.logoSelection = signature;
        selection.dataset.heroHeading = heading;
        const logo = selection.querySelector(".trk-hero-logo");
        const label = selection.querySelector(".trk-hero-heading strong");
        if (!logo) return;
        logo.onload = () => { logo.hidden = false; label.hidden = true; };
        logo.onerror = () => { logo.hidden = true; label.hidden = false; };
        logo.src = logoUrl;
      };
      if (swap) swap.fade(selection, apply);
      else apply();
    });
  }

  function updateHeroBackdrop() {
    const image = state.root?.querySelector(".trk-hero-backdrop");
    if (!image) return;
    const {data, record} = heroContext();
    const item = data?.title || record;
    const art = item?.art;
    const kind = art?.kind === "movie" ? "movie" : "tv";
    const url = art?.tmdb ? `/art/tmdb/${kind}/${encodeURIComponent(art.tmdb)}?fallback=2&kind=backdrop&av=${document.documentElement.dataset.cwArt || "t"}&size=w1280${window.CW?.ArtChoice?.param?.(kind, art.tmdb) || ""}` : "";
    if (image.dataset.url === url) return;
    image.dataset.url = url;
    image.onload = () => { image.hidden = image.dataset.url !== url; };
    image.onerror = () => { image.hidden = true; };
    if (url) image.src = url;
    else {
      image.hidden = true;
      image.removeAttribute("src");
    }
  }

  function renderTitle() {
    updateHeroSelection();
    updateHeroBackdrop();
    updateHeroPlayback();
    if (!state.ref) {
      return `<div class="ins-row"><div class="ins-icon">${symbol("info")}</div><div class="ins-title">${TH("tracker.detail.titlePlaceholder")}</div></div>
        <div class="trk-detail-empty">${symbol("touch_app")}<span>${TH("tracker.detail.pickRow")}</span></div>`;
    }
    if (!state.title) return `<div class="trk-detail-empty">${symbol("hourglass_empty")}<span>${TH("common.status.loading")}</span></div>`;
    return titleBody(state.title);
  }

  function watchProgressSummary(data, progress) {
    const total = Math.max(0, Number(progress?.total) || 0);
    const watched = Math.max(0, Number(progress?.watched) || 0);
    const pct = total ? Math.min(100, Math.round(watched / total * 100)) : 0;
    const unit = (value, label) => new Intl.NumberFormat((window.CW?.I18n || window.__cwI18n)?.locale, {style:"unit", unit:label, unitDisplay:"long", maximumFractionDigits:1}).format(value);
    const elapsed = ms => {
      const days = Math.max(0, ms / 86400000);
      if (days >= 365.25) return unit(days / 365.25, "year");
      if (days >= 30.44) return unit(days / 30.44, "month");
      if (days >= 1) return plural(Math.floor(days), "day", "days");
      if (ms >= 3600000) return unit(ms / 3600000, "hour");
      return ms >= 60000 ? plural(Math.floor(ms / 60000), "minute", "minutes") : T("tracker.detail.lessThanMinute");
    };
    const dates = rows => rows.map(row => Date.parse(row.date)).filter(value => Number.isFinite(value) && value > 0 && value <= Date.now());
    const age = stamp => Date.now() - stamp < 60000 ? T("dashboard.banner.justNow") : T("tracker.detail.ago", { time: elapsed(Date.now() - stamp) });
    const row = (label, value, hint = "") => `<div><dt>${label}</dt><dd${hint ? ` title="${esc(hint)}"` : ""}>${esc(value)}</dd></div>`;
    const stats = [];
    if (data.history_complete !== false) {
      const listed = dates(data.watchlist || []), plays = dates(data.plays || []);
      if (listed.length) { const stamp = Math.min(...listed); stats.push(row(T("tracker.detail.addedWatchlist"), age(stamp), whenText(new Date(stamp).toISOString()))); }
      if (plays.length) {
        const first = Math.min(...plays), last = Math.max(...plays);
        stats.push(row(T("tracker.detail.firstWatched"), age(first), whenText(new Date(first).toISOString())));
        stats.push(row(T("profile.hero.lastWatched"), age(last), whenText(new Date(last).toISOString())));
        const runtimes = data.plays.map(play => Number(play.check?.runtime));
        if (runtimes.every(minutes => Number.isFinite(minutes) && minutes > 0)) stats.push(row(T("tracker.detail.timeWatched"), `≈ ${unit(runtimes.reduce((sum, minutes) => sum + minutes, 0) / 60, "hour")}`, T("tracker.detail.timeWatchedHint")));
        if (plays.length > 1) stats.push(row(T("tracker.detail.span"), first === last ? T("tracker.detail.sameDay") : elapsed(last - first), T("tracker.detail.spanHint")));
      }
    }
    if (!progress?.available && !stats.length) return "";
    return `<div class="trk-watch-summary"><div class="trk-watch-summary-head"><strong>${TH("tracker.detail.watchProgress")}</strong>${progress?.available && total ? `<strong>${pct}%</strong>` : ""}</div>
      ${progress?.available && total ? `<span class="trk-progress" role="progressbar" aria-label="${TH("tracker.list.episodesWatched")}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}" aria-valuetext="${esc(`${num(watched)} of ${num(total)} episodes watched`)}"><i style="width:${pct}%"></i></span><div class="trk-watch-summary-count"><span>${esc(num(watched))} of ${esc(num(total))} episodes watched</span><span>${esc(plural(Math.max(0, total - watched), "episode", "episodes"))} left</span></div>` : ""}
      ${stats.length ? `<dl class="trk-watch-summary-stats">${stats.join("")}</dl>` : ""}</div>`;
  }

  function seasonBlock(data) {
    const tmdb = data.title?.art?.tmdb;
    const progress = tmdb ? state.progress[tmdb] : null;
    const watched = new Map(data.seasons.map((s) => [s.season, new Map(s.episodes.map((e) => [e.episode, e.watched_at]))]));
    const counts = progress?.available ? progress.seasons : data.seasons.map((s) => ({ season: s.season, episodes: Math.max(...s.episodes.map((e) => e.episode)) }));
    const head = watchProgressSummary(data, progress);
    if (!counts.length) return head ? `<div class="trk-section">${head}</div>` : "";
    const ordered = [...counts].sort((a, b) => Number(b.season) - Number(a.season));
    const rows = (state.seasonsExpanded ? ordered : ordered.slice(0, 3)).map((s) => {
      const seen = watched.get(s.season) || new Map();
      const done = [...seen.keys()].filter((e) => e <= s.episodes).length;
      const open = state.seasonOpen.has(s.season);
      const chips = open ? `<div class="trk-eps">${Array.from({ length: s.episodes }, (_, i) => i + 1).map((e) => {
        const when = seen.get(e);
        return when
          ? `<span class="trk-ep-chip is-watched" title="${TH("tracker.ui.watchedWhen", {when: whenText(when)})}">${e}</span>`
          : `<button type="button" class="trk-ep-chip" data-trk-log-ep="${s.season}:${e}" title="${TH("tracker.ui.logNow", {code: se(s.season, e)})}">${e}</button>`;
      }).join("")}</div>` : "";
      return `<div class="trk-season"><button type="button" class="trk-season-head" data-trk-season="${s.season}" aria-expanded="${open}"><span>${esc(s.season ? T("tracker.ui.seasonN", {n: String(s.season)}) : T("tracker.ui.specials"))}</span><span class="trk-progress"><i style="width:${Math.round((done / s.episodes) * 100)}%"></i></span><small>${esc(`${done}/${s.episodes}`)}</small>${symbol(open ? "expand_less" : "expand_more")}</button>${chips}</div>`;
    }).join("");
    const more = counts.length > 3 ? `<button type="button" class="cw-btn trk-detail-more" data-trk-expand="seasons" aria-expanded="${state.seasonsExpanded}">${state.seasonsExpanded ? TH("tracker.ui.showLess") : TH("tracker.ui.showMore", {what: plural(counts.length - 3, "more season", "more seasons")})}</button>` : "";
    return `<div class="trk-section">${head}<div class="trk-seasons">${rows}</div>${more}</div>`;
  }

  const cardModal = () => window.CW?.ProfileMediaModal || null;

  function cardItem(head, show) {
    const tmdb = head?.art?.tmdb || head?.ids?.tmdb;
    if (!tmdb) return null;
    return { type: show ? "show" : "movie", title: head.title || "", year: head.year || "", ids: { tmdb: String(tmdb), ...(head.ids?.imdb && !show ? { imdb: head.ids.imdb } : {}) } };
  }

  const canCard = (item) => !!item && !!cardModal()?.canOpen?.(item);
  const instanceLabel = () => state.instances.find((i) => i.id === state.instance)?.label || (state.instance === "default" ? T("common.labels.default") : state.instance);

  function openCard() {
    const data = state.title;
    const item = data ? cardItem(data.title, data.show) : null;
    if (!canCard(item)) return;
    if (sheet.mode === "title") closeSheet();
    cardModal().open(item, { tracker: { label: instanceLabel(), data, onAction: cardAction } });
  }

  function cardAction(action, key) {
    if (action === "status") return quickStatus(key);
    if (action === "edit") {
      const record = findRecord(key, "history");
      if (record) openSheet("edit", { item: record, feature: record.feature || "history" });
    } else if (action === "delete") {
      openSheet("delete", { keys: [key], feature: "history" });
    } else {
      if (action === "map") cardModal()?.close?.();
      detailAction(action);
    }
  }

  function previewCard(item) {
    if (canCard(item)) cardModal().open(item, { stacked: true });
  }

  function statusPicker(current) {
    const s = statusOf(current);
    const open = state.statusMenu;
    const label = s ? `${symbol(s.icon)}<span>${esc(s.label)}</span>` : `${symbol("flag")}<span>${TH("tracker.notes.setStatus")}</span>`;
    const items = [...STATUSES, ...(s ? [{ id: "", label: T("tracker.filters.noStatus"), icon: "remove" }] : [])].map((o) => {
      const on = o.id === current;
      return `<button type="button" class="trk-status-item${on ? " is-current" : ""}" role="menuitemradio" aria-checked="${on}" data-trk-set-status="${o.id}"${o.id ? ` data-status="${o.id}"` : ""}>${symbol(o.icon)}<span>${esc(o.label)}</span>${on ? symbol("check", "trk-status-check") : ""}</button>`;
    }).join("");
    return `<div class="trk-status-pick">
        <button type="button" class="trk-status-btn${s ? "" : " is-empty"}"${s ? ` data-status="${s.id}"` : ""} data-trk-detail="status-menu" aria-haspopup="menu" aria-expanded="${open}">${label}${symbol(open ? "expand_less" : "expand_more", "trk-status-caret")}</button>
        ${open ? `<div class="trk-status-menu" role="menu" aria-label="${TH("tracker.filters.status")}">${items}</div>` : ""}
      </div>`;
  }

  function statusHost() {
    return sheet.mode === "title" && sheet.el && !sheet.el.hidden ? sheet.el : state.root;
  }

  function setStatusMenu(open, { focus = true } = {}) {
    if (state.statusMenu === open) return;
    state.statusMenu = open;
    setSlot("title", renderTitle());
    if (sheet.mode === "title") renderSheet();
    if (!focus) return;
    const host = statusHost();
    const target = open
      ? host?.querySelector('.trk-status-menu [aria-checked="true"]') || host?.querySelector(".trk-status-menu [data-trk-set-status]")
      : host?.querySelector(".trk-status-btn");
    target?.focus();
  }

  let statusWired = false;

  function wireStatusMenu() {
    if (statusWired) return;
    statusWired = true;
    document.addEventListener("click", (event) => {
      if (state.statusMenu && !event.target.closest?.(".trk-status-pick")) setStatusMenu(false, { focus: false });
    }, true);
    document.addEventListener("keydown", (event) => {
      if (!state.statusMenu || event.key !== "Escape") return;
      event.preventDefault();
      event.stopPropagation();
      setStatusMenu(false);
    }, true);
  }

  async function quickStatus(status) {
    const data = state.title;
    if (!data?.note_key) return;
    state.statusMenu = false;
    const draft = noteDraft(data);
    draft.status_manual = true;
    draft.status = status;
    if (status === "completed" && !draft.finished) draft.finished = today();
    if (status === "watching" && !draft.started) draft.started = today();
    const name = draft.title || "this title";
    const text = status ? T("tracker.toast.markedStatus", { name, status: statusOf(status).label.toLowerCase() }) : T("tracker.toast.clearedStatus", { name });
    try {
      await postNote(draft, text);
    } catch (err) {
      toast.show(writeError(err), { tone: "bad" });
      setSlot("title", renderTitle());
    }
  }

  function noteBlock(data) {
    if (!data.note_key) return "";
    const n = data.note;
    const dates = [n?.started ? T("tracker.notes.startedOn", { date: dayText(n.started) }) : "", n?.finished ? T("tracker.notes.finishedOn", { date: dayText(n.finished) }) : ""].filter(Boolean).join(" · ");
    const fields = state.noteFields
      .filter((f) => fieldOk(f, n?.fields?.[f.id]))
      .map((f) => `<div class="trk-note-field"><dt>${esc(f.name)}</dt><dd>${esc(fieldText(f, n.fields[f.id]))}</dd></div>`)
      .join("");
    const hasContent = !!(dates || n?.tags?.length || n?.notes || fields);
    const head = `<div class="trk-section-title trk-note-head"><span>${TH("tracker.actions.myNotes")}</span>${hasContent ? `<button type="button" class="trk-icon" data-trk-detail="notes" title="${TH("tracker.notes.edit")}" aria-label="${TH("tracker.notes.edit")}">${icon("edit")}</button>` : ""}</div>`;
    return `<div class="trk-section trk-note-block">${head}
        ${dates ? `<div class="trk-note-line"><span class="trk-sub">${esc(dates)}</span></div>` : ""}
        ${tagChips(n?.tags)}
        ${n?.notes ? `<p class="trk-note-text">${esc(n.notes)}</p>` : ""}
        ${fields ? `<dl class="trk-note-fields">${fields}</dl>` : ""}
        ${hasContent ? "" : `<button type="button" class="trk-note-empty" data-trk-detail="notes">${icon("add")}<span>${TH("tracker.notes.add")}</span></button>`}
      </div>`;
  }

  function listBlock(data) {
    if (data.collection?.length) void window.CW?.Shelf?.load?.();
    const rows = [];
    const add = (feature, r, label, when) => rows.push({ feature, r, label, when });
    for (const r of data.ratings) add("ratings", r, T("tracker.detail.ratedOutOf", { value: String(r.rating ?? "?") }), r.date);
    for (const r of data.watchlist) add("watchlist", r, T("tracker.detail.onWatchlist"), r.date);
    for (const r of data.collection) add("collection", r, [T("tracker.detail.inCollection"), Array.isArray(r.copies) && r.copies.length ? plural(r.copies.length, "synced copy", "synced copies") : "", shelfInfo({...r, art: r.art || state.title?.title?.art}) ? "on my shelf" : ""].filter(Boolean).join(" · "), r.date);
    for (const r of data.progress) add("progress", r, `${Math.round(Number(r.progress_percent) || 0)}% watched`, r.date);
    if (!rows.length) return "";
    const limit = 8;
    const items = rows.slice(0, limit).map(({ feature, r, label, when }) => {
      const f = featureOf(feature);
      const ep = data.show && isChild(r) ? episodeLabel(r) : "";
      const ref = esc(`${feature}|${r.key}`);
      return `<li class="trk-play-row"><span class="trk-play-when">${symbol(f.icon)}${esc(label)}${ep ? ` · ${esc(ep)}` : ""}${when ? `<small>${esc(dateText(when))}</small>` : ""}</span><span class="trk-play-actions">${feature === "collection" && shelfRef({...r, art: r.art || state.title?.title?.art}) ? `<button type="button" class="trk-icon" data-trk-rec-copies="${ref}" title="${esc(shelfMenuLabel({...r, art: r.art || state.title?.title?.art}))}" aria-label="${esc(shelfMenuLabel({...r, art: r.art || state.title?.title?.art}))}">${icon("shelves")}</button>` : ""}<button type="button" class="trk-icon" data-trk-rec-edit="${ref}" title="${TH("common.actions.edit")}" aria-label="${TH("tracker.detail.editEntry", { feature: f.label.toLowerCase() })}">${icon("edit")}</button><button type="button" class="trk-icon is-danger" data-trk-rec-delete="${ref}" title="${TH("common.actions.remove")}" aria-label="${TH("tracker.detail.removeFrom", { feature: f.label.toLowerCase() })}">${icon("delete")}</button></span></li>`;
    }).join("");
    const more = rows.length > limit ? `<li class="trk-sub">${esc(plural(rows.length - limit, "more entry", "more entries"))}</li>` : "";
    return `<div class="trk-section"><div class="trk-section-title">${TH("tracker.detail.inLists")}</div><ul class="trk-play-list trk-record-list">${items}${more}</ul></div>`;
  }

  const episodePreviewSeasons = new Map();

  async function loadEpisodePreview(tmdb, season) {
    const key = `${tmdb}:${season}`;
    if (episodePreviewSeasons.has(key)) return;
    episodePreviewSeasons.set(key, null);
    try {
      const data = await getJSON(`/api/metadata/tmdb/season?tmdb=${encodeURIComponent(tmdb)}&season=${encodeURIComponent(season)}`);
      episodePreviewSeasons.set(key, data.episodes || []);
    } catch {
      episodePreviewSeasons.set(key, []);
    }
    if (String(state.title?.title?.art?.tmdb) === String(tmdb)) {
      setSlot("title", renderTitle());
      if (sheet.mode === "title") renderSheet();
    }
  }

  function episodePreviews(data) {
    const tmdb = data.title?.art?.tmdb;
    const progress = state.progress[tmdb];
    if (!data.show || !tmdb || !progress?.available) return "";
    const first = progress.next || progress.next_air;
    if (!first) return "";
    const seasons = [...(progress.seasons || [])].sort((a, b) => a.season - b.season);
    const index = seasons.findIndex(row => Number(row.season) === Number(first.season));
    const second = index < 0 ? null : Number(first.episode) < Number(seasons[index].episodes)
      ? {season: first.season, episode: Number(first.episode) + 1}
      : seasons[index + 1] ? {season: seasons[index + 1].season, episode: 1} : null;
    const cards = [first, second].filter(Boolean).map((pos, i) => {
      const key = `${tmdb}:${pos.season}`;
      if (!episodePreviewSeasons.has(key)) void loadEpisodePreview(tmdb, pos.season);
      const ep = episodePreviewSeasons.get(key)?.find(row => Number(row.episode) === Number(pos.episode));
      if (!ep) return "";
      const future = ep.air_date && ep.air_date > new Date().toISOString().slice(0, 10);
      const label = TH(i ? "tracker.detail.nextEpisode" : future ? "tracker.detail.upcomingEpisode" : progress.watched ? "profile.overview.continueWatching" : "tracker.detail.startWatching");
      const art = `/art/tmdb/tv/${encodeURIComponent(tmdb)}?fallback=2&kind=still&season=${encodeURIComponent(pos.season)}&episode=${encodeURIComponent(pos.episode)}&size=w300`;
      return `<article class="trk-episode-preview"><strong class="trk-episode-heading">${label}</strong><img class="trk-episode-backdrop" src="${esc(art)}" alt="" loading="lazy" decoding="async"><div class="trk-episode-copy"><strong>${esc(se(pos.season, pos.episode))}${ep.name ? ` · ${esc(ep.name)}` : ""}</strong><small>${esc([ep.air_date ? (future ? T("tracker.detail.airs", { date: dayText(ep.air_date) }) : dayText(ep.air_date)) : "", ep.runtime ? T("tracker.detail.runtime", { count: Number(ep.runtime) }) : ""].filter(Boolean).join(" · "))}</small>${ep.overview ? `<p>${esc(ep.overview)}</p>` : ""}</div></article>`;
    }).join("");
    return cards ? `<div class="trk-episode-previews">${cards}</div>` : "";
  }

  function titleBody(data) {
    const head = data.title || {};
    const url = artUrl(head);
    const card = canCard(cardItem(head, data.show));
    const t = data.show ? { label: T("common.media.show"), icon: "tv" } : typeMeta(head);
    const rating = data.ratings[0]?.rating ?? null;
    const rawProgress = data.progress[0]?.progress_percent;
    const progress = rawProgress != null && Number.isFinite(Number(rawProgress)) ? Math.round(Math.max(0, Math.min(100, Number(rawProgress)))) : null;
    const progressSummary = progress != null ? `<span class="trk-detail-progress" role="progressbar" aria-label="${TH("dashboard.activity.playbackProgress")}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${progress}" title="${progress}% watched"><span class="trk-detail-progress-track"><i style="width:${progress}%"></i></span><span>${progress}%</span></span>` : "";
    const listed = data.watchlist.length > 0;
    const badges = [
      listed ? `<span class="trk-badge">${symbol("bookmark")}${TH("tracker.ui.watchlist")}</span>` : "",
      data.collection.length ? `<span class="trk-badge">${symbol("video_library")}${TH("tracker.ui.collection")}</span>` : "",
    ].filter(Boolean).join("");
    const issue = data.attention[0];
    const info = issue ? checkInfo(issue) : null;
    const attention = info ? `<div class="trk-detail-health is-attention">${symbol(info.icon)}<div><strong>${esc(info.title)}</strong>${info.text ? `<span>${esc(info.text)}</span>` : ""}<small>${esc(checkedText(issue.check))}</small><span class="trk-inline-actions"><button type="button" class="cw-btn" data-trk-detail="map">${icon("compare_arrows")}<span>${TH("tracker.detail.fixMatch")}</span></button>${info.fix.includes("tmdb") ? `<button type="button" class="cw-btn" data-trk-detail="use-tmdb">${icon("auto_fix_high")}<span>${TH("tracker.detail.useTmdbTitle")}</span></button>` : ""}</span></div></div>` : "";
    const orderedPlays = [...data.plays].sort((a, b) => (Date.parse(b.date) || 0) - (Date.parse(a.date) || 0));
    const plays = (state.playsExpanded ? orderedPlays : orderedPlays.slice(0, 3)).map((p) => `<li class="trk-play-row"><span class="trk-play-when">${esc(whenText(p.date))}</span>${data.show ? `<span class="trk-next">${esc(episodeLabel(p))}</span>` : ""}<span class="trk-play-actions"><button type="button" class="trk-icon" data-trk-play-edit="${esc(p.key)}" title="${TH("tracker.detail.changeDate")}" aria-label="${TH("tracker.detail.changeDate")}">${icon("edit_calendar")}</button><button type="button" class="trk-icon is-danger" data-trk-play-delete="${esc(p.key)}" title="${TH("tracker.detail.deletePlay")}" aria-label="${TH("tracker.detail.deletePlay")}">${icon("delete")}</button></span></li>`).join("");
    const morePlays = data.plays.length > 3 ? `<button type="button" class="cw-btn trk-detail-more" data-trk-expand="plays" aria-expanded="${state.playsExpanded}">${state.playsExpanded ? TH("tracker.ui.showLess") : TH("tracker.ui.showMore", {what: plural(data.plays.length - 3, "more play", "more plays")})}</button>` : "";
    const watchedLine = data.plays.length ? T("tracker.detail.watchedLine", { plays: plural(data.plays.length, data.show ? "episode play" : "play", data.show ? "episode plays" : "plays"), time: agoText(orderedPlays[0].date) }) : T("tracker.detail.notWatched");
    return `
      <button type="button" class="cw-btn trk-back-to-titles" data-trk-detail="back">${icon("arrow_back")}<span>${TH("tracker.detail.backToTitles")}</span></button>
      <div class="trk-detail-top">
        ${card ? `<button type="button" class="trk-poster is-link${url ? "" : " is-empty"}" data-trk-detail="card" title="${TH("tracker.detail.openDetails")}" aria-label="Open details for ${esc(head.title || "")}">` : `<div class="trk-poster${url ? "" : " is-empty"}">`}${url ? `<img src="${esc(url)}" alt="" loading="lazy" decoding="async">` : ""}<span class="trk-poster-fallback">${symbol(t.icon)}</span>${rating != null ? `<span class="cw-rating-score trk-cover-rating" aria-label="Your rating: ${esc(rating)} out of 10"><span>${esc(rating)}</span></span>` : ""}${card ? "</button>" : "</div>"}
        <div class="trk-detail-copy">
          <div class="trk-detail-meta"><span class="trk-detail-kind">${symbol(t.icon)}${esc(t.label)}</span></div>
          <span class="trk-detail-head"><strong class="trk-detail-title">${esc(head.title || "")}</strong>${recordCount(data) ? `<button type="button" class="trk-icon trk-title-edit" data-trk-detail="edit-title" title="${TH("tracker.detail.editTitleHint")}" aria-label="${TH("tracker.detail.editTitleLabel")}">${icon("edit")}</button>` : ""}${card ? `<button type="button" class="trk-icon trk-title-info" data-trk-detail="card" title="${TH("tracker.detail.openDetails")}" aria-label="Open details for ${esc(head.title || "")}">${icon("info")}</button>` : ""}</span>
          ${head.year || rating != null || progress != null ? `<div class="trk-detail-facts">${head.year ? `<span class="trk-detail-sub">${esc(head.year)}</span>` : ""}${rating != null ? `<span class="trk-detail-rating" aria-label="Your rating: ${esc(rating)} out of 10">${symbol("star")}${esc(rating)}/10</span>` : ""}${progressSummary}</div>` : ""}
          <span class="trk-detail-sub">${esc(watchedLine)}</span>
          ${data.note_key ? statusPicker(data.note?.status || "") : ""}
          ${badges ? `<div class="trk-badges">${badges}</div>` : ""}
        </div>
      </div>
      ${attention}
      <div class="trk-detail-actions">
        <button type="button" class="cw-btn primary" data-trk-detail="log" title="${TH("tracker.detail.markWatchedHint")}" aria-label="${TH("tracker.detail.markWatched")}">${icon("add_circle")}</button>
        <button type="button" class="cw-btn" data-trk-detail="rate" title="${TH(rating != null ? "tracker.detail.changeRating" : "tracker.actions.rate")}" aria-label="${TH(rating != null ? "tracker.detail.changeRating" : "tracker.actions.rate")}">${icon(rating != null ? "star" : "star_outline")}</button>
        <button type="button" class="cw-btn" data-trk-detail="list" title="${TH(listed ? "tracker.detail.removeWatchlist" : "tracker.detail.addWatchlist")}" aria-label="${TH(listed ? "tracker.detail.removeWatchlist" : "tracker.detail.addWatchlist")}">${icon(listed ? "bookmark_remove" : "bookmark_add")}</button>
        ${info ? "" : `<button type="button" class="cw-btn" data-trk-detail="map" title="${TH("tracker.detail.fixMatchHint")}" aria-label="${TH("tracker.detail.fixMatch")}">${icon("compare_arrows")}</button>`}
        ${recordCount(data) ? `<button type="button" class="cw-btn" data-trk-detail="origin" title="${TH("tracker.detail.origin")}" aria-label="${TH("tracker.detail.origin")}">${icon("alt_route")}</button>` : ""}
        ${data.note_key ? `<button type="button" class="cw-btn danger" data-trk-detail="remove" title="${TH("tracker.detail.removeHint")}" aria-label="${TH("tracker.detail.removeHint")}">${icon("delete")}</button>` : ""}
      </div>
      ${episodePreviews(data)}
      ${completionSuggestion(data)}
      ${data.show ? seasonBlock(data) : ""}
      ${plays ? `<div class="trk-section"><div class="trk-section-title">${esc(T(data.show ? "tracker.ui.recentPlays" : "tracker.ui.plays"))}</div><ul class="trk-play-list">${plays}</ul>${morePlays}</div>` : ""}
      ${listBlock(data)}
      ${noteBlock(data)}`;
  }

  function renderHealth() {
    const h = healthOf();
    const job = state.check;
    const running = job?.state === "running";
    const tag = running ? [T("tracker.health.checking"), "is-running"] : job?.state === "failed" || job?.state === "interrupted" ? [T("profile.activity.status.stopped"), "is-bad"] : job?.state === "completed" || job?.state === "cancelled" ? [T("common.status.done"), "is-ok"] : [T("dashboard.progress.idle"), ""];
    const metrics = [HEALTH[2], HEALTH[0], HEALTH[1]].map((m) => `<button type="button" class="metric trk-metric is-${m.id}" data-trk-health="${m.id}" aria-pressed="${state.healthFilter === m.id}">${symbol(m.icon)}<div><div class="m-val">${esc(num(h[m.id]))}</div><div class="m-lbl">${esc(m.label)}</div></div></button>`).join("");
    let footer;
    if (running) {
      const pct = job.total ? Math.round((Number(job.done || 0) / job.total) * 100) : 0;
      footer = `<div class="trk-meter" aria-hidden="true"><i style="width:${pct}%"></i></div>
        <div class="trk-health-foot"><span class="cw-status-text">${TH("tracker.health.progress", { done: Number(job.done) || 0, records: plural(job.total, "record", "records") })}</span><button type="button" class="cw-btn" data-trk-action="check-cancel">${icon("stop_circle")}<span>${TH("tracker.import.stop")}</span></button></div>`;
    } else {
      const why = job?.state === "interrupted" ? T("tracker.health.restarted") : job?.state === "failed" ? ERRORS[job.error] || T("tracker.health.stopped") : job?.finished_at ? `${T("tracker.health.lastCheck", { time: agoText(job.finished_at * 1000) })}${job.failed ? ` · ${T("tracker.health.recordsNotChecked", { records: plural(job.failed, "record", "records") })}` : ""}` : T("tracker.health.notCheckedYet");
      footer = `<div class="trk-health-foot"><span class="cw-status-text">${esc(state.tmdbKey ? why : T("tracker.ui.addTmdbKey"))}</span><button type="button" class="cw-btn primary" data-trk-action="check"${state.tmdbKey ? "" : " disabled"}>${icon("health_and_safety")}<span>${TH("tracker.health.checkAll")}</span></button></div>`;
    }
    return `
      <div class="ins-row"><div class="ins-icon">${symbol("health_and_safety")}</div><div class="ins-title">TMDB health</div><span class="cw-tag trk-tag ${tag[1]}"><span class="cw-tag-dot"></span><span>${esc(tag[0])}</span></span></div>
      <p class="trk-note trk-health-note">Checks your records against TMDB using TMDB, IMDb or TVDB IDs to catch wrong matches and missing episodes.</p>
      <div class="ins-row"><div class="ins-metrics"><div class="metric-row trk-metrics">${metrics}</div></div></div>
      ${footer}`;
  }

  function render() {
    const root = state.root;
    if (!root) return;
    if (!root.querySelector(".cw-root")) {
      root.innerHTML = `
        <div class="cw-root">
          <div class="cw-topline cw-page-hero cw-page-hero-tracker cw-media-hero" data-hero-icon="inventory_2">
            <img class="trk-hero-backdrop" alt="" aria-hidden="true" hidden>
            <div class="trk-hero-topbar cw-media-hero-topbar">
              <div class="cw-media-hero-brand">
                <div class="cw-page-hero-kicker cw-media-hero-kicker">CW TRACKER</div>
                <div class="cw-media-hero-identity" data-trk-slot="identity" hidden></div>
              </div>
              <div class="cw-media-hero-account">
                <button type="button" class="cw-btn trk-hero-restore" data-trk-action="show-playback" hidden>${icon("play_circle")}<span>${TH("tracker.hero.showPlayback")}</span></button>
              </div>
            </div>
            <div class="cw-head-copy cw-page-hero-copy">
              <div class="cw-title-row"><div>
                <div class="cw-sub cw-page-hero-sub trk-hero-context" data-trk-slot="selection" aria-live="polite" aria-atomic="true">${TH("tracker.hero.selection")}</div>
              </div></div>
            </div>
            <div class="cw-editor-hero-summary cw-page-hero-actions" data-trk-slot="hero" aria-label="${TH("tracker.hero.stats")}"></div>
            <div class="trk-hero-playback cw-page-hero-actions" hidden>
              <div class="trk-hero-playback-copy">
                <span class="trk-hero-playback-state" data-trk-live="state"></span>
                <strong data-trk-live="title"></strong>
                <span data-trk-live="meta"></span>
                <div class="trk-hero-playback-track" role="progressbar" aria-label="${TH("dashboard.activity.playbackProgress")}" aria-valuemin="0" aria-valuemax="100"><i></i></div>
                <div class="trk-hero-playback-times"><span data-trk-live="percent"></span><span data-trk-live="remaining"></span></div>
              </div>
              <img alt="" hidden>
            </div>
          </div>
          <div class="cw-wrap">
            <div class="cw-main">
              <div class="cw-controls cw-page-toolbar" data-trk-slot="controls"></div>
              <div data-trk-slot="import"></div>
              <div class="cw-hist-timeline trk-months" data-trk-slot="months" hidden></div>
              <div data-trk-slot="main"></div>
            </div>
            <details class="cw-editor-filters" data-trk-slot="filters">
              <summary>${TH("tracker.views.titleDetails")}</summary>
              <aside class="cw-side">
                <div data-trk-slot="source" hidden></div>
                <div class="ins-card cw-page-panel trk-detail" data-trk-slot="title"></div>
                <div class="ins-card cw-page-panel" data-trk-slot="health"></div>
              </aside>
            </details>
          </div>
        </div>`;
      sheet.el = null;
      const selection = document.createElement("div");
      selection.className = "cw-selection-bar";
      selection.dataset.trkSlot = "selection-bar";
      selection.setAttribute("role", "group");
      selection.setAttribute("aria-label", T("tracker.selection.label"));
      selection.hidden = true;
      root.querySelector(".cw-root").appendChild(selection);
      createBulkStatusMenu();
      new ResizeObserver(() => root.style.setProperty("--cw-selection-height", `${selection.offsetHeight}px`)).observe(selection);
      const filters = root.querySelector('[data-trk-slot="filters"]');
      const media = window.matchMedia?.(NARROW);
      const fold = () => {
        filters.open = state.view === "overview" || !media?.matches;
        if (!media?.matches && sheet.mode === 'title') closeSheet(false);
      };
      fold();
      media?.addEventListener?.("change", fold);
      window.matchMedia?.("(max-width:760px)")?.addEventListener?.("change", () => {
        if (root !== state.root || !root.isConnected || !root.getClientRects().length) return;
        fitNavigation();
        if (state.view === "overview") renderOverviewSlot();
        else reload();
      });
      const side = root.querySelector(".cw-side");
      const header = document.querySelector("body > header");
      const updateSticky = () => {
        const top = (header?.getBoundingClientRect().height || 0) + 12;
        root.style.setProperty("--trk-sticky-top", `${top}px`);
        root.style.setProperty("--cw-mobile-header-height", `${top - 12}px`);
        root.style.setProperty("--trk-side-top", `${Math.min(top, window.innerHeight - side.offsetHeight - 12)}px`);
      };
      const observer = new ResizeObserver(updateSticky);
      [side, header].filter(Boolean).forEach(el => observer.observe(el));
      window.addEventListener("resize", updateSticky);
      let recentFrame = 0;
      new ResizeObserver(() => {
        cancelAnimationFrame(recentFrame);
        recentFrame = requestAnimationFrame(() => {
          if (state.view !== "overview") return;
          const grid = root.querySelector('[aria-labelledby="trk-overview-recent"] .trk-cover-grid');
          if (!grid?.clientWidth) return;
          const step = getComputedStyle(grid).gridTemplateColumns.split(/\s+/).length * 2;
          if (step === state.overviewRecentStep) return;
          state.overviewRecentStep = step;
          renderOverviewSlot();
        });
      }).observe(root.querySelector(".cw-main"));
      let navigationWidth = 0;
      new ResizeObserver(([entry]) => {
        if (entry.contentRect.width === navigationWidth) return;
        navigationWidth = entry.contentRect.width;
        fitNavigation();
      }).observe(root.querySelector('.cw-root'));
      document.fonts?.ready.then(() => { if (root === state.root && root.isConnected) fitNavigation(); });
      monthBar = null;
      bar()?.wire();
    }
    renderSlots(true);
  }

  function setSlot(name, html) {
    const slot = state.root?.querySelector(`[data-trk-slot="${name}"]`);
    if (!slot) return;
    let sourceFocus;
    if (name === "controls") {
      const source = state.root.querySelector('[data-trk-slot="source"]');
      if (source?.contains(document.activeElement)) sourceFocus = document.activeElement;
      if (source) state.root.querySelector('.cw-side').prepend(source);
      const navigation = state.root.querySelector(".trk-navigation");
      if (navigation && !slot.contains(navigation)) slot.prepend(navigation);
    }
    if (name === "source") {
      if (slot.dataset.rendered === html) return;
      const focused = slot.contains(document.activeElement);
      slot.innerHTML = html;
      slot.dataset.rendered = html;
      if (focused) slot.querySelector("summary")?.focus({preventScroll:true});
    } else if (name === "main") {
      const previous = mainMarkup.get(slot);
      if (!previous || previous.html !== html || previous.node !== slot.firstChild) {
        const airingCards = window.CW.AiringCard?.capture(slot) || [];
        slot.innerHTML = html;
        rememberOverviewMarkup(slot, html);
        window.CW.AiringCard?.restore(slot, airingCards);
      }
    } else if (slot.innerHTML !== html) slot.innerHTML = html;
    if (name === "controls") {
      fitNavigation();
      if (sourceFocus?.isConnected) sourceFocus.focus({preventScroll:true});
    }
    if (name === "main") {
      window.CW?.Shelf?.popHide?.();
      wireTrackerShelfPop(slot);
      paintMonthlyBanner();
      animateOverviewCards();
    }
    if (name === "source" || name === "health") slot.hidden = !html;
    if (name === "source") fitNavigation();
  }

  function renderSlots(controls) {
    if (!state.root) return;
    state.root.classList.toggle("trk-overview-page", state.view === "overview");
    const overview = state.view === "overview";
    if (overview && state.overview?.scope !== overviewCacheKey()) state.overview = overviewCache.get(overviewCacheKey()) || null;
    state.root.querySelector('[data-trk-slot="filters"]').open = overview || !narrow();
    const selection = state.root.querySelector('[data-trk-slot="selection-bar"]');
    const focusedAction = selection?.contains(document.activeElement) ? document.activeElement.dataset.trkAction : "";
    if (state.picked.size) setSlot("selection-bar", renderSelection());
    if (selection) selection.hidden = !state.picked.size;
    window.CW?.SelectionBar?.update(selection, state.picked.size);
    if (focusedAction && state.picked.size) selection?.querySelector(`[data-trk-action="${focusedAction}"]`)?.focus();
    setSlot("hero", renderHero());
    renderProfilePicker();
    if (controls) {
      const focused = document.activeElement?.dataset?.trkInput;
      const focusedHistoryMedia = document.activeElement?.dataset?.trkHistoryMedia;
      const focusedView = document.activeElement?.closest(".trk-navigation") && document.activeElement.dataset.trkView;
      setSlot("controls", renderControls());
      if (sheet.mode === 'filters' && !sheet.el?.hidden) renderSheet();
      if (focusedView) state.root.querySelector('.trk-tabs [aria-selected="true"]')?.focus({preventScroll:true});
      if (focusedHistoryMedia !== undefined) state.root.querySelector(`[data-trk-history-media="${focusedHistoryMedia}"]`)?.focus({preventScroll:true});
      if (["q", "view", "note-status", "note-tag", "sort", "history-group", "duplicates", "hide-dropped", "status-labels"].includes(focused)) {
        const input = state.root.querySelector(`[data-trk-input="${focused}"]`);
        input?.focus({preventScroll:true});
        if (focused === "q") input?.setSelectionRange?.(input.value.length, input.value.length);
      }
    }
    setSlot("import", renderImportBanner());
    setSlot("main", renderMain());
    setSlot("source", renderSource());
    setSlot("title", renderTitle());
    setSlot("health", state.view === "cleanup" ? renderHealth() : "");
  }

  let monthBar = null;
  const monthFmt = new Intl.DateTimeFormat((window.CW?.I18n || window.__cwI18n)?.locale, { month: "long", year: "numeric", timeZone: "UTC" });
  const monthName = (key) => {
    const [y, m] = String(key).split("-").map(Number);
    return y && m ? monthFmt.format(new Date(Date.UTC(y, m - 1, 1))) : key;
  };

  function bar() {
    if (!monthBar && window.CW?.MonthBar) {
      monthBar = window.CW.MonthBar.create({
        host: () => state.root?.querySelector('[data-trk-slot="months"]'),
        noun: (n) => plural(n, "play", "plays"),
        zoom: state.zoom,
        onZoom: (level) => {
          state.zoom = level;
          saveView();
        },
        onJump: (month) => {
          state.month = month;
          renderSlots(true);
          loadMain();
        },
      });
    }
    return monthBar;
  }

  function renderMonths() {
    bar()?.render(timelineOn() ? state.months : [], {
      visible: new Set(state.items.map((i) => String(i.date || "").slice(0, 7))),
      key: `${state.instance}|${state.media}|${state.q}`,
    });
  }

  let monthsRequest = 0;

  async function loadMonths() {
    const id = ++monthsRequest;
    if (!timelineOn()) {
      state.months = [];
      renderMonths();
      return;
    }
    let months = [];
    try {
      const params = new URLSearchParams({ feature: "history", provider_instance: state.instance, q: state.q, media: state.media, status: state.noteStatus, tag: state.noteTag, duplicates: String(state.duplicates) });
      const data = await getViewJSON(`/api/tracker/months?${params}`);
      months = Array.isArray(data.months) ? data.months : [];
    } catch {
      months = [];
    }
    if (id !== monthsRequest) return;
    state.months = months;
    renderMonths();
  }

  let summaryRequest = 0;

  async function loadSummary() {
    const id = ++summaryRequest;
    const data = await getViewJSON(`/api/tracker/summary?${inst()}`);
    if (id !== summaryRequest) throw Object.assign(new Error("stale"), { code: "stale" });
    state.instances = Array.isArray(data.instances) ? data.instances : [];
    state.instance = data.provider_instance || "default";
    state.counts = data.counts || {};
    state.health = data.health || {};
    state.effects = data.delete_effects || {};
    state.trackerOptions = data.tracker_options || {};
    state.check = data.check || null;
    state.imp = data.import || null;
    state.notes = data.notes || { total: 0, statuses: {}, tags: [] };
    state.noteFields = Array.isArray(data.note_fields) ? data.note_fields : [];
    state.tmdbKey = data.tmdb_key !== false;
    const scope = overviewCacheKey();
    void getViewJSON(`/api/tracker/stats?${inst()}`).then(stats => {
      if (id !== summaryRequest || scope !== overviewCacheKey()) return;
      state.stats = stats;
      setSlot("hero", renderHero());
      rememberOverview();
    }).catch(() => {});
    if (state.fromLink) {
      state.fromLink = false;
      if (!Number(healthOf().attention || 0)) {
        const first = FEATURES.find((f) => Number(state.health?.[f.id]?.attention || 0) > 0);
        if (first) state.cleanFeature = first.id;
      }
    }
  }

  let timelineRefreshPending = false;

  async function loadTimelineRange(params, count, request) {
    const limit = Number(params.get("limit"));
    for (let attempt = 0; attempt < 3; attempt++) {
      const items = [];
      let revision, total = 0, changed = false;
      for (let offset = 0; offset < count; offset += limit) {
        params.set("offset", String(offset));
        const data = await getViewJSON(`/api/tracker/items?${params}`, undefined, undefined, {watch:false, fresh:true, refresh:attempt > 0});
        if (request !== state.request) throw Object.assign(new Error("Cancelled"), {name:"AbortError"});
        if (offset && revision !== data._view?.revision) { changed = true; break; }
        revision = data._view?.revision;
        total = Number(data.total || 0);
        const rows = Array.isArray(data.items) ? data.items : [];
        items.push(...rows);
        if (!rows.length || items.length >= total) break;
      }
      if (!changed) {
        params.set("offset", "0");
        await getViewJSON(`/api/tracker/items?${params}`);
        return {items, total};
      }
    }
    throw new Error(T("tracker.errors.historyChanged"));
  }

  async function loadMain(append = false, quiet = false) {
    stopOverview();
    const id = ++state.request;
    const cacheKey = listCacheKey();
    const restored = quiet || (!append && restoreList());
    let loaded = false;
    state.loading = true;
    timelineRefreshPending = false;
    state.error = "";
    if (!append) {
      if (!restored) { state.items = []; state.shows = []; state.noteRows = []; }
      if (state.view === "overview") state.overview = overviewCache.get(overviewCacheKey()) || (state.overview?.scope === overviewCacheKey() ? state.overview : null);
    }
    if (!quiet) renderSlots(true);
    try {
      if (state.view === "overview") {
        const controller = overviewController = new AbortController();
        const previous = state.overview;
        const data = await getViewJSON(`/api/tracker/overview?${inst()}&day=${today()}`, undefined, controller.signal);
        if (id !== state.request) return;
        const saved = data.shows;
        state.overview = {...data, scope:overviewCacheKey(), instance:state.instance, day:today(), continue:data.continue || [], recent:data.recent || [], next:data.metadata_available === false ? [] : saved?.continue || previous?.next || [], airing:data.metadata_available === false ? [] : saved?.airing || previous?.airing || [], checking:!saved && !previous && data.metadata_available !== false, checked:saved?.checked || 0, total:saved?.total || 0, unavailable:saved?.unavailable || 0};
        void loadOverviewHero(state.overview);
        rememberOverview();
        if (data.metadata_available !== false) void loadOverviewShows(id, state.instance, controller);
      } else if (state.view === "titles") {
        const params = new URLSearchParams({
          provider_instance: state.instance,
          q: state.q,
          status: state.noteStatus,
          tag: state.noteTag,
          offset: String(state.offset),
          limit: String(pageSize()),
        });
        const data = await getViewJSON(`/api/tracker/notes?${params}`);
        if (id !== state.request) return;
        state.noteRows = Array.isArray(data.notes) ? data.notes : [];
        state.total = Number(data.total || 0);
        if (data.summary) state.notes = data.summary;
      } else if (["movies", "watched"].includes(state.view)) {
        const params = new URLSearchParams({provider_instance:state.instance, q:state.q, status:state.noteStatus, tag:state.noteTag, sort:state.sort, media:state.historyMedia, offset:String(state.offset), limit:String(pageSize())});
        const data = await getViewJSON(`/api/tracker/history/titles?${params}`);
        if (id !== state.request) return;
        state.items = Array.isArray(data.items) ? data.items : [];
        state.total = Number(data.total || 0);
      } else if (state.view === "shows") {
        const data = await getViewJSON(`/api/tracker/shows?${inst()}&q=${encodeURIComponent(state.q)}&offset=${state.offset}&limit=${pageSize()}&status=${encodeURIComponent(state.noteStatus)}&tag=${encodeURIComponent(state.noteTag)}${state.hideDropped ? "&hide_dropped=true" : ""}`);
        if (id !== state.request) return;
        state.shows = Array.isArray(data.shows) ? data.shows : [];
        state.total = Number(data.total || 0);
        loadProgress(state.shows.map((s) => s.art?.tmdb).filter(Boolean));
      } else {
        const timeline = timelineOn();
        const params = new URLSearchParams({
          feature: listFeature(),
          duplicates: String(state.view === "history" && state.duplicates),
          provider_instance: state.instance,
          q: state.q,
          status: state.noteStatus,
          tag: state.noteTag,
          media: state.view === "cleanup" ? "" : state.media,
          sort: state.view === "cleanup" ? "title" : state.sort,
          offset: String(timeline ? (append ? state.items.length : 0) : state.offset),
          limit: String(timeline && !mobile() ? TIMELINE_PAGE : pageSize()),
        });
        if (state.view === "cleanup" && state.healthFilter) params.set("health", state.healthFilter);
        if (timeline && state.month) params.set("month", state.month);
        if (state.view === "collection" && state.layouts.collection === "shelf") params.set("shelf", "true");
        const limit = Number(params.get("limit"));
        const range = timeline && (append || (quiet && state.items.length > limit));
        const count = Math.max(limit, Math.ceil(state.items.length / limit) * limit) + (append ? limit : 0);
        const data = range ? await loadTimelineRange(params, count, id) : await getViewJSON(`/api/tracker/items?${params}`);
        if (id !== state.request) return;
        const rows = Array.isArray(data.items) ? data.items : [];
        state.items = append && !range ? state.items.concat(rows) : rows;
        state.total = Number(data.total || 0);
      }
      loaded = true;
    } catch (err) {
      if (id !== state.request || err.name === "AbortError") return;
      if (restored) toast.show(errorText(err), {tone:"bad"});
      else { state.items = []; state.shows = []; state.noteRows = []; state.error = errorText(err); }
    } finally {
      if (id === state.request) {
        if (!append && loaded) rememberList(cacheKey);
        state.loading = false;
        void loadListHero();
        const anchor = loaded && timelineOn() && (quiet || append) ? [...state.root.querySelectorAll('[data-trk-slot="main"] .trk-play')].find(row => row.getBoundingClientRect().bottom > 0) : null;
        const attribute = anchor?.hasAttribute("data-trk-ref-key") ? "data-trk-ref-key" : "data-trk-ref-show";
        const anchorKey = anchor?.getAttribute(attribute), anchorTop = anchor?.getBoundingClientRect().top;
        renderSlots(true);
        renderMonths();
        const day = anchor?.dataset.trkDay;
        const replacement = anchorKey ? state.root.querySelector(`[data-trk-slot="main"] [${attribute}="${CSS.escape(anchorKey)}"]${day ? `[data-trk-day="${CSS.escape(day)}"]` : ""}`) : null;
        if (replacement) window.scrollBy({top:replacement.getBoundingClientRect().top - anchorTop, behavior:"instant"});
        if (timelineRefreshPending && timelineOn()) void loadMain(false, true);
      }
    }
  }

  let progressQueue = [];
  let progressActive = 0;

  function loadProgress(ids) {
    const wanted = ids.filter((id) => !(id in state.progress));
    progressQueue = wanted.concat(progressQueue.filter((id) => !wanted.includes(id)));
    while (progressActive < 3 && progressQueue.length) pumpProgress();
  }

  async function pumpProgress() {
    const tmdb = progressQueue.shift();
    if (!tmdb) return;
    progressActive += 1;
    try {
      state.progress[tmdb] = await getJSON(`/api/tracker/show-progress?${inst()}&tmdb=${encodeURIComponent(tmdb)}`);
    } catch {
      state.progress[tmdb] = { available: false };
    } finally {
      progressActive -= 1;
      if (state.view === "shows") setSlot("main", renderMain());
      if (state.title?.title?.art?.tmdb === tmdb) {
        setSlot("title", renderTitle());
        if (sheet.mode === "title") renderSheet();
      }
      if (progressQueue.length) pumpProgress();
    }
  }

  async function loadTitle() {
    const ref = state.ref;
    if (!ref) return;
    const id = ++state.titleRequest;
    const params = ref.note
      ? `note=${encodeURIComponent(ref.note)}`
      : ref.show ? `show=${encodeURIComponent(ref.show)}` : `feature=${encodeURIComponent(ref.feature)}&key=${encodeURIComponent(ref.key)}`;
    try {
      const data = await getJSON(`/api/tracker/title?${inst()}&${params}`);
      if (id !== state.titleRequest) return;
      state.title = data;
      void loadHeroMetadata(data, id);
      cardModal()?.updateTracker?.(data);
      if (data.show && data.title?.art?.tmdb) loadProgress([data.title.art.tmdb]);
    } catch (err) {
      if (id !== state.titleRequest) return;
      state.title = null;
      state.ref = null;
      if (err?.code !== "not_found") toast.show(errorText(err), { tone: "bad" });
    }
    setSlot("title", renderTitle());
    if (sheet.mode === "title") {
      if (state.ref) renderSheet();
      else closeSheet(false);
    }
  }

  async function loadHeroMetadata(data, id) {
    const item = cardItem(data.title, data.show);
    if (!item) return;
    try {
      const meta = await window.CW?.Meta?.get?.(item, "detail", { seriesInfo: true });
      if (id !== state.titleRequest || state.title !== data || !meta) return;
      data.heroMeta = meta;
      updateHeroSelection();
    } catch {}
  }

  function clearSelectedTitle() {
    state.titleRequest += 1;
    state.ref = null;
    state.title = null;
    state.statusMenu = false;
    state.seasonOpen.clear();
    state.root?.querySelectorAll('.trk-play.is-selected').forEach(row => {
      row.classList.remove('is-selected');
      row.setAttribute('aria-selected', 'false');
    });
    saveView();
    setSlot('title', renderTitle());
  }

  function selectRef(ref, { openMedia = mobile(), action = "", watchItem = null } = {}) {
    state.ref = ref;
    saveView();
    state.statusMenu = false;
    state.title = null;
    state.seasonOpen.clear();
    state.seasonsExpanded = false;
    state.playsExpanded = false;
    state.root.querySelectorAll(".trk-play").forEach((tr) => {
      const on = ref.note ? tr.dataset.trkRefNote === ref.note : ref.show ? tr.dataset.trkRefShow === ref.show : tr.dataset.trkRefKey === ref.key;
      tr.classList.toggle("is-selected", on);
      tr.setAttribute("aria-selected", String(on));
    });
    setSlot("title", renderTitle());
    if (narrow() && !openMedia && !action) openSheet('title');
    loadTitle().then(() => {
      if (action && state.ref === ref && state.title) {
        if (action === "log") logFromTitle(watchItem);
        else detailAction(action);
      }
      else if (openMedia && state.ref === ref) {
        if (canCard(state.title ? cardItem(state.title.title, state.title.show) : null)) openCard();
        else if (narrow()) openSheet('title');
      }
    });
  }

  let moveTimer = 0;

  function waitForMove() {
    clearTimeout(moveTimer);
    moveTimer = setTimeout(async () => {
      if (!state.root || state.root.classList.contains("hidden") || document.hidden) return;
      try {
        const data = await getJSON("/api/tracker/check/current");
        state.migration = data.migration || null;
      } catch {}
      await refresh();
    }, 3000);
  }

  async function refresh() {
    window.CW?.ViewCache?.discard();
    stopOverview();
    const position = {view:state.view,instance:state.instance,top:state.scrollTop || 0};
    pickRequest += 1;
    state.picking = false;
    clearTimeout(moveTimer);
    try {
      await loadSummary();
    } catch (err) {
      if (err?.code === "stale") return;
      state.error = errorText(err);
      state.moving = err?.code === "tracker_migrating";
      state.items = [];
      render();
      if (state.moving) waitForMove();
      return;
    }
    state.moving = false;
    state.migration = null;
    saveView();
    render();
    pollCheck();
    pollImport();
    state.month = "";
    loadMonths();
    await loadMain();
    if (state.ref) loadTitle();
    if (position.top && position.view === state.view && position.instance === state.instance) requestAnimationFrame(() => window.scrollTo({top:position.top,behavior:"instant"}));
  }

  function reload(resetPage = true) {
    pickRequest += 1;
    state.picking = false;
    state.historyTitleKeys = {};
    if (resetPage) { state.offset = 0; state.scrollTop = 0; }
    state.month = "";
    loadMonths();
    state.picked.clear();
    saveView();
    loadMain();
  }

  let pollTimer = 0;

  function pollCheck() {
    clearTimeout(pollTimer);
    if (state.check?.state !== "running") return;
    pollTimer = setTimeout(async () => {
      if (!state.root || state.root.classList.contains("hidden") || document.hidden) return;
      try {
        const data = await getJSON(`/api/tracker/check?${inst()}`);
        state.check = data.job || null;
      } catch {}
      setSlot("health", state.view === "cleanup" ? renderHealth() : "");
      if (state.check?.state === "running") {
        pollCheck();
        return;
      }
      window.dispatchEvent(new CustomEvent("cw:tracker-check-changed"));
      await refresh();
    }, 2000);
  }

  async function startCheck() {
    try {
      const data = await getJSON("/api/tracker/check", { provider_instance: state.instance });
      state.check = data.job || null;
      renderSlots(true);
      window.dispatchEvent(new CustomEvent("cw:tracker-check-changed"));
      pollCheck();
    } catch (err) {
      toast.show(writeError(err), { tone: "bad" });
    }
  }

  let importTimer = 0;

  function pollImport() {
    clearTimeout(importTimer);
    if (state.imp?.state !== "running") return;
    importTimer = setTimeout(async () => {
      if (!state.root || state.root.classList.contains("hidden") || document.hidden) return;
      try {
        const data = await getJSON(`/api/tracker/import?${inst()}`);
        state.imp = data.job || null;
      } catch {}
      setSlot("import", renderImportBanner());
      if (state.imp?.state === "running") {
        pollImport();
        return;
      }
      window.dispatchEvent(new CustomEvent("cw:tracker-check-changed"));
      await refresh();
    }, 2000);
  }

  async function cancelImport() {
    try {
      await getJSON("/api/tracker/import/cancel", { provider_instance: state.instance });
      if (state.imp) state.imp.cancel_requested = true;
      setSlot("import", renderImportBanner());
    } catch (err) {
      toast.show(writeError(err), { tone: "bad" });
    }
    pollImport();
  }

  function dismissImport() {
    try {
      if (state.imp?.id) localStorage.setItem(IMPORT_SEEN_KEY, state.imp.id);
    } catch {}
    setSlot("import", "");
  }

  async function cancelCheck() {
    try {
      await getJSON("/api/tracker/check/cancel", { provider_instance: state.instance });
    } catch {}
    pollCheck();
  }

  const toast = (() => {
    let el = null;
    let timer = 0;

    function hide() {
      window.clearTimeout(timer);
      if (el) el.hidden = true;
    }

    function show(text, { tone = "ok", undo = null } = {}) {
      if (!el) {
        el = document.createElement("div");
        el.className = "cw-ia-toast cw-page-item-actions";
        el.setAttribute("role", "status");
        el.setAttribute("aria-live", "polite");
        document.body.appendChild(el);
        el.addEventListener("click", (event) => {
          const button = event.target.closest("[data-trk-toast]");
          if (!button) return;
          const run = button.dataset.trkToast === "undo" ? el.__undo : null;
          hide();
          if (run) run();
        });
      }
      window.clearTimeout(timer);
      el.__undo = undo;
      el.dataset.tone = tone;
      el.innerHTML = `<span class="cw-ia-toast-icon">${icon(tone === "bad" ? "error" : "check_circle")}</span><span class="cw-ia-toast-text">${esc(text)}</span>${undo ? `<button type="button" data-trk-toast="undo">${icon("undo")}${TH("tracker.ui.undo")}</button>` : ""}<button type="button" class="cw-ia-toast-close" data-trk-toast="close" aria-label="${TH("tracker.import.dismiss")}">${icon("close")}</button><span class="cw-ia-toast-bar is-timer${tone === "bad" ? " is-long" : ""}"></span>`;
      el.hidden = false;
      timer = window.setTimeout(hide, tone === "bad" ? 10000 : 8000);
    }

    return { show, hide };
  })();

  function targetNames(list) {
    return (Array.isArray(list) ? list : []).map((t) => `${providerName(t.provider)}${t.instance && t.instance !== "default" ? ` (${t.instance})` : ""}`);
  }

  function effectHtml(feature, count) {
    const fx = state.effects?.[feature] || {};
    const spreads = targetNames(fx.spreads);
    const returns = targetNames(fx.returns);
    const it = count === 1 ? "it" : "them";
    const lines = [];
    if (spreads.length) lines.push(`<p class="trk-effect is-danger">${icon("sync_problem")}<span>${TH("tracker.ui.nextSyncRemoves", {what: it})} <b>${esc(spreads.join(", "))}</b>.</span></p>`);
    if (returns.length) lines.push(`<p class="trk-effect">${icon("sync")}<span><b>${esc(returns.join(", "))}</b> may add ${it} back on the next sync while ${count === 1 ? "it still has it" : "they still have them"}.</span></p>`);
    if (!lines.length) lines.push(`<p class="trk-effect">${icon("info")}<span>Only removed from this tracker. No sync pair removes ${it} anywhere else.</span></p>`);
    return lines.join("");
  }

  function addEffectHtml(feature) {
    const adds = targetNames(state.effects?.[feature]?.adds);
    return adds.length ? `<p class="trk-effect">${icon("sync")}<span>${TH("tracker.effects.nextSyncAdds")} <b>${esc(adds.join(", "))}</b>.</span></p>` : "";
  }

  let escapeWired = false;

  function ensureSheet() {
    if (sheet.el && state.root?.contains(sheet.el)) return sheet.el;
    if (!escapeWired) {
      escapeWired = true;
      document.addEventListener("keydown", (event) => {
        if (event.key !== "Escape" || !sheet.el || sheet.el.hidden || sheet.el.contains(event.target)) return;
        event.preventDefault();
        closeSheet();
      });
    }
    const el = document.createElement("div");
    el.className = "cw-ia-sheet-wrap cw-page-item-actions trk-sheet-wrap";
    el.hidden = true;
    el.innerHTML = `<div class="cw-ia-sheet-backdrop" data-trk-sheet="close"></div><section class="cw-ia-sheet trk-sheet" role="dialog" aria-modal="true" aria-labelledby="trk-sheet-title" tabindex="-1"></section>`;
    state.root.appendChild(el);
    el.addEventListener("click", onSheetClick);
    el.addEventListener("submit", onSheetSubmit);
    el.addEventListener("keydown", onSheetKey);
    el.addEventListener("input", onSheetInput);
    el.addEventListener("change", onSheetChange);
    sheet.el = el;
    return el;
  }

  function sheetHead(symbolOrItem, title, sub) {
    const item = typeof symbolOrItem === "object" ? symbolOrItem : null;
    const url = item ? artUrl(item, "w185") : "";
    const lead = item
      ? `<div class="cw-ia-sheet-poster">${url ? `<img src="${esc(url)}" alt="">` : `<span class="trk-sheet-icon">${icon(typeMeta(item).icon)}</span>`}</div>`
      : `<span class="trk-sheet-icon">${icon(symbolOrItem)}</span>`;
    return `<header class="cw-ia-sheet-head">${lead}<div class="cw-ia-sheet-title"><h2 id="trk-sheet-title">${esc(title)}</h2>${sub ? `<small>${esc(sub)}</small>` : ""}</div><button type="button" class="cw-ia-icon-btn" data-trk-sheet="close" aria-label="${TH("common.actions.close")}">${icon("close")}</button></header>`;
  }

  function idInputs(group, ids, list) {
    return list.map((name) => {
      const meta = ID_FIELDS.find((f) => f.id === name);
      return `<label class="trk-field"><span>${esc(meta?.label || name)}</span><input type="text" class="cw-input" data-trk-id="${group}:${name}" value="${esc(ids?.[name] ?? "")}" autocomplete="off" spellcheck="false" inputmode="${name === "imdb" ? "text" : "numeric"}"></label>`;
    }).join("");
  }

  function idList(ids) {
    const present = ID_FIELDS.map((f) => f.id).filter((id) => ids && ids[id] != null && ids[id] !== "");
    return Array.from(new Set([...BASE_IDS, ...present]));
  }

  const trackerAssetVersion = (() => { try { return new URL(document.currentScript?.src || location.href).searchParams.get("v") || ""; } catch { return ""; } })();
  let shelfEditorLoad = null;
  const MEDIA_RES = { uhd_4k: "4K", hd_1080p: "1080p", hd_1080i: "1080i", hd_720p: "720p", sd_576p: "576p", sd_576i: "576i", sd_480p: "480p", sd_480i: "480i" };
  const MEDIA_HDR = { dolby_vision: "Dolby Vision", hdr10_plus: "HDR10+", hdr10: "HDR10", hlg: "HLG" };
  const MEDIA_AUDIO = { dolby_atmos: "Dolby Atmos", dolby_digital_plus_atmos: "Dolby Digital Plus Atmos", dolby_truehd: "Dolby TrueHD", dolby_digital_plus: "Dolby Digital Plus", dolby_digital: "Dolby Digital", dolby_prologic: "Dolby Pro Logic", dts_x: "DTS:X", dts_ma: "DTS-HD MA", dts_hr: "DTS-HD HRA", dts: "DTS", auro_3d: "Auro-3D", lpcm: "LPCM", flac: "FLAC", aac: "AAC", mp3: "MP3", mp2: "MP2", ogg: "Ogg", ogg_opus: "Opus", wma: "WMA" };
  const MEDIA_CHANNELS = ["1.0", "2.0", "2.1", "3.0", "3.1", "4.0", "4.1", "5.0", "5.1", "5.1.2", "5.1.4", "6.1", "7.1", "7.1.2", "7.1.4", "9.1", "10.1"];
  const MEDIA_FORMATS = { "4k_bluray": "4K UHD", bluray: "Blu-ray", hd_dvd: "HD DVD", dvd: "DVD", digital: T("profile.shelf.formatDigital"), vcd: "VCD", vhs: "VHS", betamax: "Betamax", laserdisc: "LaserDisc" };

  function mediaEditor(item) {
    const media = item.media_info || {};
    const options = (map, value) => `<option value="">${TH("tracker.media.notSet")}</option>${value && !map[value] ? `<option value="${esc(value)}" selected>${esc(value)}</option>` : ""}${Object.entries(map).map(([key, label]) => `<option value="${esc(key)}"${key === value ? " selected" : ""}>${esc(label)}</option>`).join("")}`;
    const select = (name, label, map, value) => `<label class="trk-field"><span>${esc(label)}</span><select class="cw-select" data-trk-mi="${name}">${options(map, value || "")}</select></label>`;
    const copyFormat = (Array.isArray(item.copies) ? item.copies : []).map((row) => row?.format).find((fmt) => MEDIA_FORMATS[fmt]) || "";
    const suggest = copyFormat && copyFormat !== item.format ? `<button type="button" class="cw-btn trk-media-shelf" data-trk-media-shelf="${esc(copyFormat)}">${icon("album")}<span>Use copy format: ${esc(MEDIA_FORMATS[copyFormat])}</span></button>` : "";
    return `<details class="trk-identity trk-media"${item.media_manual ? " open" : ""}>
          <summary>${icon("album")}<span>${TH("tracker.media.info")}</span><small>${item.media_manual ? TH("tracker.media.setByYou") : TH("tracker.media.fromSync")}</small></summary>
          <div class="trk-identity-body">
            <div class="trk-fields">
              ${select("format", T("tracker.media.format"), MEDIA_FORMATS, item.format)}
              ${select("resolution", T("tracker.media.resolution"), MEDIA_RES, media.resolution)}
              ${select("hdr", "HDR", MEDIA_HDR, media.hdr)}
              ${select("audio", T("tracker.media.audio"), MEDIA_AUDIO, media.audio)}
              ${select("audio_channels", T("tracker.media.channels"), Object.fromEntries(MEDIA_CHANNELS.map((value) => [value, value])), media.audio_channels)}
              <label class="trk-field trk-media-check"><input type="checkbox" class="cw-checkbox" data-trk-mi="is_3d"${media.is_3d ? " checked" : ""}><span>3D</span></label>
            </div>
            ${suggest}
            ${item.media_manual ? `<label class="trk-field trk-media-check"><input type="checkbox" class="cw-checkbox" data-trk-mi="reset"><span>${TH("tracker.media.useSynced")}</span></label>` : ""}
          </div>
        </details>`;
  }

  function mediaChanges(form, item) {
    if (!form.querySelector('[data-trk-mi="format"]')) return undefined;
    if (form.querySelector('[data-trk-mi="reset"]')?.checked) return null;
    const get = (name) => form.querySelector(`[data-trk-mi="${name}"]`)?.value || "";
    const next = { format: get("format"), resolution: get("resolution"), hdr: get("hdr"), audio: get("audio"), audio_channels: get("audio_channels"), is_3d: !!form.querySelector('[data-trk-mi="is_3d"]')?.checked };
    const before = item.media_info || {};
    const same = next.format === (item.format || "") && ["resolution", "hdr", "audio", "audio_channels"].every((key) => next[key] === (before[key] || "")) && next.is_3d === !!before.is_3d;
    return same ? undefined : next;
  }

  function renderEdit() {
    const item = sheet.item;
    const f = featureOf(sheet.feature);
    const child = isChild(item);
    const dateRow = item.date_field
      ? `<label class="trk-field"><span>${esc(f.date)}</span><input type="datetime-local" step="1" class="cw-input cw-timestamp-input" data-trk-edit="date" value="${esc(localInput(item.date))}" max="${esc(localInput(new Date().toISOString()))}"></label>`
      : "";
    const ratingRow = sheet.feature === "ratings"
      ? `<div class="trk-field trk-field-wide trk-edit-rating"><input type="hidden" data-trk-edit="rating" value="${esc(item.rating ?? 0.5)}">${ratingControl(Number(item.rating ?? 0.5))}</div>`
      : "";
    const progressRow = sheet.feature === "progress"
      ? `<label class="trk-field"><span>${TH("tracker.edit.progressPercent")}</span><input type="number" class="cw-input" data-trk-edit="progress_percent" min="0" max="100" step="1" value="${esc(item.progress_percent != null ? Math.round(item.progress_percent) : "")}"></label>`
      : "";
    const yearRow = `<label class="trk-field"><span>${TH("tracker.list.colYear")}</span><input type="number" class="cw-input" data-trk-edit="year" min="1870" max="2100" value="${esc((child ? item.series_year : item.year) ?? "")}"></label>`;
    return `
      ${sheetHead(item, cardTitle(item), [cardSub(item), f.label].filter(Boolean).join(" · "))}
      <form class="trk-form" data-trk-form="edit" novalidate>
        <div class="trk-fields">
          ${dateRow}${progressRow}
          <label class="trk-field"><span>${child ? T("tracker.edit.showStatus") : T("tracker.filters.status")}</span><select class="cw-select" data-trk-edit="status">${[{id:"",label:T("tracker.filters.noStatus")}, ...STATUSES].map(s => `<option value="${s.id}"${(item.status || "") === s.id ? " selected" : ""}>${esc(s.label)}</option>`).join("")}</select></label>
          ${sheet.feature === "ratings" ? yearRow + ratingRow : ""}
          <label class="trk-field trk-field-wide"><span>${child ? T("tracker.edit.showTitle") : T("tracker.filters.groupTitle")}</span><input type="text" class="cw-input" data-trk-edit="title" value="${esc((child ? item.series_title : item.title) ?? "")}" maxlength="300"></label>
          ${sheet.feature === "ratings" ? "" : yearRow}
        </div>
        <details class="trk-identity">
          <summary>${icon("fingerprint")}<span>${TH("tracker.fieldNames.ids")}</span><small>${TH("tracker.edit.idsHint")}</small></summary>
          <div class="trk-identity-body">
            ${child ? `<div class="trk-group-label">${TH("tracker.edit.showIds")}</div><div class="trk-fields">${idInputs("show_ids", item.show_ids, idList(item.show_ids))}</div><div class="trk-group-label">${TH(item.type === "episode" ? "tracker.ui.episodeIds" : "tracker.ui.seasonIds")}</div>` : ""}
            <div class="trk-fields">${idInputs("ids", item.ids, idList(item.ids))}</div>
          </div>
        </details>
        ${sheet.feature === "collection" ? mediaEditor(item) : ""}
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        <footer class="trk-sheet-foot">
          <button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button>
          <button type="submit" class="cw-btn primary"${sheet.busy ? " disabled" : ""}>${icon("save")}<span>${TH("common.actions.save")}</span></button>
        </footer>
      </form>`;
  }

  function completionSuggestion(data, inSheet = false) {
    if (!data?.show || !data.note_key || !state.progress[data.title?.art?.tmdb]?.completed || ["completed", "dropped"].includes(data.note?.status)) return "";
    return `<button type="button" class="trk-note-suggest" ${inSheet ? 'data-trk-sheet="complete"' : 'data-trk-detail="complete"'}>${symbol("task_alt")}<span>${TH("tracker.edit.applyCompleted")}</span></button>`;
  }

  function renderTitleEdit() {
    const data = state.title;
    if (!data) return `${sheetHead("edit", T("tracker.ui.editTitle"), "")}<div class="trk-form">${emptyState(T("common.status.loading"), "hourglass_empty")}</div>`;
    const head = data.title || {};
    const n = recordCount(data);
    return `
      ${sheetHead(head, T("tracker.edit.titleHead", { title: head.title || T("tracker.check.thisTitle") }), data.show ? T("tracker.edit.showScope") : T("tracker.edit.titleScope"))}
      <form class="trk-form" data-trk-form="title-edit" novalidate>
        ${completionSuggestion(data, true)}
        <div class="trk-fields">
          <label class="trk-field trk-field-wide"><span>${data.show ? T("tracker.edit.showTitle") : T("tracker.filters.groupTitle")}</span><input type="text" class="cw-input" data-trk-te="title" value="${esc(head.title ?? "")}" maxlength="300"></label>
          <label class="trk-field"><span>${TH("tracker.list.colYear")}</span><input type="number" class="cw-input" data-trk-te="year" min="1870" max="2100" value="${esc(head.year ?? "")}"></label>
        </div>
        <details class="trk-identity">
          <summary>${icon("fingerprint")}<span>${data.show ? T("tracker.edit.showIds") : T("tracker.fieldNames.ids")}</span><small>${TH("tracker.edit.orFixMatch")}</small></summary>
          <div class="trk-identity-body"><div class="trk-fields">${idInputs("ids", head.ids, idList(head.ids))}</div></div>
        </details>
        <p class="trk-effect">${icon("info")}<span>Title, year and ID edits apply to ${esc(plural(n, "record", "records"))}. Watch status is separate.</span></p>
        ${sheetError()}
        <footer class="trk-sheet-foot">
          <button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button>
          <button type="submit" class="cw-btn primary" disabled>${icon("save")}<span>${TH("tracker.footer.saveChanges")}</span></button>
        </footer>
      </form>`;
  }

  function titleEditChanges() {
    const data = state.title;
    const form = sheet.el.querySelector('[data-trk-form="title-edit"]');
    if (!data || !form) return {};
    const head = data.title || {};
    const changes = {};
    const title = (form.querySelector('[data-trk-te="title"]')?.value ?? "").trim();
    if (title !== String(head.title ?? "")) changes.title = title;
    const year = (form.querySelector('[data-trk-te="year"]')?.value ?? "").trim();
    if (year !== String(head.year ?? "")) changes.year = year === "" ? null : Number(year);
    const ids = {};
    form.querySelectorAll('[data-trk-id^="ids:"]').forEach((input) => {
      const name = input.dataset.trkId.split(":")[1];
      const now = input.value.trim();
      if (now !== String(head.ids?.[name] ?? "")) ids[name] = now;
    });
    if (Object.keys(ids).length) changes.ids = ids;
    return changes;
  }

  async function saveTitleEdit() {
    if (sheet.busy) return;
    const data = state.title;
    const head = data?.title || {};
    const changes = titleEditChanges();
    const title = changes.title || head.title;
    if (!Object.keys(changes).length) {
      closeSheet();
      return;
    }
    const anchor = titleAnchor(data);
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const target = data.show ? { show: data.show_key } : { feature: anchor?.feature || "history", key: anchor?.key };
      const res = await getJSON("/api/tracker/edit-title", { provider_instance: state.instance, changes, ...target });
      state.ref = res.show_key ? { show: res.show_key } : { feature: res.feature, key: res.key };
      await afterWrite(res, T("tracker.edit.updatedRecords", { records: plural(res.count, "record", "records"), title: title || head.title }));
    } catch (err) {
      sheet.busy = false;
      sheet.error = err?.code === "changed_since" ? T("tracker.edit.changedWhileEditing") : writeError(err);
      renderSheet();
    }
  }

  function renderDelete() {
    const n = sheet.keys.length;
    const f = featureOf(sheet.feature);
    return `
      ${sheetHead("delete", n === 1 ? T(sheet.feature === "history" ? "tracker.delete.thisPlay" : "tracker.delete.thisRecord") : T("tracker.delete.records", { records: plural(n, "record", "records") }), T("tracker.delete.from", { feature: f.label }))}
      <div class="trk-form">
        <div class="trk-confirm">${effectHtml(sheet.feature, n)}</div>
        <p class="trk-note">${TH("tracker.delete.undoHint")}</p>
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        <footer class="trk-sheet-foot">
          <button type="button" class="cw-btn" data-trk-sheet="close">${TH("tracker.delete.keep")}</button>
          <button type="button" class="cw-btn danger" data-trk-sheet="delete"${sheet.busy ? " disabled" : ""}>${icon("delete")}<span>${TH("tracker.ui.delete")}</span></button>
        </footer>
      </div>`;
  }

  function readSavedViews() {
    try {
      const rows = JSON.parse(localStorage.getItem("cw.tracker.saved-views.v1") || "[]");
      return Array.isArray(rows) ? rows.filter(row => row && typeof row.name === "string" && row.settings && typeof row.settings === "object").slice(0, 20) : [];
    } catch { return []; }
  }

  function renderSavedViews() {
    return `${sheetHead("bookmarks", T("tracker.secondary.savedViews"), T("tracker.views.savedHint"))}
      <form class="trk-form" data-trk-form="views">
        ${sheet.views.map((view,index) => `<div class="trk-play-row"><button type="button" class="cw-btn" data-trk-saved-view="${index}">${esc(view.name)}</button><button type="button" class="trk-icon" data-trk-saved-view="${index}" data-remove="true" aria-label="Remove saved view ${esc(view.name)}">${icon("delete")}</button></div>`).join("") || '<p class="trk-note">' + TH("tracker.views.noneSaved") + '</p>'}
        <label class="trk-field trk-field-wide"><span>${TH("tracker.views.saveAs")}</span><input class="cw-input" data-trk-view-name maxlength="60" placeholder="${TH("tracker.views.example")}" required></label>
        ${sheet.error ? `<p class="trk-error" role="alert">${esc(sheet.error)}</p>` : ""}
        <footer class="trk-sheet-foot"><button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.close")}</button><button type="submit" class="cw-btn primary"${sheet.views.length >= 20 ? " disabled" : ""}>${TH("tracker.views.save")}</button></footer>
      </form>`;
  }

  function saveNamedView() {
    const name = sheet.el.querySelector("[data-trk-view-name]").value.trim().slice(0, 60);
    if (!name || sheet.views.length >= 20) return;
    if (sheet.views.some(view => view.name.toLowerCase() === name.toLowerCase())) {
      sheet.error = T("tracker.views.duplicate");
      renderSheet();
      return;
    }
    try {
      saveView();
      const settings = JSON.parse(localStorage.getItem(STORE_KEY) || "{}");
      sheet.views.push({name,settings:{...settings,offset:0,ref:null,scrollTop:0}});
      localStorage.setItem("cw.tracker.saved-views.v1", JSON.stringify(sheet.views));
      sheet.error = "";
      renderSheet();
    } catch { sheet.error = T("tracker.views.saveFailed"); renderSheet(); }
  }

  function renderNotesBulk() {
    const count = plural(sheet.keys.length, "title", "titles");
    return `${sheetHead("delete", T("tracker.titlesBulk.removeHead", { count }), T("tracker.views.myTitles"))}
      <form class="trk-form" data-trk-form="notes-bulk">
        <p class="trk-note">${TH("tracker.titlesBulk.hint")}</p>
        <p class="trk-note">${TH("tracker.titlesBulk.undoHint")}</p>
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        <footer class="trk-sheet-foot"><button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button><button type="submit" class="cw-btn danger"${sheet.busy ? " disabled" : ""}>${icon("delete")}<span>${TH("tracker.selection.removeFromTitles")}</span></button></footer>
      </form>`;
  }

  function ratingLabel(value) {
    const labels = [
      T("tracker.rate.l1"), T("tracker.rate.l2"), T("tracker.rate.l3"), T("tracker.rate.l4"), T("tracker.rate.l5"),
      T("tracker.rate.l6"), T("tracker.rate.l7"), T("tracker.rate.l8"), T("tracker.rate.l9"), T("tracker.rate.l10"),
      T("tracker.rate.l11"), T("tracker.rate.l12"), T("tracker.rate.l13"), T("tracker.rate.l14"), T("tracker.rate.l15"),
      T("tracker.rate.l16"), T("tracker.rate.l17"), T("tracker.rate.l18"), T("tracker.rate.l19"), T("tracker.rate.l20")
    ];
    return labels[Number(value) * 2 - 1] || T("tracker.rate.yourRating");
  }

  function wireRatingPreview(dialog) {
    const group = dialog.querySelector(".trk-rates");
    if (!group) return;
    const buttons = Array.from(group.querySelectorAll("[data-trk-rate]"));
    const input = dialog.querySelector('[data-trk-edit="rating"]');
    const current = () => input ? Number(input.value) : state.title?.ratings?.[0]?.rating ?? null;
    const preview = value => {
      buttons.forEach(button => button.classList.toggle("is-lit", Number(button.dataset.trkRate) <= value));
      dialog.querySelector("[data-trk-rate-value]").textContent = value ? `${value}/10` : T("tracker.rate.notRated");
      dialog.querySelector("[data-trk-rate-label]").textContent = ratingLabel(value);
    };
    const sync = () => {
      preview(current());
      buttons.forEach(button => button.setAttribute("aria-pressed", String(Number(button.dataset.trkRate) === current())));
    };
    input?.addEventListener("input", sync);
    sync();
    group.addEventListener("pointerover", event => {
      const button = event.target.closest("[data-trk-rate]");
      if (button && !button.disabled) preview(Number(button.dataset.trkRate));
    });
    group.addEventListener("pointerleave", () => preview(current()));
    group.addEventListener("focusin", event => {
      const button = event.target.closest("[data-trk-rate]");
      if (button) preview(Number(button.dataset.trkRate));
    });
    group.addEventListener("focusout", event => { if (!group.contains(event.relatedTarget)) preview(current()); });
    group.addEventListener("keydown", event => {
      const index = buttons.indexOf(event.target.closest("[data-trk-rate]"));
      if (index < 0) return;
      const next = event.key === "ArrowRight" || event.key === "ArrowUp" ? Math.min(buttons.length - 1, index + 1)
        : event.key === "ArrowLeft" || event.key === "ArrowDown" ? Math.max(0, index - 1)
        : event.key === "Home" ? 0 : event.key === "End" ? buttons.length - 1 : null;
      if (next === null) return;
      event.preventDefault();
      buttons[next].focus();
    });
  }

  async function loadCommunityRating(token) {
    const head = state.title?.title || {};
    let text = T("tracker.rate.communityUnavailable");
    let rating = null;
    try {
      const ids = { ...head.ids };
      if (head.art?.tmdb) ids.tmdb = head.art.tmdb;
      if (Object.keys(ids).length) {
        const data = await getJSON("/api/metadata/resolve", { entity: state.title?.show ? "show" : "movie", ids });
        const meta = data.result || {};
        const votes = Number(meta.vote_count);
        const score = Number(meta.detail?.vote_average ?? Number(meta.score) / 10);
        if (Number.isFinite(score) && score >= 0 && score <= 10 && votes > 0) {
          text = T("tracker.rate.community", { score: score.toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 }), count: votes });
          rating = { score, votes };
        }
        else if (votes === 0) text = T("tracker.rate.noVotes");
      }
    } catch {}
    if (sheet.mode !== "rate" || sheet.token !== token) return;
    sheet.communityRating = text;
    sheet.communityScore = rating;
    const node = sheet.el?.querySelector("[data-trk-community]");
    if (node) node.innerHTML = communityRatingHtml();
  }

  function communityRatingHtml() {
    const rating = sheet.communityScore;
    if (!rating) return `<span class="trk-note">${esc(sheet.communityRating || "Loading TMDB community rating...")}</span>`;
    return `<div class="trk-community-ring" style="--rating-progress:${rating.score * 10}%" aria-hidden="true"><span><strong>${rating.score.toFixed(1)}</strong><small>/ 10</small></span></div><div class="trk-community-copy"><strong>TMDB community</strong><span class="trk-note">${esc(num(rating.votes))} votes</span><span class="trk-note">${TH("tracker.ui.averageRating", {score: rating.score.toFixed(1)})}</span></div>`;
  }

  function ratingControl(current) {
    const pills = Array.from({ length: 20 }, (_, i) => (i + 1) / 2).map((n) => `<button type="button" class="trk-rate${Number.isInteger(n) ? " is-right" : ""}${n <= current ? " is-lit" : ""}" data-trk-rate="${n}" aria-label="Rate ${n} out of 10" aria-pressed="${current === n}"${sheet.busy ? " disabled" : ""}>${symbol("star")}</button>`).join("");
    return `<div class="trk-rate-summary"><strong data-trk-rate-value>${current != null ? `${current}/10` : T("tracker.rate.notRated")}</strong><span class="trk-note" data-trk-rate-label>${esc(ratingLabel(current))}</span></div>
        <div class="trk-rates" role="group" aria-label="${TH("common.labels.rating")}">${pills}</div>`;
  }

  function renderRate() {
    const head = state.title?.title || {};
    const current = state.title?.ratings?.[0]?.rating ?? null;
    return `
      ${sheetHead(head, T("tracker.rate.head", { title: head.title || "" }), current != null ? T("tracker.rate.now", { value: String(current) }) : T("tracker.rate.notRatedYet"))}
      <div class="trk-form">
        ${ratingControl(current)}
        <div class="trk-community" data-trk-community role="status">${communityRatingHtml()}</div>
        ${addEffectHtml("ratings")}
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        <footer class="trk-sheet-foot">
          ${current != null ? `<button type="button" class="cw-btn danger" data-trk-sheet="unrate"${sheet.busy ? " disabled" : ""}>${icon("star_half")}<span>${TH("tracker.rate.remove")}</span></button>` : ""}
          <button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.close")}</button>
        </footer>
      </div>`;
  }

  function logCardItem(log, r) {
    return r?.tmdb ? { type: log.type === "movie" ? "movie" : "show", title: r.title || "", year: r.year || "", ids: { tmdb: String(r.tmdb) } } : null;
  }

  function episodePlays(log, season, episode) {
    return log.playIndex?.get(`${season}:${episode}`) || [];
  }

  function watchSeasonRows(log, season = log.season) {
    return Array.from({length:log.seasons?.find(s => s.season === season)?.episodes || 0}, (_, i) => ({season, episode:i + 1, ...log.metadata.get(season)?.find(ep => Number(ep.episode) === i + 1)}));
  }

  function watchEligible(log, ep) {
    const watched = episodePlays(log, ep.season, Number(ep.episode)).length > 0;
    return log.action === "add" ? !watched : watched;
  }

  function watchSelection(log) {
    if (!log.advanced) return Number(log.episode) > 0 ? [{season:log.season, episode:Number(log.episode)}] : [];
    return Array.from(log.selected, key => { const [season, episode] = key.split(":").map(Number); return {season, episode}; }).filter(ep => watchEligible(log, ep)).sort((a, b) => a.season - b.season || a.episode - b.episode);
  }

  async function loadWatchSeason(log, season = log.season, repaint = true) {
    if (!log.pick?.tmdb || log.metadata.has(season)) return;
    const tmdb = log.pick.tmdb;
    log.seasonRequests ||= new Map();
    const key = `${tmdb}:${season}`;
    if (!log.seasonRequests.has(key)) log.seasonRequests.set(key, getJSON(`/api/metadata/tmdb/season?tmdb=${encodeURIComponent(tmdb)}&season=${season}`));
    try {
      const data = await log.seasonRequests.get(key);
      if (log.pick?.tmdb !== tmdb) return;
      log.metadata.set(season, data.episodes || []);
    } catch (err) {
      if (!repaint) throw new Error(T("tracker.watchEditor.datesFailed"));
    } finally {
      log.seasonRequests.delete(key);
      if (repaint && sheet.mode === "log" && sheet.log === log) renderSheet();
    }
  }

  function watchEffects(log, episodes, action) {
    return episodes.flatMap(ep => {
      const plays = episodePlays(log, ep.season, ep.episode);
      if (["add", "rewatch"].includes(action)) return [{...ep, before:null, after:ep.watched_at}];
      return (log.scope === "all" ? plays : plays.slice(0, 1)).map(play => ({...ep, before:play.date,
        after:action === "remove" ? null : new Date(Date.parse(ep.watched_at) + (log.scope === "all" ? Date.parse(play.date) - Date.parse(plays[0].date) : 0)).toISOString()}));
    });
  }

  const WATCH_ACTIONS = [
    ["add", T("tracker.watchEditor.markEpisodes"), T("tracker.watchEditor.markEpisodesHint"), "check"],
    ["rewatch", T("tracker.actions.logRewatch"), T("tracker.watchEditor.rewatchHint"), "replay"],
    ["dates", T("tracker.watchEditor.changeDates"), T("tracker.watchEditor.changeDatesHint"), "edit_calendar"],
    ["remove", T("tracker.watchEditor.removeWatches"), T("tracker.watchEditor.removeHint"), "delete_outline"],
  ];

  function renderWatchSeasons(log) {
    const selected = watchSelection(log).length;
    const eligible = log.seasons.flatMap(s => watchSeasonRows(log, s.season)).filter(ep => watchEligible(log, ep));
    const scope = ["dates", "remove"].includes(log.action) ? `<label class="trk-field"><span>${TH("tracker.watchEditor.forRewatches")}</span><select class="cw-select" data-trk-log-scope><option value="latest"${log.scope === "latest" ? " selected" : ""}>${TH("tracker.watchEditor.latestOnly")}</option><option value="all"${log.scope === "all" ? " selected" : ""}>${TH("tracker.watchEditor.allWatches")}</option></select>${log.action === "dates" && log.scope === "all" ? `<small>${TH("tracker.watchEditor.datesShift")}</small>` : ""}</label>` : "";
    return `<div class="trk-watch-selection"><button type="button" class="cw-btn" data-trk-log-select="all">${eligible.length && eligible.every(ep => log.selected.has(`${ep.season}:${ep.episode}`)) ? TH("tracker.watchEditor.clearSelection") : TH("tracker.watchEditor.selectEligible")}</button><span>${TH("tracker.watchEditor.selectedCount", { count: selected })} <small>${TH("tracker.watchEditor.max200")}</small></span></div>
      <div class="trk-watch-seasons">${log.seasons.map(s => {
        const rows = watchSeasonRows(log, s.season), available = rows.filter(ep => watchEligible(log, ep));
        const watched = rows.filter(ep => episodePlays(log, s.season, ep.episode).length).length;
        const chosen = available.filter(ep => log.selected.has(`${s.season}:${ep.episode}`)).length;
        const open = log.expanded?.has(s.season);
        return `<section class="trk-watch-season"><div class="trk-watch-season-head"><button type="button" class="trk-watch-season-pick" data-trk-log-select="${s.season}" aria-pressed="${available.length && chosen === available.length ? "true" : "false"}"${available.length ? "" : " disabled"}>${icon(chosen ? chosen === available.length ? "check_box" : "indeterminate_check_box" : "check_box_outline_blank")}<span><b>${esc(s.name)}</b><small>${TH("tracker.watchEditor.seasonWatched", { watched, total: s.episodes })}${chosen ? ` &middot; ${TH("tracker.watchEditor.selectedCount", { count: chosen })}` : ""}</small></span></button><button type="button" class="trk-watch-season-expand" data-trk-log-expand="${s.season}" aria-expanded="${!!open}" aria-label="${TH(open ? "tracker.watchEditor.collapse" : "tracker.watchEditor.expand", { name: s.name })}">${icon(open ? "expand_less" : "expand_more")}</button></div>
        ${open ? `<ul class="trk-watch-list">${rows.map(ep => {
          const plays = episodePlays(log, s.season, ep.episode), key = `${s.season}:${ep.episode}`;
          return `<li><label class="trk-watch-row"><input type="checkbox" class="cw-checkbox" data-trk-watch-pick="${key}"${log.selected.has(key) ? " checked" : ""}${watchEligible(log, ep) ? "" : " disabled"}><span><b>${esc(se(s.season, ep.episode))}${ep.name ? ` &middot; ${esc(ep.name)}` : ""}</b><small>${plays.length ? `Watched &middot; ${esc(whenText(plays[0].date))} &middot; ${plural(plays.length, "play", "plays")}` : TH("tracker.ui.unwatched")}</small></span></label></li>`;
        }).join("")}</ul>` : ""}</section>`;
      }).join("")}</div>${scope}`;
  }

  function renderWatchDates(log) {
    const choices = [["today", T("tracker.watchEditor.now"), T("tracker.watchEditor.nowHint"), "schedule"], ["release", T("tracker.watchEditor.releaseDate"), T("tracker.watchEditor.releaseHint"), "event"], ["custom", T("tracker.watchEditor.customDate"), T("tracker.watchEditor.customHint"), "edit_calendar"], ["range", T("tracker.watchEditor.dateRange"), T("tracker.watchEditor.rangeHint"), "date_range"]];
    const input = (attr, label, value) => `<label class="trk-field"><span>${label}</span><input type="datetime-local" step="1" class="cw-input cw-timestamp-input" ${attr} value="${esc(value)}" max="${esc(localInput(new Date().toISOString()))}"></label>`;
    return `<div class="trk-watch-choices trk-watch-date-choices">${choices.map(([id, label, sub, glyph]) => `<button type="button" class="trk-watch-choice" data-trk-log-date-mode="${id}" aria-pressed="${log.whenMode === id}">${icon(glyph)}<span><b>${label}</b><small>${esc(sub)}</small></span>${icon(log.whenMode === id ? "check" : "")}</button>`).join("")}</div>
      ${log.whenMode === "custom" ? input("data-trk-log-when", T("tracker.watchEditor.whenLabel"), log.when) : log.whenMode === "range" ? `<div class="trk-fields trk-watch-fields">${input("data-trk-log-when", T("tracker.log.from"), log.when)}${input("data-trk-log-end", T("tracker.log.to"), log.end)}</div><p class="trk-note">${TH("tracker.watchEditor.spreadNote")}</p>` : ""}
      ${log.watchlistKey && ["add", "rewatch"].includes(log.action) ? `<label class="trk-check"><input type="checkbox" class="cw-checkbox" data-trk-log-unlist${log.unlist ? " checked" : ""}><span>${TH("tracker.detail.removeWatchlist")}</span></label>` : ""}`;
  }

  function renderWatchReview(log) {
    const {episodes, action, effects} = log.review;
    const before = new Set(log.plays.map(p => `${p.season}:${p.episode}`)).size;
    const removed = action === "remove" ? episodes.filter(ep => log.scope === "all" || episodePlays(log, ep.season, ep.episode).length === 1).length : 0;
    const after = before + (action === "add" ? episodes.length : 0) - removed;
    return `<div class="trk-watch-review"><p>${TH("tracker.watchEditor.reviewSummary", { episodes: plural(episodes.length, "episode", "episodes"), watches: plural(effects.length, "watch", "watches") })}${["dates", "remove"].includes(action) ? ` &middot; ${TH(log.scope === "all" ? "tracker.watchEditor.allWatches" : "tracker.watchEditor.latestOnly")}` : ""}</p>
      <dl class="trk-watch-totals" aria-label="${TH("tracker.watchEditor.watchedEpisodes")}"><div><dt>${TH("tracker.watchEditor.watchedNow")}</dt><dd>${before}</dd></div><div><dt>${TH("tracker.watchEditor.afterChange")}</dt><dd>${after}</dd></div><div><dt>${TH("tracker.watchEditor.difference")}</dt><dd>${after > before ? "+" : ""}${after - before}</dd></div></dl>
      <details${effects.length <= 6 ? " open" : ""}><summary>${TH("tracker.watchEditor.affected")}<span>${effects.length}</span></summary><ul class="trk-watch-list">${effects.map(ep => `<li class="trk-watch-row"><b>${esc(se(ep.season, ep.episode))}</b><span>${ep.before ? `${esc(whenText(ep.before))} &rarr; ` : ""}${ep.after ? esc(whenText(ep.after)) : TH("tracker.ui.remove")}</span></li>`).join("")}</ul></details></div>`;
  }

  function renderGuidedWatch(log) {
    const step = log.review ? "review" : log.step || "action";
    const steps = [["action", T("tracker.watchEditor.stepAction")], ["episodes", T("tracker.watchEditor.stepEpisodes")], ...(log.action === "remove" ? [] : [["dates", T("tracker.watchEditor.stepDates")]]), ["review", T("tracker.watchEditor.stepReview")]];
    const current = steps.findIndex(([id]) => id === step);
    const watched = log.seasons?.flatMap(s => watchSeasonRows(log, s.season)).filter(ep => episodePlays(log, ep.season, ep.episode).length).length || 0;
    const total = log.seasons?.reduce((sum, s) => sum + s.episodes, 0) || 0;
    const heading = {action:T("tracker.watchEditor.headAction"), episodes:T("tracker.watchEditor.headEpisodes"), dates:T("tracker.watchEditor.headDates"), review:T("tracker.watchEditor.reviewChanges")}[step];
    const actionName = WATCH_ACTIONS.find(([id]) => id === log.action)?.[1] || T("tracker.watchEditor.title");
    const body = step === "action" ? `<div class="trk-watch-choices">${WATCH_ACTIONS.map(([id, label, sub, glyph]) => {
      const count = id === "add" ? total - watched : watched;
      return `<button type="button" class="trk-watch-choice" data-trk-log-operation="${id}" aria-pressed="${log.action === id}"${count ? "" : " disabled"}>${icon(glyph)}<span><b>${label}</b><small>${sub}</small></span><span class="trk-watch-choice-count">${count}<small>${count === 1 ? "episode" : "episodes"}</small></span>${icon(log.action === id ? "check" : "")}</button>`;
    }).join("")}</div>` : step === "episodes" ? renderWatchSeasons(log) : step === "dates" ? renderWatchDates(log) : renderWatchReview(log);
    const blocked = sheet.busy || !log.seasons?.length || (step === "action" && !log.action) || (step === "episodes" && (!watchSelection(log).length || watchSelection(log).length > 200));
    return `<div class="trk-watch-editor">${sheetHead({...log.pick, art:{kind:"tv", tmdb:log.pick.tmdb}}, T("tracker.watchEditor.title"), log.pick.title)}
      <ol class="trk-watch-steps" aria-label="${TH("tracker.watchEditor.steps")}">${steps.map(([id, label], index) => `<li${index === current ? ' aria-current="step"' : ""} class="${index < current ? "is-done" : ""}">${label}</li>`).join("")}</ol>
      <form class="trk-form" data-trk-form="log" novalidate><div class="trk-watch-heading">${step !== "action" ? `<small>${esc(actionName)}</small>` : ""}<h3>${heading}</h3></div>${body}
      ${sheet.error ? `<p class="trk-error" role="alert">${esc(sheet.error)}</p><button type="button" class="cw-btn" data-trk-log-reload>${TH("tracker.watchEditor.reload")}</button>` : ""}
      <footer class="trk-sheet-foot trk-watch-foot"><button type="button" class="cw-btn" ${step === "action" ? "data-trk-log-advanced" : "data-trk-log-back"}>${step === "action" ? T("tracker.watchEditor.quickWatch") : T("common.actions.back")}</button><button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button><button type="submit" class="cw-btn ${step === "review" && log.action === "remove" ? "danger" : "primary"}"${blocked ? " disabled" : ""}>${sheet.busy ? T("common.status.saving") : step === "review" ? log.action === "remove" ? T("tracker.watchEditor.removeWatches") : T("tracker.footer.saveChanges") : step === "dates" || step === "episodes" && log.action === "remove" ? T("tracker.watchEditor.reviewChanges") : T("common.paging.nextShort")}</button></footer></form></div>`;
  }

  function renderWatchEditor(log) {
    if (!log.seasons) return `<p class="trk-note" role="status">${TH("tracker.watchEditor.loading")}</p>`;
    const seasonOptions = log.seasons.map(s => {
      const watched = new Set(log.plays.filter(p => Number(p.season) === s.season).map(p => p.episode)).size;
      return `<option value="${s.season}"${s.season === log.season ? " selected" : ""}>${esc(s.name)} · ${watched}/${s.episodes} watched</option>`;
    }).join("");
    const season = `<label class="trk-field"><span>${TH("common.media.season")}</span><select class="cw-select" data-trk-log-season>${seasonOptions}</select></label>`;
    const toggle = `<button type="button" class="cw-btn" data-trk-log-advanced aria-expanded="${log.advanced}">${icon(log.advanced ? "expand_less" : "tune")}<span>${log.advanced ? T("tracker.watchEditor.quickWatch") : T("tracker.ui.manageEpisodes")}</span></button>`;
    const rows = watchSeasonRows(log);
    if (!log.advanced) {
      const plays = episodePlays(log, log.season, Number(log.episode));
      return `<div class="trk-fields trk-watch-fields">${season}<label class="trk-field"><span>${TH("common.media.episode")}</span><select class="cw-select" data-trk-log-episode>${rows.map(ep => `<option value="${ep.episode}"${Number(log.episode) === Number(ep.episode) ? " selected" : ""}>${esc(se(log.season, ep.episode))}${ep.name ? ` · ${esc(ep.name)}` : ""}${episodePlays(log, log.season, Number(ep.episode)).length ? T("tracker.watchEditor.watchedSuffix") : ""}</option>`).join("")}</select></label></div><div class="trk-watch-tools">${toggle}<span class="trk-note">${plays.length ? TH("tracker.ui.watchedRewatch", {when: whenText(plays[0].date)}) : TH("tracker.ui.notWatched")}</span></div>`;
    }
  }

  async function saveWatchEditor(log) {
    if (log.advanced && !log.review) {
      if (log.step === "action") {
        if (!log.action) return;
        log.step = "episodes";
        renderSheet(true);
        return;
      }
      if (log.step === "episodes" && log.action !== "remove") {
        if (!watchSelection(log).length || watchSelection(log).length > 200) { sheet.error = T("tracker.watchEditor.range200"); renderSheet(); return; }
        sheet.error = "";
        log.step = "dates";
        renderSheet(true);
        return;
      }
    }
    const episodes = watchSelection(log);
    if (!episodes.length || episodes.length > 200) { sheet.error = T("tracker.watchEditor.range200"); renderSheet(); return; }
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      if (!log.review) {
        const action = log.advanced ? log.action : episodePlays(log, episodes[0].season, episodes[0].episode).length ? "rewatch" : "add";
        const dated = action === "remove" ? episodes : await logWatchTimes(log, episodes);
        if (sheet.log !== log || sheet.mode !== "log") return;
        log.review = {action, episodes:dated, effects:watchEffects(log, dated, action)};
        if (log.advanced) { sheet.busy = false; renderSheet(true); return; }
      }
      const data = await getJSON("/api/tracker/history/bulk", {provider_instance:state.instance, show:log.pick, revision:log.revision, action:log.review.action, scope:log.scope, episodes:log.review.episodes, remove_from_watchlist:log.unlist ? log.watchlistKey : ""});
      await afterWrite(data, `${plural(data.count, "watch", "watches")} ${log.review.action === "remove" ? "removed" : "saved"}`);
    } catch (err) {
      if (sheet.log !== log || sheet.mode !== "log") return;
      sheet.busy = false;
      sheet.error = err?.code === "changed_since" ? T("tracker.watchEditor.changedWhileOpen") : err?.code === "duplicate_watch_times" || err?.code === "key_exists" ? T("tracker.watchEditor.timeTaken") : writeError(err);
      if (!log.advanced) log.review = null;
      renderSheet();
    }
  }

  function renderLog() {
    const log = sheet.log;
    const picked = log.pick;
    if (picked?.type === "show" && log.target === "history" && log.advanced) return renderGuidedWatch(log);
    let body;
    if (!picked) {
      const results = log.results.map((r, i) => {
        const pick = `<button type="button" class="trk-match" data-trk-log-pick="${i}"><span class="trk-match-poster">${r.poster_path ? `<img src="/art/tmdb/${log.type === "movie" ? "movie" : "tv"}/${esc(encodeURIComponent(r.tmdb))}?fallback=2&size=w92" alt="" loading="lazy">` : symbol(log.type === "movie" ? "movie" : "tv")}</span><span class="trk-match-copy"><b>${esc(r.title)}</b><small>${esc(r.year || "")}</small></span></button>`;
        const info = canCard(logCardItem(log, r)) ? `<button type="button" class="trk-icon trk-match-info" data-trk-log-info="${i}" title="${TH("tracker.log.details")}" aria-label="Details for ${esc(r.title)}">${icon("info")}</button>` : "";
        return `<div class="trk-match-wrap">${pick}${info}</div>`;
      }).join("");
      body = `
        <div class="cw-type-filter" role="group" aria-label="${TH("tracker.log.kind")}">
          <button type="button" class="cw-filter-chip${log.type === "movie" ? " active" : ""}" data-trk-log-type="movie" aria-pressed="${log.type === "movie"}">${symbol("movie", "cw-type-icon")}<span>${TH("common.media.movie")}</span></button>
          <button type="button" class="cw-filter-chip${log.type === "show" ? " active" : ""}" data-trk-log-type="show" aria-pressed="${log.type === "show"}">${symbol("tv", "cw-type-icon")}<span>${TH("common.media.show")}</span></button>
        </div>
        <input type="search" class="cw-input" data-trk-log-q value="${esc(log.q)}" placeholder="${TH("tracker.log.searchPlaceholder")}" aria-label="${TH("tracker.log.search")}">
        ${log.message ? `<p class="trk-note">${esc(log.message)}</p>` : ""}
        ${results ? `<div class="trk-matches" role="list">${results}</div>` : ""}`;
    } else {
      const coverKind = picked.type === "movie" ? "movie" : "tv";
      const coverId = picked.tmdb || picked.ids?.tmdb;
      const coverUrl = coverId ? `/art/tmdb/${coverKind}/${encodeURIComponent(coverId)}?fallback=2&kind=poster&size=w154${window.CW?.ArtChoice?.param?.(coverKind, coverId) || ""}` : "";
      const cover = `<span class="trk-match-poster" data-trk-log-cover="${coverKind}">${coverUrl ? `<img src="${esc(coverUrl)}" alt="" decoding="async">` : symbol(coverKind)}</span>`;
      const targets = [["history", T("tracker.dates.watched"), "history"], ["watchlist", T("common.media.watchlist"), "bookmark"], ["collection", T("profile.tabs.collection"), "video_library"]]
        .map(([id, label, ic]) => `<button type="button" class="cw-filter-chip${log.target === id ? " active" : ""}" data-trk-log-target="${id}" aria-pressed="${log.target === id}">${symbol(ic, "cw-type-icon")}<span>${esc(label)}</span></button>`).join("");
      const showHistory = picked.type === "show" && log.target === "history";
      const seasonSel = showHistory ? renderWatchEditor(log) : "";
      const whenMode = log.whenMode || "today";
      const dateOptions = [["today", T("tracker.watchEditor.now")], ["release", T("tracker.watchEditor.releaseDate")], ["custom", T("tracker.log.chooseDate")]];
      if (showHistory && log.advanced) dateOptions.push(["range", T("tracker.watchEditor.dateRange")]);
      const whenChoices = dateOptions.map(([id, label]) => `<button type="button" class="cw-filter-chip${whenMode === id ? " active" : ""}" data-trk-log-date-mode="${id}" aria-pressed="${whenMode === id}">${esc(label)}</button>`).join("");
      const dateInput = (attr, label, value) => `<label class="trk-field"><span>${label}</span><input type="datetime-local" step="1" class="cw-input cw-timestamp-input" ${attr} value="${esc(value)}" max="${esc(localInput(new Date().toISOString()))}"></label>`;
      const whenRow = log.target === "history" && !log.review && !(showHistory && log.advanced && log.action === "remove") ? `<div class="trk-field"><span>${TH("tracker.log.when")}</span><div class="cw-type-filter" role="group" aria-label="${TH("tracker.log.when")}">${whenChoices}</div>${whenMode === "custom" ? dateInput("data-trk-log-when", T("tracker.watchEditor.whenLabel"), log.when) : whenMode === "range" ? `<div class="trk-fields">${dateInput("data-trk-log-when", T("tracker.log.from"), log.when)}${dateInput("data-trk-log-end", T("tracker.log.to"), log.end)}</div><small>${TH("tracker.log.spreadNote")}</small>` : ""}</div>` : "";
      const unlist = log.target === "history" && log.watchlistKey && !log.review && (!log.advanced || ["add", "rewatch"].includes(log.action)) ? `<label class="trk-check"><input type="checkbox" class="cw-checkbox" data-trk-log-unlist${log.unlist ? " checked" : ""}><span>${TH("tracker.detail.removeWatchlist")}</span></label>` : "";
      body = `
        <div class="trk-picked">${cover}<div><b>${esc(picked.title)}</b><small>${esc([picked.year, picked.type === "movie" ? T("common.media.movie") : T("common.media.show")].filter(Boolean).join(" · "))}</small></div>${log.fixed || log.review ? "" : `<button type="button" class="cw-btn" data-trk-log-repick>${icon("search")}<span>${TH("common.actions.change")}</span></button>`}</div>
        ${log.review ? "" : `<div class="cw-type-filter" role="group" aria-label="${TH("tracker.log.addTo")}">${targets}</div>`}
        ${seasonSel}
        ${whenRow}
        ${unlist}
        ${addEffectHtml(log.target)}`;
    }
    return `
      ${sheetHead("check_circle", log.review ? T("tracker.watchEditor.reviewChanges") : T("tracker.dates.watched"), picked ? "" : T("tracker.log.find"))}
      <form class="trk-form" data-trk-form="log" novalidate>
        ${body}
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>${picked?.type === "show" ? `<button type="button" class="cw-btn" data-trk-log-reload>${TH("tracker.watchEditor.reload")}</button>` : ""}` : ""}
        <footer class="trk-sheet-foot">
          <button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button>
          ${log.review ? `<button type="button" class="cw-btn" data-trk-log-back>${TH("common.actions.back")}</button>` : ""}
          ${picked ? `<button type="submit" class="cw-btn${log.review && log.action === "remove" ? " danger" : " primary"}"${sheet.busy || (picked.type === "show" && log.target === "history" && (!log.seasons?.length || (!log.review && !watchSelection(log).length))) ? " disabled" : ""}>${icon(log.review && log.action === "remove" ? "delete" : "check")}<span>${log.target !== "history" ? TH("tracker.ui.addIt") : log.review ? log.action === "remove" ? T("tracker.watchEditor.removeWatches") : T("tracker.footer.saveChanges") : log.advanced && picked.type === "show" ? T("tracker.ui.reviewChanges") : T("tracker.ui.watched")}</span></button>` : ""}
        </footer>
      </form>`;
  }

  function changeText(change) {
    const first = change.items?.[0];
    const sub = first?.row ? episodeLabel(first.row) || first.row.year || "" : "";
    const name = first?.row ? `${cardTitle(first.row)}${sub ? ` (${sub})` : ""}` : "a record";
    const others = change.count > 1 ? T("tracker.changes.andOthers", { others: plural(change.count - 1, "other", "others") }) : "";
    if (change.action === "note") {
      if (change.feature === "status_control") return T("tracker.changes.autoPref", { name, others });
      if (!first?.before || !first.after) {
        const note = first?.after || first?.before || {};
        const fields = ["status", "started", "finished", "notes", "tags", "fields"].filter(field => {
          const value = note[field];
          return value && (typeof value !== "object" || Object.keys(value).length);
        });
        if (first?.after && fields.length === 1 && fields[0] === "status") return T("tracker.changes.setStatus", { status: statusOf(note.status)?.label || note.status, name, others });
        const label = fields.map(field => NOTE_LABELS[field]).join(", ") || "title details";
        return `${T(first?.after ? "tracker.ui.addedFor" : "tracker.ui.removedFor", {label, name})}${others}`;
      }
      if (first.old_key !== first.key) return T("tracker.changes.moved", { name, others });
      if (first.fields?.length === 1 && first.fields[0] === "status") return first.after.status
        ? T("tracker.changes.setStatus", { status: statusOf(first.after.status)?.label || first.after.status, name, others })
        : T("tracker.changes.clearedStatus", { name, others });
      const fields = Array.from(new Set((first.fields || []).map((f) => NOTE_LABELS[f]).filter(Boolean)));
      return T("tracker.changes.changedFor", { fields: fields.length ? fields.join(", ") : T("tracker.changes.titleDetails"), name, others });
    }
    if (change.action === "add") return T("tracker.changes.logged", { name, others });
    if (change.action === "delete") return T("tracker.changes.deleted", { name, others });
    if (change.action === "undo") return T("tracker.changes.undid", { name, others });
    const fields = Array.from(new Set((first?.fields || []).map((f) => FIELD_LABELS[f]).filter(Boolean)));
    return T("tracker.changes.changedOf", { fields: fields.length ? fields.join(", ") : T("tracker.changes.details"), name, others });
  }

  const CHANGE_KINDS = [
    { id: "", label: T("profile.types.all") },
    { id: "logged", label: T("tracker.changes.kinds.logged"), icon: "add_circle" },
    { id: "edited", label: T("tracker.changes.kinds.edited"), icon: "edit" },
    { id: "deleted", label: T("tracker.changes.kinds.deleted"), icon: "delete" },
    { id: "notes", label: T("tracker.changes.kinds.notes"), icon: "edit_note" },
    { id: "undone", label: T("tracker.changes.kinds.undone"), icon: "undo" },
  ];

  function changeRow(c) {
    const f = c.feature === "notes" ? NOTES_VIEW : featureOf(c.feature);
    const mark = c.action === "delete" ? "delete" : c.action === "undo" ? "undo" : c.action === "add" ? "add_circle" : c.action === "note" ? "edit_note" : "edit";
    const status = c.undone ? `<span class="trk-change-tag">${icon("undo")}${TH("tracker.ui.undone")}</span>` : "";
    const undo = c.undoable ? `<button type="button" class="cw-btn" data-trk-undo="${esc(c.batch)}"${sheet.busy ? " disabled" : ""}>${icon("undo")}<span>${TH("tracker.changes.undo")}</span></button>` : "";
    return `<li class="trk-change${c.undone ? " is-undone" : ""}"><span class="trk-change-icon">${icon(mark)}</span><span class="trk-change-copy"><span class="trk-change-text">${esc(changeText(c))}</span><small>${icon(f.icon)}${esc(f.label)} · ${esc(timeText(c.at))}</small></span>${status}${undo}</li>`;
  }

  function renderChanges() {
    const expanded = new Set([...sheet.el?.querySelectorAll('[data-trk-auto-group][open]') || []].map(el => el.dataset.trkAutoGroup));
    const days = [];
    for (const c of sheet.changes) {
      const label = dayLabel(c.at);
      if (days[days.length - 1]?.label !== label) days.push({ label, rows: [] });
      const rows = days[days.length - 1].rows;
      let group = c.automatic_group && rows.find(row => row.group === c.automatic_group);
      if (!group) {
        group = {group:c.automatic_group || "", changes:[]};
        rows.push(group);
      }
      group.changes.push(c);
    }
    const filtered = !!(sheet.changeKind || sheet.changeQ.trim());
    const list = days.map((d) => `<h3 class="trk-change-day">${esc(d.label)}</h3><ul class="trk-changes">${d.rows.map(row => row.group ? `<li class="trk-auto-changes"><details data-trk-auto-group="${esc(row.group)}"${expanded.has(row.group) ? " open" : ""}><summary>${icon("autorenew")}<span>Automatic status updates · ${num(row.changes.length)} shown</span>${icon("expand_more")}</summary><ul class="trk-changes">${row.changes.map(changeRow).join("")}</ul></details></li>` : row.changes.map(changeRow).join("")).join("")}</ul>`).join("");
    const chips = CHANGE_KINDS.map((k) => chip(`data-trk-change-kind="${k.id}"`, k.label, { active: sheet.changeKind === k.id, iconName: k.icon || "" })).join("");
    const empty = sheet.busy && !sheet.changes.length
      ? emptyState(T("common.status.loading"), "hourglass_empty")
      : emptyState(filtered ? T("tracker.changes.noMatch") : T("tracker.changes.none"), filtered ? "search_off" : "history_toggle_off");
    const more = sheet.changes.length < sheet.changesTotal
      ? `<div class="trk-changes-more" data-trk-changes-more><button type="button" class="cw-btn" data-trk-sheet="more-changes"${sheet.busy ? " disabled" : ""}>${icon(sheet.busy ? "hourglass_empty" : "expand_more")}<span>${sheet.busy ? T("common.status.loading") : T("tracker.overview.showMore")}</span></button></div>`
      : "";
    const sub = sheet.changesAll ? T("tracker.changes.countDays", { changes: plural(sheet.changesAll, "change", "changes") }) : T("tracker.changes.subtitle");
    const count = filtered && !sheet.busy ? `<p class="trk-note trk-change-count" role="status">${esc(`${num(sheet.changesTotal)} of ${num(sheet.changesAll)} match`)}</p>` : "";
    return `
      ${sheetHead("manage_history", T("tracker.secondary.changes"), sub)}
      <div class="trk-form trk-change-form">
        <div class="trk-change-filters">
          <input type="search" class="cw-input" data-trk-change-q value="${esc(sheet.changeQ)}" placeholder="${TH("tracker.changes.searchPlaceholder")}" aria-label="${TH("tracker.changes.search")}" autocomplete="off">
          <div class="trk-seg" role="group" aria-label="${TH("tracker.changes.kind")}">${chips}</div>
        </div>
        ${count}
        <p class="trk-note">${TH("tracker.changes.groupHint")}</p>
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        ${list || empty}
        ${more}
      </div>`;
  }

  let changesObserver = null;

  function watchMoreChanges(dialog) {
    changesObserver?.disconnect();
    changesObserver = null;
    const sentinel = dialog.querySelector("[data-trk-changes-more]");
    if (!sentinel || typeof IntersectionObserver !== "function") return;
    changesObserver = new IntersectionObserver((entries) => {
      if (!entries.some((e) => e.isIntersecting) || sheet.busy || sheet.mode !== "changes") return;
      if (sheet.changes.length < sheet.changesTotal) loadChanges(true);
    }, { root: dialog, rootMargin: "240px" });
    changesObserver.observe(sentinel);
  }

  function importPick(imp) {
    const [id, instance] = String(imp.source || "").split("|");
    const row = imp.sources.find((r) => r.id === id);
    return { row, id, instance, label: row ? `${row.label}${instance !== "default" ? ` (${row.instances.find((i) => i.id === instance)?.label || instance})` : ""}` : "" };
  }

  function importTargets(imp) {
    const pick = importPick(imp);
    return imp.instances.filter((i) => !(pick.id === "CROSSWATCH" && pick.instance === i.id));
  }

  function renderImport() {
    const imp = sheet.imp;
    let body;
    if (imp.loading) body = `<p class="trk-note">${TH("tracker.import.loadingServices")}</p>`;
    else if (!imp.sources.length) body = emptyState(T("tracker.import.noServices"), "link_off");
    else {
      const pick = importPick(imp);
      const options = imp.sources.flatMap((r) => r.instances.map((i) => {
        const value = `${r.id}|${i.id}`;
        return `<option value="${esc(value)}"${value === imp.source ? " selected" : ""}>${esc(r.label)}${i.id === "default" ? "" : ` · ${esc(i.label)}`}</option>`;
      })).join("");
      const chips = FEATURES.filter((f) => pick.row?.features.includes(f.id)).map((f) => {
        const on = imp.features.has(f.id);
        return `<button type="button" class="cw-filter-chip${on ? " active" : ""}" data-trk-imp-feature="${f.id}" aria-pressed="${on}">${symbol(f.icon, "cw-type-icon")}<span>${esc(f.label)}</span></button>`;
      }).join("");
      const available = importTargets(imp);
      const creating = imp.target === "new";
      const targets = available.map((i) => `<option value="${esc(i.id)}"${i.id === imp.target ? " selected" : ""}>${esc(i.label)}</option>`).join("");
      const fileLink = `#import_export?target=${encodeURIComponent(imp.target === "new" ? state.instance : imp.target)}&returnTo=${encodeURIComponent("/#tracker")}`;
      body = `
        <div class="trk-fields">
          <label class="trk-field trk-field-wide"><span>${TH("tracker.log.from")}</span><select class="cw-select" data-trk-imp="source" aria-label="${TH("tracker.import.from")}">${options}</select></label>
        </div>
        <div class="trk-group-label">${TH("tracker.import.what")}</div>
        <div class="cw-type-filter" role="group" aria-label="${TH("tracker.import.what")}">${chips}</div>
        ${pick.row?.dropped && imp.features.has("history") ? `<label class="trk-check trk-imp-dropped"><input type="checkbox" class="cw-checkbox" data-trk-imp="dropped"${imp.dropped ? " checked" : ""}><span>${TH("tracker.import.dropped")}</span></label>` : ""}
        <div class="trk-fields">
          <label class="trk-field trk-field-wide"><span>${TH("tracker.import.how")}</span><select class="cw-select" data-trk-imp="mode" aria-label="${TH("tracker.import.how")}">
            <option value="merge"${creating ? "" : " selected"}${available.length ? "" : " disabled"}>${TH("tracker.import.merge")}</option>
            ${imp.canCreate ? `<option value="new"${creating ? " selected" : ""}>${TH("tracker.import.new")}</option>` : ""}
          </select></label>
          ${creating
            ? `<label class="trk-field trk-field-wide"><span>${TH("tracker.import.newName")}</span><input type="text" class="cw-input" data-trk-imp="name" value="${esc(imp.name)}" maxlength="24" placeholder="${TH("tracker.import.namePlaceholder", { name: pick.row?.label || T("tracker.secondary.import") })}"></label>`
            : `<label class="trk-field trk-field-wide"><span>${TH("tracker.instance.label")}</span><select class="cw-select" data-trk-imp="target" aria-label="${TH("tracker.import.into")}">${targets}</select></label>`}
        </div>
        <p class="trk-effect">${icon("info")}<span>${creating
          ? (imp.review ? T("tracker.import.newReviewHint") : T("tracker.import.newHint"))
          : T("tracker.import.mergeHint")} ${TH("tracker.ui.nothingWritten", {name: pick.label})}</span></p>
        ${!creating && !available.length ? `<p class="trk-note">No other tracker is available for this source. Choose another source${imp.canCreate ? " or import into a new tracker" : ""}.</p>` : ""}
        <label class="trk-check trk-imp-dropped"><input type="checkbox" class="cw-checkbox" data-trk-imp="review"${imp.review ? " checked" : ""}><span>${TH("tracker.import.review")}</span></label>
        <p class="trk-note">${imp.review ? T("tracker.import.reviewHint") : T("tracker.import.backgroundHint")}</p>
        ${imp.busy ? `<p class="trk-effect is-danger">${icon("sync")}<span>${TH("tracker.import.syncRunning")}</span></p>` : ""}
        <p class="trk-note">${TH("tracker.import.haveFile")}<a href="${esc(fileLink)}" data-trk-sheet="close">${TH("tracker.import.importFile")}</a></p>`;
    }
    const ready = !imp.loading && imp.sources.length && imp.features.size && !imp.busy && imp.target;
    return `
      ${sheetHead("download", T("tracker.secondary.import"), T("tracker.import.subtitle"))}
      <form class="trk-form" data-trk-form="import" novalidate>
        ${body}
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        <footer class="trk-sheet-foot">
          <button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button>
          ${imp.sources.length ? `<button type="submit" class="cw-btn primary"${ready && !sheet.busy ? "" : " disabled"}>${icon(imp.review ? "fact_check" : "download")}<span>${TH(imp.review ? "tracker.ui.reviewImport" : "tracker.ui.startImport")}</span></button>` : ""}
        </footer>
      </form>`;
  }

  function setImportSource(imp, value) {
    imp.source = value;
    const pick = importPick(imp);
    imp.features = new Set(pick.row?.features || []);
    if (imp.target !== "new" && !importTargets(imp).some((i) => i.id === imp.target)) imp.target = importTargets(imp)[0]?.id || (imp.canCreate ? "new" : "");
  }

  function setImportMode(imp, value) {
    if (value === "new" && imp.canCreate) {
      if (imp.target !== "new") imp.mergeTarget = imp.target;
      imp.target = "new";
    } else if (value === "merge") {
      const targets = importTargets(imp);
      imp.target = targets.find((i) => i.id === imp.mergeTarget)?.id || targets[0]?.id || (imp.canCreate ? "new" : "");
    }
  }

  async function openImport() {
    const imp = { loading: true, sources: [], instances: [], source: "", features: new Set(), target: state.instance, name: "", busy: false, canCreate: false, dropped: false, review: true };
    openSheet("import", { imp });
    try {
      const data = await getJSON("/api/tracker/import/sources");
      imp.sources = Array.isArray(data.sources) ? data.sources : [];
      imp.instances = Array.isArray(data.instances) ? data.instances : [];
      imp.busy = !!data.busy;
      imp.canCreate = data.can_create !== false;
      const first = imp.sources.find((r) => r.id !== "CROSSWATCH") || imp.sources[0];
      if (first) setImportSource(imp, `${first.id}|${first.instances[0].id}`);
    } catch (err) {
      sheet.error = writeError(err);
    }
    imp.loading = false;
    if (sheet.mode === "import" && sheet.imp === imp) renderSheet();
  }

  async function startImport() {
    const imp = sheet.imp;
    const pick = importPick(imp);
    if (sheet.busy || imp.busy || !pick.row || !imp.features.size) return;
    if (imp.target === "new" ? !imp.canCreate : !importTargets(imp).some((i) => i.id === imp.target)) return;
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      if (imp.target === "new" && !imp.review) {
        const label = imp.name.trim() || T("tracker.import.namePlaceholder", { name: pick.row.label });
        const made = await getJSON("/api/provider-instances/CROSSWATCH/next", { label, template: { connected: true } });
        imp.instances.push({ id: made.id, label });
        imp.target = made.id;
      }
      const dropped = !!(pick.row.dropped && imp.features.has("history") && imp.dropped);
      const data = await getJSON(imp.review ? "/api/tracker/import/preview" : "/api/tracker/import", { source: pick.id, source_instance: pick.instance, provider_instance: imp.target, features: Array.from(imp.features), dropped, ...(imp.target === "new" ? {name:imp.name.trim() || T("tracker.import.namePlaceholder", { name: pick.row.label })} : {}) });
      closeSheet();
      if (imp.target !== "new") state.instance = data.provider_instance || imp.target;
      if (imp.review && data.session?.id) {
        state.ref = null; state.title = null; state.picked.clear();
        state.offset = 0; state.scrollTop = 0;
        saveView();
        window.location.hash = `interactive_sync?session=${encodeURIComponent(data.session.id)}`;
        return;
      }
      state.imp = data.job || null;
      state.ref = null;
      state.title = null;
      state.progress = {};
      state.picked.clear();
      toast.show(T("tracker.import.started", { from: pick.label }));
      window.dispatchEvent(new CustomEvent("cw:tracker-check-changed"));
      await refresh();
    } catch (err) {
      sheet.busy = false;
      sheet.error = writeError(err);
      if (err?.code === "sync_running") imp.busy = true;
      renderSheet();
    }
  }

  function noteDraft(data) {
    const n = data?.note || {};
    const fields = {};
    for (const f of state.noteFields) if (fieldOk(f, n.fields?.[f.id])) fields[f.id] = n.fields[f.id];
    return {
      key: data.note_key,
      title: data.title?.title || n.title || "",
      year: data.title?.year || n.year || null,
      status: n.status || "",
      started: n.started || "",
      finished: n.finished || "",
      notes: n.notes || "",
      tags: [...(n.tags || [])],
      fields,
      expected: data.note || null,
      tagInput: "",
    };
  }

  function addTags(draft, raw) {
    for (const part of String(raw || "").split(",")) {
      const tag = part.split(/\s+/).filter(Boolean).join(" ").slice(0, 40);
      if (!tag || draft.tags.length >= MAX_TAGS || draft.tags.some((t) => t.toLowerCase() === tag.toLowerCase())) continue;
      draft.tags.push(tag);
    }
  }

  function fieldInput(field, value) {
    const attr = `data-trk-note-field="${esc(field.id)}"`;
    if (field.type === "bool") {
      return `<label class="trk-field trk-check"><input type="checkbox" class="cw-checkbox" ${attr}${value ? " checked" : ""}><span>${esc(field.name)}</span></label>`;
    }
    const v = value ?? "";
    const control = field.type === "choice"
      ? `<select class="cw-select" ${attr}><option value=""></option>${(field.choices || []).map((c) => `<option value="${esc(c)}"${c === v ? " selected" : ""}>${esc(c)}</option>`).join("")}</select>`
      : field.type === "number"
        ? `<input type="number" class="cw-input" ${attr} value="${esc(v)}" step="any">`
        : field.type === "date"
          ? `<input type="date" class="cw-input" ${attr} value="${esc(v)}">`
          : `<input type="text" class="cw-input" ${attr} value="${esc(v)}" maxlength="500">`;
    return `<label class="trk-field"><span>${esc(field.name)}</span>${control}</label>`;
  }

  function sheetError() {
    return sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : "";
  }

  function renderNotes() {
    const d = sheet.draft;
    const head = state.title?.title || { title: d.title };
    const busy = sheet.busy ? " disabled" : "";
    const automatic = !d.status_manual && !state.title?.status_control?.manual;
    const chips = [{ id: "", label: T("tracker.filters.noStatus"), icon: "remove" }, ...STATUSES]
      .map((s) => `<button type="button" class="cw-filter-chip${d.status === s.id ? " active" : ""}" data-trk-note-status="${s.id}" aria-pressed="${d.status === s.id}"${busy}>${symbol(s.icon, "cw-type-icon")}<span>${esc(s.label)}</span></button>`)
      .join("");
    const known = (state.notes.tags || []).map((t) => t.tag).filter((t) => !d.tags.some((x) => x.toLowerCase() === t.toLowerCase()));
    const tags = d.tags.map((t, i) => `<span class="trk-label">${esc(t)}<button type="button" data-trk-note-untag="${i}" aria-label="Remove tag ${esc(t)}">${icon("close")}</button></span>`).join("");
    const fields = state.noteFields.map((f) => fieldInput(f, d.fields[f.id])).join("");
    return `
      ${sheetHead(head, head.title || d.title || T("tracker.actions.myNotes"), T("tracker.notes.subtitle"))}
      <form class="trk-form" data-trk-form="notes" novalidate>
        <div class="trk-group-label">${TH("tracker.filters.status")}</div>
        <div class="trk-note-statuses" role="group" aria-label="${TH("tracker.filters.status")}">${chips}</div>
        <p class="trk-note">${state.title?.status_control?.reason ? esc(state.title.status_control.reason) : state.title?.status_control?.manual ? T("tracker.notes.manual") : state.title?.status_control?.origin === "sync" ? T("tracker.notes.fromSync") : T("tracker.notes.autoEnabled")}</p>
        ${state.trackerOptions?.automatic_status !== false ? `<button type="button" class="cw-btn" data-trk-sheet="enable-auto"${automatic || sheet.busy ? " disabled" : ""}>${sheet.busy ? T("common.status.saving") : automatic ? TH("tracker.ui.autoOn") : TH("tracker.ui.useAuto")}</button>` : ""}
        ${sheet.automationEnabled ? '<p class="trk-note" role="status">' + TH("tracker.notes.autoSaved") + '</p>' : ""}
        <div class="trk-fields">
          <label class="trk-field"><span>${TH("tracker.notes.started")}</span><input type="date" class="cw-input" data-trk-note="started" value="${esc(d.started)}"></label>
          <label class="trk-field"><span>${TH("tracker.notes.finished")}</span><input type="date" class="cw-input" data-trk-note="finished" value="${esc(d.finished)}"></label>
        </div>
        <div class="trk-field trk-field-wide trk-note-tags">
          <span id="trk-note-tags-label">${TH("tracker.notes.tags")}</span>
          ${tags ? `<span class="trk-tags">${tags}</span>` : ""}
          <input type="text" class="cw-input" data-trk-note-tag list="trk-note-tag-list" value="${esc(d.tagInput)}" placeholder="${d.tags.length ? T("tracker.notes.addTag") : T("tracker.notes.tagHint")}" maxlength="40" autocomplete="off" aria-labelledby="trk-note-tags-label"${d.tags.length >= MAX_TAGS ? " disabled" : ""}>
          <datalist id="trk-note-tag-list">${known.slice(0, 200).map((t) => `<option value="${esc(t)}"></option>`).join("")}</datalist>
        </div>
        <label class="trk-field trk-field-wide"><span>${TH("tracker.notes.notes")}</span><textarea class="cw-input trk-note-area" data-trk-note="notes" rows="5" maxlength="10000">${esc(d.notes)}</textarea></label>
        ${fields ? `<div class="trk-group-label">${TH("tracker.fields.yourFields")}</div><div class="trk-fields">${fields}</div>` : ""}
        <button type="button" class="trk-link-btn" data-trk-sheet="fields">${icon("tune")}<span>${state.noteFields.length ? T("tracker.fields.manage") : T("tracker.fields.addOwn")}</span></button>
        ${sheetError()}
        <footer class="trk-sheet-foot">
          ${d.expected ? `<button type="button" class="cw-btn danger" data-trk-sheet="note-clear"${busy}>${icon("delete")}<span>${TH("common.actions.clear")}</span></button>` : ""}
          <button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button>
          <button type="submit" class="cw-btn primary"${busy}>${icon("save")}<span>${TH("common.actions.save")}</span></button>
        </footer>
      </form>`;
  }

  function renderFields() {
    const busy = sheet.busy ? " disabled" : "";
    const rows = sheet.defs.map((f, i) => `<div class="trk-def">
        <label class="trk-field trk-def-name"><span>${TH("tracker.fields.name")}</span><input type="text" class="cw-input" data-trk-def="${i}:name" value="${esc(f.name)}" maxlength="40"></label>
        <label class="trk-field trk-def-type"><span>${TH("tracker.filters.type")}</span><select class="cw-select" data-trk-def="${i}:type">${FIELD_TYPES.map((t) => `<option value="${t.id}"${t.id === f.type ? " selected" : ""}>${esc(t.label)}</option>`).join("")}</select></label>
        ${f.type === "choice" ? `<label class="trk-field trk-def-choices"><span>${TH("tracker.fields.choices")}</span><input type="text" class="cw-input" data-trk-def="${i}:choicesText" value="${esc(f.choicesText || "")}"></label>` : ""}
        <button type="button" class="trk-icon is-danger" data-trk-def-remove="${i}" title="${TH("tracker.fields.remove")}" aria-label="Remove ${esc(f.name || "field")}">${icon("delete")}</button>
      </div>`).join("");
    return `
      ${sheetHead("tune", T("tracker.fields.yourFields"), T("tracker.fields.subtitle"))}
      <form class="trk-form" data-trk-form="fields" novalidate>
        ${rows ? `<div class="trk-defs">${rows}</div>` : `<p class="trk-note">${TH("tracker.fields.none")}</p>`}
        ${sheet.defs.length < MAX_FIELDS ? `<button type="button" class="cw-btn" data-trk-sheet="def-add">${icon("add")}<span>${TH("tracker.fields.add")}</span></button>` : ""}
        <p class="trk-note">${TH("tracker.fields.warning")}</p>
        ${sheetError()}
        <footer class="trk-sheet-foot">
          <button type="button" class="cw-btn" data-trk-sheet="${sheet.back ? "fields-back" : "close"}">${sheet.back ? T("common.actions.back") : T("common.actions.cancel")}</button>
          <button type="submit" class="cw-btn primary"${busy}>${icon("save")}<span>${TH("tracker.fields.save")}</span></button>
        </footer>
      </form>`;
  }

  function openNotes() {
    if (state.title?.note_key) openSheet("notes", { draft: noteDraft(state.title), automationEnabled: false });
  }

  function openFields() {
    const back = sheet.mode === "notes";
    openSheet("fields", { back, draft: back ? sheet.draft : null, defs: state.noteFields.map((f) => ({ ...f, choicesText: (f.choices || []).join(", ") })) });
  }

  async function postNote(draft, text) {
    addTags(draft, draft.tagInput);
    draft.tagInput = "";
    const body = {
      provider_instance: state.instance,
      key: draft.key,
      title: draft.title,
      year: draft.year,
      status: draft.status,
      status_manual: draft.status_manual === true,
      started: draft.started,
      finished: draft.finished,
      notes: draft.notes,
      tags: draft.tags,
      fields: draft.fields,
      expected: draft.expected,
    };
    const data = await getJSON("/api/tracker/note", body);
    if (!data.batch) {
      closeSheet();
      return;
    }
    await afterWrite(data, text || (data.note ? T("tracker.notes.saved", { title: draft.title }) : T("tracker.notes.removed", { title: draft.title })));
  }

  async function saveNotes(clear = false) {
    const draft = clear ? { ...sheet.draft, status_manual: true, status: "", started: "", finished: "", notes: "", tags: [], fields: {}, tagInput: "" } : sheet.draft;
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      await postNote(draft);
    } catch (err) {
      sheet.busy = false;
      sheet.error = err?.code === "changed_since" ? T("tracker.notes.changedElsewhere") : writeError(err);
      renderSheet();
    }
  }

  let completingNote = false;

  async function completeNote() {
    if (completingNote || !state.title?.note_key) return;
    completingNote = true;
    const buttons = document.querySelectorAll('[data-trk-detail="complete"], [data-trk-sheet="complete"]');
    buttons.forEach(button => {button.disabled = true; button.setAttribute("aria-busy", "true");});
    const draft = noteDraft(state.title);
    draft.status_manual = true;
    draft.status = "completed";
    if (!draft.finished) draft.finished = today();
    try {
      await postNote(draft, T("tracker.notes.markedCompleted", { title: draft.title }));
    } catch (err) {
      toast.show(writeError(err), { tone: "bad" });
    } finally {
      completingNote = false;
      buttons.forEach(button => {button.disabled = false; button.removeAttribute("aria-busy");});
    }
  }

  async function saveFields() {
    const fields = sheet.defs.map((f) => ({
      id: f.id || "",
      name: f.name,
      type: f.type,
      ...(f.type === "choice" ? { choices: String(f.choicesText || "").split(",").map((c) => c.trim()).filter(Boolean) } : {}),
    }));
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const data = await getJSON("/api/tracker/note-fields", { provider_instance: state.instance, fields });
      state.noteFields = Array.isArray(data.fields) ? data.fields : [];
      if (sheet.back && sheet.draft) {
        const draft = sheet.draft;
        const kept = {};
        for (const f of state.noteFields) if (fieldOk(f, draft.fields[f.id])) kept[f.id] = draft.fields[f.id];
        draft.fields = kept;
        openSheet("notes", { draft });
      } else {
        closeSheet();
        toast.show(T("tracker.fields.saved"));
      }
      setSlot("title", renderTitle());
    } catch (err) {
      sheet.busy = false;
      sheet.error = writeError(err);
      renderSheet();
    }
  }

  function renderTitleSheet() {
    return `${sheetHead('info', T('tracker.ui.titleDetails'), '')}<div class="trk-form trk-detail">${state.title ? titleBody(state.title) : `<div class="trk-detail-empty" role="status">${symbol("hourglass_empty")}<span>${TH("common.status.loading")}</span></div>`}</div>`;
  }

  function renderSheet(focusFirst) {
    const el = ensureSheet();
    const dialog = el.querySelector(".trk-sheet");
    dialog.classList.toggle('trk-title-sheet', sheet.mode === 'title');
    const scroll = dialog.scrollTop;
    const active = dialog.contains(document.activeElement) ? document.activeElement : null;
    const filterFocus = sheet.mode === 'filters' ? active?.dataset?.trkInput : '';
    const logFocus = sheet.mode === "log" && !focusFirst && active ? Array.from(active.attributes).find(a => a.name.startsWith("data-trk-log-") || a.name === "data-trk-watch-pick") : null;
    const keep = ["data-trk-log-q", "data-trk-note-tag", "data-trk-change-q"].find((name) => active?.hasAttribute?.(name));
    const editValues = sheet.mode === "edit" && !focusFirst
      ? Array.from(dialog.querySelectorAll("[data-trk-edit], [data-trk-id]"), input => [input.hasAttribute("data-trk-edit") ? "data-trk-edit" : "data-trk-id", input.dataset.trkEdit || input.dataset.trkId, input.value])
      : [];
    const renderers = {
      instances: () => `${sheetHead('badge', T('tracker.ui.chooseTracker'), '')}<div class="trk-form trk-source-choices">${state.instances.map(instance => `<button type="button" class="cw-btn" data-trk-sheet-instance="${esc(instance.id)}" aria-pressed="${instance.id === state.instance}">${symbol('badge')}<span>${esc(instance.label)}</span>${instance.id === state.instance ? symbol('check', 'trk-source-check') : ''}</button>`).join('')}</div>`,
      filters: () => `${sheetHead('filter_list', T('tracker.ui.filters'), '')}<div class="trk-form">${renderExtraFilters()}<footer class="trk-sheet-foot"><button type="button" class="cw-btn primary" data-trk-sheet="close">${TH("common.status.done")}</button></footer></div>`,
      edit: renderEdit, delete: renderDelete, rate: renderRate, log: renderLog, title: renderTitleSheet,
      views: renderSavedViews, "title-remove": renderTitleRemoval, automation: renderAutomation,
      origin: () => `${sheetHead("alt_route", T("editor.table.origin"), "Tracker")}<div class="trk-form"><div class="cw-origin-body" data-trk-origin>${TH("tracker.origin.lookingUp")}</div></div>`,
      changes: renderChanges, import: renderImport, notes: renderNotes, fields: renderFields, "title-edit": renderTitleEdit, "notes-bulk": renderNotesBulk,
    };
    dialog.innerHTML = (renderers[sheet.mode] || renderChanges)();
    for (const [attr, name, value] of editValues) {
      const input = dialog.querySelector(`[${attr}="${name}"]`);
      if (input) input.value = value;
    }
    dialog.scrollTop = focusFirst ? 0 : scroll;
    if (filterFocus) dialog.querySelector(`[data-trk-input="${CSS.escape(filterFocus)}"]`)?.focus({preventScroll:true});
    if (sheet.mode === "rate" || (sheet.mode === "edit" && sheet.feature === "ratings")) wireRatingPreview(dialog);
    if (el.hidden) {
      el.hidden = false;
      document.documentElement.classList.add("cw-ia-sheet-open");
    }
    const search = dialog.querySelector("[data-trk-log-q]");
    const kept = keep ? dialog.querySelector(`[${keep}]`) : null;
    if (kept && !kept.disabled) {
      kept.focus();
      kept.setSelectionRange?.(kept.value.length, kept.value.length);
    } else if (logFocus) {
      dialog.querySelector(`[${logFocus.name}="${CSS.escape(logFocus.value)}"]`)?.focus({preventScroll:true});
    } else if (focusFirst) {
      (search || dialog).focus({ preventScroll: true });
    }
    if (sheet.mode === "changes") watchMoreChanges(dialog);
    else changesObserver?.disconnect();
  }

  let mobileSheetLayer = null;
  let mobileTitleLayer = null;
  function openSheet(mode, extra = {}) {
    clearTimeout(reviewTimer);
    if (!sheet.el || sheet.el.hidden) {
      sheet.lastFocus = document.activeElement;
      sheet.pageScroll = window.scrollY;
      sheet.returnToTitle = false;
    } else if (sheet.mode === 'title' && mode !== 'title') {
      sheet.returnToTitle = true;
      sheet.detailScroll = sheet.el.querySelector('.trk-sheet').scrollTop;
    }
    Object.assign(sheet, { mode, item: null, statusTouched: false, feature: "history", busy: false, error: "", keys: [], token: sheet.token + 1 }, extra);
    if (mode === "rate") { sheet.communityRating = ""; sheet.communityScore = null; }
    renderSheet(true);
    if (window.CW?.MobileUI?.mobile()) {
      if (!mobileSheetLayer) mobileSheetLayer = window.CW.MobileUI.layer(() => { mobileSheetLayer = null; mobileTitleLayer = null; closeSheet(false, true); });
      else if (sheet.returnToTitle && !mobileTitleLayer) mobileTitleLayer = window.CW.MobileUI.layer(() => { mobileTitleLayer = null; closeSheet(true, true); });
    }
    if (mode === "rate") loadCommunityRating(sheet.token);
  }

  function closeSheet(returnToTitle = true, fromBack = false) {
    if (!fromBack && mobileSheetLayer) return (returnToTitle && mobileTitleLayer ? mobileTitleLayer : mobileSheetLayer).dismiss();
    clearTimeout(reviewTimer);
    if (!sheet.el || sheet.el.hidden) return;
    if (returnToTitle && sheet.returnToTitle && state.ref && narrow()) {
      sheet.returnToTitle = false;
      openSheet('title');
      sheet.el.querySelector('.trk-sheet').scrollTop = sheet.detailScroll || 0;
      return;
    }
    changesObserver?.disconnect();
    changesObserver = null;
    sheet.el.hidden = true;
    const wasFilters = sheet.mode === 'filters';
    sheet.mode = "";
    if (wasFilters) { state.filtersOpen = false; setSlot('controls', renderControls()); }
    sheet.returnToTitle = false;
    document.documentElement.classList.remove("cw-ia-sheet-open");
    const focus = sheet.lastFocus?.isConnected ? sheet.lastFocus : state.root?.querySelector('.trk-play.is-selected');
    if (!state.root?.hidden && !state.root?.classList.contains('hidden')) {
      focus?.focus?.({ preventScroll: true });
      window.scrollTo({top:sheet.pageScroll || 0,behavior:'instant'});
    }
  }

  function formChanges() {
    const item = sheet.item;
    const form = sheet.el.querySelector('[data-trk-form="edit"]');
    const changes = {};
    const val = (name) => form.querySelector(`[data-trk-edit="${name}"]`)?.value ?? null;
    const date = val("date");
    if (date !== null && localInput(date) !== localInput(item.date)) {
      const d = new Date(date);
      changes.date = date && !Number.isNaN(d.getTime()) ? d.toISOString() : "";
    }
    const rating = val("rating");
    const status = val("status");
    if (status !== null && (sheet.statusTouched || status !== (item.status || ""))) changes.status = status;
    if (rating !== null && Number(rating) !== Number(item.rating)) changes.rating = Number(rating);
    const pct = val("progress_percent");
    if (pct !== null && pct !== "" && Number(pct) !== Math.round(Number(item.progress_percent))) changes.progress_percent = Number(pct);
    const child = isChild(item);
    const title = (val("title") ?? "").trim();
    if (title !== String((child ? item.series_title : item.title) ?? "")) changes.title = title;
    const year = (val("year") ?? "").trim();
    if (year !== String((child ? item.series_year : item.year) ?? "")) changes.year = year === "" ? null : Number(year);
    for (const group of ["ids", "show_ids"]) {
      const diff = {};
      form.querySelectorAll(`[data-trk-id^="${group}:"]`).forEach((input) => {
        const name = input.dataset.trkId.split(":")[1];
        const before = String(item[group]?.[name] ?? "");
        const now = input.value.trim();
        if (now !== before) diff[name] = now;
      });
      if (Object.keys(diff).length) changes[group] = diff;
    }
    if (sheet.feature === "collection") {
      const media = mediaChanges(form, item);
      if (media !== undefined) changes.media = media;
    }
    return changes;
  }

  async function afterWrite(data, text) {
    if (sheet.mode !== 'title') closeSheet();
    toast.show(text, { undo: data?.batch ? () => undo(data.batch) : null });
    state.progress = {};
    await refresh();
  }

  async function saveEdit() {
    if (sheet.busy) return;
    const changes = formChanges();
    if (!Object.keys(changes).length) {
      closeSheet();
      return;
    }
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const data = await getJSON("/api/tracker/edit", { feature: sheet.feature, provider_instance: state.instance, key: sheet.item.key, changes });
      if (state.ref && !state.ref.show && state.ref.key === sheet.item.key && data.key) state.ref = { ...state.ref, key: data.key };
      await afterWrite(data, T("tracker.toast.saved", { title: cardTitle(data.item || sheet.item) }));
    } catch (err) {
      sheet.busy = false;
      sheet.error = err?.code === "changed_since" ? T("tracker.edit.recordChanged") : writeError(err);
      renderSheet();
    }
  }

  async function applyBulkStatus(status) {
    if (state.bulkStatusBusy || state.picking || !state.picked.size || (status !== "" && !statusOf(status))) return;
    const instance = state.instance, view = state.view, keys = Array.from(state.picked);
    const notes = view === "titles";
    state.bulkStatusBusy = true;
    renderSlots(false);
    try {
      const data = await getJSON(notes ? "/api/tracker/notes/bulk" : "/api/tracker/titles/status", {
        provider_instance: instance, keys, action: "status", status,
        ...(notes ? {} : {view, feature:view === "shows" ? "history" : listFeature()}),
      });
      toast.show(data.count ? T("tracker.toast.updatedTitles", { titles: plural(data.count, "title", "titles") }) : T("tracker.toast.noTitlesChanged"), data.batch ? {undo:() => undo(data.batch, instance)} : {});
      if (state.instance === instance && state.view === view) {
        keys.forEach(key => state.picked.delete(key));
        state.offset = 0;
        state.progress = {};
        await refresh();
      }
    } catch (err) {
      toast.show(writeError(err), {tone:"bad"});
    } finally {
      state.bulkStatusBusy = false;
      renderSlots(false);
    }
  }

  async function saveNotesBulk() {
    if (sheet.busy || !sheet.keys.length) return;
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const data = await getJSON("/api/tracker/notes/bulk", {provider_instance: state.instance, keys: sheet.keys, action: "remove"});
      if (sheet.keys.includes(state.ref?.note)) { state.ref = null; state.title = null; }
      closeSheet();
      state.picked.clear();
      toast.show(data.count ? T("tracker.toast.removedTitles", { titles: plural(data.count, "title", "titles") }) : T("tracker.toast.noTitlesChanged"), data.batch ? {undo: () => undo(data.batch)} : {});
      state.offset = 0;
      state.progress = {};
      await refresh();
    } catch (err) {
      sheet.busy = false;
      sheet.error = writeError(err);
      renderSheet();
    }
  }

  function renderTitleRemoval() {
    const preview = sheet.removal;
    return `${sheetHead("delete", T("tracker.removeTitle.head"), plural(sheet.keys.length, "title", "titles"))}<div class="trk-form">
      <div class="cw-type-filter"><button type="button" class="cw-btn${sheet.removeScope === "history" ? " primary" : ""}" data-trk-sheet="remove-history"${sheet.busy ? " disabled" : ""}>${TH("tracker.removeTitle.history")}</button><button type="button" class="cw-btn${sheet.removeScope === "all" ? " primary" : ""}" data-trk-sheet="remove-all"${sheet.busy ? " disabled" : ""}>${TH("tracker.removeTitle.everything")}</button></div>
      <p class="trk-note">${sheet.removeScope === "all" ? T("tracker.removeTitle.allHint") : T("tracker.removeTitle.historyHint")}</p>
      ${preview ? Object.entries(preview.counts).filter(([,count]) => count).map(([feature,count]) => `<p>${esc(feature === "statuses" ? T("tracker.ui.autoCleared") : feature === "notes" ? T("tracker.ui.notesStatuses") : FEATURES.find(f => f.id === feature)?.label || feature)}: <b>${num(count)}</b></p>${["notes","statuses"].includes(feature) ? "" : effectHtml(feature,count)}`).join("") : '<p role="status">' + TH("tracker.removeTitle.checking") + '</p>'}
      <p class="trk-note">${TH("tracker.removeTitle.undoHint")}</p>${sheetError()}
      <footer class="trk-sheet-foot"><button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.actions.cancel")}</button><button type="button" class="cw-btn danger" data-trk-sheet="remove-confirm"${sheet.busy || !preview || !Object.values(preview.counts).some(Boolean) ? " disabled" : ""}>${TH("common.actions.remove")}</button></footer></div>`;
  }

  function openTitleRemoval(keys) {
    openSheet("title-remove", {keys, removal:null, removeScope:"history"});
    void loadTitleRemoval("history");
  }

  async function loadTitleRemoval(scope) {
    sheet.busy = true; sheet.removeScope = scope; sheet.removal = null; sheet.error = "";
    const token = sheet.token;
    renderSheet();
    try {
      const data = await getJSON("/api/tracker/titles/remove", {provider_instance:state.instance,keys:sheet.keys,scope});
      if (sheet.token !== token || sheet.mode !== "title-remove") return;
      sheet.removal = data;
    } catch (err) { if (sheet.token === token) sheet.error = writeError(err); }
    if (sheet.token === token) {sheet.busy = false; renderSheet();}
  }

  async function confirmTitleRemoval() {
    if (sheet.busy || !sheet.removal) return;
    sheet.busy = true; renderSheet();
    try {
      const data = await getJSON("/api/tracker/titles/remove", {provider_instance:state.instance,keys:sheet.keys,scope:sheet.removeScope,token:sheet.removal.token});
      state.ref = null; state.title = null; state.picked.clear(); state.progress = {};
      closeSheet(false);
      toast.show(T("tracker.removeTitle.removed"), {undo:() => undo(data.batch)});
      await refresh();
    } catch (err) {sheet.busy = false; sheet.removal = null; sheet.error = writeError(err); renderSheet();}
  }

  function renderAutomation() {
    return `${sheetHead("autorenew", T("tracker.automation.title"), T("tracker.automation.local"))}<div class="trk-form trk-automation"><section class="trk-review" data-trk-review>${renderReview()}</section>
      <footer class="trk-sheet-foot"><button type="button" class="cw-btn" data-trk-sheet="close">${TH("common.status.done")}</button></footer></div>`;
  }

  function openAutomation() {
    openSheet("automation", {autoReview:null,reviewBusy:false,reviewError:""});
    void loadReview();
  }

  let reviewTimer = null;

  function renderReview() {
    const job = sheet.autoReview || {state:"idle"};
    const active = ["scanning","processing"].includes(job.state);
    const message = job.state === "scanning" ? T("tracker.automation.scanning", { count: Number(job.scanned || 0) })
      : job.state === "processing" ? T("tracker.automation.processing", { done: Number(job.processed || 0), total: Number(job.titles || 0) })
      : job.state === "complete" ? T("tracker.automation.complete", { count: Number(job.processed || 0) })
      : job.state === "cancelled" ? T("tracker.automation.cancelled", { count: Number(job.processed || 0) }) : T("tracker.automation.idleHint");
    const loading = !sheet.autoReview && !sheet.reviewError;
    const retry = !sheet.autoReview && !!sheet.reviewError;
    const heading = active ? T("tracker.automation.updating") : job.state === "complete" ? T("tracker.automation.done") : T("tracker.automation.update");
    return `<h3>${heading}</h3><p class="trk-note">${active ? T("tracker.automation.closeHint") : T("tracker.automation.explain")}</p>
      ${job.state !== "idle" ? `<p role="status">${esc(message)}</p>` : ""}
      ${job.titles && ["processing","complete"].includes(job.state) ? `<progress aria-label="${TH("tracker.automation.reviewed")}" max="${Number(job.titles)}" value="${Number(job.processed || 0)}"></progress>` : ""}
      <button type="button" class="cw-btn${active ? "" : " primary"}" data-trk-sheet="${retry ? "review-refresh" : active ? "review-cancel" : "review-start"}"${sheet.reviewBusy || loading ? " disabled" : ""}>${loading ? T("tracker.meta.checking") : retry ? T("common.actions.tryAgain") : active ? T("tracker.automation.stop") : job.state === "complete" ? T("tracker.automation.again") : T("tracker.automation.start")}</button>
      ${job.state === "complete" ? '<p class="trk-note">' + TH("tracker.automation.saved") + '</p>' : ""}
      ${sheet.reviewError ? `<p class="trk-error" role="alert">${esc(sheet.reviewError)}</p>` : ""}`;
  }

  function updateReview() {
    const region = sheet.el?.querySelector("[data-trk-review]");
    if (!region) return;
    const focused = region.contains(document.activeElement);
    region.innerHTML = renderReview();
    if (focused) region.querySelector("button")?.focus();
  }

  async function loadReview() {
    clearTimeout(reviewTimer);
    if (sheet.mode !== "automation" || sheet.el?.hidden) return;
    if (document.hidden) {reviewTimer = setTimeout(loadReview,5000); return;}
    const token = sheet.token;
    try {
      const data = await getJSON(`/api/tracker/automatic-status/review?provider_instance=${encodeURIComponent(state.instance)}`);
      if (token !== sheet.token || sheet.mode !== "automation" || sheet.el?.hidden) return;
      const finished = ['scanning','processing'].includes(sheet.autoReview?.state) && data.review?.state === 'complete';
      sheet.autoReview = data.review;
      sheet.reviewError = "";
      updateReview();
      if (finished) {
        state.progress = {};
        void refresh();
      }
      if (["scanning","processing"].includes(data.review?.state)) reviewTimer = setTimeout(loadReview,5000);
    } catch (err) {
      if (token === sheet.token && sheet.mode === "automation") {sheet.reviewError = writeError(err); updateReview();}
    }
  }

  async function reviewAction(action) {
    if (sheet.reviewBusy) return;
    clearTimeout(reviewTimer);
    sheet.reviewBusy = true; sheet.reviewError = ""; updateReview();
    const token = sheet.token;
    try {
      const data = await getJSON("/api/tracker/automatic-status", {provider_instance:state.instance,action});
      if (token !== sheet.token || sheet.mode !== "automation") return;
      sheet.autoReview = data.review;
      sheet.reviewBusy = false;
      updateReview();
      if (["scanning","processing"].includes(data.review?.state)) reviewTimer = setTimeout(loadReview,5000);
    } catch (err) {
      if (token === sheet.token && sheet.mode === "automation") {sheet.reviewBusy = false; sheet.reviewError = writeError(err); updateReview();}
    }
  }

  async function enableTitleAutomation() {
    if (sheet.busy) return;
    const token = sheet.token, instance = state.instance, key = sheet.draft.key;
    sheet.busy = true; sheet.error = ""; renderSheet();
    try {
      await getJSON("/api/tracker/automatic-status", {provider_instance:instance,action:"enable-title",key});
      if (sheet.token !== token || state.instance !== instance) return;
      sheet.draft.status_manual = false;
      sheet.automationEnabled = true;
      if (state.title?.note_key === key) state.title.status_control = {origin:"automatic",manual:false};
      await loadTitle();
      if (sheet.token !== token || state.instance !== instance) return;
      if (state.title?.note_key === key) {
        sheet.draft.expected = state.title.note || null;
        sheet.draft.status = state.title.note?.status || "";
      }
      sheet.busy = false;
      renderSheet();
      sheet.el.querySelector('[data-trk-sheet="close"]')?.focus();
    } catch (err) {
      if (sheet.token === token && state.instance === instance) {
        sheet.busy = false; sheet.error = writeError(err); renderSheet();
      }
    }
  }

  async function deleteKeys(keys, feature) {
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const data = await getJSON("/api/tracker/delete", { feature, provider_instance: state.instance, keys });
      closeSheet();
      state.picked.clear();
      if (state.ref && !state.ref.show && keys.includes(state.ref.key)) state.ref = null;
      toast.show(T("tracker.toast.deletedRecords", { records: plural(data.count || keys.length, "record", "records") }), { undo: () => undo(data.batch) });
      state.progress = {};
      await refresh();
    } catch (err) {
      sheet.busy = false;
      sheet.error = writeError(err);
      renderSheet();
    }
  }

  async function undo(batch, instance = state.instance) {
    try {
      await getJSON("/api/tracker/undo", { provider_instance: instance, batch });
      toast.show(T("tracker.changes.undone"));
      if (sheet.mode === "changes") await loadChanges(false);
      state.progress = {};
      await refresh();
    } catch (err) {
      const text = writeError(err);
      if (sheet.mode === "changes") {
        sheet.busy = false;
        sheet.error = text;
        renderSheet();
      } else {
        toast.show(text, { tone: "bad" });
      }
    }
  }

  async function loadChanges(append) {
    const token = ++changesRequest;
    sheet.busy = true;
    sheet.error = "";
    if (!append) {
      sheet.changes = [];
      sheet.changesTotal = 0;
    }
    renderSheet();
    try {
      const params = new URLSearchParams({
        provider_instance: state.instance,
        offset: String(append ? sheet.changes.length : 0),
        limit: "40",
        kind: sheet.changeKind,
        q: sheet.changeQ.trim(),
      });
      const data = await getJSON(`/api/tracker/changes?${params}`);
      if (token !== changesRequest) return;
      const rows = Array.isArray(data.changes) ? data.changes : [];
      sheet.changes = append ? sheet.changes.concat(rows) : rows;
      sheet.changesTotal = Number(data.total || 0);
      sheet.changesAll = Number(data.all ?? data.total ?? 0);
    } catch (err) {
      if (token !== changesRequest) return;
      sheet.error = writeError(err);
    }
    sheet.busy = false;
    if (sheet.mode === "changes") renderSheet();
  }

  let changesRequest = 0;
  let changeSearchTimer = 0;

  function headItem() {
    const data = state.title;
    const head = data?.title || {};
    const ids = { ...(head.ids || {}) };
    if (head.art?.tmdb) ids.tmdb = head.art.tmdb;
    return { type: data?.show ? "show" : "movie", title: head.title, year: head.year, ids };
  }

  function openLog(prefill) {
    const log = { type: "movie", q: "", results: [], message: "", pick: null, fixed: false, target: "history", whenMode: "today", when: localInput(new Date().toISOString()), seasons: null, season: 1, episode: "", watchlistKey: "", unlist: true, searchToken: 0, advanced: false, action: "add", scope: "latest", selected: new Set(), plays: [], metadata: new Map(), review: null, end: localInput(new Date().toISOString()) };
    if (prefill) Object.assign(log, prefill, { fixed: true });
    openSheet("log", { log });
    if (log.pick?.type === "show") loadSeasons(log);
  }

  function logFromTitle(item = null) {
    const data = state.title;
    if (!data) return;
    const head = headItem();
    const next = state.progress[head.ids.tmdb]?.next;
    openLog({
      type: head.type,
      pick: { type: head.type, tmdb: head.ids.tmdb, title: head.title, year: head.year, ids: head.ids },
      watchlistKey: data.watchlist[0]?.key || "",
      season: item?.season ?? next?.season ?? 1,
      episode: item?.episode ?? next?.episode ?? "",
    });
  }

  async function loadSeasons(log) {
    if (!log.pick?.tmdb) {
      log.seasons = [];
      sheet.error = T("tracker.map.noTmdb");
      renderSheet();
      return;
    }
    try {
      const tmdb = log.pick.tmdb;
      const data = await getJSON(`/api/tracker/watch-editor?${inst()}&tmdb=${encodeURIComponent(tmdb)}`);
      if (sheet.log !== log || log.pick?.tmdb !== tmdb) return;
      log.plays = data.plays || [];
      log.playIndex = new Map();
      for (const play of log.plays) {
        const key = `${play.season}:${play.episode}`;
        if (!log.playIndex.has(key)) log.playIndex.set(key, []);
        log.playIndex.get(key).push(play);
      }
      for (const plays of log.playIndex.values()) plays.sort((a, b) => Date.parse(b.date) - Date.parse(a.date));
      if (log.advanced) log.step = "episodes";
      log.revision = data.revision;
      log.selected.clear();
      log.review = null;
      log.seasons = (data.seasons || []).filter((s) => s.episodes > 0);
      if (!log.seasons.some((s) => s.season === log.season)) log.season = log.seasons.find((s) => s.season > 0)?.season ?? log.seasons[0]?.season ?? 1;
      if (!Number(log.episode)) {
        const next = log.seasons.filter(s => s.season > 0).flatMap(s => Array.from({length:s.episodes}, (_, i) => ({season:s.season, episode:i + 1}))).find(ep => !episodePlays(log, ep.season, ep.episode).length);
        if (next) { log.season = next.season; log.episode = String(next.episode); }
        else log.episode = "1";
      }
      loadWatchSeason(log);
    } catch (err) {
      log.seasons = [];
      sheet.error = writeError(err);
    }
    if (sheet.mode === "log" && sheet.log === log) renderSheet();
  }

  let logSearchTimer = 0;

  function searchLog() {
    const log = sheet.log;
    clearTimeout(logSearchTimer);
    logSearchTimer = setTimeout(async () => {
      const q = log.q.trim();
      const token = ++log.searchToken;
      if (q.length < 2) {
        log.results = [];
        log.message = "";
        renderSheet();
        return;
      }
      log.message = T("tracker.log.searching");
      renderSheet();
      try {
        const data = await getJSON(`/api/metadata/search?q=${encodeURIComponent(q)}&typ=${log.type}&limit=8`);
        if (token !== log.searchToken || sheet.log !== log) return;
        log.results = Array.isArray(data.results) ? data.results : [];
        log.message = log.results.length ? "" : T("tracker.log.noMatches");
      } catch (err) {
        if (token !== log.searchToken) return;
        log.results = [];
        log.message = err?.message === "TMDb key missing" ? T("tracker.log.needsKey") : T("tracker.log.searchFailed");
      }
      if (sheet.log === log) renderSheet();
    }, 300);
  }

  async function pickLog(index) {
    const log = sheet.log;
    const r = log.results[index];
    if (!r) return;
    const tmdb = String(r.tmdb);
    log.pick = { type: log.type, tmdb, title: r.title, year: r.year, ids: { tmdb } };
    log.seasons = null;
    log.metadata.clear();
    log.selected.clear();
    log.review = null;
    log.episode = "";
    renderSheet();
    if (log.type === "show") loadSeasons(log);
    try {
      const data = await getJSON("/api/metadata/resolve", { entity: log.type, ids: { tmdb: r.tmdb } });
      const ids = data?.result?.ids || {};
      if (sheet.log === log && log.pick?.tmdb === tmdb) {
        for (const name of ["imdb", "tvdb", "trakt"]) if (ids[name]) log.pick.ids[name] = String(ids[name]);
      }
    } catch {}
  }

  async function logWatchTimes(log, items) {
    const mode = log.whenMode || "custom";
    if (mode === "range") {
      const start = Date.parse(log.when), end = Date.parse(log.end);
      if (!Number.isFinite(start) || !Number.isFinite(end) || start > end || end > Date.now()) throw new Error(T("tracker.log.badRange"));
      return items.map((item, i) => ({...item, watched_at:new Date(start + (end - start) * i / Math.max(1, items.length - 1)).toISOString()}));
    }
    if (mode !== "release") {
      const date = mode === "today" ? new Date() : new Date(log.when);
      if (Number.isNaN(date.getTime()) || date.getTime() > Date.now()) throw new Error(T("tracker.log.badDate"));
      return items.map(item => ({ ...item, watched_at: date.toISOString() }));
    }
    const pick = log.pick;
    let dates;
    if (pick.type === "show") {
      for (const season of new Set(items.map(item => item.season))) await loadWatchSeason(log, season, false);
      dates = items.map(item => log.metadata.get(item.season)?.find(ep => Number(ep.episode) === item.episode)?.air_date);
    } else {
      const data = await getJSON("/api/metadata/resolve", { entity: "movie", ids: pick.ids });
      dates = [data.result?.detail?.release_date];
    }
    return items.map((item, i) => {
      const date = String(dates[i] || "").slice(0, 10);
      const stamp = `${date}T12:00:00Z`;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || Number.isNaN(Date.parse(stamp))) throw new Error(T("tracker.log.noRelease"));
      if (date > new Date().toISOString().slice(0, 10)) throw new Error(T("tracker.log.futureRelease"));
      return { ...item, watched_at: new Date(Math.min(Date.parse(stamp), Date.now())).toISOString() };
    });
  }

  async function saveLog() {
    const log = sheet.log;
    const p = log.pick;
    if (!p) return;
    if (log.target === "history" && p.type === "show") return saveWatchEditor(log);
    let items = [{ type: p.type, title: p.title, year: p.year, ids: { ...p.ids } }];
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      if (log.target === "history") items = await logWatchTimes(log, items);
      const data = await getJSON("/api/tracker/add", {
        feature: log.target,
        provider_instance: state.instance,
        items,
        remove_from_watchlist: log.target === "history" && log.unlist ? log.watchlistKey : "",
      });
      const what = items.length > 1 ? plural(data.count, "episode", "episodes") : p.title;
      await afterWrite(data, log.target === "history" ? T("tracker.toast.logged", { what }) : T("tracker.toast.addedTo", { what, target: T({ watchlist: "common.media.watchlist", collection: "profile.tabs.collection" }[log.target] || "profile.tabs.collection").toLowerCase() }));
    } catch (err) {
      sheet.busy = false;
      sheet.error = writeError(err);
      renderSheet();
    }
  }

  async function quickLog(season, episode) {
    const head = headItem();
    try {
      const data = await getJSON("/api/tracker/add", {
        feature: "history",
        provider_instance: state.instance,
        items: [{ type: "episode", series_title: head.title, series_year: head.year, season, episode, show_ids: { ...head.ids }, watched_at: new Date().toISOString() }],
      });
      await afterWrite(data, T("tracker.toast.logged", { what: `${head.title} ${se(season, episode)}` }));
    } catch (err) {
      toast.show(writeError(err), { tone: "bad" });
    }
  }

  async function rate(value) {
    const data = state.title;
    if (!data) return;
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const existing = data.ratings[0];
      const res = existing
        ? await getJSON("/api/tracker/edit", { feature: "ratings", provider_instance: state.instance, key: existing.key, changes: { rating: value } })
        : await getJSON("/api/tracker/add", { feature: "ratings", provider_instance: state.instance, item: { ...headItem(), rating: value } });
      await afterWrite(res, T("tracker.toast.rated", { title: data.title.title, value: String(value) }));
    } catch (err) {
      sheet.busy = false;
      sheet.error = err?.code === "no_changes" ? T("tracker.rate.same") : writeError(err);
      renderSheet();
    }
  }

  async function unrate() {
    const existing = state.title?.ratings?.[0];
    if (!existing) return;
    sheet.busy = true;
    renderSheet();
    try {
      const res = await getJSON("/api/tracker/delete", { feature: "ratings", provider_instance: state.instance, keys: [existing.key] });
      await afterWrite(res, T("tracker.rate.removed"));
    } catch (err) {
      sheet.busy = false;
      sheet.error = writeError(err);
      renderSheet();
    }
  }

  async function toggleWatchlist() {
    const data = state.title;
    if (!data) return;
    try {
      const listed = data.watchlist[0];
      const res = listed
        ? await getJSON("/api/tracker/delete", { feature: "watchlist", provider_instance: state.instance, keys: [listed.key] })
        : await getJSON("/api/tracker/add", { feature: "watchlist", provider_instance: state.instance, item: headItem() });
      await afterWrite(res, listed ? T("tracker.toast.removedWatchlist", { title: data.title.title }) : T("tracker.toast.addedWatchlist", { title: data.title.title }));
    } catch (err) {
      toast.show(writeError(err), { tone: "bad" });
    }
  }

  async function postRaw(url, body, options = {}) {
    const res = await fetch(url, { method: "POST", cache: "no-store", credentials: "same-origin", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body), signal: options.signal });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error((typeof data?.detail === "string" && data.detail) || data?.error || T("tracker.errors.requestFailed", { status: String(res.status) }));
    return data;
  }

  async function openMapping() {
    const data = state.title;
    const record = titleAnchor(data);
    if (!record) return;
    const count = recordCount(data);
    const perEpisode = ["episode_missing", "episode_unverified"].includes(record.check?.code);
    const feature = record.feature || "history";
    const version = encodeURIComponent(window.APP_VERSION || "1");
    const original = {
      type: record.type, title: record.title, year: record.year, season: record.season, episode: record.episode,
      series_title: record.series_title, series_year: record.series_year, ids: { ...record.ids }, show_ids: { ...record.show_ids },
    };
    Object.keys(original).forEach((k) => (original[k] == null || original[k] === "") && delete original[k]);
    const reference = { provider: "CROSSWATCH", instance: state.instance || "default", pair_id: "", feature, key: record.key, original };
    const request = (action, extra = {}, options = {}) => postRaw("/api/editor/mapping", { ...reference, action, ...extra }, options);
    try {
      if (!document.querySelector('link[href*="/assets/css/interactive-sync.css"]')) {
        const style = document.createElement("link");
        style.rel = "stylesheet";
        style.href = `/assets/css/interactive-sync.css?v=${version}`;
        document.head.append(style);
      }
      const { openMappingWorkspace } = await import(`/assets/js/interactive-sync-mapping.js?v=${version}`);
      const context = await request("catalogs");
      closeSheet();
      return openMappingWorkspace({
        rows: [context.row],
        total: 1, standalone: true, staged: true, direct: true,
        allowMovieConversion: feature === "history" && ["episode", "show"].includes(record.type),
        scope: count > 1 && !perEpisode ? "title" : "record",
        scopes: count > 1
          ? [{ id: "title", label: T("tracker.map.everyRecord", { count }) }, { id: "record", label: data.show ? T("tracker.map.onlyEpisode") : T("tracker.map.onlyRecord") }]
          : [{ id: "record", label: T("tracker.map.thisRecord") }],
        scopeNote: (value) => value === "title"
          ? T("tracker.map.changesAll", { records: plural(count, "record", "records") })
          : T("tracker.map.onlyThis"),
        mappingApi: {
          catalogs: async () => context,
          search: (row, q, catalog, options) => request("search", { q, catalog, ...(row.item.type !== original.type && row.item.type === "movie" ? { search_type: "movie" } : {}) }, options),
          episodes: (edits, options) => request("episodes", { item: edits[0].item }, options),
          parts: (_row, simklId, options) => request("parts", { simkl_id: simklId }, options),
          save: async (edits, scope) => {
            const prepared = await request("prepare", { item: edits[0].item });
            if (scope === "title" && prepared.item.type === original.type) {
              const target = data.show ? { show: data.show_key } : { feature, key: record.key };
              const res = await getJSON("/api/tracker/edit-title", { provider_instance: state.instance, item: prepared.item, ...target });
              state.ref = res.show_key ? { show: res.show_key } : { feature: res.feature, key: res.key };
              await afterWrite(res, T("tracker.map.fixed", { what: plural(res.count, "record", "records") }));
              return prepared;
            }
            const res = await getJSON("/api/tracker/replace", { feature, provider_instance: state.instance, key: record.key, item: prepared.item });
            if (prepared.item.type !== original.type || (state.ref && !state.ref.show)) state.ref = { feature, key: res.key };
            await afterWrite(res, T("tracker.map.fixed", { what: cardTitle(res.item || record) }));
            return prepared;
          },
        },
      });
    } catch (err) {
      toast.show(err?.message || T("tracker.map.openFailed"), { tone: "bad" });
    }
  }

  function findRecord(key, feature = "history") {
    const list = state.title?.[feature === "history" ? "plays" : feature];
    return Array.isArray(list) ? list.find((r) => r.key === key) || null : null;
  }

  function recordCount(data) {
    return ["plays", "ratings", "watchlist", "collection", "progress"].reduce((n, name) => n + (data?.[name]?.length || 0), 0);
  }

  function titleAnchor(data) {
    return data?.attention?.[0] || data?.plays?.[0] || data?.ratings?.[0] || data?.watchlist?.[0] || data?.collection?.[0] || data?.progress?.[0] || null;
  }

  async function openOrigin() {
    const record = titleAnchor(state.title);
    if (!record) return;
    const instance = state.instance;
    openSheet("origin");
    const token = sheet.token;
    try {
      const [{renderItemOrigin}, data] = await Promise.all([
        import(`/assets/js/editor/origin.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`),
        postRaw("/api/editor/origin", {provider:"CROSSWATCH",instance,feature:record.feature || "history",key:record.key,original:record}),
      ]);
      if (sheet.mode !== "origin" || sheet.token !== token || state.instance !== instance) return;
      renderItemOrigin(sheet.el.querySelector("[data-trk-origin]"), data, {snapshot:"CROSSWATCH",instance});
    } catch (err) {
      if (sheet.mode === "origin" && sheet.token === token) sheet.el.querySelector("[data-trk-origin]").textContent = writeError(err);
    }
  }

  function detailAction(action) {
    const data = state.title;
    if (!data) return;
    if (action === "back") {
      state.root.querySelector('[data-trk-slot="filters"]').open = false;
      const selected = state.root.querySelector(".trk-play.is-selected");
      selected?.scrollIntoView({block: "center"});
      selected?.focus({preventScroll: true});
    }
    else if (action === "card") openCard();
    else if (action === "remove") openTitleRemoval([data.note_key]);
    else if (action === "log") logFromTitle();
    else if (action === "rate") openSheet("rate");
    else if (action === "list") toggleWatchlist();
    else if (action === "map") openMapping();
    else if (action === "origin") openOrigin();
    else if (action === "notes") openNotes();
    else if (action === "status-menu") setStatusMenu(!state.statusMenu);
    else if (action === "edit-title") openSheet("title-edit");
    else if (action === "complete") completeNote();
    else if (action === "use-tmdb") {
      const issue = data.attention[0];
      const c = issue?.check || {};
      const changes = {};
      if (c.title) changes.title = c.title;
      if (c.year) changes.year = c.year;
      if (issue && Object.keys(changes).length) {
        getJSON("/api/tracker/edit", { feature: issue.feature, provider_instance: state.instance, key: issue.key, changes })
          .then((res) => afterWrite(res, T("tracker.map.tookTitle", { title: cardTitle(issue) })))
          .catch((err) => toast.show(writeError(err), { tone: "bad" }));
      }
    }
  }

  function handlePanelClick(event) {
    const expand = event.target.closest("[data-trk-expand]");
    if (expand) {
      const kind = expand.dataset.trkExpand;
      const key = kind === "seasons" ? "seasonsExpanded" : "playsExpanded";
      state[key] = !state[key];
      setSlot("title", renderTitle());
      if (sheet.mode === "title") renderSheet();
      const panel = sheet.mode === "title" ? sheet.el : state.root.querySelector('[data-trk-slot="title"]');
      panel?.querySelector(`[data-trk-expand="${kind}"]`)?.focus({preventScroll: state[key]});
      return true;
    }
    const pick = event.target.closest("[data-trk-set-status]");
    if (pick) {
      quickStatus(pick.dataset.trkSetStatus);
      return true;
    }
    const detail = event.target.closest("[data-trk-detail]");
    if (detail) {
      detailAction(detail.dataset.trkDetail);
      return true;
    }
    const season = event.target.closest("[data-trk-season]");
    if (season) {
      const n = Number(season.dataset.trkSeason);
      if (state.seasonOpen.has(n)) state.seasonOpen.delete(n);
      else state.seasonOpen.add(n);
      setSlot("title", renderTitle());
      if (sheet.mode === "title") renderSheet();
      return true;
    }
    const ep = event.target.closest("[data-trk-log-ep]");
    if (ep) {
      const [s, e] = ep.dataset.trkLogEp.split(":").map(Number);
      quickLog(s, e);
      return true;
    }
    const edit = event.target.closest("[data-trk-play-edit]");
    if (edit) {
      const record = findRecord(edit.dataset.trkPlayEdit, "history");
      if (record) openSheet("edit", { item: record, feature: record.feature || "history" });
      return true;
    }
    const recCopies = event.target.closest("[data-trk-rec-copies]");
    if (recCopies) {
      const [feature, ...rest] = recCopies.dataset.trkRecCopies.split("|");
      const record = findRecord(rest.join("|"), feature);
      if (record) void openMyShelf({ ...record, art: record.art || state.title?.title?.art || {} });
      return true;
    }
    const recEdit = event.target.closest("[data-trk-rec-edit]");
    if (recEdit) {
      const [feature, ...rest] = recEdit.dataset.trkRecEdit.split("|");
      const record = findRecord(rest.join("|"), feature);
      if (record) openSheet("edit", { item: record, feature });
      return true;
    }
    const recDelete = event.target.closest("[data-trk-rec-delete]");
    if (recDelete) {
      const [feature, ...rest] = recDelete.dataset.trkRecDelete.split("|");
      openSheet("delete", { keys: [rest.join("|")], feature });
      return true;
    }
    const del = event.target.closest("[data-trk-play-delete]");
    if (del) {
      openSheet("delete", { keys: [del.dataset.trkPlayDelete], feature: "history" });
      return true;
    }
    return false;
  }

  function onSheetClick(event) {
    const instance = event.target.closest('[data-trk-sheet-instance]');
    if (instance) {
      const value = instance.dataset.trkSheetInstance;
      Promise.resolve(closeSheet(false)).then(() => selectInstance(value));
      return;
    }
    const saved = sheet.mode === "views" ? event.target.closest("[data-trk-saved-view]") : null;
    if (saved) {
      const index = Number(saved.dataset.trkSavedView);
      const view = sheet.views[index];
      if (!view) return;
      try {
        if (saved.dataset.remove === "true") {
          sheet.views.splice(index, 1);
          localStorage.setItem("cw.tracker.saved-views.v1", JSON.stringify(sheet.views));
          renderSheet();
        } else {
          localStorage.setItem(STORE_KEY, JSON.stringify({...view.settings,viewSelectedAt:Date.now(),monthlyReview:false,offset:0,ref:null,scrollTop:0}));
          state.ref = null; state.title = null; state.picked.clear();
          loadView(); closeSheet(); refresh();
        }
      } catch (err) { sheet.error = T("tracker.views.saveFailed"); renderSheet(); }
      return;
    }
    const undoBtn = event.target.closest("[data-trk-undo]");
    if (undoBtn) {
      sheet.busy = true;
      renderSheet();
      undo(undoBtn.dataset.trkUndo);
      return;
    }
    if (sheet.mode === "title" && handlePanelClick(event)) {
      event.stopPropagation();
      return;
    }
    const changeKind = sheet.mode === "changes" ? event.target.closest("[data-trk-change-kind]") : null;
    if (changeKind) {
      if (changeKind.dataset.trkChangeKind !== sheet.changeKind) {
        sheet.changeKind = changeKind.dataset.trkChangeKind;
        loadChanges(false);
      }
      return;
    }
    const rateBtn = event.target.closest("[data-trk-rate]");
    if (rateBtn) {
      if (sheet.busy || rateBtn.disabled) return;
      const input = sheet.mode === "edit" ? sheet.el.querySelector('[data-trk-edit="rating"]') : null;
      if (input) {
        input.value = rateBtn.dataset.trkRate;
        input.dispatchEvent(new Event("input", {bubbles:true}));
      } else rate(Number(rateBtn.dataset.trkRate));
      return;
    }
    if (sheet.mode === "notes") {
      const status = event.target.closest("[data-trk-note-status]");
      if (status) {
        sheet.draft.status_manual = true;
        sheet.draft.status = status.dataset.trkNoteStatus;
        if (sheet.draft.status === "completed" && !sheet.draft.finished) sheet.draft.finished = today();
        if (sheet.draft.status === "watching" && !sheet.draft.started) sheet.draft.started = today();
        renderSheet();
        return;
      }
      const untag = event.target.closest("[data-trk-note-untag]");
      if (untag) {
        sheet.draft.tags.splice(Number(untag.dataset.trkNoteUntag), 1);
        renderSheet();
        sheet.el.querySelector("[data-trk-note-tag]")?.focus();
        return;
      }
    }
    const removeDef = sheet.mode === "fields" ? event.target.closest("[data-trk-def-remove]") : null;
    if (removeDef) {
      sheet.defs.splice(Number(removeDef.dataset.trkDefRemove), 1);
      renderSheet();
      return;
    }
    const feat = sheet.mode === "import" ? event.target.closest("[data-trk-imp-feature]") : null;
    if (feat) {
      const id = feat.dataset.trkImpFeature;
      if (sheet.imp.features.has(id)) sheet.imp.features.delete(id);
      else sheet.imp.features.add(id);
      renderSheet();
      return;
    }
    const log = sheet.log;
    if (sheet.mode === "log" && log) {
      if (sheet.busy) return;
      if (event.target.closest("[data-trk-log-back]")) {
        if (log.review) log.review = null;
        else log.step = log.step === "dates" ? "episodes" : "action";
        sheet.error = ""; renderSheet(true); return;
      }
      if (event.target.closest("[data-trk-log-reload]")) { sheet.error = ""; loadSeasons(log); return; }
      if (event.target.closest("[data-trk-log-advanced]")) {
        log.advanced = !log.advanced; log.review = null; log.step = "action"; log.action = null; log.expanded = new Set(); sheet.error = "";
        if (!log.advanced && log.whenMode === "range") log.whenMode = "today";
        renderSheet(); return;
      }
      const operation = event.target.closest("[data-trk-log-operation]");
      if (operation) { log.action = operation.dataset.trkLogOperation; log.selected.clear(); log.review = null; sheet.error = ""; renderSheet(); return; }
      const select = event.target.closest("[data-trk-log-select]");
      if (select) {
        const eligible = (select.dataset.trkLogSelect === "all" ? log.seasons.flatMap(s => watchSeasonRows(log, s.season)) : watchSeasonRows(log, Number(select.dataset.trkLogSelect))).filter(ep => watchEligible(log, ep));
        const all = eligible.every(ep => log.selected.has(`${ep.season}:${ep.episode}`));
        eligible.forEach(ep => all ? log.selected.delete(`${ep.season}:${ep.episode}`) : log.selected.add(`${ep.season}:${ep.episode}`));
        renderSheet(); return;
      }
      const expand = event.target.closest("[data-trk-log-expand]");
      if (expand) {
        const season = Number(expand.dataset.trkLogExpand);
        log.expanded ||= new Set();
        if (log.expanded.has(season)) log.expanded.delete(season);
        else { log.expanded.add(season); loadWatchSeason(log, season); }
        renderSheet(); return;
      }
      const type = event.target.closest("[data-trk-log-type]");
      if (type) {
        log.type = type.dataset.trkLogType;
        log.results = [];
        renderSheet();
        searchLog();
        return;
      }
      const info = event.target.closest("[data-trk-log-info]");
      if (info) {
        previewCard(logCardItem(log, log.results[Number(info.dataset.trkLogInfo)]));
        return;
      }
      const pick = event.target.closest("[data-trk-log-pick]");
      if (pick) {
        pickLog(Number(pick.dataset.trkLogPick));
        return;
      }
      const dateMode = event.target.closest("[data-trk-log-date-mode]");
      if (dateMode) {
        log.whenMode = dateMode.dataset.trkLogDateMode;
        sheet.error = "";
        renderSheet();
        return;
      }
      const target = event.target.closest("[data-trk-log-target]");
      if (target) {
        log.review = null;
        log.target = target.dataset.trkLogTarget;
        renderSheet();
        return;
      }
      if (event.target.closest("[data-trk-log-repick]")) {
        log.pick = null;
        renderSheet(true);
        return;
      }
    }
    const action = event.target.closest("[data-trk-sheet]")?.dataset.trkSheet;
    if (!action) return;
    if (action === "close") closeSheet();
    else if (action === "remove-history") loadTitleRemoval("history");
    else if (action === "remove-all") loadTitleRemoval("all");
    else if (action === "remove-confirm") confirmTitleRemoval();
    else if (action === "review-refresh") loadReview();
    else if (action === "review-start" || action === "review-cancel") reviewAction(action);
    else if (action === "enable-auto") enableTitleAutomation();
    else if (action === "complete") completeNote();
    else if (action === "delete") deleteKeys(sheet.keys, sheet.feature);
    else if (action === "unrate") unrate();
    else if (action === "more-changes") loadChanges(true);
    else if (action === "fields") openFields();
    else if (action === "fields-back") openSheet("notes", { draft: sheet.draft });
    else if (action === "note-clear" && !sheet.busy) saveNotes(true);
    else if (action === "def-add" && sheet.defs.length < MAX_FIELDS) {
      sheet.defs.push({ id: "", name: "", type: "text", choicesText: "" });
      renderSheet();
      sheet.el.querySelectorAll('[data-trk-def$=":name"]')[sheet.defs.length - 1]?.focus();
    }
  }

  function noteInput(target) {
    const d = sheet.draft;
    if (target.matches("[data-trk-note]")) d[target.dataset.trkNote] = target.value;
    else if (target.matches("[data-trk-note-field]")) {
      const id = target.dataset.trkNoteField;
      const value = target.type === "checkbox" ? target.checked : target.value;
      if (value === "" || value === false) delete d.fields[id];
      else d.fields[id] = value;
    } else if (target.matches("[data-trk-note-tag]")) {
      d.tagInput = target.value;
      if (target.value.includes(",")) {
        addTags(d, target.value);
        d.tagInput = "";
        renderSheet();
      }
    }
  }

  function defInput(target, rerender) {
    const [index, prop] = String(target.dataset.trkDef || "").split(":");
    const def = sheet.defs[Number(index)];
    if (!def || !prop) return;
    def[prop] = target.value;
    if (rerender) renderSheet();
  }

  function onSheetInput(event) {
    if (sheet.mode === "title-edit") {
      const save = sheet.el.querySelector('[data-trk-form="title-edit"] button[type="submit"]');
      if (save) save.disabled = sheet.busy || !Object.keys(titleEditChanges()).length;
    }
    if (sheet.mode === "changes" && event.target.matches("[data-trk-change-q]")) {
      sheet.changeQ = event.target.value;
      clearTimeout(changeSearchTimer);
      changeSearchTimer = setTimeout(() => loadChanges(false), 250);
      return;
    }
    if (sheet.mode === "notes") noteInput(event.target);
    if (sheet.mode === "fields" && event.target.matches("input[data-trk-def]")) defInput(event.target, false);
    if (sheet.mode === "import" && event.target.matches('[data-trk-imp="name"]')) sheet.imp.name = event.target.value;
    if (sheet.mode === "log" && event.target.matches("[data-trk-log-q]")) {
      sheet.log.q = event.target.value;
      searchLog();
    }
  }

  function onSheetChange(event) {
    if (event.target.dataset?.trkEdit === "status") sheet.statusTouched = true;
    if (sheet.mode === "notes" && event.target.matches("select[data-trk-note-field], input[type=checkbox][data-trk-note-field]")) {
      noteInput(event.target);
      return;
    }
    if (sheet.mode === "fields" && event.target.matches("select[data-trk-def]")) {
      defInput(event.target, true);
      return;
    }
    const field = sheet.mode === "import" ? event.target.dataset?.trkImp : "";
    if (field === "review") {
      sheet.imp.review = event.target.checked;
      renderSheet();
      return;
    }
    if (field === "dropped") {
      sheet.imp.dropped = event.target.checked;
      return;
    }
    if (field === "source" || field === "target" || field === "mode") {
      if (sheet.busy) return;
      if (field === "source") setImportSource(sheet.imp, event.target.value);
      else if (field === "mode") setImportMode(sheet.imp, event.target.value);
      else sheet.imp.target = event.target.value;
      renderSheet();
      return;
    }
    const log = sheet.log;
    if (sheet.mode !== "log" || !log || sheet.busy) return;
    if (event.target.matches("[data-trk-watch-pick]")) {
      const key = event.target.dataset.trkWatchPick;
      if (event.target.checked) log.selected.add(key); else log.selected.delete(key);
      renderSheet();
    } else if (event.target.matches("[data-trk-log-scope]")) { log.scope = event.target.value; renderSheet(); }
    else if (event.target.matches("[data-trk-log-end]")) log.end = event.target.value;
    else if (event.target.matches("[data-trk-log-season]")) {
      log.season = Number(event.target.value);
      log.episode = String(watchSeasonRows(log).find(ep => !episodePlays(log, ep.season, ep.episode).length)?.episode || 1);
      loadWatchSeason(log);
      renderSheet();
    } else if (event.target.matches("[data-trk-log-episode]")) { log.episode = event.target.value; renderSheet(); }
    else if (event.target.matches("[data-trk-log-when]")) log.when = event.target.value;
    else if (event.target.matches("[data-trk-log-unlist]")) log.unlist = event.target.checked;
  }

  function onSheetSubmit(event) {
    event.preventDefault();
    if (sheet.busy) return;
    if (sheet.mode === "edit") saveEdit();
    else if (sheet.mode === "log") saveLog();
    else if (sheet.mode === "import") startImport();
    else if (sheet.mode === "notes") saveNotes();
    else if (sheet.mode === "notes-bulk") saveNotesBulk();
    else if (sheet.mode === "views") saveNamedView();
    else if (sheet.mode === "fields") saveFields();
    else if (sheet.mode === "title-edit") saveTitleEdit();
  }

  function onSheetKey(event) {
    if (sheet.mode === "notes" && event.target.matches?.("[data-trk-note-tag]")) {
      const d = sheet.draft;
      if (event.key === "Enter") {
        event.preventDefault();
        if (!event.target.value.trim()) return;
        addTags(d, event.target.value);
        d.tagInput = "";
        renderSheet();
        return;
      }
      if (event.key === "Backspace" && !event.target.value && d.tags.length) {
        d.tags.pop();
        renderSheet();
        return;
      }
    }
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      closeSheet();
      return;
    }
    if (event.key !== "Tab") return;
    const dialog = sheet.el.querySelector(".trk-sheet");
    const nodes = Array.from(dialog.querySelectorAll("button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), a[href], summary, [tabindex]:not([tabindex='-1'])")).filter((n) => n.offsetParent !== null);
    if (!nodes.length) return;
    const first = nodes[0];
    const last = nodes[nodes.length - 1];
    if (event.shiftKey && (document.activeElement === first || document.activeElement === dialog)) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  function rowRef(tr) {
    if (tr.dataset.trkRefNote) return { note: tr.dataset.trkRefNote };
    if (tr.dataset.trkRefShow) return { show: tr.dataset.trkRefShow };
    return { feature: listFeature(), key: tr.dataset.trkRefKey };
  }

  let searchTimer = 0;
  let pickRequest = 0;

  function selectableItems() {
    return state.view === "titles" ? state.noteRows : state.view === "shows" ? state.shows : state.items;
  }

  async function pickAllResults() {
    if (state.loading || state.picking) return;
    const restoreFocus = document.activeElement?.dataset?.trkAction === "pick-all";
    const id = ++pickRequest;
    const request = state.request;
    state.picking = true;
    renderSlots(false);
    const keys = new Set();
    const historyTitleKeys = {};
    try {
      for (let offset = 0; ; offset += 200) {
        const params = new URLSearchParams({ feature: listFeature(), provider_instance: state.instance, q: state.q,
          media: state.view === "cleanup" ? "" : state.media, sort: state.view === "cleanup" ? "title" : state.sort,
          offset: String(offset), limit: "200" });
        if (state.view === "cleanup" && state.healthFilter) params.set("health", state.healthFilter);
        if (timelineOn() && state.month) params.set("month", state.month);
        const notes = state.view === "titles";
        const shows = state.view === "shows";
        if (["movies", "watched"].includes(state.view)) params.set("media", state.historyMedia);
        if (shows && state.hideDropped) params.set("hide_dropped", "true");
        params.set("duplicates", String(state.view === "history" && state.duplicates));
        params.set("status", state.noteStatus);
        params.set("tag", state.noteTag);
        const data = await getJSON(`/api/tracker/${notes ? "notes" : shows ? "shows" : ["movies", "watched"].includes(state.view) ? "history/titles" : "items"}?${params}`);
        if (id !== pickRequest || request !== state.request) return;
        const rows = (notes ? data.notes : shows ? data.shows : data.items) || [];
        for (const item of rows) {
          keys.add(item.key);
          if (["movies", "watched"].includes(state.view)) historyTitleKeys[item.key] = item.title_key;
        }
        if (!rows.length || offset + rows.length >= Number(data.total)) break;
      }
      state.picked = keys;
      state.historyTitleKeys = historyTitleKeys;
    } catch (err) {
      if (id === pickRequest && request === state.request) toast.show(errorText(err), { tone: "bad" });
    } finally {
      if (id === pickRequest) {
        state.picking = false;
        renderSlots(false);
        if (restoreFocus && document.activeElement === document.body) state.root.querySelector('[data-trk-action="pick-all"]')?.focus();
      }
    }
  }

  function wire(root) {
    if (state.wired) return;
    state.wired = true;
    window.addEventListener('popstate', () => {
      const wanted = history.state?.cwTrackerView;
      if (trackerVisible() && wanted && wanted !== mobileHistoryView && VIEWS.some(view => view.id === wanted)) changeMobileView(wanted, true);
    });
    new MutationObserver(() => { if (root.classList.contains("hidden") || root.hidden) { stopOverview(); closeSheet(false); } }).observe(root, {attributes:true, attributeFilter:["class", "hidden"]});
    wireStatusMenu();
    document.addEventListener("click", (event) => {
      root.querySelectorAll(".trk-nav-menu[open]").forEach(menu => { if (!menu.contains(event.target)) menu.open = false; });
    }, true);
    root.addEventListener("focusout", (event) => {
      const menu = event.target.closest?.(".trk-nav-menu");
      if (event.relatedTarget && menu && !menu.contains(event.relatedTarget)) menu.open = false;
    });
    root.addEventListener("click", (event) => {
      if (sheet.el?.contains(event.target) && sheet.mode !== 'filters') return;
      if (mobile() && event.target.closest('.trk-source > summary')) {
        event.preventDefault();
        openSheet('instances');
        return;
      }
      const instance = event.target.closest("[data-trk-instance]");
      if (instance) {
        const menu = instance.closest("details");
        menu.open = false;
        menu.querySelector("summary").focus({preventScroll:true});
        selectInstance(instance.dataset.trkInstance);
        return;
      }
      const wrappedDismiss = event.target.closest("[data-trk-wrapped-dismiss]");
      if (wrappedDismiss) {
        const key = reviewDismissKey(true), year = wrappedDismiss.dataset.trkWrappedDismiss;
        reviewDismissals.set(key,year);
        try {localStorage.setItem(key,year);} catch {}
        renderOverviewSlot();
        root.querySelector('.trk-tabs [aria-selected="true"]')?.focus({preventScroll:true});
        return;
      }
      const wrappedEntry = event.target.closest("[data-trk-wrapped],[data-trk-wrapped-open]");
      if (wrappedEntry) {openYearlyReview(wrappedEntry,wrappedEntry.dataset.trkWrapped || ""); return;}
      const monthDismiss = event.target.closest("[data-trk-month-dismiss]");
      if (monthDismiss) {
        const key = reviewDismissKey(), month = monthDismiss.dataset.trkMonthDismiss;
        reviewDismissals.set(key,month);
        try {localStorage.setItem(key,month);} catch {}
        monthDismiss.closest(".trk-month-banner")?.remove();
        root.querySelector('.trk-tabs [aria-selected="true"]')?.focus({preventScroll:true});
        return;
      }
      const monthEntry = event.target.closest("[data-trk-month],[data-trk-month-open]");
      if (monthEntry) {openMonthlyReview(monthEntry,monthEntry.dataset.trkMonth || ""); return;}
      if (event.target.closest("[data-trk-pick],.trk-grid-select")) return;
      if (handlePanelClick(event)) return;
      const calendarAction = event.target.closest("[data-trk-calendar]");
      if (calendarAction) {
        const calendar = root.querySelector(".trk-calendar");
        const action = calendarAction.dataset.trkCalendar;
        calendar?.scrollTo({left:action === "today" ? 0 : calendar.scrollLeft + calendar.clientWidth * (action === "next" ? .8 : -.8), behavior:window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth"});
        return;
      }
      const overviewMore = event.target.closest("[data-trk-overview-more]");
      if (overviewMore) {
        const kind = overviewMore.dataset.trkOverviewMore, step = overviewStep(kind);
        state.overviewLimits[kind] = state.overviewLimits[kind] * step >= overviewRows(kind).length ? 1 : state.overviewLimits[kind] + 1;
        renderOverviewSlot();
        return;
      }
      const cardMenu = event.target.closest("[data-trk-card-menu]");
      if (cardMenu) { openGridActions(cardMenu); return; }
      const cardOpen = event.target.closest("[data-trk-card-open]");
      if (cardOpen) {
        const {ref, recent} = gridCardContext(cardOpen);
        if (ref) selectRef(ref, {openMedia:recent || mobile()});
        return;
      }
      const overviewCard = event.target.closest("[data-trk-overview-open]");
      if (overviewCard) { const [kind, index] = overviewCard.dataset.trkOverviewOpen.split(":"); overviewOpen(kind, Number(index)); return; }
      const overviewMenu = event.target.closest("[data-trk-overview-menu]");
      if (overviewMenu) { openOverviewActions(overviewMenu); return; }
      const layout = event.target.closest("[data-trk-layout]");
      if (layout) {
        const wanted = layout.dataset.trkLayout;
        const wasShelf = state.layouts[layoutView()] === "shelf";
        state.layouts[layoutView()] = wanted === "grid" || (["covers", "shelf"].includes(wanted) && state.view === "collection") ? wanted : "list";
        saveView();
        if (state.view === "collection" && wasShelf !== (state.layouts.collection === "shelf")) reload();
        else renderSlots(true);
        return;
      }
      const historyMedia = event.target.closest("[data-trk-history-media]");
      if (historyMedia) {
        if (historyMedia.dataset.trkHistoryMedia === state.historyMedia) return;
        state.historyMedia = historyMedia.dataset.trkHistoryMedia;
        state.media = state.historyMedia;
        state.view = preferredHistoryView();
        rememberView();
        state.ref = null; state.title = null;
        reload();
        state.root.querySelector(`[data-trk-history-media="${state.historyMedia}"]`)?.focus();
        return;
      }
      const view = event.target.closest("[data-trk-view]");
      if (view) {
        root.querySelector(".trk-others")?.removeAttribute("open");
        const wanted = view.dataset.trkView === "history" && view.closest(".trk-navigation") ? preferredHistoryView() : view.dataset.trkView;
        if (wanted === state.view) return;
        if (changeMobileView(wanted)) return;
        state.view = wanted;
        if (wanted === "history") state.media = state.historyMedia;
        rememberView();
        state.filtersOpen = false;
        state.q = "";
        if (state.view === "cleanup" && !state.healthFilter) state.healthFilter = "attention";
        reload();
        return;
      }
      const media = event.target.closest("[data-trk-media]");
      if (media) {
        if (media.dataset.trkMedia === state.media) return;
        state.media = media.dataset.trkMedia;
        reload();
        return;
      }
      const noteFilter = event.target.closest("[data-trk-note-filter]");
      if (noteFilter) {
        if (noteFilter.dataset.trkNoteFilter === state.noteStatus) return;
        state.noteStatus = noteFilter.dataset.trkNoteFilter;
        reload();
        return;
      }
      const health = event.target.closest("[data-trk-health]");
      if (health) {
        state.healthFilter = health.dataset.trkHealth;
        state.view = "cleanup";
        rememberView();
        reload();
        return;
      }
      const fold = event.target.closest("[data-trk-fold]");
      if (fold) {
        const key = fold.dataset.trkFold;
        if (state.open.has(key)) state.open.delete(key);
        else state.open.add(key);
        setSlot("main", renderMain());
        return;
      }
      const rowEdit = event.target.closest("[data-trk-row-edit]");
      if (rowEdit) {
        const item = state.items[Number(rowEdit.dataset.trkRowEdit)];
        if (item) openSheet("edit", { item, feature: listFeature() });
        return;
      }
      const row = event.target.closest(".trk-play");
      if (row) {
        selectRef(rowRef(row));
        return;
      }
      const action = event.target.closest("[data-trk-action]")?.dataset.trkAction;
      if (['views','changes','import'].includes(action)) root.querySelector('.trk-others')?.removeAttribute('open');
      if (action === "show-playback") {
        dismissedPlayback = null;
        updateHeroPlayback();
      } else if (action === "refresh") {
        state.progress = {};
        refresh();
      } else if (action === "prev" || action === "next") {
        state.offset = Math.max(0, state.offset + (action === "next" ? pageSize() : -pageSize()));
        reload(false);
      } else if (action === "more" && !state.loading) loadMain(true);
      else if (action === "changes") {
        openSheet("changes", { changes: [], changesTotal: 0, changesAll: 0, changeKind: "", changeQ: "" });
        loadChanges(false);
      } else if (action === "log") openLog();
      else if (action === "quick-add" && typeof window.openManualWatchedModal === "function") {
        const feature = { history: "history", watchlist: "watchlist", ratings: "rating", collection: "collection" }[state.view];
        window.openManualWatchedModal(feature ? { action: feature } : {});
      }
      else if (action === "views") openSheet("views", {views:readSavedViews()});
      else if (action === "import") openImport();
      else if (action === "month-clear") {
        state.month = "";
        renderSlots(true);
        loadMain();
      } else if (action === "by-day") {
        state.byDay = !state.byDay;
        if (state.byDay) state.sort = "recent";
        reload();
      }
      else if (action === "import-cancel") cancelImport();
      else if (action === "import-dismiss") dismissImport();
      else if (action === "import-check") {
        dismissImport();
        state.view = "cleanup";
        rememberView();
        state.healthFilter = "attention";
        startCheck();
        reload();
      }
      else if (action === "automation") openAutomation();
      else if (action === "check") startCheck();
      else if (action === "check-cancel") cancelCheck();
      else if (action === "delete-picked" && state.picked.size) {
        if (state.view === "shows") openTitleRemoval(Array.from(state.picked, key => `show:${key}`));
        else if (["movies", "watched"].includes(state.view)) {
          const titles = {...state.historyTitleKeys, ...Object.fromEntries(state.items.map(item => [item.key, item.title_key]))};
          openTitleRemoval(Array.from(state.picked, key => titles[key]));
        }
        else if (state.view === "titles") openSheet("notes-bulk", { keys: Array.from(state.picked) });
        else openSheet("delete", { keys: Array.from(state.picked), feature: listFeature() });
      }
      else if (action === "pick-visible") {
        selectableItems().forEach(item => state.picked.add(item.key));
        renderSlots(false);
      }
      else if (action === "filters") {
        if (window.CW?.MobileUI?.mobile()) {
          state.filtersOpen = true;
          openSheet('filters');
          return;
        }
        state.filtersOpen = !state.filtersOpen;
        renderSlots(true);
        state.root.querySelector('[data-trk-action="filters"]')?.focus();
      }
      else if (action === "filters-clear") {
        state.noteStatus = ""; state.noteTag = ""; state.q = "";
        state.media = "";
        state.showStatusLabels = true;
        if (historyView()) {
          state.historyMedia = ""; state.historyGroup = "watch"; state.view = "history";
          state.ref = null; state.title = null;
          rememberView();
        }
        state.sort = "recent"; state.hideDropped = false; state.duplicates = false; state.month = ""; state.healthFilter = "";
        reload();
      }
      else if (action === "pick-all") pickAllResults();
      else if (action === "clear-picked") {
        pickRequest += 1;
        state.picking = false;
        state.picked.clear();
        renderSlots(false);
        root.querySelector('[data-trk-pick]')?.focus();
      }
    });
    root.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && state.filtersOpen && event.target.closest?.("#trk-extra-filters,.trk-search-wrap")) {
        event.preventDefault();
        state.filtersOpen = false;
        renderSlots(true);
        root.querySelector('[data-trk-action="filters"]')?.focus();
        return;
      }
      const others = event.target.closest?.(".trk-nav-menu");
      if (mobile() && others?.classList.contains('trk-source') && ['ArrowDown', 'ArrowUp'].includes(event.key)) {
        event.preventDefault();
        openSheet('instances');
        return;
      }
      if (others && event.key === "Escape") {
        event.preventDefault();
        others.removeAttribute("open");
        others.querySelector("summary").focus();
        return;
      }
      if (others && ["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
        event.preventDefault();
        others.open = true;
        const items = [...others.querySelectorAll("button")].filter(button => !button.disabled && button.getClientRects().length);
        const at = items.indexOf(document.activeElement);
        const next = event.key === "Home" ? 0 : event.key === "End" || at < 0 && event.key === "ArrowUp" ? items.length - 1 : (at + (event.key === "ArrowUp" ? -1 : 1) + items.length) % items.length;
        items[next]?.focus();
        return;
      }
      const tab = event.target.closest?.('.trk-tabs [role="tab"]');
      if (tab && ["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
        const tabs = Array.from(tab.parentElement.querySelectorAll('[role="tab"]')).filter(tab => !tab.hidden);
        const at = tabs.indexOf(tab);
        const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (at + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
        event.preventDefault();
        tabs[next].click();
        state.root.querySelector(`.trk-tabs [data-trk-view="${tabs[next].dataset.trkView}"]`)?.focus();
        return;
      }
      const row = event.target.closest?.(".trk-play");
      if (!row || (event.key !== "Enter" && event.key !== " ") || event.target.closest("input, button")) return;
      event.preventDefault();
      if (row.dataset.trkOverviewOpen) {
        const [kind, index] = row.dataset.trkOverviewOpen.split(":");
        overviewOpen(kind, Number(index));
      } else selectRef(rowRef(row));
    });
    root.addEventListener("change", (event) => {
      const pick = event.target.dataset?.trkPick;
      if (pick !== undefined) {
        if (pick === "page") selectableItems().forEach((i) => (event.target.checked ? state.picked.add(i.key) : state.picked.delete(i.key)));
        else {
          const item = selectableItems()[Number(pick)];
          if (item && event.target.checked) state.picked.add(item.key);
          else if (item) state.picked.delete(item.key);
        }
        renderSlots(true);
        return;
      }
      const input = event.target.dataset?.trkInput;
      if (input === "history-group") {
        state.historyGroup = event.target.value === "title" ? "title" : "watch";
        state.media = state.historyMedia;
        state.view = preferredHistoryView();
        rememberView();
        state.ref = null; state.title = null;
        reload();
        state.root.querySelector('[data-trk-input="history-group"]')?.focus();
      } else if (input === "size") {
        const value = Number(event.target.value);
        if (mobile()) state.mobilePageSize = MOBILE_PAGE_SIZES.includes(value) ? value : 20;
        else state.pageSize = PAGE_SIZES.includes(value) ? value : 50;
        reload();
      } else if (input === "note-status") {
        state.noteStatus = event.target.value;
        if (state.noteStatus) state.hideDropped = false;
        reload();
      } else if (input === "hide-dropped") {
        state.hideDropped = event.target.checked;
        if (state.hideDropped) state.noteStatus = "";
        saveView();
        reload();
      } else if (input === "status-labels") {
        state.showStatusLabels = event.target.checked;
        saveView();
        renderSlots(true);
      } else if (input === "duplicates") {
        state.duplicates = event.target.checked;
        reload();
      } else if (input === "sort") {
        state.sort = ["recent", "oldest", "title"].includes(event.target.value) ? event.target.value : "recent";
        if (state.sort !== "recent") state.byDay = false;
        reload();
      } else if (input === "note-tag") {
        state.noteTag = event.target.value;
        reload();
      } else if (input === "clean-feature") {
        state.cleanFeature = event.target.value;
        reload();
      } else if (input === "instance") {
        state.instance = event.target.value;
        state.ref = null;
        state.title = null;
        state.progress = {};
        state.picked.clear();
        refresh();
      }
    });
    root.addEventListener("input", (event) => {
      if (event.target.dataset?.trkInput !== "q") return;
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => {
        state.q = event.target.value;
        reload();
      }, 250);
    });
    root.addEventListener("error", (event) => {
      const img = event.target;
      if (!(img instanceof HTMLImageElement)) return;
      if (img.closest(".trk-overview-art,.trk-calendar-release")) { img.hidden = true; return; }
      const cover = img.closest("[data-trk-log-cover],.trk-watch-editor .cw-ia-sheet-poster");
      if (cover) {
        cover.innerHTML = symbol(cover.dataset.trkLogCover === "movie" ? "movie" : "tv");
        return;
      }
      if (img.closest(".trk-grid-art,.trk-list-art")) { img.remove(); return; }
      if (img.classList.contains("trk-episode-backdrop")) { img.remove(); return; }
      if (!img.closest(".trk-poster")) return;
      img.closest(".trk-poster").classList.add("is-empty");
      img.remove();
    }, true);
    document.addEventListener('keydown', event => {
      if (event.key !== 'Escape' || !root.getClientRects().length || state.statusMenu) return;
      if ([...document.querySelectorAll('[role="dialog"],[role="menu"],dialog[open],.cw-icon-select-menu')].some(node => !node.closest('details:not([open])') && node.getClientRects().length && getComputedStyle(node).visibility !== 'hidden')) return;
      queueMicrotask(() => {
        if (event.defaultPrevented || event.cancelBubble) return;
        if (state.ref) clearSelectedTitle();
        else if (heroPlayback && !root.querySelector('.trk-hero-playback')?.hidden) {
          dismissedPlayback = heroPlayback.session;
          updateHeroPlayback();
        }
      });
    }, true);
    setInterval(refreshHeroPlayback, 15000);
    window.addEventListener('currently-watching-updated', refreshHeroPlayback);
    document.addEventListener('visibilitychange', refreshHeroPlayback);
    let positionTimer = 0;
    window.addEventListener("scroll", () => {
      if (root.classList.contains("hidden")) return;
      state.scrollTop = window.scrollY;
      clearTimeout(positionTimer);
      positionTimer = setTimeout(saveView, 200);
    }, {passive:true});
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) return;
      if (state.moving) waitForMove();
      else {
        pollCheck();
        pollImport();
      }
    });
    let overviewChecking = false;
    const checkOverview = async () => {
      if (overviewChecking || document.hidden || root.hidden || root.classList.contains("hidden") || state.view !== "overview" || state.overview?.checking || state.overview?.revision == null) return;
      overviewChecking = true;
      const instance = state.instance;
      try {
        const info = await getJSON(`/api/tracker/overview/revision?${inst()}`);
        if (state.view === "overview" && instance === state.instance && (info.revision !== state.overview?.revision || state.overview?.day !== today())) await refresh();
        else if (state.view === "overview" && instance === state.instance && ((info.review_month || "") !== (state.overview?.review_month || "") || JSON.stringify(info.wrapped || {}) !== JSON.stringify(state.overview?.wrapped || {}))) {
          state.overview.review_month = info.review_month || "";
          state.overview.wrapped = info.wrapped || {};
          rememberOverview();
          renderOverviewSlot();
        }
      } catch {} finally { overviewChecking = false; }
    };
    window.addEventListener("cw:month-theme-changed",paintMonthlyBanner);
    window.addEventListener("cw:auth-state-changed", renderProfilePicker);
    window.addEventListener("cw:overview-profile-changed", () => {
      if (profileScope !== null && profileScope !== currentProfileScope() && !state.root?.classList.contains('hidden')) {
        window.location.reload();
        return;
      }
      renderProfilePicker();
    });
    window.addEventListener("storage",event => {
      if (event.storageArea === localStorage && (event.key === null || [reviewDismissKey(),reviewDismissKey(true)].includes(event.key))) {
        if (event.key === null) reviewDismissals.clear();
        else reviewDismissals.delete(event.key);
        renderOverviewSlot();
      }
      paintMonthlyBanner();
    });
    setInterval(checkOverview, 60000);
    document.addEventListener("visibilitychange", checkOverview);
    window.addEventListener("cw:tracker-migrated", () => {
      if (state.moving) refresh();
    });
    document.addEventListener("click", (event) => {
      const button = event.target?.closest?.("[data-trk-media-shelf]");
      if (!button) return;
      const select = button.closest("form")?.querySelector('[data-trk-mi="format"]');
      if (select) select.value = button.dataset.trkMediaShelf;
      button.remove();
    });
  }

  function readLink() {
    const hash = window.location.hash || "";
    if (!hash.startsWith("#tracker?")) return;
    const query = new URLSearchParams(hash.slice(hash.indexOf("?") + 1));
    if (query.get("view") === "overview") {
      state.viewSelectedAt = 0;
      expireView();
      monthlyReviewPage?.close(false);
      wrappedPage?.close(false);
      rememberView();
    }
    if (query.get("instance")) {
      state.instance = query.get("instance");
      state.ref = null; state.title = null; state.offset = 0; state.scrollTop = 0;
    }
    if (query.get("attention") === "1") {
      state.monthlyReview = false;
      state.view = "cleanup";
      rememberView();
      state.healthFilter = "attention";
      state.offset = 0;
      state.fromLink = true;
    }
    const month = query.get("month"), year = query.get("year");
    try {
      window.history.replaceState(window.history.state, "", `${window.location.pathname}${window.location.search}#tracker`);
    } catch {}
    return {month:/^\d{4}-(0[1-9]|1[0-2])$/.test(month || "") ? month : "",year:/^\d{4}$/.test(year || "") ? year : ""};
  }

  async function mount(root) {
    if (!root) return;
    window.CW?.ViewCache?.bindRefresh(root, url => new URL(url, location.origin).pathname.startsWith("/api/tracker/"));
    if (state.root !== root) {
      state.root = root;
      state.wired = false;
      sheet.el = null;
      loadView();
    }
    if (expireView()) {
      monthlyReviewPage?.close(false);
      wrappedPage?.close(false);
    }
    const {month:reviewMonth, year:reviewYear} = readLink() || {};
    wire(root);
    try {
      await window.__cwAuthBootstrapPromise;
      await window.CW?.OverviewProfile?.ready;
    } catch {}
    if (profileScope !== null && profileScope !== currentProfileScope()) {
      window.location.reload();
      return;
    }
    profileScope = currentProfileScope();
    const scopedInstance = window.CW?.OverviewProfile?.instanceForProvider?.('CROSSWATCH');
    if (scopedInstance && scopedInstance !== state.instance) {
      state.instance = scopedInstance;
      state.ref = null;
      state.offset = 0;
      state.scrollTop = 0;
    }
    if (state.view === "overview" && !state.overview) restoreOverview();
    if (state.view !== "overview") restoreList();
    render();
    updateHeroSelection();
    updateHeroBackdrop();
    void refreshHeroPlayback();
    await refresh();
    if (reviewYear) openYearlyReview({},reviewYear);
    else if (reviewMonth || state.monthlyReview) openMonthlyReview({},reviewMonth || '',!!reviewMonth);
  }

  const refreshCachedView = (url, data) => {
    const parsed = new URL(url, location.origin);
    if (!trackerVisible() || !parsed.pathname.startsWith("/api/tracker/") || parsed.searchParams.get("provider_instance") !== state.instance) return;
    if (parsed.pathname === "/api/tracker/overview/shows") {
      if (state.view !== "overview" || !overviewController) return;
      if (data) applyOverviewShows(data);
      else void loadOverviewShows(state.request, state.instance, overviewController);
      return;
    }
    if (parsed.pathname.endsWith("/summary") && state.error) return void refresh();
    if (parsed.pathname.endsWith("/summary") || parsed.pathname.endsWith("/stats")) void loadSummary().then(() => renderSlots()).catch(() => {});
    else if (parsed.pathname.endsWith("/months")) void loadMonths();
    else if (timelineOn() && parsed.pathname.endsWith("/items")) {
      if (state.loading) timelineRefreshPending = true;
      else void loadMain(false, true);
    }
    else if (!state.loading) void loadMain(false, true);
  };
  window.addEventListener("cw:view-updated", event => refreshCachedView(event.detail.url, event.detail.data));
  window.addEventListener("cw:view-status", event => {
    if (!event.detail.url.startsWith("/api/tracker/") || !trackerVisible()) return;
    if (new URL(event.detail.url, location.origin).pathname === "/api/tracker/overview/shows") {
      const host = state.root.querySelector('[aria-labelledby="trk-overview-airing"]');
      const meta = {...event.detail.data, ready:event.detail.data.ready || state.overview?.checking === false};
      window.CW.ViewCache.status(host, meta, "upcoming episodes", () => { window.CW.ViewCache.retry(event.detail.url); refreshCachedView(event.detail.url); }, "episodes");
      return;
    }
    window.CW.ViewCache.status(state.root.querySelector(".cw-main"), event.detail.data, "tracker", () => { window.CW.ViewCache.retry(event.detail.url); refreshCachedView(event.detail.url); }, new URL(event.detail.url, location.origin).pathname);
  });
  window.TrackerPage = { mount, resume: () => { if (profileScope !== currentProfileScope() || expireView()) return mount(state.root); void refreshHeroPlayback(); void loadMain(false, true); } };
})();
