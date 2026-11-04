/* assets/js/tracker.js */
/* CrossWatch - Local tracker browser */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const FEATURES = [
    { id: "history", label: "History", icon: "history", date: "Watched" },
    { id: "watchlist", label: "Watchlist", icon: "bookmark", date: "" },
    { id: "ratings", label: "Ratings", icon: "star", date: "Rated" },
    { id: "progress", label: "Progress", icon: "play_circle", date: "Last played" },
    { id: "collection", label: "Collection", icon: "video_library", date: "Collected" },
  ];
  const MEDIA = [
    { id: "", label: "All titles", icon: "apps" },
    { id: "movie", label: "Movies", icon: "movie" },
    { id: "show", label: "Shows", icon: "tv" },
  ];
  const SORTS = [
    { id: "recent", label: "Newest first" },
    { id: "oldest", label: "Oldest first" },
    { id: "title", label: "Title" },
  ];
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
    rating: "rating", progress_percent: "progress", progress_ms: "progress", ids: "IDs", show_ids: "show IDs",
    title: "title", series_title: "title", year: "year", series_year: "year",
  };
  const ERRORS = {
    sync_running: "A sync is running. Try again when it has finished.",
    changed_since: "This record changed after that edit, so it can't be undone safely.",
    already_undone: "This change was already undone.",
    not_undoable: "An undo can't be undone.",
    key_exists: "Another record already uses these IDs.",
    not_found: "This record no longer exists. Refresh the page.",
    invalid_date: "Enter a valid date that isn't in the future.",
    invalid_rating: "Pick a rating from 1 to 10.",
    invalid_progress: "Progress must be between 0 and 100.",
    invalid_ids: "Check the IDs: IMDb IDs start with tt, the others are numbers. Keep at least one ID.",
    invalid_title: "Enter a title.",
    invalid_year: "Enter a valid year.",
    no_changes: "Nothing changed.",
    too_many: "Select fewer records at once.",
    tracker_unreadable: "The tracker database could not be read. Nothing was changed; check the logs for details.",
    check_running: "Another metadata check is already running.",
    tracker_migrating: "The tracker is being moved to its new database. This page opens as soon as that's done.",
    pair_scoped: "Editing is off while the tracker keeps separate data per sync pair (CW_CROSSWATCH_PAIR_SCOPED).",
    tmdb_key_missing: "Add a TMDB API key in Settings first.",
  };
  const PAGE = 60;
  const STORE_KEY = "cw.tracker.view.v1";

  const state = {
    root: null,
    instance: "default",
    instances: [],
    counts: {},
    effects: {},
    feature: "history",
    media: "",
    sort: "recent",
    q: "",
    items: [],
    total: 0,
    loading: false,
    error: "",
    request: 0,
    wired: false,
    selecting: false,
    selected: new Set(),
    attention: false,
    attentionCounts: {},
    check: null,
    tmdbKey: true,
    fromLink: false,
    scanHidden: "",
    migration: null,
  };
  const sheet = { el: null, mode: "", item: null, busy: false, error: "", confirm: false, keys: [], changes: [], changesTotal: 0, search: null, lastFocus: null, token: 0 };

  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const featureOf = (id) => FEATURES.find((f) => f.id === id) || FEATURES[0];
  const pad = (n) => String(n).padStart(2, "0");
  const plural = (n, one, many) => `${Number(n).toLocaleString()} ${n === 1 ? one : many}`;
  const providerName = (p) => window.CW?.ProviderMeta?.label?.(p) || String(p || "");

  function loadView() {
    try {
      const saved = JSON.parse(localStorage.getItem(STORE_KEY) || "{}");
      if (FEATURES.some((f) => f.id === saved.feature)) state.feature = saved.feature;
      if (MEDIA.some((m) => m.id === saved.media)) state.media = saved.media;
      if (SORTS.some((s) => s.id === saved.sort)) state.sort = saved.sort;
      if (typeof saved.instance === "string" && saved.instance) state.instance = saved.instance;
    } catch {}
  }

  function saveView() {
    try {
      localStorage.setItem(STORE_KEY, JSON.stringify({ feature: state.feature, media: state.media, sort: state.sort, instance: state.instance }));
    } catch {}
  }

  async function getJSON(url, body) {
    const init = body === undefined
      ? { cache: "no-store", credentials: "same-origin" }
      : { method: "POST", cache: "no-store", credentials: "same-origin", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) };
    const res = await fetch(url, init);
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) {
      const err = new Error(data?.error || `HTTP ${res.status}`);
      err.code = data?.error || (typeof data?.detail === "string" ? data.detail : "");
      throw err;
    }
    return data;
  }

  function errorText(err) {
    return ERRORS[err?.code] || "The tracker could not be loaded. Try again in a moment.";
  }

  function writeError(err) {
    return ERRORS[err?.code] || "The change could not be saved. Try again in a moment.";
  }

  function dateText(value) {
    if (!value) return "";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
  }

  function whenText(value) {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "";
    return d.toLocaleString(undefined, { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  }

  function localInput(value) {
    const d = new Date(value || "");
    if (!value || Number.isNaN(d.getTime())) return "";
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function artUrl(item, size = "w342") {
    const art = item?.art;
    if (!art?.tmdb) return "";
    const title = item.series_title || item.title;
    const year = item.type === "movie" ? item.year : item.series_year || item.year;
    return `/art/tmdb/${art.kind}/${encodeURIComponent(art.tmdb)}?size=${size}${title ? `&title=${encodeURIComponent(title)}` : ""}${year ? `&year=${encodeURIComponent(year)}` : ""}`;
  }

  const isChild = (item) => item?.type === "episode" || item?.type === "season";

  function cardTitle(item) {
    return isChild(item) ? item.series_title || item.title || item.key : item.title || item.series_title || item.key;
  }

  function cardSub(item) {
    const parts = [];
    if (item.type === "episode" && item.season != null && item.episode != null) parts.push(`S${pad(item.season)}E${pad(item.episode)}`);
    else if (item.type === "season" && item.season != null) parts.push(`Season ${item.season}`);
    const year = isChild(item) ? item.series_year : item.year;
    if (year) parts.push(String(year));
    return parts.join(" · ");
  }

  function typeIcon(item) {
    if (item?.type === "movie") return "movie";
    if (item?.type === "episode") return "live_tv";
    return "tv";
  }

  function checkedText(check) {
    if (!check?.checked_at) return "";
    const parts = [`Checked ${whenText(check.checked_at)}`];
    if (check.source === "cache" && check.fetched_at) parts.push(`TMDB data from ${dateText(check.fetched_at)}`);
    return parts.join(" · ");
  }

  function checkInfo(item) {
    const c = item?.check;
    if (!c) return null;
    const tm = c.title ? `"${c.title}"${c.year ? ` (${c.year})` : ""}` : "this title";
    const what = c.kind === "movie" ? "movie" : "show";
    const spot = c.episode != null ? `S${pad(c.season)}E${pad(c.episode)}` : c.season != null ? `season ${c.season}` : "this episode";
    switch (c.code) {
      case "no_tmdb":
        return { icon: "link_off", title: "No TMDB ID", text: `Add ${isChild(item) ? "the show's" : "a"} TMDB ID under Identity so covers and checks work.`, fix: ["find"] };
      case "not_found":
        return { icon: "search_off", title: "TMDB doesn't know this ID", text: `TMDB has no ${what} with ID ${c.tmdb}. Find the right title under Identity.`, fix: ["find"] };
      case "mismatch":
        return { icon: "compare_arrows", title: "Looks like a different title", text: `TMDB ${what} ${c.tmdb} is ${tm}. Fix the IDs if this record is something else, or use the TMDB title if only the name is off.`, fix: ["find", "tmdb"] };
      case "incomplete":
        return { icon: "edit_note", title: "Title or year missing", text: `TMDB has ${tm}.`, fix: ["tmdb"] };
      case "episode_missing":
        return { icon: "playlist_remove", title: "Episode not on TMDB", text: `TMDB has no ${spot} for this show. Check the show IDs or the episode number.`, fix: ["find"] };
      case "ok":
        return { ok: true, icon: "verified", title: "Matches TMDB", text: "", fix: [] };
      default:
        return null;
    }
  }

  function renderCard(item, index) {
    const title = cardTitle(item);
    const sub = cardSub(item);
    const when = dateText(item.date);
    const url = artUrl(item);
    const progress = typeof item.progress_percent === "number" ? Math.max(0, Math.min(100, item.progress_percent)) : null;
    const rating = typeof item.rating === "number" ? item.rating : null;
    const label = [title, sub, when].filter(Boolean).join(", ");
    const picked = state.selected.has(item.key);
    const action = state.selecting ? (picked ? `Deselect ${label}` : `Select ${label}`) : `Edit ${label}`;
    return `
      <article class="trk-card${picked ? " is-selected" : ""}" title="${esc(label)}">
        <button type="button" class="trk-cover${url ? "" : " is-empty"}" data-trk-open="${index}" aria-label="${esc(action)}"${state.selecting ? ` aria-pressed="${picked}"` : ""}>
          <span class="trk-cover-fallback">${icon(typeIcon(item))}</span>
          ${url ? `<img class="trk-img" src="${esc(url)}" alt="" loading="lazy" decoding="async">` : ""}
          <span class="trk-type" title="${esc(item.type || "item")}">${icon(typeIcon(item))}</span>
          ${(() => { const info = checkInfo(item); return info && !info.ok ? `<span class="trk-flag" title="${esc(info.title)}" aria-label="${esc(info.title)}">${icon("report")}</span>` : ""; })()}
          ${state.selecting ? `<span class="trk-check">${icon(picked ? "check_circle" : "radio_button_unchecked")}</span>` : `<span class="trk-edit">${icon("edit")}</span>`}
          ${rating != null ? `<span class="trk-rating" aria-label="Rated ${esc(rating)}">${icon("star")}<b>${esc(rating)}</b></span>` : ""}
          ${progress != null ? `<span class="trk-progress" aria-label="${esc(Math.round(progress))}% watched"><i style="width:${progress}%"></i></span>` : ""}
        </button>
        <div class="trk-meta">
          <div class="trk-title">${esc(title)}</div>
          <div class="trk-sub">${sub ? `<span>${esc(sub)}</span>` : ""}${when ? `<span class="trk-when">${icon(featureOf(state.feature).icon)}${esc(when)}</span>` : ""}</div>
        </div>
      </article>`;
  }

  function renderSummary() {
    const total = FEATURES.reduce((sum, f) => sum + Number(state.counts[f.id] || 0), 0);
    return `
      <div class="cw-editor-hero-seg cw-editor-hero-count"><strong>${esc(total.toLocaleString())}</strong><span>records</span></div>
      <button class="cw-editor-refresh" type="button" data-trk-action="refresh" title="Refresh tracker" aria-label="Refresh tracker">${icon("refresh")}</button>`;
  }

  function renderToolbar() {
    const instances = state.instances.length > 1
      ? `<label class="trk-instance" title="Tracker instance">${icon("person")}<select class="cw-select" data-trk-input="instance" aria-label="Tracker instance">${state.instances.map((i) => `<option value="${esc(i.id)}"${i.id === state.instance ? " selected" : ""}>${esc(i.label)}</option>`).join("")}</select></label>`
      : "";
    const features = FEATURES.map((f) => {
      const count = Number((state.attention ? state.attentionCounts : state.counts)[f.id] || 0);
      const active = f.id === state.feature;
      return `<button type="button" class="cw-btn trk-feature" data-trk-feature="${f.id}" aria-pressed="${active}" title="${esc(f.label)}" aria-label="${esc(`${f.label}, ${count} records`)}">${icon(f.icon)}<span class="trk-feature-label">${esc(f.label)}</span><b class="trk-count">${esc(count.toLocaleString())}</b></button>`;
    }).join("");
    const media = MEDIA.map((m) => `<button type="button" class="cw-btn trk-icon-btn" data-trk-media="${m.id}" aria-pressed="${m.id === state.media}" title="${esc(m.label)}" aria-label="${esc(m.label)}">${icon(m.icon)}</button>`).join("");
    const sorts = SORTS.map((s) => `<option value="${s.id}"${s.id === state.sort ? " selected" : ""}>${esc(s.label)}</option>`).join("");
    return `
      ${instances}
      <div class="trk-features" role="group" aria-label="Tracker feature">${features}</div>
      <div class="trk-filters">
        <label class="trk-search">${icon("search")}<input type="search" class="cw-input" data-trk-input="q" value="${esc(state.q)}" placeholder="Search titles" aria-label="Search titles"></label>
        <div class="trk-media" role="group" aria-label="Media type">${media}</div>
        <label class="trk-sort" title="Sort">${icon("sort")}<select class="cw-select" data-trk-input="sort" aria-label="Sort">${sorts}</select></label>
        <button type="button" class="cw-btn trk-icon-btn" data-trk-action="select" aria-pressed="${state.selecting}" title="Select records" aria-label="Select records">${icon("checklist")}</button>
        <button type="button" class="cw-btn trk-icon-btn" data-trk-action="changes" title="Change history" aria-label="Change history">${icon("manage_history")}</button>
        ${renderCheckButtons()}
      </div>`;
  }

  function renderSelectBar() {
    if (!state.selecting) return "";
    const n = state.selected.size;
    const all = state.items.length > 0 && state.items.every((i) => state.selected.has(i.key));
    return `
      <div class="trk-selbar" role="region" aria-label="Selection">
        <span class="trk-selcount">${esc(plural(n, "record", "records"))} selected</span>
        <button type="button" class="cw-btn" data-trk-action="select-all"${state.items.length ? "" : " disabled"}>${icon(all ? "deselect" : "select_all")}<span>${all ? "Clear" : "Select shown"}</span></button>
        <button type="button" class="cw-btn danger" data-trk-action="delete-selected"${n ? "" : " disabled"}>${icon("delete")}<span>Delete</span></button>
        <button type="button" class="cw-btn" data-trk-action="select">${icon("close")}<span>Done</span></button>
      </div>`;
  }

  function renderCheckButtons() {
    const flagged = Object.values(state.attentionCounts || {}).reduce((sum, n) => sum + Number(n || 0), 0);
    const here = Number(state.attentionCounts?.[state.feature] || 0);
    const running = state.check?.state === "running";
    const checkTitle = !state.tmdbKey ? "Add a TMDB API key in Settings to check metadata" : running ? "A metadata check is running" : "Check metadata with TMDB";
    const attention = flagged || state.attention
      ? `<button type="button" class="cw-btn trk-icon-btn trk-attn" data-trk-action="attention" aria-pressed="${state.attention}" title="Needs attention" aria-label="${esc(`Needs attention, ${here} records`)}">${icon("report")}<b class="trk-attn-count">${esc(here.toLocaleString())}</b></button>`
      : "";
    return `${attention}<button type="button" class="cw-btn trk-icon-btn" data-trk-action="check" title="${esc(checkTitle)}" aria-label="${esc(checkTitle)}"${!state.tmdbKey || running ? " disabled" : ""}>${icon("fact_check")}</button>`;
  }

  function renderScan() {
    const job = state.check;
    if (!job || job.state === "idle" || (job.id && state.scanHidden === job.id && job.state !== "running")) return "";
    const total = Number(job.total || 0);
    const done = Number(job.done || 0);
    const attention = Number(job.attention || 0);
    if (job.state === "running") {
      const pct = total ? Math.round((done / total) * 100) : 0;
      return `<div class="trk-scan cw-page-panel is-running" role="status">${icon("sync")}<span class="trk-scan-copy"><b>Checking metadata with TMDB</b><small>${esc(done.toLocaleString())} of ${esc(total.toLocaleString())} records · you can leave this page</small></span><span class="trk-scan-bar" aria-hidden="true"><i style="width:${pct}%"></i></span><button type="button" class="cw-btn" data-trk-action="check-cancel">${icon("stop_circle")}<span>Stop</span></button></div>`;
    }
    const close = `<button type="button" class="cw-btn trk-icon-btn" data-trk-action="check-hide" title="Hide" aria-label="Hide">${icon("close")}</button>`;
    if (job.state === "completed" || job.state === "cancelled") {
      const when = job.finished_at ? whenText(job.finished_at * 1000) : "";
      const head = job.state === "cancelled" ? "Metadata check stopped" : attention ? `${plural(attention, "record needs", "records need")} attention` : "Every record matches TMDB";
      const sub = [when && `Checked ${when}`, job.state === "cancelled" ? "results so far are kept" : "", job.failed ? `${plural(job.failed, "record", "records")} could not be checked` : ""].filter(Boolean).join(" · ");
      const show = attention && !state.attention ? `<button type="button" class="cw-btn" data-trk-action="attention">${icon("report")}<span>Show them</span></button>` : "";
      return `<div class="trk-scan cw-page-panel${attention ? " is-warn" : " is-ok"}" role="status">${icon(attention ? "report" : "verified")}<span class="trk-scan-copy"><b>${esc(head)}</b><small>${esc(sub)}</small></span>${show}${close}</div>`;
    }
    const why = job.state === "interrupted" ? "CrossWatch restarted during the check." : ERRORS[job.error] || (job.error === "tmdb_key_invalid" ? "TMDB rejected the API key. Check it in Settings." : job.error === "tmdb_unreachable" ? "TMDB could not be reached. Try again later." : "The check stopped.");
    return `<div class="trk-scan cw-page-panel is-bad" role="status">${icon("error")}<span class="trk-scan-copy"><b>Metadata check stopped</b><small>${esc(why)}</small></span><button type="button" class="cw-btn" data-trk-action="check"${state.tmdbKey ? "" : " disabled"}>${icon("refresh")}<span>Run again</span></button>${close}</div>`;
  }

  function renderResults() {
    if (state.moving) {
      const move = state.migration;
      const total = Number(move?.files_total || 0);
      const pct = total ? Math.round((Number(move.files_done || 0) / total) * 100) : 0;
      return `<div class="trk-state">${icon("database")}<span>${esc(state.error)}</span>${total ? `<small>${esc(Number(move.files_done || 0).toLocaleString())} of ${esc(total.toLocaleString())} files</small><span class="trk-scan-bar trk-move-bar" aria-hidden="true"><i style="width:${pct}%"></i></span>` : ""}</div>`;
    }
    if (state.error) return `<div class="trk-state is-error">${icon("error")}<span>${esc(state.error)}</span></div>`;
    if (!state.items.length && state.loading) return `<div class="trk-state">${icon("hourglass_empty")}<span>Loading tracker...</span></div>`;
    if (!state.items.length) {
      const f = featureOf(state.feature);
      const filtered = state.q || state.media;
      if (state.attention && !filtered) return `${renderSelectBar()}<div class="trk-state">${icon("verified")}<span>Nothing in ${esc(f.label.toLowerCase())} needs attention.</span></div>`;
      return `${renderSelectBar()}<div class="trk-state">${icon(filtered ? "filter_alt_off" : f.icon)}<span>${filtered ? "No records match these filters." : `No ${esc(f.label.toLowerCase())} records in the tracker yet.`}</span></div>`;
    }
    const more = state.items.length < state.total;
    return `
      ${renderSelectBar()}
      <div class="trk-grid">${state.items.map(renderCard).join("")}</div>
      <div class="trk-foot">
        <span class="trk-shown">${esc(state.items.length.toLocaleString())} of ${esc(state.total.toLocaleString())}</span>
        ${more ? `<button type="button" class="cw-btn trk-more" data-trk-action="more"${state.loading ? " disabled" : ""}>${icon("expand_more")}<span>Show more</span></button>` : ""}
      </div>`;
  }

  function render() {
    const root = state.root;
    if (!root) return;
    if (!root.querySelector(".trk-page")) {
      root.innerHTML = `
        <div class="trk-page">
          <div class="cw-page-hero cw-page-hero-tracker" data-hero-icon="inventory_2">
            <div class="cw-page-hero-copy">
              <div class="cw-page-hero-kicker">CW TRACKER</div>
              <h2 class="cw-page-hero-title">Tracker</h2>
              <div class="cw-page-hero-sub">Everything CrossWatch keeps in its own local tracker.</div>
            </div>
            <div class="cw-page-hero-actions">
              <div class="cw-editor-hero-summary" data-trk-slot="summary" aria-label="Tracker summary"></div>
            </div>
          </div>
          <div class="trk-toolbar cw-page-toolbar" data-trk-slot="toolbar"></div>
          <div data-trk-slot="scan"></div>
          <section class="trk-results cw-page-panel" data-trk-slot="results" aria-live="polite"></section>
        </div>`;
      sheet.el = null;
    }
    renderSlots(true);
  }

  function renderSlots(toolbar) {
    const root = state.root;
    if (!root) return;
    const summary = root.querySelector('[data-trk-slot="summary"]');
    if (summary) summary.innerHTML = renderSummary();
    const bar = root.querySelector('[data-trk-slot="toolbar"]');
    if (bar && toolbar) {
      const focused = document.activeElement?.dataset?.trkInput === "q";
      bar.innerHTML = renderToolbar();
      if (focused) {
        const input = bar.querySelector('[data-trk-input="q"]');
        input?.focus();
        input?.setSelectionRange?.(input.value.length, input.value.length);
      }
    }
    const scan = root.querySelector('[data-trk-slot="scan"]');
    if (scan) scan.innerHTML = renderScan();
    const results = root.querySelector('[data-trk-slot="results"]');
    if (results) results.innerHTML = renderResults();
  }

  let summaryRequest = 0;

  async function loadSummary() {
    const id = ++summaryRequest;
    const data = await getJSON(`/api/tracker/summary?provider_instance=${encodeURIComponent(state.instance)}`);
    if (id !== summaryRequest) throw Object.assign(new Error("stale"), { code: "stale" });
    state.instances = Array.isArray(data.instances) ? data.instances : [];
    state.instance = data.provider_instance || "default";
    state.counts = data.counts || {};
    state.effects = data.delete_effects || {};
    state.attentionCounts = data.attention || {};
    state.check = data.check || null;
    state.tmdbKey = data.tmdb_key !== false;
    if (state.fromLink) {
      state.fromLink = false;
      if (!Number(state.attentionCounts[state.feature] || 0)) {
        const first = FEATURES.find((f) => Number(state.attentionCounts[f.id] || 0) > 0);
        if (first) state.feature = first.id;
      }
    }
  }

  async function loadItems(append) {
    const id = ++state.request;
    state.loading = true;
    if (!append) {
      state.items = [];
      state.total = 0;
    }
    state.error = "";
    renderSlots(false);
    const params = new URLSearchParams({
      feature: state.feature,
      provider_instance: state.instance,
      q: state.q,
      media: state.media,
      sort: state.sort,
      offset: String(append ? state.items.length : 0),
      limit: String(PAGE),
    });
    if (state.attention) params.set("attention", "true");
    try {
      const data = await getJSON(`/api/tracker/items?${params}`);
      if (id !== state.request) return;
      const rows = Array.isArray(data.items) ? data.items : [];
      state.items = append ? state.items.concat(rows) : rows;
      state.total = Number(data.total || 0);
    } catch (err) {
      if (id !== state.request) return;
      state.error = errorText(err);
    } finally {
      if (id === state.request) {
        state.loading = false;
        renderSlots(false);
      }
    }
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
    clearTimeout(moveTimer);
    try {
      await loadSummary();
    } catch (err) {
      if (err?.code === "stale") return;
      state.error = errorText(err);
      state.moving = err?.code === "tracker_migrating";
      render();
      if (state.moving) waitForMove();
      return;
    }
    state.moving = false;
    state.migration = null;
    saveView();
    render();
    pollCheck();
    await loadItems(false);
  }

  let pollTimer = 0;

  function renderScanSlot() {
    const slot = state.root?.querySelector('[data-trk-slot="scan"]');
    if (slot) slot.innerHTML = renderScan();
    const bar = state.root?.querySelector('[data-trk-slot="toolbar"]');
    const check = bar?.querySelector('[data-trk-action="check"]');
    if (check) check.disabled = !state.tmdbKey || state.check?.state === "running";
  }

  function pollCheck() {
    clearTimeout(pollTimer);
    if (state.check?.state !== "running") return;
    pollTimer = setTimeout(async () => {
      if (!state.root || state.root.classList.contains("hidden") || document.hidden) return;
      try {
        const data = await getJSON(`/api/tracker/check?provider_instance=${encodeURIComponent(state.instance)}`);
        state.check = data.job || null;
      } catch {}
      renderScanSlot();
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
      state.scanHidden = "";
      renderSlots(true);
      window.dispatchEvent(new CustomEvent("cw:tracker-check-changed"));
      pollCheck();
    } catch (err) {
      toast.show(writeError(err), { tone: "bad" });
    }
  }

  async function cancelCheck() {
    try {
      await getJSON("/api/tracker/check/cancel", { provider_instance: state.instance });
    } catch {}
    pollCheck();
  }

  function clearSelection() {
    state.selecting = false;
    state.selected.clear();
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
      el.innerHTML = `<span class="cw-ia-toast-icon">${icon(tone === "bad" ? "error" : "check_circle")}</span><span class="cw-ia-toast-text">${esc(text)}</span>${undo ? `<button type="button" data-trk-toast="undo">${icon("undo")}Undo</button>` : ""}<button type="button" class="cw-ia-toast-close" data-trk-toast="close" aria-label="Dismiss">${icon("close")}</button><span class="cw-ia-toast-bar is-timer${tone === "bad" ? " is-long" : ""}"></span>`;
      el.hidden = false;
      timer = window.setTimeout(hide, tone === "bad" ? 10000 : 8000);
    }

    return { show, hide };
  })();

  function effectHtml(feature, count) {
    const fx = state.effects?.[feature] || {};
    const names = (list) => (Array.isArray(list) ? list : []).map((t) => `${providerName(t.provider)}${t.instance && t.instance !== "default" ? ` (${t.instance})` : ""}`);
    const spreads = names(fx.spreads);
    const returns = names(fx.returns);
    const it = count === 1 ? "it" : "them";
    const lines = [];
    if (spreads.length) lines.push(`<p class="trk-effect is-danger">${icon("sync_problem")}<span>The next sync also removes ${it} from <b>${esc(spreads.join(", "))}</b>.</span></p>`);
    if (returns.length) lines.push(`<p class="trk-effect">${icon("sync")}<span><b>${esc(returns.join(", "))}</b> may add ${it} back on the next sync while ${count === 1 ? "it still has it" : "they still have them"}.</span></p>`);
    if (!lines.length) lines.push(`<p class="trk-effect">${icon("info")}<span>Only removed from this tracker. No sync pair removes ${it} anywhere else.</span></p>`);
    return lines.join("");
  }

  function ensureSheet() {
    if (sheet.el && state.root?.contains(sheet.el)) return sheet.el;
    const el = document.createElement("div");
    el.className = "cw-ia-sheet-wrap cw-page-item-actions trk-sheet-wrap";
    el.hidden = true;
    el.innerHTML = `<div class="cw-ia-sheet-backdrop" data-trk-sheet="close"></div><section class="cw-ia-sheet trk-sheet" role="dialog" aria-modal="true" aria-labelledby="trk-sheet-title" tabindex="-1"></section>`;
    state.root.appendChild(el);
    el.addEventListener("click", onSheetClick);
    el.addEventListener("submit", onSheetSubmit);
    el.addEventListener("keydown", onSheetKey);
    sheet.el = el;
    return el;
  }

  function sheetHead(symbolOrItem, title, sub) {
    const item = typeof symbolOrItem === "object" ? symbolOrItem : null;
    const url = item ? artUrl(item, "w185") : "";
    const lead = item
      ? `<div class="cw-ia-sheet-poster">${url ? `<img src="${esc(url)}" alt="">` : `<span class="trk-sheet-icon">${icon(typeIcon(item))}</span>`}</div>`
      : `<span class="trk-sheet-icon">${icon(symbolOrItem)}</span>`;
    return `<header class="cw-ia-sheet-head">${lead}<div class="cw-ia-sheet-title"><h2 id="trk-sheet-title">${esc(title)}</h2>${sub ? `<small>${esc(sub)}</small>` : ""}</div><button type="button" class="cw-ia-icon-btn" data-trk-sheet="close" aria-label="Close">${icon("close")}</button></header>`;
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

  function renderFinding(item) {
    const info = checkInfo(item);
    if (!info) return "";
    const when = checkedText(item.check);
    if (info.ok) return `<p class="trk-finding is-ok">${icon(info.icon)}<span class="trk-finding-copy"><b>${esc(info.title)}</b>${when ? `<small>${esc(when)}</small>` : ""}</span></p>`;
    const buttons = info.fix.map((fix) => fix === "tmdb"
      ? `<button type="button" class="cw-btn" data-trk-sheet="use-tmdb">${icon("auto_fix_high")}<span>Use TMDB title</span></button>`
      : `<button type="button" class="cw-btn" data-trk-sheet="find"${sheet.busy ? " disabled" : ""}>${icon("travel_explore")}<span>Find on TMDB</span></button>`).join("");
    return `<div class="trk-finding" role="note">${icon(info.icon)}<span class="trk-finding-copy"><b>${esc(info.title)}</b><span>${esc(info.text)}</span>${when ? `<small>${esc(when)}</small>` : ""}</span>${buttons ? `<span class="trk-finding-actions">${buttons}</span>` : ""}</div>`;
  }

  function renderEdit() {
    const item = sheet.item;
    const f = featureOf(state.feature);
    const child = isChild(item);
    const title = cardTitle(item);
    const sub = [cardSub(item), f.label].filter(Boolean).join(" · ");
    const dateRow = item.date_field
      ? `<label class="trk-field"><span>${esc(f.date)}</span><input type="datetime-local" class="cw-input" data-trk-edit="date" value="${esc(localInput(item.date))}" max="${esc(localInput(new Date().toISOString()))}"></label>`
      : "";
    const ratingRow = state.feature === "ratings"
      ? `<label class="trk-field"><span>Rating</span><select class="cw-select" data-trk-edit="rating">${Array.from({ length: 10 }, (_, i) => i + 1).map((n) => `<option value="${n}"${Number(item.rating) === n ? " selected" : ""}>${n}</option>`).join("")}</select></label>`
      : "";
    const progressRow = state.feature === "progress"
      ? `<label class="trk-field"><span>Progress %</span><input type="number" class="cw-input" data-trk-edit="progress_percent" min="0" max="100" step="1" value="${esc(item.progress_percent != null ? Math.round(item.progress_percent) : "")}"></label>`
      : "";
    const titleValue = child ? item.series_title : item.title;
    const yearValue = child ? item.series_year : item.year;
    const search = sheet.search;
    const results = search?.results?.length
      ? `<div class="trk-matches" role="listbox" aria-label="TMDB matches">${search.results.map((r, i) => `<button type="button" class="trk-match" data-trk-match="${i}"><span class="trk-match-poster">${r.poster_path ? `<img src="/art/tmdb/${item.type === "movie" ? "movie" : "tv"}/${esc(encodeURIComponent(r.tmdb))}?size=w92" alt="" loading="lazy">` : icon(item.type === "movie" ? "movie" : "tv")}</span><span class="trk-match-copy"><b>${esc(r.title)}</b><small>${esc([r.year, `TMDB ${r.tmdb}`].filter(Boolean).join(" · "))}</small></span></button>`).join("")}</div>`
      : search?.message ? `<p class="trk-note">${esc(search.message)}</p>` : "";
    return `
      ${sheetHead(item, title, sub)}
      <form class="trk-form" data-trk-form="edit" novalidate>
        ${renderFinding(item)}
        ${dateRow || ratingRow || progressRow ? `<div class="trk-fields">${dateRow}${ratingRow}${progressRow}</div>` : ""}
        <details class="trk-identity"${sheet.search ? " open" : ""}>
          <summary>${icon("fingerprint")}<span>Identity</span><small>Fix a wrong match</small></summary>
          <div class="trk-identity-body">
          <div class="trk-fields">
            <label class="trk-field trk-field-wide"><span>${child ? "Show title" : "Title"}</span><input type="text" class="cw-input" data-trk-edit="title" value="${esc(titleValue ?? "")}" maxlength="300"></label>
            <label class="trk-field"><span>Year</span><input type="number" class="cw-input" data-trk-edit="year" min="1870" max="2100" value="${esc(yearValue ?? "")}"></label>
          </div>
          <div class="trk-find">
            <button type="button" class="cw-btn" data-trk-sheet="find"${sheet.busy ? " disabled" : ""}>${icon("travel_explore")}<span>Find on TMDB</span></button>
            <span class="trk-note">Searches by the title above and fills in the IDs.</span>
          </div>
          ${results}
          ${child ? `<div class="trk-group-label">Show IDs</div><div class="trk-fields trk-ids">${idInputs("show_ids", item.show_ids, idList(item.show_ids))}</div><div class="trk-group-label">${item.type === "episode" ? "Episode IDs" : "Season IDs"}</div>` : `<div class="trk-group-label">IDs</div>`}
          <div class="trk-fields trk-ids">${idInputs("ids", item.ids, idList(item.ids))}</div>
          </div>
        </details>
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        ${sheet.confirm ? `<div class="trk-confirm">${effectHtml(state.feature, 1)}</div>` : ""}
        <footer class="trk-sheet-foot">
          ${sheet.confirm
            ? `<button type="button" class="cw-btn" data-trk-sheet="cancel-delete"${sheet.busy ? " disabled" : ""}>Keep</button><button type="button" class="cw-btn danger" data-trk-sheet="delete"${sheet.busy ? " disabled" : ""}>${icon("delete")}<span>Delete record</span></button>`
            : `<button type="button" class="cw-btn danger trk-foot-delete" data-trk-sheet="ask-delete"${sheet.busy ? " disabled" : ""}>${icon("delete")}<span>Delete</span></button><button type="button" class="cw-btn" data-trk-sheet="close">Cancel</button><button type="submit" class="cw-btn primary"${sheet.busy ? " disabled" : ""}>${icon("save")}<span>Save</span></button>`}
        </footer>
      </form>`;
  }

  function renderBulkDelete() {
    const n = sheet.keys.length;
    const f = featureOf(state.feature);
    return `
      ${sheetHead("delete", `Delete ${plural(n, "record", "records")}?`, `From ${f.label}`)}
      <div class="trk-form">
        <div class="trk-confirm">${effectHtml(state.feature, n)}</div>
        <p class="trk-note">You can undo this from the change history.</p>
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        <footer class="trk-sheet-foot">
          <button type="button" class="cw-btn" data-trk-sheet="close">Keep</button>
          <button type="button" class="cw-btn danger" data-trk-sheet="bulk-delete"${sheet.busy ? " disabled" : ""}>${icon("delete")}<span>Delete ${esc(plural(n, "record", "records"))}</span></button>
        </footer>
      </div>`;
  }

  function changeText(change) {
    const first = change.items?.[0];
    const sub = first?.row ? cardSub(first.row) : "";
    const name = first?.row ? `${cardTitle(first.row)}${sub ? ` (${sub})` : ""}` : "a record";
    const others = change.count > 1 ? ` and ${plural(change.count - 1, "other", "others")}` : "";
    if (change.action === "delete") return `Deleted ${name}${others}`;
    if (change.action === "undo") return `Undid a change to ${name}${others}`;
    const fields = Array.from(new Set((first?.fields || []).map((f) => FIELD_LABELS[f]).filter(Boolean)));
    return `Changed ${fields.length ? fields.join(", ") : "details"} of ${name}${others}`;
  }

  function renderChanges() {
    const rows = sheet.changes.map((c) => {
      const f = featureOf(c.feature);
      const symbol = c.action === "delete" ? "delete" : c.action === "undo" ? "undo" : "edit";
      const status = c.undone ? `<span class="trk-change-tag">${icon("undo")}Undone</span>` : "";
      const undo = c.undoable ? `<button type="button" class="cw-btn" data-trk-undo="${esc(c.batch)}"${sheet.busy ? " disabled" : ""}>${icon("undo")}<span>Undo</span></button>` : "";
      return `<li class="trk-change${c.undone ? " is-undone" : ""}"><span class="trk-change-icon">${icon(symbol)}</span><span class="trk-change-copy"><b>${esc(changeText(c))}</b><small>${icon(f.icon)}${esc(f.label)} · ${esc(whenText(c.at))}</small></span>${status}${undo}</li>`;
    }).join("");
    const more = sheet.changes.length < sheet.changesTotal;
    return `
      ${sheetHead("manage_history", "Change history", "Edits and deletes from the last 90 days")}
      <div class="trk-form">
        ${sheet.error ? `<p class="trk-error" role="alert">${icon("error")}<span>${esc(sheet.error)}</span></p>` : ""}
        ${rows ? `<ul class="trk-changes">${rows}</ul>` : sheet.busy ? `<p class="trk-note">Loading...</p>` : `<div class="trk-state">${icon("history_toggle_off")}<span>No changes yet. Edits and deletes you make here show up in this list.</span></div>`}
        ${more ? `<footer class="trk-sheet-foot"><button type="button" class="cw-btn trk-more" data-trk-sheet="more-changes"${sheet.busy ? " disabled" : ""}>${icon("expand_more")}<span>Show more</span></button></footer>` : ""}
      </div>`;
  }

  function renderSheet(focusFirst) {
    const el = ensureSheet();
    const dialog = el.querySelector(".trk-sheet");
    const scroll = dialog.scrollTop;
    dialog.innerHTML = sheet.mode === "edit" ? renderEdit() : sheet.mode === "bulk" ? renderBulkDelete() : renderChanges();
    dialog.scrollTop = scroll;
    if (el.hidden) {
      el.hidden = false;
      document.documentElement.classList.add("cw-ia-sheet-open");
    }
    if (focusFirst) dialog.focus({ preventScroll: true });
  }

  function openSheet(mode, extra = {}) {
    sheet.lastFocus = document.activeElement;
    Object.assign(sheet, { mode, item: null, busy: false, error: "", confirm: false, keys: [], search: null, token: sheet.token + 1 }, extra);
    renderSheet(true);
  }

  function closeSheet() {
    if (!sheet.el || sheet.el.hidden) return;
    sheet.el.hidden = true;
    sheet.mode = "";
    document.documentElement.classList.remove("cw-ia-sheet-open");
    sheet.lastFocus?.focus?.({ preventScroll: true });
  }

  function formChanges() {
    const item = sheet.item;
    const form = sheet.el.querySelector('[data-trk-form="edit"]');
    const changes = {};
    const val = (name) => form.querySelector(`[data-trk-edit="${name}"]`)?.value ?? null;
    const date = val("date");
    if (date !== null && date !== localInput(item.date)) {
      const d = new Date(date);
      changes.date = date && !Number.isNaN(d.getTime()) ? d.toISOString() : "";
    }
    const rating = val("rating");
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
    return changes;
  }

  async function saveEdit() {
    const changes = formChanges();
    if (!Object.keys(changes).length) {
      closeSheet();
      return;
    }
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const data = await getJSON("/api/tracker/edit", { feature: state.feature, provider_instance: state.instance, key: sheet.item.key, changes });
      closeSheet();
      toast.show(`Saved ${cardTitle(data.item || sheet.item)}`, { undo: () => undo(data.batch) });
      await refresh();
    } catch (err) {
      sheet.busy = false;
      sheet.error = err?.code === "changed_since" ? "This record changed while you were editing it. Close it and open it again to see the latest version." : writeError(err);
      renderSheet();
    }
  }

  async function deleteKeys(keys) {
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const data = await getJSON("/api/tracker/delete", { feature: state.feature, provider_instance: state.instance, keys });
      closeSheet();
      clearSelection();
      toast.show(`Deleted ${plural(data.count || keys.length, "record", "records")}`, { undo: () => undo(data.batch) });
      await refresh();
    } catch (err) {
      sheet.busy = false;
      sheet.error = writeError(err);
      renderSheet();
    }
  }

  async function undo(batch) {
    try {
      await getJSON("/api/tracker/undo", { provider_instance: state.instance, batch });
      toast.show("Change undone");
      if (sheet.mode === "changes") await loadChanges(false);
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
    sheet.busy = true;
    sheet.error = "";
    renderSheet();
    try {
      const offset = append ? sheet.changes.length : 0;
      const data = await getJSON(`/api/tracker/changes?provider_instance=${encodeURIComponent(state.instance)}&offset=${offset}&limit=30`);
      const rows = Array.isArray(data.changes) ? data.changes : [];
      sheet.changes = append ? sheet.changes.concat(rows) : rows;
      sheet.changesTotal = Number(data.total || 0);
    } catch (err) {
      sheet.error = writeError(err);
    }
    sheet.busy = false;
    if (sheet.mode === "changes") renderSheet();
  }

  async function findMatches() {
    const item = sheet.item;
    const input = sheet.el.querySelector('[data-trk-edit="title"]');
    const q = (input?.value || cardTitle(item) || "").trim();
    const yearRaw = sheet.el.querySelector('[data-trk-edit="year"]')?.value || "";
    const typ = item.type === "movie" ? "movie" : "show";
    const keep = formChanges();
    const token = ++sheet.token;
    sheet.busy = true;
    sheet.search = { results: [], message: "Searching TMDB..." };
    renderSheet();
    restoreForm(keep);
    let search;
    try {
      if (q.length < 2) throw Object.assign(new Error("short"), { code: "short" });
      const data = await getJSON(`/api/metadata/search?q=${encodeURIComponent(q)}&typ=${typ}&limit=8${/^\d{4}$/.test(yearRaw) ? `&year=${yearRaw}` : ""}`);
      const results = Array.isArray(data.results) ? data.results : [];
      search = { results, typ, message: results.length ? "" : "No TMDB matches for this title." };
    } catch (err) {
      search = { results: [], message: err?.code === "short" ? "Enter a title to search for." : err?.message === "TMDb key missing" ? "Add a TMDB API key in Settings to search." : "TMDB search failed. Try again in a moment." };
    }
    if (token !== sheet.token || sheet.mode !== "edit" || sheet.item !== item) return;
    sheet.search = search;
    const current = formChanges();
    sheet.busy = false;
    renderSheet();
    restoreForm(current);
  }

  function restoreForm(changes) {
    const form = sheet.el?.querySelector('[data-trk-form="edit"]');
    if (!form) return;
    const set = (sel, value) => {
      const input = form.querySelector(sel);
      if (input) input.value = value ?? "";
    };
    if ("date" in changes) set('[data-trk-edit="date"]', localInput(changes.date));
    if ("rating" in changes) set('[data-trk-edit="rating"]', changes.rating);
    if ("progress_percent" in changes) set('[data-trk-edit="progress_percent"]', changes.progress_percent);
    if ("title" in changes) set('[data-trk-edit="title"]', changes.title);
    if ("year" in changes) set('[data-trk-edit="year"]', changes.year);
    for (const group of ["ids", "show_ids"]) {
      for (const [name, value] of Object.entries(changes[group] || {})) set(`[data-trk-id="${group}:${name}"]`, value);
    }
  }

  async function pickMatch(index) {
    const match = sheet.search?.results?.[index];
    if (!match) return;
    const item = sheet.item;
    const token = ++sheet.token;
    const group = isChild(item) ? "show_ids" : "ids";
    const form = sheet.el.querySelector('[data-trk-form="edit"]');
    const fill = (name, value) => {
      const input = form.querySelector(`[data-trk-id="${group}:${name}"]`);
      if (input && value != null && value !== "") input.value = String(value);
    };
    form.querySelector('[data-trk-edit="title"]').value = match.title || "";
    form.querySelector('[data-trk-edit="year"]').value = match.year || "";
    for (const name of BASE_IDS) {
      const input = form.querySelector(`[data-trk-id="${group}:${name}"]`);
      if (input) input.value = "";
    }
    fill("tmdb", match.tmdb);
    form.querySelectorAll(".trk-match").forEach((b, i) => b.setAttribute("aria-selected", String(i === index)));
    try {
      const data = await getJSON("/api/metadata/resolve", { entity: item.type === "movie" ? "movie" : "show", ids: { tmdb: match.tmdb } });
      if (token !== sheet.token || sheet.mode !== "edit" || sheet.item !== item || !form.isConnected) return;
      const ids = data?.result?.ids || {};
      for (const name of ["imdb", "tvdb", "trakt"]) fill(name, ids[name]);
    } catch {}
  }

  function onSheetClick(event) {
    const match = event.target.closest("[data-trk-match]");
    if (match) {
      pickMatch(Number(match.dataset.trkMatch));
      return;
    }
    const undoBtn = event.target.closest("[data-trk-undo]");
    if (undoBtn) {
      sheet.busy = true;
      renderSheet();
      undo(undoBtn.dataset.trkUndo);
      return;
    }
    const action = event.target.closest("[data-trk-sheet]")?.dataset.trkSheet;
    if (!action) return;
    if (action === "close") closeSheet();
    else if (action === "find") findMatches();
    else if (action === "use-tmdb") {
      const check = sheet.item?.check || {};
      const form = sheet.el.querySelector('[data-trk-form="edit"]');
      const title = form?.querySelector('[data-trk-edit="title"]');
      const year = form?.querySelector('[data-trk-edit="year"]');
      if (title && check.title) title.value = check.title;
      if (year && check.year) year.value = check.year;
      const identity = form?.querySelector(".trk-identity");
      if (identity) identity.open = true;
      title?.focus();
    }
    else if (action === "ask-delete" || action === "cancel-delete") {
      const keep = formChanges();
      sheet.confirm = action === "ask-delete";
      sheet.error = "";
      renderSheet();
      restoreForm(keep);
      sheet.el.querySelector(`[data-trk-sheet="${sheet.confirm ? "delete" : "ask-delete"}"]`)?.focus();
    } else if (action === "delete") deleteKeys([sheet.item.key]);
    else if (action === "bulk-delete") deleteKeys(sheet.keys);
    else if (action === "more-changes") loadChanges(true);
  }

  function onSheetSubmit(event) {
    event.preventDefault();
    if (!sheet.busy && sheet.mode === "edit") saveEdit();
  }

  function onSheetKey(event) {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      closeSheet();
      return;
    }
    if (event.key !== "Tab") return;
    const dialog = sheet.el.querySelector(".trk-sheet");
    const nodes = Array.from(dialog.querySelectorAll("button:not([disabled]), input:not([disabled]), select:not([disabled]), summary, [tabindex]:not([tabindex='-1'])")).filter((n) => n.offsetParent !== null);
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

  let searchTimer = 0;

  function wire(root) {
    if (state.wired) return;
    state.wired = true;
    root.addEventListener("click", (event) => {
      if (sheet.el?.contains(event.target)) return;
      const feature = event.target.closest("[data-trk-feature]");
      if (feature) {
        if (feature.dataset.trkFeature === state.feature) return;
        state.feature = feature.dataset.trkFeature;
        state.selected.clear();
        saveView();
        renderSlots(true);
        loadItems(false);
        return;
      }
      const media = event.target.closest("[data-trk-media]");
      if (media) {
        if (media.dataset.trkMedia === state.media) return;
        state.media = media.dataset.trkMedia;
        saveView();
        renderSlots(true);
        loadItems(false);
        return;
      }
      const open = event.target.closest("[data-trk-open]");
      if (open) {
        const item = state.items[Number(open.dataset.trkOpen)];
        if (!item) return;
        if (state.selecting) {
          if (state.selected.has(item.key)) state.selected.delete(item.key);
          else state.selected.add(item.key);
          renderSlots(false);
          state.root.querySelector(`[data-trk-open="${open.dataset.trkOpen}"]`)?.focus({ preventScroll: true });
        } else {
          openSheet("edit", { item });
        }
        return;
      }
      const action = event.target.closest("[data-trk-action]")?.dataset.trkAction;
      if (action === "refresh") refresh();
      else if (action === "more" && !state.loading) loadItems(true);
      else if (action === "changes") {
        openSheet("changes", { changes: [], changesTotal: 0 });
        loadChanges(false);
      } else if (action === "attention") {
        state.attention = !state.attention;
        state.selected.clear();
        renderSlots(true);
        loadItems(false);
      } else if (action === "check") {
        startCheck();
      } else if (action === "check-cancel") {
        cancelCheck();
      } else if (action === "check-hide") {
        state.scanHidden = state.check?.id || "";
        renderScanSlot();
      } else if (action === "select") {
        state.selecting = !state.selecting;
        state.selected.clear();
        renderSlots(true);
      } else if (action === "select-all") {
        const all = state.items.every((i) => state.selected.has(i.key));
        state.items.forEach((i) => (all ? state.selected.delete(i.key) : state.selected.add(i.key)));
        renderSlots(false);
      } else if (action === "delete-selected" && state.selected.size) {
        openSheet("bulk", { keys: Array.from(state.selected) });
      }
    });
    root.addEventListener("input", (event) => {
      if (event.target.dataset?.trkInput !== "q") return;
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => {
        state.q = event.target.value;
        loadItems(false);
      }, 250);
    });
    root.addEventListener("change", (event) => {
      const input = event.target.dataset?.trkInput;
      if (input === "sort") {
        state.sort = event.target.value;
        saveView();
        loadItems(false);
      }
      if (input === "instance") {
        state.instance = event.target.value;
        clearSelection();
        refresh();
      }
    });
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) return;
      if (state.moving) waitForMove();
      else pollCheck();
    });
    window.addEventListener("cw:tracker-migrated", () => {
      if (state.moving) refresh();
    });
    root.addEventListener("error", (event) => {
      const img = event.target;
      if (!(img instanceof HTMLImageElement) || !img.classList.contains("trk-img")) return;
      img.closest(".trk-cover")?.classList.add("is-empty");
      img.remove();
    }, true);
  }

  function readLink() {
    const hash = window.location.hash || "";
    if (!hash.startsWith("#tracker?")) return;
    const query = new URLSearchParams(hash.slice(hash.indexOf("?") + 1));
    if (query.get("attention") === "1") {
      state.attention = true;
      state.fromLink = true;
    }
    try {
      window.history.replaceState(window.history.state, "", `${window.location.pathname}${window.location.search}#tracker`);
    } catch {}
  }

  async function mount(root) {
    if (!root) return;
    if (state.root !== root) {
      state.root = root;
      state.wired = false;
      sheet.el = null;
      loadView();
    }
    readLink();
    wire(root);
    await refresh();
  }

  window.TrackerPage = { mount };
})();
