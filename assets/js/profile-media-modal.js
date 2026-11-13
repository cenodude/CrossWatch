/* assets/js/profile-media-modal.js */
/* CrossWatch - media details modal for the profile page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const numberFmt = new Intl.NumberFormat(window.__CW_LOCALE || navigator.language || undefined);
  const compactFmt = new Intl.NumberFormat(window.__CW_LOCALE || navigator.language || undefined, { notation: "compact", maximumFractionDigits: 1 });
  const dateFmt = { format: (date, options = {}) => window.CW.ProfileDateTime.format(date, { day: "numeric", month: "short", year: "numeric", ...options }) };
  const TABS = [["overview", "Overview"], ["episodes", "Episodes"], ["cast", "Cast & Crew"], ["media", "Media"], ["similar", "Similar"]];
  const LIVES = [
    ["watchlist", "bookmark", "Watchlist"],
    ["synced", "history", "History"],
    ["ratings", "star", "Ratings"],
    ["collection", "video_library", "Collection"],
    ["scrobble", "sensors", "Scrobbled"],
  ];
  const state = { item: null, meta: null, presence: null, progress: undefined, loading: false, presenceLoading: false, tab: "overview", seq: 0, seasons: new Map(), season: null, episode: null, seriesScroll: 0, lastFocus: null, tracker: null, host: null, stacked: false };
  let root = null;
  const extraMetadata = new Map();
  let personView = null;
  let collectionView = null;
  let pendingRender = null;
  const renderedAttributes = new WeakMap();

  const kindOf = (item) => (String(item?.type || item?.media_type || "").toLowerCase() === "movie" ? "movie" : "show");
  const tmdbOf = (item) => String(window.CW?.Meta?.tmdbId?.(item) || item?.tmdb || item?.ids?.tmdb || "").trim();
  const artType = (item) => (kindOf(item) === "movie" ? "movie" : "tv");
  const isMovie = () => kindOf(state.item) === "movie";
  const tmdbImage = (path, size) => (path ? `/art/tmdb/image?fallback=2&path=${encodeURIComponent(path)}&size=${encodeURIComponent(size)}` : "");
  const posterUrl = (item) => `/art/tmdb/${artType(item)}/${encodeURIComponent(tmdbOf(item))}?fallback=2&size=w500`;
  const backdropUrl = (item) => `/art/tmdb/${artType(item)}/${encodeURIComponent(tmdbOf(item))}?fallback=2&kind=backdrop&size=w1280`;
  const stillUrl = (season, episode) => `/art/tmdb/tv/${encodeURIComponent(tmdbOf(state.item))}?fallback=2&kind=still&season=${encodeURIComponent(season)}&episode=${encodeURIComponent(episode)}&size=w300`;
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const plural = (count, word) => `${numberFmt.format(Number(count) || 0)} ${word}${Number(count) === 1 ? "" : "s"}`;
  const placeholder = "this.onerror=null;this.src='/assets/img/placeholder_poster.svg?v=2'";

  function fmtIso(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const parsed = Date.parse(raw.length <= 10 ? `${raw}T00:00:00Z` : raw);
    return Number.isFinite(parsed) ? dateFmt.format(new Date(parsed), raw.length <= 10 ? { timeZone: "UTC" } : {}) : "";
  }

  function relTime(epoch) {
    const seconds = Math.floor(Date.now() / 1000) - (Number(epoch) || 0);
    if (!epoch || seconds < 0) return "";
    const steps = [[31536000, "year"], [2592000, "month"], [604800, "week"], [86400, "day"], [3600, "hour"], [60, "minute"]];
    for (const [size, unit] of steps) {
      if (seconds >= size) return `${plural(Math.floor(seconds / size), unit)} ago`;
    }
    return "just now";
  }

  function runtimeLabel(minutes) {
    const total = Number(minutes) || 0;
    if (total <= 0) return "";
    const hours = Math.floor(total / 60);
    return hours ? `${hours}h ${total % 60}m` : `${total}m`;
  }

  function providerChip(ref, missing = false, action = false, episode = null) {
    const provider = String(ref?.provider || "");
    const instance = String(ref?.instance || "default");
    const name = window.CW?.ProviderMeta?.label?.(provider) || provider;
    const logo = window.CW?.ProviderMeta?.logoPath?.(provider) || "";
    const title = `${name}${instance !== "default" ? ` · ${instance}` : ""}${missing ? " · missing" : ""}`;
    const mark = missing ? "" : ref?.rating != null ? `<em>${esc(ref.rating)}</em>` : `<span class="cw-mm-provider-mark material-symbols-rounded" aria-hidden="true">check</span>`;
    const inner = `${logo ? `<img src="${esc(logo)}" alt="" loading="lazy">` : `<b>${esc(name.slice(0, 2))}</b>`}${mark}`;
    if (episode) {
      const label = `${title} · open ${episodeCode(episode)}`;
      return `<button type="button" class="cw-mm-provider${missing ? " is-missing" : ""}" data-mm-episode="${esc(episode.season)}:${esc(episode.episode)}" title="${esc(label)}" aria-label="${esc(label)}">${inner}</button>`;
    }
    if (action) return `<button type="button" class="cw-mm-provider${missing ? " is-missing" : ""}" data-ia-chip data-ia-ref="${esc(`${provider.toUpperCase()}:${instance}`)}" title="${esc(title)}" aria-label="${esc(title)}" aria-haspopup="dialog">${inner}</button>`;
    return `<span class="cw-mm-provider${missing ? " is-missing" : ""}" title="${esc(title)}">${inner}</span>`;
  }

  const LIVE_FEATURES = { watchlist: "watchlist", synced: "history", ratings: "ratings", collection: "collection" };
  const QUICK_ACTIONS = { watchlist: "watchlist", synced: "history", ratings: "rating", collection: "collection" };
  const itemActions = () => (!state.stacked && window.CW?.ItemActions?.canWrite?.() ? window.CW.ItemActions : null);
  const trackerData = () => state.tracker?.data || null;
  const playEpoch = (play) => Math.floor((Date.parse(String(play?.date || "")) || 0) / 1000);
  const liveFeature = (key) => {
    if (!itemActions()) return "";
    if (key === "scrobble") return isMovie() ? "history" : "";
    return isMovie() || key === "watchlist" ? LIVE_FEATURES[key] || "" : "";
  };
  const refId = (ref) => `${String(ref?.provider || "").toUpperCase()}:${String(ref?.instance || "default")}`;

  function missingRefs(key) {
    const present = new Set((state.presence?.[key]?.present || []).map(refId));
    return (state.actionTargets?.[LIVE_FEATURES[key]] || []).filter((row) => !present.has(refId(row))).map((row) => ({ provider: row.provider, instance: row.instance || "default" }));
  }

  function otherRefs(key) {
    const present = new Set((state.presence?.[key]?.present || []).map(refId));
    return (state.actionOthers?.[LIVE_FEATURES[key]] || []).filter((row) => !present.has(refId(row)));
  }

  async function loadActionTargets(seq) {
    const actions = itemActions();
    if (!actions?.targets) return;
    const query = actions.targetQuery ? actions.targetQuery(actionItem(null)) : {};
    const keys = Object.keys(LIVE_FEATURES).filter((key) => liveFeature(key));
    const results = await Promise.all(keys.map((key) => actions.targets(LIVE_FEATURES[key], query)));
    if (seq !== state.seq || root.hidden) return;
    state.actionTargets = Object.fromEntries(keys.map((key, index) => [LIVE_FEATURES[key], results[index]?.rows || []]));
    state.actionOthers = Object.fromEntries(keys.map((key, index) => [LIVE_FEATURES[key], results[index]?.others || []]));
    state.actionPairless = new Set(keys.filter((key, index) => results[index]?.hasPair === false).map((key) => LIVE_FEATURES[key]));
    if (!personView) void scheduleRender();
  }

  function actionItem(entry) {
    const imdb = state.meta?.ids?.imdb || state.item?.ids?.imdb || "";
    const base = {
      type: kindOf(state.item),
      title: state.meta?.title || state.item?.title || "",
      year: state.meta?.year || state.item?.year || "",
      ids: { tmdb: tmdbOf(state.item), ...(imdb ? { imdb } : {}) },
    };
    const found = entry?.entry;
    return found ? { ...base, ...found, ids: { ...base.ids, ...(found.ids || {}) } } : base;
  }

  async function refreshPresence() {
    const seq = state.seq;
    const presence = await loadPresence(state.item);
    if (seq !== state.seq || root.hidden || personView) return;
    if (presence) state.presence = presence;
    await scheduleRender();
  }

  const releaseDate = () => String(state.meta?.release?.date || state.meta?.detail?.release_date || "").slice(0, 10) || null;

  function openPlays() {
    const actions = itemActions();
    if (!actions || !isMovie()) return;
    actions.openHistory({
      item: actionItem(state.presence?.synced),
      releaseDate: releaseDate(),
      poster: posterUrl(state.item),
      onChanged: refreshPresence,
    });
  }

  function openProgressActions(node) {
    const actions = itemActions();
    const rows = Array.isArray(state.progress) ? state.progress : [];
    if (!actions || !rows.length) return;
    const movie = isMovie();
    const progressRows = rows.filter((row) => row.record).map((row) => {
      const who = providerDisplay(row);
      return {
        rid: row.rid,
        provider: row.provider.toUpperCase(),
        instance: row.instance,
        provider_label: who.full,
        label: movie ? who.full : `${episodeCode(row)} on ${who.full}`,
        record: row.record,
      };
    });
    const seq = state.seq;
    void actions.openMenu({
      anchor: node,
      feature: "progress",
      item: actionItem(null),
      progressRows,
      focusRow: node.dataset.iaPlay ?? null,
      scope: () => [root?.querySelector("[data-mm-progress]")],
      onChanged: async () => {
        if (seq !== state.seq) return;
        await Promise.all([loadProgress(state.item, seq), refreshPresence()]);
      },
    });
  }

  function openEverything(node) {
    const actions = itemActions();
    if (!actions) return;
    const keys = isMovie() ? ["watchlist", "synced", "ratings", "collection"] : ["watchlist"];
    const groups = keys.map((key) => {
      const entry = state.presence?.[key];
      const { key: _recordKey, ...titleItem } = actionItem(entry);
      return {
        feature: LIVE_FEATURES[key],
        present: Array.isArray(entry?.present) ? entry.present : [],
        item: key === "synced" ? titleItem : actionItem(entry),
        scope: () => [root?.querySelector(`[data-mm-live="${key}"]`)],
      };
    }).filter((group) => group.present.length);
    if (!groups.length) return;
    void actions.openMenu({ anchor: node, mode: "everything", item: actionItem(null), groups, onChanged: refreshPresence });
  }

  function openQuickAdd(node) {
    const key = node.closest("[data-mm-live]")?.dataset.mmLive || "";
    window.openManualWatchedModal?.({
      action: QUICK_ACTIONS[key],
      item: { tmdb: tmdbOf(state.item), type: isMovie() ? "movie" : "show", title: state.meta?.title || state.item?.title || "", year: state.meta?.year || state.item?.year || "" },
      only: otherRefs(key).map(refId),
    });
  }

  async function openItemActions(node) {
    if (node.hasAttribute("data-ia-more")) {
      openQuickAdd(node);
      return;
    }
    const actions = itemActions();
    if (node.closest("[data-mm-progress]")) {
      openProgressActions(node);
      return;
    }
    if (node.closest("[data-mm-ep-live]")) {
      openEpisodeActions(node);
      return;
    }
    const key = node.closest("[data-mm-live]")?.dataset.mmLive || "";
    const feature = liveFeature(key);
    if (!actions || !feature) return;
    const stateKey = key === "scrobble" ? "synced" : key;
    const entry = state.presence?.[stateKey] || null;
    const present = Array.isArray(entry?.present) ? entry.present : [];
    const [provider, ...instance] = String(node.dataset.iaRef || "").split(":");
    void actions.openMenu({
      anchor: node,
      feature,
      item: actionItem(entry),
      refs: { present, missing: missingRefs(stateKey) },
      focus: node.dataset.iaRef ? { provider, instance: instance.join(":") || "default" } : null,
      rating: entry?.rating ?? null,
      epoch: feature === "history" ? Number(state.presence?.[key]?.last_epoch) || Number(entry?.last_epoch) || 0 : 0,
      releaseDate: feature === "history" ? releaseDate() : null,
      scope: () => [...new Set([key, stateKey])].map((name) => root?.querySelector(`[data-mm-live="${name}"]`)),
      onChanged: refreshPresence,
      openHistory: feature === "history" ? openPlays : null,
    });
  }

  function episodeItem(season, episode) {
    const imdb = state.meta?.ids?.imdb || state.item?.ids?.imdb || "";
    return {
      type: "episode",
      title: state.meta?.title || state.item?.title || "",
      season,
      episode,
      show_ids: { tmdb: tmdbOf(state.item), ...(imdb ? { imdb } : {}) },
    };
  }

  function openEpisodeActions(node) {
    const actions = itemActions();
    if (!actions || !state.episode) return;
    const { season, episode } = state.episode;
    const find = (key) => state.presence?.[key]?.episodes?.find((row) => Number(row.season) === season && Number(row.episode) === episode);
    const history = find("synced");
    const clicked = node.closest("[data-mm-ep-live]")?.dataset.mmEpLive === "scrobble" ? find("scrobble") : history;
    const [provider, ...instance] = String(node.dataset.iaRef || "").split(":");
    void actions.openMenu({
      anchor: node,
      feature: "history",
      item: episodeItem(season, episode),
      refs: { present: history?.present || [], missing: [] },
      focus: node.dataset.iaRef ? { provider, instance: instance.join(":") || "default" } : null,
      epoch: Number(clicked?.epoch || history?.epoch) || 0,
      releaseDate: String(state.seasons.get(String(season))?.episodes?.find((row) => Number(row.episode) === episode)?.air_date || "").slice(0, 10) || null,
      scope: () => [root?.querySelector(".cw-ep-providers")],
      onChanged: refreshPresence,
    });
  }

  function ensure() {
    if (root) return root;
    root = document.createElement("div");
    root.id = "cw-media-modal";
    root.className = "cw-mm cw-page-media-modal";
    root.hidden = true;
    root.innerHTML = `<div class="cw-mm-backdrop" data-mm-close></div>
      <section class="cw-mm-card" role="dialog" aria-modal="true" aria-labelledby="cw-mm-title" tabindex="-1">
        <div class="cw-mm-hero" aria-hidden="true"><div class="cw-mm-hero-art"></div></div>
        <button class="cw-mm-close" type="button" data-mm-close aria-label="Close">${icon("close")}</button>
        <div class="cw-mm-body"></div>
      </section>`;
    document.body.appendChild(root);
    root.addEventListener("click", onClick);
    root.addEventListener("change", (event) => {
      if (!event.target.matches?.("[data-mm-season-picker]")) return;
      state.overviewSeason = Number(event.target.value);
      state.overviewChosenSeason = state.overviewSeason;
      state.seasonEpisodesExpanded = false;
      renderPanel();
      void loadSeason(state.overviewSeason);
    });
    root.addEventListener("toggle", (event) => {
      if (event.target.matches?.("[data-mm-providers]") && event.target.isConnected) state.providersExpanded = event.target.open;
      if (!event.target.matches?.("[data-mm-collection]") || !event.target.isConnected) return;
      if (collectionView) collectionView.open = event.target.open;
      if (event.target.open) void loadCollection();
    }, true);
    document.addEventListener("keydown", (event) => {
      if (root.hidden || event.key !== "Escape") return;
      if (document.getElementById("cw-trailer")?.classList.contains("show")) return;
      const statusMenu = root.querySelector("[data-mm-status-picker][open]");
      if (statusMenu) {
        event.preventDefault();
        event.stopImmediatePropagation();
        statusMenu.open = false;
        statusMenu.querySelector("summary")?.focus();
        return;
      }
      if (!state.stacked && document.querySelector(".cw-ia-pop:not([hidden]),.cw-ia-sheet-wrap:not([hidden]),.cx-backdrop")) return;
      event.preventDefault();
      if (state.stacked) event.stopImmediatePropagation();
      close();
    }, true);
    window.addEventListener("cw:manual-watched-saved", () => {
      if (!root.hidden && !personView) void refreshPresence();
    });
    return root;
  }

  async function loadPresence(item) {
    try {
      const res = await fetch((window.CW?.ProfileViewAs?.scope || String)(`/api/profile/title?type=${kindOf(item)}&tmdb=${encodeURIComponent(tmdbOf(item))}`), { credentials: "same-origin", cache: "no-store" });
      const data = await res.json().catch(() => null);
      return res.ok && data?.ok ? data : null;
    } catch {
      return null;
    }
  }

  async function loadMeta(item) {
    try {
      return await window.CW?.Meta?.get?.(item, "media", { seriesInfo: true }) || null;
    } catch {
      return null;
    }
  }

  async function loadProgress(item, seq) {
    let rows = [];
    if (document.documentElement?.dataset?.cwPermPlayback !== "off") {
      try {
        const tmdb = tmdbOf(item);
        const movie = kindOf(item) === "movie";
        const query = movie ? `tmdb=${encodeURIComponent(tmdb)}&page=1&page_size=50` : `show_tmdb=${encodeURIComponent(tmdb)}&page=1&page_size=250`;
        const res = await fetch((window.CW?.ProfileViewAs?.scope || String)(`/api/playback_progress/items?${query}`), { credentials: "same-origin", cache: "no-store" });
        const data = await res.json().catch(() => null);
        const items = Array.isArray(data?.items) ? data.items : [];
        const matches = movie
          ? items.filter((entry) => {
            if (String(entry?.media_type || "").toLowerCase() !== "movie") return false;
            const records = Array.isArray(entry.records) && entry.records.length ? entry.records : [entry];
            return records.some((record) => String(record?.ids?.tmdb || "") === tmdb);
          }).slice(0, 1)
          : items;
        const records = matches.flatMap((match) => (Array.isArray(match.records) && match.records.length ? match.records : [match]));
        rows = records.map((record, index) => ({
          rid: String(index),
          provider: String(record.provider || ""),
          instance: String(record.instance_id || "default"),
          label: record.provider_label || record.provider || "",
          instanceLabel: record.instance_label || "",
          pct: Number(record.progress_percent) || 0,
          remaining: Number(record.remaining_seconds) || 0,
          updated: record.updated_at || record.progress_at || "",
          season: record.season ?? null,
          episode: record.episode ?? null,
          episodeTitle: String(record.episode_title || ""),
          record,
        }));
        if (!movie) rows.sort((a, b) => (Date.parse(String(b.updated || "")) || 0) - (Date.parse(String(a.updated || "")) || 0));
      } catch {
        rows = [];
      }
    }
    if (seq !== state.seq) return;
    state.progress = rows;
    if (state.tab === "overview") await scheduleRender(true);
  }

  let compactMediaCard = document.documentElement.dataset.cwMediaCard === "compact";

  function setMediaCardStyle(value) {
    compactMediaCard = value === "compact";
    document.querySelectorAll("select[data-cw-media-card]").forEach((control) => {
      control.value = compactMediaCard ? "compact" : "full";
      window.CW?.IconSelect?.enhance?.(control, { className: "cw-plain-select" });
    });
  }

  function canOpen(item) {
    return !compactMediaCard && !!tmdbOf(item);
  }

  async function open(item, options = {}) {
    if (!canOpen(item)) return false;
    ensure();
    const host = options.host || document.body;
    if (root.parentNode !== host) host.appendChild(root);
    state.host = options.host || null;
    state.stacked = !!(options.stacked || options.host);
    state.tracker = options.tracker || null;
    root.classList.toggle("is-stacked", state.stacked);
    const seq = ++state.seq;
    personView = null;
    collectionView = null;
    const sameTitle = state.item && tmdbOf(state.item) === tmdbOf(item) && kindOf(state.item) === kindOf(item);
    Object.assign(state, {
      item,
      meta: window.CW?.Meta?.peek?.(item) || null,
      presence: null,
      loading: true,
      presenceLoading: true,
      actionTargets: null,
      actionOthers: null,
      actionPairless: null,
      tab: "overview",
      providersExpanded: false,
      historyExpanded: false,
      statusBusy: false,
      seasons: sameTitle ? new Map([...state.seasons].filter(([, data]) => !data.loading && !data.error)) : new Map(),
      season: null,
      overviewSeason: null,
      overviewChosenSeason: null,
      seasonPickerOpen: false,
      seasonEpisodesExpanded: false,
      episode: null,
      progress: null,
    });
    if (root.hidden) state.lastFocus = document.activeElement;
    root.querySelectorAll("details[open]").forEach((details) => { details.open = false; });
    root.hidden = false;
    document.documentElement.classList.add("cw-mm-open");
    root.querySelector(".cw-mm-hero-art").style.backgroundImage = `url("${backdropUrl(item)}")`;
    root.querySelector(".cw-mm-body").scrollTop = 0;
    render();
    root.querySelector(".cw-mm-card")?.focus({ preventScroll: true });
    void loadProgress(item, seq);
    if (itemActions()) void loadActionTargets(seq);
    await Promise.all([
      loadMeta(item).then((meta) => {
        if (seq !== state.seq || root.hidden) return;
        state.meta = meta || state.meta;
        state.loading = false;
        void scheduleRender();
      }),
      loadPresence(item).then((presence) => {
        if (seq !== state.seq || root.hidden) return;
        state.presence = presence;
        state.presenceLoading = false;
        if (!isMovie()) {
          const latest = watchedEpisodes().sort((a, b) => Number(b.season) - Number(a.season) || Number(b.episode) - Number(a.episode))[0];
          if (latest) void loadSeason(latest.season);
        }
        void scheduleRender();
      }),
    ]);
    if (seq === state.seq && !root.hidden) await scheduleRender();
    return true;
  }

  const navigate = (item) => open(item, { host: state.host, stacked: state.stacked });

  function updateTracker(data) {
    if (!root || root.hidden || !state.tracker || !data) return;
    const tmdb = String(data.title?.art?.tmdb || data.title?.ids?.tmdb || "");
    if (tmdb && tmdb !== tmdbOf(state.item)) return;
    const instance = trackerData()?.provider_instance;
    if (instance && data.provider_instance && instance !== data.provider_instance) return;
    state.tracker.data = data;
    if (!personView) void scheduleRender();
  }

  function close() {
    if (!root || root.hidden) return;
    state.seq += 1;
    root.hidden = true;
    state.tracker = null;
    if (root.parentNode !== document.body) document.body.appendChild(root);
    document.documentElement.classList.remove("cw-mm-open");
    const focus = state.lastFocus;
    state.lastFocus = null;
    if (focus && typeof focus.focus === "function") focus.focus({ preventScroll: true });
  }

  function scheduleRender(panelOnly = false) {
    if (pendingRender?.seq === state.seq) {
      pendingRender.panelOnly &&= panelOnly;
      return pendingRender.promise;
    }
    const update = { seq: state.seq, panelOnly };
    pendingRender = update;
    update.promise = new Promise((resolve) => {
      window.requestAnimationFrame(() => {
        if (pendingRender === update) pendingRender = null;
        if (update.seq === state.seq && root && !root.hidden) {
          if (update.panelOnly) renderPanel();
          else render();
        }
        resolve();
      });
    });
    return update.promise;
  }

  function renderKey(node) {
    if (node.nodeType !== 1) return node.nodeName;
    const attrs = renderedAttributes.get(node) || new Map([...node.attributes].map(({ name, value }) => [name, value]));
    const identity = attrs.get("id") || attrs.get("data-mm-key") || [...attrs].filter(([name]) => /^data-(mm-|ia-)/.test(name)).map(([name, value]) => `${name}:${value}`).join("|") || attrs.get("data-src") || attrs.get("src") || attrs.get("href") || (attrs.get("class") || "").split(" ")[0];
    return `${node.nodeName}:${identity}`;
  }

  function patchChildren(host, source) {
    const available = new Map();
    for (const child of host.childNodes) {
      const key = renderKey(child);
      if (!available.has(key)) available.set(key, []);
      available.get(key).push(child);
    }
    let cursor = host.firstChild;
    for (const next of [...source.childNodes]) {
      const current = available.get(renderKey(next))?.shift() || next.cloneNode(false);
      if (current !== cursor) host.insertBefore(current, cursor);
      if (current.nodeType === 1) {
        const previous = renderedAttributes.get(current) || new Map([...current.attributes].map(({ name, value }) => [name, value]));
        const attributes = new Map([...next.attributes].map(({ name, value }) => [name, value]));
        for (const [name] of previous) if (!attributes.has(name)) current.removeAttribute(name);
        for (const [name, value] of attributes) if (previous.get(name) !== value) current.setAttribute(name, value);
        if (current.tagName === "OPTION" && previous.has("selected") !== attributes.has("selected")) current.selected = attributes.has("selected");
        renderedAttributes.set(current, attributes);
        patchChildren(current, next);
      } else if (current.nodeValue !== next.nodeValue) current.nodeValue = next.nodeValue;
      cursor = current.nextSibling;
    }
    for (const children of available.values()) for (const child of children) child.remove();
  }

  function patchContent(host, html) {
    const template = document.createElement("template");
    template.innerHTML = html;
    const body = root?.querySelector(".cw-mm-body");
    const scroll = body?.scrollTop;
    const focused = document.activeElement;
    patchChildren(host, template.content);
    if (focused?.isConnected && document.activeElement !== focused) focused.focus({ preventScroll: true });
    if (body && body.scrollTop !== scroll) body.scrollTop = scroll;
  }

  function render() {
    const body = root?.querySelector(".cw-mm-body");
    if (!body) return;
    if (personView) { patchContent(body, personPanel()); return; }
    if (state.episode) {
      patchContent(body, episodeDetail());
      return;
    }
    patchContent(body, `${tagline()}${top()}${livesCard()}${tabs()}<div class="cw-mm-panel" role="tabpanel">${panel()}</div>`);
    const logo = body.querySelector(".cw-mm-title-logo");
    const label = body.querySelector(".cw-mm-title-text");
    if (logo && label && logo.getAttribute("src") !== logo.dataset.src) {
      label.hidden = false;
      logo.onload = () => { logo.hidden = false; label.hidden = true; };
      logo.onerror = () => { logo.hidden = true; label.hidden = false; };
      logo.src = logo.dataset.src;
    }
  }

  function renderPanel() {
    if (state.episode || personView) { render(); return; }
    const host = root?.querySelector(".cw-mm-panel");
    if (host) patchContent(host, panel());
    root?.querySelectorAll("[data-mm-tab]").forEach((btn) => {
      const active = btn.dataset.mmTab === state.tab;
      if (btn.classList.contains("active") !== active) btn.classList.toggle("active", active);
      if (btn.getAttribute("aria-selected") !== String(active)) btn.setAttribute("aria-selected", String(active));
    });
  }

  function tagline() {
    const text = state.meta?.tagline;
    return text ? `<p class="cw-mm-tagline">${esc(text)}</p>` : "";
  }

  function top() {
    const { item, meta } = state;
    const movie = isMovie();
    const detail = meta?.detail || {};
    const title = meta?.title || item?.title || "Untitled";
    const year = meta?.year || item?.year || "";
    const seasons = Number(detail.number_of_seasons) || 0;
    const episodes = Number(detail.number_of_episodes) || 0;
    const chips = [
      meta?.certification || "",
      movie ? runtimeLabel(meta?.runtime_minutes) : (seasons ? plural(seasons, "Season") : ""),
      !movie && episodes ? plural(episodes, "Episode") : "",
      !movie ? detail.status || "" : "",
    ].filter(Boolean);
    const genres = Array.isArray(meta?.genres) ? meta.genres.slice(0, 3) : [];
    const overview = meta?.overview || (state.loading ? "Loading details..." : "No description available.");
    const tmdb = tmdbOf(item);
    return `<div class="cw-mm-top">
      <img class="cw-mm-poster" src="${esc(posterUrl(item))}" alt="" decoding="async" onerror="${placeholder}">
      <div class="cw-mm-heading">
        <h2 id="cw-mm-title" aria-label="${esc(title)}"><span class="cw-mm-title-text">${esc(title)}</span>${tmdb ? `<img class="cw-mm-title-logo" data-src="/art/tmdb/${artType(item)}/${encodeURIComponent(tmdb)}?kind=logo&size=w500" alt="" hidden>` : ""}</h2>
        <p class="cw-mm-sub">${esc([year, movie ? "Movie" : "TV Series"].filter(Boolean).join(" • "))}</p>
        <div class="cw-mm-chips">${chips.map((chip) => `<span class="cw-mm-chip">${esc(chip)}</span>`).join("")}${genres.map((genre) => `<span class="cw-mm-chip is-genre">${esc(genre)}</span>`).join("")}</div>
        <div class="cw-mm-actions">${actions()}</div>
        <p class="cw-mm-overview">${esc(overview)}</p>
      </div>
    </div>`;
  }

  function actions() {
    const { item, meta } = state;
    const tmdb = tmdbOf(item);
    const imdb = meta?.ids?.imdb || item?.ids?.imdb || "";
    const out = [];
    if (window.CW?.Trailer?.has?.(meta)) out.push(`<button class="cw-mm-btn" type="button" data-mm-trailer>${icon("play_arrow")}Watch trailer</button>`);
    out.push(`<a class="cw-mm-btn" href="https://www.themoviedb.org/${artType(item)}/${encodeURIComponent(tmdb)}" target="_blank" rel="noopener">TMDB</a>`);
    if (imdb) out.push(`<a class="cw-mm-btn" href="https://www.imdb.com/title/${encodeURIComponent(imdb)}/" target="_blank" rel="noopener">IMDb</a>`);
    if (removable()) out.push(`<button type="button" class="cw-mm-btn is-danger" data-mm-clear-all aria-haspopup="dialog" title="Remove from everything" aria-label="Remove from everything">${icon("delete")}Remove</button>`);
    const score = Number(meta?.detail?.vote_average) || (Number(meta?.score) || 0) / 10;
    if (score > 0) {
      const votes = Number(meta?.vote_count) || 0;
      out.push(`<span class="cw-mm-score" title="TMDB score${votes ? ` from ${numberFmt.format(votes)} votes` : ""}">${ring(score.toFixed(1), score * 10, "blue")}<span><strong>TMDB</strong><small>${votes ? `${compactFmt.format(votes)} votes` : "Score"}</small></span></span>`);
    }
    const rating = state.presence?.ratings;
    const userScore = Number(rating?.rating);
    if (rating?.rating != null && Number.isFinite(userScore)) {
      const hint = rating.agree === false ? "Scores differ" : rating.rating_adjusted ? "Equivalent scores" : "Same everywhere";
      out.push(`<span class="cw-mm-score" title="Your rating · ${esc(hint)}">${ring(rating.rating, userScore * 10, "amber")}<span><strong>Your rating</strong><small>${esc(hint)}</small></span></span>`);
    }
    return out.join("");
  }

  const ring = (value, pct, tone) => `<span class="cw-mm-ring" style="--p:${Math.max(0, Math.min(100, Math.round(pct)))};--tone:var(--mm-${tone})"><b>${esc(value)}</b></span>`;

  function liveSummary(key, entry) {
    const have = entry.present.length;
    const total = have + (entry.missing?.length || 0);
    if (key === "ratings" && entry.rating != null) return `★ ${entry.rating}`;
    if (key === "scrobble") return plural(entry.count, "scrobble");
    if (key === "collection") return plural(have, "service");
    if (key === "synced" && !isMovie() && Array.isArray(entry.episodes)) return plural(entry.episodes.length, "episode");
    if (key === "synced") return plural(entry.count, "play");
    return `${have} of ${total}`;
  }

  function livesCard() {
    const presence = state.presence;
    const section = (content) => state.tracker
      ? `<details class="cw-mm-lives" data-mm-providers${state.providersExpanded ? " open" : ""}><summary>Providers${icon("expand_more")}</summary>${content}</details>`
      : `<section class="cw-mm-lives"><h3>Providers</h3>${content}</section>`;
    if (!presence) {
      return section(`<p class="cw-mm-muted">${state.presenceLoading ? "Checking your providers..." : "Sync status is not available."}</p>`);
    }
    const tiles = LIVES.filter(([key]) => key in presence).map(([key, symbol, label]) => {
      const entry = presence[key];
      const present = entry?.present || [];
      const synced = present.length > 0;
      const feature = liveFeature(key);
      const missing = feature && key !== "scrobble" ? missingRefs(key) : (synced ? entry.missing || [] : []);
      const latest = key === "scrobble" && !isMovie() && itemActions()
        ? (entry?.episodes || []).reduce((best, row) => (!best || Number(row.epoch) > Number(best.epoch) ? row : best), null)
        : null;
      const chipList = [...present.map((ref) => providerChip(ref, false, !!feature, latest)), ...missing.map((ref) => providerChip(ref, true, !!feature, latest))];
      if (feature && key !== "scrobble" && otherRefs(key).length) chipList.push(`<button type="button" class="cw-mm-provider is-more" data-ia-chip data-ia-more title="Add to another account" aria-label="Add to another account" aria-haspopup="dialog">${icon("add")}</button>`);
      const chips = chipList.length ? `<div class="cw-mm-live-chips">${chipList.join("")}</div>` : "";
      const empty = feature && key !== "scrobble" && state.actionPairless?.has(feature) ? "No sync pair" : "Not synced";
      return `<div class="cw-mm-live${synced ? " is-on" : ""}${feature ? " has-actions" : ""}" data-mm-live="${esc(key)}">
        <div class="cw-mm-live-head">${icon(symbol)}<strong>${esc(label)}</strong><span>${esc(synced ? liveSummary(key, entry) : empty)}</span></div>
        ${chips}
      </div>`;
    }).join("");
    return section(`<div class="cw-mm-live-grid">${tiles}</div>`);
  }

  function removable() {
    const presence = state.presence;
    return !!presence && !!itemActions() && (isMovie() ? ["watchlist", "synced", "ratings", "collection"] : ["watchlist"]).some((key) => (presence[key]?.present || []).length);
  }

  function tabs() {
    const movie = isMovie();
    return `<nav class="cw-mm-tabs" role="tablist">${TABS.filter(([key]) => !(movie && key === "episodes")).map(([key, label]) => `<button type="button" role="tab" class="${state.tab === key ? "active" : ""}" aria-selected="${state.tab === key}" data-mm-tab="${key}">${esc(label)}</button>`).join("")}</nav>`;
  }

  function panel() {
    if (state.tab === "episodes") return episodesTab();
    if (state.tab === "cast") return castTab();
    if (state.tab === "media") return mediaTab();
    if (state.tab === "similar") return similarTab();
    return overviewTab() + collectionPanel();
  }

  const empty = (text) => `<div class="cw-mm-empty">${esc(text)}</div>`;

  function trackerEpisodes() {
    const episodes = new Map();
    for (const play of trackerData()?.plays || []) {
      if (play.season == null || play.episode == null) continue;
      const row = { season: play.season, episode: play.episode, epoch: playEpoch(play) };
      const key = `${row.season}:${row.episode}`;
      if (!episodes.has(key) || row.epoch > episodes.get(key).epoch) episodes.set(key, row);
    }
    return [...episodes.values()];
  }

  const watchReady = () => !!trackerData() || (!state.presenceLoading && !!state.presence);

  function watchedEpisodes() {
    if (trackerData()) return trackerEpisodes();
    const episodes = new Map();
    for (const entry of [state.presence?.synced, state.presence?.scrobble]) {
      for (const row of entry?.episodes || []) {
        const key = `${row.season}:${row.episode}`;
        const previous = episodes.get(key);
        if (!previous || Number(row.epoch) > Number(previous.epoch)) episodes.set(key, row);
      }
    }
    return [...episodes.values()];
  }

  const watchedMap = () => new Map(watchedEpisodes().map((row) => [`${row.season}:${row.episode}`, row.epoch]));

  function matchedEpisodes() {
    const seasons = seasonList({ specials: true });
    return watchedEpisodes().filter(row => seasons.some(season => Number(season.season) === Number(row.season) && Number(row.episode) > 0 && Number(row.episode) <= Number(season.episode_count)));
  }

  function unmatchedEpisodes() {
    const matched = new Set(matchedEpisodes().map(row => `${row.season}:${row.episode}`));
    return watchedEpisodes().filter(row => !matched.has(`${row.season}:${row.episode}`));
  }

  function stepEpisode(pos) {
    const seasons = seasonList();
    const index = seasons.findIndex((row) => Number(row.season) === Number(pos.season));
    if (index < 0) return null;
    if (Number(pos.episode) < Number(seasons[index].episode_count)) return { season: Number(pos.season), episode: Number(pos.episode) + 1 };
    const next = seasons[index + 1];
    return next ? { season: Number(next.season), episode: 1 } : null;
  }

  function upNext() {
    const seasons = seasonList();
    if (!watchReady() || !seasons.length) return { first: null, label: "", second: null };
    if (unmatchedEpisodes().length) return { first: null, label: "", second: null };
    const known = new Set(seasons.map((row) => Number(row.season)));
    const watched = matchedEpisodes()
      .map((row) => ({ season: Number(row.season), episode: Number(row.episode) }))
      .filter((row) => known.has(row.season))
      .sort((x, y) => x.season - y.season || x.episode - y.episode);
    if (!watched.length) {
      const first = { season: Number(seasons[0].season), episode: 1 };
      return { first, label: "Start watching", second: stepEpisode(first) };
    }
    const last = watched[watched.length - 1];
    const next = stepEpisode(last);
    if (!next) return { first: last, label: "Last watched", second: null };
    return { first: next, label: "Continue watching", second: stepEpisode(next) };
  }

  function episodeSlot(label, pos, fallback = null) {
    let ep = fallback;
    if (!ep) {
      const data = state.seasons.get(String(pos.season));
      if (!data) void loadSeason(pos.season);
      if (!data || data.loading) {
        return `<div class="cw-mm-ep is-loading"><h5>${esc(label)}</h5><span class="cw-mm-ep-still"></span><span><strong>S${esc(pos.season)}.E${esc(pos.episode)}</strong><small>Loading episode...</small></span></div>`;
      }
      ep = (data.episodes || []).find((row) => Number(row.episode) === Number(pos.episode));
      if (!ep) return "";
    }
    const watched = watchedMap().get(`${pos.season}:${pos.episode}`);
    const airs = Date.parse(String(ep.air_date || ""));
    const badge = watched
      ? `<span class="cw-mm-watched">${icon("check_circle")}Watched</span>`
      : Number.isFinite(airs) && airs > Date.now() ? `<span class="cw-mm-soon">${icon("event_upcoming")}Airs ${esc(fmtIso(ep.air_date))}</span>` : "";
    return `<button type="button" class="cw-mm-ep" data-mm-episode="${esc(pos.season)}:${esc(pos.episode)}">
      <h5>${esc(label)}${badge}</h5>
      <span class="cw-mm-ep-still"><img src="${esc(stillUrl(pos.season, pos.episode))}" alt="" loading="lazy" onerror="${placeholder}">${ep.runtime ? `<em>${esc(runtimeLabel(ep.runtime))}</em>` : ""}</span>
      <span><strong>S${esc(pos.season)}.E${esc(pos.episode)} – ${esc(ep.name || "Episode")}</strong><small>${esc(fmtIso(ep.air_date))}</small><p>${esc(ep.overview || "")}</p></span>
    </button>`;
  }

  function detailTiles() {
    const { meta } = state;
    if (!meta) return empty(state.loading ? "Loading details..." : "No details on TMDB.");
    const movie = isMovie();
    const detail = meta.detail || {};
    const leads = (meta.credits?.crew || []).filter((person) => person.job === (movie ? "Director" : "Creator")).map((person) => person.name).slice(0, 2);
    const seasons = Number(detail.number_of_seasons) || 0;
    const tiles = movie
      ? [
        ["event", "Released", fmtIso(meta.release?.date || detail.release_date)],
        ["verified_user", "Age rating", meta.certification || ""],
        ["schedule", "Runtime", runtimeLabel(meta.runtime_minutes)],
        ["movie", "Director", leads.join(", ")],
      ]
      : [
        ["event", "First aired", fmtIso(detail.first_air_date)],
        ["update", "Last aired", fmtIso(detail.last_episode_to_air?.air_date)],
        ["verified_user", "Age rating", meta.certification || ""],
        ["timer", "Episode length", runtimeLabel(meta.runtime_minutes)],
        ["stacks", "Seasons", seasons ? plural(seasons, "season") : ""],
        ["live_tv", "Status", detail.status || ""],
      ];
    const shown = tiles.filter(([, , value]) => value);
    if (!shown.length) return empty("No details on TMDB.");
    return `<div class="cw-mm-tiles">${shown.map(([symbol, label, value]) => `<div class="cw-mm-tile" title="${esc(value)}">${icon(symbol)}<span><small>${esc(label)}</small><strong>${esc(value)}</strong></span></div>`).join("")}</div>`;
  }

  const providerDisplay = (row) => {
    const name = window.CW?.ProviderMeta?.label?.(row.provider) || row.label || row.provider;
    const profile = row.instance !== "default" ? row.instanceLabel || row.instance : "";
    return { name, profile, full: profile ? `${name} · ${profile}` : name };
  };
  const episodeCode = (row) => (row.season != null && row.episode != null ? `S${row.season}E${row.episode}` : "Episode");

  function progressRowsHtml(records, episodes = false) {
    const editable = !!itemActions() && records.some((row) => row.record?.can_mark_watched || row.record?.can_remove_progress);
    const list = records.map((row) => {
      const logo = window.CW?.ProviderMeta?.logoPath?.(row.provider) || "";
      const who = providerDisplay(row);
      const updated = Date.parse(String(row.updated || ""));
      const meta = [row.remaining ? `${runtimeLabel(Math.round(row.remaining / 60))} left` : "", Number.isFinite(updated) ? `paused ${relTime(Math.floor(updated / 1000))}` : ""].filter(Boolean).join(" · ");
      const heading = who.full;
      const value = Math.round(row.pct);
      const mark = logo ? `<img src="${esc(logo)}" alt="" loading="lazy">` : `<b>${esc(who.name.slice(0, 2))}</b>`;
      const label = `${episodes ? `${episodeCode(row)} on ` : ""}${who.full}: resume point`;
      const logoHtml = editable
        ? `<button type="button" class="cw-mm-progress-logo" data-ia-chip data-ia-ref="${esc(refId(row))}" data-ia-play="${esc(row.rid)}" title="${esc(label)}" aria-label="${esc(label)}" aria-haspopup="dialog">${mark}</button>`
        : `<span class="cw-mm-progress-logo">${mark}</span>`;
      return `<div class="cw-mm-progress-row" data-mm-key="${esc(`progress:${refId(row)}:${row.season ?? ""}:${row.episode ?? ""}`)}">
        ${logoHtml}
        <span class="cw-mm-progress-copy"><strong>${esc(heading)}</strong><small>${esc(meta)}</small></span>
        <span class="cw-mm-progress-mini" role="progressbar" aria-label="${esc(`${episodes ? `${episodeCode(row)} · ` : ""}${who.full}`)}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${value}"><i style="width:${value}%"></i></span>
        <b>${value}%</b>
      </div>`;
    }).join("");
    if (!list) return "";
    return `<div class="cw-mm-progress-rows"${episodes ? "" : " data-mm-progress"}>${list}</div>`;
  }

  function showResumePoints() {
    const rows = Array.isArray(state.progress) ? state.progress : [];
    if (!rows.length) return "";
    const groups = new Map();
    rows.forEach((row) => {
      const key = row.season != null && row.episode != null ? `${row.season}:${row.episode}` : `unknown:${refId(row)}:${row.rid}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(row);
    });
    const cards = [...groups].map(([key, records]) => {
      const code = episodeCode(records[0]);
      const title = records.map(row => String(row.episodeTitle || "").trim()).find(title => title && !/^s\s*\d+\s*e\s*\d+$/i.test(title));
      return `<section class="cw-mm-resume-episode" data-mm-key="${esc(`resume:${key}`)}"><h6><span>${esc(code)}</span>${title ? `<strong>${esc(title)}</strong>` : ""}</h6>${progressRowsHtml(records, true)}</section>`;
    }).join("");
    return `<div class="cw-mm-seasons-progress cw-mm-resume"><h5>Resume points</h5><div class="cw-mm-resume-list" data-mm-progress>${cards}</div></div>`;
  }

  function movieProgressBox() {
    const local = trackerData();
    const history = local ? null : state.presence?.synced;
    const scrobble = local ? null : state.presence?.scrobble;
    const rows = state.progress;
    const records = Array.isArray(rows) ? [...rows].sort((a, b) => b.pct - a.pct) : [];
    const best = records[0] || null;
    const watched = local ? (local.plays || []).length : Math.max(Number(history?.count) || 0, Number(scrobble?.count) || 0);
    const pct = best ? Math.round(best.pct) : watched ? 100 : 0;
    const watchStatus = local ? "" : state.presenceLoading ? "Checking watch status..." : !state.presence ? "Watch status unavailable" : "";
    const summary = best ? `<b>${pct}%</b>` : watched ? "<b>Watched</b>" : watchStatus || (rows === null ? "Checking..." : "Not started");
    const lastEpoch = local ? Math.max(0, ...(local.plays || []).map(playEpoch)) : Math.max(Number(history?.last_epoch) || 0, Number(scrobble?.last_epoch) || 0);
    const lastSeen = lastEpoch ? [`Last watched ${relTime(lastEpoch)}`, dateFmt.format(new Date(lastEpoch * 1000))] : [];
    const historyDetail = watchStatus || (watched
      ? lastSeen.join(" · ") || (local ? "In this tracker" : history?.count ? "In your synced history" : "Scrobbled")
      : local ? "No plays in this tracker" : "No synced history or scrobbles");
    const watchline = `<div class="cw-mm-watch-summary cw-mm-watch-history"><div><h4>Watch history</h4><p>${esc(historyDetail)}</p></div>${watchStatus ? "" : `<div class="cw-mm-watch-stats"><strong class="cw-mm-watch-result${watched ? " is-watched" : ""}">${watched ? icon("check_circle") : ""}${esc(watched ? watched === 1 ? "Watched once" : `Watched ${plural(watched, "time")}` : "Not watched yet")}</strong></div>`}</div>`;
    return `<div class="cw-mm-box"><h4>Watch progress <small>${summary}</small></h4>
      <div class="cw-mm-progress${!best && watched ? " is-done" : ""}"><i style="width:${pct}%"></i></div>
      ${progressRowsHtml(records)}
      ${watchline}${castStrip()}</div>`;
  }

  function overviewTab() {
    const { meta } = state;
    const detail = meta?.detail || {};
    let main;
    if (isMovie()) {
      main = movieProgressBox();
    } else if (!watchReady()) {
      main = `<div class="cw-mm-box"><h4>Watch progress</h4>${empty(state.presenceLoading ? "Checking watch status..." : "Watch status unavailable")}</div>`;
    } else if (!meta) {
      main = `<div class="cw-mm-box"><h4>Watch progress <small>${plural(watchedEpisodes().length, "episode")} watched</small></h4>${empty(state.loading ? "Loading episode details..." : "No episode information on TMDB.")}</div>`;
    } else {
      const total = Number(detail.number_of_episodes) || 0;
      const watched = matchedEpisodes().filter(row => Number(row.season) > 0).length;
      const pct = total ? Math.min(100, Math.round((watched / total) * 100)) : 0;
      const next = upNext();
      const first = next.first ? episodeSlot(next.label, next.first) : "";
      let second = next.second ? episodeSlot("Next episode", next.second) : "";
      const upcoming = detail.next_episode_to_air;
      if (!second && next.label === "Last watched" && upcoming?.season_number != null && upcoming?.episode_number != null && Date.parse(upcoming.air_date || "") > Date.now() && !watchedMap().has(`${upcoming.season_number}:${upcoming.episode_number}`)) {
        second = episodeSlot("Upcoming", { season: upcoming.season_number, episode: upcoming.episode_number }, upcoming);
      }
      const pair = `${first}${second}`;
      const seasonCount = Number(detail.number_of_seasons) || seasonList().length;
      const summary = [total ? plural(total, "episode") : "", seasonCount ? plural(seasonCount, "season") : ""].filter(Boolean).join(" · ");
      main = `<div class="cw-mm-box"><div class="cw-mm-watch-summary"><div><h4>Watch progress</h4>${summary ? `<p>${esc(summary)}</p>` : ""}</div><div class="cw-mm-watch-stats"><strong>${total ? `${numberFmt.format(watched)}/${numberFmt.format(total)}` : plural(watched, "episode")} watched</strong>${total ? `<div class="cw-mm-watch-meter" role="progressbar" aria-label="Episodes watched" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}" aria-valuetext="${esc(`${plural(watched, "episode")} watched out of ${numberFmt.format(total)}`)}"><span><i style="width:${pct}%"></i></span><small>${pct}%</small></div>` : ""}</div></div>
        ${unmatchedEpisodes().length ? `<p class="cw-mm-muted">${plural(unmatchedEpisodes().length, "watched episode")} could not be matched to TMDB season numbering and ${unmatchedEpisodes().length === 1 ? "is" : "are"} excluded from progress.</p>` : pair ? `<div class="cw-mm-episodes-pair">${pair}</div>` : empty("No episode information on TMDB.")}${seasonProgress()}${showResumePoints()}</div>`;
    }
    return `<div class="cw-mm-grid-2">${main}<div class="cw-mm-stack">${trackerData() ? trackerBox() : `<div class="cw-mm-box">${watchHistory(state.presence?.plays)}</div>`}
      <div class="cw-mm-box"><h4>Details</h4>${detailTiles()}${trackerNotes()}</div>

    </div></div>`;
  }

  const TRACKER_STATUS = { watching: "Watching", planned: "Plan to watch", completed: "Completed", on_hold: "On hold", dropped: "Dropped" };
  const TRACKER_STATUS_ICONS = { watching: "play_circle", planned: "schedule", completed: "task_alt", on_hold: "pause_circle", dropped: "do_not_disturb_on" };

  function trackerStatus(data) {
    const current = data.note?.status || "";
    const label = TRACKER_STATUS[current] || "Set a status";
    const disabled = !data.note_key || state.statusBusy || window.CW?.AuthState?.read?.()?.permissions?.write === false;
    const symbol = (name, extra = "") => `<span class="material-symbol ${extra}" aria-hidden="true">${name}</span>`;
    const choices = [...Object.entries(TRACKER_STATUS), ["", "No status"]].map(([value, text]) => `<button type="button" class="trk-status-item${value === current ? " is-current" : ""}" data-mm-trk-status="${value}" data-status="${value}" aria-pressed="${value === current}"${disabled ? " disabled" : ""}>${symbol(TRACKER_STATUS_ICONS[value] || "remove")}<span>${text}</span>${value === current ? symbol("check", "trk-status-check") : ""}</button>`).join("");
    return `<details class="trk-status-pick" data-mm-status-picker><summary class="trk-status-btn${current ? "" : " is-empty"}" data-status="${esc(current)}" aria-label="Watch status: ${label}">${symbol(TRACKER_STATUS_ICONS[current] || "flag")}<span>${label}</span>${symbol("expand_more", "trk-status-caret")}</summary><div class="trk-status-menu" role="group" aria-label="Watch status">${choices}</div></details>`;
  }

  function trackerBox() {
    const data = trackerData();
    if (!data) return "";
    const show = !isMovie();
    const plays = data.plays || [];
    const rating = data.ratings?.[0]?.rating ?? null;
    const listed = (data.watchlist || []).length > 0;
    const hints = {
      log: show ? "Choose episodes and a watch date to add to this tracker" : "Choose a watch date and add a play to this tracker",
      rate: rating != null ? "Change or remove your rating in this tracker" : "Add your rating to this tracker",
      list: listed ? "Remove this title from this tracker watchlist" : "Add this title to this tracker watchlist",
      map: "Choose the correct movie or show for this tracker entry",
      origin: "Inspect the source of this tracker entry",
      remove: "Preview removing this title or its watch history",
      notes: "Edit personal dates, status, tags and notes. These stay in CrossWatch and do not sync",
    };
    const button = (action, symbol, label, primary = false) => `<button type="button" class="cw-mm-btn is-icon${primary ? " is-primary" : ""}${action === "remove" ? " is-danger" : ""}" data-mm-trk="${action}" title="${esc(`${label}: ${hints[action]}`)}" aria-label="${esc(label)}">${icon(symbol)}</button>`;
    const actions = [
      button("log", "add_circle", "Watched", true),
      button("rate", rating != null ? "star" : "star_outline", rating != null ? `Change rating (${rating}/10)` : "Rate"),
      button("list", listed ? "bookmark_remove" : "bookmark_add", listed ? "Remove from watchlist" : "Add to watchlist"),
      button("map", "compare_arrows", "Fix match"),
      ["plays", "ratings", "watchlist", "collection", "progress"].some(key => data[key]?.length) ? button("origin", "alt_route", "Where does this come from?") : "",
      data.note_key ? button("remove", "delete", "Remove title or watch history") : "",
    ].join("");
    return `<div class="cw-mm-box cw-mm-trk"><div class="cw-mm-trk-header">${trackerStatus(data)}<small class="cw-mm-muted">${esc(state.tracker.label || "")}</small></div>
      <div class="cw-mm-trk-actions">${actions}</div>${watchHistory(plays, true)}</div>`;
  }

  function trackerNotes() {
    const data = trackerData();
    if (!data?.note_key) return "";
    const note = data.note;
    const tags = (note?.tags || []).slice(0, 8).map((tag) => `<span class="cw-mm-trk-tag">${esc(tag)}</span>`).join("");
    const noteDates = [note?.started ? `Started ${note.started}` : "", note?.finished ? `Finished ${note.finished}` : ""].filter(Boolean).join(" · ");
    const hasNotes = !!(tags || note?.notes || noteDates || Object.keys(note?.fields || {}).length);
    if (!hasNotes) return `<div class="cw-mm-trk-notes"><button type="button" class="cw-mm-notes-action" data-mm-trk="notes">${icon("add")}<span>Add notes</span></button></div>`;
    return `<div class="cw-mm-trk-notes"><h4>My notes<button type="button" class="cw-mm-notes-action is-icon" data-mm-trk="notes" title="Edit notes" aria-label="Edit notes">${icon("edit_note")}</button></h4>${noteDates ? `<p class="cw-mm-muted">${esc(noteDates)}</p>` : ""}${tags ? `<div class="cw-mm-trk-note">${tags}</div>` : ""}${note?.notes ? `<p class="cw-mm-trk-note-text">${esc(note.notes)}</p>` : ""}</div>`;
  }

  function watchHistory(plays, editable = false) {
    const show = !isMovie();
    const epochOf = play => editable ? playEpoch(play) : Number(play.epoch) || 0;
    const sorted = [...(plays || [])].sort((a, b) => epochOf(b) - epochOf(a));
    const when = (play, time = false) => {
      const epoch = epochOf(play);
      if (!epoch) return "";
      const date = new Date(epoch * 1000);
      return time ? window.CW.ProfileDateTime.format(date, { hour: "2-digit", minute: "2-digit" }) : dateFmt.format(date);
    };
    const rows = (state.historyExpanded ? sorted : sorted.slice(0, 5)).map((play) => `<li class="cw-mm-trk-play" data-mm-key="${esc(play.key || `${epochOf(play)}:${play.season ?? ""}:${play.episode ?? ""}`)}">${show ? `<b>${play.season != null ? esc(`S${play.season} E${play.episode}`) : ""}</b>` : ""}<span>${esc(when(play))}</span><span>${esc(when(play, true))}</span>${editable ? `<button type="button" class="cw-mm-trk-icon" data-mm-trk-edit="${esc(play.key)}" title="Edit play" aria-label="Edit play${show ? ` ${esc(episodeCode(play))}` : ""}">${icon("edit")}</button>` : ""}</li>`).join("");
    const more = sorted.length > 5 ? `<button type="button" class="cw-mm-btn cw-mm-history-more" data-mm-history-more aria-expanded="${!!state.historyExpanded}">${state.historyExpanded ? "Show less" : `Show ${esc(plural(sorted.length - 5, "older play"))}`}</button>` : "";
    const message = editable ? "No plays in this tracker yet." : state.presenceLoading ? "Loading watch history..." : !Array.isArray(plays) ? "Watch history unavailable." : "No watch history yet.";
    return `<h4>Watch history</h4>${rows ? `<ul class="cw-mm-trk-plays${show ? "" : " is-movie"}${editable ? "" : " is-readonly"}">${rows}</ul>${more}` : `<p class="cw-mm-muted">${message}</p>`}`;
  }

  const initialsOf = (name) => String(name || "?").split(/\s+/).map((part) => part[0] || "").join("").slice(0, 2).toUpperCase();

  function seasonProgress() {
    const seasons = seasonList({ specials: true, newestFirst: true });
    if (!watchReady() || !seasons.length) return "";
    const counts = new Map();
    for (const row of matchedEpisodes()) {
      counts.set(Number(row.season), (counts.get(Number(row.season)) || 0) + 1);
    }
    const watchedEpisodes = watchedMap();
    const progress = Array.isArray(state.progress) ? state.progress : [];
    const shown = seasons.filter((row, index) => index < 3 || Number(row.season) === state.overviewChosenSeason);
    const rows = shown.map((row) => {
      const number = Number(row.season);
      const total = Number(row.episode_count) || 0;
      const watched = Math.min(total, counts.get(number) || 0);
      const pct = total ? Math.round((watched / total) * 100) : 0;
      const done = total > 0 && watched >= total;
      const resume = progress.filter((entry) => Number(entry.season) === number && Number(entry.episode) > 0 && Number(entry.episode) <= total && entry.pct > 0 && entry.pct < 100);
      const started = watched > 0 || resume.length > 0;
      const expanded = state.overviewSeason === number;
      const name = number === 0 ? "Specials" : `Season ${number}`;
      const year = /^\d{4}/.exec(String(row.air_date || ""))?.[0];
      const segments = total <= 24 ? Array.from({ length: total }, (_, index) => {
        const part = resume.find((entry) => Number(entry.episode) === index + 1);
        const value = watchedEpisodes.has(`${number}:${index + 1}`) ? 100 : part ? Math.min(100, Math.max(0, part.pct)) : 0;
        return `<span><i style="width:${value}%"></i></span>`;
      }).join("") : `<span><i style="width:${pct}%"></i></span>`;
      return `<div class="cw-mm-season-item" data-mm-key="season:${number}"><button type="button" class="cw-mm-season-row${done ? " is-done" : started ? " is-started" : ""}" data-mm-season-toggle="${esc(number)}" aria-expanded="${expanded}" aria-controls="cw-mm-season-detail-${number}">
        <span class="cw-mm-season-number">${number === 0 ? "SP" : numberFmt.format(number)}</span>
        <span class="cw-mm-season-copy"><span class="cw-mm-season-name">${esc(name)}</span><span class="cw-mm-season-meta">${esc([year, plural(total, "episode")].filter(Boolean).join(" · "))}</span></span>
        <span class="cw-mm-season-stats"><span class="cw-mm-season-count">${numberFmt.format(watched)}/${numberFmt.format(total)}<span class="cw-mm-season-watched-label"> watched</span></span><span class="cw-mm-season-status">${done ? "Completed" : started ? "In progress" : "Not started"}</span><span class="cw-mm-season-track" aria-hidden="true">${segments}</span></span>
        ${icon(expanded ? "expand_more" : "chevron_right")}
      </button><div class="cw-mm-season-detail" id="cw-mm-season-detail-${number}"${expanded ? "" : " hidden"}>${expanded ? seasonEpisodes(number, watchedEpisodes, resume) : ""}</div></div>`;
    }).join("");
    const more = seasons.length > 3 ? `<button type="button" data-mm-seasons-all aria-expanded="${!!state.seasonPickerOpen}" aria-controls="cw-mm-season-picker">All ${numberFmt.format(seasons.length)} seasons</button>` : "";
    const picker = state.seasonPickerOpen ? `<div class="cw-mm-season-picker" id="cw-mm-season-picker"><label for="cw-mm-season-select">Jump to season</label><select id="cw-mm-season-select" data-mm-season-picker><option value="" disabled${state.overviewSeason == null ? " selected" : ""}>Choose a season</option>${seasons.map((row) => `<option value="${esc(row.season)}"${Number(row.season) === state.overviewSeason ? " selected" : ""}>${esc(Number(row.season) === 0 ? "Specials" : `Season ${row.season}`)}</option>`).join("")}</select></div>` : '<div id="cw-mm-season-picker" hidden></div>';
    return `<div class="cw-mm-seasons-progress"><h5>Seasons${more}</h5><p class="cw-mm-season-order">Newest first</p>${picker}${rows}</div>`;
  }

  function seasonEpisodes(number, watched, progress) {
    const data = state.seasons.get(String(number));
    if (!data || data.loading) return '<p class="cw-mm-muted" role="status">Loading episodes...</p>';
    if (data.error) return `<p class="cw-mm-muted">Episode details unavailable.</p><button type="button" class="cw-mm-btn" data-mm-season-retry="${number}">Try again</button>`;
    const episodes = data.episodes || [];
    if (!episodes.length) return '<p class="cw-mm-muted">No episodes found for this season.</p>';
    const resumeIndex = episodes.findIndex((ep) => progress.some((row) => Number(row.episode) === Number(ep.episode)));
    const nextIndex = episodes.findIndex((ep) => !watched.has(`${number}:${ep.episode}`));
    const previewStart = Math.max(0, resumeIndex >= 0 ? resumeIndex : nextIndex);
    const start = state.seasonEpisodesExpanded ? 0 : previewStart;
    const shown = state.seasonEpisodesExpanded ? episodes : episodes.slice(start, start + 3);
    const rows = shown.map((ep) => {
      const resume = progress.find((row) => Number(row.episode) === Number(ep.episode));
      const done = watched.has(`${number}:${ep.episode}`);
      const upcoming = Date.parse(String(ep.air_date || "")) > Date.now();
      const status = resume ? `${Math.round(resume.pct)}% watched` : done ? "Watched" : upcoming ? "Upcoming" : "Unwatched";
      const details = [resume?.remaining ? `${runtimeLabel(Math.round(resume.remaining / 60))} left` : runtimeLabel(ep.runtime), fmtIso(ep.air_date)].filter(Boolean).join(" · ");
      return `<button type="button" class="cw-mm-season-episode${resume ? " is-started" : done ? " is-done" : ""}" data-mm-episode="${number}:${esc(ep.episode)}"><span class="cw-mm-season-episode-number">${esc(String(ep.episode).padStart(2, "0"))}</span><span class="cw-mm-season-copy"><strong>${esc(ep.name || `Episode ${ep.episode}`)}</strong>${details ? `<small>${esc(details)}</small>` : ""}</span><span class="cw-mm-season-episode-status">${esc(status)}</span></button>`;
    }).join("");
    const more = episodes.length > 3 || previewStart > 0 ? `<button type="button" class="cw-mm-season-more" data-mm-season-more aria-expanded="${!!state.seasonEpisodesExpanded}">${state.seasonEpisodesExpanded ? "Show less" : `Show all ${plural(episodes.length, "episode")}`}</button>` : "";
    return `<p class="cw-mm-season-detail-label">${resumeIndex >= 0 && !state.seasonEpisodesExpanded ? "Where you left off" : "Episodes"}</p>${rows}${more}`;
  }

  function castStrip() {
    const cast = (state.meta?.credits?.cast || []).slice(0, 7);
    if (!cast.length) return "";
    const faces = cast.map((person) => `<button type="button" class="cw-mm-cast-face" ${person.id ? `data-mm-person="${esc(person.id)}"` : 'data-mm-goto="cast"'} title="${esc(`${person.name}${person.character ? ` as ${person.character}` : ""}`)}">
        ${person.profile_path ? `<img src="${esc(tmdbImage(person.profile_path, "w185"))}" alt="" loading="lazy">` : `<span class="cw-mm-initials">${esc(initialsOf(person.name))}</span>`}
        <small>${esc(person.name)}</small>
      </button>`).join("");
    return `<div class="cw-mm-cast-strip"><h5>Top cast<button type="button" data-mm-goto="cast">See all</button></h5><div>${faces}</div></div>`;
  }

  function seasonList({ specials = false, newestFirst = false } = {}) {
    const seasons = (state.meta?.detail?.seasons || []).filter((row) => Number(row?.episode_count) > 0)
      .sort((a, b) => (newestFirst ? -1 : 1) * (Number(a.season) - Number(b.season)));
    const regular = seasons.filter((row) => Number(row.season) > 0);
    if (!regular.length) return seasons;
    return specials ? [...regular, ...seasons.filter((row) => Number(row.season) === 0)] : regular;
  }

  async function loadSeason(number) {
    const key = String(number);
    if (state.seasons.has(key)) return;
    state.seasons.set(key, { loading: true });
    const seq = state.seq;
    let result;
    try {
      const res = await fetch(`/api/metadata/tmdb/season?tmdb=${encodeURIComponent(tmdbOf(state.item))}&season=${encodeURIComponent(number)}`, { credentials: "same-origin" });
      const data = await res.json().catch(() => null);
      result = data?.ok ? data : { error: true };
    } catch {
      result = { error: true };
    }
    if (seq !== state.seq) return;
    state.seasons.set(key, result);
    if (state.episode || state.tab === "overview" || (state.tab === "episodes" && String(state.season) === key)) await scheduleRender(true);
  }

  function episodesTab() {
    if (!state.meta) return empty(state.loading ? "Loading seasons..." : "No season information on TMDB.");
    const seasons = seasonList({ specials: true, newestFirst: true });
    if (!seasons.length) return empty("No season information on TMDB.");
    const watched = watchedMap();
    if (state.season == null) {
      const lastWatched = [...watched.keys()].map((key) => Number(key.split(":")[0])).filter((value) => seasons.some((row) => Number(row.season) === value));
      state.season = lastWatched.length ? Math.max(...lastWatched) : seasons[0].season;
    }
    const current = String(state.season);
    const data = state.seasons.get(current);
    if (!data) void loadSeason(state.season);
    const picker = `<div class="cw-mm-seasons">${seasons.map((row) => `<button type="button" class="${String(row.season) === current ? "active" : ""}" data-mm-season="${esc(row.season)}">${esc(row.name || `Season ${row.season}`)}</button>`).join("")}</div>`;
    if (!data || data.loading) return `${picker}${empty("Loading episodes...")}`;
    if (data.error || !data.episodes?.length) return `${picker}${empty("No episodes found for this season.")}`;
    const rows = data.episodes.map((ep) => {
      const epoch = watched.get(`${state.season}:${ep.episode}`);
      return `<button type="button" class="cw-mm-eprow${epoch ? " is-watched" : ""}" data-mm-episode="${esc(state.season)}:${esc(ep.episode)}">
        <span class="cw-mm-ep-still">${ep.has_still ? `<img src="${esc(stillUrl(state.season, ep.episode))}" alt="" loading="lazy" onerror="${placeholder}">` : ""}${ep.runtime ? `<em>${esc(runtimeLabel(ep.runtime))}</em>` : ""}</span>
        <span><strong>S${esc(state.season)}.E${esc(ep.episode)} – ${esc(ep.name || "Episode")}</strong><small>${esc(fmtIso(ep.air_date))}</small><p>${esc(ep.overview || "")}</p></span>
        ${epoch ? `<span class="cw-mm-watched" title="${esc(fmtIso(new Date(epoch * 1000).toISOString()))}">${icon("check_circle")}Watched</span>` : "<span></span>"}
      </button>`;
    }).join("");
    return `${picker}<div class="cw-mm-eplist">${rows}</div>`;
  }

  function episodeDetail() {
    const { season, episode } = state.episode;
    const data = state.seasons.get(String(season));
    if (!data) void loadSeason(season);
    const ep = data?.episodes?.find(row => Number(row.episode) === episode);
    const back = `<button type="button" class="cw-mm-btn" data-mm-series>${icon("arrow_back")}Back to series</button>`;
    const title = `<p class="cw-mm-sub">${esc(state.meta?.title || state.item?.title)} · Season ${esc(season)} · Episode ${esc(episode)}</p><h2 id="cw-mm-title" tabindex="-1">${esc(ep?.name || "Episode " + episode)}</h2>`;
    if (!ep) return `${back}<div class="cw-mm-episode-detail">${title}${empty(!data || data.loading ? "Loading episode..." : "Episode details unavailable.")}</div>`;
    const watched = watchedMap().get(`${season}:${episode}`);
    const status = state.presenceLoading ? "Checking watch status..." : !state.presence ? "Watch status unavailable" : watched ? `Watched · ${fmtIso(new Date(watched * 1000).toISOString())}` : "No synced history or scrobbles";
    const providers = [["synced", "History"], ["scrobble", "Scrobbled"]].map(([key, label]) => {
      const row = state.presence?.[key]?.episodes?.find(row => Number(row.season) === season && Number(row.episode) === episode);
      if (!row) return "";
      return `<div data-mm-ep-live="${key}"><h4>${label}</h4><div class="cw-mm-live-chips">${(row.present || []).map(ref => providerChip(ref, false, !!itemActions())).join("")}</div></div>`;
    }).join("");
    const details = [fmtIso(ep.air_date), runtimeLabel(ep.runtime), Number(ep.vote_average) > 0 ? `TMDB ${Number(ep.vote_average).toFixed(1)}/10` : ""].filter(Boolean);
    return `<div class="cw-ep-page">${back}<article><div class="cw-ep-intro">${ep.has_still ? `<img class="cw-ep-art" src="${esc(stillUrl(season, episode).replace("size=w300", "size=w780"))}" alt="" onerror="${placeholder}">` : ""}<div class="cw-ep-copy"><p class="cw-ep-series">${esc(state.meta?.title || state.item?.title)} <span>Season ${esc(season)} · Episode ${esc(episode)}</span></p><h2 id="cw-mm-title" tabindex="-1">${esc(ep.name || "Episode " + episode)}</h2><div class="cw-ep-meta">${details.map(value => `<span>${esc(value)}</span>`).join("")}</div><p class="cw-ep-synopsis">${esc(ep.overview || "No synopsis available for this episode yet.")}</p></div></div><section class="cw-ep-activity"><div class="cw-ep-status"><span class="cw-ep-status-icon">${icon(watched ? "check_circle" : "visibility")}</span><div><h3>Your activity</h3><p>${esc(status)}</p></div></div>${providers ? `<div class="cw-ep-providers">${providers}</div>` : ""}</section>${episodeCredits(ep)}</article></div>`;

  }

  function episodeCredits(ep) {
    const guests = (ep.guest_stars || []).filter(person => person?.name);
    const crew = (ep.crew || []).filter(person => person?.name);
    if (!guests.length && !crew.length) return "";
    const credits = crew.map(person => `<span>${esc(person.name)}<small>${esc(person.job)}</small></span>`).join("");
    const people = guests.map(person => `<button type="button" class="cw-mm-person" ${person.id ? `data-mm-person="${esc(person.id)}"` : "disabled"}>
      ${person.profile_path ? `<img src="${esc(tmdbImage(person.profile_path, "w185"))}" alt="" loading="lazy">` : `<span class="cw-mm-initials">${esc(initialsOf(person.name))}</span>`}
      <strong>${esc(person.name)}</strong><small>${esc(person.character)}</small>
    </button>`).join("");
    return `<section class="cw-mm-box">${credits ? `<h4>Director &amp; writing credits</h4><div class="cw-mm-crew">${credits}</div>` : ""}${people ? `<h4>Guest cast</h4><div class="cw-mm-people">${people}</div>` : ""}</section>`;
  }

  function castTab() {
    const credits = state.meta?.credits;
    if (!credits) return empty(state.loading ? "Loading cast..." : "No cast information on TMDB.");
    const people = (credits.cast || []).map((person) => `<button type="button" class="cw-mm-person" ${person.id ? `data-mm-person="${esc(person.id)}"` : "disabled"}>
        ${person.profile_path ? `<img src="${esc(tmdbImage(person.profile_path, "w185"))}" alt="" loading="lazy">` : `<span class="cw-mm-initials">${esc(initialsOf(person.name))}</span>`}
        <strong>${esc(person.name)}</strong><small>${esc(person.character)}</small>
      </button>`).join("");
    const crew = (credits.crew || []).map((person) => `<span>${esc(person.name)}<small>${esc(person.job)}</small></span>`).join("");
    if (!people && !crew) return empty("No cast information on TMDB.");
    return `${people ? `<div class="cw-mm-people">${people}</div>` : ""}${crew ? `<div class="cw-mm-crew">${crew}</div>` : ""}`;
  }

  function videoUrl(video) {
    const key = encodeURIComponent(video?.key || "");
    const site = String(video?.site || "").toLowerCase();
    if (site === "youtube") return `https://www.youtube-nocookie.com/embed/${key}?autoplay=1&rel=0`;
    if (site === "vimeo") return `https://player.vimeo.com/video/${key}?autoplay=1`;
    return "";
  }

  function mediaTab() {
    const meta = state.meta;
    if (!meta) return empty(state.loading ? "Loading media..." : "No media information on TMDB.");
    const videos = (meta.videos || []).filter((video) => videoUrl(video)).slice(0, 12);
    const backdrops = (meta.images?.backdrop || [])
      .map((image) => (String(image?.url || "").match(/\/t\/p\/[^/]+(\/[A-Za-z0-9_-]+\.(?:jpg|jpeg|png))$/i) || [])[1])
      .filter(Boolean)
      .slice(0, 8);
    if (!videos.length && !backdrops.length) return empty("No trailers or images on TMDB.");
    const list = videos.map((video, index) => `<button class="cw-mm-video" type="button" data-mm-video="${index}">${icon("play_arrow")}<span><strong>${esc(video.name || video.type || "Video")}</strong><small>${esc([video.type, fmtIso(video.published_at)].filter(Boolean).join(" · "))}</small></span></button>`).join("");
    const gallery = backdrops.map((path) => `<img src="${esc(tmdbImage(path, "w780"))}" alt="" loading="lazy">`).join("");
    return `${list ? `<div class="cw-mm-videos">${list}</div>` : ""}${gallery ? `<div class="cw-mm-gallery">${gallery}</div>` : ""}`;
  }

  function similarTab() {
    const recs = state.meta?.recommendations;
    if (!recs) return empty(state.loading ? "Loading similar titles..." : "No similar titles on TMDB.");
    if (!recs.length) return empty("No similar titles on TMDB.");
    return `<div class="cw-mm-similar">${recs.map((rec, index) => `<button class="cw-mm-rec" type="button" data-mm-similar="${index}">
        <img src="${esc(tmdbImage(rec.poster_path, "w342") || "/assets/img/placeholder_poster.svg?v=2")}" alt="" loading="lazy" onerror="${placeholder}">
        <strong>${esc(rec.title)}</strong><small>${esc([rec.year, rec.type === "movie" ? "Movie" : "TV", rec.vote_average ? `★ ${Number(rec.vote_average).toFixed(1)}` : ""].filter(Boolean).join(" · "))}</small>
      </button>`).join("")}</div>`;
  }

  async function fetchExtra(kind, id) {
    const key = `${kind}:${id}`;
    if (extraMetadata.has(key)) return extraMetadata.get(key);
    const pending = (async () => {
      try {
        const res = await fetch(`/api/metadata/tmdb/${kind}?${kind}=${encodeURIComponent(id)}`, { credentials: "same-origin" });
        const data = await res.json();
        if (!res.ok || !data?.ok) throw new Error("metadata_unavailable");
        return data;
      } catch {
        extraMetadata.delete(key);
        return null;
      }
    })();
    extraMetadata.set(key, pending);
    if (extraMetadata.size > 32) extraMetadata.delete(extraMetadata.keys().next().value);
    return pending;
  }

  function collectionPanel() {
    const info = isMovie() ? state.meta?.detail?.belongs_to_collection : null;
    if (!info?.id) return "";
    const view = collectionView;
    const rows = view?.data?.parts || [];
    const known = rows.length && rows.every(row => typeof view.watched?.[row.id] === "boolean");
    const count = rows.filter(row => view?.watched?.[row.id] === true).length;
    const summary = known ? `${count} of ${rows.length} watched` : rows.length ? plural(rows.length, "film") : "Explore films";
    const contents = view?.loading ? empty("Loading collection...") : view?.error ? `${empty("Collection unavailable.")}<button type="button" class="cw-mm-btn" data-mm-collection-retry>Try again</button>` : rows.length ? rows.map(row => {
      const watched = view.watched?.[row.id];
      const current = String(row.id) === tmdbOf(state.item);
      const status = watched === true ? "Watched" : watched === false ? "Not watched" : "Status unavailable";
      return `<button type="button" data-mm-collection-movie="${esc(row.id)}" class="${current ? "is-current" : ""}"><img class="cw-collection-thumb" src="${esc(tmdbImage(row.poster_path, "w92") || "/assets/img/placeholder_poster.svg?v=2")}" alt="" loading="lazy" onerror="${placeholder}"><span class="cw-collection-title"><strong>${esc(row.title)}</strong><small>${esc([String(row.release_date || "").slice(0, 4), current ? "Viewing now" : ""].filter(Boolean).join(" · "))}</small></span><span class="cw-collection-status${watched === true ? " is-watched" : ""}">${icon(watched === true ? "check_circle" : "radio_button_unchecked")}<span>${status}</span></span>${icon("chevron_right")}</button>`;
    }).join("") : empty("No films listed in this collection.");
    return `<details class="cw-collection-list" data-mm-collection ${view?.open ? "open" : ""}><summary><span><small>MOVIE COLLECTION</small><strong>${esc(info.name || "Movie collection")}</strong></span><span class="cw-collection-summary">${esc(summary)}${icon("expand_more")}</span></summary><div class="cw-collection-rows">${contents}</div></details>`;
  }

  async function loadCollection(retry = false) {
    const id = state.meta?.detail?.belongs_to_collection?.id;
    if (!id || (!retry && collectionView)) return;
    const view = collectionView = { open: true, loading: true, watched: {} };
    const seq = state.seq;
    renderPanel();
    const data = await fetchExtra("collection", id);
    if (seq !== state.seq || collectionView !== view) return;
    if (data?.parts?.length) {
      try {
        const ids = data.parts.map(row => row.id).join(",");
        const url = (window.CW?.ProfileViewAs?.scope || String)(`/api/profile/movie-watch-status?tmdb=${encodeURIComponent(ids)}`);
        const res = await fetch(url, { credentials: "same-origin", cache: "no-store" });
        const result = await res.json();
        if (res.ok && result.ok) view.watched = result.watched || {};
      } catch {}
    }
    if (seq !== state.seq || collectionView !== view) return;
    Object.assign(view, { data, loading: false, error: !data });
    await scheduleRender(true);
  }

  async function openPerson(id, retry = false) {
    const view = personView = { id, loading: true, scroll: retry ? personView.scroll : root.querySelector(".cw-mm-body").scrollTop };
    const seq = state.seq;
    render();
    root.querySelector(".cw-mm-body").scrollTop = 0;
    root.querySelector("[data-mm-person-back]")?.focus({ preventScroll: true });
    const data = await fetchExtra("person", id);
    if (seq !== state.seq || personView !== view) return;
    Object.assign(view, { loading: false, data });
    await scheduleRender();
    root.querySelector("#cw-mm-title")?.focus({ preventScroll: true });
  }

  function personPanel() {
    const view = personView;
    const back = `<button type="button" class="cw-mm-btn" data-mm-person-back>${icon("arrow_back")}Back to ${esc(state.episode ? "episode" : state.meta?.title || state.item?.title || "title")}</button>`;
    if (view.loading) return `<section class="cw-person-view">${back}<h2 id="cw-mm-title">Cast details</h2>${empty("Loading filmography...")}</section>`;
    const person = view.data;
    if (!person) return `<section class="cw-person-view">${back}<h2 id="cw-mm-title">Cast details</h2>${empty("Person details unavailable.")}<button type="button" class="cw-mm-btn" data-mm-person-retry>Try again</button></section>`;
    return `<section class="cw-person-view">${back}<div class="cw-person-heading">${person.profile_path ? `<img src="${esc(tmdbImage(person.profile_path, "w185"))}" alt="">` : `<span class="cw-mm-initials">${esc(initialsOf(person.name))}</span>`}<div><small>CAST &amp; FILMOGRAPHY</small><h2 id="cw-mm-title" tabindex="-1">${esc(person.name)}</h2></div></div>${person.biography ? `<details class="cw-person-bio"><summary>About ${esc(person.name)}</summary><p>${esc(person.biography)}</p></details>` : ""}<h3>Also appears in</h3><div class="cw-person-films">${(person.credits || []).filter(row => String(row.id) !== tmdbOf(state.item) || row.type !== kindOf(state.item)).map(row => `<button type="button" data-mm-credit="${esc(row.type)}:${esc(row.id)}"><img src="${esc(tmdbImage(row.poster_path, "w92") || "/assets/img/placeholder_poster.svg?v=2")}" alt="" loading="lazy" onerror="${placeholder}"><span><strong>${esc(row.title)}</strong><small>${esc([String(row.date || "").slice(0, 4), row.type === "movie" ? "Movie" : "Series", row.character].filter(Boolean).join(" · "))}</small></span>${icon("chevron_right")}</button>`).join("") || empty("No other credits available.")}</div></section>`;
  }

  function onClick(event) {
    const target = event.target;
    const actionNode = target?.closest?.("[data-ia-chip],[data-ia-cover]");
    if (actionNode) {
      void openItemActions(actionNode);
      return;
    }
    const clearAll = target?.closest?.("[data-mm-clear-all]");
    if (clearAll) {
      openEverything(clearAll);
      return;
    }
    if (target?.closest?.("[data-mm-person-back]")) {
      const view = personView;
      personView = null;
      render();
      root.querySelector(".cw-mm-body").scrollTop = view.scroll;
      root.querySelector(`[data-mm-person="${view.id}"]`)?.focus({ preventScroll: true });
      return;
    }
    if (target?.closest?.("[data-mm-person-retry]")) { void openPerson(personView.id, true); return; }
    if (target?.closest?.("[data-mm-collection-retry]")) { void loadCollection(true); return; }
    const person = target?.closest?.("[data-mm-person]");
    if (person) { void openPerson(person.dataset.mmPerson); return; }
    const collectionMovie = target?.closest?.("[data-mm-collection-movie]");
    if (collectionMovie) {
      const row = collectionView?.data?.parts?.find(row => String(row.id) === collectionMovie.dataset.mmCollectionMovie);
      if (row) void navigate({ type: "movie", tmdb: String(row.id), title: row.title, year: String(row.release_date || "").slice(0, 4) });
      return;
    }
    const credit = target?.closest?.("[data-mm-credit]");
    if (credit) {
      const row = personView?.data?.credits?.find(row => `${row.type}:${row.id}` === credit.dataset.mmCredit);
      if (row) void navigate({ type: row.type, tmdb: String(row.id), title: row.title, year: String(row.date || "").slice(0, 4) });
      return;
    }

    if (target?.closest?.("[data-mm-close]")) {
      close();
      return;
    }
    if (target?.closest?.("[data-mm-history-more]")) {
      state.historyExpanded = !state.historyExpanded;
      renderPanel();
      root.querySelector("[data-mm-history-more]")?.focus({ preventScroll: true });
      return;
    }
    const statusChoice = target?.closest?.("[data-mm-trk-status]");
    if (statusChoice && !statusChoice.disabled && !state.statusBusy && state.tracker?.onAction) {
      const seq = state.seq;
      state.statusBusy = true;
      const statusPicker = root.querySelector("[data-mm-status-picker]");
      if (statusPicker) statusPicker.open = false;
      renderPanel();
      root.querySelector("[data-mm-status-picker] summary")?.focus({ preventScroll: true });
      Promise.resolve(state.tracker.onAction("status", statusChoice.dataset.mmTrkStatus)).finally(() => {
        if (seq !== state.seq || root.hidden) return;
        const restoreFocus = root.querySelector("[data-mm-status-picker]")?.contains(document.activeElement);
        state.statusBusy = false;
        renderPanel();
        if (restoreFocus) root.querySelector("[data-mm-status-picker] summary")?.focus({ preventScroll: true });
      });
      return;
    }
    const picker = root.querySelector("[data-mm-status-picker][open]");
    if (picker && !picker.contains(target)) picker.open = false;
    const trk = target?.closest?.("[data-mm-trk],[data-mm-trk-edit],[data-mm-trk-delete]");
    if (trk && state.tracker?.onAction) {
      const action = trk.dataset.mmTrk || (trk.dataset.mmTrkEdit ? "edit" : "delete");
      state.tracker.onAction(action, trk.dataset.mmTrkEdit || trk.dataset.mmTrkDelete || "");
      return;
    }
    if (target?.closest?.("[data-mm-series]")) {
      const selected = state.episode;
      state.episode = null;
      render();
      root.querySelector(".cw-mm-body").scrollTop = state.seriesScroll;
      root.querySelector(`[data-mm-episode="${selected.season}:${selected.episode}"]`)?.focus({ preventScroll: true });
      return;
    }
    const episode = target?.closest?.("[data-mm-episode]");
    if (episode) {
      const [season, number] = episode.dataset.mmEpisode.split(":").map(Number);
      state.seriesScroll = root.querySelector(".cw-mm-body").scrollTop;
      state.episode = { season, episode: number };
      render();
      root.querySelector(".cw-mm-body").scrollTop = 0;
      root.querySelector("#cw-mm-title")?.focus({ preventScroll: true });
      return;
    }
    const tab = target?.closest?.("[data-mm-tab]");
    if (tab) {
      state.tab = tab.dataset.mmTab || "overview";
      renderPanel();
      return;
    }
    if (target?.closest?.("[data-mm-trailer]")) {
      void window.CW?.Trailer?.openFor?.(state.item, state.meta);
      return;
    }
    if (target?.closest?.("[data-mm-seasons-all]")) {
      state.seasonPickerOpen = !state.seasonPickerOpen;
      renderPanel();
      return;
    }
    const seasonToggle = target?.closest?.("[data-mm-season-toggle]");
    if (seasonToggle) {
      const number = Number(seasonToggle.dataset.mmSeasonToggle);
      state.overviewSeason = state.overviewSeason === number ? null : number;
      state.seasonEpisodesExpanded = false;
      renderPanel();
      if (state.overviewSeason != null) void loadSeason(number);
      return;
    }
    if (target?.closest?.("[data-mm-season-more]")) {
      state.seasonEpisodesExpanded = !state.seasonEpisodesExpanded;
      renderPanel();
      return;
    }
    const seasonRetry = target?.closest?.("[data-mm-season-retry]");
    if (seasonRetry) {
      const number = Number(seasonRetry.dataset.mmSeasonRetry);
      state.seasons.delete(String(number));
      void loadSeason(number);
      renderPanel();
      return;
    }
    const goto = target?.closest?.("[data-mm-goto]");
    if (goto) {
      state.tab = goto.dataset.mmGoto || "overview";
      renderPanel();
      return;
    }
    const season = target?.closest?.("[data-mm-season]");
    if (season) {
      state.season = Number(season.dataset.mmSeason);
      renderPanel();
      return;
    }
    const video = target?.closest?.("[data-mm-video]");
    if (video) {
      const entry = (state.meta?.videos || []).filter((row) => videoUrl(row))[Number(video.dataset.mmVideo)];
      if (entry) window.CW?.Trailer?.open?.(videoUrl(entry), entry.name || "Trailer");
      return;
    }
    const similar = target?.closest?.("[data-mm-similar]");
    if (similar) {
      const rec = (state.meta?.recommendations || [])[Number(similar.dataset.mmSimilar)];
      if (!rec?.id) return;
      const type = rec.type === "movie" ? "movie" : "show";
      void navigate({ type, media_type: rec.type, tmdb: String(rec.id), ids: { tmdb: String(rec.id) }, title: rec.title, year: rec.year });
    }
  }

  (window.CW ||= {}).ProfileMediaModal = { open, close, canOpen, updateTracker, isOpen: () => !!root && !root.hidden, setStyle: setMediaCardStyle };
})();
