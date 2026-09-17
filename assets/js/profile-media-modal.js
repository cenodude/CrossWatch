/* assets/js/profile-media-modal.js */
/* CrossWatch - media details modal for the profile page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const numberFmt = new Intl.NumberFormat(window.__CW_LOCALE || navigator.language || undefined);
  const compactFmt = new Intl.NumberFormat(window.__CW_LOCALE || navigator.language || undefined, { notation: "compact", maximumFractionDigits: 1 });
  const dateFmt = (() => {
    try {
      return new Intl.DateTimeFormat(window.__CW_LOCALE || navigator.language || undefined, { day: "numeric", month: "short", year: "numeric" });
    } catch {
      return new Intl.DateTimeFormat(undefined, { day: "numeric", month: "short", year: "numeric" });
    }
  })();
  const TABS = [["overview", "Overview"], ["episodes", "Episodes"], ["cast", "Cast & Crew"], ["media", "Media"], ["similar", "Similar"]];
  const LIVES = [
    ["watchlist", "bookmark", "Watchlist"],
    ["synced", "history", "History"],
    ["ratings", "star", "Ratings"],
    ["collection", "video_library", "Collection"],
    ["scrobble", "sensors", "Scrobbled"],
  ];
  const state = { item: null, meta: null, presence: null, progress: undefined, loading: false, tab: "overview", seq: 0, seasons: new Map(), season: null, lastFocus: null };
  let root = null;

  const kindOf = (item) => (String(item?.type || item?.media_type || "").toLowerCase() === "movie" ? "movie" : "show");
  const tmdbOf = (item) => String(window.CW?.Meta?.tmdbId?.(item) || item?.tmdb || item?.ids?.tmdb || "").trim();
  const artType = (item) => (kindOf(item) === "movie" ? "movie" : "tv");
  const isMovie = () => kindOf(state.item) === "movie";
  const tmdbImage = (path, size) => (path ? `/art/tmdb/image?path=${encodeURIComponent(path)}&size=${encodeURIComponent(size)}` : "");
  const posterUrl = (item) => `/art/tmdb/${artType(item)}/${encodeURIComponent(tmdbOf(item))}?size=w500`;
  const backdropUrl = (item) => `/art/tmdb/${artType(item)}/${encodeURIComponent(tmdbOf(item))}?kind=backdrop&size=w1280`;
  const stillUrl = (season, episode) => `/art/tmdb/tv/${encodeURIComponent(tmdbOf(state.item))}?kind=still&season=${encodeURIComponent(season)}&episode=${encodeURIComponent(episode)}&size=w300`;
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const plural = (count, word) => `${numberFmt.format(Number(count) || 0)} ${word}${Number(count) === 1 ? "" : "s"}`;
  const placeholder = "this.onerror=null;this.src='/assets/img/placeholder_poster.svg'";

  function fmtIso(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const parsed = Date.parse(raw.length <= 10 ? `${raw}T00:00:00Z` : raw);
    return Number.isFinite(parsed) ? dateFmt.format(new Date(parsed)) : "";
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

  function providerChip(ref, missing = false) {
    const provider = String(ref?.provider || "");
    const instance = String(ref?.instance || "default");
    const name = window.CW?.ProviderMeta?.label?.(provider) || provider;
    const logo = window.CW?.ProviderMeta?.logoPath?.(provider) || "";
    const title = `${name}${instance !== "default" ? ` · ${instance}` : ""}${missing ? " · missing" : ""}`;
    const mark = missing ? "" : ref?.rating != null ? `<em>${esc(ref.rating)}</em>` : `<span class="cw-mm-provider-mark material-symbols-rounded" aria-hidden="true">check</span>`;
    return `<span class="cw-mm-provider${missing ? " is-missing" : ""}" title="${esc(title)}">${logo ? `<img src="${esc(logo)}" alt="" loading="lazy">` : `<b>${esc(name.slice(0, 2))}</b>`}${mark}</span>`;
  }

  function ensure() {
    if (root) return root;
    root = document.createElement("div");
    root.id = "cw-media-modal";
    root.className = "cw-mm";
    root.hidden = true;
    root.innerHTML = `<div class="cw-mm-backdrop" data-mm-close></div>
      <section class="cw-mm-card" role="dialog" aria-modal="true" aria-labelledby="cw-mm-title" tabindex="-1">
        <div class="cw-mm-hero" aria-hidden="true"><div class="cw-mm-hero-art"></div></div>
        <button class="cw-mm-close" type="button" data-mm-close aria-label="Close">${icon("close")}</button>
        <div class="cw-mm-body"></div>
      </section>`;
    document.body.appendChild(root);
    root.addEventListener("click", onClick);
    document.addEventListener("keydown", (event) => {
      if (root.hidden || event.key !== "Escape") return;
      if (document.getElementById("cw-trailer")?.classList.contains("show")) return;
      event.preventDefault();
      close();
    }, true);
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
        const res = await fetch((window.CW?.ProfileViewAs?.scope || String)(`/api/playback_progress/items?tmdb=${encodeURIComponent(tmdb)}&page=1&page_size=50`), { credentials: "same-origin", cache: "no-store" });
        const data = await res.json().catch(() => null);
        const match = (Array.isArray(data?.items) ? data.items : []).find((entry) => {
          if (String(entry?.media_type || "").toLowerCase() !== "movie") return false;
          const records = Array.isArray(entry.records) && entry.records.length ? entry.records : [entry];
          return records.some((record) => String(record?.ids?.tmdb || "") === tmdb);
        });
        const records = match ? (Array.isArray(match.records) && match.records.length ? match.records : [match]) : [];
        rows = records.map((record) => ({
          provider: String(record.provider || ""),
          instance: String(record.instance_id || "default"),
          label: record.provider_label || record.provider || "",
          instanceLabel: record.instance_label || "",
          pct: Number(record.progress_percent) || 0,
          remaining: Number(record.remaining_seconds) || 0,
          updated: record.updated_at || record.progress_at || "",
        }));
      } catch {
        rows = [];
      }
    }
    if (seq !== state.seq) return;
    state.progress = rows;
    if (state.tab === "overview") renderPanel();
  }

  function canOpen(item) {
    return !!tmdbOf(item);
  }

  async function open(item) {
    if (!canOpen(item)) return false;
    ensure();
    const seq = ++state.seq;
    const sameTitle = state.item && tmdbOf(state.item) === tmdbOf(item) && kindOf(state.item) === kindOf(item);
    Object.assign(state, {
      item,
      meta: window.CW?.Meta?.peek?.(item) || null,
      presence: null,
      loading: true,
      tab: "overview",
      seasons: sameTitle ? state.seasons : new Map(),
      season: null,
      progress: kindOf(item) === "movie" ? null : undefined,
    });
    if (root.hidden) state.lastFocus = document.activeElement;
    root.hidden = false;
    document.documentElement.classList.add("cw-mm-open");
    root.querySelector(".cw-mm-hero-art").style.backgroundImage = `url("${backdropUrl(item)}")`;
    root.querySelector(".cw-mm-body").scrollTop = 0;
    render();
    root.querySelector(".cw-mm-card")?.focus({ preventScroll: true });
    if (kindOf(item) === "movie") void loadProgress(item, seq);
    const [meta, presence] = await Promise.all([loadMeta(item), loadPresence(item)]);
    if (seq !== state.seq || root.hidden) return true;
    state.meta = meta || state.meta;
    state.presence = presence;
    state.loading = false;
    render();
    return true;
  }

  function close() {
    if (!root || root.hidden) return;
    state.seq += 1;
    root.hidden = true;
    document.documentElement.classList.remove("cw-mm-open");
    const focus = state.lastFocus;
    state.lastFocus = null;
    if (focus && typeof focus.focus === "function") focus.focus({ preventScroll: true });
  }

  function render() {
    const body = root?.querySelector(".cw-mm-body");
    if (!body) return;
    body.innerHTML = `${tagline()}${top()}${livesCard()}${tabs()}<div class="cw-mm-panel" role="tabpanel">${panel()}</div>`;
  }

  function renderPanel() {
    const host = root?.querySelector(".cw-mm-panel");
    if (host) host.innerHTML = panel();
    root?.querySelectorAll("[data-mm-tab]").forEach((btn) => {
      const active = btn.dataset.mmTab === state.tab;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-selected", String(active));
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
    return `<div class="cw-mm-top">
      <img class="cw-mm-poster" src="${esc(posterUrl(item))}" alt="" decoding="async" onerror="${placeholder}">
      <div class="cw-mm-heading">
        <h2 id="cw-mm-title">${esc(title)}</h2>
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
    if (window.CW?.Trailer?.has?.(meta)) out.push(`<button class="cw-mm-btn is-primary" type="button" data-mm-trailer>${icon("play_arrow")}Watch trailer</button>`);
    out.push(`<a class="cw-mm-btn" href="https://www.themoviedb.org/${artType(item)}/${encodeURIComponent(tmdb)}" target="_blank" rel="noopener">TMDB</a>`);
    if (imdb) out.push(`<a class="cw-mm-btn" href="https://www.imdb.com/title/${encodeURIComponent(imdb)}/" target="_blank" rel="noopener">IMDb</a>`);
    const score = Number(meta?.detail?.vote_average) || (Number(meta?.score) || 0) / 10;
    if (score > 0) {
      const votes = Number(meta?.vote_count) || 0;
      out.push(`<span class="cw-mm-score" title="TMDB score${votes ? ` from ${numberFmt.format(votes)} votes` : ""}">${ring(score.toFixed(1), score * 10, "blue")}<span><strong>TMDB</strong><small>${votes ? `${compactFmt.format(votes)} votes` : "Score"}</small></span></span>`);
    }
    const rating = state.presence?.ratings;
    const userScore = Number(rating?.rating);
    if (rating?.rating != null && Number.isFinite(userScore)) {
      const hint = rating.agree === false ? "Scores differ" : "Same everywhere";
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
    if (!presence) {
      return `<section class="cw-mm-lives"><h3>Where it lives</h3><p class="cw-mm-muted">${state.loading ? "Checking your providers..." : "Sync status is not available."}</p></section>`;
    }
    const tiles = LIVES.filter(([key]) => key in presence).map(([key, symbol, label]) => {
      const entry = presence[key];
      const present = entry?.present || [];
      const synced = present.length > 0;
      const chips = synced
        ? `<div class="cw-mm-live-chips">${present.map((ref) => providerChip(ref)).join("")}${(entry.missing || []).map((ref) => providerChip(ref, true)).join("")}</div>`
        : "";
      return `<div class="cw-mm-live${synced ? " is-on" : ""}">
        <div class="cw-mm-live-head">${icon(symbol)}<strong>${esc(label)}</strong><span>${esc(synced ? liveSummary(key, entry) : "Not synced")}</span></div>
        ${chips}
      </div>`;
    }).join("");
    return `<section class="cw-mm-lives"><h3>Where it lives</h3><div class="cw-mm-live-grid">${tiles}</div></section>`;
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
    return overviewTab();
  }

  const empty = (text) => `<div class="cw-mm-empty">${esc(text)}</div>`;

  const watchedMap = () => new Map((state.presence?.synced?.episodes || []).map((row) => [`${row.season}:${row.episode}`, row.epoch]));

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
    if (!seasons.length) return { first: null, label: "", second: null };
    const known = new Set(seasons.map((row) => Number(row.season)));
    const watched = (state.presence?.synced?.episodes || [])
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
    return `<div class="cw-mm-ep">
      <h5>${esc(label)}${badge}</h5>
      <span class="cw-mm-ep-still"><img src="${esc(stillUrl(pos.season, pos.episode))}" alt="" loading="lazy" onerror="${placeholder}">${ep.runtime ? `<em>${esc(runtimeLabel(ep.runtime))}</em>` : ""}</span>
      <span><strong>S${esc(pos.season)}.E${esc(pos.episode)} – ${esc(ep.name || "Episode")}</strong><small>${esc(fmtIso(ep.air_date))}</small><p>${esc(ep.overview || "")}</p></span>
    </div>`;
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

  function coverageRows() {
    const presence = state.presence;
    if (!presence) return `<p class="cw-mm-muted">${state.loading ? "Checking your providers..." : "Sync status is not available."}</p>`;
    const rows = [["watchlist", "Watchlist", "violet", "bookmark"], ["synced", "History", "blue", "history"], ["ratings", "Ratings", "amber", "star"], ["collection", "Collection", "green", "video_library"]]
      .filter(([key]) => key in presence)
      .map(([key, label, tone, symbol]) => {
        const entry = presence[key];
        const have = entry?.present?.length || 0;
        const total = key === "collection" ? have : have + (entry?.missing?.length || 0);
        const pct = total ? Math.round((have / total) * 100) : 0;
        const value = !have ? "—" : key === "collection" ? String(have) : `${have}/${total}`;
        const hint = !have ? "Not synced" : key === "collection" ? plural(have, "provider") : `On ${have} of ${total} providers`;
        return `<div class="cw-mm-cover-row${have ? "" : " is-empty"}" style="--tone:var(--mm-${tone})" title="${esc(hint)}">${icon(symbol)}<span class="cw-mm-cover-label">${esc(label)}</span><span class="cw-mm-cover-track"><i style="width:${pct}%"></i></span><b>${esc(value)}</b></div>`;
      }).join("");
    return `<div class="cw-mm-cover">${rows}</div>`;
  }

  function movieProgressBox() {
    const history = state.presence?.synced;
    const rows = state.progress;
    const records = Array.isArray(rows) ? [...rows].sort((a, b) => b.pct - a.pct) : [];
    const best = records[0] || null;
    const watched = Number(history?.count) || 0;
    const pct = best ? Math.round(best.pct) : watched ? 100 : 0;
    const summary = best ? `<b>${pct}%</b>` : watched ? "<b>Watched</b>" : rows === null ? "Checking..." : "Not started";
    const list = records.map((row) => {
      const logo = window.CW?.ProviderMeta?.logoPath?.(row.provider) || "";
      const name = window.CW?.ProviderMeta?.label?.(row.provider) || row.label || row.provider;
      const profile = row.instance !== "default" ? row.instanceLabel || row.instance : "";
      const updated = Date.parse(String(row.updated || ""));
      const meta = [row.remaining ? `${runtimeLabel(Math.round(row.remaining / 60))} left` : "", Number.isFinite(updated) ? `paused ${relTime(Math.floor(updated / 1000))}` : ""].filter(Boolean).join(" · ");
      const value = Math.round(row.pct);
      return `<div class="cw-mm-progress-row">
        <span class="cw-mm-progress-logo">${logo ? `<img src="${esc(logo)}" alt="" loading="lazy">` : `<b>${esc(name.slice(0, 2))}</b>`}</span>
        <span class="cw-mm-progress-copy"><strong>${esc(name)}${profile ? ` · ${esc(profile)}` : ""}</strong><small>${esc(meta)}</small></span>
        <span class="cw-mm-progress-mini"><i style="width:${value}%"></i></span>
        <b>${value}%</b>
      </div>`;
    }).join("");
    const lastSeen = history?.last_epoch ? [`Last watched ${relTime(history.last_epoch)}`, dateFmt.format(new Date(history.last_epoch * 1000))] : [];
    const watchline = watched
      ? `<div class="cw-mm-watchline is-watched">${icon("check_circle")}
          <span class="cw-mm-watchline-copy"><strong>${esc(watched === 1 ? "Watched once" : `Watched ${plural(watched, "time")}`)}</strong><small>${esc(lastSeen.join(" · ") || "In your synced history")}</small></span>
          <span class="cw-mm-watchline-count"><b>${esc(numberFmt.format(watched))}</b><small>${watched === 1 ? "play" : "plays"}</small></span>
        </div>`
      : `<div class="cw-mm-watchline">${icon("visibility")}<span class="cw-mm-watchline-copy"><strong>Not watched yet</strong><small>Not in your synced history</small></span></div>`;
    return `<div class="cw-mm-box"><h4>Watch progress <small>${summary}</small></h4>
      <div class="cw-mm-progress${!best && watched ? " is-done" : ""}"><i style="width:${pct}%"></i></div>
      ${list ? `<div class="cw-mm-progress-rows">${list}</div>` : ""}
      ${watchline}${castStrip()}</div>`;
  }

  function overviewTab() {
    const { meta, presence } = state;
    const detail = meta?.detail || {};
    let main;
    if (isMovie()) {
      main = movieProgressBox();
    } else {
      const total = Number(detail.number_of_episodes) || 0;
      const watched = Array.isArray(presence?.synced?.episodes) ? presence.synced.episodes.length : 0;
      const pct = total ? Math.min(100, Math.round((watched / total) * 100)) : 0;
      const next = upNext();
      const first = next.first ? episodeSlot(next.label, next.first) : "";
      let second = next.second ? episodeSlot("Next episode", next.second) : "";
      const upcoming = detail.next_episode_to_air;
      if (!second && next.label === "Last watched" && upcoming?.season_number != null && upcoming?.episode_number != null) {
        second = episodeSlot("Next episode", { season: upcoming.season_number, episode: upcoming.episode_number }, upcoming);
      }
      const pair = `${first}${second}`;
      main = `<div class="cw-mm-box"><h4>Watch progress <small>${numberFmt.format(watched)}/${numberFmt.format(total)} episodes <b>${pct}%</b></small></h4>
        <div class="cw-mm-progress"><i style="width:${pct}%"></i></div>
        ${pair ? `<div class="cw-mm-episodes-pair">${pair}</div>` : empty(meta ? "No episode information on TMDB." : "Loading episodes...")}${seasonProgress()}</div>`;
    }
    return `<div class="cw-mm-grid-2">${main}<div class="cw-mm-stack">
      <div class="cw-mm-box"><h4>Details</h4>${detailTiles()}</div>
      <div class="cw-mm-box"><h4>Sync coverage <small>Providers holding it</small></h4>${coverageRows()}</div>
    </div></div>`;
  }

  const initialsOf = (name) => String(name || "?").split(/\s+/).map((part) => part[0] || "").join("").slice(0, 2).toUpperCase();

  function seasonProgress() {
    const seasons = seasonList();
    if (!seasons.length) return "";
    const counts = new Map();
    for (const row of state.presence?.synced?.episodes || []) {
      counts.set(Number(row.season), (counts.get(Number(row.season)) || 0) + 1);
    }
    const shown = seasons.slice(0, 8);
    const rows = shown.map((row) => {
      const number = Number(row.season);
      const total = Number(row.episode_count) || 0;
      const watched = Math.min(total, counts.get(number) || 0);
      const pct = total ? Math.round((watched / total) * 100) : 0;
      const done = total > 0 && watched >= total;
      return `<button type="button" class="cw-mm-season-row${done ? " is-done" : watched ? " is-started" : ""}" data-mm-season-jump="${esc(number)}" title="${esc(row.name || `Season ${number}`)}">
        <span class="cw-mm-season-name">${esc(number === 0 ? "Specials" : `Season ${number}`)}</span>
        <span class="cw-mm-season-track"><i style="width:${pct}%"></i></span>
        <span class="cw-mm-season-count">${done ? icon("check_circle") : ""}${numberFmt.format(watched)}/${numberFmt.format(total)}</span>
      </button>`;
    }).join("");
    const more = seasons.length > shown.length ? `<button type="button" data-mm-goto="episodes">All ${numberFmt.format(seasons.length)} seasons</button>` : "";
    return `<div class="cw-mm-seasons-progress"><h5>Seasons${more}</h5>${rows}</div>`;
  }

  function castStrip() {
    const cast = (state.meta?.credits?.cast || []).slice(0, 7);
    if (!cast.length) return "";
    const faces = cast.map((person) => `<button type="button" class="cw-mm-cast-face" data-mm-goto="cast" title="${esc(`${person.name}${person.character ? ` as ${person.character}` : ""}`)}">
        ${person.profile_path ? `<img src="${esc(tmdbImage(person.profile_path, "w185"))}" alt="" loading="lazy">` : `<span class="cw-mm-initials">${esc(initialsOf(person.name))}</span>`}
        <small>${esc(person.name)}</small>
      </button>`).join("");
    return `<div class="cw-mm-cast-strip"><h5>Top cast<button type="button" data-mm-goto="cast">See all</button></h5><div>${faces}</div></div>`;
  }

  function seasonList() {
    const seasons = (state.meta?.detail?.seasons || []).filter((row) => Number(row?.episode_count) > 0);
    const regular = seasons.filter((row) => Number(row.season) > 0);
    return regular.length ? regular : seasons;
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
    if (state.tab === "overview" || (state.tab === "episodes" && String(state.season) === key)) renderPanel();
  }

  function episodesTab() {
    if (!state.meta) return empty("Loading seasons...");
    const seasons = seasonList();
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
      return `<div class="cw-mm-eprow${epoch ? " is-watched" : ""}">
        <span class="cw-mm-ep-still">${ep.has_still ? `<img src="${esc(stillUrl(state.season, ep.episode))}" alt="" loading="lazy" onerror="${placeholder}">` : ""}${ep.runtime ? `<em>${esc(runtimeLabel(ep.runtime))}</em>` : ""}</span>
        <span><strong>S${esc(state.season)}.E${esc(ep.episode)} – ${esc(ep.name || "Episode")}</strong><small>${esc(fmtIso(ep.air_date))}</small><p>${esc(ep.overview || "")}</p></span>
        ${epoch ? `<span class="cw-mm-watched" title="${esc(fmtIso(new Date(epoch * 1000).toISOString()))}">${icon("check_circle")}Watched</span>` : "<span></span>"}
      </div>`;
    }).join("");
    return `${picker}<div class="cw-mm-eplist">${rows}</div>`;
  }

  function castTab() {
    const credits = state.meta?.credits;
    if (!credits) return empty(state.loading ? "Loading cast..." : "No cast information on TMDB.");
    const people = (credits.cast || []).map((person) => `<div class="cw-mm-person">
        ${person.profile_path ? `<img src="${esc(tmdbImage(person.profile_path, "w185"))}" alt="" loading="lazy">` : `<span class="cw-mm-initials">${esc(initialsOf(person.name))}</span>`}
        <strong>${esc(person.name)}</strong><small>${esc(person.character)}</small>
      </div>`).join("");
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
    if (!meta) return empty("Loading media...");
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
        <img src="${esc(tmdbImage(rec.poster_path, "w342") || "/assets/img/placeholder_poster.svg")}" alt="" loading="lazy" onerror="${placeholder}">
        <strong>${esc(rec.title)}</strong><small>${esc([rec.year, rec.type === "movie" ? "Movie" : "TV", rec.vote_average ? `★ ${Number(rec.vote_average).toFixed(1)}` : ""].filter(Boolean).join(" · "))}</small>
      </button>`).join("")}</div>`;
  }

  function onClick(event) {
    const target = event.target;
    if (target?.closest?.("[data-mm-close]")) {
      close();
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
    const jump = target?.closest?.("[data-mm-season-jump]");
    if (jump) {
      state.tab = "episodes";
      state.season = Number(jump.dataset.mmSeasonJump);
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
      void open({ type, media_type: rec.type, tmdb: String(rec.id), ids: { tmdb: String(rec.id) }, title: rec.title, year: rec.year });
    }
  }

  (window.CW ||= {}).ProfileMediaModal = { open, close, canOpen };
})();
