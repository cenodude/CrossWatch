/* assets/js/profile-page.js */
/* CrossWatch managed user profile page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
  const API_TIMEOUT_MS = 60 * 1000;
  const VIEW_AS_PARAM = "as";
  const VIEW_AS_PATHS = [
    "/api/profile/collection", "/api/profile/history", "/api/profile/ratings", "/api/profile/watchlist", "/api/profile/title",
    "/api/playback_progress/", "/api/watch/currently_watching", "/api/state/wall", "/api/insights", "/api/status",
    "/api/dashboard/widgets", "/api/events/feed",
  ];
  const viewAsState = { id: "", label: "" };
  const viewingAs = () => !!viewAsState.id;
  const scopeUrl = (url) => {
    const text = String(url || "");
    if (!viewAsState.id || !VIEW_AS_PATHS.some((path) => text.startsWith(path))) return text;
    const parsed = new URL(text, window.location.origin);
    if (!parsed.searchParams.has("user_profile")) parsed.searchParams.set("user_profile", viewAsState.id);
    return `${parsed.pathname}${parsed.search}`;
  };
  window.CW = window.CW || {};
  window.CW.ProfileViewAs = {
    get id() { return viewAsState.id; },
    get active() { return viewingAs(); },
    scope: scopeUrl,
  };

  const api = async (url, opt = {}, ms = API_TIMEOUT_MS) => {
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    const timer = controller ? window.setTimeout(() => controller.abort("timeout"), ms) : 0;
    let res;
    try {
      const method = String(opt.method || "GET").toUpperCase();
      res = await fetch(method === "GET" ? scopeUrl(url) : url, { cache: "no-store", credentials: "same-origin", signal: controller?.signal, ...opt });
    } finally {
      if (timer) window.clearTimeout(timer);
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data?.ok === false) throw new Error(data?.error || (typeof data?.detail === "string" ? data.detail : "") || `HTTP ${res.status}`);
    return data;
  };
  const OVERVIEW_CACHE_TTL_MS = 10 * 60 * 1000;
  const profileCacheKey = (name) => {
    const shell = $(".cw-profile-shell");
    const id = shell?.dataset?.profileId || shell?.dataset?.username || document.documentElement?.dataset?.cwProfileId || "self";
    return `cw.profile.${id}${viewAsState.id ? `.as-${viewAsState.id}` : ""}.${name}.v1`;
  };
  const readCache = (key, ttl = OVERVIEW_CACHE_TTL_MS) => {
    try {
      const entry = JSON.parse(localStorage.getItem(key) || "null");
      if (!entry || !entry.payload || Date.now() - Number(entry.t || 0) > ttl) return null;
      return entry.payload;
    } catch {
      return null;
    }
  };
  const writeCache = (key, payload) => {
    try { localStorage.setItem(key, JSON.stringify({ t: Date.now(), payload })); } catch {}
  };
  const readAnyCache = (key) => {
    try {
      const entry = JSON.parse(localStorage.getItem(key) || "null");
      return entry && entry.payload ? entry.payload : null;
    } catch {
      return null;
    }
  };
  const profileRouteSegment = (value) => {
    try {
      value = decodeURIComponent(String(value || ""));
    } catch {
      value = String(value || "");
    }
    return value.trim().toLowerCase().replace(/-/g, "_");
  };
  const redirectProfileAppHash = () => {
    if (window.location?.pathname !== "/profile") return false;
    const raw = String(window.location?.hash || "");
    if (!raw) return false;
    const tab = profileRouteSegment(raw.replace(/^#\/?/, "").split("?")[0].split("/")[0]);
    if (tab === "playback_progress") {
      window.history.replaceState(null, "", "/profile#playback");
      return false;
    }
    const appTabs = new Set(["main", "snapshots", "playlists", "editor", "settings"]);
    if (!appTabs.has(tab)) return false;
    const doc = document.documentElement;
    const isAdmin = doc?.dataset?.cwRole !== "user";
    const canWrite = doc?.dataset?.cwPermWrite === "on";
    if (isAdmin) {
      window.location.replace(`/${raw}`);
      return true;
    }
    if (canWrite) {
      window.location.replace(`/?main=1${raw}`);
      return true;
    }
    return false;
  };
  if (redirectProfileAppHash()) return;
  window.addEventListener("hashchange", redirectProfileAppHash);
  const post = (url, body) => api(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) });
  const del = (url) => api(url, { method: "DELETE" });
  const providerLabel = (provider) => window.CW?.ProviderMeta?.label?.(provider) || String(provider || "").toUpperCase();
  const providerKey = (provider) => window.CW?.ProviderMeta?.keyOf?.(provider) || String(provider || "").trim().toUpperCase();
  const providerLogo = (provider) => window.CW?.ProviderMeta?.logoPath?.(provider) || "";
  const providerLogLogo = (provider) => window.CW?.ProviderMeta?.logLogoPath?.(provider) || providerLogo(provider);
  const visibleProviderLabel = (provider) => {
    const raw = String(provider || "").trim();
    if (!raw || raw === "?" || /^unknown$/i.test(raw) || /^none$/i.test(raw)) return "";
    const label = String(providerLabel(raw) || "").trim();
    return (!label || label === "?" || /^unknown$/i.test(label) || /^none$/i.test(label)) ? "" : label;
  };
  const providerIconHtml = (provider) => {
    const label = visibleProviderLabel(provider);
    if (!label) return "";
    const key = providerKey(provider).toLowerCase().replace(/[^a-z0-9-]+/g, "");
    const logo = providerLogo(provider);
    const icon = logo
      ? `<img class="cw-profile-provider-logo" src="${esc(logo)}" alt="" loading="lazy" onerror="this.onerror=null;this.hidden=true;this.nextElementSibling.hidden=false">`
      : "";
    const fallbackHidden = logo ? " hidden" : "";
    return `<span class="cw-profile-provider-badge cw-profile-provider-badge--icon" data-provider="${esc(key)}" title="${esc(label)}" aria-label="${esc(label)}">${icon}<span class="cw-profile-provider-fallback"${fallbackHidden}>${esc(label.slice(0, 2))}</span></span>`;
  };
  const providerName = (value) => {
    if (!value) return "";
    if (typeof value === "string") return value;
    if (typeof value === "object") return String(value.provider || value.name || value.key || "");
    return String(value);
  };
  const providerOf = (item) => providerName(item?.source) || providerName(item?.provider) || providerName(item?.sources?.[0]) || "";
  const providerRoute = (item) => {
    const source = providerName(item?.source) || providerName(item?.provider);
    const rest = [];
    const push = (value) => {
      const name = providerName(value);
      if (name && name !== source && !rest.includes(name)) rest.push(name);
    };
    for (const row of Array.isArray(item?.targets) ? item.targets : []) push(row);
    for (const row of Array.isArray(item?.sources) ? item.sources : []) push(row);
    return { source: source || rest.shift() || "", sinks: rest, routed: !!source };
  };
  const mediaValue = (item) => String(item?.media_type || item?.type || item?.art_type || "").toLowerCase();
  const mediaType = (item) => /^(tv|show|shows|series|season|episode|anime|anime_episode)$/i.test(mediaValue(item));
  const objectOf = (value) => value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const isEpisodeItem = (item) => /^(episode|anime_episode)$/i.test(mediaValue(item)) || !!(item?.episode_label || item?.episodeLabel || item?.episode_number || item?.season_number || item?.season || objectOf(item?.episode).season_number || objectOf(item?.episode).number);
  const tmdbId = (item) => {
    const ids = objectOf(item?.ids);
    const meta = objectOf(item?.provider_metadata);
    const show = objectOf(item?.show || item?.series || item?.anime);
    const showIds = objectOf(meta.show_ids);
    const nestedShowIds = objectOf(show.ids);
    const idsShowIds = objectOf(ids.show_ids);
    if (mediaValue(item) === "movie") return item?.tmdb || item?.tmdb_id || ids.tmdb || ids.id || "";
    return showIds.tmdb || nestedShowIds.tmdb || objectOf(item?.show_ids).tmdb || idsShowIds.tmdb || show.tmdb || show.tmdb_id || ids.tmdb_show || item?.tmdb_show || ids.show_tmdb || item?.show_tmdb || item?.tmdb || item?.tmdb_id || ids.tmdb || "";
  };
  const showTmdbId = (item) => {
    const ids = objectOf(item?.ids);
    const meta = objectOf(item?.provider_metadata);
    const show = objectOf(item?.show || item?.series || item?.anime);
    const showIds = objectOf(meta.show_ids);
    const nestedShowIds = objectOf(show.ids);
    const directShowIds = objectOf(item?.show_ids);
    const idsShowIds = objectOf(ids.show_ids);
    return String(
      showIds.tmdb || nestedShowIds.tmdb || directShowIds.tmdb || idsShowIds.tmdb || show.tmdb || show.tmdb_id
      || ids.tmdb_show || item?.tmdb_show || ids.show_tmdb || item?.show_tmdb || ""
    ).trim();
  };
  const titleOf = (item) => String(item?.series_title || item?.title || item?.name || item?.show_title || item?.label || "Untitled");
  const yearOf = (item) => String(item?.year || item?.release_year || item?.aired_year || "").trim();
  const episodeOf = (item) => {
    const explicit = String(item?.episode_label || item?.episodeLabel || "").trim();
    if (explicit) return explicit;
    const s = Number(item?.season || item?.season_number || item?.episode?.season_number || 0);
    const e = Number(item?.episode || item?.episode_number || item?.episode?.episode_number || 0);
    return s && e ? `S${String(s).padStart(2, "0")}E${String(e).padStart(2, "0")}` : "";
  };
  const poster = (item, size = "w342") => {
    const show = objectOf(item?.show || item?.series || item?.anime);
    const src = String(isEpisodeItem(item)
      ? (item?.show_cover_url || item?.show_cover || item?.show_poster_url || item?.show_poster || item?.series_poster || item?.series_cover || item?.grandparentThumb || show.poster_url || show.poster || show.cover || show.poster_cover || item?.poster_cover || "")
      : (item?.poster_url || item?.poster || item?.cover || item?.poster_cover || ""));
    if (src) return src;
    const episode = isEpisodeItem(item);
    const id = episode ? showTmdbId(item) : tmdbId(item);
    if (!id) return "/assets/img/placeholder_poster.svg";
    const kind = mediaType(item) ? "tv" : "movie";
    const title = !isEpisodeItem(item) && (item?.series_title || item?.title) ? `&title=${encodeURIComponent(String(item?.series_title || item?.title))}` : "";
    const year = !mediaType(item) && item?.year ? `&year=${encodeURIComponent(String(item.year))}` : "";
    return `/art/tmdb/${kind}/${encodeURIComponent(id)}?kind=poster&size=${encodeURIComponent(size)}${title}${year}`;
  };
  const watchlistArtEvidence = (item) => {
    const title = item?.title ? `&title=${encodeURIComponent(String(item.title))}` : "";
    const year = item?.year ? `&year=${encodeURIComponent(String(item.year))}` : "";
    return title + year;
  };
  const tmdbBackdrop = (item) => {
    const id = tmdbId(item);
    if (!id) return "";
    const kind = mediaType(item) ? "tv" : "movie";
    return `/art/tmdb/${kind}/${encodeURIComponent(id)}?kind=backdrop&size=w1280`;
  };
  const backdrop = (item) => {
    const direct = String(item?.backdrop_url || item?.background_url || item?.background || item?.fanart || "").trim();
    if (direct) return direct;
    return tmdbBackdrop(item);
  };
  const watchlistPreviewArt = (item, size = "w300") => {
    const id = tmdbId(item);
    if (!id) return "";
    const season = Number(item?.season_number || item?.episode?.season_number || item?.episode?.season || item?.season || 0);
    const episode = Number(item?.episode_number || item?.episode?.episode_number || item?.episode?.number || item?.episode || 0);
    if (mediaType(item) && season > 0 && episode > 0) {
      return `/art/tmdb/tv/${encodeURIComponent(String(id))}?kind=still&season=${encodeURIComponent(String(season))}&episode=${encodeURIComponent(String(episode))}&size=${encodeURIComponent(size)}${watchlistArtEvidence(item)}`;
    }
    const kind = mediaType(item) ? "tv" : "movie";
    const locale = encodeURIComponent(window.__CW_LOCALE || navigator.language || "en-US");
    return `/art/tmdb/${kind}/${encodeURIComponent(String(id))}?kind=backdrop&size=${encodeURIComponent(size)}&locale=${locale}${watchlistArtEvidence(item)}`;
  };
  const watchlistWidgetArt = (item, size = "w300") => {
    const preview = window.CW?.WatchlistPreview;
    const cover = preview?.artUrl?.(item, "w342") || "/assets/img/placeholder_poster.svg";
    return preview?.gridArtUrl?.(item, size) || cover;
  };
  const heroBackdrop = (item) => tmdbBackdrop(item) || backdrop(item);
  const relTime = (value) => {
    let ts = Number(value || 0);
    if (!Number.isFinite(ts) || ts <= 0) return "";
    if (ts > 100000000000) ts = Math.floor(ts / 1000);
    const delta = Math.max(1, Math.floor(Date.now() / 1000) - ts);
    const units = [["y", 31536000], ["mo", 2592000], ["w", 604800], ["d", 86400], ["h", 3600], ["m", 60]];
    for (const [name, seconds] of units) if (delta >= seconds) return `${Math.floor(delta / seconds)}${name} ago`;
    return `${delta}s ago`;
  };
  const toast = (message, error = false) => {
    const el = $("#profile-toast");
    if (!el) return;
    el.textContent = message;
    el.classList.toggle("error", !!error);
    el.classList.remove("hidden");
    clearTimeout(el.__timer);
    el.__timer = setTimeout(() => el.classList.add("hidden"), 3200);
  };
  const empty = (text) => `<div class="cw-profile-empty">${esc(text)}</div>`;
  const SETTINGS_CONFIRM_MS = 4200;
  const armConfirm = (btn, label) => {
    if (!btn) return false;
    const reset = () => {
      clearTimeout(btn.__cwConfirmTimer);
      btn.classList.remove("is-confirming");
      if (btn.__cwConfirmIdle !== undefined) btn.innerHTML = btn.__cwConfirmIdle;
      btn.__cwConfirmIdle = undefined;
    };
    if (btn.classList.contains("is-confirming")) {
      reset();
      return true;
    }
    btn.__cwConfirmIdle = btn.innerHTML;
    btn.classList.add("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span>${esc(label)}</span>`;
    btn.__cwConfirmTimer = setTimeout(reset, SETTINGS_CONFIRM_MS);
    return false;
  };
  const canWriteRecords = () => !viewingAs() && (document.documentElement?.dataset?.cwRole !== "user" || document.documentElement?.dataset?.cwPermWrite === "on");
  const RECORD_SELECTION_MAX = 1000;
  const selectionFields = (item) => {
    const out = { key: String(item?.key || ""), aliases: Array.isArray(item?.aliases) ? item.aliases : [], present: Array.isArray(item?.present) ? item.present : [] };
    for (const field of ["type", "title", "year", "season", "episode", "ids", "show_ids"]) {
      const value = item?.[field];
      if (value != null && value !== "") out[field] = value;
    }
    return out;
  };
  const skelLines = `<span class="cw-profile-skel-lines"><span class="cw-skel-line cw-skel-line--title"></span><span class="cw-skel-line cw-skel-line--meta"></span></span>`;
  const skelShapes = {
    progress: `<div class="cw-cw-card cw-dash-skeleton cw-dash-skeleton-row cw-profile-skel" aria-hidden="true"><span class="cw-cw-art cw-skel-block"></span>${skelLines}</div>`,
    watchlist: `<div class="cw-profile-row cw-dash-skeleton cw-dash-skeleton-row cw-profile-skel" aria-hidden="true"><span class="cw-skel-block"></span>${skelLines}<span class="cw-profile-skel-pill cw-skel-dot"></span></div>`,
    stats: `<div class="cw-profile-stat cw-dash-skeleton cw-dash-skeleton-row cw-profile-skel" aria-hidden="true"><span class="cw-profile-skel-icon cw-skel-block"></span>${skelLines}</div>`,
    activity: `<div class="cw-profile-activity-row cw-dash-skeleton cw-dash-skeleton-row cw-profile-skel" aria-hidden="true"><span class="material-symbols-rounded cw-skel-block"></span>${skelLines}<small class="cw-skel-line cw-skel-line--meta"></small></div>`,
    collection: `<div class="cw-collection-skel cw-dash-skeleton cw-profile-skel" aria-hidden="true"><span class="cw-collection-skel-art cw-skel-block"></span>${skelLines}</div>`,
    collectionRow: `<div class="cw-collection-skel cw-collection-skel--row cw-dash-skeleton cw-profile-skel" aria-hidden="true"><span class="cw-collection-skel-art cw-skel-block"></span>${skelLines}</div>`,
    historyCard: `<div class="cw-hist-skel cw-dash-skeleton cw-profile-skel" aria-hidden="true"><span class="cw-hist-skel-art cw-skel-block"></span>${skelLines}</div>`,
  };
  const skeleton = (kind, count) => Array.from({ length: count }, () => skelShapes[kind]).join("");

  function paintOverviewSkeletons() {
    const hosts = [["#profile-progress", "progress", 3], ["#profile-watchlist", "watchlist", 3], ["#profile-activity", "activity", 4], ["#profile-quick-stats", "stats", 6]];
    for (const [sel, kind, count] of hosts) {
      const host = $(sel);
      if (host) host.innerHTML = skeleton(kind, count);
    }
  }

  const ACTIVITY_LIMIT = 5;
  const activityLabel = (row) => {
    const kind = String(row?.kind || "").toLowerCase();
    if (kind === "scheduler") return "Scheduler";
    if (kind === "webhook") return "Webhook";
    if (kind === "watcher") return "Watcher";
    if (kind === "playlist") return "Playlist";
    return "Sync";
  };
  const activityIcon = (row) => {
    const icon = String(row?.icon || "").trim();
    if (icon) return icon;
    const kind = String(row?.kind || "").toLowerCase();
    if (kind === "scheduler") return "event_available";
    if (kind === "webhook") return "rss_feed";
    if (kind === "watcher") return "sensors";
    if (kind === "playlist") return "playlist_play";
    return "sync";
  };
  const activityTone = (row) => {
    const sev = String(row?.severity || "").toLowerCase();
    const status = String(row?.status || "").toLowerCase();
    if (sev === "error" || status === "failed") return "bad";
    if (sev === "warn" || sev === "warning" || ["warning", "unresolved", "blackboxed"].includes(status)) return "warn";
    if (status === "running" || status === "pending") return "live";
    return "ok";
  };
  const isScrobbleActivity = (row) => {
    const kind = String(row?.kind || "").toLowerCase();
    return kind === "watcher" || kind === "webhook" || String(row?.domain || "").toLowerCase() === "scrobble";
  };
  const parseScrobbleSummary = (row) => {
    const summary = String(row?.summary || row?.meta || "").trim();
    const match = summary.match(/^(Watching)\s+(.+?)\s+(?:->|\u2192)\s+([^,]+)(?:,\s*([^,]+))?/i);
    return {
      action: String(row?.scrobble_action || match?.[1] || "").trim(),
      title: String(row?.item_title || (match?.[2] || "") || row?.title || "").trim(),
      destination: String(row?.scrobble_destination || match?.[3] || "").trim(),
      progress: String(row?.progress || match?.[4] || "").trim(),
    };
  };
  const activityTitle = (row) => {
    const title = String(row?.title || row?.summary || "").trim();
    if (isScrobbleActivity(row)) {
      const parsed = parseScrobbleSummary(row);
      return parsed.title || title || `${activityLabel(row)} activity`;
    }
    if (String(row?.kind || "").toLowerCase() === "sync") {
      const status = String(row?.status || "").toLowerCase();
      if (status === "failed") return "Sync failed";
      if (status === "warning") return "Sync completed with issues";
      if (status === "running") return "Sync running";
      if (/^sync run completed$/i.test(title)) return "Sync completed";
    }
    return title || `${activityLabel(row)} activity`;
  };
  const syncActivityBits = (row) => {
    const parts = [];
    const route = String(row?.route || "").trim();
    const summary = String(row?.summary || row?.meta || "").trim();
    const pairMatch = summary.match(/(\d+\s+pairs?)(?:\s*\(([^)]+)\))?/i);
    if (route) parts.push(route);
    if (pairMatch?.[1]) parts.push(pairMatch[1]);
    if (pairMatch?.[2]) parts.push(pairMatch[2]);
    return parts;
  };
  const scrobbleActivityBits = (row) => {
    const parts = [];
    const parsed = parseScrobbleSummary(row);
    if (parsed.action) parts.push(parsed.action);
    if (parsed.destination) parts.push(parsed.destination);
    if (parsed.progress) parts.push(parsed.progress);
    if (row?.route) parts.push(String(row.route));
    return parts;
  };
  const activityProviderKey = (value) => {
    const key = providerKey(value);
    return key && window.CW?.ProviderMeta?.get?.(key) ? key : "";
  };
  const activityProviderChip = (value) => {
    const key = activityProviderKey(value);
    if (!key) return "";
    const label = visibleProviderLabel(key) || key;
    const logo = providerLogLogo(key);
    const src = logo ? `<img src="${esc(logo)}" alt="" loading="lazy" onerror="this.onerror=null;this.hidden=true">` : "";
    return `<span class="cw-profile-activity-provider" data-provider="${esc(key.toLowerCase())}" title="${esc(label)}">${src}<span>${esc(label)}</span></span>`;
  };
  const activityRouteChip = (value) => {
    const parts = String(value || "").split(/\s*(?:->|\u2192)\s*/).map((part) => part.trim()).filter(Boolean);
    if (parts.length < 2 || parts.some((part) => !activityProviderKey(part))) return "";
    const nodes = parts.map(activityProviderChip).filter(Boolean);
    if (nodes.length < 2) return "";
    return nodes.map((node, index) => `${index ? `<span class="material-symbols-rounded cw-profile-activity-route-arrow" aria-hidden="true">chevron_right</span>` : ""}${node}`).join("");
  };
  const activityChipHtml = (bit, index) => {
    const route = activityRouteChip(bit);
    const provider = !route ? activityProviderChip(bit) : "";
    const cls = `cw-profile-activity-chip${index === 0 ? " is-primary" : ""}${route ? " has-route" : ""}${provider ? " has-provider" : ""}`;
    return `<span class="${cls}">${route || provider || esc(bit)}</span>`;
  };
  const activityMetaBits = (row) => {
    if (String(row?.kind || "").toLowerCase() === "sync") return syncActivityBits(row);
    if (isScrobbleActivity(row)) return scrobbleActivityBits(row);
    const parts = [];
    const meta = String(row?.meta || row?.route || "").trim();
    if (meta && meta !== activityTitle(row)) parts.push(meta);
    return parts;
  };
  const activityEventCount = (row) => {
    const count = Number(row?.event_count || 0);
    return count > 0 ? `${count} event${count === 1 ? "" : "s"}` : "";
  };
  const futureTime = (value) => {
    const ts = Number(value || 0);
    if (!Number.isFinite(ts) || ts <= 0) return "";
    const delta = Math.floor(ts - Date.now() / 1000);
    if (delta <= 0) return relTime(ts);
    const units = [["d", 86400], ["h", 3600], ["m", 60]];
    for (const [name, seconds] of units) if (delta >= seconds) return `in ${Math.floor(delta / seconds)}${name}`;
    return "in less than 1m";
  };
  const schedulerActivityRow = (status) => {
    const eff = status?.effective || {};
    const enabled = !!eff.enabled || !!status?.running;
    if (!enabled) return null;
    const mode = String(eff.mode || status?.effective_mode || "").replace(/_/g, " ").trim();
    const next = Number(status?.next_run_at || 0);
    const last = Number(status?.last_run_at || 0);
    const tick = Number(status?.last_tick || 0);
    const bits = [];
    if (mode && mode !== "disabled") bits.push(mode);
    if (next > 0) bits.push(`next ${futureTime(next) || new Date(next * 1000).toLocaleString()}`);
    return {
      id: "scheduler-status",
      kind: "scheduler",
      icon: "event_available",
      badge: "Scheduler",
      title: status?.running ? "Scheduler running" : "Scheduler active",
      meta: bits.join(" - "),
      status: status?.running ? "running" : "completed",
      severity: status?.last_error ? "warning" : "info",
      created_at: last || tick || Math.floor(Date.now() / 1000),
      event_count: 0,
    };
  };
  const profileActivityRow = (row) => {
    const tone = activityTone(row);
    const badge = String(row?.badge || activityLabel(row)).trim();
    const bits = activityMetaBits(row);
    const time = relTime(row?.created_at);
    const events = activityEventCount(row);
    const detail = bits.length
      ? bits.map(activityChipHtml).join("")
      : `<span class="cw-profile-activity-chip">${esc(badge)}</span>`;
    const side = [
      `<span>${esc(time || badge)}</span>`,
      events ? `<span class="cw-profile-activity-count">${esc(events)}</span>` : "",
    ].filter(Boolean).join("");
    return `
      <button class="cw-profile-activity-row cw-profile-activity-row--${esc(String(row?.kind || "sync").toLowerCase())} is-${esc(tone)}" type="button" data-event-group-id="${esc(row?.id || "")}" data-event-domain="${esc(String(row?.domain || "sync").toLowerCase())}">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(activityIcon(row))}</span>
        <span class="cw-profile-activity-copy">
          <span class="cw-profile-activity-top">
            <span class="cw-profile-activity-badge">${esc(badge)}</span>
            <strong>${esc(activityTitle(row))}</strong>
          </span>
          <span class="cw-profile-activity-meta">${detail}</span>
        </span>
        <small>${side}</small>
      </button>
    `;
  };
  const waitForOverviewProfile = async () => {
    const ready = window.CW?.OverviewProfile?.ready;
    if (!ready || typeof ready.then !== "function") return;
    await Promise.race([
      ready.catch(() => {}),
      new Promise((resolve) => setTimeout(resolve, 900)),
    ]);
  };

  async function loadProfileActivity({ preserve = false } = {}) {
    const host = $("#profile-activity");
    if (!host) return;
    if (!preserve || !host.children.length) host.innerHTML = skeleton("activity", 4);
    await waitForOverviewProfile();
    const params = new URLSearchParams({ limit: String(ACTIVITY_LIMIT), visibility: "all" });
    const userProfile = String(window.CW?.OverviewProfile?.id || "").trim();
    if (userProfile) params.set("user_profile", userProfile);

    const includeSchedulerStatus = window.CW?.OverviewProfile?.isAdmin !== false;
    const [feedRes, schedulerRes] = await Promise.allSettled([
      api(`/api/events/feed?${params.toString()}`),
      includeSchedulerStatus ? api("/api/scheduling/status") : Promise.resolve(null),
    ]);
    const rows = feedRes.status === "fulfilled" && Array.isArray(feedRes.value?.items) ? feedRes.value.items.slice() : [];
    const schedulerRow = schedulerRes.status === "fulfilled" ? schedulerActivityRow(schedulerRes.value) : null;
    if (schedulerRow && !rows.some((row) => String(row?.kind || "") === "scheduler")) rows.push(schedulerRow);
    rows.sort((a, b) => Number(b?.created_at || 0) - Number(a?.created_at || 0));
    host.innerHTML = rows.length ? rows.slice(0, ACTIVITY_LIMIT).map(profileActivityRow).join("") : empty("No recent activity yet.");
  }

  function wireProfileActivity() {
    $("#profile-activity-view-all")?.addEventListener("click", () => {
      if (window.openEvents) window.openEvents();
    });
    $("#profile-activity")?.addEventListener("click", (event) => {
      const row = event.target?.closest?.("[data-event-group-id]");
      if (!row || !window.openEvents) return;
      const groupId = String(row.dataset.eventGroupId || "").trim();
      const domain = String(row.dataset.eventDomain || "sync").trim();
      if (/^\d+$/.test(groupId)) {
        window.openEvents({ groupId, domain, visibility: "all", mode: "grouped" });
      } else {
        window.openEvents();
      }
    });
    window.addEventListener("cw:overview-profile-changed", () => {
      loadProfileActivity().catch(() => {
        const host = $("#profile-activity");
        if (host) host.innerHTML = empty("Recent activity could not be loaded.");
      });
    });
  }
  let profile = null;
  let posterSeq = 0;
  const posterItems = new Map();
  const numberFmt = new Intl.NumberFormat();

  function setAvatar(url) {
    const normalized = window.CW?.AccountMenu?.normalizeAvatarUrl?.(url) || String(url || "").trim();
    const nodes = [viewingAs() ? null : $("#profile-avatar-button"), $("#cw-nav-profile-avatar"), $("#profile-settings-avatar")].filter(Boolean);
    for (const node of nodes) {
      if (window.CW?.AccountMenu?.setAvatarNode) {
        window.CW.AccountMenu.setAvatarNode(node, normalized);
        continue;
      }
      const existing = node.querySelector("img")?.getAttribute("src") || "";
      if (normalized && existing === normalized) continue;
      if (normalized) node.innerHTML = `<img src="${esc(normalized)}" alt="">`;
      else node.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">person</span>`;
    }
  }

  function updateSharedProfile(user) {
    if (!window.CW?.AuthState?.user) return;
    window.CW.AuthState.user.avatar_url = String(user?.avatar_url || "");
    window.CW.AuthState.user.preferences = user?.preferences || {};
    try {
      window.dispatchEvent(new CustomEvent("cw:auth-state-changed", { detail: window.CW.AuthState.read?.() || {} }));
    } catch {}
  }

  function bustUrl(url) {
    const text = String(url || "");
    return text ? `${text}${text.includes("?") ? "&" : "?"}v=${Date.now()}` : "";
  }

  function setAvatarUploadState({ visible = false, label = "", percent = 0 } = {}) {
    const host = $("#profile-avatar-upload-status");
    if (!host) return;
    const pct = Math.max(0, Math.min(100, Math.round(Number(percent || 0))));
    host.classList.toggle("hidden", !visible);
    $("#profile-avatar-upload-label").textContent = label || "Uploading picture";
    $("#profile-avatar-upload-percent").textContent = `${pct}%`;
    $("#profile-avatar-upload-bar").style.width = `${pct}%`;
  }

  function uploadAvatar(dataUrl, contentType, onProgress) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", "/api/profile/avatar");
      xhr.responseType = "json";
      xhr.timeout = 120000;
      xhr.setRequestHeader("Content-Type", "application/json");
      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) return;
        onProgress?.(Math.min(98, Math.round((event.loaded / event.total) * 100)));
      };
      xhr.onload = () => {
        const data = xhr.response || {};
        if (xhr.status < 200 || xhr.status >= 300 || data?.ok === false) {
          reject(new Error(data?.error || `HTTP ${xhr.status}`));
          return;
        }
        resolve(data);
      };
      xhr.onerror = () => reject(new Error("Upload failed"));
      xhr.ontimeout = () => reject(new Error("Upload timed out"));
      xhr.send(JSON.stringify({ data: dataUrl, content_type: contentType }));
    });
  }

  const readFileAsDataUrl = (file, onProgress) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error("Could not read profile picture"));
    reader.onprogress = (event) => {
      if (!event.lengthComputable) return;
      onProgress?.(Math.min(45, Math.round((event.loaded / event.total) * 45)));
    };
    reader.onload = () => resolve(String(reader.result || ""));
    reader.readAsDataURL(file);
  });

  const loadImage = (url) => new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error("Could not decode profile picture"));
    img.src = url;
  });

  const canvasToBlob = (canvas, type, quality) => new Promise((resolve) => {
    try { canvas.toBlob(resolve, type, quality); } catch { resolve(null); }
  });

  async function prepareAvatarUpload(file) {
    const previewUrl = URL.createObjectURL(file);
    try {
      const img = await loadImage(previewUrl);
      const size = 320;
      const canvas = document.createElement("canvas");
      canvas.width = size;
      canvas.height = size;
      const ctx = canvas.getContext("2d", { alpha: false });
      if (!ctx) throw new Error("Could not prepare profile picture");
      ctx.fillStyle = "#101217";
      ctx.fillRect(0, 0, size, size);
      const source = Math.min(img.naturalWidth || img.width, img.naturalHeight || img.height);
      const sx = Math.max(0, ((img.naturalWidth || img.width) - source) / 2);
      const sy = Math.max(0, ((img.naturalHeight || img.height) - source) / 2);
      ctx.drawImage(img, sx, sy, source, source, 0, 0, size, size);
      const blob = await canvasToBlob(canvas, "image/webp", 0.84) || await canvasToBlob(canvas, "image/jpeg", 0.88);
      if (!blob) throw new Error("Could not compress profile picture");
      const dataUrl = await readFileAsDataUrl(blob);
      return { previewUrl, dataUrl, contentType: blob.type || "image/jpeg" };
    } catch {
      const dataUrl = await readFileAsDataUrl(file);
      return { previewUrl, dataUrl, contentType: file.type };
    }
  }

  function renderProfile(data) {
    profile = data?.user || profile || {};
    const display = String(profile.display_name || profile.label || profile.username || "Profile");
    const viewed = viewingAs();
    $("#profile-display-name").textContent = viewed ? (viewAsState.label || viewAsState.id) : display;
    $("#profile-username").textContent = viewed ? `Opened by @${profile.username || ""}` : `@${profile.username || ""}`;
    $("#profile-role").textContent = viewed ? "User profile" : (profile.is_admin ? "Administrator" : "Managed User");
    $("#profile-display-input").value = display;
    renderTwoFactor(!!profile.totp_enabled);
    setAvatar(profile.avatar_url || "");
    renderMemberSince(viewingAs() ? {} : profile);
    renderPreferences(profile);
    renderSessions(profile);
    updateSharedProfile(profile);
  }

  function renderMemberSince(user) {
    const host = $("#profile-member-since");
    if (!host) return;
    const ts = Number(user?.created_at || 0);
    if (!Number.isFinite(ts) || ts <= 0) {
      host.classList.add("hidden");
      return;
    }
    const when = new Date(ts * 1000);
    host.querySelector("span:last-child").textContent =
      `Member since ${when.toLocaleDateString(undefined, { month: "long", year: "numeric" })}`;
    host.classList.remove("hidden");
  }

  function daysTracked(user) {
    const ts = Number(user?.created_at || 0);
    if (!Number.isFinite(ts) || ts <= 0) return 0;
    return Math.max(0, Math.floor((Date.now() / 1000 - ts) / 86400));
  }

  function renderHeroChips({ itemsWatched, services }) {
    const host = $("#profile-hero-chips");
    if (!host) return;
    const chips = [];
    const days = viewingAs() ? 0 : daysTracked(profile);
    if (days > 0) chips.push(["calendar_month", numberFmt.format(days), "Days tracked"]);
    chips.push(["visibility", numberFmt.format(itemsWatched || 0), "Items watched"]);
    chips.push(["hub", numberFmt.format(services || 0), "Services connected"]);
    host.innerHTML = chips.map(([icon, value, label]) => `
      <div class="cw-profile-chip">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <div><strong>${esc(value)}</strong><small>${esc(label)}</small></div>
      </div>`).join("");
  }

  function renderPreferences(user) {
    const prefs = user?.preferences || {};
    const card = $("#profile-pref-playing-card");
    const quick = $("#profile-pref-quick-add");
    if (card) card.checked = prefs.playing_card !== false;
    if (quick) quick.checked = prefs.quick_add !== false;
  }

  function renderTwoFactor(enabled) {
    const state = $("#profile-2fa-state");
    if (state) {
      state.textContent = enabled ? "On" : "Off";
      state.classList.toggle("is-enabled", enabled);
    }
    const copy = $("#profile-2fa-copy");
    if (copy) copy.textContent = enabled ? "You enter a code from your phone when you sign in." : "Ask for a code from your phone when you sign in.";
    for (const [sel, hide] of [["#profile-2fa-setup-btn", enabled], ["#profile-recovery-btn", !enabled], ["#profile-2fa-disable-btn", !enabled]]) {
      const node = $(sel);
      if (node) node.hidden = hide;
    }
  }

  function friendlyAgo(value) {
    let ts = Number(value || 0);
    if (!Number.isFinite(ts) || ts <= 0) return "";
    if (ts > 100000000000) ts = Math.floor(ts / 1000);
    const delta = Math.max(0, Math.floor(Date.now() / 1000) - ts);
    const units = [["year", 31536000], ["month", 2592000], ["week", 604800], ["day", 86400], ["hour", 3600], ["minute", 60]];
    for (const [name, seconds] of units) {
      const count = Math.floor(delta / seconds);
      if (count >= 1) return `${count} ${name}${count === 1 ? "" : "s"} ago`;
    }
    return "just now";
  }

  function describeAgent(value) {
    const ua = String(value || "").trim();
    if (!ua) return { name: "Unknown device", icon: "devices" };
    if (!/mozilla\//i.test(ua)) return { name: "App or script", icon: "apps" };
    const browsers = [[/edg(?:e|a|ios)?\//i, "Edge"], [/opr\/|opera/i, "Opera"], [/firefox\/|fxios/i, "Firefox"], [/chrome\/|crios/i, "Chrome"], [/safari\//i, "Safari"]];
    const systems = [[/iphone|ipad|ipod/i, "iOS"], [/android/i, "Android"], [/windows nt/i, "Windows"], [/cros/i, "ChromeOS"], [/mac os x|macintosh/i, "macOS"], [/linux/i, "Linux"]];
    const browser = browsers.find(([re]) => re.test(ua))?.[1] || "Browser";
    const os = systems.find(([re]) => re.test(ua))?.[1] || "";
    const icon = /ipad|tablet/i.test(ua) ? "tablet" : /mobi|iphone|android/i.test(ua) ? "smartphone" : "computer";
    return { name: os ? `${browser} on ${os}` : browser, icon };
  }

  function renderSessions(user) {
    const host = $("#profile-sessions");
    if (!host) return;
    const current = user?.current_session;
    const others = (Array.isArray(user?.other_sessions) ? user.other_sessions : [])
      .slice()
      .sort((a, b) => Number(b?.created_at || 0) - Number(a?.created_at || 0));
    const rows = current ? [{ ...current, current: true }, ...others] : others;
    const revokeAll = $("#profile-revoke-sessions");
    if (revokeAll) revokeAll.hidden = !others.length;
    host.innerHTML = rows.length ? rows.map((row) => {
      const agent = describeAgent(row.ua);
      const since = friendlyAgo(row.created_at);
      const meta = [since ? `Signed in ${since}` : "", row.ip].filter(Boolean).join(" · ");
      const badge = row.current ? `<em class="cw-set-badge">This device</em>` : "";
      const action = row.current ? "" : `<button class="cw-set-btn cw-set-btn--danger cw-danger-confirm" type="button" data-session-kill="${esc(row.id)}"><span class="material-symbols-rounded" aria-hidden="true">logout</span><span>Sign out</span></button>`;
      return `<div class="cw-set-session${row.current ? " is-current" : ""}"><span class="cw-set-device material-symbols-rounded" aria-hidden="true">${esc(agent.icon)}</span><span class="cw-set-copy"><strong title="${esc(row.ua || "")}"><span>${esc(agent.name)}</span>${badge}</strong><small>${esc(meta)}</small></span>${action}</div>`;
    }).join("") : empty("No active sessions.");
  }

  function overlayItem(item) {
    const type = mediaType(item) ? "show" : "movie";
    const tmdb = tmdbId(item);
    const ids = { ...(objectOf(item?.ids)) };
    if (tmdb) ids.tmdb = tmdb;
    return {
      ...item,
      ids,
      title: titleOf(item),
      type,
      media_type: type === "show" ? "tv" : "movie",
      tmdb,
      tmdb_id: tmdb,
      year: yearOf(item),
      poster_url: poster(item),
      backdrop_url: backdrop(item),
      episode_label: episodeOf(item),
    };
  }

  function storePosterItem(item) {
    const key = `profile-${++posterSeq}`;
    posterItems.set(key, overlayItem(item || {}));
    return key;
  }

  function prunePosterItems() {
    if (posterItems.size <= 600) return;
    const live = new Set();
    document.querySelectorAll("[data-profile-poster-key]").forEach((node) => {
      const key = node.dataset.profilePosterKey;
      if (key) live.add(key);
    });
    for (const key of [...posterItems.keys()]) {
      if (!live.has(key)) posterItems.delete(key);
    }
  }

  function progressPct(item) {
    const raw = Number(item?.progress_percent ?? item?.progress ?? item?.percent);
    if (!Number.isFinite(raw) || raw <= 0) return 0;
    return Math.max(0, Math.min(100, raw));
  }

  const progressProviders = (item) => {
    const out = [];
    const push = (value) => {
      const name = providerName(value);
      if (!name || name.toLowerCase() === "combined" || out.includes(name)) return;
      out.push(name);
    };
    for (const row of Array.isArray(item?.providers) ? item.providers : []) push(row);
    for (const row of Array.isArray(item?.sources) ? item.sources : []) push(row);
    if (!out.length) {
      push(item?.provider);
      push(item?.source);
    }
    return out;
  };

  function progressCard(item) {
    const key = storePosterItem(item);
    const pct = progressPct(item);
    const art = watchlistPreviewArt(item) || poster(item, "w780");
    const episode = episodeOf(item);
    const sub = episode || yearOf(item) || "";
    const episodeBadge = episode ? `<span class="cw-cw-episode">${esc(episode)}</span>` : "";
    const providerIcons = progressProviders(item).map(providerIconHtml).filter(Boolean).join("");
    const providerStrip = providerIcons ? `<span class="cw-cw-providers">${providerIcons}</span>` : "";
    return `<button class="cw-cw-card" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(titleOf(item))}">
      <span class="cw-cw-art">${episodeBadge}${providerStrip}<img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"></span>
      <span class="cw-cw-title">${esc(titleOf(item))}</span>
      <span class="cw-cw-sub">${esc(sub)}</span>
      <span class="cw-cw-foot">
        <span class="cw-cw-track"><i style="width:${pct}%"></i></span>
        <span class="cw-cw-pct">${pct ? `${Math.round(pct)}%` : ""}</span>
      </span>
    </button>`;
  }

  function mediaKindLabel(item) {
    const raw = String(item?.type || item?.media_type || "").toLowerCase();
    if (/episode/.test(raw) || item?.season || item?.episode) return "Episode";
    if (/tv|show|series|season|anime/.test(raw)) return "Show";
    if (/movie|film/.test(raw)) return "Movie";
    return episodeOf(item) ? "Show" : "Movie";
  }

  function addedEpoch(item) {
    for (const key of ["added_epoch", "added_at", "added", "created_at", "ts"]) {
      const raw = Number(item?.[key]);
      if (Number.isFinite(raw) && raw > 0) return raw > 1e12 ? Math.round(raw / 1000) : raw;
    }
    for (const key of ["added_when", "added_at", "added", "created_at"]) {
      const raw = item?.[key];
      if (typeof raw === "string" && raw.trim()) {
        const parsed = Date.parse(raw);
        if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
      }
    }
    return 0;
  }

  function syncedEpoch(item, fallbackEpoch = 0) {
    for (const key of ["synced_epoch", "synced_at", "updated_epoch", "updated_at", "last_synced", "last_sync_epoch"]) {
      const raw = Number(item?.[key]);
      if (Number.isFinite(raw) && raw > 0) return raw > 1e12 ? Math.round(raw / 1000) : raw;
    }
    for (const key of ["synced_at", "updated_at", "last_synced"]) {
      const raw = item?.[key];
      if (typeof raw === "string" && raw.trim()) {
        const parsed = Date.parse(raw);
        if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
      }
    }
    const fallback = Number(fallbackEpoch || 0);
    return Number.isFinite(fallback) && fallback > 0 ? (fallback > 1e12 ? Math.round(fallback / 1000) : fallback) : 0;
  }

  function watchlistRow(item, fallbackSyncEpoch = 0) {
    const key = storePosterItem(item);
    const kind = mediaKindLabel(item);
    const year = yearOf(item);
    const when = addedEpoch(item);
    const syncedWhen = syncedEpoch(item, fallbackSyncEpoch);
    const meta = [kind, year, when ? `updated ${relTime(when)}` : ""].filter(Boolean).join(" - ");
    const art = watchlistWidgetArt(item);
    const synced = item?.synced === false || item?.is_synced === false ? "" : `<span class="cw-profile-watchlist-status">Synced</span>`;
    const syncBadge = syncedWhen ? `<span class="cw-profile-watchlist-sync">${esc(relTime(syncedWhen))}</span>` : "";
    return `<button class="cw-profile-row cw-profile-click-row" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(titleOf(item))}">
      <span class="cw-profile-watchlist-art">${syncBadge}<img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'"></span>
      <div class="cw-profile-watchlist-copy"><strong>${esc(titleOf(item))}</strong><span>${esc(meta)}</span></div>
      ${synced}
    </button>`;
  }

  const watchedEpoch = (item) => {
    const raw = Number(item?.ts || item?.sort_epoch || item?.last_watched_at || item?.watched_at || 0);
    if (!Number.isFinite(raw) || raw <= 0) return 0;
    return raw > 100000000000 ? Math.floor(raw / 1000) : raw;
  };

  const newestItem = (rows) => {
    if (!Array.isArray(rows) || !rows.length) return null;
    return rows.reduce((best, row) => (watchedEpoch(row) > watchedEpoch(best) ? row : best), rows[0]) || null;
  };

  function bindLastWatchedPreview(node) {
    if (!node || node.dataset.previewBound === "1") return;
    node.dataset.previewBound = "1";
    const openLast = (event) => {
      const current = posterItems.get(node.dataset.profilePosterKey || "");
      const open = window.CW?.WatchlistPreview?.openPreviewDrawer || window.openPreviewDrawer;
      if (!current || !open) return;
      event?.preventDefault?.();
      event?.stopPropagation?.();
      void open(current);
    };
    node.addEventListener("click", openLast);
    node.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      openLast(event);
    });
  }

  function renderHero(scrobbleItems, historyItems) {
    const item = newestItem(scrobbleItems) || newestItem(historyItems);
    const hero = $("#profile-hero");
    const last = $(".cw-profile-last");
    const art = item ? heroBackdrop(item) : "";
    const artHost = $("#profile-hero-art");
    artHost.style.backgroundImage = art ? `url("${art.replace(/"/g, "%22")}")` : "";
    hero?.classList.toggle("has-last", !!item);
    hero?.classList.toggle("no-last", !item);
    last?.classList.toggle("hidden", !item);
    if (!item) {
      if (last) {
        delete last.dataset.profilePosterKey;
        last.removeAttribute("aria-label");
      }
      return;
    }
    const key = storePosterItem(item);
    if (last) {
      last.dataset.profilePosterKey = key;
      last.setAttribute("aria-label", `Show details for ${titleOf(item)}`);
      bindLastWatchedPreview(last);
    }
    const lastPoster = $("#profile-last-poster");
    if (lastPoster) {
      lastPoster.onerror = () => {
        lastPoster.onerror = null;
        lastPoster.src = "/assets/img/placeholder_poster.svg";
      };
      lastPoster.src = poster(item, "w185");
    }
    $("#profile-last-title").textContent = titleOf(item);
    $("#profile-last-meta").textContent = [episodeOf(item) || yearOf(item), relTime(watchedEpoch(item))].filter(Boolean).join(" - ");
    const route = providerRoute(item);
    const sourceHtml = providerIconHtml(route.source);
    const sinkHtml = route.sinks.map(providerIconHtml).filter(Boolean).join("");
    const sep = sourceHtml && sinkHtml && route.routed
      ? `<span class="cw-profile-provider-sep material-symbols-rounded" aria-hidden="true">chevron_right</span>`
      : "";
    const badges = `${sourceHtml}${sep}${sinkHtml}`;
    const providerNode = $("#profile-last-provider");
    if (providerNode) {
      providerNode.innerHTML = badges;
      providerNode.hidden = !badges;
    }
  }

  function nowPlayingItem(payload) {
    const streams = Array.isArray(payload?.streams) ? payload.streams : [];
    const active = streams.find((row) => /play|start|resume|watch/i.test(String(row?.state || "")));
    const item = active || streams[0] || (payload && payload.title ? payload : null);
    if (!item) return null;
    const pct = Number(item.progress ?? item.progress_percent ?? item.percent);
    return { ...item, _pct: Number.isFinite(pct) ? Math.max(0, Math.min(100, pct)) : 0 };
  }

  function renderNowPlaying(payload) {
    const hero = $("#profile-hero");
    const layer = $("#profile-hero-now");
    const panel = $("#profile-now");
    if (!hero || !layer || !panel) return;
    const item = nowPlayingItem(payload);
    if (!item) {
      hero.classList.remove("is-playing");
      layer.style.backgroundImage = "";
      const posterNode = $("#profile-now-poster");
      if (posterNode) posterNode.src = "/assets/img/placeholder_poster.svg";
      panel.classList.add("hidden");
      return;
    }
    const art = heroBackdrop(item);
    layer.style.backgroundImage = art ? `url("${String(art).replace(/"/g, "%22")}")` : "";
    const posterNode = $("#profile-now-poster");
    if (posterNode) {
      posterNode.onerror = () => {
        posterNode.onerror = null;
        posterNode.src = "/assets/img/placeholder_poster.svg";
      };
      posterNode.src = poster(item, "w185");
    }
    $("#profile-now-title").textContent = titleOf(item);
    $("#profile-now-meta").textContent = [
      visibleProviderLabel(providerOf(item)),
      episodeOf(item) || yearOf(item),
    ].filter(Boolean).join(" - ");
    const pct = item._pct;
    $("#profile-now-fill").style.width = `${pct}%`;
    $("#profile-now-pct").textContent = pct ? `${Math.round(pct)}%` : "";
    const runtime = durationMinutes(item);
    const left = runtime > 0 && pct > 0 ? Math.max(0, Math.round(runtime * (1 - pct / 100))) : 0;
    $("#profile-now-left").textContent = left ? `${left} min left` : "";
    panel.classList.remove("hidden");
    hero.classList.add("is-playing");
  }

  async function refreshNowPlaying() {
    try {
      renderNowPlaying(await api("/api/watch/currently_watching"));
    } catch {
      renderNowPlaying(null);
    }
  }

  function statType(item) {
    const raw = mediaValue(item);
    if (/episode|anime_episode/.test(raw) || item?.season || item?.episode || item?.episode_label) return "episode";
    if (/tv|show|shows|series|season|anime/.test(raw)) return "show";
    return "movie";
  }

  function statsFromItems(items) {
    const seen = { movie: new Set(), show: new Set(), episode: new Set() };
    for (const item of Array.isArray(items) ? items : []) {
      const type = statType(item);
      const key = String(item?.key || item?.id || tmdbId(item) || `${type}:${titleOf(item)}:${yearOf(item)}:${episodeOf(item)}`).toLowerCase();
      if (key) seen[type].add(key);
    }
    return { movies: seen.movie.size, shows: seen.show.size, episodes: seen.episode.size };
  }

  function durationMinutes(item) {
    const direct = Number(item?.runtime_minutes || item?.duration_minutes);
    if (Number.isFinite(direct) && direct > 0) return direct;
    const ms = Number(item?.duration_ms);
    if (Number.isFinite(ms) && ms > 0) return ms / 60000;
    const raw = Number(item?.duration || item?.runtime);
    if (!Number.isFinite(raw) || raw <= 0) return 0;
    if (raw > 10000) return raw / 60000;
    if (raw > 300) return raw / 60;
    return raw;
  }

  function renderQuickStats({ wall, widgets, progressItems, insights, collection }) {
    const history = widgets?.recent_history?.items || [];
    const ratings = widgets?.latest_ratings?.items || [];
    const scrobble = widgets?.recent_scrobble?.items || [];
    const sampled = [...(wall?.items || []), ...history, ...ratings, ...scrobble, ...(progressItems || [])];
    const sampleStats = statsFromItems(sampled);
    const breakdown = insights?.features?.history?.breakdown || {};
    const watchtime = insights?.watchtime || {};
    const movies = Number(breakdown.movies ?? watchtime.movies ?? sampleStats.movies) || 0;
    const shows = Number(breakdown.shows ?? watchtime.shows ?? sampleStats.shows) || 0;
    const anime = Number(breakdown.anime) || 0;
    const collectionCounts = collection?.counts || {};
    const owned = (Number(collectionCounts.movie) || 0) + (Number(collectionCounts.show) || 0);
    const watchlist = Number(wall?.total ?? (wall?.items || []).length) || 0;
    const fallbackHours = scrobble.reduce((sum, item) => sum + durationMinutes(item), 0) / 60;
    const hours = Number(widgets?.recent_scrobble?.scrobble_hours ?? fallbackHours) || 0;
    const values = [
      ["movies", "movie", "theaters", "Movies", numberFmt.format(movies), "Total movies in your syncs"],
      ["tv", "tv", "live_tv", "TV Shows", numberFmt.format(shows), "Total TV shows in your syncs"],
      ["anime", "animation", "auto_awesome", "Anime", numberFmt.format(anime), "Total anime in your syncs"],
      ["collections", "inventory_2", "video_library", "Collections", numberFmt.format(owned), "Movies and shows you own"],
      ["watchlist", "bookmark", "format_list_bulleted", "Watchlist Items", numberFmt.format(watchlist), "Items on your watchlist"],
      ["hours", "schedule", "show_chart", "Hours Watched", hours ? `${numberFmt.format(Math.round(hours * 10) / 10)} h` : "0 h", "Total time spent watching"],
    ];
    $("#profile-quick-stats").innerHTML = values.map(([key, icon, backdropIcon, label, value, description]) => `
      <div class="cw-profile-stat cw-profile-stat--${esc(key)}" data-stat="${esc(key)}" data-stat-bg="${esc(backdropIcon)}">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <div class="cw-profile-stat-copy"><small>${esc(label)}</small><strong>${esc(value)}</strong><span>${esc(description)}</span></div>
      </div>`).join("");
  }

  function renderProgressItems(progressItems) {
    $("#profile-progress").innerHTML = progressItems.length ? progressItems.slice(0, 9).map(progressCard).join("") : empty("No recent progress yet.");
  }

  function renderOverview(payload = {}) {
    prunePosterItems();
    const widgets = payload.widgets || {};
    const wall = payload.wall || { items: [] };
    const progress = payload.progress || { items: [] };
    const insights = payload.insights || null;
    const status = payload.status || null;
    const collection = payload.collection || null;
    const history = widgets?.recent_history?.items || [];
    const ratings = widgets?.latest_ratings?.items || [];
    const scrobble = widgets?.recent_scrobble?.items || [];
    const widgetProgress = widgets?.recent_progress?.items || [];
    const progressItems = progress?.items?.length ? progress.items : widgetProgress;
    const scrobbleTotal = Number(widgets?.recent_scrobble?.scrobble_total ?? widgets?.recent_scrobble?.total ?? scrobble.length) || 0;
    renderHero(scrobble, history);
    renderQuickStats({ wall, widgets, progressItems, insights, collection });
    const watchlistItems = Array.isArray(wall?.items) ? wall.items : [];
    renderHeroChips({
      itemsWatched: scrobbleTotal,
      services: connectedServices(status),
    });
    renderProgressItems(progressItems);
    $("#profile-watchlist").innerHTML = watchlistItems.length ? watchlistItems.slice(0, 3).map((item) => watchlistRow(item, wall?.last_sync_epoch)).join("") : empty("No watchlist items yet.");
  }

  async function loadOverview() {
    const cacheKey = profileCacheKey("overview");
    const cached = readCache(cacheKey) || readAnyCache(cacheKey);
    if (cached) renderOverview(cached);
    else paintOverviewSkeletons();
    const widgetParams = new URLSearchParams({
      include: "history,ratings,scrobble,progress",
      history_limit: "8",
      ratings_limit: "9",
      scrobble_limit: "8",
      progress_limit: "8",
    });
    if (cached?.widgets?.version) widgetParams.set("known_version", cached.widgets.version);

    const [widgetsRes, wallRes, insightsRes, statusRes, collectionRes] = await Promise.allSettled([
      api(`/api/dashboard/widgets?${widgetParams.toString()}`),
      api("/api/state/wall?limit=8"),
      api("/api/insights?limit_samples=0&history=0&runtime=0&include_events=0"),
      api("/api/status"),
      api("/api/profile/collection?page=1&page_size=1"),
    ]);
    const widgetsNotModified = widgetsRes.status === "fulfilled" && widgetsRes.value?.not_modified && cached?.widgets;
    const widgets = widgetsRes.status === "fulfilled"
      ? (widgetsNotModified ? (cached?.widgets || {}) : widgetsRes.value)
      : (cached?.widgets || {});
    const next = {
      widgets,
      wall: wallRes.status === "fulfilled" ? wallRes.value : (cached?.wall || { items: [] }),
      progress: cached?.progress || { items: [] },
      insights: insightsRes.status === "fulfilled" ? insightsRes.value : (cached?.insights || null),
      status: statusRes.status === "fulfilled" ? statusRes.value : (cached?.status || null),
      collection: collectionRes.status === "fulfilled"
        ? { counts: collectionRes.value?.counts || {}, total: Number(collectionRes.value?.total) || 0 }
        : (cached?.collection || null),
    };
    if (!next.progress?.items?.length && next.widgets?.recent_progress?.items?.length) {
      next.progress = { items: next.widgets.recent_progress.items };
    }
    renderOverview(next);
    writeCache(cacheKey, next);
    if (widgetsNotModified) {
      refreshNowPlaying();
      return;
    }

    api("/api/playback_progress/items?page=1&page_size=8").then((progress) => {
      const fresh = { ...next, progress: progress || { items: [] } };
      const progressItems = fresh.progress?.items?.length ? fresh.progress.items : (fresh.widgets?.recent_progress?.items || []);
      renderProgressItems(progressItems);
      renderQuickStats({ wall: fresh.wall, widgets: fresh.widgets, progressItems, insights: fresh.insights, collection: fresh.collection });
      writeCache(cacheKey, fresh);
    }).catch(() => {});
    refreshNowPlaying();
  }

  const COLLECTION_PREFS_KEY = "cw.profile.collection";
  const COLLECTION_PAGE_SIZES = [24, 48, 72, 96];

  function readCollectionPrefs() {
    try {
      const raw = JSON.parse(window.localStorage?.getItem(COLLECTION_PREFS_KEY) || "{}");
      return raw && typeof raw === "object" ? raw : {};
    } catch {
      return {};
    }
  }

  const collectionPrefs = readCollectionPrefs();

  const collectionState = {
    loaded: false,
    loading: false,
    page: 1,
    pageSize: COLLECTION_PAGE_SIZES.includes(Number(collectionPrefs.pageSize)) ? Number(collectionPrefs.pageSize) : 24,
    view: collectionPrefs.view === "list" ? "list" : "grid",
    timeline: collectionPrefs.timeline !== false,
    zoom: Number.isInteger(collectionPrefs.zoom) ? collectionPrefs.zoom : 0,
    months: [],
    type: "all",
    provider: "",
    search: "",
    sort: "collected_at",
    items: [],
    total: 0,
    pageCount: 1,
    selected: new Map(),
    cols: (collectionPrefs.cols && typeof collectionPrefs.cols === "object") ? { ...collectionPrefs.cols } : {},
  };

  function saveCollectionPrefs() {
    try {
      window.localStorage?.setItem(COLLECTION_PREFS_KEY, JSON.stringify({ view: collectionState.view, pageSize: collectionState.pageSize, cols: collectionState.cols, timeline: collectionState.timeline, zoom: collectionState.zoom }));
    } catch {}
  }

  function lockCollectionPager() {
    $("#profile-collection-pages")?.querySelectorAll("button").forEach((btn) => { btn.disabled = true; });
  }

  function epochOf(value) {
    const numeric = Number(value || 0);
    if (Number.isFinite(numeric) && numeric > 0) return numeric > 1e12 ? Math.floor(numeric / 1000) : numeric;
    const parsed = Date.parse(String(value || "").trim());
    return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
  }

  function collectionKind(item) {
    const raw = String(item?.type || item?.media_type || "").toLowerCase();
    if (raw === "season") return ["stacks", "Season"];
    if (/episode/.test(raw) || item?.episode || item?.episode_number) return ["play_circle", "Episode"];
    if (/show|tv|series|anime/.test(raw)) return ["tv", "Show"];
    return ["movie", "Movie"];
  }

  function collectionSourceProviders(item) {
    const out = [];
    const push = (value) => {
      const name = providerName(value);
      if (name && !out.some((existing) => String(existing).toLowerCase() === name.toLowerCase())) out.push(name);
    };
    for (const row of Array.isArray(item?.providers) ? item.providers : []) push(row);
    for (const row of Array.isArray(item?.sources) ? item.sources : []) push(row);
    const byProvider = item?.sources_by_provider || item?.sourcesByProvider;
    if (byProvider && typeof byProvider === "object") Object.keys(byProvider).forEach(push);
    return out;
  }

  function collectionLibrarySummary(item) {
    const libraries = Array.isArray(item?.libraries) ? item.libraries.filter(Boolean) : [];
    if (!libraries.length) return "";
    if (libraries.length === 1) return libraries[0];
    return `${libraries[0]} +${libraries.length - 1}`;
  }

  function collectionProviderStrip(item) {
    const entries = collectionSourceProviders(item)
      .map((provider) => ({ html: providerIconHtml(provider), title: visibleProviderLabel(provider) || String(provider), missing: false }))
      .filter((entry) => entry.html);
    return providerChipStrip(entries, "cw-profile-provider-badge cw-profile-provider-badge--icon");
  }

  function collectionCard(item) {
    const key = storePosterItem(item);
    const [icon, kind] = collectionKind(item);
    const title = titleOf(item);
    const year = yearOf(item);
    const episode = episodeOf(item);
    const added = epochOf(item?.last_collected_at || item?.collected_at || item?.first_collected_at);
    const when = added ? relTime(added) : "";
    const art = poster(item, "w342");
    const providerStrip = collectionProviderStrip(item);
    const meta = [kind, episode || year, when, collectionLibrarySummary(item)].filter(Boolean).join(" · ");
    const inner = `<span class="cw-collection-poster">
        <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        <span class="cw-collection-badge"><span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>${esc(kind)}</span>
      </span>
      <span class="cw-collection-info">
        <strong class="cw-collection-name">${esc(title)}</strong>
        <span class="cw-collection-meta">${esc(meta)}</span>
        ${providerStrip ? `<span class="cw-collection-sources">${providerStrip}</span>` : ""}
      </span>`;
    const selectKey = collectionSelectable() ? String(item?.key || "") : "";
    if (selectKey) {
      return `<article class="cw-collection-card cw-tl-card${collectionState.selected.has(selectKey) ? " is-selected" : ""}" data-collection-key="${esc(selectKey)}">
      <button class="cw-collection-open" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(title)}">${inner}</button>
      ${collectionSelectBox(selectKey)}
    </article>`;
    }
    return `<button class="cw-collection-card" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(title)}">${inner}</button>`;
  }

  const collectionSelectable = () => canWriteRecords();

  function collectionSelectBox(key) {
    const checked = collectionState.selected.has(key) ? " checked" : "";
    return `<label class="cw-tl-select" title="Select"><input type="checkbox" data-collection-select-item="${esc(key)}"${checked} aria-label="Select"><span class="material-symbols-rounded" aria-hidden="true">check</span></label>`;
  }

  function collectionSelectionEntry(item) {
    const present = [];
    const byProvider = item?.sources_by_provider && typeof item.sources_by_provider === "object" ? item.sources_by_provider : {};
    for (const [provider, instances] of Object.entries(byProvider)) {
      for (const instance of Array.isArray(instances) && instances.length ? instances : ["default"]) {
        present.push({ provider: String(provider).toUpperCase(), instance: String(instance || "default") });
      }
    }
    return selectionFields({ ...item, present: Array.isArray(item?.present) ? item.present : present });
  }

  function syncCollectionSelection() {
    const grid = $("#profile-collection-grid");
    const bar = $("#profile-collection-bulk");
    if (!collectionSelectable()) {
      collectionState.selected.clear();
      if (bar) bar.hidden = true;
      return;
    }
    grid?.querySelectorAll("article[data-collection-key]").forEach((node) => {
      const on = collectionState.selected.has(node.dataset.collectionKey);
      node.classList.toggle("is-selected", on);
      const box = node.querySelector("[data-collection-select-item]");
      if (box) box.checked = on;
    });
    grid?.classList.toggle("has-selection", collectionState.selected.size > 0);
    if (!bar) return;
    bar.hidden = collectionState.selected.size === 0;
    const label = bar.querySelector("[data-collection-count]");
    if (label) label.textContent = numberFmt.format(collectionState.selected.size);
  }

  async function selectCollection(mode) {
    if (!collectionSelectable()) return;
    if (mode === "none") collectionState.selected.clear();
    if (mode === "visible") {
      collectionState.items.forEach((item) => {
        if (item?.key) collectionState.selected.set(String(item.key), collectionSelectionEntry(item));
      });
    }
    if (mode === "all") {
      const params = new URLSearchParams({
        type: collectionState.type,
        provider: collectionState.provider,
        search: collectionState.search,
        sort: collectionState.sort,
        page: "1",
        page_size: "1",
        include_keys: "true",
      });
      try {
        const data = await api(`/api/profile/collection?${params.toString()}`);
        (Array.isArray(data.selection) ? data.selection : []).forEach((entry) => {
          if (entry?.key) collectionState.selected.set(String(entry.key), selectionFields(entry));
        });
        if (data.selection_truncated) toast(`Selected the first ${numberFmt.format(RECORD_SELECTION_MAX)} results`);
      } catch (e) {
        toast(e.message || "Could not select all results", true);
      }
    }
    syncCollectionSelection();
  }

  function removeCollectionSelection() {
    if (!collectionSelectable() || !collectionState.selected.size) return;
    recordRemoval.open({
      kind: "collection",
      mode: "editor",
      noun: "title",
      entries: [...collectionState.selected.values()],
      onDone: (clean) => {
        if (clean) collectionState.selected.clear();
        void loadCollection();
      },
    });
  }

  const collectionDateFmt = (epoch) => {
    if (!epoch) return "";
    try {
      return new Intl.DateTimeFormat(window.__CW_LOCALE || navigator.language || undefined, { day: "2-digit", month: "2-digit", year: "numeric" }).format(new Date(epoch * 1000));
    } catch {
      return "";
    }
  };

  const collectionIsoFmt = (iso) => {
    const raw = String(iso || "").trim();
    if (!raw) return "";
    const parsed = Date.parse(raw.length <= 10 ? raw + "T00:00:00Z" : raw);
    if (!Number.isFinite(parsed)) return "";
    try {
      return new Intl.DateTimeFormat(window.__CW_LOCALE || navigator.language || undefined, { day: "2-digit", month: "2-digit", year: "numeric", timeZone: "UTC" }).format(new Date(parsed));
    } catch {
      return "";
    }
  };

  const COLLECTION_COLUMNS = [
    { key: "poster", label: "Poster", width: 78, min: 62, max: 130 },
    { key: "title", label: "Title", width: 300, min: 170, max: 900, flex: 1.7 },
    { key: "rel", label: "Release", width: 130, min: 96, max: 240 },
    { key: "genre", label: "Genre", width: 180, min: 110, max: 420, flex: 1 },
    { key: "added", label: "Added", width: 140, min: 108, max: 260 },
    { key: "type", label: "Type", width: 112, min: 84, max: 200 },
    { key: "providers", label: "Providers", width: 130, min: 84, max: 280 },
  ];

  const collectionColumnWidth = (col) => {
    const stored = Number(collectionState.cols?.[col.key]);
    const value = Number.isFinite(stored) && stored > 0 ? stored : col.width;
    return Math.max(col.min, Math.min(col.max, Math.round(value)));
  };

  function applyCollectionColumns() {
    const grid = $("#profile-collection-grid");
    if (!grid) return;
    grid.style.setProperty("--cch-cols", COLLECTION_COLUMNS.map((col) => {
      const width = collectionColumnWidth(col);
      return col.flex ? `minmax(${width}px,${col.flex}fr)` : `${width}px`;
    }).join(" "));
  }

  function collectionListHead() {
    const cells = COLLECTION_COLUMNS.map((col, index) => {
      const handle = index < COLLECTION_COLUMNS.length - 1
        ? `<span class="cw-collection-resize" role="separator" aria-orientation="vertical" title="Drag to resize, double-click to reset" data-collection-resize="${esc(col.key)}"></span>`
        : "";
      return `<span class="cw-collection-cell cw-collection-cell--${esc(col.key)}">${esc(col.label)}${handle}</span>`;
    }).join("");
    return `<div class="cw-collection-row cw-collection-row--head" role="presentation">${cells}</div>`;
  }

  const collectionReleaseIso = (item, meta) => {
    const movie = /^movie$/i.test(String(item?.type || item?.media_type || ""));
    const fromMeta = meta ? (movie ? (meta.detail?.release_date || meta.release?.date || "") : (meta.detail?.first_air_date || meta.release?.date || "")) : "";
    return String(fromMeta || item?.release_date || item?.first_air_date || item?.released || item?.premiered || "").trim();
  };

  const collectionGenreText = (item, meta) => {
    const raw = (meta && (meta.genres || meta.detail?.genres)) || item?.genres || item?.genre || [];
    const list = Array.isArray(raw) ? raw : String(raw || "").split(",");
    return list.map((g) => (typeof g === "string" ? g : (g?.name || g?.title || ""))).map((g) => String(g).trim()).filter(Boolean).slice(0, 3).join(", ");
  };

  function collectionListRow(item, index) {
    const key = storePosterItem(item);
    const [icon, kind] = collectionKind(item);
    const title = titleOf(item);
    const year = yearOf(item);
    const episode = episodeOf(item);
    const added = epochOf(item?.last_collected_at || item?.collected_at || item?.first_collected_at);
    const art = poster(item, "w342");
    const providerStrip = collectionProviderStrip(item);
    const stamp = episode || year;
    const stored = recallCollectionMeta(item);
    const release = collectionIsoFmt(collectionReleaseIso(item, null)) || stored?.r || "";
    const genres = collectionGenreText(item, null) || stored?.g || "";
    const selectKey = collectionSelectable() ? String(item?.key || "") : "";
    const img = `<img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">`;
    const cells = `<span class="cw-collection-cell cw-collection-cell--title">
        ${selectKey ? collectionSelectBox(selectKey) : ""}
        <strong>${esc(title)}</strong>
        ${stamp ? `<span class="cw-collection-pill">${esc(stamp)}</span>` : ""}
      </span>
      <span class="cw-collection-cell cw-collection-cell--rel">${esc(release || "—")}</span>
      <span class="cw-collection-cell cw-collection-cell--genre">${esc(genres || "—")}</span>
      <span class="cw-collection-cell cw-collection-cell--added">
        <strong>${esc(collectionDateFmt(added) || "—")}</strong>
        ${added ? `<small>${esc(relTime(added))}</small>` : ""}
      </span>
      <span class="cw-collection-cell cw-collection-cell--type"><span class="cw-collection-pill cw-collection-pill--type"><span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>${esc(kind)}</span></span>
      <span class="cw-collection-cell cw-collection-cell--providers">${providerStrip}</span>`;
    if (selectKey) {
      return `<article class="cw-collection-row cw-tl-row${collectionState.selected.has(selectKey) ? " is-selected" : ""}" data-collection-index="${index}" data-collection-key="${esc(selectKey)}">
      <button class="cw-collection-cell cw-collection-cell--poster cw-collection-open" type="button" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(title)}">${img}</button>
      ${cells}
    </article>`;
    }
    return `<button class="cw-collection-row" type="button" data-collection-index="${index}" data-profile-poster-key="${esc(key)}" aria-label="Show details for ${esc(title)}">
      <span class="cw-collection-cell cw-collection-cell--poster">${img}</span>
      ${cells}
    </button>`;
  }

  function applyCollectionMeta() {
    const grid = $("#profile-collection-grid");
    if (!grid || collectionState.view !== "list") return;
    grid.querySelectorAll("[data-collection-index]").forEach((row) => {
      const item = collectionState.items[Number(row.dataset.collectionIndex)];
      if (!item) return;
      const meta = window.CW?.Meta?.peek?.(item) || null;
      const relCell = row.querySelector(".cw-collection-cell--rel");
      const genreCell = row.querySelector(".cw-collection-cell--genre");
      const stored = meta ? null : recallCollectionMeta(item);
      const release = meta ? collectionIsoFmt(collectionReleaseIso(item, meta)) : (stored?.r || "");
      const genres = meta ? collectionGenreText(item, meta) : (stored?.g || "");
      if (meta) rememberCollectionMeta(item, release, genres);
      if (relCell) relCell.textContent = release || "—";
      if (genreCell) {
        genreCell.textContent = genres || "—";
        genreCell.title = genres;
      }
    });
    saveCollectionMetaCache();
  }

  let collectionMetaSeq = 0;
  const COLLECTION_META_TTL_MS = 7 * 24 * 60 * 60 * 1000;
  const COLLECTION_META_MAX = 4000;
  let collectionMetaStore = null;
  let collectionMetaDirty = false;

  const collectionMetaKey = (item) => {
    try {
      return String(window.CW?.Meta?.key?.(item) || "");
    } catch {
      return "";
    }
  };

  function collectionMetaCache() {
    if (collectionMetaStore) return collectionMetaStore;
    const stored = readCache(profileCacheKey("collection_meta"), COLLECTION_META_TTL_MS);
    collectionMetaStore = new Map(Object.entries(stored && typeof stored === "object" ? stored : {}));
    return collectionMetaStore;
  }

  function saveCollectionMetaCache() {
    if (!collectionMetaDirty || !collectionMetaStore) return;
    collectionMetaDirty = false;
    let entries = [...collectionMetaStore.entries()];
    if (entries.length > COLLECTION_META_MAX) entries = entries.slice(-COLLECTION_META_MAX);
    writeCache(profileCacheKey("collection_meta"), Object.fromEntries(entries));
  }

  function rememberCollectionMeta(item, release, genres) {
    const key = collectionMetaKey(item);
    if (!key || (!release && !genres)) return;
    const cache = collectionMetaCache();
    const prev = cache.get(key);
    if (prev && prev.r === release && prev.g === genres) return;
    cache.delete(key);
    cache.set(key, { r: release, g: genres });
    collectionMetaDirty = true;
  }

  function recallCollectionMeta(item) {
    const key = collectionMetaKey(item);
    return key ? collectionMetaCache().get(key) || null : null;
  }

  async function hydrateCollectionMeta() {
    if (collectionState.view !== "list" || !collectionState.items.length) return;
    const meta = window.CW?.Meta;
    if (typeof meta?.batch !== "function") return;
    const token = ++collectionMetaSeq;
    try {
      await meta.batch(collectionState.items, "row");
    } catch {
      return;
    }
    if (token === collectionMetaSeq) applyCollectionMeta();
  }

  function startCollectionResize(event, key) {
    const col = COLLECTION_COLUMNS.find((entry) => entry.key === key);
    if (!col || (event.button != null && event.button !== 0)) return;
    event.preventDefault();
    event.stopPropagation();
    const startX = event.clientX;
    const startWidth = collectionColumnWidth(col);
    document.body.classList.add("cw-column-resizing");
    const onMove = (moveEvent) => {
      collectionState.cols[col.key] = Math.max(col.min, Math.min(col.max, startWidth + moveEvent.clientX - startX));
      applyCollectionColumns();
    };
    const finish = () => {
      document.removeEventListener("pointermove", onMove);
      document.removeEventListener("pointerup", finish);
      document.removeEventListener("pointercancel", finish);
      document.body.classList.remove("cw-column-resizing");
      saveCollectionPrefs();
    };
    document.addEventListener("pointermove", onMove);
    document.addEventListener("pointerup", finish);
    document.addEventListener("pointercancel", finish);
  }

  function renderCollectionTypeChips(counts = {}) {
    const host = $("#profile-collection-types");
    if (!host) return;
    const rows = [
      ["all", "apps", "All"],
      ["movie", "movie", "Movies"],
      ["show", "tv", "Shows"],
      ["season", "stacks", "Seasons"],
      ["episode", "play_circle", "Episodes"],
    ];
    host.innerHTML = rows.map(([key, icon, label]) => {
      const count = Number(counts[key] || 0);
      return `<button class="${collectionState.type === key ? "active" : ""}" type="button" data-collection-type="${esc(key)}" aria-pressed="${collectionState.type === key}">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <span>${esc(label)}</span>
        <strong>${esc(numberFmt.format(count))}</strong>
      </button>`;
    }).join("");
  }

  function renderCollectionProviders(providers = []) {
    const select = $("#profile-collection-provider");
    if (!select) return;
    const current = collectionState.provider;
    const options = [`<option value="">All providers</option>`].concat((providers || []).map((row) => {
      const key = String(row?.provider || "").toLowerCase();
      const label = visibleProviderLabel(key) || key.toUpperCase();
      const count = Number(row?.count || 0);
      return `<option value="${esc(key)}" data-provider="${esc(key)}" data-label="${esc(label)}">${esc(label)} (${esc(numberFmt.format(count))})</option>`;
    }));
    select.innerHTML = options.join("");
    select.value = current;
    enhanceCollectionProviderSelect(select);
  }

  function enhanceCollectionProviderSelect(select = $("#profile-collection-provider")) {
    if (!select || typeof window.CW?.IconSelect?.enhance !== "function") return;
    window.CW.IconSelect.enhance(select, {
      className: "cw-profile-collection-select",
      menuClassName: "cw-profile-collection-menu",
      menuMinWidth: 260,
      getOptionData: (value, option) => {
        if (!value) return { label: "All providers", icons: [{ symbol: "hub" }] };
        const provider = option?.dataset?.provider || value;
        const label = option?.dataset?.label || visibleProviderLabel(provider) || String(provider || "").toUpperCase();
        const logo = providerLogLogo(provider);
        return {
          label: option?.textContent?.trim() || label,
          icons: logo ? [{ src: logo, alt: label }] : [{ text: label.slice(0, 2) || "?" }],
        };
      },
    });
  }

  function enhanceCollectionSortSelect(select = $("#profile-collection-sort")) {
    if (!select || typeof window.CW?.IconSelect?.enhance !== "function") return;
    const sortIcons = {
      collected_at: "schedule",
      collected_at_asc: "history",
      title: "sort_by_alpha",
      title_desc: "sort_by_alpha",
      year_desc: "calendar_month",
      year_asc: "calendar_month",
    };
    window.CW.IconSelect.enhance(select, {
      className: "cw-profile-collection-select",
      menuClassName: "cw-profile-collection-menu",
      menuMinWidth: 230,
      getOptionData: (value, option) => ({
        label: option?.textContent?.trim() || "Sort",
        icons: [{ symbol: sortIcons[value] || "sort" }],
      }),
    });
  }

  function renderCollectionMetrics(data = {}) {
    const host = $("#profile-collection-metrics");
    if (!host) return;
    const counts = data.counts || {};
    const providers = Array.isArray(data.providers) ? data.providers.length : 0;
    const values = [
      ["violet", "movie", "Movies", "Owned", counts.movie || 0],
      ["blue", "tv", "Shows", "Owned", counts.show || 0],
      ["green", "hub", "Providers", "Connected", providers],
      ["amber", "inventory_2", "Items", "Total", counts.all ?? data.total ?? 0],
    ];
    host.innerHTML = values.map(([tone, icon, label, note, value]) => `<span class="cw-collection-tile" data-tone="${esc(tone)}">
      <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
      <span class="cw-collection-tile-body">
        <strong>${esc(numberFmt.format(Number(value) || 0))}</strong>
        <span>${esc(label)}</span>
        <small>${esc(note)}</small>
      </span>
    </span>`).join("");
  }

  function paintCollectionItems() {
    const grid = $("#profile-collection-grid");
    if (!grid) return;
    const list = collectionState.view === "list";
    grid.dataset.view = list ? "list" : "grid";
    grid.classList.remove("is-grouped");
    if (!collectionState.items.length) {
      grid.innerHTML = empty("No collection items match this view.");
      syncCollectionSelection();
      return;
    }
    if (list) {
      grid.innerHTML = collectionListHead() + collectionState.items.map(collectionListRow).join("");
      applyCollectionColumns();
      applyCollectionMeta();
      void hydrateCollectionMeta();
      syncCollectionSelection();
      return;
    }
    const groups = [];
    if (collectionState.timeline && String(collectionState.sort || "").startsWith("collected_at")) {
      for (const item of collectionState.items) {
        const epoch = epochOf(item?.last_collected_at || item?.collected_at || item?.first_collected_at);
        const key = timelineDayKey(epoch);
        const last = groups[groups.length - 1];
        if (last && last.key === key) last.items.push(item);
        else groups.push({ key, epoch, items: [item] });
      }
    }
    if (!groups.some((group) => group.epoch)) {
      grid.innerHTML = collectionState.items.map(collectionCard).join("");
    } else {
      const dayFmt = timelineFormatter({ weekday: "long", day: "numeric", month: "long", year: "numeric" });
      grid.classList.add("is-grouped");
      grid.innerHTML = groups.map((group) => `<section class="cw-collection-day" data-timeline-month="${esc(timelineMonthKey(group.epoch))}">
        <div class="cw-hist-day-head">
          <span class="material-symbols-rounded" aria-hidden="true">inventory_2</span>
          <h3>${esc(group.epoch ? dayFmt(group.epoch) : "No date")}</h3>
          <span class="cw-hist-day-count">${esc(`${numberFmt.format(group.items.length)} ${group.items.length === 1 ? "title" : "titles"}`)}</span>
        </div>
        <div class="cw-collection-day-body">${group.items.map(collectionCard).join("")}</div>
      </section>`).join("");
    }
    syncCollectionSelection();
  }

  function setCollectionView(view) {
    const next = view === "list" ? "list" : "grid";
    if (next === collectionState.view) return;
    collectionState.view = next;
    saveCollectionPrefs();
    syncCollectionViewButtons();
    paintCollectionItems();
  }

  function syncCollectionViewButtons() {
    document.querySelectorAll("[data-collection-view]").forEach((btn) => {
      const active = btn.dataset.collectionView === collectionState.view;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", String(active));
    });
    document.querySelectorAll("[data-collection-timeline]").forEach((btn) => {
      btn.classList.toggle("active", collectionState.timeline);
      btn.setAttribute("aria-pressed", String(collectionState.timeline));
      btn.title = collectionState.timeline ? "Hide timeline" : "Show timeline";
    });
  }

  function renderCollection(data = {}) {
    const grid = $("#profile-collection-grid");
    if (!grid) return;
    collectionState.total = Number(data.total || 0);
    renderCollectionMetrics(data);
    renderCollectionTypeChips(data.counts || {});
    renderCollectionProviders(data.providers || []);
    paintCollectionItems();
    renderCollectionPager(data);
  }

  function collectionPageNumbers(page, pageCount) {
    if (pageCount <= 7) return Array.from({ length: pageCount }, (_, i) => i + 1);
    let from = Math.max(2, page - 1);
    let to = Math.min(pageCount - 1, page + 1);
    if (page <= 3) { from = 2; to = 3; }
    else if (page >= pageCount - 2) { from = pageCount - 2; to = pageCount - 1; }
    const out = [1];
    if (from > 2) out.push("gap");
    for (let i = from; i <= to; i += 1) out.push(i);
    if (to < pageCount - 1) out.push("gap");
    out.push(pageCount);
    return out;
  }

  function renderCollectionPager(data = {}) {
    const footer = $("#profile-collection-footer");
    if (!footer) return;
    const total = Number(data.total || 0);
    const pageSize = Number(data.page_size || collectionState.pageSize) || collectionState.pageSize;
    const pageCount = total ? Math.ceil(total / pageSize) : 1;
    const page = Math.min(Math.max(1, Number(data.page || collectionState.page) || 1), pageCount);
    collectionState.page = page;
    collectionState.pageCount = pageCount;
    footer.hidden = !total;
    const start = total ? (page - 1) * pageSize : 0;
    const end = total ? Math.min(start + pageSize, total) : 0;
    const label = $("#profile-collection-page-label");
    if (label) label.textContent = total ? `Showing ${numberFmt.format(start + 1)}–${numberFmt.format(end)} of ${numberFmt.format(total)}` : "";
    const pages = $("#profile-collection-pages");
    if (pages) {
      const step = (target, icon, title, disabled) => `<button type="button" data-collection-page="${target}" title="${title}" aria-label="${title}"${disabled ? " disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></button>`;
      const numbers = collectionPageNumbers(page, pageCount).map((entry) => {
        if (entry === "gap") return `<span class="cw-collection-gap" aria-hidden="true">…</span>`;
        const active = entry === page;
        return `<button type="button" class="${active ? "active" : ""}" data-collection-page="${entry}"${active ? ' aria-current="page" disabled' : ""}>${numberFmt.format(entry)}</button>`;
      }).join("");
      pages.innerHTML = step(page - 1, "chevron_left", "Previous page", page <= 1) + numbers + step(page + 1, "chevron_right", "Next page", page >= pageCount);
    }
  }

  function goToCollectionPage(page) {
    const target = Math.max(1, Number(page) || 1);
    if (collectionState.loading || target === collectionState.page) return;
    if (collectionState.pageCount && target > collectionState.pageCount) return;
    collectionState.page = target;
    void loadCollection();
    $("#profile-panel-collection")?.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  async function loadCollection({ reset = false, month = "" } = {}) {
    if (collectionState.loading) return;
    if (reset) collectionState.page = 1;
    collectionState.items = [];
    const skeletonGrid = $("#profile-collection-grid");
    if (skeletonGrid) {
      skeletonGrid.dataset.view = collectionState.view === "list" ? "list" : "grid";
      skeletonGrid.innerHTML = collectionState.view === "list" ? skeleton("collectionRow", 6) : skeleton("collection", 7);
    }
    collectionState.loading = true;
    lockCollectionPager();
    const params = new URLSearchParams({
      type: collectionState.type,
      provider: collectionState.provider,
      search: collectionState.search,
      sort: collectionState.sort,
      page: String(collectionState.page),
      page_size: String(collectionState.pageSize),
    });
    if (month) params.set("month", month);
    try {
      const data = await api(`/api/profile/collection?${params.toString()}`);
      collectionState.items = Array.isArray(data.items) ? data.items : [];
      collectionState.loaded = true;
      renderCollection(data);
      collectionState.months = Array.isArray(data.months) ? data.months : [];
      renderCollectionTimeline();
      if (month) {
        const grid = $("#profile-collection-grid");
        (grid?.querySelector(`[data-timeline-month="${month}"]`) || grid)?.scrollIntoView({ block: "start", behavior: "auto" });
      }
    } catch (e) {
      const grid = $("#profile-collection-grid");
      if (grid) grid.innerHTML = empty("Collections could not be loaded.");
      renderCollectionPager({ total: collectionState.total, page: collectionState.page, page_size: collectionState.pageSize });
      toast(e.message || "Collections could not be loaded", true);
    } finally {
      collectionState.loading = false;
    }
  }

  let collectionMonthBar = null;
  const collectionBar = () => collectionMonthBar || (collectionMonthBar = createMonthBar({
    host: () => $("#profile-collection-timeline"),
    noun: (value) => `${numberFmt.format(value)} ${Number(value) === 1 ? "title" : "titles"}`,
    zoom: collectionState.zoom,
    onZoom: (level) => {
      collectionState.zoom = level;
      saveCollectionPrefs();
    },
    onJump: (month) => void loadCollection({ month }),
  }));

  function renderCollectionTimeline() {
    const dated = String(collectionState.sort || "").startsWith("collected_at");
    collectionBar().render(collectionState.timeline && dated ? collectionState.months : [], {
      visible: new Set(collectionState.items.map((item) => timelineMonthKey(epochOf(item?.last_collected_at || item?.collected_at || item?.first_collected_at)))),
      key: "collection",
    });
  }

  function wireCollection() {
    let searchTimer = null;
    collectionBar().wire();
    const collectionPanel = $("#profile-panel-collection");
    collectionPanel?.addEventListener("click", (event) => {
      const pick = event.target?.closest?.("[data-collection-select]");
      if (pick) {
        void selectCollection(pick.dataset.collectionSelect);
        return;
      }
      if (event.target?.closest?.("[data-collection-timeline]")) {
        collectionState.timeline = !collectionState.timeline;
        saveCollectionPrefs();
        syncCollectionViewButtons();
        paintCollectionItems();
        renderCollectionTimeline();
        return;
      }
      const collectionRefresh = event.target?.closest?.("[data-collection-refresh]");
      if (collectionRefresh) {
        if (collectionRefresh.disabled) return;
        collectionRefresh.disabled = true;
        collectionRefresh.classList.add("is-spinning");
        void Promise.all([loadCollection({ reset: true }), new Promise((done) => setTimeout(done, 450))]).finally(() => {
          collectionRefresh.disabled = false;
          collectionRefresh.classList.remove("is-spinning");
        });
        return;
      }
      if (event.target?.closest?.("[data-collection-remove]")) removeCollectionSelection();
    });
    collectionPanel?.addEventListener("change", (event) => {
      const box = event.target?.closest?.("[data-collection-select-item]");
      if (!box) return;
      const key = box.dataset.collectionSelectItem;
      const item = collectionState.items.find((entry) => String(entry?.key || "") === key);
      if (box.checked && item) collectionState.selected.set(key, collectionSelectionEntry(item));
      else collectionState.selected.delete(key);
      syncCollectionSelection();
    });
    document.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-collection-view]");
      if (!btn) return;
      setCollectionView(btn.dataset.collectionView);
    });
    syncCollectionViewButtons();
    document.addEventListener("pointerdown", (event) => {
      const handle = event.target?.closest?.("[data-collection-resize]");
      if (!handle) return;
      startCollectionResize(event, handle.dataset.collectionResize);
    });
    document.addEventListener("dblclick", (event) => {
      const handle = event.target?.closest?.("[data-collection-resize]");
      if (!handle) return;
      event.preventDefault();
      delete collectionState.cols[handle.dataset.collectionResize];
      applyCollectionColumns();
      saveCollectionPrefs();
    });
    enhanceCollectionProviderSelect();
    enhanceCollectionSortSelect();
    $("#profile-collection-types")?.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-collection-type]");
      if (!btn) return;
      collectionState.type = btn.dataset.collectionType || "all";
      void loadCollection({ reset: true });
    });
    $("#profile-collection-provider")?.addEventListener("change", (event) => {
      collectionState.provider = event.target?.value || "";
      void loadCollection({ reset: true });
    });
    $("#profile-collection-sort")?.addEventListener("change", (event) => {
      collectionState.sort = event.target?.value || "collected_at";
      void loadCollection({ reset: true });
    });
    $("#profile-collection-search")?.addEventListener("input", (event) => {
      collectionState.search = event.target?.value || "";
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => loadCollection({ reset: true }), 220);
    });
    document.addEventListener("keydown", (event) => {
      if (!(event.ctrlKey || event.metaKey) || String(event.key || "").toLowerCase() !== "k") return;
      if (!$("#profile-panel-collection")?.classList?.contains("active")) return;
      const input = $("#profile-collection-search");
      if (!input) return;
      event.preventDefault();
      input.focus();
      input.select?.();
    });
    $("#profile-collection-pages")?.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-collection-page]");
      if (!btn || btn.disabled) return;
      goToCollectionPage(Number(btn.dataset.collectionPage));
    });
    const pageSizeSelect = $("#profile-collection-page-size");
    if (pageSizeSelect) {
      pageSizeSelect.value = String(collectionState.pageSize);
      pageSizeSelect.addEventListener("change", (event) => {
        const next = Number(event.target?.value) || 24;
        if (next === collectionState.pageSize) return;
        collectionState.pageSize = next;
        saveCollectionPrefs();
        void loadCollection({ reset: true });
      });
    }
    syncCollectionViewButtons();
  }

  const TIMELINE_ZOOM_STEPS = [1, 1.6, 2.5, 4];
  const TIMELINE_PAGE_SIZES = [24, 48, 96];
  const TIMELINE_COVERAGE = ["partial", "full", "mismatch"];

  const timelineFormatter = (options) => {
    let fmt;
    try {
      fmt = new Intl.DateTimeFormat(window.__CW_LOCALE || navigator.language || undefined, options);
    } catch {
      fmt = new Intl.DateTimeFormat(undefined, options);
    }
    return (epoch) => (epoch ? fmt.format(new Date(epoch * 1000)) : "");
  };

  const timelinePad = (value) => String(value).padStart(2, "0");

  const timelineDayKey = (epoch) => {
    if (!epoch) return "";
    const date = new Date(epoch * 1000);
    return `${date.getFullYear()}-${timelinePad(date.getMonth() + 1)}-${timelinePad(date.getDate())}`;
  };

  const timelineMonthKey = (epoch) => (epoch ? new Date(epoch * 1000).toISOString().slice(0, 7) : "");

  function timelineEndpoints(rows) {
    const out = [];
    const seen = new Set();
    for (const row of Array.isArray(rows) ? rows : []) {
      const provider = String(row?.provider || "").trim().toUpperCase();
      const instance = String(row?.instance || "default").trim() || "default";
      const key = `${provider}:${instance}`;
      if (!provider || seen.has(key)) continue;
      seen.add(key);
      const rating = Number(row?.rating);
      out.push({ provider, instance, rating: row?.rating != null && Number.isFinite(rating) ? rating : null });
    }
    return out;
  }

  function timelineChipTitle(ref, role, rating = null) {
    const name = visibleProviderLabel(ref.provider) || ref.provider;
    const label = String(ref.instance || "default").toLowerCase() === "default" ? name : `${name} (${ref.instance})`;
    const verbs = { present: "On", missing: "Not on", source: "Played on", sink: "Sent to" };
    return `${verbs[role] || ""} ${label}${rating != null ? ` · ${rating}/10` : ""}`.trim();
  }

  const PROVIDER_CHIP_MAX = 6;

  function providerChipStrip(entries, moreClass) {
    if (entries.length <= PROVIDER_CHIP_MAX) return entries.map((entry) => entry.html).join("");
    const ordered = entries.filter((entry) => entry.missing).concat(entries.filter((entry) => !entry.missing));
    const shown = ordered.slice(0, PROVIDER_CHIP_MAX - 1);
    const hidden = ordered.slice(PROVIDER_CHIP_MAX - 1);
    const names = hidden.map((entry) => entry.title);
    return `${shown.map((entry) => entry.html).join("")}<span class="${esc(moreClass)} cw-provider-more" title="${esc(names.join("\n"))}" aria-label="${esc(`${hidden.length} more: ${names.join(", ")}`)}">+${hidden.length}</span>`;
  }

  function timelineChipEntry(ref, role, options = {}) {
    return { html: timelineProviderChip(ref, role, options), title: timelineChipTitle(ref, role, options.rating ?? null), missing: role === "missing" };
  }

  function timelineProviderChip(ref, role, options = {}) {
    const name = visibleProviderLabel(ref.provider) || ref.provider;
    const rating = options.rating ?? null;
    const title = timelineChipTitle(ref, role, rating);
    const logo = providerLogo(ref.provider);
    const mark = logo ? `<img src="${esc(logo)}" alt="" loading="lazy">` : `<span>${esc(name.slice(0, 2))}</span>`;
    const value = rating != null ? `<b>${esc(rating)}</b>` : "";
    const cls = ["cw-hist-provider", `is-${role}`, value ? "has-value" : "", options.differs ? "is-differs" : ""].filter(Boolean).join(" ");
    return `<span class="${esc(cls)}" title="${esc(title)}" aria-label="${esc(title)}">${mark}${value}</span>`;
  }

  function timelineCoverage(item, withRatings = false) {
    const present = timelineEndpoints(item?.present);
    const missing = timelineEndpoints(item?.missing);
    const total = present.length + missing.length;
    const full = total > 0 && !missing.length;
    const base = Number(item?.rating);
    const conflict = withRatings && item?.agree === false;
    const chips = providerChipStrip(present
      .map((ref) => timelineChipEntry(ref, "present", withRatings ? { rating: ref.rating, differs: ref.rating != null && ref.rating !== base } : {}))
      .concat(missing.map((ref) => timelineChipEntry(ref, "missing"))), "cw-hist-provider");
    const label = conflict
      ? "Scores differ"
      : full
        ? (total === 1 ? "On 1 provider" : "On every provider")
        : `On ${numberFmt.format(present.length)} of ${numberFmt.format(total)}`;
    const state = conflict ? " is-conflict" : full ? " is-full" : "";
    return `<span class="cw-hist-coverage${state}"><span class="cw-hist-providers">${chips}</span><span class="cw-hist-coverage-label">${esc(label)}</span></span>`;
  }

  function timelineMethod(item) {
    const method = String(item?.method || "").toLowerCase();
    const event = String(item?.event || "").toLowerCase();
    if (method === "webhook") return ["webhook", "Webhook"];
    if (event.includes("history")) return ["sync_alt", "History sync"];
    if (method === "watcher") return ["radar", "Watcher"];
    return ["sensors", "Scrobble"];
  }

  function timelineRoute(item) {
    const source = timelineEndpoints([item?.source])[0] || null;
    const sourceKey = source ? `${source.provider}:${source.instance}` : "";
    const sinks = timelineEndpoints(item?.targets).filter((ref) => `${ref.provider}:${ref.instance}` !== sourceKey);
    const from = source ? timelineProviderChip(source, "source") : "";
    const to = sinks.length
      ? `<span class="material-symbols-rounded cw-hist-route-arrow" aria-hidden="true">arrow_forward</span><span class="cw-hist-providers">${providerChipStrip(sinks.map((ref) => timelineChipEntry(ref, "sink")), "cw-hist-provider")}</span>`
      : "";
    const label = sinks.length ? `Sent to ${numberFmt.format(sinks.length)}` : "Not sent";
    return `<span class="cw-hist-route${sinks.length ? "" : " is-unsent"}">${from}${to}<span class="cw-hist-coverage-label">${esc(label)}</span></span>`;
  }

  function timelineArt(item) {
    const cover = poster(item, "w342");
    const art = watchlistPreviewArt(item, "w300") || cover;
    const fallback = art === cover ? "" : cover;
    return `<img data-src="${esc(art)}" data-fallback="${esc(fallback)}" alt="" decoding="async" onerror="const f=this.dataset.fallback;this.dataset.fallback='';if(f){this.src=f}else{this.onerror=null;this.src='/assets/img/placeholder_poster.svg'}">`;
  }

  const timelineImages = (() => {
    const MAX_ACTIVE = 6;
    const queue = new Set();
    const inflight = new Map();
    let scheduled = 0;

    const distance = (img) => {
      const rect = img.getBoundingClientRect();
      return Math.abs(rect.top + rect.height / 2 - window.innerHeight / 2);
    };

    function schedule() {
      if (!scheduled) scheduled = requestAnimationFrame(pump);
    }

    function finish(img) {
      if (!inflight.has(img)) return;
      clearTimeout(inflight.get(img));
      inflight.delete(img);
      schedule();
    }

    function start(img) {
      const src = img.dataset.src;
      delete img.dataset.src;
      if (!src) return;
      img.addEventListener("load", () => img.classList.add("is-loaded"));
      img.addEventListener("load", () => finish(img), { once: true });
      img.addEventListener("error", () => finish(img), { once: true });
      inflight.set(img, setTimeout(() => finish(img), 15000));
      img.src = src;
    }

    const observer = typeof IntersectionObserver === "function"
      ? new IntersectionObserver((entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) queue.add(entry.target);
          else queue.delete(entry.target);
        }
        schedule();
      }, { rootMargin: "240px 0px" })
      : null;

    function pump() {
      scheduled = 0;
      if (!queue.size || inflight.size >= MAX_ACTIVE) return;
      const ordered = [...queue].sort((a, b) => distance(a) - distance(b));
      for (const img of ordered) {
        if (inflight.size >= MAX_ACTIVE) break;
        queue.delete(img);
        observer?.unobserve(img);
        start(img);
      }
    }

    return {
      observe(root) {
        root?.querySelectorAll?.("img[data-src]").forEach((img) => {
          if (observer) observer.observe(img);
          else start(img);
        });
      },
      release(root) {
        if (!root) return;
        root.querySelectorAll("img").forEach((img) => {
          observer?.unobserve(img);
          queue.delete(img);
          if (inflight.has(img)) {
            clearTimeout(inflight.get(img));
            inflight.delete(img);
          }
        });
        schedule();
      },
    };
  })();

  function timelineKind(item) {
    const type = String(item?.type || "").toLowerCase();
    if (type === "episode") return ["play_circle", "Episode"];
    if (type === "season") return ["stacks", "Season"];
    if (type === "show" || type === "tv") return ["tv", "Show"];
    if (type === "anime") return ["animation", "Anime"];
    return ["movie", "Movie"];
  }

  function timelineCode(item) {
    const type = String(item?.type || "").toLowerCase();
    if (type === "episode") return episodeOf(item);
    if (type === "season" && item?.season != null) return `S${timelinePad(item.season)}`;
    return "";
  }

  function timelineSubtitle(item) {
    const type = String(item?.type || "").toLowerCase();
    const season = item?.season;
    const episode = item?.episode;
    if (type === "episode") return season != null && episode != null ? `Season ${season} · Episode ${episode}` : "Episode";
    if (type === "season") return season != null ? `Season ${season}` : "Season";
    if (type === "show" || type === "tv") return [yearOf(item), "Show"].filter(Boolean).join(" · ");
    if (type === "anime") return [yearOf(item), "Anime"].filter(Boolean).join(" · ");
    return [yearOf(item), "Movie"].filter(Boolean).join(" · ");
  }

  const TIMELINE_ID_KEYS = ["tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist", "mal"];

  function timelineIds(item) {
    const ids = item?.ids && typeof item.ids === "object" ? item.ids : {};
    const chips = TIMELINE_ID_KEYS
      .map((name) => [name, name === "tmdb" ? (ids.tmdb || item?.tmdb) : ids[name]])
      .filter(([, value]) => value != null && value !== "" && typeof value !== "object")
      .map(([name, value]) => `<span class="cw-tl-id"><b>${esc(name.toUpperCase())}</b>${esc(value)}</span>`)
      .join("");
    const key = item?.key ? `<span class="cw-tl-id cw-tl-id--key" title="${esc(item.key)}"><b>KEY</b>${esc(item.key)}</span>` : "";
    return chips || key ? `<span class="cw-tl-ids">${chips}${key}</span>` : "";
  }

  const providerInstanceLabels = (() => {
    let labels = null;
    let pending = null;
    const load = () => {
      if (!pending) {
        pending = fetch("/api/provider-instances", { credentials: "same-origin", cache: "no-store" })
          .then((res) => (res.ok ? res.json() : {}))
          .catch(() => ({}))
          .then((data) => {
            labels = new Map();
            for (const [provider, rows] of Object.entries(data && typeof data === "object" ? data : {})) {
              for (const row of Array.isArray(rows) ? rows : []) {
                if (row?.id) labels.set(`${String(provider).toUpperCase()}:${row.id}`, String(row.display_label || row.friendly_label || row.label || row.id));
              }
            }
            return labels;
          });
      }
      return pending;
    };
    const get = (provider, instance) => labels?.get(`${String(provider).toUpperCase()}:${instance}`) || (String(instance) === "default" ? "Default" : String(instance));
    return { load, get };
  })();

  const recordRemoval = (() => {
    const NOTES = {
      history: { main: "Removes the watched status of the selected titles on the chosen providers.", detail: "Every watch is removed, FLOPPY removes one watch per title. Other sync sources can restore them." },
      ratings: { main: "Removes the selected ratings from the chosen providers.", detail: "Other sync sources can restore them." },
      collection: { main: "Removes the selected titles from the chosen provider collections.", detail: "Other sync sources can restore them." },
      watchlist: { main: "Removes the selected titles from the chosen provider watchlists.", detail: "Other sync sources can restore them." },
    };
    const PLACES = { history: "provider history", ratings: "provider ratings", collection: "provider collections", watchlist: "provider watchlists" };
    const CONFIRM_MS = 4200;
    const state = { ctx: null, targets: [], picked: new Set(), previewId: "", empty: null, busy: false, armedUntil: 0, generation: 0 };
    const emptyHtml = ({ icon = "info", title = "", text = "", tone = "", spin = false } = {}) => `<div class="cw-tl-remove-empty${tone ? ` is-${tone}` : ""}">
        <span class="material-symbols-rounded${spin ? " is-spinning" : ""}" aria-hidden="true">${esc(icon)}</span>
        <span class="cw-tl-remove-empty-copy"><strong>${esc(title)}</strong>${text ? `<small>${esc(text)}</small>` : ""}</span>
      </div>`;
    let armTimer = 0;
    let wired = false;
    const node = () => $("#profile-remove-dialog");
    const part = (id) => $(`#profile-remove-${id}`);
    const count = (value) => numberFmt.format(Number(value) || 0);
    const targetKey = (target) => `${target.provider}:${target.instance}`;
    const chosen = () => state.targets.filter((target) => state.picked.has(targetKey(target)));

    function sendItem(entry) {
      const out = { key: entry.key };
      for (const field of ["type", "title", "year", "season", "episode"]) {
        if (entry[field] != null && entry[field] !== "") out[field] = entry[field];
      }
      const raw = entry.ids && typeof entry.ids === "object" ? entry.ids : {};
      const nested = raw.show_ids && typeof raw.show_ids === "object" ? raw.show_ids : {};
      const extra = entry.show_ids && typeof entry.show_ids === "object" ? entry.show_ids : {};
      const ids = Object.fromEntries(Object.entries(raw).filter(([, value]) => value != null && value !== "" && typeof value !== "object"));
      const showIds = { ...nested, ...extra };
      if (Object.keys(ids).length) out.ids = ids;
      if (Object.keys(showIds).length) out.show_ids = showIds;
      return out;
    }

    function removable(entry) {
      const item = sendItem(entry);
      const hasIds = [item.ids, item.show_ids].some((ids) => ids && Object.values(ids).some((value) => value != null && value !== ""));
      if (!hasIds) return false;
      const type = String(entry.type || "").toLowerCase();
      if (type !== "episode" && type !== "season") return true;
      const season = Number(entry.season);
      if (!Number.isInteger(season) || season < 0) return false;
      const episode = Number(entry.episode);
      return type === "season" || (Number.isInteger(episode) && episode >= 1);
    }

    async function postJson(url, body, ms = 180000) {
      const controller = typeof AbortController === "function" ? new AbortController() : null;
      const timer = controller ? window.setTimeout(() => controller.abort("timeout"), ms) : 0;
      try {
        const res = await fetch(url, { method: "POST", cache: "no-store", credentials: "same-origin", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body), signal: controller?.signal });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error((typeof data?.detail === "string" && data.detail) || data?.error || `HTTP ${res.status}`);
        return data;
      } finally {
        if (timer) window.clearTimeout(timer);
      }
    }

    function watchlistTargets(entries) {
      const map = new Map();
      for (const entry of entries) {
        for (const ref of timelineEndpoints(entry.present)) {
          const key = targetKey(ref);
          const target = map.get(key) || { provider: ref.provider, instance: ref.instance, label: "", instanceLabel: "", matched: 0, count: 0, entries: [] };
          target.matched += 1;
          target.count += 1;
          target.entries.push(entry);
          map.set(key, target);
        }
      }
      return [...map.values()].sort((a, b) => a.provider.localeCompare(b.provider) || (a.instance !== "default") - (b.instance !== "default") || a.instance.localeCompare(b.instance));
    }

    function syncActions() {
      const dialog = node();
      if (!dialog) return;
      const records = chosen().reduce((sum, target) => sum + target.count, 0);
      const armed = Date.now() < state.armedUntil;
      const submit = dialog.querySelector("[data-remove-submit]");
      if (submit) {
        submit.disabled = state.busy || !records;
        submit.classList.toggle("is-confirming", armed);
        submit.firstElementChild.textContent = armed ? "warning" : "delete";
        submit.lastElementChild.textContent = armed
          ? `Confirm remove ${count(records)}`
          : (records ? `Remove ${count(records)} record${records === 1 ? "" : "s"}` : "Remove");
      }
      const all = dialog.querySelector("[data-remove-all]");
      if (all) {
        all.disabled = state.busy || !state.targets.length;
        all.textContent = state.targets.length && state.targets.every((target) => state.picked.has(targetKey(target))) ? "Select none" : "Select all";
      }
      dialog.querySelectorAll("[data-remove-close]").forEach((button) => {
        button.disabled = state.busy;
      });
    }

    function render() {
      const list = part("targets");
      if (!list) return;
      list.innerHTML = state.targets.length ? state.targets.map((target) => {
        const key = targetKey(target);
        const on = state.picked.has(key);
        const name = target.label || visibleProviderLabel(target.provider) || target.provider;
        const profile = target.instanceLabel || providerInstanceLabels.get(target.provider, target.instance);
        return `<label class="cw-tl-remove-target${on ? " is-on" : ""}">
          <input type="checkbox" data-remove-target="${esc(key)}"${on ? " checked" : ""}${state.busy ? " disabled" : ""}>
          ${providerIconHtml(target.provider)}
          <span class="cw-tl-remove-copy"><strong>${esc(name)}</strong><small>${esc(profile)} · ${count(target.matched)} selected</small></span>
        </label>`;
      }).join("") : emptyHtml(state.empty || {});
      syncActions();
    }

    async function loadTargets() {
      const ctx = state.ctx;
      const generation = ++state.generation;
      state.targets = [];
      state.previewId = "";
      if (!ctx.entries.length) {
        state.empty = { icon: "link_off", title: "Nothing to match", text: "None of the selected rows have ids CrossWatch can match on your providers." };
      } else if (ctx.mode === "watchlist") {
        state.targets = watchlistTargets(ctx.entries);
        state.empty = { icon: "bookmark_remove", title: "Not on any watchlist", text: "None of the selected titles are on a provider watchlist." };
      } else {
        part("targets").innerHTML = emptyHtml({ icon: "progress_activity", title: "Checking providers", text: "Looking up where the selection is synced.", spin: true });
        syncActions();
        try {
          const data = await postJson("/api/editor/send/preview", { kind: ctx.kind, items: ctx.entries.map(sendItem) }, 60000);
          if (generation !== state.generation) return;
          state.previewId = String(data.preview_id || "");
          state.targets = (Array.isArray(data.providers) ? data.providers : []).map((row) => ({
            provider: String(row.provider || "").toUpperCase(),
            instance: String(row.instance || "default"),
            label: row.label || row.display || "",
            instanceLabel: row.instance_label || "",
            matched: Number(row.matched) || 0,
            count: Number(row.count) || 0,
          }));
          state.empty = data.empty_reason === "pair_baseline_unavailable"
            ? { icon: "sync_problem", tone: "warn", title: "Run your sync pairs first", text: "CrossWatch only removes records it has seen in a sync run. One or more pairs have not run yet for this feature." }
            : { icon: "search_off", title: "No matching records", text: "No provider profile has these records in a sync pair that supports removal." };
        } catch (e) {
          if (generation !== state.generation) return;
          state.empty = { icon: "error", tone: "bad", title: "Could not check providers", text: e.message || "Try again in a moment." };
        }
      }
      if (state.autoPick && state.targets.length) {
        state.picked = new Set(state.targets.map(targetKey));
        state.autoPick = false;
      }
      state.picked = new Set([...state.picked].filter((key) => state.targets.some((target) => targetKey(target) === key)));
      render();
    }

    async function removeEditor(ctx, targets) {
      const data = await postJson("/api/editor/send", {
        kind: ctx.kind,
        operation: "remove",
        confirmed: true,
        preview_id: state.previewId,
        providers: targets.map(({ provider, instance }) => ({ provider, instance })),
        items: ctx.entries.map(sendItem),
      });
      return (Array.isArray(data.results) ? data.results : []).map((row) => ({
        provider: String(row.provider || "").toUpperCase(),
        instance: String(row.instance || "default"),
        label: row.display || row.label || "",
        removed: Number(row.result?.removed ?? row.result?.confirmed) || 0,
        skipped: Number(row.result?.skipped) || 0,
        unresolved: Number(row.result?.unresolved) || 0,
        error: row.error ? "Removal failed" : "",
      }));
    }

    async function removeWatchlist(targets) {
      const rows = [];
      for (const target of targets) {
        let removed = 0;
        let error = "";
        for (let start = 0; start < target.entries.length; start += 50) {
          const keys = target.entries.slice(start, start + 50).map((entry) => (entry.aliases.length ? { key: entry.key, aliases: entry.aliases } : { key: entry.key }));
          try {
            const data = await postJson("/api/watchlist/delete", { provider: target.provider, provider_instance: target.instance, keys });
            removed += Number(data?.deleted_ok) || 0;
            if (data?.error) error = String(data.error);
          } catch (e) {
            error = e.message || "Removal failed";
          }
        }
        rows.push({ provider: target.provider, instance: target.instance, label: "", removed, skipped: Math.max(0, target.entries.length - removed), unresolved: 0, error });
      }
      return rows;
    }

    function resultHtml(rows) {
      return rows.map((row) => {
        const name = row.label || visibleProviderLabel(row.provider) || row.provider;
        const parts = [`${count(row.removed)} removed`];
        if (row.skipped) parts.push(`${count(row.skipped)} not found`);
        if (row.unresolved) parts.push(`${count(row.unresolved)} unresolved`);
        const bad = !!row.error || row.unresolved > 0;
        return `<div class="cw-tl-remove-line${bad ? " is-bad" : ""}">
          <span class="material-symbols-rounded" aria-hidden="true">${row.error ? "error" : bad ? "warning" : "check_circle"}</span>
          <strong>${esc(name)}</strong><small>${esc(providerInstanceLabels.get(row.provider, row.instance))}</small>
          <span>${esc(row.error || parts.join(", "))}</span>
        </div>`;
      }).join("");
    }

    async function submit() {
      const ctx = state.ctx;
      const targets = chosen();
      if (!ctx || state.busy || !targets.length) return;
      if (Date.now() >= state.armedUntil) {
        state.armedUntil = Date.now() + CONFIRM_MS;
        clearTimeout(armTimer);
        armTimer = setTimeout(() => {
          state.armedUntil = 0;
          syncActions();
        }, CONFIRM_MS);
        syncActions();
        return;
      }
      state.armedUntil = 0;
      clearTimeout(armTimer);
      state.busy = true;
      part("error").textContent = "";
      part("result").innerHTML = `<div class="cw-tl-remove-line is-running"><span class="material-symbols-rounded" aria-hidden="true">progress_activity</span><span>Removing from ${count(targets.length)} provider profile${targets.length === 1 ? "" : "s"}...</span></div>`;
      render();
      let rows = null;
      try {
        rows = ctx.mode === "watchlist" ? await removeWatchlist(targets) : await removeEditor(ctx, targets);
      } catch (e) {
        part("error").textContent = e.message || "Removal failed";
      }
      state.busy = false;
      state.picked = new Set();
      part("result").innerHTML = rows ? resultHtml(rows) : "";
      if (rows) {
        const removed = rows.reduce((sum, row) => sum + row.removed, 0);
        const clean = rows.every((row) => !row.error && !row.unresolved);
        toast(removed ? `Removed ${count(removed)} record${removed === 1 ? "" : "s"}` : "Nothing was removed", !removed);
        ctx.onDone?.(clean);
      }
      if (ctx.mode === "watchlist") {
        state.targets = [];
        state.empty = { icon: "task_alt", tone: "good", title: "Done", text: "Close this dialog to see the refreshed watchlist." };
        render();
      } else {
        await loadTargets();
      }
    }

    function wire() {
      const dialog = node();
      if (!dialog || wired) return;
      wired = true;
      dialog.addEventListener("change", (event) => {
        const box = event.target?.closest?.("[data-remove-target]");
        if (!box) return;
        if (box.checked) state.picked.add(box.dataset.removeTarget);
        else state.picked.delete(box.dataset.removeTarget);
        state.armedUntil = 0;
        render();
      });
      dialog.addEventListener("click", (event) => {
        if (event.target?.closest?.("[data-remove-submit]")) {
          void submit();
          return;
        }
        if (event.target?.closest?.("[data-remove-all]")) {
          const all = state.targets.every((target) => state.picked.has(targetKey(target)));
          state.picked = all ? new Set() : new Set(state.targets.map(targetKey));
          state.armedUntil = 0;
          render();
        }
      });
      dialog.addEventListener("cancel", (event) => {
        if (state.busy) event.preventDefault();
      });
      dialog.addEventListener("close", () => {
        state.generation += 1;
        state.armedUntil = 0;
        clearTimeout(armTimer);
      });
    }

    function open(options) {
      const dialog = node();
      if (!dialog || state.busy) return;
      const all = (options.entries || []).filter((entry) => entry?.key);
      if (!all.length) return;
      wire();
      const usable = options.mode === "watchlist" ? all : all.filter(removable);
      const entries = usable.slice(0, RECORD_SELECTION_MAX);
      const skipped = all.length - usable.length;
      const noun = options.noun || "item";
      state.ctx = { ...options, entries };
      state.picked = new Set();
      state.autoPick = true;
      state.armedUntil = 0;
      part("sub").textContent = [
        `You are about to remove ${count(entries.length)} selected ${noun}${entries.length === 1 ? "" : "s"} from your ${PLACES[options.kind] || "providers"}.`,
        usable.length > entries.length ? `Only the first ${count(entries.length)} are used.` : "",
        skipped ? `${count(skipped)} without matchable ids are skipped.` : "",
      ].filter(Boolean).join(" ");
      const note = NOTES[options.kind];
      part("note").hidden = !note;
      part("note-main").textContent = note?.main || "";
      part("note-detail").textContent = note?.detail || "";
      part("error").textContent = "";
      part("result").innerHTML = "";
      if (!dialog.open) dialog.showModal();
      Promise.resolve(providerInstanceLabels.load()).then(() => {
        if (!state.busy && dialog.open) render();
      }).catch(() => {});
      void loadTargets();
    }

    return { open };
  })();

  function createMonthBar({ host, noun, zoom = 0, onZoom = () => {}, onJump = () => {} }) {
    let level = Number.isInteger(zoom) && zoom >= 0 && zoom < TIMELINE_ZOOM_STEPS.length ? zoom : 0;
    let signature = "";

    function syncZoomUi(node = host()) {
      if (!node) return;
      const label = node.querySelector(".cw-hist-zoom-level");
      if (label) label.textContent = `${Math.round(TIMELINE_ZOOM_STEPS[level] * 100)}%`;
      node.querySelectorAll("[data-timeline-zoom]").forEach((btn) => {
        btn.disabled = Number(btn.dataset.timelineZoom) < 0 ? level <= 0 : level >= TIMELINE_ZOOM_STEPS.length - 1;
      });
    }

    function render(months = [], { visible = new Set(), key = "" } = {}) {
      const node = host();
      if (!node) return;
      const counts = new Map((Array.isArray(months) ? months : [])
        .filter((entry) => /^\d{4}-\d{2}$/.test(String(entry?.month || "")))
        .map((entry) => [String(entry.month), Number(entry.count) || 0]));
      const keys = [...counts.keys()].sort();
      if (!keys.length) {
        node.hidden = true;
        node.innerHTML = "";
        signature = "";
        return;
      }
      const series = [];
      let [year, month] = keys[0].split("-").map(Number);
      const [endYear, endMonth] = keys[keys.length - 1].split("-").map(Number);
      while ((year < endYear || (year === endYear && month <= endMonth)) && series.length < 600) {
        const monthKey = `${year}-${timelinePad(month)}`;
        series.push({ key: monthKey, year, month, count: counts.get(monthKey) || 0 });
        month += 1;
        if (month > 12) {
          month = 1;
          year += 1;
        }
      }
      const max = Math.max(1, ...series.map((entry) => entry.count));
      const total = series.reduce((sum, entry) => sum + entry.count, 0);
      const next = `${key}|${keys[0]}|${keys[keys.length - 1]}|${keys.length}|${total}`;
      const entering = next !== signature;
      signature = next;
      const last = series.length - 1;
      const monthFmt = timelineFormatter({ month: "long", year: "numeric", timeZone: "UTC" });
      const bars = series.map((entry, index) => {
        const yearLabel = (index === 0 && entry.month <= 10) || entry.month === 1 ? `<span class="cw-hist-bar-year">${esc(entry.year)}</span>` : "";
        const label = `${monthFmt(Date.UTC(entry.year, entry.month - 1, 1) / 1000)} · ${noun(entry.count)}`;
        const delay = `--d:${Math.min(last - index, 48) * 14}ms`;
        if (!entry.count) return `<span class="cw-hist-bar is-empty" style="${delay}" data-timeline-tip="${esc(label)}"><span class="cw-hist-bar-fill"></span>${yearLabel}</span>`;
        const height = Math.max(10, Math.round(Math.sqrt(entry.count / max) * 100));
        return `<button type="button" class="cw-hist-bar${visible.has(entry.key) ? " is-visible" : ""}" style="${delay};--h:${height}%" data-timeline-month-jump="${esc(entry.key)}" data-timeline-tip="${esc(label)}" aria-label="${esc(label)}"><span class="cw-hist-bar-fill"></span>${yearLabel}</button>`;
      }).join("");
      const previous = node.querySelector(".cw-hist-bars");
      const previousLeft = previous ? previous.scrollLeft : 0;
      node.hidden = false;
      node.innerHTML = `<div class="cw-hist-timeline-head">
          <span class="material-symbols-rounded" aria-hidden="true">timeline</span>
          <strong>Timeline</strong>
          <small>${esc(`${numberFmt.format(keys.length)} active ${keys.length === 1 ? "month" : "months"} · click to jump · Ctrl + scroll to zoom`)}</small>
          <div class="cw-hist-zoom" role="group" aria-label="Timeline zoom">
            <button type="button" data-timeline-zoom="-1" title="Zoom out" aria-label="Zoom out"><span class="material-symbols-rounded" aria-hidden="true">zoom_out</span></button>
            <span class="cw-hist-zoom-level"></span>
            <button type="button" data-timeline-zoom="1" title="Zoom in" aria-label="Zoom in"><span class="material-symbols-rounded" aria-hidden="true">zoom_in</span></button>
          </div>
        </div>
        <div class="cw-hist-bars${entering ? " is-entering" : ""}" style="--zoom:${TIMELINE_ZOOM_STEPS[level]}">${bars}</div>
        <div class="cw-hist-tip" hidden></div>`;
      syncZoomUi(node);
      const track = node.querySelector(".cw-hist-bars");
      if (!track) return;
      const active = track.querySelector(".cw-hist-bar.is-visible");
      const target = active ? Math.max(0, active.offsetLeft - track.clientWidth / 2) : track.scrollWidth;
      if (entering) {
        track.scrollLeft = target;
        clearTimeout(node.__enterTimer);
        node.__enterTimer = setTimeout(() => track.classList.remove("is-entering"), Math.min(last, 48) * 14 + 750);
      } else {
        track.scrollLeft = previousLeft;
        track.scrollTo({ left: target, behavior: "smooth" });
      }
    }

    function setZoom(step, anchorX = null) {
      const node = host();
      const track = node?.querySelector(".cw-hist-bars");
      const next = Math.max(0, Math.min(TIMELINE_ZOOM_STEPS.length - 1, level + step));
      if (!track || next === level) return;
      const anchor = anchorX == null ? track.clientWidth / 2 : anchorX;
      const ratio = (track.scrollLeft + anchor) / Math.max(1, track.scrollWidth);
      level = next;
      onZoom(level);
      track.classList.remove("is-entering", "is-zooming");
      track.style.setProperty("--zoom", String(TIMELINE_ZOOM_STEPS[next]));
      track.scrollLeft = ratio * track.scrollWidth - anchor;
      void track.offsetWidth;
      track.classList.add("is-zooming");
      clearTimeout(track.__zoomTimer);
      track.__zoomTimer = setTimeout(() => track.classList.remove("is-zooming"), 340);
      node.querySelector(".cw-hist-tip")?.setAttribute("hidden", "");
      syncZoomUi(node);
    }

    function wire() {
      const timeline = host();
      if (!timeline || timeline.dataset.monthBarWired === "1") return;
      timeline.dataset.monthBarWired = "1";
      let drag = null;
      let wheelAt = 0;
      const tip = () => timeline.querySelector(".cw-hist-tip");
      const hideTip = () => tip()?.setAttribute("hidden", "");
      const showTip = (bar) => {
        const node = tip();
        if (!node || !bar) return;
        node.textContent = bar.dataset.timelineTip || "";
        node.classList.toggle("is-empty", bar.classList.contains("is-empty"));
        node.hidden = false;
        const hostRect = timeline.getBoundingClientRect();
        const barRect = bar.getBoundingClientRect();
        const fillRect = bar.querySelector(".cw-hist-bar-fill")?.getBoundingClientRect() || barRect;
        const half = node.offsetWidth / 2;
        const center = barRect.left + barRect.width / 2 - hostRect.left;
        node.style.left = `${Math.max(half + 8, Math.min(hostRect.width - half - 8, center))}px`;
        node.style.top = `${fillRect.top - hostRect.top}px`;
      };
      timeline.addEventListener("click", (event) => {
        const zoomBtn = event.target?.closest?.("[data-timeline-zoom]");
        if (zoomBtn) {
          if (!zoomBtn.disabled) setZoom(Number(zoomBtn.dataset.timelineZoom) < 0 ? -1 : 1);
          return;
        }
        const monthBtn = event.target?.closest?.("[data-timeline-month-jump]");
        if (monthBtn) onJump(monthBtn.dataset.timelineMonthJump || "");
      });
      timeline.addEventListener("pointerover", (event) => {
        const bar = event.target?.closest?.("[data-timeline-tip]");
        if (bar && !drag) showTip(bar);
      });
      timeline.addEventListener("pointerleave", hideTip);
      timeline.addEventListener("focusin", (event) => {
        const bar = event.target?.closest?.("[data-timeline-tip]");
        if (bar) showTip(bar);
      });
      timeline.addEventListener("focusout", hideTip);
      timeline.addEventListener("scroll", hideTip, true);
      timeline.addEventListener("wheel", (event) => {
        const track = event.target?.closest?.(".cw-hist-bars");
        if (!track || !(event.ctrlKey || event.metaKey)) return;
        event.preventDefault();
        const now = Date.now();
        if (now - wheelAt < 140) return;
        wheelAt = now;
        setZoom(event.deltaY < 0 ? 1 : -1, event.clientX - track.getBoundingClientRect().left);
      }, { passive: false });
      timeline.addEventListener("pointerdown", (event) => {
        const track = event.target?.closest?.(".cw-hist-bars");
        if (!track || event.pointerType !== "mouse" || event.button !== 0 || track.scrollWidth <= track.clientWidth) return;
        drag = { track, x: event.clientX, left: track.scrollLeft, moved: false };
      });
      window.addEventListener("pointermove", (event) => {
        if (!drag) return;
        const dx = event.clientX - drag.x;
        if (!drag.moved && Math.abs(dx) < 5) return;
        drag.moved = true;
        drag.track.classList.add("is-dragging");
        drag.track.scrollLeft = drag.left - dx;
        hideTip();
      });
      window.addEventListener("pointerup", () => {
        if (!drag) return;
        const moved = drag.moved;
        drag.track.classList.remove("is-dragging");
        drag = null;
        if (!moved) return;
        const swallow = (event) => {
          event.stopPropagation();
          event.preventDefault();
        };
        window.addEventListener("click", swallow, { capture: true, once: true });
        setTimeout(() => window.removeEventListener("click", swallow, true), 0);
      });
    }

    return { render, wire };
  }

  function createTimelinePanel(cfg) {
    const sourceKeys = Object.keys(cfg.sources);
    const panel = () => $(`#profile-panel-${cfg.key}`);
    const q = (suffix) => $(`#profile-${cfg.key}-${suffix}`);
    const prefs = (() => {
      try {
        const raw = JSON.parse(window.localStorage?.getItem(cfg.prefsKey) || "{}");
        return raw && typeof raw === "object" ? raw : {};
      } catch {
        return {};
      }
    })();
    const state = {
      loaded: false,
      seq: 0,
      source: sourceKeys.includes(prefs.source) ? prefs.source : sourceKeys[0],
      type: "all",
      provider: "",
      coverage: "all",
      rating: "",
      search: "",
      page: 1,
      pageSize: TIMELINE_PAGE_SIZES.includes(Number(prefs.pageSize)) ? Number(prefs.pageSize) : 48,
      pageCount: 1,
      view: prefs.view === "list" ? "list" : "grid",
      zoom: Number.isInteger(prefs.zoom) && prefs.zoom >= 0 && prefs.zoom < TIMELINE_ZOOM_STEPS.length ? prefs.zoom : 0,
      timeline: prefs.timeline !== false,
      months: [],
      items: [],
      total: 0,
      selected: new Map(),
    };
    let scoresSignature = "";
    const meta = () => cfg.sources[state.source];
    const count = (value) => numberFmt.format(Number(value) || 0);
    const noun = (value) => `${count(value)} ${meta().noun}${Number(value) === 1 ? "" : "s"}`;
    const selectable = () => !!cfg.selectable && canWriteRecords();
    const selectionKey = (item) => String(item?.key || "");
    const selectionEntry = selectionFields;

    function savePrefs() {
      try {
        window.localStorage?.setItem(cfg.prefsKey, JSON.stringify({ source: state.source, view: state.view, pageSize: state.pageSize, zoom: state.zoom, timeline: state.timeline }));
      } catch {}
    }

    function detail(item) {
      const mode = meta().mode;
      return mode === "route" ? timelineRoute(item) : timelineCoverage(item, mode === "ratings");
    }

    function badge(item) {
      const mode = meta().mode;
      if (mode === "route") {
        const [icon, label] = timelineMethod(item);
        return `<span class="cw-hist-badge"><span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>${esc(label)}</span>`;
      }
      if (mode === "ratings" && item?.rating != null) {
        return `<span class="cw-media-card-score" title="Rated ${esc(item.rating)}"><span>${esc(item.rating)}</span></span>`;
      }
      return "";
    }

    function pill(item) {
      const mode = meta().mode;
      if (mode === "ratings") {
        return `<span class="cw-collection-pill cw-hist-rating-pill"><span class="material-symbols-rounded" aria-hidden="true">star</span>${esc(item?.rating ?? "—")}</span>`;
      }
      const [icon, label] = mode === "route" ? timelineMethod(item) : timelineKind(item);
      return `<span class="cw-collection-pill cw-collection-pill--type"><span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>${esc(label)}</span>`;
    }

    const watchDateFmt = timelineFormatter({ day: "numeric", month: "short", year: "numeric" });

    function rewatchPill(item, inline = false) {
      const watches = Number(item?.watch_count) || 0;
      if (watches < 2) return "";
      const dates = (Array.isArray(item.watch_epochs) ? item.watch_epochs : [])
        .map((value) => watchDateFmt(epochOf(value)))
        .filter(Boolean);
      const tip = `Watched ${watches} times${dates.length ? `: ${dates.join(", ")}` : ""}`;
      return `<span class="cw-hist-rewatch${inline ? " cw-hist-rewatch--inline" : ""}" title="${esc(tip)}" aria-label="${esc(tip)}"><span class="material-symbols-rounded" aria-hidden="true">replay</span>×${esc(count(watches))}</span>`;
    }

    function selectBox(key) {
      const checked = state.selected.has(key) ? " checked" : "";
      return `<label class="cw-tl-select" title="Select"><input type="checkbox" data-timeline-select-item="${esc(key)}"${checked} aria-label="Select"><span class="material-symbols-rounded" aria-hidden="true">check</span></label>`;
    }

    function card(item, timeFmt) {
      const posterKey = storePosterItem(item);
      const epoch = epochOf(item?.sort_epoch);
      const title = titleOf(item);
      const code = timelineCode(item);
      const art = `${timelineArt(item)}
          ${badge(item)}
          ${rewatchPill(item)}
          ${epoch ? `<span class="cw-hist-time">${esc(timeFmt(epoch))}</span>` : ""}
          ${code ? `<span class="cw-hist-episode">${esc(code)}</span>` : ""}`;
      const info = `<strong class="cw-hist-name">${esc(title)}</strong>
          <span class="cw-hist-meta">${esc(timelineSubtitle(item))}</span>
          ${detail(item)}`;
      const key = selectable() ? selectionKey(item) : "";
      if (key) {
        return `<article class="cw-hist-card cw-tl-card${state.selected.has(key) ? " is-selected" : ""}" data-timeline-key="${esc(key)}">
          <button class="cw-hist-art cw-tl-open" type="button" data-profile-poster-key="${esc(posterKey)}" aria-label="Show details for ${esc(title)}">${art}</button>
          ${selectBox(key)}
          <span class="cw-hist-info">${info}</span>
        </article>`;
      }
      return `<button class="cw-hist-card" type="button" data-profile-poster-key="${esc(posterKey)}" aria-label="Show details for ${esc(title)}">
        <span class="cw-hist-art">${art}</span>
        <span class="cw-hist-info">${info}</span>
      </button>`;
    }

    function row(item, timeFmt) {
      const posterKey = storePosterItem(item);
      const epoch = epochOf(item?.sort_epoch);
      const title = titleOf(item);
      const code = timelineCode(item);
      const key = selectable() ? selectionKey(item) : "";
      const titleCell = `${key ? selectBox(key) : ""}<span class="cw-tl-title-stack"><span class="cw-tl-title-line"><strong>${esc(title)}</strong>${code ? `<span class="cw-collection-pill">${esc(code)}</span>` : ""}${rewatchPill(item, true)}</span>${cfg.showIds ? timelineIds(item) : ""}</span>`;
      const cells = `<span class="cw-hist-cell cw-hist-cell--when"><strong>${esc(timeFmt(epoch) || "—")}</strong><small>${esc(relTime(epoch))}</small></span>
        <span class="cw-hist-cell cw-hist-cell--type">${pill(item)}</span>
        <span class="cw-hist-cell cw-hist-cell--where">${detail(item)}</span>`;
      if (key) {
        return `<article class="cw-hist-row cw-tl-row${state.selected.has(key) ? " is-selected" : ""}" data-timeline-key="${esc(key)}">
          <button class="cw-hist-cell cw-hist-cell--art cw-tl-open" type="button" data-profile-poster-key="${esc(posterKey)}" aria-label="Show details for ${esc(title)}">${timelineArt(item)}</button>
          <span class="cw-hist-cell cw-hist-cell--title">${titleCell}</span>
          ${cells}
        </article>`;
      }
      return `<button class="cw-hist-row" type="button" data-profile-poster-key="${esc(posterKey)}" aria-label="Show details for ${esc(title)}">
        <span class="cw-hist-cell cw-hist-cell--art">${timelineArt(item)}</span>
        <span class="cw-hist-cell cw-hist-cell--title">${titleCell}</span>
        ${cells}
      </button>`;
    }

    function syncSelection() {
      if (!cfg.selectable) return;
      const list = q("list");
      list?.querySelectorAll("article[data-timeline-key]").forEach((node) => {
        const on = state.selected.has(node.dataset.timelineKey);
        node.classList.toggle("is-selected", on);
        const box = node.querySelector("[data-timeline-select-item]");
        if (box) box.checked = on;
      });
      list?.classList.toggle("has-selection", state.selected.size > 0);
      const bar = q("bulk");
      if (!bar) return;
      if (!selectable()) {
        state.selected.clear();
        bar.hidden = true;
        return;
      }
      bar.hidden = state.selected.size === 0;
      const label = bar.querySelector("[data-timeline-count]");
      if (label) label.textContent = count(state.selected.size);
    }

    async function selectResults(mode) {
      if (!selectable()) return;
      if (mode === "none") state.selected.clear();
      if (mode === "visible") {
        state.items.forEach((item) => {
          const key = selectionKey(item);
          if (key) state.selected.set(key, selectionEntry(item));
        });
      }
      if (mode === "all") {
        const params = baseParams();
        params.set("page", "1");
        params.set("page_size", "1");
        params.set("include_keys", "true");
        try {
          const data = await api(`${cfg.endpoint}?${params.toString()}`);
          (Array.isArray(data.selection) ? data.selection : []).forEach((entry) => {
            if (entry?.key) state.selected.set(String(entry.key), selectionEntry(entry));
          });
          if (data.selection_truncated) toast(`Selected the first ${count(RECORD_SELECTION_MAX)} results`);
        } catch (e) {
          toast(e.message || "Could not select all results", true);
        }
      }
      syncSelection();
    }

    function removeSelected() {
      if (!selectable() || !state.selected.size) return;
      recordRemoval.open({
        kind: meta().removeKind,
        mode: cfg.removeMode || "editor",
        noun: meta().noun,
        entries: [...state.selected.values()],
        onDone: (clean) => {
          if (clean) state.selected.clear();
          void load();
        },
      });
    }

    function baseParams() {
      const params = new URLSearchParams({
        source: state.source,
        type: state.type,
        provider: state.provider,
        coverage: meta().coverage ? state.coverage : "all",
        search: state.search,
        page: String(state.page),
        page_size: String(state.pageSize),
      });
      if (meta().scores && state.rating) params.set("rating", state.rating);
      return params;
    }

    function paintItems() {
      const host = q("list");
      if (!host) return;
      timelineImages.release(host);
      const list = state.view === "list";
      host.dataset.view = list ? "list" : "grid";
      if (!state.items.length) {
        host.innerHTML = empty(meta().empty);
        syncSelection();
        return;
      }
      const timeFmt = timelineFormatter({ hour: "2-digit", minute: "2-digit" });
      const render = (item) => (list ? row(item, timeFmt) : card(item, timeFmt));
      const groups = [];
      if (state.timeline) {
        for (const item of state.items) {
          const epoch = epochOf(item?.sort_epoch);
          const key = timelineDayKey(epoch);
          const last = groups[groups.length - 1];
          if (last && last.key === key) last.items.push(item);
          else groups.push({ key, epoch, items: [item] });
        }
      }
      if (!groups.some((group) => group.epoch)) {
        host.innerHTML = `<section class="cw-hist-day"><div class="cw-hist-day-body">${state.items.map(render).join("")}</div></section>`;
      } else {
        const dayFmt = timelineFormatter({ weekday: "long", day: "numeric", month: "long", year: "numeric" });
        host.innerHTML = groups.map((group) => `<section class="cw-hist-day" data-timeline-month="${esc(timelineMonthKey(group.epoch))}">
          <div class="cw-hist-day-head">
            <span class="material-symbols-rounded" aria-hidden="true">schedule</span>
            <h3>${esc(group.epoch ? dayFmt(group.epoch) : "No date")}</h3>
            <span class="cw-hist-day-count">${esc(noun(group.items.length))}</span>
          </div>
          <div class="cw-hist-day-body">${group.items.map(render).join("")}</div>
        </section>`).join("");
      }
      timelineImages.observe(host);
      syncSelection();
    }

    const monthBar = createMonthBar({
      host: () => q("timeline"),
      noun,
      zoom: state.zoom,
      onZoom: (level) => {
        state.zoom = level;
        savePrefs();
      },
      onJump: (month) => void load({ month }),
    });

    function renderTimeline(months = []) {
      monthBar.render(state.timeline ? months : [], {
        visible: new Set(state.items.map((item) => timelineMonthKey(epochOf(item?.sort_epoch)))),
        key: state.source,
      });
    }

    function tile(tone, icon, label, note, value, coverage = "") {
      const tag = coverage ? "button" : "span";
      const active = !!coverage && state.coverage === coverage;
      const attrs = coverage ? ` type="button" data-timeline-coverage-set="${esc(coverage)}" aria-pressed="${active}" title="Filter on ${esc(label.toLowerCase())}"` : "";
      return `<${tag} class="cw-collection-tile${active ? " active" : ""}" data-tone="${esc(tone)}"${attrs}>
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <span class="cw-collection-tile-body">
          <strong>${esc(value)}</strong>
          <span>${esc(label)}</span>
          <small>${esc(note)}</small>
        </span>
      </${tag}>`;
    }

    function renderMetrics(data = {}) {
      const host = q("metrics");
      if (host) host.innerHTML = meta().tiles(data.stats || {}, tile, count).join("");
    }

    function renderTypeChips(counts = {}) {
      const host = q("types");
      if (!host) return;
      host.innerHTML = meta().types.map(([key, icon, label]) => `<button class="${state.type === key ? "active" : ""}" type="button" data-timeline-type="${esc(key)}" aria-pressed="${state.type === key}">
          <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
          <span>${esc(label)}</span>
          <strong>${esc(count(counts[key]))}</strong>
        </button>`).join("");
    }

    function renderScores(counts = {}) {
      const host = q("scores");
      if (!host) return;
      if (!meta().scores) {
        host.innerHTML = "";
        scoresSignature = "";
        return;
      }
      const histogram = counts?.ratings || {};
      const values = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
      const max = Math.max(1, ...values.map((value) => Number(histogram[value]) || 0));
      const signature = JSON.stringify([counts?.all || 0, histogram]);
      if (signature !== scoresSignature) {
        scoresSignature = signature;
        const all = `<button type="button" data-timeline-rating="" title="${esc(`All scores · ${noun(counts?.all)}`)}"><span class="cw-hist-score-value">All</span><small>${esc(count(counts?.all))}</small></button>`;
        host.innerHTML = all + values.map((value, index) => {
          const amount = Number(histogram[value]) || 0;
          const height = amount ? Math.max(8, Math.round((amount / max) * 100)) : 0;
          return `<button type="button" data-timeline-rating="${value}" title="${esc(`${value}/10 · ${noun(amount)}`)}"${amount ? "" : " disabled"}>
            <span class="cw-hist-score-bar" style="height:${height}%;--d:${index * 35}ms"></span>
            <span class="cw-hist-score-value"><span class="material-symbols-rounded" aria-hidden="true">star</span>${value}</span>
            <small>${esc(count(amount))}</small>
          </button>`;
        }).join("");
      }
      host.querySelectorAll("[data-timeline-rating]").forEach((btn) => {
        const active = btn.dataset.timelineRating === state.rating;
        btn.classList.toggle("active", active);
        btn.setAttribute("aria-pressed", String(active));
      });
    }

    function renderProviders(providers = []) {
      const select = q("provider");
      if (!select) return;
      const rows = Array.isArray(providers) ? providers : [];
      const known = rows.map((entry) => String(entry?.provider || "").toLowerCase()).filter(Boolean);
      if (state.provider && !known.includes(state.provider)) state.provider = "";
      select.innerHTML = [`<option value="">All providers</option>`].concat(rows.map((entry) => {
        const key = String(entry?.provider || "").toLowerCase();
        const label = visibleProviderLabel(key) || key.toUpperCase();
        return `<option value="${esc(key)}" data-provider="${esc(key)}" data-label="${esc(label)}">${esc(label)} (${esc(count(entry?.count))})</option>`;
      })).join("");
      select.value = state.provider;
      enhanceCollectionProviderSelect(select);
    }

    function enhanceCoverageSelect(select = q("coverage")) {
      if (!select || typeof window.CW?.IconSelect?.enhance !== "function") return;
      const icons = { all: "filter_list", partial: "sync_problem", full: "done_all", mismatch: "compare_arrows" };
      window.CW.IconSelect.enhance(select, {
        className: "cw-profile-collection-select",
        menuClassName: "cw-profile-collection-menu",
        menuMinWidth: 230,
        getOptionData: (value, option) => ({
          label: option?.textContent?.trim() || "Coverage",
          icons: [{ symbol: icons[value] || "filter_list" }],
        }),
      });
    }

    function renderPager(data = {}) {
      const footer = q("footer");
      if (!footer) return;
      const total = Number(data.total || 0);
      const pageSize = Number(data.page_size || state.pageSize) || state.pageSize;
      const pageCount = total ? Math.ceil(total / pageSize) : 1;
      const page = Math.min(Math.max(1, Number(data.page || state.page) || 1), pageCount);
      state.page = page;
      state.pageCount = pageCount;
      footer.hidden = !total;
      const start = total ? (page - 1) * pageSize : 0;
      const end = total ? Math.min(start + pageSize, total) : 0;
      const label = q("page-label");
      if (label) label.textContent = total ? `Showing ${numberFmt.format(start + 1)}–${numberFmt.format(end)} of ${numberFmt.format(total)}` : "";
      const pages = q("pages");
      if (!pages) return;
      const step = (target, icon, title, disabled) => `<button type="button" data-timeline-page="${target}" title="${title}" aria-label="${title}"${disabled ? " disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></button>`;
      const numbers = collectionPageNumbers(page, pageCount).map((entry) => {
        if (entry === "gap") return `<span class="cw-collection-gap" aria-hidden="true">…</span>`;
        const active = entry === page;
        return `<button type="button" class="${active ? "active" : ""}" data-timeline-page="${entry}"${active ? ' aria-current="page" disabled' : ""}>${numberFmt.format(entry)}</button>`;
      }).join("");
      pages.innerHTML = step(page - 1, "chevron_left", "Previous page", page <= 1) + numbers + step(page + 1, "chevron_right", "Next page", page >= pageCount);
    }

    function syncSourceUi() {
      const source = meta();
      const host = panel();
      if (host) {
        host.dataset.source = state.source;
        host.dataset.coverage = source.coverage ? "on" : "off";
      }
      const setText = (suffix, text) => {
        const node = q(suffix);
        if (node) node.textContent = text;
      };
      setText("kicker", source.kicker);
      setText("title", source.title);
      setText("lede", source.lede);
      const search = q("search");
      if (search) search.placeholder = source.search;
      host?.querySelectorAll("[data-timeline-source]").forEach((btn) => {
        const active = btn.dataset.timelineSource === state.source;
        btn.classList.toggle("active", active);
        btn.setAttribute("aria-selected", String(active));
      });
      host?.querySelectorAll("[data-timeline-view]").forEach((btn) => {
        const active = btn.dataset.timelineView === state.view;
        btn.classList.toggle("active", active);
        btn.setAttribute("aria-pressed", String(active));
      });
      host?.querySelectorAll("[data-timeline-toggle]").forEach((btn) => {
        btn.classList.toggle("active", state.timeline);
        btn.setAttribute("aria-pressed", String(state.timeline));
        btn.title = state.timeline ? "Hide timeline" : "Show timeline";
      });
    }

    function writeHash() {
      if (!panel()?.classList.contains("active")) return;
      const current = String(window.location.hash || "");
      if (current && !new RegExp(`^#/?${cfg.key}(?:[/?]|$)`, "i").test(current)) return;
      const next = sourceKeys.length > 1 ? `#${cfg.key}/${state.source}` : `#${cfg.key}`;
      if (current === next) return;
      try {
        window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}${next}`);
      } catch {}
    }

    async function load({ reset = false, month = "", scroll = false } = {}) {
      if (reset) state.page = 1;
      const seq = ++state.seq;
      const host = q("list");
      if (host) {
        timelineImages.release(host);
        host.dataset.view = state.view;
        host.innerHTML = `<div class="cw-hist-day-body">${state.view === "list" ? skeleton("collectionRow", 6) : skeleton("historyCard", 8)}</div>`;
      }
      q("pages")?.querySelectorAll("button").forEach((btn) => { btn.disabled = true; });
      const params = baseParams();
      if (month) params.set("month", month);
      try {
        const data = await api(`${cfg.endpoint}?${params.toString()}`);
        if (seq !== state.seq) return;
        state.items = Array.isArray(data.items) ? data.items : [];
        state.total = Number(data.total || 0);
        state.loaded = true;
        renderMetrics(data);
        renderTypeChips(data.counts || {});
        renderScores(data.counts || {});
        renderProviders(data.providers || []);
        renderPager(data);
        paintItems();
        state.months = Array.isArray(data.months) ? data.months : [];
        renderTimeline(state.months);
        if (month) {
          const target = q("list")?.querySelector(`[data-timeline-month="${month}"]`) || q("list");
          target?.scrollIntoView({ block: "start", behavior: "auto" });
        } else if (scroll) {
          panel()?.scrollIntoView({ block: "start", behavior: "smooth" });
        }
      } catch (e) {
        if (seq !== state.seq) return;
        if (host) host.innerHTML = empty(`${meta().title} could not be loaded.`);
        renderPager({ total: state.total, page: state.page, page_size: state.pageSize });
        toast(e.message || `${meta().title} could not be loaded`, true);
      }
    }

    function setSource(source) {
      if (!sourceKeys.includes(source)) return;
      if (source === state.source && state.loaded) return;
      state.source = source;
      state.selected.clear();
      state.provider = "";
      state.coverage = "all";
      state.rating = "";
      const coverage = q("coverage");
      if (coverage) {
        coverage.value = "all";
        enhanceCoverageSelect(coverage);
      }
      savePrefs();
      syncSourceUi();
      writeHash();
      void load({ reset: true });
    }

    function setCoverage(value, syncSelect = true) {
      state.coverage = TIMELINE_COVERAGE.includes(value) ? value : "all";
      const select = q("coverage");
      if (select && syncSelect) {
        select.value = state.coverage;
        enhanceCoverageSelect(select);
      }
      void load({ reset: true });
    }

    function sourceFromHash() {
      const parts = String(window.location?.hash || "").replace(/^#\/?/, "").split("?")[0].split("/");
      if (profileRouteSegment(parts[0]) !== cfg.key) return "";
      return cfg.aliases?.[profileRouteSegment(parts[1] || "")] || "";
    }

    function open({ fromHash = false } = {}) {
      if (cfg.selectable) void providerInstanceLabels.load();
      const wanted = fromHash ? sourceFromHash() : "";
      if (wanted && (wanted !== state.source || !state.loaded)) {
        setSource(wanted);
      } else {
        syncSourceUi();
        writeHash();
        if (!state.loaded) void load({ reset: true });
      }
      if (fromHash) $(".cw-profile-tabs")?.scrollIntoView({ block: "start", behavior: "smooth" });
    }

    function wire() {
      const host = panel();
      if (!host) return;
      let searchTimer = null;
      monthBar.wire();
      syncSourceUi();
      enhanceCollectionProviderSelect(q("provider"));
      enhanceCoverageSelect();
      host.addEventListener("click", (event) => {
        const target = event.target;
        const selectPick = target?.closest?.("[data-timeline-select]");
        if (selectPick) {
          void selectResults(selectPick.dataset.timelineSelect);
          return;
        }
        if (target?.closest?.("[data-timeline-remove]")) {
          removeSelected();
          return;
        }
        const sourceBtn = target?.closest?.("[data-timeline-source]");
        if (sourceBtn) {
          setSource(sourceBtn.dataset.timelineSource);
          return;
        }
        if (target?.closest?.("[data-timeline-toggle]")) {
          state.timeline = !state.timeline;
          savePrefs();
          syncSourceUi();
          paintItems();
          renderTimeline(state.months);
          return;
        }
        const refreshBtn = target?.closest?.("[data-timeline-refresh]");
        if (refreshBtn) {
          if (refreshBtn.disabled) return;
          refreshBtn.disabled = true;
          refreshBtn.classList.add("is-spinning");
          void Promise.all([load({ reset: true }), new Promise((done) => setTimeout(done, 450))]).finally(() => {
            refreshBtn.disabled = false;
            refreshBtn.classList.remove("is-spinning");
          });
          return;
        }
        const viewBtn = target?.closest?.("[data-timeline-view]");
        if (viewBtn) {
          const next = viewBtn.dataset.timelineView === "list" ? "list" : "grid";
          if (next === state.view) return;
          state.view = next;
          savePrefs();
          syncSourceUi();
          paintItems();
          return;
        }
        const typeBtn = target?.closest?.("[data-timeline-type]");
        if (typeBtn) {
          state.type = typeBtn.dataset.timelineType || "all";
          void load({ reset: true });
          return;
        }
        const ratingBtn = target?.closest?.("[data-timeline-rating]");
        if (ratingBtn) {
          if (ratingBtn.disabled) return;
          const value = ratingBtn.dataset.timelineRating || "";
          state.rating = state.rating === value ? "" : value;
          void load({ reset: true });
          return;
        }
        const coverageBtn = target?.closest?.("[data-timeline-coverage-set]");
        if (coverageBtn) {
          const value = coverageBtn.dataset.timelineCoverageSet;
          setCoverage(state.coverage === value ? "all" : value);
          return;
        }
        const pageBtn = target?.closest?.("[data-timeline-page]");
        if (!pageBtn || pageBtn.disabled) return;
        const page = Math.max(1, Number(pageBtn.dataset.timelinePage) || 1);
        if (page === state.page || page > state.pageCount) return;
        state.page = page;
        void load({ scroll: true });
      });
      host.addEventListener("change", (event) => {
        const box = event.target?.closest?.("[data-timeline-select-item]");
        if (box) {
          const item = state.items.find((entry) => selectionKey(entry) === box.dataset.timelineSelectItem);
          if (item) {
            if (box.checked) state.selected.set(selectionKey(item), selectionEntry(item));
            else state.selected.delete(selectionKey(item));
          }
          syncSelection();
        }
      });
      q("provider")?.addEventListener("change", (event) => {
        state.provider = event.target?.value || "";
        void load({ reset: true });
      });
      q("coverage")?.addEventListener("change", (event) => {
        setCoverage(event.target?.value || "all", false);
      });
      q("search")?.addEventListener("input", (event) => {
        state.search = event.target?.value || "";
        clearTimeout(searchTimer);
        searchTimer = setTimeout(() => load({ reset: true }), 220);
      });
      const pageSizeSelect = q("page-size");
      if (pageSizeSelect) {
        pageSizeSelect.value = String(state.pageSize);
        pageSizeSelect.addEventListener("change", (event) => {
          const next = Number(event.target?.value) || 48;
          if (next === state.pageSize) return;
          state.pageSize = next;
          savePrefs();
          void load({ reset: true });
        });
      }
    }

    return { open, wire };
  }

  const coverageTiles = (stats, tile, count) => {
    const endpoints = Number(stats.endpoints) || 0;
    return [
      tile("green", "done_all", "Everywhere", endpoints === 1 ? "On your provider" : `On all ${count(endpoints)} providers`, count(stats.full), "full"),
      tile("blue", "sync_problem", "Missing", "On some providers", count(stats.partial), "partial"),
    ];
  };

  const historyPanel = createTimelinePanel({
    key: "history",
    endpoint: "/api/profile/history",
    prefsKey: "cw.profile.history",
    aliases: { synced: "synced", sync: "synced", scrobble: "scrobble", scrobbles: "scrobble" },
    selectable: true,
    sources: {
      synced: {
        mode: "coverage",
        removeKind: "history",
        coverage: true,
        scores: false,
        kicker: "Sync output",
        title: "Synced history",
        lede: "Plays read back from every provider after sync.",
        noun: "play",
        search: "Search synced history...",
        empty: "No synced plays match this view.",
        types: [["all", "apps", "All"], ["movie", "movie", "Movies"], ["episode", "play_circle", "Episodes"]],
        tiles: (stats, tile, count) => [
          tile("violet", "history", "Plays", `${count(stats.plays)} ${Number(stats.plays) === 1 ? "title" : "titles"}`, count(stats.watches ?? stats.plays)),
          ...coverageTiles(stats, tile, count),
          tile("amber", "hub", "Providers", "Holding history", count(stats.endpoints)),
        ],
      },
      scrobble: {
        mode: "route",
        removeKind: "history",
        coverage: false,
        scores: false,
        kicker: "Recorded by CrossWatch",
        title: "Scrobbles",
        lede: "Plays CrossWatch caught live from watchers and webhooks, and where it sent them",
        noun: "scrobble",
        search: "Search scrobbles...",
        empty: "No scrobbles match this view.",
        types: [["all", "apps", "All"], ["movie", "movie", "Movies"], ["episode", "play_circle", "Episodes"]],
        tiles: (stats, tile, count) => [
          tile("violet", "sensors", "Scrobbles", "Recorded live", count(stats.plays)),
          tile("blue", "movie", "Movies", "Scrobbled", count(stats.movies)),
          tile("green", "play_circle", "Episodes", "Scrobbled", count(stats.episodes)),
          tile("amber", "schedule", "Hours", "Watched", numberFmt.format(Math.round((Number(stats.hours) || 0) * 10) / 10)),
        ],
      },
    },
  });

  const ratingsPanel = createTimelinePanel({
    key: "ratings",
    endpoint: "/api/profile/ratings",
    prefsKey: "cw.profile.ratings",
    aliases: {},
    selectable: true,
    sources: {
      ratings: {
        mode: "ratings",
        removeKind: "ratings",
        coverage: true,
        scores: true,
        kicker: "Sync output",
        title: "Ratings",
        lede: "Your scores read back from every provider after sync.",
        noun: "rating",
        search: "Search ratings...",
        empty: "No ratings match this view.",
        types: [["all", "apps", "All"], ["movie", "movie", "Movies"], ["show", "tv", "Shows"], ["season", "stacks", "Seasons"], ["episode", "play_circle", "Episodes"]],
        tiles: (stats, tile, count) => {
          const average = Number(stats.average) || 0;
          return [
            tile("amber", "star", "Ratings", average ? `Average ${numberFmt.format(average)} / 10` : "Synced ratings", count(stats.plays)),
            ...coverageTiles(stats, tile, count),
            tile("red", "compare_arrows", "Conflicts", "Scores differ", count(stats.mismatch), "mismatch"),
          ];
        },
      },
    },
  });

  const watchlistPanel = createTimelinePanel({
    key: "watchlist",
    endpoint: "/api/profile/watchlist",
    removeMode: "watchlist",
    prefsKey: "cw.profile.watchlist",
    aliases: {},
    selectable: true,
    showIds: true,
    sources: {
      watchlist: {
        mode: "coverage",
        removeKind: "watchlist",
        coverage: true,
        scores: false,
        kicker: "Sync output",
        title: "Watchlist",
        lede: "Everything on your watchlists.",
        noun: "title",
        search: "Filter by title, id or provider...",
        empty: "No watchlist titles match this view.",
        types: [["all", "apps", "All"], ["movie", "movie", "Movies"], ["show", "tv", "Shows"], ["anime", "animation", "Anime"]],
        tiles: (stats, tile, count) => [
          tile("violet", "bookmark", "Titles", "On your watchlists", count(stats.plays)),
          ...coverageTiles(stats, tile, count),
          tile("amber", "hub", "Providers", "Holding watchlists", count(stats.endpoints)),
        ],
      },
    },
  });

  const PLAYBACK_PROVIDER_KEYS = ["crosswatch", "trakt", "simkl", "mdblist", "publicmetadb", "punchplay", "flicklist", "plex", "emby", "jellyfin", "nuvio", "kodi", "stremio", "floppy"];

  const playbackPanel = (() => {
    const DEFAULT_TIMEOUT = 20;
    const PAGE_SIZES = [24, 48, 96];
    const doc = document.documentElement;
    const isManaged = () => doc?.dataset?.cwRole === "user";
    const canAct = () => !viewingAs() && (!isManaged() || doc?.dataset?.cwPermWrite === "on");
    const canConfigure = () => !isManaged();
    const q = (suffix) => $(`#profile-playback-${suffix}`);
    const panel = () => $("#profile-panel-playback");
    const prefs = (() => {
      try {
        const raw = JSON.parse(window.localStorage?.getItem("cw.profile.playback") || "{}");
        return raw && typeof raw === "object" ? raw : {};
      } catch {
        return {};
      }
    })();
    const state = {
      loaded: false,
      busy: false,
      seq: 0,
      clock: 0,
      page: 1,
      pageCount: 1,
      pageSize: PAGE_SIZES.includes(Number(prefs.pageSize)) ? Number(prefs.pageSize) : 48,
      view: prefs.view === "list" ? "list" : "grid",
      timeline: prefs.timeline !== false,
      zoom: Number.isInteger(prefs.zoom) ? prefs.zoom : 0,
      months: [],
      total: 0,
      items: [],
      providers: [],
      errors: [],
      summary: {},
      selected: new Map(),
      filters: { provider: "", type: "", progress: "", age: "", sort: "last_updated", search: "" },
      syncedAt: 0,
      failed: false,
    };
    const count = (value) => numberFmt.format(Number(value) || 0);
    const selects = () => ({ provider: q("provider"), progress: q("progress"), age: q("age"), sort: q("sort") });

    function savePrefs() {
      try {
        window.localStorage?.setItem("cw.profile.playback", JSON.stringify({ view: state.view, pageSize: state.pageSize, timeline: state.timeline, zoom: state.zoom }));
      } catch {}
    }

    async function request(url, options = {}) {
      try {
        const method = String(options.method || "GET").toUpperCase();
        const res = await fetch(method === "GET" ? scopeUrl(url) : url, { credentials: "same-origin", cache: "no-store", ...options });
        const data = await res.json().catch(() => ({}));
        const payload = data && typeof data === "object" ? data : {};
        if (!res.ok) payload.ok = false;
        return payload;
      } catch (error) {
        return { ok: false, message: String(error?.message || error || "Request failed") };
      }
    }

    const postJson = (url, body) => request(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });

    const recordsOf = (item) => (Array.isArray(item?.records) && item.records.length ? item.records : [item]);
    const endpointKey = (item) => `${item?.provider}:${item?.instance_id}`;
    const keyOf = (item) => (item?.is_combined
      ? `combined:${item.remote_id || item.canonical_key || recordsOf(item).map((record) => `${endpointKey(record)}:${record.remote_id}`).join("|")}`
      : `${endpointKey(item)}:${item?.remote_id}`);
    const CAPABILITY = { mark_watched: "can_mark_watched", update_progress: "can_update_progress", remove_progress: "can_remove_progress" };
    const actionPayloads = (items, action) => items.flatMap((item) => recordsOf(item)
      .filter((record) => record[CAPABILITY[action]])
      .map((record) => ({ provider: record.provider, instance_id: record.instance_id, remote_id: record.remote_id, canonical_key: record.canonical_key, record })));
    const editableMax = (records) => {
      const values = records.map((record) => Number(record.editable_progress_max_exclusive || 100)).filter((value) => Number.isFinite(value) && value > 2);
      return values.length ? Math.min(...values, 100) : 100;
    };
    const sliderMax = (maxExclusive) => Math.max(2, Math.ceil(Math.min(Number(maxExclusive) || 100, 100)) - 1);
    const averageProgress = (records, maxExclusive) => {
      const values = records.map((record) => Number(record.progress_percent)).filter(Number.isFinite);
      const top = sliderMax(maxExclusive);
      if (!values.length) return Math.min(25, top);
      return Math.max(2, Math.min(top, Math.round(values.reduce((sum, value) => sum + value, 0) / values.length)));
    };
    const actionTitle = (action) => (action === "mark_watched" ? "Mark as watched" : action === "update_progress" ? "Edit progress" : "Remove progress");

    const progressOf = (item) => {
      const live = Number(item?.live_progress_percent);
      const value = item?.live_progress_percent != null && Number.isFinite(live) ? live : Number(item?.progress_percent);
      return item?.progress_percent == null && item?.live_progress_percent == null ? null : Number.isFinite(value) ? Math.max(0, Math.min(100, value)) : null;
    };
    const remainingOf = (item) => {
      const live = Number(item?.live_remaining_seconds);
      const seconds = item?.live_remaining_seconds != null && Number.isFinite(live) ? live : Number(item?.remaining_seconds);
      if (!Number.isFinite(seconds) || seconds <= 0) return "";
      const minutes = Math.round(seconds / 60);
      return minutes >= 60 ? `${Math.floor(minutes / 60)}h ${minutes % 60}m left` : `${minutes}m left`;
    };
    const liveState = (item) => String(item?.live_state || "").toLowerCase();
    const pausedEpoch = (item) => {
      const live = Number(item?.live_updated);
      return Number.isFinite(live) && live > 0 ? live : epochOf(item?.updated_at || item?.progress_at);
    };
    const pausedLabel = (item) => {
      const live = liveState(item);
      if (live === "playing") return "Playing now";
      if (live === "buffering") return "Buffering now";
      const epoch = pausedEpoch(item);
      return epoch ? `Paused ${relTime(epoch)}` : "";
    };
    const titleFor = (item) => (item?.media_type === "movie" ? (item?.title || "Untitled") : (item?.series_title || item?.title || "Untitled"));
    const codeFor = (item) => (item?.media_type !== "movie" && item?.season != null && item?.episode != null ? `S${timelinePad(item.season)}E${timelinePad(item.episode)}` : "");
    const subtitleFor = (item) => {
      if (item?.media_type === "movie") return [item?.year, "Movie"].filter(Boolean).join(" · ");
      return [item?.episode_title, item?.media_type === "anime_episode" ? "Anime" : "Episode"].filter(Boolean).join(" · ");
    };
    const formatRating = (value) => {
      const rating = Number(value);
      return Number.isInteger(rating) ? String(rating) : rating.toFixed(1).replace(/\.0$/, "");
    };
    const profileLabel = (p) => {
      const provider = String(p?.provider || "");
      const providerName = String(p?.provider_label || provider);
      let label = String(p?.instance_label || p?.instance_id || "").trim();
      for (const prefix of [providerName, provider]) {
        if (prefix && label.toLowerCase().startsWith(prefix.toLowerCase())) {
          label = label.slice(prefix.length).replace(/^[\s_-]+/, "").trim();
        }
      }
      return label || String(p?.instance_id || "").trim() || "Default";
    };
    const settingsLabel = (p) => {
      const id = String(p?.instance_id || "default");
      if (id === "default") return "Default";
      if (String(p.provider || "").toLowerCase() === "crosswatch" && /^CW-P\d+$/i.test(id)) return id.toUpperCase();
      return profileLabel(p);
    };
    const providerOrder = (provider) => {
      const key = String(provider || "").toLowerCase();
      const index = PLAYBACK_PROVIDER_KEYS.indexOf(key);
      return index >= 0 ? index : PLAYBACK_PROVIDER_KEYS.length + key.charCodeAt(0);
    };
    const providerChips = (item) => {
      const rows = Array.isArray(item?.providers) && item.providers.length ? item.providers : [item];
      return providerChipStrip(timelineEndpoints(rows.map((row) => ({
        provider: row?.provider,
        instance: String(row?.instance_id || "default").toLowerCase() === "default" ? "default" : profileLabel(row),
      }))).map((ref) => timelineChipEntry(ref, "source")), "cw-hist-provider");
    };
    const errorName = (error) => {
      const provider = String(error?.provider || "").trim();
      const label = String(error?.provider_label || visibleProviderLabel(provider) || provider || "Provider").trim();
      const instance = String(error?.instance_label || error?.instance_id || "").trim();
      return !instance || /^default$/i.test(instance) || instance.toLowerCase() === label.toLowerCase() ? label : `${label} · ${instance}`;
    };
    const errorTitle = (error) => {
      const code = String(error?.error_code || "").toLowerCase();
      if (code === "provider_timeout") return "Timed out";
      if (code === "provider_unavailable") return "Unavailable";
      if (code === "not_configured") return "Needs attention";
      if (code.startsWith("http:")) return `Returned ${code.slice(5)}`;
      return error?.retryable ? "Could not refresh" : "Notice";
    };

    function selectBox(key) {
      if (!canAct()) return "";
      const checked = state.selected.has(key) ? " checked" : "";
      return `<label class="cw-tl-select" title="Select"><input type="checkbox" data-playback-select="${esc(key)}"${checked} aria-label="Select"><span class="material-symbols-rounded" aria-hidden="true">check</span></label>`;
    }

    function actions(item, key) {
      if (!canAct()) return "";
      const button = (action, icon, label, enabled, tone = "") => (enabled
        ? `<button class="cw-tl-action${tone}" type="button" data-playback-action="${action}" data-playback-key="${esc(key)}" title="${label}" aria-label="${label}"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></button>`
        : "");
      return `<span class="cw-tl-actions">${button("update_progress", "edit", "Edit progress", item?.can_update_progress)}${button("mark_watched", "check_circle", "Mark as watched", item?.can_mark_watched, " is-good")}${button("remove_progress", "delete", "Remove progress", item?.can_remove_progress, " is-danger")}</span>`;
    }

    function card(item) {
      const key = keyOf(item);
      const title = titleFor(item);
      const pct = progressOf(item);
      const code = codeFor(item);
      const remaining = remainingOf(item);
      const live = liveState(item);
      const liveBadge = live === "playing" || live === "buffering"
        ? `<span class="cw-hist-badge cw-play-live"><span class="cw-play-live-dot" aria-hidden="true"></span>${live === "playing" ? "Playing now" : "Buffering"}</span>`
        : "";
      const rating = Number(item?.rating) > 0
        ? `<span class="cw-play-rating" title="Rating ${esc(formatRating(item.rating))}"><span class="material-symbols-rounded" aria-hidden="true">star</span>${esc(formatRating(item.rating))}</span>`
        : "";
      return `<article class="cw-hist-card cw-play-card${state.selected.has(key) ? " is-selected" : ""}" data-playback-key="${esc(key)}">
        <button class="cw-hist-art cw-play-open" type="button" data-profile-poster-key="${esc(storePosterItem(item))}" aria-label="Show details for ${esc(title)}">
          ${timelineArt(item)}
          ${liveBadge}
          ${remaining ? `<span class="cw-hist-time">${esc(remaining)}</span>` : ""}
          ${code ? `<span class="cw-hist-episode">${esc(code)}</span>` : ""}
          <span class="cw-play-track" aria-hidden="true"><i style="width:${pct ?? 0}%"></i></span>
        </button>
        ${selectBox(key)}
        <span class="cw-hist-info">
          <strong class="cw-hist-name">${esc(title)}</strong>
          <span class="cw-hist-meta">${esc(subtitleFor(item))}</span>
          <span class="cw-play-progress"><strong>${pct == null ? "Unknown" : `${Math.round(pct)}%`} watched</strong><span>${esc(pausedLabel(item))}</span></span>
          <span class="cw-play-foot"><span class="cw-hist-providers">${providerChips(item)}</span>${rating}${actions(item, key)}</span>
        </span>
      </article>`;
    }

    function row(item) {
      const key = keyOf(item);
      const title = titleFor(item);
      const pct = progressOf(item);
      const code = codeFor(item);
      return `<article class="cw-hist-row cw-play-row${state.selected.has(key) ? " is-selected" : ""}" data-playback-key="${esc(key)}">
        <button class="cw-hist-cell cw-hist-cell--art cw-play-open" type="button" data-profile-poster-key="${esc(storePosterItem(item))}" aria-label="Show details for ${esc(title)}">${timelineArt(item)}</button>
        <span class="cw-hist-cell cw-hist-cell--title">${selectBox(key)}<strong>${esc(title)}</strong>${code ? `<span class="cw-collection-pill">${esc(code)}</span>` : ""}</span>
        <span class="cw-hist-cell cw-hist-cell--when"><strong>${esc(remainingOf(item) || "—")}</strong><small>${esc(pausedLabel(item))}</small></span>
        <span class="cw-hist-cell cw-hist-cell--type cw-play-cell--progress"><span class="cw-play-meter"><i style="width:${pct ?? 0}%"></i></span><strong>${pct == null ? "Unknown" : `${Math.round(pct)}%`}</strong></span>
        <span class="cw-hist-cell cw-hist-cell--where"><span class="cw-play-foot"><span class="cw-hist-providers">${providerChips(item)}</span>${actions(item, key)}</span></span>
      </article>`;
    }

    function emptyMessage() {
      const configured = state.providers.filter((p) => p.configured && p.read);
      if (!configured.length) return "Connect a compatible provider to see playback progress.";
      if (!configured.some((p) => p.included !== false)) return "Enable at least one provider profile under Providers.";
      return state.errors.length ? "No playback records could be loaded." : "Nothing unfinished right now.";
    }

    function paint() {
      const host = q("list");
      if (!host) return;
      timelineImages.release(host);
      const list = state.view === "list";
      host.dataset.view = list ? "list" : "grid";
      if (!state.items.length) {
        host.innerHTML = empty(emptyMessage());
        syncSelection();
        return;
      }
      const render = (item) => (list ? row(item) : card(item));
      if (!state.timeline || state.filters.sort !== "last_updated") {
        host.innerHTML = `<section class="cw-hist-day"><div class="cw-hist-day-body">${state.items.map(render).join("")}</div></section>`;
      } else {
        const dayFmt = timelineFormatter({ weekday: "long", day: "numeric", month: "long", year: "numeric" });
        const groups = [];
        for (const item of state.items) {
          const epoch = pausedEpoch(item);
          const key = timelineDayKey(epoch);
          const last = groups[groups.length - 1];
          if (last && last.key === key) last.items.push(item);
          else groups.push({ key, epoch, items: [item] });
        }
        host.innerHTML = groups.map((group) => `<section class="cw-hist-day" data-timeline-month="${esc(timelineMonthKey(group.epoch))}">
          <div class="cw-hist-day-head">
            <span class="material-symbols-rounded" aria-hidden="true">pause_circle</span>
            <h3>${esc(group.epoch ? dayFmt(group.epoch) : "Unknown date")}</h3>
            <span class="cw-hist-day-count">${esc(`${count(group.items.length)} unfinished`)}</span>
          </div>
          <div class="cw-hist-day-body">${group.items.map(render).join("")}</div>
        </section>`).join("");
      }
      timelineImages.observe(host);
      syncSelection();
    }

    function syncSelection() {
      const host = q("list");
      host?.querySelectorAll("article[data-playback-key]").forEach((node) => {
        const selected = state.selected.has(node.dataset.playbackKey);
        node.classList.toggle("is-selected", selected);
        const box = node.querySelector("[data-playback-select]");
        if (box) box.checked = selected;
      });
      host?.classList.toggle("has-selection", state.selected.size > 0);
      const bar = q("bulk");
      if (!bar) return;
      if (!canAct()) {
        state.selected.clear();
        bar.hidden = true;
        return;
      }
      bar.hidden = state.selected.size === 0;
      const label = bar.querySelector("[data-playback-count]");
      if (label) label.textContent = count(state.selected.size);
    }

    function tile(tone, icon, label, note, value, filter = null) {
      const active = !!filter && state.filters[filter.key] === filter.value;
      const tag = filter ? "button" : "span";
      const attrs = filter ? ` type="button" data-playback-filter="${esc(filter.key)}" data-playback-value="${esc(filter.value)}" aria-pressed="${active}" title="Filter on ${esc(label.toLowerCase())}"` : "";
      return `<${tag} class="cw-collection-tile${active ? " active" : ""}" data-tone="${esc(tone)}"${attrs}>
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <span class="cw-collection-tile-body">
          <strong>${esc(value)}</strong>
          <span>${esc(label)}</span>
          <small>${esc(note)}</small>
        </span>
      </${tag}>`;
    }

    function renderMetrics() {
      const host = q("metrics");
      if (!host) return;
      const summary = state.summary || {};
      const readable = state.providers.filter((p) => p.read && p.configured && p.included !== false).length;
      const failing = new Set(state.errors.map((error) => `${error.provider}:${error.instance_id}`)).size;
      host.innerHTML = [
        tile("violet", "play_circle", "Unfinished", "Across your players", count(summary.total ?? state.total)),
        tile("green", "flag", "Almost done", "90% or more watched", count(summary.almost_done), { key: "progress", value: "90:100" }),
        tile("amber", "hourglass_bottom", "Stale", "Paused over 30 days", count(summary.stale), { key: "age", value: "older_30d" }),
        tile(failing ? "red" : "blue", "hub", "Providers", failing ? `${count(failing)} need attention` : "All responding", `${count(Math.max(0, readable - failing))}/${count(readable)}`),
      ].join("");
    }

    function renderTypes() {
      const host = q("types");
      if (!host) return;
      const summary = state.summary || {};
      const rows = [["", "apps", "All", summary.total], ["movie", "movie", "Movies", summary.movies], ["episode", "play_circle", "Episodes", summary.episodes]];
      if (Number(summary.anime) > 0 || state.filters.type === "anime_episode") rows.push(["anime_episode", "animation", "Anime", summary.anime]);
      host.innerHTML = rows.map(([value, icon, label, amount]) => `<button class="${state.filters.type === value ? "active" : ""}" type="button" data-playback-type="${esc(value)}" aria-pressed="${state.filters.type === value}">
          <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
          <span>${esc(label)}</span>
          <strong>${esc(count(amount))}</strong>
        </button>`).join("");
    }

    function renderProviders() {
      const select = q("provider");
      if (!select) return;
      const readable = state.providers.filter((p) => p.read && p.configured && p.included !== false);
      const options = [`<option value="">All providers</option>`].concat(readable.map((p) => {
        const provider = String(p.provider || "");
        const instance = String(p.instance_id || "default");
        const label = String(p.instance_label || "").trim() || visibleProviderLabel(provider) || provider.toUpperCase();
        return `<option value="${esc(provider)}:${esc(instance)}" data-provider="${esc(provider)}" data-label="${esc(label)}">${esc(label)}</option>`;
      })).join("");
      if (select.dataset.signature !== options) {
        select.innerHTML = options;
        select.dataset.signature = options;
      }
      if (![...select.options].some((option) => option.value === state.filters.provider)) state.filters.provider = "";
      select.value = state.filters.provider;
      enhanceCollectionProviderSelect(select);
      syncFilterChrome();
    }

    function enhanceSelects() {
      if (typeof window.CW?.IconSelect?.enhance !== "function") return;
      const enhance = (select, icons, fallback) => {
        if (!select) return;
        window.CW.IconSelect.enhance(select, {
          className: "cw-profile-collection-select",
          menuClassName: "cw-profile-collection-menu",
          menuMinWidth: 220,
          getOptionData: (value, option) => ({ label: option?.textContent?.trim() || fallback, icons: [{ symbol: icons[value] || icons[""] }] }),
        });
      };
      enhance(q("progress"), { "": "donut_large", "90:100": "flag" }, "Progress");
      enhance(q("age"), { "": "schedule", older_30d: "hourglass_bottom" }, "Paused");
      enhance(q("sort"), { "": "sort", last_updated: "schedule", progress_high: "trending_up", progress_low: "trending_down", remaining_time: "timer", rating_high: "star", title: "sort_by_alpha", provider: "hub" }, "Sort");
    }

    function renderErrors() {
      const host = q("errors");
      const status = q("status");
      if (!host) return;
      if (!state.errors.length) {
        host.hidden = true;
        host.innerHTML = "";
        if (status) delete status.dataset.issues;
        return;
      }
      const partial = state.items.length > 0;
      const chips = state.errors.slice(0, 6).map((error) => {
        const provider = String(error?.provider || "").trim();
        const logo = provider ? providerLogLogo(provider) : "";
        const status = error?.remote_status ? `HTTP ${error.remote_status}` : String(error?.error_code || "").replace(/_/g, " ");
        return `<span class="cw-play-error" title="${esc(error?.message || "")}">
          ${logo ? `<img src="${esc(logo)}" alt="" onerror="this.remove()">` : `<span class="material-symbols-rounded" aria-hidden="true">warning</span>`}
          <strong>${esc(errorName(error))}</strong><span>${esc(errorTitle(error))}${status ? ` · ${esc(status)}` : ""}</span>
        </span>`;
      }).join("");
      const more = state.errors.length > 6 ? `<span class="cw-play-error-more">+${count(state.errors.length - 6)} more</span>` : "";
      host.innerHTML = `${chips}${more}`;
      host.hidden = false;
      if (status) status.dataset.issues = partial ? "partial" : "failed";
    }

    function syncLabel() {
      if (!state.syncedAt) return "not yet";
      const minutes = Math.floor(Math.max(0, Date.now() - state.syncedAt) / 60000);
      if (minutes < 1) return "just now";
      if (minutes < 60) return `${minutes} min ago`;
      const hours = Math.floor(minutes / 60);
      if (hours < 24) return `${hours}h ago`;
      const days = Math.floor(hours / 24);
      return `${days} day${days === 1 ? "" : "s"} ago`;
    }

    function renderSync() {
      const host = q("sync");
      if (host) {
        host.dataset.state = state.busy ? "refreshing" : state.failed ? "failed" : "ready";
        const label = host.querySelector("strong");
        if (label) label.textContent = state.busy ? "refreshing" : syncLabel();
      }
      const refresh = q("refresh");
      if (refresh) {
        refresh.disabled = state.busy;
        refresh.classList.toggle("is-spinning", state.busy);
      }
    }

    function renderPager() {
      const footer = q("footer");
      if (!footer) return;
      const total = state.total;
      const pageCount = total ? Math.ceil(total / state.pageSize) : 1;
      state.page = Math.min(Math.max(1, state.page), pageCount);
      state.pageCount = pageCount;
      footer.hidden = !total;
      const start = total ? (state.page - 1) * state.pageSize : 0;
      const end = total ? Math.min(start + state.pageSize, total) : 0;
      const label = q("page-label");
      if (label) label.textContent = total ? `Showing ${numberFmt.format(start + 1)}–${numberFmt.format(end)} of ${numberFmt.format(total)}` : "";
      const pages = q("pages");
      if (!pages) return;
      const step = (target, icon, title, disabled) => `<button type="button" data-playback-page="${target}" title="${title}" aria-label="${title}"${disabled ? " disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></button>`;
      const numbers = collectionPageNumbers(state.page, pageCount).map((entry) => {
        if (entry === "gap") return `<span class="cw-collection-gap" aria-hidden="true">…</span>`;
        const active = entry === state.page;
        return `<button type="button" class="${active ? "active" : ""}" data-playback-page="${entry}"${active ? ' aria-current="page" disabled' : ""}>${numberFmt.format(entry)}</button>`;
      }).join("");
      pages.innerHTML = step(state.page - 1, "chevron_left", "Previous page", state.page <= 1) + numbers + step(state.page + 1, "chevron_right", "Next page", state.page >= pageCount);
    }

    let monthBar = null;
    const bar = () => monthBar || (monthBar = createMonthBar({
      host: () => q("timeline"),
      noun: (value) => `${count(value)} unfinished`,
      zoom: state.zoom,
      onZoom: (level) => {
        state.zoom = level;
        savePrefs();
      },
      onJump: (month) => void load({ month }),
    }));

    function renderTimeline() {
      const dated = (state.filters.sort || "last_updated") === "last_updated";
      bar().render(state.timeline && dated ? state.months : [], {
        visible: new Set(state.items.map((item) => timelineMonthKey(pausedEpoch(item)))),
        key: "playback",
      });
    }

    function syncViewButtons() {
      panel()?.querySelectorAll("[data-playback-view]").forEach((btn) => {
        const active = btn.dataset.playbackView === state.view;
        btn.classList.toggle("active", active);
        btn.setAttribute("aria-pressed", String(active));
      });
      panel()?.querySelectorAll("[data-playback-timeline]").forEach((btn) => {
        btn.classList.toggle("active", state.timeline);
        btn.setAttribute("aria-pressed", String(state.timeline));
        btn.title = state.timeline ? "Hide timeline" : "Show timeline";
      });
    }

    function query({ force = false, all = false, month = "" } = {}) {
      const params = new URLSearchParams();
      const [provider, instance] = String(state.filters.provider || "").split(":");
      if (provider) params.set("provider", provider);
      if (instance) params.set("instance_id", instance);
      if (state.filters.type) params.set("media_type", state.filters.type);
      if (state.filters.age) params.set("age", state.filters.age);
      if (state.filters.search) params.set("search", state.filters.search);
      params.set("sort", state.filters.sort || "last_updated");
      if (state.filters.progress) {
        const [min, max] = state.filters.progress.split(":");
        if (min) params.set("progress_min", min);
        if (max) params.set("progress_max", max);
      }
      params.set("page", all ? "1" : String(state.page));
      params.set("page_size", all ? "250" : String(state.pageSize));
      if (force) params.set("force_refresh", "true");
      if (month && !all) params.set("month", month);
      return params.toString();
    }

    async function load({ force = false, scroll = false, month = "" } = {}) {
      const seq = ++state.seq;
      state.busy = true;
      renderSync();
      const host = q("list");
      if (host && !state.loaded) {
        timelineImages.release(host);
        host.dataset.view = state.view;
        host.innerHTML = `<div class="cw-hist-day-body">${state.view === "list" ? skeleton("collectionRow", 6) : skeleton("historyCard", 8)}</div>`;
      }
      host?.setAttribute("aria-busy", "true");
      const data = await request(`/api/playback_progress/items?${query({ force, month })}`);
      if (seq !== state.seq) return;
      state.busy = false;
      host?.removeAttribute("aria-busy");
      state.items = Array.isArray(data.items) ? data.items : [];
      if (Array.isArray(data.providers)) state.providers = data.providers;
      state.errors = Array.isArray(data.errors) ? data.errors : [];
      if (data.ok === false && !state.errors.length) {
        state.errors = [{ provider: "", provider_label: "Playback", message: data.message || data.error || "Request failed.", retryable: true }];
      }
      state.failed = data.ok === false;
      if (!state.failed) state.syncedAt = Date.now();
      if (data.summary && typeof data.summary === "object") state.summary = data.summary;
      state.total = Number(data.total || 0);
      state.page = Number(data.page || state.page) || 1;
      state.loaded = true;
      renderMetrics();
      renderTypes();
      renderProviders();
      renderErrors();
      renderPager();
      paint();
      state.months = Array.isArray(data.months) ? data.months : [];
      renderTimeline();
      renderSync();
      if (month) {
        (q("list")?.querySelector(`[data-timeline-month="${month}"]`) || q("list"))?.scrollIntoView({ block: "start", behavior: "auto" });
      } else if (scroll) {
        panel()?.scrollIntoView({ block: "start", behavior: "smooth" });
      }
    }

    const FILTER_DEFAULTS = { provider: "", progress: "", age: "", sort: "last_updated" };

    function activeFilterCount() {
      return Object.entries(FILTER_DEFAULTS).filter(([key, fallback]) => (state.filters[key] || "") !== fallback).length;
    }

    function syncFilterChrome() {
      const badge = q("filters-count");
      if (!badge) return;
      const total = activeFilterCount();
      badge.textContent = String(total);
      badge.hidden = !total;
    }

    function toggleFilters(force) {
      const panel = q("filters-panel");
      const btn = q("filters-btn");
      if (!panel || !btn) return;
      const open = typeof force === "boolean" ? force : panel.hidden;
      panel.hidden = !open;
      btn.setAttribute("aria-expanded", String(open));
    }

    function clearFilters() {
      Object.entries(FILTER_DEFAULTS).forEach(([key, fallback]) => {
        state.filters[key] = fallback;
        const select = selects()[key];
        if (select) select.value = fallback;
      });
      enhanceSelects();
      syncFilterChrome();
      state.page = 1;
      void load();
    }

    function setFilter(key, value) {
      state.filters[key] = value;
      const select = selects()[key];
      if (select && select.value !== value) {
        select.value = value;
        enhanceSelects();
      }
      syncFilterChrome();
      state.page = 1;
      void load();
    }

    function askProgress(defaultValue, recordCount, maxExclusive = 100) {
      const dialog = q("edit");
      const range = q("edit-range");
      const value = q("edit-value");
      const sub = q("edit-sub");
      const error = q("edit-error");
      const form = dialog?.querySelector("form");
      if (!dialog?.showModal || !range || !value || !form) return Promise.resolve(null);
      const upper = Math.max(3, Math.min(Number(maxExclusive) || 100, 100));
      const top = sliderMax(upper);
      const initial = Math.max(2, Math.min(top, Math.round(Number(defaultValue) || 25)));
      range.max = String(top);
      value.max = String(Math.round((upper - 0.01) * 100) / 100);
      range.value = String(initial);
      value.value = String(initial);
      if (sub) sub.textContent = `${recordCount || 1} provider record${recordCount === 1 ? "" : "s"}`;
      if (error) error.textContent = "";
      return new Promise((resolve) => {
        let result = null;
        const fromRange = () => {
          value.value = range.value;
          if (error) error.textContent = "";
        };
        const fromValue = () => {
          range.value = String(Math.max(2, Math.min(top, Math.round(Number(value.value) || initial))));
          if (error) error.textContent = "";
        };
        const onSubmit = (event) => {
          event.preventDefault();
          const percent = Number(value.value);
          if (!Number.isFinite(percent) || percent < 2 || percent >= upper) {
            if (error) error.textContent = `Use at least 2% and below ${Math.round(upper * 100) / 100}%. Use Watched for finished items.`;
            return;
          }
          result = Math.round(percent * 100) / 100;
          dialog.close();
        };
        const onClose = () => {
          range.removeEventListener("input", fromRange);
          value.removeEventListener("input", fromValue);
          form.removeEventListener("submit", onSubmit);
          dialog.removeEventListener("close", onClose);
          resolve(result);
        };
        range.addEventListener("input", fromRange);
        value.addEventListener("input", fromValue);
        form.addEventListener("submit", onSubmit);
        dialog.addEventListener("close", onClose);
        dialog.showModal();
        value.focus();
        value.select?.();
      });
    }

    async function runAction(action, items, { confirmFirst = false } = {}) {
      if (!canAct() || !items.length) return;
      const payloads = actionPayloads(items, action);
      if (!payloads.length) {
        toast(`${actionTitle(action)} is not supported for ${items.length === 1 ? "this record" : "the selected records"}.`, true);
        return;
      }
      let progressPercent = null;
      if (action === "update_progress") {
        const records = payloads.map((payload) => payload.record);
        const max = editableMax(records);
        progressPercent = await askProgress(averageProgress(records, max), payloads.length, max);
        if (progressPercent == null) return;
      } else if (confirmFirst) {
        const skipped = items.flatMap(recordsOf).length - payloads.length;
        const note = skipped ? ` ${skipped} unsupported record${skipped === 1 ? "" : "s"} will be skipped.` : "";
        if (!window.confirm(`${actionTitle(action)} for ${payloads.length} provider record${payloads.length === 1 ? "" : "s"}?${note}`)) return;
      }
      let succeeded = false;
      if (payloads.length === 1 && items.length === 1 && !items[0].is_combined) {
        const urls = {
          mark_watched: "/api/playback_progress/actions/mark_watched",
          update_progress: "/api/playback_progress/actions/update_progress",
          remove_progress: "/api/playback_progress/actions/remove",
        };
        const res = await postJson(urls[action], { ...payloads[0], progress_percent: progressPercent });
        succeeded = !!res.ok;
        toast(res.message || (res.ok ? "Done" : "Action failed"), !res.ok);
      } else {
        const res = await postJson("/api/playback_progress/actions/bulk", { action, progress_percent: progressPercent, items: payloads });
        const done = Number(res.successful) || 0;
        succeeded = done > 0;
        toast(`${count(done)} done · ${count(res.failed)} failed · ${count(res.unsupported)} unsupported`, !succeeded);
      }
      if (!succeeded) return;
      items.forEach((item) => state.selected.delete(keyOf(item)));
      await load({ force: true });
    }

    async function selectAllResults() {
      const data = await request(`/api/playback_progress/items?${query({ all: true })}`);
      (Array.isArray(data.items) ? data.items : []).forEach((item) => state.selected.set(keyOf(item), item));
      syncSelection();
    }

    function settingsMarkup(data) {
      const profiles = Array.isArray(data?.profiles) ? data.profiles : [];
      const hidden = profiles
        .filter((p) => !(p.configured && p.read))
        .map((p) => `<input type="checkbox" hidden disabled data-provider="${esc(p.provider || "")}" data-instance="${esc(p.instance_id || "default")}" data-included="${p.included !== false}">`)
        .join("");
      const groups = new Map();
      profiles.filter((p) => p.configured && p.read).forEach((p) => {
        const key = String(p.provider || "").toLowerCase();
        if (!key) return;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(p);
      });
      const sections = [...groups.entries()]
        .sort((a, b) => providerOrder(a[0]) - providerOrder(b[0]) || a[0].localeCompare(b[0]))
        .map(([provider, rows]) => {
          const label = visibleProviderLabel(provider) || provider.toUpperCase();
          const logo = providerLogLogo(provider);
          const pills = rows.map((p) => `<label class="cw-play-pill"><input type="checkbox" data-provider="${esc(p.provider || provider)}" data-instance="${esc(p.instance_id || "default")}"${p.included !== false ? " checked" : ""}><span><span class="material-symbols-rounded" aria-hidden="true">check</span>${esc(settingsLabel(p))}</span></label>`).join("");
          return `<section class="cw-play-provider"><span class="cw-play-provider-head">${logo ? `<img src="${esc(logo)}" alt="" onerror="this.remove()">` : ""}<strong>${esc(label)}</strong></span><span class="cw-play-provider-pills">${pills}</span></section>`;
        })
        .join("");
      return sections ? `${sections}${hidden}` : `<div class="cw-tl-dialog-note">No connected playback providers were found.</div>${hidden}`;
    }

    async function openSettings() {
      if (!canConfigure()) return;
      const dialog = q("providers");
      const list = q("provider-list");
      const timeout = q("timeout");
      const error = q("providers-error");
      if (!dialog?.showModal || !list || !timeout) return;
      if (error) error.textContent = "";
      list.innerHTML = `<div class="cw-tl-dialog-note">Loading provider profiles...</div>`;
      dialog.showModal();
      const data = await request("/api/playback_progress/settings");
      timeout.value = String(Math.round(Number(data.provider_timeout_seconds || DEFAULT_TIMEOUT)));
      list.innerHTML = settingsMarkup(data);
    }

    function resetSettings() {
      const timeout = q("timeout");
      if (timeout) timeout.value = String(DEFAULT_TIMEOUT);
      q("provider-list")?.querySelectorAll("input[type=checkbox][data-provider]").forEach((input) => {
        input.checked = !input.disabled;
        if (input.disabled) input.dataset.included = "false";
      });
    }

    async function saveSettings() {
      const dialog = q("providers");
      const error = q("providers-error");
      const seconds = Number(q("timeout")?.value);
      if (!Number.isFinite(seconds) || seconds < 3 || seconds > 60) {
        if (error) error.textContent = "Use a timeout between 3 and 60 seconds.";
        return;
      }
      const profiles = [...(q("provider-list")?.querySelectorAll("input[type=checkbox][data-provider]") || [])].map((input) => ({
        provider: input.dataset.provider,
        instance_id: input.dataset.instance,
        included: input.disabled ? input.dataset.included !== "false" : input.checked,
      }));
      const res = await postJson("/api/playback_progress/settings", { profiles, provider_timeout_seconds: seconds });
      if (!res.ok) {
        if (error) error.textContent = res.message || res.error || "Settings could not be saved.";
        return;
      }
      dialog?.close();
      toast("Playback providers saved");
      state.selected.clear();
      await load({ force: true });
    }

    function open({ fromHash = false } = {}) {
      renderSync();
      if (!state.loaded && !state.busy) void load();
      if (!state.clock) state.clock = window.setInterval(renderSync, 30000);
      if (fromHash) $(".cw-profile-tabs")?.scrollIntoView({ block: "start", behavior: "smooth" });
    }

    function wire() {
      const host = panel();
      if (!host) return;
      let searchTimer = null;
      q("settings")?.toggleAttribute("hidden", !canConfigure());
      enhanceSelects();
      syncViewButtons();
      syncFilterChrome();
      renderSync();
      document.addEventListener("click", (event) => {
        const panel = q("filters-panel");
        if (!panel || panel.hidden) return;
        const target = event.target;
        if (panel.contains(target)) return;
        if (target?.closest?.("[data-playback-filters-toggle]")) return;
        if (target?.closest?.(".cw-icon-select-menu")) return;
        toggleFilters(false);
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") toggleFilters(false);
      });
      host.addEventListener("click", (event) => {
        const target = event.target;
        const actionBtn = target?.closest?.("[data-playback-action]");
        if (actionBtn) {
          const item = state.items.find((entry) => keyOf(entry) === actionBtn.dataset.playbackKey);
          if (item) void runAction(actionBtn.dataset.playbackAction, [item]);
          return;
        }
        const bulkBtn = target?.closest?.("[data-playback-bulk]");
        if (bulkBtn) {
          void runAction(bulkBtn.dataset.playbackBulk, [...state.selected.values()], { confirmFirst: true });
          return;
        }
        const pick = target?.closest?.("[data-playback-bulk-select]");
        if (pick) {
          const mode = pick.dataset.playbackBulkSelect;
          if (mode === "all") {
            void selectAllResults();
            return;
          }
          if (mode === "visible") state.items.forEach((item) => state.selected.set(keyOf(item), item));
          if (mode === "none") state.selected.clear();
          syncSelection();
          return;
        }
        if (target?.closest?.("[data-playback-retry]")) {
          void load({ force: true });
          return;
        }
        if (target?.closest?.("[data-playback-filters-toggle]")) {
          toggleFilters();
          return;
        }
        if (target?.closest?.("[data-playback-filters-clear]")) {
          clearFilters();
          return;
        }
        if (target?.closest?.("[data-playback-open-settings]")) {
          void openSettings();
          return;
        }
        if (target?.closest?.("[data-playback-settings-reset]")) {
          resetSettings();
          return;
        }
        if (target?.closest?.("[data-playback-dialog-cancel]")) {
          target.closest("dialog")?.close();
          return;
        }
        if (target?.closest?.("[data-playback-timeline]")) {
          state.timeline = !state.timeline;
          savePrefs();
          syncViewButtons();
          paint();
          renderTimeline();
          return;
        }
        const viewBtn = target?.closest?.("[data-playback-view]");
        if (viewBtn) {
          const next = viewBtn.dataset.playbackView === "list" ? "list" : "grid";
          if (next === state.view) return;
          state.view = next;
          savePrefs();
          syncViewButtons();
          paint();
          return;
        }
        const typeBtn = target?.closest?.("[data-playback-type]");
        if (typeBtn) {
          state.filters.type = typeBtn.dataset.playbackType || "";
          state.page = 1;
          void load();
          return;
        }
        const tileBtn = target?.closest?.("[data-playback-filter]");
        if (tileBtn) {
          const key = tileBtn.dataset.playbackFilter;
          const value = tileBtn.dataset.playbackValue || "";
          setFilter(key, state.filters[key] === value ? "" : value);
          return;
        }
        const pageBtn = target?.closest?.("[data-playback-page]");
        if (!pageBtn || pageBtn.disabled) return;
        const page = Math.max(1, Number(pageBtn.dataset.playbackPage) || 1);
        if (page === state.page || page > state.pageCount) return;
        state.page = page;
        void load({ scroll: true });
      });
      bar().wire();
      host.addEventListener("change", (event) => {
        const box = event.target?.closest?.("[data-playback-select]");
        if (!box) return;
        const item = state.items.find((entry) => keyOf(entry) === box.dataset.playbackSelect);
        if (!item) return;
        if (box.checked) state.selected.set(keyOf(item), item);
        else state.selected.delete(keyOf(item));
        syncSelection();
      });
      Object.entries(selects()).forEach(([key, select]) => {
        select?.addEventListener("change", (event) => setFilter(key, event.target?.value || (key === "sort" ? "last_updated" : "")));
      });
      q("search")?.addEventListener("input", (event) => {
        clearTimeout(searchTimer);
        const value = event.target?.value || "";
        searchTimer = setTimeout(() => {
          state.filters.search = value;
          state.page = 1;
          void load();
        }, 220);
      });
      const pageSizeSelect = q("page-size");
      if (pageSizeSelect) {
        pageSizeSelect.value = String(state.pageSize);
        pageSizeSelect.addEventListener("change", (event) => {
          const next = Number(event.target?.value) || 48;
          if (next === state.pageSize) return;
          state.pageSize = next;
          state.page = 1;
          savePrefs();
          void load();
        });
      }
      q("providers")?.querySelector("form")?.addEventListener("submit", (event) => {
        event.preventDefault();
        void saveSettings();
      });
    }

    return { open, wire };
  })();

  function connectedServices(status) {
    const providers = status?.providers;
    if (!providers || typeof providers !== "object") return 0;
    let total = 0;
    for (const entry of Object.values(providers)) {
      if (!entry || typeof entry !== "object") continue;
      const instances = entry.instances;
      if (instances && typeof instances === "object") {
        const connected = Object.values(instances).filter((row) => row && row.connected === true).length;
        if (connected) { total += connected; continue; }
      }
      if (entry.connected === true) total += 1;
    }
    return total;
  }

  async function refreshProfile() {
    const data = await api("/api/profile");
    renderProfile(data);
    return data;
  }

  function wireTabs() {
    const tabFromHash = () => {
      const raw = profileRouteSegment(String(window.location?.hash || "").replace(/^#\/?/, "").split("?")[0].split("/")[0]);
      const key = raw === "security" || raw === "preferences" ? "account" : raw;
      return ["overview", "collection", "history", "ratings", "playback", "watchlist", "account"].includes(key) ? key : "";
    };
    const selectTab = (key, opts = {}) => {
      if (key === "account" && viewingAs()) return false;
      const btn = document.querySelector(`[data-profile-tab="${key}"]`);
      if (!btn) return false;
      document.querySelectorAll("[data-profile-tab]").forEach((tab) => tab.classList.toggle("active", tab === btn));
      document.querySelectorAll(".cw-profile-panel").forEach((panel) => panel.classList.toggle("active", panel.id === `profile-panel-${key}`));
      if (key === "collection" && !collectionState.loaded) void loadCollection({ reset: true });
      if (key === "history") historyPanel.open(opts);
      if (key === "ratings") ratingsPanel.open(opts);
      if (key === "playback") playbackPanel.open(opts);
      if (key === "watchlist") watchlistPanel.open(opts);
      return true;
    };
    document.querySelectorAll("[data-profile-tab]").forEach((btn) => {
      btn.addEventListener("click", () => {
        selectTab(btn.dataset.profileTab);
      });
    });
    window.addEventListener("hashchange", () => {
      const key = tabFromHash();
      if (key) selectTab(key, { fromHash: true });
    });
    const initial = tabFromHash();
    if (initial) selectTab(initial, { fromHash: true });
  }

  function wireAvatar() {
    const input = $("#profile-avatar-input");
    const pick = () => {
      if (!viewingAs()) input?.click();
    };
    $("#profile-avatar-button")?.addEventListener("click", pick);
    $("#profile-avatar-replace")?.addEventListener("click", pick);
    $("#profile-avatar-remove")?.addEventListener("click", async (event) => {
      if (!armConfirm(event.currentTarget, "Confirm remove")) return;
      try {
        const data = await del("/api/profile/avatar");
        renderProfile(data);
        toast("Profile picture removed");
      } catch (e) {
        toast(e.message || "Could not remove profile picture", true);
      }
    });
    input?.addEventListener("change", () => {
      const file = input.files?.[0];
      if (!file) return;
      if (file.size > 5 * 1024 * 1024) {
        toast("Profile picture must be 5 MB or smaller", true);
        input.value = "";
        return;
      }
      const allowed = ["image/png", "image/jpeg", "image/webp"];
      if (!allowed.includes(file.type)) {
        toast("Use PNG, JPG or WebP", true);
        input.value = "";
        return;
      }
      setAvatarUploadState({ visible: true, label: "Reading picture", percent: 3 });
      let previewUrl = "";
      (async () => {
        try {
          const prepared = await prepareAvatarUpload(file);
          previewUrl = prepared.previewUrl;
          setAvatar(previewUrl);
          setAvatarUploadState({ visible: true, label: "Uploading picture", percent: 50 });
          const data = await uploadAvatar(prepared.dataUrl, prepared.contentType, (percent) => {
            setAvatarUploadState({ visible: true, label: "Uploading picture", percent: Math.max(50, percent) });
          });
          if (data?.user?.avatar_url) data.user.avatar_url = bustUrl(data.user.avatar_url);
          setAvatarUploadState({ visible: true, label: "Saving picture", percent: 100 });
          renderProfile(data);
          toast("Profile picture updated");
        } catch (e) {
          setAvatar(profile?.avatar_url || "");
          toast(e.message || "Could not upload profile picture", true);
        } finally {
          if (previewUrl) setTimeout(() => URL.revokeObjectURL(previewUrl), 1000);
          setTimeout(() => setAvatarUploadState({ visible: false }), 500);
          input.value = "";
        }
      })();
    });
  }

  function wireForms() {
    $("#profile-name-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        const data = await post("/api/profile", { display_name: $("#profile-display-input")?.value || "" });
        renderProfile(data);
        toast("Profile saved");
      } catch (e) {
        toast(e.message || "Could not save profile", true);
      }
    });
    const passwordForm = $("#profile-password-form");
    const passwordToggle = $("#profile-password-toggle");
    const showPasswordForm = (open) => {
      if (passwordForm) passwordForm.hidden = !open;
      if (passwordToggle) {
        passwordToggle.hidden = open;
        passwordToggle.setAttribute("aria-expanded", String(open));
      }
      if (open) $("#profile-current-password")?.focus();
      else passwordForm?.reset();
    };
    passwordToggle?.addEventListener("click", () => showPasswordForm(true));
    $("#profile-password-cancel")?.addEventListener("click", () => showPasswordForm(false));
    passwordForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        const data = await post("/api/profile/password", {
          current_password: $("#profile-current-password")?.value || "",
          new_password: $("#profile-new-password")?.value || "",
        });
        renderProfile(data);
        showPasswordForm(false);
        toast("Password changed");
      } catch (e) {
        toast(e.message || "Could not change password", true);
      }
    });
  }

  function recoveryCodesList(codes) {
    const wrap = document.createElement("div");
    wrap.className = "cw-profile-codes";
    const title = document.createElement("strong");
    title.textContent = "Backup codes";
    wrap.appendChild(title);
    for (const value of codes) {
      const code = document.createElement("code");
      code.textContent = value;
      wrap.appendChild(code);
    }
    return wrap;
  }

  function showRecoveryCodes(codes) {
    const host = $("#profile-2fa-setup");
    if (!host) return;
    const values = (Array.isArray(codes) ? codes : []).map((code) => String(code || "").trim()).filter(Boolean);
    host.replaceChildren();
    const wrap = document.createElement("div");
    wrap.className = "cw-profile-codes cw-profile-codes--locked";
    const title = document.createElement("strong");
    title.textContent = values.length ? "Backup codes ready" : "No backup codes available";
    const copy = document.createElement("span");
    copy.textContent = values.length
      ? "Each code works once if you lose your phone. Keep them somewhere safe."
      : "No new backup codes were created.";
    wrap.append(title, copy);
    if (values.length) {
      const reveal = document.createElement("button");
      reveal.className = "btn";
      reveal.type = "button";
      reveal.textContent = "Show backup codes";
      reveal.addEventListener("click", () => {
        host.replaceChildren(recoveryCodesList(values));
      }, { once: true });
      wrap.appendChild(reveal);
    }
    host.appendChild(wrap);
  }

  function renderPlexStatus(status) {
    const state = $("#profile-plex-state");
    const summary = $("#profile-plex-summary");
    const link = $("#profile-plex-link");
    const unlink = $("#profile-plex-unlink");
    const linked = !!status?.linked;
    if (state) {
      state.textContent = linked ? "Linked" : "Off";
      state.classList.toggle("is-enabled", linked);
    }
    if (summary) summary.textContent = linked ? linkedAccountLabel(status, "Plex account") : "Not linked";
    if (link) link.textContent = linked ? "Replace" : "Link";
    if (unlink) unlink.hidden = !linked;
    const logo = $("#profile-plex-logo");
    const path = providerLogo("PLEX");
    if (logo && path && !logo.getAttribute("src")) {
      logo.onerror = () => {
        logo.hidden = true;
        if (logo.nextElementSibling) logo.nextElementSibling.hidden = false;
      };
      logo.src = path;
      logo.hidden = false;
      if (logo.nextElementSibling) logo.nextElementSibling.hidden = true;
    }
  }

  function linkedAccountLabel(status, fallback) {
    const name = String(status?.linked_username || status?.linked_email || "").trim();
    const email = String(status?.linked_email || "").trim();
    return [name || fallback, email && email !== name ? email : ""].filter(Boolean).join(" · ");
  }

  async function refreshPlexStatus() {
    try {
      renderPlexStatus(await api("/api/app-auth/plex/status"));
    } catch {
      renderPlexStatus({ linked: false });
    }
  }

  async function pollPlexLink(state) {
    for (let i = 0; i < 90; i += 1) {
      const data = await post("/api/app-auth/plex/link/check", { state });
      if (!data.pending) return data;
      await new Promise((resolve) => setTimeout(resolve, 1800));
    }
    throw new Error("Plex link timed out");
  }

  function wirePlexSso() {
    $("#profile-plex-link")?.addEventListener("click", async () => {
      try {
        const data = await post("/api/app-auth/plex/link/start", {});
        if (data.auth_url) window.open(data.auth_url, "cwPlexLink", "popup,width=720,height=760");
        toast("Finish linking in the Plex window");
        const done = await pollPlexLink(data.state);
        renderPlexStatus(done);
        toast("Plex account linked");
      } catch (e) {
        toast(e.message || "Could not link Plex account", true);
      }
    });
    $("#profile-plex-unlink")?.addEventListener("click", async (event) => {
      if (!armConfirm(event.currentTarget, "Confirm unlink")) return;
      try {
        renderPlexStatus(await post("/api/app-auth/plex/unlink", {}));
        toast("Plex account unlinked");
      } catch (e) {
        toast(e.message || "Could not unlink Plex account", true);
      }
    });
  }

  function renderOidcStatus(status) {
    const state = $("#profile-oidc-state");
    const summary = $("#profile-oidc-summary");
    const link = $("#profile-oidc-link");
    const unlink = $("#profile-oidc-unlink");
    const configured = !!status?.configured;
    const linked = !!status?.linked;
    if (state) {
      state.textContent = linked ? "Linked" : (configured ? "Off" : "Unavailable");
      state.classList.toggle("is-enabled", linked);
    }
    if (summary) {
      summary.textContent = !configured
        ? "Not available on this server"
        : linked
          ? linkedAccountLabel(status, "OIDC account")
          : "Not linked";
    }
    if (link) {
      link.textContent = linked ? "Replace" : "Link";
      link.disabled = !configured;
    }
    if (unlink) unlink.hidden = !linked;
  }

  async function refreshOidcStatus() {
    try {
      renderOidcStatus(await api("/api/app-auth/oidc/status"));
    } catch {
      renderOidcStatus({ configured: false, linked: false });
    }
  }

  async function pollOidcLink(state) {
    for (let i = 0; i < 90; i += 1) {
      const data = await post("/api/app-auth/oidc/link/check", { state });
      if (!data.pending) return data;
      await new Promise((resolve) => setTimeout(resolve, 1800));
    }
    throw new Error("OIDC link timed out");
  }

  function wireOidcSso() {
    $("#profile-oidc-link")?.addEventListener("click", async () => {
      try {
        const data = await post("/api/app-auth/oidc/link/start", {});
        if (data.auth_url) window.open(data.auth_url, "cwOidcLink", "popup,width=720,height=760");
        toast("Finish linking in the OIDC window");
        const done = await pollOidcLink(data.state);
        renderOidcStatus(done);
        toast("OIDC account linked");
      } catch (e) {
        toast(e.message || "Could not link OIDC account", true);
      }
    });
    $("#profile-oidc-unlink")?.addEventListener("click", async (event) => {
      if (!armConfirm(event.currentTarget, "Confirm unlink")) return;
      try {
        renderOidcStatus(await post("/api/app-auth/oidc/unlink", {}));
        toast("OIDC account unlinked");
      } catch (e) {
        toast(e.message || "Could not unlink OIDC account", true);
      }
    });
  }

  function wireSecurity() {
    $("#profile-sessions")?.addEventListener("click", async (event) => {
      const btn = event.target?.closest?.("[data-session-kill]");
      if (!btn || !armConfirm(btn, "Confirm")) return;
      btn.disabled = true;
      try {
        const data = await del(`/api/profile/sessions/${encodeURIComponent(btn.dataset.sessionKill || "")}`);
        renderProfile(data);
        toast("Session signed out");
      } catch (e) {
        btn.disabled = false;
        toast(e.message || "Could not sign out session", true);
      }
    });
    const confirmBox = $("#profile-2fa-confirm");
    const confirmInput = $("#profile-2fa-confirm-password");
    const confirmGo = $("#profile-2fa-confirm-go");
    let confirmAction = "";
    const closeConfirm = () => {
      confirmAction = "";
      if (confirmBox) confirmBox.hidden = true;
      if (confirmInput) confirmInput.value = "";
    };
    const askPassword = (action, label, go) => {
      confirmAction = action;
      const text = $("#profile-2fa-confirm-label");
      if (text) text.textContent = label;
      if (confirmGo) confirmGo.textContent = go;
      if (confirmBox) confirmBox.hidden = false;
      confirmInput?.focus();
    };
    const runConfirm = async () => {
      const current = confirmInput?.value || "";
      if (!current || !confirmAction) {
        confirmInput?.focus();
        return;
      }
      const action = confirmAction;
      if (confirmGo) confirmGo.disabled = true;
      try {
        if (action === "disable") {
          const data = await post("/api/profile/totp/disable", { current_password: current });
          renderProfile(data);
          const host = $("#profile-2fa-setup");
          if (host) host.innerHTML = "";
          closeConfirm();
          toast("Two-step verification turned off");
        } else {
          const data = await post("/api/profile/recovery-codes", { current_password: current });
          renderProfile(data);
          closeConfirm();
          showRecoveryCodes(data.recovery_codes || []);
          toast("New backup codes created");
        }
      } catch (e) {
        toast(e.message || (action === "disable" ? "Could not disable 2FA" : "Could not generate recovery codes"), true);
      } finally {
        if (confirmGo) confirmGo.disabled = false;
      }
    };
    confirmGo?.addEventListener("click", () => void runConfirm());
    $("#profile-2fa-confirm-cancel")?.addEventListener("click", closeConfirm);
    confirmInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        void runConfirm();
      } else if (event.key === "Escape") {
        closeConfirm();
      }
    });
    $("#profile-2fa-setup-btn")?.addEventListener("click", async () => {
      closeConfirm();
      try {
        const data = await post("/api/profile/totp/setup", {});
        $("#profile-2fa-setup").innerHTML = `<div class="cw-profile-qr"><div class="cw-profile-qr-code">${data.qr_svg || '<span class="material-symbols-rounded" aria-hidden="true">qr_code_2</span>'}</div><div class="cw-profile-qr-copy"><strong>Scan with your authenticator app</strong><span>Or enter this setup key manually.</span><code>${esc(data.secret || "")}</code><div class="cw-profile-qr-verify"><input id="profile-2fa-code" placeholder="123456" inputmode="numeric" autocomplete="one-time-code"><button id="profile-2fa-verify-now" class="btn primary" type="button">Verify</button></div></div></div>`;
        $("#profile-2fa-verify-now")?.addEventListener("click", async () => {
          try {
            const done = await post("/api/profile/totp/verify", { code: $("#profile-2fa-code")?.value || "" });
            renderProfile(done);
            if (Array.isArray(done.recovery_codes)) showRecoveryCodes(done.recovery_codes);
            toast("Two-step verification turned on");
          } catch (e) {
            toast(e.message || "Invalid verification code", true);
          }
        });
      } catch (e) {
        toast(e.message || "Could not start 2FA setup", true);
      }
    });
    $("#profile-2fa-disable-btn")?.addEventListener("click", () => askPassword("disable", "Enter your password to turn off two-step verification", "Turn off"));
    $("#profile-recovery-btn")?.addEventListener("click", () => askPassword("recovery", "Enter your password to create new backup codes", "Create codes"));
    $("#profile-revoke-sessions")?.addEventListener("click", async (event) => {
      if (!armConfirm(event.currentTarget, "Confirm sign out")) return;
      try {
        const data = await post("/api/profile/sessions/revoke-others", {});
        renderProfile(data);
        toast("Other sessions signed out");
      } catch (e) {
        toast(e.message || "Could not sign out other sessions", true);
      }
    });
  }

  function wirePosterOverlay() {
    const openFromTarget = (target, event) => {
      if (target?.closest?.("[data-collection-resize]")) return false;
      const btn = target?.closest?.("[data-profile-poster-key]");
      if (!btn) return false;
      const item = posterItems.get(btn.dataset.profilePosterKey || "");
      const modal = window.CW?.ProfileMediaModal;
      if (item && modal?.canOpen?.(item)) {
        event?.preventDefault?.();
        void modal.open(item);
        return true;
      }
      const open = window.CW?.WatchlistPreview?.openPreviewDrawer || window.openPreviewDrawer;
      if (!item || !open) return false;
      event?.preventDefault?.();
      void open(item);
      return true;
    };
    const shell = $(".cw-profile-shell");
    shell?.addEventListener("click", (event) => {
      openFromTarget(event.target, event);
    });
    shell?.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      const btn = event.target?.closest?.("[data-profile-poster-key]");
      if (!btn) return;
      openFromTarget(btn, event);
    });
  }

  function wireSettingsNav() {
    const nav = $(".cw-set-nav");
    if (!nav) return;
    const buttons = [...nav.querySelectorAll("[data-settings-nav]")];
    const mark = (key) => buttons.forEach((btn) => btn.classList.toggle("active", btn.dataset.settingsNav === key));
    nav.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-settings-nav]");
      if (!btn) return;
      mark(btn.dataset.settingsNav);
      document.getElementById(`settings-${btn.dataset.settingsNav}`)?.scrollIntoView({ block: "start", behavior: "smooth" });
    });
    if (!("IntersectionObserver" in window)) return;
    const observer = new IntersectionObserver((entries) => {
      const hit = entries
        .filter((entry) => entry.isIntersecting)
        .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
      if (hit) mark(hit.target.dataset.settingsSection);
    }, { rootMargin: "-20% 0px -65% 0px" });
    document.querySelectorAll("[data-settings-section]").forEach((section) => observer.observe(section));
  }

  function wirePreferences() {
    const inputs = [$("#profile-pref-playing-card"), $("#profile-pref-quick-add")].filter(Boolean);
    for (const input of inputs) {
      input.addEventListener("change", async () => {
        inputs.forEach((node) => { node.disabled = true; });
        try {
          const data = await api("/api/profile", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              preferences: {
                playing_card: $("#profile-pref-playing-card")?.checked !== false,
                quick_add: $("#profile-pref-quick-add")?.checked !== false,
              },
            }),
          });
          renderProfile(data);
          toast("Preferences saved");
        } catch (e) {
          input.checked = !input.checked;
          toast(e.message || "Preferences could not be saved", true);
        } finally {
          inputs.forEach((node) => { node.disabled = false; });
        }
      });
    }
  }

  let nowTimer = null;

  async function openUpgradeNotice() {
    if (document.documentElement?.dataset?.cwRole !== "admin") return;
    try {
      const meta = await api("/api/config/meta");
      if (!meta?.needs_upgrade) return;
      for (let i = 0; i < 100 && typeof window.openUpgradeWarning !== "function"; i += 1) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      if (typeof window.openUpgradeWarning === "function") await window.openUpgradeWarning(meta);
    } catch {}
  }

  function syncViewAsUrl() {
    try {
      const url = new URL(window.location.href);
      if (viewAsState.id) url.searchParams.set(VIEW_AS_PARAM, viewAsState.id);
      else url.searchParams.delete(VIEW_AS_PARAM);
      window.history.replaceState(window.history.state, "", `${url.pathname}${url.search}${url.hash}`);
    } catch {}
  }

  function renderViewAs() {
    const on = viewingAs();
    document.documentElement.dataset.profileViewAs = on ? "on" : "off";
    const banner = $("#profile-view-as-banner");
    if (banner) banner.hidden = !on;
    const name = $("#profile-view-as-name");
    if (name) name.textContent = viewAsState.label || viewAsState.id;
    const settingsTab = $("#profile-tab-account");
    if (settingsTab) settingsTab.hidden = on;
    const hero = $("#profile-avatar-button");
    if (on && hero) {
      hero.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">person</span>`;
      hero.title = "Profile picture";
      hero.setAttribute("aria-label", "Profile picture");
    }
  }

  async function resolveViewAs() {
    const overview = window.CW?.OverviewProfile;
    if (document.documentElement?.dataset?.cwRole !== "admin" || !overview) return;
    const ready = overview.ready;
    if (ready && typeof ready.then === "function") {
      await Promise.race([ready.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 3000))]);
    }
    const wanted = new URLSearchParams(window.location.search).get(VIEW_AS_PARAM);
    if (wanted !== null) overview.setActive(wanted);
    viewAsState.id = String(overview.id || "");
    viewAsState.label = String(overview.label || "");
    syncViewAsUrl();
    renderViewAs();
    $("#profile-view-as-exit")?.addEventListener("click", () => overview.setActive(""));
    window.addEventListener("cw:overview-profile-changed", (event) => {
      const next = String(event?.detail?.id || "");
      if (next === viewAsState.id) return;
      viewAsState.id = next;
      syncViewAsUrl();
      window.location.reload();
    });
  }

  async function init() {
    await resolveViewAs();
    historyPanel.wire();
    ratingsPanel.wire();
    playbackPanel.wire();
    watchlistPanel.wire();
    wireTabs();
    wireCollection();
    wirePreferences();
    wireAvatar();
    wireForms();
    wireSecurity();
    wireSettingsNav();
    wirePlexSso();
    wireOidcSso();
    wirePosterOverlay();
    wireProfileActivity();
    void openUpgradeNotice();
    const [profileResult, overviewResult, activityResult] = await Promise.allSettled([
      refreshProfile(),
      loadOverview(),
      loadProfileActivity(),
    ]);
    void refreshPlexStatus();
    void refreshOidcStatus();
    if (profileResult.status === "rejected") toast(profileResult.reason?.message || "Profile could not be loaded", true);
    if (overviewResult.status === "rejected") toast(overviewResult.reason?.message || "Profile overview could not be loaded", true);
    if (activityResult.status === "rejected") {
      const host = $("#profile-activity");
      if (host) host.innerHTML = empty("Recent activity could not be loaded.");
    }
    if (nowTimer) clearInterval(nowTimer);
    nowTimer = setInterval(() => {
      if (document.visibilityState === "visible") refreshNowPlaying();
    }, 15000);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
