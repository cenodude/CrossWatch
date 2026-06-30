/* assets/js/dashboard-widgets.js */
/* CrossWatch dashboard media widgets */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  let cfgPromise = null;
  let loadSeq = 0;
  let authRetryQueued = false;
  let hasLoaded = false;
  let widgetsDirty = true;
  let dirtyVersion = 1;
  let lastLoadedAt = 0;
  let lastSettings = null;
  let scrobbleStopRefreshTimer = null;
  const PAGE_STEP = 6;
  const RATING_PAGE_STEP = 9;
  const MAX_WIDGET_ITEMS = 24;
  const WIDGET_REFRESH_TTL_MS = 60 * 1000;
  const visibleCounts = { history: PAGE_STEP, ratings: RATING_PAGE_STEP, scrobble: PAGE_STEP };
  const latestItems = { history: [], ratings: [], scrobble: [] };

  function authPendingError(e) {
    return String(e?.message || e || "").includes("auth setup pending");
  }

  function scheduleAuthReadyRefresh() {
    if (authRetryQueued) return;
    authRetryQueued = true;
    Promise.resolve(window.__cwAuthBootstrapPromise || null)
      .catch(() => null)
      .finally(() => {
        authRetryQueued = false;
        if (authSetupPending()) return;
        window.setTimeout(() => refreshDashboardWidgets({ forceConfig: true }), 25);
      });
  }

  function scheduleWidgetRefresh(delay = 150, opts = {}) {
    window.setTimeout(() => refreshDashboardWidgets(opts), delay);
  }

  function revealCachedWidgets() {
    if (hasLoaded && lastSettings) applyVisibility(lastSettings);
  }

  function markWidgetsDirty(delay = 150, opts = {}) {
    widgetsDirty = true;
    dirtyVersion += 1;
    if (!isOnMain()) return;
    scheduleWidgetRefresh(delay, { ...opts, force: true, preserve: hasLoaded });
  }

  function scheduleScrobbleStopRefresh() {
    window.clearTimeout(scrobbleStopRefreshTimer);
    scrobbleStopRefreshTimer = window.setTimeout(() => {
      scrobbleStopRefreshTimer = null;
      markWidgetsDirty(0);
    }, 1000);
  }

  async function fetchJSON(url) {
    if (authSetupPending()) throw new Error("auth setup pending");
    if (window.CW?.API?.j) return window.CW.API.j(url);
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  async function getConfig(force = false) {
    if (cfgPromise) return cfgPromise;
    cfgPromise = (async () => {
      try {
        if (window.CW?.API?.Config?.load) return window.CW.API.Config.load(!!force);
        return await fetchJSON("/api/config");
      } finally {
        window.setTimeout(() => { cfgPromise = null; }, 1500);
      }
    })();
    return cfgPromise;
  }

  function isOnMain() {
    const tab = String(document.documentElement.dataset.tab || "").toLowerCase();
    if (tab) return tab === "main";
    return !!document.getElementById("tab-main")?.classList.contains("active");
  }

  function widgetSettings(ui) {
    return {
      history: typeof ui?.show_recent_history_widget === "boolean" ? !!ui.show_recent_history_widget : true,
      ratings: typeof ui?.show_latest_ratings_widget === "boolean" ? !!ui.show_latest_ratings_widget : true,
      scrobble: typeof ui?.show_recent_scrobble_widget === "boolean" ? !!ui.show_recent_scrobble_widget : true,
    };
  }

  function hasTmdbKeyInConfig(cfg) {
    const pickFromBlock = (block) => {
      if (!block || typeof block !== "object") return "";
      const direct = String(block.api_key || "").trim();
      if (direct) return direct;
      const insts = block.instances;
      if (!insts || typeof insts !== "object") return "";
      for (const value of Object.values(insts)) {
        const key = value && typeof value === "object" ? String(value.api_key || "").trim() : "";
        if (key) return key;
      }
      return "";
    };
    return !!pickFromBlock(cfg?.tmdb);
  }

  function hideDashboardWidgets() {
    $("#dashboard-widgets-card")?.classList.add("hidden");
  }

  function providerMeta() {
    return window.CW?.ProviderMeta || {};
  }

  function providerLabel(provider) {
    const key = String(provider || "").trim().toUpperCase();
    return providerMeta().label?.(key) || key;
  }

  function providerShort(provider) {
    const key = String(provider || "").trim().toUpperCase();
    return providerMeta().shortLabel?.(key) || providerLabel(key);
  }

  function providerLogo(provider) {
    const key = String(provider || "").trim().toUpperCase();
    return providerMeta().logoPath?.(key) || "";
  }

  function sourceRows(sources, max = 4) {
    const seen = new Set();
    const rows = [];
    for (const src of Array.isArray(sources) ? sources : []) {
      const provider = String(src?.provider || "").trim().toUpperCase();
      const instance = String(src?.instance || "default").trim() || "default";
      const key = `${provider}:${instance}`;
      if (!provider || seen.has(key)) continue;
      seen.add(key);
      rows.push({ provider, instance });
      if (rows.length >= max) break;
    }
    return rows;
  }

  function sourceLabel({ provider, instance }) {
    return instance.toLowerCase() === "default" ? providerLabel(provider) : `${providerLabel(provider)} (${instance})`;
  }

  function sourceRouteTitle(sources) {
    const rows = sourceRows(sources, 8);
    if (!rows.length) return "";
    const labels = rows.map(sourceLabel);
    return labels.length > 1 ? `Route: ${labels.join(" -> ")}` : `Source: ${labels[0]}`;
  }

  function sourceIcons(sources, max = 4) {
    return sourceRows(sources, max).map(({ provider, instance }) => {
      const logo = providerLogo(provider);
      const label = sourceLabel({ provider, instance });
      return logo
        ? `<span class="cw-dash-source"><img src="${esc(logo)}" alt="${esc(label)} logo"></span>`
        : `<span class="cw-dash-source cw-dash-source--text" aria-label="${esc(label)}">${esc(providerShort(provider).slice(0, 3))}</span>`;
    }).join("");
  }

  function scrobbleProfileLabel(instance) {
    const raw = String(instance || "default").trim() || "default";
    if (raw.toLowerCase() === "default") return "Default";
    const profile = raw.toUpperCase().match(/(?:^|[^A-Z0-9])(P\d{1,3})$/);
    return profile?.[1] || raw;
  }

  function scrobbleSourceLabel(source) {
    const provider = String(source?.provider || "").trim().toUpperCase();
    if (!provider) return "";
    const instance = String(source?.instance || "default").trim() || "default";
    const profile = scrobbleProfileLabel(instance);
    const providerName = providerLabel(provider);
    const logo = providerMeta().logLogoPath?.(provider) || providerLogo(provider);
    const providerIcon = logo
      ? `<img src="${esc(logo)}" alt="" aria-hidden="true">`
      : `<span class="cw-scrobble-source-icon--text" aria-hidden="true">${esc(providerShort(provider).slice(0, 2))}</span>`;
    const tone = providerMeta().tone?.(provider) || {};
    const rgb = String(tone?.rgb || "124,92,255");
    const description = `${providerName} profile: ${profile}`;
    return `<span class="cw-scrobble-source" style="--cw-source-rgb:${esc(rgb)}" title="${esc(description)}" aria-label="${esc(description)}">${providerIcon}<span>${esc(profile)}</span></span>`;
  }

  function scrobbleSinkIcons(targets) {
    const sinkRows = sourceRows(targets, 8).filter(({ provider }) => providerMeta().get?.(provider)?.scrobblerSink === true);
    const route = sinkRows.length ? `Sinks: ${sinkRows.map(sourceLabel).join(", ")}` : "No scrobble sinks";
    return {
      html: sourceIcons(sinkRows, 8),
      route,
    };
  }

  function countLabel(total, noun) {
    const n = Number(total || 0);
    const label = n === 1 ? noun : `${noun}s`;
    return `${Number.isFinite(n) ? n : 0} ${label}`;
  }

  function setCountChip(id, total, noun) {
    const chip = $(`#${id}`);
    if (!chip) return;
    const count = Number(total || 0);
    chip.textContent = String(Number.isFinite(count) ? count : 0);
    chip.setAttribute("aria-label", countLabel(total, noun));
    chip.classList.remove("hidden");
  }

  function typeLabel(item) {
    const raw = String(item?.type || "").toLowerCase();
    if (raw === "episode") return item?.episode_label || "Episode";
    if (raw === "season") return "Season";
    if (raw === "show") return "Show";
    return "Movie";
  }

  function relTime(epoch) {
    const ts = Number(epoch || 0);
    if (!Number.isFinite(ts) || ts <= 0) return "";
    if (typeof window.relTimeFromEpoch === "function") return window.relTimeFromEpoch(ts);
    const delta = Math.max(1, Math.floor(Date.now() / 1000) - ts);
    const units = [
      ["year", 31536000],
      ["month", 2592000],
      ["week", 604800],
      ["day", 86400],
      ["hour", 3600],
      ["minute", 60],
    ];
    for (const [name, seconds] of units) {
      if (delta >= seconds) {
        const n = Math.floor(delta / seconds);
        return `${n} ${name}${n === 1 ? "" : "s"} ago`;
      }
    }
    return "just now";
  }

  function poster(item, size = "w300") {
    const src = String(item?.poster || "");
    if (src) return src;
    const tmdb = item?.tmdb;
    if (!tmdb) return "/assets/img/placeholder_poster.svg";
    const kind = String(item?.art_type || item?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `/art/tmdb/${kind}/${encodeURIComponent(String(tmdb))}?size=${encodeURIComponent(size)}`;
  }

  function tmdbLink(item) {
    const tmdb = item?.tmdb;
    if (!tmdb) return "";
    const kind = String(item?.art_type || item?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `https://www.themoviedb.org/${kind}/${encodeURIComponent(String(tmdb))}`;
  }

  function historyCard(item) {
    const title = item?.title || "Untitled";
    const meta = [typeLabel(item), item?.year || "", relTime(item?.sort_epoch || item?.watched_at)].filter(Boolean).join(" - ");
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const art = poster(item);
    const artStyle = art ? ` style="--cw-history-art:url(&quot;${esc(art)}&quot;)"` : "";
    const route = sourceRouteTitle(item?.sources);
    return `
      <${tag} class="cw-history-widget-item"${hrefAttr}${artStyle}>
        <span class="cw-history-thumb">
          <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
          ${item?.episode_label ? `<span class="cw-history-episode">${esc(item.episode_label)}</span>` : ""}
        </span>
        <span class="cw-history-copy">
          <strong>${esc(title)}</strong>
          <span>${esc(meta || "Watched")}</span>
        </span>
        <span class="cw-history-sources" title="${esc(route)}" aria-label="${esc(route || "Sources")}">${sourceIcons(item?.sources, 3)}</span>
      </${tag}>`;
  }

  function activityLabel(item) {
    const method = String(item?.method || "").toLowerCase();
    const event = String(item?.event || "").toLowerCase();
    if (method === "webhook") return "Webhook";
    if (event.includes("history")) return "History sync";
    if (method === "watcher") return "Watcher";
    return "Activity";
  }

  function activityCard(item) {
    const title = item?.title || "Untitled";
    const meta = [activityLabel(item), typeLabel(item), relTime(item?.sort_epoch || item?.captured_at || item?.watched_at)].filter(Boolean).join(" - ");
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const art = poster(item);
    const artStyle = art ? ` style="--cw-history-art:url(&quot;${esc(art)}&quot;)"` : "";
    const source = item?.source || sourceRows(item?.sources, 1)[0] || null;
    const sinks = scrobbleSinkIcons(item?.targets);
    return `
      <${tag} class="cw-history-widget-item cw-history-widget-item--activity"${hrefAttr}${artStyle}>
        <span class="cw-history-thumb">
          <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
          ${item?.episode_label ? `<span class="cw-history-episode">${esc(item.episode_label)}</span>` : ""}
        </span>
        <span class="cw-history-copy">
          <strong>${esc(title)}</strong>
          <span>${esc(meta || "Activity")}</span>
        </span>
        <span class="cw-scrobble-route">
          ${scrobbleSourceLabel(source)}
          <span class="cw-history-sources" title="${esc(sinks.route)}" aria-label="${esc(sinks.route)}">${sinks.html}</span>
        </span>
      </${tag}>`;
  }

  function ratingCard(item) {
    const title = item?.title || "Untitled";
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const route = sourceRouteTitle(item?.sources);
    const rawType = String(item?.type || "").toLowerCase();
    const ratedLabel = relTime(item?.sort_epoch || 0);
    const season = Number(item?.season || 0);
    const episode = Number(item?.episode || 0);
    const mediaDetail = rawType === "episode"
      ? (item?.episode_label || (season && episode ? `S${String(season).padStart(2, "0")}E${String(episode).padStart(2, "0")}` : "Episode"))
      : rawType === "season" && season
        ? `S${String(season).padStart(2, "0")}`
        : "";
    const titleParts = [title, mediaDetail ? `${rawType === "season" ? "Season" : "Episode"}: ${mediaDetail}` : "", ratedLabel ? `Rated ${ratedLabel}` : ""].filter(Boolean);
    return `
      <${tag} class="cw-rating-widget-card"${hrefAttr} title="${esc(titleParts.join(" | "))}">
        <img src="${esc(poster(item, "w342"))}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        <span class="cw-rating-score"><span>${esc(item?.rating || "")}</span></span>
        <span class="cw-rating-overlay">
          <span class="cw-rating-sources" title="${esc(route)}" aria-label="${esc(route || "Sources")}">${sourceIcons(item?.sources, 3)}</span>
          ${ratedLabel ? `<span class="cw-rating-age">${esc(ratedLabel)}</span>` : ""}
        </span>
      </${tag}>`;
  }

  function setEmpty(host, text) {
    if (host) host.innerHTML = `<div class="cw-dash-empty">${esc(text)}</div>`;
  }

  function setLoading(host, kind = "list") {
    if (!host) return;
    if (kind === "ratings") {
      host.innerHTML = Array.from({ length: 6 }, () => `
        <div class="cw-rating-widget-card cw-dash-skeleton cw-dash-skeleton-poster" aria-hidden="true">
          <span class="cw-skel-shine"></span>
        </div>`).join("");
      return;
    }
    host.innerHTML = Array.from({ length: 3 }, () => `
      <div class="cw-history-widget-item cw-dash-skeleton cw-dash-skeleton-row" aria-hidden="true">
        <span class="cw-history-thumb cw-skel-block"></span>
        <span class="cw-history-copy">
          <span class="cw-skel-line cw-skel-line--title"></span>
          <span class="cw-skel-line cw-skel-line--meta"></span>
        </span>
        <span class="cw-history-sources">
          <span class="cw-dash-source cw-skel-dot"></span>
          <span class="cw-dash-source cw-skel-dot"></span>
        </span>
      </div>`).join("");
  }

  function renderPagedList(host, items, count, cardFn, emptyText, kind, keepPager = false) {
    if (!host) return;
    if (!items.length) {
      setEmpty(host, emptyText);
      return;
    }
    const visible = Math.min(count, items.length);
    const hasMore = visible < items.length;
    const button = hasMore || keepPager
      ? `<button type="button" class="cw-dash-see-more" data-cw-widget-more="${esc(kind)}" aria-label="${hasMore ? `Show more ${esc(kind)} items` : `All ${esc(kind)} items shown`}"${hasMore ? "" : " disabled"}>
          <span class="material-symbols-rounded">expand_more</span>
        </button>`
      : "";
    host.innerHTML = `${items.slice(0, visible).map(cardFn).join("")}${button}`;
  }

  function applyVisibility(settings) {
    const card = $("#dashboard-widgets-card");
    const history = $("#recent-history-widget");
    const ratings = $("#latest-ratings-widget");
    const scrobble = $("#recent-scrobble-widget");
    if (history) history.classList.toggle("hidden", !settings.history);
    if (ratings) ratings.classList.toggle("hidden", !settings.ratings);
    if (scrobble) scrobble.classList.toggle("hidden", !settings.scrobble);
    if (card) card.classList.toggle("hidden", !settings.history && !settings.ratings && !settings.scrobble);
  }

  async function refreshDashboardWidgets({ forceConfig = false, force = false, preserve = false } = {}) {
    if (!isOnMain()) {
      hideDashboardWidgets();
      return;
    }
    revealCachedWidgets();
    const fresh = hasLoaded && (Date.now() - lastLoadedAt) < WIDGET_REFRESH_TTL_MS;
    if (!force && !widgetsDirty && fresh) return;
    if (authSetupPending()) {
      scheduleAuthReadyRefresh();
      return;
    }
    if (forceConfig) cfgPromise = null;
    const seq = ++loadSeq;
    const refreshVersion = dirtyVersion;
    let cfg;
    try {
      cfg = await getConfig(forceConfig);
    } catch (e) {
      if (authPendingError(e)) {
        scheduleAuthReadyRefresh();
        return;
      }
      if (preserve && hasLoaded) {
        revealCachedWidgets();
        return;
      }
      hideDashboardWidgets();
      return;
    }
    const settings = widgetSettings(cfg?.ui || cfg?.user_interface || {});
    lastSettings = settings;
    if (seq !== loadSeq || !isOnMain()) return;
    if (!preserve || !hasLoaded) {
      visibleCounts.history = PAGE_STEP;
      visibleCounts.ratings = RATING_PAGE_STEP;
      visibleCounts.scrobble = PAGE_STEP;
    }
    applyVisibility(settings);
    if (!settings.history && !settings.ratings && !settings.scrobble) return;
    if (!hasTmdbKeyInConfig(cfg)) {
      hideDashboardWidgets();
      return;
    }

    const historyHost = $("#recent-history-list");
    const ratingsHost = $("#latest-ratings-grid");
    const scrobbleHost = $("#recent-scrobble-list");
    if (!preserve || !hasLoaded) {
      if (settings.history) setLoading(historyHost);
      if (settings.ratings) setLoading(ratingsHost, "ratings");
      if (settings.scrobble) setLoading(scrobbleHost);
    }

    try {
      const data = await fetchJSON(`/api/dashboard/widgets?history_limit=${MAX_WIDGET_ITEMS}&ratings_limit=${MAX_WIDGET_ITEMS}&scrobble_limit=${MAX_WIDGET_ITEMS}`);
      if (seq !== loadSeq || !isOnMain()) return;
      if (!data?.ok) throw new Error(data?.error || "dashboard_widgets_failed");

      const historyItems = Array.isArray(data?.recent_history?.items) ? data.recent_history.items : [];
      const scrobbleItems = Array.isArray(data?.recent_scrobble?.items) ? data.recent_scrobble.items : [];
      const ratingItems = Array.isArray(data?.latest_ratings?.items) ? data.latest_ratings.items : [];
      setCountChip("recent-history-count-chip", data?.recent_history?.total ?? historyItems.length, "item");
      setCountChip("latest-ratings-count-chip", data?.latest_ratings?.total ?? ratingItems.length, "rating");
      setCountChip("recent-scrobble-count-chip", data?.recent_scrobble?.total ?? scrobbleItems.length, "scrobble");
      latestItems.history = historyItems;
      latestItems.ratings = ratingItems;
      latestItems.scrobble = scrobbleItems;
      hasLoaded = true;
      lastLoadedAt = Date.now();
      widgetsDirty = dirtyVersion !== refreshVersion;
      if (settings.history) {
        renderPagedList(historyHost, historyItems, visibleCounts.history, historyCard, "No watched history recorded yet.", "history");
      }
      if (settings.ratings) {
        renderPagedList(ratingsHost, ratingItems, visibleCounts.ratings, ratingCard, "No ratings recorded yet.", "ratings");
      }
      if (settings.scrobble) {
        renderPagedList(scrobbleHost, scrobbleItems, visibleCounts.scrobble, activityCard, "No recent scrobble recorded yet.", "scrobble", true);
      }
    } catch (e) {
      if (authPendingError(e)) {
        scheduleAuthReadyRefresh();
        return;
      }
      if (preserve && hasLoaded) return;
      if (settings.history) setEmpty(historyHost, "Recent history could not be loaded.");
      if (settings.ratings) setEmpty(ratingsHost, "Latest ratings could not be loaded.");
      if (settings.scrobble) setEmpty(scrobbleHost, "Recent scrobble could not be loaded.");
    }
  }

  function initDashboardWidgets() {
    $("#recent-history-refresh")?.addEventListener("click", () => markWidgetsDirty(0, { forceConfig: true }));
    $("#latest-ratings-refresh")?.addEventListener("click", () => markWidgetsDirty(0, { forceConfig: true }));
    $("#recent-scrobble-refresh")?.addEventListener("click", () => markWidgetsDirty(0, { forceConfig: true }));
    $("#dashboard-widgets-card")?.addEventListener("click", (event) => {
      const btn = event.target?.closest?.("[data-cw-widget-more]");
      if (!btn) return;
      const kind = String(btn.getAttribute("data-cw-widget-more") || "");
      if (kind !== "history" && kind !== "ratings" && kind !== "scrobble") return;
      const step = kind === "ratings" ? RATING_PAGE_STEP : PAGE_STEP;
      visibleCounts[kind] = Math.min((visibleCounts[kind] || step) + step, latestItems[kind].length);
      if (kind === "history") {
        renderPagedList($("#recent-history-list"), latestItems.history, visibleCounts.history, historyCard, "No watched history recorded yet.", "history");
      } else if (kind === "ratings") {
        renderPagedList($("#latest-ratings-grid"), latestItems.ratings, visibleCounts.ratings, ratingCard, "No ratings recorded yet.", "ratings");
      } else {
        renderPagedList($("#recent-scrobble-list"), latestItems.scrobble, visibleCounts.scrobble, activityCard, "No recent scrobble recorded yet.", "scrobble", true);
      }
    });
    document.addEventListener("tab-changed", (event) => {
      const id = event?.detail?.id || event?.detail?.tab;
      if (String(id || "").toLowerCase() === "main") {
        revealCachedWidgets();
        setTimeout(() => refreshDashboardWidgets({ preserve: hasLoaded }), 50);
      } else hideDashboardWidgets();
    });
    window.addEventListener("settings-changed", () => markWidgetsDirty(300, { forceConfig: true }));
    window.addEventListener("activity-log-cleared", () => markWidgetsDirty(100));
    window.addEventListener("sync-complete", () => markWidgetsDirty(250));
    window.addEventListener("cw:scrobble-stopped", scheduleScrobbleStopRefresh);
    window.addEventListener("cw:manual-watched-saved", () => markWidgetsDirty(250));
    window.addEventListener("watchlist:refresh", () => markWidgetsDirty(250));
    if (authSetupPending()) scheduleAuthReadyRefresh();
    window.addEventListener("load", () => setTimeout(() => refreshDashboardWidgets({ forceConfig: true }), 100), { once: true });
    refreshDashboardWidgets();
  }

  window.CW = window.CW || {};
  window.CW.DashboardWidgets = { refresh: refreshDashboardWidgets };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initDashboardWidgets, { once: true });
  } else {
    initDashboardWidgets();
  }
})();
