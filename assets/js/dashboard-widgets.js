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
  const PAGE_STEP = 6;
  const RATING_PAGE_STEP = 9;
  const MAX_WIDGET_ITEMS = 24;
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

  async function fetchJSON(url) {
    if (authSetupPending()) throw new Error("auth setup pending");
    if (window.CW?.API?.j) return window.CW.API.j(url);
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  async function getConfig() {
    if (cfgPromise) return cfgPromise;
    cfgPromise = (async () => {
      try {
        if (window.CW?.API?.Config?.load) return window.CW.API.Config.load(false);
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

  async function readSettings() {
    try {
      const cfg = await getConfig();
      return widgetSettings(cfg?.ui || cfg?.user_interface || {});
    } catch (e) {
      if (authPendingError(e)) return { history: true, ratings: true, scrobble: true };
      return { history: true, ratings: true, scrobble: true };
    }
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

  function countLabel(total, noun) {
    const n = Number(total || 0);
    const label = n === 1 ? noun : `${noun}s`;
    return `${Number.isFinite(n) ? n : 0} ${label}`;
  }

  function setCountChip(id, total, noun) {
    const chip = $(`#${id}`);
    if (!chip) return;
    chip.textContent = countLabel(total, noun);
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
    const route = sourceRouteTitle(item?.sources);
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
        <span class="cw-history-sources" title="${esc(route)}" aria-label="${esc(route || "Sources")}">${sourceIcons(item?.sources, 3)}</span>
      </${tag}>`;
  }

  function ratingCard(item) {
    const title = item?.title || "Untitled";
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const route = sourceRouteTitle(item?.sources);
    return `
      <${tag} class="cw-rating-widget-card"${hrefAttr} title="${esc(title)}">
        <img src="${esc(poster(item, "w342"))}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        <span class="cw-rating-score">${esc(item?.rating || "")}</span>
        <span class="cw-rating-overlay">
          <span class="cw-rating-sources" title="${esc(route)}" aria-label="${esc(route || "Sources")}">${sourceIcons(item?.sources, 3)}</span>
        </span>
      </${tag}>`;
  }

  function setEmpty(host, text) {
    if (host) host.innerHTML = `<div class="cw-dash-empty">${esc(text)}</div>`;
  }

  function setLoading(host) {
    if (host) host.innerHTML = `<div class="cw-dash-empty">Loading...</div>`;
  }

  function renderPagedList(host, items, count, cardFn, emptyText, kind) {
    if (!host) return;
    if (!items.length) {
      setEmpty(host, emptyText);
      return;
    }
    const visible = Math.min(count, items.length);
    const hasMore = visible < items.length;
    const button = hasMore
      ? `<button type="button" class="cw-dash-see-more" data-cw-widget-more="${esc(kind)}" aria-label="Show more ${esc(kind)} items">
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

  async function refreshDashboardWidgets({ forceConfig = false } = {}) {
    if (!isOnMain()) {
      $("#dashboard-widgets-card")?.classList.add("hidden");
      return;
    }
    if (authSetupPending()) {
      scheduleAuthReadyRefresh();
      return;
    }
    if (forceConfig) cfgPromise = null;
    const seq = ++loadSeq;
    const settings = await readSettings();
    if (seq !== loadSeq || !isOnMain()) return;
    visibleCounts.history = PAGE_STEP;
    visibleCounts.ratings = RATING_PAGE_STEP;
    visibleCounts.scrobble = PAGE_STEP;
    applyVisibility(settings);
    if (!settings.history && !settings.ratings && !settings.scrobble) return;

    const historyHost = $("#recent-history-list");
    const ratingsHost = $("#latest-ratings-grid");
    const scrobbleHost = $("#recent-scrobble-list");
    if (settings.history) setLoading(historyHost);
    if (settings.ratings) setLoading(ratingsHost);
    if (settings.scrobble) setLoading(scrobbleHost);

    try {
      const data = await fetchJSON(`/api/dashboard/widgets?history_limit=${MAX_WIDGET_ITEMS}&ratings_limit=${MAX_WIDGET_ITEMS}&scrobble_limit=${MAX_WIDGET_ITEMS}`);
      if (seq !== loadSeq || !isOnMain()) return;
      if (!data?.ok) throw new Error(data?.error || "dashboard_widgets_failed");

      const historyItems = Array.isArray(data?.recent_history?.items) ? data.recent_history.items : [];
      const scrobbleItems = Array.isArray(data?.recent_scrobble?.items) ? data.recent_scrobble.items : [];
      const ratingItems = Array.isArray(data?.latest_ratings?.items) ? data.latest_ratings.items : [];
      setCountChip("recent-history-count-chip", data?.recent_history?.total ?? historyItems.length, "item");
      setCountChip("latest-ratings-count-chip", data?.latest_ratings?.total ?? ratingItems.length, "rating");
      latestItems.history = historyItems;
      latestItems.ratings = ratingItems;
      latestItems.scrobble = scrobbleItems;
      if (settings.history) {
        renderPagedList(historyHost, historyItems, visibleCounts.history, historyCard, "No watched history recorded yet.", "history");
      }
      if (settings.ratings) {
        renderPagedList(ratingsHost, ratingItems, visibleCounts.ratings, ratingCard, "No ratings recorded yet.", "ratings");
      }
      if (settings.scrobble) {
        renderPagedList(scrobbleHost, scrobbleItems, visibleCounts.scrobble, activityCard, "No recent scrobble recorded yet.", "scrobble");
      }
    } catch (e) {
      if (authPendingError(e)) {
        scheduleAuthReadyRefresh();
        return;
      }
      if (settings.history) setEmpty(historyHost, "Recent history could not be loaded.");
      if (settings.ratings) setEmpty(ratingsHost, "Latest ratings could not be loaded.");
      if (settings.scrobble) setEmpty(scrobbleHost, "Recent scrobble could not be loaded.");
    }
  }

  function initDashboardWidgets() {
    $("#recent-history-refresh")?.addEventListener("click", () => refreshDashboardWidgets({ forceConfig: true }));
    $("#latest-ratings-refresh")?.addEventListener("click", () => refreshDashboardWidgets({ forceConfig: true }));
    $("#recent-scrobble-refresh")?.addEventListener("click", () => refreshDashboardWidgets({ forceConfig: true }));
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
        renderPagedList($("#recent-scrobble-list"), latestItems.scrobble, visibleCounts.scrobble, activityCard, "No recent scrobble recorded yet.", "scrobble");
      }
    });
    document.addEventListener("tab-changed", (event) => {
      const id = event?.detail?.id || event?.detail?.tab;
      if (String(id || "").toLowerCase() === "main") setTimeout(() => refreshDashboardWidgets(), 50);
      else $("#dashboard-widgets-card")?.classList.add("hidden");
    });
    window.addEventListener("settings-changed", () => setTimeout(() => refreshDashboardWidgets({ forceConfig: true }), 300));
    window.addEventListener("activity-log-cleared", () => setTimeout(() => refreshDashboardWidgets(), 100));
    window.addEventListener("sync-complete", () => scheduleWidgetRefresh(250));
    window.addEventListener("cw:manual-watched-saved", () => scheduleWidgetRefresh(250));
    window.addEventListener("watchlist:refresh", () => scheduleWidgetRefresh(250));
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
