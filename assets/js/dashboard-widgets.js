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
  const PAGE_STEP = 9;
  const RATING_PAGE_STEP = 9;
  const MEDIA_PAGE_STEP = 4;
  const GRID_PAGE_STEP = 6;
  const MAX_WIDGET_ITEMS = 24;
  const WIDGET_REFRESH_TTL_MS = 60 * 1000;
  const WIDGET_FETCH_RETRY_DELAYS = [350, 900, 1800];
  const IMAGE_PREWARM_MAX = 12;
  const visibleCounts = { history: GRID_PAGE_STEP, ratings: RATING_PAGE_STEP, scrobble: GRID_PAGE_STEP, progress: GRID_PAGE_STEP, playlists: GRID_PAGE_STEP };
  const latestItems = { history: [], ratings: [], scrobble: [], progress: [], playlists: [] };
  const EMPTY_META = {
    history: { title: "No history yet", copy: "Watched items will appear here." },
    ratings: { title: "No ratings yet", copy: "Your ratings and scores will appear here." },
    scrobble: { title: "No scrobbles yet", copy: "Recent scrobbles from your providers will appear here." },
    progress: { title: "No progress syncs yet", copy: "Resume position sync activity will appear here." },
    playlists: { title: "No playlist syncs yet", copy: "Recent playlist sync activity will appear here." },
    watchlist: { title: "No watchlist items yet", copy: "Synced watchlist titles will appear here." },
    error: { title: "Could not load this widget", copy: "Try refreshing again in a moment." },
  };
  const LAYOUT_KEY = "cw.dashboardWidgets.layout.v3";
  const WIDGETS = [
    { key: "watchlist", id: "placeholder-card", label: "Watchlist" },
    { key: "history", id: "recent-history-widget", label: "Recent History" },
    { key: "ratings", id: "latest-ratings-widget", label: "Latest Ratings" },
    { key: "scrobble", id: "recent-scrobble-widget", label: "Recent Scrobble" },
    { key: "progress", id: "recent-progress-widget", label: "Recent Progress" },
    { key: "playlists", id: "recent-playlists-widget", label: "Recent Playlists" },
  ];
  const WIDGET_KEYS = WIDGETS.map((widget) => widget.key);
  const DEFAULT_LAYOUT = {
    watchlist: { order: 0, size: "large", view: "icon", horizontalView: "media", hidden: false },
    history: { order: 1, size: "small", view: "grid", horizontalView: "media", hidden: false },
    ratings: { order: 2, size: "small", view: "icon", horizontalView: "media", hidden: false },
    scrobble: { order: 3, size: "small", view: "grid", horizontalView: "media", hidden: false },
    playlists: { order: 4, size: "small", view: "grid", horizontalView: "poster", hidden: false },
    progress: { order: 5, size: "small", view: "grid", horizontalView: "media", hidden: false },
  };
  let widgetLayout = readLayout();
  let customizeOpen = false;
  let dragWidgetKey = "";
  let dragScrollFrame = 0;
  let dragScrollDelta = 0;
  let masonryFrame = 0;
  let detailCard = null;
  let detailItem = null;
  let detailMeta = null;
  let detailKey = "";

  function authPendingError(e) {
    return String(e?.message || e || "").includes("auth setup pending");
  }

  function normalizeLayout(raw) {
    const out = {};
    for (const widget of WIDGETS) {
      const base = DEFAULT_LAYOUT[widget.key];
      const row = raw?.[widget.key] && typeof raw[widget.key] === "object" ? raw[widget.key] : {};
      const allowedSizes = ["small", "large"];
      out[widget.key] = {
        order: Number.isFinite(+row.order) ? +row.order : base.order,
        size: allowedSizes.includes(row.size) ? row.size : base.size,
        view: ["grid", "icon"].includes(row.view) ? row.view : base.view,
        horizontalView: ["media", "poster"].includes(row.horizontalView) ? row.horizontalView : base.horizontalView,
        hidden: row.hidden === true,
      };
    }
    return Object.fromEntries(
      Object.entries(out)
        .sort((a, b) => a[1].order - b[1].order || WIDGET_KEYS.indexOf(a[0]) - WIDGET_KEYS.indexOf(b[0]))
        .map(([key, row], order) => [key, { ...row, order }])
    );
  }

  function readLayout() {
    try { return normalizeLayout(JSON.parse(localStorage.getItem(LAYOUT_KEY) || "null")); }
    catch { return normalizeLayout(null); }
  }

  function saveLayout(nextLayout) {
    widgetLayout = normalizeLayout(nextLayout);
    try { localStorage.setItem(LAYOUT_KEY, JSON.stringify(widgetLayout)); } catch {}
    window.dispatchEvent(new CustomEvent("cw:dashboard-widgets-layout-changed"));
    applyVisibility(lastSettings || {});
  }

  function resetLayout() {
    widgetLayout = normalizeLayout(null);
    try { localStorage.removeItem(LAYOUT_KEY); } catch {}
    window.dispatchEvent(new CustomEvent("cw:dashboard-widgets-layout-changed"));
    applyVisibility(lastSettings || {});
  }

  function patchLayout(key, patch) {
    if (!widgetLayout[key]) return;
    saveLayout({ ...widgetLayout, [key]: { ...widgetLayout[key], ...patch } });
  }

  function widgetNode(key) {
    const id = WIDGETS.find((widget) => widget.key === key)?.id;
    return id ? document.getElementById(id) : null;
  }

  function orderedWidgets(includeHidden = true) {
    return WIDGETS
      .map((widget) => ({ ...widget, layout: widgetLayout[widget.key] || DEFAULT_LAYOUT[widget.key] }))
      .filter((widget) => includeHidden || !widget.layout.hidden)
      .sort((a, b) => a.layout.order - b.layout.order);
  }

  function moveWidgetTo(sourceKey, targetKey, after = false) {
    if (!sourceKey || !targetKey || sourceKey === targetKey || !widgetLayout[sourceKey] || !widgetLayout[targetKey]) return;
    const keys = orderedWidgets(true).map((widget) => widget.key).filter((key) => key !== sourceKey);
    const targetIdx = keys.indexOf(targetKey);
    keys.splice(targetIdx < 0 ? keys.length : targetIdx + (after ? 1 : 0), 0, sourceKey);
    saveLayout(Object.fromEntries(keys.map((key, order) => [key, { ...widgetLayout[key], order }])));
  }

  function watchlistPosterCount() {
    const count = document.querySelectorAll("#poster-row .poster").length;
    if (count) return count;
    const chip = document.getElementById("watchlist-count-chip");
    const value = Number(chip?.textContent || 0);
    return Number.isFinite(value) ? value : 0;
  }

  function effectiveSize(key) {
    const size = widgetLayout[key]?.size || DEFAULT_LAYOUT[key]?.size || "small";
    return size === "large" ? "large" : "small";
  }

  function widgetView(key) {
    if (effectiveSize(key) === "large") {
      if (key === "playlists") return "icon";
      const horizontalView = widgetLayout[key]?.horizontalView || DEFAULT_LAYOUT[key]?.horizontalView || "media";
      return horizontalView === "poster" ? "icon" : "media";
    }
    const view = widgetLayout[key]?.view || DEFAULT_LAYOUT[key]?.view || "grid";
    return view === "icon" ? "icon" : "grid";
  }

  function widgetPageStep(key) {
    if (effectiveSize(key) === "large") return widgetView(key) === "media" ? MEDIA_PAGE_STEP : (key === "ratings" ? RATING_PAGE_STEP : PAGE_STEP);
    return widgetView(key) === "grid" ? GRID_PAGE_STEP : (key === "ratings" ? RATING_PAGE_STEP : PAGE_STEP);
  }

  function resetVisibleCount(key) {
    if (!visibleCounts.hasOwnProperty(key)) return;
    visibleCounts[key] = widgetPageStep(key);
  }

  function isWidgetEmpty(key) {
    if (key === "watchlist") {
      return !watchlistPosterCount() || document.getElementById("wall-msg")?.classList.contains("is-empty");
    }
    return !!widgetNode(key)?.querySelector(".cw-dash-empty");
  }

  function activeWidgetSettings(settings) {
    return Object.fromEntries(WIDGET_KEYS.map((key) => {
      const enabled = settings?.[key] !== false;
      const hidden = !!widgetLayout[key]?.hidden;
      return [key, enabled && !hidden];
    }));
  }

  function setWidgetEmpty(key, empty) {
    const node = widgetNode(key);
    if (!node) return;
    node.classList.toggle("is-empty-widget", !!empty);
    node.classList.toggle("is-auto-collapsed", !!empty && !customizeOpen);
    scheduleMasonry();
  }

  function updateMasonry() {
    masonryFrame = 0;
    const card = $("#dashboard-widgets-card");
    if (!card || card.classList.contains("hidden")) return;
    const styles = getComputedStyle(card);
    const row = Number.parseFloat(styles.getPropertyValue("--cw-widget-masonry-row")) || 8;
    const gap = Number.parseFloat(styles.rowGap || styles.gap) || 14;
    const singleColumn = window.matchMedia?.("(max-width: 1180px)")?.matches;
    for (const widget of WIDGETS) {
      const node = widgetNode(widget.key);
      if (!node) continue;
      if (singleColumn || node.classList.contains("hidden")) {
        node.style.gridRowEnd = "";
        continue;
      }
      node.style.gridRowEnd = "span 1";
      const height = node.getBoundingClientRect().height;
      node.style.gridRowEnd = `span ${Math.max(1, Math.ceil((height + gap) / (row + gap)))}`;
    }
  }

  function scheduleMasonry() {
    if (masonryFrame) return;
    masonryFrame = window.requestAnimationFrame(() => {
      updateMasonry();
      window.setTimeout(updateMasonry, 80);
    });
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

  async function fetchWidgetPayload(url) {
    let lastError = null;
    for (let attempt = 0; attempt <= WIDGET_FETCH_RETRY_DELAYS.length; attempt += 1) {
      try {
        const data = await fetchJSON(url);
        if (!data?.ok) throw new Error(data?.error || "dashboard_widgets_failed");
        return data;
      } catch (e) {
        if (authPendingError(e)) throw e;
        lastError = e;
        const delay = WIDGET_FETCH_RETRY_DELAYS[attempt];
        if (!delay) break;
        await new Promise((resolve) => window.setTimeout(resolve, delay));
      }
    }
    throw lastError || new Error("dashboard_widgets_failed");
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
      watchlist: typeof ui?.show_watchlist_preview === "boolean" ? !!ui.show_watchlist_preview : true,
      history: typeof ui?.show_recent_history_widget === "boolean" ? !!ui.show_recent_history_widget : true,
      ratings: typeof ui?.show_latest_ratings_widget === "boolean" ? !!ui.show_latest_ratings_widget : true,
      scrobble: typeof ui?.show_recent_scrobble_widget === "boolean" ? !!ui.show_recent_scrobble_widget : true,
      progress: typeof ui?.show_recent_progress_widget === "boolean" ? !!ui.show_recent_progress_widget : false,
      playlists: typeof ui?.show_recent_playlists_widget === "boolean" ? !!ui.show_recent_playlists_widget : false,
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

  function updateLayoutToolbar() {
    const card = $("#dashboard-widgets-card");
    card?.classList.toggle("is-customizing", customizeOpen);
  }

  function ensureLayoutToolbar() {
    const card = $("#dashboard-widgets-card");
    card?.querySelector(".cw-dashboard-layout-toolbar")?.remove();
    updateLayoutToolbar();
  }

  function showAllWidgets() {
    saveLayout(Object.fromEntries(WIDGET_KEYS.map((key) => [key, { ...widgetLayout[key], hidden: false }])));
    markWidgetsDirty(0);
  }

  function hiddenWidgetCount() {
    return WIDGET_KEYS.filter((key) => widgetLayout[key]?.hidden).length;
  }

  function createWidgetControls(key, label) {
    const controls = document.createElement("div");
    controls.className = "cw-dash-layout-controls";
    controls.innerHTML = `
      <button type="button" class="cw-dash-layout-control cw-dash-widget-drag" draggable="true" data-cw-widget-key="${esc(key)}" title="Drag ${esc(label)}" aria-label="Drag ${esc(label)}">
        <span class="material-symbols-rounded" aria-hidden="true">drag_indicator</span>
      </button>
      <button type="button" class="cw-dash-layout-control" data-cw-widget-action="size" data-cw-widget-key="${esc(key)}" title="Switch ${esc(label)} mode" aria-label="Switch ${esc(label)} mode">
        <span class="material-symbols-rounded" aria-hidden="true">view_agenda</span>
      </button>
      <button type="button" class="cw-dash-layout-control cw-dash-widget-view-toggle" data-cw-widget-action="view" data-cw-widget-key="${esc(key)}" title="Toggle ${esc(label)} view" aria-label="Toggle ${esc(label)} view">
        <span class="material-symbols-rounded" aria-hidden="true">grid_view</span>
      </button>
      <button type="button" class="cw-dash-layout-control" data-cw-widget-action="hide" data-cw-widget-key="${esc(key)}" title="Hide ${esc(label)}" aria-label="Hide ${esc(label)}">
        <span class="material-symbols-rounded" aria-hidden="true">visibility_off</span>
      </button>`;
    const drag = controls.querySelector(".cw-dash-widget-drag");
    drag?.addEventListener("dragstart", (ev) => {
      dragWidgetKey = key;
      ev.dataTransfer.effectAllowed = "move";
      ev.dataTransfer.setData("text/plain", key);
      widgetNode(key)?.classList.add("is-dragging");
    });
    drag?.addEventListener("dragend", () => {
      dragWidgetKey = "";
      stopDashboardDragScroll();
      clearDashboardDropTargets();
    });
    return controls;
  }

  function dashboardDropTopGuard() {
    const headerBottom = document.querySelector("header")?.getBoundingClientRect?.().bottom || 0;
    return Math.max(72, headerBottom + 18);
  }

  function dashboardDropAfterForNode(ev, node) {
    const rect = node.getBoundingClientRect();
    const visibleTop = Math.max(rect.top, dashboardDropTopGuard());
    const visibleHeight = Math.max(24, rect.bottom - visibleTop);
    return ev.clientY > visibleTop + visibleHeight / 2;
  }

  function runDashboardDragScroll() {
    if (!dragWidgetKey || !dragScrollDelta) {
      dragScrollFrame = 0;
      return;
    }
    window.scrollBy(0, dragScrollDelta);
    dragScrollFrame = window.requestAnimationFrame(runDashboardDragScroll);
  }

  function updateDashboardDragScroll(ev) {
    if (!dragWidgetKey) return;
    const topZone = dashboardDropTopGuard() + 34;
    const bottomZone = window.innerHeight - 76;
    let nextDelta = 0;
    if (ev.clientY < topZone) nextDelta = -Math.min(34, Math.max(10, Math.round((topZone - ev.clientY) / 2)));
    else if (ev.clientY > bottomZone) nextDelta = Math.min(34, Math.max(10, Math.round((ev.clientY - bottomZone) / 2)));
    dragScrollDelta = nextDelta;
    if (dragScrollDelta && !dragScrollFrame) dragScrollFrame = window.requestAnimationFrame(runDashboardDragScroll);
  }

  function stopDashboardDragScroll() {
    dragScrollDelta = 0;
    if (dragScrollFrame) window.cancelAnimationFrame(dragScrollFrame);
    dragScrollFrame = 0;
  }

  function clearDashboardDropTargets() {
    document.querySelectorAll(".cw-dash-widget").forEach((node) => {
      node.classList.remove("is-dragging", "is-drop-target");
      delete node.dataset.dropPosition;
    });
  }

  function markDashboardDropTarget(targetKey, after = false) {
    document.querySelectorAll(".cw-dash-widget.is-drop-target").forEach((node) => {
      if (node.dataset.widgetKey === targetKey) return;
      node.classList.remove("is-drop-target");
      delete node.dataset.dropPosition;
    });
    const node = widgetNode(targetKey);
    if (!node) return;
    node.dataset.dropPosition = after ? "after" : "before";
    node.classList.add("is-drop-target");
  }

  function resolveDashboardDrop(ev) {
    const nodes = orderedWidgets(false)
      .map((widget) => widgetNode(widget.key))
      .filter((node) => node && node.dataset.widgetKey !== dragWidgetKey && !node.classList.contains("hidden"));
    if (!nodes.length) return null;
    const pointerY = ev.clientY;
    const topGuard = dashboardDropTopGuard();
    for (const node of nodes) {
      const rect = node.getBoundingClientRect();
      const visibleTop = Math.max(rect.top, topGuard);
      const visibleHeight = Math.max(24, rect.bottom - visibleTop);
      if (pointerY < visibleTop + visibleHeight / 2) {
        return { targetKey: node.dataset.widgetKey, after: false };
      }
    }
    return { targetKey: nodes[nodes.length - 1].dataset.widgetKey, after: true };
  }

  function ensureWidgetControls() {
    const card = $("#dashboard-widgets-card");
    if (card && !card.__cwDashboardDropWired) {
      card.addEventListener("dragover", (ev) => {
        if (dragWidgetKey) updateDashboardDragScroll(ev);
        if (!dragWidgetKey || ev.target?.closest?.(".cw-dash-widget")) return;
        const drop = resolveDashboardDrop(ev);
        if (!drop) return;
        ev.preventDefault();
        ev.dataTransfer.dropEffect = "move";
        markDashboardDropTarget(drop.targetKey, drop.after);
      });
      card.addEventListener("dragleave", (ev) => {
        if (card.contains(ev.relatedTarget)) return;
        clearDashboardDropTargets();
      });
      card.addEventListener("drop", (ev) => {
        if (!dragWidgetKey || ev.target?.closest?.(".cw-dash-widget")) return;
        const drop = resolveDashboardDrop(ev);
        if (!drop) return;
        ev.preventDefault();
        clearDashboardDropTargets();
        moveWidgetTo(dragWidgetKey || ev.dataTransfer.getData("text/plain"), drop.targetKey, drop.after);
      });
      card.__cwDashboardDropWired = true;
    }
    for (const widget of WIDGETS) {
      const node = widgetNode(widget.key);
      if (!node) continue;
      node.dataset.widgetKey = widget.key;
      if (!node.querySelector(".cw-dash-layout-controls")) {
        node.appendChild(createWidgetControls(widget.key, widget.label));
      }
      if (!node.__cwDashboardDropWired) {
        node.addEventListener("dragover", (ev) => {
          if (!dragWidgetKey || dragWidgetKey === widget.key) return;
          ev.preventDefault();
          updateDashboardDragScroll(ev);
          ev.dataTransfer.dropEffect = "move";
          const after = dashboardDropAfterForNode(ev, node);
          markDashboardDropTarget(widget.key, after);
        });
        node.addEventListener("dragleave", () => {
          node.classList.remove("is-drop-target");
          delete node.dataset.dropPosition;
        });
        node.addEventListener("drop", (ev) => {
          ev.preventDefault();
          const after = node.dataset.dropPosition === "after";
          node.classList.remove("is-drop-target");
          delete node.dataset.dropPosition;
          moveWidgetTo(dragWidgetKey || ev.dataTransfer.getData("text/plain"), widget.key, after);
        });
        node.__cwDashboardDropWired = true;
      }
    }
  }

  function syncControlIcons() {
    for (const widget of WIDGETS) {
      const node = widgetNode(widget.key);
      const sizeBtn = node?.querySelector("[data-cw-widget-action='size'] .material-symbols-rounded");
      const viewBtn = node?.querySelector("[data-cw-widget-action='view']");
      const viewIcon = viewBtn?.querySelector(".material-symbols-rounded");
      const hideBtn = node?.querySelector("[data-cw-widget-action='hide']");
      const hideIcon = hideBtn?.querySelector(".material-symbols-rounded");
      const size = effectiveSize(widget.key);
      if (sizeBtn) {
        sizeBtn.textContent = size === "large" ? "view_compact" : "view_agenda";
        const sizeButton = sizeBtn.closest("button");
        if (sizeButton) {
          sizeButton.title = `Switch ${widget.label} to ${size === "large" ? "compact" : "horizontal"} mode`;
          sizeButton.setAttribute("aria-label", sizeButton.title);
        }
      }
      if (viewBtn && viewIcon) {
        const hideViewToggle = size === "large" && widget.key === "playlists";
        viewBtn.hidden = hideViewToggle;
        if (!hideViewToggle) {
          const view = widgetView(widget.key);
          if (size === "large") {
            viewIcon.textContent = view === "media" ? "view_module" : "view_carousel";
            viewBtn.title = `Switch ${widget.label} to ${view === "media" ? "poster" : "media card"} view`;
          } else {
            viewIcon.textContent = view === "icon" ? "view_list" : "grid_view";
            viewBtn.title = `Switch ${widget.label} to ${view === "icon" ? "grid" : "poster"} view`;
          }
          viewBtn.setAttribute("aria-label", viewBtn.title);
        }
      }
      if (hideBtn && hideIcon) {
        const hidden = !!widgetLayout[widget.key]?.hidden;
        hideIcon.textContent = hidden ? "visibility" : "visibility_off";
        hideBtn.title = `${hidden ? "Show" : "Hide"} ${widget.label}`;
        hideBtn.setAttribute("aria-label", hideBtn.title);
      }
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

  function widgetItemAttrs(kind, index) {
    return ` data-cw-widget-item="${esc(kind)}" data-cw-widget-index="${esc(index)}"`;
  }

  function ratingSourceIcons(sources, max = 3) {
    return sourceRows(sources, max).map(({ provider, instance }) => {
      const logo = providerLogo(provider);
      const label = sourceLabel({ provider, instance });
      return logo
        ? `<span class="cw-rating-provider-icon" title="${esc(label)}" aria-label="${esc(label)}"><img src="${esc(logo)}" alt=""></span>`
        : `<span class="cw-rating-provider-icon cw-rating-provider-icon--text" title="${esc(label)}" aria-label="${esc(label)}">${esc(providerShort(provider).slice(0, 3))}</span>`;
    }).join("");
  }

  function scrobbleProfileLabel(instance) {
    const raw = String(instance || "default").trim() || "default";
    if (raw.toLowerCase() === "default") return "Default";
    const profile = raw.toUpperCase().match(/(?:^|[^A-Z0-9])(P\d{1,3})$/);
    return profile?.[1] || raw;
  }

  function scrobbleSourcePill(source) {
    const provider = String(source?.provider || "").trim().toUpperCase();
    if (!provider) return "";
    const instance = String(source?.instance || "default").trim() || "default";
    const profile = scrobbleProfileLabel(instance);
    const providerName = providerLabel(provider);
    const logo = providerMeta().logLogoPath?.(provider) || providerLogo(provider);
    const providerIcon = logo
      ? `<img src="${esc(logo)}" alt="" aria-hidden="true">`
      : `<span class="cw-scrobble-source-pill-text" aria-hidden="true">${esc(providerShort(provider).slice(0, 2))}</span>`;
    const tone = providerMeta().tone?.(provider) || {};
    const rgb = String(tone?.rgb || "124,92,255");
    const label = providerShort(provider);
    const description = `${providerName} source profile: ${profile}`;
    return `<span class="cw-scrobble-source-pill" style="--cw-source-rgb:${esc(rgb)}" title="${esc(description)}" aria-label="${esc(description)}">${providerIcon}<span>${esc(label)}</span></span>`;
  }

  function scrobbleSourceRow(item) {
    return item?.source || sourceRows(item?.sources, 1)[0] || null;
  }

  function scrobbleRouteRows(item) {
    const rows = [];
    const seen = new Set();
    const add = (row) => {
      const provider = String(row?.provider || "").trim().toUpperCase();
      const instance = String(row?.instance || "default").trim() || "default";
      const key = `${provider}:${instance}`;
      if (!provider || seen.has(key)) return;
      seen.add(key);
      rows.push({ provider, instance });
    };
    add(scrobbleSourceRow(item));
    sourceRows(item?.targets, 8)
      .filter(({ provider }) => providerMeta().get?.(provider)?.scrobblerSink === true)
      .forEach(add);
    return rows;
  }

  function scrobbleRouteIcons(item) {
    const rows = scrobbleRouteRows(item);
    const route = rows.length ? `Route: ${rows.map(sourceLabel).join(" -> ")}` : "No scrobble route";
    return {
      html: ratingSourceIcons(rows, 8),
      route,
    };
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
    if (String(item?.art_type || "").toLowerCase() === "movie") return "Movie";
    if (raw === "show") return "Show";
    return "Movie";
  }

  function mediaLabel(item, meta = null) {
    const resolved = String(meta?.resolved_type || "").toLowerCase();
    if (resolved === "movie") return "Movie";
    if (resolved === "tv") return String(item?.type || "").toLowerCase() === "episode" ? (item?.episode_label || "Episode") : "Show";
    return typeLabel(item);
  }

  function yearFromIso(value) {
    const raw = String(value || "");
    return /^\d{4}/.test(raw) ? raw.slice(0, 4) : "";
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
    const isEpisode = String(item?.type || "").toLowerCase() === "episode";
    const title = item?.title && !isEpisode ? `&title=${encodeURIComponent(String(item.title))}` : "";
    const year = item?.year && !isEpisode ? `&year=${encodeURIComponent(String(item.year))}` : "";
    return `/art/tmdb/${kind}/${encodeURIComponent(String(tmdb))}?size=${encodeURIComponent(size)}${title}${year}`;
  }

  function coverPoster(item, size = "w342") {
    const src = String(item?.cover || item?.poster_cover || "");
    if (src) return src;
    const tmdb = item?.tmdb;
    if (!tmdb) return poster(item, size);
    const kind = String(item?.art_type || item?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    const title = item?.title ? `&title=${encodeURIComponent(String(item.title))}` : "";
    const year = item?.year ? `&year=${encodeURIComponent(String(item.year))}` : "";
    return `/art/tmdb/${kind}/${encodeURIComponent(String(tmdb))}?size=${encodeURIComponent(size)}${title}${year}`;
  }

  function backdropPoster(item, meta = null, size = "w1280") {
    const tmdb = item?.tmdb || item?.ids?.tmdb || meta?.ids?.tmdb;
    if (!tmdb) return "";
    const resolved = String(meta?.resolved_type || "").toLowerCase();
    const kind = resolved ? (resolved === "movie" ? "movie" : "tv") : (String(item?.art_type || item?.type || "").toLowerCase() === "movie" ? "movie" : "tv");
    const title = item?.title ? `&title=${encodeURIComponent(String(item.title))}` : "";
    const year = item?.year ? `&year=${encodeURIComponent(String(item.year))}` : "";
    return `/art/tmdb/${kind}/${encodeURIComponent(String(tmdb))}?kind=backdrop&size=${encodeURIComponent(size)}&locale=${encodeURIComponent(window.__CW_LOCALE || navigator.language || "en-US")}${title}${year}`;
  }

  function prewarmImageUrls(urls) {
    const list = [...new Set((urls || []).filter(Boolean))].slice(0, IMAGE_PREWARM_MAX);
    if (!list.length) return;
    if (navigator.connection?.saveData) return;
    const run = () => {
      let index = 0;
      const next = () => {
        const url = list[index++];
        if (!url) return;
        let settled = false;
        const img = new Image();
        const done = () => {
          if (settled) return;
          settled = true;
          window.setTimeout(next, 160);
        };
        img.onload = done;
        img.onerror = done;
        img.decoding = "async";
        img.src = url;
        window.setTimeout(done, 3500);
      };
      next();
    };
    const start = () => {
      if ("requestIdleCallback" in window) window.requestIdleCallback(run, { timeout: 4000 });
      else window.setTimeout(run, 800);
    };
    window.setTimeout(start, 900);
  }

  function prewarmWidgetImages(active) {
    const urls = [];
    for (const kind of ["history", "ratings", "scrobble", "progress"]) {
      if (!active?.[kind]) continue;
      const limit = Math.min(latestItems[kind]?.length || 0, visibleCounts[kind] || PAGE_STEP);
      for (const item of (latestItems[kind] || []).slice(0, limit)) {
        urls.push(poster(item), coverPoster(item, "w342"), backdropPoster(item, null, "w780"));
      }
    }
    prewarmImageUrls(urls);
  }

  function tmdbLink(item) {
    const tmdb = item?.tmdb;
    if (!tmdb) return "";
    const kind = String(item?.art_type || item?.type || "").toLowerCase() === "movie" ? "movie" : "tv";
    return `https://www.themoviedb.org/${kind}/${encodeURIComponent(String(tmdb))}`;
  }

  function imdbLink(item, meta = null) {
    const raw = String(item?.ids?.imdb || meta?.ids?.imdb || meta?.external_ids?.imdb_id || "").trim();
    if (!raw) return "";
    const clean = raw.startsWith("tt") ? raw : `tt${raw}`;
    return `https://www.imdb.com/title/${encodeURIComponent(clean)}`;
  }

  async function getDetailMeta(item) {
    if (!item?.tmdb && !item?.ids?.tmdb) return null;
    return window.CW?.Meta?.get?.(item, "detail") || null;
  }

  function detailSources(item) {
    return sourceRows(item?.sources, 6).map(({ provider, instance }) => ({
      label: sourceLabel({ provider, instance }),
      short: providerShort(provider),
      logo: providerLogo(provider),
    }));
  }

  function detailModel(item, meta = null, loading = false) {
    const resolved = String(meta?.resolved_type || "").toLowerCase();
    const isMovie = resolved ? resolved === "movie" : String(item?.art_type || item?.type || "").toLowerCase() === "movie";
    const releaseIso = isMovie
      ? (meta?.detail?.release_date || meta?.release?.date || item?.release_date || "")
      : (meta?.detail?.first_air_date || meta?.release?.date || item?.first_air_date || "");
    const year = String(item?.year || meta?.year || yearFromIso(releaseIso) || "").trim();
    const rawScore = Number(meta?.score);
    const rawRating = Number(meta?.vote_average ?? meta?.detail?.vote_average);
    const ratingValue = Number.isFinite(rawRating) ? rawRating : Number.isFinite(rawScore) ? rawScore / 10 : null;
    return {
      title: item?.title || meta?.title || "Unknown title",
      year: "",
      isMovie,
      chips: [
        { text: mediaLabel(item, meta) },
        { text: year },
        { text: window.CW?.PlayingCard?.fmt?.runtimeLabel?.(meta?.runtime_minutes) || "" },
        { text: meta?.certification || meta?.release?.cert || meta?.detail?.certification || "" },
      ],
      overview: meta?.overview || meta?.detail?.overview || meta?.detail?.tagline || (loading ? "Loading details..." : "No description available."),
      poster: coverPoster(item, "w342") || "/assets/img/placeholder_poster.svg",
      posterHref: tmdbLink(item),
      backdrop: backdropPoster(item, meta),
      information: meta ? window.CW?.PlayingCard?.fmt?.informationFor?.(meta, isMovie) || "loading" : "loading",
      rating: { value: ratingValue, votes: meta?.vote_count ?? meta?.detail?.vote_count },
      progress: Number.isFinite(Number(item?.progress)) ? { pct: Number(item.progress) } : null,
      sources: detailSources(item),
      links: [
        { href: tmdbLink(item), text: "TMDb" },
        { href: imdbLink(item, meta), text: "IMDb" },
      ],
    };
  }

  function ensureDetailCard() {
    if (detailCard) return detailCard;
    if (!window.CW?.PlayingCard?.mount) return null;
    detailCard = window.CW.PlayingCard.mount({
      id: "cw-dashboard-widget-detail",
      variant: "watchlist",
      tabScope: "main",
      label: "Dashboard media item details",
      width: "min(860px,calc(100vw - 32px))",
      onClose: () => { detailKey = ""; detailCard?.hide(); },
      onTrailer: () => { void window.CW?.Trailer?.openFor?.(detailItem, detailMeta); },
    });
    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") return;
      if (document.getElementById("cw-trailer")?.classList.contains("show")) return;
      detailKey = "";
      detailCard?.hide();
    }, true);
    return detailCard;
  }

  function renderDetailCard(item, meta = null, loading = false) {
    const card = ensureDetailCard();
    if (!card) return;
    detailItem = item;
    detailMeta = meta;
    card.render(detailModel(item, meta, loading));
    card.show();
  }

  async function openDetailCard(kind, index) {
    const item = latestItems[kind]?.[Number(index)];
    if (!item || !["history", "ratings", "scrobble", "progress"].includes(kind)) return false;
    const key = `${kind}:${index}:${item?.key || item?.id || item?.tmdb || item?.title || ""}`;
    detailKey = key;
    renderDetailCard(item, null, true);
    const meta = await getDetailMeta(item);
    if (!detailCard?.isVisible?.() || detailKey !== key) return true;
    renderDetailCard(item, meta || null, false);
    return true;
  }

  function historyCard(item, index = 0) {
    const title = item?.title || "Untitled";
    const age = relTime(item?.sort_epoch || item?.watched_at);
    const meta = [typeLabel(item), item?.year || "", age].filter(Boolean).join(" - ");
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const art = poster(item);
    const artStyle = art ? ` style="--cw-history-art:url(&quot;${esc(art)}&quot;)"` : "";
    const route = sourceRouteTitle(item?.sources);
    return `
      <${tag} class="cw-history-widget-item"${hrefAttr}${artStyle}${widgetItemAttrs("history", index)}>
        <span class="cw-history-thumb">
          <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
          ${age ? `<span class="cw-rating-grid-age">${esc(age)}</span>` : ""}
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

  function activityMethodPill(item) {
    const label = activityLabel(item);
    const slug = label.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "activity";
    const icon = slug === "webhook" ? "webhook" : slug === "watcher" ? "radar" : "bolt";
    return `
      <span class="cw-history-method-pill cw-history-method-pill--${esc(slug)}">
        <span class="material-symbols-rounded" aria-hidden="true">${esc(icon)}</span>
        <span>${esc(label)}</span>
      </span>`;
  }

  function activityCard(item, index = 0) {
    const title = item?.title || "Untitled";
    const age = relTime(item?.sort_epoch || item?.captured_at || item?.watched_at);
    const meta = [activityLabel(item), typeLabel(item), age].filter(Boolean).join(" - ");
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const art = poster(item);
    const artStyle = art ? ` style="--cw-history-art:url(&quot;${esc(art)}&quot;)"` : "";
    const source = scrobbleSourceRow(item);
    const sinks = scrobbleSinkIcons(item?.targets);
    return `
      <${tag} class="cw-history-widget-item cw-history-widget-item--activity"${hrefAttr}${artStyle}${widgetItemAttrs("scrobble", index)}>
        <span class="cw-history-thumb">
          <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
          ${age ? `<span class="cw-rating-grid-age">${esc(age)}</span>` : ""}
          ${activityMethodPill(item)}
          ${item?.episode_label ? `<span class="cw-history-episode">${esc(item.episode_label)}</span>` : ""}
        </span>
        <span class="cw-history-copy">
          <strong>${esc(title)}</strong>
          <span>${esc(meta || "Activity")}</span>
        </span>
        <span class="cw-scrobble-route">
          ${scrobbleSourcePill(source)}
          <span class="cw-history-sources" title="${esc(sinks.route)}" aria-label="${esc(sinks.route)}">${sinks.html}</span>
        </span>
      </${tag}>`;
  }

  function progressCard(item, index = 0) {
    const title = item?.title || "Untitled";
    const progress = Number(item?.progress);
    const progressLabel = Number.isFinite(progress) ? `${Math.round(progress)}%` : "";
    const age = relTime(item?.sort_epoch);
    const meta = [typeLabel(item), progressLabel ? `${progressLabel} progress` : "Progress sync", age].filter(Boolean).join(" - ");
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const art = poster(item);
    const route = sourceRouteTitle(item?.sources);
    return `
      <${tag} class="cw-history-widget-item cw-history-widget-item--progress"${hrefAttr}${widgetItemAttrs("progress", index)}>
        <span class="cw-history-thumb">
          <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
          ${age ? `<span class="cw-rating-grid-age">${esc(age)}</span>` : ""}
          ${item?.episode_label ? `<span class="cw-history-episode">${esc(item.episode_label)}</span>` : ""}
        </span>
        <span class="cw-history-copy">
          <strong>${esc(title)}</strong>
          <span>${esc(meta || "Progress sync")}</span>
        </span>
        <span class="cw-history-sources" title="${esc(route)}" aria-label="${esc(route || "Sources")}">${sourceIcons(item?.sources, 3)}</span>
      </${tag}>`;
  }

  function playlistCard(item) {
    const title = item?.label || item?.title || "Playlist activity";
    const status = String(item?.status || "completed");
    const meta = [item?.type || "Sync", item?.details || "", relTime(item?.ts)].filter(Boolean).join(" - ");
    return `
      <div class="cw-history-widget-item cw-history-widget-item--plain cw-playlist-widget-item">
        <span class="cw-history-thumb cw-history-thumb--icon">
          <span class="material-symbols-rounded" aria-hidden="true">queue_music</span>
        </span>
        <span class="cw-history-copy">
          <strong>${esc(title)}</strong>
          <span>${esc(meta || "Playlist sync")}</span>
        </span>
        <span class="cw-dash-status-pill cw-dash-status-pill--${esc(status.toLowerCase())}">${esc(status)}</span>
      </div>`;
  }

  function playlistPosterCard(item) {
    const title = item?.label || item?.title || "Playlist activity";
    const status = String(item?.status || "completed").toUpperCase();
    const meta = [item?.type || "Sync", relTime(item?.ts)].filter(Boolean).join(" - ");
    return `
      <div class="cw-rating-widget-card cw-widget-poster-card cw-widget-poster-card--playlist" title="${esc([title, meta].filter(Boolean).join(" | "))}">
        <span class="cw-widget-poster-fallback material-symbols-rounded" aria-hidden="true">queue_music</span>
        <span class="cw-widget-poster-chip">${esc(status)}</span>
      </div>`;
  }

  function mediaPosterCard(item, kind = "history", index = 0) {
    const title = item?.title || "Untitled";
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const rawType = String(item?.type || "").toLowerCase();
    const progress = Number(item?.progress);
    const progressLabel = Number.isFinite(progress) ? `${Math.round(progress)}%` : "";
    const age = relTime(item?.sort_epoch || item?.captured_at || item?.watched_at || 0);
    const meta = kind === "progress"
      ? [progressLabel ? `${progressLabel} progress` : "Progress", age].filter(Boolean).join(" - ")
      : kind === "scrobble"
        ? [activityLabel(item), typeLabel(item), age].filter(Boolean).join(" - ")
        : [typeLabel(item), age].filter(Boolean).join(" - ");
    const route = kind === "scrobble" ? scrobbleRouteIcons(item) : { html: sourceIcons(item?.sources, 3), route: sourceRouteTitle(item?.sources) };
    const corner = kind === "progress" && progressLabel ? progressLabel : age;
    const episodeChip = rawType === "episode" && item?.episode_label ? String(item.episode_label) : "";
    return `
      <${tag} class="cw-rating-widget-card cw-widget-poster-card cw-widget-poster-card--${esc(kind)}"${hrefAttr}${widgetItemAttrs(kind, index)} title="${esc([title, meta].filter(Boolean).join(" | "))}">
        <img src="${esc(coverPoster(item, "w342"))}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        ${corner ? `<span class="cw-widget-poster-chip">${esc(corner)}</span>` : ""}
        ${episodeChip ? `<span class="cw-widget-poster-chip cw-widget-poster-chip--episode">${esc(episodeChip)}</span>` : ""}
        <span class="cw-rating-provider-icons" title="${esc(route.route || "Sources")}" aria-label="${esc(route.route || "Sources")}">${route.html}</span>
      </${tag}>`;
  }

  function mediaLandscapeCard(item, kind = "history", index = 0) {
    const title = item?.title || "Untitled";
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const rawType = String(item?.type || "").toLowerCase();
    const age = relTime(item?.sort_epoch || item?.captured_at || item?.watched_at || 0);
    const progress = Number(item?.progress);
    const progressLabel = Number.isFinite(progress) ? `${Math.round(progress)}%` : "";
    const meta = kind === "ratings"
      ? [typeLabel(item), item?.rating ? `Rated ${item.rating}` : "", age].filter(Boolean).join(" - ")
      : kind === "progress"
        ? [typeLabel(item), progressLabel ? `${progressLabel} progress` : "Progress sync", age].filter(Boolean).join(" - ")
        : kind === "scrobble"
          ? [activityLabel(item), typeLabel(item), age].filter(Boolean).join(" - ")
          : [typeLabel(item), item?.year || "", age].filter(Boolean).join(" - ");
    const episodeLabel = rawType === "episode" && item?.episode_label ? String(item.episode_label) : "";
    const chips = episodeLabel
      ? [age, episodeLabel].filter(Boolean)
      : [kind === "progress" && progressLabel ? progressLabel : age].filter(Boolean);
    const art = backdropPoster(item, null, "w780") || poster(item, "w300") || coverPoster(item, "w342");
    const route = kind === "scrobble"
      ? { html: sourceIcons(scrobbleRouteRows(item), 3), route: scrobbleRouteIcons(item).route }
      : { html: sourceIcons(item?.sources, 3), route: sourceRouteTitle(item?.sources) };
    return `
      <${tag} class="cw-media-card cw-media-card--${esc(kind)}"${hrefAttr}${widgetItemAttrs(kind, index)} title="${esc([title, meta].filter(Boolean).join(" | "))}">
        <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        ${chips.length ? `<span class="cw-media-card-chips">${chips.map((chip) => `<span class="cw-media-card-chip">${esc(chip)}</span>`).join("")}</span>` : ""}
        ${kind === "scrobble" ? `<span class="cw-media-card-method">${activityMethodPill(item)}</span>` : ""}
        ${kind === "ratings" && item?.rating ? `<span class="cw-media-card-score"><span>${esc(item.rating)}</span></span>` : ""}
        <span class="cw-media-card-copy">
          <strong>${esc(title)}</strong>
          <small>${esc(meta)}</small>
        </span>
        <span class="cw-media-card-sources" title="${esc(route.route)}" aria-label="${esc(route.route || "Sources")}">${route.html}</span>
      </${tag}>`;
  }

  function ratingCard(item, index = 0) {
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
      <${tag} class="cw-rating-widget-card"${hrefAttr}${widgetItemAttrs("ratings", index)} title="${esc(titleParts.join(" | "))}">
        <img src="${esc(coverPoster(item, "w342"))}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
        <span class="cw-rating-score"><span>${esc(item?.rating || "")}</span></span>
        ${ratedLabel ? `<span class="cw-rating-age-badge">${esc(ratedLabel)}</span>` : ""}
        <span class="cw-rating-provider-icons" title="${esc(route)}" aria-label="${esc(route || "Sources")}">${ratingSourceIcons(item?.sources, 3)}</span>
      </${tag}>`;
  }

  function ratingListCard(item, index = 0) {
    const title = item?.title || "Untitled";
    const ratedLabel = relTime(item?.sort_epoch || 0);
    const meta = [typeLabel(item), item?.rating ? `Rated ${item.rating}` : "", ratedLabel].filter(Boolean).join(" - ");
    const href = tmdbLink(item);
    const tag = href ? "a" : "div";
    const hrefAttr = href ? ` href="${esc(href)}" target="_blank" rel="noopener"` : "";
    const art = poster(item);
    const route = sourceRouteTitle(item?.sources);
    return `
      <${tag} class="cw-history-widget-item cw-history-widget-item--rating"${hrefAttr}${widgetItemAttrs("ratings", index)}>
        <span class="cw-history-thumb">
          <img src="${esc(art)}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/assets/img/placeholder_poster.svg'">
          ${ratedLabel ? `<span class="cw-rating-grid-age">${esc(ratedLabel)}</span>` : ""}
          ${item?.rating ? `<span class="cw-rating-grid-score"><span>${esc(item.rating)}</span></span>` : ""}
          ${item?.episode_label ? `<span class="cw-history-episode">${esc(item.episode_label)}</span>` : ""}
        </span>
        <span class="cw-history-copy">
          <strong>${esc(title)}</strong>
          <span>${esc(meta || "Rating")}</span>
        </span>
        <span class="cw-history-sources" title="${esc(route)}" aria-label="${esc(route || "Sources")}">${sourceIcons(item?.sources, 3)}</span>
      </${tag}>`;
  }

  function setEmpty(host, kind, fallbackText = "") {
    if (!host) return;
    const meta = EMPTY_META[kind] || EMPTY_META.error;
    const copy = fallbackText || meta.copy;
    const widgetKey = WIDGET_KEYS.includes(kind) ? kind : (host.closest(".cw-dash-widget")?.dataset?.widgetKey || kind);
    setWidgetEmpty(widgetKey, true);
    host.innerHTML = `
      <div class="cw-dash-empty cw-dash-empty--${esc(kind || "empty")}">
        <strong>${esc(meta.title)}</strong>
        <small>${esc(copy)}</small>
      </div>`;
  }

  function setLoading(host, kind = "list") {
    if (!host) return;
    const widgetKey = kind === "list" ? (host.closest(".cw-dash-widget")?.dataset?.widgetKey || kind) : kind;
    const view = widgetView(widgetKey);
    setWidgetEmpty(widgetKey, false);
    host.classList.toggle("cw-widget-view-icon", view === "icon");
    host.classList.toggle("cw-widget-view-media", view === "media");
    host.classList.toggle("cw-widget-view-grid", view === "grid");
    if (view === "icon" || view === "media") {
      const skeletonClass = view === "media" ? "cw-media-card" : "cw-rating-widget-card";
      const cards = Array.from({ length: view === "media" ? 4 : (effectiveSize(widgetKey) === "large" ? 3 : PAGE_STEP) }, () => `
        <div class="${skeletonClass} cw-dash-skeleton cw-dash-skeleton-poster" aria-hidden="true">
          <span class="cw-skel-shine"></span>
        </div>`).join("");
      if (effectiveSize(widgetKey) === "large") {
        host.innerHTML = `
          <div class="cw-widget-carousel">
            <button type="button" class="cw-widget-carousel-nav cw-widget-carousel-nav--prev" disabled aria-hidden="true">
              <span class="material-symbols-rounded">chevron_left</span>
            </button>
            <div class="cw-widget-carousel-row">${cards}</div>
            <button type="button" class="cw-widget-carousel-nav cw-widget-carousel-nav--next" disabled aria-hidden="true">
              <span class="material-symbols-rounded">chevron_right</span>
            </button>
          </div>`;
        return;
      }
      host.innerHTML = cards;
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

  function renderCarousel(host, items, count, cardFn, kind) {
    const visible = Math.min(count, items.length, MAX_WIDGET_ITEMS);
    const cards = items.slice(0, visible).map(cardFn).join("");
    host.innerHTML = `
      <div class="cw-widget-carousel" data-cw-widget-carousel="${esc(kind)}">
        <button type="button" class="cw-widget-carousel-nav cw-widget-carousel-nav--prev" data-cw-widget-scroll="${esc(kind)}" data-dir="-1" aria-label="Scroll ${esc(kind)} left">
          <span class="material-symbols-rounded">chevron_left</span>
        </button>
        <div class="cw-widget-carousel-row cw-widget-scrollbar">
          ${cards}
        </div>
        <button type="button" class="cw-widget-carousel-nav cw-widget-carousel-nav--next" data-cw-widget-scroll="${esc(kind)}" data-dir="1" aria-label="Scroll ${esc(kind)} right">
          <span class="material-symbols-rounded">chevron_right</span>
        </button>
      </div>`;
  }

  function renderPagedList(host, items, count, cardFn, emptyText, kind, keepPager = false, horizontal = false) {
    if (!host) return;
    if (!items.length) {
      setEmpty(host, kind, emptyText);
      return;
    }
    setWidgetEmpty(kind, false);
    if (horizontal) {
      renderCarousel(host, items, count, cardFn, kind);
      scheduleMasonry();
      return;
    }
    const visible = Math.min(count, items.length);
    const hasMore = visible < items.length;
    const moreContent = kind === "playlists"
      ? `<span class="cw-dash-see-more-label">${hasMore ? "View more playlists" : "View all playlists"}</span>`
      : `<span class="material-symbols-rounded">expand_more</span>`;
    const button = hasMore || keepPager
      ? `<button type="button" class="cw-dash-see-more" data-cw-widget-more="${esc(kind)}" aria-label="${hasMore ? `Show more ${esc(kind)} items` : `All ${esc(kind)} items shown`}"${hasMore ? "" : " disabled"}>
          ${moreContent}
        </button>`
      : "";
    host.innerHTML = `${items.slice(0, visible).map(cardFn).join("")}${button}`;
    scheduleMasonry();
  }

  function renderWidget(kind) {
    const defs = {
      history: { host: $("#recent-history-list"), list: historyCard, icon: (item, index) => mediaPosterCard(item, "history", index), media: (item, index) => mediaLandscapeCard(item, "history", index), keepPager: false },
      ratings: { host: $("#latest-ratings-grid"), list: ratingListCard, icon: ratingCard, media: (item, index) => mediaLandscapeCard(item, "ratings", index), keepPager: false },
      scrobble: { host: $("#recent-scrobble-list"), list: activityCard, icon: (item, index) => mediaPosterCard(item, "scrobble", index), media: (item, index) => mediaLandscapeCard(item, "scrobble", index), keepPager: true },
      progress: { host: $("#recent-progress-list"), list: progressCard, icon: (item, index) => mediaPosterCard(item, "progress", index), media: (item, index) => mediaLandscapeCard(item, "progress", index), keepPager: true },
      playlists: { host: $("#recent-playlists-list"), list: playlistCard, icon: playlistPosterCard, keepPager: true },
    };
    const def = defs[kind];
    if (!def) return;
    const view = widgetView(kind);
    def.host?.classList.toggle("cw-widget-view-icon", view === "icon");
    def.host?.classList.toggle("cw-widget-view-media", view === "media");
    def.host?.classList.toggle("cw-widget-view-grid", view === "grid");
    const cardFn = view === "media" && def.media ? def.media : view === "icon" ? def.icon : def.list;
    renderPagedList(
      def.host,
      latestItems[kind] || [],
      visibleCounts[kind] || widgetPageStep(kind),
      cardFn,
      "",
      kind,
      def.keepPager,
      effectiveSize(kind) === "large"
    );
  }

  function applyVisibility(settings) {
    const card = $("#dashboard-widgets-card");
    if (!card) return;
    ensureLayoutToolbar();
    ensureWidgetControls();
    let anyVisible = false;
    let anyEnabled = false;
    for (const widget of WIDGETS) {
      const node = widgetNode(widget.key);
      if (!node) continue;
      const layout = widgetLayout[widget.key] || DEFAULT_LAYOUT[widget.key];
      const enabled = settings?.[widget.key] !== false;
      const visible = enabled && (!layout.hidden || customizeOpen);
      anyEnabled = anyEnabled || enabled;
      const size = effectiveSize(widget.key);
      node.classList.toggle("hidden", !visible);
      node.classList.toggle("cw-dash-widget--wide", size === "large");
      node.classList.toggle("cw-dash-widget--compact", size === "small");
      node.classList.toggle("is-layout-hidden", !!layout.hidden);
      node.dataset.widgetSize = size;
      node.dataset.widgetView = widgetView(widget.key);
      if (widget.key === "watchlist") window.CW?.WatchlistPreview?.applyWidgetView?.(node.dataset.widgetView);
      node.style.order = String(layout.order);
      setWidgetEmpty(widget.key, isWidgetEmpty(widget.key));
      anyVisible = anyVisible || (enabled && !layout.hidden);
    }
    syncControlIcons();
    updateLayoutToolbar();
    card.classList.toggle("has-only-hidden-widgets", anyEnabled && !anyVisible);
    card.classList.toggle("hidden", !anyEnabled);
    scheduleMasonry();
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
      ["history", "ratings", "scrobble", "progress", "playlists"].forEach(resetVisibleCount);
    }
    applyVisibility(settings);
    const active = activeWidgetSettings(settings);
    if (active.watchlist) {
      Promise.resolve(window.updatePreviewVisibility?.()).catch(() => null);
    }
    if (!active.watchlist && !active.history && !active.ratings && !active.scrobble && !active.progress && !active.playlists) return;
    if (!hasTmdbKeyInConfig(cfg)) {
      $("#placeholder-card")?.classList.add("hidden");
    }

    const historyHost = $("#recent-history-list");
    const ratingsHost = $("#latest-ratings-grid");
    const scrobbleHost = $("#recent-scrobble-list");
    const progressHost = $("#recent-progress-list");
    const playlistsHost = $("#recent-playlists-list");
    if (!preserve || !hasLoaded) {
      if (active.history) setLoading(historyHost);
      if (active.ratings) setLoading(ratingsHost, "ratings");
      if (active.scrobble) setLoading(scrobbleHost);
      if (active.progress) setLoading(progressHost);
      if (active.playlists) setLoading(playlistsHost);
    }

    try {
      const requestedKinds = ["history", "ratings", "scrobble", "progress", "playlists"].filter((key) => active[key]);
      if (!requestedKinds.length) {
        hasLoaded = true;
        lastLoadedAt = Date.now();
        widgetsDirty = dirtyVersion !== refreshVersion;
        return;
      }
      const params = new URLSearchParams({
        include: requestedKinds.join(","),
        history_limit: String(MAX_WIDGET_ITEMS),
        ratings_limit: String(MAX_WIDGET_ITEMS),
        scrobble_limit: String(MAX_WIDGET_ITEMS),
        progress_limit: String(MAX_WIDGET_ITEMS),
        playlists_limit: String(MAX_WIDGET_ITEMS),
      });
      const data = await fetchWidgetPayload(`/api/dashboard/widgets?${params.toString()}`);
      if (seq !== loadSeq || !isOnMain()) return;

      const historyItems = Array.isArray(data?.recent_history?.items) ? data.recent_history.items : [];
      const scrobbleItems = Array.isArray(data?.recent_scrobble?.items) ? data.recent_scrobble.items : [];
      const ratingItems = Array.isArray(data?.latest_ratings?.items) ? data.latest_ratings.items : [];
      const progressItems = Array.isArray(data?.recent_progress?.items) ? data.recent_progress.items : [];
      const playlistItems = Array.isArray(data?.recent_playlists?.items) ? data.recent_playlists.items : [];
      setCountChip("recent-history-count-chip", data?.recent_history?.total ?? historyItems.length, "item");
      setCountChip("latest-ratings-count-chip", data?.latest_ratings?.total ?? ratingItems.length, "rating");
      setCountChip("recent-scrobble-count-chip", data?.recent_scrobble?.total ?? scrobbleItems.length, "scrobble");
      setCountChip("recent-progress-count-chip", data?.recent_progress?.total ?? progressItems.length, "progress sync");
      setCountChip("recent-playlists-count-chip", data?.recent_playlists?.total ?? playlistItems.length, "playlist sync");
      latestItems.history = historyItems;
      latestItems.ratings = ratingItems;
      latestItems.scrobble = scrobbleItems;
      latestItems.progress = progressItems;
      latestItems.playlists = playlistItems;
      hasLoaded = true;
      lastLoadedAt = Date.now();
      widgetsDirty = dirtyVersion !== refreshVersion;
      for (const kind of ["history", "ratings", "scrobble", "progress", "playlists"]) {
        if (active[kind]) renderWidget(kind);
      }
      prewarmWidgetImages(active);
    } catch (e) {
      if (authPendingError(e)) {
        scheduleAuthReadyRefresh();
        return;
      }
      if (preserve && hasLoaded) return;
      if (active.history) setEmpty(historyHost, "error", "Recent history could not be loaded.");
      if (active.ratings) setEmpty(ratingsHost, "error", "Latest ratings could not be loaded.");
      if (active.scrobble) setEmpty(scrobbleHost, "error", "Recent scrobble could not be loaded.");
      if (active.progress) setEmpty(progressHost, "error", "Recent progress could not be loaded.");
      if (active.playlists) setEmpty(playlistsHost, "error", "Recent playlists could not be loaded.");
    }
  }

  async function refreshFromButton(btn) {
    widgetsDirty = true;
    dirtyVersion += 1;
    if (!isOnMain()) return;
    btn?.classList.add("spinning");
    if (btn) void btn.offsetWidth;
    const minSpin = new Promise((resolve) => window.setTimeout(resolve, 600));
    try {
      await Promise.all([
        refreshDashboardWidgets({ force: true, forceConfig: true, preserve: hasLoaded }),
        minSpin,
      ]);
    } finally {
      btn?.classList.remove("spinning");
    }
  }

  function initDashboardWidgets() {
    ensureLayoutToolbar();
    ensureWidgetControls();
    $("#recent-history-refresh")?.addEventListener("click", (e) => refreshFromButton(e.currentTarget));
    $("#latest-ratings-refresh")?.addEventListener("click", (e) => refreshFromButton(e.currentTarget));
    $("#recent-scrobble-refresh")?.addEventListener("click", (e) => refreshFromButton(e.currentTarget));
    $("#recent-progress-refresh")?.addEventListener("click", (e) => refreshFromButton(e.currentTarget));
    $("#recent-playlists-refresh")?.addEventListener("click", (e) => refreshFromButton(e.currentTarget));
    document.addEventListener("dragover", (event) => updateDashboardDragScroll(event), true);
    document.addEventListener("drop", stopDashboardDragScroll, true);
    $("#dashboard-widgets-card")?.addEventListener("click", (event) => {
      const dashboardLayoutBtn = event.target?.closest?.("[data-cw-dashboard-layout]");
      if (dashboardLayoutBtn) {
        const action = String(dashboardLayoutBtn.dataset.cwDashboardLayout || "");
        if (action === "customize") {
          customizeOpen = !customizeOpen;
          applyVisibility(lastSettings || {});
        } else if (action === "show-all") {
          saveLayout(Object.fromEntries(WIDGET_KEYS.map((key) => [key, { ...widgetLayout[key], hidden: false }])));
          markWidgetsDirty(0);
        } else if (action === "reset") {
          resetLayout();
          markWidgetsDirty(0);
        }
        return;
      }
      const widgetActionBtn = event.target?.closest?.("[data-cw-widget-action]");
      if (widgetActionBtn) {
        const key = String(widgetActionBtn.dataset.cwWidgetKey || "");
        const action = String(widgetActionBtn.dataset.cwWidgetAction || "");
        if (!widgetLayout[key]) return;
        if (action === "hide") {
          const wasHidden = !!widgetLayout[key].hidden;
          patchLayout(key, { hidden: !wasHidden });
          if (wasHidden) markWidgetsDirty(0);
        } else if (action === "size") {
          const current = effectiveSize(key);
          patchLayout(key, { size: current === "large" ? "small" : "large" });
          resetVisibleCount(key);
          if (key !== "watchlist") renderWidget(key);
        } else if (action === "view") {
          const current = widgetView(key);
          if (effectiveSize(key) === "large") {
            if (key === "playlists") return;
            patchLayout(key, { horizontalView: current === "media" ? "poster" : "media" });
          } else {
            patchLayout(key, { view: current === "icon" ? "grid" : "icon" });
          }
          resetVisibleCount(key);
          if (key !== "watchlist") renderWidget(key);
        }
        return;
      }
      const scrollBtn = event.target?.closest?.("[data-cw-widget-scroll]");
      if (scrollBtn) {
        const carousel = scrollBtn.closest(".cw-widget-carousel");
        const row = carousel?.querySelector(".cw-widget-carousel-row");
        if (!row) return;
        const kind = String(scrollBtn.getAttribute("data-cw-widget-scroll") || "");
        const dir = Number(scrollBtn.dataset.dir || 1) < 0 ? -1 : 1;
        const amount = Math.max(220, Math.floor(row.clientWidth * 0.82));
        if (dir > 0 && latestItems[kind]?.length > (visibleCounts[kind] || widgetPageStep(kind))) {
          const step = widgetPageStep(kind);
          const previousLeft = row.scrollLeft;
          visibleCounts[kind] = Math.min((visibleCounts[kind] || step) + step, latestItems[kind].length);
          renderWidget(kind);
          requestAnimationFrame(() => {
            const nextRow = widgetNode(kind)?.querySelector(".cw-widget-carousel-row");
            if (!nextRow) return;
            nextRow.scrollLeft = previousLeft;
            nextRow.scrollBy({ left: amount, behavior: "smooth" });
          });
          return;
        }
        row.scrollBy({ left: dir * amount, behavior: "smooth" });
        return;
      }
      const itemLink = event.target?.closest?.("[data-cw-widget-item]");
      if (itemLink && event.currentTarget.contains(itemLink)) {
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        const kind = String(itemLink.getAttribute("data-cw-widget-item") || "");
        const index = Number(itemLink.getAttribute("data-cw-widget-index") || -1);
        if (["history", "ratings", "scrobble", "progress"].includes(kind) && Number.isInteger(index) && index >= 0) {
          event.preventDefault();
          void openDetailCard(kind, index);
          return;
        }
      }
      const btn = event.target?.closest?.("[data-cw-widget-more]");
      if (!btn) return;
      const kind = String(btn.getAttribute("data-cw-widget-more") || "");
      if (!["history", "ratings", "scrobble", "progress", "playlists"].includes(kind)) return;
      const step = widgetPageStep(kind);
      visibleCounts[kind] = Math.min((visibleCounts[kind] || step) + step, latestItems[kind].length);
      renderWidget(kind);
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
    window.addEventListener("cw:watchlist-widget-state", (event) => {
      setWidgetEmpty("watchlist", !!event?.detail?.empty);
      applyVisibility(lastSettings || {});
    });
    window.addEventListener("resize", scheduleMasonry);
    if (authSetupPending()) scheduleAuthReadyRefresh();
    window.addEventListener("load", () => setTimeout(() => refreshDashboardWidgets({ forceConfig: true }), 100), { once: true });
    refreshDashboardWidgets();
  }

  window.CW = window.CW || {};
  window.CW.DashboardWidgets = { refresh: refreshDashboardWidgets, showAll: showAllWidgets, hiddenCount: hiddenWidgetCount };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initDashboardWidgets, { once: true });
  } else {
    initDashboardWidgets();
  }
})();
