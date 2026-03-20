/* assets/helpers/watchlist-preview.js */
/* Extracted watchlist preview/wall UI from core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  const isTV = window.isTV || ((v) => /^(tv|show|shows|series|season|episode|anime)$/i.test(String(v || "")));
  let wallReqSeq = 0;
  let previewBusy = false;

  window.wallLoaded = window.wallLoaded || false;
  window.__wallLoading = window.__wallLoading || false;
  window._lastSyncEpoch = window._lastSyncEpoch || null;
  window.__wallRenderSignature = window.__wallRenderSignature || "";
  const WALL_PREVIEW_CACHE_KEY = `cw.wall.preview.${window.APP_VERSION || "v1"}`;

  const json = async (url, opt) => {
    if (authSetupPending()) throw new Error("auth setup pending");
    if (window.CW?.API?.j && !opt) return window.CW.API.j(url);
    const res = await fetch(url, { cache: "no-store", ...(opt || {}) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  };

  const getConfig = async () => {
    if (window.CW?.API?.Config?.load) return window.CW.API.Config.load(false);
    if (window._cfgCache) return window._cfgCache;
    const cfg = await json("/api/config");
    window._cfgCache = cfg;
    return cfg;
  };

  const providerMeta = () => window.CW?.ProviderMeta || {};
  const providerKey = (value) => providerMeta().keyOf?.(value) || String(value || "").trim().toUpperCase();
  const providerLabel = (value) => providerMeta().label?.(value) || providerKey(value) || String(value || "");
  const providerShortLabel = (value) => providerMeta().shortLabel?.(value) || providerLabel(value);
  const providerFromStatus = (status) => {
    const raw = String(status || "").toLowerCase().trim();
    if (!raw || raw === "both" || raw === "deleted") return "";
    if (raw === "crosswatch_only" || raw === "cw_only") return "CROSSWATCH";
    if (raw.endsWith("_only")) return providerKey(raw.slice(0, -5));
    return "";
  };
  const PILL_CLASS_BY_PROVIDER = {
    PLEX: "p-px",
    SIMKL: "p-sk",
    TRAKT: "p-tr",
    ANILIST: "p-al",
    JELLYFIN: "p-sk",
    CROSSWATCH: "p-sk",
  };

  const readHidden = () => {
    try { return new Set(JSON.parse(localStorage.getItem("wl_hidden") || "[]") || []); }
    catch { return new Set(); }
  };

  const writeHidden = (set) => {
    try { localStorage.setItem("wl_hidden", JSON.stringify([...set])); } catch {}
  };

  const readWallCache = () => {
    try {
      const data = JSON.parse(localStorage.getItem(WALL_PREVIEW_CACHE_KEY) || "{}");
      return Array.isArray(data?.items) ? data : null;
    } catch {
      return null;
    }
  };

  const writeWallCache = (items, lastSyncEpoch) => {
    try {
      localStorage.setItem(WALL_PREVIEW_CACHE_KEY, JSON.stringify({ items, last_sync_epoch: lastSyncEpoch || 0 }));
    } catch {}
  };

  const firstSeenMap = () => {
    try { return JSON.parse(localStorage.getItem("wl_first_seen") || "{}"); }
    catch { return {}; }
  };

  function updateEdges() {
    const row = document.getElementById("poster-row");
    const left = document.getElementById("edgeL");
    const right = document.getElementById("edgeR");
    if (!row || !left || !right) return;
    const max = row.scrollWidth - row.clientWidth - 1;
    left.classList.toggle("hide", row.scrollLeft <= 0);
    right.classList.toggle("hide", row.scrollLeft >= max);
  }

  function scrollWall(dir) {
    const row = document.getElementById("poster-row");
    if (!row) return;
    row.scrollBy({ left: dir * row.clientWidth, behavior: "smooth" });
    setTimeout(updateEdges, 350);
  }

  function initWallInteractions() {
    const row = document.getElementById("poster-row");
    if (!row || row.__cwWallWired) return;
    row.addEventListener("scroll", updateEdges, { passive: true });
    row.addEventListener("wheel", (e) => {
      if (Math.abs(e.deltaY) <= Math.abs(e.deltaX)) return;
      e.preventDefault();
      row.scrollBy({ left: e.deltaY, behavior: "auto" });
    }, { passive: false });
    row.__cwWallWired = true;
    updateEdges();
  }

  function artUrl(item, size = "w342") {
    const tmdb = item?.tmdb;
    if (!tmdb) return null;
    return `/art/tmdb/${isTV(item.type || item.entity || item.media_type) ? "tv" : "movie"}/${tmdb}?size=${encodeURIComponent(size)}&cb=${window._lastSyncEpoch || 0}`;
  }

  const providerLogoPath = (name) => window.CW?.ProviderMeta?.logoPath?.(name) || "";
  const previewGate = async () => {
    const [wlEnabled, hasKey, uiAllowed] = await Promise.all([
      Promise.resolve(window.isWatchlistEnabledInPairs?.() ?? true).catch(() => false),
      hasTmdbKey().catch(() => false),
      isWatchlistPreviewAllowed().catch(() => true),
    ]);
    return { wlEnabled, hasKey, uiAllowed, allowed: !!(wlEnabled && hasKey && uiAllowed) };
  };
  const hidePreviewCard = (card, row, msg) => {
    card?.classList.add("hidden");
    if (row) { row.innerHTML = ""; row.classList.add("hidden"); }
    if (msg) msg.textContent = "";
    window.wallLoaded = false;
  };
  const setWallEmpty = (row, msg, text) => {
    window.__wallRenderSignature = "";
    row.replaceChildren();
    row.classList.add("hidden");
    msg.textContent = text;
    msg.classList.remove("hidden");
  };

  function pillFor(status) {
    const raw = String(status || "").toLowerCase().trim();
    if (raw === "deleted") return { text: "DELETED", cls: "p-del" };
    if (raw === "both") return { text: "SYNCED", cls: "p-syn" };
    const provider = providerFromStatus(raw);
    if (provider) return { text: providerShortLabel(provider).toUpperCase(), cls: PILL_CLASS_BY_PROVIDER[provider] || "p-sk" };
    return { text: "-", cls: "p-sk" };
  }

  function providersForItem(item) {
    const direct = Array.isArray(item?.sources)
      ? item.sources.map(providerKey).filter(Boolean)
      : [];
    if (direct.length) return [...new Set(direct)];

    const sbp = item?.sources_by_provider || item?.sourcesByProvider || {};
    const byProvider = Object.keys(sbp || {}).map(providerKey).filter(Boolean);
    if (byProvider.length) return [...new Set(byProvider)];

    const provider = providerFromStatus(item?.status);
    return provider ? [provider] : [];
  }

  function providerIconMarkup(name) {
    const src = providerLogoPath(name);
    const label = providerLabel(name);
    const shortLabel = providerShortLabel(name);
    const shell = `display:inline-flex;align-items:center;justify-content:center;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:rgba(7,11,18,.38);box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 8px 20px rgba(0,0,0,.18);backdrop-filter:blur(10px) saturate(120%);-webkit-backdrop-filter:blur(10px) saturate(120%);`;
    return src
      ? `<span title="${label}" style="${shell}width:26px;height:26px;padding:0 6px;"><img src="${src}" alt="${label} logo" style="display:block;width:auto;height:14px;max-width:16px;object-fit:contain;filter:brightness(1.02)"></span>`
      : `<span title="${label}" style="${shell}min-width:26px;height:26px;padding:0 8px;font-size:10px;font-weight:800;line-height:1;color:rgba(245,248,255,.88);">${shortLabel}</span>`;
  }

  function wallSignature(items, epoch) {
    return JSON.stringify({
      epoch: Number(epoch || 0),
      items: (Array.isArray(items) ? items : []).map((item) => [
        item?.key || "",
        item?.status || "",
        item?.tmdb || "",
        item?.type || "",
        item?.year || "",
      ]),
    });
  }

  function renderWall(row, msg, items, lastSyncEpoch, { preserveIfSame = false } = {}) {
    let wallItems = Array.isArray(items) ? items.slice() : [];
    if (!wallItems.length) {
      setWallEmpty(row, msg, "No items to show yet.");
      return false;
    }

    const signature = wallSignature(wallItems, lastSyncEpoch);
    const hasRenderedWall = row.childElementCount > 0 && !row.classList.contains("hidden");
    if (preserveIfSame && signature === window.__wallRenderSignature && hasRenderedWall) {
      msg.classList.add("hidden");
      row.classList.remove("hidden");
      return true;
    }

    const hidden = readHidden();
    const isDeleted = (item) => {
      if (hidden.has(item.key) && String(item.status || "").toLowerCase() === "deleted") return true;
      if (hidden.has(item.key) && String(item.status || "").toLowerCase() !== "deleted") {
        hidden.delete(item.key);
        writeHidden(hidden);
      }
      return !!(window._deletedKeys && window._deletedKeys.has(item.key));
    };

    const firstSeen = firstSeenMap();
    const now = Date.now();
    for (const item of wallItems) if (item?.key && !firstSeen[item.key]) firstSeen[item.key] = now;
    try { localStorage.setItem("wl_first_seen", JSON.stringify(firstSeen)); } catch {}

    const getTs = (it) => Number(it?.added_epoch ?? it?.added_ts ?? it?.created_ts ?? it?.created ?? it?.epoch ?? firstSeen[it?.key] ?? 0);
    wallItems.sort((a, b) => getTs(b) - getTs(a));
    wallItems = wallItems.slice(0, Number.isFinite(window.MAX_WALL_POSTERS) ? window.MAX_WALL_POSTERS : 20);

    const frag = document.createDocumentFragment();
    let renderedCount = 0;

    for (const item of wallItems) {
      if (!item?.tmdb) continue;
      const link = document.createElement("a");
      const source = isDeleted(item) ? "deleted" : (item.status || "");
      const pill = pillFor(source);

      link.className = "poster";
      link.href = `https://www.themoviedb.org/${isTV(item.type) ? "tv" : "movie"}/${item.tmdb}`;
      link.target = "_blank";
      link.rel = "noopener";
      link.dataset.type = item.type || "";
      link.dataset.tmdb = String(item.tmdb);
      link.dataset.key = item.key || "";
      link.dataset.source = source;

      const img = document.createElement("img");
      img.loading = "lazy";
      img.alt = `${item.title || ""} (${item.year || ""})`;
      img.src = artUrl(item, "w342") || "/assets/img/placeholder_poster.svg";
      img.onerror = function () { this.onerror = null; this.src = "/assets/img/placeholder_poster.svg"; };
      link.appendChild(img);

      const overlay = document.createElement("div");
      const currentProviders = providersForItem(item).slice(0, 3);
      const synced = String(source).toLowerCase() === "both";
      overlay.className = "ovr";
      overlay.style.left = "8px";
      overlay.style.right = synced ? "8px" : "auto";
      overlay.style.justifyContent = synced ? "center" : "flex-start";
      overlay.style.width = synced ? "calc(100% - 16px)" : "auto";
      overlay.innerHTML = synced
        ? `<div class="pill ${pill.cls}">${pill.text}</div>`
        : currentProviders.map(providerIconMarkup).join("");
      link.appendChild(overlay);

      const cap = document.createElement("div");
      cap.className = "cap";
      cap.textContent = `${item.title || ""}${item.year ? ` - ${item.year}` : ""}`;
      link.appendChild(cap);

      const hover = document.createElement("div");
      hover.className = "hover";
      hover.innerHTML = `
        <div class="titleline">${item.title || ""}</div>
        <div class="meta">
          <div class="chip time">${lastSyncEpoch ? `updated ${window.relTimeFromEpoch?.(lastSyncEpoch) || ""}` : ""}</div>
        </div>`;
      link.appendChild(hover);

      frag.appendChild(link);
      renderedCount++;
    }

    if (!renderedCount) {
      setWallEmpty(row, msg, "No items to show yet.");
      return false;
    }

    window._lastSyncEpoch = lastSyncEpoch || null;
    row.replaceChildren(frag);
    row.classList.remove("hidden");
    msg.classList.add("hidden");
    window.__wallRenderSignature = wallSignature(wallItems, lastSyncEpoch);
    initWallInteractions();
    return true;
  }

  async function loadWall() {
    const card = document.getElementById("placeholder-card");
    const msg = document.getElementById("wall-msg");
    const row = document.getElementById("poster-row");
    if (!card || !msg || !row) return;

    try {
      const gate = await previewGate();
      if (!gate.allowed) {
        card.classList.add("hidden");
        return;
      }
      card.classList.remove("hidden");
    } catch {}

    const myReq = ++wallReqSeq;
    const hasRenderedWall = row.childElementCount > 0 && !row.classList.contains("hidden");
    if (!hasRenderedWall) {
      msg.textContent = "Loading...";
      msg.classList.remove("hidden");
      row.classList.add("hidden");
      const cached = readWallCache();
      if (cached) {
        renderWall(row, msg, cached.items, cached.last_sync_epoch || 0);
      }
    }

    try {
      const limit = Number.isFinite(window.MAX_WALL_POSTERS) ? Math.max(1, Number(window.MAX_WALL_POSTERS)) : 20;
      const data = await json(`/api/state/wall?both_only=0&active_only=1&limit=${encodeURIComponent(limit)}`);
      if (myReq !== wallReqSeq) return;
      if (data?.missing_tmdb_key) { card.classList.add("hidden"); return; }
      if (!data?.ok) { msg.textContent = data?.error || "No state data found."; return; }

      let items = Array.isArray(data.items) ? data.items.slice() : [];
      if (!items.length) items = (data.items || []).filter((it) => String(it?.status || "").toLowerCase() === "both");
      window._lastSyncEpoch = data.last_sync_epoch || null;
      if (!items.length) {
        setWallEmpty(row, msg, "No items to show yet.");
        return;
      }
      writeWallCache(items, data.last_sync_epoch || 0);
      renderWall(row, msg, items, data.last_sync_epoch || 0, { preserveIfSame: true });
    } catch {
      if (!hasRenderedWall) {
        row.classList.add("hidden");
        msg.classList.remove("hidden");
      }
      msg.textContent = "Failed to load preview.";
    }
  }

  async function hasTmdbKey() {
    const pick = (cfg) => {
      const fromBlock = (blk) => {
        if (!blk || typeof blk !== "object") return "";
        const direct = String(blk.api_key || "").trim();
        if (direct) return direct;
        const insts = blk.instances;
        if (!insts || typeof insts !== "object") return "";
        for (const value of Object.values(insts)) {
          const key = value && typeof value === "object" ? String(value.api_key || "").trim() : "";
          if (key) return key;
        }
        return "";
      };
      return fromBlock(cfg?.tmdb) || fromBlock(cfg?.tmdb_sync);
    };

    try {
      return !!pick(await getConfig());
    } catch {
      return false;
    }
  }

  function isOnMain() {
    const tab = String(document.documentElement.dataset.tab || "").toLowerCase();
    if (tab) return tab === "main";
    return !!document.getElementById("tab-main")?.classList.contains("active");
  }

  async function isWatchlistPreviewAllowed() {
    try {
      const cfg = await getConfig();
      const ui = cfg?.ui || cfg?.user_interface || {};
      return typeof ui.show_watchlist_preview === "boolean" ? !!ui.show_watchlist_preview : true;
    } catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return true;
      console.warn("isWatchlistPreviewAllowed failed, falling back to true", e);
      return true;
    }
  }

  async function updateWatchlistPreview() {
    try {
      const { allowed } = await previewGate();
      const card = document.getElementById("placeholder-card");
      if (!allowed) {
        if (card) card.classList.add("hidden");
        window.wallLoaded = false;
        return;
      }
      await loadWall();
      window.wallLoaded = true;
    } catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return;
      console.error("Failed to update watchlist preview:", e);
    }
  }

  async function updatePreviewVisibility() {
    if (previewBusy) return false;
    previewBusy = true;
    try {
      const card = document.getElementById("placeholder-card");
      const row = document.getElementById("poster-row");
      const msg = document.getElementById("wall-msg");
      if (!card) return false;

      if (!isOnMain()) { hidePreviewCard(card, row, msg); return false; }
      card.classList.remove("hidden");
      if (msg && !window.wallLoaded) {
        msg.textContent = "Loading...";
        msg.classList.remove("hidden");
      }

      const { allowed } = await previewGate();
      if (!allowed) { hidePreviewCard(card, row, msg); return false; }

      if (!window.wallLoaded && !window.__wallLoading) {
        window.__wallLoading = true;
        try { await loadWall(); window.wallLoaded = true; }
        finally { window.__wallLoading = false; }
      }
      return true;
    } finally {
      previewBusy = false;
    }
  }

  window.addEventListener("storage", (event) => {
    if (event.key !== "wl_hidden") return;
    updatePreviewVisibility();
    window.dispatchEvent(new CustomEvent("watchlist-hidden-changed"));
  });

  const WatchlistPreview = {
    updateEdges,
    scrollWall,
    initWallInteractions,
    artUrl,
    loadWall,
    updateWatchlistPreview,
    hasTmdbKey,
    isOnMain,
    isWatchlistPreviewAllowed,
    updatePreviewVisibility,
  };

  (window.CW ||= {}).WatchlistPreview = WatchlistPreview;
  Object.assign(window, WatchlistPreview);
})();
