/* assets/helpers/watchlist-preview.js */
/* Extracted watchlist preview/wall UI from core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const isTV = window.isTV || (v => /^(tv|show|shows|series|season|episode|anime)$/i.test(String(v || "")));
  let wallReqSeq = 0;
  let previewBusy = false;

  window.wallLoaded = window.wallLoaded || false;
  window.__wallLoading = window.__wallLoading || false;
  window._lastSyncEpoch = window._lastSyncEpoch || null;

  const json = async (url, opt) => {
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

  const readHidden = () => {
    try { return new Set(JSON.parse(localStorage.getItem("wl_hidden") || "[]") || []); }
    catch { return new Set(); }
  };

  const writeHidden = (set) => {
    try { localStorage.setItem("wl_hidden", JSON.stringify([...set])); } catch {}
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

  function pillFor(status) {
    switch (String(status || "").toLowerCase()) {
      case "deleted": return { text: "DELETED", cls: "p-del" };
      case "both": return { text: "SYNCED", cls: "p-syn" };
      case "plex_only": return { text: "PLEX", cls: "p-px" };
      case "simkl_only": return { text: "SIMKL", cls: "p-sk" };
      case "trakt_only": return { text: "TRAKT", cls: "p-tr" };
      case "anilist_only": return { text: "ANILIST", cls: "p-al" };
      case "jellyfin_only": return { text: "JELLYFIN", cls: "p-sk" };
      case "crosswatch_only":
      case "cw_only": return { text: "CW", cls: "p-sk" };
      default: return { text: "—", cls: "p-sk" };
    }
  }

  async function resolveOverview(type, tmdb) {
    try {
      const res = await fetch("/api/metadata/resolve", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ entity: isTV(type) ? "tv" : "movie", ids: { tmdb: String(tmdb) }, need: { overview: true } }),
      });
      const data = await res.json();
      return data?.ok ? (data.result?.overview || "—") : "—";
    } catch {
      return "—";
    }
  }

  async function loadWall() {
    const card = document.getElementById("placeholder-card");
    const msg = document.getElementById("wall-msg");
    const row = document.getElementById("poster-row");
    if (!card || !msg || !row) return;

    try {
      const [wlEnabled, hasKey, uiAllowed] = await Promise.all([
        window.isWatchlistEnabledInPairs?.() ?? true,
        hasTmdbKey(),
        isWatchlistPreviewAllowed(),
      ]);
      if (!wlEnabled || !hasKey || !uiAllowed) {
        card.classList.add("hidden");
        return;
      }
      card.classList.remove("hidden");
    } catch {}

    const myReq = ++wallReqSeq;
    msg.textContent = "Loading…";
    msg.classList.remove("hidden");
    row.innerHTML = "";
    row.classList.add("hidden");

    const hidden = readHidden();
    const isDeleted = (item) => {
      if (hidden.has(item.key) && String(item.status || "").toLowerCase() === "deleted") return true;
      if (hidden.has(item.key) && String(item.status || "").toLowerCase() !== "deleted") {
        hidden.delete(item.key);
        writeHidden(hidden);
      }
      return !!(window._deletedKeys && window._deletedKeys.has(item.key));
    };

    try {
      const data = await json("/api/state/wall?both_only=0&active_only=1");
      if (myReq !== wallReqSeq) return;
      if (data?.missing_tmdb_key) { card.classList.add("hidden"); return; }
      if (!data?.ok) { msg.textContent = data?.error || "No state data found."; return; }

      let items = Array.isArray(data.items) ? data.items.slice() : [];
      if (!items.length) items = (data.items || []).filter((it) => String(it?.status || "").toLowerCase() === "both");
      window._lastSyncEpoch = data.last_sync_epoch || null;
      if (!items.length) { msg.textContent = "No items to show yet."; return; }

      const firstSeen = firstSeenMap();
      const now = Date.now();
      for (const item of items) if (item?.key && !firstSeen[item.key]) firstSeen[item.key] = now;
      try { localStorage.setItem("wl_first_seen", JSON.stringify(firstSeen)); } catch {}

      const getTs = (it) => Number(it?.added_epoch ?? it?.added_ts ?? it?.created_ts ?? it?.created ?? it?.epoch ?? firstSeen[it?.key] ?? 0);
      items.sort((a, b) => getTs(b) - getTs(a));
      items = items.slice(0, Number.isFinite(window.MAX_WALL_POSTERS) ? window.MAX_WALL_POSTERS : 20);

      msg.classList.add("hidden");
      row.classList.remove("hidden");

      for (const item of items) {
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
        img.onerror = function(){ this.onerror = null; this.src = "/assets/img/placeholder_poster.svg"; };
        link.appendChild(img);

        const overlay = document.createElement("div");
        overlay.className = "ovr";
        overlay.innerHTML = `<div class="pill ${pill.cls}">${pill.text}</div>`;
        link.appendChild(overlay);

        const cap = document.createElement("div");
        cap.className = "cap";
        cap.textContent = `${item.title || ""}${item.year ? ` · ${item.year}` : ""}`;
        link.appendChild(cap);

        const hover = document.createElement("div");
        hover.className = "hover";
        hover.innerHTML = `
          <div class="titleline">${item.title || ""}</div>
          <div class="meta">
            <div class="chip time">${window._lastSyncEpoch ? `updated ${window.relTimeFromEpoch?.(window._lastSyncEpoch) || ""}` : ""}</div>
          </div>`;
        link.appendChild(hover);

        link.addEventListener("mouseenter", async () => {
          const descEl = document.getElementById(`desc-${item.type}-${item.tmdb}`);
          if (!descEl || descEl.dataset.loaded) return;
          descEl.textContent = await resolveOverview(item.type, item.tmdb);
          descEl.dataset.loaded = "1";
        }, { passive: true });

        row.appendChild(link);
      }

      initWallInteractions();
    } catch {
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
      console.warn("isWatchlistPreviewAllowed failed, falling back to true", e);
      return true;
    }
  }

  async function updateWatchlistPreview() {
    try {
      const [hasKey, wlEnabled, uiAllowed] = await Promise.all([
        hasTmdbKey(),
        window.isWatchlistEnabledInPairs?.() ?? true,
        isWatchlistPreviewAllowed(),
      ]);
      const card = document.getElementById("placeholder-card");
      if (!hasKey || !wlEnabled || !uiAllowed) {
        if (card) card.classList.add("hidden");
        window.wallLoaded = false;
        return;
      }
      await loadWall();
      window.wallLoaded = true;
    } catch (e) {
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

      const hideAll = () => {
        card.classList.add("hidden");
        if (row) { row.innerHTML = ""; row.classList.add("hidden"); }
        if (msg) msg.textContent = "";
        window.wallLoaded = false;
      };

      if (!isOnMain()) { hideAll(); return false; }

      const [hasKey, wlEnabled, uiAllowed] = await Promise.all([
        hasTmdbKey().catch(() => false),
        Promise.resolve(window.isWatchlistEnabledInPairs?.()).catch(() => false),
        isWatchlistPreviewAllowed().catch(() => true),
      ]);

      if (!hasKey || !wlEnabled || !uiAllowed) { hideAll(); return false; }
      card.classList.remove("hidden");

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

  async function resolvePosterUrl(entity, id, size = "w342") {
    if (!id) return null;
    if (window._cfgCache && !String(window._cfgCache?.tmdb?.api_key || "").trim()) return null;
    const typ = isTV(entity) ? "tv" : "movie";
    const cb = window._lastSyncEpoch || 0;
    try {
      const res = await fetch("/api/metadata/resolve", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ entity: typ, ids: { tmdb: String(id) }, need: { poster: true } }),
      });
      if (!res.ok) return null;
      const data = await res.json();
      if (!(data?.ok && data?.result?.images?.poster?.length)) return null;
      return `/art/tmdb/${typ}/${id}?size=${encodeURIComponent(size)}&cb=${cb}`;
    } catch {
      return null;
    }
  }

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
    resolvePosterUrl,
  };

  (window.CW ||= {}).WatchlistPreview = WatchlistPreview;
  Object.assign(window, WatchlistPreview);
})();
