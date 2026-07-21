/* assets/helpers/media-meta.js */
/* Shared TMDB metadata fetcher and cache for the playing card and its watchlist variants */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  const NEED = Object.freeze({
    row: Object.freeze({ genres: 1, release: 1 }),
    detail: Object.freeze({
      overview: 1, runtime_minutes: 1, ids: 1, videos: 1, genres: 1,
      certification: 1, score: 1, vote_count: 1, release: 1, backdrop: 1,
    }),
  });

  const cache = new Map();
  const inflight = new Map();

  const isEpisode = (item) => {
    const raw = String(item?.media_type || item?.type || item?.entity || "").toLowerCase();
    return raw === "episode" || item?.season != null || item?.episode != null;
  };

  const kindOf = (item) => {
    const raw = String(item?.media_type || item?.type || item?.entity || "").toLowerCase();
    return raw === "movie" ? "movie" : "show";
  };

  const tmdbOf = (item) => {
    const ids = item?.ids || {};
    const id = isEpisode(item)
      ? (ids.tmdb_show || item?.tmdb_show || item?.tmdb || item?.tmdb_id || ids.tmdb || ids.id)
      : (item?.tmdb || item?.tmdb_id || ids.tmdb || ids.tmdb_show || ids.id);
    return String(id || "").trim();
  };

  const keyOf = (item) => {
    const tmdb = tmdbOf(item);
    return tmdb ? `${kindOf(item)}:${tmdb}` : "";
  };

  const merge = (base, extra, profile, kind, wantsSeries) => {
    const prev = base && typeof base === "object" ? base : {};
    const next = extra && typeof extra === "object" ? extra : {};
    const merged = { ...prev, ...next };
    merged.ids = { ...(prev.ids || {}), ...(next.ids || {}) };
    merged.detail = { ...(prev.detail || {}), ...(next.detail || {}) };
    merged.images = { ...(prev.images || {}), ...(next.images || {}) };
    merged.__kind = kind || prev.__kind || "";
    merged.__rowLoaded = !!(prev.__rowLoaded || profile === "row" || profile === "detail");
    merged.__detailLoaded = !!(prev.__detailLoaded || profile === "detail");
    merged.__videosLoaded = !!(
      prev.__videosLoaded
      || (profile === "detail" && Object.prototype.hasOwnProperty.call(next, "videos"))
    );
    merged.__seriesLoaded = !!(prev.__seriesLoaded || (wantsSeries && profile === "detail"));
    return merged;
  };

  const satisfies = (meta, profile, wantsSeries) => {
    if (!meta || typeof meta !== "object") return false;
    if (profile !== "detail") return !!meta.__rowLoaded;
    if (wantsSeries && meta.__kind === "show" && !meta.__seriesLoaded) return false;
    return !!meta.__detailLoaded
      && !!meta.__videosLoaded
      && !!(meta.overview || meta.detail?.overview || meta.detail?.tagline);
  };

  const descriptorFor = (item) => {
    const episode = isEpisode(item);
    const ids = item?.ids || {};
    return {
      tmdb: tmdbOf(item),
      type: kindOf(item),
      title: episode ? "" : String(item?.title || ""),
      year: episode ? "" : String(item?.year || ""),
      ids: { imdb: String(ids.imdb || ids.imdb_id || ids.imdb_show || ""), tvdb: String(ids.tvdb || "") },
    };
  };

  async function fetchGroup(descriptors, profile, wantsSeries) {
    const kind = descriptors[0].type;
    const need = { ...NEED[profile === "detail" ? "detail" : "row"] };
    if (wantsSeries && kind === "show" && profile === "detail") need.series_info = 1;
    const overview = profile === "detail" ? "full" : "none";

    try {
      const res = await fetch(`/api/metadata/bulk?overview=${encodeURIComponent(overview)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: descriptors.map((d) => ({ type: d.type, tmdb: d.tmdb, title: d.title, year: d.year, ids: d.ids })),
          need,
          concurrency: Math.min(Math.max(descriptors.length, 1), 6),
        }),
      });
      if (!res.ok) return false;
      const data = await res.json();
      const results = data?.results || {};
      let hit = false;
      for (const d of descriptors) {
        const entry = results[`${d.type}:${d.tmdb}`];
        const meta = entry?.ok ? (entry.meta || null) : null;
        if (!meta) continue;
        const cacheKey = `${d.type}:${d.tmdb}`;
        cache.set(cacheKey, merge(cache.get(cacheKey), meta, profile, d.type, wantsSeries));
        hit = true;
      }
      return hit;
    } catch {
      return false;
    }
  }

  async function batch(items, profile = "row", options = {}) {
    if (authSetupPending()) return false;
    const wantsSeries = !!options.seriesInfo;
    const profileKey = profile === "detail" ? "detail" : "row";
    const groups = new Map();
    const pending = [];
    const seen = new Set();

    for (const item of items || []) {
      const cacheKey = keyOf(item);
      if (!cacheKey || seen.has(cacheKey)) continue;
      seen.add(cacheKey);
      if (satisfies(cache.get(cacheKey), profileKey, wantsSeries)) continue;
      const inflightKey = `${profileKey}:${wantsSeries ? "s" : "-"}:${cacheKey}`;
      const running = inflight.get(inflightKey);
      if (running) {
        pending.push(running);
        continue;
      }
      const descriptor = descriptorFor(item);
      if (!descriptor.tmdb) continue;
      const bucket = groups.get(descriptor.type) || [];
      bucket.push({ descriptor, inflightKey });
      groups.set(descriptor.type, bucket);
    }

    for (const bucket of groups.values()) {
      const descriptors = bucket.map((b) => b.descriptor);
      const request = fetchGroup(descriptors, profileKey, wantsSeries)
        .finally(() => bucket.forEach((b) => inflight.delete(b.inflightKey)));
      bucket.forEach((b) => inflight.set(b.inflightKey, request));
      pending.push(request);
    }

    if (!pending.length) return false;
    return (await Promise.all(pending)).some(Boolean);
  }

  async function get(item, profile = "detail", options = {}) {
    const cacheKey = keyOf(item);
    if (!cacheKey) return null;
    const cached = cache.get(cacheKey);
    if (satisfies(cached, profile === "detail" ? "detail" : "row", !!options.seriesInfo)) return cached;
    await batch([item], profile, options);
    return cache.get(cacheKey) || null;
  }

  function peek(item) {
    const cacheKey = keyOf(item);
    return cacheKey ? cache.get(cacheKey) || null : null;
  }

  function has(item, profile = "row", options = {}) {
    return satisfies(peek(item), profile === "detail" ? "detail" : "row", !!options.seriesInfo);
  }

  function invalidate(item) {
    const cacheKey = keyOf(item);
    if (!cacheKey) return;
    const cached = cache.get(cacheKey);
    if (!cached || typeof cached !== "object") return;
    cache.set(cacheKey, { ...cached, __detailLoaded: false, __videosLoaded: false, __seriesLoaded: false });
  }

  (window.CW ||= {}).Meta = { key: keyOf, kind: kindOf, tmdbId: tmdbOf, isEpisode, get, peek, has, batch, invalidate };
})();
