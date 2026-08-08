/* assets/js/editor/rows.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function cloneRaw(raw) {
    try {
      return JSON.parse(JSON.stringify(raw || {}));
    } catch (_) {
      return {};
    }
  }

  function nextRid(options) {
    return typeof options.nextRid === "function" ? options.nextRid() : 0;
  }

  function imdbFromKey(key) {
    const s = (key || "") + "";
    if (!s.startsWith("imdb:")) return "";
    return s.slice(5).split("#")[0];
  }

  function idFromKey(key, prefix) {
    const s = (key || "") + "";
    if (!s.toLowerCase().startsWith(`${prefix}:`)) return "";
    return s.slice(prefix.length + 1).split("#")[0];
  }

  function applyManualRow(row, item, key) {
    const ids = item.ids || {};
    row.key = key;
    row.raw = item;
    row.type = "episode";
    row.episode = true;
    row.title = String(item.series_title || row.title || "");
    row.year = item.series_year != null ? String(item.series_year) : row.year || "";
    row.imdb = ids.imdb ? String(ids.imdb) : "";
    row.tmdb = ids.tmdb ? String(ids.tmdb) : "";
    row.tvdb = ids.tvdb ? String(ids.tvdb) : "";
    row.trakt = ids.trakt ? String(ids.trakt) : "";
    row.simkl = ids.simkl ? String(ids.simkl) : "";
    row.mal = ids.mal ? String(ids.mal) : "";
    row.anilist = ids.anilist ? String(ids.anilist) : "";
    row.deleted = false;
    row._origin = "manual";
    return row;
  }

  function buildManualRow(item, key, replacedKey, options = {}) {
    const row = applyManualRow(
      { _rid: nextRid(options) },
      item,
      key
    );
    if (replacedKey) row._replacedKey = replacedKey;
    return row;
  }

  function buildRows(items, options = {}) {
    const rows = [];
    for (const [key, raw] of Object.entries(items || {})) {
      const ids = raw.ids || {};
      const showIds = raw.show_ids || {};
      const type = raw.type || "";
      const isEpisode = type === "episode";
      const baseTitle = raw.title || raw.series_title || "";
      rows.push({
        _rid: nextRid(options),
        key,
        type,
        title: baseTitle,
        year: raw.year != null ? String(raw.year) : "",
        imdb: ids.imdb || (type === "season" ? showIds.imdb || imdbFromKey(key) : ""),
        tmdb: ids.tmdb || showIds.tmdb || "",
        tvdb: ids.tvdb || showIds.tvdb || "",
        trakt: ids.trakt || showIds.trakt || "",
        simkl: ids.simkl || showIds.simkl || "",
        mal: ids.mal || "",
        anilist: ids.anilist || "",
        raw: cloneRaw(raw),
        deleted: false,
        episode: isEpisode,
      });
    }
    if (options.sort !== false) rows.sort((a, b) => (a.title || "").localeCompare(b.title || ""));
    return rows;
  }

  function buildManualOverrideRows(items, blocks, options = {}) {
    const rows = buildRows(items || {}, options);
    const itemKeys = new Set(rows.map(row => String(row.key || "").toLowerCase()));
    for (const blockKey of blocks || []) {
      const key = String(blockKey || "").trim();
      if (!key || itemKeys.has(key.toLowerCase())) continue;
      rows.push({
        _rid: nextRid(options),
        key,
        type: "",
        title: "",
        year: "",
        imdb: imdbFromKey(key),
        tmdb: idFromKey(key, "tmdb"),
        tvdb: idFromKey(key, "tvdb"),
        trakt: idFromKey(key, "trakt"),
        simkl: idFromKey(key, "simkl"),
        mal: "",
        anilist: "",
        raw: { ids: {}, type: null, title: null },
        deleted: true,
        episode: false,
        _origin: "baseline",
        _manualBlock: true,
      });
    }
    rows.sort((a, b) => {
      if (a.deleted !== b.deleted) return a.deleted ? 1 : -1;
      return String(a.title || a.key || "").localeCompare(String(b.title || b.key || ""));
    });
    return rows;
  }

  Editor.Rows = {
    imdbFromKey,
    applyManualRow,
    buildManualRow,
    buildRows,
    buildManualOverrideRows,
  };
  window.CrossWatchEditorRows = Editor.Rows;
})();
