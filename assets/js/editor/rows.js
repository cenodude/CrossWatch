/* assets/js/editor/rows.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;

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

  const VIEWING_KEY = /^(.+?)@(\d{7,}(?:~.+)?|id:.+)$/i;

  function viewingBaseKey(key) {
    const match = VIEWING_KEY.exec(String(key || "").trim());
    return match ? match[1].toLowerCase() : "";
  }

  function viewingTime(row) {
    const value = Date.parse(row?.raw?.watched_at || "");
    return Number.isFinite(value) ? value : 0;
  }

  function imdbFromKey(key) {
    const s = (key || "") + "";
    if (!s.startsWith("imdb:")) return "";
    return s.slice(5).split("#")[0];
  }

  function applyManualRow(row, item, key) {
    const ids = item.ids || {};
    if (row.key && row.key !== key && !row._replacedKey) row._replacedKey = row.key;
    row.key = key;
    row.raw = item;
    row.type = item.type || row.type || "movie";
    row.episode = row.type === "episode";
    row.title = String(item.series_title || item.show_title || item.title || "");
    row.year = item.year != null ? String(item.year) : item.series_year != null ? String(item.series_year) : "";
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

  function statusForRow(row, snapshot) {
    if (!snapshot) return {label: "—", hint: T("editor.status.choose")};
    if (snapshot.loading) return {label: T("editor.status.loading"), hint: T("editor.status.reading")};
    if (snapshot.error) return {label: T("editor.status.unavailable"), hint: T("editor.status.readFailed")};
    if (!snapshot.data?.supported) return {label: "—", hint: T("editor.status.unsupported")};
    const type = row.raw?.type || row.type;
    const media = type === "movie" ? "movies" : ["show", "anime", "season", "episode"].includes(type) ? "shows" : "";
    const ids = ["season", "episode"].includes(type) ? row.raw?.show_ids || {} : row.raw?.ids || {};
    const matches = (snapshot.data.records || []).filter(record => {
      if (record.media !== media) return false;
      const shared = Object.keys(record.ids || {}).filter(key => ids[key]);
      return shared.length > 0 && shared.every(key => String(ids[key]) === String(record.ids[key]));
    });
    const status = matches.length === 1 ? matches[0].status : null;
    const label = ["planned", "watching", "on_hold", "dropped", "completed"].includes(status) ? T(`editor.status.${status}`) : "";
    return {label: label || T("editor.status.unknown"), hint: label ? T(media === "shows" ? "editor.status.currentShow" : "editor.status.currentMovie") : T("editor.status.ambiguous")};
  }

  Editor.Rows = {
    statusForRow,
    viewingBaseKey,
    viewingTime,
    imdbFromKey,
    applyManualRow,
    buildManualRow,
    buildRows,
  };
  window.CrossWatchEditorRows = Editor.Rows;
})();
