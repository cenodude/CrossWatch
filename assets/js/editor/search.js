/* assets/js/editor/search.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const NS = (window.CW ||= {});
  const Editor = (NS.Editor ||= {});

  function normalizeSearchText(value) {
    return String(value == null ? "" : value)
      .toLowerCase()
      .replace(/['"`]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .trim()
      .replace(/\s+/g, " ");
  }

  function searchInt(value) {
    if (value == null || value === "") return null;
    const n = parseInt(String(value).trim(), 10);
    return Number.isFinite(n) ? n : null;
  }

  function parseSeasonEpisodeText(value) {
    const s = String(value || "").toLowerCase();
    let m = s.match(/\bs\s*0*(\d{1,3})\s*e\s*0*(\d{1,3})\b/);
    if (m) return { season: parseInt(m[1], 10), episode: parseInt(m[2], 10) };
    m = s.match(/\b0*(\d{1,3})\s*x\s*0*(\d{1,3})\b/);
    if (m) return { season: parseInt(m[1], 10), episode: parseInt(m[2], 10) };
    m = s.match(/\bseason\s*0*(\d{1,3})(?:\s*episode\s*0*(\d{1,3}))?\b/);
    if (m) return { season: parseInt(m[1], 10), episode: m[2] ? parseInt(m[2], 10) : null };
    m = s.match(/\bs\s*0*(\d{1,3})\b/);
    if (m) return { season: parseInt(m[1], 10), episode: null };
    m = s.match(/\bepisode\s*0*(\d{1,3})\b/);
    if (m) return { season: null, episode: parseInt(m[1], 10) };
    return { season: null, episode: null };
  }

  function rowSeasonEpisode(row) {
    const raw = row && row.raw ? row.raw : {};
    let season = searchInt(raw.season);
    let episode = searchInt(raw.episode);
    if (season == null || episode == null) {
      const parsed = parseSeasonEpisodeText([
        row?.title,
        raw.title,
        raw.name,
        raw.series_title,
        row?.key,
      ].join(" "));
      if (season == null) season = parsed.season;
      if (episode == null) episode = parsed.episode;
    }
    return { season, episode };
  }

  function rowSearchHaystack(row, options = {}) {
    const raw = row && row.raw ? row.raw : {};
    const ids = raw.ids && typeof raw.ids === "object" ? raw.ids : {};
    const showIds = raw.show_ids && typeof raw.show_ids === "object" ? raw.show_ids : {};
    const { season, episode } = rowSeasonEpisode(row);
    const visualTitle = typeof options.formatEpisodeVisualTitle === "function"
      ? options.formatEpisodeVisualTitle(row)
      : "";
    const pieces = [
      row?.key,
      row?.title,
      visualTitle,
      raw.title,
      raw.name,
      raw.series_title,
      raw.show_title,
      raw.original_title,
      raw.type,
      row?.type,
      raw.year,
      row?.year,
      row?.imdb,
      row?.tmdb,
      row?.tvdb,
      row?.trakt,
      row?.simkl,
      row?.mal,
      row?.anilist,
      ids.imdb,
      ids.tmdb,
      ids.trakt,
      ids.tvdb,
      ids.simkl,
      ids.mal,
      ids.anilist,
      showIds.imdb,
      showIds.tmdb,
      showIds.trakt,
      showIds.tvdb,
      showIds.simkl,
    ];
    if (season != null) {
      const s2 = String(season).padStart(2, "0");
      pieces.push(`season ${season}`, `season ${s2}`, `s${s2}`, `s${season}`);
    }
    if (episode != null) {
      const e2 = String(episode).padStart(2, "0");
      pieces.push(`episode ${episode}`, `episode ${e2}`, `e${e2}`, `e${episode}`);
    }
    if (season != null && episode != null) {
      const s2 = String(season).padStart(2, "0");
      const e2 = String(episode).padStart(2, "0");
      pieces.push(`s${s2}e${e2}`, `${season}x${episode}`, `${s2}x${e2}`, `season ${season} episode ${episode}`);
    } else if (season != null && typeof options.formatSxxEyy === "function") {
      pieces.push(options.formatSxxEyy(season, null));
    }
    return normalizeSearchText(pieces.join(" "));
  }

  Editor.Search = {
    normalizeSearchText,
    parseSeasonEpisodeText,
    rowSeasonEpisode,
    rowSearchHaystack,
  };
  window.CrossWatchEditorSearch = Editor.Search;
})();
