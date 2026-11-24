/* assets/helpers/feature-meta.js */
/* Shared feature metadata helper */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const labelKeys = {
    watchlist: "common.media.watchlist",
    ratings: "profile.ratings.title",
    history: "profile.history.title",
    progress: "profile.playback.title",
    playlists: "navigation.tabs.playlists",
    collection: "profile.collection.title",
  };
  const labels = {};
  for (const [name, key] of Object.entries(labelKeys)) Object.defineProperty(labels, name, { enumerable: true, get: () => T(key) });
  const order = ["watchlist", "ratings", "history", "progress", "playlists", "collection"];
  const keyOf = (v) => String(v || "").trim().toLowerCase();
  const label = (v) => {
    const key = keyOf(v);
    return labels[key] || key || "?";
  };

  (window.CW ||= {});
  window.CW.FeatureMeta = { labels, order, keyOf, label };
})();
