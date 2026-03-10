/* assets/helpers/feature-meta.js */
/* Shared feature metadata helper */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const labels = {
    watchlist: "Watchlist",
    ratings: "Ratings",
    history: "History",
    progress: "Progress",
    playlists: "Playlists",
  };
  const order = ["watchlist", "ratings", "history", "progress", "playlists"];
  const keyOf = (v) => String(v || "").trim().toLowerCase();
  const label = (v) => {
    const key = keyOf(v);
    return labels[key] || key || "?";
  };

  (window.CW ||= {});
  window.CW.FeatureMeta = { labels, order, keyOf, label };
})();
