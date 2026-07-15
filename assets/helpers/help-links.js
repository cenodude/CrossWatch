/* assets/helpers/help-links.js */
/* Central wiki help link registry */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const BASE = "https://wiki.crosswatch.app/";

  const PATHS = Object.freeze({
    plex: "crosswatch/settings/connections/media-servers/plex",
    emby: "crosswatch/settings/connections/media-servers/emby",
    jellyfin: "crosswatch/settings/connections/media-servers/jellyfin",
    trakt: "crosswatch/settings/connections/trackers/trakt",
    simkl: "crosswatch/settings/connections/trackers/simkl",
    tmdb: "crosswatch/settings/connections/trackers/tmdb",
    mdblist: "crosswatch/settings/connections/trackers/mdblist",
    publicmetadb: "crosswatch/settings/connections/trackers/publicmetadb",
    anilist: "crosswatch/settings/connections/trackers/anilist",
    tautulli: "crosswatch/settings/connections/others/tautulli",
    "tmdb-metadata": "crosswatch/settings/connections/metadata/tmdb-metadata",
    "anime-mapping": "crosswatch/settings/connections/metadata/anime-id-mapping",
    "connection-profiles": "crosswatch/settings/connections/profiles",
  });

  function normalizeKey(v){ return String(v || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, ""); }

  function path(key){ return PATHS[normalizeKey(key)] || ""; }

  function url(key){
    const rel = path(key);
    return rel ? BASE + rel : BASE;
  }

  const HelpLinks = Object.freeze({ base: BASE, paths: PATHS, path, url });

  window.CW = window.CW || {};
  window.CW.HelpLinks = HelpLinks;
})();
