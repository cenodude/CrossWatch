/* assets/helpers/provider-meta.js */
/* Refactored and expanded provider metadata helper with labels, logos, and branding info */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const labels = {
    CROSSWATCH: "CrossWatch", PLEX: "Plex", SIMKL: "SIMKL", TRAKT: "Trakt",
    ANILIST: "AniList", TMDB: "TMDb", JELLYFIN: "Jellyfin", EMBY: "Emby",
    MDBLIST: "MDBList", TAUTULLI: "Tautulli"
  };
  const order = ["CROSSWATCH","PLEX","SIMKL","TRAKT","ANILIST","TMDB","JELLYFIN","EMBY","MDBLIST","TAUTULLI"];
  const brandClasses = {
    CROSSWATCH: "brand-crosswatch",
    PLEX: "brand-plex",
    SIMKL: "brand-simkl",
    TRAKT: "brand-trakt",
    ANILIST: "brand-anilist",
    TMDB: "brand-tmdb-sync",
    JELLYFIN: "brand-jellyfin",
    EMBY: "brand-emby",
    MDBLIST: "brand-mdblist",
    TAUTULLI: "brand-tautulli"
  };
  const logoProviders = new Set(order);
  const logLogoProviders = new Set(["PLEX","SIMKL","TRAKT","JELLYFIN","EMBY","MDBLIST"]);
  function keyOf(v){ return String(v || "").trim().toUpperCase(); }
  function label(v){ const k = keyOf(v); return labels[k] || k || "?"; }
  function logoPath(v){ const k = keyOf(v); return logoProviders.has(k) ? `/assets/img/${k}.svg` : ""; }
  function logLogoPath(v){ const k = keyOf(v); return logLogoProviders.has(k) ? `/assets/img/${k}-log.svg` : ""; }
  function brandInfo(v){ const k = keyOf(v); return { cls: brandClasses[k] || "", icon: logoPath(k) || "" }; }
  function logoHtml(v, cls = "token-logo"){
    const k = keyOf(v);
    const src = logoPath(k);
    if (!src) return `<span class="token-text">${v || ""}</span>`;
    return `<img class="${cls}" src="${src}" alt="${k} logo" width="28" height="28" loading="lazy">`;
  }
  function logLogoHtml(v, cls = "token-logo"){
    const k = keyOf(v);
    const src = logLogoPath(k);
    if (!src) return logoHtml(k, cls);
    return `<img class="${cls}" src="${src}" alt="${k} logo" width="28" height="28" loading="lazy">`;
  }
  (window.CW ||= {});
  window.CW.ProviderMeta = { labels, order, keyOf, label, logoPath, logLogoPath, brandInfo, logoHtml, logLogoHtml, logo: logoPath, logLogo: logLogoPath };
})();
