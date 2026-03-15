/* assets/helpers/provider-meta.js */
/* Refactored and expanded provider metadata helper with labels, logos, and branding info */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const providers = Object.freeze({
    CROSSWATCH: { key: "CROSSWATCH", label: "CrossWatch", shortLabel: "CW", brandClass: "brand-crosswatch", badgeId: "badge-crosswatch", aliases: ["CROSSWATCH"], tone: { solid: "#7c5cff", rgb: "124,92,255" }, watchlist: true },
    PLEX: { key: "PLEX", label: "Plex", shortLabel: "Plex", brandClass: "brand-plex", badgeId: "badge-plex", authSectionId: "sec-plex", authGroupId: "sec-auth-media", aliases: ["PLEX"], statusLegacy: ["plex_connected", "plex"], hasLogo: true, hasLogLogo: true, tone: { solid: "#e5a000", rgb: "229,160,0" }, watchlist: true },
    SIMKL: { key: "SIMKL", label: "SIMKL", shortLabel: "SIMKL", brandClass: "brand-simkl", badgeId: "badge-simkl", authSectionId: "sec-simkl", authGroupId: "sec-auth-trackers", aliases: ["SIMKL"], statusLegacy: ["simkl_connected", "simkl"], hasLogo: true, hasLogLogo: true, tone: { solid: "#00b8f5", rgb: "0,184,245" }, watchlist: true, scrobblerSink: true },
    TRAKT: { key: "TRAKT", label: "Trakt", shortLabel: "Trakt", brandClass: "brand-trakt", badgeId: "badge-trakt", authSectionId: "sec-trakt", authGroupId: "sec-auth-trackers", aliases: ["TRAKT"], statusLegacy: ["trakt_connected", "trakt"], hasLogo: true, hasLogLogo: true, tone: { solid: "#ed1c24", rgb: "237,28,36" }, watchlist: true, scrobblerSink: true },
    ANILIST: { key: "ANILIST", label: "AniList", shortLabel: "AniList", brandClass: "brand-anilist", badgeId: "badge-anilist", authSectionId: "sec-anilist", authGroupId: "sec-auth-trackers", aliases: ["ANILIST", "ANI LIST", "ANI-LIST"], statusLegacy: ["anilist_connected", "anilist"], hasLogo: true, hasLogLogo: false, tone: { solid: "#02a9ff", rgb: "2,169,255" }, watchlist: true },
    TMDB: { key: "TMDB", label: "TMDb", shortLabel: "TMDb", brandClass: "brand-tmdb-sync", badgeId: "badge-tmdb", authSectionId: "sec-tmdb-sync", authGroupId: "sec-auth-trackers", aliases: ["TMDB", "TMDBSYNC", "TMDB SYNC", "TMDB-SYNC"], statusLegacy: ["tmdb_connected", "tmdb"], hasLogo: true, hasLogLogo: false, tone: { solid: "#01b4e4", rgb: "1,180,228" }, watchlist: true },
    JELLYFIN: { key: "JELLYFIN", label: "Jellyfin", shortLabel: "Jellyfin", brandClass: "brand-jellyfin", badgeId: "badge-jellyfin", authSectionId: "sec-jellyfin", authGroupId: "sec-auth-media", aliases: ["JELLYFIN"], statusLegacy: ["jellyfin_connected", "jellyfin"], hasLogo: true, hasLogLogo: true, tone: { solid: "#7b61ff", rgb: "123,97,255" }, watchlist: true },
    EMBY: { key: "EMBY", label: "Emby", shortLabel: "Emby", brandClass: "brand-emby", badgeId: "badge-emby", authSectionId: "sec-emby", authGroupId: "sec-auth-media", aliases: ["EMBY"], statusLegacy: ["emby_connected", "emby"], hasLogo: true, hasLogLogo: true, tone: { solid: "#3bb273", rgb: "59,178,115" }, watchlist: true },
    MDBLIST: { key: "MDBLIST", label: "MDBList", shortLabel: "MDBList", brandClass: "brand-mdblist", badgeId: "badge-mdblist", authSectionId: "sec-mdblist", authGroupId: "sec-auth-trackers", aliases: ["MDBLIST", "MDB LIST", "MDB-LIST"], statusLegacy: ["mdblist_connected", "mdblist"], hasLogo: true, hasLogLogo: true, tone: { solid: "#2d74da", rgb: "45,116,218" }, watchlist: true, scrobblerSink: true },
    TAUTULLI: { key: "TAUTULLI", label: "Tautulli", shortLabel: "Tautulli", brandClass: "brand-tautulli", badgeId: "badge-tautulli", authSectionId: "sec-tautulli", authGroupId: "sec-auth-others", aliases: ["TAUTULLI"], statusLegacy: ["tautulli_connected", "tautulli"], hasLogo: true, hasLogLogo: false, tone: { solid: "#f59e0b", rgb: "245,158,11" } },
  });
  const order = Object.freeze(["CROSSWATCH","PLEX","SIMKL","TRAKT","ANILIST","TMDB","JELLYFIN","EMBY","MDBLIST","TAUTULLI"]);
  function normalizeToken(v){ return String(v || "").trim().toUpperCase().replace(/[^A-Z0-9]+/g, ""); }
  function aliasPool(key){
    const info = providers[key];
    return [key, ...(Array.isArray(info?.aliases) ? info.aliases : [])];
  }
  function keyOf(v){
    const raw = normalizeToken(v);
    if (!raw) return "";
    for (const key of order) {
      const pool = aliasPool(key);
      if (pool.some((alias) => normalizeToken(alias) === raw)) return key;
    }
    return String(v || "").trim().toUpperCase();
  }
  function matchKey(v){
    const exact = keyOf(v);
    if (providers[exact]) return exact;
    const raw = normalizeToken(v);
    if (!raw) return "";
    for (const key of order) {
      const pool = aliasPool(key);
      if (pool.some((alias) => raw.includes(normalizeToken(alias)))) return key;
    }
    return exact;
  }
  function get(v){ return providers[keyOf(v)] || null; }
  function label(v){ const info = get(v); return info?.label || keyOf(v) || "?"; }
  function shortLabel(v){ const info = get(v); return info?.shortLabel || info?.label || keyOf(v) || "?"; }
  function aliases(v){ const info = get(v); return Array.isArray(info?.aliases) && info.aliases.length ? info.aliases.slice() : [keyOf(v)].filter(Boolean); }
  function aliasesMap(){ return Object.fromEntries(order.map((key) => [key, aliases(key)])); }
  function badgeId(v){ return get(v)?.badgeId || ""; }
  function sectionId(v){ return get(v)?.authSectionId || ""; }
  function authGroupId(v){ return get(v)?.authGroupId || ""; }
  function statusLegacy(v){ return (get(v)?.statusLegacy || []).slice(); }
  function tone(v){ return get(v)?.tone || get("CROSSWATCH")?.tone || { solid: "#7c5cff", rgb: "124,92,255" }; }
  function statusProviders(){ return order.map((key) => get(key)).filter((info) => info?.badgeId && info.key !== "CROSSWATCH").map((info) => ({ key: info.key, badgeId: info.badgeId, legacy: statusLegacy(info.key) })); }
  function authProviders(){ return order.map((key) => get(key)).filter((info) => info?.authSectionId).map((info) => ({ key: info.key, sectionId: info.authSectionId, groupId: info.authGroupId || "" })); }
  function watchlistProviders(){ return order.filter((key) => !!get(key)?.watchlist); }
  function scrobblerSinks(){ return order.filter((key) => !!get(key)?.scrobblerSink); }
  function logoPath(v){ const info = get(v); return info?.hasLogo ? `/assets/img/${info.key}.svg` : ""; }
  function logLogoPath(v){ const info = get(v); return info?.hasLogLogo ? `/assets/img/${info.key}-log.svg` : ""; }
  function brandInfo(v){ const info = get(v); return { cls: info?.brandClass || "", icon: logoPath(v) || "" }; }
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
  window.CW.ProviderMeta = {
    providers, order, get, normalizeToken, keyOf, matchKey, label, shortLabel, aliases, aliasesMap, badgeId, sectionId, authGroupId,
    statusLegacy, tone, statusProviders, authProviders, watchlistProviders, scrobblerSinks, logoPath, logLogoPath, brandInfo, logoHtml,
    logLogoHtml, logo: logoPath, logLogo: logLogoPath,
    labels: Object.fromEntries(order.map((key) => [key, label(key)])),
  };
})();
