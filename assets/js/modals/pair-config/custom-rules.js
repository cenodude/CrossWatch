/* assets/js/modals/pair-config/custom-rules.js */
/* Provider-specific pair rules for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */

const same = (a, b) => String(a || "").trim().toLowerCase() === String(b || "").trim().toLowerCase();
const isTwoWay = state => !!state?.twoWay || String(state?.mode || "").trim().toLowerCase().startsWith("two");

const RATINGS_TYPE_RULES = {
  SIMKL: { disable: ["seasons", "episodes"] },
  TMDB: { disable: ["seasons"] },
  ANILIST: { disable: ["seasons", "episodes"] },
  STREMIO: { disable: ["seasons", "episodes"] },
  FLOPPY: { disable: ["seasons", "episodes"] },
};

const COLLECTION_TYPES = {
  PLEX: ["movies", "shows", "seasons", "episodes"],
  EMBY: ["movies", "shows", "seasons", "episodes"],
  JELLYFIN: ["movies", "shows", "seasons", "episodes"],
  KODI: ["movies", "episodes"],
  TRAKT: ["movies", "shows", "seasons", "episodes"],
  MDBLIST: ["movies", "shows", "seasons", "episodes"],
  CROSSWATCH: ["movies", "shows", "seasons", "episodes"],
  PUNCHPLAY: ["movies", "shows", "seasons"],
  FLOPPY: ["movies", "shows", "seasons", "episodes"],
  SCROB: ["movies", "shows", "seasons", "episodes"],
};
const COLLECTION_SOURCES = new Set(["PLEX", "EMBY", "JELLYFIN", "KODI", "TRAKT", "MDBLIST", "CROSSWATCH", "PUNCHPLAY", "FLOPPY", "SCROB"]);
const COLLECTION_TARGETS = new Set(["TRAKT", "MDBLIST", "CROSSWATCH", "PUNCHPLAY", "FLOPPY", "SCROB"]);
const COLLECTION_ORDER = ["movies", "shows", "seasons", "episodes"];

function providerKey(value) {
  return String(value || "").trim().toUpperCase();
}

export function ratingsDisabledForPair(state) {
  const names = [state?.src, state?.dst].map(x => String(x || "").trim().toUpperCase());
  const out = new Set();
  names.forEach(n => {
    const rule = RATINGS_TYPE_RULES[n];
    if (rule && Array.isArray(rule.disable)) rule.disable.forEach(t => out.add(t));
  });
  return out;
}

export function stremioRatingsAllowed(state) {
  return same(state?.dst, "stremio") && !same(state?.src, "stremio") && !isTwoWay(state);
}

export function featureAllowedForPair(state, feature) {
  const key = String(feature || "").trim().toLowerCase();
  if (key === "ratings" && (same(state?.src, "stremio") || same(state?.dst, "stremio"))) {
    return stremioRatingsAllowed(state);
  }
  if (key === "collection") {
    return COLLECTION_SOURCES.has(providerKey(state?.src)) && COLLECTION_TARGETS.has(providerKey(state?.dst));
  }
  return true;
}

export function collectionTypesForPair(state) {
  if (!featureAllowedForPair(state, "collection")) return [];
  const src = COLLECTION_TYPES[providerKey(state?.src)] || [];
  const dst = COLLECTION_TYPES[providerKey(state?.dst)] || [];
  const dstSet = new Set(dst);
  return COLLECTION_ORDER.filter(t => src.includes(t) && dstSet.has(t));
}

export function collectionDisabledForPair(state) {
  const allowed = new Set(collectionTypesForPair(state));
  return new Set(COLLECTION_ORDER.filter(t => !allowed.has(t)));
}

export function sanitizeFeaturesForPair(state, features) {
  const out = features && typeof features === "object" ? features : {};
  if (!featureAllowedForPair(state, "ratings") && out.ratings && typeof out.ratings === "object") {
    Object.assign(out.ratings, { enable: false, add: false, remove: false });
  }
  if (out.collection && typeof out.collection === "object") {
    if (!featureAllowedForPair(state, "collection")) {
      Object.assign(out.collection, { enable: false, add: false, remove: false, types: ["movies"] });
    } else {
      const allowed = collectionTypesForPair(state);
      const selected = Array.isArray(out.collection.types) ? out.collection.types.map(x => String(x || "").trim().toLowerCase()).filter(Boolean) : ["movies"];
      const keep = COLLECTION_ORDER.filter(t => selected.includes(t) && allowed.includes(t));
      out.collection.types = keep.length ? keep : (allowed.includes("movies") ? ["movies"] : allowed.slice(0, 1));
    }
  }
  return out;
}

export function commonFeaturesForPair(state, providerFeatures, isProgressPair) {
  if (!state?.src || !state?.dst) return [];
  const a = providerFeatures(state.src);
  const b = providerFeatures(state.dst);
  const keys = ["watchlist", "ratings", "history", "progress", "playlists", "collection"];
  return keys.filter(k => {
    if (!featureAllowedForPair(state, k)) return false;
    if (k === "progress") return isProgressPair(state) && !!a.progress && !!b.progress;
    return !!a[k] && !!b[k];
  });
}
