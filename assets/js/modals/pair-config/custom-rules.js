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
  return true;
}

export function sanitizeFeaturesForPair(state, features) {
  const out = features && typeof features === "object" ? features : {};
  if (!featureAllowedForPair(state, "ratings") && out.ratings && typeof out.ratings === "object") {
    Object.assign(out.ratings, { enable: false, add: false, remove: false });
  }
  return out;
}

export function commonFeaturesForPair(state, providerFeatures, isProgressPair) {
  if (!state?.src || !state?.dst) return [];
  const a = providerFeatures(state.src);
  const b = providerFeatures(state.dst);
  const keys = ["watchlist", "ratings", "history", "progress", "playlists"];
  return keys.filter(k => {
    if (!featureAllowedForPair(state, k)) return false;
    if (k === "progress") return isProgressPair(state) && !!a.progress && !!b.progress;
    return !!a[k] && !!b[k];
  });
}
