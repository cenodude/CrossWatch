/* assets/js/modals/pair-config/meta.js */
/* Shared metadata accessors for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export const FLOW_FEATURE_COLORS = {
  globals: "124,92,255",
  providers: "124,92,255",
  watchlist: "0,255,163",
  ratings: "255,196,0",
  history: "45,226,255",
  progress: "167,139,250",
  playlists: "255,0,229",
};

export const providerMeta = () => window.CW?.ProviderMeta || null;
export const featureMeta = () => window.CW?.FeatureMeta || null;

export const sharedFeatureOrder = () =>
  featureMeta()?.order || ["watchlist", "ratings", "history", "progress", "playlists"];

export const sharedFeatureLabel = (key) => {
  const k = String(key || "").trim().toLowerCase();
  if (k === "globals") return "Globals";
  if (k === "providers") return "Providers";
  return featureMeta()?.label?.(k) || k || "?";
};

export const providerToneRgb = (key, fallback = "124,92,255") =>
  providerMeta()?.tone?.(key)?.rgb || fallback;

export function iconPath(name) {
  const key = String(name || "").trim().toUpperCase();
  return providerMeta()?.logoPath?.(key) || `/assets/img/${key}.svg`;
}

export function providerLogoHTML(name, label) {
  const key = String(name || "").trim().toUpperCase();
  const src = iconPath(key);
  const alt = (label || name || "Provider") + " logo";
  const fallback = label || providerMeta()?.label?.(key) || name || "-";
  return `<span class="prov-wrap"><img class="prov-logo" src="${src}" alt="${alt}" width="36" height="36" onerror="this.style.display='none'; this.nextElementSibling.style.display='inline-block'"/><span class="prov-fallback" style="display:none">${fallback}</span></span>`;
}
