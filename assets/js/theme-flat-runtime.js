/* assets/js/theme-flat-runtime.js */
/* Theme state controller */
(() => {
  "use strict";

  const STORAGE_KEY = "cw.ui.theme";
  const FLAT_LINK_ID = "cw-theme-flat-css";
  const ORIGINAL_LINK_ID = "cw-theme-original-css";

  function normalizeTheme(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === "flat-light" || raw === "original") return raw;
    return "flat-dark";
  }

  function storedTheme() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw === "flat-light" || raw === "flat-dark" || raw === "original") return raw;
    } catch {}
    return "";
  }

  function setThemeColor(theme) {
    try {
      const meta = document.querySelector('meta[name="theme-color"]');
      if (meta) meta.setAttribute("content", theme === "flat-light" ? "#e9edf5" : "#0b0b0f");
    } catch {}
  }

  function setThemeStyles(theme) {
    const original = theme === "original";
    const flatLink = document.getElementById(FLAT_LINK_ID);
    const originalLink = document.getElementById(ORIGINAL_LINK_ID);

    if (flatLink) {
      flatLink.disabled = original;
      flatLink.media = original ? "not all" : "all";
    }
    if (originalLink) {
      originalLink.disabled = !original;
      originalLink.media = original ? "all" : "not all";
    }
  }

  function applyTheme(value, opts = {}) {
    const theme = normalizeTheme(value);
    const root = document.documentElement;
    if (theme === "original") delete root.dataset.cwTheme;
    else root.dataset.cwTheme = theme;

    root.classList.toggle("cw-theme-light", theme === "flat-light");
    root.classList.toggle("cw-theme-dark", theme === "flat-dark");
    root.classList.toggle("cw-theme-original", theme === "original");
    setThemeStyles(theme);
    setThemeColor(theme);

    if (opts.persist) {
      try { localStorage.setItem(STORAGE_KEY, theme); } catch {}
    }
    return theme;
  }

  function themeFromConfig(cfg) {
    return normalizeTheme(cfg?.ui?.theme || cfg?.user_interface?.theme || "flat-dark");
  }

  async function syncThemeFromConfig(opts = {}) {
    try {
      const local = storedTheme();
      if (local && !opts.force) {
        applyTheme(local);
        return;
      }
      const cfg = window._cfgCache || await fetch("/api/config", {
        cache: "no-store",
        credentials: "same-origin",
      }).then((response) => response.ok ? response.json() : null);
      if (!cfg) return;
      try { window._cfgCache = cfg; } catch {}
      applyTheme(themeFromConfig(cfg), { persist: opts.persist !== false });
    } catch {}
  }

  function syncPairRangeFill(root = document) {
    try {
      root.querySelectorAll?.('.pair-config-modal #gl-drop-adv input[type="range"]').forEach((range) => {
        const min = Number(range.min || 0);
        const max = Number(range.max || 100);
        const value = Number(range.value || 0);
        const pct = max > min ? Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100)) : 0;
        range.style.setProperty("--cw-range-pct", `${pct}%`);
      });
    } catch {}
  }

  const initialTheme = storedTheme()
    || document.documentElement.dataset.cwTheme
    || (document.documentElement.classList.contains("cw-theme-original") ? "original" : "flat-dark");
  applyTheme(initialTheme);
  syncThemeFromConfig();

  window.CWTheme = Object.assign(window.CWTheme || {}, {
    apply: applyTheme,
    normalize: normalizeTheme,
    sync: syncThemeFromConfig,
  });

  document.addEventListener("DOMContentLoaded", () => syncPairRangeFill(), { once: true });
  document.addEventListener("input", (event) => {
    const range = event.target?.closest?.('.pair-config-modal #gl-drop-adv input[type="range"]');
    if (range) syncPairRangeFill(range.parentElement || document);
  }, true);
  document.addEventListener("change", (event) => {
    const range = event.target?.closest?.('.pair-config-modal #gl-drop-adv input[type="range"]');
    if (range) syncPairRangeFill(range.parentElement || document);

    const themeSelect = event.target?.closest?.("#ui_theme");
    if (themeSelect) applyTheme(themeSelect.value, { persist: true });
  }, true);
  window.addEventListener("settings-changed", () => {
    setTimeout(() => syncThemeFromConfig({ force: true }), 0);
  });

  try {
    new MutationObserver((records) => {
      const needsRangeSync = records.some((record) =>
        Array.from(record.addedNodes).some((node) =>
          node?.nodeType === 1 && (
            node.matches?.('.pair-config-modal #gl-drop-adv input[type="range"]')
            || node.querySelector?.('.pair-config-modal #gl-drop-adv input[type="range"]')
          )
        )
      );
      if (needsRangeSync) requestAnimationFrame(() => syncPairRangeFill());
    }).observe(document.body, { childList: true, subtree: true });
  } catch {
    // Older embedded browsers still receive the static theme stylesheet.
  }
})();
