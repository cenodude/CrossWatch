/* assets/crosswatch.js
 * Minimal UI bootstrap. Saving is handled by core.js.
 */
(function () {
  'use strict';

  const CW = window.CW || {};
  const DOM = CW.DOM || {};
  const Events = CW.Events || {};
  const D = document;

  // Tabs
  function showTab(id) {
    try {
      D.querySelectorAll("#page-main, #page-watchlist, #page-settings, .tab-page")
        .forEach(el => el.classList.add("hidden"));

      const tgt = D.getElementById("page-" + id) || D.getElementById(id);
      if (tgt) tgt.classList.remove("hidden");

      ["main", "watchlist", "settings"].forEach(n => {
        const th = D.getElementById("tab-" + n);
        if (th) th.classList.toggle("active", n === id);
      });

      D.dispatchEvent(new CustomEvent("tab-changed", { detail: { id } }));
      Events.emit?.("tab:changed", { id });

      if (id === "watchlist") {
        try { window.Watchlist?.mount?.(D.getElementById("page-watchlist")); } catch {}
      }
    } catch (e) {
      console.warn("[crosswatch] showTab failed", e);
    }
  }
  if (typeof window.showTab !== "function") window.showTab = showTab;

  // Settings collectors (core.js calls __emitSettingsCollect before save)
  const collectors = (window.__settingsCollectors ||= new Set());
  if (typeof window.registerSettingsCollector !== "function") {
    window.registerSettingsCollector = fn => { if (typeof fn === "function") collectors.add(fn); };
  }
  if (typeof window.__emitSettingsCollect !== "function") {
    window.__emitSettingsCollect = cfg => {
      try { D.dispatchEvent(new CustomEvent("settings-collect", { detail: { cfg } })); } catch {}
      for (const fn of collectors) { try { fn(cfg); } catch {} }
    };
  }

  // Bootstrap
  window.addEventListener("DOMContentLoaded", () => {
    try { DOM.fixFormLabels?.(); } catch {}
    try { CW.Providers?.load?.(); } catch {}
    try { CW.Providers?.mountMetadataProviders?.(); } catch {}
    try { CW.Pairs?.list?.(); } catch {}
    try { CW.Scheduling?.load?.(); } catch {}
    try { CW.Insights?.loadLight?.(); } catch {}
  });
})();
