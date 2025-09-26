/* CrossWatch UI hardening — scope Watchlist Preview to Main + fix initial tab */
(() => {
  if (window.__cwHardeningApplied) return;
  window.__cwHardeningApplied = true;

  const ID = (x)=>document.getElementById(x);
  const TABS = new Set(["main","watchlist","settings"]);
  const norm = (t)=> (TABS.has(String(t||"").toLowerCase()) ? String(t).toLowerCase() : "main");

  const setTabFlag = (tab) => {
    const t = norm(tab);
    document.documentElement.dataset.tab = t;
    document.body.dataset.tab = t;
    return t;
  };

  const headerActive = () =>
    (ID("tab-settings")?.classList.contains("active") && "settings") ||
    (ID("tab-watchlist")?.classList.contains("active") && "watchlist") ||
    (ID("tab-main")?.classList.contains("active") && "main") || "";

  const hidePreview = () => {
    const card = ID("placeholder-card");
    if (card) card.classList.add("hidden");
  };

  // Hard gate for any preview updater: if not on Main, force-hide & bail.
  const origUpd = window.updatePreviewVisibility;
  window.updatePreviewVisibility = function(...args){
    const t = (document.body?.dataset?.tab || "").toLowerCase();
    if (t !== "main") { hidePreview(); return; }
    return origUpd?.apply(this, args);
  };

  const origShowTab = window.showTab || (async () => {});
  async function safeShowTab(id){
    const req = norm(id);
    // Call original tab switcher first (lets it set classes/DOM)
    try { await origShowTab.call(window, req); } catch (e) { console.warn("[cw] showTab error:", e); }
    // Derive effective tab from header after original runs
    const eff = norm(headerActive() || req);
    setTabFlag(eff);
    if (eff !== "main") hidePreview();
    try { await window.updatePreviewVisibility?.(); } catch {}
    document.dispatchEvent(new CustomEvent("tab-changed", { detail:{ id: eff, hardened:true } }));
  }
  window.showTab = safeShowTab;

  // Initial tab: hash > header active > main
  const hashTab = norm((location.hash || "").replace(/^#/,"").trim());
  const initial = (location.hash ? hashTab : norm(headerActive() || "main"));

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => safeShowTab(initial), { once:true });
  } else {
    // run twice to win against late header class toggles
    safeShowTab(initial);
    queueMicrotask(() => safeShowTab(initial));
  }

  // Hash routing
  window.addEventListener("hashchange", () => {
    const h = norm((location.hash || "").replace(/^#/,""));
    safeShowTab(h);
  });

  // Keep data-tab + preview rules in sync with any external tab changes
  document.addEventListener("tab-changed", (e) => {
    const t = norm(e?.detail?.id || "");
    setTabFlag(t);
    if (t !== "main") hidePreview();
  });

  // If Ops card is hidden (i.e., not on Main), ensure preview is hidden too
  const ops = ID("ops-card");
  if (ops) {
    new MutationObserver(() => {
      const onMain = !ops.classList.contains("hidden");
      if (!onMain) hidePreview();
    }).observe(ops, { attributes:true, attributeFilter:["class"] });
  }
})();
