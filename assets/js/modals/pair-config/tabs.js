/* assets/js/modals/pair-config/tabs.js */
/* Feature tab controller for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function createTabsController({
  ID,
  sharedFeatureOrder,
  sharedFeatureLabel,
  commonFeatures,
  renderFeaturePanel,
  applyFlowTheme,
  renderWarnings,
  restartFlowAnimation,
  injectHelpIcons,
}) {
  function refreshTabs(state) {
    const tabs = ID("cx-feat-tabs");
    if (!tabs) return;
    const panel = ID("cx-feat-panel");
    const ORDER = ["globals", "providers", ...sharedFeatureOrder()];
    const COMMON = new Set(commonFeatures(state));
    const isValid = (k) => k === "globals" || k === "providers" || (ORDER.includes(k) && COMMON.has(k));
    if (!isValid(state.feature)) state.feature = "globals";

    tabs.innerHTML = "";
    ORDER.forEach((k) => {
      if (!["globals", "providers"].includes(k) && !COMMON.has(k)) return;
      const b = document.createElement("button");
      b.className = "ftab";
      b.dataset.key = k;
      b.type = "button";
      b.id = `cx-tab-${k}`;
      b.setAttribute("role", "tab");
      b.setAttribute("aria-controls", "cx-feat-panel");
      const icon = k === "globals" ? "tune" : k === "providers" ? "dns" : "";
      b.innerHTML = icon
        ? `<span class="material-symbols-rounded" aria-hidden="true">${icon}</span><span>${sharedFeatureLabel(k)}</span>`
        : sharedFeatureLabel(k);
      b.onclick = () => {
        state.feature = k;
        if (panel) panel.setAttribute("aria-labelledby", b.id);
        applyFlowTheme(state);
        renderFeaturePanel(state);
        renderWarnings(state);
        queueMicrotask(() => injectHelpIcons(ID("cx-modal")));
        [...tabs.children].forEach((c) => {
          const active = c.dataset.key === k;
          c.classList.toggle("active", active);
          c.setAttribute("aria-selected", active ? "true" : "false");
          c.tabIndex = active ? 0 : -1;
        });
        restartFlowAnimation(ID("cx-mode-two")?.checked ? "two" : "one");
      };
      b.addEventListener("keydown", (e) => {
        if (e.key !== "ArrowRight" && e.key !== "ArrowLeft" && e.key !== "Home" && e.key !== "End") return;
        e.preventDefault();
        const btns = [...tabs.querySelectorAll(".ftab")];
        if (!btns.length) return;
        const idx = btns.indexOf(b);
        const nextIdx =
          e.key === "Home" ? 0 :
          e.key === "End" ? btns.length - 1 :
          e.key === "ArrowRight" ? (idx + 1) % btns.length :
          (idx - 1 + btns.length) % btns.length;
        btns[nextIdx]?.focus();
        btns[nextIdx]?.click();
      });
      const active = state.feature === k;
      if (active) b.classList.add("active");
      b.setAttribute("aria-selected", active ? "true" : "false");
      b.tabIndex = active ? 0 : -1;
      if (active && panel) panel.setAttribute("aria-labelledby", b.id);
      tabs.appendChild(b);
    });

    renderFeaturePanel(state);
    applyFlowTheme(state);
    renderWarnings(state);

    queueMicrotask(() => {
      renderFeaturePanel(state);
      applyFlowTheme(state);
      renderWarnings(state);
      queueMicrotask(() => injectHelpIcons(ID("cx-modal")));
      restartFlowAnimation(ID("cx-mode-two")?.checked ? "two" : "one");
    });
  }

  return { refreshTabs };
}
