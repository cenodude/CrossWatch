/* assets/helpers/touch-help.js */
/* CrossWatch - Tap and keyboard access to contextual help */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(() => {
  const helpSelector = ".cx-help[data-tip], .cw-meta-provider-help[title]";
  let popup, source, forwarding = false;

  function close() {
    if (popup?.matches(":popover-open")) popup.hidePopover();
    source?.removeAttribute("aria-expanded");
    source = null;
  }

  function show(target) {
    if (source === target && popup?.matches(":popover-open")) return close();
    close();
    const message = target.dataset.tip || target.title;
    if (!message) return;
    if (!popup) {
      popup = document.createElement("div");
      popup.id = "cw-touch-help";
      popup.className = "cw-page-help";
      popup.setAttribute("popover", "auto");
      popup.setAttribute("role", "dialog");
      popup.setAttribute("aria-label", "Information");
      popup.addEventListener("toggle", event => {
        if (event.newState === "closed" && !popup.matches(":popover-open")) close();
      });
    }
    source = target;
    const text = document.createElement("div");
    text.textContent = message;
    const dismiss = document.createElement("button");
    dismiss.type = "button";
    dismiss.className = "cw-page-btn";
    dismiss.textContent = "Close";
    dismiss.addEventListener("click", () => {
      const previous = source;
      close();
      previous?.focus({preventScroll: true});
    });
    popup.replaceChildren(text, dismiss);
    if (target.dataset.nav) {
      const action = document.createElement("button");
      action.type = "button";
      action.className = "cw-page-btn";
      action.textContent = target.dataset.navLabel || "Open settings";
      action.addEventListener("click", () => {
        close();
        forwarding = true;
        try { target.click(); } finally { forwarding = false; }
      });
      popup.append(action);
    }
    (target.closest("dialog, [role=dialog]") || document.body).append(popup);
    target.setAttribute("aria-expanded", "true");
    target.setAttribute("aria-controls", popup.id);
    popup.showPopover();
    const rect = target.getBoundingClientRect();
    const width = popup.offsetWidth, height = popup.offsetHeight;
    popup.style.left = `${Math.max(12, Math.min(rect.left, innerWidth - width - 12))}px`;
    popup.style.top = `${Math.max(12, Math.min(rect.bottom + 8, innerHeight - height - 12))}px`;
    dismiss.focus({preventScroll: true});
  }

  document.addEventListener("click", event => {
    if (forwarding) return;
    const touch = matchMedia("(hover: none), (pointer: coarse)").matches;
    const target = event.target.closest?.(helpSelector + (touch ? ", .sched[data-tip], #pairs_list [data-tip]" : ""));
    if (!target || target.disabled) return;
    if (target.closest("#pairs_list") && target.closest("button, a, input, label")) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    show(target);
  }, true);
  document.addEventListener("keydown", event => {
    if (event.key !== "Escape" || !popup?.matches(":popover-open")) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    const previous = source;
    close();
    previous?.focus({preventScroll: true});
  }, true);
  window.addEventListener("resize", close, {passive: true});
  document.addEventListener("tab-changed", close);
  document.addEventListener("cw-settings-pane-changed", close);
})();
