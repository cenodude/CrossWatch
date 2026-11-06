/* assets/helpers/selection-bar.js */
/* CrossWatch - Shared selection count motion and interaction state */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const counts = new WeakMap();
  const animations = new Set();
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)");
  reduced.addEventListener("change", () => {
    if (reduced.matches) {
      animations.forEach(animation => animation.cancel());
      animations.clear();
    }
  });
  (window.CW ||= {}).SelectionBar = {
    update(bar, count) {
      if (!bar) return;
      const previous = counts.get(bar);
      counts.set(bar, count);
      bar.inert = !count;
      const label = bar.querySelector(".cw-selection-count strong");
      if (!label || !count || !previous || count === previous || reduced.matches || !bar.getClientRects().length) return;
      label.getAnimations().forEach(animation => animation.cancel());
      const animation = label.animate([{opacity:.4}, {opacity:1}], {duration:160, easing:"ease-out"});
      animations.add(animation);
      animation.finished.catch(() => {}).finally(() => animations.delete(animation));
    }
  };
})();
