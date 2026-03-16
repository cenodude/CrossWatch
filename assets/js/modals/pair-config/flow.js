/* assets/js/modals/pair-config/flow.js */
/* Flow controller for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {
  FLOW_FEATURE_COLORS,
  providerToneRgb,
  providerLogoHTML,
  sharedFeatureOrder,
  sharedFeatureLabel,
} from "./meta.js";

export function createFlowController({ ID, Q, byName, renderWarnings }) {
  function renderFlowRailDots(state) {
    const arrow = ID("cx-flow-rail")?.querySelector(".arrow");
    if (!arrow) return;
    const enabled = sharedFeatureOrder().filter((key) => !!state?.options?.[key]?.enable);
    const keys = enabled.length ? enabled : [getFlowFeatureKey(state)];
    arrow.innerHTML = keys
      .map((key, index) => {
        const rgb = FLOW_FEATURE_COLORS[key] || FLOW_FEATURE_COLORS.globals;
        const delay = (index * 0.32).toFixed(2);
        return `<span class="dot flow" style="--flow-start:6%;--flow-end:94%;--flow-dot-rgb:${rgb};--flow-delay:${delay}s"></span>`;
      })
      .join("");
  }

  function restartFlowAnimation(mode) {
    const rail = ID("cx-flow-rail");
    if (!rail) return;
    const arrow = rail.querySelector(".arrow");
    const dots = [...rail.querySelectorAll(".dot.flow")];
    ["anim-one", "anim-two"].forEach((cls) => {
      rail.classList.remove(cls);
      arrow?.classList.remove(cls);
      dots.forEach((dot) => dot.classList.remove(cls));
    });
    void rail.offsetWidth;
    const cls = mode === "two" ? "anim-two" : "anim-one";
    [rail, arrow, ...dots].forEach((node) => node?.classList.add(cls));
  }

  function applyFlowTheme(state) {
    const card = Q(".flow-card");
    const rail = ID("cx-flow-rail");
    if (!card || !rail) return;
    const srcKey = String(state?.src || "").trim().toUpperCase();
    const dstKey = String(state?.dst || "").trim().toUpperCase();
    const featKey = String(state?.feature || "globals").trim().toLowerCase();
    const srcRgb = providerToneRgb(srcKey, "87,160,255");
    const dstRgb = providerToneRgb(dstKey, "167,139,250");
    const featRgb = FLOW_FEATURE_COLORS[featKey] || FLOW_FEATURE_COLORS.globals;
    [card, rail].forEach((el) => {
      el.style.setProperty("--flow-src-rgb", srcRgb);
      el.style.setProperty("--flow-dst-rgb", dstRgb);
      el.style.setProperty("--flow-feature-rgb", featRgb);
    });
  }

  function getFlowFeatureKey(state) {
    const current = String(state?.feature || "").trim().toLowerCase();
    if (
      FLOW_FEATURE_COLORS[current] &&
      !["globals", "providers"].includes(current) &&
      !!state?.options?.[current]?.enable
    ) {
      return current;
    }
    const order = sharedFeatureOrder();
    return order.find((key) => !!state?.options?.[key]?.enable) || "watchlist";
  }

  function getFlowOptions(state) {
    const key = getFlowFeatureKey(state);
    const opts = state?.options?.[key] || { enable: false, add: false, remove: false };
    return { key, opts };
  }

  function renderFlowFeatureDots(state) {
    const host = ID("cx-flow-features");
    if (!host) return;
    const items = [
      ["watchlist", "wl"],
      ["ratings", "rt"],
      ["history", "hi"],
      ["progress", "pr"],
      ["playlists", "pl"],
    ];
    host.innerHTML = items
      .map(([key, cls]) => {
        const on = !!state?.options?.[key]?.enable;
        const label = sharedFeatureLabel(key);
        return `<span class="flow-feature-dot ${cls} ${on ? "on" : ""}" title="${label}${on ? " enabled" : ""}" aria-label="${label}${on ? " enabled" : " disabled"}"></span>`;
      })
      .join("");
  }

  function updateFlowClasses(state) {
    const rail = ID("cx-flow-rail");
    if (!rail) return;
    const two = ID("cx-mode-two")?.checked;
    const enabled = !!ID("cx-enabled")?.checked;
    const { opts } = getFlowOptions(state);

    rail.className = "flow-rail pretty";
    rail.classList.toggle("mode-two", !!two);
    rail.classList.toggle("mode-one", !two);

    const flowOn = enabled && !!opts.enable && (two ? opts.add || opts.remove : opts.add || opts.remove);
    rail.classList.toggle("off", !flowOn);
    if (two) rail.classList.toggle("active", flowOn);
    else {
      rail.classList.toggle("dir-add", flowOn && !!opts.add);
      rail.classList.toggle("dir-remove", flowOn && !opts.add && !!opts.remove);
    }

    const need = two ? "anim-two" : "anim-one";
    const parts = [rail, rail.querySelector(".arrow"), ...rail.querySelectorAll(".dot.flow")];
    parts.forEach((node) => {
      if (!node || node.classList.contains(need)) return;
      node.classList.remove("anim-one", "anim-two");
      node.classList.add(need);
    });
  }

  function updateFlow(state, animate = false) {
    const src = byName(state, state.src);
    const dst = byName(state, state.dst);
    Q("#cx-flow-src").innerHTML = src ? providerLogoHTML(src.name, src.label) : "";
    Q("#cx-flow-dst").innerHTML = dst ? providerLogoHTML(dst.name, dst.label) : "";
    const two = ID("cx-mode-two");
    const ok =
      byName(state, state.src)?.capabilities?.bidirectional &&
      byName(state, state.dst)?.capabilities?.bidirectional;
    two.disabled = !ok;
    if (!ok && two.checked) ID("cx-mode-one").checked = true;
    two.nextElementSibling?.classList.toggle("disabled", !ok);
    const title = ID("cx-flow-title");
    if (title) title.textContent = ID("cx-mode-two")?.checked ? "Two-way (bidirectional)" : "One-way";
    renderFlowFeatureDots(state);
    renderFlowRailDots(state);
    applyFlowTheme(state);
    updateFlowClasses(state);
    if (animate) restartFlowAnimation(ID("cx-mode-two")?.checked ? "two" : "one");
    renderWarnings(state);
  }

  return {
    restartFlowAnimation,
    applyFlowTheme,
    updateFlow,
    updateFlowClasses,
  };
}
