/* assets/js/modals/pair-config/help.js */
/* Help text and help-icon wiring for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const HELP_IDS = ["pair-path-matching", "gl-dry", "gl-verify", "gl-drop", "gl-mass", "gl-oneway-remove", "gl-observed", "gl-bb-enable", "gl-bb-pair", "gl-bb-section", "gl-bb-promote", "gl-section-main", "gl-section-advanced", "cx-wl-enable", "cx-wl-add", "cx-wl-remove", "cx-wl-library-only", "cx-wl-anime-map", "cx-wl-anime-only", "cx-rt-enable", "cx-rt-add", "cx-rt-remove", "cx-rt-anime-map", "cx-hs-source-status", "cx-hs-anime-map", "cx-pr-anime-map", "cx-rt-anime-only", "cx-hs-enable", "cx-hs-add", "cx-hs-remove", "cx-hs-rewatches", "cx-tr-hs-col", "cx-hs-dropped", "cx-pr-enable", "cx-pr-add", "cx-pr-remove", "cx-pr-min", "cx-pr-delta", "cx-pr-maxp", "cx-pr-replay", "cx-pr-tolerance", "cx-pr-specials", "cx-pr-cw-removals", "cx-pr-library-only", "cx-hs-specials", "cx-hs-library-only", "cx-hs-undated-today", "cx-hs-all-copies", "cx-hs-trakt-resets", "cx-co-media", "cx-co-media-upd", "cx-co-copy-details", "cx-rt-specials", "cx-rt-library-only", "floppy-anime-library", "cx-jf-wl-mode", "cx-em-wl-mode", "plx-marked-watched", "plx-cloud-read", "plx-discover-fallback", "plx-wl-pms", "plx-wl-title", "plx-strict-ids", "jf-strict-ids", "jf-targeted-lookup", "em-strict-ids", "silo-strict-ids"];
const helpKey = (id) => `pairs.help.${id.replace(/-([a-z0-9])/g, (_, c) => c.toUpperCase())}`;
export const HELP_TEXT = Object.defineProperties({}, Object.fromEntries(HELP_IDS.map((id) => [id, { enumerable: true, get: () => T(helpKey(id)) }])));

function normalizeHelpText(text) {
  return String(text || "")
    .replace(/•/g, "-")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();
}

function formatHelpTip(text) {
  const raw = normalizeHelpText(text);
  if (!raw) return "";
  const lines = raw.split("\n").map((line) => line.trim()).filter(Boolean);
  if (!lines.length) return "";
  const title = lines.shift();
  if (!lines.length) return title;
  return `${title}\n\n${lines.join("\n")}`;
}

export function injectHelpIcons(root, { QA } = {}) {
  if (!root) return;

  for (const [inputId, text] of Object.entries(HELP_TEXT)) {
    const input = root.querySelector(`#${CSS.escape(inputId)}`);
    if (!input) continue;

    const sw = input.closest("label.switch");
    if (!sw) continue;

    let wrap = sw.parentElement;
    if (!wrap || !wrap.classList.contains("cx-switch-wrap")) {
      wrap = document.createElement("span");
      wrap.className = "cx-switch-wrap";
      sw.parentNode.insertBefore(wrap, sw);
      wrap.appendChild(sw);
    }

    if (wrap.querySelector(`.cx-help[data-for="${inputId}"]`)) continue;

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "cx-help material-symbols-rounded";
    btn.textContent = "help";
    btn.dataset.for = inputId;
    btn.dataset.tip = formatHelpTip(text);
    btn.title = btn.dataset.tip;
    btn.setAttribute("aria-label", T("pairs.helpButton"));
    btn.addEventListener("click", (e) => { e.preventDefault(); e.stopPropagation(); });
    btn.addEventListener("mousedown", (e) => { e.preventDefault(); e.stopPropagation(); });

    wrap.insertBefore(btn, sw);
  }

  (QA ? QA("label[data-tip-id]", root) : Array.from(root.querySelectorAll("label[data-tip-id]"))).forEach(label => {
    const key = label.dataset.tipId;
    if (!key || !HELP_TEXT[key] || label.querySelector(".cx-help")) return;
    const inputId = label.getAttribute("for") || "";
    const input = inputId ? root.querySelector(`#${CSS.escape(inputId)}`) : null;
    if (input?.closest("label.switch")) return;

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "cx-help material-symbols-rounded";
    btn.textContent = "help";
    btn.dataset.tipId = key;
    label.appendChild(btn);
  });

  (QA ? QA(".cx-help[data-tip-id]", root) : Array.from(root.querySelectorAll(".cx-help[data-tip-id]"))).forEach(btn => {
    if (btn.__wired) return;
    btn.__wired = true;
    const key = btn.dataset.tipId;
    const tip = formatHelpTip(HELP_TEXT[key] || "");
    if (!tip) return;
    btn.dataset.tip = tip;
    btn.title = tip;
    btn.setAttribute("aria-label", T("pairs.helpButton"));
    btn.addEventListener("click", (e) => { e.preventDefault(); e.stopPropagation(); });
    btn.addEventListener("mousedown", (e) => { e.preventDefault(); e.stopPropagation(); });
  });
}
