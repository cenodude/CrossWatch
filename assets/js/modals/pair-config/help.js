/* assets/js/modals/pair-config/help.js */
/* Help text and help-icon wiring for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export const HELP_TEXT = {
  "gl-dry": "Dry run\nPlan and log only; no writes. Reset states after testing (in maintenance).",
  "gl-verify": "Verify after write\nRe-check the destination after writes (when supported).",
  "gl-drop": "Drop guard\nProtects against sudden inventory drops by pausing delete plans.",
  "gl-mass": "Allow mass delete\nIf off, blocks large delete plans (roughly >10%). Enable for first runs.\n It's either mass-delete or drop-guard or none; not both.",
  "gl-oneway-remove": "Deletions based on Source\nWhen enabled there should always be a match between source and destination before deletion.\nWhen disabled it acts in mirror mode, meaning it will always follow source (destructive; use with care)",
  "gl-observed": "Include observed deletes\nIf off, observed deletes are ignored and delta-delete providers are disabled (safer).",
  "gl-bb-enable": "Blackbox: Enabled\nAutomatic flapper protection and failure quarantine.",
  "gl-bb-pair": "Blackbox: Pair scoped\nKeep blackbox decisions per pair instead of global.",
  "gl-section-main": "Globals\nThese are the overall safety and behavior settings for this connection. The defaults are good enough for most users, so only change them when you have a specific reason.",
  "gl-section-advanced": "Advanced\nThese are extra retention and blackbox safety controls. The defaults are good enough for most users, so you usually do not need to change anything here.",

  "cx-wl-enable": "Watchlist: Enable\nCompare watchlists and write missing items to the target.",
  "cx-wl-add": "Watchlist: Add\nAdds missing items to the target watchlist.",
  "cx-wl-remove": "Watchlist: Remove\nRemoves items from the target.",

  "cx-rt-enable": "Ratings: Enable\nCompare and write ratings to the target.",
  "cx-rt-add": "Ratings: Add / Update\nWrites ratings/updates to the target.",
  "cx-rt-remove": "Ratings: Remove\nClears ratings on the target (destructive and only for very specific needs).",

  "cx-hs-enable": "History: Enable\nCompare and write watch history to the target.",
  "cx-hs-add": "History: Add\nAdds plays/watched items to the target history.",
  "cx-hs-remove": "History: Remove\nRemoving history is discouraged (destructive and only for very specific needs).",
  "cx-tr-hs-col": "Trakt: Add collections\nAlso add items to Trakt collections when writing history (if enabled).",

  "cx-pr-enable": "Progress: Enable\nSync resume position (where you left off) between media servers.",
  "cx-pr-add": "Progress: Add / Update\nWrite resume position to the target.",
  "cx-pr-remove": "Progress: Remove\nClear resume position on the target (rare; Plex may not support).",
  "cx-pr-min": "Progress: Minimum seconds\nIgnore tiny offsets (scrubbing).",
  "cx-pr-delta": "Progress: Change threshold\nOnly write when the difference is large enough.",
  "cx-pr-maxp": "Progress: Ignore near complete (%)\nWhen near completion, history sync should handle watched state.",

  "cx-jf-wl-mode": "Jellyfin: Watchlist mode\nJellyfin has no native Watchlist. CrossWatch maps it to:\n• Favorites: sets the Favorite flag\n• Playlist: writes to a named playlist (episodes only; no shows)\n• Collections: writes to a named collection\nChanging mode does not move existing items.\nTip: Favorites or Collections are the most compatible.",
  "cx-em-wl-mode": "Emby: Watchlist mode\nEmby has no native Watchlist. CrossWatch maps it to:\n• Favorites: sets the Favorite flag\n• Playlist: writes to a named playlist (episodes only; no shows)\n• Collections: writes to a named collection\nChanging mode does not move existing items.\nTip: Favorites or Collections are the most compatible.",

  "plx-fallback-guid": "Plex: Fallback GUID\nAlso searches Plex's database beyond your visible libraries (including hidden/old items) to recover older matches.\nWarning: enable only for a single run, it increases duration and resource usage.",
  "plx-marked-watched": "Plex: Marked watched\nInclude items you manually marked as watched in Plex when syncing history.\nDisable if you only want actual play history.",
  "plx-strict-ids": "Plex: Strict ID matching\nWhen enabled, CrossWatch only matches by IDs (Plex IDs + external IDs). Title/year searches are disabled.",
  "jf-strict-ids": "Jellyfin: Strict ID matching\nWhen enabled, CrossWatch only matches by IDs (Jellyfin IDs + external IDs). Title/year searches are disabled.",
  "em-strict-ids": "Emby: Strict ID matching\nWhen enabled, CrossWatch only matches by IDs (Emby IDs + external IDs). Title/year searches are disabled.",
};

function normalizeHelpText(text) {
  return String(text || "")
    .replace(/â€¢/g, "-")
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
  const body = lines.join(" ").replace(/\s+/g, " ").trim();
  const sentences = body
    .split(/(?<=[.!?])\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
  const lead = sentences[0] || body;
  const follow = sentences.slice(1).join(" ").trim();
  return follow ? `${title}: ${lead}\n${follow}` : `${title}: ${lead}`;
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
    btn.setAttribute("aria-label", "Help");
    btn.addEventListener("click", (e) => { e.preventDefault(); e.stopPropagation(); });
    btn.addEventListener("mousedown", (e) => { e.preventDefault(); e.stopPropagation(); });

    wrap.insertBefore(btn, sw);
  }

  (QA ? QA(".cx-help[data-tip-id]", root) : Array.from(root.querySelectorAll(".cx-help[data-tip-id]"))).forEach(btn => {
    if (btn.__wired) return;
    btn.__wired = true;
    const key = btn.dataset.tipId;
    const tip = formatHelpTip(HELP_TEXT[key] || "");
    if (!tip) return;
    btn.dataset.tip = tip;
    btn.title = tip;
    btn.setAttribute("aria-label", "Help");
    btn.addEventListener("click", (e) => { e.preventDefault(); e.stopPropagation(); });
    btn.addEventListener("mousedown", (e) => { e.preventDefault(); e.stopPropagation(); });
  });
}
