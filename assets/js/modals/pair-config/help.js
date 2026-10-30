/* assets/js/modals/pair-config/help.js */
/* Help text and help-icon wiring for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export const HELP_TEXT = {
  "pair-path-matching": "Match by file path\nApplies only to this pair. When ID lookup fails, compare file-path endings between Plex, Jellyfin, Emby and Silo.\nSupports movies and episodes for history, ratings and progress, including with Strict ID matching enabled. Ambiguous paths and conflicting IDs are skipped.\nOff by default. The first lookup may take longer while the library catalogue is built.",
  "gl-dry": "Dry run\nPlan and log only; no writes. Reset states after testing (in maintenance).",
  "gl-verify": "Verify after write\nRe-check the destination after writes (when supported).",
  "gl-drop": "Drop guard\nProtects against sudden inventory drops by pausing delete plans.",
  "gl-mass": "Allow mass delete\nIf off, blocks large delete plans (roughly >10%). Enable for first runs.\nIt's either mass-delete or drop-guard or none; not both.",
  "gl-oneway-remove": "Deletions based on Source\nWhen enabled, this pair only removes items after CrossWatch observed them disappear from the source.\nWhen disabled, this pair runs mirror mode and removes target items missing from the source (destructive; use with care).",
  "gl-observed": "Include observed deletes\nIf off, observed deletes are ignored and delta-delete providers are disabled (safer).",
  "gl-bb-enable": "Blackbox: Enabled\nAutomatic flapper protection and failure quarantine.",
  "gl-bb-pair": "Blackbox: Pair scoped\nKeep blackbox decisions per pair instead of global.",
  "gl-bb-section": "Blackbox\nUnresolved is only a reporting state: items that fail to write are recorded, shown in diagnostics, and retried on every sync.\nBlackbox is the quarantine state and the only thing that stops an item from being planned again.",
  "gl-bb-promote": "Promote after (failed writes)\nNumber of consecutive failed write attempts before an item is moved to the blackbox.\nUntil then the item keeps being retried; unresolved alone never blocks it.",
  "gl-section-main": "General\nMatching and Pair safety apply to this connection. The remaining safety and runtime controls are shared application settings. The defaults are good enough for most users, so only change them when you have a specific reason.",
  "gl-section-advanced": "Advanced\nThese are extra retention and blackbox safety controls. The defaults are good enough for most users, so you usually do not need to change anything here.",

  "cx-wl-enable": "Watchlist: Enable\nCompare watchlists and write missing items to the target.",
  "cx-wl-add": "Watchlist: Add\nAdds missing items to the target watchlist.",
  "cx-wl-remove": "Watchlist: Remove\nRemoves items from the target.",
  "cx-wl-library-only": "Watchlist: Only items in destination library\nSkip watchlist items that are not in the media server library.\nUseful for large tracker watchlists, where most titles are not in your library yet.\nMatches on IDs only, so titles stored under a different ID are skipped too.",
  "cx-wl-anime-map": "Use Anime ID Mapping\nUse the local Anime ID database to match anime for this watchlist pair.\nRequires global Anime ID Mapping to be enabled first.",
  "cx-wl-anime-only": "Anime-only sync\nOnly sync items that Anime ID Mapping can confirm as anime. Non-anime and unmapped items are skipped before falling back to a title search.",
  "cx-rt-enable": "Ratings: Enable\nCompare and write ratings to the target.",
  "cx-rt-add": "Ratings: Add / Update\nWrites ratings/updates to the target.",
  "cx-rt-remove": "Ratings: Remove\nClears ratings on the target (destructive and only for very specific needs).",
  "cx-rt-anime-map": "Use Anime ID Mapping\nUse the local Anime ID database to match anime for this ratings pair.\nRequires global Anime ID Mapping to be enabled first.",
  "cx-hs-source-status": "Use source watch status\nKeep dropped and on-hold status when adding watched episodes between SIMKL, AniList, MyAnimeList and Kitsu.\nStatus-only changes and titles with no watched episodes are not synced. Completed titles are not changed back to dropped or on hold.\nWhen off, normal destination progress rules determine the status. SIMKL may need one extra list read per history batch to protect completed titles.",
  "cx-hs-anime-map": "Anime episode mapping\nTranslate anime episode numbering when this history pair writes to an anime tracker, using the local Anime ID database.\nRequires a TMDB metadata key and global Anime ID Mapping.\nThis can heavily increase API calls when additional anime ID and episode lookups are needed.",
  "cx-pr-anime-map": "Anime episode mapping\nTranslate anime episode numbering before writing progress to SIMKL, using the local Anime ID database.\nRequires global Anime ID Mapping.\nThis can heavily increase API calls when additional anime ID and episode lookups are needed.",
  "cx-rt-anime-only": "Anime-only sync\nOnly sync ratings for items that Anime ID Mapping can confirm as anime. Non-anime and unmapped items are skipped before falling back to a title search.",

  "cx-hs-enable": "History: Enable\nCompare and write watch history to the target.",
  "cx-hs-add": "History: Add\nAdds plays/watched items to the target history.",
  "cx-hs-remove": "History: Remove\nRemoving history is discouraged (destructive and only for very specific needs).",
  "cx-hs-rewatches": "History: Rewatches\nSync separate play events when both providers support it. SIMKL requires Pro/VIP.",
  "cx-tr-hs-col": "Trakt: Add to library\nAlso add items to your Trakt library when writing history (if enabled).",
  "cx-hs-dropped": "Dropped shows\nSync normally: dropped shows sync like any other show.\nIgnore dropped: shows dropped on either provider are skipped during history sync. Nothing is removed elsewhere.\nSync + tag dropped: plays of dropped shows still sync, and the show is marked dropped on the other provider. One-way copies the source status to the target, two-way works both ways. SIMKL and WeTrakr keep completed shows as completed. Dropping a show on FlickList also clears its resume points. Un-dropping is not synced.",

  "cx-pr-enable": "Progress: Enable\nSync resume position (where you left off) between providers.",
  "cx-pr-add": "Progress: Add / Update\nWrite resume position to the target.",
  "cx-pr-remove": "Progress: Remove\nClear resume position on the target (rare; Plex may not support).",
  "cx-pr-min": "Progress: Minimum seconds\nIgnore tiny offsets (scrubbing).",
  "cx-pr-delta": "Progress: Change threshold\nOnly write when the difference is large enough.",
  "cx-pr-maxp": "Progress: Ignore near complete (%)\nOne-way defaults to the target recommendation. Two-way keeps one shared limit and shows per-target recommendations below.",
  "cx-pr-replay": "Replay watched items\nUnwatch the target, then apply resume progress.",
  "cx-pr-tolerance": "Timestamp tolerance\nProtect targets newer by more than this many seconds.",
  "cx-pr-specials": "Progress: Specials (Season 0)\nInclude Season 0 / specials episodes. Disable to skip them on both sides of this pair.",
  "cx-pr-library-only": "Progress: Only items in destination library\nSkip resume positions for movies and episodes that are not in the media server library.\nTitles the server already has a position for are always synced.\nMatches on IDs only, so titles stored under a different ID are skipped too.",
  "cx-hs-specials": "History: Specials (Season 0)\nInclude Season 0 / specials episodes. Disable to skip them on both sides of this pair.",
  "cx-hs-library-only": "History: Only items in destination library\nSkip movies and episodes that are not in the media server library.\nMatches on IDs only, so titles stored under a different ID are skipped too.",
  "cx-hs-undated-today": "History: Undated watches as today (SIMKL)\nMyAnimeList and Kitsu only store episode counts, not watch dates. SIMKL has no 'date unknown' option, so it records these watches with today's date.\nWhen off, undated watches are skipped for SIMKL and listed as Missing watched date.",
  "cx-co-media": "Collections: Read from media server\nReads resolution, HDR and audio from the media server and sends them with new collection items.\nAdds some extra load on the media server while reading the library.",
  "cx-co-media-upd": "Collections: Update existing items\nWhen the media info of an item changes on the source (for example 1080p to 4K, or a new Atmos track), the item on the destination is updated too.\nOnly fills in or changes values, never clears them. Physical copies on the destination are left alone.\nThe first sync can update many items.",
  "cx-rt-specials": "Ratings: Specials (Season 0)\nInclude Season 0 / specials seasons and episodes. Disable to skip them on both sides of this pair.",
  "cx-rt-library-only": "Ratings: Only items in destination library\nSkip new ratings for movies and episodes that are not in the media server library.\nRating changes for titles you already rated are always synced.\nMatches on IDs only, so titles stored under a different ID are skipped too.",

  "floppy-anime-library": "Floppy: Send new anime to the Anime library\nNew anime shows are added to Floppy's Anime library instead of TV Shows.\nOnly applies to shows Floppy does not have yet. Shows already in Floppy stay in the library they are in.\nThis setting is per pair.",

  "cx-jf-wl-mode": "Jellyfin: Watchlist mode\nJellyfin has no native Watchlist. CrossWatch maps it to:\n• Favorites: sets the Favorite flag\n• Playlist: writes to a named playlist (episodes only; no shows)\n• Collections: writes to a named collection\nChanging mode does not move existing items.\nTip: Favorites or Collections are the most compatible.",
  "cx-em-wl-mode": "Emby: Watchlist mode\nEmby has no native Watchlist. CrossWatch maps it to:\n• Favorites: sets the Favorite flag\n• Playlist: writes to a named playlist (episodes only; no shows)\n• Collections: writes to a named collection\nChanging mode does not move existing items.\nTip: Favorites or Collections are the most compatible.",

  "plx-marked-watched": "Plex: Marked watched\nInclude watched items from your Plex library, even when no playback-history entry is available. Applies only to this pair.",
  "plx-cloud-read": "Plex cloud history: Plex to provider\nReads server and account watch history, including titles outside the server libraries. Server library filters do not limit these account-wide cloud reads. In two-way pairs, Read cloud history is independent of Write cloud fallback.\nHistory removals are disabled while enabled. Unavailable cloud reads fail the feature instead of returning partial history. Deleted or unmatchable catalog entries are skipped and counted in the read log. Not supported for managed or switched users.",
  "plx-discover-fallback": "Plex cloud history: Provider to Plex\nWrites to your Plex server normally. For missing movies and episodes, uses your Plex account to mark them watched and restore their original watch date. Applies to additions only; existing cloud plays stay unchanged. Rate-limited or unfinished date updates stay unresolved and are retried. In two-way pairs, Write cloud fallback is independent of Read cloud history.\nTurning this on turns off Only items in destination library for this pair.\nOnly for a regular Plex account connected with its own token. Not for managed users or switched Plex Home and shared users.",
  "plx-wl-pms": "Plex watchlist: Allow PMS fallback\nThe Plex watchlist lives in your Plex account, so CrossWatch first finds each title in the Plex online catalogue (Discover).\nWhen that fails, look the title up by ID in your own Plex server library and add it to the watchlist from there.\nHelps for titles that Discover cannot find, but only works for titles that are in your library. Off by default.",
  "plx-wl-title": "Plex watchlist: Title text search\nWhen the ID lookup in the Plex online catalogue finds nothing, search it by title, year or slug.\nFinds more titles, with a small risk of a wrong match for remakes or titles with the same name.\nTurn it off to add titles by ID only; titles without a match then stay unresolved. On by default.",
  "plx-strict-ids": "Plex: Strict ID matching\nWhen enabled, CrossWatch only matches by IDs (Plex IDs + external IDs). Title/year searches are disabled. The pair Match by file path option can separately enable a file-path fallback.",
  "jf-strict-ids": "Jellyfin: Strict ID matching\nRequire Jellyfin or external IDs to confirm matches. Title searches and legacy full-path matching are disabled. The pair Match by file path option can separately enable a file-path fallback.\nEpisodes can match by series ID plus season and episode numbers.",
  "jf-targeted-lookup": "Jellyfin: Targeted library lookup\nMatch through a cached movie and series ID catalogue, then fetch episodes only for matched series.\nThe catalogue checks for metadata changes on each sync that needs matching and refreshes fully every 24 hours.\nDisable to allow a full episode index. Title and path fallback require Strict ID matching to be off.",
  "em-strict-ids": "Emby: Strict ID matching\nWhen enabled, CrossWatch only matches by IDs (Emby IDs + external IDs). Title/year searches are disabled. The pair Match by file path option can separately enable a file-path fallback.",
  "silo-strict-ids": "Silo: Strict ID matching\nOn by default. Only match by TMDB, IMDb or TVDB IDs. Turn it off to also match by title: movies by title and year, shows by title and episodes by show title plus season and episode number. A title must point to exactly one Silo item and must not contradict known IDs. The pair Match by file path option can separately enable a file-path fallback.",
};

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
    btn.setAttribute("aria-label", "Help");
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
    btn.setAttribute("aria-label", "Help");
    btn.addEventListener("click", (e) => { e.preventDefault(); e.stopPropagation(); });
    btn.addEventListener("mousedown", (e) => { e.preventDefault(); e.stopPropagation(); });
  });
}
