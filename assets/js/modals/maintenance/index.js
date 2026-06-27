/* assets/js/modals/maintenance/index.js */
/* refactored */
/* Modal for maintenance and troubleshooting operations like clearing state, cache, tracker data, and resetting stats. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const fjson = async (url, opts = {}) => {
  const r = await fetch(url, { cache: "no-store", ...opts });
  if (!r.ok) {
    const msg = `${r.status} ${r.statusText || ""}`.trim();
    throw new Error(msg || "Request failed");
  }
  if (r.status === 204) return {};
  try {
    return await r.json();
  } catch {
    return {};
  }
};

const $ = (sel, root = document) => root.querySelector(sel);
const post = (url, body) =>
  fjson(url, body === undefined ? { method: "POST" } : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
const SIMPLE_OPS = {
  state: "/api/maintenance/clear-state",
  cache: "/api/maintenance/clear-cache",
  metadata: "/api/maintenance/clear-metadata-cache",
  activity: "/api/maintenance/clear-activity-log",
  scrobbles: "/api/maintenance/clear-recent-scrobbles",
  stats: "/api/maintenance/reset-stats",
  playing: "/api/maintenance/reset-currently-watching",
};
const OPS = [
  {
    key: "state",
    kind: "state",
    icon: "deployed_code_history",
    title: "Rebuild sync state",
    tag: "sync pairs",
    desc: 'Starts every sync pair from fresh provider baselines on its next run.',
  },
  {
    key: "cache",
    kind: "cache",
    icon: "network_node",
    title: "Retry provider items",
    tag: "runtime",
    desc: 'Clears temporary retry and health data so unresolved provider items are tried again.',
  },
  {
    key: "meta",
    kind: "metadata",
    icon: "gallery_thumbnail",
    title: "Refresh artwork & metadata",
    tag: "artwork",
    desc: 'Removes cached artwork and metadata so fresh copies are fetched when needed.',
  },
  {
    key: "tracker",
    kind: "tracker",
    icon: "deployed_code",
    title: "Reset local tracker",
    tag: "local library",
    desc: 'Clears local Watchlist, History and Ratings tracker data. Provider accounts stay untouched.',
    extra: `
      <div class="action-options">
        <label><input type="checkbox" id="cxm-cw-state" checked><span>Tracker state files</span></label>
        <label><input type="checkbox" id="cxm-cw-snaps"><span>All snapshots</span></label>
      </div>
    `,
  },
  {
    key: "activity",
    kind: "activity",
    icon: "fact_check",
    title: "Clear all activity",
    tag: "local activity",
    desc: "Clears the complete local activity list without changing provider watch history.",
  },
  {
    key: "scrobbles",
    kind: "scrobbles",
    icon: "podcasts",
    title: "Clear Recent Scrobbles",
    tag: "scrobbles only",
    desc: "Clears only the local Recent Scrobble list while keeping other Recent Activity entries.",
  },
  {
    key: "stats",
    kind: "stats",
    icon: "monitoring",
    title: "Rebuild statistics",
    tag: "stats & reports",
    desc: "Rebuilds Statistics, Reports and Insights from clean local data.",
  },
  {
    key: "playing",
    kind: "playing",
    icon: "live_tv",
    title: "Clear currently playing",
    tag: "playback",
    desc: 'Removes stuck items from the local Currently Playing list.',
  },
  {
    key: "defaults",
    kind: "defaults",
    icon: "release_alert",
    title: "Factory reset",
    tag: "danger zone",
    desc: "Returns CrossWatch to a clean install and backs up config.json. Snapshots are kept.",
  },
];
const renderActionRow = ({ key, icon, title, tag, desc, extra = "" }) => `
  <div class="action-row" data-op="${key}">
    <div class="action-main">
      <div class="action-icon">
        <span class="material-symbols-rounded" aria-hidden="true">${icon}</span>
      </div>
      <div class="action-copy">
        <div class="action-line">
          <div class="action-title">${title}</div>
          ${tag ? `<span class="action-tag">${tag}</span>` : ""}
        </div>
        <div class="action-desc">${desc}</div>
        ${extra}
      </div>
    </div>
    <button type="button" class="run-btn" data-label="${title}">Run</button>
  </div>
`;

function injectCSS() {
  if (document.getElementById("cw-maint-css")) return;
  const el = document.createElement("style");
  el.id = "cw-maint-css";
  el.textContent = `
  .cw-maint {
    position: relative;
    display: flex;
    flex-direction: column;
    height: auto;
    max-height: min(80vh, calc(100vh - 40px));
    background:
      radial-gradient(96% 125% at 0% 0%, rgba(72,52,146,.18), transparent 36%),
      linear-gradient(180deg, rgba(5,7,14,.99), rgba(3,5,11,.99));
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 22px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.025), 0 28px 60px rgba(0,0,0,.32);
  }
  .cx-modal-shell.cw-maint-shell {
    height: auto !important;
    max-height: min(80vh, calc(100vh - 40px)) !important;
  }

  .cw-maint .cx-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 12px 14px 10px;
    border-bottom: 1px solid rgba(255,255,255,.06);
    background: linear-gradient(180deg,rgba(255,255,255,.016),rgba(255,255,255,.003));
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
  }
  .cw-maint .cx-head-left {
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 0;
  }
  .cw-maint .head-icon {
    width: 36px;
    height: 36px;
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: linear-gradient(145deg,rgba(18,23,42,.96),rgba(10,15,28,.94));
    border: 1px solid rgba(124,138,255,.16);
    box-shadow: inset 0 1px 0 rgba(255,255,255,.035),0 16px 28px rgba(0,0,0,.24);
    flex-shrink: 0;
  }
  .cw-maint .head-icon .material-symbols-rounded {
    font-variation-settings:"FILL" 0,"wght" 550,"GRAD" 0,"opsz" 24;
    font-size: 20px;
    color: #b9c6ff;
  }
  .cw-maint .head-text {
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
  }
  .cw-maint .head-title {
    font-weight: 850;
    font-size: 16px;
    letter-spacing: -.01em;
    color: #f4f7ff;
  }
  .cw-maint .head-sub {
    font-size: 12px;
    color: rgba(197,206,224,.72);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .cw-maint .cx-head-right {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    justify-content: flex-end;
  }

  .cw-maint .close-btn {
    border: 1px solid rgba(255,255,255,.09);
    background: linear-gradient(180deg,rgba(18,22,38,.92),rgba(10,13,24,.9));
    color: #eef3ff;
    border-radius: 999px;
    padding: 7px 14px;
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .06em;
    cursor: pointer;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.025),0 12px 22px rgba(0,0,0,.22);
  }
  .cw-maint .close-btn:hover {
    background: linear-gradient(180deg,rgba(24,30,52,.96),rgba(12,16,30,.94));
    border-color: rgba(255,255,255,.13);
  }

  .cw-maint .cx-body {
    flex: 1;
    min-height: 0;
    padding: 10px 12px 12px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    overflow: auto;
  }

  .cw-maint .summary-card {
    background: linear-gradient(135deg,rgba(81,91,190,.12),rgba(21,27,52,.45));
    border-radius: 16px;
    border: 1px solid rgba(255,255,255,.07);
    padding: 9px 11px;
    display: grid;
    grid-template-columns: minmax(240px,1fr) auto auto;
    align-items: center;
    gap: 12px;
    font-size: 12px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.025);
  }
  @media (max-width: 980px) {
    .cw-maint .summary-card {
      grid-template-columns: minmax(0,1fr) auto;
    }
    .cw-maint .storage-details { grid-column: 1 / -1; }
  }
  .cw-maint .summary-safe {
    display: grid;
    grid-template-columns: 32px minmax(0,1fr);
    align-items: center;
    gap: 9px;
  }
  .cw-maint .summary-safe-icon {
    width: 32px;
    height: 32px;
    display: grid;
    place-items: center;
    border-radius: 11px;
    border: 1px solid rgba(124,242,176,.16);
    background: rgba(55,178,126,.09);
    color: #bdf0d0;
  }
  .cw-maint .summary-safe-icon .material-symbols-rounded { font-size: 19px; }
  .cw-maint .summary-safe strong { display:block;color:#f5f7ff;font-size:12px; }
  .cw-maint .summary-safe span { display:block;margin-top:2px;color:rgba(205,214,231,.7);font-size:11px; }
  .cw-maint .storage-details { position: relative; }
  .cw-maint .storage-details summary {
    list-style: none;
    cursor: pointer;
    padding: 6px 9px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.08);
    background: rgba(255,255,255,.03);
    color: rgba(224,230,242,.8);
    font-size: 10px;
    font-weight: 800;
    letter-spacing: .06em;
    text-transform: uppercase;
    white-space: nowrap;
  }
  .cw-maint .storage-details summary::-webkit-details-marker { display:none; }
  .cw-maint .storage-details[open] .summary-paths {
    display:block;
    position:absolute;
    z-index:8;
    right:0;
    top:calc(100% + 6px);
    min-width:310px;
    padding:9px 10px;
    border-radius:13px;
    border:1px solid rgba(255,255,255,.1);
    background:#111521;
    box-shadow:0 16px 34px rgba(0,0,0,.34);
  }
  .cw-maint .summary-paths { display:none;line-height:1.7;color:rgba(205,214,231,.76); }
  .cw-maint .summary-paths code {
    font-size: 10px;
    background: rgba(255,255,255,.035);
    padding: 1px 5px;
    border-radius: 999px;
  }
  .cw-maint .summary-badges {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .cw-maint .summary-pill {
    padding: 4px 9px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.08);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: .08em;
    opacity: .9;
    background: linear-gradient(180deg,rgba(15,18,34,.95),rgba(9,12,22,.93));
    box-shadow: inset 0 1px 0 rgba(255,255,255,.03);
  }

  .cw-maint .actions {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 7px;
    margin-top: 0;
  }
  @media (max-width: 1100px) {
    .cw-maint .actions {
      grid-template-columns: repeat(2,minmax(0,1fr));
    }
  }
  @media (max-width: 720px) { .cw-maint .actions { grid-template-columns:1fr; } }

  .cw-maint .action-row {
    background: radial-gradient(120% 145% at 0% 0%,rgba(76,54,150,.08),transparent 38%),linear-gradient(180deg,rgba(9,12,22,.97),rgba(5,8,17,.965));
    border-radius: 15px;
    border: 1px solid rgba(255,255,255,.07);
    padding: 9px 10px;
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: center;
    gap: 8px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
    transition: transform .14s ease,border-color .14s ease,box-shadow .16s ease,background .16s ease;
  }
  .cw-maint .action-row:hover {
    transform: translateY(-1px);
    border-color: rgba(130,116,220,.16);
    box-shadow: 0 12px 24px rgba(0,0,0,.18), inset 0 1px 0 rgba(255,255,255,.03);
  }
  .cw-maint .action-main {
    display: grid;
    grid-template-columns: 28px minmax(0, 1fr);
    align-items: start;
    gap: 9px;
    flex: 1;
    min-width: 0;
  }
  .cw-maint .action-icon {
    width: 28px;
    height: 28px;
    border-radius: 9px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    border: 1px solid rgba(255,255,255,.08);
    background: linear-gradient(145deg,rgba(13,16,31,.98),rgba(8,10,20,.97));
    box-shadow: inset 0 1px 0 rgba(255,255,255,.025),0 14px 22px rgba(0,0,0,.22);
  }
  .cw-maint .action-icon .material-symbols-rounded {
    font-variation-settings:"FILL" 0,"wght" 450,"GRAD" 0,"opsz" 20;
    font-size: 18px;
    color: #eef3ff;
  }

  .cw-maint .action-copy {
    display: flex;
    flex-direction: column;
    gap: 3px;
    min-width: 0;
  }
  .cw-maint .action-line {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
    min-width: 0;
  }
  .cw-maint .action-title {
    font-size: 12px;
    font-weight: 800;
    line-height: 1.2;
  }
  .cw-maint .action-tag {
    padding: 2px 6px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.08);
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: .09em;
    opacity: .86;
    background: rgba(255,255,255,.028);
  }
  .cw-maint .action-desc {
    font-size: 10.5px;
    line-height: 1.3;
    color: rgba(197,206,224,.76);
  }
  .cw-maint .action-desc code {
    font-size: 10px;
    background: rgba(255,255,255,.035);
    border-radius: 999px;
    padding: 1px 6px;
  }

  .cw-maint .action-options {
    width: 100%;
    margin-top: 4px;
    font-size: 10px;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 3px 10px;
    align-items: start;
  }
  .cw-maint .action-options label {
    display: grid;
    grid-template-columns: 16px minmax(0, 1fr);
    align-items: start;
    gap: 8px;
    width: 100%;
    cursor: pointer;
  }
  .cw-maint .action-options label span {
    min-width: 0;
    white-space: normal;
    line-height: 1.15;
  }
  .cw-maint .action-options input {
    margin: 2px 0 0;
    accent-color: #7e79ff;
  }
  @media (max-width: 720px) {
    .cw-maint .action-options {
      grid-template-columns: 1fr;
    }
  }

  .cw-maint .run-btn {
    align-self: start;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.09);
    padding: 6px 11px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: .09em;
    text-transform: uppercase;
    cursor: pointer;
    background: linear-gradient(180deg,rgba(15,18,34,.95),rgba(9,12,22,.93));
    color: #fff;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.025),0 12px 24px rgba(0,0,0,.22);
    flex-shrink: 0;
  }
  .cw-maint .run-btn:hover {
    border-color: rgba(132,120,240,.22);
    background: linear-gradient(180deg,rgba(33,28,66,.98),rgba(13,16,30,.95));
  }
  .cw-maint .run-btn[disabled] {
    opacity: .6;
    cursor: wait;
    box-shadow: none;
  }
  .cw-maint .action-row[data-op="state"] .action-icon {
    border-color: rgba(255,134,145,.16);
    background: rgba(126,121,255,.10);
  }
  .cw-maint .action-row[data-op="cache"] .action-icon {
    border-color: rgba(164,138,255,.18);
    background: rgba(164,138,255,.10);
  }
  .cw-maint .action-row[data-op="meta"] .action-icon {
    border-color: rgba(255,201,110,.18);
    background: rgba(255,201,110,.09);
  }
  .cw-maint .action-row[data-op="tracker"] .action-icon {
    border-color: rgba(115,197,255,.18);
    background: rgba(115,197,255,.09);
  }
  .cw-maint .action-row[data-op="stats"] .action-icon {
    border-color: rgba(120,220,176,.18);
    background: rgba(120,220,176,.09);
  }
  .cw-maint .action-row[data-op="playing"] .action-icon {
    border-color: rgba(190,196,255,.16);
    background: rgba(190,196,255,.08);
  }
  .cw-maint .action-row[data-op="activity"] .action-icon {
    border-color: rgba(124,242,176,.16);
    background: rgba(124,242,176,.08);
  }
  .cw-maint .action-row[data-op="scrobbles"] .action-icon {
    border-color: rgba(104,191,255,.18);
    background: rgba(104,191,255,.09);
  }

  .cw-maint .action-row[data-op="defaults"] .action-icon {
    border-color: rgba(255,106,106,.18);
    background: rgba(255,106,106,.10);
  }
  .cw-maint .action-row[data-op="defaults"] {
    border-color: rgba(255,106,106,.14);
    background: linear-gradient(145deg,rgba(75,27,40,.18),rgba(17,12,20,.86));
  }

  .cw-maint .status {
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 38px;
    margin-top: 0;
    padding: 7px 10px;
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,.07);
    background: radial-gradient(120% 140% at 0% 0%,rgba(74,54,150,.06),transparent 38%),linear-gradient(180deg,rgba(9,12,22,.97),rgba(6,8,16,.96));
    box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
    position: sticky;
    bottom: 0;
    z-index: 5;
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
  }
  .cw-maint .status::before {
    content: "STATUS";
    flex-shrink: 0;
    padding: 4px 9px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.08);
    background: linear-gradient(180deg,rgba(15,18,33,.96),rgba(9,11,21,.95));
    color: rgba(222,229,245,.92);
    font-size: 10px;
    font-weight: 800;
    letter-spacing: .12em;
    text-transform: uppercase;
  }
  .cw-maint .status-text {
    min-width: 0;
    color: rgba(226,232,245,.88);
    font-size: 12px;
    line-height: 1.35;
  }
  .cw-maint .status.ok::before {
    content: "DONE";
    color: #bdf0d0;
    border-color: rgba(124,242,176,.16);
  }
  .cw-maint .status.err::before {
    content: "ERROR";
    color: #ffc4c4;
    border-color: rgba(255,148,148,.18);
  }
  .cw-maint .status.busy::before {
    content: "RUNNING";
    color: #cfd5ff;
    border-color: rgba(126,121,255,.18);
  }
  #cw-clean-all {
    border-radius: 999px;
    border: 1px solid rgba(255,132,146,.14);
    padding: 7px 14px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .09em;
    text-transform: uppercase;
    cursor: pointer;
    background: linear-gradient(180deg,rgba(58,20,31,.94),rgba(37,12,21,.92));
    color: #fff;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.02),0 12px 24px rgba(0,0,0,.22);
    margin-right: 8px; /* small gap before CLOSE */
  }
  #cw-clear-provider-cache {
    border-radius: 999px;
    border: 1px solid rgba(139,149,255,.2);
    padding: 7px 14px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .09em;
    text-transform: uppercase;
    cursor: pointer;
    background: linear-gradient(180deg,rgba(35,39,78,.94),rgba(20,24,50,.92));
    color: #fff;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.03),0 12px 24px rgba(0,0,0,.2);
  }
  #cw-clear-provider-cache:hover {
    border-color: rgba(154,162,255,.3);
    background: linear-gradient(180deg,rgba(47,52,101,.96),rgba(25,30,61,.94));
  }
  #cw-clear-provider-cache[disabled] {
    opacity: .6;
    cursor: wait;
    box-shadow: none;
  }
  #cw-clean-all:hover {
    border-color: rgba(255,146,160,.22);
    background: linear-gradient(180deg,rgba(72,26,39,.96),rgba(44,15,25,.94));
  }
  #cw-clean-all[disabled] {
    opacity: .6;
    cursor: wait;
    box-shadow: none;
  }
  html[data-cw-theme="flat-dark"] .cw-maint,
  html[data-cw-theme="flat-dark"] .cw-maint .cx-head,
  html[data-cw-theme="flat-dark"] .cw-maint .head-icon,
  html[data-cw-theme="flat-dark"] .cw-maint .close-btn,
  html[data-cw-theme="flat-dark"] .cw-maint .summary-card,
  html[data-cw-theme="flat-dark"] .cw-maint .summary-pill,
  html[data-cw-theme="flat-dark"] .cw-maint .action-row,
  html[data-cw-theme="flat-dark"] .cw-maint .action-desc code,
  html[data-cw-theme="flat-dark"] .cw-maint .action-options label,
  html[data-cw-theme="flat-dark"] .cw-maint .action-options input,
  html[data-cw-theme="flat-dark"] .cw-maint .run-btn,
  html[data-cw-theme="flat-dark"] .cw-maint .status,
  html[data-cw-theme="flat-dark"] .cw-maint .status::before,
  html[data-cw-theme="flat-dark"] #cw-clear-provider-cache,
  html[data-cw-theme="flat-dark"] #cw-clean-all {
    background: #20242d !important;
    border-color: rgba(255,255,255,.14) !important;
    box-shadow: none !important;
    text-shadow: none !important;
    filter: none !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint .close-btn:hover,
  html[data-cw-theme="flat-dark"] .cw-maint .action-row:hover,
  html[data-cw-theme="flat-dark"] .cw-maint .run-btn:hover,
  html[data-cw-theme="flat-dark"] #cw-clear-provider-cache:hover,
  html[data-cw-theme="flat-dark"] #cw-clean-all:hover {
    background: #2b313d !important;
    border-color: rgba(255,255,255,.19) !important;
    box-shadow: none !important;
    filter: none !important;
    transform: none !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint .status.ok::before,
  html[data-cw-theme="flat-dark"] .cw-maint .status.err::before,
  html[data-cw-theme="flat-dark"] .cw-maint .status.busy::before {
    box-shadow: none !important;
    filter: none !important;
    text-shadow: none !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint #cw-clean-all,
  html[data-cw-theme="flat-dark"] #cw-clean-all {
    background: #43272e !important;
    border-color: rgba(216,102,114,.42) !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint #cw-clear-provider-cache,
  html[data-cw-theme="flat-dark"] #cw-clear-provider-cache {
    background: #292f4c !important;
    border-color: rgba(128,139,226,.4) !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint .action-row[data-op="defaults"] {
    background:#35252a !important;
    border-color:rgba(216,102,114,.3) !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint .cx-body {
    scrollbar-color: #3a414c #151821 !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint .cx-body::-webkit-scrollbar-track {
    background: #151821 !important;
  }
  html[data-cw-theme="flat-dark"] .cw-maint .cx-body::-webkit-scrollbar-thumb {
    background: #3a414c !important;
    border-color: #151821 !important;
    box-shadow: none !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint,
  html[data-cw-theme="flat-light"] .cw-maint .cx-head,
  html[data-cw-theme="flat-light"] .cw-maint .head-icon,
  html[data-cw-theme="flat-light"] .cw-maint .close-btn,
  html[data-cw-theme="flat-light"] .cw-maint .summary-card,
  html[data-cw-theme="flat-light"] .cw-maint .summary-pill,
  html[data-cw-theme="flat-light"] .cw-maint .action-row,
  html[data-cw-theme="flat-light"] .cw-maint .action-desc code,
  html[data-cw-theme="flat-light"] .cw-maint .action-options label,
  html[data-cw-theme="flat-light"] .cw-maint .action-options input,
  html[data-cw-theme="flat-light"] .cw-maint .run-btn,
  html[data-cw-theme="flat-light"] .cw-maint .status,
  html[data-cw-theme="flat-light"] .cw-maint .status::before {
    background: #ffffff !important;
    border-color: rgba(21,31,48,.14) !important;
    color: #172033 !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint .close-btn:hover,
  html[data-cw-theme="flat-light"] .cw-maint .action-row:hover,
  html[data-cw-theme="flat-light"] .cw-maint .run-btn:hover {
    background: #eef2f7 !important;
    border-color: rgba(21,31,48,.20) !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint #cw-clean-all,
  html[data-cw-theme="flat-light"] #cw-clean-all {
    background: #f7dde2 !important;
    border-color: rgba(201,79,97,.36) !important;
    color: #7f1d2d !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint #cw-clear-provider-cache,
  html[data-cw-theme="flat-light"] #cw-clear-provider-cache {
    background: #e8ebff !important;
    border-color: rgba(80,91,184,.3) !important;
    color: #313b82 !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint .summary-safe strong { color:#172033; }
  html[data-cw-theme="flat-light"] .cw-maint .summary-safe span { color:#667085; }
  html[data-cw-theme="flat-light"] .cw-maint .summary-safe-icon {
    background:#e9f8f0;
    border-color:rgba(31,143,91,.2);
    color:#207a50;
  }
  html[data-cw-theme="flat-light"] .cw-maint .action-icon .material-symbols-rounded { color:#344054; }
  html[data-cw-theme="flat-light"] .cw-maint .action-row[data-op="defaults"] {
    background:#fff4f5 !important;
    border-color:rgba(201,79,97,.24) !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint .storage-details summary {
    background:#f5f7fb;
    border-color:rgba(21,31,48,.14);
    color:#475467;
  }
  html[data-cw-theme="flat-light"] .cw-maint .storage-details[open] .summary-paths {
    background:#fff;
    border-color:rgba(21,31,48,.14);
    color:#475467;
    box-shadow:0 16px 34px rgba(15,23,42,.14);
  }
  html[data-cw-theme="flat-light"] .cw-maint .cx-body {
    scrollbar-color: #c4ccd8 #eef2f7 !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint .cx-body::-webkit-scrollbar-track {
    background: #eef2f7 !important;
  }
  html[data-cw-theme="flat-light"] .cw-maint .cx-body::-webkit-scrollbar-thumb {
    background: #c4ccd8 !important;
    border-color: #eef2f7 !important;
  }
  `;
  document.head.appendChild(el);
}

export default {
  async mount(root) {
    injectCSS();

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("cw-maint-shell");
      shell.style.setProperty("--cxModalW", "1180px");
      shell.style.setProperty("--cxModalMaxW", "1180px");
      shell.style.setProperty("--cxModalMaxH", "80vh");
    }
    const cleanAllOps = [
      () => post("/api/maintenance/clear-provider-sync-cache"),
      () => post(SIMPLE_OPS.metadata),
      () => post(SIMPLE_OPS.activity),
      () => post("/api/maintenance/crosswatch-tracker/clear", { clear_state: true, clear_snapshots: true }),
      () => post(SIMPLE_OPS.stats, {}),
      () => post(SIMPLE_OPS.playing),
    ];

    root.innerHTML = `
      <div class="cw-maint">
        <div class="cx-head">
          <div class="cx-head-left">
            <div class="head-icon">
              <span class="material-symbols-rounded" aria-hidden="true">tune</span>
            </div>
            <div class="head-text">
              <div class="head-title">Maintenance tools</div>
              <div class="head-sub">Reset, rebuild or clean the local CrossWatch layers without touching provider accounts</div>
            </div>
          </div>
          <div class="cx-head-right">
            <button id="cw-clear-provider-cache" type="button">Clear Provider Sync Cache</button>
            <button id="cw-clean-all" class="cw-btn danger">Clean Everything</button>
            <button type="button" class="close-btn" id="cxm-close">Close</button>
          </div>
        </div>

        <div class="cx-body">
          <div class="summary-card">
            <div class="summary-safe">
              <div class="summary-safe-icon"><span class="material-symbols-rounded" aria-hidden="true">verified_user</span></div>
              <div>
                <strong>Local cleanup only</strong>
                <span>These tools do not delete data from provider accounts.</span>
              </div>
            </div>
            <div class="summary-badges">
              <span class="summary-pill" id="cxm-tracker-count">Tracker: -</span>
              <span class="summary-pill" id="cxm-cache-count">Provider cache: -</span>
            </div>
            <details class="storage-details">
              <summary>Storage details</summary>
              <div class="summary-paths">
                Tracker: <code id="cxm-tracker-root">/config/.cw_provider</code><br>
                Provider cache: <code id="cxm-cache-root">/config/.cw_state</code>
              </div>
            </details>
          </div>

          <div class="actions">
            ${OPS.map(renderActionRow).join("")}
          </div>

          <div id="cxm-status" class="status"><span class="status-text">Select a maintenance action.</span></div>
        </div>
      </div>
    `;

    const statusEl = $("#cxm-status", root);
    const setStatus = (msg, kind = "") => {
      if (!statusEl) return;
      statusEl.innerHTML = `<span class="status-text">${msg}</span>`;
      statusEl.className = "status" + (kind ? " " + kind : "");
    };

    $("#cxm-close", root)?.addEventListener("click", () => {
      if (window.cxCloseModal) window.cxCloseModal();
    });

    async function refreshSummary() {
      try {
        const [tracker, cache] = await Promise.all([
          fjson("/api/maintenance/crosswatch-tracker").catch(() => null),
          fjson("/api/maintenance/provider-cache").catch(() => null),
        ]);

        const trackerRoot = tracker?.root || "/config/.cw_provider";
        const cacheRoot = cache?.root || "/config/.cw_state";

        $("#cxm-tracker-root", root).textContent = trackerRoot;
        $("#cxm-cache-root", root).textContent = cacheRoot;

        const tCounts = tracker?.counts || {};
        const tState = tCounts.state_files ?? "-";
        const tSnap = tCounts.snapshots ?? "-";
        $("#cxm-tracker-count", root).textContent =
          `Tracker: ${tState} state - ${tSnap} snapshots`;

        const cCount = cache?.count ?? "-";
        $("#cxm-cache-count", root).textContent =
          `Provider cache: ${cCount} file${cCount === 1 ? "" : "s"}`;
      } catch {

      }
    }

    async function runOp(kind, btn) {
      if (btn) btn.disabled = true;
      const label = btn?.dataset?.label || kind;
      setStatus(`Running ${label.toLowerCase()}...`, "busy");

      try {
        let res = null;
        if (SIMPLE_OPS[kind]) {
          res = await post(SIMPLE_OPS[kind], kind === "stats" ? {} : undefined);
        } else if (kind === "tracker") {
          const chkState = $("#cxm-cw-state", root);
          const chkSnaps = $("#cxm-cw-snaps", root);
          const clearState = !!(chkState && chkState.checked);
          const clearSnaps = !!(chkSnaps && chkSnaps.checked);

          if (!clearState && !clearSnaps) {
            setStatus("Select at least one option for tracker cleanup.", "err");
            return;
          }

          res = await post("/api/maintenance/crosswatch-tracker/clear", {
            clear_state: clearState,
            clear_snapshots: clearSnaps,
          });
        } else if (kind === "defaults") {
          const warn = [
            "WARNING Reset all to default",
            "",
            "This will delete local state, provider cache, tracker files, reports, metadata cache and TLS material.",
            "It will also move /config/config.json to a timestamped backup file.",
            "",
            "Snapshots are NOT deleted ( /config/snapshots ).",
            "",
            "Are you absolutely sure you want to continue?"
          ].join("\n");

          if (!confirm(warn)) return;

          const typed = prompt('Type RESET to continue');
          if (String(typed || "").trim().toUpperCase() !== "RESET") {
            setStatus("Cancelled.", "");
            return;
          }

          res = await post("/api/maintenance/reset-all-default", {});

          // Restart with overlay/timer (restart_apply.js)
          try {
            if (window.cxCloseModal) window.cxCloseModal();
          } catch (_) {}

          setTimeout(() => {
            if (window.cwRestartCrossWatchWithOverlay) {
              window.cwRestartCrossWatchWithOverlay();
            } else {
              fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" }).finally(() => {
                window.location.reload();
              });
            }
          }, 150);
          return;
        }

        if (kind === "activity" || kind === "scrobbles") {
          try { window.dispatchEvent(new CustomEvent("activity-log-cleared")); } catch {}
        }

        if (kind === "cache" || kind === "tracker") {
          await refreshSummary();
        }

        if (res && res.ok === false) {
          setStatus(`Failed: ${res.error || "Unknown error"}`, "err");
        } else {
          setStatus(`${label} completed.`, "ok");
        }
      } catch (e) {
        setStatus(`Error: ${e.message || String(e)}`, "err");
      } finally {
        if (btn) btn.disabled = false;
      }
    }

    OPS.forEach(({ key, kind }) => {
      const row = root.querySelector(`.action-row[data-op="${key}"]`);
      const btn = row?.querySelector(".run-btn");
      if (btn) btn.addEventListener("click", () => runOp(kind, btn));
    });

    const clearProviderCacheBtn = root.querySelector("#cw-clear-provider-cache");
    if (clearProviderCacheBtn) {
      clearProviderCacheBtn.addEventListener("click", async () => {
        if (!confirm("Clear provider sync baselines and runtime cache so every sync pair starts fresh? Local tracker data, metadata and recent activity will be kept.")) {
          return;
        }
        clearProviderCacheBtn.disabled = true;
        const originalText = clearProviderCacheBtn.textContent;
        clearProviderCacheBtn.textContent = "Clearing...";
        setStatus("Clearing provider sync cache...", "busy");
        try {
          const res = await post("/api/maintenance/clear-provider-sync-cache");
          if (res?.ok === false) throw new Error("Provider sync cache could not be fully cleared");
          await refreshSummary();
          setStatus("Provider sync cache cleared. Sync pairs will rebuild on their next run.", "ok");
        } catch (e) {
          setStatus(`Error: ${e.message || String(e)}`, "err");
        } finally {
          clearProviderCacheBtn.disabled = false;
          clearProviderCacheBtn.textContent = originalText;
        }
      });
    }

    // Clean Everything button
    const cleanAllBtn = root.querySelector("#cw-clean-all");
    if (cleanAllBtn) {
      cleanAllBtn.addEventListener("click", async (ev) => {
        const btn = ev.currentTarget;
        if (!confirm("This will clear all state, caches, tracker data, stats and currently playing. Continue?")) {
          return;
        }

        btn.disabled = true;
        btn.textContent = "Cleaning...";
        try {
          for (const op of cleanAllOps) await op();
          try { window.dispatchEvent(new CustomEvent("activity-log-cleared")); } catch {}

          await refreshSummary();
          setStatus("Clean Everything completed.", "ok");

          btn.textContent = "All Clean!";
          await new Promise((r) => setTimeout(r, 1200));
          btn.textContent = "Clean Everything";
        } catch (err) {
          console.error(err);
          btn.textContent = "Error";
          setStatus("Error while cleaning. See console.", "err");
        } finally {
          btn.disabled = false;
        }
      });
    }

    await refreshSummary();
    setStatus("Select a maintenance action.");
  },
  unmount() {},
};
