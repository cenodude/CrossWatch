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
  stats: "/api/maintenance/reset-stats",
  playing: "/api/maintenance/reset-currently-watching",
};
const OPS = [
  {
    key: "state",
    kind: "state",
    icon: "deployed_code_history",
    title: "Clear state",
    tag: "state.json",
    desc: 'Removes orchestrator <code>state.json</code> so the next sync rebuilds baseline state from providers.',
  },
  {
    key: "cache",
    kind: "cache",
    icon: "network_node",
    title: "Clear provider cache",
    tag: ".cw_state",
    desc: 'Clears provider shadow / flap files under <code>/config/.cw_state</code> so unresolved items and health state are retried.',
  },
  {
    key: "meta",
    kind: "metadata",
    icon: "gallery_thumbnail",
    title: "Remove metadata cache",
    tag: "/config/cache",
    desc: 'Deletes cached posters and metadata under <code>/config/cache</code>. Artwork and meta will be refetched when needed.',
  },
  {
    key: "tracker",
    kind: "tracker",
    icon: "deployed_code",
    title: "Clear CrossWatch tracker",
    tag: "tracker",
    desc: 'Cleans up local tracker files (<code>watchlist.json</code>, <code>history.json</code>, <code>ratings.json</code>) and optional snapshots.',
    extra: `
      <div class="action-options">
        <label><input type="checkbox" id="cxm-cw-state" checked><span>Tracker state files</span></label>
        <label><input type="checkbox" id="cxm-cw-snaps"><span>All snapshots</span></label>
      </div>
    `,
  },
  {
    key: "stats",
    kind: "stats",
    icon: "monitoring",
    title: "Reset statistics",
    tag: "stats + reports",
    desc: "Drops statistics, reports and insights caches, then reloads stats from a clean state. Does not touch provider data.",
  },
  {
    key: "playing",
    kind: "playing",
    icon: "live_tv",
    title: "Reset currently playing",
    desc: 'Clears <code>currently_watching.json</code> so stuck "currently playing" entries disappear.',
  },
  {
    key: "defaults",
    kind: "defaults",
    icon: "release_alert",
    title: "Reset all to default",
    tag: "DANGER",
    desc: "Resets CrossWatch to a clean install state by deleting local state, caches, tracker files, reports and TLS material, and backing up <code>config.json</code>. Snapshots in <code>/config/snapshots</code> are kept.",
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
    height: 100%;
    background:
      radial-gradient(96% 125% at 0% 0%, rgba(72,52,146,.18), transparent 36%),
      linear-gradient(180deg, rgba(5,7,14,.99), rgba(3,5,11,.99));
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 22px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.025), 0 28px 60px rgba(0,0,0,.32);
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
    padding: 12px 14px 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .cw-maint .summary-card {
    background: radial-gradient(120% 140% at 0% 0%,rgba(84,58,164,.10),transparent 38%),
                linear-gradient(180deg,rgba(10,13,24,.97),rgba(6,9,18,.965));
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,.07);
    padding: 10px 12px;
    display: grid;
    grid-template-columns: minmax(0,1.3fr) minmax(0,1fr);
    grid-gap: 10px;
    font-size: 12px;
    box-shadow: 0 18px 34px rgba(0,0,0,.24), inset 0 1px 0 rgba(255,255,255,.025);
  }
  @media (max-width: 900px) {
    .cw-maint .summary-card {
      grid-template-columns: minmax(0,1fr);
    }
  }
  .cw-maint .summary-label {
    opacity: .78;
    margin-bottom: 5px;
    font-size: 10px;
    font-weight: 800;
    letter-spacing: .14em;
    text-transform: uppercase;
  }
  .cw-maint .summary-paths code {
    font-size: 11px;
    background: rgba(255,255,255,.035);
    padding: 2px 6px;
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
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
    margin-top: 0;
  }
  @media (max-width: 980px) {
    .cw-maint .actions {
      grid-template-columns: 1fr;
    }
  }

  .cw-maint .action-row {
    background: radial-gradient(120% 145% at 0% 0%,rgba(76,54,150,.08),transparent 38%),linear-gradient(180deg,rgba(9,12,22,.97),rgba(5,8,17,.965));
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,.07);
    padding: 12px 13px;
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: center;
    gap: 12px;
    box-shadow: 0 16px 30px rgba(0,0,0,.22), inset 0 1px 0 rgba(255,255,255,.02);
    transition: transform .14s ease,border-color .14s ease,box-shadow .16s ease,background .16s ease;
  }
  .cw-maint .action-row:hover {
    transform: translateY(-1px);
    border-color: rgba(130,116,220,.16);
    box-shadow: 0 20px 34px rgba(0,0,0,.26), inset 0 1px 0 rgba(255,255,255,.03);
  }
  .cw-maint .action-main {
    display: grid;
    grid-template-columns: 26px minmax(0, 1fr);
    align-items: start;
    gap: 10px;
    flex: 1;
    min-width: 0;
  }
  .cw-maint .action-icon {
    width: 28px;
    height: 28px;
    border-radius: 10px;
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
    font-size: 13px;
    font-weight: 750;
    line-height: 1.2;
  }
  .cw-maint .action-tag {
    padding: 2px 8px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.08);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: .09em;
    opacity: .86;
    background: rgba(255,255,255,.028);
  }
  .cw-maint .action-desc {
    font-size: 11px;
    line-height: 1.35;
    color: rgba(197,206,224,.76);
  }
  .cw-maint .action-desc code {
    font-size: 11px;
    background: rgba(255,255,255,.035);
    border-radius: 999px;
    padding: 1px 6px;
  }

  .cw-maint .action-options {
    width: 100%;
    margin-top: 6px;
    font-size: 12px;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 4px 16px;
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
    padding: 7px 14px;
    font-size: 11px;
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
  }
  .cw-maint .action-row[data-op="cache"] .action-icon {
    border-color: rgba(164,138,255,.18);
  }
  .cw-maint .action-row[data-op="meta"] .action-icon {
    border-color: rgba(255,201,110,.18);
  }
  .cw-maint .action-row[data-op="tracker"] .action-icon {
    border-color: rgba(115,197,255,.18);
  }
  .cw-maint .action-row[data-op="stats"] .action-icon {
    border-color: rgba(120,220,176,.18);
  }
  .cw-maint .action-row[data-op="playing"] .action-icon {
    border-color: rgba(190,196,255,.16);
  }

  .cw-maint .action-row[data-op="defaults"] .action-icon {
    border-color: rgba(255,106,106,.18);
  }

  .cw-maint .status {
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 42px;
    margin-top: 2px;
    padding: 9px 12px;
    border-radius: 16px;
    border: 1px solid rgba(255,255,255,.07);
    background: radial-gradient(120% 140% at 0% 0%,rgba(74,54,150,.06),transparent 38%),linear-gradient(180deg,rgba(9,12,22,.97),rgba(6,8,16,.96));
    box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
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
  #cw-clean-all:hover {
    border-color: rgba(255,146,160,.22);
    background: linear-gradient(180deg,rgba(72,26,39,.96),rgba(44,15,25,.94));
  }
  #cw-clean-all[disabled] {
    opacity: .6;
    cursor: wait;
    box-shadow: none;
  }
  `;
  document.head.appendChild(el);
}

export default {
  async mount(root) {
    injectCSS();

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.style.setProperty("--cxModalW", "1180px");
      shell.style.setProperty("--cxModalMaxW", "1180px");
      shell.style.setProperty("--cxModalMaxH", "80vh");
    }
    const cleanAllOps = [
      () => post(SIMPLE_OPS.state),
      () => post(SIMPLE_OPS.cache),
      () => post(SIMPLE_OPS.metadata),
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
            <button id="cw-clean-all" class="cw-btn danger">Clean Everything</button>
            <button type="button" class="close-btn" id="cxm-close">Close</button>
          </div>
        </div>

        <div class="cx-body">
          <div class="summary-card">
            <div>
              <div class="summary-label">Paths</div>
              <div class="summary-paths">
                Tracker root:
                <code id="cxm-tracker-root">/config/.cw_provider</code><br>
                Provider cache:
                <code id="cxm-cache-root">/config/.cw_state</code>
              </div>
            </div>
            <div>
              <div class="summary-label">Counts</div>
              <div class="summary-badges">
                <span class="summary-pill" id="cxm-tracker-count">Tracker: -</span>
                <span class="summary-pill" id="cxm-cache-count">Provider cache: -</span>
              </div>
            </div>
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
