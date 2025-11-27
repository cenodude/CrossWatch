// assets/js/modals/maintenance/index.js

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
  }

  .cw-maint .cx-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 16px;
    border-bottom: 1px solid rgba(255,255,255,.12);
  }
  .cw-maint .cx-head-left {
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 0;
  }
  .cw-maint .head-icon {
    width: 32px;
    height: 32px;
    border-radius: 999px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: radial-gradient(circle at 30% 0%, #ff8b8b 0, #ff626f 35%, #321321 100%);
    box-shadow: 0 0 18px rgba(255,98,113,.55);
    flex-shrink: 0;
  }
  .cw-maint .head-icon .material-symbols-rounded {
    font-variation-settings:"FILL" 0,"wght" 650,"GRAD" 0,"opsz" 24;
    font-size: 20px;
  }
  .cw-maint .head-text {
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
  }
  .cw-maint .head-title {
    font-weight: 800;
    font-size: 15px;
  }
  .cw-maint .head-sub {
    font-size: 12px;
    opacity: .8;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .cw-maint .head-badge {
    padding: 3px 8px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.14);
    background: rgba(255,255,255,.03);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .06em;
    opacity: .9;
  }

  .cw-maint .close-btn {
    border: 1px solid rgba(255,255,255,.22);
    background: #171b2a;
    color: #fff;
    border-radius: 999px;
    padding: 6px 14px;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .06em;
    cursor: pointer;
  }
  .cw-maint .close-btn:hover {
    background: #20253d;
  }

  .cw-maint .cx-body {
    flex: 1;
    min-height: 0;
    padding: 12px 16px 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .cw-maint .summary-card {
    background: radial-gradient(circle at 0 0, rgba(255,127,159,.24), transparent 55%),
                linear-gradient(135deg, #0b0f19, #0e1624);
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,.12);
    padding: 8px 10px;
    display: grid;
    grid-template-columns: minmax(0,1.3fr) minmax(0,1fr);
    grid-gap: 8px;
    font-size: 12px;
  }
  @media (max-width: 900px) {
    .cw-maint .summary-card {
      grid-template-columns: minmax(0,1fr);
    }
  }
  .cw-maint .summary-label {
    opacity: .8;
    margin-bottom: 2px;
  }
  .cw-maint .summary-paths code {
    font-size: 11px;
    background: rgba(0,0,0,.4);
    padding: 2px 4px;
    border-radius: 4px;
  }
  .cw-maint .summary-badges {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .cw-maint .summary-pill {
    padding: 2px 8px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.18);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: .08em;
    opacity: .9;
    background: rgba(11,15,25,.9);
  }

  .cw-maint .actions {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 2px;
  }

  .cw-maint .action-row {
    background: #0b0f19;
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,.09);
    padding: 9px 11px;
    display: flex;
    align-items: center;
    gap: 10px;
    box-shadow: 0 0 0 1px rgba(0,0,0,.4) inset;
  }
  .cw-maint .action-main {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    flex: 1;
    min-width: 0;
  }
  .cw-maint .action-icon {
    width: 26px;
    height: 26px;
    border-radius: 999px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    box-shadow: 0 0 12px rgba(0,0,0,.6);
  }
  .cw-maint .action-icon .material-symbols-rounded {
    font-variation-settings:"FILL" 0,"wght" 600,"GRAD" 0,"opsz" 20;
    font-size: 18px;
  }

  .cw-maint .action-copy {
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
  }
  .cw-maint .action-line {
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
  }
  .cw-maint .action-title {
    font-size: 13px;
    font-weight: 600;
  }
  .cw-maint .action-tag {
    padding: 1px 7px;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.16);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: .09em;
    opacity: .9;
  }
  .cw-maint .action-desc {
    font-size: 12px;
    opacity: .82;
  }
  .cw-maint .action-desc code {
    font-size: 11px;
    background: rgba(255,255,255,.04);
    border-radius: 4px;
    padding: 1px 4px;
  }

  .cw-maint .action-options {
    margin-top: 6px;
    font-size: 12px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    align-items: flex-start;
  }
  .cw-maint .action-options label {
    display: inline-flex;
    align-items: center;
    justify-content: flex-start;
    gap: 6px;
    cursor: pointer;
  }
  .cw-maint .action-options input {
    accent-color: #ff627e;
  }

  .cw-maint .run-btn {
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,.2);
    padding: 6px 14px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .09em;
    text-transform: uppercase;
    cursor: pointer;
    background: linear-gradient(135deg,#ff6475,#ff8a7a);
    color: #fff;
    box-shadow: 0 0 12px rgba(255,106,126,.6);
    flex-shrink: 0;
  }
  .cw-maint .run-btn[disabled] {
    opacity: .6;
    cursor: wait;
    box-shadow: none;
  }

  /* Per-action accent colors */
  .cw-maint .action-row[data-op="state"] .action-icon {
    background: radial-gradient(circle at 0 0,#ff5a6b,#7d1e2c);
  }
  .cw-maint .action-row[data-op="cache"] .action-icon {
    background: radial-gradient(circle at 0 0,#b18bff,#3b2778);
  }
  .cw-maint .action-row[data-op="tracker"] .action-icon {
    background: radial-gradient(circle at 0 0,#71c5ff,#1f3a63);
  }
  .cw-maint .action-row[data-op="stats"] .action-icon {
    background: radial-gradient(circle at 0 0,#7cf2b0,#1d5537);
  }
  .cw-maint .action-row[data-op="playing"] .action-icon {
    background: radial-gradient(circle at 0 0,#c3c8ff,#333a7b);
  }

  .cw-maint .status {
    font-size: 12px;
    margin-top: 4px;
    opacity: .85;
  }
  .cw-maint .status.ok {
    color: #7cf2b0;
  }
  .cw-maint .status.err {
    color: #ff9a9a;
  }
  `;
  document.head.appendChild(el);
}

export default {
  async mount(root) {
    injectCSS();

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.style.setProperty("--cxModalMaxW", "850px");
    }

    root.innerHTML = `
      <div class="cw-maint">
        <div class="cx-head">
          <div class="cx-head-left">
            <div class="head-icon">
              <span class="material-symbols-rounded" aria-hidden="true">build</span>
            </div>
            <div class="head-text">
              <div class="head-title">Maintenance tools</div>
              <div class="head-sub">Reset or rebuild CrossWatch state, provider cache and tracker</div>
            </div>
          </div>
          <div class="cx-head-right">
            <span class="head-badge">Safe but irreversible</span>
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
                <span class="summary-pill" id="cxm-tracker-count">Tracker: –</span>
                <span class="summary-pill" id="cxm-cache-count">Provider cache: –</span>
              </div>
            </div>
          </div>

          <div class="actions">
            <div class="action-row" data-op="state">
              <div class="action-main">
                <div class="action-icon">
                  <span class="material-symbols-rounded" aria-hidden="true">auto_delete</span>
                </div>
                <div class="action-copy">
                  <div class="action-line">
                    <div class="action-title">Clear state</div>
                    <span class="action-tag">state.json</span>
                  </div>
                  <div class="action-desc">
                    Removes orchestrator <code>state.json</code> so the next sync rebuilds baseline state from providers.
                  </div>
                </div>
              </div>
              <button type="button" class="run-btn" data-label="Clear state">Run</button>
            </div>

            <div class="action-row" data-op="cache">
              <div class="action-main">
                <div class="action-icon">
                  <span class="material-symbols-rounded" aria-hidden="true">cloud_sync</span>
                </div>
                <div class="action-copy">
                  <div class="action-line">
                    <div class="action-title">Clear provider cache</div>
                    <span class="action-tag">.cw_state</span>
                  </div>
                  <div class="action-desc">
                    Clears provider shadow / flap files under <code>/config/.cw_state</code> so unresolved items and health state are retried.
                  </div>
                </div>
              </div>
              <button type="button" class="run-btn" data-label="Clear provider cache">Run</button>
            </div>

            <div class="action-row" data-op="tracker">
              <div class="action-main">
                <div class="action-icon">
                  <span class="material-symbols-rounded" aria-hidden="true">storage</span>
                </div>
                <div class="action-copy">
                  <div class="action-line">
                    <div class="action-title">Clear CrossWatch tracker</div>
                    <span class="action-tag">tracker</span>
                  </div>
                  <div class="action-desc">
                    Cleans up local tracker files (<code>watchlist.json</code>, <code>history.json</code>, <code>ratings.json</code>) and optional snapshots.
                  </div>
                    <div class="action-options">
                    <label>
                        <input type="checkbox" id="cxm-cw-state" checked>
                        <span>Tracker state files</span>
                    </label>
                    <label>
                        <input type="checkbox" id="cxm-cw-snaps">
                        <span>All snapshots</span>
                    </label>
                    </div>

                </div>
              </div>
              <button type="button" class="run-btn" data-label="Clear tracker">Run</button>
            </div>

            <div class="action-row" data-op="stats">
              <div class="action-main">
                <div class="action-icon">
                  <span class="material-symbols-rounded" aria-hidden="true">leaderboard</span>
                </div>
                <div class="action-copy">
                  <div class="action-line">
                    <div class="action-title">Reset statistics</div>
                    <span class="action-tag">stats + reports</span>
                  </div>
                  <div class="action-desc">
                    Drops statistics, reports and insights caches, then reloads stats from a clean state. Does not touch provider data.
                  </div>
                </div>
              </div>
              <button type="button" class="run-btn" data-label="Reset statistics">Run</button>
            </div>

            <div class="action-row" data-op="playing">
              <div class="action-main">
                <div class="action-icon">
                  <span class="material-symbols-rounded" aria-hidden="true">play_disabled</span>
                </div>
                <div class="action-copy">
                  <div class="action-line">
                    <div class="action-title">Reset currently playing</div>
                  </div>
                  <div class="action-desc">
                    Clears <code>currently_watching.json</code> so stuck “currently playing” entries disappear.
                  </div>
                </div>
              </div>
              <button type="button" class="run-btn" data-label="Reset currently playing">Run</button>
            </div>
          </div>

          <div id="cxm-status" class="status">Ready.</div>
        </div>
      </div>
    `;

    const statusEl = $("#cxm-status", root);
    const setStatus = (msg, kind = "") => {
      if (!statusEl) return;
      statusEl.textContent = msg;
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
        const tState = tCounts.state_files ?? "–";
        const tSnap = tCounts.snapshots ?? "–";
        $("#cxm-tracker-count", root).textContent =
          `Tracker: ${tState} state • ${tSnap} snapshots`;

        const cCount = cache?.count ?? "–";
        $("#cxm-cache-count", root).textContent =
          `Provider cache: ${cCount} file${cCount === 1 ? "" : "s"}`;
      } catch {
        // ignore; summary just stays generic
      }
    }

    async function runOp(kind, btn) {
      if (btn) btn.disabled = true;
      const label = btn?.dataset?.label || kind;
      setStatus(`Running: ${label}…`);

      try {
        let res = null;

        if (kind === "state") {
          res = await fjson("/api/maintenance/clear-state", {
            method: "POST",
          });
        } else if (kind === "cache") {
          res = await fjson("/api/maintenance/clear-cache", {
            method: "POST",
          });
        } else if (kind === "tracker") {
          const chkState = $("#cxm-cw-state", root);
          const chkSnaps = $("#cxm-cw-snaps", root);
          const clearState = !!(chkState && chkState.checked);
          const clearSnaps = !!(chkSnaps && chkSnaps.checked);

          if (!clearState && !clearSnaps) {
            setStatus("Select at least one option for tracker cleanup.", "err");
            return;
          }

          res = await fjson("/api/maintenance/crosswatch-tracker/clear", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              clear_state: clearState,
              clear_snapshots: clearSnaps,
            }),
          });
        } else if (kind === "stats") {
          res = await fjson("/api/maintenance/reset-stats", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}",
          });
        } else if (kind === "playing") {
          res = await fjson("/api/maintenance/reset-currently-watching", {
            method: "POST",
          });
        }

        if (kind === "cache" || kind === "tracker") {
          await refreshSummary();
        }

        if (res && res.ok === false) {
          setStatus(`Failed: ${res.error || "Unknown error"}`, "err");
        } else {
          setStatus(`Done: ${label}`, "ok");
        }
      } catch (e) {
        setStatus(`Error: ${e.message || String(e)}`, "err");
      } finally {
        if (btn) btn.disabled = false;
      }
    }

    const map = {
      state: "state",
      cache: "cache",
      tracker: "tracker",
      stats: "stats",
      playing: "playing",
    };

    Object.entries(map).forEach(([key, kind]) => {
      const row = root.querySelector(`.action-row[data-op="${key}"]`);
      const btn = row?.querySelector(".run-btn");
      if (btn) {
        btn.addEventListener("click", () => runOp(kind, btn));
      }
    });

    await refreshSummary();
    setStatus("Ready.");
  },
  unmount() {},
};
