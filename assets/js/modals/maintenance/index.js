/* assets/js/modals/maintenance/index.js */
/* refactored */
/* Modal for maintenance and troubleshooting operations like clearing state, cache, tracker data, and resetting stats. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const REQUEST_TIMEOUT_MS = 45_000;
const fjson = async (url, opts = {}) => {
  const controller = new AbortController();
  const externalSignal = opts.signal;
  const forwardAbort = () => controller.abort();
  let timedOut = false;
  if (externalSignal?.aborted) controller.abort();
  else externalSignal?.addEventListener("abort", forwardAbort, { once: true });
  const timeout = window.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, REQUEST_TIMEOUT_MS);

  try {
    const r = await fetch(url, { cache: "no-store", ...opts, signal: controller.signal });
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
  } catch (error) {
    if (timedOut) throw new Error("Request timed out after 45 seconds");
    throw error;
  } finally {
    window.clearTimeout(timeout);
    externalSignal?.removeEventListener("abort", forwardAbort);
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
  scrobbles: "/api/maintenance/clear-recent-scrobbles",
  stats: "/api/maintenance/reset-stats",
  playing: "/api/maintenance/reset-currently-watching",
  captures: "/api/snapshots/clear",
};
const OPS = [
  {
    key: "state",
    kind: "state",
    icon: "deployed_code_history",
    title: "Rebuild sync state",
    tag: "sync pairs",
    desc: "Starts every sync pair from fresh provider baselines.",
  },
  {
    key: "cache",
    kind: "cache",
    icon: "network_node",
    title: "Retry provider items",
    tag: "runtime",
    desc: "Clears temporary retry and health data so items are tried again.",
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
    desc: "Clears local Watchlist, History and Ratings tracker data.",
    extra: `
      <div class="action-options">
        <label><input type="checkbox" id="cxm-cw-state" checked><span>Tracker state files</span></label>
        <label><input type="checkbox" id="cxm-cw-snaps"><span>All snapshots</span></label>
      </div>
    `,
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
    key: "captures",
    kind: "captures",
    icon: "photo_library",
    title: "Clear all captures",
    tag: "saved captures",
    desc: "Deletes every saved provider capture from local storage.",
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
const GROUPS = [
  {
    id: "sync",
    icon: "sync",
    title: "Sync",
    desc: "Keep sync state healthy and up to date.",
    keys: ["state", "cache"],
  },
  {
    id: "local",
    icon: "verified_user",
    title: "Local cleanup",
    desc: "Clean local tracker data.",
    keys: ["tracker"],
  },
  {
    id: "playback",
    icon: "play_circle",
    title: "Playback",
    desc: "Manage playback lists and state.",
    keys: ["playing", "scrobbles"],
  },
  {
    id: "reports",
    icon: "bar_chart",
    title: "Reports & Metadata",
    desc: "Rebuild reports and refresh cached metadata.",
    keys: ["stats", "meta"],
  },
  {
    id: "captures",
    icon: "photo_library",
    title: "Captures",
    desc: "Manage saved provider captures.",
    keys: ["captures"],
  },
  {
    id: "danger",
    icon: "warning",
    title: "Danger zone",
    desc: "Irreversible actions. Proceed with caution.",
    keys: ["defaults"],
  },
];

const OPS_BY_KEY = Object.fromEntries(OPS.map((op) => [op.key, op]));
const OVERVIEW_EXCLUDED_KEYS = new Set(["tracker", "captures", "defaults"]);
const OVERVIEW_KEYS = GROUPS
  .flatMap((group) => group.keys)
  .filter((key) => !OVERVIEW_EXCLUDED_KEYS.has(key));

const renderActionRow = ({ key, kind, icon, title, desc, extra = "" }) => `
  <div class="action-row" data-op="${key}" data-kind="${kind}" tabindex="0" aria-label="Inspect ${title} status">
    <div class="action-main">
      <div class="action-icon">
        <span class="material-symbols-rounded" aria-hidden="true">${icon}</span>
      </div>
      <div class="action-copy">
        <div class="action-title">${title}</div>
        <div class="action-desc">${desc}</div>
        ${extra}
      </div>
    </div>
    <button type="button" class="run-btn" data-label="${title}">Run</button>
  </div>
`;

const renderGroup = ({ id, icon, title, desc, keys }) => `
  <section class="action-group ${id}" id="cxm-group-${id}" data-group="${id}">
    <div class="group-info">
      <div class="group-title-row">
        <div class="group-icon">
          <span class="material-symbols-rounded" aria-hidden="true">${icon}</span>
        </div>
        <div class="group-title">${title}</div>
      </div>
      <div class="group-desc">${desc}</div>
    </div>
    <div class="group-actions${keys.length === 1 ? " single" : keys.length === 2 ? " two" : ""}">
      ${keys.map((key) => renderActionRow(OPS_BY_KEY[key])).join("")}
    </div>
  </section>
`;

function injectCSS() {
  const existing = document.getElementById("cw-maint-css");
  if (existing?.tagName === "LINK") return Promise.resolve();
  existing?.remove();
  const link = document.createElement("link");
  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  link.id = "cw-maint-css";
  link.rel = "stylesheet";
  link.href = cssUrl.href;
  return new Promise((resolve) => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
    document.head.appendChild(link);
  });
}

export default {
  async mount(root) {
    await injectCSS();

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("cw-maint-shell");
      shell.style.setProperty("--cxModalW", "1180px");
      shell.style.setProperty("--cxModalMaxW", "1180px");
      shell.style.setProperty("--cxModalMaxH", "700px");
    }
    root.innerHTML = `
      <div class="cw-maint">
        <div class="cx-head">
          <div class="cx-head-left">
            <div class="head-icon">
              <span class="material-symbols-rounded" aria-hidden="true">handyman</span>
            </div>
            <div class="head-text">
              <div class="head-title">Maintenance tools</div>
              <div class="head-sub">Reset, rebuild or clean the local CrossWatch data without touching provider accounts.</div>
            </div>
          </div>
          <div class="cx-head-right">
            <button type="button" class="header-action" id="cxm-close">
              <span class="material-symbols-rounded" aria-hidden="true">close</span>
              <span class="header-action-label">Close</span>
            </button>
          </div>
        </div>

        <div class="maint-layout">
          <aside class="maint-sidebar" aria-label="Maintenance categories">
            <div>
              <nav class="side-nav primary">
                <div class="side-nav-item active" data-group="overview">
                  <button type="button" class="side-nav-btn active" data-target="cxm-main" aria-current="page">
                    <span class="material-symbols-rounded" aria-hidden="true">home</span>
                    <span>Overview</span>
                  </button>
                  <button type="button" class="category-run-btn" id="cxm-run-overview" aria-label="Run complete local cleanup">Run</button>
                </div>
              </nav>

              <div class="sidebar-label">Categories</div>
              <nav class="side-nav secondary">
                <div class="side-nav-item" data-group="sync">
                  <button type="button" class="side-nav-btn" data-target="cxm-group-sync">
                    <span class="material-symbols-rounded" aria-hidden="true">sync</span>
                    <span>Sync</span>
                  </button>
                  <button type="button" class="category-run-btn" data-run-group="sync" aria-label="Run all Sync tools">Run</button>
                </div>
                <div class="side-nav-item" data-group="local">
                  <button type="button" class="side-nav-btn" data-target="cxm-group-local">
                    <span class="material-symbols-rounded" aria-hidden="true">shield</span>
                    <span>Local cleanup</span>
                  </button>
                  <button type="button" class="category-run-btn" data-run-group="local" aria-label="Run all Local cleanup tools">Run</button>
                </div>
                <div class="side-nav-item" data-group="playback">
                  <button type="button" class="side-nav-btn" data-target="cxm-group-playback">
                    <span class="material-symbols-rounded" aria-hidden="true">play_arrow</span>
                    <span>Playback</span>
                  </button>
                  <button type="button" class="category-run-btn" data-run-group="playback" aria-label="Run all Playback tools">Run</button>
                </div>
                <div class="side-nav-item" data-group="reports">
                  <button type="button" class="side-nav-btn" data-target="cxm-group-reports">
                    <span class="material-symbols-rounded" aria-hidden="true">bar_chart</span>
                    <span>Reports & Metadata</span>
                  </button>
                  <button type="button" class="category-run-btn" data-run-group="reports" aria-label="Run all Reports and Metadata tools">Run</button>
                </div>
                <div class="side-nav-item" data-group="captures">
                  <button type="button" class="side-nav-btn" data-target="cxm-group-captures">
                    <span class="material-symbols-rounded" aria-hidden="true">photo_library</span>
                    <span>Captures</span>
                  </button>
                  <button type="button" class="category-run-btn" data-run-group="captures" aria-label="Run all Captures tools">Run</button>
                </div>
                <div class="side-nav-item danger" data-group="danger">
                  <button type="button" class="side-nav-btn danger" data-target="cxm-group-danger">
                    <span class="material-symbols-rounded" aria-hidden="true">warning</span>
                    <span>Danger zone</span>
                  </button>
                  <button type="button" class="category-run-btn" data-run-group="danger" aria-label="Run Danger zone tools">Run</button>
                </div>
              </nav>
            </div>

            <div class="sidebar-status" id="cxm-sidebar-status">
              <div class="status-heading">Status</div>
              <div id="cxm-status" class="status-message" aria-live="polite" hidden></div>
              <div id="cxm-overview-status">
                <div class="status-lines">
                  <div class="status-line">
                    <span class="status-dot" aria-hidden="true"></span>
                    <span id="cxm-tracker-count">Tracker - state · - snapshots</span>
                  </div>
                  <div class="status-line">
                    <span class="status-dot" aria-hidden="true"></span>
                    <span id="cxm-cache-count">Provider cache - files</span>
                  </div>
                </div>
                <details class="storage-details">
                  <summary>
                    <span>Storage details</span>
                    <span class="material-symbols-rounded" aria-hidden="true">expand_more</span>
                  </summary>
                  <div class="summary-paths">
                    <div class="storage-path">
                      <span>Tracker</span>
                      <code id="cxm-tracker-root">/config/.cw_provider</code>
                    </div>
                    <div class="storage-path">
                      <span>Provider cache</span>
                      <code id="cxm-cache-root">/config/.cw_state</code>
                    </div>
                  </div>
                </details>
              </div>
              <div id="cxm-action-insight" class="action-insight" aria-live="polite" hidden>
                <div class="insight-head">
                  <span class="material-symbols-rounded insight-icon" aria-hidden="true"></span>
                  <div>
                    <div class="insight-kicker">Selected tool</div>
                    <div class="insight-title"></div>
                  </div>
                </div>
                <div class="insight-metrics"></div>
                <div class="insight-note"></div>
              </div>
            </div>
          </aside>

          <main class="maint-main" id="cxm-main">
            ${GROUPS.map(renderGroup).join("")}
          </main>
        </div>
      </div>
    `;

    const statusEl = $("#cxm-status", root);
    const closeModal = () => {
      try { window.cxCloseModal?.(); } catch {}
      if (root.isConnected) {
        root.dispatchEvent(new CustomEvent("cw-modal-close", { bubbles: true }));
      }
    };
    const setStatus = (msg, kind = "") => {
      if (!statusEl) return;
      statusEl.textContent = msg;
      statusEl.className = "status-message" + (kind ? " " + kind : "");
      statusEl.hidden = !msg;
    };

    let selectedInsightKind = null;
    let insightRequestId = 0;
    let operationBusy = false;

    function setOperationBusy(busy) {
      if (operationBusy === busy) return;
      operationBusy = busy;
      const controls = root.querySelectorAll(".run-btn, .category-run-btn, #cxm-close");
      controls.forEach((control) => {
        if (busy) {
          control.dataset.cwWasDisabled = control.disabled ? "1" : "0";
          control.disabled = true;
        } else {
          control.disabled = control.dataset.cwWasDisabled === "1";
          delete control.dataset.cwWasDisabled;
        }
      });
      $(".cw-maint", root)?.toggleAttribute("aria-busy", busy);
      try { window.cxSetModalDismissible?.(!busy); } catch {}
    }

    const formatBytes = (raw) => {
      const bytes = Number(raw || 0);
      if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
      const units = ["B", "KB", "MB", "GB", "TB"];
      const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
      const value = bytes / (1024 ** index);
      const digits = index === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2;
      return `${value.toFixed(digits)} ${units[index]}`;
    };

    const resultSummary = (result) => result?.summary || result?.result?.summary || null;
    const combineSummaries = (results) => {
      const summaries = results.map(resultSummary).filter(Boolean);
      if (!summaries.length) return null;
      return summaries.reduce((total, item) => ({
        removed_files: total.removed_files + Number(item.removed_files || 0),
        removed_items: total.removed_items + Number(item.removed_items || 0),
        freed_bytes: total.freed_bytes + Number(item.freed_bytes || 0),
      }), { removed_files: 0, removed_items: 0, freed_bytes: 0 });
    };
    const plural = (count, one, many = `${one}s`) => `${new Intl.NumberFormat().format(count)} ${count === 1 ? one : many}`;
    const completionReceipt = (label, results, extra = []) => {
      const list = Array.isArray(results) ? results : [results];
      const summary = combineSummaries(list);
      const details = [...extra];
      if (summary) {
        if (summary.removed_items > 0) details.push(plural(summary.removed_items, "item"));
        if (summary.removed_files > 0) details.push(plural(summary.removed_files, "file"));
        if (summary.freed_bytes > 0) details.push(`${formatBytes(summary.freed_bytes)} cleared`);
        if (!summary.removed_items && !summary.removed_files && !summary.freed_bytes) details.push("nothing to remove");
      }
      details.push(new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
      return `${label} completed · ${details.join(" · ")}.`;
    };

    const formatMetric = ({ value, format }) => {
      if (format === "bytes") return formatBytes(value);
      if (format === "datetime") {
        if (!value) return "Never";
        const date = new Date(Number(value) * 1000);
        return Number.isNaN(date.getTime()) ? "Unknown" : date.toLocaleString();
      }
      if (typeof value === "number") return new Intl.NumberFormat().format(value);
      return value ?? "-";
    };

    function showOverviewStatus() {
      selectedInsightKind = null;
      insightRequestId += 1;
      root.querySelectorAll(".action-row.is-inspected").forEach((row) => row.classList.remove("is-inspected"));
      const overview = $("#cxm-overview-status", root);
      const insight = $("#cxm-action-insight", root);
      if (overview) overview.hidden = false;
      if (insight) {
        insight.hidden = true;
        insight.classList.remove("loading", "load-error");
      }
    }

    function renderActionInsight(op, payload = null, state = "ready") {
      const insight = $("#cxm-action-insight", root);
      if (!insight) return;
      insight.hidden = false;
      insight.classList.toggle("loading", state === "loading");
      insight.classList.toggle("load-error", state === "error");

      $(".insight-icon", insight).textContent = op.icon;
      $(".insight-title", insight).textContent = payload?.title || op.title;
      const metricsRoot = $(".insight-metrics", insight);
      metricsRoot.replaceChildren();

      if (state === "loading") {
        for (let i = 0; i < 3; i += 1) {
          const skeleton = document.createElement("div");
          skeleton.className = "insight-metric skeleton";
          metricsRoot.appendChild(skeleton);
        }
      } else {
        (payload?.metrics || []).forEach((metric) => {
          const item = document.createElement("div");
          item.className = "insight-metric";
          item.dataset.format = metric.format || "number";
          const value = document.createElement("div");
          value.className = "insight-value";
          value.textContent = formatMetric(metric);
          const label = document.createElement("div");
          label.className = "insight-label";
          label.textContent = metric.label || "Value";
          item.append(value, label);
          metricsRoot.appendChild(item);
        });
      }

      const note = $(".insight-note", insight);
      note.textContent = state === "loading"
        ? "Reading current local data..."
        : state === "error"
          ? "Status data could not be loaded. The maintenance action is still available."
          : payload?.note || "";
    }

    async function loadActionInsight(kind) {
      const op = OPS.find((item) => item.kind === kind);
      if (!op) return;
      setStatus("");
      selectedInsightKind = kind;
      const requestId = ++insightRequestId;
      root.querySelectorAll(".action-row").forEach((row) => {
        row.classList.toggle("is-inspected", row.dataset.kind === kind);
      });
      const overview = $("#cxm-overview-status", root);
      if (overview) overview.hidden = true;
      renderActionInsight(op, null, "loading");

      try {
        const payload = await fjson(`/api/maintenance/action-status/${encodeURIComponent(kind)}`);
        if (requestId !== insightRequestId || selectedInsightKind !== kind) return;
        if (payload?.ok === false) throw new Error(payload.error || "Status unavailable");
        renderActionInsight(op, payload, "ready");
      } catch {
        if (requestId !== insightRequestId || selectedInsightKind !== kind) return;
        renderActionInsight(op, null, "error");
      }
    }

    const actionGroups = [...root.querySelectorAll(".action-group")];
    root.querySelectorAll(".side-nav-btn[data-target]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const target = root.querySelector(`#${btn.dataset.target}`);
        if (!target) return;
        const isOverview = target.id === "cxm-main";
        root.querySelectorAll(".side-nav-btn").forEach((item) => {
          const active = item === btn;
          item.classList.toggle("active", active);
          item.closest(".side-nav-item")?.classList.toggle("active", active);
          if (active) item.setAttribute("aria-current", "page");
          else item.removeAttribute("aria-current");
        });
        actionGroups.forEach((group) => group.classList.toggle("is-focused", !isOverview && group === target));
        showOverviewStatus();
        if (!isOverview) {
          target.classList.remove("focus-pop");
          void target.offsetWidth;
          target.classList.add("focus-pop");
          window.setTimeout(() => target.classList.remove("focus-pop"), 520);
        }
        if (isOverview) target.scrollTo({ top: 0, behavior: "smooth" });
        else target.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    });

    $("#cxm-close", root)?.addEventListener("click", closeModal);

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
          `Tracker ${tState} state · ${tSnap} snapshots`;

        const cCount = cache?.count ?? "-";
        $("#cxm-cache-count", root).textContent =
          `Provider cache ${cCount} file${cCount === 1 ? "" : "s"}`;
      } catch {

      }
    }

    function resetActionFeedback(btn) {
      if (!btn) return;
      if (btn._cwResultTimer) window.clearTimeout(btn._cwResultTimer);
      btn._cwResultTimer = null;
      btn.classList.remove("busy", "result-success", "result-error");
      btn.removeAttribute("aria-busy");
      btn.textContent = btn.dataset.idleLabel || "Run";
      btn.closest(".action-row")?.classList.remove("is-running", "run-success", "run-error");
    }

    function startActionFeedback(btn) {
      if (!btn) return;
      btn.dataset.idleLabel ||= btn.textContent.trim() || "Run";
      resetActionFeedback(btn);
      btn.classList.add("busy");
      btn.setAttribute("aria-busy", "true");
      btn.closest(".action-row")?.classList.add("is-running");
    }

    function finishActionFeedback(btn, result) {
      if (!btn) return;
      const row = btn.closest(".action-row");
      row?.classList.remove("is-running");
      btn.classList.remove("busy");
      btn.removeAttribute("aria-busy");
      if (result === "cancel") {
        resetActionFeedback(btn);
        return;
      }

      const ok = result === "success";
      row?.classList.add(ok ? "run-success" : "run-error");
      btn.classList.add(ok ? "result-success" : "result-error");
      btn.textContent = ok ? "Done" : "Failed";
      btn._cwResultTimer = window.setTimeout(() => resetActionFeedback(btn), 1400);
    }

    async function runOp(kind, btn, options = {}) {
      const {
        manageLock = true,
        skipConfirm = false,
      } = options;
      if (manageLock && operationBusy) return false;

      if (!skipConfirm && kind === "captures" && !confirm("Delete all saved captures? This cannot be undone.")) {
        setStatus("Cancelled.", "");
        return false;
      }

      if (manageLock) setOperationBusy(true);
      startActionFeedback(btn);
      const label = btn?.dataset?.label || OPS.find((item) => item.kind === kind)?.title || kind;
      setStatus(`Running ${label.toLowerCase()}...`, "busy");

      try {
        let res = null;
        if (SIMPLE_OPS[kind]) {
          res = await post(SIMPLE_OPS[kind], kind === "stats" ? {
            recalc: false,
            purge_file: true,
            purge_state: false,
            purge_reports: true,
            purge_insights: true,
          } : undefined);
        } else if (kind === "tracker") {
          const chkState = $("#cxm-cw-state", root);
          const chkSnaps = $("#cxm-cw-snaps", root);
          const clearState = !!(chkState && chkState.checked);
          const clearSnaps = !!(chkSnaps && chkSnaps.checked);

          if (!clearState && !clearSnaps) {
            setStatus("Select at least one option for tracker cleanup.", "err");
            finishActionFeedback(btn, "error");
            return false;
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

          if (!skipConfirm && !confirm(warn)) {
            setStatus("Cancelled.", "");
            finishActionFeedback(btn, "cancel");
            return false;
          }

          if (!skipConfirm) {
            const typed = prompt("Type RESET to continue");
            if (String(typed || "").trim().toUpperCase() !== "RESET") {
              setStatus("Cancelled.", "");
              finishActionFeedback(btn, "cancel");
              return false;
            }
          }
          res = await post("/api/maintenance/reset-all-default", {});
        }

        if (res?.ok === false) {
          setStatus(`Failed: ${res.error || "Unknown error"}`, "err");
          finishActionFeedback(btn, "error");
          return false;
        }

        if (kind === "scrobbles") {
          try { window.dispatchEvent(new CustomEvent("activity-log-cleared")); } catch {}
        }
        if (kind === "cache" || kind === "tracker") await refreshSummary();

        if (kind === "defaults") {
          finishActionFeedback(btn, "success");
          closeModal();
          setTimeout(() => {
            if (window.cwRestartCrossWatchWithOverlay) {
              window.cwRestartCrossWatchWithOverlay();
            } else {
              fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" }).finally(() => {
                window.location.reload();
              });
            }
          }, 150);
          return res || { ok: true };
        }

        if (selectedInsightKind === kind) await loadActionInsight(kind);
        setStatus(completionReceipt(label, res), "ok");
        finishActionFeedback(btn, "success");
        return res || { ok: true };
      } catch (e) {
        setStatus(`Error: ${e.message || String(e)}`, "err");
        finishActionFeedback(btn, "error");
        return false;
      } finally {
        if (manageLock) setOperationBusy(false);
      }
    }

    async function runGroup(groupId, groupBtn) {
      const group = GROUPS.find((item) => item.id === groupId);
      if (!group || operationBusy) return;

      root.querySelector(`.side-nav-btn[data-target="cxm-group-${groupId}"]`)?.click();
      setOperationBusy(true);
      groupBtn.classList.add("busy");
      groupBtn.setAttribute("aria-busy", "true");
      setStatus(`Running all ${group.title.toLowerCase()} tools...`, "busy");

      const results = [];
      try {
        for (const key of group.keys) {
          const op = OPS_BY_KEY[key];
          const actionBtn = root.querySelector(`.action-row[data-op="${key}"] .run-btn`);
          if (!op || !actionBtn) continue;
          const result = await runOp(op.kind, actionBtn, { manageLock: false });
          if (!result || !root.isConnected) return;
          results.push(result);
        }
        setStatus(completionReceipt(group.title, results, [plural(results.length, "tool")]), "ok");
      } finally {
        groupBtn.classList.remove("busy");
        groupBtn.removeAttribute("aria-busy");
        setOperationBusy(false);
      }
    }

    OPS.forEach(({ key, kind }) => {
      const row = root.querySelector(`.action-row[data-op="${key}"]`);
      const btn = row?.querySelector(".run-btn");
      if (btn) btn.addEventListener("click", () => runOp(kind, btn));
      row?.addEventListener("click", (event) => {
        if (event.target.closest("button, input, label, a, summary")) return;
        loadActionInsight(kind);
      });
      row?.addEventListener("keydown", (event) => {
        if (event.target !== row || !["Enter", " "].includes(event.key)) return;
        event.preventDefault();
        loadActionInsight(kind);
      });
    });

    root.querySelectorAll(".category-run-btn[data-run-group]").forEach((btn) => {
      btn.addEventListener("click", () => runGroup(btn.dataset.runGroup, btn));
    });

    const overviewRunBtn = root.querySelector("#cxm-run-overview");
    if (overviewRunBtn) {
      overviewRunBtn.addEventListener("click", async () => {
        if (operationBusy) return;
        if (!confirm("Run the Overview maintenance tools? This clears sync state, retry data, recent scrobbles, statistics, metadata and currently playing. Local tracker data, captures and Factory reset are excluded.")) {
          return;
        }

        root.querySelector('.side-nav-btn[data-target="cxm-main"]')?.click();
        setOperationBusy(true);
        overviewRunBtn.classList.add("busy");
        overviewRunBtn.setAttribute("aria-busy", "true");
        setStatus("Running complete local cleanup...", "busy");
        const results = [];
        try {
          for (const key of OVERVIEW_KEYS) {
            const op = OPS_BY_KEY[key];
            const actionBtn = root.querySelector(`.action-row[data-op="${key}"] .run-btn`);
            if (!op || !actionBtn) continue;
            const result = await runOp(op.kind, actionBtn, {
              manageLock: false,
              skipConfirm: true,
            });
            if (!result || !root.isConnected) return;
            results.push(result);
          }
          await refreshSummary();
          setStatus(completionReceipt("Complete local cleanup", results, [plural(results.length, "tool")]), "ok");
        } finally {
          overviewRunBtn.classList.remove("busy");
          overviewRunBtn.removeAttribute("aria-busy");
          setOperationBusy(false);
        }
      });
    }

    showOverviewStatus();
    await refreshSummary();
    setStatus("");
  },
  unmount() {},
};
