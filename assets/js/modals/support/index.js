/* assets/js/modals/support/index.js */
/* Support modal: rebuild state.json from the database and pack a diagnostic bundle. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const REQUEST_TIMEOUT_MS = 45_000;
const DOWNLOAD_TIMEOUT_MS = 600_000;
const timeoutError = (label, ms) => new Error(`${label} timed out after ${Math.round(ms / 1000)}s`);
const SECTIONS = [
  { key: "config", label: "Redacted config", desc: "config.json with tokens, keys and hashes masked." },
  { key: "diagnostics", label: "Diagnostics", desc: "Database and event-archive health, environment, baseline shape." },
  { key: "reports", label: "Sync reports", desc: "The last 25 sync runs." },
  { key: "logs", label: "Logs", desc: "Recent log tails, secrets stripped." },
];

const $ = (sel, root = document) => root.querySelector(sel);
const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
let cleanupSupportMount = null;

async function fjson(url) {
  const ctrl = new AbortController();
  const timer = window.setTimeout(() => ctrl.abort(timeoutError("Request", REQUEST_TIMEOUT_MS)), REQUEST_TIMEOUT_MS);
  try {
    const r = await fetch(url, { cache: "no-store", signal: ctrl.signal });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText || ""}`.trim());
    return await r.json();
  } finally {
    window.clearTimeout(timer);
  }
}

async function fblob(url, onProgress) {
  const ctrl = new AbortController();
  const timer = window.setTimeout(() => ctrl.abort(timeoutError("Download", DOWNLOAD_TIMEOUT_MS)), DOWNLOAD_TIMEOUT_MS);
  try {
    const r = await fetch(url, { cache: "no-store", signal: ctrl.signal });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText || ""}`.trim());
    const disposition = r.headers.get("content-disposition") || "";
    const match = /filename\*?=(?:UTF-8''|")?([^";]+)/i.exec(disposition);
    const filename = match ? decodeURIComponent(match[1].replace(/"$/, "")) : "";
    const total = Number(r.headers.get("content-length") || 0);
    const type = r.headers.get("content-type") || "application/octet-stream";
    if (r.body?.getReader) {
      const reader = r.body.getReader();
      const chunks = [];
      let received = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;
        chunks.push(value);
        received += value.byteLength || value.length || 0;
        onProgress?.({ received, total });
      }
      return { blob: new Blob(chunks, { type }), filename };
    }
    const blob = await r.blob();
    onProgress?.({ received: blob.size, total: blob.size });
    return { blob, filename };
  } finally {
    window.clearTimeout(timer);
  }
}

function saveBlob(blob, filename) {
  const href = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = href;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.setTimeout(() => URL.revokeObjectURL(href), 4000);
}

function formatBytes(raw) {
  const bytes = Number(raw || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / (1024 ** index);
  return `${value.toFixed(index === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2)} ${units[index]}`;
}

function elapsedLabel(startedAt) {
  const seconds = Math.max(0, Math.round((Date.now() - Number(startedAt || Date.now())) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  const rem = seconds % 60;
  return `${mins}m ${String(rem).padStart(2, "0")}s`;
}

function injectCSS() {
  const existing = document.getElementById("cw-support-css");
  if (existing?.tagName === "LINK") return Promise.resolve();
  existing?.remove();
  const link = document.createElement("link");
  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  link.id = "cw-support-css";
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
    cleanupSupportMount?.();
    cleanupSupportMount = null;
    await injectCSS();

    const shell = root.closest(".cx-modal-shell");
    shell?.classList.add("cw-support-shell");

    root.innerHTML = `
      <div class="cw-support">
        <div class="cx-head">
          <div class="sup-head-left">
            <div class="sup-head-icon"><span class="material-symbols-rounded" aria-hidden="true">support_agent</span></div>
            <div>
              <div class="sup-title">Support</div>
              <div class="sup-sub">Export diagnostic data to attach to a bug report. Nothing is changed or removed.</div>
            </div>
          </div>
          <button type="button" class="sup-close" id="sup-close" aria-label="Close">
            <span class="material-symbols-rounded" aria-hidden="true">close</span>
          </button>
        </div>

        <div class="sup-body">
          <div class="sup-stats" id="sup-stats" aria-live="polite"></div>

          <div class="sup-cards">
            <section class="sup-card">
              <div class="sup-card-head">
                <span class="material-symbols-rounded" aria-hidden="true">description</span>
                <div>
                  <strong>Export sync state</strong>
                  <small>Rebuilds state.json from the database so it can be attached to a bug report.</small>
                </div>
              </div>
              <label class="sup-field">
                <span>Scope</span>
                <select id="sup-state-scope" class="sup-select support-scope"><option value="all">All pairs</option></select>
              </label>
              <div class="sup-actions">
                <button type="button" class="sup-btn primary" id="sup-state-download">
                  <span class="material-symbols-rounded" aria-hidden="true">download</span><span>Download</span>
                </button>
              </div>
            </section>

            <section class="sup-card">
              <div class="sup-card-head">
                <span class="material-symbols-rounded" aria-hidden="true">folder_zip</span>
                <div>
                  <strong>Support bundle</strong>
                  <small>Packs state.json, a redacted config, diagnostics, reports and log tails into one ZIP.</small>
                </div>
              </div>
              <label class="sup-field">
                <span>Scope</span>
                <select id="sup-bundle-scope" class="sup-select support-scope"><option value="all">All pairs</option></select>
              </label>
              <div class="sup-includes">
                ${SECTIONS.map((s) => `
                  <label class="sup-check" title="${esc(s.desc)}">
                    <input type="checkbox" id="sup-inc-${s.key}" checked>
                    <span>${esc(s.label)}</span>
                  </label>`).join("")}
              </div>
              <div class="sup-actions">
                <button type="button" class="sup-btn primary" id="sup-bundle-download">
                  <span class="material-symbols-rounded" aria-hidden="true">download</span><span>Download</span>
                </button>
              </div>
            </section>
          </div>
        </div>

        <div class="sup-foot">
          <div class="sup-status" id="sup-status" aria-live="polite"></div>
          <div id="sup-progress" class="sup-progress hidden" aria-live="polite">
            <div class="sup-progress-head">
              <div>
                <strong id="sup-progress-title">Preparing export</strong>
                <span id="sup-progress-sub">Starting...</span>
              </div>
              <b id="sup-progress-percent">0%</b>
            </div>
            <div class="sup-progress-bar" aria-hidden="true"><span id="sup-progress-fill"></span></div>
            <div class="sup-progress-grid">
              <div><span>Export</span><b id="sup-progress-kind">-</b></div>
              <div><span>Scope</span><b id="sup-progress-scope">-</b></div>
              <div><span>Size</span><b id="sup-progress-size">-</b></div>
              <div><span>Elapsed</span><b id="sup-progress-elapsed">0s</b></div>
            </div>
            <div id="sup-progress-message" class="sup-progress-message">Preparing download...</div>
          </div>
        </div>
      </div>
    `;

    const statusEl = $("#sup-status", root);
    const setStatus = (msg, kind = "") => {
      if (!statusEl) return;
      statusEl.textContent = msg || "";
      statusEl.className = "sup-status" + (kind ? ` ${kind}` : "");
    };

    const closeModal = () => {
      if (busy) return;
      try { window.cxCloseModal?.(); } catch {}
    };
    $("#sup-close", root)?.addEventListener("click", closeModal);

    const scopeOf = (id) => String($(`#${id}`, root)?.value || "all").trim() || "all";

    const loadScopes = async () => {
      let payload = null;
      try {
        payload = await fjson("/api/maintenance/support/scopes");
      } catch {
        setStatus("Could not read sync pairs; the full export is still available.", "warn");
      }
      const pairs = Array.isArray(payload?.pairs) ? payload.pairs : [];
      const options = ['<option value="all">All pairs</option>'].concat(pairs.map((p) => {
        const items = Number(p?.items || 0);
        const suffix = `${p?.enabled === false ? " · disabled" : ""} · ${items} item${items === 1 ? "" : "s"}`;
        return `<option value="${esc(String(p?.id || ""))}">${esc(String(p?.label || p?.id || "Pair") + suffix)}</option>`;
      })).join("");
      root.querySelectorAll(".support-scope").forEach((sel) => {
        const current = sel.value || "all";
        sel.innerHTML = options;
        const values = Array.from(sel.options).map((o) => o.value);
        sel.value = values.includes(current) ? current : "all";
      });

      const totals = payload?.totals || {};
      const stats = [
        ["Sync pairs", totals.pairs],
        ["Feature baselines", totals.baselines],
        ["Baseline items", totals.items],
        ["Unreferenced", totals.orphan_baselines],
      ];
      const statsEl = $("#sup-stats", root);
      if (statsEl) {
        statsEl.innerHTML = stats.map(([label, value]) => `
          <div class="sup-stat">
            <div class="sup-stat-value">${esc(new Intl.NumberFormat().format(Number(value || 0)))}</div>
            <div class="sup-stat-label">${esc(label)}</div>
          </div>`).join("");
      }
    };

    let busy = false;
    let progressTimer = 0;
    let progressStartedAt = 0;
    let progressPercent = 0;

    const setText = (selector, value) => {
      const el = $(selector, root);
      if (el) el.textContent = String(value ?? "");
    };

    const stopProgressTimer = () => {
      if (progressTimer) window.clearInterval(progressTimer);
      progressTimer = 0;
    };
    cleanupSupportMount = () => {
      stopProgressTimer();
      window.cxSetModalDismissible?.(true);
    };

    const updateProgress = ({
      title,
      sub,
      message,
      kind,
      scope,
      size,
      percent,
      active = true,
      done = false,
      error = false,
    } = {}) => {
      const panel = $("#sup-progress", root);
      if (!panel) return;
      const pct = Math.max(0, Math.min(100, Math.round(Number(percent ?? progressPercent ?? 0))));
      progressPercent = pct;
      panel.classList.remove("hidden");
      panel.classList.toggle("active", !!active && !done && !error);
      panel.classList.toggle("done", !!done && !error);
      panel.classList.toggle("error", !!error);
      $("#sup-progress-fill", root)?.style.setProperty("width", `${pct}%`);
      setText("#sup-progress-percent", `${pct}%`);
      if (title) setText("#sup-progress-title", title);
      if (sub) setText("#sup-progress-sub", sub);
      if (message) setText("#sup-progress-message", message);
      if (kind) setText("#sup-progress-kind", kind);
      if (scope) setText("#sup-progress-scope", scope);
      if (size) setText("#sup-progress-size", size);
      setText("#sup-progress-elapsed", elapsedLabel(progressStartedAt));
    };

    const startProgress = ({ isBundle, scope }) => {
      stopProgressTimer();
      progressStartedAt = Date.now();
      progressPercent = 2;
      updateProgress({
        title: isBundle ? "Building support bundle" : "Rebuilding state.json",
        sub: "Preparing export",
        message: isBundle ? "Collecting diagnostics, reports and log tails..." : "Rebuilding sync state from the database...",
        kind: isBundle ? "Support bundle" : "state.json",
        scope: scope === "all" ? "All pairs" : "1 pair",
        size: "Waiting",
        percent: 2,
        active: true,
      });
      progressTimer = window.setInterval(() => {
        if (!busy) return;
        const elapsed = Date.now() - progressStartedAt;
        const ceiling = elapsed > 90_000 ? 94 : elapsed > 30_000 ? 90 : 82;
        const next = Math.min(ceiling, progressPercent + (progressPercent < 30 ? 3 : 1));
        updateProgress({ percent: next, active: true });
      }, 900);
    };

    const finishProgress = ({ isBundle, scope, blob }) => {
      stopProgressTimer();
      updateProgress({
        title: "Export complete",
        sub: "Download saved",
        message: isBundle ? "Support bundle is ready." : "state.json is ready.",
        kind: isBundle ? "Support bundle" : "state.json",
        scope: scope === "all" ? "All pairs" : "1 pair",
        size: formatBytes(blob?.size || 0),
        percent: 100,
        active: false,
        done: true,
      });
    };

    const failProgress = (message) => {
      stopProgressTimer();
      updateProgress({
        title: "Export failed",
        sub: "Download stopped",
        message,
        percent: Math.max(progressPercent, 6),
        active: false,
        error: true,
      });
    };

    const setBusy = (on) => {
      busy = !!on;
      window.cxSetModalDismissible?.(!busy);
      root.querySelectorAll(".sup-btn, .sup-select, .sup-check input").forEach((el) => { el.disabled = busy; });
      $("#sup-close", root)?.toggleAttribute("disabled", busy);
      if (!busy) stopProgressTimer();
    };

    const download = async (kind) => {
      if (busy) return;
      const isBundle = kind === "bundle";
      const scope = scopeOf(isBundle ? "sup-bundle-scope" : "sup-state-scope");
      const params = new URLSearchParams();
      if (scope !== "all") params.set("pairs", scope);
      let sections = [];
      if (isBundle) {
        sections = SECTIONS.map((s) => s.key).filter((key) => $(`#sup-inc-${key}`, root)?.checked);
        (sections.length ? sections : ["none"]).forEach((name) => params.append("include", name));
      }
      const query = params.toString();
      setBusy(true);
      startProgress({ isBundle, scope });
      setStatus(isBundle ? "Building support bundle..." : "Rebuilding state.json...", "busy");
      try {
        const { blob, filename } = await fblob(`/api/maintenance/support/${isBundle ? "bundle" : "state"}${query ? `?${query}` : ""}`, ({ received, total }) => {
          const knownTotal = Number.isFinite(total) && total > 0;
          const pct = knownTotal ? Math.max(progressPercent, Math.min(98, Math.round((received / total) * 100))) : Math.max(progressPercent, 92);
          updateProgress({
            title: isBundle ? "Downloading support bundle" : "Downloading state.json",
            sub: knownTotal ? `${formatBytes(received)} of ${formatBytes(total)}` : `${formatBytes(received)} downloaded`,
            message: "Receiving export from the server...",
            size: knownTotal ? `${formatBytes(received)} / ${formatBytes(total)}` : formatBytes(received),
            percent: pct,
            active: true,
          });
        });
        saveBlob(blob, filename || (isBundle ? "crosswatch-support.zip" : "crosswatch-state.json"));
        finishProgress({ isBundle, scope, blob });
        const bits = [filename || "download", formatBytes(blob.size), scope === "all" ? "all pairs" : "1 pair"];
        if (isBundle) bits.push(sections.length ? sections.join(", ") : "state only");
        setStatus(`Downloaded · ${bits.join(" · ")}.`, "ok");
      } catch (e) {
        const message = `Download failed: ${e?.message || String(e)}`;
        failProgress(message);
        setStatus(message, "err");
      } finally {
        setBusy(false);
      }
    };

    $("#sup-state-download", root)?.addEventListener("click", () => download("state"));
    $("#sup-bundle-download", root)?.addEventListener("click", () => download("bundle"));

    await loadScopes();
    setStatus("");
  },
  unmount() {
    cleanupSupportMount?.();
    cleanupSupportMount = null;
  },
};
