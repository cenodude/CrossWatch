/* assets/js/modals/support/index.js */
/* Support modal: rebuild state.json from the database and pack a diagnostic bundle. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const REQUEST_TIMEOUT_MS = 45_000;
const DOWNLOAD_TIMEOUT_MS = 600_000;
const timeoutError = (label, ms) => new Error(T("modals.support.timedOut", {label, seconds: Math.round(ms / 1000)}));
const SECTIONS = [
  { key: "config", label: T("modals.support.redactedConfig"), desc: T("modals.support.redactedConfigDesc") },
  { key: "diagnostics", label: T("modals.support.diagnostics"), desc: T("modals.support.diagnosticsDesc") },
  { key: "reports", label: T("modals.support.syncReports"), desc: T("modals.support.syncReportsDesc") },
  { key: "logs", label: T("modals.support.logs"), desc: T("modals.support.logsDesc") },
];

const $ = (sel, root = document) => root.querySelector(sel);
const esc = (value) => String(value ?? "").replace(/[&<>"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
let cleanupSupportMount = null;

async function fjson(url) {
  const ctrl = new AbortController();
  const timer = window.setTimeout(() => ctrl.abort(timeoutError(T("modals.support.request"), REQUEST_TIMEOUT_MS)), REQUEST_TIMEOUT_MS);
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
  const timer = window.setTimeout(() => ctrl.abort(timeoutError(T("modals.support.download"), DOWNLOAD_TIMEOUT_MS)), DOWNLOAD_TIMEOUT_MS);
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

export default {
  async mount(root) {
    cleanupSupportMount?.();
    cleanupSupportMount = null;

    const shell = root.closest(".cx-modal-shell");
    shell?.classList.add("cw-support-shell", "cw-page-dialog");

    root.innerHTML = `
      <div class="cw-support cw-page-maint-content">
        <div class="mt-head">
          <div class="sup-head-left">
            <div>
              <h2 class="sup-title">${TH("modals.support.title")}</h2>
              <div class="sup-sub">${TH("modals.support.sub")}</div>
            </div>
          </div>
          <button type="button" class="cw-page-button mt-button sup-close" id="sup-close" aria-label="${TH("modals.support.close")}">
            ${TH("modals.support.close")}
          </button>
        </div>

        <div class="mt-body sup-body cw-scrollbars">
          <div class="sup-stats" id="sup-stats" aria-live="polite"></div>

          <div class="sup-cards">
            <section class="cw-page-panel sup-card">
              <div class="sup-card-head">
                <div>
                  <strong>${TH("modals.support.exportState")}</strong>
                  <small>${TH("modals.support.exportStateDesc")}</small>
                </div>
              </div>
              <label class="sup-field">
                <span>${TH("modals.support.scope")}</span>
                <select id="sup-state-scope" class="cw-page-input sup-select support-scope"><option value="all">${TH("modals.support.allPairs")}</option></select>
              </label>
              <div class="sup-actions">
                <button type="button" class="cw-page-button mt-button sup-btn primary" id="sup-state-download" aria-label="${TH("modals.support.downloadState")}">
                  <span class="material-symbols-rounded" aria-hidden="true">download</span><span>${TH("modals.support.downloadBtn")}</span>
                </button>
              </div>
            </section>

            <section class="cw-page-panel sup-card">
              <div class="sup-card-head">
                <div>
                  <strong>${TH("modals.support.bundle")}</strong>
                  <small>${TH("modals.support.bundleDesc")}</small>
                </div>
              </div>
              <label class="sup-field">
                <span>${TH("modals.support.scope")}</span>
                <select id="sup-bundle-scope" class="cw-page-input sup-select support-scope"><option value="all">${TH("modals.support.allPairs")}</option></select>
              </label>
              <div class="sup-includes">
                ${SECTIONS.map((s) => `
                  <label class="sup-check" title="${esc(s.desc)}">
                    <input type="checkbox" id="sup-inc-${s.key}" checked>
                    <span>${esc(s.label)}</span>
                  </label>`).join("")}
              </div>
              <div class="sup-actions">
                <button type="button" class="cw-page-button mt-button sup-btn primary" id="sup-bundle-download" aria-label="${TH("modals.support.downloadBundle")}">
                  <span class="material-symbols-rounded" aria-hidden="true">download</span><span>${TH("modals.support.downloadBtn")}</span>
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
                <strong id="sup-progress-title">${TH("modals.support.preparingExport")}</strong>
                <span id="sup-progress-sub">${TH("modals.support.starting")}</span>
              </div>
              <b id="sup-progress-percent">0%</b>
            </div>
            <div class="sup-progress-bar" aria-hidden="true"><span id="sup-progress-fill"></span></div>
            <div class="sup-progress-grid">
              <div><span>${TH("modals.support.export")}</span><b id="sup-progress-kind">-</b></div>
              <div><span>${TH("modals.support.scope")}</span><b id="sup-progress-scope">-</b></div>
              <div><span>${TH("modals.support.size")}</span><b id="sup-progress-size">-</b></div>
              <div><span>${TH("modals.support.elapsed")}</span><b id="sup-progress-elapsed">0s</b></div>
            </div>
            <div id="sup-progress-message" class="sup-progress-message">${TH("modals.support.preparingDownload")}</div>
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
        setStatus(T("modals.support.pairsUnreadable"), "warn");
      }
      const pairs = (Array.isArray(payload?.pairs) ? payload.pairs : []).map((pair, index) => ({ ...pair, order: index + 1 }));
      const pairOption = (value) => {
        const pair = pairs.find((item) => String(item?.id || "") === value);
        if (!pair) return null;
        const items = Number(pair.items || 0);
        const tags = [...(pair.enabled === false ? [T("modals.support.disabled")] : []), T("modals.support.items", {count: items})];
        return window.CW?.syncPairOptionData?.(pair, { tags }) || null;
      };
      const options = ['<option value="all">' + TH("modals.support.allPairs") + '</option>'].concat(pairs.map((p) => {
        const items = Number(p?.items || 0);
        const suffix = `${p?.enabled === false ? ` · ${T("modals.support.disabledLower")}` : ""} · ${T("modals.support.items", {count: items})}`;
        return `<option value="${esc(String(p?.id || ""))}">${esc(String(p?.label || p?.id || T("modals.support.pair")) + suffix)}</option>`;
      })).join("");
      root.querySelectorAll(".support-scope").forEach((sel) => {
        const current = sel.value || "all";
        sel.innerHTML = options;
        const values = Array.from(sel.options).map((o) => o.value);
        sel.value = values.includes(current) ? current : "all";
        window.CW?.IconSelect?.enhance?.(sel, { className: "cw-plain-select", getOptionData: pairOption });
      });

      const totals = payload?.totals || {};
      const stats = [
        [T("modals.support.syncPairs"), totals.pairs],
        [T("modals.support.baselines"), totals.baselines],
        [T("modals.support.baselineItems"), totals.items],
        [T("modals.support.unreferenced"), totals.orphan_baselines],
      ];
      const statsEl = $("#sup-stats", root);
      if (statsEl) {
        statsEl.innerHTML = stats.map(([label, value]) => `
          <div class="sup-stat">
            <div class="sup-stat-value">${esc(new Intl.NumberFormat(I18N()?.locale).format(Number(value || 0)))}</div>
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
        title: isBundle ? T("modals.support.buildingBundle") : T("modals.support.rebuildingState"),
        sub: T("modals.support.preparingExport"),
        message: isBundle ? T("modals.support.collecting") : T("modals.support.rebuildingFromDb"),
        kind: isBundle ? T("modals.support.bundle") : "state.json",
        scope: scope === "all" ? T("modals.support.allPairs") : T("modals.support.onePair"),
        size: T("modals.support.waiting"),
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
        title: T("modals.support.complete"),
        sub: T("modals.support.saved"),
        message: isBundle ? T("modals.support.bundleReady") : T("modals.support.stateReady"),
        kind: isBundle ? T("modals.support.bundle") : "state.json",
        scope: scope === "all" ? T("modals.support.allPairs") : T("modals.support.onePair"),
        size: formatBytes(blob?.size || 0),
        percent: 100,
        active: false,
        done: true,
      });
    };

    const failProgress = (message) => {
      stopProgressTimer();
      updateProgress({
        title: T("modals.support.failed"),
        sub: T("modals.support.stopped"),
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
      setStatus(isBundle ? T("modals.support.buildingBundleDots") : T("modals.support.rebuildingStateDots"), "busy");
      try {
        const { blob, filename } = await fblob(`/api/maintenance/support/${isBundle ? "bundle" : "state"}${query ? `?${query}` : ""}`, ({ received, total }) => {
          const knownTotal = Number.isFinite(total) && total > 0;
          const pct = knownTotal ? Math.max(progressPercent, Math.min(98, Math.round((received / total) * 100))) : Math.max(progressPercent, 92);
          updateProgress({
            title: isBundle ? T("modals.support.downloadingBundle") : T("modals.support.downloadingState"),
            sub: knownTotal ? T("modals.support.ofTotal", {received: formatBytes(received), total: formatBytes(total)}) : T("modals.support.downloadedBytes", {received: formatBytes(received)}),
            message: T("modals.support.receiving"),
            size: knownTotal ? `${formatBytes(received)} / ${formatBytes(total)}` : formatBytes(received),
            percent: pct,
            active: true,
          });
        });
        saveBlob(blob, filename || (isBundle ? "crosswatch-support.zip" : "crosswatch-state.json"));
        finishProgress({ isBundle, scope, blob });
        const bits = [filename || "download", formatBytes(blob.size), scope === "all" ? T("modals.support.allPairsLower") : T("modals.support.onePairLower")];
        if (isBundle) bits.push(sections.length ? sections.join(", ") : T("modals.support.stateOnly"));
        setStatus(T("modals.support.downloaded", {details: bits.join(" · ")}), "ok");
      } catch (e) {
        const message = T("modals.support.downloadFailed", {error: e?.message || String(e)});
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
