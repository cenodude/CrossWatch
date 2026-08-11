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

async function fblob(url) {
  const ctrl = new AbortController();
  const timer = window.setTimeout(() => ctrl.abort(timeoutError("Download", DOWNLOAD_TIMEOUT_MS)), DOWNLOAD_TIMEOUT_MS);
  try {
    const r = await fetch(url, { cache: "no-store", signal: ctrl.signal });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText || ""}`.trim());
    const disposition = r.headers.get("content-disposition") || "";
    const match = /filename\*?=(?:UTF-8''|")?([^";]+)/i.exec(disposition);
    return { blob: await r.blob(), filename: match ? decodeURIComponent(match[1].replace(/"$/, "")) : "" };
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
    const setBusy = (on) => {
      busy = on;
      root.querySelectorAll(".sup-btn, .sup-select, .sup-check input").forEach((el) => { el.disabled = on; });
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
      setStatus(isBundle ? "Building support bundle..." : "Rebuilding state.json...", "busy");
      try {
        const { blob, filename } = await fblob(`/api/maintenance/support/${isBundle ? "bundle" : "state"}${query ? `?${query}` : ""}`);
        saveBlob(blob, filename || (isBundle ? "crosswatch-support.zip" : "crosswatch-state.json"));
        const bits = [filename || "download", formatBytes(blob.size), scope === "all" ? "all pairs" : "1 pair"];
        if (isBundle) bits.push(sections.length ? sections.join(", ") : "state only");
        setStatus(`Downloaded · ${bits.join(" · ")}.`, "ok");
      } catch (e) {
        setStatus(`Download failed: ${e?.message || String(e)}`, "err");
      } finally {
        setBusy(false);
      }
    };

    $("#sup-state-download", root)?.addEventListener("click", () => download("state"));
    $("#sup-bundle-download", root)?.addEventListener("click", () => download("bundle"));

    await loadScopes();
    setStatus("");
  },
  unmount() {},
};
