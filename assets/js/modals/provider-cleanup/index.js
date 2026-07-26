/* assets/js/modals/provider-cleanup/index.js */
/* Provider cleanup modal. */

const REQUEST_TIMEOUT_MS = 30_000;
const FEATURES = [
  { key: "watchlist", icon: "bookmark_remove", title: "Watchlist", desc: "Remove saved watchlist items from the selected profile." },
  { key: "ratings", icon: "star", title: "Ratings", desc: "Remove ratings from the selected profile." },
  { key: "history", icon: "history", title: "History", desc: "Remove watched history from the selected profile." },
  { key: "progress", icon: "resume", title: "Progress", desc: "Remove playback progress from the selected profile." },
];

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

async function apiJson(url, opts = {}) {
  const ctrl = new AbortController();
  const timeout = window.setTimeout(() => ctrl.abort(), REQUEST_TIMEOUT_MS);
  try {
    const r = await fetch(url, { cache: "no-store", ...opts, signal: ctrl.signal });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText || ""}`.trim());
    return await r.json();
  } catch (e) {
    if (e?.name === "AbortError") throw new Error("Request timed out");
    throw e;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function postJson(url, body) {
  return apiJson(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
}

function newProgressId() {
  try {
    if (window.crypto?.randomUUID) return window.crypto.randomUUID();
  } catch {}
  return `provider-cleanup-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function providerLabel(provider) {
  return String(provider?.label || provider?.id || "").trim() || "-";
}

function providerId(provider) {
  return String(provider?.id || "").trim().toUpperCase();
}

function configuredProviders(providers) {
  return (Array.isArray(providers) ? providers : []).filter((provider) => provider && provider.configured !== false);
}

function providerFeatures(provider) {
  const features = provider?.features || {};
  return FEATURES.filter((feature) => !!features[feature.key]);
}

function providerInstances(provider) {
  const instances = Array.isArray(provider?.instances) ? provider.instances : [];
  return instances.length ? instances : [{ id: "default", label: "Default", configured: true }];
}

function providerLogo(provider) {
  const id = typeof provider === "string" ? provider : providerId(provider);
  const meta = window.CW?.ProviderMeta || window.CW_PROVIDER_META || {};
  return meta.logLogoPath?.(id) || meta.logoPath?.(id) || "";
}

function resultLine(key, result) {
  if (!result) return `${key}: no result`;
  if (result.skipped) return `${key}: skipped (${result.reason || "not available"})`;
  const unresolved = result.unresolved_count != null
    ? Number(result.unresolved_count || 0)
    : (Array.isArray(result.unresolved) ? result.unresolved.length : 0);
  const bits = [`removed ${Number(result.removed || 0).toLocaleString()}`];
  if (result.count != null) bits.push(`had ${Number(result.count || 0).toLocaleString()}`);
  if (unresolved) bits.push(`${unresolved.toLocaleString()} unresolved`);
  if (result.remaining) bits.push(`${Number(result.remaining || 0).toLocaleString()} remaining`);
  return `${key}: ${bits.join(", ")}`;
}

function injectCss() {
  const existing = document.getElementById("cw-provider-cleanup-css");
  if (existing?.tagName === "LINK") return Promise.resolve();
  existing?.remove();
  const link = document.createElement("link");
  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  link.id = "cw-provider-cleanup-css";
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
    await injectCss();

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("cw-provider-cleanup-shell");
      shell.style.setProperty("--cxModalW", "860px");
      shell.style.setProperty("--cxModalMaxW", "860px");
      shell.style.setProperty("--cxModalMaxH", "720px");
    }

    let providers = [];
    let busy = false;
    let confirmClearUntil = 0;
    let confirmClearTimer = 0;

    root.innerHTML = `
      <div class="cw-provider-cleanup">
        <div class="cx-head">
          <div class="cc-head-left">
            <div class="cc-head-icon"><span class="material-symbols-rounded" aria-hidden="true">cleaning_services</span></div>
            <div>
              <div class="cc-title">Provider cleanup</div>
              <div class="cc-sub">Clear provider data by profile without leaving Maintenance.</div>
            </div>
          </div>
          <button type="button" class="cc-close" id="ccc-close" title="Close" aria-label="Close"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
        </div>
        <div class="cc-body cw-scrollbars">
          <section class="cc-target-card">
            <div class="cc-card-head">
              <span class="material-symbols-rounded" aria-hidden="true">target</span>
              <div>
                <h3>Provider target</h3>
                <p>Choose a configured provider and profile, then select which data to clear.</p>
              </div>
            </div>
            <div class="cc-target-grid">
              <label>
                <span>Provider</span>
                <div class="cc-provider-picker" id="ccc-provider-picker">
                  <select id="ccc-provider" class="cc-profile-native" data-cw-profile-kind="provider"></select>
                </div>
              </label>
              <label>
                <span>Profile</span>
                <div class="cc-provider-picker" id="ccc-instance-picker">
                  <select id="ccc-instance" class="cc-profile-native" data-cw-profile-kind="profile"></select>
                </div>
              </label>
            </div>
          </section>

          <section class="cc-feature-card">
            <div class="cc-card-head compact">
              <span class="material-symbols-rounded" aria-hidden="true">checklist</span>
              <div>
                <h3>Data to clear</h3>
                <p>Select the data you want to remove from the selected provider profile.</p>
              </div>
            </div>
            <div class="cc-feature-grid" id="ccc-features">
              ${FEATURES.map((feature) => `
                <button type="button" class="cc-feature" data-feature="${feature.key}" aria-pressed="false">
                  <span class="cc-feature-icon material-symbols-rounded" aria-hidden="true">${feature.icon}</span>
                  <span class="cc-feature-copy">
                    <strong>${feature.title}</strong>
                    <small>${feature.desc}</small>
                  </span>
                </button>
              `).join("")}
            </div>
          </section>

          <section class="cc-run-card">
            <div class="cc-warning">
              <span class="material-symbols-rounded" aria-hidden="true">warning</span>
              <div>This clears data on the selected provider profile.</div>
            </div>
            <div class="cc-actions">
              <button type="button" class="cc-btn danger cw-danger-confirm" id="ccc-clear-selected">
                <span class="material-symbols-rounded" aria-hidden="true">delete</span>
                <span>Clear selected data</span>
              </button>
            </div>
            <div id="ccc-status" class="cc-status" aria-live="polite"></div>
            <div id="ccc-progress" class="cc-progress hidden" aria-live="polite">
              <div class="cc-progress-head">
                <div>
                  <strong id="ccc-progress-title">Clearing provider data</strong>
                  <span id="ccc-progress-sub">Preparing cleanup...</span>
                </div>
                <b id="ccc-progress-percent">0%</b>
              </div>
              <div class="cc-progress-bar" aria-hidden="true"><span id="ccc-progress-fill"></span></div>
              <div class="cc-progress-grid">
                <div><span>Provider</span><b id="ccc-progress-provider">-</b></div>
                <div><span>Profile</span><b id="ccc-progress-instance">default</b></div>
                <div><span>Feature</span><b id="ccc-progress-feature">-</b></div>
                <div><span>Removed</span><b id="ccc-progress-items">-</b></div>
              </div>
              <div id="ccc-progress-message" class="cc-progress-message">Starting cleanup...</div>
            </div>
          </section>
        </div>
      </div>
    `;

    const setBusy = (on) => {
      busy = !!on;
      window.cxSetModalDismissible?.(!busy);
      $$("button, select", root).forEach((el) => { el.disabled = busy || !!el.dataset.disabledByFeature; });
      $("#ccc-clear-selected", root).classList.toggle("busy", busy);
      if (!busy) updateAvailability();
    };

    const setStatus = (message, kind = "") => {
      const status = $("#ccc-status", root);
      if (!status) return;
      status.textContent = String(message || "");
      status.className = `cc-status${kind ? ` ${kind}` : ""}`;
      status.classList.toggle("hidden", !message);
    };

    const resetClearButton = () => {
      const btn = $("#ccc-clear-selected", root);
      if (!btn) return;
      btn.classList.remove("is-confirming");
      btn.innerHTML = `
        <span class="material-symbols-rounded" aria-hidden="true">delete</span>
        <span>Clear selected data</span>
      `;
    };

    const armClearConfirm = () => {
      const btn = $("#ccc-clear-selected", root);
      confirmClearUntil = Date.now() + 4200;
      if (!btn) return;
      btn.classList.add("is-confirming");
      btn.innerHTML = `
        <span class="material-symbols-rounded" aria-hidden="true">warning</span>
        <span>Confirm clear</span>
      `;
      if (confirmClearTimer) window.clearTimeout(confirmClearTimer);
      confirmClearTimer = window.setTimeout(clearConfirm, 4200);
    };

    const clearConfirm = () => {
      confirmClearUntil = 0;
      if (confirmClearTimer) window.clearTimeout(confirmClearTimer);
      confirmClearTimer = 0;
      resetClearButton();
    };

    const clearFeatureSelection = () => {
      $$(".cc-feature", root).forEach((button) => {
        button.classList.remove("selected");
        button.setAttribute("aria-pressed", "false");
      });
    };

    const progressFeatureLabel = (value) => {
      const raw = String(value || "").trim().toLowerCase();
      if (!raw || raw === "cleanup") return "-";
      return raw.charAt(0).toUpperCase() + raw.slice(1);
    };

    const progressItemText = (progress) => {
      const done = Number(progress?.items_done ?? progress?.total_items);
      const current = Number(progress?.feature_items);
      if (Number.isFinite(done) && done > 0 && Number.isFinite(current) && current > 0 && done !== current) {
        return `${done.toLocaleString()} total, ${current.toLocaleString()} current`;
      }
      if (Number.isFinite(done) && done > 0) return `${done.toLocaleString()} removed`;
      if (Number.isFinite(current) && current > 0) return `${current.toLocaleString()} current`;
      return "-";
    };

    const updateProgress = (progress = {}) => {
      const panel = $("#ccc-progress", root);
      if (!panel) return;
      const percent = Math.max(0, Math.min(100, Math.round(Number(progress.percent || 0))));
      const failed = (!!progress.done && progress.ok === false) || String(progress.stage || "").toLowerCase() === "error";
      panel.classList.remove("hidden");
      panel.classList.toggle("active", !progress.done && !failed);
      panel.classList.toggle("done", !!progress.done && !failed);
      panel.classList.toggle("error", failed);
      $("#ccc-progress-fill", root)?.style.setProperty("width", `${percent}%`);
      const pct = $("#ccc-progress-percent", root);
      if (pct) pct.textContent = `${percent}%`;
      const title = $("#ccc-progress-title", root);
      if (title) title.textContent = failed ? "Cleanup failed" : progress.done ? "Cleanup complete" : "Clearing provider data";
      const featureTotal = Number(progress.feature_total || 1);
      const featureIndex = Number(progress.feature_index || 0);
      const sub = $("#ccc-progress-sub", root);
      if (sub) sub.textContent = featureTotal > 1 && featureIndex > 0 ? `Feature ${featureIndex} of ${featureTotal}` : "Provider cleanup";
      const provider = $("#ccc-progress-provider", root);
      if (provider) provider.textContent = String(progress.provider || $("#ccc-provider", root)?.value || "-").toUpperCase();
      const instance = $("#ccc-progress-instance", root);
      if (instance) instance.textContent = String(progress.instance || $("#ccc-instance", root)?.value || "default");
      const feature = $("#ccc-progress-feature", root);
      if (feature) feature.textContent = progressFeatureLabel(progress.current_feature || progress.feature);
      const items = $("#ccc-progress-items", root);
      if (items) items.textContent = progressItemText(progress);
      const msg = $("#ccc-progress-message", root);
      if (msg) msg.textContent = String(progress.message || "Working...");
    };

    const waitForProgress = async (progressId) => {
      const id = String(progressId || "");
      let misses = 0;
      while (id) {
        try {
          const data = await apiJson(`/api/snapshots/capture-progress/${encodeURIComponent(id)}?_=${Date.now()}`);
          const progress = data?.progress || {};
          misses = 0;
          updateProgress(progress);
          if (progress.done) return progress;
        } catch (e) {
          misses += 1;
          updateProgress({ stage: "waiting", message: "Waiting for cleanup progress...", percent: 8 });
          if (misses >= 8) throw e;
        }
        await sleep(700);
      }
      return null;
    };

    const currentProvider = () => {
      const id = String($("#ccc-provider", root)?.value || "").toUpperCase();
      return providers.find((provider) => providerId(provider) === id) || null;
    };

    const enhanceTargetSelects = () => {
      const providerSelect = $("#ccc-provider", root);
      const profileSelect = $("#ccc-instance", root);
      const helper = window.CW?.ProfileSelect;
      if (helper?.enhancePair) helper.enhancePair(providerSelect, profileSelect, { provider: { className: "cc-target-select" }, profile: { className: "cc-target-select" } });
      else {
        window.CW?.IconSelect?.enhance?.(providerSelect, { className: "cw-profile-select cc-target-select" });
        window.CW?.IconSelect?.enhance?.(profileSelect, { className: "cw-profile-select cc-target-select" });
      }
    };

    const fillProviders = () => {
      const select = $("#ccc-provider", root);
      if (!select) return;
      const rows = configuredProviders(providers);
      select.innerHTML = rows.length
        ? rows.map((provider) => {
            const id = providerId(provider);
            return `<option value="${esc(id)}" data-provider="${esc(id)}" data-label="${esc(providerLabel(provider))}" data-icon="${esc(providerLogo(provider))}">${esc(providerLabel(provider))}</option>`;
          }).join("")
        : `<option value="">No configured providers</option>`;
      enhanceTargetSelects();
    };

    const fillInstances = () => {
      const select = $("#ccc-instance", root);
      const provider = currentProvider();
      if (!select) return;
      const rows = providerInstances(provider);
      select.innerHTML = rows.map((instance) => {
        const id = String(instance?.id || "default");
        const label = String(instance?.label || id || "Default");
        const configured = instance?.configured !== false;
        return `<option value="${esc(id)}" data-profile="${esc(id)}" data-label="${esc(label)}" ${configured ? "" : "disabled"}>${esc(configured ? label : `${label} (not configured)`)}</option>`;
      }).join("");
      const firstAvailable = Array.from(select.options).find((option) => !option.disabled);
      if (firstAvailable) select.value = firstAvailable.value;
      enhanceTargetSelects();
    };

    const selectedFeatures = () => $$(".cc-feature.selected", root).map((button) => String(button.dataset.feature || "")).filter(Boolean);

    function updateAvailability() {
      const provider = currentProvider();
      const available = new Set(providerFeatures(provider).map((feature) => feature.key));
      const instanceSelect = $("#ccc-instance", root);
      const instanceOk = !!instanceSelect?.selectedOptions?.[0] && !instanceSelect.selectedOptions[0].disabled;
      $$(".cc-feature", root).forEach((row) => {
        const key = String(row.dataset.feature || "");
        const enabled = !!provider && instanceOk && available.has(key);
        row.classList.toggle("disabled", !enabled);
        row.disabled = busy || !enabled;
        row.dataset.disabledByFeature = enabled ? "" : "1";
        if (!enabled) row.classList.remove("selected");
        row.setAttribute("aria-pressed", row.classList.contains("selected") ? "true" : "false");
      });
      const anySelected = selectedFeatures().length > 0;
      const clearBtn = $("#ccc-clear-selected", root);
      if (clearBtn) clearBtn.disabled = busy || !provider || !instanceOk || !anySelected;
    }

    const refresh = async () => {
      setStatus("Loading provider targets...", "busy");
      try {
        const data = await apiJson(`/api/snapshots/manifest?_=${Date.now()}`);
        providers = Array.isArray(data?.providers) ? data.providers : [];
        fillProviders();
        fillInstances();
        updateAvailability();
        setStatus(configuredProviders(providers).length ? "" : "No configured providers found.", configuredProviders(providers).length ? "" : "err");
      } catch (e) {
        console.warn("[provider-cleanup] manifest failed", e);
        setStatus(`Unable to load providers: ${e.message || e}`, "err");
      }
    };

    $("#ccc-close", root)?.addEventListener("click", () => {
      if (!busy) window.cxCloseModal?.();
    });
    $("#ccc-provider", root)?.addEventListener("change", () => { clearConfirm(); clearFeatureSelection(); fillInstances(); updateAvailability(); });
    $("#ccc-instance", root)?.addEventListener("change", () => { clearConfirm(); clearFeatureSelection(); updateAvailability(); });
    $$("#ccc-features .cc-feature", root).forEach((button) => button.addEventListener("click", () => {
      if (busy || button.disabled) return;
      clearConfirm();
      button.classList.toggle("selected");
      updateAvailability();
    }));
    $("#ccc-clear-selected", root)?.addEventListener("click", async () => {
      const provider = String($("#ccc-provider", root)?.value || "").toUpperCase();
      const instance = String($("#ccc-instance", root)?.value || "default");
      const features = selectedFeatures();
      if (!provider || !features.length) return;
      if (Date.now() > confirmClearUntil) {
        armClearConfirm();
        return;
      }
      clearConfirm();

      setBusy(true);
      const progressId = newProgressId();
      updateProgress({
        ok: true,
        done: false,
        stage: "queued",
        message: "Starting provider cleanup...",
        percent: 1,
        provider,
        instance,
        feature: "cleanup",
        feature_total: features.length,
      });
      setStatus("Cleanup is running. This window stays locked until it finishes.", "busy");
      try {
        await postJson("/api/snapshots/tools/clear", { provider, instance, features, progress_id: progressId, background: true });
        const progress = await waitForProgress(progressId);
        if (!progress) throw new Error("Cleanup progress was interrupted.");
        if (progress.ok === false) throw new Error(progress.error || progress.message || "Cleanup failed");
        const results = progress.cleanup_results || {};
        const lines = Object.keys(results).map((key) => resultLine(key, results[key]));
        setStatus(lines.length ? lines.join(" | ") : (progress.message || "Cleanup complete."), "ok");
        window.CW?.DOM?.showToast?.("Provider cleanup complete", true);
      } catch (e) {
        console.warn("[provider-cleanup] clear failed", e);
        setStatus(`Cleanup failed: ${e.message || e}`, "err");
        window.CW?.DOM?.showToast?.(`Cleanup failed: ${e.message || e}`, false);
      } finally {
        setBusy(false);
      }
    });

    await refresh();
  },
};
