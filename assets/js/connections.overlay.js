// assets/js/connections.overlay.js
/* Provider cards overlay for the Sync section. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function () {
  let _renderBusy = false;
  let _pick = { source: "", target: "" };

  const key = (s) => String(s || "").trim().toUpperCase();
  const providerMeta = () => window.CW?.ProviderMeta || null;
  const providerLabel = (item, providerKey) => providerMeta()?.label?.(providerKey) || String(item?.label || item?.name || providerKey || "Provider");
  const providerClass = (providerKey) => providerMeta()?.brandInfo?.(providerKey)?.cls || "";
  const providerLogo = (providerKey) => providerMeta()?.logoPath?.(providerKey) || "";
  const truthy = (v) => {
    if (v && typeof v === "object") v = v.enable ?? v.enabled;
    if (typeof v === "string") v = v.toLowerCase().trim();
    return v === true || v === 1 || v === "1" || v === "true" || v === "on" || v === "yes";
  };

  const FEATURE_ORDER = [
    ["watchlist", "wl", "Watchlist"],
    ["ratings", "rt", "Ratings"],
    ["history", "hi", "History"],
    ["progress", "pr", "Progress"],
    ["playlists", "pl", "Playlists"],
  ];


  function ensureHost() {
    const host = document.getElementById("providers_list");
    if (!host) return null;
    let board = host.querySelector(":scope > .providers-board");
    if (!board) {
      host.innerHTML = "";
      board = document.createElement("div");
      board.className = "providers-board";
      host.appendChild(board);
    }
    return { host, board };
  }

  function syncLegacySelectors(source = "", target = "") {
    const src = document.getElementById("source-provider");
    const dst = document.getElementById("target-provider");
    if (src) src.value = source || "";
    if (dst) dst.value = target || "";
  }

  function featureDots(features) {
    return FEATURE_ORDER.map(([field, cls, label]) => {
      const on = truthy(features?.[field]);
      return `<span class="prov-dot ${cls} ${on ? "on" : ""}" title="${label}"></span>`;
    }).join("");
  }

  function renderCards(providers) {
    const containers = ensureHost();
    if (!containers) return;
    const { host, board } = containers;
    const visibleProviders = Array.isArray(providers) && providers.some((item) => typeof item?.configured === "boolean")
      ? providers.filter((item) => item?.configured !== false)
      : providers;

    if (!Array.isArray(visibleProviders) || !visibleProviders.length) {
      board.innerHTML = '<div class="prov-empty">No providers discovered.</div>';
      syncLegacySelectors();
      return;
    }

    const source = key(_pick.source);
    const target = key(_pick.target);

    board.innerHTML = visibleProviders.map((item) => {
      const providerKey = key(item.key || item.name || item.label);
      const label = providerLabel(item, providerKey);
      const cls = providerClass(providerKey);
      const isSource = providerKey === source;
      const isTarget = providerKey === target;
      const btnClass = isSource ? "selected" : (source && !isTarget ? "target" : "");
      const btnText = isSource ? "Clear Source" : (source ? "Set as Target" : "Set as Source");
      const logo = providerLogo(providerKey);
      const watermarkStyle = logo ? ` style="--wm:url('${logo}')"` : "";
      const badge = isSource
        ? '<span class="prov-badge">Source selected</span>'
        : (isTarget ? '<span class="prov-badge">Target selected</span>' : "");

      return `
        <article class="card prov-card ${cls} ${isSource ? "is-source" : ""} ${isTarget ? "is-target" : ""}" data-prov="${providerKey}" data-sync-prov="${providerKey}">
          <div class="prov-watermark" aria-hidden="true"${watermarkStyle}></div>
          <div class="prov-main">
            <div class="prov-title">${label}</div>
            <div class="prov-features" aria-label="Supported features">${featureDots(item.features || {})}</div>
            <div class="prov-actions">
              <button type="button" class="prov-btn ${btnClass}" data-action="pick" data-prov="${providerKey}">${btnText}</button>
              ${badge}
            </div>
          </div>
        </article>`;
    }).join("");

    syncLegacySelectors(source, target);
    try { window.scheduleApplySyncVisibility?.(); } catch {}
  }

  function resetPick() {
    _pick.source = "";
    _pick.target = "";
    syncLegacySelectors();
    renderConnections();
  }

  function openPairModal(source, target) {
    const src = key(source);
    const dst = key(target);
    if (!src || !dst || src === dst) return;

    _pick.target = dst;
    syncLegacySelectors(src, dst);
    renderConnections();

    const payload = {
      source: src,
      target: dst,
      mode: "one-way",
      enabled: true,
      source_instance: "default",
      target_instance: "default",
    };

    try {
      if (typeof window.cxOpenModalFor === "function") {
        window.cxOpenModalFor(payload);
        return;
      }
      if (typeof window.openPairModal === "function") {
        window.openPairModal(payload);
      }
    } catch (e) {
      console.warn("[connections.overlay] open pair modal failed", e);
    }
  }

  function handlePick(provider) {
    const prov = key(provider);
    if (!prov) return;

    if (!_pick.source) {
      _pick.source = prov;
      _pick.target = "";
      syncLegacySelectors(prov, "");
      renderConnections();
      return;
    }

    if (_pick.source === prov) {
      _pick.source = "";
      _pick.target = "";
      syncLegacySelectors();
      renderConnections();
      return;
    }

    openPairModal(_pick.source, prov);
  }

  function wireEvents(host) {
    if (!host || host.__cxConnectionsBound) return;

    host.addEventListener("click", (ev) => {
      const btn = ev.target.closest?.(".prov-btn[data-action='pick']");
      if (!btn || !host.contains(btn)) return;
      handlePick(btn.dataset.prov || "");
    });

    host.__cxConnectionsBound = true;
  }

  async function loadProvidersIfNeeded(force = false) {
    if (!force && Array.isArray(window.cx?.providers) && window.cx.providers.length) return window.cx.providers;

    try {
      const arr = typeof window.loadProviders === "function"
        ? await window.loadProviders(!!force)
        : await fetch("/api/sync/providers", { cache: "no-store" }).then((r) => r.ok ? r.json() : []);
      window.cx = window.cx || {};
      window.cx.providers = Array.isArray(arr) ? arr : [];
    } catch (e) {
      window.cx = window.cx || {};
      if (!Array.isArray(window.cx.providers)) window.cx.providers = [];
      console.warn("[connections.overlay] provider fetch failed", e);
    }

    return window.cx.providers;
  }

  function renderConnections() {
    const host = document.getElementById("providers_list");
    if (!host) return;
    wireEvents(host);
    renderCards(Array.isArray(window.cx?.providers) ? window.cx.providers : []);
  }

  async function renderOrEnhance(force = false) {
    if (_renderBusy) return;
    _renderBusy = true;
    try {
      await loadProvidersIfNeeded(!!force);
      renderConnections();
    } finally {
      _renderBusy = false;
    }
  }

  document.addEventListener("DOMContentLoaded", renderOrEnhance);
  document.addEventListener("cw-settings-pane-changed", (ev) => {
    if (String(ev?.detail?.pane || "").toLowerCase() === "sync") renderOrEnhance(true);
  });
  document.addEventListener("cx-state-change", renderConnections);
  window.addEventListener("cx:pairs:changed", resetPick);

  window.renderConnections = renderConnections;
  window.cxRenderConnections = renderOrEnhance;
  window.cxResetConnectionPick = resetPick;
})();
