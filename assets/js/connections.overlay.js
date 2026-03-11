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

  function ensureStyles() {
    const css = `
#providers_list{display:block!important;width:100%!important;scrollbar-gutter:stable;overscroll-behavior:contain}
#providers_list .providers-board{display:grid!important;width:100%!important;grid-template-columns:repeat(auto-fit,minmax(300px,360px))!important;gap:14px!important;align-items:start!important;justify-content:start!important}
#providers_list .prov-card{position:relative;min-height:124px;padding:14px 14px 16px;border-radius:22px;background:linear-gradient(180deg,rgba(8,10,18,.96),rgba(5,7,14,.94));isolation:isolate;overflow:hidden}
#providers_list .prov-card::before{content:"";position:absolute;inset:8px;pointer-events:none;border-radius:18px;border:1px solid rgba(255,255,255,.10);opacity:.7}
#providers_list .prov-card::after{content:"";position:absolute;inset:-30% 20% auto -10%;height:70%;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.10),transparent 55%);opacity:.28;transform:rotate(-12deg)}
#providers_list .prov-card > *{position:relative;z-index:1}
#providers_list .prov-card .prov-watermark{position:absolute;inset:0;z-index:0;opacity:1}
#providers_list .prov-card .prov-watermark::after{right:var(--wm-right,-7%);bottom:var(--wm-bottom,-12%);width:var(--wm-width,78%);height:var(--wm-height,92%);opacity:.16;transform:scale(1.42);transform-origin:108% 94%;filter:grayscale(.12) brightness(1.06) saturate(1.08);mix-blend-mode:screen}
#providers_list .prov-card.brand-crosswatch .prov-watermark::after{width:84%;height:106%;right:-12%;bottom:-16%;transform:scale(1.48)}
#providers_list .prov-card.brand-mdblist .prov-watermark::after{width:70%;height:98%;right:-4%;bottom:-10%;transform:scale(1.4)}
#providers_list .prov-card.brand-plex .prov-watermark::after{width:72%;height:98%;right:-2%;bottom:-8%;transform:scale(1.46)}
#providers_list .prov-card.brand-simkl .prov-watermark::after{width:74%;height:100%;right:-4%;bottom:-10%;transform:scale(1.48)}
#providers_list .prov-card.brand-trakt .prov-watermark::after{width:72%;height:98%;right:-2%;bottom:-8%;transform:scale(1.46)}
#providers_list .prov-card.brand-anilist .prov-watermark::after{width:74%;height:100%;right:-4%;bottom:-10%;transform:scale(1.46)}
#providers_list .prov-card.brand-emby .prov-watermark::after{width:76%;height:102%;right:-5%;bottom:-11%;transform:scale(1.44)}
#providers_list .prov-card.brand-jellyfin .prov-watermark::after{width:88%;height:112%;right:-14%;bottom:-18%;transform:scale(1.44)}
#providers_list .prov-card.brand-tmdb-sync .prov-watermark::after{width:86%;height:58%;right:-10%;bottom:7%;transform:scale(1.32);transform-origin:104% 52%}
#providers_list .prov-card.brand-tautulli .prov-watermark::after{width:76%;height:102%;right:-5%;bottom:-10%;transform:scale(1.46)}
#providers_list .prov-main{display:flex;flex-direction:column;align-items:flex-start;justify-content:space-between;min-height:92px;gap:10px}
#providers_list .prov-title{font-size:1rem;font-weight:900;line-height:1.08;letter-spacing:.04em;text-transform:uppercase;color:#f6f7fb;text-shadow:0 1px 0 rgba(0,0,0,.35)}
#providers_list .prov-features{display:inline-flex;align-items:center;gap:8px;padding:0;margin:0}
#providers_list .prov-dot{width:11px;height:11px;border-radius:999px;display:inline-block;background:rgba(255,255,255,.18);box-shadow:inset 0 0 0 2px rgba(255,255,255,.14)}
#providers_list .prov-dot.on{box-shadow:none}
#providers_list .prov-dot.wl.on{background:#00ffa3;box-shadow:0 0 8px rgba(0,255,163,.95),0 0 18px rgba(0,255,163,.45)}
#providers_list .prov-dot.rt.on{background:#ffc400;box-shadow:0 0 8px rgba(255,196,0,.95),0 0 18px rgba(255,196,0,.42)}
#providers_list .prov-dot.hi.on{background:#2de2ff;box-shadow:0 0 8px rgba(45,226,255,.95),0 0 18px rgba(45,226,255,.42)}
#providers_list .prov-dot.pr.on{background:#a78bfa;box-shadow:0 0 8px rgba(167,139,250,.95),0 0 18px rgba(167,139,250,.42)}
#providers_list .prov-dot.pl.on{background:#ff00e5;box-shadow:0 0 8px rgba(255,0,229,.95),0 0 18px rgba(255,0,229,.42)}
#providers_list .prov-actions{display:flex;gap:8px;flex-wrap:nowrap;align-items:center}
#providers_list .prov-btn{appearance:none;-webkit-appearance:none;display:inline-flex;align-items:center;justify-content:center;min-width:148px;min-height:44px;padding:10px 16px;white-space:nowrap;border-radius:16px;border:1px solid rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(12,14,30,.96),rgba(7,8,20,.96));color:#f3f5fb;font-size:.96rem;font-weight:850;letter-spacing:.01em;cursor:pointer;box-shadow:0 10px 22px rgba(0,0,0,.32),inset 0 1px 0 rgba(255,255,255,.06);transition:transform .14s ease,box-shadow .18s ease,border-color .18s ease,background .18s ease}
#providers_list .prov-btn:hover{transform:translateY(-1px);box-shadow:0 14px 28px rgba(0,0,0,.38),inset 0 1px 0 rgba(255,255,255,.10)}
#providers_list .prov-btn:active{transform:translateY(0)}
#providers_list .prov-btn.target{border-color:rgba(255,255,255,.22);background:linear-gradient(180deg,rgba(18,21,38,.98),rgba(8,10,22,.98))}
#providers_list .prov-btn.selected{border-color:rgba(255,255,255,.24);background:linear-gradient(180deg,rgba(19,24,40,.98),rgba(10,13,26,.98))}
#providers_list .prov-badge{display:inline-flex;align-items:center;gap:8px;padding:7px 10px;white-space:nowrap;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);font-weight:800;font-size:.75rem;letter-spacing:.03em;text-transform:uppercase;color:#eef2ff}
#providers_list .prov-badge::before{content:"";width:8px;height:8px;border-radius:999px;background:currentColor;opacity:.9}
#providers_list .prov-card.is-source .prov-badge{color:#7c5cff;box-shadow:0 0 16px rgba(124,92,255,.22)}
#providers_list .prov-card.is-target .prov-badge{color:#19c37d;box-shadow:0 0 16px rgba(25,195,125,.18)}
#providers_list .prov-empty{padding:14px 0;color:var(--muted,#9aa4b2)}
@media (max-width:900px){#providers_list .providers-board{grid-template-columns:repeat(auto-fill,minmax(260px,1fr))!important}}
@media (max-width:760px){#providers_list .prov-actions{flex-wrap:wrap}}
@media (max-width:640px){#providers_list .providers-board{grid-template-columns:1fr!important}}
`;

    let el = document.getElementById("cx-connections-style");
    if (!el) {
      el = document.createElement("style");
      el.id = "cx-connections-style";
      document.head.appendChild(el);
    }
    el.textContent = css;
  }

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

    if (!Array.isArray(providers) || !providers.length) {
      board.innerHTML = '<div class="prov-empty">No providers discovered.</div>';
      syncLegacySelectors();
      return;
    }

    const source = key(_pick.source);
    const target = key(_pick.target);

    board.innerHTML = providers.map((item) => {
      const providerKey = key(item.key || item.name || item.label);
      const label = providerLabel(item, providerKey);
      const cls = providerClass(providerKey);
      const isSource = providerKey === source;
      const isTarget = providerKey === target;
      const btnClass = isSource ? "selected" : (source && !isTarget ? "target" : "");
      const btnText = isSource ? "Clear Source" : (source ? "Set as Target" : "Set as Source");
      const badge = isSource
        ? '<span class="prov-badge">Source selected</span>'
        : (isTarget ? '<span class="prov-badge">Target selected</span>' : "");

      return `
        <article class="card prov-card ${cls} ${isSource ? "is-source" : ""} ${isTarget ? "is-target" : ""}" data-prov="${providerKey}" data-sync-prov="${providerKey}">
          <div class="prov-watermark" aria-hidden="true"></div>
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

  async function loadProvidersIfNeeded() {
    if (Array.isArray(window.cx?.providers) && window.cx.providers.length) return window.cx.providers;

    try {
      const arr = typeof window.loadProviders === "function"
        ? await window.loadProviders()
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
    ensureStyles();
    const host = document.getElementById("providers_list");
    if (!host) return;
    wireEvents(host);
    renderCards(Array.isArray(window.cx?.providers) ? window.cx.providers : []);
  }

  async function renderOrEnhance() {
    if (_renderBusy) return;
    _renderBusy = true;
    try {
      await loadProvidersIfNeeded();
      renderConnections();
    } finally {
      _renderBusy = false;
    }
  }

  document.addEventListener("DOMContentLoaded", renderOrEnhance);
  document.addEventListener("cx-state-change", renderConnections);
  window.addEventListener("cx:pairs:changed", resetPick);

  window.renderConnections = renderConnections;
  window.cxRenderConnections = renderOrEnhance;
  window.cxResetConnectionPick = resetPick;
})();
