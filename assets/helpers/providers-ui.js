/* assets/helpers/providers-ui.js */
/* Extracted provider/auth/metadata UI from core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;
  const apiText = async (url) => {
    if (authSetupPending()) throw new Error("auth setup pending");
    if (window.CW?.API?.j) return window.CW.API.j(url);
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.text();
  };

  const listProviders = async (force = false) => {
    if (authSetupPending()) return [];
    if (window.CW?.API?.Providers?.list) return window.CW.API.Providers.list(!!force);
    return fetch("/api/sync/providers", { cache: "no-store" }).then((r) => r.json()).catch(() => []);
  };

  function bindCopyButton(btnId, inputId) {
    const btn = document.getElementById(btnId);
    if (!btn || btn.__cwBound) return;
    btn.addEventListener("click", (e) => window.copyInputValue?.(inputId, e.currentTarget));
    btn.__cwBound = true;
  }

  function wireCopyButtons() {
    bindCopyButton("btn-copy-plex-pin", "plex_pin");
    bindCopyButton("btn-copy-plex-token", "plex_token");
    bindCopyButton("btn-copy-trakt-pin", "trakt_pin");
    bindCopyButton("btn-copy-trakt-token", "trakt_token");
  }

  function collapseAuthSections(root) {
    if (!root) return;
    const want = ["media servers", "trackers", "others"];
    root.querySelectorAll(".section").forEach((sec) => {
      const text = (
        sec.querySelector(":scope > .head strong")?.textContent ||
        sec.querySelector(":scope > .head")?.textContent ||
        ""
      ).trim().toLowerCase();
      if (!want.some((w) => text.startsWith(w))) return;
      sec.classList.remove("open");
      const chev = sec.querySelector(":scope > .head .chev");
      if (chev) chev.textContent = "▶";
    });
    root.querySelectorAll("details").forEach((det) => {
      const text = (det.querySelector(":scope > summary")?.textContent || "").trim().toLowerCase();
      if (want.some((w) => text.startsWith(w))) det.open = false;
    });
  }

  function cxBrandInfo(name) {
    return window.CW?.ProviderMeta?.brandInfo?.(name) || { cls: "", icon: "" };
  }

  function cxBrandLogo(providerName) {
    return window.CW?.ProviderMeta?.logoHtml?.(providerName) || `<span class="token-text">${providerName || ""}</span>`;
  }

  function renderProviderToken(el, key) {
    if (!el) return;
    el.replaceChildren();
    if (!key) return;

    const src = window.CW?.ProviderMeta?.logoPath?.(key) || "";
    if (src) {
      const img = document.createElement("img");
      img.className = "token-logo";
      img.src = src;
      img.alt = `${key} logo`;
      img.width = 28;
      img.height = 28;
      img.loading = "lazy";
      el.appendChild(img);
      return;
    }

    const span = document.createElement("span");
    span.className = "token-text";
    span.textContent = String(key || "");
    el.appendChild(span);
  }

  function updateFlowRailLogos() {
    const rail = document.querySelector(".flow-rail.pretty");
    if (!rail) return;
    const tokens = rail.querySelectorAll(".token");
    if (!tokens.length) return;
    const keyOf = (id) => String(document.getElementById(id)?.value || "").trim().toUpperCase();
    renderProviderToken(tokens[0], keyOf("cx-src"));
    renderProviderToken(tokens[1], keyOf("cx-dst"));
  }

  let authHtml = "";
  let authInflight = null;
  async function mountAuthProviders(force = false) {
    if (authSetupPending()) return;
    if (authInflight) return authInflight;
    authInflight = (async () => {
      try {
        const slot = document.getElementById("auth-providers");
        if (!slot) return;
        if (!authHtml || force) authHtml = await apiText("/api/auth/providers/html");
        slot.innerHTML = authHtml;

        window.initMDBListAuthUI?.();
        window.initTautulliAuthUI?.();
        window.initAniListAuthUI?.();

        collapseAuthSections(slot);
        wireCopyButtons();

        ["trakt_client_id", "trakt_client_secret"].forEach((id) => {
          const el = document.getElementById(id);
          if (!el || el.__cwHintBound) return;
          el.addEventListener("input", () => window.updateTraktHint?.());
          el.__cwHintBound = true;
        });

        await window.hydrateAuthFromConfig?.();
        window.updateTraktHint?.();
        window.startTraktTokenPoll?.();
        setTimeout(() => window.updateTraktHint?.(), 0);
        requestAnimationFrame(() => window.updateTraktHint?.());
      } catch (e) {
        if (String(e?.message || e || "").includes("auth setup pending")) return;
        console.warn("mountAuthProviders failed", e);
      } finally {
        authInflight = null;
      }
    })();
    return authInflight;
  }

  let metadataHtml = "";
  let metadataInflight = null;
  async function mountMetadataProviders(force = false) {
    if (authSetupPending()) return;
    if (metadataInflight) return metadataInflight;
    metadataInflight = (async () => {
      try {
        const raw = document.getElementById("meta-provider-raw");
        const slot = raw || document.getElementById("metadata-providers");
        const panelHost = document.getElementById("meta-provider-panel");
        const tmdbPanel = panelHost?.querySelector('.cw-meta-provider-panel[data-provider="tmdb"]');
        const rawLoaded = slot?.dataset?.cwMetadataLoaded === "1";
        const rawHasMarkup = !!raw?.querySelector("#sec-tmdb, #tmdb_api_key, #metadata_locale, #metadata_ttl_hours");

        if (!force && rawLoaded && (tmdbPanel || rawHasMarkup)) {
          try { window.cwMetaProviderEnsure?.(); } catch {}
          try { window.updateTmdbHint?.(); } catch {}
          try { window.cwMetaProviderUpdateChips?.(); } catch {}
          return;
        }

        if (!metadataHtml || force) metadataHtml = await apiText("/api/metadata/providers/html");
        if (!slot) return;

        if (force && panelHost) {
          panelHost.querySelectorAll('.cw-meta-provider-panel[data-provider]').forEach((el) => el.remove());
          delete panelHost.dataset.__cwMetaBuilt;
        }

        slot.innerHTML = metadataHtml;
        slot.dataset.cwMetadataLoaded = "1";

        try { window.cwMetaProviderEnsure?.(); } catch {}
        try { window.updateTmdbHint?.(); } catch {}
        try { window.cwMetaProviderUpdateChips?.(); } catch {}
      } catch (e) {
        if (String(e?.message || e || "").includes("auth setup pending")) return;
        console.warn("mountMetadataProviders failed", e);
      } finally {
        metadataInflight = null;
      }
    })();
    return metadataInflight;
  }

  function normProviderKey(v = "") {
    const s = String(v).toUpperCase();
    if (/\bPLEX\b/.test(s)) return "PLEX";
    if (/\bSIMKL\b/.test(s)) return "SIMKL";
    if (/\bTRAKT\b/.test(s)) return "TRAKT";
    if (/\bANILIST\b/.test(s)) return "ANILIST";
    if (/\bJELLYFIN\b/.test(s)) return "JELLYFIN";
    if (/\bEMBY\b/.test(s)) return "EMBY";
    return s;
  }

  let providersInflight = null;
  async function loadProviders(force = false) {
    const div = document.getElementById("providers_list");
    if (!div) return [];
    if (providersInflight) return providersInflight;

    providersInflight = (async () => {
      try {
        const arr = await listProviders(!!force);
        if (!Array.isArray(arr) || !arr.length) {
          div.innerHTML = '<div class="muted">No providers discovered.</div>';
          return [];
        }

        window.cx = window.cx || {};
        window.cx.providers = arr;

        if (typeof window.renderConnections === "function") {
          try { window.renderConnections(); } catch (e) { console.warn("renderConnections failed", e); }
        } else {
          const chip = (label, on) => `<span class="badge ${on ? "" : "feature-disabled"}" style="margin-left:6px">${label}</span>`;
          div.innerHTML = arr.map((p) => {
            const key = normProviderKey(p.key || p.name || p.label);
            const caps = p.features || {};
            return `
              <div class="card prov-card" data-prov="${key}">
                <div style="padding:12px;display:flex;justify-content:space-between;align-items:center">
                  <div class="title" style="font-weight:700">${p.label || p.name || key}</div>
                  <div>
                    ${chip("Watchlist", !!caps.watchlist)}
                    ${chip("Ratings", !!caps.ratings)}
                    ${chip("History", !!caps.history)}
                    ${chip("Playlists", !!caps.playlists)}
                  </div>
                </div>
              </div>`;
          }).join("");
        }
        return arr;
      } catch (e) {
        div.innerHTML = '<div class="muted">Failed to load providers.</div>';
        console.warn("loadProviders error", e);
        return [];
      } finally {
        providersInflight = null;
        try {
          if (typeof window.scheduleApplySyncVisibility === "function") window.scheduleApplySyncVisibility();
          else window.applySyncVisibility?.();
        } catch {}
      }
    })();

    return providersInflight;
  }

  document.addEventListener("DOMContentLoaded", () => {
    if (authSetupPending()) return;
    wireCopyButtons();
    updateFlowRailLogos();
    ["cx-src", "cx-dst"].forEach((id) => document.getElementById(id)?.addEventListener("change", updateFlowRailLogos));
    try { mountMetadataProviders(); } catch {}
    try { mountAuthProviders(); } catch {}
    try { loadProviders(); } catch {}
  });

  const ProvidersUI = {
    cxBrandInfo,
    cxBrandLogo,
    updateFlowRailLogos,
    mountAuthProviders,
    mountMetadataProviders,
    loadProviders,
  };

  (window.CW ||= {}).ProvidersUI = ProvidersUI;
  Object.assign(window, ProvidersUI);
})();
