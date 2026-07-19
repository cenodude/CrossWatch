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
    if (btn.getAttribute("onclick")) {
      btn.__cwBound = true;
      return;
    }
    btn.addEventListener("click", (e) => window.CW.AuthShared.copyField(inputId, e.currentTarget));
    btn.__cwBound = true;
  }

  function wireCopyButtons() {
    bindCopyButton("btn-copy-plex-pin", "plex_pin");
    bindCopyButton("btn-copy-trakt-pin", "trakt_pin");
  }

  function initMountedAuthSections(root, tries = 0) {
    if (!root) return;
    if (typeof window.cwEnsureAuthSection !== "function") {
      if (tries < 20) setTimeout(() => initMountedAuthSections(root, tries + 1), 50);
      return;
    }
    const sections = Array.from(root.querySelectorAll(".section.open[id]"));
    sections.forEach((sec) => {
      try { window.cwEnsureAuthSection(sec.id).catch(() => {}); } catch {}
    });
  }

  const AUTH_GROUPS = Object.freeze([
    { id: "sec-auth-media", title: "Media servers", keys: ["PLEX", "JELLYFIN", "EMBY"] },
    { id: "sec-auth-trackers", title: "Trackers", keys: ["TRAKT", "SIMKL", "TMDB", "MDBLIST", "PUBLICMETADB", "ANILIST"] },
    { id: "sec-auth-others", title: "Others", keys: ["TAUTULLI"] },
  ]);
  const AUTH_GROUP_BY_KEY = Object.freeze(Object.fromEntries(AUTH_GROUPS.flatMap((group) => group.keys.map((key) => [key, group.id]))));
  const META_GROUP = Object.freeze({ id: "sec-auth-metadata", title: "Metadata", keys: ["TMDB_METADATA", "ANIME_MAPPING"] });
  const META_ITEMS = Object.freeze({
    TMDB_METADATA: { key: "TMDB_METADATA", label: "TMDb Metadata", logoKey: "TMDB", sectionId: "sec-meta-tmdb", provider: "tmdb" },
    ANIME_MAPPING: { key: "ANIME_MAPPING", label: "Anime ID Mapping", logoKey: "ANILIST", sectionId: "sec-meta-anime-mapping", provider: "anime-mapping" },
  });

  function providerMeta() {
    return window.CW?.ProviderMeta || {};
  }

  function authProviderInfo(key) {
    const meta = providerMeta();
    const norm = typeof meta.keyOf === "function" ? meta.keyOf(key) : String(key || "").trim().toUpperCase();
    return {
      key: norm,
      label: typeof meta.label === "function" ? meta.label(norm) : norm,
      sectionId: typeof meta.sectionId === "function" ? meta.sectionId(norm) : "",
      groupId: typeof meta.authGroupId === "function" ? meta.authGroupId(norm) : (AUTH_GROUP_BY_KEY[norm] || "sec-auth-others"),
    };
  }

  function metadataProviderInfo(key) {
    const norm = String(key || "").trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_");
    return META_ITEMS[norm] || null;
  }

  function getCachedConfig() {
    return window.CW?.Cache?.getCfg?.() || window._cfgCache || {};
  }

  async function loadConfig(force = false) {
    try {
      if (typeof window.CW?.API?.Config?.load === "function") {
        const cfg = await window.CW.API.Config.load(!!force);
        if (cfg && typeof cfg === "object") return cfg;
      }
    } catch {}
    try {
      const res = await fetch("/api/config", { cache: "no-store" });
      if (res.ok) return await res.json();
    } catch {}
    return getCachedConfig();
  }

  function configuredProviderKeys(cfg = getCachedConfig()) {
    try {
      if (typeof window.getConfiguredProviders === "function") {
        return new Set(Array.from(window.getConfiguredProviders(cfg) || []).map((key) => authProviderInfo(key).key).filter(Boolean));
      }
    } catch {}
    return new Set();
  }

  function statusProviderData(key) {
    const providers = window.loadStatusCache?.()?.providers || window._statusCache?.providers || {};
    if (!providers || typeof providers !== "object") return null;
    const upper = String(key || "").toUpperCase();
    const lower = upper.toLowerCase();
    return providers[lower] || providers[upper] || null;
  }

  function authStatusFor(key, configured) {
    const data = statusProviderData(key);
    if (data && typeof data.connected === "boolean") {
      return data.connected ? { text: "Connected", ok: true } : { text: "Check connection", ok: false };
    }
    return configured ? { text: "Configured", ok: true } : { text: "Not configured", ok: false };
  }

  function authProviderLogo(key) {
    return window.CW?.ProviderMeta?.logoHtml?.(key, "cw-auth-provider-logo") || `<span class="token-text">${key || ""}</span>`;
  }

  function metadataProviderLogo(info) {
    return window.CW?.ProviderMeta?.logoHtml?.(info?.logoKey || info?.label || "", "cw-auth-provider-logo") || `<span class="token-text">${info?.label || ""}</span>`;
  }

  function connectedText(count) {
    return `${count} connected`;
  }

  function hasConfiguredValue(v) {
    return typeof v === "string" ? v.trim().length > 0 : !!v;
  }

  function escHtml(v) {
    return String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
  }

  function providerConfigBlock(cfg, provider) {
    const p = String(provider || "").trim().toLowerCase();
    const direct = cfg?.[p];
    if (p === "tmdb") return (cfg?.tmdb_sync && typeof cfg.tmdb_sync === "object") ? cfg.tmdb_sync : (direct && typeof direct === "object" ? direct : {});
    return direct && typeof direct === "object" ? direct : {};
  }

  function profileConfigured(provider, blk, cfg) {
    const p = String(provider || "").toLowerCase();
    const b = blk && typeof blk === "object" ? blk : {};
    if (p === "plex") return hasConfiguredValue(b.account_token) || hasConfiguredValue(b.token) || hasConfiguredValue(b.access_token);
    if (p === "emby" || p === "jellyfin") return hasConfiguredValue(b.access_token) || hasConfiguredValue(b.api_key) || hasConfiguredValue(b.token);
    if (p === "trakt" || p === "simkl") return hasConfiguredValue(b.access_token) || hasConfiguredValue(b.refresh_token);
    if (p === "anilist") return hasConfiguredValue(b.access_token) || hasConfiguredValue(b.token);
    if (p === "mdblist") return hasConfiguredValue(b.api_key) || hasConfiguredValue(b.access_token);
    if (p === "tautulli") return hasConfiguredValue((b || cfg?.tautulli || cfg?.auth?.tautulli || {}).server_url || (b || cfg?.tautulli || cfg?.auth?.tautulli || {}).server);
    if (p === "tmdb") return hasConfiguredValue(b.account_id) || (hasConfiguredValue(b.api_key) && hasConfiguredValue(b.session_id || b.session));
    return hasConfiguredValue(b.access_token) || hasConfiguredValue(b.api_key) || hasConfiguredValue(b.token);
  }

  function profileDisplayName(id) {
    const raw = String(id || "").trim();
    if (!raw || raw.toLowerCase() === "default") return "Default";
    const generated = raw.match(/^[A-Z0-9]+-P0*([1-9]\d*)$/i);
    return generated ? `P${String(generated[1]).padStart(2, "0")}` : raw;
  }

  function configuredProfileIds(cfg, provider, connected = false) {
    const block = providerConfigBlock(cfg, provider);
    const out = [];
    if (profileConfigured(provider, block, cfg)) out.push("default");
    const insts = block?.instances;
    if (insts && typeof insts === "object") {
      Object.entries(insts).forEach(([id, instBlock]) => {
        if (String(id || "").trim() && profileConfigured(provider, instBlock, cfg)) out.push(String(id));
      });
    }
    if (!out.length && connected) out.push("default");
    return Array.from(new Set(out)).sort((a, b) => (a !== "default") - (b !== "default") || a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));
  }

  function authProfileBadges(card, cfg) {
    const ids = configuredProfileIds(cfg, card.provider, !!card.status?.ok)
      .filter((id) => String(id || "").trim().toLowerCase() !== "default");
    if (!ids.length) return "";
    const label = ids.length === 1 ? "Configured profile" : "Configured profiles";
    const pills = ids.map((id) => {
      const name = profileDisplayName(id);
      return `<span class="cw-auth-profile-pill" title="${escHtml(`${label}: ${name}`)}">${escHtml(name)}</span>`;
    }).join("");
    return `<span class="cw-auth-profile-strip" aria-label="${label}">${pills}</span>`;
  }

  function authProviderKeysWithSections() {
    const meta = providerMeta();
    const infos = typeof meta.authProviders === "function" ? meta.authProviders() : [];
    const fromMeta = infos.map((info) => String(info?.key || "").trim().toUpperCase()).filter(Boolean);
    const fallback = AUTH_GROUPS.flatMap((group) => group.keys);
    return Array.from(new Set(fromMeta.length ? fromMeta : fallback)).filter((key) => !!authProviderInfo(key).sectionId);
  }

  function metadataConfigured(key, cfg = getCachedConfig()) {
    const info = metadataProviderInfo(key);
    if (!info) return false;
    if (info.key === "TMDB_METADATA") return !!String(cfg?.tmdb?.api_key || cfg?.metadata?.tmdb_api_key || "").trim();
    if (info.key === "ANIME_MAPPING") return !!cfg?.anime_mapping?.enabled;
    return false;
  }

  function metadataStatusFor(key, cfg = getCachedConfig()) {
    const info = metadataProviderInfo(key);
    const on = metadataConfigured(key, cfg);
    if (info?.key === "ANIME_MAPPING") return on ? { text: "Enabled", ok: true } : { text: "Not enabled", ok: false };
    return on ? { text: "Connected", ok: true } : { text: "Not configured", ok: false };
  }

  function ensureAuthShell(slot) {
    let shell = slot.querySelector(":scope > .cw-auth-main");
    let library = slot.querySelector(":scope > .cw-auth-provider-library");
    if (!library) {
      library = document.createElement("div");
      library.className = "cw-auth-provider-library";
      library.setAttribute("aria-hidden", "true");
      Array.from(slot.children).forEach((child) => {
        if (child.classList?.contains("section") && String(child.id || "").startsWith("sec-auth-")) library.appendChild(child);
      });
      slot.appendChild(library);
    }
    if (!shell) {
      shell = document.createElement("div");
      shell.className = "cw-auth-main";
      slot.insertBefore(shell, library);
    }
    ensureAuthOverlay(slot);
    return { shell, library };
  }

  function ensureAuthOverlay(slot) {
    let overlay = slot.querySelector(":scope > .cw-auth-overlay");
    if (overlay) return overlay;
    overlay = document.createElement("div");
    overlay.id = "cw-auth-connection-overlay";
    overlay.className = "cw-auth-overlay hidden";
    overlay.setAttribute("aria-hidden", "true");
    overlay.innerHTML = `
      <div class="cw-auth-dialog" role="dialog" aria-modal="true" aria-labelledby="cw-auth-dialog-title">
        <div class="cw-auth-dialog-head">
          <span class="cw-auth-dialog-logo" aria-hidden="true"></span>
          <div class="cw-auth-dialog-copy">
            <div class="cw-auth-dialog-kicker" id="cw-auth-dialog-kicker">Connections</div>
            <h3 id="cw-auth-dialog-title">Add connection</h3>
            <div class="cw-auth-dialog-subtitle" id="cw-auth-dialog-subtitle"></div>
          </div>
          <button type="button" class="cw-auth-icon-btn material-symbols-rounded" data-cw-auth-close aria-label="Close">close</button>
        </div>
        <div class="cw-auth-provider-picker" id="cw-auth-provider-picker"></div>
        <div class="cw-auth-provider-form hidden" id="cw-auth-provider-form"></div>
      </div>`;
    slot.appendChild(overlay);
    return overlay;
  }

  function providerHome(section, key) {
    const groupId = authProviderInfo(key).groupId;
    const group = document.getElementById(groupId);
    return group?.querySelector(":scope > .body") || section?.parentElement || null;
  }

  function metadataHome() {
    return document.getElementById("meta-provider-panel") || document.getElementById("metadata-providers");
  }

  function parkActiveAuthForm() {
    const overlay = document.getElementById("cw-auth-connection-overlay");
    const form = document.getElementById("cw-auth-provider-form");
    const section = form?.querySelector(":scope > .section[id]");
    if (!section) return;
    const metaKey = Object.keys(META_ITEMS).find((candidate) => META_ITEMS[candidate].sectionId === section.id);
    const key = metaKey ? "" : authProviderKeysWithSections().find((candidate) => authProviderInfo(candidate).sectionId === section.id);
    const home = metaKey ? metadataHome() : providerHome(section, key);
    if (home) home.appendChild(section);
    form.classList.add("hidden");
    overlay?.classList.remove("cw-connection-overlay");
    if (overlay?.dataset) delete overlay.dataset.cwConnectionSize;
    overlay?.querySelector("#cw-auth-provider-picker")?.classList.remove("hidden");
  }

  function renderAuthPicker(overlay, cfg = getCachedConfig(), mode = "provider") {
    const picker = overlay?.querySelector("#cw-auth-provider-picker");
    if (!picker) return;
    if (mode === "metadata") {
      const items = META_GROUP.keys.map((key) => {
        const info = metadataProviderInfo(key);
        const status = metadataStatusFor(key, cfg);
        return `<button type="button" class="cw-auth-picker-card" data-cw-meta-pick="${info.key}">
          <span class="cw-auth-provider-mark">${metadataProviderLogo(info)}</span>
          <span class="cw-auth-provider-copy"><strong>${info.label}</strong><small>${status.text}</small></span>
          <span class="cw-auth-card-status ${status.ok ? "ok" : ""}"><span></span></span>
        </button>`;
      }).join("");
      picker.innerHTML = `<div class="cw-auth-picker-group"><h4>${META_GROUP.title}</h4><div class="cw-auth-picker-grid">${items}</div></div>`;
      return;
    }
    const configured = configuredProviderKeys(cfg);
    const rows = AUTH_GROUPS.map((group) => {
      const items = group.keys
        .filter((key) => authProviderKeysWithSections().includes(key))
        .map((key) => {
          const info = authProviderInfo(key);
          const status = authStatusFor(key, configured.has(key));
          return `<button type="button" class="cw-auth-picker-card" data-cw-auth-pick="${info.key}">
            <span class="cw-auth-provider-mark">${authProviderLogo(info.key)}</span>
            <span class="cw-auth-provider-copy"><strong>${info.label}</strong><small>${status.text}</small></span>
            <span class="cw-auth-card-status ${status.ok ? "ok" : ""}"><span></span></span>
          </button>`;
        }).join("");
      return items ? `<div class="cw-auth-picker-group"><h4>${group.title}</h4><div class="cw-auth-picker-grid">${items}</div></div>` : "";
    }).join("");
    picker.innerHTML = rows || '<div class="cw-auth-empty">No supported providers found.</div>';
  }

  function renderAuthCards(slot, cfg = getCachedConfig()) {
    const { shell } = ensureAuthShell(slot);
    const configured = configuredProviderKeys(cfg);
    const sectionCopy = {
      "sec-auth-media": "Manage your media server connections.",
      "sec-auth-trackers": "Configure and manage your tracker integrations.",
      "sec-auth-metadata": "Manage metadata providers and mappings.",
    };
    const summaryIcon = {
      "sec-auth-media": "dns",
      "sec-auth-trackers": "radar",
      "sec-auth-metadata": "database",
    };
    const groupData = AUTH_GROUPS.filter((group) => group.id !== "sec-auth-others").map((group) => {
      const supported = group.keys.filter((key) => !!authProviderInfo(key).sectionId);
      const visible = supported.filter((key) => configured.has(key) || statusProviderData(key)?.connected === true);
      const cards = visible.map((key) => {
        const info = authProviderInfo(key);
        const status = authStatusFor(key, configured.has(key) || statusProviderData(key)?.connected === true);
        return { type: "provider", key: info.key, provider: String(info.key || "").toLowerCase(), label: info.label, status, logo: authProviderLogo(info.key) };
      });
      const okCount = cards.filter((card) => card.status.ok).length;
      return { ...group, supported, cards, okCount, total: supported.length, copy: sectionCopy[group.id] || "" };
    });
    const metadataVisible = META_GROUP.keys.filter((key) => metadataConfigured(key, cfg));
    const metadataCards = metadataVisible.map((key) => {
      const info = metadataProviderInfo(key);
      const status = metadataStatusFor(key, cfg);
      return { type: "metadata", key: info.key, label: info.label, status, logo: metadataProviderLogo(info) };
    });
    const sections = [
      ...groupData,
      { ...META_GROUP, cards: metadataCards, okCount: metadataCards.filter((card) => card.status.ok).length, total: META_GROUP.keys.length, copy: sectionCopy[META_GROUP.id] },
    ];
    const summaryCards = sections.map((section) => `<div class="cw-auth-summary-card" data-cw-auth-summary="${section.id}">
      <span class="cw-auth-summary-icon material-symbols-rounded" aria-hidden="true">${summaryIcon[section.id] || "hub"}</span>
      <span><strong>${section.title}</strong><small>${section.okCount}/${section.total}</small></span>
    </div>`).join("");
    const renderCard = (card) => {
      const attr = card.type === "metadata" ? `data-cw-meta-open="${card.key}"` : `data-cw-auth-open="${card.key}"`;
      const profiles = card.type === "provider" ? authProfileBadges(card, cfg) : "";
      return `<button type="button" class="cw-auth-service-card" ${attr}>
        <span class="cw-auth-provider-mark">${card.logo}</span>
        <span class="cw-auth-service-copy">
          <strong>${card.label}</strong>
          <small><span class="cw-auth-status-dot ${card.status.ok ? "ok" : ""}"></span>${card.status.text}</small>
        </span>
        ${profiles}
        <span class="material-symbols-rounded cw-auth-chevron" aria-hidden="true">chevron_right</span>
      </button>`;
    };
    const renderAddCard = (section) => {
      const mode = section.id === META_GROUP.id ? "metadata" : "provider";
      const label = section.id === META_GROUP.id ? "Add metadata" : section.id === "sec-auth-media" ? "Add media server" : "Add tracker";
      const copy = section.id === META_GROUP.id ? "Connect another metadata source." : "Connect another service.";
      return `<button type="button" class="cw-auth-service-card cw-auth-add-card" data-cw-auth-empty-add="${mode}">
        <span class="cw-auth-add-mark"><span class="material-symbols-rounded" aria-hidden="true">add</span></span>
        <span class="cw-auth-service-copy">
          <strong>${label}</strong>
          <small>${copy}</small>
        </span>
        <span class="material-symbols-rounded cw-auth-chevron" aria-hidden="true">arrow_forward</span>
      </button>`;
    };
    const serviceSections = sections.map((section) => {
      const addCard = section.cards.length < section.total ? renderAddCard(section) : "";
      const cards = `${section.cards.map(renderCard).join("")}${addCard}`;
      return `<section class="cw-auth-service-section" data-cw-auth-group="${section.id}">
        <div class="cw-auth-service-head">
          <h4>${section.title}</h4>
          <p>${section.copy}</p>
          <span class="material-symbols-rounded cw-auth-section-chevron" aria-hidden="true">expand_more</span>
        </div>
        <div class="cw-auth-service-grid">${cards}</div>
      </section>`;
    }).join("");
    shell.innerHTML = `
      <div class="cw-auth-dashboard">
        <div class="cw-auth-summary-row">
          ${summaryCards}
        </div>
        <div class="cw-auth-service-list">${serviceSections}</div>
      </div>`;
    const overlay = document.getElementById("cw-auth-connection-overlay");
    renderAuthPicker(overlay, cfg, overlay?.dataset?.cwPickerMode || "provider");
  }

  function openAuthOverlay(mode = "picker", key = "", pickerMode = "provider") {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    const overlay = ensureAuthOverlay(slot);
    const picker = overlay.querySelector("#cw-auth-provider-picker");
    const form = overlay.querySelector("#cw-auth-provider-form");
    const title = overlay.querySelector("#cw-auth-dialog-title");
    const kicker = overlay.querySelector("#cw-auth-dialog-kicker");
    overlay.classList.remove("hidden");
    overlay.setAttribute("aria-hidden", "false");
    if (mode === "form" && key) {
      picker?.classList.add("hidden");
      form?.classList.remove("hidden");
      const info = authProviderInfo(key);
      if (title) title.textContent = info.label;
      if (kicker) kicker.textContent = "Connection settings";
      requestAnimationFrame(() => overlay.querySelector("[data-cw-auth-close]")?.focus?.());
      return;
    }
    parkActiveAuthForm();
    overlay.dataset.cwPickerMode = pickerMode;
    renderAuthPicker(overlay, getCachedConfig(), pickerMode);
    picker?.classList.remove("hidden");
    form?.classList.add("hidden");
    if (title) title.textContent = pickerMode === "metadata" ? "Add Metadata" : "Add Provider";
    if (kicker) kicker.textContent = pickerMode === "metadata" ? "Metadata" : "Providers";
    const subtitle = overlay.querySelector("#cw-auth-dialog-subtitle");
    const logo = overlay.querySelector(".cw-auth-dialog-logo");
    if (subtitle) subtitle.textContent = "";
    logo?.classList.add("hidden");
    requestAnimationFrame(() => overlay.querySelector(".cw-auth-picker-card, [data-cw-auth-close]")?.focus?.());
  }

  const CONNECTION_MODAL_INFO = Object.freeze({
    PLEX: {
      provider: "plex", logo: "PLEX", size: "wide", help: window.CW.HelpLinks.url("plex"), deleteSelector: "#btn-delete-plex",
      tabs: { auth: ["lock", "Authentication", "Connect your Plex account using a secure link code"], settings: ["tune", "Settings", "Configure server and sync preferences"], whitelist: ["verified_user", "Whitelisting", "Choose libraries and content to sync"] },
      copy: { auth: ["Plex Authentication", "Connect your Plex account to CrossWatch using a secure link code."], settings: ["Plex Settings", "Configure your server URL, selected profile and sync preferences."], whitelist: ["Plex Whitelisting", "Choose which libraries CrossWatch can use for history, ratings, progress and scrobbling."] },
      journey: ["Link Plex, then tune the server", "Click Connect Plex to get a link code. CrossWatch shows the code, then opens plex.tv/link so you can enter it and approve access. Next, validate the server URL and optionally whitelist libraries.", "229,160,13", "229,160,13", "PLEX"],
      steps: [["1", "Get link code", "Generate a code in CrossWatch"], ["2", "Approve on Plex", "Open plex.tv/link and approve the connection"], ["3", "Validate server", "Confirm your server URL and complete setup"]],
      order: ["#plex_qc_state", ".cw-subpanel[data-sub=auth]>.inline"]
    },
    JELLYFIN: {
      provider: "jellyfin", logo: "JELLYFIN", size: "wide", help: window.CW.HelpLinks.url("jellyfin"), deleteSelector: ".cw-jfy-delete",
      tabs: { auth: ["lock", "Authentication", "Connect with Quick Connect or password"], settings: ["tune", "Settings", "Configure server and user details"], whitelist: ["verified_user", "Whitelisting", "Choose libraries and content to sync"] },
      copy: { auth: ["Jellyfin Authentication", "Connect your Jellyfin server to CrossWatch."], settings: ["Jellyfin Settings", "Configure server URL, user details and sync preferences."], whitelist: ["Jellyfin Whitelisting", "Choose which libraries CrossWatch can use for sync."] },
      journey: ["Connect to Jellyfin", "Enter your Jellyfin server URL, then connect with Quick Connect (recommended) or your username and password. CrossWatch will move you to Settings next so you can validate the server, pick the user, and optionally whitelist libraries.", "0,164,220", "0,124,220", "JELLYFIN"],
      steps: [["1", "Enter server", "Add your Jellyfin server URL"], ["2", "Authorize", "Use Quick Connect or password"], ["3", "Validate", "Confirm the account and server"]],
      order: [".grid2", ".jfy-methods", ".jfy-pane[data-method=quick]", ".jfy-pane[data-method=password]", ".jfy-actions-row"]
    },
    EMBY: {
      provider: "emby", logo: "EMBY", size: "wide", help: window.CW.HelpLinks.url("emby"), deleteSelector: "#btn-emby-delete",
      tabs: { auth: ["lock", "Authentication", "Connect with your Emby credentials"], settings: ["tune", "Settings", "Configure server and user details"], whitelist: ["verified_user", "Whitelisting", "Choose libraries and content to sync"] },
      copy: { auth: ["Emby Authentication", "Connect your Emby server to CrossWatch."], settings: ["Emby Settings", "Configure server URL, user details and sync preferences."], whitelist: ["Emby Whitelisting", "Choose which libraries CrossWatch can use for sync."] },
      journey: ["Connect to Emby", "Enter your Emby server URL, username and password, then sign in. CrossWatch will move you to Settings next so you can validate the server, pick the user, and optionally whitelist libraries.", "82,181,75", "82,181,75", "EMBY"],
      steps: [["1", "Enter server", "Add your Emby server URL"], ["2", "Sign in", "Authorize with user credentials"], ["3", "Validate", "Confirm the account and server"]],
      order: [".grid2", ".inline"]
    },
    TRAKT: {
      provider: "trakt", logo: "TRAKT", help: window.CW.HelpLinks.url("trakt"), deleteSelector: "#btn-delete-trakt",
      tabs: { auth: ["lock", "Authentication", "Connect your Trakt account"] },
      copy: { auth: ["Trakt Authentication", "Connect your Trakt account with your API credentials."] },
      journey: ["Connect to Trakt", "Add your Trakt Client ID and Secret, then click Connect Trakt and approve the link code at trakt.tv/activate. Once approved, CrossWatch can sync your Trakt watchlist, history and ratings.", "225,20,60", "159,66,198", "TRAKT"],
      steps: [["1", "Add API app", "Enter your Trakt Client ID and Secret"], ["2", "Approve code", "Open trakt.tv/activate and enter the code"], ["3", "Sync account", "CrossWatch stores the approved token"]],
      order: [".grid2", "#trakt_hint", ".sep", ".inline", "#trakt_qc_state"],
      code: ["#trakt_qc_state"],
      actions: [{ row: ".inline", status: "#trakt_msg", buttons: "#btn-connect-trakt, #btn-trakt-cancel, #btn-trakt-restart" }]
    },
    SIMKL: {
      provider: "simkl", logo: "SIMKL", help: window.CW.HelpLinks.url("simkl"), deleteSelector: "#btn-delete-simkl, #btn-delete-simkl-oauth",
      tabs: { auth: ["lock", "Authentication", "Connect with PIN or OAuth"] },
      copy: { auth: ["SIMKL Authentication", "Connect SIMKL with a PIN code or your OAuth app credentials."] },
      journey: ["Connect to SIMKL", "Connect with a PIN code (recommended) - CrossWatch shows a short code you enter at simkl.com/pin, no keys needed. OAuth with your own SIMKL app credentials remains available.", "218,225,232", "12,12,12", "SIMKL"],
      steps: [["1", "Choose method", "Use PIN flow or your OAuth app"], ["2", "Approve SIMKL", "Enter the PIN code or authorize OAuth"], ["3", "Start syncing", "CrossWatch stores the approved access"]],
      order: [".smk-method-row", "#simkl_oauth_panel", "#simkl_pin_panel", ".cw-connection-method-action-row", ".inline"],
      code: ["#simkl_pin_panel"],
      actions: [{ row: ".smk-method-row", status: "#simkl_msg", buttons: "#btn-connect-simkl-pin, #btn-simkl-pin-cancel, #btn-simkl-pin-restart, #btn-connect-simkl", extract: ".smk-actions", order: "6" }]
    },
    TMDB: {
      provider: "tmdb", logo: "TMDB", help: window.CW.HelpLinks.url("tmdb"), deleteSelector: "#tmdb_sync_disconnect",
      tabs: { auth: ["lock", "Authentication", "Connect TMDb sync"] },
      copy: { auth: ["TMDb Authentication", "Connect TMDb sync with a v3 API key."] },
      journey: ["Connect to TMDb", "Add your TMDb v3 API key, then connect and approve the session in TMDb. CrossWatch stores the session ID for TMDb watchlist and ratings sync.", "0,179,229", "144,206,161", "TMDB"],
      steps: [["1", "Add API key", "Enter your TMDb v3 key"], ["2", "Approve session", "Authorize CrossWatch on TMDb"], ["3", "Validate sync", "Confirm the session is ready"]],
      order: [".grid2"],
      actions: [{ row: ".inline", status: "#tmdb_sync_msg", buttons: "#tmdb_sync_connect", wrapParent: true, parentClass: "cw-tmdb-sync-action-wrap", order: "4" }]
    },
    MDBLIST: {
      provider: "mdblist", logo: "MDBLIST", help: window.CW.HelpLinks.url("mdblist"), deleteSelector: "#mdblist_disconnect_device, #mdblist_disconnect_api",
      tabs: { auth: ["lock", "Authentication", "Connect with device code or API key"] },
      copy: { auth: ["MDBList Authentication", "Connect MDBList with device code or API key."] },
      journey: ["Connect to MDBList", "Connect with a Device Code (recommended) or paste a legacy API key. Device Code opens a browser window so you can approve CrossWatch without sharing a key.", "64,132,200", "64,132,200", "MDBLIST"],
      steps: [["1", "Choose method", "Use Device Code or API key"], ["2", "Approve access", "Authorize the device code or save the key"], ["3", "Validate account", "CrossWatch confirms MDBList access"]],
      order: [".mdbl-method-row", "#mdblist_api_panel", "#mdblist_device_panel", ".cw-connection-method-action-row", ".inline"],
      code: ["#mdblist_device_panel"],
      actions: [{ row: ".mdbl-method-row", status: "#mdblist_msg", buttons: "#mdblist_device_start, #mdblist_device_cancel, #mdblist_device_restart, #mdblist_save", extract: ".mdbl-actions", order: "6" }]
    },
    PUBLICMETADB: {
      provider: "publicmetadb", logo: "PUBLICMETADB", help: window.CW.HelpLinks.url("publicmetadb"), deleteSelector: "#publicmetadb_disconnect",
      tabs: { auth: ["lock", "Authentication", "Connect your PublicMetaDB API key"] },
      copy: { auth: ["PublicMetaDB Authentication", "Connect PublicMetaDB with an API key."] },
      journey: ["Connect to PublicMetaDB", "Paste your PublicMetaDB API key and click Connect PublicMetaDB. CrossWatch uses the key for metadata-backed matching and provider lookups.", "155,155,155", "155,155,155", "PUBLICMETADB"],
      steps: [["1", "Create API key", "Generate a key in PublicMetaDB"], ["2", "Connect key", "Paste the key and connect"], ["3", "Validate access", "CrossWatch confirms the key works"]],
      order: [".grid2", ".publicmetadb-actions"],
      actions: [{ row: ".publicmetadb-actions", status: "#publicmetadb_msg", buttons: "#publicmetadb_save" }]
    },
    ANILIST: {
      provider: "anilist", logo: "ANILIST", help: window.CW.HelpLinks.url("anilist"), deleteSelector: "#btn-delete-anilist",
      tabs: { auth: ["lock", "Authentication", "Connect your AniList account"] },
      copy: { auth: ["AniList Authentication", "Connect AniList with your API credentials."] },
      journey: ["Connect to AniList", "Add your AniList Client ID and Secret, then click Connect AniList and approve the request in the browser window. Anime ID Mapping can improve AniList matching.", "2,169,255", "2,169,255", "ANILIST"],
      steps: [["1", "Add API app", "Enter your AniList Client ID and Secret"], ["2", "Approve AniList", "Authorize CrossWatch in the browser"], ["3", "Sync anime", "CrossWatch stores the approved token"]],
      order: [".grid2", "#anilist_hint", ".anilist-mapping-rec", ".inline"],
      actions: [{ row: ".inline", status: "#anilist_msg", buttons: "#btn-connect-anilist" }]
    },
    TAUTULLI: {
      provider: "tautulli", logo: "TAUTULLI", help: window.CW.HelpLinks.url("tautulli"), deleteSelector: "#tautulli_disconnect",
      tabs: { auth: ["lock", "Authentication", "Connect your Tautulli server"] },
      copy: { auth: ["Tautulli Authentication", "Connect Tautulli with server URL and API key."] },
      journey: ["Connect to Tautulli", "Enter your Tautulli server URL and API key, optionally choose a user ID, then connect. CrossWatch can use Tautulli history for supported sync flows.", "190,190,190", "84,115,145", "TAUTULLI"],
      steps: [["1", "Enter server", "Add your Tautulli URL and API key"], ["2", "Choose user", "Optionally limit to one user ID"], ["3", "Validate server", "CrossWatch confirms Tautulli access"]],
      order: [".grid2", "#tautulli_hint", "#tautulli_actions_row"],
      actions: [{ row: "#tautulli_actions_row", status: "#tautulli_msg", buttons: "#tautulli_save" }]
    },
    TMDB_METADATA: {
      provider: "tmdb", logo: "TMDB", help: window.CW.HelpLinks.url("tmdb-metadata"), deleteSelector: "#tmdb_delete",
      tabs: { api: ["key", "API key", "Connect TMDb metadata"], advanced: ["tune", "Advanced", "Configure locale and cache behavior"] },
      copy: { api: ["TMDb Metadata", "Metadata and images fetched from TMDb."], advanced: ["TMDb Metadata Settings", "Configure metadata locale, cache and advanced options."] },
      journey: ["Connect TMDb Metadata", "Add a TMDb API key to enable metadata lookups, matching data and images. Advanced options let you tune locale and cache behavior when available.", "0,179,229", "144,206,161", "TMDB"],
      steps: [["1", "Add API key", "Enter your TMDb API key"], ["2", "Connect TMDb", "Validate the key with CrossWatch"], ["3", "Use metadata", "Metadata lookups become available"]],
      introSubs: ["api", "advanced"],
      order: [".auth-card"],
      labels: { "#tmdb_check": "Connect TMDb" },
      actions: [{ row: ".inline", status: "#tmdb_check_msg", buttons: "#tmdb_check", connectedSelector: "#tmdb_api_key", connectedText: "Connected" }]
    },
    ANIME_MAPPING: {
      provider: "anime-mapping", logo: "ANILIST", help: window.CW.HelpLinks.url("anime-mapping"), deleteSelector: "",
      tabs: { overview: ["hub", "Mapping", "Manage the local anime ID index"] },
      copy: { overview: ["Anime ID Mapping", "Local anime ID index for AniList watchlist and ratings pairs."] },
      journey: ["Enable Anime ID Mapping", "Use the local AniBridge mapping dataset to translate anime identifiers between AniList and TMDb, TVDb, IMDb, MyAnimeList and AniDB.", "2,169,255", "78,141,255", "ANILIST"],
      steps: [["1", "Enable mapping", "Turn on the local anime index"], ["2", "Update dataset", "Download or refresh AniBridge mappings"], ["3", "Improve matches", "Use mappings for AniList pairs"]],
      introSubs: ["overview"],
      order: [".anime-mapping-summary", ".anime-mapping-status-grid", ".anime-mapping-source", "#anime_mapping_error", ".anime-mapping-actions"]
    },
  });

  function connectionInfoForKey(key) {
    const norm = String(key || "").trim().toUpperCase();
    const info = CONNECTION_MODAL_INFO[norm];
    return info ? { ...info, key: norm } : null;
  }

  function connectionPanelFor(section, info) {
    if (!section || !info) return null;
    return section.querySelector(`.cw-meta-provider-panel[data-provider="${info.provider}"]`) || (section.matches?.(".cw-meta-provider-panel") ? section.querySelector(":scope > .body") || section : null) || section.querySelector(":scope > .body") || section;
  }

  function activeModalSub(panel, info) {
    return String(panel?.querySelector(".cw-subtile.active[data-sub]")?.dataset?.sub || Object.keys(info?.tabs || {})[0] || "auth").toLowerCase();
  }

  function connectionIntroPanels(panel, info) {
    const subs = Array.isArray(info?.introSubs) && info.introSubs.length ? info.introSubs.map((s) => String(s).toLowerCase()) : ["auth"];
    const found = subs.flatMap((sub) => Array.from(panel.querySelectorAll(`.cw-subpanel[data-sub="${sub}"]`)));
    if (found.length) return found;
    const active = panel.querySelector(".cw-subpanel.active");
    return active ? [active] : Array.from(panel.querySelectorAll(".cw-subpanel")).slice(0, 1);
  }

  function syncConnectionModalCopy(panel, info, overlay = null) {
    if (!panel) return;
    const active = activeModalSub(panel, info);
    const data = info?.copy?.[active] || Object.values(info?.copy || {})[0] || ["Connection", "Configure this provider."];
    const root = overlay || document.getElementById("cw-auth-connection-overlay");
    const dialogTitle = root?.querySelector("#cw-auth-dialog-title");
    const dialogCopy = root?.querySelector("#cw-auth-dialog-subtitle");
    const dialogKicker = root?.querySelector("#cw-auth-dialog-kicker");
    if (dialogTitle) dialogTitle.textContent = data[0];
    if (dialogCopy) dialogCopy.textContent = data[1];
    if (dialogKicker) dialogKicker.textContent = info?.key === "TMDB_METADATA" || info?.key === "ANIME_MAPPING" ? "Metadata" : "Connection settings";
  }

  function connectionModalSupportsProfiles(info) {
    return !!info && !["TMDB_METADATA", "ANIME_MAPPING"].includes(info.key);
  }

  function connectionModalConfigured(info, cfg = getCachedConfig()) {
    if (!info) return false;
    if (info.key === "TMDB_METADATA" || info.key === "ANIME_MAPPING") return metadataConfigured(info.key, cfg);
    return configuredProviderKeys(cfg).has(info.key) || statusProviderData(info.key)?.connected === true;
  }

  function connectionModalStatusTarget(panel) {
    if (!panel) return null;
    const nodes = Array.from(panel.querySelectorAll("#plex_msg, #jfy_msg, #emby_msg, .cw-connection-status-pill, .cw-connection-primary-action"));
    return nodes.find((node) => {
      const text = String(node?.textContent || "").trim().toLowerCase();
      return /\bconnected\b/.test(text) && !/\bnot\s+connected\b/.test(text) && !node.classList?.contains("hidden") && node.getAttribute?.("aria-hidden") !== "true";
    }) || null;
  }

  function connectionStatusText(node) {
    return String(node?.textContent || "").trim();
  }

  function connectionStatusConnected(node) {
    const text = connectionStatusText(node).toLowerCase();
    return /\bconnected\b/.test(text) && !/\bnot\s+connected\b/.test(text);
  }

  function connectionStatusWarning(node) {
    const text = connectionStatusText(node).toLowerCase();
    return node?.classList?.contains("warn") || /\b(deleted|invalid|failed|failure|error|missing|denied|expired|unauthorized|forbidden|disconnect|remove)\b/.test(text) || /\bcould\s+not\b/.test(text) || /\bnot\s+connected\b/.test(text);
  }

  function clearConnectionStatusDismissal(node) {
    if (!node) return;
    if (node.__cwConnectionStatusTimer) clearTimeout(node.__cwConnectionStatusTimer);
    if (node.__cwConnectionStatusFadeTimer) clearTimeout(node.__cwConnectionStatusFadeTimer);
    node.__cwConnectionStatusTimer = 0;
    node.__cwConnectionStatusFadeTimer = 0;
    node.__cwConnectionStatusText = "";
    node.classList.remove("cw-connection-status-transient", "cw-connection-status-dismissing");
  }

  function scheduleConnectionStatusDismissal(node) {
    const text = connectionStatusText(node);
    if (!node || !text) return;
    if (node.__cwConnectionStatusText === text && node.__cwConnectionStatusTimer) return;
    clearConnectionStatusDismissal(node);
    node.__cwConnectionStatusText = text;
    node.classList.add("cw-connection-status-transient");
    node.__cwConnectionStatusTimer = setTimeout(() => {
      node.__cwConnectionStatusTimer = 0;
      if (connectionStatusText(node) !== text || connectionStatusConnected(node)) return;
      node.classList.add("cw-connection-status-dismissing");
      node.__cwConnectionStatusFadeTimer = setTimeout(() => {
        node.__cwConnectionStatusFadeTimer = 0;
        if (connectionStatusText(node) !== text || connectionStatusConnected(node)) return;
        node.classList.add("hidden");
        node.textContent = "";
        node.classList.remove("ok", "warn", "cw-connection-status-transient", "cw-connection-status-dismissing");
        node.__cwConnectionStatusText = "";
      }, 1200);
    }, 10000);
  }

  function syncConnectionStatusDismissals(panel) {
    if (!panel) return;
    panel.querySelectorAll("#plex_msg, #jfy_msg, #emby_msg, .cw-connection-status-pill").forEach((node) => {
      const text = connectionStatusText(node);
      const visible = !!text && !node.classList.contains("hidden") && node.getAttribute("aria-hidden") !== "true";
      if (!visible) {
        clearConnectionStatusDismissal(node);
        return;
      }
      if (connectionStatusConnected(node)) {
        clearConnectionStatusDismissal(node);
        node.classList.add("ok");
        node.classList.remove("warn");
        return;
      }
      if (connectionStatusWarning(node)) {
        node.classList.add("warn");
        node.classList.remove("ok");
      }
      scheduleConnectionStatusDismissal(node);
    });
  }

  function connectionModalConnected(panel, info, cfg = getCachedConfig()) {
    return connectionModalConfigured(info, cfg) || !!connectionModalStatusTarget(panel);
  }

  function ensureConnectionSuccessBurst(panel) {
    let burst = panel?.querySelector(":scope > .cw-connection-success-burst");
    if (!burst && panel) {
      burst = document.createElement("div");
      burst.className = "cw-connection-success-burst";
      burst.setAttribute("aria-hidden", "true");
      panel.appendChild(burst);
    }
    return burst;
  }

  function connectionWindowReadyForCelebration() {
    return document.visibilityState !== "hidden" && document.hidden !== true && (typeof document.hasFocus !== "function" || document.hasFocus());
  }

  function queueConnectionSuccess(panel, info) {
    if (!panel || !info) return;
    panel.__cwConnectionSuccessPending = {
      key: info.key,
      queuedAt: Date.now(),
    };
  }

  function flushPendingConnectionSuccess() {
    if (!connectionWindowReadyForCelebration()) return;
    const overlay = document.getElementById("cw-auth-connection-overlay");
    if (!overlay || overlay.classList.contains("hidden")) return;
    document.querySelectorAll("#cw-auth-provider-form .cw-connection-modal-panel").forEach((panel) => {
      const pending = panel.__cwConnectionSuccessPending;
      if (!pending) return;
      const info = connectionInfoForKey(pending.key || panel.dataset?.cwConnectionKey);
      if (!info) return;
      panel.__cwConnectionSuccessPending = null;
      requestAnimationFrame(() => playConnectionSuccess(panel, info, { defer: false }));
    });
  }

  function playConnectionSuccess(panel, info, opts = {}) {
    if (!panel || panel.__cwConnectionSuccessTimer) return;
    if (opts.defer !== false && !connectionWindowReadyForCelebration()) {
      queueConnectionSuccess(panel, info);
      return;
    }
    const reduceMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;
    const target = connectionModalStatusTarget(panel);
    panel.classList.remove("cw-connection-success-pulse");
    void panel.offsetWidth;
    panel.classList.add("cw-connection-success-pulse");
    if (reduceMotion) {
      panel.__cwConnectionSuccessTimer = setTimeout(() => {
        panel.classList.remove("cw-connection-success-pulse");
        panel.__cwConnectionSuccessTimer = 0;
      }, 900);
      return;
    }
    const burst = ensureConnectionSuccessBurst(panel);
    if (!burst) return;
    const panelRect = panel.getBoundingClientRect?.();
    const targetRect = target?.getBoundingClientRect?.();
    const originX = panelRect?.width && targetRect ? ((targetRect.left + (targetRect.width / 2) - panelRect.left) / panelRect.width) * 100 : 78;
    const originY = panelRect?.height && targetRect ? ((targetRect.top + (targetRect.height / 2) - panelRect.top) / panelRect.height) * 100 : 62;
    const colors = ["#00e084", "#72f0b0", "#f3cc64", "#ffffff", `rgb(${info?.journey?.[2] || "93,141,255"})`];
    burst.replaceChildren();
    for (let i = 0; i < 26; i += 1) {
      const bit = document.createElement("span");
      const angle = (-150 + Math.random() * 300) * Math.PI / 180;
      const distance = 58 + Math.random() * 142;
      const width = 5 + Math.random() * 7;
      const height = 7 + Math.random() * 12;
      bit.style.setProperty("--x", `${originX + ((Math.random() - 0.5) * 7)}%`);
      bit.style.setProperty("--y", `${originY + ((Math.random() - 0.5) * 7)}%`);
      bit.style.setProperty("--tx", `${Math.cos(angle) * distance}px`);
      bit.style.setProperty("--ty", `${Math.sin(angle) * distance - 24}px`);
      bit.style.setProperty("--w", `${width}px`);
      bit.style.setProperty("--h", `${height}px`);
      bit.style.setProperty("--r", `${Math.floor(Math.random() * 180)}deg`);
      bit.style.setProperty("--spin", `${180 + Math.floor(Math.random() * 360)}deg`);
      bit.style.setProperty("--d", `${Math.random() * 120}ms`);
      bit.style.setProperty("--c", colors[i % colors.length]);
      burst.appendChild(bit);
    }
    burst.classList.remove("is-active");
    void burst.offsetWidth;
    burst.classList.add("is-active");
    panel.__cwConnectionSuccessTimer = setTimeout(() => {
      burst.classList.remove("is-active");
      burst.replaceChildren();
      panel.classList.remove("cw-connection-success-pulse");
      panel.__cwConnectionSuccessTimer = 0;
    }, 1900);
  }

  function showConnectionDisconnected(panel) {
    if (!panel) return;
    const node = panel.querySelector(".cw-connection-status-pill, #plex_msg, #jfy_msg, #emby_msg");
    if (!node) return;
    node.textContent = "Disconnected";
    node.classList.remove("hidden", "ok", "cw-connection-status-transient", "cw-connection-status-dismissing");
    node.classList.add("warn");
    node.removeAttribute("aria-hidden");
    scheduleConnectionStatusDismissal(node);
  }

  function connectionModalCurrentConnected(panel, info, cfg = getCachedConfig()) {
    if (!info || info.key === "TMDB_METADATA" || info.key === "ANIME_MAPPING") return true;
    if (connectionModalStatusTarget(panel)) return true;
    const inst = String(panel?.querySelector(".cw-profile-switcher select")?.value || "default");
    return configuredProfileIds(cfg, info.provider).some((id) => String(id) === inst);
  }

  function updateConnectionSaveEnabled(panel, info, cfg = getCachedConfig()) {
    const save = panel?.querySelector(".cw-connection-footer-save");
    if (!save) return;
    const connected = connectionModalCurrentConnected(panel, info, cfg);
    save.disabled = !connected;
    save.classList.toggle("is-disabled", !connected);
    if (connected) save.removeAttribute("title");
    else save.title = "Connect this profile before saving.";
  }

  function syncConnectionSuccessState(panel, info, cfg = getCachedConfig()) {
    if (!panel || !info) return;
    const connected = connectionModalConnected(panel, info, cfg);
    if (connected && panel.__cwConnectionWasConnected === false) playConnectionSuccess(panel, info);
    panel.__cwConnectionWasConnected = connected;
    updateConnectionSaveEnabled(panel, info, cfg);
  }

  function connectionProfileFullName(option) {
    return String(option?.dataset?.cwProfileFullName || option?.textContent || option?.value || "").trim();
  }

  function connectionProfileChipName(option) {
    return profileDisplayName(connectionProfileFullName(option) || option?.value);
  }

  function refreshConnectionProfileCreateState(select, newBtn) {
    if (!select || !newBtn) return;
    const count = Array.from(select.options || []).filter((option) => option.value).length;
    const capped = count >= 10;
    newBtn.disabled = capped;
    newBtn.setAttribute("aria-disabled", capped ? "true" : "false");
    if (capped) newBtn.title = "Maximum 10 profiles reached.";
    else newBtn.removeAttribute("title");
  }

  function syncConnectionProfileChips(card) {
    const select = card?.querySelector(".cw-profile-switcher select");
    const chips = card?.querySelector(".cw-connection-profile-chips");
    if (!select || !chips) return;
    const current = String(select.value || "default");
    const opts = Array.from(select.options || []).filter((option) => option.value && option.value !== current);
    chips.replaceChildren(...opts.map((option) => {
      const fullName = connectionProfileFullName(option);
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "cw-connection-profile-chip";
      btn.textContent = connectionProfileChipName(option);
      btn.title = `Switch to ${fullName || option.value}`;
      btn.addEventListener("click", () => {
        select.value = option.value;
        select.dispatchEvent(new Event("change", { bubbles: true }));
      });
      return btn;
    }));
    chips.classList.toggle("hidden", !opts.length);
  }

  function decorateConnectionProfileToolbar(toolbar, card, info) {
    if (!toolbar || !card) return;
    toolbar.classList.add("cw-connection-profile-toolbar");
    toolbar.style.removeProperty("margin-left");
    toolbar.querySelector("[data-cw-profile-label]")?.classList.add("cw-connection-profile-hidden-label");
    const select = toolbar.querySelector("select");
    const newBtn = toolbar.querySelector(".cw-profile-new");
    const delBtn = toolbar.querySelector(".cw-profile-delete");
    if (select) {
      select.setAttribute("aria-label", `${info?.copy?.auth?.[0] || info?.key || "Connection"} profile`);
      Array.from(select.options || []).forEach((option) => {
        const fullName = connectionProfileFullName(option);
        option.dataset.cwProfileFullName = fullName;
        option.title = fullName;
      });
      if (!select.__cwConnectionProfileChipsBound) {
        select.__cwConnectionProfileChipsBound = true;
        select.addEventListener("change", () => {
          syncConnectionProfileChips(card);
        });
        if (typeof MutationObserver === "function") {
          const mo = new MutationObserver(() => {
            Array.from(select.options || []).forEach((option) => {
              const fullName = connectionProfileFullName(option);
              option.dataset.cwProfileFullName = fullName;
              option.title = fullName;
            });
            refreshConnectionProfileCreateState(select, newBtn);
            syncConnectionProfileChips(card);
          });
          mo.observe(select, { childList: true, subtree: true, characterData: true });
          select.__cwConnectionProfileChipsObserver = mo;
        }
      }
    }
    if (newBtn && !newBtn.__cwConnectionProfileDecorated) {
      newBtn.__cwConnectionProfileDecorated = true;
      newBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">add</span><span>New profile</span>`;
    }
    refreshConnectionProfileCreateState(select, newBtn);
    if (delBtn && !delBtn.__cwConnectionProfileDecorated) {
      delBtn.__cwConnectionProfileDecorated = true;
      delBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">delete</span><span>Delete profile</span>`;
    }
    syncConnectionProfileChips(card);
  }

  function findConnectionProfileToolbar(panel, info) {
    const scoped = `.cw-profile-switcher[data-cw-profile-provider="${info?.provider}"]`;
    return panel?.querySelector(scoped) || panel?.querySelector(".cw-profile-switcher") || document.querySelector(scoped) || null;
  }

  function ensureConnectionProfileBlock(panel, nav, info) {
    if (!panel || !nav || !connectionModalSupportsProfiles(info)) return null;
    let card = nav.querySelector(":scope > .cw-connection-profile-card");
    if (!card) {
      card = document.createElement("div");
      card.className = "cw-connection-profile-card";
      card.innerHTML = `
        <div class="cw-connection-profile-head">
          <span class="material-symbols-rounded" aria-hidden="true">group</span>
          <strong>Connection profiles</strong>
          <a class="material-symbols-rounded cw-connection-profile-help" href="${window.CW.HelpLinks.url("connection-profiles")}" target="_blank" rel="noopener noreferrer" aria-label="Open connection profiles guide" title="Open connection profiles guide">help</a>
        </div>
        <div class="cw-connection-profile-body">
          <div class="cw-connection-profile-slot"></div>
          <div class="cw-connection-profile-chips hidden"></div>
        </div>`;
      nav.appendChild(card);
    }

    const slot = card.querySelector(".cw-connection-profile-slot");
    const toolbar = findConnectionProfileToolbar(panel, info);
    if (toolbar && slot) {
      if (toolbar.parentElement !== slot) slot.appendChild(toolbar);
      decorateConnectionProfileToolbar(toolbar, card, info);
    }
    card.classList.toggle("hidden", !toolbar);

    if (!toolbar && !panel.__cwConnectionProfileObserveBound && typeof MutationObserver === "function") {
      panel.__cwConnectionProfileObserveBound = true;
      const mo = new MutationObserver(() => {
        if (!findConnectionProfileToolbar(panel, info)) return;
        try { mo.disconnect(); } catch {}
        panel.__cwConnectionProfileObserveBound = false;
        ensureConnectionModalNav(panel, info);
        scheduleConnectionModalSize(panel, info);
      });
      mo.observe(panel, { childList: true, subtree: true });
      panel.__cwConnectionProfileObserver = mo;
    }
    return card;
  }

  function ensureConnectionModalNav(panel, info) {
    if (!panel) return;
    let nav = panel.querySelector(":scope > .cw-subtiles");
    if (!nav) {
      nav = document.createElement("div");
      nav.className = "cw-subtiles";
      nav.innerHTML = Object.entries(info.tabs || {}).map(([sub, data], idx) => `<button type="button" class="cw-subtile${idx === 0 ? " active" : ""}" data-sub="${sub}">${data[1]}</button>`).join("");
      const head = panel.querySelector(":scope > .cw-panel-head");
      if (head) panel.insertBefore(nav, head.nextSibling || null);
      else panel.insertBefore(nav, panel.firstElementChild || null);
    }
    if (!nav.__cwConnectionModalNav) {
      nav.__cwConnectionModalNav = true;
      nav.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
        const data = info.tabs?.[String(btn.dataset.sub || "").toLowerCase()];
        if (!data) return;
        btn.innerHTML = `<span class="material-symbols-rounded cw-connection-nav-icon" aria-hidden="true">${data[0]}</span><span class="cw-connection-nav-copy"><strong>${data[1]}</strong><small>${data[2] || ""}</small></span><span class="material-symbols-rounded cw-connection-nav-chev" aria-hidden="true">chevron_right</span>`;
        btn.addEventListener("click", () => {
          const sub = String(btn.dataset.sub || "").toLowerCase();
          if (info.key === "TMDB_METADATA") {
            try { window.cwMetaProviderSubSelect?.("tmdb", sub); } catch {}
          }
          nav.querySelectorAll(".cw-subtile[data-sub]").forEach((node) => node.classList.toggle("active", node === btn));
          panel.querySelectorAll(".cw-subpanel[data-sub]").forEach((node) => node.classList.toggle("active", String(node.dataset.sub || "").toLowerCase() === sub));
          setTimeout(() => {
            applyConnectionModalOrder(panel, info);
            syncConnectionModalCopy(panel, info);
            scheduleConnectionModalSize(panel, info);
          }, 0);
        });
      });
    }
    const profileCard = ensureConnectionProfileBlock(panel, nav, info);
    const cardVisible = !!profileCard && !profileCard.classList.contains("hidden");
    const tabCount = nav.querySelectorAll(".cw-subtile[data-sub]").length || 1;
    nav.style.gridTemplateRows = cardVisible ? `repeat(${tabCount}, 92px) auto` : `repeat(${tabCount}, 92px)`;
    if (cardVisible) profileCard.style.gridRow = String(tabCount + 1);
    return nav;
  }

  function syncConnectionDialogHead(overlay, info) {
    if (!overlay || !info) return;
    const logo = overlay.querySelector(".cw-auth-dialog-logo");
    if (logo) {
      logo.classList.remove("hidden");
      setConnectionStyle(logo, "--cw-connection-logo", `url("/assets/img/${info.logo}.svg")`);
    }
  }

  function selectConnectionModalSub(panel, info, sub, overlay = null) {
    if (!panel || !sub) return;
    const wanted = String(sub).toLowerCase();
    panel.querySelectorAll(".cw-subtile[data-sub]").forEach((node) => node.classList.toggle("active", String(node.dataset.sub || "").toLowerCase() === wanted));
    panel.querySelectorAll(".cw-subpanel[data-sub]").forEach((node) => node.classList.toggle("active", String(node.dataset.sub || "").toLowerCase() === wanted));
    applyConnectionModalOrder(panel, info);
    syncConnectionModalCopy(panel, info, overlay);
    resetConnectionModalScroll(panel);
  }

  function resetConnectionModalScroll(panel) {
    [panel?.querySelector(":scope > .cw-subpanels"), panel?.querySelector(":scope > .auth-card")].forEach((node) => {
      if (node) node.scrollTop = 0;
    });
  }

  function connectionModalMaxHeight(info) {
    const viewport = Math.max(360, window.innerHeight || document.documentElement?.clientHeight || 720);
    if (info?.key === "ANIME_MAPPING") return Math.max(520, viewport - 32);
    const cap = ["PLEX", "JELLYFIN", "EMBY"].includes(info?.key) ? 660 : 620;
    return Math.max(360, Math.min(cap, viewport - 176));
  }

  function cssPx(node, prop) {
    if (typeof getComputedStyle !== "function") return 0;
    const value = parseFloat(getComputedStyle(node).getPropertyValue(prop));
    return Number.isFinite(value) ? value : 0;
  }

  function connectionModalNavHeight(nav) {
    if (!nav) return 0;
    const tiles = Array.from(nav.querySelectorAll(":scope > .cw-subtile[data-sub]"));
    const profile = nav.querySelector(":scope > .cw-connection-profile-card");
    const tileHeight = Math.max(76, ...tiles.map((tile) => Math.ceil(tile.getBoundingClientRect?.().height || tile.offsetHeight || 0)));
    const profileHeight = profile ? Math.ceil((profile.scrollHeight || profile.offsetHeight || 0) + cssPx(profile, "margin-top") + cssPx(profile, "margin-bottom")) : 0;
    return Math.ceil((tiles.length * tileHeight) + profileHeight);
  }

  function connectionModalContentHeight(scroller) {
    if (!scroller) return 0;
    const activePanel = scroller.querySelector(":scope > .cw-subpanel.active") || scroller.querySelector(":scope > .cw-subpanel");
    const contentNode = activePanel || scroller.firstElementChild || scroller;
    const padding = cssPx(scroller, "padding-top") + cssPx(scroller, "padding-bottom");
    return Math.ceil((contentNode.scrollHeight || contentNode.offsetHeight || 0) + padding);
  }

  function updateConnectionModalSize(panel, info) {
    if (!panel) return;
    const scroller = panel.querySelector(":scope > .cw-subpanels") || panel.querySelector(":scope > .auth-card");
    const footer = panel.querySelector(":scope > .cw-connection-modal-footer");
    const nav = panel.querySelector(":scope > .cw-subtiles");
    const maxHeight = connectionModalMaxHeight(info);
    if (!scroller) return;
    const footerHeight = Math.ceil(footer?.offsetHeight || 0);
    const navHeight = connectionModalNavHeight(nav);
    const contentHeight = connectionModalContentHeight(scroller);
    const FIT_ALLOWANCE = 8;
    const wanted = Math.max(navHeight, contentHeight) + footerHeight + FIT_ALLOWANCE;
    const panelHeight = Math.max(360, Math.min(maxHeight, wanted));
    const contentBoxHeight = Math.max(220, panelHeight - footerHeight);
    const next = `${panelHeight}px`;
    const readStyle = (name) => typeof panel.style?.getPropertyValue === "function" ? panel.style.getPropertyValue(name) : panel.style?.[name];
    if (readStyle("--cw-connection-panel-height") !== next) setConnectionStyle(panel, "--cw-connection-panel-height", next);
    const nextContent = `${contentBoxHeight}px`;
    if (readStyle("--cw-connection-content-height") !== nextContent) setConnectionStyle(panel, "--cw-connection-content-height", nextContent);
  }

  function scheduleConnectionModalSize(panel, info) {
    if (!panel) return;
    if (panel.__cwConnectionModalSizeFrame) cancelAnimationFrame(panel.__cwConnectionModalSizeFrame);
    panel.__cwConnectionModalSizeFrame = requestAnimationFrame(() => {
      panel.__cwConnectionModalSizeFrame = 0;
      updateConnectionModalSize(panel, info);
    });
  }

  function setConnectionStyle(el, name, value) {
    if (!el?.style) return;
    if (typeof el.style.setProperty === "function") el.style.setProperty(name, value);
    else el.style[name] = value;
  }

  function ensureConnectionModalSteps(panel, info) {
    const steps = info.steps;
    if (!steps?.length) return;
    connectionIntroPanels(panel, info).forEach((authPanel) => {
      if (!authPanel || authPanel.querySelector(":scope > .cw-connection-auth-intro")) return;
      const intro = document.createElement("div");
      intro.className = "cw-connection-auth-intro";
      intro.innerHTML = `<div class="cw-connection-steps">${steps.map((step) => `<div><span>${step[0]}</span><strong>${step[1]}</strong><small>${step[2]}</small></div>`).join("")}</div>`;
      authPanel.insertBefore(intro, authPanel.firstElementChild || null);
    });
  }

  function ensureConnectionModalJourney(panel, info) {
    const data = info?.journey;
    if (!data?.length) return;
    const icon = info?.key === "TMDB_METADATA" || info?.key === "PUBLICMETADB" ? "key" : info?.key === "ANIME_MAPPING" ? "hub" : "link";
    connectionIntroPanels(panel, info).forEach((authPanel) => {
      if (!authPanel) return;
      let node = authPanel.querySelector(":scope > .cw-auth-journey");
      if (!node) {
        node = document.createElement("div");
        node.className = "cw-auth-journey";
        authPanel.insertBefore(node, authPanel.firstElementChild || null);
      }
      setConnectionStyle(node, "--cw-auth-c1", data[2] || "72,92,255");
      setConnectionStyle(node, "--cw-auth-c2", data[3] || data[2] || "0,224,132");
      setConnectionStyle(node, "--cw-auth-logo", `url("/assets/img/${data[4] || info.logo}.svg")`);
      node.innerHTML = `<span class="material-symbols-rounded cw-auth-journey-icon" aria-hidden="true">${icon}</span><div class="cw-auth-journey-text"><div class="cw-auth-journey-title">${data[0]}</div><div class="cw-auth-journey-copy">${data[1]}</div></div><a class="cw-auth-journey-help" href="${info.help}" target="_blank" rel="noopener noreferrer" aria-label="Open ${data[0]} guide" title="Open guide"><span class="material-symbols-rounded" aria-hidden="true">help</span></a>`;
    });
  }

  function applyConnectionModalOrder(panel, info) {
    const activePanel = panel.querySelector(".cw-subpanel.active") || panel.querySelector(".cw-subpanel") || panel;
    (info.order || []).forEach((selector, idx) => {
      activePanel.querySelectorAll(selector).forEach((el) => setConnectionStyle(el, "order", String(idx + 3)));
    });
    activePanel.querySelectorAll(".cw-auth-journey").forEach((el) => setConnectionStyle(el, "order", "1"));
    activePanel.querySelectorAll(".cw-connection-auth-intro").forEach((el) => setConnectionStyle(el, "order", "2"));
    (info.code || []).forEach((selector) => activePanel.querySelectorAll(selector).forEach((el) => {
      el.classList.add("cw-connection-code-block");
      setConnectionStyle(el, "order", "90");
    }));
  }

  function normalizeConnectionModalFields(panel, info) {
    if (info?.provider !== "tautulli") return;
    const userInput = panel.querySelector("#tautulli_user_id");
    const userField = userInput?.closest("div");
    const grid = panel.querySelector("#tautulli_server")?.closest(".grid2");
    if (!userField || !grid || userField.parentElement === grid) return;
    userField.classList.add("cw-tautulli-user-field");
    setConnectionStyle(userField, "margin-top", "0");
    setConnectionStyle(userField, "max-width", "none");
    grid.appendChild(userField);
  }

  function normalizeConnectionModalActions(panel, info) {
    Object.entries(info.labels || {}).forEach(([selector, label]) => panel.querySelectorAll(selector).forEach((el) => { el.textContent = label; }));
    (info.actions || []).forEach((cfg) => {
      panel.querySelectorAll(cfg.row).forEach((row) => {
        let actionRow = row;
        if (cfg.extract) {
          row.classList.add("cw-connection-method-row");
          row.classList.remove("cw-connection-action-row");
          actionRow = row.parentElement?.querySelector(":scope > .cw-connection-method-action-row");
          if (!actionRow) {
            actionRow = document.createElement("div");
            actionRow.className = "cw-connection-method-action-row";
            row.insertAdjacentElement("afterend", actionRow);
          }
          row.querySelectorAll(cfg.extract).forEach((el) => actionRow.appendChild(el));
        }
        actionRow.classList.add("cw-connection-action-row");
        if (cfg.wrapParent && actionRow.parentElement) {
          actionRow.parentElement.classList.add(cfg.parentClass || "cw-connection-action-wrap");
          if (cfg.order) setConnectionStyle(actionRow.parentElement, "order", String(cfg.order));
        } else if (cfg.order) {
          setConnectionStyle(actionRow, "order", String(cfg.order));
        }
        const buttons = Array.from(actionRow.querySelectorAll(cfg.buttons || ".btn"));
        buttons.forEach((btn) => btn.classList.add("cw-connection-primary-action"));
        if (!cfg.extract && buttons.length) {
          let group = actionRow.querySelector(":scope > .cw-connection-action-buttons");
          if (!group) {
            group = document.createElement("span");
            group.className = "cw-connection-action-buttons";
            actionRow.insertBefore(group, buttons[0]);
          }
          buttons.forEach((btn) => group.appendChild(btn));
        }
        const status = cfg.status ? panel.querySelector(cfg.status) : null;
        if (status) {
          status.classList.add("cw-connection-status-pill");
          if (cfg.connectedSelector && status.classList.contains("hidden")) {
            const connectedEl = panel.querySelector(cfg.connectedSelector);
            const connected = !!String(connectedEl?.value || connectedEl?.textContent || "").trim();
            if (connected) {
              status.textContent = cfg.connectedText || "Connected";
              status.classList.remove("hidden");
              status.classList.add("ok");
            }
          }
          actionRow.appendChild(status);
        }
      });
    });
    syncConnectionStatusDismissals(panel);
  }

  function connectionDeleteBlockedByProfiles(panel, info) {
    if (!panel || !info) return false;
    const sel = panel.querySelector(".cw-profile-switcher select");
    const current = String(sel?.value || "default").toLowerCase();
    if (current !== "default") return false;
    return configuredProfileIds(getCachedConfig(), info.provider).some((id) => String(id).toLowerCase() !== "default");
  }

  function flashConnectionResult(btn, ok) {
    if (!btn) return;
    if (!btn.__cwSaveLabel) btn.__cwSaveLabel = btn.textContent;
    if (btn.__cwSaveTimer) clearTimeout(btn.__cwSaveTimer);
    btn.classList.remove("is-saved", "is-failed");
    btn.innerHTML = `<span class="material-symbols-rounded cw-save-result-icon">${ok ? "check" : "close"}</span>`;
    btn.classList.add(ok ? "is-saved" : "is-failed");
    btn.__cwSaveTimer = setTimeout(() => {
      btn.textContent = btn.__cwSaveLabel || "Save changes";
      btn.classList.remove("is-saved", "is-failed");
    }, 1600);
  }

  function resetConnectionDeleteConfirm(btn) {
    if (!btn) return;
    if (btn.__cwConnectionDeleteTimer) clearTimeout(btn.__cwConnectionDeleteTimer);
    btn.__cwConnectionDeleteTimer = 0;
    btn.dataset.cwConfirmDelete = "";
    btn.classList.remove("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">delete</span><span>Delete connection</span>`;
  }

  function armConnectionDeleteConfirm(btn) {
    if (!btn) return;
    btn.dataset.cwConfirmDelete = "1";
    btn.classList.add("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span>Confirm delete</span>`;
    if (btn.__cwConnectionDeleteTimer) clearTimeout(btn.__cwConnectionDeleteTimer);
    btn.__cwConnectionDeleteTimer = setTimeout(() => resetConnectionDeleteConfirm(btn), 4200);
  }

  function ensureConnectionModalFooter(panel, info) {
    if (!panel.querySelector(":scope > .cw-connection-modal-footer")) {
      const footer = document.createElement("div");
      footer.className = "cw-connection-modal-footer";
      footer.innerHTML = `<button type="button" class="btn danger cw-connection-footer-delete"><span class="material-symbols-rounded" aria-hidden="true">delete</span><span>Delete connection</span></button><span class="cw-connection-footer-warn hidden" role="alert" aria-live="polite"></span><button type="button" class="btn cw-connection-footer-cancel">Cancel</button><button type="button" class="btn primary cw-connection-footer-save">Save changes</button>`;
      panel.appendChild(footer);
      footer.querySelector(".cw-connection-footer-delete")?.addEventListener("click", (ev) => {
        const trigger = ev.currentTarget;
        if (connectionDeleteBlockedByProfiles(panel, info)) {
          resetConnectionDeleteConfirm(trigger);
          try { window.CW.AuthShared.showConnectionWarning("Remove the additional profiles before deleting the main connection."); } catch {}
          return;
        }
        if (trigger?.dataset?.cwConfirmDelete !== "1") {
          armConnectionDeleteConfirm(trigger);
          return;
        }
        resetConnectionDeleteConfirm(trigger);
        const btn = info.deleteSelector ? panel.querySelector(info.deleteSelector) || document.querySelector(info.deleteSelector) : null;
        btn?.click?.();
        [700, 1600].forEach((delay) => setTimeout(() => {
          const warn = panel.querySelector(".cw-connection-footer-warn");
          if (warn && !warn.classList.contains("hidden")) return;
          showConnectionDisconnected(panel);
        }, delay));
      });
      footer.querySelector(".cw-connection-footer-cancel")?.addEventListener("click", () => {
        resetConnectionDeleteConfirm(footer.querySelector(".cw-connection-footer-delete"));
        closeAuthProviderOverlay();
      });
      footer.querySelector(".cw-connection-footer-save")?.addEventListener("click", async (ev) => {
        resetConnectionDeleteConfirm(footer.querySelector(".cw-connection-footer-delete"));
        const btn = ev.currentTarget;
        const keepOpen = ["PLEX", "JELLYFIN", "EMBY"].includes(info.key);
        try {
          const ret = window.saveSettings?.(btn);
          if (ret && typeof ret.then === "function") await ret;
          const cfg = await loadConfig(true);
          syncConnectionSuccessState(panel, info, cfg);
          flashConnectionResult(btn, true);
          if (!keepOpen) setTimeout(closeAuthProviderOverlay, 1100);
        } catch {
          flashConnectionResult(btn, false);
        }
      });
    }
    const deleteBtn = panel.querySelector(".cw-connection-footer-delete");
    if (!info.deleteSelector) resetConnectionDeleteConfirm(deleteBtn);
    deleteBtn?.classList.toggle("hidden", !info.deleteSelector);
  }

  function enhanceConnectionModal(section, overlay, key) {
    const info = connectionInfoForKey(key);
    const panel = connectionPanelFor(section, info);
    if (!section || !info || !panel) return;
    overlay?.classList.add("cw-connection-overlay");
    overlay.dataset.cwConnectionSize = info.size || "wide";
    overlay.dataset.cwConnectionProvider = info.provider;
    panel.classList.add("cw-connection-modal-panel");
    panel.dataset.cwConnectionProvider = info.provider;
    panel.dataset.cwConnectionKey = info.key;
    setConnectionStyle(panel, "--cw-connection-c1", info.journey?.[2] || "72,92,255");
    setConnectionStyle(panel, "--cw-connection-c2", info.journey?.[3] || info.journey?.[2] || "72,92,255");
    setConnectionStyle(panel, "--cw-connection-watermark", `url("/assets/img/${info.logo}.svg")`);
    syncConnectionDialogHead(overlay, info);
    ensureConnectionModalNav(panel, info);
    ensureConnectionModalJourney(panel, info);
    ensureConnectionModalSteps(panel, info);
    normalizeConnectionModalFields(panel, info);
    applyConnectionModalOrder(panel, info);
    normalizeConnectionModalActions(panel, info);
    ensureConnectionModalFooter(panel, info);
    if (["PLEX", "JELLYFIN", "EMBY"].includes(info.key)) selectConnectionModalSub(panel, info, "auth", overlay);
    syncConnectionModalCopy(panel, info, overlay);
    panel.__cwConnectionWasConnected = connectionModalConnected(panel, info);
    updateConnectionSaveEnabled(panel, info);
    ensureConnectionSuccessBurst(panel);
    scheduleConnectionModalSize(panel, info);
    resetConnectionModalScroll(panel);
    requestAnimationFrame(() => {
      resetConnectionModalScroll(panel);
      updateConnectionModalSize(panel, info);
    });
    if (!panel.__cwConnectionModalResizeBound) {
      panel.__cwConnectionModalResizeBound = true;
      window.addEventListener("resize", () => {
        if (panel.isConnected && !overlay?.classList?.contains("hidden")) scheduleConnectionModalSize(panel, info);
      }, { passive: true });
    }
    if (!panel.__cwConnectionModalObserveBound) {
      panel.__cwConnectionModalObserveBound = true;
      const onPanelChange = () => {
        if (panel.isConnected && !overlay?.classList?.contains("hidden")) {
          scheduleConnectionModalSize(panel, info);
          syncConnectionStatusDismissals(panel);
          syncConnectionSuccessState(panel, info);
        }
      };
      if (typeof ResizeObserver === "function") {
        const ro = new ResizeObserver(onPanelChange);
        [panel.querySelector(":scope > .cw-subtiles"), panel.querySelector(":scope > .cw-subpanels"), panel.querySelector(":scope > .auth-card"), panel.querySelector(":scope > .cw-connection-modal-footer")]
          .filter(Boolean)
          .forEach((node) => ro.observe(node));
        panel.__cwConnectionModalResizeObserver = ro;
      }
      if (typeof MutationObserver === "function") {
        const mo = new MutationObserver(onPanelChange);
        mo.observe(panel, { childList: true, subtree: true, characterData: true });
        panel.__cwConnectionModalMutationObserver = mo;
      }
    }
  }

  async function openAuthProviderForm(key) {
    const info = authProviderInfo(key);
    if (!info.sectionId) return;
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    if (!document.getElementById(info.sectionId)) await mountAuthProviders();
    const overlay = ensureAuthOverlay(slot);
    const form = overlay.querySelector("#cw-auth-provider-form");
    const section = document.getElementById(info.sectionId);
    if (!form || !section) return;
    parkActiveAuthForm();
    form.appendChild(section);
    section.classList.add("open");
    section.querySelector(":scope > .head")?.setAttribute("aria-expanded", "true");
    openAuthOverlay("form", info.key);
    try { await window.cwEnsureAuthSection?.(info.sectionId); } catch {}
    const providerId = connectionInfoForKey(info.key)?.provider || String(info.key || "").toLowerCase();
    try { window.cwAuth?.[providerId]?.init?.(); } catch {}
    enhanceConnectionModal(section, overlay, info.key);
    wireCopyButtons();
  }

  async function openMetadataProviderForm(key) {
    const info = metadataProviderInfo(key);
    if (!info) return;
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    if (!document.getElementById(info.sectionId)) await mountMetadataProviders();
    try { window.cwMetaProviderEnsure?.(); } catch {}
    const overlay = ensureAuthOverlay(slot);
    const form = overlay.querySelector("#cw-auth-provider-form");
    const section = document.getElementById(info.sectionId);
    if (!form || !section) return;
    parkActiveAuthForm();
    form.appendChild(section);
    section.classList.add("open");
    section.querySelector(":scope > .head")?.setAttribute("aria-expanded", "true");
    overlay.classList.remove("hidden");
    overlay.setAttribute("aria-hidden", "false");
    overlay.querySelector("#cw-auth-provider-picker")?.classList.add("hidden");
    form.classList.remove("hidden");
    const title = overlay.querySelector("#cw-auth-dialog-title");
    const kicker = overlay.querySelector("#cw-auth-dialog-kicker");
    if (title) title.textContent = info.label;
    if (kicker) kicker.textContent = "Metadata";
    if (info.key === "TMDB_METADATA") {
      try { window.cwMetaProviderSubSelect?.("tmdb", "api", { persist: false }); } catch {}
    }
    if (info.key === "ANIME_MAPPING") {
      try { window.cwAnimeMappingRefreshStatus?.(); } catch {}
    }
    enhanceConnectionModal(section, overlay, info.key);
    requestAnimationFrame(() => overlay.querySelector("[data-cw-auth-close]")?.focus?.());
  }

  function pruneEmptyProfileOnClose() {
    const overlay = document.getElementById("cw-auth-connection-overlay");
    const panel = overlay?.querySelector(".cw-connection-modal-panel");
    if (!panel) return;
    const info = connectionInfoForKey(panel.dataset.cwConnectionKey);
    if (!info || !connectionModalSupportsProfiles(info)) return;
    const switcher = panel.querySelector(".cw-profile-switcher");
    const apiProvider = String(switcher?.dataset?.cwProfileProvider || info.provider || "");
    const inst = String(switcher?.querySelector("select")?.value || "default");
    if (!apiProvider || inst.toLowerCase() === "default") return;
    (async () => {
      try {
        const cfg = await loadConfig(true);
        if (configuredProfileIds(cfg, info.provider).some((id) => String(id) === inst)) return;
        await fetch(`/api/provider-instances/${encodeURIComponent(apiProvider)}/${encodeURIComponent(inst)}`, { method: "DELETE", cache: "no-store" });
        try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      } catch {}
    })();
  }

  function closeAuthProviderOverlay() {
    try { pruneEmptyProfileOnClose(); } catch {}
    parkActiveAuthForm();
    try { window.CW.AuthShared.clearConnectionWarnings(); } catch {}
    const overlay = document.getElementById("cw-auth-connection-overlay");
    overlay?.classList.add("hidden");
    overlay?.setAttribute("aria-hidden", "true");
  }

  function bindAuthPresentation(slot) {
    if (!slot || slot.__cwAuthPresentationBound) return;
    slot.__cwAuthPresentationBound = true;
    slot.addEventListener("click", (event) => {
      const target = event.target;
      if (target?.closest?.("#cw-auth-add-provider")) {
        event.preventDefault();
        openAuthOverlay("picker", "", "provider");
        return;
      }
      if (target?.closest?.("#cw-auth-add-metadata")) {
        event.preventDefault();
        openAuthOverlay("picker", "", "metadata");
        return;
      }
      const emptyAdd = target?.closest?.("[data-cw-auth-empty-add]");
      if (emptyAdd) {
        event.preventDefault();
        openAuthOverlay("picker", "", emptyAdd.dataset.cwAuthEmptyAdd === "metadata" ? "metadata" : "provider");
        return;
      }
      const openCard = target?.closest?.("[data-cw-auth-open]");
      if (openCard) {
        event.preventDefault();
        openAuthProviderForm(openCard.dataset.cwAuthOpen).catch((e) => console.warn("open auth provider failed", e));
        return;
      }
      const openMetaCard = target?.closest?.("[data-cw-meta-open]");
      if (openMetaCard) {
        event.preventDefault();
        openMetadataProviderForm(openMetaCard.dataset.cwMetaOpen).catch((e) => console.warn("open metadata provider failed", e));
        return;
      }
      const pickCard = target?.closest?.("[data-cw-auth-pick]");
      if (pickCard) {
        event.preventDefault();
        openAuthProviderForm(pickCard.dataset.cwAuthPick).catch((e) => console.warn("open auth provider failed", e));
        return;
      }
      const pickMetaCard = target?.closest?.("[data-cw-meta-pick]");
      if (pickMetaCard) {
        event.preventDefault();
        openMetadataProviderForm(pickMetaCard.dataset.cwMetaPick).catch((e) => console.warn("open metadata provider failed", e));
        return;
      }
      if (target?.closest?.("[data-cw-auth-close]") || target?.classList?.contains("cw-auth-overlay")) {
        event.preventDefault();
        closeAuthProviderOverlay();
      }
    });
  }

  async function refreshAuthPresentation(slot, force = false) {
    const cfg = await loadConfig(!!force);
    renderAuthCards(slot, cfg);
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
        bindAuthPresentation(slot);
        ensureAuthShell(slot);
        initMountedAuthSections(slot);

        window.initMDBListAuthUI?.();
        window.initPublicMetaDBAuthUI?.();
        window.initTautulliAuthUI?.();
        window.initAniListAuthUI?.();

        await refreshAuthPresentation(slot, !!force);
        wireCopyButtons();

        ["trakt_client_id", "trakt_client_secret"].forEach((id) => {
          const el = document.getElementById(id);
          if (!el || el.__cwHintBound) return;
          el.addEventListener("input", () => window.updateTraktHint?.());
          el.__cwHintBound = true;
        });

        await window.hydrateAuthFromConfig?.();
        await refreshAuthPresentation(slot, false);
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
          document.getElementById("sec-meta")?.classList.add("cw-meta-integrated-source");
          document.querySelector('[data-target="sec-meta"]')?.classList.add("cw-meta-integrated-source");
          const authSlot = document.getElementById("auth-providers");
          if (authSlot) refreshAuthPresentation(authSlot, false).catch(() => {});
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
        document.getElementById("sec-meta")?.classList.add("cw-meta-integrated-source");
        document.querySelector('[data-target="sec-meta"]')?.classList.add("cw-meta-integrated-source");
        const authSlot = document.getElementById("auth-providers");
        if (authSlot) refreshAuthPresentation(authSlot, false).catch(() => {});
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
    const meta = window.CW?.ProviderMeta;
    if (typeof meta?.matchKey === "function") return meta.matchKey(v);
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
        const visible = Array.isArray(arr) && arr.some((item) => typeof item?.configured === "boolean")
          ? arr.filter((item) => item?.configured !== false)
          : arr;
        if (!Array.isArray(visible) || !visible.length) {
          div.innerHTML = '<div class="muted">No providers discovered.</div>';
          return [];
        }

        window.cx = window.cx || {};
        window.cx.providers = visible;

        if (typeof window.renderConnections === "function") {
          try { window.renderConnections(); } catch (e) { console.warn("renderConnections failed", e); }
        } else {
          const chip = (label, on) => `<span class="badge ${on ? "" : "feature-disabled"}" style="margin-left:6px">${label}</span>`;
          div.innerHTML = visible.map((p) => {
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
        return visible;
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

  function currentSettingsPane() {
    const active = document.querySelector("#page-settings .cw-settings-pane.active");
    return String(active?.dataset?.pane || window.__cwSettingsPane || "").toLowerCase();
  }

  async function ensureProvidersPaneReady(force = false) {
    if (authSetupPending()) return;
    const page = document.getElementById("page-settings");
    const settingsVisible = !!(page && !page.classList.contains("hidden"));
    const pane = currentSettingsPane();
    if (!settingsVisible || !["providers", "sync"].includes(pane)) return;

    if (pane === "sync") {
      await loadProviders(true);
      return;
    }

    await Promise.allSettled([
      mountMetadataProviders(!!force),
      mountAuthProviders(!!force),
      loadProviders(!!force),
    ]);
  }

  document.addEventListener("DOMContentLoaded", () => {
    if (authSetupPending()) return;
    wireCopyButtons();
    updateFlowRailLogos();
    ["cx-src", "cx-dst"].forEach((id) => document.getElementById(id)?.addEventListener("change", updateFlowRailLogos));
    try { ensureProvidersPaneReady(); } catch {}
  });

  document.addEventListener("cw-settings-pane-changed", (ev) => {
    if (!["providers", "sync"].includes(String(ev?.detail?.pane || "").toLowerCase())) return;
    try { ensureProvidersPaneReady(); } catch {}
  });

  document.addEventListener("tab-changed", (ev) => {
    const tab = String(ev?.detail?.id || ev?.detail?.tab || "").toLowerCase();
    if (tab !== "settings") return;
    setTimeout(() => {
      try { ensureProvidersPaneReady(); } catch {}
    }, 0);
  });

  document.addEventListener("keydown", (ev) => {
    if (ev.key !== "Escape") return;
    closeAuthProviderOverlay();
  }, true);

  document.addEventListener("cw-auth-profile-created", (ev) => {
    const overlay = document.getElementById("cw-auth-connection-overlay");
    if (!overlay || overlay.classList.contains("hidden")) return;
    const target = ev?.target;
    let panel = target?.closest?.(".cw-connection-modal-panel") || null;
    if (!panel) {
      const provider = String(ev?.detail?.provider || "").toLowerCase();
      panel = Array.from(document.querySelectorAll("#cw-auth-provider-form .cw-connection-modal-panel"))
        .find((node) => String(node.dataset.cwConnectionProvider || "").toLowerCase() === provider) || null;
    }
    const info = connectionInfoForKey(panel?.dataset?.cwConnectionKey);
    if (!panel || !info?.tabs?.auth) return;
    setTimeout(() => {
      if (!panel.isConnected || overlay.classList.contains("hidden")) return;
      selectConnectionModalSub(panel, info, "auth", overlay);
      scheduleConnectionModalSize(panel, info);
    }, 0);
  }, true);

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") setTimeout(flushPendingConnectionSuccess, 120);
  }, true);
  window.addEventListener("focus", () => setTimeout(flushPendingConnectionSuccess, 120), { passive: true });

  document.addEventListener("cw-provider-connected", (ev) => {
    const overlay = document.getElementById("cw-auth-connection-overlay");
    if (!overlay || overlay.classList.contains("hidden")) return;
    const provider = String(ev?.detail?.provider || "").toLowerCase();
    const key = String(ev?.detail?.key || "").toUpperCase();
    const panel = Array.from(document.querySelectorAll("#cw-auth-provider-form .cw-connection-modal-panel"))
      .find((node) => key ? String(node.dataset.cwConnectionKey || "").toUpperCase() === key : (provider && String(node.dataset.cwConnectionProvider || "").toLowerCase() === provider)) || null;
    const info = connectionInfoForKey(panel?.dataset?.cwConnectionKey || key);
    if (!panel || !info) return;
    panel.__cwConnectionWasConnected = true;
    playConnectionSuccess(panel, info);
  }, true);

  window.addEventListener("auth-changed", () => {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    refreshAuthPresentation(slot, true).catch(() => {});
  });

  document.addEventListener("cw-status-updated", () => {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    refreshAuthPresentation(slot, false).catch(() => {});
  }, true);

  const ProvidersUI = {
    updateFlowRailLogos,
    ensureProvidersPaneReady,
    mountAuthProviders,
    mountMetadataProviders,
    loadProviders,
    openAddConnection: () => openAuthOverlay("picker", "", "provider"),
    openAddMetadata: () => openAuthOverlay("picker", "", "metadata"),
    openAuthProviderForm,
    openMetadataProviderForm,
    closeAuthProviderOverlay,
    refreshAuthPresentation: (force = false) => {
      const slot = document.getElementById("auth-providers");
      return slot ? refreshAuthPresentation(slot, !!force) : Promise.resolve();
    },
  };

  (window.CW ||= {}).ProvidersUI = ProvidersUI;
  Object.assign(window, ProvidersUI);
})();
