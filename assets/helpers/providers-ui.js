/* assets/helpers/providers-ui.js */
/* Extracted provider/auth/metadata UI from core.js */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function(){
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
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
    { id: "sec-auth-media", title: T("connections.groups.mediaServers"), keys: ["PLEX", "JELLYFIN", "EMBY", "SILO", "TOFA"] },
    { id: "sec-auth-trackers", title: T("connections.groups.trackers"), keys: ["CROSSWATCH", "TRAKT", "SIMKL", "TMDB", "MDBLIST", "PUBLICMETADB", "ANILIST", "MYANIMELIST", "KITSU", "WETRAKR", "PUNCHPLAY", "BINGEBASE", "FLICKLIST", "MOVIEPARADISE", "FLOPPY", "SCROB"] },
    { id: "sec-auth-clients", title: T("connections.groups.mediaClients"), keys: ["NUVIO", "KODI", "STREMIO"] },
    { id: "sec-auth-others", title: T("connections.groups.others"), keys: ["TAUTULLI", "TRACEARR"] },
  ]);
  const AUTH_GROUP_BY_KEY = Object.freeze(Object.fromEntries(AUTH_GROUPS.flatMap((group) => group.keys.map((key) => [key, group.id]))));
  const META_GROUP = Object.freeze({ id: "sec-auth-metadata", title: T("connections.groups.metadata"), keys: ["TMDB_METADATA", "FANART_METADATA", "ANIME_MAPPING"] });
  const META_ITEMS = Object.freeze({
    TMDB_METADATA: { key: "TMDB_METADATA", label: T("connections.metaItems.tmdb"), logoKey: "TMDB", sectionId: "sec-meta-tmdb", provider: "tmdb" },
    FANART_METADATA: { key: "FANART_METADATA", label: T("connections.metaItems.fanart"), icon: "photo_library", sectionId: "sec-meta-fanart", provider: "fanart" },
    ANIME_MAPPING: { key: "ANIME_MAPPING", label: T("connections.metaItems.anime"), icon: "shield", sectionId: "sec-meta-anime-mapping", provider: "anime-mapping" },
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

  function stremioAddonIds(cfg) {
    const ids = cfg?.security?.webhook_ids;
    if (!ids || typeof ids !== "object") return [];
    return Object.keys(ids)
      .filter((key) => key.startsWith("stremioaddon:") && hasConfiguredValue(ids[key]))
      .map((key) => key.slice("stremioaddon:".length) || "default");
  }

  function stremioAddonOnly(cfg, provider, id) {
    if (String(provider || "").toLowerCase() !== "stremio") return false;
    const raw = String(id || "default");
    if (!stremioAddonIds(cfg).some((item) => item === raw)) return false;
    const block = providerConfigBlock(cfg, "stremio");
    const source = raw.toLowerCase() === "default" ? block : block?.instances?.[raw];
    return !profileConfigured("stremio", source, cfg);
  }

  function kodiAddonIds(cfg) {
    const ids = cfg?.security?.webhook_ids;
    if (!ids || typeof ids !== "object") return [];
    return Object.keys(ids)
      .filter((key) => key.startsWith("kodiwatcher:") && hasConfiguredValue(ids[key]))
      .map((key) => key.slice("kodiwatcher:".length) || "default");
  }

  function kodiAddonOnly(cfg, provider, id) {
    if (String(provider || "").toLowerCase() !== "kodi") return false;
    const raw = String(id || "default");
    if (!kodiAddonIds(cfg).some((item) => item === raw)) return false;
    const block = providerConfigBlock(cfg, "kodi");
    const source = raw.toLowerCase() === "default" ? block : block?.instances?.[raw];
    return !profileConfigured("kodi", source, cfg);
  }

  let managedInstances = {};

  function managedUser() {
    return document.documentElement.dataset.cwRole === "user";
  }

  async function loadManagedInstances() {
    if (!managedUser()) return;
    try { await window.__cwAuthBootstrapPromise; } catch {}
    try {
      const res = await fetch(`/api/provider-instances?configured_only=true&ts=${Date.now()}`, { cache: "no-store", credentials: "same-origin" });
      const data = res.ok ? await res.json() : {};
      managedInstances = data && typeof data === "object" ? data : {};
    } catch {
      managedInstances = {};
    }
  }

  function managedInstanceIds(provider) {
    const key = String(provider || "").toUpperCase() === "TMDB_SYNC" ? "TMDB" : String(provider || "").toUpperCase();
    const rows = Array.isArray(managedInstances[key]) ? managedInstances[key] : [];
    return rows.filter((row) => row && row.id && row.id !== "default" && row.configured !== false).map((row) => String(row.id));
  }

  function configuredProviderKeys(cfg = getCachedConfig()) {
    if (managedUser()) {
      return new Set(Object.keys(managedInstances).filter((key) => managedInstanceIds(key).length).map((key) => authProviderInfo(key).key).filter(Boolean));
    }
    let set = new Set();
    try {
      if (typeof window.getConfiguredProviders === "function") {
        set = new Set(Array.from(window.getConfiguredProviders(cfg) || []).map((key) => authProviderInfo(key).key).filter(Boolean));
      }
    } catch {}
    if (stremioAddonIds(cfg).length) set.add("STREMIO");
    if (kodiAddonIds(cfg).length) set.add("KODI");
    return set;
  }

  function statusProviderData(key) {
    const providers = window.__CW_PROVIDER_STATUS__?.providers || window._statusCache?.providers || window.loadStatusCache?.()?.providers || {};
    if (!providers || typeof providers !== "object") return null;
    const upper = String(key || "").toUpperCase();
    const lower = upper.toLowerCase();
    return providers[lower] || providers[upper] || null;
  }

  function sortProfileIds(ids) {
    return Array.from(new Set((ids || []).map((id) => String(id || "").trim()).filter(Boolean)))
      .sort((a, b) => (a !== "default") - (b !== "default") || a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));
  }

  function profileStatusEntryConnected(entry) {
    if (typeof entry === "boolean") return entry;
    if (!entry || typeof entry !== "object") return null;
    if (typeof entry.connected === "boolean") return entry.connected;
    if (typeof entry.ok === "boolean") return entry.ok;
    if (typeof entry.authorized === "boolean") return entry.authorized;
    const text = String(entry.status || entry.state || "").trim().toLowerCase();
    if (/^(ok|up|connected|ready|true|on|online|active|authorized|valid)$/.test(text)) return true;
    if (/^(no|down|disconnected|false|off|disabled|failed|error|invalid|expired|unauthorized|forbidden)$/.test(text)) return false;
    return null;
  }

  function profileStatusEntry(data, id) {
    const inst = data?.instances;
    if (!inst || typeof inst !== "object") return null;
    const raw = String(id || "").trim();
    if (Object.prototype.hasOwnProperty.call(inst, raw)) return inst[raw];
    const found = Object.keys(inst).find((key) => String(key || "").toLowerCase() === raw.toLowerCase());
    return found ? inst[found] : null;
  }

  function settingsScope() {
    const scope = window.CW?.SettingsScope;
    return scope && typeof scope.active === "function" && scope.active() ? scope : null;
  }

  function scopedProfileIds(provider, ids) {
    const scope = settingsScope();
    return scope ? ids.filter((id) => scope.allowsInstance(provider, id)) : ids;
  }

  function authProfileIds(cfg, provider, data = null, connected = false) {
    const ids = configuredProfileIds(cfg, provider, connected);
    const inst = data?.instances;
    if (inst && typeof inst === "object") {
      Object.keys(inst).forEach((id) => {
        if (String(id || "").trim()) ids.push(String(id));
      });
    }
    return scopedProfileIds(provider, sortProfileIds(ids));
  }

  function authProfileState(cfg, provider, id, data = statusProviderData(provider), connected = false) {
    if (stremioAddonOnly(cfg, provider, id) || kodiAddonOnly(cfg, provider, id)) return "ok";
    const entry = profileStatusEntry(data, id);
    const entryConnected = profileStatusEntryConnected(entry);
    if (entryConnected === true) return "ok";
    if (entryConnected === false) return "fail";
    if (entry && typeof entry === "object") {
      if (entry.probed === true) return "fail";
      if (entry.configured === true) return "ok";
    }
    if (data?.instances && typeof data.instances === "object") {
      return configuredProfileIds(cfg, provider, connected).some((pid) => String(pid) === String(id)) ? "ok" : "fail";
    }
    if (data && typeof data.connected === "boolean") return data.connected ? "ok" : "fail";
    return configuredProfileIds(cfg, provider, connected).some((pid) => String(pid) === String(id)) ? "ok" : "fail";
  }

  function failedProfileIds(cfg, provider, data = statusProviderData(provider), connected = false) {
    if (!data || typeof data !== "object") return [];
    return authProfileIds(cfg, provider, data, connected).filter((id) => authProfileState(cfg, provider, id, data, connected) === "fail");
  }

  function authStatusFor(key, configured, cfg = getCachedConfig()) {
    const data = statusProviderData(key);
    const failed = failedProfileIds(cfg, String(key || "").toLowerCase(), data, configured);
    if (failed.length) {
      const names = failed.map((id) => profileFriendlyName(cfg, String(key || "").toLowerCase(), id));
      const shown = names.slice(0, 2).join(", ");
      const overflow = names.length > 2 ? ` +${names.length - 2}` : "";
      return { text: T("connections.status.checkNamed", { names: `${shown}${overflow}` }), ok: false, failed: true };
    }
    if (String(key || "").toUpperCase() === "NUVIO" && configured) {
      return { text: T("common.status.connected"), ok: true };
    }
    if (String(key || "").toUpperCase() === "STREMIO" && data?.connected !== true && stremioAddonIds(cfg).some((id) => stremioAddonOnly(cfg, "stremio", id))) {
      return { text: T("connections.status.scrobbleAddon"), ok: true };
    }
    if (String(key || "").toUpperCase() === "KODI" && data?.connected !== true && kodiAddonIds(cfg).some((id) => kodiAddonOnly(cfg, "kodi", id))) {
      return { text: "Kodi add-on", ok: true };
    }
    if (data && typeof data.connected === "boolean") {
      return data.connected ? { text: T("common.status.connected"), ok: true } : { text: T("connections.status.check"), ok: false, failed: true };
    }
    return configured ? { text: T("common.status.configured"), ok: true } : { text: T("common.status.notConfigured"), ok: false };
  }

  function authProviderLogo(key) {
    return window.CW?.ProviderMeta?.logoHtml?.(key, "cw-auth-provider-logo") || `<span class="token-text">${key || ""}</span>`;
  }

  function metadataProviderLogo(info) {
    if (info?.icon) return `<span class="material-symbols-rounded cw-auth-provider-icon" aria-hidden="true">${escHtml(info.icon)}</span>`;
    return window.CW?.ProviderMeta?.logoHtml?.(info?.logoKey || info?.label || "", "cw-auth-provider-logo") || `<span class="token-text">${info?.label || ""}</span>`;
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
    if (p === "kitsu") return hasConfiguredValue(b.access_token) && !b.reauth_required;
    if (p === "anilist") return hasConfiguredValue(b.access_token) || hasConfiguredValue(b.token);
    if (p === "mdblist") return hasConfiguredValue(b.api_key) || hasConfiguredValue(b.access_token);
    if (p === "myanimelist") return hasConfiguredValue(b.access_token) && !b.reauth_required;
    if (p === "wetrakr") return hasConfiguredValue(b.access_token) && !b.reauth_required;
    if (p === "punchplay") return hasConfiguredValue(b.access_token);
    if (p === "movieparadise") return hasConfiguredValue(b.access_token) && b.auth_error !== "reconnect_required";
    if (p === "bingebase") return hasConfiguredValue(b.access_token) || hasConfiguredValue(b.webhook_url);
    if (p === "flicklist") return hasConfiguredValue(b.api_key) || hasConfiguredValue(b.access_token) || hasConfiguredValue(b.token);
    if (p === "floppy") return hasConfiguredValue(b.server_url || b.server) && hasConfiguredValue(b.api_token || b.token);
    if (p === "scrob") return hasConfiguredValue(b.server_url) && hasConfiguredValue(b.api_key) && hasConfiguredValue(b.username) && hasConfiguredValue(b.password);
    if (p === "nuvio") return (hasConfiguredValue(b.access_token) || hasConfiguredValue(b.refresh_token)) && hasConfiguredValue(b.profile_id);
    if (p === "silo") return hasConfiguredValue(b.server_url) && (hasConfiguredValue(b.access_token) || hasConfiguredValue(b.refresh_token)) && hasConfiguredValue(b.profile_id) && b.auth_error !== "reconnect_required";
    if (p === "tofa") return hasConfiguredValue(b.server_url) && hasConfiguredValue(b.server_id) && (hasConfiguredValue(b.access_token) || hasConfiguredValue(b.refresh_token)) && hasConfiguredValue(b.profile_id) && b.auth_error !== "reconnect_required";
    if (p === "kodi") return hasConfiguredValue(b.server) && b.connection_verified === true;
    if (p === "stremio") return hasConfiguredValue(b.auth_key) || hasConfiguredValue(b.authKey);
    if (p === "tracearr") return hasConfiguredValue(b.server_url || b.server) && hasConfiguredValue(b.api_key);
    if (p === "tautulli") return hasConfiguredValue((b || cfg?.tautulli || cfg?.auth?.tautulli || {}).server_url || (b || cfg?.tautulli || cfg?.auth?.tautulli || {}).server);
    if (p === "tmdb") return hasConfiguredValue(b.account_id) || (hasConfiguredValue(b.api_key) && hasConfiguredValue(b.session_id || b.session));
    if (p === "crosswatch") return !!(cfg?.crosswatch || cfg?.CrossWatch) && b.connected === true && b.enabled !== false;
    return hasConfiguredValue(b.access_token) || hasConfiguredValue(b.api_key) || hasConfiguredValue(b.token);
  }

  function profileDisplayName(id) {
    const raw = String(id || "").trim();
    if (!raw || raw.toLowerCase() === "default") return T("common.labels.default");
    const generated = raw.match(/^[A-Z0-9]+-P0*([1-9]\d*)$/i);
    return generated ? `P${String(generated[1]).padStart(2, "0")}` : raw;
  }

  function configuredProfileIds(cfg, provider, connected = false) {
    if (managedUser()) return scopedProfileIds(provider, sortProfileIds(managedInstanceIds(provider)));
    const block = providerConfigBlock(cfg, provider);
    const out = [];
    if (profileConfigured(provider, block, cfg)) out.push("default");
    const insts = block?.instances;
    if (insts && typeof insts === "object") {
      Object.entries(insts).forEach(([id, instBlock]) => {
        if (String(id || "").trim() && profileConfigured(provider, instBlock, cfg)) out.push(String(id));
      });
    }
    if (String(provider || "").toLowerCase() === "stremio") out.push(...stremioAddonIds(cfg));
    if (String(provider || "").toLowerCase() === "kodi") out.push(...kodiAddonIds(cfg));
    if (!out.length && connected) out.push("default");
    return scopedProfileIds(provider, sortProfileIds(out));
  }

  function profileFriendlyName(cfg, provider, id) {
    const raw = String(id || "").trim();
    if (managedUser()) {
      const key = String(provider || "").toUpperCase() === "TMDB_SYNC" ? "TMDB" : String(provider || "").toUpperCase();
      const row = (Array.isArray(managedInstances[key]) ? managedInstances[key] : []).find((item) => String(item?.id) === raw);
      return String(row?.display_label || row?.label || profileDisplayName(raw));
    }
    const isDefault = raw.toLowerCase() === "default";
    const block = providerConfigBlock(cfg, provider);
    const instances = block?.instances;
    const source = isDefault ? block : (instances && typeof instances === "object" ? instances[raw] : null);
    const label = source && typeof source === "object" ? String(source.label || "").trim() : "";
    return label || (isDefault ? T("common.labels.default") : profileDisplayName(raw));
  }

  const PROFILE_NAME_LIMIT = 2;

  function authProfileSummary(card, cfg) {
    const base = { text: card.status.text, detail: "", title: card.status.text, dot: card.status.ok ? "ok" : (card.status.failed ? "fail" : "") };
    if (card.type !== "provider") return base;
    const data = statusProviderData(card.key || card.provider);
    const ids = authProfileIds(cfg, card.provider, data, !!card.status?.ok);
    const names = ids.map((id) => {
      const state = authProfileState(cfg, card.provider, id, data, !!card.status?.ok);
      return { id, name: profileFriendlyName(cfg, card.provider, id), state };
    });
    const onlyDefault = names.length === 1 && String(names[0].id).toLowerCase() === "default";
    if (!names.length || onlyDefault) return base;
    const title = `${card.status.text} - ${names.map((item) => `${item.name}: ${item.state === "ok" ? T("common.status.connected") : T("connections.status.check")}`).join(", ")}`;
    const okCount = names.filter((item) => item.state === "ok").length;
    if (okCount < names.length) {
      if (!okCount) return { ...base, title };
      return { text: `${okCount} of ${names.length} connected`, detail: "", title, dot: "warn" };
    }
    if (!card.status.ok) return { ...base, title };
    const detail = names.length > PROFILE_NAME_LIMIT ? `${names.length} instances` : names.map((item) => item.name).join(", ");
    return { ...base, detail, title };
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
    if (info.key === "FANART_METADATA") return cfg?.fanart?.enabled === true;
    return false;
  }

  function metadataStatusFor(key, cfg = getCachedConfig()) {
    const info = metadataProviderInfo(key);
    const on = metadataConfigured(key, cfg);
    if (info?.key === "ANIME_MAPPING" || info?.key === "FANART_METADATA") return on ? { text: T("common.status.enabled"), ok: true } : { text: T("connections.status.notEnabled"), ok: false };
    return on ? { text: T("common.status.connected"), ok: true } : { text: T("common.status.notConfigured"), ok: false };
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
            <div class="cw-auth-dialog-kicker" id="cw-auth-dialog-kicker">${TH("navigation.settingsMenu.connections")}</div>
            <h3 id="cw-auth-dialog-title">${TH("connections.dialog.addConnection")}</h3>
            <div class="cw-auth-dialog-subtitle" id="cw-auth-dialog-subtitle"></div>
          </div>
          <button type="button" class="cw-auth-icon-btn material-symbols-rounded" data-cw-auth-close aria-label="${TH("common.actions.close")}">close</button>
        </div>
        <div class="cw-auth-provider-picker cw-page-provider-picker" id="cw-auth-provider-picker"></div>
        <form class="cw-auth-provider-form hidden" id="cw-auth-provider-form" autocomplete="off" onsubmit="return false"></form>
      </div>`;
    slot.appendChild(overlay);
    bindAuthPresentation(slot);
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

  function setPickerHtml(picker, html) {
    if (picker.__cwPickerHtml === html) return;
    picker.__cwPickerHtml = html;
    picker.innerHTML = html;
  }

  function renderAuthPicker(overlay, cfg = getCachedConfig(), mode = "provider") {
    const picker = overlay?.querySelector("#cw-auth-provider-picker");
    if (!picker) return;
    const groups = mode === "metadata" ? [META_GROUP] : AUTH_GROUPS;
    const category = groups.some(group => group.id === overlay.dataset.cwPickerCategory) ? overlay.dataset.cwPickerCategory : "all";
    const search = overlay.dataset.cwPickerSearch || "";
    const tabs = [{id:"all", title:mode === "metadata" ? T("connections.picker.allMetadata") : T("connections.picker.allProviders")}, ...groups];
    if (picker.dataset.mode !== mode || !picker.querySelector(".cw-auth-picker-results")) {
      picker.dataset.mode = mode;
      setPickerHtml(picker, `
        <div class="cw-auth-picker-tools">
          <label class="cw-auth-picker-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input type="search" data-cw-picker-search aria-label="${TH("connections.picker.search")}" placeholder="${TH("connections.picker.searchPlaceholder")}" autocomplete="off"></label>
          <div class="cw-auth-picker-tabs" role="tablist" aria-label="${TH("connections.picker.categories")}">${tabs.map(tab => `<button type="button" role="tab" id="cw-picker-tab-${tab.id}" data-cw-picker-category="${tab.id}" aria-controls="cw-auth-picker-results">${escHtml(tab.title)}</button>`).join("")}</div>
        </div>
        <div id="cw-auth-picker-results" class="cw-auth-picker-results" role="tabpanel" tabindex="0"></div>
        <span class="sr-only" data-cw-picker-count role="status"></span>`);
    }
    const input = picker.querySelector("[data-cw-picker-search]");
    if (input.value !== search) input.value = search;
    picker.querySelectorAll("[data-cw-picker-category]").forEach(tab => {
      const active = tab.dataset.cwPickerCategory === category;
      tab.setAttribute("aria-selected", String(active));
      tab.tabIndex = active ? 0 : -1;
    });
    const configured = configuredProviderKeys(cfg);
    const supported = new Set(authProviderKeysWithSections());
    let total = 0;
    const rows = groups.filter(group => category === "all" || category === group.id).map(group => {
      const items = group.keys
        .filter(key => mode === "metadata" || supported.has(key))
        .map(key => mode === "metadata" ? metadataProviderInfo(key) : authProviderInfo(key))
        .filter(info => `${info.label} ${info.key}`.toLowerCase().includes(search.trim().toLowerCase()))
        .sort((a, b) => String(a.label).localeCompare(String(b.label), undefined, {sensitivity:"base"}))
        .map(info => {
          total++;
          const status = mode === "metadata" ? metadataStatusFor(info.key, cfg) : authStatusFor(info.key, configured.has(info.key), cfg);
          const failedNames = status.text.startsWith("Check connection: ") ? status.text.slice(18) : "";
          const label = failedNames ? T("connections.status.check") : status.text;
          const attr = mode === "metadata" ? "data-cw-meta-pick" : "data-cw-auth-pick";
          return `<button type="button" class="cw-auth-picker-row" ${attr}="${escHtml(info.key)}">
            <span class="cw-auth-picker-logo" aria-hidden="true">${mode === "metadata" ? metadataProviderLogo(info) : authProviderLogo(info.key)}</span>
            <span class="cw-auth-picker-copy"><strong>${escHtml(info.label)}</strong><small>${status.ok ? '<span class="cw-auth-picker-dot" aria-hidden="true"></span>' : ""}<span>${escHtml(label)}</span>${failedNames ? `<span class="cw-auth-picker-instance">${escHtml(failedNames)}</span>` : ""}</small></span>
            <span class="material-symbols-rounded cw-auth-picker-chevron" aria-hidden="true">chevron_right</span>
          </button>`;
        }).join("");
      return items ? `<section class="cw-auth-picker-group" aria-labelledby="cw-picker-heading-${group.id}"><h4 id="cw-picker-heading-${group.id}">${escHtml(group.title)}</h4><div class="cw-auth-picker-grid">${items}</div></section>` : "";
    }).join("");
    const results = picker.querySelector(".cw-auth-picker-results");
    results.setAttribute("aria-labelledby", `cw-picker-tab-${category}`);
    setPickerHtml(results, rows || '<div class="cw-auth-picker-empty">' + TH("connections.picker.empty") + '</div>');
    const count = picker.querySelector("[data-cw-picker-count]");
    const countText = `${total} provider${total === 1 ? "" : "s"} found`;
    if (count.textContent !== countText) count.textContent = countText;
  }

  function syncAuthChildren(parent, nextChildren, keyOf, update) {
    const current = new Map(Array.from(parent.children, (node) => [keyOf(node), node]));
    let cursor = parent.firstElementChild;
    for (const next of nextChildren) {
      const key = keyOf(next);
      const node = current.get(key) || next;
      current.delete(key);
      if (node !== next) update(node, next);
      if (node !== cursor) parent.insertBefore(node, cursor);
      cursor = node.nextElementSibling;
    }
    current.forEach((node) => node.remove());
  }

  function updateAuthDashboard(shell, html) {
    if (!shell.querySelector(":scope > .cw-auth-dashboard")) {
      shell.innerHTML = html;
      return;
    }
    const template = document.createElement("template");
    template.innerHTML = html;
    const patchCard = (node, next) => {
      if (node.className !== next.className) node.className = next.className;
      if (node.innerHTML !== next.innerHTML) node.innerHTML = next.innerHTML;
    };
    syncAuthChildren(shell.querySelector(".cw-auth-summary-row"),
      Array.from(template.content.querySelector(".cw-auth-summary-row").children),
      (node) => node.dataset.cwAuthSummary, patchCard);
    syncAuthChildren(shell.querySelector(".cw-auth-service-list"),
      Array.from(template.content.querySelector(".cw-auth-service-list").children),
      (node) => node.dataset.cwAuthGroup, (section, next) => {
        const grid = section.querySelector(":scope > .cw-auth-service-grid");
        if (!grid) return;
        syncAuthChildren(grid, Array.from(next.querySelector(".cw-auth-service-grid").children),
          (node) => node.dataset.cwAuthOpen || node.dataset.cwMetaOpen || `add:${node.dataset.cwAuthEmptyAdd}`,
          patchCard);
      });
  }

  function fanartNudgeHtml(cfg) {
    if (settingsScope() || !metadataConfigured("TMDB_METADATA", cfg)) return "";
    if (cfg?.fanart?.enabled === true || cfg?.fanart?.nudge_dismissed === true) return "";
    return `<div class="cw-auth-nudge">
        <button type="button" class="cw-auth-service-card cw-auth-add-card" data-cw-meta-open="FANART_METADATA">
          <span class="cw-auth-add-mark"><span class="material-symbols-rounded" aria-hidden="true">auto_awesome</span></span>
          <span class="cw-auth-service-copy">
            <strong>${TH("connections.fanartNudge.title")}</strong>
            <small>${TH("connections.fanartNudge.copy")}</small>
          </span>
          <span class="material-symbols-rounded cw-auth-chevron" aria-hidden="true">arrow_forward</span>
        </button>
        <button type="button" class="cw-auth-nudge-dismiss" data-cw-fanart-nudge-dismiss title="${TH("connections.fanartNudge.hide")}" aria-label="${TH("connections.fanartNudge.hide")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
      </div>`;
  }

  async function dismissFanartNudge() {
    try {
      const r = await fetch("/api/fanart/settings", { method: "POST", headers: { "Content-Type": "application/json" }, cache: "no-store", credentials: "same-origin", body: JSON.stringify({ nudge_dismissed: true }) });
      const data = await r.json().catch(() => ({}));
      if (!r.ok || !data?.ok) return;
    } catch {
      return;
    }
    const cfg = getCachedConfig();
    cfg.fanart = { ...(cfg.fanart || {}), nudge_dismissed: true };
    const slot = document.getElementById("auth-providers");
    if (slot) renderAuthCards(slot, cfg);
  }

  function renderAuthCards(slot, cfg = getCachedConfig(), refreshProfiles = false) {
    const { shell } = ensureAuthShell(slot);
    const configured = configuredProviderKeys(cfg);
    const sectionCopy = {
      "sec-auth-media": T("connections.groupHints.media"),
      "sec-auth-trackers": T("connections.groupHints.trackers"),
      "sec-auth-clients": T("connections.groupHints.clients"),
      "sec-auth-metadata": T("connections.groupHints.metadata"),
      "sec-auth-others": T("connections.groupHints.others"),
    };
    const summaryIcon = {
      "sec-auth-media": "dns",
      "sec-auth-trackers": "radar",
      "sec-auth-clients": "tv",
      "sec-auth-metadata": "database",
      "sec-user-profiles": "groups",
    };
    const scope = settingsScope();
    const groupData = AUTH_GROUPS.map((group) => {
      const supported = group.keys.filter((key) => !!authProviderInfo(key).sectionId);
      const visible = supported.filter((key) => configured.has(key) && (!scope || scope.hasProvider(key)));
      const cards = visible.map((key) => {
        const info = authProviderInfo(key);
        const status = authStatusFor(key, configured.has(key), cfg);
        return { type: "provider", key: info.key, provider: String(info.key || "").toLowerCase(), label: info.label, status, logo: authProviderLogo(info.key) };
      });
      const okCount = cards.filter((card) => card.status.ok).length;
      return { ...group, supported, cards, okCount, total: scope ? cards.length : supported.length, copy: sectionCopy[group.id] || "" };
    }).filter((group) => (group.id !== "sec-auth-others" && !scope) || group.cards.length);
    const metadataVisible = META_GROUP.keys.filter((key) => metadataConfigured(key, cfg));
    const metadataCards = metadataVisible.map((key) => {
      const info = metadataProviderInfo(key);
      const status = metadataStatusFor(key, cfg);
      return { type: "metadata", key: info.key, label: info.label, status, logo: metadataProviderLogo(info) };
    });
    const sections = [
      ...groupData.filter((group) => group.id !== "sec-auth-others"),
      { ...META_GROUP, cards: metadataCards, okCount: metadataCards.filter((card) => card.status.ok).length, total: META_GROUP.keys.length, copy: sectionCopy[META_GROUP.id] },
      ...groupData.filter((group) => group.id === "sec-auth-others"),
    ];
    const userProfilesRaw = cfg?.user_profiles;
    const userProfileCount = userProfilesRaw && typeof userProfilesRaw === "object" ? Object.keys(userProfilesRaw).length : 0;
    const summarySections = [
      ...sections.filter((section) => section.id !== "sec-auth-others"),
      { id: "sec-user-profiles", title: T("connections.groups.profiles"), okCount: userProfileCount, total: userProfileCount || 0 },
    ];
    const summaryCards = summarySections.map((section) => `<div class="cw-auth-summary-card ${section.okCount ? "" : "is-empty"}" data-cw-auth-summary="${section.id}">
      <span class="cw-auth-summary-icon material-symbols-rounded" aria-hidden="true">${summaryIcon[section.id] || "hub"}</span>
      <span><strong>${section.title}</strong><small>${section.id === "sec-user-profiles" ? `${section.okCount} profile${section.okCount === 1 ? "" : "s"}` : `${section.okCount}/${section.total}`}</small></span>
    </div>`).join("");
    const renderCard = (card) => {
      const attr = card.type === "metadata" ? `data-cw-meta-open="${card.key}"` : `data-cw-auth-open="${card.key}"`;
      const summary = authProfileSummary(card, cfg);
      const detail = summary.detail ? `<span class="cw-auth-profile-summary"> · ${escHtml(summary.detail)}</span>` : "";
      const meta = card.type === "provider" && typeof providerMeta().get === "function" ? providerMeta().get(card.key) : null;
      const scrobbleOnly = meta?.scrobbleOnly === true
        ? `<span class="cw-auth-scrobble-only-badge material-symbols-rounded" title="${TH("connections.scrobbleOnly")}" aria-label="${TH("connections.scrobbleOnly")}">sync</span>`
        : "";
      return `<button type="button" class="cw-auth-service-card" ${attr}>
        ${scrobbleOnly}
        <span class="cw-auth-provider-mark">${card.logo}</span>
        <span class="cw-auth-service-copy">
          <strong>${card.label}</strong>
          <small title="${escHtml(summary.title)}"><span class="cw-auth-status-dot ${summary.dot}"></span><span class="cw-auth-status-text">${escHtml(summary.text)}${detail}</span></small>
        </span>
        <span class="material-symbols-rounded cw-auth-chevron" aria-hidden="true">chevron_right</span>
      </button>`;
    };
    const renderAddCard = (section) => {
      const mode = section.id === META_GROUP.id ? "metadata" : "provider";
      const label = TH(section.id === META_GROUP.id ? "settings.providers.addMetadata" : section.id === "sec-auth-media" ? "connections.add.mediaServer" : section.id === "sec-auth-clients" ? "connections.add.mediaClient" : section.id === "sec-auth-others" ? "settings.providers.addProvider" : "connections.add.tracker");
      const copy = TH(section.id === META_GROUP.id ? "connections.add.anotherMetadata" : "connections.add.anotherService");
      return `<button type="button" class="cw-auth-service-card cw-auth-add-card" data-cw-auth-empty-add="${mode}" data-cw-auth-category="${section.id}">
        <span class="cw-auth-add-mark"><span class="material-symbols-rounded" aria-hidden="true">add</span></span>
        <span class="cw-auth-service-copy">
          <strong>${label}</strong>
          <small>${copy}</small>
        </span>
        <span class="material-symbols-rounded cw-auth-chevron" aria-hidden="true">arrow_forward</span>
      </button>`;
    };
    const userProfileSection = `<section class="cw-auth-service-section cw-user-profile-section" data-cw-auth-group="sec-user-profiles">
        <div class="cw-auth-service-head">
          <h4>${TH("connections.groups.profiles")}</h4>
          <p>${TH("connections.profilesCopy")}</p>
          <span class="material-symbols-rounded cw-auth-section-chevron" aria-hidden="true">expand_more</span>
        </div>
        <div id="cw-user-profile-manager" class="cw-user-profile-manager" aria-live="polite"></div>
      </section>`;
    const serviceSections = sections.map((section) => {
      const addCard = section.cards.length < section.total ? renderAddCard(section) : "";
      const nudge = section.id === META_GROUP.id ? fanartNudgeHtml(cfg) : "";
      const cards = `${[...section.cards].sort((a, b) => String(a.label || "").localeCompare(String(b.label || ""), undefined, { sensitivity: "base", numeric: true })).map(renderCard).join("")}${nudge}${addCard}`;
      return `<section class="cw-auth-service-section" data-cw-auth-group="${section.id}">
        <div class="cw-auth-service-head">
          <h4>${section.title}</h4>
          <p>${section.copy}</p>
          <span class="material-symbols-rounded cw-auth-section-chevron" aria-hidden="true">expand_more</span>
        </div>
        <div class="cw-auth-service-grid">${cards}</div>
      </section>`;
    }).join("");
    const dashboardHtml = `
      <div class="cw-auth-dashboard">
        <div class="cw-auth-summary-row">
          ${summaryCards}
        </div>
        <div class="cw-auth-service-list">${serviceSections}${userProfileSection}</div>
      </div>`;
    if (shell.__cwAuthCardsHtml !== dashboardHtml) {
      updateAuthDashboard(shell, dashboardHtml);
      shell.__cwAuthCardsHtml = dashboardHtml;
    }
    const profileHost = shell.querySelector("#cw-user-profile-manager");
    try { window.cwUserProfilesManager?.init?.(refreshProfiles || !profileHost?.childElementCount); } catch {}
    const overlay = document.getElementById("cw-auth-connection-overlay");
    renderAuthPicker(overlay, cfg, overlay?.dataset?.cwPickerMode || "provider");
  }

  function openAuthOverlay(mode = "picker", key = "", pickerMode = "provider", category = "all") {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    const overlay = ensureAuthOverlay(slot);
    const picker = overlay.querySelector("#cw-auth-provider-picker");
    const form = overlay.querySelector("#cw-auth-provider-form");
    const title = overlay.querySelector("#cw-auth-dialog-title");
    const kicker = overlay.querySelector("#cw-auth-dialog-kicker");
    if (overlay.classList.contains("hidden")) {
      overlay.__cwReturnFocus = document.activeElement;
      window.CW?.DialogFocus?.activate(overlay.querySelector(".cw-auth-dialog"), closeAuthProviderOverlay);
    }
    overlay.classList.remove("hidden");
    overlay.classList.toggle("cw-auth-picker-open", mode !== "form");
    overlay.setAttribute("aria-hidden", "false");
    if (mode === "form" && key) {
      picker?.classList.add("hidden");
      form?.classList.remove("hidden");
      const info = authProviderInfo(key);
      if (title) title.textContent = info.label;
      if (kicker) kicker.textContent = T("dashboard.status.connectionSettings");
      requestAnimationFrame(() => overlay.querySelector("[data-cw-auth-close]")?.focus?.());
      return;
    }
    parkActiveAuthForm();
    overlay.dataset.cwPickerMode = pickerMode;
    overlay.dataset.cwPickerCategory = category;
    overlay.dataset.cwPickerSearch = "";
    renderAuthPicker(overlay, getCachedConfig(), pickerMode);
    picker?.classList.remove("hidden");
    form?.classList.add("hidden");
    if (title) title.textContent = pickerMode === "metadata" ? T("settings.providers.addMetadata") : T("settings.providers.addProvider");
    if (kicker) kicker.textContent = pickerMode === "metadata" ? T("connections.groups.metadata") : T("settings.sync.providers");
    const subtitle = overlay.querySelector("#cw-auth-dialog-subtitle");
    const logo = overlay.querySelector(".cw-auth-dialog-logo");
    if (subtitle) subtitle.textContent = T("connections.dialog.chooseService");
    logo?.classList.add("hidden");
    picker.querySelector(".cw-auth-picker-results").scrollTop = 0;
    requestAnimationFrame(() => {
      const target = window.matchMedia("(max-width: 600px)").matches ? overlay.querySelector('[aria-selected="true"]') : overlay.querySelector("[data-cw-picker-search]");
      target?.focus({preventScroll:true});
      overlay.querySelector('[aria-selected="true"]')?.scrollIntoView({block:"nearest", inline:"nearest"});
    });
  }

  const CONNECTION_MODAL_INFO = Object.freeze({
    PLEX: {
      provider: "plex", logo: "PLEX", size: "wide", help: window.CW.HelpLinks.url("plex"), deleteSelector: "#btn-delete-plex",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.plex.connectYourPlexAccountUsingA")], settings: ["tune", T("connections.modal.shared.settings"), T("connections.modal.plex.configureServerAndSyncPreferences")], whitelist: ["verified_user", T("connections.modal.shared.whitelisting"), T("connections.modal.shared.chooseLibrariesAndContentToSync")], users: ["group", T("connections.modal.shared.identities"), T("connections.modal.shared.optionalWhoCanBeSynced")] },
      copy: { auth: [T("connections.modal.plex.plexAuthentication"), T("connections.modal.plex.connectYourPlexAccountToCrosswatch")], settings: [T("connections.modal.plex.plexSettings"), T("connections.modal.plex.configureYourServerUrlSelectedProfile")], whitelist: [T("connections.modal.plex.plexWhitelisting"), T("connections.modal.shared.chooseWhichLibrariesCrosswatchCanUse")] },
      journey: [T("connections.modal.plex.linkPlexThenTuneTheServer"), T("connections.modal.plex.clickConnectPlexToGetA"), "229,160,13", "229,160,13", "PLEX"],
      steps: [["1", T("connections.modal.plex.getLinkCode"), T("connections.modal.plex.generateACodeInCrosswatch")], ["2", T("connections.modal.plex.approveOnPlex"), T("connections.modal.plex.openPlexTvLinkAndApprove")], ["3", T("connections.modal.shared.validateServer"), T("connections.modal.plex.confirmYourServerUrlAndComplete")]],
      order: ["#plex_qc_state", ".cw-subpanel[data-sub=auth]>.inline"]
    },
    JELLYFIN: {
      provider: "jellyfin", logo: "JELLYFIN", size: "wide", help: window.CW.HelpLinks.url("jellyfin"), deleteSelector: ".cw-jfy-delete",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.jellyfin.connectWithQuickConnectOrPassword")], settings: ["tune", T("connections.modal.shared.settings"), T("connections.modal.shared.configureServerAndUserDetails")], whitelist: ["verified_user", T("connections.modal.shared.whitelisting"), T("connections.modal.shared.chooseLibrariesAndContentToSync")], users: ["group", T("connections.modal.shared.identities"), T("connections.modal.shared.optionalWhoCanBeSynced")] },
      copy: { auth: [T("connections.modal.jellyfin.jellyfinAuthentication"), T("connections.modal.jellyfin.connectYourJellyfinServerToCrosswatch")], settings: [T("connections.modal.jellyfin.jellyfinSettings"), T("connections.modal.shared.configureServerUrlUserDetailsAnd")], whitelist: [T("connections.modal.jellyfin.jellyfinWhitelisting"), T("connections.modal.shared.chooseWhichLibrariesCrosswatchCanUse2")] },
      journey: [T("connections.modal.jellyfin.connectToJellyfin"), T("connections.modal.jellyfin.enterYourJellyfinServerUrlThen"), "0,164,220", "0,124,220", "JELLYFIN"],
      steps: [["1", T("connections.modal.shared.enterServer"), T("connections.modal.jellyfin.addYourJellyfinServerUrl")], ["2", T("connections.modal.jellyfin.authorize"), T("connections.modal.jellyfin.useQuickConnectOrPassword")], ["3", T("connections.modal.shared.validate"), T("connections.modal.shared.confirmTheAccountAndServer")]],
      order: [".grid2", ".jfy-methods", ".jfy-pane[data-method=quick]", ".jfy-pane[data-method=password]", ".jfy-actions-row"]
    },
    EMBY: {
      provider: "emby", logo: "EMBY", size: "wide", help: window.CW.HelpLinks.url("emby"), deleteSelector: "#btn-emby-delete",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.emby.connectWithYourEmbyCredentials")], settings: ["tune", T("connections.modal.shared.settings"), T("connections.modal.shared.configureServerAndUserDetails")], whitelist: ["verified_user", T("connections.modal.shared.whitelisting"), T("connections.modal.shared.chooseLibrariesAndContentToSync")], users: ["group", T("connections.modal.shared.identities"), T("connections.modal.shared.optionalWhoCanBeSynced")] },
      copy: { auth: [T("connections.modal.emby.embyAuthentication"), T("connections.modal.emby.connectYourEmbyServerToCrosswatch")], settings: [T("connections.modal.emby.embySettings"), T("connections.modal.shared.configureServerUrlUserDetailsAnd")], whitelist: [T("connections.modal.emby.embyWhitelisting"), T("connections.modal.shared.chooseWhichLibrariesCrosswatchCanUse2")] },
      journey: [T("connections.modal.emby.connectToEmby"), T("connections.modal.emby.enterYourEmbyServerUrlUsername"), "82,181,75", "82,181,75", "EMBY"],
      steps: [["1", T("connections.modal.shared.enterServer"), T("connections.modal.emby.addYourEmbyServerUrl")], ["2", T("connections.modal.shared.signIn"), T("connections.modal.emby.authorizeWithUserCredentials")], ["3", T("connections.modal.shared.validate"), T("connections.modal.shared.confirmTheAccountAndServer")]],
      order: [".grid2", ".inline"]
    },
    SILO: {
      provider: "silo", logo: "SILO", help: window.CW.HelpLinks.url("silo"), deleteSelector: "#silo_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.connectWithADeviceCode")], whitelist: ["verified_user", T("connections.modal.shared.whitelisting"), T("connections.modal.shared.chooseLibrariesAndContentToSync")], users: ["group", T("connections.modal.shared.identities"), T("connections.modal.shared.optionalWhichHouseholdProfilesCanBe")] },
      copy: { auth: [T("connections.modal.silo.siloAuthentication"), T("connections.modal.silo.signInToYourSiloServer")], whitelist: [T("connections.modal.silo.siloWhitelisting"), T("connections.modal.shared.chooseWhichLibrariesCrosswatchCanUse")], users: [T("connections.modal.silo.siloUsers"), T("connections.modal.shared.tickOtherHouseholdProfilesToSync")] },
      journey: [T("connections.modal.silo.connectToSilo"), T("connections.modal.silo.enterYourSiloServerUrlApprove"), "20,135,254", "245,11,79", "SILO"],
      steps: [["1", T("connections.modal.shared.enterServer"), T("connections.modal.silo.addYourSiloServerUrl")], ["2", T("connections.modal.shared.approveCode"), T("connections.modal.silo.approveTheCodeWhileSignedIn")], ["3", T("connections.modal.shared.selectProfile"), T("connections.modal.shared.chooseTheHouseholdProfileForThis")]],
      order: [".silo-server-row", ".silo-actions", "#silo_login_state", "#silo_profile_state"],
      code: ["#silo_login_state"],
      actions: [{ row: ".silo-actions", status: "#silo_msg", buttons: "#silo_connect, #silo_cancel" }]
    },
    TOFA: {
      provider: "tofa", logo: "TOFA", help: window.CW.HelpLinks.url("tofa"), deleteSelector: "#tofa_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.connectWithADeviceCode")], whitelist: ["verified_user", T("connections.modal.shared.whitelisting"), T("connections.modal.shared.chooseLibrariesAndContentToSync")], users: ["group", T("connections.modal.shared.identities"), T("connections.modal.shared.optionalWhichHouseholdProfilesCanBe")] },
      copy: { auth: [T("connections.modal.tofa.tofaAuthentication"), T("connections.modal.tofa.signInWithYourTofaAccount")], whitelist: [T("connections.modal.tofa.tofaWhitelisting"), T("connections.modal.tofa.chooseWhichLibrariesCrosswatchCanUse")], users: [T("connections.modal.tofa.tofaUsers"), T("connections.modal.shared.tickOtherHouseholdProfilesToSync")] },
      journey: [T("connections.modal.tofa.connectToTofa"), T("connections.modal.tofa.enterYourTofaServerUrlApprove"), "8,214,184", "0,152,148", "TOFA"],
      steps: [["1", T("connections.modal.shared.enterServer"), T("connections.modal.tofa.addYourTofaServerUrl")], ["2", T("connections.modal.shared.approveCode"), T("connections.modal.tofa.approveTheCodeWithYourTofa")], ["3", T("connections.modal.shared.selectProfile"), T("connections.modal.shared.chooseTheHouseholdProfileForThis")]],
      order: [".tofa-server-row", ".tofa-actions", "#tofa_login_state", "#tofa_profile_state"],
      code: ["#tofa_login_state"],
      actions: [{ row: ".tofa-actions", status: "#tofa_msg", buttons: "#tofa_connect, #tofa_cancel" }]
    },
    TRAKT: {
      provider: "trakt", logo: "TRAKT", help: window.CW.HelpLinks.url("trakt"), deleteSelector: "#btn-delete-trakt",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.trakt.connectYourTraktAccount")] },
      copy: { auth: [T("connections.modal.trakt.traktAuthentication"), T("connections.modal.trakt.connectTraktWithAPinCode")] },
      journey: [T("connections.modal.trakt.connectToTrakt"), T("connections.modal.trakt.connectWithAPinCodeRecommended"), "225,20,60", "159,66,198", "TRAKT"],
      steps: [["1", T("connections.modal.shared.chooseMethod"), T("connections.modal.trakt.usePinFlowOrYourOwn")], ["2", T("connections.modal.shared.approveCode"), T("connections.modal.trakt.openAuthTraktTvActivateAnd")], ["3", T("connections.modal.trakt.syncAccount"), T("connections.modal.shared.crosswatchStoresTheApprovedToken")]],
      order: [".trk-method-row", "#trakt_app_panel", "#trakt_pin_panel", ".cw-connection-method-action-row", ".inline"],
      code: ["#trakt_pin_panel"],
      actions: [{ row: ".trk-method-row", status: "#trakt_msg", buttons: "#btn-connect-trakt, #btn-trakt-cancel, #btn-trakt-restart", extract: ".trk-actions", order: "6" }]
    },
    SIMKL: {
      provider: "simkl", logo: "SIMKL", help: window.CW.HelpLinks.url("simkl"), deleteSelector: "#btn-delete-simkl, #btn-delete-simkl-oauth",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.simkl.connectWithPinOrOauth")] },
      copy: { auth: [T("connections.modal.simkl.simklAuthentication"), T("connections.modal.simkl.connectSimklWithAPinCode")] },
      journey: [T("connections.modal.simkl.connectToSimkl"), T("connections.modal.simkl.connectWithAPinCodeRecommended"), "218,225,232", "12,12,12", "SIMKL"],
      steps: [["1", T("connections.modal.shared.chooseMethod"), T("connections.modal.simkl.usePinFlowOrYourOauth")], ["2", T("connections.modal.simkl.approveSimkl"), T("connections.modal.simkl.enterThePinCodeOrAuthorize")], ["3", T("connections.modal.simkl.startSyncing"), T("connections.modal.simkl.crosswatchStoresTheApprovedAccess")]],
      order: [".smk-method-row", "#simkl_oauth_panel", "#simkl_pin_panel", ".cw-connection-method-action-row", ".inline"],
      code: ["#simkl_pin_panel"],
      actions: [{ row: ".smk-method-row", status: "#simkl_msg", buttons: "#btn-connect-simkl-pin, #btn-simkl-pin-cancel, #btn-simkl-pin-restart, #btn-connect-simkl", extract: ".smk-actions", order: "6" }]
    },
    TMDB: {
      provider: "tmdb", logo: "TMDB", help: window.CW.HelpLinks.url("tmdb"), deleteSelector: "#tmdb_sync_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.tmdb.connectTmdbSync")] },
      copy: { auth: [T("connections.modal.tmdb.tmdbAuthentication"), T("connections.modal.tmdb.connectTmdbSyncWithAV3")] },
      journey: [T("connections.modal.tmdb.connectToTmdb"), T("connections.modal.tmdb.addYourTmdbV3ApiKey"), "0,179,229", "144,206,161", "TMDB"],
      steps: [["1", T("connections.modal.shared.addApiKey"), T("connections.modal.tmdb.enterYourTmdbV3Key")], ["2", T("connections.modal.tmdb.approveSession"), T("connections.modal.tmdb.authorizeCrosswatchOnTmdb")], ["3", T("connections.modal.tmdb.validateSync"), T("connections.modal.tmdb.confirmTheSessionIsReady")]],
      order: [".grid2"],
      actions: [{ row: ".inline", status: "#tmdb_sync_msg", buttons: "#tmdb_sync_connect", wrapParent: true, parentClass: "cw-tmdb-sync-action-wrap", order: "4" }]
    },
    MDBLIST: {
      provider: "mdblist", logo: "MDBLIST", help: window.CW.HelpLinks.url("mdblist"), deleteSelector: "#mdblist_disconnect_device, #mdblist_disconnect_api",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.connectWithDeviceCodeOrApi")] },
      copy: { auth: [T("connections.modal.mdblist.mdblistAuthentication"), T("connections.modal.mdblist.connectMdblistWithDeviceCodeOr")] },
      journey: [T("connections.modal.mdblist.connectToMdblist"), T("connections.modal.mdblist.connectWithADeviceCodeRecommended"), "64,132,200", "64,132,200", "MDBLIST"],
      steps: [["1", T("connections.modal.shared.chooseMethod"), T("connections.modal.shared.useDeviceCodeOrApiKey")], ["2", T("connections.modal.shared.approveAccess"), T("connections.modal.shared.authorizeTheDeviceCodeOrSave")], ["3", T("connections.modal.shared.validateAccount"), T("connections.modal.mdblist.crosswatchConfirmsMdblistAccess")]],
      order: [".mdbl-method-row", "#mdblist_api_panel", "#mdblist_device_panel", ".cw-connection-method-action-row", ".inline"],
      code: ["#mdblist_device_panel"],
      actions: [{ row: ".mdbl-method-row", status: "#mdblist_msg", buttons: "#mdblist_device_start, #mdblist_device_cancel, #mdblist_device_restart, #mdblist_save", extract: ".mdbl-actions", order: "6" }]
    },
    SCROB: {
      provider: "scrob", logo: "SCROB", help: window.CW.HelpLinks.url("scrob"), deleteSelector: "#scrob_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.scrob.connectYourScrobServer")] },
      copy: { auth: [T("connections.modal.scrob.scrobAuthentication"), T("connections.modal.scrob.connectYourSelfHostedScrobServer")] },
      journey: [T("connections.modal.scrob.connectToScrob"), T("connections.modal.scrob.copyTheApiKeyFromScrob"), "111,54,204", "174,69,200", "SCROB"],
      steps: [["1", T("connections.modal.scrob.enterServerUrl"), T("connections.modal.scrob.theAddressYouOpenScrobWith")], ["2", T("connections.modal.scrob.pasteTheApiKey"), T("connections.modal.scrob.scrobConnectionsApiKey")], ["3", T("connections.modal.shared.signIn"), T("connections.modal.scrob.scrobOnlyAcceptsWritesFromA")]],
      order: [".grid2", "#scrob_totp_row", "#scrob_reauth", ".verify", "#scrob_actions_row"],
      actions: [{ row: "#scrob_actions_row", status: "#scrob_msg", buttons: "#scrob_connect" }]
    },
    MYANIMELIST: {
      provider: "myanimelist", logo: "MYANIMELIST", help: window.CW.HelpLinks.url("myanimelist"), deleteSelector: "#myanimelist_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.approveInYourBrowser")] },
      copy: { auth: [T("connections.modal.myanimelist.myanimelistAuthentication"), T("connections.modal.myanimelist.connectYourMyanimelistAccount")] },
      journey: [T("connections.modal.myanimelist.connectToMyanimelist"), T("connections.modal.myanimelist.approveCrosswatchInMyanimelistYourConnection"), "47,82,162", "47,82,162", "MYANIMELIST"],
      steps: [["1", T("connections.modal.myanimelist.openMyanimelist"), T("connections.modal.shared.signInWithYourAccount")], ["2", T("connections.modal.shared.approveAccess"), T("connections.modal.myanimelist.allowCrosswatchToConnect")], ["3", T("connections.modal.myanimelist.returnToCrosswatch"), T("connections.modal.myanimelist.yourAccountIsVerifiedAutomatically")]],
      order: ["#myanimelist_oauth_panel", ".mal-actions"],
      actions: [{ row: ".mal-actions", status: "#myanimelist_msg", buttons: "#myanimelist_oauth_start, #myanimelist_oauth_cancel" }]
    },
    WETRAKR: {
      provider: "wetrakr", logo: "WETRAKR", help: window.CW.HelpLinks.url("wetrakr"), deleteSelector: "#wetrakr_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.approveInYourBrowser")] },
      copy: { auth: [T("connections.modal.wetrakr.wetrakrAuthentication"), T("connections.modal.wetrakr.connectYourAccountAndViewYour")] },
      journey: [T("connections.modal.wetrakr.connectToWetrakr"), T("connections.modal.wetrakr.approveCrosswatchInWetrakrThenPaste"), "255,255,255", "255,255,255", "WETRAKR"],
      steps: [["1", T("connections.modal.wetrakr.openWetrakr"), T("connections.modal.shared.signInWithYourAccount")], ["2", T("connections.modal.shared.approveAccess"), T("connections.modal.wetrakr.copyTheCodeWetrakrDisplays")], ["3", T("connections.modal.wetrakr.pasteYourCode"), T("connections.modal.wetrakr.completeTheConnectionInCrosswatch")]],
      order: ["#wetrakr_oauth_panel", ".wt-actions", "#wetrakr_account"],
      actions: [{ row: ".wt-actions", status: "#wetrakr_msg", buttons: "#wetrakr_oauth_start, #wetrakr_oauth_finish, #wetrakr_oauth_cancel" }]
    },
    PUNCHPLAY: {
      provider: "punchplay", logo: "PUNCHPLAY", help: window.CW.HelpLinks.url("punchplay"), deleteSelector: "#punchplay_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.connectWithADeviceCode")] },
      copy: { auth: [T("connections.modal.punchplay.punchplayAuthentication"), T("connections.modal.punchplay.connectPunchplayWithADeviceCode")] },
      journey: [T("connections.modal.punchplay.connectToPunchplay"), T("connections.modal.punchplay.clickConnectPunchplayAndApproveThe"), "244,64,15", "0,132,216", "PUNCHPLAY"],
      steps: [["1", T("connections.modal.shared.startDeviceLogin"), T("connections.modal.shared.crosswatchShowsAShortLinkCode")], ["2", T("connections.modal.shared.approveAccess"), T("connections.modal.punchplay.enterTheCodeAtPunchplayTv")], ["3", T("connections.modal.shared.validateAccount"), T("connections.modal.punchplay.crosswatchConfirmsPunchplayAccess")]],
      order: ["#punchplay_device_panel", ".pp-actions"],
      code: ["#punchplay_device_panel"],
      actions: [{ row: ".pp-actions", status: "#punchplay_msg", buttons: "#punchplay_device_start, #punchplay_device_cancel, #punchplay_device_restart" }]
    },
    MOVIEPARADISE: {
      provider: "movieparadise", logo: "MOVIEPARADISE", help: window.CW.HelpLinks.url("movieparadise"), deleteSelector: "#movieparadise_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.connectWithADeviceCode")] },
      copy: { auth: [T("connections.modal.movieparadise.movieParadiseAuthentication"), T("connections.modal.movieparadise.connectMovieParadiseWithADevice")] },
      journey: [T("connections.modal.movieparadise.connectToMovieParadise"), T("connections.modal.movieparadise.clickConnectMovieParadiseAndApprove"), "215,98,143", "139,63,161", "MOVIEPARADISE"],
      steps: [["1", T("connections.modal.shared.startDeviceLogin"), T("connections.modal.shared.crosswatchShowsAShortLinkCode")], ["2", T("connections.modal.shared.approveAccess"), T("connections.modal.movieparadise.enterTheCodeAtMovieparadiseApp")], ["3", T("connections.modal.shared.validateAccount"), T("connections.modal.movieparadise.crosswatchConfirmsMovieParadiseAccess")]],
      order: ["#movieparadise_device_panel", ".mp-actions"],
      code: ["#movieparadise_device_panel"],
      actions: [{ row: ".mp-actions", status: "#movieparadise_msg", buttons: "#movieparadise_device_start, #movieparadise_device_cancel, #movieparadise_device_restart" }]
    },
    BINGEBASE: {
      provider: "bingebase", logo: "BINGEBASE", help: window.CW.HelpLinks.url("bingebase"), deleteSelector: "#bingebase_disconnect", allowSaveWithoutConnection: true,
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.connectWithADeviceCode")] },
      copy: { auth: [T("connections.modal.bingebase.bingebaseAuthentication"), T("connections.modal.bingebase.connectBingebaseWithADeviceCode")] },
      journey: [T("connections.modal.bingebase.connectToBingebase"), T("connections.modal.bingebase.clickConnectBingebaseAndApproveThe"), "255,133,51", "248,241,232", "BINGEBASE"],
      steps: [["1", T("connections.modal.shared.startDeviceLogin"), T("connections.modal.shared.crosswatchShowsAShortLinkCode")], ["2", T("connections.modal.shared.approveAccess"), T("connections.modal.bingebase.enterTheCodeAtBingebaseCom")], ["3", T("connections.modal.bingebase.realtime"), T("connections.modal.bingebase.useTheAutoFilledKodiWebhook")]],
      order: ["#bingebase_realtime_panel", ".bb-actions", "#bingebase_device_panel"],
      code: ["#bingebase_device_panel"],
      actions: [{ row: ".bb-actions", status: "#bingebase_msg", buttons: "#bingebase_device_start, #bingebase_device_cancel, #bingebase_device_restart" }]
    },
    FLICKLIST: {
      provider: "flicklist", logo: "FLICKLIST", help: window.CW.HelpLinks.url("flicklist"), deleteSelector: "#flicklist_disconnect_device, #flicklist_disconnect_api",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.shared.connectWithDeviceCodeOrApi")] },
      copy: { auth: [T("connections.modal.flicklist.flicklistAuthentication"), T("connections.modal.flicklist.connectFlicklistWithDeviceCodeUsing")] },
      journey: [T("connections.modal.flicklist.connectToFlicklist"), T("connections.modal.flicklist.approveCrosswatchAtFlicklistTvLink"), "8,80,184", "88,208,248", "FLICKLIST"],
      steps: [["1", T("connections.modal.shared.chooseMethod"), T("connections.modal.shared.useDeviceCodeOrApiKey")], ["2", T("connections.modal.shared.approveAccess"), T("connections.modal.shared.authorizeTheDeviceCodeOrSave")], ["3", T("connections.modal.shared.validateAccount"), T("connections.modal.flicklist.crosswatchConfirmsFlicklistAccess")]],
      order: [".fl-method-row", "#flicklist_api_panel", "#flicklist_device_panel", ".cw-connection-method-action-row", ".inline"],
      code: ["#flicklist_qc_state"],
      actions: [{ row: ".fl-method-row", status: "#flicklist_msg", buttons: "#flicklist_device_start, #flicklist_device_cancel, #flicklist_save", extract: ".fl-actions", order: "6" }]
    },
    PUBLICMETADB: {
      provider: "publicmetadb", logo: "PUBLICMETADB", help: window.CW.HelpLinks.url("publicmetadb"), deleteSelector: "#publicmetadb_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.publicmetadb.connectYourPublicmetadbApiKey")] },
      copy: { auth: [T("connections.modal.publicmetadb.publicmetadbAuthentication"), T("connections.modal.publicmetadb.connectPublicmetadbWithAnApiKey")] },
      journey: [T("connections.modal.publicmetadb.connectToPublicmetadb"), T("connections.modal.publicmetadb.pasteYourPublicmetadbApiKeyAnd"), "155,155,155", "155,155,155", "PUBLICMETADB"],
      steps: [["1", T("connections.modal.publicmetadb.createApiKey"), T("connections.modal.publicmetadb.generateAKeyInPublicmetadb")], ["2", T("connections.modal.publicmetadb.connectKey"), T("connections.modal.publicmetadb.pasteTheKeyAndConnect")], ["3", T("connections.modal.shared.validateAccess"), T("connections.modal.publicmetadb.crosswatchConfirmsTheKeyWorks")]],
      order: [".grid2", "#publicmetadb_hint", ".publicmetadb-actions"],
      actions: [{ row: ".publicmetadb-actions", status: "#publicmetadb_msg", buttons: "#publicmetadb_save" }]
    },
    FLOPPY: {
      provider: "floppy", logo: "FLOPPY", help: window.CW.HelpLinks.url("floppy"), deleteSelector: "#floppy_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.floppy.connectYourFloppyServer")] },
      copy: { auth: [T("connections.modal.floppy.floppyAuthentication"), T("connections.modal.floppy.connectFloppyWithServerUrlAnd")] },
    journey: [T("connections.modal.floppy.connectToFloppy"), T("connections.modal.floppy.createAnApiTokenInFloppy"), "245,101,30", "4,181,220", "FLOPPY"],
      steps: [["1", T("connections.modal.floppy.createToken"), T("connections.modal.floppy.generateATokenInFloppy")], ["2", T("connections.modal.shared.enterServer"), T("connections.modal.floppy.addYourFloppyServerUrl")], ["3", T("connections.modal.shared.validateAccess"), T("connections.modal.floppy.crosswatchConfirmsTheTokenWorks")]],
      order: [".grid2", ".verify", "#floppy_actions_row"],
      actions: [{ row: "#floppy_actions_row", status: "#floppy_msg", buttons: "#floppy_connect" }]
    },
    NUVIO: {
      provider: "nuvio", logo: "NUVIO", help: window.CW.HelpLinks.url("nuvio"), deleteSelector: "#nuvio_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.nuvio.connectWithTvLogin")] },
      copy: { auth: [T("connections.modal.nuvio.nuvioAuthentication"), T("connections.modal.nuvio.connectNuvioWithTvLoginAnd")] },
      journey: [T("connections.modal.nuvio.connectToNuvio"), T("connections.modal.nuvio.useNuvioTvLoginApproveThe"), "176,72,240", "64,208,232", "NUVIO"],
      steps: [["1", T("connections.modal.nuvio.startLogin"), T("connections.modal.nuvio.openTheNuvioApprovalUrl")], ["2", T("connections.modal.shared.approveCode"), T("connections.modal.nuvio.approveTheTemporaryTvLoginCode")], ["3", T("connections.modal.shared.selectProfile"), T("connections.modal.nuvio.chooseTheNuvioProfileForThis")]],
      order: [".nuvio-server-row", ".nuvio-actions", "#nuvio_profile_state", "#nuvio_login_state"],
      code: ["#nuvio_login_state"],
      actions: [{ row: ".nuvio-actions", status: "#nuvio_msg", buttons: "#nuvio_connect" }]
    },
    KODI: {
      provider: "kodi", logo: "KODI", help: window.CW.HelpLinks.url("kodi"), deleteSelector: "#kodi_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.kodi.connectOverHttpJsonRpc")], whitelist: ["verified_user", T("connections.modal.shared.whitelisting"), T("connections.modal.shared.chooseLibrariesAndContentToSync")] },
      copy: { auth: [T("connections.modal.kodi.kodiAuthentication"), T("connections.modal.kodi.connectAKodiMediaClientOver")], whitelist: [T("connections.modal.kodi.kodiWhitelisting"), T("connections.modal.kodi.chooseWhichKodiVideoSourcesCrosswatch")] },
      journey: [T("connections.modal.kodi.connectToKodi"), T("connections.modal.kodi.enterYourKodiServerUrlAnd"), "23,181,209", "20,150,200", "KODI"],
      steps: [["1", T("connections.modal.kodi.enableJsonRpc"), T("connections.modal.kodi.allowControlOfKodiViaHttp")], ["2", T("connections.modal.shared.enterServer"), T("connections.modal.kodi.addTheKodiWebServerUrl")], ["3", T("connections.modal.kodi.verify"), T("connections.modal.kodi.crosswatchChecksKodiAndJsonRpc")]],
      order: [".kodi-method-row", "#kodi_addon_block", ".grid2", ".inline"],
      actions: [{ row: ".inline", status: "#kodi_msg", buttons: "#kodi_connect" }]
    },
    STREMIO: {
      provider: "stremio", logo: "STREMIO", help: window.CW.HelpLinks.url("stremio"), deleteSelector: "#stremio_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.stremio.connectWithStremioAccount")], scrobble: ["podcasts", T("connections.modal.stremio.scrobbling"), T("connections.modal.stremio.installTheCrosswatchAddOn")] },
      copy: { auth: [T("connections.modal.stremio.stremioAuthentication"), T("connections.modal.stremio.connectWithYourStremioAccountCrosswatch")], scrobble: [T("connections.modal.stremio.stremioScrobbling"), T("connections.modal.stremio.letStremioReportPlaybackToCrosswatch")] },
      journey: [T("connections.modal.stremio.connectToStremio"), T("connections.modal.stremio.enterYourStremioEmailAndPassword"), "114,44,254", "22,182,255", "STREMIO"],
      steps: [["1", T("connections.modal.stremio.enterAccount"), T("connections.modal.stremio.useYourStremioEmailAndPassword")], ["2", T("connections.modal.stremio.exchangeKey"), T("connections.modal.stremio.crosswatchRequestsAnAuthKey")], ["3", T("connections.modal.stremio.storeKey"), T("connections.modal.stremio.onlyTheAuthKeyIsSaved")]],
      order: [".grid2", ".inline"],
      actions: [{ row: ".inline", status: "#stremio_msg", buttons: "#stremio_connect" }]
    },
    KITSU: {
      provider: "kitsu", logo: "KITSU", help: window.CW.HelpLinks.url("kitsu"), deleteSelector: "#kitsu_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.kitsu.connectYourKitsuAccount")] },
      copy: { auth: [T("connections.modal.kitsu.kitsuAuthentication"), T("connections.modal.kitsu.syncAnimeWatchlistRatingsAndWatched")] },
      journey: [T("connections.modal.kitsu.connectToKitsu"), T("connections.modal.kitsu.signInOnceThenSaveYour"), "253,117,92", "253,117,92", "KITSU"],
      steps: [["1", T("connections.modal.shared.signIn"), T("connections.modal.kitsu.useYourKitsuEmailOrUsername")], ["2", T("connections.modal.kitsu.saveConnection"), T("connections.modal.kitsu.storeTokensForThisAccount")], ["3", T("connections.modal.shared.syncAnime"), T("connections.modal.kitsu.useAnimeIdMappingAndCustom")]],
      order: [".grid2", ".inline"],
      actions: [{ row: ".inline", status: "#kitsu_msg", buttons: "#kitsu_connect" }]
    },
    ANILIST: {
      provider: "anilist", logo: "ANILIST", help: window.CW.HelpLinks.url("anilist"), deleteSelector: "#btn-delete-anilist",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.anilist.connectYourAnilistAccount")] },
      copy: { auth: [T("connections.modal.anilist.anilistAuthentication"), T("connections.modal.anilist.connectAnilistWithYourApiCredentials")] },
      journey: [T("connections.modal.anilist.connectToAnilist"), T("connections.modal.anilist.addYourAnilistClientIdAnd"), "2,169,255", "2,169,255", "ANILIST"],
      steps: [["1", T("connections.modal.anilist.addApiApp"), T("connections.modal.anilist.enterYourAnilistClientIdAnd")], ["2", T("connections.modal.anilist.approveAnilist"), T("connections.modal.anilist.authorizeCrosswatchInTheBrowser")], ["3", T("connections.modal.shared.syncAnime"), T("connections.modal.shared.crosswatchStoresTheApprovedToken")]],
      order: [".grid2", "#anilist_hint", ".anilist-mapping-rec", ".inline"],
      actions: [{ row: ".inline", status: "#anilist_msg", buttons: "#btn-connect-anilist" }]
    },
    TAUTULLI: {
      provider: "tautulli", logo: "TAUTULLI", help: window.CW.HelpLinks.url("tautulli"), deleteSelector: "#tautulli_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.tautulli.connectYourTautulliServer")] },
      copy: { auth: [T("connections.modal.tautulli.tautulliAuthentication"), T("connections.modal.tautulli.connectTautulliWithServerUrlAnd")] },
      journey: [T("connections.modal.tautulli.connectToTautulli"), T("connections.modal.tautulli.enterYourTautulliServerUrlAnd"), "190,190,190", "84,115,145", "TAUTULLI"],
      steps: [["1", T("connections.modal.shared.enterServer"), T("connections.modal.tautulli.addYourTautulliUrlAndApi")], ["2", T("connections.modal.shared.chooseUser"), T("connections.modal.tautulli.optionallyLimitToOneUserId")], ["3", T("connections.modal.shared.validateServer"), T("connections.modal.tautulli.crosswatchConfirmsTautulliAccess")]],
      order: [".grid2", "#tautulli_hint", "#tautulli_actions_row"],
      actions: [{ row: "#tautulli_actions_row", status: "#tautulli_msg", buttons: "#tautulli_save" }]
    },
    TRACEARR: {
      provider: "tracearr", logo: "TRACEARR", help: window.CW.HelpLinks.url("tracearr"), deleteSelector: "#tracearr_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.tracearr.connectYourTracearrServer")] },
      copy: { auth: [T("connections.modal.tracearr.tracearrAuthentication"), T("connections.modal.tracearr.connectTracearrWithServerUrlAnd")] },
      journey: [T("connections.modal.tracearr.connectToTracearr"), T("connections.modal.tracearr.enterYourTracearrServerUrlAnd"), "24,209,231", "14,143,166", "TRACEARR"],
      steps: [["1", T("connections.modal.shared.enterServer"), T("connections.modal.tracearr.addYourTracearrUrlAndApi")], ["2", T("connections.modal.shared.chooseUser"), T("connections.modal.tracearr.optionallyLimitToOneUser")], ["3", T("connections.modal.shared.validateServer"), T("connections.modal.tracearr.crosswatchConfirmsTracearrAccess")]],
      order: ["#tracearr_hint", ".grid2", "#tracearr_actions_row"],
      actions: [{ row: "#tracearr_actions_row", status: "#tracearr_msg", buttons: "#tracearr_save" }]
    },
    CROSSWATCH: {
      provider: "crosswatch", logo: "CROSSWATCH", help: window.CW.HelpLinks.url("crosswatch"), deleteSelector: "#cw_crosswatch_disconnect",
      tabs: { auth: ["lock", T("connections.modal.shared.authentication"), T("connections.modal.crosswatch.connectLocalTracker")], settings: ["database", T("connections.modal.shared.settings"), T("connections.modal.crosswatch.storageAndRestore")] },
      copy: { auth: [T("connections.modal.crosswatch.crosswatchAuthentication"), T("connections.modal.crosswatch.connectThisLocalTrackerInstance")], settings: [T("connections.modal.crosswatch.crosswatchLocalTracker"), T("connections.modal.crosswatch.configureStorageAndRestoreSnapshotsFor")] },
      journey: [T("connections.modal.crosswatch.connectLocalTracker2"), T("connections.modal.crosswatch.enableTheBuiltInCrosswatchTracker"), "124,92,255", "98,194,255", "CROSSWATCH"],
      steps: [["1", T("connections.modal.crosswatch.chooseInstance"), T("connections.modal.crosswatch.useDefaultOrCreateCwP")], ["2", T("connections.modal.crosswatch.connectLocally"), T("connections.modal.crosswatch.enableTheSelectedInstanceAsA")], ["3", T("connections.modal.crosswatch.syncLocally"), T("connections.modal.crosswatch.useTheInstanceInNormalSync")]],
      introSubs: ["auth"],
      order: [".cw-tracker-auth-card", ".cw-tracker-settings-stack"],
      actions: [{ row: ".cw-tracker-auth-actions", status: "#cw_tracker_auth_msg", buttons: "#cw_crosswatch_connect" }]
    },
    TMDB_METADATA: {
      provider: "tmdb", logo: "TMDB", help: window.CW.HelpLinks.url("tmdb-metadata"), deleteSelector: "#tmdb_delete",
      tabs: { api: ["key", T("connections.modal.shared.apiKey"), T("connections.modal.tmdb_metadata.connectTmdbMetadata")], advanced: ["tune", T("connections.modal.tmdb_metadata.advanced"), T("connections.modal.tmdb_metadata.configureLocaleAndCacheBehavior")] },
      copy: { api: [T("connections.modal.tmdb_metadata.tmdbMetadata"), T("connections.modal.tmdb_metadata.metadataAndImagesFetchedFromTmdb")], advanced: [T("connections.modal.tmdb_metadata.tmdbMetadataSettings"), T("connections.modal.tmdb_metadata.configureMetadataLocaleCacheAndAdvanced")] },
      journey: [T("connections.modal.tmdb_metadata.connectTmdbMetadata2"), T("connections.modal.tmdb_metadata.addATmdbApiKeyTo"), "0,179,229", "144,206,161", "TMDB"],
      steps: [["1", T("connections.modal.shared.addApiKey"), T("connections.modal.tmdb_metadata.enterYourTmdbApiKey")], ["2", T("connections.modal.tmdb_metadata.connectTmdb"), T("connections.modal.tmdb_metadata.validateTheKeyWithCrosswatch")], ["3", T("connections.modal.tmdb_metadata.useMetadata"), T("connections.modal.tmdb_metadata.metadataLookupsBecomeAvailable")]],
      introSubs: ["api", "advanced"],
      order: [".auth-card"],
      labels: { "#tmdb_check": T("connections.modal.tmdb_metadata.connectTmdb") },
      actions: [{ row: ".inline", status: "#tmdb_check_msg", buttons: "#tmdb_check", connectedSelector: "#tmdb_api_key", connectedText: T("connections.modal.tmdb_metadata.connected") }]
    },
    FANART_METADATA: {
      provider: "fanart", logo: "FANART", help: window.CW.HelpLinks.url("fanart-metadata"), deleteSelector: "#fanart_delete",
      tabs: { api: ["key", T("connections.modal.shared.apiKey"), T("connections.modal.fanart_metadata.connectFanartTv")], settings: ["tune", T("connections.modal.shared.settings"), T("connections.modal.fanart_metadata.chooseLogosAndBackgrounds")] },
      copy: { api: [T("connections.modal.fanart_metadata.fanartTvArtwork"), T("connections.modal.fanart_metadata.logosBackgroundsAndDiscArtFrom")], settings: [T("connections.modal.fanart_metadata.fanartTvSettings"), T("connections.modal.fanart_metadata.chooseWhereLogosAndBackgroundsCome")] },
      journey: [T("connections.modal.fanart_metadata.connectFanartTv2"), T("connections.modal.fanart_metadata.useFanartTvLogosBackgroundsAnd"), "167,98,255", "255,120,190", "FANART"],
      steps: [["1", T("connections.modal.fanart_metadata.addKey"), T("connections.modal.fanart_metadata.optionalPersonalFanartTvKey")], ["2", T("connections.modal.fanart_metadata.connectFanartTv"), T("connections.modal.fanart_metadata.validateWithCrosswatch")], ["3", T("connections.modal.fanart_metadata.pickArt"), T("connections.modal.fanart_metadata.chooseLogosAndBackgrounds")]],
      introSubs: ["api", "settings"],
      order: [".auth-card"],
      actions: [{ row: ".inline", status: "#fanart_check_msg", buttons: "#fanart_check" }]
    },
    ANIME_MAPPING: {
      provider: "anime-mapping", logo: "ANILIST", help: window.CW.HelpLinks.url("anime-mapping"), deleteSelector: "",
      hideNav: true,
      tabs: { overview: ["hub", T("connections.modal.anime_mapping.mapping"), T("connections.modal.anime_mapping.manageTheLocalAnimeIdIndex")] },
      copy: { overview: [T("connections.modal.anime_mapping.animeIdMapping"), T("connections.modal.anime_mapping.localAnimeIdIndexForAnilist")] },
      journey: [T("connections.modal.anime_mapping.enableAnimeIdMapping"), T("connections.modal.anime_mapping.useTheLocalMappingDatasetsTo"), "2,169,255", "78,141,255", "ANILIST"],
      steps: [["1", T("connections.modal.anime_mapping.enableMapping"), T("connections.modal.anime_mapping.turnOnTheLocalAnimeIndex")], ["2", T("connections.modal.anime_mapping.updateDatasets"), T("connections.modal.anime_mapping.downloadOrRefreshTheMappingData")], ["3", T("connections.modal.anime_mapping.improveMatches"), T("connections.modal.anime_mapping.useMappingsForAnilistAndSimkl")]],
      introSubs: ["overview"],
      order: [".anime-mapping-summary", ".anime-mapping-status-grid", ".anime-mapping-source", "#anime_mapping_error", ".anime-mapping-actions"]
    },
  });

  function connectionInfoForKey(key) {
    const norm = String(key || "").trim().toUpperCase();
    const info = CONNECTION_MODAL_INFO[norm];
    return info ? { ...info, key: norm } : null;
  }

  window.CW = window.CW || {};
  window.CW.connectionBrand = (key) => {
    const journey = connectionInfoForKey(key)?.journey;
    return journey ? { c1: journey[2], c2: journey[3] || journey[2] } : null;
  };

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
    const data = info?.copy?.[active] || Object.values(info?.copy || {})[0] || [T("connections.modal.fallbackTitle"), T("connections.modal.fallbackCopy")];
    const root = overlay || document.getElementById("cw-auth-connection-overlay");
    const dialogTitle = root?.querySelector("#cw-auth-dialog-title");
    const dialogCopy = root?.querySelector("#cw-auth-dialog-subtitle");
    const dialogKicker = root?.querySelector("#cw-auth-dialog-kicker");
    if (dialogTitle) dialogTitle.textContent = data[0];
    if (dialogCopy) dialogCopy.textContent = data[1];
    if (dialogKicker) dialogKicker.textContent = META_ITEMS[info?.key] ? T("connections.groups.metadata") : T("dashboard.status.connectionSettings");
  }

  function connectionModalSupportsProfiles(info) {
    return !!info && !META_ITEMS[info.key];
  }

  function connectionModalAllowsCelebration(info) {
    return connectionModalSupportsProfiles(info);
  }

  function connectionModalConfigured(info, cfg = getCachedConfig()) {
    if (!info) return false;
    if (META_ITEMS[info.key]) return metadataConfigured(info.key, cfg);
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

  function connectionModalCelebrationTarget(panel) {
    if (!panel) return null;
    const statuses = Array.from(panel.querySelectorAll("#plex_msg, #jfy_msg, #emby_msg, .cw-connection-status-pill"));
    const status = statuses.find((node) => {
      const text = String(node?.textContent || "").trim().toLowerCase();
      return /\bconnected\b/.test(text) && !/\bnot\s+connected\b/.test(text) && !node.classList?.contains("hidden") && node.getAttribute?.("aria-hidden") !== "true" && node.getBoundingClientRect?.().width > 0;
    });
    if (status) return status;
    const actions = Array.from(panel.querySelectorAll(".cw-connection-primary-action.cw-auth-connect-locked, .cw-connection-primary-action[aria-disabled='true'], .cw-connection-primary-action:disabled"));
    return actions.find((node) => !node.classList?.contains("hidden") && node.getAttribute?.("aria-hidden") !== "true" && node.getBoundingClientRect?.().width > 0) || connectionModalStatusTarget(panel);
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

  function connectionModalProfileId(panel) {
    return String(panel?.querySelector(".cw-profile-switcher select")?.value || "default");
  }

  function connectionModalProfileConfigured(panel, info, cfg = getCachedConfig()) {
    if (!connectionModalSupportsProfiles(info)) return connectionModalConfigured(info, cfg);
    const inst = connectionModalProfileId(panel);
    return configuredProfileIds(cfg, info.provider).some((id) => String(id) === inst);
  }

  function connectionModalProfileConnected(panel, info, cfg = getCachedConfig()) {
    return connectionModalProfileConfigured(panel, info, cfg) || !!connectionModalStatusTarget(panel);
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
    if (!panel || !connectionModalAllowsCelebration(info)) return;
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
    if (!panel || !connectionModalAllowsCelebration(info) || panel.__cwConnectionSuccessTimer) return;
    if (opts.defer !== false && !connectionWindowReadyForCelebration()) {
      queueConnectionSuccess(panel, info);
      return;
    }
    const reduceMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;
    const target = connectionModalCelebrationTarget(panel);
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
      bit.style.setProperty("--d", `${Math.random() * 55}ms`);
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
    }, 1100);
  }

  function showConnectionDisconnected(panel) {
    if (!panel) return;
    const node = panel.querySelector(".cw-connection-status-pill, #plex_msg, #jfy_msg, #emby_msg");
    if (!node) return;
    node.textContent = T("connections.status.disconnected");
    node.classList.remove("hidden", "ok", "cw-connection-status-transient", "cw-connection-status-dismissing");
    node.classList.add("warn");
    node.removeAttribute("aria-hidden");
    scheduleConnectionStatusDismissal(node);
  }

  function connectionModalCurrentConnected(panel, info, cfg = getCachedConfig()) {
    if (!info || META_ITEMS[info.key]) return true;
    if (info.key === "NUVIO") {
      const profileRow = panel?.querySelector("#nuvio_profile_state");
      if (profileRow && !profileRow.classList.contains("hidden") && String(panel?.querySelector("#nuvio_profile_select")?.value || "").trim()) return true;
    }
    if (info.key === "SILO") {
      const profileRow = panel?.querySelector("#silo_profile_state");
      if (profileRow && !profileRow.classList.contains("hidden") && String(panel?.querySelector("#silo_profile_select")?.value || "").trim()) return true;
    }
    if (info.key === "TOFA") {
      const profileRow = panel?.querySelector("#tofa_profile_state");
      if (profileRow && !profileRow.classList.contains("hidden") && String(panel?.querySelector("#tofa_profile_select")?.value || "").trim()) return true;
    }
    if (connectionModalStatusTarget(panel)) return true;
    const inst = String(panel?.querySelector(".cw-profile-switcher select")?.value || "default");
    return configuredProfileIds(cfg, info.provider).some((id) => String(id) === inst);
  }

  function updateConnectionSaveEnabled(panel, info, cfg = getCachedConfig()) {
    const save = panel?.querySelector(".cw-connection-footer-save");
    if (!save) return;
    const connected = connectionModalCurrentConnected(panel, info, cfg);
    const canSave = connected || !!info?.allowSaveWithoutConnection;
    save.disabled = !canSave;
    save.classList.toggle("is-disabled", !canSave);
    if (canSave) save.removeAttribute("title");
    else save.title = T("connections.instances.connectFirst");
  }

  function syncConnectionSuccessState(panel, info, cfg = getCachedConfig()) {
    if (!panel || !info) return;
    const profile = connectionModalProfileId(panel);
    const switched = panel.__cwConnectionProfileSeen !== profile;
    const connected = connectionModalProfileConnected(panel, info, cfg);
    if (connectionModalAllowsCelebration(info) && !switched && connected && panel.__cwConnectionWasConnected === false) playConnectionSuccess(panel, info);
    panel.__cwConnectionProfileSeen = profile;
    panel.__cwConnectionWasConnected = connected;
    updateConnectionSaveEnabled(panel, info, cfg);
  }

  function connectionProfileFullName(option) {
    return String(option?.dataset?.cwProfileFullName || option?.textContent || option?.value || "").trim();
  }

  function refreshConnectionProfileCreateState(select, newBtn) {
    if (!select || !newBtn) return;
    const raw = getCachedConfig()?.runtime?.max_profiles_per_provider;
    const valid = typeof raw === "number" || typeof raw === "boolean"
      || (typeof raw === "string" && /^[+-]?\d+$/.test(raw.trim()));
    const parsed = valid ? Number(raw) : NaN;
    const limit = Math.max(1, Math.min(100, Number.isFinite(parsed) ? Math.trunc(parsed) : 10));
    const count = Array.from(select.options || []).filter((option) => option.value).length;
    const capped = count >= limit;
    newBtn.disabled = capped;
    newBtn.setAttribute("aria-disabled", capped ? "true" : "false");
    if (capped) newBtn.title = T("connections.instances.max", { count: limit });
    else newBtn.removeAttribute("title");
  }

  function decorateConnectionProfileToolbar(toolbar, info) {
    if (!toolbar) return;
    toolbar.classList.add("cw-connection-profile-toolbar");
    toolbar.style.removeProperty("margin-left");
    toolbar.querySelector("[data-cw-profile-label]")?.classList.add("cw-connection-profile-hidden-label");
    const select = toolbar.querySelector("select");
    const newBtn = toolbar.querySelector(".cw-profile-new");
    const delBtn = toolbar.querySelector(".cw-profile-delete");
    if (select) {
      select.setAttribute("aria-label", T("connections.instanceOf", {name: info?.copy?.auth?.[0] || info?.key || T("connections.connectionWord")}));
      Array.from(select.options || []).forEach((option) => {
        const fullName = connectionProfileFullName(option);
        option.dataset.cwProfileFullName = fullName;
        option.title = fullName;
      });
      if (!select.__cwConnectionProfileToolbarBound) {
        select.__cwConnectionProfileToolbarBound = true;
        if (typeof MutationObserver === "function") {
          const mo = new MutationObserver(() => {
            Array.from(select.options || []).forEach((option) => {
              const fullName = connectionProfileFullName(option);
              option.dataset.cwProfileFullName = fullName;
              option.title = fullName;
            });
            refreshConnectionProfileCreateState(select, newBtn);
          });
          mo.observe(select, { childList: true, subtree: true, characterData: true });
          select.__cwConnectionProfileToolbarObserver = mo;
        }
      }
    }
    if (newBtn && !newBtn.__cwConnectionProfileDecorated) {
      newBtn.__cwConnectionProfileDecorated = true;
      newBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">add</span><span>${TH("connections.instances.new")}</span>`;
    }
    refreshConnectionProfileCreateState(select, newBtn);
    if (delBtn && !delBtn.__cwConnectionProfileDecorated) {
      delBtn.__cwConnectionProfileDecorated = true;
      delBtn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">delete</span><span>${TH("connections.instances.delete")}</span>`;
    }
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
          <strong>${TH("connections.instances.title")}</strong>
          <a class="material-symbols-rounded cw-connection-profile-help" href="${window.CW.HelpLinks.url("connection-instances")}" target="_blank" rel="noopener noreferrer" aria-label="${TH("connections.instances.guide")}" title="${TH("connections.instances.guide")}">help</a>
        </div>
        <div class="cw-connection-profile-body">
          <div class="cw-connection-profile-slot"></div>
        </div>`;
      nav.appendChild(card);
    }

    const slot = card.querySelector(".cw-connection-profile-slot");
    const toolbar = findConnectionProfileToolbar(panel, info);
    if (toolbar && slot) {
      if (toolbar.parentElement !== slot) slot.appendChild(toolbar);
      decorateConnectionProfileToolbar(toolbar, info);
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

  function ensureConnectionWhitelistTable() {
    if (window.cwWhitelistTable) return Promise.resolve(true);
    if (window.__cwWhitelistTableLoading) return window.__cwWhitelistTableLoading;
    window.__cwWhitelistTableLoading = new Promise((resolve) => {
      const script = document.createElement("script");
      script.src = `/assets/helpers/whitelist_table.js${window.__CW_VERSION__ ? `?v=${encodeURIComponent(window.__CW_VERSION__)}` : ""}`;
      script.async = true;
      script.onload = () => resolve(!!window.cwWhitelistTable);
      script.onerror = () => resolve(false);
      document.head.appendChild(script);
    });
    return window.__cwWhitelistTableLoading;
  }

  function connectionWhitelistConfig(info) {
    if (!info) return null;
    const map = {
      PLEX: { prefix: "plex", api: "plex" },
      JELLYFIN: { prefix: "jfy", api: "jellyfin" },
      EMBY: { prefix: "emby", api: "emby" },
      KODI: { prefix: "kodi", api: "kodi", host: "kodi_libraries" },
    };
    return map[info.key] || null;
  }

  function connectionWhitelistInstance(panel, info) {
    const scoped = panel?.querySelector(`.cw-profile-switcher[data-cw-profile-provider="${info?.provider}"] select`);
    return String(scoped?.value || panel?.querySelector(".cw-profile-switcher select")?.value || "default");
  }

  function connectionWhitelistSelect(prefix, feature) {
    const suffix = { hist: "history", rate: "ratings", prog: "progress", coll: "collection", scr: "scrobble" }[feature] || feature;
    return document.getElementById(`${prefix}_lib_${suffix}`);
  }

  function connectionWhitelistRead(prefix, feature) {
    return new Set(Array.from(connectionWhitelistSelect(prefix, feature)?.selectedOptions || []).map((opt) => String(opt.value || "").trim()).filter(Boolean));
  }

  function connectionWhitelistWrite(prefix, feature, values) {
    const sel = connectionWhitelistSelect(prefix, feature);
    if (!sel) return;
    const esc = (value) => String(value).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
    sel.innerHTML = Array.from(values || []).map((value) => `<option selected value="${esc(value)}">${esc(value)}</option>`).join("");
  }

  const CONNECTION_WHITELIST_FEATURES = { hist: "history", rate: "ratings", prog: "progress", coll: "collection", scr: "scrobble" };

  function connectionWhitelistOwner(info) {
    const name = { PLEX: "plex", JELLYFIN: "jellyfin", EMBY: "emby", KODI: "kodi" }[info?.key];
    return name ? window.cwAuth?.[name]?.whitelist || null : null;
  }

  function connectionWhitelistFromConfig(config, info, inst) {
    const block = providerConfigBlock(config, info?.provider);
    const source = String(inst || "default").toLowerCase() === "default" ? block : block?.instances?.[inst];
    return Object.fromEntries(Object.entries(CONNECTION_WHITELIST_FEATURES).map(([feature, key]) => [feature, new Set((source?.[key]?.libraries || []).map(String))]));
  }

  function bindConnectionWhitelistSwitch(panel, info) {
    panel.__cwWhitelistInfo = info;
    if (panel.__cwWhitelistSwitchBound) return;
    panel.__cwWhitelistSwitchBound = true;
    panel.addEventListener("change", (ev) => {
      if (!ev.target?.closest?.(".cw-profile-switcher")) return;
      if (!panel.querySelector('.cw-subpanel[data-sub="whitelist"].active')) return;
      void renderConnectionWhitelist(panel, panel.__cwWhitelistInfo, { force: true });
    });
  }

  function markConnectionWhitelistHydrated(info) {
    if (info?.key === "PLEX") window.__plexHydrated = true;
    else if (info?.key === "JELLYFIN") {
      window.__jellyfinHydrated = true;
      window.__jfyHydrated = true;
    } else if (info?.key === "EMBY") window.__embyHydrated = true;
    else if (info?.key === "KODI") window.__kodiHydrated = true;
  }

  async function loadConnectionWhitelistLibraries(panel, info, cfg) {
    const inst = connectionWhitelistInstance(panel, info);
    const url = new URL(`/api/${cfg.api}/libraries`, window.location.origin);
    if (inst) url.searchParams.set("instance", inst);
    const response = await fetch(url.toString(), { cache: "no-store" });
    const data = response.ok ? await response.json().catch(() => ({})) : {};
    return Array.isArray(data?.libraries) ? data.libraries : (Array.isArray(data) ? data : []);
  }

  async function renderConnectionWhitelist(panel, info, opts = {}) {
    const cfg = connectionWhitelistConfig(info);
    if (!panel || !cfg) return false;
    const host = document.getElementById(cfg.host || `${cfg.prefix}_lib_matrix`);
    if (!host) return false;
    const inst = connectionWhitelistInstance(panel, info);
    const generation = (host.__cwConnectionWhitelistGeneration || 0) + 1;
    host.__cwConnectionWhitelistGeneration = generation;
    const current = () => host.__cwConnectionWhitelistGeneration === generation && host.__cwConnectionWhitelistInstance === inst;
    if (host.__cwConnectionWhitelistInstance !== inst) {
      host.__cwConnectionWhitelistInstance = inst;
      host.__cwConnectionWhitelistSets = null;
      host.__cwConnectionWhitelistLibs = null;
      host.__cwConnectionWhitelistHandle = null;
    }
    await ensureConnectionWhitelistTable();
    if (!window.cwWhitelistTable || !current()) return false;

    bindConnectionWhitelistSwitch(panel, info);
    const owner = connectionWhitelistOwner(info);
    if (!host.__cwConnectionWhitelistSets) {
      const owned = owner?.read?.(inst);
      let initial = owned
        ? Object.fromEntries(Object.keys(CONNECTION_WHITELIST_FEATURES).map((feature) => [feature, new Set((owned[feature] || []).map(String))]))
        : null;
      if (!initial && owner) {
        const loaded = connectionWhitelistFromConfig(await loadConfig(true), info, inst);
        if (!current()) return false;
        initial = loaded;
      }
      host.__cwConnectionWhitelistSets = host.__cwConnectionWhitelistSets || initial
        || Object.fromEntries(Object.keys(CONNECTION_WHITELIST_FEATURES).map((feature) => [feature, connectionWhitelistRead(cfg.prefix, feature)]));
    }
    const sets = host.__cwConnectionWhitelistSets;
    const owns = () => host.__cwConnectionWhitelistInstance === inst && host.__cwConnectionWhitelistSets === sets;
    const syncHidden = () => {
      if (!owns()) return;
      Object.keys(sets).forEach((feature) => connectionWhitelistWrite(cfg.prefix, feature, sets[feature]));
      connectionWhitelistOwner(info)?.write?.(inst, Object.fromEntries(Object.entries(sets).map(([feature, set]) => [feature, [...set]])));
      markConnectionWhitelistHydrated(info);
    };
    const ensureLibs = async (force = false) => {
      if (force || !Array.isArray(host.__cwConnectionWhitelistLibs)) {
        const libs = await loadConnectionWhitelistLibraries(panel, info, cfg);
        if (!owns()) return null;
        host.__cwConnectionWhitelistLibs = libs;
      }
      return host.__cwConnectionWhitelistLibs;
    };

    if (!host.__cwConnectionWhitelistHandle) {
      host.__cwConnectionWhitelistLibs = Array.isArray(host.__cwConnectionWhitelistLibs) ? host.__cwConnectionWhitelistLibs : [];
      syncHidden();
      host.__cwConnectionWhitelistHandle = window.cwWhitelistTable.mount({
        host,
        features: [
          { key: "hist", label: T("profile.history.title") },
          { key: "rate", label: T("profile.ratings.title") },
          { key: "prog", label: T("profile.playback.title") },
          { key: "coll", label: T("profile.collection.title") },
          { key: "scr", label: T("connections.libraries.scrobble"), title: T("connections.libraries.scrobbleHelp") },
        ],
        getLibs: () => host.__cwConnectionWhitelistLibs || [],
        isOn: (feature, id) => sets[feature]?.has(String(id)),
        selectedIds: () => Object.values(sets).flatMap((set) => [...(set || [])]),
        setOn: (feature, id, on) => { if (on) sets[feature]?.add(String(id)); else sets[feature]?.delete(String(id)); },
        commit: syncHidden,
        load: async () => { if (await ensureLibs(true)) syncHidden(); },
      });
    }

    if (opts.force || !Array.isArray(host.__cwConnectionWhitelistLibs) || !host.__cwConnectionWhitelistLibs.length) {
      await ensureLibs(!!opts.force);
      if (!current()) return false;
    }
    syncHidden();
    host.__cwConnectionWhitelistHandle?.render?.();
    return true;
  }

  async function syncConnectionProviderSub(panel, info, sub) {
    if (!panel || !info) return;
    const wanted = String(sub || "").toLowerCase();
    if (info.key === "TMDB_METADATA") {
      try { window.cwMetaProviderSubSelect?.("tmdb", wanted); } catch {}
      return;
    }
    if (wanted !== "whitelist") return;

    await renderConnectionWhitelist(panel, info, { force: true });
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
          selectConnectionModalSub(panel, info, sub);
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
    panel.classList.toggle("cw-connection-no-nav", !!info.hideNav && !cardVisible);
    window.CW?.MobileUI?.sectionPicker(nav);
    return nav;
  }

  function syncConnectionDialogHead(overlay, info) {
    if (!overlay || !info) return;
    const logo = overlay.querySelector(".cw-auth-dialog-logo");
    if (logo) {
      logo.classList.remove("hidden");
      const mark = window.CW?.ProviderMeta?.logoPath?.(info.logo) || `/assets/img/${info.logo}.svg`;
      setConnectionStyle(logo, "--cw-connection-logo", `url("${mark}")`);
    }
  }

  function selectConnectionModalSub(panel, info, sub, overlay = null) {
    if (!panel || !sub) return;
    const wanted = String(sub).toLowerCase();
    panel.querySelectorAll(".cw-subtile[data-sub]").forEach((node) => node.classList.toggle("active", String(node.dataset.sub || "").toLowerCase() === wanted));
    panel.querySelectorAll(".cw-subpanel[data-sub]").forEach((node) => node.classList.toggle("active", String(node.dataset.sub || "").toLowerCase() === wanted));
    window.CW?.MobileUI?.sectionPicker(panel.querySelector(":scope > .cw-subtiles"));
    applyConnectionModalOrder(panel, info);
    syncConnectionModalCopy(panel, info, overlay);
    resetConnectionModalScroll(panel);
    syncConnectionProviderSub(panel, info, wanted).finally(() => scheduleConnectionModalSize(panel, info));
  }

  function resetConnectionModalScroll(panel) {
    [panel?.querySelector(":scope > .cw-subpanels"), panel?.querySelector(":scope > .auth-card")].forEach((node) => {
      if (node) node.scrollTop = 0;
    });
  }

  function connectionModalMaxHeight(info) {
    const viewport = Math.max(360, window.innerHeight || document.documentElement?.clientHeight || 720);
    const cap = (info?.size || "wide") === "wide" ? 660 : 620;
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
    if (!activePanel) {
      const kids = Array.from(scroller.children || []);
      const last = kids[kids.length - 1];
      if (!last) return 0;
      return Math.ceil((last.offsetTop || 0) + (last.offsetHeight || 0) + cssPx(scroller, "padding-bottom"));
    }
    const padding = cssPx(scroller, "padding-top") + cssPx(scroller, "padding-bottom");
    return Math.ceil((activePanel.scrollHeight || activePanel.offsetHeight || 0) + padding);
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
    const icon = info?.key === "TMDB_METADATA" || info?.key === "FANART_METADATA" || info?.key === "PUBLICMETADB" ? "key" : info?.key === "ANIME_MAPPING" ? "hub" : "link";
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
      const logo = window.CW?.ProviderMeta?.logoPath?.(data[4] || info.logo) || `/assets/img/${data[4] || info.logo}.svg`;
      setConnectionStyle(node, "--cw-auth-logo", `url("${logo}")`);
      node.innerHTML = `<span class="material-symbols-rounded cw-auth-journey-icon" aria-hidden="true">${icon}</span><div class="cw-auth-journey-text"><div class="cw-auth-journey-title">${data[0]}</div><div class="cw-auth-journey-copy">${data[1]}</div></div><a class="cw-auth-journey-help" href="${info.help}" target="_blank" rel="noopener noreferrer" aria-label="${TH("connections.modal.openNamedGuide", { name: data[0] })}" title="${TH("connections.modal.openGuide")}"><span class="material-symbols-rounded" aria-hidden="true">help</span></a>`;
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
    const provider = info?.provider;
    if (provider !== "tautulli" && provider !== "tracearr") return;
    const userInput = panel.querySelector(`#${provider}_user_id`);
    const userField = userInput?.closest("div");
    const grid = panel.querySelector(`#${provider}_server`)?.closest(".grid2");
    if (!userField || !grid || userField.parentElement === grid) return;
    userField.classList.add(`cw-${provider}-user-field`);
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
              status.textContent = cfg.connectedText || T("common.status.connected");
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
    const ids = configuredProfileIds(getCachedConfig(), info.provider).map((id) => String(id).toLowerCase());
    if (!ids.includes("default")) return false;
    return ids.some((id) => id !== "default");
  }

  function setConnectionSaveBusy(btn, busy) {
    if (!btn) return;
    if (!btn.__cwSaveLabel) btn.__cwSaveLabel = btn.textContent;
    btn.classList.toggle("is-saving", !!busy);
    if (busy) {
      btn.textContent = T("common.status.saving");
      btn.setAttribute("aria-busy", "true");
    } else {
      btn.removeAttribute("aria-busy");
    }
  }

  function flashConnectionResult(btn, ok) {
    if (!btn) return;
    if (!btn.__cwSaveLabel) btn.__cwSaveLabel = btn.textContent;
    if (btn.__cwSaveTimer) clearTimeout(btn.__cwSaveTimer);
    btn.classList.remove("is-saving", "is-saved", "is-failed");
    btn.removeAttribute("aria-busy");
    btn.innerHTML = `<span class="material-symbols-rounded cw-save-result-icon">${ok ? "check" : "close"}</span>`;
    btn.classList.add(ok ? "is-saved" : "is-failed");
    btn.__cwSaveTimer = setTimeout(() => {
      btn.textContent = btn.__cwSaveLabel || T("connections.footer.saveChanges");
      btn.classList.remove("is-saved", "is-failed");
    }, 1600);
  }

  function resetConnectionDeleteConfirm(btn) {
    if (!btn) return;
    if (btn.__cwConnectionDeleteTimer) clearTimeout(btn.__cwConnectionDeleteTimer);
    btn.__cwConnectionDeleteTimer = 0;
    btn.dataset.cwConfirmDelete = "";
    btn.classList.remove("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">delete</span><span>${TH("connections.footer.deleteConnection")}</span>`;
  }

  function armConnectionDeleteConfirm(btn) {
    if (!btn) return;
    btn.dataset.cwConfirmDelete = "1";
    btn.classList.add("is-confirming");
    btn.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span>${TH("connections.footer.confirmDelete")}</span>`;
    if (btn.__cwConnectionDeleteTimer) clearTimeout(btn.__cwConnectionDeleteTimer);
    btn.__cwConnectionDeleteTimer = setTimeout(() => resetConnectionDeleteConfirm(btn), 4200);
  }

  function connectionDeleteWarningVisible(panel) {
    const warn = panel?.querySelector(".cw-connection-footer-warn");
    return !!(warn && !warn.classList.contains("hidden"));
  }

  function connectionConnectedPill(panel) {
    if (!panel) return null;
    const nodes = Array.from(panel.querySelectorAll("#plex_msg, #jfy_msg, #emby_msg, .cw-connection-status-pill"));
    return nodes.find((node) => {
      const text = String(node?.textContent || "").trim().toLowerCase();
      return /\bconnected\b/.test(text) && !/\bnot\s+connected\b/.test(text) && !node.classList?.contains("hidden") && node.getAttribute?.("aria-hidden") !== "true";
    }) || null;
  }

  async function awaitConnectionDeleteSettled(panel, hadPill) {
    const start = Date.now();
    const deadline = start + 2500;
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 120));
      if (connectionDeleteWarningVisible(panel)) return false;
      if (hadPill) {
        if (!connectionConnectedPill(panel)) return true;
      } else if (Date.now() - start >= 900) {
        return true;
      }
    }
    return !hadPill;
  }

  async function runConnectionSave(panel, info, footer) {
    const btn = footer?.querySelector(".cw-connection-footer-save");
    if (btn?.__cwSaving) return false;
    if (btn) {
      btn.__cwSaving = true;
      setConnectionSaveBusy(btn, true);
    }
    try {
      await window.CW?.AuthShared?.saveVisibleProfileLabels?.(panel);
      await window.CW?.SyncUsers?.saveDirty?.(panel);
      if (info.key === "NUVIO") await window.cwAuth?.nuvio?.saveSelectedProfile?.({ silent: true });
      if (info.key === "SILO") await window.cwAuth?.silo?.saveSelectedProfile?.({ silent: true });
      if (info.key === "TOFA") await window.cwAuth?.tofa?.saveSelectedProfile?.({ silent: true });
      if (info.key === "FANART_METADATA") await window.cwSaveFanartModal?.();
      const ret = window.saveSettings?.();
      if (ret && typeof ret.then === "function") await ret;
      if (btn) flashConnectionResult(btn, true);
      return true;
    } catch {
      if (btn) flashConnectionResult(btn, false);
      return false;
    } finally {
      if (btn) btn.__cwSaving = false;
    }
  }

  function ensureConnectionModalFooter(panel, info) {
    if (!panel.querySelector(":scope > .cw-connection-modal-footer")) {
      const footer = document.createElement("div");
      footer.className = "cw-connection-modal-footer";
      footer.innerHTML = `<button type="button" class="btn danger cw-connection-footer-delete"><span class="material-symbols-rounded" aria-hidden="true">delete</span><span>${TH("connections.footer.deleteConnection")}</span></button><span class="cw-connection-footer-warn hidden" role="alert" aria-live="polite"></span><button type="button" class="btn cw-connection-footer-cancel">${TH("common.actions.cancel")}</button><button type="button" class="btn primary cw-connection-footer-save">${TH("connections.footer.saveChanges")}</button>`;
      panel.appendChild(footer);
      footer.querySelector(".cw-connection-footer-delete")?.addEventListener("click", async (ev) => {
        const trigger = ev.currentTarget;
        if (connectionDeleteBlockedByProfiles(panel, info)) {
          resetConnectionDeleteConfirm(trigger);
          try { window.CW.AuthShared.showConnectionWarning(T("connections.footer.removeInstancesFirst")); } catch {}
          return;
        }
        if (trigger?.dataset?.cwConfirmDelete !== "1") {
          armConnectionDeleteConfirm(trigger);
          return;
        }
        resetConnectionDeleteConfirm(trigger);
        if (trigger.__cwDeleting) return;
        trigger.__cwDeleting = true;
        try { window.CW.AuthShared.clearConnectionWarnings(); } catch {}
        try {
          const btn = info.deleteSelector ? panel.querySelector(info.deleteSelector) || document.querySelector(info.deleteSelector) : null;
          if (!btn) return;
          const hadPill = !!connectionConnectedPill(panel);
          btn.click();
          if (!await awaitConnectionDeleteSettled(panel, hadPill)) return;
          showConnectionDisconnected(panel);
          if (!await runConnectionSave(panel, info, footer)) return;
          setTimeout(closeAuthProviderOverlay, 600);
        } finally {
          trigger.__cwDeleting = false;
        }
      });
      footer.querySelector(".cw-connection-footer-cancel")?.addEventListener("click", () => {
        resetConnectionDeleteConfirm(footer.querySelector(".cw-connection-footer-delete"));
        closeAuthProviderOverlay();
      });
      footer.querySelector(".cw-connection-footer-save")?.addEventListener("click", async () => {
        resetConnectionDeleteConfirm(footer.querySelector(".cw-connection-footer-delete"));
        if (!await runConnectionSave(panel, info, footer)) return;
        loadConfig(true).then((cfg) => syncConnectionSuccessState(panel, info, cfg)).catch(() => {});
        setTimeout(closeAuthProviderOverlay, 1100);
      });
    }
    const deleteBtn = panel.querySelector(".cw-connection-footer-delete");
    if (!info.deleteSelector) resetConnectionDeleteConfirm(deleteBtn);
    deleteBtn?.classList.toggle("hidden", !info.deleteSelector);
  }

  function applyConnectionMirror(overlay, panel, info) {
    const root = document.documentElement;
    const mirror = root.dataset.cwRole === "user" || root.dataset.cwViewAs === "1";
    const keys = String(root.dataset.cwReconnect || "").split(" ").filter(Boolean);
    const state = mirror ? (keys.includes(info.key) ? "reconnect" : "locked") : "";
    if (state) overlay.dataset.cwMirror = state;
    else delete overlay.dataset.cwMirror;
    let note = panel.querySelector(":scope > .cw-connection-locked-note");
    if (state !== "locked") {
      note?.remove();
      return;
    }
    if (!note) {
      note = document.createElement("p");
      note.className = "cw-connection-locked-note";
      note.textContent = T("connections.footer.adminOnly");
      const anchor = panel.querySelector(":scope > .cw-subpanels") || panel.querySelector(":scope > .auth-card");
      if (anchor) anchor.before(note);
      else panel.appendChild(note);
    }
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
    const mark = window.CW?.ProviderMeta?.logoPath?.(info.logo) || `/assets/img/${info.logo}.svg`;
    setConnectionStyle(panel, "--cw-connection-watermark", `url("${mark}")`);
    syncConnectionDialogHead(overlay, info);
    ensureConnectionModalNav(panel, info);
    ensureConnectionModalJourney(panel, info);
    ensureConnectionModalSteps(panel, info);
    normalizeConnectionModalFields(panel, info);
    applyConnectionModalOrder(panel, info);
    normalizeConnectionModalActions(panel, info);
    ensureConnectionModalFooter(panel, info);
    applyConnectionMirror(overlay, panel, info);
    if (["PLEX", "JELLYFIN", "EMBY", "KODI"].includes(info.key)) selectConnectionModalSub(panel, info, "auth", overlay);
    syncConnectionModalCopy(panel, info, overlay);
    panel.__cwConnectionProfileSeen = connectionModalProfileId(panel);
    panel.__cwConnectionWasConnected = connectionModalProfileConnected(panel, info);
    updateConnectionSaveEnabled(panel, info);
    if (connectionModalAllowsCelebration(info)) ensureConnectionSuccessBurst(panel);
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
    let section = document.getElementById(info.sectionId);
    if (!section) {
      await mountAuthProviders(true);
      section = document.getElementById(info.sectionId);
    }
    if (!form || !section) return;
    parkActiveAuthForm();
    form.appendChild(section);
    section.classList.add("open");
    section.querySelector(":scope > .head")?.setAttribute("aria-expanded", "true");
    openAuthOverlay("form", info.key);
    enhanceConnectionModal(section, overlay, info.key);
    try { await window.cwEnsureAuthSection?.(info.sectionId); } catch {}
    const providerId = connectionInfoForKey(info.key)?.provider || String(info.key || "").toLowerCase();
    try { window.cwAuth?.[providerId]?.init?.(); } catch {}
    try { window.cwAuth?.[providerId]?.rehydrate?.(); } catch {}
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
    if (info.key === "TMDB_METADATA") {
      try { await window.cwRefreshTmdbMetadataState?.({ force: true }); } catch {}
    }
    const overlay = ensureAuthOverlay(slot);
    const form = overlay.querySelector("#cw-auth-provider-form");
    let section = document.getElementById(info.sectionId);
    if (!section) {
      try {
        if (info.key === "TMDB_METADATA") window.cwBuildTmdbPanel?.();
        if (info.key === "ANIME_MAPPING") window.cwBuildAnimeMappingPanel?.();
        if (info.key === "FANART_METADATA") window.cwBuildFanartPanel?.();
        window.cwMetaProviderEnsure?.();
      } catch {}
      section = document.getElementById(info.sectionId);
    }
    if (!section) {
      await mountMetadataProviders(true);
      try { window.cwMetaProviderEnsure?.(); } catch {}
      section = document.getElementById(info.sectionId);
    }
    if (!form || !section) return;
    parkActiveAuthForm();
    form.appendChild(section);
    section.classList.add("open");
    section.querySelector(":scope > .head")?.setAttribute("aria-expanded", "true");
    if (overlay.classList.contains("hidden")) {
      overlay.__cwReturnFocus = document.activeElement;
      window.CW?.DialogFocus?.activate(overlay.querySelector(".cw-auth-dialog"), closeAuthProviderOverlay);
    }
    overlay.classList.remove("hidden");
    overlay.setAttribute("aria-hidden", "false");
    overlay.classList.remove("cw-auth-picker-open");
    overlay.querySelector("#cw-auth-provider-picker")?.classList.add("hidden");
    form.classList.remove("hidden");
    const title = overlay.querySelector("#cw-auth-dialog-title");
    const kicker = overlay.querySelector("#cw-auth-dialog-kicker");
    if (title) title.textContent = info.label;
    if (kicker) kicker.textContent = T("connections.groups.metadata");
    if (info.key === "TMDB_METADATA") {
      try { window.cwMetaProviderSubSelect?.("tmdb", "api", { persist: false }); } catch {}
    }
    if (info.key === "ANIME_MAPPING") {
      try { window.cwAnimeMappingRefreshStatus?.(); } catch {}
    }
    enhanceConnectionModal(section, overlay, info.key);
    if (info.key === "FANART_METADATA") {
      try { await window.cwRefreshFanartState?.(); } catch {}
    }
    if (info.key === "TMDB_METADATA") {
      try { window.cwRefreshTmdbMetadataState?.(); } catch {}
    }
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
    const provider = document.getElementById("cw-auth-connection-overlay")?.dataset?.cwConnectionProvider;
    document.dispatchEvent(new CustomEvent("cw-auth-modal-closed", { detail: { provider } }));
    try { pruneEmptyProfileOnClose(); } catch {}
    parkActiveAuthForm();
    try { window.CW.AuthShared.clearConnectionWarnings(); } catch {}
    const overlay = document.getElementById("cw-auth-connection-overlay");
    overlay?.classList.add("hidden");
    overlay?.setAttribute("aria-hidden", "true");
    window.CW?.DialogFocus?.deactivate(overlay?.querySelector(".cw-auth-dialog"));
    if (overlay?.__cwReturnFocus?.isConnected) overlay.__cwReturnFocus.focus({preventScroll:true});
  }

  function bindAuthPresentation(slot) {
    if (!slot || slot.__cwAuthPresentationBound) return;
    slot.__cwAuthPresentationBound = true;
    slot.addEventListener("input", event => {
      if (!event.target.matches("[data-cw-picker-search]")) return;
      const overlay = event.target.closest(".cw-auth-overlay");
      overlay.dataset.cwPickerSearch = event.target.value;
      renderAuthPicker(overlay, getCachedConfig(), overlay.dataset.cwPickerMode);
      overlay.querySelector(".cw-auth-picker-results").scrollTop = 0;
    });
    slot.addEventListener("keydown", event => {
      const tab = event.target.closest("button[data-cw-picker-category]");
      if (!tab || !["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      const tabs = Array.from(tab.parentElement.querySelectorAll("[role=tab]"));
      const index = tabs.indexOf(tab);
      const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
      tabs[next].click();
      tabs[next].focus({preventScroll:true});
      tabs[next].scrollIntoView({block:"nearest", inline:"nearest"});
    });
    slot.addEventListener("click", (event) => {
      const target = event.target;
      const category = target?.closest?.("button[data-cw-picker-category]");
      if (category) {
        const overlay = category.closest(".cw-auth-overlay");
        overlay.dataset.cwPickerCategory = category.dataset.cwPickerCategory;
        renderAuthPicker(overlay, getCachedConfig(), overlay.dataset.cwPickerMode);
        overlay.querySelector(".cw-auth-picker-results").scrollTop = 0;
        return;
      }
      if (target?.closest?.("[data-cw-fanart-nudge-dismiss]")) {
        event.preventDefault();
        void dismissFanartNudge();
        return;
      }
      const emptyAdd = target?.closest?.("[data-cw-auth-empty-add]");
      if (emptyAdd) {
        event.preventDefault();
        openAuthOverlay("picker", "", emptyAdd.dataset.cwAuthEmptyAdd === "metadata" ? "metadata" : "provider", emptyAdd.dataset.cwAuthCategory || "all");
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
      if (target?.closest?.("[data-cw-auth-close]")) {
        event.preventDefault();
        closeAuthProviderOverlay();
      }
    });
  }

  let pointerHeld = false;
  let deferredCardRender = false;
  let deferredProfileRefresh = false;
  function flushDeferredCardRender() {
    if (!deferredCardRender) return;
    deferredCardRender = false;
    const refreshProfiles = deferredProfileRefresh;
    deferredProfileRefresh = false;
    const slot = document.getElementById("auth-providers");
    if (slot) renderAuthCards(slot, getCachedConfig(), refreshProfiles);
  }
  function releasePointerHold() {
    if (!pointerHeld) return;
    pointerHeld = false;
    setTimeout(flushDeferredCardRender, 0);
  }
  document.addEventListener("pointerdown", () => { pointerHeld = true; }, true);
  document.addEventListener("pointerup", releasePointerHold, true);
  document.addEventListener("pointercancel", releasePointerHold, true);
  window.addEventListener("blur", releasePointerHold);

  async function refreshAuthPresentation(slot, force = false) {
    const cfg = await loadConfig(!!force);
    if (pointerHeld) {
      deferredCardRender = true;
      deferredProfileRefresh ||= force;
      return;
    }
    renderAuthCards(slot, cfg, force);
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
    if (authSetupPending()) {
      try { await window.__cwAuthBootstrapPromise; } catch {}
      if (authSetupPending()) return;
    }
    if (authInflight) return authInflight;
    authInflight = (async () => {
      try {
        const slot = document.getElementById("auth-providers");
        if (!slot) return;
        await loadManagedInstances();
        if (!authHtml || force) authHtml = await apiText("/api/auth/providers/html");
        const main = slot.querySelector(":scope > .cw-auth-main");
        if (main && slot.__cwAuthSourceHtml === authHtml) {
          if (force) await window.hydrateAuthFromConfig?.();
          await refreshAuthPresentation(slot, !!force);
          return;
        }
        const overlay = slot.querySelector(":scope > .cw-auth-overlay");
        const liveSectionIds = new Set(
          Array.from(overlay?.querySelectorAll("#cw-auth-provider-form > .section[id]") || [], (node) => node.id)
        );
        overlay?.remove();
        main?.remove();
        slot.innerHTML = authHtml;
        slot.__cwAuthSourceHtml = authHtml;
        if (main) slot.prepend(main);
        if (overlay) {
          Array.from(slot.querySelectorAll(".section[id]")).forEach((node) => {
            if (liveSectionIds.has(node.id)) node.remove();
          });
          slot.appendChild(overlay);
        }
        bindAuthPresentation(slot);
        ensureAuthShell(slot);
        initMountedAuthSections(slot);
        if (overlay) slot.appendChild(overlay);

        window.initMDBListAuthUI?.();
        window.initPublicMetaDBAuthUI?.();
        window.cwAuth?.floppy?.init?.();
        window.initNuvioAuthUI?.();
        window.initSiloAuthUI?.();
        window.initTofaAuthUI?.();
        window.initTautulliAuthUI?.();
        window.initTracearrAuthUI?.();
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
        const rawHasMarkup = !!raw?.querySelector("#sec-tmdb, #tmdb_api_key, #metadata_locale");

        if (!force && rawLoaded && (tmdbPanel || rawHasMarkup)) {
          try { window.cwMetaProviderEnsure?.(); } catch {}
          try { window.updateTmdbHint?.(); } catch {}
          try { window.cwMetaProviderUpdateChips?.(); } catch {}
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
        window.cx = window.cx || {};
        window.cx.providers = Array.isArray(visible) ? visible : [];
        document.dispatchEvent(new Event("cw:sync-data-changed"));
        if (!Array.isArray(visible) || !visible.length) {
          div.innerHTML = '<div class="muted">' + TH("connections.discover.none") + '</div>';
          return [];
        }

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
                    ${chip(T("common.media.watchlist"), !!caps.watchlist)}
                    ${chip(T("profile.ratings.title"), !!caps.ratings)}
                    ${chip(T("profile.history.title"), !!caps.history)}
                    ${chip(T("navigation.tabs.playlists"), !!caps.playlists)}
                  </div>
                </div>
              </div>`;
          }).join("");
        }
        return visible;
      } catch (e) {
        div.innerHTML = '<div class="muted">' + TH("connections.discover.failed") + '</div>';
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
    if (authSetupPending()) {
      try { await window.__cwAuthBootstrapPromise; } catch {}
      if (authSetupPending()) return;
    }
    const page = document.getElementById("page-settings");
    const settingsVisible = !!(page && !page.classList.contains("hidden"));
    const pane = currentSettingsPane();
    if (!settingsVisible || !["providers", "sync"].includes(pane)) return;

    if (pane === "sync") {
      wireSyncRefresh();
      await loadProviders(true);
      renderSyncCounts();
      return;
    }

    wireConnectionsRefresh();

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

  window.addEventListener("cw:settings-scope-changed", () => {
    const slot = document.getElementById("auth-providers");
    if (!slot?.querySelector(".cw-auth-dashboard")) return;
    try { renderAuthCards(slot, getCachedConfig()); } catch {}
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
    panel.__cwConnectionProfileSeen = connectionModalProfileId(panel);
    panel.__cwConnectionWasConnected = true;
    playConnectionSuccess(panel, info);
  }, true);

  window.addEventListener("auth-changed", () => {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    (async () => {
      try { await window.refreshStatus?.(true); } catch {}
      await refreshAuthPresentation(slot, true);
    })().catch(() => {});
  });

  document.addEventListener("cw-status-updated", () => {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    refreshAuthPresentation(slot, false).catch(() => {});
  }, true);

  async function openAddConnection() {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    bindAuthPresentation(slot);
    await mountAuthProviders();
    openAuthOverlay("picker", "", "provider");
  }

  async function openAddMetadata() {
    const slot = document.getElementById("auth-providers");
    if (!slot) return;
    bindAuthPresentation(slot);
    await mountMetadataProviders();
    openAuthOverlay("picker", "", "metadata");
  }

  function wireConnectionsRefresh() {
    const btn = document.getElementById("cw-conn-refresh");
    if (!btn || btn.__cwBound) return;
    btn.__cwBound = true;
    btn.addEventListener("click", async () => {
      if (btn.classList.contains("loading")) return;
      btn.classList.add("loading", "spin");
      try {
        await window.refreshStatus?.(true);
      } catch {}
      btn.classList.remove("loading", "spin");
    });
  }

  function renderSyncCounts() {
    const providersEl = document.getElementById("cw-sync-stat-providers");
    if (!providersEl) return;
    const cx = window.cx || {};
    const pairs = Array.isArray(cx.pairs) ? cx.pairs : [];
    const active = pairs.filter((pair) => pair && pair.enabled !== false).length;
    providersEl.textContent = String(Array.isArray(cx.providers) ? cx.providers.length : 0);
    const activeEl = document.getElementById("cw-sync-stat-active");
    if (activeEl) activeEl.textContent = String(active);
    const disabledEl = document.getElementById("cw-sync-stat-disabled");
    if (disabledEl) disabledEl.textContent = String(pairs.length - active);
  }

  function wireSyncRefresh() {
    const btn = document.getElementById("cw-sync-refresh");
    if (!btn || btn.__cwBound) return;
    btn.__cwBound = true;
    btn.addEventListener("click", async () => {
      if (btn.classList.contains("loading")) return;
      btn.classList.add("loading", "spin");
      try {
        await Promise.allSettled([window.loadProviders?.(true), window.loadPairs?.(true)]);
        await window.cxRenderPairsOverlay?.(true);
      } catch {}
      btn.classList.remove("loading", "spin");
      renderSyncCounts();
    });
  }

  window.addEventListener("cx:pairs:changed", renderSyncCounts);
  document.addEventListener("cx-state-change", renderSyncCounts);

  const ProvidersUI = {
    updateFlowRailLogos,
    ensureProvidersPaneReady,
    mountAuthProviders,
    mountMetadataProviders,
    loadProviders,
    openAddConnection,
    openAddMetadata,
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
