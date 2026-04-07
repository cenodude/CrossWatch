/* assets/helpers/core.js */
/* CrossWatch - Core UI orchestration and shared helpers */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function () {
  const CW = (window.CW ||= {});
  const API = CW.API || {};
  const DOM = CW.DOM || {};
  const META = CW.ProviderMeta || {};

  const isTV = (v) => /^(tv|show|shows|series|season|episode|anime)$/i.test(String(v || ""));
  const byId = (id) => document.getElementById(id);
  const readValue = (id, fallback = "") => {
    const el = byId(id);
    return el && "value" in el ? (el.value ?? fallback) : fallback;
  };
  const readText = (id, fallback = "") => {
    const el = byId(id);
    return el ? (el.textContent ?? fallback) : fallback;
  };
  const setValue = (id, value) => {
    const el = byId(id);
    if (el) el.value = value ?? "";
  };
  const setText = (id, value) => {
    const el = byId(id);
    if (el) el.textContent = value ?? "";
  };
  const setChecked = (id, on) => {
    const el = byId(id);
    if (el) el.checked = !!on;
  };
  const boolSelect = (id) => String(readValue(id, "false")).toLowerCase() === "true";
  const onReady = (fn) => {
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", fn, { once: true });
    else fn();
  };
  const raf = (fn) => (window.requestAnimationFrame || ((cb) => setTimeout(cb, 0)))(fn);

  const PROVIDER_ORDER = META.order || [
    "CROSSWATCH", "PLEX", "SIMKL", "TRAKT", "ANILIST", "TMDB", "JELLYFIN", "EMBY", "MDBLIST", "TAUTULLI"
  ];
  const STATUS_PROVIDERS = typeof META.statusProviders === "function" ? META.statusProviders() : [
    { key: "PLEX", badgeId: "badge-plex", legacy: ["plex_connected", "plex"] },
    { key: "SIMKL", badgeId: "badge-simkl", legacy: ["simkl_connected", "simkl"] },
    { key: "TRAKT", badgeId: "badge-trakt", legacy: ["trakt_connected", "trakt"] },
    { key: "ANILIST", badgeId: "badge-anilist", legacy: ["anilist_connected", "anilist"] },
    { key: "TMDB", badgeId: "badge-tmdb", legacy: ["tmdb_connected", "tmdb"] },
    { key: "JELLYFIN", badgeId: "badge-jellyfin", legacy: ["jellyfin_connected", "jellyfin"] },
    { key: "EMBY", badgeId: "badge-emby", legacy: ["emby_connected", "emby"] },
    { key: "MDBLIST", badgeId: "badge-mdblist", legacy: ["mdblist_connected", "mdblist"] },
    { key: "TAUTULLI", badgeId: "badge-tautulli", legacy: ["tautulli_connected", "tautulli"] },
  ];
  const BADGE_IDS = Object.fromEntries([
    ...STATUS_PROVIDERS.map((p) => [p.key, p.badgeId]),
    ["CROSSWATCH", typeof META.badgeId === "function" ? (META.badgeId("CROSSWATCH") || "badge-crosswatch") : "badge-crosswatch"],
  ]);
  const PAIR_ACTIVE_KEYS = ["PLEX", "SIMKL", "TRAKT", "ANILIST", "TMDB", "JELLYFIN", "EMBY", "MDBLIST", "TAUTULLI", "CROSSWATCH"];
  const PROVIDER_ALIASES = typeof META.aliasesMap === "function" ? META.aliasesMap() : {
    CROSSWATCH: ["CROSSWATCH"],
    PLEX: ["PLEX"],
    SIMKL: ["SIMKL"],
    TRAKT: ["TRAKT"],
    ANILIST: ["ANILIST", "ANI LIST", "ANI-LIST"],
    TMDB: ["TMDB", "TMDBSYNC", "TMDB SYNC", "TMDB-SYNC"],
    JELLYFIN: ["JELLYFIN"],
    EMBY: ["EMBY"],
    MDBLIST: ["MDBLIST", "MDB LIST", "MDB-LIST"],
    TAUTULLI: ["TAUTULLI"],
  };
  const SIMPLE_PROVIDER_CHECKS = [
    { key: "PLEX", paths: [["plex"]], keys: ["account_token", "token"] },
    { key: "SIMKL", paths: [["simkl"], ["auth", "simkl"]], keys: ["access_token"] },
    { key: "TRAKT", paths: [["trakt"], ["auth", "trakt"]], keys: ["access_token"] },
    { key: "ANILIST", paths: [["anilist"], ["auth", "anilist"]], keys: ["access_token", "token"] },
    { key: "JELLYFIN", paths: [["jellyfin"], ["auth", "jellyfin"]], keys: ["access_token"] },
    { key: "EMBY", paths: [["emby"], ["auth", "emby"]], keys: ["access_token", "api_key", "token"] },
    { key: "MDBLIST", paths: [["mdblist"], ["auth", "mdblist"]], keys: ["api_key"] },
  ];

  const UI = (window._ui ||= { status: null, summary: null, pairedProviders: null });
  const state = {
    appDebug: false,
    busy: false,
    currentSummary: null,
    currentTab: "",
    lastStatusMs: 0,
    pairedFetchAt: 0,
    softMainBusy: false,
  };
  let mainBootLoadRefreshDone = false;
  const AUTO_STATUS = false;
  const STATUS_MIN_INTERVAL = 24 * 60 * 60 * 1000;
  const UPDATE_CHECK_INTERVAL_MS = 12 * 60 * 60 * 1000;
  const PAIRS_CACHE_KEY = "cw.pairs.v1";
  const PAIRS_TTL_MS = 15_000;
  const STATUS_CACHE_KEY = "cw.status.v1";
  const DETAILS_MAX_LINES = 2500;
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  function pickCase(obj, key) {
    return obj?.[key] ?? obj?.[String(key).toLowerCase()] ?? obj?.[String(key).toUpperCase()];
  }

  function pathGet(obj, path) {
    return (path || []).reduce((acc, key) => (acc && typeof acc === "object" ? acc[key] : undefined), obj);
  }

  function hasValue(v) {
    return typeof v === "string" ? v.trim().length > 0 : !!v;
  }

  function hasAnyConfigValue(root, keys = []) {
    if (!root || typeof root !== "object") return false;
    if (keys.some((key) => hasValue(root[key]))) return true;
    const instances = root.instances;
    if (!instances || typeof instances !== "object") return false;
    return Object.values(instances).some((inst) => inst && typeof inst === "object" && keys.some((key) => hasValue(inst[key])));
  }

  function hasTmdbConfig(root) {
    if (!root || typeof root !== "object") return false;
    const match = (block) => {
      if (!block || typeof block !== "object") return false;
      return ((hasValue(block.api_key) && hasValue(block.session_id)) || hasValue(block.account_id));
    };
    if (match(root)) return true;
    const instances = root.instances;
    if (!instances || typeof instances !== "object") return false;
    return Object.values(instances).some(match);
  }

  function stateAsBool(v) {
    if (v == null) return false;
    if (typeof v === "boolean") return v;
    if (typeof v === "object") {
      if ("connected" in v) return !!v.connected;
      if ("ok" in v) return !!v.ok;
      if ("authorized" in v) return !!v.authorized;
      if ("auth" in v) return !!v.auth;
      if ("status" in v) return /^(ok|connected|authorized|true|ready|valid)$/i.test(String(v.status));
    }
    return !!v;
  }

  function requestWithTimeout(url, options = {}, ms = 15000) {
    if (authSetupPending()) return Promise.reject(new Error("auth setup pending"));
    if (typeof API.f === "function") return API.f(url, options, ms);
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort("timeout"), ms);
    return fetch(url, { cache: "no-store", ...options, signal: ac.signal }).finally(() => clearTimeout(timer));
  }

  async function requestJSON(url, options = {}, ms = 15000) {
    if (typeof API.j === "function") return API.j(url, options, ms);
    const res = await requestWithTimeout(url, options, ms);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  function maybeCall(fn, ...args) {
    if (typeof fn !== "function") return undefined;
    return fn(...args);
  }

  function queueSafe(fn) {
    queueMicrotask(() => {
      try { fn(); } catch {}
    });
  }

  const providerInstanceCache = new Map();
  const providerUsersCache = new Map();

  async function cwGetProviderInstances(provider, opts = {}) {
    const key = String(provider || "").trim().toLowerCase();
    if (!key) return [{ id: "default", name: "default" }];

    const ttlMs = Number.isFinite(opts.ttlMs) ? opts.ttlMs : 15_000;
    const force = !!opts.force;
    const now = Date.now();
    const slot = providerInstanceCache.get(key) || { ts: 0, list: null, pending: null };
    providerInstanceCache.set(key, slot);

    if (!force && Array.isArray(slot.list) && (now - slot.ts) < ttlMs) return slot.list;
    if (slot.pending) return slot.pending;

    slot.pending = (async () => {
      try {
        const data = await requestJSON(`/api/provider-instances/${encodeURIComponent(key)}`);
        slot.list = [{ id: "default", name: "default" }].concat(
          (data.instances || []).map((item) => ({ id: item.id, name: item.name || item.id }))
        );
      } catch {
        slot.list = [{ id: "default", name: "default" }];
      }
      slot.ts = Date.now();
      return slot.list;
    })().finally(() => {
      slot.pending = null;
    });

    return slot.pending;
  }

  async function cwGetProviderUsers(provider, instanceId = "default", opts = {}) {
    const prov = String(provider || "").trim().toLowerCase();
    const inst = String(instanceId || "default").trim() || "default";
    if (!prov) return [];

    const ttlMs = Number.isFinite(opts.ttlMs) ? opts.ttlMs : 15_000;
    const force = !!opts.force;
    const cacheKey = `${prov}:${inst}`;
    const now = Date.now();
    const slot = providerUsersCache.get(cacheKey) || { ts: 0, value: null, pending: null };
    providerUsersCache.set(cacheKey, slot);

    if (!force && Array.isArray(slot.value) && (now - slot.ts) < ttlMs) return slot.value;
    if (slot.pending) return slot.pending;

    slot.pending = (async () => {
      try {
        slot.value = await requestJSON(`/api/${encodeURIComponent(prov)}/users?instance=${encodeURIComponent(inst)}`);
      } catch {
        slot.value = [];
      }
      slot.ts = Date.now();
      return slot.value;
    })().finally(() => {
      slot.pending = null;
    });

    return slot.pending;
  }

  function cwEnsureScrobbleRoutes(cfg) {
    if (!cfg || typeof cfg !== "object") return cfg;
    cfg.scrobble ||= {};
    cfg.scrobble.watch ||= {};
    const watch = cfg.scrobble.watch;
    if (!Array.isArray(watch.routes)) watch.routes = [];
    return cfg;
  }

  function cwNextRouteId(routes) {
    const used = new Set((routes || []).map((row) => row?.id).filter(Boolean));
    let index = 1;
    while (used.has(`R${index}`)) index += 1;
    return `R${index}`;
  }

  function applyServerSecret(inputId, hasSecret) {
    const el = byId(inputId);
    if (!el) return;
    el.value = hasSecret ? "********" : "";
    el.dataset.masked = hasSecret ? "1" : "0";
    el.dataset.loaded = "1";
    el.dataset.touched = "";
    el.dataset.clear = "";
  }

  function startSecretLoad(inputId) {
    const el = byId(inputId);
    if (!el) return;
    el.dataset.loaded = "0";
    el.dataset.touched = "";
  }

  function finishSecretLoad(inputId, hasSecret) {
    applyServerSecret(inputId, !!hasSecret);
    if (String(inputId || "") === "tmdb_api_key") {
      try { window.cwMetaSettingsHubUpdate?.(); } catch {}
    }
  }

  function getConfiguredProviders(cfg = window._cfgCache || {}) {
    const set = new Set();
    for (const def of SIMPLE_PROVIDER_CHECKS) {
      if (def.paths.some((path) => hasAnyConfigValue(pathGet(cfg, path), def.keys))) set.add(def.key);
    }

    if ([cfg?.tmdb_sync, cfg?.tmdb, cfg?.auth?.tmdb_sync].some(hasTmdbConfig)) set.add("TMDB");
    if ([cfg?.tautulli, cfg?.auth?.tautulli].some((block) => hasAnyConfigValue(block, ["api_key", "server_url", "server"]))) set.add("TAUTULLI");

    const crosswatch = cfg?.crosswatch || cfg?.CrossWatch || {};
    if (crosswatch.enabled !== false) set.add("CROSSWATCH");
    return set;
  }

  function textMatchesProvider(text, key) {
    const haystack = String(text || "").toUpperCase();
    return (PROVIDER_ALIASES[key] || [key]).some((alias) => haystack.includes(String(alias).toUpperCase()));
  }

  function resolveProviderKeyFromNode(node) {
    const direct = String(node?.getAttribute?.("data-sync-prov") || node?.dataset?.syncProv || node?.getAttribute?.("data-prov") || node?.dataset?.prov || "").toUpperCase();
    if (direct) return direct;

    const img = node?.querySelector?.('img[alt], .logo img[alt], [data-logo]');
    const logoText = `${img?.getAttribute?.("alt") || ""} ${img?.dataset?.logo || ""}`.trim();
    for (const key of PROVIDER_ORDER) {
      if (textMatchesProvider(logoText, key)) return key;
    }

    const titleNode = node?.querySelector?.('.title,.name,header,strong,h3,h4');
    const text = titleNode?.textContent || node?.textContent || "";
    for (const key of PROVIDER_ORDER) {
      if (textMatchesProvider(text, key)) return key;
    }

    return "";
  }

  function buildProviderOption(key) {
    const option = document.createElement("option");
    option.value = key;
    option.textContent = providerLabel(key);
    return option;
  }

  function applySyncVisibility() {
    const fromProviderList = Array.isArray(window.cx?.providers) ? window.cx.providers : [];
    const allowed = fromProviderList.some((item) => typeof item?.configured === "boolean")
      ? new Set(
          fromProviderList
            .filter((item) => item?.configured !== false)
            .map((item) => String(item?.key || item?.name || item?.label || "").trim().toUpperCase())
            .filter(Boolean)
        )
      : getConfiguredProviders();
    const host = byId("providers_list");
    if (host) {
      const cards = host.querySelectorAll(".prov-card").length
        ? host.querySelectorAll(".prov-card")
        : host.querySelectorAll(":scope > .card, :scope > *");

      cards.forEach((card) => {
        const key = String(card.dataset?.prov || "").toUpperCase() || resolveProviderKeyFromNode(card);
        if (!key) return;
        card.dataset.syncProv = key;
        card.style.display = allowed.has(key) ? "" : "none";
      });
    }

    ["source-provider", "target-provider"].forEach((id) => {
      const select = byId(id);
      if (!select) return;
      const hadPlaceholder = !!select.options[0] && select.options[0].value === "";
      const previous = String(select.value || "").toUpperCase();
      select.innerHTML = "";
      if (hadPlaceholder) {
        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = "— select —";
        select.appendChild(placeholder);
      }
      PROVIDER_ORDER.filter((key) => allowed.has(key)).forEach((key) => select.appendChild(buildProviderOption(key)));
      select.value = previous && allowed.has(previous) ? previous : (hadPlaceholder ? "" : select.value);
    });
  }

  let syncVisTick = 0;
  function scheduleApplySyncVisibility() {
    if (syncVisTick) return;
    syncVisTick = raf(() => {
      syncVisTick = 0;
      try { applySyncVisibility(); } catch (e) { console.warn("[sync-vis] apply failed", e); }
    });
  }

  function bindSyncVisibilityObservers() {
    const wire = (el) => {
      if (!el || el.__syncObs) return;
      const obs = new MutationObserver(scheduleApplySyncVisibility);
      obs.observe(el, { childList: true, subtree: true });
      el.__syncObs = obs;
    };

    wire(byId("providers_list"));
    wire(document.querySelector("#sec-sync .footer"));

    if (!window.__syncVisEvt) {
      window.addEventListener("settings-changed", (ev) => {
        if (ev?.detail?.scope === "settings") scheduleApplySyncVisibility();
      });
      window.__syncVisEvt = true;
    }

    scheduleApplySyncVisibility();
  }

  const pairsMemory = { ts: 0, list: null, pending: null };

  function _invalidatePairsCache() {
    pairsMemory.ts = 0;
    pairsMemory.list = null;
    pairsMemory.pending = null;
    try { localStorage.removeItem(PAIRS_CACHE_KEY); } catch {}
    try { CW.Cache?.invalidate?.("pairs"); } catch {}
  }

  function _savePairsCache(pairs) {
    const list = Array.isArray(pairs) ? pairs : [];
    pairsMemory.ts = Date.now();
    pairsMemory.list = list;
    try { localStorage.setItem(PAIRS_CACHE_KEY, JSON.stringify({ pairs: list, t: pairsMemory.ts })); } catch {}
  }

  function _loadPairsCache() {
    if (Array.isArray(pairsMemory.list) && pairsMemory.ts) return { pairs: pairsMemory.list, t: pairsMemory.ts };
    try {
      return JSON.parse(localStorage.getItem(PAIRS_CACHE_KEY) || "null");
    } catch {
      return null;
    }
  }

  async function _getPairsFresh(force = false) {
    const now = Date.now();
    if (!force && Array.isArray(pairsMemory.list) && (now - pairsMemory.ts) < PAIRS_TTL_MS) return pairsMemory.list;
    if (pairsMemory.pending) return pairsMemory.pending;

    pairsMemory.pending = (async () => {
      try {
        const list = typeof API.Pairs?.list === "function"
          ? await API.Pairs.list(!!force)
          : await requestJSON("/api/pairs");
        _savePairsCache(list);
        return Array.isArray(list) ? list : [];
      } catch {
        const cached = _loadPairsCache();
        return Array.isArray(cached?.pairs) ? cached.pairs : [];
      }
    })().finally(() => {
      pairsMemory.pending = null;
    });

    return pairsMemory.pending;
  }

  async function isWatchlistEnabledInPairs() {
    const cached = _loadPairsCache();
    if (cached && (Date.now() - (cached.t || 0)) < PAIRS_TTL_MS) {
      return (cached.pairs || []).some((pair) => !!pair?.features?.watchlist?.enable);
    }
    const list = await _getPairsFresh();
    return list.some((pair) => !!pair?.features?.watchlist?.enable);
  }

  function emptyActiveProviders() {
    return Object.fromEntries(PAIR_ACTIVE_KEYS.map((key) => [key, false]));
  }

  function toggleProviderBadges(active) {
    Object.entries(BADGE_IDS).forEach(([key, id]) => {
      const el = byId(id);
      if (el) el.classList.toggle("hidden", !active?.[key]);
    });
  }

  async function refreshPairedProviders(throttleMs = 5000) {
    const now = Date.now();
    if ((now - state.pairedFetchAt) < throttleMs && UI.pairedProviders) {
      toggleProviderBadges(UI.pairedProviders);
      return UI.pairedProviders;
    }

    state.pairedFetchAt = now;
    const pairs = await _getPairsFresh(false);
    const active = emptyActiveProviders();
    for (const pair of pairs) {
      if (!pair || pair.enabled === false) continue;
      const source = String(pair.source || "").toUpperCase();
      const target = String(pair.target || "").toUpperCase();
      if (source in active) active[source] = true;
      if (target in active) active[target] = true;
    }

    UI.pairedProviders = active;
    toggleProviderBadges(active);
    return active;
  }

  function normalizeProviderState(v) {
    if (typeof v === "boolean") return { connected: v };
    if (typeof v === "number") return { connected: v === 1 };
    if (v && typeof v === "object") return { ...v, connected: stateAsBool(v) };
    if (typeof v === "string") {
      const stateText = v.toLowerCase().trim();
      if (/^(ok|up|connected|ready|true|on|online|active)$/.test(stateText)) return { connected: true, status: v };
      if (/^(no|down|disconnected|false|off|disabled)$/.test(stateText)) return { connected: false, status: v };
    }
    return { connected: false };
  }

  function normalizeProviders(input) {
    const root = input || {};
    return Object.fromEntries(STATUS_PROVIDERS.map((def) => {
      const raw = pickCase(root, def.key) ?? def.legacy.map((field) => root?.[field]).find((v) => v !== undefined);
      return [def.key, normalizeProviderState(raw)];
    }));
  }

  function saveStatusCache(providers) {
    try {
      localStorage.setItem(STATUS_CACHE_KEY, JSON.stringify({ providers: normalizeProviders(providers), updatedAt: Date.now(), v: 1 }));
    } catch {}
  }

  function loadStatusCache(maxAgeMs = 10 * 60 * 1000) {
    try {
      const cached = JSON.parse(localStorage.getItem(STATUS_CACHE_KEY) || "null");
      if (!cached?.providers) return null;
      if ((Date.now() - (cached.updatedAt || 0)) > maxAgeMs) return null;
      return { providers: normalizeProviders(cached.providers), updatedAt: cached.updatedAt };
    } catch {
      return null;
    }
  }

  function connState(value) {
    if (value == null) return "unknown";
    if (value === true || value === 1) return "ok";
    if (value === false || value === 0) return "no";
    if (typeof value === "string") {
      const stateText = value.toLowerCase().trim();
      if (/^(ok|up|connected|ready|true|on|online|active)$/.test(stateText)) return "ok";
      if (/^(no|down|disconnected|false|off|disabled)$/.test(stateText)) return "no";
      return "unknown";
    }
    if (typeof value === "object") {
      if (typeof value.connected === "boolean") return value.connected ? "ok" : "no";
      if (typeof value.ok === "boolean") return value.ok ? "ok" : "no";
      const stateText = String(value.status ?? value.state ?? "").toLowerCase().trim();
      if (/^(ok|up|connected|ready|true|on|online|active)$/.test(stateText)) return "ok";
      if (/^(no|down|disconnected|false|off|disabled)$/.test(stateText)) return "no";
    }
    return "unknown";
  }

  function instancesTooltip(info) {
    const inst = info?.instances;
    const summary = info?.instances_summary;
    if (!inst || typeof inst !== "object") return "";

    const parts = [];
    const ok = Number(summary?.ok);
    const total = Number(summary?.total);
    const rep = String(summary?.rep || info?.rep_instance || "");
    const used = Array.isArray(summary?.used) ? summary.used : (Array.isArray(info?.instances_used) ? info.instances_used : []);

    if (Number.isFinite(ok) && Number.isFinite(total) && total > 1) parts.push(`Instances: ${ok}/${total}`);
    if (used.length && (!Number.isFinite(total) || total > 1)) {
      const labelList = used.slice(0, 4).map((id) => (id === "default" ? "Default" : String(id)));
      parts.push(`Used: ${labelList.join(", ")}${used.length > 4 ? "…" : ""}`);
    }

    const entries = Object.entries(inst).slice(0, 6).map(([id, value]) => {
      const label = id === "default" ? "Default" : String(id);
      const connected = !!(value && typeof value === "object" ? value.connected : value);
      return `${label}=${connected ? "OK" : "NO"}`;
    });
    if (entries.length && (!Number.isFinite(total) || total > 1 || entries.length > 1)) parts.push(entries.join(" · "));
    if (rep && rep !== "default" && (!Number.isFinite(total) || total > 1)) parts.push(`Rep: ${rep}`);

    return parts.filter(Boolean).join(" · ");
  }

  function svgCrown() {
    return '<svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M3 7l4 3 5-6 5 6 4-3v10H3zM5 15h14v2H5z"/></svg>';
  }

  function svgCheck() {
    return '<svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M9 16.2L5.5 12.7l1.4-1.4 2.1 2.1 6-6 1.4 1.4z"/></svg>';
  }

  function setBadge(id, providerName, rawState, stale, providerKey, info) {
    const el = byId(id);
    if (!el) return;

    const stateText = connState(rawState);
    el.classList.remove("ok", "no", "unknown", "stale");
    el.classList.add("conn", stateText);
    if (stale) el.classList.add("stale");

    let tag = "";
    if (providerKey === "PLEX" && info?.plexpass) {
      const plan = String(info?.subscription?.plan || "").toLowerCase();
      const label = plan === "lifetime" ? "Plex Pass • Lifetime" : "Plex Pass";
      tag = `<span class="tag plexpass" title="${label}">${svgCrown()}${label}</span>`;
    } else if (providerKey === "TRAKT" && info?.vip) {
      const type = String(info.vip_type || "vip").toLowerCase();
      const label = /plus|ep/.test(type) ? "VIP+" : "VIP";
      tag = `<span class="tag vip" title="Trakt ${label}">${svgCheck()}${label}</span>`;
    }

    const tips = [];
    const instanceTip = instancesTooltip(info);
    if (instanceTip) tips.push(instanceTip);
    if (providerKey === "TRAKT" && info && typeof info === "object") {
      const limits = info.limits || {};
      const watchlist = limits.watchlist || {};
      const collection = limits.collection || {};
      tips.push(info.vip ? "VIP account" : "Free account");
      if (Number.isFinite(+watchlist.used) && Number.isFinite(+watchlist.item_count) && +watchlist.item_count > 0) {
        tips.push(`Watchlist: ${watchlist.used}/${watchlist.item_count}`);
      }
      if (Number.isFinite(+collection.used) && Number.isFinite(+collection.item_count) && +collection.item_count > 0) {
        tips.push(`Collection: ${collection.used}/${collection.item_count}`);
      }
      if (info.last_limit_error?.feature && info.last_limit_error?.ts) {
        tips.push(`Last limit: ${info.last_limit_error.feature} @ ${info.last_limit_error.ts}`);
      }
    }
    el.title = tips.filter(Boolean).join(" · ");

    const labelState = stateText === "ok" ? "Connected" : (stateText === "no" ? "Not connected" : "Unknown");
    el.innerHTML = `${tag}<span class="txt"><span class="dot ${stateText}"></span><span class="name">${providerName}</span><span class="state">· ${labelState}</span></span>`;
  }

  function renderConnectorStatus(providers, { stale = false } = {}) {
    const source = providers || {};
    STATUS_PROVIDERS.forEach((def) => {
      const info = pickCase(source, def.key);
      setBadge(def.badgeId, providerLabel(def.key), info ?? false, stale, def.key, info);
    });
  }

  function applyStatusSideEffects() {
    const opsCard = byId("ops-card");
    const onMain = opsCard ? !opsCard.classList.contains("hidden") : true;
    const logPanel = byId("log-panel");
    const layout = byId("layout");
    const statsCard = byId("stats-card");
    const statsVisible = !!(statsCard && !statsCard.classList.contains("hidden"));
    logPanel?.classList.toggle("hidden", !(state.appDebug && onMain));
    layout?.classList.toggle("full", onMain && !state.appDebug && !statsVisible);
  }

  function buildStatusUIState(statusPayload, providers) {
    return {
      can_run: !!statusPayload?.can_run,
      ...Object.fromEntries(STATUS_PROVIDERS.map((def) => [`${def.key.toLowerCase()}_connected`, !!providers?.[def.key]?.connected])),
    };
  }

  function extractProviderStatus(statusPayload) {
    const rawProviders = statusPayload?.providers || {};
    return Object.fromEntries(STATUS_PROVIDERS.map((def) => {
      const direct = pickCase(rawProviders, def.key);
      const legacy = def.legacy.map((field) => statusPayload?.[field]).find((value) => value !== undefined);
      return [def.key, normalizeProviderState(direct ?? legacy)];
    }));
  }

  async function refreshStatus(force = false) {
    if (authSetupPending()) return UI.status;
    const now = Date.now();
    if (!force && state.lastStatusMs && (now - state.lastStatusMs) < STATUS_MIN_INTERVAL) return UI.status;
    state.lastStatusMs = now;

    try {
      await refreshPairedProviders(force ? 0 : 5000);
      const payload = typeof API.Status?.get === "function"
        ? await API.Status.get(!!force)
        : await requestJSON("/api/status", {}, 15000);
      state.appDebug = !!payload?.debug;
      const providers = extractProviderStatus(payload);
      renderConnectorStatus(providers, { stale: false });
      saveStatusCache(providers);
      try {
        document.dispatchEvent(new CustomEvent("cw-status-updated", { detail: { payload, providers } }));
      } catch {}
      UI.status = buildStatusUIState(payload, providers);
      recomputeRunDisabled();
      applyStatusSideEffects();
      return UI.status;
    } catch (e) {
      if (String(e?.message || e || "").includes("auth setup pending")) return UI.status;
      console.warn("refreshStatus failed", e);
      return UI.status;
    }
  }

  async function manualRefreshStatus() {
    if (manualRefreshStatus._inFlight) return;
    manualRefreshStatus._inFlight = true;

    const btn = byId("btn-status-refresh");
    btn?.classList.add("spin");
    setRefreshBusy(true);

    try {
      await refreshPairedProviders(0);
      const cached = loadStatusCache();
      if (cached?.providers) {
        renderConnectorStatus(cached.providers, { stale: true });
      } else if (UI.status) {
        renderConnectorStatus(Object.fromEntries(STATUS_PROVIDERS.map((def) => [def.key, { connected: !!UI.status?.[`${def.key.toLowerCase()}_connected`] }])), { stale: true });
      }

      try {
        await refreshStatus(true);
      } catch (e) {
        console.warn("Manual status refresh timed out; showing cached", e);
        const fallback = loadStatusCache();
        if (fallback?.providers) renderConnectorStatus(fallback.providers, { stale: true });
        queueSafe(() => { refreshStatus(true); });
      }
    } catch (e) {
      console.warn("Manual status refresh failed", e);
    } finally {
      setRefreshBusy(false);
      btn?.classList.remove("spin");
      manualRefreshStatus._inFlight = false;
    }
  }

  function bootstrapStatusFromCache() {
    try {
      const cached = loadStatusCache();
      if (cached?.providers) renderConnectorStatus(cached.providers, { stale: true });
    } catch {}
    queueSafe(() => refreshPairedProviders(0));
  }

  function toLocal(iso) {
    if (!iso) return "—";
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return iso;
    return date.toLocaleString(undefined, { hour12: false });
  }

  function computeRedirectURI() {
    return `${location.origin}/callback`;
  }

  function flashCopy(btn, ok, msg) {
    if (!btn) {
      if (!ok) alert(msg || "Copy failed");
      return;
    }
    const previous = btn.textContent;
    btn.disabled = true;
    btn.textContent = ok ? "Copied ✓" : (msg || "Copy failed");
    setTimeout(() => {
      btn.textContent = previous;
      btn.disabled = false;
    }, 1200);
  }

  function recomputeRunDisabled() {
    const disabled = !!state.busy || !!UI.summary?.running || !(UI.status ? !!UI.status.can_run : true);
    [byId("run"), byId("run-menu")].forEach((btn) => {
      if (btn) btn.disabled = disabled;
    });
  }

  window.setTimeline = function setTimeline(timeline) {
    if (window.UX?.updateTimeline) window.UX.updateTimeline(timeline || {});
    else window.dispatchEvent(new CustomEvent("ux:timeline", { detail: timeline || {} }));
  };

  function setSyncHeader(status, msg) {
    const icon = byId("sync-icon");
    if (icon) {
      icon.classList.remove("sync-ok", "sync-warn", "sync-bad");
      icon.classList.add(status);
    }
    setText("sync-status-text", msg);
  }

  function relTimeFromEpoch(epoch) {
    if (!epoch) return "";
    const ageSec = Math.max(1, Math.floor((Date.now() / 1000) - epoch));
    const units = [["y", 31536000], ["mo", 2592000], ["d", 86400], ["h", 3600], ["m", 60], ["s", 1]];
    for (const [label, span] of units) {
      if (ageSec >= span) return `${Math.floor(ageSec / span)}${label} ago`;
    }
    return "just now";
  }

  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") {
      try { window.closeAbout?.(); } catch {}
    }
  });

  function enforceMainLayout() {
    const layout = byId("layout");
    if (!layout) return;
    layout.classList.remove("single", "full");
    byId("stats-card")?.classList.remove("hidden");
  }

  async function softRefreshMain() {
    if (authSetupPending()) return;
    if (state.softMainBusy) return;
    state.softMainBusy = true;
    enforceMainLayout();
    try {
      await Promise.allSettled([
        refreshStatus(false),
        refreshStats(false),
        Promise.resolve(window.refreshInsights?.()),
        Promise.resolve(window.updatePreviewVisibility?.()),
      ]);
    } finally {
      state.softMainBusy = false;
    }
  }

  async function hardRefreshMain() {
    if (authSetupPending()) return;
    enforceMainLayout();
    state.lastStatusMs = 0;
    await Promise.allSettled([
      refreshStatus(false),
      Promise.resolve(window.refreshInsights?.(true)),
    ]);

    if (!window.esSum) queueSafe(() => window.openSummaryStream?.());
    if (!window.esLogs) queueSafe(() => window.openLogStream?.());
    window.wallLoaded = false;
    try { await window.updatePreviewVisibility?.(); } catch {}

    if (typeof window.refreshSchedulingBanner === "function") {
      window.refreshSchedulingBanner();
    } else {
      window.addEventListener("sched-banner-ready", () => {
        try { window.refreshSchedulingBanner?.(); } catch {}
      }, { once: true });
    }
  }

  function setTabHeaderState(tab) {
    ["main", "watchlist", "snapshots", "editor", "settings"].forEach((name) => {
      byId(`tab-${name}`)?.classList.toggle("active", name === tab);
    });
  }

  function setPageVisibility(tab) {
    byId("ops-card")?.classList.toggle("hidden", tab !== "main");
    byId("stats-card")?.classList.toggle("hidden", tab !== "main");
    if (tab !== "main") byId("placeholder-card")?.classList.add("hidden");

    byId("page-watchlist")?.classList.toggle("hidden", tab !== "watchlist");
    byId("page-snapshots")?.classList.toggle("hidden", tab !== "snapshots");
    byId("page-editor")?.classList.toggle("hidden", tab !== "editor");
    byId("page-settings")?.classList.toggle("hidden", tab !== "settings");

    document.documentElement.dataset.tab = tab;
    if (document.body) document.body.dataset.tab = tab;
  }

  async function ensurePageModule(key, src, namespace, extra = {}) {
    return CW.PageLoader?.ensure?.({ key, src, namespace, ...extra });
  }

  async function hydrateSettingsPage() {
    try { await window.loadConfig?.(); } catch {}
    try { await window.ensureProvidersPaneReady?.(); } catch {}

    if (typeof window.loadScheduling === "function") {
      await window.loadScheduling();
    } else {
      window.addEventListener("sched-banner-ready", () => {
        try { window.loadScheduling?.(); } catch {}
      }, { once: true });
    }

    try {
      ensureScrobbler();
      setTimeout(ensureScrobbler, 200);
    } catch {}
  }

  async function showTab(name) {
    const tab = String(name || "main").toLowerCase();
    setTabHeaderState(tab);
    setPageVisibility(tab);
    document.dispatchEvent(new CustomEvent("tab-changed", { detail: { id: tab, tab } }));

    const layout = byId("layout");
    const logPanel = byId("log-panel");

    if (tab === "main") {
      enforceMainLayout();
      if (state.currentTab === "main") await softRefreshMain();
      else await hardRefreshMain();
      logPanel?.classList.remove("hidden");
      queueSafe(() => {
        if (byId("det-log") && !window.esDet) {
          try { window.openDetailsLog?.(); } catch {}
        }
      });
      state.currentTab = "main";
      return;
    }

    layout?.classList.add("single");
    layout?.classList.remove("full");
    logPanel?.classList.add("hidden");

    if (tab === "watchlist") {
      try {
        await ensurePageModule("watchlist", "/assets/js/watchlist.js", "Watchlist", {
          refreshEvent: "watchlist:refresh",
        });
      } catch (e) {
        console.warn("Watchlist load/refresh failed:", e);
      }
      state.currentTab = "watchlist";
      return;
    }

    if (tab === "snapshots") {
      try {
        await ensurePageModule("snapshots", "/assets/js/snapshots.js", "Snapshots", {
          refreshArgs: [true],
        });
      } catch (e) {
        console.warn("Snapshots load/refresh failed:", e);
      }
      state.currentTab = "snapshots";
      return;
    }

    if (tab === "settings") {
      await hydrateSettingsPage();
      state.currentTab = "settings";
      return;
    }

    state.currentTab = tab;
  }

  document.addEventListener("tab-changed", (ev) => {
    const tab = String(ev?.detail?.id || ev?.detail?.tab || "").toLowerCase();
    if (tab !== "main") return;
    enforceMainLayout();
    setTimeout(() => {
      try { window.openSummaryStream?.(); } catch {}
      try { window.openLogStream?.(); } catch {}
      try { window.UX?.refresh?.(); } catch {}
      try { recomputeRunDisabled(); } catch {}
    }, 0);
  });

  window.addEventListener("cw-auth-setup-pending", (ev) => {
    if (ev?.detail?.pending !== false) return;
    const tab = String(state.currentTab || document.documentElement?.dataset?.tab || document.body?.dataset?.tab || "main").toLowerCase();
    if (tab !== "main") return;
    queueSafe(() => {
      hardRefreshMain().catch(() => {});
    });
  });

  window.addEventListener("load", () => {
    if (mainBootLoadRefreshDone) return;
    mainBootLoadRefreshDone = true;
    if (authSetupPending()) return;
    const tab = String(state.currentTab || document.documentElement?.dataset?.tab || document.body?.dataset?.tab || "main").toLowerCase();
    if (tab !== "main") return;
    queueSafe(() => {
      hardRefreshMain().catch(() => {});
    });
  }, { once: true });

  let scrobblerInit = false;
  function ensureScrobbler() {
    if (scrobblerInit) return;
    const mount = byId("scrobble-mount") || byId("scrobbler");
    if (!mount) return;

    const configured = getConfiguredProviders();
    const sourceOk = configured.has("PLEX") || configured.has("EMBY") || configured.has("JELLYFIN");
    const sinkOk = configured.has("TRAKT") || configured.has("SIMKL") || configured.has("MDBLIST");
    if (!(sourceOk && sinkOk)) return;

    const start = () => {
      if (scrobblerInit) return;
      if (window.Scrobbler?.init) window.Scrobbler.init({ mountId: mount.id });
      else if (window.Scrobbler?.mount) window.Scrobbler.mount(mount, window._cfgCache || {});
      else return;
      scrobblerInit = true;
    };

    if (window.Scrobbler) {
      start();
      return;
    }

    let script = byId("scrobbler-js");
    if (!script) {
      script = document.createElement("script");
      script.id = "scrobbler-js";
      script.src = "/assets/js/scrobbler.js";
      script.defer = true;
      script.onload = start;
      script.onerror = () => console.warn("[scrobbler] script failed to load");
      document.head.appendChild(script);
    } else {
      script.onload = start;
    }
  }

  function toggleSection(id) {
    byId(id)?.classList.toggle("open");
  }

  function setBusy(on) {
    state.busy = !!on;
    window.busy = !!on;
    recomputeRunDisabled();
  }

  function escapeHtml(value) {
    return String(value || "").replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[ch] || ch));
  }

  function providerLabel(key) {
    try {
      return CW.ProviderMeta?.label?.(key) || String(key || "").trim().toUpperCase() || "?";
    } catch {
      return String(key || "").trim().toUpperCase() || "?";
    }
  }

  function featureEnabled(value) {
    if (value === true) return true;
    if (!value || typeof value !== "object") return false;
    return !!(value.enable ?? value.enabled);
  }

  function pairCanRun(pair) {
    if (!pair || pair.enabled === false) return false;
    const features = pair.features || {};
    const keys = ["watchlist", "ratings", "history", "progress", "playlists"];
    if (!pair.features) return true;
    return keys.some((key) => featureEnabled(features[key]));
  }

  function pairTitle(pair) {
    const source = providerLabel(pair.source);
    const target = providerLabel(pair.target);
    const sourceInst = String(pair.source_instance || "").trim();
    const targetInst = String(pair.target_instance || "").trim();
    const sourceLabel = sourceInst && sourceInst.toLowerCase() !== "default" ? `${source} · ${sourceInst}` : source;
    const targetLabel = targetInst && targetInst.toLowerCase() !== "default" ? `${target} · ${targetInst}` : target;
    return `${sourceLabel} → ${targetLabel}`;
  }

  function clampMenuToViewport(menu, margin = 10) {
    if (!menu?.getBoundingClientRect) return;
    menu.style.transform = "";
    const rect = menu.getBoundingClientRect();
    let dx = 0;
    if (rect.right > window.innerWidth - margin) dx -= rect.right - (window.innerWidth - margin);
    if (rect.left < margin) dx += margin - rect.left;
    if (dx) menu.style.transform = `translateX(${Math.round(dx)}px)`;
  }

  function portalMenuToBody(menu) {
    if (!menu) return;
    if (!window.__cwSyncMenuHome) window.__cwSyncMenuHome = { parent: menu.parentNode, next: menu.nextSibling };
    if (menu.parentNode !== document.body) document.body.appendChild(menu);
  }

  function restoreMenuHome(menu) {
    const home = window.__cwSyncMenuHome;
    if (!menu || !home?.parent || menu.parentNode === home.parent) return;
    if (home.next && home.next.parentNode === home.parent) home.parent.insertBefore(menu, home.next);
    else home.parent.appendChild(menu);
  }

  function positionSyncMenu(anchor, menu) {
    if (!anchor?.getBoundingClientRect || !menu) return;
    const rect = anchor.getBoundingClientRect();
    const gap = 10;
    const margin = 10;
    Object.assign(menu.style, {
      position: "fixed",
      left: "0px",
      top: "0px",
      right: "auto",
      bottom: "auto",
      transform: "",
      zIndex: "99999",
    });

    const width = Math.max(240, menu.offsetWidth || 0);
    const height = Math.max(120, menu.offsetHeight || 0);
    let left = rect.right - width;
    let top = rect.bottom + gap;
    if (left < margin) left = margin;
    if (left + width > window.innerWidth - margin) left = window.innerWidth - margin - width;
    if (top + height > window.innerHeight - margin) {
      const above = rect.top - gap - height;
      top = above >= margin ? above : Math.max(margin, window.innerHeight - margin - height);
    }
    menu.style.left = `${Math.round(left)}px`;
    menu.style.top = `${Math.round(top)}px`;
    clampMenuToViewport(menu, margin);
  }

  function removeSyncMenuListeners() {
    if (window.__cwSyncMenuOutside) {
      document.removeEventListener("mousedown", window.__cwSyncMenuOutside, true);
      window.__cwSyncMenuOutside = null;
    }
    if (window.__cwSyncMenuEsc) {
      document.removeEventListener("keydown", window.__cwSyncMenuEsc, true);
      window.__cwSyncMenuEsc = null;
    }
    if (window.__cwSyncMenuPos) {
      window.removeEventListener("resize", window.__cwSyncMenuPos, true);
      window.removeEventListener("scroll", window.__cwSyncMenuPos, true);
      window.__cwSyncMenuPos = null;
    }
  }

  function cwCloseSyncMenu() {
    const btn = byId("run-menu");
    const menu = byId("cw-sync-menu");
    if (!menu) return;
    menu.classList.add("hidden");
    Object.assign(menu.style, { transform: "", visibility: "", left: "", top: "", right: "", bottom: "", position: "", zIndex: "" });
    if (btn) btn.setAttribute("aria-expanded", "false");
    restoreMenuHome(menu);
    removeSyncMenuListeners();
  }

  function buildSyncMenuButton(label, onClick, extraClass = "") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `cw-menu-item${extraClass ? ` ${extraClass}` : ""}`;
    button.setAttribute("role", "menuitem");
    button.textContent = label;
    button.addEventListener("click", onClick);
    return button;
  }

  async function cwBuildSyncMenu() {
    const menu = byId("cw-sync-menu");
    if (!menu) return;
    menu.innerHTML = "";
    menu.appendChild(buildSyncMenuButton("Sync all", () => {
      cwCloseSyncMenu();
      runSync();
    }));

    let pairs = Array.isArray(window.cx?.pairs) && window.cx.pairs.length ? window.cx.pairs : await _getPairsFresh(false);
    const runnable = (Array.isArray(pairs) ? pairs : []).filter(pairCanRun);
    if (!runnable.length) {
      const empty = document.createElement("div");
      empty.className = "cw-sync-menu-empty";
      empty.textContent = "No enabled pairs";
      menu.appendChild(empty);
      return;
    }

    runnable.forEach((pair) => {
      const button = document.createElement("button");
      const mode = String(pair.mode || "").toLowerCase();
      const modeLabel = mode === "two-way" ? "two-way" : (mode === "one-way" ? "one-way" : "");
      button.type = "button";
      button.className = "cw-menu-item";
      button.setAttribute("role", "menuitem");
      button.innerHTML = `<span class="cw-sync-menu-title">${escapeHtml(pairTitle(pair))}</span>${modeLabel ? `<span class="cw-sync-menu-meta">${escapeHtml(modeLabel)}</span>` : ""}`;
      button.addEventListener("click", () => {
        cwCloseSyncMenu();
        runSync({ pair_id: String(pair.id || "").trim() });
      });
      menu.appendChild(button);
    });
  }

  async function cwToggleSyncMenu(ev) {
    try { ev?.preventDefault?.(); ev?.stopPropagation?.(); } catch {}
    const btn = byId("run-menu");
    const menu = byId("cw-sync-menu");
    if (!btn || !menu) return;
    if (!menu.classList.contains("hidden")) {
      cwCloseSyncMenu();
      return;
    }

    await cwBuildSyncMenu();
    portalMenuToBody(menu);
    menu.style.visibility = "hidden";
    menu.classList.remove("hidden");
    raf(() => {
      positionSyncMenu(btn, menu);
      menu.style.visibility = "";
    });
    btn.setAttribute("aria-expanded", "true");

    window.__cwSyncMenuPos = () => {
      const liveMenu = byId("cw-sync-menu");
      const liveBtn = byId("run-menu");
      if (!liveMenu || !liveBtn || liveMenu.classList.contains("hidden")) return;
      positionSyncMenu(liveBtn, liveMenu);
    };
    window.__cwSyncMenuOutside = (event) => {
      const target = event?.target;
      if (!target || target === btn || menu.contains(target)) return;
      cwCloseSyncMenu();
    };
    window.__cwSyncMenuEsc = (event) => {
      if (event?.key === "Escape") cwCloseSyncMenu();
    };

    window.addEventListener("resize", window.__cwSyncMenuPos, true);
    window.addEventListener("scroll", window.__cwSyncMenuPos, true);
    document.addEventListener("mousedown", window.__cwSyncMenuOutside, true);
    document.addEventListener("keydown", window.__cwSyncMenuEsc, true);
  }

  function resetSyncButtons(pairId) {
    [byId("run"), byId("run-menu")].forEach((button) => {
      if (!button) return;
      button.removeAttribute("disabled");
      button.setAttribute("aria-busy", "false");
      button.classList.remove("glass");
      button.title = button.id === "run"
        ? (pairId ? "Run synchronization (single pair)" : "Run synchronization")
        : "Sync options";
    });
    try { window.syncBar?.reset?.(); } catch {}
  }

  function clearDetailsLogBeforeRun() {
    try { window.resetDetailsSyncLog?.(); } catch {}
    const details = byId("details");
    const isVisible = !!(details && !details.classList.contains("hidden"));
    if (!isVisible) {
      try { window.closeDetailsLog?.(); } catch {}
      return;
    }
    if (!window.esDet) {
      try { window.openDetailsLog?.(); } catch {}
    }
  }

  async function runSync(opts) {
    if (state.busy) return;
    try { cwCloseSyncMenu(); } catch {}

    let pairId = "";
    if (typeof opts === "string") pairId = opts;
    else if (opts && typeof opts === "object") pairId = String(opts.pair_id || opts.pairId || opts.id || "");
    pairId = pairId.trim();

    setBusy(true);
    try {
      window.UX?.updateTimeline?.({ start: true, pre: false, post: false, done: false });
      window.UX?.updateProgress?.({ pct: 0 });
    } catch {}

    clearDetailsLogBeforeRun();

    try {
      const init = { method: "POST" };
      if (pairId) {
        init.headers = { "Content-Type": "application/json" };
        init.body = JSON.stringify({ pair_id: pairId });
      }

      const response = await fetch("/api/run", init);
      let payload = null;
      try { payload = await response.json(); } catch {}

      if (!response.ok || !payload || payload.ok !== true) {
        setSyncHeader("sync-bad", `Failed to start${payload?.error ? ` – ${payload.error}` : ""}`);
        resetSyncButtons(pairId);
        window.UX?.updateTimeline?.({ start: false, pre: false, post: false, done: false });
        return;
      }

      if (payload.skipped) {
        const message = payload.skipped === "no_pairs_configured"
          ? "No pairs configured — skipping sync"
          : `Sync skipped — ${payload.skipped}`;
        setSyncHeader("sync-warn", message);
        resetSyncButtons(pairId);
        window.UX?.updateTimeline?.({ start: false, pre: false, post: false, done: false });
        return;
      }
    } catch {
      setSyncHeader("sync-bad", "Failed to reach server");
      resetSyncButtons(pairId);
      window.UX?.updateTimeline?.({ start: false, pre: false, post: false, done: false });
    } finally {
      setBusy(false);
      recomputeRunDisabled();
      if (AUTO_STATUS) queueSafe(() => refreshStatus(false));
    }
  }

  function setStatsExpanded(expanded) {
    const card = byId("stats-card");
    if (!card) return;
    card.classList.toggle("collapsed", !expanded);
    card.classList.toggle("expanded", !!expanded);
    if (expanded) {
      try { refreshInsights(); } catch {}
    }
  }

  function isElementOpen(el) {
    if (!el) return false;
    if (el.classList?.contains("open") || el.classList?.contains("expanded") || el.classList?.contains("show")) return true;
    const style = window.getComputedStyle(el);
    return !(style.display === "none" || style.visibility === "hidden" || el.offsetHeight === 0);
  }

  function findDetailsButton() {
    return byId("btn-details")
      || document.querySelector('[data-action="details"], .btn-details')
      || Array.from(document.querySelectorAll("button")).find((btn) => String(btn.textContent || "").trim().toLowerCase() === "view details");
  }

  function findDetailsPanel() {
    return byId("sync-output") || byId("details") || document.querySelector('#sync-log, .sync-output, [data-pane="details"]');
  }

  function wireDetailsToStats() {
    const panel = findDetailsPanel();
    const btn = findDetailsButton();
    setStatsExpanded(isElementOpen(panel));
    btn?.addEventListener("click", () => setTimeout(() => setStatsExpanded(isElementOpen(panel)), 50));
    (byId("btn-sync") || document.querySelector('[data-action="sync"], .btn-sync'))?.addEventListener("click", () => setStatsExpanded(false));
  }

  async function fetchJSON() {
    if (window.Insights?.fetchJSON) return window.Insights.fetchJSON.apply(this, arguments);
    return null;
  }

  function scheduleInsights() {
    return window.Insights?.scheduleInsights?.apply(this, arguments);
  }

  async function refreshInsights() {
    return window.Insights?.refreshInsights?.apply(this, arguments);
  }

  function renderSparkline() {
    return window.Insights?.renderSparkline?.apply(this, arguments);
  }

  function animateNumber() {
    return window.Insights?.animateNumber?.apply(this, arguments);
  }

  function animateChart() {
    return window.Insights?.animateChart?.apply(this, arguments);
  }

  async function refreshStats(force = false) {
    if (window.Insights?.refreshStats) return window.Insights.refreshStats(force);
    return null;
  }

  function _setBarValues(now, week, month) {
    [
      [".bar.week", week],
      [".bar.month", month],
      [".bar.now", now],
    ].forEach(([selector, value]) => {
      const el = document.querySelector(selector);
      if (el) el.dataset.v = String(value);
    });
  }

  function _initStatsTooltip() {
    const chart = byId("stats-chart");
    const tip = byId("stats-tip");
    if (!chart || !tip) return;

    [
      [document.querySelector(".bar.week"), "Last Week"],
      [document.querySelector(".bar.month"), "Last Month"],
      [document.querySelector(".bar.now"), "Now"],
    ].forEach(([el, label]) => {
      if (!el) return;
      const show = (x, y) => {
        tip.textContent = `${label}: ${el.dataset.v || "0"} items`;
        tip.style.left = `${x}px`;
        tip.style.top = `${y}px`;
        tip.hidden = false;
        tip.classList.add("show");
      };
      el.addEventListener("mousemove", (ev) => {
        const rect = chart.getBoundingClientRect();
        show(ev.clientX - rect.left, ev.clientY - rect.top);
      });
      el.addEventListener("mouseleave", () => {
        tip.hidden = true;
        tip.classList.remove("show");
      });
      el.addEventListener("touchstart", (ev) => {
        const touch = ev.touches?.[0];
        if (!touch) return;
        const rect = chart.getBoundingClientRect();
        show(touch.clientX - rect.left, touch.clientY - rect.top);
      }, { passive: true });
      el.addEventListener("touchend", () => {
        tip.hidden = true;
        tip.classList.remove("show");
      }, { passive: true });
    });
  }

  function ensureMainUpdateSlot() {
    let slot = byId("st-main-update");
    if (slot) return slot;

    const syncButton = Array.from(document.querySelectorAll("button")).find((btn) => /synchroni[sz]e/i.test(btn.textContent || ""));
    const actionsRow = syncButton
      ? (syncButton.closest(".sync-actions, .cx-sync-actions, .actions, .row, .toolbar") || syncButton.parentElement)
      : document.querySelector(".sync-actions, .cx-sync-actions, .actions, .row, .toolbar");

    slot = document.createElement("div");
    slot.id = "st-main-update";
    slot.className = "hidden";
    if (actionsRow?.parentElement) {
      actionsRow.insertAdjacentElement("afterend", slot);
      return slot;
    }

    const previewHeader = Array.from(document.querySelectorAll("h2, .section-title")).find((node) => /watchlist\s*preview/i.test(node.textContent || ""));
    if (previewHeader?.parentElement) {
      previewHeader.insertAdjacentElement("beforebegin", slot);
      return slot;
    }

    const main = document.querySelector('#tab-main, [data-tab="main"], .page-main, main') || document.body;
    main.insertBefore(slot, main.firstChild || null);
    return slot;
  }

  function renderMainUpdatePill(hasUpdate, latest, url) {
    const host = ensureMainUpdateSlot();
    if (!host) return;
    if (hasUpdate && latest) {
      host.innerHTML = `<div class="pill"><span class="dot" aria-hidden="true"></span><span>Update <strong>${latest}</strong> available · <a href="${url}" target="_blank" rel="noopener">Release notes</a></span></div>`;
      host.classList.remove("hidden");
      return;
    }
    host.classList.add("hidden");
    host.textContent = "";
  }

  async function checkForUpdate() {
    try {
      const payload = await requestJSON("/api/version", {}, 15000);
      const current = String(payload.current ?? "0.0.0").trim();
      const latest = payload.latest ? String(payload.latest).trim() : null;
      const url = payload.html_url || "https://github.com/cenodude/CrossWatch/releases";
      const hasUpdate = !!payload.update_available;

      const versionEl = byId("app-version");
      if (versionEl) versionEl.textContent = `Version ${current}`;

      const badge = byId("st-update");
      if (badge) {
        if (hasUpdate && latest) {
          const changed = latest !== (badge.dataset.lastLatest || "");
          badge.classList.add("badge", "upd");
          badge.innerHTML = `<a href="${url}" target="_blank" rel="noopener" title="Open release page">Update ${latest} available</a>`;
          badge.classList.remove("hidden");
          if (changed) {
            badge.dataset.lastLatest = latest;
            badge.classList.remove("reveal");
            void badge.offsetWidth;
            badge.classList.add("reveal");
          }
        } else {
          badge.classList.add("hidden");
          badge.classList.remove("reveal");
          badge.textContent = "";
          badge.removeAttribute("aria-label");
          delete badge.dataset.lastLatest;
        }
      }

      renderMainUpdatePill(hasUpdate, latest, url);
    } catch (err) {
      console.debug("Version check failed:", err);
    }
  }

  (function initUpdateChecks() {
    if (window.__cwUpdateInitDone) return;
    window.__cwUpdateInitDone = true;
    const run = () => {
      try { checkForUpdate(); } catch (e) { console.debug("checkForUpdate failed:", e); }
    };
    onReady(run);
    setInterval(run, UPDATE_CHECK_INTERVAL_MS);
  })();

  function renderSummaryCore(summary) {
    state.currentSummary = summary;
    UI.summary = summary;

    const chips = {
      "chip-plex": summary.plex_post ?? summary.plex_pre,
      "chip-simkl": summary.simkl_post ?? summary.simkl_pre,
      "chip-dur": summary.duration_sec != null ? `${summary.duration_sec}s` : "–",
      "chip-exit": summary.exit_code != null ? String(summary.exit_code) : "–",
    };
    Object.entries(chips).forEach(([id, value]) => setText(id, value ?? "–"));

    if (summary.running) setSyncHeader("sync-warn", "Running…");
    else if (summary.exit_code === 0) setSyncHeader("sync-ok", String(summary.result || "").toUpperCase() === "EQUAL" ? "In sync " : "Synced ");
    else if (summary.exit_code != null) setSyncHeader("sync-bad", "Attention needed ⚠️");
    else setSyncHeader("sync-warn", "Idle — run a sync to see results");

    setText("det-cmd", summary.cmd || "–");
    setText("det-ver", summary.version || "–");
    setText("det-start", toLocal(summary.started_at));
    setText("det-finish", toLocal(summary.finished_at));
  }

  const previousRenderSummary = window.renderSummary;
  function renderSummary(summary) {
    try { previousRenderSummary?.(summary); } catch {}
    try { renderSummaryCore(summary); } catch {}
    try { refreshStats(false); } catch {}
  }
  window.renderSummary = renderSummary;

  async function copySummary(btn) {
    const summary = state.currentSummary || UI.summary;
    if (!summary) {
      flashCopy(btn, false, "No summary yet");
      return false;
    }

    const lines = [
      `Status: ${readText("sync-status-text", "—")}`,
      `Command: ${summary.cmd || "—"}`,
      `Version: ${summary.version || "—"}`,
      `Started: ${toLocal(summary.started_at)}`,
      `Finished: ${toLocal(summary.finished_at)}`,
      `Duration: ${summary.duration_sec != null ? `${summary.duration_sec}s` : "—"}`,
      `Exit: ${summary.exit_code != null ? summary.exit_code : "—"}`,
    ];

    try {
      await navigator.clipboard.writeText(lines.join("\n"));
      flashCopy(btn, true);
      return true;
    } catch {
      flashCopy(btn, false);
      return false;
    }
  }

  function setRefreshBusy(busy) {
    const btn = byId("btn-status-refresh");
    if (!btn) return;
    btn.disabled = !!busy;
    btn.classList.toggle("loading", !!busy);
  }

  window.openAbout = () => window.ModalRegistry.open("about");
  window.cxEnsureCfgModal = window.cxEnsureCfgModal || function () {};
  window.wireSecretTouch = window.wireSecretTouch || function wireSecretTouch(id) {
    const el = byId(id);
    if (!el || el.__wiredTouch) return;
    el.addEventListener("input", () => {
      el.dataset.touched = "1";
      el.dataset.masked = "0";
    });
    el.__wiredTouch = true;
  };

  window.maskSecret = function maskSecret(elOrId) {
    const el = typeof elOrId === "string" ? byId(elOrId) : elOrId;
    if (!el) return;
    el.dataset.masked = "0";
    el.dataset.loaded = "1";
    el.dataset.touched = "";
    el.dataset.clear = "";
  };


  function isPlaceholder(value, placeholder) {
    return String(value || "").trim().toUpperCase() === String(placeholder || "").trim().toUpperCase();
  }

  function isSettingsVisible() {
    const page = byId("page-settings");
    return !!(page && !page.classList.contains("hidden"));
  }

  function setBtnBusy(id, busy) {
    const el = byId(id);
    if (!el) return;
    el.disabled = !!busy;
    el.classList.toggle("opacity-50", !!busy);
  }

  function flashBtnOK(btnEl) {
    if (!btnEl) return;
    btnEl.disabled = true;
    btnEl.classList.add("copied");
    setTimeout(() => {
      btnEl.classList.remove("copied");
      btnEl.disabled = false;
    }, 700);
  }

  function normalizePairPayload(data, editingId = "") {
    const src = String(data?.source || "").trim();
    const dst = String(data?.target || "").trim();
    const mode = String(data?.mode || "one-way").toLowerCase() === "two-way" ? "two-way" : "one-way";
    const enabled = data?.enabled !== false;
    const features = data?.features || {};
    const basic = (value) => ({ enable: !!value?.enable, add: !!value?.add, remove: !!value?.remove });

    return {
      id: String(editingId || data?.id || "").trim() || undefined,
      source: src,
      target: dst,
      enabled,
      mode,
      features: {
        watchlist: basic(features.watchlist),
        ratings: {
          ...basic(features.ratings),
          types: Array.isArray(features.ratings?.types) ? features.ratings.types : undefined,
          mode: features.ratings?.mode,
          from_date: features.ratings?.from_date,
        },
        history: basic(features.history),
        playlists: basic(features.playlists),
      },
    };
  }

  function renderConnectionsSafe() {
    try { window.renderConnections?.(); } catch {}
  }

  async function loadPairs(force = false) {
    try {
      const list = await _getPairsFresh(!!force);
      window.cx = window.cx || {};
      window.cx.pairs = Array.isArray(list) ? list : [];
      renderConnectionsSafe();
      return window.cx.pairs;
    } catch (e) {
      console.warn("[cx] loadPairs failed", e);
      return [];
    }
  }

  async function deletePair(id) {
    if (!id) return false;
    try {
      if (typeof API.Pairs?.delete === "function") await API.Pairs.delete(id);
      else {
        const res = await fetch(`/api/pairs/${encodeURIComponent(id)}`, { method: "DELETE" });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      }
      _invalidatePairsCache();
      await loadPairs(true);
      return true;
    } catch (e) {
      console.warn("[cx] deletePair failed", e);
      return false;
    }
  }

  async function cxSavePair(data, editingId = "") {
    try {
      const payload = normalizePairPayload(data, editingId);
      if (typeof API.Pairs?.save === "function") await API.Pairs.save(payload);
      else {
        const id = payload.id;
        const url = id ? `/api/pairs/${encodeURIComponent(id)}` : "/api/pairs";
        const method = id ? "PUT" : "POST";
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      }
      _invalidatePairsCache();
      await loadPairs(true);
      return { ok: true };
    } catch (e) {
      console.warn("[cx] cxSavePair failed", e);
      return { ok: false, error: String(e?.message || e) };
    }
  }

  window.addEventListener("cx:open-modal", (ev) => {
    try {
      if (typeof window.cxOpenModalFor === "function") window.cxOpenModalFor(ev.detail || {});
    } catch (e) {
      console.warn("cx modal bridge failed", e);
    }
  });

  function fixFormLabels(root = document) {
    if (typeof DOM.fixFormLabels === "function") return DOM.fixFormLabels(root);
    let uid = 0;
    root.querySelectorAll("label").forEach((label) => {
      if (label.hasAttribute("for")) return;
      if (label.querySelector("input,select,textarea")) return;
      let control = label.nextElementSibling;
      while (control && !control.matches?.("input,select,textarea")) control = control.nextElementSibling;
      if (!control) control = label.parentElement?.querySelector?.("input,select,textarea");
      if (!control) return;
      if (!control.id) control.id = `auto_lbl_${++uid}`;
      label.setAttribute("for", control.id);
    });
  }

Object.assign(window, {
  _setVal: setValue,
  getConfiguredProviders,
  applySyncVisibility, scheduleApplySyncVisibility, bindSyncVisibilityObservers,
  _invalidatePairsCache, isWatchlistEnabledInPairs,
  loadStatusCache, renderConnectorStatus, refreshStatus, manualRefreshStatus,
  computeRedirectURI, recomputeRunDisabled, relTimeFromEpoch,
  showTab, toggleSection, runSync,
  copySummary, loadPairs, cxSavePair,
  cwToggleSyncMenu, DETAILS_MAX_LINES,
});

  onReady(() => {
    const authPendingAtReady = authSetupPending();
    try { fixFormLabels(); } catch {}
    try { wireDetailsToStats(); } catch {}
    try { scheduleInsights(); } catch {}
    try { refreshInsights(); } catch {}
    try { _initStatsTooltip(); } catch {}
    try { showTab("main"); } catch {}
    try { loadPairs(false); } catch {}
    try { window.mountMetadataProviders?.(); } catch {}
    try { window.cwSchedProviderEnsure?.(); } catch {}
    try { bindSyncVisibilityObservers(); } catch {}
    try { window.cwInitPendingProtoBanner?.(); } catch {}
    bootstrapStatusFromCache();

    if (authPendingAtReady) {
      Promise.resolve(window.__cwAuthBootstrapPromise)
        .catch(() => null)
        .finally(() => {
          if (authSetupPending()) return;
          const tab = String(state.currentTab || document.documentElement?.dataset?.tab || document.body?.dataset?.tab || "main").toLowerCase();
          if (tab !== "main") return;
          queueSafe(() => {
            hardRefreshMain().catch(() => {});
          });
        });
    }
  });
})();
