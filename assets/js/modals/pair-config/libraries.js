/* assets/js/modals/pair-config/libraries.js */
/* Provider library loading UI for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function createLibraryController({
  ID,
  hasPlex,
  hasJelly,
  hasEmby,
  hasKodi,
  getOpts,
  onLibrariesChanged,
  instanceFor,
}) {
  let pairServerCfgPromise = null;
  let pairServerCfgAt = 0;
  const PAIR_CFG_TTL_MS = 30000;

  function getFeatureLibraries(state, feature, provider) {
    const f = getOpts(state, feature);
    const libs = f.libraries && typeof f.libraries === "object" ? f.libraries : {};
    if (!f.libraries) f.libraries = libs;
    const cur = libs[provider];
    const arr = Array.isArray(cur) ? cur.map((x) => String(x)) : [];
    return { config: f, libraries: libs, selected: arr };
  }

  function setFeatureLibraries(state, feature, provider, values) {
    const f = getOpts(state, feature);
    const libs = f.libraries && typeof f.libraries === "object" ? f.libraries : {};
    libs[provider] = Array.isArray(values) ? values.map((x) => String(x)) : [];
    f.libraries = libs;
    state.options[feature] = f;
    state.visited.add(feature);
  }

  function invalidatePairServerCfg() {
    pairServerCfgPromise = null;
    pairServerCfgAt = 0;
  }

  function providerKeyFor(kind) {
    if (kind === "PLEX") return "plex";
    if (kind === "JELLYFIN") return "jellyfin";
    if (kind === "EMBY") return "emby";
    if (kind === "KODI") return "kodi";
    return "";
  }

  function resolveInstance(state, kind) {
    try {
      const inst = instanceFor?.(state, kind);
      return String(inst || "default").trim() || "default";
    } catch {
      return "default";
    }
  }

  function fetchServerLibraries(state, kind) {
    const prov = providerKeyFor(kind);
    if (!prov) return Promise.resolve([]);
    const url = `/api/${prov}/libraries`;
    const inst = resolveInstance(state, kind);
    const qs = `?cb=${Date.now()}&instance=${encodeURIComponent(inst)}`;
    return fetch(url + qs, { cache: "no-store" })
      .then((r) => (r.ok ? r.json() : null))
      .then((j) => (j && Array.isArray(j.libraries) ? j.libraries : []))
      .catch(() => []);
  }

  function fetchPairServerConfig() {
    const now = Date.now();
    if (pairServerCfgPromise && now - pairServerCfgAt < PAIR_CFG_TTL_MS) return pairServerCfgPromise;
    pairServerCfgAt = now;
    pairServerCfgPromise = fetch("/api/config", { cache: "no-store" })
      .then((r) => (r.ok ? r.json() : {}))
      .catch(() => ({}));
    return pairServerCfgPromise;
  }

  function filterLibsByServerConfig(libs, kind, feature, cfg, instance) {
    try {
      const prov = providerKeyFor(kind);
      if (!prov) return libs;
      const f = feature === "history" ? "history" : feature === "ratings" ? "ratings" : feature;
      const root = cfg?.[prov];
      const inst = String(instance || "default");
      const block = inst === "default" ? root : root?.instances?.[inst];
      const serverLibs = block?.[f]?.libraries;
      const ids = Array.isArray(serverLibs) ? serverLibs.map((x) => String(x)) : [];
      if (!ids.length) return libs;
      const set = new Set(ids);
      return (libs || []).filter((lib) => set.has(String(lib.key)));
    } catch {
      return libs;
    }
  }

  function fetchPairLibraries(state, kind, feature) {
    const inst = resolveInstance(state, kind);
    return Promise.all([fetchServerLibraries(state, kind), fetchPairServerConfig()]).then(([libs, cfg]) =>
      filterLibsByServerConfig(libs, kind, feature, cfg, inst)
    );
  }

  function renderPairLibChips(state, kind, feature, libs) {
    let hostId = "";
    if (kind === "PLEX" && feature === "history") hostId = "plx-hist-libs";
    else if (kind === "PLEX" && feature === "ratings") hostId = "plx-rate-libs";
    else if (kind === "PLEX" && feature === "progress") hostId = "plx-prog-libs";
    else if (kind === "JELLYFIN" && feature === "history") hostId = "jf-hist-libs";
    else if (kind === "JELLYFIN" && feature === "ratings") hostId = "jf-rate-libs";
    else if (kind === "JELLYFIN" && feature === "progress") hostId = "jf-prog-libs";
    else if (kind === "EMBY" && feature === "history") hostId = "em-hist-libs";
    else if (kind === "EMBY" && feature === "ratings") hostId = "em-rate-libs";
    else if (kind === "EMBY" && feature === "progress") hostId = "em-prog-libs";
    else if (kind === "KODI" && feature === "history") hostId = "kodi-hist-libs";
    else if (kind === "KODI" && feature === "ratings") hostId = "kodi-rate-libs";
    else if (kind === "KODI" && feature === "progress") hostId = "kodi-prog-libs";
    const host = ID(hostId);
    if (!host) return;
    const info = getFeatureLibraries(state, feature, kind);
    const sel = new Set(info.selected);
    const list = Array.isArray(libs) && libs.length ? libs : info.selected.map((id) => ({ key: id, title: id }));
    host.innerHTML = "";
    list.forEach((lib) => {
      const key = String(lib.key);
      const title = lib.title || key;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "chip" + (sel.has(key) ? " on" : "");
      btn.textContent = title;
      btn.dataset.key = key;
      btn.addEventListener("click", () => {
        const cur = getFeatureLibraries(state, feature, kind);
        const next = new Set(cur.selected);
        if (next.has(key)) next.delete(key);
        else next.add(key);
        setFeatureLibraries(state, feature, kind, Array.from(next));
        renderPairLibChips(state, kind, feature, list);
        onLibrariesChanged?.(state, kind, feature);
      });
      host.appendChild(btn);
    });
    if (!list.length) {
      const empty = document.createElement("div");
      empty.className = "muted";
      empty.textContent = "No libraries";
      host.appendChild(empty);
    }
  }

  function wireProviderLibraries(state, kind) {
    const btnId =
      kind === "PLEX" ? "plx-libs-load" :
      kind === "JELLYFIN" ? "jf-libs-load" :
      kind === "EMBY" ? "em-libs-load" :
      kind === "KODI" ? "kodi-libs-load" : "";
    const btn = ID(btnId);
    const load = () => {
      if (btn) {
        btn.disabled = true;
        btn.textContent = "Loading...";
      }
      Promise.all([
        fetchPairLibraries(state, kind, "history").then((libs) => {
          renderPairLibChips(state, kind, "history", libs);
        }),
        fetchPairLibraries(state, kind, "ratings").then((libs) => {
          renderPairLibChips(state, kind, "ratings", libs);
        }),
        fetchPairLibraries(state, kind, "progress").then((libs) => {
          renderPairLibChips(state, kind, "progress", libs);
        }),
      ]).finally(() => {
        if (btn) {
          btn.disabled = false;
          btn.textContent = "Load libraries";
        }
      });
    };

    if (btn && !btn.__wired) {
      btn.__wired = true;
      btn.addEventListener("click", load);
    }
    const autoKey = `${kind}:${resolveInstance(state, kind)}`;
    if (!state._libsAutoload[autoKey]) {
      state._libsAutoload[autoKey] = true;
      load();
    }
  }

  function initPairLibraryUI(state) {
    if (!state._libsAutoload) state._libsAutoload = {};
    const hasPL = hasPlex(state);
    const hasJF = hasJelly(state);
    const hasEM = hasEmby(state);
    const hasKO = hasKodi(state);
    const plBox = ID("plx-pair-libs");
    const jfBox = ID("jf-pair-libs");
    const emBox = ID("em-pair-libs");
    const koBox = ID("kodi-pair-libs");
    if (plBox) plBox.style.display = hasPL ? "" : "none";
    if (jfBox) jfBox.style.display = hasJF ? "" : "none";
    if (emBox) emBox.style.display = hasEM ? "" : "none";
    if (koBox) koBox.style.display = hasKO ? "" : "none";

    if (hasPL) wireProviderLibraries(state, "PLEX");
    else if (ID("plx-libs-load")) ID("plx-libs-load").disabled = true;

    if (hasJF) wireProviderLibraries(state, "JELLYFIN");
    else if (ID("jf-libs-load")) ID("jf-libs-load").disabled = true;

    if (hasEM) wireProviderLibraries(state, "EMBY");
    else if (ID("em-libs-load")) ID("em-libs-load").disabled = true;

    if (hasKO) wireProviderLibraries(state, "KODI");
    else if (ID("kodi-libs-load")) ID("kodi-libs-load").disabled = true;
  }

  return {
    getFeatureLibraries,
    setFeatureLibraries,
    initPairLibraryUI,
    invalidatePairServerCfg,
  };
}
