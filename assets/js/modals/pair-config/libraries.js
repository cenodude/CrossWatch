/* assets/js/modals/pair-config/libraries.js */
/* Provider library loading UI for the pair-config modal. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function createLibraryController({
  ID,
  hasPlex,
  hasJelly,
  hasEmby,
  getOpts,
  onLibrariesChanged,
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

  function fetchServerLibraries(kind) {
    let url = null;
    if (kind === "PLEX") url = "/api/plex/libraries";
    else if (kind === "JELLYFIN") url = "/api/jellyfin/libraries";
    else if (kind === "EMBY") url = "/api/emby/libraries";
    if (!url) return Promise.resolve([]);
    return fetch(url + "?cb=" + Date.now(), { cache: "no-store" })
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

  function filterLibsByServerConfig(libs, kind, feature, cfg) {
    try {
      let prov = "";
      if (kind === "PLEX") prov = "plex";
      else if (kind === "JELLYFIN") prov = "jellyfin";
      else if (kind === "EMBY") prov = "emby";
      if (!prov) return libs;
      const f = feature === "history" ? "history" : feature === "ratings" ? "ratings" : feature;
      const serverLibs = cfg?.[prov]?.[f]?.libraries;
      const ids = Array.isArray(serverLibs) ? serverLibs.map((x) => String(x)) : [];
      if (!ids.length) return libs;
      const set = new Set(ids);
      return (libs || []).filter((lib) => set.has(String(lib.key)));
    } catch {
      return libs;
    }
  }

  function fetchPairLibraries(kind, feature) {
    return Promise.all([fetchServerLibraries(kind), fetchPairServerConfig()]).then(([libs, cfg]) =>
      filterLibsByServerConfig(libs, kind, feature, cfg)
    );
  }

  function renderPairLibChips(state, kind, feature, libs) {
    let hostId = "";
    if (kind === "PLEX" && feature === "history") hostId = "plx-hist-libs";
    else if (kind === "PLEX" && feature === "ratings") hostId = "plx-rate-libs";
    else if (kind === "JELLYFIN" && feature === "history") hostId = "jf-hist-libs";
    else if (kind === "JELLYFIN" && feature === "ratings") hostId = "jf-rate-libs";
    else if (kind === "EMBY" && feature === "history") hostId = "em-hist-libs";
    else if (kind === "EMBY" && feature === "ratings") hostId = "em-rate-libs";
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
    const btnId = kind === "PLEX" ? "plx-libs-load" : kind === "JELLYFIN" ? "jf-libs-load" : "em-libs-load";
    const btn = ID(btnId);
    const load = () => {
      if (btn) {
        btn.disabled = true;
        btn.textContent = "Loading...";
      }
      Promise.all([
        fetchPairLibraries(kind, "history").then((libs) => {
          renderPairLibChips(state, kind, "history", libs);
        }),
        fetchPairLibraries(kind, "ratings").then((libs) => {
          renderPairLibChips(state, kind, "ratings", libs);
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
    if (!state._libsAutoload[kind]) {
      state._libsAutoload[kind] = true;
      load();
    }
  }

  function initPairLibraryUI(state) {
    if (!state._libsAutoload) state._libsAutoload = {};
    const hasPL = hasPlex(state);
    const hasJF = hasJelly(state);
    const hasEM = hasEmby(state);
    const plBox = ID("plx-pair-libs");
    const jfBox = ID("jf-pair-libs");
    const emBox = ID("em-pair-libs");
    if (plBox) plBox.style.display = hasPL ? "" : "none";
    if (jfBox) jfBox.style.display = hasJF ? "" : "none";
    if (emBox) emBox.style.display = hasEM ? "" : "none";

    if (hasPL) wireProviderLibraries(state, "PLEX");
    else if (ID("plx-libs-load")) ID("plx-libs-load").disabled = true;

    if (hasJF) wireProviderLibraries(state, "JELLYFIN");
    else if (ID("jf-libs-load")) ID("jf-libs-load").disabled = true;

    if (hasEM) wireProviderLibraries(state, "EMBY");
    else if (ID("em-libs-load")) ID("em-libs-load").disabled = true;
  }

  return {
    getFeatureLibraries,
    setFeatureLibraries,
    initPairLibraryUI,
    invalidatePairServerCfg,
  };
}
