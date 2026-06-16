// auth_loader.js - auth provider scripts
(function (w, d) {
  if (w.cwAuthLoader) return;

  const SCRIPTS = {
    plex: "/assets/auth/auth.plex.js",
    simkl: "/assets/auth/auth.simkl.js",
    trakt: "/assets/auth/auth.trakt.js",
    jellyfin: "/assets/auth/auth.jellyfin.js",
    emby: "/assets/auth/auth.emby.js",
    mdblist: "/assets/auth/auth.mdblist.js",
    publicmetadb: "/assets/auth/auth.publicmetadb.js",
    tmdb: "/assets/auth/auth.tmdb.js",
    tautulli: "/assets/auth/auth.tautulli.js",
    anilist: "/assets/auth/auth.anilist.js",
  };

  const SECTION_TO_PROVIDER = {
    "sec-plex": "plex",
    "sec-simkl": "simkl",
    "sec-trakt": "trakt",
    "sec-jellyfin": "jellyfin",
    "sec-emby": "emby",
    "sec-mdblist": "mdblist",
    "sec-publicmetadb": "publicmetadb",
    "sec-tmdb-sync": "tmdb",
    "sec-tautulli": "tautulli",
    "sec-anilist": "anilist",
  };

  const loaded = new Map();
  const initialized = new WeakSet();
  const pendingSections = new WeakMap();
  let sharedPromise = null;

  function _prefetch(host) {
    try {
      if (host?.querySelector?.("#sec-emby")) load("emby").catch(() => {});
    } catch (_) {}
  }

  function _ver() {
    return String(w.__CW_VERSION__ || "").trim();
  }

  function loadShared() {
    if (w.CW?.AuthShared) return Promise.resolve(true);
    if (sharedPromise) return sharedPromise;
    const url = new URL("/assets/auth/auth.shared.js", d.baseURI);
    const v = _ver();
    if (v) url.searchParams.set("v", v);
    sharedPromise = new Promise((resolve, reject) => {
      const s = d.createElement("script");
      s.src = url.toString();
      s.async = true;
      s.onload = () => resolve(true);
      s.onerror = () => reject(new Error("Failed to load shared auth helpers"));
      d.head.appendChild(s);
    });
    return sharedPromise;
  }

  function load(provider) {
    const key = String(provider || "").toLowerCase();
    if (loaded.has(key)) return loaded.get(key);

    const src = SCRIPTS[key];
    if (!src) return Promise.reject(new Error(`Unknown auth provider: ${key}`));

    const url = new URL(src, d.baseURI);
    const v = _ver();
    if (v) url.searchParams.set("v", v);

    const p = loadShared().then(() => new Promise((resolve, reject) => {
      const s = d.createElement("script");
      s.src = url.toString();
      // Dynamic scripts: let them execute as soon as they load.
      s.async = true;
      s.onload = () => resolve(true);
      s.onerror = () => reject(new Error(`Failed to load auth script: ${key}`));
      d.head.appendChild(s);
    }));

    loaded.set(key, p);
    return p;
  }

  async function ensureSection(secId) {
    const key = SECTION_TO_PROVIDER[String(secId || "")];
    if (!key) return false;
    const sec = d.getElementById(String(secId || ""));
    if (sec && initialized.has(sec)) return true;
    if (sec && pendingSections.has(sec)) return pendingSections.get(sec);

    const run = (async () => {
      await load(key);
      try { w.cwAuth?.[key]?.init?.(); } catch (_) {}
      if (sec) initialized.add(sec);
      return true;
    })();

    if (sec) {
      pendingSections.set(sec, run);
      run.finally(() => {
        try { pendingSections.delete(sec); } catch (_) {}
      }).catch(() => {});
    }
    return run;
  }

  function _isSettingsTab(ev) {
    const id = ev?.detail?.id ? String(ev.detail.id) : "";
    if (id) return /settings/i.test(id);
    const page = d.getElementById("page-settings");
    return !!(page && !page.classList.contains("hidden"));
  }

  function _scanOpen(host) {
    try {
      host.querySelectorAll(".section.open").forEach((sec) => {
        if (sec?.id && !initialized.has(sec) && !pendingSections.has(sec)) ensureSection(sec.id).catch(() => {});
      });
    } catch (_) {}
  }

  function attach() {
    const host = d.getElementById("auth-providers");
    if (!host || host.__cwAuthLoaderAttached) return;
    host.__cwAuthLoaderAttached = true;

    host.addEventListener(
      "click",
      (e) => {
        const head = e?.target?.closest?.(".head");
        if (!head) return;
        const sec = head.closest(".section");
        if (!sec?.id || !SECTION_TO_PROVIDER[sec.id]) return;
        const opening = !sec.classList.contains("open");
        if (!opening) return;

        ensureSection(sec.id).catch(() => {});
      },
      true
    );

    const mo = new MutationObserver(() => {
      _prefetch(host);
      _scanOpen(host);
    });
    mo.observe(host, { childList: true, subtree: true, attributes: true, attributeFilter: ["class"] });

    d.addEventListener(
      "tab-changed",
      (ev) => {
        if (!_isSettingsTab(ev)) return;
        _prefetch(host);
        _scanOpen(host);
      },
      true
    );

    _prefetch(host);
    _scanOpen(host);
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", attach, { once: true });
  else attach();

  w.cwAuthLoader = { load, ensureSection, loadShared };
  w.cwLoadAuth = load;
  w.cwEnsureAuthSection = ensureSection;
})(window, document);
