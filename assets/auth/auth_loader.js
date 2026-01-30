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
    "sec-tmdb-sync": "tmdb",
    "sec-tautulli": "tautulli",
    "sec-anilist": "anilist",
  };

  const loaded = new Map();

  function _ver() {
    return String(w.__CW_BUILD__ || w.__CW_VERSION__ || "").trim();
  }

  function load(provider) {
    const key = String(provider || "").toLowerCase();
    if (loaded.has(key)) return loaded.get(key);

    const src = SCRIPTS[key];
    if (!src) return Promise.reject(new Error(`Unknown auth provider: ${key}`));

    const url = new URL(src, d.baseURI);
    const v = _ver();
    if (v) url.searchParams.set("v", v);

    const p = new Promise((resolve, reject) => {
      const s = d.createElement("script");
      s.src = url.toString();
      s.defer = true;
      s.onload = () => resolve(true);
      s.onerror = () => reject(new Error(`Failed to load auth script: ${key}`));
      d.head.appendChild(s);
    });

    loaded.set(key, p);
    return p;
  }

  async function ensureSection(secId) {
    const key = SECTION_TO_PROVIDER[String(secId || "")];
    if (!key) return false;
    await load(key);
    try { w.cwAuth?.[key]?.init?.(); } catch (_) {}
    return true;
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
        if (sec?.id) ensureSection(sec.id).catch(() => {});
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

    const mo = new MutationObserver(() => _scanOpen(host));
    mo.observe(host, { childList: true, subtree: true, attributes: true, attributeFilter: ["class"] });

    d.addEventListener(
      "tab-changed",
      (ev) => {
        if (!_isSettingsTab(ev)) return;
        _scanOpen(host);
      },
      true
    );

    _scanOpen(host);
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", attach, { once: true });
  else attach();

  w.cwAuthLoader = { load, ensureSection };
  w.cwLoadAuth = load;
  w.cwEnsureAuthSection = ensureSection;
})(window, document);
