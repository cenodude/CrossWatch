// assets/auth/auth.kodi.js
(function () {
  if (window._kodiAuthPatched) return;
  window._kodiAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const Q = (s, r = document) => r.querySelector(s);
  const profile = Shared.createProfileAdapter({
    provider: "kodi",
    configKey: "kodi",
    label: "Kodi",
    sectionId: "sec-kodi",
    selectId: "kodi_instance",
    storageKey: "cw.ui.kodi.auth.instance.v1",
    title: "Select which Kodi media client this config applies to.",
  });
  const SUBTAB_KEY = "cw.ui.kodi.auth.subtab.v1";
  let H = new Set();
  let R = new Set();
  let P = new Set();
  let S = new Set();
  let lastLibraries = [];
  let wlHandle = null;
  let wlHost = null;
  let connected = false;

  function api(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  function cfgBlock(cfg, create) {
    return profile ? profile.cfgBlock(cfg, create) : {};
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  function ensureWhitelistTable() {
    if (window.cwWhitelistTable) return Promise.resolve(true);
    if (window.__cwWhitelistTableLoading) return window.__cwWhitelistTableLoading;
    window.__cwWhitelistTableLoading = new Promise((resolve) => {
      const s = document.createElement("script");
      s.src = `/assets/helpers/whitelist_table.js${window.__CW_VERSION__ ? `?v=${encodeURIComponent(window.__CW_VERSION__)}` : ""}`;
      s.async = true;
      s.onload = () => resolve(!!window.cwWhitelistTable);
      s.onerror = () => resolve(false);
      document.head.appendChild(s);
    });
    return window.__cwWhitelistTableLoading;
  }

  function setConn(ok, msg) {
    connected = !!ok;
    try { Shared.setConnectLocked("kodi_connect", !!ok); } catch {}
    syncTabs();
    return Shared.setStatus("kodi_msg", ok, msg || (ok ? "Connected" : "Not connected"));
  }

  const setFor = (fk) => ({ hist: H, rate: R, prog: P, scr: S }[fk]);

  function syncHidden() {
    const esc = (value) => String(value).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
    const write = (id, values) => {
      const node = el(id);
      if (node) node.innerHTML = Array.from(values || []).map((v) => `<option selected value="${esc(v)}">${esc(v)}</option>`).join("");
    };
    write("kodi_lib_history", H);
    write("kodi_lib_ratings", R);
    write("kodi_lib_progress", P);
    write("kodi_lib_scrobble", S);
    window.__kodiHydrated = true;
  }

  function selectSub(tab, opts = {}) {
    const root = Q('#sec-kodi .cw-meta-provider-panel[data-provider="kodi"]') || Q("#sec-kodi");
    if (!root) return;
    const sub = ["auth", "whitelist"].includes(String(tab || "").toLowerCase()) ? String(tab).toLowerCase() : "auth";
    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => btn.classList.toggle("active", btn.dataset.sub === sub));
    root.querySelectorAll(".cw-subpanel[data-sub]").forEach((panel) => panel.classList.toggle("active", panel.dataset.sub === sub));
    if (opts.persist !== false) {
      try { localStorage.setItem(SUBTAB_KEY, sub); } catch {}
    }
    if (sub === "whitelist") {
      renderLibraries(lastLibraries);
      void loadLibraries();
    }
  }

  function syncTabs() {
    const root = Q('#sec-kodi .cw-meta-provider-panel[data-provider="kodi"]') || Q("#sec-kodi");
    if (!root) return;
    try { Shared.applyMediaTabState(root, { configured: connected, connected, whitelistEnabled: connected, settingsEnabled: false }); } catch {}
  }

  function renderLibraries(libs) {
    if (Array.isArray(libs)) lastLibraries = libs;
    syncHidden();
    const host = el("kodi_libraries");
    if (!host) return;
    if (!window.cwWhitelistTable) {
      host.innerHTML = `<div class="cw-wl"><div class="cw-wl-foot"><div class="cw-wl-note">Empty = all libraries.</div><div class="cw-wl-foot-r"><button type="button" class="cw-wl-load" data-kodi-load-libs><span class="material-symbols-rounded" aria-hidden="true">sync</span>Load libraries</button></div></div></div>`;
      void ensureWhitelistTable().then(() => renderLibraries(lastLibraries));
      return;
    }
    if (wlHandle && wlHost === host) { wlHandle.render(); return; }
    wlHandle = null;
    wlHost = host;
    wlHandle = window.cwWhitelistTable.mount({
      host,
      features: [
        { key: "hist", label: "History" },
        { key: "rate", label: "Ratings" },
        { key: "prog", label: "Progress" },
        { key: "scr", label: "Scrobble" },
      ],
      getLibs: () => lastLibraries,
      isOn: (fk, id) => setFor(fk).has(String(id)),
      setOn: (fk, id, on) => { const s = setFor(fk); if (!s) return; if (on) s.add(String(id)); else s.delete(String(id)); },
      commit: syncHidden,
      load: async () => { await loadLibraries(true); },
    });
  }

  function liveQuery(url) {
    const parts = [];
    const server = txt(el("kodi_server")?.value || "");
    if (server) parts.push(`server=${encodeURIComponent(server)}`);
    if (el("kodi_verify_ssl")) parts.push(`verify_ssl=${el("kodi_verify_ssl").checked ? 1 : 0}`);
    return parts.length ? `${url}${url.includes("?") ? "&" : "?"}${parts.join("&")}` : url;
  }

  async function loadLibraries(force = false) {
    if (!force && lastLibraries.length) return renderLibraries(lastLibraries);
    const host = el("kodi_libraries");
    const btn = host?.querySelector?.(".cw-wl-load");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    try {
      const r = await fetchJSON(liveQuery(api("/api/kodi/libraries")), { cache: "no-store" });
      const libs = Array.isArray(r?.data?.libraries) ? r.data.libraries : (Array.isArray(r?.data) ? r.data : []);
      renderLibraries(libs);
    } catch {
      renderLibraries([]);
    } finally {
      const nextBtn = el("kodi_libraries")?.querySelector?.(".cw-wl-load");
      if (nextBtn) { nextBtn.disabled = false; nextBtn.classList.remove("busy"); }
    }
  }

  function friendlyError(data) {
    const reason = txt(data?.reason || data?.error);
    switch (reason) {
      case "missing_server": return "Enter a Kodi server URL";
      case "invalid_credentials": return "Kodi rejected the credentials";
      case "unreachable": return "Kodi server is unreachable";
      case "not_kodi": return "That server is not Kodi";
      case "version_too_old": return "Kodi 21.0 Omega or newer is required";
      case "jsonrpc_too_old": return "Kodi JSON-RPC 13.5.0 or newer is required";
      case "invalid_response": return "Kodi returned an unexpected response";
      default: return txt(data?.error) || "Kodi connection failed";
    }
  }

  async function hydrate() {
    profile?.ensureUI(() => { void hydrate(); });
    const cfg = window._cfgCache || await Shared.getConfig();
    const k = cfgBlock(cfg, true);

    if (el("kodi_server")) el("kodi_server").value = txt(k?.server || "");
    if (el("kodi_username")) el("kodi_username").value = txt(k?.username || "");
    if (el("kodi_verify_ssl")) el("kodi_verify_ssl").checked = !!k?.verify_ssl;
    H = new Set((k?.history?.libraries || []).map(String));
    R = new Set((k?.ratings?.libraries || []).map(String));
    P = new Set((k?.progress?.libraries || []).map(String));
    S = new Set((k?.scrobble?.libraries || []).map(String));
    syncHidden();
    renderLibraries(lastLibraries);

    try {
      const r = await fetchJSON(api("/api/kodi/status"), { cache: "no-store" });
      const data = r.data || {};
      Shared.maskSecret(el("kodi_password"), !!data.has_password);
      const ok = !!(r.ok && data.connected);
      const version = txt(data.kodi_version);
      setConn(ok, ok ? `Connected${version ? `: Kodi ${version}` : ""}` : "Not connected");
      if (ok) void loadLibraries();
    } catch {
      Shared.maskSecret(el("kodi_password"), false);
      setConn(false, "Not connected");
    }
  }

  async function onConnect() {
    const server = txt(el("kodi_server")?.value || "");
    const username = txt(el("kodi_username")?.value || "");
    const passInfo = Shared.readSecretField(el("kodi_password"));
    const verify_ssl = !!el("kodi_verify_ssl")?.checked;
    if (!server) {
      note("Enter a Kodi server URL");
      return;
    }

    try {
      setConn(false, "Connecting...");
      const r = await fetchJSON(api("/api/kodi/connect"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          server,
          username,
          password: passInfo.masked ? "********" : passInfo.value,
          verify_ssl,
        }),
        cache: "no-store",
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError(r.data || {}));
      Shared.maskSecret(el("kodi_password"), passInfo.hasValue);
      const version = txt(r.data?.kodi_version);
      setConn(true, `Connected${version ? `: Kodi ${version}` : ""}`);
      await loadLibraries(true);
      note("Kodi connected");
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      const msg = e && e.message ? e.message : "Kodi connection failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisconnect() {
    try {
      const r = await fetchJSON(api("/api/kodi/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");
      Shared.maskSecret(el("kodi_password"), false);
      lastLibraries = [];
      wlHandle = null;
      wlHost = null;
      renderLibraries([]);
      setConn(false, "Not connected");
      note("Kodi disconnected");
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      note("Kodi disconnect failed" + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    const c = el("kodi_connect");
    if (c && !c.__wired) { c.addEventListener("click", onConnect); c.__wired = true; }

    const d = el("kodi_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisconnect); d.__wired = true; }

    const p = el("kodi_password");
    if (p && !p.__wiredSecret) {
      Shared.wireSecretInput(p);
      p.__wiredSecret = true;
    }
    const host = el("kodi_libraries");
    if (host && !host.__kodiLoadFallback) {
      host.__kodiLoadFallback = true;
      host.addEventListener("click", (ev) => {
        if (ev.target?.closest?.("[data-kodi-load-libs]")) void loadLibraries(true);
      });
    }
    document.querySelectorAll('#sec-kodi .cw-subtile[data-sub]').forEach((btn) => {
      if (btn.__kodiTabWired) return;
      btn.__kodiTabWired = true;
      btn.addEventListener("click", () => selectSub(btn.dataset.sub));
    });
    let last = "auth";
    try { last = localStorage.getItem(SUBTAB_KEY) || "auth"; } catch {}
    selectSub(last, { persist: false });
  }

  function boot() {
    wire();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;

    const server = txt(el("kodi_server")?.value || "");
    const username = txt(el("kodi_username")?.value || "");
    const passInfo = Shared.readSecretField(el("kodi_password"));
    const verify_ssl = !!el("kodi_verify_ssl")?.checked;
    const hasWhitelist = H.size || R.size || P.size || S.size;
    if (!server && !username && !passInfo.hasValue && !verify_ssl && !hasWhitelist) return;

    cfg.kodi = cfg.kodi || {};
    let k = cfg.kodi;
    const inst = profile?.getInstance?.() || "default";
    if (inst !== "default") {
      k.instances = k.instances || {};
      k.instances[inst] = k.instances[inst] || {};
      k = k.instances[inst];
    }

    if (server) k.server = server;
    if (username) k.username = username;
    if (passInfo.hasValue && !passInfo.masked) k.password = passInfo.value;
    k.verify_ssl = verify_ssl;
    k.history = k.history || {};
    k.ratings = k.ratings || {};
    k.progress = k.progress || {};
    k.scrobble = k.scrobble || {};
    syncHidden();
    k.history.libraries = Array.from(H);
    k.ratings.libraries = Array.from(R);
    k.progress.libraries = Array.from(P);
    k.scrobble.libraries = Array.from(S);
  });

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.kodi = { init: boot, hydrate };
  boot();
})();
