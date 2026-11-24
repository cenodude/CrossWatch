// assets/auth/auth.kitsu.js
// CrossWatch - Kitsu account authentication UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._kitsuAuthPatched) return;
  window._kitsuAuthPatched = true;
  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const profile = Shared.createProfileAdapter({ provider: "kitsu", configKey: "kitsu", label: "Kitsu",
    sectionId: "sec-kitsu", selectId: "kitsu_instance", storageKey: "cw.ui.kitsu.auth.instance.v1" });
  const pending = new Map();
  let generation = 0;
  const mappingDismissKey = "cw.ui.kitsu.animeMapping.dismissed.v1";
  let connected = false;
  let mappingStatus = null;
  let mappingBusy = false;
  let mappingRequest = 0;

  function renderMappingRecommendation() {
    const box = el("kitsu_mapping_recommendation");
    if (!box) return;
    let dismissed = false;
    try { dismissed = localStorage.getItem(mappingDismissKey) === "1"; } catch {}
    const enabled = mappingStatus?.enabled ?? window._cfgCache?.anime_mapping?.enabled ?? false;
    const error = String(mappingStatus?.error || "");
    box.classList.toggle("hidden", !connected || dismissed || (enabled && !error));
    box.classList.toggle("busy", mappingBusy);
    for (const id of ["kitsu_enable_mapping", "kitsu_dismiss_mapping"]) {
      if (el(id)) el(id).disabled = mappingBusy;
    }
    if (el("kitsu_enable_mapping")) el("kitsu_enable_mapping").textContent = mappingBusy ? T("connections.auth.kitsu.enabling") : T("connections.auth.enableMapping");
    const state = el("kitsu_mapping_recommendation_state");
    if (state) {
      state.textContent = mappingBusy ? T("connections.auth.downloadingMapping") : error;
      state.classList.toggle("hidden", !mappingBusy && !error);
    }
  }

  async function refreshMappingRecommendation() {
    if (mappingBusy) return;
    const current = ++mappingRequest;
    try {
      const r = await Shared.fetchJSON("/api/anime-mapping/status", { cache: "no-store" });
      if (current !== mappingRequest) return;
      if (!r.ok) throw new Error(T("connections.auth.checkMappingFailed"));
      mappingStatus = { ...r.data, error: r.data?.error ? r.data.message || T("connections.auth.loadMappingFailed") : "" };
    } catch (error) {
      if (current !== mappingRequest) return;
      mappingStatus = { ...mappingStatus, error: error.message || T("connections.auth.checkMappingFailed") };
    }
    renderMappingRecommendation();
  }

  async function enableMapping() {
    if (mappingBusy) return;
    ++mappingRequest;
    mappingBusy = true;
    renderMappingRecommendation();
    try {
      const r = await Shared.fetchJSON("/api/anime-mapping/settings", { method: "POST", cache: "no-store",
        headers: { "Content-Type": "application/json" }, body: JSON.stringify({ enabled: true }) });
      if (!r.ok || r.data?.ok === false) throw new Error(r.data?.message || r.data?.error || T("connections.auth.enableMappingFailed"));
      const cfg = window.CW?.Cache?.getCfg?.() || window._cfgCache || {};
      cfg.anime_mapping = r.data?.anime_mapping || { ...cfg.anime_mapping, enabled: true };
      window._cfgCache = cfg;
      window.CW?.Cache?.setCfg?.(cfg);
      mappingStatus = { ...r.data?.status, enabled: true, error: r.data?.bootstrap_error || "" };
      try { window.cwAnimeMappingRenderStatus?.(mappingStatus); } catch {}
      try { await window.cwAnimeMappingRefreshStatus?.(); } catch {}
      Shared.notify(mappingStatus.error || T("connections.auth.mappingEnabled"));
    } catch (error) {
      mappingStatus = { ...mappingStatus, error: error.message || T("connections.auth.enableMappingFailed") };
    } finally {
      mappingBusy = false;
      renderMappingRecommendation();
    }
  }

  function dismissMapping() {
    try { localStorage.setItem(mappingDismissKey, "1"); } catch {}
    renderMappingRecommendation();
  }

  function clearCredentials() {
    for (const id of ["kitsu_username", "kitsu_password"]) if (el(id)) el(id).value = "";
  }

  function setConn(ok, message) {
    connected = !!ok;
    Shared.setConnectLocked("kitsu_connect", ok);
    Shared.setStatus("kitsu_msg", ok, message || (ok ? T("connections.auth.connected") : T("connections.auth.notConnected")));
    renderMappingRecommendation();
    if (connected) void refreshMappingRecommendation();
  }

  async function hydrate() {
    const current = ++generation;
    connected = false;
    renderMappingRecommendation();
    profile.ensureUI(() => { void hydrate(); });
    clearCredentials();
    const staged = pending.get(profile.getInstance());
    if (staged) { setConn(true, T("connections.auth.connectedSave")); return; }
    try {
      const r = await Shared.fetchJSON(profile.api("/api/kitsu/status"), { cache: "no-store" });
      if (current === generation) setConn(!!r.data?.connected, r.data?.reauth_required ? T("connections.auth.kitsu.reconnect") : undefined);
    } catch { if (current === generation) setConn(false); }
  }

  async function connect() {
    const username = String(el("kitsu_username")?.value || "").trim();
    let password = String(el("kitsu_password")?.value || "");
    if (!username || !password) { Shared.notify(T("connections.auth.kitsu.enterCreds")); return; }
    const instance = profile.getInstance();
    const current = ++generation;
    const url = profile.api("/api/kitsu/connect");
    setConn(false, T("connections.auth.connecting"));
    try {
      const request = Shared.fetchJSON(url, { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }), cache: "no-store" });
      password = "";
      clearCredentials();
      const r = await request;
      if (current !== generation || instance !== profile.getInstance()) return;
      if (!r.ok || !r.data?.ok) throw new Error(r.data?.error || T("connections.auth.kitsu.failed"));
      if (r.data.saved) { pending.delete(instance); setConn(true); return; }
      pending.set(instance, r.data.connection);
      setConn(true, T("connections.auth.connectedSave"));
    } catch (error) {
      if (current === generation) setConn(false, error.message || T("connections.auth.kitsu.failed"));
    } finally { password = ""; if (current === generation) clearCredentials(); }
  }

  async function disconnect() {
    ++generation;
    pending.delete(profile.getInstance());
    clearCredentials();
    try {
      const r = await Shared.fetchJSON(profile.api("/api/kitsu/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || r.data?.ok === false) throw new Error(T("connections.auth.kitsu.disconnectFailed"));
      setConn(false);
      window.dispatchEvent(new CustomEvent("auth-changed"));
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (error) { Shared.notify(error.message); }
  }

  document.addEventListener("settings-collect", (event) => {
    const cfg = event.detail?.cfg;
    if (!cfg) return;
    for (const [instance, connection] of pending) {
      cfg.kitsu ||= {};
      const target = instance === "default" ? cfg.kitsu : ((cfg.kitsu.instances ||= {}), (cfg.kitsu.instances[instance] ||= {}));
      Object.assign(target, connection);
      delete target.password;
    }
  });
  document.addEventListener("cw-auth-modal-closed", (event) => {
    if (event.detail?.provider !== "kitsu") return;
    ++generation;
    pending.clear();
    clearCredentials();
    connected = false;
    ++mappingRequest;
    renderMappingRecommendation();
  });

  function boot() {
    for (const [id, handler] of [["kitsu_connect", connect], ["kitsu_disconnect", disconnect],
      ["kitsu_enable_mapping", enableMapping], ["kitsu_dismiss_mapping", dismissMapping]]) {
      const node = el(id);
      if (node && !node.__wired) { node.addEventListener("click", handler); node.__wired = true; }
    }
    for (const id of ["kitsu_username", "kitsu_password"]) {
      const node = el(id);
      if (node && !node.__wired) {
        node.addEventListener("input", () => { ++generation; pending.delete(profile.getInstance()); setConn(false); });
        node.__wired = true;
      }
    }
    void hydrate();
  }
  window.cwAuth ||= {};
  window.cwAuth.kitsu = { init: boot, hydrate };
  boot();
})();
