// assets/auth/auth.silo.js
// CrossWatch - Silo Auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._siloAuthPatched) return;
  window._siloAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const fetchJSON = Shared.fetchJSON;
  const profile = Shared.createProfileAdapter({
    provider: "silo",
    configKey: "silo",
    label: "Silo",
    sectionId: "sec-silo",
    selectId: "silo_instance",
    storageKey: "cw.ui.silo.auth.instance.v1",
  });

  let connected = false;
  let authenticated = false;
  let profileSelectionReady = false;
  let savedProfileId = "";
  let profiles = [];
  let poller = null;
  let expiryTimer = null;
  const LIB_FEATURES = { hist: "history", rate: "ratings", prog: "progress", coll: "collection", scr: "scrobble" };
  const libSets = { hist: new Set(), rate: new Set(), prog: new Set(), coll: new Set(), scr: new Set() };
  let libraries = [];
  let librariesHydrated = false;
  let wlHandle = null;

  function api(path) {
    return profile.api(path);
  }

  function jsonPost(path, body) {
    return fetchJSON(api(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify(body || {}),
    });
  }

  function selectedProfile() {
    const id = txt(el("silo_profile_select")?.value);
    return profiles.find((row) => String(row.id) === id) || null;
  }

  function notifyModalState() {
    const panel = document.getElementById("sec-silo")?.closest?.(".cw-connection-modal-panel")
      || document.querySelector?.(".cw-connection-modal-panel[data-cw-connection-provider='silo'], .cw-connection-modal-panel[data-provider='silo']");
    const save = panel?.querySelector?.(".cw-connection-footer-save");
    if (!save) return;
    const chosen = selectedProfile();
    const pinOk = !chosen?.has_pin || String(chosen.id) === savedProfileId || !!txt(el("silo_profile_pin")?.value);
    const canSave = connected || (authenticated && !!chosen && pinOk);
    save.disabled = !canSave;
    save.classList.toggle("is-disabled", !canSave);
    if (canSave) save.removeAttribute("title");
    else save.title = chosen?.has_pin ? T("connections.auth.enterPinBefore") : T("connections.auth.connectBeforeSave");
  }

  function syncConnectLocked() {
    try { Shared.setConnectLocked("silo_connect", connected || authenticated); } catch {}
  }

  function setStatus(ok, msg) {
    const node = el("silo_msg");
    if (!node) return;
    node.classList.remove("hidden", "ok", "warn");
    node.classList.add(ok ? "ok" : "warn");
    node.textContent = msg || (ok ? T("connections.auth.connected") : T("connections.auth.notConnected"));
    notifyModalState();
  }

  function hideStatus() {
    const node = el("silo_msg");
    if (!node) return;
    node.classList.add("hidden");
    node.textContent = "";
    notifyModalState();
  }

  function formatCode(code) {
    const digits = txt(code).replace(/[^0-9A-Za-z]/g, "");
    return digits.length === 8 ? `${digits.slice(0, 4)} ${digits.slice(4)}` : (txt(code) || "---- ----");
  }

  function setLoginState(show, data) {
    el("silo_login_state")?.classList.toggle("hidden", !show);
    el("silo_cancel")?.classList.toggle("hidden", !show);
    if (!show) {
      stopExpiryTimer();
      return;
    }
    const code = txt(data?.user_code);
    if (el("silo_code")) el("silo_code").textContent = formatCode(code);
    if (el("silo_code_input")) el("silo_code_input").value = code.replace(/[^0-9A-Za-z]/g, "");
    const link = el("silo_login_url");
    const url = txt(data?.verification_uri_complete || data?.verification_uri);
    if (link) link.href = url || "#";
    startExpiryTimer(data?.expires_at);
  }

  function updateExpiry(expiresAt) {
    const node = el("silo_expiry");
    if (!node) return;
    const exp = Number(expiresAt || 0);
    if (!exp) {
      node.textContent = "";
      return;
    }
    const left = Math.max(0, Math.floor(exp - Date.now() / 1000));
    node.textContent = left ? T("connections.auth.expiresIn", {time: `${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")}`}) : T("connections.auth.expired");
  }

  function stopExpiryTimer() {
    if (expiryTimer) clearInterval(expiryTimer);
    expiryTimer = null;
  }

  function startExpiryTimer(expiresAt) {
    stopExpiryTimer();
    const exp = Number(expiresAt || 0);
    updateExpiry(exp);
    if (!exp) return;
    expiryTimer = setInterval(() => {
      updateExpiry(exp);
      if (exp <= Math.floor(Date.now() / 1000)) stopExpiryTimer();
    }, 1000);
  }

  function setPolling(text) {
    const node = el("silo_polling");
    if (node) node.textContent = text || "";
  }

  function syncServerUI() {
    const locked = connected || authenticated;
    const url = el("silo_server_url");
    if (url) {
      url.disabled = locked;
      url.title = locked ? T("connections.auth.gen.disconnectToChange", {provider: "Silo"}) : "";
    }
    const ssl = el("silo_verify_ssl");
    if (ssl) ssl.disabled = locked;
  }

  function renderServer(data) {
    const url = el("silo_server_url");
    if (url && (txt(data?.server_url) || !url.value)) url.value = txt(data?.server_url);
    const ssl = el("silo_verify_ssl");
    if (ssl) ssl.checked = data?.verify_ssl === true;
    syncServerUI();
  }

  function syncPinField() {
    const chosen = selectedProfile();
    const needsPin = !!chosen?.has_pin && String(chosen.id) !== savedProfileId;
    el("silo_pin_wrap")?.classList.toggle("hidden", !needsPin);
    if (!needsPin && el("silo_profile_pin")) el("silo_profile_pin").value = "";
    notifyModalState();
  }

  function renderProfiles(rows, selectedId, forceShow) {
    const wrap = el("silo_profile_state");
    const sel = el("silo_profile_select");
    if (!wrap || !sel) return;
    profiles = Array.isArray(rows) ? rows : [];
    savedProfileId = txt(selectedId || "");
    const effective = savedProfileId || (profiles.length === 1 ? String(profiles[0].id) : "");
    wrap.classList.toggle("hidden", !(connected || profileSelectionReady || forceShow));
    sel.innerHTML = "";
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = profiles.length ? (effective ? T("connections.auth.profileUnavailableSel") : T("connections.auth.gen.selectProfile", {provider: "Silo"})) : T("connections.auth.gen.noProfiles", {provider: "Silo"});
    placeholder.disabled = true;
    placeholder.selected = true;
    sel.appendChild(placeholder);
    profiles.forEach((row) => {
      const option = document.createElement("option");
      option.value = String(row.id);
      const tags = [row.is_primary ? "primary" : "", row.has_pin ? "PIN" : "", row.is_child ? "child" : ""].filter(Boolean);
      option.textContent = tags.length ? `${row.name} (${tags.join(", ")})` : String(row.name);
      sel.appendChild(option);
    });
    if (effective && profiles.some((row) => String(row.id) === effective)) sel.value = effective;
    if (!sel.__wiredSiloProfileChange) {
      sel.addEventListener("change", syncPinField);
      sel.__wiredSiloProfileChange = true;
    }
    syncPinField();
  }

  async function refreshProfiles(statusData) {
    if (!statusData?.authenticated) {
      renderProfiles([], statusData?.profile_id, false);
      return;
    }
    const r = await fetchJSON(api("/api/silo/profiles"), { cache: "no-store" });
    renderProfiles(r.ok && r.data?.ok ? (r.data.profiles || []) : [], statusData?.profile_id, true);
  }

  async function refresh() {
    try {
      const r = await fetchJSON(api("/api/silo/status"), { cache: "no-store" });
      const data = r.data || {};
      connected = !!(r.ok && data.connected);
      authenticated = !!(r.ok && data.authenticated);
      profileSelectionReady = connected || authenticated;
      syncConnectLocked();
      renderServer(data);
      if (data.reconnect_required) setStatus(false, T("connections.auth.gen.sessionEndedDisconnect", {provider: "Silo"}));
      else if (connected && data.profile_pin_required) setStatus(false, T("connections.auth.gen.enterPinAgain", {provider: "Silo"}));
      else if (connected) setStatus(true, data.profile_name ? T("connections.auth.connectedProfile", {provider: "Silo", profile: data.profile_name}) : T("connections.auth.gen.connected", {provider: "Silo"}));
      else if (authenticated) setStatus(false, T("connections.auth.gen.chooseProfile", {provider: "Silo"}));
      else hideStatus();
      await refreshProfiles(data);
      if (connected && data.profile_pin_required) {
        savedProfileId = "";
        syncPinField();
      }
    } catch {
      connected = false;
      authenticated = false;
      syncConnectLocked();
      setStatus(false, T("connections.auth.gen.statusFailed", {provider: "Silo"}));
    }
  }

  function friendlyError(code) {
    switch (String(code || "")) {
      case "invalid_url": return T("connections.auth.gen.validServerUrl", {provider: "Silo"});
      case "connection_failed": return T("connections.auth.gen.reachServer", {provider: "Silo"});
      case "ssl_error": return T("connections.auth.sslFailed");
      case "not_silo": return T("connections.auth.gen.notServer", {provider: "Silo"});
      case "http_error": return T("connections.auth.gen.serverReturnedError", {provider: "Silo"});
      case "device_login_unavailable": return T("connections.auth.gen.deviceDisabled", {provider: "Silo"});
      case "disconnect_first": return T("connections.auth.gen.disconnectBefore", {provider: "Silo"});
      case "rate_limited": return T("connections.auth.gen.rateLimitingSignIn", {provider: "Silo"});
      case "service_unavailable": return T("connections.auth.gen.serviceUnavailable", {provider: "Silo"});
      case "reconnect_required": return T("connections.auth.gen.sessionEnded", {provider: "Silo"});
      case "authentication_failed": return T("connections.auth.gen.authenticationFailed", {provider: "Silo"});
      case "profile_unavailable": return T("connections.auth.gen.profileUnavailable", {provider: "Silo"});
      case "pin_required": return T("connections.auth.enterPin");
      case "invalid_pin": return T("connections.auth.incorrectPin");
      case "pin_locked": return T("connections.auth.pinLocked");
      case "invalid_response": return T("connections.validation.returnedUnexpected", {provider: "Silo"});
      case "denied": return T("connections.auth.gen.deniedOn", {provider: "Silo"});
      case "canceled": return T("connections.auth.silo.canceled");
      case "consumed": return T("connections.auth.silo.consumed");
      default: return T("connections.auth.gen.requestFailedShort", {provider: "Silo"});
    }
  }

  function stopPendingLogin(clearStatus) {
    if (poller) poller.stop();
    setLoginState(false);
    setPolling("");
    if (clearStatus && !connected && !authenticated) hideStatus();
  }

  async function cancelLogin() {
    stopPendingLogin(true);
    try { await jsonPost("/api/silo/device/cancel", {}); } catch {}
  }

  function ensurePoller() {
    if (poller) return poller;
    poller = Shared.createDevicePoll({
      minIntervalMs: 3000,
      maxTotalMs: 900000,
      url: () => api("/api/silo/device/poll"),
      classify(status, data) {
        const state = txt(data?.status).toLowerCase();
        if (data?.ok || state === "authorized") return { state: "authorized" };
        if (state === "expired") return { state: "expired" };
        if (state === "slow_down") return { state: "slow_down" };
        if (state === "service_unavailable") return { state: "server" };
        if (state && state !== "pending") return { state: "terminal", message: state };
        return { state: "pending", intervalMs: Math.max(3000, Number(data?.interval || 3) * 1000) };
      },
      onPending(data) {
        setPolling(data?.opened ? T("connections.auth.gen.codeOpened", {provider: "Silo"}) : T("connections.auth.waitingApproval"));
      },
      async onAuthorized(data) {
        setLoginState(false);
        setPolling("");
        authenticated = true;
        profileSelectionReady = true;
        note(T("connections.auth.gen.signInApproved", {provider: "Silo"}));
        try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
        await refresh();
        if (!connected) setStatus(false, T("connections.auth.gen.chooseProfile", {provider: "Silo"}));
      },
      onExpired() {
        setLoginState(false);
        setPolling("");
        setStatus(false, T("connections.auth.gen.codeExpiredConnect", {provider: "Silo"}));
      },
      onTerminal(verdict) {
        setLoginState(false);
        setStatus(false, friendlyError(verdict?.message));
      },
      onTimeout() {
        setLoginState(false);
        setStatus(false, T("connections.auth.gen.signInTimedOut", {provider: "Silo"}));
      },
    });
    return poller;
  }

  async function startLogin() {
    const serverUrl = txt(el("silo_server_url")?.value).trim();
    if (!serverUrl) {
      setStatus(false, friendlyError("invalid_url"));
      return;
    }
    const win = Shared.openWaitWindow("Silo", T("connections.auth.gen.contactingServer", {provider: "Silo"}), "SILO");
    try {
      profileSelectionReady = false;
      renderProfiles([], "", false);
      setStatus(false, T("connections.auth.gen.contactingServerShort", {provider: "Silo"}));
      const r = await jsonPost("/api/silo/device/start", { server_url: serverUrl, verify_ssl: !!el("silo_verify_ssl")?.checked });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(String(data.error || "start_failed"));
      setLoginState(true, data);
      setStatus(false, T("connections.auth.gen.waitingApproval", {provider: "Silo"}));
      setPolling(T("connections.auth.waitingApproval"));
      const url = txt(data.verification_uri_complete || data.verification_uri);
      if (url && win && !win.closed) {
        Shared.showWaitCode(win, { key: "SILO", label: "Silo", code: txt(data.user_code), url });
      } else if (url) {
        try { window.open(url, "_blank", "noopener"); } catch {}
        note(T("connections.auth.silo.popup"));
      } else {
        try { if (win && !win.closed) win.close(); } catch {}
      }
      ensurePoller().start({ intervalMs: Math.max(3000, Number(data.interval || 3) * 1000), deadlineMs: Number(data.expires_at || 0) * 1000 });
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch {}
      setStatus(false, friendlyError(e?.message));
      note(friendlyError(e?.message));
    }
  }

  async function disconnect() {
    try {
      ensurePoller().stop();
      const r = await fetchJSON(api("/api/silo/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || r.data?.ok === false) throw new Error(String(r.data?.error || "disconnect_failed"));
      connected = false;
      authenticated = false;
      profileSelectionReady = false;
      syncConnectLocked();
      syncServerUI();
      stopPendingLogin(false);
      renderProfiles([], "", false);
      setStatus(false, T("connections.auth.gen.disconnected", {provider: "Silo"}));
      note(T("connections.auth.gen.disconnected", {provider: "Silo"}));
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    } catch {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Silo"}));
    }
  }

  async function saveSelectedProfile(opts = {}) {
    const chosen = selectedProfile();
    if (!chosen) return false;
    if (connected && savedProfileId && String(savedProfileId) === String(chosen.id)) return true;
    const pin = txt(el("silo_profile_pin")?.value);
    const r = await jsonPost("/api/silo/profile/select", { profile_id: chosen.id, pin: chosen.has_pin ? pin : undefined });
    if (!r.ok || r.data?.ok === false) {
      const msg = friendlyError(r.data?.error || "profile_select_failed");
      setStatus(false, msg);
      throw new Error(msg);
    }
    if (el("silo_profile_pin")) el("silo_profile_pin").value = "";
    connected = true;
    authenticated = true;
    profileSelectionReady = true;
    savedProfileId = String(chosen.id);
    syncPinField();
    setStatus(true, T("connections.auth.connectedProfile", {provider: "Silo", profile: txt(r.data?.profile_name || chosen.name)}));
    try { window.invalidateConfigCache?.(); } catch {}
    try { window.CW?.Cache?.invalidate?.("config"); } catch {}
    setTimeout(async () => {
      try { window.refreshStatus?.(true); } catch {}
      try { await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true); } catch {}
      try { window.manualRefreshStatus?.(); } catch {}
    }, 0);
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    if (!opts.silent) note(T("connections.auth.gen.profileSaved", {provider: "Silo"}));
    return true;
  }

  function ensureInstanceUI() {
    profile.ensureUI(() => {
      stopPendingLogin(true);
      if (el("silo_server_url")) el("silo_server_url").value = "";
      librariesHydrated = false;
      libraries = [];
      wlHandle = null;
      void refresh();
    });
  }

  function wire() {
    ensureInstanceUI();
    const bind = (id, handler) => {
      const node = el(id);
      if (node && !node.__wired) { node.addEventListener("click", handler); node.__wired = true; }
    };
    bind("silo_connect", startLogin);
    bind("silo_cancel", cancelLogin);
    bind("silo_disconnect", disconnect);
    const copy = el("silo_code_copy");
    if (copy && !copy.__wired) {
      copy.addEventListener("click", (ev) => {
        ev.preventDefault();
        void Shared.copyField("silo_code_input", copy, { emptyMessage: T("connections.auth.nothingToCopy") });
      });
      copy.__wired = true;
    }
    const pin = el("silo_profile_pin");
    if (pin && !pin.__wired) { pin.addEventListener("input", notifyModalState); pin.__wired = true; }
  }

  function mountUsers() {
    const host = el("silo_sync_users");
    if (!host) return;
    if (!connected) {
      host.__cwSyncUsersState = null;
      host.innerHTML = `<div class="cw-wl cw-su"><div class="cw-wl-empty">${TH("connections.auth.chooseBeforeUsers", {provider: "Silo"})}</div></div>`;
      return;
    }
    try { window.CW?.SyncUsers?.mount(host, "silo", profile.getInstance ? profile.getInstance() : "default"); } catch {}
  }

  function cfgBlock(cfg) {
    return profile.cfgBlock ? profile.cfgBlock(cfg, true) : {};
  }

  function hydrateLibraries(cfg) {
    const block = cfgBlock(cfg || window.__cfg || {});
    Object.entries(LIB_FEATURES).forEach(([fk, feature]) => {
      libSets[fk] = new Set(((block[feature] || {}).libraries || []).map(String));
    });
    librariesHydrated = true;
  }

  async function loadLibraries() {
    const r = await fetchJSON(api("/api/silo/libraries"), { cache: "no-store" });
    libraries = r.ok && r.data?.ok && Array.isArray(r.data.libraries) ? r.data.libraries : [];
    wlHandle?.render?.();
  }

  async function mountWhitelist() {
    const host = el("silo_lib_matrix");
    if (!host) return;
    if (!connected) {
      wlHandle = null;
      host.innerHTML = `<div class="cw-wl"><div class="cw-wl-empty">${TH("connections.auth.chooseBeforeLibs", {provider: "Silo"})}</div></div>`;
      return;
    }
    if (!librariesHydrated) {
      let cfg = window.__cfg && Object.keys(window.__cfg).length ? window.__cfg : null;
      if (!cfg) {
        const r = await fetchJSON("/api/config", { cache: "no-store" });
        cfg = r.ok ? (r.data || {}) : {};
      }
      hydrateLibraries(cfg);
    }
    if (!window.cwWhitelistTable) return;
    if (!wlHandle || !host.childElementCount) {
      host.innerHTML = "";
      wlHandle = window.cwWhitelistTable.mount({
        host,
        features: [{ key: "hist", label: T("connections.auth.feat.history") }, { key: "rate", label: T("connections.auth.feat.ratings") }, { key: "prog", label: T("connections.auth.feat.progress") }, { key: "coll", label: T("connections.auth.feat.collections") }, { key: "scr", label: T("connections.auth.feat.scrobble"), title: T("connections.auth.feat.scrobbleWatcherTitle") }],
        getLibs: () => libraries,
        isOn: (fk, id) => libSets[fk].has(String(id)),
        selectedIds: () => Object.values(libSets).flatMap((set) => [...set]),
        setOn: (fk, id, on) => { if (on) libSets[fk].add(String(id)); else libSets[fk].delete(String(id)); },
        commit: () => {},
        load: loadLibraries,
      });
    }
    if (!libraries.length) await loadLibraries();
  }

  function mergeSiloIntoCfg(cfg) {
    cfg = cfg || (window.__cfg ||= {});
    if (!librariesHydrated) return cfg;
    const block = cfgBlock(cfg);
    Object.entries(LIB_FEATURES).forEach(([fk, feature]) => {
      block[feature] = Object.assign({}, block[feature] || {}, { libraries: Array.from(libSets[fk]) });
    });
    return cfg;
  }

  function wireDelegates() {
    if (wireDelegates.done) return;
    wireDelegates.done = true;
    document.addEventListener("click", (ev) => {
      const tab = ev.target?.closest?.('.cw-subtile[data-sub="users"], .cw-subtile[data-sub="whitelist"]');
      if (!tab || !tab.closest?.("#sec-silo, .cw-connection-modal-panel[data-cw-connection-provider='silo']")) return;
      setTimeout(tab.dataset.sub === "users" ? mountUsers : mountWhitelist, 0);
    });
    document.addEventListener("click", (ev) => {
      const btn = ev.target?.closest?.("#silo_connect, #silo_cancel, #silo_disconnect");
      if (!btn || btn.__wired) return;
      ev.preventDefault();
      if (btn.id === "silo_connect") void startLogin();
      else if (btn.id === "silo_cancel") void cancelLogin();
      else void disconnect();
    }, true);
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => wire());
    watch._obs.observe(host, { childList: true, subtree: true, attributes: true, attributeFilter: ["class"] });
  }

  function watchOverlayClose() {
    const overlay = document.getElementById("cw-auth-connection-overlay");
    if (!overlay || watchOverlayClose._obs) return;
    watchOverlayClose._obs = new MutationObserver(() => {
      if ((overlay.classList.contains("hidden") || overlay.getAttribute("aria-hidden") === "true") && poller?.isRunning?.()) {
        void cancelLogin();
      }
    });
    watchOverlayClose._obs.observe(overlay, { attributes: true, attributeFilter: ["class", "aria-hidden"] });
  }

  function boot() {
    wireDelegates();
    wire();
    watch();
    watchOverlayClose();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", refresh, { once: true });
    } else {
      refresh();
    }
  }

  window.initSiloAuthUI = boot;
  window.cwAuth = window.cwAuth || {};
  window.cwAuth.silo = { init: boot, rehydrate: refresh, saveSelectedProfile };
  window.mergeSiloIntoCfg = mergeSiloIntoCfg;
  window.registerSettingsCollector?.(mergeSiloIntoCfg);
  document.addEventListener("settings-collect", (e) => { try { mergeSiloIntoCfg(e?.detail?.cfg || (window.__cfg ||= {})); } catch {} }, true);
  boot();
})();
