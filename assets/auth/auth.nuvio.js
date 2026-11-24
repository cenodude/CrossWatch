// assets/auth/auth.nuvio.js
// CrossWatch - Nuvio Auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._nuvioAuthPatched) return;
  window._nuvioAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const fetchJSON = Shared.fetchJSON;
  const profile = Shared.createProfileAdapter({
    provider: "nuvio",
    configKey: "nuvio",
    label: "Nuvio",
    sectionId: "sec-nuvio",
    selectId: "nuvio_instance",
    storageKey: "cw.ui.nuvio.auth.instance.v1",
  });

  let connected = false;
  let authenticated = false;
  let profileSelectionReady = false;
  let savedProfileId = "";
  let savedProfileName = "";
  let poller = null;
  let expiryTimer = null;

  function api(path) {
    return profile.api(path);
  }

  function notifyModalState() {
    const panel = document.getElementById("sec-nuvio")?.closest?.(".cw-connection-modal-panel")
      || document.querySelector?.(".cw-connection-modal-panel[data-cw-connection-provider='nuvio'], .cw-connection-modal-panel[data-provider='nuvio'], .cw-connection-modal-panel");
    const save = panel?.querySelector?.(".cw-connection-footer-save");
    if (!save) return;
    const canSave = connected || (authenticated && !!txt(el("nuvio_profile_select")?.value));
    save.disabled = !canSave;
    save.classList.toggle("is-disabled", !canSave);
    if (canSave) save.removeAttribute("title");
    else save.title = T("connections.auth.connectBeforeSave");
  }

  function syncConnectLocked() {
    try { Shared.setConnectLocked("nuvio_connect", connected); } catch {}
  }

  function setStatus(ok, msg) {
    const node = el("nuvio_msg");
    if (!node) return;
    node.classList.remove("hidden", "ok", "warn");
    node.classList.add(ok ? "ok" : "warn");
    node.textContent = msg || (ok ? T("connections.auth.connected") : T("connections.auth.notConnected"));
    notifyModalState();
  }

  function hideStatus() {
    const node = el("nuvio_msg");
    if (!node) return;
    node.classList.add("hidden");
    node.textContent = "";
    notifyModalState();
  }

  function setLoginState(show, data) {
    const box = el("nuvio_login_state");
    if (box) box.classList.toggle("hidden", !show);
    if (!show) {
      stopExpiryTimer();
      return;
    }
    const code = txt(data?.code);
    const url = txt(data?.login_url);
    const shortCode = code ? code.slice(0, 5).toUpperCase() : "-----";
    if (el("nuvio_code")) {
      el("nuvio_code").textContent = shortCode;
      el("nuvio_code").title = code;
    }
    if (el("nuvio_code_input")) el("nuvio_code_input").value = code;
    const a = el("nuvio_login_url");
    if (a) {
      a.href = url || "#";
      a.textContent = url ? T("connections.auth.nuvio.openApproval") : "";
    }
    const localUrl = localDashboardUrl(url);
    el("nuvio_local_hint")?.classList.toggle("hidden", !localUrl);
    const localLink = el("nuvio_local_url");
    if (localLink) localLink.href = localUrl || "#";
    startExpiryTimer(data?.expires_at);
  }

  function localDashboardUrl(url) {
    if (selectedServerMode() !== "self_hosted" || !url) return "";
    try {
      const u = new URL(url);
      if (!["localhost", "127.0.0.1", "[::1]"].includes(u.hostname) || u.port === "3000") return "";
      u.port = "3000";
      return u.toString();
    } catch {
      return "";
    }
  }

  function normalizeExpiryEpoch(value) {
    const raw = Number(value || 0);
    if (!raw) return 0;
    return raw > 1000000000 ? Math.floor(raw) : 0;
  }

  function updateExpiry(expiresAt) {
    const node = el("nuvio_expiry");
    if (!node) return;
    const exp = normalizeExpiryEpoch(expiresAt);
    if (!exp) {
      node.textContent = "";
      return;
    }
    const left = Math.max(0, Math.floor(exp - Date.now() / 1000));
    if (!left) {
      node.textContent = T("connections.auth.expired");
      return;
    }
    const mins = Math.floor(left / 60);
    const secs = String(left % 60).padStart(2, "0");
    node.textContent = T("connections.auth.expiresIn", {time: `${mins}:${secs}`});
  }

  function stopExpiryTimer() {
    if (expiryTimer) clearInterval(expiryTimer);
    expiryTimer = null;
  }

  function startExpiryTimer(expiresAt) {
    stopExpiryTimer();
    const exp = normalizeExpiryEpoch(expiresAt);
    updateExpiry(exp);
    if (!exp) return;
    expiryTimer = setInterval(() => {
      updateExpiry(exp);
      if (exp <= Math.floor(Date.now() / 1000)) stopExpiryTimer();
    }, 1000);
  }

  function selectedServerMode() {
    return txt(el("nuvio_server_mode")?.value) === "self_hosted" ? "self_hosted" : "cloud";
  }

  function syncServerUI() {
    const selfHosted = selectedServerMode() === "self_hosted";
    const locked = connected || authenticated;
    el("nuvio_server_url_wrap")?.classList.toggle("hidden", !selfHosted);
    el("nuvio_server_check")?.classList.toggle("hidden", !selfHosted || locked);
    const mode = el("nuvio_server_mode");
    if (mode) {
      mode.disabled = locked;
      mode.title = locked ? T("connections.auth.gen.disconnectToChange", {provider: "Nuvio"}) : "";
    }
    const url = el("nuvio_server_url");
    if (url) url.disabled = locked;
  }

  function renderServer(data) {
    const mode = el("nuvio_server_mode");
    if (mode) mode.value = txt(data?.server_mode) === "self_hosted" ? "self_hosted" : "cloud";
    const url = el("nuvio_server_url");
    if (url) url.value = txt(data?.base_url);
    syncServerUI();
  }

  async function applyServer() {
    const mode = selectedServerMode();
    const baseUrl = txt(el("nuvio_server_url")?.value).trim();
    if (mode === "self_hosted" && !baseUrl) throw new Error("invalid_url");
    const r = await fetchJSON(api("/api/nuvio/server"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ server_mode: mode, base_url: baseUrl }),
    });
    if (!r.ok || !r.data?.ok) throw new Error(String(r.data?.error || "server_failed"));
    renderServer(r.data);
    return r.data;
  }

  async function checkServer() {
    const btn = el("nuvio_server_check");
    if (btn) btn.disabled = true;
    try {
      await applyServer();
      setStatus(true, T("connections.auth.nuvio.found"));
    } catch (e) {
      setStatus(false, friendlyError(e?.message));
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  function setPolling(text) {
    const node = el("nuvio_polling");
    if (node) node.textContent = text || "";
  }

  function stopPendingLogin(clearStatus) {
    if (poller) poller.stop();
    setLoginState(false);
    setPolling("");
    if (clearStatus && !connected) hideStatus();
  }

  function renderProfiles(profiles, selectedId, selectedName, forceShow) {
    const wrap = el("nuvio_profile_state");
    const sel = el("nuvio_profile_select");
    if (!wrap || !sel) return;
    const rows = Array.isArray(profiles) ? profiles : [];
    const effectiveSelectedId = selectedId || (rows.length === 1 ? (rows[0].profile_id || rows[0].profile_index || "") : "");
    savedProfileId = txt(selectedId || "");
    savedProfileName = txt(selectedName || "");
    const showProfiles = !!(connected || profileSelectionReady || forceShow);
    wrap.classList.toggle("hidden", !showProfiles);
    sel.style.width = "320px";
    sel.style.minWidth = "280px";
    sel.innerHTML = "";
    const hasSelected = !!(effectiveSelectedId && rows.some((row) => String(row.profile_id || row.profile_index || "") === String(effectiveSelectedId)));
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = rows.length
      ? (effectiveSelectedId ? T("connections.auth.profileUnavailableSel") : T("connections.auth.gen.selectProfile", {provider: "Nuvio"}))
      : T("connections.auth.gen.noProfiles", {provider: "Nuvio"});
    placeholder.disabled = true;
    placeholder.selected = true;
    sel.appendChild(placeholder);
    [...rows].sort((a, b) => String(a.name || `Profile ${a.profile_id || a.profile_index || ""}`).localeCompare(String(b.name || `Profile ${b.profile_id || b.profile_index || ""}`), undefined, { sensitivity: "base", numeric: true })).forEach((row) => {
      const option = document.createElement("option");
      option.value = String(row.profile_id || row.profile_index || "");
      option.textContent = String(row.name || T("connections.auth.profileN", {n: option.value}));
      sel.appendChild(option);
    });
    if (hasSelected) {
      sel.value = String(effectiveSelectedId);
    }
    requestAnimationFrame(() => {
      const iconSelect = sel.nextElementSibling;
      if (iconSelect?.classList?.contains("cw-icon-select")) {
        iconSelect.style.width = "320px";
        iconSelect.style.minWidth = "280px";
        iconSelect.style.maxWidth = "100%";
      }
    });
    if (!sel.__wiredNuvioProfileChange) {
      sel.addEventListener("change", notifyModalState);
      sel.__wiredNuvioProfileChange = true;
    }
    notifyModalState();
  }

  async function refreshProfiles(statusData) {
    const selectedId = statusData?.profile_id;
    const selectedName = txt(statusData?.profile_name);
    if (!statusData?.authenticated && !connected) {
      renderProfiles([], selectedId, selectedName, false);
      return;
    }
    const r = await fetchJSON(api("/api/nuvio/profiles"), { cache: "no-store" });
    const showProfiles = !!(connected || authenticated || profileSelectionReady);
    if (r.ok && r.data?.ok) {
      renderProfiles(r.data.profiles || [], selectedId, selectedName, showProfiles);
    } else {
      renderProfiles([], selectedId, selectedName, showProfiles);
    }
  }

  async function refresh() {
    try {
      const r = await fetchJSON(api("/api/nuvio/status"), { cache: "no-store" });
      const data = r.data || {};
      connected = !!(r.ok && data.connected);
      authenticated = !!(r.ok && data.authenticated);
      savedProfileId = txt(data.profile_id);
      savedProfileName = txt(data.profile_name);
      profileSelectionReady = connected || authenticated || profileSelectionReady;
      syncConnectLocked();
      renderServer(data);
      if (connected) setStatus(true, T("connections.auth.gen.connected", {provider: "Nuvio"}));
      else if (data.authenticated) setStatus(false, T("connections.auth.gen.chooseProfile", {provider: "Nuvio"}));
      else hideStatus();
      await refreshProfiles(data);
    } catch {
      connected = false;
      authenticated = false;
      syncConnectLocked();
      setStatus(false, T("connections.auth.gen.statusFailed", {provider: "Nuvio"}));
    }
  }

  async function finishLogin() {
    const r = await fetchJSON(api("/api/nuvio/device/finish"), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    if (!r.ok || !r.data?.ok) throw new Error(String(r.data?.status || "finish_failed"));
    setLoginState(false);
    setPolling("");
    note(T("connections.auth.nuvio.approved"));
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    authenticated = true;
    profileSelectionReady = true;
    renderProfiles(r.data.profiles || [], null, "", true);
    await refresh();
    return true;
  }

  function ensurePoller() {
    if (poller) return poller;
    poller = Shared.createDevicePoll({
      minIntervalMs: 3000,
      maxTotalMs: 300000,
      url: () => api("/api/nuvio/device/poll"),
      classify(status, data) {
        const state = txt(data?.status).toLowerCase();
        if (data?.ok || state === "approved") return { state: "authorized" };
        if (state === "expired") return { state: "expired" };
        if (status >= 400 && !["pending", "authorization_pending", ""].includes(state)) return { state: "terminal", message: state || "poll_failed" };
        return { state: "pending", intervalMs: Math.max(3000, Number(data?.interval || 3) * 1000) };
      },
      onPending(data) {
        setPolling(T("connections.auth.waitingApproval"));
      },
      onAuthorized() {
        finishLogin().catch((e) => {
          setStatus(false, friendlyError(e?.message));
          note(T("connections.auth.nuvio.completionFailed"));
        });
      },
      onExpired() {
        setPolling(T("connections.auth.codeExpiredConnect"));
        setStatus(false, T("connections.auth.nuvio.expired"));
      },
      onTerminal(verdict) {
        setStatus(false, friendlyError(verdict?.message || "poll_failed"));
      },
      onTimeout() {
        setStatus(false, T("connections.auth.nuvio.timedOut"));
      },
    });
    return poller;
  }

  function friendlyError(code) {
    switch (String(code || "")) {
      case "authentication_failed": return T("connections.auth.gen.authenticationFailed", {provider: "Nuvio"});
      case "token_refresh_failed": return T("connections.auth.nuvio.refresh");
      case "anonymous_session_not_object": return T("connections.auth.nuvio.anonResponse");
      case "missing_access_token": return T("connections.auth.nuvio.noAccess");
      case "missing_refresh_token": return T("connections.auth.nuvio.noRefresh");
      case "start_session_not_list_object": return T("connections.auth.nuvio.tvResponse");
      case "missing_code_or_qr_content": return T("connections.auth.nuvio.tvUrl");
      case "invalid_expiry": return T("connections.auth.nuvio.tvExpiry");
      case "invalid_poll_interval": return T("connections.auth.nuvio.tvPoll");
      case "invalid_response": return T("connections.validation.returnedUnexpected", {provider: "Nuvio"});
      case "service_unavailable": return T("connections.auth.gen.serviceUnavailable", {provider: "Nuvio"});
      case "profile_unavailable": return T("connections.auth.gen.profileUnavailable", {provider: "Nuvio"});
      case "invalid_url": return T("connections.auth.nuvio.backendUrl");
      case "official_server": return T("connections.auth.nuvio.official");
      case "connection_failed": return T("connections.auth.gen.reachServer", {provider: "Nuvio"});
      case "http_error": return T("connections.auth.nuvio.discovery");
      case "invalid_document":
      case "wrong_service":
      case "not_self_hosted":
      case "missing_configuration": return T("connections.auth.nuvio.notSelfHosted");
      case "unsupported_version": return T("connections.auth.nuvio.version");
      case "response_too_large": return T("connections.auth.nuvio.tooLarge");
      case "tv_login_unsupported": return T("connections.auth.nuvio.noTv");
      case "disconnect_first": return T("connections.auth.gen.disconnectBefore", {provider: "Nuvio"});
      default: return T("connections.auth.gen.requestFailedShort", {provider: "Nuvio"});
    }
  }

  async function startLogin() {
    profileSelectionReady = false;
    renderProfiles([], null, "", false);
    const win = Shared.openWaitWindow("Nuvio", T("connections.auth.nuvio.checking"), "NUVIO");
    try {
      setStatus(false, T("connections.auth.nuvio.checking"));
      await applyServer();
      const r = await fetchJSON(api("/api/nuvio/device/start"), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(String(data.error || "start_failed"));
      setLoginState(true, data);
      setStatus(false, T("connections.auth.gen.waitingApproval", {provider: "Nuvio"}));
      setPolling(T("connections.auth.waitingApproval"));
      const url = txt(data.login_url);
      if (url) {
        if (!Shared.showWaitCode(win, { key: "NUVIO", label: "Nuvio", code: txt(data.code).slice(0, 5).toUpperCase(), url })) {
          try { window.open(url, "_blank", "noopener"); } catch {}
          note(T("connections.auth.nuvio.popup"));
        }
      }
      ensurePoller().start({ intervalMs: Math.max(3000, Number(data.interval || 3) * 1000), deadlineMs: normalizeExpiryEpoch(data.expires_at) * 1000 });
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch {}
      setStatus(false, friendlyError(e?.message));
      note(friendlyError(e?.message));
    }
  }

  async function disconnect() {
    try {
      ensurePoller().stop();
      const r = await fetchJSON(api("/api/nuvio/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || r.data?.ok === false) throw new Error(String(r.data?.error || "disconnect_failed"));
    connected = false;
    authenticated = false;
    profileSelectionReady = false;
    syncConnectLocked();
    syncServerUI();
    stopPendingLogin(false);
    renderProfiles([], null, "", false);
      setStatus(false, T("connections.auth.gen.disconnected", {provider: "Nuvio"}));
      note(T("connections.auth.gen.disconnected", {provider: "Nuvio"}));
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    } catch {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Nuvio"}));
    }
  }

  async function saveSelectedProfile(opts = {}) {
    const select = el("nuvio_profile_select");
    const profileId = txt(select?.value);
    if (!profileId) return false;
    const profileName = txt(select?.selectedOptions?.[0]?.textContent);
    if (connected && savedProfileId && String(savedProfileId) === String(profileId)) {
      setStatus(true, T("connections.auth.gen.connected", {provider: "Nuvio"}));
      if (!opts.silent) note(T("connections.auth.gen.profileSaved", {provider: "Nuvio"}));
      return true;
    }
    const r = await fetchJSON(api("/api/nuvio/profile/select"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify({ profile_id: /^\d+$/.test(profileId) ? Number(profileId) : profileId }),
    });
    if (!r.ok || r.data?.ok === false) throw new Error(String(r.data?.error || r.data?.status || "profile_select_failed"));
    connected = true;
    authenticated = true;
    profileSelectionReady = true;
    savedProfileId = profileId;
    savedProfileName = txt(r.data?.profile_name || profileName);
    setStatus(true, T("connections.auth.gen.connected", {provider: "Nuvio"}));
    try { window.invalidateConfigCache?.(); } catch {}
    try { window.CW?.Cache?.invalidate?.("config"); } catch {}
    setTimeout(async () => {
      try { window.refreshStatus?.(true); } catch {}
      try { await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true); } catch {}
      try { window.manualRefreshStatus?.(); } catch {}
    }, 0);
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    if (!opts.silent) note(T("connections.auth.gen.profileSaved", {provider: "Nuvio"}));
    return true;
  }

  function ensureInstanceUI() {
    profile.ensureUI(() => {
      stopPendingLogin(true);
      void refresh();
    });
  }

  function wire() {
    ensureInstanceUI();
    const connect = el("nuvio_connect");
    if (connect && !connect.__wired) { connect.addEventListener("click", startLogin); connect.__wired = true; }
    const copy = el("nuvio_code_copy");
    if (copy && !copy.__wired) {
      copy.addEventListener("click", (ev) => {
        ev.preventDefault();
        void Shared.copyField("nuvio_code_input", copy, { emptyMessage: T("connections.auth.nothingToCopy") });
      });
      copy.__wired = true;
    }
    const del = el("nuvio_disconnect");
    if (del && !del.__wired) { del.addEventListener("click", disconnect); del.__wired = true; }
    const mode = el("nuvio_server_mode");
    if (mode && !mode.__wired) {
      mode.addEventListener("change", () => {
        syncServerUI();
        if (!connected) hideStatus();
      });
      mode.__wired = true;
    }
    const check = el("nuvio_server_check");
    if (check && !check.__wired) { check.addEventListener("click", checkServer); check.__wired = true; }
  }

  function wireDelegates() {
    if (wireDelegates.done) return;
    wireDelegates.done = true;
    document.addEventListener("click", (ev) => {
      const btn = ev.target?.closest?.("#nuvio_connect, #nuvio_disconnect");
      if (!btn || btn.__wired) return;
      ev.preventDefault();
      if (btn.id === "nuvio_connect") void startLogin();
      else if (btn.id === "nuvio_disconnect") void disconnect();
    }, true);
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => {
      wire();
    });
    watch._obs.observe(host, { childList: true, subtree: true, attributes: true, attributeFilter: ["class"] });
  }

  function watchOverlayClose() {
    const overlay = document.getElementById("cw-auth-connection-overlay");
    if (!overlay || watchOverlayClose._obs) return;
    watchOverlayClose._obs = new MutationObserver(() => {
      if (overlay.classList.contains("hidden") || overlay.getAttribute("aria-hidden") === "true") {
        stopPendingLogin(true);
      }
    });
    watchOverlayClose._obs.observe(overlay, { attributes: true, attributeFilter: ["class", "aria-hidden"] });
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;
    const select = el("nuvio_profile_select");
    const profileId = txt(select?.value);
    if (!profileId) return;
    const target = profile.cfgBlock(cfg, true);
    target.profile_id = /^\d+$/.test(profileId) ? Number(profileId) : profileId;
    target.profile_name = txt(select?.selectedOptions?.[0]?.textContent);
  }, true);

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

  window.initNuvioAuthUI = boot;
  window.cwAuth = window.cwAuth || {};
  window.cwAuth.nuvio = { init: boot, rehydrate: refresh, saveSelectedProfile };
  boot();
})();
