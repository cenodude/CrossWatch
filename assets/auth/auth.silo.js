// assets/auth/auth.silo.js
// CrossWatch - Silo Auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
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
    else save.title = chosen?.has_pin ? "Enter the profile PIN before saving." : "Connect this profile before saving.";
  }

  function syncConnectLocked() {
    try { Shared.setConnectLocked("silo_connect", connected || authenticated); } catch {}
  }

  function setStatus(ok, msg) {
    const node = el("silo_msg");
    if (!node) return;
    node.classList.remove("hidden", "ok", "warn");
    node.classList.add(ok ? "ok" : "warn");
    node.textContent = msg || (ok ? "Connected" : "Not connected");
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
    node.textContent = left ? `Expires in ${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")}` : "Expired";
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
      url.title = locked ? "Disconnect Silo to change the server." : "";
    }
    const ssl = el("silo_verify_ssl");
    if (ssl) ssl.disabled = locked;
  }

  function renderServer(data) {
    const url = el("silo_server_url");
    if (url && (txt(data?.server_url) || !url.value)) url.value = txt(data?.server_url);
    const ssl = el("silo_verify_ssl");
    if (ssl) ssl.checked = data?.verify_ssl !== false;
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
    placeholder.textContent = profiles.length ? (effective ? "Selected profile unavailable" : "Select Silo profile") : "No Silo profiles loaded";
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
      if (data.reconnect_required) setStatus(false, "Silo session ended - disconnect and connect again");
      else if (connected && data.profile_pin_required) setStatus(false, "Enter the Silo profile PIN again");
      else if (connected) setStatus(true, data.profile_name ? `Silo connected - ${data.profile_name}` : "Silo connected");
      else if (authenticated) setStatus(false, "Choose a Silo profile to finish setup");
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
      setStatus(false, "Silo status failed");
    }
  }

  function friendlyError(code) {
    switch (String(code || "")) {
      case "invalid_url": return "Enter a valid Silo server URL";
      case "connection_failed": return "Could not reach the Silo server";
      case "ssl_error": return "SSL check failed - fix the certificate or turn off Verify SSL";
      case "not_silo": return "That URL is not a Silo server";
      case "http_error": return "Silo server returned an error";
      case "device_login_unavailable": return "Device sign-in is disabled on this Silo server";
      case "disconnect_first": return "Disconnect Silo before changing the server";
      case "rate_limited": return "Silo is rate limiting sign-in - try again shortly";
      case "service_unavailable": return "Silo service unavailable";
      case "reconnect_required": return "Silo session ended - connect again";
      case "authentication_failed": return "Silo authentication failed";
      case "profile_unavailable": return "Selected Silo profile is unavailable";
      case "pin_required": return "Enter the profile PIN";
      case "invalid_pin": return "Incorrect profile PIN";
      case "pin_locked": return "Too many incorrect PINs - try again later";
      case "invalid_response": return "Silo returned an unexpected response";
      case "denied": return "The sign-in was denied on Silo";
      case "canceled": return "The sign-in was canceled";
      case "consumed": return "That code was already used - connect again";
      default: return "Silo request failed";
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
        setPolling(data?.opened ? "Code opened on Silo - waiting for approval..." : "Waiting for approval...");
      },
      async onAuthorized(data) {
        setLoginState(false);
        setPolling("");
        authenticated = true;
        profileSelectionReady = true;
        note("Silo sign-in approved");
        try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
        await refresh();
        if (!connected) setStatus(false, "Choose a Silo profile to finish setup");
      },
      onExpired() {
        setLoginState(false);
        setPolling("");
        setStatus(false, "Silo code expired - connect again");
      },
      onTerminal(verdict) {
        setLoginState(false);
        setStatus(false, friendlyError(verdict?.message));
      },
      onTimeout() {
        setLoginState(false);
        setStatus(false, "Silo sign-in timed out");
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
    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch {}
    try {
      win.document.write(
        '<!doctype html><meta charset="utf-8"><title>CrossWatch -> Silo</title>' +
        '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif">' +
        '<div style="font-size:14px;opacity:.7">Contacting your Silo server...</div></body>'
      );
      win.document.close();
    } catch {}
    try {
      profileSelectionReady = false;
      renderProfiles([], "", false);
      setStatus(false, "Contacting Silo server...");
      const r = await jsonPost("/api/silo/device/start", { server_url: serverUrl, verify_ssl: !!el("silo_verify_ssl")?.checked });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(String(data.error || "start_failed"));
      setLoginState(true, data);
      setStatus(false, "Waiting for Silo approval");
      setPolling("Waiting for approval...");
      const url = txt(data.verification_uri_complete || data.verification_uri);
      if (url && win && !win.closed) {
        try { win.location.href = url; } catch {}
      } else if (url) {
        try { window.open(url, "_blank", "noopener"); } catch {}
        note("Popup blocked - open the Silo activate link.");
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
      setStatus(false, "Silo disconnected");
      note("Silo disconnected");
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    } catch {
      note("Silo disconnect failed");
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
    setStatus(true, `Silo connected - ${txt(r.data?.profile_name || chosen.name)}`);
    try { window.invalidateConfigCache?.(); } catch {}
    try { window.CW?.Cache?.invalidate?.("config"); } catch {}
    setTimeout(async () => {
      try { window.refreshStatus?.(true); } catch {}
      try { await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true); } catch {}
      try { window.manualRefreshStatus?.(); } catch {}
    }, 0);
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    if (!opts.silent) note("Silo profile saved");
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
        void Shared.copyField("silo_code_input", copy, { emptyMessage: "Nothing to copy" });
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
      host.innerHTML = '<div class="cw-wl cw-su"><div class="cw-wl-empty">Connect Silo and choose a profile before adding household users.</div></div>';
      return;
    }
    try { window.CW?.SyncUsers?.mount(host, "silo", profile.getInstance ? profile.getInstance() : "default"); } catch {}
  }

  function wireDelegates() {
    if (wireDelegates.done) return;
    wireDelegates.done = true;
    document.addEventListener("click", (ev) => {
      const tab = ev.target?.closest?.('.cw-subtile[data-sub="users"]');
      if (tab && tab.closest?.("#sec-silo, .cw-connection-modal-panel[data-cw-connection-provider='silo']")) setTimeout(mountUsers, 0);
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
  boot();
})();
