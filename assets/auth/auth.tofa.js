// assets/auth/auth.tofa.js
// CrossWatch - Tofa Auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  if (window._tofaAuthPatched) return;
  window._tofaAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const fetchJSON = Shared.fetchJSON;
  const profile = Shared.createProfileAdapter({
    provider: "tofa",
    configKey: "tofa",
    label: "Tofa",
    sectionId: "sec-tofa",
    selectId: "tofa_instance",
    storageKey: "cw.ui.tofa.auth.instance.v1",
  });

  let connected = false;
  let authenticated = false;
  let profileSelectionReady = false;
  let savedProfileId = "";
  let profiles = [];
  let poller = null;
  let pollUrl = "";
  let loginGen = 0;
  let viewGen = 0;
  let expiryTimer = null;
  const LIB_FEATURES = { hist: "history", prog: "progress", coll: "collection", scr: "scrobble" };
  const libSets = { hist: new Set(), prog: new Set(), coll: new Set(), scr: new Set() };
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
    const id = txt(el("tofa_profile_select")?.value);
    return profiles.find((row) => String(row.id) === id) || null;
  }

  function notifyModalState() {
    const panel = document.getElementById("sec-tofa")?.closest?.(".cw-connection-modal-panel")
      || document.querySelector?.(".cw-connection-modal-panel[data-cw-connection-provider='tofa'], .cw-connection-modal-panel[data-provider='tofa']");
    const save = panel?.querySelector?.(".cw-connection-footer-save");
    if (!save) return;
    const chosen = selectedProfile();
    const pinOk = !chosen?.has_pin || String(chosen.id) === savedProfileId || !!txt(el("tofa_profile_pin")?.value);
    const canSave = connected || (authenticated && !!chosen && pinOk);
    save.disabled = !canSave;
    save.classList.toggle("is-disabled", !canSave);
    if (canSave) save.removeAttribute("title");
    else save.title = chosen?.has_pin ? "Enter the profile PIN before saving." : "Connect this instance before saving.";
  }

  function syncConnectLocked() {
    try { Shared.setConnectLocked("tofa_connect", connected || authenticated); } catch {}
  }

  function setStatus(ok, msg) {
    const node = el("tofa_msg");
    if (!node) return;
    node.classList.remove("hidden", "ok", "warn");
    node.classList.add(ok ? "ok" : "warn");
    node.textContent = msg || (ok ? "Connected" : "Not connected");
    notifyModalState();
  }

  function hideStatus() {
    const node = el("tofa_msg");
    if (!node) return;
    node.classList.add("hidden");
    node.textContent = "";
    notifyModalState();
  }

  function renderQr(svg) {
    const host = el("tofa_qr");
    if (!host) return;
    host.innerHTML = "";
    const raw = txt(svg);
    host.classList.toggle("hidden", !raw.startsWith("<svg"));
    if (!raw.startsWith("<svg")) return;
    const img = document.createElement("img");
    img.alt = "";
    img.width = 120;
    img.height = 120;
    img.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(raw)}`;
    host.appendChild(img);
  }

  function setLoginState(show, data) {
    el("tofa_login_state")?.classList.toggle("hidden", !show);
    el("tofa_cancel")?.classList.toggle("hidden", !show);
    if (!show) {
      stopExpiryTimer();
      renderQr("");
      return;
    }
    const code = txt(data?.user_code);
    if (el("tofa_code")) el("tofa_code").textContent = code || "----";
    if (el("tofa_code_input")) el("tofa_code_input").value = code;
    const link = el("tofa_login_url");
    const url = txt(data?.verification_uri_complete || data?.verification_uri);
    if (link) link.href = url || "#";
    renderQr(data?.qr_code_svg);
    startExpiryTimer(data?.expires_at);
  }

  function updateExpiry(expiresAt) {
    const node = el("tofa_expiry");
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
    const node = el("tofa_polling");
    if (node) node.textContent = text || "";
  }

  function syncServerUI() {
    const locked = connected || authenticated;
    const url = el("tofa_server_url");
    if (url) {
      url.disabled = locked;
      url.title = locked ? "Disconnect Tofa to change the server." : "";
    }
    const ssl = el("tofa_verify_ssl");
    if (ssl) ssl.disabled = locked;
  }

  function renderServer(data) {
    const url = el("tofa_server_url");
    if (url && (txt(data?.server_url) || !url.value)) url.value = txt(data?.server_url);
    const ssl = el("tofa_verify_ssl");
    if (ssl) ssl.checked = data?.verify_ssl === true;
    syncServerUI();
  }

  function syncPinField() {
    const chosen = selectedProfile();
    const needsPin = !!chosen?.has_pin && String(chosen.id) !== savedProfileId;
    el("tofa_pin_wrap")?.classList.toggle("hidden", !needsPin);
    if (!needsPin && el("tofa_profile_pin")) el("tofa_profile_pin").value = "";
    notifyModalState();
  }

  function renderProfiles(rows, selectedId, forceShow) {
    const wrap = el("tofa_profile_state");
    const sel = el("tofa_profile_select");
    if (!wrap || !sel) return;
    profiles = Array.isArray(rows) ? rows : [];
    savedProfileId = txt(selectedId || "");
    const effective = savedProfileId || (profiles.length === 1 ? String(profiles[0].id) : "");
    wrap.classList.toggle("hidden", !(connected || profileSelectionReady || forceShow));
    sel.innerHTML = "";
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = profiles.length ? (effective ? "Selected profile unavailable" : "Select Tofa profile") : "No Tofa profiles loaded";
    placeholder.disabled = true;
    placeholder.selected = true;
    sel.appendChild(placeholder);
    profiles.forEach((row) => {
      const option = document.createElement("option");
      option.value = String(row.id);
      const tags = [row.is_primary ? "primary" : "", row.has_pin ? "PIN" : "", row.is_child ? "kids" : ""].filter(Boolean);
      option.textContent = tags.length ? `${row.name} (${tags.join(", ")})` : String(row.name);
      sel.appendChild(option);
    });
    if (effective && profiles.some((row) => String(row.id) === effective)) sel.value = effective;
    if (!sel.__wiredTofaProfileChange) {
      sel.addEventListener("change", syncPinField);
      sel.__wiredTofaProfileChange = true;
    }
    syncPinField();
  }

  async function refreshProfiles(statusData, gen = viewGen) {
    if (!statusData?.authenticated) {
      renderProfiles([], statusData?.profile_id, false);
      return;
    }
    const r = await fetchJSON(api("/api/tofa/profiles"), { cache: "no-store" });
    if (gen !== viewGen) return;
    renderProfiles(r.ok && r.data?.ok ? (r.data.profiles || []) : [], statusData?.profile_id, true);
  }

  async function refresh() {
    const gen = viewGen;
    try {
      const r = await fetchJSON(api("/api/tofa/status"), { cache: "no-store" });
      if (gen !== viewGen) return;
      const data = r.data || {};
      connected = !!(r.ok && data.connected);
      authenticated = !!(r.ok && data.authenticated);
      profileSelectionReady = connected || authenticated;
      syncConnectLocked();
      renderServer(data);
      if (data.reconnect_required) setStatus(false, "Tofa session ended - disconnect and connect again");
      else if (connected && data.profile_pin_required) setStatus(false, "Enter the Tofa profile PIN again");
      else if (connected) setStatus(true, data.profile_name ? `Tofa connected - ${data.profile_name}` : "Tofa connected");
      else if (authenticated) setStatus(false, "Choose a Tofa profile to finish setup");
      else hideStatus();
      await refreshProfiles(data, gen);
      if (gen !== viewGen) return;
      if (connected && data.profile_pin_required) {
        savedProfileId = "";
        syncPinField();
      }
    } catch {
      if (gen !== viewGen) return;
      connected = false;
      authenticated = false;
      syncConnectLocked();
      setStatus(false, "Tofa status failed");
    }
  }

  function friendlyError(code) {
    switch (String(code || "")) {
      case "invalid_url": return "Enter a valid Tofa server URL";
      case "connection_failed": return "Could not reach the Tofa server";
      case "ssl_error": return "SSL check failed - fix the certificate or turn off Verify SSL";
      case "not_tofa": return "That URL is not a Tofa server";
      case "server_unclaimed": return "This Tofa server is not set up yet - finish its setup first";
      case "invalid_connect_url": return "This Tofa server reports an unexpected sign-in service";
      case "http_error": return "Tofa server returned an error";
      case "disconnect_first": return "Disconnect Tofa before changing the server";
      case "rate_limited": return "Tofa is rate limiting sign-in - try again shortly";
      case "service_unavailable": return "Tofa service unavailable";
      case "reconnect_required": return "Tofa session ended - connect again";
      case "authentication_failed": return "Tofa authentication failed";
      case "profile_unavailable": return "Selected Tofa profile is unavailable";
      case "profile_locked": return "Enter the profile PIN";
      case "pin_required": return "Enter the profile PIN";
      case "invalid_pin": return "Incorrect profile PIN";
      case "pin_locked": return "Too many incorrect PINs - try again later";
      case "invalid_response": return "Tofa returned an unexpected response";
      case "denied": return "The sign-in was denied on Tofa";
      default: return "Tofa request failed";
    }
  }

  function stopPendingLogin(clearStatus) {
    loginGen += 1;
    if (poller) poller.stop();
    setLoginState(false);
    setPolling("");
    if (clearStatus && !connected && !authenticated) hideStatus();
  }

  async function cancelLogin() {
    stopPendingLogin(true);
    try { await jsonPost("/api/tofa/device/cancel", {}); } catch {}
  }

  function ensurePoller() {
    if (poller) return poller;
    poller = Shared.createDevicePoll({
      minIntervalMs: 5000,
      maxTotalMs: 600000,
      url: () => pollUrl,
      classify(status, data) {
        const state = txt(data?.status).toLowerCase();
        if (data?.ok || state === "authorized") return { state: "authorized" };
        if (state === "expired") return { state: "expired" };
        if (state === "slow_down") return { state: "slow_down" };
        if (state === "service_unavailable") return { state: "server" };
        if (state && state !== "pending") return { state: "terminal", message: state };
        return { state: "pending", intervalMs: Math.max(5000, Number(data?.interval || 5) * 1000) };
      },
      onPending() {
        setPolling("Waiting for approval...");
      },
      async onAuthorized() {
        setLoginState(false);
        setPolling("");
        authenticated = true;
        profileSelectionReady = true;
        note("Tofa sign-in approved");
        try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
        await refresh();
        if (!connected) setStatus(false, "Choose a Tofa profile to finish setup");
      },
      onExpired() {
        setLoginState(false);
        setPolling("");
        setStatus(false, "Tofa code expired - connect again");
      },
      onTerminal(verdict) {
        setLoginState(false);
        setStatus(false, friendlyError(verdict?.message));
      },
      onTimeout() {
        setLoginState(false);
        setStatus(false, "Tofa sign-in timed out");
      },
    });
    return poller;
  }

  async function startLogin() {
    const serverUrl = txt(el("tofa_server_url")?.value).trim();
    if (!serverUrl) {
      setStatus(false, friendlyError("invalid_url"));
      return;
    }
    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch {}
    try {
      win.document.write(
        '<!doctype html><meta charset="utf-8"><title>CrossWatch -> Tofa</title>' +
        '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif">' +
        '<div style="font-size:14px;opacity:.7">Contacting Tofa...</div></body>'
      );
      win.document.close();
    } catch {}
    const gen = ++loginGen;
    const startPollUrl = api("/api/tofa/device/poll");
    try {
      profileSelectionReady = false;
      renderProfiles([], "", false);
      setStatus(false, "Contacting Tofa server...");
      const r = await jsonPost("/api/tofa/device/start", { server_url: serverUrl, verify_ssl: !!el("tofa_verify_ssl")?.checked });
      if (gen !== loginGen) {
        try { if (win && !win.closed) win.close(); } catch {}
        return;
      }
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(String(data.error || "start_failed"));
      setLoginState(true, data);
      setStatus(false, "Waiting for Tofa approval");
      setPolling("Waiting for approval...");
      const url = txt(data.verification_uri_complete || data.verification_uri);
      if (url && win && !win.closed) {
        try { win.location.href = url; } catch {}
      } else if (url) {
        try { window.open(url, "_blank", "noopener"); } catch {}
        note("Popup blocked - open the Tofa link page.");
      } else {
        try { if (win && !win.closed) win.close(); } catch {}
      }
      pollUrl = startPollUrl;
      ensurePoller().start({ intervalMs: Math.max(5000, Number(data.interval || 5) * 1000), deadlineMs: Number(data.expires_at || 0) * 1000 });
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch {}
      if (gen !== loginGen) return;
      setStatus(false, friendlyError(e?.message));
      note(friendlyError(e?.message));
    }
  }

  async function disconnect() {
    try {
      ensurePoller().stop();
      const r = await fetchJSON(api("/api/tofa/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || r.data?.ok === false) throw new Error(String(r.data?.error || "disconnect_failed"));
      connected = false;
      authenticated = false;
      profileSelectionReady = false;
      syncConnectLocked();
      syncServerUI();
      stopPendingLogin(false);
      renderProfiles([], "", false);
      setStatus(false, "Tofa disconnected");
      note("Tofa disconnected");
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    } catch {
      note("Tofa disconnect failed");
    }
  }

  async function saveSelectedProfile(opts = {}) {
    const chosen = selectedProfile();
    if (!chosen) return false;
    if (connected && savedProfileId && String(savedProfileId) === String(chosen.id)) return true;
    const pin = txt(el("tofa_profile_pin")?.value);
    const r = await jsonPost("/api/tofa/profile/select", { profile_id: chosen.id, pin: chosen.has_pin ? pin : undefined });
    if (!r.ok || r.data?.ok === false) {
      const msg = friendlyError(r.data?.error || "profile_select_failed");
      setStatus(false, msg);
      throw new Error(msg);
    }
    if (el("tofa_profile_pin")) el("tofa_profile_pin").value = "";
    connected = true;
    authenticated = true;
    profileSelectionReady = true;
    savedProfileId = String(chosen.id);
    syncPinField();
    setStatus(true, `Tofa connected - ${txt(r.data?.profile_name || chosen.name)}`);
    try { window.invalidateConfigCache?.(); } catch {}
    try { window.CW?.Cache?.invalidate?.("config"); } catch {}
    setTimeout(async () => {
      try { window.refreshStatus?.(true); } catch {}
      try { await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true); } catch {}
      try { window.manualRefreshStatus?.(); } catch {}
    }, 0);
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    if (!opts.silent) note("Tofa profile saved");
    return true;
  }

  function ensureInstanceUI() {
    profile.ensureUI(() => {
      viewGen += 1;
      stopPendingLogin(true);
      if (el("tofa_server_url")) el("tofa_server_url").value = "";
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
    bind("tofa_connect", startLogin);
    bind("tofa_cancel", cancelLogin);
    bind("tofa_disconnect", disconnect);
    const copy = el("tofa_code_copy");
    if (copy && !copy.__wired) {
      copy.addEventListener("click", (ev) => {
        ev.preventDefault();
        void Shared.copyField("tofa_code_input", copy, { emptyMessage: "Nothing to copy" });
      });
      copy.__wired = true;
    }
    const pin = el("tofa_profile_pin");
    if (pin && !pin.__wired) { pin.addEventListener("input", notifyModalState); pin.__wired = true; }
  }

  function mountUsers() {
    const host = el("tofa_sync_users");
    if (!host) return;
    if (!connected) {
      host.__cwSyncUsersState = null;
      host.innerHTML = '<div class="cw-wl cw-su"><div class="cw-wl-empty">Connect Tofa and choose a profile before adding household users.</div></div>';
      return;
    }
    try { window.CW?.SyncUsers?.mount(host, "tofa", profile.getInstance ? profile.getInstance() : "default"); } catch {}
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
    const gen = viewGen;
    const r = await fetchJSON(api("/api/tofa/libraries"), { cache: "no-store" });
    if (gen !== viewGen) return;
    libraries = r.ok && r.data?.ok && Array.isArray(r.data.libraries) ? r.data.libraries : [];
    wlHandle?.render?.();
  }

  async function mountWhitelist() {
    const host = el("tofa_lib_matrix");
    if (!host) return;
    if (!connected) {
      wlHandle = null;
      host.innerHTML = '<div class="cw-wl"><div class="cw-wl-empty">Connect Tofa and choose a profile before whitelisting libraries.</div></div>';
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
        features: [{ key: "hist", label: "History" }, { key: "prog", label: "Progress" }, { key: "coll", label: "Collections" }, { key: "scr", label: "Scrobble", title: "Libraries the watcher reads from. Scrobble destinations use History and Progress, or the libraries set on the route." }],
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

  function mergeTofaIntoCfg(cfg) {
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
      if (!tab || !tab.closest?.("#sec-tofa, .cw-connection-modal-panel[data-cw-connection-provider='tofa']")) return;
      setTimeout(tab.dataset.sub === "users" ? mountUsers : mountWhitelist, 0);
    });
    document.addEventListener("click", (ev) => {
      const btn = ev.target?.closest?.("#tofa_connect, #tofa_cancel, #tofa_disconnect");
      if (!btn || btn.__wired) return;
      ev.preventDefault();
      if (btn.id === "tofa_connect") void startLogin();
      else if (btn.id === "tofa_cancel") void cancelLogin();
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
      if (!(overlay.classList.contains("hidden") || overlay.getAttribute("aria-hidden") === "true")) return;
      if (poller?.isRunning?.()) void cancelLogin();
      else loginGen += 1;
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

  window.initTofaAuthUI = boot;
  window.cwAuth = window.cwAuth || {};
  window.cwAuth.tofa = { init: boot, rehydrate: refresh, saveSelectedProfile };
  window.mergeTofaIntoCfg = mergeTofaIntoCfg;
  window.registerSettingsCollector?.(mergeTofaIntoCfg);
  document.addEventListener("settings-collect", (e) => { try { mergeTofaIntoCfg(e?.detail?.cfg || (window.__cfg ||= {})); } catch {} }, true);
  boot();
})();
