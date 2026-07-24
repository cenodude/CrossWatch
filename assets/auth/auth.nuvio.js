// assets/auth/auth.nuvio.js
// CrossWatch - Nuvio Auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
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
  let poller = null;
  let expiryTimer = null;

  function api(path) {
    return profile.api(path);
  }

  function escapeHtml(value) {
    return txt(value).replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#39;",
    }[ch]));
  }

  function notifyModalState() {
    const panel = document.getElementById("sec-nuvio")?.closest?.(".cw-connection-modal-panel")
      || document.querySelector?.(".cw-connection-modal-panel[data-provider='nuvio'], .cw-connection-modal-panel");
    const save = panel?.querySelector?.(".cw-connection-footer-save");
    if (!save) return;
    const canSave = connected || (authenticated && !!txt(el("nuvio_profile_select")?.value));
    save.disabled = !canSave;
    save.classList.toggle("is-disabled", !canSave);
    if (canSave) save.removeAttribute("title");
    else save.title = "Connect this profile before saving.";
  }

  function syncConnectLocked() {
    try { Shared.setConnectLocked("nuvio_connect", connected); } catch {}
  }

  function setStatus(ok, msg) {
    const node = el("nuvio_msg");
    if (!node) return;
    node.classList.remove("hidden", "ok", "warn");
    node.classList.add(ok ? "ok" : "warn");
    node.textContent = msg || (ok ? "Connected" : "Not connected");
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
      a.textContent = url ? "Open Nuvio approval page" : "";
    }
    startExpiryTimer(data?.expires_at);
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
      node.textContent = "Expired";
      return;
    }
    const mins = Math.floor(left / 60);
    const secs = String(left % 60).padStart(2, "0");
    node.textContent = `Expires in ${mins}:${secs}`;
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
    wrap.classList.toggle("hidden", !(forceShow || rows.length || connected));
    sel.innerHTML = "";
    const hasSelected = !!(effectiveSelectedId && rows.some((row) => String(row.profile_id || row.profile_index || "") === String(effectiveSelectedId)));
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = rows.length
      ? (effectiveSelectedId ? "Selected profile unavailable" : "Select Nuvio profile")
      : "No Nuvio profiles loaded";
    placeholder.disabled = true;
    placeholder.selected = true;
    sel.appendChild(placeholder);
    rows.forEach((row) => {
      const option = document.createElement("option");
      option.value = String(row.profile_id || row.profile_index || "");
      option.textContent = String(row.name || `Profile ${option.value}`);
      sel.appendChild(option);
    });
    if (hasSelected) {
      sel.value = String(effectiveSelectedId);
    }
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
    if (r.ok && r.data?.ok) {
      renderProfiles(r.data.profiles || [], selectedId, selectedName, !!statusData?.authenticated);
    } else {
      renderProfiles([], selectedId, selectedName, !!statusData?.authenticated);
    }
  }

  async function refresh() {
    try {
      const r = await fetchJSON(api("/api/nuvio/status"), { cache: "no-store" });
      const data = r.data || {};
      connected = !!(r.ok && data.connected);
      authenticated = !!(r.ok && data.authenticated);
      syncConnectLocked();
      if (connected) setStatus(true, "Nuvio connected");
      else if (data.authenticated) hideStatus();
      else hideStatus();
      await refreshProfiles(data);
    } catch {
      connected = false;
      authenticated = false;
      syncConnectLocked();
      setStatus(false, "Nuvio status failed");
    }
  }

  async function finishLogin() {
    const r = await fetchJSON(api("/api/nuvio/device/finish"), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    if (!r.ok || !r.data?.ok) throw new Error(String(r.data?.status || "finish_failed"));
    setLoginState(false);
    setPolling("");
    note("Nuvio login approved");
    try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    authenticated = true;
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
        setPolling("Waiting for approval...");
      },
      onAuthorized() {
        finishLogin().catch((e) => {
          setStatus(false, friendlyError(e?.message));
          note("Nuvio login completion failed");
        });
      },
      onExpired() {
        setPolling("Link code expired. Connect again to retry.");
        setStatus(false, "Nuvio login expired");
      },
      onTerminal(verdict) {
        setStatus(false, friendlyError(verdict?.message || "poll_failed"));
      },
      onTimeout() {
        setStatus(false, "Nuvio login timed out");
      },
    });
    return poller;
  }

  function friendlyError(code) {
    switch (String(code || "")) {
      case "authentication_failed": return "Nuvio authentication failed";
      case "token_refresh_failed": return "Nuvio token refresh failed";
      case "anonymous_session_not_object": return "Nuvio anonymous login returned an unexpected response";
      case "missing_access_token": return "Nuvio anonymous login did not return an access token";
      case "missing_refresh_token": return "Nuvio anonymous login did not return a refresh token";
      case "start_session_not_list_object": return "Nuvio TV login returned an unexpected response";
      case "missing_code_or_qr_content": return "Nuvio TV login did not return a login URL";
      case "invalid_expiry": return "Nuvio TV login returned an invalid expiry";
      case "invalid_poll_interval": return "Nuvio TV login returned an invalid poll interval";
      case "invalid_response": return "Nuvio returned an unexpected response";
      case "service_unavailable": return "Nuvio service unavailable";
      case "profile_unavailable": return "Selected Nuvio profile is unavailable";
      default: return "Nuvio request failed";
    }
  }

  async function startLogin() {
    if (connected) return;
    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch {}
    try {
      const r = await fetchJSON(api("/api/nuvio/device/start"), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(String(data.error || "start_failed"));
      setLoginState(true, data);
      setStatus(false, "Waiting for Nuvio approval");
      setPolling("Waiting for approval...");
      const url = txt(data.login_url);
      if (url) {
        if (win && !win.closed) {
          try {
            const code = escapeHtml(txt(data.code).slice(0, 5).toUpperCase());
            win.document.write(
              '<!doctype html><meta charset="utf-8"><title>CrossWatch -> Nuvio</title>' +
              '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
              '<div style="padding:24px;max-width:86vw"><div style="font-size:14px;opacity:.7;margin-bottom:12px">Opening the Nuvio approval page...</div>' +
              '<div style="font-size:30px;font-weight:700;letter-spacing:.14em;color:#8ff0c2;line-height:1.35;word-break:break-all">' + code + '</div>' +
              '<div style="font-size:12px;opacity:.6;margin-top:12px">Redirecting in a moment...</div></div></body>'
            );
            win.document.close();
          } catch {}
          setTimeout(function () { try { if (win && !win.closed) win.location.href = url; } catch {} }, 3000);
        } else {
          try { window.open(url, "_blank", "noopener"); } catch {}
          note("Popup blocked - open the Nuvio approval link.");
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
    syncConnectLocked();
    stopPendingLogin(false);
    renderProfiles([], null, "", false);
      setStatus(false, "Nuvio disconnected");
      note("Nuvio disconnected");
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    } catch {
      note("Nuvio disconnect failed");
    }
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
        void Shared.copyField("nuvio_code_input", copy, { emptyMessage: "Nothing to copy" });
      });
      copy.__wired = true;
    }
    const del = el("nuvio_disconnect");
    if (del && !del.__wired) { del.addEventListener("click", disconnect); del.__wired = true; }
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
  window.cwAuth.nuvio = { init: boot, rehydrate: refresh };
  boot();
})();
