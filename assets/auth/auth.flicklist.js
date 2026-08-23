// assets/auth/auth.flicklist.js
(function () {
  if (window._flicklistPatched) return;
  window._flicklistPatched = true;

  const Shared = window.CW?.AuthShared;
  if (!Shared) return;
  const { el, txt, fetchJSON } = Shared;
  const VERIFY_URL = "https://flicklist.tv/link";

  const profile = Shared.createProfileAdapter({
    provider: "flicklist",
    configKey: "flicklist",
    label: "FlickList",
    sectionId: "sec-flicklist",
    selectId: "flicklist_instance",
    storageKey: "cw.ui.flicklist.auth.instance.v1",
  });

  let poller = null;
  let countdownTimer = 0;
  let expiresAt = 0;
  let methodOverride = "";

  function flApi(path) { return profile.api(path); }

  function note(msg, ok, lockConnect) {
    try { Shared.setConnectLocked(["flicklist_save", "flicklist_device_start"], lockConnect == null ? !!ok : !!lockConnect); } catch {}
    return Shared.setStatus("flicklist_msg", ok, msg);
  }

  function emitConnected() {
    try { document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "flicklist", key: "FLICKLIST" } })); } catch {}
  }

  function setPolling(show) {
    const box = el("flicklist_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const start = el("flicklist_device_start"), cancel = el("flicklist_device_cancel");
    if (start) start.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
  }

  function activeMethodFromStatus(data) {
    if (data?.pending) return "device_code";
    if (data?.api_key_configured && !data?.session_configured && !data?.expires_at) return "api_key";
    if (data?.auth_method === "api_key") return "api_key";
    return "device_code";
  }

  function setMethodUI(method) {
    const m = method === "api_key" ? "api_key" : "device_code";
    const hidden = el("flicklist_auth_method");
    if (hidden) hidden.value = m;
    document.querySelectorAll("#sec-flicklist .fl-method").forEach((btn) => {
      const on = (btn.dataset.method || "") === m;
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
    document.querySelectorAll("#sec-flicklist [data-method-actions]").forEach((node) => {
      node.classList.toggle("hidden", (node.dataset.methodActions || "") !== m);
    });
    const dev = el("flicklist_device_panel");
    const api = el("flicklist_api_panel");
    if (dev) dev.style.display = m === "device_code" ? "" : "none";
    if (api) api.style.display = m === "api_key" ? "" : "none";
  }

  function setApiHintVisible(visible) {
    const h = el("flicklist_hint");
    if (!h) return;
    h.classList.toggle("hidden", !visible);
    h.style.display = visible ? "" : "none";
  }

  function stopPoll() {
    try { poller?.stop?.(); } catch {}
    clearInterval(countdownTimer);
    countdownTimer = 0;
    expiresAt = 0;
    setPolling(false);
  }

  function updateCountdown() {
    const t = el("flicklist_qc_timer"); if (!t) return;
    if (!expiresAt) { t.textContent = ""; return; }
    const left = Math.max(0, Math.ceil((expiresAt * 1000 - Date.now()) / 1000));
    const min = Math.floor(left / 60), sec = left % 60;
    t.textContent = left ? `Expires in ${min}:${String(sec).padStart(2, "0")}` : "";
    if (!left) expireCode();
  }

  function expireCode() {
    try { poller?.stop?.(); } catch {}
    clearInterval(countdownTimer);
    countdownTimer = 0;
    const st = el("flicklist_qc_status"); if (st) st.textContent = "Link code expired. Start again.";
    const t = el("flicklist_qc_timer"); if (t) t.textContent = "";
    setPolling(false);
  }

  function setCode(data) {
    const code = txt(data?.user_code);
    const codeInput = el("flicklist_device_code"); if (codeInput) codeInput.value = code || "";
    const codeEl = el("flicklist_qc_code"); if (codeEl) codeEl.textContent = code || "--------";
    const st = el("flicklist_qc_status"); if (st) st.textContent = "Waiting for approval...";
    expiresAt = Number(data?.expires_at || 0) || Math.floor(Date.now() / 1000) + Number(data?.expires_in || 900);
    clearInterval(countdownTimer);
    countdownTimer = setInterval(updateCountdown, 1000);
    updateCountdown();
    setPolling(true);
  }

  function friendlyError(status) {
    const s = String(status || "").toLowerCase();
    if (s === "missing_client_id") return "FlickList client id is missing.";
    if (s === "authorization_pending") return "Still waiting for approval.";
    if (s === "expired" || s === "expired_token") return "The code expired. Start again.";
    if (s === "slow_down" || s === "rate_limited") return "FlickList asked us to slow down. Waiting a bit.";
    if (s === "access_denied") return "FlickList approval was denied.";
    if (s === "invalid_api_key") return "FlickList rejected that API key.";
    return s ? s.replace(/_/g, " ") : "FlickList request failed.";
  }

  async function hydrate() {
    try {
      const r = await fetchJSON(flApi("/api/flicklist/status"), { cache: "no-store" });
      const d = r.data || {};
      const connected = !!d.connected;
      const statusMethod = activeMethodFromStatus(d);
      const method = methodOverride || statusMethod;
      setMethodUI(method);
      note(connected ? (statusMethod === "api_key" ? "Connected with API key" : "Connected with Device Code") : "Ready for Device Code.", connected, connected);
      Shared.maskSecret(el("flicklist_api_key"), !!d.api_key_configured);
      setApiHintVisible(!d.api_key_configured);
      if (d.pending && d.pending.user_code && !poller?.isRunning?.()) {
        setCode(d.pending);
        startPolling(d.pending);
      }
    } catch (_) {
      note("FlickList status check failed.", false);
    }
  }

  function startPolling(data) {
    if (!poller) {
      poller = Shared.createDevicePoll({
        url: () => flApi("/api/flicklist/device/poll"),
        minIntervalMs: 5000,
        maxTotalMs: 900000,
        body: "{}",
        classify: (_status, body) => {
          const s = String(body?.status || body?.error || "").toLowerCase();
          if (body?.ok || s === "authorized") return { state: "authorized" };
          if (s === "expired" || s === "expired_token" || s === "no_device_code") return { state: "expired" };
          if (s === "slow_down" || s === "rate_limited") return { state: "slow_down" };
          if (s && !["authorization_pending", "pending"].includes(s)) return { state: "terminal", message: friendlyError(s) };
          return { state: "pending" };
        },
        onPending: () => { const st = el("flicklist_qc_status"); if (st) st.textContent = "Waiting for approval..."; },
        onAuthorized: async () => {
          stopPoll();
          note("FlickList connected.", true);
          emitConnected();
          try { window.invalidateConfigCache?.(); } catch {}
          try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
          await hydrate();
        },
        onExpired: () => { expireCode(); note("The FlickList code expired.", false); },
        onTerminal: (verdict) => { stopPoll(); note(verdict?.message || "FlickList authorization failed.", false); },
        onTimeout: () => { expireCode(); note("The FlickList code expired.", false); },
      });
    }
    poller.start({ intervalMs: Math.max(5, Number(data?.interval || 5)) * 1000, deadlineMs: (Number(data?.expires_at || 0) || 0) * 1000 });
  }

  async function saveApiKey() {
    const input = el("flicklist_api_key");
    const state = Shared.readSecretField(input);
    if (state.masked) { note("API key unchanged.", true); return; }
    if (!state.value) { note("Enter your FlickList API key.", false); return; }
    try {
      const r = await fetchJSON(flApi("/api/flicklist/save"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ api_key: state.value }),
        cache: "no-store",
      });
      const d = r.data || {};
      if (!r.ok || d.ok === false) throw new Error(friendlyError(d.error || "save_failed"));
      Shared.maskSecret(input, !!(state.value || d.username));
      setApiHintVisible(false);
      methodOverride = "";
      note(d.username ? "Connected as " + d.username : "FlickList saved.", true);
      emitConnected();
      try { window.invalidateConfigCache?.(); } catch {}
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await hydrate();
    } catch (e) {
      note(e?.message || "FlickList save failed.", false);
    }
  }

  async function startDevice() {
    stopPoll();
    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch (_) {}
    const startBtn = el("flicklist_device_start");
    if (startBtn) { startBtn.disabled = true; startBtn.classList.add("busy"); }
    note("Requesting a FlickList link code...", true, false);
    try {
      const r = await fetchJSON(flApi("/api/flicklist/device/start"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ credential: "session" }),
        cache: "no-store",
      });
      const d = r.data || {};
      if (!r.ok || !d.ok) throw new Error(friendlyError(d.error || "device_start_failed"));
      const url = txt(d.verification_uri_complete) || txt(d.verification_uri) || VERIFY_URL;
      setCode(d);
      startPolling(d);
      note("Enter the code at flicklist.tv/link.", true, false);
      if (win && !win.closed) {
        try {
          win.document.write(
            '<!doctype html><meta charset="utf-8"><title>CrossWatch - FlickList</title>' +
            '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
            '<div><div style="font-size:14px;opacity:.7;margin-bottom:12px">Opening the FlickList approval page...</div>' +
            '<div style="font-size:36px;font-weight:800;letter-spacing:.18em;color:#58d0f8;text-transform:uppercase">' + txt(d.user_code) + '</div>' +
            '<div style="font-size:12px;opacity:.6;margin-top:12px">Redirecting in a moment...</div></div></body>'
          );
        } catch (_) {}
        setTimeout(() => { try { if (win && !win.closed) win.location.href = url; } catch (_) {} }, 3000);
      } else {
        note("Popup blocked - open flicklist.tv/link and enter the code.", false);
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      stopPoll();
      note(e?.message || "Could not start FlickList device login.", false);
    } finally {
      if (startBtn) { startBtn.disabled = false; startBtn.classList.remove("busy"); }
    }
  }

  async function cancelDevice() {
    try { await fetchJSON(flApi("/api/flicklist/device/cancel"), { method: "POST", cache: "no-store" }); } catch (_) {}
    stopPoll();
    note("FlickList login cancelled.", false);
  }

  async function disconnect() {
    try {
      const r = await fetchJSON(flApi("/api/flicklist/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage?.(r)) return;
      const d = r.data || {};
      if (!r.ok || d.ok === false) throw new Error(d.error || "disconnect_failed");
      stopPoll();
      Shared.maskSecret(el("flicklist_api_key"), false);
      setApiHintVisible(true);
      const code = el("flicklist_device_code"); if (code) code.value = "";
      const codeEl = el("flicklist_qc_code"); if (codeEl) codeEl.textContent = "--------";
      note("FlickList disconnected.", false);
      try { window.invalidateConfigCache?.(); } catch {}
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    } catch (_) {
      note("Could not delete FlickList connection.", false);
    }
  }

  function init() {
    profile.ensureUI(hydrate);
    setMethodUI("device_code");
    Shared.wireSecretInput(el("flicklist_api_key"));
    document.querySelectorAll("#sec-flicklist .fl-method").forEach((btn) => {
      if (btn.__flMethod) return;
      btn.__flMethod = true;
      btn.addEventListener("click", () => {
        const method = btn.dataset.method === "api_key" ? "api_key" : "device_code";
        methodOverride = method;
        if (method === "api_key") stopPoll();
        setMethodUI(method);
      });
    });
    const save = el("flicklist_save"); if (save && !save.__fl) { save.__fl = true; save.addEventListener("click", saveApiKey); }
    const start = el("flicklist_device_start"); if (start && !start.__fl) { start.__fl = true; start.addEventListener("click", startDevice); }
    const cancel = el("flicklist_device_cancel"); if (cancel && !cancel.__fl) { cancel.__fl = true; cancel.addEventListener("click", cancelDevice); }
    ["flicklist_disconnect_device", "flicklist_disconnect_api"].forEach((id) => {
      const del = el(id); if (del && !del.__fl) { del.__fl = true; del.addEventListener("click", disconnect); }
    });
    hydrate();
  }

  window.addEventListener("settings-changed", () => {
    try { hydrate(); } catch {}
  });

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.flicklist = { init };
  window.initFlickListAuthUI = init;
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
