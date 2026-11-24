// assets/auth/auth.bingebase.js
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._bingebasePatched) return;
  window._bingebasePatched = true;

  const Shared = window.CW?.AuthShared;
  if (!Shared) return;
  const { el, txt, fetchJSON } = Shared;
  const VERIFY_URL = "https://bingebase.com/activate";

  const profile = Shared.createProfileAdapter({
    provider: "bingebase",
    configKey: "bingebase",
    label: "BingeBase",
    sectionId: "sec-bingebase",
    selectId: "bingebase_instance",
    storageKey: "cw.ui.bingebase.auth.instance.v1",
  });

  let poller = null;
  let countdownTimer = 0;
  let expiresAt = 0;

  function bbApi(path) { return profile.api(path); }
  function note(msg, ok, lockConnect) {
    try { Shared.setConnectLocked(["bingebase_device_start", "bingebase_device_restart"], lockConnect == null ? !!ok : !!lockConnect); } catch {}
    return Shared.setStatus("bingebase_msg", ok, msg);
  }
  function emitConnected() {
    try { document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "bingebase", key: "BINGEBASE" } })); } catch {}
  }
  function writeForwardingPage(win, code, url) {
    return Shared.showWaitCode(win, { key: "BINGEBASE", label: "BingeBase", code, url: url || VERIFY_URL });
  }
  function setPolling(show) {
    const box = el("bingebase_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const start = el("bingebase_device_start"), cancel = el("bingebase_device_cancel"), restart = el("bingebase_device_restart");
    if (start) start.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.toggle("hidden", !show);
  }
  function stopPoll() {
    try { poller?.stop?.(); } catch {}
    clearInterval(countdownTimer);
    countdownTimer = 0;
    setPolling(false);
    const restart = el("bingebase_device_restart"), start = el("bingebase_device_start"), cancel = el("bingebase_device_cancel");
    if (restart) restart.classList.add("hidden");
    if (start) start.classList.remove("hidden");
    if (cancel) cancel.classList.add("hidden");
  }
  function updateCountdown() {
    const t = el("bingebase_qc_timer"); if (!t) return;
    if (!expiresAt) { t.textContent = ""; return; }
    const left = Math.max(0, Math.ceil((expiresAt * 1000 - Date.now()) / 1000));
    const min = Math.floor(left / 60), sec = left % 60;
    t.textContent = left ? `${min}:${String(sec).padStart(2, "0")}` : "";
    if (!left) expireCode();
  }
  function expireCode() {
    try { poller?.stop?.(); } catch {}
    clearInterval(countdownTimer);
    const st = el("bingebase_qc_status"); if (st) st.textContent = T("connections.auth.codeExpiredRestart");
    const t = el("bingebase_qc_timer"); if (t) t.textContent = "";
    const cancel = el("bingebase_device_cancel"), restart = el("bingebase_device_restart");
    if (cancel) cancel.classList.add("hidden");
    if (restart) restart.classList.remove("hidden");
  }
  function setCode(data) {
    const code = txt(data?.user_code);
    const codeInput = el("bingebase_device_code"); if (codeInput) codeInput.value = code || "";
    const codeEl = el("bingebase_qc_code"); if (codeEl) codeEl.textContent = code || "----";
    const st = el("bingebase_qc_status"); if (st) st.textContent = T("connections.auth.waitingApproval");
    expiresAt = Number(data?.expires_at || 0) || Math.floor(Date.now() / 1000) + Number(data?.expires_in || 600);
    clearInterval(countdownTimer);
    countdownTimer = setInterval(updateCountdown, 1000);
    updateCountdown();
    setPolling(true);
  }
  async function copyCode() {
    const code = ((el("bingebase_qc_code") && el("bingebase_qc_code").textContent) || (el("bingebase_device_code") && el("bingebase_device_code").value) || "").replace(/\s+/g, "").trim();
    await Shared.copyText(code, el("bingebase_qc_copy"), { copiedText: T("connections.auth.copied"), emptyMessage: T("connections.auth.noCode") });
  }
  function friendlyError(status) {
    const s = String(status || "").toLowerCase();
    if (s === "authorization_pending") return T("connections.auth.stillWaiting");
    if (s === "expired" || s === "expired_token") return T("connections.auth.codeExpiredRestart2");
    if (s === "slow_down" || s === "rate_limited") return T("connections.auth.bb.slowDown");
    if (s === "no_device_code") return T("connections.auth.startDevice");
    return T("connections.auth.gen.authFailedDot", {provider: "BingeBase"});
  }
  function revealDeviceWebhook(url) {
    const wh = el("bingebase_webhook_url");
    const value = txt(url);
    if (!wh || !value) return;
    try { wh.type = "text"; } catch (_) {}
    wh.value = value;
    wh.dataset.masked = "0";
    wh.dataset.loaded = "1";
    wh.dataset.touched = "";
    wh.dataset.clear = "";
    wh.dataset.hasKey = "1";
    wh.dataset.revealedFromDevice = "1";
  }
  async function hydrate() {
    try {
      const r = await fetchJSON(bbApi("/api/bingebase/status"), { cache: "no-store" });
      const d = r.data || {};
      const connected = !!d.connected;
      const webhookConfigured = !!d.webhook_configured;
      const apiKeyConfigured = !!d.api_key_configured;
      if (connected && webhookConfigured) note(T("connections.auth.connectedWebhook"), true, true);
      else if (connected) note(T("connections.auth.connectedDot"), true, true);
      else if (webhookConfigured) note(T("connections.auth.webhookStaged"), true, false);
      else note(T("connections.auth.notConnectedDot"), false);
      const wh = el("bingebase_webhook_url");
      if (wh) {
        try { wh.type = "password"; } catch (_) {}
        wh.dataset.revealedFromDevice = "";
        Shared.maskSecret(wh, webhookConfigured);
      }
      const api = el("bingebase_api_key");
      if (api) {
        Shared.maskSecret(api, apiKeyConfigured);
      }
      if (d.pending && d.pending.user_code && !poller?.isRunning?.()) {
        setCode(d.pending);
        startPolling(d.pending);
      }
    } catch (_) {}
  }
  function startPolling(data) {
    if (!poller) {
      poller = Shared.createDevicePoll({
        url: () => bbApi("/api/bingebase/device/poll"),
        minIntervalMs: 5000,
        maxTotalMs: 600000,
        body: "{}",
        classify: (_status, body) => {
          const s = String(body?.status || "").toLowerCase();
          if (body?.ok || s === "authorized") return { state: "authorized" };
          if (s === "expired" || s === "expired_token") return { state: "expired" };
          if (s === "slow_down" || s === "rate_limited") return { state: "slow_down" };
          if (s && !["authorization_pending", "pending"].includes(s)) return { state: "terminal", message: friendlyError(s) };
          return { state: "pending" };
        },
        onPending: () => { const st = el("bingebase_qc_status"); if (st) st.textContent = T("connections.auth.waitingApproval"); },
        onAuthorized: (data) => {
          stopPoll();
          note(T("connections.auth.gen.connectedDot", {provider: "BingeBase"}), true);
          emitConnected();
          try { window.invalidateConfigCache?.(); } catch {}
          try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
          void hydrate().then(() => revealDeviceWebhook(data?.generated_webhook_url));
        },
        onExpired: () => { expireCode(); note(T("connections.auth.gen.codeExpired", {provider: "BingeBase"}), false); },
        onTerminal: (verdict) => { stopPoll(); note(verdict?.message || T("connections.auth.gen.authFailedDot", {provider: "BingeBase"}), false); },
        onTimeout: () => { expireCode(); note(T("connections.auth.gen.codeExpired", {provider: "BingeBase"}), false); },
      });
    }
    const interval = Number(data?.interval || 5) * 1000;
    poller.start({ intervalMs: interval, deadlineMs: (Number(data?.expires_at || 0) || 0) * 1000 });
  }
  async function startDevice() {
    stopPoll();
    const win = Shared.openWaitWindow("BingeBase", T("connections.auth.gen.requestingCode", {provider: "BingeBase"}), "BINGEBASE");
    const startBtn = el("bingebase_device_start");
    if (startBtn) { startBtn.disabled = true; startBtn.classList.add("busy"); }
    note(T("connections.auth.gen.requestingLinkCode", {provider: "BingeBase"}), true, false);
    try {
      const r = await fetchJSON(bbApi("/api/bingebase/device/start"), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}", cache: "no-store" });
      const d = r.data || {};
      if (!r.ok || !d.ok) throw new Error(d.error || "device_start_failed");
      const url = txt(d.verification_uri || d.verification_url) || VERIFY_URL;
      setCode(d);
      startPolling(d);
      note(T("connections.auth.bb.enterCode"), true, false);
      if (!writeForwardingPage(win, txt(d.user_code), url)) {
        note(T("connections.auth.bb.popup"), false);
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      stopPoll();
      note(T("connections.auth.gen.deviceStartFailed", {provider: "BingeBase"}), false);
    } finally {
      if (startBtn) { startBtn.disabled = false; startBtn.classList.remove("busy"); }
    }
  }
  async function cancelDevice() {
    try { await fetchJSON(bbApi("/api/bingebase/device/cancel"), { method: "POST", cache: "no-store" }); } catch (_) {}
    stopPoll();
    note(T("connections.auth.gen.cancelledDot", {provider: "BingeBase"}), false);
  }
  function cfgBlock(cfg, create) {
    return profile ? profile.cfgBlock(cfg, create !== false) : {};
  }
  function collectSecretField(cfg, inputId, key) {
    if (!cfg) return;
    const input = el(inputId);
    if (!input) return;
    const state = Shared.readSecretField(input);
    const touched = input.dataset.touched === "1";
    if (state.masked) return;
    if (!state.value && !touched) return;
    const block = cfgBlock(cfg, true);
    block[key] = state.value || "";
  }
  function collectRealtime(cfg) {
    collectSecretField(cfg, "bingebase_webhook_url", "webhook_url");
    collectSecretField(cfg, "bingebase_api_key", "api_key");
  }
  async function disconnect() {
    try {
      const r = await fetchJSON(bbApi("/api/bingebase/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage?.(r)) return;
      const d = r.data || {};
      if (!r.ok || d.ok === false) throw new Error(d.error || "disconnect_failed");
      stopPoll();
      const code = el("bingebase_device_code"); if (code) code.value = "";
      const codeEl = el("bingebase_qc_code"); if (codeEl) codeEl.textContent = "----";
      Shared.maskSecret(el("bingebase_webhook_url"), false);
      Shared.maskSecret(el("bingebase_api_key"), false);
      note(T("connections.auth.gen.disconnectedDot", {provider: "BingeBase"}), false);
      try { window.invalidateConfigCache?.(); } catch {}
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
    } catch (_) {
      note(T("connections.auth.gen.couldNotDeleteConnection", {provider: "BingeBase"}), false);
    }
  }
  function init() {
    profile.ensureUI(hydrate);
    Shared.wireSecretInput(el("bingebase_webhook_url"));
    Shared.wireSecretInput(el("bingebase_api_key"));
    const start = el("bingebase_device_start"); if (start && !start.__bb) { start.__bb = true; start.addEventListener("click", startDevice); }
    const copy = el("bingebase_qc_copy"); if (copy && !copy.__bb) { copy.__bb = true; copy.addEventListener("click", copyCode); }
    const cancel = el("bingebase_device_cancel"); if (cancel && !cancel.__bb) { cancel.__bb = true; cancel.addEventListener("click", cancelDevice); }
    const restart = el("bingebase_device_restart"); if (restart && !restart.__bb) { restart.__bb = true; restart.addEventListener("click", startDevice); }
    const del = el("bingebase_disconnect"); if (del && !del.__bb) { del.__bb = true; del.addEventListener("click", disconnect); }
    hydrate();
  }

  document.addEventListener("settings-collect", (ev) => {
    try { collectRealtime(ev?.detail?.cfg || (window.__cfg ||= {})); } catch {}
  }, true);

  window.addEventListener("settings-changed", () => {
    try { hydrate(); } catch {}
  });

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.bingebase = { init };
  window.initBingeBaseAuthUI = init;
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
