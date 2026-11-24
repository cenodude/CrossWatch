// assets/auth/auth.punchplay.js
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._punchplayPatched) return;
  window._punchplayPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const VERIFY_URL = "https://punchplay.tv/link";
  const profile = Shared.createProfileAdapter({
    provider: "punchplay",
    configKey: "punchplay",
    label: "PunchPlay",
    sectionId: "sec-punchplay",
    selectId: "punchplay_instance",
    storageKey: "cw.ui.punchplay.auth.instance.v1",
  });
  let ppPoller = null;
  let ppStarting = false;

  const PP_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  const PP_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  let ppQcDeadline = 0, ppQcTimer = null, ppQcCopyRevert = null;

  function ppApi(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  function friendlyError(code, data = {}) {
    const key = String(code || "").trim();
    if (key === "rate_limited") {
      const local = data.rate_limit_source === "local" || (data.local && !data.rate_limit_source);
      const message = local ? T("connections.auth.localLimit") : T("connections.auth.gen.cooldown", {provider: "PunchPlay"});
      const seconds = Math.ceil(Number(data.retry_after));
      if (!Number.isFinite(seconds) || seconds <= 0) return T("connections.auth.tryLater", {message});
      const minutes = Math.floor(seconds / 60), remainder = seconds % 60;
      const wait = minutes ? minutes + "m" + (remainder ? " " + remainder + "s" : "") : seconds + "s";
      return T("connections.auth.tryIn", {message, wait});
    }
    const map = {
      missing_client_id: T("connections.auth.gen.noAppId", {provider: "PunchPlay"}),
      invalid_client: T("connections.auth.gen.rejectedAppId", {provider: "PunchPlay"}),
      unauthorized_client: T("connections.auth.gen.appInactive", {provider: "PunchPlay"}),
      invalid_scope: T("connections.auth.gen.appScope", {provider: "PunchPlay"}),
      invalid_grant: T("connections.auth.gen.accessExpired", {provider: "PunchPlay"}),
      expired: T("connections.auth.linkExpired"),
      network_error: T("connections.validation.reachFailed", {provider: "PunchPlay"}),
      device_start_failed: T("connections.auth.gen.deviceStartFailedShort", {provider: "PunchPlay"}),
    };
    return map[key] || key.replace(/_/g, " ") || T("connections.auth.gen.requestFailedShort", {provider: "PunchPlay"});
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked(["punchplay_device_start", "punchplay_device_restart"], !!ok); } catch {}
    return Shared.setStatus("punchplay_msg", ok, msg);
  }

  function emitConnected() {
    try { document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "punchplay", key: "PUNCHPLAY" } })); } catch {}
  }

  function ppQcSetState(show) {
    const box = el("punchplay_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const start = el("punchplay_device_start"), cancel = el("punchplay_device_cancel"), restart = el("punchplay_device_restart");
    if (start) start.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.add("hidden");
  }

  function ppQcShowRestart() {
    const restart = el("punchplay_device_restart"), start = el("punchplay_device_start"), cancel = el("punchplay_device_cancel");
    if (restart) restart.classList.remove("hidden");
    if (start) start.classList.add("hidden");
    if (cancel) cancel.classList.add("hidden");
  }

  function ppQcUpdateTimer() {
    if (ppQcDeadline && Date.now() > ppQcDeadline) { ppQcTimeout(); return; }
    const t = el("punchplay_qc_timer"); if (!t) return;
    const left = Math.max(0, Math.round((ppQcDeadline - Date.now()) / 1000));
    const mm = Math.floor(left / 60), ss = String(left % 60).padStart(2, "0");
    t.textContent = left > 0 ? T("connections.auth.expiresIn", {time: mm + ":" + ss}) : "";
  }

  function ppQcStop() {
    if (ppQcTimer) { clearInterval(ppQcTimer); ppQcTimer = null; }
    stopPoll();
    ppQcDeadline = 0;
    ppQcSetState(false);
    setQr("");
  }

  function ppQcTimeout() {
    if (ppQcTimer) { clearInterval(ppQcTimer); ppQcTimer = null; }
    stopPoll();
    ppQcDeadline = 0;
    const st = el("punchplay_qc_status"); if (st) st.textContent = T("connections.auth.codeExpiredRestart");
    const t = el("punchplay_qc_timer"); if (t) t.textContent = "";
    ppQcShowRestart();
  }

  function setQr(src) {
    const wrap = el("punchplay_qc_qrwrap"), img = el("punchplay_qc_qr");
    const ok = !!src && /^data:image\//i.test(String(src));
    if (img) img.src = ok ? src : "";
    if (wrap) wrap.classList.toggle("hidden", !ok);
  }

  function ppQcShowCode(code, secondsLeft) {
    const codeInput = el("punchplay_device_code"); if (codeInput) codeInput.value = code || "";
    const codeEl = el("punchplay_qc_code"); if (codeEl) codeEl.textContent = code || "----–----";
    const st = el("punchplay_qc_status"); if (st) st.textContent = T("connections.auth.waitingApprovalEllipsis");
    ppQcDeadline = Date.now() + (Math.max(30, Number(secondsLeft) || 600) * 1000);
    ppQcSetState(true);
    ppQcUpdateTimer();
    if (ppQcTimer) clearInterval(ppQcTimer);
    ppQcTimer = setInterval(ppQcUpdateTimer, 1000);
  }

  async function ppQcCopy(btn) {
    const code = ((el("punchplay_qc_code") && el("punchplay_qc_code").textContent) || (el("punchplay_device_code") && el("punchplay_device_code").value) || "").replace(/\s+/g, "").trim();
    if (!code || /^-+$/.test(code.replace(/–/g, "-"))) return;
    let ok = false;
    ok = (await window.cwCopyText?.(code)) === true;
    if (!ok) { note(T("connections.auth.copyFailed")); return; }
    btn.classList.add("copied"); btn.innerHTML = PP_ICON_CHECK; btn.title = T("connections.auth.copiedBang");
    if (ppQcCopyRevert) clearTimeout(ppQcCopyRevert);
    ppQcCopyRevert = setTimeout(function () { btn.classList.remove("copied"); btn.innerHTML = PP_ICON_COPY; btn.title = T("connections.auth.copyCode"); }, 1400);
  }

  function ensurePunchPlayInstanceUI() {
    profile?.ensureUI(() => {
      try { ppQcStop(); } catch (_) {}
      void hydrate();
    });
  }

  async function refresh(showToast) {
    try {
      const r = await fetchJSON(ppApi("/api/punchplay/status"), { cache: "no-store" });
      const data = r.data || {};
      const ok = !!(r.ok && data.connected);
      if (data.pending && data.pending.user_code) {
        const code = el("punchplay_device_code");
        if (code) code.value = txt(data.pending.user_code);
        if (!(ppPoller && ppPoller.isRunning())) startPoll(Math.max(5, Number(data.pending.interval || 5)));
      }
      let msg = ok ? (data.username ? T("connections.auth.connectedAs", {name: data.username}) : T("connections.auth.connected")) : T("connections.auth.notConnected");
      if (data.pending && !data.connected) msg = T("connections.auth.gen.waitingApproval", {provider: "PunchPlay"});
      setConn(data.pending && !data.connected ? false : ok, msg);
      if (ok) { try { ppQcStop(); } catch (_) {} }
      if (showToast) note(ok ? T("connections.auth.gen.verified", {provider: "PunchPlay"}) : T("connections.auth.gen.notConnected", {provider: "PunchPlay"}));
    } catch {
      setConn(false, T("connections.auth.gen.verifyFailedShort", {provider: "PunchPlay"}));
      if (showToast) note(T("connections.auth.gen.verifyFailedShort", {provider: "PunchPlay"}));
    }
  }

  async function hydrate() {
    ensurePunchPlayInstanceUI();
    const cfg = window._cfgCache || await Shared.getConfig();
    const blk = profile ? profile.cfgBlock(cfg, true) : {};
    const pend = blk?._pending_device || {};
    if (pend.user_code) {
      const code = el("punchplay_device_code");
      if (code) code.value = txt(pend.user_code);
      const left = Math.max(0, Number(pend.expires_at || 0) * 1000 - Date.now());
      if (left > 0) {
        ppQcShowCode(txt(pend.user_code), Math.round(left / 1000));
        startPoll(Math.max(5, Number(pend.interval || 5)));
      }
    }
    await refresh(false);
  }

  function ensurePpPoller() {
    if (ppPoller) return ppPoller;
    ppPoller = Shared.createDevicePoll({
      url: () => ppApi("/api/punchplay/device/poll"),
      method: "POST",
      body: "{}",
      maxTotalMs: 660000,
      onAuthorized: async () => {
        try { ppQcStop(); } catch (_) {}
        note(T("connections.auth.gen.connected", {provider: "PunchPlay"}));
        await hydrate();
        emitConnected();
      },
      onExpired: () => ppQcTimeout(),
      onTimeout: () => ppQcTimeout(),
      onTerminal: (verdict) => {
        const label = friendlyError((verdict && verdict.message) || "");
        const st = el("punchplay_qc_status"); if (st) st.textContent = label;
        setConn(false, label);
        if (ppQcTimer) { clearInterval(ppQcTimer); ppQcTimer = null; }
        ppQcDeadline = 0;
        ppQcShowRestart();
      },
      classify: (status, data) => {
        if (data && data.ok) return { state: "authorized" };
        const s = String((data && (data.status || data.error)) || "");
        if (!s || s === "authorization_pending") return { state: "pending" };
        if (s === "slow_down" || s === "rate_limited") return { state: "slow_down" };
        if (s === "expired" || s === "expired_token" || s === "no_device_code") return { state: "expired" };
        if (s === "network_error" || s === "internal" || s === "server_error") return { state: "network" };
        return { state: "terminal", message: s };
      },
    });
    return ppPoller;
  }

  function stopPoll() { if (ppPoller) ppPoller.stop(); }

  function startPoll(intervalSec) {
    ensurePpPoller().start({
      intervalMs: Math.max(5, Number(intervalSec || 5)) * 1000,
      deadlineMs: ppQcDeadline || 0,
    });
  }

  async function onDeviceStart() {
    if (ppStarting) return;
    ppStarting = true;
    try { ppQcStop(); } catch (_) {}
    const startBtn = el("punchplay_device_start");
    if (startBtn) { startBtn.disabled = true; startBtn.classList.add("busy"); }

    const win = Shared.openWaitWindow("PunchPlay", T("connections.auth.gen.requestingCode", {provider: "PunchPlay"}), "PUNCHPLAY");
    try {
      const r = await fetchJSON(ppApi("/api/punchplay/device/start"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store"
      });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(friendlyError(data.error || "device_start_failed", data));

      const code = txt(data.user_code);
      const url = txt(data.verification_uri_complete) || txt(data.verification_uri) || VERIFY_URL;
      const secs = Number(data.expires_in || 0) || 600;

      const helpEl = el("punchplay_qc_help");
      if (helpEl) helpEl.textContent = win
        ? T("connections.auth.pp.opening")
        : T("connections.auth.pp.open");

      ppQcShowCode(code, secs);
      setQr(txt(data.verification_uri_qr));
      setConn(false, T("connections.auth.waitingForApproval"));
      startPoll(Math.max(5, Number(data.interval || 5)));

      if (!Shared.showWaitCode(win, { key: "PUNCHPLAY", label: "PunchPlay", code, url })) {
        note(T("connections.auth.pp.popup"));
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      try { ppQcStop(); } catch (_) {}
      const msg = e && e.message ? e.message : String(e);
      setConn(false, msg);
      note(T("connections.auth.deviceFailed", {provider: "PunchPlay", error: msg}));
    } finally {
      ppStarting = false;
      if (startBtn) { startBtn.disabled = false; startBtn.classList.remove("busy"); }
    }
  }

  async function onCancel() {
    try { ppQcStop(); } catch (_) {}
    try { await fetchJSON(ppApi("/api/punchplay/device/cancel"), { method: "POST" }); } catch (_) {}
    setConn(false, T("connections.auth.notConnected"));
  }

  async function onDisc() {
    try { ppQcStop(); } catch (_) {}
    try {
      const r = await fetchJSON(ppApi("/api/punchplay/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      const code = el("punchplay_device_code"); if (code) code.value = "";
      const codeEl = el("punchplay_qc_code"); if (codeEl) codeEl.textContent = "----–----";
      setConn(false);
      note(T("connections.auth.gen.disconnected", {provider: "PunchPlay"}));
      await hydrate();
    } catch {
      note(T("connections.auth.gen.disconnectFailed", {provider: "PunchPlay"}));
    }
  }

  function wire() {
    const start = el("punchplay_device_start");
    if (start && !start.__wired) { start.addEventListener("click", onDeviceStart); start.__wired = true; }
    const copyCode = el("punchplay_qc_copy");
    if (copyCode && !copyCode.__wired) { copyCode.addEventListener("click", (e) => { e.preventDefault(); ppQcCopy(copyCode); }); copyCode.__wired = true; }
    const cancel = el("punchplay_device_cancel");
    if (cancel && !cancel.__wired) { cancel.addEventListener("click", onCancel); cancel.__wired = true; }
    const restart = el("punchplay_device_restart");
    if (restart && !restart.__wired) { restart.addEventListener("click", () => onDeviceStart()); restart.__wired = true; }
    const d = el("punchplay_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => { ensurePunchPlayInstanceUI(); wire(); });
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    ensurePunchPlayInstanceUI();
    wire();
    watch();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  window.initPunchPlayAuthUI = boot;
  boot();
})();
