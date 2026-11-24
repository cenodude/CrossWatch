// assets/auth/auth.movieparadise.js
// CrossWatch - Movie Paradise device code authentication UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._movieparadisePatched) return;
  window._movieparadisePatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const VERIFY_URL = "https://movieparadise.app/oauth/device";
  const VERIFY_LABEL = "movieparadise.app/oauth/device";
  const profile = Shared.createProfileAdapter({
    provider: "movieparadise",
    configKey: "movieparadise",
    label: "Movie Paradise",
    sectionId: "sec-movieparadise",
    selectId: "movieparadise_instance",
    storageKey: "cw.ui.movieparadise.auth.instance.v1",
  });
  let mpPoller = null;
  let mpStarting = false;

  const MP_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  const MP_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  let mpQcDeadline = 0, mpQcTimer = null, mpQcCopyRevert = null;

  function mpApi(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  function cleanCode(value) {
    return txt(value).toUpperCase().replace(/[^A-Z0-9-]/g, "");
  }

  function friendlyError(code, data = {}) {
    const key = String(code || "").trim();
    if (key === "rate_limited") {
      const seconds = Math.ceil(Number(data.retry_after));
      if (!Number.isFinite(seconds) || seconds <= 0) return T("connections.auth.gen.busyLater", {provider: "Movie Paradise"});
      return T("connections.auth.busyIn", {provider: "Movie Paradise", seconds});
    }
    const map = {
      missing_client_id: T("connections.auth.gen.noAppId", {provider: "Movie Paradise"}),
      invalid_client: T("connections.auth.gen.rejectedAppId", {provider: "Movie Paradise"}),
      unauthorized_client: T("connections.auth.gen.appInactive", {provider: "Movie Paradise"}),
      invalid_scope: T("connections.auth.gen.appScope", {provider: "Movie Paradise"}),
      invalid_grant: T("connections.auth.gen.accessExpired", {provider: "Movie Paradise"}),
      access_denied: T("connections.auth.gen.declinedIn", {provider: "Movie Paradise"}),
      answer_lost: T("connections.auth.gen.noAnswer", {provider: "Movie Paradise"}),
      reconnect_required: T("connections.auth.gen.accessExpired", {provider: "Movie Paradise"}),
      expired_token: T("connections.auth.linkExpired"),
      network_error: T("connections.validation.reachFailed", {provider: "Movie Paradise"}),
      device_start_failed: T("connections.auth.gen.deviceStartFailedShort", {provider: "Movie Paradise"}),
    };
    return map[key] || key.replace(/_/g, " ") || T("connections.auth.gen.requestFailedShort", {provider: "Movie Paradise"});
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked(["movieparadise_device_start", "movieparadise_device_restart"], !!ok); } catch {}
    return Shared.setStatus("movieparadise_msg", ok, msg);
  }

  function emitConnected() {
    try { document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "movieparadise", key: "MOVIEPARADISE" } })); } catch {}
  }

  function mpQcSetState(show) {
    const box = el("movieparadise_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const start = el("movieparadise_device_start"), cancel = el("movieparadise_device_cancel"), restart = el("movieparadise_device_restart");
    if (start) start.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.add("hidden");
  }

  function mpQcShowRestart() {
    const restart = el("movieparadise_device_restart"), start = el("movieparadise_device_start"), cancel = el("movieparadise_device_cancel");
    if (restart) restart.classList.remove("hidden");
    if (start) start.classList.add("hidden");
    if (cancel) cancel.classList.add("hidden");
  }

  function mpQcUpdateTimer() {
    if (mpQcDeadline && Date.now() > mpQcDeadline) { mpQcTimeout(); return; }
    const t = el("movieparadise_qc_timer"); if (!t) return;
    const left = Math.max(0, Math.round((mpQcDeadline - Date.now()) / 1000));
    const mm = Math.floor(left / 60), ss = String(left % 60).padStart(2, "0");
    t.textContent = left > 0 ? T("connections.auth.expiresIn", {time: mm + ":" + ss}) : "";
  }

  function mpQcStop() {
    if (mpQcTimer) { clearInterval(mpQcTimer); mpQcTimer = null; }
    stopPoll();
    mpQcDeadline = 0;
    mpQcSetState(false);
  }

  function mpQcTimeout() {
    if (mpQcTimer) { clearInterval(mpQcTimer); mpQcTimer = null; }
    stopPoll();
    mpQcDeadline = 0;
    const st = el("movieparadise_qc_status"); if (st) st.textContent = T("connections.auth.codeExpiredRestart");
    const t = el("movieparadise_qc_timer"); if (t) t.textContent = "";
    mpQcShowRestart();
  }

  function mpQcShowCode(code, deadlineMs) {
    const codeEl = el("movieparadise_qc_code"); if (codeEl) codeEl.textContent = code || "----–----";
    const st = el("movieparadise_qc_status"); if (st) st.textContent = T("connections.auth.waitingApprovalEllipsis");
    mpQcDeadline = Number(deadlineMs) || (Date.now() + 600000);
    mpQcSetState(true);
    mpQcUpdateTimer();
    if (mpQcTimer) clearInterval(mpQcTimer);
    mpQcTimer = setInterval(mpQcUpdateTimer, 1000);
  }

  async function mpQcCopy(btn) {
    const code = cleanCode(el("movieparadise_qc_code") && el("movieparadise_qc_code").textContent);
    if (!code || /^-+$/.test(code)) return;
    const ok = await Shared.copyText(code, null, { failureMessage: T("connections.auth.copyFailed") });
    if (!ok || !btn) return;
    btn.classList.add("copied"); btn.innerHTML = MP_ICON_CHECK; btn.title = T("connections.auth.copiedBang");
    if (mpQcCopyRevert) clearTimeout(mpQcCopyRevert);
    mpQcCopyRevert = setTimeout(function () { btn.classList.remove("copied"); btn.innerHTML = MP_ICON_COPY; btn.title = T("connections.auth.copyCode"); }, 1400);
  }

  function ensureMovieParadiseInstanceUI() {
    profile?.ensureUI(() => {
      try { mpQcStop(); } catch (_) {}
      void hydrate();
    });
  }

  async function refresh(showToast) {
    try {
      const r = await Shared.fetchJSON(mpApi("/api/movieparadise/status"), { cache: "no-store" });
      const data = r.data || {};
      const ok = !!(r.ok && data.connected && !data.reconnect_required);
      if (data.pending && data.pending.user_code && !(mpPoller && mpPoller.isRunning())) {
        const deadline = Number(data.pending.expires_at || 0) * 1000;
        if (deadline > Date.now()) {
          mpQcShowCode(cleanCode(data.pending.user_code), deadline);
          startPoll(data.pending.interval);
        }
      }
      let msg = ok ? (data.username ? T("connections.auth.connectedAs", {name: data.username}) : T("connections.auth.connected")) : T("connections.auth.notConnected");
      if (data.reconnect_required) msg = T("connections.auth.gen.accessExpired", {provider: "Movie Paradise"});
      if (data.pending && !data.connected) msg = T("connections.auth.gen.waitingApproval", {provider: "Movie Paradise"});
      setConn(data.pending && !data.connected ? false : ok, msg);
      if (ok) { try { mpQcStop(); } catch (_) {} }
      if (showToast) note(ok ? T("connections.auth.gen.verified", {provider: "Movie Paradise"}) : T("connections.auth.gen.notConnected", {provider: "Movie Paradise"}));
    } catch {
      setConn(false, T("connections.auth.gen.verifyFailedShort", {provider: "Movie Paradise"}));
      if (showToast) note(T("connections.auth.gen.verifyFailedShort", {provider: "Movie Paradise"}));
    }
  }

  async function hydrate() {
    ensureMovieParadiseInstanceUI();
    await refresh(false);
  }

  function ensureMpPoller() {
    if (mpPoller) return mpPoller;
    mpPoller = Shared.createDevicePoll({
      url: () => mpApi("/api/movieparadise/device/poll"),
      method: "POST",
      body: "{}",
      maxTotalMs: 660000,
      onAuthorized: async () => {
        try { mpQcStop(); } catch (_) {}
        note(T("connections.auth.gen.connected", {provider: "Movie Paradise"}));
        try { window.invalidateConfigCache?.(); } catch {}
        try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
        await hydrate();
        emitConnected();
      },
      onExpired: () => mpQcTimeout(),
      onTimeout: () => mpQcTimeout(),
      onTerminal: (verdict) => {
        const label = friendlyError((verdict && verdict.message) || "");
        const st = el("movieparadise_qc_status"); if (st) st.textContent = label;
        setConn(false, label);
        if (mpQcTimer) { clearInterval(mpQcTimer); mpQcTimer = null; }
        mpQcDeadline = 0;
        mpQcShowRestart();
      },
      classify: (status, data) => {
        if (data && data.ok) return { state: "authorized" };
        const s = String((data && (data.status || data.error)) || "");
        if (!s || s === "authorization_pending") return { state: "pending" };
        if (s === "slow_down") return { state: "slow_down", intervalMs: Number(data && data.interval) > 0 ? Number(data.interval) * 1000 : 0 };
        if (s === "expired_token" || s === "no_device_code") return { state: "expired" };
        if (s === "network_error" || s === "internal") return { state: "network" };
        return { state: "terminal", message: s };
      },
    });
    return mpPoller;
  }

  function stopPoll() { if (mpPoller) mpPoller.stop(); }

  function startPoll(intervalSec) {
    ensureMpPoller().start({
      intervalMs: Math.max(5, Number(intervalSec || 5)) * 1000,
      deadlineMs: mpQcDeadline || 0,
    });
  }

  async function onDeviceStart() {
    if (mpStarting) return;
    mpStarting = true;
    try { mpQcStop(); } catch (_) {}
    const startBtn = el("movieparadise_device_start");
    if (startBtn) { startBtn.disabled = true; startBtn.classList.add("busy"); }

    const win = Shared.openWaitWindow("Movie Paradise", T("connections.auth.gen.requestingCode", {provider: "Movie Paradise"}), "MOVIEPARADISE");
    try {
      const r = await Shared.fetchJSON(mpApi("/api/movieparadise/device/start"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store",
      });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(friendlyError(data.error || "device_start_failed", data));

      const code = cleanCode(data.user_code);
      const url = txt(data.verification_uri) || VERIFY_URL;
      const deadline = (Number(data.expires_at || 0) * 1000) || (Date.now() + (Number(data.expires_in || 600) * 1000));

      const helpEl = el("movieparadise_qc_help");
      if (helpEl) helpEl.textContent = win
        ? T("connections.auth.openingVerify", {url: VERIFY_LABEL})
        : T("connections.auth.openVerify", {url: VERIFY_LABEL});

      mpQcShowCode(code, deadline);
      setConn(false, T("connections.auth.waitingForApproval"));
      startPoll(data.interval);

      if (!Shared.showWaitCode(win, { key: "MOVIEPARADISE", label: "Movie Paradise", code, url })) {
        note(T("connections.auth.popupVerify", {url: VERIFY_LABEL}));
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      try { mpQcStop(); } catch (_) {}
      const msg = e && e.message ? e.message : String(e);
      setConn(false, msg);
      note(T("connections.auth.deviceFailed", {provider: "Movie Paradise", error: msg}));
    } finally {
      mpStarting = false;
      if (startBtn) { startBtn.disabled = false; startBtn.classList.remove("busy"); }
    }
  }

  async function onCancel() {
    try { mpQcStop(); } catch (_) {}
    try { await Shared.fetchJSON(mpApi("/api/movieparadise/device/cancel"), { method: "POST" }); } catch (_) {}
    setConn(false, T("connections.auth.notConnected"));
  }

  async function onDisc() {
    try { mpQcStop(); } catch (_) {}
    try {
      const r = await Shared.fetchJSON(mpApi("/api/movieparadise/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      const codeEl = el("movieparadise_qc_code"); if (codeEl) codeEl.textContent = "----–----";
      setConn(false);
      note(T("connections.auth.gen.disconnected", {provider: "Movie Paradise"}));
      try { window.invalidateConfigCache?.(); } catch {}
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await hydrate();
    } catch {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Movie Paradise"}));
    }
  }

  function wire() {
    const start = el("movieparadise_device_start");
    if (start && !start.__wired) { start.addEventListener("click", onDeviceStart); start.__wired = true; }
    const copyCode = el("movieparadise_qc_copy");
    if (copyCode && !copyCode.__wired) { copyCode.addEventListener("click", (e) => { e.preventDefault(); mpQcCopy(copyCode); }); copyCode.__wired = true; }
    const cancel = el("movieparadise_device_cancel");
    if (cancel && !cancel.__wired) { cancel.addEventListener("click", onCancel); cancel.__wired = true; }
    const restart = el("movieparadise_device_restart");
    if (restart && !restart.__wired) { restart.addEventListener("click", () => onDeviceStart()); restart.__wired = true; }
    const d = el("movieparadise_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => { ensureMovieParadiseInstanceUI(); wire(); });
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    ensureMovieParadiseInstanceUI();
    wire();
    watch();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  window.addEventListener("settings-changed", () => {
    try { hydrate(); } catch {}
  });

  window.initMovieParadiseAuthUI = boot;
  boot();
})();
