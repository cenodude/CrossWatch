// assets/auth/auth.punchplay.js
(function () {
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

  const PP_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  const PP_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  let ppQcDeadline = 0, ppQcTimer = null, ppQcCopyRevert = null;

  function ppApi(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  function friendlyError(code) {
    const key = String(code || "").trim();
    const map = {
      missing_client_id: "No PunchPlay app id configured",
      rate_limited: "PunchPlay is rate limiting device codes - wait a few minutes",
      invalid_client: "PunchPlay rejected the CrossWatch app id",
      unauthorized_client: "The CrossWatch PunchPlay app is inactive",
      invalid_scope: "The CrossWatch PunchPlay app is missing a required scope",
      invalid_grant: "PunchPlay access expired - connect again",
      expired: "Link code expired",
      network_error: "Could not reach PunchPlay",
      device_start_failed: "Could not start PunchPlay device login",
    };
    return map[key] || key.replace(/_/g, " ") || "PunchPlay request failed";
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
    t.textContent = left > 0 ? ("Expires in " + mm + ":" + ss) : "";
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
    const st = el("punchplay_qc_status"); if (st) st.textContent = "Link code expired. Restart to try again.";
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
    const st = el("punchplay_qc_status"); if (st) st.textContent = "Waiting for approval…";
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
    try { if (navigator.clipboard && navigator.clipboard.writeText) { await navigator.clipboard.writeText(code); ok = true; } } catch (_) {}
    if (!ok) {
      try {
        const ta = document.createElement("textarea");
        ta.value = code; ta.style.position = "fixed"; ta.style.opacity = "0";
        document.body.appendChild(ta); ta.focus(); ta.select();
        ok = document.execCommand("copy");
        document.body.removeChild(ta);
      } catch (_) {}
    }
    if (!ok) { note("Copy failed"); return; }
    btn.classList.add("copied"); btn.innerHTML = PP_ICON_CHECK; btn.title = "Copied!";
    if (ppQcCopyRevert) clearTimeout(ppQcCopyRevert);
    ppQcCopyRevert = setTimeout(function () { btn.classList.remove("copied"); btn.innerHTML = PP_ICON_COPY; btn.title = "Copy code"; }, 1400);
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
      let msg = ok ? (data.username ? "Connected as " + data.username : "Connected") : "Not connected";
      if (data.pending && !data.connected) msg = "Waiting for PunchPlay approval";
      setConn(data.pending && !data.connected ? false : ok, msg);
      if (ok) { try { ppQcStop(); } catch (_) {} }
      if (showToast) note(ok ? "PunchPlay verified" : "PunchPlay not connected");
    } catch {
      setConn(false, "PunchPlay verify failed");
      if (showToast) note("PunchPlay verify failed");
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
        note("PunchPlay connected");
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
    try { ppQcStop(); } catch (_) {}
    const startBtn = el("punchplay_device_start");
    if (startBtn) { startBtn.disabled = true; startBtn.classList.add("busy"); }

    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch (_) {}
    try {
      const r = await fetchJSON(ppApi("/api/punchplay/device/start"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store"
      });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(friendlyError(data.error || "device_start_failed"));

      const code = txt(data.user_code);
      const url = txt(data.verification_uri_complete) || txt(data.verification_uri) || VERIFY_URL;
      const secs = Number(data.expires_in || 0) || 600;

      const helpEl = el("punchplay_qc_help");
      if (helpEl) helpEl.textContent = win
        ? "Opening punchplay.tv/link — enter this code there and approve CrossWatch."
        : "Open punchplay.tv/link and enter this code to approve CrossWatch.";

      ppQcShowCode(code, secs);
      setQr(txt(data.verification_uri_qr));
      setConn(false, "Waiting for approval");
      startPoll(Math.max(5, Number(data.interval || 5)));

      if (win && !win.closed) {
        try {
          win.document.write(
            '<!doctype html><meta charset="utf-8"><title>CrossWatch → PunchPlay</title>' +
            '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
            '<div><div style="font-size:14px;opacity:.7;margin-bottom:12px">Opening the PunchPlay approval page…</div>' +
            '<div style="font-size:36px;font-weight:700;letter-spacing:.18em;color:#ff9d6a">' + code + '</div>' +
            '<div style="font-size:12px;opacity:.6;margin-top:12px">Redirecting in a moment…</div></div></body>'
          );
        } catch (_) {}
        setTimeout(function () { try { if (win && !win.closed) win.location.href = url; } catch (_) {} }, 3000);
      } else {
        note("Popup blocked - open punchplay.tv/link and enter the code.");
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      try { ppQcStop(); } catch (_) {}
      const msg = e && e.message ? e.message : String(e);
      setConn(false, msg);
      note("PunchPlay device login failed: " + msg);
    } finally {
      if (startBtn) { startBtn.disabled = false; startBtn.classList.remove("busy"); }
    }
  }

  async function onCancel() {
    try { ppQcStop(); } catch (_) {}
    try { await fetchJSON(ppApi("/api/punchplay/device/cancel"), { method: "POST" }); } catch (_) {}
    setConn(false, "Not connected");
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
      note("PunchPlay disconnected");
      await hydrate();
    } catch {
      note("PunchPlay disconnect failed");
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
