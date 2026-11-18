// assets/auth/auth.movieparadise.js
// CrossWatch - Movie Paradise device code authentication UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
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
      if (!Number.isFinite(seconds) || seconds <= 0) return "Movie Paradise is busy - try again later";
      return "Movie Paradise is busy - try again in " + seconds + "s";
    }
    const map = {
      missing_client_id: "No Movie Paradise app id configured",
      invalid_client: "Movie Paradise rejected the CrossWatch app id",
      unauthorized_client: "The CrossWatch Movie Paradise app is inactive",
      invalid_scope: "The CrossWatch Movie Paradise app is missing a required scope",
      invalid_grant: "Movie Paradise access expired - connect again",
      access_denied: "Access was declined in Movie Paradise",
      answer_lost: "Movie Paradise did not answer in time - restart to get a new code",
      reconnect_required: "Movie Paradise access expired - connect again",
      expired_token: "Link code expired",
      network_error: "Could not reach Movie Paradise",
      device_start_failed: "Could not start Movie Paradise device login",
    };
    return map[key] || key.replace(/_/g, " ") || "Movie Paradise request failed";
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
    t.textContent = left > 0 ? ("Expires in " + mm + ":" + ss) : "";
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
    const st = el("movieparadise_qc_status"); if (st) st.textContent = "Link code expired. Restart to try again.";
    const t = el("movieparadise_qc_timer"); if (t) t.textContent = "";
    mpQcShowRestart();
  }

  function mpQcShowCode(code, deadlineMs) {
    const codeEl = el("movieparadise_qc_code"); if (codeEl) codeEl.textContent = code || "----–----";
    const st = el("movieparadise_qc_status"); if (st) st.textContent = "Waiting for approval…";
    mpQcDeadline = Number(deadlineMs) || (Date.now() + 600000);
    mpQcSetState(true);
    mpQcUpdateTimer();
    if (mpQcTimer) clearInterval(mpQcTimer);
    mpQcTimer = setInterval(mpQcUpdateTimer, 1000);
  }

  async function mpQcCopy(btn) {
    const code = cleanCode(el("movieparadise_qc_code") && el("movieparadise_qc_code").textContent);
    if (!code || /^-+$/.test(code)) return;
    const ok = await Shared.copyText(code, null, { failureMessage: "Copy failed" });
    if (!ok || !btn) return;
    btn.classList.add("copied"); btn.innerHTML = MP_ICON_CHECK; btn.title = "Copied!";
    if (mpQcCopyRevert) clearTimeout(mpQcCopyRevert);
    mpQcCopyRevert = setTimeout(function () { btn.classList.remove("copied"); btn.innerHTML = MP_ICON_COPY; btn.title = "Copy code"; }, 1400);
  }

  function waitingScreen(win, code, url) {
    const logo = location.origin + "/assets/img/MOVIEPARADISE.png";
    const safeUrl = JSON.stringify(String(url));
    const safeCode = JSON.stringify(code);
    win.document.write(
      '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>CrossWatch - Movie Paradise</title>' +
      '<body style="margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#09090b;color:#f4f4f5;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
      '<div style="padding:24px;max-width:420px">' +
      '<img src="' + logo + '" alt="" style="width:64px;height:auto;margin-bottom:18px">' +
      '<div style="font-size:14px;opacity:.75;margin-bottom:14px">Enter this code on the Movie Paradise approval page</div>' +
      '<div style="font-size:38px;font-weight:800;letter-spacing:.18em;padding-left:.18em;background:linear-gradient(135deg,#f08fb8,#b56fd0);-webkit-background-clip:text;background-clip:text;color:transparent">' + code + '</div>' +
      '<div id="mp-copy-state" style="font-size:12px;opacity:.65;margin-top:10px;min-height:16px"></div>' +
      '<button id="mp-go" type="button" style="margin-top:18px;cursor:pointer;border:0;border-radius:10px;padding:11px 18px;font:600 14px system-ui,sans-serif;color:#fff;background:linear-gradient(135deg,#d7628f,#8b3fa1);box-shadow:0 0 16px rgba(139,63,161,.4)">Copy code and continue</button>' +
      '<div style="font-size:12px;opacity:.55;margin-top:14px">Continuing to Movie Paradise in <span id="mp-left">6</span>s</div>' +
      '</div>' +
      '<script>(function(){var code=' + safeCode + ',url=' + safeUrl + ',left=6,done=false;' +
      'function state(t){var s=document.getElementById("mp-copy-state");if(s)s.textContent=t}' +
      'function legacy(){try{var t=document.createElement("textarea");t.value=code;t.setAttribute("readonly","");t.style.position="fixed";t.style.top="-9999px";document.body.appendChild(t);t.select();var ok=document.execCommand("copy");t.remove();return ok}catch(e){return false}}' +
      'function go(){if(done)return;done=true;location.href=url}' +
      'try{if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(code).then(function(){state("Code copied - paste it on the next page")},function(){})}}catch(e){}' +
      'document.getElementById("mp-go").addEventListener("click",function(){if(legacy())state("Code copied");go()});' +
      'var iv=setInterval(function(){left-=1;var n=document.getElementById("mp-left");if(n)n.textContent=String(Math.max(0,left));if(left<=0){clearInterval(iv);go()}},1000);' +
      '})();<\/script></body>'
    );
    win.document.close();
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
      let msg = ok ? (data.username ? "Connected as " + data.username : "Connected") : "Not connected";
      if (data.reconnect_required) msg = "Movie Paradise access expired - connect again";
      if (data.pending && !data.connected) msg = "Waiting for Movie Paradise approval";
      setConn(data.pending && !data.connected ? false : ok, msg);
      if (ok) { try { mpQcStop(); } catch (_) {} }
      if (showToast) note(ok ? "Movie Paradise verified" : "Movie Paradise not connected");
    } catch {
      setConn(false, "Movie Paradise verify failed");
      if (showToast) note("Movie Paradise verify failed");
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
        note("Movie Paradise connected");
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

    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch (_) {}
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
        ? "Opening " + VERIFY_LABEL + " - enter this code there and approve CrossWatch."
        : "Open " + VERIFY_LABEL + " and enter this code to approve CrossWatch.";

      mpQcShowCode(code, deadline);
      setConn(false, "Waiting for approval");
      startPoll(data.interval);

      if (win && !win.closed) {
        try { waitingScreen(win, code, url); }
        catch (_) { try { win.location.href = url; } catch (_) {} }
      } else {
        note("Popup blocked - open " + VERIFY_LABEL + " and enter the code.");
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      try { mpQcStop(); } catch (_) {}
      const msg = e && e.message ? e.message : String(e);
      setConn(false, msg);
      note("Movie Paradise device login failed: " + msg);
    } finally {
      mpStarting = false;
      if (startBtn) { startBtn.disabled = false; startBtn.classList.remove("busy"); }
    }
  }

  async function onCancel() {
    try { mpQcStop(); } catch (_) {}
    try { await Shared.fetchJSON(mpApi("/api/movieparadise/device/cancel"), { method: "POST" }); } catch (_) {}
    setConn(false, "Not connected");
  }

  async function onDisc() {
    try { mpQcStop(); } catch (_) {}
    try {
      const r = await Shared.fetchJSON(mpApi("/api/movieparadise/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      const codeEl = el("movieparadise_qc_code"); if (codeEl) codeEl.textContent = "----–----";
      setConn(false);
      note("Movie Paradise disconnected");
      try { window.invalidateConfigCache?.(); } catch {}
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await hydrate();
    } catch {
      note("Movie Paradise disconnect failed");
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
