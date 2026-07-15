// assets/auth/auth.mdblist.js
(function () {
  if (window._mdblPatched) return;
  window._mdblPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const MASK = "********";
  const profile = Shared.createProfileAdapter({
    provider: "mdblist",
    configKey: "mdblist",
    label: "MDBList",
    sectionId: "sec-mdblist",
    selectId: "mdblist_instance",
    storageKey: "cw.ui.mdblist.auth.instance.v1",
  });
  let mdblPoller = null;
  let methodOverride = "";

  function isMaskedSecret(v) {
    const value = txt(v);
    return !!value && (value === MASK || /^([*]|[•]){3,}$/.test(value));
  }

  function readSecretField(i) {
    const raw = txt(i && i.value);
    const masked = !!(i && (i.dataset.masked === "1" || isMaskedSecret(raw)));
    if (!raw && !masked) return { hasValue: false, masked: false, value: "" };
    if (masked) return { hasValue: true, masked: true, value: "" };
    return { hasValue: true, masked: false, value: raw };
  }

  function getMDBListInstance() {
    return profile ? profile.getInstance() : "default";
  }

  function setMDBListInstance(v) {
    if (profile) profile.setInstance(v);
  }

  function mdblApi(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  function friendlyError(code) {
    const key = String(code || "").trim();
    const map = {
      api_key_required: "Enter your MDBList API key",
      invalid_api_key: "Invalid MDBList API key",
      validation_timeout: "MDBList validation timed out",
      validation_failed: "Could not validate MDBList API key",
      validation_bad_response: "MDBList returned an invalid validation response",
      save_failed: "Saving MDBList key failed",
    };
    return map[key] || key.replace(/_/g, " ") || "Saving MDBList key failed";
  }

  async function getCfg() {
    return Shared.getConfig();
  }

  function getMDBListCfgBlock(cfg) {
    return profile ? profile.cfgBlock(cfg, true) : {};
  }

  function activeMethodFromBlock(blk) {
    if (blk?._pending_device && (txt(blk._pending_device.user_code) || txt(blk._pending_device.device_code))) return "device_code";
    if (txt(blk?.api_key) && !txt(blk?.access_token) && !txt(blk?.refresh_token)) return "api_key";
    if (txt(blk?.access_token) || txt(blk?.refresh_token)) return "device_code";
    const raw = txt(String(blk?.auth_method || "")).toLowerCase().replace("-", "_");
    if (raw === "api" || raw === "apikey" || raw === "api_key") return "api_key";
    if (raw === "device" || raw === "device_code" || raw === "oauth") return "device_code";
    return "device_code";
  }

  function activeMethodFromStatus(data) {
    if (data && data.pending) return "device_code";
    if (data && data.api_key_configured && !data.device_configured) return "api_key";
    if (data && data.device_configured) return "device_code";
    return data && data.auth_method === "api_key" ? "api_key" : "device_code";
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked(["mdblist_device_start", "mdblist_device_restart", "mdblist_save"], !!ok); } catch {}
    return Shared.setStatus("mdblist_msg", ok, msg);
  }

  function maskInput(i, has) {
    return Shared.maskSecret(i, has, { mask: MASK });
  }

  async function copyField(id, btn) {
    return Shared.copyField(id, btn, { emptyMessage: "Nothing to copy" });
  }

  const MDBL_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  const MDBL_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  let mdblQcDeadline = 0, mdblQcTimer = null, mdblQcCopyRevert = null;

  function mdblQcSetState(show) {
    const box = el("mdblist_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const start = el("mdblist_device_start"), cancel = el("mdblist_device_cancel"), restart = el("mdblist_device_restart");
    if (start) start.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.add("hidden");
  }
  function mdblQcShowRestart() {
    const restart = el("mdblist_device_restart"), start = el("mdblist_device_start"), cancel = el("mdblist_device_cancel");
    if (restart) restart.classList.remove("hidden");
    if (start) start.classList.add("hidden");
    if (cancel) cancel.classList.add("hidden");
  }
  function mdblQcUpdateTimer() {
    if (mdblQcDeadline && Date.now() > mdblQcDeadline) { mdblQcTimeout(); return; }
    const t = el("mdblist_qc_timer"); if (!t) return;
    const left = Math.max(0, Math.round((mdblQcDeadline - Date.now()) / 1000));
    const mm = Math.floor(left / 60), ss = String(left % 60).padStart(2, "0");
    t.textContent = left > 0 ? ("Expires in " + mm + ":" + ss) : "";
  }
  function mdblQcStop() {
    if (mdblQcTimer) { clearInterval(mdblQcTimer); mdblQcTimer = null; }
    stopPoll();
    mdblQcDeadline = 0;
    mdblQcSetState(false);
  }
  function mdblQcTimeout() {
    if (mdblQcTimer) { clearInterval(mdblQcTimer); mdblQcTimer = null; }
    stopPoll();
    mdblQcDeadline = 0;
    const st = el("mdblist_qc_status"); if (st) st.textContent = "Link code expired. Restart to try again.";
    const t = el("mdblist_qc_timer"); if (t) t.textContent = "";
    mdblQcShowRestart();
  }
  function mdblQcShowCode(code, secondsLeft) {
    const codeInput = el("mdblist_device_code"); if (codeInput) codeInput.value = code || "";
    const codeEl = el("mdblist_qc_code"); if (codeEl) codeEl.textContent = code || "------";
    const st = el("mdblist_qc_status"); if (st) st.textContent = "Waiting for approval…";
    mdblQcDeadline = Date.now() + (Math.max(30, Number(secondsLeft) || 300) * 1000);
    mdblQcSetState(true);
    mdblQcUpdateTimer();
    if (mdblQcTimer) clearInterval(mdblQcTimer);
    mdblQcTimer = setInterval(mdblQcUpdateTimer, 1000);
  }
  async function mdblQcCopy(btn) {
    const code = ((el("mdblist_qc_code") && el("mdblist_qc_code").textContent) || (el("mdblist_device_code") && el("mdblist_device_code").value) || "").replace(/\s+/g, "").trim();
    if (!code || code === "------") return;
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
    btn.classList.add("copied"); btn.innerHTML = MDBL_ICON_CHECK; btn.title = "Copied!";
    if (mdblQcCopyRevert) clearTimeout(mdblQcCopyRevert);
    mdblQcCopyRevert = setTimeout(function () { btn.classList.remove("copied"); btn.innerHTML = MDBL_ICON_COPY; btn.title = "Copy code"; }, 1400);
  }

  function setMethodUI(method) {
    const m = method === "api_key" ? "api_key" : "device_code";
    const hidden = el("mdblist_auth_method");
    if (hidden) hidden.value = m;
    document.querySelectorAll("#sec-mdblist .mdbl-method").forEach((b) => {
      const on = (b.dataset.method || "") === m;
      b.classList.toggle("active", on);
      b.setAttribute("aria-selected", on ? "true" : "false");
    });
    document.querySelectorAll("#sec-mdblist [data-method-actions]").forEach((node) => {
      node.classList.toggle("hidden", (node.dataset.methodActions || "") !== m);
    });
    const dev = el("mdblist_device_panel");
    const api = el("mdblist_api_panel");
    if (dev) dev.style.display = m === "device_code" ? "" : "none";
    if (api) api.style.display = m === "api_key" ? "" : "none";
  }

  function setApiHintVisible(visible) {
    const h = el("mdblist_hint");
    if (!h) return;
    h.classList.toggle("hidden", !visible);
    h.style.display = visible ? "" : "none";
  }

  async function refreshMDBListInstanceOptions(preserve) {
    if (profile) await profile.refreshOptions(preserve);
  }

  function ensureMDBListInstanceUI() {
    profile?.ensureUI(() => {
      methodOverride = "";
      try { mdblQcStop(); } catch (_) {}
      void hydrate();
    });
  }

  async function saveAuth(payload) {
    const r = await fetchJSON(mdblApi("/api/mdblist/save"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {})
    });
    if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError((r.data && r.data.error) || "save_failed"));
    return r.data || {};
  }

  async function refresh(showToast) {
    try {
      const r = await fetchJSON(mdblApi("/api/mdblist/status"), { cache: "no-store" });
      const data = r.data || {};
      const ok = !!(r.ok && data.connected);
      const statusMethod = activeMethodFromStatus(data);
      const method = methodOverride || statusMethod;
      setMethodUI(method);
      if (data.pending) {
        const code = el("mdblist_device_code");
        if (code && data.pending.user_code) code.value = txt(data.pending.user_code);
        if (data.pending.user_code && !(mdblPoller && mdblPoller.isRunning())) startPoll(Math.max(2, Number(data.pending.interval || 5)));
      }
      let msg = ok ? (statusMethod === "api_key" ? "Connected with API key" : "Connected with Device Code") : "Not connected";
      if (data.pending && !data.device_configured) msg = "Waiting for Device Code approval";
      if (ok && data.expires_at && statusMethod === "device_code") msg = "Connected with Device Code";
      setConn(data.pending && !data.device_configured ? false : ok, msg);
      if (ok) { try { mdblQcStop(); } catch (_) {} }
      if (showToast) note(ok ? "MDBList verified" : "MDBList not connected");
    } catch {
      setConn(false, "MDBList verify failed");
      if (showToast) note("MDBList verify failed");
    }
  }

  async function hydrate() {
    ensureMDBListInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    const blk = getMDBListCfgBlock(cfg);
    const method = methodOverride || activeMethodFromBlock(blk);
    setMethodUI(method);
    const hasApiKey = !!txt(blk?.api_key);
    maskInput(el("mdblist_key"), hasApiKey);
    setApiHintVisible(!hasApiKey);

    const pend = blk?._pending_device || {};
    if (pend.user_code) {
      const code = el("mdblist_device_code");
      if (code) code.value = txt(pend.user_code);
      startPoll(Math.max(2, Number(pend.interval || 5)));
    }
    await refresh(false);
  }

  async function onMethodChange() {
    const method = el("mdblist_auth_method")?.value === "api_key" ? "api_key" : "device_code";
    methodOverride = method === "device_code" ? "device_code" : "";
    if (method === "api_key") { try { mdblQcStop(); } catch (_) {} }
    setMethodUI(method);
    try {
      await saveAuth({ auth_method: method });
      await refresh(false);
    } catch {
      note("MDBList method switch failed");
    }
  }

  async function onSaveApiKey() {
    const i = el("mdblist_key");
    const keyState = readSecretField(i);
    if (!keyState.value) {
      if (keyState.masked || (i && i.dataset.hasKey === "1")) { await refresh(true); note("Key unchanged"); return; }
      note("Enter your MDBList API key"); return;
    }
    try {
      methodOverride = "";
      await saveAuth({ auth_method: "api_key", api_key: keyState.value });
      maskInput(i, true);
      setApiHintVisible(false);
      note("MDBList API key saved");
      await refresh(true);
    } catch (e) {
      const msg = e && e.message ? e.message : "Saving MDBList key failed";
      setConn(false, msg);
      note(msg);
    }
  }

  function ensureMdblPoller() {
    if (mdblPoller) return mdblPoller;
    mdblPoller = Shared.createDevicePoll({
      url: () => mdblApi("/api/mdblist/device/poll"),
      method: "POST",
      body: "{}",
      onAuthorized: async () => {
        methodOverride = "";
        try { mdblQcStop(); } catch (_) {}
        note("MDBList connected");
        await hydrate();
      },
      onExpired: () => mdblQcTimeout(),
      onTimeout: () => mdblQcTimeout(),
      onTerminal: (verdict) => {
        const label = String((verdict && verdict.message) || "").replace(/_/g, " ") || "Authorization failed";
        const st = el("mdblist_qc_status"); if (st) st.textContent = label;
        setConn(false, label);
        if (mdblQcTimer) { clearInterval(mdblQcTimer); mdblQcTimer = null; }
        mdblQcDeadline = 0;
        mdblQcShowRestart();
      },
      classify: (status, data) => {
        if (data && data.ok) return { state: "authorized" };
        const s = String((data && (data.status || data.error)) || "");
        if (!s || s === "authorization_pending") return { state: "pending" };
        if (s === "slow_down") return { state: "slow_down" };
        if (s === "expired_token" || s === "expired") return { state: "expired" };
        if (s === "network_error" || s === "internal") return { state: "network" };
        if (s === "http:429") return { state: "slow_down" };
        if (/^http:5\d\d$/.test(s)) return { state: "network" };
        return { state: "terminal", message: s };
      },
    });
    return mdblPoller;
  }

  function stopPoll() { if (mdblPoller) mdblPoller.stop(); }

  function startPoll(intervalSec) {
    ensureMdblPoller().start({
      intervalMs: Math.max(5, Number(intervalSec || 5)) * 1000,
      deadlineMs: mdblQcDeadline || 0,
    });
  }

  async function onDeviceStart() {
    try { mdblQcStop(); } catch (_) {}
    const startBtn = el("mdblist_device_start");
    if (startBtn) { startBtn.disabled = true; startBtn.classList.add("busy"); }

    let win = null;
    try { win = window.open("about:blank", "_blank"); } catch (_) {}
    try {
      methodOverride = "device_code";
      const r = await fetchJSON(mdblApi("/api/mdblist/device/start"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store"
      });
      const data = r.data || {};
      if (!r.ok || !data.ok) throw new Error(String(data.error || "device_start_failed"));

      const code = txt(data.user_code);
      const url = txt(data.verification_uri || data.verification_url) || "https://mdblist.com/oauth/device/";
      const secs = Number(data.expires_in || data.expiresIn || 0) || 300;

      const helpEl = el("mdblist_qc_help");
      if (helpEl) helpEl.textContent = win
        ? "Opening the MDBList approval page — enter this code there and approve CrossWatch."
        : "Open the MDBList approval page and enter this code to approve CrossWatch.";

      mdblQcShowCode(code, secs);
      setConn(false, "Waiting for approval");
      startPoll(Math.max(2, Number(data.interval || 5)));

      if (win && !win.closed) {
        try {
          win.document.write(
            '<!doctype html><meta charset="utf-8"><title>CrossWatch → MDBList</title>' +
            '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
            '<div><div style="font-size:14px;opacity:.7;margin-bottom:12px">Opening the MDBList approval page…</div>' +
            '<div style="font-size:36px;font-weight:700;letter-spacing:.22em;color:#8ff0c2">' + code + '</div>' +
            '<div style="font-size:12px;opacity:.6;margin-top:12px">Redirecting in a moment…</div></div></body>'
          );
        } catch (_) {}
        setTimeout(function () { try { if (win && !win.closed) win.location.href = url; } catch (_) {} }, 3000);
      } else {
        note("Popup blocked - open the MDBList page and enter the code.");
      }
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      try { mdblQcStop(); } catch (_) {}
      note("MDBList Device Code start failed: " + (e && e.message ? e.message : e));
    } finally {
      if (startBtn) { startBtn.disabled = false; startBtn.classList.remove("busy"); }
    }
  }

  async function onDisc() {
    try { mdblQcStop(); } catch (_) {}
    try {
      methodOverride = "";
      const r = await fetchJSON(mdblApi("/api/mdblist/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      maskInput(el("mdblist_key"), false);
      const code = el("mdblist_device_code"); if (code) code.value = "";
      const codeEl = el("mdblist_qc_code"); if (codeEl) codeEl.textContent = "------";
      setConn(false);
      note("MDBList disconnected");
      await hydrate();
    } catch {
      note("MDBList disconnect failed");
    }
  }

  function wire() {
    document.querySelectorAll("#sec-mdblist .mdbl-method").forEach((b) => {
      if (b.__wired) return;
      b.__wired = true;
      b.addEventListener("click", () => { setMethodUI(b.dataset.method); onMethodChange(); });
    });
    const s = el("mdblist_save");
    if (s && !s.__wired) { s.addEventListener("click", onSaveApiKey); s.__wired = true; }
    const start = el("mdblist_device_start");
    if (start && !start.__wired) { start.addEventListener("click", onDeviceStart); start.__wired = true; }
    const copyCode = el("mdblist_qc_copy");
    if (copyCode && !copyCode.__wired) { copyCode.addEventListener("click", (e) => { e.preventDefault(); mdblQcCopy(copyCode); }); copyCode.__wired = true; }
    const devCancel = el("mdblist_device_cancel");
    if (devCancel && !devCancel.__wired) { devCancel.addEventListener("click", () => mdblQcStop()); devCancel.__wired = true; }
    const devRestart = el("mdblist_device_restart");
    if (devRestart && !devRestart.__wired) { devRestart.addEventListener("click", () => onDeviceStart()); devRestart.__wired = true; }
    const d = el("mdblist_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
    const dd = el("mdblist_disconnect_device");
    if (dd && !dd.__wired) { dd.addEventListener("click", onDisc); dd.__wired = true; }
    const da = el("mdblist_disconnect_api");
    if (da && !da.__wired) { da.addEventListener("click", onDisc); da.__wired = true; }
    const k = el("mdblist_key");
    if (k && !k.__wiredSecret) {
      Shared.wireSecretInput(k);
      k.__wiredSecret = true;
    }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => { ensureMDBListInstanceUI(); wire(); });
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    ensureMDBListInstanceUI();
    wire();
    watch();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;

    const inst = getMDBListInstance();
    cfg.mdblist = cfg.mdblist || {};
    const target = inst === "default"
      ? cfg.mdblist
      : ((cfg.mdblist.instances = cfg.mdblist.instances || {}), (cfg.mdblist.instances[inst] = cfg.mdblist.instances[inst] || {}));

    const method = el("mdblist_auth_method")?.value === "api_key" ? "api_key" : "device_code";
    target.auth_method = method;
    const keyState = readSecretField(el("mdblist_key"));
    if (method === "api_key" && keyState.value) target.api_key = keyState.value;
  });

  window.initMDBListAuthUI = boot;
  boot();
})();
