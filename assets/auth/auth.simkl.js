// auth.simkl.js
(function (w, d) {
  const Shared = w.CW && w.CW.AuthShared;
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = Shared.notify;
  const bust = () => `?ts=${Date.now()}`;

  // Profiles
  const profile = Shared.createProfileAdapter({
    provider: "simkl",
    configKey: "simkl",
    label: "SIMKL",
    sectionId: "sec-simkl",
    selectId: "simkl_instance",
    storageKey: "cw.ui.simkl.auth.instance.v1",
    panelSelector: '#sec-simkl .cw-meta-provider-panel[data-provider="simkl"], .cw-meta-provider-panel[data-provider="simkl"]',
    title: "Select which SIMKL account this config applies to.",
  });
  const _str = Shared.txt;
  const _setVal = (id, v) => { const e = $(id); if (e && String(e.value || "") !== String(v || "")) e.value = String(v || ""); };
  let simklConnected = false;

  function getSimklInstance() { return profile ? profile.getInstance() : "default"; }
  function setSimklInstance(id) { if (profile) profile.setInstance(id); }
  function simklApi(url) { return profile ? profile.api(url) : String(url || ""); }
  async function refreshSimklInstanceOptions(preserve) { if (profile) await profile.refreshOptions(preserve); }
  function ensureSimklInstanceUI() {
    profile?.ensureUI(() => { try { smqcStop(); } catch (_) {} void hydrateSimklFromConfig(); });
  }

  let _simklPersistT = null;
  async function persistSimklClientFields() {
    try {
      const cid = _str($("simkl_client_id")?.value);
      const sec = _str($("simkl_client_secret")?.value);
      const cfg = await Shared.getConfig();
      if (!cfg) return;
      const block = profile ? profile.cfgBlock(cfg, true) : (cfg.simkl = cfg.simkl || {});
      block.client_id = cid;
      block.client_secret = sec;
      await fetch("/api/config", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(cfg) }).catch(() => {});
    } catch (_) {}
  }
  function schedulePersistSimkl() {
    if (_simklPersistT) clearTimeout(_simklPersistT);
    _simklPersistT = setTimeout(persistSimklClientFields, 350);
  }

  // Method selector
  function activeSimklMethod(blk) {
    const m = String((blk && blk.auth_method) || "").toLowerCase();
    if (m === "pin") return "pin";
    if (m === "oauth") return "oauth";
    if (blk && typeof blk._pending_pin === "object" && blk._pending_pin && blk._pending_pin.user_code) return "pin";
    if (_str(blk && blk.client_secret)) return "oauth";
    if (_str(blk && blk.access_token)) return "oauth"; // legacy connected config
    return "pin";
  }

  function setSimklMethodUI(method) {
    const m = method === "oauth" ? "oauth" : "pin";
    const hidden = $("simkl_auth_method"); if (hidden) hidden.value = m;
    d.querySelectorAll("#sec-simkl .smk-method").forEach((b) => {
      const on = (b.dataset.method || "") === m;
      b.classList.toggle("active", on);
      b.setAttribute("aria-selected", on ? "true" : "false");
    });
    d.querySelectorAll("#sec-simkl [data-method-actions]").forEach((el) => {
      el.classList.toggle("hidden", (el.dataset.methodActions || "") !== m);
    });
    const pin = $("simkl_pin_panel"), oauth = $("simkl_oauth_panel");
    if (pin) pin.style.display = m === "pin" ? "" : "none";
    if (oauth) oauth.style.display = m === "oauth" ? "" : "none";
    if (m === "oauth") { try { updateSimklButtonState(); } catch (_) {} }
  }

  function onSimklMethodChange(method) {
    if (method === "oauth") { try { smqcStop(); } catch (_) {} }
    setSimklMethodUI(method);
  }

  async function hydrateSimklFromConfig() {
    try {
      const cfg = await fetch("/api/config" + bust(), { cache: "no-store", credentials: "same-origin" }).then((r) => (r.ok ? r.json() : null)).catch(() => null);
      if (!cfg) return;
      const inst = getSimklInstance();
      const base = (cfg.simkl && typeof cfg.simkl === "object") ? cfg.simkl : {};
      const blk = profile ? profile.cfgBlock(cfg, false) : ((inst === "default") ? base : (base.instances && base.instances[inst]) || {});
      const isDefault = (inst === "default");
      _setVal("simkl_client_id", _str(blk.client_id || (isDefault ? base.client_id : "")));
      _setVal("simkl_client_secret", _str(blk.client_secret || (isDefault ? base.client_secret : "")));
      const tok = _str(blk.access_token || (isDefault ? (cfg?.auth?.simkl?.access_token || "") : ""));
      const method = activeSimklMethod(blk);
      setSimklMethodUI(method);
      try { setSimklSuccess(!!tok, tok ? (method === "pin" ? "Connected using PIN" : "Connected") : ""); } catch {}
      if (tok) { try { smqcStop(); } catch (_) {} }
      try { updateSimklButtonState(); } catch {}
    } catch (e) {
      console.warn("[simkl] hydrate failed", e);
    }
  }

  const computeRedirect = () =>
    (typeof w.computeRedirectURI === "function" ? w.computeRedirectURI() : (location.origin + "/callback"));

  function setSimklBanner(kind, text) { return Shared.setStatusPill("simkl_msg", kind, text); }

  function setSimklSuccess(on, text) {
    simklConnected = !!on;
    if (on) setSimklBanner("ok", text || "Connected");
    else setSimklBanner(null, "");
    try { Shared.setConnectLocked(["btn-connect-simkl-pin", "btn-simkl-pin-restart", "btn-connect-simkl"], simklConnected); } catch {}
    if (!simklConnected) { try { updateSimklButtonState(); } catch (_) {} }
  }

  function updateSimklButtonState() {
    try {
      const cid = ($("simkl_client_id")?.value || "").trim();
      const sec = ($("simkl_client_secret")?.value || "").trim();
      const btn = $("btn-connect-simkl") || $("simkl_start_btn") || $("btn_simkl_connect");
      const hint = $("simkl_hint");
      const rid = $("redirect_uri_preview");
      if (rid) rid.textContent = computeRedirect();
      const ok = cid.length > 0 && sec.length > 0;
      if (btn && !simklConnected) btn.disabled = !ok;
      try { Shared.setConnectLocked(["btn-connect-simkl-pin", "btn-simkl-pin-restart", "btn-connect-simkl"], simklConnected); } catch {}
      if (hint) hint.classList.toggle("hidden", ok);
    } catch (e) {
      console.warn("updateSimklButtonState failed", e);
    }
  }
  const updateSimklHint = updateSimklButtonState;

  async function copyRedirect() {
    return Shared.copyText(computeRedirect(), $("btn-copy-simkl-redirect"), { successMessage: "Redirect URI copied" });
  }

  async function simklDeleteToken() {
    try { smqcStop(); } catch (_) {}
    const btn = $("btn-delete-simkl") || $("btn-delete-simkl-oauth") || q("#sec-simkl .btn.danger");
    const msg = $("simkl_msg");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    if (msg) { msg.classList.remove("hidden"); msg.classList.remove("warn"); msg.textContent = ""; }
    try {
      const r = await fetch(simklApi("/api/simkl/token/delete"), {
        method: "POST", headers: { "Content-Type": "application/json" }, body: "{}", cache: "no-store",
      });
      const j = await r.json().catch(() => ({}));
      if (Shared.reportProviderUsage({ status: r.status, data: j })) return;
      if (r.ok && (j.ok !== false)) {
        try { setSimklSuccess(false); } catch {}
        try { const ce = $("simkl_qc_code"); if (ce) ce.textContent = "------"; } catch {}
        try { const ci = $("simkl_pin_code"); if (ci) ci.value = ""; } catch {}
        notify("SIMKL disconnected.");
      } else {
        if (msg) { msg.classList.add("warn"); msg.textContent = "Could not remove token."; }
      }
    } catch {
      if (msg) { msg.classList.add("warn"); msg.textContent = "Error removing token."; }
    } finally {
      if (btn) { btn.disabled = false; btn.classList.remove("busy"); }
    }
  }

  const SMK_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  const SMK_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  let smqcDeadline = 0, smqcTimer = null, smqcCopyRevert = null, simklPinPoller = null;

  function ensureSimklPinPoller() {
    if (simklPinPoller) return simklPinPoller;
    simklPinPoller = Shared.createDevicePoll({
      url: () => simklApi("/api/simkl/pin/poll"),
      method: "POST",
      body: "{}",
      onAuthorized: () => { smqcStop(); try { setSimklSuccess(true, "Connected using PIN"); } catch {} },
      onExpired: () => smqcTimeout(),
      onTimeout: () => smqcTimeout(),
      onTerminal: (verdict) => {
        const label = String((verdict && verdict.message) || "").replace(/_/g, " ") || "Authorization failed";
        const st = $("simkl_qc_status"); if (st) st.textContent = label;
        if (smqcTimer) { clearInterval(smqcTimer); smqcTimer = null; }
        smqcDeadline = 0;
        smqcShowRestart();
      },
      classify: (status, data) => {
        const s = String((data && data.status) || "");
        if (s === "authorized") return { state: "authorized" };
        if (s === "expired") return { state: "expired" };
        if (s === "slow_down") return { state: "slow_down" };
        if (s === "pending" || s === "authorization_pending") return { state: "pending" };
        if (s === "network_error") return { state: "network" };
        if (s === "http:429") return { state: "slow_down" };
        if (/^http:5\d\d$/.test(s)) return { state: "network" };
        if (s.startsWith("http:") || s === "bad_json") return { state: "pending" };
        if (data && data.ok === false && s) return { state: "terminal", message: s };
        return { state: "pending" };
      },
    });
    return simklPinPoller;
  }

  function stopSimklPinPoll() { if (simklPinPoller) simklPinPoller.stop(); }

  function smqcSetState(show) {
    const box = $("simkl_qc_state"); if (box) box.classList.toggle("hidden", !show);
    const start = $("btn-connect-simkl-pin"), cancel = $("btn-simkl-pin-cancel"), restart = $("btn-simkl-pin-restart");
    if (start) start.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.add("hidden");
  }
  function smqcShowRestart() {
    const restart = $("btn-simkl-pin-restart"), start = $("btn-connect-simkl-pin"), cancel = $("btn-simkl-pin-cancel");
    if (restart) restart.classList.remove("hidden");
    if (start) start.classList.add("hidden");
    if (cancel) cancel.classList.add("hidden");
  }
  function smqcUpdateTimer() {
    if (smqcDeadline && Date.now() > smqcDeadline) { smqcTimeout(); return; }
    const el = $("simkl_qc_timer"); if (!el) return;
    const left = Math.max(0, Math.round((smqcDeadline - Date.now()) / 1000));
    const mm = Math.floor(left / 60), ss = String(left % 60).padStart(2, "0");
    el.textContent = left > 0 ? ("Expires in " + mm + ":" + ss) : "";
  }
  function smqcStop() {
    if (smqcTimer) { clearInterval(smqcTimer); smqcTimer = null; }
    stopSimklPinPoll();
    smqcDeadline = 0;
    smqcSetState(false);
  }
  function smqcTimeout() {
    if (smqcTimer) { clearInterval(smqcTimer); smqcTimer = null; }
    stopSimklPinPoll();
    smqcDeadline = 0;
    const st = $("simkl_qc_status"); if (st) st.textContent = "Link code expired. Restart to try again.";
    const el = $("simkl_qc_timer"); if (el) el.textContent = "";
    smqcShowRestart();
  }
  function smqcShowCode(code, secondsLeft) {
    const codeInput = $("simkl_pin_code"); if (codeInput) codeInput.value = code || "";
    const codeEl = $("simkl_qc_code"); if (codeEl) codeEl.textContent = code || "------";
    const st = $("simkl_qc_status"); if (st) st.textContent = "Waiting for authorization…";
    smqcDeadline = Date.now() + (Math.max(30, Number(secondsLeft) || 900) * 1000);
    smqcSetState(true);
    smqcUpdateTimer();
    if (smqcTimer) clearInterval(smqcTimer);
    smqcTimer = setInterval(smqcUpdateTimer, 1000);
  }
  async function smqcCopy(btn) {
    const code = (($("simkl_qc_code") && $("simkl_qc_code").textContent) || ($("simkl_pin_code") && $("simkl_pin_code").value) || "").replace(/\s+/g, "").trim();
    if (!code || code === "------") return;
    let ok = false;
    try { if (navigator.clipboard && navigator.clipboard.writeText) { await navigator.clipboard.writeText(code); ok = true; } } catch (_) {}
    if (!ok) {
      try {
        const ta = d.createElement("textarea");
        ta.value = code; ta.style.position = "fixed"; ta.style.opacity = "0";
        d.body.appendChild(ta); ta.focus(); ta.select();
        ok = d.execCommand("copy");
        d.body.removeChild(ta);
      } catch (_) {}
    }
    if (!ok) { notify("Copy failed"); return; }
    btn.classList.add("copied"); btn.innerHTML = SMK_ICON_CHECK; btn.title = "Copied!";
    if (smqcCopyRevert) clearTimeout(smqcCopyRevert);
    smqcCopyRevert = setTimeout(function () { btn.classList.remove("copied"); btn.innerHTML = SMK_ICON_COPY; btn.title = "Copy code"; }, 1400);
  }

  async function startSimklPin() {
    try { smqcStop(); } catch (_) {}
    try { setSimklSuccess(false); } catch {}
    const btn = $("btn-connect-simkl-pin");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }

    let win = null;
    try { win = w.open("about:blank", "_blank"); } catch (_) {}

    let data = null;
    try {
      const r = await fetch(simklApi("/api/simkl/pin/start"), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}", cache: "no-store" });
      data = await r.json().catch(() => null);
      if (!r.ok || !data || data.ok === false) throw new Error((data && data.error) || "pin_start_failed");
    } catch (e) {
      try { if (win && !win.closed) win.close(); } catch (_) {}
      notify("Could not start SIMKL PIN: " + (e && e.message ? e.message : e));
      if (btn) { btn.disabled = false; btn.classList.remove("busy"); }
      return;
    }

    const code = _str(data.user_code);
    const url = _str(data.verification_url) || "https://simkl.com/pin";
    const secs = Number(data.expires_in || 0) || 900;

    const helpEl = $("simkl_qc_help");
    if (helpEl) helpEl.textContent = win
      ? "Opening simkl.com/pin — enter this code there and approve CrossWatch."
      : "Open simkl.com/pin and enter this code to approve CrossWatch.";

    smqcShowCode(code, secs);
    startSimklPinPoll(Math.max(2, Number(data.interval || 5)));

    if (win && !win.closed) {
      try {
        win.document.write(
          '<!doctype html><meta charset="utf-8"><title>CrossWatch → SIMKL</title>' +
          '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
          '<div><div style="font-size:14px;opacity:.7;margin-bottom:12px">Opening simkl.com/pin…</div>' +
          '<div style="font-size:36px;font-weight:700;letter-spacing:.22em;color:#8ff0c2">' + code + '</div>' +
          '<div style="font-size:12px;opacity:.6;margin-top:12px">Redirecting in a moment…</div></div></body>'
        );
      } catch (_) {}
      setTimeout(function () { try { if (win && !win.closed) win.location.href = url; } catch (_) {} }, 3000);
    } else {
      notify("Popup blocked - open simkl.com/pin and enter the code.");
    }

    if (btn) { btn.disabled = false; btn.classList.remove("busy"); }
  }

  function startSimklPinPoll(intervalSec) {
    ensureSimklPinPoller().start({
      intervalMs: Math.max(5, Number(intervalSec || 5)) * 1000,
      deadlineMs: smqcDeadline || 0,
    });
  }

  let simklPoll = null;
  let simklVisHandler = null;

  async function startSimkl() {
    try { setSimklSuccess(false); } catch {}

    const cid = ($("simkl_client_id")?.value || "").trim();
    const sec = ($("simkl_client_secret")?.value || "").trim();
    if (!cid || !sec) {
      notify("Fill in SIMKL Client ID + Client Secret first");
      updateSimklButtonState();
      return;
    }

    let win = null;
    try { win = w.open("https://simkl.com/", "_blank"); } catch {}

    try { await w.saveSettings?.(); } catch {}

    const origin = location.origin;
    const j = await fetch(simklApi("/api/simkl/authorize"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ origin }),
      cache: "no-store",
    }).then((r) => r.json()).catch(() => null);

    if (!j?.ok || !j.authorize_url) {
      const err = (j && (j.error || j.message)) ? String(j.error || j.message) : "SIMKL authorize failed";
      notify(err);
      try { if (win && !win.closed) win.close(); } catch {}
      return;
    }

    if (win && !win.closed) {
      try { win.location.href = j.authorize_url; win.focus(); } catch {}
    } else {
      notify("Popup blocked - allow popups and try again");
    }

    const cleanup = () => {
      if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }
      if (simklVisHandler) { d.removeEventListener("visibilitychange", simklVisHandler); simklVisHandler = null; }
    };
    cleanup();

    const startTs = Date.now();
    const deadline = startTs + 120000;
    const fastUntil = startTs + 30000;
    const back = [5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;
    let inFlight = false;

    const poll = async () => {
      if (Date.now() >= deadline) { cleanup(); return; }

      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { simklPoll = setTimeout(poll, 5000); return; }
      if (inFlight) return;

      inFlight = true;
      let cfg = null;
      try {
        const r = await fetch("/api/config" + bust(), { cache: "no-store", credentials: "same-origin" });
        if (r.status === 401) { notify("Session expired - please log in again"); cleanup(); return; }
        cfg = await r.json();
      } catch {} finally { inFlight = false; }

      const inst = getSimklInstance();
      const base = (cfg?.simkl && typeof cfg.simkl === "object") ? cfg.simkl : {};
      const blk = (inst === "default") ? base : (base.instances && base.instances[inst]) || {};
      const tok = _str(blk.access_token || (inst === "default" ? (cfg?.auth?.simkl?.access_token || "") : ""));
      if (tok) {
        try { setSimklSuccess(true); } catch {}
        cleanup();
        return;
      }

      const delay = (Date.now() < fastUntil) ? 2000 : back[Math.min(i++, back.length - 1)];
      simklPoll = setTimeout(poll, delay);
    };

    simklVisHandler = () => {
      if (d.hidden) return;
      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (!settingsVisible) return;
      if (!simklPoll) return;
      clearTimeout(simklPoll);
      simklPoll = null;
      void poll();
    };
    d.addEventListener("visibilitychange", simklVisHandler);

    simklPoll = setTimeout(poll, 2000);
  }

  let __simklInitDone = false;
  function initSimklAuthUI() {
    try { ensureSimklInstanceUI(); } catch (_) {}
    try { refreshSimklInstanceOptions(true); } catch (_) {}
    try { hydrateSimklFromConfig(); } catch (_) {}

    d.querySelectorAll("#sec-simkl .smk-method").forEach((b) => {
      if (b.__simklBound) return;
      b.__simklBound = true;
      b.addEventListener("click", () => onSimklMethodChange(b.dataset.method));
    });

    const cid = $("simkl_client_id");
    if (cid && !cid.__simklBound) { cid.addEventListener("input", updateSimklButtonState); cid.__simklBound = true; }
    const sec = $("simkl_client_secret");
    if (sec && !sec.__simklBound) { sec.addEventListener("input", updateSimklButtonState); sec.__simklBound = true; }
    const copy = $("btn-copy-simkl-redirect");
    if (copy && !copy.__simklBound) { copy.addEventListener("click", copyRedirect); copy.__simklBound = true; }

    const connect = $("btn-connect-simkl");
    if (connect && !connect.__simklBound) { connect.addEventListener("click", startSimkl); connect.__simklBound = true; }
    const connectPin = $("btn-connect-simkl-pin");
    if (connectPin && !connectPin.__simklBound) { connectPin.addEventListener("click", startSimklPin); connectPin.__simklBound = true; }
    const pinCancel = $("btn-simkl-pin-cancel");
    if (pinCancel && !pinCancel.__simklBound) { pinCancel.addEventListener("click", () => smqcStop()); pinCancel.__simklBound = true; }
    const pinRestart = $("btn-simkl-pin-restart");
    if (pinRestart && !pinRestart.__simklBound) { pinRestart.addEventListener("click", () => startSimklPin()); pinRestart.__simklBound = true; }
    const pinCopy = $("simkl_qc_copy");
    if (pinCopy && !pinCopy.__simklBound) { pinCopy.addEventListener("click", (e) => { e.preventDefault(); smqcCopy(pinCopy); }); pinCopy.__simklBound = true; }

    const del = $("btn-delete-simkl");
    if (del && !del.__simklBound) { del.addEventListener("click", simklDeleteToken); del.__simklBound = true; }
    const delO = $("btn-delete-simkl-oauth");
    if (delO && !delO.__simklBound) { delO.addEventListener("click", simklDeleteToken); delO.__simklBound = true; }

    const rid = $("redirect_uri_preview");
    if (rid) rid.textContent = computeRedirect();

    updateSimklButtonState();
    __simklInitDone = true;
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", initSimklAuthUI, { once: true });
  else initSimklAuthUI();

  w.cwAuth = w.cwAuth || {};
  w.cwAuth.simkl = w.cwAuth.simkl || {};
  w.cwAuth.simkl.init = initSimklAuthUI;

  Object.assign(w, {
    setSimklSuccess,
    updateSimklButtonState,
    updateSimklHint,
    startSimkl,
    startSimklPin,
    copyRedirect,
    simklDeleteToken,
  });
})(window, document);
