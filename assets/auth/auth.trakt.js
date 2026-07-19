// auth.trakt.js
(function () {
  if (window._traktPatched) return;
  window._traktPatched = true;

  // Utils
  const Shared = window.CW.AuthShared;
  const traktProfile = Shared.createProfileAdapter({
    provider: "trakt",
    configKey: "trakt",
    label: "Trakt",
    sectionId: "sec-trakt",
    selectId: "trakt_instance",
    storageKey: "cw.ui.trakt.auth.instance.v1",
    panelSelector: '#sec-trakt .cw-meta-provider-panel[data-provider="trakt"], .cw-meta-provider-panel[data-provider="trakt"]',
    title: "Select which Trakt account this config applies to.",
  });
  function _notify(msg) { Shared.notify(msg); }
  function _el(id) { return Shared.el(id); }
  function _setVal(id, v) { var el = _el(id); if (el) el.value = v == null ? "" : String(v); }
  function _str(x) { return Shared.txt(x); }
  function _isMaskedSecret(v) {
    return Shared.isMaskedSecret(v);
  }
  function _markSecretField(el, value) {
    return Shared.markSecretField(el, value);
  }
  function _wireSecretField(el, onChange) {
    return Shared.wireSecretInput(el, { onInput: onChange });
  }
  function _readSecretField(el) {
    return Shared.readSecretField(el);
  }

  var traktConnected = false;
  var traktPoller = null;

  function getTraktInstance() {
    return traktProfile ? traktProfile.getInstance() : "default";
  }

  function setTraktInstance(v) {
    if (traktProfile) traktProfile.setInstance(v);
  }

  function traktApi(path) {
    return traktProfile ? traktProfile.api(path) : String(path || "");
  }

  function getTraktCfgBlock(cfg) {
    return traktProfile ? traktProfile.cfgBlock(cfg, true) : {};
  }

  async function refreshTraktInstanceOptions(preserve) {
    if (traktProfile) await traktProfile.refreshOptions(preserve);
  }

  function ensureTraktInstanceUI() {
    traktProfile?.ensureUI(() => {
      void hydrateAuthFromConfig();
      try { startTraktTokenPoll(); } catch (_) {}
    });
  }

  async function persistTraktClientFields() {
    try {
      var cidState = _readSecretField(_el('trakt_client_id'));
      var secState = _readSecretField(_el('trakt_client_secret'));
      var cfg = await fetchConfig();
      if (!cfg) return;
      var t = getTraktCfgBlock(cfg);
      if (cidState.value) t.client_id = cidState.value;
      else if (!cidState.masked) try { delete t.client_id; } catch (_) {}
      if (secState.value) t.client_secret = secState.value;
      else if (!secState.masked) try { delete t.client_secret; } catch (_) {}
      await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
    } catch (_) {}
  }

  function updateTraktBanner() {
    try {
      var msg = _el('trakt_msg');
      if (!msg) return;
      try { Shared.setConnectLocked(["btn-connect-trakt", "btn-trakt-restart"], !!traktConnected); } catch (_) {}
      if (traktConnected) return Shared.setStatusPill(msg, "ok", "Connected");
      return Shared.setStatusPill(msg, null);
    } catch (_) {}
  }
  // status banner
  function setTraktSuccess(show) {
    try {
      traktConnected = !!show;
      if (show) { try { trqcStop(); } catch (_) {} updateTraktBanner(); }
      else {
        try { Shared.setConnectLocked(["btn-connect-trakt", "btn-trakt-restart"], false); } catch (_) {}
        var el = _el('trakt_msg'); if (el) { el.classList.add('hidden'); el.textContent = ''; el.classList.remove('ok','warn'); }
      }
    } catch (_) {}
  }

  async function fetchConfig() {
    try {
      var r = await fetch("/api/config", { cache: "no-store" });
      if (!r.ok) return null;
      var cfg = await r.json();
      return cfg || {};
    } catch (_) {
      return null;
    }
  }

  // Hydrate
  async function hydrateAuthFromConfig() {
    try {
      var cfg = await fetchConfig(); if (!cfg) return;
      var t = getTraktCfgBlock(cfg);
      var a = (cfg.auth && cfg.auth.trakt) || {};
            var isDefault = (getTraktInstance() === "default");
      _markSecretField(_el("trakt_client_id"),     _str(t.client_id || (isDefault ? (cfg.trakt && cfg.trakt.client_id) : "")));
      _markSecretField(_el("trakt_client_secret"), _str(t.client_secret || (isDefault ? (cfg.trakt && cfg.trakt.client_secret) : "")));
      traktConnected = !!_str(t.access_token || (getTraktInstance() === 'default' ? a.access_token : ''));
      _setVal("trakt_pin",           _str((t._pending_device && t._pending_device.user_code) || ''));
      if (traktConnected) { try { trqcStop(); } catch (_) {} }
      updateTraktHint();
      updateTraktBanner();
    } catch (e) {
      console.warn("[trakt] hydrateAuthFromConfig failed", e);
    }
  }

  async function hydrateAllSecretsRaw() {
    try { await hydrateAuthFromConfig(); } catch (_) {}
  }

  // Hint
  function updateTraktHint() {
    try {
      var cidState = _readSecretField(_el("trakt_client_id"));
      var secState = _readSecretField(_el("trakt_client_secret"));
      var hint = _el("trakt_hint");
      if (!hint) return;
      var show = !(cidState.hasValue && secState.hasValue);
      hint.classList.toggle("hidden", !show);
      hint.style.display = show ? "" : "none";
    } catch (_) {}
  }

  // Copy helpers
  async function _copyText(text, btn) {
    return Shared.copyText(text, btn, { failureMessage: false });
  }

  window.copyTraktRedirect = async function () {
    var code = document.getElementById("trakt_redirect_uri_preview");
    var text = (code && code.textContent ? code.textContent : "urn:ietf:wg:oauth:2.0:oob").trim();
    await _copyText(text);
  };

  var TRK_ICON_COPY = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
  var TRK_ICON_CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  var trqcDeadline = 0, trqcTimer = null, trqcCopyRevert = null;

  function trqcSetState(show) {
    var box = _el("trakt_qc_state"); if (box) box.classList.toggle("hidden", !show);
    var connect = _el("btn-connect-trakt"), cancel = _el("btn-trakt-cancel"), restart = _el("btn-trakt-restart");
    if (connect) connect.classList.toggle("hidden", show);
    if (cancel) cancel.classList.toggle("hidden", !show);
    if (restart) restart.classList.add("hidden");
  }
  function trqcShowRestart() {
    var restart = _el("btn-trakt-restart"), connect = _el("btn-connect-trakt"), cancel = _el("btn-trakt-cancel");
    if (restart) restart.classList.remove("hidden");
    if (connect) connect.classList.add("hidden");
    if (cancel) cancel.classList.add("hidden");
  }
  function trqcUpdateTimer() {
    if (trqcDeadline && Date.now() > trqcDeadline) { trqcTimeout(); return; }
    var el = _el("trakt_qc_timer"); if (!el) return;
    var left = Math.max(0, Math.round((trqcDeadline - Date.now()) / 1000));
    var mm = Math.floor(left / 60), ss = String(left % 60).padStart(2, "0");
    el.textContent = left > 0 ? ("Expires in " + mm + ":" + ss) : "";
  }
  function trqcStop() {
    if (trqcTimer) { clearInterval(trqcTimer); trqcTimer = null; }
    if (traktPoller) traktPoller.stop();
    trqcDeadline = 0;
    trqcSetState(false);
  }
  function trqcTimeout() {
    if (trqcTimer) { clearInterval(trqcTimer); trqcTimer = null; }
    trqcDeadline = 0;
    var st = _el("trakt_qc_status"); if (st) st.textContent = "Link code expired. Restart to try again.";
    var el = _el("trakt_qc_timer"); if (el) el.textContent = "";
    trqcShowRestart();
  }
  function trqcShowCode(code, secondsLeft) {
    var pinEl = _el("trakt_pin"); if (pinEl) pinEl.value = code || "";
    var codeEl = _el("trakt_qc_code"); if (codeEl) codeEl.textContent = code || "------";
    var st = _el("trakt_qc_status"); if (st) st.textContent = "Waiting for authorization…";
    trqcDeadline = Date.now() + (Math.max(30, Number(secondsLeft) || 300) * 1000);
    trqcSetState(true);
    trqcUpdateTimer();
    if (trqcTimer) clearInterval(trqcTimer);
    trqcTimer = setInterval(trqcUpdateTimer, 1000);
  }
  async function trqcCopy(btn) {
    var code = ((_el("trakt_qc_code") && _el("trakt_qc_code").textContent) || (_el("trakt_pin") && _el("trakt_pin").value) || "").replace(/\s+/g, "").trim();
    if (!code || code === "------") return;
    var ok = false;
    try { if (navigator.clipboard && navigator.clipboard.writeText) { await navigator.clipboard.writeText(code); ok = true; } } catch (_) {}
    if (!ok) {
      try {
        var ta = document.createElement("textarea");
        ta.value = code; ta.style.position = "fixed"; ta.style.opacity = "0";
        document.body.appendChild(ta); ta.focus(); ta.select();
        ok = document.execCommand("copy");
        document.body.removeChild(ta);
      } catch (_) {}
    }
    if (!ok) { _notify("Copy failed"); return; }
    btn.classList.add("copied"); btn.innerHTML = TRK_ICON_CHECK; btn.title = "Copied!";
    if (trqcCopyRevert) clearTimeout(trqcCopyRevert);
    trqcCopyRevert = setTimeout(function () { btn.classList.remove("copied"); btn.innerHTML = TRK_ICON_COPY; btn.title = "Copy code"; }, 1400);
  }
  function wireTraktQc() {
    var copy = _el("trakt_qc_copy");
    if (copy && !copy.__wired) { copy.__wired = true; copy.addEventListener("click", function (e) { e.preventDefault(); trqcCopy(copy); }); }
    var cancel = _el("btn-trakt-cancel");
    if (cancel && !cancel.__wired) { cancel.__wired = true; cancel.addEventListener("click", function () { trqcStop(); }); }
    var restart = _el("btn-trakt-restart");
    if (restart && !restart.__wired) { restart.__wired = true; restart.addEventListener("click", function () { requestTraktPin(); }); }
  }

  var __traktInitDone = false;
  function initTraktAuthUI() {
    wireTraktQc();

    try {
      ensureTraktInstanceUI();
      var idEl  = _el("trakt_client_id");
      var secEl = _el("trakt_client_secret");
      _wireSecretField(idEl, updateTraktHint);
      _wireSecretField(secEl, updateTraktHint);

      var copyRedirect = _el("btn-copy-trakt-redirect");
      if (copyRedirect && !copyRedirect.__wired) { copyRedirect.addEventListener("click", () => window.copyTraktRedirect()); copyRedirect.__wired = true; }
      var connectBtn = _el("btn-connect-trakt");
      if (connectBtn && !connectBtn.__wired) { connectBtn.addEventListener("click", requestTraktPin); connectBtn.__wired = true; }
      var deleteBtn = _el("btn-delete-trakt");
      if (deleteBtn && !deleteBtn.__wired) { deleteBtn.addEventListener("click", traktDeleteToken); deleteBtn.__wired = true; }

      updateTraktHint();
      updateTraktBanner();
      hydrateAllSecretsRaw();
      startTraktTokenPoll();
    } catch (e) {
      console.warn("[trakt] init failed", e);
    }

    if (__traktInitDone) return;
    __traktInitDone = true;

    try {
      if (!window.__traktBannerTick) {
        window.__traktBannerTick = setInterval(function(){ try { updateTraktBanner(); } catch (_) {} }, 800);
      }
    } catch (_) {}
  }

  // Flush Trakt credentials
  async function flushTraktCreds() {
    try {
      await traktDeleteToken();
      _notify('Trakt cleared for this profile');
    } catch (e) {
      console.warn("flushTraktCreds failed", e);
      _notify("Wissen mislukt");
    }
  }

  function traktTokenFromCfg(cfg) {
    var tok = _str(cfg && ((cfg.trakt && cfg.trakt.access_token) || (cfg.auth && cfg.auth.trakt && cfg.auth.trakt.access_token)));
    try {
      var t = cfg ? getTraktCfgBlock(cfg) : null;
      tok = _str(t && t.access_token) || (getTraktInstance() === 'default' ? tok : '');
    } catch (_) {}
    return tok;
  }

  function ensureTraktPoller() {
    if (traktPoller) return traktPoller;
    traktPoller = Shared.createDevicePoll({
      url: function () { return "/api/config?ts=" + Date.now(); },
      method: "GET",
      minIntervalMs: 2000,
      onAuthorized: function () { setTraktSuccess(true); },
      shouldPause: function () {
        if (document.hidden) return true;
        var page = _el("page-settings");
        return !(page && !page.classList.contains("hidden"));
      },
      classify: function (status, data) {
        return traktTokenFromCfg(data) ? { state: "authorized" } : { state: "pending" };
      },
    });
    return traktPoller;
  }

  function startTraktTokenPoll(deadlineMs) {
    var pinNow = _str(_el("trakt_pin")?.value);
    if (traktConnected || !pinNow) {
      if (traktPoller) traktPoller.stop();
      return;
    }
    ensureTraktPoller().start({ intervalMs: 2000, deadlineMs: Number(deadlineMs) || 0 });
  }

  async function requestTraktPin() {
    try { trqcStop(); } catch (_) {}
    setTraktSuccess(false);

    var cidEl = _el("trakt_client_id");
    var secEl = _el("trakt_client_secret");
    var cidState = _readSecretField(cidEl);
    var secState = _readSecretField(secEl);

    if (!cidState.hasValue) { _notify('Enter your Trakt Client ID'); return; }
    if (!secState.hasValue) { _notify('Enter your Trakt Client Secret'); return; }

    var payload = {};
    if (cidState.value) payload.client_id = cidState.value;
    if (secState.value) payload.client_secret = secState.value;

    var connectBtn = _el("btn-connect-trakt");
    if (connectBtn) { connectBtn.disabled = true; connectBtn.classList.add("busy"); }

    var win = null;
    try { win = window.open("about:blank", "_blank"); } catch (_) {}

    var resp, data;
    try {
      resp = await fetch(traktApi("/api/trakt/pin/new"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    } catch (e) {
      console.warn("[trakt] pin fetch failed", e);
      try { if (win && !win.closed) win.close(); } catch (_) {}
      _notify("Failed to request code");
      if (connectBtn) { connectBtn.disabled = false; connectBtn.classList.remove("busy"); }
      return;
    }

    try { data = await resp.json(); } catch (_) { data = null; }
    if (!resp.ok || !data || data.ok === false) {
      console.warn("[trakt] pin error payload", data);
      const status = (data && data.status) ? ` (HTTP ${data.status})` : "";
      const body   = (data && data.body) ? `: ${String(data.body).slice(0, 180)}` : "";
      _notify(((data && data.error) || "Code request failed") + status + body);
      try { if (win && !win.closed) win.close(); } catch (_) {}
      if (connectBtn) { connectBtn.disabled = false; connectBtn.classList.remove("busy"); }
      return;
    }

    var code = _str(data.user_code);
    var url  = _str(data.verification_url || data.verificationUrl) || "https://trakt.tv/activate";
    var secs = Number(data.expiresIn || data.expires_in || 0) || 300;

    var helpEl = _el("trakt_qc_help");
    if (helpEl) helpEl.textContent = win
      ? "Opening trakt.tv/activate — enter this code there and approve CrossWatch."
      : "Open trakt.tv/activate and enter this code to approve CrossWatch.";

    trqcShowCode(code, secs);
    try { startTraktTokenPoll(trqcDeadline); } catch (_) {}

    if (win && !win.closed) {
      try {
        win.document.write(
          '<!doctype html><meta charset="utf-8"><title>CrossWatch → Trakt</title>' +
          '<body style="margin:0;height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
          '<div><div style="font-size:14px;opacity:.7;margin-bottom:12px">Opening trakt.tv/activate…</div>' +
          '<div style="font-size:36px;font-weight:700;letter-spacing:.22em;color:#8ff0c2">' + code + '</div>' +
          '<div style="font-size:12px;opacity:.6;margin-top:12px">Redirecting in a moment…</div></div></body>'
        );
      } catch (_) {}
      setTimeout(function () { try { if (win && !win.closed) win.location.href = url; } catch (_) {} }, 3000);
    } else {
      _notify("Popup blocked - open trakt.tv/activate and enter the code.");
    }

    if (connectBtn) { connectBtn.disabled = false; connectBtn.classList.remove("busy"); }
  }

  // Disconnect Trakt via backend endpoint
  async function traktDeleteToken() {
    var btn = _el('btn-delete-trakt') || document.querySelector('#sec-trakt .btn.danger');
    var msg = document.getElementById('trakt_msg');
    try { trqcStop(); } catch (_) {}
    if (btn) { btn.disabled = true; btn.classList.add('busy'); }
    if (msg) { msg.classList.remove('hidden'); msg.classList.remove('warn'); msg.textContent=''; }
    try {
      var r = await fetch(traktApi('/api/trakt/token/delete'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
        cache: 'no-store'
      });
      var j = await r.json().catch(()=>({}));
      if (window.CW.AuthShared.reportProviderUsage({ status: r.status, data: j })) return;
      if (r.ok && (j.ok !== false)) {
        _setVal('trakt_pin',''); setTraktSuccess(false);
        try { var ce = _el('trakt_qc_code'); if (ce) ce.textContent = '------'; } catch (_) {}
        if (msg) msg.textContent = 'Disconnected';
      } else {
        if (msg) { msg.classList.add('warn'); msg.textContent = 'Could not remove token.'; }
      }
    } catch (_) {
      if (msg) { msg.classList.add('warn'); msg.textContent = 'Error removing token.'; }
    } finally {
      if (btn) { btn.disabled = false; btn.classList.remove('busy'); }
    }
  }

  // Lifecycle
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", initTraktAuthUI, { once: true });
  else initTraktAuthUI();

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.trakt = window.cwAuth.trakt || {};
  window.cwAuth.trakt.init = initTraktAuthUI;

  document.addEventListener("tab-changed", function (ev) {
    try {
      var id = ev && ev.detail ? ev.detail.id : "";
      if (id === "settings") {
        setTimeout(function () {
          hydrateAllSecretsRaw();
          startTraktTokenPoll();
        }, 150);
      }
    } catch (_) {}
  });

  // exports
  try {
    window.updateTraktHint              = updateTraktHint;
    window.flushTraktCreds              = flushTraktCreds;
    window.hydrateAuthFromConfig        = hydrateAuthFromConfig;
    window.hydrateSecretsRaw            = hydrateAllSecretsRaw;
    window.requestTraktPin              = requestTraktPin;
    window.startTraktTokenPoll          = startTraktTokenPoll;
    window.traktDeleteToken             = traktDeleteToken;
  } catch (_) {}
})();
