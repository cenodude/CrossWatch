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
      var pinEl = _el('trakt_pin'); var pin = _str(pinEl ? pinEl.value : '');
      if (traktConnected) return Shared.setStatusPill(msg, "ok", "Connected");
      if (pin) return Shared.setStatusPill(msg, "warn", "Code: " + pin);
      return Shared.setStatusPill(msg, null);
    } catch (_) {}
  }
  // status banner
  function setTraktSuccess(show) {
    try {
      traktConnected = !!show;
      if (show) updateTraktBanner();
      else { var el = _el('trakt_msg'); if (el) { el.classList.add('hidden'); el.textContent = ''; el.classList.remove('ok','warn'); } }
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

  var __traktInitDone = false;
  function initTraktAuthUI() {
    Shared.wireCopyButton("btn-copy-trakt-pin", "trakt_pin");

    try {
      ensureTraktInstanceUI();
      var idEl  = _el("trakt_client_id");
      var secEl = _el("trakt_client_secret");
      _wireSecretField(idEl, updateTraktHint);
      _wireSecretField(secEl, updateTraktHint);
      if (idEl)  idEl.addEventListener('change', function(){ void persistTraktClientFields(); });
      if (secEl) secEl.addEventListener('change', function(){ void persistTraktClientFields(); });

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

  // Device code poller
  function startTraktDevicePoll(maxMs) {
    try { if (String(location.port || "") === "8787") return; } catch (_) {}

    try { if (window._traktPoll) clearTimeout(window._traktPoll); } catch (_){}
    var MAX_MS = typeof maxMs === "number" ? maxMs : 180000; // 3 min
    var deadline = Date.now() + MAX_MS;
    var backoff = [1200, 2000, 3000, 4000, 5000, 7000, 10000, 12000];
    var i = 0;

    var tick = async function () {
      if (Date.now() >= deadline) { window._traktPoll = null; return; }
      try {
        var cfg = await fetchConfig();
        var t = cfg ? getTraktCfgBlock(cfg) : null;
        var ok = !!(t && _str(t.access_token));
        if (ok) { setTraktSuccess(true); window._traktPoll = null; return; }
      } catch (_) { /* ignore; keep polling */ }
      var delay = backoff[Math.min(i++, backoff.length - 1)];
      window._traktPoll = setTimeout(tick, delay);
    };

    window._traktPoll = setTimeout(tick, backoff[0]);
  }

  function startTraktTokenPoll() {
    try { if (window._traktPollCfg) clearTimeout(window._traktPollCfg); } catch (_){}
    try { if (window._traktVisPollCfg) document.removeEventListener("visibilitychange", window._traktVisPollCfg); } catch (_){}
    window._traktVisPollCfg = null;

    var pinNow = _str(_el("trakt_pin")?.value);
    if (traktConnected || !pinNow) {
      window._traktPollCfg = null;
      return;
    }

    var MAX_MS    = 120000;
    var startTs   = Date.now();
    var deadline  = startTs + MAX_MS;
    var fastUntil = startTs + 30000; // 2s polling for ~30s
    var backoff   = [5000, 7500, 10000, 15000, 20000, 20000];
    var i = 0;
    var inFlight = false;

    var cleanup = function () {
      try { if (window._traktPollCfg) clearTimeout(window._traktPollCfg); } catch (_){}
      window._traktPollCfg = null;
      try {
        if (window._traktVisPollCfg) document.removeEventListener("visibilitychange", window._traktVisPollCfg);
      } catch (_){}
      window._traktVisPollCfg = null;
    };

    var poll = async function () {
      if (Date.now() >= deadline) { cleanup(); return; }

      var page = _el("page-settings");
      var settingsVisible = !!(page && !page.classList.contains("hidden"));
      if (document.hidden || !settingsVisible) {
        window._traktPollCfg = setTimeout(poll, 5000);
        return;
      }
      if (inFlight) return;

      inFlight = true;
      var cfg = null;
      try { cfg = await fetchConfig(); } catch (_) { cfg = null; }
      inFlight = false;

      var tok = _str(cfg && ((cfg.trakt && cfg.trakt.access_token) || (cfg.auth && cfg.auth.trakt && cfg.auth.trakt.access_token)));
      try {
        var t = cfg ? getTraktCfgBlock(cfg) : null;
        tok = _str(t && t.access_token) || (getTraktInstance() === 'default' ? tok : '');
      } catch (_) {}
      if (tok) {
        setTraktSuccess(true);
        cleanup();
        return;
      }

      var delay = (Date.now() < fastUntil) ? 2000 : backoff[Math.min(i++, backoff.length - 1)];
      window._traktPollCfg = setTimeout(poll, delay);
    };

    window._traktVisPollCfg = function () {
      if (document.hidden) return;
      var page = _el("page-settings");
      var settingsVisible = !!(page && !page.classList.contains("hidden"));
      if (!settingsVisible) return;
      if (!window._traktPollCfg) return;
      clearTimeout(window._traktPollCfg);
      window._traktPollCfg = null;
      void poll();
    };
    document.addEventListener("visibilitychange", window._traktVisPollCfg);

    window._traktPollCfg = setTimeout(poll, 2000);
  }

  // Pin request flow
  async function requestTraktPin() {
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

    var win = null;
    try { win = window.open("https://trakt.tv/activate", "_blank"); } catch (_) {}

    var resp, data;
    try {
      resp = await fetch(traktApi("/api/trakt/pin/new"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    } catch (e) {
      console.warn("[trakt] pin fetch failed", e);
      _notify("Failed to request code");
      try { if (win && !win.closed) win.close(); } catch (_) {}
      return;
    }

    try { data = await resp.json(); } catch (_) { data = null; }
    if (!resp.ok || !data || data.ok === false) {
      console.warn("[trakt] pin error payload", data);
      const status = (data && data.status) ? ` (HTTP ${data.status})` : "";
      const body   = (data && data.body) ? `: ${String(data.body).slice(0, 180)}` : "";
      _notify(((data && data.error) || "Code request failed") + status + body);
      try { if (win && !win.closed) win.close(); } catch (_) {}
      return;
    }

    var code = _str(data.user_code);
    var url  = _str(data.verification_url) || "https://trakt.tv/activate";

    try {
      var pinEl = _el("trakt_pin");
      if (pinEl) pinEl.value = code;

      var msg = _el("trakt_msg");
      if (msg) {
        msg.textContent = code ? "Code: " + code : "Code request ok";
        msg.classList.remove("hidden");
      }

      if (code) {
        try { await Shared.copyText(code, null, { failureMessage: false }); } catch (_) {}
        try { startTraktDevicePoll(); } catch (_) {}
        try { startTraktTokenPoll(); } catch (_) {}
      }

      if (win && !win.closed) {
        try { win.location.href = url; win.focus(); } catch (_) {}
      } else {
        _notify("Popup blocked - allow popups and try again");
      }
    } catch (e) {
      console.warn("[trakt] ui update failed", e);
    }
  }

  // Disconnect Trakt via backend endpoint
  async function traktDeleteToken() {
    var btn = document.querySelector('#sec-trakt .btn.danger');
    var msg = document.getElementById('trakt_msg');
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
      if (r.ok && (j.ok !== false)) {
        _setVal('trakt_pin',''); setTraktSuccess(false);
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
    window.startTraktDevicePoll         = startTraktDevicePoll;
    window.traktDeleteToken             = traktDeleteToken;
  } catch (_) {}
})();
