// auth.trakt.js
(function () {
  if (window._traktPatched) return;
  window._traktPatched = true;

  // Utils
  function _notify(msg) { try { if (typeof window.notify === "function") window.notify(msg); } catch (_) {} }
  function _el(id) { return document.getElementById(id); }
  function _setVal(id, v) { var el = _el(id); if (el) el.value = v == null ? "" : String(v); }
  function _str(x) { return (typeof x === "string" ? x : "").trim(); }

  const TRAKT_INSTANCE_KEY = "cw.ui.trakt.auth.instance.v1";

  function getTraktInstance() {
    var el = _el("trakt_instance");
    var v = el ? _str(el.value) : "";
    if (!v) { try { v = localStorage.getItem(TRAKT_INSTANCE_KEY) || ""; } catch (_) {} }
    v = _str(v) || "default";
    return v.toLowerCase() === "default" ? "default" : v;
  }

  function setTraktInstance(v) {
    var id = _str(String(v || "")) || "default";
    try { localStorage.setItem(TRAKT_INSTANCE_KEY, id); } catch (_) {}
    var el = _el("trakt_instance");
    if (el) el.value = id;
  }

  function traktApi(path) {
    var p = String(path || "");
    var sep = p.indexOf("?") >= 0 ? "&" : "?";
    return p + sep + "instance=" + encodeURIComponent(getTraktInstance()) + "&ts=" + Date.now();
  }

  function getTraktCfgBlock(cfg) {
    cfg = cfg || {};
    var base = (cfg.trakt && typeof cfg.trakt === "object") ? cfg.trakt : (cfg.trakt = {});
    var inst = getTraktInstance();
    if (inst === "default") return base;
    if (!base.instances || typeof base.instances !== "object") base.instances = {};
    if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
    return base.instances[inst];
  }

  async function refreshTraktInstanceOptions(preserve) {
    var sel = _el("trakt_instance");
    if (!sel) return;
    var want = preserve === false ? "default" : getTraktInstance();
    try {
      var r = await fetch("/api/provider-instances/trakt?ts=" + Date.now(), { cache: "no-store" });
      var arr = await r.json().catch(function(){ return []; });
      var opts = Array.isArray(arr) ? arr : [];
      sel.innerHTML = "";
      function addOpt(id, label) {
        var o = document.createElement("option");
        o.value = String(id);
        o.textContent = String(label || id);
        sel.appendChild(o);
      }
      addOpt("default", "Default");
      opts.forEach(function(o){ if (o && o.id && o.id !== "default") addOpt(o.id, o.label || o.id); });
      if (!Array.from(sel.options).some(function(o){ return o.value === want; })) want = "default";
      sel.value = want;
      setTraktInstance(want);
    } catch (_) {}
  }

  function ensureTraktInstanceUI() {
    var panel = document.querySelector('#sec-trakt .cw-meta-provider-panel[data-provider="trakt"]') || document.querySelector('.cw-meta-provider-panel[data-provider="trakt"]') || document.querySelector('#sec-trakt .cw-meta-provider-panel') || document.querySelector('#sec-trakt') || document.querySelector('[data-provider="trakt"]');
    if (!panel) return;
    if (panel.querySelector('#trakt_instance')) return;
    var head = panel.querySelector('.cw-panel-head') || panel.querySelector('.panel-head') || panel.querySelector('header') || panel;
    if (head.__traktInstanceUI) return;
    head.__traktInstanceUI = true;

    var wrap = document.createElement('div');
    wrap.className = 'inline';
    wrap.style.display = 'flex';
    wrap.style.gap = '8px';
    wrap.style.alignItems = 'center';
    try { head.style.flexWrap = 'wrap'; } catch (_) {}
    wrap.title = 'Select which Trakt account this config applies to.';

    var lab = document.createElement('span');
    lab.className = 'muted';
    lab.textContent = 'Profile';

    var sel = document.createElement('select');
    sel.id = 'trakt_instance';
    sel.className = 'input';
    sel.style.minWidth = '140px';
    sel.style.width = 'auto';
    sel.style.maxWidth = '220px';
    sel.style.flex = '0 0 auto';

    var btnNewEl = document.createElement('button');
    btnNewEl.type = 'button';
    btnNewEl.className = 'btn secondary';
    btnNewEl.id = 'trakt_instance_new';
    btnNewEl.textContent = 'New';

    var btnDelEl = document.createElement('button');
    btnDelEl.type = 'button';
    btnDelEl.className = 'btn secondary';
    btnDelEl.id = 'trakt_instance_del';
    btnDelEl.textContent = 'Delete';

    wrap.appendChild(lab);
    wrap.appendChild(sel);
    wrap.appendChild(btnNewEl);
    wrap.appendChild(btnDelEl);
    if (head === panel) panel.insertBefore(wrap, panel.firstChild);
    else head.appendChild(wrap);

    refreshTraktInstanceOptions(true);

    var sel = _el('trakt_instance');
    if (sel && !sel._wired) {
      sel._wired = true;
      sel.addEventListener('change', function(){
        setTraktInstance(sel.value);
        void hydrateAuthFromConfig();
        try { startTraktTokenPoll(); } catch (_) {}
      });
    }

    var btnNew = _el('trakt_instance_new');
    if (btnNew && !btnNew._wired) {
      btnNew._wired = true;
      btnNew.addEventListener('click', async function(){
        try {
          var r = await fetch('/api/provider-instances/trakt/next?ts=' + Date.now(), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}', cache: 'no-store' });
          var j = await r.json().catch(function(){ return {}; });
          var id = _str((j && j.id) || '');
          if (!r.ok || (j && j.ok === false) || !id) throw new Error(String((j && j.error) || 'create_failed'));
          setTraktInstance(id);
          await refreshTraktInstanceOptions(true);
          void hydrateAuthFromConfig();
        } catch (e) {
          _notify('Could not create profile: ' + (e && e.message ? e.message : e));
        }
      });
    }

    var btnDel = _el('trakt_instance_del');
    if (btnDel && !btnDel._wired) {
      btnDel._wired = true;
      btnDel.addEventListener('click', async function(){
        var id = getTraktInstance();
        if (id === 'default') return _notify('Default profile cannot be deleted.');
        if (!confirm('Delete Trakt profile "' + id + '"?')) return;
        try {
          var r = await fetch('/api/provider-instances/trakt/' + encodeURIComponent(id), { method: 'DELETE', cache: 'no-store' });
          var j = await r.json().catch(function(){ return {}; });
          if (!r.ok || (j && j.ok === false)) throw new Error(String((j && j.error) || 'delete_failed'));
          setTraktInstance('default');
          await refreshTraktInstanceOptions(false);
          void hydrateAuthFromConfig();
        } catch (e) {
          _notify('Could not delete profile: ' + (e && e.message ? e.message : e));
        }
      });
    }
  }

  async function persistTraktClientFields() {
    try {
      var cid = _str((_el('trakt_client_id') || {}).value);
      var secr = _str((_el('trakt_client_secret') || {}).value);
      var cfg = await fetchConfig();
      if (!cfg) return;
      var base = (cfg.trakt && typeof cfg.trakt === 'object') ? cfg.trakt : (cfg.trakt = {});
      var inst = getTraktInstance();
      if (cid) base.client_id = cid;
      if (secr) base.client_secret = secr;
      if (inst !== 'default') {
        if (!base.instances || typeof base.instances !== 'object') base.instances = {};
        if (!base.instances[inst] || typeof base.instances[inst] !== 'object') base.instances[inst] = {};
        base.instances[inst].client_id = cid;
        base.instances[inst].client_secret = secr;
      }
      await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg) });
    } catch (_) {}
  }

  function updateTraktBanner() {
    try {
      var msg = _el('trakt_msg');
      if (!msg) return;
      var pin = _str(_el('trakt_pin')?.value);
      var tok = _str(_el('trakt_token')?.value);
      msg.classList.remove('hidden', 'ok', 'warn');
      if (tok) { msg.classList.add('ok'); msg.textContent = 'Connected.'; return; }
      if (pin) { msg.classList.add('warn'); msg.textContent = 'Code: ' + pin; return; }
      msg.classList.add('hidden'); msg.textContent = '';
    } catch (_) {}
  }
  // status banner
  function setTraktSuccess(show) {
    try {
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
  async function hydratePlexFromConfigRaw() {
    var cfg = await fetchConfig(); if (!cfg) return;
    var tok = _str(cfg.plex && cfg.plex.account_token);
    if (tok) _setVal("plex_token", tok);
  }

  async function hydrateSimklFromConfigRaw() {
    var cfg = await fetchConfig(); if (!cfg) return;
    var s = cfg.simkl || {};
    var a = (cfg.auth && cfg.auth.simkl) || {};
    _setVal("simkl_client_id",     _str(s.client_id));
    _setVal("simkl_client_secret", _str(s.client_secret));
    _setVal("simkl_access_token",  _str(s.access_token || a.access_token));
  }

  async function hydrateAuthFromConfig() {
    try {
      var cfg = await fetchConfig(); if (!cfg) return;
      var t = getTraktCfgBlock(cfg);
      var a = (cfg.auth && cfg.auth.trakt) || {};
            var isDefault = (getTraktInstance() === "default");
      _setVal("trakt_client_id",     _str(t.client_id || (isDefault ? (cfg.trakt && cfg.trakt.client_id) : "")));
      _setVal("trakt_client_secret", _str(t.client_secret || (isDefault ? (cfg.trakt && cfg.trakt.client_secret) : "")));
      _setVal("trakt_token",         _str(t.access_token || (getTraktInstance() === 'default' ? a.access_token : '')));
      _setVal("trakt_pin",           _str((t._pending_device && t._pending_device.user_code) || ''));
      updateTraktHint();
      updateTraktBanner();
    } catch (e) {
      console.warn("[trakt] hydrateAuthFromConfig failed", e);
    }
  }

  async function hydrateAllSecretsRaw() {
    try { await hydratePlexFromConfigRaw(); } catch (_) {}
    try { await hydrateSimklFromConfigRaw(); } catch (_) {}
    try { await hydrateAuthFromConfig(); } catch (_) {}
  }

  // Hint
  function updateTraktHint() {
    try {
      var cid  = _str((_el("trakt_client_id")    || {}).value);
      var secr = _str((_el("trakt_client_secret")|| {}).value);
      var hint = _el("trakt_hint");
      if (!hint) return;
      var show = !(cid && secr);
      hint.classList.toggle("hidden", !show);
      hint.style.display = show ? "" : "none";
    } catch (_) {}
  }

  // Copy helpers
  async function _copyText(text, btn) {
    if (!text) return false;
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
      } else {
        var ta = document.createElement("textarea");
        ta.value = text;
        ta.setAttribute("readonly", "");
        ta.style.position = "fixed";
        ta.style.top = "-9999px";
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        document.body.removeChild(ta);
      }
      if (btn) {
        btn.classList.add("copied");
        setTimeout(function(){ btn.classList.remove("copied"); }, 1200);
      }
      return true;
    } catch (e) {
      console.warn("Copy failed", e);
      return false;
    }
  }

  window.copyInputValue = async function (inputId, btn) {
    var el = document.getElementById(inputId);
    if (!el) return;
    await _copyText(el.value || "", btn);
  };

  window.copyTraktRedirect = async function () {
    var code = document.getElementById("trakt_redirect_uri_preview");
    var text = (code && code.textContent ? code.textContent : "urn:ietf:wg:oauth:2.0:oob").trim();
    await _copyText(text);
  };

  window.copyRedirect = async function () {
    var code = document.getElementById("redirect_uri_preview");
    var text = ((code && code.textContent) || (code && code.value) || "").trim();
    await _copyText(text);
  };

  var __traktInitDone = false;
  function initTraktAuthUI() {
    if (__traktInitDone) return;
    __traktInitDone = true;

    [
      ["btn-copy-trakt-pin",   "trakt_pin"],
      ["btn-copy-trakt-token", "trakt_token"],
      ["btn-copy-plex-pin",    "plex_pin"],
      ["btn-copy-plex-token",  "plex_token"]
    ].forEach(function (pair) {
      var btnId = pair[0], inputId = pair[1];
      var b = document.getElementById(btnId);
      if (b && !b._copyHooked) {
        b.addEventListener("click", function () { window.copyInputValue(inputId, this); });
        b._copyHooked = true;
      }
    });

    try {
      ensureTraktInstanceUI();
      var idEl  = _el("trakt_client_id");
      var secEl = _el("trakt_client_secret");
      if (idEl)  idEl.addEventListener("input", function(){ updateTraktHint(); });
      if (secEl) secEl.addEventListener("input", function(){ updateTraktHint(); });
      if (idEl)  idEl.addEventListener('change', function(){ void persistTraktClientFields(); });
      if (secEl) secEl.addEventListener('change', function(){ void persistTraktClientFields(); });

      updateTraktHint();
      updateTraktBanner();
      hydrateAllSecretsRaw();
      startTraktTokenPoll();
    } catch (e) {
      console.warn("[trakt] init failed", e);
    }

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
        _setVal("trakt_token", tok);
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
    var cid   = _str(cidEl ? cidEl.value : "");
    var secr  = _str(secEl ? secEl.value : "");

    if (!cid) { _notify("Vul je Trakt Client ID in"); return; }

    var win = null;
    try { win = window.open("https://trakt.tv/activate", "_blank"); } catch (_) {}

    var resp, data;
    try {
      resp = await fetch(traktApi("/api/trakt/pin/new"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ client_id: cid, client_secret: secr })
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
        try { await navigator.clipboard.writeText(code); } catch (_) {}
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

  // Delete Trakt access token via backend endpoint
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
        _setVal('trakt_token',''); _setVal('trakt_pin',''); setTraktSuccess(false);
        if (msg) msg.textContent = 'Access token removed.';
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
    window.hydratePlexFromConfigRaw     = hydratePlexFromConfigRaw;
    window.hydrateSimklFromConfigRaw    = hydrateSimklFromConfigRaw;
    window.hydrateSecretsRaw            = hydrateAllSecretsRaw;
    window.requestTraktPin              = requestTraktPin;
    window.startTraktTokenPoll          = startTraktTokenPoll;       // legacy/fallback
    window.startTraktDevicePoll         = startTraktDevicePoll;
    window.traktDeleteToken             = traktDeleteToken;
  } catch (_) {}
})();
