// auth.trakt.js — secrets hydration + Trakt device flow (idempotent, no LHS optional-chaining)
(function () {
  if (window._traktPatched) return;
  window._traktPatched = true;

  // --- helpers --------------------------------------------------------------
  function _notify(msg) { try { if (typeof window.notify === "function") window.notify(msg); } catch (_) {} }
  function _el(id) { return document.getElementById(id); }
  function _setVal(id, v) { var el = _el(id); if (el) el.value = v == null ? "" : String(v); }
  function _str(x) { return (typeof x === "string" ? x : "").trim(); }

  function setTraktSuccess(show) {
    try { var el = _el("trakt_msg"); if (el) el.classList.toggle("hidden", !show); } catch (_) {}
  }

  // --- CONFIG fetch (single place) -----------------------------------------
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

  // --- RAW hydration (Plex + SIMKL + Trakt token) --------------------------
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

  // Trakt-only (CID/Secret/Token) straight from cfg (unmasked)
  async function hydrateAuthFromConfig() {
    try {
      var cfg = await fetchConfig(); if (!cfg) return;
      var t = cfg.trakt || {};
      var a = (cfg.auth && cfg.auth.trakt) || {};
      _setVal("trakt_client_id",     _str(t.client_id));
      _setVal("trakt_client_secret", _str(t.client_secret));
      _setVal("trakt_token",         _str(t.access_token || a.access_token));
      updateTraktHint();
    } catch (e) {
      console.warn("[trakt] hydrateAuthFromConfig failed", e);
    }
  }

  async function hydrateAllSecretsRaw() {
    // call raw hydrators; keep separate calls so each can succeed independently
    try { await hydratePlexFromConfigRaw(); } catch (_) {}
    try { await hydrateSimklFromConfigRaw(); } catch (_) {}
    try { await hydrateAuthFromConfig(); } catch (_) {}
  }

  // --- Trakt hint and copy --------------------------------------------------
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

  // Robust copy helpers + auto-bind for copy buttons
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

  // For the hint buttons:
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

  // Bind listeners in case inline onclicks are off or 'self' was used somewhere
  document.addEventListener("DOMContentLoaded", function () {
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
  });

  // --- Flush Trakt creds from cfg (new + legacy location) -------------------
  async function flushTraktCreds() {
    try {
      var cfg = await fetchConfig(); if (!cfg) return;
      cfg.trakt = cfg.trakt || {};
      cfg.auth  = cfg.auth  || {};
      cfg.auth.trakt = cfg.auth.trakt || {};

      delete cfg.trakt.access_token;
      delete cfg.trakt.refresh_token;
      delete cfg.trakt.scope;
      delete cfg.trakt.token_type;
      delete cfg.trakt.expires_at;
      delete cfg.trakt._pending_device;

      delete cfg.auth.trakt.access_token;
      delete cfg.auth.trakt.refresh_token;
      delete cfg.auth.trakt.scope;
      delete cfg.auth.trakt.token_type;
      delete cfg.auth.trakt.expires_at;

      await fetch("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(cfg),
      });

      _setVal("trakt_token", "");
      _setVal("trakt_pin", "");
      setTraktSuccess(false);
      _notify("Trakt-gegevens gewist");
    } catch (e) {
      console.warn("flushTraktCreds failed", e);
      _notify("Wissen mislukt");
    }
  }

  // --- Pollers --------------------------------------------------------------
  // New: poll the backend /api/trakt/pin/poll (device flow) until token is issued
  function startTraktDevicePoll(maxMs) {
    try { if (window._traktPoll) clearTimeout(window._traktPoll); } catch (_){}
    var MAX_MS   = typeof maxMs === "number" ? maxMs : 180000; // 3 min
    var deadline = Date.now() + MAX_MS;
    var interval = 4000; // server may suggest slow_down; we keep modest default

    var tick = async function () {
      if (Date.now() >= deadline) { window._traktPoll = null; return; }

      try {
        var r = await fetch("/api/trakt/pin/poll", { method: "POST" });
        var data = null;
        try { data = await r.json(); } catch (_){ data = null; }

        if (!r.ok || !data || data.ok === false) {
          // Pending is not an error; keep polling
          if (data && (data.pending || data.error === "authorization_pending" || data.error === "slow_down")) {
            window._traktPoll = setTimeout(tick, interval);
            return;
          }
          // Hard error: stop and surface
          console.warn("[trakt] device poll error", data);
          _notify((data && data.error) || "PIN poll failed");
          window._traktPoll = null;
          return;
        }

        // Success — token was stored by backend
        var token =
          (data.data && data.data.access_token) ? data.data.access_token :
          null;

        if (token) {
          _setVal("trakt_token", token);
          setTraktSuccess(true);
        } else {
          // Fall back to config read if payload omitted token echo
          try {
            var cfg = await fetchConfig();
            var tok = _str(cfg && ((cfg.trakt && cfg.trakt.access_token) || (cfg.auth && cfg.auth.trakt && cfg.auth.trakt.access_token)));
            if (tok) {
              _setVal("trakt_token", tok);
              setTraktSuccess(true);
            }
          } catch (_) {}
        }
        window._traktPoll = null;
      } catch (e) {
        console.warn("[trakt] device poll failed", e);
        window._traktPoll = setTimeout(tick, 6000);
      }
    };

    window._traktPoll = setTimeout(tick, 1500);
  }

  // Legacy: poll /api/config for the token (kept as a secondary fallback)
  function startTraktTokenPoll() {
    try { if (window._traktPollCfg) clearTimeout(window._traktPollCfg); } catch (_){}
    var MAX_MS   = 120000;
    var deadline = Date.now() + MAX_MS;
    var backoff  = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    var i = 0;

    var poll = async function () {
      if (Date.now() >= deadline) { window._traktPollCfg = null; return; }

      var page = _el("page-settings");
      var settingsVisible = !!(page && !page.classList.contains("hidden"));
      if (document.hidden || !settingsVisible) {
        window._traktPollCfg = setTimeout(poll, 5000);
        return;
      }

      var cfg = await fetchConfig();
      var tok = _str(cfg && ((cfg.trakt && cfg.trakt.access_token) || (cfg.auth && cfg.auth.trakt && cfg.auth.trakt.access_token)));
      if (tok) {
        _setVal("trakt_token", tok);
        setTraktSuccess(true);
        window._traktPollCfg = null;
        return;
      }

      var delay = backoff[Math.min(i, backoff.length - 1)];
      i++;
      window._traktPollCfg = setTimeout(poll, delay);
    };

    window._traktPollCfg = setTimeout(poll, 1000);
  }

  // --- Trakt: request PIN (device flow) ------------------------------------
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
      resp = await fetch("/api/trakt/pin/new", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        // backend will forward only client_id to Trakt (secret kept for token exchange)
        body: JSON.stringify({ client_id: cid, client_secret: secr })
      });
    } catch (e) {
      console.warn("[trakt] pin fetch failed", e);
      _notify("Failed to request code");
      return;
    }

    try { data = await resp.json(); } catch (_) { data = null; }
    if (!resp.ok || !data || data.ok === false) {
      console.warn("[trakt] pin error payload", data);
      const status = (data && data.status) ? ` (HTTP ${data.status})` : "";
      const body   = (data && data.body) ? `: ${String(data.body).slice(0, 180)}` : "";
      _notify(((data && data.error) || "Code request failed") + status + body);
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
        // Start backend device poll (primary) + config poll (fallback)
        try { startTraktDevicePoll(); } catch (_) {}
        try { startTraktTokenPoll(); } catch (_) {}
      }

      if (win && !win.closed) {
        try { win.location.href = url; win.focus(); } catch (_) {}
      }
    } catch (e) {
      console.warn("[trakt] ui update failed", e);
    }
  }

  // --- Wire up + lifecycle --------------------------------------------------
  document.addEventListener("DOMContentLoaded", function () {
    try {
      var idEl  = _el("trakt_client_id");
      var secEl = _el("trakt_client_secret");
      if (idEl)  idEl.addEventListener("input", function(){ updateTraktHint(); });
      if (secEl) secEl.addEventListener("input", function(){ updateTraktHint(); });

      updateTraktHint();
      // raw hydration for all providers
      hydrateAllSecretsRaw();
      // begin background poll in case user already activated (legacy path)
      startTraktTokenPoll();
    } catch (e) {
      console.warn("[trakt] DOMContentLoaded init failed", e);
    }
  });

  // Also (re)hydrate when switching to the Settings tab
  document.addEventListener("tab-changed", function (ev) {
    try {
      var id = ev && ev.detail ? ev.detail.id : "";
      if (id === "settings") {
        // run shortly after the settings DOM is rendered (and after providers mount)
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
    window.hydrateAuthFromConfig        = hydrateAuthFromConfig;     // Trakt-only
    window.hydratePlexFromConfigRaw     = hydratePlexFromConfigRaw;
    window.hydrateSimklFromConfigRaw    = hydrateSimklFromConfigRaw;
    window.hydrateSecretsRaw            = hydrateAllSecretsRaw;      // all of the above
    window.requestTraktPin              = requestTraktPin;
    window.startTraktTokenPoll          = startTraktTokenPoll;       // legacy/fallback
    window.startTraktDevicePoll         = startTraktDevicePoll;      // new primary poller
  } catch (_) {}
})();
