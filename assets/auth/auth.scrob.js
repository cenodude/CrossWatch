// assets/auth/auth.scrob.js
// CrossWatch - Scrob auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._scrobAuthPatched) return;
  window._scrobAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const profile = Shared.createProfileAdapter({
    provider: "scrob",
    configKey: "scrob",
    label: "Scrob",
    sectionId: "sec-scrob",
    selectId: "scrob_instance",
    storageKey: "cw.ui.scrob.auth.instance.v1",
    title: T("connections.auth.gen.selectServer", {provider: "Scrob"}),
  });

  const api = (path) => profile ? profile.api(path) : String(path || "");
  const cfgBlock = (cfg) => profile ? profile.cfgBlock(cfg, true) : {};
  window.__cwScrobPendingAuth = window.__cwScrobPendingAuth || {};

  function friendlyError(code) {
    const key = String(code || "").trim();
    switch (key) {
      case "server_url_required": return T("connections.validation.serverUrlRequired", {provider: "Scrob"});
      case "api_key_required": return T("connections.validation.apiKeyRequired", {provider: "Scrob"});
      case "username_required": return T("connections.validation.usernameRequired", {provider: "Scrob"});
      case "password_required": return T("connections.validation.passwordRequired", {provider: "Scrob"});
      case "invalid_api_key": return T("connections.validation.invalidApiKey", {provider: "Scrob"});
      case "invalid_credentials": return T("connections.validation.invalidCredentials", {provider: "Scrob"});
      case "totp_required": return T("connections.auth.totpRequired");
      case "invalid_totp_code": return T("connections.auth.totpInvalid");
      case "credentials_mismatch": return T("connections.validation.credentialsMismatch", {provider: "Scrob"});
      case "password_login_disabled": return T("connections.validation.passwordLoginDisabled", {provider: "Scrob"});
      case "email_not_confirmed": return T("connections.validation.emailNotConfirmed", {provider: "Scrob"});
      case "api_not_found": return T("connections.validation.apiNotFound", {provider: "Scrob"});
      case "api_prefix_mismatch": return T("connections.validation.apiPrefixMismatch", {provider: "Scrob"});
      case "validation_timeout": return T("connections.validation.timeout", {provider: "Scrob"});
      case "unreachable": return T("connections.validation.reachFailed", {provider: "Scrob"});
      case "invalid_ssl": return T("connections.validation.sslFailed", {provider: "Scrob"});
      case "validation_bad_response": return T("connections.validation.returnedUnexpected", {provider: "Scrob"});
      case "server_error": return T("connections.validation.serverError", {provider: "Scrob"});
      default:
        if (key.startsWith("validation_http_")) return T("connections.validation.httpFailed", {provider: "Scrob"});
        return key || T("connections.auth.notConnected");
    }
  }

  function showTotp(show) {
    const row = el("scrob_totp_row");
    if (row) row.classList.toggle("hidden", !show);
    if (show) el("scrob_totp")?.focus();
  }

  function showReauth(show) {
    el("scrob_reauth")?.classList.toggle("hidden", !show);
    if (show) showTotp(true);
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked("scrob_connect", !!ok); } catch {}
    return Shared.setStatus("scrob_msg", ok, msg || (ok ? T("connections.auth.connected") : T("connections.auth.notConnected")));
  }

  function currentSignature() {
    const keyInput = el("scrob_key");
    const passInput = el("scrob_password");
    return JSON.stringify({
      server_url: txt(el("scrob_server")?.value || ""),
      api_key: keyInput?.dataset?.masked === "1" ? "" : txt(keyInput?.value || ""),
      has_key: keyInput?.dataset?.hasKey === "1",
      username: txt(el("scrob_username")?.value || ""),
      password: passInput?.dataset?.masked === "1" ? "" : String(passInput?.value || ""),
      has_password: passInput?.dataset?.hasKey === "1",
      verify_ssl: !!el("scrob_verify_ssl")?.checked,
    });
  }

  function pendingKey() {
    try { return profile?.getInstance?.() || "default"; } catch { return "default"; }
  }

  function stagePending(data) {
    const inst = pendingKey();
    window.__cwScrobPendingAuth[inst] = { signature: currentSignature(), data: { ...(data || {}) } };
  }

  function clearPending() {
    try { delete window.__cwScrobPendingAuth[pendingKey()]; } catch {}
  }

  function ensureUI() {
    profile?.ensureUI(() => { void hydrate(); });
  }

  async function refresh() {
    try {
      const r = await Shared.fetchJSON(api("/api/scrob/status?verify=1"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      const reauth = !!(r.data && r.data.reauth_required);
      showReauth(reauth);
      if (!reauth && r.data && r.data.totp_enabled) showTotp(true);
      setConn(ok && !reauth, reauth ? T("connections.auth.twoFactorRequired") : ok ? T("connections.auth.connected") : friendlyError(r.data && r.data.reason));
    } catch {
      setConn(false, T("connections.validation.connectFailed", {provider: "Scrob"}));
    }
  }

  async function hydrate() {
    ensureUI();
    const cfg = window._cfgCache || await Shared.getConfig();
    const s = cfgBlock(cfg);
    const serverEl = el("scrob_server");
    if (serverEl) {
      serverEl.value = txt(s?.server_url || "");
      serverEl.dataset.loaded = "1";
      serverEl.dataset.touched = "";
    }
    const userEl = el("scrob_username");
    if (userEl) {
      userEl.value = txt(s?.username || "");
      userEl.dataset.loaded = "1";
      userEl.dataset.touched = "";
    }
    const verifyEl = el("scrob_verify_ssl");
    if (verifyEl) {
      verifyEl.checked = s?.verify_ssl === true;
      verifyEl.dataset.loaded = "1";
      verifyEl.dataset.touched = "";
    }
    Shared.maskSecret(el("scrob_key"), !!txt(s?.api_key || ""));
    Shared.maskSecret(el("scrob_password"), !!txt(s?.password || ""));
    if (el("scrob_totp")) el("scrob_totp").value = "";
    showTotp(!!s?.totp_enabled);
    showReauth(!!s?.reauth_required);
    await refresh();
  }

  async function onSave() {
    const server = txt(el("scrob_server")?.value || "");
    const keyInput = el("scrob_key");
    const passInput = el("scrob_password");
    const key = txt(keyInput?.value || "");
    const username = txt(el("scrob_username")?.value || "");
    const password = passInput?.value || "";
    const verify_ssl = !!el("scrob_verify_ssl")?.checked;

    if (!server) { note(T("connections.validation.serverUrlRequired", {provider: "Scrob"})); return; }
    if (!key && !(keyInput && keyInput.dataset.hasKey === "1")) { note(T("connections.validation.apiKeyRequired", {provider: "Scrob"})); return; }
    if (!username) { note(T("connections.validation.usernameRequired", {provider: "Scrob"})); return; }
    if (!password && !(passInput && passInput.dataset.hasKey === "1")) { note(T("connections.validation.passwordRequired", {provider: "Scrob"})); return; }

    try {
      const r = await Shared.fetchJSON(api("/api/scrob/save?validate_only=1"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_key: key, username, password, verify_ssl, totp_code: txt(el("scrob_totp")?.value || ""), validate_only: true }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) {
        if (r.data?.requires_2fa) showTotp(true);
        throw new Error(friendlyError(r.data?.error || "validation_failed"));
      }
      if (key) Shared.maskSecret(keyInput, true);
      if (password) Shared.maskSecret(passInput, true);
      if (el("scrob_totp")) el("scrob_totp").value = "";
      showReauth(false);
      showTotp(!!r.data?.totp_enabled);
      stagePending(r.data || {});
      setConn(true, T("connections.auth.connectedSave"));
      note(T("connections.auth.gen.connectedSave", {provider: "Scrob"}));
    } catch (e) {
      const msg = e && e.message ? e.message : T("connections.validation.saveFailed", {provider: "Scrob"});
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisconnect() {
    try {
      const r = await Shared.fetchJSON(api("/api/scrob/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");
      if (el("scrob_server")) el("scrob_server").value = "";
      if (el("scrob_username")) el("scrob_username").value = "";
      Shared.maskSecret(el("scrob_key"), false);
      Shared.maskSecret(el("scrob_password"), false);
      setConn(false);
      note(T("connections.auth.gen.disconnected", {provider: "Scrob"}));
    } catch (e) {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Scrob"}) + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wireTouched(id) {
    const node = el(id);
    if (node && !node.__wiredTouched) {
      const dirty = () => {
        node.dataset.touched = "1";
        clearPending();
        try { Shared.setConnectLocked("scrob_connect", false); } catch {}
      };
      node.addEventListener("input", dirty);
      node.addEventListener("change", dirty);
      node.__wiredTouched = true;
    }
  }

  function wire() {
    const save = el("scrob_connect");
    if (save && !save.__wired) { save.addEventListener("click", onSave); save.__wired = true; }
    const disconnect = el("scrob_disconnect");
    if (disconnect && !disconnect.__wired) { disconnect.addEventListener("click", onDisconnect); disconnect.__wired = true; }
    for (const id of ["scrob_key", "scrob_password"]) {
      const node = el(id);
      if (node && !node.__wiredSecret) {
        Shared.wireSecretInput(node);
        node.__wiredSecret = true;
      }
    }
    wireTouched("scrob_server");
    wireTouched("scrob_key");
    wireTouched("scrob_username");
    wireTouched("scrob_password");
    wireTouched("scrob_verify_ssl");
    wireTouched("scrob_totp");
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;
    const pending = window.__cwScrobPendingAuth?.[pendingKey()];
    if (!pending || pending.signature !== currentSignature() || !pending.data) return;
    const inst = pendingKey();
    cfg.scrob = cfg.scrob && typeof cfg.scrob === "object" ? cfg.scrob : {};
    const target = inst === "default"
      ? cfg.scrob
      : ((cfg.scrob.instances ||= {}), (cfg.scrob.instances[inst] ||= {}));
    Object.assign(target, pending.data);
  });

  function boot() {
    wire();
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    else hydrate();
  }

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.scrob = { init: boot, hydrate, currentSignature, clearPending };
  boot();
})();
