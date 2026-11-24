// assets/auth/auth.floppy.js
// CrossWatch - Floppy auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._floppyAuthPatched) return;
  window._floppyAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const profile = Shared.createProfileAdapter({
    provider: "floppy",
    configKey: "floppy",
    label: "Floppy",
    sectionId: "sec-floppy",
    selectId: "floppy_instance",
    storageKey: "cw.ui.floppy.auth.instance.v1",
    title: T("connections.auth.gen.selectServer", {provider: "Floppy"}),
  });

  const api = (path) => profile ? profile.api(path) : String(path || "");
  const cfgBlock = (cfg) => profile ? profile.cfgBlock(cfg, true) : {};

  function friendlyError(code) {
    const key = String(code || "").trim();
    switch (key) {
      case "server_url_required": return T("connections.validation.serverUrlRequired", {provider: "Floppy"});
      case "api_token_required": return T("connections.validation.apiTokenRequired", {provider: "Floppy"});
      case "invalid_api_token": return T("connections.validation.invalidApiToken", {provider: "Floppy"});
      case "validation_timeout": return T("connections.validation.timeout", {provider: "Floppy"});
      case "unreachable": return T("connections.validation.reachFailed", {provider: "Floppy"});
      case "invalid_ssl": return T("connections.validation.sslFailed", {provider: "Floppy"});
      case "validation_bad_response": return T("connections.validation.returnedUnexpected", {provider: "Floppy"});
      case "server_error": return T("connections.validation.serverError", {provider: "Floppy"});
      default:
        if (key.startsWith("validation_http_")) return T("connections.validation.httpFailed", {provider: "Floppy"});
        return key || T("connections.auth.notConnected");
    }
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked("floppy_connect", !!ok); } catch {}
    return Shared.setStatus("floppy_msg", ok, msg || (ok ? T("connections.auth.connected") : T("connections.auth.notConnected")));
  }

  function ensureUI() {
    profile?.ensureUI(() => { void hydrate(); });
  }

  async function refresh() {
    try {
      const r = await Shared.fetchJSON(api("/api/floppy/status?verify=1"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      setConn(ok, ok ? T("connections.auth.connected") : friendlyError(r.data && r.data.reason));
    } catch {
      setConn(false, T("connections.validation.connectFailed", {provider: "Floppy"}));
    }
  }

  async function hydrate() {
    ensureUI();
    const cfg = window._cfgCache || await Shared.getConfig();
    const f = cfgBlock(cfg);
    const server = txt(f?.server_url || "");
    const hasToken = !!txt(f?.api_token || "");
    const serverEl = el("floppy_server");
    if (serverEl) {
      serverEl.value = server;
      serverEl.dataset.loaded = "1";
      serverEl.dataset.touched = "";
    }
    const verifyEl = el("floppy_verify_ssl");
    if (verifyEl) {
      verifyEl.checked = f?.verify_ssl === true;
      verifyEl.dataset.loaded = "1";
      verifyEl.dataset.touched = "";
    }
    Shared.maskSecret(el("floppy_token"), hasToken);
    await refresh();
  }

  async function onSave() {
    const server = txt(el("floppy_server")?.value || "");
    const tokenInput = el("floppy_token");
    const token = txt(tokenInput?.value || "");
    const verify_ssl = !!el("floppy_verify_ssl")?.checked;
    if (!server) { note(T("connections.validation.serverUrlRequired", {provider: "Floppy"})); return; }
    if (!token && !(tokenInput && tokenInput.dataset.hasKey === "1")) {
      note(T("connections.validation.apiTokenRequired", {provider: "Floppy"}));
      return;
    }
    try {
      const r = await Shared.fetchJSON(api("/api/floppy/save"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_token: token, verify_ssl }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError(r.data?.error || "save_failed"));
      if (token) Shared.maskSecret(tokenInput, true);
      note(T("connections.auth.gen.saved", {provider: "Floppy"}));
      await refresh();
    } catch (e) {
      const msg = e && e.message ? e.message : T("connections.validation.saveFailed", {provider: "Floppy"});
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisconnect() {
    try {
      const r = await Shared.fetchJSON(api("/api/floppy/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");
      if (el("floppy_server")) el("floppy_server").value = "";
      Shared.maskSecret(el("floppy_token"), false);
      setConn(false);
      note(T("connections.auth.gen.disconnected", {provider: "Floppy"}));
    } catch (e) {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Floppy"}) + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    const save = el("floppy_connect");
    if (save && !save.__wired) { save.addEventListener("click", onSave); save.__wired = true; }
    const disconnect = el("floppy_disconnect");
    if (disconnect && !disconnect.__wired) { disconnect.addEventListener("click", onDisconnect); disconnect.__wired = true; }
    const token = el("floppy_token");
    if (token && !token.__wiredSecret) {
      Shared.wireSecretInput(token);
      token.__wiredSecret = true;
    }
    const server = el("floppy_server");
    if (server && !server.__wiredTouched) {
      server.addEventListener("input", () => { server.dataset.touched = "1"; });
      server.addEventListener("change", () => { server.dataset.touched = "1"; });
      server.__wiredTouched = true;
    }
    const verify = el("floppy_verify_ssl");
    if (verify && !verify.__wiredTouched) {
      verify.addEventListener("input", () => { verify.dataset.touched = "1"; });
      verify.addEventListener("change", () => { verify.dataset.touched = "1"; });
      verify.__wiredTouched = true;
    }
  }

  function boot() {
    wire();
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    else hydrate();
  }

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.floppy = { init: boot, hydrate };
  boot();
})();
