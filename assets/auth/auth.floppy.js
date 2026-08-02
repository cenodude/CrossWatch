// assets/auth/auth.floppy.js
// CrossWatch - Floppy auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
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
    title: "Select which Floppy server this config applies to.",
  });

  const api = (path) => profile ? profile.api(path) : String(path || "");
  const cfgBlock = (cfg) => profile ? profile.cfgBlock(cfg, true) : {};

  function friendlyError(code) {
    const key = String(code || "").trim();
    switch (key) {
      case "server_url_required": return "Enter Floppy server URL";
      case "api_token_required": return "Enter your Floppy API token";
      case "invalid_api_token": return "Invalid Floppy API token";
      case "validation_timeout": return "Floppy validation timed out";
      case "unreachable": return "Could not reach Floppy";
      case "invalid_ssl": return "Floppy SSL validation failed";
      case "validation_bad_response": return "Floppy returned an unexpected response";
      case "server_error": return "Floppy server error";
      default:
        if (key.startsWith("validation_http_")) return "Floppy validation failed";
        return key || "Not connected";
    }
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked("floppy_connect", !!ok); } catch {}
    return Shared.setStatus("floppy_msg", ok, msg || (ok ? "Connected" : "Not connected"));
  }

  function ensureUI() {
    profile?.ensureUI(() => { void hydrate(); });
  }

  async function refresh() {
    try {
      const r = await Shared.fetchJSON(api("/api/floppy/status?verify=1"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      setConn(ok, ok ? "Connected" : friendlyError(r.data && r.data.reason));
    } catch {
      setConn(false, "Could not connect to Floppy");
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
    if (!server) { note("Enter Floppy server URL"); return; }
    if (!token && !(tokenInput && tokenInput.dataset.hasKey === "1")) {
      note("Enter your Floppy API token");
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
      note("Floppy saved");
      await refresh();
    } catch (e) {
      const msg = e && e.message ? e.message : "Saving Floppy failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisconnect() {
    try {
      const r = await Shared.fetchJSON(api("/api/floppy/disconnect"), { method: "POST", cache: "no-store" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");
      if (el("floppy_server")) el("floppy_server").value = "";
      Shared.maskSecret(el("floppy_token"), false);
      setConn(false);
      note("Floppy disconnected");
    } catch (e) {
      note("Floppy disconnect failed" + (e && e.message ? ": " + e.message : ""));
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
