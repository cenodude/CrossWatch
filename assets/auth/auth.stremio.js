// assets/auth/auth.stremio.js
// CrossWatch - Stremio Auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  if (window._stremioAuthPatched) return;
  window._stremioAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const profile = Shared.createProfileAdapter({
    provider: "stremio",
    configKey: "stremio",
    label: "Stremio",
    sectionId: "sec-stremio",
    selectId: "stremio_instance",
    storageKey: "cw.ui.stremio.auth.instance.v1",
  });

  let connected = false;

  function api(path) {
    return profile.api(path);
  }

  function syncConnectLocked() {
    try { Shared.setConnectLocked("stremio_connect", connected); } catch {}
  }

  function setConn(ok, msg) {
    connected = !!ok;
    syncConnectLocked();
    return Shared.setStatus("stremio_msg", ok, msg || (ok ? "Connected" : "Not connected"));
  }

  function friendlyError(data) {
    const reason = txt(data?.reason || data?.error);
    switch (reason) {
      case "missing_credentials": return "Enter your Stremio email and password";
      case "invalid_credentials": return "Stremio rejected the credentials";
      case "unreachable": return "Stremio API is unreachable";
      case "service_unavailable": return "Stremio API is unavailable";
      case "invalid_response": return "Stremio returned an unexpected response";
      default: return txt(data?.error) || "Stremio connection failed";
    }
  }

  async function hydrate() {
    profile.ensureUI(() => { void hydrate(); });
    try {
      const r = await Shared.fetchJSON(api("/api/stremio/status?verify=1"), { cache: "no-store" });
      const data = r.data || {};
      const ok = !!(r.ok && data.connected);
      if (el("stremio_email")) el("stremio_email").value = "";
      if (el("stremio_password")) el("stremio_password").value = "";
      setConn(ok, ok ? "Stremio connected" : "Not connected");
    } catch {
      setConn(false, "Not connected");
    }
  }

  async function onConnect() {
    const email = txt(el("stremio_email")?.value || "");
    const password = String(el("stremio_password")?.value || "");
    if (!email || !password) {
      note("Enter your Stremio email and password");
      return;
    }
    try {
      setConn(false, "Connecting...");
      const r = await Shared.fetchJSON(api("/api/stremio/connect"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
        cache: "no-store",
      });
      if (!r.ok || r.data?.ok === false) throw new Error(friendlyError(r.data || {}));
      if (el("stremio_password")) el("stremio_password").value = "";
      setConn(true, "Stremio connected");
      note("Stremio connected");
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      const msg = e && e.message ? e.message : "Stremio connection failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisconnect() {
    try {
      const r = await Shared.fetchJSON(api("/api/stremio/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || r.data?.ok === false) throw new Error(r.data?.error || "disconnect_failed");
      if (el("stremio_email")) el("stremio_email").value = "";
      if (el("stremio_password")) el("stremio_password").value = "";
      setConn(false, "Not connected");
      note("Stremio disconnected");
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      note("Stremio disconnect failed" + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    profile.ensureUI(() => { void hydrate(); });
    const c = el("stremio_connect");
    if (c && !c.__wired) { c.addEventListener("click", onConnect); c.__wired = true; }
    const d = el("stremio_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisconnect); d.__wired = true; }
  }

  function boot() {
    wire();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;
    profile.cfgBlock(cfg, true);
  });

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.stremio = { init: boot, hydrate };
  boot();
})();
