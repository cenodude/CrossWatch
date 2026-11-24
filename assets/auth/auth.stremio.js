// assets/auth/auth.stremio.js
// CrossWatch - Stremio Auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
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
    return Shared.setStatus("stremio_msg", ok, msg || (ok ? T("connections.auth.connected") : T("connections.auth.notConnected")));
  }

  function friendlyError(data) {
    const reason = txt(data?.reason || data?.error);
    switch (reason) {
      case "missing_credentials": return T("connections.auth.gen.enterEmailPassword", {provider: "Stremio"});
      case "invalid_credentials": return T("connections.auth.gen.rejectedCreds", {provider: "Stremio"});
      case "unreachable": return T("connections.auth.gen.apiUnreachable", {provider: "Stremio"});
      case "service_unavailable": return T("connections.auth.gen.apiUnavailable", {provider: "Stremio"});
      case "invalid_response": return T("connections.validation.returnedUnexpected", {provider: "Stremio"});
      default: return txt(data?.error) || T("connections.auth.gen.connectionFailed", {provider: "Stremio"});
    }
  }

  function addonAge(seconds) {
    const s = Math.max(0, Number(seconds) || 0);
    if (s < 90) return "just now";
    if (s < 5400) return Math.round(s / 60) + " min ago";
    if (s < 129600) return Math.round(s / 3600) + " h ago";
    return Math.round(s / 86400) + " d ago";
  }

  function renderAddon(data) {
    const on = !!data?.enabled;
    if (el("stremio_addon_off")) el("stremio_addon_off").hidden = on;
    if (el("stremio_addon_on")) el("stremio_addon_on").hidden = !on;
    if (!on) return;
    if (el("stremio_addon_url")) el("stremio_addon_url").value = txt(data.manifest_url);
    const install = el("stremio_addon_install");
    if (install) {
      install.href = txt(data.install_url) || "#";
      install.hidden = !data.installable || !data.install_url;
    }
    const web = el("stremio_addon_web");
    if (web) {
      web.href = txt(data.web_install_url) || "#";
      web.hidden = !data.installable || !data.web_install_url;
    }
    if (el("stremio_addon_warn")) el("stremio_addon_warn").hidden = !!data.installable;
    const info = el("stremio_addon_info");
    if (!info) return;
    const parts = [];
    if (!data.installed) parts.push(T("connections.auth.stremio.notInstalled"));
    else if (data.last_event) parts.push(T("connections.auth.stremio.lastEvent", {age: addonAge(data.age_seconds)}));
    else parts.push(T("connections.auth.stremio.installed"));
    if (!data.watcher_enabled) parts.push(T("connections.auth.stremio.watcherOff"));
    else if (!data.routes) parts.push(T("connections.auth.stremio.addRoute"));
    info.textContent = parts.join(" ");
  }

  async function loadAddon() {
    try {
      const r = await Shared.fetchJSON(api("/api/stremio/addon"), { cache: "no-store" });
      renderAddon(r.ok ? (r.data || {}) : {});
    } catch {
      renderAddon({});
    }
  }

  async function updateAddon(body, done) {
    try {
      const r = await Shared.fetchJSON(api("/api/stremio/addon"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        cache: "no-store",
      });
      if (!r.ok || r.data?.ok === false) throw new Error(txt(r.data?.message || r.data?.error) || T("connections.auth.requestFailedShort"));
      renderAddon(r.data || {});
      if (done) note(done);
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      note(T("connections.auth.stremio.addonError", {error: e && e.message ? e.message : T("connections.auth.stremio.requestFailedLower")}));
    }
  }

  function wireAddon() {
    const on = (id, fn) => {
      const node = el(id);
      if (node && !node.__wired) { node.addEventListener("click", fn); node.__wired = true; }
    };
    on("stremio_addon_enable", () => { void updateAddon({ enabled: true }, T("connections.auth.stremio.on")); });
    on("stremio_addon_copy", () => {
      void Shared.copyText(el("stremio_addon_url")?.value || "", el("stremio_addon_copy"), { copiedText: T("connections.auth.copied"), emptyMessage: T("connections.auth.stremio.noUrl") });
    });
    on("stremio_addon_regen", () => {
      if (window.confirm(T("connections.auth.stremio.confirmNew"))) void updateAddon({ enabled: true, regenerate: true }, T("connections.auth.stremio.newUrl"));
    });
    on("stremio_addon_disable", () => {
      if (window.confirm(T("connections.auth.stremio.confirmOff"))) void updateAddon({ enabled: false }, T("connections.auth.stremio.off"));
    });
    on("stremio_addon_install", (ev) => {
      if ((el("stremio_addon_install")?.getAttribute("href") || "#") === "#") ev.preventDefault();
    });
  }

  async function hydrate() {
    profile.ensureUI(() => { void hydrate(); });
    wireAddon();
    void loadAddon();
    try {
      const r = await Shared.fetchJSON(api("/api/stremio/status?verify=1"), { cache: "no-store" });
      const data = r.data || {};
      const ok = !!(r.ok && data.connected);
      if (el("stremio_email")) el("stremio_email").value = "";
      if (el("stremio_password")) el("stremio_password").value = "";
      setConn(ok, ok ? T("connections.auth.gen.connected", {provider: "Stremio"}) : T("connections.auth.notConnected"));
    } catch {
      setConn(false, T("connections.auth.notConnected"));
    }
  }

  async function onConnect() {
    const email = txt(el("stremio_email")?.value || "");
    const password = String(el("stremio_password")?.value || "");
    if (!email || !password) {
      note(T("connections.auth.gen.enterEmailPassword", {provider: "Stremio"}));
      return;
    }
    try {
      setConn(false, T("connections.auth.connecting"));
      const r = await Shared.fetchJSON(api("/api/stremio/connect"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
        cache: "no-store",
      });
      if (!r.ok || r.data?.ok === false) throw new Error(friendlyError(r.data || {}));
      if (el("stremio_password")) el("stremio_password").value = "";
      setConn(true, T("connections.auth.gen.connected", {provider: "Stremio"}));
      note(T("connections.auth.gen.connected", {provider: "Stremio"}));
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      const msg = e && e.message ? e.message : T("connections.auth.gen.connectionFailed", {provider: "Stremio"});
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
      setConn(false, T("connections.auth.notConnected"));
      note(T("connections.auth.gen.disconnected", {provider: "Stremio"}));
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Stremio"}) + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    profile.ensureUI(() => { void hydrate(); });
    const c = el("stremio_connect");
    if (c && !c.__wired) { c.addEventListener("click", onConnect); c.__wired = true; }
    const d = el("stremio_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisconnect); d.__wired = true; }
    wireAddon();
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
