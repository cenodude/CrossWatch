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
    if (!data.installed) parts.push("Not installed in Stremio yet. If the buttons do not open the install dialog, copy the URL and paste it into Add addon in Stremio.");
    else if (data.last_event) parts.push("Last playback event " + addonAge(data.age_seconds) + ".");
    else parts.push("Installed in Stremio, no playback reported yet.");
    if (!data.watcher_enabled) parts.push("The Watcher is off, so events are ignored.");
    else if (!data.routes) parts.push("Add a Watcher route with Stremio as the source to start scrobbling.");
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
      if (!r.ok || r.data?.ok === false) throw new Error(txt(r.data?.message || r.data?.error) || "Request failed");
      renderAddon(r.data || {});
      if (done) note(done);
      try { window.dispatchEvent(new CustomEvent("auth-changed")); } catch {}
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      note("Stremio add-on: " + (e && e.message ? e.message : "request failed"));
    }
  }

  function wireAddon() {
    const on = (id, fn) => {
      const node = el(id);
      if (node && !node.__wired) { node.addEventListener("click", fn); node.__wired = true; }
    };
    on("stremio_addon_enable", () => { void updateAddon({ enabled: true }, "Stremio scrobble add-on turned on"); });
    on("stremio_addon_copy", () => {
      void Shared.copyText(el("stremio_addon_url")?.value || "", el("stremio_addon_copy"), { copiedText: "Copied", emptyMessage: "No add-on URL yet." });
    });
    on("stremio_addon_regen", () => {
      if (window.confirm("Create a new add-on URL? The installed add-on stops working until you install it again.")) void updateAddon({ enabled: true, regenerate: true }, "New Stremio add-on URL created");
    });
    on("stremio_addon_disable", () => {
      if (window.confirm("Turn off the Stremio scrobble add-on? You can then remove it in Stremio.")) void updateAddon({ enabled: false }, "Stremio scrobble add-on turned off");
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
