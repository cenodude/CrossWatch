// assets/auth/auth.tautulli.js
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (window._tautPatched) return;
  window._tautPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const profile = Shared.createProfileAdapter({
    provider: "tautulli",
    configKey: "tautulli",
    label: "Tautulli",
    sectionId: "sec-tautulli",
    selectId: "tautulli_instance",
    storageKey: "cw.ui.tautulli.auth.instance.v1",
    title: T("connections.auth.gen.selectServer", {provider: "Tautulli"}),
  });

  function getTautulliInstance() {
    return profile ? profile.getInstance() : "default";
  }

  function setTautulliInstance(v) {
    if (profile) profile.setInstance(v);
  }

  function tautApi(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  async function getCfg() {
    return Shared.getConfig();
  }

  function friendlyError(code) {
    const key = String(code || "").trim();
    switch (key) {
      case "server_url_required": return T("connections.validation.serverUrlRequired", {provider: "Tautulli"});
      case "server_url required": return T("connections.validation.serverUrlRequired", {provider: "Tautulli"});
      case "api_key_required": return T("connections.validation.apiKeyRequired", {provider: "Tautulli"});
      case "api_key required": return T("connections.validation.apiKeyRequired", {provider: "Tautulli"});
      case "invalid_api_key": return T("connections.validation.invalidApiKey", {provider: "Tautulli"});
      case "validation_timeout": return T("connections.validation.timeout", {provider: "Tautulli"});
      case "validation_failed": return T("connections.validation.connectFailed", {provider: "Tautulli"});
      case "validation_bad_response": return T("connections.validation.badResponse", {provider: "Tautulli"});
      default:
        if (key.startsWith("validation_http_")) return T("connections.validation.httpFailed", {provider: "Tautulli"});
        return key || T("connections.auth.notConnected");
    }
  }

  function getTautulliCfgBlock(cfg) {
    return profile ? profile.cfgBlock(cfg, true) : {};
  }

  async function refreshTautulliInstanceOptions(preserve) {
    if (profile) await profile.refreshOptions(preserve);
  }

  function ensureTautulliInstanceUI() {
    profile?.ensureUI(() => { void hydrate(); });
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked("tautulli_save", !!ok); } catch {}
    return Shared.setStatus("tautulli_msg", ok, msg);
  }

  function maskKey(i, has) {
    return Shared.maskSecret(i, has);
  }

  async function refresh() {
    try {
      const r = await fetchJSON(tautApi("/api/tautulli/status?verify=1"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      setConn(ok, ok ? T("connections.auth.connected") : friendlyError(r.data && r.data.reason));
      note(ok ? T("connections.auth.gen.connected", {provider: "Tautulli"}) : T("connections.auth.gen.notConnected", {provider: "Tautulli"}));
    } catch {
      setConn(false, T("connections.validation.connectFailed", {provider: "Tautulli"}));
      note(T("connections.validation.httpFailed", {provider: "Tautulli"}));
    }
  }

  async function hydrate() {
    ensureTautulliInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    const t = getTautulliCfgBlock(cfg);
    const h = t && typeof t.history === "object" ? t.history : {};

    const server = txt((t && t.server_url) || "");
    const hasKey = !!txt((t && t.api_key) || "");
    const userId = txt((h && h.user_id) || "");

    const serverEl = el("tautulli_server");
    if (serverEl) {
      serverEl.value = server;
      serverEl.dataset.loaded = "1";
      serverEl.dataset.touched = "";
    }

    const userIdEl = el("tautulli_user_id");
    if (userIdEl) {
      userIdEl.value = userId;
      userIdEl.dataset.loaded = "1";
      userIdEl.dataset.touched = "";
    }

    maskKey(el("tautulli_key"), hasKey);
    el("tautulli_hint")?.classList.toggle("hidden", hasKey);

    await refresh();
  }

  async function onSave() {
    const server = txt(el("tautulli_server")?.value || "");
    const keyInput = el("tautulli_key");
    const key = txt(keyInput?.value || "");
    const user_id = txt(el("tautulli_user_id")?.value || "");

    if (!server) { note(T("connections.validation.serverUrlRequired", {provider: "Tautulli"})); return; }

    if (!key && !(keyInput && keyInput.dataset.hasKey === "1")) {
      note(T("connections.validation.apiKeyRequired", {provider: "Tautulli"}));
      return;
    }

    try {
      const r = await fetchJSON(tautApi("/api/tautulli/save"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_key: key, user_id }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError(r.data?.error || r.data?.detail || "save_failed"));

      if (key) maskKey(keyInput, true);
      el("tautulli_hint")?.classList.add("hidden");
      note(T("connections.auth.gen.saved", {provider: "Tautulli"}));
      await refresh();
    } catch (e) {
      const msg = e && e.message ? e.message : T("connections.validation.saveFailed", {provider: "Tautulli"});
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON(tautApi("/api/tautulli/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");

      maskKey(el("tautulli_key"), false);
      el("tautulli_hint")?.classList.remove("hidden");
      setConn(false);
      note(T("connections.auth.gen.disconnected", {provider: "Tautulli"}));
    } catch (e) {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Tautulli"}) + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    const s = el("tautulli_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }

    const d = el("tautulli_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }

    const k = el("tautulli_key");
    if (k && !k.__wiredSecret) {
      Shared.wireSecretInput(k);
      k.__wiredSecret = true;
    }

    const server = el("tautulli_server");
    if (server && !server.__wiredTouched) {
      server.addEventListener("input", () => { server.dataset.touched = "1"; });
      server.addEventListener("change", () => { server.dataset.touched = "1"; });
      server.__wiredTouched = true;
    }

    const u = el("tautulli_user_id");
    if (u && !u.__wiredUser) {
      u.addEventListener("input", () => { u.dataset.touched = "1"; });
      u.addEventListener("change", () => { u.dataset.touched = "1"; });
      u.__wiredUser = true;
    }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => wire());
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    wire();
    watch();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;

    const inst = getTautulliInstance();

    const serverEl = el("tautulli_server");
    const server = txt(serverEl?.value || "");
    const serverTouched = !!serverEl?.dataset.touched;
    const keyEl = el("tautulli_key");
    let key = txt(keyEl?.value || "");
    const keyTouched = !!keyEl?.dataset.touched;

    const userIdEl = el("tautulli_user_id");
    const user_id = txt(userIdEl?.value || "");
    const uidTouched = !!userIdEl?.dataset.touched;

    if (keyEl && (keyEl.dataset.masked === "1" || key === "********" || key === "********" || key === "**********")) {
      key = "";
    }

    if (!serverTouched && !keyTouched && !uidTouched) return;

    cfg.tautulli = cfg.tautulli || {};
    let t = cfg.tautulli;
    if (inst !== "default") {
      t.instances = t.instances || {};
      t.instances[inst] = t.instances[inst] || {};
      t = t.instances[inst];
    }

    if (serverTouched && server) t.server_url = server;
    if (keyTouched && key) t.api_key = key;

    if (uidTouched) {
      t.history = t.history || {};
      t.history.user_id = user_id;
    }
  });

  window.initTautulliAuthUI = boot;
  boot();
})();
