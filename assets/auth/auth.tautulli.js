// assets/auth/auth.tautulli.js
(function () {
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
    title: "Select which Tautulli server this config applies to.",
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
      case "server_url_required": return "Enter Tautulli server URL";
      case "api_key_required": return "Enter your Tautulli API key";
      case "invalid_api_key": return "Invalid Tautulli API key";
      case "validation_timeout": return "Tautulli validation timed out";
      case "validation_failed": return "Could not connect to Tautulli";
      case "validation_bad_response": return "Tautulli validation returned an unexpected response";
      default:
        if (key.startsWith("validation_http_")) return "Tautulli validation failed";
        return key || "Not connected";
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
    return Shared.setStatus("tautulli_msg", ok, msg);
  }

  function maskKey(i, has) {
    return Shared.maskSecret(i, has);
  }

  async function refresh() {
    try {
      const r = await fetchJSON(tautApi("/api/tautulli/status?verify=1"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      setConn(ok, ok ? "Connected" : friendlyError(r.data && r.data.reason));
      note(ok ? "Tautulli connected" : "Tautulli not connected");
    } catch {
      setConn(false, "Could not connect to Tautulli");
      note("Tautulli validation failed");
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

    if (el("tautulli_server")) el("tautulli_server").value = server;
    if (el("tautulli_user_id")) el("tautulli_user_id").value = userId;

    maskKey(el("tautulli_key"), hasKey);
    el("tautulli_hint")?.classList.toggle("hidden", hasKey);

    await refresh();
  }

  async function onSave() {
    const server = txt(el("tautulli_server")?.value || "");
    const keyInput = el("tautulli_key");
    const key = txt(keyInput?.value || "");
    const user_id = txt(el("tautulli_user_id")?.value || "");

    if (!server) { note("Enter Tautulli server URL"); return; }

    if (!key && !(keyInput && keyInput.dataset.hasKey === "1")) {
      note("Enter your Tautulli API key");
      return;
    }

    try {
      const r = await fetchJSON(tautApi("/api/tautulli/save"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_key: key, user_id }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError(r.data?.error || "save_failed"));

      if (key) maskKey(keyInput, true);
      el("tautulli_hint")?.classList.add("hidden");
      note("Tautulli saved");
      await refresh();
    } catch (e) {
      const msg = e && e.message ? e.message : "Saving Tautulli failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON(tautApi("/api/tautulli/disconnect"), { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");

      maskKey(el("tautulli_key"), false);
      el("tautulli_hint")?.classList.remove("hidden");
      setConn(false);
      note("Tautulli disconnected");
    } catch (e) {
      note("Tautulli disconnect failed" + (e && e.message ? ": " + e.message : ""));
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

    const u = el("tautulli_user_id");
    if (u && !u.__wiredUser) {
      u.addEventListener("input", () => { u.dataset.touched = "1"; });
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

    const server = txt(el("tautulli_server")?.value || "");
    const keyEl = el("tautulli_key");
    let key = txt(keyEl?.value || "");

    const userIdEl = el("tautulli_user_id");
    const user_id = txt(userIdEl?.value || "");
    const uidTouched = !!userIdEl?.dataset.touched;

    if (keyEl && (keyEl.dataset.masked === "1" || key === "********" || key === "********" || key === "**********")) {
      key = "";
    }

    if (!server && !key && !user_id && !uidTouched) return;

    cfg.tautulli = cfg.tautulli || {};
    let t = cfg.tautulli;
    if (inst !== "default") {
      t.instances = t.instances || {};
      t.instances[inst] = t.instances[inst] || {};
      t = t.instances[inst];
    }

    if (server) t.server_url = server;
    if (key) t.api_key = key;

    if (user_id || uidTouched) {
      t.history = t.history || {};
      t.history.user_id = user_id;
    }
  });

  window.initTautulliAuthUI = boot;
  boot();
})();
