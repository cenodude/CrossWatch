// assets/auth/auth.publicmetadb.js
(function () {
  if (window._publicMetaDBPatched) return;
  window._publicMetaDBPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const readSecretField = Shared.readSecretField;
  const profile = Shared.createProfileAdapter({
    provider: "publicmetadb",
    configKey: "publicmetadb",
    label: "PublicMetaDB",
    sectionId: "sec-publicmetadb",
    selectId: "publicmetadb_instance",
    storageKey: "cw.ui.publicmetadb.auth.instance.v1",
  });

  function getInstance() {
    return profile ? profile.getInstance() : "default";
  }

  function setInstance(v) {
    if (profile) profile.setInstance(v);
  }

  function api(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  async function getCfg() {
    return Shared.getConfig();
  }

  function cfgBlock(cfg) {
    return profile ? profile.cfgBlock(cfg, true) : {};
  }

  async function refreshInstanceOptions(preserve) {
    if (profile) await profile.refreshOptions(preserve);
  }

  function ensureInstanceUI() {
    profile?.ensureUI(() => { void hydrate(); });
  }

  async function saveKey(key) {
    const r = await fetchJSON(api("/api/publicmetadb/save"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: key })
    });
    if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError((r.data && r.data.error) || "save_failed"));
  }

  function friendlyError(code) {
    switch (String(code || "")) {
      case "api_key_required": return "Enter your PublicMetaDB API key";
      case "invalid_api_key": return "Invalid PublicMetaDB API key";
      case "validation_timeout": return "PublicMetaDB validation timed out";
      case "validation_failed": return "Could not validate PublicMetaDB API key";
      case "validation_bad_response": return "PublicMetaDB validation returned an unexpected response";
      default:
        if (String(code || "").startsWith("validation_http_")) {
          return "PublicMetaDB validation failed";
        }
        return "Saving PublicMetaDB key failed";
    }
  }

  function setConn(ok, msg) {
    return Shared.setStatus("publicmetadb_msg", ok, msg);
  }

  async function refresh() {
    try {
      const r = await fetchJSON(api("/api/publicmetadb/status"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      const reason = String((r.data && r.data.reason) || "");
      setConn(ok, ok || reason === "api_key_required" ? "" : friendlyError(reason));
      note(ok ? "PublicMetaDB connected" : "PublicMetaDB not connected");
    } catch {
      setConn(false, "PublicMetaDB verify failed");
      note("PublicMetaDB verify failed");
    }
  }

  function maskInput(i, has) {
    return Shared.maskSecret(i, has);
  }

  async function hydrate() {
    ensureInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    const blk = cfgBlock(cfg);
    const has = !!txt(blk && blk.api_key);
    maskInput(el("publicmetadb_key"), has);
    el("publicmetadb_hint")?.classList.toggle("hidden", has);
    await refresh();
  }

  async function onSave() {
    const i = el("publicmetadb_key");
    const keyState = readSecretField(i);
    if (!keyState.value) {
      if (keyState.masked || (i && i.dataset.hasKey === "1")) { await refresh(); note("Key unchanged"); return; }
      note("Enter your PublicMetaDB API key"); return;
    }
    try {
      await saveKey(keyState.value);
      maskInput(i, true);
      el("publicmetadb_hint")?.classList.add("hidden");
      note("PublicMetaDB key saved");
      await refresh();
    } catch (e) {
      const msg = e && e.message ? e.message : "Saving PublicMetaDB key failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON(api("/api/publicmetadb/disconnect"), { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error("disconnect_failed");
      maskInput(el("publicmetadb_key"), false);
      el("publicmetadb_hint")?.classList.remove("hidden");
      setConn(false);
      note("PublicMetaDB disconnected");
    } catch {
      note("PublicMetaDB disconnect failed");
    }
  }

  function wire() {
    const s = el("publicmetadb_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }
    const d = el("publicmetadb_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
    const k = el("publicmetadb_key");
    if (k && !k.__wiredSecret) {
      Shared.wireSecretInput(k);
      k.__wiredSecret = true;
    }
  }

  function watch() {
    const host = document.getElementById("auth-providers");
    if (!host || watch._obs) return;
    watch._obs = new MutationObserver(() => { ensureInstanceUI(); wire(); });
    watch._obs.observe(host, { childList: true, subtree: true });
  }

  function boot() {
    ensureInstanceUI();
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
    const keyState = readSecretField(el("publicmetadb_key"));
    if (!keyState.value) return;
    const inst = getInstance();
    cfg.publicmetadb = cfg.publicmetadb || {};
    if (inst === "default") {
      cfg.publicmetadb.api_key = keyState.value;
      return;
    }
    cfg.publicmetadb.instances = cfg.publicmetadb.instances || {};
    cfg.publicmetadb.instances[inst] = cfg.publicmetadb.instances[inst] || {};
    cfg.publicmetadb.instances[inst].api_key = keyState.value;
  });

  window.initPublicMetaDBAuthUI = boot;
  boot();
})();
