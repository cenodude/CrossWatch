// assets/auth/auth.tracearr.js
// CrossWatch - Tracearr auth UI
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
(function () {
  if (window._tracearrPatched) return;
  window._tracearrPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const profile = Shared.createProfileAdapter({
    provider: "tracearr",
    configKey: "tracearr",
    label: "Tracearr",
    sectionId: "sec-tracearr",
    selectId: "tracearr_instance",
    storageKey: "cw.ui.tracearr.auth.instance.v1",
    title: "Select which Tracearr server this config applies to.",
  });

  function getTracearrInstance() {
    return profile ? profile.getInstance() : "default";
  }

  function tracearrApi(path) {
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
      case "server_url_required": return "Enter Tracearr server URL";
      case "api_key_required": return "Enter your Tracearr API key";
      case "invalid_api_key": return "Invalid Tracearr API key";
      case "api_v2_unavailable": return "Tracearr 2.0 or later is required";
      case "validation_timeout": return "Tracearr validation timed out";
      case "validation_failed": return "Could not connect to Tracearr";
      case "validation_bad_response": return "Tracearr validation returned an unexpected response";
      default:
        if (key.startsWith("validation_http_")) return "Tracearr validation failed";
        return key || "Not connected";
    }
  }

  function getTracearrCfgBlock(cfg) {
    return profile ? profile.cfgBlock(cfg, true) : {};
  }

  function ensureTracearrInstanceUI() {
    profile?.ensureUI(() => { void hydrate(); });
  }

  function setConn(ok, msg) {
    try { Shared.setConnectLocked("tracearr_save", !!ok); } catch {}
    return Shared.setStatus("tracearr_msg", ok, msg);
  }

  function maskKey(i, has) {
    return Shared.maskSecret(i, has);
  }

  function syncHint() {
    const k = el("tracearr_key");
    const filled = !!txt(k?.value || "") || k?.dataset.hasKey === "1";
    el("tracearr_hint")?.classList.toggle("hidden", filled);
  }

  function renderUsers(users, selected) {
    const sel = el("tracearr_user_id");
    if (!sel) return;
    const keep = String(selected || "");
    sel.innerHTML = "";
    const all = document.createElement("option");
    all.value = "";
    all.textContent = "All users";
    sel.appendChild(all);
    let found = !keep;
    (Array.isArray(users) ? users : []).forEach((u) => {
      const id = txt(u && u.id);
      if (!id) return;
      const opt = document.createElement("option");
      opt.value = id;
      const servers = Array.isArray(u.servers) && u.servers.length ? ` (${u.servers.join(", ")})` : "";
      opt.textContent = `${txt(u.username) || id}${servers}`;
      sel.appendChild(opt);
      if (id === keep) found = true;
    });
    if (!found) {
      const opt = document.createElement("option");
      opt.value = keep;
      opt.textContent = keep;
      sel.appendChild(opt);
    }
    sel.value = keep;
  }

  async function loadUsers(selected) {
    renderUsers([], selected);
    try {
      const r = await fetchJSON(tracearrApi("/api/tracearr/users"), { cache: "no-store" });
      if (r.ok && r.data && Array.isArray(r.data.users)) renderUsers(r.data.users, selected);
    } catch {}
  }

  async function refresh() {
    try {
      const r = await fetchJSON(tracearrApi("/api/tracearr/status?verify=1"), { cache: "no-store" });
      const ok = !!(r.ok && r.data && r.data.connected);
      setConn(ok, ok ? "Connected" : friendlyError(r.data && r.data.reason));
      note(ok ? "Tracearr connected" : "Tracearr not connected");
      return ok;
    } catch {
      setConn(false, "Could not connect to Tracearr");
      note("Tracearr validation failed");
      return false;
    }
  }

  async function hydrate() {
    ensureTracearrInstanceUI();
    const cfg = window._cfgCache || await getCfg();
    const t = getTracearrCfgBlock(cfg);
    const h = t && typeof t.history === "object" ? t.history : {};

    const server = txt((t && t.server_url) || "");
    const hasKey = !!txt((t && t.api_key) || "");
    const userId = txt((h && h.user_id) || "");

    const serverEl = el("tracearr_server");
    if (serverEl) {
      serverEl.value = server;
      serverEl.dataset.loaded = "1";
      serverEl.dataset.touched = "";
    }

    const userEl = el("tracearr_user_id");
    if (userEl) {
      renderUsers([], userId);
      userEl.dataset.loaded = "1";
      userEl.dataset.touched = "";
    }

    maskKey(el("tracearr_key"), hasKey);
    syncHint();

    const ok = await refresh();
    if (ok) await loadUsers(userId);
  }

  async function onSave() {
    const server = txt(el("tracearr_server")?.value || "");
    const keyInput = el("tracearr_key");
    const key = txt(keyInput?.value || "");
    const userEl = el("tracearr_user_id");
    const user_id = txt(userEl?.value || "");

    if (!server) { note("Enter Tracearr server URL"); return; }

    if (!key && !(keyInput && keyInput.dataset.hasKey === "1")) {
      note("Enter your Tracearr API key");
      return;
    }

    try {
      const r = await fetchJSON(tracearrApi("/api/tracearr/save"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_key: key, user_id }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError(r.data?.error || r.data?.detail || "save_failed"));

      if (key) maskKey(keyInput, true);
      syncHint();
      if (userEl) userEl.dataset.touched = "";
      note("Tracearr saved");
      if (await refresh()) await loadUsers(user_id);
    } catch (e) {
      const msg = e && e.message ? e.message : "Saving Tracearr failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON(tracearrApi("/api/tracearr/disconnect"), { method: "POST" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");

      maskKey(el("tracearr_key"), false);
      syncHint();
      renderUsers([], "");
      setConn(false);
      note("Tracearr disconnected");
    } catch (e) {
      note("Tracearr disconnect failed" + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    const s = el("tracearr_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }

    const d = el("tracearr_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }

    const k = el("tracearr_key");
    if (k && !k.__wiredSecret) {
      Shared.wireSecretInput(k);
      k.addEventListener("input", syncHint);
      k.__wiredSecret = true;
    }

    const server = el("tracearr_server");
    if (server && !server.__wiredTouched) {
      server.addEventListener("input", () => { server.dataset.touched = "1"; });
      server.addEventListener("change", () => { server.dataset.touched = "1"; });
      server.__wiredTouched = true;
    }

    const u = el("tracearr_user_id");
    if (u && !u.__wiredUser) {
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

    const inst = getTracearrInstance();

    const serverEl = el("tracearr_server");
    const server = txt(serverEl?.value || "");
    const serverTouched = !!serverEl?.dataset.touched;
    const keyEl = el("tracearr_key");
    let key = txt(keyEl?.value || "");
    const keyTouched = !!keyEl?.dataset.touched;

    const userEl = el("tracearr_user_id");
    const user_id = txt(userEl?.value || "");
    const uidTouched = !!userEl?.dataset.touched;

    if (keyEl && (keyEl.dataset.masked === "1" || /^[*•]+$/.test(key))) {
      key = "";
    }

    if (!serverTouched && !keyTouched && !uidTouched) return;

    cfg.tracearr = cfg.tracearr || {};
    let t = cfg.tracearr;
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

  window.initTracearrAuthUI = boot;
  boot();
})();
