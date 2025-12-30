// auth.tautulli.js - Tautulli API-key auth UI
(function () {
  const el = (id) => document.getElementById(id);
  const txt = (v) => String(v ?? "").trim();

  function note(msg, ok = true) {
    const m = el("tautulli_msg");
    if (!m) return;
    m.textContent = msg || "";
    m.classList.remove("hidden");
    m.classList.toggle("warn", !ok);
  }

  function setConn(connected, msg) {
    try { window.setConnBadge?.("TAUTULLI", !!connected); } catch {}
    if (msg) note(msg, !!connected);
  }

  async function refresh() {
    try {
      const s = await window.apiFetchJSON?.("/api/status?fresh=1");
      const p = s?.data?.providers?.TAUTULLI;
      if (!p) return setConn(false, "Tautulli not configured");
      setConn(!!p.connected, p.connected ? "Connected" : (p.reason || "Disconnected"));
    } catch {
      setConn(false, "Tautulli verify failed");
    }
  }

  async function onSave() {
    const server = txt(el("tautulli_server")?.value);
    const key = txt(el("tautulli_key")?.value);
    const user_id = txt(el("tautulli_user_id")?.value);
    if (!server || !key) return note("Server + API key required", false);

    try {
      const r = await window.apiFetchJSON?.("/api/tautulli/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_key: key, user_id }),
      });
      if (!r?.ok) throw new Error("save_failed");
      note("Saved");
      await refresh();
    } catch {
      note("Save failed", false);
    }
  }

  async function onDisc() {
    try {
      const r = await window.apiFetchJSON?.("/api/tautulli/disconnect", { method: "POST" });
      if (!r?.ok) throw new Error("disconnect_failed");
      note("Disconnected");
      await refresh();
    } catch {
      note("Disconnect failed", false);
    }
  }

  function wire() {
    const s = el("tautulli_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }

    const v = el("tautulli_verify");
    if (v && !v.__wired) { v.addEventListener("click", refresh); v.__wired = true; }

    const d = el("tautulli_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }
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
      document.addEventListener("DOMContentLoaded", refresh, { once: true });
    } else {
      refresh();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;
    const server = txt(el("tautulli_server")?.value);
    const key = txt(el("tautulli_key")?.value);
    const user_id = txt(el("tautulli_user_id")?.value);

    if (!server && !key && !user_id) return;
    cfg.tautulli = cfg.tautulli || {};
    if (server) cfg.tautulli.server_url = server;
    if (key) cfg.tautulli.api_key = key;
    cfg.tautulli.history = cfg.tautulli.history || {};
    if (user_id) cfg.tautulli.history.user_id = user_id;
  });

  window.initTautulliAuthUI = boot;
  boot();
})();
