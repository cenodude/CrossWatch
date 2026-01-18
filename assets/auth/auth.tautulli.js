// assets/auth/auth.tautulli.js
(function () {
  if (window._tautPatched) return;
  window._tautPatched = true;

  const el = (id) => document.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : "").trim();
  const note = (m) => (typeof window.notify === "function" ? window.notify(m) : void 0);

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts || {});
    let j = null; try { j = await r.json(); } catch {}
    return { ok: r.ok, data: j, status: r.status };
  }

  async function getCfg() {
    const r = await fetchJSON("/api/config", { cache: "no-store" });
    return r.ok ? (r.data || {}) : {};
  }
  function setConn(ok, msg) {
    const m = el("tautulli_msg");
    if (!m) return;
    const text = ok ? (msg || "Connected.") : (msg || "Not connected");
    m.textContent = text;
    m.classList.remove("hidden", "ok", "warn");
    m.classList.add(ok ? "ok" : "warn");
  }

  function maskKey(i, has) {
    if (!i) return;
    if (has) { i.value = "••••••••"; i.dataset.masked = "1"; }
    else { i.value = ""; i.dataset.masked = "0"; }
    i.dataset.hasKey = has ? "1" : "";
  }

  async function refresh() {
    try {
      await fetch("/api/debug/clear_probe_cache", { method: "POST" }).catch(() => {});
      const r = await fetchJSON("/api/status?fresh=1", { cache: "no-store" });
      const ok = !!(r.ok && r.data?.providers?.TAUTULLI?.connected);
      setConn(ok, ok ? "Connected" : (r.data?.providers?.TAUTULLI?.reason || "Not connected"));
      note(ok ? "Tautulli verified ✓" : "Tautulli not connected");
    } catch {
      setConn(false, "Verify failed");
      note("Tautulli verify failed");
    }
  }

  async function hydrate() {
    const cfg = window._cfgCache || await getCfg();
    const t = cfg?.tautulli || {};
    const h = t?.history || {};

    const server = txt(t.server_url || "");
    const hasKey = !!txt(t.api_key || "");
    const userId = txt(h.user_id || "");

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
      const r = await fetchJSON("/api/tautulli/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ server_url: server, api_key: key, user_id }),
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "save_failed");

      if (key) maskKey(keyInput, true);

      el("tautulli_hint")?.classList.add("hidden");
      note("Tautulli saved");
      await refresh();
    } catch (e) {
      note(`Saving Tautulli failed${e?.message ? `: ${e.message}` : ""}`);
    }
  }

  async function onDisc() {
    try {
      const r = await fetchJSON("/api/tautulli/disconnect", { method: "POST" });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");

      maskKey(el("tautulli_key"), false);
      el("tautulli_hint")?.classList.remove("hidden");
      setConn(false);
      note("Tautulli disconnected");
    } catch (e) {
      note(`Tautulli disconnect failed${e?.message ? `: ${e.message}` : ""}`);
    }
  }

  function wire() {
    const s = el("tautulli_save");
    if (s && !s.__wired) { s.addEventListener("click", onSave); s.__wired = true; }

    const v = el("tautulli_verify");
    if (v && !v.__wired) { v.addEventListener("click", refresh); v.__wired = true; }

    const d = el("tautulli_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisc); d.__wired = true; }

    const k = el("tautulli_key");
    if (k && !k.__wiredSecret) {
      k.addEventListener("focus", () => {
        if (k.dataset.masked === "1") {
          k.value = "";
          k.dataset.masked = "0";
          k.dataset.touched = "1";
        }
      });
      k.addEventListener("input", () => {
        k.dataset.masked = "0";
        k.dataset.touched = "1";
        k.dataset.hasKey = "";
      });
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

    const server = txt(el("tautulli_server")?.value || "");
    const keyEl = el("tautulli_key");
    let key = txt(keyEl?.value || "");

    const userIdEl = el("tautulli_user_id");
    const user_id = txt(userIdEl?.value || "");
    const uidTouched = !!userIdEl?.dataset.touched;

    if (keyEl && (keyEl.dataset.masked === "1" || key === "••••••••" || key === "********" || key === "**********")) {
      key = "";
    }

    if (!server && !key && !user_id && !uidTouched) return;

    cfg.tautulli = cfg.tautulli || {};
    if (server) cfg.tautulli.server_url = server;
    if (key) cfg.tautulli.api_key = key;

    if (user_id || uidTouched) {
      cfg.tautulli.history = cfg.tautulli.history || {};
      cfg.tautulli.history.user_id = user_id;
    }
  });

  window.initTautulliAuthUI = boot;
  boot();
})();
