// auth.simkl.js
(function (w, d) {
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;
  const computeRedirect = () =>
    (typeof w.computeRedirectURI === "function"
      ? w.computeRedirectURI()
      : (location.origin + "/callback"));

  function setSimklBanner(kind, text) {
    const el = $("simkl_msg");
    if (!el) return;
    el.classList.remove("hidden", "ok", "warn");
    if (!kind) { el.classList.add("hidden"); el.textContent = ""; return; }
    el.classList.add(kind);
    el.textContent = text || "";
  }

  function setSimklSuccess(on, text) {
    if (on) setSimklBanner("ok", text || "Connected.");
    else setSimklBanner(null, "");
  }

  // Keep SIMKL hint banner intact: only toggle visibility, do NOT replace its HTML/text
  function updateSimklButtonState() {
    try {
      const cid = ($("simkl_client_id")?.value || "").trim();
      const sec = ($("simkl_client_secret")?.value || "").trim();
      const btn = $("btn-connect-simkl") || $("simkl_start_btn") || $("btn_simkl_connect");
      const hint = $("simkl_hint");
      const rid = $("redirect_uri_preview");

      if (rid) rid.textContent = computeRedirect();

      const ok = cid.length > 0 && sec.length > 0;
      if (btn) btn.disabled = !ok;
      if (hint) hint.classList.toggle("hidden", ok);
    } catch (e) {
      console.warn("updateSimklButtonState failed", e);
    }
  }
  const updateSimklHint = updateSimklButtonState;

  async function copyRedirect() {
    try {
      await navigator.clipboard.writeText(computeRedirect());
      notify("Redirect URI copied ✓");
    } catch {
      notify("Copy failed");
    }
  }

  async function simklDeleteToken() {
    const btn = q("#sec-simkl .btn.danger");
    const msg = $("simkl_msg");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    if (msg) { msg.classList.remove("hidden"); msg.classList.remove("warn"); msg.textContent = ""; }
    try {
      const r = await fetch("/api/simkl/token/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        cache: "no-store",
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok && (j.ok !== false)) {
        try { const el = $("simkl_access_token"); if (el) el.value = ""; } catch {}
        try { setSimklSuccess(false); } catch {}
        notify("SIMKL access token removed.");
      } else {
        if (msg) { msg.classList.add("warn"); msg.textContent = "Could not remove token."; }
      }
    } catch {
      if (msg) { msg.classList.add("warn"); msg.textContent = "Error removing token."; }
    } finally {
      if (btn) { btn.disabled = false; btn.classList.remove("busy"); }
    }
  }

  let simklPoll = null;
  let simklVisHandler = null;

  async function startSimkl() {
    try { setSimklSuccess(false); } catch {}

    const cid = ($("simkl_client_id")?.value || "").trim();
    const sec = ($("simkl_client_secret")?.value || "").trim();
    if (!cid || !sec) {
      notify("Fill in SIMKL Client ID + Client Secret first");
      updateSimklButtonState();
      return;
    }

    let win = null;
    try { win = w.open("https://simkl.com/", "_blank"); } catch {}

    try { await w.saveSettings?.(); } catch {}

    const origin = location.origin;
    const j = await fetch("/api/simkl/authorize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ origin }),
      cache: "no-store",
    }).then((r) => r.json()).catch(() => null);

    if (!j?.ok || !j.authorize_url) {
      const err = (j && (j.error || j.message)) ? String(j.error || j.message) : "SIMKL authorize failed";
      notify(err);
      try { if (win && !win.closed) win.close(); } catch {}
      return;
    }

    if (win && !win.closed) {
      try { win.location.href = j.authorize_url; win.focus(); } catch {}
    } else {
      notify("Popup blocked - allow popups and try again");
    }

    const cleanup = () => {
      if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }
      if (simklVisHandler) { d.removeEventListener("visibilitychange", simklVisHandler); simklVisHandler = null; }
    };
    cleanup();

    const startTs = Date.now();
    const deadline = startTs + 120000;
    const fastUntil = startTs + 30000; // 2s polling for ~30s
    const back = [5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;
    let inFlight = false;

    const poll = async () => {
      if (Date.now() >= deadline) { cleanup(); return; }

      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { simklPoll = setTimeout(poll, 5000); return; }
      if (inFlight) return;

      inFlight = true;
      let cfg = null;
      try {
        const r = await fetch("/api/config" + bust(), { cache: "no-store", credentials: "same-origin" });
        if (r.status === 401) { notify("Session expired - please log in again"); cleanup(); return; }
        cfg = await r.json();
      } catch {} finally { inFlight = false; }

      const tok = (cfg?.simkl?.access_token || cfg?.auth?.simkl?.access_token || "").trim();
      if (tok) {
        try { const el = $("simkl_access_token"); if (el) el.value = tok; } catch {}
        try { setSimklSuccess(true); } catch {}
        cleanup();
        return;
      }

      const delay = (Date.now() < fastUntil) ? 2000 : back[Math.min(i++, back.length - 1)];
      simklPoll = setTimeout(poll, delay);
    };

    simklVisHandler = () => {
      if (d.hidden) return;
      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (!settingsVisible) return;
      if (!simklPoll) return;
      clearTimeout(simklPoll);
      simklPoll = null;
      void poll();
    };
    d.addEventListener("visibilitychange", simklVisHandler);

    simklPoll = setTimeout(poll, 2000);
  }

  let __simklInitDone = false;
  function initSimklAuthUI() {
    if (__simklInitDone) return;
    __simklInitDone = true;

    $("simkl_client_id")?.addEventListener("input", updateSimklButtonState);
    $("simkl_client_secret")?.addEventListener("input", updateSimklButtonState);

    const rid = $("redirect_uri_preview");
    if (rid) rid.textContent = computeRedirect();

    updateSimklButtonState();
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", initSimklAuthUI, { once: true });
  else initSimklAuthUI();

  w.cwAuth = w.cwAuth || {};
  w.cwAuth.simkl = w.cwAuth.simkl || {};
  w.cwAuth.simkl.init = initSimklAuthUI;

  Object.assign(w, {
    setSimklSuccess,
    updateSimklButtonState,
    updateSimklHint,
    startSimkl,
    copyRedirect,
    simklDeleteToken,
  });
})(window, document);
