// auth.simkl.js — SIMKL auth only
(function (w, d) {
  // --- tiny utils
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;
  const computeRedirect = () =>
    (typeof w.computeRedirectURI === "function"
      ? w.computeRedirectURI()
      : (location.origin + "/callback"));

  // success banner
  function setSimklSuccess(on) { $("simkl_msg")?.classList.toggle("hidden", !on); }

  // form state
  function updateSimklButtonState() {
    try {
      const cid = ($("simkl_client_id")?.value || "").trim();
      const sec = ($("simkl_client_secret")?.value || "").trim();
      const btn = $("btn-connect-simkl") || $("simkl_start_btn");
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
    } catch {}
  }

  // delete SIMKL access token
  async function simklDeleteToken() {
    const btn = q('#sec-simkl .btn.danger');
    const msg = $('simkl_msg');
    if (btn) { btn.disabled = true; btn.classList.add('busy'); }
    if (msg) { msg.classList.remove('hidden'); msg.classList.remove('warn'); msg.textContent = ''; }
    try {
      const r = await fetch('/api/simkl/token/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
        cache: 'no-store'
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok && (j.ok !== false)) {
        try { const el = $('simkl_access_token'); if (el) el.value = ''; } catch {}
        try { setSimklSuccess(false); } catch {}
        if (msg) { msg.textContent = 'Access token removed.'; }
        notify('SIMKL access token removed.');
      } else {
        if (msg) { msg.classList.add('warn'); msg.textContent = 'Could not remove token.'; }
      }
    } catch {
      if (msg) { msg.classList.add('warn'); msg.textContent = 'Error removing token.'; }
    } finally {
      if (btn) { btn.disabled = false; btn.classList.remove('busy'); }
    }
  }

  //  OAuth start
  let simklPoll = null;
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
      body: JSON.stringify({ origin }),
      cache: "no-store"
    }).then(r => r.json()).catch(() => null);

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

    if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }
    const deadline = Date.now() + 120000;
    const back = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;
    const poll = async () => {
      if (Date.now() >= deadline) { simklPoll = null; return; }
      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { simklPoll = setTimeout(poll, 5000); return; }
      let cfg = null;
      try { cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then(r => r.json()); } catch {}
      const tok = (cfg?.simkl?.access_token || cfg?.auth?.simkl?.access_token || "").trim();
      if (tok) {
        try { const el = $("simkl_access_token"); if (el) el.value = tok; } catch {}
        try { setSimklSuccess(true); } catch {}
        simklPoll = null;
        return;
      }
      simklPoll = setTimeout(poll, back[Math.min(i++, back.length - 1)]);
    };
    simklPoll = setTimeout(poll, 1000);
  }

  // lifecycle
  d.addEventListener("DOMContentLoaded", () => {
    $("simkl_client_id")?.addEventListener("input", updateSimklButtonState);
    $("simkl_client_secret")?.addEventListener("input", updateSimklButtonState);
    const rid = $("redirect_uri_preview");
    if (rid) rid.textContent = computeRedirect();
    updateSimklButtonState();
  });

  // exports
  Object.assign(w, {
    setSimklSuccess, updateSimklButtonState, updateSimklHint,
    startSimkl, copyRedirect, simklDeleteToken
  });
})(window, document);
