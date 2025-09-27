// auth.plex-simkl.js
(function (w, d) {
  // --- Small module-local helpers ---
  const $ = (s) => d.getElementById(s);
  const notify = w.notify || (msg => console.log("[notify]", msg));
  const nowIso = () => new Date().toISOString();
  const computeRedirect = () =>
    (typeof w.computeRedirectURI === "function")
      ? w.computeRedirectURI()
      : (location.origin + "/callback");

  // ======== PLEX ========
  /**
   * Toggle visibility of the Plex success message.
   * @param {boolean} show Whether to display the message.
   */
  function setPlexSuccess(show) {
    $("plex_msg")?.classList.toggle("hidden", !show);
  }

  /**
   * Initiate the Plex PIN linking flow:
   * - Opens the Plex link page in a new tab.
   * - Requests a new PIN from the server and shows it in the UI (and copies it to clipboard when possible).
   * - Starts polling for the Plex account token to appear in the server config.
   */
  async function requestPlexPin() {
    try { setPlexSuccess(false); } catch {}
    let win = null;
    try { win = w.open("https://plex.tv/link", "_blank"); } catch {}

    let data = null;
    try {
      const resp = await fetch("/api/plex/pin/new", { method: "POST" });
      data = await resp.json();
      if (!resp.ok || data?.ok === false) throw new Error(data?.error || "PIN request failed");
    } catch (e) {
      console.warn("plex pin fetch failed", e);
      notify("Failed to request PIN");
      return;
    }

    const pin = data.code || data.pin || data.id || "";
    try {
      d.querySelectorAll('#plex_pin, input[name="plex_pin"]').forEach(el => { el.value = pin; });
      const msg = $("plex_msg");
      if (msg) { msg.textContent = pin ? "PIN: " + pin : "PIN request ok"; msg.classList.remove("hidden"); }
      if (pin) { try { await navigator.clipboard.writeText(pin); } catch {} }
      if (win && !win.closed) win.focus();
    } catch (e) { console.warn("pin ui update failed", e); }

    try { startPlexTokenPoll(data); } catch (e) { console.warn("startPlexTokenPoll error", e); }
  }

  // Local poll state (kept inside module)
  let plexPoll = null;
  /**
   * Poll for a Plex account token in the server config with exponential backoff.
   * Stops when a token is found or after a two-minute timeout. Polling pauses
   * when the settings page is hidden or the document is not visible to avoid
   * unnecessary work.
   */
  function startPlexTokenPoll() {
    try { if (plexPoll) clearTimeout(plexPoll); } catch {}
    const MAX_MS = 120000;
    const deadline = Date.now() + MAX_MS;
    const backoff = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;

    const poll = async () => {
      if (Date.now() >= deadline) { plexPoll = null; return; }

      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { plexPoll = setTimeout(poll, 5000); return; }

      let cfg = null;
      try { cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json()); } catch {}
      const tok = (cfg?.plex?.account_token || "").trim();
      if (tok) {
        try { const el = $("plex_token"); if (el) el.value = tok; } catch {}
        try { setPlexSuccess(true); } catch {}
        plexPoll = null;
        return;
      }
      const delay = backoff[Math.min(i, backoff.length - 1)]; i++;
      plexPoll = setTimeout(poll, delay);
    };
    plexPoll = setTimeout(poll, 1000);
  }

  // ======== SIMKL ========
  /**
   * Toggle visibility of the SIMKL success message.
   * @param {boolean} show Whether to display the message.
   */
  function setSimklSuccess(show) {
    $("simkl_msg")?.classList.toggle("hidden", !show);
  }

  // Keep the SIMKL start button enabled only when Client ID and Client Secret
  // are both present. Also, update the redirect URI preview so users can copy it.
  function updateSimklButtonState() {
    try {
      const cid  = ($("simkl_client_id")?.value || "").trim();
      const sec  = ($("simkl_client_secret")?.value || "").trim();
      const btn  = $("simkl_start_btn");
      const hint = $("simkl_hint");
      const rid  = $("redirect_uri_preview");
      if (rid) rid.textContent = computeRedirect();
      const ok = cid.length > 0 && sec.length > 0;
      if (btn)  btn.disabled = !ok;
      if (hint) hint.classList.toggle("hidden", ok);
    } catch (e) {
      console.warn("updateSimklButtonState failed", e);
    }
  }
  // Back-compat: some code calls updateSimklHint()
  const updateSimklHint = updateSimklButtonState;

  /**
   * Copy the computed redirect URI to the clipboard and show a confirmation.
   */
  async function copyRedirect() {
    try { await navigator.clipboard.writeText(computeRedirect()); notify("Redirect URI copied ✓"); }
    catch { /* ignore */ }
  }

  // Local poll state for SIMKL
  let simklPoll = null;
  /**
   * Start the SIMKL OAuth flow:
   * - Optionally persists current settings using a host-provided save function.
   * - Asks the server to create an authorization URL and opens it in a new tab.
   * - Polls for an access token to appear in the server config, with backoff,
   *   until success or a two-minute timeout.
   */
  async function startSimkl() {
    try { setSimklSuccess(false); } catch {}

    // Persist current client id/secret first if the host page exposes it
    try { await w.saveSettings?.(); } catch {}

    const origin = location.origin;
    const j = await fetch("/api/simkl/authorize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ origin }),
      cache: "no-store",
    }).then(r => r.json()).catch(() => null);

    if (!j?.ok || !j.authorize_url) return;

    // open SIMKL consent
    w.open(j.authorize_url, "_blank");

    // clear previous poll
    if (simklPoll) { clearTimeout(simklPoll); simklPoll = null; }

    const MAX_MS = 120000; // 2 min
    const deadline = Date.now() + MAX_MS;
    const backoff = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;

    const poll = async () => {
      if (Date.now() >= deadline) { simklPoll = null; return; }
      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { simklPoll = setTimeout(poll, 5000); return; }

      let cfg = null;
      try { cfg = await fetch("/api/config", { cache: "no-store" }).then(r => r.json()); } catch {}
      const tok = (cfg?.simkl?.access_token || cfg?.auth?.simkl?.access_token || "").trim();
      if (tok) {
        try { const el = $("simkl_access_token"); if (el) el.value = tok; } catch {}
        try { setSimklSuccess(true); } catch {}
        simklPoll = null;
        return;
      }
      const delay = backoff[Math.min(i, backoff.length - 1)]; i++;
      simklPoll = setTimeout(poll, delay);
    };

    simklPoll = setTimeout(poll, 1000);
  }

  // --- Wire basic input listeners on load (safe if IDs are absent) ---
  d.addEventListener("DOMContentLoaded", () => {
    $("simkl_client_id")?.addEventListener("input", updateSimklButtonState);
    $("simkl_client_secret")?.addEventListener("input", updateSimklButtonState);
    const rid = $("redirect_uri_preview"); if (rid) rid.textContent = computeRedirect();
    updateSimklButtonState();
  });

  // --- Expose a limited API on the window for the existing UI ---
  Object.assign(w, {
    // PLEX
    setPlexSuccess, requestPlexPin, startPlexTokenPoll,
    // SIMKL
    setSimklSuccess, updateSimklButtonState, updateSimklHint, startSimkl, copyRedirect
  });
})(window, document);
