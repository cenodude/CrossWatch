/* assets/auth/auth.myanimelist.js */
/* CrossWatch - MyAnimeList hosted login and account connection */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  if (window.cwAuth?.myanimelist) return;
  const Shared = window.CW.AuthShared, el = Shared.el;
  const profile = Shared.createProfileAdapter({ provider: "myanimelist", configKey: "myanimelist", label: "MyAnimeList", sectionId: "sec-myanimelist", selectId: "myanimelist_instance", storageKey: "cw.ui.myanimelist.auth.instance.v1" });
  let generation = 0, timer = null, flow = null, starting = false, popup = null;

  function api(action, instance = profile.getInstance()) {
    return '/api/myanimelist/' + action + '?instance=' + encodeURIComponent(instance);
  }

  function post(action, instance, payload = {}) {
    return Shared.fetchJSON(api(action, instance), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload), cache: "no-store" });
  }

  function errorText(code) {
    return ({ network_dns: "CrossWatch could not resolve the authentication service hostname. Check DNS on the CrossWatch server.", network_tls: "CrossWatch could not establish a trusted TLS connection to the authentication service. Check the server clock and CA certificates.", network_proxy: "CrossWatch could not connect through its configured proxy. Check the server proxy settings.", network_timeout: "The CrossWatch server timed out contacting the authentication service. Please try again.", network_connection: "The CrossWatch server could not connect to the authentication service. Check its outbound network access.", broker_unavailable: "MyAnimeList login is not available on this installation yet.", network_error: "Could not reach the authentication service. Please try again.", broker_error: "The authentication service is temporarily unavailable.", invalid_client: "MyAnimeList login is temporarily unavailable.", invalid_response: "The authentication service returned an incomplete response.", invalid_account: "Could not verify your MyAnimeList account.", account_unavailable: "Could not reach your MyAnimeList account.", invalid_flow: "This login is no longer active. Connect again.", expired_flow: "This login expired. Connect again.", rate_limited: "Please wait before trying again.", access_denied: "Access was declined.", reconnect_required: "Your connection expired. Connect again." })[code] || "MyAnimeList login failed. Please try again.";
  }

  function setStatus(connected, message) {
    Shared.setConnectLocked(["myanimelist_oauth_start"], connected);
    Shared.setStatus("myanimelist_msg", connected, message);
  }

  function pending(show) {
    el("myanimelist_oauth_panel")?.classList.toggle("hidden", !show);
    el("myanimelist_oauth_start")?.classList.toggle("hidden", show);
    el("myanimelist_oauth_cancel")?.classList.toggle("hidden", !show);
  }

  function stop() {
    generation++;
    clearTimeout(timer);
    timer = null;
    flow = null;
    starting = false;
    const button = el("myanimelist_oauth_start");
    button?.classList.remove("busy");
    if (button) button.disabled = false;
    try { if (popup && !popup.closed && popup.location.href === "about:blank") popup.close(); } catch (_) {}
    popup = null;
    pending(false);
    el("myanimelist_approval_link")?.removeAttribute("href");
  }

  async function cancel() {
    const previous = flow;
    stop();
    if (previous) await post("oauth/cancel", previous.instance, { flow_id: previous.flow_id }).catch(() => {});
  }

  function schedule(current, delay) {
    if (current === generation && flow) timer = setTimeout(() => { void poll(current); }, delay * 1000);
  }

  async function poll(current) {
    if (current !== generation || !flow) return;
    const active = flow;
    const seconds = Math.max(0, Math.ceil(active.expires_at - Date.now() / 1000));
    el("myanimelist_login_timer").textContent = 'Login session: ' + seconds + ' seconds remaining';
    if (!seconds) { await cancel(); setStatus(false, errorText("expired_flow")); return; }
    try {
      const response = await post("oauth/poll", active.instance, { flow_id: active.flow_id });
      if (current !== generation) return;
      const data = response.data || {};
      if (!response.ok || !data.ok) {
        if (data.retryable) { setStatus(false, errorText(data.reason || data.error)); schedule(current, 5); return; }
        await cancel();
        setStatus(false, errorText(data.reason || data.error));
        return;
      }
      if (data.status === "pending") { schedule(current, Math.max(3, Number(data.interval) || 5)); return; }
      if (data.status !== "authorized") { await cancel(); setStatus(false, errorText("invalid_response")); return; }
      stop();
      await refresh();
      document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "myanimelist", key: "MYANIMELIST", instance: active.instance } }));
      window.dispatchEvent(new Event("auth-changed"));
      Shared.notify("MyAnimeList connected");
    } catch (_) {
      if (current === generation) { setStatus(false, errorText("network_error")); schedule(current, 5); }
    }
  }

  async function start() {
    if (starting || flow) return;
    stop();
    starting = true;
    const instance = profile.getInstance(), current = generation;
    const button = el("myanimelist_oauth_start");
    button.disabled = true;
    button.classList.add("busy");
    try {
      popup = window.open("about:blank", "_blank");
      if (popup) {
        popup.opener = null;
        popup.document.write(
          '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>CrossWatch - MyAnimeList</title></head>' +
          '<body style="margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#0b0d12;color:#e9eefb;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;text-align:center">' +
          '<div style="padding:24px"><h1 style="font-size:24px">Connecting to MyAnimeList</h1>' +
          '<p role="status" style="font-size:14px;opacity:.8">Preparing your authorization page...</p>' +
          '<p style="font-size:12px;opacity:.7">This window will open MyAnimeList automatically.</p></div></body></html>'
        );
        popup.document.close();
      }
    } catch (_) {}
    const win = popup;
    try {
      const response = await post("oauth/start", instance);
      const data = response.data || {};
      if (current !== generation) {
        if (data.flow_id) await post("oauth/cancel", instance, { flow_id: data.flow_id });
        return;
      }
      if (!response.ok || !data.ok) throw new Error(data.reason || data.error || "invalid_response");
      const url = new URL(data.authorization_url);
      if (url.origin !== "https://myanimelist.net" || url.pathname !== "/v1/oauth2/authorize" || !data.flow_id || !Number.isFinite(data.expires_at)) throw new Error("invalid_response");
      flow = { instance, flow_id: data.flow_id, expires_at: data.expires_at };
      el("myanimelist_approval_link").href = url.href;
      pending(true);
      setStatus(false, "Approve CrossWatch in MyAnimeList. Waiting for authorization...");
      schedule(current, Math.max(3, Number(data.interval) || 5));
      if (win && !win.closed) { win.location.replace(url.href); popup = null; }
      else Shared.notify("Use the approval link to open MyAnimeList.");
    } catch (error) {
      if (current === generation) { await cancel(); setStatus(false, errorText(error.message)); }
    } finally {
      if (current === generation) { starting = false; button.disabled = false; button.classList.remove("busy"); }
    }
  }

  async function refresh() {
    const instance = profile.getInstance(), current = generation;
    try {
      const response = await Shared.fetchJSON(api("status", instance), { cache: "no-store" });
      if (current !== generation || instance !== profile.getInstance() || flow || starting) return;
      const data = response.data || {}, connected = response.ok && !!data.connected;
      setStatus(connected, connected ? 'Connected' + (data.username ? ' as ' + data.username : '') : data.error ? errorText(data.reason || data.error) : data.reauth_required ? errorText("reconnect_required") : data.login_available ? "Not connected" : errorText("broker_unavailable"));
      if (!connected && !data.login_available && el("myanimelist_oauth_start")) el("myanimelist_oauth_start").disabled = true;
    } catch (_) { if (current === generation) setStatus(false, errorText("network_error")); }
  }

  async function disconnect() {
    const instance = profile.getInstance();
    await cancel();
    const response = await post("disconnect", instance);
    if (Shared.reportProviderUsage(response)) return;
    if (!response.ok || !response.data?.ok) { Shared.notify("Could not disconnect MyAnimeList"); return; }
    window.dispatchEvent(new Event("auth-changed"));
    await refresh();
  }

  function init() {
    profile.ensureUI(() => cancel().then(refresh));
    const actions = { myanimelist_oauth_start: start, myanimelist_oauth_cancel: () => cancel().then(refresh), myanimelist_disconnect: disconnect };
    for (const [id, action] of Object.entries(actions)) {
      const button = el(id);
      if (button && !button.dataset.malWired) {
        button.dataset.malWired = "1";
        button.addEventListener("click", () => { void action().catch(() => Shared.notify("Could not update MyAnimeList")); });
      }
    }
    void refresh();
  }

  document.addEventListener("cw-auth-modal-closed", event => { if (event.detail?.provider === "myanimelist") void cancel(); });
  window.addEventListener("pagehide", () => { void cancel(); });
  (window.cwAuth ||= {}).myanimelist = { init };
  init();
})();
