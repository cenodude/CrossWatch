/* assets/auth/auth.wetrakr.js */
/* CrossWatch - WeTrakr PKCE browser login and account details */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  if (window.cwAuth?.wetrakr) return;
  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const profile = Shared.createProfileAdapter({ provider: "wetrakr", configKey: "wetrakr", label: "WeTrakr", sectionId: "sec-wetrakr", selectId: "wetrakr_instance", storageKey: "cw.ui.wetrakr.auth.instance.v1" });
  let generation = 0, timer = null, flow = null, popup = null, starting = false, submitting = false;

  function api(action, instance = profile.getInstance()) {
    return `/api/wetrakr/${action}?instance=${encodeURIComponent(instance)}`;
  }

  function post(action, instance, payload = {}) {
    return Shared.fetchJSON(api(action, instance), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload), cache: "no-store" });
  }

  function errorText(code) {
    return ({ access_blocked: "WeTrakr blocked the connection. Contact WeTrakr support.", account_access_denied: "WeTrakr did not grant access to your account.", invalid_account: "WeTrakr did not return a valid account.", invalid_response: "WeTrakr returned an incomplete login response.", missing_client_id: "The WeTrakr application ID is missing.", invalid_client: "WeTrakr rejected the application ID.", invalid_flow: "This login is no longer active. Connect again.", expired_flow: "This login expired. Connect again.", invalid_code: "The authorization code is missing, invalid or expired.", rate_limited: "WeTrakr is limiting requests. Please wait before trying again.", access_denied: "Access was declined.", network_error: "Could not reach WeTrakr.", server_error: "WeTrakr is temporarily unavailable.", reconnect_required: "Your connection expired. Connect again.", account_unavailable: "Could not verify your WeTrakr account." })[code] || "WeTrakr login failed. Please try again.";
  }

  function setStatus(connected, message) {
    Shared.setConnectLocked(["wetrakr_oauth_start"], connected);
    Shared.setStatus("wetrakr_msg", connected, message);
  }

  function showPending(show) {
    el("wetrakr_oauth_panel")?.classList.toggle("hidden", !show);
    el("wetrakr_oauth_start")?.classList.toggle("hidden", show);
    el("wetrakr_oauth_finish")?.classList.toggle("hidden", !show);
    el("wetrakr_oauth_cancel")?.classList.toggle("hidden", !show);
    if (!show && el("wetrakr_code")) el("wetrakr_code").value = "";
  }

  function stop() {
    generation++;
    clearInterval(timer);
    timer = null;
    flow = null;
    starting = submitting = false;
    for (const id of ["wetrakr_oauth_start", "wetrakr_oauth_finish"]) {
      const button = el(id);
      button?.classList.remove("busy");
      if (button) button.disabled = false;
    }
    try { if (popup && !popup.closed && popup.location.href === "about:blank") popup.close(); } catch (_) {}
    popup = null;
    showPending(false);
    el("wetrakr_approval_link")?.removeAttribute("href");
  }

  async function cancel() {
    const previous = flow;
    stop();
    if (previous) await post("oauth/cancel", previous.instance, { flow_id: previous.flow_id }).catch(() => {});
  }

  function tick() {
    if (!flow) return;
    const seconds = Math.max(0, Math.ceil((flow.expires_at * 1000 - Date.now()) / 1000));
    el("wetrakr_login_timer").textContent = `Login session: ${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")} remaining`;
    if (!seconds && !submitting) {
      void cancel();
      setStatus(false, errorText("expired_flow"));
    }
  }

  async function start() {
    if (starting || submitting) return;
    const previous = flow;
    stop();
    starting = true;
    const instance = profile.getInstance(), current = generation;
    const button = el("wetrakr_oauth_start");
    button.classList.add("busy");
    button.disabled = true;
    try { popup = window.open("about:blank", "_blank"); if (popup) popup.opener = null; } catch (_) {}
    const win = popup;
    try {
      if (previous) await post("oauth/cancel", previous.instance, { flow_id: previous.flow_id });
      if (current !== generation) return;
      const response = await post("oauth/start", instance);
      const data = response.data || {};
      if (current !== generation) {
        if (data.flow_id) await post("oauth/cancel", instance, { flow_id: data.flow_id });
        return;
      }
      if (!response.ok || !data.ok) throw new Error(data.error || "invalid_response");
      const url = new URL(data.authorization_url);
      if (url.origin !== "https://api.wetrakr.com" || url.pathname !== "/oauth/authorize" || !data.flow_id) throw new Error("invalid_response");
      flow = { instance, flow_id: data.flow_id, expires_at: data.expires_at };
      el("wetrakr_approval_link").href = url.href;
      showPending(true);
      tick();
      timer = setInterval(tick, 1000);
      setStatus(false, "Approve in WeTrakr, then paste the code here.");
      if (win && !win.closed) { win.location.replace(url.href); popup = null; }
      else Shared.notify("Use the approval link to open WeTrakr.");
      el("wetrakr_code").focus();
    } catch (error) {
      if (current === generation) {
        await cancel();
        setStatus(false, errorText(error.message));
      }
    } finally {
      if (current === generation) { starting = false; button.classList.remove("busy"); button.disabled = false; }
    }
  }

  async function finish() {
    if (!flow || submitting) return;
    const code = String(el("wetrakr_code")?.value || "").trim();
    if (!code) { setStatus(false, "Paste the authorization code from WeTrakr first."); return; }
    submitting = true;
    const current = generation, active = flow;
    const button = el("wetrakr_oauth_finish");
    button.classList.add("busy");
    button.disabled = true;
    setStatus(false, "Verifying your WeTrakr account...");
    try {
      const response = await post("oauth/finish", active.instance, { code, flow_id: active.flow_id });
      if (current !== generation) return;
      const data = response.data || {};
      if (!response.ok || !data.ok) {
        if (!data.retryable) await cancel();
        setStatus(false, errorText(data.error || data.status));
        return;
      }
      stop();
      await refresh();
      document.dispatchEvent(new CustomEvent("cw-provider-connected", { bubbles: true, detail: { provider: "wetrakr", key: "WETRAKR", instance: active.instance } }));
      window.dispatchEvent(new Event("auth-changed"));
      Shared.notify("WeTrakr connected");
    } catch (_) {
      if (current === generation) setStatus(false, "Could not complete the connection. Try again or restart login.");
    } finally {
      if (current === generation) { submitting = false; button.classList.remove("busy"); button.disabled = false; }
    }
  }

  async function refresh() {
    const instance = profile.getInstance(), current = generation;
    try {
      const response = await Shared.fetchJSON(api("status", instance), { cache: "no-store" });
      if (current !== generation || instance !== profile.getInstance() || flow || starting) return;
      const data = response.data || {}, connected = response.ok && !!data.connected;
      const plan = data.plan === "vip" ? "VIP" : data.plan === "free" ? "Free" : "";
      setStatus(connected, connected ? `Connected${data.username ? " as " + data.username : ""}${plan ? " / " + plan : ""}` : data.error ? errorText(data.error) : data.reauth_required ? errorText("reconnect_required") : "Not connected");
      el("wetrakr_account")?.classList.toggle("hidden", !connected);
      if (el("wetrakr_plan")) el("wetrakr_plan").textContent = plan ? `Account: ${plan}` : "";
      const usage = data.plan_usage, labels = { lists: "Lists", list_items: "List items", notes: "Notes" };
      if (el("wetrakr_limits")) el("wetrakr_limits").textContent = (Array.isArray(usage?.features) ? usage.features : []).filter(item => item?.type === "quota" && labels[item.key]).map(item => `${labels[item.key]}: ${item.used ?? 0}${usage.tier === "free" ? " / " + item.limit : " used"}`).join(" / ");
    } catch (_) {
      if (current === generation && !flow && !starting) setStatus(false, "Could not verify WeTrakr");
    }
  }

  async function disconnect() {
    const instance = profile.getInstance();
    await cancel();
    const response = await post("disconnect", instance);
    if (Shared.reportProviderUsage(response)) return;
    if (!response.ok || !response.data?.ok) { Shared.notify("Could not disconnect WeTrakr"); return; }
    window.dispatchEvent(new Event("auth-changed"));
    await refresh();
  }

  function init() {
    profile.ensureUI(() => cancel().then(refresh));
    const actions = { wetrakr_oauth_start: start, wetrakr_oauth_finish: finish, wetrakr_oauth_cancel: () => cancel().then(refresh), wetrakr_disconnect: disconnect };
    for (const [id, action] of Object.entries(actions)) {
      const button = el(id);
      if (button && !button.dataset.wetrakrWired) {
        button.dataset.wetrakrWired = "1";
        button.addEventListener("click", () => { void action().catch(() => Shared.notify("Could not update WeTrakr")); });
      }
    }
    const input = el("wetrakr_code");
    if (input && !input.dataset.wetrakrWired) {
      input.dataset.wetrakrWired = "1";
      input.addEventListener("keydown", event => { if (event.key === "Enter") { event.preventDefault(); void finish(); } });
    }
    void refresh();
  }

  document.addEventListener("cw-auth-modal-closed", event => { if (event.detail?.provider === "wetrakr") void cancel(); });
  window.addEventListener("pagehide", () => { void cancel(); });
  (window.cwAuth ||= {}).wetrakr = { init };
  init();
})();
