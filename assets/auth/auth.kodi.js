// assets/auth/auth.kodi.js
(function () {
  if (window._kodiAuthPatched) return;
  window._kodiAuthPatched = true;

  const Shared = window.CW.AuthShared;
  const el = Shared.el;
  const txt = Shared.txt;
  const note = Shared.notify;
  const Q = (s, r = document) => r.querySelector(s);
  const profile = Shared.createProfileAdapter({
    provider: "kodi",
    configKey: "kodi",
    label: "Kodi",
    sectionId: "sec-kodi",
    selectId: "kodi_instance",
    storageKey: "cw.ui.kodi.auth.instance.v1",
    title: "Select which Kodi media client this config applies to.",
  });
  const SUBTAB_KEY = "cw.ui.kodi.auth.subtab.v1";
  let H = new Set();
  let R = new Set();
  let P = new Set();
  let C = new Set();
  let S = new Set();
  let lastLibraries = [];
  let wlHandle = null;
  let wlHost = null;
  let connected = false;

  function api(path) {
    return profile ? profile.api(path) : String(path || "");
  }

  function cfgBlock(cfg, create) {
    return profile ? profile.cfgBlock(cfg, create) : {};
  }

  async function fetchJSON(url, opts) {
    return Shared.fetchJSON(url, opts);
  }

  function ensureWhitelistTable() {
    if (window.cwWhitelistTable) return Promise.resolve(true);
    if (window.__cwWhitelistTableLoading) return window.__cwWhitelistTableLoading;
    window.__cwWhitelistTableLoading = new Promise((resolve) => {
      const s = document.createElement("script");
      s.src = `/assets/helpers/whitelist_table.js${window.__CW_VERSION__ ? `?v=${encodeURIComponent(window.__CW_VERSION__)}` : ""}`;
      s.async = true;
      s.onload = () => resolve(!!window.cwWhitelistTable);
      s.onerror = () => resolve(false);
      document.head.appendChild(s);
    });
    return window.__cwWhitelistTableLoading;
  }

  function setConn(ok, msg) {
    connected = !!ok;
    try { Shared.setConnectLocked("kodi_connect", !!ok); } catch {}
    syncTabs();
    return Shared.setStatus("kodi_msg", ok, msg || (ok ? "Connected" : "Not connected"));
  }

  const setFor = (fk) => ({ hist: H, rate: R, prog: P, coll: C, scr: S }[fk]);

  function syncHidden() {
    const esc = (value) => String(value).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
    const write = (id, values) => {
      const node = el(id);
      if (node) node.innerHTML = Array.from(values || []).map((v) => `<option selected value="${esc(v)}">${esc(v)}</option>`).join("");
    };
    write("kodi_lib_history", H);
    write("kodi_lib_ratings", R);
    write("kodi_lib_progress", P);
    write("kodi_lib_collection", C);
    write("kodi_lib_scrobble", S);
    window.__kodiHydrated = true;
  }

  function selectSub(tab, opts = {}) {
    const root = Q('#sec-kodi .cw-meta-provider-panel[data-provider="kodi"]') || Q("#sec-kodi");
    if (!root) return;
    const sub = ["auth", "whitelist"].includes(String(tab || "").toLowerCase()) ? String(tab).toLowerCase() : "auth";
    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => btn.classList.toggle("active", btn.dataset.sub === sub));
    root.querySelectorAll(".cw-subpanel[data-sub]").forEach((panel) => panel.classList.toggle("active", panel.dataset.sub === sub));
    if (opts.persist !== false) {
      try { localStorage.setItem(SUBTAB_KEY, sub); } catch {}
    }
    if (sub === "whitelist") {
      renderLibraries(lastLibraries);
      void loadLibraries();
    }
  }

  function syncTabs() {
    const root = Q('#sec-kodi .cw-meta-provider-panel[data-provider="kodi"]') || Q("#sec-kodi");
    if (!root) return;
    try { Shared.applyMediaTabState(root, { configured: connected, connected, whitelistEnabled: connected, settingsEnabled: false }); } catch {}
  }

  function renderLibraries(libs) {
    if (Array.isArray(libs)) lastLibraries = libs;
    syncHidden();
    const host = el("kodi_libraries");
    if (!host) return;
    if (!window.cwWhitelistTable) {
      host.innerHTML = `<div class="cw-wl"><div class="cw-wl-foot"><div class="cw-wl-note">Empty = all libraries.</div><div class="cw-wl-foot-r"><button type="button" class="cw-wl-load" data-kodi-load-libs><span class="material-symbols-rounded" aria-hidden="true">sync</span>Load libraries</button></div></div></div>`;
      void ensureWhitelistTable().then(() => renderLibraries(lastLibraries));
      return;
    }
    if (wlHandle && wlHost === host) { wlHandle.render(); return; }
    wlHandle = null;
    wlHost = host;
    wlHandle = window.cwWhitelistTable.mount({
      host,
      features: [
        { key: "hist", label: "History" },
        { key: "rate", label: "Ratings" },
        { key: "prog", label: "Progress" },
        { key: "coll", label: "Collections" },
        { key: "scr", label: "Scrobble", title: "Libraries watchers and webhooks read from. Scrobble destinations use History and Progress, or the libraries set on the route." },
      ],
      getLibs: () => lastLibraries,
      isOn: (fk, id) => setFor(fk).has(String(id)),
      selectedIds: () => ["hist", "rate", "prog", "coll", "scr"].flatMap((fk) => [...(setFor(fk) || [])]),
      setOn: (fk, id, on) => { const s = setFor(fk); if (!s) return; if (on) s.add(String(id)); else s.delete(String(id)); },
      commit: syncHidden,
      load: async () => { await loadLibraries(true); },
    });
  }

  function liveQuery(url) {
    const parts = [];
    const server = txt(el("kodi_server")?.value || "");
    if (server) parts.push(`server=${encodeURIComponent(server)}`);
    if (el("kodi_verify_ssl")) parts.push(`verify_ssl=${el("kodi_verify_ssl").checked ? 1 : 0}`);
    return parts.length ? `${url}${url.includes("?") ? "&" : "?"}${parts.join("&")}` : url;
  }

  async function loadLibraries(force = false) {
    if (!force && lastLibraries.length) return renderLibraries(lastLibraries);
    const host = el("kodi_libraries");
    const btn = host?.querySelector?.(".cw-wl-load");
    if (btn) { btn.disabled = true; btn.classList.add("busy"); }
    try {
      const r = await fetchJSON(liveQuery(api("/api/kodi/libraries")), { cache: "no-store" });
      const libs = Array.isArray(r?.data?.libraries) ? r.data.libraries : (Array.isArray(r?.data) ? r.data : []);
      renderLibraries(libs);
    } catch {
      renderLibraries([]);
    } finally {
      const nextBtn = el("kodi_libraries")?.querySelector?.(".cw-wl-load");
      if (nextBtn) { nextBtn.disabled = false; nextBtn.classList.remove("busy"); }
    }
  }

  const METHOD_KEY = "cw.ui.kodi.auth.method.v1";
  const ADDON_COPY = {
    code: {
      title: "Connect the Kodi add-on",
      copy: "The add-on reports playback and viewers to CrossWatch, so scrobbling from Kodi works without a JSON-RPC connection. Pair it with a short code.",
      steps: [["Install add-on", "Install the CrossWatch add-on on Kodi"], ["Get a code", "Press Connect add-on below"], ["Pair", "Enter the address and code in the add-on"]],
    },
    link: {
      title: "Connect the Kodi add-on",
      copy: "The add-on reports playback and viewers to CrossWatch. This Kodi is connected over JSON-RPC, so CrossWatch can send the add-on its settings directly.",
      steps: [["Install add-on", "Install the CrossWatch add-on on Kodi"], ["Link", "Press Link add-on below"], ["Confirm", "Confirm on your TV"]],
    },
  };

  const ADDON_HTML = `
    <div class="kodi-addon">
      <div id="kodi_addon_live" class="kodi-addon-status" hidden>
        <div id="kodi_addon_info" class="muted"></div>
        <button id="kodi_addon_repair" class="btn" type="button">Pair again</button>
        <div id="kodi_addon_msg" class="msg cw-connection-status-pill hidden" role="status" aria-live="polite"></div>
      </div>

      <div id="kodi_addon_link_row" hidden>
        <label for="kodi_addon_address">CrossWatch address this Kodi can reach</label>
        <div class="inp-row">
          <input id="kodi_addon_address" class="grow" autocomplete="off" spellcheck="false" autocapitalize="off" placeholder="http://host:8787">
          <button id="kodi_addon_link" class="btn kodi-addon-primary cw-connection-primary-action" type="button">Link add-on</button>
        </div>
        <div class="muted" id="kodi_addon_link_note" style="margin-top:6px"></div>
      </div>

      <details id="kodi_addon_alt">
        <summary id="kodi_addon_alt_sum" class="muted" hidden>Other ways to connect</summary>
        <div class="kodi-addon kodi-addon-alt">
          <div id="kodi_addon_start" class="kodi-addon-status" hidden>
            <button id="kodi_addon_connect" class="btn" type="button">Connect add-on</button>
            <span class="muted">Shows a short code to enter in the add-on on your Kodi.</span>
          </div>

          <div id="kodi_addon_codecard" class="kodi-qc" hidden>
            <div class="kodi-qc-codewrap">
              <div class="kodi-qc-code" id="kodi_addon_code">------</div>
              <button id="kodi_addon_pair" class="btn" type="button">New code</button>
              <button id="kodi_addon_cancel" class="btn danger" type="button">Cancel</button>
            </div>
            <div class="kodi-qc-meta">
              <span class="muted">In the add-on, enter the address <strong id="kodi_addon_address_text"></strong> and this code.</span>
              <span class="muted" id="kodi_addon_code_note"></span>
            </div>
          </div>

          <details id="kodi_addon_manual" hidden>
            <summary class="muted">Manual setup</summary>
            <label for="kodi_addon_url" style="margin-top:8px;display:block">Add-on URL</label>
            <div class="inp-row">
              <input id="kodi_addon_url" class="grow" readonly autocomplete="off" spellcheck="false">
              <button id="kodi_addon_copy" class="btn" type="button">Copy</button>
              <button id="kodi_addon_regen" class="btn" type="button">Regenerate</button>
            </div>
            <div class="muted" style="margin-top:6px">Paste this into the Webhook URL setting of the add-on. Regenerating unlinks the add-on until it is paired again.</div>
            <div id="kodi_addon_disable_row" class="inp-row" style="margin-top:12px" hidden>
              <button id="kodi_addon_disable" class="btn danger" type="button">Turn off add-on</button>
            </div>
          </details>
        </div>
      </details>
    </div>`;

  let addonMethod = "";
  let addonPaired = null;
  let addonInstance = "";
  let addonPoll = null;
  let addonPollUntil = 0;
  let addonLinkUntil = 0;
  let addonRpc = false;

  function authSubpanel() {
    return Q('#sec-kodi .cw-subpanel[data-sub="auth"]');
  }

  function swapText(node, value) {
    if (!node) return;
    if (value === null) {
      if (node.dataset.kodiOrig !== undefined) {
        node.textContent = node.dataset.kodiOrig;
        delete node.dataset.kodiOrig;
      }
      return;
    }
    if (node.dataset.kodiOrig === undefined) node.dataset.kodiOrig = node.textContent || "";
    node.textContent = value;
  }

  function applyMethodCopy(method) {
    const sub = authSubpanel();
    if (!sub) return;
    const addon = method === "addon";
    const copy = addonRpc ? ADDON_COPY.link : ADDON_COPY.code;
    swapText(sub.querySelector(".cw-auth-journey-title"), addon ? copy.title : null);
    swapText(sub.querySelector(".cw-auth-journey-copy"), addon ? copy.copy : null);
    sub.querySelectorAll(".cw-connection-steps > div").forEach((step, idx) => {
      const text = copy.steps[idx];
      swapText(step.querySelector("strong"), addon && text ? text[0] : null);
      swapText(step.querySelector("small"), addon && text ? text[1] : null);
    });
  }

  function setMethod(method, opts = {}) {
    const sub = authSubpanel();
    const row = sub?.querySelector(".kodi-method-row");
    if (!sub || !row) return;
    const m = method === "addon" ? "addon" : "jsonrpc";
    addonMethod = m;
    sub.dataset.kodiMethod = m;
    row.querySelectorAll(".kodi-method").forEach((btn) => {
      const on = btn.dataset.method === m;
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
    applyMethodCopy(m);
    if (opts.persist) {
      try { localStorage.setItem(METHOD_KEY, m); } catch {}
    }
  }

  function clearMethod() {
    const sub = authSubpanel();
    if (!sub) return;
    applyMethodCopy("jsonrpc");
    delete sub.dataset.kodiMethod;
    addonMethod = "";
    addonPaired = null;
    const row = sub.querySelector(".kodi-method-row");
    if (row) row.hidden = true;
  }

  function showMethods(data) {
    const row = authSubpanel()?.querySelector(".kodi-method-row");
    if (!row) return;
    row.hidden = false;
    if (!row.__kodiWired) {
      row.__kodiWired = true;
      row.querySelectorAll(".kodi-method").forEach((btn) => btn.addEventListener("click", () => setMethod(btn.dataset.method, { persist: true })));
      const sub = authSubpanel();
      if (sub && typeof MutationObserver === "function") {
        new MutationObserver(() => {
          const title = sub.querySelector(".cw-auth-journey-title");
          if (addonMethod === "addon" && title && title.dataset.kodiOrig === undefined) applyMethodCopy("addon");
        }).observe(sub, { childList: true, subtree: true });
      }
    }
    let wanted = addonMethod;
    if (!wanted) {
      try { wanted = localStorage.getItem(METHOD_KEY) || ""; } catch {}
    }
    if (!wanted) wanted = data.paired && !data.jsonrpc_connected ? "addon" : "jsonrpc";
    setMethod(wanted);
  }

  function addonAge(seconds) {
    if (seconds === null || seconds === undefined || seconds === "") return "";
    const n = Number(seconds);
    if (!Number.isFinite(n) || n < 0) return "";
    if (n < 90) return "just now";
    if (n < 5400) return `${Math.round(n / 60)} min ago`;
    if (n < 129600) return `${Math.round(n / 3600)} h ago`;
    return `${Math.round(n / 86400)} d ago`;
  }

  function stopAddonPoll() {
    if (addonPoll) clearInterval(addonPoll);
    addonPoll = null;
  }

  function watchAddon(seconds) {
    addonPollUntil = Date.now() + Math.max(0, Number(seconds) || 0) * 1000;
    if (addonPoll) return;
    addonPoll = setInterval(() => {
      if (Date.now() > addonPollUntil + 5000 || !el("kodi_addon_codecard")) return stopAddonPoll();
      void loadAddon();
    }, 4000);
  }

  function wireAddon() {
    el("kodi_addon_connect")?.addEventListener("click", () => { void pairAddon(); });
    el("kodi_addon_repair")?.addEventListener("click", () => { void pairAddon(); });
    el("kodi_addon_pair")?.addEventListener("click", () => { void pairAddon(); });
    el("kodi_addon_cancel")?.addEventListener("click", () => { void updateAddon({ enabled: false }, "Pairing cancelled"); });
    el("kodi_addon_link")?.addEventListener("click", () => { void linkAddon(); });
    el("kodi_addon_copy")?.addEventListener("click", () => {
      void Shared.copyText(el("kodi_addon_url")?.value || "", el("kodi_addon_copy"), { copiedText: "Copied", emptyMessage: "No add-on URL yet." });
    });
    el("kodi_addon_regen")?.addEventListener("click", () => {
      if (window.confirm("Regenerate the add-on URL? The add-on stops working until it is paired again.")) void updateAddon({ enabled: true, regenerate: true }, "Kodi add-on URL regenerated");
    });
    el("kodi_addon_disable")?.addEventListener("click", () => {
      if (window.confirm("Turn off the add-on for this Kodi? The add-on stops working until it is paired again.")) void updateAddon({ enabled: false }, "Kodi add-on turned off");
    });
  }

  function renderAddon(data) {
    const host = el("kodi_addon_block");
    if (!host) return;
    if (!data) {
      stopAddonPoll();
      clearMethod();
      host.hidden = true;
      host.textContent = "";
      return;
    }
    if (!el("kodi_addon_codecard")) {
      host.innerHTML = ADDON_HTML;
      wireAddon();
    }
    host.hidden = false;
    addonRpc = !!data.jsonrpc_connected;
    showMethods(data);

    const instance = txt(data.instance);
    const paired = !!data.paired;
    const rpc = !!data.jsonrpc_connected;
    const code = txt(data.pair_code);
    const left = Number(data.pair_expires_in) || 0;
    const open = paired || !!code;

    const alt = el("kodi_addon_alt");
    el("kodi_addon_alt_sum").hidden = !rpc;
    if (!rpc || code) alt.open = true;
    else if (alt.dataset.kodiRpc !== "1") alt.open = false;
    alt.dataset.kodiRpc = rpc ? "1" : "0";

    const connectBtn = el("kodi_addon_connect");
    connectBtn.textContent = rpc ? "Use a pairing code" : "Connect add-on";
    connectBtn.classList.toggle("kodi-addon-primary", !rpc);
    connectBtn.classList.toggle("cw-connection-primary-action", !rpc);
    el("kodi_addon_start").hidden = rpc ? !!code : open;
    el("kodi_addon_live").hidden = !paired;
    el("kodi_addon_repair").hidden = !!code || rpc;
    el("kodi_addon_codecard").hidden = !code;
    el("kodi_addon_cancel").hidden = paired;
    if (paired && addonPaired === false) addonLinkUntil = 0;
    const linking = Date.now() < addonLinkUntil;
    el("kodi_addon_link_row").hidden = !rpc;
    el("kodi_addon_link").textContent = paired ? "Link again" : "Link add-on";
    el("kodi_addon_link_note").textContent = linking
      ? "Sent to Kodi. Confirm on the TV to finish."
      : "Sends the address and token to the Kodi of this profile. You only confirm on the TV.";
    el("kodi_addon_manual").hidden = !open;
    el("kodi_addon_disable_row").hidden = !paired;
    el("kodi_addon_url").value = open ? txt(data.url) : "";

    el("kodi_addon_code").textContent = code || "------";
    el("kodi_addon_code_note").textContent = code ? `Waiting for the add-on, valid for ${Math.max(1, Math.ceil(left / 60))} more min` : "";
    el("kodi_addon_address_text").textContent = txt(data.address);
    const address = el("kodi_addon_address");
    if (address && !txt(address.value) && document.activeElement !== address) address.value = txt(data.address);

    if (paired) {
      const seen = addonAge(data.age_seconds);
      let msg = "Add-on connected";
      if (!data.active && seen) msg = `Add-on connected, last seen ${seen}`;
      Shared.setStatus("kodi_addon_msg", true, msg);
      const info = [];
      if (txt(data.device_name)) info.push(txt(data.device_name));
      if (txt(data.addon_version)) info.push(`Add-on ${txt(data.addon_version)}`);
      const viewers = Array.isArray(data.viewers) ? data.viewers.map(txt).filter(Boolean) : [];
      if (viewers.length) info.push(`Viewers: ${viewers.join(", ")}`);
      if (Number(data.pkc_skipped) > 0) info.push(`PKC playback skipped: ${Number(data.pkc_skipped)}`);
      el("kodi_addon_info").textContent = info.join(" | ");
    } else {
      Shared.setStatusPill("kodi_addon_msg", "hidden");
      el("kodi_addon_info").textContent = "";
    }

    if (paired && addonPaired === false && addonInstance === instance) note("Kodi add-on connected");
    addonPaired = paired;
    addonInstance = instance;
    if (code) watchAddon(left);
  }

  async function loadAddon() {
    if (!el("kodi_addon_block")) return;
    try {
      const r = await fetchJSON(api("/api/kodi/addon"), { cache: "no-store" });
      renderAddon(r.ok && r.data && r.data.ok ? r.data : null);
    } catch {
      renderAddon(null);
    }
  }

  async function pairAddon() {
    try {
      const r = await fetchJSON(api("/api/kodi/addon/pair"), { method: "POST", cache: "no-store" });
      if (!r.ok || !r.data || r.data.ok === false) throw new Error(txt(r.data?.error) || "Could not create a pairing code");
      renderAddon(r.data);
    } catch (e) {
      note(e && e.message ? e.message : "Could not create a pairing code");
      void loadAddon();
    }
  }

  async function linkAddon() {
    const btn = el("kodi_addon_link");
    try {
      if (btn) btn.disabled = true;
      const r = await fetchJSON(api("/api/kodi/addon/link"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ address: txt(el("kodi_addon_address")?.value || "") }),
        cache: "no-store",
      });
      if (!r.ok || !r.data || r.data.ok === false) throw new Error(txt(r.data?.error) || "Could not reach the add-on");
      note("Sent to Kodi. Confirm on the TV.");
      addonLinkUntil = Date.now() + 120000;
      watchAddon(120);
      await loadAddon();
    } catch (e) {
      note(e && e.message ? e.message : "Could not reach the add-on");
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function updateAddon(body, done) {
    try {
      const r = await fetchJSON(api("/api/kodi/addon"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
        cache: "no-store",
      });
      if (Shared.reportProviderUsage(r)) return void loadAddon();
      if (!r.ok || !r.data || r.data.ok === false) throw new Error(txt(r.data?.error) || "Kodi add-on update failed");
      addonPaired = null;
      renderAddon(r.data);
      if (done) note(done);
    } catch (e) {
      note(e && e.message ? e.message : "Kodi add-on update failed");
      void loadAddon();
    }
  }

  function friendlyError(data) {
    const reason = txt(data?.reason || data?.error);
    switch (reason) {
      case "missing_server": return "Enter a Kodi server URL";
      case "invalid_credentials": return "Kodi rejected the credentials";
      case "unreachable": return "Kodi server is unreachable";
      case "not_kodi": return "That server is not Kodi";
      case "version_too_old": return "Kodi 21.0 Omega or newer is required";
      case "jsonrpc_too_old": return "Kodi JSON-RPC 13.5.0 or newer is required";
      case "invalid_response": return "Kodi returned an unexpected response";
      default: return txt(data?.error) || "Kodi connection failed";
    }
  }

  async function hydrate() {
    profile?.ensureUI(() => { void hydrate(); });
    const cfg = window._cfgCache || await Shared.getConfig();
    const k = cfgBlock(cfg, true);

    if (el("kodi_server")) el("kodi_server").value = txt(k?.server || "");
    if (el("kodi_username")) el("kodi_username").value = txt(k?.username || "");
    if (el("kodi_verify_ssl")) el("kodi_verify_ssl").checked = !!k?.verify_ssl;
    H = new Set((k?.history?.libraries || []).map(String));
    R = new Set((k?.ratings?.libraries || []).map(String));
    P = new Set((k?.progress?.libraries || []).map(String));
    C = new Set((k?.collection?.libraries || []).map(String));
    S = new Set((k?.scrobble?.libraries || []).map(String));
    syncHidden();
    renderLibraries(lastLibraries);
    void loadAddon();

    try {
      const r = await fetchJSON(api("/api/kodi/status"), { cache: "no-store" });
      const data = r.data || {};
      Shared.maskSecret(el("kodi_password"), !!data.has_password);
      const ok = !!(r.ok && data.connected);
      const version = txt(data.kodi_version);
      setConn(ok, ok ? `Connected${version ? `: Kodi ${version}` : ""}` : "Not connected");
      if (ok) void loadLibraries();
    } catch {
      Shared.maskSecret(el("kodi_password"), false);
      setConn(false, "Not connected");
    }
  }

  async function onConnect() {
    const server = txt(el("kodi_server")?.value || "");
    const username = txt(el("kodi_username")?.value || "");
    const passInfo = Shared.readSecretField(el("kodi_password"));
    const verify_ssl = !!el("kodi_verify_ssl")?.checked;
    if (!server) {
      note("Enter a Kodi server URL");
      return;
    }

    try {
      setConn(false, "Connecting...");
      const r = await fetchJSON(api("/api/kodi/connect"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          server,
          username,
          password: passInfo.masked ? "********" : passInfo.value,
          verify_ssl,
        }),
        cache: "no-store",
      });
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(friendlyError(r.data || {}));
      Shared.maskSecret(el("kodi_password"), passInfo.hasValue);
      const version = txt(r.data?.kodi_version);
      setConn(true, `Connected${version ? `: Kodi ${version}` : ""}`);
      await loadLibraries(true);
      note("Kodi connected");
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      const msg = e && e.message ? e.message : "Kodi connection failed";
      setConn(false, msg);
      note(msg);
    }
  }

  async function onDisconnect() {
    try {
      const r = await fetchJSON(api("/api/kodi/disconnect"), { method: "POST", cache: "no-store" });
      if (Shared.reportProviderUsage(r)) return;
      if (!r.ok || (r.data && r.data.ok === false)) throw new Error(r.data?.error || "disconnect_failed");
      Shared.maskSecret(el("kodi_password"), false);
      lastLibraries = [];
      wlHandle = null;
      wlHost = null;
      renderLibraries([]);
      setConn(false, "Not connected");
      await loadAddon();
      note("Kodi disconnected");
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      note("Kodi disconnect failed" + (e && e.message ? ": " + e.message : ""));
    }
  }

  function wire() {
    const c = el("kodi_connect");
    if (c && !c.__wired) { c.addEventListener("click", onConnect); c.__wired = true; }

    const d = el("kodi_disconnect");
    if (d && !d.__wired) { d.addEventListener("click", onDisconnect); d.__wired = true; }

    const p = el("kodi_password");
    if (p && !p.__wiredSecret) {
      Shared.wireSecretInput(p);
      p.__wiredSecret = true;
    }
    const host = el("kodi_libraries");
    if (host && !host.__kodiLoadFallback) {
      host.__kodiLoadFallback = true;
      host.addEventListener("click", (ev) => {
        if (ev.target?.closest?.("[data-kodi-load-libs]")) void loadLibraries(true);
      });
    }
    document.querySelectorAll('#sec-kodi .cw-subtile[data-sub]').forEach((btn) => {
      if (btn.__kodiTabWired) return;
      btn.__kodiTabWired = true;
      btn.addEventListener("click", () => selectSub(btn.dataset.sub));
    });
    let last = "auth";
    try { last = localStorage.getItem(SUBTAB_KEY) || "auth"; } catch {}
    selectSub(last, { persist: false });
  }

  function boot() {
    wire();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", hydrate, { once: true });
    } else {
      hydrate();
    }
  }

  document.addEventListener("settings-collect", (ev) => {
    const cfg = ev?.detail?.cfg;
    if (!cfg) return;

    const server = txt(el("kodi_server")?.value || "");
    const username = txt(el("kodi_username")?.value || "");
    const passInfo = Shared.readSecretField(el("kodi_password"));
    const verify_ssl = !!el("kodi_verify_ssl")?.checked;
    const hasWhitelist = H.size || R.size || P.size || C.size || S.size;
    if (!server && !username && !passInfo.hasValue && !verify_ssl && !hasWhitelist) return;

    cfg.kodi = cfg.kodi || {};
    let k = cfg.kodi;
    const inst = profile?.getInstance?.() || "default";
    if (inst !== "default") {
      k.instances = k.instances || {};
      k.instances[inst] = k.instances[inst] || {};
      k = k.instances[inst];
    }

    if (server) k.server = server;
    if (username) k.username = username;
    if (passInfo.hasValue && !passInfo.masked) k.password = passInfo.value;
    k.verify_ssl = verify_ssl;
    k.history = k.history || {};
    k.ratings = k.ratings || {};
    k.progress = k.progress || {};
    k.collection = k.collection || {};
    k.scrobble = k.scrobble || {};
    syncHidden();
    k.history.libraries = Array.from(H);
    k.ratings.libraries = Array.from(R);
    k.progress.libraries = Array.from(P);
    k.collection.libraries = Array.from(C);
    k.scrobble.libraries = Array.from(S);
  });

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.kodi = { init: boot, hydrate };
  boot();
})();
