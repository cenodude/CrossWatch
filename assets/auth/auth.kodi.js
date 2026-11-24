// assets/auth/auth.kodi.js
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
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
    title: T("connections.auth.kodi.select"),
  });
  const SUBTAB_KEY = "cw.ui.kodi.auth.subtab.v1";
  let H = new Set();
  let loadedInstance = null;
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
    return Shared.setStatus("kodi_msg", ok, msg || (ok ? T("connections.auth.connected") : T("connections.auth.notConnected")));
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
      host.innerHTML = `<div class="cw-wl"><div class="cw-wl-foot"><div class="cw-wl-note">${TH("connections.wl.empty")}</div><div class="cw-wl-foot-r"><button type="button" class="cw-wl-load" data-kodi-load-libs><span class="material-symbols-rounded" aria-hidden="true">sync</span>${TH("connections.auth.loadLibraries")}</button></div></div></div>`;
      void ensureWhitelistTable().then(() => renderLibraries(lastLibraries));
      return;
    }
    if (wlHandle && wlHost === host) { wlHandle.render(); return; }
    wlHandle = null;
    wlHost = host;
    wlHandle = window.cwWhitelistTable.mount({
      host,
      features: [
        { key: "hist", label: T("connections.auth.feat.history") },
        { key: "rate", label: T("connections.auth.feat.ratings") },
        { key: "prog", label: T("connections.auth.feat.progress") },
        { key: "coll", label: T("connections.auth.feat.collections") },
        { key: "scr", label: T("connections.auth.feat.scrobble"), title: T("connections.auth.feat.scrobbleTitle") },
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
      title: T("connections.auth.kodi.connectTitle"),
      copy: T("connections.auth.kodi.copyCode"),
      steps: [[T("connections.auth.kodi.install"), T("connections.auth.kodi.installSub")], [T("connections.auth.kodi.getCode"), T("connections.auth.kodi.getCodeSub")], [T("connections.auth.kodi.pair"), T("connections.auth.kodi.pairSub")]],
    },
    link: {
      title: T("connections.auth.kodi.connectTitle"),
      copy: T("connections.auth.kodi.copyRpc"),
      steps: [[T("connections.auth.kodi.install"), T("connections.auth.kodi.installSub")], [T("connections.auth.kodi.link"), T("connections.auth.kodi.linkSub")], [T("connections.auth.kodi.confirm"), T("connections.auth.kodi.confirmSub")]],
    },
  };

  const ADDON_HTML = `
    <div class="kodi-addon">
      <div id="kodi_addon_live" class="kodi-addon-status" hidden>
        <div id="kodi_addon_info" class="muted"></div>
        <button id="kodi_addon_repair" class="btn" type="button">${TH("connections.auth.kodi.pairAgain")}</button>
        <div id="kodi_addon_msg" class="msg cw-connection-status-pill hidden" role="status" aria-live="polite"></div>
      </div>

      <div id="kodi_addon_sync" class="kodi-sync" hidden>
        <div class="kodi-sync-head">
          <strong>${TH("connections.auth.kodi.librarySync")}</strong>
          <span id="kodi_addon_sync_note" class="muted"></span>
        </div>
        <div id="kodi_addon_sync_rows" class="kodi-sync-rows"></div>
        <div class="muted">Removals follow the add-on setting Send removals made in Kodi.</div>
      </div>

      <div id="kodi_addon_link_row" hidden>
        <label for="kodi_addon_address">${TH("connections.auth.kodi.address")}</label>
        <div class="inp-row">
          <input id="kodi_addon_address" class="grow" autocomplete="off" spellcheck="false" autocapitalize="off" placeholder="http://host:8787">
          <button id="kodi_addon_link" class="btn kodi-addon-primary cw-connection-primary-action" type="button">${TH("connections.auth.kodi.linkAddon")}</button>
        </div>
        <div class="muted" id="kodi_addon_link_note" style="margin-top:6px"></div>
      </div>

      <details id="kodi_addon_alt">
        <summary id="kodi_addon_alt_sum" class="muted" hidden>${TH("connections.auth.kodi.otherWays")}</summary>
        <div class="kodi-addon kodi-addon-alt">
          <div id="kodi_addon_start" class="kodi-addon-status" hidden>
            <button id="kodi_addon_connect" class="btn" type="button">${TH("connections.auth.kodi.connectAddon")}</button>
            <span class="muted">${TH("connections.auth.kodi.showsCode")}</span>
          </div>

          <div id="kodi_addon_codecard" class="kodi-qc" hidden>
            <div class="kodi-qc-codewrap">
              <div class="kodi-qc-code" id="kodi_addon_code">------</div>
              <button id="kodi_addon_pair" class="btn" type="button">${TH("connections.auth.kodi.newCode")}</button>
              <button id="kodi_addon_cancel" class="btn danger" type="button">${TH("connections.auth.kodi.cancel")}</button>
            </div>
            <div class="kodi-qc-meta">
              <span class="muted">${TH("connections.auth.kodi.enterAddress")} <strong id="kodi_addon_address_text"></strong> ${TH("connections.auth.kodi.andCode")}</span>
              <span class="muted" id="kodi_addon_code_note"></span>
            </div>
          </div>

          <details id="kodi_addon_manual" hidden>
            <summary class="muted">${TH("connections.auth.kodi.manual")}</summary>
            <label for="kodi_addon_url" style="margin-top:8px;display:block">${TH("connections.auth.kodi.addonUrl")}</label>
            <div class="inp-row">
              <input id="kodi_addon_url" class="grow" readonly autocomplete="off" spellcheck="false">
              <button id="kodi_addon_copy" class="btn" type="button">${TH("connections.auth.copy")}</button>
              <button id="kodi_addon_regen" class="btn" type="button">${TH("connections.auth.kodi.regenerate")}</button>
            </div>
            <div class="muted" style="margin-top:6px">${TH("connections.auth.kodi.pasteUrl")}</div>
            <div id="kodi_addon_disable_row" class="inp-row" style="margin-top:12px" hidden>
              <button id="kodi_addon_disable" class="btn danger" type="button">${TH("connections.auth.kodi.turnOff")}</button>
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
  let syncFor = "";
  let syncBusy = false;

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
    if (!wanted) wanted = data.jsonrpc_connected && !data.paired ? "jsonrpc" : "addon";
    setMethod(wanted);
  }

  function addonAge(seconds) {
    if (seconds === null || seconds === undefined || seconds === "") return "";
    const n = Number(seconds);
    if (!Number.isFinite(n) || n < 0) return "";
    if (n < 90) return T("connections.auth.kodi.justNow");
    if (n < 5400) return T("connections.auth.kodi.minAgo", {n: Math.round(n / 60)});
    if (n < 129600) return T("connections.auth.kodi.hAgo", {n: Math.round(n / 3600)});
    return T("connections.auth.kodi.dAgo", {n: Math.round(n / 86400)});
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
    el("kodi_addon_cancel")?.addEventListener("click", () => { void updateAddon({ enabled: false }, T("connections.auth.kodi.pairingCancelled")); });
    el("kodi_addon_link")?.addEventListener("click", () => { void linkAddon(); });
    el("kodi_addon_copy")?.addEventListener("click", () => {
      void Shared.copyText(el("kodi_addon_url")?.value || "", el("kodi_addon_copy"), { copiedText: T("connections.auth.copied"), emptyMessage: T("connections.auth.stremio.noUrl") });
    });
    el("kodi_addon_regen")?.addEventListener("click", () => {
      if (window.confirm(T("connections.auth.kodi.confirmRegen"))) void updateAddon({ enabled: true, regenerate: true }, T("connections.auth.kodi.regenerated"));
    });
    el("kodi_addon_disable")?.addEventListener("click", () => {
      if (window.confirm(T("connections.auth.kodi.confirmOff"))) void updateAddon({ enabled: false }, T("connections.auth.kodi.turnedOff"));
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
    connectBtn.textContent = rpc ? T("connections.auth.kodi.usePairing") : T("connections.auth.kodi.connectAddon");
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
    el("kodi_addon_link").textContent = paired ? T("connections.auth.kodi.linkAgain") : T("connections.auth.kodi.linkAddon");
    el("kodi_addon_link_note").textContent = linking
      ? T("connections.auth.kodi.sentConfirm")
      : T("connections.auth.kodi.sendsAddress");
    el("kodi_addon_manual").hidden = !open;
    el("kodi_addon_disable_row").hidden = !paired;
    el("kodi_addon_url").value = open ? txt(data.url) : "";

    el("kodi_addon_code").textContent = code || "------";
    el("kodi_addon_code_note").textContent = code ? T("connections.auth.kodi.waitingAddon", {n: Math.max(1, Math.ceil(left / 60))}) : "";
    el("kodi_addon_address_text").textContent = txt(data.address);
    const address = el("kodi_addon_address");
    if (address && !txt(address.value) && document.activeElement !== address) address.value = txt(data.address);

    if (paired) {
      const seen = addonAge(data.age_seconds);
      let msg = T("connections.auth.kodi.addonConnected");
      if (!data.active && seen) msg = T("connections.auth.kodi.lastSeen", {when: seen});
      Shared.setStatus("kodi_addon_msg", true, msg);
      const info = [];
      if (txt(data.device_name)) info.push(txt(data.device_name));
      if (txt(data.addon_version)) info.push(T("connections.auth.kodi.addonVersion", {version: txt(data.addon_version)}));
      const viewers = Array.isArray(data.viewers) ? data.viewers.map(txt).filter(Boolean) : [];
      if (viewers.length) info.push(T("connections.auth.kodi.viewers", {list: viewers.join(", ")}));
      if (Number(data.pkc_skipped) > 0) info.push(T("connections.auth.kodi.pkcSkipped", {n: Number(data.pkc_skipped)}));
      el("kodi_addon_info").textContent = info.join(" | ");
    } else {
      Shared.setStatusPill("kodi_addon_msg", "hidden");
      el("kodi_addon_info").textContent = "";
    }

    if (paired && !rpc) {
      if (syncFor !== instance) {
        syncFor = instance;
        void loadSync();
      }
    } else {
      syncFor = "";
      renderSync(null);
    }

    if (paired && addonPaired === false && addonInstance === instance) note(T("connections.auth.kodi.kodiAddonConnected"));
    addonPaired = paired;
    addonInstance = instance;
    if (code) watchAddon(left);
  }

  function syncRow(viewer, targets) {
    const row = document.createElement("div");
    row.className = "kodi-sync-row";
    const name = document.createElement("span");
    name.className = "kodi-sync-name";
    name.textContent = txt(viewer.name) || txt(viewer.viewer_id);
    const state = document.createElement("span");
    state.className = "muted kodi-sync-state";
    const pairs = Array.isArray(viewer.pairs) ? viewer.pairs : [];
    const labels = pairs.map((p) => (p.enabled === false ? `${txt(p.label)} (${T("connections.auth.kodi.off")})` : txt(p.label))).filter(Boolean).join(", ");
    state.textContent = pairs.length ? `${T("connections.auth.kodi.syncsWith", {labels})}${viewer.ready ? "" : `, ${T("connections.auth.kodi.afterUpload")}`}` : T("connections.auth.kodi.notSynced");
    row.append(name, state);
    const used = new Set(pairs.map((p) => `${txt(p.provider)}|${txt(p.instance)}`));
    const free = targets.filter((t) => !used.has(`${txt(t.provider)}|${txt(t.instance)}`));
    if (!free.length) return row;
    const select = document.createElement("select");
    select.setAttribute("aria-label", T("connections.auth.kodi.syncWith", {name: name.textContent}));
    free.forEach((t) => {
      const opt = document.createElement("option");
      opt.value = `${txt(t.provider)}|${txt(t.instance)}`;
      opt.textContent = txt(t.label) || txt(t.provider);
      select.appendChild(opt);
    });
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "btn";
    btn.textContent = pairs.length ? T("connections.auth.kodi.add") : T("connections.auth.kodi.setUpSync");
    btn.addEventListener("click", () => { void setupSync(txt(viewer.viewer_id), select.value, btn); });
    row.append(select, btn);
    return row;
  }

  function renderSync(data) {
    const host = el("kodi_addon_sync");
    const rows = el("kodi_addon_sync_rows");
    if (!host || !rows) return;
    const viewers = Array.isArray(data?.viewers) ? data.viewers : [];
    if (!data || data.jsonrpc || !viewers.length) {
      host.hidden = true;
      rows.replaceChildren();
      return;
    }
    const targets = Array.isArray(data.targets) ? data.targets : [];
    let msg = T("connections.auth.kodi.pickWhere");
    if (!targets.length) msg = T("connections.auth.kodi.connectService");
    if (!data.sync_on) msg = T("connections.auth.kodi.turnOnSync");
    el("kodi_addon_sync_note").textContent = msg;
    rows.replaceChildren(...viewers.map((v) => syncRow(v, targets)));
    host.hidden = false;
  }

  async function loadSync() {
    if (!el("kodi_addon_sync")) return;
    try {
      const r = await fetchJSON(api("/api/kodi/addon/sync"), { cache: "no-store" });
      renderSync(r.ok && r.data && r.data.ok ? r.data : null);
    } catch {
      renderSync(null);
    }
  }

  async function setupSync(viewerId, value, btn) {
    const raw = String(value || "");
    const cut = raw.indexOf("|");
    const target = cut > 0 ? raw.slice(0, cut) : "";
    if (syncBusy || !viewerId || !target) return;
    syncBusy = true;
    if (btn) btn.disabled = true;
    try {
      const r = await fetchJSON(api("/api/kodi/addon/sync-setup"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ viewer_id: viewerId, target, target_instance: raw.slice(cut + 1) || "default" }),
        cache: "no-store",
      });
      if (!r.ok || !r.data || r.data.ok === false) throw new Error(txt(r.data?.error) || T("connections.auth.kodi.setupFailed"));
      renderSync(r.data);
      note(T("connections.auth.kodi.syncSetUp"));
    } catch (e) {
      note(e && e.message ? e.message : T("connections.auth.kodi.setupFailed"));
    } finally {
      syncBusy = false;
      if (btn) btn.disabled = false;
    }
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
      if (!r.ok || !r.data || r.data.ok === false) throw new Error(txt(r.data?.error) || T("connections.auth.kodi.codeFailed"));
      renderAddon(r.data);
    } catch (e) {
      note(e && e.message ? e.message : T("connections.auth.kodi.codeFailed"));
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
      if (!r.ok || !r.data || r.data.ok === false) throw new Error(txt(r.data?.error) || T("connections.auth.kodi.reachFailed"));
      note(T("connections.auth.kodi.sent"));
      addonLinkUntil = Date.now() + 120000;
      watchAddon(120);
      await loadAddon();
    } catch (e) {
      note(e && e.message ? e.message : T("connections.auth.kodi.reachFailed"));
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
      if (!r.ok || !r.data || r.data.ok === false) throw new Error(txt(r.data?.error) || T("connections.auth.kodi.updateFailed"));
      addonPaired = null;
      renderAddon(r.data);
      if (done) note(done);
    } catch (e) {
      note(e && e.message ? e.message : T("connections.auth.kodi.updateFailed"));
      void loadAddon();
    }
  }

  function friendlyError(data) {
    const reason = txt(data?.reason || data?.error);
    switch (reason) {
      case "missing_server": return T("connections.auth.kodi.enterUrl");
      case "invalid_credentials": return T("connections.auth.gen.rejectedCreds", {provider: "Kodi"});
      case "unreachable": return T("connections.auth.kodi.unreachable");
      case "not_kodi": return T("connections.auth.kodi.notKodi");
      case "version_too_old": return T("connections.auth.kodi.version");
      case "jsonrpc_too_old": return T("connections.auth.kodi.jsonrpc");
      case "invalid_response": return T("connections.validation.returnedUnexpected", {provider: "Kodi"});
      default: return txt(data?.error) || T("connections.auth.gen.connectionFailed", {provider: "Kodi"});
    }
  }

  async function hydrate() {
    profile?.ensureUI(() => { void hydrate(); });
    const inst = profile?.getInstance?.() || "default";
    loadedInstance = null;
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
    loadedInstance = (profile?.getInstance?.() || "default") === inst ? inst : null;
    syncHidden();
    renderLibraries(lastLibraries);
    syncFor = "";
    void loadAddon();

    try {
      const r = await fetchJSON(api("/api/kodi/status"), { cache: "no-store" });
      const data = r.data || {};
      Shared.maskSecret(el("kodi_password"), !!data.has_password);
      const ok = !!(r.ok && data.connected);
      const version = txt(data.kodi_version);
      setConn(ok, ok ? (version ? T("connections.auth.kodi.connectedVersion", {version}) : T("connections.auth.connected")) : T("connections.auth.notConnected"));
      if (ok) void loadLibraries();
    } catch {
      Shared.maskSecret(el("kodi_password"), false);
      setConn(false, T("connections.auth.notConnected"));
    }
  }

  async function onConnect() {
    const server = txt(el("kodi_server")?.value || "");
    const username = txt(el("kodi_username")?.value || "");
    const passInfo = Shared.readSecretField(el("kodi_password"));
    const verify_ssl = !!el("kodi_verify_ssl")?.checked;
    if (!server) {
      note(T("connections.auth.kodi.enterUrl"));
      return;
    }

    try {
      setConn(false, T("connections.auth.connecting"));
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
      setConn(true, version ? T("connections.auth.kodi.connectedVersion", {version}) : T("connections.auth.connected"));
      await loadLibraries(true);
      note(T("connections.auth.gen.connected", {provider: "Kodi"}));
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      const msg = e && e.message ? e.message : T("connections.auth.gen.connectionFailed", {provider: "Kodi"});
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
      setConn(false, T("connections.auth.notConnected"));
      await loadAddon();
      note(T("connections.auth.gen.disconnected", {provider: "Kodi"}));
      await window.CW?.ProvidersUI?.refreshAuthPresentation?.(true);
    } catch (e) {
      note(T("connections.auth.gen.disconnectFailed", {provider: "Kodi"}) + (e && e.message ? ": " + e.message : ""));
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
    if (loadedInstance !== inst) return;
    syncHidden();
    k.history = Object.assign({}, k.history || {}, { libraries: Array.from(H) });
    k.ratings = Object.assign({}, k.ratings || {}, { libraries: Array.from(R) });
    k.progress = Object.assign({}, k.progress || {}, { libraries: Array.from(P) });
    k.collection = Object.assign({}, k.collection || {}, { libraries: Array.from(C) });
    k.scrobble = Object.assign({}, k.scrobble || {}, { libraries: Array.from(S) });
  });

  window.cwAuth = window.cwAuth || {};
  window.cwAuth.kodi = {
    init: boot,
    hydrate,
    whitelist: {
      read: (inst) => loadedInstance === inst ? { hist: [...H], rate: [...R], prog: [...P], coll: [...C], scr: [...S] } : null,
      write: (inst, sets) => {
        if (loadedInstance !== inst) return;
        H = new Set((sets?.hist || []).map(String));
        R = new Set((sets?.rate || []).map(String));
        P = new Set((sets?.prog || []).map(String));
        C = new Set((sets?.coll || []).map(String));
        S = new Set((sets?.scr || []).map(String));
      },
    },
  };
  boot();
})();
