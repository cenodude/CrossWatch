// scrobbler.js — CrossWatch Scrobbler UI (Webhook, Watcher, and Plex Server helpers)

(function (w, d) {
  // -------- HTTP helpers --------
  async function fetchJSON(url, opt) {
    const r = await fetch(url, Object.assign({ cache: "no-store" }, opt || {}));
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }
  async function fetchText(url, opt) {
    const r = await fetch(url, Object.assign({ cache: "no-store" }, opt || {}));
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.text();
  }

  // -------- DOM helpers --------
  const $    = (sel, root) => (root || d).querySelector(sel);
  const $all = (sel, root) => Array.from((root || d).querySelectorAll(sel));
  const el   = (tag, attrs) => { const x = d.createElement(tag); if (attrs) Object.assign(x, attrs); return x; };
  const on   = (node, ev, fn) => node && node.addEventListener(ev, fn);

  function setNote(id, msg, kind) {
    const n = d.getElementById(id);
    if (!n) return;
    n.textContent = msg || "";
    n.style.color = kind === "err" ? "#ff6b6b" : "var(--muted, #a7a7a7)";
    n.style.margin = "6px 0 2px";
    n.style.fontSize = "12px";
    n.style.opacity = "0.9";
  }

  // -------- Tiny CSS for basic alignment and appearance --------
  function injectStyles() {
    if (d.getElementById("sc-styles")) return;
    const css = `
    .sc-row{display:grid;grid-template-columns:1fr auto;gap:16px;align-items:center}
    .sc-status-line{display:flex;align-items:center;gap:10px;min-height:32px}
    .sc-actions{display:flex;flex-direction:column;align-items:flex-end;gap:8px}
    .sc-btnrow{display:flex;gap:8px}
    .sc-toggle{display:inline-flex;align-items:center;gap:8px;white-space:nowrap}
    .badge{padding:4px 10px;border-radius:999px;font-weight:600;opacity:.9}
    .badge.is-on{background:#0a3;color:#fff}
    .badge.is-off{background:#333;color:#bbb;border:1px solid #444}
    .status-dot{width:10px;height:10px;border-radius:50%;display:inline-block}
    .status-dot.on{background:#22c55e}
    .status-dot.off{background:#ef4444}
    `;
    const s = d.createElement("style");
    s.id = "sc-styles"; s.textContent = css; d.head.appendChild(s);
  }

  // -------- Component state --------
  const STATE = {
    mount: null,
    webhookHost: null,
    watcherHost: null,
    cfg: {},
    users: [],
    pms: [],
    pollTimer: null,
  };

  // -------- Configuration helpers --------
  function deepSet(obj, path, val) {
    const parts = path.split(".");
    let o = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      const k = parts[i];
      if (!o[k] || typeof o[k] !== "object") o[k] = {};
      o = o[k];
    }
    o[parts[parts.length - 1]] = val;
  }
  function read(path, dflt) {
    let v = STATE.cfg;
    for (const k of path.split(".")) {
      if (!v || typeof v !== "object") return dflt;
      v = v[k];
    }
    return v == null ? dflt : v;
  }
  function write(path, val) {
    deepSet(STATE.cfg, path, val);
    try {
      if (!w._cfgCache || typeof w._cfgCache !== "object") w._cfgCache = {};
      if (path.startsWith("plex.")) {
        if (!w._cfgCache.plex || typeof w._cfgCache.plex !== "object") w._cfgCache.plex = {};
        const key = path.slice("plex.".length);
        deepSet(w._cfgCache, path, val);
        if (key === "server_url") w._cfgCache.plex.server_url = val;
      } else {
        deepSet(w._cfgCache, path, val);
      }
    } catch {}
    syncHiddenServerUrl();
  }
  const asArray = (v) => Array.isArray(v) ? v.slice() : (v == null || v === "" ? [] : [String(v)]);

  // -------- API helpers (server endpoints) --------
  const API = {
    cfgGet: () => fetchJSON("/api/config"),
    users: async () => {
      const j = await fetchJSON("/api/plex/users");
      const list = Array.isArray(j) ? j : (Array.isArray(j?.users) ? j.users : []);
      return Array.isArray(list) ? list : [];
    },
    serverUUID: () => fetchJSON("/api/plex/server_uuid"),
    pms: async () => {
      const j = await fetchJSON("/api/plex/pms");
      const list = Array.isArray(j) ? j : (Array.isArray(j?.servers) ? j.servers : []);
      return Array.isArray(list) ? list : [];
    },
    watch: {
      status: () => fetchJSON("/debug/watch/status"),
      start:  () => fetchText("/debug/watch/start", { method: "POST" }),
      stop:   () => fetchText("/debug/watch/stop",  { method: "POST" }),
    },

  };

  // -------- UI helpers / small components --------
  function chip(label, onRemove) {
    const c = el("span", { className: "chip" });
    const t = el("span"); t.textContent = label;
    const rm = el("span", { className: "rm", title: "Remove" }); rm.textContent = "×";
    on(rm, "click", () => onRemove && onRemove(label));
    c.append(t, rm);
    return c;
  }

  function setWatcherStatus(ui) {
    const alive = !!ui?.alive;
    const dot = $("#sc-status-dot", STATE.mount);
    const txt = $("#sc-status-text", STATE.mount);
    const badge = $("#sc-status-badge", STATE.mount);
    if (dot) {
      dot.classList.remove("on", "off");
      dot.classList.add(alive ? "on" : "off");
    }
    if (txt) txt.textContent = alive ? "Active" : "Inactive";
    if (badge) {
      badge.textContent = alive ? "Active" : "Stopped";
      badge.classList.toggle("is-on", alive);
      badge.classList.toggle("is-off", !alive);
    }
    const last = $("#sc-status-last", STATE.mount);
    if (last) last.textContent = ui?.lastSeen ? `Last seen: ${ui.lastSeen}` : "";
    const up = $("#sc-status-up", STATE.mount);
    if (up) up.textContent = ui?.uptime ? `Uptime: ${ui.uptime}` : "";
  }

  function applyModeDisable() {
    const wh = $("#sc-enable-webhook", STATE.mount);
    const wa = $("#sc-enable-watcher", STATE.mount);
    const webhookOn = !!wh?.checked;
    const watcherOn = !!wa?.checked;

    write("scrobble.enabled", webhookOn || watcherOn);
    write("scrobble.mode", watcherOn ? "watch" : "webhook");

    $all("#sc-sec-webhook .input, #sc-sec-webhook input, #sc-sec-webhook button", STATE.mount)
      .forEach(n => n.disabled = !webhookOn && n.id !== "sc-enable-webhook");
    $all("#sc-sec-watch .input, #sc-sec-watch input, #sc-sec-watch button, #sc-sec-watch select", STATE.mount)
      .forEach(n => n.disabled = !watcherOn && n.id !== "sc-enable-watcher");

    const srv = String(read("plex.server_url", "") || "");
    if (watcherOn) {
      if (!isValidServerUrl(srv)) {
        setNote("sc-pms-note", "Plex Server is required (http(s)://…)", "err");
      } else {
        setNote("sc-pms-note", `Using ${srv}`);
      }
    } else {
      setNote("sc-pms-note", "");
    }
  }

  function isValidServerUrl(v) {
    if (!v) return false;
    try {
      const u = new URL(v);
      return (u.protocol === "http:" || u.protocol === "https:") && !!u.host;
    } catch { return false; }
  }

  // -------- Layout (Server card left, Status card right) --------
  function buildUI() {
    injectStyles();

  // Webhook section (UI for webhook mode)
    if (STATE.webhookHost) {
      STATE.webhookHost.innerHTML = `
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
          <label style="display:inline-flex;gap:8px;align-items:center">
            <input type="checkbox" id="sc-enable-webhook"> Enable
          </label>
        </div>
        <div class="muted">Endpoint</div>
        <div style="display:flex; gap:8px; align-items:center">
          <code id="sc-webhook-url">/webhook/trakt</code>
          <button id="sc-copy-endpoint" class="btn small">Copy</button>
        </div>
        <div class="micro-note" style="margin-top:8px">
          Webhooks can be flaky; <strong>Watcher</strong> is recommended. Only one mode active at a time.
        </div>
      `;
    }

  // Watcher section (Plex server discovery and watcher controls)
    if (STATE.watcherHost) {
      STATE.watcherHost.innerHTML = `
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
          <label style="display:inline-flex;gap:8px;align-items:center">
            <input type="checkbox" id="sc-enable-watcher"> Enable
          </label>
        </div>

        <div class="watcher-row" style="display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:start">
          <!-- Server Card (left) -->
          <div class="card" id="sc-card-server" style="padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset;">
            <div class="h" style="display:flex;justify-content:space-between;align-items:center">
              <div>Plex Server <span class="pill req">required</span></div>
              <button id="sc-pms-refresh" class="btn small">Fetch</button>
            </div>
            <div id="sc-pms-note" class="micro-note" style="margin-top:6px"></div>

            <div style="margin-top:8px">
              <div class="muted">Discovered servers</div>
              <select id="sc-pms-select" class="input" style="width:100%;margin-top:6px">
                <option value="">— select a server —</option>
              </select>
            </div>

            <div style="margin-top:12px">
              <div class="muted">Manual URL (http(s)://host[:port])</div>
              <input id="sc-pms-input" class="input" placeholder="https://192.168.1.10:32400" />
            </div>
          </div>

          <!-- Status Card (right) -->
          <div class="card" id="sc-card-status" style="padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset;">
            <div class="h" style="display:flex;justify-content:space-between;align-items:center">
              <div>Watcher Status</div>
              <span id="sc-status-badge" class="badge is-off">Stopped</span>
            </div>

            <div class="sc-row" style="margin-top:10px">
              <div class="sc-status-line">
                <span id="sc-status-dot" class="status-dot off"></span>
                <span class="muted">Status:</span>
                <span id="sc-status-text" class="status-text">Unknown</span>
              </div>

              <div class="sc-actions">
                <div class="sc-btnrow">
                  <button id="sc-watch-start" class="btn small">Start</button>
                  <button id="sc-watch-stop"  class="btn small">Stop</button>
                  <button id="sc-watch-refresh" class="btn small">Refresh</button>
                </div>
                <label class="sc-toggle"><input type="checkbox" id="sc-autostart"> Autostart on boot</label>
              </div>
            </div>

            <div id="sc-status-msg" style="margin-top:6px;font-weight:600;font-size:13px"></div>

            <div id="sc-status-last" class="micro-note" style="margin-top:8px"></div>
            <div id="sc-status-up" class="micro-note"></div>

          </div>

  <!-- Advanced settings -->
        <details style="margin-top:14px">
          <summary>Advanced</summary>

          <div class="h" style="margin-top:12px">Filters</div>
          <div class="muted">Username whitelist</div>
          <div id="sc-whitelist" class="chips" style="margin-top:4px"></div>
          <div id="sc-users-note" class="micro-note"></div>
          <div style="display:flex; gap:8px; margin-top:6px">
            <input id="sc-user-input" class="input" placeholder="Add username..." style="flex:1">
            <button id="sc-add-user" class="btn small">Add</button>
            <button id="sc-load-users" class="btn small">Load Plex users</button>
          </div>

          <div class="h" style="margin-top:12px">Server UUID (optional)</div>
          <div id="sc-uuid-note" class="micro-note"></div>
          <div style="display:flex; gap:8px; align-items:center">
            <input id="sc-server-uuid" class="input" placeholder="e.g. abcd1234..." style="flex:1">
            <button id="sc-fetch-uuid" class="btn small">Fetch</button>
          </div>
        </details>
      `;
    }

  // Add a hidden input so generic savers will persist root-level plex.server_url
    ensureHiddenServerUrlInput();

  // Interactions — webhook controls
    on($("#sc-copy-endpoint", STATE.mount), "click", () => {
      try { navigator.clipboard.writeText(`${location.origin}/webhook/trakt`); setNote("sc-users-note", "Endpoint copied"); }
      catch { setNote("sc-users-note", "Copy failed", "err"); }
    });

  // Watcher action bindings
    on($("#sc-add-user", STATE.mount), "click", onAddUser);
    on($("#sc-load-users", STATE.mount), "click", loadUsers);
    on($("#sc-watch-start", STATE.mount), "click", onWatchStart);
    on($("#sc-watch-stop", STATE.mount), "click", onWatchStop);
    on($("#sc-watch-refresh", STATE.mount), "click", () => { refreshWatcher(); refreshWatchLogs(); });
    on($("#sc-fetch-uuid", STATE.mount), "click", fetchServerUUID);

  // Plex Media Server (PMS) interactions
    on($("#sc-pms-refresh", STATE.mount), "click", loadPmsList);
    on($("#sc-pms-select", STATE.mount), "change", e => {
      const v = String(e.target.value || "").trim();
      if (v) {
        $("#sc-pms-input", STATE.mount).value = v;
        write("plex.server_url", v);
        try {
          if (!w._cfgCache) w._cfgCache = {};
          if (!w._cfgCache.plex) w._cfgCache.plex = {};
          w._cfgCache.plex.server_url = v;
        } catch {}
        setNote("sc-pms-note", `Using ${v}`);
      }
      applyModeDisable();
    });
    on($("#sc-pms-input", STATE.mount), "input", e => {
      const v = String(e.target.value || "").trim();
      write("plex.server_url", v);
      try {
        if (!w._cfgCache) w._cfgCache = {};
        if (!w._cfgCache.plex) w._cfgCache.plex = {};
        w._cfgCache.plex.server_url = v;
      } catch {}
      if (v && !isValidServerUrl(v)) {
        setNote("sc-pms-note", "Invalid URL. Use http(s)://host[:port]", "err");
      } else if (v) {
        setNote("sc-pms-note", `Using ${v}`);
      } else {
        setNote("sc-pms-note", "Plex Server is required when Watcher is enabled", "err");
      }
      applyModeDisable();
    });

  // Make webhook and watcher mutually exclusive (only one active at a time)
    const wh = $("#sc-enable-webhook", STATE.mount);
    const wa = $("#sc-enable-watcher", STATE.mount);
    const syncExclusive = (src) => {
      const webOn = !!wh?.checked, watOn = !!wa?.checked;
      if (src === "webhook" && webOn && wa) wa.checked = false;
      if (src === "watch"   && watOn && wh) wh.checked = false;
      write("scrobble.enabled", (!!wh?.checked) || (!!wa?.checked));
      write("scrobble.mode", (!!wa?.checked) ? "watch" : "webhook");
      applyModeDisable();
    };
    if (wh) on(wh, "change", () => syncExclusive("webhook"));
    if (wa) on(wa, "change", () => syncExclusive("watch"));
    on($("#sc-autostart", STATE.mount), "change", e => write("scrobble.watch.autostart", !!e.target.checked));
    on($("#sc-server-uuid", STATE.mount), "input",  e => write("scrobble.watch.filters.server_uuid", String(e.target.value || "").trim()));
  }

  // Hidden input helpers (used by root-level save)
  function ensureHiddenServerUrlInput() {
    let hidden = d.getElementById("cfg-plex-server-url");
    const form = d.querySelector("form#settings, form#settings-form, form[data-settings]") || (STATE.mount || d.body);
    if (!hidden) {
      hidden = d.createElement("input");
      hidden.type = "hidden";
      hidden.id = "cfg-plex-server-url";
      hidden.name = "plex.server_url";
      form.appendChild(hidden);
    }
    syncHiddenServerUrl();
  }
  function syncHiddenServerUrl() {
    const h = d.getElementById("cfg-plex-server-url");
    if (h) h.value = String(read("plex.server_url", "") || "");
  }

  // -------- Populate UI from configuration --------
  function populate() {
    const enabled   = !!read("scrobble.enabled", false);
    const mode      = String(read("scrobble.mode", "webhook")).toLowerCase();
    const wl        = asArray(read("scrobble.watch.filters.username_whitelist", []));
    const su        = read("scrobble.watch.filters.server_uuid", "");
    const autostart = !!read("scrobble.watch.autostart", false);
    const serverUrl = String(read("plex.server_url", "") || "");

    const useWebhook = enabled && mode === "webhook";
    const useWatch   = enabled && mode === "watch";

    const wh = $("#sc-enable-webhook", STATE.mount);
    const wa = $("#sc-enable-watcher", STATE.mount);
    if (wh) wh.checked = useWebhook;
    if (wa) wa.checked = useWatch;

    const host = $("#sc-whitelist", STATE.mount);
    if (host) {
      host.innerHTML = "";
      wl.forEach(u => host.append(chip(u, removeUser)));
    }

    const suInp = $("#sc-server-uuid", STATE.mount);
    if (suInp) suInp.value = su || "";

    const auto = $("#sc-autostart", STATE.mount);
    if (auto) auto.checked = !!autostart;

    const pmsInp = $("#sc-pms-input", STATE.mount);
    if (pmsInp) pmsInp.value = serverUrl;

    try {
      if (!w._cfgCache) w._cfgCache = {};
      if (!w._cfgCache.plex) w._cfgCache.plex = {};
      w._cfgCache.plex.server_url = serverUrl;
    } catch {}

    syncHiddenServerUrl();
    applyModeDisable();
    refreshWatcher();
    loadPmsList().catch(() => {});
  }

  // -------- Actions / event handlers --------
  async function refreshWatcher() {
    try {
      const s = await API.watch.status();
      setWatcherStatus(s || {});
    } catch {
      setWatcherStatus({ alive: false });
    }
  }

  function setStatusMsg(msg, ok = true) {
    const n = document.getElementById("sc-status-msg");
    if (!n) return;
    n.textContent = msg || "";
    n.style.color = ok ? "var(--fg,#fff)" : "#ff6b6b";
  }

  async function onWatchStart() {
    const srv = String(read("plex.server_url", "") || "");
    if (!isValidServerUrl(srv)) {
      setNote("sc-pms-note", "Plex Server is required (http(s)://…)", "err");
      return;
    }
    try {
      await API.watch.start();
    } catch {
      setNote("sc-pms-note", "Start failed", "err");
    }
    refreshWatcher();
  }

  async function onWatchStop() {
    try {
      await API.watch.stop();
    } catch {
      setNote("sc-pms-note", "Stop failed", "err");
    }
    refreshWatcher();
  }

  async function fetchServerUUID() {
    try {
      const j = await API.serverUUID();
      const v = j?.server_uuid || j?.uuid || j?.id || "";
      const inp = $("#sc-server-uuid", STATE.mount);
      if (inp && v) { inp.value = v; write("scrobble.watch.filters.server_uuid", v); setNote("sc-uuid-note", "Server UUID fetched"); }
      else setNote("sc-uuid-note", "No server UUID", "err");
    } catch {
      setNote("sc-uuid-note", "Fetch failed", "err");
    }
  }

  async function loadPmsList() {
    try {
      const sel = $("#sc-pms-select", STATE.mount);
      if (!sel) return;
      sel.innerHTML = `<option value="">Loading…</option>`;
      const list = await API.pms();
      STATE.pms = list;

      sel.innerHTML = `<option value="">— select a server —</option>`;
      for (const s of list) {
        const best = s.best_url || "";
        const name = s.name || s.product || "Plex Media Server";
        const owned = s.owned ? " (owned)" : "";
        const label = best ? `${name}${owned} — ${best}` : name + owned;
        const opt = el("option", { value: best || "", textContent: label });
        sel.append(opt);
      }
      setNote("sc-pms-note", list.length ? "Pick a discovered server or enter a URL" : "No servers discovered. Enter a URL.", list.length ? null : "err");
    } catch {
      const sel = $("#sc-pms-select", STATE.mount);
      if (sel) sel.innerHTML = `<option value="">— select a server —</option>`;
      setNote("sc-pms-note", "Fetch failed. Enter a URL manually.", "err");
    }
  }

  function onAddUser() {
    const inp = $("#sc-user-input", STATE.mount);
    const v = String((inp?.value || "").trim());
    if (!v) return;
    const cur = asArray(read("scrobble.watch.filters.username_whitelist", []));
    if (!cur.includes(v)) {
      cur.push(v);
      write("scrobble.watch.filters.username_whitelist", cur);
      const host = $("#sc-whitelist", STATE.mount);
      host.append(chip(v, removeUser));
      inp.value = "";
    }
  }
  function removeUser(u) {
    const cur = asArray(read("scrobble.watch.filters.username_whitelist", []));
    const next = cur.filter(x => String(x) !== String(u));
    write("scrobble.watch.filters.username_whitelist", next);
    const host = $("#sc-whitelist", STATE.mount);
    host.innerHTML = "";
    next.forEach(v => host.append(chip(v, removeUser)));
  }

  async function loadUsers() {
    try {
      const list = await API.users();
      const filtered = list.filter(u => {
        const t = String(u?.type || "").toLowerCase();
        return t === "managed" || t === "owner" || u?.owned === true || u?.isHomeUser === true;
      });

      STATE.users = filtered;
      const names = filtered.map(u => u?.username || u?.title).filter(Boolean);

      const host = $("#sc-whitelist", STATE.mount);
      let added = 0;
      for (const n of names) {
        const cur = asArray(read("scrobble.watch.filters.username_whitelist", []));
        if (!cur.includes(n)) {
          cur.push(n);
          write("scrobble.watch.filters.username_whitelist", cur);
          host.append(chip(n, removeUser));
          added++;
        }
      }
      setNote("sc-users-note", added ? `Loaded ${added} user(s)` : "No eligible managed/owner users");
    } catch {
      setNote("sc-users-note", "Load users failed", "err");
    }
  }

  // -------- Public API --------
  function init(opts = {}) {
    STATE.mount = opts.mountId ? d.getElementById(opts.mountId) : d;
    STATE.cfg = opts.cfg || w._cfgCache || {};

    STATE.webhookHost = $("#scrob-webhook", STATE.mount);
    STATE.watcherHost = $("#scrob-watcher", STATE.mount);

    // Fallback mounts if missing
    if (!STATE.webhookHost || !STATE.watcherHost) {
      const root = STATE.mount || d.body;
      const makeSec = (id, title) => {
        const sec = el("div", { className: "section", id });
        sec.innerHTML = `<div class="head"><strong>${title}</strong></div><div class="body"><div id="${id === "sc-sec-webhook" ? "scrob-webhook" : "scrob-watcher"}"></div></div>`;
        root.append(sec);
      };
      if (!STATE.webhookHost) { makeSec("sc-sec-webhook", "Webhook"); STATE.webhookHost = $("#scrob-webhook", STATE.mount); }
      if (!STATE.watcherHost) { makeSec("sc-sec-watch", "Watcher");  STATE.watcherHost = $("#scrob-watcher", STATE.mount); }
    }

    buildUI();
    populate();
  }

  // Backwards-compatibility: legacy mount(targetEl, cfg)
  function mountLegacy(targetEl, initialCfg) {
    STATE.mount = targetEl || d;
    STATE.cfg = initialCfg || (w._cfgCache || {});
    STATE.webhookHost = $("#scrob-webhook", STATE.mount) || targetEl;
    STATE.watcherHost = $("#scrob-watcher", STATE.mount) || targetEl;
    buildUI();
    populate();
  }

  // Return only the scrobble config block (do not nest plex here)
  function getScrobbleConfig() {
    const enabled   = !!read("scrobble.enabled", false);
    const mode      = String(read("scrobble.mode", "webhook")).toLowerCase();
    const wl        = asArray(read("scrobble.watch.filters.username_whitelist", []));
    const su        = read("scrobble.watch.filters.server_uuid", "");
    const autostart = !!read("scrobble.watch.autostart", false);

    return {
      enabled,
      mode: mode === "watch" ? "watch" : "webhook",
      webhook: {},
      watch: {
        autostart,
        filters: {
          username_whitelist: wl,
          server_uuid: su || ""
        }
      }
    };
  }

  // Root patch for saver to merge into config.json (ensures plex.server_url lives at root)
  function getRootPatch() {
    const serverUrl = String(read("plex.server_url", "") || "");
    return { plex: { server_url: serverUrl } };
  }

  // Expose
  w.Scrobbler = { init, mount: mountLegacy, getConfig: getScrobbleConfig, getRootPatch };
  w.getScrobbleConfig = getScrobbleConfig;
  w.getRootPatch = getRootPatch;

  // Auto-init
  d.addEventListener("DOMContentLoaded", async () => {
    const root = d.getElementById("scrobble-mount");
    if (!root) return;
    let cfg = null;
    try { cfg = await API.cfgGet(); } catch { cfg = w._cfgCache || {}; }
    init({ mountId: "scrobble-mount", cfg });
  });
})(window, document);
