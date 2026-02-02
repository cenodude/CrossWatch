// auth.plex.js - Plex auth
(function (w, d) {

  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;
  const exists = (sel) => !!q(sel);
  function waitFor(sel, timeout = 12000) {
    return new Promise((res) => {
      const end = Date.now() + timeout;
      (function loop() {
        if (exists(sel)) return res(q(sel));
        if (Date.now() > end) return res(null);
        requestAnimationFrame(loop);
      })();
    });
  }


  const PLEX_SUBTAB_KEY = "cw.ui.plex.auth.subtab.v1";

const PLEX_INSTANCE_KEY = "cw.ui.plex.auth.instance.v1";

function getPlexInstance() {
  const el = $("plex_instance");
  let v = el ? String(el.value || "").trim() : "";
  if (!v) { try { v = localStorage.getItem(PLEX_INSTANCE_KEY) || ""; } catch {} }
  v = (v || "").trim() || "default";
  return v.toLowerCase() === "default" ? "default" : v;
}

function setPlexInstance(v) {
  const id = (String(v || "").trim() || "default");
  try { localStorage.setItem(PLEX_INSTANCE_KEY, id); } catch {}
  const el = $("plex_instance");
  if (el) el.value = id;
}

function plexApi(path) {
  const p = String(path || "");
  const sep = p.includes("?") ? "&" : "?";
  return p + sep + "instance=" + encodeURIComponent(getPlexInstance()) + "&ts=" + Date.now();
}

function getPlexCfgBlock(cfg) {
  cfg = cfg || {};
  const base = (cfg.plex && typeof cfg.plex === "object") ? cfg.plex : (cfg.plex = {});
  const inst = getPlexInstance();
  if (inst === "default") return base;
  if (!base.instances || typeof base.instances !== "object") base.instances = {};
  if (!base.instances[inst] || typeof base.instances[inst] !== "object") base.instances[inst] = {};
  return base.instances[inst];
}

async function refreshPlexInstanceOptions(preserve = true) {
  const sel = $("plex_instance");
  if (!sel) return;
  let want = preserve ? getPlexInstance() : "default";
  try {
    const r = await fetch("/api/provider-instances/plex?ts=" + Date.now(), { cache: "no-store" });
    const arr = await r.json().catch(() => []);
    const opts = Array.isArray(arr) ? arr : [];

    sel.innerHTML = "";
    const addOpt = (id, label) => {
      const o = d.createElement("option");
      o.value = String(id);
      o.textContent = String(label || id);
      sel.appendChild(o);
    };

    addOpt("default", "Default");
    opts.forEach((o) => { if (o && o.id && o.id !== "default") addOpt(o.id, o.label || o.id); });

    if (!Array.from(sel.options).some(o => o.value === want)) want = "default";
    sel.value = want;
    setPlexInstance(want);
  } catch {}
}

function ensurePlexInstanceUI() {
  const panel = q('#sec-plex .cw-meta-provider-panel[data-provider="plex"]');
  const head = panel ? q(".cw-panel-head", panel) : null;
  if (!head || head.__plexInstanceUI) return;
  head.__plexInstanceUI = true;

  const wrap = d.createElement("div");
  wrap.className = "inline";
  wrap.style.display = "flex";
  wrap.style.gap = "8px";
  wrap.style.alignItems = "center";
  wrap.title = "Select which Plex account this config applies to.";

  const lab = d.createElement("span");
  lab.className = "muted";
  lab.textContent = "Profile";

  const sel = d.createElement("select");
  sel.id = "plex_instance";
  sel.className = "input";
  sel.style.minWidth = "160px";

  const btnNew = d.createElement("button");
  btnNew.type = "button";
  btnNew.className = "btn secondary";
  btnNew.id = "plex_instance_new";
  btnNew.textContent = "New";

  const btnDel = d.createElement("button");
  btnDel.type = "button";
  btnDel.className = "btn secondary";
  btnDel.id = "plex_instance_del";
  btnDel.textContent = "Delete";

  wrap.appendChild(lab);
  wrap.appendChild(sel);
  wrap.appendChild(btnNew);
  wrap.appendChild(btnDel);
  head.appendChild(wrap);

  refreshPlexInstanceOptions(true);

  sel.addEventListener("change", async () => {
    setPlexInstance(sel.value);
    try { await hydratePlexFromConfigRaw(); } catch {}
    try { refreshPlexLibraries(); } catch {}
    try { mountPlexUserPicker(); } catch {}
    try { schedulePlexPmsProbe(200); } catch {}
  });

  btnNew.addEventListener("click", async () => {
    try {
      const r = await fetch(`/api/provider-instances/plex/next?ts=${Date.now()}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}", cache: "no-store" });
      const j = await r.json().catch(() => ({}));
      const id = String(j?.id || "").trim();
      if (!r.ok || j?.ok === false || !id) throw new Error(String(j?.error || "create_failed"));
      setPlexInstance(id);
      await refreshPlexInstanceOptions(true);
      sel.value = id;
      try { await hydratePlexFromConfigRaw(); } catch {}
      try { refreshPlexLibraries(); } catch {}
      try { mountPlexUserPicker(); } catch {}
      try { schedulePlexPmsProbe(200); } catch {}
    } catch (e) {
      notify("Could not create profile: " + (e?.message || e));
    }
  });

  btnDel.addEventListener("click", async () => {
    const id = getPlexInstance();
    if (id === "default") return notify("Default profile cannot be deleted.");
    if (!confirm(`Delete Plex profile "${id}"?`)) return;
    try {
      const r = await fetch(`/api/provider-instances/plex/${encodeURIComponent(id)}`, { method: "DELETE", cache: "no-store" });
      const j = await r.json().catch(() => ({}));
      if (!r.ok || j?.ok === false) throw new Error(String(j?.error || "delete_failed"));
      setPlexInstance("default");
      await refreshPlexInstanceOptions(false);
      sel.value = "default";
      try { await hydratePlexFromConfigRaw(); } catch {}
    } catch (e) {
      notify("Could not delete profile: " + (e?.message || e));
    }
  });
}


  function plexAuthSubSelect(tab, opts = {}) {
    const root = q('#sec-plex .cw-meta-provider-panel[data-provider="plex"]') || q("#sec-plex .cw-panel");
    if (!root) return;

    const want = String(tab || "auth").toLowerCase();
    const sub = ["auth", "settings", "whitelist"].includes(want) ? want : "auth";

    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
      btn.classList.toggle("active", String(btn.dataset.sub || "").toLowerCase() === sub);
    });
    root.querySelectorAll(".cw-subpanel[data-sub]").forEach((sp) => {
      sp.classList.toggle("active", String(sp.dataset.sub || "").toLowerCase() === sub);
    });

    if (opts.persist !== false) {
      try { localStorage.setItem(PLEX_SUBTAB_KEY, sub); } catch {}
    }

    if (sub === "whitelist") {
      try { mountPlexLibraryMatrix(); } catch {}
    }
  }

  function mountPlexAuthTabs() {
    const root = q('#sec-plex .cw-meta-provider-panel[data-provider="plex"]');
    if (!root) return;

    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
      if (btn.__plexTabWired) return;
      btn.__plexTabWired = true;
      btn.addEventListener("click", () => plexAuthSubSelect(btn.dataset.sub));
    });

    if (root.__plexTabsInit) return;
    root.__plexTabsInit = true;

    let last = "auth";
    try { last = localStorage.getItem(PLEX_SUBTAB_KEY) || "auth"; } catch {}
    plexAuthSubSelect(last, { persist: false });
  }

  let __plexHydrateWatch = null;
  let __plexHydrateTimer = null;
  function schedulePlexHydrate(delayMs = 0) {
    try { if (__plexHydrateTimer) clearTimeout(__plexHydrateTimer); } catch {}
    __plexHydrateTimer = setTimeout(async () => {
      try { ensurePlexInstanceUI(); } catch {}
      try { mountPlexAuthTabs(); } catch {}
      if (!$("plex_token") || !$("plex_server_url") || !$("plex_username")) return;
      try { await hydratePlexFromConfigRaw(); } catch {}
      try { mountPlexLibraryMatrix(); } catch {}
      try { mountPlexUserPicker(); } catch {}
      try { schedulePlexPmsProbe(250); } catch {}
    }, Math.max(0, delayMs | 0));
  }

  function watchForPlexDom() {
    if (__plexHydrateWatch) return;
    schedulePlexHydrate(0);
    try {
      __plexHydrateWatch = new MutationObserver(() => schedulePlexHydrate(60));
      __plexHydrateWatch.observe(d.documentElement || d.body, { childList: true, subtree: true });
    } catch {}
  }

  // status banner
  function setPlexBanner(kind, text) {
    const el = $("plex_msg");
    if (!el) return;
    el.classList.remove("hidden", "ok", "warn");
    if (!kind) { el.classList.add("hidden"); el.textContent = ""; return; }
    el.classList.add(kind);
    el.textContent = text || "";
  }

  function setPlexBannerDetail(kind, text) {
    const el = $("plex_msg_detail");
    if (!el) return;
    el.classList.remove("hidden", "warn");
    if (!kind || !text) { el.classList.add("hidden"); el.textContent = ""; return; }
    if (kind) el.classList.add(kind);
    el.textContent = text || "";
  }

  function setPlexSuccess(on, text) {
    if (on) setPlexBanner("ok", text || "Connected");
    else { setPlexBanner(null, ""); setPlexBannerDetail(null, ""); }
  }

  function setPlexPending(text) {
    setPlexBanner("warn", text || "Waiting for Plex...");
    setPlexBannerDetail(null, "");
  }

  function setPlexConnected(token) {
    setPlexSuccess(true, "Connected");
    setPlexBannerDetail(null, "");
    schedulePlexPmsProbe(200);
  }

  let __plexProbeT = null;
  function schedulePlexPmsProbe(delayMs = 400) {
    try { if (__plexProbeT) clearTimeout(__plexProbeT); } catch {}
    __plexProbeT = setTimeout(() => { plexProbePmsReachability(); }, Math.max(0, delayMs | 0));
  }

  async function plexProbePmsReachability() {
    const tok = String($("plex_token")?.value || "").trim();
    if (!tok) { setPlexBannerDetail(null, ""); return; }
    try {
      const r = await fetch(plexApi("/api/plex/pms/probe"), { cache: "no-store" });
      const j = await r.json().catch(() => ({}));
      if (r.ok && j?.reachable) {
        setPlexBannerDetail(null, "");
        return;
      }
      const base = String(j?.server_url || "").trim();
      const sc = Number(j?.status);
      let msg = "Connected, but PMS is not reachable - validate settings.";
      if (!base) msg = "Connected, but no PMS URL is set - validate settings.";
      else if (sc === 401 || sc === 403) msg = "Connected, but PMS rejected the token - validate settings.";
      setPlexBannerDetail("warn", msg);
    } catch {
      setPlexBannerDetail("warn", "Connected, but PMS is not reachable - validate settings.");
    }
  }

  // PIN flow
  async function requestPlexPin() {
    try { setPlexSuccess(false); } catch {}
    let win = null; try { win = w.open("https://plex.tv/link", "_blank"); } catch {}
    let data = null;
    try {
      const r = await fetch(plexApi("/api/plex/pin/new"), { method: "POST", cache: "no-store" });
      data = await r.json();
      if (!r.ok || data?.ok === false) throw new Error(data?.error || "PIN request failed");
    } catch (e) {
      console.warn("plex pin fetch failed", e);
      notify("Failed to request PIN");
      try { if (win && !win.closed) win.close(); } catch {}
      return;
    }
    const pin = data.code || data.pin || data.id || "";
    try {
      d.querySelectorAll('#plex_pin, input[name="plex_pin"]').forEach(el => { el.value = pin; });
      const msg = $("plex_msg"); if (msg) { msg.textContent = pin ? ("PIN: " + pin) : "PIN request ok"; msg.classList.remove("hidden"); }
      try { setPlexBannerDetail(null, ""); } catch {}
      if (pin) { try { await navigator.clipboard.writeText(pin); } catch {} }
      if (win && !win.closed) {
        try { win.focus(); } catch {}
      } else {
        notify("Popup blocked - allow popups and try again");
      }
    } catch (e) { console.warn("pin ui update failed", e); }
    try { startPlexTokenPoll(); } catch {}
  }

  // token poll
  let plexPoll = null;
  function startPlexTokenPoll() {
    try { if (plexPoll) clearTimeout(plexPoll); } catch {}
    const deadline = Date.now() + 120000;
    const back = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;
    let detailTries = 0;
    let autoTried = false;

    const poll = async () => {
      if (Date.now() >= deadline) { plexPoll = null; return; }

      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) {
        plexPoll = setTimeout(poll, 5000);
        return;
      }

      let cfg = null;
      try {
        cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then(r => r.json());
      } catch {}

      const p = getPlexCfgBlock(cfg || {});
      const tok = (p.account_token || "").trim();

      if (tok) {
        try {
          const tokenEl = $("plex_token");
          if (tokenEl) tokenEl.value = tok;

          const urlEl  = $("plex_server_url");
          const userEl = $("plex_username");
          const idEl   = $("plex_account_id");

          const cfgUrl  = (p.server_url || "").trim();
          const cfgUser = (p.username || "").trim();
          const cfgId   = (p.account_id != null ? String(p.account_id) : "").trim();

          if (urlEl && !urlEl.value && cfgUrl)  urlEl.value = cfgUrl;
          if (userEl && !userEl.value && cfgUser) userEl.value = cfgUser;
          if (idEl && !idEl.value && cfgId)    idEl.value = cfgId;

          if (!autoTried && typeof plexAuto === "function" && (!cfgUser || !cfgId)) {
            autoTried = true;
            try { await plexAuto(); } catch {}
          }

          const haveDetails = !!(cfgUrl || cfgUser || cfgId);

          if (haveDetails || detailTries++ >= 5) {
            try { setPlexSuccess(true); } catch {}
            try { await plexProbePmsReachability(); } catch {}
            plexPoll = null;
            return;
          }
        } catch (e) {
          console.warn("plex token poll hydrate failed", e);
          try { setPlexSuccess(true); } catch {}
          try { await plexProbePmsReachability(); } catch {}
          plexPoll = null;
          return;
        }
      }

      plexPoll = setTimeout(poll, back[Math.min(i++, back.length - 1)]);
    };

    plexPoll = setTimeout(poll, 1000);
  }

  // delete Plex account token
async function plexDeleteToken() {
  const btn = d.querySelector('#sec-plex .btn.danger, #sec-plex [data-action="plex-delete"], #sec-plex button[id*="delete"]');
  try { if (btn) { btn.disabled = true; btn.classList.add("busy"); } } catch {}
  try {
    const r = await fetch(plexApi("/api/plex/token/delete"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
      cache: "no-store"
    });
    const j = await r.json().catch(() => ({}));
    if (r.ok && (j.ok !== false)) {
      ["plex_token", "plex_pin", "plex_username", "plex_account_id"].forEach((id) => { const el = $(id); if (el) el.value = ""; });
      try { getPlexState().libs = []; } catch {}
      try { setPlexBanner("warn", "Disconnected"); } catch {}
      try { setPlexBannerDetail("warn", "Token deleted and saved."); } catch {}
      try { notify("Plex disconnected (saved)."); } catch {}
      try { refreshPlexLibraries(); } catch {}
    } else {
      const msg = String(j?.error || j?.message || "").trim() || "Could not remove Plex token.";
      try { setPlexBannerDetail("warn", msg); } catch {}
    }
  } catch (e) {
    console.warn("plex delete token failed", e);
    try { setPlexBannerDetail("warn", "Error removing Plex token."); } catch {}
  } finally {
    try { if (btn) { btn.disabled = false; btn.classList.remove("busy"); } } catch {}
  }
}


  function getPlexState() { return (w.__plexState ||= { hist: new Set(), rate: new Set(), scr: new Set(), libs: [], hydrated: false }); }

  // Config
  async function hydratePlexFromConfigRaw() {
    try {
      const r = await fetch("/api/config", { cache: "no-store" }); if (!r.ok) return;
      const cfg = await r.json(); const p = getPlexCfgBlock(cfg);
      await waitFor("#plex_server_url"); await waitFor("#plex_username");
      const set = (id, val) => { const el = $(id); if (el != null && val != null) el.value = String(val); };
      set("plex_token", p.account_token || "");
      const tok = String(p.account_token || '').trim();
      if (tok) setPlexConnected(tok);
      else setPlexSuccess(false);
      set("plex_pin", p._pending_pin?.code || "");
      set("plex_server_url", p.server_url || "");
      set("plex_username", p.username || "");
      set("plex_account_id", p.account_id ?? "");

      const st = getPlexState();
      st.hist = new Set((p.history?.libraries || []).map(x => String(x)));
      st.rate = new Set((p.ratings?.libraries || []).map(x => String(x)));
      st.scr  = new Set((p.scrobble?.libraries || []).map(x => String(x)));
      st.hydrated = true;

      ["plex_lib_history", "plex_lib_ratings", "plex_lib_scrobble"].forEach(id => {
        const el = $(id); if (!el) return;
        Array.from(el.options || []).forEach(o => {
          if (id === "plex_lib_history") o.selected = st.hist.has(o.value);
          if (id === "plex_lib_ratings") o.selected = st.rate.has(o.value);
          if (id === "plex_lib_scrobble") o.selected = st.scr.has(o.value);
        });
      });
    } catch (e) { console.warn("[plex] hydrate failed", e); }
  }

  // build server suggestions (from /api/plex/pms)
  function fillPlexServerSuggestions(servers) {
    const dl = document.getElementById("plex_server_suggestions");
    if (!dl) return "";

    const items = [];
    const seen = new Set();

    const isPrivateHost = (host) => {
      if (!host) return false;
      const h = host.toLowerCase();

      const isPrivateDotted = (ip) => {
        const p = ip.split(".").map(n => parseInt(n, 10));
        if (p.length !== 4 || p.some(n => !Number.isFinite(n) || n < 0 || n > 255)) return false;
        if (p[0] === 10) return true;
        if (p[0] === 192 && p[1] === 168) return true;
        if (p[0] === 172 && p[1] >= 16 && p[1] <= 31) return true;
        if (p[0] === 127) return true;
        if (p[0] === 169 && p[1] === 254) return true;
        return false;
      };

      if (/^\d{1,3}(\.\d{1,3}){3}$/.test(h)) return isPrivateDotted(h);

      const m = h.match(/^(\d{1,3}(?:-\d{1,3}){3})\.plex\.direct$/);
      if (m) return isPrivateDotted(m[1].replace(/-/g, "."));

      return false;
    };

    const add = (key, meta = {}) => {
      const url = (key || "").trim().replace(/\/+$/, "");
      if (!url || seen.has(url)) return;
      seen.add(url);

      const local    = !!meta.local;
      const relay    = !!meta.relay;
      const proto    = (meta.proto || "").toLowerCase();
      const hostKind = meta.hostKind || "domain";

      let host = "";
      try { host = new URL(url).hostname || ""; } catch {}
      const privateHost = isPrivateHost(host);
      const effProto = proto || (url.startsWith("https://") ? "https" : "http");

      const score =
        (local ? 8 : 0) +
        (!relay ? 4 : 0) +
        (privateHost ? 3 : 0) +
        (effProto === "http" ? 2 : 0) +
        (hostKind === "ip" ? 1 : 0);

      const tags = [
        local ? "local" : "remote",
        relay ? "relay" : "direct",
        effProto,
        privateHost ? "private" : hostKind
      ].join(", ");

      items.push({ url, score, label: `${url} — ${tags}` });
    };

    (servers || []).forEach((s) => {
      (s.connections || []).forEach((c) => {
        const address = (c.address || "").trim();
        const port = c.port ? `:${c.port}` : "";
        const local = !!c.local;
        const relay = !!c.relay;

        if (address) {
          add(`http://${address}${port}`,  { local, relay, proto: "http",  hostKind: "ip" });
          add(`https://${address}${port}`, { local, relay, proto: "https", hostKind: "ip" });
        }

        if (c.uri) {
          try {
            const u = new URL(c.uri);
            add(c.uri, {
              local,
              relay,
              proto: u.protocol.replace(":", ""),
              hostKind: "domain"
            });
          } catch {}
        }
      });
    });

    items.sort((a, b) => b.score - a.score || a.url.length - b.url.length);
    dl.innerHTML = items
      .map((it) => `<option value="${it.url}" label="${it.label}"></option>`)
      .join("");

    return items[0]?.url || "";
  }

  // Auto-Fetch: prefer /api/plex/pms; then hydrate user/id via /api/plex/inspect
  async function plexAuto() {
    const urlEl = document.getElementById("plex_server_url");
    const setIfEmpty = (el, val) => { if (el && !el.value && val) el.value = String(val); };

    try {
      let cfgUrl = "";
      try {
        const rCfg = await fetch("/api/config?ts=" + Date.now(), { cache: "no-store" });
        if (rCfg.ok) {
          const cfg = await rCfg.json();
          cfgUrl = (cfg?.plex?.server_url || "").trim();
          if (cfgUrl && urlEl) urlEl.value = cfgUrl;
        }
      } catch {}

      // Fetch /api/plex/pms for server suggestions
      let bestSuggestion = "";
      try {
        const r = await fetch(plexApi("/api/plex/pms"), { cache: "no-store" });
        if (r.ok) {
          const j = await r.json();
          const servers = Array.isArray(j?.servers) ? j.servers : [];
          bestSuggestion = fillPlexServerSuggestions(servers) || "";
        }
      } catch {}

      if (urlEl && bestSuggestion) {
        const curr = (urlEl.value || "").trim();

        const currCloudish = (() => {
          if (!curr) return false;
          try {
            const h = (new URL(curr)).hostname.toLowerCase();
            return h.endsWith(".plex.direct") || h.endsWith(".plex.tv") || h.endsWith(".plexapp.com");
          } catch {
            return /plex\.direct|plex\.tv|plexapp\.com/i.test(curr);
          }
        })();

        const bestPrivateHttp =
          /^http:\/\//i.test(bestSuggestion) &&
          /^(http:\/\/)?(10\.|192\.168\.|172\.(1[6-9]|2\d|3[0-1])\.|127\.|169\.254\.)/i.test(bestSuggestion);

        if (!curr || (currCloudish && bestPrivateHttp)) {
          urlEl.value = bestSuggestion;
          urlEl.dispatchEvent(new Event("input",  { bubbles: true }));
          urlEl.dispatchEvent(new Event("change", { bubbles: true }));
        }
      }

      // Hydrate username/account_id via /api/plex/inspect
      try {
        const rr = await fetch(plexApi("/api/plex/inspect"), { cache: "no-store" });
        if (rr.ok) {
          const dta = await rr.json();
          const set = (id, val) => {
            const el = document.getElementById(id);
            if (el && val != null) el.value = String(val);
          };
          setIfEmpty(urlEl, dta.server_url);
          if (dta.username) set("plex_username", dta.username);
          if (dta.account_id != null) set("plex_account_id", dta.account_id);
        }
      } catch {}

    } catch (e) {
      console.warn("[plex] Auto-Fetch failed", e);
    }
    try { schedulePlexPmsProbe(200); } catch {}
  }
  
  // User picker
  const __plexUsersByInst = new Map();

  async function fetchPlexUsers() {
    const inst = getPlexInstance();
    if (__plexUsersByInst.has(inst)) return __plexUsersByInst.get(inst) || [];
    let out = [];
    try {
      const r = await fetch(plexApi("/api/plex/pickusers"), { cache: "no-store" });
      const j = await r.json();
      out = Array.isArray(j?.users) ? j.users : [];
    } catch { out = []; }
    __plexUsersByInst.set(inst, out);
    return out;
  }

  function renderPlexUserList() {
    const listEl = $("plex_user_list"); if (!listEl) return;
    const qv = ($("plex_user_filter")?.value || "").trim().toLowerCase();
    const rank = { owner:0, managed:1, friend:2 };
    const by = new Map();

    const src = __plexUsersByInst.get(getPlexInstance()) || [];
    // dedup by username/title; prefer PMS id, then better type, then smaller id
    for (const u of src) {
      const uname = (u.username || u.title || `user#${u.id}`).trim();
      const key = uname.toLowerCase();
      const isPms = Number.isInteger(u.id) && u.id < 100000;
      const cur = by.get(key);
      if (!cur) { by.set(key, { id:u.id, username:uname, type:u.type||"friend" }); continue; }
      const curIsPms = Number.isInteger(cur.id) && cur.id < 100000;
      const better = (isPms && !curIsPms) ||
                    (rank[(u.type||"friend")] < rank[cur.type||"friend"]) ||
                    (Number.isInteger(u.id) && Number.isInteger(cur.id) && u.id < cur.id);
      if (better) by.set(key, { id:u.id, username:uname, type:u.type||"friend" });
    }

    let users = Array.from(by.values());
    users = users.filter(u => !qv || (u.username.toLowerCase().includes(qv) || (u.type||"").toLowerCase().includes(qv)));
    users.sort((a,b)=> (rank[a.type||"friend"] - rank[b.type||"friend"]) || a.username.localeCompare(b.username));

    const esc = s => String(s||"").replace(/[&<>"']/g,c=>({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
    listEl.innerHTML = users.length ? users.map(u => `
      <button type="button" class="userrow" data-uid="${esc(u.id)}" data-username="${esc(u.username)}">
        <div class="row1">
          <strong>${esc(u.username)}</strong>
          <span class="tag ${esc(u.type)}">${esc(u.type)}</span>
        </div>
      </button>
    `).join("") : '<div class="sub">No users found.</div>';
  }

  function placePlexUserPop() {
    const pop = $("plex_user_pop");
    const anchor = $("plex_user_pick_btn")?.closest(".userpick") || $("plex_user_pick_btn");
    if (!pop || !anchor) return;
    const r = anchor.getBoundingClientRect();
    const W = Math.min(360, Math.max(280, Math.round(window.innerWidth * 0.9)));
    pop.style.width = W + "px";
    const left = Math.max(8, Math.min(r.right - W, window.innerWidth - W - 8));
    const top  = Math.min(window.innerHeight - 48, r.bottom + 8);
    pop.style.left = left + "px";
    pop.style.top  = top  + "px";
  }

  function openPlexUserPicker() {
    const pop = $("plex_user_pop"); if (!pop) return;
    pop.classList.remove("hidden");
    fetchPlexUsers().then(() => { renderPlexUserList(); placePlexUserPop(); $("plex_user_filter")?.focus(); });
  }

  function closePlexUserPicker() { $("plex_user_pop")?.classList.add("hidden"); }

  function mountPlexUserPicker() {
    const pickBtn = $("plex_user_pick_btn");
    if (pickBtn && !pickBtn.__wired){
      pickBtn.__wired = true;
      pickBtn.addEventListener("click", (e)=>{ e.preventDefault(); openPlexUserPicker(); try{ placePlexUserPop(); }catch{} });
    }
    const closeBtn = $("plex_user_close");
    if (closeBtn && !closeBtn.__wired){
      closeBtn.__wired = true;
      closeBtn.addEventListener("click", (e)=>{ e.preventDefault(); closePlexUserPicker(); });
    }
    const filter = $("plex_user_filter");
    if (filter && !filter.__wired){
      filter.__wired = true;
      filter.addEventListener("input", renderPlexUserList);
    }
    const list = $("plex_user_list");
    if (list && !list.__wired){
      list.__wired = true;
      list.addEventListener("click",(e)=>{
        const row = e.target.closest(".userrow"); if (!row) return;
        const uname = row.dataset.username || "";
        const uid   = row.dataset.uid || "";
        const uEl = $("plex_username"); if (uEl) uEl.value = uname;
        const aEl = $("plex_account_id"); if (aEl) aEl.value = uid;
        closePlexUserPicker();
        try{ document.dispatchEvent(new CustomEvent("settings-collect",{detail:{section:"plex-users"}})); }catch{}
      });
    }
    if (!document.__plexUserAway){
      document.__plexUserAway = true;
      document.addEventListener("click",(e)=>{
        const pop = $("plex_user_pop");
        if (!pop || pop.classList.contains("hidden")) return;
        if (pop.contains(e.target) || e.target.id==="plex_user_pick_btn") return;
        closePlexUserPicker();
      });
      document.addEventListener("keydown",(e)=>{ if (e.key === "Escape") closePlexUserPicker(); });
    }
    if (!window.__plexUserPos){
      window.__plexUserPos = true;
      let raf = null;
      const safeReposition = ()=>{
        const pop = $("plex_user_pop");
        if (!pop || pop.classList.contains("hidden")) return;
        if (raf) return;
        raf = requestAnimationFrame(()=>{ raf = null; try{ placePlexUserPop(); }catch{} });
      };
      window.addEventListener("resize", safeReposition, { passive:true });
      window.addEventListener("scroll", safeReposition, { passive:true, capture:true });
      document.addEventListener("scroll", safeReposition, { passive:true, capture:true });
    }
  }

  // Libraries
  async function plexLoadLibraries() {
    let libs = [];
    try {
      const r = await fetch(plexApi("/api/plex/libraries"), { cache: "no-store" });
      if (r.ok) {
        const j = await r.json();
        libs = Array.isArray(j?.libraries) ? j.libraries : [];
      }
    } catch (e) {
      console.warn("[plex] libraries fetch failed", e);
    }

    try {
      const fill = (id) => {
        const el = $(id); if (!el) return;
        const keep = new Set(Array.from(el.selectedOptions || []).map(o => o.value));
        el.innerHTML = "";
        libs.forEach(it => {
          const o = d.createElement("option");
          o.value = String(it.key);
          o.textContent = `${it.title} (${it.type || "lib"}) — #${it.key}`;
          if (keep.has(o.value)) o.selected = true;
          el.appendChild(o);
        });
      };
      fill("plex_lib_history");
      fill("plex_lib_ratings");
      fill("plex_lib_scrobble");
    } catch (e) {
      console.warn("[plex] library select fill failed", e);
    }

    try {
      getPlexState().libs = libs.map(it => ({
        id: String(it.key),
        title: String(it.title),
        type: String(it.type || "lib")
      }));
    } catch (e) {
      console.warn("[plex] state update failed", e);
    }
    try {
      const hasServer =
        (document.getElementById("plex_server_url")?.value?.trim() || "") &&
        (document.getElementById("plex_token")?.value?.trim() || "");
      if (!libs.length && hasServer) {
        notify("No libraries could be loaded from Plex. Check the Server URL and make sure this is a Plex server your account can access.");
      }
    } catch {}

    return libs;
  }


  async function refreshPlexLibraries() {
    try {
      const host = document.getElementById("plex_lib_matrix");
      if (host) host.innerHTML = '<div class="sub">Loading libraries…</div>';
    } catch {}
    try { getPlexState().libs = []; } catch {}
    try { await hydratePlexFromConfigRaw(); } catch {}
    try { await plexLoadLibraries(); } catch {}
    try { mountPlexLibraryMatrix(); } catch {}
  }

  // Matrix UI
  function mountPlexLibraryMatrix() {
    const host    = $("plex_lib_matrix");
    const histSel = $("plex_lib_history");
    const rateSel = $("plex_lib_ratings");
    const scrSel  = $("plex_lib_scrobble");
    const filter  = $("plex_lib_filter");
    if (!host) return;
    const firstMount = !host.__wired;
    if (firstMount) host.__wired = true;

    const st = getPlexState();
    let syncing = false;

    const setSelFromSet = (sel, set) => {
      if (!sel) return;
      syncing = true;
      const want = new Set([...set].map(String));
      Array.from(sel.options).forEach(o => { o.selected = want.has(String(o.value)); });
      syncing = false;
    };

    const rowHTML = (lib) =>
      `<div class="lm-row" data-id="${lib.id}" data-name="${lib.title.toLowerCase()}">
         <div class="lm-name" title="#${lib.id}">${lib.title} <span class="lm-id">#${lib.id}</span></div>
         <button type="button" class="lm-dot hist ${st.hist.has(lib.id) ? "on" : ""}" aria-label="History" aria-pressed="${st.hist.has(lib.id)}"></button>
         <button type="button" class="lm-dot rate ${st.rate.has(lib.id) ? "on" : ""}" aria-label="Ratings" aria-pressed="${st.rate.has(lib.id)}"></button>
         <button type="button" class="lm-dot scr ${st.scr.has(lib.id) ? "on" : ""}" aria-label="Scrobble" aria-pressed="${st.scr.has(lib.id)}"></button>
       </div>`;

    function applyFilter() {
      const qv = (filter?.value || "").trim().toLowerCase();
      host.querySelectorAll(".lm-row").forEach(r => {
        const hit = !qv || r.dataset.name.includes(qv) || (r.querySelector(".lm-id")?.textContent || "").includes(qv);
        r.classList.toggle("hide", !hit);
      });
    }

    function render() {
      const libs = getPlexState().libs;
      const hasServer =
        (document.getElementById("plex_server_url")?.value?.trim() || "") &&
        (document.getElementById("plex_token")?.value?.trim() || "");
      if (!libs.length && hasServer) {
        notify("No libraries could be loaded from Plex. Check the Server URL and make sure this is a Plex server your account can access.");
      }

      host.innerHTML = libs.length
        ? libs.map(rowHTML).join("")
        : `<div class="sub">No libraries loaded.</div>`;
      applyFilter();
      setSelFromSet(histSel, st.hist);
      setSelFromSet(rateSel, st.rate);
      setSelFromSet(scrSel,  st.scr);
    }

    function toggleOne(id, which) {
      if (which === "hist") { st.hist.has(id) ? st.hist.delete(id) : st.hist.add(id); render(); return; }
      if (which === "rate") { st.rate.has(id) ? st.rate.delete(id) : st.rate.add(id); render(); return; }
      if (which === "scr")  { st.scr.has(id) ? st.scr.delete(id) : st.scr.add(id);  render(); return; }
    }

    if (firstMount) {
      host.addEventListener("click", (ev) => {
        const btn = ev.target.closest(".lm-dot"); if (!btn) return;
        const row = ev.target.closest(".lm-row"); const id = row?.dataset?.id; if (!id) return;
        const which = btn.classList.contains("hist") ? "hist" : (btn.classList.contains("scr") ? "scr" : "rate");
        toggleOne(id, which);
      });

      $("plex_hist_all")?.addEventListener("click", () => {
        const visible = Array.from(host.querySelectorAll(".lm-row:not(.hide)")).map(r => r.dataset.id);
        const allOn = visible.every(id => st.hist.has(id));
        if (allOn) visible.forEach(id => st.hist.delete(id)); else visible.forEach(id => st.hist.add(id));
        render();
      });

      $("plex_rate_all")?.addEventListener("click", () => {
        const visible = Array.from(host.querySelectorAll(".lm-row:not(.hide)")).map(r => r.dataset.id);
        const allOn = visible.every(id => st.rate.has(id));
        if (allOn) visible.forEach(id => st.rate.delete(id)); else visible.forEach(id => st.rate.add(id));
        render();
      });

      $("plex_scr_all")?.addEventListener("click", () => {
        const visible = Array.from(host.querySelectorAll(".lm-row:not(.hide)")).map(r => r.dataset.id);
        const allOn = visible.every(id => st.scr.has(id));
        if (allOn) visible.forEach(id => st.scr.delete(id)); else visible.forEach(id => st.scr.add(id));
        render();
      });

      filter?.addEventListener("input", applyFilter);

      histSel?.addEventListener("change", () => {
        if (syncing) return;
        st.hist = new Set(Array.from(histSel.selectedOptions || []).map(o => String(o.value)));
        render();
      });
      rateSel?.addEventListener("change", () => {
        if (syncing) return;
        st.rate = new Set(Array.from(rateSel.selectedOptions || []).map(o => String(o.value)));
        render();
      });
      scrSel?.addEventListener("change", () => {
        if (syncing) return;
        st.scr = new Set(Array.from(scrSel.selectedOptions || []).map(o => String(o.value)));
        render();
      });
    }

    (async () => {
      if (!getPlexState().libs.length) await plexLoadLibraries();
      render();
    })();
  }

  // Read UI on save
  function readMatrixSelection(which) {
    const host = $("plex_lib_matrix");
    if (!host) return null;
    const sels = new Set();
    const sel = which === "hist" ? ".lm-dot.hist.on" : (which === "rate" ? ".lm-dot.rate.on" : ".lm-dot.scr.on");
    host.querySelectorAll(sel).forEach(btn => {
      const id = btn.closest(".lm-row")?.dataset?.id;
      const n = parseInt(String(id), 10);
      if (Number.isFinite(n)) sels.add(n);
    });
    return Array.from(sels);
  }
  function readSelectInts(sel) {
    const el = q(sel); if (!el) return null;
    return Array.from(el.selectedOptions || []).map(o => parseInt(String(o.value), 10)).filter(Number.isFinite);
  }

  function mergePlexIntoCfg(cfg) {
    const v = (sel) => {
      const el = q(sel);
      return el ? String(el.value || "").trim() : null;
    };

    cfg = cfg || (w.__cfg ||= {});
    const plex = getPlexCfgBlock(cfg);

    const url  = v("#plex_server_url");
    const user = v("#plex_username");
    const aid  = v("#plex_account_id");
    const pin  = v("#plex_home_pin");

    if (url)  plex.server_url = url;
    if (user) plex.username   = user;

    if (pin !== null) plex.home_pin = pin;

    // Always ensure account_id is a positive integer; fallback to 1
    if (aid !== null) {
      let n = parseInt(aid, 10);
      if (!Number.isFinite(n) || n <= 0) n = 1;
      plex.account_id = n;
    }

    const st = getPlexState();
    const uiReady = !!st.hydrated ||
      !!document.querySelector("#plex_lib_matrix .lm-row") ||
      !!document.querySelector("#plex_lib_history option, #plex_lib_ratings option, #plex_lib_scrobble option");
    if (uiReady) {
      const toInts = (set) => Array.from(set || []).map(x => parseInt(String(x), 10)).filter(Number.isFinite);
      const hist = toInts(st.hist);
      const rate = toInts(st.rate);
      const scr  = toInts(st.scr);
      plex.scrobble = Object.assign({}, plex.scrobble || {}, { libraries: scr });
      plex.history  = Object.assign({}, plex.history  || {}, { libraries: hist });
      plex.ratings  = Object.assign({}, plex.ratings  || {}, { libraries: rate });
    }
    return cfg;
  }


  let __plexUrlDirty = false;


  function hookPlexSave() {
    try {
      const api = w.CW?.API?.Config;
      if (api && typeof api.save === "function" && !api._wrappedByPlex) {
        const orig = api.save.bind(api);
        api.save = async (cfg) => {
          try { mergePlexIntoCfg(cfg); } catch {}
          const prevUrl = (w.__lastPlexUrl || "");
          const currUrl = $("#plex_server_url")?.value?.trim() || "";
          __plexUrlDirty = (currUrl !== prevUrl);
          const res = await orig(cfg);
          try {
            if (__plexUrlDirty) {
              await refreshPlexLibraries();
              w.__lastPlexUrl = currUrl;
              __plexUrlDirty = false;
            }
          } catch {}
          return res;
        };
        api._wrappedByPlex = true;
      }
    } catch {}

    d.addEventListener("click", (e) => {
      const t = e.target;
      if (!t) return;
      if (t.id === "save-fab-btn" || t.matches('[data-action="save"], .btn.save, button#save, button[id*="save"]')) {
        try { mergePlexIntoCfg(w.__cfg ||= {}); } catch {}
        setTimeout(() => {
          const prevUrl = (w.__lastPlexUrl || "");
          const currUrl = $("#plex_server_url")?.value?.trim() || "";
          if (currUrl !== prevUrl) {
            refreshPlexLibraries()?.then(()=>{ w.__lastPlexUrl = currUrl; }).catch(()=>{});
          }
        }, 0);
      }
    }, true);

    d.addEventListener("settings-collect", (ev) => {
      try { mergePlexIntoCfg(ev?.detail?.cfg || (w.__cfg ||= {})); } catch {}
    }, true);

    w.registerSettingsCollector?.((cfg) => { try { mergePlexIntoCfg(cfg); } catch {} });
  }


  let __plexInitDone = false;
  function initPlexAuthUI() {
    if (__plexInitDone) return;
    __plexInitDone = true;
    try { watchForPlexDom(); } catch {}
    try { mountPlexAuthTabs(); } catch {}
    try { hookPlexSave(); } catch {}
    setTimeout(() => { try { hydratePlexFromConfigRaw(); } catch {} }, 100);
    setTimeout(() => { try { schedulePlexPmsProbe(300); } catch {} }, 450);
    try { mountPlexLibraryMatrix(); } catch {}
    try { mountPlexUserPicker(); } catch {}
    try { w.__lastPlexUrl = $("plex_server_url")?.value?.trim() || ""; } catch {}

    try {
      const urlEl = $("plex_server_url");
      if (urlEl && !urlEl.__pmsProbeWired) {
        urlEl.__pmsProbeWired = true;
        urlEl.addEventListener("change", () => schedulePlexPmsProbe(300));
        urlEl.addEventListener("blur", () => schedulePlexPmsProbe(300));
      }
      const sslEl = $("plex_verify_ssl");
      if (sslEl && !sslEl.__pmsProbeWired) {
        sslEl.__pmsProbeWired = true;
        sslEl.addEventListener("change", () => schedulePlexPmsProbe(300));
      }
    } catch {}
  }

  if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", initPlexAuthUI, { once: true });
  else initPlexAuthUI();

  w.cwAuth = w.cwAuth || {};
  w.cwAuth.plex = w.cwAuth.plex || {};
  w.cwAuth.plex.init = initPlexAuthUI;

  d.addEventListener("tab-changed", async (ev) => {
    const onSettings = ev?.detail?.id ? /settings/i.test(ev.detail.id) : !!q("#sec-plex");
    if (onSettings) {
      try { watchForPlexDom(); } catch {}
      try { mountPlexAuthTabs(); } catch {}
      await waitFor("#plex_server_url");
      try { hydratePlexFromConfigRaw(); } catch {}
      try { plexAuto(); } catch {}
      try { await plexLoadLibraries(); } catch {}
      try { mountPlexLibraryMatrix(); } catch {}
      try { mountPlexUserPicker(); } catch {}
    } else {
      try { setPlexSuccess(false); } catch {}
    }
  });

  //  exports
  Object.assign(w, {
    setPlexSuccess, requestPlexPin, startPlexTokenPoll, plexDeleteToken,
    mergePlexIntoCfg, plexAuto, plexLoadLibraries,
    hydratePlexFromConfigRaw, mountPlexLibraryMatrix,
    openPlexUserPicker, closePlexUserPicker, mountPlexUserPicker,
    refreshPlexLibraries,
    plexProbePmsReachability,
  });

})(window, document);