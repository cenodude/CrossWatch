// auth.plex.js — Plex auth + settings + library matrix UI 
(function (w, d) {
  // --- tiny utils
  const $ = (s) => d.getElementById(s);
  const q = (sel, root = d) => root.querySelector(sel);
  const notify = w.notify || ((m) => console.log("[notify]", m));
  const bust = () => `?ts=${Date.now()}`;
  const exists = (sel) => !!q(sel);
  function waitFor(sel, timeout = 2000) {
    return new Promise((res) => {
      const end = Date.now() + timeout;
      (function loop() {
        if (exists(sel)) return res(q(sel));
        if (Date.now() > end) return res(null);
        requestAnimationFrame(loop);
      })();
    });
  }

  // ---------- success banner
  function setPlexSuccess(on) { $("plex_msg")?.classList.toggle("hidden", !on); }

  // ---------- PIN flow
  async function requestPlexPin() {
    try { setPlexSuccess(false); } catch {}
    let win = null; try { win = w.open("https://plex.tv/link", "_blank"); } catch {}
    let data = null;
    try {
      const r = await fetch("/api/plex/pin/new", { method: "POST", cache: "no-store" });
      data = await r.json();
      if (!r.ok || data?.ok === false) throw new Error(data?.error || "PIN request failed");
    } catch (e) { console.warn("plex pin fetch failed", e); notify("Failed to request PIN"); return; }
    const pin = data.code || data.pin || data.id || "";
    try {
      d.querySelectorAll('#plex_pin, input[name="plex_pin"]').forEach(el => { el.value = pin; });
      const msg = $("plex_msg"); if (msg) { msg.textContent = pin ? ("PIN: " + pin) : "PIN request ok"; msg.classList.remove("hidden"); }
      if (pin) { try { await navigator.clipboard.writeText(pin); } catch {} }
      if (win && !win.closed) win.focus();
    } catch (e) { console.warn("pin ui update failed", e); }
    try { startPlexTokenPoll(); } catch {}
  }

  // ---------- token poll
  let plexPoll = null;
  function startPlexTokenPoll() {
    try { if (plexPoll) clearTimeout(plexPoll); } catch {}
    const deadline = Date.now() + 120000;
    const back = [1000, 2500, 5000, 7500, 10000, 15000, 20000, 20000];
    let i = 0;
    const poll = async () => {
      if (Date.now() >= deadline) { plexPoll = null; return; }
      const settingsVisible = !!($("page-settings") && !$("page-settings").classList.contains("hidden"));
      if (d.hidden || !settingsVisible) { plexPoll = setTimeout(poll, 5000); return; }
      let cfg = null; try { cfg = await fetch("/api/config" + bust(), { cache: "no-store" }).then(r => r.json()); } catch {}
      const tok = (cfg?.plex?.account_token || "").trim();
      if (tok) { try { const el = $("plex_token"); if (el) el.value = tok; } catch {} try { setPlexSuccess(true); } catch {} plexPoll = null; return; }
      plexPoll = setTimeout(poll, back[Math.min(i++, back.length - 1)]);
    };
    plexPoll = setTimeout(poll, 1000);
  }

  // --- delete Plex account token
  async function plexDeleteToken() {
    const btn = document.querySelector('#sec-plex .btn.danger');
    try { if (btn) { btn.disabled = true; btn.classList.add('busy'); } } catch {}
    try {
      const r = await fetch('/api/plex/token/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
        cache: 'no-store'
      });
      const j = await r.json().catch(() => ({}));
      if (r.ok && (j.ok !== false)) {
        const el = document.getElementById('plex_token'); if (el) el.value = '';
        try { setPlexSuccess(false); } catch {}
        (window.notify || ((m)=>console.log('[notify]', m)))('Plex account token removed.');
      } else {
        (window.notify || ((m)=>console.log('[notify]', m)))('Could not remove Plex token.');
      }
    } catch {
      (window.notify || ((m)=>console.log('[notify]', m)))('Error removing Plex token.');
    } finally {
      try { if (btn) { btn.disabled = false; btn.classList.remove('busy'); } } catch {}
    }
  }

  // ---------- local state (UI only)
  function getPlexState() { return (w.__plexState ||= { hist: new Set(), rate: new Set(), libs: [] }); }

  // ---------- Config → UI
  async function hydratePlexFromConfigRaw() {
    try {
      const r = await fetch("/api/config", { cache: "no-store" }); if (!r.ok) return;
      const cfg = await r.json(); const p = cfg?.plex || {};
      await waitFor("#plex_server_url"); await waitFor("#plex_username");
      const set = (id, val) => { const el = $(id); if (el != null && val != null) el.value = String(val); };
      set("plex_token", p.account_token || "");
      set("plex_pin", p._pending_pin?.code || "");
      set("plex_server_url", p.server_url || "");
      set("plex_username", p.username || "");
      set("plex_account_id", p.account_id ?? "");

      const st = getPlexState();
      st.hist = new Set((p.history?.libraries || []).map(x => String(x)));
      st.rate = new Set((p.ratings?.libraries || []).map(x => String(x)));

      ["plex_lib_history", "plex_lib_ratings"].forEach(id => {
        const el = $(id); if (!el) return;
        Array.from(el.options || []).forEach(o => {
          if (id === "plex_lib_history") o.selected = st.hist.has(o.value);
          if (id === "plex_lib_ratings") o.selected = st.rate.has(o.value);
        });
      });
    } catch (e) { console.warn("[plex] hydrate failed", e); }
  }

  // --- build server suggestions (from /api/plex/pms)
  // Preference: local > non-relay > HTTP > plain IP host > everything else
  function fillPlexServerSuggestions(servers) {
    const dl = document.getElementById("plex_server_suggestions");
    if (!dl) return "";

    const seen = new Set();
    const items = [];

    const add = (url, meta = {}) => {
      if (!url) return;
      const key = url.trim();
      if (seen.has(key)) return;
      seen.add(key);

      const { local = false, relay = false, proto = "", hostKind = "domain" } = meta;
      const effProto = proto || (key.startsWith("https://") ? "https" : "http");
      const score =
        (local ? 8 : 0) +
        (!relay ? 4 : 0) +
        (effProto === "http" ? 2 : 0) +
        (hostKind === "ip" ? 1 : 0);

      const tags = [
        local ? "local" : "remote",
        relay ? "relay" : "direct",
        effProto,
        hostKind
      ].join(", ");

      items.push({ url: key, score, label: `${key} — ${tags}` });
    };

    (servers || []).forEach((s) => {
      (s.connections || []).forEach((c) => {
        const address = (c.address || "").trim();
        const port = c.port ? `:${c.port}` : "";
        const local = !!c.local;
        const relay = !!c.relay;

        // Always include clean IP forms (http + https)
        if (address) {
          add(`http://${address}${port}`,  { local, relay, proto: "http",  hostKind: "ip" });
          add(`https://${address}${port}`, { local, relay, proto: "https", hostKind: "ip" });
        }

        // Also include the original connection URI (plex.direct)
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

    // return best candidate (useful for auto-fill)
    return items[0]?.url || "";
  }

  // --- Auto-Fetch: prefer /api/plex/pms; then hydrate user/id via /api/plex/inspect
  async function plexAuto() {
    const urlEl = document.getElementById("plex_server_url");
    const setIfEmpty = (el, val) => { if (el && !el.value && val) el.value = String(val); };

    try {
      // 1) Read current config first
      let cfgUrl = "";
      try {
        const rCfg = await fetch("/api/config?ts=" + Date.now(), { cache: "no-store" });
        if (rCfg.ok) {
          const cfg = await rCfg.json();
          cfgUrl = (cfg?.plex?.server_url || "").trim();
          if (cfgUrl && urlEl) urlEl.value = cfgUrl; // lock to existing config value
        }
      } catch {}

      // 2) Load PMS servers to populate suggestions (but don't override field)
      let bestSuggestion = "";
      try {
        const r = await fetch("/api/plex/pms?ts=" + Date.now(), { cache: "no-store" });
        if (r.ok) {
          const j = await r.json();
          const servers = Array.isArray(j?.servers) ? j.servers : [];
          bestSuggestion = fillPlexServerSuggestions(servers) || "";
        }
      } catch {}

      // If the input is still empty (no config url), put in the best suggestion
      if (urlEl && !urlEl.value && bestSuggestion) {
        urlEl.value = bestSuggestion;
        urlEl.dispatchEvent(new Event("input",  { bubbles: true }));
        urlEl.dispatchEvent(new Event("change", { bubbles: true }));
      }

      // 3) Hydrate username / PMS account_id (never overwrite a non-empty server_url)
      try {
        const rr = await fetch("/api/plex/inspect?ts=" + Date.now(), { cache: "no-store" });
        if (rr.ok) {
          const dta = await rr.json();
          const set = (id, val) => { const el = document.getElementById(id); if (el && val != null) el.value = String(val); };
          // only set server_url if field is still empty
          setIfEmpty(urlEl, dta.server_url);
          if (dta.username) set("plex_username", dta.username);
          if (dta.account_id != null) set("plex_account_id", dta.account_id);
        }
      } catch {}

    } catch (e) {
      console.warn("[plex] Auto-Fetch failed", e);
    }
  }
    // ---------- User picker
  let __plexUsers = null;

  async function fetchPlexUsers() {
    if (Array.isArray(__plexUsers)) return __plexUsers;
    try {
      const r = await fetch("/api/plex/pickusers" + bust(), { cache: "no-store" });
      const j = await r.json();
      __plexUsers = Array.isArray(j?.users) ? j.users : [];
    } catch { __plexUsers = []; }
    return __plexUsers;
  }

  function renderPlexUserList() {
    const listEl = $("plex_user_list"); if (!listEl) return;
    const qv = ($("plex_user_filter")?.value || "").trim().toLowerCase();
    const rank = { owner:0, managed:1, friend:2 };
    const by = new Map();

    // dedup by username/title; prefer PMS id, then better type, then smaller id
    for (const u of (__plexUsers || [])) {
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

  // ---------- Libraries
  async function plexLoadLibraries() {
    try {
      const r = await fetch("/api/plex/libraries" + bust(), { cache: "no-store" }); if (!r.ok) return [];
      const j = await r.json(); const libs = Array.isArray(j?.libraries) ? j.libraries : [];
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
      fill("plex_lib_history"); fill("plex_lib_ratings");
      getPlexState().libs = libs.map(it => ({ id: String(it.key), title: String(it.title), type: String(it.type || "lib") }));
      return libs;
    } catch { return []; }
  }

  // --- refresh libs (used by the "Load libraries" button) ---
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

  // ---------- Matrix UI
  function mountPlexLibraryMatrix() {
    const host    = $("plex_lib_matrix");
    const histSel = $("plex_lib_history");
    const rateSel = $("plex_lib_ratings");
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
      host.innerHTML = libs.length ? libs.map(rowHTML).join("") : `<div class="sub">No libraries loaded.</div>`;
      applyFilter();
      setSelFromSet(histSel, st.hist);
      setSelFromSet(rateSel, st.rate);
    }

    function toggleOne(id, which) {
      if (which === "hist") { st.hist.has(id) ? st.hist.delete(id) : st.hist.add(id); }
      else { st.rate.has(id) ? st.rate.delete(id) : st.rate.add(id); }
      render();
    }

    if (firstMount) {
      host.addEventListener("click", (ev) => {
        const btn = ev.target.closest(".lm-dot"); if (!btn) return;
        const row = ev.target.closest(".lm-row"); const id = row?.dataset?.id; if (!id) return;
        toggleOne(id, btn.classList.contains("hist") ? "hist" : "rate");
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
    }

    (async () => {
      if (!getPlexState().libs.length) await plexLoadLibraries();
      render(); // always render, even on subsequent calls
    })();
  }

  // ---------- Read UI on save
  function readMatrixSelection(which) {
    const host = $("plex_lib_matrix");
    if (!host) return null;
    const sels = new Set();
    const sel = which === "hist" ? ".lm-dot.hist.on" : ".lm-dot.rate.on";
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
    const v = sel => { const el = q(sel); return el ? String(el.value || "").trim() : ""; };
    cfg = cfg || (w.__cfg ||= {});
    const plex = (cfg.plex = cfg.plex || {});
    const url = v("#plex_server_url");
    const user = v("#plex_username");
    const aid  = v("#plex_account_id");
    if (url)  plex.server_url = url;
    if (user) plex.username   = user;
    if (aid !== "") { const n = parseInt(aid, 10); if (Number.isFinite(n)) plex.account_id = n; }
    let hist = readMatrixSelection("hist");
    let rate = readMatrixSelection("rate");
    if (hist === null) hist = readSelectInts("#plex_lib_history") || [];
    if (rate === null) rate = readSelectInts("#plex_lib_ratings") || [];
    plex.history = Object.assign({}, plex.history || {}, { libraries: hist });
    plex.ratings = Object.assign({}, plex.ratings || {}, { libraries: rate });
    return cfg;
  }

  // track if server URL changed (so we can refresh after save)
  let __plexUrlDirty = false;

  // ---------- Hook save
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

  // ---------- Lifecycle
  d.addEventListener("DOMContentLoaded", () => {
    hookPlexSave();
    setTimeout(() => { try { hydratePlexFromConfigRaw(); } catch {} }, 100);
    try { mountPlexLibraryMatrix(); } catch {}
    try { mountPlexUserPicker(); } catch {}
    // remember initial URL to detect changes
    try { w.__lastPlexUrl = $("#plex_server_url")?.value?.trim() || ""; } catch {}
  });

  d.addEventListener("tab-changed", async (ev) => {
    const onSettings = ev?.detail?.id ? /settings/i.test(ev.detail.id) : !!q("#sec-plex");
    if (onSettings) {
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

  // ---------- exports
  Object.assign(w, {
    setPlexSuccess, requestPlexPin, startPlexTokenPoll, plexDeleteToken,
    mergePlexIntoCfg, plexAuto, plexLoadLibraries,
    hydratePlexFromConfigRaw, mountPlexLibraryMatrix,
    openPlexUserPicker, closePlexUserPicker, mountPlexUserPicker,
    refreshPlexLibraries,
  });

})(window, document);