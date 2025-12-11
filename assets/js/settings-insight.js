/* assets/settings-insight.js */
(function (w, d) {
  "use strict";

  // Helpers
  const $  = (sel, root) => (root || d).querySelector(sel);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  async function fetchJSON(url){ try{ const r=await fetch(url,{cache:"no-store"}); if(!r.ok) return null; return await r.json(); }catch{ return null; } }
  function toLocal(v){
    if (v===undefined||v===null||v==="") return "—";
    if (typeof v==="number"){ const ms = v<10_000_000_000 ? v*1000 : v; const dt=new Date(ms); return isNaN(+dt)?"—":dt.toLocaleString(undefined,{hour12:false}); }
    const dt=new Date(v); return isNaN(+dt)?"—":dt.toLocaleString(undefined,{hour12:false});
  }
  const coalesceNextRun = o => o ? (o.next_run_at ?? o.next_run ?? o.next ?? null) : null;

  // Styles
  const css = `
  #cw-settings-grid{
    width:100%; margin-top:12px; display:grid;
    grid-template-columns:minmax(560px,1fr) 380px;
    gap:28px; align-items:start;
  }
  #cw-settings-grid > *{ margin-top:0 !important; }
  @media (max-width:1280px){ #cw-settings-grid{ display:block; } }

  #cw-settings-insight{ position:sticky; top:12px; }

  .si-card{
    position: relative;
    border-radius:18px;
    background: rgba(13,15,22,0.96);
    border: 1px solid rgba(120,128,160,0.14);
    box-shadow:
      0 0 0 1px rgba(160,140,255,0.06),
      0 0 22px rgba(160,140,255,0.10),
      0 10px 24px rgba(0,0,0,0.45),
      inset 0 1px 0 rgba(255,255,255,0.02);
    overflow:hidden;
  }
  .si-card::after {
    content: "";
    position: absolute;
    inset: 0;
    background: url("/assets/img/background.svg") center/220px no-repeat;
    opacity: 0.12;
    pointer-events: none;
  }

  .si-header{
    padding:14px 16px;
    background: linear-gradient(180deg, rgba(16,18,26,0.98), rgba(13,15,22,0.96));
    border-bottom: 1px solid rgba(120,128,160,0.14);
    position: relative; z-index: 2;
  }
  .si-title{ font-size:16px; font-weight:800; letter-spacing:.2px; color:#E6EAFF; }

  #cw-si-scroll{ overflow:auto; overscroll-behavior:contain; position: relative; z-index: 2; }
  .si-body{ padding:6px 10px; }

  .si-row{ display:flex; align-items:center; gap:12px; padding:10px 6px; }
  .si-row + .si-row{ border-top:1px solid rgba(120,128,160,0.10); }

  .si-ic {
    display: flex;
    align-items: center;
    justify-content: center;
    flex: 0 0 auto;
    margin-right: 4px;
    background:none!important;
    border:none!important;
  }
  .si-ic .material-symbols-rounded {
    font-size: 30px;
    color: #d0d4e8;
    opacity: 0.95;
  }

  .si-col { display: flex; flex-direction: column; }
  .si-h { color: #E6EAFD; font-weight: 700; line-height: 1.2; }
  .si-one { color: #C3CAE3; font-size: 13px; margin-top: 2px; }

  /* Whitelisting block (Option A) */
  .si-wl { display:flex; flex-direction:column; gap:8px; margin-top:2px; }
  .si-wl-level { display:flex; flex-direction:column; gap:6px; }
  .si-wl-level-title{
    font-size:12px; font-weight:700; letter-spacing:.2px;
    color:#D6DBF0; opacity:.9; margin-left:2px;
  }
  .si-wl-list{ display:flex; flex-direction:column; gap:4px; padding-left:2px; }
  .si-wl-item{
    display:flex; align-items:center; justify-content:space-between; gap:8px;
    padding:2px 0;
  }
  .si-wl-name{
    color:#E6EAFD; font-weight:600; font-size:13px; white-space:nowrap;
  }
  .si-wl-chips{ display:flex; flex-wrap:wrap; gap:6px; justify-content:flex-end; }
  .si-chip{
    display:inline-flex; align-items:center; gap:4px;
    font-size:11px; font-weight:700; letter-spacing:.2px;
    color:#C9CFF0;
    padding:2px 6px;
    border-radius:6px;
    background: rgba(140,120,255,0.08);
    border: 1px solid rgba(140,120,255,0.22);
    line-height:1.2;
  }

  /* Wizard (empty state) */
  .si-empty {
    display: flex;
    align-items: flex-start;
    gap: 16px;
    padding: 26px 22px 28px 22px;
    text-align: left;
  }
  .si-empty .hero-ic { flex: 0 0 auto; display: flex; align-items: center; justify-content: center; }
  .si-empty .hero-ic .material-symbols-rounded { font-size: 42px; color: #e6eaff; opacity: 0.95; }
  .si-empty .hero-text { display: flex; flex-direction: column; gap: 6px; }
  .si-empty .h1 { color:#E8ECFF; font-size:18px; font-weight:800; margin:0; }
  .si-empty .p  { color:#C3CAE3; font-size:14px; line-height:1.45; margin:0; }
  .si-empty .tip{
    margin-top:10px; font-size:13px; color:#CFCFF5;
    background: rgba(140,120,255,0.08);
    border: 1px solid rgba(140,120,255,0.18);
    border-radius: 8px; padding: 6px 10px;
  }
  `;

  function ensureStyle(){ if($("#cw-settings-insight-style")) return; const s=d.createElement("style"); s.id="cw-settings-insight-style"; s.textContent=css; d.head.appendChild(s); }

  function ensureGrid(){
    const page=$("#page-settings"); if(!page) return null;
    let left=page.querySelector("#cw-settings-left, .settings-wrap, .settings, .accordion, .content, .page-inner");
    if(!left) left=page.firstElementChild||page;

    let grid=page.querySelector("#cw-settings-grid");
    if(!grid){
      grid=d.createElement("div"); grid.id="cw-settings-grid";
      left.parentNode.insertBefore(grid, left); grid.appendChild(left);
      left.style.marginTop="0"; if(!left.style.paddingTop) left.style.paddingTop="0";
      left.id=left.id||"cw-settings-left";
    }

    let aside=$("#cw-settings-insight", grid);
    if(!aside){ aside=d.createElement("aside"); aside.id="cw-settings-insight"; grid.appendChild(aside); }
    return { grid, left, right:aside };
  }

  function ensureCard(){
    const nodes=ensureGrid(); if(!nodes) return null;
    const { right }=nodes;
    if(!$(".si-card", right)){
      right.innerHTML=`
        <div class="si-card">
          <div class="si-header"><div class="si-title">Settings Insight</div></div>
          <div id="cw-si-scroll"><div class="si-body" id="cw-si-body"></div></div>
        </div>`;
    }
    return nodes;
  }

  // Data fetching
  async function readConfig() {
    const cfg = await fetchJSON("/api/config?t=" + Date.now());
    return cfg || {};
  }

  async function getAuthSummary(cfg) {
    const list = await fetchJSON("/api/auth/providers?t=" + Date.now());
    const detected = Array.isArray(list) ? list.length : 4;

    const plexOK     = !!(cfg?.plex?.account_token);
    const simklOK    = !!(cfg?.simkl?.access_token);
    const traktOK    = !!(cfg?.trakt?.access_token);
    const jellyfinOK = !!(cfg?.jellyfin?.access_token || cfg?.jellyfin?.user_id);

    const configured = [plexOK, simklOK, traktOK, jellyfinOK].filter(Boolean).length;
    return { detected, configured };
  }

  async function getPairsSummary(cfg) {
    let list = await fetchJSON("/api/pairs?t=" + Date.now());
    if (!Array.isArray(list)) {
      const a = cfg?.pairs || cfg?.connections || [];
      list = Array.isArray(a) ? a : [];
    }
    return { count: list.length };
  }

  async function getMetadataSummary() {
    const [cfg, mansRaw] = await Promise.all([
      fetchJSON("/api/config?t=" + Date.now()),
      fetchJSON("/api/metadata/providers?t=" + Date.now())
    ]);

    const mans = Array.isArray(mansRaw) ? mansRaw : [];
    let detected = mans.length;

    const rawKey     = String(cfg?.tmdb?.api_key ?? "").trim();
    const isMasked   = rawKey.length > 0 && /^[•]+$/.test(rawKey);
    const hasTmdbKey = rawKey.length > 0 || isMasked;

    let configured = hasTmdbKey ? 1 : 0;

    if (detected > 0) {
      configured = 0;
      let hasTmdbProvider = false;

      for (const m of mans) {
        const id     = String(m?.id || m?.name || "").toLowerCase();
        const isTmdb = id.includes("tmdb");
        if (isTmdb) hasTmdbProvider = true;

        const enabled = isTmdb && hasTmdbKey ? true : (m?.enabled !== false);

        let ready = (typeof m?.ready === "boolean") ? m.ready
                : (typeof m?.ok    === "boolean") ? m.ok
                : undefined;

        if (isTmdb && hasTmdbKey) ready = true;

        if (enabled && ready === true) configured++;
      }

      if (hasTmdbKey && !hasTmdbProvider) configured += 1;
    }

    if (detected === 0 && hasTmdbKey) detected = 1;

    return { detected, configured };
  }

  async function getSchedulingSummary(){
    const cfg = await fetchJSON("/api/scheduling?t=" + Date.now());
    const st  = await fetchJSON("/api/scheduling/status?t=" + Date.now());
    const enabled = !!(cfg && cfg.enabled);
    let next = coalesceNextRun(st); if (next===null) next = coalesceNextRun(cfg);
    return { enabled, nextRun: next };
  }

  async function getScrobblerSummary(cfg){
    const sc=cfg?.scrobble||{}; const mode=(sc?.mode||"").toLowerCase(); const enabled=!!sc?.enabled;
    let watcher={ alive:false, has_watch:false, stop_set:false };
    if(enabled && mode==="watch"){ const s=await fetchJSON("/api/watch/status"); watcher={ alive:!!s?.alive, has_watch:!!s?.has_watch, stop_set:!!s?.stop_set }; }
    return { mode: enabled ? (mode||"webhook") : "", enabled, watcher };
  }

  // Whitelisting summary
  function getWhitelistingSummary(cfg){
    const providers = [
      { key:"plex", label:"Plex" },
      { key:"emby", label:"Emby" },
      { key:"jellyfin", label:"Jellyfin" }
    ];
    const cats = ["history","ratings","watchlist","playlists"];

    const serverActive = [];
    for (const p of providers) {
      const base = cfg?.[p.key] || {};
      const byCat = {};

      for (const c of cats) {
        const libs = base?.[c]?.libraries;
        if (Array.isArray(libs) && libs.length > 0) byCat[c] = libs.length;
      }

      if (Object.keys(byCat).length) serverActive.push({ label:p.label, byCat });
    }

    const pairsRaw = cfg?.pairs || cfg?.connections || [];
    const pairs = Array.isArray(pairsRaw) ? pairsRaw : [];

    const pairActive = [];
    const seen = new Set();
    const provKeys = ["PLEX","EMBY","JELLYFIN"];
    const provLabel = { PLEX:"Plex", EMBY:"Emby", JELLYFIN:"Jellyfin" };

    for (const pair of pairs) {
      const feats = pair?.features || {};
      const provSets = { PLEX:new Set(), EMBY:new Set(), JELLYFIN:new Set() };

      for (const fk of Object.keys(feats)) {
        const libsObj = feats?.[fk]?.libraries;
        if (!libsObj || typeof libsObj !== "object") continue;

        for (const pk of provKeys) {
          const libs = libsObj?.[pk];
          if (Array.isArray(libs) && libs.length > 0) libs.forEach(x => provSets[pk].add(String(x)));
        }
      }

      const byProv = {};
      for (const pk of provKeys) {
        if (provSets[pk].size > 0) byProv[provLabel[pk]] = provSets[pk].size;
      }

      if (Object.keys(byProv).length) {
        const s = String(pair?.source || "").toUpperCase();
        const t = String(pair?.target || "").toUpperCase();
        const label = (s && t) ? `${s}→${t}` : String(pair?.id || "pair");
        if (!seen.has(label)) {
          seen.add(label);
          pairActive.push({ label, byProv });
        }
      }
    }

    return { serverActive, pairActive };
  }

  
  // UI Rendering
  const I = (name, size) => `<span class="material-symbols-rounded" style="font-size:${size||30}px">${name}</span>`;

  function row(iconName, title, oneLine){
    const el=d.createElement("div"); el.className="si-row";
    el.innerHTML=`
      <div class="si-ic">${I(iconName, 30)}</div>
      <div class="si-col">
        <div class="si-h">${title}</div>
        <div class="si-one">${oneLine}</div>
      </div>`;
    return el;
  }

  function whitelistHTML(wl){
    if (!wl) return "";
    const { serverActive, pairActive } = wl;
    if (!serverActive.length && !pairActive.length) return "";

    const catShort = { history:"H", ratings:"R", watchlist:"W", playlists:"P" };

    const out = [];
    out.push(`<div class="si-wl">`);

    if (serverActive.length) {
      out.push(`<div class="si-wl-level">`);
      out.push(`<div class="si-wl-level-title">Server-level</div>`);
      out.push(`<div class="si-wl-list">`);
      for (const s of serverActive) {
        const chips = Object.keys(s.byCat)
          .map(c => `<span class="si-chip">${catShort[c] || c[0].toUpperCase()} ${s.byCat[c]}</span>`)
          .join("");
        out.push(`
          <div class="si-wl-item">
            <span class="si-wl-name">${s.label}</span>
            <span class="si-wl-chips">${chips}</span>
          </div>
        `);
      }
      out.push(`</div></div>`);
    }

    if (pairActive.length) {
      out.push(`<div class="si-wl-level">`);
      out.push(`<div class="si-wl-level-title">Pair-level</div>`);
      out.push(`<div class="si-wl-list">`);
      for (const p of pairActive) {
        const chips = Object.keys(p.byProv)
          .map(k => `<span class="si-chip">${k} ${p.byProv[k]}</span>`)
          .join("");
        out.push(`
          <div class="si-wl-item">
            <span class="si-wl-name">${p.label}</span>
            <span class="si-wl-chips">${chips}</span>
          </div>
        `);
      }
      out.push(`</div></div>`);
    }

    out.push(`</div>`);
    return out.join("");
  }

  function renderWizard() {
    const body = $("#cw-si-body"); if (!body) return;
    body.innerHTML = `
      <div class="si-empty">
        <div class="hero-ic">${I("lock", 42)}</div>
        <div class="hero-text">
          <div class="h1">No authentication providers configured</div>
          <p class="p">Hey! You don’t have any authentication providers configured yet.<br>
          To start syncing, you need at least two of them.</p>
          <div class="tip">💡 Tip: configure at least one provider (e.g. Plex, Trakt, SIMKL or Jellyfin). The local CrossWatch tracker can work on top of that.</div>
        </div>
      </div>
    `;
  }

  function renderPairsWizard() {
    const body = $("#cw-si-body"); if (!body) return;
    body.innerHTML = `
      <div class="si-empty">
        <div class="hero-ic">${I("link", 42)}</div>
        <div class="hero-text">
          <div class="h1">No synchronization pairs/Scrobbler configured</div>
          <p class="p">Authentication looks good. Next step: create at least one pair under <b>Synchronization providers</b><br>
          — <i>or/and</i> enable the <b>Scrobbler</b> (Webhook or Watcher) if you only want live progress tracking.</p>
          <div class="tip">💡 Tip: pick a source and a target (e.g., <b>Plex → Trakt</b>), or/and toggle Webhook/Watcher under <b>Scrobbler</b>.</div>
        </div>
      </div>
    `;
  }

  function render(data){
    const body=$("#cw-si-body"); if(!body) return;

    if (!data.auth.configured) return renderWizard();

    const scrobReady = !!(data?.scrob?.enabled);
    if (data.auth.configured && data.pairs.count === 0 && !scrobReady) return renderPairsWizard();

    body.innerHTML="";
    body.appendChild(row("lock","Authentication Providers",
      `Detected providers: ${data.auth.detected}, Configured: ${data.auth.configured}`));
    body.appendChild(row("link","Synchronization Pairs",  `Pairs: ${data.pairs.count}`));

    const wlBlock = whitelistHTML(data.whitelist);
    if (wlBlock) body.appendChild(row("filter_alt","Whitelisting", wlBlock));

    if (data.meta.configured === 0) {
      body.appendChild(row(
        "image",
        "Metadata Providers",
        `You're missing out on some great stuff.<br><b>Configure a Metadata Provider</b> ✨`
      ));
    } else {
      body.appendChild(row(
        "image",
        "Metadata Providers",
        `Detected providers: ${data.meta.detected}, Configured: ${data.meta.configured}`
      ));
    }
    body.appendChild(row("schedule","Scheduling",
      data.sched.enabled ? `Enabled | Next run: ${toLocal(data.sched.nextRun)}` : "Disabled"));
    const mode = !data.scrob.enabled ? "Disabled" : (data.scrob.mode==="watch" ? "Watcher mode" : "Webhook mode");
    const status = !data.scrob.enabled ? "" : (data.scrob.mode==="watch" ? (data.scrob.watcher.alive ? "Running" : "Stopped") : "—");
    body.appendChild(row("sensors","Scrobbler", `${mode}${mode && status ? " | " : ""}${status}`));
  }


  // Layout sync
  function syncHeight(){
    const left=$("#cw-settings-left") || $("#page-settings .settings-wrap, #page-settings .settings, #page-settings .accordion, #page-settings .content, #page-settings .page-inner");
    const scroll=$("#cw-si-scroll");
    if(!left || !scroll) return;
    const rect=left.getBoundingClientRect();
    const top=12, maxViewport=Math.max(200,(w.innerHeight-top-16)), maxByLeft=Math.max(200,rect.height);
    scroll.style.maxHeight = `${Math.min(maxByLeft, maxViewport)}px`;
  }
  // Main loop
  let _loopTimer = null;

  async function tick(){
    const nodes=ensureCard(); if(!nodes){ _loopTimer = setTimeout(tick,1200); return; }
    const page=$("#page-settings"); const visible=!!(page && !page.classList.contains("hidden"));
    if(visible){
      const cfg=await readConfig();
      const [auth,pairs,meta,sched,scrob]=await Promise.all([
        getAuthSummary(cfg), getPairsSummary(cfg), getMetadataSummary(), getSchedulingSummary(), getScrobblerSummary(cfg)
      ]);
      const whitelist = getWhitelistingSummary(cfg);
      render({auth,pairs,meta,sched,scrob,whitelist});
      syncHeight();
      if (_loopTimer) clearTimeout(_loopTimer);
      _loopTimer = setTimeout(tick, 10000);
    }else{
      if (_loopTimer) clearTimeout(_loopTimer);
      _loopTimer = setTimeout(tick, 3000);
    }
  }
  // Bootstrapping
  (async function boot(){
    if(!$("#cw-settings-insight-style")){ const s=d.createElement("style"); s.id="cw-settings-insight-style"; s.textContent=css; d.head.appendChild(s); }

    d.addEventListener("tab-changed",(e)=>{ if(e?.detail?.id==="settings") setTimeout(()=>{ tick(); syncHeight(); },150); });

    d.addEventListener("config-saved", (e) => {
      const sec = e?.detail?.section;
      if (!sec || sec === "scheduling" || sec === "auth" || sec === "sync" || sec === "pairs" || sec === "connections") {
        tick();
      }
    });
    d.addEventListener("scheduling-status-refresh", () => tick());
    d.addEventListener("visibilitychange", () => { if (!d.hidden) tick(); });
    w.addEventListener("focus", () => tick());

    let tries=0; while(!$("#page-settings") && tries<40){ tries++; await sleep(250); }
    tick();
    w.addEventListener("resize", syncHeight);
    w.addEventListener("scroll", syncHeight, { passive:true });

    w.refreshSettingsInsight = () => tick();
  })();
})(window, document);
