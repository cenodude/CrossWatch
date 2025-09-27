/* assets/settings-insight.js */
(function (w, d) {
  "use strict";

  const $  = (sel, root) => (root || d).querySelector(sel);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  async function fetchJSON(url){ try{ const r=await fetch(url,{cache:"no-store"}); if(!r.ok) return null; return await r.json(); }catch{ return null; } }
  function toLocal(v){
    if (v===undefined||v===null||v==="") return "—";
    if (typeof v==="number"){ const ms = v<10_000_000_000 ? v*1000 : v; const dt=new Date(ms); return isNaN(+dt)?"—":dt.toLocaleString(undefined,{hour12:false}); }
    const dt=new Date(v); return isNaN(+dt)?"—":dt.toLocaleString(undefined,{hour12:false});
  }
  const coalesceNextRun = o => o ? (o.next_run_at ?? o.next_run ?? o.next ?? null) : null;

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

  // Ensure the settings insight stylesheet is present
  function ensureStyle(){ if($("#cw-settings-insight-style")) return; const s=d.createElement("style"); s.id="cw-settings-insight-style"; s.textContent=css; d.head.appendChild(s); }

  // Create a two-column settings grid and an aside for the insight card if needed
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

  // Ensure the insight card exists in the aside (creates markup if missing)
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

// Read current config (no caching)
async function readConfig() {
  const cfg = await fetchJSON("/api/config?t=" + Date.now());
  return cfg || {};
}

// Summarize authentication providers (detected via backend; configured via tokens)
async function getAuthSummary(cfg) {
  // Ask backend which auth providers exist
  const list = await fetchJSON("/api/auth/providers?t=" + Date.now());
  const detected = Array.isArray(list) ? list.length : 4; // fallback to known set

  // Count providers that actually have valid credentials/tokens
  const plexOK     = !!(cfg?.plex?.account_token);
  const simklOK    = !!(cfg?.simkl?.access_token);
  const traktOK    = !!(cfg?.trakt?.access_token);
  // Jellyfin considered configured if we have an access token or a resolved user_id
  const jellyfinOK = !!(cfg?.jellyfin?.access_token || cfg?.jellyfin?.user_id);

  const configured = [plexOK, simklOK, traktOK, jellyfinOK].filter(Boolean).length;
  return { detected, configured };
}

  // Retrieve synchronization pairs (prefer API; fallback to config)
  async function getPairsSummary(cfg) {
    let list = await fetchJSON("/api/pairs?t=" + Date.now());
    if (!Array.isArray(list)) {
      const a = cfg?.pairs || cfg?.connections || [];
      list = Array.isArray(a) ? a : [];
    }
    return { count: list.length };
  }

  // Summarize metadata providers and TMDB key readiness
  async function getMetadataSummary() {
    const [cfg, mansRaw] = await Promise.all([
      fetchJSON("/api/config?t=" + Date.now()),
      fetchJSON("/api/metadata/providers?t=" + Date.now())
    ]);

    const mans = Array.isArray(mansRaw) ? mansRaw : [];
    let detected = mans.length;

    // TMDB key may be present or masked (e.g., "••••••••")
    const rawKey     = String(cfg?.tmdb?.api_key ?? "").trim();
    const isMasked   = rawKey.length > 0 && /^[•]+$/.test(rawKey);
    const hasTmdbKey = rawKey.length > 0 || isMasked;

    // Default: if provider manifests fail to load, still count TMDB when a key exists
    let configured = hasTmdbKey ? 1 : 0;

    if (detected > 0) {
      configured = 0;
      let hasTmdbProvider = false;

      for (const m of mans) {
        const id     = String(m?.id || m?.name || "").toLowerCase();
        const isTmdb = id.includes("tmdb");
        if (isTmdb) hasTmdbProvider = true;

        // Treat TMDB as enabled if a key exists (ignore provider's enabled flag)
        const enabled = isTmdb && hasTmdbKey ? true : (m?.enabled !== false);

        // Derive readiness; then force TMDB ready if a key exists
        let ready = (typeof m?.ready === "boolean") ? m.ready
                : (typeof m?.ok    === "boolean") ? m.ok
                : undefined;

        if (isTmdb && hasTmdbKey) ready = true;

        if (enabled && ready === true) configured++;
      }

      // If TMDB key exists but TMDB provider isn't listed, still count it as configured
      if (hasTmdbKey && !hasTmdbProvider) configured += 1;
    }

    // If manifests failed but we do have a TMDB key, report at least one detected
    if (detected === 0 && hasTmdbKey) detected = 1;

    return { detected, configured };
  }

  // Get scheduling enabled state and next run time
  async function getSchedulingSummary(){
    const cfg = await fetchJSON("/api/scheduling?t=" + Date.now());
    const st  = await fetchJSON("/api/scheduling/status?t=" + Date.now());
    const enabled = !!(cfg && cfg.enabled);
    let next = coalesceNextRun(st); if (next===null) next = coalesceNextRun(cfg);
    return { enabled, nextRun: next };
  }
  // Get scrobbler (webhook/watcher) summary and watcher status when applicable
  async function getScrobblerSummary(cfg){
    const sc=cfg?.scrobble||{}; const mode=(sc?.mode||"").toLowerCase(); const enabled=!!sc?.enabled;
    let watcher={ alive:false, has_watch:false, stop_set:false };
    if(enabled && mode==="watch"){ const s=await fetchJSON("/debug/watch/status"); watcher={ alive:!!s?.alive, has_watch:!!s?.has_watch, stop_set:!!s?.stop_set }; }
    return { mode: enabled ? (mode||"webhook") : "", enabled, watcher };
  }

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

  // Empty state when no authentication providers are configured
  function renderWizard() {
    const body = $("#cw-si-body"); if (!body) return;
    body.innerHTML = `
      <div class="si-empty">
        <div class="hero-ic">${I("lock", 42)}</div>
        <div class="hero-text">
          <div class="h1">No authentication providers configured</div>
          <p class="p">Hey! You don’t have any authentication providers configured yet.<br>
          To start syncing, you need at least two of them.</p>
          <div class="tip">💡 Tip: configure Plex + SIMKL or Plex + Trakt to enable synchronization features.</div>
        </div>
      </div>
    `;
  }

  // Empty state when no synchronization pairs exist
  function renderPairsWizard() {
    const body = $("#cw-si-body"); if (!body) return;
    body.innerHTML = `
      <div class="si-empty">
        <div class="hero-ic">${I("link", 42)}</div>
        <div class="hero-text">
          <div class="h1">No synchronization pairs configured</div>
          <p class="p">Authentication looks good. Next step: create at least one pair under <b>Synchronization providers</b>.</p>
          <div class="tip">💡 Tip: pick a source and a target (e.g., <b>Plex → Trakt</b> or <b>Plex → SIMKL</b>), then save.</div>
        </div>
      </div>
    `;
  }

  // Render the insight rows based on collected summaries
  function render(data){
    const body=$("#cw-si-body"); if(!body) return;

    // Show the right empty state
    if (!data.auth.configured) return renderWizard();
    if (data.auth.configured && data.pairs.count === 0) return renderPairsWizard();

    // Normal info rows
    body.innerHTML="";
    body.appendChild(row("lock","Authentication Providers",
      `Detected providers: ${data.auth.detected}, Configured: ${data.auth.configured}`));
    body.appendChild(row("link","Synchronization Pairs",  `Pairs: ${data.pairs.count}`));
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

  // Adjust the insight card height to match the left column and viewport
  function syncHeight(){
    const left=$("#cw-settings-left") || $("#page-settings .settings-wrap, #page-settings .settings, #page-settings .accordion, #page-settings .content, #page-settings .page-inner");
    const scroll=$("#cw-si-scroll");
    if(!left || !scroll) return;
    const rect=left.getBoundingClientRect();
    const top=12, maxViewport=Math.max(200,(w.innerHeight-top-16)), maxByLeft=Math.max(200,rect.height);
    scroll.style.maxHeight = `${Math.min(maxByLeft, maxViewport)}px`;
  }

  // Prevent multiple overlapping refresh loops
  let _loopTimer = null;

  // Periodic refresh loop: gather data and render when settings page is visible
  async function tick(){
    const nodes=ensureCard(); if(!nodes){ _loopTimer = setTimeout(tick,1200); return; }
    const page=$("#page-settings"); const visible=!!(page && !page.classList.contains("hidden"));
    if(visible){
      const cfg=await readConfig();
      const [auth,pairs,meta,sched,scrob]=await Promise.all([
        getAuthSummary(cfg), getPairsSummary(cfg), getMetadataSummary(), getSchedulingSummary(), getScrobblerSummary(cfg)
      ]);
      render({auth,pairs,meta,sched,scrob});
      syncHeight();
      if (_loopTimer) clearTimeout(_loopTimer);
      _loopTimer = setTimeout(tick, 10000);
    }else{
      if (_loopTimer) clearTimeout(_loopTimer);
      _loopTimer = setTimeout(tick, 3000);
    }
  }

  (async function boot(){
    // Ensure style is present
    if(!$("#cw-settings-insight-style")){ const s=d.createElement("style"); s.id="cw-settings-insight-style"; s.textContent=css; d.head.appendChild(s); }

    // Refresh when user navigates to the settings tab
    d.addEventListener("tab-changed",(e)=>{ if(e?.detail?.id==="settings") setTimeout(()=>{ tick(); syncHeight(); },150); });

    // Refresh on saves (including pairs/connections/sync)
    d.addEventListener("config-saved", (e) => {
      const sec = e?.detail?.section;
      if (!sec || sec === "scheduling" || sec === "auth" || sec === "sync" || sec === "pairs" || sec === "connections") {
        tick();
      }
    });
    d.addEventListener("scheduling-status-refresh", () => tick());
    d.addEventListener("visibilitychange", () => { if (!d.hidden) tick(); });
    w.addEventListener("focus", () => tick());

    // Wait for the settings page to exist, but don't wait forever
    let tries=0; while(!$("#page-settings") && tries<40){ tries++; await sleep(250); }
    tick();
    w.addEventListener("resize", syncHeight);
    w.addEventListener("scroll", syncHeight, { passive:true });

    // Manual refresh hook for other modules
    w.refreshSettingsInsight = () => tick();
  })();
})(window, document);
