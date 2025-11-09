# _FastAPI.py
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response
from starlette.staticfiles import StaticFiles

__all__ = ["register_assets_and_favicons", "register_ui_root", "get_index_html"]

# ── Static favicon (shared by /favicon.svg and /favicon.ico)
FAVICON_SVG: str = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
<defs><linearGradient id="g" x1="0" y1="0" x2="64" y2="64" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#2de2ff"/><stop offset="0.5" stop-color="#7c5cff"/><stop offset="1" stop-color="#ff7ae0"/></linearGradient></defs>
<rect width="64" height="64" rx="14" fill="#0b0b0f"/>
<rect x="10" y="16" width="44" height="28" rx="6" fill="none" stroke="url(#g)" stroke-width="3"/>
<rect x="24" y="46" width="16" height="3" rx="1.5" fill="url(#g)"/>
<circle cx="20" cy="30" r="2.5" fill="url(#g)"/>
<circle cx="32" cy="26" r="2.5" fill="url(#g)"/>
<circle cx="44" cy="22" r="2.5" fill="url(#g)"/>
<path d="M20 30 L32 26 L44 22" fill="none" stroke="url(#g)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"""


def register_assets_and_favicons(app: FastAPI, root: Path) -> None:
    assets_dir = root / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    def _svg_resp() -> Response:
        return Response(
            content=FAVICON_SVG,
            media_type="image/svg+xml",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    @app.get("/favicon.svg", include_in_schema=False, tags=["ui"])
    def favicon_svg() -> Response:  # noqa: D401
        return _svg_resp()

    @app.get("/favicon.ico", include_in_schema=False, tags=["ui"])
    def favicon_ico() -> Response:  # serve SVG for legacy path
        return _svg_resp()

def register_ui_root(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False, tags=["ui"])
    def ui_root() -> HTMLResponse:
        return HTMLResponse(get_index_html(), headers={"Cache-Control": "no-store"})

def get_index_html() -> str:
    return r"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CrossWatch | Sync-licious</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg"><link rel="alternate icon" href="/favicon.ico">

<link rel="stylesheet" href="/assets/crosswatch.css">

<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded" rel="stylesheet" />
<style>
  .material-symbol{font-family:'Material Symbols Rounded';font-weight:normal;font-style:normal;font-size:1em;line-height:1;display:inline-block;vertical-align:middle;-webkit-font-feature-settings:'liga';-webkit-font-smoothing:antialiased}
  .pair-selectors,button[onclick="addPair()"],#batches_list,button[onclick="addBatch()"],button[onclick="runAllBatches()"]{display:none!important}
  #providers_list.grid2{display:block!important}#providers_list .pairs-board{display:flex;flex-direction:column;align-items:flex-start;text-align:left}
  #save-frost{position:fixed;left:0;right:0;bottom:0;height:84px;background:linear-gradient(0deg,rgba(10,10,14,.85) 0%,rgba(10,10,14,.60) 35%,rgba(10,10,14,0) 100%);border-top:1px solid var(--border);backdrop-filter:blur(6px) saturate(110%);-webkit-backdrop-filter:blur(6px) saturate(110%);pointer-events:none;z-index:9998}
  #save-fab{position:fixed;left:0;right:0;bottom:max(12px,env(safe-area-inset-bottom));z-index:10000;display:flex;justify-content:center;align-items:center;pointer-events:none;background:transparent}
  #save-fab .btn{pointer-events:auto;position:relative;z-index:10001;padding:14px 22px;border-radius:14px;font-weight:800;text-transform:uppercase;letter-spacing:.02em;background:linear-gradient(135deg,#ff4d4f,#ff7a7a);border:1px solid #ff9a9a55;box-shadow:0 10px 28px rgba(0,0,0,.35),0 0 14px #ff4d4f55}
  #save-fab.hidden,#save-frost.hidden{display:none}
  .ops-header{display:flex;align-items:center;gap:12px}
  .ops-header-flex { display:flex; align-items:center; gap:.75rem; }
  #btn-status-refresh.sync-ctrl{
    width:32px; height:32px; border-radius:999px;
    display:inline-flex; align-items:center; justify-content:center;
    border:1px solid var(--border-color, rgba(255,255,255,.10));
    background:rgba(255,255,255,.04);
    cursor:pointer; transition:transform .15s ease, opacity .15s ease;
  }
  #btn-status-refresh.sync-ctrl:hover{ transform:scale(1.04); opacity:.95; }
  #btn-status-refresh.sync-ctrl.spinning{ animation:spin 1s linear infinite; }
  #btn-status-refresh.spinning { animation: spin 1s linear infinite; }
  #btn-status-refresh.spinning .icon { animation: spin 1s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head><body>

<header>
  <div class="brand" role="button" tabindex="0" title="Go to Main" onclick="showTab('main')" onkeypress="if(event.key==='Enter'||event.key===' ')showTab('main')">
    <svg class="logo" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="CrossWatch">
      <defs><linearGradient id="cw-g" x1="0" y1="0" x2="24" y2="24"><stop offset="0" stop-color="#2de2ff"/><stop offset=".5" stop-color="#7c5cff"/><stop offset="1" stop-color="#ff7ae0"/></linearGradient></defs>
      <rect x="3" y="4" width="18" height="12" rx="2" ry="2" stroke="url(#cw-g)" stroke-width="1.7"/>
      <rect x="8" y="18" width="8" height="1.6" rx=".8" fill="url(#cw-g)"/>
      <circle cx="8" cy="9" r="1" fill="url(#cw-g)"/><circle cx="12" cy="11" r="1" fill="url(#cw-g)"/><circle cx="16" cy="8" r="1" fill="url(#cw-g)"/>
      <path d="M8 9 L12 11 L16 8" stroke="url(#cw-g)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
    <span class="name">CrossWatch</span>
  </div>

  <div class="tabs">
    <div id="tab-main" class="tab active" onclick="showTab('main')">Main</div>
    <div id="tab-watchlist" class="tab" onclick="showTab('watchlist')">Watchlist</div>
    <div id="tab-settings" class="tab" onclick="showTab('settings')">Settings</div>
    <div id="tab-about" class="tab" onclick="openAbout()">About</div>
  </div>
</header>

<main id="layout">
  <section id="ops-card" class="card">
    <div class="title">Synchronization</div>
    <div class="ops-header">
      <div id="conn-badges" class="vip-badges" style="margin-left:auto"></div>
      <div id="update-banner" class="hidden"><span id="update-text">A new version is available.</span>
        <a id="update-link" href="https://github.com/cenodude/crosswatch/releases" target="_blank" rel="noopener">Get update</a>
      </div>
      <button id="btn-status-refresh" class="iconbtn" title="Re-check status" aria-label="Refresh status">
        <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
          <path d="M21 12a9 9 0 1 1-2.64-6.36" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M21 5v5h-5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      </button>
    </div>

    <div class="sync-status" style="display:none"><div id="sync-icon"></div><div id="sync-status-text"></div><span id="sched-inline" style="display:none"></span></div>
    <div id="ux-progress"></div><div id="ux-lanes"></div><div id="ux-spotlight"></div>

    <div class="action-row">
      <div class="action-buttons">
        <button id="run" class="btn acc" onclick="runSync()"><span class="label">Synchronize</span><span class="spinner" aria-hidden="true"></span></button>
        <button class="btn" onclick="toggleDetails()">View details</button>
        <button class="btn" onclick="openAnalyzer()">Analyzer</button>
        <button class="btn" onclick="openExporter()">Exporter</button>
        <button class="btn" onclick="downloadSummary()">Download report</button>
      </div>
    </div>

    <div id="details" class="details hidden">
      <div class="details-grid">
        <div class="det-left">
          <div class="title" style="margin-bottom:6px;font-weight:700">Sync output</div>
          <div id="det-log" class="log"></div>
        </div>
        <div class="det-right">
          <div class="meta-card">
            <div class="meta-grid">
              <div class="meta-label">Module</div><div class="meta-value"><span id="det-cmd" class="pillvalue truncate">–</span></div>
              <div class="meta-label">Version</div><div class="meta-value"><span id="det-ver" class="pillvalue">–</span></div>
              <div class="meta-label">Started</div><div class="meta-value"><span id="det-start" class="pillvalue mono">–</span></div>
              <div class="meta-label">Finished</div><div class="meta-value"><span id="det-finish" class="pillvalue mono">–</span></div>
            </div>
            <div class="meta-actions"><button class="btn" onclick="copySummary(this)">Copy summary</button><button class="btn" onclick="downloadSummary()">Download</button></div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <section id="stats-card" class="card collapsed">
    <div class="title">Statistics</div>

    <div class="stats-modern v2">
      <div class="now"><div class="label">Now</div><div id="stat-now" class="value" data-v="0">0</div><div class="chips"><span id="trend-week" class="chip trend flat">no change</span></div></div>
      <div class="facts">
        <div class="fact"><span class="k">Last Week</span><span id="stat-week" class="v" data-v="0">0</span></div>
        <div class="fact"><span class="k">Last Month</span><span id="stat-month" class="v" data-v="0">0</span></div>
        <div class="mini-legend"><span class="dot add"></span><span class="l">Added</span><span id="stat-added" class="n">0</span><span class="dot del"></span><span class="l">Removed</span><span id="stat-removed" class="n">0</span></div>
        <div class="stat-meter" aria-hidden="true"><span id="stat-fill"></span></div>
      </div>
    </div>

    <div class="stat-tiles" id="stat-providers"></div>

    <div class="stat-block">
      <div class="stat-block-header"><span class="pill plain">Recent syncs</span><button class="ghost refresh-insights" onclick="refreshInsights()" title="Refresh">⟲</button></div>
      <div id="sync-history" class="history-list"></div>
    </div>
  </section>

  <section id="placeholder-card" class="card hidden">
    <div class="title">Watchlist Preview</div>
    <div id="wall-msg" class="wall-msg">Loading…</div>
    <div class="wall-wrap">
      <div id="edgeL" class="edge left"></div><div id="edgeR" class="edge right"></div>
      <div id="poster-row" class="row-scroll" aria-label="Watchlist preview"></div>
      <button class="nav prev" type="button" onclick="scrollWall(-1)" aria-label="Scroll left">‹</button>
      <button class="nav next" type="button" onclick="scrollWall(1)" aria-label="Scroll right">›</button>
    </div>
  </section>

  <section id="page-watchlist" class="card hidden"><div class="title">Watchlist</div><div id="watchlist-root"></div></section>

  <section id="page-settings" class="card hidden">
    <div class="title">Settings</div>
    <div id="cw-settings-grid">
      <div id="cw-settings-left">
      
        <div class="section" id="sec-auth">
          <div class="head" onclick="toggleSection('sec-auth')" style="display:flex;align-items:center">
            <span class="chev">▶</span><strong>Authentication Providers</strong>
            <span style="margin-left:auto;display:flex;gap:6px;align-items:center">
              <img src="/assets/img/PLEX-log.svg" alt="Plex" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/JELLYFIN-log.svg" alt="Jellyfin" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/SIMKL-log.svg" alt="SIMKL" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/TRAKT-log.svg" alt="Trakt" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/MDBLIST-log.svg" alt="MDBList" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/EMBY-log.svg" alt="Emby" style="height:24px;width:auto;opacity:.9">
            </span>
          </div>
          <div class="body"><div id="auth-providers"></div></div>
        </div>

        <div class="section" id="sec-sync">
          <div class="head" onclick="toggleSection('sec-sync')"><span class="chev">▶</span><strong>Synchronization Providers</strong></div>
          <div class="body">
            <div class="sub">Providers</div><div id="providers_list" class="grid2"></div>
            <div class="sep"></div><div class="sub">Pairs</div><div id="pairs_list"></div>
            <div class="footer"><div class="pair-selectors" style="margin-top:1em;">
              <label style="margin-right:1em;">Source:<select id="source-provider" style="margin-left:.5em;"></select></label>
              <label>Target:<select id="target-provider" style="margin-left:.5em;"></select></label>
            </div></div>
          </div>
        </div>

        <div class="section" id="sec-meta"><div class="head" onclick="toggleSection('sec-meta')"><span class="chev">▶</span><strong>Metadata Providers</strong></div><div class="body"><div id="metadata-providers"></div></div></div>

        <div class="section" id="sec-scheduling">
          <div class="head" onclick="toggleSection('sec-scheduling')"><span class="chev">▶</span><strong>Scheduling</strong></div>
          <div class="body">
            <div class="grid2">
              <div><label>Enable</label><select id="schEnabled"><option value="false">Disabled</option><option value="true">Enabled</option></select></div>
              <div><label>Frequency</label><select id="schMode"><option value="hourly">Every hour</option><option value="every_n_hours">Every N hours</option><option value="daily_time">Daily at…</option></select></div>
              <div><label>Every N hours</label><input id="schN" type="number" min="1" max="24" value="2"></div>
              <div><label>Time</label><input id="schTime" type="time" value="03:30"></div>
            </div>
          </div>
        </div>
        
        <div class="section" id="sec-scrobbler">
          <div class="head" onclick="toggleSection('sec-scrobbler')" style="display:flex;align-items:center">
            <span class="chev">▶</span><strong>Scrobbler</strong>
            <span title="Plex/Jellyfin/Emby to Trakt" style="margin-left:auto;display:flex;gap:6px;align-items:center">
              <img src="/assets/img/PLEX-log.svg" alt="Plex" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/JELLYFIN-log.svg" alt="Jellyfin" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/TRAKT-log.svg" alt="Trakt" style="height:18px;width:auto;opacity:.9">
              <img src="/assets/img/EMBY-log.svg" alt="Emby" style="height:24px;width:auto;opacity:.9">
            </span>
          </div>
          <div class="body" id="scrobble-mount">
            <div class="section" id="sc-sec-webhook">
              <div class="head" onclick="toggleSection('sc-sec-webhook')"><span class="chev">▶</span><strong>Webhook</strong></div>
              <div class="body"><div id="scrob-webhook"></div></div>
            </div>
            <div class="section" id="sc-sec-watch">
              <div class="head" onclick="toggleSection('sc-sec-watch')"><span class="chev">▶</span><strong>Watcher</strong></div>
              <div class="body"><div id="scrob-watcher"></div></div>
            </div>
          </div>
        </div>

        <div class="section" id="sec-troubleshoot">
          <div class="head" onclick="toggleSection('sec-troubleshoot')"><span class="chev">▶</span><strong>Maintenance</strong></div>
          <div class="body">
            <div class="sub">Use these actions to reset application state. They are safe but cannot be undone.</div>
            <div><label>Debug</label><select id="debug"><option value="off">off</option><option value="on">on</option><option value="mods">on — including MOD debug</option><option value="full">on — full</option></select></div>
            <div class="chiprow"><button class="btn danger" onclick="clearState()">Clear State</button><button class="btn danger" onclick="clearCache()">Clear Cache</button><button class="btn danger" onclick="resetStats()">Reset Statistics</button></div>
            <div id="tb_msg" class="msg ok hidden">Done ✓</div>
          </div>
        </div>

      </div>
      <aside id="cw-settings-insight" aria-label="Settings Insight"></aside>
    </div>
  </section>

</main>

<script src="/assets/helpers/core.js"></script>
<script src="/assets/helpers/dom.js"></script>
<script src="/assets/helpers/events.js"></script>
<script src="/assets/helpers/api.js"></script>
<script src="/assets/helpers/legacy-bridge.js"></script>
<script src="/assets/crosswatch.js"></script>
<script src="/assets/js/syncbar.js" defer></script>
<script src="/assets/js/main.js" defer></script>
<script src="/assets/js/connections.overlay.js" defer></script>
<script src="/assets/js/connections.pairs.overlay.js" defer></script>
<script src="/assets/js/scheduler.js" defer></script>
<script src="/assets/js/schedulerbanner.js" defer></script>
<script src="/assets/js/insights.js" defer></script>
<script src="/assets/js/settings-insight.js" defer></script>
<script src="/assets/js/scrobbler.js" defer></script>

<script src="/assets/auth/auth.plex.js" defer></script>
<script src="/assets/auth/auth.simkl.js" defer></script>
<script src="/assets/auth/auth.trakt.js" defer></script>
<script src="/assets/auth/auth.jellyfin.js" defer></script>
<script src="/assets/auth/auth.emby.js" defer></script>
<script src="/assets/auth/auth.mdblist.js" defer></script>

<script src="/assets/js/client-formatter.js" defer></script>

<script>window.__CW_BUILD__="0.2.5-20251014-02";</script>
<link rel="stylesheet" href="/assets/js/modals/core/styles.css?v=0.2.5-20251014-02">
<script type="module" src="/assets/js/modals.js?v=0.2.5-20251014-02"></script>

<script>document.addEventListener('DOMContentLoaded',()=>{try{if(typeof openSummaryStream==='function')openSummaryStream()}catch(e){}});</script>

<div id="save-frost" class="hidden" aria-hidden="true"></div>
<div id="save-fab" class="hidden" role="toolbar" aria-label="Sticky save">
  <button id="save-fab-btn" class="btn" onclick="saveSettings(this)"><span class="btn-ic">✔</span> <span class="btn-label">Save</span></button>
</div>

<script>
// Accordion: one open per container (top-level and nested)
(() => {
  const isOpen = s => s.classList.contains('open');
  const open  = s => { s.classList.add('open');  s.querySelector('.head')?.setAttribute('aria-expanded','true');  const c=s.querySelector('.chev'); if(c) c.textContent='▼'; };
  const close = s => { s.classList.remove('open'); s.querySelector('.head')?.setAttribute('aria-expanded','false'); const c=s.querySelector('.chev'); if(c) c.textContent='▶'; };
  const siblingsOf = (sec) => { const p = sec?.parentElement; if (!p) return []; return Array.from(p.querySelectorAll(':scope > .section')); };

  window.toggleSection = function(id){
    const sec = document.getElementById(id); if (!sec) return;
    const was = isOpen(sec);
    siblingsOf(sec).forEach(s => { if (s !== sec) close(s); });
    was ? close(sec) : open(sec);
  };

  function initAccordion(){
    const containers = new Set();
    document.querySelectorAll('.section').forEach(s => s.parentElement && containers.add(s.parentElement));
    containers.forEach(p => {
      const secs = Array.from(p.querySelectorAll(':scope > .section'));
      const opened = secs.filter(isOpen);
      if (opened.length > 1) opened.slice(1).forEach(close);
      secs.forEach(s => (isOpen(s) ? open(s) : close(s)));
    });
  }
  document.addEventListener('DOMContentLoaded', initAccordion, { once:true });
})();
</script>

<script>
(()=>{const CROWN='<svg viewBox="0 0 64 64" fill="currentColor" aria-hidden="true"><path d="M8 20l10 8 10-14 10 14 10-8 4 26H4l4-26zM10 52h44v4H10z"/></svg>';
let __cfg=null;

async function getConfig(force=false){
  if(__cfg && !force) return __cfg;
  try{ const r=await fetch('/api/config?ts='+Date.now(),{cache:'no-store'}); __cfg=r.ok?await r.json():{}; }
  catch{ __cfg={}; }
  return __cfg;
}
function invalidateConfigCache(){ __cfg=null; }

function isProviderConfigured(key,cfg){
  const k=(key||'').toUpperCase(), c=cfg||__cfg||{};
  switch(k){
    case 'PLEX':     return !!(c?.plex?.account_token);
    case 'TRAKT':    return !!(c?.trakt?.access_token || c?.auth?.trakt?.access_token);
    case 'SIMKL':    return !!(c?.simkl?.access_token);
    case 'JELLYFIN': return !!(c?.jellyfin?.access_token);
    case 'EMBY':     return !!(c?.emby?.access_token || c?.auth?.emby?.access_token); 
    case 'MDBLIST':  return !!(c?.mdblist?.api_key);
    default: return false;
  }
}

// Connection pill; 'detail' becomes the tooltip
function makeConn({name,connected,vip,detail}){
  const w=document.createElement('div'); w.className='conn-item';
  const p=document.createElement('div'); p.className=`conn-pill ${connected?'ok':'no'}${vip?' has-vip':''}`;
  p.role='status'; p.ariaLabel=`${name} ${connected?'Connected':'Disconnected'}`;
  if(detail) p.title=detail;
  if(vip){const s=document.createElement('span'); s.className='conn-slot'; s.innerHTML=CROWN; p.appendChild(s);}
  const t=document.createElement('span'); t.className='conn-text'; t.textContent=`${name} ${connected?'Connected':'Disconnected'}`;
  p.appendChild(t); w.appendChild(p);
  return w;
}
function titleCase(k){k=String(k||'');return k? (k[0]+k.slice(1).toLowerCase()) : k;}

// Keep refresh button in a stable spot
function placeRefreshTopRight(){
  const card=document.getElementById('ops-card')||document.querySelector('.ops-header');
  const btn=document.getElementById('btn-status-refresh');
  if(!card||!btn) return;
  if(btn.parentElement!==card) card.appendChild(btn);
  btn.classList.add('sync-ctrl-fixed');
  btn.onclick=null;
  btn.removeEventListener('click',fetchAndRender,true);
  btn.addEventListener('click',fetchAndRender,true);
}
window.putRefreshBeforeTrakt=placeRefreshTopRight;
document.addEventListener('DOMContentLoaded',placeRefreshTopRight,{once:true});
(function patchFetchAndRender(){
  const orig=window.fetchAndRender;
  if(typeof orig==='function'){
    window.fetchAndRender=async function(...args){ try{ return await orig.apply(this,args);} finally{placeRefreshTopRight();} };
  }else{
    const t=setInterval(()=>{ if(typeof window.fetchAndRender==='function'){ clearInterval(t); patchFetchAndRender(); } },50);
  }
})();

// Render status; shows reason as tooltip when disconnected
function render(payload){
  const host=document.getElementById('conn-badges'); if(!host) return;
  host.classList.add('vip-badges');

  const btn=document.getElementById('btn-status-refresh');
  if(btn && host.contains(btn)) host.removeChild(btn);
  host.querySelectorAll('.conn-item').forEach(n=>n.remove());

  const P=payload?.providers||{};
  const cfg=__cfg||{};
  const keys=Object.keys(P).filter(k=>isProviderConfigured(k,cfg)).sort();

  const none=keys.length===0;
  host.classList.toggle('hidden',none);
  if(none){ const hdr=document.querySelector('.ops-header'); if(btn&&hdr) hdr.appendChild(btn); return; }

  keys.forEach(K=>{
    const d = P[K] || {};
    const LABELS = { PLEX:'Plex', TRAKT:'Trakt', SIMKL:'SIMKL', JELLYFIN:'Jellyfin', EMBY:'Emby', MDBLIST:'MDBlist' };
    const name = LABELS[K] || titleCase(K);
    const connected=!!d.connected;
    let vip=false, detail='';

    if(!connected){
      detail=d.reason || `${name} not connected`;
    }else{
      if(K.toUpperCase()==='PLEX'){
        vip=!!(d.plexpass||d.subscription?.plan);
        if(vip){ detail=`Plex Pass — ${d.subscription?.plan||'Active'}`; }
      }else if(K.toUpperCase()==='TRAKT'){
        vip=!!d.vip;
        detail = vip ? 'VIP status — Enabled' : '';
      }else if(K.toUpperCase()==='EMBY'){
        vip=!!d.premiere;
        if(vip){ detail='Premiere — Active'; }
      } else if (K.toUpperCase() === 'MDBLIST') {
        vip = !!d.vip; // crown if patron
        const lim = (d && typeof d === 'object' && d.limits && typeof d.limits === 'object') ? d.limits : {};
        const used = Number(lim.api_requests_count);
        const max  = Number(lim.api_requests);
        const usedStr = Number.isFinite(used) ? used.toLocaleString() : '-';
        const maxStr  = Number.isFinite(max)  ? max.toLocaleString()  : '-';
        const pat = d.patron_status || '';
        detail = `API requests: ${usedStr}/${maxStr}` + (pat ? ` — Status: ${pat}` : '');
      }
    }
    host.appendChild(makeConn({name,connected,vip,detail}));
  });

  putRefreshBeforeTrakt();
}

// Abortable fetch with short timeout for snappy UI
async function fetchAndRender(e){
  e?.preventDefault?.();
  const btn=e?.currentTarget||document.getElementById('btn-status-refresh'); if(!btn) return;
  if(btn.dataset.busy==='1') return;

  btn.dataset.busy='1'; btn.classList.add('spinning'); btn.setAttribute('aria-busy','true'); btn.disabled=true;
  const minSpin=new Promise(r=>setTimeout(r,600));
  const ctl=new AbortController(); const t=setTimeout(()=>ctl.abort(),4500);

  try{
    await getConfig(true);
    const r=await fetch('/api/status?fresh=1',{cache:'no-store',signal:ctl.signal});
    const d=r.ok?await r.json():null;
    render(d?.providers?d:{providers:{}});
  }catch(err){
    console.error('Status refresh failed:',err);
    render({providers:{}});
  }finally{
    clearTimeout(t);
    await minSpin;
    btn.classList.remove('spinning'); btn.removeAttribute('aria-busy'); btn.disabled=false; delete btn.dataset.busy;
    placeRefreshTopRight?.();
  }
}
window.manualRefreshStatus=fetchAndRender;

async function init(){
  if(typeof putRefreshBeforeTrakt==='function') putRefreshBeforeTrakt();
  if(typeof getConfig==='function') await getConfig();
  fetchAndRender();
}
document.readyState==='loading'
  ? document.addEventListener('DOMContentLoaded',init,{once:true})
  : init();
})();
</script>

<script>
// Sticky Save FAB: show when Settings page is visible
(() => {
  const fab   = document.getElementById('save-fab');
  const frost = document.getElementById('save-frost');
  const page  = document.getElementById('page-settings');
  const tab   = document.getElementById('tab-settings');

  function isSettingsVisible(){
    if (!page) return false;
    const cs = getComputedStyle(page);
    return !page.classList.contains('hidden') && cs.display !== 'none' && cs.visibility !== 'hidden';
  }

  function update(){
    const show = isSettingsVisible();
    if (fab)   fab.classList.toggle('hidden', !show);
    if (frost) frost.classList.toggle('hidden', !show);
  }

  function bindObservers(){
    if (page){
      const mo = new MutationObserver(update);
      mo.observe(page, { attributes: true, attributeFilter: ['class','style'] });
    }
    if (tab){
      const mo2 = new MutationObserver(update);
      mo2.observe(tab, { attributes: true, attributeFilter: ['class'] });
    }
  }

  document.addEventListener('DOMContentLoaded', () => { bindObservers(); update(); }, { once:true });
  document.addEventListener('tab-changed', update);
  window.addEventListener('hashchange', update);
  document.querySelector('.tabs')?.addEventListener('click', update, true);
})();
</script>

<script>
// Save UX wrapper: resilient, updates status, restores button label
(() => {
  const install = () => {
    const orig = window.saveSettings;
    if (typeof orig !== 'function' || orig._wrapped) return;

    async function wrapped(btnOrEvent){
      const btn = btnOrEvent instanceof HTMLElement ? btnOrEvent : document.getElementById('save-fab-btn');
      if (btn && !btn.dataset.defaultHtml) btn.dataset.defaultHtml = btn.innerHTML;
      if (btn) btn.disabled = true;

      try {
        const ret = orig.apply(this, arguments);
        await (ret && typeof ret.then === 'function' ? ret : Promise.resolve());
        window.invalidateConfigCache?.();
        window.manualRefreshStatus?.();

        if (btn){
          btn.innerHTML = 'Settings saved ✓';
          setTimeout(() => {
            btn.innerHTML = btn.dataset.defaultHtml || '<span class="btn-ic">✔</span> <span class="btn-label">Save</span>';
            btn.disabled = false;
          }, 1600);
        }
        return ret;
      } catch (e) {
        if (btn){
          btn.innerHTML = 'Save failed';
          setTimeout(() => {
            btn.innerHTML = btn.dataset.defaultHtml || '<span class="btn-ic">✔</span> <span class="btn-label">Save</span>';
            btn.disabled = false;
          }, 2000);
        }
        throw e;
      }
    }

    wrapped._wrapped = true;
    window.saveSettings = wrapped;
  };

  if (document.readyState === 'complete') {
    install();
  } else {
    window.addEventListener('load', install, { once:true });
  }
})();
</script>

</body></html>
"""
