/* assets/js/schedulerbanner.js */
/* CrossWatch - Scheduler Banner and playing card run */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(()=>{ if(window.__SCHED_BANNER_INIT__) return; window.__SCHED_BANNER_INIT__=1; const $=(s,r=document)=>r.querySelector(s);

/* CSS */
(()=>{ if($('#sched-banner-css')) return; const st=document.createElement('style'); st.id='sched-banner-css'; st.textContent=`
#sched-inline-log{
  position:absolute;
  right:16px;
  bottom:10px;
  z-index:4;
  pointer-events:none;
  display:flex;
  gap:8px;
  align-items:center;
  flex-wrap:wrap;
}

/* pill */
#sched-inline-log .sched{
  position:relative;
  display:inline-flex;
  align-items:center;
  gap:8px;
  white-space:nowrap;
  max-width:92vw;
  padding:3px 12px;
  border-radius:999px;
  font-size:11px;
  line-height:1;
  background:linear-gradient(180deg,rgba(16,18,26,.78),rgba(16,18,26,.92));
  backdrop-filter:blur(5px) saturate(110%);
  border:1px solid rgba(140,160,255,.15);
  box-shadow:0 2px 8px rgba(0,0,0,.22),0 0 10px rgba(110,140,255,.06);
  overflow:visible;
  pointer-events:auto;
}

#sched-inline-log .ic.dot{
  width:8px;
  height:8px;
  border-radius:50%;
  background:#ef4444;
  box-shadow:0 0 0 1px rgba(0,0,0,.6),0 0 6px rgba(239,68,68,.28);
}
#sched-inline-log .sched.ok .ic.dot{
  background:#22c55e;
  box-shadow:0 0 0 1px rgba(0,0,0,.6),0 0 6px rgba(34,197,94,.28);
}

#sched-inline-log .sub{
  display:flex;
  align-items:center;
  line-height:1;
  font-weight:800;
  letter-spacing:.1px;
  opacity:.95;
  transform:translateY(4px);
}

/* status dot */
#sched-inline-log .ic{
  position:relative;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  flex:0 0 auto;
}
#sched-inline-log .ic.dot{
  width:9px;
  height:9px;
  border-radius:50%;
  background:#ef4444;
  box-shadow:0 0 0 1px rgba(0,0,0,.6),0 0 8px rgba(239,68,68,.4);
}
#sched-inline-log .sched.ok .ic.dot{
  background:#22c55e;
  box-shadow:0 0 0 1px rgba(0,0,0,.6),0 0 8px rgba(34,197,94,.4);
}
#sched-inline-log .sched.live .ic.dot::after{
  content:"";
  position:absolute;
  inset:-3px;
  border-radius:50%;
  border:1px solid rgba(34,197,94,.6);
  opacity:.8;
  animation:ringPulse 1.6s ease-out infinite;
}
@keyframes ringPulse{
  0%{transform:scale(.7);opacity:.8}
  80%{transform:scale(1.25);opacity:0}
  100%{transform:scale(1.25);opacity:0}
}

/* label */
#sched-inline-log .sub{
  display:flex;
  align-items:center;
  line-height:1;
  font-weight:700;
  letter-spacing:.06em;
  opacity:.9;
  text-transform:uppercase;
}
`; document.head.appendChild(st); })();

/* Host */
function findBox(){const picks=['#ops-out','#ops_log','#ops-card','#sync-output','.sync-output','#ops'];for(const s of picks){const n=$(s);if(n)return n}
const h=[...document.querySelectorAll('h2,h3,h4,div.head,.head')].find(x=>(x.textContent||'').trim().toUpperCase()==='SYNC OUTPUT');return h?h.parentElement?.querySelector('pre,textarea,.box,.card,div'):null}
function ensureBanner(){const host=findBox(); if(!host) return null; const cs=getComputedStyle(host); if(cs.position==='static') host.style.position='relative';
let wrap=$('#sched-inline-log',host); if(!wrap){ wrap=document.createElement('div'); wrap.id='sched-inline-log';
wrap.innerHTML=`<div class="sched" id="chip-sched" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="sched-sub">Scheduler: —</span></div>
<div class="sched" id="chip-watch" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="watch-sub">Watcher: —</span></div>
<div class="sched" id="chip-hook" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="hook-sub">Webhook: —</span></div>`; host.appendChild(wrap) } return wrap}

/* Helpers */
const tClock=s=>{if(!s)return'—';const ms=s<1e10?s*1e3:s,d=new Date(ms);return isNaN(+d)?'—':d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})};

/* Polling policy */
const SCROBBLE_POLL_WATCH_MS=15000;
const SCROBBLE_POLL_WEBHOOK_MS=60000;

const MIN_SCHED_FETCH_MS=2000;
const MIN_SCROBBLE_FETCH_MS=2000;

/* State */
let S={enabled:false,running:false,next:0,busy:false},
    W={enabled:false,alive:false,busy:false,title:null,media_type:null,year:null,season:null,episode:null,progress:0,state:null,streams_count:0},
    H={enabled:false,busy:false,title:null,media_type:null,year:null,season:null,episode:null,progress:0,state:null,streams_count:0};

let _schedLastAt=0, _schedQueued=false, _schedQueuedForce=false;
let _scrobLastAt=0, _scrobQueued=false, _scrobQueuedForceCfg=false, _scrobQueuedForce=false;
let _cwAbort=null;

/* De-dupe event spam to UI listeners */
const _lastCW = { watcher: '', webhook: '' };
function emitCurrentlyWatching(source, detail){
  const payload = { source, ...detail };
  const key = JSON.stringify(payload);
  if(_lastCW[source] === key) return;
  _lastCW[source] = key;
  try{
    window.dispatchEvent(new CustomEvent('currently-watching-updated', { detail: payload }));
  }catch(e){}
}

let _cfgMemo=null, _cfgAt=0, _cfgBusy=null;
const CFG_TTL_MS=60000;
function invalidateCfg(){ _cfgMemo=null; _cfgAt=0; _cfgBusy=null; }
async function readCfg(force=false){
  const now=Date.now();
  if(!force && _cfgMemo && (now-_cfgAt)<CFG_TTL_MS) return _cfgMemo;
  if(_cfgBusy) return _cfgBusy;
  _cfgBusy = fetch('/api/config?t='+now,{cache:'no-store'})
    .then(r=>r.ok?r.json():null)
    .catch(()=>null)
    .finally(()=>{ _cfgBusy=null; });
  const v = await _cfgBusy;
  if(v && typeof v === 'object'){
    _cfgMemo = v;
    _cfgAt = Date.now();
    return _cfgMemo;
  }
  if(!_cfgMemo) _cfgMemo = {};
  _cfgAt = Date.now();
  return _cfgMemo;
}

let _pend=false, _forceCfg=false, _forceNow=false;
function rfr(forceCfg=false){
  if(forceCfg){ _forceCfg = true; _forceNow = true; }
  if(_pend) return;
  _pend=true;
  setTimeout(()=>{
    _pend=false;
    const cfg=_forceCfg;
    const force=_forceNow;
    _forceCfg=false;
    _forceNow=false;
    fetchSched(force);
    fetchScrobble(cfg, force);
    ensureScrobbleLoop(cfg);
  },120);
}

/* Render */
function renderSched(){
  const host=ensureBanner(); if(!host) return;
  const sub=$('#sched-sub',host), chip=$('#chip-sched',host);
  if(!S.enabled){ chip.style.display='none'; return; }
  chip.style.display='inline-flex';
  chip.classList.toggle('ok',true);
  chip.classList.toggle('bad',false);
  chip.classList.toggle('live',!!S.running);
  sub.textContent=`Scheduler: ${S.running?'running':'scheduled'}${S.next?` (next ${tClock(S.next)})`:''}`;
}

function renderWatch(){
  const host=ensureBanner(); if(!host) return;
  const sub=$('#watch-sub',host), chip=$('#chip-watch',host);
  if(!W.enabled){ chip.style.display='none'; return; }
  chip.style.display='inline-flex';
  const live=!!W.alive;
  const hasPlay=!!(live && W.state === 'playing' && W.title);
  chip.classList.toggle('ok',live);
  chip.classList.toggle('bad',!live);
  chip.classList.toggle('live',hasPlay);
  const sc = Number(W.streams_count)||0;
  const scLabel = (hasPlay && sc > 1) ? ` (${sc} streams)` : '';
  sub.textContent=hasPlay?`Watcher: ${W.title}${scLabel}`:`Watcher: ${live?'running':'not running'}`;

  const pct=Math.max(0,Math.min(100,Number(W.progress)||0));

  if(hasPlay){
    emitCurrentlyWatching('watcher',{
      title:W.title||'',
      media_type:W.media_type||null,
      year:W.year||null,
      season:W.season??null,
      episode:W.episode??null,
      progress:pct,
      state:W.state||'playing',
      _streams_count: sc
    });
  }else{
    emitCurrentlyWatching('watcher',{ state:'stopped' });
  }
}

function renderHook(){
  const host=ensureBanner(); if(!host) return;
  const sub=$('#hook-sub',host), chip=$('#chip-hook',host);
  if(!H.enabled){ chip.style.display='none'; return; }
  chip.style.display='inline-flex';
  const hasPlay=!!(H.state === 'playing' && H.title);
  chip.classList.toggle('ok',true);
  chip.classList.toggle('bad',false);
  chip.classList.toggle('live',hasPlay);
  const sc = Number(H.streams_count)||0;
  const scLabel = (hasPlay && sc > 1) ? ` (${sc} streams)` : '';
  sub.textContent=hasPlay?`Webhook: ${H.title}${scLabel}`:'Webhook: enabled';

  const pct=Math.max(0,Math.min(100,Number(H.progress)||0));

  if(hasPlay){
    emitCurrentlyWatching('webhook',{
      title:H.title||'',
      media_type:H.media_type||null,
      year:H.year||null,
      season:H.season??null,
      episode:H.episode??null,
      progress:pct,
      state:H.state||'playing',
      _streams_count: sc
    });
  }else{
    emitCurrentlyWatching('webhook',{ state:'stopped' });
  }
}

/* Fetch */
async function fetchSched(force=false){ if(S.busy){ _schedQueued=true; _schedQueuedForce=_schedQueuedForce||force; return; }
  const now=Date.now();
  if(!force && _schedLastAt && (now-_schedLastAt)<MIN_SCHED_FETCH_MS){ renderSched(); return; }
  S.busy=true;
try{
  if(document.hidden){ S.busy=false; return; }
  const r=await fetch('/api/scheduling/status?t='+Date.now(),{cache:'no-store'}); if(!r.ok) throw 0; const j=await r.json();
  S.enabled=!!(j?.config?.enabled); S.running=!!j?.running; S.next=+(j?.next_run_at||0)||0
}catch{ S.enabled=false; S.running=false; S.next=0 }
_schedLastAt=Date.now();
S.busy=false; renderSched();
if(_schedQueued){ const f=_schedQueuedForce; _schedQueued=false; _schedQueuedForce=false; setTimeout(()=>fetchSched(f),0); } }

/* Read scrobble once and derive both Watcher and Webhook states */
async function fetchScrobble(forceCfg=false, force=false){ if(W.busy||H.busy){ _scrobQueued=true; _scrobQueuedForceCfg=_scrobQueuedForceCfg||forceCfg; _scrobQueuedForce=_scrobQueuedForce||force; return; }
  const now=Date.now();
  const hard=force||forceCfg;
  if(!hard && _scrobLastAt && (now-_scrobLastAt)<MIN_SCROBBLE_FETCH_MS){ renderWatch(); renderHook(); return; }
  W.busy=H.busy=true;
try{
  if(document.hidden){ W.busy=H.busy=false; return; }

  const c=await readCfg(!!forceCfg);
  const mode=String(c?.scrobble?.mode||'').toLowerCase();
  const enabled=!!c?.scrobble?.enabled;

  W.enabled=enabled && mode==='watch';
  H.enabled=enabled && mode==='webhook';

  /* Reset details */
  W.title=null; W.media_type=null; W.year=null; W.season=null; W.episode=null; W.progress=0; W.state=null; W.streams_count=0;
  H.title=null; H.media_type=null; H.year=null; H.season=null; H.episode=null; H.progress=0; H.state=null; H.streams_count=0;

  if(W.enabled){
    const s=await fetch('/api/watch/status?t='+Date.now(),{cache:'no-store'}).then(r=>r.ok?r.json():{}).catch(()=>({}));
    W.alive=!!s?.alive;
  } else { W.alive=false }

  /* If scrobble is off, don't hit currently_watching */
  if(!(W.enabled || H.enabled)) throw 0;

  try{ _cwAbort?.abort(); }catch(e){}
  _cwAbort = new AbortController();
  const cw = await fetch('/api/watch/currently_watching?t='+Date.now(), {cache:'no-store', signal:_cwAbort.signal})
    .then(r => r.ok ? r.json() : null)
    .catch((e) => (e && e.name === 'AbortError') ? null : null);
  const cur = cw && (cw.currently_watching || cw);
  const sc = Number(cw?.streams_count) || 0;

  if(cur && cur.state && cur.state!=='stopped'){
    const src=String(cur.source||'').toLowerCase();
    const tgt =
      (src.includes('webhook') && H.enabled) ? H :
      ((src.includes('watch') || src.includes('watcher')) && W.enabled) ? W :
      (W.enabled ? W : H);

    tgt.title=cur.title||'';
    tgt.media_type=cur.media_type||null;
    tgt.year=cur.year||null;
    tgt.season=cur.season??null;
    tgt.episode=cur.episode??null;
    tgt.progress=+cur.progress||0;
    tgt.state=cur.state||null;
    tgt.streams_count = sc;
  }
}catch{
  W.enabled=false; W.alive=false; H.enabled=false;
  W.title=null; W.media_type=null; W.year=null; W.season=null; W.episode=null; W.progress=0; W.state=null;
  H.title=null; H.media_type=null; H.year=null; H.season=null; H.episode=null; H.progress=0; H.state=null
}
_scrobLastAt=Date.now();
W.busy=H.busy=false; renderWatch(); renderHook();
if(_scrobQueued){ const fc=_scrobQueuedForceCfg; const f=_scrobQueuedForce; _scrobQueued=false; _scrobQueuedForceCfg=false; _scrobQueuedForce=false; setTimeout(()=>fetchScrobble(fc, f),0); } }

/* Adaptive scrobble polling */
async function scrobblePollMs(forceCfg=false){
  const c=await readCfg(!!forceCfg);
  const enabled=!!c?.scrobble?.enabled;
  if(!enabled) return 0;
  const mode=String(c?.scrobble?.mode||'').toLowerCase();
  if(mode==='watch') return SCROBBLE_POLL_WATCH_MS;
  if(mode==='webhook') return SCROBBLE_POLL_WEBHOOK_MS;
  return 0;
}
function stopScrobbleLoop(){
  if(window._scrobPollT){ clearTimeout(window._scrobPollT); window._scrobPollT=null; }
  window._scrobPollMs = 0;
}
function scheduleScrobbleLoop(ms){
  if(!ms){ stopScrobbleLoop(); return; }
  if(window._scrobPollT && window._scrobPollMs === ms) return;
  stopScrobbleLoop();
  window._scrobPollMs = ms;
  window._scrobPollT = setTimeout(async ()=>{
    window._scrobPollT = null;
    if(document.hidden){ scheduleScrobbleLoop(window._scrobPollMs||ms); return; }
    await fetchScrobble(false, false);
    const next = await scrobblePollMs(false);
    scheduleScrobbleLoop(next);
  }, ms);
}
async function ensureScrobbleLoop(forceCfg=false){
  const ms = await scrobblePollMs(!!forceCfg);
  scheduleScrobbleLoop(ms);
}

/* API */
window.refreshSchedulingBanner=rfr;

/* Boot */
function bootOnce(){
  if(window.__SCHED_BANNER_STARTED__) return;
  window.__SCHED_BANNER_STARTED__ = true;

  fetchSched();
  fetchScrobble(true, true);
  ensureScrobbleLoop(true);

  if(!window._schedPoll) window._schedPoll = setInterval(fetchSched, 3e4);
  if(!window._schedTick) window._schedTick = setInterval(()=>{renderSched();renderWatch();renderHook()}, 1e3);

  try{window.dispatchEvent(new Event('sched-banner-ready'))}catch{}
}

document.addEventListener('DOMContentLoaded',()=>{
  if(window.__SCHED_BANNER_WAIT__) { clearInterval(window.__SCHED_BANNER_WAIT__); window.__SCHED_BANNER_WAIT__ = null; }
  window.__SCHED_BANNER_WAIT__ = setInterval(()=>{
    if(findBox()){
      clearInterval(window.__SCHED_BANNER_WAIT__);
      window.__SCHED_BANNER_WAIT__ = null;
      bootOnce();
    }
  },300);

  document.addEventListener('visibilitychange',()=>{if(!document.hidden)rfr()},{passive:true});

  document.addEventListener('config-saved',e=>{
    const sec=e?.detail?.section;
    if(!sec||sec==='scrobble'||sec==='scheduling'){
      invalidateCfg();
      rfr(true);
    }
  });
  document.addEventListener('scheduling-status-refresh',rfr);
  document.addEventListener('watcher-status-refresh',rfr);
  document.addEventListener('tab-changed',e=>{const id=e?.detail?.id;if(id==='main'||id==='settings')rfr()});
  window.addEventListener('focus',rfr);
});
})();
