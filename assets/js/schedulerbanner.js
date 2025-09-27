// /assets/js/schedulerbanner.js
//* Refactoring project: schedulerbanner.js (v0.1) */
//*------------------------------------------------*/

(()=>{ if(window.__SCHED_BANNER_INIT__) return; window.__SCHED_BANNER_INIT__=1; const $=(s,r=document)=>r.querySelector(s);

/* CSS */
(()=>{ if($('#sched-banner-css')) return; const st=document.createElement('style'); st.id='sched-banner-css'; st.textContent=`
#sched-inline-log{position:absolute;right:12px;bottom:4px;z-index:3;pointer-events:none;display:flex;gap:10px;align-items:center}

/* pill */
#sched-inline-log .sched{position:relative;display:inline-flex;align-items:center;gap:8px;white-space:nowrap;max-width:92vw;
  height:24px;padding:0 10px;border-radius:10px;font-size:11px;line-height:1;
  background:linear-gradient(180deg,rgba(16,18,26,.78),rgba(16,18,26,.92));backdrop-filter:blur(5px) saturate(110%);
  border:1px solid rgba(140,160,255,.15);box-shadow:0 2px 8px rgba(0,0,0,.22),0 0 10px rgba(110,140,255,.06);
  overflow:hidden;contain:paint}
#sched-inline-log .sched.ok{border-color:rgba(34,197,94,.5);box-shadow:0 2px 10px rgba(34,197,94,.15),0 0 10px rgba(34,197,94,.08)}
#sched-inline-log .sched.bad{border-color:rgba(239,68,68,.5);box-shadow:0 2px 10px rgba(239,68,68,.12),0 0 10px rgba(239,68,68,.06)}

/* sheen (clipped) */
#sched-inline-log .sched.ok::after{content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;
  background:linear-gradient(90deg,rgba(255,255,255,.08),rgba(255,255,255,0),rgba(255,255,255,.08));
  transform:translateX(-110%);animation:sheen 6s ease-in-out infinite}
@keyframes sheen{0%{transform:translateX(-110%)}60%{transform:translateX(110%)}100%{transform:translateX(110%)}}

/* status dot */
#sched-inline-log .ic{position:relative;display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto}
#sched-inline-log .ic.dot{width:10px;height:10px;border-radius:50%;background:#ef4444;box-shadow:0 0 0 2px rgba(255,255,255,.06) inset,0 0 6px rgba(239,68,68,.28)}
#sched-inline-log .sched.ok .ic.dot{background:#22c55e;box-shadow:0 0 0 2px rgba(255,255,255,.06) inset,0 0 6px rgba(34,197,94,.28)}
#sched-inline-log .sched.live .ic.dot::after{content:"";position:absolute;inset:-2px;border-radius:50%;border:1.5px solid rgba(34,197,94,.45);opacity:.7;animation:ringPulse 1.6s ease-out infinite}
@keyframes ringPulse{0%{transform:scale(.7);opacity:.7}80%{transform:scale(1.2);opacity:0}100%{transform:scale(1.2);opacity:0}}

/* label (scoped) */
#sched-inline-log .sub{display:flex;align-items:center;line-height:1;font-weight:800;letter-spacing:.1px;opacity:.95;transform:translateY(5px)}
`; document.head.appendChild(st); })();

/* Host */
function findBox(){const picks=['#ops-out','#ops_log','#ops-card','#sync-output','.sync-output','#ops'];for(const s of picks){const n=$(s);if(n)return n}
const h=[...document.querySelectorAll('h2,h3,h4,div.head,.head')].find(x=>(x.textContent||'').trim().toUpperCase()==='SYNC OUTPUT');return h?h.parentElement?.querySelector('pre,textarea,.box,.card,div'):null}
function ensureBanner(){const host=findBox(); if(!host) return null; const cs=getComputedStyle(host); if(cs.position==='static') host.style.position='relative';
let wrap=$('#sched-inline-log',host); if(!wrap){ wrap=document.createElement('div'); wrap.id='sched-inline-log';
wrap.innerHTML=`<div class="sched" id="chip-sched" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="sched-sub">Scheduler: —</span></div>
<div class="sched" id="chip-watch" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="watch-sub">Watcher: —</span></div>`; host.appendChild(wrap) } return wrap}

/* Helpers */
const tClock=s=>{if(!s)return'—';const ms=s<1e10?s*1e3:s,d=new Date(ms);return isNaN(+d)?'—':d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})};

/* State */
let S={enabled:false,running:false,next:0,busy:false}, W={enabled:false,alive:false,busy:false};
let _pend=false; function rfr(){ if(_pend) return; _pend=true; setTimeout(()=>{_pend=false;fetchSched();fetchWatch();},120) }

/* Render */
function renderSched(){
  const host=ensureBanner(); if(!host) return;
  const sub=$('#sched-sub',host), chip=$('#chip-sched',host);
  if(!S.enabled){ chip.style.display='none'; return; }
  chip.style.display='inline-flex';
  chip.classList.toggle('ok',true);          // green when visible
  chip.classList.toggle('bad',false);
  chip.classList.toggle('live',!!S.running);
  sub.textContent=`Scheduler: ${S.running?'running':'scheduled'}${S.next?` (next ${tClock(S.next)})`:''}`;
}

function renderWatch(){
  const host=ensureBanner(); if(!host) return;
  const sub=$('#watch-sub',host), chip=$('#chip-watch',host);
  if(!W.enabled){ chip.style.display='none'; return; }   // hide when disabled
  chip.style.display='inline-flex';
  chip.classList.toggle('ok',!!W.alive);                 // green only when running
  chip.classList.toggle('bad',!W.alive);                 // red when enabled but not running
  chip.classList.toggle('live',!!W.alive);
  sub.textContent=`Watcher: ${W.alive?'running':'not running'}`;
}

/* Fetch */
async function fetchSched(){ if(S.busy) return; S.busy=true;
try{const r=await fetch('/api/scheduling/status?t='+Date.now(),{cache:'no-store'}); if(!r.ok) throw 0; const j=await r.json();
S.enabled=!!(j?.config?.enabled); S.running=!!j?.running; S.next=+(j?.next_run_at||0)||0 }catch{ S.enabled=false; S.running=false; S.next=0 }
S.busy=false; renderSched() }

async function fetchWatch(){ if(W.busy) return; W.busy=true;
try{ const c=await fetch('/api/config?t='+Date.now(),{cache:'no-store'}).then(r=>r.ok?r.json():{}).catch(()=>({}));
  const mode=String(c?.scrobble?.mode||'').toLowerCase();
  W.enabled=!!c?.scrobble?.enabled && mode==='watch';
  if(W.enabled){ const s=await fetch('/debug/watch/status?t='+Date.now(),{cache:'no-store'}).then(r=>r.ok?r.json():{}).catch(()=>({})); W.alive=!!s?.alive; }
  else { W.alive=false }
}catch{ W.enabled=false; W.alive=false }
W.busy=false; renderWatch() }

/* API */
window.refreshSchedulingBanner=rfr;

/* Boot */
document.addEventListener('DOMContentLoaded',()=>{const wait=setInterval(()=>{if(findBox()){clearInterval(wait);fetchSched();fetchWatch();
clearInterval(window._schedPoll);window._schedPoll=setInterval(fetchSched,3e4);
clearInterval(window._schedTick);window._schedTick=setInterval(()=>{renderSched();renderWatch()},1e3);
clearInterval(window._watchPoll);window._watchPoll=setInterval(fetchWatch,1e4);
try{window.dispatchEvent(new Event('sched-banner-ready'))}catch{}}},300);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)rfr()},{passive:true});
document.addEventListener('config-saved',e=>{const sec=e?.detail?.section;if(!sec||sec==='scrobble'||sec==='scheduling')rfr()});
document.addEventListener('scheduling-status-refresh',rfr);
document.addEventListener('watcher-status-refresh',rfr);
document.addEventListener('tab-changed',e=>{const id=e?.detail?.id;if(id==='main'||id==='settings')rfr()});
window.addEventListener('focus',rfr);
});
})();
// EOF