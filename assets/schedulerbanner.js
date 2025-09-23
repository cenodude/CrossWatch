// /assets/schedulerbanner.js
(()=>{if(window.__SCHED_BANNER_INIT__)return;window.__SCHED_BANNER_INIT__=1;const $=(s,r=document)=>r.querySelector(s);

/* CSS */
(()=>{ if($('#sched-banner-css')) return; const st=document.createElement('style'); st.id='sched-banner-css'; st.textContent=`
#sched-inline-log{position:absolute;right:12px;bottom:4px;z-index:3;pointer-events:none}
.sched{display:inline-flex;align-items:center;gap:4px;white-space:nowrap;max-width:92vw;
  padding:2px 8px;border-radius:8px;line-height:1;font-size:12px;
  background:linear-gradient(180deg,rgba(16,18,26,.78),rgba(16,18,26,.92));
  backdrop-filter:blur(5px) saturate(110%);
  border:1px solid rgba(140,160,255,.15);
  box-shadow:0 2px 8px rgba(0,0,0,.22),0 0 10px rgba(110,140,255,.06)}
.sched .ic{
  display:inline-flex;align-items:center;justify-content:center;
  width:1em;height:1em;font-size:1em;line-height:1;
  transform-origin:50% 50%;
  font-variation-settings:'FILL' 0,'wght' 600,'GRAD' 0,'opsz' 20;
  animation:spinY 45s linear infinite;
}
.sched .sub{font-weight:800;letter-spacing:.1px;opacity:.95;line-height:1;display:inline-flex;align-items:center}
@keyframes spinY{
  from{transform:translateY(-4px) rotate(0deg)}
  to  {transform:translateY(-4px) rotate(360deg)}
}
`; document.head.appendChild(st); })();

/* Host */
function findBox(){const picks=['#ops-out','#ops_log','#ops-card','#sync-output','.sync-output','#ops'];for(const s of picks){const n=$(s);if(n)return n}
const h=[...document.querySelectorAll('h2,h3,h4,div.head,.head')].find(x=>(x.textContent||'').trim().toUpperCase()==='SYNC OUTPUT');return h?h.parentElement?.querySelector('pre,textarea,.box,.card,div'):null}
function ensureBanner(){const host=findBox();if(!host)return null;const cs=getComputedStyle(host);if(cs.position==='static')host.style.position='relative';
let f=$('#sched-inline-log',host);if(!f){f=document.createElement('div');f.id='sched-inline-log';
f.innerHTML=`<div class="sched"><span class="ic material-symbols-rounded" aria-hidden="true">schedule</span><span class="sub" id="sched-sub">Schedular: —</span></div>`;host.appendChild(f)}return f}

/* Format */
const tClock=s=>{if(!s)return'—';const ms=s<1e10?s*1e3:s,d=new Date(ms);return isNaN(+d)?'—':d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})};

/* State */
let enabled=false,running=false,nextRunAt=0,_busy=false,_pend=false;
function rfr(){if(_pend)return;_pend=true;setTimeout(()=>{_pend=false;fetchStatus()},120)}

/* Render */
function render(){const host=ensureBanner();if(!host)return;const sub=$('#sched-sub',host);
if(!enabled){host.style.display='none';return}host.style.display='block';
sub.textContent=`Schedular: ${running?'running':'scheduled'}${nextRunAt?` (next at ${tClock(nextRunAt)})`:''}`}

/* Fetch */
async function fetchStatus(){if(_busy)return;_busy=true;try{const r=await fetch('/api/scheduling/status?t='+Date.now(),{cache:'no-store'});if(!r.ok)throw 0;const j=await r.json();
enabled=!!(j?.config?.enabled);running=!!j?.running;nextRunAt=+(j?.next_run_at||0)||0}catch{enabled=false;running=false;nextRunAt=0}_busy=false;render()}

/* API */
window.refreshSchedulingBanner=rfr;

/* Boot */
document.addEventListener('DOMContentLoaded',()=>{const wait=setInterval(()=>{if(findBox()){clearInterval(wait);fetchStatus();
clearInterval(window._schedPoll);window._schedPoll=setInterval(fetchStatus,3e4);
clearInterval(window._schedTick);window._schedTick=setInterval(render,1e3);
try{window.dispatchEvent(new Event('sched-banner-ready'))}catch{}}},300);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)rfr()},{passive:true});
document.addEventListener('config-saved',e=>{const sec=e?.detail?.section;if(!sec||sec==='scheduling')rfr()});
document.addEventListener('scheduling-status-refresh',rfr);
document.addEventListener('tab-changed',e=>{const id=e?.detail?.id;if(id==='main'||id==='settings')rfr()});
window.addEventListener('focus',rfr);
});
})();
