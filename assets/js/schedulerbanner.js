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

/* State */
let S={enabled:false,running:false,next:0,busy:false},
    W={enabled:false,alive:false,busy:false,title:null,media_type:null,year:null,season:null,episode:null,progress:0,state:null},
    H={enabled:false,busy:false,title:null,media_type:null,year:null,season:null,episode:null,progress:0,state:null};
let _pend=false; function rfr(){ if(_pend) return; _pend=true; setTimeout(()=>{_pend=false;fetchSched();fetchScrobble();},120) }

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
  sub.textContent=hasPlay?`Watcher: ${W.title}`:`Watcher: ${live?'running':'not running'}`;

  const pct=Math.max(0,Math.min(100,Number(W.progress)||0));

  // Notify UI listeners (playing card)
  if(hasPlay){
    try{
      window.dispatchEvent(new CustomEvent('currently-watching-updated',{
        detail:{
          source:'watcher',
          title:W.title||'',
          media_type:W.media_type||null,
          year:W.year||null,
          season:W.season??null,
          episode:W.episode??null,
          progress:pct,
          state:W.state||'playing'
        }
      }));
    }catch(e){}
  }else{
    try{
      window.dispatchEvent(new CustomEvent('currently-watching-updated',{
        detail:{
          source:'watcher',
          state:'stopped'
        }
      }));
    }catch(e){}
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
  sub.textContent=hasPlay?`Webhook: ${H.title}`:'Webhook: enabled';

  const pct=Math.max(0,Math.min(100,Number(H.progress)||0));

  // Notify UI listeners (playing card)
  if(hasPlay){
    try{
      window.dispatchEvent(new CustomEvent('currently-watching-updated',{
        detail:{
          source:'webhook',
          title:H.title||'',
          media_type:H.media_type||null,
          year:H.year||null,
          season:H.season??null,
          episode:H.episode??null,
          progress:pct,
          state:H.state||'playing'
        }
      }));
    }catch(e){}
  }else{
    try{
      window.dispatchEvent(new CustomEvent('currently-watching-updated',{
        detail:{
          source:'webhook',
          state:'stopped'
        }
      }));
    }catch(e){}
  }
}

/* Fetch */
async function fetchSched(){ if(S.busy) return; S.busy=true;
try{const r=await fetch('/api/scheduling/status?t='+Date.now(),{cache:'no-store'}); if(!r.ok) throw 0; const j=await r.json();
S.enabled=!!(j?.config?.enabled); S.running=!!j?.running; S.next=+(j?.next_run_at||0)||0 }catch{ S.enabled=false; S.running=false; S.next=0 }
S.busy=false; renderSched() }

/* Read scrobble once and derive both Watcher and Webhook states */
async function fetchScrobble(){ if(W.busy||H.busy) return; W.busy=H.busy=true;
try{
  const c=await fetch('/api/config?t='+Date.now(),{cache:'no-store'}).then(r=>r.ok?r.json():{}).catch(()=>({}));
  const mode=String(c?.scrobble?.mode||'').toLowerCase();
  const enabled=!!c?.scrobble?.enabled;

  W.enabled=enabled && mode==='watch';
  if(W.enabled){
    const s=await fetch('/api/watch/status?t='+Date.now(),{cache:'no-store'}).then(r=>r.ok?r.json():{}).catch(()=>({}));
    W.alive=!!s?.alive;
  } else { W.alive=false }

  H.enabled=enabled && mode==='webhook';

  W.title=null; W.media_type=null; W.year=null; W.season=null; W.episode=null; W.progress=0; W.state=null;
  H.title=null; H.media_type=null; H.year=null; H.season=null; H.episode=null; H.progress=0; H.state=null;

  const cw = await fetch('/api/watch/currently_watching?t='+Date.now(), {cache:'no-store'})
    .then(r => r.ok ? r.json() : null)
    .catch(() => null);
  const cur = cw && (cw.currently_watching || cw);

  if(cur && cur.state && cur.state!=='stopped'){
    const src=String(cur.source||'').toLowerCase();
    const tgt=(src==='plex'||src==='emby')?W:H;
    tgt.title=cur.title||'';
    tgt.media_type=cur.media_type||null;
    tgt.year=cur.year||null;
    tgt.season=cur.season??null;
    tgt.episode=cur.episode??null;
    tgt.progress=+cur.progress||0;
    tgt.state=cur.state||null;
  }
}catch{
  W.enabled=false; W.alive=false; H.enabled=false;
  W.title=null; W.media_type=null; W.year=null; W.season=null; W.episode=null; W.progress=0; W.state=null;
  H.title=null; H.media_type=null; H.year=null; H.season=null; H.episode=null; H.progress=0; H.state=null
}
W.busy=H.busy=false; renderWatch(); renderHook() }

/* API */
window.refreshSchedulingBanner=rfr;

/* Boot */
document.addEventListener('DOMContentLoaded',()=>{const wait=setInterval(()=>{if(findBox()){clearInterval(wait);fetchSched();fetchScrobble();
clearInterval(window._schedPoll);window._schedPoll=setInterval(fetchSched,3e4);
clearInterval(window._schedTick);window._schedTick=setInterval(()=>{renderSched();renderWatch();renderHook()},1e3);
clearInterval(window._scrobPoll);window._scrobPoll=setInterval(fetchScrobble,1e4);
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
