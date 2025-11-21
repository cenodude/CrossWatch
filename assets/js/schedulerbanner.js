(()=>{ if(window.__SCHED_BANNER_INIT__) return; window.__SCHED_BANNER_INIT__=1; const $=(s,r=document)=>r.querySelector(s);

/* CSS */
(()=>{ if($('#sched-banner-css')) return; const st=document.createElement('style'); st.id='sched-banner-css'; st.textContent=`
#sched-inline-log{position:absolute;right:12px;bottom:4px;z-index:3;pointer-events:none;display:flex;gap:10px;align-items:center}

/* pill */
#sched-inline-log .sched{position:relative;display:inline-flex;align-items:center;gap:8px;white-space:nowrap;max-width:92vw;
  height:24px;padding:0 10px;border-radius:10px;font-size:11px;line-height:1;
  background:linear-gradient(180deg,rgba(16,18,26,.78),rgba(16,18,26,.92));backdrop-filter:blur(5px) saturate(110%);
  border:1px solid rgba(140,160,255,.15);box-shadow:0 2px 8px rgba(0,0,0,.22),0 0 10px rgba(110,140,255,.06);
  overflow:visible;pointer-events:auto}
#sched-inline-log .sched.ok{border-color:rgba(34,197,94,.5);box-shadow:0 2px 10px rgba(34,197,94,.15),0 0 10px rgba(34,197,94,.08)}
#sched-inline-log .sched.bad{border-color:rgba(239,68,68,.5);box-shadow:0 2px 10px rgba(239,68,68,.12),0 0 10px rgba(239,68,68,.06)}

/* status dot */
#sched-inline-log .ic{position:relative;display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto}
#sched-inline-log .ic.dot{width:10px;height:10px;border-radius:50%;background:#ef4444;box-shadow:0 0 0 2px rgba(255,255,255,.06) inset,0 0 6px rgba(239,68,68,.28)}
#sched-inline-log .sched.ok .ic.dot{background:#22c55e;box-shadow:0 0 0 2px rgba(255,255,255,.06) inset,0 0 6px rgba(34,197,94,.28)}
#sched-inline-log .sched.live .ic.dot::after{content:"";position:absolute;inset:-2px;border-radius:50%;border:1.5px solid rgba(34,197,94,.45);opacity:.7;animation:ringPulse 1.6s ease-out infinite}
@keyframes ringPulse{0%{transform:scale(.7);opacity:.7}80%{transform:scale(1.2);opacity:0}100%{transform:scale(1.2);opacity:0}}

/* label */
#sched-inline-log .sub{display:flex;align-items:center;line-height:1;font-weight:800;letter-spacing:.1px;opacity:.95;transform:translateY(5px)}

#sched-inline-log .sched-tip{position:absolute;right:0;bottom:110%;min-width:280px;max-width:360px;padding:12px 16px;border-radius:16px;
  background:linear-gradient(135deg,rgba(124,92,255,.32),rgba(45,161,255,.16)),linear-gradient(180deg,rgba(255,255,255,.04),transparent),#0b0b16;
  box-shadow:0 18px 40px rgba(0,0,0,.85),0 0 32px rgba(124,92,255,.55);
  border:1px solid rgba(255,255,255,.16);opacity:0;visibility:hidden;transform:translateY(6px) scale(.97);
  transition:opacity .18s ease-out,transform .18s ease-out,visibility .18s ease-out;pointer-events:none;z-index:5}
#sched-inline-log .sched:hover .sched-tip{opacity:1;visibility:visible;transform:translateY(0) scale(1)}
#sched-inline-log .tip-title{font-size:12px;font-weight:800;color:#e5e7ff;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#sched-inline-log .tip-meta{font-size:10px;color:#9ca3af;margin-bottom:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#sched-inline-log .tip-bar{position:relative;width:100%;height:4px;border-radius:999px;background:linear-gradient(180deg,rgba(5,9,20,1),rgba(5,9,20,.94));
  overflow:hidden;box-shadow:0 0 0 1px rgba(148,163,255,.65) inset,0 0 14px rgba(79,70,229,.7)}
#sched-inline-log .tip-bar-inner{position:absolute;inset:0;border-radius:999px;background:linear-gradient(90deg,#2de2ff,#7c5cff,#ff7ae0);
  width:0%;transform-origin:left center;animation:tipFlow 3s linear infinite}
@keyframes tipFlow{0%{filter:brightness(1)}50%{filter:brightness(1.27)}100%{filter:brightness(1)}}
`; document.head.appendChild(st); })();

/* Host */
function findBox(){const picks=['#ops-out','#ops_log','#ops-card','#sync-output','.sync-output','#ops'];for(const s of picks){const n=$(s);if(n)return n}
const h=[...document.querySelectorAll('h2,h3,h4,div.head,.head')].find(x=>(x.textContent||'').trim().toUpperCase()==='SYNC OUTPUT');return h?h.parentElement?.querySelector('pre,textarea,.box,.card,div'):null}
function ensureBanner(){const host=findBox(); if(!host) return null; const cs=getComputedStyle(host); if(cs.position==='static') host.style.position='relative';
let wrap=$('#sched-inline-log',host); if(!wrap){ wrap=document.createElement('div'); wrap.id='sched-inline-log';
wrap.innerHTML=`<div class="sched" id="chip-sched" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="sched-sub">Scheduler: —</span></div>
<div class="sched" id="chip-watch" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="watch-sub">Watcher: —</span><div class="sched-tip" id="watch-tip"><div class="tip-title" id="watch-tip-title">No active playback</div><div class="tip-meta" id="watch-tip-meta">—</div><div class="tip-bar"><div class="tip-bar-inner" id="watch-tip-bar"></div></div></div></div>
<div class="sched" id="chip-hook" aria-live="polite"><span class="ic dot" aria-hidden="true"></span><span class="sub" id="hook-sub">Webhook: —</span><div class="sched-tip" id="hook-tip"><div class="tip-title" id="hook-tip-title">No active playback</div><div class="tip-meta" id="hook-tip-meta">—</div><div class="tip-bar"><div class="tip-bar-inner" id="hook-tip-bar"></div></div></div></div>`; host.appendChild(wrap) } return wrap}

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
  const tip=$('#watch-tip',host), tTitle=$('#watch-tip-title',host), tMeta=$('#watch-tip-meta',host), tBar=$('#watch-tip-bar',host);
  if(tTitle&&tMeta&&tBar){
    if(hasPlay){
      tTitle.textContent=W.title||'';
      const parts=[];
      if(W.media_type){
        const mt=String(W.media_type).toLowerCase()==='episode'?'Show / Episode':'Movie';
        if(W.media_type.toLowerCase()==='episode'&&W.season!=null&&W.episode!=null){
          parts.push(`${mt} S${String(W.season).padStart(2,'0')}E${String(W.episode).padStart(2,'0')}`);
        }else{
          parts.push(mt);
        }
      }
      if(W.year) parts.push(String(W.year));
      const pct=Math.max(0,Math.min(100,Number(W.progress)||0));
      parts.push(pct+'%');
      tMeta.textContent=parts.join(' • ')||'—';
      tBar.style.width=pct+'%';
    }else{
      tTitle.textContent='No active playback';
      tMeta.textContent='—';
      tBar.style.width='0%';
    }
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
  const tip=$('#hook-tip',host), tTitle=$('#hook-tip-title',host), tMeta=$('#hook-tip-meta',host), tBar=$('#hook-tip-bar',host);
  if(tTitle&&tMeta&&tBar){
    if(hasPlay){
      tTitle.textContent=H.title||'';
      const parts=[];
      if(H.media_type){
        const mt=String(H.media_type).toLowerCase()==='episode'?'Show / Episode':'Movie';
        if(H.media_type.toLowerCase()==='episode'&&H.season!=null&&H.episode!=null){
          parts.push(`${mt} S${String(H.season).padStart(2,'0')}E${String(H.episode).padStart(2,'0')}`);
        }else{
          parts.push(mt);
        }
      }
      if(H.year) parts.push(String(H.year));
      const pct=Math.max(0,Math.min(100,Number(H.progress)||0));
      parts.push(pct+'%');
      tMeta.textContent=parts.join(' • ')||'—';
      tBar.style.width=pct+'%';
    }else{
      tTitle.textContent='No active playback';
      tMeta.textContent='—';
      tBar.style.width='0%';
    }
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
