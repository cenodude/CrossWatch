// Sync bar UI component
(() => {
  (document.getElementById("syncbar-css")||{}).remove?.();
  document.head.appendChild(Object.assign(document.createElement("style"), {
    id:"syncbar-css", textContent:`
#ux-progress{margin-top:12px}
.sb-rail{position:relative;height:10px;border-radius:999px;background:#1f1f26;overflow:visible}
.sb-rail.error{background:linear-gradient(90deg,#311,#401818)}
.sb-fill{position:absolute;inset:0 auto 0 0;width:0%;height:100%;border-radius:inherit;background:linear-gradient(90deg,#7c4dff,#00d4ff);box-shadow:inset 0 0 14px rgba(124,77,255,.35);transition:width .28s ease;z-index:1}
@keyframes sbShift{0%{background-position:0% 50%}100%{background-position:100% 50%}}
.sb-rail.running .sb-fill{background-size:200% 100%}
.sb-rail.starting .sb-fill{animation:sbPulse .9s ease-in-out infinite alternate}
.sb-rail.finishing .sb-fill{filter:saturate(1.2) brightness(1.05)}
@keyframes sbShimmer{to{transform:translateX(100%)}}
@keyframes sbPulse{from{opacity:.9}to{opacity:.75}}
@media (prefers-reduced-motion:reduce){
  .sb-rail.running.indet::after,.sb-fill.indet{animation:none}
  .sb-rail.starting .sb-fill{animation:none}
}
.sb-steps{display:flex;justify-content:space-between;font-size:11px;margin-top:6px;opacity:.8}
.sb-steps span{white-space:nowrap}
.sb-fly{position:absolute;top:-24px;left:0;transform:translateX(-50%);
  font-size:11px;padding:2px 8px;border-radius:10px;white-space:nowrap;
  background:rgba(0,0,0,.45);backdrop-filter:blur(4px);
  border:1px solid rgba(255,255,255,.08);box-shadow:0 2px 10px rgba(0,0,0,.25);
  opacity:.92;pointer-events:none;transition:left .25s ease, opacity .15s ease;z-index:2}
.sb-fly.hide{opacity:0}
/* Clip shimmer to the filled part only */
.sb-fill{overflow:hidden}

.sb-rail.indet .sb-fill{
  background-size:200% 100%;
  animation:sbShift 1.2s ease-in-out infinite;
}

.sb-rail.indet .sb-fill::after{
  content:"";
  position:absolute;
  inset:0;
  background:linear-gradient(120deg,
    transparent 0%,
    rgba(255,255,255,.09) 20%,
    transparent 40%);
  transform:translateX(-100%);
  animation:sbShimmer 1.4s linear infinite;
  pointer-events:none;
}

.sb-rail.apply.indet .sb-fill::after{
  animation-duration:1.0s;
}

`})); 

  const Anch=Object.freeze({start0:0,preStart:35,preEnd:57,postEnd:67,done:100});
  const clamp=(n,lo=0,hi=100)=>Math.max(lo,Math.min(hi,Math.round(n)));
  const POST_DONE_GRACE_MS = 20000;

  const PhaseAgg={snap:{done:0,total:0,started:false,finished:false},
                  apply:{done:0,total:0,started:false,finished:false}};
  const SnapAgg={
    buckets:Object.create(null),
    reset(){ this.buckets=Object.create(null); },
    update(d){
      const k=`${(d.dst||"ALL").toUpperCase()}:${(d.feature||"all").toLowerCase()}`;
      this.buckets[k]={done:+(d.done||0),total:+(d.total||0),final:!!d.final};
      let tot=0,don=0,allFinal=true;
      for(const v of Object.values(this.buckets)){
        const dn=Math.min(+v.done||0,+v.total||0);
        don+=dn; tot+=(+v.total||0);
        allFinal=allFinal && (!!v.final || dn>=(+v.total||0));
      }
      PhaseAgg.snap.total=tot;
      PhaseAgg.snap.done=don;
      PhaseAgg.snap.started=tot>0;
      PhaseAgg.snap.finished=allFinal && tot>0 && don>=tot;
    }
  };
  const ApplyAgg={
    buckets:Object.create(null),
    reset(){ this.buckets=Object.create(null); PhaseAgg.apply={done:0,total:0,started:false,finished:false}; },
    _ensure(k){ return (this.buckets[k] ||= {done:0,total:0,final:false}); },
    start({feature="__global__",total=0}){ const b=this._ensure(feature); b.total=+total||0; this._recalc(); },
    prog({feature="__global__",done,total}){ const b=this._ensure(feature); if(typeof done==="number") b.done=+done||0; if(typeof total==="number") b.total=+total||0; this._recalc(); },
    done({feature="__global__",count}){ const b=this._ensure(feature); const c=+(count||b.total||0); b.done=c; b.total=Math.max(b.total,c); b.final=true; this._recalc(); },
    _recalc(){ let tot=0,don=0,allFinal=true,any=false; for(const v of Object.values(this.buckets)){ any=true; tot+=+v.total||0; don+=Math.min(+v.done||0,+v.total||0); allFinal=allFinal && (!!v.final || ((+v.total||0)>0 && (+v.done||0)>=(+v.total||0))); } PhaseAgg.apply.total=tot; PhaseAgg.apply.done=don; PhaseAgg.apply.started=any && tot>0; PhaseAgg.apply.finished=any && allFinal && tot>0 && don>=tot; }
  };

  const phaseIdx=tl=>tl?.done?3:tl?.post?2:tl?.pre?1:tl?.start?0:-1;
  const asPctFromTimeline=(tl,allowDone=false)=>((allowDone && tl?.done)?Anch.done:tl?.post?Anch.postEnd:tl?.pre?Anch.preStart:tl?.start?Anch.start0:0);
  const pctFromPhase=()=>{ const sTot=PhaseAgg.snap.total|0,sDone=PhaseAgg.snap.done|0,aTot=PhaseAgg.apply.total|0,aDone=PhaseAgg.apply.done|0;
    const snapPct=sTot>0?(Anch.preStart+(Anch.preEnd-Anch.preStart)*Math.max(0,Math.min(1,sDone/sTot))):null;
    const appPct=(PhaseAgg.snap.finished&&aTot>0)?(Anch.preEnd+(Anch.postEnd-Anch.preEnd)*Math.max(0,Math.min(1,aDone/aTot))):null;
    return appPct!=null?clamp(appPct):snapPct!=null?clamp(snapPct):null; };

  class SyncBar{
    constructor({el,onStart,onStop}={}){ 
      this.el=el||document.getElementById("ux-progress");
      this.timeline={start:false,pre:false,post:false,done:false};
      this._pctMemo=0; this._phaseMemo=-1; this._holdAtTen=false; this._optimistic=false;
      this._lastPhaseAt=Date.now(); this._lastEventTs=Date.now(); this._onStart=onStart; this._onStop=onStop;
      this._runKey=null; this._pairText=""; this._streamArmed=false;
      this._pendingDone=false; this._pendingDoneTimer=null; this._doneAt=0;
      this._successExit0Seen=false; this._exitCode=null; this._hadError=false;
      this.render(); 
    }

    _runKeyOf(s){ return s?.run_id||s?.run_uuid||s?.raw_started_ts||(s?.started_at?Date.parse(s.started_at):null)||null; }
    isStreamArmed(){ return !!this._streamArmed; }
    lastEvent(){ return this._lastEventTs; }
    isRunning(){ return !!(this._running || (this.timeline.start && !this.timeline.done)); }
    state(){ return {timeline:{...this.timeline}, running:this.isRunning()}; }

    reset(){ 
      clearTimeout(this._pendingDoneTimer); this._pendingDone=false; this._doneAt=0;
      this._pctMemo=0; this._phaseMemo=-1; this._holdAtTen=false; this._pairText=""; this._streamArmed=false;
      this._successExit0Seen=false; this._exitCode=null; this._hadError=false;
      PhaseAgg.snap={done:0,total:0,started:false,finished:false}; 
      PhaseAgg.apply={done:0,total:0,started:false,finished:false};
      SnapAgg.reset(); ApplyAgg.reset(); 
      this.timeline={start:false,pre:false,post:false,done:false}; 
      this.render(); 
    }

    markInit(){ 
      if (this.timeline.start && !this.timeline.done){ 
        this._running=true; this._streamArmed=true; this._lastEventTs=Date.now(); return;
      }
      this.reset(); this._optimistic=true; this._holdAtTen=true; this._streamArmed=true;
      this.timeline={start:true,pre:false,post:false,done:false}; 
      this._lastEventTs=Date.now(); 
      this.render(); 
      this._onStart?.(); 
    }

    setPair(d){
      const p=(d&&(d.pair||d))||{};
      const src=(p.src||p.provider_src||p.source||p.src_name||"").toString().toUpperCase();
      const dst=(p.dst||p.provider_dst||p.target||p.dst_name||"").toString().toUpperCase();
      const feat=(p.feature||p.lane||p.kind||"").toString().toLowerCase();
      const parts=[]; if(src||dst) parts.push([src,dst].filter(Boolean).join(" → ")); if(feat) parts.push(feat);
      this._pairText=parts.join(" · "); this.render();
    }

    _pairString(d){
      if(!d) return "";
      const src=(d.src||d.provider_src||d.source||"").toString().toUpperCase();
      const dst=(d.dst||d.provider_dst||d.target||"").toString().toUpperCase();
      const feat=(d.feature||d.lane||d.kind||"").toString().toLowerCase();
      const parts=[]; if(src||dst) parts.push([src,dst].filter(Boolean).join(" → ")); if(feat) parts.push(feat);
      return parts.join(" · ");
    }

    _maybePair(d){ 
      if(!this._streamArmed) return;
      const s=this._pairString(d); if(s) this._pairText=s; 
    }

    ingestLogLine(line){
      if(!line) return;
      const s=String(line);
      const m=s.match(/\[SYNC\]\s*exit code:\s*(\d+)/i);
      if(!m) return;
      const code=Number(m[1]);
      if(code===0) this.success();
      else this.fail(code);
    }

    success(){
      this._successExit0Seen=true; this._exitCode=0; this._hadError=false;
      clearTimeout(this._pendingDoneTimer); this._pendingDone=false; this._doneAt=0;
      this.timeline={start:true,pre:true,post:true,done:true};
      this._running=false; this._streamArmed=false;
      try{ this._onStop?.(); }catch{}
      this._lastEventTs=Date.now(); this.render();
    }

    fail(code=1){
      this._hadError=true; this._exitCode=code; this._successExit0Seen=false;
      clearTimeout(this._pendingDoneTimer); this._pendingDone=false; this._doneAt=0;
      this.timeline={start:true,pre:true,post:true,done:true};
      this._running=false; this._streamArmed=false;
      try{ this._onStop?.(); }catch{}
      this._lastEventTs=Date.now(); this.render();
    }

    _reopenForLateWork(){
      if(this.timeline.done && !this._pendingDone && !this._successExit0Seen){
        this.timeline.done=false;
        this._pendingDone=true;
        this._doneAt=this._doneAt||Date.now();
        this._streamArmed=true;
      }
    }

    _scheduleDone(delay=900){
      this._pendingDone=true;
      clearTimeout(this._pendingDoneTimer);
      this._pendingDoneTimer=setTimeout(()=>{
        const now=Date.now();
        const elapsed=now-(this._doneAt||now);
        const quietFor=now-(this._lastEventTs||0);
        if(elapsed<POST_DONE_GRACE_MS || quietFor<delay) return this._scheduleDone(delay);
        this._pendingDone=false;
        this.timeline={start:true,pre:true,post:true,done:true};
        this._running=false; this._streamArmed=false;
        try{ this._onStop?.(); }catch{}
        this._lastEventTs=Date.now(); this.render();
      },delay);
    }

    snap(d){
      if(!this._streamArmed && !this._pendingDone) return;
      this._reopenForLateWork();
      this._holdAtTen=false; this._maybePair(d);
      SnapAgg.update(d||{}); this.timeline.pre=true;
      this._lastEventTs=Date.now();
      if(this._pendingDone) this._scheduleDone();
      this.render();
    }
    applyStart(d){
      if(!this._streamArmed && !this._pendingDone) return;
      this._reopenForLateWork();
      this._maybePair(d); ApplyAgg.start(d||{}); this.timeline.post=true;
      this._lastEventTs=Date.now();
      if(this._pendingDone) this._scheduleDone();
      this.render();
    }
    applyProg(d){
      if(!this._streamArmed && !this._pendingDone) return;
      this._reopenForLateWork();
      this._maybePair(d); ApplyAgg.prog(d||{}); this.timeline.post=true;
      this._lastEventTs=Date.now();
      if(this._pendingDone) this._scheduleDone();
      this.render();
    }
    applyDone(d){
      if(!this._streamArmed && !this._pendingDone) return;
      this._reopenForLateWork();
      this._maybePair(d); ApplyAgg.done(d||{}); this.timeline.post=true;
      this._lastEventTs=Date.now();
      if(this._pendingDone) this._scheduleDone();
      this.render();
    }

    done(){
      this._running=false;
      this.timeline={start:true,pre:true,post:true,done:false};
      this._doneAt=Date.now();
      this._lastEventTs=Date.now();
      this._scheduleDone(); this.render();
    }
    error(){
      this._hadError=true; this._exitCode=1; this._successExit0Seen=false;
      clearTimeout(this._pendingDoneTimer); this._pendingDone=false; this._doneAt=0;
      this.timeline.done=true; this._running=false; this._streamArmed=false;
      try{ this._onStop?.(); }catch{}
      this._lastEventTs=Date.now(); this.render();
    }

    fromSummary(sum){
      const prevRunning=this.isRunning(), prevTL={...this.timeline};
      if(!sum) return {running:prevRunning,justStarted:false,justFinished:false};

      const key=this._runKeyOf(sum);
      const running=sum?.running===true||sum?.state==="running";
      if(!running&&!this.timeline.start&&!this.timeline.pre&&!this.timeline.post&&!this.timeline.done
        &&(sum?.exit_code!=null||sum?.finished||sum?.end||sum?.state==="idle")){
        this.reset(); return {running:false,justStarted:false,justFinished:false};
      }

      const mappedRaw={
        start:!!(sum?.timeline?.start||sum?.timeline?.started||sum?.timeline?.[0]||sum?.started),
        pre:!!(sum?.timeline?.pre||sum?.timeline?.discovery||sum?.timeline?.discovering||sum?.timeline?.[1]),
        post:!!(sum?.timeline?.post||sum?.timeline?.syncing||sum?.timeline?.apply||sum?.timeline?.[2]),
        done:!!(sum?.timeline?.done||sum?.timeline?.finished||sum?.timeline?.complete||sum?.timeline?.[3]),
      };
      let mapped={...mappedRaw};
      if(sum?.phase){
        const p=String(sum.phase).toLowerCase();
        if(p==="snapshot") mapped.pre=true;
        if(p==="apply"||p==="sync"||p==="syncing") mapped.post=true;
      }
      const exitCode=sum?.exit_code!=null?Number(sum.exit_code):null;

      // Hard finalize
      if (exitCode != null) {
        if (exitCode === 0) this.success();
        else this.fail(exitCode);
        return { running: false, justStarted: false, justFinished: true };
      }

      if(key&&key!==this._runKey){ this._runKey=key; this.markInit(); }
      this._streamArmed=!!(running||(mapped.start&&!mapped.done)||this._pendingDone);

      const ph=sum?._phase||{};
      if(ph.snapshot && PhaseAgg.snap.total===0){
        PhaseAgg.snap.total=+ph.snapshot.total||0;
        PhaseAgg.snap.done=+ph.snapshot.done||0;
        PhaseAgg.snap.started=PhaseAgg.snap.total>0;
        PhaseAgg.snap.finished=!!ph.snapshot.final||(PhaseAgg.snap.total>0&&PhaseAgg.snap.done>=PhaseAgg.snap.total);
      }
      if(ph.apply){
        PhaseAgg.apply.total=+ph.apply.total||0;
        PhaseAgg.apply.done=+ph.apply.done||0;
        PhaseAgg.apply.started=PhaseAgg.apply.total>0;
        PhaseAgg.apply.finished=!!ph.apply.final||(PhaseAgg.apply.total>0&&PhaseAgg.apply.done>=PhaseAgg.apply.total);
      }

      this._running=running;
      const clampTL=next=>(phaseIdx(next)<phaseIdx(this.timeline))?this.timeline:next;
      mapped=clampTL(mapped);
      if(mapped.start!==prevTL.start||mapped.pre!==prevTL.pre||mapped.post!==prevTL.post||mapped.done!==prevTL.done) this._lastPhaseAt=Date.now();
      this.timeline=mapped;

      const logicalDone=(PhaseAgg.snap.finished&&(PhaseAgg.apply.finished||PhaseAgg.apply.total===0));
      const nowInProgress=running||(this.timeline.start&&!this.timeline.done);
      const wasInProgress=prevRunning||(prevTL.start&&!prevTL.done)||this._optimistic;
      const justFinished=wasInProgress&&!nowInProgress&&(this.timeline.done||logicalDone);

      if(!nowInProgress&&!this._pendingDone&&(sum.exit_code!=null||this.timeline.done)) this._streamArmed=false;

      this._lastEventTs=Date.now(); this.render();
      return {running:nowInProgress, justStarted:(!prevRunning&&nowInProgress), justFinished};
    }

    updateTimeline(tl){
      const clampTL=next=>(phaseIdx(next)<phaseIdx(this.timeline))?this.timeline:next;
      this.timeline=clampTL({start:!!tl.start,pre:!!tl.pre,post:!!tl.post,done:!!tl.done});
      this.render();
    }
    updatePct(p){ if(typeof p==="number"){ this._pctMemo=Math.max(this._pctMemo,clamp(p)); this.render(); } }

    render(){
      const host=this.el; if(!host) return;
      host.innerHTML="";
      const rail=document.createElement("div"); rail.className="sb-rail";
      const fill=document.createElement("div"); fill.className="sb-fill";
      const fly=document.createElement("div"); fly.className="sb-fly hide"; fly.textContent=this._pairText||"";

      const allowDone=!!this._successExit0Seen;
      const logicalDone=(PhaseAgg.snap.finished&&(PhaseAgg.apply.finished||PhaseAgg.apply.total===0));
      const hardDone=allowDone && (!this._pendingDone) && (this.timeline.done||logicalDone);

      const byPhases=pctFromPhase();
      let base=byPhases;

      if(hardDone){
        base=Anch.done;
      }else{
        if(base==null||(this.timeline.post&&!PhaseAgg.apply.started)) base=asPctFromTimeline(this.timeline, allowDone);
        if(this._holdAtTen&&!PhaseAgg.snap.started) base=Math.max(base,10);
        base=Math.min(base,Anch.postEnd);
      }

      const idx=phaseIdx(this.timeline);
      if(idx<this._phaseMemo) base=this._pctMemo;
      if(idx>this._phaseMemo) this._phaseMemo=idx;

      this._pctMemo=Math.max(this._pctMemo,clamp(base));
      fill.style.width=this._pctMemo+"%";

      const isRunning = this.isRunning();
      const shouldFlow = isRunning && !hardDone;

      rail.classList.toggle("running", isRunning && !this.timeline.done);
      rail.classList.toggle("indet", shouldFlow);
      rail.classList.toggle("apply", PhaseAgg.apply.started && !PhaseAgg.apply.finished);
      rail.classList.toggle("starting", isRunning && !(this.timeline.pre || this.timeline.post));
      rail.classList.toggle(
        "finishing",
        !isRunning && !this.timeline.done && (logicalDone || this._pendingDone)
      );
      rail.classList.toggle("error", this._hadError);

      const pct=this._pctMemo/100, railW=host.clientWidth||1;
      const left=Math.max(8,Math.min(railW-8, railW*pct));
      fly.style.left=left+"px"; fly.classList.toggle("hide",!(isRunning&&this._pairText));

      const steps=document.createElement("div"); steps.className="sb-steps muted";
      [["Start","start"],["Discovering","discovering"],["Syncing","syncing"],["Done","done"]].forEach(([txt,key])=>{
        const s=document.createElement("span"); s.textContent=txt; s.dataset.step=key; steps.appendChild(s);
      });

      rail.append(fill,fly); host.append(rail,steps);
    }
  }
  window.SyncBar=SyncBar;
})();