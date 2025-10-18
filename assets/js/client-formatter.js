(function (w, d) {
  "use strict";

  // Inject styles once
  if (!d.getElementById("cf-styles")) {
    d.head.insertAdjacentHTML("beforeend", `<style id="cf-styles">
      .cf-log{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:13px;line-height:1.35}
      .cf-line,.cf-event{display:block;margin:1px 0}
      .cf-event{padding:4px 6px;border-left:2px solid rgba(255,255,255,.12);border-radius:4px;background:rgba(255,255,255,.02)}
      .cf-event .cf-ico{margin-right:8px}
      .cf-event .cf-meta{opacity:.85;font-size:12px;margin-left:4px}
      .cf-event .cf-meta b{opacity:1}
      .cf-sep{opacity:.6;margin:0 4px}
      .cf-event.start{border-color:#9aa0a6}
      .cf-event.pair{border-color:#8ab4f8}
      .cf-event.plan{border-color:#cfcfcf}
      .cf-event.remove{border-color:#ef5350}
      .cf-event.add{border-color:#66bb6a}
      .cf-event.done{border-color:#2fb170}
      .cf-event.complete{border-color:#25a05f;font-weight:700;position:relative;overflow:hidden}
      .cf-muted{opacity:.72}
      .cf-ok{color:#2fb170;font-weight:600}
      .cf-ok-strong{color:#25a05f;font-weight:700}
      .cf-arrow{opacity:.9;margin:0 6px}
      .cf-badge{display:inline-flex;align-items:center;gap:6px;padding:2px 8px;border-radius:999px;font-weight:700;font-size:12px;line-height:1.2;border:1px solid rgba(255,255,255,.15);margin:0 2px;vertical-align:baseline;box-shadow:0 0 .5px rgba(255,255,255,.15) inset,0 0 6px rgba(255,255,255,.03) inset}
      .cf-badge img{width:14px;height:14px;display:block;filter:drop-shadow(0 0 1px rgba(0,0,0,.25))}
      .cf-plex{background:#2b240a;color:#ffbf3a;border-color:rgba(255,191,58,.28)}
      .cf-simkl{background:#072430;color:#35d1ff;border-color:rgba(53,209,255,.28)}
      .cf-trakt{background:#2b0a0a;color:#ff6470;border-color:rgba(255,100,112,.28)}
      .cf-jellyfin{background:#15102b;color:#9aa5ff;border-color:rgba(154,165,255,.28)}
      .cf-generic{background:#1b1b1b;color:#eaeaea}
      .cf-event.progress{border-color:#0ea5e9}
      .cf-prog-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
      .cf-prog-meta{opacity:.85;font-size:12px;margin-left:8px}
      .cf-prog-bar{position:relative;height:10px;border-radius:6px;background:rgba(255,255,255,.08);overflow:hidden;margin-left:auto;flex:0 0 38%;min-width:220px}
      .cf-prog-fill{position:absolute;inset:0 100% 0 0;background:linear-gradient(90deg,#0ea5e9,#0369a1);transition:inset .25s ease;box-shadow:0 0 8px rgba(14,165,233,.22) inset}
      .cf-prog-text{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:11px;opacity:.95;color:#e8f6ff;text-shadow:0 1px 2px rgba(0,0,0,.35)}
      .cf-prog-done .cf-prog-fill{background:linear-gradient(90deg,#0aa56c,#0fbf72)}
      .cf-prog-done .cf-prog-text{font-weight:700}
      .cf-prog-sub{opacity:.85;font-size:12px;margin-left:6px}
      .cf-prog-badge{margin-left:4px}
      .cf-fade-in{animation:cfFade .14s ease-out}
      .cf-pop{animation:cfPop .18s ease-out}
      .cf-slide-in{animation:cfSlide .18s ease-out}
      .cf-pulse{animation:cfPulse .6s ease-out}
      @keyframes cfFade{from{opacity:0;transform:translateY(1px)}to{opacity:1;transform:none}}
      @keyframes cfPop{from{transform:scale(.98)}to{transform:scale(1)}}
      @keyframes cfSlide{from{transform:translateX(-6px);opacity:.0}to{transform:none;opacity:1}}
      @keyframes cfPulse{0%{box-shadow:0 0 0 0 rgba(14,165,233,.35)}100%{box-shadow:0 0 0 10px rgba(14,165,233,0)}}

      .cf-prog-stats{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;justify-content:flex-end}
      .cf-stat{display:inline-flex;align-items:center;gap:6px;padding:2px 8px;border-radius:999px;font-size:11px;line-height:1.2;border:1px solid rgba(255,255,255,.15);opacity:.95}
      .cf-stat b{font-weight:700}
      .cf-stat.stat-ok{color:#25a05f;border-color:rgba(37,160,95,.35);background:rgba(37,160,95,.08)}
      .cf-stat.stat-warn{color:#f59e0b;border-color:rgba(245,158,11,.35);background:rgba(245,158,11,.08)}
      .cf-stat.stat-err{color:#ef5350;border-color:rgba(239,83,80,.35);background:rgba(239,83,80,.08)}
      .cf-stat.stat-muted{opacity:.8}
    </style>`);
  }

  // Small helpers
  const esc = (s)=>String(s??"").replace(/[&<>]/g,(m)=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[m]));
  const ICON={start:"▶",pair:"🔗",plan:"📝",add:"➕",remove:"➖",done:"✅",complete:"🏁",unresolved:"⚠️"};
  const PROV={PLEX:{cls:"cf-plex",logo:"/assets/img/PLEX-log.svg"},SIMKL:{cls:"cf-simkl",logo:"/assets/img/SIMKL-log.svg"},TRAKT:{cls:"cf-trakt",logo:"/assets/img/TRAKT-log.svg"},JELLYFIN:{cls:"cf-jellyfin",logo:"/assets/img/JELLYFIN-log.svg"}};
  const arrowFor=(m)=>String(m||"").toLowerCase().startsWith("two")?"⇄":"→";
  const cap=(s)=>String(s||"").replace(/^./,(c)=>c.toUpperCase());
  const badge=(name)=>{const key=String(name||"").toUpperCase(),p=PROV[key]||{cls:"cf-generic"};const img=p.logo?`<img src="${p.logo}" alt="" aria-hidden="true">`:"";return `<span class="cf-badge ${p.cls}">${img}${esc(key)}</span>`;};
  const block=(type,titleHTML,metaText,extra)=>`<div class="cf-event ${type} ${(type==="start"?"cf-slide-in cf-pulse":"cf-fade-in")}${extra?(" "+String(extra).replace(/\b(?:cf-)?complete-shimmer\b/g,"").replace(/\s+/g," ").trim()):""}"><span class="cf-ico"></span>${titleHTML}${metaText?`<span class="cf-sep">·</span><span class="cf-meta">${metaText}</span>`:""}</div>`;

  // State
  let pendingRunId=null; let pair={A:"A",B:"B"}; let counts={add:{},remove:{}}; const resetCounts=(a,b)=>(counts={add:{[a]:0,[b]:0},remove:{[a]:0,[b]:0}});
  const dstNameFrom=(ev)=>ev?.dst?String(ev.dst).toUpperCase():String(ev?.event||"").includes(":A:")?pair.A:pair.B;
  let squelchPlain=0;

  // Progress bookkeeping
  const progMap=Object.create(null);
  const progPendingTick=Object.create(null);
  const progKey=(ev)=>{const dst=String(ev.dst||ev.provider||"DST").toUpperCase();const feat=String(ev.feature||"watchlist").toLowerCase();if(ev.event==="snapshot:progress")return `snap|${dst}|${feat}`;if(/^apply:/.test(ev.event||"")){const action=(ev.event.split(":")[1]||"add").toLowerCase();return `apply|${dst}|${feat}|${action}`}return null};
  const slug=(s)=>String(s).replace(/[^a-z0-9|_-]/gi,"_");

  // Create a progress row if missing
  function ensureProgressRow(root,key,ev){
    let el=progMap[key]; if(el&&root.contains(el)) return el;
    const [mode,dst,feat,action]=String(key).split("|");
    const dstBadge=badge(dst);
    const titleIcon=mode==="snap"?"📸":action==="remove"?ICON.remove:ICON.add;
    const verb=mode==="snap"?"Snapshot":action==="remove"?"Removing":"Adding";
    const wrap=d.createElement("div");
    wrap.className="cf-event progress cf-fade-in";
    wrap.setAttribute("data-cf-prog",slug(key));
    // CHANGED: moved stats container below the head (bar)
    wrap.innerHTML=`<div class="cf-prog-head"><span class="cf-ico"></span>${titleIcon} ${verb} <span class="cf-prog-badge">${dstBadge}</span><span class="cf-prog-sub">· ${esc(cap(feat))}</span><div class="cf-prog-bar"><div class="cf-prog-fill"></div><div class="cf-prog-text">0%</div></div></div><div class="cf-prog-stats"></div>`;
    root.appendChild(wrap); progMap[key]=wrap; return wrap;
  }

  // Update progress row with guardrails
  function updateProgressRow(root,ev,opts={}){
    const key=progKey(ev); if(!key) return;
    const row=ensureProgressRow(root,key,ev);
    const fill=row.querySelector(".cf-prog-fill"), txt=row.querySelector(".cf-prog-text");
    const statsEl=row.querySelector(".cf-prog-stats");

    let done=Number(ev.done||0);
    let total=Number(ev.total||0)||Number(ev.count||0)||Number(row.getAttribute("data-total")||0);
    if(!total&&"count"in ev&&Number(ev.count)>0) total=Number(ev.count);
    if(total>0) row.setAttribute("data-total",String(total));

    if(total>0 && done>=total-1 && (ev.event==="snapshot:progress" || /^apply:.*:progress$/.test(String(ev.event)))){
      done=total; opts.final=true;
    }

    const pct=total>0?Math.max(0,Math.min(100,Math.round((done/total)*100))):0;

    if(fill) fill.style.insetInlineEnd=`${100-pct}%`;
    if(txt) txt.textContent=total>0?`${Math.min(done,total)}/${total} · ${pct}%`:`${done}`;

    if((total>0&&done>=total)||opts.final===true){
      row.classList.add("cf-prog-done");
      if(txt) txt.textContent=total>0?`${total}/${total} · 100%`:"100%";

      // render compact stats as pills on their own line (apply rows only)
      const isApply = /^apply:/.test(String(ev.event||"")) || String(key||"").startsWith("apply|");
      if (isApply && statsEl) {
        const action     = (String(ev.event||"").includes(":remove:") ? "remove" : "add");
        const attempted  = Number(ev.attempted  ?? ev.result?.attempted  ?? 0);
        const confirmed  = Number(ev.confirmed  ?? ev.result?.confirmed  ?? ev.result?.count ?? 0);
        const skipped    = Number(ev.skipped    ?? ev.result?.skipped    ?? 0);
        const unresolved = Number(ev.unresolved ?? ev.result?.unresolved ?? 0);
        const errors     = Number(ev.errors     ?? ev.result?.errors     ?? 0);

        const word = action === "remove" ? "removed" : "added";
        const pills = [
          { cls: "",          txt: `<b>${attempted}</b> attempted` },
          { cls: "stat-ok",   txt: `<b>${confirmed}</b> ${word}` },
          skipped    > 0 && { cls: "stat-muted", txt: `<b>${skipped}</b> skipped` },
          unresolved > 0 && { cls: "stat-warn",  txt: `<b>${unresolved}</b> unresolved` },
          errors     > 0 && { cls: "stat-err",   txt: `<b>${errors}</b> errors` }
        ].filter(Boolean).map(p => `<span class="cf-stat ${p.cls}">${p.txt}</span>`).join(" ");

        statsEl.innerHTML = pills;
      }

      progPendingTick[key]=false;
      return;
    }

    progPendingTick[key]=true;
  }

  // Force-finish any armed bars on the very next line
  function finishArmedBars(root){
    for(const key in progPendingTick){
      if(!progPendingTick[key]) continue;
      const row=progMap[key]; if(!row) { progPendingTick[key]=false; continue; }
      const total=Number(row.getAttribute("data-total")||0);
      const fill=row.querySelector(".cf-prog-fill"), txt=row.querySelector(".cf-prog-text");
      if(total>0){
        if(fill) fill.style.insetInlineEnd=`0%`;
        row.classList.add("cf-prog-done");
        if(txt) txt.textContent=`${total}/${total} · 100%`;
      }else{
        if(fill) fill.style.insetInlineEnd=`0%`;
        row.classList.add("cf-prog-done");
        if(txt) txt.textContent=`100%`;
      }
      progPendingTick[key]=false;
    }
  }

  // Friendly JSON → HTML
  function formatFriendlyLog(line){
    if(!line||line[0]!=="{") return null;
    let ev; try{ev=JSON.parse(line);}catch{return null;}
    if(!ev?.event) return null;

    switch(ev.event){
      case "run:start": {
        const meta=[`dry_run=${!!ev.dry_run}`,`conflict=${esc(ev.conflict||"source")}`];
        if(pendingRunId){meta.push(`run_id=${pendingRunId}`); pendingRunId=null;}
        return block("start",`${ICON.start} Sync started`,meta.join(" · "));
      }
      case "run:pair": {
        const i=ev.i|0,n=ev.n|0,A=String(ev.src||"").toUpperCase(),B=String(ev.dst||"").toUpperCase();
        pair={A,B}; resetCounts(A,B);
        const idx=i&&n?` ${i}/${n}`:"";
        const fs=Array.isArray(ev.features)?ev.features:(ev.feature?[ev.feature]:[]);
        const ftxt=fs.length>1?`features=<b>${esc(fs.map(x=>cap(String(x||""))).join(", "))}</b>`:`feature=<b>${esc(cap(String(fs[0]||"")))}</b>`;
        const meta=`${ftxt} · mode=${esc(ev.mode||"one-way")}${ev.dry_run?" · dry_run=true":""}`;
        return block("pair",`${ICON.pair} Pair${idx}: ${badge(A)} <span class="cf-arrow">${arrowFor(ev.mode)}</span> ${badge(B)}`,meta);
      }
      case "two:start": return block("start",`⇄ Two-way sync`, `feature=${esc(ev.feature)} · removals=${!!ev.removals}`);
      case "snapshot:progress":
      case "apply:add:start":
      case "apply:add:progress":
      case "apply:remove:start":
      case "apply:remove:progress":
        return null;

      case "one:plan": {
        const adds=ev.adds|0,removes=ev.removes|0,has=adds+removes;
        const feature=esc(cap(ev.feature));
        const title=`${ICON.plan} Plan for ${feature}`;
        const meta=has?`adding ${adds}, removing ${removes}`:"nothing to do";
        return block("plan",title,meta,has?"":"cf-muted");
      }

      case "apply:unresolved": {
        const count=ev.count|0,feature=esc(cap(ev.feature));
        return block("plan",`${ICON.unresolved} ${count} unresolved ${feature} on ${badge(ev.provider)}`,"item could not be matched","cf-muted");
      }

      case "two:plan": {
        const aA=ev.add_to_A|0,aB=ev.add_to_B|0,rA=ev.rem_from_A|0,rB=ev.rem_from_B|0,has=aA+aB+rA+rB;
        return block("plan",`${ICON.plan} Plan`,has?`add A=${aA}, add B=${aB}, remove A=${rA}, remove B=${rB}`:`nothing to do`,has?"":"cf-muted");
      }

      case "two:apply:add:A:done":
      case "two:apply:add:B:done":
      case "two:apply:remove:A:done":
      case "two:apply:remove:B:done": {
        const kind=ev.event.includes("add")?"add":"remove";
        const who=dstNameFrom(ev);
        const cnt=Number(ev.result?.count??ev.count??0);
        counts[kind][who]=(counts[kind][who]||0)+cnt;
        return null;
      }

      case "two:done": {
        const {A,B}=pair;
        const row=(kind,aCnt,bCnt)=>block(kind,`${ICON[kind]} ${cap(kind)}`,`${A}·${aCnt} / ${B}·${bCnt}`,aCnt+bCnt?"":"cf-muted");
        return [row("remove",counts.remove[A]|0,counts.remove[B]|0),row("add",counts.add[A]|0,counts.add[B]|0)].join("");
      }

      case "run:done": {
        const parts = [`+${ev.added|0} / -${ev.removed|0}`, `pairs=${ev.pairs|0}`];
        if ("skipped" in ev) parts.push(`skipped=${ev.skipped|0}`);
        if ("unresolved" in ev) parts.push(`unresolved=${ev.unresolved|0}`);
        if ("errors" in ev) parts.push(`errors=${ev.errors|0}`);
        return block("complete",`${ICON.complete} Sync complete`, parts.join(" · "));
      }

      case "debug": return null;
      default: return null;
    }
  }

  // WL prettifier
  function formatWL(line){
    const t=String(line||"").trim();
    let m=t.match(/^\[WL]\s*delete\s+(\d+)\s+([A-Za-z]+)\s+'([^']*)'\s+on\s+([A-Z]+)\s*:\s*(OK|NOOP)\s*$/i);
    if(m){const[, ,kindRaw,labelRaw,provRaw,statusRaw]=m;const prov=String(provRaw).toUpperCase();const ok=String(statusRaw).toUpperCase()==="OK";const kind=String(kindRaw||"").toLowerCase();const safe=String(labelRaw||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/'/g,"’");const title=`${badge(prov)} ${ICON.remove} Delete ${esc(kind)} ‘${safe}’`;const meta=ok?"removed":"no change";return block(ok?"remove":"plan",title,meta,ok?"cf-pop":"cf-muted");}
    m=t.match(/^\[WL]\s*delete\s+(\d+)\s+on\s+([A-Z]+)\s*:\s*(OK|NOOP)\s*$/i);
    if(m){const[,cnt,provRaw,statusRaw]=m;const prov=String(provRaw).toUpperCase();const ok=String(statusRaw).toUpperCase()==="OK";const title=`${badge(prov)} ${ICON.remove} Delete ${Number(cnt)||0} item(s)`;return block(ok?"remove":"plan",title,ok?"removed":"no change",ok?"cf-pop":"cf-muted");}
    m=t.match(/^\[WL]\s*delete\s+on\s+([A-Z]+)\s+failed:\s*(.+)$/i);
    if(m){const[,provRaw,reason]=m;const prov=String(provRaw).toUpperCase();const title=`${badge(prov)} ${ICON.remove} Delete failed`;return block("remove",title,esc(reason||"error"),"cf-muted");}
    return null;
  }

  // Host/plain filtering
  function filterPlainLine(line,isDebug){
    const t=String(line||"").trim(); if(!t) return null;
    const mOrch=t.match(/^>\s*SYNC start:\s*orchestrator\s+pairs\s+run_id=(\d+)/i);
    if(mOrch){pendingRunId=mOrch[1]; return block("start",`${ICON.start} Start: orchestrator PAIR: ${pendingRunId}`);}
    const mRun=t.match(/^>\s*SYNC start:.*?\brun_id=(\d+)/i);
    if(mRun){pendingRunId=mRun[1]; return null;}
    if(!isDebug){
      if(/^sync start:\s*orchestrator/i.test(t)) return null;
      if(/^\[i]\s*triggered sync run/i.test(t)) return null;
      if(/^\[i]\s*orchestrator module:/i.test(t)) return null;
      if(/^\[i]\s*providers:/i.test(t)){squelchPlain=2; return null;}
      if(/^\[i]\s*features:/i.test(t)){squelchPlain=3; return null;}
      if(/^\[\d+\/\d+]\s+/i.test(t)) return null;
      if(/^•\s*feature=/i.test(t)) return null;
      if(/^\[SYNC]\s*exit code:/i.test(t)) return null;
    }
    const mDone=t.match(/^\[i]\s*Done\.\s*Total added:\s*(\d+),\s*Total removed:\s*(\d+)(?:,\s*Total skipped:\s*(\d+))?(?:,\s*Total unresolved:\s*(\d+))?(?:,\s*Total errors:\s*(\d+))?/i);
    if(mDone){
      const added=Number(mDone[1]||0), removed=Number(mDone[2]||0), skipped=Number(mDone[3]||0), unresolved=Number(mDone[4]||0), errors=Number(mDone[5]||0);
      const parts=[`+${added} / -${removed}`];
      if(mDone[3]) parts.push(`skipped=${skipped}`);
      if(mDone[4]) parts.push(`unresolved=${unresolved}`);
      if(mDone[5]) parts.push(`errors=${errors}`);
      return block("complete",`${ICON.complete} Sync complete`, parts.join(" · "));
    }
    const mSched1=t.match(/^\s*(?:\[?INFO]?)\s*\[?SCHED]?\s*scheduler\s+(started|stopped|refreshed)\s*\((enabled|disabled)\)/i);
    if(mSched1) return block(mSched1[2].toLowerCase()==="enabled"?"start":"remove",`⏱️ Scheduler`,`${mSched1[1].toLowerCase()} · ${mSched1[2].toLowerCase()}`);
    const mSched2=t.match(/^\s*(?:\[?INFO]?)\s*scheduler:\s*started\s*(?:&|&amp;)\s*refreshed\s*$/i);
    if(mSched2) return block("start",`⏱️ Scheduler`,`started · refreshed`);
    const wl=formatWL(t); if(wl) return wl;
    return t;
  }

  // Chunking + JSON extract
  function splitHost(s){
    return String(s).replace(/\r\n/g,"\n").replace(/(?<!\n)(>\s*SYNC start:[^\n]*)/g,"\n$1").replace(/(?<!\n)(\[\s*i\s*]\s*[^\n]*)/gi,"\n$1").replace(/(?<!\n)(\[SYNC]\s*exit code:[^\n]*)/g,"\n$1").replace(/(?<!\n)(▶\s*Sync started[^\n]*)/g,"\n$1").replace(/(?<!\n)(🔗\s*Pair:[^\n]*)/g,"\n$1").replace(/(?<!\n)(📝\s*Plan[^\n]*)/g,"\n$1").replace(/(?<!\n)(✅\s*Pair finished[^\n]*)/g,"\n$1").replace(/(?<!\n)(🏁\s*Sync complete[^\n]*)/g,"\n$1").replace(/}\s*(?=\{")/g,"}\n").split(/\n+/);
  }

  function processChunk(buf,chunk){
    let s=(buf||"")+String(chunk||""), tokens=[], i=0;
    const emitPlain=(piece)=>{if(!piece) return; for(const ln of splitHost(piece)) if(ln.trim()) tokens.push(ln);};
    while(i<s.length){
      if(s[i]!=="{"){
        const j=s.indexOf("{",i);
        if(j===-1){emitPlain(s.slice(i)); i=s.length; break;}
        emitPlain(s.slice(i,j)); i=j;
      }
      let depth=0,inStr=false,escp=false,k=i;
      for(;k<s.length;k++){
        const ch=s[k];
        if(inStr){escp?(escp=false):ch==="\\"?(escp=true):ch===`"`&&(inStr=false);}
        else{ch===`"`?(inStr=true):ch==="{"?depth++:ch==="}"&&--depth===0&&(k++,1);}
        if(depth===0&&!inStr&&k>i&&s[k-1]==="}") break;
      }
      if(depth===0&&k<=s.length){tokens.push(s.slice(i,k)); i=k;} else break;
    }
    return {tokens,buf:s.slice(i)};
  }

  // Plain squelch
  const isContinuationLine=(t)=>/^[\{\[]/.test(t)||/^['"]?[A-Za-z0-9_]+['"]?\s*:/.test(t)||/^\s{2,}\S/.test(t)||/[}\]]$/.test(t);
  const shouldDropAndMaybeSquelch=(t,isDebug)=>{
    if(isDebug) return false;
    if(/^\[i]\s*providers:/i.test(t)){squelchPlain=2; return true;}
    if(/^\[i]\s*features:/i.test(t)){squelchPlain=3; return true;}
    if(/^>\s*SYNC start:/i.test(t)) return true;
    if(/^\[i]\s*triggered sync run/i.test(t)) return true;
    if(/^\[i]\s*orchestrator module:/i.test(t)) return true;
    if(/^\[\d+\/\d+]\s+/i.test(t)) return true;
    if(/^•\s*feature=/i.test(t)) return true;
    if(/^\[SYNC]\s*exit code:/i.test(t)) return true;
    return false;
  };

  // Row limit (keeps UI snappy)
  const CF_MAX_ROWS = 400;
  function trimRows(el){
    const rows = el.querySelectorAll(".cf-event,.cf-line");
    const extra = rows.length - CF_MAX_ROWS;
    for(let i=0;i<extra;i++) rows[i]?.remove();
  }

  // Render one line
  function renderInto(el,line,isDebug){
    if(!el||!line) return;
    isDebug=!!(isDebug??(typeof window!=="undefined"&&window.appDebug));

    finishArmedBars(el);

    const trimmed=String(line).trim();

    if(trimmed.startsWith("{")){
      try{
        const ev=JSON.parse(trimmed);
        if(ev&&ev.event){
          if(ev.event==="debug"){
            if(isDebug){
              const meta=Object.entries(ev).filter(([k])=>!["event","msg"].includes(k)).map(([k,v])=>`${k}=${v}`).join(", ");
              el.insertAdjacentHTML("beforeend", block("plan", `🐞 ${esc(ev.msg||"debug")}`, meta, "cf-muted"));
              trimRows(el);
            }else if(String(ev.msg)==="blocked.counts"){
              const n=Number(ev.blocked_total||ev.total||0);
              if(n>0){ el.insertAdjacentHTML("beforeend", block("plan", `🚫 Blocked: ${n}`, "")); trimRows(el); }
            }
            return;
          }

          if(ev.event==="snapshot:progress"){ updateProgressRow(el,ev,{final:ev.final===true}); trimRows(el); return; }
          if(/^apply:/.test(ev.event)){
            if(/:start$/.test(ev.event)){ updateProgressRow(el,{...ev,done:0,total:ev.count||ev.total||0}); trimRows(el); return; }
            if(/:progress$/.test(ev.event)){ updateProgressRow(el,ev); trimRows(el); return; }
            if(/:done$/.test(ev.event)){
              const key=progKey(ev), row=key&&progMap[key];
              const tot=Number(ev.result?.count??ev.count??(row&&row.getAttribute("data-total"))??0);
              updateProgressRow(el,{...ev,done:tot,total:tot},{final:true}); trimRows(el); return;
            }
          }
        }
      }catch{/* ignore bad json */}
    }

    if(isDebug){const div=d.createElement("div"); div.className="cf-line"; div.textContent=String(line); el.appendChild(div); trimRows(el); return;}

    const fancy=formatFriendlyLog(line);
    if(fancy!=null){el.insertAdjacentHTML("beforeend",fancy); trimRows(el); return;}

    if(String(line).trim().startsWith("{")) return;

    const t=String(line).trim(); if(!t) return;
    if(squelchPlain>0&&isContinuationLine(t)){squelchPlain--; return;}
    if(squelchPlain>0&&!isContinuationLine(t)) squelchPlain=0;
    if(shouldDropAndMaybeSquelch(t,false)) return;

    const out=filterPlainLine(t,false); if(!out) return;
    if(/^<.+>/.test(out)) el.insertAdjacentHTML("beforeend",out);
    else {const div=d.createElement("div"); div.className="cf-line cf-fade-in"; div.textContent=out; el.appendChild(div);}
    trimRows(el);
  }

  w.ClientFormatter={formatFriendlyLog,filterPlainLine,splitHost,processChunk,renderInto};
})(window,document);
