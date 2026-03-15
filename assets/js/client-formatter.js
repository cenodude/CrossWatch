/* assets/js/client-formatter.js */
/* refactored */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  "use strict";

  if (!d.getElementById("cf-styles")) d.head.insertAdjacentHTML("beforeend", `<style id="cf-styles">
    .cf-log{white-space:normal!important;overflow-wrap:anywhere;word-break:break-word;font:500 13px/1.25 "Segoe UI",system-ui,sans-serif;color:#e7ebff}
    .cf-line,.cf-event{display:block;margin:1px 0}
    .cf-line{padding:1px 0;color:rgba(226,231,255,.78)}
    .cf-event{padding:1px 0;border:0;border-radius:0;background:transparent;box-shadow:none}
    .cf-head{display:inline-flex;align-items:center;gap:8px;flex-wrap:nowrap;vertical-align:middle}
    .cf-tag,.cf-badge,.cf-stat{display:inline-flex;align-items:center;border-radius:999px}
    .cf-tag{justify-content:center;min-width:54px;padding:3px 8px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.04);color:#d7defc;font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}
    .cf-title{font-weight:700;color:#f4f7ff}
    .cf-event .cf-meta,.cf-prog-sub,.cf-prog-meta{opacity:.82;font-size:11px}
    .cf-event .cf-meta b,.cf-stat b{opacity:1;font-weight:700}
    .cf-sep{opacity:.5;margin:0 6px}
    .cf-muted{opacity:.68}.cf-ok{color:#7fe0a6;font-weight:700}.cf-ok-strong{color:#9af0b8;font-weight:800}.cf-arrow{opacity:.72;margin:0 4px}
    .cf-badge,.cf-stat{gap:6px;padding:3px 9px;border:1px solid rgba(130,149,210,.18);background:rgba(255,255,255,.04);font-size:11px;line-height:1.2}
    .cf-badge{font-weight:800;margin:0 2px;vertical-align:baseline}.cf-badge img{width:13px;height:13px;display:block}
    .cf-prog-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
    .cf-prog-badge{margin-left:2px}
    .cf-prog-bar{position:relative;height:8px;border-radius:999px;background:rgba(255,255,255,.06);overflow:hidden;margin-left:auto;flex:0 0 36%;min-width:240px;border:1px solid rgba(130,149,210,.12)}
    .cf-prog-fill{position:absolute;inset:0 100% 0 0;background:linear-gradient(90deg,#4f5ff0,#5b93ff);transition:inset .25s ease}
    .cf-prog-done .cf-prog-fill{background:linear-gradient(90deg,#189c68,#38c98d)}
    .cf-prog-text{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:11px;font-weight:700;color:#eef3ff}
    .cf-prog-stats{display:flex;gap:6px;flex-wrap:wrap;margin-top:4px;justify-content:flex-end}
    .cf-stat.stat-ok{color:#25a05f;border-color:rgba(37,160,95,.35);background:rgba(37,160,95,.08)}
    .cf-stat.stat-warn{color:#f59e0b;border-color:rgba(245,158,11,.35);background:rgba(245,158,11,.08)}
    .cf-stat.stat-err{color:#ef5350;border-color:rgba(239,83,80,.35);background:rgba(239,83,80,.08)}
    .cf-stat.stat-muted{opacity:.8}
    .cf-fade-in{animation:cfFade .14s ease-out}.cf-pop{animation:cfPop .18s ease-out}.cf-slide-in{animation:cfSlide .18s ease-out}
    @keyframes cfFade{from{opacity:0;transform:translateY(1px)}to{opacity:1;transform:none}}
    @keyframes cfPop{from{transform:scale(.98)}to{transform:scale(1)}}
    @keyframes cfSlide{from{transform:translateX(-6px);opacity:0}to{transform:none;opacity:1}}
  </style>`);

  const esc = (s)=>String(s ?? "").replace(/[&<>]/g, (m)=>({ "&":"&amp;", "<":"&lt;", ">":"&gt;" }[m]));
  const providerMeta = ()=>w.CW?.ProviderMeta || {};
  const cap = (s)=>String(s || "").replace(/^./, (c)=>c.toUpperCase());
  const arrowFor = (m)=>String(m || "").toLowerCase().startsWith("two") ? "<->" : "->";
  const logoFor = (key)=>{ const meta=providerMeta(); return meta.logLogoPath?.(key) || meta.logoPath?.(key) || ""; };
  const badge = (name)=>{ const key=String(name || "").toUpperCase(), meta=providerMeta(), label=meta.label?.(key) || key, logo=logoFor(key), img=logo ? `<img src="${logo}" alt="" aria-hidden="true">` : ""; return `<span class="cf-badge">${img}${esc(label)}</span>`; };
  const head = (tag,title)=>`<span class="cf-head"><span class="cf-tag">${esc(tag)}</span><span class="cf-title">${title}</span></span>`;
  const block = (type,titleHTML,metaText="",extra="")=>`<div class="cf-event ${type} ${type==="start"?"cf-slide-in":"cf-fade-in"} ${String(extra).replace(/\b(?:cf-)?complete-shimmer\b/g,"").replace(/\s+/g," ").trim()}">${titleHTML}${metaText?`<span class="cf-sep">·</span><span class="cf-meta">${metaText}</span>`:""}</div>`;
  const summary = (ev, withPairs=true)=>{ const parts=[`+${ev.added|0} / -${ev.removed|0}`]; if(withPairs && "pairs" in ev) parts.push(`pairs=${ev.pairs|0}`); if("skipped" in ev) parts.push(`skipped=${ev.skipped|0}`); if("unresolved" in ev) parts.push(`unresolved=${ev.unresolved|0}`); if("errors" in ev) parts.push(`errors=${ev.errors|0}`); if("blocked" in ev) parts.push(`blocked=${ev.blocked|0}`); return parts.join(" · "); };

  let pendingRunId=null, pair={A:"A",B:"B"}, counts={add:{},remove:{}}, squelchPlain=0;
  const resetCounts=(a,b)=>(counts={add:{[a]:0,[b]:0},remove:{[a]:0,[b]:0}});
  const dstNameFrom=(ev)=>ev?.dst ? String(ev.dst).toUpperCase() : String(ev?.event || "").includes(":A:") ? pair.A : pair.B;

  const progMap=Object.create(null), progPendingTick=Object.create(null), progActiveKeyByBase=Object.create(null), progRunCounterByBase=Object.create(null);
  const clearObj=(obj)=>{ for(const key of Object.keys(obj)) delete obj[key]; };
  function resetState(){
    pendingRunId=null;
    pair={A:"A",B:"B"};
    counts={add:{},remove:{}};
    squelchPlain=0;
    clearObj(progMap);
    clearObj(progPendingTick);
    clearObj(progActiveKeyByBase);
    clearObj(progRunCounterByBase);
  }
  const progKey=(ev)=>{
    const name=String(ev.event || ""), dst=String(ev.dst || ev.provider || "DST").toUpperCase(), feat=String(ev.feature || "watchlist").toLowerCase();
    const base=name==="snapshot:progress" ? `snap|${dst}|${feat}` : /^apply:/.test(name) ? `apply|${dst}|${feat}|${(name.split(":")[1] || "add").toLowerCase()}` : null;
    if(!base) return null;
    const current=progActiveKeyByBase[base], row=current && progMap[current], forceNew=(name==="snapshot:progress" && row?.classList.contains("cf-prog-done") && Number(ev.done || 0)===0) || /:start$/.test(name);
    if(forceNew || !current){ const next=(progRunCounterByBase[base] || 0)+1, full=`${base}|${next}`; progRunCounterByBase[base]=next; progActiveKeyByBase[base]=full; return full; }
    return current;
  };
  const updateProgText=(txt,a,b)=>{ if(txt) txt.textContent=b>0 ? `${Math.min(a,b)}/${b} · ${Math.round((Math.min(a,b)/b)*100)}%` : `${a}`; };

  function ensureProgressRow(root,key){
    let el=progMap[key]; if(el && root.contains(el)) return el;
    const [mode,dst,feat,action]=String(key).split("|"), verb=mode==="snap" ? "Snapshot" : action==="remove" ? "Removing" : "Adding";
    el=d.createElement("div");
    el.className="cf-event progress cf-fade-in";
    el.innerHTML=`<div class="cf-prog-head"><span class="cf-tag">${mode==="snap"?"Scan":"Apply"}</span><span class="cf-title">${verb}</span><span class="cf-prog-badge">${badge(dst)}</span><span class="cf-prog-sub">· ${esc(cap(feat))}</span><div class="cf-prog-bar"><div class="cf-prog-fill"></div><div class="cf-prog-text">0%</div></div></div><div class="cf-prog-stats"></div>`;
    root.appendChild(el); progMap[key]=el; return el;
  }

  function updateProgressRow(root,ev,opts={}){
    const key=progKey(ev); if(!key) return;
    const row=ensureProgressRow(root,key), fill=row.querySelector(".cf-prog-fill"), txt=row.querySelector(".cf-prog-text"), statsEl=row.querySelector(".cf-prog-stats");
    let done=Number(ev.done || 0), total=Number(ev.total || 0) || Number(ev.count || 0) || Number(row.getAttribute("data-total") || 0);
    if(!total && "count" in ev && Number(ev.count)>0) total=Number(ev.count);
    if(total>0) row.setAttribute("data-total", String(total));
    if(total>0 && done>=total-1 && (ev.event==="snapshot:progress" || /^apply:.*:progress$/.test(String(ev.event)))){ done=total; opts.final=true; }
    if(fill) fill.style.insetInlineEnd=`${100-(total>0 ? Math.max(0, Math.min(100, Math.round((done/total)*100))) : 0)}%`;
    updateProgText(txt, done, total);
    if((total>0 && done>=total) || opts.final===true){
      row.classList.add("cf-prog-done");
      if(txt) txt.textContent=total>0 ? `${total}/${total} · 100%` : "100%";
      if((/^apply:/.test(String(ev.event || "")) || String(key).startsWith("apply|")) && statsEl){
        const action=String(ev.event || "").includes(":remove:") ? "remove" : "add", word=action==="remove" ? "removed" : "added";
        const vals={attempted:Number(ev.attempted ?? ev.result?.attempted ?? 0),confirmed:Number(ev.confirmed ?? ev.result?.confirmed ?? ev.result?.count ?? 0),skipped:Number(ev.skipped ?? ev.result?.skipped ?? 0),unresolved:Number(ev.unresolved ?? ev.result?.unresolved ?? 0),errors:Number(ev.errors ?? ev.result?.errors ?? 0)};
        statsEl.innerHTML=[{cls:"",txt:`<b>${vals.attempted}</b> attempted`},{cls:"stat-ok",txt:`<b>${vals.confirmed}</b> ${word}`},vals.skipped>0&&{cls:"stat-muted",txt:`<b>${vals.skipped}</b> skipped`},vals.unresolved>0&&{cls:"stat-warn",txt:`<b>${vals.unresolved}</b> unresolved`},vals.errors>0&&{cls:"stat-err",txt:`<b>${vals.errors}</b> errors`}].filter(Boolean).map((p)=>`<span class="cf-stat ${p.cls}">${p.txt}</span>`).join(" ");
      }
      progPendingTick[key]=false;
      return;
    }
    progPendingTick[key]=true;
  }

  function finishArmedBars(){
    for(const key in progPendingTick){
      if(!progPendingTick[key]) continue;
      const row=progMap[key], txt=row?.querySelector(".cf-prog-text"), fill=row?.querySelector(".cf-prog-fill"), total=Number(row?.getAttribute("data-total") || 0);
      if(!row){ progPendingTick[key]=false; continue; }
      if(fill) fill.style.insetInlineEnd="0%";
      row.classList.add("cf-prog-done");
      if(txt) txt.textContent=total>0 ? `${total}/${total} · 100%` : "100%";
      progPendingTick[key]=false;
    }
  }

  function formatFriendlyLog(line){
    if(!line || line[0] !== "{") return null;
    let ev; try{ ev=JSON.parse(line); }catch{ return null; }
    if(!ev?.event) return null;
    switch(ev.event){
      case "run:start": {
        const meta=[`dry_run=${!!ev.dry_run}`,`conflict=${esc(ev.conflict || "source")}`];
        if(pendingRunId){ meta.push(`run_id=${pendingRunId}`); pendingRunId=null; }
        return block("start", head("Sync","Sync started"), meta.join(" · "));
      }
      case "run:pair": {
        const i=ev.i|0, n=ev.n|0, A=String(ev.src || "").toUpperCase(), B=String(ev.dst || "").toUpperCase(), fs=Array.isArray(ev.features) ? ev.features : (ev.feature ? [ev.feature] : []);
        pair={A,B}; resetCounts(A,B);
        const meta=`${fs.length>1 ? `features=<b>${esc(fs.map((x)=>cap(String(x || ""))).join(", "))}</b>` : `feature=<b>${esc(cap(String(fs[0] || "")))}</b>`} · mode=${esc(ev.mode || "one-way")}${ev.dry_run ? " · dry_run=true" : ""}`;
        return block("pair", head(`Pair${i&&n?` ${i}/${n}`:""}`, `${badge(A)} <span class="cf-arrow">${arrowFor(ev.mode)}</span> ${badge(B)}`), meta);
      }
      case "two:start": return block("start", head("Mode","Two-way sync"), `feature=${esc(ev.feature)} · removals=${!!ev.removals}`);
      case "snapshot:progress":
      case "apply:add:start":
      case "apply:add:progress":
      case "apply:remove:start":
      case "apply:remove:progress":
        return null;
      case "one:plan": {
        const adds=ev.adds|0, removes=ev.removes|0, has=adds+removes;
        return block("plan", head("Plan", `Plan for ${esc(cap(ev.feature))}`), has ? `adding ${adds}, removing ${removes}` : "nothing to do", has ? "" : "cf-muted");
      }
      case "apply:unresolved": return block("plan", head("Check", `${ev.count|0} unresolved ${esc(cap(ev.feature))} on ${badge(ev.provider)}`), "item could not be matched", "cf-muted");
      case "two:plan": {
        const has=(ev.add_to_A|0)+(ev.add_to_B|0)+(ev.rem_from_A|0)+(ev.rem_from_B|0);
        return block("plan", head("Plan","Plan"), has ? `add A=${ev.add_to_A|0}, add B=${ev.add_to_B|0}, remove A=${ev.rem_from_A|0}, remove B=${ev.rem_from_B|0}` : "nothing to do", has ? "" : "cf-muted");
      }
      case "two:apply:add:A:done":
      case "two:apply:add:B:done":
      case "two:apply:remove:A:done":
      case "two:apply:remove:B:done": {
        const kind=ev.event.includes("add") ? "add" : "remove", who=dstNameFrom(ev), cnt=Number(ev.result?.count ?? ev.count ?? 0);
        counts[kind][who]=(counts[kind][who] || 0)+cnt;
        return null;
      }
      case "two:done": {
        const {A,B}=pair, row=(kind,a,b)=>block(kind, head(kind, cap(kind)), `${A}·${a} / ${B}·${b}`, a+b ? "" : "cf-muted");
        return [row("remove",counts.remove[A]|0,counts.remove[B]|0), row("add",counts.add[A]|0,counts.add[B]|0)].join("");
      }
      case "run:done": return block("complete", head("Done","Sync complete"), summary(ev));
      case "debug":
      default: return null;
    }
  }

  function formatWL(line){
    const t=String(line || "").trim();
    let m=t.match(/^\[WL]\s*delete\s+(\d+)\s+([A-Za-z]+)\s+'([^']*)'\s+on\s+([A-Z]+)\s*:\s*(OK|NOOP)\s*$/i);
    if(m){ const [, ,kindRaw,labelRaw,provRaw,statusRaw]=m, prov=String(provRaw).toUpperCase(), ok=String(statusRaw).toUpperCase()==="OK", safe=String(labelRaw || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/'/g,"’"); return block(ok?"remove":"plan", head("Delete", `${badge(prov)} ${esc(cap(kindRaw))} '${safe}'`), ok ? "removed" : "no change", ok ? "cf-pop" : "cf-muted"); }
    m=t.match(/^\[WL]\s*delete\s+(\d+)\s+on\s+([A-Z]+)\s*:\s*(OK|NOOP)\s*$/i);
    if(m){ const [,cnt,provRaw,statusRaw]=m, prov=String(provRaw).toUpperCase(), ok=String(statusRaw).toUpperCase()==="OK"; return block(ok?"remove":"plan", head("Delete", `${badge(prov)} Delete ${Number(cnt)||0} item(s)`), ok ? "removed" : "no change", ok ? "cf-pop" : "cf-muted"); }
    m=t.match(/^\[WL]\s*delete\s+on\s+([A-Z]+)\s+failed:\s*(.+)$/i);
    if(m){ const [,provRaw,reason]=m, prov=String(provRaw).toUpperCase(); return block("remove", head("Delete", `${badge(prov)} Delete failed`), esc(reason || "error"), "cf-muted"); }
    return null;
  }

  function filterPlainLine(line,isDebug){
    const t=String(line || "").trim(); if(!t) return null;
    const mOrch=t.match(/^>\s*SYNC start:\s*orchestrator\s+pairs\s+run_id=(\d+)/i), mRun=t.match(/^>\s*SYNC start:.*?\brun_id=(\d+)/i);
    if(mOrch){ pendingRunId=mOrch[1]; return block("start", head("Sync", `Orchestrator run ${pendingRunId}`)); }
    if(mRun){ pendingRunId=mRun[1]; return null; }
    if(!isDebug){
      if(/^sync start:\s*orchestrator/i.test(t) || /^\[i]\s*triggered sync run/i.test(t) || /^\[i]\s*orchestrator module:/i.test(t) || /^\[\d+\/\d+]\s+/i.test(t) || /^[•*-]\s*feature=/i.test(t) || /^\[SYNC]\s*exit code:/i.test(t)) return null;
      if(/^\[i]\s*providers:/i.test(t)){ squelchPlain=2; return null; }
      if(/^\[i]\s*features:/i.test(t)){ squelchPlain=3; return null; }
    }
    const mDone=t.match(/^\[i]\s*Done\.\s*Total added:\s*(\d+),\s*Total removed:\s*(\d+)(?:,\s*Total skipped:\s*(\d+))?(?:,\s*Total unresolved:\s*(\d+))?(?:,\s*Total errors:\s*(\d+))?(?:,\s*Total blocked:\s*(\d+))?/i);
    if(mDone){ const ev={added:Number(mDone[1] || 0),removed:Number(mDone[2] || 0),skipped:Number(mDone[3] || 0),unresolved:Number(mDone[4] || 0),errors:Number(mDone[5] || 0),blocked:Number(mDone[6] || 0)}; return block("complete", head("Done","Sync complete"), summary(ev,false)); }
    const mSched1=t.match(/^\s*(?:\[?INFO]?)\s*\[?SCHED]?\s*scheduler\s+(started|stopped|refreshed)\s*\((enabled|disabled)\)/i);
    if(mSched1) return block(mSched1[2].toLowerCase()==="enabled" ? "start" : "remove", head("Sched","Scheduler"), `${mSched1[1].toLowerCase()} · ${mSched1[2].toLowerCase()}`);
    if(/^\s*(?:\[?INFO]?)\s*scheduler:\s*started\s*(?:&|&amp;)\s*refreshed\s*$/i.test(t)) return block("start", head("Sched","Scheduler"), "started · refreshed");
    const mSchedThread=t.match(/^\s*(?:\[?INFO]?)\s*\[?SCHED]?\s*scheduler\s+thread\s+(started|stopped)\s*$/i);
    if(mSchedThread) return block(mSchedThread[1].toLowerCase()==="started" ? "start" : "remove", head("Sched","Scheduler"), `thread ${mSchedThread[1].toLowerCase()}`);
    const mSchedNext=t.match(/^\s*(?:\[?INFO]?)\s*\[?SCHED]?\s*next\s+run\s+scheduled\s+at\s+(.+)$/i);
    if(mSchedNext) return block("plan", head("Sched","Next run scheduled"), esc(mSchedNext[1]));
    return formatWL(t) || t;
  }

  function splitHost(s){
    return String(s).replace(/\r\n/g,"\n").replace(/(?<!\n)(>\s*SYNC start:[^\n]*)/g,"\n$1").replace(/(?<!\n)(\[\s*i\s*]\s*[^\n]*)/gi,"\n$1").replace(/(?<!\n)(\[SYNC]\s*exit code:[^\n]*)/g,"\n$1").replace(/(?<!\n)(Sync started[^\n]*)/g,"\n$1").replace(/(?<!\n)(Pair[^\n]*:\s*[^\n]*)/g,"\n$1").replace(/(?<!\n)(Plan[^\n]*)/g,"\n$1").replace(/(?<!\n)(Sync complete[^\n]*)/g,"\n$1").replace(/}\s*(?=\{")/g,"}\n").split(/\n+/);
  }

  function processChunk(buf,chunk){
    let s=(buf || "")+String(chunk || ""), tokens=[], i=0;
    const emitPlain=(piece)=>{ if(!piece) return; for(const ln of splitHost(piece)) if(ln.trim()) tokens.push(ln); };
    while(i<s.length){
      if(s[i] !== "{"){ const j=s.indexOf("{", i); if(j===-1){ emitPlain(s.slice(i)); i=s.length; break; } emitPlain(s.slice(i, j)); i=j; }
      let depth=0, inStr=false, escp=false, k=i;
      for(; k<s.length; k++){
        const ch=s[k];
        if(inStr) escp ? escp=false : ch==="\\" ? escp=true : ch===`"` && (inStr=false);
        else ch===`"` ? inStr=true : ch==="{" ? depth++ : ch==="}" && --depth===0 && (k++,1);
        if(depth===0 && !inStr && k>i && s[k-1]==="}") break;
      }
      if(depth===0 && k<=s.length){ tokens.push(s.slice(i,k)); i=k; } else break;
    }
    return {tokens,buf:s.slice(i)};
  }

  const isContinuationLine=(t)=>/^[\{\[]/.test(t) || /^['"]?[A-Za-z0-9_]+['"]?\s*:/.test(t) || /^\s{2,}\S/.test(t) || /[}\]]$/.test(t);
  const shouldDropAndMaybeSquelch=(t,isDebug)=>{
    if(isDebug) return false;
    if(/^\[i]\s*providers:/i.test(t)){ squelchPlain=2; return true; }
    if(/^\[i]\s*features:/i.test(t)){ squelchPlain=3; return true; }
    return /^>\s*SYNC start:/i.test(t) || /^\[i]\s*triggered sync run/i.test(t) || /^\[i]\s*orchestrator module:/i.test(t) || /^\[\d+\/\d+]\s+/i.test(t) || /^[•*-]\s*feature=/i.test(t) || /^\[SYNC]\s*exit code:/i.test(t);
  };

  const CF_MAX_ROWS=400;
  const trimRows=(el)=>{ const rows=el.querySelectorAll(".cf-event,.cf-line"), extra=rows.length-CF_MAX_ROWS; for(let i=0;i<extra;i++) rows[i]?.remove(); };

  function renderInto(el,line,isDebug){
    if(!el || !line) return;
    isDebug=!!(isDebug ?? (typeof window !== "undefined" && window.appDebug));
    finishArmedBars();
    const trimmed=String(line).trim();
    if(trimmed.startsWith("{")) try{
      const ev=JSON.parse(trimmed);
      if(ev?.event==="debug"){
        if(isDebug){ el.insertAdjacentHTML("beforeend", block("plan", head("Debug", esc(ev.msg || "debug")), Object.entries(ev).filter(([k])=>!["event","msg"].includes(k)).map(([k,v])=>`${k}=${v}`).join(", "), "cf-muted")); trimRows(el); return; }
        if(String(ev.msg)==="blocked.counts"){ const n=Number(ev.blocked_total || ev.total || 0); if(n>0){ el.insertAdjacentHTML("beforeend", block("plan", head("Blocked", `Blocked: ${n}`))); trimRows(el); } return; }
        if(String(ev.msg)==="blocked.manual"){ const n=Number(ev.blocked_items ?? ev.blocked_keys ?? (Array.isArray(ev.blocked_keys) ? ev.blocked_keys.length : 0) ?? 0); if(n>0){ const meta=[ev.feature ? `feature=${esc(cap(ev.feature))}` : "", ev.pair ? `pair=${esc(ev.pair)}` : ""].filter(Boolean).join(" · "); el.insertAdjacentHTML("beforeend", block("plan", head("Blocked", `Blocked: ${n}`), meta || "manual", "cf-pop")); trimRows(el); } return; }
        return;
      }
      if(ev?.event==="snapshot:progress"){ updateProgressRow(el, ev, {final:ev.final===true}); trimRows(el); return; }
      if(/^apply:/.test(ev?.event || "")){
        if(/:start$/.test(ev.event)){ updateProgressRow(el, {...ev, done:0, total:ev.count || ev.total || 0}); trimRows(el); return; }
        if(/:progress$/.test(ev.event)){ updateProgressRow(el, ev); trimRows(el); return; }
        if(/:done$/.test(ev.event)){ const key=progKey(ev), row=key && progMap[key], tot=Number(ev.result?.count ?? ev.count ?? row?.getAttribute("data-total") ?? 0); updateProgressRow(el, {...ev, done:tot, total:tot}, {final:true}); trimRows(el); return; }
      }
    }catch{}

    if(isDebug){ const div=d.createElement("div"); div.className="cf-line"; div.textContent=String(line); el.appendChild(div); trimRows(el); return; }
    const fancy=formatFriendlyLog(line);
    if(fancy != null){ el.insertAdjacentHTML("beforeend", fancy); trimRows(el); return; }
    if(trimmed.startsWith("{")) return;
    if(!trimmed) return;
    if(squelchPlain>0 && isContinuationLine(trimmed)){ squelchPlain--; return; }
    if(squelchPlain>0 && !isContinuationLine(trimmed)) squelchPlain=0;
    if(shouldDropAndMaybeSquelch(trimmed,false)) return;
    const out=filterPlainLine(trimmed,false); if(!out) return;
    if(/^<.+>/.test(out)) el.insertAdjacentHTML("beforeend", out);
    else { const div=d.createElement("div"); div.className="cf-line cf-fade-in"; div.textContent=out; el.appendChild(div); }
    trimRows(el);
  }

  w.ClientFormatter={formatFriendlyLog,filterPlainLine,splitHost,processChunk,renderInto,reset:resetState};
})(window, document);
