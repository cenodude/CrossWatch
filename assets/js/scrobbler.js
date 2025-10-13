// Scrobbler UI
(function (w, d) {
  const $=(s,r)=> (r||d).querySelector(s),
        $all=(s,r)=>[...(r||d).querySelectorAll(s)],
        el=(t,a)=>Object.assign(d.createElement(t),a||{}),
        on=(n,e,f)=>n&&n.addEventListener(e,f);
  const j=async(u,o)=>{const r=await fetch(u,{cache:"no-store",...(o||{})});if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json();};
  const t=async(u,o)=>{const r=await fetch(u,{cache:"no-store",...(o||{})});if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.text();};

  function setNote(id,msg,kind){
    const n=d.getElementById(id); if(!n) return;
    n.textContent=msg||""; n.style.cssText="margin:6px 0 2px;font-size:12px;opacity:.9;color:"+(kind==="err"?"#ff6b6b":"var(--muted,#a7a7a7)");
  }

  function injectStyles(){
    if(d.getElementById("sc-styles")) return;
    const s=el("style",{id:"sc-styles"});
    s.textContent=`
    .row{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
    .codepair{display:flex;gap:8px;align-items:center}
    .codepair code{padding:6px 8px;border-radius:8px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}
    .badge{padding:4px 10px;border-radius:999px;font-weight:600;opacity:.9}.badge.is-on{background:#0a3;color:#fff}.badge.is-off{background:#333;color:#bbb;border:1px solid #444}
    .status-dot{width:10px;height:10px;border-radius:50%}.status-dot.on{background:#22c55e}.status-dot.off{background:#ef4444}
    .watcher-row{display:grid;grid-template-columns:1fr 1fr;gap:16px}
    .chips{display:flex;flex-wrap:wrap;gap:6px}.chip{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08)}.chip .rm{cursor:pointer;opacity:.7}
    .sc-filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .sc-adv-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;align-items:end}
    .field{display:flex;gap:6px;align-items:center;position:relative}.field label{white-space:nowrap;font-size:12px;opacity:.8}.field input{width:100%}
    details.sc-filters,details.sc-advanced{display:block;margin-top:12px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset}
    details.sc-filters>summary,details.sc-advanced>summary{cursor:pointer;list-style:none;padding:14px;border-radius:12px;font-weight:600}
    details.sc-filters[open]>summary,details.sc-advanced[open]>summary{border-bottom:1px solid rgba(255,255,255,.06)}
    details.sc-filters .body,details.sc-advanced .body{padding:12px 14px}`;
    d.head.appendChild(s);
  }

  const DEFAULTS={watch:{pause_debounce_seconds:5,suppress_start_at:99},trakt:{stop_pause_threshold:80,force_stop_at:95,regress_tolerance_percent:5}};
  const STATE={mount:null,webhookHost:null,watcherHost:null,cfg:{},users:[],pms:[]};

  const deepSet=(o,p,v)=>p.split(".").reduce((a,k,i,arr)=>(i===arr.length-1?(a[k]=v):((a[k]&&typeof a[k]==="object")||(a[k]={})),a[k]),o);
  const read=(p,dflt)=>p.split(".").reduce((v,k)=>v&&typeof v==="object"?v[k]:undefined,STATE.cfg) ?? dflt;
  function write(p,v){ deepSet(STATE.cfg,p,v); try{ w._cfgCache ||= {}; deepSet(w._cfgCache,p,v);}catch{} try{syncHiddenServerUrl();}catch{} }

  const asArray=v=>Array.isArray(v)?v.slice():v==null||v===""?[]:[String(v)];
  const clamp100=n=>Math.min(100,Math.max(1,Math.round(Number(n))));
  const norm100=(n,dflt)=>clamp100(Number.isFinite(+n)?+n:dflt);

  const API={
    cfgGet:()=>j("/api/config"),
    users:async()=>{const x=await j("/api/plex/users");const a=Array.isArray(x)?x:Array.isArray(x?.users)?x.users:[];return Array.isArray(a)?a:[];},
    serverUUID:()=>j("/api/plex/server_uuid"),
    pms:async()=>{const x=await j("/api/plex/pms");const a=Array.isArray(x)?x:Array.isArray(x?.servers)?x.servers:[];return Array.isArray(a)?a:[];},
    watch:{ status:()=>j("/debug/watch/status"), start:()=>t("/debug/watch/start",{method:"POST"}), stop:()=>t("/debug/watch/stop",{method:"POST"}) }
  };

  function chip(label,onRemove){
    const c=el("span",{className:"chip"}),t=el("span",{textContent:label}),rm=el("span",{className:"rm",title:"Remove",textContent:"×"});
    on(rm,"click",()=>onRemove&&onRemove(label)); c.append(t,rm); return c;
  }

  function setWatcherStatus(ui){
    const alive=!!ui?.alive,q=id=>$(id,STATE.mount),dot=q("#sc-status-dot"),txt=q("#sc-status-text"),badge=q("#sc-status-badge"),last=q("#sc-status-last"),up=q("#sc-status-up");
    if(dot){ dot.classList.toggle("on",alive); dot.classList.toggle("off",!alive); }
    if(txt) txt.textContent=alive?"Active":"Inactive";
    if(badge){ badge.textContent=alive?"Active":"Stopped"; badge.classList.toggle("is-on",alive); badge.classList.toggle("is-off",!alive); }
    if(last) last.textContent=ui?.lastSeen?`Last seen: ${ui.lastSeen}`:"";
    if(up)   up.textContent=ui?.uptime?`Uptime: ${ui.uptime}`:"";
  }

  function isValidServerUrl(v){ if(!v) return false; try{const u=new URL(v);return (u.protocol==="http:"||u.protocol==="https:")&&!!u.host;}catch{return false;} }
  function applyModeDisable(){
    const wh=$("#sc-enable-webhook",STATE.mount),wa=$("#sc-enable-watcher",STATE.mount),webhookOn=!!wh?.checked,watcherOn=!!wa?.checked;
    write("scrobble.enabled",webhookOn||watcherOn); write("scrobble.mode",watcherOn?"watch":"webhook");
    const webRoot=$("#sc-sec-webhook",STATE.mount)||STATE.mount,watchRoot=$("#sc-sec-watch",STATE.mount)||STATE.mount;
    $all(".input, input, button, select, textarea",webRoot).forEach(n=>{if(n.id!=="sc-enable-webhook") n.disabled=!webhookOn;});
    $all(".input, input, button, select, textarea",watchRoot).forEach(n=>{if(n.id!=="sc-enable-watcher") n.disabled=!watcherOn;});
    const srv=String(read("plex.server_url","")||"");
    if(watcherOn){ isValidServerUrl(srv)?setNote("sc-pms-note",`Using ${srv}`):setNote("sc-pms-note","Plex Server is required (http(s)://…)","err"); } else setNote("sc-pms-note","");
  }

  function buildUI(){
    injectStyles();
    if(STATE.webhookHost){
      STATE.webhookHost.innerHTML=`
        <div class="row" style="margin-bottom:8px">
          <label style="display:inline-flex;gap:8px;align-items:center"><input type="checkbox" id="sc-enable-webhook"> Enable</label>
          <div class="codepair"><code id="sc-webhook-url-plex"></code><button id="sc-copy-plex" class="btn small">Copy</button></div>
          <div class="codepair"><code id="sc-webhook-url-jf"></code><button id="sc-copy-jf" class="btn small">Copy</button></div>
        </div>
        <div id="sc-endpoint-note" class="micro-note"></div>
        <details id="sc-filters-webhook" class="sc-filters"><summary>Filters (Plex only)</summary>
          <div class="body">
            <div class="sc-filter-grid">
              <div>
                <div class="muted">Username whitelist</div>
                <div id="sc-whitelist-webhook" class="chips" style="margin-top:4px"></div>
                <div id="sc-users-note-webhook" class="micro-note"></div>
                <div style="display:flex;gap:8px;margin-top:6px">
                  <input id="sc-user-input-webhook" class="input" placeholder="Add username..." style="flex:1">
                  <button id="sc-add-user-webhook" class="btn small">Add</button>
                  <button id="sc-load-users-webhook" class="btn small">Load Plex users</button>
                </div>
              </div>
              <div>
                <div class="muted">Server UUID</div>
                <div id="sc-uuid-note-webhook" class="micro-note"></div>
                <div style="display:flex;gap:8px;align-items:center;margin-top:6px">
                  <input id="sc-server-uuid-webhook" class="input" placeholder="e.g. abcd1234..." style="flex:1">
                  <button id="sc-fetch-uuid-webhook" class="btn small">Fetch</button>
                </div>
              </div>
            </div>
          </div>
        </details>
        <details class="sc-advanced" id="sc-advanced-webhook"><summary>Advanced</summary>
          <div class="body">
            <div class="sc-adv-grid">
              <div class="field" data-tip="Per-session PAUSE debounce; quick double PAUSEs are ignored."><label for="sc-pause-debounce-webhook">Pause debounce</label><input id="sc-pause-debounce-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.pause_debounce_seconds}"></div>
              <div class="field" data-tip="Suppress end-credits START when progress ≥ threshold."><label for="sc-suppress-start-webhook">Suppress start @</label><input id="sc-suppress-start-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.suppress_start_at}"></div>
              <div class="field" data-tip="Allow small regressions; avoids rollbacks and decides session vs new progress update."><label for="sc-regress-webhook">Regress tol %</label><input id="sc-regress-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.regress_tolerance_percent}"></div>
              <div class="field" data-tip="STOP below threshold is sent as PAUSE; also downgrades suspicious 100% jumps (STOP→PAUSE)."><label for="sc-stop-pause-webhook">Stop pause ≥</label><input id="sc-stop-pause-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.stop_pause_threshold}"></div>
              <div class="field" data-tip="Debounce bypass: a final STOP (≥ threshold) always goes through."><label for="sc-force-stop-webhook">Force stop @</label><input id="sc-force-stop-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.force_stop_at}"></div>
            </div>
            <div class="micro-note" style="margin-top:6px">Empty resets to defaults. Values are 1–100.</div>
          </div>
        </details>`;
    }
    if(STATE.watcherHost){
      STATE.watcherHost.innerHTML=`
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px"><label style="display:inline-flex;gap:8px;align-items:center"><input type="checkbox" id="sc-enable-watcher"> Enable</label></div>
        <div class="watcher-row">
          <div class="card" id="sc-card-server" style="padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset;">
            <div class="h" style="display:flex;justify-content:space-between;align-items:center"><div>Plex Server <span class="pill req">required</span></div><button id="sc-pms-refresh" class="btn small">Fetch</button></div>
            <div id="sc-pms-note" class="micro-note" style="margin-top:6px"></div>
            <div style="margin-top:8px"><div class="muted">Discovered servers</div><select id="sc-pms-select" class="input" style="width:100%;margin-top:6px"><option value="">— select a server —</option></select></div>
            <div style="margin-top:12px"><div class="muted">Manual URL (http(s)://host[:port])</div><input id="sc-pms-input" class="input" placeholder="https://192.168.1.10:32400" /></div>
          </div>
          <div class="card" id="sc-card-status" style="padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset;">
            <div class="h" style="display:flex;justify-content:space-between;align-items:center"><div>Watcher Status</div><span id="sc-status-badge" class="badge is-off">Stopped</span></div>
            <div class="row" style="margin-top:10px">
              <div class="codepair"><span id="sc-status-dot" class="status-dot off"></span><span class="muted">Status:</span><span id="sc-status-text" class="status-text">Unknown</span></div>
              <div style="display:flex;gap:8px;margin-left:auto"><button id="sc-watch-start" class="btn small">Start</button><button id="sc-watch-stop" class="btn small">Stop</button><button id="sc-watch-refresh" class="btn small">Refresh</button></div>
              <label style="margin-left:auto" class="sc-toggle"><input type="checkbox" id="sc-autostart"> Autostart on boot</label>
            </div>
            <div id="sc-status-last" class="micro-note" style="margin-top:8px"></div>
            <div id="sc-status-up" class="micro-note"></div>
          </div>
        </div>
        <details id="sc-filters" class="sc-filters"><summary>Filters (optional)</summary>
          <div class="body">
            <div class="sc-filter-grid">
              <div>
                <div class="muted">Username whitelist</div>
                <div id="sc-whitelist" class="chips" style="margin-top:4px"></div>
                <div id="sc-users-note" class="micro-note"></div>
                <div style="display:flex; gap:8px; margin-top:6px">
                  <input id="sc-user-input" class="input" placeholder="Add username..." style="flex:1">
                  <button id="sc-add-user" class="btn small">Add</button>
                  <button id="sc-load-users" class="btn small">Load Plex users</button>
                </div>
              </div>
              <div>
                <div class="muted">Server UUID</div>
                <div id="sc-uuid-note" class="micro-note"></div>
                <div style="display:flex; gap:8px; align-items:center; margin-top:6px">
                  <input id="sc-server-uuid" class="input" placeholder="e.g. abcd1234..." style="flex:1">
                  <button id="sc-fetch-uuid" class="btn small">Fetch</button>
                </div>
              </div>
            </div>
          </div>
        </details>
        <details class="sc-advanced" id="sc-advanced"><summary>Advanced</summary>
          <div class="body">
            <div class="sc-adv-grid">
              <div class="field" data-tip="Per-session PAUSE debounce; quick double PAUSEs are ignored."><label for="sc-pause-debounce">Pause debounce</label><input id="sc-pause-debounce" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.pause_debounce_seconds}"></div>
              <div class="field" data-tip="Suppress end-credits START when progress ≥ threshold."><label for="sc-suppress-start">Suppress start @</label><input id="sc-suppress-start" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.suppress_start_at}"></div>
              <div class="field" data-tip="Allow small regressions; avoids rollbacks and decides session vs new progress update."><label for="sc-regress">Regress tol %</label><input id="sc-regress" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.regress_tolerance_percent}"></div>
              <div class="field" data-tip="STOP below threshold is sent as PAUSE; also downgrades suspicious 100% jumps (STOP→PAUSE)."><label for="sc-stop-pause">Stop pause ≥</label><input id="sc-stop-pause" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.stop_pause_threshold}"></div>
              <div class="field" data-tip="Debounce bypass: a final STOP (≥ threshold) always goes through."><label for="sc-force-stop">Force stop @</label><input id="sc-force-stop" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.force_stop_at}"></div>
            </div>
            <div class="micro-note" style="margin-top:6px">Empty resets to defaults. Values are 1–100.</div>
          </div>
        </details>`;
    }
  }

  function ensureHiddenServerUrlInput(){
    let hidden=d.getElementById("cfg-plex-server-url");
    const form=d.querySelector("form#settings, form#settings-form, form[data-settings]") || (STATE.mount||d.body);
    if(!hidden){ hidden=el("input",{type:"hidden",id:"cfg-plex-server-url",name:"plex.server_url"}); form.appendChild(hidden); }
    syncHiddenServerUrl();
  }
  function syncHiddenServerUrl(){ const h=d.getElementById("cfg-plex-server-url"); if(h) h.value=String(read("plex.server_url","")||""); }

  function restoreDetailsState(sel,def,key){
    const n=$(sel,STATE.mount); if(!n) return;
    let open=def; try{const v=localStorage.getItem(key); if(v!=null) open=(v==="1");}catch{}
    n.open=!!open;
    on(n,"toggle",()=>{try{localStorage.setItem(key,n.open?"1":"0");}catch{}});
  }

  const readNum=(sel,dflt)=>{const raw=String($(sel,STATE.mount)?.value??"").trim();return raw===""?clamp100(dflt):norm100(raw,dflt);};

  // Copy helper (HTTPS + HTTP fallback)
  async function copyText(s){
    try{ await navigator.clipboard.writeText(s); return true; }catch{
      try{
        const ta=el("textarea",{style:"position:fixed;left:-9999px;top:-9999px"}); ta.value=s; d.body.appendChild(ta); ta.select();
        const ok=d.execCommand?d.execCommand("copy"):document.execCommand("copy"); d.body.removeChild(ta); return !!ok;
      }catch{ return false; }
    }
  }

  // Watcher advanced
  function commitAdvancedInputsWatch(){
    write("scrobble.watch.pause_debounce_seconds",readNum("#sc-pause-debounce",DEFAULTS.watch.pause_debounce_seconds));
    write("scrobble.watch.suppress_start_at",readNum("#sc-suppress-start",DEFAULTS.watch.suppress_start_at));
  }
  // Webhook advanced
  function commitAdvancedInputsWebhook(){
    write("scrobble.webhook.pause_debounce_seconds",readNum("#sc-pause-debounce-webhook",DEFAULTS.watch.pause_debounce_seconds));
    write("scrobble.webhook.suppress_start_at",readNum("#sc-suppress-start-webhook",DEFAULTS.watch.suppress_start_at));
  }
  // Trakt (shared)
  function commitAdvancedInputsTrakt(){
    const keys=[["#sc-stop-pause", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
                ["#sc-force-stop","scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at],
                ["#sc-regress","scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent],
                ["#sc-stop-pause-webhook", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
                ["#sc-force-stop-webhook","scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at],
                ["#sc-regress-webhook","scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent]];
    for(const [sel,path,dflt] of keys){ const v=readNum(sel,dflt); write(path,v); }
  }
  function bindPercentInput(sel,path,dflt){
    const n=$(sel,STATE.mount); if(!n) return;
    const set=(val,commitEmpty=false)=>{const raw=String(val ?? n.value ?? "").trim(); if(raw===""){ if(commitEmpty){const v=clamp100(dflt);write(path,v);n.value=v;} return;} const v=norm100(raw,dflt); write(path,v); n.value=v;};
    on(n,"input",()=>set(n.value,false)); on(n,"change",()=>set(n.value,true)); on(n,"blur",()=>set(n.value,true));
  }

  function namesFromChips(hostId){
    const host=$(hostId,STATE.mount); if(!host) return [];
    return $all(".chip > span:first-child",host).map(s=>String(s.textContent||"").trim()).filter(Boolean);
  }

  function populate(){
    const enabled=!!read("scrobble.enabled",false),mode=String(read("scrobble.mode","webhook")).toLowerCase();
    const useWebhook=enabled&&mode==="webhook",useWatch=enabled&&mode==="watch";

    const whEl=$("#sc-enable-webhook",STATE.mount),waEl=$("#sc-enable-watcher",STATE.mount);
    if(whEl) whEl.checked=useWebhook; if(waEl) waEl.checked=useWatch;

    // Watcher filters
    const wlWatch=asArray(read("scrobble.watch.filters.username_whitelist",[]));
    const hostW=$("#sc-whitelist",STATE.mount); if(hostW){ hostW.innerHTML=""; wlWatch.forEach(u=>hostW.append(chip(u,removeUserWatch))); }
    const suWatch=read("scrobble.watch.filters.server_uuid",""),suInpW=$("#sc-server-uuid",STATE.mount); if(suInpW) suInpW.value=suWatch||"";

    // Webhook (Plex-only) filters
    const wlWeb=asArray(read("scrobble.webhook.filters_plex.username_whitelist",[]));
    const hostWB=$("#sc-whitelist-webhook",STATE.mount); if(hostWB){ hostWB.innerHTML=""; wlWeb.forEach(u=>hostWB.append(chip(u,removeUserWebhook))); }
    const suWeb=read("scrobble.webhook.filters_plex.server_uuid",""),suInpWB=$("#sc-server-uuid-webhook",STATE.mount); if(suInpWB) suInpWB.value=suWeb||"";

    // Endpoints
    const base=location.origin;
    const plexCode=$("#sc-webhook-url-plex",STATE.mount),jfCode=$("#sc-webhook-url-jf",STATE.mount);
    if(plexCode) plexCode.textContent=`${base}/webhook/plextrakt`;
    if(jfCode)   jfCode.textContent=`${base}/webhook/jellyfintrakt`;

    // Misc
    const autostart=!!read("scrobble.watch.autostart",false),serverUrl=String(read("plex.server_url","")||"");
    const auto=$("#sc-autostart",STATE.mount); if(auto) auto.checked=!!autostart;
    const pmsInp=$("#sc-pms-input",STATE.mount); if(pmsInp) pmsInp.value=serverUrl;

    const set=(id,v)=>{const n=$(id,STATE.mount); if(n) n.value=norm100(v,v);};
    set("#sc-pause-debounce",read("scrobble.watch.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds));
    set("#sc-suppress-start",read("scrobble.watch.suppress_start_at",DEFAULTS.watch.suppress_start_at));
    set("#sc-pause-debounce-webhook",read("scrobble.webhook.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds));
    set("#sc-suppress-start-webhook",read("scrobble.webhook.suppress_start_at",DEFAULTS.watch.suppress_start_at));
    set("#sc-stop-pause",read("scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold));
    set("#sc-force-stop",read("scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at));
    set("#sc-regress",read("scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent));
    set("#sc-stop-pause-webhook",read("scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold));
    set("#sc-force-stop-webhook",read("scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at));
    set("#sc-regress-webhook",read("scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent));

    // Default closed
    restoreDetailsState("#sc-filters",false,"sc-filters-open");
    restoreDetailsState("#sc-advanced",false,"sc-advanced-open");
    restoreDetailsState("#sc-filters-webhook",false,"sc-filters-webhook-open");
    restoreDetailsState("#sc-advanced-webhook",false,"sc-advanced-webhook-open");

    syncHiddenServerUrl(); applyModeDisable();
  }

  async function refreshWatcher(){ try{ setWatcherStatus(await API.watch.status()||{});}catch{ setWatcherStatus({alive:false}); } }
  async function onWatchStart(){
    const srv=String(read("plex.server_url","")||""); if(!isValidServerUrl(srv)) return setNote("sc-pms-note","Plex Server is required (http(s)://…)","err");
    try{ await API.watch.start(); }catch{ setNote("sc-pms-note","Start failed","err"); } refreshWatcher();
  }
  async function onWatchStop(){ try{ await API.watch.stop(); }catch{ setNote("sc-pms-note","Stop failed","err"); } refreshWatcher(); }

  // Watcher ops
  async function fetchServerUUID(){
    try{
      const x=await API.serverUUID(),v=x?.server_uuid||x?.uuid||x?.id||"",inp=$("#sc-server-uuid",STATE.mount);
      if(inp&&v){ inp.value=v; write("scrobble.watch.filters.server_uuid",v); setNote("sc-uuid-note","Server UUID fetched"); } else setNote("sc-uuid-note","No server UUID","err");
    }catch{ setNote("sc-uuid-note","Fetch failed","err"); }
  }
  async function loadPmsList(){
    try{
      const sel=$("#sc-pms-select",STATE.mount); if(!sel) return;
      sel.innerHTML=`<option value="">Loading…</option>`;
      const list=await API.pms(); STATE.pms=list; sel.innerHTML=`<option value="">— select a server —</option>`;
      for(const s of list){ const best=s.best_url||"",nm=s.name||s.product||"Plex Media Server",owned=s.owned?" (owned)":""; sel.append(el("option",{value:best||"",textContent:best?`${nm}${owned} — ${best}`:nm+owned})); }
      setNote("sc-pms-note",list.length?"Pick a discovered server or enter a URL":"No servers discovered. Enter a URL.",list.length?null:"err");
    }catch{
      const sel=$("#sc-pms-select",STATE.mount); if(sel) sel.innerHTML=`<option value="">— select a server —</option>`;
      setNote("sc-pms-note","Fetch failed. Enter a URL manually.","err");
    }
  }
  function onAddUserWatch(){
    const inp=$("#sc-user-input",STATE.mount),v=String((inp?.value||"").trim()); if(!v) return;
    const cur=asArray(read("scrobble.watch.filters.username_whitelist",[])); if(!cur.includes(v)){ const next=[...cur,v]; write("scrobble.watch.filters.username_whitelist",next); $("#sc-whitelist",STATE.mount).append(chip(v,removeUserWatch)); inp.value=""; }
  }
  function removeUserWatch(u){
    const cur=asArray(read("scrobble.watch.filters.username_whitelist",[])),next=cur.filter(x=>String(x)!==String(u));
    write("scrobble.watch.filters.username_whitelist",next);
    const host=$("#sc-whitelist",STATE.mount); host.innerHTML=""; next.forEach(v=>host.append(chip(v,removeUserWatch)));
  }
  async function loadUsers(){
    try{
      const list=await API.users(),filtered=list.filter(u=>["managed","owner"].includes(String(u?.type||"").toLowerCase())||u?.owned===true||u?.isHomeUser===true);
      const names=filtered.map(u=>u?.username||u?.title).filter(Boolean),host=$("#sc-whitelist",STATE.mount); let added=0;
      for(const n of names){ const cur=asArray(read("scrobble.watch.filters.username_whitelist",[])); if(!cur.includes(n)){ write("scrobble.watch.filters.username_whitelist",[...cur,n]); host.append(chip(n,removeUserWatch)); added++; } }
      setNote("sc-users-note",added?`Loaded ${added} user(s)`:"No eligible managed/owner users");
    }catch{ setNote("sc-users-note","Load users failed","err"); }
  }

  // Webhook ops (Plex-only filters)
  async function fetchServerUUIDWebhook(){
    try{
      const x=await API.serverUUID(),v=x?.server_uuid||x?.uuid||x?.id||"",inp=$("#sc-server-uuid-webhook",STATE.mount);
      if(inp&&v){ inp.value=v; write("scrobble.webhook.filters_plex.server_uuid",v); setNote("sc-uuid-note-webhook","Server UUID fetched"); } else setNote("sc-uuid-note-webhook","No server UUID","err");
    }catch{ setNote("sc-uuid-note-webhook","Fetch failed","err"); }
  }
  function onAddUserWebhook(){
    const inp=$("#sc-user-input-webhook",STATE.mount),v=String((inp?.value||"").trim()); if(!v) return;
    const cur=asArray(read("scrobble.webhook.filters_plex.username_whitelist",[])); if(!cur.includes(v)){ const next=[...cur,v]; write("scrobble.webhook.filters_plex.username_whitelist",next); $("#sc-whitelist-webhook",STATE.mount).append(chip(v,removeUserWebhook)); inp.value=""; }
  }
  function removeUserWebhook(u){
    const cur=asArray(read("scrobble.webhook.filters_plex.username_whitelist",[])),next=cur.filter(x=>String(x)!==String(u));
    write("scrobble.webhook.filters_plex.username_whitelist",next);
    const host=$("#sc-whitelist-webhook",STATE.mount); host.innerHTML=""; next.forEach(v=>host.append(chip(v,removeUserWebhook)));
  }
  async function loadUsersWebhook(){
    try{
      const list=await API.users(),filtered=list.filter(u=>["managed","owner"].includes(String(u?.type||"").toLowerCase())||u?.owned===true||u?.isHomeUser===true);
      const names=filtered.map(u=>u?.username||u?.title).filter(Boolean),host=$("#sc-whitelist-webhook",STATE.mount); let added=0;
      for(const n of names){ const cur=asArray(read("scrobble.webhook.filters_plex.username_whitelist",[])); if(!cur.includes(n)){ write("scrobble.webhook.filters_plex.username_whitelist",[...cur,n]); host.append(chip(n,removeUserWebhook)); added++; } }
      setNote("sc-users-note-webhook",added?`Loaded ${added} user(s)`:"No eligible managed/owner users");
    }catch{ setNote("sc-users-note-webhook","Load users failed","err"); }
  }

  function wire(){
    ensureHiddenServerUrlInput();

    // Copy buttons
    on($("#sc-copy-plex",STATE.mount),"click",async()=>{ const ok=await copyText(`${location.origin}/webhook/plextrakt`); setNote("sc-endpoint-note",ok?"Plex endpoint copied":"Copy failed",ok?"":"err"); });
    on($("#sc-copy-jf",STATE.mount),"click",async()=>{ const ok=await copyText(`${location.origin}/webhook/jellyfintrakt`); setNote("sc-endpoint-note",ok?"Jellyfin endpoint copied":"Copy failed",ok?"":"err"); });

    // Watcher wiring
    on($("#sc-add-user",STATE.mount),"click",onAddUserWatch);
    on($("#sc-load-users",STATE.mount),"click",loadUsers);
    on($("#sc-watch-start",STATE.mount),"click",onWatchStart);
    on($("#sc-watch-stop",STATE.mount),"click",onWatchStop);
    on($("#sc-watch-refresh",STATE.mount),"click",()=>{refreshWatcher(); try{ w.refreshWatchLogs?.(); }catch{}});
    on($("#sc-fetch-uuid",STATE.mount),"click",fetchServerUUID);
    on($("#sc-server-uuid",STATE.mount),"input",e=>write("scrobble.watch.filters.server_uuid",String(e.target.value||"").trim()));

    // Webhook wiring
    on($("#sc-add-user-webhook",STATE.mount),"click",onAddUserWebhook);
    on($("#sc-load-users-webhook",STATE.mount),"click",loadUsersWebhook);
    on($("#sc-fetch-uuid-webhook",STATE.mount),"click",fetchServerUUIDWebhook);
    on($("#sc-server-uuid-webhook",STATE.mount),"input",e=>write("scrobble.webhook.filters_plex.server_uuid",String(e.target.value||"").trim()));

    // Percent binds
    bindPercentInput("#sc-pause-debounce","scrobble.watch.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start","scrobble.watch.suppress_start_at",DEFAULTS.watch.suppress_start_at);
    bindPercentInput("#sc-pause-debounce-webhook","scrobble.webhook.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start-webhook","scrobble.webhook.suppress_start_at",DEFAULTS.watch.suppress_start_at);
    bindPercentInput("#sc-stop-pause","scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold);
    bindPercentInput("#sc-force-stop","scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress","scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent);
    bindPercentInput("#sc-stop-pause-webhook","scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold);
    bindPercentInput("#sc-force-stop-webhook","scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress-webhook","scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent);

    // Mode + plex url
    const wh=$("#sc-enable-webhook",STATE.mount),wa=$("#sc-enable-watcher",STATE.mount);
    const syncExclusive=src=>{const webOn=!!wh?.checked,watOn=!!wa?.checked; if(src==="webhook"&&webOn&&wa) wa.checked=false; if(src==="watch"&&watOn&&wh) wh.checked=false; write("scrobble.enabled",(!!wh?.checked)|| (!!wa?.checked)); write("scrobble.mode",(!!wa?.checked)?"watch":"webhook"); applyModeDisable();};
    if(wh) on(wh,"change",()=>syncExclusive("webhook"));
    if(wa) on(wa,"change",()=>syncExclusive("watch"));
    on($("#sc-autostart",STATE.mount),"change",e=>write("scrobble.watch.autostart",!!e.target.checked));

    on($("#sc-pms-refresh",STATE.mount),"click",loadPmsList);
    on($("#sc-pms-select",STATE.mount),"change",e=>{const v=String(e.target.value||"").trim(); if(v){ $("#sc-pms-input",STATE.mount).value=v; write("plex.server_url",v); setNote("sc-pms-note",`Using ${v}`);} applyModeDisable();});
    on($("#sc-pms-input",STATE.mount),"input",e=>{const v=String(e.target.value||"").trim(); write("plex.server_url",v); if(v&&!isValidServerUrl(v)) setNote("sc-pms-note","Invalid URL. Use http(s)://host[:port]","err"); else if(v) setNote("sc-pms-note",`Using ${v}`); else setNote("sc-pms-note","Plex Server is required when Watcher is enabled","err"); applyModeDisable();});
  }

  function init(opts={}){
    STATE.mount = opts.mountId ? d.getElementById(opts.mountId) : d;
    STATE.cfg   = opts.cfg || w._cfgCache || {};
    STATE.webhookHost = $("#scrob-webhook",STATE.mount);
    STATE.watcherHost = $("#scrob-watcher",STATE.mount);
    if(!STATE.webhookHost || !STATE.watcherHost){
      const root=STATE.mount||d.body,makeSec=(id,title)=>{const sec=el("div",{className:"section",id}); sec.innerHTML=`<div class="head"><strong>${title}</strong></div><div class="body"><div id="${id==="sc-sec-webhook"?"scrob-webhook":"scrob-watcher"}"></div></div>`; root.append(sec);};
      if(!STATE.webhookHost){ makeSec("sc-sec-webhook","Webhook"); STATE.webhookHost=$("#scrob-webhook",STATE.mount); }
      if(!STATE.watcherHost){ makeSec("sc-sec-watch","Watcher"); STATE.watcherHost=$("#scrob-watcher",STATE.mount); }
    }
    buildUI(); wire(); populate(); refreshWatcher(); loadPmsList().catch(()=>{});
  }
  function mountLegacy(targetEl,cfg){ init({ mountId: targetEl?.id, cfg: cfg||(w._cfgCache||{}) }); }

  function getScrobbleConfig(){
    // build from DOM → avoids “only saves when another field changed”
    commitAdvancedInputsWatch();
    commitAdvancedInputsWebhook();
    commitAdvancedInputsTrakt();

    const enabled=!!read("scrobble.enabled",false),mode=String(read("scrobble.mode","webhook")).toLowerCase();

    // Webhook filters from DOM (fallback to STATE)
    const wlWeb = namesFromChips("#sc-whitelist-webhook");
    const suWeb = String($("#sc-server-uuid-webhook",STATE.mount)?.value ?? read("scrobble.webhook.filters_plex.server_uuid","")).trim();

    // Watcher filters from DOM (fallback to STATE)
    const wlWatch = namesFromChips("#sc-whitelist");
    const suWatch = String($("#sc-server-uuid",STATE.mount)?.value ?? read("scrobble.watch.filters.server_uuid","")).trim();

    return {
      enabled,
      mode: mode==="watch"?"watch":"webhook",
      webhook:{
        pause_debounce_seconds:read("scrobble.webhook.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds),
        suppress_start_at:read("scrobble.webhook.suppress_start_at",DEFAULTS.watch.suppress_start_at),
        filters_plex:{ username_whitelist: wlWeb.length?wlWeb:asArray(read("scrobble.webhook.filters_plex.username_whitelist",[])), server_uuid: suWeb||"" },
        filters_jellyfin: read("scrobble.webhook.filters_jellyfin",{}) || { username_whitelist: [] }
      },
      watch:{
        autostart:!!read("scrobble.watch.autostart",false),
        pause_debounce_seconds:read("scrobble.watch.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds),
        suppress_start_at:read("scrobble.watch.suppress_start_at",DEFAULTS.watch.suppress_start_at),
        filters:{ username_whitelist: wlWatch.length?wlWatch:asArray(read("scrobble.watch.filters.username_whitelist",[])), server_uuid: suWatch||"" }
      },
      trakt:{
        stop_pause_threshold:read("scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold),
        force_stop_at:read("scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at),
        regress_tolerance_percent:read("scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent)
      }
    };
  }

  const getRootPatch=()=>({ plex:{ server_url:String(read("plex.server_url","")||"") } });

  w.ScrobUI={ $, $all, el, on, setNote, injectStyles, DEFAULTS, STATE, read, write, asArray, clamp100, norm100, API };
  w.Scrobbler={ init, mount:mountLegacy, getConfig:getScrobbleConfig, getRootPatch };
  w.getScrobbleConfig=getScrobbleConfig; w.getRootPatch=getRootPatch;

  d.addEventListener("DOMContentLoaded", async ()=>{
    const root=d.getElementById("scrobble-mount"); if(!root) return;
    let cfg=null; try{ cfg=await API.cfgGet(); }catch{ cfg=w._cfgCache||{}; }
    init({ mountId:"scrobble-mount", cfg });
  });
})(window, document);
