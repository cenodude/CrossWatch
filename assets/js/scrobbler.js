// CrossWatch Scrobbler UI — compact, friendly, no fluff
(function (w, d) {
  // Tiny helpers: obvious, predictable
  const $=(s,r)=> (r||d).querySelector(s),
        $all=(s,r)=>[...(r||d).querySelectorAll(s)],
        el=(t,a)=>Object.assign(d.createElement(t),a||{}),
        on=(n,e,f)=>n&&n.addEventListener(e,f);
  const fetchJSON=async(url,opt)=>{const r=await fetch(url,{cache:"no-store",...(opt||{})});if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json();};
  const fetchText=async(url,opt)=>{const r=await fetch(url,{cache:"no-store",...(opt||{})});if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.text();};

  // UI note: quick status blip
  function setNote(id,msg,kind){
    const n=d.getElementById(id); if(!n) return;
    n.textContent=msg||""; n.style.cssText="margin:6px 0 2px;font-size:12px;opacity:.9;color:"+(kind==="err"?"#ff6b6b":"var(--muted,#a7a7a7)");
  }

  // CSS: scoped, idempotent
  const injectStyles=()=>d.getElementById("sc-styles")||d.head.appendChild(Object.assign(d.createElement("style"),{id:"sc-styles",textContent:".sc-row{display:grid;grid-template-columns:1fr auto;gap:16px;align-items:center}.sc-status-line{display:flex;align-items:center;gap:10px;min-height:32px}.sc-actions{display:flex;flex-direction:column;align-items:flex-end;gap:8px}.sc-btnrow{display:flex;gap:8px}.sc-toggle{display:inline-flex;align-items:center;gap:8px;white-space:nowrap}.badge{padding:4px 10px;border-radius:999px;font-weight:600;opacity:.9}.badge.is-on{background:#0a3;color:#fff}.badge.is-off{background:#333;color:#bbb;border:1px solid #444}.status-dot{width:10px;height:10px;border-radius:50%;display:inline-block}.status-dot.on{background:#22c55e}.status-dot.off{background:#ef4444}#scrob-watcher,#sc-sec-watch,.card,details,.sc-advanced,.sc-filters{overflow:visible}.watcher-row{display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:start}.chips{display:flex;flex-wrap:wrap;gap:6px}.chip{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08)}.chip .rm{cursor:pointer;opacity:.7}.sc-filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:start}.sc-adv-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;align-items:end}.field{display:flex;gap:6px;align-items:center;position:relative}.field label{white-space:nowrap;font-size:12px;opacity:.8}.field input{width:100%}details.sc-filters,details.sc-advanced{display:block;margin-top:12px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset;scroll-margin-top:16px}details.sc-filters>summary,details.sc-advanced>summary{cursor:pointer;list-style:none;padding:14px;border-radius:12px;font-weight:600}details.sc-filters[open]>summary,details.sc-advanced[open]>summary{border-bottom:1px solid rgba(255,255,255,.06)}details.sc-filters .body,details.sc-advanced .body{padding:12px 14px}.field[data-tip]:hover::after{content:attr(data-tip);position:absolute;bottom:100%;left:0;transform:translateY(-6px);background:rgba(0,0,0,.9);color:#fff;padding:6px 8px;border-radius:6px;font-size:12px;white-space:normal;max-width:260px;box-shadow:0 2px 8px rgba(0,0,0,.4);z-index:1000}.field[data-tip]:hover::before{content:\"\";position:absolute;bottom:100%;left:12px;border:6px solid transparent;border-top-color:rgba(0,0,0,.9)"}));

  // Defaults
  const DEFAULTS={watch:{pause_debounce_seconds:5,suppress_start_at:99},trakt:{stop_pause_threshold:80,force_stop_at:95,regress_tolerance_percent:5}};

  // In-memory state
  const STATE={mount:null,webhookHost:null,watcherHost:null,cfg:{},users:[],pms:[],pollTimer:null};

  // Safe getters/setters
  const deepSet=(o,p,v)=>p.split(".").reduce((a,k,i,arr)=>(i===arr.length-1?(a[k]=v):((a[k]&&typeof a[k]==="object")||(a[k]={})),a[k]),o);
  function read(path, dflt){ return path.split(".").reduce((v,k)=>v&&typeof v==="object"?v[k]:undefined, STATE.cfg) ?? dflt; }
  function write(path,val){
    deepSet(STATE.cfg,path,val);
    try{
      w._cfgCache ||= {};
      if(path.startsWith("plex.")){ w._cfgCache.plex ||= {}; deepSet(w._cfgCache,path,val); }
      else deepSet(w._cfgCache,path,val);
    }catch{}
    try{ syncHiddenServerUrl?.(); }catch{}
  }

    // Number helpers
    const asArray=v=>Array.isArray(v)?v.slice():v==null||v===""?[]:[String(v)];
    const clamp100=n=>Math.min(100,Math.max(1,Math.round(Number(n))));
    const norm100=(n,dflt)=>clamp100(Number.isFinite(+n)?+n:dflt);

    // Backend surface
    const API={
      cfgGet:()=>fetchJSON("/api/config"),
      users:async()=>{const j=await fetchJSON("/api/plex/users");const list=Array.isArray(j)?j:Array.isArray(j?.users)?j.users:[];return Array.isArray(list)?list:[];},
      serverUUID:()=>fetchJSON("/api/plex/server_uuid"),
      pms:async()=>{const j=await fetchJSON("/api/plex/pms");const list=Array.isArray(j)?j:Array.isArray(j?.servers)?j.servers:[];return Array.isArray(list)?list:[];},
      watch:{ status:()=>fetchJSON("/debug/watch/status"), start:()=>fetchText("/debug/watch/start",{method:"POST"}), stop:()=>fetchText("/debug/watch/stop",{method:"POST"}) }
    };

    // expose what’s needed elsewhere (optional)
    w.ScrobUI={ $, $all, el, on, setNote, injectStyles, DEFAULTS, STATE, read, write, asArray, clamp100, norm100, API };
  })(window, document);

  // Tiny chip with a removable “x”
  function chip(label, onRemove){
    const c=el("span",{className:"chip"}),
          t=el("span",{textContent:label}),
          rm=el("span",{className:"rm",title:"Remove",textContent:"×"});
    on(rm,"click",()=>onRemove&&onRemove(label));
    c.append(t,rm); return c;
  }

  // Paint watcher status in one pass
  function setWatcherStatus(ui){
    const alive=!!ui?.alive,
          q=id=>$(id,STATE.mount),
          dot=q("#sc-status-dot"),
          txt=q("#sc-status-text"),
          badge=q("#sc-status-badge"),
          last=q("#sc-status-last"),
          up=q("#sc-status-up");
    if(dot){ dot.classList.toggle("on",alive); dot.classList.toggle("off",!alive); }
    if(txt) txt.textContent=alive?"Active":"Inactive";
    if(badge){ badge.textContent=alive?"Active":"Stopped"; badge.classList.toggle("is-on",alive); }
    if(last) last.textContent=ui?.lastSeen?`Last seen: ${ui.lastSeen}`:"";
    if(up)   up.textContent=ui?.uptime?`Uptime: ${ui.uptime}`:"";
  }

  // Flip fields based on mode; nudge with a note
  function applyModeDisable(){
    const wh=$("#sc-enable-webhook",STATE.mount),
          wa=$("#sc-enable-watcher",STATE.mount),
          webhookOn=!!wh?.checked,
          watcherOn=!!wa?.checked;

    write("scrobble.enabled", webhookOn||watcherOn);
    write("scrobble.mode", watcherOn?"watch":"webhook");

    const webRoot=STATE.webhookHost||$("#sc-sec-webhook",STATE.mount)||STATE.mount,
          watchRoot=STATE.watcherHost||$("#sc-sec-watch",STATE.mount)||STATE.mount;

    $all(".input, input, button, select, textarea",webRoot)
      .forEach(n=>{ if(n.id!=="sc-enable-webhook") n.disabled=!webhookOn; });
    $all(".input, input, button, select, textarea",watchRoot)
      .forEach(n=>{ if(n.id!=="sc-enable-watcher") n.disabled=!watcherOn; });

    const srv=String(read("plex.server_url","")||"");
    if(watcherOn){
      isValidServerUrl(srv)
        ? setNote("sc-pms-note",`Using ${srv}`)
        : setNote("sc-pms-note","Plex Server is required (http(s)://…)", "err");
    }else setNote("sc-pms-note","");
  }

  // URL sanity check with grace
  function isValidServerUrl(v){
    if(!v) return false;
    try{ const u=new URL(v); return (u.protocol==="http:"||u.protocol==="https:")&&!!u.host; }
    catch{ return false; }
  }

  // Build sections on demand
  function buildUI(){
    injectStyles();

    if(STATE.webhookHost){
      STATE.webhookHost.innerHTML=`
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
          <label style="display:inline-flex;gap:8px;align-items:center">
            <input type="checkbox" id="sc-enable-webhook"> Enable
          </label>
        </div>
        <div class="muted">Endpoint</div>
        <div style="display:flex; gap:8px; align-items:center">
          <code id="sc-webhook-url">/webhook/trakt</code>
          <button id="sc-copy-endpoint" class="btn small">Copy</button>
        </div>
        <div class="micro-note" style="margin-top:8px">Webhooks can be flaky; <strong>Watcher</strong> is recommended. Only one mode active at a time.</div>
      `;
    }

    if(STATE.watcherHost){
      STATE.watcherHost.innerHTML=`
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
          <label style="display:inline-flex;gap:8px;align-items:center">
            <input type="checkbox" id="sc-enable-watcher"> Enable
          </label>
        </div>

        <div class="watcher-row">
          <div class="card" id="sc-card-server" style="padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset;">
            <div class="h" style="display:flex;justify-content:space-between;align-items:center">
              <div>Plex Server <span class="pill req">required</span></div>
              <button id="sc-pms-refresh" class="btn small">Fetch</button>
            </div>
            <div id="sc-pms-note" class="micro-note" style="margin-top:6px"></div>
            <div style="margin-top:8px">
              <div class="muted">Discovered servers</div>
              <select id="sc-pms-select" class="input" style="width:100%;margin-top:6px">
                <option value="">— select a server —</option>
              </select>
            </div>
            <div style="margin-top:12px">
              <div class="muted">Manual URL (http(s)://host[:port])</div>
              <input id="sc-pms-input" class="input" placeholder="https://192.168.1.10:32400" />
            </div>
          </div>

          <div class="card" id="sc-card-status" style="padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset;">
            <div class="h" style="display:flex;justify-content:space-between;align-items:center">
              <div>Watcher Status</div>
              <span id="sc-status-badge" class="badge is-off">Stopped</span>
            </div>
            <div class="sc-row" style="margin-top:10px">
              <div class="sc-status-line">
                <span id="sc-status-dot" class="status-dot off"></span>
                <span class="muted">Status:</span>
                <span id="sc-status-text" class="status-text">Unknown</span>
              </div>
              <div class="sc-actions">
                <div class="sc-btnrow">
                  <button id="sc-watch-start" class="btn small">Start</button>
                  <button id="sc-watch-stop" class="btn small">Stop</button>
                  <button id="sc-watch-refresh" class="btn small">Refresh</button>
                </div>
                <label class="sc-toggle"><input type="checkbox" id="sc-autostart"> Autostart on boot</label>
              </div>
            </div>
            <div id="sc-status-msg" style="margin-top:6px;font-weight:600;font-size:13px"></div>
            <div id="sc-status-last" class="micro-note" style="margin-top:8px"></div>
            <div id="sc-status-up" class="micro-note"></div>
          </div>
        </div>

        <details id="sc-filters" class="sc-filters">
          <summary>Filters (optional)</summary>
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

        <details class="sc-advanced" id="sc-advanced">
          <summary>Advanced</summary>
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
        </details>
      `;
    }
  }

  // Wire UI, keep it lean
  ensureHiddenServerUrlInput();
  restoreDetailsState("#sc-filters", false, "sc-filters-open");
  restoreDetailsState("#sc-advanced", false, "sc-advanced-open");

  // Actions
  on($("#sc-copy-endpoint", STATE.mount), "click", () =>
    navigator.clipboard.writeText(`${location.origin}/webhook/trakt`)
      .then(()=>setNote("sc-users-note","Endpoint copied"))
      .catch(()=>setNote("sc-users-note","Copy failed","err"))
  );
  on($("#sc-add-user", STATE.mount), "click", onAddUser);
  on($("#sc-load-users", STATE.mount), "click", loadUsers);
  on($("#sc-watch-start", STATE.mount), "click", onWatchStart);
  on($("#sc-watch-stop", STATE.mount), "click", onWatchStop);
  on($("#sc-watch-refresh", STATE.mount), "click", () => { refreshWatcher(); try{ refreshWatchLogs(); }catch{} });
  on($("#sc-fetch-uuid", STATE.mount), "click", fetchServerUUID);
  on($("#sc-server-uuid", STATE.mount), "input", e => write("scrobble.watch.filters.server_uuid", String(e.target.value||"").trim()));

  // Percent inputs (blank = default)
  bindPercentInput("#sc-pause-debounce","scrobble.watch.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds);
  bindPercentInput("#sc-suppress-start","scrobble.watch.suppress_start_at",DEFAULTS.watch.suppress_start_at);
  bindPercentInput("#sc-stop-pause","scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold);
  bindPercentInput("#sc-force-stop","scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at);
  bindPercentInput("#sc-regress","scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent);

  // PMS selectors
  on($("#sc-pms-refresh", STATE.mount), "click", loadPmsList);
  on($("#sc-pms-select", STATE.mount), "change", e => {
    const v=String(e.target.value||"").trim();
    if(v){ $("#sc-pms-input",STATE.mount).value=v; write("plex.server_url",v); setNote("sc-pms-note",`Using ${v}`); }
    applyModeDisable();
  });
  on($("#sc-pms-input", STATE.mount), "input", e => {
    const v=String(e.target.value||"").trim();
    write("plex.server_url", v);
    if(v && !isValidServerUrl(v)) setNote("sc-pms-note","Invalid URL. Use http(s)://host[:port]","err");
    else if(v) setNote("sc-pms-note",`Using ${v}`);
    else setNote("sc-pms-note","Plex Server is required when Watcher is enabled","err");
    applyModeDisable();
  });

  // Mode exclusivity
  const wh=$("#sc-enable-webhook",STATE.mount), wa=$("#sc-enable-watcher",STATE.mount);
  const syncExclusive = src => {
    const webOn=!!wh?.checked, watOn=!!wa?.checked;
    if(src==="webhook" && webOn && wa) wa.checked=false;
    if(src==="watch"   && watOn && wh) wh.checked=false;
    write("scrobble.enabled", (!!wh?.checked)|| (!!wa?.checked));
    write("scrobble.mode", (!!wa?.checked) ? "watch" : "webhook");
    applyModeDisable();
  };
  if(wh) on(wh,"change",()=>syncExclusive("webhook"));
  if(wa) on(wa,"change",()=>syncExclusive("watch"));
  on($("#sc-autostart", STATE.mount),"change",e=>write("scrobble.watch.autostart",!!e.target.checked));

  /* --- Persist open/close of details blocks --- */
  function restoreDetailsState(sel, defaultOpen, key){
    const n=$(sel,STATE.mount); if(!n) return;
    let open=defaultOpen; try{ const v=localStorage.getItem(key); if(v!=null) open=(v==="1"); }catch{}
    n.open=!!open;
    on(n,"toggle",()=>{ try{localStorage.setItem(key,n.open?"1":"0");}catch{}; try{
      n.closest(".card")?.style && (n.closest(".card").style.overflow="visible");
      n.closest("#sc-sec-watch")?.style && (n.closest("#sc-sec-watch").style.overflow="visible");
    }catch{} });
  }

  /* --- Numbers with bounds --- */
  function readNum(sel,dflt){
    const raw=String($(sel,STATE.mount)?.value??"").trim();
    return raw==="" ? clamp100(dflt) : norm100(raw,dflt);
  }
  function commitAdvancedInputs(){
    write("scrobble.watch.pause_debounce_seconds",      readNum("#sc-pause-debounce",DEFAULTS.watch.pause_debounce_seconds));
    write("scrobble.watch.suppress_start_at",           readNum("#sc-suppress-start",DEFAULTS.watch.suppress_start_at));
    write("scrobble.trakt.stop_pause_threshold",        readNum("#sc-stop-pause",DEFAULTS.trakt.stop_pause_threshold));
    write("scrobble.trakt.force_stop_at",               readNum("#sc-force-stop",DEFAULTS.trakt.force_stop_at));
    write("scrobble.trakt.regress_tolerance_percent",   readNum("#sc-regress",DEFAULTS.trakt.regress_tolerance_percent));
  }
  function bindPercentInput(sel,path,dflt){
    const n=$(sel,STATE.mount); if(!n) return;
    const set=(val,commitEmpty=false)=>{
      const raw=String(val ?? n.value ?? "").trim();
      if(raw===""){ if(commitEmpty){ const v=clamp100(dflt); write(path,v); n.value=v; } return; }
      const v=norm100(raw,dflt); write(path,v); n.value=v;
    };
    on(n,"input", ()=>set(n.value,false));
    on(n,"change",()=>set(n.value,true));
    on(n,"blur",  ()=>set(n.value,true));
  }

  /* --- Hidden input for forms --- */
  function ensureHiddenServerUrlInput(){
    let hidden=d.getElementById("cfg-plex-server-url");
    const form=d.querySelector("form#settings, form#settings-form, form[data-settings]") || (STATE.mount||d.body);
    if(!hidden){
      hidden=d.createElement("input");
      hidden.type="hidden"; hidden.id="cfg-plex-server-url"; hidden.name="plex.server_url";
      form.appendChild(hidden);
    }
    syncHiddenServerUrl();
  }
  function syncHiddenServerUrl(){
    const h=d.getElementById("cfg-plex-server-url");
    if(h) h.value=String(read("plex.server_url","")||"");
  }

  /* --- Populate UI from cfg and hydrate lists --- */
  function populate(){
    const enabled=!!read("scrobble.enabled",false);
    const mode=String(read("scrobble.mode","webhook")).toLowerCase();
    const wl=asArray(read("scrobble.watch.filters.username_whitelist",[]));
    const su=read("scrobble.watch.filters.server_uuid","");
    const autostart=!!read("scrobble.watch.autostart",false);
    const serverUrl=String(read("plex.server_url","")||"");

    const useWebhook=enabled && mode==="webhook";
    const useWatch  =enabled && mode==="watch";

    const whEl=$("#sc-enable-webhook",STATE.mount);
    const waEl=$("#sc-enable-watcher",STATE.mount);
    if(whEl) whEl.checked=useWebhook;
    if(waEl) waEl.checked=useWatch;

    const host=$("#sc-whitelist",STATE.mount);
    if(host){ host.innerHTML=""; wl.forEach(u=>host.append(chip(u,removeUser))); }

    const suInp=$("#sc-server-uuid",STATE.mount); if(suInp) suInp.value=su||"";
    const auto=$("#sc-autostart",STATE.mount); if(auto) auto.checked=!!autostart;
    const pmsInp=$("#sc-pms-input",STATE.mount); if(pmsInp) pmsInp.value=serverUrl;

    set("#sc-pause-debounce", read("scrobble.watch.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds));
    set("#sc-suppress-start", read("scrobble.watch.suppress_start_at",DEFAULTS.watch.suppress_start_at));
    set("#sc-stop-pause",     read("scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold));
    set("#sc-force-stop",     read("scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at));
    set("#sc-regress",        read("scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent));
    function set(id,v){ const n=$(id,STATE.mount); if(n) n.value=norm100(v,v); }

    syncHiddenServerUrl();
    applyModeDisable();
    refreshWatcher();
    loadPmsList().catch(()=>{});
  }

  /* --- Watcher controls --- */
  async function refreshWatcher(){
    try{ setWatcherStatus(await API.watch.status()||{}); }
    catch{ setWatcherStatus({alive:false}); }
  }
  function setStatusMsg(msg, ok=true){
    const n=document.getElementById("sc-status-msg"); if(!n) return;
    n.textContent=msg||""; n.style.color = ok ? "var(--fg,#fff)" : "#ff6b6b";
  }
  async function onWatchStart(){
    const srv=String(read("plex.server_url","")||"");
    if(!isValidServerUrl(srv)) return setNote("sc-pms-note","Plex Server is required (http(s)://…)","err");
    try{ await API.watch.start(); }catch{ setNote("sc-pms-note","Start failed","err"); }
    refreshWatcher();
  }
  async function onWatchStop(){
    try{ await API.watch.stop(); }catch{ setNote("sc-pms-note","Stop failed","err"); }
    refreshWatcher();
  }

  /* --- Helpers: UUID, servers, users --- */
  async function fetchServerUUID(){
    try{
      const j=await API.serverUUID(); const v=j?.server_uuid||j?.uuid||j?.id||"";
      const inp=$("#sc-server-uuid",STATE.mount);
      if(inp && v){ inp.value=v; write("scrobble.watch.filters.server_uuid",v); setNote("sc-uuid-note","Server UUID fetched"); }
      else setNote("sc-uuid-note","No server UUID","err");
    }catch{ setNote("sc-uuid-note","Fetch failed","err"); }
  }
  async function loadPmsList(){
    try{
      const sel=$("#sc-pms-select",STATE.mount); if(!sel) return;
      sel.innerHTML=`<option value="">Loading…</option>`;
      const list=await API.pms(); STATE.pms=list;
      sel.innerHTML=`<option value="">— select a server —</option>`;
      for(const s of list){
        const best=s.best_url||"", nm=s.name||s.product||"Plex Media Server", owned=s.owned?" (owned)":"";
        sel.append(el("option",{value:best||"",textContent:best?`${nm}${owned} — ${best}`:nm+owned}));
      }
      setNote("sc-pms-note", list.length? "Pick a discovered server or enter a URL" : "No servers discovered. Enter a URL.", list.length? null : "err");
    }catch{
      const sel=$("#sc-pms-select",STATE.mount); if(sel) sel.innerHTML=`<option value="">— select a server —</option>`;
      setNote("sc-pms-note","Fetch failed. Enter a URL manually.","err");
    }
  }
  function onAddUser(){
    const inp=$("#sc-user-input",STATE.mount);
    const v=String((inp?.value||"").trim()); if(!v) return;
    const cur=asArray(read("scrobble.watch.filters.username_whitelist",[]));
    if(!cur.includes(v)){
      const next=[...cur,v]; write("scrobble.watch.filters.username_whitelist",next);
      $("#sc-whitelist",STATE.mount).append(chip(v,removeUser)); inp.value="";
    }
  }
  function removeUser(u){
    const cur=asArray(read("scrobble.watch.filters.username_whitelist",[]));
    const next=cur.filter(x=>String(x)!==String(u));
    write("scrobble.watch.filters.username_whitelist",next);
    const host=$("#sc-whitelist",STATE.mount); host.innerHTML=""; next.forEach(v=>host.append(chip(v,removeUser)));
  }
  async function loadUsers(){
    try{
      const list=await API.users();
      const filtered=list.filter(u=>["managed","owner"].includes(String(u?.type||"").toLowerCase())||u?.owned===true||u?.isHomeUser===true);
      STATE.users=filtered;
      const names=filtered.map(u=>u?.username||u?.title).filter(Boolean);
      const host=$("#sc-whitelist",STATE.mount);
      let added=0; for(const n of names){
        const cur=asArray(read("scrobble.watch.filters.username_whitelist",[]));
        if(!cur.includes(n)){ write("scrobble.watch.filters.username_whitelist",[...cur,n]); host.append(chip(n,removeUser)); added++; }
      }
      setNote("sc-users-note", added?`Loaded ${added} user(s)`:"No eligible managed/owner users");
    }catch{ setNote("sc-users-note","Load users failed","err"); }
  }

  /* --- Public API --- */
  function init(opts={}){
    STATE.mount = opts.mountId ? d.getElementById(opts.mountId) : d;
    STATE.cfg   = opts.cfg || w._cfgCache || {};
    STATE.webhookHost = $("#scrob-webhook",STATE.mount);
    STATE.watcherHost = $("#scrob-watcher",STATE.mount);

    if(!STATE.webhookHost || !STATE.watcherHost){
      const root=STATE.mount||d.body;
      const makeSec=(id,title)=>{ const sec=el("div",{className:"section",id});
        sec.innerHTML=`<div class="head"><strong>${title}</strong></div><div class="body"><div id="${id==="sc-sec-webhook"?"scrob-webhook":"scrob-watcher"}"></div></div>`;
        root.append(sec);
      };
      if(!STATE.webhookHost){ makeSec("sc-sec-webhook","Webhook"); STATE.webhookHost=$("#scrob-webhook",STATE.mount); }
      if(!STATE.watcherHost){ makeSec("sc-sec-watch","Watcher"); STATE.watcherHost=$("#scrob-watcher",STATE.mount); }
    }

    buildUI();
    populate();
  }
  function mountLegacy(targetEl, initialCfg){
    STATE.mount=targetEl||d; STATE.cfg=initialCfg||(w._cfgCache||{});
    STATE.webhookHost=$("#scrob-webhook",STATE.mount)||targetEl;
    STATE.watcherHost=$("#scrob-watcher",STATE.mount)||targetEl;
    buildUI(); populate();
  }
  function getScrobbleConfig(){
    commitAdvancedInputs();
    const enabled=!!read("scrobble.enabled",false);
    const mode   =String(read("scrobble.mode","webhook")).toLowerCase();
    const wl     =asArray(read("scrobble.watch.filters.username_whitelist",[]));
    const su     =read("scrobble.watch.filters.server_uuid","");
    const autostart=!!read("scrobble.watch.autostart",false);
    return {
      enabled,
      mode: mode==="watch"?"watch":"webhook",
      webhook:{},
      watch:{
        autostart,
        pause_debounce_seconds:     read("scrobble.watch.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds),
        suppress_start_at:          read("scrobble.watch.suppress_start_at",DEFAULTS.watch.suppress_start_at),
        filters:{ username_whitelist: wl, server_uuid: su||"" }
      },
      trakt:{
        stop_pause_threshold:       read("scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold),
        force_stop_at:              read("scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at),
        regress_tolerance_percent:  read("scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent)
      }
    };
  }
  function getRootPatch(){ return { plex:{ server_url:String(read("plex.server_url","")||"") } }; }

  w.Scrobbler={ init, mount:mountLegacy, getConfig:getScrobbleConfig, getRootPatch };
  w.getScrobbleConfig=getScrobbleConfig;
  w.getRootPatch=getRootPatch;

  // Auto-mount if present
  d.addEventListener("DOMContentLoaded", async ()=>{
    const root=d.getElementById("scrobble-mount"); if(!root) return;
    let cfg=null; try{ cfg=await API.cfgGet(); }catch{ cfg=w._cfgCache||{}; }
    init({ mountId:"scrobble-mount", cfg });
  });