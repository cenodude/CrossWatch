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
    if(document.getElementById("sc-styles")) return;
    const s=document.createElement("style"); s.id="sc-styles";
    s.textContent=`
    .row{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
    .codepair{display:flex;gap:8px;align-items:center}
    .codepair code{padding:6px 8px;border-radius:8px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}
    .badge{padding:4px 10px;border-radius:999px;font-weight:600;opacity:.9}.badge.is-on{background:#0a3;color:#fff}.badge.is-off{background:#333;color:#bbb;border:1px solid #444}
    .status-dot{width:10px;height:10px;border-radius:50%}.status-dot.on{background:#22c55e}.status-dot.off{background:#ef4444}
    .watcher-row{display:grid;grid-template-columns:1fr 1fr;gap:16px}
    @media (max-width:960px){.watcher-row{grid-template-columns:1fr}}
    .chips{display:flex;flex-wrap:wrap;gap:6px}.chip{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08)}.chip .rm{cursor:pointer;opacity:.7}
    .sc-filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .sc-adv-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;align-items:end}
    .field{display:flex;gap:6px;align-items:center;position:relative}.field label{white-space:nowrap;font-size:12px;opacity:.8}.field input{width:100%}
    details.sc-filters,details.sc-advanced{display:block;margin-top:12px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset}
    details.sc-filters>summary,details.sc-advanced>summary{cursor:pointer;list-style:none;padding:14px;border-radius:12px;font-weight:600}
    details.sc-filters[open]>summary,details.sc-advanced[open]>summary{border-bottom:1px solid rgba(255,255,255,.06)}
    details.sc-filters .body,details.sc-advanced .body{padding:12px 14px}
    .sc-ctrls{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-top:10px}
    .sc-ctrls .left{display:flex;align-items:center;gap:8px}
    .sc-ctrls .right{display:flex;align-items:center;gap:8px;margin-left:auto}
    .sc-toggle{display:inline-flex;align-items:center;gap:8px;font-size:12px;opacity:.9;white-space:nowrap}
    .wh-top{display:grid;grid-template-columns:auto 1fr;align-items:start;gap:12px;margin-bottom:8px;position:relative}
    .wh-toggle{display:inline-flex;gap:8px;align-items:center}
    .wh-endpoints{display:flex;flex-direction:column;gap:8px;align-items:flex-end}
    .codepair.right{justify-content:flex-end}
    @media(max-width:960px){.wh-top{grid-template-columns:1fr}.wh-endpoints{align-items:flex-start}}
    .wh-logo{width:var(--wh-logo,24px);height:var(--wh-logo,24px);aspect-ratio:1/1;object-fit:contain;display:block;transform-origin:center}
    .wh-logo[alt="Plex"]{transform:scale(1.15)}
    .wh-logo[alt="Jellyfin"]{transform:scale(1.0)}
    .wh-logo[alt="Emby"]{transform:scale(1.15)}
    `;
    document.head.appendChild(s);
  }

  const DEFAULTS={watch:{pause_debounce_seconds:5,suppress_start_at:99},trakt:{stop_pause_threshold:80,force_stop_at:95,regress_tolerance_percent:5}};
  const STATE={mount:null,webhookHost:null,watcherHost:null,cfg:{},users:[],pms:[]};

  const deepSet=(o,p,v)=>p.split(".").reduce((a,k,i,arr)=>(i===arr.length-1?(a[k]=v):((a[k]&&typeof a[k]==="object")||(a[k]={})),a[k]),o);
  const read=(p,dflt)=>p.split(".").reduce((v,k)=>v&&typeof v==="object"?v[k]:undefined,STATE.cfg) ?? dflt;
  function write(p,v){ deepSet(STATE.cfg,p,v); try{ w._cfgCache ||= {}; deepSet(w._cfgCache,p,v);}catch{} try{syncHiddenServerInputs();}catch{} }

  const asArray=v=>Array.isArray(v)?v.slice():v==null||v===""?[]:[String(v)];
  const clamp100=n=>Math.min(100,Math.max(1,Math.round(Number(n))));
  const norm100=(n,dflt)=>clamp100(Number.isFinite(+n)?+n:dflt);
  const provider=()=>String(read("scrobble.watch.provider","plex")||"plex").toLowerCase();

  const API={
    cfgGet:()=>j("/api/config"),
    users:async()=>{
      if(provider()==="emby"){
        const x=await j("/api/emby/users");
        const a=Array.isArray(x)?x:Array.isArray(x?.users)?x.users:[];
        return Array.isArray(a)?a:[]; 
      }else{
        const x=await j("/api/plex/users");
        const a=Array.isArray(x)?x:Array.isArray(x?.users)?x.users:[];
        return Array.isArray(a)?a:[]; 
      }
    },
    serverUUID:async()=>{
      if(provider()==="emby"){
        const x=await j("/api/emby/inspect");
        const uid=x?.user_id||x?.user?.Id||x?.id||"";
        return { id: uid };
      }else{
        return j("/api/plex/server_uuid");
      }
    },
    pms:async()=>{const x=await j("/api/plex/pms");const a=Array.isArray(x)?x:Array.isArray(x?.servers)?x.servers:[];return Array.isArray(a)?a:[];},
    watch:{
      status:()=>j("/debug/watch/status"),
      start:(prov)=>t(`/debug/watch/start${prov?`?provider=${encodeURIComponent(prov)}`:""}`,{method:"POST"}),
      stop:()=>t("/debug/watch/stop",{method:"POST"})
    }
  };

  function chip(label,onRemove,onPick){
    const c=el("span",{className:"chip"}),t=el("span",{textContent:label}),rm=el("span",{className:"rm",title:"Remove",textContent:"×"});
    on(rm,"click",()=>onRemove&&onRemove(label)); if(onPick) on(t,"click",()=>onPick(label)); c.append(t,rm); return c;
  }

  function setWatcherStatus(ui){
    const alive=!!ui?.alive,q=id=>$(id,STATE.mount),dot=q("#sc-status-dot"),txt=q("#sc-status-text"),badge=q("#sc-status-badge"),last=q("#sc-status-last"),up=q("#sc-status-up");
    if(dot){ dot.classList.toggle("on",alive); dot.classList.toggle("off",!alive); }
    if(txt) txt.textContent=alive?"Active":"Inactive";
    if(badge){ badge.textContent=alive?"Active":"Stopped"; badge.classList.toggle("is-on",alive); badge.classList.toggle("is-off",!alive); }
    if(last) last.textContent=ui?.lastSeen?`Last seen: ${ui.lastSeen}`:"";
    if(up)   up.textContent=ui?.uptime?`Uptime: ${ui.uptime}`:"";
    STATE.watchAlive = alive;
  }

  function isValidServerUrl(v){ if(!v) return false; try{const u=new URL(v);return (u.protocol==="http:"||u.protocol==="https:")&&!!u.host;}catch{return false;} }

  function applyModeDisable(){
    const wh=$("#sc-enable-webhook",STATE.mount),wa=$("#sc-enable-watcher",STATE.mount);
    const webhookOn=!!wh?.checked,watcherOn=!!wa?.checked;
    write("scrobble.enabled",webhookOn||watcherOn);
    write("scrobble.mode",watcherOn?"watch":"webhook");

    const prov=provider();
    const webRoot=$("#sc-sec-webhook",STATE.mount)||STATE.mount,watchRoot=$("#sc-sec-watch",STATE.mount)||STATE.mount;
    $all(".input, input, button, select, textarea",webRoot).forEach(n=>{if(n.id!=="sc-enable-webhook") n.disabled=!webhookOn;});
    $all(".input, input, button, select, textarea",watchRoot).forEach(n=>{if(!["sc-enable-watcher"].includes(n.id)) n.disabled=!watcherOn;});

    const srv = prov==="plex" ? String(read("plex.server_url","")||"") : String(read("emby.server","")||"");
    const lbl = prov==="plex" ? "Plex Server" : "Emby Server";
    const req = $("#sc-server-required",STATE.mount); if(req) req.style.display = prov==="plex" ? "" : "none";
    const lab = $("#sc-server-label",STATE.mount); if(lab) lab.textContent = lbl;

    const disc=$("#sc-pms-discovered",STATE.mount); if(disc) disc.style.display = prov==="plex" ? "" : "none";
    const btnDisc=$("#sc-pms-refresh",STATE.mount); if(btnDisc) btnDisc.disabled = prov!=="plex";

    const loadBtn=$("#sc-load-users",STATE.mount); if(loadBtn){ loadBtn.style.display=""; loadBtn.textContent = prov==="plex" ? "Load Plex users" : "Load Emby users"; }
    const fetchUuid=$("#sc-fetch-uuid",STATE.mount); if(fetchUuid) fetchUuid.disabled = false;
    const uuidLabel=$("#sc-uuid-label",STATE.mount); if(uuidLabel) uuidLabel.textContent = prov==="plex" ? "Server UUID" : "User ID";
    const uuidInput=$("#sc-server-uuid",STATE.mount); if(uuidInput) uuidInput.placeholder = prov==="plex" ? "e.g. abcd1234..." : "e.g. 80ee72c0...";

    const delWrap=$("#sc-delete-plex-watch-wrap",STATE.mount); if(delWrap) delWrap.style.display = "";

    const plexTokenOk=!!String(read("plex.account_token","")||"").trim();
    const embyTokenOk=!!String(read("emby.access_token","")||"").trim();
    if(watcherOn){
      if(prov==="plex"){
        if(!plexTokenOk){ setNote("sc-pms-note","Not connected to Plex. Go to Authentication → Plex.","err"); }
        else if(!isValidServerUrl(srv)) setNote("sc-pms-note","Plex Server is required (http(s)://…)","err");
        else setNote("sc-pms-note",`Using ${srv}`);
      }else{
        if(!embyTokenOk){ setNote("sc-pms-note","Not connected to Emby. Go to Authentication → Emby.","err"); }
        else setNote("sc-pms-note", srv?`Using ${srv}`:"");
      }
    } else setNote("sc-pms-note","");

    if(loadBtn){
      if(prov==="plex" && !plexTokenOk) loadBtn.disabled=true; else if(prov==="emby" && !embyTokenOk) loadBtn.disabled=true; else loadBtn.disabled=!watcherOn;
    }
    if(fetchUuid){
      if(prov==="plex" && !plexTokenOk) fetchUuid.disabled=true; else if(prov==="emby" && !embyTokenOk) fetchUuid.disabled=true; else fetchUuid.disabled=!watcherOn;
    }
  }

  function buildUI(){
    injectStyles();
    if(STATE.webhookHost){
      STATE.webhookHost.innerHTML=`
        <div class="wh-top" style="--wh-logo:28px">
          <div class="wh-left">
            <label class="wh-toggle"><input type="checkbox" id="sc-enable-webhook"><span>Enable</span></label>
            <div style="margin-top:6px">
              <label class="sc-toggle">
                <input type="checkbox" id="sc-delete-plex-webhook">
                <span class="one-line">Auto-remove from Watchlists</span>
              </label>
            </div>
          </div>
          <div class="wh-endpoints">
            <div class="codepair right">
              <img class="wh-logo" src="/assets/img/PLEX-log.svg" alt="Plex">
              <code id="sc-webhook-url-plex"></code>
              <button id="sc-copy-plex" class="btn small">Copy</button>
            </div>
            <div class="codepair right">
              <img class="wh-logo" src="/assets/img/JELLYFIN-log.svg" alt="Jellyfin">
              <code id="sc-webhook-url-jf"></code>
              <button id="sc-copy-jf" class="btn small">Copy</button>
            </div>
            <div class="codepair right">
              <img class="wh-logo" src="/assets/img/EMBY-log.svg" alt="Emby">
              <code id="sc-webhook-url-emby"></code>
              <button id="sc-copy-emby" class="btn small">Copy</button>
            </div>
          </div>
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
              <div class="field"><label for="sc-pause-debounce-webhook">Pause debounce</label><input id="sc-pause-debounce-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.pause_debounce_seconds}"></div>
              <div class="field"><label for="sc-suppress-start-webhook">Suppress start @</label><input id="sc-suppress-start-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.suppress_start_at}"></div>
              <div class="field"><label for="sc-regress-webhook">Regress tol %</label><input id="sc-regress-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.regress_tolerance_percent}"></div>
              <div class="field"><label for="sc-stop-pause-webhook">Stop pause ≥</label><input id="sc-stop-pause-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.stop_pause_threshold}"></div>
              <div class="field"><label for="sc-force-stop-webhook">Force stop @</label><input id="sc-force-stop-webhook" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.force_stop_at}"></div>
            </div>
            <div class="micro-note" style="margin-top:6px">Empty resets to defaults. Values are 1–100.</div>
          </div>
        </details>
      `;
    }

    if(STATE.watcherHost){
      STATE.watcherHost.innerHTML=`
        <style>
          .cc-wrap{display:grid;grid-template-columns:1fr 1fr;gap:16px}
          .cc-card{padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset}
          .cc-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
          .cc-body{display:grid;gap:14px}
          .cc-gauge{width:100%;min-height:68px;display:flex;align-items:center;gap:14px;padding:14px 16px;border-radius:14px;background:rgba(255,255,255,.05);box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)}
          .cc-state{display:flex;flex-direction:column;line-height:1.15}
          .cc-state .lbl{font-size:12px;opacity:.75}
          .cc-state .val{font-size:22px;font-weight:800;letter-spacing:.2px}
          .cc-meta{display:flex;gap:16px;flex-wrap:wrap;font-size:12px;opacity:.85}
          .cc-actions{display:flex;gap:12px;justify-content:center;flex-wrap:wrap}
          .cc-auto{display:flex;justify-content:center;margin-top:2px}
          .status-dot{width:16px;height:16px;border-radius:50%;box-shadow:0 0 18px currentColor}
          .status-dot.on{background:#22c55e;color:#22c55e}
          .status-dot.off{background:#ef4444;color:#ef4444}
          @media (max-width:900px){.cc-wrap{grid-template-columns:1fr}}
        </style>

        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
          <label style="display:inline-flex;gap:8px;align-items:center"><input type="checkbox" id="sc-enable-watcher"> Enable</label>
          <div style="margin-left:auto;display:flex;gap:8px;align-items:center">
            <span style="opacity:.75;font-size:12px">Provider</span>
            <select id="sc-provider" class="input" style="width:140px">
              <option value="plex">Plex</option>
              <option value="emby">Emby</option>
            </select>
          </div>
        </div>

        <div class="cc-wrap">
          <div class="cc-card" id="sc-card-server">
            <div class="cc-head">
              <div><span id="sc-server-label">Plex Server</span> <span id="sc-server-required" class="pill req">required</span></div>
              <button id="sc-pms-refresh" class="btn small">Fetch</button>
            </div>
            <div id="sc-pms-note" class="micro-note" style="margin-top:2px"></div>
            <div id="sc-pms-discovered" style="margin-top:10px">
              <div class="muted">Discovered servers</div>
              <select id="sc-pms-select" class="input" style="width:100%;margin-top:6px"><option value="">— select a server —</option></select>
            </div>
            <div style="margin-top:12px">
              <div class="muted">Manual URL (http(s)://host[:port])</div>
              <input id="sc-pms-input" class="input" placeholder="https://192.168.1.10:32400" />
            </div>
          </div>

          <div class="cc-card" id="sc-card-status">
            <div class="cc-head">
              <div>Watcher Status</div>
              <span id="sc-status-badge" class="badge is-off">Stopped</span>
            </div>
            <div class="cc-body">
              <div class="cc-gauge">
                <span id="sc-status-dot" class="status-dot off"></span>
                <div class="cc-state">
                  <span class="lbl">Status</span>
                  <span id="sc-status-text" class="val">Inactive</span>
                </div>
              </div>
              <div class="cc-meta">
                <span id="sc-status-last" class="micro-note"></span>
                <span id="sc-status-up" class="micro-note"></span>
              </div>
              <div class="cc-actions">
                <button id="sc-watch-start" class="btn small">Start</button>
                <button id="sc-watch-stop" class="btn small">Stop</button>
                <button id="sc-watch-refresh" class="btn small">Refresh</button>
              </div>
              <div class="cc-auto">
                <label class="sc-toggle"><input type="checkbox" id="sc-autostart"> Autostart on boot</label>
              </div>
            </div>
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
                <div class="muted" id="sc-uuid-label">Server UUID</div>
                <div id="sc-uuid-note" class="micro-note"></div>
                <div style="display:flex; gap:8px; align-items:center; margin-top:6px">
                  <input id="sc-server-uuid" class="input" placeholder="e.g. abcd1234..." style="flex:1">
                  <button id="sc-fetch-uuid" class="btn small">Fetch</button>
                </div>
              </div>
            </div>
            <div id="sc-delete-plex-watch-wrap" style="margin-top:8px">
              <label class="sc-toggle">
                <input type="checkbox" id="sc-delete-plex-watch">
                <span class="one-line">Auto-remove from Watchlists</span>
              </label>
            </div>
          </div>
        </details>

        <details class="sc-advanced" id="sc-advanced"><summary>Advanced</summary>
          <div class="body">
            <div class="sc-adv-grid">
              <div class="field" data-tip="Per-session PAUSE debounce."><label for="sc-pause-debounce">Pause debounce</label><input id="sc-pause-debounce" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.pause_debounce_seconds}"></div>
              <div class="field" data-tip="Suppress START when progress ≥ threshold."><label for="sc-suppress-start">Suppress start @</label><input id="sc-suppress-start" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.watch.suppress_start_at}"></div>
              <div class="field" data-tip="Allow small regressions."><label for="sc-regress">Regress tol %</label><input id="sc-regress" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.regress_tolerance_percent}"></div>
              <div class="field" data-tip="STOP below threshold → PAUSE."><label for="sc-stop-pause">Stop pause ≥</label><input id="sc-stop-pause" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.stop_pause_threshold}"></div>
              <div class="field" data-tip="Final STOP threshold."><label for="sc-force-stop">Force stop @</label><input id="sc-force-stop" class="input" type="number" min="1" max="100" step="1" placeholder="${DEFAULTS.trakt.force_stop_at}"></div>
            </div>
            <div class="micro-note" style="margin-top:6px">Empty resets to defaults. Values are 1–100.</div>
          </div>
        </details>`;
    }
  }

  function ensureHiddenServerInputs(){
    const form=d.querySelector("form#settings, form#settings-form, form[data-settings]") || (STATE.mount||d.body);
    let h1=d.getElementById("cfg-plex-server-url");
    if(!h1){ h1=el("input",{type:"hidden",id:"cfg-plex-server-url",name:"plex.server_url"}); form.appendChild(h1); }
    let h2=d.getElementById("cfg-emby-server-url");
    if(!h2){ h2=el("input",{type:"hidden",id:"cfg-emby-server-url",name:"emby.server"}); form.appendChild(h2); }
    syncHiddenServerInputs();
  }
  function syncHiddenServerInputs(){
    const h1=d.getElementById("cfg-plex-server-url"); if(h1) h1.value=String(read("plex.server_url","")||"");
    const h2=d.getElementById("cfg-emby-server-url"); if(h2) h2.value=String(read("emby.server","")||"");
  }

  function restoreDetailsState(sel,def,key){
    const n=$(sel,STATE.mount); if(!n) return;
    let open=def; try{const v=localStorage.getItem(key); if(v!=null) open=(v==="1");}catch{}
    n.open=!!open;
    on(n,"toggle",()=>{try{localStorage.setItem(key,n.open?"1":"0");}catch{}});
  }

  const readNum=(sel,dflt)=>{const raw=String($(sel,STATE.mount)?.value??"").trim();return raw===""?clamp100(dflt):norm100(raw,dflt);};

  async function copyText(s){
    try{ await navigator.clipboard.writeText(s); return true; }catch{
      try{ const ta=el("textarea",{style:"position:fixed;left:-9999px;top:-9999px"}); ta.value=s; d.body.appendChild(ta); ta.select(); const ok=d.execCommand?d.execCommand("copy"):document.execCommand("copy"); d.body.removeChild(ta); return !!ok; }catch{ return false; }
    }
  }

  function commitAdvancedInputsWatch(){
    write("scrobble.watch.pause_debounce_seconds",readNum("#sc-pause-debounce",DEFAULTS.watch.pause_debounce_seconds));
    write("scrobble.watch.suppress_start_at",readNum("#sc-suppress-start",DEFAULTS.watch.suppress_start_at));
  }
  function commitAdvancedInputsWebhook(){
    write("scrobble.webhook.pause_debounce_seconds",readNum("#sc-pause-debounce-webhook",DEFAULTS.watch.pause_debounce_seconds));
    write("scrobble.webhook.suppress_start_at",readNum("#sc-suppress-start-webhook",DEFAULTS.watch.suppress_start_at));
  }
  function commitAdvancedInputsTrakt(){
    const keys=[["#sc-stop-pause","scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold],
                ["#sc-force-stop","scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at],
                ["#sc-regress","scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent],
                ["#sc-stop-pause-webhook","scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold],
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

  function onSelectWatchUser(name){
    if(provider()!=="emby") return;
    const list=Array.isArray(STATE.users)?STATE.users:[];
    const hit=list.find(u=>String(u?.username||u?.Name||u?.name||"").toLowerCase()===String(name||"").toLowerCase());
    const id=hit?.id||hit?.Id;
    if(id){
      const inp=$("#sc-server-uuid",STATE.mount); if(inp) inp.value=id;
      write("scrobble.watch.filters.server_uuid",id);
      write("scrobble.watch.filters.user_id",id);
      setNote("sc-uuid-note","User ID set from username");
    }else{
      setNote("sc-uuid-note","User not found","err");
    }
  }

  function populate(){
    const enabled=!!read("scrobble.enabled",false),mode=String(read("scrobble.mode","webhook")).toLowerCase();
    const useWebhook=enabled&&mode==="webhook",useWatch=enabled&&mode==="watch";
    const prov=provider();

    const whEl=$("#sc-enable-webhook",STATE.mount),waEl=$("#sc-enable-watcher",STATE.mount),pvSel=$("#sc-provider",STATE.mount);
    if(whEl) whEl.checked=useWebhook; if(waEl) waEl.checked=useWatch; if(pvSel) pvSel.value=prov;

    let wlWatch=asArray(read("scrobble.watch.filters.username_whitelist",[]));
    if(prov==="emby" && wlWatch.length===0){
      const embyUser = String(read("emby.username", read("emby.user",""))||"").trim();
      if(embyUser){ wlWatch=[embyUser]; write("scrobble.watch.filters.username_whitelist",wlWatch); }
    }
    const hostW=$("#sc-whitelist",STATE.mount); if(hostW){ hostW.innerHTML=""; wlWatch.forEach(u=>hostW.append(chip(u,removeUserWatch,prov==="emby"?onSelectWatchUser:undefined))); }
    const suWatch=read("scrobble.watch.filters.server_uuid",""),suInpW=$("#sc-server-uuid",STATE.mount); if(suInpW) suInpW.value=suWatch||"";

    const wlWeb=asArray(read("scrobble.webhook.filters_plex.username_whitelist",[]));
    const hostWB=$("#sc-whitelist-webhook",STATE.mount); if(hostWB){ hostWB.innerHTML=""; wlWeb.forEach(u=>hostWB.append(chip(u,removeUserWebhook))); }
    const suWeb=read("scrobble.webhook.filters_plex.server_uuid",""),suInpWB=$("#sc-server-uuid-webhook",STATE.mount); if(suInpWB) suInpWB.value=suWeb||"";

    const base=location.origin;
    const plexCode=$("#sc-webhook-url-plex",STATE.mount),
          jfCode  =$("#sc-webhook-url-jf",STATE.mount),
          embyCode=$("#sc-webhook-url-emby",STATE.mount);
    if(plexCode) plexCode.textContent=`${base}/webhook/plextrakt`;
    if(jfCode)   jfCode.textContent=`${base}/webhook/jellyfintrakt`;
    if(embyCode) embyCode.textContent=`${base}/webhook/embytrakt`;

    const autostart=!!read("scrobble.watch.autostart",false);
    const auto=$("#sc-autostart",STATE.mount); if(auto) auto.checked=!!autostart;

    const pmsInp=$("#sc-pms-input",STATE.mount);
    const plexUrl=String(read("plex.server_url","")||"");
    const embyUrl=String(read("emby.server","")||"");
    if(pmsInp) pmsInp.value = prov==="plex" ? plexUrl : embyUrl;

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

    const delEnabled=!!read("scrobble.delete_plex",false);
    const delWh=$("#sc-delete-plex-webhook",STATE.mount); if(delWh) delWh.checked=delEnabled;
    const delW=$("#sc-delete-plex-watch",STATE.mount); if(delW) delW.checked=delEnabled;
    const delWrap=$("#sc-delete-plex-watch-wrap",STATE.mount); if(delWrap) delWrap.style.display = "";

    restoreDetailsState("#sc-filters",false,"sc-filters-open");
    restoreDetailsState("#sc-advanced",false,"sc-advanced-open");
    restoreDetailsState("#sc-filters-webhook",false,"sc-filters-webhook-open");
    restoreDetailsState("#sc-advanced-webhook",false,"sc-advanced-webhook-open");

    syncHiddenServerInputs(); applyModeDisable();
  }

  async function refreshWatcher(){ try{ setWatcherStatus(await API.watch.status()||{});}catch{ setWatcherStatus({alive:false}); } }

  async function onWatchStart(){
    const prov=provider();
    const srvProv = prov==="plex" ? "plex.server_url" : "emby.server";
    const srv=String(read(srvProv,"")||"");
    const plexTokenOk=!!String(read("plex.account_token","")||"").trim();
    const embyTokenOk=!!String(read("emby.access_token","")||"").trim();
    if(prov==="plex"){
      if(!plexTokenOk) return setNote("sc-pms-note","Not connected to Plex. Go to Authentication → Plex.","err");
      if(!isValidServerUrl(srv)) return setNote("sc-pms-note","Plex Server is required (http(s)://…)","err");
    }else{
      if(!embyTokenOk) return setNote("sc-pms-note","Not connected to Emby. Go to Authentication → Emby.","err");
    }
    try{ await API.watch.start(prov); }catch{ setNote("sc-pms-note","Start failed","err"); }
    refreshWatcher();
  }
  async function onWatchStop(){ try{ await API.watch.stop(); }catch{ setNote("sc-pms-note","Stop failed","err"); } refreshWatcher(); }

  async function fetchServerUUID(){
    try{
      const prov=provider();
      const x=await API.serverUUID(),v=x?.server_uuid||x?.uuid||x?.id||"",inp=$("#sc-server-uuid",STATE.mount);
      if(inp&&v){
        inp.value=v;
        write("scrobble.watch.filters.server_uuid",v);
        if(prov==="emby") write("scrobble.watch.filters.user_id",v);
        setNote("sc-uuid-note",prov==="plex"?"Server UUID fetched":"User ID fetched");
      } else setNote("sc-uuid-note",prov==="plex"?"No server UUID":"No user ID","err");
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
    const cur=asArray(read("scrobble.watch.filters.username_whitelist",[]));
    if(!cur.includes(v)){ const next=[...cur,v]; write("scrobble.watch.filters.username_whitelist",next); $("#sc-whitelist",STATE.mount).append(chip(v,removeUserWatch,provider()==="emby"?onSelectWatchUser:undefined)); inp.value=""; }
  }
  function removeUserWatch(u){
    const cur=asArray(read("scrobble.watch.filters.username_whitelist",[])),next=cur.filter(x=>String(x)!==String(u));
    write("scrobble.watch.filters.username_whitelist",next);
    const host=$("#sc-whitelist",STATE.mount); host.innerHTML=""; next.forEach(v=>host.append(chip(v,removeUserWatch,provider()==="emby"?onSelectWatchUser:undefined)));
  }
  async function loadUsers(){
    try{
      const list=await API.users(),filtered=list.filter(u=>["managed","owner"].includes(String(u?.type||"").toLowerCase())||u?.owned===true||u?.isHomeUser===true||u?.IsAdministrator===true||u?.IsHidden===false||u?.IsDisabled===false);
      STATE.users = Array.isArray(list)?list:[];
      const names=filtered.map(u=>u?.username||u?.title||u?.Name||u?.name).filter(Boolean),host=$("#sc-whitelist",STATE.mount); let added=0;
      for(const n of names){ const cur=asArray(read("scrobble.watch.filters.username_whitelist",[])); if(!cur.includes(n)){ write("scrobble.watch.filters.username_whitelist",[...cur,n]); host.append(chip(n,removeUserWatch,provider()==="emby"?onSelectWatchUser:undefined)); added++; } }
      setNote("sc-users-note",added?`Loaded ${added} user(s)`:"No eligible users");
    }catch{ setNote("sc-users-note","Load users failed","err"); }
  }

  async function fetchServerUUIDWebhook(){
    try{
      const x=await j("/api/plex/server_uuid"),v=x?.server_uuid||x?.uuid||x?.id||"",inp=$("#sc-server-uuid-webhook",STATE.mount);
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
      const x=await j("/api/plex/users"); const list=Array.isArray(x)?x:Array.isArray(x?.users)?x.users:[];
      const filtered=list.filter(u=>["managed","owner"].includes(String(u?.type||"").toLowerCase())||u?.owned===true||u?.isHomeUser===true);
      const names=filtered.map(u=>u?.username||u?.title).filter(Boolean),host=$("#sc-whitelist-webhook",STATE.mount); let added=0;
      for(const n of names){ const cur=asArray(read("scrobble.webhook.filters_plex.username_whitelist",[])); if(!cur.includes(n)){ write("scrobble.webhook.filters_plex.username_whitelist",[...cur,n]); host.append(chip(n,removeUserWebhook)); added++; } }
      setNote("sc-users-note-webhook",added?`Loaded ${added} user(s)`:"No eligible managed/owner users");
    }catch{ setNote("sc-users-note-webhook","Load users failed","err"); }
  }

  async function hydrateEmby(){
    try{
      const info=await j("/api/emby/inspect");
      const server=String(info?.server||"").trim();
      const username=String(info?.username||info?.user?.Name||"").trim();
      const uid=String(info?.user_id||info?.user?.Id||"").trim();
      if(server){ write("emby.server",server); const inp=$("#sc-pms-input",STATE.mount); if(inp) inp.value=server; }
      if(username){
        const cur=asArray(read("scrobble.watch.filters.username_whitelist",[]));
        if(!cur.includes(username)){ write("scrobble.watch.filters.username_whitelist",[...cur,username]); const host=$("#sc-whitelist",STATE.mount); if(host) host.append(chip(username,removeUserWatch,onSelectWatchUser)); }
      }
      if(uid){
        const inp=$("#sc-server-uuid",STATE.mount); if(inp) inp.value=uid;
        write("scrobble.watch.filters.server_uuid",uid);
        write("scrobble.watch.filters.user_id",uid);
        setNote("sc-uuid-note","User ID detected");
      }
    }catch{}
  }

  function wire(){
    ensureHiddenServerInputs();

    on($("#sc-copy-plex",STATE.mount),"click",async()=>{ const ok=await copyText(`${location.origin}/webhook/plextrakt`); setNote("sc-endpoint-note",ok?"Plex endpoint copied":"Copy failed",ok?"":"err"); });
    on($("#sc-copy-jf",STATE.mount),"click",async()=>{ const ok=await copyText(`${location.origin}/webhook/jellyfintrakt`); setNote("sc-endpoint-note",ok?"Jellyfin endpoint copied":"Copy failed",ok?"":"err"); });
    on($("#sc-copy-emby",STATE.mount),"click",async()=>{ const ok=await copyText(`${location.origin}/webhook/embytrakt`); setNote("sc-endpoint-note",ok?"Emby endpoint copied":"Copy failed",ok?"":"err"); });

    on($("#sc-add-user",STATE.mount),"click",onAddUserWatch);
    on($("#sc-load-users",STATE.mount),"click",()=>{ loadUsers(); });
    on($("#sc-watch-start",STATE.mount),"click",onWatchStart);
    on($("#sc-watch-stop",STATE.mount),"click",onWatchStop);
    on($("#sc-watch-refresh",STATE.mount),"click",()=>{refreshWatcher(); try{ w.refreshWatchLogs?.(); }catch{}});
    on($("#sc-fetch-uuid",STATE.mount),"click",()=>{ fetchServerUUID(); });
    on($("#sc-server-uuid",STATE.mount),"input",e=>{
      const v=String(e.target.value||"").trim();
      write("scrobble.watch.filters.server_uuid",v);
      if(provider()==="emby") write("scrobble.watch.filters.user_id",v);
    });

    on($("#sc-add-user-webhook",STATE.mount),"click",onAddUserWebhook);
    on($("#sc-load-users-webhook",STATE.mount),"click",loadUsersWebhook);
    on($("#sc-fetch-uuid-webhook",STATE.mount),"click",fetchServerUUIDWebhook);
    on($("#sc-server-uuid-webhook",STATE.mount),"input",e=>write("scrobble.webhook.filters_plex.server_uuid",String(e.target.value||"").trim()));

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

    const wh=$("#sc-enable-webhook",STATE.mount),wa=$("#sc-enable-watcher",STATE.mount),pv=$("#sc-provider",STATE.mount);
    const syncExclusive=async src=>{
      const webOn=!!wh?.checked,watOn=!!wa?.checked;
      if(src==="webhook"&&webOn&&wa) wa.checked=false;
      if(src==="watch"&&watOn&&wh) wh.checked=false;
      write("scrobble.enabled",(!!wh?.checked)|| (!!wa?.checked));
      write("scrobble.mode",(!!wa?.checked)?"watch":"webhook");
      if(src==="watch" && !wa.checked){
        try{ await API.watch.stop(); }catch{}
        write("scrobble.watch.autostart",false);
        const auto=$("#sc-autostart",STATE.mount); if(auto) auto.checked=false;
      }
      applyModeDisable();
    };
    if(wh) on(wh,"change",()=>syncExclusive("webhook"));
    if(wa) on(wa,"change",()=>syncExclusive("watch"));
    on($("#sc-autostart",STATE.mount),"change",e=>write("scrobble.watch.autostart",!!e.target.checked));
    on(pv,"change",e=>{ const val=String(e.target.value||"plex").toLowerCase(); write("scrobble.watch.provider",val); populate(); if(val==="emby") hydrateEmby(); });

    on($("#sc-pms-refresh",STATE.mount),"click",()=>{ if(provider()==="plex") loadPmsList(); });
    on($("#sc-pms-select",STATE.mount),"change",e=>{ if(provider()!=="plex") return; const v=String(e.target.value||"").trim(); if(v){ $("#sc-pms-input",STATE.mount).value=v; write("plex.server_url",v); setNote("sc-pms-note",`Using ${v}`);} applyModeDisable();});
    on($("#sc-pms-input",STATE.mount),"input",e=>{
      const v=String(e.target.value||"").trim();
      if(provider()==="plex"){ write("plex.server_url",v); if(v&&!isValidServerUrl(v)) setNote("sc-pms-note","Invalid URL. Use http(s)://host[:port]","err"); else if(v) setNote("sc-pms-note",`Using ${v}`); else setNote("sc-pms-note","Plex Server is required when Watcher is enabled","err"); }
      else{ write("emby.server",v); setNote("sc-pms-note", v?`Using ${v}`:""); }
      applyModeDisable();
    });

    on($("#sc-delete-plex-webhook",STATE.mount),"change",e=>{
      const v=!!e.target.checked;
      write("scrobble.delete_plex",v);
      const other=$("#sc-delete-plex-watch",STATE.mount); if(other) other.checked=v;
    });
    on($("#sc-delete-plex-watch",STATE.mount),"change",e=>{
      const v=!!e.target.checked;
      write("scrobble.delete_plex",v);
      const other=$("#sc-delete-plex-webhook",STATE.mount); if(other) other.checked=v;
    });
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
    buildUI(); wire(); populate(); refreshWatcher(); if(provider()==="plex") loadPmsList().catch(()=>{}); if(provider()==="emby") hydrateEmby();
  }

  function mountLegacy(targetEl,cfg){ init({ mountId: targetEl?.id, cfg: cfg||(w._cfgCache||{}) }); }

  function getScrobbleConfig(){
    commitAdvancedInputsWatch();
    commitAdvancedInputsWebhook();
    commitAdvancedInputsTrakt();

    const enabled=!!read("scrobble.enabled",false),mode=String(read("scrobble.mode","webhook")).toLowerCase();

    const wlWeb = namesFromChips("#sc-whitelist-webhook");
    const suWeb = String($("#sc-server-uuid-webhook",STATE.mount)?.value ?? read("scrobble.webhook.filters_plex.server_uuid","")).trim();

    const wlWatch = namesFromChips("#sc-whitelist");
    const suWatch = String($("#sc-server-uuid",STATE.mount)?.value ?? read("scrobble.watch.filters.server_uuid","")).trim();
    const userIdWatch = String(read("scrobble.watch.filters.user_id","")||"").trim();

    return {
      enabled,
      mode: mode==="watch"?"watch":"webhook",
      delete_plex: !!read("scrobble.delete_plex", false),
      delete_plex_types: read("scrobble.delete_plex_types", ["movie"]),
      webhook:{
        pause_debounce_seconds:read("scrobble.webhook.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds),
        suppress_start_at:read("scrobble.webhook.suppress_start_at",DEFAULTS.watch.suppress_start_at),
        filters_plex:{ username_whitelist: wlWeb.length?wlWeb:asArray(read("scrobble.webhook.filters_plex.username_whitelist",[])), server_uuid: suWeb||"" },
        filters_jellyfin: read("scrobble.webhook.filters_jellyfin",{}) || { username_whitelist: [] }
      },
      watch:{
        provider: provider(),
        autostart:!!read("scrobble.watch.autostart",false),
        pause_debounce_seconds:read("scrobble.watch.pause_debounce_seconds",DEFAULTS.watch.pause_debounce_seconds),
        suppress_start_at:read("scrobble.watch.suppress_start_at",DEFAULTS.watch.suppress_start_at),
        filters:{ username_whitelist: wlWatch.length?wlWatch:asArray(read("scrobble.watch.filters.username_whitelist",[])), server_uuid: suWatch||"", user_id: userIdWatch || (provider()==="emby"?suWatch:"") }
      },
      trakt:{
        stop_pause_threshold:read("scrobble.trakt.stop_pause_threshold",DEFAULTS.trakt.stop_pause_threshold),
        force_stop_at:read("scrobble.trakt.force_stop_at",DEFAULTS.trakt.force_stop_at),
        regress_tolerance_percent:read("scrobble.trakt.regress_tolerance_percent",DEFAULTS.trakt.regress_tolerance_percent)
      }
    };
  }

  const getRootPatch=()=>({
    plex:{ server_url:String(read("plex.server_url","")||"") },
    emby:{ server:String(read("emby.server","")||"") }
  });

  w.ScrobUI={ $, $all, el, on, setNote, injectStyles, DEFAULTS, STATE, read, write, asArray, clamp100, norm100, API };
  w.Scrobbler={ init, mount:mountLegacy, getConfig:getScrobbleConfig, getRootPatch };
  w.getScrobbleConfig=getScrobbleConfig; w.getRootPatch=getRootPatch;

  d.addEventListener("DOMContentLoaded", async ()=>{
    const root=d.getElementById("scrobble-mount"); if(!root) return;
    let cfg=null; try{ cfg=await API.cfgGet(); }catch{ cfg=w._cfgCache||{}; }
    init({ mountId:"scrobble-mount", cfg });
  });
})(window, document);
