// modals.js - Configure Connection modal

// Get modal state
function _cxGetState() {
  const el = document.getElementById('cx-modal');
  return (el && el.__state) || window.__cxState || null;
}

function openPairModal(){const m=document.getElementById("pairModal");if(m)m.classList.remove("hidden")}
function closePairModal(){const m=document.getElementById("pairModal");if(m)m.classList.add("hidden")}


function openUpdateModal(){
  try{
    if(typeof _updInfo === 'undefined' || !_updInfo) return;
    const m = document.getElementById('upd-modal'); if(!m) return;
    m.classList.remove('hidden');
    const t = document.getElementById('upd-title'); if(t) t.textContent = `v${_updInfo.latest}`;
    const n = document.getElementById('upd-notes'); if(n) n.textContent = _updInfo.notes || "(No release notes)";
    const l = document.getElementById('upd-link'); if(l) l.href = _updInfo.url || "#";
  }catch(_){}
}
function closeUpdateModal(){
  const m = document.getElementById('upd-modal');
  if(m) m.classList.add('hidden');
}
function dismissUpdate(){
  try{
    if(typeof _updInfo !== 'undefined' && _updInfo?.latest){
      localStorage.setItem("dismissed_version", _updInfo.latest);
    }
  }catch(_){}
  const pill = document.getElementById("upd-pill");
  if(pill) pill.classList.add("hidden");
  closeUpdateModal();
}

async function openAbout(){
  try{
    // --- App version ---
    const r = await fetch("/api/version?cb=" + Date.now(), { cache: "no-store" });
    const j = r.ok ? await r.json() : {};
    const cur = (j.current ?? "0.0.0").toString().trim();
    const latest = (j.latest ?? "").toString().trim() || null;
    const url = j.html_url || "https://github.com/cenodude/crosswatch/releases";
    const upd = !!j.update_available;

    document.getElementById("about-version")?.append?.();
    const verEl = document.getElementById("about-version");
    if (verEl){ verEl.textContent = `Version ${j.current}`; verEl.dataset.version = cur; }

    const headerVer = document.getElementById("app-version");
    if (headerVer){ headerVer.textContent = `Version ${cur}`; headerVer.dataset.version = cur; }

    const relEl = document.getElementById("about-latest");
    if (relEl){
      relEl.href = url;
      relEl.textContent = latest ? `v${latest}` : "Releases";
      relEl.setAttribute("aria-label", latest ? `Latest release v${latest}` : "Releases");
    }

    const updEl = document.getElementById("about-update");
    if (updEl){
      updEl.classList.add("badge","upd");
      if (upd && latest){
        updEl.textContent = `Update ${latest} available`;
        updEl.classList.remove("hidden","reveal"); void updEl.offsetWidth; updEl.classList.add("reveal");
      } else {
        updEl.textContent = ""; updEl.classList.add("hidden"); updEl.classList.remove("reveal");
      }
    }

    // --- Module versions (idempotent + pretty) ---
    try{
      const mr = await fetch("/api/modules/versions?cb=" + Date.now(), { cache: "no-store" });
      if (!mr.ok) throw new Error(String(mr.status));
      const mv = await mr.json();

      const body = document.querySelector('#about-backdrop .modal-card .modal-body');
      const firstGrid = body?.querySelector('.about-grid');
      if (!body || !firstGrid) throw new Error("about body/grid missing");

      // Remove previous render so it never duplicates
      body.querySelector("#about-mods")?.remove();

      // Ensure styles once
      if (!document.getElementById("about-mods-style")){
        const style = document.createElement("style");
        style.id = "about-mods-style";
        style.textContent = `
          .mods-card{margin-top:16px;border:1px solid rgba(255,255,255,.12);
            border-radius:12px;padding:12px;background:rgba(255,255,255,.03)}
          .mods-header{font-weight:600;opacity:.9;margin:0 0 8px 2px}
          .mods-grid{display:grid;grid-template-columns:minmax(120px,160px) 1fr auto;
            gap:8px 12px;align-items:center}
          .mods-group{grid-column:1/-1;margin-top:8px;font-weight:600;opacity:.8;
            border-top:1px solid rgba(255,255,255,.08);padding-top:8px}
          .mods-name{opacity:.9}
          .mods-key{opacity:.7}
          .mods-ver{justify-self:end;font-variant-numeric:tabular-nums;opacity:.95}
          @media (max-width:520px){ .mods-grid{grid-template-columns:1fr auto} .mods-key{display:none} }
        `;
        document.head.appendChild(style);
      }

      // Build card
      const wrap = document.createElement("div");
      wrap.id = "about-mods";
      wrap.className = "mods-card";
      wrap.innerHTML = `
        <div class="mods-header">Modules</div>
        <div class="mods-grid" role="table" aria-label="Module versions"></div>
      `;
      const grid = wrap.querySelector(".mods-grid");

      const addGroup = (label) => {
        const g = document.createElement("div");
        g.className = "mods-group";
        g.textContent = label;
        grid.appendChild(g);
      };
      const addRow = (label, key, ver) => {
        const n = document.createElement("div"); n.className = "mods-name"; n.textContent = label.replace(/^_+/, "");
        const k = document.createElement("div"); k.className = "mods-key";  k.textContent = key;
        const v = document.createElement("div"); v.className = "mods-ver";  v.textContent = ver ? `v${ver}` : "v0.0.0";
        grid.appendChild(n); grid.appendChild(k); grid.appendChild(v);
      };

      const groups = mv?.groups || {};
      addGroup("Authentication Providers");
      Object.entries(groups.AUTH || {}).forEach(([name, ver]) => addRow(name, name, ver));
      addGroup("Synchronization Providers");
      Object.entries(groups.SYNC || {}).forEach(([name, ver]) => addRow(name, name, ver));


      // Insert right after the first about-grid
      firstGrid.insertAdjacentElement("afterend", wrap);
    }catch(e){
      console.warn("[about] modules render failed", e);
    }

  }catch(e){
    console.warn("[about] openAbout failed", e);
  }

  document.getElementById("about-backdrop")?.classList.remove("hidden");
}

function closeAbout(ev){
  if (ev && ev.type === "click" && ev.currentTarget !== ev.target) return;
  document.getElementById("about-backdrop")?.classList.add("hidden");
}


// Ensure: create once, hydrate, wire sticky bridge
async function cxEnsureCfgModal() {
  // de-dup
  const dups=[...document.querySelectorAll('#cx-modal')]; for(let i=1;i<dups.length;i++){try{dups[i].remove();}catch(_){}}

  let wrap=document.querySelector('#cx-modal');
  if(!wrap){
    wrap=document.createElement('div');
    wrap.id='cx-modal';
    wrap.className='modal-backdrop cx-wide hidden';
    wrap.innerHTML=`
      <div class="modal-shell">
        <div class="cx-card">
          <div class="cx-head">
            <div class="title-wrap">
              <div class="app-logo"></div>
              <div>
                <div class="app-name">Configure Connection</div>
                <div class="app-sub">Choose source &rarr; target and what to sync</div>
              </div>
            </div>
            <label class="switch big head-toggle" title="Enable/Disable connection">
              <input type="checkbox" id="cx-enabled" checked>
              <span class="slider" aria-hidden="true"></span>
              <span class="lab on" aria-hidden="true">Enabled</span>
              <span class="lab off" aria-hidden="true">Disabled</span>
            </label>
          </div>

          <div class="cx-body">
            <div class="cx-top grid">
              <div class="top-left">
                <div class="cx-row">
                  <div class="field">
                    <label>Source</label>
                    <div id="cx-src-display" class="input static"></div>
                    <select id="cx-src" class="hidden"></select>
                  </div>
                  <div class="field">
                    <label>Target</label>
                    <div id="cx-dst-display" class="input static"></div>
                    <select id="cx-dst" class="hidden"></select>
                  </div>
                </div>

                <div class="cx-row cx-mode-row">
                  <div class="seg">
                    <input type="radio" name="cx-mode" id="cx-mode-one" value="one" />
                    <label for="cx-mode-one">One-way</label>
                    <input type="radio" name="cx-mode" id="cx-mode-two" value="two" />
                    <label for="cx-mode-two">Two-way</label>
                  </div>
                </div>

                <div class="cx-row">
                  <div id="cx-feat-tabs" class="feature-tabs"></div>
                </div>
              </div>

              <div class="top-right">
                <div class="flow-card">
                  <div class="flow-title">Sync flow: <span id="cx-flow-title">One-way</span></div>
                  <div class="flow-rail pretty" id="cx-flow-rail">
                    <span class="token" id="cx-flow-src">-</span>
                    <span class="arrow">
                      <span class="dot flow a"></span>
                      <span class="dot flow b"></span>
                    </span>
                    <span class="token" id="cx-flow-dst">-</span>
                  </div>
                </div>
              </div>
            </div>

            <div class="cx-main">
              <div class="left"><div class="panel" id="cx-feat-panel"></div></div>
              <div class="right">
                <div class="panel rules-card" id="cx-right-panel">
                  <div class="panel-title" id="cx-right-title">Rule preview</div>
                  <div class="sub" id="cx-right-sub">These rules are generated from your selections.</div>
                  <div class="rules" id="cx-rules">
                    <div class="r note" id="cx-notes">Pick a feature.</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <!-- No footer buttons; sticky bar handles Save/Cancel -->
        </div>
      </div>`;
    document.body.appendChild(wrap);

    // state
    wrap.__state={
      providers:[], src:null, dst:null, feature:'globals',
      options:{
        watchlist:{enable:true,add:true,remove:false},
        ratings:{enable:false,add:false,remove:false},
        history:{enable:false,add:false,remove:false},
        playlists:{enable:false,add:false,remove:false}
      },
      globals:null,
      visited:new Set()
    };
    window.__cxState=wrap.__state;
    if(typeof cxBindCfgEvents==='function') cxBindCfgEvents();
  }

  // helpers on the modal
  wrap.__persistCurrentFeature = function persistCurrentFeature(){
    const state=wrap.__state||{};
    const f=state.feature;
    const pick=(on,add,rem)=>({enable:!!on?.checked,add:!!add?.checked,remove:!!rem?.checked});
    if(f==='watchlist') state.options.watchlist=pick(document.getElementById('cx-wl-enable'),document.getElementById('cx-wl-add'),document.getElementById('cx-wl-remove'));
    if(f==='ratings')   state.options.ratings  =pick(document.getElementById('cx-rt-enable'),document.getElementById('cx-rt-add'),document.getElementById('cx-rt-remove'));
    if(f==='history')   state.options.history  =pick(document.getElementById('cx-hs-enable'),document.getElementById('cx-hs-add'),document.getElementById('cx-hs-remove'));
    if(f==='playlists') state.options.playlists=pick(document.getElementById('cx-pl-enable'),document.getElementById('cx-pl-add'),document.getElementById('cx-pl-remove'));
    state.visited.add(f);
  };

  wrap.__buildPairPayload = function buildPairPayload(){
    const st=wrap.__state||{};
    const src=st.src || document.getElementById('cx-src')?.value || document.getElementById('cx-src-display')?.dataset.value || '';
    const dst=st.dst || document.getElementById('cx-dst')?.value || document.getElementById('cx-dst-display')?.dataset.value || '';
    const modeTwo=!!document.getElementById('cx-mode-two')?.checked;
    const enabled=!!document.getElementById('cx-enabled')?.checked;
    const get=(k)=>Object.assign({enable:false,add:false,remove:false},(st.options||{})[k]||{});
    const payload={
      source:src,target:dst,enabled,
      mode:modeTwo?'two-way':'one-way',
      features:{
        watchlist:get('watchlist'),
        ratings:get('ratings'),
        history:get('history'),
        playlists:get('playlists')
      }
    };
    const eid=(wrap.dataset && wrap.dataset.editingId) ? (wrap.dataset.editingId||'').trim() : '';
    if(eid) payload.id=eid;
    return payload;
  };

  // Globals: safe read-modify-write
  wrap.__saveGlobalsIfPresent = async function saveGlobalsIfPresent(){
    try{
      if(document.getElementById('gl-dry')){
        const sync={
          dry_run:!!document.getElementById('gl-dry')?.checked,
          verify_after_write:!!document.getElementById('gl-verify')?.checked,
          drop_guard:!!document.getElementById('gl-drop')?.checked,
          allow_mass_delete:!!document.getElementById('gl-mass')?.checked,
          tombstone_ttl_days:Math.max(0,parseInt(document.getElementById('gl-ttl')?.value||'0',10)||0),
          include_observed_deletes:!!document.getElementById('gl-observed')?.checked
        };
        const curResp=await fetch('/api/config',{cache:'no-store'});
        const current=curResp.ok?await curResp.json():{};
        const cfg=(typeof structuredClone==='function')?structuredClone(current||{}):JSON.parse(JSON.stringify(current||{}));
        cfg.sync=Object.assign({},cfg.sync||{},sync);
        const saveResp=await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
        if(!saveResp.ok) throw new Error(`POST /api/config ${saveResp.status}`);
      }
    }catch(e){console.warn('[cx] saving globals failed',e);}
  };


  // Snapshot all feature toggles from the DOM (only overrides if inputs exist)
  wrap.__snapshotAll = function snapshotAll(){
    const st = wrap.__state || {};
    const pick = (on,add,rem)=>({enable:!!on?.checked, add:!!add?.checked, remove:!!rem?.checked});

    const wl_on=document.getElementById('cx-wl-enable'),
          wl_add=document.getElementById('cx-wl-add'),
          wl_rem=document.getElementById('cx-wl-remove');
    if(wl_on||wl_add||wl_rem) st.options.watchlist = pick(wl_on, wl_add, wl_rem);

    const rt_on=document.getElementById('cx-rt-enable'),
          rt_add=document.getElementById('cx-rt-add'),
          rt_rem=document.getElementById('cx-rt-remove');
    if(rt_on||rt_add||rt_rem) st.options.ratings   = pick(rt_on, rt_add, rt_rem);

    const hs_on=document.getElementById('cx-hs-enable'),
          hs_add=document.getElementById('cx-hs-add'),
          hs_rem=document.getElementById('cx-hs-remove');
    if(hs_on||hs_add||hs_rem) st.options.history   = pick(hs_on, hs_add, hs_rem);

    const pl_on=document.getElementById('cx-pl-enable'),
          pl_add=document.getElementById('cx-pl-add'),
          pl_rem=document.getElementById('cx-pl-remove');
    if(pl_on||pl_add||pl_rem) st.options.playlists = pick(pl_on, pl_add, pl_rem);
  };

  // Save: only via window.cxSavePair to protect config.json, then close on success
  wrap.__doSave = async function doSave(){
    if(wrap.__saving) return;
    wrap.__saving = true;
    try{
      // Capture latest UI state
      wrap.__persistCurrentFeature && wrap.__persistCurrentFeature();
      wrap.__snapshotAll && wrap.__snapshotAll();

      const payload = wrap.__buildPairPayload ? wrap.__buildPairPayload() : null;
      if(!payload){ console.warn('[cx] no payload to save'); return; }

      // Persist globals safely (if visible)
      await (wrap.__saveGlobalsIfPresent && wrap.__saveGlobalsIfPresent());

      // Persist pair through host handler (authoritative writer to config.json)
      if(typeof window.cxSavePair === 'function'){
        const res = await Promise.resolve(window.cxSavePair(payload, payload.id||''));
        // If host returns a boolean or {ok}, treat truthy as success
        const ok = (typeof res === 'object') ? (res?.ok !== false) : (res !== false);
        if(ok) cxCloseModal(true);
        else console.warn('[cx] cxSavePair indicated failure; modal left open for review.');
      }else{
        console.warn('[cx] cxSavePair missing; not writing pairs to config.json');
      }
    }catch(err){
      console.warn('[cx] save failed:', err);
    }finally{
      wrap.__saving = false;
    }
  };


  // Feature gating + defaults
  const __TEMP_DISABLED=new Set(["ratings","history","playlists"]);
  const DEFAULT_GLOBALS={dry_run:false,verify_after_write:false,drop_guard:false,allow_mass_delete:true,tombstone_ttl_days:30,include_observed_deletes:true};
  const state=wrap.__state;
  state.globals={...DEFAULT_GLOBALS};

  // Hydrate globals from config
  try{
    const cfg=await fetch('/api/config',{cache:'no-store'}).then(r=>r.json());
    const s=cfg?.sync||{};
    state.globals={
      dry_run:!!s.dry_run,
      verify_after_write:!!s.verify_after_write,
      drop_guard:!!s.drop_guard,
      allow_mass_delete:!!s.allow_mass_delete,
      tombstone_ttl_days:Number.isFinite(s.tombstone_ttl_days)?s.tombstone_ttl_days:DEFAULT_GLOBALS.tombstone_ttl_days,
      include_observed_deletes:!!s.include_observed_deletes
    };
  }catch(_){}

  // Providers
  async function loadProviders(){
    try{
      const res=await fetch('/api/sync/providers');
      const list=await res.json();
      state.providers=Array.isArray(list)?list:[];
    }catch(e){
      console.warn('Failed to load /api/sync/providers',e);
      state.providers=[
        {name:'PLEX',label:'Plex',features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true}},
        {name:'SIMKL',label:'Simkl',features:{watchlist:true,ratings:true,history:true,playlists:false},capabilities:{bidirectional:true}},
        {name:'TRAKT',label:'Trakt',features:{watchlist:true,ratings:true,history:true,playlists:true},capabilities:{bidirectional:true}},
      ];
    }
  }
  const byName=(n)=>state.providers.find(p=>p.name===n);
  const commonFeatures=()=>!state.src||!state.dst?[]:['watchlist','ratings','history','playlists'].filter(k=>byName(state.src)?.features?.[k]&&byName(state.dst)?.features?.[k]);
  const defaultForFeature=(k)=>k==='watchlist'?{enable:true,add:true,remove:false}:k==='playlists'?{enable:false,add:true,remove:false}:{enable:false,add:false,remove:false};

  function getOpts(key){
    if(!state.visited.has(key)){
      state.options[key]=state.options[key]??defaultForFeature(key);
      state.visited.add(key);
    }
    return state.options[key];
  }

  // UI renderers
  function renderProviderSelects(){
    const srcSel=wrap.querySelector('#cx-src');
    const dstSel=wrap.querySelector('#cx-dst');
    const srcLab=wrap.querySelector('#cx-src-display');
    const dstLab=wrap.querySelector('#cx-dst-display');

    const opts=state.providers.map(p=>`<option value="${p.name}">${p.label}</option>`).join('');
    srcSel.innerHTML=`<option value="">Select…</option>${opts}`;
    dstSel.innerHTML=`<option value="">Select…</option>${opts}`;

    if(state.src) srcSel.value=state.src;
    if(state.dst) dstSel.value=state.dst;

    const updateLabels=()=>{
      const sObj=byName(srcSel.value), dObj=byName(dstSel.value);
      if(srcLab){srcLab.textContent=sObj?.label||'—'; srcLab.dataset.value=srcSel.value||'';}
      if(dstLab){dstLab.textContent=dObj?.label||'—'; dstLab.dataset.value=dstSel.value||'';}
    };

    srcSel.onchange=()=>{ state.src=srcSel.value||null; updateLabels(); updateFlow(true); refreshTabs(); updateFlowRailLogos?.(); };
    dstSel.onchange=()=>{ state.dst=dstSel.value||null; updateLabels(); updateFlow(true); refreshTabs(); updateFlowRailLogos?.(); };

    updateLabels();
  }

  // Flow animation: toggle classes to restart CSS animation
  function restartFlowAnimation(mode){
    const rail=wrap.querySelector('#cx-flow-rail');
    if(!rail) return;
    const arrow=rail.querySelector('.arrow');
    const dots=[...rail.querySelectorAll('.dot.flow')];
    // reset classes to re-trigger CSS animations
    rail.classList.remove('anim-one','anim-two');
    arrow?.classList.remove('anim-one','anim-two');
    dots.forEach(d=>d.classList.remove('anim-one','anim-two'));
    void rail.offsetWidth; // reflow
    const animCls=mode==='two'?'anim-two':'anim-one';
    rail.classList.add(animCls);
    arrow?.classList.add(animCls);
    dots.forEach(d=>d.classList.add(animCls));
  }

  function updateFlow(animate=false){
    wrap.querySelector('#cx-flow-src').textContent=byName(state.src)?.label||'—';
    wrap.querySelector('#cx-flow-dst').textContent=byName(state.dst)?.label||'—';
    const two=wrap.querySelector('#cx-mode-two');
    const ok=(byName(state.src)?.capabilities?.bidirectional)&&(byName(state.dst)?.capabilities?.bidirectional);
    two.disabled=!ok;
    if(!ok&&two.checked){wrap.querySelector('#cx-mode-one').checked=true;}
    if(two.nextElementSibling) two.nextElementSibling.classList.toggle('disabled',!ok);
    const tEl=wrap.querySelector('#cx-flow-title'); if(tEl) tEl.textContent=(wrap.querySelector('#cx-mode-two').checked?'Two-way (bidirectional)':'One-way');
    updateFlowClasses();
    if(animate) restartFlowAnimation(wrap.querySelector('#cx-mode-two')?.checked?'two':'one');
    updateFlowRailLogos?.();
  }

  function updateFlowClasses(){
    const rail=wrap.querySelector('#cx-flow-rail'); if(!rail) return;
    const two=wrap.querySelector('#cx-mode-two')?.checked;
    const enabled=!!document.getElementById('cx-enabled')?.checked;
    const wl=state.options.watchlist||defaultForFeature('watchlist');
    rail.className='flow-rail pretty';
    rail.classList.toggle('mode-two',!!two);
    rail.classList.toggle('mode-one',!two);
    try{ restartFlowAnimation && restartFlowAnimation(two?'two':'one'); }catch(_){}
    const flowOn=enabled&&wl.enable&&(two?(wl.add||wl.remove):(wl.add||wl.remove));
    rail.classList.toggle('off',!flowOn);
    if(two){rail.classList.toggle('active',flowOn);}else{rail.classList.toggle('dir-add',flowOn&&wl.add);rail.classList.toggle('dir-remove',flowOn&&!wl.add&&wl.remove);}
  }

  function refreshTabs(){
    const tabs=wrap.querySelector("#cx-feat-tabs"); if(!tabs) return;
    tabs.innerHTML="";
    const items=["globals",...commonFeatures()];
    if(items.length===0){ tabs.innerHTML='<div class="ftab disabled">No common features</div>'; renderFeaturePanel(); return; }

    const LABELS={globals:"Globals",watchlist:"Watchlist",ratings:"Ratings",history:"History",playlists:"Playlists"};
    items.forEach((k)=>{
      const btn=document.createElement("button");
      btn.className="ftab"; btn.dataset.key=k; btn.textContent=LABELS[k]||(k[0]?.toUpperCase?.()+k.slice(1));
      if(__TEMP_DISABLED.has(k)){ btn.classList.add("disabled"); btn.setAttribute("aria-disabled","true"); btn.title="Coming soon"; }
      else { btn.onclick=()=>{ wrap.__persistCurrentFeature(); state.feature=k; refreshTabs(); renderFeaturePanel(); }; }
      if(state.feature===k) btn.classList.add("active");
      tabs.appendChild(btn);
    });

    if(!items.includes(state.feature) || __TEMP_DISABLED.has(state.feature)){
      state.feature=items.find((k)=>k!=="globals"&&!__TEMP_DISABLED.has(k))||"globals";
    }
    [...tabs.children].forEach((c)=>c.classList.toggle("active",c.dataset.key===state.feature));
    renderFeaturePanel();
  }

  function ensureRulesCard(){
    const rp=wrap.querySelector("#cx-right-panel"); if(!rp) return;
    rp.classList.add("rules-card");
    rp.innerHTML=`
      <div class="panel-title" id="cx-right-title">Rule preview</div>
      <div class="sub" id="cx-right-sub">These rules are generated from your selections.</div>
      <div class="rules" id="cx-rules">
        <div class="r note" id="cx-notes">Pick a feature.</div>
      </div>`;
  }

  function renderFeaturePanel(){
    const left=wrap.querySelector("#cx-feat-panel");
    const right=wrap.querySelector("#cx-right-panel");
    if(!left||!right) return;

    if(state.feature==="globals"){
      const g=state.globals||{};
      left.innerHTML=`
        <div class="panel-title">Global sync options</div>
        <div class="opt-row"><label for="gl-dry">Dry run</label><label class="switch"><input id="gl-dry" type="checkbox" ${g.dry_run?"checked":""}><span class="slider"></span></label></div>
        <div class="muted">Simulate changes only; do not write to providers.</div>
        <div class="opt-row"><label for="gl-verify">Verify after write</label><label class="switch"><input id="gl-verify" type="checkbox" ${g.verify_after_write?"checked":""}><span class="slider"></span></label></div>
        <div class="muted">Re-check items after writes.</div>
        <div class="opt-row"><label for="gl-drop">Drop guard</label><label class="switch"><input id="gl-drop" type="checkbox" ${g.drop_guard?"checked":""}><span class="slider"></span></label></div>
        <div class="muted">Protect against empty source snapshots.</div>
        <div class="opt-row"><label for="gl-mass">Allow mass delete</label><label class="switch"><input id="gl-mass" type="checkbox" ${g.allow_mass_delete?"checked":""}><span class="slider"></span></label></div>
        <div class="muted">Permit bulk removals when required.</div>`;
      right.classList.remove("rules-card");
      right.innerHTML=`
        <div class="panel-title">More options</div>
        <div class="opt-row"><label for="gl-ttl">Tombstone TTL (days)</label><input id="gl-ttl" class="input" type="number" min="0" step="1" value="${g.tombstone_ttl_days??30}"></div>
        <div class="muted">Keep delete markers to avoid re-adding.</div>
        <div class="opt-row"><label for="gl-observed">Include observed deletes</label><label class="switch"><input id="gl-observed" type="checkbox" ${g.include_observed_deletes?"checked":""}><span class="slider"></span></label></div>
        <div class="muted">Apply deletions detected from activity.</div>`;
      return;
    }

    ensureRulesCard();

    if(__TEMP_DISABLED.has(state.feature)){
      left.innerHTML=`
        <div class="panel-title">${state.feature[0].toUpperCase()+state.feature.slice(1)} options</div>
        <div class="muted" style="padding:18px 0;">Temporarily unavailable. This will be enabled in a future update.</div>`;
      const title=wrap.querySelector("#cx-right-title");
      const sub=wrap.querySelector("#cx-right-sub");
      const rules=wrap.querySelector("#cx-rules");
      if(title) title.textContent="Rule preview";
      if(sub) sub.textContent="Feature disabled · coming soon";
      if(rules) rules.innerHTML="";
      return;
    }

    if(state.feature==="watchlist"){
      const wl=getOpts("watchlist");
      left.innerHTML=`
        <div class="panel-title">Watchlist options</div>
        <div class="opt-row"><label for="cx-wl-enable">Enable</label><label class="switch"><input id="cx-wl-enable" type="checkbox" ${wl.enable?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-wl-add">Add</label><label class="switch"><input id="cx-wl-add" type="checkbox" ${wl.add?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-wl-remove">Remove</label><label class="switch"><input id="cx-wl-remove" type="checkbox" ${wl.remove?"checked":""}><span class="slider"></span></label></div>
        <div class="panel sub"><div class="panel-title small">Advanced</div><ul class="adv"><li>Filter by type</li><li>Skip duplicates</li><li>Prefer target metadata</li><li>Conflict policy</li></ul></div>`;
      const wlRem=document.getElementById("cx-wl-remove");
      if(wlRem){wlRem.disabled=!wl.enable;if(!wl.enable&&wlRem.checked) wlRem.checked=false;}
      updateRules(); return;
    }

    left.innerHTML=`<div class="panel-title">${state.feature[0].toUpperCase()+state.feature.slice(1)} options</div><div class="muted">Configuration for this feature will be available soon.</div>`;
    const notes=wrap.querySelector("#cx-notes"); if(notes) notes.textContent=`Selected: ${state.feature}.`;
  }

  function updateRules(){
    if(state.feature==='globals') return;
    const rules=wrap.querySelector('#cx-rules'); if(!rules) return;
    rules.innerHTML='';
    const push=(t,s='')=>{const d=document.createElement('div'); d.className='r'; d.innerHTML=`<div class="t">${t}</div>${s?`<div class="s">${s}</div>`:''}`; rules.appendChild(d);};
    const two=wrap.querySelector('#cx-mode-two')?.checked;
    if(state.feature==='watchlist'){
      const wl=state.options.watchlist||defaultForFeature('watchlist');
      wrap.querySelector('#cx-notes')?.remove();
      push(`Watchlist • ${two?'two-way':'one-way'}`, wl.enable?'enabled':'disabled');
      if(wl.add)    push('Add • new items', two?'both directions':'from source → target');
      if(wl.remove) push('Delete • when removed', two?'both directions':'from source → target');
      return;
    }
    if(state.feature==='ratings'){
      const rt=state.options.ratings||defaultForFeature('ratings');
      wrap.querySelector('#cx-notes')?.remove();
      push(`Ratings • ${two?'two-way':'one-way'}`, rt.enable?'enabled':'disabled');
      if(rt.add)    push('Add/Update • ratings', two?'both directions':'from source → target');
      if(rt.remove) push('Remove • cleared ratings', two?'both directions':'from source → target');
      return;
    }
    if(state.feature==='history'){
      const hs=state.options.history||defaultForFeature('history');
      wrap.querySelector('#cx-notes')?.remove();
      push(`History • ${two?'two-way':'one-way'}`, hs.enable?'enabled':'disabled');
      if(hs.add)    push('Add • new plays', two?'both directions':'from source → target');
      if(hs.remove) push('Remove • deleted plays', two?'both directions':'from source → target');
      return;
    }
    if(state.feature==='playlists'){
      const pl=state.options.playlists||defaultForFeature('playlists');
      wrap.querySelector('#cx-notes')?.remove();
      push(`Playlists • ${two?'two-way':'one-way'}`, pl.enable?'enabled':'disabled');
      if(pl.add)    push('Add/Update • playlist items', two?'both directions':'from source → target');
      if(pl.remove) push('Remove • items not present', two?'both directions':'from source → target');
      return;
    }
    const notes=wrap.querySelector('#cx-notes'); if(notes) notes.textContent=`Selected: ${state.feature}.`;
  }

  // Changes watcher -> refresh flow + rules + animation
  wrap.addEventListener('change',(e)=>{
    const id=e.target.id;
    const map={"cx-wl-enable":"cx-wl-remove","cx-rt-enable":"cx-rt-remove","cx-hs-enable":"cx-hs-remove","cx-pl-enable":"cx-pl-remove"};
    if(map[id]){const rm=document.getElementById(map[id]); if(rm){rm.disabled=!e.target.checked; if(!e.target.checked) rm.checked=false;}}
    if(id.startsWith('cx-wl-')){state.options.watchlist={enable:!!document.getElementById('cx-wl-enable')?.checked,add:!!document.getElementById('cx-wl-add')?.checked,remove:!!document.getElementById('cx-wl-remove')?.checked};state.visited.add('watchlist');}
    if(id.startsWith('cx-rt-')){state.options.ratings  ={enable:!!document.getElementById('cx-rt-enable')?.checked,add:!!document.getElementById('cx-rt-add')?.checked,remove:!!document.getElementById('cx-rt-remove')?.checked};state.visited.add('ratings');}
    if(id.startsWith('cx-hs-')){state.options.history  ={enable:!!document.getElementById('cx-hs-enable')?.checked,add:!!document.getElementById('cx-hs-add')?.checked,remove:!!document.getElementById('cx-hs-remove')?.checked};state.visited.add('history');}
    if(id.startsWith('cx-pl-')){state.options.playlists={enable:!!document.getElementById('cx-pl-enable')?.checked,add:!!document.getElementById('cx-pl-add')?.checked,remove:!!document.getElementById('cx-pl-remove')?.checked};state.visited.add('playlists');}
    if(id.startsWith('gl-')){state.globals={dry_run:!!wrap.querySelector('#gl-dry')?.checked,verify_after_write:!!wrap.querySelector('#gl-verify')?.checked,drop_guard:!!wrap.querySelector('#gl-drop')?.checked,allow_mass_delete:!!wrap.querySelector('#gl-mass')?.checked,tombstone_ttl_days:parseInt(wrap.querySelector('#gl-ttl')?.value||'0',10)||0,include_observed_deletes:!!wrap.querySelector('#gl-observed')?.checked};}
    if(id==='cx-enabled'||id==='cx-mode-one'||id==='cx-mode-two') updateFlow(true);
    updateRules(); updateFlowClasses();
  });

  try{
    await loadProviders();
    renderProviderSelects();
    refreshTabs();
    updateFlow(true);
    updateFlowRailLogos?.();
    document.getElementById("cx-src")?.addEventListener("change",updateFlowRailLogos);
    document.getElementById("cx-dst")?.addEventListener("change",updateFlowRailLogos);
  }catch(_){}

  // Expose internals
  wrap.__refreshTabs = refreshTabs;
  wrap.__renderProviderSelects = renderProviderSelects;
  wrap.__updateFlow = updateFlow;
  wrap.__updateFlowClasses = updateFlowClasses;
  wrap.__renderFeaturePanel = renderFeaturePanel;

  // Always use sticky Save/Cancel
  try{ installStickyBridge && installStickyBridge(wrap); }catch(_){}

  return wrap;
}

// Sticky Save/Cancel
function ensureModalFooterCSS(){
  if(document.getElementById('cx-modal-footer-css')) return;
  const css = `
  /* Modal-local transparent footer (clone of FastAPI, but namespaced) */
  #cx-save-frost{
    position:fixed; left:0; right:0; bottom:0; height:84px;
    background:linear-gradient(0deg,rgba(10,10,14,.85) 0%,rgba(10,10,14,.60) 35%,rgba(10,10,14,0) 100%);
    border-top:1px solid var(--border);
    backdrop-filter:blur(6px) saturate(110%); -webkit-backdrop-filter:blur(6px) saturate(110%);
    pointer-events:none; z-index:2147483000;
  }
  #cx-save-fab{
    position:fixed; left:0; right:0; bottom:max(12px, env(safe-area-inset-bottom));
    z-index:2147483001; display:flex; justify-content:center; align-items:center;
    pointer-events:none; background:transparent;
  }
  #cx-save-fab .btn{
    pointer-events:auto; position:relative; z-index:2147483002;
    padding:14px 22px; border-radius:14px; font-weight:800; text-transform:uppercase; letter-spacing:.02em;
    background:linear-gradient(135deg,#ff4d4f,#ff7a7a);
    border:1px solid #ff9a9a55; box-shadow:0 10px 28px rgba(0,0,0,.35),0 0 14px #ff4d4f55; color:#fff;
  }
  #cx-save-fab.hidden, #cx-save-frost.hidden{ display:none; }
  `;
  const st = document.createElement('style');
  st.id = 'cx-modal-footer-css';
  st.textContent = css;
  document.head.appendChild(st);
}

function installStickyBridge(modalEl){
  if(modalEl.__stickyBridgeInstalled) return;
  modalEl.__stickyBridgeInstalled = true;

  const BODY_CLASS = 'cx-modal-open';
  const GLOBAL_SEL = '#save-fab,#save-frost,.savebar,.actions.sticky';

  // Temporarily detach FastAPI footer to avoid visual/click conflicts during modal
  function suspendFastAPIFooter(){
    try{
      if(modalEl.__cxSuspendedFooter) return;
      const fab = document.getElementById('save-fab');
      const frost = document.getElementById('save-frost');
      const st = { anchorFab:null, anchorFrost:null, fabNode:null, frostNode:null };
      if(frost && frost.parentNode){
        st.anchorFrost = document.createComment('cx-anchor-frost');
        frost.parentNode.insertBefore(st.anchorFrost, frost);
        st.frostNode = frost;
        frost.parentNode.removeChild(frost);
      }
      if(fab && fab.parentNode){
        st.anchorFab = document.createComment('cx-anchor-fab');
        fab.parentNode.insertBefore(st.anchorFab, fab);
        st.fabNode = fab;
        fab.parentNode.removeChild(fab);
      }
      modalEl.__cxSuspendedFooter = st;
    }catch(_){}
  }
  function resumeFastAPIFooter(){
    try{
      const st = modalEl.__cxSuspendedFooter;
      if(!st) return;
      if(st.anchorFrost && st.anchorFrost.parentNode && st.frostNode){
        st.anchorFrost.replaceWith(st.frostNode);
      }else if(st.anchorFrost){ try{ st.anchorFrost.remove(); }catch(_){} }
      if(st.anchorFab && st.anchorFab.parentNode && st.fabNode){
        st.anchorFab.replaceWith(st.fabNode);
      }else if(st.anchorFab){ try{ st.anchorFab.remove(); }catch(_){} }
      // Force reflow + visibility in case other code hid it
      requestAnimationFrame(()=>{ try{
        const fab = document.getElementById('save-fab');
        const frost = document.getElementById('save-frost');
        if(fab){ fab.classList.remove('hidden'); fab.style.display=''; void fab.offsetHeight; }
        if(frost){ frost.classList.remove('hidden'); frost.style.display=''; }
      }catch(_){ }});
      modalEl.__cxSuspendedFooter = null;
    }catch(_){}
  }
// Ensure FastAPI footer re-appears (never rely solely on their observers)
  function restoreGlobalFooters(){
  try{
    const fab = document.getElementById('save-fab');
    const frost = document.getElementById('save-frost');
    [fab, frost].forEach(n => {
      if(!n) return;
      try{ if(n.style && n.style.display==='none') n.style.display=''; }catch(_){}
      try{ n.classList && n.classList.remove('hidden'); }catch(_){}
    });
  }catch(_){}
}

function postCloseRepair(){
  try{
    let n=0;
    const tick=()=>{
      try{
        document.body.classList.remove('cx-modal-open');
        restoreGlobalFooters();
      }catch(_){}
      if(++n<5) setTimeout(tick, n<3 ? 80 : 200);
    };
    setTimeout(tick,0);
  }catch(_){}
}
const isOpen = ()=> !!modalEl && document.body.contains(modalEl) && !modalEl.classList.contains('hidden');

  // Inject CSS once to hide global bars while modal is open
  (function ensureStyle(){
    if(document.getElementById('cx-modal-sticky-style')) return;
    const css = `
      body.${BODY_CLASS} ${GLOBAL_SEL} { display: none !important; }
      @supports (padding-bottom: env(safe-area-inset-bottom)) {
        #cx-modal-sticky { padding-bottom: env(safe-area-inset-bottom); }
      }
    `;
    const style = document.createElement('style');
    style.id = 'cx-modal-sticky-style';
    style.textContent = css;
    document.head.appendChild(style);
  })();

  function addBodyClass(){ document.body.classList.add(BODY_CLASS); }
  function removeBodyClass(){ document.body.classList.remove(BODY_CLASS); }

  // Repair: clear stale inline display:none from previous crashes/versions
  function repairGlobals(){
    try{
      document.querySelectorAll(GLOBAL_SEL).forEach(n=>{
        if(n && n.style && n.style.display === 'none'){
          n.style.display = ''; // reset
        }
      });
    }catch(_){}
  }

  
  function ensureLocalBar(){
    // Ensure modal footer CSS present
    try{ ensureModalFooterCSS && ensureModalFooterCSS(); }catch(_){}
    // Create or reveal the frost strip
    let frost = document.getElementById('cx-save-frost');
    if(!frost){
      frost = document.createElement('div');
      frost.id = 'cx-save-frost';
      document.body.appendChild(frost);
    } else {
      frost.classList.remove('hidden');
    }
    // Create or reveal the centered FAB
    let fab = document.getElementById('cx-save-fab');
    if(!fab){
      fab = document.createElement('div');
      fab.id = 'cx-save-fab';
      fab.setAttribute('role','toolbar');
      const btnCancel = document.createElement('button');
      btnCancel.className = 'btn ghost';
      btnCancel.textContent = 'Cancel';
      btnCancel.style.marginRight = '16px';
      btnCancel.addEventListener('click', (e)=>{ e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation?.(); cxCloseModal(false); }, true);

      const btnSave = document.createElement('button');
      btnSave.className = 'btn';
      btnSave.id = 'cx-save-fab-btn';
      btnSave.innerHTML = '<span class=\"btn-ic\">✔</span> <span class=\"btn-label\">Save</span>';
      btnSave.addEventListener('click', async (e)=>{
        e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation?.();
        try{ await modalEl.__doSave?.(); }catch(err){ console.warn('[cx] modal save failed:', err); }
      }, true);

      // Inner group to keep center alignment
      const group = document.createElement('div');
      group.style.cssText = 'display:flex;gap:16px;align-items:center;pointer-events:auto;';
      group.appendChild(btnCancel);
      group.appendChild(btnSave);
      fab.appendChild(group);
      document.body.appendChild(fab);

      modalEl.__stickyRefs = { container: fab, saveEl: btnSave, cancelEl: btnCancel };
    } else {
      fab.classList.remove('hidden');
    }
    return { frost, fab };
  }

  function teardown(){
    try{ document.getElementById('cx-save-frost')?.remove(); }catch(_){}
    try{ document.getElementById('cx-save-fab')?.remove(); }catch(_){}

    try{ document.getElementById('cx-modal-sticky')?.remove(); }catch(_){}
    removeBodyClass(); try{ restoreGlobalFooters(); }catch(_){ } try{ resumeFastAPIFooter(); }catch(_){ } try{ postCloseRepair(); }catch(_){ }
    modalEl.__stickyRefs = null;
  
    try{
      if(modalEl.__cxBodyWatchdog){ modalEl.__cxBodyWatchdog.disconnect(); modalEl.__cxBodyWatchdog=null; }
      if(modalEl.__cxTidy){
        window.removeEventListener('hashchange', modalEl.__cxTidy);
        window.removeEventListener('popstate', modalEl.__cxTidy);
        window.removeEventListener('pageshow', modalEl.__cxTidy);
        document.removeEventListener('visibilitychange', modalEl.__cxTidy);
        modalEl.__cxTidy = null;
      }
    }catch(_){}
}

  // Debounced ensure on any modal mutation
  let raf = 0;
  function scheduleEnsure(){
    if(raf) return;
    raf = requestAnimationFrame(()=>{ raf=0;
      if(isOpen()){
        repairGlobals();      // fix any stale inline style
        addBodyClass();       // hide globals via CSS (no inline mutation)
        try{ ensureModalFooterCSS && ensureModalFooterCSS(); }catch(_){} ensureLocalBar();     // show our own centered sticky
      }
    });
  }

  // Observe only the modal
  const mo = new MutationObserver(()=>{
    if(!isOpen()){ teardown(); return; }
    scheduleEnsure();
  });
  mo.observe(modalEl, { attributes:true, attributeFilter:['class'], childList:true, subtree:true });
  modalEl.__stickyMO = mo;

  // Initial activation
  if(isOpen()){ repairGlobals(); addBodyClass(); suspendFastAPIFooter(); try{ ensureModalFooterCSS && ensureModalFooterCSS(); }catch(_){} ensureLocalBar(); }

  // Failsafe watchdog: ensure body class is cleared if modal vanishes unexpectedly
  (function cxModalBodyWatchdog(){
    try {
      const tidy = ()=>{
        const m = document.getElementById('cx-modal');
        const open = !!(m && !m.classList.contains('hidden'));
        if (!open) { document.body.classList.remove('cx-modal-open'); try{ restoreGlobalFooters(); }catch(_){} try{ resumeFastAPIFooter(); }catch(_){} try{ postCloseRepair(); }catch(_){} }
      };
      modalEl.__cxTidy = tidy;
      window.addEventListener('hashchange', tidy);
      window.addEventListener('popstate', tidy);
      window.addEventListener('pageshow', tidy);
      document.addEventListener('visibilitychange', tidy);
      const bw = new MutationObserver(tidy);
      bw.observe(document.body, { childList: true, subtree: false });
      // Patch history navigation to tidy after SPA route changes
      try{
        const _push = history.pushState; const _replace = history.replaceState;
        history.pushState = function(){ const r=_push.apply(this, arguments); try{ tidy(); }catch(_){} return r; };
        history.replaceState = function(){ const r=_replace.apply(this, arguments); try{ tidy(); }catch(_){} return r; };
      }catch(_){}
      modalEl.__cxBodyWatchdog = bw;
    } catch (_) {}
  })();

  modalEl.__cxStickyUnwire = teardown;
}


// Lightweight bind helper (unchanged behavior)
function cxBindCfgEvents(){
  

  try{
    const _m1=document.getElementById('cx-mode-one');
    const _m2=document.getElementById('cx-mode-two');
    const _bind=(el)=>{ if(!el) return; el.addEventListener('change', ()=>{ try{ wrap.__updateFlow && wrap.__updateFlow(true); }catch(_){} }, true); };
    _bind(_m1); _bind(_m2);
  }catch(_){}

const ids=["cx-src","cx-dst","cx-mode-one","cx-mode-two","cx-wl-enable","cx-wl-add","cx-wl-remove","cx-rt-enable","cx-rt-add","cx-rt-remove","cx-hs-enable","cx-hs-add","cx-hs-remove","cx-pl-enable","cx-pl-add","cx-pl-remove","cx-enabled"];
  ids.forEach((id)=>{
    const el=document.getElementById(id); if(!el) return;
    el.addEventListener("change",(ev)=>{
      const id=ev.target?.id||"";
      const map={"cx-wl-enable":"cx-wl-remove","cx-rt-enable":"cx-rt-remove","cx-hs-enable":"cx-hs-remove","cx-pl-enable":"cx-pl-remove"};
      if(map[id]){
        const rm=document.getElementById(map[id]);
        if(rm){rm.disabled=!ev.target.checked; if(!ev.target.checked) rm.checked=false;}
      }
      try{cxUpdateSummary&&cxUpdateSummary();}catch(_){}
      if(id==="cx-enabled"){
        const el=document.getElementById('cx-modal');
        el?.__updateFlowClasses?.();
      }
    });
  });
}

// Open editor for an existing pair (uses sticky Save/Cancel)
async function cxOpenModalFor(pair, editingId){
  await cxEnsureCfgModal();
  const el = document.getElementById('cx-modal');
  if(!el) return;

  const st = el.__state;
  st.src = pair?.source || pair?.src || null;
  st.dst = pair?.target || pair?.dst || null;

  // Mode/enabled
  document.getElementById('cx-mode-two')?.removeAttribute('checked');
  document.getElementById('cx-mode-one')?.removeAttribute('checked');
  if((pair?.mode||'').toLowerCase().startsWith('two')) document.getElementById('cx-mode-two').checked=true;
  else document.getElementById('cx-mode-one').checked=true;
  const enEl=document.getElementById('cx-enabled'); if(enEl) enEl.checked = (pair && typeof pair.enabled !== 'undefined') ? !!pair.enabled : true;

  // Features
  const fx = pair?.features || {};
  st.options.watchlist = Object.assign({enable:true,add:true,remove:false}, fx.watchlist||{});
  st.options.ratings   = Object.assign({enable:false,add:false,remove:false}, fx.ratings||{});
  st.options.history   = Object.assign({enable:false,add:false,remove:false}, fx.history||{});
  st.options.playlists = Object.assign({enable:false,add:true,remove:false}, fx.playlists||{});
  st.visited = new Set(['watchlist','ratings','history','playlists']);

  // Hydrate UI
  const reSel = ()=>{
    const srcSel=el.querySelector('#cx-src'); const dstSel=el.querySelector('#cx-dst');
    if(srcSel) srcSel.value = st.src||'';
    if(dstSel) dstSel.value = st.dst||'';
    const labS=el.querySelector('#cx-src-display'); const labD=el.querySelector('#cx-dst-display');
    if(labS){ const o = (st.src && st.providers?.find(p=>p.name===st.src)); labS.textContent=o?.label||'—'; labS.dataset.value=st.src||''; }
    if(labD){ const o = (st.dst && st.providers?.find(p=>p.name===st.dst)); labD.textContent=o?.label||'—'; labD.dataset.value=st.dst||''; }
  };

  el.__renderProviderSelects?.();
  el.__refreshTabs?.();
  reSel();
  el.__updateFlow?.(true);

  // Remember editing id and show
  el.dataset.editingId = editingId || (pair?.id || '');
  el.classList.remove('hidden');

  // Reinstall sticky bridge on each open (observer may have been disconnected on close)
  try{ el.__stickyBridgeInstalled = false; }catch(_){}
  try{ installStickyBridge && installStickyBridge(el); }catch(_){}
}

// Close modal and unhook sticky listeners
function cxCloseModal(persist=false){
  const mod=document.getElementById('cx-modal');
  if(!mod) return;
  if(persist){try{mod.__persistCurrentFeature&&mod.__persistCurrentFeature();}catch(_){}} // best-effort
  mod.classList.add('hidden');

  // 👇 add this line here
  try{ mod.__cxStickyUnwire && mod.__cxStickyUnwire(); }catch(_){}

  try{
    if(mod.__stickyMO) { mod.__stickyMO.disconnect(); mod.__stickyMO=null; }
    if(mod.__stickyBodyMO) { mod.__stickyBodyMO.disconnect(); mod.__stickyBodyMO=null; }
    if(mod.__stickyRefs){
      if(mod.__stickyRefs.saveEl && mod.__stickyRefs.onSave) mod.__stickyRefs.saveEl.removeEventListener('click', mod.__stickyRefs.onSave, true);
      if(mod.__stickyRefs.cancelEl && mod.__stickyRefs.onCancel) mod.__stickyRefs.cancelEl.removeEventListener('click', mod.__stickyRefs.onCancel, true);
      mod.__stickyRefs=null;
    }
  }catch(_){}
  try{mod.dataset.editingId=''; const st=mod.__state; if(st) st.visited=new Set();}catch(_){}
  document.querySelectorAll('#cx-modal').forEach(el=>el.classList.add('hidden'));
  try{window.cx=window.cx||{};window.cx.connect=window.cx.connect||{source:null,target:null};window.cx.connect.source=null;window.cx.connect.target=null;}catch(_){}
}


// Barebones fallback (rare)
function _ensureCfgModal(){
  if(document.getElementById("cx-modal")) return true;
  if(typeof cxEnsureCfgModal==="function"){cxEnsureCfgModal();return !!document.getElementById("cx-modal");}
  const wrap=document.createElement("div");
  wrap.id="cx-modal";wrap.className="modal-backdrop hidden";
  wrap.innerHTML=
    '<div class="modal-card"><div class="modal-header"><div class="title">Configure</div><button class="btn-ghost" onclick="cxCloseModal()">✕</button></div>'+
    '<div class="modal-body">Minimal modal (fallback)</div></div>';
  document.body.appendChild(wrap);
  return true;
}

// --- UPDATED: Flow animation CSS (fixed stray char; keeps one-way/two-way animations)
function ensureFlowAnimCSS(){
  if(document.getElementById('cx-flow-anim-css')) return;
  const css = `  /* full-width animated rail */
  @keyframes cx-flow-one   { 0%{left:0;opacity:.2} 50%{opacity:1} 100%{left:calc(100% - 8px);opacity:.2} }
  @keyframes cx-flow-two-a { 0%{left:0;opacity:.2} 50%{opacity:1} 100%{left:calc(100% - 8px);opacity:.2} }
  @keyframes cx-flow-two-b { 0%{left:calc(100% - 8px);opacity:.2} 50%{opacity:1} 100%{left:0;opacity:.2} }

  .flow-rail.pretty{display:flex;align-items:center;gap:10px;--flowColor:currentColor}
  .flow-rail.pretty .token{min-width:40px;text-align:center;opacity:.9}
  .flow-rail.pretty .arrow{position:relative;display:block;flex:1;height:12px}
  .flow-rail.pretty .arrow::before{content:'';position:absolute;left:0;right:0;top:50%;height:2px;background:var(--flowColor);opacity:.25;transform:translateY(-50%);border-radius:2px}
  .flow-rail.pretty .dot.flow{position:absolute;top:50%;transform:translateY(-50%);width:8px;height:8px;border-radius:50%;background:var(--flowColor);opacity:.6}

  /* one-way */
  .flow-rail.pretty.anim-one  .dot.flow.a{animation:cx-flow-one 1.2s ease-in-out infinite}
  .flow-rail.pretty.anim-one  .dot.flow.b{animation:cx-flow-one 1.2s ease-in-out .6s infinite}

  /* two-way */
  .flow-rail.pretty.anim-two  .dot.flow.a{animation:cx-flow-two-a 1.2s ease-in-out infinite}
  .flow-rail.pretty.anim-two  .dot.flow.b{animation:cx-flow-two-b 1.2s ease-in-out infinite}`;
  const st = document.createElement('style');
  st.id = 'cx-flow-anim-css';
  st.textContent = css;
  document.head.appendChild(st);
}

// Ensure animation CSS exists
try{ ensureFlowAnimCSS(); }catch(_){}


// Public exports
window.Modals=Object.assign(window.Modals||{},{
  openAbout,closeAbout,openUpdateModal,closeUpdateModal,dismissUpdate,
  cxEnsureCfgModal,cxBindCfgEvents,cxCloseModal,openPairModal,closePairModal,cxEditPair: (id)=>{const pr=(window.cx?.pairs||[]).find(p=>p.id===id);if(!pr)return;cxOpenModalFor(pr,id);}, cxOpenModalFor
});
Object.assign(window,{
  openAbout,closeAbout,openUpdateModal,closeUpdateModal,dismissUpdate,
  cxEnsureCfgModal,cxBindCfgEvents,cxCloseModal,openPairModal,closePairModal,cxEditPair: (id)=>{const pr=(window.cx?.pairs||[]).find(p=>p.id===id);if(!pr)return;cxOpenModalFor(pr,id);}, cxOpenModalFor
});