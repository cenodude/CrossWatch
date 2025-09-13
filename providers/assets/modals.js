// modals.js — Configure Connection modal

function _cxGetState() {
  const el = document.getElementById('cx-modal');
  return (el && el.__state) || window.__cxState || null;
}

function openPairModal(){var m=document.getElementById("pairModal");if(m)m.classList.remove("hidden")}
function closePairModal(){var m=document.getElementById("pairModal");if(m)m.classList.add("hidden")}

async function openAbout(){
  try{
    const r=await fetch("/api/version?cb="+Date.now(),{cache:"no-store"});
    const j=r.ok?await r.json():{};
    const cur=(j.current??"0.0.0").toString().trim();
    const latest=(j.latest??"").toString().trim()||null;
    const url=j.html_url||"https://github.com/cenodude/plex-simkl-watchlist-sync/releases";
    const upd=!!j.update_available;

    const verEl=document.getElementById("about-version");
    if(verEl){verEl.textContent=`Version ${j.current}`;verEl.dataset.version=cur;}

    const headerVer=document.getElementById("app-version");
    if(headerVer){headerVer.textContent=`Version ${cur}`;headerVer.dataset.version=cur;}

    const relEl=document.getElementById("about-latest");
    if(relEl){
      relEl.href=url;
      relEl.textContent=latest?`v${latest}`:"Releases";
      relEl.setAttribute("aria-label",latest?`Latest release v${latest}`:"Releases");
    }

    const updEl=document.getElementById("about-update");
    if(updEl){
      updEl.classList.add("badge","upd");
      if(upd&&latest){
        updEl.textContent=`Update ${latest} available`;
        updEl.classList.remove("hidden","reveal");
        void updEl.offsetWidth;
        updEl.classList.add("reveal");
      }else{
        updEl.textContent="";
        updEl.classList.add("hidden");
        updEl.classList.remove("reveal");
      }
    }
  }catch(_){}
  document.getElementById("about-backdrop")?.classList.remove("hidden");
}
function closeAbout(ev){if(ev&&ev.type==="click"&&ev.currentTarget!==ev.target)return;document.getElementById("about-backdrop")?.classList.add("hidden")}

async function cxEnsureCfgModal(){
  const ex=document.querySelector('#cx-modal'); if(ex) return ex;

  const wrap=document.createElement('div');
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
            <input type="checkbox" id="cx-enabled">
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
                  <label for="cx-src">Source</label>
                  <select id="cx-src" class="input"></select>
                </div>
                <div class="field">
                  <label for="cx-dst">Target</label>
                  <select id="cx-dst" class="input"></select>
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
                  <span class="arrow"><span class="dot flow a"></span><span class="dot flow b"></span></span>
                  <span class="token" id="cx-flow-dst">-</span>
                </div>
              </div>
            </div>
          </div>

          <div class="cx-main">
            <div class="left">
              <div class="panel" id="cx-feat-panel"></div>
            </div>
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

        <div class="cx-foot">
          <button id="cx-cancel" class="btn ghost">Cancel</button>
          <button id="cx-save" class="btn primary">Save</button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(wrap);

  if(typeof cxBindCfgEvents==='function') cxBindCfgEvents();
  wrap.querySelector('#cx-cancel').onclick=()=>wrap.classList.add('hidden');

  const state={
    providers:[],
    src:null,
    dst:null,
    feature:'globals',
    options:{
      watchlist:{enable:true,  add:true,  remove:false},
      ratings:  {enable:false, add:false, remove:false},
      history:  {enable:false, add:false, remove:false},
      playlists:{enable:false, add:false, remove:false}
    },
    visited: new Set()
  };
  wrap.__state = state;
  window.__cxState = state;

  // Temp gate for unfinished features
  const __TEMP_DISABLED = new Set(["ratings", "history", "playlists"]);

  // Global defaults
  const DEFAULT_GLOBALS={
    dry_run:false,
    verify_after_write:false,
    drop_guard:false,
    allow_mass_delete:true,
    tombstone_ttl_days:30,
    include_observed_deletes:true
  };
  state.globals={...DEFAULT_GLOBALS};
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
  function byName(n){return state.providers.find(p=>p.name===n)}
  function commonFeatures(){
    if(!state.src||!state.dst) return [];
    const a=byName(state.src), b=byName(state.dst); if(!a||!b) return [];
    const keys=['watchlist','ratings','history','playlists'];
    return keys.filter(k=>a.features?.[k] && b.features?.[k]);
  }

  function defaultForFeature(key){
    if(key==='watchlist') return {enable:true, add:true, remove:false};
    if(key==='playlists') return {enable:false, add:true, remove:false};
    return {enable:false, add:false, remove:false};
  }
  function hydrateFeatureFromPair(key, fallback){
    let v = fallback ?? defaultForFeature(key);
    try{
      const mid=(wrap&&wrap.dataset?wrap.dataset.editingId:'')||'';
      if(mid && Array.isArray(window.cx?.pairs)){
        const p=window.cx.pairs.find(x=>String(x.id||'')===String(mid));
        if(p && p.features && p.features[key]){
          v=Object.assign(defaultForFeature(key), p.features[key]);
        }
      }
    }catch(_){}
    return v;
  }

  function persistCurrentFeature() {
    const f = state.feature;
    if (f === 'watchlist') {
      state.options.watchlist = {
        enable: !!document.getElementById('cx-wl-enable')?.checked,
        add:    !!document.getElementById('cx-wl-add')?.checked,
        remove: !!document.getElementById('cx-wl-remove')?.checked
      };
      state.visited.add('watchlist');
    } else if (f === 'ratings') {
      state.options.ratings = {
        enable: !!document.getElementById('cx-rt-enable')?.checked,
        add:    !!document.getElementById('cx-rt-add')?.checked,
        remove: !!document.getElementById('cx-rt-remove')?.checked
      };
      state.visited.add('ratings');
    } else if (f === 'history') {
      state.options.history = {
        enable: !!document.getElementById('cx-hs-enable')?.checked,
        add:    !!document.getElementById('cx-hs-add')?.checked,
        remove: !!document.getElementById('cx-hs-remove')?.checked
      };
      state.visited.add('history');
    } else if (f === 'playlists') {
      state.options.playlists = {
        enable: !!document.getElementById('cx-pl-enable')?.checked,
        add:    !!document.getElementById('cx-pl-add')?.checked,
        remove: !!document.getElementById('cx-pl-remove')?.checked
      };
      state.visited.add('playlists');
    }
  }
  state.persistCurrentFeature = persistCurrentFeature;

  function getOpts(key) {
    if (!state.visited.has(key)) {
      const v = hydrateFeatureFromPair(key, state.options[key]);
      state.options[key] = v;
      state.visited.add(key);
      return v;
    }
    return state.options[key];
  }

  function hydrateSrcDstFromPair(){
    try{
      const mid=(wrap&&wrap.dataset?wrap.dataset.editingId:'')||'';
      if(mid && Array.isArray(window.cx?.pairs)){
        const p=window.cx.pairs.find(x=>String(x.id||'')===String(mid));
        if(p){
          state.src = p.source || state.src;
          state.dst = p.target || state.dst;
          const en=document.getElementById('cx-enabled'); if(en) en.checked = !!p.enabled;
          const mTwo=document.getElementById('cx-mode-two');
          const mOne=document.getElementById('cx-mode-one');
          if(p.mode==='two-way'){ if(mTwo) mTwo.checked=true; } else { if(mOne) mOne.checked=true; }
        }
      }
    }catch(_){}
  }

  function renderProviderSelects(){
    const srcSel=wrap.querySelector('#cx-src');
    const dstSel=wrap.querySelector('#cx-dst');
    const opts=state.providers.map(p=>`<option value="${p.name}">${p.label}</option>`).join('');
    srcSel.innerHTML=`<option value="">Select…</option>${opts}`;
    dstSel.innerHTML=`<option value="">Select…</option>${opts}`;
    if(state.src) srcSel.value=state.src;
    if(state.dst) dstSel.value=state.dst;

    // Locked in modal (editing an existing pair)
    srcSel.disabled = true;
    dstSel.disabled = true;

    srcSel.onchange=()=>{ state.src=srcSel.value||null; updateFlow(); refreshTabs(); };
    dstSel.onchange=()=>{ state.dst=dstSel.value||null; updateFlow(); refreshTabs(); };
  }

  function updateFlow(){
    wrap.querySelector('#cx-flow-src').textContent=byName(state.src)?.label||'—';
    wrap.querySelector('#cx-flow-dst').textContent=byName(state.dst)?.label||'—';
    const two=wrap.querySelector('#cx-mode-two');
    const ok=(byName(state.src)?.capabilities?.bidirectional)&&(byName(state.dst)?.capabilities?.bidirectional);
    two.disabled=!ok;
    if(!ok && two.checked){ wrap.querySelector('#cx-mode-one').checked=true; }
    if(two.nextElementSibling) two.nextElementSibling.classList.toggle('disabled',!ok);
    const title=(wrap.querySelector('#cx-mode-two').checked?'Two-way (bidirectional)':'One-way');
    const tEl=wrap.querySelector('#cx-flow-title'); if(tEl) tEl.textContent=title;
    updateFlowClasses();
  }

  function updateFlowClasses(){
    // Animate rail only when connection + feature are enabled
    const rail=wrap.querySelector('#cx-flow-rail'); if(!rail) return;
    const two=wrap.querySelector('#cx-mode-two')?.checked;
    const enabled = !!document.getElementById('cx-enabled')?.checked;
    const wl = state.options.watchlist || defaultForFeature('watchlist');

    rail.className='flow-rail pretty';
    rail.classList.toggle('mode-two',!!two);
    rail.classList.toggle('mode-one',!two);

    const flowOn = enabled && wl.enable && (two ? (wl.add||wl.remove) : (wl.add || wl.remove));
    rail.classList.toggle('off', !flowOn);

    if(two){
      rail.classList.toggle('active', flowOn);
    }else{
      rail.classList.toggle('dir-add',   flowOn && wl.add);
      rail.classList.toggle('dir-remove',flowOn && !wl.add && wl.remove);
    }
  }

  function refreshTabs() {
    const TEMP_DISABLED = __TEMP_DISABLED;
    const tabs = wrap.querySelector("#cx-feat-tabs");
    if (!tabs) return;
    tabs.innerHTML = "";

    const items = ["globals", ...commonFeatures()];
    if (items.length === 0) {
      tabs.innerHTML = '<div class="ftab disabled">No common features</div>';
      renderFeaturePanel();
      return;
    }

    const LABELS = {
      globals: "Globals",
      watchlist: "Watchlist",
      ratings: "Ratings",
      history: "History",
      playlists: "Playlists",
    };

    items.forEach((k) => {
      const btn = document.createElement("button");
      btn.className = "ftab";
      btn.dataset.key = k;
      btn.textContent = LABELS[k] || (k[0]?.toUpperCase?.() + k.slice(1));

      if (TEMP_DISABLED.has(k)) {
        btn.classList.add("disabled");
        btn.setAttribute("aria-disabled", "true");
        btn.title = "Coming soon";
      } else {
        btn.onclick = () => {
          persistCurrentFeature();
          state.feature = k;
          refreshTabs();
          renderFeaturePanel();
        };
      }

      if (state.feature === k) btn.classList.add("active");
      tabs.appendChild(btn);
    });

    if (!items.includes(state.feature) || __TEMP_DISABLED.has(state.feature)) {
      state.feature = items.find((k) => k !== "globals" && !__TEMP_DISABLED.has(k)) || "globals";
    }

    [...tabs.children].forEach((c) => c.classList.toggle("active", c.dataset.key === state.feature));
    renderFeaturePanel();
  }

  function ensureRulesCard() {
    const rp = wrap.querySelector("#cx-right-panel");
    if (!rp) return;
    rp.classList.add("rules-card");
    rp.innerHTML = `
      <div class="panel-title" id="cx-right-title">Rule preview</div>
      <div class="sub" id="cx-right-sub">These rules are generated from your selections.</div>
      <div class="rules" id="cx-rules">
        <div class="r note" id="cx-notes">Pick a feature.</div>
      </div>`;
  }

  function renderFeaturePanel() {
    const TEMP_DISABLED = __TEMP_DISABLED;
    const left  = wrap.querySelector("#cx-feat-panel");
    const right = wrap.querySelector("#cx-right-panel");
    if (!left || !right) return;

    if (state.feature === "globals") {
      const g = state.globals || {};
      left.innerHTML = `
        <div class="panel-title">Global sync options</div>

        <div class="opt-row">
          <label for="gl-dry">Dry run</label>
          <label class="switch">
            <input id="gl-dry" type="checkbox" ${g.dry_run ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="muted">Simulate changes only; do not write to providers.</div>

        <div class="opt-row">
          <label for="gl-verify">Verify after write</label>
          <label class="switch">
            <input id="gl-verify" type="checkbox" ${g.verify_after_write ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="muted">Re-check items after writes.</div>

        <div class="opt-row">
          <label for="gl-drop">Drop guard</label>
          <label class="switch">
            <input id="gl-drop" type="checkbox" ${g.drop_guard ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="muted">Protect against empty source snapshots.</div>

        <div class="opt-row">
          <label for="gl-mass">Allow mass delete</label>
          <label class="switch">
            <input id="gl-mass" type="checkbox" ${g.allow_mass_delete ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="muted">Permit bulk removals when required.</div>
      `;

      right.classList.remove("rules-card");
      right.innerHTML = `
        <div class="panel-title">More options</div>

        <div class="opt-row">
          <label for="gl-ttl">Tombstone TTL (days)</label>
          <input id="gl-ttl" class="input" type="number" min="0" step="1" value="${g.tombstone_ttl_days ?? 30}">
        </div>
        <div class="muted">Keep delete markers to avoid re-adding.</div>

        <div class="opt-row">
          <label for="gl-observed">Include observed deletes</label>
          <label class="switch">
            <input id="gl-observed" type="checkbox" ${g.include_observed_deletes ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="muted">Apply deletions detected from activity.</div>
      `;
      return;
    }

    ensureRulesCard();

    if (TEMP_DISABLED.has(state.feature)) {
      left.innerHTML = `
        <div class="panel-title">${state.feature[0].toUpperCase() + state.feature.slice(1)} options</div>
        <div class="muted" style="padding:18px 0;">
          Temporarily unavailable. This will be enabled in a future update.
        </div>`;
      const title = wrap.querySelector("#cx-right-title");
      const sub   = wrap.querySelector("#cx-right-sub");
      const rules = wrap.querySelector("#cx-rules");
      if (title) title.textContent = "Rule preview";
      if (sub)   sub.textContent   = "Feature disabled · coming soon";
      if (rules) rules.innerHTML   = "";
      return;
    }

    if (state.feature === "watchlist") {
      const wl = getOpts("watchlist");
      left.innerHTML = `
        <div class="panel-title">Watchlist options</div>
        <div class="opt-row">
          <label for="cx-wl-enable">Enable</label>
          <label class="switch">
            <input id="cx-wl-enable" type="checkbox" ${wl.enable ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="opt-row">
          <label for="cx-wl-add">Add</label>
          <label class="switch">
            <input id="cx-wl-add" type="checkbox" ${wl.add ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="opt-row">
          <label for="cx-wl-remove">Remove</label>
          <label class="switch">
            <input id="cx-wl-remove" type="checkbox" ${wl.remove ? "checked" : ""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="panel sub">
          <div class="panel-title small">Advanced</div>
          <ul class="adv">
            <li>Filter by type</li>
            <li>Skip duplicates</li>
            <li>Prefer target metadata</li>
            <li>Conflict policy</li>
          </ul>
        </div>`;
      const wlRem = document.getElementById("cx-wl-remove");
      if (wlRem) { wlRem.disabled = !wl.enable; if (!wl.enable && wlRem.checked) wlRem.checked = false; }
      updateRules();
      return;
    }

    left.innerHTML = `
      <div class="panel-title">${state.feature[0].toUpperCase() + state.feature.slice(1)} options</div>
      <div class="muted">Configuration for this feature will be available soon.</div>`;
    const notes = wrap.querySelector("#cx-notes");
    if (notes) notes.textContent = `Selected: ${state.feature}.`;
  }

  function updateRules(){
    if(state.feature==='globals') return;

    const rules=wrap.querySelector('#cx-rules');
    if(!rules) return;
    rules.innerHTML='';
    const push=(title,sub='')=>{
      const d=document.createElement('div');
      d.className='r';
      d.innerHTML=`<div class="t">${title}</div>${sub?`<div class="s">${sub}</div>`:''}`;
      rules.appendChild(d);
    };

    const two=wrap.querySelector('#cx-mode-two')?.checked;

    if(state.feature==='watchlist'){
      const wl=state.options.watchlist||defaultForFeature('watchlist');
      wrap.querySelector('#cx-notes')?.remove();
      push(`Watchlist • ${two?'two-way':'one-way'}`, wl.enable?'enabled':'disabled');
      if(wl.add)   push('Add • new items', two?'both directions':'from source → target');
      if(wl.remove)push('Delete • when removed', two?'both directions':'from source → target');
      return;
    }

    if(state.feature==='ratings'){
      const rt=state.options.ratings||defaultForFeature('ratings');
      wrap.querySelector('#cx-notes')?.remove();
      push(`Ratings • ${two?'two-way':'one-way'}`, rt.enable?'enabled':'disabled');
      if(rt.add)   push('Add/Update • ratings', two?'both directions':'from source → target');
      if(rt.remove)push('Remove • cleared ratings', two?'both directions':'from source → target');
      return;
    }

    if(state.feature==='history'){
      const hs=state.options.history||defaultForFeature('history');
      wrap.querySelector('#cx-notes')?.remove();
      push(`History • ${two?'two-way':'one-way'}`, hs.enable?'enabled':'disabled');
      if(hs.add)   push('Add • new plays', two?'both directions':'from source → target');
      if(hs.remove)push('Remove • deleted plays', two?'both directions':'from source → target');
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

  // Unified change handling (incl. power switch)
  wrap.addEventListener('change',(e)=>{
    const id = e.target.id;

    const map = {
      "cx-wl-enable":"cx-wl-remove",
      "cx-rt-enable":"cx-rt-remove",
      "cx-hs-enable":"cx-hs-remove",
      "cx-pl-enable":"cx-pl-remove"
    };
    if (map[id]) {
      const rm = document.getElementById(map[id]);
      if (rm) { rm.disabled = !e.target.checked; if (!e.target.checked) rm.checked = false; }
    }

    if (id.startsWith('cx-wl-')) {
      state.options.watchlist = {
        enable: !!document.getElementById('cx-wl-enable')?.checked,
        add:    !!document.getElementById('cx-wl-add')?.checked,
        remove: !!document.getElementById('cx-wl-remove')?.checked
      };
      state.visited.add('watchlist');
    }
    if (id.startsWith('cx-rt-')) {
      state.options.ratings = {
        enable: !!document.getElementById('cx-rt-enable')?.checked,
        add:    !!document.getElementById('cx-rt-add')?.checked,
        remove: !!document.getElementById('cx-rt-remove')?.checked
      };
      state.visited.add('ratings');
    }
    if (id.startsWith('cx-hs-')) {
      state.options.history = {
        enable: !!document.getElementById('cx-hs-enable')?.checked,
        add:    !!document.getElementById('cx-hs-add')?.checked,
        remove: !!document.getElementById('cx-hs-remove')?.checked
      };
      state.visited.add('history');
    }
    if (id.startsWith('cx-pl-')) {
      state.options.playlists = {
        enable: !!document.getElementById('cx-pl-enable')?.checked,
        add:    !!document.getElementById('cx-pl-add')?.checked,
        remove: !!document.getElementById('cx-pl-remove')?.checked
      };
      state.visited.add('playlists');
    }

    if(id.startsWith('gl-')){
      state.globals = {
        dry_run: !!wrap.querySelector('#gl-dry')?.checked,
        verify_after_write: !!wrap.querySelector('#gl-verify')?.checked,
        drop_guard: !!wrap.querySelector('#gl-drop')?.checked,
        allow_mass_delete: !!wrap.querySelector('#gl-mass')?.checked,
        tombstone_ttl_days: parseInt(wrap.querySelector('#gl-ttl')?.value||'0',10)||0,
        include_observed_deletes: !!wrap.querySelector('#gl-observed')?.checked
      };
    }

    // Reflect power switch instantly in flow rail
    if (id === 'cx-enabled') {
      updateFlowClasses();
    }

    updateRules();
    updateFlowClasses();
  });

  try{
    await loadProviders();
    hydrateSrcDstFromPair();
    renderProviderSelects();
    refreshTabs();
    updateFlow();
    updateFlowRailLogos?.();
    document.getElementById("cx-src")?.addEventListener("change",updateFlowRailLogos);
    document.getElementById("cx-dst")?.addEventListener("change",updateFlowRailLogos);
  }catch(_){}

  return wrap;
}

function cxBindCfgEvents(){
  const ids=[
    "cx-src","cx-dst","cx-mode-one","cx-mode-two",
    "cx-wl-enable","cx-wl-add","cx-wl-remove",
    "cx-rt-enable","cx-rt-add","cx-rt-remove",
    "cx-hs-enable","cx-hs-add","cx-hs-remove",
    "cx-pl-enable","cx-pl-add","cx-pl-remove",
    "cx-enabled"
  ];
  ids.forEach((id)=>{
    const el=document.getElementById(id); if(!el) return;
    el.addEventListener("change",(ev)=>{
      const id=ev.target?.id||"";
      const map={
        "cx-wl-enable":"cx-wl-remove",
        "cx-rt-enable":"cx-rt-remove",
        "cx-hs-enable":"cx-hs-remove",
        "cx-pl-enable":"cx-pl-remove"
      };
      if(map[id]){
        const rm=document.getElementById(map[id]);
        if(rm){ rm.disabled=!ev.target.checked; if(!ev.target.checked) rm.checked=false; }
      }
      try{cxUpdateSummary&&cxUpdateSummary();}catch(_){}
      if (id === "cx-enabled") updateFlowClasses();
    });
  });

  const save=document.getElementById("cx-save");
  if(save){
    save.addEventListener("click", async ()=>{
      try {
        const st = _cxGetState(); if (st) { (st.persistCurrentFeature || persistCurrentFeature)(); }
      } catch(_){}

      const state = _cxGetState() || {};

      const srcEl=document.getElementById("cx-src");
      const dstEl=document.getElementById("cx-dst");
      const twoEl=document.getElementById("cx-mode-two");
      const enEl =document.getElementById("cx-enabled");

      const wl = state.options?.watchlist || defaultForFeature('watchlist');
      const rt = state.options?.ratings   || defaultForFeature('ratings');
      const hs = state.options?.history   || defaultForFeature('history');
      const pl = state.options?.playlists || defaultForFeature('playlists');

      const data={
        source: state.src ?? (srcEl?srcEl.value:""),
        target: state.dst ?? (dstEl?dstEl.value:""),
        enabled: enEl?!!enEl.checked:true,
        mode: twoEl&&twoEl.checked?"two-way":"one-way",
        features:{
          watchlist:{ enable:!!wl.enable, add:!!wl.add, remove:!!wl.remove },
          ratings:  { enable:!!rt.enable, add:!!rt.add, remove:!!rt.remove },
          history:  { enable:!!hs.enable, add:!!hs.add, remove:!!hs.remove },
          playlists:{ enable:!!pl.enable, add:!!pl.add, remove:!!pl.remove }
        }
      };

      try{
        if(document.getElementById('gl-dry')){
          const sync={
            dry_run:!!document.getElementById('gl-dry')?.checked,
            verify_after_write:!!document.getElementById('gl-verify')?.checked,
            drop_guard:!!document.getElementById('gl-drop')?.checked,
            allow_mass_delete:!!document.getElementById('gl-mass')?.checked,
            tombstone_ttl_days:Math.max(0, parseInt(document.getElementById('gl-ttl')?.value||'0',10) || 0),
            include_observed_deletes:!!document.getElementById('gl-observed')?.checked
          };

          const curResp = await fetch('/api/config', { cache:'no-store' });
          const current = curResp.ok ? await curResp.json() : {};

          const cfg = (typeof structuredClone==='function')
            ? structuredClone(current||{})
            : JSON.parse(JSON.stringify(current||{}));
          cfg.sync = Object.assign({}, cfg.sync||{}, sync);

          const saveResp = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type':'application/json' },
            body: JSON.stringify(cfg)
          });
          if(!saveResp.ok) throw new Error(`POST /api/config ${saveResp.status}`);
        }
      }catch(e){console.warn('[cx] saving globals failed', e)}

      if(typeof window.cxSavePair==="function"){try{await window.cxSavePair(data);}catch(_){}} else {console.log("cxSavePair payload",data);}
      cxCloseModal&&cxCloseModal();
    });
  }
}

function openUpdateModal(){if(!_updInfo)return;document.getElementById("upd-modal").classList.remove("hidden");document.getElementById("upd-title").textContent=`v${_updInfo.latest}`;document.getElementById("upd-notes").textContent=_updInfo.notes||"(No release notes)";document.getElementById("upd-link").href=_updInfo.url||"#"}
function closeUpdateModal(){document.getElementById("upd-modal").classList.add("hidden")}
function dismissUpdate(){if(_updInfo?.latest){localStorage.setItem("dismissed_version",_updInfo.latest)}document.getElementById("upd-pill").classList.add("hidden");closeUpdateModal()}
function cxEditPair(id){const pr=(window.cx.pairs||[]).find((p)=>p.id===id);if(!pr)return;cxOpenModalFor(pr,id)}
function cxCloseModal(){try{persistCurrentFeature&&persistCurrentFeature();}catch(_){}const modal=document.getElementById("cx-modal");if(modal)modal.classList.add("hidden");window.cx.connect={source:null,target:null}}

function _ensureCfgModal(){
  if(document.getElementById("cx-modal"))return true;
  if(typeof cxEnsureCfgModal==="function"){cxEnsureCfgModal();return !!document.getElementById("cx-modal");}
  var wrap=document.createElement("div");
  wrap.id="cx-modal";wrap.className="modal-backdrop hidden";
  wrap.innerHTML=
    '<div class="modal-card"><div class="modal-header"><div class="title">Configure</div><button class="btn-ghost" onclick="cxCloseModal()">✕</button></div>'+
    '<div class="modal-body"><div class="form-grid">'+
    '<div class="field"><label for="cx-src">Source</label><select id="cx-src"><option>PLEX</option><option>SIMKL</option></select></div>'+
    '<div class="field"><label for="cx-dst">Target</label><select id="cx-dst"><option>PLEX</option><option>SIMKL</option></select></div>'+
    '</div><div class="form-grid" style="margin-top:8px">'+
    '<div class="field"><label>Mode</label><div class="seg">'+
    '<input id="cx-mode-one" type="radio" name="cx-mode" value="one-way" checked><label for="cx-mode-one">One-way</label>'+
    '<input id="cx-mode-two" type="radio" name="cx-mode" value="two-way"><label id="cx-two-label" for="cx-mode-two">Two-way</label>'+
    "</div></div>"+
    '<div class="field"><label>Enabled</label><div class="row"><input id="cx-enabled" type="checkbox" checked></div></div>'+
    "</div>"+
    '<div class="features"><div class="fe-row">'+
    '<div class="fe-name">Watchlist</div>'+
    '<label class="row"><input id="cx-wl-add" type="checkbox" checked><span>Add</span></label>'+
    '<label class="row"><input id="cx-wl-remove" type="checkbox"><span>Remove</span></label>'+
    '<div id="cx-wl-note" class="micro-note"></div>'+
    "</div></div></div>"+
    '<div class="modal-footer"><button class="btn acc" id="cx-save">Save</button><button class="btn" onclick="cxCloseModal()">Cancel</button></div></div>';
  document.body.appendChild(wrap);
  return true;
}

window.Modals = Object.assign(window.Modals||{},{
  openAbout,closeAbout,openUpdateModal,closeUpdateModal,dismissUpdate,
  cxEnsureCfgModal,cxBindCfgEvents,cxCloseModal,openPairModal,closePairModal,cxEditPair
});
Object.assign(window,{
  openAbout,closeAbout,openUpdateModal,closeUpdateModal,dismissUpdate,
  cxEnsureCfgModal,cxBindCfgEvents,cxCloseModal,openPairModal,closePairModal,cxEditPair
});
