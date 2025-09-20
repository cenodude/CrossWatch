// modals.js — Configure Connection modal UI and helpers  (Ratings-enabled build)

/* ----------------------------- Utilities / state ----------------------------- */

// Retrieve current modal state (from DOM or global cache)
function _cxGetState() {
  const el = document.getElementById('cx-modal');
  return (el && el.__state) || window.__cxState || null;
}

function openPairModal(){const m=document.getElementById("pairModal");if(m)m.classList.remove("hidden")}
function closePairModal(){const m=document.getElementById("pairModal");if(m)m.classList.add("hidden")}

/* ---------- Helper: provider detection (case-insensitive & label fallback) --- */

// Returns true if value equals "simkl" (case-insensitive)
function __isSimkl(val){ return String(val||'').trim().toLowerCase() === 'simkl'; }

// Robust check: by state values, selects and visible labels
function __pairHasSimkl(state){
  try{
    if(__isSimkl(state?.src) || __isSimkl(state?.dst)) return true;
    const srcSelVal = document.getElementById('cx-src')?.value || document.getElementById('cx-src-display')?.dataset.value || '';
    const dstSelVal = document.getElementById('cx-dst')?.value || document.getElementById('cx-dst-display')?.dataset.value || '';
    if(__isSimkl(srcSelVal) || __isSimkl(dstSelVal)) return true;
    const srcText = document.getElementById('cx-src-display')?.textContent || '';
    const dstText = document.getElementById('cx-dst-display')?.textContent || '';
    if(/simkl/i.test(srcText) || /simkl/i.test(dstText)) return true;
  }catch(_){}
  return false;
}

/* ---------- NEW: draggable modal + compact helpers (summary-safe) ---------- */

// Inject minimal CSS for dragging + small inputs + preview-wide
function ensureCxDragAndCompactCSS(){
  if (!document.getElementById('cx-drag-compact-css')) {
    const st = document.createElement('style');
    st.id = 'cx-drag-compact-css';
    st.textContent = `
      /* Draggable shell: we flip to fixed positioning while dragging */
      #cx-modal.cx-draggable .modal-shell{ position:fixed; left:50%; top:50%; transform:translate(-50%,-50%); }
      #cx-modal[data-dragging="true"] .modal-shell{ transform:none; transition:none; }
      #cx-modal .cx-head{ cursor:move; user-select:none; }
      #cx-modal .cx-head .switch, #cx-modal .cx-head input, #cx-modal .cx-head button{ cursor:auto; } /* don't show move over controls */

      /* Small input sizing helpers */
      #cx-modal .grid2.compact{ grid-template-columns: 1fr 1fr; gap:8px 10px; }
      #cx-modal .opt-row .input.small{ height:28px; padding:6px 8px; }

      /* Preview tab pill style */
      #cx-feat-tabs .ftab.preview{
        border-color:#6b8cff; color:#dfe6ff;
        background:linear-gradient(180deg, rgba(107,140,255,.12), rgba(107,140,255,.04));
      }
      #cx-feat-tabs .ftab.preview.active{
        background:linear-gradient(180deg, rgba(107,140,255,.20), rgba(107,140,255,.08));
      }

      /* Preview spans full width and the rule list is 2 columns */
      #cx-modal.cx-preview .cx-main{ display:block; }
      #cx-modal.cx-preview .cx-main .right{ display:none !important; }
      #cx-modal.cx-preview .cx-main .left{ width:100%; }

      /* Two-column grid for the rule list */
      #cx-modal.cx-preview .panel .rules{
        display:grid !important;
        grid-template-columns: repeat(2, minmax(0,1fr));
        gap:12px 16px;
        align-content:start;
      }
      #cx-modal.cx-preview .panel .rules .r{
        margin:0;
        grid-column:auto;
      }

      /* Fall back to one column on narrow screens */
      @media (max-width: 980px){
        #cx-modal.cx-preview .panel .rules{
          grid-template-columns: 1fr;
        }
      }
    `;
    document.head.appendChild(st);
  }
}
try{ ensureCxDragAndCompactCSS(); }catch(_){}

/**
 * Make the main modal draggable by its header (cx-head).
 * - Remembers last position per session (per page load).
 */
function installModalDrag(wrap){
  if (!wrap || wrap.__dragInstalled) return;
  wrap.__dragInstalled = true;
  wrap.classList.add('cx-draggable');

  const shell = wrap.querySelector('.modal-shell');
  const head  = wrap.querySelector('.cx-head');
  if (!shell || !head) return;

  // Restore last position if any
  const pos = (wrap.__dragPos || null);
  if (pos && Number.isFinite(pos.left) && Number.isFinite(pos.top)) {
    shell.style.position = 'fixed';
    shell.style.left = pos.left + 'px';
    shell.style.top  = pos.top  + 'px';
    shell.style.transform = 'none';
  }

  let dragging = false, sx = 0, sy = 0, startLeft = 0, startTop = 0, pid = null;

  function clamp(n, a, b){ return Math.max(a, Math.min(b, n)); }
  function bounds(){
    const vw = Math.max(320, window.innerWidth  || 0);
    const vh = Math.max(240, window.innerHeight || 0);
    const r  = shell.getBoundingClientRect();
    // Keep at least 32px visible horizontally, 48px vertically
    const minL = 32 - r.width;
    const maxL = vw - 32;
    const minT = 48 - r.height;
    const maxT = vh - 48;
    return { minL, maxL, minT, maxT };
  }

  function start(e){
    if (e.button !== 0) return;                                    // left click only
    if (e.target.closest('input,select,textarea,button,.switch')) return; // don’t grab controls
    dragging = true;
    pid = e.pointerId ?? null;
    head.setPointerCapture?.(pid);
    const r = shell.getBoundingClientRect();
    startLeft = r.left;
    startTop  = r.top;
    sx = e.clientX; sy = e.clientY;
    shell.style.position = 'fixed';
    shell.style.left     = startLeft + 'px';
    shell.style.top      = startTop  + 'px';
    shell.style.transform = 'none';
    wrap.dataset.dragging = 'true';
    e.preventDefault();
  }
  function move(e){
    if (!dragging) return;
    const dx = e.clientX - sx;
    const dy = e.clientY - sy;
    const lim = bounds();
    const nx = clamp(startLeft + dx, lim.minL, lim.maxL);
    const ny = clamp(startTop  + dy, lim.minT, lim.maxT);
    shell.style.left = nx + 'px';
    shell.style.top  = ny + 'px';
  }
  function end(){
    if (!dragging) return;
    dragging = false;
    wrap.dataset.dragging = 'false';
    head.releasePointerCapture?.(pid);
    pid = null;
    // Persist last position
    const r = shell.getBoundingClientRect();
    wrap.__dragPos = { left: Math.round(r.left), top: Math.round(r.top) };
  }

  head.addEventListener('pointerdown', start, true);
  head.addEventListener('pointermove', move,  true);
  head.addEventListener('pointerup',   end,   true);
  head.addEventListener('pointercancel', end, true);

  // Re-clamp on resize
  window.addEventListener('resize', ()=>{
    if (!wrap.__dragPos) return;
    const lim = bounds();
    const nx = Math.max(lim.minL, Math.min(lim.maxL, wrap.__dragPos.left));
    const ny = Math.max(lim.minT, Math.min(lim.maxT, wrap.__dragPos.top));
    shell.style.left = nx + 'px';
    shell.style.top  = ny + 'px';
    wrap.__dragPos = { left:nx, top:ny };
  }, { passive:true });
}

/**
 * (Summary pills for Ratings advanced — safe no-op if container not present)
 */
function updateRtCompactSummary(){
  const m  = document.getElementById('cx-modal');
  const st = m?.__state;
  const rt = st?.options?.ratings || {};
  const det = document.getElementById('cx-rt-adv');
  const sum = det?.querySelector('summary');
  if (!sum) return;

  const types = Array.isArray(rt.types) && rt.types.length ? rt.types.join(', ') : 'movies, shows';
  const modeMap = { only_new: 'New since last sync', from_date: 'From a date', all: 'Everything' };
  const mode = modeMap[String(rt.mode||'only_new')] || 'New since last sync';
  const from = (rt.mode === 'from_date' && rt.from_date) ? `From ${rt.from_date}` : '';

  sum.innerHTML = `
    <span class="pill">Types: ${types}</span>
    <span class="summary-gap">•</span>
    <span class="pill">Mode: ${mode}${from ? ` — ${from}` : ''}</span>
  `;
  sum.setAttribute('aria-expanded', det.open ? 'true' : 'false');
}

/* -------------------------- Lightweight event binder ------------------------ */
// Purpose: bind small reactive bits that aren't tied to a specific render pass.
function cxBindCfgEvents(){
  try{
    const _m1=document.getElementById('cx-mode-one');
    const _m2=document.getElementById('cx-mode-two');
    const _bind=(el)=>{ if(!el) return; el.addEventListener('change', ()=>{ try{ const wrap=document.getElementById('cx-modal'); wrap?.__updateFlow && wrap.__updateFlow(true); }catch(_){} }, true); };
    _bind(_m1); _bind(_m2);
  }catch(_){}

  // Keep this list in sync with visible controls
  const ids=[
    "cx-src","cx-dst","cx-mode-one","cx-mode-two",
    "cx-wl-enable","cx-wl-add","cx-wl-remove",
    "cx-rt-enable","cx-rt-add","cx-rt-remove",
    "cx-rt-type-all","cx-rt-type-movies","cx-rt-type-shows","cx-rt-type-seasons","cx-rt-type-episodes",
    "cx-rt-mode","cx-rt-from-date",
    "cx-hs-enable","cx-hs-add","cx-hs-remove",
    "cx-pl-enable","cx-pl-add","cx-pl-remove",
    "cx-enabled"
  ];
  ids.forEach((id)=>{
    const el=document.getElementById(id); if(!el) return;
    el.addEventListener("change",(ev)=>{
      const id=ev.target?.id||"";
      const map={"cx-wl-enable":"cx-wl-remove","cx-rt-enable":"cx-rt-remove","cx-hs-enable":"cx-hs-remove","cx-pl-enable":"cx-pl-remove"};
      if(map[id]){
        const rm=document.getElementById(map[id]);
        if(rm){rm.disabled=!ev.target.checked; if(!ev.target.checked) rm.checked=false;}
      }
      if(id==="cx-enabled"){
        const m=document.getElementById('cx-modal');
        m?.__updateFlowClasses?.();
      }
      try{ updateRtCompactSummary && updateRtCompactSummary(); }catch(_){}
    });
  });
}
// Expose as global explicitly (safer in module/strict environments)
if (typeof window !== 'undefined') window.cxBindCfgEvents = window.cxBindCfgEvents || cxBindCfgEvents;

/* ------------------------------- Update modal ------------------------------- */

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


/* --------------------------------- About UI -------------------------------- */

async function openAbout(){
  try{
    const r = await fetch("/api/version?cb=" + Date.now(), { cache: "no-store" });
    const j = r.ok ? await r.json() : {};
    const cur = (j.current ?? "0.0.0").toString().trim();
    const latest = (j.latest ?? "").toString().trim() || null;
    const url = j.html_url || "https://github.com/cenodude/crosswatch/releases";
    const upd = !!j.update_available;

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

    // Modules table
    try{
      const mr = await fetch("/api/modules/versions?cb=" + Date.now(), { cache: "no-store" });
      if (!mr.ok) throw new Error(String(mr.status));
      const mv = await mr.json();

      const body = document.querySelector('#about-backdrop .modal-card .modal-body');
      const firstGrid = body?.querySelector('.about-grid');
      if (!body || !firstGrid) throw new Error("about body/grid missing");

      body.querySelector("#about-mods")?.remove();

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

/* ---------------------- Ensure configuration main modal --------------------- */

async function cxEnsureCfgModal() {
  // De-dupe
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
                <div class="app-sub">Choose source → target and what to sync</div>
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

            <!-- Both columns are for options; preview spans both -->
            <div class="cx-main">
              <div class="left"><div class="panel" id="cx-feat-panel"></div></div>
              <div class="right"><div class="panel" id="cx-adv-panel"></div></div>
            </div>
          </div>
          <!-- Sticky footer is injected separately -->
        </div>
      </div>`;
    document.body.appendChild(wrap);
    try{ installModalDrag && installModalDrag(wrap); }catch(_){}

    // Local modal state (Ratings with simple advanced: types/mode/from_date)
    wrap.__state={
      providers:[], src:null, dst:null, feature:'globals',
      options:{
        watchlist:{enable:true,add:true,remove:false},
        ratings:{
          enable:false, add:false, remove:false,
          types:['movies','shows'],   // per-pair scope
          mode:'only_new',            // 'only_new' | 'from_date' | 'all'
          from_date:''                // ISO date, used only when mode==='from_date'
        },
        history:{enable:false,add:false,remove:false},
        playlists:{enable:false,add:false,remove:false}
      },
      globals:null,
      visited:new Set()
    };
    window.__cxState=wrap.__state;

    // Bind light global events if present (safe no-op otherwise)
    if (typeof window !== 'undefined' && typeof window.cxBindCfgEvents === 'function') {
      window.cxBindCfgEvents();
    }
  }

  /* --------------------------- Modal helper methods -------------------------- */

  wrap.__persistCurrentFeature = function persistCurrentFeature(){
    const state=wrap.__state||{};
    const f=state.feature;
    const pick=(on,add,rem)=>({enable:!!on?.checked,add:!!add?.checked,remove:!!rem?.checked});

    if(f==='watchlist') state.options.watchlist=pick(
      document.getElementById('cx-wl-enable'),
      document.getElementById('cx-wl-add'),
      document.getElementById('cx-wl-remove')
    );

    if(f==='ratings'){
      const base = pick(
        document.getElementById('cx-rt-enable'),
        document.getElementById('cx-rt-add'),
        document.getElementById('cx-rt-remove')
      );

      // Advanced: derive types/mode/from_date from UI
      const allOn = !!document.getElementById('cx-rt-type-all')?.checked;
      const types = allOn ? ['movies','shows','seasons','episodes'] : (()=>{
        const arr=[]; ['movies','shows','seasons','episodes'].forEach(t=>{
          if(document.getElementById(`cx-rt-type-${t}`)?.checked) arr.push(t);
        });
        return arr;
      })();

      const mode = (document.getElementById('cx-rt-mode')?.value || 'only_new').toString();
      const from_date = (document.getElementById('cx-rt-from-date')?.value || '').trim();

      state.options.ratings = Object.assign({}, state.options.ratings || {}, base, { types, mode, from_date });
    }

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

    // Ratings carries only supported fields (unknowns safely ignored by backend)
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

  // Persist global sync options to /api/config when present in the UI
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

  // Pull toggles from DOM into state (idempotent)
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
    if(rt_on||rt_add||rt_rem){
      const base = pick(rt_on, rt_add, rt_rem);
      const allOn = !!document.getElementById('cx-rt-type-all')?.checked;
      const types = allOn ? ['movies','shows','seasons','episodes'] : (()=>{
        const arr=[]; ['movies','shows','seasons','episodes'].forEach(t=>{
          if(document.getElementById(`cx-rt-type-${t}`)?.checked) arr.push(t);
        });
        return arr;
      })();
      const mode = (document.getElementById('cx-rt-mode')?.value || 'only_new').toString();
      const from_date = (document.getElementById('cx-rt-from-date')?.value || '').trim();

      st.options.ratings = Object.assign({}, st.options.ratings || {}, base, { types, mode, from_date });
    }

    const hs_on=document.getElementById('cx-hs-enable'),
          hs_add=document.getElementById('cx-hs-add'),
          hs_rem=document.getElementById('cx-hs-remove');
    if(hs_on||hs_add||hs_rem) st.options.history   = pick(hs_on, hs_add, hs_rem);

    const pl_on=document.getElementById('cx-pl-enable'),
          pl_add=document.getElementById('cx-pl-add'),
          pl_rem=document.getElementById('cx-pl-remove');
    if(pl_on||pl_add||pl_rem) st.options.playlists = pick(pl_on, pl_add, pl_rem);
  };

  // Save flow: write globals (if visible) and pair via host bridge
  wrap.__doSave = async function doSave(){
    if(wrap.__saving) return;
    wrap.__saving = true;
    try{
      wrap.__persistCurrentFeature && wrap.__persistCurrentFeature();
      wrap.__snapshotAll && wrap.__snapshotAll();

      const payload = wrap.__buildPairPayload ? wrap.__buildPairPayload() : null;
      if(!payload){ console.warn('[cx] no payload to save'); return; }

      await (wrap.__saveGlobalsIfPresent && wrap.__saveGlobalsIfPresent());

      if(typeof window.cxSavePair === 'function'){
        const res = await Promise.resolve(window.cxSavePair(payload, payload.id||''));
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

  /* ---------------------------- Providers / tabs ----------------------------- */

  const __TEMP_DISABLED=new Set(["history","playlists"]);
  const DEFAULT_GLOBALS={dry_run:false,verify_after_write:false,drop_guard:false,allow_mass_delete:true,tombstone_ttl_days:30,include_observed_deletes:true};
  const state=wrap.__state;
  state.globals={...DEFAULT_GLOBALS};

  // Load globals for display
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
  const byName=(n)=>state.providers.find(p=>p.name===n);
  const commonFeatures=()=>!state.src||!state.dst?[]:['watchlist','ratings','history','playlists'].filter(k=>byName(state.src)?.features?.[k]&&byName(state.dst)?.features?.[k]);
  const defaultForFeature=(k)=>k==='watchlist'?{enable:true,add:true,remove:false}:k==='playlists'?{enable:false,add:true,remove:false}:{enable:false,add:false,remove:false};

  function getOpts(key){
    if(!state.visited.has(key)){
      if(key==='ratings'){
        state.options.ratings = Object.assign({
          enable:false,add:false,remove:false,
          types:['movies','shows'],
          mode:'only_new',
          from_date:''
        }, state.options.ratings||{});
      }else{
        state.options[key]=state.options[key]??defaultForFeature(key);
      }
      state.visited.add(key);
    }
    return state.options[key];
  }

  /* ------------------------------- UI builders ------------------------------- */

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

  function restartFlowAnimation(mode){
    const rail=wrap.querySelector('#cx-flow-rail');
    if(!rail) return;
    const arrow=rail.querySelector('.arrow');
    const dots=[...rail.querySelectorAll('.dot.flow')];
    rail.classList.remove('anim-one','anim-two');
    arrow?.classList.remove('anim-one','anim-two');
    dots.forEach(d=>d.classList.remove('anim-one','anim-two'));
    void rail.offsetWidth;
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

  // Build rules HTML once per render request (used by the Preview tab)
  function buildRulesHTML(){
    const two = wrap.querySelector('#cx-mode-two')?.checked;
    const rules = [];
    const push=(t,s='')=>rules.push(`<div class="r"><div class="t">${t}</div>${s?`<div class="s">${s}</div>`:''}</div>`);

    const show = (k)=> byName(state.src)?.features?.[k] && byName(state.dst)?.features?.[k];

    if(show('watchlist')){
      const wl=state.options.watchlist||defaultForFeature('watchlist');
      push(`Watchlist • ${two?'two-way':'one-way'}`, wl.enable?'enabled':'disabled');
      if(wl.add)    push('Add • new items', two?'both directions':'from source → target');
      if(wl.remove) push('Delete • when removed', two?'both directions':'from source → target');
    }

    if(show('ratings')){
      const rt=state.options.ratings||{enable:false,add:false,remove:false,types:['movies','shows'],mode:'only_new',from_date:''};
      push(`Ratings • ${two?'two-way':'one-way'}`, rt.enable?'enabled':'disabled');
      if(rt.add)    push('Add/Update • ratings', two?'both directions':'from source → target');
      if(rt.remove) push('Remove • cleared ratings', two?'both directions':'from source → target');
      if(rt.enable){
        const types = Array.isArray(rt.types)&&rt.types.length ? rt.types.join(', ') : 'movies, shows';
        const modeMap = { only_new:'new since last sync', from_date:`from ${rt.from_date||'—'}`, all:'everything' };
        const win = modeMap[String(rt.mode||'only_new')] || 'new since last sync';
        push('Scope', `types: ${types}`);
        push('Window', win);
      }
    }

    if(show('history')){
      const hs=state.options.history||defaultForFeature('history');
      push(`History • ${two?'two-way':'one-way'}`, hs.enable?'enabled':'disabled');
      if(hs.add)    push('Add • new plays', two?'both directions':'from source → target');
      if(hs.remove) push('Remove • deleted plays', two?'both directions':'from source → target');
    }

    if(show('playlists')){
      const pl=state.options.playlists||defaultForFeature('playlists');
      push(`Playlists • ${two?'two-way':'one-way'}`, pl.enable?'enabled':'disabled');
      if(pl.add)    push('Add/Update • playlist items', two?'both directions':'from source → target');
      if(pl.remove) push('Remove • items not present', two?'both directions':'from source → target');
    }

    return rules.join("") || '<div class="r note">No rules yet. Configure a feature first.</div>';
  }

  function refreshTabs(){
    const tabs=wrap.querySelector("#cx-feat-tabs"); if(!tabs) return;
    tabs.innerHTML="";
    const items=["globals",...commonFeatures(),"preview"];
    if(items.length===0){ tabs.innerHTML='<div class="ftab disabled">No common features</div>'; renderFeaturePanel(); return; }

    const LABELS={globals:"Globals",watchlist:"Watchlist",ratings:"Ratings",history:"History",playlists:"Playlists",preview:"Rule preview"};
    items.forEach((k)=>{
      const btn=document.createElement("button");
      btn.className="ftab" + (k==="preview" ? " preview" : "");
      btn.dataset.key=k; btn.textContent=LABELS[k]||(k[0]?.toUpperCase?.()+k.slice(1));
      if(__TEMP_DISABLED.has(k) && k!=="preview"){ btn.classList.add("disabled"); btn.setAttribute("aria-disabled","true"); btn.title="Coming soon"; }
      else { btn.onclick=()=>{ wrap.__persistCurrentFeature(); state.feature=k; refreshTabs(); renderFeaturePanel(); }; }
      if(state.feature===k) btn.classList.add("active");
      tabs.appendChild(btn);
    });

    if(!items.includes(state.feature) || (__TEMP_DISABLED.has(state.feature)&&state.feature!=="preview")){
      state.feature=items.find((k)=>k!=="globals"&&!__TEMP_DISABLED.has(k))||"globals";
    }
    [...tabs.children].forEach((c)=>c.classList.toggle("active",c.dataset.key===state.feature));
    renderFeaturePanel();
  }

  // apply help hints to whatever controls are currently visible
  function seedHelpForVisibleControls(){
    const set = (id, text)=>{ const el=document.getElementById(id); if(el && typeof window.__cxSetHelp==='function'){ window.__cxSetHelp(id, text); } };

    // Ratings (pair-level)
    set('cx-rt-enable',    'Toggle ratings sync for this connection.');
    set('cx-rt-add',       'Write ratings that exist in the source but not in the target, or update when values differ.');
    set('cx-rt-remove',    'WARNING! Clear ratings on the other side when they are cleared at the source. Use with caution.');
    set('cx-rt-type-all',  'Select all item kinds (Movies, Shows, Seasons, Episodes).');
    set('cx-rt-mode',      'How much rating history to consider on the source side.');
    set('cx-rt-from-date', 'Lower bound for rated_at, used only when mode is "From a date".');

    // Watchlist
    set('cx-wl-enable',    'Toggle watchlist sync for this connection.');
    set('cx-wl-add',       'Add items present in the source watchlist to the target.');
    set('cx-wl-remove',    'Remove items from the target when they are removed at the source.');

    // History
    set('cx-hs-enable',    'Toggle playback history sync for this connection.');
    set('cx-hs-add',       'Add new plays that exist in the source but not in the target.');
    set('cx-hs-remove',    'Remove plays in the target when removed in the source.');

    // Globals
    set('gl-dry',          'Simulate changes only. No writes will be sent to providers.');
    set('gl-verify',       'After writing, re-check a small sample to ensure it stuck.');
    set('gl-drop',         'Protect against accidental mass drops when the source snapshot is unexpectedly empty.');
    set('gl-mass',         'Allow large deletions when they are truly intended.');
    set('gl-ttl',          'How long to keep delete tombstones (in days). Prevents re-adding items you removed.');
    set('gl-observed',     'Consider removals inferred from provider activity streams, not just from snapshot diffs.');
  }

  function renderFeaturePanel(){
    const left=wrap.querySelector("#cx-feat-panel");
    const right=wrap.querySelector("#cx-adv-panel");
    if(!left||!right) return;

    // toggle preview-wide layout class
    wrap.classList.toggle('cx-preview', (state.feature==="preview"));

    /* ----------------------------- Globals tab ------------------------------ */
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
      right.innerHTML=`
        <div class="panel-title">More options</div>
        <div class="opt-row"><label for="gl-ttl">Tombstone TTL (days)</label><input id="gl-ttl" class="input" type="number" min="0" step="1" value="${g.tombstone_ttl_days??30}"></div>
        <div class="muted">Keep delete markers to avoid re-adding.</div>
        <div class="opt-row"><label for="gl-observed">Include observed deletes</label><label class="switch"><input id="gl-observed" type="checkbox" ${g.include_observed_deletes?"checked":""}><span class="slider"></span></label></div>
        <div class="muted">Apply deletions detected from activity.</div>`;
      seedHelpForVisibleControls();
      return;
    }

    /* ---------------------------- Preview tab ------------------------------- */
    if(state.feature==="preview"){
      left.innerHTML = `
        <div class="panel-title">Rule preview</div>
        <div class="sub">Short overview based on your selections.</div>
        <div class="rules" id="cx-rules">${buildRulesHTML()}</div>`;
      right.innerHTML = ''; // hidden in cx-preview mode
      return;
    }

    /* ----------------------------- Watchlist tab ---------------------------- */
    if(state.feature==="watchlist"){
      const wl=getOpts("watchlist");
      left.innerHTML=`
        <div class="panel-title">Watchlist — basics</div>
        <div class="opt-row"><label for="cx-wl-enable">Enable</label><label class="switch"><input id="cx-wl-enable" type="checkbox" ${wl.enable?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-wl-add">Add</label><label class="switch"><input id="cx-wl-add" type="checkbox" ${wl.add?"checked":""}><span class="slider"></span></label></div>
        <div class="opt-row"><label for="cx-wl-remove">Remove</label><label class="switch"><input id="cx-wl-remove" type="checkbox" ${wl.remove?"checked":""}><span class="slider"></span></label></div>`;
      right.innerHTML=`
        <div class="panel-title">Advanced</div>
        <div class="muted">Filters and conflict options will appear here.</div>`;
      const wlRem=document.getElementById("cx-wl-remove");
      if(wlRem){wlRem.disabled=!wl.enable;if(!wl.enable&&wlRem.checked) wlRem.checked=false;}
      seedHelpForVisibleControls();
      return;
    }

    /* ------------------------------ Ratings tab ----------------------------- */
    if(state.feature==="ratings"){
      const rt=getOpts("ratings");
      const hasType = (t)=>Array.isArray(rt.types) && rt.types.includes(t);

      // basics + SCOPE on the left
      left.innerHTML=`
        <div class="panel-title">Ratings — basics</div>
        <div class="opt-row">
          <label for="cx-rt-enable">Enable</label>
          <label class="switch">
            <input id="cx-rt-enable" type="checkbox" ${rt.enable?"checked":""}>
            <span class="slider"></span>
          </label>
        </div>
        <div class="muted">Synchronize user ratings between providers.</div>
        <div class="grid2">
          <div class="opt-row">
            <label for="cx-rt-add">Add / Update</label>
            <label class="switch">
              <input id="cx-rt-add" type="checkbox" ${rt.add?"checked":""}>
              <span class="slider"></span>
            </label>
          </div>
          <div class="opt-row">
            <label for="cx-rt-remove">Remove (clear)</label>
            <label class="switch">
              <input id="cx-rt-remove" type="checkbox" ${rt.remove?"checked":""}>
              <span class="slider"></span>
            </label>
          </div>
        </div>

        <div class="panel-title small">Scope</div>
        <div class="grid2 compact" id="cx-rt-types-wrap">
          <div class="opt-row">
            <label for="cx-rt-type-all">All</label>
            <label class="switch">
              <input id="cx-rt-type-all" type="checkbox" ${(hasType('movies')&&hasType('shows')&&hasType('seasons')&&hasType('episodes'))?"checked":""}>
              <span class="slider"></span>
            </label>
          </div>
          <div class="opt-row">
            <label for="cx-rt-type-movies">Movies</label>
            <label class="switch">
              <input id="cx-rt-type-movies" type="checkbox" ${hasType('movies')?"checked":""}>
              <span class="slider"></span>
            </label>
          </div>
          <div class="opt-row">
            <label for="cx-rt-type-shows">Shows</label>
            <label class="switch">
              <input id="cx-rt-type-shows" type="checkbox" ${hasType('shows')?"checked":""}>
              <span class="slider"></span>
            </label>
          </div>
          <div class="opt-row">
            <label for="cx-rt-type-seasons">Seasons</label>
            <label class="switch">
              <input id="cx-rt-type-seasons" type="checkbox" ${hasType('seasons')?"checked":""}>
              <span class="slider"></span>
            </label>
          </div>
          <div class="opt-row">
            <label for="cx-rt-type-episodes">Episodes</label>
            <label class="switch">
              <input id="cx-rt-type-episodes" type="checkbox" ${hasType('episodes')?"checked":""}>
              <span class="slider"></span>
            </label>
          </div>
        </div>`;

      // advanced on the right (summary pills + history window + SIMKL warning)
      const __simklInPair = __pairHasSimkl(state); // robust detection
      let __advHTML=`
        <div class="panel-title">Advanced</div>
        <details id="cx-rt-adv" open>
          <summary class="muted" style="margin-bottom:10px;"></summary>

          <div class="panel-title small">History window</div>
          <div class="grid2">
            <div class="opt-row">
              <label for="cx-rt-mode">Mode</label>
              <select id="cx-rt-mode" class="input">
                <option value="only_new" ${rt.mode==='only_new'?'selected':''}>New since last sync</option>
                <option value="from_date" ${rt.mode==='from_date'?'selected':''}>From a date…</option>
                <option value="all" ${rt.mode==='all'?'selected':''}>Everything (advanced)</option>
              </select>
            </div>
            <div class="opt-row">
              <label for="cx-rt-from-date">From date</label>
              <input id="cx-rt-from-date" class="input small" type="date" value="${(rt.from_date||'')}" ${rt.mode==='from_date'?'':'disabled'}>
            </div>
          </div>
          <div class="hint">“New since last sync” is safe and automatic. Use “From a date” once to import a slice of history, or “Everything” for full backfill.</div>
        </details>`;
      if(__simklInPair){
        __advHTML += `
          <div class="simkl-alert" role="note" aria-live="polite">
            <div class="title"><span class="ic">⚠</span> Simkl heads-up for Ratings</div>
            <div class="body">
              <ul class="bul">
                <li><b>Movies:</b> Rating auto-marks as <i>Completed</i> on Simkl (server-side).</li>
                <li>These may surface under <i>Recently watched</i> and <i>My List</i>.</li>
              </ul>
              <div class="mini">Tip: Prefer <b>Mode: New since last sync</b> and avoid sending ratings for items you haven't watched.</div>
            </div>
          </div>`;
      }
      right.innerHTML = __advHTML;

      const rm=document.getElementById("cx-rt-remove");
      const en=document.getElementById("cx-rt-enable");
      if(rm){ rm.disabled = !rt.enable; if(!rt.enable && rm.checked) rm.checked=false; }
      if(en){ en.addEventListener('change', ()=>{ const on=en.checked; if(rm){ rm.disabled=!on; if(!on) rm.checked=false; }}, {once:true}); }

      try{ updateRtCompactSummary && updateRtCompactSummary(); }catch(_){}
      seedHelpForVisibleControls();
      return;
    }

    // Disabled/coming-soon features
    left.innerHTML=`<div class="panel-title">${state.feature[0].toUpperCase()+state.feature.slice(1)} options</div><div class="muted" style="padding:18px 0;">Temporarily unavailable. This will be enabled in a future update.</div>`;
    right.innerHTML=`<div class="panel-title">Advanced</div><div class="muted">Nothing to configure yet.</div>`;
    seedHelpForVisibleControls();
  }

  // Change wiring
  wrap.addEventListener('change',(e)=>{
    const id=e.target.id;
    const map={"cx-wl-enable":"cx-wl-remove","cx-rt-enable":"cx-rt-remove","cx-hs-enable":"cx-hs-remove","cx-pl-enable":"cx-pl-remove"};
    if(map[id]){const rm=document.getElementById(map[id]); if(rm){rm.disabled=!e.target.checked; if(!e.target.checked) rm.checked=false;}}
    if(id.startsWith('cx-wl-')){
      state.options.watchlist={
        enable:!!document.getElementById('cx-wl-enable')?.checked,
        add:!!document.getElementById('cx-wl-add')?.checked,
        remove:!!document.getElementById('cx-wl-remove')?.checked
      }; state.visited.add('watchlist');
    }
    if(id.startsWith('cx-rt-')){
      // Handle ALL toggle for types
      if(id==='cx-rt-type-all'){
        const on = !!document.getElementById('cx-rt-type-all')?.checked;
        ['movies','shows','seasons','episodes'].forEach(t=>{
          const cb=document.getElementById(`cx-rt-type-${t}`);
          if(cb){ cb.checked = on; }
        });
      }else if(/^cx-rt-type-(movies|shows|seasons|episodes)$/.test(id)){
        const allOn = ['movies','shows','seasons','episodes'].every(t=>document.getElementById(`cx-rt-type-${t}`)?.checked);
        const allCb = document.getElementById('cx-rt-type-all'); if(allCb) allCb.checked = !!allOn;
      }
      if(id==='cx-rt-mode'){
        const md=(document.getElementById('cx-rt-mode')?.value||'only_new');
        const fd=document.getElementById('cx-rt-from-date');
        if(fd){ fd.disabled = (md!=='from_date'); if(md!=='from_date'){ fd.value=''; } }
      }

      const rt=state.options.ratings||{};
      const types = (()=>{
        if(document.getElementById('cx-rt-type-all')?.checked) return ['movies','shows','seasons','episodes'];
        const arr=[]; ['movies','shows','seasons','episodes'].forEach(t=>{
          if(document.getElementById(`cx-rt-type-${t}`)?.checked) arr.push(t);
        });
        return arr;
      })();

      state.options.ratings=Object.assign({}, rt, {
        enable:!!document.getElementById('cx-rt-enable')?.checked,
        add:!!document.getElementById('cx-rt-add')?.checked,
        remove:!!document.getElementById('cx-rt-remove')?.checked,
        types,
        mode:(document.getElementById('cx-rt-mode')?.value||rt.mode||'only_new'),
        from_date:(document.getElementById('cx-rt-from-date')?.value||'').trim()
      });
      state.visited.add('ratings');
    }
    if(id.startsWith('cx-hs-')){
      state.options.history  ={enable:!!document.getElementById('cx-hs-enable')?.checked,add:!!document.getElementById('cx-hs-add')?.checked,remove:!!document.getElementById('cx-hs-remove')?.checked}; state.visited.add('history');
    }
    if(id.startsWith('cx-pl-')){
      state.options.playlists={enable:!!document.getElementById('cx-pl-enable')?.checked,add:!!document.getElementById('cx-pl-add')?.checked,remove:!!document.getElementById('cx-pl-remove')?.checked}; state.visited.add('playlists');
    }
    if(id.startsWith('gl-')){
      state.globals={
        dry_run:!!wrap.querySelector('#gl-dry')?.checked,
        verify_after_write:!!wrap.querySelector('#gl-verify')?.checked,
        drop_guard:!!wrap.querySelector('#gl-drop')?.checked,
        allow_mass_delete:!!wrap.querySelector('#gl-mass')?.checked,
        tombstone_ttl_days:parseInt(wrap.querySelector('#gl-ttl')?.value||'0',10)||0,
        include_observed_deletes:!!wrap.querySelector('#gl-observed')?.checked
      };
    }
    if(id==='cx-enabled'||id==='cx-mode-one'||id==='cx-mode-two') updateFlow(true);

    // Keep Preview live if it's the active tab
    if(state.feature==='preview') renderFeaturePanel();

    try{ updateRtCompactSummary && updateRtCompactSummary(); }catch(_){}
    updateFlowClasses();
  });

  // First render
  try{
    await loadProviders();
    renderProviderSelects();
    refreshTabs();
    updateFlow(true);
    updateFlowRailLogos?.();
    document.getElementById("cx-src")?.addEventListener("change",updateFlowRailLogos);
    document.getElementById("cx-dst")?.addEventListener("change",updateFlowRailLogos);
  }catch(_){}

  // Expose internal API
  wrap.__refreshTabs = refreshTabs;
  wrap.__renderProviderSelects = renderProviderSelects;
  wrap.__updateFlow = updateFlow;
  wrap.__updateFlowClasses = updateFlowClasses;
  wrap.__renderFeaturePanel = renderFeaturePanel;

  // Sticky Save/Cancel footer
  try{ installStickyBridge && installStickyBridge(wrap); }catch(_){}

  return wrap;
}

/* ---------------------------- Sticky footer (FAB) --------------------------- */

function ensureModalFooterCSS(){
  if(document.getElementById('cx-modal-footer-css')) return;
  const css = `
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
  #cx-save-fab .btn.ghost{
    background:transparent; color:#fff; border:1px solid rgba(255,255,255,.18);
    box-shadow:none;
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
      requestAnimationFrame(()=>{ try{
        const fab = document.getElementById('save-fab');
        const frost = document.getElementById('save-frost');
        if(fab){ fab.classList.remove('hidden'); fab.style.display=''; void fab.offsetHeight; }
        if(frost){ frost.classList.remove('hidden'); frost.style.display=''; }
      }catch(_){ }});
      modalEl.__cxSuspendedFooter = null;
    }catch(_){}
  }
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

  function ensureLocalBar(){
    try{ ensureModalFooterCSS && ensureModalFooterCSS(); }catch(_){}
    let frost = document.getElementById('cx-save-frost');
    if(!frost){
      frost = document.createElement('div');
      frost.id = 'cx-save-frost';
      document.body.appendChild(frost);
    } else frost.classList.remove('hidden');

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
      btnSave.innerHTML = '<span class="btn-ic">✔</span> <span class="btn-label">Save</span>';
      btnSave.addEventListener('click', async (e)=>{
        e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation?.();
        try{ await modalEl.__doSave?.(); }catch(err){ console.warn('[cx] modal save failed:', err); }
      }, true);

      const group = document.createElement('div');
      group.style.cssText = 'display:flex;gap:16px;align-items:center;pointer-events:auto;';
      group.appendChild(btnCancel);
      group.appendChild(btnSave);
      fab.appendChild(group);
      document.body.appendChild(fab);

      modalEl.__stickyRefs = { container: fab, saveEl: btnSave, cancelEl: btnCancel };
    } else fab.classList.remove('hidden');

    return { frost, fab };
  }

  function teardown(){
    try{ document.getElementById('cx-save-frost')?.remove(); }catch(_){}
    try{ document.getElementById('cx-save-fab')?.remove(); }catch(_){}
    try{ document.getElementById('cx-modal-sticky')?.remove(); }catch(_){}
    try{ document.body.classList.remove('cx-modal-open'); }catch(_){}
    try{ restoreGlobalFooters(); }catch(_){}
    try{ resumeFastAPIFooter(); }catch(_){}
    try{ postCloseRepair(); }catch(_){}
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

  let raf = 0;
  function scheduleEnsure(){
    if(raf) return;
    raf = requestAnimationFrame(()=>{ raf=0;
      if(isOpen()){
        try{
          document.querySelectorAll('#save-fab,#save-frost,.savebar,.actions.sticky').forEach(n=>{
            if(n && n.style && n.style.display === 'none') n.style.display = '';
          });
        }catch(_){}
        try{ document.body.classList.add('cx-modal-open'); }catch(_){}
        try{ ensureModalFooterCSS && ensureModalFooterCSS(); }catch(_){}
        ensureLocalBar();
      }
    });
  }

  const mo = new MutationObserver(()=>{
    if(!isOpen()){ teardown(); return; }
    scheduleEnsure();
  });
  mo.observe(modalEl, { attributes:true, attributeFilter:['class'], childList:true, subtree:true });
  modalEl.__stickyMO = mo;

  if(isOpen()){
    try{ document.body.classList.add('cx-modal-open'); }catch(_){}
    suspendFastAPIFooter();
    try{ ensureModalFooterCSS && ensureModalFooterCSS(); }catch(_){}
    ensureLocalBar();
  }

  (function cxModalBodyWatchdog(){
    try {
      const tidy = ()=>{
        const m = document.getElementById('cx-modal');
        const open = !!(m && !m.classList.contains('hidden'));
        if (!open) { try{ document.body.classList.remove('cx-modal-open'); }catch(_){}
          try{ restoreGlobalFooters(); }catch(_){}
          try{ resumeFastAPIFooter(); }catch(_){}
        }
      };
      modalEl.__cxTidy = tidy;
      window.addEventListener('hashchange', tidy);
      window.addEventListener('popstate', tidy);
      window.addEventListener('pageshow', tidy);
      document.addEventListener('visibilitychange', tidy);
      const bw = new MutationObserver(tidy);
      bw.observe(document.body, { childList: true, subtree: false });

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


/* ------------------------------ Flow anim CSS ------------------------------- */

function ensureFlowAnimCSS(){
  if(document.getElementById('cx-flow-anim-css')) return;
  const css = `
  @keyframes cx-flow-one   { 0%{left:0;opacity:.2} 50%{opacity:1} 100%{left:calc(100% - 8px);opacity:.2} }
  @keyframes cx-flow-two-a { 0%{left:0;opacity:.2} 50%{opacity:1} 100%{left:calc(100% - 8px);opacity:.2} }
  @keyframes cx-flow-two-b { 0%{left:calc(100% - 8px);opacity:.2} 50%{opacity:1} 100%{left:0;opacity:.2} }

  .flow-rail.pretty{display:flex;align-items:center;gap:10px;--flowColor:currentColor}
  .flow-rail.pretty .token{min-width:40px;text-align:center;opacity:.9}
  .flow-rail.pretty .arrow{position:relative;display:block;flex:1;height:12px}
  .flow-rail.pretty .arrow::before{content:'';position:absolute;left:0;right:0;top:50%;height:2px;background:var(--flowColor);opacity:.25;transform:translateY(-50%);border-radius:2px}
  .flow-rail.pretty .dot.flow{position:absolute;top:50%;transform:translateY(-50%);width:8px;height:8px;border-radius:50%;background:var(--flowColor);opacity:.6}

  .flow-rail.pretty.anim-one  .dot.flow.a{animation:cx-flow-one 1.2s ease-in-out infinite}
  .flow-rail.pretty.anim-one  .dot.flow.b{animation:cx-flow-one 1.2s ease-in-out .6s infinite}

  .flow-rail.pretty.anim-two  .dot.flow.a{animation:cx-flow-two-a 1.2s ease-in-out infinite}
  .flow-rail.pretty.anim-two  .dot.flow.b{animation:cx-flow-two-b 1.2s ease-in-out infinite}`;
  const st = document.createElement('style');
  st.id = 'cx-flow-anim-css';
  st.textContent = css;
  document.head.appendChild(st);
}
try{ ensureFlowAnimCSS(); }catch(_){}

/* ---------------------- SIMKL caution styles (in-theme, high contrast) ------ */
// Small isolated CSS payload for the Ratings > Advanced notice.
// Safe to load unconditionally; block only renders when SIMKL is in-pair.
function ensureSimklCautionCSS(){
  if(document.getElementById('cx-simkl-caution-css')) return;
  const st = document.createElement('style');
  st.id = 'cx-simkl-caution-css';
  st.textContent = `
    /* SIMKL ratings caution block */
    #cx-modal .simkl-alert{
      margin-top:12px; padding:12px 14px; border-radius:14px;
      background:linear-gradient(180deg, rgba(255,170,54,.12), rgba(255,170,54,.05));
      border:1px solid rgba(255,170,54,.55);
      box-shadow:0 8px 28px rgba(255,170,54,.10), 0 0 0 1px rgba(255,170,54,.25) inset;
    }
    #cx-modal .simkl-alert .title{
      display:flex; align-items:center; gap:8px;
      font-weight:700; letter-spacing:.2px; margin:0 0 6px 0;
      color:#ffd79a;
    }
    #cx-modal .simkl-alert .title .ic{ font-size:14px; line-height:1; transform:translateY(-1px); }
    #cx-modal .simkl-alert .body{ color:#fff; opacity:.92; }
    #cx-modal .simkl-alert .body .bul{ margin:6px 0 0 18px; padding:0; }
    #cx-modal .simkl-alert .body .bul li{ margin:2px 0; }
    #cx-modal .simkl-alert .mini{ margin-top:8px; font-size:12.5px; opacity:.85; }
  `;
  document.head.appendChild(st);
}
try{ ensureSimklCautionCSS(); }catch(_){}

/* ---------------------- Modal-scoped scrollbars + tooltips ------------------ */

// Themed scrollbars (modal-scoped)
(function reinforceCxScrollbarTheme(){
  if (document.getElementById('cx-scrollbar-css-merged')) return;
  const css = `
    /* Firefox */
    #cx-modal .cx-body, #cx-modal .rules {
      scrollbar-width: thin;
      scrollbar-color: rgba(255,255,255,.32) rgba(255,255,255,.08);
    }
    /* WebKit/Blink */
    #cx-modal .cx-body::-webkit-scrollbar, #cx-modal .rules::-webkit-scrollbar { width:10px; height:10px; }
    #cx-modal .cx-body::-webkit-scrollbar-track, #cx-modal .rules::-webkit-scrollbar-track {
      background: rgba(255,255,255,.06); border-radius:10px;
    }
    #cx-modal .cx-body::-webkit-scrollbar-thumb, #cx-modal .rules::-webkit-scrollbar-thumb {
      background: linear-gradient(180deg, rgba(255,255,255,.38), rgba(255,255,255,.24));
      border-radius:10px; border:2px solid transparent; background-clip: padding-box;
    }
    #cx-modal .cx-body::-webkit-scrollbar-thumb:hover, #cx-modal .rules::-webkit-scrollbar-thumb:hover {
      background: linear-gradient(180deg, rgba(255,255,255,.55), rgba(255,255,255,.35));
    }
  `;
  const st = document.createElement('style');
  st.id = 'cx-scrollbar-css-merged';
  st.textContent = css;
  document.head.appendChild(st);
})();

// Delegated tooltips (no eager seeding; we seed after render)
(function cxTooltips(){
  if (!document.getElementById('cx-tooltips-css')) {
    const css = `
      .cx-tip{
        position:fixed; z-index:2147483600;
        max-width:380px; padding:10px 12px; border-radius:10px;
        background:rgba(20,20,26,.95); color:#fff; font-size:12.5px; line-height:1.35;
        border:1px solid rgba(255,255,255,.1); box-shadow:0 8px 30px rgba(0,0,0,.35);
        pointer-events:none; transform:translateY(-6px); opacity:0; transition:opacity .12s, transform .12s;
      }
      .cx-tip.show{ opacity:1; transform:translateY(0); }
      .cx-help{ opacity:.75; cursor:help; }
      .cx-help:hover{ opacity:.95; }
    `;
    const st = document.createElement('style');
    st.id = 'cx-tooltips-css';
    st.textContent = css;
    document.head.appendChild(st);
  }

  let tipEl=null, hideTO=0, lastHost=null;

  function ensureTip(){
    if (tipEl) return tipEl;
    tipEl = document.createElement('div');
    tipEl.className='cx-tip';
    document.body.appendChild(tipEl);
    return tipEl;
  }
  function showTip(text, x, y){
    const el = ensureTip();
    if (el.textContent !== text) el.textContent = text;
    const pad=10, vw=innerWidth, vh=innerHeight;
    el.style.left = Math.min(vw-pad-20, Math.max(pad, x+14))+'px';
    el.style.top  = Math.min(vh-pad-20, Math.max(pad, y+12))+'px';
    clearTimeout(hideTO);
    requestAnimationFrame(()=> el.classList.add('show'));
  }
  function hideTip(force=false){
    if (!tipEl) return;
    const el = tipEl;
    el.classList.remove('show');
    clearTimeout(hideTO);
    hideTO = setTimeout(()=>{ try{ el.remove(); }catch(_){} tipEl=null; lastHost=null; }, force ? 0 : 120);
  }

  // Global delegation so it works even if modal is rendered later
  document.addEventListener('mousemove', (e)=>{
    const host = e.target.closest('#cx-modal [data-help]');
    if (!host){ hideTip(); return; }
    if (host !== lastHost){
      lastHost = host;
      showTip(host.getAttribute('data-help')||'', e.clientX, e.clientY);
    }else{
      // keep position fresh
      if (tipEl) {
        const pad=10, vw=innerWidth, vh=innerHeight;
        tipEl.style.left = Math.min(vw-pad-20, Math.max(pad, e.clientX+14))+'px';
        tipEl.style.top  = Math.min(vh-pad-20, Math.max(pad, e.clientY+12))+'px';
      }
    }
  }, { passive:true });
  ['mouseleave','mousedown','click','wheel','scroll'].forEach(ev=>{
    document.addEventListener(ev, ()=>hideTip(ev==='mouseleave'), { passive:true });
  });

  // Helper to stamp help on entire .opt-row + control + label
  window.__cxSetHelp = function(id, text){
    const el = document.getElementById(id); if(!el) return;
    const row = el.closest('.opt-row') || el;
    [row, el].forEach(n=>{ if(n) n.setAttribute('data-help', text); });
    row.classList.add('cx-help');
    const label = row.querySelector('label'); if(label) label.classList.add('cx-help');
  };
})();

/* --------------------------------- Exports --------------------------------- */

window.Modals=Object.assign(window.Modals||{},{
  openAbout,closeAbout,openUpdateModal,closeUpdateModal,dismissUpdate,
  cxEnsureCfgModal,cxBindCfgEvents,cxCloseModal,openPairModal,closePairModal,
  cxEditPair: (id)=>{const pr=(window.cx?.pairs||[]).find(p=>p.id===id);if(!pr)return;cxOpenModalFor(pr,id);},
  cxOpenModalFor
});
Object.assign(window,{
  openAbout,closeAbout,openUpdateModal,closeUpdateModal,dismissUpdate,
  cxEnsureCfgModal,cxBindCfgEvents,cxCloseModal,openPairModal,closePairModal,
  cxEditPair: (id)=>{const pr=(window.cx?.pairs||[]).find(p=>p.id===id);if(!pr)return;cxOpenModalFor(pr,id);},
  cxOpenModalFor
});

/* ----------------------------- Open/close modal ----------------------------- */
// Keep these at end so helpers above are defined before used.

async function cxOpenModalFor(pair, editingId){
  await cxEnsureCfgModal();
  const el = document.getElementById('cx-modal');
  if(!el) return;

  const st = el.__state;
  st.src = pair?.source || pair?.src || null;
  st.dst = pair?.target || pair?.dst || null;

  document.getElementById('cx-mode-two')?.removeAttribute('checked');
  document.getElementById('cx-mode-one')?.removeAttribute('checked');
  if((pair?.mode||'').toLowerCase().startsWith('two')) document.getElementById('cx-mode-two').checked=true;
  else document.getElementById('cx-mode-one').checked=true;
  const enEl=document.getElementById('cx-enabled'); if(enEl) enEl.checked = (pair && typeof pair.enabled !== 'undefined') ? !!pair.enabled : true;

  const fx = pair?.features || {};
  st.options.watchlist = Object.assign({enable:true,add:true,remove:false}, fx.watchlist||{});
  st.options.ratings   = Object.assign({
    enable:false,add:false,remove:false,
    types:['movies','shows'], mode:'only_new', from_date:''
  }, fx.ratings||{});
  st.options.history   = Object.assign({enable:false,add:false,remove:false}, fx.history||{});
  st.options.playlists = Object.assign({enable:false,add:true,remove:false}, fx.playlists||{});
  st.visited = new Set(['watchlist','ratings','history','playlists']);

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

  el.dataset.editingId = editingId || (pair?.id || '');
  el.classList.remove('hidden');

  try{ el.__stickyBridgeInstalled = false; }catch(_){}
  try{ installStickyBridge && installStickyBridge(el); }catch(_){}
  try{ installModalDrag && installModalDrag(el); }catch(_){}
  try{ updateRtCompactSummary && updateRtCompactSummary(); }catch(_){}
}

function cxCloseModal(persist=false){
  const mod=document.getElementById('cx-modal');
  if(!mod) return;
  if(persist){try{mod.__persistCurrentFeature&&mod.__persistCurrentFeature();}catch(_){}} 
  mod.classList.add('hidden');

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
