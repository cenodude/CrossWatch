
// Fallback Pair modal helpers
function openPairModal() {
  var m = document.getElementById("pairModal");
  if (m) m.classList.remove("hidden");
}
function closePairModal() {
  var m = document.getElementById("pairModal");
  if (m) m.classList.add("hidden");
}

/* Crosswatch modals (extracted)
 * Exposes modal functions on window.Modals and window.
 */

async function openAbout() {
  try {
    

    const r = await fetch("/api/version?cb=" + Date.now(), {
      cache: "no-store",
    });

    const j = r.ok ? await r.json() : {};

    const cur = (j.current ?? "0.0.0").toString().trim();

    const latest = (j.latest ?? "").toString().trim() || null;

    const url =
      j.html_url ||
      "https://github.com/cenodude/plex-simkl-watchlist-sync/releases";

    const upd = !!j.update_available;

    

    const verEl = document.getElementById("about-version");

    if (verEl) {
      verEl.textContent = `Version ${j.current}`;

      verEl.dataset.version = cur; 
    }

    

    const headerVer = document.getElementById("app-version");

    if (headerVer) {
      headerVer.textContent = `Version ${cur}`;

      headerVer.dataset.version = cur;
    }

    

    const relEl = document.getElementById("about-latest");

    if (relEl) {
      relEl.href = url;

      relEl.textContent = latest ? `v${latest}` : "Releases";

      relEl.setAttribute(
        "aria-label",
        latest ? `Latest release v${latest}` : "Releases"
      );
    }

    

    const updEl = document.getElementById("about-update");

    if (updEl) {
      updEl.classList.add("badge", "upd");

      if (upd && latest) {
        updEl.textContent = `Update ${latest} available`;

        updEl.classList.remove("hidden", "reveal");

        void updEl.offsetWidth; 

        updEl.classList.add("reveal");
      } else {
        updEl.textContent = "";

        updEl.classList.add("hidden");

        updEl.classList.remove("reveal");
      }
    }
  } catch (_) {}

  document.getElementById("about-backdrop")?.classList.remove("hidden");
}

function closeAbout(ev) {
  if (ev && ev.type === "click" && ev.currentTarget !== ev.target) return; 

  document.getElementById("about-backdrop")?.classList.add("hidden");
}

/* #-------------PASCAL----BEGIN----- modal-template-cxEnsureCfgModal */
/* #-------------PASCAL----BEGIN----- modal-template-cxEnsureCfgModal */
async function cxEnsureCfgModal() {
  
  const ex = document.querySelector('#cx-modal');
  if (ex) return ex;

  
  const wrap = document.createElement('div');
  wrap.id = 'cx-modal';
  wrap.className = 'modal-backdrop cx-wide hidden';

  
  wrap.innerHTML = `
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
            <span class="lab on"  aria-hidden="true">Enabled</span>
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
              <div class="panel" id="cx-feat-panel"><!-- watchlist options injected --></div>
            </div>
            <div class="right">
              <div class="panel rules-card">
                <div class="panel-title">Rule preview</div>
                <div class="sub">These rules are generated from your selections.</div>
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

  
  if (typeof cxBindCfgEvents === 'function') {
    cxBindCfgEvents();
  }

  
  const close = () => wrap.classList.add('hidden');
  var _cbtn = wrap.querySelector('#cx-close'); if (_cbtn) _cbtn.onclick = close;
  wrap.querySelector('#cx-cancel').onclick = close;

  
  const state = {
    providers: [],
    src: null,
    dst: null,
    feature: 'watchlist',
    options: { watchlist: { enable: true, add: true, remove: false } }
  };

  async function loadProviders(){
    try{
      const res = await fetch('/api/sync/providers');
      const list = await res.json();
      state.providers = Array.isArray(list) ? list : [];
    }catch(e){
      console.warn('Failed to load /api/sync/providers', e);
      state.providers = [
        {name:'PLEX', label:'Plex', features:{watchlist:true,ratings:true,history:true,playlists:true}, capabilities:{bidirectional:true}},
        {name:'SIMKL', label:'Simkl', features:{watchlist:true,ratings:true,history:true,playlists:true}, capabilities:{bidirectional:true}},
        {name:'TRAKT', label:'Trakt', features:{watchlist:true,ratings:true,history:true,playlists:true}, capabilities:{bidirectional:true}},
      ];
    }
  }

  function byName(n){ return state.providers.find(p=>p.name===n); }
  function commonFeatures(){
    if(!state.src || !state.dst) return [];
    const a = byName(state.src), b = byName(state.dst);
    if(!a || !b) return [];
    const keys = ['watchlist','ratings','history','playlists'];
    return keys.filter(k => a.features?.[k] && b.features?.[k]);
  }

  function renderProviderSelects(){
    const srcSel = wrap.querySelector('#cx-src');
    const dstSel = wrap.querySelector('#cx-dst');
    const opts = state.providers.map(p=>`<option value="${p.name}">${p.label}</option>`).join('');
    srcSel.innerHTML = `<option value="">Select…</option>${opts}`;
    dstSel.innerHTML = `<option value="">Select…</option>${opts}`;
    if(state.src) srcSel.value = state.src;
    if(state.dst) dstSel.value = state.dst;

    srcSel.onchange = () => { state.src = srcSel.value || null; updateFlow(); refreshTabs(); };
    dstSel.onchange = () => { state.dst = dstSel.value || null; updateFlow(); refreshTabs(); };
  }

  function updateFlow(){
    wrap.querySelector('#cx-flow-src').textContent = byName(state.src)?.label || '—';
    wrap.querySelector('#cx-flow-dst').textContent = byName(state.dst)?.label || '—';
    const two = wrap.querySelector('#cx-mode-two');
    const ok = (byName(state.src)?.capabilities?.bidirectional) && (byName(state.dst)?.capabilities?.bidirectional);
    
    two.disabled = !ok;
    if(!ok && two.checked){ wrap.querySelector('#cx-mode-one').checked = true; }
    if (two.nextElementSibling) two.nextElementSibling.classList.toggle('disabled', !ok);
    
    const title = (wrap.querySelector('#cx-mode-two').checked ? 'Two-way (bidirectional)' : 'One-way');
    const tEl = wrap.querySelector('#cx-flow-title');
    if (tEl) tEl.textContent = title;
    
    updateFlowClasses();
}

  function refreshTabs(){
    const tabs = wrap.querySelector('#cx-feat-tabs');
    tabs.innerHTML = '';
    const items = commonFeatures();
    if(items.length===0){
      tabs.innerHTML = '<div class="ftab disabled">No common features</div>';
      renderFeaturePanel(); 
      return;
    }
    items.forEach(k=>{
      const b = document.createElement('button');
      b.className = 'ftab' + (state.feature===k?' active':'');
      b.dataset.key = k;
      b.textContent = k[0].toUpperCase()+k.slice(1);
      b.onclick = () => { state.feature = k; refreshTabs(); renderFeaturePanel(); };
      tabs.appendChild(b);
    });
    if(!items.includes(state.feature)){ state.feature = items[0]; }
    
    [...tabs.children].forEach(c=>c.classList.toggle('active', c.dataset.key===state.feature));
    renderFeaturePanel();
  }

  function renderFeaturePanel(){
    const pane = wrap.querySelector('#cx-feat-panel');
    if(state.feature!=='watchlist'){
      pane.innerHTML = `
        <div class="panel-title">${state.feature[0].toUpperCase()+state.feature.slice(1)} options</div>
        <div class="muted">Configuration for this feature will be available soon.</div>`;
      wrap.querySelector('#cx-notes').textContent = `Selected: ${state.feature}.`;
      return;
    }
    
    let wl = state.options.watchlist;
    try {
      const mid = (wrap && wrap.dataset ? wrap.dataset.editingId : '') || '';
      if (mid && Array.isArray(window.cx?.pairs)) {
        const p = window.cx.pairs.find(x => String(x.id||'') === String(mid));
        if (p && p.features && p.features.watchlist) {
          wl = Object.assign({ enable:true, add:true, remove:false }, p.features.watchlist);
          state.options.watchlist = wl;
        }
      }
    } catch(_) {}
    const wlEnableEl = document.getElementById('cx-wl-enable');

    pane.innerHTML = `
      <div class="panel-title">Watchlist options</div>
      <div class="opt-row">
        <label for="cx-wl-enable">Enable</label>
        <label class="switch">
          <input id="cx-wl-enable" type="checkbox" ${wl.enable?'checked':''}>
          <span class="slider"></span>
        </label>
      </div>
      <div class="opt-row">
        <label for="cx-wl-add">Add</label>
        <label class="switch">
          <input id="cx-wl-add" type="checkbox" ${wl.add?'checked':''}>
          <span class="slider"></span>
        </label>
      </div>
      <div class="opt-row">
        <label for="cx-wl-remove">Remove</label>
        <label class="switch">
          <input id="cx-wl-remove" type="checkbox" ${wl.remove?'checked':''}>
          <span class="slider"></span>
        </label>
      </div>
      <div class="panel sub">
        <div class="panel-title small">Advanced</div>
        <ul class="adv">
          <li>Filter by type (Movies/Shows)</li>
          <li>Skip duplicates</li>
          <li>Prefer target metadata</li>
          <li>Conflict policy: target wins</li>
        </ul>
      </div>`;
    
    updateRules();
  }

  
function updateFlowClasses(){
  const rail = wrap.querySelector('#cx-flow-rail');
  if (!rail) return;
  const two = wrap.querySelector('#cx-mode-two')?.checked;
  const on  = wrap.querySelector('#cx-wl-enable')?.checked ?? true;
  const add = wrap.querySelector('#cx-wl-add')?.checked ?? true;
  const rem = wrap.querySelector('#cx-wl-remove')?.checked ?? false;
  rail.className = 'flow-rail pretty';
  rail.classList.toggle('off', !on);
  rail.classList.toggle('mode-two', !!two);
  rail.classList.toggle('mode-one', !two);
  if (two) {
    rail.classList.toggle('active', on && (add || rem));
  } else {
    rail.classList.toggle('dir-add', on && add);
    rail.classList.toggle('dir-remove', on && !add && rem);
  }
}
function updateRules(){
    const rules = wrap.querySelector('#cx-rules');
    rules.innerHTML = '';
    const push = (title, sub='')=>{
      const d = document.createElement('div');
      d.className = 'r';
      d.innerHTML = `<div class="t">${title}</div>${sub?`<div class="s">${sub}</div>`:''}`;
      rules.appendChild(d);
    };
    if(state.feature==='watchlist'){
      const two = wrap.querySelector('#cx-mode-two').checked;
      const wl = {
        enable: wrap.querySelector('#cx-wl-enable')?.checked ?? false,
        add:    wrap.querySelector('#cx-wl-add')?.checked ?? false,
        remove: wrap.querySelector('#cx-wl-remove')?.checked ?? false,
      };
      wrap.querySelector('#cx-notes')?.remove(); 
      push(`Watchlist • ${two?'two‑way':'one‑way'}`, wl.enable?'enabled':'disabled');
      if(wl.add) push('Add • new items', two?'both directions':'from source → target');
      if (wl.remove) push('Delete • when removed', two ? 'both directions' : 'from source → target');
    }
  }

  updateFlowClasses();

  
  wrap.addEventListener('change', (e)=>{
    if(['cx-mode-one','cx-mode-two','cx-wl-enable','cx-wl-add','cx-wl-remove'].includes(e.target.id)){
      updateRules();
      updateFlow();
    }
  });

  
  try {
    loadProviders().then(() => {
      renderProviderSelects();
      refreshTabs();
      updateFlow();

      
      updateFlowRailLogos();
      document.getElementById("cx-src")?.addEventListener("change", updateFlowRailLogos);
      document.getElementById("cx-dst")?.addEventListener("change", updateFlowRailLogos);
    });
  } catch (_) {}

  return wrap;
}
/* #-------------PASCAL----END----- modal-template-cxEnsureCfgModal */
/* #-------------PASCAL----END----- modal-template-cxEnsureCfgModal */

function cxBindCfgEvents(
/* #-------------PASCAL----END----- connector-modal */
) {
  const ids = [
    "cx-src",
    "cx-dst",
    "cx-mode-one",
    "cx-mode-two",
    "cx-wl-enable",
    "cx-wl-add",
    "cx-wl-remove",
    "cx-enabled",
  ];

  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;

    el.addEventListener("change", () => {
      
      const twoEl   = document.getElementById("cx-mode-two");
      const wlEl    = document.getElementById("cx-wl-enable");
      const remEl   = document.getElementById("cx-wl-remove");

      const two     = !!(twoEl && twoEl.checked);
      const wlOn    = !!(wlEl && wlEl.checked);

      if (remEl) {
        remEl.disabled = !wlOn;
        if (remEl.disabled) remEl.checked = false;
      }

      try { cxUpdateSummary && cxUpdateSummary(); } catch (_) {}
    });
  });

  const save = document.getElementById("cx-save");
  if (save) {
    save.addEventListener("click", () => {
      const srcEl = document.getElementById("cx-src");
      const dstEl = document.getElementById("cx-dst");
      const twoEl = document.getElementById("cx-mode-two");
      const enEl  = document.getElementById("cx-enabled");
      const wlE   = document.getElementById("cx-wl-enable");
      const wlA   = document.getElementById("cx-wl-add");
      const wlR   = document.getElementById("cx-wl-remove");

      const data = {
        source:  srcEl ? srcEl.value : "",
        target:  dstEl ? dstEl.value : "",
        enabled: enEl ? !!enEl.checked : true,
        mode:    twoEl && twoEl.checked ? "two-way" : "one-way",
        features: {
          watchlist: {
            enable: wlE ? !!wlE.checked : true,
            add:    wlA ? !!wlA.checked : true,
            remove: wlR ? !!wlR.checked : false, 
          },
        },
      };

      if (typeof window.cxSavePair === "function") {
        window.cxSavePair(data);
      } else {
        console.log("cxSavePair payload", data);
      }
      cxCloseModal && cxCloseModal();
    });
  }
}

function openUpdateModal() {
  if (!_updInfo) return;
  document.getElementById("upd-modal").classList.remove("hidden");
  document.getElementById("upd-title").textContent = `v${_updInfo.latest}`;
  document.getElementById("upd-notes").textContent =
    _updInfo.notes || "(No release notes)";
  document.getElementById("upd-link").href = _updInfo.url || "#";
}

function closeUpdateModal() {
  document.getElementById("upd-modal").classList.add("hidden");
}

function dismissUpdate() {
  if (_updInfo?.latest) {
    localStorage.setItem("dismissed_version", _updInfo.latest);
  }
  document.getElementById("upd-pill").classList.add("hidden");
  closeUpdateModal();
}

function cxEditPair(id) {
    const pr = (window.cx.pairs || []).find((p) => p.id === id);
    if (!pr) return;
    cxOpenModalFor(pr, id);
  }

function cxCloseModal(
/* #-------------PASCAL----END----- connector-events */
) {
    const modal = document.getElementById("cx-modal");
    if (modal) modal.classList.add("hidden");
    window.cx.connect = { source: null, target: null };
  }

/* #-------------PASCAL----BEGIN----- modal-template-_ensureCfgModal */
/* #-------------PASCAL----BEGIN----- modal-template-_ensureCfgModal */
/* #-------------PASCAL----BEGIN----- modal-template-_ensureCfgModal */
function _ensureCfgModal() {
    if (document.getElementById("cx-modal")) return true;
    if (typeof cxEnsureCfgModal === "function") {
      cxEnsureCfgModal();
      return !!document.getElementById("cx-modal");
    }
    
    var wrap = document.createElement("div");
    wrap.id = "cx-modal";
    wrap.className = "modal-backdrop hidden";
    wrap.innerHTML =
      '<div class="modal-card"><div class="modal-header"><div class="title">Configure</div><button class="btn-ghost" onclick="cxCloseModal()">✕</button></div>' +
      '<div class="modal-body"><div class="form-grid">' +
      '<div class="field"><label for="cx-src">Source</label><select id="cx-src"><option>PLEX</option><option>SIMKL</option></select></div>' +
      '<div class="field"><label for="cx-dst">Target</label><select id="cx-dst"><option>PLEX</option><option>SIMKL</option></select></div>' +
      '</div><div class="form-grid" style="margin-top:8px">' +
      '<div class="field"><label>Mode</label><div class="seg">' +
      '<input id="cx-mode-one" type="radio" name="cx-mode" value="one-way" checked><label for="cx-mode-one">One-way</label>' +
      '<input id="cx-mode-two" type="radio" name="cx-mode" value="two-way"><label id="cx-two-label" for="cx-mode-two">Two-way</label>' +
      "</div></div>" +
      '<div class="field"><label>Enabled</label><div class="row"><input id="cx-enabled" type="checkbox" checked></div></div>' +
      "</div>" +
      '<div class="features"><div class="fe-row">' +
      '<div class="fe-name">Watchlist</div>' +
      '<label class="row"><input id="cx-wl-add" type="checkbox" checked><span>Add</span></label>' +
      '<label class="row"><input id="cx-wl-remove" type="checkbox"><span>Remove</span></label>' +
      '<div id="cx-wl-note" class="micro-note"></div>' +
      "</div></div></div>" +
      '<div class="modal-footer"><button class="btn acc" id="cx-save">Save</button><button class="btn" onclick="cxCloseModal()">Cancel</button></div></div>';
    document.body.appendChild(wrap);
    return true;
  }

window.Modals = Object.assign(window.Modals || {}, {
  openAbout,
  closeAbout,
  openUpdateModal,
  closeUpdateModal,
  dismissUpdate,
  cxEnsureCfgModal,
  cxBindCfgEvents,
  cxCloseModal,
  openPairModal,
  closePairModal,
  cxEditPair
});

Object.assign(window, {
  openAbout,
  closeAbout,
  openUpdateModal,
  closeUpdateModal,
  dismissUpdate,
  cxEnsureCfgModal,
  cxBindCfgEvents,
  cxCloseModal,
  openPairModal,
  closePairModal,
  cxEditPair
});
