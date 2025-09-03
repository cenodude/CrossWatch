// connections.overlay.js
// Drop-in overlay to enhance provider cards and pair saving without touching the original crosswatch.js.
// - Dynamic labels: Set as Source / Set as Target / Cancel
// - Immediate re-render after selection
// - Drag & drop (source → target)
// - Safe save fallback when loadPairs() is not defined

(function(){

  async function _overlayRefreshPairs(){
    try{
      const arr = await fetch('/api/pairs',{cache:'no-store'}).then(r=>r.json());
      window.cx = window.cx || {};
      window.cx.pairs = Array.isArray(arr) ? arr : [];
      try { if (typeof renderConnections === 'function') renderConnections(); } catch(_){}
    }catch(e){ console.warn('[overlay] refresh pairs failed', e); }
  }

  function ensureStyles(){
    if (document.getElementById('cx-overlay-style')) return;
    const css = `
      .prov-card.selected{outline:1px solid rgba(124,92,255,.6); box-shadow:0 0 22px rgba(124,92,255,.25)}
      .cx-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px;margin-top:6px}
    `;
    const s = document.createElement('style');
    s.id = 'cx-overlay-style';
    s.textContent = css;
    document.head.appendChild(s);
    // extra overrides
    var extra=document.createElement('style'); extra.textContent = `
  /* --- Strong UI overrides --- */
  #providers_list.grid2{ display:block !important; }              /* break the 2-col grid wrapper */
  #providers_list .pairs-board{ display:flex; flex-direction:column; align-items:flex-start; text-align:left; margin-top:10px;}
  #providers_list .cx-grid{ display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:16px; }
  /* Kill legacy Add pair/Batches UI hard via selectors */
  .pair-selectors, #pairs_list,
  button[onclick="addPair()"],
  #batches_list, button[onclick="addBatch()"], button[onclick="runAllBatches()"]{ display:none !important; }
`; document.head.appendChild(extra);
  }

  function cap(obj, key){ try { return !!(obj && obj.features && obj.features[key]); } catch(_){ return false; } }

  function rebuildProviders(){
    try {
      ensureStyles();
      const host = document.getElementById('providers_list');
      if (!host) return;

      const provs = (window.cx && window.cx.providers) || [];
      if (!provs.length) return; // nothing to build yet

      const selSrc = window.cx && window.cx.connect && window.cx.connect.source || null;

      const html = provs.map(p => {
        const name = p.label || p.name;
        const isSrc = !!(selSrc && String(selSrc).toUpperCase() === String(p.name).toUpperCase());
        const btnLabel  = !selSrc ? 'Set as Source' : (isSrc ? 'Cancel' : 'Set as Target');
        const btnOnclick = !selSrc
            ? `cxToggleConnect('${p.name}')`
            : (isSrc ? `cxToggleConnect('${p.name}')` : `cxPickTarget('${p.name}')`);

        const wl  = cap(p,'watchlist');
        const rat = cap(p,'ratings');
        const hist= cap(p,'history');
        const pl  = cap(p,'playlists');

        return `<div class="prov-card${isSrc?' selected':''}" data-prov="${p.name}" draggable="true">
          <div class="prov-title">${name}</div>
          <div class="prov-caps">
            <span class="dot ${wl?'on':'off'}" title="Watchlist"></span>
            <span class="dot ${rat?'on':'off'}" title="Ratings"></span>
            <span class="dot ${hist?'on':'off'}" title="History"></span>
            <span class="dot ${pl?'on':'off'}" title="Playlists"></span>
          </div>
          <button class="btn neon" onclick="${btnOnclick}">${btnLabel}</button>
        </div>`;
      }).join('');

      // Replace only the cards grid (leave pairs/board that the original render added)
      if (!host.querySelector('.cx-grid')) {
        host.innerHTML = `<div class="cx-grid">${html}</div>`;
      } else {
        const grid = host.querySelector('.cx-grid');
        grid.innerHTML = html;
      }
    } catch (e) {
      console.warn('[overlay] rebuildProviders failed', e);
    }
  }

  // --- Wrap original functions ---
  const _origRender = window.renderConnections;
  window.renderConnections = function(){
    try { if (typeof _origRender === 'function') _origRender(); } catch(e){}
    rebuildProviders();
  };

  const _origStart = window.cxStartConnect;
  window.cxStartConnect = function(name){
    try { if (typeof _origStart === 'function') _origStart(name); else { window.cx = window.cx||{}; window.cx.connect = { source:String(name), target:null }; } } catch(_){}
    try { window.renderConnections(); } catch(_){}
  };

  window.cxPickTarget = window.cxPickTarget || function(name){
    if(!window.cx || !window.cx.connect || !window.cx.connect.source) return;
    window.cx.connect.target = String(name);
    try{ if (typeof cxOpenModalFor === 'function') cxOpenModalFor({ source: window.cx.connect.source, target: window.cx.connect.target, mode:'one-way', enabled:true, features:{watchlist:{add:true,remove:false}} }); }catch(_){}
  };

  window.cxToggleConnect = function(name){
    name = String(name||'');
    if(!window.cx || !window.cx.connect) window.cx = { providers: [], pairs: [], connect: { source:null, target:null } };
    const sel = window.cx.connect;
    if(!sel.source){
      window.cxStartConnect(name);
      return;
    }
    if(sel.source && sel.source !== name){
      window.cxPickTarget(name);
      return;
    }
    // same again -> cancel
    window.cx.connect = { source:null, target:null };
    try { window.renderConnections(); } catch(_){}
  };

  // drag & drop
  document.addEventListener('dragstart', (e)=>{
    const card = e.target.closest && e.target.closest('.prov-card');
    if(!card) return;
    const name = card.getAttribute('data-prov');
    if (!name) return;
    window.cx = window.cx || {}; window.cx.connect = { source:name, target:null };
    try { e.dataTransfer.setData('text/plain', name); } catch(_){}
    try { window.renderConnections(); } catch(_){}
  });
  document.addEventListener('dragover', (e)=>{
    const card = e.target.closest && e.target.closest('.prov-card');
    if(card) e.preventDefault();
  });
  document.addEventListener('drop', (e)=>{
    const card = e.target.closest && e.target.closest('.prov-card');
    if(!card) return;
    e.preventDefault();
    const target = card.getAttribute('data-prov');
    const src = (window.cx.connect && window.cx.connect.source) || (e.dataTransfer && e.dataTransfer.getData('text/plain'));
    if(src && target && src !== target){
      window.cxPickTarget(target);
    }
  });

  // Patch cxOpenModalFor to provide a save fallback
  const _origOpen = window.cxOpenModalFor;
  window.cxOpenModalFor = function(pair, editingId){
    if (typeof _origOpen === 'function') _origOpen(pair, editingId);
    setTimeout(()=>{
      try {
        const saveBtn = document.getElementById('cx-save');
        if (!saveBtn) return;
        saveBtn.onclick = async ()=>{
          const modeEl = document.querySelector('input[name="cx-mode"]:checked');
          const mode    = (modeEl && modeEl.value) || 'one-way';
          const enabled = !!document.getElementById('cx-enabled')?.checked;
          const wlAdd   = !!document.getElementById('cx-wl-add')?.checked;
          const payload = {
            source: pair.source, target: pair.target, mode, enabled,
            features: { watchlist: { add: !!wlAdd, remove: false } }
          };
          try{
            if (editingId){
              await fetch(`/api/pairs/${editingId}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
            } else {
              const dupe = (window.cx.pairs||[]).some(x => String(x.source).toUpperCase()===payload.source.toUpperCase() && String(x.target).toUpperCase()===payload.target.toUpperCase());
              if(dupe){ alert('This connection already exists.'); return; }
              const r = await fetch('/api/pairs', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
              if(!r.ok) throw new Error('HTTP '+r.status);
            }
            try { if (typeof cxCloseModal === 'function') cxCloseModal(); } catch(_){}
            if (typeof loadPairs === 'function') {
              if (typeof loadPairs==='function') { if (typeof loadPairs==='function') { await loadPairs(); } else { await _overlayRefreshPairs(); } } else { await _overlayRefreshPairs(); }
            } else {
              const arr = await fetch('/api/pairs',{cache:'no-store'}).then(r=>r.json());
              window.cx = window.cx || {};
              window.cx.pairs = Array.isArray(arr) ? arr : [];
              try { if (typeof renderConnections === 'function') renderConnections(); } catch(_){}
            }
          }catch(e){
            console.warn('[overlay] Save connection failed', e);
            alert('Failed to save connection.');
          }
        };
      } catch(e){ console.warn('[overlay] modal patch failed', e); }
    }, 0);
  };

  // Kick a re-render when providers are loaded
  document.addEventListener('DOMContentLoaded', function(){
    try { window.renderConnections && window.renderConnections(); } catch(_){}
  });
})();


  // Force left alignment within our board
  (function ensureLeftAlign(){
    try{
      const s = document.createElement('style');
      s.id = 'cx-left-align';
      s.textContent = `.pairs-board{display:flex;flex-direction:column;align-items:flex-start;text-align:left}`;
      if(!document.getElementById('cx-left-align')) document.head.appendChild(s);
    // extra overrides
    var extra=document.createElement('style'); extra.textContent = `
  /* --- Strong UI overrides --- */
  #providers_list.grid2{ display:block !important; }              /* break the 2-col grid wrapper */
  #providers_list .pairs-board{ display:flex; flex-direction:column; align-items:flex-start; text-align:left; margin-top:10px;}
  #providers_list .cx-grid{ display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:16px; }
  /* Kill legacy Add pair/Batches UI hard via selectors */
  .pair-selectors, #pairs_list,
  button[onclick="addPair()"],
  #batches_list, button[onclick="addBatch()"], button[onclick="runAllBatches()"]{ display:none !important; }
`; document.head.appendChild(extra);
    }catch(_){}
  })();


  // Hide static 'Pairs' and 'Batches' headers + helper text
  function _hideLegacyStaticSections(){
    try{
      const isSub = el => el && el.classList && el.classList.contains('sub');
      document.querySelectorAll('#sec-sync .sub').forEach(el => {
        const txt = (el.textContent||'').trim().toLowerCase();
        if (txt === 'pairs' || txt === 'batches'){
          el.style.display = 'none';
          const next = el.nextElementSibling;
          if (next && next.classList.contains('muted')) next.style.display = 'none';
        }
      });
    }catch(_){}
  }
  document.addEventListener('DOMContentLoaded', _hideLegacyStaticSections);

