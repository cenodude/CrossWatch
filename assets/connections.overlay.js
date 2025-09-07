
// connections.overlay.js (robust open modal on target click)
(function(){
  function _brandInfo(name){
    var key = String(name||'').trim().toUpperCase();
    if (key === 'PLEX')  return { cls:'brand-plex',  icon:'/assets/PLEX.svg'  };
    if (key === 'SIMKL') return { cls:'brand-simkl', icon:'/assets/SIMKL.svg' };
    if (key === 'TRAKT') return { cls:'brand-trakt', icon:'/assets/TRAKT.svg' };
    return { cls:'', icon:'' };
  }

  function ensureStyles(){
    if (document.getElementById('cx-overlay-style')) return;
    const css = `
      :root{ --plex:#e5a00d; --simkl:#00b7eb; --trakt:#ed1c24; }
      .cx-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;margin-top:6px}
      .prov-card{position:relative;border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:14px;background:#0d0f14;box-shadow:0 4px 18px rgba(0,0,0,.35)}
      .prov-card.selected{outline:2px solid rgba(124,92,255,.6); box-shadow:0 0 22px rgba(124,92,255,.25)}
      .prov-title{font-weight:800;margin-bottom:8px}
      .prov-brand{position:absolute;top:8px;right:10px;width:36px;height:36px;display:block;z-index:3;
        padding:6px;border-radius:10px;background:rgba(0,0,0,.55);filter:drop-shadow(0 2px 6px rgba(0,0,0,.55))}
      .prov-card.brand-plex{border-color:rgba(229,160,13,.55);box-shadow:0 0 0 1px rgba(229,160,13,.35) inset, 0 0 24px rgba(229,160,13,.22)}
      .prov-card.brand-simkl{border-color:rgba(0,183,235,.55); box-shadow:0 0 0 1px rgba(0,183,235,.35) inset, 0 0 24px rgba(0,183,235,.22)}
      .prov-card.brand-trakt{border-color:rgba(237,28,36,.55); box-shadow:0 0 0 1px rgba(237,28,36,.35) inset, 0 0 24px rgba(237,28,36,.22)}
      .prov-caps{display:flex;gap:6px;margin:8px 0}
      .prov-caps .dot{width:8px;height:8px;border-radius:50%;background:#444}
      .prov-caps .dot.on{background:#5ad27a}
      .prov-caps .dot.off{background:#555}
      .btn.neon{display:inline-block;padding:8px 14px;border-radius:12px;border:1px solid rgba(255,255,255,.18);background:#121224;color:#fff;font-weight:700;cursor:pointer}
    `;
    const s=document.createElement('style'); s.id='cx-overlay-style'; s.textContent=css; document.head.appendChild(s);
  }

  function cap(obj, key){ try { return !!(obj && obj.features && obj.features[key]); } catch(_){ return false; } }

  function rebuildProviders(){
    ensureStyles();
    const host = document.getElementById('providers_list');
    if(!host) return;
    const provs = (window.cx && window.cx.providers) || [];
    if(!provs.length) return;

    const selSrc = window.cx && window.cx.connect && window.cx.connect.source || null;

    const html = provs.map(p => {
      const name   = p.label || p.name;
      const brand  = _brandInfo(p.name);
      const isSrc  = !!(selSrc && String(selSrc).toUpperCase() === String(p.name).toUpperCase());
      const btnLab = !selSrc ? 'Set as Source' : (isSrc ? 'Cancel' : 'Set as Target');
      const btnOn  = !selSrc
          ? `cxToggleConnect('${p.name}')`
          : (isSrc ? `cxToggleConnect('${p.name}')` : `cxPickTarget('${p.name}')`);

      const wl  = cap(p,'watchlist'), rat = cap(p,'ratings'), hist=cap(p,'history'), pl=cap(p,'playlists');
      const caps = `<div class="prov-caps">
        <span class="dot ${wl?'on':'off'}" title="Watchlist"></span>
        <span class="dot ${rat?'on':'off'}" title="Ratings"></span>
        <span class="dot ${hist?'on':'off'}" title="History"></span>
        <span class="dot ${pl?'on':'off'}" title="Playlists"></span>
      </div>`;

      const iconHtml = brand.icon ? `<img class="prov-brand" src="${brand.icon}" alt="${name}" onerror="this.remove()">` : '';

      return `<div class="prov-card${isSrc?' selected':''} ${brand.cls}" data-prov="${p.name}" draggable="true">
        <div class="prov-head">
          <div class="prov-title">${name}</div>
          ${iconHtml}
        </div>
        ${caps}
        <button type="button" class="btn neon prov-action" onclick="${btnOn}">${btnLab}</button>
      </div>`;
    }).join('');

    const wrap = host.querySelector('.cx-grid') || (()=>{const d=document.createElement('div'); d.className='cx-grid'; host.innerHTML=''; host.appendChild(d); return d;})();
    wrap.innerHTML = html;
  }
// Rebuild provider cards when connection state changes
document.addEventListener('cx-state-change', function(){
  try {
    const host = document.getElementById('providers_list');
    if (host) rebuildProviders(host);
  } catch(_) {}
});


  // --- glue ---
  const _origRender = window.renderConnections;
  window.renderConnections = function(){
    try{ if (typeof _origRender === 'function') _origRender(); }catch(_){}
    rebuildProviders();
  };

  const _origStart = window.cxStartConnect;
  window.cxStartConnect = function(name){
    try{ if (typeof _origStart === 'function') _origStart(name); }catch(_){}
    window.cx = window.cx || {};
    window.cx.connect = { source:String(name), target:null };
    try{ window.renderConnections(); }catch(_){}
  };

  window.cxPickTarget = window.cxPickTarget || function(name){
    if(!window.cx || !window.cx.connect || !window.cx.connect.source) return;
    window.cx.connect.target = String(name);
    const detail = { source: window.cx.connect.source, target: window.cx.connect.target };
    // robust open: prefer direct function, otherwise fire an event
    if (typeof window.cxOpenModalFor === 'function'){
      try{ window.cxOpenModalFor(detail); }catch(e){ console.warn('cxOpenModalFor failed', e); }
    } else {
      window.dispatchEvent(new CustomEvent('cx:open-modal', { detail }));
    }
  };

  window.cxToggleConnect = function(name){
    name = String(name||'');
    window.cx = window.cx || { providers:[], pairs:[], connect:{source:null,target:null} };
    const sel = window.cx.connect || (window.cx.connect={source:null,target:null});
    if(!sel.source){ window.cxStartConnect(name); return; }
    if(sel.source && sel.source !== name){ window.cxPickTarget(name); return; }
    window.cx.connect = { source:null, target:null };
    try{ window.renderConnections(); }catch(_){}
  };

  // DnD open support
  document.addEventListener('dragstart', (e)=>{
    const card = e.target.closest && e.target.closest('.prov-card');
    if(!card) return;
    const name = card.getAttribute('data-prov'); if(!name) return;
    window.cx = window.cx || {}; window.cx.connect = { source:name, target:null };
    try{ e.dataTransfer.setData('text/plain', name); }catch(_){}
    try{ window.renderConnections(); }catch(_){}
  });
  document.addEventListener('dragover', (e)=>{
    const card = e.target.closest && e.target.closest('.prov-card');
    if(card) e.preventDefault();
  });
  document.addEventListener('drop', (e)=>{
    const card = e.target.closest && e.target.closest('.prov-card');
    if(!card) return; e.preventDefault();
    const target = card.getAttribute('data-prov');
    const src = (window.cx.connect && window.cx.connect.source) || (e.dataTransfer && e.dataTransfer.getData('text/plain'));
    if(src && target && src !== target){ window.cxPickTarget(target); }
  });

  // Event bridge for older builds of crosswatch.js
  window.addEventListener('cx:open-modal', (ev)=>{
    if (typeof window.cxOpenModalFor === 'function') return;
    // last resort: try legacy open function name
    if (typeof window.openCxModalFor === 'function'){
      try{ window.openCxModalFor(ev.detail); }catch(_){}
    }
  });

  document.addEventListener('DOMContentLoaded', ()=>{
    try{ window.renderConnections && window.renderConnections(); }catch(_){}
  });
})();
