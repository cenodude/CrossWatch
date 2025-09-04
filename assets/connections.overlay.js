// connections.overlay.js (fixed: brand class + icon support)
(function(){

  function _brandInfo(name){
    var key = String(name||'').trim().toUpperCase();
    if (key === 'PLEX')  return { cls:'brand-plex',  icon:'/assets/PLEX.svg'  };
    if (key === 'SIMKL') return { cls:'brand-simkl', icon:'/assets/SIMKL.svg' };
    return { cls:'', icon:'' };
  }

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
      .cx-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:16px;margin-top:6px}

      /* brand visuals (keihard, ook zonder global css) */
      :root{ --plex:#e5a00d; --simkl:#00b7eb; }
      .prov-head{position:relative}
      .prov-brand{
        position:absolute;top:8px;right:10px;width:28px;height:28px;display:block;z-index:3;
        padding:4px;border-radius:8px;background:rgba(0,0,0,.55);
        filter:drop-shadow(0 2px 6px rgba(0,0,0,.55))
      }
      .prov-card.brand-plex{
        border:1px solid var(--plex) !important;
        box-shadow:0 0 0 1px var(--plex) inset, 0 0 24px rgba(229,160,13,.30) !important;
      }
      .prov-card.brand-simkl{
        border:1px solid var(--simkl) !important;
        box-shadow:0 0 0 1px var(--simkl) inset, 0 0 24px rgba(0,183,235,.30) !important;
      }
    `;
    const s = document.createElement('style');
    s.id = 'cx-overlay-style';
    s.textContent = css;
    document.head.appendChild(s);

    // extra overrides
    var extra=document.createElement('style'); extra.textContent = `
      #providers_list.grid2{ display:block !important; }
      #providers_list .pairs-board{ display:flex; flex-direction:column; align-items:flex-start; text-align:left; margin-top:10px;}
      #providers_list .cx-grid{ display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:16px; }
      .pair-selectors, #pairs_list,
      button[onclick="addPair()"],
      #batches_list, button[onclick="addBatch()"], button[onclick="runAllBatches()"]{ display:none !important; }
    `;
    document.head.appendChild(extra);
  }

  function cap(obj, key){ try { return !!(obj && obj.features && obj.features[key]); } catch(_){ return false; } }

  function rebuildProviders(){
    try {
      ensureStyles();
      const host = document.getElementById('providers_list');
      if (!host) return;

      const provs = (window.cx && window.cx.providers) || [];
      if (!provs.length) return;

      const selSrc = window.cx && window.cx.connect && window.cx.connect.source || null;

      const html = provs.map(p => {
        const name   = p.label || p.name;
        const brand  = _brandInfo(name);
        const isSrc  = !!(selSrc && String(selSrc).toUpperCase() === String(p.name).toUpperCase());
        const btnLab = !selSrc ? 'Set as Source' : (isSrc ? 'Cancel' : 'Set as Target');
        const btnOn  = !selSrc
            ? `cxToggleConnect('${p.name}')`
            : (isSrc ? `cxToggleConnect('${p.name}')` : `cxPickTarget('${p.name}')`);

        const wl  = cap(p,'watchlist');
        const rat = cap(p,'ratings');
        const hist= cap(p,'history');
        const pl  = cap(p,'playlists');

        const iconHtml = brand.icon ? `<img class="prov-brand" src="${brand.icon}" alt="${name}" onerror="this.remove()">` : '';

        return `<div class="prov-card${isSrc?' selected':''} ${brand.cls}" data-prov="${p.name}" draggable="true">
          <div class="prov-head">
            <div class="prov-title">${name}</div>
            ${iconHtml}
          </div>
          <div class="prov-caps">
            <span class="dot ${wl?'on':'off'}" title="Watchlist"></span>
            <span class="dot ${rat?'on':'off'}" title="Ratings"></span>
            <span class="dot ${hist?'on':'off'}" title="History"></span>
            <span class="dot ${pl?'on':'off'}" title="Playlists"></span>
          </div>
          <button class="btn neon" onclick="${btnOn}">${btnLab}</button>
        </div>`;
      }).join('');

      if (!host.querySelector('.cx-grid')) {
        host.innerHTML = `<div class="cx-grid">${html}</div>`;
      } else {
        host.querySelector('.cx-grid').innerHTML = html;
      }

      // graceful: broken icons weg
      host.querySelectorAll('.prov-brand').forEach(img => img.addEventListener('error', ()=>img.remove()));

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
    if(!sel.source){ window.cxStartConnect(name); return; }
    if(sel.source && sel.source !== name){ window.cxPickTarget(name); return; }
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
    if(src && target && src !== target){ window.cxPickTarget(target); }
  });

  // keep the left alignment tweak
  (function ensureLeftAlign(){
    try{
      const s = document.createElement('style');
      s.id = 'cx-left-align';
      s.textContent = `.pairs-board{display:flex;flex-direction:column;align-items:flex-start;text-align:left}`;
      if(!document.getElementById('cx-left-align')) document.head.appendChild(s);
    }catch(_){}
  })();

  // Hide legacy headers
  function _hideLegacyStaticSections(){
    try{
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
  document.addEventListener('DOMContentLoaded', function(){
    try { window.renderConnections && window.renderConnections(); } catch(_){}
    _hideLegacyStaticSections();
  });

})();
