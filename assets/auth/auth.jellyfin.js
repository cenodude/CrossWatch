// assets/auth/auth.jellyfin.js — authoritative Jellyfin settings+auth
(function (w, d) {
  if (w._jfAuthPatched) return; w._jfAuthPatched = true;

  const $ = (id) => d.getElementById(id);
  const q = (sel, root=d) => root.querySelector(sel);
  const exists = (sel) => !!q(sel);
  const waitFor = (sel, timeout=2000) => new Promise(res=>{
    const end = Date.now()+timeout;
    (function loop(){ if (exists(sel)) return res(q(sel)); if (Date.now()>end) return res(null); requestAnimationFrame(loop); })();
  });

  // keep Jellyfin banner tidy
  (function patchNotify(){
    const orig = typeof w.notify === "function" ? w.notify.bind(w) : null;
    w.notify = function(msg, ...rest){ if (String(msg||"").toLowerCase().includes("jellyfin")) return; return orig ? orig(msg, ...rest) : undefined; };
  })();

  const normalizeServer = (s)=>{ s=(s||'').trim(); if(!s) return ''; if(!/^https?:\/\//i.test(s)) s='http://'+s; return s; };
  const setErr = (el,on)=>{ try{ if(el) el.classList[on?'add':'remove']('err'); }catch{} };
  const clearErrAll = ()=>['jfy_server','jfy_user','jfy_pass'].forEach(id=>setErr($(id),false));
  const maskToken = (has)=>{ try{ if(typeof w.applyServerSecret==='function') return w.applyServerSecret('jfy_tok', !!has); }catch{} const el=$('jfy_tok'); if(el){ el.value=has?'••••••••':''; el.dataset.masked=has?'1':'0'; } };
  const _fetchConfig = async()=>{ try{ const r=await fetch('/api/config',{cache:'no-store'}); return r.ok?await r.json():null; }catch{ return null; } };

  function bar(kind, text){
    const btn = q('button.btn.jellyfin'); if (!btn) return;
    const anchor = btn.closest('.row,.group,.field,.form-row,.line,.ctrls') || btn.parentElement;
    const parent = anchor?.parentElement || btn.parentElement; if (!parent) return;
    let el = $('jfy_msg'); if (!el){ el=d.createElement('div'); el.id='jfy_msg'; el.className='msg hidden'; el.setAttribute('role','status'); el.setAttribute('aria-live','polite'); parent.insertBefore(el, anchor.nextSibling); }
    el.className = 'msg' + (kind ? ' ' + kind : ' hidden'); el.textContent = text || '';
  }

  // ---- Auth: sign in → token
  w.jfyLogin = async function(){
    const serverEl=$('jfy_server'), userEl=$('jfy_user'), passEl=$('jfy_pass');
    const btnEl=q('button.btn.jellyfin');
    const server=normalizeServer(serverEl?.value||''), username=(userEl?.value||'').trim(), password=passEl?.value||'';
    clearErrAll(); bar(null);
    const miss=[]; if(!server){setErr(serverEl,true);miss.push('server');} if(!username){setErr(userEl,true);miss.push('username');} if(!password){setErr(passEl,true);miss.push('password');}
    if(miss.length){ bar('warn', `Please fill ${miss.join(', ')}.`); return; }
    if (btnEl){ btnEl.disabled=true; btnEl.classList.add('busy'); }

    try{
      const r = await fetch('/api/jellyfin/login',{ method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({server,username,password}), cache:'no-store' });
      let j={}; try{ j=await r.json(); }catch{}
      if (!r.ok || j?.ok===false){
        const msg=(j && (j.error||j.message))||''; if (r.status===401){ setErr(userEl,true); setErr(passEl,true); } else if (r.status===502||r.status===504){ setErr(serverEl,true); }
        bar('warn', `Jellyfin login failed — ${r.status===401?'Invalid credentials': msg || `Login failed (${r.status})`}`); throw 0;
      }
      try{ if(serverEl) serverEl.value=server; if($('jfy_server_url')) $('jfy_server_url').value=server; }catch{}
      try{ if(userEl)   userEl.value=username; if($('jfy_username')) $('jfy_username').value=username; }catch{}
      try{ maskToken(true); if(passEl) passEl.value=''; }catch{}
      bar('ok', 'Jellyfin connected.');
      try{ w.updateJellyfinState?.(); }catch{}
    }finally{ if (btnEl){ btnEl.disabled=false; btnEl.classList.remove('busy'); } }
  };

  // ---- Hydrate from config
  async function hydrate(){
    try{
      const cfg = await _fetchConfig(); if(!cfg) return;
      await waitFor('#jfy_server');
      const jf = cfg.jellyfin || {};
      const server=String(jf.server||'').trim();
      const username=String(jf.username||jf.user||'').trim();
      const hasTok=!!String(jf.access_token||'').trim();
      if ($('jfy_server'))     $('jfy_server').value     = server;
      if ($('jfy_user'))       $('jfy_user').value       = username;
      if ($('jfy_server_url')) $('jfy_server_url').value = server;
      if ($('jfy_username'))   $('jfy_username').value   = username;
      maskToken(hasTok);
      // preselect libs if present
      const hist = new Set((jf.history?.libraries||[]).map(String));
      const rate = new Set((jf.ratings?.libraries||[]).map(String));
      ['jfy_lib_history','jfy_lib_ratings'].forEach(id=>{
        const el=$(id); if(!el) return;
        Array.from(el.options||[]).forEach(o=>{
          if(id==='jfy_lib_history' && hist.has(o.value)) o.selected=true;
          if(id==='jfy_lib_ratings' && rate.has(o.value)) o.selected=true;
        });
      });
      bar(hasTok ? 'ok' : null, hasTok ? 'Jellyfin connected.' : '');
    }catch(e){ console.warn('[jellyfin] hydrate failed', e); }
  }
  w.hydrateJellyfinFromConfigRaw = hydrate;

  // ---- Load helpers
  w.jfyAuto = async function(){
    try{
      const r = await fetch('/api/jellyfin/inspect?ts='+Date.now(), {cache:'no-store'});
      if(!r.ok) throw 0;
      const d = await r.json();
      if(d.server_url){ if($('jfy_server_url')) $('jfy_server_url').value=d.server_url; if($('jfy_server')) $('jfy_server').value=d.server_url; }
      if(d.username){   if($('jfy_username'))   $('jfy_username').value=d.username;   if($('jfy_user'))   $('jfy_user').value=d.username; }
    }catch{}
  };

  w.jfyLoadLibraries = async function(){
    try{
      const r = await fetch('/api/jellyfin/libraries?ts='+Date.now(), {cache:'no-store'}); if(!r.ok) throw 0;
      const d = await r.json(); const libs = Array.isArray(d?.libraries) ? d.libraries : [];
      const fill = (id) => {
        const el = $(id); if(!el) return;
        const keep = new Set(Array.from(el.selectedOptions||[]).map(o=>o.value));
        el.innerHTML='';
        libs.forEach(it=>{
          const o = d.createElement('option');
          o.value = String(it.key); o.textContent = `${it.title} (${it.type||'lib'}) — #${it.key}`;
          if(keep.has(o.value)) o.selected = true; el.appendChild(o);
        });
      };
      fill('jfy_lib_history'); fill('jfy_lib_ratings');
    }catch{}
  };

  // ---- Authoritative merge (Settings wins)
  function valsInt(sel){
    const el=q(sel); return el ? Array.from(el.selectedOptions||[]).map(o=>parseInt(o.value,10)).filter(Number.isFinite) : [];
  }
  function mergeJellyfinIntoCfg(cfg){
    const first = (...sels) => {
      for (const s of sels){ const el=q(s); if(!el) continue; const v=(el.value||'').trim(); if(v) return v; }
      return '';
    };
    const jf = (cfg.jellyfin = cfg.jellyfin || {});
    const server = first('#jfy_server_url','#jfy_server'); if (server) jf.server = server;
    const user   = first('#jfy_username','#jfy_user');    if (user) { jf.user = user; jf.username = user; }
    jf.history = Object.assign({}, jf.history||{}, { libraries: valsInt('#jfy_lib_history') });
    jf.ratings = Object.assign({}, jf.ratings||{}, { libraries: valsInt('#jfy_lib_ratings') });
    return cfg;
  }
  mergeJellyfinIntoCfg._tag = 'auth-js-authoritative';
  w.mergeJellyfinIntoCfg = mergeJellyfinIntoCfg;                 // overwrite any inline version
  w.registerSettingsCollector?.(mergeJellyfinIntoCfg);           // let crosswatch.js collect it
  d.addEventListener('settings-collect', (e)=>{                  // ensure it runs before others save
    try{ mergeJellyfinIntoCfg(e?.detail?.cfg || (w.__cfg ||= {})); }catch{}
  }, true); // capture

  // ---- Lifecycle wires
  d.addEventListener('DOMContentLoaded', () => {
    setTimeout(()=>{ try{ hydrate(); }catch{} }, 100);
    const btn = $('save-fab-btn');
    if (btn) btn.addEventListener('click', ()=>{ try{ mergeJellyfinIntoCfg(w.__cfg ||= {}); }catch{} }, true);
  });
  d.addEventListener('tab-changed', async (ev) => {
    const onSettings = ev?.detail?.id ? /settings/i.test(ev.detail.id) : !!q('#sec-jellyfin');
    if (onSettings) { await waitFor('#jfy_server'); try{ hydrate(); }catch{}; }
  });

})(window, document);
