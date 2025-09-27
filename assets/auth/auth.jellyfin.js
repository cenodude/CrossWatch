// auth.jellyfin.js — username/password login for Jellyfin (user AccessToken flow)
(function (w, d) {
  if (w._jfAuthPatched) return; w._jfAuthPatched = true;

  const $ = (id) => d.getElementById(id);

  // No global header toasts for Jellyfin
  (function patchNotify(){
    const orig = typeof w.notify === "function" ? w.notify.bind(w) : null;
    w.notify = function(msg, ...rest){
      if (String(msg||"").toLowerCase().includes("jellyfin")) return;
      return orig ? orig(msg, ...rest) : undefined;
    };
  })();

  // Helpers
  const q = (sel, root=d) => root.querySelector(sel);
  const exists = (sel) => !!q(sel);
  function waitFor(sel, timeout=2000){
    return new Promise(res=>{
      const end = Date.now()+timeout;
      (function loop(){
        if (exists(sel)) return res(q(sel));
        if (Date.now()>end) return res(null);
        requestAnimationFrame(loop);
      })();
    });
  }

  // Remove inline bar node
  function hideBar(){
    const el = $('jfy_msg');
    if (el && el.parentNode) el.parentNode.removeChild(el);
  }

  // Inline bar directly under the Sign in button (only on Settings)
  function bar(kind, text) {
    const btn = q('button.btn.jellyfin');
    if (!btn) return; // never show outside Settings

    function placeBelowLogin(el){
      const anchor = btn.closest('.row,.group,.field,.form-row,.line,.ctrls') || btn.parentElement;
      const parent = anchor && anchor.parentElement ? anchor.parentElement : btn.parentElement;
      if (!parent) return false;
      const after = anchor.nextSibling;
      if (after) {
        if (el.parentNode !== parent || el.previousSibling !== anchor) parent.insertBefore(el, after);
      } else {
        if (el.parentNode !== parent || el.previousSibling !== anchor) parent.appendChild(el);
      }
      return true;
    }

    let el = $('jfy_msg');
    if (!el) {
      el = d.createElement('div');
      el.id = 'jfy_msg';
      el.className = 'msg hidden';
      el.setAttribute('role','status');
      el.setAttribute('aria-live','polite');
      if (!placeBelowLogin(el)) return; // no fallback → prevents Main leaks
    } else {
      placeBelowLogin(el);
    }

    el.className = 'msg' + (kind ? ' ' + kind : ' hidden');
    el.textContent = text || '';
  }

  // Small utils
  function normalizeServer(s){ s=(s||'').trim(); if(!s) return ''; if(!/^https?:\/\//i.test(s)) s='http://'+s; return s; }
  function setErr(el,on){ try{ if(el) el.classList[on?'add':'remove']('err'); }catch{} }
  function clearErrAll(){ ['jfy_server','jfy_user','jfy_pass'].forEach(id=>setErr($(id),false)); }

  // Sign in
  async function jfyLogin() {
    const serverEl=$('jfy_server'), userEl=$('jfy_user'), passEl=$('jfy_pass');
    const btnEl=q('button.btn.jellyfin');
    const server=normalizeServer(serverEl?.value||''), username=(userEl?.value||'').trim(), password=passEl?.value||'';

    clearErrAll(); bar(null);
    const miss=[]; if(!server){setErr(serverEl,true);miss.push('server');} if(!username){setErr(userEl,true);miss.push('username');} if(!password){setErr(passEl,true);miss.push('password');}
    if(miss.length){ bar('warn', `Please fill ${miss.join(', ')}.`); return; }

    if (btnEl){ btnEl.disabled=true; btnEl.classList.add('busy'); }

    try {
      const r = await fetch('/api/jellyfin/login', {
        method:'POST', headers:{ 'Content-Type':'application/json' },
        body: JSON.stringify({ server, username, password }), cache:'no-store',
      });
      let j={}; try{ j=await r.json(); }catch{}

      if (!r.ok || j?.ok===false) {
        const serverMsg=(j && (j.error||j.message))||'';
        if (r.status===401){ setErr(userEl,true); setErr(passEl,true); }
        else if (r.status===502||r.status===504){ setErr(serverEl,true); }
        else if (r.status===400){
          const msg=(serverMsg||'').toLowerCase();
          if (msg.includes('server')) setErr(serverEl,true);
          if (msg.includes('username')) setErr(userEl,true);
          if (msg.includes('password')) setErr(passEl,true);
        }
        const errMsg = r.status===401 ? 'Invalid credentials' : serverMsg || `Login failed (${r.status})`;
        bar('warn', `Jellyfin login failed — ${errMsg}`);
        throw new Error(errMsg);
      }

      // success
      try{ if(serverEl) serverEl.value=server; }catch{}
      try{ if(userEl)   userEl.value=username; }catch{}
      try{ w.applyServerSecret?.('jfy_tok', true); }catch{}
      try{ const tok=$('jfy_tok'); if(tok && (!tok.value||tok.value==='')){ tok.value='••••••••'; tok.dataset.masked='1'; } }catch{}
      try{ if(passEl) passEl.value=''; }catch{}

      bar('ok', 'Jellyfin connected.');
      try{ w.updateJellyfinState?.(); }catch{}
    } catch(e) {
      console.warn('Jellyfin login failed', e);
    } finally {
      if (btnEl){ btnEl.disabled=false; btnEl.classList.remove('busy'); }
    }
  }
  w.jfyLogin = jfyLogin;

  // Config → fields (wait for inputs before filling)
  async function _fetchConfig(){ try{ const r=await fetch('/api/config',{cache:'no-store'}); if(!r.ok) return null; return (await r.json())||null; }catch{ return null; } }
  function _maskToken(has){ try{ if(typeof w.applyServerSecret==='function') return w.applyServerSecret('jfy_tok', !!has); }catch{} const el=$('jfy_tok'); if(el){ el.value=has?'••••••••':''; el.dataset.masked=has?'1':'0'; } }

  async function hydrateJellyfinFromConfigRaw(){
    try{
      const cfg = await _fetchConfig(); if(!cfg) return;
      // wait for fields to exist (lazy tabs)
      await waitFor('#jfy_server');
      const sEl=$('jfy_server'), uEl=$('jfy_user');
      if(!sEl || !uEl) return;
      const jf = cfg.jellyfin || {};
      const server=String(jf.server||'').trim(), username=String(jf.username||jf.user||'').trim(), hasTok=!!String(jf.access_token||'').trim();
      sEl.value = server; uEl.value = username; _maskToken(hasTok);
      bar(hasTok ? 'ok' : null, hasTok ? 'Jellyfin connected.' : '');
    }catch(e){ console.warn('[jellyfin] hydrate failed', e); }
  }
  w.hydrateJellyfinFromConfigRaw = hydrateJellyfinFromConfigRaw;

  // Wire up
  d.addEventListener('DOMContentLoaded', () => {
    setTimeout(() => { try{ hydrateJellyfinFromConfigRaw(); }catch{} }, 100);
  });

  // Hide messages outside Settings; hydrate when returning
  d.addEventListener('tab-changed', async (ev) => {
    const onSettings = ev?.detail?.id ? /settings/i.test(ev.detail.id) : !!q('#sec-jellyfin');
    if (onSettings) {
      // ensure fields exist, then hydrate
      await waitFor('#jfy_server');
      try{ hydrateJellyfinFromConfigRaw(); }catch{}
    } else {
      hideBar();
    }
  });

})(window, document);
