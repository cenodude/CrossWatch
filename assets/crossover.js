// script block #1
/* ====== Globals ====== */
  let lastStatusMs = 0;
  const STATUS_MIN_INTERVAL = 120000; // ms

  let busy=false, esDet=null, esSum=null, plexPoll=null, simklPoll=null, appDebug=false, currentSummary=null;
  let detStickBottom = true;  // auto-stick to bottom voor details-log
  let wallLoaded=false, _lastSyncEpoch=null, _wasRunning=false;
  window._ui = { status: null, summary: null };

  /* ====== Utilities ====== */
  // Render dynamic auth provider cards if container exists
  function renderAuthCards(providers){
    const host = document.getElementById('auth-cards') || document.getElementById('settings-auth') || document.querySelector('[data-auth-cards]');
    if (!host) return; // nothing to render into; keep legacy badges
    // build cards
    const frag = document.createDocumentFragment();
    providers.forEach(p => {
      const st = p.status || {};
      const man = p.manifest || {};
      const caps = p.capabilities || {};
      const card = document.createElement('div');
      card.className = 'auth-card';
      const connected = !!st.connected;
      const header = document.createElement('div');
      header.className = 'auth-card-header';
      header.innerHTML = `<h3>${man.label || p.name}</h3><span class="status ${connected?'ok':'no'}">${connected?'Connected':'Not connected'}</span>`;
      card.appendChild(header);

      if (man.notes){
        const notes = document.createElement('div');
        notes.className = 'auth-card-notes';
        notes.textContent = man.notes;
        card.appendChild(notes);
      }

      // Fields (api_keys/oauth fields)
      if (Array.isArray(man.fields) && man.fields.length){
        const form = document.createElement('div');
        form.className = 'auth-card-fields';
        man.fields.forEach(f => {
          const row = document.createElement('div');
          row.className = 'field';
          const input = document.createElement('input');
          input.type = f.type || 'text';
          input.placeholder = f.label || f.key;
          input.dataset.key = f.key;
          row.appendChild(input);
          form.appendChild(row);
        });
        card.appendChild(form);
        card._fieldsForm = form;
      }

      // Actions
      const actions = document.createElement('div');
      actions.className = 'auth-card-actions';

      // helper to gather form values into config keys
      async function saveFieldValues(){
        const form = card._fieldsForm;
        if (!form) return;
        const inputs = form.querySelectorAll('input[data-key]');
        if (!inputs.length) return;
        // POST to a tiny helper endpoint if you have one; otherwise, update via start which persists cfg
        // Here we include field values as part of start payload so backend can save them
      }

      const doStart = async ()=>{
        try{
          // gather field values
          const payload = { redirect_uri: window.location.origin + '/callback' };
          if (card._fieldsForm){
            for (const el of card._fieldsForm.querySelectorAll('input[data-key]')){
              payload[el.dataset.key] = el.value;
              // also persist to config-specific namespaces via dedicated endpoints if needed
            }
          }
          const r = await fetch(`/api/platform/auth/${p.name}/start`, {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify(payload)
          });
          const j = await r.json();
          if (man.flow === 'oauth' && j && j.url){
            window.location.href = j.url;
          } else if (man.flow === 'device_pin' && j && j.pin){
            alert(`PIN: ${j.pin}\nGo to: ${j.verify_url || man.verify_url || ''}`);
          }
        }catch(e){ console.warn(e); }
      };

      const doFinish = async ()=>{
        try{
          const r = await fetch(`/api/platform/auth/${p.name}/finish`, { method:'POST' });
          await r.json();
          refreshStatus();
        }catch(e){ console.warn(e); }
      };

      const doRefresh = async ()=>{
        try{
          const r = await fetch(`/api/platform/auth/${p.name}/refresh`, { method:'POST' });
          await r.json();
          refreshStatus();
        }catch(e){ console.warn(e); }
      };

      const doDisconnect = async ()=>{
        try{
          const r = await fetch(`/api/platform/auth/${p.name}/disconnect`, { method:'POST' });
          await r.json();
          refreshStatus();
        }catch(e){ console.warn(e); }
      };

      const can = man.actions || {};
      // Start/connect
      if (can.start){
        const btn = document.createElement('button');
        btn.className = 'btn';
        btn.textContent = connected ? 'Reconnect' : (man.flow === 'oauth' ? 'Connect' : 'Get PIN');
        btn.onclick = doStart;
        actions.appendChild(btn);
      }
      // Finish (for device_pin)
      if (can.finish){
        const btn = document.createElement('button');
        btn.className = 'btn';
        btn.textContent = 'Check PIN';
        btn.onclick = doFinish;
        actions.appendChild(btn);
      }
      // Refresh
      if (can.refresh){
        const btn = document.createElement('button');
        btn.className = 'btn';
        btn.textContent = 'Refresh';
        btn.onclick = doRefresh;
        actions.appendChild(btn);
      }
      // Disconnect
      if (can.disconnect && connected){
        const btn = document.createElement('button');
        btn.className = 'btn danger';
        btn.textContent = 'Disconnect';
        btn.onclick = doDisconnect;
        actions.appendChild(btn);
      }

      card.appendChild(actions);

      // Capabilities summary
      if (caps && caps.features){
        const feats = document.createElement('div');
        feats.className = 'auth-card-caps';
        const items = Object.keys(caps.features).map(k => {
          const f = caps.features[k] || {};
          const rw = [f.read?'R':'-', f.write?'W':'-'].join('/');
          return `<span class="cap"><code>${k}</code> ${rw}</span>`;
        }).join('');
        feats.innerHTML = `<div class="caps">${items}</div>`;
        card.appendChild(feats);
      }

      frag.appendChild(card);
    });
    host.innerHTML = '';
    host.appendChild(frag);
  }

  function computeRedirectURI(){ return location.origin + '/callback'; }

  function flashCopy(btn, ok, msg){
    if (!btn) { if(!ok) alert(msg || 'Copy failed'); return; }
    const old = btn.textContent;
    btn.disabled = true;
    btn.textContent = ok ? 'Copied ✓' : (msg || 'Copy failed');
    setTimeout(()=>{ btn.textContent = old; btn.disabled = false; }, 1200);
  }

  function setRunProgress(pct){
    const btn = document.getElementById('run');
    if (!btn) return;
    const p = Math.max(0, Math.min(100, Math.floor(pct)));
    btn.style.setProperty('--prog', String(p));
  }

  function startRunVisuals(indeterminate = true){
    const btn = document.getElementById('run');
    if (!btn) return;
    btn.classList.add('loading');
    btn.classList.toggle('indet', !!indeterminate);
    if (indeterminate) setRunProgress(8);
  }

  function stopRunVisuals(){
    const btn = document.getElementById('run');
    if (!btn) return;
    setRunProgress(100);
    btn.classList.remove('indet');
    setTimeout(() => { btn.classList.remove('loading'); setRunProgress(0); }, 700);
  }

  function updateProgressFromTimeline(tl){
    const order = ['start','pre','post','done'];
    let done = 0;
    for (const k of order){ if (tl && tl[k]) done++; }
    let pct = (done / order.length) * 100;
    if (pct > 0 && pct < 15) pct = 15;
    setRunProgress(pct);
  }

  function recomputeRunDisabled() {
    const btn = document.getElementById('run');
    if (!btn) return;
    const busyNow = !!window.busy;
    const canRun = !(window._ui?.status) ? true : !!window._ui.status.can_run;
    const running = !!(window._ui?.summary && window._ui.summary.running);
    btn.disabled = busyNow || running || !canRun;
  }

  function setTimeline(tl){ ['start','pre','post','done'].forEach(k=>{ document.getElementById('tl-'+k).classList.toggle('on', !!(tl && tl[k])); }); }

  function setSyncHeader(status, msg){
    const icon = document.getElementById('sync-icon');
    icon.classList.remove('sync-ok','sync-warn','sync-bad'); icon.classList.add(status);
    document.getElementById('sync-status-text').textContent = msg;
  }

  function relTimeFromEpoch(epoch){
    if(!epoch) return '';
    const secs = Math.max(1, Math.floor(Date.now()/1000 - epoch));
    const units = [["y",31536000],["mo",2592000],["d",86400],["h",3600],["m",60],["s",1]];
    for(const [label,span] of units){
      if(secs >= span) return Math.floor(secs/span) + label + " ago";
    }
    return "just now";
  }

  /* ====== About modal ====== */
  async function openAbout(){
    try{
      // Bust any cache so we don't show an old payload
      const r = await fetch('/api/version?cb=' + Date.now(), { cache: 'no-store' });
      const j = r.ok ? await r.json() : {};

      const cur    = (j.current ?? '0.0.0').toString().trim();
      const latest = (j.latest  ?? '').toString().trim() || null;
      const url    = j.html_url || 'https://github.com/cenodude/plex-simkl-watchlist-sync/releases';
      const upd    = !!j.update_available;

      // "Version x.y.z" — from CURRENT
      const verEl = document.getElementById('about-version');
      if (verEl){
        verEl.textContent = `Version ${j.current}`;
        verEl.dataset.version = cur; // guard against later accidental overwrites
      }

      // If you also show a version elsewhere, keep it in sync (safe no-op if missing)
      const headerVer = document.getElementById('app-version');
      if (headerVer){
        headerVer.textContent = `Version ${cur}`;
        headerVer.dataset.version = cur;
      }

      // Latest release link/label (separate from current)
      const relEl = document.getElementById('about-latest');
      if (relEl){
        relEl.href = url;
        relEl.textContent = latest ? `v${latest}` : 'Releases';
        relEl.setAttribute('aria-label', latest ? `Latest release v${latest}` : 'Releases');
      }

      // Update badge
      const updEl = document.getElementById('about-update');
      if (updEl){
        updEl.classList.add('badge','upd');
        if (upd && latest){
          updEl.textContent = `Update ${latest} available`;
          updEl.classList.remove('hidden','reveal');
          void updEl.offsetWidth; // restart CSS animation
          updEl.classList.add('reveal');
        } else {
          updEl.textContent = '';
          updEl.classList.add('hidden');
          updEl.classList.remove('reveal');
        }
      }
    } catch(_) {}
    document.getElementById('about-backdrop')?.classList.remove('hidden');
  }

  function closeAbout(ev){
    if (ev && ev.type === 'click' && ev.currentTarget !== ev.target) return; // ignore clicks inside card
    document.getElementById('about-backdrop')?.classList.add('hidden');
  }

  document.addEventListener('keydown', (e)=>{ if (e.key === 'Escape') closeAbout(); });


  /* ====== Tabs ====== */
  async function showTab(n) {
    const pageSettings  = document.getElementById('page-settings');
    const pageWatchlist = document.getElementById('page-watchlist');
    const logPanel      = document.getElementById('log-panel');
    const layout        = document.getElementById('layout');
    const statsCard     = document.getElementById('stats-card');

    document.getElementById('tab-main')     ?.classList.toggle('active', n === 'main');
    document.getElementById('tab-watchlist')?.classList.toggle('active', n === 'watchlist');
    document.getElementById('tab-settings') ?.classList.toggle('active', n === 'settings');

    document.getElementById('ops-card')        ?.classList.toggle('hidden', n !== 'main');
    document.getElementById('placeholder-card')?.classList.toggle('hidden', n !== 'main');
    statsCard                                   ?.classList.toggle('hidden', n !== 'main');
    pageWatchlist?.classList.toggle('hidden', n !== 'watchlist');
    pageSettings ?.classList.toggle('hidden', n !== 'settings');

    const hasStats = !!(statsCard && !statsCard.classList.contains('hidden'));
    if (n === 'main') {
      layout.classList.remove('single');
      layout.classList.toggle('full', !appDebug && !hasStats);
      refreshStatus();
      if (!esSum) openSummaryStream();
      await updatePreviewVisibility();
      refreshSchedulingBanner();
      refreshStats(true);
    } else {
      layout.classList.add('single');
      layout.classList.remove('full');
      logPanel.classList.add('hidden');
      if (n === 'watchlist') { loadWatchlist(); }
      else { document.getElementById('sec-auth')?.classList.add('open'); await loadConfig();
            updateTmdbHint?.(); updateSimklHint?.(); updateSimklButtonState?.(); loadScheduling?.(); }
    }
  }



  function toggleSection(id){ document.getElementById(id).classList.toggle('open'); }

  /* ====== Run (synchronize) ====== */
  function setBusy(v){ busy = v; recomputeRunDisabled(); }

  async function runSync() {
    if (busy) return;
    const btn = document.getElementById('run');
    setBusy(true);
    try { btn?.classList.add('glass'); } catch(_){}
    try {
      const resp = await fetch('/api/run', { method: 'POST' });
      const j = await resp.json();
      if (!resp.ok || !j || j.ok !== true) {
        setSyncHeader('sync-bad', `Failed to start${j?.error ? ` – ${j.error}` : ''}`);
      } else {
        await updateWatchlistPreview();
      }
    } catch (e) {
      setSyncHeader('sync-bad', 'Failed to reach server');
    } finally {
      setBusy(false);
      recomputeRunDisabled();
      refreshStatus();
    }
  }

  /* Version check + update notification */
  const UPDATE_CHECK_INTERVAL_MS = 12 * 60 * 60 * 1000;
  let _updInfo = null;

  function openUpdateModal(){
    if(!_updInfo) return;
    document.getElementById('upd-modal').classList.remove('hidden');
    document.getElementById('upd-title').textContent = `v${_updInfo.latest}`;
    document.getElementById('upd-notes').textContent = _updInfo.notes || '(No release notes)';
    document.getElementById('upd-link').href = _updInfo.url || '#';
  }
  function closeUpdateModal(){ document.getElementById('upd-modal').classList.add('hidden'); }
  function dismissUpdate(){
    if(_updInfo?.latest){ localStorage.setItem('dismissed_version', _updInfo.latest); }
    document.getElementById('upd-pill').classList.add('hidden');
    closeUpdateModal();
  }

  /* expanded insight Statistics */
  function setStatsExpanded(expanded){
    const sc = document.getElementById('stats-card');
    if(!sc) return;
    sc.classList.toggle('collapsed', !expanded);
    sc.classList.toggle('expanded', !!expanded);
    if (expanded) { try { refreshInsights(); } catch(e){} }
  }

  function isElementOpen(el){
    if (!el) return false;
    // consider "open"/"expanded"/"show" class OR visibility/height as open
    const c = el.classList || {};
    if (c.contains?.('open') || c.contains?.('expanded') || c.contains?.('show')) return true;
    const style = window.getComputedStyle(el);
    return !(style.display === 'none' || style.visibility === 'hidden' || el.offsetHeight === 0);
  }

  function findDetailsButton(){
    // try common IDs/classes first
    return document.getElementById('btn-details')
        || document.querySelector('[data-action="details"], .btn-details')
        || Array.from(document.querySelectorAll('button'))
            .find(b => (b.textContent || '').trim().toLowerCase() === 'view details');
  }

  function findDetailsPanel(){
    // adapt to your DOM: try several likely targets
    return document.getElementById('sync-output')
        || document.getElementById('details')
        || document.querySelector('#sync-log, .sync-output, [data-pane="details"]');
  }

  function wireDetailsToStats(){
    const btn = findDetailsButton();
    const panel = findDetailsPanel();

    // set initial state based on whether details are open at load
    setStatsExpanded(isElementOpen(panel));

    if (btn){
      btn.addEventListener('click', () => {
        // wait a tick for the panel to toggle
        setTimeout(() => setStatsExpanded(isElementOpen(panel)), 50);
      });
    }

    // optional: collapse stats when starting a new sync
    const syncBtn = document.getElementById('btn-sync') 
                || document.querySelector('[data-action="sync"], .btn-sync');
    if (syncBtn){
      syncBtn.addEventListener('click', () => setStatsExpanded(false));
    }
  }

  document.addEventListener('DOMContentLoaded', wireDetailsToStats);

  // BLOCK INSIGHT
  async function fetchJSON(u){ const r = await fetch(u, {cache: 'no-store'}); return r.ok ? r.json() : null; }
  

  async function refreshInsights(){
    const data = await fetchJSON('/api/insights?limit_samples=60&history=3');
    if(!data) return;

    // 1) Sparkline
    renderSparkline('sparkline', data.series || []);

    // 3) History
    const hist = data.history || [];
    const hEl = document.getElementById('sync-history');
    hEl.innerHTML = hist.map(row => {
      const dur = (row.duration_sec!=null) ? (row.duration_sec.toFixed ? row.duration_sec.toFixed(1) : row.duration_sec) : '—';
      const added = (row.added ?? '—');
      const removed = (row.removed ?? '—');
      const badgeClass = (String(row.result||'').toUpperCase()==='EQUAL') ? 'ok' : 'warn';
      const when = row.finished_at || row.started_at || '';
      return `
        <div class="history-item">
          <div class="history-meta">
            <span class="badge ${badgeClass}">${row.result || '—'}</span>
            <span>${when}</span>
            <span>⏱ ${dur}s</span>
          </div>
          <div class="history-meta">
            <span class="badge">+${added}</span>
            <span class="badge">-${removed}</span>
            <span class="badge">P:${row.plex_post ?? '—'}</span>
            <span class="badge">S:${row.simkl_post ?? '—'}</span>
          </div>
        </div>`;
    }).join('') || `<div class="history-item"><div class="history-meta">No history yet</div></div>`;

    // 6) Watchtime
    const wt = data.watchtime || {minutes:0, movies:0, shows:0, method:'fallback'};
    const wEl = document.getElementById('watchtime');
    const note = document.getElementById('watchtime-note');
    wEl.innerHTML = `
      <div class="big">≈ ${wt.hours}</div>
      <div class="units">hrs <span style="opacity:.6">(${wt.days} days)</span><br>
        <span style="opacity:.8">${wt.movies} movies • ${wt.shows} shows</span>
      </div>`;
    note.textContent = (wt.method==='tmdb') ? 'Based on TMDb runtimes' :
                      (wt.method==='mixed') ? 'TMDb + defaults (115/45 min)' :
                      'Defaults used (115/45 min)';
  }

  // lightweight SVG sparkline with neon gradient
  function renderSparkline(id, points){
    const el = document.getElementById(id);
    if(!el){ return; }
    if(!points.length){ el.innerHTML = `<div class="muted">No data yet</div>`; return; }

    const w = el.clientWidth || 260, h = el.clientHeight || 64, pad=4;
    const xs = points.map(p=>Number(p.ts)||0);
    const ys = points.map(p=>Number(p.count)||0);
    const minX = Math.min(...xs), maxX = Math.max(...xs);
    const minY = Math.min(...ys), maxY = Math.max(...ys);
    const x = t => (maxX===minX)? pad : pad + (w-2*pad) * (t-minX)/(maxX-minX);
    const y = v => (maxY===minY)? (h/2) : (h-pad) - (h-2*pad) * (v-minY)/(maxY-minY);

    const d = points.map((p,i)=> (i? 'L':'M') + x(p.ts) + ',' + y(p.count)).join(' ');
    const dots = points.map(p=> `<circle class="dot" cx="${x(p.ts)}" cy="${y(p.count)}"></circle>`).join('');
    el.innerHTML = `
    <svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
      <defs>
        <linearGradient id="spark-grad" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0" stop-color="var(--grad1,#7c5cff)"/>
          <stop offset="1" stop-color="var(--grad2,#2de2ff)"/>
        </linearGradient>
      </defs>
      <path class="line" d="${d}"></path>
      ${dots}
    </svg>`;
  }

  document.addEventListener('DOMContentLoaded', refreshInsights);


async function checkForUpdate() {
  try {
    const r = await fetch('/api/version', { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();

    const cur     = j.current || '0.0.0';
    const latest  = j.latest  || null;
    const url     = j.html_url || 'https://github.com/cenodude/plex-simkl-watchlist-sync/releases';
    const hasUpdate = !!j.update_available;

    // Update the "Version x.y.z" label (if present)
    const vEl = document.getElementById('app-version');
    if (vEl) vEl.textContent = `Version ${cur}`;

    // Update badge (right side)
    const updEl = document.getElementById('st-update');
    if (!updEl) return;

    // Make sure it has the badge classes (harmless if already present)
    updEl.classList.add('badge','upd');

    if (hasUpdate && latest) {
      // Only re-animate if the version changed since last check
      const prev = updEl.dataset.lastLatest || '';
      const changed = (latest !== prev);

      updEl.innerHTML = `<a href="${url}" target="_blank" rel="noopener" title="Open release page">
                           Update ${latest} available
                         </a>`;
      updEl.setAttribute('aria-label', `Update ${latest} available`);
      updEl.classList.remove('hidden');

      if (changed) {
        // store for next time
        updEl.dataset.lastLatest = latest;
        // retrigger the one-time "reveal" animation
        updEl.classList.remove('reveal'); // reset
        void updEl.offsetWidth;           // reflow to restart CSS animation
        updEl.classList.add('reveal');    // CSS handles pop + pulse
      }
    } else {
      // No update -> hide and clear state
      updEl.classList.add('hidden');
      updEl.classList.remove('reveal');
      updEl.removeAttribute('aria-label');
      updEl.removeAttribute('data-last-latest');
      updEl.textContent = '';
    }
  } catch (err) {
    console.debug('Version check failed:', err);
  }
}

// Run once after DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  checkForUpdate();
  // Optional: re-check when the tab becomes visible again
  // document.addEventListener('visibilitychange', () => {
  //   if (!document.hidden) checkForUpdate();
  // });
});

  // tiny toast 
  function showToast(text, onClick){
    const toast = document.createElement('div');
    toast.className = 'msg ok';
    toast.textContent = text;
    toast.style.position = 'fixed';
    toast.style.right = '16px'; toast.style.bottom = '16px';
    toast.style.zIndex = 1000; toast.style.cursor = 'pointer';
    toast.onclick = () => { onClick && onClick(); toast.remove(); }
    document.body.appendChild(toast);
    setTimeout(()=>toast.classList.add('hidden'), 3000);
  }

  // call at boot, and on a timer
  checkForUpdate(true);
  setInterval(()=>checkForUpdate(false), UPDATE_CHECK_INTERVAL_MS);

  /* ====== Summary stream + details log ====== */
  function renderSummary(sum){
    currentSummary = sum;
    window._ui = window._ui || {};
    window._ui.summary = sum;

    const pp = sum.plex_post ?? sum.plex_pre;
    const sp = sum.simkl_post ?? sum.simkl_pre;
    document.getElementById('chip-plex').textContent  = (pp ?? '–');
    document.getElementById('chip-simkl').textContent = (sp ?? '–');
    document.getElementById('chip-dur').textContent   = sum.duration_sec != null ? (sum.duration_sec + 's') : '–';
    document.getElementById('chip-exit').textContent  = sum.exit_code  != null ? String(sum.exit_code)   : '–';

    if (sum.running){
      setSyncHeader('sync-warn', 'Running…');
    } else if (sum.exit_code === 0){
      setSyncHeader('sync-ok', (sum.result||'').toUpperCase()==='EQUAL' ? 'In sync ' : 'Synced ');
    } else if (sum.exit_code != null){
      setSyncHeader('sync-bad', 'Attention needed ⚠️');
    } else {
      setSyncHeader('sync-warn', 'Idle — run a sync to see results');
    }

    document.getElementById('det-cmd').textContent    = sum.cmd        || '–';
    document.getElementById('det-ver').textContent    = sum.version    || '–';
    document.getElementById('det-start').textContent  = sum.started_at || '–';
    document.getElementById('det-finish').textContent = sum.finished_at|| '–';

    const tl = sum.timeline || {};
    setTimeline(tl);
    updateProgressFromTimeline?.(tl);

    const btn = document.getElementById('run');
    if (btn){
      if (sum.running){
        btn.classList.add('glass','loading');
        if (tl.pre || tl.post || tl.done) btn.classList.remove('indet');
        else                               btn.classList.add('indet');
        if (!_wasRunning && !(tl.pre || tl.post || tl.done)) { setRunProgress?.(8); }
      } else {
        if (_wasRunning){
          setRunProgress?.(100);
          btn.classList.remove('indet');
          setTimeout(()=>{ btn.classList.remove('loading','glass'); setRunProgress?.(0); }, 700);
        } else {
          btn.classList.remove('loading','indet','glass');
          setRunProgress?.(0);
        }
      }
    }

    if (typeof recomputeRunDisabled === 'function') recomputeRunDisabled();

    if (_wasRunning && !sum.running) {
      window.wallLoaded = false;
      updatePreviewVisibility?.();
      loadWatchlist?.();
      refreshSchedulingBanner?.();
    }
    _wasRunning = !!sum.running;
  }

  function openSummaryStream(){
    esSum = new EventSource('/api/run/summary/stream');
    esSum.onmessage = (ev)=>{ try{ renderSummary(JSON.parse(ev.data)); }catch(_){} };
    fetch('/api/run/summary').then(r=>r.json()).then(renderSummary).catch(()=>{});
  }

    let _lastStatsFetch = 0;

    function _ease(t){ return t<.5 ? 2*t*t : -1+(4-2*t)*t; }
    function animateNumber(el,to){
      const from = parseInt(el.dataset.v||'0',10)||0;
      if(from===to){ el.textContent=String(to); el.dataset.v=String(to); return; }
      const dur=600, t0=performance.now();
      function step(now){
        const p=Math.min(1,(now-t0)/dur), v=Math.round(from+(to-from)*_ease(p));
        el.textContent=String(v);
        if(p<1) requestAnimationFrame(step); else el.dataset.v=String(to);
      }
      requestAnimationFrame(step);
    }

    function animateChart(now,week,month){
      const bars={
        now:document.querySelector('.bar.now'),
        week:document.querySelector('.bar.week'),
        month:document.querySelector('.bar.month'),
      };
      const max=Math.max(1,now,week,month);
      const h=v=>Math.max(.04,v/max);
      if(bars.week)  bars.week.style.transform  = `scaleY(${h(week)})`;
      if(bars.month) bars.month.style.transform = `scaleY(${h(month)})`;
      if(bars.now)   bars.now.style.transform   = `scaleY(${h(now)})`;
    }

    async function refreshStats(force=false){
      const nowT = Date.now();
      if (!force && nowT - _lastStatsFetch < 900) return;
      _lastStatsFetch = nowT;

      try{
        const j = await fetch('/api/stats', { cache:'no-store' }).then(r=>r.json());
        if (!j?.ok) return;

        const elNow = document.getElementById('stat-now');
        const elW   = document.getElementById('stat-week');
        const elM   = document.getElementById('stat-month');
        if (!elNow || !elW || !elM) return;

        const n = j.now|0, w = j.week|0, m = j.month|0;

        animateNumber(elNow, n);
        animateNumber(elW,   w);
        animateNumber(elM,   m);

        // meter
        const max = Math.max(1, n, w, m);
        const fill = document.getElementById('stat-fill');
        if (fill) fill.style.width = Math.round((n / max) * 100) + '%';

        // deltas
        const bumpOne = (delta, label) => {
          const t = document.getElementById('trend-week'); if (!t) return;
          const cls = delta>0 ? 'up' : (delta<0 ? 'down' : 'flat');
          t.className = 'chip trend ' + cls;
          t.textContent = delta === 0 ? 'no change' : `${delta>0?'+':''}${delta} vs ${label}`;
          if (cls === 'up'){ const c = document.getElementById('stats-card'); c?.classList.remove('celebrate'); void c?.offsetWidth; c?.classList.add('celebrate'); }
        };
        bumpOne(n - w, 'last week'); // or: bumpOne(n - m, 'last month')

        // optional API fields
        const by        = j.by_source || {};
        const totalAdd  = Number.isFinite(j.added)   ? j.added   : null; // all-time totals
        const totalRem  = Number.isFinite(j.removed) ? j.removed : null;
        const lastAdd   = Number.isFinite(j.new)     ? j.new     : null; // last run only
        const lastRem   = Number.isFinite(j.del)     ? j.del     : null;

        // legend numbers (all-time)
        const setTxt = (id, val) => {
          const el = document.getElementById(id);
          if (el) el.textContent = String(val ?? 0);
        };
        setTxt('stat-added',   totalAdd);
        setTxt('stat-removed', totalRem);

        // tiles (last run only, auto-hide when null)
        const setTile = (tileId, numId, val) => {
          const t = document.getElementById(tileId), nEl = document.getElementById(numId);
          if (!t || !nEl) return;
          if (val == null) { t.hidden = true; return; }
          nEl.textContent = String(val); t.hidden = false;
        };
        setTile('tile-new', 'stat-new', lastAdd);
        setTile('tile-del', 'stat-del', lastRem);

        // brand totals for Plex / SIMKL (transparent tiles + subtle edge glow)
        const plexVal  = Number.isFinite(by.plex_total)  ? by.plex_total  : ((by.plex  ?? 0) + (by.both ?? 0));
        const simklVal = Number.isFinite(by.simkl_total) ? by.simkl_total : ((by.simkl ?? 0) + (by.both ?? 0));

        const elP = document.getElementById('stat-plex');
        const elS = document.getElementById('stat-simkl');

        const curP = Number(elP?.textContent || 0);
        const curS = Number(elS?.textContent || 0);

        const pop = (el) => { if (!el) return; el.classList.remove('bump'); void el.offsetWidth; el.classList.add('bump'); };

        if (elP){
          if (plexVal !== curP){ animateNumber(elP, plexVal); pop(elP); }
          else { elP.textContent = String(plexVal); }
        }
        if (elS){
          if (simklVal !== curS){ animateNumber(elS, simklVal); pop(elS); }
          else { elS.textContent = String(simklVal); }
        }

        // ensure tiles are visible
        document.getElementById('tile-plex')?.removeAttribute('hidden');
        document.getElementById('tile-simkl')?.removeAttribute('hidden');

      }catch(_){}
    }

    function _setBarValues(n,w,m){
      const bw=document.querySelector('.bar.week');
      const bm=document.querySelector('.bar.month');
      const bn=document.querySelector('.bar.now');
      if(bw) bw.dataset.v = String(w);
      if(bm) bm.dataset.v = String(m);
      if(bn) bn.dataset.v = String(n);
    }

    function _initStatsTooltip(){
      const chart = document.getElementById('stats-chart');
      const tip   = document.getElementById('stats-tip');
      if(!chart || !tip) return;

      const map = [
        {el: document.querySelector('.bar.week'),  label: 'Last Week'},
        {el: document.querySelector('.bar.month'), label: 'Last Month'},
        {el: document.querySelector('.bar.now'),   label: 'Now'},
      ];

      function show(e, label, value){
        tip.textContent = `${label}: ${value} items`;
        tip.style.left = e.offsetX + 'px';
        tip.style.top  = e.offsetY + 'px';
        tip.classList.add('show'); tip.hidden = false;
      }
      function hide(){ tip.classList.remove('show'); tip.hidden = true; }

      map.forEach(({el,label})=>{
        if(!el) return;
        el.addEventListener('mousemove', (ev)=>{
          const rect = chart.getBoundingClientRect();
          const x = ev.clientX - rect.left, y = ev.clientY - rect.top;
          show({offsetX:x,offsetY:y}, label, el.dataset.v || '0');
        });
        el.addEventListener('mouseleave', hide);
        el.addEventListener('touchstart', (ev)=>{
          const t = ev.touches[0];
          const rect = chart.getBoundingClientRect();
          show({offsetX:t.clientX-rect.left, offsetY:t.clientY-rect.top}, label, el.dataset.v || '0');
        }, {passive:true});
        el.addEventListener('touchend', ()=>{ tip.classList.remove('show'); }, {passive:true});
      });
    }

    // Call once on boot
    document.addEventListener('DOMContentLoaded', _initStatsTooltip);

  // Call at boot
  document.addEventListener('DOMContentLoaded', () => { refreshStats(true); });

  // Nudge the stats whenever the summary updates or a run finishes
  const _origRenderSummary = (typeof renderSummary === 'function') ? renderSummary : null;
  window.renderSummary = function(sum){
    if (_origRenderSummary) _origRenderSummary(sum);
    refreshStats(false);
  };


  function openDetailsLog(){
    const el = document.getElementById('det-log');
    const slider = document.getElementById('det-scrub');
    if (!el) return;
    el.innerHTML = '';
    detStickBottom = true;
    if (esDet) { try{ esDet.close(); }catch(_){} esDet = null; }

    const updateSlider = () => {
      if (!slider) return;
      const max = el.scrollHeight - el.clientHeight;
      slider.value = max <= 0 ? 100 : Math.round((el.scrollTop / max) * 100);
    };
    const updateStick = () => {
      const pad = 6; // tolerantierandje
      detStickBottom = (el.scrollTop >= (el.scrollHeight - el.clientHeight - pad));

    };

    el.addEventListener('scroll', () => { updateSlider(); updateStick(); }, { passive:true });
    if (slider){
      slider.addEventListener('input', () => {
        const max = el.scrollHeight - el.clientHeight;
        el.scrollTop = Math.round((slider.value/100) * max);
        detStickBottom = (slider.value >= 99);
      });
    }

    esDet = new EventSource('/api/logs/stream?tag=SYNC');
    esDet.onmessage = (ev) => {
      if (!ev?.data) return;
      el.insertAdjacentHTML('beforeend', ev.data + '<br>');
      if (detStickBottom) el.scrollTop = el.scrollHeight;
      updateSlider();
    };

    esDet.onerror = () => { try { esDet?.close(); } catch(_){} esDet = null; };
    requestAnimationFrame(() => { el.scrollTop = el.scrollHeight; updateSlider(); });
  }

  function closeDetailsLog(){ try { esDet?.close(); } catch(_){ } esDet=null; }
  function toggleDetails(){
    const d = document.getElementById('details');
    d.classList.toggle('hidden');
    if (!d.classList.contains('hidden')) openDetailsLog(); else closeDetailsLog();
  }
  window.addEventListener('beforeunload', closeDetailsLog);

  /* ====== Summary copy / download ====== */
  async function copySummary(btn){
    if (!window.currentSummary) {
      try { window.currentSummary = await fetch('/api/run/summary').then(r=>r.json()); }
      catch { flashCopy(btn, false, 'No summary'); return; }
    }
    const s = window.currentSummary;
    if (!s) { flashCopy(btn, false, 'No summary'); return; }

    const lines = [];
    lines.push(`CrossWatch Watchlist Sync ${s.version || ''}`.trim());
    if (s.started_at)   lines.push(`Start:   ${s.started_at}`);
    if (s.finished_at)  lines.push(`Finish:  ${s.finished_at}`);
    if (s.cmd)          lines.push(`Cmd:     ${s.cmd}`);
    if (s.plex_pre != null && s.simkl_pre != null)   lines.push(`Pre:     Plex=${s.plex_pre} vs SIMKL=${s.simkl_pre}`);
    if (s.plex_post != null && s.simkl_post != null) lines.push(`Post:    Plex=${s.plex_post} vs SIMKL=${s.simkl_post} -> ${s.result || 'UNKNOWN'}`);
    if (s.duration_sec != null) lines.push(`Duration: ${s.duration_sec}s`);
    if (s.exit_code != null)    lines.push(`Exit:     ${s.exit_code}`);
    const text = lines.join('\n');

    let ok = false;
    try { await navigator.clipboard.writeText(text); ok = true; } catch(e) { ok = false; }
    if (!ok){
      try{
        const ta = document.createElement('textarea');
        ta.value = text; ta.setAttribute('readonly','');
        ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.focus(); ta.select();
        ok = document.execCommand('copy'); document.body.removeChild(ta);
      }catch(e){ ok = false; }
    }
    flashCopy(btn, ok);
  }
  function downloadSummary(){ window.open('/api/run/summary/file', '_blank'); }

  /* ====== Status refresh (Plex & SIMKL) ====== */
  function setRefreshBusy(busy){
    const btn = document.getElementById('btn-status-refresh');
    if (!btn) return;
    btn.disabled = !!busy;
    btn.classList.toggle('loading', !!busy);
  }
  async function manualRefreshStatus(){
    const btn = document.getElementById('btn-status-refresh');
    try{
      setRefreshBusy(true);
      btn.classList.add('spin');
      setTimeout(()=> btn.classList.remove('spin'), 2000);
      await refreshStatus(true); // forces /api/status?fresh=1
    }catch(e){
      console?.warn('Manual status refresh failed', e);
    }finally{
      setRefreshBusy(false);
    }
  }
  async function refreshStatus(force = false){
    const now = Date.now();
    if (!force && now - lastStatusMs < STATUS_MIN_INTERVAL) return;
    lastStatusMs = now;

    const r = await fetch('/api/status' + (force ? '?fresh=1' : '')).then(r=>r.json());
    appDebug = !!r.debug;

    const pb = document.getElementById('badge-plex');
    const sb = document.getElementById('badge-simkl');
    pb.className = 'badge ' + (r.plex_connected ? 'ok' : 'no');
    pb.innerHTML = `<span class="dot ${r.plex_connected?'ok':'no'}"></span>Plex: ${r.plex_connected?'Connected':'Not connected'}`;
    sb.className = 'badge ' + (r.simkl_connected ? 'ok' : 'no');
    sb.innerHTML = `<span class="dot ${r.simkl_connected?'ok':'no'}"></span>SIMKL: ${r.simkl_connected?'Connected':'Not connected'}`;

    window._ui.status = { can_run: !!r.can_run, plex_connected: !!r.plex_connected, simkl_connected: !!r.simkl_connected };
    recomputeRunDisabled();

    const onMain = !document.getElementById('ops-card').classList.contains('hidden');
    const logPanel = document.getElementById('log-panel');
    const layout   = document.getElementById('layout');
    const stats    = document.getElementById('stats-card');
    const hasStatsVisible = !!(stats && !stats.classList.contains('hidden'));

    logPanel.classList.toggle('hidden', !(appDebug && onMain));
    layout.classList.toggle('full', onMain && !appDebug && !hasStatsVisible);
  }

  /* ====== Config & Settings ====== */
  async function loadConfig(){
    const cfg = await fetch('/api/config', {cache:'no-store'}).then(r=>r.json());

    // Sync Options
    document.getElementById('mode').value   = cfg.sync?.bidirectional?.mode || 'two-way';
    document.getElementById('source').value = cfg.sync?.bidirectional?.source_of_truth || 'plex';

    // Troubleshoot / Runtime
    document.getElementById('debug').value  = String(!!cfg.runtime?.debug);

    // Auth / Keys
    document.getElementById('plex_token').value           = cfg.plex?.account_token || '';
    document.getElementById('simkl_client_id').value      = cfg.simkl?.client_id || '';
    document.getElementById('simkl_client_secret').value  = cfg.simkl?.client_secret || '';
    document.getElementById('simkl_access_token').value   = cfg.simkl?.access_token || '';
    document.getElementById('tmdb_api_key').value         = cfg.tmdb?.api_key || '';

    // Scheduling (pull from /api/config so the same source drives the UI)
    const s = cfg.scheduling || {};
    document.getElementById('schEnabled').value = String(!!s.enabled);
    document.getElementById('schMode').value    = (typeof s.mode === 'string' && s.mode) ? s.mode : 'hourly';
    document.getElementById('schN').value       = Number.isFinite(s.every_n_hours) ? String(s.every_n_hours) : '2';
    document.getElementById('schTime').value    = (typeof s.daily_time === 'string' && s.daily_time) ? s.daily_time : '03:30';
    // Optional timezone field if you add an <input id="schTz">
    if (document.getElementById('schTz')) document.getElementById('schTz').value = s.timezone || '';

    // keep your existing hints
    updateSimklButtonState?.(); updateSimklHint?.(); updateTmdbHint?.();
  }

  // Save settings back to server
  async function saveSettings(){
    const toast = document.getElementById('save_msg');
    const showToast = (text, ok=true) => {
      if (!toast) return;
      toast.classList.remove('hidden','ok','warn');
      toast.classList.add(ok ? 'ok' : 'warn');
      toast.textContent = text;
      // keep it visible long enough to read
      setTimeout(() => toast.classList.add('hidden'), 2000);
    };

    try {
      // 1) Pull current server config and clone
      const serverResp = await fetch('/api/config');
      if (!serverResp.ok) throw new Error(`GET /api/config ${serverResp.status}`);
      const serverCfg = await serverResp.json();
      const cfg = (typeof structuredClone === 'function')
        ? structuredClone(serverCfg)
        : JSON.parse(JSON.stringify(serverCfg || {}));

      let changed = false;

      // --- SYNC ---
      const uiMode   = document.getElementById('mode').value;
      const uiSource = document.getElementById('source').value;
      const prevMode   = serverCfg?.sync?.bidirectional?.mode || 'two-way';
      const prevSource = serverCfg?.sync?.bidirectional?.source_of_truth || 'plex';
      if (uiMode !== prevMode) {
        cfg.sync = cfg.sync || {}; cfg.sync.bidirectional = cfg.sync.bidirectional || {};
        cfg.sync.bidirectional.mode = uiMode; changed = true;
      }
      if (uiSource !== prevSource) {
        cfg.sync = cfg.sync || {}; cfg.sync.bidirectional = cfg.sync.bidirectional || {};
        cfg.sync.bidirectional.source_of_truth = uiSource; changed = true;
      }

      // --- RUNTIME ---
      const uiDebug = (document.getElementById('debug').value === 'true');
      const prevDebug = !!serverCfg?.runtime?.debug;
      if (uiDebug !== prevDebug) {
        cfg.runtime = cfg.runtime || {};
        cfg.runtime.debug = uiDebug; changed = true;
      }

      // --- PLEX ---
      const uiPlexToken = (document.getElementById('plex_token').value || '').trim();
      const prevPlexTok = serverCfg?.plex?.account_token || '';
      if (uiPlexToken && uiPlexToken !== prevPlexTok) {
        cfg.plex = cfg.plex || {};
        cfg.plex.account_token = uiPlexToken; changed = true;
      }

      // --- SIMKL ---
      const uiCid = (document.getElementById('simkl_client_id').value || '').trim();
      const uiSec = (document.getElementById('simkl_client_secret').value || '').trim();
      const prevCid = serverCfg?.simkl?.client_id || '';
      const prevSec = serverCfg?.simkl?.client_secret || '';
      if (uiCid && uiCid !== prevCid) { cfg.simkl = cfg.simkl || {}; cfg.simkl.client_id = uiCid; changed = true; }
      if (uiSec && uiSec !== prevSec) { cfg.simkl = cfg.simkl || {}; cfg.simkl.client_secret = uiSec; changed = true; }

      // --- TMDb ---
      const uiTmdb = (document.getElementById('tmdb_api_key').value || '').trim();
      const prevTmdb = serverCfg?.tmdb?.api_key || '';
      if (uiTmdb && uiTmdb !== prevTmdb) { cfg.tmdb = cfg.tmdb || {}; cfg.tmdb.api_key = uiTmdb; changed = true; }

      // 2) Save updated config (only if changed)
      if (changed) {
        const postCfg = await fetch('/api/config', {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify(cfg)
        });
        if (!postCfg.ok) throw new Error(`POST /api/config ${postCfg.status}`);
      }

      // 3) Save scheduling 
      try {
        const schPayload = {
          enabled: document.getElementById('schEnabled').value === 'true',
          mode: document.getElementById('schMode').value,
          every_n_hours: parseInt(document.getElementById('schN').value || '2', 10),
          daily_time: document.getElementById('schTime').value || '03:30',
          timezone: (document.getElementById('schTz')?.value || '').trim() || undefined
        };
        const postSch = await fetch('/api/scheduling', {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify(schPayload)
        });
        if (!postSch.ok) throw new Error(`POST /api/scheduling ${postSch.status}`);
      } catch (e) {
        console.warn('saveSettings: scheduling failed', e);
      }

      // 4) Refresh UI pieces
      try { await refreshStatus(true); } catch {}
      try { updateTmdbHint?.(); } catch {}
      try { updateSimklState?.(); } catch {}
      try { await updateWatchlistTabVisibility?.(); } catch {}
      try { await loadScheduling?.(); } catch {}

      // 5) Success toast
      showToast('Settings saved ✓', true);

    } catch (err) {
      console.error('saveSettings failed', err);
      showToast('Save failed — see console', false);
    }
  }

  // ---- Scheduling UI ----
  async function loadScheduling(){
    try{
      const res = await fetch('/api/scheduling', { cache: 'no-store' });
      const s = await res.json();

      // Debug log so we can verify what the UI received
      console.debug('[UI] /api/scheduling ->', s);

      const en = document.getElementById('schEnabled');
      const mo = document.getElementById('schMode');
      const nh = document.getElementById('schN');
      const ti = document.getElementById('schTime');

      if (!en || !mo || !nh || !ti) {
        console.warn('[UI] scheduling controls not found in DOM');
        return;
      }

      // Map JSON -> controls (strings for <select> values)
      const valEnabled = s && s.enabled === true ? 'true' : 'false';
      const valMode    = (s && typeof s.mode === 'string' && s.mode) ? s.mode : 'hourly';
      const valN       = (s && Number.isFinite(s.every_n_hours)) ? String(s.every_n_hours) : '2';
      const valTime    = (s && typeof s.daily_time === 'string' && s.daily_time) ? s.daily_time : '03:30';

      // Update controls
      en.value = valEnabled;
      mo.value = valMode;
      nh.value = valN;
      ti.value = valTime;

      // nudge browser to update UI state
      en.dispatchEvent(new Event('change'));
      mo.dispatchEvent(new Event('change'));
      nh.dispatchEvent(new Event('change'));
      ti.dispatchEvent(new Event('change'));

    } catch (e) {
      console.warn('Failed to load scheduling config', e);
    }
    refreshSchedulingBanner();
  }


  async function saveScheduling(){
    const payload = {
      enabled: document.getElementById('schEnabled').value === 'true',
      mode: document.getElementById('schMode').value,
      every_n_hours: parseInt(document.getElementById('schN').value || '2', 10),
      daily_time: document.getElementById('schTime').value || '03:30'
    };
    const r = await fetch('/api/scheduling', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
    const j = await r.json().catch(()=>({}));
    const m = document.getElementById('schStatus'); m.classList.remove('hidden'); m.textContent = j.ok ? 'Saved ✓' : 'Error';
    setTimeout(()=>m.classList.add('hidden'), 1500);
    refreshSchedulingBanner();
  }
  function refreshSchedulingBanner(){
    fetch('/api/scheduling/status')
      .then(r => r.json())
      .then(j => {
        const span = document.getElementById('sched-inline');
        if (!span) return;
        if (j && j.config && j.config.enabled) {
          const nextRun = j.next_run_at ? new Date(j.next_run_at*1000).toLocaleString() : '—';
          span.textContent = `—   Scheduler running (next ${nextRun})`;
          span.style.display = 'inline';
        } else {
          span.textContent = '';
          span.style.display = 'none';
        }
      })
      .catch(() => {
        const span = document.getElementById('sched-inline');
        if (span){ span.textContent = ''; span.style.display = 'none'; }
      });
  }

  /* Troubleshooting actions */
  async function clearState(){
    const btnText = "Clear State";
    try{
      const r = await fetch('/api/troubleshoot/reset-state', {method:'POST'});
      const j = await r.json();
      const m = document.getElementById('tb_msg');
      m.classList.remove('hidden'); m.textContent = j.ok ? (btnText + ' – started ✓') : (btnText + ' – failed');
      setTimeout(()=>m.classList.add('hidden'), 1600);
    }catch(_){}
  }
  async function clearCache(){
    const btnText = "Clear Cache";
    try{
      const r = await fetch('/api/troubleshoot/clear-cache', {method:'POST'});
      const j = await r.json();
      const m = document.getElementById('tb_msg');
      m.classList.remove('hidden'); m.textContent = j.ok ? (btnText + ' – done ✓') : (btnText + ' – failed');
      setTimeout(()=>m.classList.add('hidden'), 1600);
    }catch(_){}
  }

  async function resetStats(){
    const btnText = "Reset Statistics";
    try{
      const r = await fetch('/api/troubleshoot/reset-stats', { method:'POST' });
      const j = await r.json();
      const m = document.getElementById('tb_msg');
      m.classList.remove('hidden');
      m.textContent = j.ok ? (btnText + ' – done ✓') : (btnText + ' – failed' + (j.error ? ` (${j.error})` : ''));
      setTimeout(()=>m.classList.add('hidden'), 2200);

      if (j.ok && typeof refreshStats === 'function') refreshStats(true);
    }catch(e){
      const m = document.getElementById('tb_msg');
      m.classList.remove('hidden');
      m.textContent = btnText + ' – failed (network)';
      setTimeout(()=>m.classList.add('hidden'), 2200);
    }
  }

  /* TMDb hint logic (Settings page only) */
  async function updateTmdbHint(){
    const hint  = document.getElementById('tmdb_hint');
    const input = document.getElementById('tmdb_api_key');
    if (!hint || !input) return;
    const settingsVisible = !document.getElementById('page-settings')?.classList.contains('hidden');
    if (!settingsVisible) return;
    const v = (input.value || '').trim();
    if (document.activeElement === input) input.dataset.dirty = '1';
    if (input.dataset.dirty === '1'){ hint.classList.toggle('hidden', !!v); return; }
    if (v){ hint.classList.add('hidden'); return; }
    try {
      const cfg = await fetch('/api/config', { cache: 'no-store' }).then(r => r.json());
      const has = !!((cfg.tmdb?.api_key || '').trim());
      hint.classList.toggle('hidden', has);
    } catch { hint.classList.remove('hidden'); }
  }

  /* Plex auth (PIN flow) */
  function setPlexSuccess(show){
    document.getElementById('plex_msg').classList.toggle('hidden', !show);
  }

  async function requestPlexPin(){
    setPlexSuccess(false);
    const r = await fetch('/api/plex/pin/new', {method:'POST'});
    const j = await r.json();
    if(!j.ok){ return; }

    document.getElementById('plex_pin').value = j.code;
    try{ await navigator.clipboard.writeText(j.code); }catch(_){}
    window.open('https://plex.tv/link', '_blank');

    if(plexPoll) { clearInterval(plexPoll); }
    let ticks = 0;

    plexPoll = setInterval(async ()=>{
      ticks++;
      const cfg = await fetch('/api/config', { cache: 'no-store' })
        .then(r=>r.json()).catch(()=>null);
      const tok = cfg?.plex?.account_token || '';

      const el = document.getElementById('plex_token');
      if (tok && el && el.value !== tok) {
        // Polling Current token until it changes
        el.value = tok;
        setPlexSuccess(true);
        await refreshStatus();
        clearInterval(plexPoll);
      }

      if(ticks > 360){ clearInterval(plexPoll); }
    }, 1000);
  }

  /* SIMKL auth */
  function setSimklSuccess(show){ document.getElementById('simkl_msg').classList.toggle('hidden', !show); }
  function isPlaceholder(v, ph){ return (v||'').trim().toUpperCase() === ph.toUpperCase(); }
  function updateSimklButtonState(){
    const cid = (document.getElementById('simkl_client_id').value || '').trim();
    const sec = (document.getElementById('simkl_client_secret').value || '').trim();
    const badCid = (!cid) || isPlaceholder(cid, 'YOUR_SIMKL_CLIENT_ID');
    const badSec = (!sec) || isPlaceholder(sec, 'YOUR_SIMKL_CLIENT_SECRET');
    const btn = document.getElementById('simkl_start_btn'); const hint = document.getElementById('simkl_hint');
    document.getElementById('redirect_uri_preview').textContent = computeRedirectURI();
    const ok = !(badCid || badSec); btn.disabled = !ok; hint.classList.toggle('hidden', ok);
  }
  async function copyRedirect(){ try{ await navigator.clipboard.writeText(computeRedirectURI()); }catch(_){} }
  async function startSimkl(){
    setSimklSuccess(false); await saveSettings();
    const origin = window.location.origin;
    const r = await fetch('/api/simkl/authorize', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({origin})});
    const j = await r.json(); if(!j.ok){ return; }
    window.open(j.authorize_url, '_blank');
    if(simklPoll) { clearInterval(simklPoll); }
    let ticks = 0;
    simklPoll = setInterval(async ()=>{
      ticks++;
      const cfg = await fetch('/api/config').then(r=>r.json()).catch(()=>null);
      const tok = cfg?.simkl?.access_token || '';
      if(tok){
        document.getElementById('simkl_access_token').value = tok; setSimklSuccess(true); await refreshStatus(); clearInterval(simklPoll);
      }
      if(ticks > 600){ clearInterval(simklPoll); }
    }, 1000);
  }
  // triggered by a real click only
  async function copyInputValue(inputId, btnEl) {
    const el = document.getElementById(inputId);
    const val = el?.value?.trim() || "";
    if (!val) return;

    try {
      // Modern, secure-context path (localhost/127.0.0.1 is ok in moderne browsers)
      await navigator.clipboard.writeText(val);
      flashBtnOK(btnEl);
    } catch (e) {
      // Fallback for older/locked-down contexts
      const ta = document.createElement('textarea');
      ta.value = val;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); flashBtnOK(btnEl); } catch(_) {}
      document.body.removeChild(ta);
    }
  }

  function flashBtnOK(btnEl){
    if (!btnEl) return;
    btnEl.disabled = true;
    btnEl.classList.add('copied');      // optional style hook
    setTimeout(() => { 
      btnEl.classList.remove('copied'); 
      btnEl.disabled = false; 
    }, 700);
  }

  // Wire up buttons once the DOM is ready (no layout changes)
  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('btn-copy-plex-pin')
      ?.addEventListener('click', (e) => copyInputValue('plex_pin', e.currentTarget));
    document.getElementById('btn-copy-plex-token')
      ?.addEventListener('click', (e) => copyInputValue('plex_token', e.currentTarget));
  });

  /* ====== Poster carousel helpers ====== */
  function updateEdges(){
    const row = document.getElementById('poster-row');
    const L = document.getElementById('edgeL'), R = document.getElementById('edgeR');
    const max = row.scrollWidth - row.clientWidth - 1;
    L.classList.toggle('hide', row.scrollLeft <= 0);
    R.classList.toggle('hide', row.scrollLeft >= max);
  }
  function scrollWall(dir){
    const row = document.getElementById('poster-row');
    const step = row.clientWidth;
    row.scrollBy({ left: dir * step, behavior: 'smooth' });
    setTimeout(updateEdges, 350);
  }
  function initWallInteractions(){
    const row = document.getElementById('poster-row');
    row.addEventListener('scroll', updateEdges);
    row.addEventListener('wheel', (e)=>{
      if(Math.abs(e.deltaY) > Math.abs(e.deltaX)){
        e.preventDefault(); row.scrollBy({left:e.deltaY, behavior:'auto'});
      }
    }, {passive:false});
    updateEdges();
  }

  /* ====== Watchlist (grid) ====== */
  function artUrl(item, size){
    const typ  = (item.type === 'tv' || item.type === 'show') ? 'tv' : 'movie';
    const tmdb = item.tmdb;
    if(!tmdb) return null;
    const cb = window._lastSyncEpoch || 0;
    return `/art/tmdb/${typ}/${tmdb}?size=${encodeURIComponent(size || 'w342')}&cb=${cb}`;
  }

  async function loadWall() {
    const card = document.getElementById('placeholder-card');
    const msg = document.getElementById('wall-msg');
    const row = document.getElementById('poster-row');
    msg.textContent = 'Loading…'; row.innerHTML = ''; row.classList.add('hidden'); card.classList.remove('hidden');

    const hiddenMap = new Map(
      (JSON.parse(localStorage.getItem('wl_hidden') || '[]') || []).map(k => [k, true])
    );
    const isLocallyHidden = (k) => hiddenMap.has(k);

    const isDeleted = (item) => {
      if (isLocallyHidden(item.key) && item.status === 'deleted') return true;
      if (isLocallyHidden(item.key) && item.status !== 'deleted') {
        hiddenMap.delete(item.key);
        localStorage.setItem('wl_hidden', JSON.stringify([...hiddenMap.keys()]));
      }
      return (window._deletedKeys && window._deletedKeys.has(item.key)) || false;
    };

    try {
      const data = await fetch('/api/state/wall').then(r => r.json());
      if (data.missing_tmdb_key) { card.classList.add('hidden'); return; }
      if (!data.ok) { msg.textContent = data.error || 'No state data found.'; return; }
      let items = data.items || [];
      _lastSyncEpoch = data.last_sync_epoch || null;
      if (items.length === 0) { msg.textContent = 'No items to show yet.'; return; }
      msg.classList.add('hidden'); row.classList.remove('hidden');

      const firstSeen = (() => { try { return JSON.parse(localStorage.getItem('wl_first_seen') || '{}'); } catch { return {}; } })();
      const getTs = (it) => {
        const s =
          it.added_epoch ?? it.added_ts ?? it.created_ts ?? it.created ?? it.epoch ?? null;
        return Number(s || firstSeen[it.key] || 0);
      };

      const now = Date.now();
      for (const it of items) {
        if (!firstSeen[it.key]) firstSeen[it.key] = now;
      }
      localStorage.setItem('wl_first_seen', JSON.stringify(firstSeen));

      items = items.slice().sort((a, b) => getTs(b) - getTs(a));

      for (const it of items) {
        if (!it.tmdb) continue;
        const a = document.createElement('a');
        a.className = 'poster';
        a.href = `https://www.themoviedb.org/${it.type}/${it.tmdb}`; a.target = '_blank'; a.rel = 'noopener';
        a.dataset.type = it.type; a.dataset.tmdb = String(it.tmdb); a.dataset.key = it.key || '';
        const uiStatus = isDeleted(it) ? 'deleted' : it.status; a.dataset.source = uiStatus;

        const img = document.createElement('img');
        img.loading = 'lazy'; img.alt = `${it.title || ''} (${it.year || ''})`; img.src = artUrl(it, 'w342'); a.appendChild(img);

        const ovr = document.createElement('div'); ovr.className = 'ovr';
        let pillText, pillClass;
        if (uiStatus === 'deleted')      { pillText = 'DELETED';  pillClass = 'p-del'; }
        else if (uiStatus === 'both')    { pillText = 'SYNCED';   pillClass = 'p-syn'; }
        else if (uiStatus === 'plex_only'){ pillText = 'PLEX';     pillClass = 'p-px'; }
        else                              { pillText = 'SIMKL';    pillClass = 'p-sk'; }
        const pill = document.createElement('div'); pill.className = 'pill ' + pillClass; pill.textContent = pillText; ovr.appendChild(pill); a.appendChild(ovr);

        const cap = document.createElement('div'); cap.className = 'cap'; cap.textContent = `${it.title || ''} ${it.year ? '· ' + it.year : ''}`; a.appendChild(cap);

        const hover = document.createElement('div'); hover.className = 'hover';
        hover.innerHTML = `
          <div class="titleline">${it.title || ''}</div>
          <div class="meta">
            <div class="chip src">${uiStatus === 'deleted' ? 'Status: Deleted' : (uiStatus === 'both' ? 'Source: Synced' : (uiStatus === 'plex_only' ? 'Source: Plex' : 'Source: SIMKL'))}</div>
            <div class="chip time" id="time-${it.type}-${it.tmdb}">${_lastSyncEpoch ? ('updated ' + relTimeFromEpoch(_lastSyncEpoch)) : ''}</div>
          </div>
          <div class="desc" id="desc-${it.type}-${it.tmdb}">Fetching description…</div>
        `;
        a.appendChild(hover);

        a.addEventListener('mouseenter', async () => {
          const descEl = document.getElementById(`desc-${it.type}-${it.tmdb}`);
          if (!descEl || descEl.dataset.loaded) return;
          try {
            const cb = window._lastSyncEpoch || 0;
            const meta = await fetch(`/api/tmdb/meta/${it.type}/${it.tmdb}?cb=${cb}`).then(r => r.json());
            descEl.textContent = meta?.overview || '—';
            descEl.dataset.loaded = '1';
          } catch { descEl.textContent = '—'; descEl.dataset.loaded = '1'; }
        }, { passive: true });

        row.appendChild(a);
      }
      initWallInteractions();
    } catch { msg.textContent = 'Failed to load preview.'; }
  }


  async function loadWatchlist() {
    const grid = document.getElementById('wl-grid');
    const msg = document.getElementById('wl-msg');
    grid.innerHTML = ''; grid.classList.add('hidden'); msg.textContent = 'Loading…'; msg.classList.remove('hidden');
    try {
      const data = await fetch('/api/watchlist').then(r => r.json());
      if (data.missing_tmdb_key) { msg.textContent = 'Set a TMDb API key to see posters.'; return; }
      if (!data.ok) { msg.textContent = data.error || 'No state data found.'; return; }
      const items = data.items || [];
      if (items.length === 0) { msg.textContent = 'No items on your watchlist yet.'; return; }
      msg.classList.add('hidden'); grid.classList.remove('hidden');
      for (const it of items) {
        if (!it.tmdb) continue;
        const node = document.createElement('div');
        node.className = 'wl-poster poster';
        node.dataset.key = it.key;
        node.dataset.type = it.type === 'tv' || it.type === 'show' ? 'tv' : 'movie';
        node.dataset.tmdb = String(it.tmdb || '');
        node.dataset.status = it.status;
        const pillText = it.status === 'both' ? 'SYNCED' : (it.status === 'plex_only' ? 'PLEX' : 'SIMKL');
        const pillClass = it.status === 'both' ? 'p-syn' : (it.status === 'plex_only' ? 'p-px' : 'p-sk');
        
        node.innerHTML = `
          <img alt="" src="${artUrl(it, 'w342') || ''}" onerror="this.style.display='none'">
          <button class="wl-del icon-btn trash"
                  type="button"
                  title="Remove from Plex watchlist"
                  aria-label="Remove from Plex watchlist"
                  onclick="deletePoster(event, '${encodeURIComponent(it.key)}', this)">
            <svg class="ico" viewBox="0 0 24 24" aria-hidden="true">
              <path class="lid" d="M9 4h6l1 2H8l1-2z"/>
              <path d="M6 7h12l-1 13H7L6 7z"/>
              <path d="M10 11v6M14 11v6"/>
            </svg>
          </button>

          <div class="wl-ovr ovr"><span class="pill ${pillClass}">${pillText}</span></div>
          <div class="wl-cap cap">${(it.title || '').replace(/"/g, '&quot;')} ${it.year ? '· ' + it.year : ''}</div>
          <div class="wl-hover hover">
            <div class="titleline">${(it.title || '')}</div>
            <div class="meta">
              <div class="chip src">${it.status === 'both' ? 'Source: Synced' : (it.status === 'plex_only' ? 'Source: Plex' : 'Source: SIMKL')}</div>
              <div class="chip time">${relTimeFromEpoch(it.added_epoch)}</div>
            </div>
            <div class="desc" id="wldesc-${node.dataset.type}-${node.dataset.tmdb}">${it.tmdb ? 'Fetching description…' : '—'}</div>
          </div>`;

        const hidden = new Set(JSON.parse(localStorage.getItem('wl_hidden') || '[]'));
        if (hidden.has(it.key)) {
          const pill = node.querySelector('.pill');
          // Instead of forcing DELETED, mark visually
          pill.classList.add('p-del');
          // keep original text (SYNCED, PLEX, SIMKL, …)
          // or optionally append marker
          // pill.textContent = pill.textContent + ' (hidden)';
        }

        node.addEventListener('mouseenter', async () => {
          const descEl = document.getElementById(`wldesc-${it.type}-${it.tmdb}`);
          if (!descEl || descEl.dataset.loaded) return;
          try {
            const cb = window._lastSyncEpoch || Date.now();
            const meta = await fetch(`/api/tmdb/meta/${it.type}/${it.tmdb}?cb=${cb}`).then(r => r.json());
            descEl.textContent = meta?.overview || '—';
            descEl.dataset.loaded = '1';
          } catch { descEl.textContent = '—'; descEl.dataset.loaded = '1'; }
        }, { passive: true });

        grid.appendChild(node);
      }
    } catch (error) {
      console.error('Error loading watchlist:', error);
      msg.textContent = 'Failed to load preview.';
    }
  }

  // Delete a watchlist item (from Plex)
  async function deletePoster(ev, encKey, btnEl) {
    ev?.stopPropagation?.();
    const key  = decodeURIComponent(encKey);
    const card = btnEl.closest('.wl-poster');

    // visual state
    btnEl.disabled = true;
    btnEl.classList.remove('done','error');
    btnEl.classList.add('working');

    try {
      const res = await fetch('/api/watchlist/' + encodeURIComponent(key), { method: 'DELETE' });
      if (!res.ok) throw new Error('HTTP ' + res.status);

      // fade out and remove
      if (card) {
        card.classList.add('wl-removing');
        setTimeout(() => { card.remove(); }, 350);
      }

      // persist hidden key (your existing behavior)
      const hidden = new Set(JSON.parse(localStorage.getItem('wl_hidden') || '[]'));
      hidden.add(key);
      localStorage.setItem('wl_hidden', JSON.stringify([...hidden]));
      window.dispatchEvent(new Event('storage'));

      btnEl.classList.remove('working');
      btnEl.classList.add('done');
    } catch (e) {
      console.warn('deletePoster error', e);
      btnEl.classList.remove('working');
      btnEl.classList.add('error');
      setTimeout(() => btnEl.classList.remove('error'), 1200);
    } finally {
      setTimeout(() => { btnEl.disabled = false; }, 600);
    }
  }

  /* ====== Watchlist preview visibility ====== */
  async function updateWatchlistPreview(){ await loadWatchlist(); }
  async function updateWatchlistTabVisibility(){
    try {
      const cfg = await fetch('/api/config').then(r=>r.json());
      const tmdbKey = (cfg.tmdb?.api_key || '').trim();
      document.getElementById('tab-watchlist').style.display = tmdbKey ? 'block' : 'none';
    } catch(e){ document.getElementById('tab-watchlist').style.display = 'none'; }
  }
  async function hasTmdbKey(){
    try{ const cfg = await fetch('/api/config').then(r=>r.json()); return !!(cfg.tmdb?.api_key || '').trim(); }catch(_){ return false; }
  }
  function isOnMain(){ return !document.getElementById('ops-card').classList.contains('hidden'); }
  async function updatePreviewVisibility(){
    const card = document.getElementById('placeholder-card');
    const row  = document.getElementById('poster-row');
    if (!isOnMain()) { card.classList.add('hidden'); return false; }
    const show = await hasTmdbKey();
    if(!show){
      card.classList.add('hidden');
      if(row){ row.innerHTML = ''; row.classList.add('hidden'); }
      window.wallLoaded = false; return false;
    } else {
      card.classList.remove('hidden');
      if(!window.wallLoaded){ await loadWall(); window.wallLoaded = true; }
      return true;
    }
  }

  /* ====== Boot ====== */
  showTab('main');
  updateWatchlistTabVisibility();
  window.addEventListener('storage', (event) => { if (event.key === 'wl_hidden') { loadWatchlist(); } });

