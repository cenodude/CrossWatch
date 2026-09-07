/* assets/js/logs.js */
/* CrossWatch - Live and saved diagnostic logs */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const date = ts => ts ? new Date(ts * 1000).toLocaleString() : 'Live';
export function highlight(text, query) {
  if (!query) return esc(text);
  const lower = text.toLowerCase(), needle = query.toLowerCase();
  let pos = 0, result = '', found;
  while ((found = lower.indexOf(needle, pos)) !== -1) {
    result += esc(text.slice(pos, found)) + '<mark>' + esc(text.slice(found, found + query.length)) + '</mark>';
    pos = found + query.length;
  }
  return result + esc(text.slice(pos));
}

let dispose = null, root = null;
const LogsPage = {
  async mount(host) {
    dispose?.(); root = host;
    if (!host) return;
    if (!document.getElementById('cw-logs-css')) {
      const css = document.createElement('link'); css.id = 'cw-logs-css'; css.rel = 'stylesheet';
      css.href = `/assets/css/logs.css?v=${encodeURIComponent(window.APP_VERSION || '1')}`; document.head.append(css);
    }
    const route = new URLSearchParams(location.hash.split('?')[1] || '');
    let channel = ['sync','watcher','debug'].includes(route.get('channel')) ? route.get('channel') : 'sync';
    let pairId = route.get('pairId') || '', latestRequested = route.get('latest') === '1', pairs = [], latestRunId = '';
    if (channel === 'watcher') pairId = '';
    let runId = route.get('runId') || '', mode = runId || latestRequested ? 'saved' : 'live', sid = '', sessions = [], current = null;
    let alive = true, busy = false, sequence = 0, timer, searchTimer, offset = 0, total = 0, follow = mode === 'live', rows = [], matchIndex = -1;
    let query = '', level = '', provider = '', listPage = 0, showingRuns = mode === 'saved', controller = null, pendingMatch = null;
    const profile = window.CW?.OverviewProfile?.id || '';
    host.innerHTML = `<div class="logs-page"><a href="#main" class="logs-back">${icon('arrow_back')}Main</a>
      <header class="logs-heading"><div><div class="logs-eyebrow">DIAGNOSTICS</div><h1>Logs</h1></div><span data-connection role="status">Connecting…</span></header>
      <div class="logs-toolbar"><div class="logs-tabs" role="tablist" aria-label="Log channel">${['sync','watcher','debug'].map(c=>`<button data-channel="${c}" role="tab">${c[0].toUpperCase()+c.slice(1)}</button>`).join('')}</div><div class="logs-tabs" role="tablist" aria-label="Log source"><button data-mode="live" role="tab">Live</button><button data-mode="saved" role="tab">Saved logs</button></div><div class="logs-pair-controls" data-pair-controls><select data-pair aria-label="Sync pair"><option value="">All pairs</option></select><button data-latest title="Open the latest run for this selection">${icon('history')}Latest run</button></div><span data-storage></span></div>
      <div class="logs-toolbar" data-filters><label class="logs-search">${icon('search')}<input data-query type="search" placeholder="Search logs…" aria-label="Search logs"></label><select data-level aria-label="Log level"><option value="">All levels</option><option value="ERROR">Errors</option><option value="WARN">Warnings</option><option value="INFO">Info</option><option value="DEBUG">Debug</option></select><input data-provider placeholder="Provider" aria-label="Filter by provider" list="log-providers"><datalist id="log-providers"></datalist><button data-prev-match title="Previous match" aria-label="Previous match">${icon('keyboard_arrow_up')}</button><button data-next-match title="Next match" aria-label="Next match">${icon('keyboard_arrow_down')}</button><button data-follow>${icon('podcasts')}Follow live</button>
      <details class="logs-options"><summary>More</summary><div><label><input data-wrap type="checkbox" checked>Wrap lines</label><button data-copy>Copy filtered log</button><button data-copy-full>Copy full log</button><button data-download>Download filtered log</button><button data-download-full>Download full log</button></div></details></div>
      <div class="logs-toolbar logs-runbar"><button data-back-runs hidden>${icon('arrow_back')}Saved logs</button><strong data-title></strong><button data-errors class="logs-error"></button><button data-warnings class="logs-warn"></button><button data-pin hidden>${icon('push_pin')}Keep this log</button><button data-delete hidden>${icon('delete')}Delete</button><button data-clear hidden>Clear older logs</button></div>
      <p class="logs-notice" data-notice role="status" hidden></p><div class="logs-content" tabindex="0" aria-label="Log output"><div data-output></div></div>
      <footer class="logs-footer"><span data-count></span><button data-previous>Previous</button><span data-page></span><button data-next>Next</button></footer></div>`;
    const $ = s => host.querySelector(s);
    const content = $('.logs-content');
    function resize() { const page=$('.logs-page'); if(alive && page)page.style.height = `${Math.max(260, window.innerHeight - host.getBoundingClientRect().top - 16)}px`; }
    window.addEventListener('resize', resize); resize();
    function url(path, values = {}) { const p = new URLSearchParams({...values, user_profile:profile}); return '/api/logs/archive' + path + '?' + p; }
    const scope = () => ({pair_id:pairId, run_id:runId});
    function writeRoute() {
      const params = new URLSearchParams({channel});
      if(pairId)params.set('pairId',pairId);
      if(runId)params.set('runId',runId);
      if(latestRequested)params.set('latest','1');
      window.history.replaceState(null,'','#logs?'+params);
    }
    function clearFilters() {
      query=level=provider='';
      for(const selector of ['[data-query]','[data-level]','[data-provider]'])$(selector).value='';
      $('[data-level]').dispatchEvent(new Event('change',{bubbles:true}));
    }
    async function api(path, values = {}, options = {}) {
      const res = await fetch(url(path, values), {cache:'no-store', signal:controller?.signal, ...options});
      if (!res.ok) { let message = 'Could not load logs.'; try { message = (await res.json()).detail || message; } catch {} throw new Error(message); }
      return res.json();
    }
    function notice(text = '') { $('[data-notice]').textContent = text; $('[data-notice]').hidden = !text; }
    function revealMatch(index) {
      matchIndex = Math.max(0, Math.min(rows.length - 1, index));
      host.querySelectorAll('.logs-current-match').forEach(el=>el.classList.remove('logs-current-match'));
      const line = host.querySelector(`[data-line="${rows[matchIndex]?.id}"]`);
      line?.classList.add('logs-current-match');
      line?.scrollIntoView({block:'center'});
    }
    function paintControls() {
      host.querySelectorAll('[data-channel]').forEach(b=>b.setAttribute('aria-selected',String(b.dataset.channel===channel)));
      host.querySelectorAll('[data-mode]').forEach(b=>b.setAttribute('aria-selected',String(b.dataset.mode===mode)));
      $('[data-follow]').textContent = follow ? 'Following live' : 'Resume live';
      $('[data-follow]').hidden = mode !== 'live';
      $('[data-filters]').hidden = showingRuns;
      $('[data-back-runs]').hidden = showingRuns || mode === 'live';
      $('[data-clear]').hidden = !showingRuns || !!pairId;
      $('[data-pair-controls]').hidden = channel === 'watcher';
      $('[data-latest]').disabled = !latestRunId;
      $('[data-pin]').hidden = showingRuns || !current;
      $('[data-delete]').hidden = showingRuns || !current || current.ended === null;
      $('[data-pin]').textContent = current?.pinned ? 'Unpin log' : 'Keep this log';
      $('[data-pin]').title = 'Keep the entire saved log, including any other pairs in it';
      $('[data-errors]').hidden = showingRuns || !current;
      $('[data-warnings]').hidden = showingRuns || !current;
      $('[data-errors]').textContent = `${current?.errors || 0} errors`;
      $('[data-warnings]').textContent = `${current?.warnings || 0} warnings`;
      const pairLabel = pairs.find(p=>p.id===pairId)?.label;
      $('[data-title]').textContent = showingRuns ? 'Saved logs' : current ? `${pairLabel || current.label} · ${date(current.started)}` : 'No log selected';
      $('[data-count]').textContent = showingRuns ? `${sessions.length} saved logs` : `${total} matching lines`;
      $('[data-page]').textContent = showingRuns ? `${listPage+1} / ${Math.max(1,Math.ceil(sessions.length/10))}` : `${Math.floor(offset/500)+1} / ${Math.max(1,Math.ceil(total/500))}`;
      $('[data-previous]').disabled = showingRuns ? listPage === 0 : offset === 0;
      $('[data-next]').disabled = showingRuns ? (listPage+1)*10 >= sessions.length : offset+500 >= total;
      $('[data-prev-match]').disabled = $('[data-next-match]').disabled = !query || !total;
    }
    function renderRuns() {
      listPage = Math.min(listPage, Math.max(0, Math.ceil(sessions.length / 10) - 1));
      $('[data-output]').innerHTML = sessions.slice(listPage*10,listPage*10+10).map(s=>`<button class="logs-saved-row" data-session="${esc(s.id)}"><span>${icon(s.pinned ? 'push_pin' : 'description')}<strong>${esc(s.label)}</strong></span><span>${esc(date(s.started))} · ${s.ended ? Math.max(0,Math.round(s.ended-s.started))+'s' : 'Live'} · ${esc(s.status)}</span><span>${s.errors} errors · ${s.warnings} warnings</span></button>`).join('') || '<div class="logs-empty">No saved logs for this selection. New logs are saved automatically; older logs from before this update are unavailable.</div>';
      paintControls();
    }
    function lineMarkup(row, context = false) {
      return `<div class="logs-line logs-${row.level.toLowerCase()}${context?' logs-context-line':''}" ${context?'':`data-line="${row.id}"`}><time>${esc(new Date(row.ts*1000).toLocaleTimeString())}</time><span class="logs-level">${esc(row.level)}</span><span class="logs-provider">${esc(row.provider)}</span><span class="logs-message">${highlight(row.text,query)}</span>${context?'':`<button data-context="${row.id}" aria-label="Show context for line ${row.id}" title="Show surrounding lines">${icon('unfold_more')}</button>`}</div>`;
    }
    async function refresh() {
      if (busy || !alive) return;
      busy = true; const token = sequence;
      controller = new AbortController();
      try {
        let listing = await api('', {channel, ...scope()});
        if (!alive || token !== sequence) return;
        pairs=listing.pairs || [];latestRunId=listing.latest_run_id || '';
        const options='<option value="">All pairs</option>'+pairs.map(p=>`<option value="${esc(p.id)}">${esc(p.label)}</option>`).join('')+(pairId&&!pairs.some(p=>p.id===pairId)?`<option value="${esc(pairId)}" disabled>Pair unavailable</option>`:'');
        if($('[data-pair]').innerHTML!==options){$('[data-pair]').innerHTML=options;$('[data-pair]').value=pairId;$('[data-pair]').dispatchEvent(new Event('change'));}
        if(latestRequested){
          latestRequested=false;
          if(latestRunId){runId=latestRunId;listing=await api('',{channel,...scope()});if(!alive||token!==sequence)return;}
          else notice('No saved run for this pair yet. Logs will appear after its next sync.');
          writeRoute();
        }
        sessions = listing.items;
        host.querySelectorAll('[data-channel]').forEach(b=>{b.hidden=!listing.channels.includes(b.dataset.channel)});
        if (!listing.channels.includes(channel)) { channel='sync'; runId=''; sid=''; scheduleReload(); return; }
        $('[data-storage]').textContent = `${listing.retention_days}-day retention · ${Math.round(listing.used_bytes/1048576)} / ${Math.round(listing.max_bytes/1048576)} MB`;
        if (showingRuns) {
          if (runId && sessions.length) { sid=sessions[0].id; showingRuns=false; }
          else {renderRuns(); $('[data-connection]').textContent='Saved on this server'; return;}
        }
        if (mode === 'live') {
          const newest = sessions.find(s=>s.ended===null) || sessions[0];
          if (newest && newest.id!==sid) {sid=newest.id;offset=0;}
        }
        current=sessions.find(s=>s.id===sid) || null;
        if (!current) { rows=[];total=0;$('[data-output]').innerHTML='<div class="logs-empty">No logs available yet. Logging is saved automatically as activity arrives.</div>';$('[data-connection]').textContent='Waiting for activity';paintControls();return; }
        let data=await api(`/${sid}/lines`,{q:query,level,provider,offset,limit:500,...scope()});
        if (!alive || token!==sequence) return;
        const nextOffset=follow?Math.max(0,Math.floor((data.total-1)/500)*500):Math.min(offset,Math.max(0,Math.floor((data.total-1)/500)*500));
        if (nextOffset!==offset) {offset=nextOffset;data=await api(`/${sid}/lines`,{q:query,level,provider,offset,limit:500,...scope()});}
        if (!alive || token!==sequence) return;
        current=data.session;total=data.total;
        // Keep expanded context and text selection intact when the page did not change.
        if (JSON.stringify(rows)!==JSON.stringify(data.items)) {
          rows=data.items;
          $('[data-output]').innerHTML=rows.map(r=>lineMarkup(r)).join('') || '<div class="logs-empty">No matching lines. Try a different search or level.</div>';
          $('#log-providers').innerHTML=[...new Set(rows.map(r=>r.provider))].sort().map(p=>`<option value="${esc(p)}"></option>`).join('');
          if(follow) content.scrollTop=content.scrollHeight;
        }
        if (pendingMatch !== null) {revealMatch(pendingMatch < 0 ? rows.length-1 : 0);pendingMatch=null;}
        $('[data-connection]').textContent = mode==='live' ? current.ended ? 'Waiting for activity' : 'Live · updates every 2s' : 'Saved log';
        if(current.truncated) notice('This log reached the storage limit. Some lines were not saved. Delete or unpin older logs to free space.');
        paintControls();
      } catch(error) {if(alive && token===sequence && error.name!=='AbortError'){notice(error.message);$('[data-connection]').textContent='Disconnected · retrying';}}
      finally {busy=false;}
    }
    function scheduleReload() {
      sequence++; controller?.abort(); rows=[];matchIndex=-1;
      clearTimeout(searchTimer); searchTimer=setTimeout(()=>{if(busy){scheduleReload();return;}refresh();},150);
    }
    function reset() {sid='';current=null;offset=0;total=0;listPage=0;rows=[];pendingMatch=null;$('[data-output]').replaceChildren();notice();scheduleReload();paintControls();writeRoute();}
    host.addEventListener('click', onClick);
    async function onClick(event) {
      const button=event.target.closest('button');if(!button)return;
      try {
        if(button.dataset.channel){channel=button.dataset.channel;if(channel==='watcher'){runId='';pairId='';latestRequested=false;}showingRuns=mode==='saved';reset();}
        else if(button.dataset.mode){mode=button.dataset.mode;showingRuns=mode==='saved';follow=mode==='live';runId='';latestRequested=false;reset();}
        else if(button.hasAttribute('data-latest')){latestRequested=true;runId='';mode='saved';showingRuns=true;follow=false;clearFilters();reset();}
        else if(button.dataset.session){sid=button.dataset.session;if(channel==='sync')runId=sessions.find(s=>s.id===sid)?.run_id||'';showingRuns=false;offset=0;rows=[];notice();scheduleReload();writeRoute();}
        else if(button.hasAttribute('data-back-runs')){showingRuns=true;runId='';reset();}
        else if(button.hasAttribute('data-follow')){follow=!follow;paintControls();if(follow){content.scrollTop=content.scrollHeight;scheduleReload();}}
        else if(button.hasAttribute('data-errors')||button.hasAttribute('data-warnings')){level=button.hasAttribute('data-errors')?'ERROR':'WARN';$('[data-level]').value=level;follow=false;offset=0;scheduleReload();}
        else if(button.hasAttribute('data-previous')||button.hasAttribute('data-next')){const delta=button.hasAttribute('data-next')?1:-1;follow=false;if(showingRuns){listPage+=delta;renderRuns();}else{offset=Math.max(0,offset+delta*500);scheduleReload();}content.scrollTop=0;}
        else if(button.hasAttribute('data-next-match')||button.hasAttribute('data-prev-match')){
          const delta=button.hasAttribute('data-next-match')?1:-1;
          follow=false;
          if(delta>0 && matchIndex>=rows.length-1 && offset+500<total){offset+=500;scheduleReload();pendingMatch=1;}
          else if(delta<0 && matchIndex===0 && offset>0){offset-=500;scheduleReload();pendingMatch=-1;}
          else revealMatch(matchIndex<0 ? (delta>0?0:rows.length-1) : matchIndex+delta);
          paintControls();
        }
        else if(button.dataset.context){const container=button.closest('[data-line]');if(container.nextElementSibling?.classList.contains('logs-context')){container.nextElementSibling.remove();return;}const token=sequence, target=sid;const data=await api(`/${sid}/context/${button.dataset.context}`,scope());if(!alive||token!==sequence||target!==sid||!container.isConnected)return;const el=document.createElement('div');el.className='logs-context';el.innerHTML=data.items.map(r=>lineMarkup(r,true)).join('');container.after(el);}
        else if(button.hasAttribute('data-pin')){await api(`/${sid}`,{}, {method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({pinned:!current.pinned})});scheduleReload();}
        else if(button.hasAttribute('data-delete')){if(!window.confirm('Delete this entire saved log, including any other pairs in it?'))return;await api(`/${sid}`,{}, {method:'DELETE'});showingRuns=true;runId='';reset();}
        else if(button.hasAttribute('data-clear')){if(!window.confirm('Delete completed, unpinned logs in this channel for the current profile?'))return;await api('',{channel},{method:'DELETE'});reset();}
        else if(['data-copy','data-copy-full','data-download','data-download-full'].some(a=>button.hasAttribute(a))){
          if(!sid)return; const full=button.hasAttribute('data-copy-full')||button.hasAttribute('data-download-full');
          const res=await fetch(url(`/${sid}/download`,{...scope(),...(full?{}:{q:query,level,provider})}),{cache:'no-store'});if(!res.ok)throw new Error('Could not export this log.');
          const text=await res.text();if(!alive)return;
          if(button.hasAttribute('data-copy')||button.hasAttribute('data-copy-full')){await navigator.clipboard.writeText(text);notice('Log copied.');}
          else {const link=document.createElement('a');const object=URL.createObjectURL(new Blob([text],{type:'text/plain'}));link.href=object;link.download=`crosswatch-${channel}-${sid}.log`;link.click();setTimeout(()=>URL.revokeObjectURL(object),1000);}
        }
      } catch(error){if(alive)notice(error.message);}
    }
    for(const selector of ['[data-query]','[data-level]','[data-provider]']) $(selector).addEventListener('input',()=>{query=$('[data-query]').value;level=$('[data-level]').value;provider=$('[data-provider]').value.trim().toUpperCase();offset=0;follow=false;pendingMatch=null;notice();scheduleReload();});
    $('[data-pair]').addEventListener('input',()=>{pairId=$('[data-pair]').value;runId='';latestRequested=mode==='saved';showingRuns=mode==='saved';follow=mode==='live';clearFilters();reset();});
    $('[data-wrap]').onchange=()=>content.classList.toggle('logs-nowrap',!$('[data-wrap]').checked);
    content.addEventListener('wheel',event=>{if(event.deltaY<0){follow=false;paintControls();}},{passive:true});
    content.addEventListener('scroll',()=>{if(follow && content.scrollHeight-content.scrollTop-content.clientHeight>50){follow=false;paintControls();}});
    timer=setInterval(refresh,2000);
    dispose=()=>{alive=false;sequence++;controller?.abort();clearInterval(timer);clearTimeout(searchTimer);window.removeEventListener('resize',resize);host.removeEventListener('click',onClick);};
    paintControls();await refresh();resize();
  },
  hide(){dispose?.();dispose=null;},
  unmount(){this.hide();root?.replaceChildren();root=null;}
};
window.LogsPage=LogsPage;
for(const name of ['cw:overview-profile-changed','cw:auth-state-changed'])window.addEventListener(name,()=>{const host=root;LogsPage.unmount();if(host&&document.documentElement.dataset.tab==='logs')LogsPage.mount(host);});
export default LogsPage;
