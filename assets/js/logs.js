/* assets/js/logs.js */
/* CrossWatch - Live and saved diagnostic logs */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {readableLogHTML} from './logs-format.js';
import {pageBackLink} from './page-return.js';

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const date = ts => ts ? new Date(ts * 1000).toLocaleString() : 'Live';
const logFeatures = [['watchlist','Watchlist','wl'],['ratings','Ratings','rt'],['history','History','hi'],['progress','Progress','pr'],['playlists','Playlists','pl'],['collection','Collections','co']];
export function savedLogFeatures(session, pairId = '') {
  let pairs;
  try { pairs = typeof session.pairs === 'string' ? JSON.parse(session.pairs) : session.pairs; } catch { return null; }
  if (!Array.isArray(pairs)) return null;
  const selected = pairs.filter(p => p && (!pairId || String(p.id) === pairId));
  if (!selected.length || selected.some(p => !p.features || typeof p.features !== 'object' || Array.isArray(p.features))) return null;
  const enabled = value => {
    if (value && typeof value === 'object') value = value.enable ?? value.enabled;
    return value === true || value === 1 || ['true','1','on','yes'].includes(String(value).toLowerCase());
  };
  return logFeatures.map(([key]) => selected.some(p => enabled(p.features[key])));
}
export function savedLogsTable(sessions, pairId = '', channel = 'sync') {
  const rows = sessions.map(s => {
    const started = new Date(s.started * 1000);
    const features = savedLogFeatures(s, pairId);
    const enabledNames = features ? logFeatures.filter((_, i) => features[i]).map(([,name]) => name) : [];
    const featureLabel = enabledNames.length ? `Enabled features: ${enabledNames.join(', ')}. Combined across pairs in this run.` : 'No enabled features';
    const dots = features ? `<span class="logs-features" role="img" aria-label="${esc(featureLabel)}" title="${esc(featureLabel)}">${logFeatures.map(([,name,cls],i) => `<span class="logs-feature-dot ${cls}${features[i] ? ' on' : ''}" title="${name}: ${features[i] ? 'enabled' : 'disabled'}" aria-hidden="true"></span>`).join('')}</span>` : '<span class="logs-unavailable" title="Feature settings were not recorded for this log" aria-label="Feature settings unavailable">—</span>';
    const status = String(s.status || 'unknown').toLowerCase();
    const statuses = {completed:['Completed','check_circle','success'],complete:['Completed','check_circle','success'],issues:['Completed with issues','warning','warning'],failed:['Failed','error','error'],error:['Error','error','error'],cancelled:['Cancelled','cancel','warning'],interrupted:['Interrupted','warning','warning'],running:['Running','sync','active'],live:['Live','sensors','active'],saved:['Saved','inventory_2','neutral']};
    const [label, symbol, tone] = Object.hasOwn(statuses,status) ? statuses[status] : [s.status || 'Unknown','info','neutral'];
    const errors = Math.max(0, Number(s.errors) || 0), warnings = Math.max(0, Number(s.warnings) || 0);
    const duration = s.ended == null ? 'Live' : `${Math.max(0,Math.round(s.ended-s.started))}s`;
    return `<tr data-session="${esc(s.id)}"><td><button class="logs-saved-open" data-session="${esc(s.id)}" aria-label="${esc(`Open log: ${s.label}`)}">${icon(s.pinned ? 'push_pin' : 'description')}<strong>${esc(s.label)}</strong></button></td><td>${dots}</td><td><time datetime="${started.toISOString()}">${esc(started.toLocaleDateString())}<small>${esc(started.toLocaleTimeString())}</small></time></td><td>${duration}</td><td><span class="logs-status logs-status-${tone}">${icon(symbol)}${esc(label)}</span></td><td class="${errors ? 'logs-error' : ''}">${errors}</td><td class="${warnings ? 'logs-warn' : ''}">${warnings}</td><td class="logs-saved-arrow">${icon('chevron_right')}</td></tr>`;
  }).join('');
  return `<table class="logs-saved-table" aria-label="Saved logs"><thead><tr><th scope="col">${channel === 'sync' ? 'Sync pair' : 'Log'}</th><th scope="col">Features</th><th scope="col" aria-sort="descending">Started ${icon('arrow_downward')}</th><th scope="col">Duration</th><th scope="col">Status</th><th scope="col">Errors</th><th scope="col">Warnings</th><th scope="col"><span class="logs-sr-only">Open log</span></th></tr></thead><tbody>${rows}</tbody></table>`;
}
export const logsBackLink = pageBackLink;
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
    const back = logsBackLink(route.get('returnTo'), location.href);
    let channel = ['sync','watcher','debug'].includes(route.get('channel')) ? route.get('channel') : 'sync';
    let pairId = route.get('pairId') || '', latestRequested = route.get('latest') === '1', pairs = [], latestRunId = '';
    if (channel === 'watcher') pairId = '';
    let runId = route.get('runId') || '', mode = runId || latestRequested ? 'saved' : 'live', sid = '', sessions = [], current = null;
    let alive = true, busy = false, sequence = 0, timer, searchTimer, offset = 0, total = 0, follow = mode === 'live', rows = [], matchIndex = -1;
    let query = '', level = '', provider = '', listPage = 0, showingRuns = mode === 'saved', controller = null, pendingMatch = null;
    let readable = true;
    try { readable = window.localStorage.getItem('cw.logs.readable') !== 'off'; } catch {}
    const visibleRows = new Map();
    const copyFeedback = new Map();
    const profile = window.CW?.OverviewProfile?.id || '';
    host.innerHTML = `<div class="logs-page"><a href="${esc(back.href)}" class="logs-back">${icon('arrow_back')}${esc(back.label)}</a>
      <header class="logs-heading"><div><div class="logs-eyebrow">DIAGNOSTICS</div><h1>Logs</h1></div></header>
      <div class="logs-toolbar logs-channels"><div class="logs-tabs" role="tablist" aria-label="Log channel">${['sync','watcher','debug'].map(c=>`<button data-channel="${c}" role="tab"${c === 'debug' ? ' hidden' : ''}>${icon({sync:"sync",watcher:"sensors",debug:"bug_report"}[c])}${c[0].toUpperCase()+c.slice(1)}</button>`).join('')}</div><span class="logs-connection" data-connection role="status">${icon('sensors')}<span data-connection-text>Connecting…</span></span></div>
      <div class="logs-toolbar logs-controls"><div class="logs-tabs" role="tablist" aria-label="Log source"><button data-mode="live" role="tab">${icon("podcasts")}Live</button><button data-mode="saved" role="tab">${icon("inventory_2")}Saved logs</button></div><div class="logs-pair-controls" data-pair-controls><select data-pair aria-label="Sync pair"><option value="">All pairs</option></select><button data-latest title="Open the latest run for this selection">${icon('history')}Latest run</button></div><div class="logs-storage" data-storage hidden><div class="logs-storage-copy"><span>${icon("hard_drive")}<span data-storage-size></span></span><span data-retention></span></div><progress data-storage-meter max="100" value="0" aria-label="Log storage used"></progress></div></div>
      <div class="logs-toolbar" data-filters><label class="logs-search">${icon('search')}<input data-query type="search" placeholder="Search logs…" aria-label="Search logs"></label><select data-level aria-label="Log level"><option value="">All levels</option><option value="ERROR">Errors</option><option value="WARN">Warnings</option><option value="INFO">Info</option><option value="DEBUG">Debug</option></select><input data-provider placeholder="Provider" aria-label="Filter by provider" list="log-providers"><datalist id="log-providers"></datalist><button data-prev-match title="Previous match" aria-label="Previous match">${icon('keyboard_arrow_up')}</button><button data-next-match title="Next match" aria-label="Next match">${icon('keyboard_arrow_down')}</button><button data-follow>${icon("podcasts")}<span>Follow live</span></button>
      <label class="logs-readable-switch" title="Format log messages for easier reading"><input data-readable type="checkbox" role="switch" ${readable ? 'checked' : ''}><span class="logs-switch-track" aria-hidden="true"></span>Readable view</label>
      <details class="logs-options"><summary>${icon("tune")}More</summary><div><label><input data-wrap type="checkbox" checked>Wrap lines</label><button data-copy>${icon("content_copy")}Copy filtered log</button><button data-copy-full>${icon("copy_all")}Copy full log</button><button data-download>${icon("download")}Download filtered log</button><button data-download-full>${icon("file_download")}Download full log</button></div></details></div>
      <div class="logs-toolbar logs-runbar"><button data-back-runs hidden>${icon('arrow_back')}Saved logs</button><div class="logs-run-title">${icon("terminal")}<strong data-title></strong><span class="logs-run-count" data-run-count hidden></span></div><button data-errors class="logs-error">${icon("error_outline")}<span></span></button><button data-warnings class="logs-warn">${icon("warning")}<span></span></button><button data-pin hidden>${icon("push_pin")}<span>Keep this log</span></button><button data-delete hidden>${icon('delete')}Delete</button><button data-clear hidden>${icon("delete_sweep")}Clear older logs</button></div>
      <span class="logs-sr-only" data-copy-status role="status"></span><p class="logs-notice" data-notice role="status" hidden></p><div class="logs-content" tabindex="0" aria-label="Log output"><div data-output></div></div>
      <footer class="logs-footer"><span data-count></span><button data-previous>${icon("chevron_left")}Previous</button><span data-page></span><button data-next>Next${icon("chevron_right")}</button></footer></div>`;
    const $ = s => host.querySelector(s);
    const content = $('.logs-content');
    content.classList.toggle('logs-pretty', readable);
    function resize() { const page=$('.logs-page'); if(alive && page)page.style.height = `${Math.max(260, window.innerHeight - host.getBoundingClientRect().top - 16)}px`; }
    window.addEventListener('resize', resize); resize();
    function url(path, values = {}) { const p = new URLSearchParams({...values, user_profile:profile}); return '/api/logs/archive' + path + '?' + p; }
    const scope = () => ({pair_id:pairId, run_id:runId});
    function writeRoute() {
      const params = new URLSearchParams({channel});
      if(pairId)params.set('pairId',pairId);
      if(runId)params.set('runId',runId);
      if(latestRequested)params.set('latest','1');
      if(route.has('returnTo'))params.set('returnTo',route.get('returnTo'));
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
    function connection(text, symbol = 'sensors', state = 'idle') {
      $('[data-connection-text]').textContent = text;
      $('[data-connection] .material-symbols-rounded').textContent = symbol;
      $('[data-connection]').dataset.state = state;
    }
    function emptyState(symbol, title, detail, liveAction = false) {
      return `<div class="logs-empty"><span class="logs-empty-icon">${icon(symbol)}</span><h2>${esc(title)}</h2><p>${esc(detail)}</p>${liveAction ? `<button data-mode="live" class="logs-empty-action">View live logs${icon('arrow_forward')}</button><small>${icon('info')}Logs are saved automatically on this server.</small>` : ''}</div>`;
    }
    function output(html) {
      if ($('[data-output]').innerHTML !== html) $('[data-output]').innerHTML = html;
    }
    function notice(text = '') { $('[data-notice]').textContent = text; $('[data-notice]').hidden = !text; }
    function copied(button) {
      if (!alive) return;
      const symbol = button.querySelector('.material-symbols-rounded');
      const previous = copyFeedback.get(button);
      clearTimeout(previous?.timer);
      const original = previous?.original ?? symbol.textContent;
      symbol.textContent = 'check';
      button.classList.remove('logs-copied');
      void button.offsetWidth;
      button.classList.add('logs-copied');
      $('[data-copy-status]').textContent = 'Log copied to clipboard.';
      const timer = setTimeout(() => {
        symbol.textContent = original;
        button.classList.remove('logs-copied');
        copyFeedback.delete(button);
        if (!copyFeedback.size) $('[data-copy-status]').textContent = '';
      }, 1800);
      copyFeedback.set(button, {original, timer});
    }
    function revealMatch(index) {
      matchIndex = Math.max(0, Math.min(rows.length - 1, index));
      host.querySelectorAll('.logs-current-match').forEach(el=>el.classList.remove('logs-current-match'));
      const line = host.querySelector(`[data-line="${rows[matchIndex]?.id}"]`);
      line?.classList.add('logs-current-match');
      line?.scrollIntoView({block:'center'});
    }
    function paintControls() {
      host.querySelectorAll('[data-channel]').forEach(b=>b.setAttribute('aria-selected',String(b.dataset.channel===channel)));
      host.querySelectorAll('[data-mode][role="tab"]').forEach(b=>b.setAttribute('aria-selected',String(b.dataset.mode===mode)));
      $('[data-follow] span:not(.material-symbols-rounded)').textContent = follow ? 'Following live' : 'Resume live';
      $('[data-follow]').hidden = mode !== 'live';
      $('[data-filters]').hidden = showingRuns;
      $('[data-back-runs]').hidden = showingRuns || mode === 'live';
      $('[data-clear]').hidden = !showingRuns || !!pairId;
      $('[data-pair-controls]').hidden = channel === 'watcher';
      $('[data-latest]').disabled = !latestRunId;
      $('[data-pin]').hidden = showingRuns || !current;
      $('[data-delete]').hidden = showingRuns || !current || current.ended === null;
      $('[data-pin] span:not(.material-symbols-rounded)').textContent = current?.pinned ? 'Unpin log' : 'Keep this log';
      $('[data-pin]').title = 'Keep the entire saved log, including any other pairs in it';
      $('[data-errors]').hidden = showingRuns || !current;
      $('[data-warnings]').hidden = showingRuns || !current;
      $('[data-errors] span:not(.material-symbols-rounded)').textContent = `${current?.errors || 0} errors`;
      $('[data-warnings] span:not(.material-symbols-rounded)').textContent = `${current?.warnings || 0} warnings`;
      const pairLabel = pairs.find(p=>p.id===pairId)?.label;
      $('[data-title]').textContent = showingRuns ? 'Saved logs' : current ? `${pairLabel || current.label} · ${date(current.started)}` : 'No log selected';
      $('[data-run-count]').hidden = !showingRuns;
      $('[data-run-count]').textContent = String(sessions.length);
      $('.logs-run-title .material-symbols-rounded').textContent = showingRuns ? 'inventory_2' : 'terminal';
      $('[data-count]').textContent = showingRuns ? `${sessions.length} saved logs` : `${total} matching lines`;
      $('[data-page]').textContent = showingRuns ? `${listPage+1} / ${Math.max(1,Math.ceil(sessions.length/10))}` : `${Math.floor(offset/500)+1} / ${Math.max(1,Math.ceil(total/500))}`;
      $('[data-previous]').disabled = showingRuns ? listPage === 0 : offset === 0;
      $('[data-next]').disabled = showingRuns ? (listPage+1)*10 >= sessions.length : offset+500 >= total;
      $('[data-prev-match]').disabled = $('[data-next-match]').disabled = !query || !total;
    }
    function renderRuns() {
      listPage = Math.min(listPage, Math.max(0, Math.ceil(sessions.length / 10) - 1));
      output(sessions.length ? savedLogsTable(sessions.slice(listPage*10,listPage*10+10), pairId, channel) : emptyState('manage_history', 'No saved logs yet', runId ? 'No saved log is available for this run.' : pairId ? 'Logs will appear after the next sync for this pair.' : 'Logs will appear here as new activity is recorded.', true));
      paintControls();
    }
    function lineMarkup(row, context = false) {
      visibleRows.set(String(row.id), row);
      const message = readable ? readableLogHTML(row, highlight, query, window.CW?.ProviderMeta) : highlight(row.text,query);
      return `<div class="logs-line logs-${row.level.toLowerCase()}${context?' logs-context-line':''}" data-log-id="${esc(row.id)}" ${context?'':`data-line="${row.id}"`}><time>${esc(new Date(row.ts*1000).toLocaleTimeString())}</time><span class="logs-level">${esc(row.level)}</span><span class="logs-provider">${esc(row.provider)}</span><div class="logs-message">${message}</div>${context?'':`<button data-context="${row.id}" aria-label="Show context for line ${row.id}" title="Show surrounding lines">${icon('unfold_more')}</button>`}</div>`;
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
          writeRoute();
        }
        sessions = listing.items;
        host.querySelectorAll('[data-channel]').forEach(b=>{b.hidden=!listing.channels.includes(b.dataset.channel)});
        if (!listing.channels.includes(channel)) { channel='sync'; runId=''; latestRequested=false; showingRuns=mode==='saved'; clearFilters(); reset(); return; }
        const used = Math.max(0, Number(listing.used_bytes) || 0), capacity = Math.max(0, Number(listing.max_bytes) || 0);
        const percent = capacity ? Math.min(100, used / capacity * 100) : 0;
        const size = bytes => (bytes / 1048576).toLocaleString(undefined, {maximumFractionDigits:1});
        const storageText = `${size(used)} / ${size(capacity)} MB`;
        $('[data-storage]').hidden = false;
        $('[data-storage]').dataset.usage = percent >= 95 ? 'high' : percent >= 80 ? 'warning' : 'normal';
        $('[data-storage-size]').textContent = storageText;
        $('[data-retention]').textContent = `${listing.retention_days}-day retention`;
        $('[data-storage-meter]').value = percent;
        $('[data-storage-meter]').setAttribute('aria-valuetext', `${storageText} used`);
        if (showingRuns) {
          if (runId && sessions.length) { sid=sessions[0].id; showingRuns=false; }
          else {renderRuns(); connection('Saved on this server', 'dns'); return;}
        }
        if (mode === 'live') {
          const newest = sessions.find(s=>s.ended===null) || sessions[0];
          if (newest && newest.id!==sid) {sid=newest.id;offset=0;}
        }
        current=sessions.find(s=>s.id===sid) || null;
        if (!current) { rows=[];total=0;output(emptyState('sensors', 'Waiting for activity', 'New log lines will appear here automatically.'));connection('Waiting for activity');paintControls();return; }
        let data=await api(`/${sid}/lines`,{q:query,level,provider,offset,limit:500,...scope()});
        if (!alive || token!==sequence) return;
        const nextOffset=follow?Math.max(0,Math.floor((data.total-1)/500)*500):Math.min(offset,Math.max(0,Math.floor((data.total-1)/500)*500));
        if (nextOffset!==offset) {offset=nextOffset;data=await api(`/${sid}/lines`,{q:query,level,provider,offset,limit:500,...scope()});}
        if (!alive || token!==sequence) return;
        current=data.session;total=data.total;
        // Keep expanded context and text selection intact when the page did not change.
        if (!data.items.length || JSON.stringify(rows)!==JSON.stringify(data.items)) {
          rows=data.items;
          visibleRows.clear();
          output(rows.map(r=>lineMarkup(r)).join('') || emptyState('search_off', 'No matching lines', 'Try another search or adjust the level and provider filters.'));
          $('#log-providers').innerHTML=[...new Set(rows.map(r=>r.provider))].sort().map(p=>`<option value="${esc(p)}"></option>`).join('');
          if(follow) content.scrollTop=content.scrollHeight;
        }
        if (pendingMatch !== null) {revealMatch(pendingMatch < 0 ? rows.length-1 : 0);pendingMatch=null;}
        connection(mode==='live' ? current.ended ? 'Waiting for activity' : 'Live · updates every 2s' : 'Saved log', mode==='live' ? 'sensors' : 'dns', mode==='live' && !current.ended ? 'live' : 'idle');
        if(current.truncated) notice('This log reached the storage limit. Some lines were not saved. Delete or unpin older logs to free space.');
        paintControls();
      } catch(error) {if(alive && token===sequence && error.name!=='AbortError'){notice(error.message);connection('Disconnected · retrying', 'cloud_off', 'error');}}
      finally {busy=false;}
    }
    function scheduleReload() {
      sequence++; controller?.abort(); rows=[];matchIndex=-1;
      clearTimeout(searchTimer); searchTimer=setTimeout(()=>{if(busy){scheduleReload();return;}refresh();},150);
    }
    function reset() {sid='';current=null;offset=0;total=0;listPage=0;rows=[];pendingMatch=null;$('[data-output]').replaceChildren();notice();scheduleReload();paintControls();writeRoute();}
    host.addEventListener('click', onClick);
    async function onClick(event) {
      const button=event.target.closest('button, tr[data-session]');if(!button)return;
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
          if(button.hasAttribute('data-copy')||button.hasAttribute('data-copy-full')){await navigator.clipboard.writeText(text);copied(button);}
          else {const link=document.createElement('a');const object=URL.createObjectURL(new Blob([text],{type:'text/plain'}));link.href=object;link.download=`crosswatch-${channel}-${sid}.log`;link.click();setTimeout(()=>URL.revokeObjectURL(object),1000);}
        }
      } catch(error){if(alive)notice(error.message);}
    }
    for(const selector of ['[data-query]','[data-level]','[data-provider]']) $(selector).addEventListener('input',()=>{query=$('[data-query]').value;level=$('[data-level]').value;provider=$('[data-provider]').value.trim().toUpperCase();offset=0;follow=false;pendingMatch=null;notice();scheduleReload();});
    $('[data-pair]').addEventListener('input',()=>{pairId=$('[data-pair]').value;runId='';latestRequested=mode==='saved';showingRuns=mode==='saved';follow=mode==='live';clearFilters();reset();});
    $('[data-wrap]').onchange=()=>{
      content.classList.toggle('logs-nowrap',!$('[data-wrap]').checked);
      content.scrollLeft=0;
      if(follow)content.scrollTop=content.scrollHeight;
    };
    $('[data-readable]').onchange=()=>{
      readable=$('[data-readable]').checked;
      try { window.localStorage.setItem('cw.logs.readable',readable ? 'on' : 'off'); } catch {}
      const top=content.scrollTop;
      content.classList.toggle('logs-pretty',readable);
      host.querySelectorAll('[data-log-id]').forEach(line=>{
        const row=visibleRows.get(line.dataset.logId);
        if(row)line.querySelector('.logs-message').innerHTML=readable ? readableLogHTML(row,highlight,query,window.CW?.ProviderMeta) : highlight(row.text,query);
      });
      content.scrollTop=follow ? content.scrollHeight : top;
    };
    content.addEventListener('wheel',event=>{if(event.deltaY<0){follow=false;paintControls();}},{passive:true});
    content.addEventListener('scroll',()=>{if(follow && content.scrollHeight-content.scrollTop-content.clientHeight>50){follow=false;paintControls();}});
    timer=setInterval(refresh,2000);
    dispose=()=>{alive=false;sequence++;controller?.abort();clearInterval(timer);clearTimeout(searchTimer);for(const feedback of copyFeedback.values())clearTimeout(feedback.timer);copyFeedback.clear();window.removeEventListener('resize',resize);host.removeEventListener('click',onClick);};
    paintControls();await refresh();resize();
  },
  hide(){dispose?.();dispose=null;},
  unmount(){this.hide();root?.replaceChildren();root=null;}
};
window.LogsPage=LogsPage;
for(const name of ['cw:overview-profile-changed','cw:auth-state-changed'])window.addEventListener(name,()=>{const host=root;LogsPage.unmount();if(host&&document.documentElement.dataset.tab==='logs')LogsPage.mount(host);});
export default LogsPage;
