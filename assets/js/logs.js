/* assets/js/logs.js */
/* CrossWatch - Raw diagnostic log reader and issue exports */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {pageBackLink} from './page-return.js';

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const channels = {sync:['Sync','sync'],watcher:['Watcher','sensors'],captures:['Captures','photo_camera'],backups:['Backups','backup'],audits:['Audits','policy'],system:['System','terminal']};
const stamp = ts => ts ? new Date(ts * 1000).toLocaleString() : '';
const statuses = {completed:['Completed','success'],complete:['Completed','success'],issues:['With issues','warning'],failed:['Failed','error'],error:['Error','error'],cancelled:['Cancelled','warning'],interrupted:['Interrupted','warning'],running:['Running','active'],live:['Live','active'],saved:['Saved','neutral']};
export const logsBackLink = pageBackLink;
export function highlight(text, query) {
  text = String(text ?? '');
  if (!query) return esc(text);
  const lower = text.toLowerCase(), needle = query.toLowerCase();
  let pos = 0, result = '', found;
  while ((found = lower.indexOf(needle, pos)) !== -1) {
    result += esc(text.slice(pos, found)) + '<mark>' + esc(text.slice(found, found + query.length)) + '</mark>';
    pos = found + query.length;
  }
  return result + esc(text.slice(pos));
}
export function savedLogList(sessions, selected = '') {
  return sessions.map(s => {
    const [label,tone] = Object.hasOwn(statuses,s.status) ? statuses[s.status] : [s.status || 'Unknown','neutral'];
    const duration = s.ended == null ? 'Active' : `${Math.max(0,Math.round(s.ended-s.started))}s`;
    return `<button type="button" class="logs-session" data-session="${esc(s.id)}" aria-pressed="${s.id===selected}" aria-label="${esc(`Open log: ${s.label}`)}"><span class="logs-session-top"><time>${esc(stamp(s.started))}</time><span class="logs-status-${tone}">${esc(label)}</span></span><strong>${esc(s.label)}</strong><small>${duration} · ${Number(s.lines)||0} records${s.errors ? ` · ${Number(s.errors)} errors` : ''}${s.warnings ? ` · ${Number(s.warnings)} warnings` : ''}${s.pinned ? ' · Pinned' : ''}</small></button>`;
  }).join('');
}

let dispose = null, root = null;
const LogsPage = {
  async mount(host) {
    dispose?.(); root = host;
    if (!host) return;
    const route = new URLSearchParams(location.hash.split('?')[1] || '');
    const back = logsBackLink(route.get('returnTo'),location.href);
    let channel = Object.hasOwn(channels,route.get('channel')) ? route.get('channel') : route.get('channel')==='debug' ? 'system' : 'sync';
    let mode = route.get('mode')==='live' ? 'live' : 'saved';
    let pairId = route.get('pairId') || '', runId = route.get('runId') || '', sid = route.get('sessionId') || '';
    let latestRequested = route.get('latest')==='1', query = '', level = route.get('channel')==='debug' ? 'DEBUG' : '', provider = '';
    if (channel!=='sync') pairId='';
    let sessions = [], current = null, rows = [], pairs = [], latestRunId = '', listQuery = '', listFilter = '', listPage = 0;
    let offset = 0, total = 0, snapshotLines = 0, follow = mode==='live', frozen = false, matchIndex = -1;
    let alive = true, busy = false, queued = false, generation = 0, controller, timer, searchTimer, refreshLines = true;
    let storageDirty = false, savingStorage = false, canManageStorage = false;
    const feedbackTimers = new Set();
    const profile = window.CW?.OverviewProfile?.id || '';
    host.innerHTML = `<div class="logs-page cw-page-logs cw-themed-page"><a href="${esc(back.href)}" class="logs-back">${icon('arrow_back')}${esc(back.label)}</a>
      <header class="logs-heading"><h1>Logs</h1></header>
      <div class="logs-navigation"><div class="logs-tabs logs-channels" aria-label="Log source">${Object.entries(channels).map(([key,[label,symbol]])=>`<button type="button" data-channel="${key}" aria-pressed="${channel===key}" hidden>${icon(symbol)}${label}</button>`).join('')}</div><div class="logs-tabs logs-modes" aria-label="Log mode"><button type="button" data-mode="saved">${icon('inventory_2')}Saved logs</button><button type="button" data-mode="live">${icon('podcasts')}Live</button></div></div>
      <p class="logs-notice" data-notice role="status" hidden></p>
      <div class="logs-workspace cw-page-panel"><aside class="logs-sidebar" aria-label="Log sessions"><div class="logs-list-controls"><strong data-list-count>Saved logs</strong><input data-session-query type="search" placeholder="Find a session…" aria-label="Find a session"><select data-session-filter aria-label="Session filter"><option value="">All sessions</option><option value="issues">With issues</option><option value="pinned">Pinned</option></select><div class="logs-pair-controls" data-pair-controls><select data-pair aria-label="Sync pair"><option value="">All pairs</option></select><button type="button" data-latest>Latest run</button></div><button type="button" data-all-runs hidden>All sessions</button></div><div class="logs-sessions" data-sessions></div><div class="logs-list-pages"><button type="button" data-list-prev aria-label="Previous sessions">${icon('chevron_left')}</button><small data-list-page></small><button type="button" data-list-next aria-label="Next sessions">${icon('chevron_right')}</button></div></aside>
      <section class="logs-reader" aria-label="Raw log reader"><div class="logs-reader-heading"><div class="logs-reader-identity"><h2 data-title>Choose a session</h2><small data-meta></small></div><span data-status class="logs-status"></span><div class="logs-actions"><button type="button" data-copy-full class="logs-primary">${icon('copy_all')}Copy full log</button><button type="button" data-download-full>${icon('download')}Download .log</button><button type="button" data-copy hidden>${icon('content_copy')}Copy filtered log</button><button type="button" data-pin>${icon('push_pin')}<span>Pin</span></button><button type="button" data-follow hidden>${icon('pause')}<span>Pause display</span></button><button type="button" data-ai-analyze data-ai-logs hidden>${icon('auto_awesome')}Analyze</button><details class="logs-options"><summary aria-label="More log actions">${icon('more_horiz')}</summary><div><button type="button" data-download>Download filtered log</button><button type="button" data-delete>Delete session</button></div></details></div></div>
      <div class="logs-filters"><input data-query maxlength="200" type="search" placeholder="Filter log text…" aria-label="Filter log text"><select data-level aria-label="Log level"><option value="">All levels</option><option value="ERROR">Errors</option><option value="WARN">Warnings</option><option value="INFO">Info</option><option value="DEBUG">Debug</option></select><input type="text" maxlength="80" data-provider placeholder="Provider" aria-label="Filter by provider" list="log-providers"><datalist id="log-providers"></datalist><label><input data-wrap type="checkbox" checked>Wrap</label><button type="button" data-prev-match aria-label="Previous matching record">${icon('keyboard_arrow_up')}</button><button type="button" data-next-match aria-label="Next matching record">${icon('keyboard_arrow_down')}</button></div>
      <div class="logs-content" tabindex="0" aria-label="Raw log text"><div data-output></div></div>
      <div class="logs-reader-footer"><span data-count></span><label><input data-report-header type="checkbox" checked>Include report header</label><div class="logs-pagination"><button type="button" data-previous aria-label="Previous log records">${icon('chevron_left')}</button><span data-page></span><button type="button" data-next aria-label="Next log records">${icon('chevron_right')}</button></div></div><div class="logs-reader-footer"><span data-copy-status role="status">Select text to copy an excerpt.</span><span data-connection role="status"></span></div></section></div>
      <section class="logs-storage" aria-label="Log storage"><div class="logs-storage-summary"><span data-storage-size></span><span data-retention></span><button type="button" data-storage-toggle aria-expanded="false" aria-controls="logs-storage-panel">Storage &amp; retention${icon('expand_more')}</button></div><div id="logs-storage-panel" class="logs-storage-panel cw-page-panel" hidden>
        <form data-storage-form hidden><label for="logs-max-size">Archive size limit (MB)<input id="logs-max-size" data-storage-limit type="number" min="10" max="10240" step="1" required aria-describedby="logs-storage-help"></label><button type="submit" data-save-storage class="logs-primary" disabled>Save limit</button><span data-storage-status role="status"></span></form>
        <p id="logs-storage-help">Applies to all log sources on this server. Choose 10–10,240 MB. Lowering the limit removes the oldest finished, unpinned logs first.</p><p data-storage-admin hidden>Storage settings are managed by your server administrator.</p><p>Unpinned sessions expire after <span data-retention-days>7</span> days, or sooner when storage fills. Pinned and active sessions are kept.</p><p data-storage-over-limit hidden>Pinned or active sessions exceed this limit. New records may be skipped until space is freed or the limit is increased.</p><div class="logs-storage-cleanup"><span>Remove finished, unpinned logs from this source.</span><button type="button" data-clear>Delete finished, unpinned logs</button></div>
      </div></section></div>`;
    const $ = selector => host.querySelector(selector), content = $('.logs-content');
    $('[data-level]').value=level;
    const scope = () => ({pair_id:pairId,run_id:runId});
    const filtered = () => Boolean(query||level||provider||pairId||runId);
    const url = (path,values={}) => '/api/logs/archive'+path+'?'+new URLSearchParams({...values,user_profile:profile});
    const notice = text => { $('[data-notice]').textContent=text; $('[data-notice]').hidden=!text; };
    const setOutput = html => {if($('[data-output]').innerHTML!==html)$('[data-output]').innerHTML=html;};
    const empty = (title,description) => `<div class="logs-empty"><h2>${esc(title)}</h2><p>${esc(description)}</p></div>`;
    function paintStorage(data) {
      const size=n=>(Number(n||0)/1048576).toLocaleString(undefined,{maximumFractionDigits:1});
      $('[data-storage-size]').textContent=`${size(data.used_bytes)} / ${size(data.max_bytes)} MB`;
      $('[data-retention]').textContent=`${data.retention_days}-day retention`;
      $('[data-retention-days]').textContent=data.retention_days;
      if(Object.hasOwn(data,'can_manage_storage'))canManageStorage=Boolean(data.can_manage_storage);
      $('[data-storage-form]').hidden=!canManageStorage;
      $('[data-storage-admin]').hidden=canManageStorage;
      if(!storageDirty&&!savingStorage)$('[data-storage-limit]').value=String(Math.round(data.max_bytes/1048576));
      $('[data-save-storage]').disabled=!canManageStorage||savingStorage||!storageDirty;
      $('[data-storage-over-limit]').hidden=!canManageStorage||data.used_bytes<=data.max_bytes;
    }
    const listing = () => sessions.filter(s=>(!listQuery||`${s.label} ${stamp(s.started)} ${s.run_id||''}`.toLowerCase().includes(listQuery))&&(!listFilter||listFilter==='pinned'&&s.pinned||listFilter==='issues'&&(s.errors||s.warnings||['issues','failed','error','interrupted','cancelled'].includes(s.status))));
    function writeRoute() {
      const params=new URLSearchParams({channel,mode});
      if(pairId)params.set('pairId',pairId);if(runId)params.set('runId',runId);if(sid)params.set('sessionId',sid);
      if(route.has('returnTo'))params.set('returnTo',route.get('returnTo'));
      window.history.replaceState(null,'','#logs?'+params);
    }
    function renderSessions() {
      const list=listing();listPage=Math.min(listPage,Math.max(0,Math.ceil(list.length/20)-1));
      const html=savedLogList(list.slice(listPage*20,listPage*20+20),sid)||empty('No sessions',listQuery||listFilter?'Adjust the session filters.':'Logs appear when new activity is recorded.');
      if($('[data-sessions]').innerHTML!==html)$('[data-sessions]').innerHTML=html;
      $('[data-list-count]').textContent=`${list.length} sessions`;
      $('[data-list-page]').textContent=`${listPage+1} / ${Math.max(1,Math.ceil(list.length/20))}`;
      $('[data-list-prev]').disabled=listPage===0;$('[data-list-next]').disabled=(listPage+1)*20>=list.length;
    }
    function paint() {
      host.querySelectorAll('[data-channel]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.channel===channel)));
      host.querySelectorAll('[data-mode]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.mode===mode)));
      $('[data-title]').textContent=current?.label||'Choose a session';
      $('[data-meta]').textContent=current?`${stamp(current.started)} · ${current.ended==null?'Active':Math.max(0,Math.round(current.ended-current.started))+'s'} · ${current.id}`:'';
      const [label,tone]=current&&Object.hasOwn(statuses,current.status)?statuses[current.status]:['','neutral'];
      $('[data-status]').textContent=label;$('[data-status]').className=`logs-status logs-status-${tone}`;
      for(const selector of ['[data-copy-full]','[data-copy]','[data-download-full]','[data-download]','[data-pin]'])$(selector).disabled=!current;
      $('[data-delete]').disabled=!current||current.ended==null;
      $('[data-copy]').hidden=!filtered();$('[data-download]').hidden=!filtered();
      $('[data-pin] span:not(.material-symbols-rounded)').textContent=current?.pinned?'Unpin':'Pin';
      $('[data-pin]').setAttribute('aria-pressed',String(Boolean(current?.pinned)));
      $('[data-follow]').hidden=mode!=='live';
      $('[data-follow] span:not(.material-symbols-rounded)').textContent=follow?'Pause display':`Resume live${current&&current.lines>snapshotLines?' · '+(current.lines-snapshotLines)+' new':''}`;
      $('[data-follow] .material-symbols-rounded').textContent=follow?'pause':'play_arrow';
      $('[data-count]').textContent=`${total} ${filtered()?'matching':'retained'} records${current?.truncated?' · Incomplete':''}`;
      $('[data-page]').textContent=`${Math.floor(offset/500)+1} / ${Math.max(1,Math.ceil(total/500))}`;
      $('[data-previous]').disabled=offset===0;$('[data-next]').disabled=offset+500>=total;
      $('[data-prev-match]').disabled=$('[data-next-match]').disabled=!query||!rows.length;
      $('[data-pair-controls]').hidden=channel!=='sync';$('[data-all-runs]').hidden=!runId;$('[data-latest]').disabled=!latestRunId;
      $('.logs-storage-cleanup').hidden=Boolean(pairId||runId);
      $('[data-ai-logs]').hidden=!current;
      $('[data-ai-logs]').dataset.aiQuestion=current?`Analyze the ${channel} log session ${current.id} (${current.label}, started ${stamp(current.started)}). Explain any errors.`:'';
      renderSessions();
    }
    function lineMarkup(row,context=false) {
      const tone=['ERROR','WARN'].includes(row.level)?row.level.toLowerCase():'';
      return `<div class="logs-line logs-${tone}${context?' logs-context-line':''}" ${context?'':`data-line="${Number(row.id)}"`}><pre class="logs-message">${highlight(row.text,query)}</pre>${context||!filtered()?'':`<button type="button" data-context="${Number(row.id)}" aria-label="Show surrounding records for ${Number(row.id)}">${icon('unfold_more')}</button>`}</div>`;
    }
    async function api(path,values={},options={}) {
      const response=await fetch(url(path,values),{cache:'no-store',signal:controller?.signal,...options});
      if(!response.ok){let message='Could not load logs.';try{message=(await response.json()).detail||message;}catch{}throw new Error(message);}
      return response.json();
    }
    async function refresh() {
      if(!alive)return;if(busy){queued=true;return;}busy=true;
      const token=generation;controller=new AbortController();
      try {
        let data=await api('',{channel,...scope()});if(!alive||token!==generation)return;
        const available=data.channels||['sync'];
        host.querySelectorAll('[data-channel]').forEach(b=>{b.hidden=!available.includes(b.dataset.channel);});
        if(!available.includes(channel)){channel='sync';pairId=runId='';reset();return;}
        pairs=data.pairs||[];latestRunId=data.latest_run_id||'';
        const options='<option value="">All pairs</option>'+pairs.map(p=>`<option value="${esc(p.id)}">${esc(p.label)}</option>`).join('');
        if($('[data-pair]').innerHTML!==options){$('[data-pair]').innerHTML=options;$('[data-pair]').value=pairId;}
        if(latestRequested){latestRequested=false;runId=latestRunId;if(runId)data=await api('',{channel,...scope()});if(!alive||token!==generation)return;}
        sessions=data.items||[];
        paintStorage(data);
        if(mode==='live'&&follow){const next=sessions.find(s=>s.ended==null);if(next?.id!==sid){sid=next?.id||'';offset=0;rows=[];refreshLines=true;}}
        else if(!sid)sid=listing()[0]?.id||'';
        current=sessions.find(s=>s.id===sid)||null;
        if(!current){rows=[];total=0;setOutput(empty(sid?'Log unavailable':mode==='live'?'Waiting for activity':'No saved logs yet',sid?'This log may have expired, been deleted, or be unavailable for this profile.':'New sessions will appear here automatically.'));$('[data-connection]').textContent='Connected';paint();return;}
        if(mode==='live'&&!follow&&frozen&&!refreshLines){$('[data-connection]').textContent='Display paused · recording continues';paint();return;}
        let page=await api(`/${encodeURIComponent(sid)}/lines`,{q:query,level,provider,offset,limit:500,...scope()});if(!alive||token!==generation)return;
        const nextOffset=follow?Math.max(0,Math.floor((page.total-1)/500)*500):Math.min(offset,Math.max(0,Math.floor((page.total-1)/500)*500));
        if(offset!==nextOffset){offset=nextOffset;page=await api(`/${encodeURIComponent(sid)}/lines`,{q:query,level,provider,offset,limit:500,...scope()});if(!alive||token!==generation)return;}
        current=page.session;total=page.total;
        if(JSON.stringify(rows)!==JSON.stringify(page.items)||refreshLines){rows=page.items;setOutput(rows.map(r=>lineMarkup(r)).join('')||empty('No matching records','Clear the text, severity or provider filter.'));if(follow)content.scrollTop=content.scrollHeight;}
        snapshotLines=current.lines;frozen=true;refreshLines=false;
        $('#log-providers').innerHTML=[...new Set(rows.map(r=>r.provider))].sort().map(p=>`<option value="${esc(p)}"></option>`).join('');
        $('[data-connection]').textContent=mode==='live'?(follow?'Following live · updates every 2s':'Display paused · recording continues'):'Saved on this server';
        notice(current.truncated?'Storage limit reached: this session is incomplete. Exports contain only retained records.':'');
        paint();writeRoute();
      } catch(error){if(alive&&token===generation&&error.name!=='AbortError'){notice(error.message);$('[data-connection]').textContent='Disconnected · retrying';}}
      finally{busy=false;if(queued&&alive){queued=false;clearTimeout(searchTimer);searchTimer=setTimeout(refresh,0);}}
    }
    function invalidate() {generation++;controller?.abort();refreshLines=true;matchIndex=-1;clearTimeout(searchTimer);searchTimer=setTimeout(refresh,120);}
    function reset(clear=true) {sid='';sessions=[];current=null;offset=0;total=0;rows=[];frozen=false;listPage=0;if(clear){query=level=provider='';for(const s of ['[data-query]','[data-level]','[data-provider]'])$(s).value='';}notice('');setOutput(empty('Loading logs…',''));invalidate();paint();writeRoute();}
    function copied(button,full) {
      $('[data-copy-status]').textContent=full?'Full retained session copied, including records hidden by filters.':'Filtered log copied.';
      button.classList.add('logs-copied');const timer=setTimeout(()=>{button.classList.remove('logs-copied');feedbackTimers.delete(timer);},1800);feedbackTimers.add(timer);
    }
    async function onClick(event) {
      const button=event.target.closest('button');if(!button)return;
      try {
        if(button.hasAttribute('data-storage-toggle')){const open=button.getAttribute('aria-expanded')!=='true';button.setAttribute('aria-expanded',String(open));button.querySelector('.material-symbols-rounded').textContent=open?'expand_less':'expand_more';$('#logs-storage-panel').hidden=!open;if(open)$('#logs-storage-panel').scrollIntoView({block:'nearest'});}
        else if(button.dataset.channel){channel=button.dataset.channel;pairId=runId='';latestRequested=false;listQuery=listFilter='';$('[data-session-query]').value=$('[data-session-filter]').value='';reset();}
        else if(button.dataset.mode){mode=button.dataset.mode;follow=mode==='live';reset(false);}
        else if(button.dataset.session){sid=button.dataset.session;current=sessions.find(s=>s.id===sid)||null;mode='saved';follow=false;offset=0;total=0;rows=[];frozen=false;notice('');setOutput(empty('Loading logs…',''));invalidate();paint();writeRoute();}
        else if(button.hasAttribute('data-latest')){mode='saved';follow=false;latestRequested=true;runId='';reset();}
        else if(button.hasAttribute('data-all-runs')){runId='';reset();}
        else if(button.hasAttribute('data-follow')){follow=!follow;if(!follow){generation++;controller?.abort();}if(follow){frozen=false;invalidate();}paint();}
        else if(button.hasAttribute('data-list-prev')||button.hasAttribute('data-list-next')){listPage+=button.hasAttribute('data-list-next')?1:-1;renderSessions();}
        else if(button.hasAttribute('data-previous')||button.hasAttribute('data-next')){offset=Math.max(0,offset+(button.hasAttribute('data-next')?500:-500));follow=false;invalidate();content.scrollTop=0;}
        else if(button.hasAttribute('data-prev-match')||button.hasAttribute('data-next-match')){const step=button.hasAttribute('data-next-match')?1:-1;matchIndex=(matchIndex+step+rows.length)%rows.length;host.querySelectorAll('.logs-current-match').forEach(el=>el.classList.remove('logs-current-match'));const line=host.querySelector(`[data-line="${Number(rows[matchIndex]?.id)}"]`);line?.classList.add('logs-current-match');line?.scrollIntoView({block:'nearest'});}
        else if(button.dataset.context){const line=button.closest('[data-line]');if(line.nextElementSibling?.classList.contains('logs-context')){line.nextElementSibling.remove();return;}const token=generation,target=sid;const result=await api(`/${encodeURIComponent(sid)}/context/${button.dataset.context}`,scope());if(!alive||token!==generation||sid!==target||!line.isConnected)return;const el=document.createElement('div');el.className='logs-context';el.innerHTML=result.items.map(r=>lineMarkup(r,true)).join('');line.after(el);}
        else if(button.hasAttribute('data-pin')){await api(`/${encodeURIComponent(sid)}`,{},{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({pinned:!current.pinned})});invalidate();}
        else if(button.hasAttribute('data-delete')){if(!(await (window.CW?.MobileUI?.ask || window.confirm)('Delete this entire saved session, including any other pairs in it?')))return;await api(`/${encodeURIComponent(sid)}`,{},{method:'DELETE'});reset(false);}
        else if(button.hasAttribute('data-clear')){if(!(await (window.CW?.MobileUI?.ask || window.confirm)('Delete all finished, unpinned log sessions in this source for the current profile? Active and pinned sessions will be kept.')))return;await api('',{channel},{method:'DELETE'});reset(false);}
        else if(['data-copy','data-copy-full','data-download','data-download-full'].some(a=>button.hasAttribute(a))){
          if(!sid)return;const full=button.hasAttribute('data-copy-full')||button.hasAttribute('data-download-full'),target=sid;
          const downloadUrl=url(`/${encodeURIComponent(target)}/download`,{raw:'true',header:String($('[data-report-header]').checked),...(full?{}:{q:query,level,provider,...scope()})});
          const load=fetch(downloadUrl,{cache:'no-store'}).then(res=>{if(!res.ok)throw new Error('Could not export this log.');return res.text();});
          if(button.hasAttribute('data-copy')||button.hasAttribute('data-copy-full')){const ok=window.cwCopyTextFrom?await window.cwCopyTextFrom(load):await load.then(text=>navigator.clipboard.writeText(text).then(()=>true));if(alive){if(ok)copied(button,full);else notice('Could not copy. Download the log or select the text instead.');}}
          else{const text=await load;if(!alive)return;const object=URL.createObjectURL(new Blob([text],{type:'text/plain'})),link=document.createElement('a');link.href=object;link.download=`crosswatch-${channel}-${target}.log`;link.click();setTimeout(()=>URL.revokeObjectURL(object),1000);}
        }
      }catch(error){if(alive)notice(error.message);}
    }
    host.addEventListener('click',onClick);
    $('[data-storage-limit]').addEventListener('input',()=>{storageDirty=true;$('[data-storage-status]').textContent='';$('[data-save-storage]').disabled=!canManageStorage||savingStorage;});
    $('[data-storage-form]').addEventListener('submit',async event=>{
      event.preventDefault();
      if(!canManageStorage||savingStorage||!event.target.reportValidity())return;
      savingStorage=true;$('[data-save-storage]').disabled=true;$('[data-storage-limit]').disabled=true;$('[data-storage-status]').textContent='Saving…';
      try {
        const data=await api('/settings',{},{method:'PUT',signal:undefined,headers:{'Content-Type':'application/json'},body:JSON.stringify({max_mb:Number($('[data-storage-limit]').value)})});
        if(!alive)return;
        storageDirty=false;paintStorage(data);$('[data-storage-status]').textContent='Storage limit saved.';invalidate();
      } catch(error){if(alive)$('[data-storage-status]').textContent=error.message;}
      finally{savingStorage=false;if(alive){$('[data-storage-limit]').disabled=false;$('[data-save-storage]').disabled=!canManageStorage||!storageDirty;}}
    });
    $('[data-session-query]').addEventListener('input',()=>{listQuery=$('[data-session-query]').value.toLowerCase().trim();listPage=0;renderSessions();});
    $('[data-session-filter]').addEventListener('change',()=>{listFilter=$('[data-session-filter]').value;listPage=0;renderSessions();});
    $('[data-pair]').addEventListener('change',()=>{pairId=$('[data-pair]').value;runId='';reset();});
    for(const selector of ['[data-query]','[data-level]','[data-provider]'])$(selector).addEventListener('input',()=>{query=$('[data-query]').value;level=$('[data-level]').value;provider=$('[data-provider]').value.trim().toUpperCase();offset=0;follow=false;content.scrollTop=0;invalidate();paint();});
    $('[data-wrap]').addEventListener('change',()=>content.classList.toggle('logs-nowrap',!$('[data-wrap]').checked));
    content.addEventListener('wheel',event=>{if(event.deltaY<0&&follow){follow=false;paint();}},{passive:true});
    content.addEventListener('scroll',()=>{if(follow&&content.scrollHeight-content.scrollTop-content.clientHeight>50){follow=false;paint();}},{passive:true});
    timer=setInterval(()=>{if(!busy&&!document.hidden)refresh();},2000);
    dispose=()=>{alive=false;generation++;controller?.abort();clearInterval(timer);clearTimeout(searchTimer);for(const id of feedbackTimers)clearTimeout(id);host.removeEventListener('click',onClick);};
    paint();await refresh();
  },
  hide(){dispose?.();dispose=null;},
  unmount(){this.hide();root?.replaceChildren();root=null;}
};
window.LogsPage=LogsPage;
for(const name of ['cw:overview-profile-changed','cw:auth-state-changed'])window.addEventListener(name,()=>{const host=root;LogsPage.unmount();if(host&&document.documentElement.dataset.tab==='logs')LogsPage.mount(host);});
export default LogsPage;
