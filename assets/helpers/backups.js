/* assets/helpers/backups.js */
/* CrossWatch - Application, tracker and capture backup interface */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const s = { plan: null, draft: null, schedule: {}, backups: [], jobs: [], view: 'backups', choosing: false, page: 0, remotePage: 0, remote: [], section: '', restore: null, message: '', error: false, busy: false, timer: null, opener: null, stamp: '' };
  const $ = id => document.getElementById(id);
  const names = { application: 'CrossWatch application', trackers: 'CW trackers', captures: 'Saved captures' };
  const recoverySections = { mappings: 'Mappings and blocks only', anime_rules: 'Custom anime rules only', captures: 'Saved captures only' };
  const legacy = { config_only: 'Settings and connections', app_state: 'Application data, trackers excluded', full: 'Legacy full backup' };
  const stages = { queued: 'Waiting', creating: 'Creating local backup', validating: 'Checking local backup', encrypting: 'Encrypting archive', uploading: 'Uploading destination copy', verifying: 'Verifying destination copy', complete: 'Complete', failed: 'Failed' };

  function el(tag, props = {}, children = []) {
    const n = document.createElement(tag);
    Object.entries(props).forEach(([k,v]) => {
      if (v === undefined || v === null || v === false) return;
      if (k === 'text') n.textContent = v;
      else if (k === 'class') n.className = v;
      else if (k === 'on') Object.entries(v).forEach(([event,fn]) => n.addEventListener(event,fn));
      else if (['value','checked','disabled'].includes(k)) n[k] = v;
      else n.setAttribute(k,v);
    });
    [].concat(children).forEach(c => { if (c != null) n.append(typeof c === 'string' ? document.createTextNode(c) : c); });
    return n;
  }
  const text = (v, cls = 'br-muted') => el('p',{class:cls,text:v});
  const title = v => el('h3',{text:v});
  const row = nodes => el('div',{class:'br-row'},nodes);
  const actions = nodes => el('div',{class:'br-actions'},nodes);
  const button = (label,fn,primary = false) => el('button',{type:'button',class:`cw-page-button br-button${primary?' primary':''}`,disabled:s.busy,on:{click:fn}},[label]);
  const field = (label,control) => {control.setAttribute('aria-label',label);return el('label',{class:'br-field'},[el('span',{text:label}),control]);};
  const input = (value,fn,type = 'text',props = {}) => el('input',{class:'cw-page-input br-input',type,value,...props,on:{input:e=>fn(e.target.value)}});
  const check = (label,checked,fn) => el('label',{class:'br-check'},[el('input',{type:'checkbox',checked,'aria-label':label,on:{change:e=>fn(e.target.checked)}}),el('span',{text:label})]);
  const bytes = n => {const size=Number(n||0);return size<1024?`${size} B`:size<1048576?`${(size/1024).toFixed(1)} KB`:`${(size/1048576).toFixed(1)} MB`;};
  const pageSize = 5;
  const date = v => v ? new Date(typeof v === 'number' ? v*1000 : v).toLocaleString() : 'Not yet';
  const contents = b => Array.isArray(b.components) ? b.components.map(c=>names[c]||c).join(' + ') : legacy[b.scope] || 'Imported archive';
  function select(options,value,fn) { const n=el('select',{class:'cw-page-input br-input',on:{change:e=>fn(e.target.value)}},options.map(([v,label])=>el('option',{value:v,text:label}))); n.value=String(value); return n; }
  async function api(path,body) {
    const r=await fetch(`/api/backups/${path}`,body===undefined?{cache:'no-store'}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const data=await r.json(); if(!r.ok||data.ok===false) throw new Error(data.error||'Backup request failed.'); return data;
  }
  function message(value,error=false) { s.message=value; s.error=error; const n=$('br-message'); if(n){n.textContent=value;n.hidden=!value;n.classList.toggle('is-error',error);} }
  async function perform(fn) {
    if(s.busy)return;
    const active=document.activeElement,modal=$('cw-backups-modal');s.busy=true;modal?.setAttribute('aria-busy','true');
    modal?.querySelectorAll('button').forEach(n=>{n.disabled=true;});
    try{await fn();}catch(e){message(e.message||String(e),true);}
    finally{s.busy=false;modal?.setAttribute('aria-busy','false');modal?.querySelectorAll('button').forEach(n=>{n.disabled=n.dataset.locked==='true';});if(modal&&!modal.contains(document.activeElement))(active?.isConnected?active:modal.querySelector('button'))?.focus({preventScroll:true});}
  }
  function shell() {
    if($('cw-backups-modal'))return;
    const modal=el('div',{id:'cw-backups-modal',class:'cw-page-backups hidden'});
    modal.append(el('section',{class:'br-dialog',role:'dialog','aria-modal':'true','aria-labelledby':'br-title'},[
      el('header',{class:'br-head'},[el('div',{},[el('h2',{id:'br-title',text:'Backup & Restore'}),text('Protect your application, trackers and saved captures.')]),button('Close',close)]),
      el('div',{id:'br-body',class:'br-body cw-scrollbars'})]));
    ['input','change'].forEach(event=>modal.addEventListener(event,e=>e.stopPropagation()));
    modal.addEventListener('keydown',e=>{
      if(e.key==='Escape'){e.stopPropagation();close();}
      if(e.key==='Tab'){
        const nodes=[...modal.querySelectorAll('button:not(:disabled),input:not(:disabled),select:not(:disabled),summary')].filter(n=>n.getClientRects().length);
        const first=nodes[0],last=nodes[nodes.length-1];
        if(e.shiftKey&&document.activeElement===first){e.preventDefault();last?.focus();}
        else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first?.focus();}
      }
    });
    document.body.append(modal);
  }
  function selection() {
    const d=s.draft, summary=d.components.map(key=>key==='trackers'&&d.trackers!==null?`${d.trackers.length} selected tracker${d.trackers.length===1?'':'s'}`:names[key]).join(' \u00b7 ')||'Choose backup contents';
    const block=el('section',{class:'br-section'},[row([el('div',{class:'br-content-summary'},[title('What to back up'),text(summary)]),button(s.choosing?'Done':'Choose contents',()=>{s.choosing=!s.choosing;render();})])]);
    if(s.choosing){
      const options=el('div',{class:'br-content-options'});
      Object.entries(names).forEach(([key,label])=>{
        const option=el('div',{class:'cw-page-panel br-content-option'},[check(label,d.components.includes(key),yes=>{d.components=yes?[...new Set([...d.components,key])]:d.components.filter(v=>v!==key);render();})]);
        if(key==='application'){
          option.append(text('Settings, connections, sync state, mappings, blocks and custom anime rules.'));
          if(d.components.includes(key)&&s.plan.key_source==='external')option.append(text('Keep your external configuration key separately.'));
          if(d.components.includes(key)&&s.plan.key_source==='included')option.append(text('Configuration key included.'));
        }
        if(key==='trackers'&&d.components.includes(key)){
          option.append(check('All trackers',d.trackers===null,all=>{d.trackers=all?null:s.plan.available_trackers.map(t=>t.id);render();}));
          if(d.trackers!==null)option.append(el('div',{class:'br-tracker-options cw-scrollbars'},s.plan.available_trackers.map(t=>check(t.label,d.trackers.includes(t.id),yes=>{d.trackers=yes?[...d.trackers,t.id]:d.trackers.filter(id=>id!==t.id);}))));
        }
        if(key==='captures')option.append(text('Saved capture files. Restoring them does not apply them to providers.'));
        options.append(option);
      });
      block.append(options);
    }
    block.append(row([text(`${s.schedule.active?'Automatic: '+s.schedule.at:'Automatic backups off'} \u00b7 ${d.encrypt_local?'Encrypted local backups':'Local ZIP backups'}`),button('Back up now',()=>perform(async()=>{await savePlan();await api('create',{managed:true});s.choosing=false;s.page=0;render();message('Backup queued. You can close this dialog while it runs.');await refreshJobs();}),true)]));
    return block;
  }
  function encryptionFields() {
    const d=s.draft;
    return [field(s.plan.has_password?'New backup password (leave empty to keep current)':'Backup password or recovery key',input(d.password||'',v=>{d.password=v;},'password',{autocomplete:'new-password'})),
      button('Generate recovery key',()=>perform(async()=>{d.password=(await api('recovery-key',{})).key;d.password_saved=false;render();})),
      ...(d.password?[field('Save this outside CW',input(d.password,()=>{},'text',{readonly:true})),check('I saved this password or key outside CW',d.password_saved,v=>{d.password_saved=v;})]:[check('I saved any new password outside CW',d.password_saved,v=>{d.password_saved=v;})]),
      text('This protects the entire archive. You need it after losing this CW installation.')];
  }
  function scheduleForm() {
    const d=s.draft;
    return el('div',{class:'br-expand'},[check('Back up automatically',!!d.active,v=>{d.active=v;}),el('div',{class:'br-fields'},[
      field('Time',input(d.at,v=>{d.at=v;},'time')),field('Automatic local copies to keep',select([7,14,30,60].map(n=>[String(n),`Latest ${n}`]),d.max_backups,v=>{d.max_backups=Number(v);}))]),
      text('Uses the CW scheduling timezone. Manual and safety backups are kept separately.'),
      el('details',{},[el('summary',{text:'Days'}),el('div',{class:'br-days'},['Mon','Tue','Wed','Thu','Fri','Sat','Sun'].map((day,i)=>check(day,d.days.includes(i+1),yes=>{d.days=yes?[...d.days,i+1]:d.days.filter(n=>n!==i+1);})))]),
      actions([button('Save settings',()=>perform(async()=>{if(d.active&&(!d.at||!d.days.length))throw new Error('Choose a time and at least one day.');await savePlan();s.schedule=(await api('schedule',{enabled:d.active,at:d.at,days:d.days,max_backups:d.max_backups,retention_days:0,auto_delete_old:true,managed:true})).schedule;s.section='';render();message('Backup settings saved.');}),true)])]);
  }
  function encryptionForm() {
    const d=s.draft;
    return el('div',{class:'br-expand'},[check('Encrypt local backups',d.encrypt_local,v=>{d.encrypt_local=v;render();}),
      ...(d.encrypt_local||d.destination.kind!=='none'||s.plan.has_password?encryptionFields():[]),
      button('Save encryption',()=>perform(async()=>{await savePlan();s.section='';render();message('Encryption settings saved.');}),true)]);
  }
  function destinationForm() {
    const d=s.draft, dest=d.destination;
    const block=el('div',{class:'br-expand'},[field('Destination',select([['none','None'],['webdav','Nextcloud / WebDAV'],['folder','Mounted folder (advanced)']],dest.kind,kind=>{d.destination={kind,url:'',username:'',password:'',folder:'CrossWatch',vendor:'nextcloud',path:''};render();}))]);
    if(dest.kind!=='none'&&!s.plan.rclone_available)block.append(text(s.plan.rclone_error||'rclone is unavailable. Use the CW image with backup support or install rclone.','br-error'));
    if(dest.kind==='webdav')block.append(field('Server type',select([['nextcloud','Nextcloud'],['owncloud','ownCloud'],['other','Other WebDAV']],dest.vendor||'nextcloud',v=>{dest.vendor=v;})),field('WebDAV URL',input(dest.url||'',v=>{dest.url=v;},'url',{placeholder:'https://cloud.example.com/remote.php/dav/files/user/'})),
      el('div',{class:'br-fields'},[field('Username',input(dest.username||'',v=>{dest.username=v;})),field(dest.has_password?'App password (leave empty to keep)':'App password',input(dest.password||'',v=>{dest.password=v;},'password',{autocomplete:'new-password'}))]),field('Backup folder',input(dest.folder||'CrossWatch',v=>{dest.folder=v;})),check('Allow HTTP for a trusted local server',!!dest.allow_http,v=>{dest.allow_http=v;}));
    if(dest.kind==='folder')block.append(field('Mounted folder inside CW',input(dest.path||'',v=>{dest.path=v;},'text',{placeholder:s.plan.mounted_root||'/backups'})),text(`Use ${s.plan.mounted_root||'/backups'} or a folder inside it. Mount it into the container, outside /config.`));
    if(dest.kind!=='none')block.append(...(s.plan.has_password?[text('Copies use your saved backup password.')]:encryptionFields()),field('Destination copies to keep',select([7,14,30,60].map(n=>[String(n),`Latest ${n}`]),d.remote_keep,v=>{d.remote_keep=Number(v);})),text('Copies are encrypted before transfer.'));
    block.append(actions([button('Save destination',()=>perform(async()=>{await savePlan();s.section='';render();message('Destination saved. Test the connection before relying on these copies.');}),true),...(dest.kind!=='none'?[button('Test connection',()=>perform(async()=>{await savePlan();message('Checking upload, download and checksum.');message((await api('destination/test',{})).message);}))]:[])]));
    return block;
  }
  async function savePlan() {
    const d=s.draft;
    s.plan=(await api('plan',{components:d.components,trackers:d.trackers,encrypt_local:!!d.encrypt_local,password:d.password||'',password_saved:!!d.password_saved,destination:d.destination,remote_keep:d.remote_keep})).plan;
    d.password='';d.password_saved=false;d.destination=structuredClone(s.plan.destination);
  }
  function renderStatus() {
    const slot=$('br-status');if(!slot)return;
    const job=s.jobs[0],nodes=[];
    if(job){nodes.push(title(job.stage==='failed'&&job.local_status==='saved'?'Local backup saved, destination copy failed':stages[job.stage]||job.stage),text(`Local copy: ${job.local_status==='saved'?'Saved '+date(job.local_saved_at):job.local_status}`));
      if(job.remote_status!=='disabled')nodes.push(text(`Destination copy: ${job.remote_status}${job.remote_verified_at?' '+date(job.remote_verified_at):''}`));
      if(job.stage==='uploading')nodes.push(el('progress',{max:Math.max(job.total_bytes||0,1),value:job.bytes||0,'aria-label':'Upload progress'}),text(`${bytes(job.bytes)} of ${bytes(job.total_bytes)}`));
      if(job.error)nodes.push(text(job.error,'br-error'));if(job.warning)nodes.push(text(job.warning));if(job.stage==='failed')nodes.push(text(job.retry_at?`Automatic retry: ${date(job.retry_at)}`:'Automatic retries stopped. Check the destination before retrying.'),button('Retry',()=>perform(async()=>{await api('retry',{id:job.id});await refreshJobs();})));
    }
    const verified=s.jobs.find(j=>j.remote_status==='verified');if(verified&&verified.id!==job?.id)nodes.push(text(`Last verified destination copy: ${date(verified.remote_verified_at)}`));slot.hidden=!nodes.length;slot.replaceChildren(...nodes);
  }
  function pagination(total,key,draw) {
    const pages=Math.max(1,Math.ceil(total/pageSize));s[key]=Math.min(s[key],pages-1);
    const control=(label,step,locked)=>{const n=button(label,()=>{s[key]+=step;draw();document.querySelector(`[data-pager="${key}"] button:not(:disabled)`)?.focus({preventScroll:true});});n.dataset.locked=String(locked);n.disabled=s.busy||locked;return n;};
    return el('nav',{class:'br-row br-pagination','aria-label':key==='page'?'Backup pages':'Destination backup pages','data-pager':key},[
      text(`${s[key]*pageSize+1}-${Math.min((s[key]+1)*pageSize,total)} of ${total}`),
      actions([control('Previous',-1,s[key]===0),control('Next',1,s[key]>=pages-1)])]);
  }
  function renderHistory() {
    const slot=$('br-history');if(!slot)return;
    const expanded=new Set([...slot.querySelectorAll('details[open]')].map(n=>n.dataset.path));
    slot.replaceChildren();
    if(!s.backups.length){s.page=0;slot.append(text('No saved backups yet. Create one above or restore from a file.'));return;}
    const pager=pagination(s.backups.length,'page',renderHistory);
    s.backups.slice(s.page*pageSize,(s.page+1)*pageSize).forEach(b=>{
      const job=s.jobs.find(j=>j.path===b.path);
      const details=el('details',{'data-path':b.path,open:expanded.has(b.path)},[el('summary',{text:'Details'}),el('div',{class:'br-backup-details'},[
        text(b.path),text(`${b.encrypted?'Encrypted archive':'ZIP archive'} \u00b7 ${job?.remote_status==='verified'?'Destination copy verified':'Local copy'} \u00b7 ${b.trigger==='scheduler'?'Automatic retention':'Kept until deleted'}`),actions([
          button('Download',()=>{window.location.href=`/api/backups/download?path=${encodeURIComponent(b.path)}`;}),
          button('Delete',()=>{if(window.confirm('Delete this local backup? Destination copies are kept separately.'))perform(async()=>{await api('delete',{path:b.path});await refreshList();});})])])]);
      slot.append(el('article',{class:'br-history-row'},[row([el('div',{class:'br-backup-info'},[el('strong',{text:date(b.created_at||b.mtime)}),text(`${contents(b)} \u00b7 ${bytes(b.size)}`)]),button('Restore',()=>{s.restore={path:b.path,password:'',validation:null,mode:'replace',instance:'',label:'Recovered tracker'};render();})]),details]));
    });
    slot.append(pager);
  }
  function restoreView() {
    const r=s.restore;
    const section=el('section',{},[button('Back to backups',()=>{s.restore=null;render();}),title('Review restoration'),text(r.path),field('Backup password, if encrypted',input(r.password,v=>{r.password=v;},'password',{autocomplete:'off'})),button('Check backup',()=>perform(async()=>{r.validation=(await api('validate',{path:r.path,password:r.password})).validation;r.mode=r.validation.manifest.trackers?.length?'new_tracker':r.validation.available_sections?.[0]||'replace';r.instance=r.validation.manifest.trackers?.[0]?.id||'';r.confirm=false;render();}))]);
    if(r.validation){const manifest=r.validation.manifest;section.append(text('Archive checksums and database integrity checked.'),text(contents(manifest)));
      const options=(r.validation.available_sections||[]).filter(key=>recoverySections[key]).map(key=>[key,recoverySections[key]]);
      if(manifest.trackers?.length)options.unshift(['new_tracker','Recover one tracker as a new instance']);
      options.push(['replace','Replace the data in this backup']);
      section.append(field('Restore action',select(options,r.mode,v=>{r.mode=v;r.confirm=false;render();})));
      if(r.mode==='replace'&&r.validation.application_error)section.append(text(r.validation.application_error,'br-error'));
      if(r.mode==='new_tracker')section.append(field('Tracker',select(manifest.trackers.map(t=>[t.id,t.label||t.id]),r.instance,v=>{r.instance=v;})),field('New tracker name',input(r.label,v=>{r.label=v;})),text('No sync pairs are created. Your existing tracker stays in place.'));
      if(r.mode==='replace')section.append(text('CW saves a safety copy and applies the replacement during restart. Captures are restored as files only. No captured data is sent to providers.'),check('Replace the data contained in this backup',!!r.confirm,v=>{r.confirm=v;}));
      if(recoverySections[r.mode])section.append(text(r.mode==='captures'?'Restores saved capture files and replaces files with the same names.':'Replaces the selected rules with those from this backup.'),text('CW saves a safety copy and restores your selection during restart.'),check('Restore only the selected data',!!r.confirm,v=>{r.confirm=v;}));
      section.append(button(r.mode==='new_tracker'?'Recover tracker':'Restore and restart',()=>perform(async()=>{if(r.mode==='replace'&&r.validation.application_ready===false)throw new Error(r.validation.application_error);if(r.mode!=='new_tracker'&&!r.confirm)throw new Error('Confirm replacement before restoring.');const result=await api('restore',{path:r.path,password:r.password,mode:r.mode,instance:r.instance,label:r.label,confirm:!!r.confirm,restart:true});if(r.mode==='new_tracker'){s.restore=null;render();message(result.result.message);}else message('Restore prepared. CrossWatch is restarting. Reopen CW to see the result.');}),true));}
    return section;
  }
  function importView() {
    let password='';const file=el('input',{type:'file',accept:'.zip,.cwbackup',class:'br-input','aria-label':'Backup archive'});
    return el('div',{class:'br-expand'},[field('Backup archive',file),field('Backup password, if encrypted',input('',v=>{password=v;},'password',{autocomplete:'off'})),button('Import and review',()=>perform(async()=>{if(!file.files?.length)throw new Error('Choose a backup archive.');const form=new FormData();form.append('file',file.files[0]);form.append('password',password);const r=await fetch('/api/backups/upload',{method:'POST',body:form});const data=await r.json();if(!r.ok||data.ok===false)throw new Error(data.error||'Could not import this archive.');s.restore={path:data.backup.path,password,validation:null,mode:'replace',instance:'',label:'Recovered tracker'};s.section='';await refreshList();render();}),true)]);
  }
  async function browseRemote() {
    message('Loading destination backups.');s.remote=(await api('remote')).backups;s.remotePage=0;s.section='remote';render();message('');
  }
  function renderRemote() {
    const slot=$('br-remote');if(!slot)return;slot.replaceChildren();
    if(!s.remote.length){slot.append(text('No CrossWatch archives found at this destination.'));return;}
    const pager=pagination(s.remote.length,'remotePage',renderRemote);
    slot.append(...s.remote.slice(s.remotePage*pageSize,(s.remotePage+1)*pageSize).map(b=>el('article',{class:'br-history-row'},[row([el('div',{class:'br-backup-info'},[text(b.path),text(`${date(b.created_at)} \u00b7 ${bytes(b.size)}`)]),button('Download to CW',()=>perform(async()=>{message('Downloading the destination backup.');const result=await api('remote/download',{path:b.path});s.restore={path:result.backup.path,password:'',validation:null,mode:'replace',instance:'',label:'Recovered tracker'};await refreshList();render();}))])])),pager);
  }
  function render() {
    const previous=document.activeElement,focusName=previous?.getAttribute('aria-label')||(previous?.tagName==='BUTTON'?previous.textContent:null);
    queueMicrotask(()=>{if(previous?.isConnected||!focusName)return;[...document.querySelectorAll('#br-body input,#br-body select,#br-body button')].find(n=>(n.getAttribute('aria-label')||n.textContent)===focusName)?.focus({preventScroll:true});});
    const body=$('br-body');if(!body)return;const msg=el('div',{id:'br-message',class:s.error?'br-message is-error':'br-message',role:'status',hidden:!s.message,text:s.message});
    if(!s.plan){body.replaceChildren(msg,text('Loading backup settings...'));return;}if(s.restore){body.replaceChildren(msg,restoreView());return;}
    const dest={webdav:'Nextcloud / WebDAV',folder:'Mounted folder'}[s.plan.destination.kind]||'This server only';
    const nav=el('nav',{class:'br-actions','aria-label':'Backup views'},[['backups','Backups'],['settings','Settings']].map(([view,label])=>{
      const n=button(label,()=>{s.view=view;s.section='';render();body.scrollTop=0;});n.setAttribute('aria-pressed',String(s.view===view));return n;
    }));
    const section=(name,summary,key,label,form)=>el('section',{class:'br-section'},[row([el('div',{class:'br-content-summary'},[title(name),text(summary)]),button(s.section===key?'Close settings':label,()=>{s.section=s.section===key?'':key;render();})]),...(s.section===key?[form()]:[])]);
    if(s.view==='settings'){
      body.replaceChildren(msg,nav,
        section('Automatic backups',s.schedule.active?`At ${s.schedule.at} \u00b7 ${s.schedule.managed?'Uses your backup selections':'Existing schedule: '+(legacy[s.schedule.scope]||s.schedule.scope)}`:'Off','settings','Edit schedule',scheduleForm),
        section('Destination',dest,'destination','Edit destination',destinationForm),
        section('Encryption',s.plan.encrypt_local?'Local backups encrypted':'Local backups saved as ZIP','encryption','Edit encryption',encryptionForm),
        ...(s.plan.destination_notice?[text(s.plan.destination_notice)]:[]));
      return;
    }
    body.replaceChildren(msg,nav,selection(),el('section',{id:'br-status',class:'cw-page-panel br-status','aria-live':'polite',hidden:true}),
      ...(s.plan.destination_notice?[text(s.plan.destination_notice)]:[]),
      row([title('Saved backups'),button('Restore from file',()=>{s.section=s.section==='import'?'':'import';render();})]),
      ...(s.plan.destination.kind!=='none'?[actions([button('On this server',()=>{s.section='';render();},s.section!=='remote'),button('At destination',()=>perform(browseRemote),s.section==='remote')])]:[]),
      ...(s.section==='import'?[importView()]:[]),s.section==='remote'?el('div',{id:'br-remote'}):el('div',{id:'br-history'}));renderStatus();renderHistory();renderRemote();
  }
  async function refreshList(){s.backups=(await api('list')).backups||[];renderHistory();}
  async function refreshJobs(){const result=await api('status');s.jobs=result.jobs||[];renderStatus();const stamp=JSON.stringify(s.jobs.map(j=>[j.id,j.stage,j.local_status,j.remote_status]));if(stamp!==s.stamp){s.stamp=stamp;await refreshList();}if(result.restore&&!s.message)message(result.restore.message,!result.restore.ok);}
  async function refresh(){const [p,sch,l]=await Promise.all([api('plan'),api('schedule'),api('list')]);s.plan=p.plan;s.schedule=sch.schedule||{};s.backups=l.backups||[];s.draft={...structuredClone(p.plan),password:'',password_saved:false,active:!!s.schedule.active,at:s.schedule.at||'03:00',days:s.schedule.days?.length?[...s.schedule.days]:[1,2,3,4,5,6,7],max_backups:s.schedule.max_backups||7};render();await refreshJobs();}
  function open(){shell();s.opener=document.activeElement;s.message='';s.view='backups';s.choosing=false;s.page=0;s.remotePage=0;s.section='';s.restore=null;$('cw-backups-modal').classList.remove('hidden');document.body.classList.add('br-backups-open','cx-modal-open');render();refresh().catch(e=>message(e.message,true));$('cw-backups-modal').querySelector('button')?.focus();clearInterval(s.timer);s.timer=setInterval(()=>refreshJobs().catch(e=>message(e.message,true)),2500);}
  function close(){if(s.busy)return;clearInterval(s.timer);$('cw-backups-modal')?.classList.add('hidden');document.body.classList.remove('br-backups-open','cx-modal-open');s.draft=null;s.plan=null;s.restore=null;s.opener?.focus();}
  window.openBackupRestore=open;(window.CW||={}).Backups={open,close,refresh};
})();
