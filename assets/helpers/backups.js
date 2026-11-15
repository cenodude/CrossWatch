/* assets/helpers/backups.js */
/* CrossWatch - Application, tracker and capture backup interface */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const s = { plan: null, draft: null, schedule: {}, backups: [], jobs: [], section: '', restore: null, cloud: null, message: '', error: false, busy: false, timer: null, opener: null, stamp: '' };
  const $ = id => document.getElementById(id);
  const names = { application: 'CrossWatch application', trackers: 'CW trackers', captures: 'Saved captures' };
  const legacy = { config_only: 'Settings and connections', app_state: 'Application data, trackers excluded', full: 'Legacy full backup' };
  const stages = { queued: 'Waiting', creating: 'Creating local backup', validating: 'Checking local backup', encrypting: 'Encrypting archive', uploading: 'Uploading additional copy', verifying: 'Verifying additional copy', complete: 'Complete', failed: 'Failed' };

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
  const bytes = n => `${(Number(n || 0)/1024/1024).toFixed(1)} MB`;
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
    finally{s.busy=false;modal?.setAttribute('aria-busy','false');modal?.querySelectorAll('button').forEach(n=>{n.disabled=false;});if(modal&&!modal.contains(document.activeElement))(active?.isConnected?active:modal.querySelector('button'))?.focus({preventScroll:true});}
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
    const d=s.draft, block=el('section',{class:'br-section'},[title('What to back up')]);
    Object.entries(names).forEach(([key,label])=>{
      block.append(check(label,d.components.includes(key),yes=>{d.components=yes?[...new Set([...d.components,key])]:d.components.filter(v=>v!==key);render();}));
      if(key==='application'&&d.components.includes(key)){
        block.append(text('Settings, connections, sync state, mappings, blocks and custom anime rules.'));
        if(s.plan.key_source==='external')block.append(text('Uses an external configuration key. Keep that key separately for restoration.'));
        if(s.plan.key_source==='included')block.append(text('Configuration key included. This archive can recover saved connections.'));
      }
      if(key==='trackers'&&d.components.includes(key)){
        block.append(check('All trackers',d.trackers===null,all=>{d.trackers=all?null:s.plan.available_trackers.map(t=>t.id);render();}));
        if(d.trackers!==null)s.plan.available_trackers.forEach(t=>block.append(check(t.label,d.trackers.includes(t.id),yes=>{d.trackers=yes?[...d.trackers,t.id]:d.trackers.filter(id=>id!==t.id);})));
      }
      if(key==='captures'&&d.components.includes(key))block.append(text('Captures already saved in CW. This does not contact providers or apply captures to them.'));
    });
    block.append(actions([button('Back up now',()=>perform(async()=>{await savePlan();await api('create',{managed:true});message('Backup queued. You can close this dialog while it runs.');await refreshJobs();}),true)]));
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
      el('details',{},[el('summary',{text:'Days and encryption'}),el('div',{class:'br-days'},['Mon','Tue','Wed','Thu','Fri','Sat','Sun'].map((day,i)=>check(day,d.days.includes(i+1),yes=>{d.days=yes?[...d.days,i+1]:d.days.filter(n=>n!==i+1);}))),check('Encrypt local backups',d.encrypt_local,v=>{d.encrypt_local=v;}),...encryptionFields()]),
      actions([button('Save settings',()=>perform(async()=>{if(d.active&&(!d.at||!d.days.length))throw new Error('Choose a time and at least one day.');await savePlan();s.schedule=(await api('schedule',{enabled:d.active,at:d.at,days:d.days,max_backups:d.max_backups,retention_days:0,auto_delete_old:true,managed:true})).schedule;s.section='';render();message('Backup settings saved.');}),true)])]);
  }
  function destinationForm() {
    const d=s.draft, dest=d.destination;
    const block=el('div',{class:'br-expand'},[field('Additional copy',select([['none','None'],['onedrive','OneDrive'],['dropbox','Dropbox'],['webdav','Nextcloud / WebDAV'],['folder','Mounted folder (advanced)']],dest.kind,kind=>{s.cloud=null;d.destination={kind,url:'',username:'',password:'',folder:'CrossWatch',vendor:'nextcloud',path:''};render();}))]);
    if(dest.kind!=='none'&&!s.plan.rclone_available)block.append(text(s.plan.rclone_error||'rclone is unavailable. Use the CW image with backup support or install rclone.','br-error'));
    if(['onedrive','dropbox'].includes(dest.kind))block.append(cloudForm(dest),field('Backup folder',input(dest.folder||'CrossWatch',v=>{dest.folder=v;})));
    if(dest.kind==='webdav')block.append(field('Server type',select([['nextcloud','Nextcloud'],['owncloud','ownCloud'],['other','Other WebDAV']],dest.vendor||'nextcloud',v=>{dest.vendor=v;})),field('WebDAV URL',input(dest.url||'',v=>{dest.url=v;},'url',{placeholder:'https://cloud.example.com/remote.php/dav/files/user/'})),
      el('div',{class:'br-fields'},[field('Username',input(dest.username||'',v=>{dest.username=v;})),field(dest.has_password?'App password (leave empty to keep)':'App password',input(dest.password||'',v=>{dest.password=v;},'password',{autocomplete:'new-password'}))]),field('Backup folder',input(dest.folder||'CrossWatch',v=>{dest.folder=v;})),check('Allow HTTP for a trusted local server',!!dest.allow_http,v=>{dest.allow_http=v;}));
    if(dest.kind==='folder')block.append(field('Mounted folder inside CW',input(dest.path||'',v=>{dest.path=v;},'text',{placeholder:s.plan.mounted_root||'/backups'})),text(`Use ${s.plan.mounted_root||'/backups'} or a folder inside it. Mount it into the container, outside /config.`));
    if(dest.kind!=='none')block.append(...encryptionFields(),field('Automatic additional copies to keep',select([7,14,30,60].map(n=>[String(n),`Latest ${n}`]),d.remote_keep,v=>{d.remote_keep=Number(v);})),text('Additional copies are always encrypted before transfer.'));
    block.append(actions([button('Save destination',()=>perform(async()=>{await savePlan();s.section='';render();message('Destination saved. Test the connection before relying on additional copies.');}),true),...(dest.kind!=='none'?[button('Test connection',()=>perform(async()=>{await savePlan();message('Checking upload, download and checksum.');message((await api('destination/test',{})).message);}))]:[])]));
    return block;
  }
  function cloudForm(dest) {
    const provider=dest.kind==='onedrive'?'OneDrive':'Dropbox';
    const block=el('div',{class:'br-expand'},[text(dest.connection?'Account authorization saved.':'Connect your account once. CW keeps it connected for scheduled backups.'),
      text('Initial setup needs rclone on a computer with a browser. Afterwards, you can manage backups from any device.'),
      button(dest.connection?`Reconnect ${provider}`:`Connect ${provider}`,()=>perform(async()=>{s.cloud=(await api('cloud/connect',{kind:dest.kind})).connection;render();}))]);
    const c=s.cloud;if(!c||c.kind!==dest.kind)return block;
    const submit=answer=>perform(async()=>{c.answer='';const next=(await api('cloud/continue',{id:c.id,answer})).connection;if(next.status==='connected'){dest.connection=next.id;s.cloud=null;render();message('Account authorization saved. Save the destination and test the connection.');}else{s.cloud=next;render();}});
    if(c.question==='config_token')block.append(field('Run on your computer with rclone 1.76.0 or newer',input(`rclone authorize "${dest.kind}"`,()=>{},'text',{readonly:true})),
      el('a',{href:'https://rclone.org/remote_setup/',target:'_blank',rel:'noopener noreferrer',text:'rclone connection instructions'}),
      field('Authorization result',input('',v=>{c.answer=v;},'password',{autocomplete:'off',spellcheck:'false'})),button('Continue connection',()=>submit(c.answer||'')));
    if(c.question==='config_driveid'){c.answer=c.answer||c.choices?.[0]?.value||'';block.append(field('OneDrive account',select((c.choices||[]).map(choice=>[choice.value,choice.label]),c.answer,v=>{c.answer=v;})),button('Use this OneDrive',()=>submit(c.answer)));}
    if(c.question==='config_drive_ok')block.append(text('The selected OneDrive is available. Confirm it for your backups.'),button('Confirm OneDrive',()=>submit('true')));
    return block;
  }
  async function savePlan() {
    const d=s.draft;
    if(s.cloud?.kind===d.destination.kind)throw new Error('Finish connecting the cloud account before saving.');
    s.plan=(await api('plan',{components:d.components,trackers:d.trackers,encrypt_local:!!d.encrypt_local,password:d.password||'',password_saved:!!d.password_saved,destination:d.destination,remote_keep:d.remote_keep})).plan;
    d.password='';d.password_saved=false;d.destination=structuredClone(s.plan.destination);
  }
  function renderStatus() {
    const slot=$('br-status');if(!slot)return;
    const job=s.jobs[0],nodes=[];
    if(job){nodes.push(title(job.stage==='failed'&&job.local_status==='saved'?'Local backup saved, additional copy failed':stages[job.stage]||job.stage),text(`Local copy: ${job.local_status==='saved'?'Saved '+date(job.local_saved_at):job.local_status}`));
      if(job.remote_status!=='disabled')nodes.push(text(`Additional copy: ${job.remote_status}${job.remote_verified_at?' '+date(job.remote_verified_at):''}`));
      if(job.stage==='uploading')nodes.push(el('progress',{max:Math.max(job.total_bytes||0,1),value:job.bytes||0,'aria-label':'Upload progress'}),text(`${bytes(job.bytes)} of ${bytes(job.total_bytes)}`));
      if(job.error)nodes.push(text(job.error,'br-error'));if(job.warning)nodes.push(text(job.warning));if(job.stage==='failed')nodes.push(text(job.retry_at?`Automatic retry: ${date(job.retry_at)}`:'Automatic retries stopped. Check the destination before retrying.'),button('Retry',()=>perform(async()=>{await api('retry',{id:job.id});await refreshJobs();})));
    }else nodes.push(title(s.backups.length?'Saved backups are available':'No backups yet'),text('Create a backup or restore one from a file.'));
    const verified=s.jobs.find(j=>j.remote_status==='verified');if(verified&&verified.id!==job?.id)nodes.push(text(`Last verified additional copy: ${date(verified.remote_verified_at)}`));slot.replaceChildren(...nodes);
  }
  function renderHistory() {
    const slot=$('br-history');if(!slot)return;slot.replaceChildren();
    s.backups.forEach(b=>{const job=s.jobs.find(j=>j.path===b.path);slot.append(el('article',{class:'br-history-row'},[el('div',{},[el('strong',{text:date(b.created_at||b.mtime)}),text(contents(b)),text(`${bytes(b.size)} · ${b.encrypted?'Encrypted':'ZIP archive'} · ${job?.remote_status==='verified'?'Local and additional copy verified':'Local copy'} · ${b.trigger==='scheduler'?'Automatic':'Kept until deleted'}`)]),actions([
      button('Restore',()=>{s.restore={path:b.path,password:'',validation:null,mode:'replace',instance:'',label:'Recovered tracker'};render();}),button('Download',()=>{window.location.href=`/api/backups/download?path=${encodeURIComponent(b.path)}`;}),button('Delete',()=>{if(window.confirm('Delete this local backup? Additional copies are kept separately.'))perform(async()=>{await api('delete',{path:b.path});await refreshList();});})])]));});
    if(!s.backups.length)slot.append(text('Saved backups will appear here.'));
  }
  function restoreView() {
    const r=s.restore;
    const section=el('section',{},[button('Back to backups',()=>{s.restore=null;render();}),title('Review restoration'),text(r.path),field('Backup password, if encrypted',input(r.password,v=>{r.password=v;},'password',{autocomplete:'off'})),button('Check backup',()=>perform(async()=>{r.validation=(await api('validate',{path:r.path,password:r.password})).validation;r.mode=r.validation.manifest.trackers?.length?'new_tracker':'replace';r.instance=r.validation.manifest.trackers?.[0]?.id||'';render();}))]);
    if(r.validation){const manifest=r.validation.manifest;section.append(text('Archive checksums and database integrity checked.'),text(contents(manifest)));if(r.validation.application_error)section.append(text(r.validation.application_error,'br-error'));
      if(manifest.trackers?.length){section.append(field('Restore action',select([['new_tracker','Recover one tracker as a new instance'],['replace','Replace the data in this backup']],r.mode,v=>{r.mode=v;render();})));if(r.mode==='new_tracker')section.append(field('Tracker',select(manifest.trackers.map(t=>[t.id,t.label||t.id]),r.instance,v=>{r.instance=v;})),field('New tracker name',input(r.label,v=>{r.label=v;})),text('No sync pairs are created. Your existing tracker stays in place.'));}
      if(r.mode==='replace')section.append(text('CW saves a safety copy and applies the replacement during restart. Captures are restored as files only. No captured data is sent to providers.'),check('Replace the data contained in this backup',!!r.confirm,v=>{r.confirm=v;}));
      section.append(button(r.mode==='new_tracker'?'Recover tracker':'Restore and restart',()=>perform(async()=>{if(r.mode==='replace'&&r.validation.application_ready===false)throw new Error(r.validation.application_error);if(r.mode==='replace'&&!r.confirm)throw new Error('Confirm replacement before restoring.');const result=await api('restore',{path:r.path,password:r.password,mode:r.mode,instance:r.instance,label:r.label,confirm:!!r.confirm,restart:true});if(r.mode==='new_tracker'){s.restore=null;render();message(result.result.message);}else message('Restore prepared. CrossWatch is restarting. Reopen CW to see the result.');}),true));}
    return section;
  }
  function importView() {
    let password='';const file=el('input',{type:'file',accept:'.zip,.cwbackup',class:'br-input','aria-label':'Backup archive'});
    return el('div',{class:'br-expand'},[field('Backup archive',file),field('Backup password, if encrypted',input('',v=>{password=v;},'password',{autocomplete:'off'})),button('Import and review',()=>perform(async()=>{if(!file.files?.length)throw new Error('Choose a backup archive.');const form=new FormData();form.append('file',file.files[0]);form.append('password',password);const r=await fetch('/api/backups/upload',{method:'POST',body:form});const data=await r.json();if(!r.ok||data.ok===false)throw new Error(data.error||'Could not import this archive.');s.restore={path:data.backup.path,password,validation:null,mode:'replace',instance:'',label:'Recovered tracker'};s.section='';await refreshList();render();}),true)]);
  }
  async function browseRemote() {
    message('Loading additional copies.');const result=await api('remote');s.section='remote';render();const slot=$('br-remote');
    slot.replaceChildren(...result.backups.map(b=>row([el('div',{},[text(b.path),text(`${date(b.created_at)} · ${bytes(b.size)}`)]),button('Download to CW',()=>perform(async()=>{message('Downloading the additional copy.');const result=await api('remote/download',{path:b.path});s.restore={path:result.backup.path,password:'',validation:null,mode:'replace',instance:'',label:'Recovered tracker'};await refreshList();render();}))])));
    if(!result.backups.length)slot.append(text('No CrossWatch archives found at this destination.'));message('');
  }
  function render() {
    const previous=document.activeElement,focusName=previous?.getAttribute('aria-label')||(previous?.tagName==='BUTTON'?previous.textContent:null);
    queueMicrotask(()=>{if(previous?.isConnected||!focusName)return;[...document.querySelectorAll('#br-body input,#br-body select,#br-body button')].find(n=>(n.getAttribute('aria-label')||n.textContent)===focusName)?.focus({preventScroll:true});});
    const body=$('br-body');if(!body)return;const msg=el('div',{id:'br-message',class:s.error?'br-message is-error':'br-message',role:'status',hidden:!s.message,text:s.message});
    if(!s.plan){body.replaceChildren(msg,text('Loading backup settings...'));return;}if(s.restore){body.replaceChildren(msg,restoreView());return;}
    const dest={onedrive:'OneDrive',dropbox:'Dropbox',webdav:'Nextcloud / WebDAV',folder:'Mounted folder'}[s.plan.destination.kind]||'This server only';
    body.replaceChildren(msg,el('section',{id:'br-status',class:'cw-page-panel br-status','aria-live':'polite'}),selection(),
      el('section',{class:'br-section'},[row([el('div',{},[title('Automatic backups'),text(s.schedule.active?`At ${s.schedule.at} · ${s.schedule.managed?'Uses your backup selections':'Existing schedule: '+(legacy[s.schedule.scope]||s.schedule.scope)}`:'Off')]),button(s.section==='settings'?'Close settings':'Edit settings',()=>{s.section=s.section==='settings'?'':'settings';render();})]),...(s.section==='settings'?[scheduleForm()]:[])]),
      el('section',{class:'br-section'},[row([el('div',{},[title('Additional copy'),text(dest)]),button('Connection settings',()=>{s.section=s.section==='destination'?'':'destination';render();})]),...(s.section==='destination'?[destinationForm()]:[])]),
      row([title('Saved backups'),actions([button('Restore from file',()=>{s.section=s.section==='import'?'':'import';render();}),...(s.plan.destination.kind!=='none'?[button('Browse additional copies',()=>perform(browseRemote))]:[])])]),
      ...(s.section==='import'?[importView()]:[]),...(s.section==='remote'?[el('div',{id:'br-remote'})]:[]),el('div',{id:'br-history'}));renderStatus();renderHistory();
  }
  async function refreshList(){s.backups=(await api('list')).backups||[];renderHistory();}
  async function refreshJobs(){const result=await api('status');s.jobs=result.jobs||[];renderStatus();const stamp=JSON.stringify(s.jobs.map(j=>[j.id,j.stage,j.local_status,j.remote_status]));if(stamp!==s.stamp){s.stamp=stamp;await refreshList();}if(result.restore&&!s.message)message(result.restore.message,!result.restore.ok);}
  async function refresh(){const [p,sch,l]=await Promise.all([api('plan'),api('schedule'),api('list')]);s.plan=p.plan;s.schedule=sch.schedule||{};s.backups=l.backups||[];s.draft={...structuredClone(p.plan),password:'',password_saved:false,active:!!s.schedule.active,at:s.schedule.at||'03:00',days:s.schedule.days?.length?[...s.schedule.days]:[1,2,3,4,5,6,7],max_backups:s.schedule.max_backups||7};render();await refreshJobs();}
  function open(){shell();s.opener=document.activeElement;s.message='';s.section='';s.restore=null;s.cloud=null;$('cw-backups-modal').classList.remove('hidden');document.body.classList.add('br-backups-open','cx-modal-open');render();refresh().catch(e=>message(e.message,true));$('cw-backups-modal').querySelector('button')?.focus();clearInterval(s.timer);s.timer=setInterval(()=>refreshJobs().catch(e=>message(e.message,true)),2500);}
  function close(){if(s.busy)return;clearInterval(s.timer);$('cw-backups-modal')?.classList.add('hidden');document.body.classList.remove('br-backups-open','cx-modal-open');s.draft=null;s.plan=null;s.restore=null;s.cloud=null;s.opener?.focus();}
  window.openBackupRestore=open;(window.CW||={}).Backups={open,close,refresh};
})();
