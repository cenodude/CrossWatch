/* assets/helpers/backups.js */
/* CrossWatch - Application, tracker and capture backup interface */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const s = { plan: null, draft: null, schedule: {}, backups: [], jobs: [], view: 'backups', page: 0, remotePage: 0, remote: [], section: '', restore: null, message: '', noticeJob: '', error: false, busy: false, timer: null, stamp: '', submitted: null, pendingRestore: false, statusReady: false, mounted: false };
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
  const runningJob = () => s.submitted || s.jobs.find(job=>!['complete','failed'].includes(job.stage));
  const locked = () => !s.statusReady || !!runningJob() || s.pendingRestore;
  const acknowledge = () => {try{if(localStorage.getItem('cw.mobile.haptics')!=='0')navigator.vibrate?.(10);}catch{}};
  const button = (label,fn,primary = false,safe = false) => el('button',{type:'button',class:`cw-page-button br-button${primary?' primary':''}`,'data-job-lock':String(!safe),disabled:s.busy||(!safe&&locked()),on:{click:e=>{if(e.currentTarget.disabled)return;acknowledge();fn(e);}}},[label]);
  function updateLocks() {
    const root=$('page-backups');if(!root)return;
    root.setAttribute('aria-busy',String(s.busy));
    root.querySelectorAll('button').forEach(n=>{n.disabled=s.busy||n.dataset.locked==='true'||(n.dataset.jobLock==='true'&&locked());});
    root.querySelectorAll('input,select').forEach(n=>{n.disabled=s.busy||locked();});
    const create=$('br-create');
    if(create){create.disabled ||= !s.draft?.components.length || (s.draft.components.includes('trackers')&&s.draft.trackers?.length===0);create.textContent=s.busy?'Working…':runningJob()?'Backup in progress…':s.pendingRestore?'Restart pending':'Back up now';}
    const restore=$('br-restore-confirm');
    if(restore&&s.restore){const r=s.restore;restore.disabled ||= r.mode!=='new_tracker'&&!r.confirm || r.mode==='replace'&&r.validation?.application_ready===false;}
  }
  const field = (label,control) => {control.setAttribute('aria-label',label);return el('label',{class:'br-field'},[el('span',{text:label}),control]);};
  const input = (value,fn,type = 'text',props = {}) => el('input',{class:'cw-page-input br-input',type,value,...props,on:{input:e=>fn(e.target.value)}});
  const check = (label,checked,fn) => el('label',{class:'br-check'},[el('input',{type:'checkbox',checked,'aria-label':label,on:{change:e=>fn(e.target.checked)}}),el('span',{text:label})]);
  const bytes = n => {const size=Number(n||0);return size<1024?`${size} B`:size<1048576?`${(size/1024).toFixed(1)} KB`:`${(size/1048576).toFixed(1)} MB`;};
  const pageSize = 5;
  const shortNames = {application:'Application',trackers:'Trackers',captures:'Captures'};
  const badges = values => el('div',{class:'br-badges'},values.map(value=>el('span',{class:'br-badge',text:value})));
  const date = v => v ? new Date(typeof v === 'number' ? v*1000 : v).toLocaleString() : 'Not yet';
  const contents = b => Array.isArray(b.components) ? b.components.map(c=>names[c]||c).join(' + ') : legacy[b.scope] || 'Imported archive';
  function select(options,value,fn) { const n=el('select',{class:'cw-page-input br-input',on:{change:e=>fn(e.target.value)}},options.map(([v,label])=>el('option',{value:v,text:label}))); n.value=String(value); return n; }
  async function api(path,body) {
    const r=await fetch(`/api/backups/${path}`,body===undefined?{cache:'no-store'}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const data=await r.json(); if(!r.ok||data.ok===false) throw new Error(data.error||'Backup request failed.'); return data;
  }
  function message(value,error=false,jobId='') { s.message=value; s.noticeJob=jobId; s.error=error; const n=$('br-message'); if(n){n.textContent=value;n.hidden=!value;n.classList.toggle('is-error',error);} }
  async function perform(fn,safe = false) {
    if(s.busy||(!safe&&locked()))return;
    s.busy=true;updateLocks();
    try{await fn();}catch(e){message(e.message||String(e),true);}
    finally{s.busy=false;updateLocks();}
  }
  function shell(root) {
    if(root.querySelector('#br-body'))return;
    root.classList.add('cw-page-backups');
    root.replaceChildren(
      el('header',{class:'cw-page-hero br-head'},[el('div',{},[el('h2',{id:'br-title',class:'cw-page-hero-title',text:'Backup & Restore'}),text('Protect your application, trackers and saved captures.','cw-page-hero-subtitle br-muted')]),button('Restore from file',()=>{s.restore=null;s.view='backups';s.section=s.section==='import'?'':'import';render();$('br-import')?.scrollIntoView({block:'nearest'});})]),
      el('div',{id:'br-tabs',class:'br-tabs',role:'tablist','aria-label':'Backup views'}),
      el('div',{id:'br-body',class:'br-body',role:'tabpanel'}));
    ['input','change'].forEach(event=>root.addEventListener(event,e=>{e.stopPropagation();updateLocks();}));
  }
  function renderTabs() {
    const tabs=$('br-tabs'),body=$('br-body');if(!tabs)return;
    tabs.hidden=false;
    body.setAttribute('aria-labelledby',s.restore?'br-title':`br-tab-${s.view}`);
    const activate=view=>{s.view=view;s.section='';s.restore=null;render();body.scrollTop=0;$(`br-tab-${view}`)?.focus();};
    tabs.replaceChildren(...[['backups','Backups'],['settings','Schedule & storage']].map(([view,label],index)=>el('button',{
      id:`br-tab-${view}`,type:'button',class:'cw-page-tab br-tab',role:'tab','aria-selected':String(s.view===view),'aria-controls':'br-body',tabindex:s.view===view?'0':'-1','data-locked':String(!s.plan),disabled:s.busy||!s.plan,
      on:{click:()=>{acknowledge();activate(view);},keydown:e=>{if(s.busy||!s.plan||!['ArrowLeft','ArrowRight','Home','End'].includes(e.key))return;e.preventDefault();activate(e.key==='Home'?'backups':e.key==='End'?'settings':index===0?'settings':'backups');}}
    },[label])));
  }
  function selection() {
    const d=s.draft;
    const create=button('Back up now',()=>perform(async()=>{await savePlan();const result=await api('create',{managed:true});s.submitted={...result.job,local_status:'pending',remote_status:'disabled'};s.page=0;render();message('Backup queued. You can leave this page while it runs.',false,result.job.id);await refreshJobs();}),true);
    create.id='br-create';
    const block=el('section',{class:'cw-page-panel br-plan','aria-labelledby':'br-plan-title'},[el('h3',{id:'br-plan-title',text:'What to back up'})]);
    const options=el('div',{id:'br-content-options',class:'br-content-options'});
    Object.entries(names).forEach(([key,label])=>{
      const option=el('div',{class:'br-content-option'},[check(label,d.components.includes(key),yes=>{d.components=yes?[...new Set([...d.components,key])]:d.components.filter(v=>v!==key);render();})]);
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
    block.append(options,create,text('A local copy is saved on this server.'),el('div',{class:'br-plan-meta'},[el('span',{text:s.schedule.active?'Automatic: '+s.schedule.at:'Automatic backups off'}),el('span',{text:d.encrypt_local?'Encrypted':'Local ZIP'}),el('span',{text:{webdav:'Nextcloud / WebDAV',folder:'Mounted folder'}[d.destination.kind]||'This server only'})]));
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
    mergeDraft((await api('plan')).plan,s.schedule,true);
    const d=s.draft;
    s.plan=(await api('plan',{components:d.components,trackers:d.trackers,encrypt_local:!!d.encrypt_local,password:d.password||'',password_saved:!!d.password_saved,destination:d.destination,remote_keep:d.remote_keep})).plan;
    Object.assign(d,structuredClone(s.plan),{password:'',password_saved:false});
  }
  function renderStatus() {
    const slot=$('br-status');if(!slot)return;
    const job=runningJob()||s.jobs[0],nodes=[];
    slot.dataset.state=job?.stage||'';
    if(s.pendingRestore)nodes.push(title('Restore prepared. Restart pending.'));
    if(!s.statusReady)nodes.push(text('Checking backup status. Actions are paused until the server responds.'));
    if(job){
      const heading=job.stage==='failed'&&job.local_status==='saved'?'Local backup saved, destination copy failed':stages[job.stage]||job.stage;
      nodes.push(row([el('div',{class:'br-status-heading'},[el('span',{class:'br-status-dot','aria-hidden':'true'}),title(heading)]),text(job.local_status==='saved'?`Saved ${date(job.local_saved_at)}`:`Local copy: ${job.local_status}`)]));
      if(job.remote_status!=='disabled')nodes.push(text(`Destination copy: ${job.remote_status}${job.remote_verified_at?' '+date(job.remote_verified_at):''}`));
      if(job.stage==='uploading')nodes.push(el('progress',{max:Math.max(job.total_bytes||0,1),value:job.bytes||0,'aria-label':'Upload progress'}),text(`${bytes(job.bytes)} of ${bytes(job.total_bytes)}`));
      if(job.error)nodes.push(text(job.error,'br-error'));if(job.warning)nodes.push(text(job.warning));
      if(job.stage==='failed')nodes.push(row([text(job.retry_at?`Automatic retry: ${date(job.retry_at)}`:'Automatic retries stopped. Check the destination before retrying.'),button('Retry',()=>perform(async()=>{const result=await api('retry',{id:job.id});s.submitted={...job,...result.job,stage:'queued'};await refreshJobs();}))]));
    }
    const verified=s.jobs.find(j=>j.remote_status==='verified');if(verified&&verified.id!==job?.id)nodes.push(text(`Last verified destination copy: ${date(verified.remote_verified_at)}`));slot.hidden=!nodes.length;slot.replaceChildren(...nodes);
  }
  function pagination(total,key,draw) {
    const pages=Math.max(1,Math.ceil(total/pageSize));s[key]=Math.min(s[key],pages-1);
    const control=(label,step,locked)=>{const n=button(label,()=>{s[key]+=step;draw();document.querySelector(`[data-pager="${key}"] button:not(:disabled)`)?.focus({preventScroll:true});},false,true);n.dataset.locked=String(locked);n.disabled=s.busy||locked;return n;};
    return el('nav',{class:'br-row br-pagination','aria-label':key==='page'?'Backup pages':'Destination backup pages','data-pager':key},[
      text(`${s[key]*pageSize+1}-${Math.min((s[key]+1)*pageSize,total)} of ${total}`),
      actions([control('Previous',-1,s[key]===0),control('Next',1,s[key]>=pages-1)])]);
  }
  function renderHistory() {
    const slot=$('br-history');if(!slot)return;
    const count=$('br-count');if(count)count.textContent=`${s.backups.length} saved on this server`;
    const expanded=new Set([...slot.querySelectorAll('details[open]')].map(n=>n.dataset.path));
    slot.replaceChildren();
    if(!s.backups.length){s.page=0;slot.append(el('div',{class:'br-empty'},[title('No backups yet'),text('Create your first backup or restore from a file.')]));return;}
    const pager=pagination(s.backups.length,'page',renderHistory);
    s.backups.slice(s.page*pageSize,(s.page+1)*pageSize).forEach(b=>{
      const job=s.jobs.find(j=>j.path===b.path);
      const details=el('details',{'data-path':b.path,open:expanded.has(b.path)},[el('summary',{},[el('span',{text:`${bytes(b.size)} \u00b7 ${b.encrypted?'Encrypted':'ZIP'} \u00b7 Details`})]),el('div',{class:'br-backup-details'},[
        text(b.path),text(`${b.encrypted?'Encrypted archive':'ZIP archive'} \u00b7 ${job?.remote_status==='verified'?'Destination copy verified':'Local copy'} \u00b7 ${b.trigger==='scheduler'?'Automatic retention':'Kept until deleted'}`),actions([
          button('Download',()=>{window.location.href=`/api/backups/download?path=${encodeURIComponent(b.path)}`;},false,true),
          button('Delete',async ()=>{if((await (window.CW?.MobileUI?.ask || window.confirm)('Delete this local backup? Destination copies are kept separately.')))perform(async()=>{await api('delete',{path:b.path});await refreshList();});})])])]);
      const created=b.created_at||b.mtime,when=created?new Date(typeof created==='number'?created*1000:created):null;
      const labels=Array.isArray(b.components)?Object.keys(names).filter(key=>b.components.includes(key)).map(key=>shortNames[key]):[legacy[b.scope]||'Imported archive'];
      slot.append(el('article',{class:'br-history-row'},[el('div',{class:'br-history-main'},[
        el('div',{class:'br-backup-date'},[el('strong',{text:when?when.toLocaleDateString():'Unknown date'}),text(when?when.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}):'')]),
        el('div',{class:'br-backup-info'},[badges(labels),details]),
        button('Restore',()=>{s.restore={path:b.path,password:'',validation:null,mode:'replace',instance:'',label:'Recovered tracker'};render();})])]));
    });
    slot.append(pager);updateLocks();
  }
  function restoreView() {
    const r=s.restore;
    const heading=(step,id,label,description)=>el('div',{class:'br-restore-heading'},[el('span',{class:'br-restore-step','aria-hidden':'true',text:step}),el('div',{},[el('h3',{id,text:label}),text(description)])]);
    const details=el('section',{class:'cw-page-panel br-restore-card','aria-labelledby':'br-restore-details-title'},[
      heading('1','br-restore-details-title','Backup details','Check the archive before choosing what to restore.'),
      el('div',{class:'br-restore-archive'},[text('Selected archive'),el('strong',{text:r.path.split('/').pop()}),text(r.path)]),
      field('Backup password, if encrypted',input(r.password,v=>{r.password=v;},'password',{autocomplete:'off'})),
      button('Check backup',()=>perform(async()=>{r.validation=(await api('validate',{path:r.path,password:r.password})).validation;r.mode=r.validation.manifest.trackers?.length?'new_tracker':r.validation.available_sections?.[0]||'replace';r.instance=r.validation.manifest.trackers?.[0]?.id||'';r.confirm=false;render();}))
    ]);
    const optionsPanel=el('section',{class:'cw-page-panel br-restore-card','aria-labelledby':'br-restore-options-title'},[heading('2','br-restore-options-title','Restore options','Choose what to recover from this backup.')]);
    const section=el('section',{class:'br-restore','aria-labelledby':'br-restore-title'},[
      el('div',{class:'br-row br-restore-header'},[el('div',{},[el('h3',{id:'br-restore-title',text:'Review restoration'}),text('Review your backup and recovery settings before continuing.')]),button('Back to backups',()=>{s.restore=null;render();},false,true)]),
      el('div',{class:'br-restore-grid'},[details,optionsPanel])
    ]);
    if(r.validation){const manifest=r.validation.manifest;
      details.append(el('div',{class:'br-restore-validation',role:'status'},[el('strong',{text:'Backup checked'}),text('Archive checksums and database integrity checked.')]),el('div',{class:'br-restore-contents'},[text('Included in this backup'),Array.isArray(manifest.components)?badges(manifest.components.map(c=>names[c]||c)):text(contents(manifest))]));
      const options=(r.validation.available_sections||[]).filter(key=>recoverySections[key]).map(key=>[key,recoverySections[key]]);
      if(manifest.trackers?.length)options.unshift(['new_tracker','Recover one tracker as a new instance']);
      options.push(['replace','Replace the data in this backup']);
      optionsPanel.append(field('Restore action',select(options,r.mode,v=>{r.mode=v;r.confirm=false;render();})));
      if(r.mode==='replace'&&r.validation.application_error)optionsPanel.append(text(r.validation.application_error,'br-error'));
      if(r.mode==='new_tracker')optionsPanel.append(el('div',{class:'br-fields'},[field('Tracker',select(manifest.trackers.map(t=>[t.id,t.label||t.id]),r.instance,v=>{r.instance=v;})),field('New tracker name',input(r.label,v=>{r.label=v;}))]));
      const footer=el('div',{class:'br-restore-footer'});
      if(r.mode==='new_tracker')footer.append(text('No sync pairs are created. Your existing tracker stays in place.'));
      if(r.mode==='replace')footer.append(text('CW saves a safety copy and applies the replacement during restart. Captures are restored as files only. No captured data is sent to providers.'),check('Replace the data contained in this backup',!!r.confirm,v=>{r.confirm=v;updateLocks();}));
      if(recoverySections[r.mode])footer.append(text(r.mode==='captures'?'Restores saved capture files and replaces files with the same names.':'Replaces the selected rules with those from this backup.'),text('CW saves a safety copy and restores your selection during restart.'),check('Restore only the selected data',!!r.confirm,v=>{r.confirm=v;updateLocks();}));
      const confirm=button(r.mode==='new_tracker'?'Recover tracker':'Restore and restart',()=>perform(async()=>{if(r.mode==='replace'&&r.validation.application_ready===false)throw new Error(r.validation.application_error);if(r.mode!=='new_tracker'&&!r.confirm)throw new Error('Confirm replacement before restoring.');const result=await api('restore',{path:r.path,password:r.password,mode:r.mode,instance:r.instance,label:r.label,confirm:!!r.confirm,restart:true});if(r.mode==='new_tracker'){s.restore=null;render();message(result.result.message);}else{s.pendingRestore=true;message('Restore prepared. CrossWatch is restarting. Reopen CW to see the result.');}}),true);confirm.id='br-restore-confirm';footer.append(confirm);optionsPanel.append(footer);
    }else optionsPanel.append(el('div',{class:'br-restore-placeholder'},[el('strong',{text:'Check your backup to continue'}),text('Restore options will appear here once the archive has been checked.')]));
    return section;
  }
  function importView() {
    let password='';const file=el('input',{type:'file',accept:'.zip,.cwbackup',class:'br-input','aria-label':'Backup archive'});
    return el('section',{id:'br-import',class:'cw-page-panel br-import'},[title('Restore from file'),field('Backup archive',file),field('Backup password, if encrypted',input('',v=>{password=v;},'password',{autocomplete:'off'})),button('Import and review',()=>perform(async()=>{if(!file.files?.length)throw new Error('Choose a backup archive.');const form=new FormData();form.append('file',file.files[0]);form.append('password',password);const r=await fetch('/api/backups/upload',{method:'POST',body:form});const data=await r.json();if(!r.ok||data.ok===false)throw new Error(data.error||'Could not import this archive.');s.restore={path:data.backup.path,password,validation:null,mode:'replace',instance:'',label:'Recovered tracker'};s.section='';await refreshList();render();}),true)]);
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
    const body=$('br-body');if(!body)return;queueMicrotask(updateLocks);renderTabs();const msg=el('div',{id:'br-message',class:s.error?'br-message is-error':'br-message',role:'status',hidden:!s.message,text:s.message});
    if(!s.plan){body.replaceChildren(msg,text('Loading backup settings...'));return;}if(s.restore){body.replaceChildren(msg,el('section',{id:'br-status',class:'br-status','aria-live':'polite',hidden:true}),restoreView());renderStatus();return;}
    const dest={webdav:'Nextcloud / WebDAV',folder:'Mounted folder'}[s.plan.destination.kind]||'This server only';
    const section=(name,summary,key,label,form)=>el('section',{class:'cw-page-panel br-section br-setting'},[row([el('div',{class:'br-content-summary'},[title(name),text(summary)]),button(s.section===key?'Close settings':label,()=>{s.section=s.section===key?'':key;render();},false,true)]),...(s.section===key?[form()]:[])]);
    if(s.view==='settings'){
      body.replaceChildren(msg,
        section('Automatic backups',s.schedule.active?`At ${s.schedule.at} \u00b7 ${s.schedule.managed?'Uses your backup selections':'Existing schedule: '+(legacy[s.schedule.scope]||s.schedule.scope)}`:'Off','settings','Edit schedule',scheduleForm),
        section('Destination',dest,'destination','Edit destination',destinationForm),
        section('Encryption',s.plan.encrypt_local?'Local backups encrypted':'Local backups saved as ZIP','encryption','Edit encryption',encryptionForm),
        ...(s.plan.destination_notice?[text(s.plan.destination_notice)]:[]));
      return;
    }
    const library=el('section',{class:'br-library','aria-label':'Saved backups'},[
      row([title('Saved backups'),el('p',{id:'br-count',class:'br-muted',text:`${s.backups.length} saved on this server`})]),
      ...(s.plan.destination_notice?[text(s.plan.destination_notice)]:[]),
      ...(s.plan.destination.kind!=='none'?[actions([button('On this server',()=>{s.section='';render();},s.section!=='remote',true),button('At destination',()=>perform(browseRemote,true),s.section==='remote',true)])]:[]),
      ...(s.section==='import'?[importView()]:[]),s.section==='remote'?el('div',{id:'br-remote'}):el('div',{id:'br-history'})]);
    body.replaceChildren(msg,el('section',{id:'br-status',class:'br-status','aria-live':'polite',hidden:true}),el('div',{class:'br-workspace'},[selection(),library]));renderStatus();renderHistory();renderRemote();
  }

  async function refreshList(){s.backups=(await api('list')).backups||[];renderHistory();}
  async function refreshJobs(){
    try{
      const result=await api('status');s.jobs=result.jobs||[];s.statusReady=true;s.pendingRestore ||= !!result.pending_restore;
      if(s.submitted&&s.jobs.some(j=>j.id===s.submitted.id))s.submitted=null;
      if(s.noticeJob&&s.jobs.some(j=>j.id===s.noticeJob))message('');
      renderStatus();updateLocks();
      const stamp=JSON.stringify(s.jobs.map(j=>[j.id,j.stage,j.local_status,j.remote_status]));
      if(stamp!==s.stamp){s.stamp=stamp;await refreshList();}
      if(result.restore&&!s.message)message(result.restore.message,!result.restore.ok);
    }catch(e){s.statusReady=false;renderStatus();updateLocks();throw e;}
  }
  function draftValues(plan,schedule) {
    return {...structuredClone(plan),password:'',password_saved:false,active:!!schedule.active,at:schedule.at||'03:00',days:schedule.days?.length?[...schedule.days]:[1,2,3,4,5,6,7],max_backups:schedule.max_backups||7};
  }
  function mergeDraft(plan,schedule,preserve) {
    const next=draftValues(plan,schedule);
    if(preserve&&s.draft&&s.plan){
      const previous=draftValues(s.plan,s.schedule);
      for(const key of ['components','trackers','encrypt_local','destination','remote_keep','password','password_saved','active','at','days','max_backups']){
        if(JSON.stringify(s.draft[key])!==JSON.stringify(previous[key]))next[key]=structuredClone(s.draft[key]);
      }
    }
    Object.assign(s.draft||={},next);s.plan=plan;s.schedule=schedule;
  }
  async function refresh(preserve=false){const [p,sch,l]=await Promise.all([api('plan'),api('schedule'),api('list')]);mergeDraft(p.plan,sch.schedule||{},preserve);s.backups=l.backups||[];render();await refreshJobs();}
  async function mount(root){
    if(!root)return;
    s.mounted=true;s.statusReady=false;shell(root);render();updateLocks();
    clearInterval(s.timer);
    await perform(()=>refresh(true),true);
    if(s.mounted){clearInterval(s.timer);s.timer=setInterval(()=>{if(!s.busy)refreshJobs().catch(e=>message(e.message,true));},2500);}
  }
  function hide(){s.mounted=false;clearInterval(s.timer);s.timer=null;}
  function open(){if(window.showTab&&$('page-backups'))return window.showTab('backups');window.location.href='/?main=1#backups';}
  window.BackupsPage={mount,hide};
  window.openBackupRestore=open;(window.CW||={}).Backups={open,refresh};
})();
