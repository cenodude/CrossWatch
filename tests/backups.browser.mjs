/* tests/backups.browser.mjs */
/* CrossWatch - Backup selections, recovery review and themed page checks */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

async function fixture(t, theme = '', width = 1100, populated = false, overrides = {}, validationOverrides = {}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width,height:900}});
  page.setDefaultTimeout(5000);
  const errors = [], writes = [];
  page.on('pageerror', e => errors.push(e.message));
  t.after(() => assert.deepEqual(errors, []));
  let plan = {components:['application','trackers','captures'],trackers:null,has_password:false,encrypt_local:false,destination:{kind:'none'},remote_keep:30,key_source:'included',rclone_available:true,available_trackers:[{id:'default',label:'Main tracker'},{id:'one',label:'Family tracker'}],...overrides};
  let schedule = {active:false};
  const backup = {path:'2026-11-15/crosswatch-backup-test.cwbackup',created_at:'2026-11-15T12:00:00Z',components:['application','trackers','captures'],size:218000,encrypted:true};
  let backups = populated ? Array.from({length:typeof populated==='number'?populated:1},(_,i)=>({...backup,path:`saved/crosswatch-backup-${i}.cwbackup`,size:i===0?420:backup.size,created_at:new Date(Date.UTC(2026,10,15-i,12,0)).toISOString(),...(i%3===1?{components:['application']}:i%3===2?{components:['trackers','captures']}: {})})) : [];
  const manifest = {...backup,trackers:[{id:'default',label:'Main tracker'}]};
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) {
      try {return route.fulfill({contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript',body:await readFile(new URL('..'+url.pathname,import.meta.url))});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/api/')) {
      const method = route.request().method(), name = url.pathname.split('/').slice(3).join('/');
      const body = method === 'POST' ? route.request().postDataJSON() : null;
      if(body)writes.push({name,body});
      if(name==='plan'&&body)plan={...plan,...body,has_password:!!body.password||plan.has_password};
      if(name==='delete')backups=backups.filter(b=>b.path!==body.path);
      const data = {remote:{backups:overrides.remoteBackups||[]},plan:{plan},schedule:{schedule},list:{backups},status:{jobs:overrides.jobs??(populated?[{id:'job',path:backup.path,stage:'failed',local_status:'saved',remote_status:'failed',error:'Server unreachable',retry_at:1794750000}]:[])},create:{job:{id:'new',stage:'queued'}},validate:{validation:{ok:true,manifest,application_ready:true,...validationOverrides}},restore:{result:{message:'Tracker recovered. No sync pairs were created.'}},'recovery-key':{key:'generated-recovery-key-for-test'}}[name]||{};
      return route.fulfill({json:{ok:true,...data}});
    }
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html${theme?` data-cw-theme="${theme}"`:''}><head><meta name="viewport" content="width=device-width,initial-scale=1">${['themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','themes/flat','themes/original-coverage'].map(f=>`<link rel="stylesheet" href="/assets/${f}.css">`).join('')}</head><body style="padding:16px"><main id="page-backups" class="cw-page-backups"></main><script src="/assets/helpers/backups.js"></script></body></html>`});
  });
  await page.goto('http://cw.test/');
  await page.evaluate(()=>BackupsPage.mount(document.getElementById('page-backups')));
  await page.getByRole('heading',{name:'What to back up'}).waitFor();
  return {page,writes,updateServer:changes=>{if(changes.plan)plan={...plan,...changes.plan};if(changes.schedule)schedule=changes.schedule;if(changes.backups)backups=changes.backups;},empty:async()=>{populated=false;backups=[];overrides.jobs=[];await page.evaluate(()=>CW.Backups.refresh());}};
}

test('returning refreshes saved settings and inventory while keeping unsaved selections',async t=>{
  const {page,writes,updateServer}=await fixture(t);
  await page.getByLabel('Saved captures',{exact:true}).uncheck();
  await page.getByRole('tab',{name:'Schedule & storage'}).click();
  await page.getByRole('button',{name:'Edit schedule'}).click();
  await page.getByLabel('Time',{exact:true}).fill('06:30');
  await page.evaluate(()=>BackupsPage.hide());
  updateServer({plan:{encrypt_local:true,has_password:true,destination:{kind:'folder',path:'/backups'},available_trackers:[{id:'new',label:'New tracker'}]},schedule:{active:true,at:'04:00',days:[1,3,5],max_backups:14,managed:true},backups:[{path:'imported/backup.zip',components:['application'],size:123}]});
  await page.evaluate(()=>BackupsPage.mount(document.getElementById('page-backups')));
  assert.equal(await page.getByLabel('Time',{exact:true}).inputValue(),'06:30');
  assert.equal(await page.getByLabel('Back up automatically',{exact:true}).isChecked(),true);
  assert.equal(await page.getByLabel('Automatic local copies to keep').inputValue(),'14');
  await page.getByRole('tab',{name:'Backups',exact:true}).click();
  assert.equal(await page.getByLabel('Saved captures',{exact:true}).isChecked(),false);
  assert.equal(await page.locator('#br-history article').count(),1);
  await page.getByLabel('All trackers',{exact:true}).uncheck();
  assert.equal(await page.getByLabel('New tracker',{exact:true}).isChecked(),true);
  assert.equal(await page.getByLabel('Main tracker',{exact:true}).count(),0);
  await page.locator('#br-create').click();
  await page.getByText('Backup queued. You can leave this page while it runs.',{exact:true}).waitFor();
  const saved=writes.findLast(w=>w.name==='plan').body;
  assert.equal(saved.encrypt_local,true);
  assert.deepEqual(saved.destination,{kind:'folder',path:'/backups'});
  assert.deepEqual(saved.components,['application','trackers']);
  assert.deepEqual(saved.trackers,['new']);
});

test('starting a backup preserves settings changed in another tab without navigation',async t=>{
  const {page,writes,updateServer}=await fixture(t);
  await page.getByLabel('Saved captures',{exact:true}).uncheck();
  updateServer({plan:{encrypt_local:true,has_password:true,destination:{kind:'folder',path:'/backups'},remote_keep:60}});
  await page.locator('#br-create').click();
  await page.getByText('Backup queued. You can leave this page while it runs.',{exact:true}).waitFor();
  const saved=writes.findLast(w=>w.name==='plan').body;
  assert.equal(saved.encrypt_local,true);
  assert.deepEqual(saved.destination,{kind:'folder',path:'/backups'});
  assert.equal(saved.remote_keep,60);
  assert.deepEqual(saved.components,['application','trackers']);
});

test('a failed settings refresh cannot save an old plan or start a backup',async t=>{
  const {page,writes}=await fixture(t);
  await page.route('**/api/backups/plan',route=>route.request().method()==='GET'?route.fulfill({status:503,json:{ok:false,error:'Settings unavailable'}}):route.fallback());
  await page.locator('#br-create').click();
  await page.getByText('Settings unavailable',{exact:true}).waitFor();
  assert.equal(writes.length,0);
  assert.equal(await page.locator('#br-create').isDisabled(),false);
  await page.unroute('**/api/backups/plan');
  await page.locator('#br-create').click();
  await page.getByText('Backup queued. You can leave this page while it runs.',{exact:true}).waitFor();
  assert.equal(writes.filter(w=>w.name==='create').length,1);
});

test('unsaved encryption edits survive returning and become clean after saving',async t=>{
  const {page,writes,updateServer}=await fixture(t);
  await page.getByRole('tab',{name:'Schedule & storage'}).click();
  await page.getByRole('button',{name:'Edit encryption'}).click();
  await page.getByLabel('Encrypt local backups',{exact:true}).check();
  await page.getByLabel('Backup password or recovery key',{exact:true}).fill('unsaved-recovery-key');
  await page.getByLabel('I saved any new password outside CW').check();
  await page.evaluate(()=>BackupsPage.hide());
  updateServer({plan:{remote_keep:60}});
  await page.evaluate(()=>BackupsPage.mount(document.getElementById('page-backups')));
  assert.equal(await page.getByLabel('Encrypt local backups',{exact:true}).isChecked(),true);
  assert.equal(await page.getByLabel('Backup password or recovery key',{exact:true}).inputValue(),'unsaved-recovery-key');
  await page.getByRole('button',{name:'Save encryption',exact:true}).click();
  await page.getByText('Encryption settings saved.',{exact:true}).waitFor();
  const saved=writes.findLast(w=>w.name==='plan').body;
  assert.equal(saved.encrypt_local,true);
  assert.equal(saved.password,'unsaved-recovery-key');
  assert.equal(saved.password_saved,true);
  assert.equal(saved.remote_keep,60);
  await page.evaluate(()=>BackupsPage.hide());
  updateServer({plan:{encrypt_local:false}});
  await page.evaluate(()=>BackupsPage.mount(document.getElementById('page-backups')));
  await page.getByRole('button',{name:'Edit encryption'}).click();
  assert.equal(await page.getByLabel('Encrypt local backups',{exact:true}).isChecked(),false);
  assert.equal(await page.getByLabel('New backup password (leave empty to keep current)',{exact:true}).inputValue(),'');
});

test('backs up only selected contents and tracker instances',async t=>{
  const {page,writes}=await fixture(t);
  await page.getByText('Settings, connections, sync state, mappings, blocks and custom anime rules.',{exact:true}).waitFor();
  await page.getByLabel('CrossWatch application',{exact:true}).uncheck();
  await page.getByLabel('Saved captures',{exact:true}).uncheck();
  await page.getByLabel('All trackers',{exact:true}).uncheck();
  await page.getByLabel('Main tracker',{exact:true}).uncheck();
  await page.getByRole('button',{name:'Back up now',exact:true}).click();
  await page.getByText('Backup queued. You can leave this page while it runs.').waitFor();
  assert.deepEqual(writes[0].body.components,['trackers']);
  assert.deepEqual(writes[0].body.trackers,['one']);
  assert.equal(writes[1].name,'create');
  await page.waitForFunction(()=>document.querySelector('#page-backups').getAttribute('aria-busy')==='false');
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  assert.equal(await page.getByRole('dialog').count(),0);
});

test('tracker restore defaults to a new instance and replacement requires confirmation',async t=>{
  const {page,writes}=await fixture(t,'flat-dark',1100,true);
  await page.getByRole('button',{name:'Restore',exact:true}).click();
  await page.getByRole('button',{name:'Check backup'}).click();
  await page.getByRole('button',{name:'Recover tracker',exact:true}).waitFor();
  assert.equal(await page.getByLabel('Restore action').inputValue(),'new_tracker');
  await page.getByLabel('Restore action').selectOption('replace');
  assert.equal(await page.getByRole('button',{name:'Restore and restart'}).isDisabled(),true);
  assert.equal(writes.filter(w=>w.name==='restore').length,0);
  await page.getByLabel('Replace the data contained in this backup',{exact:true}).check();
  await page.getByRole('button',{name:'Restore and restart'}).click();
  await page.getByText('Restore prepared. CrossWatch is restarting. Reopen CW to see the result.').waitFor();
  assert.equal(writes.at(-1).body.confirm,true);
});

for(const mode of ['mappings','anime_rules','captures'])test(`${mode} recovery confirms only the selected data`,async t=>{
  const {page,writes}=await fixture(t,'flat-dark',390,true,{}, {available_sections:['mappings','anime_rules','captures'],application_ready:false,application_error:'Original configuration key required'});
  await page.getByRole('button',{name:'Restore',exact:true}).click();
  await page.getByRole('button',{name:'Check backup'}).click();
  await page.getByLabel('Restore action').selectOption(mode);
  assert.equal(await page.getByText('Original configuration key required',{exact:true}).count(),0);
  assert.equal(await page.getByRole('button',{name:'Restore and restart'}).isDisabled(),true);
  assert.equal(writes.filter(w=>w.name==='restore').length,0);
  await page.getByLabel('Restore only the selected data',{exact:true}).check();
  await page.getByLabel('Restore action').selectOption('replace');
  assert.equal(await page.getByLabel('Replace the data contained in this backup',{exact:true}).isChecked(),false);
  await page.getByLabel('Restore action').selectOption(mode);
  await page.getByLabel('Restore only the selected data',{exact:true}).check();
  await page.getByRole('button',{name:'Restore and restart'}).click();
  await page.getByText('Restore prepared. CrossWatch is restarting. Reopen CW to see the result.').waitFor();
  assert.equal(writes.at(-1).body.mode,mode);
  assert.equal(writes.at(-1).body.confirm,true);
});

test('recovery hides sections absent from the backup',async t=>{
  const {page}=await fixture(t,'',1100,true,{}, {available_sections:['anime_rules']});
  await page.getByRole('button',{name:'Restore',exact:true}).click();
  await page.getByRole('button',{name:'Check backup'}).click();
  await page.getByLabel('Restore action').waitFor();
  assert.deepEqual(await page.getByLabel('Restore action').locator('option').evaluateAll(nodes=>nodes.map(n=>n.value)),['new_tracker','anime_rules','replace']);
});

test('unusable rclone is explained in destination settings',async t=>{
  const {page}=await fixture(t,'flat-dark',1100,false,{rclone_available:false,rclone_error:'rclone could not start. Check its installation.'});
  await page.getByRole('tab',{name:'Schedule & storage',exact:true}).click();
  await page.getByRole('button',{name:'Edit destination'}).click();
  await page.getByLabel('Destination',{exact:true}).selectOption('webdav');
  await page.getByText('rclone could not start. Check its installation.',{exact:true}).waitFor();
});

for(const mountedRoot of ['/backups','/mnt/cw-backups'])test(`mounted folder shows its allowed root ${mountedRoot}`,async t=>{
  const {page}=await fixture(t,'flat-dark',1100,false,{mounted_root:mountedRoot});
  await page.getByRole('tab',{name:'Schedule & storage',exact:true}).click();
  await page.getByRole('button',{name:'Edit destination'}).click();
  await page.getByLabel('Destination',{exact:true}).selectOption('folder');
  assert.equal(await page.getByLabel('Mounted folder inside CW').getAttribute('placeholder'),mountedRoot);
  await page.getByText(`Use ${mountedRoot} or a folder inside it. Mount it into the container, outside /config.`,{exact:true}).waitFor();
});

for(const theme of ['', 'flat-dark', 'flat-light'])for(const width of [1100,390]) {
  test(`backup page ${theme||'original'} ${width}`,async t=>{
    const {page,empty}=await fixture(t,theme,width,12,{jobs:[{id:'latest',stage:'complete',local_status:'saved',local_saved_at:1794744000,remote_status:'disabled'}]});
    const folder=new URL('../build/backup-ui/',import.meta.url);
    await mkdir(folder,{recursive:true});
    const suffix=`${theme||'original'}-${width}`;
    const noOverflow=async()=>assert.equal(await page.locator('#page-backups').evaluate(n=>n.scrollWidth<=n.clientWidth&&n.querySelector('.br-body').scrollWidth<=n.querySelector('.br-body').clientWidth),true);
    await noOverflow();
    assert.equal(await page.getByRole('tab',{name:'Backups',exact:true}).getAttribute('aria-selected'),'true');
    assert.equal(await page.locator('#br-history').evaluate(n=>n.scrollHeight<=n.clientHeight+1),true);
    assert.equal(await page.locator('#page-backups').evaluate(n=>getComputedStyle(n).position==='fixed'),false);
    const tabStyle=await page.getByRole('tab',{name:'Backups',exact:true}).evaluate(n=>{const s=getComputedStyle(n);return [s.borderRadius,s.borderBottomWidth,s.backgroundColor];});
    assert.deepEqual(tabStyle,['0px','2px','rgba(0, 0, 0, 0)']);
    assert.ok(await page.locator('#br-status').evaluate(n=>n.getBoundingClientRect().height<130));
    await page.screenshot({fullPage:true,path:new URL(`overview-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByRole('button',{name:'Restore',exact:true}).first().click();
    await page.getByText('Check your backup to continue',{exact:true}).waitFor();
    assert.equal(await page.getByLabel('Restore action').count(),0);
    await noOverflow();
    const cards=page.locator('.br-restore-card');
    const details=await cards.nth(0).boundingBox(),options=await cards.nth(1).boundingBox();
    if(width>1000){
      assert.ok(Math.abs(details.y-options.y)<1);
      assert.ok(options.x>=details.x+details.width);
      assert.ok(options.width>details.width);
    }else{
      assert.ok(options.y>=details.y+details.height);
      assert.ok(Math.abs(details.x-options.x)<1);
    }
    await page.screenshot({fullPage:true,path:new URL(`restore-unchecked-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByRole('button',{name:'Check backup'}).click();
    await page.getByText('Backup checked',{exact:true}).waitFor();
    assert.equal(await page.getByRole('button',{name:'Recover tracker',exact:true}).isEnabled(),true);
    await noOverflow();
    await page.screenshot({fullPage:true,path:new URL(`restore-tracker-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByLabel('Restore action').selectOption('replace');
    assert.equal(await page.getByRole('button',{name:'Restore and restart'}).isDisabled(),true);
    await noOverflow();
    await page.screenshot({fullPage:true,path:new URL(`restore-replace-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByLabel('Replace the data contained in this backup',{exact:true}).check();
    assert.equal(await page.getByRole('button',{name:'Restore and restart'}).isEnabled(),true);
    await page.getByRole('button',{name:'Back to backups'}).click();
    await page.getByRole('tab',{name:'Schedule & storage',exact:true}).click();
    await page.getByRole('button',{name:'Edit destination'}).click();
    await page.getByLabel('Destination',{exact:true}).selectOption('webdav');
    await page.getByLabel('WebDAV URL').fill('https://cloud.example.com/remote.php/dav/files/user/');
    if(width<700)assert.equal(await page.getByLabel('WebDAV URL').evaluate(n=>getComputedStyle(n).fontSize),'16px');
    await page.getByRole('button',{name:'Generate recovery key'}).click();
    await page.getByLabel('Save this outside CW').waitFor();
    await page.getByLabel('I saved this password or key outside CW').check();
    await page.getByLabel('WebDAV URL').scrollIntoViewIfNeeded();
    await noOverflow();
    await page.screenshot({fullPage:true,path:new URL(`destination-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    assert.equal(await page.locator('#page-backups').innerText().then(s=>s.includes('\u2014')),false);
    await empty();
    await page.getByRole('tab',{name:'Backups',exact:true}).click();
    await page.locator('.br-body').evaluate(n=>n.scrollTop=0);
    await noOverflow();
    await page.screenshot({fullPage:true,path:new URL(`empty-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
      await noOverflow();
    await page.screenshot({fullPage:true,path:new URL(`selection-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByRole('tab',{name:'Schedule & storage',exact:true}).click();
    await page.getByRole('button',{name:'Edit schedule'}).click();
    await page.getByText('Days',{exact:true}).click();
    await page.getByLabel('Time',{exact:true}).scrollIntoViewIfNeeded();
    await noOverflow();
    await page.screenshot({fullPage:true,path:new URL(`schedule-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByRole('button',{name:'Edit encryption'}).click();
    await page.getByLabel('Encrypt local backups',{exact:true}).check();
    await noOverflow();
    await page.screenshot({fullPage:true,path:new URL(`encryption-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
  });
}


test('destination choices save WebDAV with a recovery key',async t=>{
  const {page,writes}=await fixture(t,'flat-light',390);
  await page.getByRole('tab',{name:'Schedule & storage',exact:true}).click();
  await page.getByRole('button',{name:'Edit destination'}).click();
  const choice=page.getByLabel('Destination',{exact:true});
  assert.deepEqual(await choice.locator('option').evaluateAll(nodes=>nodes.map(n=>n.value)),['none','webdav','folder']);
  await choice.selectOption('webdav');
  await page.getByLabel('WebDAV URL').fill('https://example.com/dav/');
  await page.getByLabel('Username',{exact:true}).fill('backup-user');
  await page.getByLabel('App password',{exact:true}).fill('webdav-app-password');
  await page.getByRole('button',{name:'Generate recovery key'}).click();
  await page.getByLabel('I saved this password or key outside CW').check();
  await page.getByRole('button',{name:'Save destination',exact:true}).click();
  await page.getByText('Destination saved. Test the connection before relying on these copies.').waitFor();
  const saved=writes.findLast(w=>w.name==='plan').body;
  assert.equal(saved.destination.kind,'webdav');
  assert.equal(saved.destination.username,'backup-user');
  assert.equal(saved.password,'generated-recovery-key-for-test');
});

test('removed destination notice explains local fallback',async t=>{
  const notice='The saved destination is no longer supported. Backups are saved locally. Choose Nextcloud / WebDAV or a mounted folder.';
  const {page}=await fixture(t,'flat-dark',390,false,{destination_notice:notice});
  await page.getByText(notice,{exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'At destination'}).count(),0);
});


test('history paginates, restores the selected archive and clamps after deletion',async t=>{
  const {page,writes}=await fixture(t,'flat-dark',390,11,{jobs:[]});
  const history=page.locator('#br-history');
  assert.equal(await history.locator('article').count(),5);
  assert.equal(await history.getByRole('button',{name:'Previous'}).isDisabled(),true);
  await history.getByRole('button',{name:'Next'}).click();
  await history.getByText('6-10 of 11',{exact:true}).waitFor();
  await history.getByRole('button',{name:'Restore',exact:true}).first().click();
  await page.getByRole('button',{name:'Check backup'}).click();
  assert.equal(writes.find(w=>w.name==='validate').body.path,'saved/crosswatch-backup-5.cwbackup');
  await page.getByRole('button',{name:'Back to backups'}).click();
  await history.getByText('6-10 of 11',{exact:true}).waitFor();
  await history.getByRole('button',{name:'Next'}).click();
  assert.equal(await history.getByRole('button',{name:'Next'}).isDisabled(),true);
  await history.locator('summary').click();
  page.once('dialog',dialog=>dialog.accept());
  await history.getByRole('button',{name:'Delete',exact:true}).click();
  await history.getByText('6-10 of 10',{exact:true}).waitFor();
  await page.waitForFunction(()=>document.querySelector('#page-backups').getAttribute('aria-busy')==='false');
  assert.equal(await history.getByRole('button',{name:'Next'}).isDisabled(),true);
  assert.equal(writes.find(w=>w.name==='delete').body.path,'saved/crosswatch-backup-10.cwbackup');
  assert.equal(await history.locator('article').count(),5);
});

test('destination history paginates and downloads the selected archive for recovery',async t=>{
  const remoteBackups=Array.from({length:8},(_,i)=>({path:`install/crosswatch-backup-${i}.cwbackup`,size:1024}));
  const {page,writes}=await fixture(t,'flat-dark',390,false,{destination:{kind:'webdav'},has_password:true,remoteBackups});
  await page.route('**/api/backups/remote/download',route=>{writes.push({name:'remote/download',body:route.request().postDataJSON()});return route.fulfill({json:{ok:true,backup:{path:'imported/selected.cwbackup'}}});});
  await page.getByRole('button',{name:'At destination'}).click();
  const remote=page.locator('#br-remote');
  await remote.getByText('1-5 of 8',{exact:true}).waitFor();
  assert.equal(await remote.getByRole('button',{name:'Previous'}).isDisabled(),true);
  await remote.getByRole('button',{name:'Next'}).click();
  assert.equal(await remote.locator('article').count(),3);
  await remote.getByRole('button',{name:'Download to CW'}).first().click();
  await page.getByRole('heading',{name:'Review restoration'}).waitFor();
  assert.equal(writes.find(w=>w.name==='remote/download').body.path,'install/crosswatch-backup-5.cwbackup');
});

test('content selection survives switching views and local encryption saves independently',async t=>{
  const {page,writes}=await fixture(t,'flat-light',390);
  await page.getByLabel('Saved captures',{exact:true}).uncheck();
  await page.getByRole('tab',{name:'Schedule & storage',exact:true}).click();
  await page.getByRole('button',{name:'Edit encryption'}).click();
  await page.getByLabel('Encrypt local backups',{exact:true}).check();
  await page.getByRole('button',{name:'Generate recovery key'}).click();
  await page.getByLabel('I saved this password or key outside CW').check();
  await page.getByRole('button',{name:'Save encryption'}).click();
  await page.getByText('Encryption settings saved.').waitFor();
  const saved=writes.findLast(w=>w.name==='plan').body;
  assert.equal(saved.encrypt_local,true);
  assert.deepEqual(saved.components,['application','trackers']);
  await page.getByRole('tab',{name:'Backups',exact:true}).click();
  assert.equal(await page.getByLabel('Saved captures',{exact:true}).isChecked(),false);
});


for(const [stage,heading] of [['queued','Waiting'],['creating','Creating local backup'],['complete','Complete'],['failed','Local backup saved, destination copy failed']])test(`queued notice gives way to ${stage} for the created job`,async t=>{
  const overrides={jobs:[{id:'older',stage:'complete',local_status:'saved',remote_status:'disabled'}]};
  const {page}=await fixture(t,'flat-dark',390,false,overrides);
  await page.getByRole('button',{name:'Back up now',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('#page-backups').getAttribute('aria-busy')==='false');
  await page.getByText('Backup queued. You can leave this page while it runs.',{exact:true}).waitFor();
  overrides.jobs=[{id:'new',stage,local_status:'saved',remote_status:stage==='failed'?'failed':'disabled',error:stage==='failed'?'Destination unreachable':''}];
  await page.evaluate(()=>CW.Backups.refresh());
  assert.equal(await page.locator('#br-message').isVisible(),false);
  await page.locator('#br-status').getByRole('heading',{name:heading,exact:true}).waitFor();
  if(stage==='failed')await page.getByText('Destination unreachable',{exact:true}).waitFor();
});


test('underline tabs support keyboard navigation and preserve backup selection',async t=>{
  const {page}=await fixture(t,'flat-dark',390);
  await page.getByLabel('Saved captures',{exact:true}).uncheck();
  await page.getByRole('tab',{name:'Backups',exact:true}).focus();
  await page.keyboard.press('ArrowRight');
  const settings=page.getByRole('tab',{name:'Schedule & storage',exact:true});
  assert.equal(await settings.getAttribute('aria-selected'),'true');
  assert.equal(await settings.evaluate(n=>n===document.activeElement),true);
  assert.equal(await page.getByRole('tabpanel').getAttribute('aria-labelledby'),'br-tab-settings');
  await page.keyboard.press('Home');
  assert.equal(await page.getByRole('tab',{name:'Backups',exact:true}).getAttribute('aria-selected'),'true');
  assert.equal(await page.getByLabel('Saved captures',{exact:true}).isChecked(),false);
  await page.keyboard.press('End');
  assert.equal(await settings.getAttribute('aria-selected'),'true');
  await page.keyboard.press('ArrowLeft');
  assert.equal(await page.getByRole('tab',{name:'Backups',exact:true}).evaluate(n=>n===document.activeElement),true);
});

test('rapid taps create one backup and keep it locked through navigation and transfer',async t=>{
  const overrides={jobs:[]};
  const {page,writes}=await fixture(t,'flat-dark',390,true,overrides);
  await page.evaluate(()=>{window.buzzes=[];navigator.vibrate=value=>{window.buzzes.push(value);return true;};});
  let release;
  const pending=new Promise(resolve=>{release=resolve;});
  await page.route('**/api/backups/create',async route=>{
    writes.push({name:'create',body:route.request().postDataJSON()});
    await pending;
    await route.fulfill({json:{ok:true,job:{id:'new',stage:'queued'}}});
  });
  await page.locator('#br-create').evaluate(n=>{n.click();n.click();n.click();});
  await page.waitForFunction(()=>document.getElementById('page-backups').getAttribute('aria-busy')==='true');
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  assert.deepEqual(await page.evaluate(()=>window.buzzes),[10]);
  release();
  await page.getByText('Backup queued. You can leave this page while it runs.',{exact:true}).waitFor();
  assert.equal(writes.filter(w=>w.name==='create').length,1);
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  for(const stage of ['queued','creating','validating','encrypting','uploading','verifying']){
    overrides.jobs=[{id:'new',stage,local_status:'saved',remote_status:'pending'}];
    await page.evaluate(()=>CW.Backups.refresh());
    assert.equal(await page.locator('#br-create').isDisabled(),true,stage);
    assert.equal(await page.getByRole('button',{name:'Restore',exact:true}).isDisabled(),true,stage);
  }
  await page.evaluate(()=>{BackupsPage.hide();document.getElementById('page-backups').hidden=true;});
  await page.evaluate(()=>{document.getElementById('page-backups').hidden=false;return BackupsPage.mount(document.getElementById('page-backups'));});
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  await page.getByRole('tab',{name:'Schedule & storage'}).click();
  await page.getByRole('tab',{name:'Backups',exact:true}).click();
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  overrides.jobs=[{id:'new',stage:'complete',local_status:'saved',remote_status:'verified'}];
  await page.evaluate(()=>CW.Backups.refresh());
  assert.equal(await page.locator('#br-create').isDisabled(),false);
  assert.equal(await page.getByRole('button',{name:'Restore',exact:true}).isDisabled(),false);
});

test('failed requests release the button and haptic feedback respects the preference',async t=>{
  const {page}=await fixture(t);
  await page.evaluate(()=>{localStorage.setItem('cw.mobile.haptics','0');window.buzzes=[];navigator.vibrate=value=>{window.buzzes.push(value);return true;};});
  await page.route('**/api/backups/create',route=>route.fulfill({status:400,json:{ok:false,error:'Backup storage is unavailable.'}}));
  await page.locator('#br-create').click();
  await page.getByText('Backup storage is unavailable.',{exact:true}).waitFor();
  assert.equal(await page.locator('#br-create').isDisabled(),false);
  assert.deepEqual(await page.evaluate(()=>window.buzzes),[]);
});

test('missing selections and unavailable job status cannot start a backup',async t=>{
  const {page,writes}=await fixture(t);
  for(const label of ['CrossWatch application','CW trackers','Saved captures'])await page.getByLabel(label,{exact:true}).uncheck();
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  await page.getByLabel('CW trackers',{exact:true}).check();
  await page.getByLabel('All trackers',{exact:true}).uncheck();
  await page.getByLabel('Main tracker',{exact:true}).uncheck();
  await page.getByLabel('Family tracker',{exact:true}).uncheck();
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  await page.getByLabel('Main tracker',{exact:true}).check();
  assert.equal(await page.locator('#br-create').isDisabled(),false);
  await page.route('**/api/backups/status',route=>route.fulfill({status:503,json:{ok:false,error:'Status unavailable'}}));
  await page.evaluate(()=>BackupsPage.mount(document.getElementById('page-backups')));
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  await page.getByText('Checking backup status. Actions are paused until the server responds.',{exact:true}).waitFor();
  assert.equal(writes.filter(w=>w.name==='create').length,0);
  await page.unroute('**/api/backups/status');
  await page.evaluate(()=>BackupsPage.mount(document.getElementById('page-backups')));
  assert.equal(await page.locator('#br-create').isDisabled(),false);
});

test('restore stays locked after submission while CrossWatch restarts',async t=>{
  const {page,writes}=await fixture(t,'flat-dark',390,true);
  await page.getByRole('button',{name:'Restore',exact:true}).click();
  await page.getByRole('button',{name:'Check backup'}).click();
  await page.getByLabel('Restore action').selectOption('replace');
  await page.getByLabel('Replace the data contained in this backup',{exact:true}).check();
  await page.getByRole('button',{name:'Restore and restart'}).click();
  await page.getByText('Restore prepared. CrossWatch is restarting. Reopen CW to see the result.',{exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'Restore and restart'}).isDisabled(),true);
  await page.getByRole('button',{name:'Back to backups'}).click();
  assert.equal(await page.locator('#br-create').isDisabled(),true);
  assert.equal(await page.getByRole('button',{name:'Restore from file'}).isDisabled(),true);
  assert.equal(writes.filter(w=>w.name==='restore').length,1);
});
