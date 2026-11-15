/* tests/backups.browser.mjs */
/* CrossWatch - Backup selections, recovery review and themed dialog checks */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

async function fixture(t, theme = '', width = 1100, populated = false, overrides = {}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width,height:900}});
  page.setDefaultTimeout(5000);
  const errors = [], writes = [];
  page.on('pageerror', e => errors.push(e.message));
  t.after(() => assert.deepEqual(errors, []));
  let plan = {components:['application','trackers','captures'],trackers:null,has_password:false,encrypt_local:false,destination:{kind:'none'},remote_keep:30,key_source:'included',rclone_available:true,available_trackers:[{id:'default',label:'Main tracker'},{id:'one',label:'Family tracker'}],...overrides};
  let cloudKind='',cloudStep=0;
  const backup = {path:'2026-11-15/crosswatch-backup-test.cwbackup',created_at:'2026-11-15T12:00:00Z',components:['application','trackers','captures'],size:218000,encrypted:true};
  const manifest = {...backup,trackers:[{id:'default',label:'Main tracker'}]};
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) {
      try {return route.fulfill({contentType:url.pathname.endsWith('.css')?'text/css':'text/javascript',body:await readFile(new URL('..'+url.pathname,import.meta.url))});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/api/')) {
      const method = route.request().method(), name = url.pathname.split('/').slice(3).join('/');
      const body = method === 'POST' ? route.request().postDataJSON() : null;
      if(body)writes.push({name,body});
      if(name==='plan'&&body)plan={...plan,...body,has_password:!!body.password||plan.has_password};
      if(name==='cloud/connect'){cloudKind=body.kind;cloudStep=0;return route.fulfill({json:{ok:true,connection:{id:'cloud-account',kind:cloudKind,status:'connecting',question:'config_token'}}});}
      if(name==='cloud/continue'){cloudStep++;const question=cloudKind==='onedrive'&&cloudStep<3?(cloudStep===1?'config_driveid':'config_drive_ok'):'';return route.fulfill({json:{ok:true,connection:{id:'cloud-account',kind:cloudKind,status:question?'connecting':'connected',question,choices:[{value:'drive-one',label:'My OneDrive'}]}}});}
      const data = {plan:{plan},schedule:{schedule:{active:false}},list:{backups:populated?[backup]:[]},status:{jobs:populated?[{id:'job',path:backup.path,stage:'failed',local_status:'saved',remote_status:'failed',error:'Server unreachable',retry_at:1794750000}]:[]},create:{job:{id:'new',stage:'queued'}},validate:{validation:{ok:true,manifest,application_ready:true}},restore:{result:{message:'Tracker recovered. No sync pairs were created.'}},'recovery-key':{key:'generated-recovery-key-for-test'}}[name]||{};
      return route.fulfill({json:{ok:true,...data}});
    }
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html${theme?` data-cw-theme="${theme}"`:''}><head><meta name="viewport" content="width=device-width,initial-scale=1">${['themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','themes/flat','themes/original-coverage'].map(f=>`<link rel="stylesheet" href="/assets/${f}.css">`).join('')}</head><body><button id="open" onclick="openBackupRestore()">Backups</button><script src="/assets/helpers/backups.js"></script></body></html>`});
  });
  await page.goto('http://cw.test/');
  await page.locator('#open').click();
  await page.getByRole('heading',{name:'What to back up'}).waitFor();
  return {page,writes,empty:async()=>{populated=false;await page.evaluate(()=>CW.Backups.refresh());}};
}

test('backs up only selected contents and tracker instances',async t=>{
  const {page,writes}=await fixture(t);
  await page.getByText('Settings, connections, sync state, mappings, blocks and custom anime rules.',{exact:true}).waitFor();
  await page.getByLabel('CrossWatch application',{exact:true}).uncheck();
  await page.getByLabel('Saved captures',{exact:true}).uncheck();
  await page.getByLabel('All trackers',{exact:true}).uncheck();
  await page.getByLabel('Main tracker',{exact:true}).uncheck();
  await page.getByRole('button',{name:'Back up now',exact:true}).click();
  await page.getByText('Backup queued. You can close this dialog while it runs.').waitFor();
  assert.deepEqual(writes[0].body.components,['trackers']);
  assert.deepEqual(writes[0].body.trackers,['one']);
  assert.equal(writes[1].name,'create');
  await page.waitForFunction(()=>document.querySelector('#cw-backups-modal').getAttribute('aria-busy')==='false');
  await page.keyboard.press('Escape');
  assert.equal(await page.locator('#open').evaluate(n=>n===document.activeElement),true);
});

test('tracker restore defaults to a new instance and replacement requires confirmation',async t=>{
  const {page,writes}=await fixture(t,'flat-dark',1100,true);
  await page.getByRole('button',{name:'Restore',exact:true}).click();
  await page.getByRole('button',{name:'Check backup'}).click();
  await page.getByRole('button',{name:'Recover tracker',exact:true}).waitFor();
  assert.equal(await page.getByLabel('Restore action').inputValue(),'new_tracker');
  await page.getByLabel('Restore action').selectOption('replace');
  await page.getByRole('button',{name:'Restore and restart'}).click();
  await page.getByText('Confirm replacement before restoring.').waitFor();
  assert.equal(writes.filter(w=>w.name==='restore').length,0);
  await page.getByLabel('Replace the data contained in this backup',{exact:true}).check();
  await page.getByRole('button',{name:'Restore and restart'}).click();
  await page.getByText('Restore prepared. CrossWatch is restarting. Reopen CW to see the result.').waitFor();
  assert.equal(writes.at(-1).body.confirm,true);
});

for(const kind of ['onedrive','dropbox'])test(`${kind} account connection saves only its reference`,async t=>{
  const {page,writes}=await fixture(t,'flat-light',390);
  await page.getByRole('button',{name:'Connection settings'}).click();
  await page.getByLabel('Additional copy',{exact:true}).selectOption(kind);
  await page.getByRole('button',{name:kind==='onedrive'?'Connect OneDrive':'Connect Dropbox',exact:true}).click();
  assert.equal(await page.getByLabel('Run on your computer with rclone 1.76.0 or newer').inputValue(),`rclone authorize "${kind}"`);
  await mkdir(new URL('../build/backup-ui/',import.meta.url),{recursive:true});
  await page.getByLabel('Authorization result',{exact:true}).scrollIntoViewIfNeeded();
  await page.screenshot({path:new URL(`../build/backup-ui/cloud-${kind}.png`,import.meta.url).pathname.replace(/^\/([A-Z]:)/,'$1')});
  await page.getByLabel('Authorization result',{exact:true}).fill('fake-authorization-result');
  await page.getByRole('button',{name:'Continue connection'}).click();
  if(kind==='onedrive'){
    await page.getByLabel('OneDrive account').selectOption('drive-one');
    await page.getByRole('button',{name:'Use this OneDrive'}).click();
    await page.getByRole('button',{name:'Confirm OneDrive',exact:true}).click();
  }
  await page.getByText('Account authorization saved. Save the destination and test the connection.').waitFor();
  assert.equal(await page.getByLabel('Authorization result',{exact:true}).count(),0);
  await page.getByRole('button',{name:'Generate recovery key'}).click();
  await page.getByLabel('I saved this password or key outside CW').check();
  await page.getByRole('button',{name:'Save destination',exact:true}).click();
  await page.getByText('Destination saved. Test the connection before relying on additional copies.').waitFor();
  const saved=writes.findLast(w=>w.name==='plan').body.destination;
  assert.equal(saved.kind,kind);
  assert.equal(saved.connection,'cloud-account');
  assert.equal(JSON.stringify(saved).includes('fake-authorization-result'),false);
});

test('unusable rclone is explained in destination settings',async t=>{
  const {page}=await fixture(t,'flat-dark',1100,false,{rclone_available:false,rclone_error:'rclone could not start. Check its installation.'});
  await page.getByRole('button',{name:'Connection settings'}).click();
  await page.getByLabel('Additional copy',{exact:true}).selectOption('onedrive');
  await page.getByText('rclone could not start. Check its installation.',{exact:true}).waitFor();
});

for(const mountedRoot of ['/backups','/mnt/cw-backups'])test(`mounted folder shows its allowed root ${mountedRoot}`,async t=>{
  const {page}=await fixture(t,'flat-dark',1100,false,{mounted_root:mountedRoot});
  await page.getByRole('button',{name:'Connection settings'}).click();
  await page.getByLabel('Additional copy',{exact:true}).selectOption('folder');
  assert.equal(await page.getByLabel('Mounted folder inside CW').getAttribute('placeholder'),mountedRoot);
  await page.getByText(`Use ${mountedRoot} or a folder inside it. Mount it into the container, outside /config.`,{exact:true}).waitFor();
});

for(const theme of ['', 'flat-dark', 'flat-light'])for(const width of [1100,390]) {
  test(`backup dialog ${theme||'original'} ${width}`,async t=>{
    const {page,empty}=await fixture(t,theme,width,true);
    const folder=new URL('../build/backup-ui/',import.meta.url);
    await mkdir(folder,{recursive:true});
    const suffix=`${theme||'original'}-${width}`;
    const noOverflow=async()=>assert.equal(await page.locator('.br-dialog').evaluate(n=>n.scrollWidth<=n.clientWidth&&n.querySelector('.br-body').scrollWidth<=n.querySelector('.br-body').clientWidth),true);
    await noOverflow();
    await page.screenshot({path:new URL(`overview-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByRole('button',{name:'Connection settings'}).click();
    await page.getByLabel('Additional copy',{exact:true}).selectOption('webdav');
    await page.getByLabel('WebDAV URL').fill('https://cloud.example.com/remote.php/dav/files/user/');
    await page.getByRole('button',{name:'Generate recovery key'}).click();
    await page.getByLabel('Save this outside CW').waitFor();
    await page.getByLabel('I saved this password or key outside CW').check();
    await page.getByLabel('WebDAV URL').scrollIntoViewIfNeeded();
    await noOverflow();
    await page.screenshot({path:new URL(`destination-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    assert.equal(await page.locator('#cw-backups-modal').innerText().then(s=>s.includes('\u2014')),false);
    await empty();
    await page.getByRole('button',{name:'Connection settings'}).click();
    await page.locator('.br-body').evaluate(n=>n.scrollTop=0);
    await noOverflow();
    await page.screenshot({path:new URL(`empty-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
    await page.getByRole('button',{name:'Edit settings'}).click();
    await page.getByText('Days and encryption',{exact:true}).click();
    await page.getByLabel('Encrypt local backups',{exact:true}).check();
    await page.getByLabel('Time',{exact:true}).scrollIntoViewIfNeeded();
    await noOverflow();
    await page.screenshot({path:new URL(`schedule-${suffix}.png`,folder).pathname.replace(/^\/([A-Z]:)/,'$1')});
  });
}
