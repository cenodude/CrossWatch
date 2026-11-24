/* tests/maintenance-ui.browser.mjs */
/* CrossWatch - Maintenance layouts, cleanup confirmation and support downloads */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import './i18n-browser-setup.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';
import {englishFrontendSource} from './i18n-test-helper.mjs';

async function fixture(t, theme='', width=1100, empty=false, api=null) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:900}});
  page.setDefaultTimeout(5000);
  const errors=[],requests=[];
  page.on('pageerror',e=>errors.push(e.message));
  t.after(()=>assert.deepEqual(errors,[]));
  const source=englishFrontendSource();
  const start=source.lastIndexOf('        <section class="cw-settings-pane cw-page-maintenance"');
  const pane=source.slice(start,source.indexOf('\n        </section>',start)+19);
  let releaseCleanup;
  const cleanupGate=new Promise(resolve=>{releaseCleanup=resolve;});
  await page.route('**/*',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) {
      try{return route.fulfill({contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript',body:await readFile(new URL('..'+url.pathname,import.meta.url))});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/api/')) {
      requests.push({url,method:route.request().method(),body:route.request().postDataJSON()});
      if(api && await api(route,url)) return;
      if(url.pathname==='/api/snapshots/manifest')return route.fulfill({json:{providers:empty?[]:[{id:'CROSSWATCH',label:'CrossWatch',features:{watchlist:true,history:true,ratings:true,progress:true,collection:true},instances:[{id:'default',label:'Default'},{id:'family',label:'Family'}]},{id:'OTHER',label:'Read only',features:{}}]}});
      if(url.pathname==='/api/snapshots/tools/clear'){await cleanupGate;return route.fulfill({json:{ok:true}});}
      if(url.pathname.startsWith('/api/snapshots/capture-progress/'))return route.fulfill({json:{progress:{ok:true,done:true,percent:100,stage:'done',cleanup_results:{watchlist:{removed:3}}}}});
      if(url.pathname==='/api/maintenance/support/scopes')return route.fulfill({json:{pairs:empty?[]:[{id:'family',label:'Family',items:16,features:['history']}],totals:empty?{}:{pairs:3,baselines:4,items:16,orphan_baselines:0}}});
      if(['/api/maintenance/support/state','/api/maintenance/support/bundle'].includes(url.pathname))return route.fulfill({contentType:'application/octet-stream',headers:{'Content-Disposition':'attachment; filename="support-export.zip"'},body:'mock export'});
      if(url.pathname==='/api/provider-instances/CROSSWATCH')return route.fulfill({json:empty?[]:[{id:'default',label:'Default'},{id:'family',label:'Family'}]});
      return route.fulfill({json:{ok:true}});
    }
    const css=['themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','ui-shell','themes/flat','themes/original-coverage','js/modals/core/styles','fonts/material-symbols-rounded'];
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html${theme?` data-cw-theme="${theme}"`:''}><head><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(f=>`<link rel="stylesheet" href="/assets/${f}.css">`).join('')}<style>#page-settings{padding:24px;max-width:1000px;margin:auto}.cw-page-maintenance{display:block!important}#page-maintenance[hidden]{display:none}</style></head><body><main id="page-settings">${pane}</main><main id="page-maintenance" hidden></main><script src="/assets/helpers/mobile-experience.js"></script><script src="/assets/helpers/dialog-focus.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/profile-select.js"></script><script type="module">
      import {ModalHost} from '/assets/js/modals/core/host.js';
      import cleanup from '/assets/js/modals/provider-cleanup/index.js';
      import support from '/assets/js/modals/support/index.js';
      import maintenance from '/assets/js/maintenance/index.js';
      const host=new ModalHost();
      window.openProviderCleanupModal=()=>host.mount(cleanup);
      window.openSupportModal=()=>host.mount(support);
      window.openMaintenance=()=>{document.querySelector('#page-settings').hidden=true;document.querySelector('#page-maintenance').hidden=false;maintenance.mount(document.querySelector('#page-maintenance'));};
      window.openBackupRestore=()=>{window.openedBackup=true;};
      window.restartCrossWatch=()=>{window.requestedRestart=true;};
    </script></body></html>`});
  });
  await page.goto('http://cw.test/');
  await page.waitForFunction(()=>!!window.openSupportModal);
  return {page,requests,releaseCleanup};
}

test('provider cleanup keeps selection, confirmation and running lock',async t=>{
  const {page,requests,releaseCleanup}=await fixture(t);
  await page.getByRole('button',{name:/^Provider cleanup/}).click();
  await page.locator('#ccc-instance-picker .cw-icon-select-btn').click();
  await page.getByRole('listbox').getByRole('button',{name:'Family',exact:true}).click();
  assert.equal(await page.locator('#ccc-clear-selected').isDisabled(),true);
  await page.getByRole('button',{name:'Watchlist',exact:true}).click();
  await page.getByRole('button',{name:'Clear selected data'}).click();
  assert.equal(requests.some(r=>r.method==='POST'),false);
  await page.getByRole('button',{name:'Confirm clear'}).click();
  await page.waitForFunction(()=>document.querySelector('#ccc-close')?.disabled);
  await page.keyboard.press('Escape');
  assert.equal(await page.getByRole('dialog').count(),1);
  releaseCleanup();
  await page.getByText('watchlist: removed 3',{exact:true}).waitFor();
  const write=requests.find(r=>r.method==='POST');
  assert.equal(write.body.instance,'family');
  assert.deepEqual(write.body.features,['watchlist']);
  await page.getByRole('button',{name:'Close',exact:true}).click();
  assert.equal(await page.getByRole('dialog').count(),0);
});

test('support downloads respect the selected pair and included sections',async t=>{
  const {page,requests}=await fixture(t);
  await page.getByRole('button',{name:/^Support/}).click();
  await page.locator('.sup-card').filter({hasText:'Support bundle'}).locator('.cw-icon-select-btn').click();
  await page.getByRole('listbox').getByRole('button',{name:/Family/}).click();
  await page.getByLabel('Logs',{exact:true}).uncheck();
  const download=page.waitForEvent('download');
  await page.getByRole('button',{name:'Download support bundle'}).click();
  assert.equal((await download).suggestedFilename(),'support-export.zip');
  await page.getByText('Export complete',{exact:true}).waitFor();
  const request=requests.find(r=>r.url.pathname.endsWith('/bundle'));
  assert.equal(request.url.searchParams.get('pairs'),'family');
  assert.equal(request.url.searchParams.getAll('include').includes('logs'),false);
  assert.equal(request.url.searchParams.getAll('include').length,3);
});

for(const theme of ['', 'flat-dark','flat-light'])for(const width of [1100,390,320])test(`maintenance surfaces ${theme||'original'} ${width}`,async t=>{
  const {page}=await fixture(t,theme,width);
  const folder=new URL('../build/maintenance-ui/',import.meta.url);
  await mkdir(folder,{recursive:true});
  const screenshot=async name=>{
    await page.evaluate(()=>Promise.all(document.getAnimations().filter(a=>a.effect?.getComputedTiming().iterations!==Infinity).map(a=>a.finished.catch(()=>{}))));
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    if(await page.getByRole('dialog').count())assert.equal(await page.getByRole('dialog').evaluate(n=>{const r=n.getBoundingClientRect();return r.left>=0&&r.top>=0&&r.right<=innerWidth&&r.bottom<=innerHeight;}),true);
    await page.screenshot({path:new URL(`${name}-${theme||'original'}-${width}.png`,folder).pathname.replace(/^\/(?=[A-Za-z]:)/,'')});
  };
  assert.equal(await page.locator('.mt-tool').count(),3);
  assert.equal(await page.locator('.mt-page-head').evaluate(n=>getComputedStyle(n).position),'static');
  assert.equal(await page.locator('.mt-page-head').evaluate(n=>getComputedStyle(n).backgroundColor),'rgba(0, 0, 0, 0)');
  await screenshot('landing');
  await page.getByRole('button',{name:/^Provider cleanup/}).click();
  await page.locator('#ccc-provider option[value="CROSSWATCH"]').waitFor({state:'attached'});
  await page.getByRole('button',{name:'Watchlist',exact:true}).click();
  assert.equal(await page.getByRole('button',{name:'Watchlist',exact:true}).getAttribute('aria-pressed'),'true');
  await screenshot('cleanup');
  assert.equal(await page.locator('.mt-body').evaluate(n=>n.scrollWidth<=n.clientWidth),true);
  await page.locator('#ccc-provider').selectOption('OTHER',{force:true});
  await page.getByText('No cleanup options are available for this instance.',{exact:true}).waitFor();
  assert.equal(await page.locator('#ccc-clear-selected').isDisabled(),true);
  await page.getByRole('button',{name:'Close',exact:true}).click();
  await page.getByRole('button',{name:/^Support/}).click();
  await page.getByLabel('Logs',{exact:true}).uncheck();
  await screenshot('support');
  assert.equal(await page.locator('.mt-body').evaluate(n=>n.scrollWidth<=n.clientWidth),true);
  await page.getByRole('button',{name:'Close',exact:true}).click();
  await page.getByRole('button',{name:/^Maintenance tools/}).click();
  await page.getByRole('heading',{name:'What needs fixing?'}).waitFor();
  await screenshot('tools');
  await page.locator('[data-section="sync"]').click();
  await page.locator('[data-pane="sync"] [data-op="pair-state"]').click();
  await page.locator('#cxm-pair-select option[value="family"]').waitFor({state:'attached'});
  await page.locator('#cxm-pair-select').selectOption('family',{force:true});
  await screenshot('pair');
  await page.locator('#cxm-task-cancel').click();
  await page.locator('[data-section="tracker"]').click();
  await page.locator('#cxm-cw-profile option[value="family"]').waitFor({state:'attached'});
  await page.locator('#cxm-cw-profile').selectOption('family',{force:true});
  await page.locator('#cxm-cw-state').check();
  await page.locator('#cxm-cw-review').click();
  await screenshot('tracker');
  assert.ok((await page.locator('#cxm-cw-clear').boundingBox()).height<=48);
  await page.locator('[data-section="advanced"]').click();
  await page.locator('[data-group="danger"]>summary').click();
  await screenshot('advanced');
});

for(const theme of ['', 'flat-dark','flat-light'])test(`maintenance heading stays below the settings menu ${theme||'original'}`,async t=>{
  const {page}=await fixture(t,theme);
  const source=englishFrontendSource();
  const start=source.indexOf('\n<header>');
  const header=source.slice(start,source.indexOf('</header>',start)+9).replace(/__CW_[A-Z_]+__/g,'');
  await page.evaluate(markup=>{
    document.body.insertAdjacentHTML('afterbegin',markup);
    document.body.insertAdjacentHTML('beforeend','<div style="height:1200px"></div>');
    document.querySelector('#cw-settings-menu').classList.remove('hidden');
    document.querySelector('#tab-settings').setAttribute('aria-expanded','true');
  },header);
  const headingTop=await page.locator('.mt-page-head').evaluate(n=>n.getBoundingClientRect().top);
  await page.evaluate(()=>window.scrollTo(0,100));
  await page.evaluate(()=>new Promise(requestAnimationFrame));
  assert.equal(await page.locator('.mt-page-head').evaluate(n=>n.getBoundingClientRect().top+scrollY),headingTop);
  assert.equal(await page.locator('#cw-settings-menu').evaluate(menu=>[...menu.querySelectorAll('button')].every(button=>{
    const r=button.getBoundingClientRect();
    return button.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));
  })),true);
});

for(const theme of ['', 'flat-dark','flat-light'])test(`empty maintenance dialogs ${theme||'original'}`,async t=>{
  const {page}=await fixture(t,theme,390,true);
  await page.getByRole('button',{name:/^Provider cleanup/}).click();
  await page.getByText('No configured providers found.',{exact:true}).waitFor();
  assert.equal(await page.locator('#ccc-clear-selected').isDisabled(),true);
  await page.getByRole('button',{name:'Close',exact:true}).click();
  await page.getByRole('button',{name:/^Support/}).click();
  assert.equal(await page.locator('#sup-state-scope option').count(),1);
  assert.equal(await page.getByRole('button',{name:'Download sync state',exact:true}).isEnabled(),true);
  await page.getByRole('button',{name:'Close',exact:true}).click();
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-section="tracker"]').click();
  await page.waitForFunction(()=>document.querySelector('#cxm-cw-profile').textContent.includes('No Tracker instances configured'));
  await page.locator('#cxm-cw-state').check();
  assert.equal(await page.locator('#cxm-cw-review').isDisabled(),true);
  assert.equal(await page.locator('#cxm-cw-export').isDisabled(),true);
  await page.locator('[data-section="sync"]').click();
  await page.locator('[data-pane="sync"] [data-op="pair-state"]').click();
  await page.waitForFunction(()=>document.querySelector('#cxm-pair-select').textContent.includes('No sync pairs configured'));
  assert.equal(await page.locator('#cxm-pair-reset').isDisabled(),true);
});


test('quick task opens without writing and records the existing action result',async t=>{
  const {page,requests}=await fixture(t);
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-pane="quick"] [data-op="playing"]').click();
  assert.equal(requests.some(r=>r.method==='POST'),false);
  await page.locator('#cxm-run-action').click();
  await page.locator('#cxm-task-status.ok').waitFor();
  assert.equal(requests.filter(r=>r.method==='POST').length,1);
  assert.equal(requests.find(r=>r.method==='POST').url.pathname,'/api/maintenance/reset-currently-watching');
  await page.locator('#cxm-task-back').click();
  await page.locator('#cxm-history-toggle').click();
  await page.locator('#cxm-history-list').getByText('Clear the Currently Playing list',{exact:true}).waitFor();
});

test('Tracker cleanup requires review, resets on instance change and locks writes',async t=>{
  let release;
  const gate=new Promise(resolve=>{release=resolve;});
  const {page,requests}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/crosswatch-tracker/clear')return false;
    await gate;
    await route.fulfill({json:{ok:true}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-section="tracker"]').click();
  await page.locator('#cxm-cw-profile option[value="family"]').waitFor({state:'attached'});
  assert.equal(await page.locator('#cxm-cw-review').isDisabled(),true);
  await page.locator('#cxm-cw-state').check();
  await page.locator('#cxm-cw-review').click();
  assert.equal(requests.some(r=>r.method==='POST'),false);
  await page.locator('#cxm-cw-profile').selectOption('family',{force:true});
  assert.equal(await page.locator('#cxm-cw-state').isChecked(),false);
  assert.equal(await page.locator('#cxm-cleanup-review').isHidden(),true);
  await page.locator('#cxm-cw-snaps').check();
  await page.locator('#cxm-cw-review').click();
  assert.match(await page.locator('#cxm-cleanup-title').textContent(),/Family/);
  await page.locator('#cxm-cw-clear').click();
  assert.equal(await page.locator('#cxm-cw-profile').isDisabled(),true);
  assert.equal(await page.locator('[data-section="sync"]').isDisabled(),true);
  release();
  await page.locator('#cxm-archive-status.ok').waitFor();
  const write=requests.find(r=>r.method==='POST');
  assert.deepEqual(write.body,{clear_state:false,clear_snapshots:true,provider_instance:'family'});
  assert.equal(await page.locator('#cxm-cleanup-review').isHidden(),true);
});

test('pair reset confirms the chosen scope and requires a selected feature',async t=>{
  const {page,requests}=await fixture(t,'',390);
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-section="sync"]').click();
  await page.locator('[data-pane="sync"] [data-op="pair-state"]').click();
  await page.locator('#cxm-pair-select option[value="family"]').waitFor({state:'attached'});
  assert.equal(await page.locator('#cxm-pair-reset').isDisabled(),true);
  await page.locator('#cxm-pair-select').selectOption('family',{force:true});
  await page.locator('#cxm-pair-features input').uncheck();
  assert.equal(await page.locator('#cxm-pair-reset').isDisabled(),true);
  await page.locator('#cxm-pair-features input').check();
  await page.locator('#cxm-pair-retry').uncheck();
  await page.locator('#cxm-pair-reset').click();
  await page.getByRole('dialog').getByRole('button',{name:'Cancel',exact:true}).click();
  assert.equal(requests.some(r=>r.method==='POST'),false);
  await page.locator('#cxm-pair-reset').click();
  await page.getByRole('dialog').getByRole('button',{name:'Continue',exact:true}).click();
  await page.locator('#cxm-task-status.ok').waitFor();
  const write=requests.find(r=>r.method==='POST');
  assert.equal(write.url.pathname,'/api/maintenance/clear-pair-state');
  assert.deepEqual(write.body,{pair_id:'family',features:['history'],clear_retry:false});
});

test('failed Tracker loading blocks cleanup and backup until retry succeeds',async t=>{
  let fail=true;
  const {page,requests}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/provider-instances/CROSSWATCH'||!fail)return false;
    await route.fulfill({status:503,json:{error:'unavailable'}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-section="tracker"]').click();
  await page.locator('#cxm-tracker-retry').waitFor();
  await page.locator('#cxm-cw-state').check();
  assert.equal(await page.locator('#cxm-cw-review').isDisabled(),true);
  assert.equal(await page.locator('#cxm-cw-export').isDisabled(),true);
  assert.equal(requests.some(r=>r.method==='POST'),false);
  fail=false;
  await page.locator('#cxm-tracker-retry').click();
  await page.waitForFunction(()=>!document.querySelector('#cxm-cw-review').disabled);
  assert.equal(await page.locator('#cxm-cw-export').isDisabled(),false);
});

test('legacy links select the right section and preserve the return destination',async t=>{
  const {page}=await fixture(t);
  for(const [group,section] of [['archive','tracker'],['events','advanced'],['sync','sync']]) {
    await page.evaluate(group=>{location.hash='#maintenance?group='+group+'&returnTo=%2F%3Fmain%3D1%23settings';window.openMaintenance();},group);
    await page.locator(`[data-section="${section}"][aria-current]`).waitFor();
    assert.equal(await page.locator('#cxm-back').getAttribute('href'),'/?main=1#settings');
  }
  await page.locator('[data-section="quick"]').click();
  assert.equal(new URLSearchParams(new URL(page.url()).hash.split('?')[1]).get('returnTo'),'/?main=1#settings');
});


test('a failed pair inventory is reported as unavailable, not an empty configuration',async t=>{
  const {page,requests}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/support/scopes')return false;
    await route.fulfill({json:{ok:false,error:'internal_error'}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-section="sync"]').click();
  await page.locator('[data-pane="sync"] [data-op="pair-state"]').click();
  await page.waitForFunction(()=>document.querySelector('#cxm-pair-select').textContent.includes('Could not load sync pairs'));
  assert.equal(await page.locator('#cxm-pair-reset').isDisabled(),true);
  assert.equal(requests.some(r=>r.method==='POST'),false);
});

test('storage summary follows an automatically selected non-default Tracker instance',async t=>{
  const {page}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname==='/api/provider-instances/CROSSWATCH') {
      await route.fulfill({json:[{id:'family',label:'Family'}]});
      return true;
    }
    if(url.pathname==='/api/maintenance/crosswatch-tracker') {
      const instance=url.searchParams.get('provider_instance');
      await route.fulfill({json:{ok:true,root:'/tracker/'+instance,counts:{state_files:instance==='family'?5:0,snapshots:0}}});
      return true;
    }
    return false;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.waitForFunction(()=>document.querySelector('#cxm-tracker-root').textContent==='/tracker/default');
  await page.locator('[data-section="tracker"]').click();
  await page.locator('#cxm-cw-profile option[value="family"]').waitFor({state:'attached'});
  await page.locator('[data-section="advanced"]').click();
  await page.locator('#cxm-overview-status>summary').click();
  await page.locator('#cxm-tracker-root').getByText('/tracker/family',{exact:true}).waitFor();
  assert.equal(await page.locator('#cxm-tracker-count').textContent(),'Tracker 5 state \u00b7 0 snapshots');
});

test('resumed health checks keep issues visible as an error result',async t=>{
  let reads=0;
  const {page}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/jobs/current')return false;
    await route.fulfill({json:{id:'health-job',action:'database-health',state:++reads===1?'running':'completed',result:{ok:true,healthy:false,integrity:'failed'}}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('#cxm-status').getByText(/issues found/).waitFor();
  assert.equal(await page.locator('#cxm-status').evaluate(n=>n.classList.contains('err')),true);
});

test('opening maintenance shows dated history without replaying completed jobs or writing',async t=>{
  const {page,requests}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/jobs/current')return false;
    await route.fulfill({json:{id:'old-job',action:'database-health',state:'completed',started_at:1,result:{ok:true}}});
    return true;
  });
  const at=Date.UTC(2026,10,21,14,8,3);
  await page.evaluate(at=>{
    localStorage.setItem('cw.maintenance.history.v1:admin',JSON.stringify([
      {key:'playing',status:'success',at},
      {key:'database-health',status:'success',at:at-1000},
    ]));
    window.openMaintenance();
  },at);
  await page.waitForFunction(()=>!document.querySelector('#cxm-cache-count').textContent.includes('not loaded'));
  assert.equal(await page.locator('#cxm-status').isHidden(),true);
  const date=await page.evaluate(at=>new Date(at).toLocaleString(window.CW?.I18n?.locale),at);
  assert.equal(await page.locator('[data-pane="quick"] [data-op="playing"] .task-status').textContent(),`Last run: Completed · ${date}`);
  const checkDate=await page.evaluate(at=>new Date(at-1000).toLocaleString(window.CW?.I18n?.locale),at);
  assert.equal(await page.locator('[data-pane="quick"] [data-op="database-health"] .task-status').textContent(),`Last run: Completed · ${checkDate}`);
  assert.equal(await page.locator('#cxm-last-maintenance').textContent(),`Last maintenance: ${date} · Completed`);
  assert.equal(requests.filter(r=>r.url.pathname==='/api/maintenance/jobs/current').length,1);
  assert.equal(requests.some(r=>r.method!=='GET'),false);
  await page.locator('[data-pane="quick"] [data-op="database-health"]').click();
  assert.equal(requests.some(r=>r.method!=='GET'),false);
  assert.equal(await page.locator('#cxm-task-status').isHidden(),true);
});

for (const [state,result,label] of [
  ['completed',{ok:true,healthy:true},'Completed'],
  ['completed',{ok:true,healthy:false},'Issues found'],
  ['failed',{ok:false,error:'check_failed'},'Failed'],
  ['interrupted',{ok:false,error:'worker_interrupted'},'Failed'],
]) test(`resumed ${state} ${label} is recorded once and survives reload`,async t=>{
  let reads=0;
  const started_at=1795269600,duration_ms=2500;
  const {page,requests}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/jobs/current')return false;
    await route.fulfill({json:{id:'resume-job',action:'database-health',state:++reads===1?'running':state,started_at,duration_ms,result}});
    return true;
  });
  const open=async()=>{
    await page.waitForFunction(()=>!!window.openMaintenance);
    await page.evaluate(()=>window.openMaintenance());
    await page.waitForFunction(()=>!document.querySelector('#cxm-cache-count').textContent.includes('not loaded'));
  };
  const verify=async()=>{
    const entries=await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.maintenance.history.v1:admin')));
    assert.equal(entries.length,1);
    assert.equal(entries[0].jobId,'resume-job');
    assert.equal(entries[0].at,started_at*1000+duration_ms);
    assert.match(await page.locator('[data-pane="quick"] [data-op="database-health"] .task-status').textContent(),new RegExp(`Last run: ${label}`));
    assert.match(await page.locator('#cxm-last-maintenance').textContent(),new RegExp(label));
    await page.locator('#cxm-history-toggle').click();
    assert.equal(await page.locator('#cxm-history-list li').count(),1);
    assert.equal(await page.locator('#cxm-history-list .task-status').textContent(),label);
  };
  await open();
  await verify();
  for(let reload=0;reload<2;reload++) {
    await page.reload();
    await open();
    if(label==='Completed') assert.equal(await page.locator('#cxm-status').isHidden(),true);
    else assert.match(await page.locator('#cxm-status.err').textContent(),/^Previous maintenance/);
    await verify();
  }
  assert.equal(requests.some(request=>request.method!=='GET'),false);
});

test('existing legacy job history is adopted once across page reloads',async t=>{
  const job={id:'legacy-job',action:'database-health',state:'completed',started_at:1795269600,duration_ms:2500,result:{ok:true,healthy:true}};
  const at=job.started_at*1000+job.duration_ms;
  const {page,requests}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/jobs/current')return false;
    await route.fulfill({json:job});
    return true;
  });
  await page.evaluate(at=>localStorage.setItem('cw.maintenance.history.v1:admin',JSON.stringify([
    {key:'database-health',status:'success',at:at+850,batch:false},
    {key:'database-health',status:'success',at:at-2000,batch:false},
    {key:'playing',status:'success',at:at+850,batch:false},
  ])),at);
  for(let visit=0;visit<3;visit++) {
    if(visit) await page.reload();
    await page.waitForFunction(()=>!!window.openMaintenance);
    await page.evaluate(()=>window.openMaintenance());
    await page.waitForFunction(()=>!document.querySelector('#cxm-cache-count').textContent.includes('not loaded'));
    const entries=await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.maintenance.history.v1:admin')));
    assert.equal(entries.length,3);
    assert.equal(entries.filter(entry=>entry.jobId===job.id).length,1);
    assert.equal(entries.find(entry=>entry.jobId===job.id).at,at);
    assert.equal(entries.filter(entry=>entry.key==='database-health'&&!entry.jobId).length,1);
    assert.equal(await page.locator('#cxm-status').isHidden(),true);
    await page.locator('#cxm-history-toggle').click();
    assert.equal(await page.locator('#cxm-history-list li').count(),3);
  }
  assert.equal(requests.some(request=>request.method!=='GET'),false);
});

test('a job run on this page keeps one dated history entry after reload',async t=>{
  let started=false;
  const job={id:'manual-job',action:'database-health',state:'completed',started_at:1795269600,duration_ms:1200,result:{ok:true,healthy:true}};
  const {page,requests}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname==='/api/maintenance/jobs') {
      started=true;
      await route.fulfill({json:job});
      return true;
    }
    if(url.pathname!=='/api/maintenance/jobs/current')return false;
    await route.fulfill({json:started?job:{state:'idle'}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-pane="quick"] [data-op="database-health"]').click();
  await page.locator('#cxm-run-action').click();
  await page.locator('#cxm-task-status.ok').waitFor();
  const before=await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.maintenance.history.v1:admin')));
  assert.equal(before.length,1);
  assert.equal(before[0].jobId,job.id);
  assert.equal(before[0].at,job.started_at*1000+job.duration_ms);
  await page.reload();
  await page.waitForFunction(()=>!!window.openMaintenance);
  await page.evaluate(()=>window.openMaintenance());
  await page.waitForFunction(()=>!document.querySelector('#cxm-cache-count').textContent.includes('not loaded'));
  assert.equal(await page.locator('#cxm-status').isHidden(),true);
  assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.maintenance.history.v1:admin'))),before);
  assert.equal(requests.filter(request=>request.method==='POST').length,1);
});

test('invalid action responses cannot be recorded as successful maintenance',async t=>{
  const {page}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/reset-currently-watching')return false;
    await route.fulfill({contentType:'text/html',body:'<html>Service unavailable</html>'});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-pane="quick"] [data-op="playing"]').click();
  await page.locator('#cxm-run-action').click();
  await page.locator('#cxm-task-status.err').waitFor();
  await page.locator('#cxm-history-toggle').click();
  assert.equal(await page.locator('#cxm-history-list .task-status').textContent(),'Failed');
});

test('opening technical details keeps the failed action result visible',async t=>{
  const {page}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/reset-currently-watching')return false;
    await route.fulfill({json:{ok:false,error:'cleanup_failed'}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-pane="quick"] [data-op="playing"]').click();
  await page.locator('#cxm-run-action').click();
  await page.locator('#cxm-task-status.err').waitFor();
  const details=page.waitForResponse('**/api/maintenance/action-status/playing');
  await page.locator('#cxm-technical-details>summary').click();
  await details;
  await page.waitForFunction(()=>!document.querySelector('#cxm-action-insight').classList.contains('loading'));
  assert.equal(await page.locator('#cxm-task-status').isVisible(),true);
  assert.match(await page.locator('#cxm-task-status').textContent(),/cleanup_failed/);
});

for(const theme of ['', 'flat-dark','flat-light'])test(`long Tracker names fit mobile cleanup ${theme||'original'}`,async t=>{
  const {page}=await fixture(t,theme,320,false,async(route,url)=>{
    if(url.pathname!=='/api/provider-instances/CROSSWATCH')return false;
    await route.fulfill({json:[{id:'family',label:'FamilyTrackerWithAnExtremelyLongUnbrokenInstanceName'}]});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-section="tracker"]').click();
  await page.locator('#cxm-cw-profile option[value="family"]').waitFor({state:'attached'});
  await page.locator('#cxm-cw-state').check();
  await page.locator('#cxm-cw-review').click();
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,JSON.stringify(await page.locator('.cw-maint *').evaluateAll(nodes=>nodes.filter(n=>n.getBoundingClientRect().right>innerWidth).map(n=>({tag:n.tagName,id:n.id,cls:n.className})))));
  const folder=new URL('../build/maintenance-ui/',import.meta.url);
  await mkdir(folder,{recursive:true});
  await page.screenshot({path:new URL(`tracker-long-${theme||'original'}.png`,folder).pathname.replace(/^\/(?=[A-Za-z]:)/,''),fullPage:true});
});

test('a slow details refresh does not keep a completed action locked',async t=>{
  let release, reads=0;
  const gate=new Promise(resolve=>{release=resolve;});
  t.after(()=>release());
  const {page}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname!=='/api/maintenance/action-status/playing')return false;
    if(++reads>1)await gate;
    await route.fulfill({json:{ok:true,metrics:[]}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.locator('[data-pane="quick"] [data-op="playing"]').click();
  const details=page.waitForResponse('**/api/maintenance/action-status/playing');
  await page.locator('#cxm-technical-details>summary').click();
  await details;
  const refreshed=page.waitForResponse('**/api/maintenance/action-status/playing');
  await page.locator('#cxm-run-action').click();
  await page.locator('#cxm-task-status.ok').waitFor();
  assert.equal(await page.locator('#cxm-run-action').isEnabled(),true);
  await page.locator('[data-section="sync"]').click();
  release();
  await refreshed;
  assert.equal(await page.locator('#cxm-task-panel').isHidden(),true);
});

test('a slow storage refresh does not hide the result or lock navigation after cleanup',async t=>{
  let release, written=false;
  const gate=new Promise(resolve=>{release=resolve;});
  t.after(()=>release());
  const {page}=await fixture(t,'',390,false,async(route,url)=>{
    if(url.pathname==='/api/maintenance/clear-cache')written=true;
    if(url.pathname!=='/api/maintenance/provider-cache')return false;
    if(written)await gate;
    await route.fulfill({json:{ok:true,count:0}});
    return true;
  });
  await page.evaluate(()=>window.openMaintenance());
  await page.waitForFunction(()=>document.querySelector('#cxm-cache-count').textContent==='Provider cache 0 files');
  await page.locator('[data-section="sync"]').click();
  await page.locator('[data-pane="sync"] [data-op="cache"]').click();
  await page.locator('#cxm-run-action').click();
  await page.locator('#cxm-task-status.ok').waitFor();
  assert.equal(await page.locator('#cxm-task-cancel').isEnabled(),true);
  await page.locator('[data-section="tracker"]').click();
  release();
  assert.equal(await page.locator('[data-section="tracker"]').getAttribute('aria-current'),'page');
});
