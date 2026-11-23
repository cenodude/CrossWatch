/* tests/maintenance-ui.browser.mjs */
/* CrossWatch - Maintenance layouts, cleanup confirmation and support downloads */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

async function fixture(t, theme='', width=1100, empty=false) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:900}});
  page.setDefaultTimeout(5000);
  const errors=[],requests=[];
  page.on('pageerror',e=>errors.push(e.message));
  t.after(()=>assert.deepEqual(errors,[]));
  const source=await readFile(new URL('../ui_frontend.py',import.meta.url),'utf8');
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
      if(url.pathname==='/api/snapshots/manifest')return route.fulfill({json:{providers:empty?[]:[{id:'CROSSWATCH',label:'CrossWatch',features:{watchlist:true,history:true,ratings:true,progress:true,collection:true},instances:[{id:'default',label:'Default'},{id:'family',label:'Family'}]},{id:'OTHER',label:'Read only',features:{}}]}});
      if(url.pathname==='/api/snapshots/tools/clear'){await cleanupGate;return route.fulfill({json:{ok:true}});}
      if(url.pathname.startsWith('/api/snapshots/capture-progress/'))return route.fulfill({json:{progress:{ok:true,done:true,percent:100,stage:'done',cleanup_results:{watchlist:{removed:3}}}}});
      if(url.pathname==='/api/maintenance/support/scopes')return route.fulfill({json:{pairs:empty?[]:[{id:'family',label:'Family',items:16,features:['history']}],totals:empty?{}:{pairs:3,baselines:4,items:16,orphan_baselines:0}}});
      if(['/api/maintenance/support/state','/api/maintenance/support/bundle'].includes(url.pathname))return route.fulfill({contentType:'application/octet-stream',headers:{'Content-Disposition':'attachment; filename="support-export.zip"'},body:'mock export'});
      if(url.pathname==='/api/provider-instances/CROSSWATCH')return route.fulfill({json:[{id:'default',label:'Default'}]});
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

for(const theme of ['', 'flat-dark','flat-light'])for(const width of [1100,390])test(`maintenance surfaces ${theme||'original'} ${width}`,async t=>{
  const {page}=await fixture(t,theme,width);
  const folder=new URL('../build/maintenance-ui/',import.meta.url);
  await mkdir(folder,{recursive:true});
  const screenshot=async name=>{
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    if(await page.getByRole('dialog').count())assert.equal(await page.getByRole('dialog').evaluate(n=>{const r=n.getBoundingClientRect();return r.left>=0&&r.top>=0&&r.right<=innerWidth&&r.bottom<=innerHeight;}),true);
    await page.screenshot({path:new URL(`${name}-${theme||'original'}-${width}.png`,folder).pathname.replace(/^\/(?=[A-Za-z]:)/,'')});
  };
  assert.equal(await page.locator('.mt-tool').count(),3);
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
  await page.getByRole('heading',{name:'Quick tasks'}).waitFor();
  await screenshot('tools');
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
});
