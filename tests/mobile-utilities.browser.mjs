/* tests/mobile-utilities.browser.mjs */
/* CrossWatch - Mobile import previews and live log controls */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

async function fixture(t,width,kind) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:844},hasTouch:width<760});
  page.setDefaultTimeout(6000);
  const errors=[],requests=[];
  page.on('pageerror',error=>errors.push(error.message));
  const source=await readFile(new URL('../ui_frontend.py',import.meta.url),'utf8');
  const detailsStart=source.indexOf('<div id="details"');
  const details=source.slice(detailsStart,source.indexOf('  <section id="stats-card"',detailsStart));
  const css=['fonts/material-symbols-rounded','themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','themes/flat'];
  const row={id:'r1',key:'r1',title:'A long movie title that must remain readable on a small phone',media_type:'movie',year:2026,feature:'history',status:'ready',ids:{tmdb:'123456',imdb:'tt123456',tvdb:'654321'},watched_at:'2026-11-14T00:00:00Z'};
  await page.route('http://utilities.test/**',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) {
      try{return route.fulfill({body:await readFile(new URL('..'+url.pathname,import.meta.url)),contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/api/')) {
      requests.push(url.pathname);
      let json={};
      if(url.pathname==='/api/import/options') json={sources:[{id:'auto',label:'Detect automatically'}],targets:[{id:'default',label:'My tracker',connected:true}],limits:{upload_bytes:26214400}};
      if(url.pathname==='/api/export/options') json={providers:['TRAKT'],instances:{TRAKT:[{id:'default',label:'Personal'}]},formats:{watchlist:['csv']},labels:{csv:'CSV'},capabilities:{csv:{media_types:['movie','episode']}},rewatches:{}};
      if(url.pathname==='/api/import/preview') json={import_id:'preview1',source:'auto',total:2};
      if(url.pathname==='/api/import/preview/preview1') json={rows:[row,{...row,id:'r2',title:'Already watched',status:'exists'}],filtered_total:2,total:2,summary:{total:2,ready:1,exists:1,by_status:{ready:1,exists:1}}};
      if(url.pathname==='/api/export/sample') json={items:[row],total:1,matched_total:1};
      if(url.pathname==='/api/import/plex-recovery/options') json={sources:[],targets:[],databases:[]};
      return route.fulfill({json});
    }
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-theme="flat-dark" data-tab="${kind==='logs'?'main':'import_export'}"><head><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}<style>body{padding:12px}#layout{display:block;padding:0}#ops-card{padding:8px}</style></head><body><main id="layout">${kind==='logs'?`<section id="ops-card"><button id="btn-details" onclick="toggleDetails()">View details</button>${details}`:'<section id="page-import_export"></section>'}</main></body></html>`});
  });
  await page.goto('http://utilities.test/');
  await page.evaluate(()=>{
    window.CW={AuthState:{read:()=>({isAdmin:true})}};
    window._cfgCache={};
    window.EventSource=class {constructor(){this.readyState=1;}addEventListener(){}close(){this.readyState=2;}};
    window.openLogs=()=>{window.logsOpened=true;};
  });
  await mkdir('.tmp/mobile-utilities',{recursive:true});
  return {page,errors,requests};
}

async function fits(page) {
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
}

for(const width of [320,390,1440]) test(`Import/export preserves selection and fits at ${width}px`,async t=>{
  const {page,errors,requests}=await fixture(t,width,'import');
  await page.addScriptTag({url:'/assets/helpers/icon-select.js'});
  await page.evaluate(async()=>{const {default: view}=await import('/assets/js/import-export/index.js');await view.mount(document.querySelector('#page-import_export'));});
  assert.equal(await page.locator('#ie-controls').isDisabled(),false);
  const mobile=width<760;
  assert.equal(await page.locator('.ie-tabs').isVisible(),!mobile);
  assert.equal(await page.locator('#ie-inclusions').isVisible(),!mobile);
  if(mobile) {
    await page.locator('#ie-options').click();
    await page.locator('[data-group="features"][value="ratings"]').uncheck();
    await page.locator('#ie-options').click();
    assert.equal(await page.locator('[data-group="features"][value="ratings"]').isChecked(),false);
  }
  await fits(page);
  await page.screenshot({path:`.tmp/mobile-utilities/import-${width}.png`,fullPage:true});
  await page.locator('#ie-file').setInputFiles({name:'history.csv',mimeType:'text/csv',buffer:Buffer.from('title,year\nExample,2026')});
  await page.locator('#ie-rows tr').nth(1).waitFor();
  await page.waitForFunction(()=>!document.querySelector('#ie-controls').disabled);
  assert.equal(await page.locator('#ie-rows input').nth(1).isDisabled(),true);
  if(mobile) await page.locator('#ie-selection-options').click();
  await page.locator('#ie-existing').check();
  await page.locator('#ie-rows input').nth(1).check();
  await page.locator('#ie-rows input').first().uncheck();
  assert.match(await page.locator('#ie-selected').textContent(),/1 item selected/);
  await fits(page);
  const row=page.locator('#ie-rows tr').first();
  assert.equal(await row.evaluate(el=>el.scrollWidth>el.clientWidth),false);
  await row.scrollIntoViewIfNeeded();
  await page.screenshot({path:`.tmp/mobile-utilities/preview-${width}.png`,fullPage:true});
  if(mobile) await page.locator('#ie-mobile-mode').selectOption('export');
  else await page.locator('[data-mode="export"]').click();
  await page.locator('#ie-export-setup').waitFor();
  await page.waitForFunction(()=>!document.querySelector('#ie-download').disabled);
  assert.ok(requests.includes('/api/export/sample'));
  await fits(page);
  if(mobile) {
    await page.setViewportSize({width:1440,height:900});
    assert.equal(await page.locator('.ie-tabs').isVisible(),true);
    assert.equal(await page.locator('#ie-inclusions').isVisible(),true);
    assert.equal(await page.locator('[data-mode="export"]').getAttribute('aria-pressed'),'true');
    await page.setViewportSize({width,height:844});
    await page.locator('#ie-mobile-mode').selectOption('recovery');
    await page.locator('#pr-start').waitFor();
    await fits(page);
  }
  assert.deepEqual(errors,[]);
});

for(const width of [320,390,1440]) test(`Sync details wrap messages and keep actions at ${width}px`,async t=>{
  const {page,errors}=await fixture(t,width,'logs');
  await page.addScriptTag({url:'/assets/helpers/card-actions.js'});
  await page.addScriptTag({url:'/assets/helpers/details-log.js'});
  await page.locator('#btn-details').click();
  await page.waitForFunction(()=>window._detailsTabsWired);
  await page.evaluate(()=>{
    for(let i=0;i<6;i++) {
      const row=document.createElement('div');row.className='wlog-line det-structured-line';
      row.innerHTML='<span class="wlog-tag">PLEX-WATCH</span><span class="wlog-level">INFO</span><span class="wlog-msg">Matching a very long episode name with a provider and an_identifier_that_does_not_have_spaces_1234567890.</span><span class="wlog-time">12:34:56</span>';
      document.querySelector('#det-log').append(row);
    }
  });
  await fits(page);
  const mobile=width<760;
  assert.equal(await page.locator('#det-more').isVisible(),mobile);
  assert.equal(await page.locator('#det-copy').isVisible(),!mobile);
  await page.screenshot({path:`.tmp/mobile-utilities/logs-${width}.png`,fullPage:true});
  if(mobile) {
    const row=page.locator('.det-structured-line').first();
    const message=await row.locator('.wlog-msg').boundingBox(),tag=await row.locator('.wlog-tag').boundingBox();
    assert.ok(message.y>=tag.y+tag.height);
    assert.ok(message.width>width-100);
    await page.locator('#det-more').click();
    assert.equal(await page.getByRole('menuitem',{name:'Analyze with assistant',exact:true}).count(),0);
    await page.getByRole('menuitem',{name:'Clear output',exact:true}).click();
    await page.waitForFunction(()=>!document.querySelector('#det-log').children.length);
    await page.locator('#det-more').click();
    await page.getByRole('menuitem',{name:'Open full logs',exact:true}).click();
    await page.waitForFunction(()=>window.logsOpened);
    await page.locator('#det-follow').click();
    assert.equal(await page.locator('#det-follow').getAttribute('aria-pressed'),'false');
    await page.locator('#det-more').click();
    await page.getByRole('menuitem',{name:'Close details',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('#details').classList.contains('hidden'));
    assert.equal(await page.locator('#btn-details').getAttribute('aria-expanded'),'false');
  }
  assert.deepEqual(errors,[]);
});
