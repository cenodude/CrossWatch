/* tests/main-provider-info.browser.mjs */
/* CrossWatch - Mobile Sync Hub provider information sheets */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

for (const width of [390,320,1440]) test(`Sync Hub provider information at ${width}px`,async t=>{
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:850},hasTouch:width<760});
  page.setDefaultTimeout(6000);
  const errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('http://provider-info.test/**',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')){
      try{return route.fulfill({body:await readFile(new URL('..'+url.pathname,import.meta.url)),contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.svg')?'image/svg+xml':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    const css=['fonts/material-symbols-rounded','themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','themes/flat'];
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-theme="flat-dark" data-tab="main"><head><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body class="${width<760?'cw-has-bottom-nav':''}"><main id="layout"><section id="ops-card" class="card"><h2>Sync Hub</h2><div id="conn-badges" class="vip-badges"></div></section></main>${width<760?'<nav id="test-bottom-nav" style="position:fixed;inset:auto 0 0;height:65px;display:flex;align-items:center;justify-content:space-around;background:#101318">Home · My Media Hub · Tracker · More</nav>':''}</body></html>`});
  });
  await page.goto('http://provider-info.test/');
  await page.evaluate(()=>{
    window.CW={AuthState:{read:()=>({isManaged:false})},createAuthDotsController:()=>({syncObserver(){},refresh:async()=>true}),ProvidersUI:{openAuthProviderForm:async provider=>{window.openedProvider=provider;}}};
    window.getConfiguredProviders=()=>new Set(['PLEX','TRAKT']);
    window._cfgCache={};
    window.loadStatusCache=()=>({providers:{plex:{connected:true,plexpass:true,instance:'P01',instances:{default:{},P01:{}},instances_summary:{total:2,ok:2,used:['P01']},used_by:['pair','watcher']},trakt:{connected:false,username:'<img src=x onerror=alert(1)>',vip:true,instances:{default:{}}}}});
    window.showTab=async tab=>{window.openedTab=tab;};
  });
  await page.addScriptTag({url:'/assets/helpers/provider-meta.js'});
  await page.addScriptTag({url:'/assets/helpers/card-actions.js'});
  await page.addScriptTag({url:'/assets/js/main-status.js'});
  const plex=page.locator('.conn-pill[data-provider-key="PLEX"]');
  await plex.waitFor();
  if(width>=760){
    await plex.click();
    await page.waitForFunction(()=>window.openedProvider==='PLEX');
    assert.equal(await page.locator('.cw-provider-info-sheet').count(),0);
    assert.equal(await page.evaluate(()=>localStorage.getItem('cw.ui.plex.auth.instance.v1')),'P01');
  } else {
    await plex.tap();
    const sheet=page.locator('.cw-provider-info-sheet:popover-open');
    await sheet.waitFor();
    assert.match(await sheet.textContent(),/Connected instances: 2\/2/);
    assert.match(await sheet.textContent(),/Plan: Plex Pass/);
    assert.match(await sheet.textContent(),/Used by: Sync and Watcher/);
    assert.equal(await page.evaluate(()=>window.openedTab),undefined);
    await sheet.evaluate(el=>Promise.all(el.getAnimations().map(animation=>animation.finished)));
    const bounds=await sheet.boundingBox(),nav=await page.locator('#test-bottom-nav').boundingBox();
    assert.ok(bounds.x>=0&&bounds.x+bounds.width<=width&&bounds.y>=0);
    assert.ok(Math.abs(bounds.y+bounds.height-nav.y)<2);
    await mkdir('.tmp/main-mobile',{recursive:true});
    await page.screenshot({path:`.tmp/main-mobile/provider-info-${width}.png`});
    await page.goBack();
    await sheet.waitFor({state:'detached'});
    assert.equal(await plex.evaluate(el=>el===document.activeElement),true);
    await page.locator('.conn-pill[data-provider-key="TRAKT"]').tap();
    await sheet.waitFor();
    assert.match(await sheet.textContent(),/Status: Not connected/);
    assert.equal(await sheet.locator('img').count(),0);
    await sheet.getByRole('menuitem',{name:'Close',exact:true}).tap();
    await sheet.waitFor({state:'detached'});
    await plex.tap();
    await sheet.waitFor();
    await page.keyboard.press('Escape');
    await sheet.waitFor({state:'detached'});
    await plex.tap();
    await sheet.getByRole('menuitem',{name:'Connection settings',exact:true}).tap();
    await page.waitForFunction(()=>window.openedProvider==='PLEX');
    assert.equal(await page.evaluate(()=>localStorage.getItem('cw.ui.plex.auth.instance.v1')),'P01');
    await page.evaluate(()=>CW.AuthState.read=()=>({isManaged:true}));
    await plex.tap();
    await sheet.waitFor();
    assert.equal(await sheet.getByRole('menuitem',{name:'Connection settings',exact:true}).count(),0);
    await page.mouse.click(10,10);
    await sheet.waitFor({state:'detached'});
    assert.equal(await page.locator('main').evaluate(el=>el.inert),false);
  }
  assert.deepEqual(errors,[]);
});
