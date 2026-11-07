/* tests/capture-status.browser.mjs */
/* CrossWatch - Captured status visibility and restore results across themes. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {chromium} from 'playwright';

for (const theme of ['original','flat-dark','flat-light']) {
  test(`capture status restore in ${theme}`, async t => {
    const browser = await chromium.launch({headless:true});
    t.after(()=>browser.close());
    const page = await browser.newPage({viewport:{width:1280,height:1000}});
    page.setDefaultTimeout(7000);
    const errors=[], writes=[];
    page.on('pageerror', error=>errors.push(error.message));
    const capture = {path:'history.json',provider:'SIMKL',instance:'default',feature:'history',created_at:'2026-11-07T12:00:00Z',stats:{count:0,title_statuses:1},items:{}};
    const legacy = {...capture,path:'legacy.json',stats:{count:0}};
    await page.route('**/*', async route=>{
      const url=new URL(route.request().url()), path=url.pathname;
      if (path.startsWith('/assets/')) {
        try {return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':'text/javascript'});}
        catch {return route.fulfill({status:404,body:''});}
      }
      if (path==='/api/snapshots/manifest') return route.fulfill({json:{providers:[{id:'SIMKL',label:'SIMKL',configured:true,features:{history:true},instances:[{id:'default',label:'Default',configured:true}]}]}});
      if (path==='/api/snapshots/list') return route.fulfill({json:{snapshots:[capture,legacy]}});
      if (path==='/api/snapshots/read') return route.fulfill({json:{snapshot:url.searchParams.get('path')==='legacy.json'?legacy:capture}});
      if (path==='/api/snapshots/restore') {
        writes.push(route.request().postDataJSON());
        return route.fulfill({json:{result:{ok:true,added:0,removed:0,title_statuses:{updated:1},message:'Restore complete: 0 added, 0 removed. Title statuses: 1 restored.'}}});
      }
      if (path.startsWith('/api/')) return route.fulfill({json:{}});
      return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-theme="${theme}"><head><meta name="viewport" content="width=device-width, initial-scale=1">${['themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','fonts/material-symbols-rounded','ui-shell','themes/flat','themes/original-coverage'].map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body><div id="page-snapshots"></div><script>window.CW={};</script><script src="/assets/js/snapshots.js"></script></body></html>`});
    });
    await page.goto('https://capture-status.test/');
    await page.locator('#ss-list tr[data-path="history.json"]').click();
    await page.locator('#ss-restore-statuses:not(.hidden)').waitFor();
    assert.match(await page.locator('[data-restore-mode="merge"]').innerText(), /restore statuses/);
    await page.locator('#ss-restore').click();
    await page.locator('#ss-restore').click();
    await page.waitForFunction(()=>document.querySelector('#ss-restore-out')?.textContent.includes('1 restored'));
    assert.equal(writes.length,1);
    await page.locator('#ss-capture-ack').click();
    await page.setViewportSize({width:390,height:844});
    await page.locator('#ss-restore-statuses').scrollIntoViewIfNeeded();
    const bounds=await page.locator('#ss-restore-statuses').boundingBox();
    assert.ok(bounds.x>=0 && bounds.x+bounds.width<=391);
    await page.locator('#ss-list tr[data-path="legacy.json"]').click();
    await page.waitForFunction(()=>document.querySelector('#ss-restore-statuses')?.classList.contains('hidden'));
    assert.match(await page.locator('[data-restore-mode="merge"]').innerText(), /Only add items/);
    assert.deepEqual(errors,[]);
  });
}
