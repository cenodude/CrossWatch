/* tests/selection-controls.browser.mjs */
/* CrossWatch - Selection appearance, keyboard access and touch layouts */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

async function fixture(t, theme, width) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:900}});
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  await page.addInitScript(()=>localStorage.setItem('cw.tracker.view.v3',JSON.stringify({view:'history',viewSelectedAt:Date.now(),layouts:{history:'grid'}})));
  await page.route('https://selection.test/**',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) {
      try {return route.fulfill({contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript',body:await readFile(new URL('..'+url.pathname,import.meta.url))});}
      catch {return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/api/')) {
      const items=[1,2,3].map(id=>({key:`tmdb:${id}`,type:'movie',title:`Movie ${id}`,year:2026,ids:{tmdb:String(id)},date:'2026-11-15T12:00:00Z'}));
      const data={
        '/api/tracker/summary':{instances:[{id:'default',label:'Family tracker'}],counts:{history:3},tmdb_key:true},
        '/api/tracker/items':{items,total:3},
        '/api/import/options':{sources:[{id:'auto',label:'Auto detect'}],targets:[{id:'default',label:'Family tracker',connected:true}],limits:{upload_bytes:26214400}},
        '/api/export/options':{providers:['CROSSWATCH']}
      }[url.pathname]||{ok:true};
      return route.fulfill({json:data});
    }
    const styles=['themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','css/profile-page','ui-shell','themes/flat','themes/original-coverage','fonts/material-symbols-rounded'];
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${styles.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body><div id="layout"><section id="page-tracker" class="cw-themed-page"></section><section id="page-import_export" class="cw-themed-page" hidden></section></div><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/js/tracker.js"></script><script type="module">import importer from '/assets/js/import-export/index.js';window.importer=importer;</script></body></html>`});
  });
  await page.goto('https://selection.test/');
  await page.waitForFunction(()=>window.importer&&window.TrackerPage);
  await page.evaluate(theme=>{
    if(theme!=='original')document.documentElement.dataset.cwTheme=theme;
    return TrackerPage.mount(document.querySelector('#page-tracker'));
  },theme);
  return page;
}

for(const theme of ['original','flat-dark','flat-light'])for(const width of [1100,390])test(`selection controls ${theme} ${width}`,async t=>{
  const page=await fixture(t,theme,width);
  const folder=new URL('../build/selection-controls/',import.meta.url);
  await mkdir(folder,{recursive:true});
  const screenshot=async name=>page.screenshot({path:new URL(`${name}-${theme}-${width}.png`,folder).pathname.replace(/^\/(?=[A-Za-z]:)/,'')});
  const first=page.locator('.trk-grid-pick').first();
  await first.waitFor();
  const normal=await first.evaluate(node=>getComputedStyle(node.closest('.trk-grid-card')).backgroundColor);
  const target=first.locator('..');
  if(width<760)assert.ok((await target.boundingBox()).width>=44);
  await target.click({position:{x:2,y:2}});
  assert.equal(await first.isChecked(),true);
  assert.equal(await first.evaluate(node=>getComputedStyle(node.closest('.trk-grid-card')).backgroundColor),normal);
  const placement=await first.evaluate(node=>{const box=node.getBoundingClientRect(),card=node.closest('.trk-grid-card').getBoundingClientRect();return box.x>=card.x&&box.right<=card.right&&box.y>=card.y&&box.bottom<=card.bottom;});
  assert.equal(placement,true);
  await page.keyboard.press('Tab');
  await first.focus();
  assert.notEqual(await first.evaluate(node=>getComputedStyle(node).outlineStyle),'none');
  await page.keyboard.press('Space');
  assert.equal(await first.isChecked(),false);
  await first.focus();
  await page.keyboard.press('Space');
  await page.mouse.move(0,0);
  await screenshot('tracker');
  await page.evaluate(async()=>{
    document.querySelector('#page-tracker').hidden=true;
    document.querySelector('#page-import_export').hidden=false;
    await importer.mount(document.querySelector('#page-import_export'));
  });
  if(width<760)await page.locator('#ie-options').click();
  const history=page.getByLabel('History',{exact:true});
  await history.uncheck();
  assert.equal(await history.isChecked(),false);
  await history.focus();
  await page.keyboard.press('Space');
  assert.equal(await history.isChecked(),true);
  const style=await history.evaluate(node=>({background:getComputedStyle(node).backgroundColor,tick:getComputedStyle(node,'::after').borderBottomColor,outline:getComputedStyle(node).outlineStyle,box:getComputedStyle(node.parentElement).borderTopColor}));
  assert.notEqual(style.background,style.tick);
  assert.notEqual(style.outline,'none');
  assert.equal(style.box,'rgba(0, 0, 0, 0)');
  await history.evaluate(node=>node.disabled=true);
  assert.equal(await history.isDisabled(),true);
  await history.evaluate(node=>node.disabled=false);
  await page.mouse.move(0,0);
  assert.equal(await page.locator('.ie-inclusions').evaluate(node=>node.scrollWidth<=node.clientWidth),true);
  await screenshot('import');
});
