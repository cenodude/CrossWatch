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
  await page.evaluate(()=>{
    document.querySelector('#layout').style.setProperty('display','none','important');
    document.body.classList.add('cw-profile-page');
    const controls=document.createElement('section');
    controls.className='cw-profile-panel';
    controls.style.cssText='margin:24px;display:grid;gap:24px;--cch-u:1px';
    controls.innerHTML=`<h2>Checkbox states</h2>
      <div style="display:flex;gap:18px;flex-wrap:wrap"><label><input id="sample-on" type="checkbox" checked> Checked</label><label><input id="sample-off" type="checkbox"> Unchecked</label><label><input id="sample-mixed" type="checkbox"> Partial</label><label><input type="checkbox" checked disabled> Disabled</label></div>
      <section id="page-snapshots"><h3>Snapshot table</h3><table class="ss-table"><tbody><tr><td class="ss-col-check"><input id="sample-snapshot" class="ss-chk" type="checkbox" checked aria-label="Select snapshot"></td><td>Selected snapshot</td></tr></tbody></table></section>
      <section class="cw-page-maint-content"><h3>Provider cleanup</h3><button type="button" class="cc-feature selected" aria-pressed="true"><span class="cc-feature-mark" id="sample-cleanup"></span><span>Watchlist</span></button></section>
      <section><h3>Media card</h3><div class="cw-tl-card is-selected" style="position:relative;width:220px;height:100px"><label id="sample-media" class="cw-tl-select" style="opacity:1"><input type="checkbox" checked aria-label="Select media"><span class="material-symbols-rounded">check</span></label></div></section>`;
    document.body.appendChild(controls);
    document.querySelector('#sample-mixed').indeterminate=true;
  });
  const native=await page.locator('#sample-on').evaluate(node=>({background:getComputedStyle(node).backgroundColor,border:getComputedStyle(node).borderTopColor,tick:getComputedStyle(node,'::after').borderBottomColor}));
  assert.equal(native.background,theme==='flat-light'?'rgb(233, 228, 240)':'rgb(37, 32, 57)');
  assert.equal(native.tick,theme==='flat-light'?'rgb(85, 67, 104)':'rgb(196, 189, 212)');
  for(const selector of ['#sample-snapshot','#sample-cleanup']) {
    assert.deepEqual(await page.locator(selector).evaluate(node=>({background:getComputedStyle(node).backgroundColor,border:getComputedStyle(node).borderTopColor,tick:getComputedStyle(node,'::after').borderBottomColor})),native,selector);
  }
  assert.equal(await page.locator('#sample-mixed').evaluate(node=>getComputedStyle(node).backgroundColor),native.background);
  assert.equal(await page.locator('#sample-mixed').evaluate(node=>getComputedStyle(node,'::after').backgroundColor),native.tick);
  assert.notEqual(await page.locator('#sample-off').evaluate(node=>getComputedStyle(node).backgroundColor),native.background);
  assert.equal(await page.locator('#sample-media').evaluate((node,mobile)=>getComputedStyle(node,mobile?'::before':null).backgroundColor,width<760),'rgb(37, 32, 57)');
  assert.equal(await page.locator('#sample-media').evaluate(node=>getComputedStyle(node).color),'rgb(196, 189, 212)');
  await screenshot('checkboxes');
});
