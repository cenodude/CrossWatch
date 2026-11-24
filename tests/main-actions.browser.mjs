// tests/main-actions.browser.mjs
// CrossWatch - Sync Hub action alignment across available widths
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import './i18n-browser-setup.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';
import {englishFrontendSource} from './i18n-test-helper.mjs';

const source=englishFrontendSource();
for(const theme of ['original','flat-dark','flat-light']) test(`Sync Hub actions align in ${theme}`,async t=>{
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width:1600,height:900}});
  const html=await page.evaluate(({source,theme})=>{
    const start=source.indexOf('<!doctype html>');
    const doc=new DOMParser().parseFromString(source.slice(start,source.indexOf('</html>',start)+7),'text/html');
    doc.querySelectorAll('script').forEach(el=>el.remove());
    doc.querySelector('#cw-main-loading')?.remove();
    doc.documentElement.dataset.tab='main';
    doc.documentElement.className=theme==='original'?'cw-theme-original':'cw-theme-dark';
    if(theme!=='original')doc.documentElement.dataset.cwTheme=theme;
    doc.querySelector(theme==='original'?'#cw-theme-flat-css':'#cw-theme-original-css')?.remove();
    const card=doc.querySelector('#ops-card');
    const row=card.querySelector('.action-row');
    card.replaceChildren(row);
    row.querySelector('.cw-status-dock')?.remove();
    const layout=doc.querySelector('#layout');
    layout.replaceChildren(card);
    layout.className='single';
    doc.body.replaceChildren(layout);
    return '<!doctype html>'+doc.documentElement.outerHTML;
  },{source,theme});
  await page.route('http://main-actions.test/**',async route=>{
    const path=new URL(route.request().url()).pathname;
    if(path.startsWith('/assets/')){
      try{return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    return route.fulfill({body:html,contentType:'text/html'});
  });
  await page.goto('http://main-actions.test/');
  await page.addScriptTag({url:'/assets/js/schedulerbanner.js'});
  await page.evaluate(()=>document.fonts.ready);
  for(const width of [1600,1000,850,740,390,320]){
    await page.setViewportSize({width,height:900});
    const boxes=await page.locator('.action-buttons').evaluate(el=>({row:el.getBoundingClientRect().toJSON(),buttons:[...el.children].map(b=>({text:b.textContent.trim(),box:b.getBoundingClientRect().toJSON(),margin:getComputedStyle(b).marginInlineStart}))}));
    const [sync,details,analyzer,events,exporter]=boxes.buttons.map(button=>button.box);
    if(width>=1000){
      const row=await page.locator('.action-row').evaluate(el=>{
        const box=el.getBoundingClientRect(),style=getComputedStyle(el);
        return {left:box.left+parseFloat(style.borderLeftWidth)+parseFloat(style.paddingLeft),right:box.right-parseFloat(style.borderRightWidth)-parseFloat(style.paddingRight)};
      });
      assert.ok(Math.abs(sync.left-row.left)<1);
      assert.ok(Math.abs(exporter.right-row.right)<1, 'Tools reach the right edge of the toolbar');
      assert.ok(analyzer.left-details.right>20);
      assert.equal(analyzer.top,sync.top);
      assert.equal(events.top,sync.top);
    }else{
      assert.equal(parseFloat(boxes.buttons[2].margin),0);
    }
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    for(const button of boxes.buttons){
      assert.ok(button.box.left>=boxes.row.left-1);
      assert.ok(button.box.right<=boxes.row.right+1);
    }
    await mkdir('build/actions-screenshots',{recursive:true});
    await page.screenshot({path:`build/actions-screenshots/${theme}-${width}.png`});
  }
});
