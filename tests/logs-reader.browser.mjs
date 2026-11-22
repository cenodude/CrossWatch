/* tests/logs-reader.browser.mjs */
/* CrossWatch - Raw log exports, navigation, live updates and theme regressions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

async function open(t,theme='original',hash='') {
  const browser=await chromium.launch({headless:true});t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width:1440,height:1000}});page.setDefaultTimeout(8000);
  const errors=[];page.on('pageerror',error=>errors.push(error.message));t.after(()=>assert.deepEqual(errors,[]));
  const source=await readFile(new URL('../ui_frontend.py',import.meta.url),'utf8');
  const html=await page.evaluate(({source,theme})=>{
    const begin=source.indexOf('<!doctype html>');
    const doc=new DOMParser().parseFromString(source.slice(begin,source.indexOf('</html>',begin)+7),'text/html');
    doc.querySelectorAll('script,style').forEach(el=>el.remove());doc.querySelector('#cw-main-loading')?.remove();
    if(theme!=='original')doc.documentElement.dataset.cwTheme=theme;
    doc.documentElement.dataset.tab='logs';
    doc.documentElement.className=theme==='original'?'cw-theme-original':`cw-theme-${theme==='flat-light'?'light':'dark'}`;
    doc.querySelector(theme==='original'?'#cw-theme-flat-css':'#cw-theme-original-css')?.remove();
    const layout=doc.querySelector('#layout');layout.classList.add('full');
    for(const child of layout.children)child.classList.toggle('hidden',child.id!=='page-logs');
    return doc.documentElement.outerHTML;
  },{source,theme});
  const state={empty:false,active:false,count:620,requests:[],exports:[],deleted:[],delay:0};
  const session=id=>({id,run_id:id,channel:'sync',label:id==='a'?'PLEX → TRAKT <script>':'PLEX → SIMKL',started:1795344842,ended:state.active?null:1795344854,status:state.active?'running':'failed',pinned:false,errors:1,warnings:0,lines:state.count,truncated:0,pairs:'[]'});
  await page.route('http://logs.test/**',async route=>{
    const url=new URL(route.request().url()),path=url.pathname,params=Object.fromEntries(url.searchParams);
    if(path.startsWith('/assets/')){
      try{return await route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.js')?'text/javascript':path.endsWith('.woff2')?'font/woff2':'image/svg+xml'});}catch{return route.fulfill({status:404,body:''});}
    }
    if(!path.startsWith('/api/'))return route.fulfill({contentType:'text/html',body:html});
    if(path==='/api/logs/archive'){
      if(route.request().method()==='DELETE')return route.fulfill({json:{ok:true,deleted:0}});
      state.requests.push(params);
      return route.fulfill({json:{items:state.empty?[]:[session('a'),session('b')],pairs:[{id:'pair-a',label:'Plex → Trakt'}],latest_run_id:'a',channels:['sync','watcher','captures','backups','audits','system'],used_bytes:1024,max_bytes:104857600,retention_days:7}});
    }
    if(path.endsWith('/download')){state.exports.push(params);return route.fulfill({contentType:'text/plain',body:params.level?'[ERROR] original failure\n':'[INFO] original first line\n[ERROR] original failure\n'});}
    if(path.endsWith('/lines')){
      const sid=path.split('/')[4],all=Array.from({length:state.count},(_,i)=>({id:i+1,ts:1795344842+i,level:i===5?'ERROR':'INFO',provider:'PLEX',text:i===5?'[ERROR] original failure\nTraceback (most recent call last):\n  <script>never execute</script>':`[INFO] ${sid} record ${i+1}`}));
      const items=all.filter(row=>(!params.level||row.level===params.level)&&(!params.q||row.text.toLowerCase().includes(params.q.toLowerCase())));
      if(state.delay)await new Promise(resolve=>setTimeout(resolve,state.delay));
      return route.fulfill({json:{session:session(sid),items:items.slice(Number(params.offset||0),Number(params.offset||0)+500),total:items.length}});
    }
    if(path.includes('/context/'))return route.fulfill({json:{items:[{id:4,ts:1795344842,level:'INFO',provider:'PLEX',text:'[INFO] preceding request'}]}});
    if(route.request().method()==='DELETE'){state.deleted.push(path);return route.fulfill({json:{ok:true}});}
    return route.fulfill({json:{ok:true}});
  });
  await page.goto('http://logs.test/'+hash);
  await page.evaluate(async()=>{
    window.CW={OverviewProfile:{id:''}};
    window.cwCopyTextFrom=async text=>{window.copied=await text;return true;};
    const view=(await import('/assets/js/logs.js')).default;
    await view.mount(document.querySelector('#page-logs'));
  });
  return {page,state};
}

test('raw reader preserves text and exports the full session independently of filters',async t=>{
  const {page,state}=await open(t,'flat-dark','#logs?pairId=pair-a');
  assert.match(await page.locator('[data-output]').innerText(),/Traceback/);
  assert.match(await page.locator('[data-output]').innerText(),/<script>never execute<\/script>/);
  assert.equal(await page.locator('[data-output] script').count(),0);
  await page.locator('[data-level]').selectOption('ERROR');
  await page.waitForFunction(()=>document.querySelector('[data-count]').textContent.startsWith('1 matching'));
  await page.locator('[data-copy-full]').click();await page.waitForFunction(()=>window.copied?.includes('original first'));
  assert.equal(state.exports.at(-1).pair_id,undefined);assert.equal(state.exports.at(-1).level,undefined);assert.equal(state.exports.at(-1).raw,'true');
  await page.locator('[data-copy]').click();await page.waitForFunction(()=>window.copied==='[ERROR] original failure\n');
  assert.equal(state.exports.at(-1).level,'ERROR');assert.equal(state.exports.at(-1).pair_id,'pair-a');
  await page.locator('[data-context]').first().click();assert.match(await page.locator('.logs-context').innerText(),/preceding request/);
  await page.locator('[data-session="b"]').click();await page.waitForFunction(()=>document.querySelector('[data-title]').textContent==='PLEX → SIMKL');
  assert.equal(await page.locator('.logs-context').count(),0);
  await page.locator('[data-level]').selectOption('');await page.waitForFunction(()=>document.querySelector('[data-count]').textContent.startsWith('620'));
  await page.locator('[data-next]').click();await page.waitForFunction(()=>document.querySelector('[data-page]').textContent==='2 / 2');
  assert.match(await page.locator('[data-output]').innerText(),/record 501/);
});

test('live pause freezes output while recording continues and source changes discard stale requests',async t=>{
  const {page,state}=await open(t);
  state.active=true;state.count=10;
  await page.locator('[data-mode="live"]').click();await page.waitForFunction(()=>document.querySelector('[data-count]').textContent.startsWith('10 '));
  await page.locator('[data-follow]').click();state.count=11;
  await page.waitForFunction(()=>document.querySelector('[data-follow]').textContent.includes('1 new'));
  assert.doesNotMatch(await page.locator('[data-output]').innerText(),/record 11/);
  await page.locator('[data-follow]').click();await page.waitForFunction(()=>document.querySelector('[data-output]').textContent.includes('record 11'));
  state.delay=350;
  await page.locator('[data-query]').fill('absent');
  await page.locator('[data-channel="captures"]').click();
  await page.waitForFunction(()=>document.querySelector('[data-count]').textContent.startsWith('11 retained'));
  assert.equal(await page.locator('[data-query]').inputValue(),'');
  assert.match(await page.locator('[data-output]').innerText(),/record 11/);
  assert.equal(state.requests.at(-1).channel,'captures');
});

for(const theme of ['original','flat-dark','flat-light'])test(`logs layout and states: ${theme}`,async t=>{
  const {page,state}=await open(t,theme);
  for(const width of [1440,760,390,320]){
    await page.setViewportSize({width,height:1000});
    const overflow=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));
    assert.ok(overflow.scroll<=width+1,JSON.stringify(overflow));
    if(width<=760)assert.ok((await page.locator('.logs-sessions').boundingBox()).height>=100);
    if(process.env.CW_LOG_SCREENSHOTS&&width===390){await mkdir(process.env.CW_LOG_SCREENSHOTS,{recursive:true});await page.screenshot({path:`${process.env.CW_LOG_SCREENSHOTS}/logs-${theme}-mobile.png`,fullPage:true});}
  }
  await page.setViewportSize({width:1440,height:1000});
  await page.locator('.logs-options summary').click();assert.ok(await page.locator('[data-delete]').isVisible());
  page.once('dialog',dialog=>dialog.dismiss());await page.locator('[data-delete]').click();assert.equal(state.deleted.length,0);
  await page.locator('.logs-options summary').click();
  if(process.env.CW_LOG_SCREENSHOTS){await mkdir(process.env.CW_LOG_SCREENSHOTS,{recursive:true});await page.screenshot({path:`${process.env.CW_LOG_SCREENSHOTS}/logs-${theme}.png`,fullPage:true});}
  state.empty=true;await page.locator('[data-channel="backups"]').click();
  await page.waitForFunction(()=>document.querySelector('[data-output]').textContent.includes('No saved logs yet'));
  assert.equal(await page.locator('[data-session]').count(),0);
  assert.equal(await page.locator('[data-copy-full]').isDisabled(),true);
  if(process.env.CW_LOG_SCREENSHOTS)await page.screenshot({path:`${process.env.CW_LOG_SCREENSHOTS}/logs-${theme}-empty.png`,fullPage:true});
});
