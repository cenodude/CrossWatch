/* tests/main-mobile.browser.mjs */
/* CrossWatch - Mobile sync statistics and recent activity layout checks */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

const source=await readFile(new URL('../ui_frontend.py',import.meta.url),'utf8');
const start=source.indexOf('<!doctype html>');
const html=source.slice(start,source.indexOf('</html>',start)+7);

for (const width of [390,320,1440]) test(`Main sync activity fits at ${width}px`,async t=>{
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:1000},hasTouch:width<760});
  page.setDefaultTimeout(6000);
  const errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  const body=await page.evaluate(source=>{
    const doc=new DOMParser().parseFromString(source,'text/html');
    doc.querySelectorAll('script').forEach(el=>el.remove());
    doc.documentElement.dataset.cwTheme='flat-dark';
    doc.documentElement.dataset.tab='main';
    doc.documentElement.className='cw-theme-dark';
    const stats=doc.querySelector('#stats-card');
    stats.classList.remove('collapsed');
    const main=doc.createElement('main');
    main.id='layout';
    main.append(stats);
    doc.body.replaceChildren(main);
    const style=doc.createElement('style');
    style.textContent='body{padding:12px}#layout{display:block;padding:0}#stats-card{max-width:650px;margin:auto}';
    doc.head.append(style);
    return '<!doctype html>'+doc.documentElement.outerHTML;
  },html);
  let empty=false;
  const artRequests=[];
  const providers=['crosswatch','PLEX','MDBLIST','SIMKL','TRAKT'];
  const recent=Array.from({length:3},(_,i)=>({title:['Marvel’s VisionQuest','A very long show title that still needs to fit on a small phone','South Park'][i],ids:i===2?{}:{tmdb:100+i},media_type:'episode',season:1,episode:i+1,source:'PLEX',source_instance:'default',targets:[{target:'CROSSWATCH'},{target:'TRAKT'},{target:'SIMKL'}],method:i===1?'webhook':'watcher',status:i===1?'failed':'ok',progress:91,captured_at:Date.now()/1000-3600*(i+1)}));
  await page.route('http://main-mobile.test/**',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/art/')) {
      artRequests.push(url.pathname);
      if(url.pathname.endsWith('/101')) return route.fulfill({status:404,body:''});
      return route.fulfill({body:await readFile(new URL('../assets/img/placeholder_poster.svg',import.meta.url)),contentType:'image/svg+xml'});
    }
    if(url.pathname.startsWith('/assets/')) {
      try{return route.fulfill({body:await readFile(new URL('..'+url.pathname,import.meta.url)),contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.svg')?'image/svg+xml':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/api/')) {
      let json={ok:true};
      if(url.pathname==='/api/config') json={ui:{show_recent_activity:true},pairs:[]};
      if(url.pathname==='/api/activity/recent'||url.pathname==='/api/activity/history') json={ok:true,items:empty?[]:recent,total:empty?0:recent.length};
      if(url.pathname==='/api/insights') json={features:Object.fromEntries(['watchlist','history','ratings','progress','playlists','collection'].map(feature=>[feature,{now:5,week:2,month:3,added:2,removed:0,providers:Object.fromEntries(providers.map((p,i)=>[p,i===1?10000:5])),providers_active:Object.fromEntries(providers.map(p=>[p,true])),providers_mse:Object.fromEntries(providers.map(p=>[p,{movies:5}]))}])),history:empty?[]:[0,1,2].map(i=>({finished_at:new Date(Date.now()-i*3600000).toISOString(),duration_sec:7.9,exit_code:i===2?1:0,result:i===2?'FAILED':'OK',features:{watchlist:{added:i===1?16:0,removed:0}}}))};
      return route.fulfill({json});
    }
    return route.fulfill({contentType:'text/html',body});
  });
  await page.goto('http://main-mobile.test/');
  await page.evaluate(providers=>{window.CW={AuthState:{read:()=>({isAdmin:true})}};window.getConfiguredProviders=()=>providers;window._cfgCache={ui:{show_recent_activity:true}};},providers);
  await page.addScriptTag({url:'/assets/helpers/provider-meta.js'});
  await page.addScriptTag({url:'/assets/helpers/media-meta.js'});
  await page.addScriptTag({url:'/assets/js/insights.js'});
  await page.addScriptTag({url:'/assets/js/activity.js'});
  await page.evaluate(async()=>{await refreshInsights();await refreshRecentActivity();});
  await page.locator('#stat-providers .tile').first().waitFor();
  assert.equal(await page.locator('#recent-activity .activity-item').count(),3);
  assert.equal(await page.locator('#sync-history .history-item').count(),3);
  assert.equal(await page.locator('#recent-activity .activity-badge.err').textContent(),'failed');
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  if(width<760){
    const tiles=await page.locator('#stat-providers .tile').evaluateAll(nodes=>nodes.map(el=>({x:el.offsetLeft,y:el.offsetTop,h:el.offsetHeight})));
    assert.equal(tiles[0].y,tiles[1].y);
    assert.ok(tiles[2].y>tiles[0].y);
    assert.ok(tiles.every(tile=>tile.h<=90));
    assert.equal(await page.locator('#stat-providers .tile-k').first().isVisible(),true);
    assert.equal(await page.locator('#stat-providers .tile').evaluateAll(nodes=>nodes.every(tile=>{
      const bounds=tile.getBoundingClientRect(),label=tile.querySelector('.tile-k').getBoundingClientRect();
      return label.left>=bounds.left && label.right<=bounds.right;
    })),true);
    assert.equal(await page.locator('#sync-history .history-mobile-result').first().innerText(),'No changes');
    assert.equal(await page.locator('#sync-history .history-item.err .history-mobile-title').innerText(),'Watchlist needs attention');
    assert.equal(await page.locator('#sync-history .history-mobile-result').nth(1).innerText(),'16 added');
    assert.equal(await page.locator('#recent-activity .activity-mobile-method.is-error').innerText(),'Webhook \u00b7 Failed to scrobble');
    await page.locator('#recent-activity').scrollIntoViewIfNeeded();
    await page.waitForFunction(()=>document.querySelector('#recent-activity .activity-art img')?.complete);
    await page.waitForFunction(()=>!document.querySelectorAll('#recent-activity .activity-art picture')[1]?.querySelector('source'));
    assert.ok(artRequests.length>=2);
    assert.equal(await page.locator('#recent-activity .activity-art>.material-symbols-rounded').count(),1);
    assert.equal(await page.locator('#recent-activity .activity-mobile-route').first().innerText(),'Plex \u2192 CrossWatch \u00b7 Trakt \u00b7 SIMKL');
    assert.equal(await page.locator('#recent-activity .activity-metric.media').first().isVisible(),false);
    await page.locator('#insights-mobile-feature').selectOption('history');
    await page.waitForFunction(()=>localStorage.getItem('insights.feature')==='history');
    await page.locator('#insights-mobile-feature').selectOption('watchlist');
    await page.waitForFunction(()=>localStorage.getItem('insights.feature')==='watchlist');
    await page.evaluate(()=>{window.CW.ProfileMediaModal={canOpen:item=>!!item.ids?.tmdb,open:item=>{window.openedScrobble=item;}};});
    await page.locator('#recent-activity [data-recent-media="0"]').click();
    assert.equal(await page.evaluate(()=>window.openedScrobble.ids.tmdb),100);
    await page.locator('#recent-activity [data-recent-media="1"]').focus();
    await page.keyboard.press('Enter');
    assert.equal(await page.evaluate(()=>window.openedScrobble.ids.tmdb),101);
  } else {
    assert.equal(await page.locator('#stat-providers .tile-k').first().isVisible(),false);
    assert.equal(await page.locator('#sync-history .history-mobile').first().isVisible(),false);
    assert.equal(artRequests.length,0);
    assert.equal(await page.locator('#recent-activity .activity-metric.media').first().isVisible(),true);
  }
  await mkdir('.tmp/main-mobile',{recursive:true});
  await page.screenshot({path:`.tmp/main-mobile/main-${width}.png`,fullPage:true});
  await page.locator('#activity-view-all').click();
  await page.locator('.activity-dialog').waitFor();
  assert.equal(await page.locator('.activity-dialog .activity-mobile').count(),0);
  await page.keyboard.press('Escape');
  empty=true;
  await page.evaluate(async()=>{await refreshInsights();await refreshRecentActivity();});
  await page.locator('#recent-activity .activity-empty').waitFor({state:'attached'});
  assert.equal(await page.locator('#sync-history .history-item').count(),1);
  assert.equal(await page.locator('#recent-activity-block').isVisible(),width>760);
  assert.equal(await page.locator('.cw-recent-feed:has(#sync-history)').isVisible(),width>760);
  if(width<760) {
    await page.setViewportSize({width:1440,height:1000});
    assert.equal(await page.locator('#recent-activity-block').isVisible(),true);
    assert.equal(await page.locator('.cw-recent-feed:has(#sync-history)').isVisible(),true);
    await page.setViewportSize({width,height:1000});
  }
  empty=false;
  await page.evaluate(async()=>{await refreshInsights();await refreshRecentActivity();});
  assert.equal(await page.locator('#recent-activity-block').isVisible(),true);
  assert.equal(await page.locator('.cw-recent-feed:has(#sync-history)').isVisible(),true);
  assert.deepEqual(errors,[]);
});
