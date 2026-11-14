/* tests/dashboard-widgets.browser.mjs */
/* CrossWatch - Overview widget presentation and Main isolation checks */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */

import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFile,mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

const profileHTML=execFileSync('python',['-c',"import ui_frontend; print(ui_frontend.get_profile_html({'is_admin':True,'username':'preview'}))"],{encoding:'utf8',maxBuffer:1024*1024});
const mainSource=await readFile(new URL('../ui_frontend.py',import.meta.url),'utf8');
const mainHTML=mainSource.slice(mainSource.indexOf('<!doctype html>'),mainSource.indexOf('</html>',mainSource.indexOf('<!doctype html>'))+7);

for(const [profile,width] of [[true,1440],[true,390],[false,1440],[false,390]]) test(`${profile?'Overview':'Main'} shared widgets at ${width}px`,async t=>{
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:1000},hasTouch:width<760});
  page.setDefaultTimeout(5000);
  const errors=[],queries=[];
  page.on('pageerror',error=>errors.push(error.message));
  const state={empty:false};
  const html=await page.evaluate(({source,profile})=>{
    const doc=new DOMParser().parseFromString(source,'text/html');
    doc.querySelectorAll('script').forEach(el=>el.remove());
    doc.documentElement.dataset.cwTheme='flat-dark';
    doc.documentElement.dataset.tab='main';
    doc.documentElement.className='cw-theme-dark';
    const widgets=doc.querySelector('#dashboard-widgets-card');
    const host=doc.createElement('main');
    host.id=profile?'profile-panel-overview':'layout';
    host.append(widgets);
    doc.body.replaceChildren(host);
    doc.body.className=profile?'cw-profile-page':'';
    if(profile){const hero=doc.createElement('div');hero.id='profile-hero';doc.body.prepend(hero);}
    const style=doc.createElement('style');
    style.textContent='body{padding:60px 24px}#profile-panel-overview{display:block}';
    doc.head.append(style);
    return '<!doctype html>'+doc.documentElement.outerHTML;
  },{source:profile?profileHTML:mainHTML,profile});
  await page.route('http://widgets.test/**',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) {
      try{return route.fulfill({body:await readFile(new URL('..'+url.pathname,import.meta.url)),contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.svg')?'image/svg+xml':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/art/')) return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180"><rect width="320" height="180" fill="#243c57"/><circle cx="220" cy="70" r="60" fill="#477c88"/><path d="M0 180L100 40L210 180" fill="#b89468"/></svg>'});
    if(url.pathname.startsWith('/api/')) {
      queries.push({path:url.pathname,params:Object.fromEntries(url.searchParams)});
      let json={ok:true};
      if(url.pathname==='/api/config') json={ui:{show_watchlist_preview:false,show_recent_progress_widget:true,show_recent_playlists_widget:true},pairs:[]};
      if(url.pathname==='/api/dashboard/widgets') {
        const items=state.empty?[]:Array.from({length:8},(_,i)=>({title:['The Institute','House of the Dragon','A Story of Derry'][i%3],tmdb:String(i+1),type:'episode',art_type:'tv',episode_label:'S02E01',year:2026,rating:8,progress:65,sort_epoch:1794568000,method:'watcher',source:{provider:'PLEX',instance:'default'},targets:[{provider:'TRAKT',instance:'default'}],sources:[{provider:'CROSSWATCH',instance:'default'}]}));
        json={ok:true,...Object.fromEntries(['recent_history','latest_ratings','recent_scrobble','recent_progress','recent_playlists'].map(key=>[key,{items,total:items.length}]))};
      }
      return route.fulfill({json});
    }
    return route.fulfill({contentType:'text/html',body:html});
  });
  await page.goto('http://widgets.test/');
  await page.evaluate(()=>{
    window.CW={AuthState:{read:()=>({isAdmin:true})},OverviewProfile:{id:''},ItemActions:{canWrite:()=>true,openMenu:options=>{window.__action={feature:options.feature,title:options.item.title};}},ProfileMediaModal:{canOpen:()=>true,open:item=>{window.__opened=item;}}};
  });
  await page.addScriptTag({url:'/assets/helpers/provider-meta.js'});
  await page.addScriptTag({url:'/assets/js/dashboard-widgets.js'});
  if (!profile && width < 760) {
    await page.evaluate(()=>CW.DashboardWidgets.refresh({force:true}));
    assert.equal(await page.locator('#dashboard-widgets-card').isVisible(),false);
    assert.equal(queries.filter(q=>q.path==='/api/dashboard/widgets').length,0);
    await page.setViewportSize({width:1440,height:1000});
    await page.locator('#recent-history-list [data-cw-widget-item]').first().waitFor();
    assert.equal(await page.locator('#dashboard-widgets-card').isVisible(),true);
    await page.setViewportSize({width,height:1000});
    assert.equal(await page.locator('#dashboard-widgets-card').isVisible(),false);
    assert.deepEqual(errors,[]);
    return;
  }
  const first=page.locator('#recent-history-list [data-cw-widget-item]').first();
  await first.waitFor();
  if(profile) {
    for(const id of ['recent-history-list','latest-ratings-grid','recent-scrobble-list','recent-progress-list','recent-playlists-list']) {
      assert.equal(await page.locator(`#${id} .cw-hub-widget-row`).count(),3);
      assert.equal(await page.locator(`#${id} .cw-hub-widget-more`).count(),1);
    }
    assert.equal(await page.locator('#recent-history-widget').evaluate(el=>getComputedStyle(el).backgroundColor),'rgba(0, 0, 0, 0)');
    assert.ok(await page.locator('#recent-scrobble-list [data-ia-ref]').count()>0);
    const providerArt=await page.locator('#recent-scrobble-list [data-ia-ref] img').first().boundingBox();
    assert.ok(providerArt.width>0 && providerArt.height>0);
    const art=await first.locator('.cw-hub-widget-art').boundingBox();
    assert.equal(art.width,88);
    assert.equal(art.height,56);
    assert.equal(await first.locator('.cw-hub-widget-title strong').evaluate(el=>getComputedStyle(el).fontWeight),'500');
    assert.equal(await page.locator('#latest-ratings-grid .cw-rating-grid-score').count(),0);
    assert.equal(await page.locator('#latest-ratings-grid .cw-hub-widget-rating').first().innerText(),'8/10');
    assert.equal(await first.innerText().then(text=>(text.match(/S02E01/g)||[]).length),1);
    assert.equal(await page.locator('#recent-history-widget .cw-dash-layout-controls').isVisible(),false);
    await page.locator('#recent-history-widget').hover();
    assert.equal(await page.locator('#recent-history-widget .cw-dash-layout-controls').isVisible(),false);
    await first.locator('[data-ia-ref]').first().click();
    assert.deepEqual(await page.evaluate(()=>window.__action),{feature:'history',title:'The Institute'});
    assert.equal(await page.evaluate(()=>window.__opened),undefined);
    await first.locator('.cw-hub-widget-title').click();
    assert.equal(await page.evaluate(()=>window.__opened.title),'The Institute');
    assert.ok(queries.filter(q=>q.path==='/api/dashboard/widgets').every(q=>Number(q.params.history_limit)<=3));
    await page.locator('[data-cw-dashboard-layout="customize"]').click();
    assert.equal(await page.locator('#recent-history-widget .cw-dash-layout-controls').isVisible(),true);
    await page.locator('[data-cw-dashboard-layout="customize"]').click();
  } else {
    assert.equal(await page.locator('.cw-hub-widget-row').count(),0);
    assert.equal(await page.locator('#recent-history-list .cw-history-widget-item').count(),6);
    assert.equal(await page.locator('#latest-ratings-grid .cw-rating-grid-score').count(),8);
    assert.ok(queries.some(q=>Number(q.params.history_limit)===6));
  }
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  if(process.env.CW_WIDGET_SCREENSHOTS){
    await mkdir(process.env.CW_WIDGET_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_WIDGET_SCREENSHOTS}/${profile?'overview':'main'}-${width}.png`,fullPage:true});
  }
  state.empty=true;
  await page.evaluate(()=>window.CW.DashboardWidgets.refresh({force:true}));
  await page.locator('#recent-history-list .cw-dash-empty').waitFor();
  assert.deepEqual(errors,[]);
});
