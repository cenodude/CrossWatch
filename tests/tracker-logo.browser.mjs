/* tests/tracker-logo.browser.mjs */
/* CrossWatch - Header title logos, text fallback and responsive sizing */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

const items = [
  {key:"movie:1",title:"Arrival",type:"movie",year:2016,art:{tmdb:1,kind:"movie"}},
  {key:"movie:2",title:"A movie without title artwork",type:"movie",year:2026,art:{tmdb:2,kind:"movie"}},
  {key:"episode:3",title:"The Red Sowing",series_title:"House of the Dragon",type:"episode",season:2,episode:7,art:{tmdb:3,kind:"tv"}},
].map(item=>({...item,date:"2026-11-08T12:00:00Z",status:item.art.tmdb===2?null:item.type==='episode'?'watching':'completed',user_rating:7.5}));
const logo = '<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="150"><text x="5" y="110" fill="white" font-family="serif" font-size="100">ARRIVAL</text></svg>';
const metadata={
  1:{title:"Arrival",year:2016,runtime_minutes:116,genres:["Science Fiction","Drama"],detail:{}},
  3:{title:"House of the Dragon",year:2022,genres:["Drama","Fantasy"],detail:{number_of_seasons:2,number_of_episodes:16,status:"Returning Series"}},
};

async function fixture(t,theme,width) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:900},isMobile:width<800,hasTouch:width<800,reducedMotion:'reduce'});
  page.setDefaultTimeout(6000);
  await page.addInitScript(() => { if (!localStorage.getItem("cw.tracker.view.v3")) localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history",viewSelectedAt:Date.now()})); });
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  let release;
  const heldLogo=new Promise(resolve=>{release=resolve;});
  let requested;
  const logoRequested=new Promise(resolve=>{requested=resolve;});
  await page.route('https://tracker-logo.test/**',async route=>{
    const url=new URL(route.request().url()),path=url.pathname;
    if(path.startsWith('/assets/')) {
      try{return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(path.startsWith('/art/')) {
      if(url.searchParams.get('kind')==='logo') {
        if(path.endsWith('/2')) return route.fulfill({status:404,body:'Logo not available'});
        if(path.endsWith('/3')) {requested();await heldLogo;}
        return route.fulfill({contentType:'image/svg+xml',body:path.endsWith('/3')?'<svg xmlns="http://www.w3.org/2000/svg" width="260" height="400"><text x="5" y="100" fill="white" font-family="serif" font-size="70"><tspan x="5">HOUSE</tspan><tspan x="5" dy="100">OF THE</tspan><tspan x="5" dy="100">DRAGON</tspan></text></svg>':logo});
      }
      return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="720"><rect width="1280" height="720" fill="#33445c"/></svg>'});
    }
    if(path.startsWith('/api/')) {
      let json={ok:true};
      if(path==='/api/tracker/summary') json={provider_instance:'default',instances:[{id:'default',label:'Default'}],counts:{history:3},health:{},notes:{total:0,statuses:{},tags:[]}};
      if(path==='/api/tracker/stats') json={week:3,month:3,year:3,total:3,timed:3,minutes:300,streak:1};
      if(path==='/api/tracker/items') json={items,total:items.length};
      if(path==='/api/tracker/title') {
        const item=items.find(item=>item.key===url.searchParams.get('key'))||items[0];
        json={title:{...item,title:item.series_title||item.title},show:item.type==='episode',plays:[item],ratings:[],watchlist:[],collection:[],progress:[],attention:[],note:item.art.tmdb===2?null:{status:item.type==='episode'?'watching':'completed'},seasons:[]};
      }
      return route.fulfill({json});
    }
    const attrs=theme==='original'?'class="cw-theme-original"':`data-cw-theme="${theme}" class="cw-theme-${theme==='flat-light'?'light':'dark'}"`;
    const css=['fonts/material-symbols-rounded','themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','ui-shell',theme==='original'?'themes/original-coverage':'themes/flat'];
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html ${attrs}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body><main id="layout"><section id="page-tracker" class="cw-themed-page"></section></main><script>const metadata=${JSON.stringify(metadata)};window.CW={AuthState:{read:()=>({isAdmin:true})},Meta:{peek:item=>metadata[item.ids?.tmdb]||null,get:async item=>metadata[item.ids?.tmdb]||null},ProfileDateTime:{format:date=>date.toISOString().slice(0,10)}};</script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/js/profile-media-modal.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
  });
  await page.goto('https://tracker-logo.test/');
  await page.locator('.trk-play').first().waitFor();
  const select=async index=>{
    await page.locator('.trk-play').nth(index).click();
    if(width<=1320) {
      await page.locator('.trk-title-sheet [data-trk-sheet="close"]').waitFor();
      await page.locator('.trk-title-sheet [data-trk-sheet="close"]').click();
    }
  };
  return {page,select,release,logoRequested};
}

test('Tracker media card controls and watch history', async t => {
  const {page, release} = await fixture(t, 'flat-dark', 1440);
  release();
  await page.evaluate(async () => {
    window.mediaActions = [];
    CW.Trailer = {has: () => true};
    CW.ProfileDateTime.format = (date, options) => new Intl.DateTimeFormat('en-GB', {...options, timeZone:'UTC', hourCycle:'h23'}).format(date);
    window.mediaData = {title:{art:{tmdb:3}}, note_key:'show:3', note:{status:'dropped'}, plays:Array.from({length:7}, (_, i) => ({key:`play:${i}`, season:2, episode:i+1, date:`2026-11-0${i+1}T20:29:00Z`}))};
    await CW.ProfileMediaModal.open({type:'show', ids:{tmdb:3}}, {tracker:{label:'Default', data:mediaData, onAction:async(action, key) => {
      mediaActions.push([action,key]);
      if (action === 'status') {mediaData.note.status = key; CW.ProfileMediaModal.updateTracker(mediaData);}
    }}});
  });
  const modal = page.locator('#cw-media-modal');
  assert.equal(await modal.locator('[data-mm-trailer]').getAttribute('class'), 'cw-mm-btn');
  assert.equal(await modal.locator('.cw-mm-chips [data-status]').count(), 0);
  assert.equal(await modal.locator('.cw-mm-trk-header [data-mm-status-picker]').count(), 1);
  assert.equal(await modal.locator('.cw-mm-trk>.cw-mm-trk-notes:last-child [data-mm-trk="notes"]').count(), 1);
  assert.equal(await modal.locator('.cw-mm-trk-actions [data-mm-trk="notes"]').count(), 0);
  assert.equal(await modal.locator('[data-mm-providers]').evaluate(el=>el.open), false);
  await modal.locator('[data-mm-providers]').evaluate(el=>{window.providersToggled=new Promise(resolve=>el.addEventListener('toggle',resolve,{once:true}));});
  await modal.locator('[data-mm-providers] summary').click();
  await page.evaluate(()=>window.providersToggled);
  await page.evaluate(()=>CW.ProfileMediaModal.updateTracker(mediaData));
  assert.equal(await modal.locator('[data-mm-providers]').evaluate(el=>el.open), true);
  await modal.locator('[data-mm-providers] summary').click();
  assert.equal(await modal.locator('.cw-mm-trk-play').count(), 5);
  assert.equal(await modal.locator('.cw-mm-trk-play b').first().textContent(), 'S2 E7');
  assert.match(await modal.locator('.cw-mm-trk-play').first().textContent(), /7 Nov 2026.*20:29/);
  assert.equal(await modal.locator('[data-mm-trk-delete]').count(), 0);
  await modal.locator('[data-mm-history-more]').click();
  assert.equal(await modal.locator('.cw-mm-trk-play').count(), 7);
  await modal.locator('[data-mm-history-more]').click();
  await modal.locator('[data-mm-trk-edit]').first().click();
  assert.deepEqual(await page.evaluate(()=>mediaActions[0]), ['edit','play:6']);
  await modal.locator('[data-mm-status-picker] summary').click();
  assert.equal(await modal.locator('[data-mm-trk-status]:visible').count(), 6);
  await page.keyboard.press('Escape');
  assert.equal(await modal.isVisible(), true);
  assert.equal(await modal.locator('[data-mm-status-picker]').evaluate(el=>el.open), false);
  await modal.locator('[data-mm-status-picker] summary').click();
  await modal.locator('[data-mm-trk-status="watching"]').click();
  await page.waitForFunction(()=>document.querySelector('.cw-mm-trk .trk-status-btn')?.dataset.status === 'watching');
  assert.deepEqual(await page.evaluate(()=>mediaActions[1]), ['status','watching']);
  await modal.locator('[data-mm-status-picker] summary').click();
  await modal.locator('[data-mm-trk-status=""]').click();
  await page.waitForFunction(()=>document.querySelector('.cw-mm-trk .trk-status-btn')?.dataset.status === '');
  assert.equal(await modal.locator('.cw-mm-body').evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
  await mkdir('.tmp/media-card', {recursive:true});
  await modal.screenshot({path:'.tmp/media-card/updated.png'});
  await page.evaluate(()=>{mediaData.note_key=null;CW.ProfileMediaModal.updateTracker(mediaData);});
  await modal.locator('[data-mm-status-picker] summary').click();
  assert.equal(await modal.locator('[data-mm-trk-status]:disabled').count(),6);
  await page.evaluate(()=>CW.ProfileMediaModal.open({type:'movie',ids:{tmdb:1}}));
  assert.equal(await modal.locator('[data-mm-providers]').count(),0);
  assert.equal(await modal.locator('section.cw-mm-lives').count(),1);
});

for(const theme of ['original','flat-dark','flat-light']) for(const width of [320,390,1440]) {
  test(`${theme}: header logos and fallback at ${width}px`,async t=>{
    const {page,select,release,logoRequested}=await fixture(t,theme,width);
    const heading=page.locator('.trk-hero-heading');
    assert.equal(await page.locator('.cw-page-hero-title').count(),0);
    assert.equal(await page.locator('.cw-page-hero-kicker').textContent(),'CW TRACKER');
    if(process.env.CW_LOGO_SCREENSHOTS) {
      await mkdir(process.env.CW_LOGO_SCREENSHOTS,{recursive:true});
      await page.locator('.cw-page-hero').screenshot({path:`${process.env.CW_LOGO_SCREENSHOTS}/${theme}-${width}-empty.png`});
    }
    await select(0);
    await heading.locator('img').waitFor();
    assert.equal(await heading.locator('strong').isVisible(),false);
    assert.equal(await heading.locator('img').getAttribute('alt'),'Arrival');
    assert.equal(await heading.locator('img').evaluate(image=>getComputedStyle(image).backgroundColor),'rgba(0, 0, 0, 0)');
    const bounds=await heading.locator('img').boundingBox();
    assert.ok(bounds.width<=(width<761?220:400)&&bounds.x>=0&&bounds.x+bounds.width<=width);
    assert.equal(bounds.height,width<761?64:80);
    const yearBounds=await page.locator('.trk-hero-meta').boundingBox();
    assert.ok(yearBounds.y>=bounds.y+bounds.height);
    assert.ok(Math.abs(yearBounds.x-bounds.x)<1);
    assert.match(await page.locator('.trk-hero-meta').textContent(),/2016.*Movie.*116 min.*Science Fiction/);
    assert.equal(await page.locator('.trk-hero-details .trk-status').getAttribute('data-status'),'completed');
    assert.match(await page.locator('.trk-play').first().textContent(),/Arrival/);
    if(process.env.CW_LOGO_SCREENSHOTS) {
      await mkdir(process.env.CW_LOGO_SCREENSHOTS,{recursive:true});
      await page.locator('.cw-page-hero').screenshot({path:`${process.env.CW_LOGO_SCREENSHOTS}/${theme}-${width}.png`});
    }
    await select(1);
    await heading.locator('strong').waitFor();
    assert.equal(await heading.locator('strong').textContent(),items[1].title);
    await page.waitForFunction(()=>{const image=document.querySelector('.trk-hero-logo');return image?.complete&&!image.naturalWidth;});
    assert.equal(await heading.locator('img').isVisible(),false);
    assert.equal(await page.locator('.trk-hero-details .trk-status').count(),0);
    await select(2);
    await logoRequested;
    assert.equal(await heading.locator('strong').textContent(),'House of the Dragon');
    assert.match(await page.locator('.trk-hero-meta').textContent(),/S02.*E07/);
    await select(0);
    release();
    await heading.locator('img').waitFor();
    assert.equal(await heading.locator('img').getAttribute('alt'),'Arrival');
    await select(2);
    await heading.locator('img').waitFor();
    assert.equal(await heading.locator('img').getAttribute('alt'),'House of the Dragon');
    assert.match(await page.locator('.trk-hero-selection small').textContent(),/The Red Sowing/);
    const episodeLogo=await heading.locator('img').boundingBox();
    const episodeMeta=await page.locator('.trk-hero-meta').boundingBox();
    const episodeTitle=await page.locator('.trk-hero-selection small').boundingBox();
    assert.ok(episodeMeta.y>=episodeLogo.y+episodeLogo.height);
    assert.ok(Math.abs(episodeMeta.x-episodeLogo.x)<1);
    assert.ok(episodeTitle.y>=episodeMeta.y-2);
    assert.match(await page.locator('.trk-hero-meta').textContent(),/2022.*TV Series.*2 seasons.*Returning Series.*Drama/);
    assert.equal(await page.locator('.trk-hero-meta').evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
    assert.equal(await page.locator('.trk-hero-details .trk-status').getAttribute('data-status'),'watching');
    if(process.env.CW_LOGO_SCREENSHOTS) {
      await page.locator('.cw-page-hero').screenshot({path:`${process.env.CW_LOGO_SCREENSHOTS}/${theme}-${width}-episode.png`});
    }
    assert.equal(await page.evaluate(w=>document.documentElement.scrollWidth>w+1,width),false);
    await page.locator('[data-trk-layout="grid"]').click();
    assert.equal(await page.locator('.trk-grid-status').count(),2);
    const grid=page.locator('.trk-grid-card').first();
    const dateBounds=await grid.locator('.trk-grid-date').boundingBox();
    const statusBounds=await grid.locator('.trk-grid-status').boundingBox();
    const scoreBounds=await grid.locator('.cw-media-card-score').boundingBox();
    assert.ok(statusBounds.x>=dateBounds.x+dateBounds.width);
    assert.ok(Math.abs(statusBounds.y+statusBounds.height/2-dateBounds.y-dateBounds.height/2)<1);
    assert.ok(statusBounds.y>=scoreBounds.y+scoreBounds.height);
    await page.locator('[data-trk-action="filters"]').click();
    await page.locator('[data-trk-input="status-labels"]').uncheck();
    assert.equal(await page.locator('.trk-grid-status').count(),0);
    await page.reload();
    await page.locator('.trk-grid-card').first().waitFor();
    assert.equal(await page.locator('.trk-grid-status').count(),0);
    await page.locator('[data-trk-action="filters"]').click();
    assert.equal(await page.locator('[data-trk-input="status-labels"]').isChecked(),false);
    await page.locator('[data-trk-input="status-labels"]').check();
    assert.equal(await page.locator('.trk-grid-status').count(),2);
    const infoButton=page.locator('.trk-grid-card [data-trk-row-info]').nth(2);
    assert.equal(await infoButton.getAttribute('aria-label'),'Open details for House of the Dragon');
    assert.equal(await infoButton.evaluate(el=>!!el.closest('.trk-grid-art')),true);
    assert.equal(await page.locator('.trk-grid-copy [data-trk-row-info]').count(),0);
    if (width >= 800) {
      await page.mouse.move(0,0);
      assert.equal(await infoButton.evaluate(el=>getComputedStyle(el).opacity),'0');
      await page.keyboard.press('Tab');
      await infoButton.focus();
      assert.equal(await infoButton.evaluate(el=>getComputedStyle(el).opacity),'1');
      await infoButton.evaluate(el=>el.blur());
      await page.locator('.trk-grid-art').nth(2).hover();
    }
    assert.equal(await infoButton.evaluate(el=>getComputedStyle(el).opacity),'1');
    const infoBounds=await infoButton.boundingBox();
    assert.ok(infoBounds.width>=44&&infoBounds.height>=44);
    if(process.env.CW_LOGO_SCREENSHOTS) await page.locator('.trk-grid-card').first().screenshot({path:`${process.env.CW_LOGO_SCREENSHOTS}/${theme}-${width}-grid.png`});
    await infoButton.click();
    const modal=page.locator('#cw-media-modal');
    await modal.locator('.cw-mm-title-logo').waitFor();
    assert.equal(await page.locator('.trk-sheet:visible').count(),0);
    assert.equal(await modal.locator('.cw-mm-title-text').isVisible(),false);
    assert.equal(await modal.locator('#cw-mm-title').getAttribute('aria-label'),'House of the Dragon');
    assert.equal(await modal.locator('.cw-mm-trk .trk-status-btn').getAttribute('data-status'),'watching');
    const modalLogo=await modal.locator('.cw-mm-title-logo').boundingBox();
    assert.ok(modalLogo.x>=0&&modalLogo.x+modalLogo.width<=width&&modalLogo.height<=(width<=720?64:90));
    if(process.env.CW_LOGO_SCREENSHOTS) await modal.screenshot({path:`${process.env.CW_LOGO_SCREENSHOTS}/${theme}-${width}-card.png`});
    await page.evaluate(async()=>{
      const data=await (await fetch('/api/tracker/title?key=episode:3')).json();
      data.note.status='on_hold';
      CW.ProfileMediaModal.updateTracker(data);
    });
    assert.equal(await modal.locator('.cw-mm-trk .trk-status-btn').getAttribute('data-status'),'on_hold');
    await page.evaluate(()=>CW.ProfileMediaModal.open({type:'movie',title:'Missing logo',ids:{tmdb:'2'}}));
    await page.waitForFunction(()=>{const image=document.querySelector('.cw-mm-title-logo');return image?.complete&&!image.naturalWidth;});
    assert.equal(await modal.locator('.cw-mm-title-text').isVisible(),true);
    assert.equal(await modal.locator('.cw-mm-chips .cw-mm-trk-status').count(),0);
    assert.equal(await page.evaluate(()=>{const body=document.querySelector('.cw-mm-body');return body.scrollWidth>body.clientWidth+1;}),false);
  });
}
