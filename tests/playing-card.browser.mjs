/* tests/playing-card.browser.mjs */
/* CrossWatch - Cinematic playing card themes, stream navigation and route selection */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

const styles=['fonts/material-symbols-rounded','themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','ui-shell'];
const metadata={vote_average:7.5,vote_count:223,overview:'When Luke Ellis is kidnapped, he awakens at The Institute, a facility full of children with unusual abilities. In a nearby town, former police officer Tim Jamieson is looking for a new life, but their stories are destined to collide.',genres:['Mystery','Sci-Fi & Fantasy'],runtime_minutes:58,detail:{status:'Returning series',number_of_seasons:2,number_of_episodes:16,next_episode_to_air:{season_number:2,episode_number:2,air_date:'2026-11-15'}}};
const routes=[{id:'watcher:1',kind:'watcher',source:'plex',source_instance:'home',source_label:'Home server',destination:'trakt',destination_instance:'main',destination_label:'Main account'}, {id:'webhook:1',kind:'webhook',source:'plex',source_instance:'home',source_label:'Home server',destination:'simkl',destination_instance:'personal',destination_label:'Personal'}];

async function fixture(t,theme,width,options={}) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:900},reducedMotion:'reduce',...options});
  page.setDefaultTimeout(6000);
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  const streams=[{_key:'one',source:'plex',provider_instance:'home',provider_instance_label:'Home server',ids:{tmdb_show:'253372'},title:'The Institute',year:2026,media_type:'episode',season:2,episode:1,progress:24,duration_ms:3480000,state:'playing',routes}, {_key:'two',source:'jellyfin',ids:{tmdb:'329865'},title:'Arrival',year:2016,media_type:'movie',progress:50,state:'paused',routes:[]}, {_key:'three',source:'plextrakt',title:'Another stream',media_type:'movie',progress:5,state:'playing',routes:[routes[1]]}];
  await page.route('https://playing-card.test/**',async route=>{
    const path=new URL(route.request().url()).pathname;
    if(path.startsWith('/assets/'))return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});
    if(path.startsWith('/art/tmdb/')&&new URL(route.request().url()).searchParams.get('kind')==='backdrop')return route.fulfill(process.env.CW_PLAYING_BACKDROP ? {body:await readFile(process.env.CW_PLAYING_BACKDROP),contentType:'image/jpeg'} : {body:'<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="720"><rect width="1280" height="720" fill="#617780"/><path d="M0 600L380 180L650 500L950 100L1280 550V720H0Z" fill="#243940"/><path d="M0 680L450 400L850 650L1280 320V720H0Z" fill="#14262c"/></svg>',contentType:'image/svg+xml'});
    if(path==='/art'||path.startsWith('/art/tmdb/'))return route.fulfill(process.env.CW_PLAYING_ART ? {body:await readFile(process.env.CW_PLAYING_ART),contentType:'image/jpeg'} : {body:'<svg xmlns="http://www.w3.org/2000/svg" width="500" height="750"><defs><linearGradient id="a" x2="1" y2="1"><stop stop-color="#182831"/><stop offset="1" stop-color="#1a1030"/></linearGradient></defs><rect width="500" height="750" fill="url(#a)"/><circle cx="310" cy="270" r="160" fill="#385057"/></svg>',contentType:'image/svg+xml'});
    if(path==='/api/watch/currently_watching')return route.fulfill({json:{streams:streams.map(item=>({...item,cover:'/art',updated:Math.floor(Date.now()/1000)})),ts:Math.floor(Date.now()/1000)}});
    const attrs=theme==='original'?'':`data-cw-theme="${theme}"`;
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html ${attrs} data-tab="main"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${[...styles,theme==='original'?'themes/original-coverage':'themes/flat'].map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body><main><h1>Sync Hub</h1></main><script>window._cfgCache={tmdb_configured:true,ui:{}};window.CW={Meta:{peek:()=>null,get:async()=>(${JSON.stringify(metadata)}),tmdbId:item=>item?.ids?.tmdb_show||item?.ids?.tmdb||item?.tmdb||'',isEpisode:p=>p.media_type==='episode'},ProviderMeta:{label:s=>({plex:'Plex',trakt:'Trakt',simkl:'SIMKL',jellyfin:'Jellyfin'}[s]||s)}};</script><script src="/assets/helpers/playing-card.js"></script><script src="/assets/js/playingcard.js"></script></body></html>`});
  });
  await page.goto('https://playing-card.test/');
  await page.locator('.cw-pc-scrobble.show .pc-rating').filter({hasText:'7.5'}).waitFor({state:'attached'});
  await page.evaluate(()=>document.fonts.ready);
  return page;
}

for(const theme of ['original','flat-dark','flat-light'])for(const width of [320,390,760,820,1440])test(`${theme}: playing card at ${width}px`,async t=>{
  const page=await fixture(t,theme,width);
  const card=page.locator('.cw-pc-scrobble');
  const box=await card.boundingBox();
  assert.ok(box.x>=0&&box.x+box.width<=width);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  assert.equal(await card.evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
  assert.equal(await card.locator('.pc-overview-wrap').isVisible(),width>760);
  assert.equal(await card.locator('.pc-poster').isVisible(),width>760);
  const backdrop=card.locator(width<=760?'.pc-poster-link':'.pc-body');
  assert.match(await backdrop.evaluate(el=>getComputedStyle(el,'::before').backgroundImage),/kind=backdrop/);
  assert.match(await backdrop.evaluate(el=>getComputedStyle(el,'::before').backgroundImage),/v=2/);
  if(width<=760)assert.ok(box.height<300,`Mobile height ${box.height}`);
  else {
    assert.ok(box.width<=720,`Desktop width ${box.width}`);
    assert.ok(box.height<280,`Desktop height ${box.height}`);
  }
  assert.match(await card.locator('.pc-route-list').textContent(),/Plex → Trakt/);
  if(process.env.CW_PLAYING_SCREENSHOTS){
    await mkdir(process.env.CW_PLAYING_SCREENSHOTS,{recursive:true});
    await card.screenshot({path:`${process.env.CW_PLAYING_SCREENSHOTS}/${theme}-${width}.png`});
  }
  if(width<=760){
    await card.locator('.pc-route-toggle').click();
    if(process.env.CW_PLAYING_SCREENSHOTS&&width===390)await card.screenshot({path:`${process.env.CW_PLAYING_SCREENSHOTS}/${theme}-${width}-routes.png`});
    await card.locator('.pc-route-options .pc-route').nth(1).click();
    assert.match(await card.locator('.pc-route-list .is-selected').textContent(),/webhookPlex → SIMKL/i);
    assert.equal(await card.locator('.pc-nav-count').textContent(),'1 / 3');
    await card.locator('.pc-route-toggle').click();
    await page.keyboard.press('Escape');
    assert.equal(await card.locator('.pc-route-menu').isVisible(),false);
    assert.equal(await card.locator('.pc-route-toggle').evaluate(el=>el===document.activeElement),true);
  }
  await card.locator('.pc-nav .pc-next').click();
  assert.equal(await card.locator('.pc-title').textContent(),'Another stream');
  await card.locator('.pc-nav .pc-prev').click();
  assert.equal(await card.locator('.pc-title').textContent(),'The Institute');
  await card.locator('.pc-nav .pc-next').click();
  await card.locator('.pc-nav .pc-next').click();
  assert.equal(await card.locator('.pc-title').textContent(),'Arrival');
  assert.equal(await card.locator('.pc-status').textContent(),'Paused');
  assert.match(await card.locator('.pc-route-list').textContent(),/Destination unavailable/);
  assert.equal(await card.locator('.pc-route-toggle').isVisible(),true);
  await card.locator('.pc-nav .pc-next').click();
  assert.equal(await card.locator('.pc-title').textContent(),'The Institute');
  await card.locator('.pc-close').click();
  assert.equal(await card.isVisible(),false);
});

for(const width of [390,1440])test(`route selection switches playback and artwork at ${width}px`,async t=>{
  const page=await fixture(t,'flat-dark',width);
  const card=page.locator('.cw-pc-scrobble');
  await page.evaluate(()=>{
    CW.Meta.get=async item=>({overview:`${item.title} synopsis`,vote_average:8,runtime_minutes:100});
    window.openedMedia=[];
    CW.ProfileMediaModal={canOpen:()=>true,open:item=>openedMedia.push(item)};
  });
  await card.locator('.pc-route-toggle').click();
  assert.equal(await card.locator('.pc-route-options button').count(),4);
  await card.locator('.pc-route-options button').filter({hasText:'Another stream'}).click();
  assert.equal(await card.locator('.pc-title').textContent(),'Another stream');
  assert.equal(await card.locator('.pc-nav-count').textContent(),'2 / 3');
  assert.equal(await card.locator('.pc-progress-pct').textContent(),'5% watched');
  assert.equal(await card.evaluate(el=>el.style.getPropertyValue('--pc-backdrop')),'none');
  await card.locator('.pc-route-toggle').click();
  assert.equal(await card.locator('.pc-route-options [aria-pressed="true"]').count(),1);
  assert.match(await card.locator('.pc-route-options [aria-pressed="true"]').textContent(),/Another stream/);
  await card.locator('.pc-route-options button').filter({hasText:'Arrival'}).click();
  await card.locator('.pc-overview').filter({hasText:'Arrival synopsis'}).waitFor({state:'attached'});
  assert.equal(await card.locator('.pc-title').textContent(),'Arrival');
  assert.equal(await card.locator('.pc-status').textContent(),'Paused');
  assert.equal(await card.locator('.pc-progress-pct').textContent(),'50% watched');
  assert.match(await card.locator('.pc-poster').getAttribute('src'),/\/movie\/329865\?/);
  const backdrop=await card.evaluate(el=>el.style.getPropertyValue('--pc-backdrop'));
  assert.match(backdrop,/\/movie\/329865\?kind=backdrop/);
  assert.notEqual(backdrop,`url("${await card.locator('.pc-poster').getAttribute('src')}")`);
  await card.locator(width<=760?'.pc-mobile-title':'.pc-poster-link').click();
  assert.deepEqual(await page.evaluate(()=>openedMedia.map(item=>item.title)),['Arrival']);
  const refreshed=page.waitForResponse('**/api/watch/currently_watching');
  await page.evaluate(()=>window.dispatchEvent(new Event('cw:auth-state-changed')));
  await refreshed;
  await card.locator('.pc-overview').filter({hasText:'Arrival synopsis'}).waitFor({state:'attached'});
  assert.equal(await card.locator('.pc-title').textContent(),'Arrival');
  if(width>760){
    await card.locator('.pc-route-list button').filter({hasText:'Trakt'}).click();
    assert.equal(await card.locator('.pc-title').textContent(),'The Institute');
    assert.match(await card.locator('.pc-poster').getAttribute('src'),/\/tv\/253372\?/);
  }
  await page.route('**/api/watch/currently_watching',route=>route.fulfill({json:{streams:[{_key:'remaining',source:'plex',title:'Remaining playback',media_type:'movie',state:'playing',progress:10,routes:[routes[0]],updated:Math.floor(Date.now()/1000)}]}}));
  await page.evaluate(()=>window.dispatchEvent(new Event('cw:auth-state-changed')));
  await card.locator('.pc-title').filter({hasText:'Remaining playback'}).waitFor({state:'attached'});
  assert.equal(await card.locator('.pc-route-options button').count(),1);
  assert.equal(await card.locator('.pc-route-toggle').isVisible(),false);
  assert.equal(await card.evaluate(el=>el.style.getPropertyValue('--pc-backdrop')),'none');
  await page.route('**/api/watch/currently_watching',route=>route.fulfill({json:{streams:[]}}));
  await page.evaluate(()=>window.dispatchEvent(new Event('cw:auth-state-changed')));
  await card.waitFor({state:'hidden'});
});

test('desktop card stays compact on a shorter viewport with display scaling',async t=>{
  const page=await fixture(t,'flat-dark',1500,{deviceScaleFactor:1.5});
  await page.setViewportSize({width:1500,height:820});
  await page.evaluate(()=>{
    const card=document.querySelector('.cw-pc-scrobble');
    card.querySelector('.pc-overview').textContent='When 14 year old genius Luke Ellis is kidnapped, he awakens at The Institute, a facility full of children who all got there the same way he did, and who are all possessed of unusual abilities. In a nearby town, haunted former police officer Tim Jamieson has come looking to start a new life, but the peace and quiet will not last.';
  });
  const card=page.locator('.cw-pc-scrobble');
  const box=await card.boundingBox();
  assert.ok(box.width<=720);
  assert.ok(box.height<280);
  assert.ok(box.y>400,`Card top ${box.y}`);
  await card.locator('.pc-overview-more').click();
  assert.equal(await card.locator('.pc-overview-more').getAttribute('aria-expanded'),'true');
});

for(const width of [390,1440])test(`playing card opens internal media details at ${width}px`,async t=>{
  const page=await fixture(t,'flat-dark',width);
  await page.route('**/api/profile/title?*',route=>route.fulfill({json:{}}));
  await page.route('**/api/playback_progress/items?*',route=>route.fulfill({json:{items:[]}}));
  await page.evaluate(()=>{
    CW.ProfileDateTime={format:value=>String(value)};
    window.compactOpened=[];
    CW.WatchlistPreview={openPreviewDrawer:item=>compactOpened.push(item)};
  });
  await page.addScriptTag({url:'/assets/js/profile-media-modal.js'});
  const card=page.locator('.cw-pc-scrobble');
  const opener=card.locator(width<=760?'.pc-mobile-title':'.pc-poster-link');
  assert.equal(await card.locator('.pc-poster-link').getAttribute('href'),null);
  await opener.click();
  await page.locator('.cw-mm:not([hidden])').waitFor();
  assert.equal(await card.isVisible(),false);
  assert.match(await page.locator('.cw-mm').textContent(),/The Institute/);
  await page.evaluate(()=>CW.ProfileMediaModal.close());
  assert.equal(await card.isVisible(),true);
  await opener.focus();
  await page.keyboard.press('Enter');
  await page.locator('.cw-mm:not([hidden])').waitFor();
  await page.evaluate(()=>CW.ProfileMediaModal.close());
  await card.locator('.pc-nav .pc-next').click();
  assert.equal(await page.evaluate(()=>CW.ProfileMediaModal.isOpen()),false);
  assert.equal(await opener.getAttribute('aria-disabled'),'true');
  await card.locator('.pc-heading').click();
  assert.equal(await page.evaluate(()=>CW.ProfileMediaModal.isOpen()),false);
  await card.locator('.pc-nav .pc-next').click();
  await card.locator('.pc-heading').click();
  await page.locator('.cw-mm:not([hidden])').waitFor();
  assert.match(await page.locator('.cw-mm').textContent(),/Arrival/);
  await page.evaluate(()=>CW.ProfileMediaModal.close());
  await page.evaluate(()=>CW.ProfileMediaModal.setStyle('compact'));
  await opener.click();
  assert.deepEqual(await page.evaluate(()=>compactOpened.map(item=>[item.title,item.media_type,item.tmdb])),[['Arrival','movie','329865']]);
  assert.equal(page.context().pages().length,1);
  assert.equal(page.url(),'https://playing-card.test/');
});

test('long titles, route names, missing art and watchlist preview remain usable',async t=>{
  const page=await fixture(t,'flat-light',320);
  await page.evaluate(()=>{
    document.querySelector('.cw-pc-scrobble').remove();
    window.preview=CW.PlayingCard.mount({variant:'scrobble'});
    preview.render({key:'long',title:'A very long title that needs to wrap without hiding the route controls',heading:'S01 E01',poster:'/missing-art',routes:Array.from({length:5},(_,i)=>({id:String(i),kind:'watcher',source:'Jellyfin',destination:'CrossWatch',sourceLabel:'A very long home server name',destinationLabel:'A different destination account'})),chips:[{text:'2026'},{text:'58m'}],information:[],rating:{value:null},status:{text:'Buffering...',icon:'play_arrow'},progress:{pct:0}});
    preview.show();
  });
  const card=page.locator('.cw-pc-scrobble');
  assert.equal(await card.locator('.pc-nav').isVisible(),false);
  assert.equal(await card.locator('.pc-nav .pc-next').isDisabled(),true);
  assert.equal(await card.locator('.pc-route-count-mobile').textContent(),'+4 routes');
  assert.equal(await card.locator('.pc-route-count-desktop').textContent(),'+3 routes');
  await card.locator('.pc-route-toggle').click();
  await card.locator('.pc-route-options button').last().click();
  assert.equal(await card.evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
  assert.equal(await card.locator('.pc-mobile-score').textContent(),'--');
  await page.evaluate(()=>{
    preview.destroy();
    window.preview=CW.PlayingCard.mount({variant:'watchlist'});
    preview.render({title:'Arrival',poster:'/art',overview:'A watchlist preview',information:[],rating:{value:7.5},sources:[{label:'Plex',short:'Plex'}],links:[{text:'TMDB',href:'https://www.themoviedb.org/movie/329865'}]});
    preview.show();
  });
  assert.equal(await page.locator('.cw-pc-watchlist .pc-title').textContent(),'Arrival');
  assert.equal(await page.locator('.cw-pc-watchlist .pc-trailer').isVisible(),true);
  assert.equal(await page.locator('.cw-pc-watchlist .pc-link').isVisible(),true);
});
