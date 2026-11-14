/* tests/profile-mobile.browser.mjs */
/* CrossWatch - Profile artwork and touch workflows at phone widths */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from "node:test";
import assert from "node:assert/strict";
import {execFileSync} from "node:child_process";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const template = execFileSync("python", ["-c", "import ui_frontend; print(ui_frontend.get_profile_html({'is_admin':True,'username':'mobile','display_name':'Mobile preview'}))"], {encoding:"utf8",maxBuffer:1024*1024});
const items = ["The Grand Budapest Hotel", "A long episode title with plenty of words", "Arrival"].map((title,index)=>({key:`movie:tmdb:${index+1}`,type:"movie",title,year:2014,ids:{tmdb:String(index+1)},tmdb:String(index+1),sort_epoch:1793989800,watched_at:"2026-11-06T18:30:00Z",last_collected_at:1793989800,rating:8.5,watch_count:3,watch_epochs:[1793989800],providers:["PLEX","TRAKT"],sources_by_provider:{PLEX:["default"],TRAKT:["default"]},progress:45,can_update_progress:true,can_mark_watched:true,can_remove_progress:true,provider:"PLEX",provider_instance:"default",duration:120,remaining_minutes:66,media:{resolution:"4k",hdr:"dolby_vision",audio:"truehd"}}));

async function fixture(t,theme,width,{count=0,activity=false,history=false,hero=false,playback=false,library=false,liveRoutes=false}={}) {
  const browser=await (process.env.CW_PROFILE_BROWSER === "webkit" ? webkit : chromium).launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:844},hasTouch:width<=760,isMobile:width<=760,reducedMotion:'reduce'});
  page.setDefaultTimeout(6000);
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  const state={empty:false,queries:[],playing:null,watchGate:null};
  if(hero) await page.clock.install();
  const fixtureItems = count ? Array.from({length:count},(_,index)=>({...items[index % items.length],key:`movie:tmdb:${index+1}`,title:`Movie ${index+1}`,tmdb:String(index+1),ids:{tmdb:String(index+1)}})) : items;
  const html=await page.evaluate(({source,theme,hero,activity})=>{
    const doc=new DOMParser().parseFromString(source,'text/html');
    doc.querySelectorAll('script').forEach(script=>script.remove());
    doc.documentElement.className=theme==='original'?'cw-theme-original':`cw-theme-${theme==='flat-light'?'light':'dark'}`;
    if(theme!=='original') doc.documentElement.dataset.cwTheme=theme;
    doc.getElementById(theme==='original'?'cw-theme-flat-css':'cw-theme-original-css')?.remove();
    const bootstrap=doc.createElement('script');
    bootstrap.textContent='window.CW={AuthState:{read:()=>({isAdmin:true,permissions:{write:true}})},Meta:{peek:()=>null,get:async()=>({year:2026,runtime_minutes:58,genres:["Drama","Mystery"],detail:{number_of_seasons:2,status:"Returning Series"}})}};';
    doc.body.append(bootstrap);
    for(const path of [...(activity?['helpers/provider-meta']:[]),'helpers/card-actions','helpers/selection-bar','helpers/profile-datetime','helpers/month-bar','helpers/icon-select','helpers/account-menu',...(hero?['helpers/profile-select','js/overview-profile']:[]),'js/profile-page']) {
      const script=doc.createElement('script');
      script.src=`/assets/${path}.js`;
      doc.body.append(script);
    }
    return '<!doctype html>'+doc.documentElement.outerHTML;
  },{source:template,theme,hero,activity});
  await page.route('https://profile-mobile.test/**',async route=>{
    const url=new URL(route.request().url()),path=url.pathname;
    if(path.startsWith('/assets/')) {
      try{return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(path.startsWith('/art/')) return route.fulfill({contentType:'image/svg+xml',body:url.searchParams.get('kind')==='logo'?'<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="150"><text x="0" y="120" font-size="110" font-family="sans-serif" fill="white">THE INSTITUTE</text></svg>':'<svg xmlns="http://www.w3.org/2000/svg" width="400" height="600"><rect width="400" height="600" fill="#243c57"/><circle cx="200" cy="200" r="100" fill="#447a89"/><path d="M0 600L200 250L400 600" fill="#c89162"/></svg>'});
    if(path.startsWith('/api/')) {
      const list=state.empty?[]:fixtureItems;
      state.queries.push({path,params:Object.fromEntries(url.searchParams)});
      if(path==='/api/watch/currently_watching') {
        state.watchStarted?.();
        if(state.watchGate) await state.watchGate;
      }
      let json={ok:true};
      if(path==='/api/profile') json={user:{is_admin:true,username:'mobile',display_name:'Mobile preview',permissions:{write:true},preferences:{},sessions:[]}};
      if(hero && path==='/api/app-auth/status') json={authenticated:true,user:{is_admin:true,username:'mobile',display_name:'Mobile preview'}};
      if(hero && path==='/api/user-profiles') json={items:[{id:'family',label:'Family'}]};
      if(path==='/api/watch/currently_watching') json={streams:state.playing?[state.playing]:[]};
      if(['/api/profile/collection','/api/profile/history','/api/profile/ratings','/api/profile/watchlist'].includes(path)) json={items:list,total:list.length,page:1,page_size:24,pages:1,counts:{total:list.length,movies:list.length},providers:['PLEX','TRAKT'],months:[]};
      if(history && path==='/api/profile/history') Object.assign(json,{
        items:list.map(item=>({...item,present:[{provider:'PLEX',instance:'default'}],missing:[{provider:'TRAKT',instance:'default'}],source:{provider:'PLEX',instance:'default'},targets:[{provider:'TRAKT',instance:'default'}],method:'watcher'})),
        stats:{watches:list.length,plays:list.length,full:0,partial:list.length,endpoints:2,scrobbles:list.length},
        counts:{all:list.length,movie:list.length,episode:0},
        providers:[{provider:'PLEX',count:list.length},{provider:'TRAKT',count:list.length}],
        months:list.length?[{month:'2025-01',count:1},{month:'2026-11',count:2}]:[],
      });
      if(library && ['/api/profile/watchlist','/api/profile/ratings','/api/profile/collection'].includes(path)) Object.assign(json,{
        items:list.map(item=>({...item,media_info:{resolution:'uhd_4k',hdr:'dolby_vision',audio:'dolby_atmos'},present:[{provider:'PLEX',instance:'default',rating:8.5}],missing:[{provider:'TRAKT',instance:'default'}]})),
        stats:{plays:list.length,full:0,partial:list.length,endpoints:2,mismatch:1,average:8.5},
        counts:{all:list.length,movie:list.length,ratings:{8.5:list.length}},
        providers:[{provider:'PLEX',count:list.length},{provider:'TRAKT',count:list.length}],
        months:list.length?[{month:'2025-01',count:1},{month:'2026-11',count:2}]:[],
      });
      if(path==='/api/playback_progress/items') json={items:list,total:list.length,providers:[],errors:[]};
      if(playback && path==='/api/playback_progress/items') Object.assign(json,{
        items:list.map((item,index)=>({...item,media_type:'movie',instance_id:'default',remote_id:`play-${index}`,progress_percent:45,providers:[{provider:'PLEX',instance_id:'default'}]})),
        providers:[{provider:'PLEX',instance_id:'default',configured:true,read:true,included:true},{provider:'TRAKT',instance_id:'default',configured:true,read:true,included:true}],
        errors:[{provider:'TRAKT',instance_id:'default',error_code:'provider_timeout',message:'Provider timed out',retryable:true}],
        months:list.length?[{month:'2025-01',count:1},{month:'2026-11',count:2}]:[],
        summary:{total:list.length,movies:list.length,almost_done:1,stale:1},
      });
      if(liveRoutes && path==='/api/playback_progress/items') json.items=json.items.slice(0,1).map(item=>({...item,
        live_state:'playing', live_progress_percent:76,
        live_routes:['crosswatch','trakt','simkl'].map(destination=>({kind:'watcher',source:'plex',source_instance:'default',source_label:'Living room',destination,destination_instance:'default',destination_label:destination==='trakt'?'My Trakt':'Default'})),
      }));
      if(path==='/api/dashboard/widgets') json={recent_history:{items:list},recent_scrobble:{items:list},recent_progress:{items:list},latest_ratings:{items:list}};
      if(path==='/api/state/wall') json={items:list};
      if(path==='/api/profile/title') json={item:items[0],title:items[0].title};
      if(path==='/api/profile/preferences') json={preferences:{}};
      if(activity && path==='/api/events/feed') json={items:state.empty?[]:Array.from({length:8},(_,index)=>({id:String(index+1),kind:'sync',title:'Sync run completed',feature:index%2?'history':'watchlist',summary:'Sync run completed successfully, Plex -> Trakt, SIMKL, CrossWatch (Watchlist)',created_at:Math.floor(Date.now()/1000)-60-index*3600,event_count:14,status:index===1?'warning':'completed'}))};
      if(activity && path==='/api/scheduling/status') json=state.empty?{}:{running:true,effective:{enabled:true,mode:'every_n_hours'},next_run_at:Date.now()/1000+10800};
      if (count && ['/api/profile/collection','/api/profile/history','/api/profile/ratings','/api/profile/watchlist','/api/playback_progress/items'].includes(path)) {
        const page = Number(url.searchParams.get('page') || 1), size = Number(url.searchParams.get('page_size') || 48);
        Object.assign(json,{items:list.slice((page-1)*size,page*size),page,page_size:size,pages:Math.ceil(list.length/size)});
      }
      if(state.providerRefs && path==='/api/profile/history') json.items=json.items.map(item=>({...item,present:state.providerRefs,missing:[]}));
      return route.fulfill({json});
    }
    return route.fulfill({contentType:'text/html',body:html});
  });
  await page.goto('https://profile-mobile.test/profile');
  await page.locator('.cw-profile-tabs').waitFor({state:'attached'});
  await page.evaluate(()=>document.fonts.ready);
  return {page,state};
}

async function snapshot(page,name) {
  if(!process.env.CW_PROFILE_SCREENSHOTS) return;
  await mkdir(process.env.CW_PROFILE_SCREENSHOTS,{recursive:true});
  await page.locator('.cw-profile-panel.active').screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/${name}.png`});
}

async function openRecordTools(panel) {
  if(await panel.evaluate(el=>el.classList.contains('has-mobile-tools')) && !await panel.locator('.cw-hub-mobile-sheet').evaluate(el=>el.open)) await panel.locator('.cw-hub-mobile-filter').click();
}

async function clickRecordControl(panel,selector) {
  const mobile=await panel.evaluate(el=>el.classList.contains('has-mobile-tools'));
  const sheet=panel.locator('.cw-hub-mobile-sheet');
  if(mobile && selector.includes('filters-toggle')) {
    if(await sheet.evaluate(el=>el.open)) await sheet.getByRole('button',{name:'Done',exact:true}).click();
    else await openRecordTools(panel);
    return;
  }
  if(mobile && selector.includes('data-timeline-source')) {
    await panel.getByRole('combobox',{name:'History source'}).selectOption('scrobble');
    return;
  }
  await openRecordTools(panel);
  await panel.locator(selector).click();
  if(mobile && await sheet.evaluate(el=>el.open)) await sheet.getByRole('button',{name:'Done',exact:true}).click();
}

test('Hub mobile tools keep filters accessible without crowding the media lists',async t=>{
  const {page}=await fixture(t,'flat-dark',390,{history:true,playback:true,library:true});
  for(const key of ['history','watchlist','ratings','playback','collection']) {
    await page.locator(`[data-profile-tab="${key}"]`).click();
    const panel=page.locator(`#profile-panel-${key}`);
    const sheet=panel.locator('.cw-hub-mobile-sheet');
    await panel.locator('.cw-hist-card,.cw-play-card,.cw-collection-card').first().waitFor();
    assert.equal(await panel.locator('.cw-hub-records-head').isVisible(),false);
    assert.equal(await panel.locator('.cw-hub-records-controls').isVisible(),false);
    assert.match(await panel.locator('.cw-hub-mobile-summary').innerText(),/3/);
    const toolbar=await panel.locator('.cw-hub-records-toolbar').boundingBox();
    assert.ok(toolbar.height<=48);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
    await panel.locator('.cw-hub-mobile-filter').click();
    assert.equal(await sheet.evaluate(el=>el.open),true);
    assert.equal(await sheet.locator('[data-timeline-filters-toggle]:visible,[data-playback-filters-toggle]:visible').count(),0);
    assert.equal(await sheet.evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
    if(process.env.CW_PROFILE_SCREENSHOTS) {
      await mkdir(process.env.CW_PROFILE_SCREENSHOTS,{recursive:true});
      await page.screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/mobile-sheet-${key}.png`});
    }
    const provider=page.locator(`#profile-${key}-provider`);
    const wrap=provider.locator('xpath=following-sibling::*[1]');
    await wrap.locator('button').click();
    await sheet.locator('.cw-icon-select-menu:not(.hidden) [data-value]').filter({hasText:/plex/i}).first().click();
    await page.waitForFunction(key=>document.querySelector(`#profile-${key}-provider`).value.toUpperCase().includes('PLEX'),key);
    await sheet.getByRole('button',{name:'Done',exact:true}).click();
    const clear=panel.locator('.cw-hub-mobile-active button[aria-label^="Remove"]');
    await clear.first().waitFor();
    await clear.first().click();
    await page.waitForFunction(key=>document.querySelector(`#profile-${key}-provider`).value==='',key);
    await panel.locator('.cw-hub-mobile-filter').click();
    await sheet.locator('[data-timeline-type="movie"],[data-playback-type="movie"],[data-collection-type="movie"]').click();
    await sheet.getByRole('button',{name:'Done',exact:true}).click();
    await panel.getByRole('button',{name:'Remove Movies filter',exact:true}).click();
    await page.waitForFunction(key=>!document.querySelector(`#profile-panel-${key} .cw-hub-mobile-active [aria-label="Remove Movies filter"]`),key);
    if(key==='history') {
      await panel.getByRole('combobox',{name:'History source'}).selectOption('scrobble');
      await panel.locator('.cw-hist-route').first().waitFor();
      await panel.locator('.cw-hub-mobile-filter').click();
      assert.equal(await page.locator('#profile-history-filters').isVisible(),false);
      await page.keyboard.press('Escape');
      assert.equal(await sheet.evaluate(el=>el.open),false);
      await panel.getByRole('combobox',{name:'History source'}).selectOption('synced');
    }
    if(key==='playback') assert.equal(await panel.getByRole('button',{name:'Provider needs attention'}).isVisible(),true);
    await panel.evaluate(el=>el.scrollIntoView({block:'start'}));
    await snapshot(page,`mobile-tools-${key}`);
  }
  const panel=page.locator('#profile-panel-collection');
  await panel.locator('.cw-hub-mobile-filter').click();
  await page.setViewportSize({width:1440,height:900});
  await page.waitForFunction(()=>!document.querySelector('#profile-panel-collection').classList.contains('has-mobile-tools'));
  assert.equal(await panel.locator('.cw-hub-mobile-sheet').evaluate(el=>el.open),false);
  assert.equal(await panel.locator('.cw-hub-records-head').isVisible(),true);
  assert.equal(await panel.locator('.cw-hub-records-controls').isVisible(),true);
  assert.equal(await panel.locator('.cw-hub-mobile-filter').isVisible(),false);
  assert.equal(await panel.locator('#profile-collection-provider').count(),1);
  await page.setViewportSize({width:390,height:844});
  await panel.locator('.cw-hub-mobile-filter').click();
  await page.locator('#profile-collection-sort').selectOption('title',{force:true});
  await panel.locator('.cw-hub-mobile-sheet').getByRole('button',{name:'Done',exact:true}).click();
  await panel.getByRole('button',{name:/Remove.*Title.*filter/i}).waitFor();
});

for(const width of [390,1440]) test(`Hub header follows playback and falls back to last watched at ${width}px`,async t=>{
  const {page,state}=await fixture(t,'flat-dark',width,{hero:true});
  const heading=page.locator('#profile-hero-media');
  await heading.locator('img:not([hidden])').waitFor();
  assert.equal(await heading.locator('img').evaluate(el=>getComputedStyle(el).backgroundColor),'rgba(0, 0, 0, 0)');
  assert.equal(await page.locator('.cw-media-hero-kicker').textContent(),'My Media Hub');
  assert.equal(await page.locator('.cw-media-hero-brand #profile-display-name').textContent(),'Mobile preview');
  const picker=await page.locator('#cw-view-as').boundingBox();
  const identity=await page.locator('.cw-profile-identity').boundingBox();
  assert.ok(identity.x+identity.width<=picker.x);
  assert.equal(await page.locator('.cw-profile-avatar-badge').count(),0);
  assert.equal(await page.locator('#profile-hero-chips').isVisible(),width>760);
  assert.match(await heading.innerText(),/2026.*Movie/);
  assert.equal(await page.locator('.cw-profile-last').isVisible(),true);
  assert.match(await page.locator('#profile-last-meta').innerText(),/ago/);
  const bounds=await page.locator('#profile-hero').boundingBox();
  assert.ok(bounds.height>=220);
  assert.equal(await page.locator('#profile-hero').evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
  state.playing={type:'episode',title:'The Institute',series_title:'The Institute',ids:{tmdb_show:'99'},season:2,episode:1,state:'playing',progress:65,duration:58,source:{provider:'PLEX'},targets:[{provider:'TRAKT'}]};
  await page.clock.runFor(15000);
  await page.locator('#profile-hero.is-playing').waitFor();
  await heading.locator('img[alt="The Institute"]:not([hidden])').waitFor();
  assert.match(await heading.locator('img').getAttribute('src'),/tv\/99\?kind=logo/);
  assert.match(await page.locator('#profile-hero-now').getAttribute('style'),/tv\/99\?.*kind=backdrop/);
  assert.match(await heading.innerText(),/2026.*TV Series.*S02E01/);
  assert.equal(await page.locator('#profile-hero-extra').isVisible(),width>760);
  assert.equal(await page.locator('.cw-profile-last').isVisible(),false);
  assert.equal(await page.locator('#profile-now-fill').evaluate(el=>el.style.width),'65%');
  assert.match(await page.locator('#profile-now-left').innerText(),/20 min left/);
  assert.equal(await page.locator('#profile-now-provider .cw-profile-provider-badge').count(),2);
  await page.evaluate(()=>{window.CW.ProfileMediaModal={canOpen:()=>true,open:item=>window.__openedHero=item.title};});
  await heading.click();
  assert.equal(await page.evaluate(()=>window.__openedHero),'The Institute');
  if(process.env.CW_PROFILE_SCREENSHOTS){
    await mkdir(process.env.CW_PROFILE_SCREENSHOTS,{recursive:true});
    await page.locator('#profile-hero').screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/hub-header-${width}.png`});
  }
  state.playing=null;
  await page.clock.runFor(15000);
  await page.locator('#profile-hero:not(.is-playing)').waitFor();
  await heading.locator('img[alt="The Grand Budapest Hotel"]:not([hidden])').waitFor();
  assert.equal(await page.locator('.cw-profile-last').isVisible(),true);
  if(process.env.CW_PROFILE_SCREENSHOTS) await page.locator('#profile-hero').screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/hub-header-idle-${width}.png`});
  await page.route('**/art/tmdb/movie/1?kind=logo*',route=>route.fulfill({status:404,body:''}));
  await page.reload();
  await page.waitForFunction(()=>{const image=document.querySelector('#profile-hero-media img');return image?.alt==='The Grand Budapest Hotel'&&image.complete&&!image.naturalWidth;});
  await heading.locator('strong:not([hidden])').waitFor();
  assert.match(await heading.locator('strong').innerText(),/The Grand Budapest Hotel/);
  state.empty=true;
  await page.reload();
  await page.locator('#profile-hero.no-last').waitFor();
  assert.equal(await heading.locator('img').isVisible(),false);
  assert.equal(await page.locator('.cw-profile-last').isVisible(),false);
});

test('Hub header shares slow playback polls and retains loaded artwork',async t=>{
  const {page,state}=await fixture(t,'flat-dark',1440,{hero:true});
  const heading=page.locator('#profile-hero-media');
  await heading.locator('img:not([hidden])').waitFor();
  state.playing={type:'movie',title:'Arrival',ids:{tmdb:'3'},state:'playing',progress:30,duration:120};
  await page.clock.runFor(15000);
  await heading.locator('img[alt="Arrival"]:not([hidden])').waitFor();
  await page.waitForFunction(()=>document.querySelector('#profile-now-fill').style.width==='30%');
  state.playing={...state.playing,progress:35};
  let release;
  state.watchGate=new Promise(resolve=>{release=resolve;});
  t.after(()=>release());
  const reads=()=>state.queries.filter(query=>query.path==='/api/watch/currently_watching').length;
  const before=reads();
  const started=new Promise(resolve=>{state.watchStarted=resolve;});
  await page.clock.runFor(15000);
  await started;
  assert.equal(reads(),before+1);
  await page.clock.runFor(30000);
  assert.equal(reads(),before+1);
  state.watchGate=null;
  release();
  await page.waitForFunction(()=>document.querySelector('#profile-now-fill').style.width==='35%');
  const artRequests=[];
  page.on('request',request=>{if(new URL(request.url()).pathname.startsWith('/art/')) artRequests.push(request.url());});
  await page.evaluate(()=>{
    window.__heroLogo=document.querySelector('#profile-hero-media img');
    window.__heroLogoChanges=0;
    new MutationObserver(records=>{window.__heroLogoChanges+=records.length;}).observe(window.__heroLogo,{attributes:true,attributeFilter:['src','hidden']});
  });
  state.playing={...state.playing,progress:40};
  await page.clock.runFor(15000);
  await page.waitForFunction(()=>document.querySelector('#profile-now-fill').style.width==='40%');
  assert.equal(reads(),before+2);
  assert.equal(await page.evaluate(()=>window.__heroLogo===document.querySelector('#profile-hero-media img')&&window.__heroLogoChanges===0),true);
  assert.deepEqual(artRequests,[]);
});

for(const width of [1440,2250]) for(const count of [3,12]) test(`Hub media grids match Tracker sizing at ${width}px with ${count} items`,async t=>{
  const {page}=await fixture(t,'flat-dark',width,{count,history:true,playback:true,library:true});
  for(const key of ['history','watchlist','ratings','playback','collection']) {
    await page.locator(`[data-profile-tab="${key}"]`).click();
    const panel=page.locator(`#profile-panel-${key}`);
    const card=panel.locator(key==='collection'?'.cw-collection-card':'.cw-hist-card').first();
    await card.waitFor();
    const size=await card.boundingBox();
    if(key==='collection') {
      assert.ok(size.width<=240,JSON.stringify({key,size}));
    } else {
      const reference=await panel.locator('.cw-hist-day-body').first().evaluate(grid=>{
        const tracker=document.createElement('div');
        tracker.id='page-tracker';
        tracker.style.cssText='position:absolute;visibility:hidden;padding:0;border:0';
        tracker.innerHTML='<div class="trk-cover-grid"><article class="trk-grid-card"><div class="trk-grid-art"></div></article></div>';
        tracker.firstElementChild.style.width=`${grid.getBoundingClientRect().width}px`;
        document.body.append(tracker);
        const {width,height}=tracker.querySelector('.trk-grid-art').getBoundingClientRect();
        tracker.remove();
        return {width,height};
      });
      const art=await card.locator('.cw-hist-art').boundingBox();
      assert.ok(Math.abs(size.width-reference.width)<=2,JSON.stringify({key,size,reference}));
      assert.ok(Math.abs(art.height-reference.height)<=2,JSON.stringify({key,art,reference}));
      assert.ok(await card.evaluate(el=>el.scrollWidth<=el.clientWidth+1),`${key} card overflows`);
    }
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
    if(count===12) await snapshot(page,`hub-sizing-${key}-${width}`);
  }
});

for(const width of [390,980,1440]) test(`Hub Progress keeps provider actions, filters and timeline at ${width}px`,async t=>{
  const {page,state}=await fixture(t,'flat-dark',width,{playback:true});
  await page.evaluate(()=>{
    window.CW.ItemActions={openMenu:args=>{window.__providerAction=args;}};
    window.CW.ProfileMediaModal={canOpen:()=>true,open:item=>window.__openedMedia=item};
  });
  if(width<=760) await page.locator('[data-profile-tab="playback"]').click();
  else await page.locator('[data-profile-tab="playback"]').click();
  const panel=page.locator('#profile-panel-playback');
  await panel.locator('.cw-play-card').first().waitFor();
  assert.equal(await panel.locator('.cw-collection-tile').count(),0);
  assert.equal(await panel.locator('.cw-hub-records-stat').count(),4);
  assert.equal(await page.locator('#profile-playback-timeline').isVisible(),false);
  assert.match(await page.locator('#profile-playback-errors').innerText(),/TRAKT.*Timed out/is);
  assert.equal(await panel.locator('.cw-play-progress strong').first().evaluate(el=>getComputedStyle(el).fontWeight),'400');
  const columns=await panel.locator('.cw-hist-day-body').first().evaluate(el=>getComputedStyle(el).gridTemplateColumns.split(' ').length);
  const gridWidth=await panel.locator('.cw-hist-day-body').first().evaluate(el=>el.getBoundingClientRect().width);
  assert.equal(columns,width<=760?2:Math.floor((gridWidth+16)/256));
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await panel.locator('[data-profile-poster-key]').first().click();
  assert.equal(await page.evaluate(()=>window.__openedMedia.title),items[0].title);
  await panel.locator('[data-ia-chip]').first().click();
  assert.ok(await page.evaluate(()=>!!window.__providerAction));
  await panel.locator('[data-playback-menu]').first().click();
  await page.locator('.cw-card-action-menu [data-playback-action="update_progress"]').click();
  await page.locator('#profile-playback-edit[open]').waitFor();
  assert.match(await page.locator('#profile-playback-edit-sub').innerText(),/1 provider record/);
  await page.locator('#profile-playback-edit [data-playback-dialog-cancel]').click();
  await panel.locator('[data-playback-select]').first().check({force:true});
  assert.equal(await page.locator('#profile-playback-bulk').isVisible(),true);
  await panel.locator('[data-playback-select]').first().uncheck({force:true});
  await clickRecordControl(panel,'[data-playback-filters-toggle]');
  const filters=page.locator('#profile-playback-filters-panel');
  const bounds=await filters.boundingBox();
  assert.ok(bounds.x>=0&&bounds.x+bounds.width<=width);
  const providerRequest=page.waitForResponse(r=>r.url().includes('/api/playback_progress/items?')&&new URL(r.url()).searchParams.get('provider')==='PLEX');
  await page.locator('#profile-playback-provider').selectOption('PLEX:default',{force:true});
  await providerRequest;
  assert.equal(await page.locator('#profile-playback-filters-count').innerText(),'1');
  await clickRecordControl(panel,'[data-playback-filters-toggle]');
  await clickRecordControl(panel,'[data-playback-filter="progress"]');
  await page.waitForFunction(()=>document.querySelector('[data-playback-filter="progress"]')?.getAttribute('aria-pressed')==='true');
  await clickRecordControl(panel,'[data-playback-timeline]');
  await page.locator('#profile-playback-timeline .cw-hist-bars').waitFor();
  assert.equal(await panel.locator('[data-playback-timeline]').getAttribute('aria-label'),'Hide timeline');
  await page.locator('#profile-playback-timeline [data-timeline-zoom="1"]').click();
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.profile.playback')));
  assert.equal(saved.timeline,true);
  await clickRecordControl(panel,'[data-playback-timeline]');
  await snapshot(page,`hub-progress-${width}`);
  await clickRecordControl(panel,'[data-playback-view="list"]');
  await panel.locator('.cw-play-row').first().waitFor();
  const progressRow=panel.locator('.cw-play-row').first();
  assert.equal(await progressRow.locator('.cw-hist-cell--when').isVisible(),true);
  assert.equal(await progressRow.locator('.cw-play-cell--progress').isVisible(),true);
  assert.match(await progressRow.locator('.cw-play-cell--progress').innerText(),/45%/);
  const rowBounds=await progressRow.boundingBox();
  for(const cell of await progressRow.locator(':scope > .cw-hist-cell').all()) {
    const bounds=await cell.boundingBox();
    assert.ok(bounds&&bounds.x>=rowBounds.x&&bounds.x+bounds.width<=rowBounds.x+rowBounds.width+1);
  }
  await snapshot(page,`hub-progress-list-${width}`);
  await progressRow.locator('[data-profile-poster-key]').click();
  assert.equal(await page.evaluate(()=>window.__openedMedia.title),items[0].title);
  await progressRow.evaluate(el=>el.scrollIntoView({block:'center',behavior:'instant'}));
  await progressRow.locator('.cw-tl-select').click();
  assert.equal(await progressRow.locator('[data-playback-select]').isChecked(),true);
  assert.equal(await page.locator('#profile-playback-bulk').isVisible(),true);
  await progressRow.locator('.cw-tl-select').click();
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await page.reload();
  if(width<=760) await page.locator('[data-profile-tab="playback"]').click();
  else await page.locator('[data-profile-tab="playback"]').click();
  await panel.locator('.cw-play-row').first().waitFor();
  assert.equal(await page.locator('#profile-playback-timeline').isVisible(),false);
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.profile.playback')).zoom),saved.zoom);
  state.empty=true;
  await clickRecordControl(panel,'#profile-playback-refresh');
  await panel.locator('.cw-profile-empty').waitFor();
  await snapshot(page,`hub-progress-empty-${width}`);
});

for(const width of [390,1440]) test(`Hub History preserves filters, provider actions and timeline at ${width}px`,async t=>{
  const {page,state}=await fixture(t,'flat-dark',width,{history:true});
  await page.evaluate(()=>{
    window.CW.ItemActions={openMenu:args=>{window.__providerAction={feature:args.feature,refs:args.refs,focus:args.focus};}};
    window.CW.ProfileMediaModal={canOpen:()=>true,open:item=>window.__openedMedia=item};
  });
  if(width<=760) await page.locator('[data-profile-tab="history"]').click();
  else await page.locator('[data-profile-tab="history"]').click();
  const panel=page.locator('#profile-panel-history');
  await panel.locator('.cw-hist-card').first().waitFor();
  assert.equal(await page.locator('#profile-history-title').textContent(),'History');
  assert.equal(await page.locator('#profile-history-metrics .cw-hub-records-stat').count(),4);
  assert.equal(await page.locator('#profile-history-timeline').isVisible(),false);
  assert.equal(await panel.locator('.cw-hist-name').first().evaluate(el=>getComputedStyle(el).fontWeight),'500');
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await panel.locator('[data-ia-chip]').first().click();
  assert.equal(await page.evaluate(()=>window.__providerAction.feature),'history');
  assert.equal(await page.evaluate(()=>window.__providerAction.refs.missing[0].provider),'TRAKT');
  await panel.locator('[data-profile-poster-key]').first().click();
  assert.equal(await page.evaluate(()=>window.__openedMedia.title),items[0].title);
  await clickRecordControl(panel,'[data-timeline-filters-toggle]');
  assert.equal(await page.locator('#profile-history-filters').isVisible(),true);
  const coverageRequest=page.waitForResponse(r=>r.url().includes('/api/profile/history?')&&new URL(r.url()).searchParams.get('coverage')==='partial');
  await page.locator('#profile-history-coverage').selectOption('partial',{force:true});
  await coverageRequest;
  assert.equal(await panel.locator('[data-timeline-filters-toggle]').textContent(),'Filters · 1');
  await clickRecordControl(panel,'[data-timeline-filters-toggle]');
  await clickRecordControl(panel,'[data-timeline-toggle]');
  await page.locator('#profile-history-timeline .cw-hist-bars').waitFor();
  assert.equal(await page.locator('#profile-history-timeline [data-timeline-month-jump]').count(),2);
  const zoom=page.locator('#profile-history-timeline .cw-hist-zoom-level');
  const initialZoom=await zoom.textContent();
  await page.locator('#profile-history-timeline [data-timeline-zoom="1"]').click();
  assert.notEqual(await zoom.textContent(),initialZoom);
  const monthRequest=page.waitForResponse(r=>r.url().includes('/api/profile/history?')&&new URL(r.url()).searchParams.get('month')==='2025-01');
  await page.locator('[data-timeline-month-jump="2025-01"]').click();
  await monthRequest;
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.profile.history')));
  assert.equal(saved.timeline,true);
  await snapshot(page,`hub-history-${width}`);
  const syncedAccent=await panel.evaluate(el=>getComputedStyle(el).getPropertyValue('--hist-tone-rgb'));
  await clickRecordControl(panel,'[data-timeline-source="scrobble"]');
  await panel.locator('.cw-hist-route').first().waitFor();
  assert.equal(await panel.locator('[data-timeline-filters-toggle]').isVisible(),false);
  assert.equal(await page.locator('#profile-history-filters').isVisible(),false);
  assert.notEqual(await panel.evaluate(el=>getComputedStyle(el).getPropertyValue('--hist-tone-rgb')),syncedAccent);
  assert.match(await panel.locator('[data-hub-route-actions]').first().getAttribute('aria-label'),/Sent to 1/);
  assert.doesNotMatch(await panel.locator('.cw-hist-route').first().textContent(),/Sent to/);
  await panel.locator('[data-hub-route-actions]').first().click();
  await page.getByRole('menuitem',{name:'View media details'}).click();
  assert.equal(await page.evaluate(()=>window.__openedMedia.title),items[0].title);
  await clickRecordControl(panel,'[data-timeline-view="list"]');
  await panel.locator('.cw-hist-row').first().waitFor();
  assert.equal(await panel.locator('.cw-hist-cell--when').first().isVisible(),true);
  assert.equal(await panel.locator('.cw-hist-cell--title strong').first().evaluate(el=>getComputedStyle(el).fontWeight),'500');
  await snapshot(page,`hub-history-list-${width}`);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await page.reload();
  await page.locator('#profile-history-timeline .cw-hist-bars').waitFor();
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.profile.history')).zoom),saved.zoom);
  state.empty=true;
  await clickRecordControl(panel,'#profile-history-refresh');
  await panel.locator('.cw-profile-empty').waitFor();
  assert.equal(await page.locator('#profile-history-timeline').isVisible(),false);
});

for(const width of [390,1440]) test(`Hub library pages preserve filters, sorting and timelines at ${width}px`,async t=>{
  const {page,state}=await fixture(t,'flat-dark',width,{library:true});
  await page.evaluate(()=>{
    window.CW.ItemActions={openMenu:args=>window.__providerAction=args.feature};
    window.CW.ProfileMediaModal={canOpen:()=>true,open:item=>window.__openedMedia=item};
  });
  for(const key of ['watchlist','ratings','collection']) {
    state.empty=false;
    if(width<=760) await page.locator(`[data-profile-tab="${key}"]`).click();
    else await page.locator(`[data-profile-tab="${key}"]`).click();
    const panel=page.locator(`#profile-panel-${key}`), collection=key==='collection';
    await panel.locator(collection?'.cw-collection-card':'.cw-hist-card').first().waitFor();
    assert.equal(await panel.locator('.cw-hub-records-stat').count(),4);
    assert.equal(await panel.locator('.cw-collection-tile').count(),0);
    assert.equal(await page.locator(`#profile-${key}-timeline`).isVisible(),false);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
    await snapshot(page,`hub-${key}-${width}`);
    const card=panel.locator(collection?'.cw-collection-card':'.cw-hist-card').first();
    const selectBounds=await card.locator('.cw-tl-select').boundingBox();
    const cardBounds=await card.boundingBox();
    assert.ok(selectBounds.x<cardBounds.x+cardBounds.width/2);
    assert.equal(await card.locator('.cw-card-actions').isVisible(),true);
    if(collection) {
      await card.locator('[data-collection-actions]').click();
      await page.getByRole('menuitem',{name:'Manage providers'}).click();
      assert.equal(await page.evaluate(()=>window.__providerAction),'collection');
      assert.equal(await panel.locator('.cw-collection-quality img[alt="4K"]').count(),items.length);
      await openRecordTools(panel);
      const sortRequest=page.waitForResponse(r=>r.url().includes('/api/profile/collection?')&&new URL(r.url()).searchParams.get('sort')==='title');
      await page.locator('#profile-collection-sort').selectOption('title',{force:true});
      await sortRequest;
      await page.locator('#profile-collection-sort').selectOption('collected_at',{force:true});
      if(width<=760) await panel.locator('.cw-hub-mobile-sheet').getByRole('button',{name:'Done',exact:true}).click();
    } else {
      await panel.locator('[data-ia-chip]').first().click();
      assert.equal(await page.evaluate(()=>window.__providerAction),key);
      await clickRecordControl(panel,'[data-timeline-filters-toggle]');
      const coverageRequest=page.waitForResponse(r=>r.url().includes(`/api/profile/${key}?`)&&new URL(r.url()).searchParams.get('coverage')==='partial');
      await page.locator(`#profile-${key}-coverage`).selectOption('partial',{force:true});
      await coverageRequest;
      await clickRecordControl(panel,'[data-timeline-filters-toggle]');
      if(key==='ratings') {
        assert.equal(await panel.locator('[data-timeline-rating="10"]').isDisabled(),true);
        const ratingRequest=page.waitForResponse(r=>r.url().includes('/api/profile/ratings?')&&new URL(r.url()).searchParams.get('rating')==='8.5');
        await clickRecordControl(panel,'[data-timeline-rating="8.5"]');
        await ratingRequest;
        assert.equal(await panel.locator('[data-timeline-rating="8.5"]').getAttribute('aria-pressed'),'true');
      }
    }
    if(key!=='watchlist') {
      await clickRecordControl(panel,collection?'[data-collection-timeline]':'[data-timeline-toggle]');
      await page.locator(`#profile-${key}-timeline .cw-hist-bars`).waitFor();
      assert.equal(await page.locator(`#profile-${key}-timeline [data-timeline-month-jump]`).count(),2);
      assert.equal(await page.evaluate(key=>JSON.parse(localStorage.getItem(`cw.profile.${key}`)).timeline,key),true);
    }
    await clickRecordControl(panel,collection?'[data-collection-view="list"]':'[data-timeline-view="list"]');
    await panel.locator(collection?'.cw-collection-row:not(.cw-collection-row--head)':'.cw-hist-row').first().waitFor();
    if(key==='watchlist') assert.equal(await panel.locator('.cw-hist-cell--when').first().isVisible(),false);
    await snapshot(page,`hub-${key}-list-${width}`);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
    state.empty=true;
    await clickRecordControl(panel,`#profile-${key}-refresh`);
    await panel.locator('.cw-profile-empty').waitFor();
  }
});

for (const width of [390,1440]) test(`Hub shared navigation order and icons at ${width}px`, async t => {
  const {page} = await fixture(t, 'flat-dark', width);
  const tabs = page.locator('.cw-profile-tabs [data-profile-tab]:not([data-profile-tab="account"])');
  assert.deepEqual(await tabs.evaluateAll(nodes => nodes.map(node => node.dataset.profileTab)), ['overview','watchlist','history','ratings','playback','collection']);
  assert.deepEqual(await tabs.locator('[data-tab-label]').allTextContents(), ['Overview','Watchlist','History','Ratings','Progress','Collection']);
  assert.deepEqual(await tabs.locator('.material-symbols-rounded').allTextContents(), ['space_dashboard','bookmark','history','star','play_circle','video_library']);
  if (width > 760) {
    const style = await tabs.first().locator('.material-symbols-rounded').evaluate(el => ({size:getComputedStyle(el).fontSize,axes:getComputedStyle(el).fontVariationSettings}));
    assert.equal(style.size, '19px');
    assert.match(style.axes, /"FILL" 0/);
  }
  await page.locator('[data-profile-tab="watchlist"]').click();
  await page.locator('#profile-panel-watchlist.active').waitFor();
  await mkdir('.tmp/shared-navigation', {recursive:true});
  await page.locator('.cw-profile-tabs').screenshot({path:`.tmp/shared-navigation/hub-${width}.png`});
});

for (const width of [390,1440]) test(`Account menu opens profile Settings and preserves media tabs at ${width}px`, async t => {
  const {page} = await fixture(t,'flat-dark',width);
  await page.locator('#cw-nav-profile-link').click();
  await page.locator('[data-cw-profile-menu-action="account"]').click();
  await page.locator('#profile-panel-account.active').waitFor();
  assert.equal(new URL(page.url()).hash, '#account');
  await page.locator('#cw-nav-profile-link').click();
  assert.equal(await page.locator('[data-cw-profile-menu-action="account"]').getAttribute('aria-current'), 'page');
  await page.keyboard.press('Escape');
  await page.locator('[data-profile-tab="ratings"]').click();
  await page.locator('#profile-panel-ratings.active').waitFor();
  if (width <= 760) await page.locator('[data-profile-tab="history"]').click();
  else await page.locator('[data-profile-tab="history"]').click();
  await page.locator('#cw-nav-profile-link').click();
  assert.equal(await page.locator('#cw-profile-menu [aria-current="page"]').count(), 0);
  await page.keyboard.press('Escape');
  if (width <= 760) await page.locator('[data-mobile-route="profile/overview"]').click();
  else await page.locator('#tab-profile').click();
  await page.locator('#profile-panel-overview.active').waitFor();
  assert.equal(new URL(page.url()).hash, '#overview');
});

test('My Media Hub shortcut returns to Overview when the profile tab changed without a hash change', async t => {
  const {page} = await fixture(t,'flat-dark',390);
  await page.evaluate(()=>{
    localStorage.setItem('cw.mobile.shortcuts.v1',JSON.stringify(['profile/overview','settings/sync']));
    history.replaceState(null,'','/profile#overview');
  });
  await page.reload();
  const hub = page.locator('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]');
  await page.locator('#profile-panel-overview.active').waitFor();
  assert.equal(await hub.getAttribute('aria-current'), 'page');
  await page.locator('[data-profile-tab="history"]').click();
  await page.locator('#profile-panel-history.active').waitFor();
  assert.equal(new URL(page.url()).hash, '#overview');
  await page.waitForFunction(()=>!document.querySelector('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]').hasAttribute('aria-current'));
  await hub.focus();
  await page.keyboard.press('Enter');
  await page.locator('#profile-panel-overview.active').waitFor();
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]').getAttribute('aria-current')==='page');
  await page.reload();
  await page.locator('#profile-panel-overview.active').waitFor();
  assert.equal(await hub.getAttribute('aria-current'), 'page');
});

for(const theme of ['original','flat-dark','flat-light']) {
  for(const [width, height] of [[320,740],[390,844],[740,360],[844,390],[932,430],[768,1024],[1024,768],[1194,834],[1366,1024],[1440,900]]) {
    test(`${theme}: profile covers and controls at ${width}x${height}`,async t=>{
      const {page,state}=await fixture(t,theme,width);
      await page.setViewportSize({width, height});
      for(const section of ['overview','collection','history','ratings','watchlist','playback','account']) {
        if(width<=760) await page.locator(`[data-profile-tab="${section}"]`).click();
        else await page.locator(`[data-profile-tab="${section}"]`).click();
        const panel=page.locator(`#profile-panel-${section}`);
        await panel.waitFor();
        if(['collection','history','ratings','watchlist','playback'].includes(section)) {
          const first=panel.locator('.cw-collection-card,.cw-hist-card').first();
          await first.waitFor();
          await first.scrollIntoViewIfNeeded();
          await page.waitForFunction(()=>{const img=document.querySelector('.cw-profile-panel.active .cw-hist-art img');return !img||img.classList.contains('is-loaded');});
        }
        await snapshot(page,`${theme}-${width}-${section}`);
        assert.equal(await page.evaluate(w=>document.documentElement.scrollWidth>w+1,width),false,section);
        if(width<=760&&section==='overview') {
          assert.ok((await panel.locator('.cw-cw-card').first().boundingBox()).width>=180);
          const cover=await panel.locator('.cw-cw-art').first().boundingBox();
          const badge=await panel.locator('.cw-cw-providers .cw-profile-provider-badge').first().boundingBox();
          assert.ok(badge.width<=22&&badge.height<cover.height/4);
          const poster=await panel.locator('.cw-hub-poster-art').first().boundingBox();
          const title=await panel.locator('.cw-hub-poster-title').first().boundingBox();
          assert.ok(poster.y+poster.height<=title.y);
        }
        if(width<=760&&['collection','history','ratings','watchlist','playback'].includes(section)) {
          const select=panel.locator('.cw-tl-select').first();
          assert.ok((await select.boundingBox()).width>=44);
          assert.equal(await select.evaluate(el=>getComputedStyle(el,'::before').width),'22px');
          await select.tap();
          assert.equal(await select.locator('input').isChecked(),true);
          await snapshot(page,`${theme}-${width}-${section}-selected`);
          await select.tap();
          if(section==='ratings') {
            const box=await select.boundingBox();
            const rating=await panel.locator('.cw-media-card-score').first().boundingBox();
            assert.ok(box.x+box.width<=rating.x);
          }
          if(section==='playback') {
            await panel.locator('[data-playback-menu]').first().tap();
            await page.locator('.cw-card-action-menu [data-playback-action="update_progress"]').tap();
            await page.locator('#profile-playback-edit[open]').waitFor();
            const dialog=await page.locator('#profile-playback-edit').boundingBox();
            assert.ok(dialog.x>=0&&dialog.x+dialog.width<=width&&dialog.y>=0&&dialog.y+dialog.height<=height+1,JSON.stringify(dialog));
            if(process.env.CW_PROFILE_SCREENSHOTS) await page.screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/${theme}-${width}-dialog.png`});
            await page.locator('#profile-playback-edit [data-playback-dialog-cancel]').tap();
          }
          const list=panel.locator('[data-timeline-view="list"],[data-playback-view="list"],[data-collection-view="list"]');
          if(await list.count()) {
            await openRecordTools(panel);
            await list.tap();
            await panel.locator('.cw-hist-row,.cw-collection-row').first().waitFor();
            assert.equal(await page.evaluate(w=>document.documentElement.scrollWidth>w+1,width),false,`${section} list`);
          }
        }
      }
      state.empty=true;
      if(width<=760) await page.locator('[data-profile-tab="watchlist"]').click();
      else await page.locator('[data-profile-tab="watchlist"]').click();
      await clickRecordControl(page.locator('#profile-panel-watchlist'),'#profile-watchlist-refresh');
      await page.locator('#profile-watchlist-list .cw-profile-empty').waitFor();
      await snapshot(page,`${theme}-${width}-empty`);
    });
  }
}


for (const width of [390,1440,2250]) test(`Hub Overview keeps provider actions and bounded previews at ${width}px`, async t => {
  const {page,state} = await fixture(t,'flat-dark',width,{count:12,activity:true});
  await page.waitForFunction(()=>document.querySelectorAll('#profile-watchlist .cw-hub-poster').length===4);
  await page.waitForFunction(expected=>document.querySelectorAll('#profile-progress .cw-cw-card').length===expected,width<=760?3:4);
  assert.equal(await page.locator('#profile-activity .cw-profile-activity-row').count(),width<=760?3:4);
  assert.equal(await page.locator('#profile-activity .cw-profile-activity-row>.material-symbols-rounded').count(),0);
  assert.equal(await page.locator('#profile-activity .cw-profile-activity-row strong').first().innerText(),'Watchlist sync');
  assert.match(await page.locator('#profile-activity .cw-profile-activity-row').nth(1).innerText(),/History sync.*Needs attention/s);
  assert.match(await page.locator('#profile-activity .cw-profile-activity-meta').first().innerText(),/Trakt.*SIMKL.*CrossWatch/s);
  assert.equal(await page.locator('#profile-activity .cw-profile-activity-schedule').count(),1);
  assert.doesNotMatch(await page.locator('#profile-activity').innerText(),/every.n.hours|Scheduler running/);
  assert.equal(await page.locator('#profile-activity .cw-profile-activity-row').first().evaluate(el=>el.querySelector('small').getBoundingClientRect().right<el.querySelector('strong').getBoundingClientRect().left),true);
  await page.evaluate(()=>{window.openEvents=props=>{window.__activityEvent=props;};});
  await page.locator('#profile-activity .cw-profile-activity-row').first().click();
  assert.equal(await page.evaluate(()=>window.__activityEvent.groupId),'1');
  assert.equal(await page.locator('#profile-progress [role="progressbar"]').first().getAttribute('aria-valuenow'),'45');
  assert.equal(await page.locator('#profile-watchlist .cw-hub-providers').first().locator('[data-provider]').count(),2);
  assert.match(await page.locator('.cw-hub-poster-art img').first().getAttribute('src'),/kind=poster/);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  const progress=await page.locator('.cw-hub-continue').boundingBox();
  const watchlist=await page.locator('#profile-watchlist').boundingBox();
  assert.ok(watchlist.y>=progress.y+progress.height);
  if(width>760) {
    const reference=await page.evaluate(()=>{
      const tracker=document.createElement('div');
      tracker.id='page-tracker';
      tracker.style.cssText=`position:absolute;visibility:hidden;padding:0;border:0;width:${document.querySelector('#profile-progress').getBoundingClientRect().width}px`;
      tracker.innerHTML='<div class="trk-overview-grid">'+Array.from({length:4},()=>'<article class="trk-overview-card"><div class="trk-overview-art"></div></article>').join('')+'</div>';
      tracker.firstElementChild.style.width=`${document.querySelector('#profile-progress').getBoundingClientRect().width}px`;
      document.body.append(tracker);
      const {width,height}=tracker.querySelector('.trk-overview-art').getBoundingClientRect();
      tracker.remove();
      return {width,height};
    });
    const hubCard=await page.locator('.cw-cw-card').first().boundingBox();
    assert.ok(Math.abs(hubCard.width-reference.width)<=2,JSON.stringify({hubCard,reference}));
    assert.ok(Math.abs(hubCard.height-reference.height)<=2,JSON.stringify({hubCard,reference}));
    const activity=await page.locator('#profile-activity').boundingBox();
    assert.ok(Math.abs(watchlist.y-activity.y)<2);
    assert.ok(activity.height<=watchlist.height+40,JSON.stringify({activity,watchlist}));
  }
  await page.evaluate(()=>{window.CW.ProfileMediaModal={canOpen:()=>true,open:item=>window.__openedMedia=item};});
  await page.locator('.cw-hub-poster').first().click();
  assert.equal(await page.evaluate(()=>window.__openedMedia.title),'Movie 1');
  await page.locator('.cw-cw-card').first().click();
  assert.equal(await page.evaluate(()=>window.__openedMedia.progress),45);
  assert.equal(await page.locator('.cw-cw-title').first().evaluate(el=>getComputedStyle(el).fontWeight),'500');
  assert.equal(await page.locator('.cw-profile-activity-row strong').first().evaluate(el=>getComputedStyle(el).fontWeight),'500');
  const avatar=page.getByRole('link',{name:'Open user settings'});
  assert.equal(await avatar.getAttribute('href'),'/profile#account');
  await avatar.click();
  await page.locator('#profile-panel-account.active').waitFor();
  if(width<=760) await page.locator('[data-profile-tab="overview"]').click();
  else await page.locator('[data-profile-tab="overview"]').click();
  await avatar.focus();
  await page.keyboard.press('Enter');
  await page.locator('#profile-panel-account.active').waitFor();
  if(width<=760) await page.locator('[data-profile-tab="overview"]').click();
  else await page.locator('[data-profile-tab="overview"]').click();
  if(process.env.CW_PROFILE_SCREENSHOTS) {
    await mkdir(process.env.CW_PROFILE_SCREENSHOTS,{recursive:true});
    await page.locator('#profile-panel-overview img').evaluateAll(images=>Promise.all(images.map(img=>{img.loading='eager';return img.decode().catch(()=>{});})));
    await page.evaluate(()=>window.scrollTo(0,0));
    await page.screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/hub-overview-${width}.png`,fullPage:true});
    await page.locator("#profile-activity").locator("..").screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/hub-activity-${width}.png`});
  }
  state.empty=true;
  await page.evaluate(()=>{localStorage.clear();sessionStorage.clear();});
  await page.reload();
  await page.locator('#profile-watchlist .cw-profile-empty').waitFor();
  assert.equal(await page.locator('#profile-progress .cw-cw-card').count(),0);
  assert.equal(await page.locator('#profile-activity .cw-profile-activity-row').count(),0);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
});

test("Media Hub uses compact mobile previews and independent paged lists", async t => {
  const {page,state} = await fixture(t,'flat-dark',390,{count:55});
  await page.waitForFunction(()=>document.querySelectorAll('#profile-progress .cw-cw-card').length===3);
  assert.equal(await page.locator('#profile-progress').evaluate(el=>el.scrollWidth>el.clientWidth),false);
  const sections = [['history','/api/profile/history',48],['ratings','/api/profile/ratings',48],['watchlist','/api/profile/watchlist',48],['collection','/api/profile/collection',24],['playback','/api/playback_progress/items',48]];
  for (const [section,path,desktopSize] of sections) {
    const requested = page.waitForResponse(response=>new URL(response.url()).pathname===path && new URL(response.url()).searchParams.get('page_size')==='20');
    await page.locator(`[data-profile-tab="${section}"]`).click();
    await requested;
    const selector = page.locator(`#profile-${section}-page-size`);
    assert.equal(await selector.inputValue(),'20');
    const changed = page.waitForResponse(response=>new URL(response.url()).pathname===path && new URL(response.url()).searchParams.get('page_size')==='10');
    await selector.selectOption('10',{force:true});
    await changed;
    const saved = await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),`cw.profile.${section}`);
    assert.equal(saved.mobilePageSize,10);
    assert.equal(saved.pageSize,desktopSize);
  }
  const unrelatedReads = state.queries.filter(query=>sections.slice(0,4).some(([,path])=>path===query.path)).length;
  const desktop = page.waitForResponse(response=>new URL(response.url()).pathname==='/api/playback_progress/items' && new URL(response.url()).searchParams.get('page_size')==='48');
  await page.setViewportSize({width:1440,height:900});
  await desktop;
  assert.equal(await page.locator('#profile-playback-page-size').inputValue(),'48');
  assert.equal(state.queries.filter(query=>sections.slice(0,4).some(([,path])=>path===query.path)).length,unrelatedReads);
  await page.setViewportSize({width:390,height:844});
  await page.reload();
  await page.locator('[data-profile-tab="playback"]').click();
  await page.locator('#profile-panel-playback.active').waitFor();
  assert.equal(await page.locator('#profile-playback-page-size').inputValue(),'10');
});

for(const width of [390,1440]) test(`Hub card actions and provider overflow remain usable at ${width}px`,async t=>{
  const {page,state}=await fixture(t,'flat-dark',width,{history:true,playback:true});
  state.providerRefs=['PLEX','TRAKT','SIMKL','MDBLIST','CROSSWATCH','JELLYFIN','EMBY','KODI','TOFA','SILO'].map(provider=>({provider,instance:'default'}));
  await page.evaluate(()=>{
    window.CW.ItemActions={openMenu:args=>{window.__providerAction=args.focus;}};
    window.CW.ProfileMediaModal={canOpen:()=>true,open:item=>window.__openedMedia=item};
  });
  if(width<=760) await page.locator('[data-profile-tab="history"]').click();
  else await page.locator('[data-profile-tab="history"]').click();
  const card=page.locator('#profile-history-list .cw-hist-card').first();
  await card.waitFor();
  const strip=card.locator('.cw-provider-strip'),more=strip.locator('[data-provider-overflow]');
  await more.waitFor();
  const shown=await strip.locator('[data-ia-chip]:visible').count();
  assert.ok(shown<10);
  assert.equal(await more.innerText(),`+${10-shown}`);
  const bounds=await strip.boundingBox();
  for(const chip of await strip.locator(':scope > :visible').all()) {
    const box=await chip.boundingBox();
    assert.ok(box.x+box.width<=bounds.x+bounds.width+1);
    assert.ok(box.y>=bounds.y && box.y+box.height<=bounds.y+bounds.height+1);
  }
  await more.click();
  assert.equal(await page.getByRole('menuitem').count(),10-shown);
  await page.keyboard.press('End');
  assert.match(await page.locator(':focus').innerText(),/Silo/i);
  await page.keyboard.press('Enter');
  assert.equal(await page.evaluate(()=>window.__providerAction.provider),'SILO');
  assert.equal(await page.locator('.cw-card-action-menu:popover-open').count(),0);
  if(width===1440) {
    await card.evaluate(el=>el.style.width='600px');
    await more.waitFor({state:'hidden'});
    assert.equal(await strip.locator('[data-ia-chip]:visible').count(),10);
    await card.evaluate(el=>el.style.width='');
    await more.waitFor();
  }
  await card.locator('[data-ia-cover]').click();
  assert.equal(await page.evaluate(()=>window.__providerAction),null);
  if(width<=760) await page.locator('[data-profile-tab="playback"]').click();
  else await page.locator('[data-profile-tab="playback"]').click();
  const progress=page.locator('#profile-panel-playback [data-playback-menu]').first();
  await progress.click();
  assert.equal(await page.getByRole('menuitem',{name:'Edit progress'}).count(),1);
  assert.equal(await page.getByRole('menuitem',{name:'Mark as watched'}).count(),1);
  assert.equal(await page.getByRole('menuitem',{name:'Remove resume point'}).count(),1);
  const menu=await page.locator('.cw-card-action-menu:popover-open').boundingBox();
  assert.ok(menu.x>=0 && menu.x+menu.width<=width);
  if(process.env.CW_PROFILE_SCREENSHOTS) await page.screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/card-actions-open-${width}.png`});
  await page.keyboard.press('Escape');
  assert.equal(await progress.getAttribute('aria-expanded'),'false');
  await page.evaluate(()=>{document.documentElement.dataset.cwRole='user';document.documentElement.dataset.cwPermWrite='off';});
  await progress.click();
  assert.equal(await page.getByRole('menuitem',{name:'Edit progress'}).count(),0);
  await page.getByRole('menuitem',{name:'View media details'}).click();
  assert.equal(await page.evaluate(()=>window.__openedMedia.title),items[0].title);
});

test('Mobile card action sheets dismiss without navigating or changing playback',async t=>{
  const {page}=await fixture(t,'flat-dark',390,{playback:true});
  await page.locator('[data-profile-tab="playback"]').click();
  const trigger=page.locator('#profile-panel-playback [data-playback-menu]').first();
  const sheet=page.locator('.cw-card-action-menu:popover-open');
  const url=page.url();
  await trigger.click();
  await sheet.waitFor();
  assert.equal(await sheet.getAttribute('role'),'dialog');
  assert.equal(await sheet.locator('h2').innerText(),items[0].title);
  const bounds=await sheet.boundingBox();
  const grip=await sheet.locator('.cw-card-action-grip').boundingBox();
  const title=await sheet.locator('h2').boundingBox();
  assert.ok(Math.abs(grip.x+grip.width/2-195)<1);
  assert.ok(title.y>grip.y+grip.height && title.x<32);
  assert.equal(bounds.x,0);
  assert.equal(bounds.width,390);
  assert.equal(Math.round(bounds.y+bounds.height),844);
  assert.equal(await page.evaluate(()=>document.documentElement.classList.contains('cw-card-sheet-open')),true);
  await page.keyboard.press('End');
  await page.keyboard.press('Tab');
  assert.match(await page.locator(':focus').innerText(),/View media details/);
  if(process.env.CW_PROFILE_SCREENSHOTS) {
    await mkdir(process.env.CW_PROFILE_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/mobile-actions-sheet.png`});
  }
  await page.goBack();
  await sheet.waitFor({state:'hidden'});
  assert.equal(page.url(),url);
  assert.equal(await trigger.evaluate(el=>el===document.activeElement),true);
  assert.equal(await page.evaluate(()=>document.documentElement.classList.contains('cw-card-sheet-open')),false);
  await trigger.click();
  await page.touchscreen.tap(8,100);
  await sheet.waitFor({state:'hidden'});
  await page.waitForFunction(()=>!history.state?.cwCardActions);
  assert.equal(page.url(),url);
  await trigger.click();
  const head=await sheet.locator('header').boundingBox();
  const cdp=await page.context().newCDPSession(page);
  await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:195,y:head.y+15}]});
  await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:195,y:head.y+95}]});
  await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
  await sheet.waitFor({state:'hidden'});
  await page.waitForFunction(()=>!history.state?.cwCardActions);
  assert.equal(await page.locator('body > [inert]').count(),0);
  assert.equal(page.url(),url);
});

test('Mobile Hub uses compact shelves, sticky tabs and two-column progress', async t => {
  const {page} = await fixture(t,'flat-dark',390,{count:12,playback:true});
  await page.locator('.cw-cw-card').first().waitFor();
  const art = await page.locator('.cw-cw-art').first().boundingBox();
  assert.ok(Math.abs(art.width - 240) < 3,JSON.stringify(art));
  assert.ok(Math.abs(art.height - 135) < 3);
  const copy = await page.locator('.cw-cw-copy').first().boundingBox();
  assert.ok(copy.y >= art.y + art.height);
  assert.ok(Math.abs((await page.locator('.cw-hub-poster').first().boundingBox()).width - 130) < 2);
  await mkdir('.tmp/mobile-implementation',{recursive:true});
  await page.screenshot({path:'.tmp/mobile-implementation/hub-overview.png'});
  await page.locator('[data-profile-tab="playback"]').click();
  await page.locator('#profile-panel-playback .cw-hist-card').first().waitFor();
  const cards = await page.locator('#profile-panel-playback .cw-hist-card').evaluateAll(els => els.slice(0,2).map(el => {const r=el.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width};}));
  assert.ok(Math.abs(cards[0].y - cards[1].y) < 2);
  assert.ok(cards[0].width < 190 && cards[0].width > 140);
  await page.evaluate(()=>window.scrollTo(0,200));
  const nav = await page.locator('.cw-profile-tabs').boundingBox();
  const header = await page.locator('body > header').boundingBox();
  assert.ok(Math.abs(nav.y - header.height) < 2, JSON.stringify({nav,header}));
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth > innerWidth),false);
  await page.screenshot({path:'.tmp/mobile-implementation/hub-progress.png'});
});


test('Hub shows observed live destinations without inventing resume actions', async t => {
  const {page}=await fixture(t,'flat-dark',1440,{playback:true,liveRoutes:true,activity:true});
  const icons=page.locator('.cw-cw-card').first().locator('.cw-cw-providers [data-provider]');
  await page.waitForFunction(()=>document.querySelectorAll('.cw-cw-card .cw-cw-providers [data-provider]').length===4);
  assert.deepEqual(await icons.evaluateAll(nodes=>nodes.map(node=>node.dataset.provider)),['plex','crosswatch','trakt','simkl']);
  assert.match(await page.locator('.cw-cw-card').first().locator('[data-provider="trakt"]').getAttribute('title'),/My Trakt: active watcher route/);
  await page.locator('[data-profile-tab="playback"]').click();
  const card=page.locator('#profile-panel-playback .cw-hist-card').first();
  await card.waitFor();
  const chips=card.locator('.cw-provider-strip > .cw-hist-provider:not(.cw-provider-more)');
  assert.equal(await chips.count(),4);
  assert.match(await chips.nth(2).getAttribute('title'),/My Trakt: active watcher route/);
  assert.equal(await chips.nth(2).evaluate(node=>node.tagName),'SPAN');
  await card.locator('[data-playback-menu]').click();
  await page.locator('.cw-card-action-menu [data-playback-action="update_progress"]').click();
  await page.locator('#profile-playback-edit[open]').waitFor();
  assert.match(await page.locator('#profile-playback-edit-sub').innerText(),/1 provider record/);
});
