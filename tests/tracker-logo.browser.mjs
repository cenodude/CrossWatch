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
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html ${attrs}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body><main id="layout"><section id="page-tracker" class="cw-themed-page"></section></main><script>const metadata=${JSON.stringify(metadata)};window.CW={AuthState:{read:()=>({isAdmin:true,user:{display_name:"Header user",username:"header"}})},OverviewProfile:{isAdmin:true,id:"",profiles:[{id:"family",label:"Family"}],instanceForProvider:()=>"",setActive:()=>{}},Meta:{peek:item=>metadata[item.ids?.tmdb]||null,get:async item=>metadata[item.ids?.tmdb]||null},ProfileDateTime:{format:date=>date.toISOString().slice(0,10)}};</script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/card-actions.js"></script><script src="/assets/js/profile-media-modal.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
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
  const notes=modal.locator('.cw-mm-stack>.cw-mm-box:last-child>.cw-mm-trk-notes');
  assert.equal(await notes.locator('[data-mm-trk="notes"]').count(), 1);
  assert.equal(await notes.locator('h4').count(), 0);
  assert.match(await notes.textContent(), /Add notes/);
  assert.equal(await modal.locator('.cw-mm-trk .cw-mm-trk-notes').count(), 0);
  assert.equal(await modal.locator('.cw-mm-trk-actions [data-mm-trk="notes"]').count(), 0);
  const actions = modal.locator('.cw-mm-trk-actions');
  assert.deepEqual(await actions.locator('button').evaluateAll(buttons => buttons.map(button => button.dataset.mmTrk)), ['log','rate','list','map','origin','remove']);
  assert.equal(await actions.locator('.is-icon').count(), 6);
  assert.equal(await actions.locator('[data-mm-trk="log"]').getAttribute('aria-label'), 'Watched');
  assert.equal(await actions.locator('[data-mm-trk="log"]').evaluate(el => [...el.childNodes].some(node => node.nodeType === Node.TEXT_NODE && node.textContent.trim())), false);
  await actions.locator('[data-mm-trk="origin"]').click();
  await actions.locator('[data-mm-trk="remove"]').click();
  assert.deepEqual(await page.evaluate(()=>mediaActions.splice(0)), [['origin',''],['remove','']]);
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
  await page.evaluate(()=>{mediaData.note.notes='Watch again with the family.';CW.ProfileMediaModal.updateTracker(mediaData);});
  await notes.locator('[aria-label="Edit notes"]').waitFor();
  assert.match(await notes.textContent(),/My notes.*Watch again with the family\./s);
  assert.deepEqual(await modal.locator('.cw-mm-stack h4').allTextContents(),['Watch history','Details','My notesedit_note']);
  await notes.locator('[data-mm-trk="notes"]').click();
  assert.deepEqual(await page.evaluate(()=>mediaActions.at(-1)),['notes','']);
  await modal.locator('.cw-mm-stack').screenshot({path:'.tmp/media-card/notes-populated.png'});
  await page.setViewportSize({width:390,height:844});
  await page.evaluate(()=>{mediaData.note.notes='';CW.ProfileMediaModal.updateTracker(mediaData);});
  await notes.getByRole('button',{name:'Add notes'}).waitFor();
  assert.equal(await notes.locator('h4').count(),0);
  assert.equal(await modal.locator('.cw-mm-stack').evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
  await modal.locator('.cw-mm-stack').screenshot({path:'.tmp/media-card/notes-empty-mobile.png'});
  await page.evaluate(()=>{mediaData.note_key=null;CW.ProfileMediaModal.updateTracker(mediaData);});
  await modal.locator('[data-mm-status-picker] summary').click();
  assert.equal(await modal.locator('[data-mm-trk-status]:disabled').count(),6);
  await page.evaluate(()=>CW.ProfileMediaModal.open({type:'movie',ids:{tmdb:1}}));
  assert.equal(await modal.locator('[data-mm-providers]').count(),0);
  assert.equal(await modal.locator('section.cw-mm-lives').count(),1);
});

for (const mode of ['Tracker','Media hub']) test(`${mode}: delayed media updates preserve mounted content`, async t => {
  const {page, release} = await fixture(t, 'flat-dark', 1440);
  release();
  let finishPresence, finishProgress;
  const presence = new Promise(resolve=>{finishPresence=resolve;});
  const progress = new Promise(resolve=>{finishProgress=resolve;});
  t.after(()=>{finishPresence();finishProgress();});
  await page.route('**/api/profile/title?**', async route=>{await presence;return route.fulfill({json:{ok:true,plays:[],synced:{count:0,episodes:[],present:[],missing:[]}}});});
  await page.route('**/api/playback_progress/items?**', async route=>{await progress;return route.fulfill({json:{items:[{provider:'PLEX',instance_id:'default',season:1,episode:2,progress_percent:66,remaining_seconds:1320}]}});});
  await page.route('**/api/metadata/tmdb/season?**', route=>route.fulfill({json:{ok:true,episodes:[1,2,3].map(episode=>({episode,name:`Episode ${episode}`,runtime:42}))}}));
  await page.evaluate(mode=>{
    metadata[3].detail.seasons=[{season:1,episode_count:3}];
    window.mediaData={title:{art:{tmdb:3}},note_key:'show:3',note:{status:'watching'},plays:[]};
    CW.Meta.get=()=>new Promise(resolve=>{window.finishMeta=()=>resolve({...metadata[3],overview:'Fresh description',tagline:'New metadata'});});
    window.opening=CW.ProfileMediaModal.open({type:'show',ids:{tmdb:3}},mode==='Tracker'?{tracker:{data:mediaData,onAction(){}}}:{});
  },mode);
  const modal=page.locator('#cw-media-modal');
  await modal.locator('.cw-mm-title-logo:visible').waitFor();
  await modal.locator('[data-mm-tab="overview"]').focus();
  await page.evaluate(()=>{
    const body=document.querySelector('.cw-mm-body');
    body.scrollTop=120;
    window.mounted={poster:body.querySelector('.cw-mm-poster'),logo:body.querySelector('.cw-mm-title-logo'),tab:document.activeElement,scroll:body.scrollTop};
  });
  await page.evaluate(()=>finishMeta());
  await page.waitForFunction(()=>document.querySelector('.cw-mm-overview')?.textContent==='Fresh description');
  assert.deepEqual(await page.evaluate(()=>({poster:mounted.poster===document.querySelector('.cw-mm-poster'),logo:mounted.logo===document.querySelector('.cw-mm-title-logo'),focus:document.activeElement===mounted.tab,scroll:document.querySelector('.cw-mm-body').scrollTop===mounted.scroll})),{poster:true,logo:true,focus:true,scroll:true});
  finishPresence();
  await modal.locator('[data-mm-season-toggle="1"]').click();
  await modal.locator('#cw-mm-season-detail-1 .cw-mm-season-episode').first().waitFor();
  if(mode==='Tracker')await modal.locator('[data-mm-status-picker] summary').click();
  await page.evaluate(()=>{
    window.seasonNode=document.querySelector('[data-mm-season-toggle="1"]');
    seasonNode.focus();
    window.resumeScroll=document.querySelector('.cw-mm-body').scrollTop;
  });
  finishProgress();
  await modal.locator('.cw-mm-resume').waitFor();
  await page.evaluate(()=>opening);
  assert.deepEqual(await page.evaluate(()=>({season:seasonNode===document.querySelector('[data-mm-season-toggle="1"]'),expanded:seasonNode.getAttribute('aria-expanded'),focus:document.activeElement===seasonNode,scroll:document.querySelector('.cw-mm-body').scrollTop===resumeScroll,logo:mounted.logo===document.querySelector('.cw-mm-title-logo')})),{season:true,expanded:'true',focus:true,scroll:true,logo:true});
  if(mode==='Tracker'){
    assert.equal(await modal.locator('[data-mm-status-picker]').evaluate(el=>el.open),true);
    await page.evaluate(()=>{
      window.refreshMutations=[];
      window.refreshObserver=new MutationObserver(rows=>refreshMutations.push(...rows));
      refreshObserver.observe(document.querySelector('.cw-mm-body'),{subtree:true,childList:true,attributes:true,characterData:true});
      CW.ProfileMediaModal.updateTracker(mediaData);
    });
    await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    assert.equal(await page.evaluate(()=>refreshMutations.length),0);
    await page.evaluate(()=>{refreshObserver.disconnect();mediaData.note.status='completed';CW.ProfileMediaModal.updateTracker(mediaData);});
    await page.waitForFunction(()=>document.querySelector('.cw-mm-trk .trk-status-btn')?.dataset.status==='completed');
    assert.equal(await modal.locator('[data-mm-status-picker]').evaluate(el=>el.open),true);
    await page.evaluate(()=>{mediaData.plays=[{key:'first',season:1,episode:1,date:'2026-11-01T12:00:00Z'}];CW.ProfileMediaModal.updateTracker(mediaData);});
    await modal.locator('[data-mm-trk-edit="first"]').waitFor();
    await page.evaluate(()=>{
      window.editPlay=document.querySelector('[data-mm-trk-edit="first"]');
      editPlay.focus();
      mediaData.plays.unshift({key:'second',season:1,episode:2,date:'2026-11-02T12:00:00Z'});
      CW.ProfileMediaModal.updateTracker(mediaData);
    });
    await modal.locator('[data-mm-trk-edit="second"]').waitFor();
    assert.equal(await page.evaluate(()=>document.activeElement===editPlay&&editPlay===document.querySelector('[data-mm-trk-edit="first"]')),true);
  }
});

test('Tracker resume points include providers and stay mounted during title updates', async t => {
  const {page,release}=await fixture(t,'flat-dark',1440);
  release();
  let providerRequests=0;
  await page.route('**/api/playback_progress/items?**',route=>{
    providerRequests++;
    return route.fulfill({json:{items:[
      {provider:'CROSSWATCH',provider_label:'CrossWatch',instance_id:'family',instance_label:'Family',season:2,episode:7,progress_percent:25},
      {provider:'PUBLICMETADB',instance_id:'default',season:2,episode:7,progress_percent:66,remaining_seconds:1320},
    ]}});
  });
  await page.evaluate(async()=>{
    window.localProgress={title:{art:{tmdb:3}},provider_instance:'family',plays:[],progress:[]};
    await CW.ProfileMediaModal.open({type:'show',ids:{tmdb:3}},{tracker:{label:'Family',data:localProgress,onAction(){}}});
  });
  const resume=page.locator('#cw-media-modal .cw-mm-resume');
  await resume.getByText('66%',{exact:true}).waitFor();
  assert.match(await resume.textContent(),/CrossWatch · Family.*25%.*PUBLICMETADB.*22m left.*66%/s);
  assert.equal(await resume.locator('.cw-mm-resume-episode').count(),1);
  assert.equal(providerRequests,1);
  await page.evaluate(()=>{
    window.resumeRow=document.querySelector('.cw-mm-resume .cw-mm-progress-row');
    localProgress.note={status:'watching'};
    CW.ProfileMediaModal.updateTracker(localProgress);
  });
  await page.waitForFunction(()=>document.querySelector('.cw-mm-trk .trk-status-btn')?.dataset.status==='watching');
  assert.equal(await page.evaluate(()=>resumeRow===document.querySelector('.cw-mm-resume .cw-mm-progress-row')),true);
  assert.match(await resume.textContent(),/PUBLICMETADB.*66%/s);
  assert.equal(providerRequests,1);
});

test('movie watch history uses the compact summary for Tracker and Media hub', async t => {
  const {page,release}=await fixture(t,'flat-dark',1440);
  release();
  await page.route('**/api/profile/title?**',route=>route.fulfill({json:{ok:true,synced:{count:1,last_epoch:1785369600}}}));
  await page.evaluate(async()=>{
    window.movieHistory={title:{art:{tmdb:1}},plays:[{key:'play:1',date:'2026-07-30T00:00:00Z'}]};
    await CW.ProfileMediaModal.open({type:'movie',ids:{tmdb:1}},{tracker:{data:movieHistory}});
  });
  const summary=page.locator('#cw-media-modal .cw-mm-watch-history');
  assert.match(await summary.textContent(),/Watch history.*Last watched.*2026-07-30.*Watched once/s);
  assert.equal(await summary.locator('.cw-mm-watchline-count').count(),0);
  assert.equal(await summary.evaluate(node=>getComputedStyle(node).borderTopWidth),'0px');
  await mkdir('.tmp/media-card',{recursive:true});
  await summary.screenshot({path:'.tmp/media-card/watch-history-desktop.png'});
  await page.evaluate(()=>{movieHistory.plays.push({key:'play:2',date:'2026-08-01T00:00:00Z'});CW.ProfileMediaModal.updateTracker(movieHistory);});
  await summary.locator('.cw-mm-watch-result').filter({hasText:'Watched 2 times'}).waitFor();
  await page.setViewportSize({width:390,height:844});
  await summary.scrollIntoViewIfNeeded();
  assert.equal(await summary.evaluate(node=>node.scrollWidth<=node.clientWidth),true);
  await summary.screenshot({path:'.tmp/media-card/watch-history-mobile.png'});
  await page.evaluate(()=>{movieHistory.plays=[];CW.ProfileMediaModal.updateTracker(movieHistory);});
  await summary.getByText('Not watched yet',{exact:true}).waitFor();
  assert.equal(await summary.locator('.material-symbols-rounded').count(),0);
  await page.evaluate(()=>CW.ProfileMediaModal.open({type:'movie',ids:{tmdb:1}}));
  await summary.locator('.cw-mm-watch-result').filter({hasText:'Watched once'}).waitFor();
  assert.equal(await summary.locator('.material-symbols-rounded').count(),1);
});

test('grouped resume points keep provider actions and fit a narrow media card', async t => {
  const {page,release}=await fixture(t,'flat-dark',1440);
  release();
  await page.route('**/api/playback_progress/items?**',route=>route.fulfill({json:{items:[
    {provider:'CROSSWATCH',instance_id:'default',season:1,episode:1,episode_title:'S01E01',progress_percent:65,remaining_seconds:1080},
    {provider:'PLEX',instance_id:'family',instance_label:'Family',season:1,episode:1,episode_title:'No Strings',progress_percent:42,remaining_seconds:1500,can_remove_progress:true},
    {provider:'TRAKT',instance_id:'default',season:1,episode:1,episode_title:'No Strings',progress_percent:60,remaining_seconds:1200},
  ]}}));
  await page.evaluate(async()=>{
    CW.ItemActions={canWrite:()=>true,openMenu:options=>{window.resumeAction={feature:options.feature,focusRow:options.focusRow,rows:options.progressRows.length,scopeRows:options.scope()[0].querySelectorAll('.cw-mm-progress-row').length};}};
    await CW.ProfileMediaModal.open({type:'show',ids:{tmdb:3}});
  });
  const resume=page.locator('#cw-media-modal .cw-mm-resume');
  assert.equal(await resume.locator('.cw-mm-resume-episode').count(),1);
  assert.equal(await resume.locator('.cw-mm-progress-row').count(),3);
  assert.equal(await resume.locator('h6').textContent(),'S1E1No Strings');
  assert.deepEqual(await resume.getByRole('progressbar').evaluateAll(nodes=>nodes.map(node=>node.getAttribute('aria-valuenow'))),['65','42','60']);
  const action=resume.locator('[data-ia-ref="PLEX:family"]');
  const rid=await action.getAttribute('data-ia-play');
  await action.click();
  assert.deepEqual(await page.evaluate(()=>resumeAction),{feature:'progress',focusRow:rid,rows:3,scopeRows:3});
  await mkdir('.tmp/media-card',{recursive:true});
  await resume.screenshot({path:'.tmp/media-card/resume-grouped-desktop.png'});
  await page.setViewportSize({width:390,height:844});
  await resume.scrollIntoViewIfNeeded();
  assert.equal(await resume.evaluate(node=>node.scrollWidth<=node.clientWidth),true);
  assert.equal(await resume.locator('.cw-mm-progress-row').evaluateAll(rows=>rows.every(row=>row.scrollWidth<=row.clientWidth)),true);
  await resume.screenshot({path:'.tmp/media-card/resume-grouped-mobile.png'});
});

test('media refresh retains failed artwork and resets title logo visibility on navigation', async t => {
  const {page,release}=await fixture(t,'flat-dark',1440);
  release();
  let posterRequests=0;
  await page.route('**/art/tmdb/movie/1?**',route=>{
    if(new URL(route.request().url()).searchParams.get('kind')==='logo')return route.fallback();
    posterRequests++;
    return route.fulfill({status:404,body:''});
  });
  await page.evaluate(async()=>{
    window.data={title:{art:{tmdb:1}},plays:[]};
    await CW.ProfileMediaModal.open({type:'movie',ids:{tmdb:1}},{tracker:{data,onAction(){}}});
  });
  const modal=page.locator('#cw-media-modal');
  await modal.locator('.cw-mm-title-logo:visible').waitFor();
  await page.waitForFunction(()=>document.querySelector('.cw-mm-poster')?.getAttribute('src').includes('placeholder_poster'));
  const previousRequests=posterRequests;
  await page.evaluate(()=>{window.failedPoster=document.querySelector('.cw-mm-poster');CW.ProfileMediaModal.updateTracker(data);});
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  assert.equal(await page.evaluate(()=>failedPoster===document.querySelector('.cw-mm-poster')&&failedPoster.getAttribute('src').includes('placeholder_poster')),true);
  assert.equal(posterRequests,previousRequests);
  await page.evaluate(()=>CW.ProfileMediaModal.open({type:'movie',ids:{tmdb:2},title:'Another title'}));
  await modal.locator('.cw-mm-title-text:visible').waitFor();
  assert.equal(await modal.locator('.cw-mm-title-text').textContent(),'Another title');
  assert.equal(await modal.locator('.cw-mm-title-logo').isVisible(),false);
});

for (const [mode, width] of [['Tracker',1440],['Media hub',390]]) test(`${mode}: compact season overview expands cached episodes`, async t => {
  const {page, release} = await fixture(t, 'flat-dark', width);
  release();
  const requests = new Map();
  const plays = [1,3].map(episode => ({season:4,episode,epoch:1794000000,date:'2026-11-07T12:00:00Z'}));
  await page.route('**/api/profile/title?**', route => route.fulfill({json:{ok:true,plays,synced:{count:2,episodes:plays,present:[],missing:[]}}}));
  await page.route('**/api/playback_progress/items?**', route => route.fulfill({json:{items:[{provider:'PLEX',instance_id:'default',season:4,episode:2,progress_percent:66,remaining_seconds:1320}]}}));
  await page.route('**/api/metadata/tmdb/season?**', route => {
    const season = Number(new URL(route.request().url()).searchParams.get('season'));
    requests.set(season,(requests.get(season)||0)+1);
    if(season===5 && requests.get(season)===1)return route.fulfill({json:{ok:false}});
    return route.fulfill({json:{ok:true,episodes:Array.from({length:5},(_,index)=>({episode:index+1,name:`Episode ${index+1}`,runtime:42,air_date:'2025-11-01'}))}});
  });
  await page.evaluate(async ({mode,plays}) => {
    metadata[3].detail.seasons = [1,2,3,4,5,0].map(season=>({season,episode_count:5,air_date:season?'2025-11-01':''}));
    window.openSeasonCard = () => CW.ProfileMediaModal.open({type:'show',ids:{tmdb:3}}, mode==='Tracker'?{tracker:{data:{plays},onAction(){}}}:{});
    await openSeasonCard();
  },{mode,plays});
  const modal = page.locator('#cw-media-modal');
  const seasons = modal.locator('.cw-mm-seasons-progress').filter({has:page.locator('[data-mm-seasons-all]')});
  assert.deepEqual(await seasons.locator('[data-mm-season-toggle]').evaluateAll(rows=>rows.map(row=>row.dataset.mmSeasonToggle)),['5','4','3']);
  assert.equal(requests.has(5),false);
  const current = seasons.locator('[data-mm-season-toggle="4"]');
  await current.focus();
  await page.keyboard.press('Enter');
  await seasons.locator('#cw-mm-season-detail-4 .cw-mm-season-episode').first().waitFor();
  assert.equal(await current.getAttribute('aria-expanded'),'true');
  assert.equal(await current.evaluate(el=>el===document.activeElement),true);
  assert.match(await current.textContent(),/2025.*5 episodes.*2\/5.*In progress/s);
  assert.match(await seasons.locator('#cw-mm-season-detail-4').textContent(),/Where you left off.*22m left.*66% watched/);
  assert.equal(await seasons.locator('#cw-mm-season-detail-4 .cw-mm-season-episode').count(),3);
  await seasons.locator('[data-mm-season-more]').click();
  assert.equal(await seasons.locator('#cw-mm-season-detail-4 .cw-mm-season-episode').count(),5);
  await seasons.locator('[data-mm-season-more]').click();
  await seasons.locator('[data-mm-episode="4:2"]').click();
  assert.equal(await modal.locator('#cw-mm-title').textContent(),'Episode 2');
  await modal.locator('[data-mm-series]').click();
  assert.equal(await current.getAttribute('aria-expanded'),'true');
  await current.click();
  await current.click();
  assert.equal(requests.get(4),1);
  await seasons.locator('[data-mm-seasons-all]').click();
  const picker = seasons.locator('[data-mm-season-picker]');
  assert.deepEqual(await picker.locator('option:not([disabled])').evaluateAll(options=>options.map(option=>option.value)),['5','4','3','2','1','0']);
  await picker.selectOption('0');
  await seasons.locator('#cw-mm-season-detail-0 .cw-mm-season-episode').first().waitFor();
  await seasons.locator('[data-mm-season-toggle="0"]').click();
  assert.equal(await seasons.locator('[data-mm-season-toggle="0"]').count(),1);
  await seasons.locator('[data-mm-season-toggle="5"]').click();
  await seasons.locator('[data-mm-season-retry="5"]').click();
  await seasons.locator('#cw-mm-season-detail-5 .cw-mm-season-episode').first().waitFor();
  assert.equal(requests.get(5),2);
  await current.click();
  await mkdir('.tmp/media-card',{recursive:true});
  const summary=modal.locator('.cw-mm-watch-summary');
  assert.match(await summary.textContent(),/16 episodes · 2 seasons.*2\/16 watched.*13%/s);
  assert.equal(await summary.locator('[role="progressbar"]').getAttribute('aria-valuenow'),'13');
  assert.equal(await summary.locator('.cw-mm-watch-meter').evaluate(el=>el.getBoundingClientRect().width<=160),true);
  assert.equal(await summary.evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
  await summary.screenshot({path:`.tmp/media-card/watch-summary-${mode.replace(' ','-').toLowerCase()}.png`});
  await seasons.screenshot({path:`.tmp/media-card/seasons-${mode.replace(' ','-').toLowerCase()}.png`});
  assert.deepEqual(await seasons.evaluate(el=>[el,...el.querySelectorAll('button,select')].filter(node=>node.scrollWidth>node.clientWidth+1).map(node=>({className:node.className,width:node.clientWidth,scrollWidth:node.scrollWidth}))),[]);
  await page.evaluate(()=>openSeasonCard());
  assert.equal(await modal.locator('[data-mm-season-toggle="4"]').getAttribute('aria-expanded'),'false');
  await modal.locator('[data-mm-season-toggle="4"]').click();
  await modal.locator('#cw-mm-season-detail-4 .cw-mm-season-episode').first().waitFor();
  assert.equal(requests.get(4),1);
});

for(const theme of ['original','flat-dark','flat-light']) for(const width of [320,768,1440]) {
  test(`${theme}: shared media card history at ${width}px`, async t => {
    const {page, release} = await fixture(t, theme, width);
    release();
    const plays = Array.from({length:7}, (_, i) => ({epoch:Date.parse(`2026-11-0${i+1}T20:29:00Z`)/1000, season:2, episode:i+1}));
    await page.route('**/api/profile/title?**', route => route.fulfill({json:{ok:true, plays, synced:{count:7, episodes:plays, present:[], missing:[]}}}));
    await page.evaluate(async () => {
      CW.Trailer = {has:() => true, openFor:item => {window.trailerItem=item;}};
      CW.ProfileDateTime.format = (date, options) => new Intl.DateTimeFormat('en-GB', {...options, timeZone:'UTC', hourCycle:'h23'}).format(date);
      await CW.ProfileMediaModal.open({type:'show', ids:{tmdb:3}});
    });
    const modal = page.locator('#cw-media-modal');
    const trailer = modal.locator('[data-mm-trailer]');
    assert.equal(await trailer.getAttribute('class'), 'cw-mm-btn');
    await trailer.click();
    assert.equal(await page.evaluate(() => trailerItem.ids.tmdb), 3);
    assert.equal(await modal.locator('.cw-mm-trk-play').count(), 5);
    assert.equal(await modal.locator('.cw-mm-trk-play b').first().textContent(), 'S2 E7');
    assert.match(await modal.locator('.cw-mm-trk-play').first().textContent(), /7 Nov 2026.*20:29/);
    assert.equal(await modal.locator('[data-mm-trk-edit], [data-mm-trk], [data-mm-status-picker]').count(), 0);
    assert.deepEqual(await modal.locator('.cw-mm-grid-2>.cw-mm-stack h4').allTextContents(), ['Watch history', 'Details']);
    await modal.locator('[data-mm-history-more]').focus();
    await page.keyboard.press('Enter');
    assert.equal(await modal.locator('.cw-mm-trk-play').count(), 7);
    assert.equal(await modal.locator('[data-mm-history-more]').getAttribute('aria-expanded'), 'true');
    await page.keyboard.press('Enter');
    assert.equal(await modal.locator('.cw-mm-trk-play').count(), 5);
    const overflow = await modal.evaluate(el => [...el.querySelectorAll('.cw-mm-body, .cw-mm-trk-play')].some(node => node.scrollWidth > node.clientWidth + 1));
    assert.equal(overflow, false);
    await mkdir('.tmp/media-card', {recursive:true});
    await modal.locator('.cw-mm-body').evaluate(el => {el.scrollTop=0;});
    await modal.screenshot({path:`.tmp/media-card/shared-${theme}-${width}.png`});
    if(width===320) {
      await modal.locator('.cw-mm-trk-plays').scrollIntoViewIfNeeded();
      await modal.screenshot({path:`.tmp/media-card/shared-${theme}-${width}-history.png`});
    }
    await page.route('**/api/profile/title?**', route => route.fulfill({json:{ok:true, plays:[]}}));
    await page.evaluate(() => CW.ProfileMediaModal.open({type:'movie', ids:{tmdb:1}}));
    assert.match(await modal.locator('.cw-mm-grid-2>.cw-mm-stack').textContent(), /No watch history yet/);
    assert.equal(await modal.locator('[data-mm-history-more]').count(), 0);
  });
}

for(const theme of ['original','flat-dark','flat-light']) for(const width of [320,390,1440]) {
  test(`${theme}: header logos and fallback at ${width}px`,async t=>{
    const {page,select,release,logoRequested}=await fixture(t,theme,width);
    const heading=page.locator('.trk-hero-heading');
    assert.equal(await page.locator('.cw-page-hero-title').count(),0);
    assert.equal(await page.locator('.cw-page-hero-kicker').textContent(),'CW TRACKER');
    assert.match(await page.locator('[data-trk-slot="identity"]').innerText(),/Header user/);
    assert.equal(await page.locator('[data-trk-slot="identity"]').getByRole('link',{name:'Open user settings'}).getAttribute('href'),'/profile#account');
    const profileBounds=await page.locator('.trk-profile-picker').boundingBox();
    const identityBounds=await page.locator('[data-trk-slot="identity"]').boundingBox();
    assert.ok(identityBounds.x+identityBounds.width<=profileBounds.x);
    await heading.locator('img').waitFor();
    assert.equal(await heading.locator('img').getAttribute('alt'),'Arrival');
    assert.equal(await page.locator('.trk-play.is-selected').count(),0);
    assert.equal(await page.locator('[data-trk-slot="title"] .trk-detail-empty').count(),1);
    assert.equal(await page.locator('[data-trk-slot="title"] .trk-detail-empty').isVisible(),width>760);
    if(process.env.CW_LOGO_SCREENSHOTS) {
      await mkdir(process.env.CW_LOGO_SCREENSHOTS,{recursive:true});
      await page.locator('.cw-page-hero').screenshot({path:`${process.env.CW_LOGO_SCREENSHOTS}/${theme}-${width}-default.png`});
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
    if(width<=760) assert.equal(await page.locator('.trk-hero-meta').innerText(),'2016 · Movie');
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
    if(width<=760) {
      assert.equal(episodeTitle,null);
      assert.equal(await page.locator('.trk-hero-meta').innerText(),'2022 · TV Series');
    } else assert.ok(episodeTitle.y>=episodeMeta.y-2);
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
    const infoButton=page.locator('.trk-grid-card [data-trk-card-open]').nth(2);
    assert.equal(await infoButton.getAttribute('aria-label'),'Open details for House of the Dragon');
    assert.equal(await infoButton.evaluate(el=>!!el.closest('.trk-grid-art')),true);
    assert.equal(await page.locator('.trk-grid-copy [data-trk-card-menu]').count(),3);
    assert.equal(await page.locator('.trk-cover-action').count(),0);
    await infoButton.focus();
    const infoBounds=await infoButton.boundingBox();
    const minimumTarget = width < 800 ? 44 : 40;
    assert.ok(infoBounds.width>=minimumTarget&&infoBounds.height>=minimumTarget);
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
    await page.waitForFunction(()=>document.querySelector('.cw-mm-trk .trk-status-btn')?.dataset.status==='on_hold');
    assert.equal(await modal.locator('.cw-mm-trk .trk-status-btn').getAttribute('data-status'),'on_hold');
    await page.evaluate(()=>CW.ProfileMediaModal.open({type:'movie',title:'Missing logo',ids:{tmdb:'2'}}));
    await page.waitForFunction(()=>{const image=document.querySelector('.cw-mm-title-logo');return image?.complete&&!image.naturalWidth;});
    assert.equal(await modal.locator('.cw-mm-title-text').isVisible(),true);
    assert.equal(await modal.locator('.cw-mm-chips .cw-mm-trk-status').count(),0);
    assert.equal(await page.evaluate(()=>{const body=document.querySelector('.cw-mm-body');return body.scrollWidth>body.clientWidth+1;}),false);
  });
}


test('Tracker header follows the newest loaded result without selecting it', async t => {
  const {page,select,release} = await fixture(t,'flat-dark',1440);
  release();
  const titleReads = [];
  page.on('request', request => {if (new URL(request.url()).pathname === '/api/tracker/title') titleReads.push(request.url());});
  await page.route('**/api/tracker/items?**', route => {
    const query = new URL(route.request().url()).searchParams.get('q');
    const list = query === 'empty' ? [] : [{...items[1],date:'2026-11-05T12:00:00Z'},{...items[2],date:'2026-11-09T12:00:00Z'},items[0]];
    return route.fulfill({json:{items:list,total:list.length}});
  });
  await page.locator('[data-trk-input="q"]').fill('latest');
  await page.locator('.trk-hero-logo[alt="House of the Dragon"]').waitFor();
  assert.match(await page.locator('.trk-hero-backdrop').getAttribute('src'),/tv\/3/);
  assert.equal(await page.locator('.trk-hero-details .trk-status').getAttribute('data-status'),'watching');
  assert.equal(await page.locator('.trk-play.is-selected').count(),0);
  assert.equal(await page.locator('.trk-title-sheet').count(),0);
  assert.equal(titleReads.length,0);
  await page.locator('[data-trk-input="q"]').fill('empty');
  await page.getByText('Everything you watched, rated and want to watch',{exact:true}).waitFor();
  assert.equal(await page.locator('.trk-hero-backdrop').isHidden(),true);
  await page.locator('[data-trk-input="q"]').fill('latest');
  await page.locator('.trk-hero-logo[alt="House of the Dragon"]').waitFor();
  await select(2);
  await page.locator('.trk-hero-logo[alt="Arrival"]').waitFor();
  assert.equal(await page.locator('.trk-play.is-selected').count(),1);
  assert.equal(titleReads.length,1);
});
