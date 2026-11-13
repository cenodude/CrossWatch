/* tests/tracker-overview.browser.mjs */
/* CrossWatch - Overview navigation, actions and responsive theme coverage */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const sizes = [[320,740],[390,844],[430,932],[740,360],[844,390],[768,1024],[820,1180],[1024,768],[1194,834],[1366,1024],[1366,768],[1440,900],[1920,1080]];
const source = await readFile(new URL("../ui_frontend.py", import.meta.url),"utf8");
const profileMenu = source.match(/def _nav_profile_link\([^]*?return """([^]*?)"""/)[1].replaceAll("__CW_NAV_PROFILE_AVATAR__", "").replaceAll("__CW_NAV_PROFILE_ITEMS__", "");
const assets = new Map();
const rows = Array.from({length:20},(_,index) => ({key:`movie:${index}`,ref:{feature:"progress",key:`movie:${index}`},type:"movie",title:index === 1 ? 'A title with <script>window.injected=true</script>' : `Movie ${index + 1}`,year:2026,art:{kind:"movie",tmdb:String(index + 1)},ids:{tmdb:String(index + 1)},date:"2026-11-09T12:00:00Z",progress_percent:40,remaining_minutes:22}));
const episodes = Array.from({length:12},(_,index) => ({key:`episode:${index}`,ref:{show:`tmdb:${index + 100}`},type:"episode",title:`Show ${index + 1}`,art:{kind:"tv",tmdb:String(index + 100)},show_ids:{tmdb:String(index + 100)},season:2,episode:index ? 3 : 1,episode_title:"The next chapter",air_date:new Date(Date.UTC(2026,10,10 + [1,5,5,10,12,14,16,18,20,22,25,29][index])).toISOString().slice(0,10),date:"2026-11-08T12:00:00Z"}));

async function fixture(t, theme, {realMedia = false, touch = true, width = 1440, motion = "reduce", profiles = null, managed = false, airing = episodes, continuing = rows, next = episodes.slice(0,2), wrapped = {}, trackerOptions = {}} = {}) {
  const browser = await ({chromium,webkit})[process.env.CW_AUDIT_BROWSER || "chromium"].launch({headless:true});
  t.after(() => browser.close());
  const context = await browser.newContext({viewport:{width,height:900},hasTouch:touch,reducedMotion:motion});
  const page = await context.newPage();
  await page.clock.setFixedTime(new Date("2026-11-10T12:00:00Z"));
  page.setDefaultTimeout(7000);
  const errors = [], writes = [], requests = [], drops = [], scopedReads = [];
  let empty = false, fail = false, metadata = true, release, reviewMonth = "2026-10";
  let hold = null;
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors,[]));
  const html = await page.evaluate(({source,theme}) => {
    const doc = new DOMParser().parseFromString(source.slice(source.indexOf("<!doctype html>"),source.indexOf("</html>",source.indexOf("<!doctype html>"))+7),"text/html");
    doc.querySelectorAll("script,style").forEach(el => el.remove());
    doc.querySelector("#cw-main-loading")?.remove();
    if (theme !== "original") doc.documentElement.dataset.cwTheme = theme;
    doc.documentElement.className = theme === "original" ? "cw-theme-original" : `cw-theme-${theme === "flat-light" ? "light" : "dark"}`;
    doc.documentElement.dataset.tab = "tracker";
    doc.querySelector(theme === "original" ? "#cw-theme-flat-css" : "#cw-theme-original-css")?.remove();
    doc.querySelector("#layout").classList.add("full");
    for (const el of doc.querySelector("#layout").children) el.classList.toggle("hidden",el.id !== "page-tracker");
    return "<!doctype html>" + doc.documentElement.outerHTML.replaceAll("__CW_ASSET_BLOCK__","").replaceAll("__CW_CURRENT_VERSION__","Preview");
  },{source:source.replaceAll("__CW_NAV_PROFILE_MENU__",profileMenu),theme});
  await page.route("https://overview.test/**",async route => {
    const url = new URL(route.request().url()), path = url.pathname;
    if (path.startsWith('/api/')) requests.push(path);
    if (path.startsWith("/assets/")) {
      try {
        if (!assets.has(path)) assets.set(path,await readFile(new URL(".."+path,import.meta.url)));
        return route.fulfill({body:assets.get(path),contentType:path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : path.endsWith(".svg") ? "image/svg+xml" : "text/javascript"});
      } catch { return route.fulfill({status:404,body:""}); }
    }
    if (path.startsWith("/art/")) {
      if (path.endsWith("/1")) return route.fulfill({status:404,body:""});
      return route.fulfill({contentType:"image/svg+xml",body:'<svg xmlns="http://www.w3.org/2000/svg" width="600" height="900"><defs><linearGradient id="a" x2="1" y2="1"><stop stop-color="#25233f"/><stop offset="1" stop-color="#356b72"/></linearGradient></defs><rect width="600" height="900" fill="url(#a)"/><circle cx="450" cy="250" r="180" fill="#9ebcb8" opacity=".25"/><path d="M0 800 400 300 600 900" fill="#17212b" opacity=".7"/></svg>'});
    }
    if (!path.startsWith("/api/")) return route.fulfill({contentType:"text/html",body:html});
    const instance = url.searchParams.get("provider_instance") || "default";
    const viewAs = route.request().headers()['x-cw-view-as'] || '';
    if (path.startsWith('/api/tracker/')) scopedReads.push({path,viewAs,instance});
    let json = {ok:true};
    if (path === '/api/app-auth/status') json = {is_admin:!managed,user:{is_admin:!managed,profile_id:managed ? 'ann' : '',permissions:{write:true}}};
    if (path === '/api/user-profiles') json = {items:profiles || []};
    if (path === "/api/tracker/summary") json = {provider_instance:instance,tracker_options:trackerOptions,instances:[{id:"default",label:"Default"},{id:"other",label:"Second tracker"}],counts:{history:24},health:{history:{attention:2}},notes:{total:0,statuses:{},tags:[]}};
    if (path === '/api/tracker/summary' && (viewAs || managed)) json.instances = json.instances.filter(row => row.id === profiles?.find(profile => profile.id === (viewAs || 'ann'))?.instances?.CROSSWATCH?.[0]);
    if (path === "/api/tracker/stats") json = {week:4,month:15,year:244,total:244};
    if (path === "/api/tracker/overview/revision") json = {revision:"fixture",review_month:reviewMonth,wrapped};
    if (path === "/api/tracker/overview/wrapped") json = {ok:true,year:url.searchParams.get("year") || "2025",current_year:"2026",generated:1,pending:false,stale:false,report:{year:"2025",plays:0,titles:[],genres:[]}};
    if (path === "/api/tracker/overview/month") json = {ok:true,month:"2026-10",current_month:"2026-11",generated:1,pending:false,stale:false,report:{month:"2026-10",plays:0,titles:[],genres:[]}};
    if (path === "/api/tracker/overview") json = {revision:"fixture",review_month:reviewMonth,wrapped,metadata_available:metadata,continue:empty ? [] : continuing,recent:empty ? [] : rows.map(row=>({...row,progress_percent:null,ref:{feature:"history",key:row.key},plays:2}))};
    if (path === "/api/tracker/overview/shows") {
      const pending = hold;
      if (pending && instance === "default") await pending;
      if (fail) return route.fulfill({status:503,json:{error:"unavailable"}});
      json = {continue:empty ? [] : next,airing:empty ? [] : airing.map(row=>({...row,title:instance === "other" ? "Other tracker show" : row.title})),total:4,checked:4,done:true,unavailable:0,metadata_available:metadata};
      return route.fulfill({contentType:"application/x-ndjson",body:JSON.stringify(json)+"\n"});
    }
    if (path === "/api/tracker/title") {
      const row = episodes.find(row => row.ref.show === url.searchParams.get('show')) || rows.find(row => row.key === url.searchParams.get('key')) || rows[0];
      json = {title:row,show:row.art.kind === 'tv',plays:[],ratings:[],watchlist:[],collection:[],progress:[],attention:[],seasons:[]};
    }
    if (path === "/api/tracker/add") { writes.push(route.request().postDataJSON()); await new Promise(resolve => setTimeout(resolve,100)); json = {ok:true,batch:"watched"}; }
    if (path === "/api/tracker/titles/status") { drops.push(route.request().postDataJSON()); json = {ok:true,batch:"dropped",count:1}; }
    if (path === "/api/tracker/items") json = {items:empty ? [] : rows,total:empty ? 0 : rows.length};
    return route.fulfill({json});
  });
  await page.goto("https://overview.test/#tracker");
  const boot = async () => {
    await page.evaluate(managed => { document.documentElement.dataset.cwRole = managed ? "user" : "admin"; window.CW = {AuthState:{read:()=>({isAdmin:!managed,permissions:{write:true}})},ProfileMediaModal:{canOpen:item=>!!item.ids?.tmdb,open:(item,options)=>{window.openedTitle={item,options};}}}; },managed);
    if (realMedia) {
      await page.evaluate(() => {
        const meta = item => ({title:item.title,year:2026,genres:[],detail:{},overview:"Media card overview"});
        window.CW.Meta = {peek:meta,get:async item => meta(item)};
      });
      await page.addScriptTag({url:'/assets/js/profile-media-modal.js'});
    }
    for (const file of ["helpers/account-menu","helpers/icon-select","helpers/selection-bar","helpers/card-actions","js/tracker"]) await page.addScriptTag({url:`/assets/${file}.js`});
    if (profiles !== null) for (const file of ['helpers/api','helpers/profile-select','js/overview-profile']) await page.addScriptTag({url:`/assets/${file}.js`});
    await page.evaluate(() => TrackerPage.mount(document.querySelector("#page-tracker")));
    await page.locator('[data-trk-overview-open="airing:0"]').waitFor();
    await page.evaluate(() => document.fonts.ready);
  };
  await boot();
  return {page,boot,scopedReads,writes,requests,drops,wrapped:value=>{wrapped=value;},reviewMonth:value=>{reviewMonth=value;},airing:value=>{airing=value;},empty:value=>{empty=value;},fail:value=>{fail=value;},metadata:value=>{metadata=value;},hold:()=>{hold=new Promise(resolve=>{release=resolve;});},release:()=>{hold=null;release?.();}};
}

async function choose(page, selector, value) {
  const button = page.locator(`${selector} + .cw-icon-select .cw-icon-select-btn`);
  await button.click();
  await page.locator(`.cw-icon-select-menu:not(.hidden) [data-value="${value}"]`).click();
}

test("selected History, Shows and My titles tabs show their loaded totals", async t => {
  const {page} = await fixture(t,"flat-dark",{width:1440});
  await page.route('**/api/tracker/items?**', route => route.fulfill({json:{items:[rows[0]],total:12345}}));
  await page.route('**/api/tracker/shows?**', route => route.fulfill({json:{shows:[],total:10000}}));
  await page.route('**/api/tracker/notes?**', route => route.fulfill({json:{notes:[],total:1200000}}));
  for (const [view,total,compact] of [['history',12345,'12.3K'],['shows',10000,'10K'],['titles',1200000,'1.2M']]) {
    await page.locator(`.trk-tabs [data-trk-view="${view}"]`).click();
    const badge = page.locator(`.trk-tabs [data-trk-view="${view}"] .trk-tab-count`);
    await badge.filter({hasText:compact}).waitFor();
    const exact = await page.evaluate(value=>value.toLocaleString(),total);
    assert.equal(await badge.getAttribute('title'),exact);
    assert.equal(await badge.getAttribute('aria-label'),exact);
    assert.equal(await page.locator('.trk-tabs .trk-tab-count').count(),1);
  }
  await page.route('**/api/tracker/items?**', route => route.fulfill({json:{items:[rows[0]],total:2}}));
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('.trk-tabs [data-trk-view="history"] .trk-tab-count').filter({hasText:'2'}).waitFor();
});

for (const theme of ["original","flat-dark","flat-light"]) for (const width of [320,768,1440]) test(`${theme}: Continue watching overlays and honest badges at ${width}px`, async t => {
  const left = width === 320 ? 1 : 3;
  const {page,requests} = await fixture(t,theme,{width,continuing:[rows[0],{...rows[1],remaining_minutes:null,progress_percent:14}],next:[{...episodes[0],aired_episodes_left:left},{...episodes[1],remaining_minutes:42}]});
  const cards = page.locator('[data-trk-overview-open^="continue:"]');
  while (await cards.count() < 4) await page.locator('[data-trk-overview-more="continue"]').click();
  assert.deepEqual(await cards.locator('.trk-overview-badge').allTextContents(),['22 min left','14% watched',`${left} aired episode${left === 1 ? '' : 's'} left`,'Up next']);
  assert.equal(await cards.locator('.trk-overview-progress').count(),2);
  assert.equal(await cards.nth(1).locator('.trk-overview-progress i').evaluate(el=>el.style.width),'14%');
  assert.equal(await page.evaluate(()=>window.injected),undefined);
  const layout = await cards.evaluateAll(elements => elements.map(el => {
    const rect = selector => el.querySelector(selector).getBoundingClientRect();
    const art = rect('.trk-overview-art'), copy = rect('.trk-overview-copy'), badge = rect('.trk-overview-badge'), arrow = rect('.trk-overview-arrow');
    const actions = el.parentElement.querySelector('.trk-overview-menu').getBoundingClientRect();
    return {inside:copy.left>=art.left && copy.right<=art.right && copy.bottom<=art.bottom,clear:badge.bottom<=copy.top && actions.right<=badge.left && copy.right<=arrow.left,overflow:el.scrollWidth>el.clientWidth+1};
  }));
  assert.ok(layout.every(row=>row.inside && row.clear && !row.overflow),JSON.stringify(layout));
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  const reads = requests.length;
  await cards.first().hover();
  assert.equal(requests.length,reads);
  await cards.first().focus();
  await page.keyboard.press('Enter');
  await page.waitForFunction(()=>window.openedTitle?.options.tracker);
  assert.equal(await cards.first().getAttribute('aria-label'),'Open Movie 1, 40% watched, 22 min left');
  assert.equal(await cards.first().locator('.trk-overview-arrow').getAttribute('aria-hidden'),'true');
  await page.mouse.move(0,0);
  if (process.env.CW_OVERVIEW_SCREENSHOTS) {
    await mkdir(process.env.CW_OVERVIEW_SCREENSHOTS,{recursive:true});
    await (width === 320 ? cards.first() : page.locator('[aria-labelledby="trk-overview-continue"]')).screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/continue-${theme}-${width}.png`});
  }
});

for (const width of [390,1440]) test(`Tracker restores History and Month in review within one hour at ${width}px`,async t => {
  const {page}=await fixture(t,"flat-dark",{width});
  const leave = async () => {
    await page.evaluate(()=>document.querySelector('#page-tracker').classList.add('hidden'));
    await page.waitForFunction(()=>!document.querySelector('.mr-page'));
  };
  const returnToTracker = () => page.evaluate(async()=>{
    const root=document.querySelector('#page-tracker');
    root.classList.remove('hidden');
    await TrackerPage.mount(root);
  });
  if (width < 760) await choose(page,'#trk-mobile-view','history');
  else await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await leave();
  await returnToTracker();
  assert.equal(await page.locator('#trk-mobile-view').inputValue(),'history');
  if (width < 760) await choose(page,'#trk-mobile-view','month-review');
  else {
    await page.locator('.trk-others summary').click();
    await page.locator('[data-trk-month-open]').click();
  }
  await page.locator('.mr-page').waitFor();
  await leave();
  await returnToTracker();
  await page.locator('.mr-page').waitFor();
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.tracker.view.v3')).monthlyReview),true);
  await page.reload();
  await page.addScriptTag({url:'/assets/js/tracker.js'});
  await page.evaluate(()=>TrackerPage.mount(document.querySelector('#page-tracker')));
  await page.locator('.mr-page').waitFor();
  await page.locator('.mr-nav [data-mr="back"]').click();
  assert.equal(await page.locator('#trk-mobile-view').inputValue(),'history');
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.tracker.view.v3')).monthlyReview),false);
  await leave();
  await returnToTracker();
  assert.equal(await page.locator('.mr-page').count(),0);
});

for (const width of [390,1440]) test(`Account Overview link resets the remembered tracker view at ${width}px`, async t => {
  const {page} = await fixture(t,"flat-dark",{width});
  if (width < 760) await choose(page,'#trk-mobile-view','history');
  else await page.locator('.trk-tabs [data-trk-view="history"]').click();
  const openOverview = () => page.evaluate(async () => {
    window.location.hash = '#tracker?view=overview';
    await TrackerPage.mount(document.querySelector('#page-tracker'));
  });
  await openOverview();
  assert.equal(await page.locator('#trk-mobile-view').inputValue(), 'overview');
  assert.equal(new URL(page.url()).hash, '#tracker');
  if (width < 760) await choose(page,'#trk-mobile-view','month-review');
  else {
    await page.locator('.trk-others summary').click();
    await page.locator('[data-trk-month-open]').click();
  }
  await page.locator('.mr-page').waitFor();
  await openOverview();
  assert.equal(await page.locator('.mr-page').count(), 0);
  assert.equal(await page.locator('#trk-mobile-view').inputValue(), 'overview');
  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('cw.tracker.view.v3')));
  assert.equal(saved.view, 'overview');
  assert.equal(saved.monthlyReview, false);
  await page.evaluate(() => {
    const saved = JSON.parse(localStorage.getItem('cw.tracker.view.v3'));
    localStorage.setItem('cw.tracker.view.v3', JSON.stringify({...saved, view:'history', monthlyReview:true}));
  });
  await page.reload();
  await page.addScriptTag({url:'/assets/js/tracker.js'});
  await openOverview();
  assert.equal(await page.locator('#trk-mobile-view').inputValue(), 'overview');
  assert.equal(await page.locator('.mr-page').count(), 0);
});

for (const width of [390,1440]) test(`Tracker navigation expires the selected view after one hour at ${width}px`,async t => {
  const {page}=await fixture(t,'flat-dark',{width});
  await page.addScriptTag({url:'/assets/helpers/core.js'});
  await page.evaluate(()=>window.showTab('tracker'));
  assert.equal(await page.locator('#trk-mobile-view').inputValue(),'overview');
  if (width < 760) await choose(page,'#trk-mobile-view','history');
  else await page.locator('.trk-tabs [data-trk-view="history"]').click();
  const chosenAt = await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.tracker.view.v3')).viewSelectedAt);
  await page.clock.setFixedTime(new Date(chosenAt+3599999));
  await page.evaluate(async()=>{
    const root=document.querySelector('#page-tracker');
    root.classList.add('hidden');
    root.classList.remove('hidden');
    await TrackerPage.mount(root);
  });
  assert.equal(await page.locator('#trk-mobile-view').inputValue(),'history');
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.tracker.view.v3')).viewSelectedAt),chosenAt);
  await page.clock.setFixedTime(new Date(chosenAt+3600000));
  await page.locator(width < 760 ? '#cw-mobile-bottom-nav [data-mobile-route="tracker"]' : '#tab-tracker').click();
  await page.waitForFunction(()=>document.querySelector('#trk-mobile-view').value==='overview');
  if (width < 760) await choose(page,'#trk-mobile-view','month-review');
  else {
    await page.locator('.trk-others summary').click();
    await page.locator('[data-trk-month-open]').click();
  }
  await page.locator('.mr-page').waitFor();
  const reviewAt=await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.tracker.view.v3')).viewSelectedAt);
  await page.clock.setFixedTime(new Date(reviewAt+3600000));
  await page.locator(width < 760 ? '#cw-mobile-bottom-nav [data-mobile-route="tracker"]' : '#tab-tracker').click();
  await page.locator('.mr-page').waitFor({state:'detached'});
  assert.equal(await page.locator('#trk-mobile-view').inputValue(),'overview');
  await page.reload();
  await page.addScriptTag({url:'/assets/js/tracker.js'});
  await page.evaluate(()=>TrackerPage.mount(document.querySelector('#page-tracker')));
  assert.equal(await page.locator('#trk-mobile-view').inputValue(),'overview');
});

test("Overview navigation and tracker selector have matching panel heights",async t => {
  for (const theme of ['original','flat-dark','flat-light']) {
    const {page}=await fixture(t,theme,{touch:false});
    const panels=await page.evaluate(()=>{
      const box=selector=>{const r=document.querySelector(selector).getBoundingClientRect();return {top:r.top,height:r.height};};
      return {nav:box('[data-trk-slot="controls"]'),tracker:box('[data-trk-slot="source"]')};
    });
    assert.ok(Math.abs(panels.nav.top-panels.tracker.top)<1,`${theme}: ${JSON.stringify(panels)}`);
    assert.ok(Math.abs(panels.nav.height-panels.tracker.height)<1,`${theme}: ${JSON.stringify(panels)}`);
    const tabs=await page.evaluate(()=>{
      const center=selector=>{const r=document.querySelector(selector).getBoundingClientRect();return r.top+r.height/2;};
      return {tab:center('.trk-tabs .trk-tab span:last-child'),others:center('.trk-others>summary span:nth-child(2)')};
    });
    assert.ok(Math.abs(tabs.tab-tabs.others)<1,`${theme}: ${JSON.stringify(tabs)}`);
    await page.setViewportSize({width:390,height:844});
    assert.ok(await page.locator('#trk-instance + .cw-icon-select').isVisible());
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  }
});

test('Resizing Tracker keeps Main widgets hidden and restores their layout on Main', async t => {
  const {page,requests} = await fixture(t,'flat-dark',{touch:false});
  await page.addScriptTag({url:'/assets/helpers/core.js'});
  await page.evaluate(()=>window.showTab('tracker'));
  await page.addScriptTag({url:'/assets/js/dashboard-widgets.js'});
  const dashboard = page.locator('#dashboard-widgets-card');
  const resize = async width => {
    await page.setViewportSize({width,height:900});
    await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  };
  const widgetRequests = () => requests.filter(path=>path.includes('/dashboard/widgets')).length;
  const before = widgetRequests();
  for (const width of [390,1440,760,761]) {
    await resize(width);
    assert.equal(await dashboard.isVisible(),false,`Main widgets leaked into Tracker at ${width}px`);
    assert.equal(await page.locator('#page-tracker').isVisible(),true);
  }
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('cw:watchlist-widget-state',{detail:{empty:true}})));
  assert.equal(await dashboard.isVisible(),false);
  assert.equal(widgetRequests(),before);
  await page.evaluate(()=>window.showTab('main'));
  await dashboard.waitFor({state:'visible'});
  for (const width of [390,1440]) {
    await resize(width);
    assert.equal(await dashboard.isVisible(),true);
    assert.equal(await page.locator('#recent-history-widget').isVisible(),true);
    assert.equal(await page.locator('#recent-progress-widget').isVisible(),false);
  }
  await page.evaluate(()=>window.showTab('tracker'));
  for (const width of [390,1440]) {
    await resize(width);
    assert.equal(await dashboard.isVisible(),false);
  }
});

test("Monthly notification links open the requested tracker and month",async t => {
  const {page}=await fixture(t,"flat-dark",{touch:false});
  await page.addScriptTag({url:'/assets/helpers/core.js'});
  await page.evaluate(()=>window.showTab('tracker'));
  await page.addScriptTag({url:'/assets/helpers/notifications.js'});
  let pending=true;
  await page.route('https://overview.test/api/tracker/overview/month**',route=>{
    const url=new URL(route.request().url());
    return route.fulfill({json:{month:url.searchParams.get('month'),current_month:'2026-11',pending,stale:pending,report:pending?null:{plays:0,titles:[],genres:[]}}});
  });
  await choose(page,'#trk-instance','other');
  await page.locator('[data-trk-month]').click();
  await page.locator('.mr-month-input input').fill('2025-08');
  await page.locator('.mr-month-input input').dispatchEvent('change');
  await page.waitForResponse(response=>response.url().includes('month=2025-08'));
  await page.locator('.mr-nav [data-mr="back"]').click();
  pending=false;
  await page.locator('#cw-notifications-button').click();
  const link=page.locator('.cw-notification[href="#tracker?instance=other&month=2025-08"]');
  await link.waitFor();
  const response=page.waitForRequest(request=>request.url().includes('/overview/month?') && request.url().includes('month=2025-08'));
  await link.click();
  const url=new URL((await response).url());
  assert.equal(url.searchParams.get('provider_instance'),'other');
  assert.equal(url.searchParams.get('month'),'2025-08');
  await page.locator('.mr-empty').waitFor();
  assert.equal(await page.locator('.mr-month-input input').inputValue(),'2025-08');
});

test("Monthly review opens lazily from Overview and returns to the tracker",async t => {
  const {page,requests} = await fixture(t,"flat-dark",{touch:false});
  assert.equal(requests.includes("/api/tracker/overview/month"),false);
  await page.locator("[data-trk-month]").click();
  await page.locator(".mr-empty").waitFor();
  assert.equal(await page.locator("#page-tracker>.cw-root").isVisible(),false);
  assert.equal(requests.filter(p=>p==="/api/tracker/overview/month").length,1);
  await page.locator('.mr-nav [data-mr="back"]').click();
  assert.equal(await page.locator("[data-trk-month]").isVisible(),true);
  assert.equal(await page.locator(".mr-page").count(),0);
});

test("Monthly banner and poster share the chosen holiday theme",async t=>{
  const {page,requests}=await fixture(t,"flat-dark",{touch:false});
  await page.waitForFunction(()=>document.querySelector('.trk-month-banner')?.dataset.monthTheme === 'halloween');
  await page.locator('[data-trk-month]').click();
  await page.locator('.mr-empty').waitFor();
  await page.locator('.mr-options>summary').click();
  await page.selectOption('[data-mr-theme]','christmas');
  assert.equal(await page.locator('.mr-page').getAttribute('data-month-theme'),'christmas');
  await page.locator('.mr-nav [data-mr="back"]').click();
  await page.waitForFunction(()=>document.querySelector('.trk-month-banner')?.dataset.monthTheme === 'christmas');
  assert.equal(await page.locator('.trk-month-decoration .mr-ornaments').count(),1);
  assert.equal(requests.filter(path=>path==='/api/tracker/overview/month').length,1);
  await mkdir('.tmp/month-review',{recursive:true});
  await page.locator('.trk-month-banner').screenshot({path:'.tmp/month-review/christmas-banner.png'});
});

test("Monthly banner dismissal persists per user and tracker until a newer report is ready",async t=>{
  const app = await fixture(t,"flat-dark",{touch:false}), {page,requests} = app;
  await page.locator('[data-trk-month-dismiss]').click();
  assert.equal(await page.locator('.trk-month-banner').count(),0);
  assert.equal(requests.includes('/api/tracker/overview/month'),false);
  await page.locator('.trk-others summary').click();
  await page.locator('[data-trk-month-open]').click();
  await page.locator('.mr-empty').waitFor();
  await page.locator('.mr-nav [data-mr="back"]').click();
  assert.equal(await page.locator('.trk-month-banner').count(),0);
  const refresh = async()=>{
    await page.locator('.trk-tabs [data-trk-view="history"]').click();
    await page.locator('.trk-tabs [data-trk-view="overview"]').click();
    await page.locator('[data-trk-overview-open="airing:0"]').waitFor();
  };
  await refresh();
  assert.equal(await page.locator('.trk-month-banner').count(),0);
  await choose(page,'#trk-instance','other');
  await page.locator('[data-trk-month]').waitFor();
  await choose(page,'#trk-instance','default');
  await page.waitForFunction(()=>!document.querySelector('.trk-month-banner'));
  await page.evaluate(()=>{window.CW.AuthState.read=()=>({user:{id:"ann"},profileId:"ann",role:"user",isAdmin:false,permissions:{write:true}});});
  await refresh();
  await page.locator('[data-trk-month]').waitFor();
  await page.locator('[data-trk-month-dismiss]').click();
  await refresh();
  assert.equal(await page.locator('.trk-month-banner').count(),0);
  app.reviewMonth("");
  await refresh();
  assert.equal(await page.locator('.trk-month-banner').count(),0);
  app.reviewMonth("2026-11");
  await page.evaluate(()=>document.dispatchEvent(new Event("visibilitychange")));
  await page.locator('[data-trk-month="2026-11"]').waitFor();
  await page.setViewportSize({width:390,height:844});
  await page.locator('[data-trk-month-dismiss]').click();
  await choose(page,'#trk-mobile-view','month-review');
  await page.locator('.mr-empty').waitFor();
});

for (const theme of ["original","flat-dark","flat-light"]) test(`${theme}: overview layout and actions across devices`,async t => {
  const app = await fixture(t,theme), {page,writes} = app;
  assert.equal(await page.locator('.trk-tabs [role="tab"]').first().textContent(),"space_dashboardOverview");
  assert.equal(await page.locator('.trk-tabs [data-trk-view="cleanup"]').count(),0);
  assert.equal(await page.locator("[data-trk-slot=filters]").isVisible(),true);
  assert.equal(await page.locator('[data-trk-overview-open^="continue:"]').count(),4);
  assert.equal(await page.locator('[data-trk-overview-open^="recent:"]').count(),10);
  assert.equal(await page.evaluate(() => window.injected),undefined);
  assert.equal(await page.locator('.trk-calendar-day').count(),30);
  assert.equal(await page.locator('.trk-calendar-day.is-today time').getAttribute('datetime'),"2026-11-10");
  assert.equal(await page.locator('[data-calendar-date="2026-11-15"] .trk-calendar-release').count(),2);
  assert.equal(await page.locator('.trk-calendar-day').last().getAttribute('data-calendar-date'),"2026-12-09");
  assert.equal(await page.locator('[data-trk-overview-more="airing"]').count(),0);
  for (const [width,height] of sizes) {
    await page.setViewportSize({width,height});
    await page.waitForTimeout(90);
    assert.equal(await page.locator('[data-trk-slot="source"] .cw-icon-select-btn').isVisible(),true,`${theme} ${width}x${height}: Tracker selector`);
    const layout = await page.evaluate(() => ({width:innerWidth,scroll:document.documentElement.scrollWidth,nested:[...document.querySelectorAll("#page-tracker *")].filter(el=>el.checkVisibility()&&/auto|scroll/.test(getComputedStyle(el).overflowY)&&el.scrollHeight>el.clientHeight+1).map(el=>el.className)}));
    assert.ok(layout.scroll<=width+1,`${theme} ${width}x${height}: ${JSON.stringify(layout)}`);
    assert.deepEqual(layout.nested,[],`${theme} ${width}x${height}`);
    const timeline = page.locator('.trk-calendar');
    assert.equal(await timeline.evaluate(el => getComputedStyle(el).scrollbarWidth),"none");
    assert.equal(await timeline.evaluate(el => el.scrollHeight > el.clientHeight + 1),false);
    await page.locator('[data-trk-calendar="next"]').click();
    assert.ok(await timeline.evaluate(el => el.scrollLeft > 0));
    await page.locator('[data-trk-calendar="previous"]').click();
    assert.equal(await timeline.evaluate(el => Math.round(el.scrollLeft)),0);
    await page.locator('[data-trk-overview-open="airing:11"]').focus();
    await page.keyboard.press('Enter');
    await page.waitForFunction(() => window.openedTitle?.options.tracker);
    assert.ok(await timeline.evaluate(el => el.scrollLeft > 0));
    await page.locator('[data-trk-calendar="today"]').click();
    assert.equal(await timeline.evaluate(el => Math.round(el.scrollLeft)),0);
    const more = page.locator('[data-trk-overview-more="recent"]');
    const recent = page.locator('[data-trk-overview-open^="recent:"]');
    const step = await page.locator('[aria-labelledby="trk-overview-recent"] .trk-cover-grid').evaluate(el => getComputedStyle(el).gridTemplateColumns.split(/\s+/).length * 2);
    assert.equal(await recent.count(),step);
    await more.click();
    assert.equal(await recent.count(),Math.min(step*2,20));
    while (await recent.count() < 20) await more.click();
    await more.click();
    assert.equal(await recent.count(),step);
    if (process.env.CW_OVERVIEW_SCREENSHOTS) {
      await mkdir(process.env.CW_OVERVIEW_SCREENSHOTS,{recursive:true});
      await page.evaluate(() => window.scrollTo(0,0));
      await page.screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/${theme}-${width}x${height}.png`,fullPage:true});
    }
  }
  await page.setViewportSize({width:1440,height:900});
  const summary = page.locator(".trk-others summary");
  await summary.focus();
  await page.keyboard.press("ArrowDown");
  assert.equal(await page.locator('.trk-others button:visible').first().evaluate(el=>el===document.activeElement),true);
  await page.keyboard.press("Escape");
  assert.equal(await page.locator(".trk-others").getAttribute("open"),null);
  await summary.click();
  await page.locator('.trk-others [data-trk-view="cleanup"]').click();
  await page.locator(".trk-toolbar-cleanup").waitFor();
  assert.equal(await page.locator(".trk-others.is-active").count(),1);
  await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  await page.locator('[data-trk-overview-open="continue:0"]').click();
  await page.waitForFunction(() => window.openedTitle?.options.tracker);
  await page.locator('[data-trk-overview-menu="0"]').click();
  await page.getByRole('menuitem',{name:'Log a watch',exact:true}).click();
  await page.waitForFunction(() => document.querySelector('[data-trk-overview-menu="0"]')?.disabled === false);
  assert.equal(writes.length,1);
  assert.equal(writes[0].provider_instance,"default");
  assert.equal(writes[0].items[0].ids.tmdb,"1");
  await page.setViewportSize({width:390,height:844});
  await choose(page,"#trk-mobile-view","cleanup");
  await page.locator(".trk-toolbar-cleanup").waitFor();
  assert.equal(await page.locator('#trk-mobile-view optgroup[label="More"] option[value="cleanup"]').textContent(),"Clean up (2)");
  await choose(page,"#trk-mobile-view","overview");
  await page.locator('[data-trk-overview-open="airing:0"]').waitFor();
  await page.setViewportSize({width:1440,height:900});
  app.hold();
  await page.locator('[data-trk-action="refresh"]').click();
  await page.locator('[data-trk-overview-open="continue:0"]').waitFor();
  await choose(page,'[data-trk-input="instance"]',"other");
  await page.locator(".trk-calendar-copy strong").filter({hasText:"Other tracker show"}).first().waitFor();
  app.release();
  await page.waitForTimeout(100);
  assert.equal(await page.locator('[data-trk-overview-open="airing:0"] .trk-calendar-copy strong').textContent(),"Other tracker show");
  app.fail(true);
  await page.locator('[data-trk-action="refresh"]').click();
  await page.locator(".trk-overview-notice").filter({hasText:"couldn't be checked"}).waitFor();
  assert.equal(await page.locator('[data-trk-overview-open^="recent:"]').count(),10);
  app.fail(false); app.empty(true); app.metadata(false);
  await page.locator('[data-trk-action="refresh"]').click();
  await page.locator(".trk-overview-empty").filter({hasText:"TMDB API key"}).waitFor();
  assert.equal(await page.locator(".trk-overview-empty").count(),3);
});

for (const theme of ["original","flat-dark","flat-light"]) test(`${theme}: cover menus and real media cards`, async t => {
  const {page,requests,writes,drops} = await fixture(t,theme,{realMedia:true,touch:false});
  const cover = page.locator('[data-trk-overview-open="continue:0"] .trk-overview-art');
  const actions = page.locator('.trk-overview-menu').first();
  await page.mouse.move(0,0);
  assert.equal(await actions.evaluate(el => getComputedStyle(el).backgroundColor),"rgba(0, 0, 0, 0)");
  const before = requests.length;
  await cover.hover();
  assert.equal(await actions.isVisible(),true);
  assert.equal(await page.locator('.trk-overview-peek').count(),0);
  assert.equal(requests.length,before);
  if (process.env.CW_OVERVIEW_SCREENSHOTS) {
    await mkdir(process.env.CW_OVERVIEW_SCREENSHOTS,{recursive:true});
    await page.locator('.trk-overview-card').first().screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/${theme}-hover.png`});
  }
  await page.locator('[data-trk-overview-open="continue:0"] .trk-overview-copy>strong').hover();
  assert.equal(await actions.isVisible(),true);
  await page.locator('[data-trk-overview-open="continue:0"]').focus();
  await page.keyboard.press('Tab');
  assert.equal(await page.locator('[data-trk-overview-menu="0"]').evaluate(el => el === document.activeElement),true);
  assert.equal(await actions.isVisible(),true);
  for (const [kind, name] of [["continue","Movie 1"],["recent","Movie 1"],["airing","Show 1"]]) {
    const target = page.locator(`[data-trk-overview-open="${kind}:0"]`);
    if (kind === 'airing') await target.click({position:{x:100,y:100}});
    else await target.locator(kind === 'recent' ? '.trk-grid-art' : '.trk-overview-art').click({position:{x:100,y:100}});
    const modal = page.locator('#cw-media-modal');
    await modal.waitFor();
    assert.equal(await modal.locator('#cw-mm-title').getAttribute('aria-label'),name);
    assert.equal(await page.locator('.trk-sheet:visible').count(),0);
    if (process.env.CW_OVERVIEW_SCREENSHOTS && kind === 'airing') await modal.screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/${theme}-media-card.png`});
    await page.keyboard.press('Escape');
    await modal.waitFor({state:'hidden'});
  }
  await cover.hover();
  await page.locator('[data-trk-overview-menu="0"]').click();
  await page.getByRole('menuitem',{name:'Log a watch',exact:true}).click();
  await page.waitForFunction(() => document.querySelector('[data-trk-overview-menu="0"]')?.disabled === false);
  assert.equal(writes.length,1);
  assert.equal(await page.locator('#cw-media-modal').isVisible(),false);
  await cover.hover();
  await page.locator('[data-trk-overview-menu="0"]').click();
  await page.getByRole('menuitem',{name:'Drop title',exact:true}).click();
  await page.waitForFunction(() => document.querySelector('.trk-overview-menu')?.disabled === false);
  assert.deepEqual(drops,[{provider_instance:"default",keys:["movie:0"],status:"dropped",view:"progress",feature:"progress"}]);
  assert.equal(await page.locator('#cw-media-modal').isVisible(),false);
  for (let i=0;i<5;i++) await page.locator('[data-trk-overview-more="continue"]').click();
  await page.locator('[data-trk-overview-open="continue:20"] .trk-overview-art').hover();
  await page.locator('[data-trk-overview-menu="20"]').click();
  await page.getByRole('menuitem',{name:'Drop show',exact:true}).click();
  await page.waitForFunction(() => document.querySelector('.trk-overview-menu')?.disabled === false);
  assert.deepEqual(drops[1],{provider_instance:"default",keys:["tmdb:100"],status:"dropped",view:"shows",feature:"history"});
  await page.evaluate(() => { window.CW.AuthState.read = () => ({permissions:{write:false}}); });
  await page.locator('[data-trk-overview-more="continue"]').click();
  await page.locator('[data-trk-overview-menu="0"]').click();
  assert.equal(await page.getByRole('menuitem',{name:'Drop title',exact:true}).isDisabled(),true);
  assert.equal(await page.getByRole('menuitem',{name:'Log a watch',exact:true}).isDisabled(),true);
});

test("touch covers expose an unboxed action menu and open the real media card", async t => {
  const {page} = await fixture(t,"flat-dark",{realMedia:true,touch:true,width:390});
  const action = page.locator('[data-trk-overview-menu="0"]');
  assert.equal(await action.isVisible(),true);
  const bounds = await action.boundingBox();
  assert.ok(bounds.width >= 44 && bounds.height >= 44);
  assert.equal(await action.evaluate(el => getComputedStyle(el).backgroundColor),'rgba(0, 0, 0, 0)');
  await action.tap();
  const menu = page.locator('.cw-card-action-menu:popover-open');
  await menu.waitFor();
  const menuBounds = await menu.boundingBox();
  assert.ok(menuBounds.x >= 0 && menuBounds.x + menuBounds.width <= 390);
  if (process.env.CW_OVERVIEW_SCREENSHOTS) {
    await mkdir(process.env.CW_OVERVIEW_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/overview-menu-phone.png`});
  }
  await page.keyboard.press('Escape');
  await page.locator('[data-trk-overview-open="recent:0"] .trk-grid-art').tap();
  await page.locator('#cw-media-modal').waitFor();
  assert.equal(await page.locator('#cw-mm-title').getAttribute('aria-label'),"Movie 1");
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth+1),false);
});

test("airing cards share available width according to release density", async t => {
  const app = await fixture(t,"flat-dark",{width:1900,touch:false,airing:episodes.slice(1,2)});
  const {page,requests} = app;
  const card = page.locator('.trk-calendar-release').first();
  const width = () => card.evaluate(el => el.getBoundingClientRect().width);
  const single = await width();
  assert.ok(single > 800 && single <= 960);
  assert.equal(await page.locator('.trk-calendar-day:not(.has-releases)').first().evaluate(el => el.getBoundingClientRect().width),52);
  const requestCount = requests.length;
  await page.setViewportSize({width:1440,height:900});
  assert.ok(await width() < single);
  assert.equal(requests.length,requestCount);
  await page.setViewportSize({width:1900,height:900});
  app.airing(episodes.slice(1,3));
  await page.locator('[data-trk-action="refresh"]').click();
  await page.waitForFunction(() => document.querySelectorAll('.trk-calendar-release').length === 2);
  const paired = await card.evaluate(el => ({width:el.getBoundingClientRect().width,next:el.nextElementSibling.getBoundingClientRect().width}));
  assert.ok(paired.width < single && paired.width >= 240);
  assert.equal(paired.width,paired.next);
  app.airing(episodes);
  await page.locator('[data-trk-action="refresh"]').click();
  await page.waitForFunction(() => document.querySelectorAll('.trk-calendar-release').length === 12);
  assert.ok(await width() < paired.width);
  await page.setViewportSize({width:390,height:844});
  assert.ok(await width() <= 300);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1),false);
  await page.waitForFunction(() => { const calendar=document.querySelector('.trk-calendar'); return calendar.scrollWidth > calendar.clientWidth; });
  await page.setViewportSize({width:1900,height:900});
  app.empty(true);
  await page.locator('[data-trk-action="refresh"]').click();
  await page.waitForFunction(() => !document.querySelector('.trk-calendar-release'));
  assert.equal(await page.locator('.trk-calendar-day').count(),30);
});

test("streaming overview preserves existing cards, batches rendering and aborts on navigation", async t => {
  const {page} = await fixture(t,"flat-dark");
  await page.evaluate(() => {
    const fetch = window.fetch;
    window.streamRequests = 0;
    window.streamAborts = 0;
    window.fetch = (url, init) => {
      if (!String(url).includes("/overview/shows?")) return fetch(url, init);
      window.streamRequests++;
      const body = new ReadableStream({start(controller) {
        window.overviewStream = controller;
        init.signal.addEventListener("abort", () => { window.streamAborts++; controller.error(new DOMException("Aborted", "AbortError")); }, {once:true});
      }});
      return Promise.resolve(new Response(body, {headers:{"Content-Type":"application/x-ndjson"}}));
    };
  });
  await page.locator('[data-trk-action="refresh"]').click();
  await page.waitForFunction(() => window.streamRequests === 1);
  await page.locator('[data-trk-overview-open="recent:0"]').waitFor();
  await page.evaluate(() => {
    window.recentCard = document.querySelector('[data-trk-overview-open="recent:0"]');
    window.continueCard = document.querySelector('[data-trk-overview-open="continue:0"]');
    window.calendarNode = document.querySelector('.trk-calendar');
  });
  const send = async event => page.evaluate(event => {
    const bytes = new TextEncoder().encode(JSON.stringify(event)+"\n");
    window.overviewStream.enqueue(bytes.slice(0,17));
    window.overviewStream.enqueue(bytes.slice(17));
  },event);
  await send({airing:[{...episodes[0],title:"Première"}],checked:4,total:500,done:false});
  await page.waitForTimeout(150);
  assert.equal(await page.locator('.trk-calendar-release').count(),12);
  await page.evaluate(() => {
    window.firstRelease = document.querySelector('[data-trk-overview-open="airing:0"]');
    window.calendarNode.scrollLeft = 60;
  });
  await send({airing:[episodes[1]],checked:8,total:500,done:false});
  await page.locator('.trk-calendar-copy strong').filter({hasText:"Show 2"}).waitFor();
  assert.deepEqual(await page.evaluate(() => ({
    recent:window.recentCard === document.querySelector('[data-trk-overview-open="recent:0"]'),
    continuing:window.continueCard === document.querySelector('[data-trk-overview-open="continue:0"]'),
    calendar:window.calendarNode === document.querySelector('.trk-calendar'),
    release:window.firstRelease === document.querySelector('[data-trk-overview-open="airing:0"]'),
    scroll:window.calendarNode.scrollLeft,
  })),{recent:true,continuing:true,calendar:true,release:true,scroll:60});
  await page.evaluate(() => {
    window.overviewPaints = 0;
    window.overviewObserver = new MutationObserver(() => window.overviewPaints++);
    window.overviewObserver.observe(document.querySelector('.trk-overview'),{childList:true,subtree:true});
  });
  await page.evaluate(base => {
    const encoder = new TextEncoder();
    for (let i=8;i<500;i+=4) window.overviewStream.enqueue(encoder.encode(JSON.stringify({
      airing:Array.from({length:4},(_,j)=>({...base,key:`scale:${i+j}`,title:`Show ${i+j}`,air_date:"2026-12-09"})),
      checked:i+4,total:500,done:false,
    })+"\n"));
  },episodes[2]);
  await page.waitForTimeout(150);
  assert.equal(await page.locator('.trk-calendar-release').count(),12);
  await send({checked:500,total:500,done:true});
  await page.waitForFunction(() => document.querySelectorAll('.trk-calendar-release').length === 494);
  assert.ok(await page.evaluate(() => window.overviewPaints <= 3));
  assert.equal(await page.evaluate(() => window.streamRequests),1);
  assert.equal(await page.evaluate(() => window.recentCard.isConnected),true);
  await page.evaluate(() => document.querySelector('#page-tracker').classList.add('hidden'));
  await page.waitForFunction(() => window.streamAborts === 1);
  await page.evaluate(async () => {
    document.querySelector('#page-tracker').classList.remove('hidden');
    await TrackerPage.mount(document.querySelector('#page-tracker'));
  });
  await page.waitForFunction(() => window.streamRequests === 2);
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.waitForFunction(() => window.streamAborts === 2);
  assert.equal(await page.locator('.trk-overview').count(),0);
});


test("cached overview skips show rebuilding and shares the compact History cards", async t => {
  const {page, requests, drops} = await fixture(t,"flat-dark",{touch:false});
  assert.equal(await page.locator('[data-trk-slot="controls"] [data-trk-action="log"]').count(),0);
  const logWatch = page.locator('.trk-overview-section[aria-labelledby="trk-overview-recent"] [data-trk-action="log"]');
  await logWatch.click();
  await page.locator('.trk-sheet:visible [data-trk-log-q]').waitFor();
  await page.keyboard.press('Escape');
  const cached = {continue:episodes.slice(0,2),airing:episodes,total:4,checked:4,done:true,unavailable:0};
  await page.route("**/api/tracker/overview?**", route => route.fulfill({json:{revision:"warm",metadata_available:true,hero:episodes[0],continue:rows,recent:rows.map(row=>({...row,progress_percent:null,ref:{feature:"history",key:row.key}})),shows:cached}}));
  const streamCount = requests.filter(path => path === "/api/tracker/overview/shows").length;
  await page.locator('[data-trk-action="refresh"]').click();
  await page.locator('.trk-hero-heading strong').filter({hasText:"Show 1"}).waitFor({state:'attached'});
  assert.equal(await page.locator('.trk-hero-logo').getAttribute('alt'),"Show 1");
  assert.match(await page.locator('.trk-hero-backdrop').getAttribute('src'),/tv\/100/);
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('[data-trk-layout="grid"]').click();
  const history = page.locator('.trk-grid-card').first();
  assert.equal(await history.locator('.trk-cover-action').count(),0);
  assert.equal(await history.locator('[data-trk-card-open]').count(),1);
  await history.locator('[data-trk-card-menu]').click();
  await page.getByRole('menuitem',{name:'Log a rewatch',exact:true}).click();
  await page.locator('.trk-sheet:visible').waitFor();
  await page.keyboard.press('Escape');
  const title = await history.locator('.trk-grid-copy>strong').boundingBox();
  const edit = await history.locator('[data-trk-card-menu]').boundingBox();
  assert.ok(Math.abs(title.y + title.height/2 - edit.y - edit.height/2) < 2);
  await history.locator('[data-trk-card-menu]').click();
  await page.getByRole('menuitem',{name:'Mark as dropped',exact:true}).click();
  await page.waitForFunction(() => document.querySelector('[data-trk-card-menu]')?.disabled === false);
  assert.equal(drops[0].feature,"history");
  await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  const recent = page.locator('[data-trk-overview-open="recent:0"]');
  await recent.waitFor();
  assert.equal(await recent.locator('.trk-grid-art').count(),1);
  assert.equal(await recent.locator('[data-trk-card-open]').count(),1);
  assert.equal(await recent.locator('[data-trk-pick]').count(),0);
  await recent.focus();
  await page.keyboard.press('Enter');
  await page.waitForFunction(() => window.openedTitle?.options.tracker);
  await recent.locator('[data-trk-card-menu]').click();
  await page.getByRole('menuitem',{name:'Edit watch',exact:true}).click();
  await page.locator('.trk-sheet:visible').waitFor();
  await page.keyboard.press('Escape');
  assert.equal(requests.filter(path => path === "/api/tracker/overview/shows").length,streamCount);
  const positions = await page.evaluate(() => {
    const controls = document.querySelector('[data-trk-slot="controls"]').getBoundingClientRect();
    const source = document.querySelector('[data-trk-slot="source"]').getBoundingClientRect();
    const main = document.querySelector('.cw-main').getBoundingClientRect();
    return {aligned:Math.abs(controls.top-source.top)<2,separate:source.left>=controls.right,fullWidth:main.width>controls.width};
  });
  assert.deepEqual(positions,{aligned:true,separate:true,fullWidth:true});
  if (process.env.CW_OVERVIEW_SCREENSHOTS) {
    await mkdir(process.env.CW_OVERVIEW_SCREENSHOTS,{recursive:true});
    await page.evaluate(() => window.scrollTo(0,0));
    await page.screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/overview-updated.png`,fullPage:true});
  }
});


test("Continue watching links to Shows for next episodes and Progress for saved playback", async t => {
  const {page} = await fixture(t,"flat-dark");
  let continuing = [];
  await page.route("**/api/tracker/overview?**", route => route.fulfill({json:{metadata_available:true,continue:continuing,recent:[],shows:{continue:episodes.slice(0,2),airing:episodes,done:true}}}));
  await page.route("**/api/tracker/shows?**", route => route.fulfill({json:{shows:[{key:"tmdb:100",title:"Show 1",art:{kind:"tv",tmdb:"100"},watched:1,total:10}],total:1}}));
  await page.locator('[data-trk-action="refresh"]').click();
  const section = page.locator('[aria-labelledby="trk-overview-continue"]');
  await section.getByRole('button',{name:'View shows',exact:true}).click();
  await page.locator('.trk-tabs [data-trk-view="shows"][aria-selected="true"]').waitFor();
  await page.locator('[data-trk-ref-show="tmdb:100"]').waitFor();
  continuing = rows.slice(0,1);
  await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  await section.getByRole('button',{name:'Playback progress',exact:true}).click();
  await page.locator('.trk-tabs [data-trk-view="progress"][aria-selected="true"]').waitFor();
  await page.locator('[data-trk-ref-key="movie:0"]').waitFor();
});


test("Recently watched fills two responsive rows and expands in full rows", async t => {
  const {page} = await fixture(t,"flat-dark");
  const cards = page.locator('[data-trk-overview-open^="recent:"]');
  const more = page.locator('[data-trk-overview-more="recent"]');
  for (const [width,count] of [[1440,10],[1024,6],[740,4],[390,2],[1440,10]]) {
    await page.setViewportSize({width,height:900});
    await page.waitForFunction(count => document.querySelectorAll('[data-trk-overview-open^="recent:"]').length === count,count);
    const rows = await cards.evaluateAll(items => new Set(items.map(el => Math.round(el.getBoundingClientRect().top))).size);
    assert.equal(rows,2);
    await more.click();
    assert.equal(await cards.count(),count*2);
    while (await cards.count() < 20) await more.click();
    await more.click();
    assert.equal(await cards.count(),count);
  }
});


test("Tracker navigation moves whole tabs into More and keeps the selected view visible", async t => {
  const {page} = await fixture(t,"flat-dark",{width:1920,touch:false});
  const tabs = page.locator('.trk-tabs');
  const more = page.locator('.trk-others summary');
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.waitForFunction(() => document.querySelectorAll('.trk-tabs [role="tab"]:not([hidden])').length === 8);
  await page.evaluate(() => document.querySelector('[data-trk-slot="controls"]').style.maxWidth = '600px');
  await page.waitForFunction(() => document.querySelector('.trk-tabs [data-trk-view="collection"]').hidden);
  assert.deepEqual(await tabs.locator('[role="tab"]:visible').evaluateAll(items => items.slice(0,3).map(el => el.dataset.trkView)),['overview','history','shows']);
  assert.equal(await tabs.locator('[aria-selected="false"] .trk-tab-count').count(),0);
  const fits = () => tabs.evaluate(list => [...list.querySelectorAll('[role="tab"]:not([hidden])')].every(tab => tab.getBoundingClientRect().right <= list.getBoundingClientRect().right + 1));
  assert.equal(await fits(),true);
  await more.focus();
  await page.keyboard.press('ArrowDown');
  assert.equal(await page.locator('.trk-overflow button:visible').first().evaluate(el => el === document.activeElement),true);
  const collection = page.locator('.trk-overflow [data-trk-view="collection"]');
  await collection.focus();
  await page.keyboard.press('Enter');
  await page.locator('.trk-tabs [data-trk-view="collection"][aria-selected="true"]').waitFor();
  await page.waitForFunction(() => document.activeElement?.matches('.trk-tabs [data-trk-view="collection"]'));
  assert.equal(await fits(),true);
  assert.equal(await page.locator('.trk-overflow [data-trk-view="collection"]').isVisible(),false);
  await page.keyboard.press('Home');
  await page.locator('.trk-tabs [data-trk-view="overview"][aria-selected="true"]').waitFor();
  await more.focus();
  await page.keyboard.press('ArrowUp');
  assert.equal(await page.locator('.trk-others [data-trk-view="cleanup"]').evaluate(el => el === document.activeElement),true);
  await page.keyboard.press('Escape');
  assert.equal(await more.evaluate(el => el === document.activeElement),true);
  await page.evaluate(() => document.querySelector('[data-trk-slot="controls"]').style.maxWidth = '');
  await page.waitForFunction(() => document.querySelectorAll('.trk-tabs [role="tab"]:not([hidden])').length === 8);
  assert.equal(await page.locator('.trk-overflow').isHidden(),true);
});


test("Mobile tracker uses short previews, deliberate paging and one-time card motion", async t => {
  const {page} = await fixture(t,"flat-dark",{width:390,motion:"no-preference"});
  const cards = page.locator('.trk-overview-card');
  const more = page.locator('[data-trk-overview-more="continue"]');
  assert.equal(await cards.count(),3);
  assert.equal(await cards.evaluateAll(items => new Set(items.map(el => el.offsetTop)).size),1);
  assert.equal(await page.locator('.trk-overview-grid').evaluate(el => el.scrollWidth > el.clientWidth),true);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),true);
  await page.evaluate(() => {
    window.cardEntrances = [];
    document.addEventListener('animationstart', event => {
      if (event.animationName === 'trk-card-reveal') window.cardEntrances.push(event.target.dataset.trkMotionKey);
    });
  });
  await more.click();
  assert.equal(await cards.count(),6);
  const key = await cards.nth(3).getAttribute('data-trk-motion-key');
  await cards.nth(3).scrollIntoViewIfNeeded();
  await page.waitForFunction(key => window.cardEntrances.includes(key),key);
  await more.click();
  await cards.nth(3).scrollIntoViewIfNeeded();
  assert.equal(await page.evaluate(key => window.cardEntrances.filter(value => value === key).length,key),1);
  await page.emulateMedia({reducedMotion:'reduce'});
  await more.click();
  await cards.nth(9).scrollIntoViewIfNeeded();
  assert.equal(await cards.nth(9).evaluate(el => getComputedStyle(el).animationName),'none');
  assert.equal(await cards.nth(9).locator('img').evaluate(el => getComputedStyle(el).transitionDuration),'0s');
  const queries = [];
  await page.route('**/api/tracker/items?**', route => {
    const params = new URL(route.request().url()).searchParams;
    const offset = Number(params.get('offset')), limit = Number(params.get('limit'));
    queries.push({offset,limit});
    return route.fulfill({json:{total:55,items:Array.from({length:Math.min(limit,55-offset)},(_,index) => ({...rows[index % rows.length],key:`movie:${offset+index}`}))}});
  });
  await choose(page,'#trk-mobile-view','history');
  await page.waitForFunction(() => document.querySelector('[data-trk-action="next"]'));
  assert.deepEqual(queries.at(-1),{offset:0,limit:20});
  await page.locator('[data-trk-action="next"]').click();
  await page.waitForFunction(() => document.querySelector('[data-trk-ref-key="movie:20"]'));
  assert.deepEqual(queries.at(-1),{offset:20,limit:20});
  await page.locator('[data-trk-input="size"]').selectOption('10',{force:true});
  await page.waitForFunction(() => document.querySelector('[data-trk-ref-key="movie:0"]'));
  assert.deepEqual(queries.at(-1),{offset:0,limit:10});
  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('cw.tracker.view.v3')));
  assert.equal(saved.mobilePageSize,10);
  assert.equal(saved.pageSize,50);
  await page.locator('[data-trk-action="by-day"]').click();
  await page.waitForFunction(() => document.querySelector('[data-trk-action="more"]'));
  assert.deepEqual(queries.at(-1),{offset:0,limit:10});
});

const pickerProfiles = [{id:"ann",label:"Ann and family",instances:{CROSSWATCH:["other"]}},{id:"bob",label:"Bob",instances:{CROSSWATCH:["default"]}}];
for (const theme of ["original","flat-dark","flat-light"]) for (const width of [320,768,1440]) test(`${theme}: Tracker profile picker at ${width}px`, async t => {
  const {page,boot,scopedReads} = await fixture(t,theme,{width,profiles:pickerProfiles});
  const button = page.locator('#trk-profile + .cw-icon-select button');
  await button.waitFor();
  assert.match(await button.textContent(),/All profiles/);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await button.focus();
  await page.keyboard.press('Enter');
  const menu = page.locator('.cw-icon-select-menu:not(.hidden)');
  await menu.waitFor();
  const bounds = await menu.boundingBox();
  assert.ok(bounds.x>=0 && bounds.x+bounds.width<=width+1);
  if (process.env.CW_PROFILE_PICKER_SCREENSHOTS) {
    await mkdir(process.env.CW_PROFILE_PICKER_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_PROFILE_PICKER_SCREENSHOTS}/${theme}-${width}.png`});
  }
  await page.keyboard.press('Escape');
  assert.equal(await button.evaluate(el=>el===document.activeElement),true);
  await Promise.all([page.waitForEvent('load'),choose(page,'#trk-profile','ann')]);
  await boot();
  assert.equal(await page.locator('#trk-profile').inputValue(),'ann');
  assert.equal(await page.locator('#trk-instance').inputValue(),'other');
  assert.equal(await page.locator('#trk-instance option').count(),1);
  assert.equal(scopedReads.filter(row=>row.path==='/api/tracker/summary').at(-1).viewAs,'ann');
  assert.match(await page.locator('[data-trk-overview-open="airing:0"]').textContent(),/Other tracker show/);
  await page.reload();
  await boot();
  assert.equal(await page.locator('#trk-profile').inputValue(),'ann');
  await Promise.all([page.waitForEvent('load'),choose(page,'#trk-profile','__default__')]);
  await boot();
  assert.equal(await page.locator('#trk-profile').inputValue(),'__default__');
  assert.match(await button.textContent(),/Admin \/ Default instances/);
  assert.equal(await page.locator('#trk-instance').inputValue(),'default');
  const defaultReads = scopedReads.filter(row=>row.path==='/api/tracker/summary').at(-1);
  assert.equal(defaultReads.instance,'default');
  assert.equal(defaultReads.viewAs,'');
  await page.reload();
  await boot();
  assert.equal(await page.locator('#trk-profile').inputValue(),'__default__');
  assert.equal(await page.locator('#trk-instance').inputValue(),'default');
  await Promise.all([page.waitForEvent('load'),choose(page,'#trk-profile','')]);
  await boot();
  assert.equal(await page.locator('#trk-profile').inputValue(),'');
  assert.equal(await page.locator('#trk-instance option').count(),2);
  assert.equal(scopedReads.filter(row=>row.path==='/api/tracker/summary').at(-1).viewAs,'');
});

test('Tracker profile picker respects managed users and an empty profile list', async t => {
  const managed = await fixture(t,'flat-dark',{width:390,profiles:pickerProfiles,managed:true});
  assert.equal(await managed.page.locator('.trk-profile-picker').isVisible(),false);
  assert.equal(await managed.page.locator('#trk-instance').inputValue(),'other');
  assert.equal(await managed.page.evaluate(()=>CW.OverviewProfile.setActive('bob')),false);
  const empty = await fixture(t,'flat-light',{width:390,profiles:[]});
  assert.equal(await empty.page.locator('#trk-profile').isDisabled(),false);
  assert.deepEqual(await empty.page.locator('#trk-profile option').allTextContents(),['All profiles','Admin / Default instances']);
  await Promise.all([empty.page.waitForEvent('load'),choose(empty.page,'#trk-profile','__default__')]);
  await empty.boot();
  assert.equal(await empty.page.locator('#trk-profile').inputValue(),'__default__');
  assert.equal(await empty.page.locator('#trk-instance').inputValue(),'default');
});

test('Tracker reloads the selected profile when returning from another page', async t => {
  const {page,boot,scopedReads} = await fixture(t,'flat-dark',{profiles:pickerProfiles});
  await page.evaluate(()=>{
    document.querySelector('#page-tracker').classList.add('hidden');
    CW.OverviewProfile.setActive('ann');
  });
  assert.equal(await page.locator('#trk-profile').inputValue(),'ann');
  await Promise.all([page.waitForEvent('load'),page.evaluate(()=>{
    document.querySelector('#page-tracker').classList.remove('hidden');
    TrackerPage.resume();
  })]);
  await boot();
  assert.equal(await page.locator('#trk-instance').inputValue(),'other');
  assert.equal(scopedReads.filter(row=>row.path==='/api/tracker/summary').at(-1).viewAs,'ann');
});


for(const theme of ['original','flat-dark','flat-light']) test(`${theme}: Wrapped uses one banner slot, restores monthly and stays available in More`,async t=>{
  const {page,requests}=await fixture(t,theme,{touch:false,wrapped:{year:'2025',posters:[]}});
  await page.locator('[data-trk-wrapped="2025"]').waitFor();
  assert.equal(await page.locator('.trk-month-banner').count(),0);
  assert.equal(requests.includes('/api/tracker/overview/wrapped'),false);
  for(const width of [1440,320]) {
    await page.setViewportSize({width,height:900});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    await mkdir('.tmp/wrapped-audit',{recursive:true});
    await page.locator('.trk-wrapped-banner').screenshot({path:`.tmp/wrapped-audit/banner-${theme}-${width}.png`});
  }
  await page.locator('[data-trk-wrapped-dismiss]').click();
  await page.locator('[data-trk-month]').waitFor();
  assert.equal(await page.locator('.trk-wrapped-banner').count(),0);
  await choose(page,'#trk-mobile-view','wrapped');
  await page.getByText('Your 2025 awaits.',{exact:true}).waitFor();
  assert.equal(await page.locator('#page-tracker>.cw-root').isVisible(),false);
  await page.locator('[data-wr="close"]').click();
  await page.locator('[data-trk-month]').waitFor();
  await page.setViewportSize({width:1440,height:900});
  await page.locator('.trk-others summary').click();await page.locator('[data-trk-wrapped-open]').click();
  await page.locator('.wr-page').waitFor();
  await page.evaluate(async()=>{location.hash='#tracker?view=overview';await TrackerPage.mount(document.querySelector('#page-tracker'));});
  assert.equal(await page.locator('.wr-page').count(),0);
  await page.locator('[data-trk-month]').waitFor();
});

test('Wrapped dismissal is per tracker, profile and year; options hide only the disabled review',async t=>{
  const options={month_review:true,wrapped:true};
  const app=await fixture(t,'flat-dark',{touch:false,wrapped:{year:'2025'},trackerOptions:options});
  const {page}=app;
  await page.locator('[data-trk-wrapped-dismiss]').click();
  await choose(page,'#trk-instance','other');await page.locator('[data-trk-wrapped]').waitFor();
  await choose(page,'#trk-instance','default');await page.locator('[data-trk-month]').waitFor();
  assert.equal(await page.locator('.trk-wrapped-banner').count(),0);
  await page.evaluate(()=>{CW.AuthState.read=()=>({user:{id:'ann'},profileId:'ann',role:'user',permissions:{write:true}});});
  await page.locator('.trk-tabs [data-trk-view="history"]').click();await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  await page.locator('[data-trk-wrapped-dismiss]').click();
  app.wrapped({year:'2026'});
  await page.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));
  await page.locator('[data-trk-wrapped="2026"]').waitFor();
  options.wrapped=false;
  await page.evaluate(()=>TrackerPage.mount(document.querySelector('#page-tracker')));
  await page.locator('[data-trk-month]').waitFor();
  assert.equal(await page.locator('[data-trk-wrapped-open],option[value="wrapped"]').count(),0);
  options.wrapped=true;options.month_review=false;
  await page.evaluate(()=>TrackerPage.mount(document.querySelector('#page-tracker')));
  await page.locator('[data-trk-wrapped="2026"]').waitFor();
  assert.equal(await page.locator('[data-trk-month-open],option[value="month-review"]').count(),0);
});

test('Wrapped bell links open the correct year and tracker through the real router',async t=>{
  const {page}=await fixture(t,'flat-dark',{touch:false});
  await page.addScriptTag({url:'/assets/helpers/core.js'});
  await page.evaluate(()=>window.showTab('tracker'));
  await page.addScriptTag({url:'/assets/helpers/notifications.js'});
  let pending=true;
  await page.route('https://overview.test/api/tracker/overview/wrapped**',route=>{
    const url=new URL(route.request().url());
    return route.fulfill({json:{year:url.searchParams.get('year')||'2025',current_year:'2026',pending,stale:pending,report:pending?null:{plays:0,titles:[],genres:[]}}});
  });
  await choose(page,'#trk-instance','other');
  await page.locator('.trk-others summary').click();await page.locator('[data-trk-wrapped-open]').click();
  await page.getByText('Preparing your premiere.',{exact:true}).waitFor();
  await page.locator('[data-wr-year]').fill('2024');await page.locator('[data-wr-year]').press('Tab');
  await page.waitForFunction(()=>document.querySelector('[data-wr-year]').value==='2024');
  await page.locator('[data-wr="close"]').click();pending=false;
  await page.locator('#cw-notifications-button').click();
  const link=page.locator('.cw-notification[href="#tracker?instance=other&year=2024"]');
  await link.waitFor();
  const request=page.waitForRequest(req=>req.url().includes('/overview/wrapped?')&&req.url().includes('year=2024'));
  await link.click();assert.equal(new URL((await request).url()).searchParams.get('provider_instance'),'other');
  await page.getByText('Your 2024 awaits.',{exact:true}).waitFor();
  assert.equal(await page.locator('[data-wr-year]').inputValue(),'2024');
  assert.equal(await page.locator('.mr-page').count(),0);
});

test('Wrapped closes when the remembered tracker view expires',async t=>{
  const {page}=await fixture(t,'flat-dark',{touch:false,wrapped:{year:'2025'}});
  await page.locator('[data-trk-wrapped="2025"]').click();
  await page.getByText('Your 2025 awaits.',{exact:true}).waitFor();
  const chosenAt=await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.tracker.view.v3')).viewSelectedAt);
  await page.clock.setFixedTime(new Date(chosenAt+3600000));
  await page.evaluate(()=>TrackerPage.mount(document.querySelector('#page-tracker')));
  assert.equal(await page.locator('.wr-page').count(),0);
  assert.equal(await page.locator('#page-tracker>.cw-root').isVisible(),true);
  assert.equal(await page.locator('#trk-mobile-view').inputValue(),'overview');
});

test('Wrapped dismissal storage events update only the matching profile and tracker',async t=>{
  const {page,requests}=await fixture(t,'flat-dark',{touch:false,wrapped:{year:'2025'}});
  const other=await page.context().newPage();
  await other.route('https://overview.test/**',route=>route.fulfill({contentType:'text/html',body:'<!doctype html><title>Other tracker tab</title>'}));
  await other.goto('https://overview.test/other');
  const before=requests.length;
  await other.evaluate(()=>localStorage.setItem('cw-wrapped-dismissed:["other","","","default"]','2025'));
  assert.equal(await page.locator('[data-trk-wrapped="2025"]').count(),1);
  await other.evaluate(()=>localStorage.setItem('cw-wrapped-dismissed:["local","","","default"]','2025'));
  await page.locator('[data-trk-month]').waitFor();
  assert.equal(await page.locator('[data-trk-wrapped]').count(),0);
  await other.evaluate(()=>localStorage.removeItem('cw-wrapped-dismissed:["local","","","default"]'));
  await page.locator('[data-trk-wrapped="2025"]').waitFor();
  await page.locator('[data-trk-wrapped-dismiss]').click();
  await page.locator('[data-trk-month]').waitFor();
  await other.evaluate(()=>localStorage.clear());
  await page.locator('[data-trk-wrapped="2025"]').waitFor();
  assert.equal(requests.length,before);
});

for (const width of [1440,390]) test(`Tracker grid action menus preserve editing and artwork navigation at ${width}px`, async t => {
  const {page} = await fixture(t,"flat-dark",{width,touch:width < 760});
  await page.route("**/api/tracker/shows?**", route => route.fulfill({json:{shows:[{...rows[0],key:"show:1",watched:2,total:10}],total:1}}));
  await page.route("**/api/tracker/notes?**", route => route.fulfill({json:{notes:[rows[0]],total:1}}));
  await page.route("**/api/tracker/items?**", route => route.fulfill({json:{items:rows.map(row => ({...row,date_field:"watched_at"})),total:rows.length}}));
  const menu = page.locator('.cw-card-action-menu:popover-open');
  for (const [view,label,field] of [["history","Edit watch","date"],["progress","Edit progress","progress_percent"],["ratings","Edit rating","rating"],["watchlist","Edit item","title"],["collection","Edit item","title"],["shows","Edit title",null],["titles","My notes",null]]) {
    if (width < 760) await choose(page,'#trk-mobile-view',view);
    else if (await page.locator(`.trk-tabs [data-trk-view="${view}"]`).isVisible()) await page.locator(`.trk-tabs [data-trk-view="${view}"]`).click();
    else {
      await page.locator('.trk-others summary').click();
      await page.locator(`.trk-others [data-trk-view="${view}"]`).click();
    }
    await page.locator('[data-trk-layout="grid"]').click();
    const card = page.locator('.trk-grid-card').first();
    const trigger = card.locator('[data-trk-card-menu]');
    await card.waitFor();
    const before = await card.boundingBox();
    await trigger.click();
    await menu.waitFor();
    assert.equal(await menu.getByRole('menuitem',{name:label,exact:true}).count(),1);
    const bounds = await menu.boundingBox();
    assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= width && bounds.y >= 0 && bounds.y + bounds.height <= 900);
    assert.deepEqual(await card.boundingBox(),before);
    assert.notEqual(await menu.evaluate(el => getComputedStyle(el).backgroundColor),'rgba(0, 0, 0, 0)');
    await page.keyboard.press('End');
    assert.equal(await page.evaluate(() => document.activeElement.textContent.trim()),'do_not_disturb_onMark as dropped');
    await page.keyboard.press('Escape');
    assert.equal(await trigger.evaluate(el => el === document.activeElement),true);
    if (field) {
      await trigger.click();
      await menu.getByRole('menuitem',{name:label,exact:true}).click();
      await page.locator(`.trk-sheet:visible [data-trk-edit="${field}"]`).waitFor({state:"attached"});
      await page.keyboard.press('Escape');
    }
    await page.evaluate(() => {window.openedTitle = null;});
    await card.locator('[data-trk-card-open]').click();
    await page.waitForFunction(() => !!window.openedTitle?.options.tracker);
    await card.locator('[data-trk-pick]').check();
    assert.equal(await page.locator('.trk-grid-card').first().getAttribute('aria-selected'),'true');
    await page.locator('.trk-grid-card').first().locator('[data-trk-pick]').uncheck();
  }
  await page.evaluate(() => {window.CW.AuthState.read = () => ({permissions:{write:false}});});
  await page.locator('.trk-grid-card').first().locator('[data-trk-card-menu]').click();
  assert.equal(await menu.locator('button:not(:disabled)').count(),1);
  await page.keyboard.press('End');
  assert.equal(await page.evaluate(() => document.activeElement.textContent.trim()),'infoView details');
  await page.keyboard.press('Escape');
  if (process.env.CW_TRACKER_ACTION_SCREENSHOTS) {
    await page.evaluate(() => {window.CW.AuthState.read = () => ({permissions:{write:true}});});
    if (width < 760) await choose(page,'#trk-mobile-view','history');
    else await page.locator('.trk-tabs [data-trk-view="history"]').click();
    await page.locator('.trk-grid-card').first().locator('[data-trk-card-menu]').click();
    await mkdir(process.env.CW_TRACKER_ACTION_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_TRACKER_ACTION_SCREENSHOTS}/tracker-actions-${width}.png`});
  }
});
