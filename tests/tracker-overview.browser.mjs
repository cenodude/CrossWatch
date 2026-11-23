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

test('Airing cards retain artwork focus on refresh and avoid incomplete catch-up totals', async t => {
  const season_progress = {complete:false,episodes:[{episode:1,status:'unwatched',runtime:58},{episode:2,status:'upcoming',air_date:'2026-11-12'}]};
  const {page} = await fixture(t,'flat-dark',{touch:false,airing:[{...episodes[0],season_progress}]});
  const card = page.locator('.cw-airing-card').first();
  await card.locator('.cw-airing-front').focus();
  await card.evaluate(node=>{
    const host=node.parentElement;
    const saved=CW.AiringCard.capture(host);
    node.replaceWith(node.cloneNode(true));
    CW.AiringCard.restore(host,saved);
  });
  assert.equal(await card.locator('.cw-airing-front').evaluate(node=>node===document.activeElement),true);
  assert.equal(await card.getAttribute('data-airing-open'),null);
  await page.keyboard.press('Enter');
  assert.equal(await card.locator('.cw-airing-catchup strong').textContent(),'Status unavailable');
  assert.equal(await card.locator('.cw-airing-catchup>span').textContent(),'Episode details below');
});

for (const width of [390,1440]) test(`Airing cards retain keyboard focus when selecting and paging episodes at ${width}px`, async t => {
  const season_progress = {complete:true,episodes:Array.from({length:20},(_,i)=>({episode:i+1,status:i<4?'watched':i<18?'unwatched':'upcoming',runtime:25,air_date:'2026-11-12'}))};
  const {page} = await fixture(t,'flat-dark',{width,touch:width===390,airing:[{...episodes[0],season_progress}]});
  const card = page.locator('.cw-airing-card').first();
  await card.locator('.cw-airing-front').focus();
  await page.keyboard.press('Enter');
  await card.locator('[data-airing-select="5"]').focus();
  await page.keyboard.press('Enter');
  assert.equal(await card.getAttribute('data-airing-open'),'');
  assert.equal(await card.locator('[data-airing-select="5"]').evaluate(node=>node===document.activeElement),true);
  await card.locator('[data-airing-step="1"]').focus();
  await page.keyboard.press('Enter');
  assert.equal(await card.getAttribute('data-airing-open'),'');
  assert.equal(await card.locator('[data-airing-step="1"]').evaluate(node=>node===document.activeElement),true);
  await card.locator('[data-airing-select="8"]').focus();
  await page.keyboard.press('Enter');
  assert.equal(await card.locator('.cw-airing-next-copy').textContent(),'Episode 8 · 25 min');
  assert.equal(await card.locator('[data-airing-select="8"]').evaluate(node=>node===document.activeElement),true);
  await page.keyboard.press('Escape');
  assert.equal(await card.getAttribute('data-airing-open'),null);
  assert.equal(await card.locator('.cw-airing-front').evaluate(node=>node===document.activeElement),true);
  await page.keyboard.press('Enter');
  await page.locator('.trk-tabs [data-trk-view="overview"]').focus();
  assert.equal(await card.getAttribute('data-airing-open'),null);
});

test('Airing flip supports long seasons and retains selection during background updates', async t => {
  const season_progress = {complete:true,episodes:Array.from({length:20},(_,i)=>({episode:i+1,status:i<4?'watched':i<18?'unwatched':'upcoming',runtime:25,air_date:'2026-11-12'}))};
  const app = await fixture(t,'flat-dark',{width:1440,touch:false,clock:true,motion:'no-preference',airing:[{...episodes[0],season_progress}]});
  const {page,requests} = app;
  const card = page.locator('.cw-airing-card').first();
  await card.locator('.cw-airing-front').click();
  await page.clock.runFor(500);
  assert.equal(await card.locator('[data-airing-select]').count(),6);
  const before=requests.length;
  await card.getByRole('button',{name:'Next episodes',exact:true}).click();
  await card.getByRole('button',{name:'Episode 8, unwatched',exact:true}).click();
  assert.equal(await card.locator('.cw-airing-next-copy').textContent(),'Episode 8 · 25 min');
  assert.equal(requests.length,before);
  await card.evaluate(node=>{
    const host=node.parentElement;
    const saved=CW.AiringCard.capture(host);
    node.replaceWith(node.cloneNode(true));
    CW.AiringCard.restore(host,saved);
  });
  assert.equal(await card.getAttribute('data-airing-open'),'');
  assert.equal(await card.locator('[data-airing-select="8"]').getAttribute('aria-pressed'),'true');
  await card.getByRole('button',{name:'Next episodes',exact:true}).click();
  await card.getByRole('button',{name:'Next episodes',exact:true}).click();
  assert.equal(await card.getByRole('button',{name:'Next episodes',exact:true}).isDisabled(),true);
  assert.equal(await card.locator('[data-airing-select]').count(),2);
  await card.getByRole('button',{name:'Episode 19, upcoming',exact:true}).click();
  assert.equal(await card.locator('.cw-airing-next-state').textContent(),'UPCOMING');
  assert.match(await card.locator('.cw-airing-next-copy').textContent(),/Episode 19.*12/);
});

for (const theme of ['original','flat-dark','flat-light']) for (const width of [390,1440]) test(`${theme} ${width}: airing flip cards show season progress at their real size`, async t => {
  const season_progress = {complete:true,episodes:Array.from({length:8},(_,i)=>({episode:i+1,title:`Episode ${i+1}`,status:i<4?'watched':i<6?'unwatched':'upcoming',runtime:58,air_date:i<6?'2026-11-09':'2026-11-12'}))};
  const {page} = await fixture(t,theme,{width,touch:width===390,clock:true,airing:[{...episodes[0],title:'The Rings of Power',season:3,episode:7,season_progress}]});
  const card = page.locator('.cw-airing-card').first();
  await card.scrollIntoViewIfNeeded();
  const before = await card.boundingBox();
  if(width===390) await card.locator('.cw-airing-front').tap();
  else {
    await card.hover();
    await page.clock.runFor(400);
    assert.equal(await card.getAttribute('data-airing-open'),null);
    await page.mouse.move(0,0);
    await page.clock.runFor(100);
    assert.equal(await card.getAttribute('data-airing-open'),null);
    await card.hover();
    await page.clock.runFor(450);
  }
  assert.equal(await card.getAttribute('data-airing-open'),'');
  assert.equal(await card.locator('.cw-airing-ring strong').textContent(),'4/6');
  assert.equal(await card.locator('.cw-airing-ring small').count(),0);
  assert.match(await card.locator('.cw-airing-catchup').innerText(),/2 to catch up/);
  assert.match(await card.locator('.cw-airing-catchup').innerText(),/1h 56m/);
  const boxes = await card.evaluate(node=>{
    const selectors=['.cw-airing-title','.cw-airing-summary','.cw-airing-season','.cw-airing-episodes','.cw-airing-next'];
    return selectors.map(selector=>{const r=node.querySelector(selector).getBoundingClientRect();return {top:r.top,bottom:r.bottom};});
  });
  for(let i=1;i<boxes.length;i++) assert.ok(boxes[i].top>=boxes[i-1].bottom-1,`${theme} ${width}: overlap at ${i}`);
  assert.ok(boxes.at(-1).bottom <= before.y+before.height);
  assert.equal((await card.boundingBox()).height,width===390?190:210);
  await card.getByRole('button',{name:'Episode 4, watched: Episode 4',exact:true}).click();
  assert.equal(await card.locator('.cw-airing-next-state').textContent(),'WATCHED');
  assert.equal(await card.locator('.cw-airing-next-copy').textContent(),'Episode 4 · 58 min');
  await mkdir('.tmp/airing-flip',{recursive:true});
  await card.screenshot({path:`.tmp/airing-flip/${theme}-${width}.png`});
  await page.keyboard.press('Escape');
  assert.equal(await card.getAttribute('data-airing-open'),null);
  await card.locator('.cw-airing-front').focus();
  await page.keyboard.press('Enter');
  assert.equal(await card.getAttribute('data-airing-open'),'');
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: mobile More follows Tracker views and restores default destinations`, async t => {
  const {page} = await fixture(t,theme,{width:390,trackerOptions:{month_review:false,wrapped:false}});
  const more = page.getByRole('button',{name:'More',exact:true});
  await more.click();
  const menu = page.locator('.cw-mobile-context-destinations');
  assert.equal(await menu.getByRole('button',{name:'Watchlist',exact:true}).isVisible(),true);
  if (process.env.CW_MENU_SCREENSHOTS) {
    await mkdir(process.env.CW_MENU_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-tracker-context.png`});
  }
  assert.equal(await menu.getByRole('button',{name:'Month in review',exact:true}).count(),0);
  assert.equal(await menu.getByRole('button',{name:'CrossWatch Wrapped',exact:true}).count(),0);
  await menu.getByRole('button',{name:'Ratings',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.trk-tabs [data-trk-view="ratings"]').classList.contains('active'));
  assert.equal(await more.getAttribute('aria-expanded'),'false');
  await more.click();
  assert.equal(await menu.getByRole('button',{name:'Ratings',exact:true}).getAttribute('aria-current'),'page');
  assert.equal(await menu.evaluate(node=>[...node.children].every(item=>item.getBoundingClientRect().right<=innerWidth)),true);
  await page.keyboard.press('Escape');
  await page.evaluate(()=>{location.hash='main';});
  await more.click();
  assert.equal(await page.locator('#tab-snapshots').isVisible(),true);
  assert.equal(await menu.getByRole('button').count(),0);
});

async function fixture(t, theme, {realMedia = false, playback = false, touch = true, width = 1440, motion = "reduce", profiles = null, managed = false, airing = episodes, continuing = rows, next = episodes.slice(0,2), wrapped = {}, trackerOptions = {}, clock = false} = {}) {
  const browser = await ({chromium,webkit})[process.env.CW_AUDIT_BROWSER || "chromium"].launch({headless:true});
  t.after(() => browser.close());
  const context = await browser.newContext({viewport:{width,height:900},hasTouch:touch,reducedMotion:motion});
  const page = await context.newPage();
  if (clock) await page.clock.install({time:new Date("2026-11-10T12:00:00Z")});
  else await page.clock.setFixedTime(new Date("2026-11-10T12:00:00Z"));
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
    if (path === '/api/watch/currently_watching') json = {streams:[]};
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
      return route.fulfill({json});
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
  const boot = async ({wait = true, account = "admin", scope = "", permissions = {write:true}} = {}) => {
    await page.evaluate(managed => { document.documentElement.dataset.cwRole = managed ? "user" : "admin"; window.CW = {AuthState:{read:()=>({isAdmin:!managed,permissions:{write:true}})},ProfileMediaModal:{canOpen:item=>!!item.ids?.tmdb,open:(item,options)=>{window.openedTitle={item,options};}}}; },managed);
    await page.evaluate(({account,scope,permissions}) => {
      const read = window.CW.AuthState.read;
      window.CW.AuthState.read = () => ({...read(),user:{id:account},permissions});
      if (scope) window.CW.OverviewProfile = {id:scope,profiles:[],isAdmin:true};
    }, {account,scope,permissions});
    if (realMedia) {
      await page.evaluate(() => {
        const meta = item => ({title:item.title,year:2026,genres:[],detail:{},overview:"Media card overview"});
        window.CW.Meta = {peek:meta,get:async item => meta(item)};
      });
      await page.addScriptTag({url:'/assets/js/profile-media-modal.js'});
    }
    if (playback) for (const file of ['helpers/playing-card','js/playingcard']) await page.addScriptTag({url:`/assets/${file}.js`});
    for (const file of ["helpers/view-cache","helpers/account-menu","helpers/icon-select","helpers/selection-bar","helpers/card-actions", "helpers/airing-card","js/tracker"]) await page.addScriptTag({url:`/assets/${file}.js`});
    if (profiles !== null) for (const file of ['helpers/api','helpers/profile-select','js/overview-profile']) await page.addScriptTag({url:`/assets/${file}.js`});
    if (!wait) { await page.evaluate(() => { void TrackerPage.mount(document.querySelector("#page-tracker")); }); return; }
    await page.evaluate(() => TrackerPage.mount(document.querySelector("#page-tracker")));
    await page.locator('[data-trk-overview-open="airing:0"]').waitFor();
    await page.evaluate(() => document.fonts.ready);
  };
  await boot();
  return {page,boot,scopedReads,writes,requests,drops,wrapped:value=>{wrapped=value;},reviewMonth:value=>{reviewMonth=value;},airing:value=>{airing=value;},empty:value=>{empty=value;},fail:value=>{fail=value;},metadata:value=>{metadata=value;},hold:()=>{hold=new Promise(resolve=>{release=resolve;});},release:()=>{hold=null;release?.();}};
}

async function choose(page, selector, value) {
  if (selector === '#trk-instance' || selector === '[data-trk-input="instance"]') {
    await page.locator('.trk-source > summary').click();
    await page.locator(`[data-trk-instance="${value}"]`).click();
    return;
  }
  if (selector === '#trk-mobile-view') {
    if (value === 'shows') {
      await page.locator('.trk-tabs [data-trk-view="history"]').click();
      await page.locator('[data-trk-action="filters"]').click();
      await choose(page,'[data-trk-input="history-group"]','title');
      await page.locator('[data-trk-history-media="show"]').click();
      await page.keyboard.press('Escape');
    } else if (['month-review','wrapped','cleanup'].includes(value)) {
      await page.locator('.trk-others summary').click();
      await page.locator(value === 'cleanup' ? '.trk-others [data-trk-view="cleanup"]' : value === 'wrapped' ? '[data-trk-wrapped-open]' : '[data-trk-month-open]').click();
    } else await page.locator(`.trk-tabs [data-trk-view="${value}"]`).click();
    return;
  }
  const button = page.locator(`${selector} + .cw-icon-select .cw-icon-select-btn`);
  if (await button.evaluate(el => !!el.closest('.trk-others-menu')) && !await button.isVisible()) await page.locator('.trk-others summary').click();
  await button.click();
  await page.locator(`.cw-icon-select-menu:not(.hidden) [data-value="${value}"]`).click();
}

for(const theme of ['original','flat-dark','flat-light']) test(`Tracker list cache and sync feature colors in ${theme}`,async t=>{
  const {page}=await fixture(t,theme,{width:390});
  for(const [view,feature] of [['watchlist','watchlist'],['history','history'],['ratings','ratings'],['progress','progress'],['collection','collection']]) {
    await page.locator(`.trk-tabs [data-trk-view="${view}"]`).click();
    await page.waitForFunction(view=>document.querySelector(`.trk-tabs [data-trk-view="${view}"]`)?.classList.contains('active'),view);
    await page.waitForFunction(({view,feature})=>{
      const el=document.querySelector(`.trk-tabs [data-trk-view="${view}"]`);
      const probe=document.createElement('span');probe.style.color=`rgb(var(--cw-feature-${feature}-rgb))`;document.body.append(probe);
      const expected=getComputedStyle(probe).color;probe.remove();return getComputedStyle(el).borderBottomColor===expected;
    },{view,feature});
  }
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('[data-trk-layout="grid"]').click();
  await page.locator('.trk-grid-card').first().waitFor();
  await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  let release;
  const gate=new Promise(resolve=>{release=resolve;});t.after(()=>release());
  await page.route('**/api/tracker/items?**',async route=>{await gate;await route.fulfill({json:{items:rows,total:rows.length}});});
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('.trk-grid-card').first().waitFor();
  assert.equal(await page.getByText('Loading your history...',{exact:true}).count(),0);
  release();
});

for (const width of [390,1440]) test(`Tracker restores its overview before slow summary requests at ${width}px`, async t => {
  const {page,boot,requests} = await fixture(t, "flat-dark", {width,playback:true});
  await page.waitForFunction(() => JSON.parse(sessionStorage.getItem("cw.tracker.overview.v1") || "[]").length > 0);
  await page.evaluate(()=>{
    const key='cw.tracker.overview.v1',entries=JSON.parse(sessionStorage.getItem(key));
    entries.forEach(entry=>{entry.t=Date.now()-15*60*1000;});
    sessionStorage.setItem(key,JSON.stringify(entries));
  });
  let release, started;
  const gate = new Promise(resolve => { release = resolve; });
  const pending = new Promise(resolve => { started = resolve; });
  t.after(() => release());
  await page.route("**/api/tracker/summary?**", async route => {
    started();
    await gate;
    await route.fallback();
  });
  await page.reload();
  requests.length = 0;
  await boot({wait:false});
  await pending;
  await page.locator('[data-trk-overview-open="continue:0"]').waitFor();
  assert.match(await page.locator('.trk-overview').innerText(), /Movie 1/);
  assert.equal(requests.includes('/api/tracker/overview'), false);
  await page.waitForFunction(() => document.querySelector('.trk-hero-title') || document.querySelector('.trk-overview'));
  release();
  await page.waitForResponse(response => new URL(response.url()).pathname === '/api/tracker/overview');
  await page.locator('[data-trk-overview-open="airing:0"]').waitFor();
});

for (const invalidation of ['account','profile','permissions','instance','expired','corrupt']) test(`Tracker rejects overview snapshots after ${invalidation} changes`, async t => {
  const {page,boot} = await fixture(t, "flat-dark");
  await page.waitForFunction(() => JSON.parse(sessionStorage.getItem("cw.tracker.overview.v1") || "[]").length > 0);
  await page.evaluate(invalidation => {
    const key = 'cw.tracker.overview.v1';
    if (invalidation === 'expired') {
      const entries = JSON.parse(sessionStorage.getItem(key));
      entries.forEach(entry => { entry.t = Date.now() - 25 * 60 * 60 * 1000; });
      sessionStorage.setItem(key,JSON.stringify(entries));
    }
    if (invalidation === 'corrupt') sessionStorage.setItem(key,'invalid json');
    if (invalidation === 'instance') localStorage.setItem('cw.tracker.view.v3',JSON.stringify({instance:'other',view:'overview'}));
  },invalidation);
  let release, started;
  const gate = new Promise(resolve => { release = resolve; });
  const pending = new Promise(resolve => { started = resolve; });
  t.after(() => release());
  await page.route("**/api/tracker/summary?**", async route => { started(); await gate; await route.fallback(); });
  await page.reload();
  await boot({wait:false,account:invalidation === 'account' ? 'another-user' : 'admin',scope:invalidation === 'profile' ? 'family' : '',permissions:invalidation === 'permissions' ? {write:false} : {write:true}});
  await pending;
  assert.equal(await page.locator('[data-trk-overview-open="continue:0"]').count(),0);
  release();
  await page.locator('[data-trk-overview-open="continue:0"]').waitFor();
});

for (const width of [390,1440]) test(`Tracker shared navigation order and icons at ${width}px`, async t => {
  const {page} = await fixture(t, 'flat-dark', {width});
  const tabs = page.locator('.trk-tabs [data-trk-view]');
  assert.deepEqual(await tabs.evaluateAll(nodes => nodes.map(node => node.dataset.trkView)), ['overview','watchlist','history','ratings','progress','collection','titles']);
  assert.deepEqual(await tabs.locator('.material-symbol').allTextContents(), ['space_dashboard','bookmark','history','star','play_circle','video_library','bookmarks']);
  if (width > 760) {
    const style = await tabs.first().locator('.material-symbol').evaluate(el => ({size:getComputedStyle(el).fontSize,axes:getComputedStyle(el).fontVariationSettings}));
    assert.equal(style.size, '19px');
    assert.match(style.axes, /"FILL" 0/);
  }
  await page.locator('.trk-tabs [data-trk-view="watchlist"]').click();
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'), 'watchlist');
  await mkdir('.tmp/shared-navigation', {recursive:true});
  await page.locator('.trk-navigation').screenshot({path:`.tmp/shared-navigation/tracker-${width}.png`});
});

test("selected History, Shows and My titles tabs show their loaded totals", async t => {
  const {page} = await fixture(t,"flat-dark",{width:1440});
  await page.route('**/api/tracker/items?**', route => route.fulfill({json:{items:[rows[0]],total:12345}}));
  await page.route('**/api/tracker/shows?**', route => route.fulfill({json:{shows:[],total:10000}}));
  await page.route('**/api/tracker/notes?**', route => route.fulfill({json:{notes:[],total:1200000}}));
  for (const [view,total,compact] of [['history',12345,'12.3K'],['shows',10000,'10K'],['titles',1200000,'1.2M']]) {
    await choose(page,'#trk-mobile-view',view);
    const badge = page.locator(`.trk-tabs [data-trk-view="${view === 'shows' ? 'history' : view}"] .trk-tab-count`);
    await badge.filter({hasText:compact}).waitFor();
    const exact = await page.evaluate(value=>value.toLocaleString(),total);
    assert.equal(await badge.getAttribute('title'),exact);
    assert.equal(await badge.getAttribute('aria-label'),exact);
    assert.equal(await page.locator('.trk-tabs .trk-tab-count').count(),1);
  }
  await page.route('**/api/tracker/items?**', route => route.fulfill({json:{items:[rows[0]],total:2}}));
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('[data-trk-action="filters"]').click();
  await choose(page,'[data-trk-input="history-group"]','watch');
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
    const mobile = innerWidth <= 760;
    const bounds = el.getBoundingClientRect();
    return {inside:copy.left>=bounds.left && copy.right<=bounds.right && copy.bottom<=bounds.bottom && (mobile ? copy.top>=art.bottom : copy.bottom<=art.bottom),clear:badge.bottom<=copy.top && actions.right<=badge.left && (mobile || copy.right<=arrow.left),overflow:el.scrollWidth>el.clientWidth+1};
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
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'),'history');
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
  await page.addScriptTag({url:'/assets/helpers/airing-card.js'});
  await page.addScriptTag({url:'/assets/js/tracker.js'});
  await page.evaluate(()=>TrackerPage.mount(document.querySelector('#page-tracker')));
  await page.locator('.mr-page').waitFor();
  await page.locator('.mr-nav [data-mr="back"]').click();
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'),'history');
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
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'), 'overview');
  assert.equal(new URL(page.url()).hash, '#tracker');
  if (width < 760) await choose(page,'#trk-mobile-view','month-review');
  else {
    await page.locator('.trk-others summary').click();
    await page.locator('[data-trk-month-open]').click();
  }
  await page.locator('.mr-page').waitFor();
  await openOverview();
  assert.equal(await page.locator('.mr-page').count(), 0);
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'), 'overview');
  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('cw.tracker.view.v3')));
  assert.equal(saved.view, 'overview');
  assert.equal(saved.monthlyReview, false);
  await page.evaluate(() => {
    const saved = JSON.parse(localStorage.getItem('cw.tracker.view.v3'));
    localStorage.setItem('cw.tracker.view.v3', JSON.stringify({...saved, view:'history', monthlyReview:true}));
  });
  await page.reload();
  await page.addScriptTag({url:'/assets/helpers/airing-card.js'});
  await page.addScriptTag({url:'/assets/js/tracker.js'});
  await openOverview();
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'), 'overview');
  assert.equal(await page.locator('.mr-page').count(), 0);
});

for (const width of [390,1440]) test(`Tracker navigation expires the selected view after one hour at ${width}px`,async t => {
  const {page}=await fixture(t,'flat-dark',{width});
  await page.addScriptTag({url:'/assets/helpers/core.js'});
  await page.evaluate(()=>window.showTab('tracker'));
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'),'overview');
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
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'),'history');
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.tracker.view.v3')).viewSelectedAt),chosenAt);
  await page.clock.setFixedTime(new Date(chosenAt+3600000));
  await page.locator(width < 760 ? '#cw-mobile-bottom-nav [data-mobile-route="tracker"]' : '#tab-tracker').click();
  await page.waitForFunction(()=>document.querySelector('.trk-tabs [aria-selected="true"]')?.dataset.trkView==='overview');
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
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'),'overview');
  await page.reload();
  await page.addScriptTag({url:'/assets/helpers/airing-card.js'});
  await page.addScriptTag({url:'/assets/js/tracker.js'});
  await page.evaluate(()=>TrackerPage.mount(document.querySelector('#page-tracker')));
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'),'overview');
});

test("Tracker navigation and badge selector share one flat row in every theme",async t => {
  await mkdir('.tmp/tracker-navigation', {recursive:true});
  for (const theme of ['original','flat-dark','flat-light']) {
    const {page,empty}=await fixture(t,theme,{touch:false});
    const panels=await page.evaluate(()=>{
      const box=selector=>{const r=document.querySelector(selector).getBoundingClientRect();return {top:r.top,height:r.height};};
      return {nav:box('.trk-navigation'),tracker:box('[data-trk-slot="source"]')};
    });
    assert.ok(Math.abs(panels.nav.top-panels.tracker.top)<1,`${theme}: ${JSON.stringify(panels)}`);
    assert.ok(Math.abs(panels.nav.height-panels.tracker.height)<=1,`${theme}: ${JSON.stringify(panels)}`);
    const tabs=await page.evaluate(()=>{
      const center=selector=>{const r=document.querySelector(selector).getBoundingClientRect();return r.top+r.height/2;};
      return {tab:center('.trk-tabs .trk-tab span:last-child'),others:center('.trk-others>summary span:nth-child(2)')};
    });
    assert.ok(Math.abs(tabs.tab-tabs.others)<1,`${theme}: ${JSON.stringify(tabs)}`);
    const style = await page.evaluate(() => {
      const properties = ['backgroundColor','backgroundImage','borderRadius','color','fontSize','fontWeight'];
      const read = selector => Object.fromEntries(properties.map(key => [key,getComputedStyle(document.querySelector(selector))[key]]));
      return {more:read('.trk-others > summary'),source:read('.trk-source > summary')};
    });
    assert.deepEqual(style.source,style.more);
    assert.equal(style.source.backgroundColor,'rgba(0, 0, 0, 0)');
    assert.equal(await page.locator('.trk-source > summary .trk-source-badge').textContent(),'badge');
    await page.screenshot({path:`.tmp/tracker-navigation/${theme}-desktop.png`});
    const source = page.locator('.trk-source > summary');
    await source.focus();
    await page.keyboard.press('ArrowDown');
    await page.keyboard.press('End');
    assert.equal(await page.locator('[data-trk-instance="other"]').evaluate(el=>el===document.activeElement),true);
    await page.keyboard.press('Enter');
    await page.waitForFunction(()=>document.querySelector('#trk-instance')?.value==='other');
    await page.locator('.trk-source > summary[aria-label="Tracker: Second tracker"]').waitFor();
    await source.click();
    await page.screenshot({path:`.tmp/tracker-navigation/${theme}-menu.png`});
    await page.keyboard.press('Escape');
    assert.equal(await source.evaluate(el=>el===document.activeElement),true);
    await source.click();
    await page.locator('[data-trk-instance="default"]').evaluate(el=>el.disabled=true);
    await page.keyboard.press('ArrowDown');
    assert.equal(await page.locator('[data-trk-instance="other"]').evaluate(el=>el===document.activeElement),true);
    await page.screenshot({path:`.tmp/tracker-navigation/${theme}-disabled.png`});
    await page.keyboard.press('Escape');
    empty(true);
    await page.locator('[data-trk-action="refresh"]').click();
    await page.getByText('Your recent watches will appear here once you log or sync them.',{exact:true}).waitFor();
    await page.screenshot({path:`.tmp/tracker-navigation/${theme}-empty.png`});
    await page.setViewportSize({width:390,height:844});
    assert.ok(await page.locator('.trk-source > summary').isVisible());
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    await source.click();
    await page.screenshot({path:`.tmp/tracker-navigation/${theme}-mobile-menu.png`});
    const menu = await page.locator('.trk-source-options').boundingBox();
    assert.ok(menu.x>=0 && menu.x+menu.width<=390);
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
    assert.equal(await dashboard.isVisible(),width > 760);
    assert.equal(await page.locator('#recent-history-widget').isVisible(),width > 760);
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
  await page.addScriptTag({url:'/assets/js/notification-feed.js'});
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
  await page.route('https://overview.test/api/profile/notifications?**',route=>route.fulfill({json:{items:[{id:'month:other:2025-08',source:'tracker',title:'Your monthly review is ready',detail:'August 2025',href:'#tracker?instance=other&month=2025-08'}]}}));
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
  assert.equal(await page.locator("[data-trk-slot=filters]").isVisible(),false);
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
    assert.equal(await page.locator('.trk-source > summary').isVisible(),true,`${theme} ${width}x${height}: Tracker selector`);
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
    await page.locator('.cw-airing-card').filter({has:page.locator('[data-trk-overview-open="airing:11"]')}).locator('.cw-airing-front').focus();
    await page.keyboard.press('Enter');
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
  assert.match(await page.locator('.trk-others [data-trk-view="cleanup"]').textContent(), /Clean up2/);
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
  assert.equal(await page.locator('.cw-airing-card').filter({has:page.locator('[data-trk-overview-open="airing:0"]')}).locator('.trk-calendar-copy strong').textContent(),"Other tracker show");
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
    if (kind === 'airing') { await page.locator('.cw-airing-front').first().click(); await target.click(); }
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

test("unchanged background refresh keeps Tracker cards and an open episode card mounted", async t => {
  const {page} = await fixture(t, "flat-dark", {touch:false});
  await page.locator('.cw-airing-front').first().click();
  const statistics = page.waitForResponse(response => new URL(response.url()).pathname === '/api/tracker/stats');
  await page.evaluate(() => {
    window.retainedCards = [...document.querySelectorAll('.trk-overview-card,.trk-grid-card,.cw-airing-card')];
    window.retainedFront = document.querySelector('.cw-airing-front');
    window.dispatchEvent(new CustomEvent('cw:view-updated', {detail:{url:'/api/tracker/stats?provider_instance=default'}}));
  });
  await statistics;
  await page.waitForTimeout(100);
  assert.equal(await page.evaluate(() => retainedCards.every(node => node.isConnected)), true);
  assert.equal(await page.locator('.cw-airing-card').first().getAttribute('data-airing-open'), '');
  const shows = page.waitForResponse(response => new URL(response.url()).pathname === '/api/tracker/overview/shows');
  await page.evaluate(() => window.dispatchEvent(new CustomEvent('cw:view-updated', {detail:{url:'/api/tracker/overview/shows?provider_instance=default'}})));
  await shows;
  await page.waitForTimeout(100);
  assert.equal(await page.evaluate(() => retainedCards.every(node => node.isConnected)), true);
  assert.equal(await page.evaluate(() => retainedFront === document.querySelector('.cw-airing-front')), true);
  const overview = page.waitForResponse(response => new URL(response.url()).pathname === '/api/tracker/overview');
  await page.evaluate(() => window.dispatchEvent(new CustomEvent('cw:view-updated', {detail:{url:'/api/tracker/overview?provider_instance=default'}})));
  await overview;
  await page.waitForTimeout(100);
  assert.equal(await page.evaluate(() => retainedCards.every(node => node.isConnected)), true);
});

test("background episode refresh preserves cards and scroll, retries and resumes after navigation", async t => {
  const {page, boot} = await fixture(t,"flat-dark",{touch:false});
  let phase = "cached", reads = 0, retry = false;
  const cached = {continue:episodes.slice(0,2), airing:episodes, total:12, checked:12, done:true};
  await page.route('**/api/tracker/overview/shows?**', route => {
    const url = new URL(route.request().url());
    assert.equal(url.searchParams.get('background'), '1');
    reads++;
    retry ||= url.searchParams.has('retry_view');
    return route.fulfill({json:phase === "cold" ? {ok:true,_view:{ready:false,refreshing:true,retry_after:1}} : {
      ...cached, airing:phase === "fresh" ? episodes.map((row,index)=>index === 1 ? {...row,title:"Updated show"} : row) : episodes,
      _view:{ready:true,refreshing:phase === "cached",version:phase,error:phase === "failed" ? "refresh_failed" : null,retry_after:1},
    }});
  });
  await page.locator('[data-trk-action="refresh"]').click();
  await page.waitForFunction(() => document.querySelector('[data-trk-overview-open="airing:0"]'));
  await page.waitForTimeout(150);
  await page.evaluate(() => {
    window.recentCard = document.querySelector('[data-trk-overview-open="recent:0"]');
    window.continueCard = document.querySelector('[data-trk-overview-open="continue:0"]');
    window.calendarNode = document.querySelector('.trk-calendar');
    window.firstRelease = document.querySelector('[data-trk-overview-open="airing:0"]');
    window.calendarNode.scrollLeft = 60;
    document.querySelectorAll('.cw-airing-front')[1].focus({preventScroll:true});
  });
  assert.equal(await page.locator('[data-view-status]:visible').count(),0);
  phase = "fresh";
  await page.locator('.trk-calendar-copy strong').filter({hasText:"Updated show"}).waitFor();
  assert.equal(await page.locator('[data-airing-key="episode:1"] .cw-airing-front').evaluate(node=>node===document.activeElement),true);
  assert.deepEqual(await page.evaluate(() => ({
    recent:window.recentCard === document.querySelector('[data-trk-overview-open="recent:0"]'),
    continuing:window.continueCard === document.querySelector('[data-trk-overview-open="continue:0"]'),
    calendar:window.calendarNode === document.querySelector('.trk-calendar'),
    release:window.firstRelease === document.querySelector('[data-trk-overview-open="airing:0"]'),
    scroll:window.calendarNode.scrollLeft,
  })),{recent:true,continuing:true,calendar:true,release:true,scroll:60});
  phase = "failed";
  await page.evaluate(() => window.dispatchEvent(new CustomEvent('cw:view-status',{detail:{url:'/api/tracker/overview/shows?provider_instance=default',data:{ready:true,error:'refresh_failed'}}})));
  await page.getByRole('button',{name:'Retry',exact:true}).click();
  await page.getByText('Refresh failed. Showing saved results.',{exact:true}).waitFor();
  assert.equal(await page.locator('.trk-calendar-release').count(),12);
  phase = "fresh";
  await page.getByRole('button',{name:'Retry',exact:true}).click();
  await page.locator('.trk-calendar-copy strong').filter({hasText:"Updated show"}).waitFor();
  assert.equal(retry,true);
  phase = "cold";
  await page.evaluate(() => sessionStorage.clear());
  await page.reload();
  await boot({wait:false});
  await page.locator("#page-tracker[data-view-loading]").waitFor();
  await page.locator('[aria-labelledby="trk-overview-airing"] [data-view-status]').waitFor({state:"attached"});
  assert.equal(await page.locator(".cw-page-progress").count(),0);
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  const before = reads;
  phase = "fresh";
  await page.waitForTimeout(1200);
  assert.equal(reads,before);
  await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  await page.locator('.trk-calendar-copy strong').filter({hasText:"Updated show"}).waitFor();
  assert.equal(await page.locator('[data-view-status]:visible').count(),0);
});


test("Tracker episode errors recover automatically even when cached results are unchanged", async t => {
  const {page} = await fixture(t, "flat-dark", {clock:true});
  let failed = true;
  await page.route('**/api/tracker/overview/shows?**', route => route.fulfill({json:{
    continue:episodes.slice(0,2), airing:episodes, total:12, checked:12, done:true,
    _view:{ready:true, refreshing:false, version:"same", error:failed ? "refresh_failed" : null, retry_after:30},
  }}));
  await page.locator('[data-trk-action="refresh"]').click();
  await page.getByText("Some shows couldn't be checked. Refresh to try again.", {exact:true}).waitFor();
  assert.equal(await page.locator('.trk-calendar-release').count(),12);
  await page.evaluate(() => { document.querySelector('.trk-calendar').scrollLeft = 60; });
  failed = false;
  await page.clock.runFor(30001);
  await page.getByText("Some shows couldn't be checked. Refresh to try again.", {exact:true}).waitFor({state:"hidden"});
  assert.equal(await page.locator('[data-view-status]:visible').count(),0);
  assert.equal(await page.locator('.trk-calendar-release').count(),12);
  assert.equal(await page.locator('.trk-calendar').evaluate(node => node.scrollLeft),60);
});

test("cached overview rechecks episode snapshots and shares the compact History cards", async t => {
  const {page, requests, drops} = await fixture(t,"flat-dark",{touch:false});
  assert.equal(await page.locator('[data-trk-slot="controls"] [data-trk-action="log"]').count(),0);
  const logWatch = page.locator('.trk-overview-section[aria-labelledby="trk-overview-recent"] [data-trk-action="log"]');
  await logWatch.click();
  await page.locator('.trk-sheet:visible [data-trk-log-q]').waitFor();
  await page.keyboard.press('Escape');
  const cached = {continue:episodes.slice(0,2),airing:episodes,total:4,checked:4,done:true,unavailable:0};
  await page.route("**/api/tracker/overview?**", route => route.fulfill({json:{revision:"warm",metadata_available:true,hero:episodes[0],continue:rows,recent:rows.map(row=>({...row,progress_percent:null,ref:{feature:"history",key:row.key}})),shows:cached}}));
  const episodeReads = requests.filter(path => path === "/api/tracker/overview/shows").length;
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
  assert.ok(requests.filter(path => path === "/api/tracker/overview/shows").length > episodeReads);
  const positions = await page.evaluate(() => {
    const controls = document.querySelector('.trk-navigation').getBoundingClientRect();
    const source = document.querySelector('[data-trk-slot="source"]').getBoundingClientRect();
    const main = document.querySelector('.cw-main').getBoundingClientRect();
    return {aligned:Math.abs(controls.top-source.top)<2,contained:source.left>=controls.left && source.right<=controls.right,fullWidth:Math.abs(main.width-controls.width)<2};
  });
  assert.deepEqual(positions,{aligned:true,contained:true,fullWidth:true});
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
  await page.locator('.trk-tabs [data-trk-view="history"][aria-selected="true"]').waitFor();
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
  for (const width of [1440,1024,740,390,1440]) {
    await page.setViewportSize({width,height:900});
    await page.waitForFunction(() => {
      const grid=document.querySelector('[aria-labelledby="trk-overview-recent"] .trk-cover-grid');
      return grid && grid.querySelectorAll('[data-trk-overview-open]').length === getComputedStyle(grid).gridTemplateColumns.split(/\s+/).length * 2;
    });
    const count=await cards.count();
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
  await page.waitForFunction(() => document.querySelectorAll('.trk-tabs [role="tab"]:not([hidden])').length === 7);
  await page.setViewportSize({width:900,height:900});
  await page.waitForFunction(() => document.querySelector('.trk-tabs [data-trk-view="collection"]').hidden);
  assert.deepEqual(await tabs.locator('[role="tab"]:visible').evaluateAll(items => items.slice(0,3).map(el => el.dataset.trkView)),['overview','watchlist','history']);
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
  await page.setViewportSize({width:1920,height:900});
  await page.waitForFunction(() => document.querySelectorAll('.trk-tabs [role="tab"]:not([hidden])').length === 7);
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
const pickProfile = (page, id) => page.evaluate(value => CW.OverviewProfile.setActive(value), id);
const pickedProfile = page => page.evaluate(() => CW.OverviewProfile.id);

for (const width of [390,1440]) test(`Tracker follows the selected profile at ${width}px`, async t => {
  const {page,boot,scopedReads} = await fixture(t,'flat-dark',{width,profiles:pickerProfiles});
  assert.equal(await page.locator('.trk-profile-picker').count(),0);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await Promise.all([page.waitForEvent('load'),pickProfile(page,'ann')]);
  await boot();
  assert.equal(await pickedProfile(page),'ann');
  assert.equal(await page.locator('#trk-instance').inputValue(),'other');
  assert.equal(await page.locator('#trk-instance option').count(),1);
  assert.equal(scopedReads.filter(row=>row.path==='/api/tracker/summary').at(-1).viewAs,'ann');
  assert.match(await page.locator('.cw-airing-card').filter({has:page.locator('[data-trk-overview-open="airing:0"]')}).locator('.cw-airing-front').textContent(),/Other tracker show/);
  await page.reload();
  await boot();
  assert.equal(await pickedProfile(page),'ann');
  await Promise.all([page.waitForEvent('load'),pickProfile(page,'__default__')]);
  await boot();
  assert.equal(await pickedProfile(page),'__default__');
  assert.equal(await page.evaluate(()=>CW.OverviewProfile.label),'Admin');
  assert.equal(await page.locator('#trk-instance').inputValue(),'default');
  const defaultReads = scopedReads.filter(row=>row.path==='/api/tracker/summary').at(-1);
  assert.equal(defaultReads.instance,'default');
  assert.equal(defaultReads.viewAs,'');
  await page.reload();
  await boot();
  assert.equal(await pickedProfile(page),'__default__');
  assert.equal(await page.locator('#trk-instance').inputValue(),'default');
  await Promise.all([page.waitForEvent('load'),pickProfile(page,'')]);
  await boot();
  assert.equal(await pickedProfile(page),'');
  assert.equal(await page.locator('#trk-instance option').count(),2);
  assert.equal(scopedReads.filter(row=>row.path==='/api/tracker/summary').at(-1).viewAs,'');
});

test('Tracker profile scope respects managed users and an empty profile list', async t => {
  const managed = await fixture(t,'flat-dark',{width:390,profiles:pickerProfiles,managed:true});
  assert.equal(await managed.page.locator('#trk-instance').inputValue(),'other');
  assert.equal(await managed.page.evaluate(()=>CW.OverviewProfile.setActive('bob')),false);
  const empty = await fixture(t,'flat-light',{width:390,profiles:[]});
  await Promise.all([empty.page.waitForEvent('load'),pickProfile(empty.page,'__default__')]);
  await empty.boot();
  assert.equal(await pickedProfile(empty.page),'__default__');
  assert.equal(await empty.page.locator('#trk-instance').inputValue(),'default');
});

test('Tracker reloads the selected profile when returning from another page', async t => {
  const {page,boot,scopedReads} = await fixture(t,'flat-dark',{profiles:pickerProfiles});
  await page.evaluate(()=>{
    document.querySelector('#page-tracker').classList.add('hidden');
    CW.OverviewProfile.setActive('ann');
  });
  assert.equal(await pickedProfile(page),'ann');
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
  await page.addScriptTag({url:'/assets/js/notification-feed.js'});
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
  await page.route('https://overview.test/api/profile/notifications?**',route=>route.fulfill({json:{items:[{id:'wrapped:other:2024',source:'tracker',title:'Your Wrapped is ready',detail:'2024',href:'#tracker?instance=other&year=2024'}]}}));
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
  assert.equal(await page.locator('.trk-tabs [aria-selected="true"]').getAttribute('data-trk-view'),'overview');
});

test('Wrapped dismissal storage events update only the matching profile and tracker',async t=>{
  const {page,requests}=await fixture(t,'flat-dark',{touch:false,wrapped:{year:'2025'}});
  const other=await page.context().newPage();
  await other.route('https://overview.test/**',route=>route.fulfill({contentType:'text/html',body:'<!doctype html><title>Other tracker tab</title>'}));
  await other.goto('https://overview.test/other');
  const before=requests.length;
  await other.evaluate(()=>localStorage.setItem('cw-wrapped-dismissed:["other","","","default"]','2025'));
  assert.equal(await page.locator('[data-trk-wrapped="2025"]').count(),1);
  await other.evaluate(()=>localStorage.setItem('cw-wrapped-dismissed:["admin","","","default"]','2025'));
  await page.locator('[data-trk-month]').waitFor();
  assert.equal(await page.locator('[data-trk-wrapped]').count(),0);
  await other.evaluate(()=>localStorage.removeItem('cw-wrapped-dismissed:["admin","","","default"]'));
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
    if (width < 760 || view === 'shows') await choose(page,'#trk-mobile-view',view);
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
    if (width < 760) {
      await page.waitForFunction(() => !!window.openedTitle?.options.tracker);
      assert.equal(await page.locator('.trk-sheet:visible').count(),0);
    } else {
      await page.locator('.trk-detail-title:visible').first().waitFor();
      assert.equal(await page.evaluate(() => window.openedTitle),null);
    }
    assert.equal(await card.getAttribute('aria-selected'),'true');
    await trigger.click();
    await menu.getByRole('menuitem',{name:'View details',exact:true}).click();
    await page.waitForFunction(() => !!window.openedTitle?.options.tracker);
    await page.evaluate(() => {window.openedTitle = null;});
    await card.locator('[data-trk-pick]').check();
    assert.equal(await page.locator('.trk-grid-card').first().getAttribute('aria-selected'),'true');
    await page.locator('.trk-grid-card').first().locator('[data-trk-pick]').uncheck();
    assert.equal(await page.evaluate(() => window.openedTitle),null);
    await card.locator('.trk-grid-copy strong').click();
    if (width < 760) await page.waitForFunction(() => !!window.openedTitle?.options.tracker);
    else assert.equal(await page.evaluate(() => window.openedTitle),null);
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

for (const width of [1440,390]) test(`Tracker list navigation opens media only on mobile at ${width}px`, async t => {
  const {page} = await fixture(t,"flat-dark",{width,touch:width < 760});
  await choose(page,'#trk-mobile-view','history');
  await page.locator('[data-trk-layout="list"]').click();
  const row = page.locator('.trk-play').first();
  for (const keyboard of [false,true]) {
    await page.evaluate(() => {window.openedTitle = null;});
    if (keyboard) {await row.focus(); await page.keyboard.press('Enter');}
    else await row.click();
    if (width < 760) {
      await page.waitForFunction(() => !!window.openedTitle?.options.tracker);
      assert.equal(await page.locator('.trk-sheet:visible').count(),0);
    } else {
      await page.locator('.trk-detail-title:visible').first().waitFor();
      assert.equal(await page.evaluate(() => window.openedTitle),null);
    }
  }
  if (width < 760) {
    await page.evaluate(() => {window.openedTitle = null; CW.ProfileMediaModal.canOpen=()=>false;});
    await row.click();
    await page.locator('.trk-title-sheet:visible .trk-detail-title').waitFor();
    assert.equal(await page.evaluate(() => window.openedTitle),null);
  }
});

for (const width of [1440,390]) test(`Compact rating editor keeps half-star changes local until Save at ${width}px`, async t => {
  const {page} = await fixture(t,"flat-dark",{width,touch:width < 760});
  const item = {...rows[0],title:"A Cure for Wellness",year:2017,rating:7,status:"completed",date_field:"rated_at"};
  const edits = [];
  await page.route("**/api/tracker/items?**", route => route.fulfill({json:{items:[item],total:1}}));
  await page.route("**/api/tracker/edit", async route => {
    edits.push(route.request().postDataJSON());
    await route.fulfill({status:409,json:{error:"changed_since"}});
  });
  if (width < 760) await choose(page,'#trk-mobile-view','ratings');
  else await page.locator('.trk-tabs [data-trk-view="ratings"]').click();
  await page.locator('[data-trk-layout="grid"]').click();
  const open = async () => {
    await page.locator('[data-trk-card-menu]').first().click();
    await page.getByRole('menuitem',{name:'Edit rating',exact:true}).click();
  };
  await open();
  const dialog = page.locator('.trk-sheet:visible');
  const rating = dialog.locator('[data-trk-edit="rating"]');
  assert.equal(await rating.inputValue(),'7');
  assert.equal(await dialog.locator('[data-trk-rate]').count(),20);
  assert.equal(await dialog.locator('select[data-trk-edit="rating"]').count(),0);
  await dialog.locator('[data-trk-rate="7.5"]').click();
  assert.equal(await rating.inputValue(),'7.5');
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.evaluate(() => document.activeElement.dataset.trkRate),'8');
  assert.equal(await rating.inputValue(),'7.5');
  await page.keyboard.press('Enter');
  assert.equal(await rating.inputValue(),'8');
  await dialog.locator('[data-trk-edit="title"]').focus();
  assert.equal(await dialog.locator('[data-trk-rate-value]').textContent(),'8/10');
  assert.equal(edits.length,0);
  await dialog.getByRole('button',{name:'Cancel',exact:true}).click();
  assert.equal(edits.length,0);
  await open();
  assert.equal(await rating.inputValue(),'7');
  await dialog.locator('[data-trk-rate="7.5"]').click();
  await dialog.locator('[data-trk-edit="title"]').fill('Edited title');
  await dialog.getByRole('button',{name:'Save',exact:true}).click();
  await dialog.locator('.trk-error').waitFor();
  assert.equal(edits.length,1);
  assert.deepEqual(edits[0].changes,{rating:7.5,title:'Edited title'});
  assert.equal(await rating.inputValue(),'7.5');
  assert.equal(await dialog.locator('[data-trk-rate="7.5"]').getAttribute('aria-pressed'),'true');
  assert.equal(await dialog.locator('[data-trk-edit="title"]').inputValue(),'Edited title');
  await dialog.locator('[data-trk-edit="title"]').fill(item.title);
  await mkdir('.tmp/tracker-rating',{recursive:true});
  await page.screenshot({path:`.tmp/tracker-rating/edit-${width}.png`});
  assert.equal(await dialog.evaluate(el => el.scrollWidth <= el.clientWidth),true);
});

test('Tracker Escape clears selection after menus and dialogs, including a pending title request', async t => {
  const {page} = await fixture(t,'flat-dark',{touch:false});
  await page.locator('.trk-tabs [data-trk-view="progress"]').click();
  await page.locator('[data-trk-layout="grid"]').click();
  const row = page.locator('.trk-grid-card').first();
  await row.locator('.trk-grid-copy strong').click();
  await page.locator('[data-trk-detail="rate"]').waitFor();
  await page.locator('.trk-source > summary').click();
  await page.keyboard.press('Escape');
  assert.equal(await page.locator('.trk-source').getAttribute('open'),null);
  assert.equal(await row.getAttribute('aria-selected'),'true');
  await row.locator('[data-trk-card-menu]').click();
  await page.keyboard.press('Escape');
  assert.equal(await row.getAttribute('aria-selected'),'true');
  await page.locator('[data-trk-detail="rate"]').click();
  await page.locator('.trk-sheet:visible').waitFor();
  await page.keyboard.press('Escape');
  assert.equal(await row.getAttribute('aria-selected'),'true');
  await page.keyboard.press('Escape');
  await page.locator('[data-trk-slot="title"] .trk-detail-empty').waitFor();
  assert.equal(await row.getAttribute('aria-selected'),'false');
  let release;
  const held = new Promise(resolve => {release=resolve;});
  await page.route('**/api/tracker/title?**', async route => {
    await held;
    await route.fulfill({json:{title:rows[1],plays:[],ratings:[],watchlist:[],collection:[],progress:[],attention:[],seasons:[]}});
  });
  await page.locator('.trk-grid-card').nth(1).locator('.trk-grid-copy strong').click();
  await page.keyboard.press('Escape');
  const response = page.waitForResponse(r => r.url().includes('/api/tracker/title?'));
  release();
  await response;
  await page.waitForFunction(() => document.querySelector('[data-trk-slot="title"]').textContent.includes('Pick a row'));
  assert.equal(await page.locator('.trk-play.is-selected').count(),0);
});

for (const width of [1440,390]) test(`Tracker playback banner shares the live cache and restores stats at ${width}px`, async t => {
  const {page} = await fixture(t,'flat-dark',{width,playback:true});
  let stream = {type:'episode',title:'VisionQuest',ids:{tmdb_show:'99'},season:1,episode:1,state:'playing',progress:65,duration_ms:3600000,source:'PLEX',session_key:'session-one'};
  let reads = 0;
  await page.route('**/api/watch/currently_watching*', route => { reads++; return route.fulfill({json:{streams:stream ? [stream] : []}}); });
  const update = () => page.evaluate(async () => {
    await CW.PlayingCard.currentlyWatching(true);
    window.dispatchEvent(new Event('currently-watching-updated'));
  });
  await update();
  const banner = page.locator('.trk-hero-playback');
  await banner.waitFor();
  assert.equal(await banner.locator('[role="progressbar"]').getAttribute('aria-valuenow'),'65');
  assert.equal(await banner.locator('[data-trk-live="remaining"]').textContent(),'21:00 left');
  assert.match(await banner.locator('[data-trk-live="meta"]').textContent(),/PLEX.*S01E01/);
  assert.equal(await page.locator('[data-trk-slot="hero"]').isVisible(),false);
  assert.match(await page.locator('.trk-hero-heading').textContent(),/VisionQuest/);
  await page.evaluate(() => {window.savedLiveBar=document.querySelector('.trk-hero-playback-track'); window.savedLiveLogo=document.querySelector('.trk-hero-logo');});
  stream={...stream,state:'paused',progress:70};
  await update();
  await page.waitForFunction(() => document.querySelector('[data-trk-live="state"]').textContent === 'Paused');
  assert.equal(await page.evaluate(() => window.savedLiveBar === document.querySelector('.trk-hero-playback-track') && window.savedLiveLogo === document.querySelector('.trk-hero-logo')),true);
  assert.equal(reads,2);
  assert.equal(await banner.locator('[role="progressbar"]').getAttribute('aria-valuenow'),'70');
  if (width === 1440) {
    await page.locator('.trk-tabs [data-trk-view="progress"]').click();
    await page.locator('[data-trk-layout="grid"]').click();
    await page.locator('.trk-grid-copy strong').nth(2).click();
    await page.locator('[data-trk-detail="rate"]').waitFor();
    assert.equal(await banner.isVisible(),false);
    assert.equal(await page.locator('[data-trk-slot="hero"]').isVisible(),true);
    await page.keyboard.press('Escape');
    await banner.waitFor();
    assert.match(await page.locator('.trk-hero-heading').textContent(),/VisionQuest/);
  }
  await mkdir('.tmp/tracker-playback',{recursive:true});
  await page.locator('.cw-page-hero-tracker').screenshot({path:`.tmp/tracker-playback/hero-${width}.png`});
  assert.equal(await banner.evaluate(el => el.scrollWidth <= el.clientWidth),true);
  if (width === 1440) assert.ok((await banner.boundingBox()).width >= 400);
  await page.keyboard.press('Escape');
  await banner.waitFor({state:'hidden'});
  const restore=page.locator('[data-trk-action="show-playback"]');
  await restore.waitFor();
  await page.locator('.cw-page-hero-tracker').screenshot({path:`.tmp/tracker-playback/hidden-${width}.png`});
  assert.equal(await page.locator('[data-trk-slot="hero"]').getAttribute('hidden'),null);
  stream={...stream,state:'playing',progress:72};
  await update();
  assert.equal(await banner.isVisible(),false);
  assert.equal(await restore.isVisible(),true);
  await restore.click();
  await banner.waitFor();
  assert.equal(await banner.locator('[role="progressbar"]').getAttribute('aria-valuenow'),'72');
  assert.equal(await restore.isVisible(),false);
  await page.keyboard.press('Escape');
  await banner.waitFor({state:'hidden'});
  await update();
  assert.equal(await banner.isVisible(),false);
  stream={...stream,session_key:'session-two',progress:5};
  await update();
  await banner.waitFor();
  assert.equal(await banner.locator('[role="progressbar"]').getAttribute('aria-valuenow'),'5');
  await page.locator('.cw-page-hero-tracker').screenshot({path:`.tmp/tracker-playback/restored-${width}.png`});
  stream=null;
  await update();
  await banner.waitFor({state:'hidden'});
  assert.equal(await page.locator('[data-trk-slot="hero"]').getAttribute('hidden'),null);
  assert.doesNotMatch(await page.locator('.trk-hero-heading').textContent(),/VisionQuest/);
});

test('Shared rating stars retain immediate saving in the dedicated rating dialog', async t => {
  const {page} = await fixture(t,"flat-dark",{width:1440,touch:false});
  const item = {...rows[0],rating:7,date_field:"rated_at"};
  const edits = [];
  await page.route("**/api/tracker/items?**", route => route.fulfill({json:{items:[item],total:1}}));
  await page.route("**/api/tracker/title?**", route => route.fulfill({json:{title:item,show:false,plays:[],ratings:[item],watchlist:[],collection:[],progress:[],attention:[],seasons:[]}}));
  await page.route("**/api/tracker/edit", route => {
    edits.push(route.request().postDataJSON());
    return route.fulfill({json:{ok:true,batch:"rating"}});
  });
  await page.locator('.trk-tabs [data-trk-view="ratings"]').click();
  await page.locator('[data-trk-layout="grid"]').click();
  await page.locator('.trk-grid-copy strong').first().click();
  await page.locator('[data-trk-detail="rate"]').click();
  const dialog = page.locator('.trk-sheet:visible');
  assert.equal(await dialog.locator('[data-trk-rate-value]').textContent(),'7/10');
  assert.equal(await dialog.locator('[data-trk-edit="rating"]').count(),0);
  const saved = page.waitForResponse(response => response.url().endsWith('/api/tracker/edit'));
  await dialog.locator('[data-trk-rate="8.5"]').click();
  await saved;
  assert.equal(edits.length,1);
  assert.deepEqual(edits[0].changes,{rating:8.5});
});

test('Mobile Tracker uses a sticky view bar, compact shelves and two-column cards', async t => {
  const {page} = await fixture(t,'flat-dark',{width:390});
  await page.locator('.trk-overview-card').first().waitFor();
  const art = await page.locator('.trk-overview-art').first().boundingBox();
  assert.ok(Math.abs(art.width - 240) < 2);
  assert.ok(Math.abs(art.height - 135) < 2);
  const copy = await page.locator('.trk-overview-copy').first().boundingBox();
  assert.ok(copy.y >= art.y + art.height);
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('[data-trk-layout="grid"]').click();
  await page.locator('.trk-grid-card').first().waitFor();
  const cards = await page.locator('.trk-grid-card').evaluateAll(els => els.slice(0,2).map(el => {const r=el.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width};}));
  assert.ok(Math.abs(cards[0].y - cards[1].y) < 2);
  assert.ok(cards[0].width < 190 && cards[0].width > 140);
  await page.evaluate(()=>window.scrollTo(0,450));
  const nav = await page.locator('.trk-navigation').boundingBox();
  const header = await page.locator('body > header').boundingBox();
  assert.ok(Math.abs(nav.y - header.height) < 2, JSON.stringify({nav,header}));
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth > innerWidth),false);
  await mkdir('.tmp/mobile-implementation',{recursive:true});
  await page.screenshot({path:'.tmp/mobile-implementation/tracker-history.png'});
  await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  await page.evaluate(()=>window.scrollTo(0,0));
  await page.screenshot({path:'.tmp/mobile-implementation/tracker-overview.png'});
});

test('Tracker keeps its badge dropdown in the navigation across view and viewport changes', async t => {
  const {page,scopedReads} = await fixture(t,'flat-dark',{width:390});
  assert.equal(await page.locator('#trk-instance').count(),1);
  assert.equal(await page.locator('[data-trk-slot="filters"]').isVisible(),false);
  await page.locator('.trk-others summary').click();
  assert.equal(await page.locator('.trk-source > summary').isVisible(),true);
  await choose(page,'#trk-instance','other');
  await page.waitForFunction(()=>document.querySelector('#trk-instance')?.value === 'other');
  await page.waitForFunction(()=>document.querySelector('.trk-overview'));
  assert.ok(scopedReads.some(row=>row.instance === 'other' && row.path === '/api/tracker/summary'));
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  assert.equal(await page.locator('#trk-instance').count(),1);
  await page.locator('.trk-others summary').click();
  assert.equal(await page.locator('.trk-navigation > [data-trk-slot="source"] #trk-instance').inputValue(),'other');
  await page.setViewportSize({width:1440,height:900});
  await page.locator('.trk-source > summary').waitFor({state:'visible'});
  assert.equal(await page.locator('#trk-instance').count(),1);
  assert.equal(await page.locator('#trk-instance').inputValue(),'other');
  await page.setViewportSize({width:390,height:844});
  await page.waitForFunction(()=>document.querySelector('.trk-navigation > [data-trk-slot="source"] #trk-instance'));
  assert.equal(await page.locator('#trk-instance').count(),1);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth > innerWidth),false);
});

for (const width of [390,320]) test(`Mobile Tracker toolbar stays in two rows at ${width}px`, async t => {
  const {page} = await fixture(t,'flat-dark',{width});
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('.trk-toolbar').waitFor();
  assert.equal(await page.locator('[data-trk-slot="filters"]').isVisible(),false);
  const search = await page.locator('.trk-search-wrap').boundingBox();
  const toolbar = await page.locator('.trk-toolbar').boundingBox();
  const actions = await page.locator('.trk-actions').evaluate(el => [...el.querySelectorAll('button:not([role="menuitem"])')].filter(button=>button.checkVisibility()).map(button=>{const r=button.getBoundingClientRect();return {y:r.y,height:r.height,right:r.right};}));
  assert.ok(toolbar.height <= 104,JSON.stringify(toolbar));
  assert.ok(actions.every(button=>Math.abs(button.y-actions[0].y)<2 && button.height >= 44 && button.right <= width));
  assert.ok(actions[0].y >= search.y + search.height);
  await page.locator('[data-trk-action="filters"]').click();
  await page.locator('#trk-extra-filters').waitFor({state:'visible'});
  await page.locator('[data-trk-action="filters"]').click();
  await page.locator('.trk-others summary').click();
  for (const action of ['views','changes','import']) assert.equal(await page.locator(`.trk-others-menu [data-trk-action="${action}"]`).isVisible(),true);
  await page.locator('.trk-others-menu [data-trk-action="views"]').click();
  await page.locator('.trk-sheet:visible').waitFor();
  assert.equal(await page.locator('.trk-others').getAttribute('open'),null);
  await page.keyboard.press('Escape');
  await page.locator('[data-trk-layout="grid"]').click();
  await page.locator('.trk-grid-card').first().waitFor();
  await page.evaluate(()=>window.scrollTo(0,0));
  await mkdir('.tmp/mobile-implementation',{recursive:true});
  await page.screenshot({path:`.tmp/mobile-implementation/tracker-toolbar-${width}.png`});
  await page.setViewportSize({width:1440,height:900});
  await page.locator('.trk-actions [data-trk-action="views"]').waitFor({state:'visible'});
  assert.equal(await page.locator('[data-trk-action="views"]').count(),1);
});


for (const width of [1440,390]) test(`Recently watched menu opens the shared rating dialog at ${width}px`, async t => {
  const {page,writes} = await fixture(t,'flat-dark',{width,touch:width<760});
  const recent=page.locator('[data-trk-overview-open="recent:0"]');
  await recent.locator('[data-trk-card-menu]').click();
  await page.getByRole('menuitem',{name:'Rate',exact:true}).click();
  const dialog=page.locator('.trk-sheet:visible');
  await dialog.locator('[data-trk-rate-value]').waitFor();
  assert.match(await dialog.innerText(),/Rate Movie 1/);
  assert.equal(await dialog.locator('[data-trk-rate-value]').textContent(),'Not rated');
  const saved=page.waitForResponse(response=>response.url().endsWith('/api/tracker/add'));
  await dialog.locator('[data-trk-rate="8.5"]').click();
  await saved;
  assert.equal(writes.length,1);
  assert.equal(writes[0].feature,'ratings');
  assert.equal(writes[0].item.rating,8.5);
  assert.equal(writes[0].item.title,'Movie 1');
  assert.equal(writes[0].provider_instance,'default');
});


for (const width of [1440,390]) test(`Tracker cards keep routine statuses quiet and exceptions visible at ${width}px`, async t => {
  const {page}=await fixture(t,'flat-dark',{width,touch:width<760});
  const items=['watching','completed','dropped','on_hold'].map((status,index)=>({...rows[index],status,ref:{feature:'history',key:rows[index].key}}));
  await page.route('**/api/tracker/overview?**',route=>route.fulfill({json:{revision:'status-icons',metadata_available:true,continue:[],recent:items,shows:{continue:[],airing:[],done:true}}}));
  await page.locator('[data-trk-action="refresh"]').evaluate(el=>el.click());
  const cards=page.locator('[data-trk-overview-open^="recent:"]');
  await cards.first().locator('.trk-grid-status-icon').waitFor();
  for (const [index,label] of [[0,'Watching'],[1,'Completed']]) {
    const card=cards.nth(index);
    assert.equal(await card.locator('.trk-grid-art .trk-status').count(),0);
    const icon=card.locator('.trk-grid-copy>small .trk-grid-status-icon');
    assert.equal(await icon.getAttribute('aria-label'),label);
    await icon.focus();
    assert.equal(await icon.evaluate(el=>getComputedStyle(el,'::after').visibility),'visible');
    const fits=await card.locator('.trk-grid-copy>small').evaluate(el=>el.scrollWidth<=el.clientWidth+1);
    assert.equal(fits,true);
  }
  assert.match(await cards.nth(2).locator('.trk-grid-status').innerText(),/Dropped/);
  assert.match(await cards.nth(3).locator('.trk-grid-status').innerText(),/On hold/);
  assert.equal(await cards.nth(2).locator('.trk-grid-status-icon').count(),0);
  await mkdir('.tmp/tracker-status',{recursive:true});
  await cards.first().scrollIntoViewIfNeeded();
  await page.screenshot({path:`.tmp/tracker-status/cards-${width}.png`});
  await page.route('**/api/tracker/items?**',route=>route.fulfill({json:{items,total:items.length}}));
  await page.locator('.trk-tabs [data-trk-view="history"]').evaluate(el=>el.click());
  await page.locator('[data-trk-layout="grid"]').click();
  await page.locator('.trk-grid-status-icon').first().waitFor();
  await page.locator('[data-trk-input="status-labels"]').evaluate(el=>el.click());
  assert.equal(await page.locator('.trk-grid-status-icon,.trk-grid-status').count(),0);
});


test('Tracker Overview prioritizes live playback while Escape closes dialogs and selection first', async t => {
  const {page}=await fixture(t,'flat-dark',{playback:true,touch:false});
  const stream={type:'movie',title:'Live movie',ids:{tmdb:'99'},state:'playing',progress:30,duration_ms:3600000,source:'PLEX',session_key:'live'};
  await page.route('**/api/watch/currently_watching*',route=>route.fulfill({json:{streams:[stream]}}));
  await page.evaluate(async()=>{
    await CW.PlayingCard.currentlyWatching(true);
    window.dispatchEvent(new Event('currently-watching-updated'));
  });
  const banner=page.locator('.trk-hero-playback');
  await banner.waitFor();
  const recent=page.locator('[data-trk-overview-open="recent:0"]');
  await recent.locator('[data-trk-card-menu]').click();
  await page.getByRole('menuitem',{name:'Rate',exact:true}).click();
  await page.locator('.trk-sheet:visible [data-trk-rate-value]').waitFor();
  assert.equal(await banner.isVisible(),true);
  assert.match(await page.locator('.trk-hero-heading').textContent(),/Live movie/);
  await page.keyboard.press('Escape');
  await page.locator('.trk-sheet:visible').waitFor({state:'hidden'});
  assert.equal(await banner.isVisible(),true);
  await page.keyboard.press('Escape');
  assert.equal(await banner.isVisible(),true);
  await page.keyboard.press('Escape');
  await banner.waitFor({state:'hidden'});
  await page.locator('[data-trk-action="show-playback"]').click();
  await banner.waitFor();
  await recent.locator('[data-trk-card-menu]').click();
  await page.keyboard.press('Escape');
  assert.equal(await banner.isVisible(),true);
});


test('Tracker overview does not wait for slow statistics on a cold load',async t=>{
  const {page,boot}=await fixture(t,'flat-dark',{width:390});
  await page.evaluate(()=>sessionStorage.clear());
  let release;
  const gate=new Promise(resolve=>{release=resolve;});t.after(()=>release());
  await page.route('**/api/tracker/stats?**',async route=>{await gate;await route.fallback();});
  await page.reload();
  await boot({wait:false});
  await page.locator('[data-trk-overview-open="airing:0"]').waitFor();
  release();
});
