/* tests/tracker-overview.browser.mjs */
/* CrossWatch - Overview navigation, actions and responsive theme coverage */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const sizes = [[320,740],[390,844],[430,932],[740,360],[844,390],[768,1024],[820,1180],[1024,768],[1194,834],[1366,1024],[1366,768],[1440,900],[1920,1080]];
const source = await readFile(new URL("../ui_frontend.py", import.meta.url),"utf8");
const assets = new Map();
const rows = Array.from({length:20},(_,index) => ({key:`movie:${index}`,ref:{feature:"progress",key:`movie:${index}`},type:"movie",title:index === 1 ? 'A title with <script>window.injected=true</script>' : `Movie ${index + 1}`,year:2026,art:{kind:"movie",tmdb:String(index + 1)},ids:{tmdb:String(index + 1)},date:"2026-11-09T12:00:00Z",progress_percent:40,remaining_minutes:22}));
const episodes = Array.from({length:12},(_,index) => ({key:`episode:${index}`,ref:{show:`tmdb:${index + 100}`},type:"episode",title:`Show ${index + 1}`,art:{kind:"tv",tmdb:String(index + 100)},show_ids:{tmdb:String(index + 100)},season:2,episode:index ? 3 : 1,episode_title:"The next chapter",air_date:new Date(Date.UTC(2026,10,10 + [1,5,5,10,12,14,16,18,20,22,25,29][index])).toISOString().slice(0,10),date:"2026-11-08T12:00:00Z"}));

async function fixture(t, theme, {realMedia = false, touch = true, width = 1440, airing = episodes} = {}) {
  const browser = await ({chromium,webkit})[process.env.CW_AUDIT_BROWSER || "chromium"].launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width,height:900},hasTouch:touch,reducedMotion:"reduce"});
  await page.clock.setFixedTime(new Date("2026-11-10T12:00:00Z"));
  page.setDefaultTimeout(7000);
  const errors = [], writes = [], requests = [], drops = [];
  let empty = false, fail = false, metadata = true, release;
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
  },{source,theme});
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
    let json = {ok:true};
    if (path === "/api/tracker/summary") json = {provider_instance:instance,instances:[{id:"default",label:"Default"},{id:"other",label:"Second tracker"}],counts:{history:24},health:{history:{attention:2}},notes:{total:0,statuses:{},tags:[]}};
    if (path === "/api/tracker/stats") json = {week:4,month:15,year:244,total:244};
    if (path === "/api/tracker/overview") json = {metadata_available:metadata,continue:empty ? [] : rows,recent:empty ? [] : rows.map(row=>({...row,progress_percent:null,ref:{feature:"history",key:row.key},plays:2}))};
    if (path === "/api/tracker/overview/shows") {
      const pending = hold;
      if (pending && instance === "default") await pending;
      if (fail) return route.fulfill({status:503,json:{error:"unavailable"}});
      json = {continue:empty ? [] : episodes.slice(0,2),airing:empty ? [] : airing.map(row=>({...row,title:instance === "other" ? "Other tracker show" : row.title})),total:4,checked:4,done:true,unavailable:0,metadata_available:metadata};
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
  await page.evaluate(() => { window.CW = {AuthState:{read:()=>({isAdmin:true,permissions:{write:true}})},ProfileMediaModal:{canOpen:item=>!!item.ids?.tmdb,open:(item,options)=>{window.openedTitle={item,options};}}}; });
  if (realMedia) {
    await page.evaluate(() => {
      const meta = item => ({title:item.title,year:2026,genres:[],detail:{},overview:"Media card overview"});
      window.CW.Meta = {peek:meta,get:async item => meta(item)};
    });
    await page.addScriptTag({url:'/assets/js/profile-media-modal.js'});
  }
  for (const file of ["helpers/account-menu","helpers/icon-select","helpers/selection-bar","js/tracker"]) await page.addScriptTag({url:`/assets/${file}.js`});
  await page.evaluate(() => TrackerPage.mount(document.querySelector("#page-tracker")));
  await page.locator('[data-trk-overview-open="airing:0"]').waitFor();
  await page.evaluate(() => document.fonts.ready);
  return {page,writes,requests,drops,airing:value=>{airing=value;},empty:value=>{empty=value;},fail:value=>{fail=value;},metadata:value=>{metadata=value;},hold:()=>{hold=new Promise(resolve=>{release=resolve;});},release:()=>{hold=null;release?.();}};
}

async function choose(page, selector, value) {
  const button = page.locator(`${selector} + .cw-icon-select .cw-icon-select-btn`);
  await button.click();
  await page.locator(`.cw-icon-select-menu:not(.hidden) [data-value="${value}"]`).click();
}

for (const theme of ["original","flat-dark","flat-light"]) test(`${theme}: overview layout and actions across devices`,async t => {
  const app = await fixture(t,theme), {page,writes} = app;
  assert.equal(await page.locator('.trk-tabs [role="tab"]').first().textContent(),"space_dashboardOverview");
  assert.equal(await page.locator('.trk-tabs [data-trk-view="cleanup"]').count(),0);
  assert.equal(await page.locator("[data-trk-slot=filters]").isVisible(),false);
  assert.equal(await page.locator('[data-trk-overview-open^="continue:"]').count(),4);
  assert.equal(await page.locator('[data-trk-overview-open^="recent:"]').count(),8);
  assert.equal(await page.evaluate(() => window.injected),undefined);
  assert.equal(await page.locator('.trk-calendar-day').count(),30);
  assert.equal(await page.locator('.trk-calendar-day.is-today time').getAttribute('datetime'),"2026-11-10");
  assert.equal(await page.locator('[data-calendar-date="2026-11-15"] .trk-calendar-release').count(),2);
  assert.equal(await page.locator('.trk-calendar-day').last().getAttribute('data-calendar-date'),"2026-12-09");
  assert.equal(await page.locator('[data-trk-overview-more="airing"]').count(),0);
  for (const [width,height] of sizes) {
    await page.setViewportSize({width,height});
    await page.waitForTimeout(90);
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
    await more.click();
    assert.equal(await page.locator('[data-trk-overview-open^="recent:"]').count(),16);
    await more.click();
    assert.equal(await page.locator('[data-trk-overview-open^="recent:"]').count(),20);
    await more.click();
    assert.equal(await page.locator('[data-trk-overview-open^="recent:"]').count(),8);
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
  assert.equal(await page.locator('.trk-others [data-trk-view="cleanup"]').evaluate(el=>el===document.activeElement),true);
  await page.keyboard.press("Escape");
  assert.equal(await page.locator(".trk-others").getAttribute("open"),null);
  await summary.click();
  await page.locator('.trk-others [data-trk-view="cleanup"]').click();
  await page.locator(".trk-toolbar-cleanup").waitFor();
  assert.equal(await page.locator(".trk-others.is-active").count(),1);
  await page.locator('.trk-tabs [data-trk-view="overview"]').click();
  await page.locator('[data-trk-overview-open="continue:0"]').click();
  await page.waitForFunction(() => window.openedTitle?.options.tracker);
  await page.locator('[data-trk-overview-watch="0"]').click();
  await page.waitForFunction(() => document.querySelector('[data-trk-overview-watch="0"]')?.disabled === false);
  assert.equal(writes.length,1);
  assert.equal(writes[0].provider_instance,"default");
  assert.equal(writes[0].items[0].ids.tmdb,"1");
  await page.setViewportSize({width:390,height:844});
  await choose(page,"#trk-mobile-view","cleanup");
  await page.locator(".trk-toolbar-cleanup").waitFor();
  assert.equal(await page.locator('#trk-mobile-view optgroup[label="Others"] option').textContent(),"Others · Clean up (2)");
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
  assert.equal(await page.locator('[data-trk-overview-open^="recent:"]').count(),8);
  app.fail(false); app.empty(true); app.metadata(false);
  await page.locator('[data-trk-action="refresh"]').click();
  await page.locator(".trk-overview-empty").filter({hasText:"TMDB API key"}).waitFor();
  assert.equal(await page.locator(".trk-overview-empty").count(),3);
});

for (const theme of ["original","flat-dark","flat-light"]) test(`${theme}: cover hover actions and real media cards`, async t => {
  const {page,requests,writes,drops} = await fixture(t,theme,{realMedia:true,touch:false});
  const cover = page.locator('[data-trk-overview-open="continue:0"] .trk-overview-art');
  const actions = page.locator('.trk-overview-actions').first();
  await page.mouse.move(0,0);
  assert.equal(await actions.evaluate(el => getComputedStyle(el).opacity),"0");
  const before = requests.length;
  await cover.hover();
  assert.equal(await actions.evaluate(el => getComputedStyle(el).opacity),"1");
  assert.equal(await cover.locator('.trk-overview-peek').evaluate(el => getComputedStyle(el).opacity),"1");
  assert.equal(requests.length,before);
  if (process.env.CW_OVERVIEW_SCREENSHOTS) {
    await mkdir(process.env.CW_OVERVIEW_SCREENSHOTS,{recursive:true});
    await page.locator('.trk-overview-card').first().screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/${theme}-hover.png`});
  }
  await page.locator('[data-trk-overview-open="continue:0"]>strong').hover();
  assert.equal(await actions.evaluate(el => getComputedStyle(el).opacity),"0");
  await page.locator('[data-trk-overview-open="continue:0"]').focus();
  await page.keyboard.press('Tab');
  assert.equal(await page.locator('[data-trk-overview-watch="0"]').evaluate(el => el === document.activeElement),true);
  assert.equal(await actions.evaluate(el => getComputedStyle(el).opacity),"1");
  for (const [kind, name] of [["continue","Movie 1"],["recent","Movie 1"],["airing","Show 1"]]) {
    const target = page.locator(`[data-trk-overview-open="${kind}:0"]`);
    if (kind === 'airing') await target.click({position:{x:100,y:100}});
    else await target.locator('.trk-overview-art').click({position:{x:100,y:100}});
    const modal = page.locator('#cw-media-modal');
    await modal.waitFor();
    assert.equal(await modal.locator('#cw-mm-title').getAttribute('aria-label'),name);
    assert.equal(await page.locator('.trk-sheet:visible').count(),0);
    if (process.env.CW_OVERVIEW_SCREENSHOTS && kind === 'airing') await modal.screenshot({path:`${process.env.CW_OVERVIEW_SCREENSHOTS}/${theme}-media-card.png`});
    await page.keyboard.press('Escape');
    await modal.waitFor({state:'hidden'});
  }
  await cover.hover();
  await page.locator('[data-trk-overview-watch="0"]').click();
  await page.waitForFunction(() => document.querySelector('[data-trk-overview-watch="0"]')?.disabled === false);
  assert.equal(writes.length,1);
  assert.equal(await page.locator('#cw-media-modal').isVisible(),false);
  await cover.hover();
  await page.locator('[data-trk-overview-drop="0"]').click();
  await page.waitForFunction(() => document.querySelector('[data-trk-overview-drop="0"]')?.disabled === false);
  assert.deepEqual(drops,[{provider_instance:"default",keys:["movie:0"],status:"dropped",view:"progress",feature:"progress"}]);
  assert.equal(await page.locator('#cw-media-modal').isVisible(),false);
  for (let i=0;i<5;i++) await page.locator('[data-trk-overview-more="continue"]').click();
  await page.locator('[data-trk-overview-open="continue:20"] .trk-overview-art').hover();
  await page.locator('[data-trk-overview-drop="20"]').click();
  await page.waitForFunction(() => document.querySelector('[data-trk-overview-drop="20"]')?.disabled === false);
  assert.deepEqual(drops[1],{provider_instance:"default",keys:["tmdb:100"],status:"dropped",view:"shows",feature:"history"});
  await page.evaluate(() => { window.CW.AuthState.read = () => ({permissions:{write:false}}); });
  await page.locator('[data-trk-overview-more="continue"]').click();
  assert.equal(await page.locator('[data-trk-overview-drop="0"]').isDisabled(),true);
  assert.equal(await page.locator('[data-trk-overview-watch="0"]').isDisabled(),true);
});

test("touch covers expose accessible quick actions and open the real media card", async t => {
  const {page} = await fixture(t,"flat-dark",{realMedia:true,touch:true,width:390});
  const action = page.locator('[data-trk-overview-watch="0"]');
  assert.equal(await action.isVisible(),true);
  const bounds = await action.boundingBox();
  assert.ok(bounds.width >= 44 && bounds.height >= 44);
  await page.locator('[data-trk-overview-open="recent:0"] .trk-overview-art').tap();
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
  assert.equal(await page.locator('.trk-calendar').evaluate(el => el.scrollWidth > el.clientWidth),true);
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
  await page.locator('.trk-calendar-copy strong').filter({hasText:"Première"}).waitFor();
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
