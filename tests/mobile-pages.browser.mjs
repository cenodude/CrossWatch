/* tests/mobile-pages.browser.mjs */
/* CrossWatch - Phone workflows for Editor, Tracker, Events and Analyzer */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFile, readdir, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const styles = ["fonts/material-symbols-rounded", "themes/tokens", "css/base", "crosswatch", "css/layout", "css/components", "css/pages", "css/page-theme", "ui-shell"];
const titles = ["The Grand Budapest Hotel", "A very long movie title that needs room on a small phone screen", "Arrival"];
const items = titles.map((title, i) => ({key:`movie:${i+1}`,type:"movie",title,year:2014+i,ids:{tmdb:String(i+1)},date:"2026-11-06T18:30:00Z"}));
const findings = items.map(item => ({...item,provider:"PLEX",feature:"history",type:"missing_peer",item_type:"movie",pair_id:"pair",message:"Missing from the destination snapshot",targets:["TRAKT"]}));
const events = items.map((item, i) => ({...item,id:String(i+1),event_type:"write_failed",severity:"error",domain:"sync",feature:"history",provider:"TRAKT",created_at:1793989800,reason_message:"The destination could not match this title.",title:item.title,summary:"History update failed",item_key:item.key}));

async function fixture(t, kind, theme = "flat-dark", width = 390) {
  const browser = await (process.env.CW_MOBILE_BROWSER === "webkit" ? webkit : chromium).launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width,height:844},hasTouch:width<800,isMobile:width<800});
  page.setDefaultTimeout(6000);
  const errors = [], requests = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  const state = {empty:false,eventTotal:items.length};
  const editorModules = (await readdir(new URL("../assets/js/editor/", import.meta.url))).filter(name=>name.endsWith(".js")&&!['episode-groups.js','episode-alignment.js','mapping.js','origin.js','saved-mappings.js'].includes(name));
  await page.route("https://mobile-pages.test/**", async route => {
    const url = new URL(route.request().url()), path = url.pathname;
    if(path.startsWith("/assets/")) {
      try { return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".woff2")?"font/woff2":path.endsWith(".svg")?"image/svg+xml":"text/javascript"}); }
      catch { return route.fulfill({status:404,body:""}); }
    }
    if(path.startsWith("/api/")) {
      requests.push(path+url.search);
      const list = state.empty ? [] : items;
      let json = {ok:true};
      if(path==="/api/pairs") json=[{id:"pair",source:"PLEX",target:"TRAKT",enabled:true,features:{history:true}}];
      if(path==="/api/provider-instances") json={PLEX:[{id:"default",label:"Home"}],TRAKT:[{id:"default",label:"Personal"}]};
      if(path==="/api/editor/state/providers") json={providers:["CROSSWATCH"]};
      if(path==="/api/editor") json={provider:"CROSSWATCH",provider_instance:"default",rating_scale:{values:[1,2,3,4,5,6,7,8,9,10],max:10,display_factor:1},items:Object.fromEntries(list.map(item=>[item.key,item])),manual_adds:{},manual_blocks:[]};
      if(path==="/api/editor/send/options") json={providers:[]};
      if(path==="/api/tracker/summary") json={provider_instance:"default",instances:[{id:"default",label:"Personal"}],counts:{history:list.length,watchlist:list.length,ratings:list.length,collection:list.length},health:{},notes:{total:list.length,statuses:{watching:1},tags:[]}};
      if(path==="/api/tracker/stats") json={week:3,month:12,year:50,total:50,timed:50,minutes:5500,streak:2};
      if(["/api/tracker/items","/api/tracker/notes","/api/tracker/shows"].includes(path)) json={items:list,total:list.length};
      if(path==="/api/tracker/title") json={title:list.find(item=>item.key===url.searchParams.get("key"))||list[0],plays:list,ratings:[],watchlist:[],collection:[],progress:[],attention:[],notes:{}};
      if(path==="/api/analyzer/problems") json={problems:state.empty?[]:findings};
      if(path==="/api/analyzer/state") json={items:state.empty?[]:findings,total:list.length};
      if(path==="/api/analyzer/system") json={problems:[]};
      if(path==="/api/analyzer/detail") json={item:items[Number(url.searchParams.get("key")?.split(":")[1]||1)-1],targets:["TRAKT"],hints:[]};
      if(path==="/api/events/search") json={items:state.empty?[]:events,total:state.empty?0:state.eventTotal};
      if(path==="/api/events/context") json={event:events.find(event=>event.id===url.searchParams.get("event_id")),timeline:[]};
      return route.fulfill({json});
    }
    const attrs=theme==="original"?'class="cw-theme-original"':`data-cw-theme="${theme}" class="cw-theme-${theme==="flat-light"?"light":"dark"}"`;
    const scripts=kind==="editor"?[...editorModules.map(name=>"editor/"+name),"editor.js"]:kind==="tracker"?["tracker.js"]:[];
    const mount=kind==="analyzer"?`import('/assets/js/analyzer/index.js').then(()=>Analyzer.mount(document.querySelector('#page-analyzer')))` :kind==="events"?`import('/assets/js/events/index.js').then(m=>m.default.mount(document.querySelector('#page-events'),{mode:'raw'}))`:kind==="tracker"?`TrackerPage.mount(document.querySelector('#page-tracker'))`:"Promise.resolve()";
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${attrs} data-tab="${kind}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${[...styles,theme==="original"?"themes/original-coverage":"themes/flat"].map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><main id="layout"><section id="page-${kind}" class="cw-themed-page"></section></main><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/dialog-focus.js"></script>${scripts.map(file=>`<script src="/assets/js/${file}"></script>`).join("")}<script>window.ready=${mount};</script></body></html>`});
  });
  await page.goto("https://mobile-pages.test/");
  await page.evaluate(() => window.ready);
  await page.locator({editor:"#cw-tbody tr",tracker:".trk-play",events:".ev-row",analyzer:".an-table tbody tr"}[kind]).first().waitFor();
  await page.evaluate(() => document.fonts.ready);
  return {page,state,requests};
}

async function screenshot(page, name) {
  if (!process.env.CW_MOBILE_PAGES_SCREENSHOTS) return;
  await mkdir(process.env.CW_MOBILE_PAGES_SCREENSHOTS,{recursive:true});
  await page.screenshot({path:`${process.env.CW_MOBILE_PAGES_SCREENSHOTS}/${name}.png`,fullPage:true});
}

async function noOverflow(page) {
  const layout = await page.evaluate(() => ({width: innerWidth, scroll: document.documentElement.scrollWidth, overflow: [...document.querySelectorAll("main *")].filter(el => el.checkVisibility() && el.getBoundingClientRect().right > innerWidth + 1).slice(0, 8).map(el => el.className || el.id)}));
  assert.ok(layout.scroll <= layout.width + 1, JSON.stringify(layout));
}

for(const theme of ["original","flat-dark","flat-light"]) {
  for(const [width, height] of [[320,740],[390,844],[740,360]]) {
    for(const kind of ["editor","tracker","events","analyzer"]) {
      test(`${theme} ${kind}: phone workflow at ${width}x${height}`,async t=>{
        const {page,state,requests}=await fixture(t,kind,theme,width);
        await page.setViewportSize({width, height});
        await noOverflow(page);
        await screenshot(page,`${theme}-${kind}-${width}`);
        if(kind==="editor") {
          assert.match(await page.locator('.cw-editor-source-summary').textContent(),/Current State.*Watchlist/);
          assert.equal(await page.locator('#cw-wide-btn').isVisible(),false);
          assert.equal(await page.locator('.cw-empty-extra').first().isVisible(),false);
          await page.locator('.cw-mobile-fields-toggle').first().tap();
          assert.equal(await page.locator('#cw-tbody .cw-col-year').first().isVisible(),true);
          await page.locator('.cw-mobile-fields-toggle').first().tap();
          await page.locator('#cw-tbody .cw-checkbox').first().check();
          assert.equal(await page.locator('#cw-bulk').isVisible(),true);
          await noOverflow(page);
          await page.locator('#cw-bulk-clear').tap();
          await page.locator('.cw-editor-filters>summary').tap();
          await page.locator('#cw-kind').selectOption('ratings');
          await page.waitForFunction(()=>document.querySelector('.cw-editor-source-summary').textContent.includes('Ratings'));
          await page.waitForFunction(()=>document.querySelector('.cw-col-extra .cw-extra-display')?.disabled===false);
          await page.locator('.cw-col-extra .cw-extra-display').first().tap();
          await page.locator('.cw-pop').first().waitFor();
          await noOverflow(page);
          await screenshot(page,`${theme}-${kind}-${width}-dialog`);
          await page.locator('[data-editor-rating="8"]').tap();
          await page.waitForFunction(()=>!document.querySelector('.cw-pop'));
          assert.match(await page.locator('.cw-col-extra .cw-extra-display').first().textContent(),/8/);
          state.empty=true;
          await page.locator('#cw-kind').selectOption('watchlist');
          await page.locator('#cw-empty').waitFor();
        } else if(kind==="tracker") {
          assert.equal(await page.locator('#trk-mobile-view').isVisible(),true);
          assert.equal(await page.locator('#trk-mobile-view option').count(),8);
          assert.equal(await page.locator('.trk-tabs').isVisible(),false);
          await page.locator('.trk-play .cw-merged-title').first().tap();
          await page.locator('.trk-sheet').waitFor();
          await page.locator('.trk-sheet .trk-detail').waitFor();
          await noOverflow(page);
          await screenshot(page,`${theme}-${kind}-${width}-dialog`);
          await page.locator('.trk-sheet [data-trk-sheet="close"]').first().tap();
          await page.locator('#trk-mobile-view').selectOption('collection');
          await page.waitForFunction(()=>document.querySelector('#trk-mobile-view')?.value==='collection'&&document.querySelector('.trk-list'));
          assert(requests.some(url=>url.startsWith('/api/tracker/items?')&&url.includes('feature=collection')));
          await page.locator('[data-trk-layout="grid"]').tap();
          await page.locator('.trk-cover-grid').waitFor();
          await noOverflow(page);
          await page.locator('[data-trk-layout="list"]').tap();
          await page.locator('tbody [data-trk-pick]').first().check();
          assert.equal(await page.locator('[data-trk-slot="selection-bar"]').isVisible(),true);
          await noOverflow(page);
          state.empty=true;
          await page.locator('#trk-mobile-view').selectOption('watchlist');
          await page.waitForFunction(()=>!document.querySelector('.trk-list tbody tr'));
          assert.equal(await page.locator('#trk-mobile-view').inputValue(),'watchlist');
        } else if(kind==="events") {
          const time=page.locator('.ev-row .ev-time').first();
          assert.equal(await time.isVisible(),true);
          assert.ok((await time.textContent()).trim().length>3);
          assert.ok((await page.locator('.ev-ack').first().boundingBox()).height>=44);
          assert.equal(await page.locator('.ev-rowact').nth(1).evaluate(el=>getComputedStyle(el).opacity),'1');
          assert.ok((await page.locator('#ev-prev-top').boundingBox()).height>=44);
          assert.equal(await page.locator('#ev-prev-top').isDisabled(),true);
          await page.locator('#ev-more').tap();
          assert.equal(await page.locator('#ev-morefilters').isVisible(),true);
          await noOverflow(page);
          await page.locator('#ev-more').tap();
          await page.locator('.ev-row .ev-line').nth(1).tap();
          await page.locator('#ev-detail-back').waitFor();
          assert.equal(await page.locator('.ev-col-list').isVisible(),false);
          await noOverflow(page);
          await screenshot(page,`${theme}-${kind}-${width}-detail`);
          await page.locator('#ev-detail-back').tap();
          assert.equal(await page.locator('.ev-col-list').isVisible(),true);
          state.eventTotal=1000;
          await page.locator('#ev-refresh').tap();
          await page.waitForFunction(()=>!document.querySelector('#ev-next').disabled);
          await page.locator('#ev-next').tap();
          await page.waitForFunction(()=>document.querySelector('.ev-pg-num.on')?.textContent==='2');
          assert.equal(await page.locator('.ev-pg-num:visible').count(),1);
          await noOverflow(page);
          state.empty=true;
          await page.locator('#ev-refresh').tap();
          await page.waitForFunction(()=>!document.querySelector('.ev-row'));
        } else {
          assert.equal(await page.locator('#an-mobile-view').isVisible(),true);
          assert.equal(await page.locator('#an-detail').isVisible(),false);
          assert.equal(await page.locator('.an-tabs').isVisible(),false);
          assert.equal(await page.locator('.an-table-scroll').evaluate(el=>el.scrollWidth>el.clientWidth),false);
          assert.equal(await page.locator('td[data-label="Finding"]').first().isVisible(),true);
          const row=page.locator('.an-item-button').nth(1);
          await row.scrollIntoViewIfNeeded();
          const scroll=await page.evaluate(()=>scrollY);
          await row.tap();
          await page.locator('#an-detail').waitFor();
          assert.equal(await page.locator('.an-list-column').isVisible(),false);
          assert.equal(await page.locator('#an-close-detail').isVisible(),true);
          await noOverflow(page);
          await screenshot(page,`${theme}-${kind}-${width}-detail`);
          await page.locator('#an-close-detail').tap();
          assert.equal(await page.locator('.an-list-column').isVisible(),true);
          assert.ok(Math.abs(await page.evaluate(()=>scrollY)-scroll)<3);
          assert.equal(await page.evaluate(()=>document.activeElement?.dataset.row),'1');
          await page.locator('#an-mobile-view').selectOption('system');
          assert.equal(await page.locator('#an-view-title').textContent(),'System health');
          await page.locator('#an-mobile-view').selectOption('all');
          await page.locator('.an-item-button').first().waitFor();
          state.empty=true;
          await page.locator('#an-run').tap();
          await page.waitForFunction(()=>document.querySelector('#an-result-count')?.textContent==='0 results');
        }
        await noOverflow(page);
        await screenshot(page,`${theme}-${kind}-${width}-empty`);
      });
    }
  }
  for(const kind of ["editor","tracker","events","analyzer"]) {
    test(`${theme} ${kind}: desktop navigation stays available`,async t=>{
      const {page}=await fixture(t,kind,theme,1440);
      await noOverflow(page);
      if(kind==='editor') assert.equal(await page.locator('#cw-wide-btn').isVisible(),true);
      if(kind==='tracker') {
        assert.equal(await page.locator('#trk-mobile-view').isVisible(),false);
        assert.equal(await page.locator('.trk-tabs').isVisible(),true);
        await page.locator('[data-trk-view="collection"]').click();
        assert.equal(await page.locator('#trk-mobile-view').inputValue(),'collection');
      }
      if(kind==='analyzer') {
        assert.equal(await page.locator('#an-mobile-view').isVisible(),false);
        assert.equal(await page.locator('.an-tabs').isVisible(),true);
        assert.equal(await page.locator('#an-detail').isVisible(),true);
      }
    });
  }
}

for (const theme of ["original", "flat-dark", "flat-light"]) for (const kind of ["editor", "tracker", "events", "analyzer"]) {
  test(`${theme} ${kind}: tablet portrait, landscape and desktop layouts`, async t => {
    const {page} = await fixture(t, kind, theme, 1194);
    for (const [width, height] of [[844,390],[932,430],[768,1024],[1024,768],[1194,834],[1366,1024],[1920,1080]]) {
      await page.setViewportSize({width, height});
      await screenshot(page, `audit-${theme}-${kind}-${width}x${height}`);
      await noOverflow(page);
      if (["editor", "tracker"].includes(kind) && width <= 1320) {
        const bounds = await page.locator(".cw-wrap").evaluate(el => ({wrap: el.getBoundingClientRect().width, main: el.querySelector(".cw-main").getBoundingClientRect().width}));
        assert.ok(Math.abs(bounds.wrap - bounds.main) <= 1, JSON.stringify({theme, kind, width, ...bounds}));
      }
      if (kind === "tracker") {
        const rows = await page.locator(".trk-toolbar .trk-seg button").evaluateAll(buttons => buttons.map(el => el.getBoundingClientRect().top));
        assert.ok(Math.max(...rows) - Math.min(...rows) <= 1, `${theme} ${width}: media filters wrap inside their group`);
      }
    }
  });
}
