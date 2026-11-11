/* tests/tracker-watch-editor.browser.mjs */
/* CrossWatch - Quick watches and advanced episode history editing */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

async function fixture(t, {width=1440,height=900,theme="flat-dark"}={}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width,height},reducedMotion:"reduce"});
  page.setDefaultTimeout(6000);
  await page.addInitScript(() => localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history"})));
  const writes=[], requests=[], errors=[];
  let fail=false;
  page.on("pageerror", error => errors.push(error.message));
  await page.route("https://watch-editor.test/**",async route => {
    const url=new URL(route.request().url()), path=url.pathname;
    if(path.startsWith("/assets/")) {
      try { return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".woff2")?"font/woff2":"text/javascript"}); }
      catch { return route.fulfill({status:404,body:""}); }
    }
    if(path.startsWith("/art/")) return route.fulfill({status:404,body:""});
    if(path.startsWith("/api/")) {
      requests.push(path+url.search);
      let json={ok:true};
      if(path==="/api/tracker/summary") json={provider_instance:"default",instances:[{id:"default",label:"Default"}],counts:{history:0},health:{},notes:{total:0,statuses:{},tags:[]}};
      if(path==="/api/tracker/items") json={items:[],total:0};
      if(path==="/api/metadata/search") json={results:[{tmdb:123,title:"Example show",year:2020}]};
      if(path==="/api/metadata/resolve") json={result:{ids:{tmdb:"123"}}};
      if(path==="/api/tracker/watch-editor") json={revision:"snapshot-1",seasons:[{season:1,episodes:3,name:"Season 1"},{season:2,episodes:2,name:"Season 2"}],plays:[{key:"old",season:1,episode:1,date:"2025-01-01T12:00:00Z"},{key:"latest",season:1,episode:1,date:"2025-02-01T12:00:00Z"}]};
      if(path==="/api/metadata/tmdb/season") json={episodes:Array.from({length:url.searchParams.get("season")==="2"?2:3},(_,i)=>({episode:i+1,name:`Episode title ${i+1}`,air_date:`2025-01-0${i+1}`}))};
      if(path==="/api/tracker/history/bulk" || path==="/api/tracker/add") {
        writes.push(route.request().postDataJSON());
        if(fail) return route.fulfill({status:409,json:{ok:false,error:"changed_since"}});
        json={ok:true,batch:"batch-1",count:route.request().postDataJSON().episodes?.length || 1};
      }
      return route.fulfill({json});
    }
    const attrs=theme==="original"?'class="cw-theme-original"':`data-cw-theme="${theme}"`;
    const css=["fonts/material-symbols-rounded","themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","ui-shell",theme==="original"?"themes/original-coverage":"themes/flat"];
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${attrs}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><main id="layout" class="full"><section id="page-tracker" class="cw-themed-page"></section></main><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
  });
  await page.goto("https://watch-editor.test/");
  const open=async () => {
    await page.locator('[data-trk-action="log"]').first().click();
    await page.locator('[data-trk-log-type="show"]').click();
    await page.locator('[data-trk-log-q]').fill("Example");
    await page.locator('[data-trk-log-pick="0"]').click();
    await page.locator('[data-trk-log-episode]').waitFor();
    await page.waitForFunction(()=>document.querySelector('[data-trk-log-episode]')?.textContent.includes('Episode title'));
  };
  await open();
  t.after(()=>assert.deepEqual(errors,[]));
  return {page,writes,requests,open,fail:value=>{fail=value;}};
}

test("Quick watch selects one unwatched episode; an existing watch becomes a rewatch",async t=>{
  const {page,writes,requests,open}=await fixture(t);
  assert.equal(await page.locator('[data-trk-log-episode]').inputValue(),"2");
  assert.equal(await page.locator('[data-trk-log-episode] option[value="all"]').count(),0);
  assert.equal(requests.filter(p=>p.includes('/tmdb/season')).length,1);
  await page.locator('form[data-trk-form="log"] button[type="submit"]').click();
  await page.locator('.trk-sheet-wrap').waitFor({state:"hidden"});
  assert.equal(writes[0].action,"add");
  assert.deepEqual(writes[0].episodes.map(({season,episode})=>({season,episode})),[{season:1,episode:2}]);
  await open();
  await page.locator('[data-trk-log-episode]').selectOption("1");
  assert.match(await page.locator('.trk-watch-tools').textContent(),/This adds a rewatch/);
  await page.locator('form[data-trk-form="log"] button[type="submit"]').click();
  await page.locator('.trk-sheet-wrap').waitFor({state:"hidden"});
  assert.equal(writes[1].action,"rewatch");
});

test("Guided editor preserves selections, reviews dates and defaults to latest watch",async t=>{
  const {page,writes,requests,fail}=await fixture(t);
  const next=()=>page.locator('form[data-trk-form="log"] button[type="submit"]').click();
  const back=()=>page.locator('[data-trk-log-back]').click();
  await page.locator('[data-trk-log-advanced]').click();
  assert.equal(await page.locator('[data-trk-watch-pick]').count(),0);
  assert.equal(await page.locator('button[type="submit"]').isDisabled(),true);
  assert.match(await page.locator('[data-trk-log-operation="add"]').textContent(),/4.*episodes/);
  await page.locator('[data-trk-log-operation="add"]').click();
  await next();
  await page.locator('[data-trk-log-expand="1"]').click();
  assert.equal(await page.locator('[data-trk-watch-pick="1:1"]').isDisabled(),true);
  assert.match(await page.locator('.trk-watch-list').textContent(),/2 plays/);
  await page.locator('[data-trk-log-select="1"]').click();
  await page.locator('[data-trk-log-expand="2"]').click();
  await page.locator('[data-trk-watch-pick="2:1"]').check();
  assert.equal(await page.locator('[data-trk-watch-pick="1:2"]').isChecked(),true);
  assert.equal(requests.filter(p=>p.includes('/tmdb/season')).length,2);
  await next();
  await page.locator('[data-trk-log-date-mode="range"]').click();
  await page.locator('[data-trk-log-when]').fill('2025-03-01T12:00');
  await page.locator('[data-trk-log-end]').fill('2025-03-03T12:00');
  await next();
  assert.equal(writes.length,0);
  assert.match(await page.locator('.trk-watch-review').textContent(),/3 episodes.*3 watches affected/);
  assert.equal(await page.locator('.trk-watch-review li').count(),3);
  await back();
  assert.equal(await page.locator('[data-trk-log-when]').inputValue(),'2025-03-01T12:00');
  await back();
  assert.equal(await page.locator('[data-trk-watch-pick="2:1"]').isChecked(),true);
  await back();
  await page.locator('[data-trk-log-operation="dates"]').click();
  await next();
  assert.equal(await page.locator('[data-trk-log-scope]').inputValue(),"latest");
  assert.equal(await page.locator('[data-trk-watch-pick="1:2"]').isDisabled(),true);
  await page.locator('[data-trk-watch-pick="1:1"]').check();
  await page.locator('[data-trk-log-scope]').selectOption('all');
  assert.match(await page.locator('form').textContent(),/Earlier watches move by the same amount/);
  await next();
  await next();
  assert.equal(await page.locator('.trk-watch-review li').count(),2);
  fail(true);
  await next();
  await page.locator('.trk-error').waitFor();
  assert.match(await page.locator('.trk-error').textContent(),/History changed/);
  await page.locator('[data-trk-log-reload]').click();
  await page.locator('[data-trk-watch-pick="1:1"]').waitFor();
  assert.equal(await page.locator('[data-trk-watch-pick="1:1"]').isChecked(),false);
  assert.equal(await page.locator('[data-trk-log-scope]').inputValue(),"all");
  await back();
  await page.locator('[data-trk-log-operation="remove"]').click();
  await next();
  await page.locator('[data-trk-log-scope]').selectOption('latest');
  await page.locator('[data-trk-watch-pick="1:1"]').check();
  await next();
  assert.deepEqual(await page.locator('.trk-watch-totals dd').allTextContents(),['1','1','0']);
  fail(false);
  await next();
  await page.locator('.trk-sheet-wrap').waitFor({state:"hidden"});
  assert.equal(writes.at(-1).scope,"latest");
  assert.equal(writes.at(-1).action,"remove");
});

test("Release dates use cached season details; movie and watchlist flows stay available",async t=>{
  const {page,writes,requests,open}=await fixture(t);
  await page.locator('[data-trk-log-date-mode="release"]').click();
  await page.locator('form[data-trk-form="log"] button[type="submit"]').click();
  await page.locator('.trk-sheet-wrap').waitFor({state:"hidden"});
  assert.equal(writes[0].episodes[0].watched_at,"2025-01-02T12:00:00.000Z");
  assert.equal(requests.filter(p=>p.includes('/tmdb/season')).length,1);
  await open();
  await page.locator('[data-trk-log-target="watchlist"]').click();
  await page.locator('form[data-trk-form="log"] button[type="submit"]').click();
  await page.locator('.trk-sheet-wrap').waitFor({state:"hidden"});
  assert.equal(writes.at(-1).feature,"watchlist");
  assert.equal(writes.at(-1).items[0].type,"show");
  await page.locator('[data-trk-action="log"]').first().click();
  await page.locator('[data-trk-log-q]').fill("Example");
  await page.locator('[data-trk-log-pick="0"]').click();
  await page.locator('form[data-trk-form="log"] button[type="submit"]').click();
  await page.locator('.trk-sheet-wrap').waitFor({state:"hidden"});
  assert.equal(writes.at(-1).feature,"history");
  assert.equal(writes.at(-1).items[0].type,"movie");
});

if(process.env.CW_WATCH_EDITOR_LAYOUTS) for(const theme of ["original","flat-dark","flat-light"]) for(const [width,height] of [[390,844],[844,390],[768,1024],[1024,768],[1180,820],[1440,900],[1920,1080]]) {
  test(`${theme}: watch editor at ${width}x${height}`,async t=>{
    const {page}=await fixture(t,{width,height,theme});
    for(const mode of ["quick","advanced","episodes","dates","review"]) {
      if(mode==="advanced") await page.locator('[data-trk-log-advanced]').click();
      if(mode==="episodes") {
        await page.locator('[data-trk-log-operation="add"]').click();
        await page.locator('form[data-trk-form="log"] button[type="submit"]').click();
        await page.locator('[data-trk-log-expand="1"]').click();
        await page.locator('[data-trk-log-select="all"]').click();
      }
      if(mode==="dates" || mode==="review") await page.locator('form[data-trk-form="log"] button[type="submit"]').click();
      const layout=await page.locator('.trk-sheet').evaluate(el=>({width:el.clientWidth,scrollWidth:el.scrollWidth,nested:[...el.querySelectorAll('*')].filter(n=>['auto','scroll'].includes(getComputedStyle(n).overflowY)&&n.scrollHeight>n.clientHeight+1).length}));
      assert.ok(layout.scrollWidth<=layout.width+1,JSON.stringify(layout));
      assert.equal(layout.nested,0);
      const submit=page.locator('form[data-trk-form="log"] button[type="submit"]');
      await submit.scrollIntoViewIfNeeded();
      const box=await submit.boundingBox();
      assert.ok(box.x>=0&&box.x+box.width<=width&&box.y>=0&&box.y+box.height<=height);
      if(process.env.CW_WATCH_EDITOR_SCREENSHOTS&&width===1440) {
        await mkdir(process.env.CW_WATCH_EDITOR_SCREENSHOTS,{recursive:true});
        await page.screenshot({path:`${process.env.CW_WATCH_EDITOR_SCREENSHOTS}/${theme}-${mode}.png`});
      }
    }
  });
}
