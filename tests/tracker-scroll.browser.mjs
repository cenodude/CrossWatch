/* tests/tracker-scroll.browser.mjs */
/* CrossWatch - Tracker detail expansion and single-scroll responsive layouts */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const viewports = [[320,740],[390,844],[430,932],[740,360],[844,390],[768,1024],[820,1180],[1024,768],[1194,834],[1366,1024],[1366,768],[1440,900],[1920,1080]];
const seasons = Array.from({length:30}, (_, season) => ({season, episodes:12}));
const shows = Array.from({length:48}, (_, index) => ({key:`show:${index}`,title:index ? `Example show ${index}` : "South Park",type:"show",art:{tmdb:index + 1,kind:"tv"},last_watched:"2026-11-08T12:00:00Z",last_season:29,last_episode:4,plays:8}));
const plays = Array.from({length:8}, (_, index) => ({key:`play:${index}`,type:"episode",season:29,episode:index + 1,date:`2026-10-${String(index + 1).padStart(2,"0")}T12:00:00Z`}));
const source = await readFile(new URL("../ui_frontend.py", import.meta.url), "utf8");
const assets = new Map();

async function fixture(browser, theme, width, height) {
  const page = await browser.newPage({viewport:{width,height},hasTouch:width<=1366,isMobile:width<768,reducedMotion:"reduce"});
  page.setDefaultTimeout(7000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  const html = await page.evaluate(({source,theme}) => {
    const doc = new DOMParser().parseFromString(source.slice(source.indexOf("<!doctype html>"), source.indexOf("</html>",source.indexOf("<!doctype html>")) + 7), "text/html");
    doc.querySelectorAll("script,style").forEach(el => el.remove());
    doc.querySelector("#cw-main-loading")?.remove();
    if (theme !== "original") doc.documentElement.dataset.cwTheme = theme;
    doc.documentElement.dataset.tab = "tracker";
    doc.documentElement.className = theme === "original" ? "cw-theme-original" : `cw-theme-${theme === "flat-light" ? "light" : "dark"}`;
    doc.querySelector(theme === "original" ? "#cw-theme-flat-css" : "#cw-theme-original-css")?.remove();
    const layout = doc.querySelector("#layout");
    layout.classList.add("full");
    for (const el of layout.children) el.classList.toggle("hidden", el.id !== "page-tracker");
    return "<!doctype html>" + doc.documentElement.outerHTML.replaceAll("__CW_ASSET_BLOCK__", "").replaceAll("__CW_CURRENT_VERSION__", "Preview");
  }, {source,theme});
  await page.route("https://tracker-scroll.test/**", async route => {
    const url = new URL(route.request().url()), path = url.pathname;
    if (path.startsWith("/assets/")) {
      try {
        if (!assets.has(path)) assets.set(path, await readFile(new URL(".." + path, import.meta.url)));
        return route.fulfill({body:assets.get(path),contentType:path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : path.endsWith(".svg") ? "image/svg+xml" : "text/javascript"});
      } catch { return route.fulfill({status:404,body:""}); }
    }
    if (path.startsWith("/art/")) return route.fulfill({status:404,body:""});
    if (!path.startsWith("/api/")) return route.fulfill({contentType:"text/html",body:html});
    let json = {ok:true};
    if (path === "/api/tracker/summary") json = {provider_instance:"default",instances:[{id:"default",label:"Default"},{id:"other",label:"Other tracker"}],counts:{history:48},health:{},notes:{total:0,statuses:{},tags:[]}};
    if (path === "/api/tracker/stats") json = {week:3,month:9,year:244,total:244};
    if (path === "/api/tracker/shows") json = {shows,total:shows.length};
    if (path === "/api/tracker/title") {
      const title = shows.find(show => show.key === url.searchParams.get("show")) || shows[0];
      const empty = title.key === "show:1";
      json = {title,show:true,plays:empty ? [] : plays,ratings:[],watchlist:[],collection:[],progress:[],attention:[],note:null,note_key:empty ? "" : title.key,seasons:empty ? [] : [{season:29,episodes:plays.map(play => ({episode:play.episode,watched_at:play.date}))}]};
    }
    if (path === "/api/tracker/show-progress") json = url.searchParams.get("tmdb") === "2" ? {available:false} : {available:true,seasons,watched:8,total:348,next:{season:29,episode:9}};
    if (path === "/api/metadata/tmdb/season") json = {episodes:[9,10].map(episode => ({episode,name:`Upcoming episode ${episode}`,air_date:"2026-12-01",runtime:22,overview:"A longer episode description to check the expanded title panel at different screen sizes."}))};
    return route.fulfill({json});
  });
  await page.goto("https://tracker-scroll.test/#tracker");
  await page.evaluate(() => {
    localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"shows",layouts:{shows:"grid"}}));
    window.CW = {AuthState:{read:()=>({isAdmin:true,isManaged:false,permissions:{write:true}})}};
  });
  for (const script of ["helpers/account-menu", "helpers/icon-select", "helpers/selection-bar", "js/tracker"]) await page.addScriptTag({url:`/assets/${script}.js`});
  await page.evaluate(() => TrackerPage.mount(document.querySelector("#page-tracker")));
  await page.locator(".trk-grid-card").first().waitFor();
  await page.evaluate(() => document.fonts.ready);
  return {page,errors};
}

async function checkScroll(page, label) {
  const result = await page.evaluate(() => ({
    width:innerWidth, scroll:document.documentElement.scrollWidth,
    nested:[...document.querySelectorAll("#page-tracker *")].filter(el => el.checkVisibility() && /auto|scroll/.test(getComputedStyle(el).overflowY) && el.scrollHeight > el.clientHeight + 1).map(el => el.className),
  }));
  assert.ok(result.scroll <= result.width + 1, `${label}: horizontal overflow ${JSON.stringify(result)}`);
  assert.deepEqual(result.nested, [], `${label}: nested vertical scrollbars`);
}

for (const theme of ["original","flat-dark","flat-light"]) {
  test(`${theme}: tracker details and scrolling across phones, tablets and computers`, async t => {
    const browser = await ({chromium,webkit})[process.env.CW_AUDIT_BROWSER || "chromium"].launch({headless:true});
    t.after(() => browser.close());
    for (const [width,height] of viewports) {
      const {page,errors} = await fixture(browser,theme,width,height);
      const label = `${theme} ${width}x${height}`;
      const panel = page.locator('[data-trk-slot="title"]');
      const controls = page.locator('[data-trk-slot="controls"]');
      const side = page.locator(".cw-side");
      await checkScroll(page, label + " empty");
      await page.locator('.trk-grid-card[data-trk-ref-show="show:0"]').click();
      await panel.locator('[data-trk-season="29"]').waitFor();
      await panel.locator(".trk-episode-preview").last().waitFor();
      assert.deepEqual(await panel.locator("[data-trk-season]").evaluateAll(nodes => nodes.map(node => Number(node.dataset.trkSeason))), [29,28,27], label);
      assert.deepEqual(await panel.locator("[data-trk-play-edit]").evaluateAll(nodes => nodes.map(node => node.dataset.trkPlayEdit)), ["play:7","play:6","play:5"], label);
      assert.equal(await page.locator(".trk-sheet:visible").count(),0,label);
      if (width <= 1320) assert.equal(await page.locator('[data-trk-slot="filters"]').getAttribute("open"), "", label);
      await checkScroll(page, label + " collapsed");
      for (const kind of ["seasons","plays"]) {
        const button = panel.locator(`[data-trk-expand="${kind}"]`);
        await button.focus();
        await page.keyboard.press("Enter");
        assert.equal(await button.getAttribute("aria-expanded"), "true", label);
        assert.equal(await button.evaluate(el => el === document.activeElement), true, label);
      }
      assert.equal(await panel.locator("[data-trk-season]").count(),30,label);
      assert.equal(await panel.locator("[data-trk-season]").last().textContent().then(text=>text.startsWith("Specials")),true,label);
      assert.equal(await panel.locator(".trk-play-row").count(),8,label);
      await panel.locator('[data-trk-season="29"]').click();
      assert.equal(await panel.locator(".trk-ep-chip").count(),12,label);
      await checkScroll(page, label + " expanded");
      await panel.locator('[data-trk-expand="plays"]').scrollIntoViewIfNeeded();
      const bottom = await panel.locator('[data-trk-expand="plays"]').boundingBox();
      assert.ok(bottom.y >= 0 && bottom.y + bottom.height <= height + 1, label + " last action reachable");
      for (const kind of ["seasons","plays"]) {
        const toggle = panel.locator(`[data-trk-expand="${kind}"]`);
        await toggle.evaluate(el => el.scrollIntoView({block:"center"}));
        await page.waitForTimeout(200);
        const box = await toggle.boundingBox();
        assert.equal(await toggle.evaluate(el => { const r = el.getBoundingClientRect(); return el.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2)); }),true,label + " collapse control reachable");
        if (width <= 1366) await page.touchscreen.tap(box.x + box.width / 2, box.y + box.height / 2);
        else await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
        assert.equal(await panel.locator(`[data-trk-expand="${kind}"]`).getAttribute("aria-expanded"),"false",label);
      }
      await panel.locator('[data-trk-season="29"]').click();
      if (width <= 1320) await panel.locator('[data-trk-detail="back"]').click();
      await page.evaluate(() => window.scrollTo(0,1000));
      await page.waitForTimeout(80);
      const sticky = await controls.evaluate(el => ({top:el.getBoundingClientRect().top,expected:parseFloat(getComputedStyle(el).top),position:getComputedStyle(el).position}));
      assert.notEqual(sticky.position, "sticky", label + " toolbar scrolls with the page");
      if (width > 1320) {
        const bounds = await side.evaluate(el => ({top:el.getBoundingClientRect().top,expected:parseFloat(getComputedStyle(el.parentElement).top)}));
        assert.ok(Math.abs(bounds.top - bounds.expected) < 2, label + " sticky details " + JSON.stringify(bounds));
      }
      await page.locator('.trk-grid-card[data-trk-ref-show="show:1"]').click();
      await panel.locator(".trk-detail-title").filter({hasText:"Example show 1"}).waitFor();
      assert.equal(await panel.locator("[data-trk-expand]").count(),0,label + " empty title");
      await checkScroll(page, label + " empty title");
      if (width <= 1320) await panel.locator('[data-trk-detail="back"]').click();
      await page.locator('.trk-grid-card[data-trk-ref-show="show:0"]').click();
      await panel.locator('[data-trk-season="29"]').waitFor();
      assert.equal(await panel.locator("[data-trk-season]").count(),3,label + " reset seasons");
      assert.equal(await panel.locator(".trk-play-row").count(),3,label + " reset plays");
      if (width === 1366 && height === 1024) {
        await page.setViewportSize({width:1024,height:1366});
        await panel.locator('[data-trk-season="29"]').waitFor();
        await checkScroll(page, label + " rotated to portrait");
        assert.equal(await panel.isVisible(),true,label + " title retained on rotation");
        await page.setViewportSize({width,height});
        await checkScroll(page, label + " rotated to landscape");
      }
      if (process.env.CW_TRACKER_SCROLL_SCREENSHOTS) {
        await mkdir(process.env.CW_TRACKER_SCROLL_SCREENSHOTS,{recursive:true});
        await page.screenshot({path:`${process.env.CW_TRACKER_SCROLL_SCREENSHOTS}/${theme}-${width}x${height}.png`});
      }
      assert.deepEqual(errors,[],label);
      await page.close();
    }
  });
}
