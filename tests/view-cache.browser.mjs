/* tests/view-cache.browser.mjs */
/* CrossWatch - Cached tracker views and background refresh behavior */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import './i18n-browser-setup.mjs';
import test from "node:test";
import assert from "node:assert/strict";
import {mkdir, readFile} from "node:fs/promises";
import {execFileSync} from "node:child_process";
import {chromium} from "playwright";

test("Refresh icons follow concurrent requests, failures and cancellation", async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route("https://refresh.test/**", route => route.fulfill({contentType:"text/html", body:'<section id="host"><button data-trk-action="refresh"><span class="material-symbols-rounded">refresh</span></button></section>'}));
  await page.goto("https://refresh.test/");
  await page.addScriptTag({path:"assets/helpers/view-cache.js"});
  await page.addStyleTag({path:"assets/css/pages.css"});
  await page.evaluate(() => {
    const host = document.querySelector("#host");
    CW.ViewCache.bindRefresh(host, url => url.startsWith("/api/tracker/"));
    window.finish = {};
    window.requests = ["summary", "items"].map(name => CW.ViewCache.read(`/api/tracker/${name}`, () => new Promise(resolve => { finish[name] = resolve; })));
  });
  const spinning = () => page.locator("#host span").evaluate(node => getComputedStyle(node).animationName);
  assert.equal(await spinning(), "cwRefreshSpin");
  await page.evaluate(async () => { finish.summary({ok:true}); await requests[0]; });
  assert.equal(await spinning(), "cwRefreshSpin");
  await page.evaluate(async () => { finish.items({ok:true}); await requests[1]; });
  assert.equal(await spinning(), "none");
  await page.evaluate(async () => {
    window.controller = new AbortController();
    await CW.ViewCache.read("/api/tracker/items", async () => ({_view:{ready:true, refreshing:true, version:"1"}}), {signal:controller.signal});
  });
  assert.equal(await spinning(), "cwRefreshSpin");
  await page.evaluate(() => controller.abort());
  assert.equal(await spinning(), "none");
  await page.evaluate(async () => {
    await CW.ViewCache.read("/api/tracker/items", async () => { throw new Error("Offline"); }).catch(() => {});
  });
  assert.equal(await spinning(), "none");
  await page.evaluate(async () => {
    await CW.ViewCache.read("/api/tracker/items", async () => ({_view:{ready:true, refreshing:true, version:"1"}}));
    document.querySelector("#host").innerHTML = '<button data-trk-action="refresh"><span class="material-symbols-rounded">refresh</span></button>';
  });
  assert.equal(await spinning(), "cwRefreshSpin");
  await page.emulateMedia({reducedMotion:"reduce"});
  assert.equal(await spinning(), "none");
  await page.emulateMedia({reducedMotion:"no-preference"});
  assert.equal(await spinning(), "cwRefreshSpin");
  await page.evaluate(() => CW.ViewCache.invalidate());
  assert.equal(await spinning(), "none");
  assert.equal(await page.getByRole("progressbar").count(), 0);
});

for (const direct of [false, true]) test(`${direct ? "Direct Collection" : "Hub to Collection"} prepares cached results and refreshes the shared hero`, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1440, height:900}});
  page.setDefaultTimeout(8000);
  const template = execFileSync("python", ["-c", "import ui_frontend; print(ui_frontend.get_hub_html({'is_admin':True,'username':'preview'}))"], {encoding:"utf8", maxBuffer:1024*1024});
  const html = await page.evaluate(source => {
    const doc = new DOMParser().parseFromString(source, "text/html");
    doc.querySelectorAll("script").forEach(node => node.remove());
    const boot = doc.createElement("script");
    boot.textContent = 'window.CW={AuthState:{read:()=>({isAdmin:true,permissions:{write:true}})},ItemActions:{openMenu:options=>options.onChanged()}};';
    doc.body.append(boot);
    for (const path of ["helpers/view-cache", "helpers/card-actions", "helpers/airing-card", "helpers/selection-bar", "helpers/profile-datetime", "helpers/month-bar", "helpers/icon-select", "js/profile-page"]) {
      const script = doc.createElement("script");
      script.src = `/assets/${path}.js`;
      doc.body.append(script);
    }
    return "<!doctype html>" + doc.documentElement.outerHTML;
  }, template);
  let phase = "cold";
  let heroPhase = "cached";
  let overviewPhase = "cached";
  const queries = [], heroQueries = [], overviewQueries = [], errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.route("https://hub-cache.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      try { return route.fulfill({body:await readFile(new URL(".." + url.pathname, import.meta.url)), contentType:url.pathname.endsWith(".css") ? "text/css" : url.pathname.endsWith(".woff2") ? "font/woff2" : "text/javascript"}); }
      catch { return route.fulfill({status:404, body:""}); }
    }
    if (url.pathname.startsWith("/api/")) {
      let json = {ok:true};
      if (url.pathname === "/api/profile") json.user = {is_admin:true, username:"preview", permissions:{write:true}, preferences:{}, sessions:[]};
      if (url.pathname === "/api/dashboard/widgets" && url.searchParams.get("view") === "hero") {
        heroQueries.push(Object.fromEntries(url.searchParams));
        json = {ok:true, recent_history:{items:[{type:"movie", title:heroPhase === "edited" ? "Edited hero movie" : heroPhase === "fresh" ? "Fresh hero movie" : "Cached hero movie", watched_at:"2026-11-08T12:00:00Z"}]}, recent_scrobble:{items:[]}, _view:{ready:true, refreshing:heroPhase !== "edited", version:heroPhase, retry_after:1}};
      } else if (url.pathname === "/api/dashboard/widgets") {
        overviewQueries.push(Object.fromEntries(url.searchParams));
        json = {ok:true, recent_history:{items:[{type:"movie", title:overviewPhase === "fresh" ? "Fresh overview movie" : "Cached overview movie", watched_at:"2026-11-08T12:00:00Z"}]}, recent_scrobble:{items:[]}, _view:{ready:true, refreshing:overviewPhase === "cached", version:overviewPhase, retry_after:1}};
      }
      if (url.pathname === "/api/profile/collection") {
        queries.push(Object.fromEntries(url.searchParams));
        assert.equal(url.searchParams.get("background"), "1");
        if (url.searchParams.has("summary_only")) json = {ok:true, total:100000, counts:{all:100000, movie:100000}};
        else if (phase === "cold") json._view = {ready:false, refreshing:true, retry_after:1};
        else json = {ok:true, total:100000, page:1, page_size:48, counts:{all:100000, movie:100000}, months:[], providers:[{provider:"plex", count:100000}], items:[{key:"tmdb:1", type:"movie", title:phase === "fresh" ? "Fresh collection movie" : "Cached collection movie", ids:{tmdb:"1"}, providers:["plex"], sources_by_provider:{plex:["default"]}}], _view:{ready:true, refreshing:phase === "cached", version:phase === "fresh" ? "2" : "1", error:phase === "failed" ? "refresh_failed" : null, retry_after:1}};
      }
      return route.fulfill({json});
    }
    return route.fulfill({contentType:"text/html", body:html});
  });
  await page.goto(`https://hub-cache.test/hub${direct ? "#collection" : ""}`);
  if (!direct) {
    await page.locator("#profile-hero-media strong").getByText("Cached overview movie", {exact:true}).waitFor();
    await page.locator('[data-profile-tab="collection"]').click();
  }
  await page.locator("#profile-hero-media strong").getByText("Cached hero movie", {exact:true}).waitFor();
  heroPhase = "fresh";
  await page.locator("#profile-hero-media strong").getByText("Fresh hero movie", {exact:true}).waitFor();
  assert.ok(heroQueries.length >= 2);
  assert.ok(heroQueries.every(query => query.background === "1"));
  await page.locator("#profile-panel-collection[data-view-loading]").waitFor();
  assert.equal(await page.locator(".cw-page-progress").count(), 0);
  assert.equal(await page.locator("#profile-hub-refresh .material-symbols-rounded").evaluate(node => getComputedStyle(node).animationName), "cwRefreshSpin");
  assert.equal(await page.getByText("Collections could not be loaded.", {exact:true}).count(), 0);
  phase = "cached";
  await page.getByText("Cached collection movie", {exact:true}).first().waitFor();
  phase = "failed";
  await page.locator('#profile-panel-collection [data-view-status]').getByText(/Refresh failed/).waitFor();
  assert.equal(await page.getByText("Cached collection movie", {exact:true}).first().isVisible(), true);
  phase = "fresh";
  await page.locator('#profile-panel-collection [data-view-status] button').click();
  await page.getByText("Fresh collection movie", {exact:true}).first().waitFor();
  await page.evaluate(() => {
    window.retainedCollection = document.querySelector('#profile-collection-grid').firstElementChild;
    retainedCollection.querySelector('img').dispatchEvent(new Event('error'));
  });
  const unchangedCollection = page.waitForResponse(response => new URL(response.url()).pathname === '/api/profile/collection');
  await page.evaluate(() => (CW.ViewCache.discard(), window.dispatchEvent(new CustomEvent('cw:view-updated', {detail:{url:'/api/profile/collection'}}))));
  await unchangedCollection;
  await page.waitForTimeout(100);
  assert.equal(await page.evaluate(() => retainedCollection === document.querySelector('#profile-collection-grid').firstElementChild), true);
  assert.equal(queries.some(query => query.summary_only === "true"), false);
  assert.ok(queries.some(query => query.retry_view === "1"));
  const collectionReload = page.waitForResponse(response => new URL(response.url()).pathname === "/api/profile/collection");
  await page.locator("[data-collection-actions]").first().click();
  await page.getByRole("menuitem", {name:"Manage providers"}).click();
  await collectionReload;
  heroPhase = "edited";
  await page.locator("#profile-hero-media strong").getByText("Edited hero movie", {exact:true}).waitFor();
  const beforeReturn = overviewQueries.length;
  const returned = page.waitForResponse(response => {
    const url = new URL(response.url());
    return url.pathname === "/api/dashboard/widgets" && url.searchParams.get("view") !== "hero";
  });
  await page.locator('[data-profile-tab="overview"]').click();
  await returned;
  overviewPhase = "fresh";
  await page.locator("#profile-hero-media strong").getByText("Fresh overview movie", {exact:true}).waitFor();
  assert.ok(overviewQueries.length > beforeReturn);
  assert.equal(await page.locator("#profile-hero > [data-view-status]").count(), 0);
  assert.deepEqual(errors, []);
});

test("Tracker refreshes all loaded timeline pages without mixing revisions or moving the reader", {timeout:20000}, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1440, height:900}});
  page.setDefaultTimeout(8000);
  await page.addInitScript(() => {
    localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history", byDay:true, viewSelectedAt:Date.now(), layouts:{history:"list"}}));
    const original = window.setTimeout;
    window.setTimeout = (fn, delay, ...args) => original(fn, delay >= 290000 && delay <= 300000 ? 200 : delay, ...args);
  });
  const records = Array.from({length:201}, (_, index) => ({key:`movie:${index}`, type:"movie", title:`Movie ${index}`, date:"2026-11-08T12:00:00Z"}));
  const added = {key:"new", type:"movie", title:"Newly watched movie", date:"2026-11-09T12:00:00Z"};
  let changed = false, edited = false, lagged = false, reportLag, release;
  const stalePage = new Promise(resolve => { reportLag = resolve; });
  const resume = new Promise(resolve => { release = resolve; });
  t.after(() => release());
  const reads = [], errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.route("https://timeline-range.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      try { return route.fulfill({body:await readFile(new URL(".." + url.pathname, import.meta.url)), contentType:"text/javascript"}); }
      catch { return route.fulfill({status:404, body:""}); }
    }
    if (url.pathname.startsWith("/api/")) {
      let json = {ok:true};
      if (url.pathname === "/api/tracker/summary") json = {ok:true, provider_instance:"default", instances:[{id:"default", label:"Default"}], counts:{history:201}, health:{}, notes:{total:0, statuses:{}, tags:[]}};
      if (url.pathname === "/api/tracker/items") {
        const offset = Number(url.searchParams.get("offset")), limit = Number(url.searchParams.get("limit"));
        reads.push({offset, changed});
        const stale = changed && offset === 200 && !lagged;
        if (stale) lagged = true;
        else if (changed && lagged) await resume;
        const fresh = changed && !stale;
        const rows = fresh ? [added, ...records.map(row => edited && row.key === "movie:200" ? {...row, title:"Updated older movie"} : row)] : records;
        json = {ok:true, total:rows.length, items:rows.slice(offset, offset + limit), _view:{ready:true, refreshing:false, version:`${fresh ? edited && offset ? 3 : 2 : 1}:${offset}`, revision:fresh ? edited ? "3" : "2" : "1"}};
        await route.fulfill({json});
        if (stale) reportLag();
        return;
      }
      return route.fulfill({json});
    }
    return route.fulfill({contentType:"text/html", body:`<html><body><section id="page-tracker"></section><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/view-cache.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/airing-card.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
  });
  await page.goto("https://timeline-range.test/");
  await page.locator('[data-trk-action="more"]').click();
  const main = page.locator('[data-trk-slot="main"]');
  await main.locator('[data-trk-ref-key="movie:200"]').waitFor();
  await main.locator('[data-trk-ref-key="movie:150"]').evaluate(node => node.scrollIntoView({block:"center"}));
  const before = await main.locator('[data-trk-ref-key="movie:150"]').evaluate(node => node.getBoundingClientRect().top);
  changed = true;
  await stalePage;
  assert.equal(await main.locator('[data-trk-ref-key="new"]').count(), 0);
  assert.equal(await main.locator('[data-trk-ref-key]').count(), 201);
  release();
  await main.locator('[data-trk-ref-key="new"]').waitFor({state:"attached"});
  const keys = await main.locator('[data-trk-ref-key]').evaluateAll(rows => rows.map(row => row.dataset.trkRefKey));
  assert.equal(keys.length, 202);
  assert.equal(new Set(keys).size, 202);
  assert.equal(keys[0], "new");
  const after = await main.locator('[data-trk-ref-key="movie:150"]').evaluate(node => node.getBoundingClientRect().top);
  assert.ok(Math.abs(after - before) < 2, `Reader moved by ${after - before}px`);
  assert.ok(reads.filter(read => read.changed && read.offset === 0).length >= 2);
  assert.ok(reads.filter(read => read.changed && read.offset === 200).length >= 2);
  edited = true;
  await main.getByText("Updated older movie", {exact:true}).waitFor();
  assert.equal(await main.locator('[data-trk-ref-key]').count(), 202);
  assert.deepEqual(errors, []);
});

for (const theme of ["original", "flat-dark", "flat-light"]) for (const width of [375, 1440]) {
  test(`${theme} ${width}: cached tracker results survive refresh`, async t => {
    const browser = await chromium.launch({headless:true});
    t.after(() => browser.close());
    const page = await browser.newPage({viewport:{width, height:900}, reducedMotion:"reduce"});
    page.setDefaultTimeout(7000);
    await page.addInitScript(() => localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history", viewSelectedAt:Date.now(), layouts:{history:"list"}})));
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    let version = "cold";
    let reads = 0;
    await page.route("https://view-cache.test/**", async route => {
      const url = new URL(route.request().url());
      const path = url.pathname;
      if (path.startsWith("/assets/")) {
        try { return route.fulfill({body:await readFile(new URL(".." + path, import.meta.url)), contentType:path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : "text/javascript"}); }
        catch { return route.fulfill({status:404, body:""}); }
      }
      if (path.startsWith("/api/")) {
        let json = {ok:true};
        if (path === "/api/tracker/summary") json = {ok:true, provider_instance:"default", instances:[{id:"default", label:"Default"}], counts:{history:1}, health:{}, notes:{total:0, statuses:{}, tags:[]}};
        if (path === "/api/tracker/items") {
          assert.equal(url.searchParams.get("background"), "1");
          reads++;
          json = {ok:true, total:1, items:[{key:"movie:1", type:"movie", title:version === "cached" ? "Cached movie" : "Updated movie", year:2026, date:"2026-11-08T12:00:00Z"}], _view:{ready:true, refreshing:version === "cached", version, retry_after:1}};
          if (version === "cold") json = {ok:true, _view:{ready:false, refreshing:true, retry_after:1}};
        }
        return route.fulfill({json});
      }
      const attrs = theme === "original" ? 'class="cw-theme-original"' : `data-cw-theme="${theme}"`;
      const css = ["fonts/material-symbols-rounded", "themes/tokens", "css/base", "crosswatch", "css/layout", "css/components", "css/pages", "css/page-theme", "ui-shell", theme === "original" ? "themes/original-coverage" : "themes/flat"];
      return route.fulfill({contentType:"text/html", body:`<!doctype html><html ${attrs} data-tab="tracker"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file => `<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><main id="layout" class="full"><section id="page-tracker" class="card tab-page cw-themed-page"></section></main><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/view-cache.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/airing-card.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
    });
    await page.goto("https://view-cache.test/");
    const loading = page.locator('[data-trk-slot="main"] .cw-page-loading');
    await loading.waitFor();
    await page.locator("#page-tracker[data-view-loading]").waitFor();
    assert.equal(await page.locator(".cw-page-progress").count(), 0);
    await page.emulateMedia({reducedMotion:"no-preference"});
    assert.equal(await page.locator('[data-trk-action="refresh"] span').evaluate(node => getComputedStyle(node).animationName), "cwRefreshSpin");
    await page.emulateMedia({reducedMotion:"reduce"});
    assert.equal(await page.locator('[data-trk-action="refresh"] span').evaluate(node => getComputedStyle(node).animationName), "none");
    assert.equal(await page.locator('.cw-main > [data-view-status]').isVisible(), false);
    const initialHeroTop = await page.locator('.cw-media-hero').evaluate(node => node.getBoundingClientRect().top);
    await mkdir('.tmp/view-loading', {recursive:true});
    await page.screenshot({path:`.tmp/view-loading/${theme}-${width}-cold.png`, fullPage:true});
    version = "cached";
    await page.getByText("Cached movie", {exact:true}).first().waitFor();
    assert.equal(await page.locator('.cw-main > [data-view-status]').isHidden(), true);
    assert.equal(await page.locator('.cw-main > [data-view-status]').textContent(), "");
    await page.screenshot({path:`.tmp/view-loading/${theme}-${width}-refresh.png`, fullPage:true});
    assert.equal(await page.locator('#page-tracker > [data-view-status]').count(), 0);
    const heroTop = await page.locator('.cw-media-hero').evaluate(node => node.getBoundingClientRect().top);
    assert.equal(heroTop, initialHeroTop);
    const controlsTop = await page.locator('[data-trk-slot="controls"]').evaluate(node => node.getBoundingClientRect().top);
    await page.locator('[data-trk-pick="0"]').check();
    await page.locator('.trk-source > summary').click();
    await page.keyboard.press('ArrowDown');
    version = "updated";
    await page.getByText("Updated movie", {exact:true}).first().waitFor();
    assert.equal(await page.locator('[data-trk-pick="0"]').isChecked(), true);
    assert.equal(await page.locator('.trk-source').evaluate(el => el.open), true);
    assert.equal(await page.locator('[data-trk-instance="default"]').evaluate(el => el === document.activeElement), true);
    assert.equal(await page.locator("[data-view-status]").isHidden(), true);
    assert.equal(await page.locator('.cw-media-hero').evaluate(node => node.getBoundingClientRect().top), heroTop);
    assert.equal(await page.locator('[data-trk-slot="controls"]').evaluate(node => node.getBoundingClientRect().top), controlsTop);
    assert.equal(await page.locator("#page-tracker").getAttribute("data-view-loading"), null);
    assert.ok(reads >= 2);
    assert.deepEqual(errors, []);
  });
}

test("Tracker metadata lookups preserve polling and saved watches restart it", async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  page.setDefaultTimeout(7000);
  await page.addInitScript(() => localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history", viewSelectedAt:Date.now(), layouts:{history:"list"}})));
  let version = 1, resolves = 0, writes = 0;
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.route("https://tracker-metadata.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      return route.fulfill({contentType:"text/javascript", body:await readFile(new URL(".." + url.pathname, import.meta.url))});
    }
    let json = {ok:true};
    if (url.pathname === "/api/tracker/summary") json = {ok:true, provider_instance:"default", instances:[{id:"default", label:"Default"}], counts:{history:1}, health:{}, notes:{total:0, statuses:{}, tags:[]}};
    if (url.pathname === "/api/tracker/items") json = {ok:true, total:1, items:[{key:"movie:1", type:"movie", title:`Movie version ${version}`, date:"2026-11-08T12:00:00Z"}], _view:{ready:true, refreshing:true, version:String(version), revision:String(version), retry_after:1}};
    if (url.pathname === "/api/metadata/search") json = {ok:true, results:[{tmdb:"2", title:"Picked movie", year:2026}]};
    if (url.pathname === "/api/metadata/resolve") { resolves++; json = {ok:true, result:{ids:{tmdb:"2"}}}; }
    if (url.pathname === "/api/tracker/add") { writes++; version = 3; json = {ok:true, count:1}; }
    if (url.pathname.startsWith("/api/")) return route.fulfill({json});
    return route.fulfill({contentType:"text/html", body:'<section id="page-tracker"></section><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/view-cache.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/airing-card.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector("#page-tracker"));</script>'});
  });
  await page.goto("https://tracker-metadata.test/");
  const main = page.locator('[data-trk-slot="main"]');
  await main.getByText("Movie version 1", {exact:true}).waitFor();
  for (const save of [false, true]) {
    await page.locator('[data-trk-action="log"]').first().click();
    await page.locator("[data-trk-log-q]").fill("Picked");
    const resolved = page.waitForResponse(response => new URL(response.url()).pathname === "/api/metadata/resolve");
    await page.locator('[data-trk-log-pick="0"]').click();
    await resolved;
    if (save) await page.locator('[data-trk-form="log"] button[type="submit"]').click();
    else {
      await page.locator('[data-trk-form="log"] [data-trk-sheet="close"]').click();
      version = 2;
    }
    await main.getByText(`Movie version ${save ? 3 : 2}`, {exact:true}).waitFor();
  }
  version = 4;
  await main.getByText("Movie version 4", {exact:true}).waitFor();
  assert.equal(resolves, 2);
  assert.equal(writes, 1);
  assert.deepEqual(errors, []);
});

for (const stop of [false, true]) test(`Tracker keeps refreshing after ${stop ? "stopping" : "starting"} automatic statuses and closing the dialog`, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  page.setDefaultTimeout(7000);
  await page.addInitScript(() => localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history", byDay:true, viewSelectedAt:Date.now(), layouts:{history:"list"}})));
  let version = 1, job = "idle";
  const reads = {}, actions = [], errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.route("https://tracker-automation.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) return route.fulfill({contentType:"text/javascript", body:await readFile(new URL(".." + url.pathname, import.meta.url))});
    const name = url.pathname.split("/").pop();
    let json = {ok:true};
    if (["summary", "stats", "months", "items"].includes(name)) {
      reads[name] = (reads[name] || 0) + 1;
      json._view = {ready:true, refreshing:true, version:String(version), revision:String(version), retry_after:1};
    }
    if (name === "summary") Object.assign(json, {provider_instance:"default", instances:[{id:"default", label:"Default"}], counts:{history:1}, health:{}, notes:{total:0, statuses:{}, tags:[]}});
    if (name === "stats") json.week = version;
    if (name === "months") json.months = [];
    if (name === "items") Object.assign(json, {total:1, items:[{key:"movie:1", type:"movie", title:`Movie version ${version}`, date:"2026-11-08T12:00:00Z"}]});
    if (name === "review") json.review = {state:job};
    if (name === "automatic-status") {
      const action = route.request().postDataJSON().action;
      actions.push(action);
      job = action === "review-start" ? "processing" : "cancelled";
      json.review = {state:job, titles:1};
    }
    if (url.pathname.startsWith("/api/")) return route.fulfill({json});
    return route.fulfill({contentType:"text/html", body:'<section id="page-tracker"></section><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/view-cache.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/airing-card.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector("#page-tracker"));</script>'});
  });
  await page.goto("https://tracker-automation.test/");
  const main = page.locator('[data-trk-slot="main"]');
  await main.getByText("Movie version 1", {exact:true}).waitFor();
  await page.getByLabel("More Tracker views", {exact:true}).click();
  await page.locator('[data-trk-action="automation"]').click();
  await page.locator('[data-trk-sheet="review-start"]').click();
  await page.locator('[data-trk-sheet="review-cancel"]').waitFor();
  if (stop) {
    await page.locator('[data-trk-sheet="review-cancel"]').click();
    await page.locator('[data-trk-sheet="review-start"]').waitFor();
  }
  await page.locator('.trk-automation [data-trk-sheet="close"]').click();
  const before = {...reads};
  job = stop ? "cancelled" : "complete";
  version = 2;
  await main.getByText("Movie version 2", {exact:true}).waitFor();
  await page.locator('[data-trk-slot="hero"] strong').getByText("2", {exact:true}).waitFor();
  version = 3;
  await main.getByText("Movie version 3", {exact:true}).waitFor();
  for (const name of ["summary", "stats", "months", "items"]) assert.ok(reads[name] > before[name], `${name} polling stopped`);
  assert.deepEqual(actions, stop ? ["review-start", "review-cancel"] : ["review-start"]);
  assert.deepEqual(errors, []);
});

test("obsolete requests cannot repaint", async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route("https://example.com/", route => route.fulfill({contentType:"text/html", body:"<main></main>"}));
  await page.goto("https://example.com/");
  await page.addScriptTag({content:await readFile(new URL("../assets/helpers/view-cache.js", import.meta.url), "utf8")});
  const result = await page.evaluate(async () => {
    const responses = [];
    let resolveOld;
    const old = CW.ViewCache.read("https://example.com/api/profile/collection?q=old", () => new Promise(resolve => { resolveOld = resolve; }), {active:() => true}).catch(error => error.name);
    const fresh = await CW.ViewCache.read("https://example.com/api/profile/collection?q=new", async () => ({items:["new"], _view:{ready:true, refreshing:false, version:"2"}}), {active:() => true});
    resolveOld({items:["old"], _view:{ready:true, refreshing:false, version:"1"}});
    responses.push(await old, fresh.items[0]);
    CW.ViewCache.invalidate();
    return responses;
  });
  assert.deepEqual(result, ["AbortError", "new"]);
});

for (const ready of [false, true]) for (const failure of ["response", "network"]) for (const changed of [false, true]) test(`${ready ? "Cached" : "Cold"} views recover automatically after a ${failure} failure with ${changed ? "changed" : "unchanged"} results`, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route("https://recovery.test/", route => route.fulfill({contentType:"text/html", body:"<main></main>"}));
  await page.goto("https://recovery.test/");
  await page.clock.install();
  await page.addScriptTag({content:await readFile(new URL("../assets/helpers/view-cache.js", import.meta.url), "utf8")});
  await page.evaluate(async ({ready, failure, changed}) => {
    window.reads = [];
    window.updates = [];
    window.statuses = [];
    window.phase = ready ? "cached" : "failed";
    window.addEventListener("cw:view-updated", event => updates.push(event.detail.data.items));
    window.addEventListener("cw:view-status", event => statuses.push(event.detail.data));
    window.initial = await CW.ViewCache.read("/api/tracker/overview/shows", async url => {
      reads.push(url);
      if (phase === "failed" && failure === "network") throw new TypeError("Failed to fetch");
      return {items:[changed && phase === "fresh" ? "updated" : "saved"], _view:{ready:ready || phase === "fresh", refreshing:phase === "cached", version:changed && phase === "fresh" ? "new" : "old", error:phase === "failed" ? "refresh_failed" : null, retry_after:phase === "failed" ? 30 : 1}};
    }).catch(error => error.code || error.message);
  }, {ready, failure, changed});
  if (ready) {
    assert.deepEqual(await page.evaluate(() => initial.items), ["saved"]);
    await page.evaluate(() => { phase = "failed"; });
    await page.clock.runFor(1001);
  }
  assert.equal(await page.evaluate(() => statuses.at(-1).error), "refresh_failed");
  assert.deepEqual(await page.evaluate(() => updates), []);
  await page.evaluate(() => { phase = "fresh"; });
  await page.clock.runFor(30001);
  assert.deepEqual(await page.evaluate(() => updates), [[changed ? "updated" : "saved"]]);
  assert.equal(await page.evaluate(() => statuses.at(-1).error), null);
  assert.equal(await page.evaluate(() => reads.some(url => url.includes("retry_view"))), false);
  const before = await page.evaluate(() => { CW.ViewCache.invalidate(); return reads.length; });
  await page.clock.runFor(60001);
  assert.equal(await page.evaluate(() => reads.length), before);
});

test("Range reads wait for rebuilt snapshots and reject failed refreshes", {timeout:10000}, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route("https://range-read.test/", route => route.fulfill({contentType:"text/html", body:"<main></main>"}));
  await page.goto("https://range-read.test/");
  await page.addScriptTag({content:await readFile(new URL("../assets/helpers/view-cache.js", import.meta.url), "utf8")});
  const result = await page.evaluate(async () => {
    let reads = 0, updates = 0;
    window.addEventListener("cw:view-updated", () => updates++);
    const data = await CW.ViewCache.read("/api/tracker/items?offset=200", async () => {
      reads++;
      return {items:[reads === 1 ? "old" : "fresh"], _view:{ready:true, refreshing:reads === 1, version:String(reads), revision:String(reads), retry_after:1}};
    }, {watch:false, fresh:true});
    const error = await CW.ViewCache.read("/api/tracker/items?offset=400", async () => ({items:["stale"], _view:{ready:true, refreshing:false, error:"refresh_failed"}}), {watch:false, fresh:true}).catch(error => error.code);
    CW.ViewCache.invalidate();
    return {items:data.items, reads, updates, error};
  });
  assert.deepEqual(result, {items:["fresh"], reads:2, updates:0, error:"view_refresh_failed"});
});

for (const ready of [false, true]) test(`${ready ? "Cached" : "Cold"} views resume after the browser tab is hidden`, {timeout:15000}, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route("https://visibility.test/", route => route.fulfill({contentType:"text/html", body:"<main></main>"}));
  await page.goto("https://visibility.test/");
  await page.addScriptTag({content:await readFile(new URL("../assets/helpers/view-cache.js", import.meta.url), "utf8")});
  const result = await page.evaluate(async initialReady => {
    let hidden = false, reads = 0;
    Object.defineProperty(document, "hidden", {configurable:true, get:() => hidden});
    const updated = new Promise(resolve => window.addEventListener("cw:view-updated", event => resolve(event.detail.data), {once:true}));
    const pending = CW.ViewCache.read("/api/profile/collection", async () => {
      reads++;
      return reads === 1 ? {_view:{ready:initialReady, refreshing:true, version:"old", retry_after:1}} : {items:["fresh"], _view:{ready:true, refreshing:false, version:"new"}};
    });
    if (initialReady) await pending;
    else await new Promise(resolve => setTimeout(resolve, 0));
    hidden = true;
    document.dispatchEvent(new Event("visibilitychange"));
    await new Promise(resolve => setTimeout(resolve, 1150));
    const whileHidden = reads;
    hidden = false;
    document.dispatchEvent(new Event("visibilitychange"));
    const data = await (initialReady ? updated : pending);
    CW.ViewCache.invalidate();
    return {whileHidden, reads, items:data.items};
  }, ready);
  assert.deepEqual(result, {whileHidden:1, reads:2, items:["fresh"]});
});

test("A paused cold request is cancelled when its scope is invalidated", {timeout:10000}, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route("https://visibility.test/", route => route.fulfill({contentType:"text/html", body:"<main></main>"}));
  await page.goto("https://visibility.test/");
  await page.addScriptTag({content:await readFile(new URL("../assets/helpers/view-cache.js", import.meta.url), "utf8")});
  const result = await page.evaluate(async () => {
    Object.defineProperty(document, "hidden", {configurable:true, value:true});
    const pending = CW.ViewCache.read("/api/profile/collection", async () => { throw new Error("Hidden request must not fetch"); }).catch(error => error.name);
    CW.ViewCache.invalidate();
    return pending;
  });
  assert.equal(result, "AbortError");
});

for (const initial of ["failed", "empty", "summary-failed"]) test(`Tracker timeline recovers from ${initial} initial results`, {timeout:15000}, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1440, height:900}});
  page.setDefaultTimeout(7000);
  await page.addInitScript(() => localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history", byDay:true, viewSelectedAt:Date.now(), layouts:{history:"list"}})));
  let phase = initial, reads = 0;
  const queries = [], summaryQueries = [];
  await page.route("https://timeline.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      try { return route.fulfill({body:await readFile(new URL(".." + url.pathname, import.meta.url)), contentType:"text/javascript"}); }
      catch { return route.fulfill({status:404, body:""}); }
    }
    if (url.pathname.startsWith("/api/")) {
      let json = {ok:true};
      if (url.pathname === "/api/tracker/summary") {
        summaryQueries.push(Object.fromEntries(url.searchParams));
        json = phase === "summary-failed" ? {ok:true, _view:{ready:false, refreshing:false, error:"refresh_failed"}} : {ok:true, provider_instance:"default", instances:[{id:"default", label:"Default"}], counts:{history:1}, health:{}, notes:{total:0, statuses:{}, tags:[]}};
      }
      if (url.pathname === "/api/tracker/items") {
        reads++;
        queries.push(Object.fromEntries(url.searchParams));
        json = {ok:true, total:phase === "fresh" ? 1 : 0, items:phase === "fresh" ? [{key:"movie:1", type:"movie", title:"Recovered movie", date:"2026-11-08T12:00:00Z"}] : [], _view:{ready:phase !== "failed", refreshing:phase === "empty", version:phase, error:phase === "failed" ? "refresh_failed" : null, retry_after:1}};
      }
      return route.fulfill({json});
    }
    return route.fulfill({contentType:"text/html", body:`<html><body><section id="page-tracker"></section><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/view-cache.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/airing-card.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
  });
  await page.goto("https://timeline.test/");
  const status = page.locator("[data-view-status]");
  await status.waitFor({state:"attached"});
  if (initial !== "empty") {
    const retry = status.getByRole("button", {name:"Retry"});
    await retry.waitFor();
    phase = "fresh";
    await retry.click();
  } else {
    await page.locator('[data-trk-slot="main"] .cw-empty').waitFor();
    assert.equal(await status.isHidden(), true);
    phase = "fresh";
  }
  await page.getByText("Recovered movie", {exact:true}).first().waitFor();
  assert.ok(reads >= (initial === "summary-failed" ? 1 : 2));
  assert.equal(await page.locator('[data-trk-slot="main"]').getByText("The tracker could not be loaded. Try again in a moment.", {exact:true}).count(), 0);
  if (initial === "failed") assert.ok(queries.some(query => query.retry_view === "1"));
  if (initial === "summary-failed") assert.ok(summaryQueries.some(query => query.retry_view === "1"));
});


test("Fresh view reads survive tab switches without another request or spinner", async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route("https://reuse.test/**", route => route.fulfill({contentType:"text/html", body:'<section id="host"></section>'}));
  await page.goto("https://reuse.test/");
  await page.clock.install();
  await page.addScriptTag({path:"assets/helpers/view-cache.js"});
  await page.evaluate(async () => {
    window.reads = [];
    window.updates = [];
    window.statuses = [];
    window.version = "1";
    window.activeTab = "history";
    CW.ViewCache.bindRefresh(document.querySelector("#host"), () => true);
    addEventListener("cw:view-updated", event => updates.push(event.detail.data));
    addEventListener("cw:view-status", event => statuses.push(event.detail));
    window.openTab = (tab, options = {}) => {
      activeTab = tab;
      return CW.ViewCache.read(`/api/tracker/items?feature=${tab}`, async url => {
        reads.push(url);
        return {items:[version], _view:{ready:true,refreshing:false,version,revision:version}};
      }, {maxAge:300000, interval:300000, cacheScope:"alice", active:() => activeTab === tab, ...options});
    };
    await openTab("history");
    await openTab("ratings");
  });
  await page.clock.runFor(2000);
  await page.evaluate(async () => {
    window.loadingSeen = false;
    new MutationObserver(() => { loadingSeen ||= document.querySelector("#host").hasAttribute("data-view-loading"); }).observe(document.querySelector("#host"), {attributes:true});
    const data = await openTab("history");
    data.items[0] = "mutated";
    window.restored = await openTab("history");
  });
  assert.equal(await page.evaluate(() => reads.length), 2);
  assert.equal(await page.evaluate(() => loadingSeen), false);
  assert.deepEqual(await page.evaluate(() => restored.items), ["1"]);
  assert.deepEqual(await page.evaluate(() => statuses.at(-1)), {url:"/api/tracker/items?feature=history", data:{ready:true,refreshing:false,version:"1",revision:"1"}});
  assert.equal(await page.evaluate(() => statuses.length), 4);
  await page.clock.runFor(297000);
  assert.equal(await page.evaluate(() => reads.length), 2);
  await page.evaluate(() => { version = "2"; });
  await page.clock.runFor(1000);
  assert.equal(await page.evaluate(() => reads.length), 3);
  assert.deepEqual(await page.evaluate(() => updates.map(data => data.items)), [["2"]]);
  await page.evaluate(async () => {
    CW.ViewCache.retry("/api/tracker/items?feature=history");
    await openTab("history");
  });
  assert.equal(await page.evaluate(() => reads.length), 4);
  assert.match(await page.evaluate(() => reads.at(-1)), /retry_view=1/);
  await page.evaluate(async () => { await openTab("history", {cacheScope:"bob"}); });
  assert.equal(await page.evaluate(() => reads.length), 5);
  await page.evaluate(async () => { CW.ViewCache.invalidate(); await openTab("history"); });
  assert.equal(await page.evaluate(() => reads.length), 6);
  await page.evaluate(async () => { CW.ViewCache.discard(); await openTab("history"); });
  assert.equal(await page.evaluate(() => reads.length), 7);
});
