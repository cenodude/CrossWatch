/* tests/tracker-history-grouping.browser.mjs */
/* CrossWatch - Grouped History navigation, selection and responsive menus */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import './i18n-browser-setup.mjs';
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

const css = ["fonts/material-symbols-rounded", "themes/tokens", "css/base", "crosswatch", "css/layout", "css/components", "css/pages", "css/page-theme", "ui-shell"];

async function fixture(t, theme, width) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width, height:950}, reducedMotion:"reduce"});
  page.setDefaultTimeout(6000);
  const errors = [], requests = [], removals = [];
  const data = {movies:[
    {key:"tmdb:949@latest", title_key:"movie:tmdb:949", type:"movie", title:"Heat", year:1995, ids:{tmdb:"949"}, plays:3, date:"2026-11-01T12:00:00Z", user_rating:9, status:"completed"},
    {key:"imdb:tt2543164", title_key:"movie:imdb:tt2543164", type:"movie", title:"Arrival", year:2016, ids:{imdb:"tt2543164"}, plays:1, date:"", user_rating:null},
  ]};
  page.on("pageerror", error => errors.push(error.message));
  await page.addInitScript(() => {
    if (!localStorage.getItem("cw.tracker.view.v3")) localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"movies", viewSelectedAt:Date.now()}));
  });
  await page.route("https://tracker-history.test/**", async route => {
    const url = new URL(route.request().url()), path = url.pathname;
    if (path.startsWith("/assets/")) {
      try {return route.fulfill({body:await readFile(new URL(".." + path, import.meta.url)), contentType:path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : "text/javascript"});}
      catch {return route.fulfill({status:404, body:""});}
    }
    if (path.startsWith("/api/")) {
      requests.push(url);
      let json = {ok:true};
      if (path === "/api/tracker/summary") json = {provider_instance:"default", instances:[{id:"default",label:"Default"}], counts:{history:5}, health:{}, notes:{total:0,statuses:{},tags:[]}};
      if (path === "/api/tracker/history/titles") {
        const mixed = url.searchParams.get("media") === "movie" ? data.movies : [...data.movies, {key:"tmdb:3#s01e02",title_key:"show:tmdb:3",type:"show",title:"Example show",year:2026,ids:{tmdb:"3"},plays:2,date:"2026-11-01T12:00:00Z"}];
        const rows = mixed.filter(item => item.title.toLowerCase().includes((url.searchParams.get("q") || "").toLowerCase()));
        json = {total:rows.length,items:rows};
      }
      if (path === "/api/tracker/items") {
        const episodes = [1,2].map(n => ({key:`tmdb:3#s01e0${n}`,type:"episode",series_title:"Example show",title:`S01E0${n}`,season:1,episode:n,show_ids:{tmdb:"3"},date:"2026-11-01T12:00:00Z"}));
        const items = url.searchParams.get("feature") === "watchlist" ? [] : url.searchParams.get("media") === "show" ? episodes : url.searchParams.get("media") === "movie" ? data.movies : [...data.movies,...episodes];
        json = {total:items.length,items};
      }
      if (path === "/api/tracker/shows") json = {total:1,shows:[{key:"tmdb:3",title:"Example show",year:2026,episodes:2,plays:2,last_season:1,last_episode:2,last_watched:"2026-11-01T12:00:00Z"}]};
      if (path === "/api/tracker/titles/remove") {
        const body = route.request().postDataJSON();
        removals.push(body);
        json = {counts:{history:body.keys.length === 2 ? 4 : 3},token:"preview",titles:body.keys.length};
      }
      return route.fulfill({json});
    }
    const attrs = theme === "original" ? 'class="cw-theme-original"' : `data-cw-theme="${theme}"`;
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${attrs}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${[...css,theme === "original" ? "themes/original-coverage" : "themes/flat"].map(file => `<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><main id="layout" class="full"><section id="page-tracker" class="tab-page cw-themed-page"></section></main><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
  });
  await page.goto("https://tracker-history.test/");
  await page.locator(".trk-movies tbody tr").first().waitFor();
  assert.equal(await page.locator("#trk-extra-filters").isVisible(), false);
  await openFilters(page);
  return {page, data, errors, requests, removals};
}

async function openFilters(page) {
  if (await page.locator('[data-trk-action="filters"]').getAttribute("aria-expanded") !== "true") await page.locator('[data-trk-action="filters"]').click();
}

async function screenshot(page, name) {
  if (!process.env.CW_TRACKER_HISTORY_SCREENSHOTS) return;
  await mkdir(process.env.CW_TRACKER_HISTORY_SCREENSHOTS, {recursive:true});
  await page.screenshot({path:`${process.env.CW_TRACKER_HISTORY_SCREENSHOTS}/${name}.png`});
}

for (const theme of ["original", "flat-dark", "flat-light"]) for (const width of [320, 900, 1600]) {
  test(`${theme}: grouped History and plain More menu at ${width}px`, async t => {
    const {page, data, errors} = await fixture(t, theme, width);
    assert.equal(await page.locator('.trk-tabs [data-trk-view="movies"]').count(), 0);
    const history = page.locator('.trk-tabs [data-trk-view="history"]');
    assert.equal(await page.locator('.trk-tabs [data-trk-view="shows"]').count(), 0);
    assert.match(await history.innerText(), /History/);
    assert.equal(await history.getAttribute("aria-selected"), "true");
    assert.equal(await page.getByRole("combobox", {name:"Group history by"}).inputValue(), "title");
    assert.equal(await page.locator(".trk-movies tbody tr").count(), 2);
    assert.match(await page.locator(".trk-movies").innerText(), /3 watches/);
    assert.match(await page.locator(".trk-movies").innerText(), /Unknown/);
    const searchBox = await page.locator('.trk-search').boundingBox();
    const filterBox = await page.locator('.trk-filter-toggle').boundingBox();
    assert.ok(Math.abs(searchBox.x + searchBox.width - filterBox.x - filterBox.width - 1) < 0.5);
    assert.ok(Math.abs(filterBox.y - searchBox.y - 1) < 0.5);
    assert.ok(Math.abs(searchBox.height - filterBox.height - 2) < 0.5);
    await page.getByRole("combobox", {name:"Group history by"}).selectOption("watch");
    await page.getByRole("checkbox", {name:"Only possible duplicates"}).waitFor();
    const options = await page.locator('.trk-filter-check').evaluateAll(labels => labels.map(label => {
      const box = label.getBoundingClientRect();
      const input = label.querySelector('input').getBoundingClientRect();
      const text = label.querySelector('span').getBoundingClientRect();
      return {x:box.x,y:box.y,right:box.right,gap:text.x-input.right};
    }));
    assert.equal(options.length, 2);
    assert.ok(options.every(option => option.gap >= 8));
    assert.ok(options[1].y > options[0].y || options[1].x - options[0].right >= 20);
    await screenshot(page, `filter-${theme}-${width}`);
    await page.getByRole("combobox", {name:"Group history by"}).selectOption("title");
    await page.locator('.trk-movies tbody tr').first().waitFor();
    await page.locator('[data-trk-action="filters"]').click();
    await screenshot(page, `list-${theme}-${width}`);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    const tops = await page.locator('.trk-tabs [role="tab"]:visible').evaluateAll(rows => rows.map(row => Math.round(row.getBoundingClientRect().top)));
    assert.equal(new Set(tops).size, 1);
    await page.locator('.trk-others summary').click();
    const menu = page.locator('.trk-others-menu');
    await menu.getByRole("button", {name:"Automatic statuses"}).waitFor();
    const button = menu.getByRole("button", {name:"Automatic statuses"});
    const appearance = await button.evaluate(el => {
      const style = getComputedStyle(el);
      return {border:style.borderTopColor,background:style.backgroundColor,height:el.getBoundingClientRect().height};
    });
    assert.equal(appearance.border, "rgba(0, 0, 0, 0)");
    assert.equal(appearance.background, "rgba(0, 0, 0, 0)");
    assert.ok(appearance.height >= 44);
    await button.hover();
    await page.waitForFunction(() => getComputedStyle(document.querySelector('.trk-others-menu [data-trk-action="automation"]')).backgroundColor !== "rgba(0, 0, 0, 0)");
    await page.keyboard.press("Tab");
    await button.focus();
    assert.notEqual(await button.evaluate(el => getComputedStyle(el).outlineStyle), "none");
    const box = await menu.boundingBox();
    assert.ok(box.x >= 0 && box.x + box.width <= width + 1);
    await screenshot(page, `menu-${theme}-${width}`);
    await page.locator('.trk-others summary').click();
    await openFilters(page);
    await page.locator('[data-trk-history-media="show"]').click();
    await page.locator('.trk-shows tbody tr').waitFor();
    await page.reload();
    await openFilters(page);
    await page.locator('.trk-shows tbody tr').waitFor();
    assert.equal(await page.locator('[data-trk-history-media="show"]').getAttribute("aria-pressed"), "true");
    const dropped = page.getByRole("checkbox", {name:"Hide dropped", exact:true});
    assert.equal(await dropped.locator('xpath=ancestor::div[contains(@class,"trk-filter-checks")]').count(), 1);
    assert.equal(await page.locator('.trk-filter-fields [data-trk-input="hide-dropped"]').count(), 0);
    if (width === 1600) {
      const fields = await page.locator('.trk-filter-fields> :is(.trk-filter-type,.trk-status-filter)').evaluateAll(fields => fields.map(field => {
        const label = field.querySelector('span').getBoundingClientRect();
        const control = field.querySelector('select,.trk-seg').getBoundingClientRect();
        return {label:label.y,top:control.y,height:control.height};
      }));
      for (const field of fields) {
        assert.ok(Math.abs(field.label - fields[0].label) < 1);
        assert.ok(Math.abs(field.top - fields[0].top) < 1);
        assert.ok(Math.abs(field.height - fields[0].height) < 1);
      }
    }
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    await screenshot(page, `shows-filter-${theme}-${width}`);
    await page.locator('[data-trk-history-media="movie"]').click();
    await page.locator('.trk-movies tbody tr').first().waitFor();
    await page.locator('[data-trk-layout="grid"]').click();
    assert.equal(await page.locator('.trk-grid-card').count(), 2);
    assert.ok((await page.locator('.trk-grid-copy>strong').first().boundingBox()).width > 30);
    assert.match(await page.locator('.trk-grid-card').first().innerText(), /3 watches/);
    await screenshot(page, `movies-${theme}-${width}`);
    data.movies = [];
    await page.reload();
    await openFilters(page);
    await page.getByText("No movies in your history yet.", {exact:true}).waitFor();
    await screenshot(page, `empty-${theme}-${width}`);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    assert.deepEqual(errors, []);
  });
}

test("movie selection previews all watches and keeps grouping preferences after leaving", async t => {
  const {page, removals, requests, errors} = await fixture(t, "flat-dark", 1600);
  await page.getByRole("checkbox", {name:"Select Heat", exact:true}).check();
  await page.locator('[data-trk-action="delete-picked"]').click();
  await page.getByText("History: 3", {exact:false}).waitFor();
  assert.deepEqual(removals.at(-1).keys, ["movie:tmdb:949"]);
  await page.locator('[data-trk-sheet="close"]').last().click();
  await page.locator('[data-trk-action="pick-all"]').click();
  await page.waitForFunction(() => document.querySelector('.cw-selection-count strong')?.textContent === "2");
  await page.locator('[data-trk-action="delete-picked"]').click();
  await page.getByText("History: 4", {exact:false}).waitFor();
  assert.deepEqual(removals.at(-1).keys, ["movie:tmdb:949", "movie:imdb:tt2543164"]);
  assert.ok(requests.some(url => url.pathname === "/api/tracker/history/titles" && url.searchParams.get("limit") === "200"));
  await page.locator('[data-trk-sheet="close"]').last().click();
  await page.locator('.trk-tabs [data-trk-view="watchlist"]').click();
  await page.locator('.trk-tabs [data-trk-view="history"]').click();
  await page.locator('.trk-movies tbody tr').first().waitFor();
  assert.equal(await page.locator('[data-trk-history-media="movie"]').getAttribute("aria-pressed"), "true");
  assert.deepEqual(errors, []);
});


test("History switches between watches and mixed titles while preserving type and layout", async t => {
  const {page, errors, requests} = await fixture(t, "flat-dark", 1600);
  await page.getByRole("combobox", {name:"Group history by"}).focus();
  await page.getByRole("combobox", {name:"Group history by"}).selectOption("watch");
  await page.locator('.trk-list tbody tr').first().waitFor();
  assert.equal(await page.evaluate(() => document.activeElement?.dataset.trkInput), "history-group");
  await page.locator('[data-trk-history-media="show"]').click();
  await page.waitForFunction(() => document.querySelectorAll('.trk-list tbody .trk-play').length === 2);
  assert.match(await page.locator('.trk-list').innerText(), /S01/);
  assert.equal(await page.evaluate(() => document.activeElement?.dataset.trkHistoryMedia), "show");
  await page.getByRole("combobox", {name:"Group history by"}).selectOption("title");
  await page.locator('.trk-shows tbody tr').waitFor();
  assert.equal(await page.locator('.trk-shows tbody tr').count(), 1);
  await page.locator('[data-trk-layout="grid"]').click();
  await page.locator('[data-trk-history-media=""]').click();
  await page.waitForFunction(() => document.querySelectorAll('.trk-grid-card').length === 3);
  assert.match(await page.locator('.trk-grid-copy>strong').allTextContents().then(rows => rows.join(",")), /Example show/);
  assert.doesNotMatch(await page.locator('.trk-grid-card').last().innerText(), /S01E02/);
  await page.locator('[data-trk-history-media="movie"]').click();
  await page.waitForFunction(() => document.querySelectorAll('.trk-grid-card').length === 2);
  await page.locator('[data-trk-history-media=""]').click();
  await page.reload();
    await openFilters(page);
  await page.waitForFunction(() => document.querySelectorAll('.trk-grid-card').length === 3);
  assert.equal(await page.getByRole("combobox", {name:"Group history by"}).inputValue(), "title");
  assert.equal(await page.locator('[data-trk-history-media=""]').getAttribute("aria-pressed"), "true");
  assert.equal(await page.locator('[data-trk-layout="grid"]').getAttribute("aria-pressed"), "true");
  assert.ok(requests.some(url => url.pathname === "/api/tracker/history/titles" && url.searchParams.get("media") === ""));
  await page.getByRole("combobox", {name:"Group history by"}).selectOption("watch");
  await page.waitForFunction(() => document.querySelectorAll('.trk-list tbody .trk-play').length === 4);
  assert.deepEqual(errors, []);
});


test("filter badge tracks hidden choices and Clear filters restores defaults", async t => {
  const {page, errors} = await fixture(t, "flat-dark", 1600);
  const toggle = page.locator('[data-trk-action="filters"]');
  assert.equal(await toggle.getAttribute("aria-label"), "Filters (2 active)");
  await page.getByRole("combobox", {name:"Group history by"}).selectOption("watch");
  await page.getByRole("checkbox", {name:"Only possible duplicates"}).check();
  await page.getByRole("checkbox", {name:"Status labels on cards",exact:true}).uncheck();
  assert.equal(await toggle.getAttribute("aria-label"), "Filters (3 active)");
  await page.getByRole("button", {name:"Clear filters",exact:true}).click();
  await page.waitForFunction(() => document.querySelectorAll('.trk-list tbody .trk-play').length === 4);
  assert.equal(await toggle.getAttribute("aria-label"), "Filters");
  assert.equal(await page.locator('[data-trk-history-media=""]').getAttribute("aria-pressed"), "true");
  assert.equal(await page.getByRole("combobox", {name:"Group history by"}).inputValue(), "watch");
  assert.equal(await page.getByRole("checkbox", {name:"Only possible duplicates"}).isChecked(), false);
  assert.equal(await page.getByRole("checkbox", {name:"Status labels on cards",exact:true}).isChecked(), true);
  await toggle.click();
  assert.equal(await page.locator('[data-trk-input="history-group"]').isVisible(), false);
  assert.equal(await page.locator('[data-trk-history-media="movie"]').isVisible(), false);
  assert.deepEqual(errors, []);
});


test("Hide dropped checkbox filters shows, persists and clears conflicting status", async t => {
  const {page, requests, errors} = await fixture(t, "flat-dark", 1600);
  await page.locator('[data-trk-history-media="show"]').click();
  await page.locator('.trk-shows tbody tr').waitFor();
  const dropped = page.getByRole("checkbox", {name:"Hide dropped", exact:true});
  const status = page.getByRole("combobox", {name:"Filter by status", exact:true});
  await dropped.check();
  await page.waitForFunction(() => document.activeElement?.dataset.trkInput === "hide-dropped");
  await page.locator('.trk-shows tbody tr').waitFor();
  assert.ok(requests.filter(url => url.pathname === "/api/tracker/shows").at(-1).searchParams.get("hide_dropped") === "true");
  await page.reload();
  await openFilters(page);
  assert.equal(await dropped.isChecked(), true);
  await status.selectOption("dropped");
  assert.equal(await dropped.isChecked(), false);
  await page.locator('.trk-shows tbody tr').waitFor();
  assert.equal(requests.filter(url => url.pathname === "/api/tracker/shows").at(-1).searchParams.has("hide_dropped"), false);
  await dropped.check();
  assert.equal(await status.inputValue(), "");
  await dropped.uncheck();
  await page.locator('.trk-shows tbody tr').waitFor();
  assert.equal(requests.filter(url => url.pathname === "/api/tracker/shows").at(-1).searchParams.has("hide_dropped"), false);
  await dropped.check();
  await page.locator('[data-trk-action="filters-clear"]').click();
  await page.locator('.trk-list tbody tr').first().waitFor();
  await page.getByRole("combobox", {name:"Group history by"}).selectOption("title");
  await page.locator('[data-trk-history-media="show"]').click();
  await page.locator('.trk-shows tbody tr').waitFor();
  assert.equal(await dropped.isChecked(), false);
  assert.deepEqual(errors, []);
});
