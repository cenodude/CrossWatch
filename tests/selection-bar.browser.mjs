/* tests/selection-bar.browser.mjs */
/* CrossWatch - Shared Editor and Tracker selection bar interactions and layout */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFile, readdir} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

async function fixture(t, view, total = 75) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1500,height:900}});
  page.setDefaultTimeout(6000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  const rows = Array.from({length:total}, (_, i) => ({key:`tmdb:${i + 1}`,type:"movie",title:`Movie ${String(i + 1).padStart(3,"0")}`,year:2026,ids:{tmdb:String(i + 1)},date:"2026-11-01T20:00:00Z"}));
  const modules = (await readdir(new URL("../assets/js/editor/", import.meta.url))).filter(name => name.endsWith(".js") && !["episode-groups.js","episode-alignment.js","mapping.js","origin.js","saved-mappings.js"].includes(name));
  await page.route("**/*", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      try { return await route.fulfill({contentType:url.pathname.endsWith(".css") ? "text/css" : url.pathname.endsWith(".woff2") ? "font/woff2" : "text/javascript",body:await readFile(new URL(".." + url.pathname, import.meta.url))}); }
      catch { return route.fulfill({status:404,body:""}); }
    }
    if (url.pathname.startsWith("/api/")) {
      let json = {ok:true};
      if (url.pathname === "/api/tracker/summary") json = {instances:[{id:"default",label:"Default"}],counts:{history:total},tmdb_key:true};
      if (url.pathname === "/api/tracker/items") {
        const offset = Number(url.searchParams.get("offset") || 0);
        const limit = Number(url.searchParams.get("limit") || 50);
        const filtered = rows.filter(row => row.title.toLowerCase().includes((url.searchParams.get("q") || "").toLowerCase()));
        json = {items:filtered.slice(offset,offset+limit),total:filtered.length};
      }
      if (url.pathname === "/api/editor/state/providers") json = {providers:["CROSSWATCH"]};
      if (url.pathname === "/api/editor") json = {ok:true,provider:"CROSSWATCH",provider_instance:"default",items:Object.fromEntries(rows.map(row=>[row.key,row])),manual_blocks:[],manual_adds:{}};
      if (url.pathname.includes("provider-instances")) json = {instances:[{id:"default",label:"Default"}]};
      if (url.pathname === "/api/editor/send/options") json = {providers:[]};
      return route.fulfill({json});
    }
    const scripts = view === "editor" ? [...modules.map(name=>`editor/${name}`),"editor.js"] : ["tracker.js"];
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html data-cw-theme="flat-dark"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
      ${["themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","fonts/material-symbols-rounded","ui-shell","themes/flat","themes/original-coverage"].map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}
      </head><body><div id="layout"><section id="page-${view}" class="cw-themed-page"></section></div>
      <script src="/assets/helpers/selection-bar.js"></script>
      ${scripts.map(file=>`<script src="/assets/js/${file}"></script>`).join("")}
      ${view === "tracker" ? '<script>TrackerPage.mount(document.querySelector("#page-tracker"))</script>' : ""}</body></html>`});
  });
  await page.goto(`https://selection.test/${view}`);
  await page.locator(view === "editor" ? "#cw-tbody tr" : ".trk-play").first().waitFor();
  return {page,bar:page.locator(".cw-selection-bar")};
}

test("Editor selects visible and filtered results and preserves block/unblock actions", async t => {
  const {page,bar} = await fixture(t,"editor");
  await bar.waitFor({state:"hidden"});
  assert.equal(await bar.isVisible(),false);
  await page.locator("#cw-select-page").check();
  assert.equal(await bar.locator("strong").textContent(),"50");
  assert.equal(await bar.getByRole("button",{name:"Send to...",exact:true}).isVisible(),true);
  await bar.getByRole("button",{name:"All results",exact:true}).click();
  assert.equal(await bar.locator("strong").textContent(),"75");
  await bar.getByRole("button",{name:"Clear",exact:true}).click();
  await bar.waitFor({state:"hidden"});
  assert.equal(await bar.isVisible(),false);
  await page.locator("#cw-filter").fill("Movie 00");
  await page.waitForTimeout(350);
  await page.locator("#cw-select-page").check();
  await bar.getByRole("button",{name:"All results",exact:true}).click();
  assert.equal(await bar.locator("strong").textContent(),"9");
  await bar.getByRole("button",{name:"Block",exact:true}).click();
  await bar.waitFor({state:"hidden"});
  assert.equal(await bar.isVisible(),false);
  assert.equal(await page.locator("#cw-change-bar").isVisible(),true);
  await page.locator("#cw-select-page").check();
  assert.equal(await bar.getByRole("button",{name:"Unblock",exact:true}).isVisible(),true);
  assert.equal(await bar.getByRole("button",{name:"Block",exact:true}).isVisible(),false);
  await page.setViewportSize({width:390,height:900});
  await page.evaluate(()=>{document.body.classList.add("cw-has-bottom-nav");window.scrollTo(0,document.body.scrollHeight);});
  await page.waitForTimeout(100);
  const changes = await page.locator("#cw-change-bar").boundingBox();
  const selection = await bar.boundingBox();
  assert.ok(changes.y+changes.height<selection.y);
  await bar.getByRole("button",{name:"Unblock",exact:true}).click();
  await bar.waitFor({state:"hidden"});
  assert.equal(await bar.isVisible(),false);
});

test("Tracker selects results across pages, keeps filters, and uses the existing delete dialog", async t => {
  const {page,bar} = await fixture(t,"tracker",275);
  await page.locator('[data-trk-pick="0"]').check();
  assert.equal(await bar.locator("strong").textContent(),"1");
  assert.equal(await page.getByRole("group",{name:"Filter",exact:true}).isVisible(),true);
  await bar.getByRole("button",{name:"Visible",exact:true}).click();
  assert.equal(await bar.locator("strong").textContent(),"50");
  await bar.getByRole("button",{name:"All results",exact:true}).click();
  await page.waitForFunction(()=>document.querySelector(".cw-selection-count strong")?.textContent === "275");
  await bar.getByRole("button",{name:"Delete",exact:true}).click();
  assert.equal(await page.locator(".trk-sheet").isVisible(),true);
  assert.match(await page.locator(".trk-sheet").textContent(),/275/);
  await page.keyboard.press("Escape");
  await bar.getByRole("button",{name:"Clear",exact:true}).click();
  await bar.waitFor({state:"hidden"});
  assert.equal(await bar.isVisible(),false);
  await page.locator('[data-trk-pick="0"]').check();
  await page.locator('[data-trk-view="ratings"]').click();
  await bar.waitFor({state:"hidden"});
  assert.equal(await bar.isVisible(),false);
});

test("Tracker explains the server bulk limit before selecting all", async t => {
  const {page,bar} = await fixture(t,"tracker",1001);
  await page.locator('[data-trk-pick="0"]').check();
  const all = bar.getByRole("button",{name:"All results",exact:true});
  assert.equal(await all.isDisabled(),true);
  assert.match(await all.getAttribute("title"),/1000/);
});

test("clearing a pending Tracker selection prevents late results from restoring it", async t => {
  const {page,bar} = await fixture(t,"tracker");
  let release;
  const pending = new Promise(resolve=>{release=resolve;});
  await page.route("**/api/tracker/items?**",async route=>{
    if (new URL(route.request().url()).searchParams.get("limit") === "200") await pending;
    return route.fallback();
  });
  await page.locator('[data-trk-pick="0"]').check();
  await bar.getByRole("button",{name:"All results",exact:true}).click();
  assert.equal(await bar.getByRole("button",{name:"Selecting…",exact:true}).isDisabled(),true);
  await bar.getByRole("button",{name:"Clear",exact:true}).click();
  release();
  await page.waitForTimeout(200);
  await bar.waitFor({state:"hidden"});
  assert.equal(await bar.isVisible(),false);
  assert.equal(await page.locator('[data-trk-pick]:checked').count(),0);
});

for (const view of ["editor","tracker"]) {
  test(`${view} selection bar fits all themes, stays visible on scroll, and clears the mobile navigation`, async t => {
    const {page,bar} = await fixture(t,view);
    await page.locator(view === "editor" ? "#cw-select-page" : '[data-trk-pick="0"]').check();
    for (const theme of ["original","flat-dark","flat-light"]) {
      for (const width of [1500,390]) {
        await page.setViewportSize({width,height:900});
        await page.evaluate(({theme,width})=>{
          if (theme === "original") document.documentElement.removeAttribute("data-cw-theme");
          else document.documentElement.setAttribute("data-cw-theme",theme);
          document.body.classList.toggle("cw-has-bottom-nav",width<720);
        },{theme,width});
        await page.evaluate(()=>window.scrollTo(0,document.body.scrollHeight));
        await page.waitForTimeout(100);
        const box = await bar.boundingBox();
        assert.ok(box.x>=0 && box.x+box.width<=width);
        assert.ok(box.y>0 && box.y+box.height<=900-(width<720?65:0));
        for (const button of await bar.locator("button:visible").all()) {
          const rect = await button.boundingBox();
          assert.ok(rect.x>=box.x && rect.x+rect.width<=box.x+box.width);
          assert.ok(rect.y>=box.y && rect.y+rect.height<=box.y+box.height);
        }
        await bar.screenshot({path:`.tmp/selection-${view}-${theme}-${width}.png`});
      }
    }
    await page.locator(`#page-${view}`).evaluate(el=>el.hidden=true);
    await bar.waitFor({state:"hidden"});
    assert.equal(await bar.isVisible(),false);
  });
}

for (const view of ["editor","tracker"]) {
  test(`${view} motion handles clear and immediate reselection, and respects reduced motion`, async t => {
    const {page,bar} = await fixture(t,view);
    const pick = view === "editor" ? "#cw-select-page" : '[data-trk-pick="0"]';
    await page.locator(pick).check();
    await bar.evaluate(async el=>{await Promise.allSettled(el.getAnimations().map(animation=>animation.finished));});
    const countMotion = await bar.evaluate(el=>{
      el.querySelectorAll("button")[1].click();
      return el.querySelector("strong").getAnimations().map(animation=>animation.effect.getTiming().duration);
    });
    if (view === "editor") assert.deepEqual(countMotion,[160]);
    else await page.waitForFunction(()=>document.querySelector(".cw-selection-count strong")?.textContent === "75");
    const exit = await bar.evaluate(el=>{
      getComputedStyle(el).opacity;
      [...el.querySelectorAll("button")].find(button=>button.textContent.includes("Clear")).click();
      return {inert:el.inert,hidden:el.hidden,pointerEvents:getComputedStyle(el).pointerEvents,durations:el.getAnimations().map(animation=>animation.effect.getTiming().duration)};
    });
    assert.equal(exit.inert,true);
    assert.equal(exit.hidden,true);
    assert.equal(exit.pointerEvents,"none");
    assert.ok(exit.durations.includes(120));
    await page.locator(pick).evaluate(el=>el.click());
    await page.waitForTimeout(200);
    assert.equal(await bar.isVisible(),true);
    assert.equal(await bar.evaluate(el=>el.inert),false);
    await page.emulateMedia({reducedMotion:"reduce"});
    await bar.getByRole("button",{name:"Clear",exact:true}).click();
    assert.equal(await bar.isVisible(),false);
    await page.locator(pick).check();
    assert.equal(await bar.evaluate(el=>el.getAnimations({subtree:true}).length),0);
    await bar.getByRole("button",{name:"All results",exact:true}).click();
    await page.waitForFunction(()=>document.querySelector(".cw-selection-count strong")?.textContent === "75");
    assert.equal(await bar.evaluate(el=>el.getAnimations({subtree:true}).length),0);
  });
}
