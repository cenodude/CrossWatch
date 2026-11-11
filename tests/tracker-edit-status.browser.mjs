/* tests/tracker-edit-status.browser.mjs */
/* CrossWatch - Tracker edit status selection, persistence and retry */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

for (const theme of ["original", "flat-dark", "flat-light"]) for (const width of [390, 1440]) {
  test(`${theme}: edit status at ${width}px`, async t => {
    const browser = await chromium.launch({headless:true});
    t.after(() => browser.close());
    const page = await browser.newPage({viewport:{width,height:900},reducedMotion:"reduce"});
    page.setDefaultTimeout(5000);
    await page.addInitScript(() => localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"history",viewSelectedAt:Date.now()})));
    const errors = [], writes = [];
    page.on("pageerror", error => errors.push(error.message));
    const child = width < 800;
    const item = {key:"tmdb:1",type:child ? "episode" : "movie",title:"Example title",year:2026,series_title:"Example show",series_year:2026,season:1,episode:2,ids:{tmdb:"1"},show_ids:{tmdb:"2"},status:"planned",date:"2026-11-04T12:00:34Z",date_field:"watched_at"};
    let fail = false;
    await page.route("https://tracker-edit-status.test/**", async route => {
      const path = new URL(route.request().url()).pathname;
      if (path.startsWith("/assets/")) {
        try {return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".woff2")?"font/woff2":"text/javascript"});}
        catch {return route.fulfill({status:404,body:""});}
      }
      if (path.startsWith("/api/")) {
        let json = {ok:true};
        if (path === "/api/tracker/summary") json = {provider_instance:"default",instances:[{id:"default",label:"Default"}],counts:{history:1},health:{},notes:{total:0,statuses:{},tags:[]}};
        if (path === "/api/tracker/items") json = {items:[item],total:1};
        if (path === "/api/tracker/edit") {
          const body = route.request().postDataJSON();
          writes.push(body);
          if (fail) return route.fulfill({status:409,json:{ok:false,error:"changed_since"}});
          if ("status" in body.changes) item.status = body.changes.status || null;
          if ("title" in body.changes) item[child ? "series_title" : "title"] = body.changes.title;
          json = {ok:true,key:item.key,item,batch:"edit-status"};
        }
        return route.fulfill({json});
      }
      const attrs = theme === "original" ? 'class="cw-theme-original"' : `data-cw-theme="${theme}"`;
      const css = ["fonts/material-symbols-rounded","themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","ui-shell",theme === "original" ? "themes/original-coverage" : "themes/flat"];
      return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${attrs}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><main id="layout" class="full"><section id="page-tracker" class="cw-themed-page"></section></main><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
    });
    await page.goto("https://tracker-edit-status.test/");
    await page.locator(".trk-play").first().waitFor();
    await page.locator('[data-trk-layout="grid"]').click();
    const edit = page.locator('[data-trk-row-edit="0"]');
    const status = page.locator('[data-trk-edit="status"]');
    const title = page.locator('[data-trk-edit="title"]');
    const save = page.locator('[data-trk-form="edit"] button[type="submit"]');
    const close = page.locator('.trk-sheet [data-trk-sheet="close"]').first();
    await edit.click();
    assert.equal(await status.inputValue(), "planned");
    assert.equal(await status.locator("..").locator("span").textContent(), child ? "Show status" : "Status");
    assert.deepEqual(await status.locator("option").allTextContents(), ["No status","Watching","Plan to watch","Completed","On hold","Dropped"]);
    await status.selectOption("dropped");
    await close.click();
    assert.equal(writes.length, 0);
    await edit.click();
    assert.equal(await status.inputValue(), "planned");
    await status.selectOption("completed");
    await title.fill("Edited title");
    fail = true;
    await save.click();
    await page.locator('.trk-error[role="alert"]').waitFor();
    assert.equal(await status.inputValue(), "completed");
    assert.equal(await title.inputValue(), "Edited title");
    assert.deepEqual(writes[0], {feature:"history",provider_instance:"default",key:"tmdb:1",changes:{status:"completed",title:"Edited title"}});
    fail = false;
    await save.click();
    await page.locator(".trk-sheet-wrap").waitFor({state:"hidden"});
    await edit.click();
    assert.equal(await status.inputValue(), "completed");
    assert.equal(await title.inputValue(), "Edited title");
    const bounds = await status.boundingBox();
    assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= width);
    if (process.env.CW_EDIT_STATUS_SCREENSHOTS) {
      await mkdir(process.env.CW_EDIT_STATUS_SCREENSHOTS, {recursive:true});
      await page.screenshot({path:`${process.env.CW_EDIT_STATUS_SCREENSHOTS}/${theme}-${width}.png`});
    }
    await status.selectOption("");
    await save.click();
    await page.locator(".trk-sheet-wrap").waitFor({state:"hidden"});
    assert.deepEqual(writes.at(-1).changes, {status:""});
    await edit.click();
    assert.equal(await status.inputValue(), "");
    await title.fill("Renamed only");
    await save.click();
    await page.locator(".trk-sheet-wrap").waitFor({state:"hidden"});
    assert.deepEqual(writes.at(-1).changes, {title:"Renamed only"});
    assert.deepEqual(errors, []);
  });
}
