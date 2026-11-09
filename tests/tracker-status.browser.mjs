/* tests/tracker-status.browser.mjs */
/* CrossWatch - Direct bulk status menu, placement and keyboard interaction */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

for (const theme of ["original","flat-dark","flat-light"]) for (const width of [320,1440]) {
  test(`${theme}: bulk status menu at ${width}px`,async t=>{
    const browser=await chromium.launch({headless:true});
    t.after(()=>browser.close());
    const page=await browser.newPage({viewport:{width,height:900},isMobile:width<800,hasTouch:width<800,reducedMotion:"reduce"});
    page.setDefaultTimeout(5000);
    const errors=[],writes=[];
    page.on("pageerror",error=>errors.push(error.message));
    const items=[1,2].map(id=>({key:`movie:${id}`,type:"movie",title:`Movie ${id}`,year:2026,date:"2026-11-08T12:00:00Z"}));
    await page.route("https://tracker-status.test/**",async route=>{
      const path=new URL(route.request().url()).pathname;
      if(path.startsWith("/assets/")) {
        try {return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".woff2")?"font/woff2":"text/javascript"});}
        catch {return route.fulfill({status:404,body:""});}
      }
      if(path.startsWith("/api/")) {
        let json={ok:true};
        if(path==="/api/tracker/summary") json={provider_instance:"default",instances:[{id:"default",label:"Default"}],counts:{history:2},health:{},notes:{total:0,statuses:{},tags:[]}};
        if(path==="/api/tracker/items") json={items,total:2};
        if(path==="/api/tracker/titles/status") {
          writes.push(route.request().postDataJSON());
          json={ok:true,count:2,batch:"batch-status"};
        }
        return route.fulfill({json});
      }
      const attrs=theme==="original"?'class="cw-theme-original"':`data-cw-theme="${theme}"`;
      const css=["fonts/material-symbols-rounded","themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","ui-shell",theme==="original"?"themes/original-coverage":"themes/flat"];
      return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${attrs}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><main id="layout"><section id="page-tracker" class="cw-themed-page"></section></main><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
    });
    await page.goto("https://tracker-status.test/");
    await page.locator(".trk-play").first().waitFor();
    await page.locator('[data-trk-layout="grid"]').click();
    await page.locator('[data-trk-pick="0"]').check();
    await page.locator('[data-trk-pick="1"]').check();
    const trigger=page.locator('[data-trk-action="status-picked"]');
    const menu=page.locator("#trk-bulk-status-menu");
    await trigger.click();
    await menu.waitFor();
    assert.equal(await trigger.getAttribute("aria-expanded"),"true");
    assert.equal(await page.locator(".trk-sheet:visible").count(),0);
    assert.equal(await menu.locator('[role="menuitem"]').count(),6);
    const bounds=await menu.boundingBox(),button=await trigger.boundingBox();
    assert.ok(bounds.x>=0&&bounds.x+bounds.width<=width&&bounds.y>=0&&bounds.y+bounds.height<=button.y);
    await page.waitForTimeout(200);
    assert.equal(await menu.isVisible(),true);
    await page.keyboard.press("ArrowDown");
    assert.equal(await page.evaluate(()=>document.activeElement.dataset.trkBulkSetStatus),"planned");
    await page.keyboard.press("Escape");
    assert.equal(await menu.isVisible(),false);
    assert.equal(await trigger.getAttribute("aria-expanded"),"false");
    await trigger.click();
    await trigger.click();
    assert.equal(await menu.isVisible(),false);
    await trigger.click();
    await page.locator(".cw-page-hero-kicker").click();
    assert.equal(await menu.isVisible(),false);
    await trigger.click();
    if(process.env.CW_STATUS_SCREENSHOTS) {
      await mkdir(process.env.CW_STATUS_SCREENSHOTS,{recursive:true});
      await page.screenshot({path:`${process.env.CW_STATUS_SCREENSHOTS}/${theme}-${width}.png`});
    }
    await menu.locator('[data-trk-bulk-set-status="completed"]').click();
    await page.waitForFunction(()=>document.querySelector('[data-trk-slot="selection-bar"]').hidden);
    assert.equal(writes.length,1);
    assert.deepEqual(writes[0],{provider_instance:"default",keys:["movie:1","movie:2"],action:"status",status:"completed",view:"history",feature:"history"});
    assert.equal(await page.locator(".trk-sheet:visible").count(),0);
    assert.deepEqual(errors,[]);
  });
}
