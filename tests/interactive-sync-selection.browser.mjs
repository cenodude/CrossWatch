/* tests/interactive-sync-selection.browser.mjs */
/* CrossWatch - Interactive review bulk selection, notices and responsive themes. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import './i18n-browser-setup.mjs';
import assert from "node:assert/strict";
import test from "node:test";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

async function fixture(t, theme) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1280,height:1000}});
  page.setDefaultTimeout(7000);
  const errors = [], selections = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  const rows = Array.from({length:91}, (_, i) => ({id:String(i), key:`tmdb:${i}`, feature:"history", result:i === 90 ? "blocked" : "update",
    operation:"update", provider:"SCROB", instance:"default", selectable:i !== 90, selected:false,
    item:{type:"show",title:`${i < 12 ? "Target" : "Other"} show ${i}`,watch_status:"dropped",_cw_status_mapping:{from:"watching",to:"dropped"}},
    before:{watch_status:"watching",_cw_status_mapping:true}}));
  const state = {id:"test",revision:1,selection_version:0,status:"review",pair:{source:"WETRAKR",target:"SCROB"},
    features:["history"],notices:[{feature:"history",provider:"SCROB",reason:"Status mapping",detail:"A provider entry needs attention."}]};
  const counts = () => ({changes:rows.length,selected:rows.filter(row => row.selected).length,attention:rows.filter(row => !row.selectable).length,conflicts:0});
  const matches = (row, filters) => (!filters.q || row.item.title.toLowerCase().includes(filters.q.toLowerCase()))
    && (!filters.feature || filters.feature === row.feature) && (!filters.result || filters.result === row.result);
  await page.route("https://sync.test/**", async route => {
    const request = route.request(), url = new URL(request.url());
    if (url.pathname.startsWith("/assets/")) {
      try {
        return route.fulfill({body:await readFile(new URL(".." + url.pathname, import.meta.url)),
          contentType:url.pathname.endsWith(".css") ? "text/css" : url.pathname.endsWith(".woff2") ? "font/woff2" : "text/javascript"});
      } catch { return route.fulfill({status:404,body:""}); }
    }
    if (url.pathname.endsWith("/selection")) {
      const payload = request.postDataJSON();
      selections.push(payload);
      for (const row of rows) if (row.selectable && (payload.ids ? payload.ids.includes(row.id) : matches(row, payload))) row.selected = payload.selected;
      state.selection_version++;
      return route.fulfill({json:{...state,counts:counts()}});
    }
    if (url.pathname.endsWith("/rows")) {
      const filtered = rows.filter(row => matches(row, Object.fromEntries(url.searchParams)));
      const offset = Number(url.searchParams.get("offset") || 0), limit = Number(url.searchParams.get("limit") || 75);
      return route.fulfill({json:{items:filtered.slice(offset, offset + limit),total:filtered.length,
        selectable:filtered.filter(row => row.selectable).length,selected:filtered.filter(row => row.selected).length,
        offset,limit,counts:counts(),selection_version:state.selection_version}});
    }
    if (url.pathname === "/api/interactive-sync/test") return route.fulfill({json:{...state,counts:counts()}});
    if (url.pathname.startsWith("/api/")) return route.fulfill({json:{}});
    const styles = ["themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme",
      "fonts/material-symbols-rounded","ui-shell","themes/flat","themes/original-coverage"];
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html data-cw-theme="${theme}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">${styles.map(name => `<link rel="stylesheet" href="/assets/${name}.css">`).join("")}</head><body><main id="page-interactive_sync"></main><script src="/assets/js/interactive-sync.js"></script></body></html>`});
  });
  await page.goto("https://sync.test/#interactive_sync?session=test");
  await page.waitForFunction(() => document.querySelector("[data-select-all]")?.disabled === false);
  return {page,rows,state,selections};
}

for (const theme of ["original","flat-dark","flat-light"]) {
  test(`${theme}: bulk selection spans filtered pages and notices stay compact`, async t => {
    const {page,rows,state,selections} = await fixture(t, theme);
    const all = page.locator("[data-select-all]");
    assert.equal(await all.isChecked(), false);
    assert.equal(await page.locator("#is-notices p").count(), 0);
    await all.focus();
    await page.keyboard.press("Space");
    await page.waitForFunction(() => document.querySelector('[data-action="apply"]')?.textContent.includes("(90)"));
    assert.equal(rows.filter(row => row.selected).length, 90);
    assert.equal(selections.at(-1).ids, undefined);
    await page.locator('[data-action="next"]').click();
    await page.waitForFunction(() => document.querySelector('[data-select="75"]')?.checked);
    assert.equal(await page.locator('[data-select="90"]').isDisabled(), true);
    await page.locator('[data-select="75"]').uncheck();
    await page.waitForFunction(() => document.querySelector('[data-select-all]')?.indeterminate);
    await page.locator('[data-action="select"]').click();
    await page.waitForFunction(() => document.querySelector('[data-select-all]')?.checked);
    await page.locator('[data-filter="query"]').fill("Target");
    await page.waitForFunction(() => document.querySelectorAll('[data-select]').length === 12 && !document.querySelector('[data-select-all]').disabled);
    await all.uncheck();
    await page.waitForFunction(() => document.querySelector('[data-action="apply"]')?.textContent.includes("(78)"));
    assert.equal(selections.at(-1).q, "Target");
    assert.equal(rows.filter(row => row.selected).length, 78);
    assert.equal(await page.locator('[data-action="select"]').innerText(), "select_all\nSelect all filtered");
    await page.locator('[data-select="0"]').check();
    await page.waitForFunction(() => document.querySelector('[data-select-all]')?.indeterminate);
    await page.locator('[data-action="toggle-notices"]').click();
    assert.equal(await page.locator("#is-notices p").count(), 1);
    await mkdir("test-results/interactive-sync-selection", {recursive:true});
    for (const width of [1280,320,360,390]) {
      await page.setViewportSize({width,height:1000});
      await page.locator(".is-list-actions").scrollIntoViewIfNeeded();
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1));
      const button = await page.locator('[data-action="select"]').boundingBox();
      assert.ok(button.x >= 0 && button.x + button.width <= width + 1);
      const paging = page.locator('.is-pagination');
      const controls = await paging.locator('button:visible').evaluateAll(nodes => nodes.map(node => {const r=node.getBoundingClientRect();return {y:r.y,width:r.width,height:r.height};}));
      assert.equal(controls.length,width<=760?2:4);
      assert.ok(controls.every(control=>Math.abs(control.y+control.height/2-controls[0].y-controls[0].height/2)<1),JSON.stringify({width,controls}));
      if(width<=760)assert.ok(controls.every(control=>control.width>=44&&control.height>=44));
      assert.equal(await paging.evaluate(node=>node.scrollWidth>node.clientWidth),false);
      await page.screenshot({path:`test-results/interactive-sync-selection/${theme}-${width}.png`});
    }
    await page.locator('[data-filter="query"]').fill("no matching title");
    await page.waitForFunction(() => document.querySelectorAll('[data-select]').length === 0 && document.querySelector('[data-select-all]')?.disabled);
    assert.equal(await page.locator('[data-action="select"]').isDisabled(), true);
    state.status = "error";
    state.notices = [{feature:"history",provider:"SCROB",reason:"Status mapping",detail:"Status read failed."}];
    await page.reload();
    await page.locator("#is-notices p").waitFor();
    assert.equal(await all.isDisabled(), true);
    assert.equal(await page.locator('[data-action="apply"]').isDisabled(), true);
  });
}
