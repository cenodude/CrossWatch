/* tests/quick-add-rating.browser.mjs */
/* CrossWatch - Quick add star interactions and provider scale browser checks */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";

const half = {values:Array.from({length:20}, (_, i) => (i + 1) / 2), max:10, display_factor:1};
const whole = {values:Array.from({length:10}, (_, i) => i + 1), max:10, display_factor:1};
const target = (provider, rating_scale) => ({provider, instance:"default", ratings_enabled:true, rating_scale});

async function fixture(t, providers, options = {}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1100,height:850}, ...options});
  page.setDefaultTimeout(5000);
  const errors = [];
  const writes = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route("**/*", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      try {
        const contentType = url.pathname.endsWith(".css") ? "text/css" : url.pathname.endsWith(".woff2") ? "font/woff2" : "text/javascript";
        return route.fulfill({contentType, body:await readFile(new URL(".." + url.pathname, import.meta.url))});
      } catch { return route.fulfill({status:404,body:""}); }
    }
    if (url.pathname.startsWith("/api/")) {
      if (route.request().method() === "POST") writes.push(route.request().postDataJSON());
      return route.fulfill({json:url.pathname === "/api/manual/providers" ? {providers} : {ok:true}});
    }
    if (url.pathname !== "/") return route.fulfill({status:404,body:""});
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html data-cw-theme="flat-dark"><head><meta name="viewport" content="width=device-width, initial-scale=1">
      ${["themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","fonts/material-symbols-rounded","js/modals/core/styles","themes/flat","themes/original-coverage"].map(file => `<link rel="stylesheet" href="/assets/${file}.css">`).join("")}
      </head><body><script type="module">
      import modal from '/assets/js/modals/manual-watched/index.js';
      import {ModalHost} from '/assets/js/modals/core/host.js';
      window.openRating = async keys => {
        localStorage.setItem('cw.manualWatched.providers', JSON.stringify(keys));
        const host = new ModalHost();
        window.cxCloseModal = () => host.unmount();
        await host.mount(modal, {action:'rating',item:{tmdb:'1',type:'movie',title:'Example movie',year:2026}});
      };
      </script></body></html>`});
  });
  await page.goto("https://quick-add.test/");
  await page.waitForFunction(() => window.openRating);
  await page.evaluate(keys => window.openRating(keys), providers.map(row => `${row.provider}:${row.instance}`));
  await page.evaluate(() => document.fonts.ready);
  return {page,writes};
}

test("stars preview without saving, reset on exit, and submit the selected half point", async t => {
  const {page,writes} = await fixture(t, [target("CROSSWATCH", half)]);
  assert.equal(await page.locator(".cw-mw-star").count(), 20);
  await page.locator('[data-rating="7.5"]').hover();
  assert.equal(await page.locator("[data-rating-value]").textContent(), "7.5/10");
  assert.equal(await page.locator(".cw-mw-star.is-lit").count(), 15);
  assert.equal(await page.locator('[data-role="send"]').textContent(), "Rate 8/10 on 1 provider");
  await page.locator(".cw-mw-title").hover();
  assert.equal(await page.locator("[data-rating-value]").textContent(), "8/10");
  await page.locator('[data-rating="7.5"]').click();
  assert.equal(await page.locator('[data-rating="7.5"]').getAttribute("aria-pressed"), "true");
  assert.equal(writes.length, 0);
  await page.locator('[data-role="send"]').click();
  await page.waitForFunction(() => document.querySelector(".cw-mw-status")?.textContent.includes("Saved"));
  assert.equal(writes[0].rating, 7.5);
});

test("whole-point targets restrict stars and keyboard choices, and deselection restores halves", async t => {
  const {page} = await fixture(t, [target("CROSSWATCH", half),target("SIMKL", whole)]);
  assert.equal(await page.locator(".cw-mw-star").count(), 10);
  assert.equal(await page.locator('[data-rating="7.5"]').count(), 0);
  await page.locator('[data-rating="7"]').focus();
  await page.keyboard.press("ArrowRight");
  assert.equal(await page.locator(":focus").getAttribute("data-rating"), "8");
  await page.keyboard.press("Home");
  assert.equal(await page.locator(":focus").getAttribute("data-rating"), "1");
  await page.keyboard.press("End");
  await page.keyboard.press("Enter");
  assert.equal(await page.locator('[data-rating="10"]').getAttribute("aria-pressed"), "true");
  await page.locator('[data-provider-key="SIMKL:default"]').click();
  assert.equal(await page.locator(".cw-mw-star").count(), 20);
});

test("mobile taps select a half star without a duplicate action", async t => {
  const {page,writes} = await fixture(t, [target("CROSSWATCH", half)], {viewport:{width:375,height:812},hasTouch:true,isMobile:true});
  await page.locator('[data-rating="4.5"]').tap();
  assert.equal(await page.locator("[data-rating-value]").textContent(), "4.5/10");
  assert.equal(await page.locator('[data-rating="4.5"]').getAttribute("aria-pressed"), "true");
  assert.equal(writes.length, 0);
});

test("Silo keeps five stars and Stremio keeps reaction labels", async t => {
  const {page} = await fixture(t, [target("SILO", {values:[2,4,6,8,10],max:5,display_factor:0.5})]);
  assert.equal(await page.locator(".cw-mw-star").count(), 5);
  assert.equal(await page.locator("[data-rating-value]").textContent(), "4/5");
  const reaction = await fixture(t, [target("STREMIO", {values:[6,8],max:10,display_factor:1,labels:{6:"Liked",8:"Loved"}})]);
  assert.equal(await reaction.page.locator(".cw-mw-star").count(), 0);
  assert.equal(await reaction.page.locator(".cw-mw-reaction.active").textContent(), "Loved");
});

test("incompatible targets show no stars and cannot submit", async t => {
  const {page} = await fixture(t, [target("SIMKL",whole),target("STREMIO",{values:[6.5,9.5],max:10,display_factor:1,labels:{6.5:"Liked",9.5:"Loved"}})]);
  assert.equal(await page.locator(".cw-mw-star").count(), 0);
  assert.equal(await page.locator('[data-role="send"]').isDisabled(), true);
  assert.match(await page.locator(".cw-mw-form").textContent(), /no shared rating choices/);
});

test("stars fit desktop and narrow dialogs in each theme, including unavailable scores", async t => {
  const {page} = await fixture(t, [target("MDBLIST", {...half,values:half.values.filter(value => value >= 1)})]);
  for (const theme of ["original","flat-dark","flat-light"]) {
    await page.evaluate(theme => document.documentElement.dataset.cwTheme = theme, theme);
    for (const [width,height] of [[1100,850],[1024,768],[844,390],[740,360],[375,812],[320,740]]) {
      await page.setViewportSize({width,height});
      const layout = await page.locator(".cw-mw-stars").evaluate(node => {
        const rect = node.getBoundingClientRect();
        const lit = getComputedStyle(node.querySelector(".is-lit"));
        const unlit = getComputedStyle(node.querySelector(".cw-mw-star:not(.is-lit)"));
        return {left:rect.left,right:rect.right,width:rect.width,overflow:node.scrollWidth > node.clientWidth,background:lit.backgroundColor,lit:lit.color,unlit:unlit.color};
      });
      assert.ok(layout.left >= 0 && layout.right <= width && layout.width > 0, JSON.stringify(layout));
      assert.equal(layout.overflow, false);
      assert.equal(layout.background, "rgba(0, 0, 0, 0)");
      assert.notEqual(layout.lit, layout.unlit);
      assert.equal(await page.locator('[data-rating="0.5"]').isDisabled(), true);
      if (process.env.CW_RATING_SCREENSHOTS) await page.screenshot({path:`${process.env.CW_RATING_SCREENSHOTS}/${theme}-${width}.png`});
    }
  }
});
