/* tests/editor-rating.browser.mjs */
/* CrossWatch - Editor star rating interaction and theme checks */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";

const half = {values:Array.from({length:20}, (_, i) => (i + 1) / 2), max:10, display_factor:1};

async function fixture(t, options = {}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1000,height:800}, ...options});
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route("**/*", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      try {
        const contentType = url.pathname.endsWith(".css") ? "text/css" : url.pathname.endsWith(".woff2") ? "font/woff2" : "text/javascript";
        return route.fulfill({contentType,body:await readFile(new URL(".." + url.pathname, import.meta.url))});
      } catch { return route.fulfill({status:404,body:""}); }
    }
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html data-cw-theme="flat-dark"><head><meta name="viewport" content="width=device-width, initial-scale=1">
      ${["themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","fonts/material-symbols-rounded","themes/flat","themes/original-coverage"].map(file => `<link rel="stylesheet" href="/assets/${file}.css">`).join("")}
      </head><body><button id="outside">Outside picker</button>
      <script src="/assets/js/editor/datetime.js"></script><script src="/assets/js/editor/extra-editors.js"></script>
      <script>
      window.openRating = (scale, rating = 8) => {
        document.querySelector('.cw-pop')?.remove();
        window.row = {raw:{rating,rated_at:'2026-11-03T02:32:25.123Z'}};
        window.changes = 0;
        CW.Editor.ExtraEditors.openRatingEditor(row,null,null,{
          state:{ratingScale:scale},
          openPopup:(_, build) => {
            const pop = Object.assign(document.createElement('div'),{className:'cw-pop'});
            Object.assign(pop.style,{top:'70px',left:'14px'});
            document.body.append(pop);
            build(pop,() => pop.remove());
          },
          appendPopupTitle:(pop,title) => pop.append(Object.assign(document.createElement('div'),{className:'cw-pop-title',textContent:title})),
          appendPopupActions:(pop,actions) => {
            const group = Object.assign(document.createElement('div'),{className:'cw-pop-actions'});
            actions.forEach(action => group.append(Object.assign(document.createElement('button'),{type:'button',className:'cw-pop-btn',textContent:action.label,onclick:action.onClick})));
            pop.append(group);
          },
          finishExtraChange:(_,el,close) => { window.changes++; close(); }
        });
      };
      </script></body></html>`});
  });
  await page.goto("https://editor-rating.test/");
  await page.evaluate(scale => window.openRating(scale), half);
  await page.evaluate(() => document.fonts.ready);
  return page;
}

test("Editor previews, resets and applies a half star once without changing its timestamp", async t => {
  const page = await fixture(t);
  await page.locator('[data-editor-rating="7.5"]').hover();
  assert.equal(await page.locator("[data-editor-rating-value]").textContent(), "7.5/10");
  assert.equal(await page.locator(".cw-editor-star.is-lit").count(), 15);
  assert.equal(await page.evaluate(() => row.raw.rating), 8);
  await page.locator("#outside").hover();
  assert.equal(await page.locator("[data-editor-rating-value]").textContent(), "8/10");
  await page.locator('[data-editor-rating="7.5"]').click();
  assert.deepEqual(await page.evaluate(() => ({...row.raw,changes})), {rating:7.5,rated_at:"2026-11-03T02:32:25.123Z",changes:1});
  assert.equal(await page.locator(".cw-pop").count(), 0);
});

test("whole-point Editor stars and keyboard navigation follow the provider scale", async t => {
  const page = await fixture(t);
  await page.evaluate(scale => window.openRating(scale), {...half,values:half.values.filter(Number.isInteger)});
  assert.equal(await page.locator(".cw-editor-star").count(), 10);
  assert.equal(await page.locator('[data-editor-rating="7.5"]').count(), 0);
  await page.locator('[data-editor-rating="8"]').focus();
  await page.keyboard.press("ArrowLeft");
  assert.equal(await page.locator("[data-editor-rating-value]").textContent(), "7/10");
  await page.keyboard.press("Enter");
  assert.equal(await page.evaluate(() => row.raw.rating), 7);
});

test("mobile Editor taps select half stars and Clear removes the rating", async t => {
  const page = await fixture(t, {viewport:{width:375,height:812},hasTouch:true,isMobile:true});
  await page.locator('[data-editor-rating="4.5"]').tap();
  assert.equal(await page.evaluate(() => row.raw.rating), 4.5);
  assert.equal(await page.evaluate(() => changes), 1);
  await page.evaluate(scale => window.openRating(scale, 4.5), half);
  await page.getByRole("button",{name:"Clear",exact:true}).tap();
  assert.equal(await page.evaluate(() => row.raw.rating), null);
});

test("Editor stars fit every theme on desktop and mobile, with empty and disabled choices", async t => {
  const page = await fixture(t);
  await page.emulateMedia({reducedMotion:"reduce"});
  for (const theme of ["original","flat-dark","flat-light"]) {
    await page.evaluate(theme => document.documentElement.dataset.cwTheme = theme, theme);
    for (const width of [1000,375]) {
      await page.mouse.move(0,0);
      await page.setViewportSize({width,height:800});
      await page.evaluate(scale => window.openRating(scale, null), {...half,values:half.values.filter(value => value >= 1)});
      assert.equal(await page.locator("[data-editor-rating-value]").textContent(), "Not rated");
      assert.equal(await page.locator('[data-editor-rating="0.5"]').isDisabled(), true);
      await page.locator('[data-editor-rating="7.5"]').hover();
      const layout = await page.locator(".cw-editor-stars").evaluate(node => {
        const rect = node.getBoundingClientRect();
        const lit = getComputedStyle(node.querySelector('.is-lit'));
        const unlit = getComputedStyle(node.querySelector('.cw-editor-star:not(.is-lit)'));
        const filledIcon = getComputedStyle(node.querySelector('.is-lit .material-symbols-rounded'));
        const emptyIcon = getComputedStyle(node.querySelector('.cw-editor-star:not(.is-lit) .material-symbols-rounded'));
        return {left:rect.left,right:rect.right,overflow:node.scrollWidth > node.clientWidth,lit:lit.color,unlit:unlit.color,background:lit.backgroundColor,filledIcon:filledIcon.webkitTextFillColor,emptyIcon:emptyIcon.webkitTextFillColor};
      });
      assert.ok(layout.left >= 0 && layout.right <= width, JSON.stringify(layout));
      assert.equal(layout.overflow, false);
      assert.notEqual(layout.lit, layout.unlit);
      assert.notEqual(layout.filledIcon, layout.emptyIcon);
      assert.equal(layout.background, "rgba(0, 0, 0, 0)");
      if (process.env.CW_RATING_SCREENSHOTS) await page.screenshot({path:`${process.env.CW_RATING_SCREENSHOTS}/editor-${theme}-${width}.png`});
    }
  }
  await page.evaluate(() => window.openRating({values:[],max:10,display_factor:1}));
  assert.equal(await page.locator(".cw-editor-star").count(), 0);
  assert.match(await page.locator(".cw-pop").textContent(), /Select a provider with rating support/);
});
