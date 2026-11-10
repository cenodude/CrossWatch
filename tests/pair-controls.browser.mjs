/* tests/pair-controls.browser.mjs */
/* CrossWatch - Responsive pair ordering and reset browser regressions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const sheets = ["themes/tokens.css", "css/base.css", "crosswatch.css", "css/layout.css", "css/components.css", "css/pages.css", "css/page-theme.css", "ui-shell.css", "css/topology.css"];
const pairs = Array.from({length: 8}, (_, i) => ({id: `pair-${i}`, source: "WETRAKR", target: i % 2 ? "SCROB" : "FLOPPY", enabled: i !== 1, mode: i % 2 ? "one-way" : "two-way", features: {watchlist: true, history: {enable: true}, ratings: false}}));

async function open(t, {theme = "original", width = 1024, height = 768, touch = false, reducedMotion = "no-preference"} = {}) {
  const browser = await (process.env.CW_PAIR_BROWSER === "webkit" ? webkit : chromium).launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport: {width, height}, hasTouch: touch, isMobile: touch, reducedMotion});
  page.setDefaultTimeout(5000);
  const errors = [], requests = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  let savedPairs = structuredClone(pairs);
  await page.route("https://pairs.test/**", async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith("/assets/")) return route.fulfill({contentType: path.endsWith(".css") ? "text/css" : "text/javascript", body: await readFile(new URL(".." + path, import.meta.url))});
    if (path === "/api/pairs/reorder") {
      const order = route.request().postDataJSON();
      requests.push({path, body: order});
      savedPairs.sort((a, b) => order.indexOf(a.id) - order.indexOf(b.id));
      return route.fulfill({json: {ok: true, final_order: order}});
    }
    if (path === "/api/maintenance/clear-pair-state") {
      requests.push({path, body: route.request().postDataJSON()});
      return route.fulfill({json: {ok: true}});
    }
    if (path === "/api/pairs") return route.fulfill({json: savedPairs});
    if (path.startsWith("/api/")) return route.fulfill({json: {}});
    const attributes = theme === "original" ? 'class="cw-theme-original"' : `data-cw-theme="${theme}"`;
    const themeSheet = theme === "original" ? "themes/original-coverage.css" : "themes/flat.css";
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html ${attributes}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${[...sheets, themeSheet].map(name => `<link rel="stylesheet" href="/assets/${name}">`).join("")}<style>body{margin:0;padding:16px}.test-layout{display:grid;grid-template-columns:240px minmax(0,1fr);gap:24px;max-width:1600px;margin:auto}#sec-sync{min-width:0}@media(max-width:760px){.test-layout{grid-template-columns:minmax(0,1fr)}aside{display:none}}</style></head><body><div class="test-layout"><aside>Synchronization</aside><section id="sec-sync"><div id="pairs_list"></div></section></div><script>window.cx={pairs:${JSON.stringify(savedPairs)}};</script><script src="/assets/js/connections.pairs.overlay.js"></script></body></html>`});
  });
  await page.goto("https://pairs.test/");
  await page.locator(".pair-card").first().waitFor();
  return {page, requests};
}

const order = page => page.locator(".pair-card").evaluateAll(cards => cards.map(card => card.dataset.id));
const saved = page => page.waitForFunction(() => document.querySelector(".pair-order-status").textContent === "Pair order saved.");

for (const theme of ["original", "flat-dark", "flat-light"]) {
  test(`${theme}: phone, tablet portrait/landscape and desktop fit with accessible controls`, async t => {
    const {page} = await open(t, {theme, touch: true});
    const run = page.getByRole("button", {name: "Interactive sync", exact: true}).first();
    assert.equal(await run.getAttribute("data-tip"), "Interactive sync");
    assert.notEqual(await run.evaluate(el => getComputedStyle(el).background), await page.getByRole("button", {name: "Latest logs", exact: true}).first().evaluate(el => getComputedStyle(el).background));
    const direction = page.getByRole("img", {name: "Two-way: WETRAKR and FLOPPY", exact: true}).first();
    assert.equal(await page.getByRole("img", {name: "One-way: WETRAKR to SCROB", exact: true}).count(), 4);
    assert.equal(await page.locator(".pair-pill.mode,.pair-mobile-mode,.pair-left>.arrow").count(), 0);
    assert.equal(await direction.locator("svg").count(), 1);
    await direction.focus();
    await page.waitForFunction(() => document.querySelector(".cx-tip.on")?.textContent === "Two-way: WETRAKR and FLOPPY");
    await page.keyboard.press("Escape");
    assert.equal(await page.locator(".cx-tip.on").count(), 0);
    for (const [width, height] of [[320, 740], [390, 844], [740, 360], [844, 390], [932, 430], [768, 1024], [1024, 768], [1194, 834], [1280, 800], [1366, 1024], [1440, 900], [1920, 1080]]) {
      await page.setViewportSize({width, height});
      await page.waitForTimeout(180);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false, `${width}: page overflow`);
      assert.equal(await page.locator("#pairs_list").evaluate(el => el.scrollHeight > el.clientHeight + 1), false, `${width}: nested pair scrollbar`);
      const issues = await page.locator(".pair-card").first().evaluate(card => {
        const bounds = card.getBoundingClientRect();
        return [...card.querySelectorAll("button, label.power, .pair-pill.src, .pair-pill.dst")].flatMap(el => {
          if (!el.checkVisibility()) return [];
          const r = el.getBoundingClientRect();
          return r.left < bounds.left || r.right > bounds.right || (el.matches("button, label") && (r.width < 43 || r.height < 43)) ? [el.outerHTML] : [];
        });
      });
      assert.deepEqual(issues, [], `${width}: clipped or small controls`);
      const rows = await page.locator(".pair-card").first().evaluate(card => {
        const centers = selectors => [...card.querySelectorAll(selectors)].filter(el => el.checkVisibility()).map(el => { const r = el.getBoundingClientRect(); return r.top + r.height / 2; });
        const rows = [centers(".pair-action-buttons>.icon-btn"), centers(".cw-page-pair-handle,.pair-pill.src,.cw-page-pair-direction,.pair-pill.dst")];
        if (card.closest("#pairs_list").clientWidth > 860) rows.push(centers(".cw-page-pair-handle,.pair-pill.src,.pair-pill.dst,.cw-page-pair-direction,.pair-action-buttons>.icon-btn,.feat-beads"));
        return rows;
      });
      for (const row of rows) assert.ok(Math.max(...row) - Math.min(...row) <= 1, `${width}: controls wrapped onto another row: ${JSON.stringify(row)}`);
      assert.equal(await page.getByRole("button", {name: "Move left", exact: true}).count(), 0);
      assert.equal(await page.locator(".pair-card").nth(1).getByRole("button", {name: "Interactive sync"}).isDisabled(), true);
      await page.locator(".cw-page-pair-handle").first().focus();
      assert.equal(await page.locator(".cw-page-pair-handle").first().evaluate(el => getComputedStyle(el).outlineStyle), "solid");
      if (process.env.CW_PAIR_SCREENSHOTS) {
        await mkdir(process.env.CW_PAIR_SCREENSHOTS, {recursive: true});
        await page.screenshot({path: `${process.env.CW_PAIR_SCREENSHOTS}/${theme}-${width}.png`});
      }
    }
    await page.route("https://pairs.test/api/pairs", route => route.fulfill({json: []}));
    await page.evaluate(async () => { window.cx.pairs = []; await window.cxRenderPairsOverlay(); });
    assert.equal(await page.locator("#pairs_list").isVisible(), false);
  });
}

test("mouse drag animates, saves once and survives reload", async t => {
  const {page, requests} = await open(t, {width: 1440});
  await page.evaluate(() => { window.cacheInvalidations = 0; window._invalidatePairsCache = () => { window.cacheInvalidations++; }; });
  const handle = await page.locator(".cw-page-pair-handle").first().boundingBox();
  const target = await page.locator(".pair-card").nth(1).boundingBox();
  await page.mouse.move(handle.x + 20, handle.y + 20);
  await page.mouse.down();
  await page.mouse.move(handle.x + 20, target.y + target.height - 10);
  assert.equal(await page.locator(".pair-reordering").count(), 1);
  assert.equal(await page.locator(".pair-card").evaluateAll(cards => cards.some(card => card.getAnimations().length)), true);
  await page.mouse.up();
  await saved(page);
  assert.deepEqual((await order(page)).slice(0, 2), ["pair-1", "pair-0"]);
  assert.equal(requests.length, 1);
  assert.equal(await page.evaluate(() => window.cacheInvalidations), 1);
  await page.reload();
  assert.deepEqual((await order(page)).slice(0, 2), ["pair-1", "pair-0"]);
});

test("touch drag saves on release and cancellation leaves order unchanged", async t => {
  const {page, requests} = await open(t, {width: 390, height: 900, touch: true});
  const session = await page.context().newCDPSession(page);
  const handle = await page.locator(".cw-page-pair-handle").first().boundingBox();
  const target = await page.locator(".pair-card").nth(1).boundingBox();
  const send = (type, y) => session.send("Input.dispatchTouchEvent", {type, touchPoints: type === "touchEnd" || type === "touchCancel" ? [] : [{x: handle.x + 20, y}]});
  await send("touchStart", handle.y + 20);
  await send("touchMove", target.y + target.height - 20);
  await send("touchCancel");
  assert.deepEqual(await order(page), pairs.map(pair => pair.id));
  assert.equal(requests.length, 0);
  await page.waitForFunction(() => [...document.querySelectorAll(".pair-card")].every(card => !card.getAnimations().length));
  await send("touchStart", handle.y + 20);
  await send("touchMove", target.y + target.height - 20);
  assert.equal(await page.locator(".pair-reordering").count(), 1);
  await send("touchEnd");
  await saved(page);
  assert.deepEqual((await order(page)).slice(0, 2), ["pair-1", "pair-0"]);
  assert.equal(requests.length, 1);
});

test("keyboard reorder supports reduced motion and restores order after a rejected save", async t => {
  const {page} = await open(t, {reducedMotion: "reduce"});
  const first = page.locator('[data-id="pair-0"] .cw-page-pair-handle');
  await first.focus();
  await page.keyboard.press("ArrowDown");
  await saved(page);
  assert.deepEqual((await order(page)).slice(0, 2), ["pair-1", "pair-0"]);
  assert.equal(await first.evaluate(el => el === document.activeElement), true);
  assert.equal(await page.locator(".pair-card").evaluateAll(cards => cards.some(card => card.getAnimations().length)), false);
  await page.route("https://pairs.test/api/pairs/reorder", route => route.fulfill({json: {ok: false}}));
  const dialog = page.waitForEvent("dialog");
  await page.keyboard.press("ArrowDown");
  const failure = await dialog;
  assert.match(failure.message(), /previous order is restored/);
  await failure.accept();
  await page.waitForFunction(() => !document.querySelector(".pairs-board").inert);
  assert.deepEqual((await order(page)).slice(0, 2), ["pair-1", "pair-0"]);
});

test("dragging at the edge scrolls to later pairs and Escape cancels without saving", async t => {
  const {page, requests} = await open(t, {width: 1440, height: 450});
  const handle = await page.locator(".cw-page-pair-handle").first().boundingBox();
  const host = await page.locator("#pairs_list").boundingBox();
  await page.mouse.move(handle.x + 20, handle.y + 20);
  await page.mouse.down();
  await page.mouse.move(handle.x + 20, Math.min(438, host.y + host.height - 8));
  await page.waitForFunction(() => document.querySelector(".pairs-board").lastElementChild.dataset.id === "pair-0");
  assert.ok(await page.evaluate(() => window.scrollY > 0));
  assert.equal(await page.locator("#pairs_list").evaluate(el => el.scrollTop), 0);
  assert.equal(requests.length, 0);
  await page.keyboard.press("Escape");
  await page.mouse.up();
  assert.deepEqual(await order(page), pairs.map(pair => pair.id));
  assert.equal(requests.length, 0);
});

test("reset remains single-flight across a card refresh and respects unavailable controls", async t => {
  const {page} = await open(t);
  let release, calls = 0;
  const response = new Promise(resolve => { release = resolve; });
  await page.route("https://pairs.test/api/maintenance/clear-pair-state", async route => {
    calls++;
    await response;
    await route.fulfill({json: {ok: true}});
  });
  page.on("dialog", dialog => dialog.accept());
  await page.locator("[data-pair-reset]").first().click();
  await page.waitForFunction(() => document.querySelector("[data-pair-reset]").disabled);
  await page.evaluate(async () => { window.cx.pairs[0].mode = "one-way"; await window.cxRenderPairsOverlay(); });
  assert.equal(await page.locator("[data-pair-reset]").first().isDisabled(), true);
  await page.locator("[data-pair-reset]").first().dispatchEvent("click");
  assert.equal(calls, 1);
  release();
  await page.waitForFunction(() => !document.querySelector("[data-pair-reset]").disabled);
  await page.evaluate(async () => { window.cx.pairs[0].features = {}; await window.cxRenderPairsOverlay(); });
  assert.equal(await page.locator("[data-pair-reset]").first().isDisabled(), true);
  await page.evaluate(async () => { window.CW = {AuthState: {read: () => ({isManaged: true})}}; await window.cxRenderPairsOverlay(); });
  assert.equal(await page.locator("[data-pair-reset]").nth(1).isDisabled(), true);
});

test("reset cancellation makes no request; confirmation resets only the selected pair's enabled features", async t => {
  const {page, requests} = await open(t);
  const reset = page.locator('[data-id="pair-1"] [data-pair-reset]');
  page.once("dialog", dialog => dialog.dismiss());
  await reset.click();
  assert.equal(requests.length, 0);
  const messages = [];
  page.on("dialog", async dialog => { messages.push(dialog.message()); await dialog.accept(); });
  await reset.click();
  await page.waitForFunction(() => !document.querySelector('[data-id="pair-1"] [data-pair-reset]').disabled);
  assert.deepEqual(requests, [{path: "/api/maintenance/clear-pair-state", body: {pair_id: "pair-1", features: ["watchlist", "history"], clear_retry: true}}]);
  assert.match(messages[0], /Recent deletions won't carry across/);
  assert.match(messages[0], /Other pairs keep their sync state/);
  assert.match(messages[1], /Pair reset!/);
});

for (const [status, body, message] of [[409, {}, /task is running/], [403, {}, /permission/], [200, {ok: false}, /couldn't finish/]]) {
  test(`reset reports failure without success: ${status} ${JSON.stringify(body)}`, async t => {
    const {page} = await open(t);
    await page.route("https://pairs.test/api/maintenance/clear-pair-state", route => route.fulfill({status, json: body}));
    const messages = [];
    page.on("dialog", async dialog => { messages.push(dialog.message()); await dialog.accept(); });
    const reset = page.locator("[data-pair-reset]").first();
    await reset.click();
    await page.waitForFunction(() => !document.querySelector("[data-pair-reset]").disabled);
    assert.equal(messages.length, 2);
    assert.match(messages[1], message);
    assert.equal(await page.locator(".pair-card").count(), 8);
  });
}


for (const theme of ["original", "flat-dark", "flat-light"]) {
  test(`${theme}: narrow pair actions remain reachable without wrapping`, async t => {
    const {page, requests} = await open(t, {theme, width: 320, height: 740, touch: true});
    const card = page.locator(".pair-card").first();
    const more = card.getByRole("button", {name: "More pair actions", exact: true});
    const menu = card.locator(".pair-overflow");
    await more.click();
    await page.waitForFunction(() => document.activeElement?.hasAttribute("data-pair-logs"));
    await more.click();
    assert.equal(await menu.isVisible(), false);
    await more.click();
    await page.waitForFunction(() => document.activeElement?.hasAttribute("data-pair-logs"));
    assert.equal(await more.getAttribute("aria-expanded"), "true");
    assert.equal(await menu.locator("button:visible").count(), 4);
    const bounds = await menu.boundingBox();
    assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= 320 && bounds.y >= 0 && bounds.y + bounds.height <= 740);
    assert.ok(bounds.height <= 260, "More actions should remain a compact menu");
    if (process.env.CW_PAIR_SCREENSHOTS) {
      await mkdir(process.env.CW_PAIR_SCREENSHOTS, {recursive:true});
      await menu.screenshot({path:`${process.env.CW_PAIR_SCREENSHOTS}/${theme}-more.png`});
    }
    assert.equal(await menu.locator("button").first().evaluate(el => el === document.activeElement), true);
    await page.keyboard.press("Escape");
    assert.equal(await menu.isVisible(), false);
    assert.equal(await more.evaluate(el => el === document.activeElement), true);
    await page.evaluate(() => { window.openLogs = options => { window.lastPairLogs = options; }; });
    await more.click();
    await menu.getByRole("button", {name:"Latest logs", exact:true}).click();
    assert.deepEqual(await page.evaluate(() => window.lastPairLogs), {pairId:"pair-0", latest:true});
    assert.equal(await menu.isVisible(), false);
    await more.click();
    page.once("dialog", dialog => dialog.dismiss());
    await menu.getByRole("button", {name: "Reset pair", exact: true}).click();
    assert.equal(requests.length, 0);
    assert.equal(await menu.isVisible(), false);
    await more.click();
    await menu.getByRole("button", {name: "Delete", exact: true}).click();
    assert.equal(await menu.isVisible(), true);
    assert.equal(await menu.locator(".danger.is-confirming").count(), 1);
    await page.keyboard.press("Escape");
    assert.equal(await menu.locator(".is-confirming").count(), 0);
    await more.click();
    await page.setViewportSize({width: 1440, height: 900});
    await more.waitFor({state: "hidden"});
    await page.waitForFunction(() => document.querySelector(".pair-card .pair-action-buttons").querySelectorAll(".icon-btn").length === 7);
    assert.equal(await menu.isVisible(), false);
    assert.equal(await card.locator(".pair-action-buttons>.icon-btn:visible").count(), 6);
    assert.equal(await card.getByRole("button", {name: "Reset pair", exact: true}).isVisible(), true);
  });
}
