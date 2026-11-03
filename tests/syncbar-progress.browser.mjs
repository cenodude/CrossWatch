/* tests/syncbar-progress.browser.mjs */
/* CrossWatch - Sync bar layout checks in Chromium with the real stylesheets */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";

const SHARED = ["themes/tokens.css", "css/base.css", "crosswatch.css", "css/layout.css", "css/components.css", "css/pages.css", "css/page-theme.css", "ui-shell.css"];
const THEMES = {
  "flat-dark": {attributes: 'data-cw-theme="flat-dark" class="cw-theme-dark"', sheet: "themes/flat.css"},
  "flat-light": {attributes: 'data-cw-theme="flat-light" class="cw-theme-light"', sheet: "themes/flat.css"},
  original: {attributes: 'class="cw-theme-original"', sheet: "themes/original-coverage.css"},
};

async function open(t, theme, viewport = {width: 1100, height: 260}) {
  const browser = await chromium.launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport});
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route("https://bar.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      const contentType = url.pathname.endsWith(".css") ? "text/css" : "text/javascript";
      return route.fulfill({contentType, body: await readFile(new URL(".." + url.pathname, import.meta.url), "utf8")});
    }
    const links = [...SHARED, THEMES[theme].sheet].map(name => `<link rel="stylesheet" href="/assets/${name}">`).join("");
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html ${THEMES[theme].attributes}><head><meta name="viewport" content="width=device-width,initial-scale=1">${links}<style>*{transition:none!important}</style></head><body style="padding:24px"><div id="ux-progress" style="max-width:900px"></div><script src="/assets/js/syncbar.js"></script></body></html>`});
  });
  await page.goto("https://bar.test/");
  await page.evaluate(() => {
    window.bar = new window.SyncBar({el: document.getElementById("ux-progress")});
    window.bar.markInit();
    window.bar.setPair({src: "STREMIO", dst: "SCROB", feature: "history"});
    window.bar.applyStart({event: "apply:add:start", dst: "SCROB", feature: "history", count: 1554});
  });
  return page;
}

const layoutAt = (page, progress) => page.evaluate(value => {
  window.bar.fromSummary({running: true, _phase: {apply: {total: 1554, done: 0, progress: value, final: false}}});
  const box = selector => document.querySelector(selector).getBoundingClientRect();
  const [fly, badge, rail, fill] = [".sb-fly", ".sb-badge", ".sb-rail", ".sb-fill"].map(box);
  return {fly: {left: fly.left, right: fly.right, top: fly.top, bottom: fly.bottom}, badge: {left: badge.left, top: badge.top, bottom: badge.bottom}, rail: {left: rail.left, right: rail.right, width: rail.width}, fill: {width: fill.width, height: fill.height}};
}, progress);

for (const theme of Object.keys(THEMES)) {
  test(`${theme}: the bar fills and the pair label stays clear of the badge`, async t => {
    const page = await open(t, theme);
    for (const progress of [0, 0.25, 0.5, 0.75, 1]) {
      const box = await layoutAt(page, progress);
      assert.ok(box.fly.right <= box.badge.left, `label reaches the badge at ${progress}: ${box.fly.right} > ${box.badge.left}`);
      assert.ok(box.fly.left >= box.rail.left - 1, `label leaves the rail at ${progress}`);
      assert.ok(box.fill.height > 0, "the bar has height");
      assert.ok(Math.abs(box.fill.width - box.rail.width * (67 + 30 * progress) / 100) < 12, `the fill does not match ${progress}: ${box.fill.width}`);
    }
  });
}

test("the page does not scroll sideways on a phone", async t => {
  const page = await open(t, "flat-dark", {width: 390, height: 420});
  await layoutAt(page, 0.8);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), false);
  assert.match(await page.locator(".sb-mobile-detail").textContent(), /Adding to SCROB history/);
  assert.match(await page.locator(".sb-mobile-percent").textContent(), /91%/);
});
