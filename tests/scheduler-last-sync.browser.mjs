/* tests/scheduler-last-sync.browser.mjs */
/* CrossWatch - Last sync history in scheduler tooltips */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import assert from "node:assert/strict";
import {mkdir, readFile} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

const now = Date.parse("2026-11-07T17:20:00Z");
const themes = {
  original: ['class="cw-theme-original"', "themes/original-coverage.css"],
  "flat-dark": ['data-cw-theme="flat-dark" class="cw-theme-dark"', "themes/flat.css"],
  "flat-light": ['data-cw-theme="flat-light" class="cw-theme-light"', "themes/flat.css"],
};

for (const [theme, [attributes, sheet]] of Object.entries(themes)) {
  for (const width of [390, 1280]) {
    test(`${theme} ${width}px: tooltip shows sync history and handles running, empty and unavailable states`, async t => {
      const browser = await chromium.launch({headless: true});
      t.after(() => browser.close());
      const page = await browser.newPage({viewport: {width, height: 700}, locale: "en-US", timezoneId: "Europe/Amsterdam", hasTouch: width < 800, isMobile: width < 800});
      page.setDefaultTimeout(5000);
      const errors = [];
      page.on("pageerror", error => errors.push(error.message));
      t.after(() => assert.deepEqual(errors, []));
      let summary = {running: false, finished_at: (now - 12 * 60_000) / 1000};
      await page.addInitScript(fixed => { Date.now = () => fixed; }, now);
      await page.route("https://scheduler-tooltip.test/**", async route => {
        const path = new URL(route.request().url()).pathname;
        if (path.startsWith("/assets/")) return route.fulfill({contentType: path.endsWith(".woff2") ? "font/woff2" : path.endsWith(".css") ? "text/css" : "text/javascript; charset=utf-8", body: await readFile(new URL(".." + path, import.meta.url))});
        if (path === "/api/run/summary") return route.fulfill({status: summary ? 200 : 503, json: summary || {error: "unavailable"}});
        if (path === "/api/health") return route.fulfill({json: {ok: true, status: "ok"}});
        return route.fulfill({contentType: "text/html", body: `<!doctype html><html ${attributes}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${["fonts/material-symbols-rounded.css", "themes/tokens.css", "css/base.css", "crosswatch.css", "css/layout.css", "css/components.css", "css/pages.css", "css/page-theme.css", "ui-shell.css", sheet].map(name => `<link rel="stylesheet" href="/assets/${name}">`).join("")}</head><body style="padding:180px 16px 16px">
          <section id="ops-card"><div class="action-row"></div></section>
          <script>
            const cfg={scheduling:{enabled:true,mode:'hourly'}};
            window.CW={API:{Config:{load:async()=>cfg},Pairs:{list:async()=>[]},Scheduling:{status:async()=>({config:cfg.scheduling,next_run_at:${now / 1000 + 3600},last_run_at:${now / 1000 - 1},last_backup_job_id:'backup'})}}};
          </script><script src="/assets/helpers/touch-help.js"></script><script src="/assets/js/schedulerbanner.js"></script>
        </body></html>`});
      });
      await page.goto("https://scheduler-tooltip.test/");
      const chip = page.locator("#chip-sched");
      await page.waitForFunction(() => document.querySelector("#chip-sched")?.dataset.tip?.includes("12 minutes ago"));
      assert.match(await chip.getAttribute("aria-label"), /Last sync: 12 minutes ago\nNov 7, 2026, 06:08 PM GMT\+1/);
      if (width <= 760) {
        for (const viewport of [320, 760, 761, 1280, 390]) {
          await page.setViewportSize({width:viewport,height:700});
          assert.equal(await page.locator('.cw-status-dock').isVisible(),viewport>760);
          for (const id of ['chip-sched','chip-watch','chip-pairs','chip-health']) {
            if (viewport<=760) assert.equal(await page.locator(`#${id}`).isVisible(),false);
          }
        }
        return;
      }
      if (width < 800) await chip.tap();
      else await chip.focus();
      const tip = page.locator(width < 800 ? "#cw-touch-help" : "#chip-sched .cw-hub-tip");
      await page.waitForFunction(selector => getComputedStyle(document.querySelector(selector)).opacity === "1", width < 800 ? "#cw-touch-help" : "#chip-sched .cw-hub-tip");
      assert.match(await tip.textContent(), /Last sync: 12 minutes ago/);
      const box = await tip.boundingBox();
      assert.ok(box.x >= 0 && box.x + box.width <= width);
      await mkdir(".pytest_tmp/scheduler-tooltip", {recursive: true});
      await page.evaluate(() => document.fonts.ready);
      await page.screenshot({path: `.pytest_tmp/scheduler-tooltip/${theme}-${width}.png`});
      if (width < 800) await page.getByRole("button", {name: "Close", exact: true}).click();

      for (const [value, expected] of [
        [{running: true, started_at: "2026-11-07T17:18:00Z"}, "Sync started: 2 minutes ago"],
        [{running: false, finished_at: now - 10_000}, "Last sync: Just now"],
        [{running: false}, "Last sync: No runs recorded"],
        [{finished_at: "invalid"}, "Last sync: No runs recorded"],
        [null, "Last sync: Unavailable"],
      ]) {
        summary = value;
        await page.evaluate(() => document.dispatchEvent(new Event("scheduling-status-refresh")));
        await page.waitForFunction(text => document.querySelector("#chip-sched")?.dataset.tip?.includes(text), expected);
        assert.equal(await chip.isVisible(), true);
      }
    });
  }
}
