/* tests/mobile-sync.browser.mjs */
/* CrossWatch - Mobile sync status, help and pair navigation regressions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

const sheets = ["fonts/material-symbols-rounded.css", "themes/tokens.css", "css/base.css", "crosswatch.css", "css/layout.css", "css/components.css", "css/pages.css", "css/page-theme.css", "ui-shell.css"];
const themes = {
  original: ['class="cw-theme-original"', "themes/original-coverage.css"],
  "flat-dark": ['data-cw-theme="flat-dark" class="cw-theme-dark"', "themes/flat.css"],
  "flat-light": ['data-cw-theme="flat-light" class="cw-theme-light"', "themes/flat.css"],
};

async function open(t, theme, width = 390) {
  const browser = await chromium.launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport: {width, height: 844}, hasTouch: width < 800, isMobile: width < 800});
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  const frontend = await readFile(new URL("../ui_frontend.py", import.meta.url), "utf8");
  const navigation = await page.evaluate(source => {
    const doc = new DOMParser().parseFromString(source, "text/html");
    return [...doc.scripts].map(script => script.textContent).filter(script => script.startsWith("(()=>{const panes=") || script.startsWith("(()=>{const scrollTo="));
  }, frontend);
  assert.equal(navigation.length, 2);
  await page.route("https://mobile.test/**", async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith("/assets/")) return route.fulfill({contentType: path.endsWith(".woff2") ? "font/woff2" : path.endsWith(".css") ? "text/css" : "text/javascript", body: await readFile(new URL(".." + path, import.meta.url))});
    if (path.startsWith("/api/")) return route.fulfill({json: path === "/api/health" ? {ok: true, status: "ok"} : path === "/api/pairs" ? Array.from({length:8}, (_, i) => ({id:String(i),source:"PLEX",target:"TRAKT",source_instance:"home",enabled:i!==1,features:{history:true}})) : []});
    const [attributes, sheet] = themes[theme];
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html ${attributes}><head><meta name="viewport" content="width=device-width,initial-scale=1">${[...sheets, sheet].map(name => `<link rel="stylesheet" href="/assets/${name}">`).join("")}</head><body style="padding:16px">
      <section id="ops-card" class="cw-main-card"><div class="action-row"></div></section>
      <label>Verify after write <button class="cx-help" type="button" data-tip="Verify after write: re-check the destination after writes.">?</button><input type="checkbox" id="verify"></label>
      <button id="open-modal" type="button">Pair settings</button><dialog id="modal"><button class="cx-help" type="button" data-tip="Long help for a pair setting. This should remain readable inside the dialog without changing the setting.">?</button><input id="modal-setting" type="checkbox"></dialog>
      <div id="page-settings"><div id="cw-settings-left"><div class="cw-settings-pane active" data-pane="sync"><section id="sec-sync" class="open"><div id="pairs_list"></div></section></div><div class="cw-settings-pane" data-pane="overview"></div><div class="cw-settings-pane" data-pane="scheduling"></div></div></div>
      <script>
      window.cx={pairs:Array.from({length:8},(_,i)=>({id:String(i),source:'PLEX',target:'TRAKT',source_instance:'home',enabled:i!==1,features:{history:true}}))};
      window.cfg={scheduling:{enabled:true,mode:'hourly'},scrobble:{enabled:true,sources:{watcher:true,webhook:true}}};
      window.schedStatus={config:cfg.scheduling,next_run_at:Math.floor(Date.now()/1000)+3600,running:false};
      window.CW={API:{Config:{load:async()=>cfg},Pairs:{list:async()=>cx.pairs},Scheduling:{status:async()=>schedStatus},Watch:{status:async()=>({alive:true}),currentlyWatching:async()=>({streams:[]})}}};
      window.__cwSettingsPane='sync';window.openMaintenance=()=>{};
      document.querySelector('#open-modal').onclick=()=>document.querySelector('#modal').showModal();
      </script>${navigation.map(script => `<script>${script}</script>`).join("")}
      <script src="/assets/helpers/touch-help.js"></script><script src="/assets/js/schedulerbanner.js"></script><script src="/assets/js/connections.pairs.overlay.js"></script>
      </body></html>`});
  });
  await page.goto("https://mobile.test/");
  await page.waitForFunction(() => document.querySelector("#sched-meta")?.textContent.startsWith("Next run"));
  return page;
}

for (const theme of Object.keys(themes)) {
  test(`${theme}: phone hides status dock and keeps setting help accessible`, async t => {
    const page = await open(t, theme, 320);
    assert.equal(await page.locator(".cw-status-dock").isVisible(), false);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    await page.evaluate(() => { window.schedStatus.running = true; document.dispatchEvent(new Event("scheduling-status-refresh")); });
    await page.waitForFunction(() => document.querySelector("#sched-value").textContent === "running");
    assert.equal(await page.locator("#sched-meta").isVisible(), false);
    await page.locator("label .cx-help").tap();
    assert.equal(await page.locator("#verify").isChecked(), false);
    assert.match(await page.locator("#cw-touch-help").textContent(), /re-check the destination/);
    await page.getByRole("button", {name: "Close", exact: true}).tap();
    await page.locator("#open-modal").tap();
    await page.locator("#modal .cx-help").tap();
    assert.equal(await page.locator("#modal #cw-touch-help").isVisible(), true);
    assert.equal(await page.locator("#modal-setting").isChecked(), false);
    if (process.env.CW_MOBILE_SCREENSHOTS) {
      await mkdir(process.env.CW_MOBILE_SCREENSHOTS, {recursive: true});
      await page.screenshot({path: `${process.env.CW_MOBILE_SCREENSHOTS}/${theme}-help.png`});
    }
    await page.keyboard.press("Escape");
    assert.equal(await page.locator("#cw-touch-help").isVisible(), false);
    assert.equal(await page.locator("#modal").isVisible(), true);
    await page.keyboard.press("Escape");
    if (process.env.CW_MOBILE_SCREENSHOTS) await page.screenshot({path: `${process.env.CW_MOBILE_SCREENSHOTS}/${theme}-status.png`});
    await page.evaluate(() => { window.schedStatus.config.enabled = false; document.dispatchEvent(new Event("scheduling-status-refresh")); });
    await page.waitForFunction(() => document.querySelector("#chip-sched").classList.contains("is-hidden"));
  });

  test(`${theme}: refreshing phone pairs preserves the page scroll`, async t => {
    const page = await open(t, theme);
    await page.waitForFunction(() => document.querySelectorAll(".pair-card").length === 8);
    await page.evaluate(() => window.scrollTo(0, 800));
    await page.evaluate(() => { window.originalPairCard = document.querySelector(".pair-card"); });
    const before = await page.evaluate(() => scrollY);
    assert.ok(before > 100);
    await page.evaluate(async () => { await window.cxRenderPairsOverlay(true); window.cwSyncJump(); });
    assert.equal(await page.evaluate(() => scrollY), before);
    assert.equal(await page.evaluate(() => window.originalPairCard === document.querySelector(".pair-card")), true);
    assert.equal(await page.locator("#pairs_list").evaluate(el => el.style.maxHeight), "");
    await page.route("https://mobile.test/api/pairs", route => route.fulfill({json: []}));
    await page.evaluate(async () => { window.cx.pairs = []; await window.cxRenderPairsOverlay(); });
    assert.equal(await page.locator("#pairs_list").isVisible(), false);
    await page.unroute("https://mobile.test/api/pairs");
    await page.evaluate(() => window.cxRenderPairsOverlay(true));
    assert.equal(await page.locator(".pair-card").count(), 8);
  });
}

test("desktop help opens from the keyboard and leaves the setting unchanged", async t => {
  const page = await open(t, "flat-light", 1280);
  await page.locator("label .cx-help").focus();
  await page.keyboard.press("Enter");
  assert.equal(await page.locator("#cw-touch-help").isVisible(), true);
  assert.equal(await page.locator("#verify").isChecked(), false);
  await page.keyboard.press("Escape");
  assert.equal(await page.locator("#cw-touch-help").isVisible(), false);
});
