/* tests/dialog-keyboard.browser.mjs */
/* CrossWatch - Chromium keyboard regressions using complete frontend scripts and mocked APIs */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";

async function fixture(t) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route("https://dialogs.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      return route.fulfill({contentType:"text/javascript", body:await readFile(new URL(".." + url.pathname, import.meta.url), "utf8")});
    }
    if (url.pathname.startsWith("/api/")) {
      let data = {ok:true,items:[],files:[],users:[]};
      if (url.pathname === "/api/activity/history") data = {ok:true,total:2,has_more:url.searchParams.get("offset") === "0",items:[{title:"Movie",media_type:"movie",status:"ok"}]};
      if (url.pathname === "/api/insights") data = {features:{watchlist:{providers:{crosswatch:1}}},crosswatch_snapshots:{_profiles:[{id:"default",label:"Default"},{id:"second",label:"Second"}]}};
      return route.fulfill({json:data});
    }
    return route.fulfill({contentType:"text/html",body:`<!doctype html><style>
      .hidden,.cw-snap-hidden{display:none!important}#cw-trailer{display:none}#cw-trailer.show{display:block}
      .activity-modal,.cw-snap-picker,.cw-editor-send-overlay,.cx-backdrop{position:fixed;inset:0;background:white}
      .cw-icon-select-menu{position:fixed;z-index:99999;background:white}
      </style><button id="opener">Open</button><button id="activity-view-all">Activity</button><input id="outside">
      <section id="stats-card"><span id="stat-now"></span></section>
      <script src="/assets/helpers/dialog-focus.js"></script><script src="/assets/helpers/icon-select.js"></script>
      <script src="/assets/helpers/trailer.js"></script><script src="/assets/js/editor/send-modal.js"></script>
      <script src="/assets/js/activity.js"></script><script src="/assets/js/insights.js"></script>
      <script type="module">import {ModalHost} from '/assets/js/modals/core/host.js';window.ModalHost=ModalHost;</script>`});
  });
  await page.goto("https://dialogs.test/");
  await page.waitForFunction(() => window.ModalHost && window.Insights);
  return page;
}

async function inside(page, selector) {
  assert.equal(await page.evaluate(selector => document.querySelector(selector).contains(document.activeElement), selector), true);
}

test("Activity recovers Tab and Escape after disabled controls, text clicks and portalled filters", async t => {
  const page = await fixture(t);
  await page.click("#activity-view-all");
  await page.waitForFunction(() => !document.querySelector("#activity-load").disabled);
  await page.click("#activity-load");
  await page.waitForFunction(() => document.querySelector("#activity-load").textContent === "All loaded");
  await page.keyboard.press("Tab");
  await inside(page, ".activity-modal");
  await page.click(".activity-head-title");
  await page.keyboard.press("Escape");
  assert.equal(await page.locator(".activity-modal").isVisible(), false);
  await page.click("#activity-view-all");
  const filter = page.locator('.activity-modal .cw-icon-select-btn[title="All media"]');
  await filter.click();
  await page.keyboard.press("Escape");
  assert.equal(await page.locator(".activity-modal").isVisible(), true);
  assert.equal(await page.locator(".cw-icon-select-menu:not(.hidden)").count(), 0);
  await filter.click();
  await page.locator('.cw-icon-select-menu:not(.hidden) [data-value="movie"]').click();
  await page.keyboard.press("Shift+Tab");
  await inside(page, ".activity-modal");
  await page.keyboard.press("Escape");
  assert.equal(await page.locator(".activity-modal").isVisible(), false);
});

test("Snapshot instance replacement retains keyboard dismissal and dropdown-first Escape", async t => {
  const page = await fixture(t);
  await page.click('.tile[data-provider="crosswatch"]');
  const picker = page.locator(".cw-snap-picker");
  await picker.locator(".cw-icon-select-btn").click();
  await page.keyboard.press("Escape");
  assert.equal(await picker.isVisible(), true);
  assert.equal(await page.locator(".cw-icon-select-menu:not(.hidden)").count(), 0);
  await picker.locator(".cw-icon-select-btn").click();
  await page.locator('.cw-icon-select-menu:not(.hidden) [data-value="second"]').click();
  await page.keyboard.press("Tab");
  await inside(page, ".cw-snap-picker");
  await page.keyboard.press("Escape");
  assert.equal(await picker.isVisible(), false);
});

test("Shared host closes its dropdown first and trailer restores Send focus", async t => {
  const page = await fixture(t);
  await page.evaluate(async () => {
    window.host = new ModalHost();
    await host.mount({mount(root) {root.innerHTML = '<h2>Settings</h2><select><option>A</option><option>B</option></select>';}});
  });
  await page.locator(".cx-modal-shell .cw-icon-select-btn").click();
  await page.keyboard.press("Escape");
  assert.equal(await page.locator(".cx-modal-shell").count(), 1);
  assert.equal(await page.locator(".cw-icon-select-menu:not(.hidden)").count(), 0);
  await page.keyboard.press("Escape");
  assert.equal(await page.locator(".cx-modal-shell").count(), 0);
  await page.evaluate(() => CW.Editor.SendModal.open({state:{kind:"watchlist"},selectedRowsForSend:()=>[{type:"movie",ids:{tmdb:"1"}}],isProviderPickerSource:()=>false,fetchJSON:async()=>({providers:[]})}));
  await page.evaluate(() => CW.Trailer.open("about:blank", "Trailer"));
  await page.keyboard.press("Escape");
  await inside(page, "#cw-editor-send-modal");
  await page.keyboard.press("Escape");
  assert.equal(await page.locator("#cw-editor-send-modal").count(), 0);
});


for (const underlying of ["activity", "snapshot"]) {
  test(`Shared host owns Escape and Tab over ${underlying} after focus loss`, async t => {
    const page = await fixture(t);
    const selector = underlying === "activity" ? ".activity-modal" : ".cw-snap-picker";
    await page.click(underlying === "activity" ? "#activity-view-all" : '.tile[data-provider="crosswatch"]');
    await page.evaluate(async () => {
      window.host = new ModalHost();
      await host.mount({mount(root) {root.innerHTML = '<h2>Upgrade</h2><button id="busy-control">Save</button>';}});
      const button = document.querySelector("#busy-control");
      button.focus();
      button.disabled = true;
    });
    await page.keyboard.press("Tab");
    await inside(page, ".cx-modal-shell");
    await page.evaluate(() => {host.setOptions({dismissible:false});document.activeElement.blur();});
    await page.keyboard.press("Escape");
    assert.equal(await page.locator(".cx-modal-shell").count(), 1);
    assert.equal(await page.locator(selector).isVisible(), true);
    await page.evaluate(() => host.setOptions({dismissible:true}));
    await page.keyboard.press("Escape");
    assert.equal(await page.locator(".cx-modal-shell").count(), 0);
    assert.equal(await page.locator(selector).isVisible(), true);
    await page.keyboard.press("Escape");
    assert.equal(await page.locator(selector).isVisible(), false);
  });
}

test("Send recovers focus and dismisses after focus moves to the page", async t => {
  const page = await fixture(t);
  await page.evaluate(() => CW.Editor.SendModal.open({state:{kind:"watchlist"},selectedRowsForSend:()=>[{type:"movie",ids:{tmdb:"1"}}],isProviderPickerSource:()=>false,fetchJSON:async()=>({providers:[]})}));
  await page.evaluate(() => document.activeElement.blur());
  await page.keyboard.press("Tab");
  await inside(page, "#cw-editor-send-modal");
  await page.evaluate(() => document.activeElement.blur());
  await page.keyboard.press("Escape");
  assert.equal(await page.locator("#cw-editor-send-modal").count(), 0);
});


test("Shared host preserves Tab order through closed and open summaries", async t => {
  const page = await fixture(t);
  await page.evaluate(async () => {
    window.host = new ModalHost();
    await host.mount({mount(root) {
      root.innerHTML = `<button id="a">First</button>
        <details><summary id="s1">Closed section</summary><input id="in1"></details>
        <details open><summary id="s2">Open section</summary><input id="in2"></details>
        <button id="b">Last</button>`;
    }});
  });
  await page.locator("#a").focus();
  for (const expected of ["s1", "s2", "in2", "b", "a"]) {
    await page.keyboard.press("Tab");
    assert.equal(await page.evaluate(() => document.activeElement.id), expected);
  }
  await page.locator("#b").focus();
  for (const expected of ["in2", "s2", "s1", "a", "b"]) {
    await page.keyboard.press("Shift+Tab");
    assert.equal(await page.evaluate(() => document.activeElement.id), expected);
  }
});
