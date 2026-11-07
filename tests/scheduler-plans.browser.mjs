/* tests/scheduler-plans.browser.mjs */
/* CrossWatch - Independent scheduler plan controls and save round trips */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

async function fixture(t, width, initial) {
  const browser = await chromium.launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport: {width, height: 1000}});
  page.setDefaultTimeout(6000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  let saved = initial || {enabled: false, mode: "daily_time", daily_time: "11:00", advanced: {enabled: false, jobs: []}};
  const configPosts = [];
  const schedulingPosts = [];
  const script = await readFile(new URL("../assets/js/scheduler.js", import.meta.url), "utf8");
  const saveScript = await readFile(new URL("../assets/helpers/settings-save.js", import.meta.url), "utf8");
  await page.route("**/*", async route => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/scheduler.js") return route.fulfill({contentType: "text/javascript", body: script});
    if (path === "/settings-save.js") return route.fulfill({contentType: "text/javascript", body: saveScript});
    if (path === "/api/config") {
      if (route.request().method() === "POST") {
        const cfg = route.request().postDataJSON();
        configPosts.push(cfg.scheduling);
        saved = cfg.scheduling;
        return route.fulfill({json: {ok: true}});
      }
      return route.fulfill({json: {scheduling: saved, app_auth: {enabled: true, username: "operator"}}});
    }
    if (path === "/api/scheduling") {
      if (route.request().method() === "POST") {
        saved = route.request().postDataJSON();
        schedulingPosts.push(saved);
        return route.fulfill({json: {ok: true}});
      }
      return route.fulfill({json: saved});
    }
    if (path === "/api/pairs") return route.fulfill({json: []});
    if (path === "/api/app-auth/status") return route.fulfill({json: {configured: true}});
    if (path.startsWith("/api/")) return route.fulfill({json: {}});
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html><head><meta charset="utf-8"></head><body>
      <input id="app_auth_username" value="operator"><section id="sec-scheduling"><div class="body">
      <div class="cw-subpanel active" data-sub="basic"><div class="auth-card"><div class="grid2">
      <div class="field"><label>Enable</label><select id="schEnabled"><option value="false">Disabled</option><option value="true">Enabled</option></select></div>
      <div class="field"><select id="schMode"><option value="hourly">Hourly</option><option value="daily_time">Daily</option><option value="every_n_hours">Every N hours</option><option value="custom_interval">Custom</option></select></div>
      <div class="field"><input id="schN" value="12"></div>
      <div class="field"><input id="schTime" type="time" value="11:00"></div>
      <div class="field"><input id="schCustomValue" value="60"><select id="schCustomUnit"><option value="minutes">Minutes</option><option value="hours">Hours</option></select></div>
      </div></div></div><div id="sched_advanced_mount"></div><div id="sched_webhooks_mount"></div>
      </div></section><script src="/scheduler.js"></script><script src="/settings-save.js"></script></body></html>`});
  });
  await page.goto("https://scheduler-plans.test/");
  await page.waitForFunction(mode => document.querySelector("#schMode")?.value === mode, saved.mode);
  return {page, saved: () => saved, configPosts, schedulingPosts};
}

async function persist(page) {
  const posted = page.waitForResponse(response => new URL(response.url()).pathname === "/api/scheduling" && response.request().method() === "POST");
  await page.evaluate(() => window.saveSettings());
  await posted;
  const mode = await page.locator("#schMode").inputValue();
  await page.reload();
  await page.waitForFunction(expected => document.querySelector("#schMode")?.value === expected, mode);
}

test("application Save preserves untouched scheduling fields when only Advanced changes", async t => {
  const initial = {
    enabled: true, mode: "custom_interval", custom_interval_minutes: 15, every_n_hours: 2,
    daily_time: "13:45", timezone: "Asia/Tokyo", jitter_seconds: 17,
    profiles: {p: {enabled: true, mode: "hourly"}},
    advanced: {enabled: false, jobs: [], backup_jobs: [{id: "backup", at: "08:00", scope: "app_state"}]}
  };
  const {page, saved, configPosts, schedulingPosts} = await fixture(t, 1400, initial);
  await page.locator("#schAdvEnabled").locator("..").click();
  await persist(page);
  assert.equal(configPosts.length, 1);
  assert.equal(schedulingPosts.length, 1);
  for (const cfg of [configPosts[0], schedulingPosts[0], saved()]) {
    for (const key of ["enabled", "mode", "custom_interval_minutes", "every_n_hours", "daily_time", "timezone", "jitter_seconds", "profiles"]) {
      assert.deepEqual(cfg[key], initial[key], key);
    }
    assert.deepEqual(cfg.advanced.backup_jobs, initial.advanced.backup_jobs);
    assert.equal(cfg.advanced.enabled, true);
  }
});

for (const width of [390, 1400]) {
  for (const first of ["standard", "advanced"]) {
    test(`${width}px: enable ${first} first, save both and disable each separately`, async t => {
      const {page, saved} = await fixture(t, width);
      const standard = page.locator("#schEnabledToggle");
      const advanced = page.locator("#schAdvEnabled");
      const toggle = name => name === "standard" ? standard.locator("..").click() : advanced.locator("..").click();
      await toggle(first);
      await toggle(first === "standard" ? "advanced" : "standard");
      assert.equal(await standard.isChecked(), true);
      assert.equal(await advanced.isChecked(), true);
      assert.equal(await page.locator("#schMode").isEnabled(), true);
      assert.equal(await page.locator("#btnAddStep").isEnabled(), true);
      assert.match(await page.locator("#schStdSummaryState").textContent(), /Standard and advanced/);
      await persist(page);
      assert.equal(saved()._independent_plans_v1, true);
      assert.equal(await standard.isChecked(), true);
      assert.equal(await advanced.isChecked(), true);
      await toggle("standard");
      await persist(page);
      assert.equal(await standard.isChecked(), false);
      assert.equal(await advanced.isChecked(), true);
      await toggle("standard");
      await toggle("advanced");
      await persist(page);
      assert.equal(await standard.isChecked(), true);
      assert.equal(await advanced.isChecked(), false);
    });
  }
}
