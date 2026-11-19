/* tests/notification-startup.browser.mjs */
/* CrossWatch - Notification startup, refresh and cancellation browser regressions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";

const endpoints = ["/api/version", "/api/health/database", "/api/maintenance/jobs/current",
  "/api/tracker/check/current", "/api/import/plex-recovery/active", "/api/interactive-sync"];
const read = path => readFile(new URL(`../${path}`, import.meta.url), "utf8");
const crosswatch = await read("assets/crosswatch.js");
const bootstrap = crosswatch.slice(crosswatch.indexOf("async function _cwFetchBootstrapAuthState()"), crosswatch.indexOf("  // Bootstrap"));
assert.ok(bootstrap.includes("window.__cwAuthBootstrapPromise ||="));
const main = await read("assets/js/main.js");
const mainAuth = main.slice(main.indexOf("  const refreshAuthAccess = async () => {"), main.indexOf("  const refreshSoon = () => syncVisibility();"));
assert.ok(mainAuth.includes("window.CW?.AuthState?.apply?.(status)"));

async function fixture(t, {standalone = false, bootstrapDelay = 50, authDelay = 20, managed = false} = {}) {
  const browser = await chromium.launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  page.setDefaultTimeout(6000);
  const errors = [], failed = [], requests = [], pending = [];
  const user = {id: "alice", is_admin: !managed, profile_id: managed ? "home" : "",
    display_name: "Alice", permissions: {write: true}, preferences: {theme: "dark"}};
  let hold = true;
  const expected = endpoints.filter(path => !managed || path !== "/api/maintenance/jobs/current");
  page.on("pageerror", error => errors.push(error.message));
  page.on("requestfailed", request => { if (endpoints.includes(new URL(request.url()).pathname)) failed.push(request.url()); });
  await page.addInitScript(paths => {
    window.notificationRequests = [];
    const original = window.fetch;
    window.fetch = async (resource, options) => {
      const path = new URL(resource, location.href).pathname;
      if (!options?.signal || !paths.includes(path)) return original(resource, options);
      const entry = {path, aborted: false, done: false};
      window.notificationRequests.push(entry);
      options.signal.addEventListener("abort", () => { if (!entry.done) entry.aborted = true; });
      try { return await original(resource, options); }
      finally { entry.done = true; }
    };
  }, endpoints);
  await page.route("https://notification-startup.test/**", async route => {
    const url = new URL(route.request().url()), path = url.pathname;
    if (path === "/bootstrap.js") return route.fulfill({contentType: "text/javascript", body: bootstrap});
    if (path === "/main-auth.js") return route.fulfill({contentType: "text/javascript",
      body: `let authWriteAllowed; const syncVisibility = () => {}; ${mainAuth} window.mainAuthReady = refreshAuthAccess();`});
    if (path.startsWith("/assets/")) {
      if (path.endsWith(".css")) return route.fulfill({contentType: "text/css", body: ""});
      return route.fulfill({contentType: "text/javascript", body: await read(path.slice(1))});
    }
    if (path.startsWith("/api/")) {
      requests.push(path);
      if (endpoints.includes(path)) {
        const reply = () => route.fulfill({json: {ok: true, sessions: [], job: null}});
        if (hold) { pending.push(reply); return; }
        return reply();
      }
      if (path === "/api/app-auth/status") {
        await new Promise(resolve => setTimeout(resolve, url.search ? bootstrapDelay : authDelay));
        return route.fulfill({json: {authenticated: true, configured: true, user}});
      }
      if (path === "/api/profile") return route.fulfill({json: {user}});
      if (path === "/api/user-profiles") return route.fulfill({json: {items: [{id: "home", label: "Home"}]}});
      return route.fulfill({json: {ok: true}});
    }
    const scripts = ["/assets/helpers/auth-state.js", "/assets/helpers/notifications.js",
      "/assets/helpers/update-notifications.js", "/assets/helpers/database-notifications.js",
      "/assets/helpers/api.js", ...(!standalone ? ["/bootstrap.js"] : []),
      "/assets/js/overview-profile.js", "/assets/js/sync-reviews.js",
      "/assets/js/plex-recovery-notifications.js", "/assets/js/tracker-notifications.js",
      ...(!standalone ? ["/main-auth.js"] : [])];
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html data-cw-role="${managed ? "user" : "admin"}" data-cw-perm-tracker="on" data-tab="main"><head>
      ${scripts.map(src => `<script src="${src}" defer></script>`).join("")}
      </head><body><div id="cw-nav-profile-menu"></div></body></html>`});
  });
  await page.goto("https://notification-startup.test/", {waitUntil: "domcontentloaded"});
  await page.evaluate(() => window.CW.OverviewProfile.ready);
  await page.evaluate(() => window.mainAuthReady);
  await page.waitForFunction(expected => expected.every(path => window.notificationRequests.some(request => request.path === path)), expected);
  return {page, errors, failed, requests, expected, user,
    async release() {
      hold = false;
      await Promise.all(pending.splice(0).map(reply => reply()));
      await page.waitForFunction(() => window.notificationRequests.every(request => request.done));
    },
  };
}

for (const options of [
  {bootstrapDelay: 100, authDelay: 10},
  {bootstrapDelay: 10, authDelay: 100},
  {standalone: true},
  {managed: true},
]) {
  test(`startup and overlapping refreshes keep one request per notification: ${JSON.stringify(options)}`, async t => {
    const app = await fixture(t, options);
    await app.page.evaluate(() => {
      for (let i = 0; i < 3; i++) {
        window.dispatchEvent(new CustomEvent("cw:notifications-refresh"));
        document.dispatchEvent(new CustomEvent("tab-changed"));
        document.dispatchEvent(new CustomEvent("cw-settings-pane-changed"));
        document.dispatchEvent(new Event("visibilitychange"));
      }
      window.CW.AuthState.apply(structuredClone(window.CW.AuthState.read().status));
    });
    await app.release();
    const entries = await app.page.evaluate(() => window.notificationRequests);
    assert.deepEqual(entries.map(entry => entry.path).sort(), [...app.expected].sort());
    assert.ok(entries.every(entry => !entry.aborted));
    assert.deepEqual(app.failed, []);
    assert.deepEqual(app.errors, []);
    await app.page.evaluate(() => window.dispatchEvent(new CustomEvent("cw:notifications-refresh")));
    await app.page.waitForFunction(expected => expected.every(path => window.notificationRequests.filter(request => request.path === path && request.done).length === 2), app.expected.filter(path => path !== "/api/version"));
    assert.deepEqual(app.errors, []);
  });
}

test("real account changes cancel old requests and start new ones", async t => {
  const app = await fixture(t);
  await app.page.evaluate(() => {
    const next = structuredClone(window.CW.AuthState.read().status);
    next.user.id = "bob";
    window.CW.AuthState.apply(next);
  });
  await app.page.waitForFunction(paths => paths.every(path => window.notificationRequests.filter(request => request.path === path).length === 2), app.expected);
  const entries = await app.page.evaluate(() => window.notificationRequests);
  for (const path of app.expected) {
    const matching = entries.filter(entry => entry.path === path);
    assert.equal(matching[0].aborted, true);
    assert.equal(matching[1].aborted, false);
  }
  await app.release();
  assert.deepEqual(app.errors, []);
});

test("job changes replace pending job status requests", async t => {
  const app = await fixture(t);
  await app.page.evaluate(() => {
    for (const type of ["cw:maintenance-job-changed", "cw:tracker-check-changed", "cw:plex-recovery-changed"]) {
      window.dispatchEvent(new CustomEvent(type));
    }
  });
  const changed = ["/api/maintenance/jobs/current", "/api/tracker/check/current", "/api/import/plex-recovery/active"];
  await app.page.waitForFunction(paths => paths.every(path => window.notificationRequests.filter(request => request.path === path).length === 2), changed);
  const entries = await app.page.evaluate(() => window.notificationRequests);
  assert.deepEqual(entries.filter(entry => entry.aborted).map(entry => entry.path).sort(), changed.sort());
  await app.release();
  assert.deepEqual(app.errors, []);
});

test("profile switching replaces scoped notification requests", async t => {
  const app = await fixture(t);
  await app.page.evaluate(() => window.CW.OverviewProfile.setActive("home"));
  const changed = ["/api/import/plex-recovery/active", "/api/interactive-sync"];
  await app.page.waitForFunction(paths => paths.every(path => window.notificationRequests.filter(request => request.path === path).length === 2), changed);
  const entries = await app.page.evaluate(() => window.notificationRequests);
  assert.deepEqual(entries.filter(entry => entry.aborted).map(entry => entry.path).sort(), changed.sort());
  await app.release();
  assert.deepEqual(app.errors, []);
});

test("hidden tabs cancel polling and resume when visible", async t => {
  const app = await fixture(t);
  const polled = app.expected.filter(path => path !== "/api/version");
  await app.page.evaluate(() => {
    Object.defineProperty(document, "hidden", {configurable: true, value: true});
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await app.page.waitForFunction(paths => paths.every(path => window.notificationRequests.some(request => request.path === path && request.aborted)), polled);
  await app.page.evaluate(() => {
    Object.defineProperty(document, "hidden", {configurable: true, value: false});
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await app.page.waitForFunction(paths => paths.every(path => window.notificationRequests.filter(request => request.path === path).length === 2), polled);
  await app.release();
  const entries = await app.page.evaluate(() => window.notificationRequests);
  for (const path of polled) assert.equal(entries.filter(entry => entry.path === path).at(-1).aborted, false);
  assert.deepEqual(app.errors, []);
});
