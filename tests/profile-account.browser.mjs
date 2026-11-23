/* tests/profile-account.browser.mjs */
/* CrossWatch - Dedicated account settings and Hub routing */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test, {before, after} from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {execFileSync} from "node:child_process";
import {chromium} from "playwright";

const templates = JSON.parse(execFileSync("python", ["-c", "import json, ui_frontend as ui; u={'is_admin':True,'username':'admin','display_name':'Pascal van den Berg'}; print(json.dumps({'profile':ui.get_profile_html(u),'hub':ui.get_hub_html(u)}))"], {encoding:"utf8", maxBuffer:1024*1024}));
let browser;
before(async () => { browser = await chromium.launch(); });
after(async () => { await browser?.close(); });

async function fixture(t, theme = "flat-dark", width = 1440) {
  const context = await browser.newContext({viewport:{width, height:1000}, reducedMotion:"reduce"});
  t.after(() => context.close());
  const page = await context.newPage();
  page.setDefaultTimeout(8000);
  await page.addInitScript(theme => localStorage.setItem("cw.ui.theme", theme), theme);
  const requests = [], errors = [];
  const user = {is_admin:true, username:"admin", display_name:"Pascal van den Berg", preferences:{timezone:"auto", time_format:"auto", media_card:"compact"}, other_sessions:[]};
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route("https://account.test/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith("/assets/")) {
      try { return route.fulfill({body:await readFile(new URL(".." + url.pathname, import.meta.url)), contentType:url.pathname.endsWith(".css") ? "text/css" : url.pathname.endsWith(".woff2") ? "font/woff2" : url.pathname.endsWith(".svg") ? "image/svg+xml" : "text/javascript"}); }
      catch { return route.fulfill({status:404, body:""}); }
    }
    if (url.pathname.startsWith("/api/")) {
      requests.push(url.pathname);
      if (url.pathname === "/api/profile" && route.request().method() === "POST") Object.assign(user, route.request().postDataJSON());
      return route.fulfill({json:{ok:true, authenticated:true, user, configured:false, items:[], notifications:[], permissions:{write:true}}});
    }
    return route.fulfill({contentType:"text/html", body:templates[url.pathname === "/hub" ? "hub" : "profile"]});
  });
  return {page, requests, user};
}

for (const theme of ["original", "flat-dark", "flat-light"]) for (const width of [390, 1440]) {
  test(`${theme} ${width}: account settings stay independent of Hub`, async t => {
    const {page, requests, user} = await fixture(t, theme, width);
    await page.goto("https://account.test/profile#account");
    await page.locator('#profile-display-input').waitFor();
    await page.waitForFunction(() => document.querySelector('#profile-display-input').value === 'Pascal van den Berg');
    await page.evaluate(() => document.fonts.ready);
    assert.equal(await page.locator('#profile-hero, .cw-profile-tabs, #profile-panel-overview').count(), 0);
    assert.equal(await page.locator('#tab-profile').getAttribute('aria-current'), null);
    assert.equal(await page.locator('#settings-datetime').isVisible(), true);
    await mkdir('.tmp/profile-account', {recursive:true});
    await page.screenshot({path:`.tmp/profile-account/${theme}-${width}.png`, fullPage:true});
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    await page.locator('#profile-display-input').fill('Pascal');
    await page.locator('#profile-name-form button').click();
    await page.waitForFunction(() => document.querySelector('#profile-display-name').textContent === 'Pascal');
    assert.equal(user.display_name, 'Pascal');
    await page.locator('#profile-pref-timezone').focus();
    await page.locator('#profile-pref-timezone').selectOption('UTC');
    await page.locator('#profile-pref-timezone').blur();
    await page.waitForFunction(() => document.querySelector('#profile-pref-timezone').disabled === false);
    assert.equal(user.preferences.timezone, 'UTC');
    await page.locator('[data-settings-nav="security"]').click();
    await page.locator('#profile-password-toggle').click();
    assert.equal(await page.locator('#profile-password-form').isVisible(), true);
    await page.locator('#profile-password-cancel').click();
    await page.locator('[data-settings-nav="linked"]').click();
    assert.equal(await page.locator('#profile-plex-link').isEnabled(), true);
    assert.equal(await page.locator('#profile-oidc-link').isDisabled(), true);
    await page.locator('[data-settings-nav="sessions"]').click();
    await page.getByText('No active sessions.', {exact:true}).waitFor();
    await page.locator('[data-settings-nav="interface"]').click();
    assert.equal(await page.locator('#profile-pref-timezone').isVisible(), false);
    assert.equal(await page.locator('#profile-media-card').inputValue(), 'compact');
    await page.goBack();
    assert.equal(await page.locator('#settings-sessions').isVisible(), true);
    await page.goForward();
    assert.equal(await page.locator('#settings-interface').isVisible(), true);
    assert.equal(requests.some(path => /collection|history|ratings|watchlist|airing|widgets|currently_watching|insights|playback_progress/.test(path)), false, requests.join('\n'));
  });
}

test('legacy media links preserve queries and open Hub', async t => {
  const {page} = await fixture(t);
  for (const [hash, expected] of [['collection', 'collection'], ['history/scrobble?source=all', 'history/scrobble?source=all'], ['playback_progress', 'playback']]) {
    await page.goto(`https://account.test/profile?as=someone#${hash}`);
    await page.waitForURL(`https://account.test/hub?as=someone#${expected}`);
    assert.equal(await page.locator('#profile-panel-account, #profile-tab-account').count(), 0);
  }
});

test('Hub navigation and account avatar open the dedicated page', async t => {
  const {page, user} = await fixture(t);
  user.avatar_url = '/assets/img/CROSSWATCH.svg';
  await page.goto('https://account.test/hub#collection');
  await page.locator('[data-profile-tab="ratings"]').click();
  await page.waitForURL('**/hub#ratings');
  await page.goBack();
  await page.locator('#profile-panel-collection.active').waitFor();
  await page.locator('#profile-hero-avatar').click();
  await page.waitForURL('**/profile');
  await page.locator('#profile-settings-avatar img').waitFor();
  const picker = page.waitForEvent('filechooser');
  await page.locator('#profile-avatar-replace').click();
  assert.equal(await (await picker).element().getAttribute('id'), 'profile-avatar-input');
});
