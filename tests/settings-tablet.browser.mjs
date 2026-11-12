/* tests/settings-tablet.browser.mjs */
/* CrossWatch - Settings card layouts on tablets and narrow screens */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

async function open(t, theme) {
  const browser = await (process.env.CW_SETTINGS_BROWSER === "webkit" ? webkit : chromium).launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport: {width: 1194, height: 834}, hasTouch: true});
  const source = await readFile(new URL("../ui_frontend.py", import.meta.url), "utf8");
  const markup = await page.evaluate(source => {
    const doc = new DOMParser().parseFromString(source.slice(source.indexOf('<!doctype html>')), "text/html");
    const settings = doc.querySelector("#page-settings");
    settings.classList.remove("hidden");
    settings.querySelectorAll("script").forEach(script => script.remove());
    settings.querySelectorAll("*").forEach(el => [...el.attributes].filter(attr => attr.name.startsWith("on")).forEach(attr => el.removeAttribute(attr.name)));
    return {settings: settings.outerHTML, styles: [...doc.querySelectorAll('link[rel="stylesheet"]')].map(el => el.getAttribute("href").split("?")[0])};
  }, source);
  await page.route("https://settings.test/**", async route => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/api/app-auth/status") return route.fulfill({json: {enabled: true, configured: true, is_admin: true}});
    if (path === "/api/user-profiles") return route.fulfill({json: {items: [{id: "viewer", label: "Household"}]}});
    if (path === "/api/app-auth/users") return route.fulfill({json: {items: []}});
    if (path.startsWith("/assets/")) {
      try { return route.fulfill({body: await readFile(new URL(".." + path, import.meta.url)), contentType: path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : path.endsWith(".js") ? "text/javascript" : "image/svg+xml"}); }
      catch { return route.fulfill({status: 404, body: ""}); }
    }
    const attrs = theme === "original" ? 'class="cw-theme-original"' : `data-cw-theme="${theme}" class="cw-theme-${theme === "flat-light" ? "light" : "dark"}"`;
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html ${attrs} data-tab="settings"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${markup.styles.map(path => `<link rel="stylesheet" href="${path}">`).join("")}</head><body><main id="layout" class="full">${markup.settings}</main><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/settings-ui.js"></script><script src="/assets/js/app-users.js"></script></body></html>`});
  });
  await page.goto("https://settings.test/");
  await page.evaluate(() => document.fonts.ready);
  return page;
}

async function select(page, pane, tab = "ui") {
  await page.evaluate(({pane, tab}) => {
    document.querySelectorAll(".cw-settings-pane").forEach(el => el.classList.toggle("active", el.dataset.pane === pane));
    window.cwUiSettingsSelect(tab, {persist: false});
    document.querySelector("#app_auth_totp_setup").classList.remove("hidden");
    document.querySelector("#app_auth_totp_secret").value = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP";
    document.querySelector("#btn-app-auth-totp-verify").classList.remove("hidden");
    document.querySelector("#btn-app-auth-totp-disable").disabled = true;
    document.querySelector("#app_auth_oidc_provision_fields").hidden = false;
    document.querySelector("#app_auth_other_sessions_detail").textContent = "Safari on iPad · Chrome on desktop";
  }, {pane, tab});
}

async function checkCards(page) {
  const issues = await page.locator(".cw-app-panel.active .cw-app-card").evaluateAll(cards => cards.flatMap(card => {
    const issues = [];
    for (const el of card.querySelectorAll("input, select, textarea, button, .sub, .cw-settings-status")) {
      if (!el.checkVisibility() || el.closest("[hidden],.hidden")) continue;
      const parent = el.closest(".cw-settings-2col>div, .cw-auth-session-row>div") || card;
      const r = el.getBoundingClientRect(), p = parent.getBoundingClientRect(), c = card.getBoundingClientRect();
      if (r.width && (r.left < Math.max(p.left, c.left) - 1 || r.right > Math.min(p.right, c.right) + 1)) issues.push(`${el.id || el.className}: ${Math.round(r.width)}px outside ${Math.round(p.width)}px field`);
    }
    return issues;
  }));
  assert.deepEqual(issues, []);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
}

for (const theme of ["original", "flat-dark", "flat-light"]) {
  test(`${theme}: Setup, Connections, Synchronization and Maintenance settings fit across devices`, async t => {
    const page = await open(t, theme);
    const menu = page.locator(".cw-settings-nav-btn");
    const checkMenuBorders = async () => assert.deepEqual(await menu.evaluateAll(buttons => buttons.filter(el => {
      const style = getComputedStyle(el);
      return [style.borderTopWidth, style.borderRightWidth, style.borderBottomWidth, style.borderLeftWidth].some(width => width !== "0px");
    }).map(el => el.textContent.trim())), []);
    await checkMenuBorders();
    await menu.nth(1).hover();
    await checkMenuBorders();
    await menu.nth(1).focus();
    await checkMenuBorders();
    for (const [width, height] of [[320,740],[390,844],[740,360],[844,390],[932,430],[768,1024],[1024,768],[1194,834],[1366,1024],[1920,1080]]) {
      await page.setViewportSize({width, height});
      for (const pane of ["overview", "providers", "sync", "maintenance"]) {
        await select(page, pane);
        const size = await page.evaluate(() => ({viewport: innerWidth, document: document.documentElement.scrollWidth}));
        assert.ok(size.document <= size.viewport + 1, JSON.stringify({pane, ...size}));
      }
    }
  });
  test(`${theme}: UI and Security adapt to iPad landscape, portrait, phone and desktop`, async t => {
    const page = await open(t, theme);
    await select(page, "app", "ui");
    const switches = page.getByRole("switch");
    assert.equal(await switches.count(), 10);
    assert.equal(await page.locator('.cw-app-ui-layout input:checked').count(), 8);
    const watchlist = page.getByRole("switch", {name: "Watchlist widget", exact: true});
    await page.evaluate(() => {
      window.switchChanges = 0;
      document.getElementById("ui_show_watchlist_preview").addEventListener("change", () => window.switchChanges++);
    });
    await page.locator('label[for="ui_show_watchlist_preview"]').click();
    assert.equal(await watchlist.isChecked(), false);
    await watchlist.focus();
    await page.keyboard.press("Space");
    assert.equal(await watchlist.isChecked(), true);
    assert.equal(await page.evaluate(() => window.switchChanges), 2);
    await watchlist.evaluate(el => el.disabled = true);
    await page.keyboard.press("Space");
    assert.equal(await watchlist.isChecked(), true);
    assert.equal(await page.evaluate(() => window.switchChanges), 2);
    await watchlist.evaluate(el => el.disabled = false);
    const header = await page.locator(".cw-app-hero-copy").innerText();
    await page.evaluate(() => window.cwAppUsersRefresh());
    assert.match(await page.locator("#app_users_list").innerText(), /Nobody else can sign in/);
    await page.route("https://settings.test/api/app-auth/users", route => route.fulfill({json: {items: [
      {id: "administrator", username: "admin", protected: true},
      {id: "viewer", username: "Household viewer", profile_id: "viewer", enabled: false, permissions: {write: false}}
    ]}}));
    await page.evaluate(() => window.cwAppUsersRefresh());
    assert.equal(await page.locator(".cw-app-user-row").count(), 2);
    await select(page, "app", "security");
    assert.equal(await page.locator(".cw-app-hero-copy").innerText(), header);
    for (const id of ["app_auth_password_details", "app_auth_oidc_details", "app_auth_network_details", "ai_section"]) {
      assert.equal(await page.locator(`#${id}`).getAttribute("open"), null, id);
    }
    assert.equal(await page.locator("#app_auth_oidc_issuer").isVisible(), false);
    assert.equal(await page.locator("#assistant_model").isVisible(), false);
    for (const [action, inactive] of [["btn-app-auth-plex-unlink", "btn-app-auth-plex-link"], ["btn-app-auth-totp-disable", "btn-app-auth-totp-setup"]]) {
      assert.equal(await page.locator(`#${action}`).isVisible(), false);
      await page.locator(`#${action}`).evaluate(el => el.disabled = false);
      assert.equal(await page.locator(`#${action}`).isVisible(), true);
      assert.equal(await page.locator(`#${inactive}`).isVisible(), false);
      await page.locator(`#${action}`).evaluate(el => el.disabled = true);
    }
    if (process.env.CW_SETTINGS_SCREENSHOTS) {
      await mkdir(process.env.CW_SETTINGS_SCREENSHOTS, {recursive: true});
      await page.evaluate(() => {
        document.getElementById("app_auth_totp_setup").classList.add("hidden");
        document.getElementById("btn-app-auth-totp-verify").classList.add("hidden");
        document.getElementById("app_auth_username").value = "admin";
        document.getElementById("app_auth_state").textContent = "Signed in - This browser";
      });
      for (const width of [390, 1920]) {
        await page.setViewportSize({width, height: 1080});
        await checkCards(page);
        await page.locator(".cw-app-settings-pane").screenshot({path: `${process.env.CW_SETTINGS_SCREENSHOTS}/${theme}-${width}-security-default.png`});
      }
    }
    for (const id of ["app_auth_password_details", "app_auth_network_details", "ai_section"]) {
      await page.locator(`#${id}>summary`).focus();
      await page.keyboard.press("Enter");
      assert.notEqual(await page.locator(`#${id}`).getAttribute("open"), null);
    }
    await page.locator("#app_auth_password").fill("example-password");
    await page.locator("#app_auth_password_details>summary").click();
    await page.locator("#app_auth_password_details>summary").click();
    assert.equal(await page.locator("#app_auth_password").inputValue(), "example-password");
    await page.locator("#assistant_model").fill("example-model");
    await page.locator("#ai_section>summary").click();
    await page.locator("#ai_section>summary").click();
    assert.equal(await page.locator("#assistant_model").inputValue(), "example-model");
    const disclosure = page.locator("#app_auth_oidc_details");
    assert.equal(await disclosure.getAttribute("open"), null);
    await checkCards(page);
    await disclosure.locator("summary").focus();
    await page.keyboard.press("Enter");
    assert.notEqual(await disclosure.getAttribute("open"), null);
    assert.deepEqual(await disclosure.locator("input, select").evaluateAll(fields => fields.filter(el => !el.labels?.length).map(el => el.id)), []);
    await page.locator("#app_auth_oidc_issuer").fill("https://accounts.example.com");
    await disclosure.locator("summary").click();
    await disclosure.locator("summary").click();
    assert.equal(await page.locator("#app_auth_oidc_issuer").inputValue(), "https://accounts.example.com");
    for (const [width, height] of [[740, 360], [844, 390], [932, 430], [1194, 834], [1366, 1024], [1180, 820], [1024, 768], [820, 1180], [390, 844], [320, 740], [1920, 1080]]) {
      await page.setViewportSize({width, height});
      for (const tab of ["ui", "security"]) {
        await select(page, "app", tab);
        await checkCards(page);
        if (process.env.CW_SETTINGS_SCREENSHOTS) {
          await mkdir(process.env.CW_SETTINGS_SCREENSHOTS, {recursive: true});
          const target = page.locator(".cw-app-settings-pane");
          await target.screenshot({path: `${process.env.CW_SETTINGS_SCREENSHOTS}/${theme}-${width}-${tab}.png`});
        }
      }
    }
  });
}
