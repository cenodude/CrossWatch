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
    if (path.startsWith("/assets/")) {
      try { return route.fulfill({body: await readFile(new URL(".." + path, import.meta.url)), contentType: path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : path.endsWith(".js") ? "text/javascript" : "image/svg+xml"}); }
      catch { return route.fulfill({status: 404, body: ""}); }
    }
    const attrs = theme === "original" ? 'class="cw-theme-original"' : `data-cw-theme="${theme}" class="cw-theme-${theme === "flat-light" ? "light" : "dark"}"`;
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html ${attrs} data-tab="settings"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${markup.styles.map(path => `<link rel="stylesheet" href="${path}">`).join("")}</head><body><main id="layout" class="full">${markup.settings}</main><script src="/assets/helpers/icon-select.js"></script></body></html>`});
  });
  await page.goto("https://settings.test/");
  await page.evaluate(() => document.fonts.ready);
  return page;
}

async function select(page, pane, tab = "ui") {
  await page.evaluate(({pane, tab}) => {
    document.querySelectorAll(".cw-settings-pane").forEach(el => el.classList.toggle("active", el.dataset.pane === pane));
    document.querySelectorAll("#ui_settings_panels>.cw-settings-panel").forEach(el => el.classList.toggle("active", el.dataset.tab === tab));
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
    for (const [width, height] of [[740, 360], [844, 390], [932, 430], [1194, 834], [1366, 1024], [1180, 820], [1024, 768], [820, 1180], [390, 844], [320, 740], [1920, 1080]]) {
      await page.setViewportSize({width, height});
      for (const tab of ["ui", "security"]) {
        await select(page, "app", tab);
        await checkCards(page);
        if (process.env.CW_SETTINGS_SCREENSHOTS) {
          await mkdir(process.env.CW_SETTINGS_SCREENSHOTS, {recursive: true});
          const target = tab === "security" ? page.locator("#app_auth_fields") : page.locator(".cw-app-panel.active");
          await target.screenshot({path: `${process.env.CW_SETTINGS_SCREENSHOTS}/${theme}-${width}-${tab}.png`});
        }
      }
    }
  });
}
