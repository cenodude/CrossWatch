/* tests/account-menu.browser.mjs */
/* CrossWatch - Grouped account navigation across shells, permissions and devices */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test, {before, after} from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium, webkit} from 'playwright';

const templates = JSON.parse(execFileSync('python', ['-c', `
import json, ui_frontend as ui
ui._managed_tracker_allowed = lambda user: bool((user or {}).get('tracker'))
users = {'admin': {'is_admin': True, 'username': 'admin'}, 'reader': {'is_admin': False, 'username': 'reader', 'permissions': {'write': False, 'watchlist': False, 'playback': False}}, 'writer': {'is_admin': False, 'username': 'writer', 'tracker': True, 'permissions': {'write': True, 'watchlist': True, 'playback': True}}}
print(json.dumps({name: {'main': ui.get_index_html(include_admin=name == 'admin', user=user), 'profile': ui.get_profile_html(user)} for name, user in users.items()}))
`], {encoding:'utf8', maxBuffer:4*1024*1024}));
let browser;
const assets = new Map();
before(async () => { browser = await ({chromium, webkit})[process.env.CW_MENU_BROWSER || 'chromium'].launch({headless:true}); });
after(async () => { await browser?.close(); });

async function fixture(t, {shell='main', role='admin', theme='flat-dark', width=1440, height=900}={}) {
  const context = await browser.newContext({viewport:{width,height},hasTouch:width<=1024,isMobile:width<=760,reducedMotion:'reduce'});
  t.after(() => context.close());
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  const errors = [], writes = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  const html = await page.evaluate(({source,theme}) => {
    const doc = new DOMParser().parseFromString(source, 'text/html');
    doc.querySelectorAll('script').forEach(node => node.remove());
    doc.getElementById('cw-main-loading')?.remove();
    doc.documentElement.className = theme === 'original' ? 'cw-theme-original' : `cw-theme-${theme === 'flat-light' ? 'light' : 'dark'}`;
    if (theme !== 'original') doc.documentElement.dataset.cwTheme = theme;
    doc.getElementById(theme === 'original' ? 'cw-theme-flat-css' : 'cw-theme-original-css')?.remove();
    const script = doc.createElement('script');
    script.src = '/assets/helpers/account-menu.js';
    doc.body.append(script);
    return '<!doctype html>' + doc.documentElement.outerHTML;
  }, {source:templates[role][shell], theme});
  await page.route('https://account-menu.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith('/assets/')) {
      try {
        if (!assets.has(path)) assets.set(path, await readFile(new URL('..'+path, import.meta.url)));
        return route.fulfill({body:assets.get(path),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});
      } catch { return route.fulfill({status:404,body:''}); }
    }
    if (path.startsWith('/api/')) {
      if (route.request().method() === 'POST') writes.push(path);
      return route.fulfill({json:{ok:true}});
    }
    return route.fulfill({contentType:'text/html',body:html});
  });
  await page.goto(`https://account-menu.test/${shell === 'profile' ? 'profile' : ''}`);
  await page.evaluate(() => document.fonts.ready);
  return {page,writes};
}

for (const theme of ['original','flat-dark','flat-light']) {
  for (const shell of ['main','profile']) {
    for (const [width,height] of [[320,740],[390,844],[740,360],[768,1024],[1024,768],[1440,900]]) {
      test(`${theme}: ${shell} account menu at ${width}x${height}`, async t => {
        const {page} = await fixture(t, {theme,shell,width,height});
        const toggle = page.locator('#cw-nav-profile-link');
        await toggle.click();
        const menu = page.locator('#cw-profile-menu');
        assert.equal(await toggle.getAttribute('aria-expanded'), 'true');
        const bounds = await menu.boundingBox();
        assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= width + 1, JSON.stringify(bounds));
        assert.ok(bounds.y >= 0 && bounds.y + bounds.height <= height, JSON.stringify(bounds));
        const bottomNav = await page.locator('#cw-mobile-bottom-nav').boundingBox();
        if (bottomNav?.height) assert.ok(bounds.y + bounds.height <= bottomNav.y);
        assert.equal(await menu.evaluate(node => node.scrollWidth > node.clientWidth + 1), false);
        const dangerStyle = await menu.locator('[data-cw-profile-menu-action="logout"]').evaluate(node => {
          const style = getComputedStyle(node);
          return {background:style.backgroundColor,color:style.color,fill:style.webkitTextFillColor};
        });
        assert.equal(dangerStyle.background, 'rgba(0, 0, 0, 0)');
        assert.equal(dangerStyle.fill, dangerStyle.color);
        for (const item of await menu.getByRole('menuitem').all()) assert.ok((await item.boundingBox()).height >= 44);
        const expectedCurrent = shell === 'profile' ? 'profile' : null;
        assert.equal(await menu.locator('[aria-current]').count(), expectedCurrent ? 1 : 0);
        if (expectedCurrent) assert.equal(await menu.locator('[aria-current]').getAttribute('data-cw-profile-menu-action'), expectedCurrent);
        if (process.env.CW_MENU_SCREENSHOTS) {
          await mkdir(process.env.CW_MENU_SCREENSHOTS, {recursive:true});
          await menu.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-${shell}-${width}.png`});
          if (theme === 'flat-dark' && [390,768,1440].includes(width)) await page.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-${shell}-${width}-screen.png`});
        }
        const media = menu.locator('[aria-controls="cw-account-media"]');
        await media.click();
        assert.equal(await page.locator('#cw-account-media').isVisible(), false);
        await page.keyboard.press('ArrowDown');
        assert.equal(await page.evaluate(() => document.activeElement.getAttribute('aria-controls')), 'cw-account-tracker');
        await page.keyboard.press('Enter');
        assert.equal(await page.locator('#cw-account-tracker').isVisible(), false);
        await page.keyboard.press('Home');
        await page.keyboard.press('Enter');
        assert.equal(await page.locator('#cw-account-media').isVisible(), true);
        await page.keyboard.press('End');
        assert.equal(await page.evaluate(() => document.activeElement.dataset.cwProfileMenuAction), 'logout');
        const logout = await menu.locator('[data-cw-profile-menu-action="logout"]').boundingBox();
        assert.ok(logout.y >= bounds.y && logout.y + logout.height <= height);
        await page.keyboard.press('Escape');
        assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
        assert.equal(await toggle.evaluate(node => document.activeElement === node), true);
        await page.keyboard.press('ArrowUp');
        assert.equal(await page.evaluate(() => document.activeElement.dataset.cwProfileMenuAction), 'logout');
        await page.keyboard.press('Tab');
        assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
      });
    }
  }
}

for (const shell of ['main','profile']) {
  test(`${shell}: existing media destinations, profile settings, tracker and logout`, async t => {
    const {page,writes} = await fixture(t, {shell});
    const start = `https://account-menu.test/${shell === 'profile' ? 'profile' : ''}`;
    for (const [action,path] of Object.entries({profile:'/profile',watchlist:'/profile#watchlist',history:'/profile#history',ratings:'/profile#ratings',collections:'/profile#collection',playback:'/profile#playback',account:'/profile#account',tracker:'/#tracker?view=overview',logout:'/login'})) {
      await page.goto(start);
      await page.locator('#cw-nav-profile-link').click();
      await page.locator(`[data-cw-profile-menu-action="${action}"]`).click();
      await page.waitForURL(`https://account-menu.test${path}`);
    }
    assert.deepEqual(writes, ['/api/app-auth/logout']);
  });
  for (const role of ['reader','writer']) {
    test(`${shell}: ${role} menu permissions`, async t => {
      const {page} = await fixture(t, {shell,role,width:390,height:844});
      await page.locator('#cw-nav-profile-link').click();
      for (const action of ['watchlist','playback','tracker']) assert.equal(await page.locator(`[data-cw-profile-menu-action="${action}"]`).count(), role === 'writer' ? 1 : 0);
      for (const action of ['profile','history','ratings','collections','account','logout']) assert.equal(await page.locator(`[data-cw-profile-menu-action="${action}"]`).count(), 1);
      if (role === 'writer') {
        await page.locator('[data-cw-profile-menu-action="tracker"]').click();
        await page.waitForURL('https://account-menu.test/?main=1#tracker?view=overview');
      }
    });
  }
}
