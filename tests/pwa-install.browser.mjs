/* tests/pwa-install.browser.mjs */
/* CrossWatch - Mobile install availability and native prompt lifecycle */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

const core = await readFile(new URL('../assets/crosswatch.js', import.meta.url), 'utf8');
const pwa = core.slice(core.indexOf('let _cwInstallPrompt'), core.indexOf('function _cwGetByPath'));
const styles = ['fonts/material-symbols-rounded','themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','ui-shell','themes/flat'];

async function fixture(t, {early = false, secure = true, standalone = false, mobile = true} = {}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:mobile?390:1200,height:844},isMobile:mobile,hasTouch:mobile,reducedMotion:'reduce'});
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route('**/pwa-install.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith('/assets/')) return route.fulfill({body:await readFile(new URL('..'+path, import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':'text/javascript'});
    if (path.startsWith('/api/')) return route.fulfill({json:{}});
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-theme="flat-dark" class="cw-theme-dark" data-cw-role="admin"><head><meta name="viewport" content="width=device-width,initial-scale=1">${styles.map(path=>`<link rel="stylesheet" href="/assets/${path}.css">`).join('')}</head><body>
      <header><nav class="tabs"><button id="tab-main" class="tab">Home</button><button id="tab-tracker" class="tab">Tracker</button><button id="tab-playlists" class="tab">Playlists</button></nav></header><main><h1>CrossWatch</h1></main><a href="/profile" data-cw-profile-menu-action="profile">My Media Hub</a>
      <script>
        Object.defineProperty(navigator,'standalone',{configurable:true,value:${standalone}});
        window.emitInstall = (mode = 'normal') => {
          const event = new Event('beforeinstallprompt',{cancelable:true});
          event.userChoice = new Promise(resolve => { window.resolveInstall = resolve; });
          event.prompt = async () => { window.promptCalls = (window.promptCalls || 0) + 1; if(mode === 'error') throw new Error('Install unavailable'); };
          window.dispatchEvent(event);
          return event.defaultPrevented;
        };
        (()=>{const D=document;const _cwIsLikelyHandheld=()=>${mobile};const _cwHasCompactViewport=()=>${mobile};const _cwShouldAutoCompact=()=>false;${pwa}})();
        ${early ? 'window.emitInstall();' : ''}
      </script><script src="/assets/helpers/account-menu.js"></script></body></html>`});
  });
  await page.goto(`${secure?'https':'http'}://pwa-install.test/`);
  await page.locator('#cw-mobile-bottom-nav').waitFor({state:'attached'});
  return page;
}

test('mobile install option handles an early browser event and native acceptance', async t => {
  const page = await fixture(t, {early:true});
  await page.locator('#cw-mobile-more').click();
  const install = page.getByRole('button',{name:'Install CrossWatch'});
  assert.equal(await install.isVisible(), true);
  assert.equal(await page.evaluate(() => cwPwaDiag().hasInstallPrompt), true);
  await mkdir('.tmp/pwa-install',{recursive:true});
  await page.screenshot({path:'.tmp/pwa-install/mobile-menu.png'});
  await install.click();
  assert.equal(await page.evaluate(() => window.promptCalls), 1);
  assert.equal(await install.count(), 0);
  assert.equal(await page.locator('#cw-mobile-more').getAttribute('aria-expanded'), 'false');
  assert.equal(await page.evaluate(() => cwPwaDiag().hasInstallPrompt), false);
  await page.evaluate(() => resolveInstall({outcome:'accepted'}));
  await page.evaluate(() => emitInstall());
  assert.equal(await install.count(), 0);
});

test('mobile install option handles dismissal, fresh prompts and installation outside the menu', async t => {
  const page = await fixture(t);
  const install = page.getByRole('button',{name:'Install CrossWatch'});
  assert.equal(await install.count(), 0);
  assert.equal(await page.evaluate(() => emitInstall()), false);
  await page.locator('#cw-mobile-more').click();
  await install.click();
  await page.evaluate(() => resolveInstall({outcome:'dismissed'}));
  assert.equal(await install.count(), 0);
  await page.evaluate(() => emitInstall('error'));
  await page.locator('#cw-mobile-more').click();
  await install.click();
  assert.equal(await page.evaluate(() => window.promptCalls), 2);
  assert.equal(await install.count(), 0);
  await page.evaluate(() => emitInstall());
  assert.equal(await page.locator('#cw-mobile-install button').count(), 1);
  await page.evaluate(() => window.dispatchEvent(new Event('appinstalled')));
  assert.equal(await install.count(), 0);
  assert.equal(await page.evaluate(() => cwPwaDiag().hasInstallPrompt), false);
  await page.evaluate(() => emitInstall());
  assert.equal(await install.count(), 0);
});

test('native dismissal clears the menu option and ineligible contexts do not advertise installation', async t => {
  const page = await fixture(t, {early:true});
  await page.evaluate(() => resolveInstall({outcome:'dismissed'}));
  assert.equal(await page.locator('#cw-mobile-install').count(), 0);
  for (const options of [{secure:false},{standalone:true},{mobile:false}]) {
    const other = await fixture(t, options);
    assert.equal(await other.evaluate(() => emitInstall()), false);
    assert.equal(await other.locator('#cw-mobile-install').count(), 0);
    assert.equal(await other.evaluate(() => cwPwaDiag().hasInstallPrompt), false);
  }
});
