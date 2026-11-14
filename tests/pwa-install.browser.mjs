/* tests/pwa-install.browser.mjs */
/* CrossWatch - Mobile install availability and native prompt lifecycle */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

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
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-theme="flat-dark" class="cw-theme-dark" data-cw-role="admin"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${styles.map(path=>`<link rel="stylesheet" href="/assets/${path}.css">`).join('')}</head><body>
      <header><nav class="tabs"><button id="tab-main" class="tab">Home</button><button id="tab-tracker" class="tab">Tracker</button><button id="tab-playlists" class="tab">Playlists</button><a id="tab-profile" class="tab" href="/profile#overview">My Media Hub</a></nav></header><main><h1>CrossWatch</h1></main>
      <script>
        Object.defineProperty(navigator,'standalone',{configurable:true,value:${standalone}});
        window.emitInstall = (mode = 'normal') => {
          const event = new Event('beforeinstallprompt',{cancelable:true});
          event.userChoice = new Promise(resolve => { window.resolveInstall = resolve; });
          event.prompt = async () => { window.promptCalls = (window.promptCalls || 0) + 1; if(mode === 'error') throw new Error('Install unavailable'); };
          window.dispatchEvent(event);
          return event.defaultPrevented;
        };
        Object.defineProperty(navigator,'serviceWorker',{configurable:true,value:{register:async (...args) => {window.swRegistration = args;}}});
      </script><script src="/assets/helpers/pwa-install.js"></script><script>
        ${early ? 'window.emitInstall();' : ''}
      </script><script src="/assets/helpers/account-menu.js"></script></body></html>`});
  });
  await page.goto(`${secure?'https':'http'}://pwa-install.test/`);
  await page.waitForFunction(() => typeof window.cwPwaDiag === 'function');
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
  assert.equal(await page.evaluate(() => emitInstall()), true);
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


test('native install banner opens the browser prompt independently of the More menu', async t => {
  const page = await fixture(t, {early:true});
  const banner = page.locator('#cw-install');
  await banner.waitFor({state:'visible'});
  assert.deepEqual(await page.evaluate(() => window.swRegistration), ['/sw.js',{scope:'/'}]);
  assert.equal(await page.locator('#cw-mobile-more').getAttribute('aria-expanded'), 'false');
  await mkdir('.tmp/pwa-install',{recursive:true});
  await page.screenshot({path:'.tmp/pwa-install/install-banner.png'});
  await page.locator('#cw-install-primary').click();
  assert.equal(await page.evaluate(() => window.promptCalls), 1);
  assert.equal(await banner.isVisible(), false);
  assert.equal(await page.getByRole('dialog').count(), 0);
  await page.evaluate(() => resolveInstall({outcome:'accepted'}));
  await page.evaluate(() => emitInstall());
  assert.equal(await banner.isVisible(), false);
});

test('dismissing the banner respects cooldown while keeping manual installation available', async t => {
  const page = await fixture(t, {early:true});
  const banner = page.locator('#cw-install');
  await page.locator('#cw-install-secondary').click();
  assert.equal(await banner.isVisible(), false);
  assert.equal(await page.evaluate(() => cwPwaDiag().dismissedRecently), true);
  assert.equal(await page.getByRole('dialog').count(), 0);
  await page.reload();
  assert.equal(await banner.isVisible(), false);
  await page.locator('#cw-mobile-more').click();
  await page.locator('#cw-mobile-install button').click();
  assert.equal(await page.evaluate(() => window.promptCalls), 1);
});

test('legacy Compact preferences and links keep full mobile navigation and routes', async t => {
  const frontend = await readFile(new URL('../ui_frontend.py', import.meta.url),'utf8');
  const page = await fixture(t);
  const bootstrap = await page.evaluate(source => {
    const doc = new DOMParser().parseFromString(source, 'text/html');
    return [...doc.querySelectorAll('script')].find(script => script.textContent.includes('dataset.cwInitialTab'))?.textContent;
  }, frontend);
  assert.ok(bootstrap);
  await page.evaluate(() => localStorage.setItem('cw_ui_mode','compact'));
  for (const query of ['', '?ui=compact', '?compact=1']) {
    await page.goto(`https://pwa-install.test/${query}#settings/sync`);
    await page.addScriptTag({content:bootstrap});
    await page.addScriptTag({url:'/assets/crosswatch.js'});
    assert.equal(await page.evaluate(() => document.documentElement.classList.contains('cw-compact')),false);
    assert.equal(await page.evaluate(() => document.documentElement.dataset.cwInitialTab),'settings');
    await page.evaluate(() => {
      const main = document.querySelector('main');
      main.innerHTML = '<section id="page-main" class="hidden">Home</section><section id="page-settings" class="hidden">Settings</section><section id="page-tracker" class="tab-page hidden">Tracker</section>';
      window.showTab('settings');
    });
    assert.equal(await page.locator('#page-settings').isVisible(),true);
    await page.evaluate(() => window.showTab('tracker'));
    assert.equal(await page.locator('#page-tracker').isVisible(),true);
    assert.equal(await page.locator('#cw-mobile-bottom-nav').isVisible(),true);
    await page.locator('#cw-mobile-more').click();
    assert.equal(await page.locator('#cw-mobile-more').getAttribute('aria-expanded'),'true');
  }
});
