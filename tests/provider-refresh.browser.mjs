/* tests/provider-refresh.browser.mjs */
/* CrossWatch - Provider refresh feedback and request lifecycle */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

const core = await readFile(new URL('../assets/helpers/core.js', import.meta.url), 'utf8');
const main = await readFile(new URL('../assets/js/main.js', import.meta.url), 'utf8');
const refresh = core.slice(core.indexOf('  async function manualRefreshStatus()'), core.indexOf('  function bootstrapStatusFromCache()'));
const busy = core.slice(core.indexOf('  function setRefreshBusy('), core.indexOf('  window.openAbout ='));
const strip = main.slice(main.indexOf('  const ensureHubLayoutStrip ='), main.indexOf('  window.addEventListener("cw:dashboard-widgets-layout-changed"'));

for (const width of [390,1440]) test(`provider refresh remains visible through completion and failure at ${width}px`, async t => {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width,height:800},hasTouch:width<760});
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('http://provider-refresh.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith('/assets/')) return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':'image/svg+xml'});
    const css = ['fonts/material-symbols-rounded','themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','themes/flat'];
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-theme="flat-dark"><head><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body><section id="ops-card" style="margin:20px;padding:20px;min-height:220px"><h2>Sync Hub</h2><div class="cw-main-card-head-actions" style="display:flex;justify-content:flex-end"><div id="conn-badges"><img src="/assets/img/PLEX.svg" width="44" height="44" alt="Plex"></div><button id="btn-status-refresh"><span class="material-symbols-rounded">refresh</span></button></div></section></body></html>`});
  });
  await page.goto('http://provider-refresh.test/');
  await page.addScriptTag({content:`(() => {
    const byId = id => document.getElementById(id);
    const UI = {}, loadStatusCache = () => null, refreshPairedProviders = async () => {};
    const renderConnectorStatus = () => {}, queueSafe = () => {};
    const refreshStatus = () => { window.requests = (window.requests || 0) + 1; return new Promise((resolve,reject) => { window.finish = resolve; window.fail = reject; }); };
    ${busy}
    ${refresh}
    window.manualRefreshStatus = manualRefreshStatus;
    let hubLayoutHost = null;
    const renderHubLayoutStrip = () => {};
    ${strip}
    byId('btn-status-refresh').addEventListener('click', manualRefreshStatus);
    ensureHubLayoutStrip();
  })();`});
  const toggle = page.locator('.cw-hub-layout-toggle');
  const button = page.locator('.cw-hub-refresh-proxy');
  const label = page.locator('.cw-hub-refresh-status');
  await toggle.click();
  await button.click();
  await page.waitForFunction(() => window.requests === 1);
  assert.equal(await toggle.getAttribute('aria-expanded'), 'true');
  assert.equal(await button.isVisible(), true);
  assert.equal(await button.isDisabled(), true);
  assert.equal(await button.getAttribute('aria-busy'), 'true');
  assert.equal(await label.isVisible(), true);
  assert.equal(await button.locator('span').evaluate(el => getComputedStyle(el).animationName), 'cw-provider-refresh-spin');
  await page.evaluate(() => { void manualRefreshStatus(); });
  assert.equal(await page.evaluate(() => window.requests), 1);
  const bounds = await label.boundingBox();
  assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= width);
  await mkdir('.tmp/provider-refresh', {recursive:true});
  await page.screenshot({path:`.tmp/provider-refresh/checking-${width}.png`});
  await toggle.click();
  assert.equal(await label.isVisible(), true);
  await page.evaluate(() => finish());
  await page.waitForFunction(() => !document.querySelector('.cw-hub-refresh-proxy').disabled);
  assert.equal(await label.isVisible(), false);
  assert.equal(await button.getAttribute('aria-busy'), 'false');
  await toggle.click();
  await page.emulateMedia({reducedMotion:'reduce'});
  await button.click();
  await page.waitForFunction(() => window.requests === 2);
  assert.equal(await label.isVisible(), true);
  assert.equal(await button.locator('span').evaluate(el => getComputedStyle(el).animationName), 'none');
  await page.evaluate(() => fail(new Error('Connection failed')));
  await page.waitForFunction(() => !document.querySelector('.cw-hub-refresh-proxy').disabled);
  assert.equal(await label.isVisible(), false);
  assert.equal(await page.locator('#conn-badges').getAttribute('aria-busy'), 'false');
  assert.deepEqual(errors, []);
});
