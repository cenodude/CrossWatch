/* tests/mobile-preferences.browser.mjs */
/* CrossWatch - Mobile shortcuts, playing card visibility and quick add workflows */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

const dashboard = await readFile(new URL('../assets/js/dashboard-widgets.js', import.meta.url), 'utf8');
const picker = dashboard.slice(dashboard.indexOf('  function updateMobilePicker('), dashboard.indexOf('  function showAllWidgets('));
const main = await readFile(new URL('../assets/js/main.js', import.meta.url), 'utf8');
const quickAdd = main.slice(main.lastIndexOf('(() => {', main.indexOf('  const ROOT_ID = "cw-quick-add"')));
const styles = ['fonts/material-symbols-rounded', 'themes/tokens', 'css/base', 'crosswatch', 'css/layout', 'css/components', 'css/pages', 'css/page-theme', 'ui-shell'];

async function fixture(t, theme = 'flat-dark', restricted = false, saved = null, {profileAvailable = true, managedWrite = false} = {}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:390,height:844},hasTouch:true,isMobile:true,reducedMotion:'reduce'});
  page.setDefaultTimeout(7000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  if (saved !== null) await page.addInitScript(value => localStorage.setItem('cw.mobile.shortcuts.v1', value), saved);
  let empty = false;
  await page.route('https://mobile-preferences.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith('/assets/')) return route.fulfill({body:await readFile(new URL('..' + path, import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});
    if (path === '/api/watch/currently_watching') return route.fulfill({json:{streams:empty ? [] : [{title:'Arrival',state:'playing',media_type:'movie',progress:30,source:'plex'}]}});
    if (path.startsWith('/api/')) return route.fulfill({json:{}});
    const attrs = theme === 'original' ? 'class="cw-theme-original"' : `data-cw-theme="${theme}" class="cw-theme-${theme === 'flat-light' ? 'light' : 'dark'}"`;
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html ${attrs} data-tab="main" data-cw-role="${restricted || managedWrite ? 'user' : 'admin'}"><head><meta name="viewport" content="width=device-width,initial-scale=1">${[...styles,theme==='original'?'themes/original-coverage':'themes/flat'].map(path=>`<link rel="stylesheet" href="/assets/${path}.css">`).join('')}</head><body>
      <header><nav class="tabs"><button id="tab-main" class="tab">Home</button>${restricted?'':'<button id="tab-tracker" class="tab">Tracker</button><button id="tab-snapshots" class="tab">Captures</button><button id="tab-playlists" class="tab">Playlists</button><button id="tab-editor" class="tab">Editor</button><button id="tab-settings" class="tab">Settings</button>'}<button id="tab-about" class="tab">About</button></nav></header>
      <main id="layout"><section id="dashboard-widgets-card"></section><h1>Home</h1></main>
      ${profileAvailable ? '<a href="/profile" data-cw-profile-menu-action="profile">Profile</a>' : ''}
      <script>window.auth={isManaged:${restricted},permissions:{write:${!restricted}},preferences:{}};window._cfgCache={tmdb_configured:true,ui:{}};window.CW={AuthState:{read:()=>auth}};window.openManualWatchedModal=()=>{window.quickAddOpened=true};window.addEventListener('hashchange',()=>{document.documentElement.dataset.tab=location.hash.slice(1).split('/')[0]||'main';document.dispatchEvent(new Event('tab-changed'));});</script>
      <script src="/assets/helpers/account-menu.js"></script><script src="/assets/helpers/playing-card.js"></script><script src="/assets/js/playingcard.js"></script>
      <script>(()=>{const $=selector=>document.querySelector(selector),esc=value=>value;const WIDGETS=[{key:'history',label:'Recent history'},{key:'watchlist',label:'Watchlist'}],widgetLayout={},lastSettings={},currentConfig=null,mobileViewport=matchMedia('(max-width:760px)');function patchLayout(){}function resetLayout(){}function markWidgetsDirty(){}${picker}window.updateMobilePicker=updateMobilePicker;document.addEventListener('cw-mobile-navigation-ready',()=>updateMobilePicker());document.addEventListener('DOMContentLoaded',()=>updateMobilePicker());})();</script>
      <script>${quickAdd}</script></body></html>`});
  });
  await page.goto('https://mobile-preferences.test/');
  await page.locator('#cw-mobile-bottom-nav').waitFor();
  return {page,empty:() => {empty=true;}};
}

async function openSettings(page) {
  await page.locator('#cw-mobile-more').click();
  await page.locator('#cw-mobile-widgets > summary').click();
}

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: shortcuts persist and mobile playing-card visibility stays separate`, async t => {
  const {page,empty} = await fixture(t,theme);
  const routes = () => page.locator('#cw-mobile-bottom-nav a').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute));
  assert.deepEqual(await routes(),['main','tracker','settings/providers','settings/sync']);
  await page.waitForFunction(()=>document.querySelector('#playing-detail').classList.contains('show'));
  await openSettings(page);
  assert.equal(await page.locator('#tab-main').isVisible(),false);
  assert.equal(await page.locator('#tab-tracker').isVisible(),false);
  assert.equal(await page.locator('#cw-mobile-navigation [data-mobile-route="settings/scrobbler"]').isVisible(),true);
  await page.locator('#cw-mobile-shortcut-0').selectOption('settings/scrobbler');
  assert.deepEqual(await routes(),['main','tracker','settings/scrobbler','settings/sync']);
  assert.equal(await page.locator('#cw-mobile-shortcut-1 option[value="settings/scrobbler"]').isDisabled(),true);
  await page.keyboard.press('Escape');
  assert.equal(await page.locator('#cw-mobile-more').evaluate(node=>node===document.activeElement),true);
  await page.locator('#cw-mobile-more').click();
  await page.locator('[data-mobile-playing-card]').uncheck();
  await page.waitForFunction(()=>!document.querySelector('#playing-detail').classList.contains('show'));
  await page.evaluate(()=>{window._cfgCache.ui.show_playingcard=false;updateMobilePicker({history:false});});
  assert.equal(await page.locator('[data-mobile-playing-card]').isDisabled(),true);
  assert.match(await page.locator('[data-mobile-playing-card]').locator('..').textContent(),/Disabled in main settings/);
  await mkdir('.tmp/mobile-preferences',{recursive:true});
  await page.screenshot({path:`.tmp/mobile-preferences/${theme}-settings.png`});
  for (const width of [320,760,390]) {
    await page.setViewportSize({width,height:844});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  }
  await page.reload();
  assert.deepEqual(await routes(),['main','tracker','settings/scrobbler','settings/sync']);
  assert.equal(await page.locator('#playing-detail').evaluate(el=>el.classList.contains('show')),false);
  await page.setViewportSize({width:1200,height:900});
  await page.waitForFunction(()=>document.querySelector('#playing-detail').classList.contains('show'));
  assert.equal(await page.locator('#tab-tracker').isVisible(),true);
  await page.setViewportSize({width:390,height:844});
  await page.waitForFunction(()=>!document.querySelector('#playing-detail').classList.contains('show'));
  await page.locator('#cw-mobile-bottom-nav [data-mobile-route="tracker"]').click();
  await page.locator('.cw-qa-fab').waitFor({state:'visible'});
  await page.locator('.cw-qa-fab').click();
  assert.equal(await page.evaluate(()=>window.quickAddOpened),true);
  const fab = await page.locator('.cw-qa-fab').boundingBox(), nav = await page.locator('#cw-mobile-bottom-nav').boundingBox();
  assert.ok(fab.y+fab.height<=nav.y);
  await openSettings(page);
  await page.evaluate(()=>{auth.preferences.playing_card=false;updateMobilePicker();});
  assert.match(await page.locator('[data-mobile-playing-card]').locator('..').textContent(),/Disabled in your profile settings/);
  await page.evaluate(()=>{auth.preferences.playing_card=true;updateMobilePicker();});
  await page.locator('[data-mobile-playing-card]').check();
  empty();
  await page.evaluate(()=>window.dispatchEvent(new Event('cw:auth-state-changed')));
  await page.waitForFunction(()=>!document.querySelector('#playing-detail').classList.contains('show'));
  await page.screenshot({path:`.tmp/mobile-preferences/${theme}-empty.png`});
});

test('restricted accounts discard inaccessible shortcuts and cannot quick add', async t => {
  const {page}=await fixture(t,'flat-dark',true,'["settings/providers","settings/scrobbler"]');
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)),['main','profile/overview']);
  await page.evaluate(()=>{document.documentElement.dataset.tab='tracker';document.dispatchEvent(new Event('tab-changed'));});
  assert.equal(await page.locator('.cw-qa-fab').isVisible(),false);
  await openSettings(page);
  assert.equal(await page.locator('.cw-mobile-shortcuts').count(),0);
});

test('invalid saved shortcuts recover to defaults', async t => {
  const {page}=await fixture(t,'flat-dark',false,'{broken');
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)),['main','tracker','settings/providers','settings/sync']);
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: My Media Hub pins Overview without adding slots`, async t => {
  const {page} = await fixture(t,theme);
  const pinned = page.locator('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]');
  const extra = page.locator('#cw-mobile-navigation [data-mobile-route="profile/overview"]');
  await openSettings(page);
  assert.equal(await extra.getAttribute('href'), '/profile#overview');
  const first = page.locator('#cw-mobile-shortcut-0');
  await first.focus();
  await page.keyboard.press('End');
  await page.keyboard.press('Enter');
  assert.equal(await first.inputValue(), 'profile/overview');
  assert.equal(await pinned.count(), 1);
  assert.equal(await extra.count(), 0);
  assert.equal(await page.locator('#cw-mobile-bottom-nav > *').count(), 5);
  const second = page.locator('#cw-mobile-shortcut-1');
  assert.equal(await second.locator('option[value="profile/overview"]').isDisabled(), true);
  await second.evaluate(select => {select.value='profile/overview';select.dispatchEvent(new Event('change',{bubbles:true}));});
  assert.equal(await second.inputValue(), 'settings/sync');
  assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem('cw.mobile.shortcuts.v1'))), ['profile/overview','settings/sync']);
  await page.keyboard.press('Escape');
  for (const width of [320,360,390]) {
    await page.setViewportSize({width,height:740});
    const nav = page.locator('#cw-mobile-bottom-nav');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth), false);
    for (const item of await nav.locator(':scope > *').all()) {
      const rect = await item.boundingBox();
      assert.ok(rect.width >= 44 && rect.height >= 44);
      assert.equal(await item.evaluate(node=>node.scrollWidth>node.clientWidth), false);
    }
    await mkdir('.tmp/mobile-preferences',{recursive:true});
    await nav.screenshot({path:`.tmp/mobile-preferences/${theme}-media-hub-${width}.png`});
  }
  await page.reload();
  assert.equal(await pinned.count(), 1);
  assert.equal(await page.locator('#cw-mobile-bottom-nav [data-mobile-route="settings/sync"]').count(), 1);
  await pinned.focus();
  await page.keyboard.press('Enter');
  await page.waitForURL('**/profile#overview');
  assert.equal(await pinned.getAttribute('aria-current'), 'page');
  assert.equal(await page.locator('#cw-mobile-more').evaluate(node=>node.classList.contains('is-current')), false);
  await page.evaluate(()=>location.hash='history');
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-more').classList.contains('is-current'));
  assert.equal(await pinned.getAttribute('aria-current'), null);
  await openSettings(page);
  await first.selectOption('settings/providers');
  assert.equal(await pinned.count(), 0);
  assert.equal(await extra.count(), 1);
  await extra.focus();
  await page.keyboard.press('Enter');
  await page.waitForURL('**/profile#overview');
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-navigation [data-mobile-route="profile/overview"]').getAttribute('aria-current')==='page');
  assert.equal(await extra.getAttribute('aria-current'), 'page');
  assert.equal(await page.locator('#cw-mobile-more').getAttribute('aria-expanded'), 'false');
  assert.equal(await page.locator('#cw-mobile-more').evaluate(node=>node.classList.contains('is-current')), true);
});

for (const [saved, expected] of [
  ['["playlists","editor"]', ['playlists','editor']],
  ['["profile/overview","profile/overview"]', ['profile/overview','settings/providers']],
  ['["missing",null,17,{},"profile/overview"]', ['profile/overview','settings/providers']],
]) test(`saved shortcuts retain valid choices and remove duplicates: ${saved}`, async t => {
  const {page} = await fixture(t,'flat-dark',false,saved);
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)), ['main','tracker',...expected]);
});

test('My Media Hub is unavailable when the profile destination is absent', async t => {
  const {page} = await fixture(t,'flat-dark',false,'["profile/overview","editor"]',{profileAvailable:false});
  await openSettings(page);
  assert.equal(await page.locator('option[value="profile/overview"], [data-mobile-route="profile/overview"]').count(), 0);
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)), ['main','tracker','editor','settings/providers']);
});

test('managed writers retain permitted shortcuts and routes from the media hub', async t => {
  const {page} = await fixture(t,'flat-dark',false,'["settings/providers","profile/overview"]',{managedWrite:true});
  await page.locator('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]').click();
  await page.waitForURL('**/profile#overview');
  await openSettings(page);
  assert.deepEqual(await page.locator('#cw-mobile-shortcut-0 option').evaluateAll(nodes=>nodes.map(node=>node.value)), ['snapshots','playlists','editor','profile/overview']);
  for (const route of ['main','tracker','snapshots']) assert.equal(await page.locator(`#cw-mobile-bottom-nav [data-mobile-route="${route}"]`).getAttribute('href'), `/?main=1&ui=full#${route}`);
});
