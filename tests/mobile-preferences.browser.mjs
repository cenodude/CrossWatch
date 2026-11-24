/* tests/mobile-preferences.browser.mjs */
/* CrossWatch - Mobile shortcuts, playing card visibility and quick add workflows */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import './i18n-browser-setup.mjs';
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
  if (saved !== null) await page.addInitScript(value => {
    if (sessionStorage.getItem('shortcuts-seeded')) return;
    localStorage.setItem('cw.mobile.shortcuts.v1', value);
    sessionStorage.setItem('shortcuts-seeded', '1');
  }, saved);
  let empty = false;
  await page.route('https://mobile-preferences.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith('/assets/')) return route.fulfill({body:await readFile(new URL('..' + path, import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});
    if (path === '/api/watch/currently_watching') return route.fulfill({json:{streams:empty ? [] : [{title:'Arrival',state:'playing',media_type:'movie',progress:30,source:'plex'}]}});
    if (path.startsWith('/api/')) return route.fulfill({json:{}});
    const attrs = theme === 'original' ? 'class="cw-theme-original"' : `data-cw-theme="${theme}" class="cw-theme-${theme === 'flat-light' ? 'light' : 'dark'}"`;
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html ${attrs} data-tab="main" data-cw-role="${restricted || managedWrite ? 'user' : 'admin'}"><head><meta name="viewport" content="width=device-width,initial-scale=1">${[...styles,theme==='original'?'themes/original-coverage':'themes/flat'].map(path=>`<link rel="stylesheet" href="/assets/${path}.css">`).join('')}</head><body>
      <header><nav class="tabs"><button id="tab-main" class="tab">Home</button>${restricted?'':'<button id="tab-tracker" class="tab">Tracker</button><button id="tab-snapshots" class="tab">Captures</button><button id="tab-playlists" class="tab">Playlists</button><button id="tab-editor" class="tab">Editor</button><div class="cw-tabmenu" id="tab-settings-menu"><button id="tab-settings" class="tab" aria-haspopup="menu" aria-expanded="false" onclick="document.getElementById(&quot;cw-settings-menu&quot;).classList.toggle(&quot;hidden&quot;)">Settings</button><div id="cw-settings-menu" class="cw-menu hidden"><button class="cw-menu-item" data-settings-pane="providers">Connections</button><button class="cw-menu-item" data-settings-pane="sync">Sync pairs</button><button class="cw-menu-item" data-settings-pane="scrobbler">Scrobbler</button><button class="cw-menu-item" data-settings-pane="app">UI and Security</button></div></div>'}<div class="cw-tabmenu" id="tab-about-menu"><button id="tab-about" class="tab">About</button></div>${profileAvailable ? '<a id="tab-profile" class="tab" href="/hub">Profile</a>' : ''}</nav></header>
      <main id="layout"><section id="dashboard-widgets-card"></section><h1>Home</h1></main>
      <script>window.auth={isManaged:${restricted},permissions:{write:${!restricted}},preferences:{}};window._cfgCache={tmdb_configured:true,ui:{}};window.CW={AuthState:{read:()=>auth}};window.openManualWatchedModal=()=>{window.quickAddOpened=true};window.addEventListener('hashchange',()=>{document.documentElement.dataset.tab=location.hash.slice(1).split('/')[0]||'main';document.dispatchEvent(new Event('tab-changed'));});</script>
      <script src="/assets/helpers/mobile-experience.js"></script><script src="/assets/helpers/account-menu.js"></script><script src="/assets/helpers/mobile-settings.js"></script><script src="/assets/helpers/playing-card.js"></script><script src="/assets/js/playingcard.js"></script>
      <script>(()=>{const $=selector=>document.querySelector(selector),esc=value=>value;const ON_PROFILE_PAGE=location.pathname==='/hub',WIDGETS=[{key:'history',label:'Recent history'},{key:'watchlist',label:'Watchlist'}],widgetLayout={},lastSettings={},currentConfig=null,mobileViewport=matchMedia('(max-width:760px)');function patchLayout(){}function resetLayout(){}function markWidgetsDirty(){}${picker}window.updateMobilePicker=updateMobilePicker;document.addEventListener('cw-mobile-navigation-ready',()=>updateMobilePicker());document.addEventListener('DOMContentLoaded',()=>updateMobilePicker());})();</script>
      <script>${quickAdd}</script></body></html>`});
  });
  await page.goto('https://mobile-preferences.test/');
  await page.locator('#cw-mobile-bottom-nav').waitFor();
  return {page,empty:() => {empty=true;}};
}

async function openSettings(page) {
  if (await page.locator('#cw-mobile-more').getAttribute('aria-expanded') !== 'true') await page.locator('#cw-mobile-more').click();
  if (await page.locator('#cw-mobile-navigation').getAttribute('data-mobile-view') !== 'settings') await page.locator('#cw-mobile-widgets > summary').click();
  await page.waitForFunction(() => document.querySelector('#cw-mobile-navigation').dataset.mobileView === 'settings');
}

async function backToMenu(page) {
  await page.locator('.cw-mobile-sheet-back').click();
  await page.waitForFunction(() => document.querySelector('#cw-mobile-navigation').dataset.mobileView === 'main');
}

async function closeMenu(page) {
  await page.locator('.cw-mobile-sheet-close').click();
  await page.waitForFunction(() => !history.state?.cwMobileLayer && document.querySelector('#cw-mobile-more').getAttribute('aria-expanded') === 'false');
}

for(const theme of ['original','flat-dark','flat-light']) test(`${theme}: compact settings preserve device preferences and disabled controls`,async t=>{
  const {page}=await fixture(t,theme);
  assert.equal(await page.locator('html').getAttribute('class'),theme==='original'?'cw-theme-original':`cw-theme-${theme==='flat-light'?'light':'dark'}`);
  await page.setViewportSize({width:320,height:740});
  await openSettings(page);
  const menu=page.locator('#cw-mobile-navigation');
  assert.equal(await menu.getAttribute('data-mobile-view'),'settings');
  assert.equal(await menu.locator('[data-mobile-push]').isVisible(),false);
  await menu.locator('[data-mobile-playing-card]').uncheck();
  assert.equal(await page.evaluate(()=>localStorage.getItem('cw.mobile.playingCard')),'0');
  await menu.locator('[data-mobile-notification-alerts]').check();
  assert.equal(await page.evaluate(()=>localStorage.getItem('cw.mobile.notificationAlerts')),'1');
  await page.evaluate(()=>{
    localStorage.setItem('cw.mobile.notificationAlerts','0');
    window.dispatchEvent(new CustomEvent('cw:mobile-notification-alerts-changed'));
  });
  assert.equal(await menu.locator('[data-mobile-notification-alerts]').isChecked(),false);
  await page.evaluate(()=>{
    localStorage.setItem('cw.mobile.notificationAlerts','1');
    window.dispatchEvent(new StorageEvent('storage',{key:'cw.mobile.notificationAlerts'}));
  });
  assert.equal(await menu.locator('[data-mobile-notification-alerts]').isChecked(),true);
  await menu.locator('[data-mobile-provider-stats]').uncheck();
  assert.equal(await page.evaluate(()=>localStorage.getItem('cw.mobile.providerStats')),'0');
  await menu.locator('[data-mobile-sync-results]').check();
  assert.equal(await page.evaluate(()=>localStorage.getItem('cw.mobile.syncResults')),'1');
  await page.evaluate(()=>{window._cfgCache.ui.show_playingcard=false;updateMobilePicker({history:false});});
  assert.equal(await menu.locator('[data-mobile-playing-card]').isDisabled(),true);
  assert.equal(await menu.locator('[data-mobile-widget="history"]').isDisabled(),true);
  assert.equal(await menu.evaluate(node=>node.scrollWidth<=node.clientWidth+1),true);
  await menu.locator('.cw-mobile-sheet-back').click();
  assert.equal(await menu.getAttribute('data-mobile-view'),'main');
  assert.equal(await menu.locator('#cw-mobile-widgets > summary').isVisible(),true);
  await page.keyboard.press('Escape');
  await openSettings(page);
  assert.equal(await menu.locator('[data-mobile-provider-stats]').isChecked(),false);
  assert.equal(await menu.locator('[data-mobile-sync-results]').isChecked(),true);
});

test('mobile push waits for its status and keeps changes busy until completion',async t=>{
  const {page}=await fixture(t);
  await page.evaluate(()=>{
    window.pushReads=0;
    window.CW.Notifications={push:{
      status:()=>{window.pushReads++;return new Promise(resolve=>{window.resolvePushStatus=resolve;});},
      set:on=>{window.pushChoice=on;return new Promise(resolve=>{window.resolvePushChange=resolve;});},
    }};
  });
  await openSettings(page);
  const push=page.locator('[data-mobile-push]');
  await page.waitForFunction(()=>window.pushReads===1);
  assert.equal(await push.isDisabled(),true);
  await page.evaluate(()=>window.resolvePushStatus({available:true,on:true,note:'Enabled'}));
  await page.waitForFunction(()=>!document.querySelector('[data-mobile-push]').disabled);
  assert.equal(await push.isChecked(),true);
  await push.uncheck();
  assert.equal(await page.evaluate(()=>window.pushChoice),false);
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('cw:notification-push-changed')));
  assert.equal(await push.isDisabled(),true);
  assert.equal(await page.evaluate(()=>window.pushReads),1);
  await page.evaluate(()=>window.resolvePushChange({available:true,on:false,note:'Disabled'}));
  await page.waitForFunction(()=>!document.querySelector('[data-mobile-push]').disabled);
  assert.equal(await push.isChecked(),false);
});

test('Quick add is available on Home and stays hidden in Tracker and Media Hub',async t=>{
  const {page}=await fixture(t);
  await page.locator('#cw-quick-add .cw-qa-fab').waitFor();
  for(const tab of ['tracker','profile','main']) {
    await page.evaluate(tab=>{
      document.documentElement.dataset.tab=tab;
      document.dispatchEvent(new Event('tab-changed'));
    },tab);
    assert.equal(await page.locator('#cw-quick-add .cw-qa-fab').isVisible(),tab==='main');
  }
});

async function swipeSurface(page, content = '<button id="swipe-card" style="width:100%;height:100%">Media card</button>') {
  await page.evaluate(content => {
    document.querySelector('#swipe-surface')?.remove();
    const surface = document.createElement('div');
    surface.id = 'swipe-surface';
    surface.style.cssText = 'position:fixed;inset:200px 0 auto;height:260px;z-index:1000;background:#111;';
    surface.innerHTML = content;
    surface.addEventListener('click', () => { window.swipeCardClicks = (window.swipeCardClicks || 0) + 1; });
    document.body.appendChild(surface);
  }, content);
}

const touchSessions = new WeakMap();

async function swipe(page, from = {x:290,y:300}, to = {x:90,y:300}, steps = 8, interval = 0) {
  if (!touchSessions.has(page)) touchSessions.set(page, await page.context().newCDPSession(page));
  const session = touchSessions.get(page);
  await session.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:[from]});
  for (let step = 1; step <= steps; step++) {
    if (interval) await page.waitForTimeout(interval);
    await session.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:[{x:from.x+(to.x-from.x)*step/steps,y:from.y+(to.y-from.y)*step/steps}]});
  }
  await session.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
}

test('mobile swipes claim the first horizontal move and allow slower gestures', async t => {
  const {page} = await fixture(t);
  await swipeSurface(page);
  await page.evaluate(() => { document.body.style.minHeight = '2400px'; });
  await swipe(page, {x:294,y:300}, {x:90,y:306}, 12);
  await page.waitForURL('**/#tracker');
  const started = Date.now();
  await swipe(page, {x:90,y:300}, {x:294,y:306}, 40, 25);
  assert.ok(Date.now() - started > 900);
  await page.waitForURL('**/#main');
  assert.equal(await page.evaluate(() => window.swipeCardClicks || 0), 0);
});

test('mobile swipes follow visible shortcuts and wrap in both directions without opening cards', async t => {
  const {page} = await fixture(t);
  await swipeSurface(page);
  await page.locator('#swipe-card').tap();
  await page.waitForFunction(() => window.swipeCardClicks === 1);
  await page.evaluate(() => { window.swipeCardClicks = 0; });
  await swipe(page);
  await page.waitForURL('**/#tracker');
  assert.equal(await page.evaluate(() => window.swipeCardClicks || 0), 0);
  await swipe(page);
  await page.waitForURL(url => url.pathname === '/hub');
  await page.locator('#cw-mobile-bottom-nav').waitFor();
  await swipeSurface(page);
  await swipe(page);
  await page.waitForURL('**/#main');
  await page.locator('#cw-mobile-bottom-nav').waitFor();
  await swipeSurface(page);
  await swipe(page, {x:90,y:300}, {x:290,y:300});
  await page.waitForURL(url => url.pathname === '/hub');
  await page.locator('#cw-mobile-bottom-nav').waitFor();
  await swipeSurface(page);
  await swipe(page, {x:90,y:300}, {x:290,y:300});
  await page.waitForURL('**/#tracker');
  await page.locator('#cw-mobile-bottom-nav').waitFor();
  await swipeSurface(page);
  await swipe(page, {x:90,y:300}, {x:290,y:300});
  await page.waitForURL(url => url.hash === '#main');
  await page.locator('#swipe-card').tap();
  await page.waitForFunction(() => window.swipeCardClicks === 1);
  await swipe(page);
  await page.waitForURL('**/#tracker');
  assert.equal(await page.evaluate(() => window.swipeCardClicks || 0), 1);
});

test('mobile swipes pick up shortcut changes and leave gestures to interactive content', async t => {
  const {page} = await fixture(t);
  await openSettings(page);
  await page.locator('#cw-mobile-shortcut-0').selectOption('snapshots');
  await page.locator('#cw-mobile-shortcut-1').selectOption('editor');
  await closeMenu(page);
  await swipeSurface(page);
  await swipe(page);
  await page.waitForURL('**/#snapshots');
  await swipe(page);
  await page.waitForURL('**/#editor');
  await swipe(page, {x:90,y:300}, {x:290,y:300});
  await page.waitForURL('**/#snapshots');
  for (const content of [
    '<div style="height:100%;overflow-x:auto"><div style="width:1200px;height:100%">Carousel</div></div>',
    '<input type="range" style="width:100%;height:240px">',
    '<textarea style="width:100%;height:240px"></textarea>',
    '<div contenteditable="true" style="height:100%">Edit title</div>',
    '<div draggable="true" style="height:100%">Drag item</div>',
    '<div style="touch-action:none;height:100%">Timeline</div>',
    '<div data-no-swipe style="height:100%">Custom gesture</div>'
  ]) {
    await swipeSurface(page, content);
    await swipe(page);
    assert.equal(new URL(page.url()).hash, '#snapshots', content);
  }
  await swipeSurface(page, '<div id="swipe-carousel" style="height:100%;overflow-x:auto"><div style="width:1200px;height:100%">Carousel</div></div>');
  await swipe(page, {x:290,y:300}, {x:90,y:300});
  await page.waitForFunction(() => document.querySelector('#swipe-carousel').scrollLeft > 0);
  assert.equal(new URL(page.url()).hash, '#snapshots');
  await swipeSurface(page, '<div id="swipe-scroll" style="height:100%;overflow-y:auto"><div style="height:1200px">Scrollable content</div></div>');
  await swipe(page, {x:190,y:390}, {x:190,y:230});
  await page.waitForFunction(() => document.querySelector('#swipe-scroll').scrollTop > 0);
  assert.equal(new URL(page.url()).hash, '#snapshots');
  await swipeSurface(page);
  await swipe(page, {x:90,y:300}, {x:110,y:400});
  await swipe(page, {x:5,y:300}, {x:290,y:300});
  await swipe(page, {x:90,y:300}, {x:125,y:300});
  assert.equal(new URL(page.url()).hash, '#snapshots');
  await page.evaluate(() => {
    const dialog = document.createElement('dialog');
    dialog.id = 'swipe-dialog';
    dialog.textContent = 'Dialog';
    document.body.appendChild(dialog);
    dialog.showModal();
  });
  await swipe(page);
  assert.equal(new URL(page.url()).hash, '#snapshots');
  await page.evaluate(() => document.querySelector('#swipe-dialog').remove());
  await page.locator('#cw-mobile-more').click();
  await swipe(page);
  assert.equal(new URL(page.url()).hash, '#snapshots');
  await page.keyboard.press('Escape');
  await page.setViewportSize({width:1200,height:900});
  await swipe(page);
  assert.equal(new URL(page.url()).hash, '#snapshots');
});

test('default mobile bar has two replaceable shortcuts and destination icons', async t => {
  const {page} = await fixture(t);
  const routes = () => page.locator('#cw-mobile-bottom-nav a[data-mobile-route]').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute));
  assert.deepEqual(await routes(), ['main','tracker','profile/overview']);
  assert.equal(await page.locator('#cw-mobile-bottom-nav > *').count(), 5);
  await openSettings(page);
  assert.equal(await page.locator('#cw-mobile-shortcut-0').inputValue(), 'tracker');
  assert.equal(await page.locator('#cw-mobile-shortcut-1').inputValue(), 'profile/overview');
  for (const [route, icon] of Object.entries({main:'home',tracker:'inventory_2',snapshots:'photo_library',playlists:'queue_music',editor:'edit_note'})) {
    assert.equal(await page.locator(`#tab-${route} .cw-mobile-destination-icon`).textContent(), icon);
  }
  await page.locator('#cw-mobile-shortcut-0').selectOption('snapshots');
  await page.locator('#cw-mobile-shortcut-1').selectOption('editor');
  assert.deepEqual(await routes(), ['main','snapshots','editor']);
  await backToMenu(page);
  assert.equal(await page.locator('#tab-tracker').isVisible(), true);
  assert.equal(await page.locator('#cw-mobile-navigation #tab-profile').isVisible(), true);
  await openSettings(page);
  await page.locator('#cw-mobile-shortcut-1').selectOption('tracker');
  assert.equal(await page.locator('#tab-tracker').isVisible(), false);
  await page.reload();
  assert.deepEqual(await routes(), ['main','snapshots','tracker']);
  await page.locator('#cw-mobile-bottom-nav [data-mobile-route="tracker"]').click();
  await page.waitForURL('**/#tracker');
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-bottom-nav [data-mobile-route="tracker"]').getAttribute('aria-current')==='page');
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: More menu has quiet surfaces, readable labels and keyboard focus`, async t => {
  const {page} = await fixture(t,theme,false,'["tracker","settings/sync"]');
  await page.locator('#cw-mobile-more').click();
  const menu = page.locator('#cw-mobile-navigation');
  const tile = menu.locator('#tab-profile');
  for (const width of [320,390,760]) {
    await page.setViewportSize({width,height:844});
    assert.equal(await menu.locator('#tab-tracker').isVisible(),false);
    assert.equal(await menu.locator('#tab-about').isVisible(),false);
    const style = await tile.evaluate(node=>{
      const s=getComputedStyle(node);
      const context=document.createElement('canvas').getContext('2d');
      const rgb=value=>{context.fillStyle=value;context.fillRect(0,0,1,1);return [...context.getImageData(0,0,1,1).data].slice(0,3);};
      const luminance=value=>rgb(value).map(c=>c/255).map(c=>c<=.04045?c/12.92:((c+.055)/1.055)**2.4).reduce((sum,c,i)=>sum+c*[.2126,.7152,.0722][i],0);
      const contrast=(a,b)=>(Math.max(luminance(a),luminance(b))+.05)/(Math.min(luminance(a),luminance(b))+.05);
      context.fillStyle=getComputedStyle(document.documentElement).getPropertyValue('--cw-theme-bg') || '#000';
      context.fillRect(0,0,1,1);
      const ancestors=[];
      for(let current=node;current;current=current.parentElement) ancestors.unshift(current);
      for(const current of ancestors){context.fillStyle=getComputedStyle(current).backgroundColor;context.fillRect(0,0,1,1);}
      const background=`rgb(${[...context.getImageData(0,0,1,1).data].slice(0,3).join(',')})`;
      return {border:s.borderTopWidth,radius:s.borderTopLeftRadius,textContrast:contrast(s.color,background)};
    });
    assert.equal(style.border,'0px');
    assert.equal(style.radius,'0px');
    assert.ok(style.textContrast>=7,JSON.stringify(style));
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
    await page.keyboard.press('Tab');
    await tile.focus();
    assert.equal(await tile.evaluate(node=>getComputedStyle(node).outlineWidth),'2px');
    await tile.evaluate(node=>node.blur());
    await mkdir('.tmp/mobile-preferences',{recursive:true});
    const bounds=await menu.boundingBox();
    await page.screenshot({path:`.tmp/mobile-preferences/${theme}-tracker-more-${width}.png`,clip:{x:0,y:bounds.y,width,height:844-bounds.y}});
  }
  await tile.click();
  await page.waitForURL(url => url.pathname === '/hub');
  await page.locator('#cw-mobile-more').click();
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-navigation #tab-profile')?.getAttribute('aria-current')==='page');
  const selected=await tile.evaluate(node=>getComputedStyle(node).backgroundColor);
  assert.notEqual(selected,await menu.locator('#tab-editor').evaluate(node=>getComputedStyle(node).backgroundColor));
  await page.setViewportSize({width:1200,height:900});
  assert.equal(await menu.evaluate(node=>node.classList.contains('cw-page-mobile-navigation')),false);
  assert.equal(await page.locator('#tab-editor').evaluate(node=>getComputedStyle(node).borderTopWidth),'0px');
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: shortcuts persist and mobile playing-card visibility stays separate`, async t => {
  const {page,empty} = await fixture(t,theme,false,'["settings/providers","tracker"]');
  const routes = () => page.locator('#cw-mobile-bottom-nav a[data-mobile-route]').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute));
  assert.deepEqual(await routes(),['main','settings/providers','tracker']);
  await page.waitForFunction(()=>document.querySelector('#playing-detail').classList.contains('show'));
  await openSettings(page);
  assert.equal(await page.locator('#tab-main').isVisible(),false);
  assert.equal(await page.locator('#tab-tracker').isVisible(),false);
  await backToMenu(page);
  assert.equal(await page.locator('#cw-mobile-navigation [data-mobile-route="settings/scrobbler"]').isVisible(),true);
  await openSettings(page);
  await page.locator('#cw-mobile-shortcut-0').selectOption('settings/scrobbler');
  assert.deepEqual(await routes(),['main','settings/scrobbler','tracker']);
  assert.equal(await page.locator('#cw-mobile-shortcut-1 option[value="settings/scrobbler"]').isDisabled(),true);
  await closeMenu(page);
  assert.equal(await page.locator('#cw-mobile-more').evaluate(node=>node===document.activeElement),true);
  await openSettings(page);
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
  assert.deepEqual(await routes(),['main','settings/scrobbler','tracker']);
  assert.equal(await page.locator('#playing-detail').evaluate(el=>el.classList.contains('show')),false);
  await page.setViewportSize({width:1200,height:900});
  await page.waitForFunction(()=>document.querySelector('#playing-detail').classList.contains('show'));
  assert.equal(await page.locator('#tab-tracker').isVisible(),true);
  await page.setViewportSize({width:390,height:844});
  await page.waitForFunction(()=>!document.querySelector('#playing-detail').classList.contains('show'));
  await page.locator('#cw-mobile-bottom-nav [data-mobile-route="tracker"]').click();
  await page.locator('.cw-qa-fab').waitFor({state:'hidden'});
  await page.locator('#cw-mobile-bottom-nav [data-mobile-route="main"]').click();
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
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a[data-mobile-route]').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)),['main','profile/overview']);
  await page.evaluate(()=>{document.documentElement.dataset.tab='tracker';document.dispatchEvent(new Event('tab-changed'));});
  assert.equal(await page.locator('.cw-qa-fab').isVisible(),false);
  await openSettings(page);
  assert.equal(await page.locator('.cw-mobile-shortcuts').count(),0);
  assert.equal(await page.locator('#cw-mobile-navigation #tab-tracker').count(),0);
});

test('invalid saved shortcuts recover to defaults', async t => {
  const {page}=await fixture(t,'flat-dark',false,'{broken');
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a[data-mobile-route]').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)),['main','tracker','profile/overview']);
});

test('More excludes fixed and pinned destinations while keeping unpinned settings accessible', async t => {
  const {page} = await fixture(t,'flat-dark',false,'["settings/providers","settings/sync"]');
  await page.locator('#cw-mobile-more').click();
  assert.equal(await page.locator('#cw-mobile-navigation [data-mobile-route="settings/scrobbler"]').isVisible(),true);
  for (const pane of ['providers','sync']) assert.equal(await page.locator(`#cw-mobile-navigation [data-mobile-route="settings/${pane}"]`).count(),0);
  await openSettings(page);
  await page.locator('#cw-mobile-shortcut-0').selectOption('snapshots');
  await page.locator('#cw-mobile-shortcut-1').selectOption('settings/scrobbler');
  await backToMenu(page);
  assert.equal(await page.locator('#tab-snapshots').isVisible(),false);
  assert.equal(await page.locator('#cw-mobile-navigation [data-mobile-route="settings/providers"]').isVisible(),true);
  assert.equal(await page.locator('#cw-mobile-navigation [data-mobile-route="settings/scrobbler"]').count(),0);
  assert.equal(await page.locator('#cw-mobile-navigation [data-mobile-route="settings/sync"]').isVisible(),true);
  assert.equal(await page.locator('#tab-about-menu').isVisible(),false);
  await page.reload();
  await page.locator('#cw-mobile-more').click();
  for (const id of ['tab-main','tab-snapshots','tab-about-menu']) assert.equal(await page.locator(`#${id}`).isVisible(),false);
  assert.equal(await page.locator('#tab-tracker').isVisible(),true);
  await page.setViewportSize({width:1200,height:900});
  for (const id of ['tab-main','tab-tracker','tab-snapshots','tab-about-menu']) assert.equal(await page.locator(`#${id}`).isVisible(),true);
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: My Media Hub pins Overview without adding slots`, async t => {
  const {page} = await fixture(t,theme,false,'["tracker","settings/sync"]');
  const pinned = page.locator('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]');
  const extra = page.locator('#cw-mobile-navigation #tab-profile');
  await openSettings(page);
  assert.equal(await extra.getAttribute('href'), '/hub');
  const first = page.locator('#cw-mobile-shortcut-0');
  await first.focus();
  await page.keyboard.press('End');
  await page.keyboard.press('Enter');
  assert.equal(await first.inputValue(), 'profile/overview');
  assert.equal(await pinned.count(), 1);
  assert.equal(await extra.isVisible(), false);
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
  await page.waitForURL(url => url.pathname === '/hub');
  assert.equal(await pinned.getAttribute('aria-current'), 'page');
  assert.equal(await page.locator('#cw-mobile-more').evaluate(node=>node.classList.contains('is-current')), false);
  await page.evaluate(()=>location.hash='history');
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]').getAttribute('aria-current')==='page');
  assert.equal(await page.locator('#cw-mobile-more').evaluate(node=>node.classList.contains('is-current')), false);
  await openSettings(page);
  await first.selectOption('settings/providers');
  assert.equal(await pinned.count(), 0);
  assert.equal(await extra.count(), 1);
  await backToMenu(page);
  await extra.focus();
  await page.keyboard.press('Enter');
  await page.waitForURL(url => url.pathname === '/hub');
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-navigation #tab-profile')?.getAttribute('aria-current')==='page');
  assert.equal(await extra.getAttribute('aria-current'), 'page');
  assert.equal(await page.locator('#cw-mobile-more').getAttribute('aria-expanded'), 'false');
  assert.equal(await page.locator('#cw-mobile-more').evaluate(node=>node.classList.contains('is-current')), true);
});

for (const [saved, expected] of [
  ['["playlists","editor"]', ['playlists','editor']],
  ['["profile/overview","profile/overview"]', ['profile/overview','tracker']],
  ['["missing",null,17,{},"profile/overview"]', ['profile/overview','tracker']],
]) test(`saved shortcuts retain valid choices and remove duplicates: ${saved}`, async t => {
  const {page} = await fixture(t,'flat-dark',false,saved);
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a[data-mobile-route]').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)), ['main',...expected]);
});

test('My Media Hub is unavailable when the profile destination is absent', async t => {
  const {page} = await fixture(t,'flat-dark',false,'["profile/overview","editor"]',{profileAvailable:false});
  await openSettings(page);
  assert.equal(await page.locator('option[value="profile/overview"], [data-mobile-route="profile/overview"]').count(), 0);
  assert.deepEqual(await page.locator('#cw-mobile-bottom-nav a[data-mobile-route]').evaluateAll(nodes=>nodes.map(node=>node.dataset.mobileRoute)), ['main','editor','tracker']);
});

test('managed writers retain permitted shortcuts and routes from the media hub', async t => {
  const {page} = await fixture(t,'flat-dark',false,'["settings/providers","profile/overview"]',{managedWrite:true});
  await page.locator('#cw-mobile-bottom-nav [data-mobile-route="profile/overview"]').click();
  await page.waitForURL(url => url.pathname === '/hub');
  await openSettings(page);
  assert.deepEqual(await page.locator('#cw-mobile-shortcut-0 option').evaluateAll(nodes=>nodes.map(node=>node.value)), ['tracker','snapshots','playlists','editor','profile/overview']);
  for (const route of ['main','tracker']) assert.equal(await page.locator(`#cw-mobile-bottom-nav [data-mobile-route="${route}"]`).getAttribute('href'), `/?main=1#${route}`);
});
