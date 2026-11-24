/* tests/account-menu.browser.mjs */
/* CrossWatch - Shared account and primary navigation across shells, permissions and devices */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import './i18n-browser-setup.mjs';
import test, {before, after} from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium, webkit} from 'playwright';
import {i18nHead, localeRoute} from './i18n-test-helper.mjs';

const templates = JSON.parse(execFileSync('python', ['-c', `
import json, ui_frontend as ui
ui._managed_tracker_allowed = lambda user: bool((user or {}).get('tracker'))
users = {'admin': {'is_admin': True, 'username': 'admin'}, 'reader': {'is_admin': False, 'username': 'reader', 'permissions': {'write': False, 'watchlist': False, 'playback': False}}, 'writer': {'is_admin': False, 'username': 'writer', 'tracker': True, 'permissions': {'write': True, 'watchlist': True, 'playback': True}}}
print(json.dumps({name: {'main': ui.get_index_html(include_admin=name == 'admin', user=user), 'profile': ui.get_profile_html(user), 'hub': ui.get_hub_html(user)} for name, user in users.items()}))
`], {encoding:'utf8', maxBuffer:4*1024*1024}));
let browser;
const assets = new Map();
before(async () => { browser = await ({chromium, webkit})[process.env.CW_MENU_BROWSER || 'chromium'].launch({headless:true}); });
after(async () => { await browser?.close(); });

async function fixture(t, {shell='main', role='admin', theme='flat-dark', width=1440, height=900, avatar=''}={}) {
  const context = await browser.newContext({viewport:{width,height},hasTouch:width<=1024,isMobile:width<=760,reducedMotion:'reduce'});
  t.after(() => context.close());
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  const errors = [], writes = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  const html = await page.evaluate(({source,theme,avatar,head}) => {
    const doc = new DOMParser().parseFromString(source, 'text/html');
    doc.querySelectorAll('script').forEach(node => node.remove());
    doc.head.insertAdjacentHTML('afterbegin', head);
    doc.getElementById('cw-main-loading')?.remove();
    doc.documentElement.className = theme === 'original' ? 'cw-theme-original' : `cw-theme-${theme === 'flat-light' ? 'light' : 'dark'}`;
    if (theme !== 'original') doc.documentElement.dataset.cwTheme = theme;
    doc.getElementById(theme === 'original' ? 'cw-theme-flat-css' : 'cw-theme-original-css')?.remove();
    if (avatar) {
      const node = doc.getElementById('cw-nav-profile-avatar');
      const img = doc.createElement('img');
      img.src = avatar;
      img.alt = '';
      node.replaceChildren(img);
      node.dataset.cwAvatarUrl = avatar;
      node.classList.remove('material-symbols-rounded');
    }
    const experience = doc.createElement('script');
    experience.src = '/assets/helpers/mobile-experience.js';
    doc.body.append(experience);
    const script = doc.createElement('script');
    script.src = '/assets/helpers/account-menu.js';
    doc.body.append(script);
    const preferences = doc.createElement('script');
    preferences.src = '/assets/helpers/mobile-settings.js';
    doc.body.append(preferences);
    return '<!doctype html>' + doc.documentElement.outerHTML;
  }, {source:templates[role][shell], theme, avatar, head:i18nHead()});
  await page.route('https://account-menu.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    const locale = localeRoute(path);
    if (locale) return route.fulfill(locale);
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
  await page.goto(`https://account-menu.test/${shell === 'main' ? '' : shell}`);
  await page.evaluate(() => document.fonts.ready);
  return {page,writes};
}

test('Mobile Back unwinds settings and tools before closing More without leaving the page', async t => {
  const {page} = await fixture(t,{width:390});
  const panel = page.locator('#cw-mobile-navigation');
  const start = page.url();
  await page.locator('#cw-mobile-more').click();
  await panel.locator('#tab-tools').click();
  await panel.locator('#cw-mobile-widgets > summary').click();
  await page.goBack();
  await page.waitForFunction(() => document.querySelector('#cw-mobile-navigation').dataset.mobileView === 'tools');
  await page.goBack();
  await page.waitForFunction(() => document.querySelector('#cw-mobile-navigation').dataset.mobileView === 'main');
  await page.goBack();
  await page.waitForFunction(() => document.querySelector('#cw-mobile-more').getAttribute('aria-expanded') === 'false');
  assert.equal(page.url(), start);
  assert.equal(await page.evaluate(() => history.state?.cwMobileLayer), undefined);
  await page.locator('#cw-mobile-more').click();
  await panel.locator('#tab-tools').click();
  await page.locator('#cw-mobile-bottom-nav [data-mobile-route="tracker"]').click();
  await page.waitForURL('**/#tracker');
  assert.equal(await page.evaluate(() => history.state?.cwMobileLayer), undefined);
  await page.goBack();
  assert.equal(page.url(), start);
});

for (const theme of ['original','flat-dark','flat-light']) for (const shell of ['main','hub','profile']) test(`${theme}: ${shell} keeps compact Mobile settings in every menu`,async t=>{
  const {page} = await fixture(t,{theme,shell,width:390,height:844});
  await page.locator('#cw-mobile-more').click();
  const panel = page.locator('#cw-mobile-navigation');
  const footer = panel.locator('#cw-mobile-widgets > summary');
  assert.equal(await footer.isVisible(),true);
  assert.ok((await footer.boundingBox()).height <= 48);
  await panel.locator('.cw-mobile-sheet-close').click();
  await page.setViewportSize({width:740,height:360});
  await page.locator('#cw-mobile-more').click();
  if (shell !== 'hub') await panel.locator('#tab-tools').click();
  assert.equal(await footer.evaluate(node=>{
    const box=node.getBoundingClientRect();
    return box.y>=0 && box.bottom<=innerHeight && node.contains(document.elementFromPoint(box.x+box.width/2,box.y+box.height/2));
  }),true);
  await panel.locator('.cw-mobile-sheet-close').click();
  await page.setViewportSize({width:390,height:844});
  await page.locator('#cw-mobile-more').click();
  if (shell !== 'hub') await panel.locator('#tab-tools').click();
  await footer.click();
  assert.equal(await panel.getAttribute('data-mobile-view'),'settings');
  assert.equal(await panel.locator('.cw-mobile-sheet-heading strong').textContent(),'Mobile settings');
  assert.equal(await panel.locator('[data-mobile-haptics]').isVisible(),true);
  assert.equal(await panel.locator('#tab-tools').isVisible(),false);
  const haptics = panel.locator('[data-mobile-haptics]');
  if (await page.evaluate(()=>typeof navigator.vibrate === 'function')) {
    await haptics.uncheck();
    assert.equal(await page.evaluate(()=>localStorage.getItem('cw.mobile.haptics')),'0');
  } else {
    assert.equal(await haptics.isDisabled(),true);
    assert.equal(await haptics.isChecked(),false);
    assert.match(await haptics.locator('..').textContent(),/Not supported by this browser/);
  }
  await panel.locator('[data-mobile-shortcuts], .cw-mobile-shortcuts').waitFor();
  await panel.locator('#cw-mobile-shortcut-0').selectOption('playlists');
  assert.equal(await panel.getAttribute('data-mobile-view'),'settings');
  assert.equal(await page.locator('#cw-mobile-profile').getAttribute('href'),'/profile#account');
  if(process.env.CW_MENU_SCREENSHOTS) {
    await mkdir(process.env.CW_MENU_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-${shell}-mobile-settings.png`});
  }
  await panel.locator('.cw-mobile-sheet-back').click();
  assert.equal(await panel.getAttribute('data-mobile-view'),shell==='hub'?'main':'tools');
  assert.equal(await footer.isVisible(),true);
  assert.equal(await footer.evaluate(node=>document.activeElement===node),true);
  await panel.locator('.cw-mobile-sheet-close').click();
  assert.equal(await page.locator('#cw-mobile-more').getAttribute('aria-expanded'),'false');
  await page.locator('#cw-mobile-more').click();
  assert.equal(await panel.getAttribute('data-mobile-view'),'main');
  await page.setViewportSize({width:1440,height:900});
  assert.equal(await panel.locator('.cw-mobile-sheet-heading').isVisible(),false);
  assert.equal(await footer.isVisible(),false);
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: mobile Profile keeps the account avatar and glass navigation`,async t=>{
  const avatar = 'data:image/svg+xml,'+encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><rect width="40" height="40" fill="#8063ff"/><circle cx="20" cy="15" r="9" fill="#fff"/><path d="M4 40a16 16 0 0132 0" fill="#fff"/></svg>');
  const {page} = await fixture(t,{theme,width:390,height:844,avatar});
  const mobile = page.locator('#cw-mobile-profile-avatar');
  assert.equal(await mobile.locator('img').getAttribute('src'),avatar);
  await page.waitForFunction(()=>document.querySelector('#cw-mobile-profile-avatar img').naturalWidth>0);
  const glass = await page.locator('#cw-mobile-bottom-nav').evaluate(node=>({background:getComputedStyle(node).backgroundColor,blur:getComputedStyle(node).backdropFilter}));
  assert.match(glass.blur,/blur\(18px\)/);
  assert.match(glass.background,/0\.78/);
  await page.locator('#cw-mobile-more').click();
  if (process.env.CW_MENU_SCREENSHOTS) {
    await mkdir(process.env.CW_MENU_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-mobile-profile-glass.png`});
  }
  await page.keyboard.press('Escape');
  await page.evaluate(()=>{
    document.getElementById('cw-nav-profile-link').classList.add('is-viewing-as');
    CW.AccountMenu.setAvatarNode(document.getElementById('cw-nav-profile-avatar'),'');
  });
  assert.equal(await mobile.locator('img').getAttribute('src'),avatar);
  await page.evaluate(()=>{
    document.getElementById('cw-nav-profile-link').classList.remove('is-viewing-as');
    CW.AccountMenu.setAvatarUrl('');
  });
  assert.equal(await mobile.textContent(),'person');
  await page.evaluate(avatar=>CW.AccountMenu.setAvatarUrl(avatar),avatar);
  assert.equal(await mobile.locator('img').getAttribute('src'),avatar);
  await page.evaluate(()=>CW.AccountMenu.setAvatarUrl('/assets/missing-avatar.png'));
  await page.waitForFunction(()=>document.getElementById('cw-mobile-profile-avatar').textContent==='person');
  await page.setViewportSize({width:1440,height:900});
  await page.locator('#cw-nav-profile-link').waitFor({state:'visible'});
  assert.equal(await page.locator('#cw-mobile-bottom-nav').isVisible(),false);
});

for (const theme of ['original','flat-dark','flat-light']) {
  for (const shell of ['main','profile']) {
    for (const [width,height] of [[320,740],[390,844],[740,360],[768,1024],[1024,768],[1440,900]]) {
      test(`${theme}: ${shell} account menu at ${width}x${height}`, async t => {
        const {page} = await fixture(t, {theme,shell,width,height});
        const toggle = page.locator('#cw-nav-profile-link');
        if (width <= 760) {
          assert.equal(await toggle.isVisible(),false);
          const nav = page.locator('#cw-mobile-bottom-nav');
          assert.equal(await nav.locator(':scope > a, :scope > button').count(),5);
          for (const item of await nav.locator(':scope > a, :scope > button').all()) {
            assert.ok(await item.getAttribute('aria-label'));
            assert.ok((await item.boundingBox()).width >= 44);
            assert.equal(await item.locator('span:not([aria-hidden="true"])').count(),0);
          }
          await nav.getByRole('link',{name:'Profile',exact:true}).click();
          await page.waitForURL('https://account-menu.test/profile#account');
          assert.equal(await page.locator('#cw-mobile-profile').getAttribute('aria-current'),'page');
          return;
        }
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
        for (const item of await menu.getByRole('menuitem').all()) assert.ok((await item.boundingBox()).height >= (width <= 1024 ? 44 : 34));
        if (width === 1440) assert.ok(bounds.width <= 240 && bounds.height <= 450, JSON.stringify(bounds));
        const expectedCurrent = shell === 'profile' ? 'account' : null;
        assert.equal(await menu.locator('[aria-current]').count(), expectedCurrent ? 1 : 0);
        if (expectedCurrent) assert.equal(await menu.locator('[aria-current]').getAttribute('data-cw-profile-menu-action'), expectedCurrent);
        if (process.env.CW_MENU_SCREENSHOTS) {
          await mkdir(process.env.CW_MENU_SCREENSHOTS, {recursive:true});
          await menu.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-${shell}-${width}.png`});
          if (theme === 'flat-dark' && [390,768,1440].includes(width)) await page.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-${shell}-${width}-screen.png`});
        }
        await menu.locator('[data-cw-account-view="help"]').click();
        assert.equal(await menu.locator('[data-cw-account-panel="help"]').isVisible(), true);
        assert.equal(await page.evaluate(() => document.activeElement.dataset.cwAccountView), 'main');
        await page.keyboard.press('ArrowDown');
        assert.equal(await page.evaluate(() => document.activeElement.dataset.cwProfileMenuAction), 'help');
        await page.keyboard.press('Home');
        await page.keyboard.press('Enter');
        assert.equal(await menu.locator('[data-cw-account-panel="main"]').isVisible(), true);
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

for (const shell of ['main','hub']) test(`${shell}: mobile page tabs stay below the More menu`, async t => {
  const {page} = await fixture(t,{shell,width:390,height:844});
  await page.evaluate(shell => {
    let tabs = document.querySelector('.cw-profile-tabs');
    if (shell === 'main') {
      const tracker = document.getElementById('page-tracker');
      tracker.hidden = false;
      tracker.classList.remove('hidden');
      tracker.style.display = 'block';
      tracker.innerHTML = '<nav class="trk-navigation"><button type="button">Overview</button><button type="button">History</button></nav>';
      const content = document.createElement('main');
      content.style.cssText = 'padding-top:200px;min-height:1500px';
      content.append(tracker);
      document.querySelectorAll('body>main').forEach(node=>node.remove());
      document.querySelector('body>header').after(content);
      tabs = tracker.querySelector('.trk-navigation');
    }
    tabs.scrollIntoView();
    window.scrollBy(0,-260);
  },shell);
  await page.locator('#cw-mobile-more').click();
  const tabs = page.locator(shell === 'main' ? '#page-tracker .trk-navigation' : '.cw-profile-tabs');
  const covered = await page.locator('#cw-mobile-navigation .tab:visible').evaluateAll(nodes=>nodes.filter(node=>{
    const rect=node.getBoundingClientRect();
    return !node.contains(document.elementFromPoint(rect.x+rect.width/2,rect.y+rect.height/2));
  }).map(node=>node.textContent));
  assert.deepEqual(covered,[]);
  await page.keyboard.press('Escape');
  assert.equal(await page.locator('#cw-mobile-navigation').isVisible(),false);
  assert.equal(await tabs.isVisible(),true);
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: profile Settings cannot cover the account menu`, async t => {
  const {page} = await fixture(t, {shell:'profile',theme,width:1440,height:900});
  await page.evaluate(() => {
    document.querySelectorAll('.cw-profile-panel').forEach(panel=>panel.classList.toggle('active',panel.id==='profile-panel-account'));
    document.querySelectorAll('[data-profile-tab]').forEach(tab=>tab.classList.toggle('active',tab.dataset.profileTab==='account'));
    document.querySelector('#settings-profile').scrollIntoView();
    window.scrollBy(0,-160);
  });
  await page.locator('#cw-nav-profile-link').click();
  const menu = page.locator('#cw-profile-menu');
  const covered = await menu.getByRole('menuitem').evaluateAll(items=>items.filter(item=>{
    const rect=item.getBoundingClientRect();
    return !item.contains(document.elementFromPoint(rect.x+rect.width/2,rect.y+rect.height/2));
  }).map(item=>item.textContent));
  assert.deepEqual(covered, []);
  if (process.env.CW_MENU_SCREENSHOTS) await page.screenshot({path:`${process.env.CW_MENU_SCREENSHOTS}/${theme}-profile-settings.png`});
});

for (const shell of ['main','profile']) {
  test(`${shell}: More destinations use mobile tile styles and restore desktop tabs`, async t => {
    const {page} = await fixture(t,{shell,width:390,height:844});
    await page.evaluate(() => {
      window.navigationBuzzes = [];
      navigator.vibrate = duration => { window.navigationBuzzes.push(duration); return true; };
    });
    await page.locator('#cw-mobile-more').click();
    assert.deepEqual(await page.evaluate(() => window.navigationBuzzes), [30]);
    const menu=page.locator('#cw-mobile-navigation');
    assert.equal(await menu.locator('#tab-tools').isVisible(),true);
    assert.equal(await menu.locator('.cw-mobile-tool-destinations [data-mobile-route="events"]').isVisible(),false);
    for (const selector of ['#tab-settings','#tab-tools']) {
      const style=await menu.locator(selector).evaluate(node=>{
        const s=getComputedStyle(node);
        return {border:s.borderTopWidth,radius:s.borderTopLeftRadius,color:s.color,fill:s.webkitTextFillColor};
      });
      assert.equal(style.border,'0px');
      assert.equal(style.radius,'0px');
      assert.equal(style.color,style.fill);
    }
    await page.setViewportSize({width:1440,height:900});
    assert.equal(await page.locator('#tab-settings').evaluate(node=>getComputedStyle(node).borderTopWidth),'0px');
    await page.setViewportSize({width:390,height:844});
    await page.locator('#cw-mobile-more').click();
    assert.equal(await page.locator('#tab-settings').evaluate(node=>getComputedStyle(node).borderTopWidth),'0px');
    await page.evaluate(() => { window.navigationBuzzes = []; });
    await menu.locator('#tab-tools').click();
    assert.deepEqual(await page.evaluate(() => window.navigationBuzzes), [30]);
    assert.equal(await menu.locator('#cw-tools-menu').isVisible(),false);
    assert.equal(await menu.locator('#tab-settings').isVisible(),false);
    assert.deepEqual(await menu.locator('.cw-mobile-tool-destinations .tab').evaluateAll(nodes=>nodes.filter(node=>node.getClientRects().length).map(node=>node.dataset.mobileRoute || 'back')), ['events','logs','editor','analyzer','import_export','backups']);
    const backups=menu.locator('[data-mobile-route="backups"]');
    assert.equal(await backups.innerText().then(value=>value.includes('Backup & Restore')),true);
    assert.equal(await backups.evaluate(node=>node.scrollWidth<=node.clientWidth),true);
    assert.equal(await backups.getAttribute('href'),shell==='profile'?'/#backups':'#backups');
    await menu.locator('.cw-mobile-sheet-back').click();
    assert.deepEqual(await page.evaluate(() => window.navigationBuzzes), [30,30]);
    assert.equal(await page.locator('#cw-mobile-nav-toggle').getAttribute('aria-expanded'),'true');
    assert.equal(await menu.locator('#tab-tools').isVisible(),true);
    assert.equal(await menu.locator('.cw-mobile-tool-destinations [data-mobile-route="events"]').isVisible(),false);
    assert.equal(await menu.locator('#tab-editor').getAttribute('onclick'),"window.cwToolsMenuSelect('editor')");
    if(shell==='profile') assert.equal(await menu.locator('#cw-tools-menu').getAttribute('data-href-prefix'),'/');
    await page.evaluate(() => {
      document.querySelector('#cw-mobile-navigation').addEventListener('click', event => event.preventDefault());
    });
    await menu.locator('#tab-tools').click();
    await backups.click();
    assert.deepEqual(await page.evaluate(() => window.navigationBuzzes), [30,30,30,30]);
    await page.evaluate(() => { localStorage.setItem('cw.mobile.haptics','0'); });
    await page.locator('#cw-mobile-more').click();
    await menu.locator('#tab-tools').click();
    await menu.locator('.cw-mobile-sheet-back').click();
    assert.deepEqual(await page.evaluate(() => window.navigationBuzzes), [30,30,30,30]);
    await page.evaluate(() => { localStorage.setItem('cw.mobile.haptics','1'); });
    await page.setViewportSize({width:1440,height:900});
    await menu.locator('#tab-tools').evaluate(node => node.removeAttribute('onclick'));
    await menu.locator('#tab-tools').dispatchEvent('click');
    assert.deepEqual(await page.evaluate(() => window.navigationBuzzes), [30,30,30,30]);
  });

  test(`${shell}: profile settings and logout`, async t => {
    const {page,writes} = await fixture(t, {shell});
    const start = `https://account-menu.test/${shell === 'main' ? '' : shell}`;
    for (const [action,path] of Object.entries({account:'/profile#account',logout:'/login'})) {
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
      assert.equal(await page.locator('#cw-nav-profile-link').isVisible(),false);
      assert.equal(await page.locator('#cw-mobile-profile').getAttribute('href'),'/profile#account');
      for (const action of ['account','help','about','logout']) assert.equal(await page.locator(`[data-cw-profile-menu-action="${action}"]`).count(), 1);
      assert.equal(await page.locator('[data-cw-profile-menu-action="preferences"]').count(), 0);
      assert.equal(await page.locator('#tab-tracker').count(), role === 'writer' ? 1 : 0);
      assert.equal(await page.locator('#tab-profile').count(), 1);
      assert.equal(await page.locator('#tab-settings-menu').count(), 0);
      if (role === 'writer' && shell === 'profile') assert.equal(await page.locator('#tab-tracker').getAttribute('onclick'), "location.href='/?main=1#tracker'");
    });
  }
}

for (const shell of ['main','profile']) test(`${shell}: primary order and desktop overflow`, async t => {
  const {page} = await fixture(t,{shell,width:1440});
  assert.deepEqual(await page.locator('#cw-mobile-navigation > .tab, #cw-mobile-navigation > .cw-tabmenu:not(#cw-desktop-more) > .tab').evaluateAll(nodes=>nodes.map(node=>node.id)), ['tab-main','tab-tracker','tab-profile','tab-snapshots','tab-playlists','tab-tools','tab-settings']);
  assert.equal(await page.locator('#tab-about').count(),0);
  assert.equal(await page.locator('#tab-settings').getAttribute('aria-label'),'Settings');
  assert.equal(await page.locator('#tab-settings .cw-settings-icon').evaluate(node=>getComputedStyle(node).fontSize),'20px');
  await page.setViewportSize({width:1000,height:800});
  await page.locator('#cw-desktop-more-toggle').click();
  assert.deepEqual(await page.locator('#cw-desktop-more-menu [role="menuitem"]').evaluateAll(nodes=>nodes.map(node=>node.id)),['tab-snapshots','tab-playlists']);
  assert.equal(await page.locator('#tab-tools').isVisible(),true);
  await page.keyboard.press('Escape');
  assert.equal(await page.locator('#cw-desktop-more-menu').isVisible(),false);
  await page.setViewportSize({width:390,height:844});
  await page.locator('#cw-mobile-more').click();
  assert.equal(await page.locator('#tab-tools').isVisible(),true);
  assert.equal(await page.locator('#cw-desktop-more').isVisible(),false);
  await page.setViewportSize({width:1440,height:900});
  assert.equal(await page.locator('#tab-tools').isVisible(),true);
  await page.locator('#tab-profile').click();
  await page.waitForURL('https://account-menu.test/hub');
});

for (const shell of ['main','profile']) test(`${shell}: Help and About use local dialogs`, async t => {
  const {page} = await fixture(t,{shell});
  await page.evaluate(() => {
    window.openAbout = () => {};
    window.cwAboutMenuSelect = action => { window.lastAccountAction = action; };
  });
  for (const action of ['help','about']) {
    await page.locator('#cw-nav-profile-link').click();
    await page.locator('[data-cw-account-view="help"]').click();
    await page.locator(`[data-cw-profile-menu-action="${action}"]`).click();
    assert.equal(await page.evaluate(()=>window.lastAccountAction),action);
    assert.equal(await page.locator('#cw-profile-menu').isVisible(),false);
  }
});
