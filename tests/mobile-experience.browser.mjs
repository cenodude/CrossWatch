/* tests/mobile-experience.browser.mjs */
/* CrossWatch - Mobile sheet history, confirmation and keyboard behavior */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import './i18n-browser-setup.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium, webkit} from 'playwright';

async function fixture(t, theme = 'flat-dark') {
  const browser = await (process.env.CW_MOBILE_BROWSER === 'webkit' ? webkit : chromium).launch();
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:390,height:844},isMobile:true,hasTouch:true,reducedMotion:'reduce'});
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route('https://mobile.test/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith('/assets/')) return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-theme="${theme}" class="${theme === 'original' ? 'cw-theme-original' : theme === 'flat-light' ? 'cw-theme-light' : 'cw-theme-dark'}"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/assets/themes/tokens.css"><link rel="stylesheet" href="/assets/css/pages.css"><link rel="stylesheet" href="/assets/css/page-theme.css"><body><button id="open">Remove</button><input id="search"><nav id="cw-mobile-bottom-nav">Navigation</nav><dialog id="filter"><input id="filter-search"><button>Apply</button></dialog><script src="/assets/helpers/mobile-experience.js"></script><script src="/assets/helpers/dialog-focus.js"></script></body></html>`});
  });
  await page.goto('https://mobile.test/');
  return page;
}

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: mobile confirmation cancels on Back and confirms only after closing its history entry`, async t => {
  const page = await fixture(t, theme);
  await page.evaluate(() => {
    window.confirmed = null;
    document.querySelector('#open').onclick = () => { void CW.MobileUI.confirm({title:'Delete log entry?',message:'Watch history on your providers stays unchanged.',confirmLabel:'Delete'}).then(value => { window.confirmed = value; }); };
  });
  await page.locator('#open').click();
  await page.locator('.cw-page-confirm[open]').waitFor();
  const box = await page.locator('.cw-page-confirm').boundingBox();
  assert.ok(box.x >= 0 && box.x + box.width <= 390 && Math.abs(box.y + box.height - 844) < 2);
  if (process.env.CW_MOBILE_SCREENSHOTS) {
    await mkdir(process.env.CW_MOBILE_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_MOBILE_SCREENSHOTS}/${theme}-confirm.png`});
  }
  await page.goBack();
  await page.waitForFunction(() => window.confirmed === false);
  await page.locator('#open').click();
  await page.locator('.cw-page-confirm').getByRole('button',{name:'Delete',exact:true}).click();
  await page.waitForFunction(() => window.confirmed === true);
  assert.equal(await page.evaluate(() => history.state?.cwMobileLayer), undefined);
  assert.equal(await page.locator('dialog[open]').count(), 0);
});

test('Nested dialogs and rapid close/open preserve the underlying page history', async t => {
  const page = await fixture(t);
  await page.evaluate(() => {
    CW.MobileUI.openDialog(document.querySelector('#filter'));
    const first = document.querySelector('#filter');
    first.close();
    void CW.MobileUI.confirm({title:'Confirm',message:'Next action'});
  });
  await page.waitForFunction(() => !!history.state?.cwMobileLayer && !!document.querySelector('.cw-page-confirm[open]'));
  await page.locator('.cw-page-confirm').getByRole('button',{name:'Cancel',exact:true}).click();
  await page.waitForFunction(() => !history.state?.cwMobileLayer && !document.querySelector('dialog[open]'));
  assert.equal(page.url(),'https://mobile.test/');
});

test('Keyboard shrinks sheets and hides bottom navigation while preserving touch settings', async t => {
  const page = await fixture(t);
  await page.evaluate(() => {
    window.buzzes = [];
    navigator.vibrate = value => { buzzes.push(value); return true; };
    CW.MobileUI.openDialog(document.querySelector('#filter'));
    document.querySelector('#filter-search').focus();
    Object.defineProperty(visualViewport,'height',{configurable:true,get:() => 480});
    visualViewport.dispatchEvent(new Event('resize'));
  });
  await page.waitForFunction(() => document.documentElement.classList.contains('cw-mobile-keyboard'));
  assert.equal(await page.locator('#cw-mobile-bottom-nav').isVisible(), false);
  const box = await page.locator('#filter').boundingBox();
  assert.ok(box.y >= 0 && box.y + box.height <= 480);
  await page.evaluate(() => {
    localStorage.setItem('cw.mobile.haptics','0'); CW.MobileUI.feedback();
    localStorage.setItem('cw.mobile.haptics','1'); CW.MobileUI.feedback();
    delete visualViewport.height; document.querySelector('#filter-search').blur(); visualViewport.dispatchEvent(new Event('resize'));
  });
  await page.waitForFunction(() => !document.documentElement.classList.contains('cw-mobile-keyboard'));
  assert.deepEqual(await page.evaluate(() => buzzes),[20]);
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: shared tool dialogs fit phones and Back respects busy operations`, async t => {
  const page = await fixture(t, theme);
  await page.evaluate(async () => {
    const {ModalHost} = await import('/assets/js/modals/core/host.js');
    window.toolHost = new ModalHost();
    await toolHost.mount({mount(root) { root.innerHTML = '<h2>Pair settings</h2><label>Name <input value="Personal"></label><button>Save</button>'; }});
    toolHost.setOptions({dismissible:false});
  });
  const surface = page.locator('.cx-modal-shell');
  const box = await surface.boundingBox();
  assert.ok(box.x >= 0 && box.width <= 390 && Math.abs(box.y + box.height - 844) < 2, JSON.stringify(box));
  await page.goBack();
  await page.waitForFunction(() => history.state?.cwMobileLayer);
  assert.equal(await surface.isVisible(), true);
  await page.evaluate(() => toolHost.setOptions({dismissible:true}));
  await page.goBack();
  await surface.waitFor({state:'detached'});
  await page.waitForFunction(() => !history.state?.cwMobileLayer);
  assert.equal(new URL(page.url()).pathname, '/');
});

test('Back from an unsaved mapping asks before closing and keeps the underlying editor', async t => {
  const page = await fixture(t);
  await page.evaluate(() => {
    const dialog = document.querySelector('#filter');
    dialog.addEventListener('cancel', async event => {
      event.preventDefault();
      if (await CW.MobileUI.ask('Discard unsaved mapping corrections?')) dialog.close();
    });
    CW.MobileUI.openDialog(dialog);
  });
  await page.goBack();
  await page.locator('.cw-page-confirm[open]').waitFor();
  await page.locator('.cw-page-confirm').getByRole('button',{name:'Cancel',exact:true}).click();
  await page.locator('.cw-page-confirm').waitFor({state:'detached'});
  assert.equal(await page.locator('#filter').isVisible(), true);
  await page.goBack();
  await page.locator('.cw-page-confirm[open]').waitFor();
  await page.locator('.cw-page-confirm').getByRole('button',{name:'Continue',exact:true}).click();
  await page.waitForFunction(() => !document.querySelector('#filter').open && !history.state?.cwMobileLayer);
  assert.equal(new URL(page.url()).pathname, '/');
});

test('Desktop tool confirmations keep their native behavior', async t => {
  const page = await fixture(t);
  await page.setViewportSize({width:1440,height:900});
  await page.waitForFunction(() => !CW.MobileUI.mobile());
  page.once('dialog', dialog => dialog.accept());
  assert.equal(await page.evaluate(() => CW.MobileUI.ask('Delete this entry?')), true);
  assert.equal(await page.locator('.cw-page-confirm').count(), 0);
  assert.equal(await page.evaluate(() => !!history.state?.cwMobileLayer), false);
});

test('Mobile text prompts preserve typed values and cancel on Back', async t => {
  const page = await fixture(t);
  await page.evaluate(() => { window.value = undefined; void CW.MobileUI.prompt('Add user').then(value => { window.value = value; }); });
  await page.getByRole('textbox',{name:'Add user',exact:true}).fill('Pascal');
  await page.getByRole('button',{name:'Continue',exact:true}).click();
  await page.waitForFunction(() => window.value === 'Pascal' && !history.state?.cwMobileLayer);
  await page.evaluate(() => { window.value = undefined; void CW.MobileUI.prompt('Type RESET to continue').then(value => { window.value = value; }); });
  await page.goBack();
  await page.waitForFunction(() => window.value === null && !history.state?.cwMobileLayer);
});

test('Playlist edits remain open when discard is cancelled and close cleanly on Back', async t => {
  const page = await fixture(t);
  await page.route('https://mobile.test/api/**', route => {
    const path = new URL(route.request().url()).pathname;
    return route.fulfill({json:path.endsWith('/providers') ? {providers:[{provider:'PLEX',instance:'default',configured:true}]} : {endpoints:[],mappings:[],rulesets:[],activity:[],lists:[],resources:[]}});
  });
  await page.evaluate(() => { document.documentElement.dataset.tab = 'playlists'; document.body.insertAdjacentHTML('beforeend','<section id="page-playlists"></section>'); });
  await page.addScriptTag({url:'/assets/js/playlists.js'});
  await page.locator('[data-action="toggle-advanced"]').click();
  await page.locator('[data-action="endpoint-new"]').click();
  await page.locator('#pl-ep-name').fill('Weekend');
  await page.goBack();
  await page.locator('.cw-page-confirm[open]').waitFor();
  await page.locator('.cw-page-confirm').getByRole('button',{name:'Cancel',exact:true}).click();
  await page.locator('.cw-page-confirm').waitFor({state:'detached'});
  assert.equal(await page.locator('#pl-ep-name').inputValue(),'Weekend');
  await page.goBack();
  await page.locator('.cw-page-confirm[open]').waitFor();
  await page.locator('.cw-page-confirm').getByRole('button',{name:'Continue',exact:true}).click();
  await page.locator('.pl-modal').waitFor({state:'detached'});
  await page.waitForFunction(() => !history.state?.cwMobileLayer);
});

test('Enhanced selectors move with their sheet and Back closes the selector first', async t => {
  const page = await fixture(t);
  await page.addStyleTag({content:'.hidden{display:none!important}.cw-icon-select-menu{position:fixed;background:var(--cw-theme-surface);z-index:30060}'});
  await page.addScriptTag({url:'/assets/helpers/icon-select.js'});
  await page.evaluate(() => {
    const panel = document.createElement('div');
    panel.innerHTML = '<select id="tool-sort"><option value="title">Title</option><option value="date">Date</option></select>';
    document.body.append(panel);
    CW.IconSelect.enhance(panel.querySelector('select'));
    CW.MobileUI.sheet(panel,{title:'Filters'});
  });
  await page.locator('.cw-page-panel-sheet .cw-icon-select-btn').click();
  assert.equal(await page.locator('.cw-page-panel-sheet .cw-icon-select-menu:not(.hidden)').count(),1);
  await page.goBack();
  await page.waitForFunction(() => !document.querySelector('.cw-icon-select-menu:not(.hidden)'));
  assert.equal(await page.locator('.cw-page-panel-sheet').isVisible(),true);
  await page.locator('.cw-page-panel-sheet .cw-icon-select-btn').click();
  await page.locator('.cw-page-panel-sheet [data-value="date"]').click();
  await page.waitForFunction(() => document.querySelector('#tool-sort').value === 'date');
  await page.goBack();
  await page.locator('.cw-page-panel-sheet').waitFor({state:'detached'});
  assert.equal(await page.locator('#tool-sort').inputValue(),'date');
});

test('Leaving a tool detail clears its history before navigating to another page', async t => {
  const page = await fixture(t);
  await page.evaluate(() => {
    history.replaceState(null,'','#analyzer');
    window.closedDetail = false;
    CW.MobileUI.layer(() => { window.closedDetail = true; });
  });
  await page.evaluate(async () => {
    await CW.MobileUI.beforeNavigate();
    history.replaceState(history.state,'','#events');
  });
  assert.equal(await page.evaluate(() => window.closedDetail),true);
  assert.equal(new URL(page.url()).hash,'#events');
  assert.equal(await page.evaluate(() => !!history.state?.cwMobileLayer),false);
});

test('Following a page link from a mobile detail does not navigate back to the old route', async t => {
  const page = await fixture(t);
  await page.evaluate(() => {
    history.replaceState(null,'','#analyzer');
    document.body.insertAdjacentHTML('beforeend','<a id="leave-detail" href="#events">Events</a>');
    window.routes = [];
    CW.MobileUI.layer(() => {});
    window.addEventListener('hashchange', async () => {
      const route = location.hash;
      routes.push(route);
      await CW.MobileUI.beforeNavigate();
      history.replaceState(null,'',route);
      window.renderedRoute = route;
    });
  });
  await page.locator('#leave-detail').click();
  await page.waitForFunction(() => window.renderedRoute && !history.state?.cwMobileLayer);
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  assert.deepEqual(await page.evaluate(() => routes),['#events']);
  assert.equal(new URL(page.url()).hash,'#events');
});

for (const theme of ['original','flat-dark','flat-light']) for (const kind of ['route','webhook']) test(`${theme}: Scrobbler ${kind} sections fit phones and retain form values`, async t => {
  const page = await fixture(t, theme);
  page.setDefaultTimeout(7000);
  await page.setViewportSize({width:320,height:740});
  await page.route('**/api/**', route => route.fulfill({json:{profiles:[]}}));
  await page.evaluate(async ({kind,theme}) => {
    await Promise.all(['fonts/material-symbols-rounded','css/base','crosswatch','css/components','css/auth-providers','ui-shell',theme === 'original' ? 'themes/original-coverage' : 'themes/flat'].map(name => new Promise(resolve => {
      const link = document.createElement('link'); link.rel='stylesheet'; link.href=`/assets/${name}.css`; link.onload=resolve; document.head.append(link);
    })));
    const {ModalHost} = await import('/assets/js/modals/core/host.js');
    const module = await import(`/assets/js/modals/scrobbler-${kind}/index.js`);
    window.toolHost = new ModalHost();
    const entry = {id:'test',provider:'plex',provider_instance:'default',sink:'trakt',sink_instance:'default',filters:{},options:{}};
    await toolHost.mount(module, {route:entry,webhook:entry,overview:{eligible_sources:[{provider:'plex',profiles:[{instance:'default',label:'Home'}]}],destination_availability:[{provider:'trakt',profiles:[{instance:'default',label:'Personal'}]}]}});
  },{kind,theme});
  const picker = page.locator('.cw-mobile-section-picker select');
  await picker.waitFor();
  assert.equal(await page.locator('.scrm-nav-item').first().isVisible(),false);
  const options = await picker.locator('option').allTextContents();
  assert.ok(options.some(label => /Filters/.test(label)));
  for (let index = 0; index < options.length; index++) {
    await picker.selectOption(String(index));
    assert.equal(await page.locator('.scrm-panel.active').count(),1);
    const bounds = await page.locator('.cx-modal-shell').boundingBox();
    assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= 321,JSON.stringify(bounds));
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1),false);
  }
  await picker.selectOption('0');
  const input = page.locator('.scrm-panel.active input[type=text]').first();
  if (await input.count()) {
    await input.fill('Mobile draft');
    await picker.selectOption('1');
    await picker.selectOption('0');
    assert.equal(await input.inputValue(),'Mobile draft');
  }
  if (process.env.CW_MOBILE_SCREENSHOTS) {
    await mkdir(process.env.CW_MOBILE_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_MOBILE_SCREENSHOTS}/${theme}-scrobbler-${kind}.png`});
  }
  await page.goBack();
  await page.locator('.cx-modal-shell').waitFor({state:'detached'});
  await page.waitForFunction(() => !history.state?.cwMobileLayer);
});

for (const theme of ['original','flat-dark','flat-light']) test(`${theme}: pair configuration keeps feature controls and Back available on phones`, async t => {
  const page = await fixture(t,theme);
  page.setDefaultTimeout(7000);
  await page.setViewportSize({width:320,height:740});
  await page.route('**/api/**', route => route.fulfill({json:new URL(route.request().url()).pathname === '/api/sync/providers' ? ['PLEX','TRAKT'].map(name=>({name,label:name,features:{watchlist:true,history:true,ratings:true,progress:true}})) : {}}));
  await page.evaluate(async () => {
    const {ModalHost} = await import('/assets/js/modals/core/host.js');
    const module = await import('/assets/js/modals/pair-config/index.js');
    window.toolHost = new ModalHost();
    await toolHost.mount(module.default,{pairOrId:{id:'test',source:'PLEX',target:'TRAKT',features:{watchlist:{enable:true}}}});
  });
  const sections = page.locator('#cx-mobile-section');
  await sections.waitFor();
  const values = await sections.locator('option').evaluateAll(options=>options.map(option=>option.value));
  assert.ok(values.includes('watchlist'));
  for(const value of values) {
    await sections.selectOption(value);
    assert.equal(await page.locator('#cx-modal').evaluate(node=>node.__state.feature),value);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth > innerWidth + 1),false);
  }
  await page.goBack();
  await page.locator('.cx-modal-shell').waitFor({state:'detached'});
  await page.waitForFunction(()=>!history.state?.cwMobileLayer);
});
