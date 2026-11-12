/* tests/tracker-options.browser.mjs */
/* CrossWatch - Tracker options persistence, removal previews and responsive dialogs */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {execFileSync} from "node:child_process";
import {chromium} from "playwright";

const provider = execFileSync("python", ["-c", "from providers.auth._auth_CROSSWATCH import html; print(html())"], {encoding:"utf8"});
const css = ["fonts/material-symbols-rounded","themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","css/auth-providers","ui-shell"];

async function fixture(t, theme, width, settings = false) {
  const review = {state:"idle",scanned:0,titles:0,processed:0};
  const reviewRequests = [], undos = [];
  const changes = [1,2].map(n => ({batch:`auto-${n}`,automatic_group:"automatic-pass",feature:"notes",action:"note",count:1,at:"2026-11-01T12:00:00Z",undoable:true,items:[{key:`show:tmdb:${n}`,old_key:`show:tmdb:${n}`,row:{title:`Example ${n}`,type:"show"},before:{title:`Example ${n}`,status:"watching"},after:{title:`Example ${n}`,status:"completed"},fields:["status"]}]}));
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width,height:900},reducedMotion:"reduce"});
  page.setDefaultTimeout(6000);
  const errors = [], writes = [], automaticActions = [];
  const cfg = {app_auth:{enabled:true,username:"admin"},crosswatch:{connected:true,automatic_status:false,month_review:false,instances:{other:{connected:true,label:"Other"}}}};
  page.on("pageerror", e => errors.push(e.message));
  await page.addInitScript(() => {
    localStorage.setItem("cw.tracker.view.v3", JSON.stringify({view:"shows",viewSelectedAt:Date.now()}));
    localStorage.setItem("cw.ui.crosswatch.auth.instance.v1", "other");
  });
  await page.route("https://tracker-options.test/**", async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith("/assets/")) {
      try {return route.fulfill({body:await readFile(new URL(".." + path, import.meta.url)),contentType:path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : "text/javascript"});}
      catch {return route.fulfill({status:404,body:""});}
    }
    if (path.startsWith("/api/")) {
      let json = {ok:true};
      if (path === "/api/app-auth/status") json = {configured:true,enabled:true,is_admin:true};
      if (path === "/api/config") {
        if (route.request().method() === "POST") Object.assign(cfg, route.request().postDataJSON());
        json = cfg;
      }
      if (path === "/api/tracker/summary") json = {provider_instance:"default",instances:[{id:"default",label:"Default"}],tracker_options:{automatic_status:true,month_review:true},counts:{history:2},health:{},notes:{total:0,statuses:{},tags:[]}};
      if (path === "/api/tracker/shows") json = {total:1,shows:[{key:"tmdb:3",title:"Example show",year:2026,episodes:2,plays:2,last_season:1,last_episode:2,last_watched:"2026-11-01T12:00:00Z"}]};
      if (path === "/api/tracker/titles/remove") {
        const data = route.request().postDataJSON();
        writes.push(data);
        json = data.token ? {ok:true,batch:"removed",count:5002} : {counts:data.scope === "all" ? {history:5002,ratings:1,notes:1} : {history:5002},token:"preview-" + data.scope,count:5002,titles:1};
      }
      if (path === "/api/tracker/automatic-status/review") {reviewRequests.push(path); json = {review};}
      if (path === "/api/tracker/automatic-status") {
        const action = route.request().postDataJSON().action;
        automaticActions.push(action);
        if (action === "review-start") Object.assign(review,{state:"scanning",scanned:0,titles:0,processed:0});
        if (action === "review-cancel") review.state = "cancelled";
        json = action.startsWith("review-") ? {review} : {items:[{title:"Example show",before:"watching",after:"completed",reason:"All regular episodes watched; series ended"}],next:""};
      }
      if (path === "/api/tracker/changes") json = {total:changes.length,all:changes.length,changes};
      if (path === "/api/tracker/undo") {
        const batch = route.request().postDataJSON().batch;
        undos.push(batch);
        const change = changes.find(c => c.batch === batch);
        if (change) {change.undone = true; change.undoable = false;}
      }
      return route.fulfill({json});
    }
    const attrs = theme === "original" ? 'class="cw-theme-original"' : `data-cw-theme="${theme}"`;
    const settingsBody = `<input id="app_auth_username" value="admin" hidden><section id="page-settings" class="cw-themed-page"><div id="auth-providers"><div id="cw-auth-connection-overlay" class="cw-auth-overlay cw-connection-overlay" data-cw-connection-size="large"><div class="cw-auth-dialog" role="dialog" aria-modal="true" aria-label="CrossWatch Local Tracker"><div class="cw-auth-dialog-head"><h3>CrossWatch Local Tracker</h3></div><form id="cw-auth-provider-form" class="cw-auth-provider-form" onsubmit="return false">${provider}</form><button type="button" id="save-options" onclick="saveSettings()">Save</button></div></div></div></section><script>window.CW={AuthShared:{createProfileAdapter:()=>({getInstance:()=>localStorage.getItem('cw.ui.crosswatch.auth.instance.v1'),ensureUI:()=>{}})}};document.querySelectorAll('.cw-subpanel,.cw-subtile').forEach(el=>el.classList.toggle('active',el.dataset.sub==='settings'));document.querySelector('#sec-crosswatch').classList.add('open');document.querySelector('.cw-meta-provider-panel').classList.add('cw-connection-modal-panel');</script><script src="/assets/helpers/settings-save.js"></script><script src="/assets/auth/auth.crosswatch.js"></script>`;
    const trackerBody = `<section id="page-tracker" class="cw-themed-page"></section><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script>`;
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${attrs}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${[...css,theme === "original" ? "themes/original-coverage" : "themes/flat"].map(f=>`<link rel="stylesheet" href="/assets/${f}.css">`).join("")}</head><body><main id="layout" class="full">${settings ? settingsBody : trackerBody}</main></body></html>`});
  });
  await page.goto("https://tracker-options.test/");
  return {page,cfg,errors,writes,review,reviewRequests,undos,changes,automaticActions};
}

async function screenshot(page, name) {
  if (!process.env.CW_TRACKER_OPTIONS_SCREENSHOTS) return;
  await mkdir(process.env.CW_TRACKER_OPTIONS_SCREENSHOTS, {recursive:true});
  await page.screenshot({path:`${process.env.CW_TRACKER_OPTIONS_SCREENSHOTS}/${name}.png`});
}

for (const theme of ["original","flat-dark","flat-light"]) for (const width of [320,768,1440]) {
  test(`${theme}: history review and grouped undo at ${width}px`, async t => {
    const {page,review,reviewRequests,undos,changes,errors} = await fixture(t,theme,width);
    await page.locator('.trk-play').first().waitFor();
    await page.clock.install();
    const open = async () => {
      if (await page.locator('#trk-mobile-view').isVisible()) await page.locator('#trk-mobile-view').selectOption('automation');
      else {await page.locator('.trk-others summary').click(); await page.locator('[data-trk-action="automation"]').click();}
      await page.locator('[data-trk-review] button:not(:disabled)').waitFor();
    };
    await open();
    assert.equal(await page.locator('#trk-sheet-title').evaluate(el=>el.scrollWidth>el.clientWidth+1),false);
    const start = page.locator('[data-trk-sheet="review-start"]');
    await start.focus();
    await page.keyboard.press('Enter');
    await page.getByText('Finding titles · 0 history entries checked', {exact:true}).waitFor();
    Object.assign(review,{state:"processing",scanned:50000,titles:11000,processed:25});
    await page.clock.fastForward(5100);
    await page.locator('[data-trk-review] progress').waitFor();
    assert.equal(await page.locator('[data-trk-review] progress').getAttribute('value'), '25');
    const stop = page.locator('[data-trk-sheet="review-cancel"]');
    await stop.focus();
    review.processed = 50;
    await page.clock.fastForward(5100);
    await page.waitForFunction(() => document.querySelector('[data-trk-review] progress')?.value === 50);
    assert.equal(await stop.evaluate(el => document.activeElement === el), true);
    await screenshot(page,`review-${theme}-${width}`);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    await page.keyboard.press('Escape');
    const reads = reviewRequests.length;
    await page.clock.fastForward(15000);
    assert.equal(reviewRequests.length, reads);
    await open();
    await stop.waitFor();
    await stop.focus();
    await page.keyboard.press('Enter');
    await page.getByText('Review stopped · 50 titles reviewed', {exact:true}).waitFor();
    await start.click();
    await stop.waitFor();
    Object.assign(review,{state:"complete",scanned:0,titles:0,processed:0});
    await page.clock.fastForward(5100);
    await page.getByText('Review complete · 0 titles reviewed', {exact:true}).waitFor();
    await page.keyboard.press('Escape');
    await page.locator('[data-trk-action="changes"]').click();
    const summary = page.locator('[data-trk-auto-group] summary');
    await summary.waitFor();
    assert.equal(await page.locator('[data-trk-auto-group]').getAttribute('open'), null);
    await summary.focus();
    await page.keyboard.press('Enter');
    assert.equal(await page.locator('[data-trk-undo]').count(), 2);
    await screenshot(page,`automatic-changes-${theme}-${width}`);
    await page.locator('[data-trk-undo="auto-1"]').click();
    await page.waitForFunction(() => !document.querySelector('[data-trk-undo="auto-1"]') && document.querySelector('[data-trk-undo="auto-2"]'));
    assert.deepEqual(undos, ['auto-1']);
    assert.equal(await page.locator('[data-trk-undo="auto-2"]').count(), 1);
    await page.keyboard.press('Escape');
    changes.splice(0);
    await page.locator('[data-trk-action="changes"]').click();
    await page.getByText('No changes yet.', {exact:true}).waitFor();
    assert.deepEqual(errors, []);
  });

  test(`${theme}: independent tracker options at ${width}px`, async t => {
    const {page,cfg,errors} = await fixture(t,theme,width,true);
    const automatic = page.locator("#cw_tracker_automatic_status"), month = page.locator("#cw_tracker_month_review");
    await page.waitForFunction(() => document.querySelector('#cw_tracker_month_review').dataset.loaded === '1');
    assert.equal(await automatic.isChecked(), true);
    assert.equal(await month.isChecked(), true);
    await automatic.focus();
    await page.keyboard.press("Space");
    await month.uncheck();
    await page.evaluate(() => saveSettings());
    assert.equal(cfg.crosswatch.instances.other.automatic_status, false);
    assert.equal(cfg.crosswatch.instances.other.month_review, false);
    assert.equal(cfg.crosswatch.automatic_status, false);
    await page.reload();
    await page.waitForFunction(() => document.querySelector('#cw_tracker_month_review').dataset.loaded === '1');
    assert.equal(await automatic.isChecked(), false);
    assert.equal(await month.isChecked(), false);
    await automatic.check();
    await automatic.focus();
    await screenshot(page,`options-${theme}-${width}`);
    const bounds = await automatic.boundingBox();
    assert.ok(bounds.width >= 40 && bounds.width <= 48 && bounds.x >= 0 && bounds.x + bounds.width <= width);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    assert.deepEqual(errors, []);
  });

  test(`${theme}: title removal preview and automation at ${width}px`, async t => {
    const {page,writes,errors} = await fixture(t,theme,width);
    await page.locator('.trk-play').first().waitFor();
    await page.locator('[data-trk-layout="grid"]').click();
    await page.locator('[data-trk-pick="0"]').check();
    await page.locator('[data-trk-action="delete-picked"]').click();
    const confirm = page.locator('[data-trk-sheet="remove-confirm"]');
    await page.waitForFunction(() => !document.querySelector('[data-trk-sheet="remove-confirm"]')?.disabled);
    assert.equal(writes.length, 1);
    assert.deepEqual(writes[0],{provider_instance:"default",keys:["show:tmdb:3"],scope:"history"});
    await page.locator('[data-trk-sheet="remove-all"]').click();
    await page.waitForFunction(() => !document.querySelector('[data-trk-sheet="remove-confirm"]')?.disabled);
    assert.ok((await page.locator('.trk-sheet').textContent()).includes('Notes and statuses'));
    await screenshot(page,`remove-${theme}-${width}`);
    const bounds = await page.locator('.trk-sheet').boundingBox();
    assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= width);
    await confirm.click();
    await page.locator('.trk-sheet-wrap').waitFor({state:'hidden'});
    assert.deepEqual(writes.at(-1),{provider_instance:"default",keys:["show:tmdb:3"],scope:"all",token:"preview-all"});
    if (await page.locator('#trk-mobile-view').isVisible()) await page.locator('#trk-mobile-view').selectOption('automation');
    else {await page.locator('.trk-others summary').click(); await page.locator('[data-trk-action="automation"]').click();}
    await page.locator('[data-trk-auto-details="preview"] summary').click();
    await page.getByText('All regular episodes watched; series ended', {exact:true}).waitFor();
    await page.keyboard.press('Escape');
    await page.locator('.trk-sheet-wrap').waitFor({state:'hidden'});
    assert.deepEqual(errors, []);
  });
}


test("Automatic statuses starts simply and loads a paged preview only on request", async t => {
  const {page,automaticActions,errors} = await fixture(t,'flat-dark',1440);
  await page.locator('.trk-play').first().waitFor();
  await page.locator('.trk-others summary').click();
  await page.locator('[data-trk-action="automation"]').click();
  await page.getByRole('button',{name:'Update statuses',exact:true}).waitFor();
  const preview = page.locator('[data-trk-auto-details="preview"]');
  assert.equal(await preview.getAttribute('open'),null);
  assert.equal(await page.locator('[data-trk-auto-details="help"]').getAttribute('open'),null);
  assert.deepEqual(automaticActions,[]);
  await screenshot(page,'automation-simple');
  const previews = [];
  await page.route('**/api/tracker/automatic-status', route => {
    const data = route.request().postDataJSON();
    if (data.action !== 'preview') return route.fallback();
    previews.push(data.after);
    return route.fulfill({json:{items:[{title:data.after ? 'Second title' : 'First title',before:'watching',after:'completed',reason:'All episodes watched',skipped:false}],next:data.after ? '' : 'next-page'}});
  });
  await preview.locator('summary').focus();
  await page.keyboard.press('Enter');
  await page.getByText('First title',{exact:true}).waitFor();
  assert.equal(await page.locator('.trk-auto-summary').textContent(),'1 title \u00b7 1 suggested change');
  assert.deepEqual(previews,['']);
  await page.locator('[data-trk-sheet="auto-next"]').click();
  await page.getByText('Second title',{exact:true}).waitFor();
  await page.locator('[data-trk-sheet="auto-prev"]').click();
  await page.getByText('First title',{exact:true}).waitFor();
  assert.deepEqual(previews,['','next-page','']);
  await preview.locator('summary').click();
  await preview.locator('summary').click();
  assert.deepEqual(previews,['','next-page','']);
  await screenshot(page,'automation-preview');
  await page.getByRole('button',{name:'Update this batch',exact:true}).click();
  await page.locator('.trk-sheet-wrap').waitFor({state:'hidden'});
  assert.deepEqual(automaticActions,['recalculate']);
  assert.deepEqual(errors,[]);
});
