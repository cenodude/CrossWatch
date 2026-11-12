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
        json = action === "apply" ? {updated:route.request().postDataJSON().items.length,items:route.request().postDataJSON().items.map(item=>({...item,title:"First title",batch:"applied",reason:"Movie watched"}))} : action.startsWith("review-") ? {review} : {items:[{key:"show:tmdb:3",title:"Example show",before:"watching",after:"completed",reason:"All regular episodes watched; series ended"}],next:""};
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
    const refreshed = page.waitForRequest(req=>req.url().includes("/api/tracker/summary"));
    await page.clock.fastForward(5100);
    await page.getByText('Review complete · 0 titles reviewed', {exact:true}).waitFor();
    await refreshed;
    assert.equal(await page.getByRole('button',{name:'View changes',exact:true}).count(),0);
    assert.equal(await page.getByText('Preview titles',{exact:true}).count(),0);
    assert.equal(await page.locator('#trk-sheet-title').evaluate(el=>getComputedStyle(el).fontWeight),'500');
    await page.getByRole('button',{name:'Done',exact:true}).click();
    await page.locator('[data-trk-action="changes"]').click();
    const summary = page.locator('[data-trk-auto-group] summary');
    await summary.waitFor();
    assert.equal(await page.locator('[data-trk-auto-group]').getAttribute('open'), null);
    await summary.focus();
    await page.keyboard.press('Enter');
    assert.equal(await page.locator('[data-trk-undo]').count(), 2);
    assert.equal(await page.locator('[data-trk-undo] span').last().evaluate(el=>getComputedStyle(el).fontWeight),'400');
    assert.equal(await page.locator('.trk-change-text').first().evaluate(el=>getComputedStyle(el).fontWeight),'400');
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
    const automatic = page.locator("#cw_tracker_automatic_status"), month = page.locator("#cw_tracker_month_review"), wrapped = page.locator("#cw_tracker_wrapped");
    await page.waitForFunction(() => document.querySelector('#cw_tracker_month_review').dataset.loaded === '1');
    assert.equal(await automatic.isChecked(), true);
    assert.equal(await month.isChecked(), true);
    assert.equal(await wrapped.isChecked(), true);
    await automatic.focus();
    await page.keyboard.press("Space");
    await month.uncheck();
    await page.evaluate(() => saveSettings());
    assert.equal(cfg.crosswatch.instances.other.automatic_status, false);
    assert.equal(cfg.crosswatch.instances.other.month_review, false);
    assert.notEqual(cfg.crosswatch.instances.other.wrapped, false);
    assert.equal(cfg.crosswatch.automatic_status, false);
    await page.reload();
    await page.waitForFunction(() => document.querySelector('#cw_tracker_month_review').dataset.loaded === '1');
    assert.equal(await automatic.isChecked(), false);
    assert.equal(await month.isChecked(), false);
    assert.equal(await wrapped.isChecked(), true);
    await wrapped.uncheck();
    await month.check();
    await page.evaluate(() => saveSettings());
    assert.equal(cfg.crosswatch.instances.other.wrapped, false);
    assert.equal(cfg.crosswatch.instances.other.month_review, true);
    assert.equal(cfg.crosswatch.wrapped, undefined);
    await page.reload();
    await page.waitForFunction(() => document.querySelector("#cw_tracker_wrapped").dataset.loaded === "1");
    assert.equal(await wrapped.isChecked(), false);
    assert.equal(await month.isChecked(), true);
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
    await page.getByRole('button',{name:'Update statuses',exact:true}).waitFor();
    assert.equal(await page.getByText('Preview titles',{exact:true}).count(),0);
    await page.keyboard.press('Escape');
    await page.locator('.trk-sheet-wrap').waitFor({state:'hidden'});
    assert.deepEqual(errors, []);
  });
}


test("Automatic statuses offers one update action without fetching previews", async t => {
  const {page,automaticActions,errors} = await fixture(t,"flat-dark",1440);
  await page.locator('.trk-play').first().waitFor();
  await page.locator('.trk-others summary').click();
  await page.locator('[data-trk-action="automation"]').click();
  await page.getByRole('button',{name:'Update statuses',exact:true}).waitFor();
  assert.equal(await page.getByText('Preview titles',{exact:true}).count(),0);
  assert.equal(await page.getByRole('button',{name:'View changes',exact:true}).count(),0);
  assert.deepEqual(automaticActions,[]);
  await page.getByRole('button',{name:'Update statuses',exact:true}).click();
  await page.getByRole('button',{name:'Stop update',exact:true}).waitFor();
  assert.deepEqual(automaticActions,['review-start']);
  assert.deepEqual(errors,[]);
});

test("Completion suggestion applies directly from title editing and survives reopening", async t => {
  const {page,errors} = await fixture(t,"flat-dark",1440);
  const noteWrites = [];
  let note = {type:"show",title:"Example show",status:"watching"};
  const title = {title:"Example show",year:2026,ids:{tmdb:"3"},art:{kind:"tv",tmdb:"3"}};
  await page.route('**/api/tracker/title?*', route => route.fulfill({json:{title,show:true,show_key:"tmdb:3",note_key:"show:tmdb:3",note,status_control:{origin:"existing",manual:false},plays:[{key:"episode:1",feature:"history",type:"episode",title:"Example show",season:1,episode:1}],ratings:[],watchlist:[],collection:[],progress:[],attention:[],seasons:[]}}));
  await page.route('**/api/tracker/show-progress?*', route => route.fulfill({json:{available:true,completed:true,watched:1,total:1,seasons:[]}}));
  await page.route('**/api/tracker/note', async route => {
    const body = route.request().postDataJSON();
    noteWrites.push(body);
    note = {...note,status:body.status,finished:body.finished};
    await route.fulfill({json:{ok:true,batch:"completed",note}});
  });
  await page.locator('.trk-play').first().click();
  await page.locator('[data-trk-detail="complete"]').waitFor();
  await page.locator('[data-trk-detail="edit-title"]').click();
  const save = page.getByRole('button',{name:'Save changes',exact:true});
  assert.equal(await save.isDisabled(),true);
  await page.locator('[data-trk-te="title"]').fill('Changed title');
  assert.equal(await save.isEnabled(),true);
  await page.locator('[data-trk-te="title"]').fill('Example show');
  assert.equal(await save.isDisabled(),true);
  await page.locator('[data-trk-sheet="complete"]').focus();
  await page.keyboard.press('Enter');
  await page.waitForFunction(() => document.querySelector('.trk-status-btn')?.textContent.includes('Completed'));
  assert.equal(noteWrites.length,1);
  assert.equal(noteWrites[0].status,'completed');
  assert.equal(noteWrites[0].status_manual,true);
  assert.equal(noteWrites[0].key,'show:tmdb:3');
  assert.equal(await page.locator('[data-trk-detail="complete"]').count(),0);
  await page.locator('[data-trk-detail="edit-title"]').click();
  assert.equal(await page.locator('[data-trk-sheet="complete"]').count(),0);
  assert.equal(await save.isDisabled(),true);
  assert.deepEqual(errors,[]);
});


test("Enabling automatic status keeps notes open and confirms the saved setting", async t => {
  const {page,errors} = await fixture(t,"flat-dark",1440);
  let control = {origin:"manual",manual:true};
  let status = "watching", attempts = 0;
  const title = {title:"Example show",year:2026,ids:{tmdb:"3"},art:{kind:"tv",tmdb:"3"}};
  await page.route('**/api/tracker/title?*', route => route.fulfill({json:{title,show:true,show_key:"tmdb:3",note_key:"show:tmdb:3",note:{type:"show",title:"Example show",status},status_control:control,plays:[],ratings:[],watchlist:[],collection:[],progress:[],attention:[],seasons:[]}}));
  await page.route('**/api/tracker/automatic-status', route => {
    const body = route.request().postDataJSON();
    assert.equal(body.action,'enable-title');
    assert.equal(body.key,'show:tmdb:3');
    attempts++;
    if (attempts === 1) return route.fulfill({status:409,json:{error:'sync_running'}});
    control = {origin:"automatic",manual:false,reason:"All regular episodes watched; series ended"};
    status = "completed";
    return route.fulfill({json:{ok:true,batch:"enabled"}});
  });
  await page.locator('.trk-play').first().click();
  await page.locator('[data-trk-detail="notes"]').click();
  await page.locator('[data-trk-note="notes"]').fill('Keep my unsaved note');
  await page.getByRole('button',{name:'Use automatic status',exact:true}).click();
  await page.locator('.trk-sheet [role="alert"]').waitFor();
  assert.equal(await page.locator('[data-trk-form="notes"]').isVisible(),true);
  await page.getByRole('button',{name:'Use automatic status',exact:true}).click();
  await page.getByRole('button',{name:'Automatic status is on',exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'Automatic status is on',exact:true}).isDisabled(),true);
  assert.equal(await page.locator('[data-trk-form="notes"]').isVisible(),true);
  assert.equal(await page.locator('[data-trk-note="notes"]').inputValue(),'Keep my unsaved note');
  assert.equal(await page.locator('[data-trk-note-status="completed"]').getAttribute('aria-pressed'),'true');
  assert.match(await page.locator('[data-trk-form="notes"] [role="status"]').textContent(),/No extra Save is needed/);
  await page.getByRole('button',{name:'Cancel',exact:true}).click();
  await page.locator('[data-trk-detail="notes"]').click();
  assert.equal(await page.getByRole('button',{name:'Automatic status is on',exact:true}).isDisabled(),true);
  assert.equal(attempts,2);
  assert.deepEqual(errors,[]);
});


test("Older titles use automatic status by default and can still be set manually", async t => {
  const {page,errors,automaticActions} = await fixture(t,"flat-dark",1440);
  const title = {title:"Example show",year:2026,ids:{tmdb:"3"},art:{kind:"tv",tmdb:"3"}};
  await page.route('**/api/tracker/title?*', route => route.fulfill({json:{title,show:true,show_key:"tmdb:3",note_key:"show:tmdb:3",note:{type:"show",title:"Example show",status:"watching"},status_control:{origin:"existing",manual:false},plays:[],ratings:[],watchlist:[],collection:[],progress:[],attention:[],seasons:[]}}));
  await page.locator('.trk-play').first().click();
  await page.locator('[data-trk-detail="notes"]').click();
  assert.equal(await page.getByRole('button',{name:'Automatic status is on',exact:true}).isDisabled(),true);
  assert.equal(await page.getByText('Older status: enable automatic updates to include this title.',{exact:true}).count(),0);
  assert.deepEqual(automaticActions,[]);
  await page.locator('[data-trk-note-status="watching"]').click();
  assert.equal(await page.getByRole('button',{name:'Use automatic status',exact:true}).isEnabled(),true);
  const saved = page.waitForRequest(request=>request.url().endsWith('/api/tracker/note'));
  await page.getByRole('button',{name:'Save',exact:true}).click();
  const request = await saved;
  assert.equal(request.postDataJSON().status_manual,true);
  assert.equal(request.postDataJSON().status,'watching');
  assert.deepEqual(errors,[]);
});
