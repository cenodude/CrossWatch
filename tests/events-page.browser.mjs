/* tests/events-page.browser.mjs */
/* CrossWatch - Events deep links and filter reset browser tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

for (const scenario of ['clear_review_filter', 'clear_profile_filter', 'stale_overview', 'merged_playback']) {
  test(`activity regression: ${scenario}`, async t => {
    const browser = await chromium.launch({headless: true});
    t.after(() => browser.close());
    const page = await browser.newPage();
    page.setDefaultTimeout(10000);
    const queries = [], errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const base = {id: 100, domain: 'scrobble', feature: 'scrobble', status: 'running', severity: 'info', title: 'Example', event_count: 1, first_event_at: 100, last_event_at: 100, source_provider: 'PLEX', source_instance: 'default', destination_provider: 'TRAKT', destination_instance: 'default'};
    const second = {...base, id: 101, destination_provider: 'CROSSWATCH', last_event_at: 200, status: 'completed', severity: 'success'};
    await page.route('http://regression.test/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname.startsWith('/assets/')) return route.fulfill({body: await readFile(new URL('..' + url.pathname, import.meta.url)), contentType: url.pathname.endsWith('.css') ? 'text/css' : 'text/javascript'});
      let json = {ok: true, items: [], pairs: []};
      if (url.pathname === '/api/user-profiles') json = {items: [{id: 'P01', label: 'Family'}]};
      if (url.pathname === '/api/events/groups') {
        queries.push(Object.fromEntries(url.searchParams));
        const empty = scenario === 'clear_review_filter' && url.searchParams.get('visibility') === 'acknowledged' || scenario === 'clear_profile_filter' && url.searchParams.get('user_profile') === 'P01';
        const items = empty ? [] : [scenario === 'merged_playback' ? {...second, id: 100, destinations: [base, second]} : base];
        json = {items, total: items.length};
      }
      if (/\/api\/events\/groups\/10[01]$/.test(url.pathname)) {
        const isSecond = url.pathname.endsWith('/101');
        const group = {...(isSecond ? second : base), status: 'completed', severity: 'success', last_event_at: 200};
        const detail = scenario === 'merged_playback' ? {progress: isSecond ? 100 : 30, playback: [[isSecond ? 110 : 100, 'start', 0], [isSecond ? 200 : 150, 'stop', isSecond ? 100 : 30]]} : {progress: 100};
        json = {ok: true, group, events: [{...group, id: isSecond ? 2 : 1, group_id: group.id, event_type: 'scrobble_playback', created_at: isSecond ? 110 : 100, detail}]};
      }
      if (url.pathname.startsWith('/api/')) return route.fulfill({json});
      return route.fulfill({contentType: 'text/html', body: '<!doctype html><html data-tab="events"><head><link rel="stylesheet" href="/assets/css/pages.css"><link rel="stylesheet" href="/assets/css/page-theme.css"></head><body><main id="page-events"></main></body></html>'});
    });
    await page.goto('http://regression.test');
    await page.evaluate(async scenario => {
      window.CW = {AuthState: {read: () => ({isAdmin: true})}, OverviewProfile: scenario === 'clear_profile_filter' ? {id: 'P01', label: 'Family'} : null};
      const view = (await import('/assets/js/events/index.js')).default;
      await view.mount(document.querySelector('#page-events'), {domain: 'scrobble', visibility: scenario === 'clear_review_filter' ? 'acknowledged' : 'all'});
    }, scenario);
    if (scenario.startsWith('clear_')) {
      await page.locator('#ev-empty-reset').click();
      await page.waitForFunction(() => document.querySelector('#ev-count').textContent === '1 activity');
      assert.equal(queries.at(-1).visibility, 'all');
      assert.equal(queries.at(-1).user_profile, 'all');
    } else {
      await page.locator('#ev-detail .ev-kv').first().waitFor();
      assert.match(await page.locator('[data-pane="details"]').innerText(), /Status\s*Watched/);
      assert.match(await page.locator('[data-pane="details"]').innerText(), /Progress\s*100%/);
      const raw = JSON.parse(await page.locator('[data-pane="raw"] pre').textContent());
      assert.equal(raw.group.last_event_at, 200);
      if (scenario === 'merged_playback') {
        await page.locator('#ev-detail .ev-tab[data-tab="timeline"]').click();
        assert.equal(await page.locator('[data-pane="timeline"] .ev-tl').count(), 4);
        assert.match(await page.locator('[data-pane="timeline"] .ev-tl').first().innerText(), /100%.*CROSSWATCH/s);
        assert.match(await page.locator('[data-pane="timeline"]').innerText(), /TRAKT/);
      }
    }
    assert.deepEqual(errors, []);
  });
}

test('activity links show their sync thread and cloud filters reset to all types',async t=>{
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width:1440,height:960}});
  const queries=[],errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  const group={id:42,domain:'sync',operation:'run',status:'completed',summary:'Sync run completed successfully, PLEX → TRAKT (Watchlist)',event_count:15,first_event_at:1794568000,last_event_at:1794568010};
  const cloud={id:43,domain:'sync',operation:'read',status:'completed',summary:'Plex cloud history read',event_count:1,first_event_at:1794568000,last_event_at:1794568010};
  await page.route('http://events.test/**',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) return route.fulfill({body:await readFile(new URL('..'+url.pathname,import.meta.url)),contentType:url.pathname.endsWith('.css')?'text/css':'text/javascript'});
    if(url.pathname.startsWith('/api/')) {
      queries.push(Object.fromEntries(url.searchParams));
      let json={ok:true,items:[],pairs:[]};
      if(url.pathname==='/api/events/tree') {
        const rows=url.searchParams.has('since')||url.searchParams.get('q')?[]:url.searchParams.get('event_type')==='cloud_history_read'?[cloud]:[group,cloud];
        json={items:rows,total:rows.length};
      }
      if(url.pathname==='/api/events/groups/42') json={group,context:{},events:[{id:1,event_type:'sync_run_finished',created_at:1794568010,run_id:'run-one'}]};
      return route.fulfill({json});
    }
    return route.fulfill({contentType:'text/html',body:'<!doctype html><html data-tab="events"><head><meta charset="utf-8"><link rel="stylesheet" href="/assets/themes/tokens.css"><link rel="stylesheet" href="/assets/css/pages.css"><link rel="stylesheet" href="/assets/css/page-theme.css"><style>body{margin:20px;background:#080a0d;font-family:Arial,sans-serif}*{box-sizing:border-box}.material-symbols-rounded{font-size:0!important}</style></head><body><main id="page-events"></main></body></html>'});
  });
  await page.goto('http://events.test/#events?groupId=42');
  await page.evaluate(async()=>{
    localStorage.setItem('cw.events.range','24h');
    localStorage.setItem('cw.events.visibility','open');
    window.CW={AuthState:{read:()=>({isAdmin:true})}};
    window.eventsView=(await import('/assets/js/events/index.js')).default;
    await window.eventsView.mount(document.querySelector('#page-events'),{groupId:'42',domain:'sync',returnTo:'/profile'});
  });
  assert.ok(queries.some(q=>q.domain==='sync'&&q.visibility==='all'&&!q.since&&!q.event_type));
  assert.equal(await page.locator('#ev-list .ev-row[data-id="42"].sel').count(),1);
  assert.match(await page.locator('#ev-detail .ev-dtitle').innerText(),/Sync run completed successfully/);
  assert.equal(await page.locator('#ev-detail .ev-scards').count(),0);
  assert.equal(await page.locator('#ev-mf-summary').isVisible(),false);
  await page.locator('#ev-more').click();
  await page.locator('#ev-filters .ev-dd-btn').filter({hasText:'All types'}).click();
  await page.locator('.ev-dd-item[data-v="cloud_history_read"]').click();
  await page.waitForFunction(()=>document.querySelector('#ev-count').textContent==='1 activity');
  assert.equal(await page.locator('#ev-list .ev-row[data-id="42"]').count(),0);
  assert.equal(await page.locator('#ev-detail .ev-dtitle').count(),0);
  await page.locator('#ev-more').click();
  assert.equal(await page.locator('#ev-mf-summary').isVisible(),true);
  assert.equal(await page.locator('#ev-clear-hidden').count(),0);
  await page.locator('#ev-chip-clear').click();
  await page.waitForFunction(()=>document.querySelector('#ev-count').textContent==='2 activities');
  assert.equal(queries.at(-1).event_type,undefined);
  await page.locator('#ev-list .ev-row[data-id="42"]').click();
  await page.locator('#ev-detail .ev-dtitle').waitFor();
  if(process.env.CW_EVENTS_SCREENSHOTS) {
    await mkdir(process.env.CW_EVENTS_SCREENSHOTS,{recursive:true});
    await page.screenshot({path:`${process.env.CW_EVENTS_SCREENSHOTS}/sync-activity.png`,fullPage:true});
  }
  await page.evaluate(async()=>{
    await window.eventsView.mount(document.querySelector('#page-events'),{runId:'run-one',domain:'sync'});
  });
  await page.locator('#ev-filters .ev-dd-btn').filter({hasText:'All types'}).click();
  await page.locator('.ev-dd-item[data-v="cloud_history_read"]').click();
  await page.locator('#ev-q').fill('missing item');
  await page.locator('#ev-empty-reset').click();
  await page.waitForFunction(()=>document.querySelector('#ev-count').textContent==='2 activities');
  assert.equal(queries.at(-1).run_id,undefined);
  assert.equal(queries.at(-1).event_type,undefined);
  assert.equal(queries.at(-1).q,undefined);
  assert.deepEqual(errors,[]);
});

test('captures and backups have their own tabs and detail',async t=>{
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width:1440,height:960}});
  const queries=[],errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  const capture={id:51,domain:'capture',feature:'capture',status:'failed',severity:'error',summary:'Watchlist capture, Plex failed',event_count:1,first_event_at:1794568000,last_event_at:1794568000,source_provider:'PLEX',source_instance:'default'};
  const backup={id:52,domain:'backup',feature:'backup',status:'completed',summary:'Full backup',event_count:1,first_event_at:1794567000,last_event_at:1794567000};
  const captureEvent={id:7,domain:'capture',feature:'capture',event_type:'capture_failed',severity:'error',created_at:1794568000,source_provider:'PLEX',source_instance:'default',source_kind:'scheduler',title:'Watchlist capture, Plex failed',reason:'Provider not configured',detail:JSON.stringify({capture_feature:'watchlist',items:0})};
  await page.route('http://events.test/**',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')) return route.fulfill({body:await readFile(new URL('..'+url.pathname,import.meta.url)),contentType:url.pathname.endsWith('.css')?'text/css':'text/javascript'});
    if(url.pathname.startsWith('/api/')) {
      queries.push({path:url.pathname,...Object.fromEntries(url.searchParams)});
      let json={ok:true,items:[],pairs:[]};
      if(url.pathname==='/api/events/groups') json=url.searchParams.get('domain')==='backup'?{items:[backup],total:1}:{items:[capture],total:1};
      if(url.pathname==='/api/events/groups/51') json={group:capture,events:[captureEvent]};
      return route.fulfill({json});
    }
    return route.fulfill({contentType:'text/html',body:'<!doctype html><html data-tab="events"><head><meta charset="utf-8"><link rel="stylesheet" href="/assets/themes/tokens.css"><link rel="stylesheet" href="/assets/css/pages.css"><link rel="stylesheet" href="/assets/css/page-theme.css"><style>body{margin:20px;background:#080a0d;font-family:Arial,sans-serif}*{box-sizing:border-box}.material-symbols-rounded{font-size:0!important}</style></head><body><main id="page-events"></main></body></html>'});
  });
  await page.goto('http://events.test/#events');
  await page.evaluate(async()=>{
    window.CW={AuthState:{read:()=>({isAdmin:true})}};
    window.eventsView=(await import('/assets/js/events/index.js')).default;
    await window.eventsView.mount(document.querySelector('#page-events'),{domain:'capture'});
  });
  await page.locator('#ev-list .ev-row[data-id="51"]').waitFor();
  assert.ok(queries.some(q=>q.path==='/api/events/groups'&&q.domain==='capture'));
  assert.equal(queries.some(q=>q.path==='/api/events/tree'),false);
  assert.match(await page.locator('.ev-vtab.on').innerText(),/Captures/);
  assert.equal(await page.locator('#ev-list .ev-row[data-id="52"]').count(),0);
  await page.locator('#ev-list .ev-row[data-id="51"]').click();
  await page.locator('#ev-detail .ev-kv').waitFor();
  const detail=await page.locator('#ev-detail').innerText();
  assert.match(detail,/Watchlist capture, Plex failed/);
  assert.match(detail,/Triggered by\s*Schedule/);
  assert.match(detail,/Provider not configured/);
  await page.locator('.ev-vtab[data-v="backup"]').click();
  await page.locator('#ev-list .ev-row[data-id="52"]').waitFor();
  assert.ok(queries.some(q=>q.path==='/api/events/groups'&&q.domain==='backup'));
  assert.equal(await page.locator('#ev-list .ev-row[data-id="51"]').count(),0);
  assert.deepEqual(errors,[]);
});


test('activity workspace keeps destination results, bulk review and large histories usable', async t => {
  const browser = await chromium.launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport: {width: 1600, height: 1100}});
  page.setDefaultTimeout(10000);
  const errors = [], requests = [], reviewed = new Set();
  page.on('pageerror', error => errors.push(error.message));
  const routes = ['CROSSWATCH', 'TRAKT', 'SIMKL'].map((provider, index) => ({
    id: 100 + index, domain: 'scrobble', feature: 'ratings', operation: 'update', status: index === 2 ? 'skipped' : 'rated',
    severity: index === 2 ? 'info' : 'success', title: "Marvel's VisionQuest", media_type: 'episode', season: 1, episode: 2,
    source_provider: 'PLEX', source_instance: 'default', destination_provider: provider, destination_instance: 'default',
    summary: index === 2 ? 'Rating skipped on SIMKL' : `Rated 6/10 on ${provider}`, rating: 6, event_count: 1,
    reason_label: index === 2 ? 'Episode ratings are not supported by this provider' : '',
    first_event_at: 1795131670, last_event_at: 1795131670,
  }));
  await page.route('http://events.test/**', async route => {
    const url = new URL(route.request().url());
    if (url.pathname.startsWith('/assets/')) {
      const ext = url.pathname.split('.').at(-1);
      return route.fulfill({body: await readFile(new URL('..' + url.pathname, import.meta.url)), contentType: {css: 'text/css', svg: 'image/svg+xml', woff2: 'font/woff2'}[ext] || 'text/javascript'});
    }
    if (url.pathname.startsWith('/api/')) {
      requests.push({path: url.pathname, ...Object.fromEntries(url.searchParams), method: route.request().method()});
      let json = {ok: true, items: [], pairs: []};
      const match = url.pathname.match(/^\/api\/events\/groups\/(\d+)(?:\/(acknowledge|unacknowledge))?$/);
      if (match) {
        const id = Number(match[1]);
        if (match[2]) {
          if (match[2] === 'acknowledge') reviewed.add(id); else reviewed.delete(id);
          json = {ok: true, acknowledged_at: match[2] === 'acknowledge' ? 1795131800 : null};
        } else {
          const group = {...routes.find(row => row.id === id), acknowledged_at: reviewed.has(id) ? 1795131800 : null};
          json = {ok: true, group, events: [{...group, event_type: id === 102 ? 'rating_skipped' : 'rating_applied', created_at: group.last_event_at, detail: {account: 'PascalvandenBerg', rating: 6}}]};
          if (id >= 1000) {
            const audit = {id, domain: 'audit', status: 'completed', severity: 'success', summary: 'admin used settings', last_event_at: 1795131670};
            json = {ok: true, group: audit, events: [{...audit, event_type: 'audit_api_action', created_at: 1795131670, detail: {actor: {username: 'admin', role: 'admin'}, request: {method: 'POST', path: '/api/config', ip: '192.168.2.254'}, status: 'success'}}]};
          }

        }
      }
      if (url.pathname === '/api/events/groups') {
        const audit = url.searchParams.get('domain') === 'audit';
        const offset = Number(url.searchParams.get('offset') || 0);
        const total = audit ? 1184 : 132;
        const items = Array.from({length: Math.min(10, total - offset)}, (_, index) => audit ? {
          id: 1000 + offset + index, domain: 'audit', status: 'completed', severity: 'success', event_count: 1,
          summary: 'admin used settings', first_event_at: 1795131670, last_event_at: 1795131670,
        } : {
          ...routes[0], id: index ? 200 + offset + index : 100, title: index ? `Example activity ${offset + index}` : routes[0].title,
          event_count: 3, destinations: routes.map(row => ({...row, acknowledged_at: reviewed.has(row.id) ? 1795131800 : null})),
        });
        json = url.searchParams.get('q') || url.searchParams.get('category') === 'problems' ? {items: [], total: 0} : {items, total};
      }
      if (url.pathname === '/api/events/search') json = {items: [{...routes[0], event_type: 'rating_applied', created_at: 1795131670}], total: 396};
      return route.fulfill({json});
    }
    return route.fulfill({contentType: 'text/html', body: `<!doctype html><html data-tab="events"><head><meta charset="utf-8"><link rel="stylesheet" href="/assets/themes/tokens.css"><link rel="stylesheet" href="/assets/css/pages.css"><link rel="stylesheet" href="/assets/css/page-theme.css"><style>@font-face{font-family:Material Symbols Rounded;src:url('/assets/fonts/material-symbols-rounded-full-v355.woff2')}body{margin:20px;background:var(--cw-theme-bg,#080a0d);font-family:Arial,sans-serif}.material-symbols-rounded{font-family:Material Symbols Rounded;font-weight:normal;font-style:normal;font-size:20px;line-height:1;display:inline-block;white-space:nowrap;font-feature-settings:'liga'}</style></head><body><main id="page-events"></main></body></html>`});
  });
  await page.goto('http://events.test/');
  await page.evaluate(async () => {
    window.CW = {AuthState: {read: () => ({isAdmin: true})}};
    await import('/assets/helpers/provider-meta.js');
    window.eventsView = (await import('/assets/js/events/index.js')).default;
    await window.eventsView.mount(document.querySelector('#page-events'), {domain: 'scrobble'});
  });
  assert.equal(await page.locator('#ev-count').textContent(), '132 activities');
  await page.locator('#ev-more').click();
  await page.locator('#ev-review button[data-v="acknowledged"]').click();
  await page.locator('#ev-mf-summary [data-k="review"]').waitFor();
  await page.locator('#ev-more').click();
  assert.equal(await page.locator('#ev-mf-summary').isVisible(), true);
  await page.locator('#ev-mf-summary [data-k="review"]').click();
  await page.waitForFunction(() => !document.querySelector('#ev-mf-summary [data-k="review"]'));

  assert.ok(requests.some(row => row.activities === 'true'));
  assert.equal(await page.locator('.ev-delivery').count(), 3, await page.locator('#ev-detail').innerText());
  assert.equal(await page.locator('#ev-list .ev-row').count(), 10);
  assert.match(await page.locator('#ev-list .ev-row').first().innerText(), /CROSSWATCH.*TRAKT.*SIMKL/s);
  await page.locator('.ev-delivery summary').filter({hasText: 'SIMKL'}).click();
  assert.match(await page.locator('.ev-delivery[open]').innerText(), /Episode ratings are not supported/);
  await page.locator('#ev-detail .ev-tab[data-tab="raw"]').click();
  assert.match(await page.locator('[data-pane="raw"] pre').textContent(), /rating_skipped/);
  await page.locator('#ev-detail .ev-tab[data-tab="timeline"]').click();
  assert.equal(await page.locator('[data-pane="timeline"] .ev-tl').count(), 3);
  await page.locator('#ev-detail .ev-tab[data-tab="details"]').click();
  for (const theme of ['original', 'flat-dark', 'flat-light']) {
    await page.evaluate(theme => document.documentElement.dataset.cwTheme = theme, theme);
    await page.locator('#ev-list .ev-row').first().click();
    await page.locator('.ev-delivery').first().waitFor();
    if (process.env.CW_EVENTS_SCREENSHOTS) {
      await mkdir(process.env.CW_EVENTS_SCREENSHOTS, {recursive: true});
      await page.screenshot({path: `${process.env.CW_EVENTS_SCREENSHOTS}/activity-${theme}.png`, fullPage: true});
    }
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.locator('#ev-sort .ev-dd-btn').click();
    assert.equal(await page.locator('.ev-dd-menu').isVisible(), true);
    if (process.env.CW_EVENTS_SCREENSHOTS) await page.screenshot({path: `${process.env.CW_EVENTS_SCREENSHOTS}/menu-${theme}.png`});
    await page.keyboard.press('Escape');
    await page.locator('#ev-q').fill('missing');
    await page.locator('#ev-empty-reset').waitFor();
    if (process.env.CW_EVENTS_SCREENSHOTS) await page.screenshot({path: `${process.env.CW_EVENTS_SCREENSHOTS}/empty-${theme}.png`});
    await page.locator('#ev-empty-reset').click();
    await page.locator('#ev-list .ev-row').first().waitFor();
  }
  await page.locator('#ev-list .ev-check').first().check();
  await page.locator('#ev-review-selected').click();
  await page.waitForFunction(() => document.querySelector('#ev-toast').textContent.includes('Selected activities marked reviewed'));
  assert.deepEqual([...reviewed].sort(), [100, 101, 102]);
  await page.locator('#ev-select-page').check();
  assert.equal(await page.locator('#ev-selection-count').textContent(), '10 selected');
  await page.locator('#ev-next').click();
  assert.equal(await page.locator('#ev-review-selected').isDisabled(), true);
  await page.locator('.ev-vtab[data-v="audit"]').click();
  await page.waitForFunction(() => document.querySelector('#ev-count').textContent.replace(/[^0-9]/g, '') === '1184');
  await page.locator('#ev-last').click();
  await page.waitForFunction(() => document.querySelector('#ev-page-number').value === '119');
  assert.equal(await page.locator('#ev-list .ev-row').count(), 4);
  await page.locator('#ev-list .ev-row').first().click();
  await page.locator('#ev-detail .ev-kv').first().waitFor();
  assert.match(await page.locator('[data-pane="details"]').innerText(), /admin/);
  assert.doesNotMatch(await page.locator('[data-pane="details"]').innerText(), /192\.168/);
  await page.locator('#ev-detail .ev-tab[data-tab="raw"]').click();
  assert.match(await page.locator('[data-pane="raw"]').innerText(), /192\.168\.2\.254/);
  await page.locator('#ev-detail .ev-tab[data-tab="details"]').click();
  if (process.env.CW_EVENTS_SCREENSHOTS) await page.screenshot({path: `${process.env.CW_EVENTS_SCREENSHOTS}/activity-audit.png`, fullPage: true});

  assert.equal(await page.locator('#ev-next').isDisabled(), true);
  await page.locator('#ev-page-number').fill('2');
  await page.locator('#ev-page-number').press('Tab');
  await page.waitForFunction(() => document.querySelector('#ev-list .ev-row').dataset.id === '1010');
  await page.locator('.ev-vtab[data-v="scrobble"]').click();
  await page.locator('#ev-mode button[data-v="raw"]').click();
  await page.waitForFunction(() => document.querySelector('#ev-count').textContent === '396 events');
  assert.equal(await page.locator('#ev-attention').isVisible(), false);
  await page.locator('#ev-mode button[data-v="grouped"]').click();
  await page.locator('#ev-list .ev-row[data-id="100"]').waitFor();
  await page.setViewportSize({width: 390, height: 844});
  await page.locator('#ev-list .ev-row').first().click();
  await page.locator('.ev-delivery').first().waitFor();
  assert.equal(await page.locator('#ev-detail-back').isVisible(), true);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  if (process.env.CW_EVENTS_SCREENSHOTS) await page.screenshot({path: `${process.env.CW_EVENTS_SCREENSHOTS}/activity-mobile-detail.png`, fullPage: true});
  await page.locator('#ev-detail-back').click();
  assert.equal(await page.locator('#ev-list').isVisible(), true);
  assert.ok((await page.locator('#ev-list .ev-row').first().boundingBox()).height < 180);
  await page.setViewportSize({width: 320, height: 800});
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await page.setViewportSize({width: 390, height: 844});
  if (process.env.CW_EVENTS_SCREENSHOTS) await page.screenshot({path: `${process.env.CW_EVENTS_SCREENSHOTS}/activity-mobile-list.png`, fullPage: true});
  assert.deepEqual(errors, []);
});
