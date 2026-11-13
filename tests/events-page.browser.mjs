/* tests/events-page.browser.mjs */
/* CrossWatch - Events deep links and filter reset browser tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';

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
    return route.fulfill({contentType:'text/html',body:'<!doctype html><html data-tab="events"><head><style>body{margin:20px;background:#080a0d;font-family:Arial,sans-serif}*{box-sizing:border-box}.material-symbols-rounded{font-size:0!important}</style></head><body><main id="page-events"></main></body></html>'});
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
  await page.waitForFunction(()=>document.querySelector('#ev-count').textContent==='1 thread');
  assert.equal(await page.locator('#ev-list .ev-row[data-id="42"]').count(),0);
  assert.equal(await page.locator('#ev-detail .ev-dtitle').count(),0);
  await page.locator('#ev-more').click();
  assert.equal(await page.locator('#ev-mf-summary').isVisible(),true);
  assert.equal(await page.locator('#ev-clear-hidden').count(),0);
  await page.locator('#ev-chip-clear').click();
  await page.waitForFunction(()=>document.querySelector('#ev-count').textContent==='2 threads');
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
  await page.waitForFunction(()=>document.querySelector('#ev-count').textContent==='2 threads');
  assert.equal(queries.at(-1).run_id,undefined);
  assert.equal(queries.at(-1).event_type,undefined);
  assert.equal(queries.at(-1).q,undefined);
  assert.deepEqual(errors,[]);
});
