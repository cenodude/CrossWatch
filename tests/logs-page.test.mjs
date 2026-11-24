/* tests/logs-page.test.mjs */
/* CrossWatch - Log search highlighting tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import './i18n-vm-setup.mjs';
globalThis.window={addEventListener(){}};
const {englishI18n}=await import('./i18n-test-helper.mjs');
globalThis.window ??= {};
globalThis.window.__cwI18n ??= englishI18n();
const {highlight,logsBackLink,savedLogList}=await import('../assets/js/logs.js');

test('session list shows selection and diagnostic metadata',()=>{
  const base={id:'one',label:'PLEX to TRAKT',started:100,ended:102,status:'completed',lines:25,errors:0,warnings:0};
  const html=savedLogList([base],'one');
  assert.match(html,/aria-pressed="true"/);
  assert.match(html,/25 records/);
  assert.match(html,/2s/);
  assert.match(html,/logs-status-success/);
  assert.match(savedLogList([{...base,status:'issues',pinned:true}]),/Pinned/);
  assert.match(savedLogList([{...base,status:'__proto__'}]),/logs-status-neutral/);
});

test('saved log labels escape tags and quotes in both text and attributes',()=>{
  const base={id:'one',started:100,ended:102,status:'completed',errors:0,warnings:0};
  for(const [label,escaped] of [
    ['<script>alert(1)</script>','&lt;script&gt;alert(1)&lt;/script&gt;'],
    ['<SCRIPT>alert(1)</SCRIPT>','&lt;SCRIPT&gt;alert(1)&lt;/SCRIPT&gt;'],
    ['<ScRiPt src="x">alert(1)</ScRiPt >','&lt;ScRiPt src=&quot;x&quot;&gt;alert(1)&lt;/ScRiPt &gt;'],
    ['" onmouseover="alert(1)" & \'quoted\'','&quot; onmouseover=&quot;alert(1)&quot; &amp; &#39;quoted&#39;'],
  ]) {
    const html=savedLogList([{...base,label}]);
    assert.ok(html.includes(`<strong>${escaped}</strong>`));
    assert.ok(html.includes(`aria-label="Open log: ${escaped}"`));
    assert.equal(html,savedLogList([{...base,label:'SAFE_LABEL'}]).replaceAll('SAFE_LABEL',escaped));
  }
});
test('search highlights literal matches and escapes log HTML',()=>{
  assert.equal(highlight('<script>Error & ERROR</script>','error'),'&lt;script&gt;<mark>Error</mark> &amp; <mark>ERROR</mark>&lt;/script&gt;');
  assert.equal(highlight('x.* x.*','.*'),'x<mark>.*</mark> x<mark>.*</mark>');
  assert.equal(highlight('plain <text>',''),'plain &lt;text&gt;');
});

test('Logs returns to its caller with the correct label and route parameters',()=>{
  const current='http://localhost/?main=1#logs?channel=sync';
  assert.deepEqual(logsBackLink('/?main=1#settings/sync',current),{href:'#settings/sync',label:'Sync pairs'});
  assert.deepEqual(logsBackLink('/?main=1#events?runId=abc&visibility=all',current),{href:'#events?runId=abc&visibility=all',label:'Events'});
  assert.deepEqual(logsBackLink('/profile#preferences',current),{href:'/profile#preferences',label:'Profile'});
  assert.deepEqual(logsBackLink('/?main=1',current),{href:'#main',label:'Main'});
});

test('direct links and invalid return destinations fall back to Main',()=>{
  for(const target of [null,'https://example.com/#events','//example.com/#events','javascript:alert(1)','/api/logs/archive','/#logs?channel=debug','/#unknown']){
    assert.deepEqual(logsBackLink(target,'http://localhost/#logs'),{href:'#main',label:'Main'});
  }
});

test('opening Logs captures the caller before navigation and preserves it when reopened',async()=>{
  const source=readFileSync(new URL('../assets/js/modals.js',import.meta.url),'utf8');
  const start=source.indexOf('window.openLogs =');
  const script=source.slice(start,source.indexOf('window.openEvents =',start));
  const url=new URL('http://localhost/?main=1#settings/sync');
  const location={get pathname(){return url.pathname;},get search(){return url.search;},
    get hash(){return url.hash;},set hash(value){url.hash=value;},
    get href(){return url.href;},set href(value){url.href=new URL(value,url).href;}};
  const context={location,URLSearchParams,ModalRegistry:{close(){}},document:{getElementById(){return {}; }},
    window:{showTab(tab){assert.equal(tab,'logs');}},history:{pushState(_state,_title,hash){location.hash=hash;}}};
  vm.runInNewContext(script,context);
  await context.window.openLogs({pairId:'pair 5',latest:true});
  let route=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(route.get('returnTo'),'/?main=1#settings/sync');
  assert.equal(route.get('pairId'),'pair 5');
  await context.window.openLogs({channel:'debug'});
  route=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(route.get('returnTo'),'/?main=1#settings/sync');
  location.href='http://localhost/profile';
  context.document.getElementById=()=>null;
  await context.window.openLogs();
  assert.equal(new URLSearchParams(location.hash.split('?')[1]).get('returnTo'),'/profile');
});
