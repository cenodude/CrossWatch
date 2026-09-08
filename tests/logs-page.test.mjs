/* tests/logs-page.test.mjs */
/* CrossWatch - Log search highlighting tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
globalThis.window={addEventListener(){}};
const {highlight,logsBackLink,savedLogFeatures,savedLogsTable}=await import('../assets/js/logs.js');

test('saved features use the archived settings and respect the selected pair',()=>{
  const pairs=[{id:'a',features:{watchlist:true,ratings:{enable:false},history:{enable:true}}},{id:'b',features:{ratings:{enabled:'on'},progress:true,playlists:'false',collection:1}}];
  const session={pairs:JSON.stringify(pairs)};
  assert.deepEqual(savedLogFeatures(session),[true,true,true,true,false,true]);
  assert.deepEqual(savedLogFeatures(session,'a'),[true,false,true,false,false,false]);
  assert.equal(savedLogFeatures(session,'missing'),null);
  assert.equal(savedLogFeatures({pairs:'invalid'}),null);
  assert.equal(savedLogFeatures({pairs:[{id:'old'}]}),null);
  pairs[0].features.watchlist=false;
  assert.equal(savedLogFeatures(session,'a')[0],true);
});

test('saved logs render columns, feature states, status badges and safe log labels',()=>{
  const base={id:'one',label:'PLEX <script> to TRAKT',started:100,ended:102,status:'completed',errors:0,warnings:0,pairs:JSON.stringify([{id:'a',features:{ratings:true,history:true,progress:true}}])};
  const html=savedLogsTable([base]);
  for(const name of ['Sync pair','Features','Started','Duration','Status','Errors','Warnings'])assert.ok(html.includes(name));
  assert.equal((html.match(/class="logs-feature-dot /g)||[]).length,6);
  assert.equal((html.match(/class="logs-feature-dot \w+ on"/g)||[]).length,3);
  assert.match(html,/logs-status-success/);assert.match(html,/>2s</);
  assert.match(html,/PLEX &lt;script&gt;/);assert.doesNotMatch(html,/<script>/);
  assert.match(savedLogsTable([{...base,status:'issues'}]),/logs-status-warning/);
  assert.match(savedLogsTable([{...base,status:'__proto__'}]),/logs-status-neutral/);
  assert.match(savedLogsTable([{...base,pairs:'[]'}]),/Feature settings unavailable/);
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

test('opening Logs captures the caller before navigation and preserves it when reopened',()=>{
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
  context.window.openLogs({pairId:'pair 5',latest:true});
  let route=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(route.get('returnTo'),'/?main=1#settings/sync');
  assert.equal(route.get('pairId'),'pair 5');
  context.window.openLogs({channel:'debug'});
  route=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(route.get('returnTo'),'/?main=1#settings/sync');
  location.href='http://localhost/profile';
  context.document.getElementById=()=>null;
  context.window.openLogs();
  assert.equal(new URLSearchParams(location.hash.split('?')[1]).get('returnTo'),'/profile');
});
