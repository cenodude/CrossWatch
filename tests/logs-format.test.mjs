/* tests/logs-format.test.mjs */
/* CrossWatch - Readable log formatting and original data preservation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {describeLog,readableLogHTML} from '../assets/js/logs-format.js';
globalThis.window={addEventListener(){}};
const {highlight}=await import('../assets/js/logs.js');
const row = data => ({text:JSON.stringify(data),provider:'SYNC',level:'INFO'});

test('sync start uses a matching card and pair details use branded pills',()=>{
  const text='[SYNC] INFO SYNC start: orchestrator pairs run_id=1788821602';
  const result=describeLog({text});
  assert.equal(result.summary,'Sync started');
  assert.equal(result.start,true);
  assert.deepEqual(result.pills,[{label:'Run',value:'1788821602',tone:''}]);
  const html=readableLogHTML({text},highlight,'orchestrator');
  assert.match(html,/logs-completion-start/);
  assert.match(html,/<details class="logs-original" open>/);
  const pair={text:'[i] Running single pair: PLEX → SIMKL (pair_981e2ec7613e)'};
  assert.deepEqual(describeLog(pair).pills,[{label:'From',value:'PLEX',tone:'provider'},{label:'To',value:'SIMKL',tone:'provider'},{label:'Pair',value:'pair_981e2ec7613e',tone:''}]);
  assert.match(readableLogHTML(pair,highlight,'',{brandInfo:value=>({label:value,icon:`/assets/img/${value}.svg`})}),/PLEX\.svg/);
  assert.equal(describeLog(pair).raw,pair.text);
  assert.equal(describeLog({text:'SYNC start: something else'}).structured,false);
});

test('state diagnostics distinguish loaded adapters from saved-state providers',()=>{
  for(const [field,label,count] of [['loaded_adapters','Loaded adapters',18],['saved_state_providers','Saved-state providers',4],['saved_state_providers','Saved-state providers',0]]) {
    const entry=row({event:'debug',msg:'state.persisted',[field]:count});
    const result=describeLog(entry);
    assert.equal(result.summary,'Sync state saved');
    assert.deepEqual(result.pills,[{label,value:String(count),tone:''}]);
    assert.equal(result.raw,entry.text);
    assert.match(readableLogHTML(entry,highlight,field),/<details class="logs-original" open>/);
  }
  assert.deepEqual(describeLog(row({event:'debug',msg:'state.persisted',providers:4})).pills,[{label:'Providers',value:'4',tone:''}]);
});

test('final text totals become a prominent summary with all seven counts',()=>{
  const text='2026-09-07T20:48:22.000+00:00 [SYNC] INFO [i] Done. Total added: 0, Total removed: 0, Total updated: 0, Total skipped: 0, Total unresolved: 0, Total errors: 0, Total blocked: 0';
  const result=describeLog({text});
  assert.equal(result.summary,'Sync completed');
  assert.equal(result.completion,'success');
  assert.deepEqual(result.pills.map(p=>p.label),['Added','Removed','Updated','Skipped','Unresolved','Errors','Blocked']);
  assert.ok(result.pills.every(p=>p.value==='0' && p.tone===''));
  assert.equal(result.raw,text);
  const errors=describeLog({text:text.replace('Total errors: 0','Total errors: 12')});
  assert.equal(errors.summary,'Sync finished with issues');
  assert.equal(errors.completion,'error');
  assert.ok(errors.pills.some(p=>p.label==='Errors' && p.value==='12' && p.tone==='error'));
  assert.equal(describeLog({text:text.replace('Total unresolved: 0','Total unresolved: 3')}).completion,'warning');
  assert.equal(describeLog({text:text.replace('Done.','Cancelled.')}).summary,'Sync cancelled');
  const html=readableLogHTML({text},highlight,'Total added');
  assert.match(html,/logs-completion-success/);
  assert.match(html,/<details class="logs-original" open>/);
  for(const invalid of [text+' extra details',text.replace('Total errors: 0','Total errors: unknown')])assert.equal(describeLog({text:invalid}).structured,false);
});

test('structured sync events become messages and useful pills',()=>{
  const result=describeLog(row({event:'run:done',added:2,updated:1,removed:0,errors:0,pairs:1}));
  assert.equal(result.summary,'Sync finished');
  assert.ok(result.pills.some(p=>p.label==='Added' && p.value==='2'));
  assert.ok(result.pills.some(p=>p.label==='Errors' && p.value==='0'));
  assert.equal(describeLog(row({event:'run:done',errors:1})).summary,'Sync finished with issues');
  assert.equal(describeLog(row({event:'run:done',cancelled:true})).summary,'Sync cancelled');
  assert.equal(describeLog(row({event:'debug',msg:'state.persisted'})).summary,'Sync state saved');
  const progress=describeLog(row({event:'snapshot:progress',done:6,total:6,feature:'watchlist'}));
  assert.equal(progress.summary,'Reading progress');
  assert.ok(progress.pills.some(p=>p.value==='6 / 6'));
});

test('provider health retains nested details in the original log',()=>{
  const health=row({event:'health',provider:'PLEX',status:'ok',latency_ms:137,api:{pms:{status:200}},features:{watchlist:true}});
  const result=describeLog(health);
  assert.equal(result.summary,'Provider health checked');
  assert.ok(result.pills.some(p=>p.value==='137 ms'));
  assert.equal(result.raw,health.text);
  const html=readableLogHTML(health,highlight,'', {brandInfo:()=>({label:'Plex',icon:'/assets/img/PLEX.svg'})});
  assert.match(html,/PLEX\.svg/);
  assert.match(html,/Original log/);
  assert.match(html,/&quot;pms&quot;/);
});

test('known timestamp and level prefixes are removed without losing the original',()=>{
  const text='2026-09-07T20:48:20.486+00:00 [SYNC] INFO [i] Sync started';
  assert.deepEqual(describeLog({text}),{summary:'Sync started',raw:text,pills:[],structured:false});
  const structured='[2026-09-07 20:48:21] [SYNC] DEBUG {"event":"debug","msg":"state.persisted"}';
  assert.equal(describeLog({text:structured}).summary,'Sync state saved');
  for(const text of ['unexpected {broken json','[custom message] with details','{"event":broken','[]']) {
    assert.equal(describeLog({text}).summary,text);
    assert.equal(describeLog({text}).raw,text);
  }
});

test('search reveals raw-only matches and all log values are escaped',()=>{
  const entry=row({event:'run:done',nested:{internal_key:'<img src=x onerror=alert(1)>'},errors:0});
  const html=readableLogHTML(entry,highlight,'internal_key');
  assert.match(html,/<details class="logs-original" open>/);
  assert.match(html,/<mark>internal_key<\/mark>/);
  assert.doesNotMatch(html,/<img src=x/);
  assert.match(html,/&lt;img/);
  assert.equal(describeLog(row({event:'__proto__'})).summary,'Proto');
});

test('readable summaries and original logs escape HTML in every tag case',()=>{
  for(const [text,escaped] of [
    ['<script>alert(1)</script>','&lt;script&gt;alert(1)&lt;/script&gt;'],
    ['<SCRIPT>alert(1)</SCRIPT>','&lt;SCRIPT&gt;alert(1)&lt;/SCRIPT&gt;'],
    ['<ScRiPt src="x">alert(1)</ScRiPt >','&lt;ScRiPt src=&quot;x&quot;&gt;alert(1)&lt;/ScRiPt &gt;'],
    ["<IMG src=x onerror='alert(1)'> & text",'&lt;IMG src=x onerror=&#39;alert(1)&#39;&gt; &amp; text'],
  ]) {
    assert.equal(readableLogHTML({text},highlight),
      `<span class="logs-readable-summary">${escaped}</span><details class="logs-original"><summary>Original log</summary><pre>${escaped}</pre></details>`);
  }
  assert.equal(highlight('<ScRiPt>alert(1)</ScRiPt>','script'),
    '&lt;<mark>ScRiPt</mark>&gt;alert(1)&lt;/<mark>ScRiPt</mark>&gt;');
});
