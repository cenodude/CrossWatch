/* CrossWatch - Maintenance page navigation and action regression tests */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {pageBackLink} from '../assets/js/page-return.js';

const source=readFileSync(new URL('../assets/js/maintenance/index.js',import.meta.url),'utf8');
const catalog=source.slice(source.indexOf('const SIMPLE_OPS'),source.indexOf('function injectCSS()'));

test('Maintenance keeps its caller and category, including the old entry point',()=>{
  const modals=readFileSync(new URL('../assets/js/modals.js',import.meta.url),'utf8');
  const script=modals.slice(modals.indexOf('window.openMaintenance ='),modals.indexOf('window.openManualWatchedModal ='));
  const location=new URL('http://localhost/?main=1#settings/maintenance');
  const navigations=[];
  const context={location,URLSearchParams,ModalRegistry:{close(){}},document:{getElementById:()=>({})},
    window:{showTab:tab=>navigations.push(tab)},history:{pushState(_state,_title,hash){location.hash=hash;}}};
  vm.runInNewContext(script,context);
  context.window.openMaintenance({group:'events'});
  const first=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(first.get('group'),'events');
  assert.equal(first.get('returnTo'),'/?main=1#settings/maintenance');
  context.window.openMaintenanceModal({target:'sync'});
  const next=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(next.get('returnTo'),first.get('returnTo'));
  assert.equal(next.get('group'),'sync');
  assert.deepEqual(navigations,['maintenance','maintenance']);
  assert.deepEqual(pageBackLink(next.get('returnTo'),location.href,'maintenance'),{href:'#settings/maintenance',label:'Maintenance'});
  assert.equal(pageBackLink('https://example.com',location.href,'maintenance').href,'#main');
});

test('all existing tools have one category and bulk recommendations exclude resets',()=>{
  const ctx={};
  vm.runInNewContext(catalog+';globalThis.ops=OPS;globalThis.groups=GROUPS;globalThis.recommended=recommendedKeys([]);',ctx);
  assert.equal(ctx.ops.length,16);
  for (const op of ctx.ops) assert.equal(ctx.groups.filter(group=>group.keys.includes(op.key)).length,1,op.key);
  assert.deepEqual(Array.from(ctx.recommended),['cache','database-health','playing']);
});

test('successful individual usage promotes safe tools without promoting destructive or batch actions',()=>{
  const ctx={};
  vm.runInNewContext(catalog+';globalThis.rank=recommendedKeys;',ctx);
  const runs=(key,status='success',batch=false)=>Array.from({length:5},()=>({key,status,batch}));
  assert.deepEqual(Array.from(ctx.rank(runs('defaults'))),['cache','database-health','playing']);
  assert.deepEqual(Array.from(ctx.rank(runs('events-health','error'))),['cache','database-health','playing']);
  assert.deepEqual(Array.from(ctx.rank(runs('events-health','issues'))),['cache','database-health','playing']);
  assert.deepEqual(Array.from(ctx.rank(runs('events-health','success',true))),['cache','database-health','playing']);
  assert.deepEqual(Array.from(ctx.rank(runs('events-health'))),['events-health','cache','database-health']);
  assert.deepEqual(Array.from(ctx.rank([...runs('events-health'),...runs('cache')])),['cache','events-health','database-health']);
});

test('saved history is bounded and validated, and account keys stay separate',()=>{
  const ctx={};
  vm.runInNewContext(catalog+';globalThis.clean=cleanHistory;globalThis.key=historyKey;',ctx);
  const entries=Array.from({length:105},(_,i)=>({key:'cache',status:'success',at:i+1,batch:false}));
  const cleaned=ctx.clean([...entries,{key:'__proto__',status:'success',at:500},{key:'cache',status:'success',at:1e99},{key:'cache',status:'running',at:501},null]);
  assert.equal(cleaned.length,100);
  assert.equal(cleaned[0].at,105);
  assert.equal(cleaned[99].at,6);
  assert.equal(ctx.clean({}).length,0);
  assert.notEqual(ctx.key({user:{id:'a'}}),ctx.key({user:{id:'b'}}));
  assert.equal(ctx.key({user:{id:'a',username:'old'}}),ctx.key({user:{id:'a',username:'new'}}));
});

test('managed users cannot open Maintenance even with write permission',()=>{
  const core=readFileSync(new URL('../assets/helpers/core.js',import.meta.url),'utf8');
  const context={window:{CW:{AuthState:{read:()=>({isManaged:true,permissions:{write:true}})}}}};
  vm.runInNewContext(core.slice(core.indexOf('  const ROUTE_TABS'),core.indexOf('  function pickCase')),context);
  assert.equal(context.normalizeRouteTab('maintenance'),'maintenance');
  assert.equal(context.canUseRouteTab('maintenance'),false);
  context.window.CW.AuthState.read=()=>({isManaged:false});
  assert.equal(context.canUseRouteTab('maintenance'),true);
});

test('recommended batch runs card actions despite filters, excludes unsafe actions and stops on an unhealthy result',async()=>{
  let click;
  const calls=[];
  const rows=[['database-health',true],['events-health',false],['events-optimize',false],['defaults',false]]
    .map(([key,hidden])=>({hidden,dataset:{op:key,kind:key},querySelector:()=>({})}));
  const context={rows,operationBusy:false,batchRunning:false,recommended:['events-health','events-optimize','defaults'],
    $:()=>({addEventListener:(_event,handler)=>{click=handler;}}),root:{},
    setOperationBusy:value=>{context.operationBusy=value;},
    runOp:async kind=>{calls.push(kind);return {ok:true,healthy:false};},
    setStatus(){},completionReceipt(){},plural(){},
  };
  const start=source.indexOf('    $("#cxm-run-recommended", root).addEventListener');
  vm.runInNewContext(catalog+source.slice(start,source.indexOf('    syncRoute();',start)),context);
  await click();
  assert.deepEqual(calls,['events-health']);
  assert.equal(context.operationBusy,false);
  assert.equal(context.batchRunning,false);
  context.runOp=async kind=>{calls.push(kind);return {ok:true,healthy:true};};
  calls.length=0;
  await click();
  assert.deepEqual(calls,['events-health','events-optimize']);
  assert.equal(context.batchRunning,false);
});

function runner({confirmed=true,typed='RESET',response={ok:true},reject=false}={}) {
  const posts=[],feedback=[],messages=[];
  const context={window:{},console,Date,URLSearchParams,operationBusy:false,selectedInsightKind:null,
    confirm:()=>confirmed,prompt:()=>typed,root:{isConnected:true},
    post:async(url,body)=>{posts.push({url,body});if(reject)throw new Error('offline');return response;},
    setOperationBusy(value){context.operationBusy=value;},startActionFeedback(){},finishActionFeedback:(_button,status)=>feedback.push(status),
    setStatus:message=>messages.push(message),refreshSummary:async()=>{},loadActionInsight:async()=>{},
    eventsReceipt:()=>null,databaseReceipt:()=>null,stateFileReceipt:()=>null,statePruneReceipt:()=>null,
    completionReceipt:()=> 'Done',returnToCaller(){},setTimeout(){},
  };
  vm.runInNewContext(catalog+source.slice(source.indexOf('    async function runOp('),source.indexOf('    OPS.forEach(')),context);
  return {context,posts,feedback,messages,run:kind=>context.runOp(kind,{dataset:{}})};
}

test('cancelling destructive tasks sends no request',async()=>{
  for(const kind of ['captures','events-purge','events-rebuild','state-file','state-file-prune','defaults']) {
    const fixture=runner({confirmed:false});
    assert.equal(await fixture.run(kind),false);
    assert.equal(fixture.posts.length,0,kind);
    assert.equal(fixture.context.operationBusy,false);
  }
  const fixture=runner({typed:'no'});
  await fixture.run('defaults');
  assert.equal(fixture.posts.length,0);
});

test('event deletion retains explicit confirmation in the API payload',async()=>{
  const fixture=runner();
  await fixture.run('events-purge');
  assert.equal(fixture.posts[0].url,'/api/maintenance/events-purge');
  assert.equal(fixture.posts[0].body.confirm,true);
  assert.deepEqual(fixture.feedback,['success']);
});

test('unhealthy diagnostics show issues and failed requests release the operation lock',async()=>{
  const unhealthy=runner({response:{ok:true,healthy:false}});
  await unhealthy.run('database-health');
  assert.deepEqual(unhealthy.feedback,['issues']);
  const failed=runner({reject:true});
  assert.equal(await failed.run('events-health'),false);
  assert.deepEqual(failed.feedback,['error']);
  assert.equal(failed.context.operationBusy,false);
  assert.match(failed.messages.at(-1),/offline/);
});

function categoryRunner(options={}) {
  const fixture=runner(options);
  const {context}=fixture;
  vm.runInNewContext('globalThis.categoryOps=OPS;',context);
  context.rows=Array.from(context.categoryOps,op=>({hidden:false,dataset:{op:op.key,kind:op.kind},querySelector:()=>({dataset:{}})}));
  context.batchRunning=false;
  context.plural=(count,noun)=>`${count} ${noun}s`;
  context.setArchiveOpen=value=>{context.archiveOpen=value;};
  const label={textContent:'Run all'};
  const details={open:false};
  const button={closest:()=>details,querySelector:()=>label,setAttribute(){},removeAttribute(){}};
  return {...fixture,label,details,runGroup:id=>context.runCategory(id,button)};
}

test('category batches run visible tasks sequentially and block another batch',async()=>{
  const fixture=categoryRunner();
  let release;
  fixture.context.post=async url=>{
    fixture.posts.push({url});
    if(fixture.posts.length===1) await new Promise(resolve=>{release=resolve;});
    return {ok:true};
  };
  const pending=fixture.runGroup('sync');
  assert.equal(fixture.posts.length,1);
  assert.equal(fixture.context.operationBusy,true);
  assert.equal(fixture.context.batchRunning,true);
  assert.equal(fixture.label.textContent,'1/2');
  assert.equal(fixture.details.open,true);
  await fixture.runGroup('playback');
  assert.equal(fixture.posts.length,1);
  release();
  await pending;
  assert.deepEqual(fixture.posts.map(item=>item.url),['/api/maintenance/clear-state','/api/maintenance/clear-cache']);
  assert.equal(fixture.context.operationBusy,false);
  assert.equal(fixture.context.batchRunning,false);
  assert.equal(fixture.label.textContent,'Run all');
  fixture.posts.length=0;
  fixture.context.post=async url=>{fixture.posts.push({url});return {ok:true};};
  fixture.context.rows.find(row=>row.dataset.op==='state').hidden=true;
  await fixture.runGroup('sync');
  assert.deepEqual(fixture.posts.map(item=>item.url),['/api/maintenance/clear-cache']);
});

test('category cancellation and unhealthy checks stop later tasks',async()=>{
  const cancelled=categoryRunner({confirmed:false});
  await cancelled.runGroup('sync');
  assert.equal(cancelled.posts.length,0);
  const unhealthy=categoryRunner({response:{ok:true,healthy:false}});
  await unhealthy.runGroup('events');
  assert.deepEqual(unhealthy.posts.map(item=>item.url),['/api/maintenance/events-health']);
  assert.equal(unhealthy.context.operationBusy,false);
  const failed=categoryRunner({reject:true});
  await failed.runGroup('sync');
  assert.equal(failed.posts.length,1);
  assert.equal(failed.context.operationBusy,false);
});

test('category batches retain destructive task confirmation and archive configuration',async()=>{
  const fixture=categoryRunner();
  const prompts=[];
  fixture.context.confirm=message=>{prompts.push(message);return prompts.length===1;};
  await fixture.runGroup('events');
  assert.match(prompts[0],/Clear event data/);
  assert.match(prompts[1],/Clear all event data/);
  assert.deepEqual(fixture.posts.map(item=>item.url),['/api/maintenance/events-health','/api/maintenance/events-optimize']);
  assert.equal(fixture.context.operationBusy,false);
  fixture.posts.length=0;
  await fixture.runGroup('archive');
  assert.equal(fixture.context.archiveOpen,true);
  assert.equal(fixture.posts.length,0);
});
