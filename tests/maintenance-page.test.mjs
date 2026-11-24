/* CrossWatch - Maintenance page navigation and action regression tests */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {pageBackLink} from '../assets/js/page-return.js';
import './i18n-vm-setup.mjs';

const source=readFileSync(new URL('../assets/js/maintenance/index.js',import.meta.url),'utf8');
const catalog=source.slice(source.indexOf('const SIMPLE_OPS'),source.indexOf('let activeRoot, syncRoute;'));

test('Maintenance keeps its caller and category, including the old entry point',async()=>{
  const modals=readFileSync(new URL('../assets/js/modals.js',import.meta.url),'utf8');
  const script=modals.slice(modals.indexOf('window.openMaintenance ='),modals.indexOf('window.openManualWatchedModal ='));
  const location=new URL('http://localhost/?main=1#settings/maintenance');
  const navigations=[];
  const context={location,URLSearchParams,ModalRegistry:{close(){}},document:{getElementById:()=>({})},
    window:{showTab:tab=>navigations.push(tab)},history:{pushState(_state,_title,hash){location.hash=hash;}}};
  vm.runInNewContext(script,context);
  await context.window.openMaintenance({group:'events'});
  const first=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(first.get('group'),'events');
  assert.equal(first.get('returnTo'),'/?main=1#settings/maintenance');
  await context.window.openMaintenanceModal({target:'sync'});
  const next=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(next.get('returnTo'),first.get('returnTo'));
  assert.equal(next.get('group'),'sync');
  assert.deepEqual(navigations,['maintenance','maintenance']);
  assert.deepEqual(pageBackLink(next.get('returnTo'),location.href,'maintenance'),{href:'#settings/maintenance',label:'Maintenance'});
  assert.equal(pageBackLink('https://example.com',location.href,'maintenance').href,'#main');
});

test('all tools remain available and sections follow the approved order',()=>{
  const ctx={};
  vm.runInNewContext(catalog+';globalThis.ops=OPS;globalThis.groups=GROUPS;globalThis.quick=QUICK_TASKS;globalThis.sections=SECTIONS;',ctx);
  assert.equal(ctx.ops.length,18);
  for (const op of ctx.ops) assert.equal(ctx.groups.filter(group=>group.keys.includes(op.key)).length,1,op.key);
  assert.deepEqual(Array.from(ctx.quick),['meta','playing','database-health']);
  assert.deepEqual(Array.from(ctx.sections,row=>row[1]),['Quick fixes','Sync issues','CW Tracker','Advanced']);
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
  assert.equal(ctx.clean([{key:'cache',status:'success',at:1,jobId:'job-1'}])[0].jobId,'job-1');
  assert.equal(ctx.clean([{key:'cache',status:'success',at:1,jobId:{}}])[0].jobId,undefined);
  assert.notEqual(ctx.key({user:{id:'a'}}),ctx.key({user:{id:'b'}}));
  assert.equal(ctx.key({user:{id:'a',username:'old'}}),ctx.key({user:{id:'a',username:'new'}}));
});

test('legacy job history gains an ID without merging distinct runs',()=>{
  for (const delay of [0,850,5000]) {
    const at=1795269602500;
    const legacy={key:'database-health',status:'success',at:at+delay,batch:false};
    const other=[
      {...legacy,at:at-2000},
      {...legacy,status:'issues'},
      {...legacy,key:'playing'},
      {...legacy,jobId:'another-job'},
      {...legacy,batch:true},
    ];
    let saved=JSON.stringify([legacy,...other]);
    const context={entries:[],results:new Map(),storageKey:'history',Date,
      localStorage:{getItem:()=>saved,setItem:(_,value)=>{saved=value;}}};
    vm.runInNewContext(catalog+source.slice(source.indexOf('    function saveResult('),source.indexOf('    function updateRows(')),context);
    for(let visit=0;visit<2;visit++) {
      context.saveResult('database-health','success',{jobId:'existing-job',at});
      const entries=JSON.parse(saved);
      assert.equal(entries.length,6);
      assert.deepEqual(entries.find(entry=>entry.jobId==='existing-job'),{key:'database-health',status:'success',at,batch:false,jobId:'existing-job'});
      for(const entry of other) assert.ok(entries.some(item=>JSON.stringify(item)===JSON.stringify(entry)));
    }
  }
});

test('legacy history is preserved when the job match is ambiguous or outside the completion window',()=>{
  const at=1795269602500;
  for(const times of [[at+200,at+800],[at-200],[at+5001]]) {
    const original=times.map(time=>({key:'database-health',status:'success',at:time,batch:false}));
    let saved=JSON.stringify(original);
    const context={entries:[],results:new Map(),storageKey:'history',Date,
      localStorage:{getItem:()=>saved,setItem:(_,value)=>{saved=value;}}};
    vm.runInNewContext(catalog+source.slice(source.indexOf('    function saveResult('),source.indexOf('    function updateRows(')),context);
    context.saveResult('database-health','success',{jobId:'new-job',at});
    const entries=JSON.parse(saved);
    assert.equal(entries.length,original.length+1);
    assert.deepEqual(entries.filter(entry=>!entry.jobId).sort((a,b)=>a.at-b.at),original);
  }
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

function runner({confirmed=true,typed='RESET',response={ok:true},reject=false}={}) {
  const posts=[],feedback=[],messages=[];
  const context={window:{confirm:()=>confirmed,prompt:()=>typed},console,Date,URLSearchParams,operationBusy:false,selectedInsightKind:null,
    confirm:()=>confirmed,prompt:()=>typed,root:{isConnected:true},
    post:async(url,body)=>{posts.push({url,body});if(reject)throw new Error('offline');return response;},
    setOperationBusy(value){context.operationBusy=value;},startActionFeedback(){},finishActionFeedback:(_button,status)=>feedback.push(status),
    setStatus:message=>messages.push(message),refreshSummary:async()=>{},loadActionInsight:async()=>{},
    eventsReceipt:()=>null,databaseReceipt:()=>null,stateFileReceipt:()=>null,statePruneReceipt:()=>null,
    completionReceipt:()=> 'Done',returnToCaller(){},setTimeout(){},followJob:async()=>response,
  };
  vm.runInNewContext(catalog+source.slice(source.indexOf('    async function runOp('),source.indexOf('    root.querySelector("#cxm-cw-export")')),context);
  return {context,posts,feedback,messages,run:kind=>context.runOp(kind,{dataset:{}})};
}

test('cancelling destructive tasks sends no request',async()=>{
  for(const kind of ['captures','events-purge','events-rebuild','state-file','state-file-prune','database-rebuild','defaults']) {
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
  assert.equal(fixture.posts[0].url,'/api/maintenance/jobs');
  assert.equal(fixture.posts[0].body.action,'events-purge');
  assert.equal(fixture.posts[0].body.confirm,true);
  assert.deepEqual(fixture.feedback,['success']);
});

test('database rebuild sends explicit confirmation and reports the backup',async()=>{
  const fixture=runner({response:{ok:true,backup:'/config/.cw_databases/backups/crosswatch-1.sqlite3'}});
  await fixture.run('database-rebuild');
  assert.equal(fixture.posts[0].url,'/api/maintenance/jobs');
  assert.equal(fixture.posts[0].body.action,'database-rebuild');
  assert.equal(fixture.posts[0].body.confirm,true);
  assert.deepEqual(fixture.feedback,['success']);
  assert.match(fixture.messages.at(-1),/backup \/config\/\.cw_databases\/backups\/crosswatch-1\.sqlite3/);
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

test('maintenance polling continues beyond the old request deadline',async()=>{
  let polls=0, elapsed=0;
  const context={window:{setTimeout:fn=>{elapsed+=1000;fn();}},setStatus(){},recordJobResult(){},
    fjson:async()=>({id:'job',action:'events-optimize',state:++polls>60?'completed':'running',result:{ok:true}})};
  vm.runInNewContext(source.slice(source.indexOf('    async function followJob('),source.indexOf('    async function runOp(')),context);
  const result=await context.followJob({id:'job',action:'events-optimize',state:'queued'});
  assert.equal(result.ok,true);
  assert.equal(elapsed,61000);
});

test('refresh reconnects to maintenance and restart reports interruption',async()=>{
  const messages=[],locks=[];
  let state='running', outcome='completed';
  const context={window:{setTimeout:fn=>fn()},setStatus:message=>messages.push(message),recordJobResult(){},
    setOperationBusy:value=>locks.push(value),eventsReceipt:()=>'Optimization completed',databaseReceipt:()=>null,
    fjson:async()=>{const current=state;state=outcome;return {id:'job',action:'events-optimize',state:current,result:{ok:current!=='interrupted',error:'server_restarted'}};}};
  vm.runInNewContext(source.slice(source.indexOf('    async function followJob('),source.indexOf('    async function runOp(')),context);
  await context.resumeJob();
  assert.equal(messages.at(-1),'Optimization completed');
  assert.deepEqual(locks,[true,false]);
  state='running';
  outcome='interrupted';
  await context.resumeJob();
  assert.match(messages.at(-1),/server_restarted/);
});

test('opening maintenance does not replay completed jobs or announce a new result',async()=>{
  for (const state of ['idle','completed']) {
    const requests=[],messages=[],events=[],locks=[];
    const context={window:{dispatchEvent:event=>events.push(event)},CustomEvent:class {},
      setStatus:message=>messages.push(message),setOperationBusy:value=>locks.push(value),recordJobResult(){},
      eventsReceipt:()=>null,databaseReceipt:()=>null,
      fjson:async url=>{requests.push(url);return {id:'old-job',action:'state-file',state,started_at:1,result:{ok:state==='completed'}};}};
    vm.runInNewContext(source.slice(source.indexOf('    async function followJob('),source.indexOf('    async function runOp(')),context);
    await context.resumeJob();
    assert.deepEqual(requests,['/api/maintenance/jobs/current']);
    assert.deepEqual(messages,[],state);
    assert.deepEqual(events,[],state);
    assert.deepEqual(locks,[true,false]);
  }
});

test('confirmation holds the operation lock against duplicate requests',async()=>{
  const fixture=runner();
  let release;
  fixture.context.window.confirm=()=>new Promise(resolve=>{release=resolve;});
  const pending=fixture.run('events-purge');
  assert.equal(fixture.context.operationBusy,true);
  await fixture.run('events-purge');
  assert.equal(fixture.posts.length,0);
  release(true);
  await pending;
  assert.equal(fixture.posts.length,1);
  assert.equal(fixture.context.operationBusy,false);
});
