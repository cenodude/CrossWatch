/* tests/plex-recovery-selection.test.mjs */
/* CrossWatch - Plex recovery selection tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const source=readFileSync(new URL("../assets/js/import-export/plex-recovery.js",import.meta.url),"utf8");
function fixture() {
  const nodes=new Map(),handlers={};
  const $=selector=>{
    if(!nodes.has(selector)) nodes.set(selector,{innerHTML:"",textContent:""});
    return nodes.get(selector);
  };
  const sandbox=vm.createContext({$,host:{},all:false,chosen:new Set(),excluded:new Set(),busy:false,reading:false,loading:false,
    rows:[{id:"0",title:"Known",status:"ready"},{id:"1",title:"Guess",status:"needs_review"},{id:"2",title:"Invalid",status:"invalid"}],
    data:{summary:{total:3,ready:1,by_status:{needs_review:1}},filtered_total:3},offset:0,
    filters:()=>({status:"all"}),controls(){},on(_host,type,handler){handlers[type]=handler;}});
  vm.runInContext(source.slice(0,source.indexOf("export function mountPlexRecovery")),sandbox);
  for (const [start,end] of [["  function count()","  function controls()"],["  function render()","  async function page("],['  on(host,"click"','  on($("#pr-search")']]) {
    vm.runInContext(source.slice(source.indexOf(start),source.indexOf(end)),sandbox);
  }
  const click=id=>handlers.click({target:{closest:()=>({id,disabled:false,dataset:{},closest:()=>null})}});
  const check=(id,checked)=>handlers.change({target:{dataset:{select:id},checked}});
  const row=id=>$("#pr-rows").innerHTML.split("</tr>").find(html=>html.includes(`data-select="${id}"`));
  return {sandbox,click,check,row};
}

test("Select all skips title guesses; checking a guess adds only that explicit selection",async()=>{
  const {sandbox,click,check,row}=fixture();
  await click("pr-all");
  assert.equal(sandbox.count(),1);
  assert.match(row("0"),/ checked /);
  assert.doesNotMatch(row("1"),/ checked | disabled/);
  assert.match(row("1"),/Confirm title match and select Guess/);
  check("1",true);
  assert.equal(sandbox.count(),2);
  assert.deepEqual([...sandbox.chosen],["1"]);
  assert.match(row("1"),/ checked /);
  check("0",false);
  assert.equal(sandbox.count(),1);
  check("1",false);
  assert.equal(sandbox.count(),0);
  check("2",true);
  assert.equal(sandbox.count(),0);
});

test("bulk selection resets individual guesses and review-only bulk selection stays empty",async()=>{
  const {sandbox,click,check}=fixture();
  check("1",true);
  assert.equal(sandbox.count(),1);
  await click("pr-all");
  assert.equal(sandbox.chosen.size,0);
  sandbox.filters=()=>({status:"needs_review"});
  await click("pr-all");
  assert.equal(sandbox.count(),0);
  check("1",true);
  assert.equal(sandbox.count(),1);
  await click("pr-none");
  assert.equal(sandbox.count(),0);
});

test("accepting title matches confirms the filtered batch without importing or selecting it",async()=>{
  const nodes=new Map(),calls=[];
  const $=id=>{if(!nodes.has(id))nodes.set(id,{value:"needs_review",dispatchEvent(){}});return nodes.get(id);};
  const sandbox=vm.createContext({$,Event,CustomEvent,number:String,busy:false,all:true,chosen:new Set(["old"]),excluded:new Set(["skip"]),
    job:{id:"scan",revision:4},data:{summary:{by_status:{needs_review:125}}},lifetime:{signal:{aborted:false}},
    window:{confirm(text){calls.push(["confirm",text]);return true;},dispatchEvent(){}},controls(){},
    filters:()=>({q:"Antlers",status:$("#pr-result").value}),
    async request(path,body){calls.push([path,body]);return {revision:5,accepted:125};},
    async page(){calls.push(["page"]);},message(text){calls.push(["message",text]);}});
  vm.runInContext(source.slice(source.indexOf("  async function acceptTitleMatches()"),source.indexOf("  async function bulkMatch()")),sandbox);
  await sandbox.acceptTitleMatches();
  assert.match(calls[0][1],/125.*across all pages/);
  assert.equal(calls[1][0],"/scan/accept-title-matches");
  assert.equal(calls[1][1].q,"Antlers");
  assert.equal(calls[1][1].revision,4);
  assert.equal($("#pr-result").value,"ready");
  assert.equal(sandbox.job.revision,5);
  assert.equal(sandbox.all,false);
  assert.equal(sandbox.chosen.size,0);
  assert.equal(sandbox.excluded.size,0);
  assert.equal(sandbox.busy,false);
  assert(!calls.some(([path])=>path.includes("commit")));
  calls.length=0;
  sandbox.window.confirm=()=>false;
  await sandbox.acceptTitleMatches();
  assert.equal(calls.length,0);
});

test("live activity shows candidate counts and escapes recent titles",()=>{
  const nodes=new Map();
  const $=id=>{if(!nodes.has(id))nodes.set(id,{});return nodes.get(id);};
  const job={activity:{checked:40,found:5,skipped:35,current_item:"Series <Special>",current_action:"Looking up identity"},
    recent:[{title:"Old movie",result:"Skipped: still in Plex"},{title:"<img src=x onerror=alert(1)>",result:"Found: needs a match"}]};
  const sandbox=vm.createContext({$,job});
  vm.runInContext(source.slice(0,source.indexOf("export function mountPlexRecovery")),sandbox);
  vm.runInContext(source.slice(source.indexOf("  function renderActivity()"),source.indexOf("  async function start()")),sandbox);
  sandbox.renderActivity();
  assert.equal($("#pr-activity").hidden,false);
  assert.match($("#pr-live-counts").textContent,/40 checked.*5 found for review.*35 skipped/);
  assert.equal($("#pr-current").textContent,"Looking up identity: Series <Special>");
  const html=$("#pr-recent").innerHTML;
  assert(!html.includes("<img"));
  assert(html.indexOf("&lt;img")<html.indexOf("Old movie"));
  job.activity={};job.recent=[];
  sandbox.renderActivity();
  assert.equal($("#pr-activity").hidden,true);
  assert.equal($("#pr-recent-wrap").hidden,true);
});


function searchFixture() {
  const nodes=new Map(),handlers={},pending=[],timers=new Map();
  let active=null, timerId=0;
  const $=id=>{
    if(!nodes.has(id)) {
      const node={id:id.slice(1),value:"",dataset:{},closest:selector=>selector === ".ie-filters" && ["#pr-search","#pr-result"].includes(id),
        focus(){active=this;},selectionStart:3};
      let disabled=false;
      Object.defineProperty(node,"disabled",{get:()=>disabled,set(value){disabled=value;if(value && active===node)active=null;}});
      nodes.set(id,node);
    }
    return nodes.get(id);
  };
  $("#pr-source").value=$("#pr-target").value="default";
  $("#pr-result").value="all";
  const sandbox=vm.createContext({$,window:{},host:{querySelectorAll:()=>[...nodes.values()]},URLSearchParams,
    busy:false,reading:false,loading:false,otherRecovery:false,all:false,chosen:new Set(["0"]),excluded:new Set(),
    job:{id:"scan"},data:{summary:{ready:1},filtered_total:1},rows:[],offset:0,sequence:0,searchTimer:null,
    options:{targets:[{id:"default",connected:true}]},receipt:null,lifetime:{signal:{aborted:false}},
    setTimeout(fn){timers.set(++timerId,fn);return timerId;},clearTimeout(id){timers.delete(id);},
    request(path){return new Promise((resolve,reject)=>pending.push({path,resolve,reject}));},
    message(text){sandbox.error=text;},render(){sandbox.controls();},on(_el,type,fn){handlers[type]=fn;}});
  vm.runInContext(source.slice(0,source.indexOf("export function mountPlexRecovery")),sandbox);
  for(const [start,end] of [["  function filters()","  function render()"],["  async function page(","  async function poll()"],['  on($("#pr-search"),"input"','  $("#pr-result").innerHTML']]) {
    vm.runInContext(source.slice(source.indexOf(start),source.indexOf(end)),sandbox);
  }
  return {sandbox,$,pending,get active(){return active;},
    input(value){$("#pr-search").value=value;handlers.input();},
    flush(){const tasks=[...timers.values()];timers.clear();return Promise.all(tasks.map(fn=>fn()));}};
}

test("search keeps focus during debounce and requests, and blocks stale selections",async()=>{
  const f=searchFixture(), search=f.$("#pr-search");
  search.focus();
  f.input("Bet");
  assert.equal(f.active,search);
  assert.equal(search.disabled,false);
  assert.equal(f.$("#pr-import").disabled,true);
  assert.equal(f.sandbox.chosen.size,0);
  const first=f.flush();
  assert.equal(f.active,search);
  assert.equal(search.selectionStart,3);
  f.pending[0].resolve({rows:[{id:"first"}],summary:{ready:1}});
  await first;
  assert.equal(f.active,search);
  f.input("Better");
  const second=f.flush();
  assert.equal(f.active,search);
  f.pending[1].resolve({rows:[{id:"second"}],summary:{ready:1}});
  await second;
  assert.equal(f.active,search);
  assert.equal(f.sandbox.rows[0].id,"second");
});

test("typing invalidates older results before debounce and ignores out-of-order failures",async()=>{
  const f=searchFixture();
  f.input("B");const first=f.flush();
  f.input("Be");
  f.pending[0].resolve({rows:[{id:"stale"}]});await first;
  assert.equal(f.sandbox.rows.length,0);
  assert.equal(f.sandbox.loading,true);
  const second=f.flush();
  f.input("Better");const third=f.flush();
  f.pending[2].resolve({rows:[{id:"latest"}],summary:{}});await third;
  f.pending[1].reject(new Error("old failure"));await second;
  assert.equal(f.sandbox.rows[0].id,"latest");
  assert.equal(f.sandbox.error,undefined);
  assert.equal(f.sandbox.loading,false);
});

test("failed current search clears stale rows and leaves search focused for retry",async()=>{
  const f=searchFixture(), search=f.$("#pr-search");
  search.focus();f.input("Better");const done=f.flush();
  f.pending[0].reject(new Error("Search failed"));await done;
  assert.equal(f.sandbox.rows.length,0);
  assert.equal(f.$("#pr-import").disabled,true);
  assert.equal(f.active,search);
  assert.equal(f.sandbox.error,"Search failed");
});


test("returning restores the server recovery without starting or discarding it",async()=>{
  const nodes=new Map(),calls=[];
  const $=id=>{if(!nodes.has(id))nodes.set(id,{value:""});return nodes.get(id);};
  const sandbox=vm.createContext({$,job:null,otherRecovery:false,stoppingRecovery:false,controls(){},
    request:async path=>{calls.push(path);return {job:{id:"existing",source_instance:"plex2",target_instance:"tracker2"},busy:true};},
    updateScope(){},poll:async()=>{calls.push("poll");},message(){}});
  vm.runInContext(source.slice(source.indexOf("  async function resume()"),source.indexOf("  async function closeRecovery()")),sandbox);
  await sandbox.resume();
  assert.equal(sandbox.job.id,"existing");
  assert.equal($("#pr-source").value,"plex2");
  assert.equal($("#pr-target").value,"tracker2");
  assert.deepEqual(calls,["/active","poll"]);
  sandbox.job=null;
  sandbox.request=async()=>({job:null,busy:true});
  await sandbox.resume();
  assert.equal(sandbox.otherRecovery,true);
  assert.equal(sandbox.job,null);
});

test("opening a shortcut preserves the active recovery and its source",()=>{
  const sandbox=vm.createContext({window:{CW:{pendingPlexRecovery:{source_instance:"another"}}},
    job:{id:"existing"},options:{},busy:false,reading:true,loading:false,
    $(){assert.fail("An active recovery must not change source");}});
  vm.runInContext(source.slice(source.indexOf("  function openRequestedSource()"),source.indexOf("  function updateScope()")),sandbox);
  sandbox.openRequestedSource();
  assert.equal(sandbox.job.id,"existing");
  assert.equal(sandbox.window.CW.pendingPlexRecovery,null);
});

test("recovery notification changes when ready or imported, with a resumable route",()=>{
  const text=readFileSync(new URL("../assets/js/plex-recovery-notifications.js",import.meta.url),"utf8"),values=[];
  const sandbox=vm.createContext({window:{CW:{Notifications:{setSource:(name,value)=>values.push({name,...value})}}}});
  vm.runInContext(text.slice(text.indexOf("  function render("),text.indexOf("  async function refresh()")),sandbox);
  sandbox.render({id:"one",status:"reading"});
  sandbox.render({id:"one",status:"review"});
  sandbox.render({id:"one",status:"review",imported:5});
  assert.equal(values[0].items[0].action,"View progress");
  assert.match(values[1].items[0].detail,/Ready to review/);
  assert.notEqual(values[0].items[0].revision,values[1].items[0].revision);
  assert.match(values[2].items[0].detail,/5 items imported/);
  assert.notEqual(values[1].items[0].revision,values[2].items[0].revision);
  assert.equal(values[2].items[0].href,"/#import_export?recovery=1");
  sandbox.render();
  assert.equal(values.at(-1).items.length,0);
  const core=readFileSync(new URL("../assets/helpers/core.js",import.meta.url),"utf8");
  vm.runInContext(core.slice(core.indexOf("  function routeHash("),core.indexOf("  function ",core.indexOf("  function routeHash(")+10)),sandbox);
  sandbox.window.location={hash:"#import_export?recovery=1"};
  assert.equal(sandbox.routeHash("import_export"),"#import_export?recovery=1");
});


function pollingFixture() {
  const nodes=new Map(),paths=[],pages=[];
  const $=id=>{
    if(!nodes.has(id))nodes.set(id,{style:{},setAttribute(){},removeAttribute(){},querySelector(){return {style:{}};}});
    return nodes.get(id);
  };
  const current={id:"scan",import_id:"scan",status:"review",revision:1,auto_match:{id:"auto",status:"running",done:1,total:300}};
  const sandbox=vm.createContext({$,job:{...current},data:{...current},offset:100,receipt:{keys:[]},
    pollSequence:0,timer:null,reading:false,lifetime:{signal:{aborted:false}},
    window:{dispatchEvent(){}},CustomEvent:function(){},clearTimeout(){},setTimeout:()=>1,
    number:String,controls(){},renderActivity(){},message(){},render(){},
    request:async path=>{paths.push(path);return path.endsWith("/receipt") ? {keys:[]} : {...current};},
    page:async offset=>{pages.push(offset);sandbox.data={...current};}});
  vm.runInContext(source.slice(source.indexOf("  async function poll()"),source.indexOf("  function renderActivity()")),sandbox);
  return {sandbox,current,paths,pages};
}

test("auto progress polling preserves page and fetches rows only when the preview changes",async()=>{
  const f=pollingFixture();
  await f.sandbox.poll();
  f.current.auto_match.done=2;
  await f.sandbox.poll();
  assert.deepEqual(f.paths,["/scan","/scan"]);
  assert.deepEqual(f.pages,[]);
  assert.equal(f.sandbox.offset,100);
  f.current.revision=2;
  await f.sandbox.poll();
  assert.deepEqual(f.pages,[100]);
  assert.equal(f.paths.at(-1),"/scan/receipt");
  await f.sandbox.poll();
  assert.deepEqual(f.pages,[100]);
  f.sandbox.data=null;
  await f.sandbox.poll();
  assert.deepEqual(f.pages,[100,100]);
});

test("late progress response cannot reopen a closed recovery",async()=>{
  const f=pollingFixture();
  let finish;
  f.sandbox.request=()=>new Promise(resolve=>{finish=resolve;});
  const pending=f.sandbox.poll();
  ++f.sandbox.pollSequence;
  f.sandbox.job=null;
  finish(f.current);
  await pending;
  assert.equal(f.sandbox.job,null);
  assert.deepEqual(f.pages,[]);
});

test("closing recovery waits for its worker without restoring the review",async()=>{
  const states=[{job:null,busy:true,stopping:true},{job:null,busy:false}],messages=[],timers=[];
  const sandbox=vm.createContext({job:null,otherRecovery:false,stoppingRecovery:false,timer:null,
    request:async()=>states.shift(),controls(){},message:text=>messages.push(text),
    clearTimeout(){},setTimeout:fn=>{timers.push(fn);return timers.length;},poll(){assert.fail("Closed jobs must not be polled");}});
  vm.runInContext(source.slice(source.indexOf("  async function resume()"),source.indexOf("  async function closeRecovery()")),sandbox);
  await sandbox.resume();
  assert.equal(sandbox.job,null);
  assert.equal(sandbox.otherRecovery,true);
  assert.match(messages[0],/last provider request/);
  await timers[0]();
  assert.equal(sandbox.job,null);
  assert.equal(sandbox.otherRecovery,false);
  assert.match(messages.at(-1),/start a new scan/);
});

function expiredFixture() {
  const f=searchFixture(),calls=[];
  Object.assign(f.sandbox,{api:"/api/import/plex-recovery",pollSequence:0,timer:null,stoppingRecovery:false,
    editing:{id:"old"},matchingWorkspace:{destroy(){calls.push("destroy");}},CustomEvent:function(){},
    fetch:async(url,options)=>{calls.push({url,options});return {ok:false,status:404,json:async()=>({detail:"Recovery expired or not found. Start another scan."})};}});
  f.sandbox.window.dispatchEvent=()=>{};
  f.$("#pr-match").close=()=>{calls.push("close dialog");};
  vm.runInContext(source.slice(source.indexOf("  async function request("),source.indexOf("  function message(")),f.sandbox);
  vm.runInContext(source.slice(source.indexOf("  function render()"),source.indexOf("  async function page(")),f.sandbox);
  return {...f,calls};
}

test("expired recovery responses release controls and clear stale previews for every session action",async()=>{
  for(const path of ["/scan","/scan/rows?q=old","/scan/match-groups","/scan/match","/scan/commit","/scan/receipt","/scan/auto-match"]) {
    const f=expiredFixture();
    Object.assign(f.sandbox,{busy:true,reading:true,loading:true,all:true,receipt:{keys:["old"]}});
    f.$("#pr-search").value="old";
    await assert.rejects(f.sandbox.request(path),{status:404});
    assert.equal(f.sandbox.job,null,path);
    assert.equal(f.sandbox.data,null);
    assert.equal(f.sandbox.receipt,null);
    assert.equal(f.sandbox.editing,null);
    assert.equal(f.sandbox.matchingWorkspace,null);
    assert.equal(f.sandbox.chosen.size,0);
    assert.equal(f.sandbox.all,false);
    assert.equal(f.$("#pr-start").disabled,false);
    assert.equal(f.$("#pr-source").disabled,false);
    assert.equal(f.$("#pr-target").disabled,false);
    assert.equal(f.$("#pr-close").hidden,true);
    assert.equal(f.$("#pr-review").hidden,true);
    assert.equal(f.$("#pr-progress").hidden,true);
    assert.equal(f.$("#pr-search").value,"");
    assert.match(f.sandbox.error,/Start another scan/);
    assert.ok(f.calls.includes("destroy"));
  }
});

test("an expired results page can start a new scan without remounting",async()=>{
  const f=expiredFixture();
  await f.sandbox.page();
  assert.equal(f.$("#pr-start").disabled,false);
  f.sandbox.fetch=async(url,options)=>{
    f.calls.push({url,options});
    return {ok:true,json:async()=>({id:"new-scan",status:"reading"})};
  };
  f.sandbox.poll=async()=>{};
  vm.runInContext(source.slice(source.indexOf("  async function start()"),source.indexOf("  async function resume()")),f.sandbox);
  await f.sandbox.start();
  assert.equal(f.sandbox.job.id,"new-scan");
  assert.equal(f.sandbox.reading,true);
  assert.equal(f.$("#pr-start").disabled,true);
  assert.equal(f.calls.at(-1).options.method,"POST");
  assert.deepEqual(JSON.parse(f.calls.at(-1).options.body),{source_instance:"default",target_instance:"default"});
});

test("closing an already expired recovery unlocks the next scan",async()=>{
  const f=expiredFixture();
  vm.runInContext(source.slice(source.indexOf("  async function closeRecovery()"),source.indexOf("  async function commit()")),f.sandbox);
  await f.sandbox.closeRecovery();
  assert.equal(f.sandbox.job,null);
  assert.equal(f.$("#pr-start").disabled,false);
  assert.equal(f.$("#pr-close").hidden,true);
});

test("unrelated errors and expired responses from an older scan preserve the current recovery",async()=>{
  for(const [path,status,detail] of [["/old-scan/rows",404,"Recovery expired or not found. Start another scan."],
    ["/scan/match-search",404,"Unresolved movie or series not found"],["/scan/rows",502,"Provider unavailable"]]) {
    const f=expiredFixture();
    f.sandbox.fetch=async()=>({ok:false,status,json:async()=>({detail})});
    await assert.rejects(f.sandbox.request(path),{status});
    assert.equal(f.sandbox.job.id,"scan");
    assert.equal(f.sandbox.chosen.size,1);
    assert.ok(!f.calls.includes("destroy"));
  }
});
