/* tests/tracker-titles.test.mjs */
/* CrossWatch - Filtered My titles bulk selection */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/tracker.js", import.meta.url), "utf8");
const selection = source.slice(source.indexOf("  function selectableItems("), source.indexOf("  function setAddMenu("));

function statusSave(getJSON, view = "history") {
  const state = {instance:"CW-P01",view,offset:50,picked:new Set(["first","second"]),picking:false};
  const messages = [];
  let refreshed = false;
  const context = vm.createContext({state,getJSON,statusOf:status=>["watching","completed"].includes(status),listFeature:()=>"history",renderSlots:()=>{},plural:(n,label)=>`${n} ${label}`,writeError:error=>error.message,undo:(...args)=>args,refresh:async()=>{refreshed=true;},toast:{show:(text,options)=>messages.push({text,...options})}});
  const code = source.slice(source.indexOf("  async function applyBulkStatus("),source.indexOf("  async function saveNotesBulk("));
  vm.runInContext(code,context);
  return {state,messages,run:status=>{context.status=status;return vm.runInContext("applyBulkStatus(status)",context);},refreshed:()=>refreshed};
}

for (const view of ["history","shows","titles"]) {
  test(`${view}: menu status applies directly to selected titles and offers undo`,async()=>{
    let sent;
    const app=statusSave(async(url,body)=>{sent={url,body};return {count:2,batch:"batch"};},view);
    await app.run("watching");
    assert.equal(sent.url,view==="titles"?"/api/tracker/notes/bulk":"/api/tracker/titles/status");
    assert.equal(sent.body.provider_instance,"CW-P01");
    assert.deepEqual(Array.from(sent.body.keys),["first","second"]);
    assert.equal(sent.body.action,"status");
    assert.equal(sent.body.status,"watching");
    assert.equal(sent.body.view,view==="titles"?undefined:view);
    assert.equal(app.state.picked.size,0);
    assert.equal(app.state.bulkStatusBusy,false);
    assert.equal(app.refreshed(),true);
    assert.deepEqual(app.messages[0].undo(),["batch","CW-P01"]);
  });
}

test("failed menu status saves keep selected titles available for retry",async()=>{
  const app=statusSave(async()=>{throw Error("offline");});
  await app.run("completed");
  assert.deepEqual([...app.state.picked],["first","second"]);
  assert.equal(app.state.bulkStatusBusy,false);
  assert.equal(app.refreshed(),false);
  assert.equal(app.messages[0].tone,"bad");
});

test("clearing status is supported and repeated clicks cannot submit twice",async()=>{
  let finish, calls=0, sent;
  const app=statusSave((url,body)=>{calls++;sent=body;return new Promise(resolve=>{finish=resolve;});});
  const pending=app.run("");
  await app.run("completed");
  assert.equal(calls,1);
  assert.equal(sent.status,"");
  finish({count:2});
  await pending;
  assert.equal(app.state.bulkStatusBusy,false);
});

function setup(getJSON, overrides = {}) {
  const state = {view:"titles",instance:"CW-P01",q:"Show",noteStatus:"dropped",noteTag:"Later",total:205,request:1,picked:new Set(["previous"]),noteRows:[{key:"note"}],items:[{key:"history"}],...overrides};
  const errors = [];
  const context = vm.createContext({state,URLSearchParams,getJSON,document:{activeElement:null},renderSlots:()=>{},listFeature:()=>"history",timelineOn:()=>false,errorText:String,toast:{show:error=>errors.push(error)}});
  vm.runInContext(`let pickRequest = 0; ${selection}`, context);
  return {state,errors,all:()=>vm.runInContext("pickAllResults()",context),visible:()=>vm.runInContext("selectableItems()",context),cancel:()=>vm.runInContext("pickRequest += 1; state.request += 1; state.picking = false; state.picked.clear();",context)};
}

test("all results selects filtered titles across pages using their note keys", async () => {
  const calls = [];
  const rows = Array.from({length:205},(_,i)=>({key:`show:tmdb:${i+1}`}));
  const {state,all,visible} = setup(async url => {
    const parsed = new URL(url,"https://tracker.test");
    calls.push(parsed);
    const offset = Number(parsed.searchParams.get("offset"));
    return {notes:rows.slice(offset,offset+200),total:rows.length};
  });
  assert.equal(visible(),state.noteRows);
  await all();
  assert.equal(state.picked.size,205);
  assert(!state.picked.has("previous"));
  assert(state.picked.has("show:tmdb:205"));
  assert.equal(state.picking,false);
  assert.deepEqual(calls.map(u=>u.searchParams.get("offset")),["0","200"]);
  for(const url of calls) {
    assert.equal(url.pathname,"/api/tracker/notes");
    for(const [key,value] of Object.entries({provider_instance:"CW-P01",q:"Show",status:"dropped",tag:"Later"})) assert.equal(url.searchParams.get(key),value);
  }
});

test("changing filters while selection loads cannot apply stale title keys", async () => {
  let finish;
  const {state,all,cancel} = setup(()=>new Promise(resolve=>{finish=resolve;}));
  const pending = all();
  cancel();
  finish({notes:[{key:"stale"}],total:1});
  await pending;
  assert.equal(state.picked.size,0);
  assert.equal(state.picking,false);
});

test("selection failures preserve the current selection", async () => {
  const {state,all,errors} = setup(async()=>{throw Error("offline");});
  await all();
  assert.deepEqual([...state.picked],["previous"]);
  assert.equal(state.picking,false);
  assert.equal(errors.length,1);
});

for (const [view,field] of [["titles","notes"],["history","items"],["shows","shows"]]) {
  test(`${view}: all results selects more than 1000 filtered rows`, async () => {
    const rows=Array.from({length:1301},(_,i)=>({key:`row:${i}`}));
    const calls=[];
    const app=setup(async url=>{
      const parsed=new URL(url,"https://tracker.test");
      const offset=Number(parsed.searchParams.get("offset"));
      calls.push(offset);
      assert.equal(parsed.searchParams.get("provider_instance"),"CW-P01");
      assert.equal(parsed.searchParams.get("status"),"dropped");
      assert.equal(parsed.searchParams.get("q"),"Show");
      return {[field]:rows.slice(offset,offset+200),total:rows.length};
    },{view,total:rows.length});
    const render=source.slice(source.indexOf("  function renderSelection("),source.indexOf("  function emptyState("));
    const html=vm.runInNewContext(render+"renderSelection()",{state:app.state,icon:()=>"",esc:String,num:String});
    assert.doesNotMatch(html,/data-trk-action="pick-all" disabled/);
    await app.all();
    assert.equal(app.state.picked.size,1301);
    assert(app.state.picked.has("row:1300"));
    assert.deepEqual(calls,[0,200,400,600,800,1000,1200]);
    assert.equal(app.state.picking,false);
  });
}

test("history selection continues to use record keys and the items endpoint", async () => {
  let path;
  const {state,all,visible} = setup(async url=>{path=url;return {items:[{key:"watch"}],total:1};},{view:"history",total:1});
  assert.equal(visible(),state.items);
  await all();
  assert.match(path,/^\/api\/tracker\/items\?/);
  const query = new URL(path,"https://tracker.test").searchParams;
  assert.equal(query.get("status"),"dropped");
  assert.equal(query.get("tag"),"Later");
  assert.deepEqual([...state.picked],["watch"]);
});

const save = source.slice(source.indexOf("  async function saveNotesBulk("), source.indexOf("  async function deleteKeys("));

function bulkSave(getJSON) {
  const state = {instance:"CW-P01",offset:50,picked:new Set(["note"]),ref:{note:"note"},title:{}};
  const sheet = {keys:["note"],noteAction:"remove",bulkStatus:null};
  let refreshed = false, closed = false, undoBatch;
  const toast = {};
  const context = vm.createContext({state,sheet,getJSON,renderSheet:()=>{},closeSheet:()=>{closed=true;},plural:(n,label)=>`${n} ${label}`,writeError:error=>error.message,undo:batch=>{undoBatch=batch;},refresh:async()=>{refreshed=true;},toast:{show:(text,options)=>{Object.assign(toast,{text,...options});}}});
  return {state,sheet,toast,run:()=>vm.runInContext(save+"saveNotesBulk()",context),result:()=>({refreshed,closed,undoBatch})};
}

test("removing the last page returns to the first page and offers batch undo", async () => {
  let sent;
  const test = bulkSave(async(url,body)=>{sent={url,body};return {count:1,batch:"batch"};});
  await test.run();
  assert.equal(sent.url,"/api/tracker/notes/bulk");
  assert.equal(sent.body.provider_instance,"CW-P01");
  assert.deepEqual(sent.body.keys,["note"]);
  assert.equal(test.state.offset,0);
  assert.equal(test.state.picked.size,0);
  assert.equal(test.state.ref,null);
  test.toast.undo();
  assert.deepEqual(test.result(),{refreshed:true,closed:true,undoBatch:"batch"});
});

test("failed bulk changes keep selection and the dialog available for retry", async () => {
  const test = bulkSave(async()=>{throw Error("not_found");});
  await test.run();
  assert.equal(test.sheet.busy,false);
  assert.equal(test.sheet.error,"not_found");
  assert.deepEqual([...test.state.picked],["note"]);
  assert.equal(test.state.offset,50);
  assert.deepEqual(test.result(),{refreshed:false,closed:false,undoBatch:undefined});
});


test("show selection uses show identities and keeps filters across every page", async () => {
  const calls = [];
  const shows = Array.from({length:201},(_,i)=>({key:`tmdb:${i}`}));
  const {state,all,visible} = setup(async url => {
    const parsed = new URL(url,"https://tracker.test");
    calls.push(parsed);
    const offset = Number(parsed.searchParams.get("offset"));
    return {shows:shows.slice(offset,offset+200),total:shows.length};
  },{view:"shows",shows:shows.slice(0,50),hideDropped:true});
  assert.equal(visible(),state.shows);
  await all();
  assert.equal(state.picked.size,201);
  assert(state.picked.has("tmdb:200"));
  for (const url of calls) {
    assert.equal(url.pathname,"/api/tracker/shows");
    assert.equal(url.searchParams.get("status"),"dropped");
    assert.equal(url.searchParams.get("tag"),"Later");
    assert.equal(url.searchParams.get("hide_dropped"),"true");
  }
});
