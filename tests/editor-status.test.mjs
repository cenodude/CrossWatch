/* tests/editor-status.test.mjs */
/* CrossWatch - Provider status identity matching and read-only display. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const context = vm.createContext({window:{}});
vm.runInContext(readFileSync(new URL("../assets/js/editor/rows.js",import.meta.url),"utf8"),context);
const display = context.window.CW.Editor.Rows.statusForRow;
const snapshot = {data:{supported:true,records:[{media:"shows",ids:{tmdb:"1",tvdb:"10"},status:"on_hold"},{media:"movies",ids:{tmdb:"1"},status:"dropped"}]}};

test("show and episode statuses use show identities and do not modify saved items",()=>{
  const row={raw:{type:"episode",ids:{tmdb:"999"},show_ids:{tmdb:"1"}}};
  const before=JSON.stringify(row);
  assert.equal(display(row,snapshot).label,"On hold");
  assert.equal(JSON.stringify(row),before);
  assert.equal(display({raw:{type:"movie",ids:{tmdb:"1"}}},snapshot).label,"Dropped");
  assert.equal(display({raw:{type:"episode",ids:{tmdb:"1"}}},snapshot).label,"Unknown");
});

test("conflicting identifiers, duplicate matches and missing observations stay unknown",()=>{
  assert.equal(display({raw:{type:"show",ids:{tmdb:"1",tvdb:"11"}}},snapshot).label,"Unknown");
  const row={raw:{type:"show",ids:{tmdb:"1"}}};
  assert.equal(display(row,{data:{supported:true,records:[snapshot.data.records[0],snapshot.data.records[0]]}}).label,"Unknown");
  assert.equal(display(row,{data:{supported:true,records:[{...snapshot.data.records[0],status:null}]}}).label,"Unknown");
  assert.equal(display(row,{loading:true}).label,"Loading…");
  assert.equal(display(row,{error:true}).label,"Unavailable");
  assert.equal(display(row,{data:{supported:false}}).label,"—");
  assert.equal(display(row,null).label,"—");
});

test("status reads are lazy, cached for the loaded rows and ignore old instance responses",async()=>{
  const source=readFileSync(new URL("../assets/js/editor.js",import.meta.url),"utf8");
  const fn=source.slice(source.indexOf("  function syncStatusColumn()"),source.indexOf("  function renderRows()"));
  const requests=[];
  let visible=false, renders=0;
  const state={rows:[{}],snapshot:"TRAKT",instance:"default",source:"state"};
  const ctx=vm.createContext({state,URLSearchParams,ALL_PROVIDERS:"__all__",columnShown:()=>visible,
    isProviderPickerSource:()=>true,isMergedView:()=>false,updateStatusCells:()=>renders++,
    fetchJSON:url=>new Promise((resolve,reject)=>requests.push({url,resolve,reject}))});
  vm.runInContext(fn,ctx);
  ctx.syncStatusColumn();
  assert.equal(requests.length,0);
  visible=true;
  ctx.syncStatusColumn(); ctx.syncStatusColumn();
  assert.equal(requests.length,1);
  state.instance="P01";
  ctx.syncStatusColumn();
  assert.match(requests[1].url,/provider_instance=P01/);
  requests[0].resolve({supported:true,records:[]});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(renders,0);
  requests[1].reject(new Error("Read failed"));
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(renders,1);
  assert.equal(state.statusSnapshot.error,true);
  ctx.syncStatusColumn();
  assert.equal(requests.length,2);
  state.rows=[{}];
  ctx.syncStatusColumn();
  assert.equal(requests.length,3);
  state.rows=[];
  ctx.syncStatusColumn();
  requests[2].resolve({supported:true,records:[]});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(state.statusSnapshot,null);
  assert.equal(renders,1);
});
