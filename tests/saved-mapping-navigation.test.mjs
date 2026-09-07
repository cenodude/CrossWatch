/* tests/saved-mapping-navigation.test.mjs */
/* CrossWatch - Saved mapping navigation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {navigateToMapping} from "../assets/js/editor/saved-mappings.js";

const mapping = {provider:"SIMKL",instance:"second",feature:"history",key:"tmdb:286801#s01e05"};
function context() {
  const target = {key:mapping.key,_rid:72};
  const state = {source:"state",snapshot:"PLEX",instance:"default",kind:"ratings",filter:"unrelated",
    typeFilter:{movie:true,episode:false},blockedOnly:true,page:3,pageSize:50,rows:[{key:"old"}],hasChanges:false};
  const calls=[];
  const ctx={state,calls,clearFilter(){state.filter="";},syncSourceUI(){},syncTypeFilterUI(){},
    async loadSnapshots(){calls.push([state.source,state.snapshot,state.instance,state.kind]);},
    async loadState(){state.rows=[...Array.from({length:60},(_,i)=>({key:String(i)})),target];state.hasChanges=false;},
    sortRows:rows=>rows,renderRows(){},persistUIState(){calls.push("persist");},
    restoreUI(){calls.push("restore");},focusMapping(row){calls.push(row);}};
  return {ctx,target};
}

test("opens the correct provider/profile/feature and focuses a mapping beyond the first page",async()=>{
  const {ctx,target}=context();
  const focus=await navigateToMapping(mapping,ctx);
  assert.deepEqual(ctx.calls[0],["manual","SIMKL","second","history"]);
  assert.equal(ctx.state.filter,""); assert.equal(ctx.state.blockedOnly,false);
  assert.equal(ctx.state.typeFilter.episode,true); assert.equal(ctx.state.page,1);
  assert.equal(ctx.state.hasChanges,false);
  focus(); assert.equal(ctx.calls.at(-1),target);
});

test("cancelling preserves unsaved work without loading another scope",async()=>{
  const {ctx}=context(); ctx.state.hasChanges=true;
  globalThis.window={confirm:()=>false};
  try {
    const before=structuredClone(ctx.state);
    assert.equal(await navigateToMapping(mapping,ctx),false);
    assert.deepEqual(ctx.state,before); assert.deepEqual(ctx.calls,[]);
  } finally {delete globalThis.window;}
});

test("unavailable profiles and missing mappings restore the previous Editor",async()=>{
  for (const fail of ["profile","removed","load"]) {
    const {ctx}=context(), before=structuredClone(ctx.state);
    if(fail==="profile") ctx.loadSnapshots=async()=>{ctx.state.instance="default";};
    if(fail==="removed") ctx.loadState=async()=>{ctx.state.rows=[];};
    if(fail==="load") ctx.loadState=async()=>{ctx.state.loadError=new Error("offline");};
    await assert.rejects(navigateToMapping(mapping,ctx));
    assert.equal(ctx.state.source,before.source); assert.equal(ctx.state.snapshot,before.snapshot);
    assert.deepEqual(ctx.state.rows,before.rows); assert.deepEqual(ctx.state.typeFilter,before.typeFilter);
    assert.equal(ctx.calls.at(-1),"restore"); assert.ok(!ctx.calls.includes("persist"));
  }
});
