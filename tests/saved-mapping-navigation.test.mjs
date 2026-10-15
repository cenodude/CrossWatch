/* tests/saved-mapping-navigation.test.mjs */
/* CrossWatch - Saved mapping navigation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
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
  assert.deepEqual(ctx.calls[0],["state","SIMKL","second","history"]);
  assert.equal(ctx.state.filter,""); assert.equal(ctx.state.blockedOnly,false);
  assert.equal(ctx.state.typeFilter.episode,true); assert.equal(ctx.state.page,1);
  assert.equal(ctx.state.hasChanges,false);
  focus(); assert.equal(ctx.calls.at(-1),target);
});

test("the saved mapping pencil opens the Editor mapping dialog after the list closes", async () => {
  const {ctx, target} = context();
  const source = readFileSync(new URL("../assets/js/editor.js", import.meta.url), "utf8");
  const start = source.indexOf("      focusMapping(row) {");
  const end = source.indexOf("\n      },", start);
  assert.ok(start >= 0 && end > start);
  let listOpen = true;
  const calls = [];
  const input = {closest:() => ({scrollIntoView(){}}), focus(){calls.push("focus");}};
  const workspace = {open:true};
  ctx.focusMapping = vm.runInNewContext(`({${source.slice(start, end)}\n}}).focusMapping`, {
    tbody:{querySelector(selector){assert.equal(selector, `[name="cw-row-${target._rid}-title"]`);return input;}},
    openItemReplacer(row, anchor) {
      assert.equal(listOpen, false);
      assert.equal(row, target);
      assert.equal(anchor, input);
      assert.equal(ctx.state.snapshot, mapping.provider);
      assert.equal(ctx.state.instance, mapping.instance);
      assert.equal(ctx.state.kind, mapping.feature);
      calls.push("open");
      return Promise.resolve(workspace);
    },
  });
  const afterClose = await navigateToMapping(mapping, ctx);
  assert.deepEqual(calls, []);
  listOpen = false;
  assert.equal(await afterClose(), workspace);
  assert.deepEqual(calls, ["focus", "open"]);
});

test("saved pair mappings open their own scope and shared mappings reset it", async () => {
  const {ctx} = context();
  await navigateToMapping({...mapping, pair_id:"pair-one"}, ctx);
  assert.equal(ctx.state.mappingPair, "pair-one");
  await navigateToMapping(mapping, ctx);
  assert.equal(ctx.state.mappingPair, "");
});

test("mapping navigation counts only visible current-state rows for pagination", async () => {
  const {ctx,target} = context();
  ctx.loadState = async () => {ctx.state.rows=[...Array.from({length:60},(_,i)=>({key:String(i),deleted:true})),target];};
  ctx.applyFilter = rows => rows.filter(row => !row.deleted);
  await navigateToMapping(mapping,ctx);
  assert.equal(ctx.state.page,0);
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


test("saved rules open across all scopes regardless of Editor filters", async () => {
  const source = readFileSync(new URL("../assets/js/editor/saved-mappings.js", import.meta.url), "utf8");
  for (const mappingPair of ["", "pair-one"]) {
    const elements = new Map();
    const element = selector => {
      if (!elements.has(selector)) elements.set(selector, {value:"", textContent:"", innerHTML:"",
        addEventListener(){}, prepend(){}, setAttribute(){}});
      return elements.get(selector);
    };
    const tabs = ["mapping", "block"].map(view => ({dataset:{view}, setAttribute(){}}));
    const root = {open:false, setAttribute(){}, querySelector:element,
      querySelectorAll:selector => selector === "[data-view]" ? tabs : [],
      addEventListener(){}, showModal(){this.open=true;}};
    const requests = [];
    const editor = {state:{source:"state", mappingPair, snapshot:"CROSSWATCH", instance:"default", kind:"history"}};
    const before = structuredClone(editor.state);
    const sandbox = {
      document:{getElementById:() => ({}), createElement:tag => tag === "dialog" ? root : {},
        body:{append(){}}, addEventListener(){}},
      window:{CW:{OverviewProfile:{id:"",ready:Promise.resolve()}}, addEventListener(){}},
      URLSearchParams, AbortController, setTimeout, clearTimeout,
      fetch:async url => {requests.push(new URL(url,"http://localhost"));
        return {ok:true,status:200,json:async()=>({sources:[],total:0,items:[]})};},
    };
    vm.runInNewContext(source.replaceAll("export ", ""), sandbox);
    sandbox.open(null, editor);
    await new Promise(resolve => setImmediate(resolve));
    tabs[1].onclick();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(requests.length, 2);
    assert.deepEqual(requests.map(url => url.searchParams.get("entry_type")), ["mapping", "block"]);
    for (const url of requests) {
      assert.equal(url.searchParams.has("pair_id"), false);
      assert.equal(url.searchParams.has("provider"), false);
      assert.equal(url.searchParams.get("feature"), "");
    }
    assert.deepEqual(editor.state, before);
    element("[data-source]").value = JSON.stringify(["PUNCHPLAY", "default"]);
    element("[data-feature]").value = "history";
    element("[data-feature]").onchange();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(requests.at(-1).searchParams.get("provider"), "PUNCHPLAY");
    assert.equal(requests.at(-1).searchParams.get("feature"), "history");
    assert.equal(requests.at(-1).searchParams.has("pair_id"), false);
  }
});
