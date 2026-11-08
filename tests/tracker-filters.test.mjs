/* tests/tracker-filters.test.mjs */
/* CrossWatch - Tracker filter controls and request consistency */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/tracker.js", import.meta.url), "utf8");
const renders = source.slice(source.indexOf("  function renderNoteFilters("), source.indexOf("  function renderControls("));
const main = source.slice(source.indexOf("  async function loadMain("), source.indexOf("  let progressQueue"));
const months = source.slice(source.indexOf("  async function loadMonths("), source.indexOf("  let summaryRequest"));

function setup(view) {
  const state = {view,instance:"CW-P01",noteStatus:"dropped",noteTag:"Sci-fi",q:"Show",media:"show",sort:"recent",pageSize:50,offset:0,request:0,filtersOpen:false,month:"",hideDropped:false,notes:{total:3,statuses:{dropped:2},tags:[{tag:"Sci-fi",count:2}]}};
  const requests = [];
  const context = vm.createContext({state,URLSearchParams,inst:()=>"provider_instance=CW-P01",getJSON:async path=>{requests.push(new URL(path,"https://tracker.test"));return {total:0,items:[],shows:[],notes:[],months:[]};},loadProgress:()=>{},renderSlots:()=>{},renderMonths:()=>{},errorText:String,listFeature:()=>view === "cleanup" ? "history" : view,timelineOn:()=>view === "history",TIMELINE_PAGE:200,esc:String,num:String,STATUSES:[{id:"dropped",label:"Dropped"}],MEDIA:[],symbol:()=>"",chip:()=>"",FEATURES:[{id:"history",label:"History"}],HEALTH:[],healthOf:()=>({})});
  vm.runInContext(`let monthsRequest=0; ${renders} ${main} ${months}`,context);
  return {state,requests,run:code=>vm.runInContext(code,context)};
}

test("filters stay visible as an active count when the panel is collapsed", () => {
  const {state,run} = setup("history");
  assert.match(run("renderFilters()"),/Filters \(2\)/);
  assert.match(run("renderFilters()"),/aria-expanded="false"/);
  assert.match(run("renderExtraFilters()"),/ hidden/);
  state.filtersOpen = true;
  const html = run("renderExtraFilters()");
  assert.doesNotMatch(html,/ hidden/);
  assert.match(html,/value="none">No status/);
  assert.match(html,/Sort results/);
  assert.match(html,/Clear filters/);
  assert.match(html,/value="dropped" selected/);
});

test("titles keep their direct filters and a selected tag remains clear after deletion", () => {
  const {state,run} = setup("titles");
  state.notes.tags = [];
  assert.match(run("renderFilters()"),/value="Sci-fi" selected/);
  assert.match(run("renderFilters()"),/All statuses \(3\)/);
  assert.equal(run("renderExtraFilters()"),"");
  assert.deepEqual(state.notes.tags,[]);
});

for (const view of ["history","watchlist","ratings","progress","collection","cleanup","shows","titles"]) {
  test(`${view} sends the same status and tag filters to its results endpoint`, async () => {
    const {requests,run} = setup(view);
    await run("loadMain()");
    assert.equal(requests.length,1);
    const request = requests[0];
    assert.equal(request.pathname,`/api/tracker/${view === "titles" ? "notes" : view === "shows" ? "shows" : "items"}`);
    for(const [key,value] of Object.entries({provider_instance:"CW-P01",status:"dropped",tag:"Sci-fi",q:"Show"})) assert.equal(request.searchParams.get(key),value);
  });
}

test("timeline month counts receive the same title filters as history results", async () => {
  const {requests,run} = setup("history");
  await run("loadMonths()");
  assert.equal(requests[0].pathname,"/api/tracker/months");
  assert.equal(requests[0].searchParams.get("status"),"dropped");
  assert.equal(requests[0].searchParams.get("tag"),"Sci-fi");
});


test("duplicate review reaches the history list and timeline consistently", async () => {
  const {state,requests,run} = setup("history");
  state.duplicates = true;
  await run("loadMain()");
  await run("loadMonths()");
  assert.ok(requests.every(request=>request.searchParams.get("duplicates")==="true"));
  assert.match(run("renderFilters()"),/Filters \(3\)/);
});
