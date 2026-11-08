/* tests/tracker-views.test.mjs */
/* CrossWatch - Saved Tracker views and browsing preferences */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/tracker.js", import.meta.url), "utf8");
const prefs = source.slice(source.indexOf("  function loadView("), source.indexOf("  async function getJSON("));
const views = source.slice(source.indexOf("  function readSavedViews("), source.indexOf("  function renderNotesBulk("));
const click = source.slice(source.indexOf('    const saved = sheet.mode === "views"'), source.indexOf('    const undoBtn = event.target.closest'));

function setup() {
  const storage = new Map();
  const state = {view:"history",instance:"desk",layouts:{history:"grid"},media:"show",pageSize:50,q:"Severance",sort:"title",noteStatus:"watching",noteTag:"Sci-fi",duplicates:true,offset:50,scrollTop:750,ref:{key:"episode",feature:"history"},picked:new Set(["episode"])};
  const context = vm.createContext({state,STORE_KEY:"prefs",VIEWS:[{id:"history"},{id:"shows"}],MEDIA:[{id:"show"}],PAGE_SIZES:[50],
    sheet:{mode:"views",views:[],el:{querySelector:()=>({value:"Watching TV"})}},renderSheet(){},closeSheet(){context.sheet.mode="";},refresh(){},
    localStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value)}});
  vm.runInContext(prefs+views+`function choose(index,remove=false) {const event={target:{closest:()=>({dataset:{trkSavedView:index,remove:String(remove)}})}};${click}}`,context);
  return {context,state,storage};
}

test("browsing resumes with filters, page and selected title", () => {
  const {context:app,state} = setup();
  app.saveView();
  Object.assign(state,{q:"",noteStatus:"",noteTag:"",offset:0,scrollTop:0,ref:null,duplicates:false});
  app.loadView();
  assert.equal(state.q,"Severance");
  assert.equal(state.noteStatus,"watching");
  assert.equal(state.noteTag,"Sci-fi");
  assert.equal(state.offset,50);
  assert.equal(state.scrollTop,750);
  assert.equal(state.ref.key,"episode");
  assert.equal(state.duplicates,true);
});

test("saved views restore filters and layout without carrying a stale selection or page", () => {
  const {context:app,state} = setup();
  app.saveNamedView();
  app.sheet.views = app.readSavedViews();
  assert.equal(app.sheet.views.length,1);
  Object.assign(state,{view:"shows",q:"wrong",noteStatus:"dropped",instance:"default",layouts:{history:"list"}});
  app.choose(0);
  assert.equal(state.view,"history");
  assert.equal(state.instance,"desk");
  assert.equal(state.layouts.history,"grid");
  assert.equal(state.noteStatus,"watching");
  assert.equal(state.q,"Severance");
  assert.equal(state.offset,0);
  assert.equal(state.scrollTop,0);
  assert.equal(state.ref,null);
  assert.equal(state.picked.size,0);
  app.sheet.mode="views";
  app.choose(0,true);
  assert.equal(app.readSavedViews().length,0);
});

test("duplicate names and corrupt browser storage do not overwrite saved views", () => {
  const {context:app,storage} = setup();
  app.saveNamedView();
  app.saveNamedView();
  assert.match(app.sheet.error,/already exists/);
  assert.equal(app.readSavedViews().length,1);
  storage.set("cw.tracker.saved-views.v1","invalid");
  assert.equal(app.readSavedViews().length,0);
  storage.set("prefs",JSON.stringify({offset:-5,scrollTop:"bad",sort:"invalid",ref:"bad"}));
  app.loadView();
  assert.equal(app.state.offset,0);
  assert.equal(app.state.scrollTop,0);
});


test("returning from an import report selects its Tracker and clears stale title references", () => {
  const {context:app,state} = setup();
  const fn = source.slice(source.indexOf("  function readLink()"), source.indexOf("  async function mount("));
  app.URLSearchParams = URLSearchParams;
  app.window = {location:{hash:"#tracker?instance=CW-P02",pathname:"/",search:""},history:{state:null,replaceState(){}}};
  vm.runInContext(fn+"readLink();",app);
  assert.equal(state.instance,"CW-P02");
  assert.equal(state.ref,null);
  assert.equal(state.offset,0);
});
