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
  let now = Date.now();
  const state = {view:"history",instance:"desk",layouts:{history:"grid"},media:"show",pageSize:50,q:"Severance",sort:"title",noteStatus:"watching",noteTag:"Sci-fi",duplicates:true,offset:50,scrollTop:750,ref:{key:"episode",feature:"history"},picked:new Set(["episode"])};
  state.viewSelectedAt = now;
  const context = vm.createContext({state,Date:class extends Date { static now() { return now; } },STORE_KEY:"prefs",VIEWS:[{id:"overview"},{id:"history"},{id:"shows"}],MEDIA:[{id:"show"}],PAGE_SIZES:[50],
    sheet:{mode:"views",views:[],el:{querySelector:()=>({value:"Watching TV"})}},renderSheet(){},closeSheet(){context.sheet.mode="";},refresh(){},
    localStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value)}});
  vm.runInContext(prefs+views+`function choose(index,remove=false) {const event={target:{closest:()=>({dataset:{trkSavedView:index,remove:String(remove)}})}};${click}}`,context);
  return {context,state,storage,advance:milliseconds=>{now+=milliseconds;}};
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

test("monthly-review preference survives reload without replacing the underlying view", () => {
  const {context:app,state,storage} = setup();
  state.monthlyReview = true;
  app.saveView();
  state.monthlyReview = false;
  app.loadView();
  assert.equal(state.monthlyReview,true);
  assert.equal(state.view,'history');
  storage.set('prefs',JSON.stringify({view:'history',monthlyReview:'true'}));
  app.loadView();
  assert.equal(state.monthlyReview,false);
});

test("view choices expire at one hour without being extended by routine saves or visits", () => {
  const {context:app,state,storage,advance} = setup();
  app.rememberView();
  const selectedAt = state.viewSelectedAt;
  advance(3599999);
  app.saveView();
  app.loadView();
  assert.equal(state.view,'history');
  assert.equal(state.viewSelectedAt,selectedAt);
  assert.equal(app.expireView(),false);
  advance(1);
  app.loadView();
  assert.equal(state.view,'overview');
  assert.equal(state.ref,null);
  assert.equal(state.offset,0);
  assert.equal(state.scrollTop,0);
  assert.equal(state.monthlyReview,false);
  assert.equal(JSON.parse(storage.get('prefs')).viewSelectedAt,selectedAt);
});

test("legacy, missing, invalid and future timestamps default to Overview", () => {
  for (const timestamp of [undefined,null,0,'123',Date.now()+3600000]) {
    const {context:app,state,storage} = setup();
    storage.set('prefs',JSON.stringify({view:'shows',monthlyReview:true,viewSelectedAt:timestamp,ref:{key:'old'},offset:50,scrollTop:800}));
    app.loadView();
    assert.equal(state.view,'overview');
    assert.equal(state.monthlyReview,false);
    assert.equal(state.ref,null);
  }
});

test("selecting another view starts a fresh hour", () => {
  const {context:app,state,advance} = setup();
  app.rememberView();
  advance(3500000);
  state.view='shows';
  app.rememberView();
  advance(100000);
  app.loadView();
  assert.equal(state.view,'shows');
  state.monthlyReview=true;
  app.rememberView();
  advance(3600000);
  assert.equal(app.expireView(),true);
  assert.equal(state.view,'overview');
  assert.equal(state.monthlyReview,false);
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

test("saved filter text stays escaped and cannot overwrite unrelated state", () => {
  const {context:app,state,storage} = setup();
  const markup = '\"><img src=x onerror=alert(1)>';
  state.statusMenu = false;
  state.title = {ratings:[{rating:8}]};
  state.notes = {tags:[]};
  storage.set("prefs", JSON.stringify({q:markup,noteStatus:markup,noteTag:markup,cleanFeature:"history",healthFilter:"attention",
    viewSelectedAt:state.viewSelectedAt,byDay:markup,statusMenu:markup,title:markup}));
  app.loadView();
  assert.equal(state.q,markup);
  assert.equal(state.noteStatus,markup);
  assert.equal(state.noteTag,markup);
  assert.equal(state.cleanFeature,"history");
  assert.equal(state.healthFilter,"attention");
  assert.equal(state.byDay,false);
  assert.equal(state.statusMenu,false);
  assert.equal(state.title.ratings[0].rating,8);
  app.STATUSES = [];
  app.num = String;
  const escape = source.slice(source.indexOf("  const esc ="),source.indexOf("\n",source.indexOf("  const esc =")));
  const filters = source.slice(source.indexOf("  function renderNoteFilters()"),source.indexOf("  function renderFilters()"));
  vm.runInContext(escape+filters,app);
  const html = app.renderNoteFilters();
  assert(!html.includes("<img"));
  assert(html.includes("&quot;&gt;&lt;img src=x onerror=alert(1)&gt;"));
  storage.set("prefs",JSON.stringify({q:"x".repeat(300),noteStatus:42,noteTag:null,cleanFeature:[],healthFilter:{}}));
  app.loadView();
  assert.equal(state.q.length,200);
  assert.equal(state.noteStatus,markup);
  assert.equal(state.noteTag,markup);
  assert.equal(state.cleanFeature,"history");
  assert.equal(state.healthFilter,"attention");
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
