/* tests/tracker-grid.test.mjs */
/* CrossWatch - Tracker cover grid and layout preferences */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/tracker.js", import.meta.url), "utf8");
const renderer = source.slice(source.indexOf("  function renderGrid("), source.indexOf("  function renderList("));
const esc = value => String(value ?? "").replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;");

function render(state) {
  return vm.runInNewContext(renderer + "renderGrid()", {
    window:{CW:{AuthState:{read:()=>({permissions:{write:true}})}}},
    state: {picked:new Set(),items:[],shows:[],noteRows:[],ref:null,...state}, esc,
    listFeature:()=>state.view, cardTitle:item=>item.series_title || item.title,
    cardItem:item=>item.art?.tmdb || item.ids?.tmdb ? item : null, canCard:Boolean,
    artUrl:item=>item.art ? `/art/tmdb/${item.art.kind}/${item.art.tmdb}?size=w500` : "",
    isChild:item=>item.type === "episode", typeMeta:item=>({label:item.type,icon:"movie"}),
    episodeCode:item=>item.episode ? `S${item.season}E${item.episode}` : "", se:(s,e)=>`S${s}E${e}`,
    timelineOn:()=>!!state.byDay, dayLabel:String, timeText:()=>"22:00", whenText:String,
    symbol:()=>"icon", icon:()=>"icon", healthIcon:()=>"", progressCell:()=>"progress", nextCell:()=>"next",
    statusPill:status=>status ? `<span class="trk-status" data-status="${status}">${status}</span>` : "",tagChips:()=>"",extraCell:()=>"",pager:()=>"pager",
    renderShows:()=>"empty shows",renderTitles:()=>"empty titles",renderList:()=>"empty list",
  });
}

test("record cards retain half ratings, episode artwork, selection and editing", () => {
  const html = render({view:"history",items:[{key:"episode",title:"<Title>",type:"episode",season:1,episode:2,art:{kind:"tv",tmdb:"42"},user_rating:7.5,date:"today"}],picked:new Set(["episode"]),ref:{key:"episode",feature:"history"}});
  assert.match(html,/kind=still&amp;season=1&amp;episode=2/);
  assert.match(html,/Your rating: 7.5 out of 10/);
  assert.match(html,/data-trk-pick="0" checked/);
  assert.match(html,/data-trk-row-edit="0"/);
  assert.match(html,/data-trk-row-info/);
  assert.match(html,/is-selected/);
  assert.match(html,/&lt;Title>/);
});

test("anime episode stills use the TMDB numbering from the artwork", () => {
  const html = render({view:"history",items:[{key:"anime",title:"Anime",type:"episode",season:3,episode:13,art:{kind:"tv",tmdb:"30",season:1,episode:1},date:"today"}]});
  assert.match(html,/\/art\/tmdb\/tv\/30\?size=w500&amp;kind=still&amp;season=1&amp;episode=1/);
});

test("undated history rows show no date", () => {
  const whenText = vm.runInNewContext(source.slice(source.indexOf("  function whenText("), source.indexOf("  function timeText(")) + "whenText");
  assert.equal(whenText(null), "");
  assert.equal(whenText(""), "");
  assert.notEqual(whenText("2026-10-07T00:00:00Z"), "");
});

test("day groups do not duplicate headings and retain load more", () => {
  const items = [1,2].map(key=>({key:String(key),type:"movie",title:"Movie",date:"today"}));
  const html = render({view:"history",byDay:true,items,total:3});
  assert.equal((html.match(/trk-grid-day/g)||[]).length,1);
  assert.match(html,/data-trk-action="more"/);
  assert.match(html,/22:00/);
});

for (const view of ["history","shows","titles","watchlist","ratings","progress","collection","cleanup"]) {
  test(`${view}: optional saved status labels share the artwork footer with the date`,()=>{
    const items=[{key:"one",type:"movie",title:"One",date:"today",last_watched:"today",updated:"today",status:"dropped",user_rating:7.5},{key:"two",type:"movie",title:"Two",date:"today"}];
    const state={view,items,shows:items,noteRows:items};
    const html=render(state);
    assert.equal((html.match(/class="trk-grid-status"/g)||[]).length,1);
    assert.match(html,/trk-grid-labels.*trk-grid-date.*trk-grid-status/);
    assert.match(html,/data-status="dropped"/);
    assert.doesNotMatch(render({...state,showStatusLabels:false}),/class="trk-grid-status"/);
  });
}

test("show and note cards preserve their detail references", () => {
  assert.match(render({view:"shows",shows:[{key:"show",title:"Show"}]}),/data-trk-ref-show="show"/);
  assert.match(render({view:"titles",noteRows:[{key:"note",title:"Note"}]}),/data-trk-ref-note="note"/);
  assert.equal(render({view:"history"}),"empty list");
});

test("My titles cards have independent bulk selection without opening their details", () => {
  const html = render({view:"titles",noteRows:[{key:"note",title:"Note"},{key:"other",title:"Other"}],picked:new Set(["note"])});
  assert.match(html,/data-trk-pick="0" checked/);
  assert.match(html,/data-trk-pick="1" aria-label="Select Other"/);
  assert.equal((html.match(/is-picked/g)||[]).length,1);
  assert.doesNotMatch(html,/is-selected/);
  assert.match(render({view:"shows",shows:[{key:"show",title:"Show"}]}),/data-trk-pick="0"/);
});

function preferences(initial = {}) {
  const prefs = source.slice(source.indexOf("  function loadView("), source.indexOf("  async function getJSON("));
  const click = source.slice(source.indexOf("      if (layout) {"), source.indexOf('      const view = event.target.closest("[data-trk-view]")'));
  let saved = JSON.stringify(initial);
  const state = {view:"history",layouts:{},picked:new Set()};
  const views = ["history","shows","titles","watchlist","ratings","progress","collection","cleanup"];
  const context = vm.createContext({state,STORE_KEY:"test",VIEWS:views.map(id=>({id})),MEDIA:[],PAGE_SIZES:[],renderSlots:()=>{},localStorage:{getItem:()=>saved,setItem:(_,value)=>{saved=value;}}});
  vm.runInContext(prefs + "loadView();",context);
  vm.runInContext(`function choose(value) { const layout = {dataset:{trkLayout:value}}; ${click} }`,context);
  return {state,views,choose:value=>vm.runInContext(`choose(${JSON.stringify(value)})`,context),reload:()=>vm.runInContext("loadView()",context)};
}

test("each tab saves its own layout and restores it after reload", () => {
  const {state,views,choose,reload} = preferences();
  for (const view of views) assert.equal(state.layouts[view],"list");
  for (const [index,view] of views.entries()) {
    state.view = view;
    choose(index % 2 ? "list" : "grid");
  }
  state.layouts = {};
  reload();
  for (const [index,view] of views.entries()) {
    state.view = view;
    assert.equal(state.layouts[state.view],index % 2 ? "list" : "grid");
  }
  state.view = "history";
  choose("list");
  reload();
  assert.equal(state.layouts.history,"list");
  assert.equal(state.layouts.ratings,"grid");
});

test("legacy grid preference migrates only to the previously active tab", () => {
  const {state} = preferences({view:"ratings",layout:"grid"});
  assert.equal(state.layouts.ratings,"grid");
  assert.equal(state.layouts.history,"list");
  assert.equal(state.layouts.shows,"list");
});

test("saved per-tab choices override legacy preferences and reject invalid values", () => {
  const {state} = preferences({view:"history",layout:"grid",layouts:{history:"list",shows:"grid",ratings:"invalid",unknown:"grid"}});
  assert.equal(state.layouts.history,"list");
  assert.equal(state.layouts.shows,"grid");
  assert.equal(state.layouts.ratings,"list");
  assert.equal(state.layouts.unknown,undefined);
});
