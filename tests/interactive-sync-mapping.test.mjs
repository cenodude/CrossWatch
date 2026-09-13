/* tests/interactive-sync-mapping.test.mjs */
/* CrossWatch - Batch episode correction tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import {correctedItem, correctedEpisode, mappingGroups, sameSeason, seriesTitle} from "../assets/js/interactive-sync-mapping.js";

test("a season can move to a separate show while preserving episode order and watched dates", () => {
  const originals = Array.from({length:10}, (_,i) => ({type:"episode", series_title:"Monster", title:`S02E${i+1}`,
    ids:{tvdb:String(1000+i)}, show_ids:{tmdb:"113988"}, season:2, episode:i+1, watched_at:"2024-09-19T08:00:00Z"}));
  const before = structuredClone(originals);
  const corrections = originals.map(item => correctedItem(item, {ids:{tmdb:"1398"}, title:"Another season title", season:1}));
  assert.deepEqual(originals, before);
  assert.deepEqual(corrections.map(item => item.episode), [1,2,3,4,5,6,7,8,9,10]);
  for (const item of corrections) {
    assert.deepEqual(item.ids, {tmdb:"1398"});
    assert.deepEqual(item.show_ids, {tmdb:"1398"});
    assert.equal(item.season, 1);
    assert.equal(item.series_title, "Another season title");
    assert.equal(item.watched_at, "2024-09-19T08:00:00Z");
  }
});

test("episode offsets preserve gaps instead of renumbering by table order", () => {
  const episodes = [21,23,27].map(episode => correctedItem({type:"episode",season:2,episode}, {season:1,offset:-20}));
  assert.deepEqual(episodes.map(item=>item.episode), [1,3,7]);
});

test("season grouping respects source, destination, feature and season", () => {
  const row = {source:"PLEX",source_instance:"one",provider:"SIMKL",instance:"two",feature:"history",
    item:{type:"episode",series_title:"Monster",season:2,episode:1}};
  assert.equal(sameSeason(row, {...row,item:{...row.item,episode:9}}), true);
  for (const field of ["source","source_instance","provider","instance","feature"]) {
    assert.equal(sameSeason(row,{...row,[field]:"other"}),false);
  }
  assert.equal(sameSeason(row,{...row,item:{...row.item,season:3}}),false);
  assert.equal(sameSeason(row,{...row,item:{...row.item,series_title:"Another show"}}),false);
});

test("show searches use the series title without episode suffixes", () => {
  assert.equal(seriesTitle({title:"Monster · S02E09"}), "Monster");
  assert.equal(seriesTitle({title:"Episode name",series_title:"Monster"}), "Monster");
});

test("nonadjacent episodes group together without mixing destinations or movies", () => {
  const episode = {source:"SIMKL", source_instance:"default", provider:"MDBLIST", instance:"one", feature:"history",
    item:{type:"episode", title:"Monster", season:3, episode:1}};
  const movie = {...episode,item:{type:"movie",title:"Monster"}};
  assert.deepEqual(mappingGroups([episode,movie,{...episode,item:{...episode.item,episode:2}},
    {...episode,instance:"two"},movie]), [[0,2],[1],[3],[4]]);
});

test("episode suggestions keep enriched IDs for the same show and discard IDs for a replaced show", () => {
  const original = {type:"episode",title:"Monster",season:3,episode:5,watched_at:"2025-10-03T08:00:00Z",
    ids:{tmdb:"286801",imdb:"tt1234567",mdblist:"abc"}};
  const match = {title:"Monster: The Ed Gein Story",ids:{tmdb:"286801"},season:1,episode:5};
  const same = correctedEpisode(original,match);
  assert.deepEqual(same.show_ids,original.ids);
  assert.equal(same.season,1);
  assert.equal(same.episode,5);
  assert.equal(same.watched_at,original.watched_at);
  assert.deepEqual(correctedEpisode(original,{...match,ids:{tmdb:"999"}}).show_ids,{tmdb:"999"});
});


const workspaceSource=readFileSync(new URL("../assets/js/interactive-sync-mapping.js",import.meta.url),"utf8");
function recoveryWorkspaceFixture() {
  const nodes=new Map(),calls=[],saved=[];
  const $=id=>{if(!nodes.has(id))nodes.set(id,{value:"recovery",querySelectorAll:()=>[]});return nodes.get(id);};
  const make=(id,title)=>({row:{id,item:{type:"show",title,ids:{}}},item:{type:"show",title,ids:{}},checked:true,dirty:false});
  const drafts=[make("1","Known"),make("2","Ambiguous"),make("3","Unavailable")];
  const sandbox=vm.createContext({$,drafts,recovery:true,correctedItem,seriesTitle,AbortController,
    searchController:null,searching:false,saving:false,closed:false,backgroundRun:null,metadataLanguage:()=>"nl-NL",updateLanguage(){},refreshSearchSelects(){},lock(){},status(text){sandbox.statusText=text;},
    paint(d){d.dirty=true;},draftStatus(){},showSearch(){},matchEpisodes:async()=>0,
    search:async(row)=>{
      calls.push(row.id);
      return {results:row.id==="1" ? [{title:"Known",year:2001,ids:{tmdb:"1"},exact_title:true}] :
        row.id==="2" ? [{title:"Ambiguous",year:2001,ids:{tmdb:"2"},exact_title:true},{title:"Ambiguous",year:2005,ids:{tmdb:"3"},exact_title:true}] : []};
    },api:{save:async(edits)=>{saved.push(edits);return {};}},dialog:{close(){},remove(){}},onSaved(){}});
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf("  const saveable ="),workspaceSource.indexOf("  const cache =")),sandbox);
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf('  $("[data-suggest]").onclick'),workspaceSource.indexOf('  $("[data-bulk]").onclick')),sandbox);
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf('  $("[data-save]").onclick'),workspaceSource.indexOf('  return {close, destroy()')),sandbox);
  return {sandbox,drafts,calls,saved,auto:()=>$("[data-suggest]").onclick(),save:()=>$("[data-save]").onclick()};
}

test("recovery auto match stages only unambiguous suggestions and retry preserves staged matches",async()=>{
  const f=recoveryWorkspaceFixture();
  await f.auto();
  assert.deepEqual(f.calls,["1","2","3"]);
  assert.deepEqual(f.drafts.map(d=>d.dirty),[true,false,false]);
  assert.equal(f.drafts[0].item.year,2001);
  assert.equal(f.saved.length,0);
  await f.auto();
  assert.deepEqual(f.calls,["1","2","3","2","3"]);
  await f.save();
  assert.deepEqual(Array.from(f.saved[0],edit=>edit.row_id),["1"]);
});

test("unchecked recovery suggestions are excluded from saving",async()=>{
  const f=recoveryWorkspaceFixture();
  await f.auto();
  f.drafts[0].checked=false;
  await f.save();
  assert.equal(f.saved.length,0);
});

test("stopping recovery search keeps staged suggestions without saving them",async()=>{
  const f=recoveryWorkspaceFixture();
  f.sandbox.search=async row=>{
    if(row.id==="2") {
      f.sandbox.searchController.abort();
      const error=new Error("Stopped");error.name="AbortError";throw error;
    }
    return {results:[{title:"Known",ids:{tmdb:"1"},exact_title:true}]};
  };
  await f.auto();
  assert.deepEqual(f.drafts.map(d=>d.dirty),[true,false,false]);
  assert.equal(f.saved.length,0);
  assert.equal(f.sandbox.searching,false);
  assert.match(f.sandbox.statusText,/Stopped/);
});


test("background auto match warns before starting and cancelling the warning sends nothing",async()=>{
  const f=recoveryWorkspaceFixture(),started=[],warnings=[];
  f.sandbox.window={confirm:text=>{warnings.push(text);return false;}};
  f.sandbox.background=false;
  f.sandbox.api.autoStart=async(ids,catalog,language)=>{started.push({ids:[...ids],catalog,language});return {auto_match:{status:"running"}};};
  f.sandbox.syncBackground=data=>{f.sandbox.state=data;};
  await f.auto();
  assert.equal(started.length,0);
  assert.match(warnings[0],/3 movie\/show titles/);
  assert.match(warnings[0],/heavy API traffic/);
  f.sandbox.window.confirm=()=>true;
  await f.auto();
  assert.deepEqual(started[0].ids,["1","2","3"]);
  assert.equal(started[0].language,"nl-NL");
  assert.equal(f.calls.length,0);
  assert.equal(f.saved.length,0);
  assert.equal(f.sandbox.state.auto_match.status,"running");
});

test("background suggestions restore without overwriting manual edits or saving",()=>{
  const f=recoveryWorkspaceFixture();
  f.sandbox.background=false;f.sandbox.backgroundTimer=null;
  f.sandbox.clearTimeout=()=>{};f.sandbox.setTimeout=()=>42;
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf("  function syncBackground("),workspaceSource.indexOf('  $("[data-suggest]").onclick')),f.sandbox);
  f.sandbox.syncBackground({auto_match:{id:"run",status:"complete",catalog:"tmdb",language:"nl-NL",done:3,total:3,matched:1,message:"Ready"},
    suggestions:{"1":{match:{title:"Known",ids:{tmdb:"1"},year:2001}},"2":{match:null,reason:"Ambiguous"}}});
  assert.equal(f.drafts[0].item.ids.tmdb,"1");
  assert.equal(f.drafts[0].persisted,JSON.stringify(f.drafts[0].item));
  assert.equal(f.drafts[1].review,"Ambiguous");
  assert.equal(f.saved.length,0);
  assert.equal(f.sandbox.$("[data-language]").value,"nl-NL");
  assert.equal(f.sandbox.$("[data-catalog]").value,"tmdb");
  f.sandbox.$("[data-language]").value="de-DE";
  f.drafts[0].item.title="Manually corrected";
  f.sandbox.syncBackground({auto_match:{},suggestions:{"1":{match:{title:"Known",ids:{tmdb:"1"}}}}});
  assert.equal(f.drafts[0].item.title,"Manually corrected");
  f.sandbox.syncBackground({auto_match:{id:"run",status:"running",done:1,total:3,matched:1,message:"Searching",current_title:"Movie"}});
  assert.equal(f.sandbox.background,true);
  assert.equal(f.sandbox.searching,true);
  assert.match(f.sandbox.statusText,/1 \/ 3 titles checked/);
  assert.equal(f.sandbox.$("[data-language]").value,"de-DE");
});

test("manual metadata searches keep language caches separate and hide language for unsupported catalogs", async()=>{
  const nodes=new Map(),calls=[];
  const $=key=>{if(!nodes.has(key))nodes.set(key,{value:"",hidden:false});return nodes.get(key);};
  const row={provider:"PLEX",instance:"default",item:{type:"show"}};
  const context=vm.createContext({$,recovery:true,first:row,drafts:[{row,checked:true}],cache:new Map(),searchController:null,
    catalogs:new Map([["route",[{id:"tmdb",supports_language:true},{id:"trakt",supports_language:false}]]]),routeKey:()=>"route",
    api:{search:async(_row,_term,catalog,options)=>{calls.push([catalog,options.language]);return {results:[]};}}});
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf("  function metadataLanguage("),workspaceSource.indexOf("  if (recovery) {\n    const searchOptionsChanged")),context);
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf("  async function search("),workspaceSource.indexOf("  async function find(")),context);
  $("[data-catalog]").value="tmdb";
  $("[data-language]").value="en-US";
  context.updateLanguage();
  assert.equal($("[data-language-wrap]").hidden,false);
  await context.search(row,"De verborgen tuin");
  $("[data-language]").value="nl-NL";
  await context.search(row,"De verborgen tuin");
  await context.search(row,"De verborgen tuin");
  assert.deepEqual(calls,[["tmdb","en-US"],["tmdb","nl-NL"]]);
  $("[data-catalog]").value="trakt";
  context.updateLanguage();
  assert.equal($("[data-language-wrap]").hidden,true);
  await context.search(row,"De verborgen tuin");
  assert.deepEqual(calls.at(-1),["trakt","en-US"]);
});
