/* tests/interactive-sync-mapping.test.mjs */
/* CrossWatch - Batch episode correction tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import {mappingAppliesToRow, candidateConflict, changeSummary, correctedItem, correctedEpisode, movieDraft, mappingGroups, sameSeason, seriesTitle} from "../assets/js/interactive-sync-mapping.js";

test("status conflicts show actual labels and default to leaving both sides unchanged", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const render = source.slice(source.indexOf("  function render()"), source.indexOf("  async function action("));
  const value = source.slice(source.indexOf("  function value("), source.indexOf("  const duration ="));
  const row = {id:"status-1",result:"conflict",feature:"history",key:"private-scope-json",source:"AniList",target:"SIMKL",winner:"",
    left:{title:"Example",watch_status:"on_hold",_cw_status_mapping:true},right:{title:"Example",watch_status:"dropped",_cw_status_mapping:true}};
  const update = {id:"status-2",result:"update",operation:"update",feature:"history",provider:"SIMKL",selectable:true,
    item:{title:"Other",type:"show",watch_status:"dropped",_cw_status_mapping:{from:"watching",to:"dropped"}},
    before:{watch_status:"watching",_cw_status_mapping:true}};
  const context = {host:{contains:()=>false,innerHTML:""},document:{activeElement:null},window:{},
    session:{status:"review",pair:{source:"ANILIST",target:"SIMKL"},counts:{changes:1,selected:0,conflicts:1},features:["history"]},
    pageData:{total:2,items:[row,update]},page:0,PAGE_SIZE:100,pageLoading:false,downloadOpen:false,downloading:false,
    query:"",feature:"",result:"",labels:{update:"Update"},pending:()=>false,origin:()=>["main","Main"],aiButton:()=>"",
    esc:value=>String(value ?? ""),endpoint:p=>p,progressHTML:()=>"",number:value=>Number(value || 0),title:item=>item.title,
    reasonDetailsHTML:()=>"",noticesHTML:()=>""};
  vm.createContext(context);
  vm.runInContext(value + render + "render();", context);
  assert.match(context.host.innerHTML,/AniList: On hold/);
  assert.match(context.host.innerHTML,/SIMKL: Dropped/);
  assert.match(context.host.innerHTML,/<option value="" selected>Leave unchanged<\/option>/);
  assert.doesNotMatch(context.host.innerHTML,/private-scope-json|data-map="status-2"/);
  assert.match(context.host.innerHTML,/Watching/);
  context.session.status_mappings = [{src:"TRAKT",dst:"WETRAKR",state:"complete",preview:true,matched:3,proposed:1,unchanged:2,updated:0}];
  vm.runInContext("render();", context);
  assert.match(context.host.innerHTML, /Status mapping/);
  assert.match(context.host.innerHTML, /Preview checked/);
  assert.match(context.host.innerHTML, /3 matched · 1 proposed · 2 unchanged/);
  row.winner = "SIMKL";
  vm.runInContext("render();", context);
  assert.match(context.host.innerHTML,/<option value="SIMKL" selected>Use SIMKL<\/option>/);
});

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

test("movie conversion discards episode identity while keeping the original watch and source unchanged", () => {
  const original = {type:"episode",title:"S01E01",series_title:"Miss 267970",series_year:2016,
    ids:{tvdb:"999"},show_ids:{tmdb:"267970"},season:1,episode:1,watched_at:"2017-02-08T23:33:27Z"};
  const draft = movieDraft(original);
  assert.equal(draft.type,"movie");
  assert.equal(draft.title,"Miss 267970");
  assert.equal(draft.watched_at,original.watched_at);
  assert.deepEqual(draft.ids,{});
  for (const key of ["show_ids","season","episode","series_title"]) assert.equal(draft[key],undefined);
  assert.equal(original.type,"episode");
  assert.equal(original.episode,1);
  assert.deepEqual(original.ids,{tvdb:"999"});
});

test("type conversion is unavailable to other mapping workspaces and requires a movie ID before saving", () => {
  const code = workspaceSource.slice(workspaceSource.indexOf("  const canConvert ="), workspaceSource.indexOf("  const cache ="));
  const row = {source:"CROSSWATCH",feature:"history",item:{type:"episode",ids:{},show_ids:{tmdb:"267970"},season:1,episode:1}};
  const evaluate = extra => vm.runInNewContext(code + "({allowed:canConvert,ready:saveable(drafts[0])})", {
    allowMovieConversion:true,direct:true,standalone:true,recovery:false,rows:[row],
    drafts:[{row,item:movieDraft(row.item),dirty:true}], ...extra,
  });
  assert.equal(evaluate({}).allowed,true);
  assert.equal(evaluate({}).ready,false);
  const ready = {row,item:{...movieDraft(row.item),ids:{tmdb:"267970"}},dirty:true};
  assert.equal(evaluate({drafts:[ready]}).ready,true);
  for (const extra of [{allowMovieConversion:false},{direct:false},{recovery:true},{rows:[row,row]},
    {rows:[{...row,source:"PLEX"}]},{rows:[{...row,feature:"ratings"}]}]) assert.equal(evaluate(extra).allowed,false);
});

function recoveryWorkspaceFixture() {
  const nodes=new Map(),calls=[],saved=[];
  const $=id=>{if(!nodes.has(id))nodes.set(id,{value:"recovery",querySelectorAll:()=>[]});return nodes.get(id);};
  const make=(id,title)=>({row:{id,item:{type:"show",title,ids:{}}},item:{type:"show",title,ids:{}},checked:true,dirty:false});
  const drafts=[make("1","Known"),make("2","Ambiguous"),make("3","Unavailable")];
  const sandbox=vm.createContext({$,drafts,recovery:true,converting:()=>false,correctedItem,seriesTitle,AbortController,
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
  return {sandbox,drafts,calls,saved,$,auto:()=>$("[data-suggest]").onclick(),save:()=>$("[data-save]").onclick()};
}

test("bulk recovery switches a selected series to movie search and saves the verified match", async () => {
  const f = recoveryWorkspaceFixture(), handlers = {}, candidates = [], searches = [];
  const draft = f.drafts[0];
  f.drafts.forEach((d, i) => { d.checked = i === 0; });
  Object.assign(f.sandbox, {canConvert:false, movieDraft, structuredClone, candidateConflict,
    esc:String, endpoint:()=>"PLEX", routeKey:()=>"PLEX", applyParts(){}, window:{},
    document:{createElement:()=>({setAttribute(){}, querySelector:()=>null})},
    search:async(row, query)=>{
      searches.push({type:row.item.type, query});
      return {catalog:"TMDb",results:[{type:"movie",title:"Miss Hokusai",year:2015,ids:{tmdb:"267970"}}]};
    }});
  f.sandbox.dialog.addEventListener = (name, fn) => { handlers[name] = fn; };
  Object.assign(f.$(".is-map-candidates"), {replaceChildren(){candidates.length=0;},append(button){candidates.push(button);}});
  f.$("[data-change-match]").focus = () => {};
  f.$("[data-catalog]").value = "tmdb";
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf('  dialog.addEventListener("change"'),
    workspaceSource.indexOf('  dialog.addEventListener("click"')), f.sandbox);
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf("  async function find(row)"),
    workspaceSource.indexOf('  dialog.addEventListener("input"')), f.sandbox);
  const changeType = value => handlers.change({target:{value,hasAttribute:name=>name === "data-media-type"}});
  changeType("movie");
  assert.equal(draft.item.type,"movie");
  assert.equal(draft.row.item.type,"show");
  await f.save();
  assert.equal(f.saved.length,0);
  f.$("[data-search-query]").value = "Miss Hokusai";
  await f.sandbox.find(draft.row);
  assert.deepEqual(searches,[{type:"movie",query:"Miss Hokusai"}]);
  assert.equal(candidates.length,1);
  await candidates[0].onclick();
  assert.equal(draft.item.ids.tmdb,"267970");
  assert.equal(draft.item.year,2015);
  await f.save();
  assert.equal(f.saved.length,1);
  assert.equal(f.saved[0].length,1);
  assert.equal(f.saved[0][0].item.type,"movie");
  assert.deepEqual(JSON.parse(JSON.stringify(f.saved[0][0].matchSearch)),
    {catalog:"tmdb",match_query:"Miss Hokusai",language:"nl-NL"});
  changeType("show");
  assert.deepEqual(draft.item,draft.row.item);
  assert.equal(draft.matchSearch,undefined);
  f.drafts[1].checked = true;
  changeType("movie");
  assert.equal(draft.item.type,"show");
});

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


test("episode groups open without closing or discarding the mapping workspace", async () => {
  const button = {}, error = {}, calls = [];
  const context = vm.createContext({
    document:{createElement:() => button}, saving:false, searching:false, closed:false,
    onEpisodeGroup:async trigger => calls.push(trigger),
    $:selector => selector === ".is-map-intro" ? {append() {}} : error,
  });
  button.dataset = {};
  vm.runInContext(workspaceSource.slice(workspaceSource.indexOf("  if (onEpisodeGroup) {"), workspaceSource.indexOf('  $("[data-save]").onclick')), context);
  assert.equal(button.textContent, "Map split episodes");
  await button.onclick();
  assert.equal(calls[0], button);
  assert.equal(context.closed, false);
  assert.equal(button.disabled, false);
  context.searching = true;
  await button.onclick();
  assert.equal(calls.length, 1);
  context.searching = false;
  context.onEpisodeGroup = async () => { throw new Error("Could not open groups"); };
  await button.onclick();
  assert.equal(error.textContent, "Could not open groups");
  assert.equal(button.disabled, false);
});

test("conflicting destination IDs can be deliberately mapped without changing history", () => {
  const original = {type:"movie", title:"Our Girls", ids:{tmdb:"1457705", imdb:"tt36474012"}, watched_at:"2026-09-10T14:33:50Z"};
  const candidate = {title:"Our Girls: The Southport Families", ids:{tmdb:"1595884", tvdb:"371950"}};
  assert.match(candidateConflict(original, candidate), /TMDB IDs differ/);
  const corrected = correctedItem(original, candidate);
  assert.deepEqual(corrected.ids, candidate.ids);
  assert.equal(corrected.watched_at, original.watched_at);
  assert.equal(original.ids.tmdb, "1457705");
  assert.equal(candidateConflict(original, {ids:{tmdb:"1457705"}}), "");
  assert.equal(candidateConflict(original, {ids:{tvdb:"371950"}}), "");
});

test("mapping removal only offers rules for this item, source instance and pair", () => {
  const row = {source:"JELLYFIN", source_instance:"default", feature:"history", key:"tmdb:1"};
  const rule = {provider:"JELLYFIN", instance:"default", feature:"history", original_key:"tmdb:1", key:"tmdb:2", pair_id:"p1"};
  assert.equal(mappingAppliesToRow(rule, row, "p1"), true);
  assert.equal(mappingAppliesToRow({...rule, pair_id:""}, row, "p1"), true);
  assert.equal(mappingAppliesToRow(rule, {...row, key:"tmdb:2"}, "p1"), true);
  for (const change of [{pair_id:"p2"}, {instance:"other"}, {provider:"EMBY"}, {feature:"ratings"}, {original_key:"tmdb:3"}]) {
    assert.equal(mappingAppliesToRow({...rule, ...change}, row, "p1"), false);
  }
});


test("matching evidence escapes provider metadata and renders legacy rows", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  function matchDetailsHTML(row)"), source.indexOf("  function reportIssuesHTML()"));
  const context = {esc:value=>String(value ?? "").replaceAll("<","&lt;").replaceAll(">","&gt;"), row:{
    reason_origin:"previous_attempt", previous_attempt:{ts:1234567890}, item:{type:"episode",show_ids:{tvdb:"42"}},
    match_details:{candidate_count:11,truncated:true,candidates:[{local_id:"<id>",title:"<img src=x>",ids:{tmdb:"<bad>"},season:1,episode:2}]}
  }};
  vm.createContext(context);
  vm.runInContext(fn + "globalThis.html = matchDetailsHTML(row) + previousAttemptHTML(row);", context);
  assert.match(context.html, /Possible matches \(11\) from previous attempt/);
  assert.match(context.html, /show tvdb:42/);
  assert.match(context.html, /&lt;img src=x&gt;/);
  assert.match(context.html, /&lt;bad&gt;/);
  assert.match(context.html, /Only the first candidates/);
  assert.doesNotMatch(context.html, /<img|<bad>|<id>/);
  context.row = {};
  vm.runInContext("globalThis.html = matchDetailsHTML(row) + previousAttemptHTML(row);", context);
  assert.equal(context.html, "");
});

test("report issues preserve and escape the additional provider hint", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  function matchDetailsHTML(row)"), source.indexOf("  function reportHTML()"));
  const row = {item:{title:"Movie"},reason:"write_failed",hint:"<provider_hint>",explanation:"Could not mark watched"};
  const context = {session:{report:{issue_count:1,features:[]}},pageData:{total:1,items:[row]},PAGE_SIZE:75,page:0,
    pageLoading:false,query:"",feature:"",result:"",labels:{},endpoint:()=>"EMBY",title:item=>item.title,
    number:value=>Number(value || 0),matchDetailsHTML:()=>"",
    esc:value=>String(value ?? "").replaceAll("<","&lt;").replaceAll(">","&gt;")};
  vm.createContext(context);
  vm.runInContext(fn + "globalThis.html = reportIssuesHTML();", context);
  assert.match(context.html, /write_failed/);
  assert.match(context.html, /&lt;provider_hint&gt;/);
  assert.doesNotMatch(context.html, /<provider_hint>/);
  delete row.hint;
  vm.runInContext("globalThis.html = reportIssuesHTML();", context);
  assert.doesNotMatch(context.html, /provider_hint|undefined/);
});

test("long reasons stay collapsed behind a short label with full details available", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  function matchDetailsHTML(row)"), source.indexOf("  function reportIssuesHTML()"));
  const message = "Multiple possible destination matches were found. " + "Check their IDs before retrying. ".repeat(10);
  const context = {row:{reason:"ambiguous",reason_label:"Multiple matching items found",explanation:message,
    match_details:{candidate_count:1,candidates:[{title:"<Movie>",local_id:"42"}]}},
    esc:value=>String(value ?? "").replaceAll("<","&lt;").replaceAll(">","&gt;")};
  vm.createContext(context);
  vm.runInContext(fn + "globalThis.html = reasonDetailsHTML(row);", context);
  assert.equal(context.html.split("<details>")[0], '<small class="is-reason">Multiple matching items found</small>');
  assert.match(context.html, /<details><summary>Details<\/summary>/);
  assert.ok(context.html.includes(message));
  assert.match(context.html, /&lt;Movie&gt;/);
  assert.match(context.html, /<small>ambiguous<\/small>/);
  assert.equal((context.html.match(/<details/g) || []).length, 1);
  assert.doesNotMatch(context.html, /<details[^>]*open/);
  context.row = {reason_origin:"previous_attempt",reason_label:"Could not mark as watched",
    reason:"Previous attempt: Could not mark watched. Select and apply to retry.",previous_attempt:{reason:"write_failed",hint:"mark_played_failed",ts:1234567890}};
  vm.runInContext("globalThis.html = reasonDetailsHTML(row);", context);
  assert.match(context.html, /<small class="is-reason">Could not mark as watched<\/small><details>/);
  assert.match(context.html, /Select and apply to retry/);
  assert.match(context.html, /mark_played_failed/);
  context.row = {reason:"x".repeat(2000)};
  vm.runInContext("globalThis.html = reasonDetailsHTML(row);", context);
  assert.match(context.html, /^<small class="is-reason">Needs attention<\/small><details>/);
  assert.ok(context.html.includes(context.row.reason));
  context.row = {};
  vm.runInContext("globalThis.html = reasonDetailsHTML(row);", context);
  assert.equal(context.html, "");
});

test("failed plans identify partial progress proposals and expose failure details", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  const noticeSubject"), source.indexOf("  function aiQuestion()"))
    + source.slice(source.indexOf("  function statusMappingHTML("), source.indexOf("  const duration ="))
    + source.slice(source.indexOf("  function render()"), source.indexOf("  async function action("));
  const rows = [1,2].map(id => ({id:String(id), key:`movie:${id}`, item:{type:"movie",title:`Movie ${id}`},
    feature:"progress", operation:"add", result:"add", provider:"FLOPPY", instance:"default", selectable:true}));
  const context = {host:{contains:()=>false,innerHTML:""},document:{activeElement:null},window:{},
    session:{status:"error",pair:{source:"WETRAKR",target:"FLOPPY"},counts:{changes:2,selected:0,attention:0,conflicts:0},
      features:["progress"],notices:[{event:"feature:error",feature:"history",provider:"WETRAKR",side:"source",route:"WETRAKR to FLOPPY",reason:"feature:error",
        detail:"Provider <diagnostic>",error:"History <failure>"}]},
    pageData:{total:2,items:rows},page:0,PAGE_SIZE:100,pageLoading:false,downloadOpen:false,downloading:false,noticesExpanded:null,
    query:"",feature:"",result:"",labels:{add:"Add"},pending:()=>false,origin:()=>["main","Main"],aiButton:()=>"",
    esc:value=>String(value ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;"),
    endpoint:(p,i)=>p,progressHTML:()=>"",number:value=>Number(value || 0),title:item=>item.title,
    value:()=>"25%",reasonDetailsHTML:()=>"",reportHTML:()=>"Completed report"};
  vm.createContext(context);
  vm.runInContext(fn + "render();", context);
  const html = context.host.innerHTML;
  assert.match(html, /Partial plan: review incomplete/);
  assert.match(html, /<strong>2<\/strong><span>Partial proposals/);
  assert.match(html, /Unknown<\/strong><span>Item checks incomplete/);
  assert.doesNotMatch(html, /Need attention/);
  assert.match(html, /<section id="is-notices" class="is-notices" aria-label="Failure notices and sync protections"><strong>Failure notices and sync protections \(1\)<\/strong><p>/);
  assert.match(html, /history \/ WETRAKR \(source\)/);
  assert.doesNotMatch(html, /history \/ FLOPPY/);
  assert.match(html, /Planning failed for history from WETRAKR \(source\)\. Those features have no proposals in this plan/);
  assert.match(html, /History &lt;failure&gt;/);
  assert.match(html, /Provider &lt;diagnostic&gt;/);
  assert.doesNotMatch(html, /<failure>|<diagnostic>/);
  assert.match(html, /Apply and Download review are disabled until a complete plan/);
  assert.match(html, /Events or logs/);
  assert.match(html, /reread providers and retry/);
  assert.match(html, /data-action="apply"[^>]*disabled/);
  assert.match(html, /data-action="toggle-download"[^>]*disabled/);
  assert.doesNotMatch(html, /data-action="refresh"[^>]*disabled/);
  assert.equal((html.match(/<td>progress<\/td>/g) || []).length, 2);
  context.session.status = "review";
  context.session.failed_features = ["history"];
  context.session.completed_features = ["progress"];
  context.session.counts.selected = 2;
  vm.runInContext("render();", context);
  assert.match(context.host.innerHTML, /You can apply changes from completed features/);
  assert.match(context.host.innerHTML, /Failed features \(history\) are excluded/);
  assert.doesNotMatch(context.host.innerHTML, /data-action="apply"[^>]*disabled/);
  assert.doesNotMatch(context.host.innerHTML, /data-action="toggle-download"[^>]*disabled/);
  assert.match(context.host.innerHTML, /History &lt;failure&gt;/);
  context.session.status = "error";
  context.session.failed_features = [];
  context.session.counts.selected = 0;
  context.session.notices = [];
  context.session.counts.changes = 0;
  context.pageData = {total:0,items:[]};
  vm.runInContext("render();", context);
  assert.match(context.host.innerHTML, /No detailed failure notice was captured/);
  assert.match(context.host.innerHTML, /The proposals below were collected before planning stopped/);
  assert.match(context.host.innerHTML, /No proposals available from this partial plan/);
  assert.doesNotMatch(context.host.innerHTML, /done_all|No changes proposed/);
  context.session.status = "review";
  context.session.notices = [{reason:"Existing notice"}];
  vm.runInContext("render();", context);
  assert.doesNotMatch(context.host.innerHTML, /Partial plan:|Item checks incomplete|toggle-notices/);
  assert.match(context.host.innerHTML, /Sync protections and provider notices \(1\)<\/strong><p><strong><\/strong> Existing notice<\/p>/);
  assert.match(context.host.innerHTML, /<strong>0<\/strong><span>Need attention/);
  context.session.status = "error";
  context.session.report = {outcome:"failed"};
  vm.runInContext("render();", context);
  assert.match(context.host.innerHTML, /Completed report/);
  assert.doesNotMatch(context.host.innerHTML, /Partial plan:/);
});

test("notices stay visible and the status mapping count links to them", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  const noticeSubject"), source.indexOf("  function aiQuestion()"))
    + source.slice(source.indexOf("  function statusMappingHTML("), source.indexOf("  const duration ="));
  const notices = Array.from({length: 8}, (_, i) => ({feature:"history", provider:"SCROB", reason:`Notice ${i + 1}`}));
  const context = {session:{notices}, noticesExpanded:null, number:value=>Number(value || 0).toLocaleString("en-US"),
    esc:value=>String(value ?? ""), endpoint:p=>p};
  vm.createContext(context);
  vm.runInContext(fn + "globalThis.collapsed = noticesHTML(false, false); globalThis.summary = statusMappingHTML([{src:'WETRAKR',dst:'SCROB',matched:4,unresolved:2,state:'attention'}]);", context);
  assert.equal((context.collapsed.match(/<p>/g) || []).length, 3);
  assert.match(context.collapsed, /data-action="toggle-notices" aria-expanded="false">Show all 8 notices<\/button>/);
  assert.match(context.summary, /<button class="is-link" data-action="show-notices">2 review notices<\/button>/);
  vm.runInContext("noticesExpanded = true; globalThis.expanded = noticesHTML(false, false); session.notices = session.notices.slice(0, 5); noticesExpanded = null; globalThis.few = noticesHTML(false, false);", context);
  assert.equal((context.expanded.match(/<p>/g) || []).length, 8);
  assert.match(context.expanded, /aria-expanded="true">Show fewer<\/button>/);
  assert.equal((context.few.match(/<p>/g) || []).length, 5);
  vm.runInContext("session.notices = []; globalThis.none = noticesHTML(false, false); globalThis.plain = statusMappingHTML([{src:'A',dst:'B',matched:1,unresolved:2}]);", context);
  assert.equal(context.none, "");
  assert.match(context.plain, /2 review notices/);
  assert.doesNotMatch(context.plain, /show-notices/);
});

test("completion report distinguishes cloud outcomes and escapes titles", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  function statusMappingHTML("), source.indexOf("  const duration ="))
    + source.slice(source.indexOf("  function reportHTML()"), source.indexOf("  function render()"));
  const report = {outcome:"success", totals:{added:1,updated:0,removed:0}, features:[], cloud_results:
    ["dated","present","cached","plex_discover_date_pending"].map(outcome => ({title:"<Example>",key:"movie:1",provider:"PLEX",instance:"own",outcome}))};
  const context = {session:{report}, esc:value=>String(value ?? "").replaceAll("<","&lt;").replaceAll(">","&gt;"),
    number:value=>Number(value || 0), duration:()=>"", requestBreakdown:()=>"", endpoint:(p,i)=>`${p} (${i})`,
    reportIssuesHTML:()=>"", origin:()=>["main","Main"]};
  vm.createContext(context);
  vm.runInContext(fn + "globalThis.html = reportHTML();", context);
  assert.match(context.html, /watched and original date restored/);
  assert.match(context.html, /already watched, unchanged/);
  assert.match(context.html, /previously confirmed/);
  assert.match(context.html, /date repair pending/);
  assert.match(context.html, /PLEX \(own\)/);
  assert.match(context.html, /&lt;Example&gt;/);
  assert.doesNotMatch(context.html, /<Example>/);
  report.cloud_results = Array.from({length:51}, (_, n) => ({title:`Cloud title ${n}`, key:`movie:${n}`, outcome:"dated"}));
  vm.runInContext("globalThis.html = reportHTML();", context);
  assert.match(context.html, /Showing the first 50 of 51/);
  assert.doesNotMatch(context.html, /Cloud title 50/);
  assert.equal(report.cloud_results.length, 51);
  report.cloud_results = [];
  vm.runInContext("globalThis.html = reportHTML();", context);
  assert.doesNotMatch(context.html, /<h2>Plex Discover fallback/);
  report.status_mappings = [{src:"TRAKT",src_instance:"default",dst:"WETRAKR",dst_instance:"<other>",state:"complete",matched:1,updated:1,proposed:1}];
  vm.runInContext("globalThis.html = reportHTML();", context);
  assert.match(context.html, /Status mapping/);
  assert.match(context.html, /WETRAKR \(&lt;other&gt;\): Checked/);
  assert.match(context.html, /1 updated/);
  report.status_mappings = [{state:"skipped",reason:"History is disabled. <not markup>"}];
  vm.runInContext("globalThis.html = reportHTML();", context);
  assert.match(context.html, /Not run/);
  assert.match(context.html, /History is disabled. &lt;not markup&gt;/);
  assert.doesNotMatch(context.html, /0 matched/);
});


test("Tracker import reviews use the shared selection controls without permanent mapping edits", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  function render()"), source.indexOf("  async function action("));
  const context = {host:{contains:()=>false,innerHTML:""},document:{activeElement:null},window:{},
    session:{tracker_import:true,status:"review",pair:{source:"TRAKT",target:"CROSSWATCH"},counts:{changes:1,selected:1},features:["history"]},
    pageData:{total:1,items:[{id:"one",key:"tmdb:949",item:{type:"movie",title:"Heat"},feature:"history",operation:"add",result:"add",provider:"CROSSWATCH",selectable:true,selected:true}]},
    page:0,PAGE_SIZE:75,pageLoading:false,downloadOpen:false,query:"",feature:"",result:"",labels:{add:"Add"},pending:()=>false,
    origin:()=>["tracker","Tracker"],aiButton:()=>"",esc:value=>String(value??""),endpoint:value=>value,progressHTML:()=>"",statusMappingHTML:()=>"",noticesHTML:()=>"",
    number:value=>Number(value||0),title:item=>item.title,value:()=>"",reasonDetailsHTML:()=>""};
  vm.createContext(context);
  vm.runInContext(fn+"render();",context);
  assert.match(context.host.innerHTML,/Review your import/);
  assert.match(context.host.innerHTML,/href="#tracker"/);
  assert.match(context.host.innerHTML,/data-select="one"/);
  assert.match(context.host.innerHTML,/Apply selected \(1\)/);
  assert.match(context.host.innerHTML,/Nothing is written back to the source/);
  assert.doesNotMatch(context.host.innerHTML,/data-map=|data-action="map-selected"|Mappings are saved permanently/);
  context.session.tracker_import=false;
  vm.runInContext("render();",context);
  assert.match(context.host.innerHTML,/data-map="one"/);
  assert.match(context.host.innerHTML,/data-action="map-selected"/);
});

test("Review footer explains the one-time dry run confirmation", () => {
  const source = readFileSync(new URL("../assets/js/interactive-sync.js", import.meta.url), "utf8");
  const fn = source.slice(source.indexOf("  function render()"), source.indexOf("  async function action("));
  const context = {host:{contains:()=>false,innerHTML:""},document:{activeElement:null},window:{},
    session:{dry_run:true,status:"review",pair:{source:"WETRAKR",target:"FLOPPY"},counts:{changes:1,selected:1},features:["history"]},
    pageData:{total:1,items:[{id:"one",key:"tmdb:949",item:{type:"movie",title:"Heat"},feature:"history",operation:"add",result:"add",provider:"FLOPPY",selectable:true,selected:true}]},
    page:0,PAGE_SIZE:75,pageLoading:false,downloadOpen:false,query:"",feature:"",result:"",labels:{add:"Add"},pending:()=>false,
    origin:()=>["sync","Synchronization"],aiButton:()=>"",esc:value=>String(value??""),endpoint:value=>value,progressHTML:()=>"",statusMappingHTML:()=>"",noticesHTML:()=>"",
    number:value=>Number(value||0),title:item=>item.title,value:()=>"",reasonDetailsHTML:()=>""};
  vm.createContext(context);
  vm.runInContext(fn+"render();",context);
  assert.match(context.host.innerHTML,/Dry run is on/);
  assert.match(context.host.innerHTML,/ask for confirmation/);
  assert.match(context.host.innerHTML,/Global dry run stays on/);
  assert.doesNotMatch(context.host.innerHTML,/data-action="apply" disabled/);
  context.session.counts.selected=0;
  vm.runInContext("render();",context);
  assert.match(context.host.innerHTML,/data-action="apply" disabled/);
  context.session.counts.selected=1;
  context.session.dry_run=false;
  vm.runInContext("render();",context);
  assert.match(context.host.innerHTML,/1 changes selected/);
  assert.doesNotMatch(context.host.innerHTML,/data-action="apply" disabled/);
});

test("Apply requires confirmation for dry run and never reuses it", async () => {
  const source=readFileSync(new URL("../assets/js/interactive-sync.js",import.meta.url),"utf8");
  const fn=source.slice(source.indexOf("  async function action("),source.indexOf("  async function mapping("));
  const requests=[],prompts=[];
  let confirmed=false;
  const context={session:{id:"review-1",dry_run:true,revision:3,selection_version:4,counts:{selected:2}},
    generation:1,pageLoading:false,busy:false,pageData:{items:[]},API:"/api/interactive-sync",
    pending:()=>false,invalidatePage:()=>{},render:()=>{},accept:()=>{},showError:assert.fail,poll:assert.fail,
    window:{confirm:message=>{prompts.push(message);return confirmed;}},
    post:async (url,body)=>{requests.push({url,body});return {};}};
  vm.createContext(context);
  vm.runInContext(fn,context);
  await vm.runInContext('action("apply")',context);
  assert.equal(requests.length,0);
  assert.match(prompts[0],/Apply 2 selected changes/);
  assert.match(prompts[0],/Global dry run will stay enabled/);
  confirmed=true;
  await vm.runInContext('action("apply")',context);
  assert.equal(requests.length,1);
  assert.equal(requests[0].url,"/api/interactive-sync/review-1/apply");
  assert.equal(requests[0].body.confirm_dry_run,true);
  assert.equal(requests[0].body.revision,3);
  assert.equal(requests[0].body.selection_version,4);
  assert.equal(context.session.dry_run,true);
  confirmed=false;
  await vm.runInContext('action("apply")',context);
  assert.equal(requests.length,1);
  context.session.dry_run=false;
  await vm.runInContext('action("apply")',context);
  assert.equal(requests.length,2);
  assert.equal(requests[1].body.confirm_dry_run,undefined);
  assert.equal(prompts.length,3);
});

test("change summary reports a filled year", () => {
  const original = {type:"movie", title:"Ant-Man", ids:{tmdb:"102899"}};
  assert.equal(changeSummary(original, {...original, year:2015}), "year");
  assert.equal(changeSummary({...original, year:2015}, {...original, year:2015}), "");
  assert.equal(changeSummary({type:"episode", series_title:"Show", season:1, episode:1, show_ids:{tmdb:"1"}}, {type:"episode", series_title:"Show", series_year:2020, season:1, episode:1, show_ids:{tmdb:"1"}}), "year");
  assert.equal(changeSummary({type:"episode", series_title:"Show", year:2020, season:1, episode:1, show_ids:{tmdb:"1"}}, {type:"episode", series_title:"Show", year:2020, series_year:2020, season:1, episode:1, show_ids:{tmdb:"1"}}), "year");
  assert.equal(changeSummary(original, {...original, year:"2015"}), "year");
  assert.equal(changeSummary({...original, year:2015}, {...original, year:"2015"}), "");
});
