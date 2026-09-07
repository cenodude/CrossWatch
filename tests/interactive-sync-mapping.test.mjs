/* tests/interactive-sync-mapping.test.mjs */
/* CrossWatch - Batch episode correction tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
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
