/* tests/editor-mapping.test.mjs */
/* CrossWatch - Editor mapping staging and shared workspace routing */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

function modules() {
  const context = vm.createContext({window:{}, structuredClone});
  for (const name of ["rows", "persistence", "metadata-replacer"]) {
    vm.runInContext(readFileSync(new URL(`../assets/js/editor/${name}.js`, import.meta.url), "utf8"), context);
  }
  return context.window.CW.Editor;
}

test("both Editor mapping entry points use the shared workspace", () => {
  const {MetadataReplacer} = modules();
  for (const type of ["movie", "show", "season", "episode"]) {
    const row = {type}, called = [];
    const ctx = {isPolicySource:() => true, openMapping:value => called.push(value)};
    MetadataReplacer.openItemReplacer(row, null, ctx);
    MetadataReplacer.openTitleSearchEditor(row, null, {}, ctx);
    assert.deepEqual(called, [row, row]);
  }
});

test("staged corrections preserve media type, identity and value fields", () => {
  const {Rows, Persistence} = modules();
  for (const type of ["movie", "show", "season", "episode"]) {
    const row = {key:"tmdb:1", _rid:1};
    const item = {type, title:"Correct title", year:2025, ids:{tmdb:"2"}, rating:8,
      watched_at:"2026-09-08T12:00:00Z", ...(type === "episode" ? {season:1, episode:2} : {})};
    Rows.applyManualRow(row, item, "tmdb:2");
    assert.equal(row.type, type);
    assert.equal(row.episode, type === "episode");
    assert.equal(row.title, "Correct title");
    const ctx = {state:{rows:[row], kind:"history", source:"state", snapshot:"PLEX", instance:"second", mappingPair:"pair-one"},
      isPolicySource:() => true, isProviderPickerSource:() => true};
    const {payload} = Persistence.buildSaveData(ctx);
    assert.equal(payload.pair_id, "pair-one");
    assert.equal(payload.mapping_originals["tmdb:2"], "tmdb:1");
    assert.equal(payload.items["tmdb:2"].rating, 8);
    assert.equal(payload.items["tmdb:2"].watched_at, item.watched_at);
    ctx.state.mappingPair = "";
    assert.equal(Persistence.buildSaveData(ctx).payload.pair_id, "");
  }
});

test("saving current-state mappings reloads rows and their mapping indicators", async () => {
  const {Persistence} = modules();
  const calls = [];
  const state = {source:"state", rows:[], kind:"watchlist", snapshot:"PLEX", mappingPair:"pair-one"};
  await Persistence.saveState({state, isPolicySource:() => true, isProviderPickerSource:() => true,
    fetchJSON:async () => { calls.push("save"); return {count:0}; },
    loadSnapshots:async () => calls.push("providers"),
    loadState:async () => calls.push("rows"),
  });
  assert.deepEqual(calls, ["save", "providers", "rows"]);
  assert.equal(state.saving, false);
});

test("saving mappings preserves block rules for items absent from current state", () => {
  const {Persistence} = modules();
  const ctx = {state:{source:"state", rows:[], preservedBlocks:["imdb:tt9999999"], mappingPair:"p1"},
    isPolicySource:() => true, isProviderPickerSource:() => true};
  assert.deepEqual([...Persistence.buildSaveData(ctx).payload.blocks], ["imdb:tt9999999"]);
});
