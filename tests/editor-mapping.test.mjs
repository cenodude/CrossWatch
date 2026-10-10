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

const editorSource = readFileSync(new URL("../assets/js/editor.js", import.meta.url), "utf8");
const providerNavigation = editorSource.slice(editorSource.indexOf("  async function openMappingProvider("), editorSource.indexOf("  function openItemReplacer("));

function navigationFixture({unavailable = false, failed = false, dirty = false} = {}) {
  const original = {_rid:1, key:"tmdb:1", title:"Example", type:"movie"};
  const state = {source:"state", snapshot:"all", instance:"household", kind:"history", rows:[original],
    mappingPair:"old-pair", filter:"old filter", typeFilter:{movie:false, episode:true}, hasChanges:dirty};
  const calls = [], messages = [], filterInput = {value:state.filter};
  const sandbox = vm.createContext({state, filterInput, instanceSel:{},
    syncSourceUI() {}, syncTypeFilterUI() {}, persistUIState() {}, renderRows() {}, rebuildSnapshots() {}, syncProfileIconSelect() {},
    setStatusSticky:message => messages.push(message),
    async loadSnapshots() { calls.push("providers"); if (unavailable) state.instance = "other"; },
    async loadState() { calls.push("rows"); state.rows = [{key:"imdb:tt123", title:"Example"}]; state.loadError = failed; },
  });
  vm.runInContext(providerNavigation, sandbox);
  return {sandbox, state, calls, messages, filterInput, original};
}

test("merged mapping entry opens the chosen provider account and preserves the feature", async () => {
  const {sandbox, state, calls, filterInput} = navigationFixture();
  await sandbox.openMappingProvider({title:"Example"}, {provider:"SIMKL", instance:"SIMKL-P01"});
  assert.equal(state.snapshot, "SIMKL");
  assert.equal(state.instance, "SIMKL-P01");
  assert.equal(state.kind, "history");
  assert.equal(state.mappingPair, "");
  assert.equal(filterInput.value, "Example");
  assert.equal(state.typeFilter.movie, true);
  assert.deepEqual(calls, ["providers", "rows"]);
});

test("merged mapping navigation restores the view if the source is unavailable or fails", async () => {
  for (const options of [{unavailable:true}, {failed:true}]) {
    const {sandbox, state, original, filterInput, messages} = navigationFixture(options);
    await sandbox.openMappingProvider({title:"Example"}, {provider:"SIMKL", instance:"SIMKL-P01"});
    assert.equal(state.snapshot, "all");
    assert.equal(state.instance, "household");
    assert.equal(state.rows[0], original);
    assert.equal(filterInput.value, "old filter");
    assert.equal(state.typeFilter.movie, false);
    assert.match(messages.at(-1), /no longer available|Could not load/);
  }
});

test("merged mapping navigation leaves unsaved changes untouched", async () => {
  const {sandbox, state, calls} = navigationFixture({dirty:true});
  await sandbox.openMappingProvider({title:"Example"}, {provider:"SIMKL", instance:"SIMKL-P01"});
  assert.equal(state.snapshot, "all");
  assert.equal(state.hasChanges, true);
  assert.deepEqual(calls, []);
});

test("merged rows keep a working mapping action beside their title", () => {
  const elements = [];
  const element = () => {
    const node = {children:[], style:{}, dataset:{}, classList:{add() {}},
      appendChild(child) { this.children.push(child); }, append(...children) { this.children.push(...children); },
      setAttribute(name, value) { this[name] = value; }};
    elements.push(node);
    return node;
  };
  const context = vm.createContext({window:{}, document:{createElement:element}});
  vm.runInContext(readFileSync(new URL("../assets/js/editor/table.js", import.meta.url), "utf8"), context);
  const row = {_rid:1, type:"movie", title:"Example", raw:{}};
  const opened = [];
  const tr = context.window.CW.Editor.Table.createRowElement(row, {state:{kind:"history"}, merged:true,
    isPolicySource:() => true, isRowLocked:() => true, canReplaceRow:() => true,
    openItemReplacer:(value, anchor) => opened.push({value, anchor})});
  const button = elements.find(el => el.title === "Edit mapping");
  const attached = node => node === button || node.children.some(attached);
  assert.equal(attached(tr), true);
  assert.equal(button.disabled, false);
  button.onclick();
  assert.equal(opened[0].value, row);
  assert.equal(opened[0].anchor, button);
});
