/* tests/tracker-health.test.mjs */
/* CrossWatch - Tracker episode health explanations */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/tracker.js", import.meta.url), "utf8");
const context = vm.createContext({se: (season, episode) => `S${season}E${episode}`});
vm.runInContext(source.slice(source.indexOf("  function checkInfo("), source.indexOf("  function healthIcon(")), context);

test("unverified anime episodes explain the missing check and need attention", () => {
  const info = context.checkInfo({check: {code: "episode_unverified", kind: "show", season: 3, episode: 13}});
  assert.equal(info.state, "attention");
  assert.equal(info.title, "Episode not verified");
  assert.match(info.text, /anime mapping/);
  assert.match(info.text, /Check all/);
});

test("verified and missing episodes retain distinct health results", () => {
  assert.equal(context.checkInfo({check: {code: "ok"}}).state, "ok");
  const info = context.checkInfo({check: {code: "episode_missing", season: 1, episode: 12}});
  assert.equal(info.state, "attention");
  assert.match(info.text, /S1E12/);
});

for (const [code, scope] of [["episode_unverified", "record"], ["episode_missing", "record"], ["mismatch", "title"]]) {
  test(`${code}: Fix match opens the appropriate mapping scope`, async () => {
    const record = {type: "episode", key: "tvdb:10#s03e13", season: 3, episode: 13, check: {code}};
    let opened;
    const mapping = source.slice(source.indexOf("  async function openMapping("), source.indexOf("  function findRecord("))
      .replace('import(`/assets/js/interactive-sync-mapping.js?v=${version}`)', "loadMappingWorkspace()");
    const app = vm.createContext({
      state: {instance: "default", title: {show: true}}, window: {},
      titleAnchor: () => record, recordCount: () => 5, num: String,
      document: {querySelector: () => true}, closeSheet: () => {},
      postRaw: async () => ({row: {item: record}}),
      loadMappingWorkspace: async () => ({openMappingWorkspace: options => {opened = options;}}),
      toast: {show: message => assert.fail(message)},
    });
    vm.runInContext(mapping, app);
    await app.openMapping();
    assert.equal(opened.scope, scope);
    assert.equal(opened.rows[0].item.episode, 13);
  });
}

test("cleanup attention filter requests and retains unverified episodes", async () => {
  const record = {key: "tvdb:10#s03e13", check: {code: "episode_unverified"}};
  const state = {view: "cleanup", healthFilter: "attention", instance: "default", request: 0, offset: 0, pageSize: 50};
  let requested;
  const app = vm.createContext({
    state, URLSearchParams, listFeature: () => "history", timelineOn: () => false,
    renderSlots: () => {}, renderMonths: () => {}, errorText: String, stopOverview: () => {},
    getJSON: async url => {requested = new URL(url, "https://tracker.test"); return {items: [record], total: 1};},
  });
  vm.runInContext(source.slice(source.indexOf("  async function loadMain("), source.indexOf("  let progressQueue")), app);
  await app.loadMain();
  assert.equal(requested.searchParams.get("health"), "attention");
  assert.equal(requested.searchParams.get("feature"), "history");
  assert.equal(state.total, 1);
  assert.equal(context.checkInfo(state.items[0]).state, "attention");
});
