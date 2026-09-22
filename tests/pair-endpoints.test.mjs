/* tests/pair-endpoints.test.mjs */
/* CrossWatch - Pair endpoint selection, libraries and save regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";
import { parse } from "acorn";

const index = readFileSync(new URL("../assets/js/modals/pair-config/index.js", import.meta.url), "utf8");
const librarySource = readFileSync(new URL("../assets/js/modals/pair-config/libraries.js", import.meta.url), "utf8");
const functions = parse(index, {ecmaVersion: "latest", sourceType: "module"}).body.filter(node => node.type === "FunctionDeclaration");
const sourceFor = name => {
  const node = functions.find(node => node.id.name === name);
  return index.slice(node.start, node.end);
};

for (const scenario of [
  {name: "defaults off", dst: "SIMKL", expected: false},
  {name: "explicitly enabled", dst: "SIMKL", enabled: true, expected: true},
  {name: "global mapping disabled", dst: "SIMKL", enabled: true, global: false, expected: false},
  {name: "SIMKL is only the source", src: "SIMKL", enabled: true, expected: false},
  {name: "SIMKL receives in two-way mode", src: "SIMKL", twoWay: true, enabled: true, expected: true},
  {name: "no SIMKL endpoint", enabled: true, expected: false},
]) {
  test(`progress anime mapping: ${scenario.name}`, () => {
    const context = vm.createContext({
      ID: id => id === "cx-mode-two" ? {checked: !!scenario.twoWay} : null,
      isSimkl: value => value === "SIMKL", isAniList: () => false, isMDBList: () => false,
      hasOwn: (value, key) => Object.hasOwn(value, key), defaultFor: () => ({}),
      applyProgressMaxRecommendation: () => ({maxPercent: 90}), ratingsDisabledFor: () => new Set(),
      pairSupportsHistoryRewatches: () => false, collectionTypesForPair: () => ["movies"],
      sanitizeFeaturesForPair: (_, features) => features, getPairProviderStrictValue: () => false,
    });
    for (const name of ["isTwoWayMode", "globalAnimeMappingEnabled", "simklCanReceiveProgress", "normalizeAnimeProgressOptions", "buildPayload"])
      vm.runInContext(sourceFor(name), context);
    const state = {
      src: scenario.src || "PLEX", dst: scenario.dst || "PLEX",
      cfgRaw: {anime_mapping: {enabled: scenario.global !== false}},
      options: {history: {use_anime_mapping: true}, progress: {use_anime_mapping: scenario.enabled}},
    };
    const saved = context.buildPayload(state, {dataset: {}});
    assert.equal(saved.features.progress.use_anime_mapping, scenario.expected);
    assert.equal(saved.features.progress.anime_only_sync, false);
    assert.equal(context.normalizeAnimeProgressOptions(state).use_anime_mapping, scenario.expected);
    assert.equal(state.options.history.use_anime_mapping, true);
  });
}

for (const response of [
  {ok: true, data: {ok: false, error: "not_found"}},
  {ok: false, data: {ok: false}},
  {ok: false, invalidJson: true},
  {ok: true, invalidJson: true, redirected: true},
  {networkError: true},
]) {
  test(`failed PUT never creates another pair: ${JSON.stringify(response)}`, async () => {
    const requests = [];
    const context = vm.createContext({fetch: async (url, options) => {
      requests.push({url, options});
      if (response.networkError) throw new Error("Connection lost");
      return {...response, json: async () => {
        if (response.invalidJson) throw new SyntaxError("HTML response");
        return response.data;
      }};
    }});
    vm.runInContext(sourceFor("savePair"), context);
    const result = await context.savePair({id: "existing", source: "PLEX", target: "PLEX"});
    assert.equal(result.ok, false);
    assert.equal(requests.length, 1);
    assert.equal(requests[0].options.method, "PUT");
    assert.equal(requests[0].url, "/api/pairs/existing");
  });
}

test("new pairs POST once and existing pairs PUT once", async () => {
  const requests = [];
  const context = vm.createContext({fetch: async (url, options) => {
    requests.push(options.method);
    return {ok: true, json: async () => ({ok: true})};
  }});
  vm.runInContext(sourceFor("savePair"), context);
  assert.equal((await context.savePair({})).ok, true);
  assert.equal((await context.savePair({id: "existing"})).ok, true);
  assert.deepEqual(requests, ["POST", "PUT"]);
});

function element() {
  let html = "";
  return {
    children: [], dataset: {}, style: {}, value: "", listeners: {}, textContent: "", disabled: false,
    get innerHTML() { return html; },
    set innerHTML(value) { html = value; this.children = []; if (value === '<option value="">No other configured instance</option>') this.value = ""; },
    appendChild(child) { this.children.push(child); },
    addEventListener(event, listener) { this.listeners[event] = listener; },
    classList: {toggle() {}},
  };
}

for (const instance of ["default", "P01"]) {
  test(`a sole ${instance} instance leaves the destination explicitly empty`, () => {
    const src = element(), dst = element();
    const context = vm.createContext({
      ID: id => id === "cx-src-inst" ? src : dst,
      same: (a, b) => a === b, escHTML: value => value, G: {}, renderPairUserProfileControl() {},
      defaultFor: () => ({}), isAniList: () => false, isSimkl: () => false, isMDBList: () => false,
      applyProgressMaxRecommendation: () => ({maxPercent: 90}), ratingsDisabledFor: () => new Set(),
      pairSupportsHistoryRewatches: () => false, collectionTypesForPair: () => ["movies"],
      sanitizeFeaturesForPair: (_, features) => features, getPairProviderStrictValue: () => false,
    });
    vm.runInContext(sourceFor("renderInstanceSelects") + "\n" + sourceFor("buildPayload"), context);
    const state = {src: "PLEX", dst: "PLEX", src_instance: instance, instanceMap: {PLEX: [{id: instance, configured: true}]}};
    context.renderInstanceSelects(state);
    assert.equal(state.src_instance, instance);
    assert.equal(state.dst_instance, "");
    assert.equal(dst.disabled, true);
    const payload = context.buildPayload(state, {dataset: {}});
    assert.equal(payload.target_instance, "");
  });
}

test("library lists load and save independently for both instances", async () => {
  const nodes = new Map(), requests = [];
  let mounted = false;
  const ID = id => {
    if (!mounted) return null;
    if (!nodes.has(id)) nodes.set(id, element());
    return nodes.get(id);
  };
  const context = vm.createContext({
    document: {createElement: element},
    fetch: async url => {
      requests.push(url);
      if (url === "/api/config") return {ok: true, json: async () => ({})};
      const instance = new URL(url, "http://cw.test").searchParams.get("instance");
      return {ok: true, json: async () => ({libraries: [{key: instance, title: `Library ${instance}`} ]})};
    },
  });
  vm.runInContext(librarySource.replace("export function", "function"), context);
  const controller = context.createLibraryController({
    ID, hasPlex: () => true, hasJelly: () => false, hasEmby: () => false, hasKodi: () => false,
    getOpts: (state, feature) => state.options[feature] ||= {},
  });
  const state = {src: "PLEX", dst: "PLEX", src_instance: "P01", dst_instance: "default", options: {}, visited: new Set()};
  controller.initPairLibraryUI(state);
  controller.initPairLibraryUI(state);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(requests, []);
  mounted = true;
  controller.initPairLibraryUI(state);
  await new Promise(resolve => setImmediate(resolve));
  const groups = ID("plx-hist-libs").children;
  assert.equal(groups.length, 2);
  assert.equal(groups[0].children[0].textContent, "Source: P01");
  assert.equal(groups[1].children[0].textContent, "Target: Default");
  groups[0].children[1].children[0].listeners.click();
  groups[1].children[1].children[0].listeners.click();
  assert.deepEqual(JSON.parse(JSON.stringify(state.options.history.libraries)), {"PLEX#P01": ["P01"], "PLEX#default": ["default"]});
  groups[0].children[1].children[0].listeners.click();
  assert.deepEqual(JSON.parse(JSON.stringify(state.options.history.libraries)), {"PLEX#P01": [], "PLEX#default": ["default"]});
  assert.ok(requests.some(url => url.includes("instance=default")));
  assert.ok(requests.some(url => url.includes("instance=P01")));
  nodes.clear();
  controller.initPairLibraryUI(state);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(ID("plx-hist-libs").children.length, 2);
});
