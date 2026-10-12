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

for (const peer of ["SIMKL", "FLOPPY", "KITSU"]) {
  for (const twoWay of [false, true]) {
    for (const reverse of [false, true]) {
      test(`AniList history remains enabled with ${peer}, reverse=${reverse}, twoWay=${twoWay}`, () => {
        const source = readFileSync(new URL("../assets/js/modals/pair-config/custom-rules.js", import.meta.url), "utf8")
          .replace(/export function/g, "function");
        const context = vm.createContext({pair: {src: reverse ? peer : "ANILIST", dst: reverse ? "ANILIST" : peer, twoWay}});
        vm.runInContext(source, context);
        assert.equal(vm.runInContext('featureAllowedForPair(pair, "history")', context), true);
        const features = vm.runInContext('sanitizeFeaturesForPair(pair, {history: {enable: true, add: true, remove: true, rewatches: true}})', context);
        assert.equal(features.history.enable, true);
        assert.equal(features.history.add, true);
        assert.equal(features.history.remove, true);
        assert.equal(features.history.rewatches, false);
        const common = vm.runInContext('commonFeaturesForPair(pair, () => ({history: true}), () => false)', context);
        assert.equal(common.includes("history"), true);
      });
    }
  }
}

for (const feature of ["watchlist", "ratings"]) {
  for (const [src, dst] of [["SIMKL", "KITSU"], ["KITSU", "SIMKL"], ["SIMKL", "ANILIST"], ["ANILIST", "SIMKL"]]) {
    test(`${feature} shows the anime-only restriction for ${src} to ${dst}`, () => {
      const nodes = new Map();
      const context = vm.createContext({
        same: (a, b) => String(a).toLowerCase() === String(b).toLowerCase(),
        ID: id => nodes.get(id), Q: selector => nodes.get(selector.slice(1)),
      });
      vm.runInContext([sourceFor("renderAnimeOnlyOption"), sourceFor("applySubDisable")].join(";"), context);
      const prefix = feature === "ratings" ? "cx-rt" : "cx-wl";
      for (const mapping of [false, true]) {
        const html = context.renderAnimeOnlyOption({src, dst}, feature);
        assert.match(html, /Anime-only sync/);
        assert.match(html, /data-anime-only="1" checked disabled/);
        const only = {checked: true, disabled: true, dataset: {animeOnly: "1"}};
        nodes.set(`${prefix}-anime-only`, only);
        nodes.set(`${prefix}-anime-map`, {checked: mapping, dataset: {}});
        for (const enabled of [false, true]) {
          nodes.set(`${prefix}-enable`, {checked: enabled});
          context.applySubDisable(feature);
          assert.equal(only.checked, true);
          assert.equal(only.disabled, true);
        }
      }
    });
  }
}

test("Pairs without an anime-only provider omit the restriction", () => {
  const context = vm.createContext({same: (a, b) => a === b});
  vm.runInContext(sourceFor("renderAnimeOnlyOption"), context);
  assert.equal(context.renderAnimeOnlyOption({src: "SIMKL", dst: "TRAKT"}, "watchlist"), "");
});

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
      same: (a, b) => String(a).toLowerCase() === String(b).toLowerCase(),
      isSimkl: value => value === "SIMKL", isAniList: () => false, isMDBList: () => false, isCrossWatch: () => false,
      hasOwn: (value, key) => Object.hasOwn(value, key), defaultFor: () => ({}),
      applyProgressMaxRecommendation: () => ({maxPercent: 90}), ratingsDisabledFor: () => new Set(),
      pairSupportsHistoryRewatches: () => false, collectionTypesForPair: () => ["movies"],
      sanitizeFeaturesForPair: (_, features) => features, getPairProviderStrictValue: () => false,
      libraryOnlyOn: () => false, tmdbMetadataReady: () => true,
    });
    for (const name of ["isTwoWayMode", "globalAnimeMappingEnabled", "hasAniList", "hasAnimeProvider", "simklCanReceiveProgress", "normalizeAnimeProgressOptions", "supportsPathMatching", "getPairMarkedWatchedValue", "buildPayload"])
      vm.runInContext(sourceFor(name), context);
    const state = {
      src: scenario.src || "PLEX", dst: scenario.dst || "PLEX", path_matching: true,
      cfgRaw: {anime_mapping: {enabled: scenario.global !== false}, plex: {history: {include_marked_watched: true}}},
      pairProviders: {plex: {history: {include_marked_watched: false}}},
      options: {history: {use_anime_mapping: true}, progress: {use_anime_mapping: scenario.enabled}},
    };
    const saved = context.buildPayload(state, {dataset: {}});
    assert.equal(saved.features.progress.use_anime_mapping, scenario.expected);
    assert.equal(saved.path_matching, context.supportsPathMatching(state));
    assert.equal(saved.providers.plex.history.include_marked_watched, false);
    assert.equal(state.cfgRaw.plex.history.include_marked_watched, true);
    assert.equal(saved.features.progress.anime_only_sync, false);
    assert.equal(context.normalizeAnimeProgressOptions(state).use_anime_mapping, scenario.expected);
    assert.equal(state.options.history.use_anime_mapping, true);
  });
}

for (const scenario of [
  {name: "CrossWatch target", src: "PLEX", dst: "CROSSWATCH", expected: true},
  {name: "CrossWatch source", src: "CROSSWATCH", dst: "TRAKT", expected: true},
  {name: "SIMKL target", src: "PLEX", dst: "SIMKL", expected: true},
  {name: "AniList target", src: "PLEX", dst: "ANILIST", expected: true},
  {name: "no anime provider", src: "PLEX", dst: "TRAKT", expected: false},
  {name: "global mapping disabled", src: "PLEX", dst: "CROSSWATCH", global: false, expected: false},
]) {
  test(`watchlist and ratings anime mapping survives a save: ${scenario.name}`, () => {
    const context = vm.createContext({
      ID: () => null,
      same: (a, b) => String(a).toLowerCase() === String(b).toLowerCase(),
      isSimkl: value => value === "SIMKL", isAniList: value => value === "ANILIST",
      isCrossWatch: value => value === "CROSSWATCH", isMDBList: () => false,
      hasOwn: (value, key) => Object.hasOwn(value, key), defaultFor: () => ({}),
      applyProgressMaxRecommendation: () => ({maxPercent: 90}), ratingsDisabledFor: () => new Set(),
      pairSupportsHistoryRewatches: () => false, collectionTypesForPair: () => ["movies"],
      sanitizeFeaturesForPair: (_, features) => features, getPairProviderStrictValue: () => false,
      libraryOnlyOn: () => false, tmdbMetadataReady: () => true,
    });
    for (const name of ["isTwoWayMode", "globalAnimeMappingEnabled", "hasAniList", "hasAnimeProvider", "simklCanReceiveProgress", "supportsPathMatching", "getPairMarkedWatchedValue", "buildPayload"])
      vm.runInContext(sourceFor(name), context);
    const state = {
      src: scenario.src, dst: scenario.dst,
      cfgRaw: {anime_mapping: {enabled: scenario.global !== false}},
      options: {watchlist: {use_anime_mapping: true}, ratings: {use_anime_mapping: true}},
    };
    const saved = context.buildPayload(state, {dataset: {}});
    assert.equal(saved.features.watchlist.use_anime_mapping, scenario.expected);
    assert.equal(saved.features.ratings.use_anime_mapping, scenario.expected);
    if (!saved.providers?.plex) return;
    assert.equal(Object.hasOwn(saved.providers.plex, "history"), false);
    state.cfgRaw.plex = {history: {include_marked_watched: false}};
    state.pairProviders = saved.providers;
    assert.equal(context.getPairMarkedWatchedValue(state), false);
    state.cfgRaw.plex.history.include_marked_watched = true;
    assert.equal(context.getPairMarkedWatchedValue(state), true);
    const resaved = context.buildPayload(state, {dataset: {}});
    assert.equal(Object.hasOwn(resaved.providers.plex, "history"), false);
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
      defaultFor: () => ({}), isAniList: () => false, isSimkl: () => false, isMDBList: () => false, isCrossWatch: () => false,
      applyProgressMaxRecommendation: () => ({maxPercent: 90}), ratingsDisabledFor: () => new Set(),
      pairSupportsHistoryRewatches: () => false, collectionTypesForPair: () => ["movies"],
      sanitizeFeaturesForPair: (_, features) => features, getPairProviderStrictValue: () => false,
      libraryOnlyOn: () => false, tmdbMetadataReady: () => true,
    });
    vm.runInContext([sourceFor("renderInstanceSelects"), sourceFor("hasAniList"), sourceFor("hasAnimeProvider"), sourceFor("supportsPathMatching"), sourceFor("getPairMarkedWatchedValue"), sourceFor("buildPayload")].join(";"), context);
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


for (const [src, dst, expected] of [["PLEX", "JELLYFIN", true], ["EMBY", "PLEX", true], ["JELLYFIN", "JELLYFIN", true], ["PLEX", "TRAKT", false], ["KODI", "PLEX", false]]) {
  test(`path matching availability: ${src} to ${dst}`, () => {
    const context = vm.createContext({});
    vm.runInContext(sourceFor("supportsPathMatching"), context);
    assert.equal(context.supportsPathMatching({src, dst}), expected);
  });
}

for (const inherited of [false, true]) {
  test(`Marked Watched pair override survives changes without changing inherited default: ${inherited}`, () => {
    const listeners = [];
    const context = vm.createContext({ID: () => null});
    vm.runInContext([sourceFor("getPairMarkedWatchedValue"), sourceFor("bindChangeHandlers")].join(";"), context);
    const cfgRaw = {plex: {history: {include_marked_watched: inherited}}};
    const state = {cfgRaw};
    assert.equal(context.getPairMarkedWatchedValue(state), inherited);
    context.bindChangeHandlers(state, {addEventListener: (event, handler) => { if (event === "change") listeners.push(handler); }});
    for (const handler of listeners) handler({target: {id: "plx-marked-watched", checked: !inherited}});
    assert.equal(context.getPairMarkedWatchedValue(state), !inherited);
    assert.equal(context.getPairMarkedWatchedValue({cfgRaw}), inherited);
    assert.equal(cfgRaw.plex.history.include_marked_watched, inherited);
  });
}

test("General groups supported matching controls without requiring a media-server pair", () => {
  const left = element(), right = element();
  const context = vm.createContext({
    ID: id => id === "cx-feat-panel" ? left : id === "cx-adv-panel" ? right : null, Q: () => null,
    isTwoWayMode: () => false, normalizeRemoveMode: () => "source_deletes",
    hasPlex: state => [state.src, state.dst].includes("PLEX"),
    hasJelly: state => [state.src, state.dst].includes("JELLYFIN"),
    hasEmby: state => [state.src, state.dst].includes("EMBY"),
    getPairProviderStrictValue: () => true,
  });
  vm.runInContext([sourceFor("supportsPathMatching"), sourceFor("renderFeaturePanel")].join(";"), context);
  context.renderFeaturePanel({feature: "globals", src: "PLEX", dst: "TRAKT"});
  assert.match(left.innerHTML, /General/);
  assert.match(right.innerHTML, /Plex: Strict ID matching/);
  assert.doesNotMatch(right.innerHTML, /id="pair-path-matching"|id="jf-strict-ids"|plx-marked-watched/);
  context.renderFeaturePanel({feature: "globals", src: "PLEX", dst: "JELLYFIN"});
  assert.match(right.innerHTML, /id="pair-path-matching"/);
  assert.match(right.innerHTML, /Jellyfin: Strict ID matching/);
});

test("saving History leaves the shared Plex Marked Watched default unchanged", async () => {
  let saved;
  const context = vm.createContext({
    ID: id => id === "plx-marked-watched" ? {checked: false} : null,
    structuredClone, console,
    fetch: async (_, options) => {
      if (options?.method === "POST") saved = JSON.parse(options.body);
      return {ok: true, json: async () => ({plex: {history: {include_marked_watched: true}}})};
    },
  });
  vm.runInContext(sourceFor("saveConfigBits"), context);
  await context.saveConfigBits({src: "PLEX", dst: "JELLYFIN"});
  assert.equal(saved.plex.history.include_marked_watched, true);
});
