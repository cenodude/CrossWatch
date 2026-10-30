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

test("New provider selections retain library filtering defaults while saved pairs keep their setting", () => {
  const exported = parse(index, {ecmaVersion: "latest", sourceType: "module"}).body.find(node => node.type === "ExportDefaultDeclaration");
  const mount = exported.declaration.properties.find(node => node.key.name === "mount").value;
  const initialize = mount.body.body.find(node => node.type === "IfStatement" && index.slice(node.test.start, node.test.end) === 'pair && typeof pair==="object"');
  for (const scenario of [
    {pair: {}, expected: true},
    {pair: {features: {history: {library_only: false}}}, expected: false},
    {pair: {id: "saved"}, expected: false},
    {pair: {id: "saved", features: {history: {library_only: true}}}, expected: true},
  ]) {
    const context = vm.createContext({
      PROGRESS_LEGACY_MAX_PERCENT: 95,
      pair: {source: "JELLYFIN", target: "SILO", ...scenario.pair},
      wrap: {dataset: {}}, normalizePairProviders: value => value || {},
      isTwoWayMode: () => false,
      byName: (_, name) => ({capabilities: {library_presence: {features: name === "SILO" ? ["history", "ratings", "progress"] : []}}}),
    });
    vm.runInContext(["defaultState", "providerSupportsLibraryOnly", "pairSupportsLibraryOnly", "libraryOnlyOn"].map(sourceFor).join(";"), context);
    vm.runInContext(`const state = defaultState(); ${index.slice(initialize.start, initialize.end)}`, context);
    assert.equal(vm.runInContext('libraryOnlyOn(state, state.options.history)', context), scenario.expected);
    if (!scenario.pair.id) {
      assert.equal(vm.runInContext('libraryOnlyOn(state, state.options.progress, "progress")', context), true);
      assert.equal(vm.runInContext('libraryOnlyOn(state, state.options.watchlist, "watchlist")', context), false);
    }
  }
});

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
      ID: id => id === "cx-src-inst" ? src : dst, document: {documentElement: {dataset: {}}},
      same: (a, b) => a === b, escHTML: value => value, G: {}, renderPairUserProfileControl() {},
      defaultFor: () => ({}), isAniList: () => false, isSimkl: () => false, isMDBList: () => false, isCrossWatch: () => false,
      applyProgressMaxRecommendation: () => ({maxPercent: 90}), ratingsDisabledFor: () => new Set(),
      pairSupportsHistoryRewatches: () => false, collectionTypesForPair: () => ["movies"],
      sanitizeFeaturesForPair: (_, features) => features, getPairProviderStrictValue: () => false,
      libraryOnlyOn: () => false, tmdbMetadataReady: () => true,
    });
    vm.runInContext([sourceFor("scopedPairView"), sourceFor("renderInstanceSelects"), sourceFor("hasAniList"), sourceFor("hasAnimeProvider"), sourceFor("supportsPathMatching"), sourceFor("getPairMarkedWatchedValue"), sourceFor("buildPayload")].join(";"), context);
    const state = {src: "PLEX", dst: "PLEX", src_instance: instance, instanceMap: {PLEX: [{id: instance, configured: true}]}};
    context.renderInstanceSelects(state);
    assert.equal(state.src_instance, instance);
    assert.equal(state.dst_instance, "");
    assert.equal(dst.disabled, true);
    const payload = context.buildPayload(state, {dataset: {}});
    assert.equal(payload.target_instance, "");
  });
}

for (const dataset of [{cwRole: "user"}, {cwViewAs: "1"}]) {
  test(`a scoped ${Object.keys(dataset)[0]} view never offers the default instance`, () => {
    const src = element(), dst = element();
    const context = vm.createContext({
      ID: id => id === "cx-src-inst" ? src : dst, document: {documentElement: {dataset}},
      same: (a, b) => a === b, escHTML: value => value, G: {}, renderPairUserProfileControl() {},
    });
    vm.runInContext([sourceFor("scopedPairView"), sourceFor("renderInstanceSelects")].join(";"), context);
    const state = {src: "SIMKL", dst: "MDBLIST", src_instance: "default", dst_instance: "default", instancesLoaded: true, instanceMap: {SIMKL: [{id: "SIMKL-P01", configured: true}]}};
    context.renderInstanceSelects(state);
    assert.equal(state.src_instance, "SIMKL-P01");
    assert.equal(state.dst_instance, "");
    assert.equal(dst.disabled, true);
    assert.ok(!src.innerHTML.includes("Default"));
    assert.ok(dst.innerHTML.includes("No configured instance"));
    state.dst = "SIMKL";
    context.renderInstanceSelects(state);
    assert.equal(state.dst_instance, "");
    assert.ok(dst.innerHTML.includes("No other configured instance"));
  });
}

test("a scoped edit never keeps an unconfigured saved instance or swaps it silently", () => {
  const src = element(), dst = element();
  const context = vm.createContext({
    ID: id => id === "cx-src-inst" ? src : dst, document: {documentElement: {dataset: {cwRole: "user"}}},
    same: (a, b) => a === b, escHTML: value => value, G: {}, renderPairUserProfileControl() {},
    resetPairUserProfileControl() {}, renderFeaturePanel() {},
  });
  vm.runInContext([sourceFor("scopedPairView"), sourceFor("renderInstanceSelects")].join(";"), context);
  const state = {
    src: "SIMKL", dst: "TRAKT", src_instance: "SIMKL-P02", dst_instance: "TRAKT-P01", instancesLoaded: true,
    savedInstances: {src: {provider: "SIMKL", instance: "SIMKL-P02"}, dst: {provider: "TRAKT", instance: "TRAKT-P01"}},
    instanceMap: {SIMKL: [{id: "SIMKL-P01", configured: true}, {id: "SIMKL-P02", configured: false}]},
  };
  context.renderInstanceSelects(state);
  assert.equal(state.src_instance, "");
  assert.ok(src.innerHTML.includes("Choose an account"));
  assert.ok(!src.innerHTML.includes("SIMKL-P02"));
  assert.equal(state.dst_instance, "");
  assert.equal(dst.disabled, true);
  state.dst = "MDBLIST";
  context.renderInstanceSelects(state);
  assert.equal(state.src_instance, "");
  assert.ok(src.innerHTML.includes("Choose an account"));
  src.value = "SIMKL-P01";
  src.onchange();
  assert.equal(state.src_instance, "SIMKL-P01");
  assert.ok(!src.innerHTML.includes("Choose an account"));
});

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

test("Discover opt-in stays in its pair and allows items outside the destination library", () => {
  const listeners = [];
  const libraryOnly = {checked: true};
  const context = vm.createContext({ID: id => id === "cx-hs-library-only" ? libraryOnly : null});
  vm.runInContext(sourceFor("bindChangeHandlers"), context);
  const cfgRaw = {plex: {history: {discover_fallback: false}}};
  const state = {cfgRaw, options: {history: {library_only: true, add: true}},
    pairProviders: {plex: {history: {include_marked_watched: false}}}};
  context.bindChangeHandlers(state, {addEventListener: (event, handler) => { if (event === "change") listeners.push(handler); }});
  for (const handler of listeners) handler({target: {id: "plx-discover-fallback", checked: true}});
  assert.equal(state.pairProviders.plex.history.discover_fallback, true);
  assert.equal(state.pairProviders.plex.history.include_marked_watched, false);
  assert.equal(state.options.history.library_only, false);
  assert.equal(state.options.history.add, true);
  assert.equal(libraryOnly.checked, false);
  assert.equal(cfgRaw.plex.history.discover_fallback, false);
  for (const handler of listeners) handler({target: {id: "plx-discover-fallback", checked: false}});
  assert.equal(state.pairProviders.plex.history.discover_fallback, false);
});

test("Discover control is off by default and only shown when the pair writes to Plex", () => {
  const render = index.split("\n").find(line => line.includes('parts.push(`') && line.includes('id="plx-discover-fallback"'));
  for (const [src, dst, twoWay, visible] of [["TRAKT", "PLEX", false, true], ["PLEX", "TRAKT", false, false], ["PLEX", "TRAKT", true, true]]) {
    const context = vm.createContext({parts: [], state: {src, dst},
      same: (a, b) => a.toLowerCase() === b.toLowerCase(), isTwoWayMode: () => twoWay});
    vm.runInContext(render, context);
    assert.equal(context.parts.length > 0, visible);
    assert.doesNotMatch(context.parts.join(""), /type="checkbox" checked/);
  }
});

test("Discover turns off when destination library filtering is enabled", () => {
  const listeners = [];
  const nodes = new Map([["cx-hs-library-only", {checked: true}], ["plx-discover-fallback", {checked: true}]]);
  const context = vm.createContext({ID: id => nodes.get(id), Q: () => null, updateFlow: () => {}});
  vm.runInContext(sourceFor("bindChangeHandlers"), context);
  const state = {options: {history: {}}, visited: new Set(), pairProviders: {plex: {history: {discover_fallback: true}}}};
  context.bindChangeHandlers(state, {addEventListener: (event, handler) => { if (event === "change") listeners.push(handler); }});
  for (const handler of listeners) handler({target: {id: "cx-hs-library-only", checked: true}});
  assert.equal(state.pairProviders.plex.history.discover_fallback, false);
  assert.equal(nodes.get("plx-discover-fallback").checked, false);
  assert.equal(state.options.history.library_only, true);
});

for (const [feature, prefix, rule] of [["ratings", "rt", "applyRatingsTypeRules"], ["collection", "co", "applyCollectionTypeRules"]]) {
  test(feature + " scope stays disabled until the feature is enabled", () => {
    const nodes = new Map();
    const types = ["movies", "shows", "seasons", "episodes"];
    for (const name of ["enable", "type-all", ...types.map(t => "type-" + t)]) {
      const classes = new Set();
      const row = {classList: {add: x => classes.add(x), toggle: (x, on) => on ? classes.add(x) : classes.delete(x)}};
      nodes.set("cx-" + prefix + "-" + name, {checked: name !== "enable", disabled: false, closest: () => row, classes});
    }
    const context = vm.createContext({ID: id => nodes.get(id), ratingsDisabledFor: () => new Set(["seasons"]), collectionDisabledFor: () => new Set(["seasons"]), collectionAllowedTypes: () => ["movies", "shows", "episodes"], updateRtSummary: () => {}, state: {options: {[feature]: {}}}});
    vm.runInContext(sourceFor(rule), context);
    for (const enabled of [false, true, false]) {
      nodes.get("cx-" + prefix + "-enable").checked = enabled;
      vm.runInContext(rule + "(state)", context);
      for (const type of types) {
        const node = nodes.get("cx-" + prefix + "-type-" + type);
        assert.equal(node.disabled, !enabled || type === "seasons");
        assert.equal(node.classes.has("muted"), node.disabled);
        assert.equal(node.checked, type !== "seasons");
      }
    }
  });
}

for (const controlId of ["plx-marked-watched", "plx-cloud-read", "plx-discover-fallback"]) test(`${controlId} follows History enable without changing its value`, () => {
  for (const checked of [false, true]) {
    const classes = new Set();
    const control = {checked, dataset: {}, closest: () => ({classList: {toggle: (name, on) => on ? classes.add(name) : classes.delete(name)}})};
    const enable = {checked: false};
    const nodes = new Map([["cx-hs-enable", enable], [controlId, control]]);
    const context = vm.createContext({ID: id => nodes.get(id), Q: selector => nodes.get(selector.slice(1))});
    vm.runInContext(sourceFor("applySubDisable"), context);
    for (const enabled of [false, true, false]) {
      enable.checked = enabled;
      vm.runInContext('applySubDisable("history")', context);
      assert.equal(control.disabled, !enabled);
      assert.equal(classes.has("muted"), !enabled);
      assert.equal(control.checked, checked);
    }
  }
});


test("cloud read opt-in is independent of writes and disables removals", () => {
  const listeners = [];
  const remove = {checked:true};
  const context = vm.createContext({ID: id => id === "cx-hs-remove" ? remove : null});
  vm.runInContext(sourceFor("bindChangeHandlers"), context);
  const state = {options:{history:{remove:true}}, pairProviders:{plex:{history:{discover_fallback:true}}}};
  context.bindChangeHandlers(state, {addEventListener:(event, handler)=>{if(event === "change") listeners.push(handler);}});
  for(const handler of listeners) handler({target:{id:"plx-cloud-read",checked:true}});
  assert.equal(state.pairProviders.plex.history.read_cloud_history, true);
  assert.equal(state.pairProviders.plex.history.discover_fallback, true);
  assert.equal(state.options.history.remove, false);
  assert.equal(remove.checked, false);
  for(const handler of listeners) handler({target:{id:"plx-cloud-read",checked:false}});
  assert.equal(state.pairProviders.plex.history.read_cloud_history, false);
  assert.equal(state.pairProviders.plex.history.discover_fallback, true);
});

test("cloud read is offered only when Plex sends history", () => {
  const render = index.split("\n").find(line => line.includes('parts.push(`') && line.includes('id="plx-cloud-read"'));
  for(const [src,dst,twoWay,visible] of [["PLEX","TRAKT",false,true],["TRAKT","PLEX",false,false],["TRAKT","PLEX",true,true]]){
    const context = vm.createContext({parts:[],state:{src,dst},same:(a,b)=>a.toLowerCase()===b.toLowerCase(),isTwoWayMode:()=>twoWay});
    vm.runInContext(render, context);
    assert.equal(context.parts.length > 0,visible);
    assert.doesNotMatch(context.parts.join(""), /type="checkbox" checked/);
  }
});


test("Plex cloud controls follow direction and preserve independent settings across mode changes", () => {
  const render = index.split("\n").filter(line => line.includes('parts.push(`') && /id="plx-(cloud-read|discover-fallback)"/.test(line)).join("\n");
  for (const [src, dst] of [["PLEX", "TRAKT"], ["SIMKL", "PLEX"], ["JELLYFIN", "PLEX"], ["TRAKT", "SIMKL"], ["PLEX", "PLEX"]]) {
    for (const read of [false, true]) for (const write of [false, true]) {
      const state = {src, dst, twoWay:false, pairProviders:{plex:{history:{read_cloud_history:read, discover_fallback:write}}}};
      const before = JSON.stringify(state.pairProviders);
      const context = vm.createContext({parts:[],state,same:(a,b)=>a.toLowerCase()===b.toLowerCase(),isTwoWayMode:state=>state.twoWay});
      for (const twoWay of [false, true, false]) {
        state.twoWay = twoWay;
        context.parts = [];
        vm.runInContext(render, context);
        const html = context.parts.join("");
        const hasPlex = src === "PLEX" || dst === "PLEX";
        const separate = twoWay || (src === "PLEX" && dst === "PLEX");
        assert.equal((html.match(/type="checkbox"/g) || []).length, hasPlex ? separate ? 2 : 1 : 0);
        if (hasPlex) {
          assert.equal(html.includes(">Include Plex cloud history</label>"), !separate);
          assert.equal(html.includes(">Read cloud history</label>"), separate);
          assert.equal(html.includes(">Write cloud fallback</label>"), separate);
          for (const [id, visible, checked] of [["plx-cloud-read", twoWay || src === "PLEX", read], ["plx-discover-fallback", twoWay || dst === "PLEX", write]]) {
            const input = html.match(new RegExp(`<input id="${id}"[^>]*>`));
            assert.equal(!!input, visible);
            if (input) assert.equal(input[0].includes(" checked"), checked);
          }
        }
        assert.equal(JSON.stringify(state.pairProviders), before);
      }
    }
  }
});
