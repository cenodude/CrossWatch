// tests/kitsu-auth.test.mjs
// CrossWatch - Kitsu auth modal staging and cancellation tests
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

function harness({ connected = false, enabled = false, dismissed = false } = {}) {
  const listeners = new Map();
  const nodes = new Map(["kitsu_username", "kitsu_password", "kitsu_connect", "kitsu_disconnect",
    "kitsu_mapping_recommendation", "kitsu_mapping_recommendation_state", "kitsu_enable_mapping", "kitsu_dismiss_mapping"
  ].map(id => {
    const classes = new Set();
    return [id, {
      value: "", handlers: {}, addEventListener(name, callback) { this.handlers[name] = callback; },
      classList: { toggle(name, on) { if (on) classes.add(name); else classes.delete(name); }, contains: name => classes.has(name) }
    }];
  }));
  const requests = [];
  const storage = new Map(dismissed ? [["cw.ui.kitsu.animeMapping.dismissed.v1", "1"]] : []);
  let complete;
  let completeMapping;
  const Shared = {
    el: id => nodes.get(id), setConnectLocked() {}, setStatus() {}, notify() {}, reportProviderUsage: () => false,
    createProfileAdapter: () => ({ getInstance: () => "P01", ensureUI() {}, api: path => `${path}?instance=P01` }),
    fetchJSON: async (url, options) => {
      requests.push({url, options});
      if (url.includes("/connect")) return await new Promise(resolve => { complete = resolve; });
      if (url === "/api/anime-mapping/status") return {ok: true, data: {enabled}};
      if (url === "/api/anime-mapping/settings") return await new Promise(resolve => { completeMapping = resolve; });
      return {ok: true, data: {ok: true, connected}};
    }
  };
  const context = vm.createContext({
    window: { CW: { AuthShared: Shared }, _cfgCache: {anime_mapping: {enabled, auto_update: false, use_for_pairs: ["simkl"]}}, dispatchEvent() {} },
    document: { addEventListener: (name, callback) => listeners.set(name, callback) },
    localStorage: {getItem: key => storage.get(key), setItem: (key, value) => storage.set(key, value)},
    CustomEvent: class {}, Map, console
  });
  vm.runInContext(readFileSync(new URL("../assets/auth/auth.kitsu.js", import.meta.url), "utf8"), context);
  return {nodes, requests, storage, window: context.window,
    completeMapping: result => completeMapping(result),
    complete: data => complete({ok: true, data: {ok: true, connection: data}}),
    event: (name, detail) => listeners.get(name)?.({detail})};
}

test("Kitsu stages tokens for the selected instance and immediately clears credentials", async () => {
  const h = harness();
  await Promise.resolve();
  h.nodes.get("kitsu_username").value = "tester";
  h.nodes.get("kitsu_password").value = "private-password";
  const request = h.nodes.get("kitsu_connect").handlers.click();
  assert.equal(h.nodes.get("kitsu_password").value, "");
  assert.equal(h.nodes.get("kitsu_username").value, "");
  h.complete({access_token: "access", refresh_token: "refresh"});
  await request;
  const cfg = {};
  h.event("settings-collect", {cfg});
  assert.equal(cfg.kitsu.instances.P01.access_token, "access");
  assert.equal(cfg.kitsu.access_token, undefined);
  assert.equal(JSON.stringify(cfg).includes("private-password"), false);
  h.event("cw-auth-modal-closed", {provider: "kitsu"});
  const cancelled = {};
  h.event("settings-collect", {cfg: cancelled});
  assert.deepEqual(cancelled, {});
});

test("Kitsu mapping recommendation is conditional and dismissible", async () => {
  for (const state of [
    {connected: false, enabled: false, hidden: true},
    {connected: true, enabled: true, hidden: true},
    {connected: true, enabled: false, hidden: false},
    {connected: true, enabled: false, dismissed: true, hidden: true}
  ]) {
    const h = harness(state);
    await h.window.cwAuth.kitsu.hydrate();
    await Promise.resolve();
    assert.equal(h.nodes.get("kitsu_mapping_recommendation").classList.contains("hidden"), state.hidden);
    if (!state.hidden) {
      h.nodes.get("kitsu_dismiss_mapping").handlers.click();
      assert.equal(h.nodes.get("kitsu_mapping_recommendation").classList.contains("hidden"), true);
      assert.equal(h.storage.get("cw.ui.kitsu.animeMapping.dismissed.v1"), "1");
    }
    assert.equal(h.requests.some(r => r.options?.method === "POST"), false);
  }
});

test("Kitsu enables global mapping without changing existing mapping preferences", async () => {
  const h = harness({connected: true});
  await h.window.cwAuth.kitsu.hydrate();
  const button = h.nodes.get("kitsu_enable_mapping");
  const enabling = button.handlers.click();
  assert.equal(button.disabled, true);
  assert.equal(h.nodes.get("kitsu_dismiss_mapping").disabled, true);
  assert.match(h.nodes.get("kitsu_mapping_recommendation_state").textContent, /Downloading/);
  const request = h.requests.find(r => r.url === "/api/anime-mapping/settings");
  assert.deepEqual(JSON.parse(request.options.body), {enabled: true});
  h.completeMapping({ok: true, data: {ok: true, status: {enabled: true}}});
  await enabling;
  assert.equal(h.window._cfgCache.anime_mapping.enabled, true);
  assert.equal(h.window._cfgCache.anime_mapping.auto_update, false);
  assert.deepEqual(Array.from(h.window._cfgCache.anime_mapping.use_for_pairs), ["simkl"]);
  assert.equal(h.nodes.get("kitsu_mapping_recommendation").classList.contains("hidden"), true);
  assert.equal(button.disabled, false);
});

test("Kitsu mapping failures remain visible and can be retried", async () => {
  const h = harness({connected: true});
  await h.window.cwAuth.kitsu.hydrate();
  const button = h.nodes.get("kitsu_enable_mapping");
  const enabling = button.handlers.click();
  h.completeMapping({ok: false, data: {message: "Could not enable mapping"}});
  await enabling;
  assert.equal(button.disabled, false);
  assert.equal(h.nodes.get("kitsu_mapping_recommendation").classList.contains("hidden"), false);
  assert.equal(h.nodes.get("kitsu_mapping_recommendation_state").textContent, "Could not enable mapping");
  const retry = button.handlers.click();
  h.completeMapping({ok: true, data: {ok: true, status: {enabled: true}, bootstrap_error: "Dataset download failed"}});
  await retry;
  assert.equal(h.nodes.get("kitsu_mapping_recommendation").classList.contains("hidden"), false);
  assert.equal(h.nodes.get("kitsu_mapping_recommendation_state").textContent, "Dataset download failed");
});

test("Closing Kitsu during login prevents a late response from staging tokens", async () => {
  const h = harness();
  await Promise.resolve();
  h.nodes.get("kitsu_username").value = "tester";
  h.nodes.get("kitsu_password").value = "private-password";
  const request = h.nodes.get("kitsu_connect").handlers.click();
  h.event("cw-auth-modal-closed", {provider: "kitsu"});
  h.complete({access_token: "late-token"});
  await request;
  const cfg = {};
  h.event("settings-collect", {cfg});
  assert.deepEqual(cfg, {});
});

test("Kitsu is selectable for watcher and webhook scrobbles without Plex rating forwarding", () => {
  for (const name of ["route", "webhook"]) {
    const source = readFileSync(new URL(`../assets/js/modals/scrobbler-${name}/index.js`, import.meta.url), "utf8")
      .replace(/export default \{ mount, unmount \};/, "").replace(/export (async )?function/g, "$1function");
    const context = vm.createContext({window: {}, console});
    vm.runInContext(source, context);
    assert.equal(vm.runInContext('sinks.includes("kitsu")', context), true);
    assert.equal(vm.runInContext('ratingSinks.includes("kitsu")', context), false);
    assert.equal(vm.runInContext('label("kitsu")', context), "Kitsu");
  }
});

test("Kitsu allows history removal in both directions but locks rewatches and episode ratings", () => {
  const source = readFileSync(new URL("../assets/js/modals/pair-config/custom-rules.js", import.meta.url), "utf8")
    .replace(/export function/g, "function");
  const context = vm.createContext({});
  vm.runInContext(source, context);
  for (const pair of [{src: "KITSU", dst: "TRAKT", twoWay: true}, {src: "TRAKT", dst: "KITSU"}]) {
    context.pair = pair;
    assert.equal(vm.runInContext('featureAllowedForPair(pair, "history")', context), true);
    assert.equal(vm.runInContext('historyRemoveLockedForPair(pair)', context), false);
    assert.equal(vm.runInContext('ratingsDisabledForPair(pair).has("episodes")', context), true);
    const features = vm.runInContext('sanitizeFeaturesForPair(pair, {history: {enable: true, add: true, remove: true, rewatches: true}})', context);
    assert.equal(features.history.enable, true);
    assert.equal(features.history.remove, true);
    assert.equal(features.history.rewatches, false);
  }
  context.pair = {src: "SIMKL", dst: "ANILIST"};
  assert.equal(vm.runInContext('historyRemoveLockedForPair(pair)', context), true);
  assert.equal(vm.runInContext('sanitizeFeaturesForPair(pair, {history: {remove: true}}).history.remove', context), false);
  context.pair = {src: "ANILIST", dst: "KITSU"};
  assert.equal(vm.runInContext('featureAllowedForPair(pair, "history")', context), false);
});


test("Source watch status is offered for SIMKL to Kitsu and AniList", () => {
  const source = readFileSync(new URL("../assets/js/modals/pair-config/custom-rules.js", import.meta.url), "utf8")
    .replace(/export function/g, "function");
  const context = vm.createContext({});
  vm.runInContext(source, context);
  for (const [src, dst, expected] of [["SIMKL", "KITSU", true], ["simkl", "anilist", true],
    ["KITSU", "SIMKL", false], ["TRAKT", "KITSU", false], ["SIMKL", "TRAKT", false]]) {
    context.pair = {src, dst};
    assert.equal(vm.runInContext('sourceWatchStatusAllowed(pair)', context), expected);
  }
});
