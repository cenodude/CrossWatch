/* tests/scrobbler-sink-instances.test.mjs */
/* CrossWatch - Scrobbler Sink Instance Tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

function modal(name) {
  const source = readFileSync(new URL(`../assets/js/modals/scrobbler-${name}/index.js`, import.meta.url), "utf8")
    .replace(/export default \{ mount, unmount \};/, "")
    .replace(/export (async )?function/g, "$1function");
  const context = vm.createContext({window: {}, console});
  vm.runInContext(source, context);
  return (script) => JSON.parse(JSON.stringify(vm.runInContext(script, context)));
}

const overview = {
  eligible_sources: [{provider: "plex", profiles: [{instance: "default", eligible: true}, {instance: "P01", eligible: true}]}],
  destination_availability: [{provider: "plex", profiles: [{instance: "default", configured: true}, {instance: "P01", configured: true}]}],
};

for (const provider of ["plex", "jellyfin", "emby", "kodi", "scrob"]) {
  test(`${provider} watcher filters its own instance while allowing another`, () => {
    const run = modal("route");
    const data = JSON.stringify(overview).replaceAll('"plex"', JSON.stringify(provider));
    run(`props = {overview: ${data}}; draft = {provider: "${provider}", provider_instance: "default"}; null`);
    assert.deepEqual(run(`sinkProviders("", "${provider}")`), [provider]);
    assert.deepEqual(run(`allSinkProfiles("${provider}").map(p => p.instance)`), ["P01"]);
    run('draft.provider_instance = "P01"; null');
    assert.deepEqual(run(`allSinkProfiles("${provider}").map(p => p.instance)`), ["default"]);
  });
}

test("watcher allows the same provider with a different instance in both directions", () => {
  const run = modal("route");
  run(`props = {overview: ${JSON.stringify(overview)}}; draft = {provider: "plex", provider_instance: "default"}; null`);
  assert.deepEqual(run('sinkProviders("", "plex")'), ["plex"]);
  assert.deepEqual(run('allSinkProfiles("plex").map(p => p.instance)'), ["P01"]);
  run('draft.provider_instance = "P01"; null');
  assert.deepEqual(run('allSinkProfiles("plex").map(p => p.instance)'), ["default"]);
});

test("watcher defaults select an allowed destination instance", () => {
  const run = modal("route");
  run(`props = {overview: ${JSON.stringify(overview)}}; null`);
  const route = run('defaultRoute()');
  assert.equal(route.provider_instance, "default");
  assert.equal(route.sink_instance, "P01");
});

test("watcher hides a provider when its only instance is the source", () => {
  const run = modal("route");
  const onlyDefault = structuredClone(overview);
  onlyDefault.destination_availability[0].profiles.pop();
  run(`props = {overview: ${JSON.stringify(onlyDefault)}}; draft = {provider: "plex", provider_instance: "default"}; null`);
  assert.deepEqual(run('sinkProviders("", "plex")'), []);
});

test("webhook filters instances rather than excluding the whole source provider", () => {
  const run = modal("webhook");
  run(`props = {overview: ${JSON.stringify(overview)}, webhook: {provider: "plex", provider_instance: "default"}}; null`);
  assert.deepEqual(run('availableSinks()'), ["plex"]);
  assert.deepEqual(run('sinkProfiles("plex").map(p => p.instance)'), ["P01"]);
  assert.equal(run('selectedSinkInstance("plex")'), "P01");
  run('props.webhook = {provider: "plex", provider_instance: "P01", sink: "plex", sink_instance: "P01"}; null');
  assert.deepEqual(run('sinkProfiles("plex").map(p => p.instance)'), ["default"]);
  assert.equal(run('selectedSinkInstance("plex")'), "default");
});

test("watcher rewatch option defaults off, renders only for SIMKL and saves the selection", () => {
  const run = modal("route");
  run('props = {}; draft = {provider: "plex", sink: "simkl", options: {watch: {}, scrobble: {}}}; null');
  let html = run('optionsPanel(draft)');
  assert.match(html, /Track rewatches/);
  assert.doesNotMatch(html, /id="scr-simkl-rewatches" checked/);
  run('draft.options.watch.simkl_rewatches = true; null');
  assert.match(run('optionsPanel(draft)'), /id="scr-simkl-rewatches" checked/);
  run('root = {querySelector: (id) => id === "#scr-simkl-rewatches" ? {checked: true} : null, querySelectorAll: () => []}; null');
  assert.equal(run('collect().options.watch.simkl_rewatches'), true);
  run('draft.sink = "trakt"; null');
  assert.doesNotMatch(run('optionsPanel(draft)'), /Track rewatches/);
  assert.equal(run('Object.hasOwn(collect().options.watch, "simkl_rewatches")'), false);
});

test("webhook rewatch option defaults off and round trips the destination setting", () => {
  const run = modal("webhook");
  run('props = {mode: "edit", overview: {destination_availability: [{provider: "simkl", profiles: [{instance: "default", configured: true}]}]}, webhook: {provider: "plex", provider_instance: "default", sink: "simkl", effective_settings: {sinks: ["simkl"]}}}; null');
  assert.doesNotMatch(run('optionsPanel()'), /id="scw-simkl-rewatches" checked/);
  run('props.webhook.effective_settings.simkl_rewatches = true; null');
  assert.match(run('optionsPanel()'), /id="scw-simkl-rewatches" checked/);
  run('root = {querySelector: (id) => id === "#scw-sink" ? {value: "simkl"} : id === "#scw-simkl-rewatches" ? {checked: true} : null}; null');
  assert.equal(run('payload().simkl_rewatches'), true);
});
