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
