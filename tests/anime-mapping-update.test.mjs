import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/helpers/settings-ui.js", import.meta.url), "utf8");
const start = source.indexOf("async function cwAnimeMappingRun(action)");
const end = source.indexOf("function cwBuildAnimeMappingPanel()", start);
assert.ok(start >= 0 && end > start);

for (const [name, result, expected, success] of [
  ["unchanged datasets", {ok:true, updated:false}, "Anime mapping already up to date", true],
  ["identity update", {ok:true, updated:true, identity_updated:true, mappings_updated:false}, "Anime identity data updated", true],
  ["both datasets", {ok:true, updated:true, identity_updated:true, mappings_updated:true}, "Anime mapping updated", true],
  ["index repair", {ok:true, updated:false, rebuilt:true}, "Anime mapping index rebuilt", true],
  ["identity failure", {ok:false, updated:false, error:"animeApi update failed (RuntimeError)"}, "animeApi update failed (RuntimeError)", false],
]) {
  test(`Update feedback: ${name}`, async () => {
    const toasts = [], busy = [], requests = [];
    const context = vm.createContext({
      window:{CW:{DOM:{showToast:(...args) => toasts.push(args)}}},
      _cwAnimeMappingSetBusy:value => busy.push(value),
      cwAnimeMappingRenderStatus() {}, async cwAnimeMappingRefreshStatus() {},
      async fetch(url, options) {requests.push([url, JSON.parse(options.body)]); return {ok:true, json:async () => result};},
    });
    vm.runInContext(source.slice(start, end), context);
    await context.cwAnimeMappingRun("update");
    assert.deepEqual(toasts, [[expected, success]]);
    assert.deepEqual(busy, [true, false]);
    assert.deepEqual(requests, [["/api/anime-mapping/update", {force:false}]]);
  });
}

test("Updated date reflects the local identity update, not the aniBridge publication or latest check", () => {
  const elements = new Map();
  const context = vm.createContext({window:{}, animeMappingBusy:false,
    document:{getElementById(id) {
      if (!elements.has(id)) elements.set(id, {classList:{toggle() {}}, setAttribute() {}, removeAttribute() {}});
      return elements.get(id);
    }},
  });
  const from = source.indexOf("function _cwSetText(id, value)");
  const to = source.indexOf("async function cwAnimeMappingRefreshStatus()", from);
  vm.runInContext(source.slice(from, to), context);
  const status = {installed:true, index_ready:true, dataset_generated_on:"2026-09-06T06:44:44Z",
    release_tag:"v3", identity_installed:true, identity_release_tag:"v3",
    identity_generated_on:"2026-09-11T09:25:19Z", identity_revision:"abc123",
    last_updated_at:Date.parse("2026-09-12T01:25:00Z") / 1000,
    last_checked_at:Date.parse("2026-09-13T02:00:00Z") / 1000};
  context.cwAnimeMappingRenderStatus(status);
  assert.equal(elements.get("anime_mapping_generated").textContent, "12 Sep, 01:25 UTC");
  assert.equal(elements.get("anime_mapping_last_update").textContent, "2026-09-12 01:25 UTC");
  assert.equal(elements.get("anime_mapping_episodes_version").textContent, "v3 · 2026-09-06");
  assert.equal(elements.get("anime_mapping_identity_version").textContent, "v3 · 2026-09-11");
  assert.match(elements.get("anime_mapping_identity_version").title, /abc123/);
  context.cwAnimeMappingRenderStatus({...status, last_updated_at:0});
  assert.equal(elements.get("anime_mapping_generated").textContent, "-");
  context.cwAnimeMappingRenderStatus({...status, identity_generated_on:"", identity_revision:""});
  assert.equal(elements.get("anime_mapping_identity_version").textContent, "v3 · Date unavailable");
  context.cwAnimeMappingRenderStatus({});
  assert.equal(elements.get("anime_mapping_identity_version").textContent, "Not installed");
});
