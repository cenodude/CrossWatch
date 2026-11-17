/* tests/scrobbler-route-save.test.mjs */
/* CrossWatch - Watcher Route Save Regression Tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/modals/scrobbler-route/index.js", import.meta.url), "utf8")
  .replace(/^import .*;\r?\n/gm, "")
  .replace(/export default \{ mount, unmount \};/, "")
  .replace(/export (async )?function/g, "$1function");

function modal(mode, routes, route) {
  const requests = [];
  const saved = [];
  let closed = false;
  const response = {ok: true, routes: [{id: "R20"}]};
  const context = vm.createContext({
    window: {cxCloseModal: () => { closed = true; }},
    fetch: async (url, options) => {
      requests.push({url, method: options.method, body: JSON.parse(options.body)});
      return {ok: true, json: async () => response};
    },
    incoming: {
      mode,
      route,
      overview: {
        routes,
        eligible_sources: [{provider: "plex", profiles: [{instance: "PLEX-Angel", eligible: true}]}],
        destination_availability: [{provider: "emby", profiles: [{instance: "EMBY-Angel", configured: true}]}],
      },
      onSaved: (data) => saved.push(data),
    },
    shell: {
      querySelector: (selector) => selector === "#scr-users" ? {value: "angelblue05"} : null,
      querySelectorAll: () => [],
    },
  });
  vm.runInContext(source, context);
  vm.runInContext('props = incoming; root = shell; selectedUserProfileId = "angel"; ensureDraft();', context);
  return {context, requests, saved, response, closed: () => closed};
}

for (const hiddenId of [1, 2, 7]) {
  test(`creation leaves ID allocation to the server when hidden route R${hiddenId} already exists`, async () => {
    const routes = Array.from({length: hiddenId - 1}, (_, index) => ({
      id: `R${index + 1}`, provider: "plex", provider_instance: "PLEX-Angel", sink: "crosswatch", sink_instance: `CW-${index}`,
    }));
    const run = modal("create", routes);
    assert.equal(vm.runInContext("draft.id", run.context), `R${hiddenId}`);
    await vm.runInContext("save()", run.context);
    assert.equal(run.requests.length, 1);
    const {url, method, body} = run.requests[0];
    assert.equal(url, "/api/scrobbler/routes");
    assert.equal(method, "POST");
    assert.equal(Object.hasOwn(body, "id"), false);
    assert.equal(body.provider_instance, "PLEX-Angel");
    assert.equal(body.sink_instance, "EMBY-Angel");
    assert.equal(body.profile_id, "angel");
    assert.deepEqual(body.filters.username_whitelist, ["angelblue05"]);
    assert.deepEqual(run.saved, [run.response]);
    assert.equal(run.closed(), true);
  });
}

test("editing retains the existing route ID in the URL and payload", async () => {
  const route = {
    id: "R7", provider: "plex", provider_instance: "PLEX-Angel", sink: "emby", sink_instance: "EMBY-Angel",
  };
  const run = modal("edit", [route], route);
  await vm.runInContext("save()", run.context);
  assert.equal(run.requests.length, 1);
  assert.equal(run.requests[0].url, "/api/scrobbler/routes/R7");
  assert.equal(run.requests[0].method, "PUT");
  assert.equal(run.requests[0].body.id, "R7");
  assert.deepEqual(run.saved, [run.response]);
  assert.equal(run.closed(), true);
});
