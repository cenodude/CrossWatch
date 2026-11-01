/* tests/pair-delete-busy.test.mjs */
/* CrossWatch - Pair deletion conflict feedback regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";

const source = path => readFileSync(new URL(path, import.meta.url), "utf8");
const busy = {ok: false, status: 409, statusText: "Conflict", json: async () => ({detail: "database_operation_busy"})};

test("pair API deletion explains the conflict without reporting success", async () => {
  const script = source("../assets/helpers/api.js");
  const messages = [];
  const ctx = {f: async () => busy, window: {alert: message => messages.push(message)}};
  vm.runInNewContext(script.slice(script.indexOf("  async function j("), script.indexOf("  const _memo")), ctx);
  await assert.rejects(ctx.j("/api/pairs/one", {method: "DELETE"}), /409/);
  assert.equal(messages.length, 1);
  assert.match(messages[0], /sync or database maintenance task is running/);
});

test("core deletion fallback explains conflict and preserves the pair", async () => {
  const script = source("../assets/helpers/core.js");
  const messages = [];
  const ctx = {API: {}, fetch: async () => busy, encodeURIComponent, console: {warn() {}},
    window: {alert: message => messages.push(message)},
    _invalidatePairsCache: () => assert.fail("must not invalidate on rejected delete"), loadPairs: () => assert.fail("must not reload")};
  vm.runInNewContext(script.slice(script.indexOf("  async function deletePair("), script.indexOf("  async function cxSavePair(")), ctx);
  assert.equal(await ctx.deletePair("one"), false);
  assert.match(messages[0], /Wait for it to finish/);
});

for (const outcome of ["busy", "server error", "network error", "success"]) {
test(`pair card removal waits for successful deletion: ${outcome}`, async () => {
  const script = source("../assets/js/connections.pairs.overlay.js");
  const messages = [], timers = [], classes = new Set();
  let removed = false, resolveRequest, rejectRequest;
  const request = new Promise((resolve, reject) => { resolveRequest = resolve; rejectRequest = reject; });
  const card = {remove: () => { removed = true; },
    classList: {add: name => classes.add(name), remove: name => classes.delete(name)}};
  const ctx = {window: {cx: {pairs: [{id: "one"}]}, alert: message => messages.push(message)},
    document: {querySelector: () => ({querySelector: () => card})}, isPlaylistManagedPair: () => false,
    fetch: () => request, setTimeout: callback => timers.push(callback), refreshBadges() {}, console: {warn() {}}};
  vm.runInNewContext(script.slice(script.indexOf("  async function deletePairCard("), script.indexOf("  window.deletePairCard")), ctx);
  const deletion = ctx.deletePairCard("one");
  assert.equal(timers.length, 0);
  assert.equal(classes.has("removing"), false);
  assert.equal(ctx.window.cx.pairs.length, 1);
  if (outcome === "network error") rejectRequest(new Error("Network failed"));
  else resolveRequest(outcome === "busy" ? busy : {
    ok: outcome === "success", status: outcome === "success" ? 200 : 500,
    json: async () => ({ok: outcome === "success", deleted: outcome === "success" ? 1 : 0})
  });
  await deletion;
  if (outcome === "busy") assert.match(messages[0], /sync or database maintenance task is running/);
  assert.equal(removed, false);
  for (const callback of timers) callback();
  assert.equal(removed, outcome === "success");
  assert.equal(classes.has("removing"), outcome === "success");
  assert.equal(ctx.window.cx.pairs.length, outcome === "success" ? 0 : 1);
});
}
