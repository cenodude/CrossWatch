/* tests/item-actions.test.mjs */
/* CrossWatch - provider icon action request and result tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/item-actions.js", import.meta.url), "utf8");

function load(responses, dataset = {}) {
  const calls = [];
  const element = () => ({ setAttribute() {}, addEventListener() {}, appendChild() {}, querySelector: () => null, querySelectorAll: () => [], classList: { add() {}, remove() {} }, dataset: {}, style: {} });
  const context = {
    window: { CW: {}, setTimeout, clearTimeout },
    navigator: { language: "en-US" },
    document: {
      documentElement: { dataset, classList: { add() {}, remove() {} } },
      head: { appendChild() {} },
      body: { appendChild() {} },
      getElementById: () => null,
      createElement: element,
      addEventListener() {},
    },
    AbortController,
    fetch: async (url, init = {}) => {
      const body = init.body ? JSON.parse(init.body) : undefined;
      calls.push({ url, body });
      const next = responses.shift();
      const data = typeof next === "function" ? next(url, body) : next;
      return { ok: data?.status ? data.status < 400 : true, status: data?.status || 200, json: async () => data?.body ?? data };
    },
  };
  context.window.document = context.document;
  vm.runInNewContext(source.replace("  (window.CW ||= {}).ItemActions =", "  window.testActions = { identity, sendAdd, editorRemove, watchlistRemove, progressTask, watchlistKeys };\n  (window.CW ||= {}).ItemActions ="), context);
  return { api: context.window.testActions, actions: context.window.CW.ItemActions, calls };
}

test("identity keeps typed ids and drops nested objects", () => {
  const { api } = load([]);
  const out = api.identity({ key: "x", type: "tv", title: "Show", year: 2026, tmdb: "12", ids: { imdb: "tt1", show_ids: { tmdb: 9 }, bad: { a: 1 } }, present: [] });
  assert.deepEqual(JSON.parse(JSON.stringify(out)), { type: "show", title: "Show", year: 2026, ids: { imdb: "tt1", tmdb: "12" }, show_ids: { tmdb: 9 } });
});

test("adds report confirmations, skips and single-play providers", async () => {
  const { api, calls } = load([
    { results: [{ ok: true, result: { confirmed: 1 } }] },
    { results: [{ ok: false, error: "rewatch_not_supported" }] },
    { results: [{ ok: true, result: { confirmed: 0, unresolved: 1 } }] },
    { results: [{ ok: true, result: { confirmed: 0, skipped: 1 } }] },
  ]);
  const ref = { provider: "TRAKT", instance: "default" };
  assert.equal((await api.sendAdd("history", ref, { type: "movie" }, "Trakt", true)).ok, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body)), { kind: "history", items: [{ type: "movie" }], providers: [ref], rewatch: true });
  const single = await api.sendAdd("history", ref, {}, "Plex", true);
  assert.equal(single.ok, false);
  assert.equal(single.skipped, true);
  assert.match(single.error, /keeps one play/);
  assert.match((await api.sendAdd("watchlist", ref, {}, "Trakt")).error, /could not match/);
  assert.equal((await api.sendAdd("watchlist", ref, {}, "Trakt")).note, "already there");
});

test("tracker removals go direct by id without a preview", async () => {
  const { api, calls } = load([
    { results: [{ ok: true, result: { removed: 1 } }] },
    { results: [{ ok: false, result: { unresolved: 1 } }] },
  ]);
  const ref = { provider: "SIMKL", instance: "default" };
  assert.equal((await api.editorRemove("ratings", ref, { type: "movie", ids: { tmdb: "1" } }, "SIMKL")).ok, true);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "/api/editor/send");
  assert.equal(calls[0].body.direct, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body.providers)), [ref]);
  assert.match((await api.editorRemove("ratings", ref, { type: "movie", ids: { tmdb: "1" } }, "SIMKL")).error, /did not find/);
});

test("media servers fall back to the preview and live match", async () => {
  const { api, calls } = load([
    { results: [{ ok: false, error: "needs_live_match", result: {} }] },
    { providers: [{ provider: "PLEX", instance: "default" }], preview_id: "abc" },
    { results: [{ ok: true, result: { removed: 1 } }] },
  ]);
  const ref = { provider: "PLEX", instance: "default" };
  assert.equal((await api.editorRemove("history", ref, { type: "movie", ids: { tmdb: "1" } }, "Plex")).ok, true);
  assert.deepEqual(calls.map((call) => call.url), ["/api/editor/send", "/api/editor/send/preview", "/api/editor/send"]);
  assert.equal(calls[2].body.preview_id, "abc");
  assert.equal(calls[2].body.direct, undefined);
});

test("watchlist removal sends the key and aliases and explains a missing record", async () => {
  const { api, calls } = load([
    { deleted_ok: 1 },
    { deleted_ok: 0, results: [{ per_key: [{ reason: "not_in_state" }] }] },
  ]);
  const ref = { provider: "PLEX", instance: "home" };
  assert.equal((await api.watchlistRemove(ref, { key: "tmdb:1", aliases: ["imdb:tt1"] }, "Plex")).ok, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body)), { provider: "PLEX", provider_instance: "home", keys: [{ key: "tmdb:1", aliases: ["imdb:tt1"] }] });
  assert.match((await api.watchlistRemove(ref, { key: "tmdb:1" }, "Plex")).error, /no watchlist record/);
});

test("watchlist removal without a stored key falls back to id keys", async () => {
  const { api, calls } = load([{ deleted_ok: 1 }]);
  assert.deepEqual(JSON.parse(JSON.stringify(api.watchlistKeys({ type: "movie", ids: { tmdb: "11", imdb: "tt1" } }))), ["imdb:tt1", "tmdb:11"]);
  const result = await api.watchlistRemove({ provider: "TRAKT", instance: "default" }, { type: "movie", ids: { tmdb: "11", imdb: "tt1" } }, "Trakt");
  assert.equal(result.ok, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body.keys)), [{ key: "imdb:tt1", aliases: ["tmdb:11"] }]);
});

test("watchlist fallback keys keep shows apart from movies with the same TMDB id", () => {
  const { api } = load([]);
  assert.deepEqual(JSON.parse(JSON.stringify(api.watchlistKeys({ type: "show", ids: { tmdb: "11", tvdb: "5" } }))), ["tmdb:11#show", "tvdb:5"]);
  assert.deepEqual(JSON.parse(JSON.stringify(api.watchlistKeys({ type: "movie", ids: { tmdb: "11" } }))), ["tmdb:11"]);
});

test("undated plays are skipped on providers that need a date", async () => {
  const { api } = load([{ results: [{ ok: false, error: "undated_not_supported" }] }]);
  const result = await api.sendAdd("history", { provider: "PLEX", instance: "default" }, { watched: true, _cw_watched_state: true }, "Plex");
  assert.equal(result.ok, false);
  assert.equal(result.skipped, true);
  assert.match(result.error, /needs a watch date/);
});

test("resume point actions post the record and report the provider message", async () => {
  const record = { provider: "PLEX", instance_id: "home", remote_id: "42", canonical_key: "tmdb:1", can_mark_watched: true };
  const { api, calls } = load([
    { ok: true },
    { status: 400, body: { ok: false, message: "Plex refused the change" } },
  ]);
  const watched = api.progressTask("mark_watched", { provider: "PLEX", instance: "home" }, record, "Plex");
  assert.equal((await watched.exec()).ok, true);
  assert.equal(calls[0].url, "/api/playback_progress/actions/mark_watched");
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body)), { provider: "PLEX", instance_id: "home", remote_id: "42", canonical_key: "tmdb:1", record });
  const removed = api.progressTask("remove", { provider: "PLEX", instance: "home" }, record, "Plex");
  await assert.rejects(removed.exec(), /Plex refused the change/);
  assert.equal(calls[1].url, "/api/playback_progress/actions/remove");
});

test("write access follows the role and view-as mode", () => {
  assert.equal(load([], { cwRole: "user" }).actions.canWrite(), false);
  assert.equal(load([], { cwRole: "user", cwPermWrite: "on" }).actions.canWrite(), true);
  assert.equal(load([], {}).actions.canWrite(), true);
});
