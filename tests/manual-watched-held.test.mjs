/* tests/manual-watched-held.test.mjs */
/* CrossWatch - Quick add notes for titles an account already holds */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/modals/manual-watched/index.js", import.meta.url), "utf8");
const helper = source.slice(source.indexOf("const providerKey ="), source.indexOf("\n", source.indexOf("const providerKey =")));
const notes = source.slice(source.indexOf("const HELD_SOURCES"), source.indexOf("export default"));
const context = {};
vm.runInNewContext(`${helper}\n${notes}\nglobalThis.heldNotes = heldNotes;\nglobalThis.watchedTargets = watchedTargets;\nglobalThis.watchedEpisodes = watchedEpisodes;\nglobalThis.historyFits = historyFits;`, context);
const heldNotes = (...args) => [...context.heldNotes(...args)];
const watchedTargets = (...args) => [...context.watchedTargets(...args)];

const trakt = { provider: "TRAKT", instance: "default", display: "Trakt", rewatch_write: true };
const simkl = { provider: "SIMKL", instance: "default", display: "SIMKL" };
const second = { provider: "TRAKT", instance: "second", display: "Trakt (second)" };
const held = {
  synced: { present: [{ provider: "TRAKT", instance: "default" }], episodes: [{ season: 1, episode: 2, present: [{ provider: "TRAKT", instance: "default" }] }] },
  ratings: { present: [{ provider: "TRAKT", instance: "default", rating: 7 }] },
  watchlist: { present: [{ provider: "SIMKL", instance: "default" }] },
  collection: null,
};

test("a watched movie says a rewatch is added on the selected account", () => {
  assert.deepEqual(heldNotes(held, ["history"], [trakt]), ["Already watched on Trakt. Sending adds a rewatch."]);
});

test("a provider that keeps one play is named apart and skipped", () => {
  const plex = { provider: "TRAKT", instance: "default", display: "Plex", rewatch_write: false };
  assert.deepEqual(heldNotes(held, ["history"], [plex]), ["Already watched on Plex, which keeps one play. History is skipped there."]);
});

test("picked episodes only count when that episode is watched", () => {
  assert.deepEqual(heldNotes(held, ["history"], [trakt], { show: true, picked: [{ season: 1, episode: 2 }] }), ["Already watched on Trakt. Sending adds a rewatch."]);
  assert.deepEqual(heldNotes(held, ["history"], [trakt], { show: true, picked: [{ season: 1, episode: 3 }] }), []);
  assert.deepEqual(watchedTargets(held, [trakt], [{ season: 1, episode: 3 }]), []);
  assert.equal(watchedTargets(held, [trakt], [{ season: 1, episode: 2 }]).length, 1);
});

test("only selected accounts and picked actions are named", () => {
  assert.deepEqual(heldNotes(held, ["history"], [simkl]), []);
  assert.deepEqual(heldNotes(held, ["history"], [second]), []);
  assert.deepEqual(heldNotes(held, ["collection"], [trakt, simkl]), []);
  assert.deepEqual(heldNotes(held, ["watchlist"], [trakt, simkl]), ["Already in your watchlist on SIMKL."]);
});

test("a rating shows the stored score", () => {
  assert.deepEqual(heldNotes(held, ["rating"], [trakt]), ["Already rated on Trakt (7/10). Sending may replace the score."]);
});

test("a show says episodes were watched, not that a new add is a repeat", () => {
  assert.deepEqual(heldNotes(held, ["history"], [trakt], { show: true }), ["Has watched episodes on Trakt."]);
});

test("only the watched episodes of a pick are marked per account", () => {
  const rows = context.watchedEpisodes(held, trakt, [{ season: 1, episode: 2 }, { season: 1, episode: 3 }]);
  assert.deepEqual(JSON.parse(JSON.stringify(rows)), [{ season: 1, episode: 2 }]);
  assert.equal(context.watchedEpisodes(held, simkl, [{ season: 1, episode: 2 }]).length, 0);
});

test("one-play providers say the watched episodes are skipped, not all history", () => {
  const plex = { provider: "TRAKT", instance: "default", display: "Plex", rewatch_write: false };
  assert.deepEqual(heldNotes(held, ["history"], [plex], { show: true, picked: [{ season: 1, episode: 2 }] }), ["Already watched on Plex, which keeps one play. Those episodes are skipped there."]);
});

test("whole show and picked episodes each follow their own history rule", () => {
  const floppy = { history_whole_show: false, history_episodes: true };
  assert.equal(context.historyFits(floppy, true, false), false);
  assert.equal(context.historyFits(floppy, true, true), true);
  assert.equal(context.historyFits(floppy, false, false), true);
  assert.equal(context.historyFits({}, true, false), true);
});

test("no stored data means no note", () => {
  assert.deepEqual(heldNotes(null, ["history", "rating"], [trakt]), []);
  assert.deepEqual(heldNotes({}, ["history"], [trakt]), []);
});

test("several actions give one note each", () => {
  assert.equal(heldNotes(held, ["history", "rating"], [trakt]).length, 2);
});
