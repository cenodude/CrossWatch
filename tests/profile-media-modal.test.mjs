/* tests/profile-media-modal.test.mjs */
/* CrossWatch - Profile media modal watch status tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/profile-media-modal.js", import.meta.url), "utf8");

function modal(presence = null, type = "movie") {
  const context = {
    window: { CW: { ProfileDateTime: { format: (date) => date.toISOString().slice(0, 10) } } },
    navigator: { language: "en-US" },
    document: { documentElement: { dataset: {} } },
  };
  vm.runInNewContext(source.replace("  (window.CW ||= {}).ProfileMediaModal =", "  window.testModal = { state, movieProgressBox, overviewTab, episodesTab, seasonProgress, upNext, episodeSlot, livesCard };\n  (window.CW ||= {}).ProfileMediaModal ="), context);
  const api = context.window.testModal;
  Object.assign(api.state, { item: { type, tmdb: "123" }, presence, progress: [] });
  return api;
}

const epoch = Date.parse("2026-01-02T12:00:00Z") / 1000;
const entry = (count, last_epoch = epoch, episodes = []) => ({ count, last_epoch, episodes, present: [{ provider: "TRAKT" }], missing: [] });

test("a scrobbled movie is watched without synced history", () => {
  const api = modal({ synced: null, scrobble: entry(1) });
  const html = api.movieProgressBox();
  assert.match(html, /<b>Watched<\/b>/);
  assert.match(html, /Watched once/);
  assert.match(html, /width:100%/);
  assert.match(html, /2026-01-02/);
  assert.doesNotMatch(html, /Not watched yet/);
  assert.match(api.livesCard(), /History<\/strong><span>Not synced/);
});

test("movie counts do not add overlapping sources and use the latest watch date", () => {
  const api = modal({ synced: entry(2, epoch - 86400), scrobble: entry(2) });
  assert.match(api.movieProgressBox(), /Watched 2 times/);
  assert.match(api.movieProgressBox(), /2026-01-02/);
  api.state.presence.scrobble.count = 3;
  assert.match(api.movieProgressBox(), /Watched 3 times/);
});

test("history-only, unwatched and absent presence retain their watch status", () => {
  assert.match(modal({ synced: entry(1), scrobble: null }).movieProgressBox(), /Watched once/);
  for (const presence of [null, {}, { synced: null, scrobble: null }]) {
    const html = modal(presence).movieProgressBox();
    assert.match(html, /Not watched yet/);
    assert.match(html, /Not started/);
    assert.match(html, /width:0%/);
  }
});

test("a current rewatch keeps playback progress alongside the completed scrobble", () => {
  const api = modal({ scrobble: entry(1) });
  api.state.progress = [{ provider: "TRAKT", instance: "default", pct: 35 }];
  assert.match(api.movieProgressBox(), /<b>35%<\/b>/);
  assert.match(api.movieProgressBox(), /Watched once/);
});

function show(presence) {
  const api = modal(presence, "show");
  api.state.meta = { detail: { number_of_episodes: 3, seasons: [{ season: 1, episode_count: 3 }] } };
  api.state.seasons.set("1", { episodes: [1, 2, 3].map((episode) => ({ episode, name: "Episode " + episode })) });
  return api;
}

test("scrobbled episodes drive totals, next episode and watched badges", () => {
  const api = show({ scrobble: entry(1, epoch, [{ season: 1, episode: 1, epoch }]) });
  assert.match(api.overviewTab(), /1\/3 episodes <b>33%/);
  assert.equal(api.upNext().label, "Continue watching");
  assert.equal(api.upNext().first.episode, 2);
  assert.match(api.seasonProgress(), /1\/3/);
  assert.equal((api.episodesTab().match(/cw-mm-eprow is-watched/g) || []).length, 1);
  assert.match(api.episodeSlot("Last watched", { season: 1, episode: 1 }), /Watched/);
});

test("episodes shared by history and scrobbles count once and retain the latest date", () => {
  const api = show({
    synced: entry(1, epoch - 86400, [{ season: 1, episode: 1, epoch: epoch - 86400 }]),
    scrobble: entry(2, epoch, [{ season: 1, episode: 1, epoch }, { season: 1, episode: 2, epoch }]),
  });
  assert.match(api.overviewTab(), /2\/3 episodes <b>67%/);
  assert.match(api.seasonProgress(), /2\/3/);
  assert.equal(api.upNext().first.episode, 3);
  const html = api.episodesTab();
  assert.equal((html.match(/cw-mm-eprow is-watched/g) || []).length, 2);
  assert.match(html, /title="2026-01-02"/);
  assert.doesNotMatch(html, /title="2026-01-01"/);
});

test("unwatched shows start at the first episode and history-only shows still advance", () => {
  const empty = show(null);
  assert.equal(empty.upNext().label, "Start watching");
  assert.match(empty.overviewTab(), /0\/3 episodes/);
  const api = show({ synced: entry(1, epoch, [{ season: 1, episode: 1, epoch }]) });
  assert.equal(api.upNext().first.episode, 2);
  assert.match(api.overviewTab(), /1\/3 episodes/);
});
