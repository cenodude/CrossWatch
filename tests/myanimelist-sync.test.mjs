/* tests/myanimelist-sync.test.mjs */
/* CrossWatch - MyAnimeList provider surfaces and pair options */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import { hasCumulativeAnimeHistory, sourceWatchStatusAllowed, ratingsDisabledForPair, sanitizeFeaturesForPair } from "../assets/js/modals/pair-config/custom-rules.js";

test("MAL exposes supported surfaces and completion scrobbling", () => {
  const context = { window: {} };
  vm.runInNewContext(readFileSync(new URL("../assets/helpers/provider-meta.js", import.meta.url), "utf8"), context);
  const meta = context.window.CW.ProviderMeta;
  assert.equal(meta.label("MYANIMELIST"), "MyAnimeList");
  const source = readFileSync(new URL("../assets/helpers/provider-meta.js", import.meta.url), "utf8");
  const line = source.split("\n").find(row => row.includes("MYANIMELIST:"));
  for (const flag of ["watchlist", "ratings", "history", "scrobblerSink", "syncSurface"]) assert.ok(line.includes(flag + ": true"));
  for (const flag of ["progress", "playlists"]) assert.ok(line.includes(flag + ": false"));
});

test("MAL applies cumulative history and title rating rules in both directions", () => {
  for (const state of [{src:"SIMKL",dst:"MYANIMELIST"}, {src:"MYANIMELIST",dst:"KITSU"}]) {
    assert.equal(hasCumulativeAnimeHistory(state), true);
    assert.equal(sourceWatchStatusAllowed(state), true);
    assert.deepEqual([...ratingsDisabledForPair(state)], ["seasons", "episodes"]);
    assert.equal(sanitizeFeaturesForPair(state, {history:{enable:true,rewatches:true}}).history.rewatches, false);
  }
  assert.equal(sourceWatchStatusAllowed({src:"PLEX", dst:"MYANIMELIST"}), false);
});


test("source watch status is available in every anime tracker direction with help", async () => {
  for (const src of ["SIMKL", "ANILIST", "MYANIMELIST", "KITSU"]) {
    for (const dst of ["SIMKL", "ANILIST", "MYANIMELIST", "KITSU"]) {
      assert.equal(sourceWatchStatusAllowed({src, dst}), true, `${src} to ${dst}`);
      assert.equal(sourceWatchStatusAllowed({src, dst, twoWay: true}), true);
    }
    assert.equal(sourceWatchStatusAllowed({src, dst: "PLEX"}), false);
    assert.equal(sourceWatchStatusAllowed({src: "PLEX", dst: src}), false);
  }
  const {HELP_TEXT} = await import("../assets/js/modals/pair-config/help.js");
  assert.match(HELP_TEXT["cx-hs-source-status"], /Status-only changes/);
  assert.match(HELP_TEXT["cx-hs-source-status"], /SIMKL, AniList, MyAnimeList and Kitsu/);
});
