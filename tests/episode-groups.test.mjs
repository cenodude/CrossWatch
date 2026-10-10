/* tests/episode-groups.test.mjs */
/* CrossWatch - Episode group coordinate entry regressions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {parseEpisodes} from "../assets/js/editor/episode-groups.js";

test("episode entry preserves gaps, specials and explicit season boundaries", () => {
  const rows = parseEpisodes("S00E01, s04e23; S04E25\nS05E01", {tvdb:"79168"}, "Example");
  assert.deepEqual(rows.map(row=>[row.season,row.episode]), [[0,1],[4,23],[4,25],[5,1]]);
  assert.ok(rows.every(row=>row.show_ids.tvdb === "79168" && row.type === "episode"));
  assert.ok(rows.every(row=>!("watched_at" in row)));
});

test("malformed coordinates and oversized groups cannot become a draft", () => {
  for (const value of ["", "S04E0", "4:23", "S04E23-24", "S04E23 bad", Array(21).fill("S04E23").join(",")]) {
    assert.throws(()=>parseEpisodes(value,{tmdb:"100"},"Example"));
  }
});
