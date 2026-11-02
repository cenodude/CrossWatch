/* tests/pair-history-remove.test.mjs */
/* CrossWatch - History Remove rules for import-only pair sources */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from "node:test";
import assert from "node:assert/strict";
import { historyRemoveAllowed, sanitizeFeaturesForPair } from "../assets/js/modals/pair-config/custom-rules.js";

test("Tautulli and Tracearr sources cannot keep History Remove", () => {
  for (const src of ["TAUTULLI", "TRACEARR"]) {
    const state = { src, dst: "FLOPPY" };
    assert.equal(historyRemoveAllowed(state), false);
    assert.equal(sanitizeFeaturesForPair(state, { history: { enable: true, add: true, remove: true } }).history.remove, false);
  }
});

test("other sources keep History Remove", () => {
  const state = { src: "PLEX", dst: "FLOPPY" };
  assert.equal(historyRemoveAllowed(state), true);
  assert.equal(sanitizeFeaturesForPair(state, { history: { enable: true, add: true, remove: true } }).history.remove, true);
});
