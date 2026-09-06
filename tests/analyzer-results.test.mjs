/* tests/analyzer-results.test.mjs */
/* CrossWatch - Analyzer Large Result Filtering Tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";

globalThis.window = {};
globalThis.document = { addEventListener() {} };
const { createResultFilter } = await import("../assets/js/analyzer/index.js");
const features = ["history", "watchlist", "ratings", "progress", "collection"];
const rows = Object.freeze(Array.from({ length: 5000 }, (_, n) => Object.freeze({
  title: `Movie ${String(n).padStart(5, "0")}`, provider: "SIMKL", feature: features[n % 5],
  key: `tmdb:${100000 + n}`, ids: { tmdb: String(100000 + n) },
  severity: ["info", "warn", "error"][n % 3], reason: "Provider could not match the item."
})));

test("all 5,000 issues remain reachable across pages without duplicates", () => {
  const filtered = createResultFilter()(rows);
  const visited = [];
  for (let offset = 0; offset < filtered.length; offset += 50) visited.push(...filtered.slice(offset, offset + 50));
  assert.equal(visited.length, 5000);
  assert.equal(new Set(visited.map(row => row.key)).size, 5000);
  assert.equal(visited.at(-1).key, "tmdb:104999");
});

test("search and feature filters cover every page of 5,000 issues", () => {
  const filter = createResultFilter();
  assert.deepEqual(filter(rows, { query: "SIMKL tmdb:104999" }), [rows[4999]]);
  for (const feature of features) assert.equal(filter(rows, { feature }).length, 1000);
  assert.equal(filter(rows, { query: "could not match" }).length, 5000);
  assert.equal(filter(rows, { query: "unknown item" }).length, 0);
});

test("sorting is global and preserves the original result ordering", () => {
  const filtered = createResultFilter()(rows, { sort: "title-desc" });
  assert.equal(filtered[0].key, "tmdb:104999");
  assert.equal(filtered[4999].key, "tmdb:100000");
  assert.equal(rows[0].key, "tmdb:100000");
});

test("5,000 diagnostics prioritize errors and filter by severity", () => {
  const filter = createResultFilter();
  const sorted = filter(rows, { diagnostics: true });
  assert.equal(sorted[0].severity, "error");
  assert.equal(sorted.at(-1).severity, "info");
  const errors = filter(rows, { level: "error", diagnostics: true });
  assert.equal(errors.length, 1666);
  assert(errors.every(row => row.severity === "error"));
});

test("repeated filtering reuses searchable labels for unchanged rows", () => {
  let labels = 0;
  const filter = createResultFilter(provider => { labels++; return `${provider} Family`; });
  filter(rows);
  const prepared = labels;
  assert.equal(prepared, 5000);
  filter(rows, { query: "Family", sort: "title-desc" });
  filter(rows, { feature: "ratings" });
  assert.equal(labels, prepared);
});

test("pending episode retries are searchable by title, IDs and episode", () => {
  const row = { provider: "SIMKL", feature: "history", item: { type: "episode", series_title: "The Series", season: 1, episode: 23, ids: { tvdb: "123" } } };
  assert.deepEqual(createResultFilter()([row], { query: "series S01E23 tvdb:123" }), [row]);
});
