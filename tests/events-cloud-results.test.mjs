/* tests/events-cloud-results.test.mjs */
/* CrossWatch - Cloud outcome rendering tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/events/index.js", import.meta.url), "utf8");
const helper = source.slice(source.indexOf("const cloudResultsHTML"), source.indexOf("const PAGE_SIZE"));
const context = {esc: value => String(value ?? "").replaceAll("<", "&lt;")};
vm.createContext(context);
vm.runInContext(helper + "globalThis.render = cloudResultsHTML;", context);

test("cloud event details tolerate malformed rows and show bounded-list totals", () => {
  assert.equal(context.render({detail:{cloud_results:{bad:true}}}), "");
  assert.equal(context.render({detail:{}}), "");
  const html = context.render({detail:{cloud_results:[null, {title:"<Movie>",outcome:"failed",reason:"Readable failure"}],
    cloud_outcome_counts:{dated:200},cloud_results_omitted:199}});
  assert.match(html, /Readable failure/);
  assert.match(html, /&lt;Movie>/);
  assert.match(html, /199 additional item details omitted/);
  assert.match(html, /original date restored: 200/);
  assert.doesNotMatch(html, /<details|<p>/);
});


test("cloud read summary is distinct from write receipts", () => {
  const fn = source.slice(source.indexOf("const titleLine ="), source.indexOf("const EVENT_TYPES"));
  const ctx = vm.createContext({detailOf: event => event.detail});
  vm.runInContext(fn + "globalThis.title = titleLine;", ctx);
  const text = ctx.title({event_type:"cloud_history_read", detail:{cloud_history_entries:12}});
  assert.match(text, /12 entries included after merging with server history/);
  assert.doesNotMatch(text, /succeeded|written/);
});

test("Editor labels cloud source only for history, including mixed viewing groups", () => {
  const table = readFileSync(new URL("../assets/js/editor/table.js", import.meta.url), "utf8");
  const fn = table.slice(table.indexOf("  function cloudHistoryLabel"), table.indexOf("  function createRowElement"));
  const ctx = vm.createContext({document:{createElement: () => ({})}});
  vm.runInContext(fn, ctx);
  assert.equal(ctx.cloudHistoryLabel([{raw:{}}], "history"), null);
  assert.equal(ctx.cloudHistoryLabel([{raw:{_plex_cloud_history:true}}], "ratings"), null);
  const label = ctx.cloudHistoryLabel([{raw:{}}, {raw:{_plex_cloud_history:true}}], "history");
  assert.equal(label.textContent, "Source: Plex cloud history");
  assert.match(label.title, /does not confirm presence/);
});
