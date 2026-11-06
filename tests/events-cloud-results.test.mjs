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


test("failure detail sections escape evidence and keep candidates collapsed and bounded", () => {
  const code = source.slice(source.indexOf("const failureDetailsHTML"), source.indexOf("const PAGE_SIZE"));
  const ctx = vm.createContext({});
  vm.runInContext(source.slice(source.indexOf("const esc ="), source.indexOf("function withEventScope")), ctx);
  vm.runInContext(code + "globalThis.render = failureDetailsHTML;", ctx);
  assert.equal(ctx.render({}), "");
  assert.equal(ctx.render({event_type:"write_succeeded", reason_message:"Success"}), "");
  assert.equal(ctx.render({match_details:{candidates:[null]}}), "");
  const html = ctx.render({event_type:"write_failed", reason_message:"<Failure>", hint_message:"Specific hint", match_details:{candidate_count:12, truncated:true,
    candidates:Array.from({length:12}, () => ({title:"<Candidate>", ids:{tmdb:"2"}, show_ids:{tvdb:"3"}, season:1, episode:2}))}});
  assert.match(html, /What happened/);
  assert.match(html, /Specific hint/);
  assert.match(html, /&lt;Failure&gt;/);
  assert.match(html, /&lt;Candidate&gt;/);
  assert.doesNotMatch(html, /<Failure>|<Candidate>|<details[^>]* open/);
  assert.match(html, /Possible matches \(12\)/);
  assert.match(html, /show tvdb:3/);
  assert.match(html, /S1E2/);
  assert.match(html, /Only the first candidates/);
  assert.equal((html.match(/&lt;Candidate&gt;/g) || []).length, 10);
});


test("grouped failure pairs show diagnostics once while raw events retain them", () => {
  const ctx = vm.createContext({});
  vm.runInContext(source.slice(source.indexOf("const esc ="), source.indexOf("function withEventScope")), ctx);
  vm.runInContext(source.slice(source.indexOf("const failureDetailsHTML"), source.indexOf("const PAGE_SIZE")) + "globalThis.render = failureDetailsHTML;", ctx);
  const failed = {event_type:"write_failed", run_id:"run1", item_key:"tmdb:1", destination_instance:"P01", reason_code:"ambiguous", reason_message:'A < B & "C"'};
  const unresolved = {...failed, event_type:"unresolved_recorded"};
  const events = [failed, unresolved];
  assert.equal((events.map(event => ctx.render(event, events)).join("").match(/What happened/g) || []).length, 1);
  assert.match(ctx.render(unresolved), /A &lt; B &amp; &quot;C&quot;/);
  for (const change of [{run_id:"run2"}, {destination_instance:"P02"}, {item_key:"tmdb:2"}, {hint_message:"Different evidence"}]) {
    assert.match(ctx.render({...unresolved, ...change}, events), /What happened/);
  }
});
