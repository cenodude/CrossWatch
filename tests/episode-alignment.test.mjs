/* tests/episode-alignment.test.mjs */
/* CrossWatch - Episode numbering review selection and rendering */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import {selection} from "../assets/js/editor/episode-alignment.js";

const side = (key, episode, title = "") => ({key, season:6, episode, title});
const proposal = () => ({metadata:"used", source:{provider:"JELLYFIN", instance:"default"}, target:{provider:"TRAKT", instance:"default"},
  rows:[{source:side("s15", 15, "Could Have Been (1)"), target:side("t15", 15, "Could Have Been"), status:"same"},
    {source:side("s16", 16, "Could Have Been (2)"), target:null, status:"unmatched"},
    {source:side("s17", 17, "Unagi"), target:side("t16", 16, "Unagi"), status:"shifted"},
    {source:side("s18", 18, "Student"), target:side("t17", 17, "Student"), status:"shifted"}],
  corrections:[{key:"s17", season:6, episode:16}, {key:"s18", season:6, episode:17}],
  groups:[{name:"Friends S06E15", season:6, orphans:["s16"], split:"source", suggested:"previous", reason:"TMDb lists the combined episode at about double length.",
    options:{previous:{source:["s15", "s16"], target:["t15"]}, next:{source:["s16", "s17"], target:["t16"]}}}]});

test("the suggested group and every shifted episode are selected by default", () => {
  const chosen = selection(proposal());
  assert.deepEqual(chosen.groups, [{name:"Friends S06E15", source:["s15", "s16"], target:["t15"]}]);
  assert.deepEqual(chosen.corrections.map(item => item.key), ["s17", "s18"]);
});

test("choosing another group drops the corrections it covers, and declining keeps them", () => {
  assert.deepEqual(selection(proposal(), {0:"next"}).corrections.map(item => item.key), ["s18"]);
  const declined = selection(proposal(), {0:"none"}, new Set(["s18"]));
  assert.deepEqual(declined.groups, []);
  assert.deepEqual(declined.corrections.map(item => item.key), ["s17"]);
});

test("a group without a suggestion is not selected until the user picks one", () => {
  const value = proposal();
  value.groups[0].suggested = null;
  assert.deepEqual(selection(value).groups, []);
  assert.equal(selection(value, {0:"previous"}).groups.length, 1);
});

test("the review renders the question, saves the confirmed fixes and refreshes its opener", async () => {
  const nodes = new Map(), listeners = {}, calls = [];
  const node = () => ({textContent:"", innerHTML:"", disabled:false, onclick:null});
  const get = selector => nodes.get(selector) || nodes.set(selector, node()).get(selector);
  const dialog = {setAttribute() {}, querySelector:get, querySelectorAll:() => [get("[data-close]")], showModal() {}, remove() {},
    addEventListener:(name, handler) => { listeners[name] = handler; }, close() { listeners.close(); }};
  let saved = 0;
  const context = vm.createContext({AbortController, window:{}, document:{createElement:() => dialog, body:{append() {}}},
    fetch:async (url, options) => { calls.push([url, JSON.parse(options.body)]); return {ok:true, json:async () => url.endsWith("/apply") ? {ok:true} : proposal()}; }});
  vm.runInContext(readFileSync(new URL("../assets/js/editor/episode-alignment.js", import.meta.url), "utf8").replaceAll("export ", ""), context);
  await context.openEpisodeAlignment(null, {pairId:"p1", showIds:{tmdb:"1668"}, onSaved:() => saved++});
  const html = get("[data-review]").innerHTML;
  assert.match(html, /JELLYFIN S06E16 has no match on TRAKT/);
  assert.match(html, /Combine: JELLYFIN S06E15 \+ S06E16 ↔ TRAKT S06E15/);
  assert.match(html, /JELLYFIN S06E17 → TRAKT S06E16/);
  assert.equal(get("[data-save]").textContent, "Save 3 fixes");
  listeners.change({target:{dataset:{group:"0"}, value:"next"}});
  assert.match(get("[data-review]").innerHTML, /Handled by the episode group above/);
  assert.equal(get("[data-save]").textContent, "Save 2 fixes");
  await get("[data-save]").onclick();
  assert.equal(JSON.stringify(calls[1]), JSON.stringify(["/api/editor/episode-alignment/apply", {pair_id:"p1", show_ids:{tmdb:"1668"},
    groups:[{name:"Friends S06E15", source:["s16", "s17"], target:["t16"]}], corrections:[{key:"s18", season:6, episode:17}]}]));
  assert.equal(saved, 1);
});
