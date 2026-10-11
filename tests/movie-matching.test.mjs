/* tests/movie-matching.test.mjs */
/* CrossWatch - Movie matching review dialog */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import {describeCandidate} from "../assets/js/analyzer/movie-matching.js";

test("a candidate is described by title, year and its IDs", () => {
  assert.deepEqual(describeCandidate({title:"Insidious", year:2026, ids:{imdb:"tt1", tmdb:"5"}}), ["Insidious · 2026", "IMDB tt1 · TMDB 5"]);
  assert.deepEqual(describeCandidate({title:"", year:null, ids:{}}), []);
});

async function open(diagnosis) {
  const nodes = new Map(), listeners = {}, calls = [], events = [];
  const get = selector => nodes.get(selector) || nodes.set(selector, {textContent:"", innerHTML:"", disabled:false, onclick:null}).get(selector);
  const dialog = {setAttribute() {}, querySelector:get, showModal() {}, remove() {},
    addEventListener:(name, handler) => { listeners[name] = handler; }, close() { listeners.close(); }};
  const context = vm.createContext({AbortController, document:{createElement:() => dialog, body:{append() {}}},
    fetch:async (url, options) => {
      const body = JSON.parse(options.body); calls.push(body);
      return {ok:true, json:async () => body.action === "context" ? {ok:true, version:"v1"} : body.action === "diagnose" ? diagnosis : {ok:true}};
    }});
  vm.runInContext(readFileSync(new URL("../assets/js/analyzer/movie-matching.js", import.meta.url), "utf8").replaceAll("export ", ""), context);
  await context.openMovieMatching(null, {title:"Insidious", reference:{pair_id:"p1", provider:"SIMKL", feature:"history", key:"imdb:tt1"},
    onSaved:() => events.push("saved"), onManual:() => events.push("manual")});
  return {get, calls, events};
}

test("a proposed match can be saved in one click", async () => {
  const {get, calls, events} = await open({ok:true, status:"twin", destination:"SIMKL", checked:["SIMKL items already synced"],
    message:"SIMKL already has this film.", candidates:[], candidate:{title:"Insidious", year:2026, ids:{tmdb:"5"}}});
  assert.match(get("[data-review]").innerHTML, /Proposed match on SIMKL/);
  assert.equal(get("[data-save]").disabled, false);
  await get("[data-save]").onclick();
  assert.equal(JSON.stringify(calls[2]), JSON.stringify({pair_id:"p1", provider:"SIMKL", feature:"history", key:"imdb:tt1", version:"v1",
    action:"save", item:{type:"movie", title:"Insidious", year:2026, ids:{tmdb:"5"}}}));
  assert.deepEqual(events, ["saved"]);
});

test("without a candidate nothing can be saved and manual search is offered", async () => {
  const {get, events} = await open({ok:true, status:"not_found", destination:"SIMKL", checked:["SIMKL catalogue by ID"],
    message:"No film with this title and year was found.", candidates:[], candidate:null});
  assert.equal(get("[data-save]").disabled, true);
  assert.match(get("[data-review]").innerHTML, /No film with this title/);
  get("[data-manual]").onclick();
  assert.deepEqual(events, ["manual"]);
});
