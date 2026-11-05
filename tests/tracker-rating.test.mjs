/* tests/tracker-rating.test.mjs */
/* CrossWatch - Star rating previews and community metadata */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";
const source = readFileSync(new URL("../assets/js/tracker.js", import.meta.url), "utf8");
const code = source.slice(source.indexOf("  function ratingLabel("), source.indexOf("  function renderLog("));

test("hover and keyboard preview stars without saving a rating", () => {
  const events = {}, value = {}, label = {};
  let focused;
  const buttons = Array.from({length:20}, (_, i) => ({dataset:{trkRate:String((i+1)/2)}, classList:{toggle(_, on){this.lit=on;}}, focus(){focused=i;}, closest(){return this;}}));
  const group = {querySelectorAll:()=>buttons, addEventListener:(name, fn)=>{events[name]=fn;}, contains:node=>buttons.includes(node)};
  const dialog = {querySelector:selector=>selector === ".trk-rates" ? group : selector === "[data-trk-rate-label]" ? label : value};
  const state = {title:{ratings:[{rating:4}]}};
  const context = vm.createContext({state, dialog});
  vm.runInContext(code + "wireRatingPreview(dialog);", context);
  events.pointerover({target:buttons[6]});
  assert.equal(value.textContent,"3.5/10");
  assert.equal(label.textContent,"Mostly regret");
  assert.equal(buttons.filter(b=>b.classList.lit).length,7);
  assert.equal(state.title.ratings[0].rating,4);
  events.pointerleave();
  assert.equal(value.textContent,"4/10");
  assert.equal(label.textContent,"Background noise");
  events.focusin({target:buttons[8]});
  assert.equal(value.textContent,"4.5/10");
  assert.equal(label.textContent,"Meh");
  events.keydown({target:buttons[8],key:"ArrowRight",preventDefault(){}});
  assert.equal(focused,9);
  events.focusout({relatedTarget:null});
  assert.equal(value.textContent,"4/10");
});

test("TMDB summary shows actual votes and ignores stale dialog requests", async () => {
  let resolve;
  const node = {};
  const sheet = {mode:"rate",token:1,el:{querySelector:()=>node}};
  const context = vm.createContext({sheet,state:{title:{title:{ids:{tmdb:"1"}}}},num:String,esc:String,getJSON:()=>new Promise(done=>{resolve=done;})});
  vm.runInContext(code,context);
  const pending = vm.runInContext("loadCommunityRating(1)",context);
  resolve({result:{vote_count:120,detail:{vote_average:7.25}}});
  await pending;
  assert.match(node.innerHTML, /--rating-progress:72.5%/);
  assert.match(node.innerHTML, /120 votes/);
  assert.match(node.innerHTML, /Average rating 7.3 out of 10/);
  const stale = vm.runInContext("loadCommunityRating(1)",context);
  sheet.token=2;
  resolve({result:{vote_count:0}});
  await stale;
  assert.match(node.innerHTML, /--rating-progress:72.5%/);
  assert.match(node.innerHTML, /120 votes/);
  assert.match(node.innerHTML, /Average rating 7.3 out of 10/);
});
