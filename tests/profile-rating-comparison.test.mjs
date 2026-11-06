/* tests/profile-rating-comparison.test.mjs */
/* CrossWatch - Provider-adjusted rating coverage labels */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/profile-page.js", import.meta.url), "utf8");
const context = vm.createContext({esc: String, visibleProviderLabel: String, providerLogo: () => "", numberFmt: new Intl.NumberFormat("en-US")});
vm.runInContext(source.slice(source.indexOf("  function timelineEndpoints("), source.indexOf("  function timelineMethod(")), context);

test("expected rounding keeps scores visible without red conflict styling", () => {
  const html = context.timelineCoverage({rating:7.5,agree:true,rating_adjusted:true,
    present:[{provider:"CROSSWATCH",rating:7.5},{provider:"SIMKL",rating:8}],missing:[]},true,true);
  assert.match(html,/Equivalent scores/);
  assert.match(html,/On every provider/);
  assert.match(html,/is-full/);
  assert.match(html,/Scores match after provider rounding or reaction conversion/);
  assert.match(html,/>7.5<\/b>/);
  assert.match(html,/>8<\/b>/);
  assert.doesNotMatch(html,/Scores differ|is-conflict|is-differs/);
});

test("genuine differences retain their warning", () => {
  const html = context.timelineCoverage({rating:7.5,agree:false,rating_adjusted:false,
    present:[{provider:"CROSSWATCH",rating:7.5},{provider:"SIMKL",rating:7}],missing:[]},true,true);
  assert.match(html,/Scores differ/);
  assert.match(html,/is-conflict/);
  assert.doesNotMatch(html,/is-differs/);
  assert.match(html,/On every provider/);
  assert.doesNotMatch(html,/Equivalent scores/);
});

test("equivalent ratings retain partial coverage counts", () => {
  const html = context.timelineCoverage({rating:7.5,agree:true,rating_adjusted:true,
    present:[{provider:"CROSSWATCH",rating:7.5},{provider:"SIMKL",rating:8}],missing:[{provider:"TRAKT"},{provider:"PLEX"}]},true,true);
  assert.match(html,/On 2 of 4/);
  assert.match(html,/Equivalent scores/);
  assert.doesNotMatch(html,/is-full|is-conflict/);
});

test("mixed conflicts flag the row without blaming equivalent provider badges", () => {
  const html = context.timelineCoverage({rating:7.5,agree:false,rating_adjusted:false,
    present:[{provider:"CROSSWATCH",rating:7.5},{provider:"SIMKL",rating:8},{provider:"TRAKT",rating:7}],missing:[{provider:"PLEX"}]},true,true);
  assert.match(html,/On 3 of 4/);
  assert.match(html,/Scores differ/);
  assert.match(html,/after provider rounding or reaction conversion/);
  assert.match(html,/>7.5<\/b>/);
  assert.match(html,/>8<\/b>/);
  assert.match(html,/>7<\/b>/);
  assert.doesNotMatch(html,/is-differs/);
});

test("identical scores keep normal coverage labels", () => {
  const html = context.timelineCoverage({rating:8,agree:true,rating_adjusted:false,
    present:[{provider:"CROSSWATCH",rating:8},{provider:"SIMKL",rating:8}],missing:[]},true,true);
  assert.match(html,/On every provider/);
  assert.doesNotMatch(html,/Scores differ|Equivalent scores|is-differs/);
});
