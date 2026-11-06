/* tests/item-actions.test.mjs */
/* CrossWatch - provider icon action request and result tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/item-actions.js", import.meta.url), "utf8");

function load(responses, dataset = {}) {
  const calls = [];
  const focused = [];
  const element = () => ({ setAttribute() {}, addEventListener() {}, appendChild() {}, querySelector: () => null, querySelectorAll: () => [], classList: { add() {}, remove() {}, toggle() {} }, dataset: {}, style: {} });
  const clicks = [];
  const keydowns = [];
  const created = [];
  const context = {
    window: { CW: {}, setTimeout, clearTimeout },
    navigator: { language: "en-US" },
    document: {
      documentElement: { dataset, classList: { add() {}, remove() {} } },
      head: { appendChild() {} },
      body: { appendChild() {} },
      getElementById: () => null,
      createElement: () => {
        const node = element();
        node.querySelector = selector => selector.startsWith("[data-ia-score=") ? { focus: () => focused.push(selector) } : null;
        node.addEventListener = (type, handler) => {
          if (type === "click") clicks.push(handler);
          if (type === "keydown") keydowns.push(handler);
        };
        created.push(node);
        return node;
      },
      addEventListener() {},
    },
    AbortController,
    URLSearchParams,
    fetch: async (url, init = {}) => {
      const body = init.body ? JSON.parse(init.body) : undefined;
      calls.push({ url, body });
      const next = responses.shift();
      const data = typeof next === "function" ? next(url, body) : next;
      return { ok: data?.status ? data.status < 400 : true, status: data?.status || 200, json: async () => data?.body ?? data };
    },
  };
  context.window.document = context.document;
  context.window.addEventListener = () => {};
  vm.runInNewContext(source.replace("  (window.CW ||= {}).ItemActions =", "  window.testActions = { identity, sendAdd, editorRemove, watchlistRemove, progressTask, watchlistKeys };\n  (window.CW ||= {}).ItemActions ="), context);
  context.CSS = { escape: String };
  return { api: context.window.testActions, actions: context.window.CW.ItemActions, calls, created, clicks, keydowns, focused };
}

test("identity keeps typed ids and drops nested objects", () => {
  const { api } = load([]);
  const out = api.identity({ key: "x", type: "tv", title: "Show", year: 2026, tmdb: "12", ids: { imdb: "tt1", show_ids: { tmdb: 9 }, bad: { a: 1 } }, present: [] });
  assert.deepEqual(JSON.parse(JSON.stringify(out)), { type: "show", title: "Show", year: 2026, ids: { imdb: "tt1", tmdb: "12" }, show_ids: { tmdb: 9 } });
});

test("adds report confirmations, skips and single-play providers", async () => {
  const { api, calls } = load([
    { results: [{ ok: true, result: { confirmed: 1 } }] },
    { results: [{ ok: false, error: "rewatch_not_supported" }] },
    { results: [{ ok: true, result: { confirmed: 0, unresolved: 1 } }] },
    { results: [{ ok: true, result: { confirmed: 0, skipped: 1 } }] },
  ]);
  const ref = { provider: "TRAKT", instance: "default" };
  assert.equal((await api.sendAdd("history", ref, { type: "movie" }, "Trakt", true)).ok, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body)), { kind: "history", items: [{ type: "movie" }], providers: [ref], rewatch: true });
  const single = await api.sendAdd("history", ref, {}, "Plex", true);
  assert.equal(single.ok, false);
  assert.equal(single.skipped, true);
  assert.match(single.error, /keeps one play/);
  assert.match((await api.sendAdd("watchlist", ref, {}, "Trakt")).error, /could not match/);
  assert.equal((await api.sendAdd("watchlist", ref, {}, "Trakt")).note, "already there");
});

test("tracker removals go direct by id without a preview", async () => {
  const { api, calls } = load([
    { results: [{ ok: true, result: { removed: 1 } }] },
    { results: [{ ok: false, result: { unresolved: 1 } }] },
  ]);
  const ref = { provider: "SIMKL", instance: "default" };
  assert.equal((await api.editorRemove("ratings", ref, { type: "movie", ids: { tmdb: "1" } }, "SIMKL")).ok, true);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "/api/editor/send");
  assert.equal(calls[0].body.direct, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body.providers)), [ref]);
  assert.match((await api.editorRemove("ratings", ref, { type: "movie", ids: { tmdb: "1" } }, "SIMKL")).error, /did not find/);
});

test("media servers fall back to the preview and live match", async () => {
  const { api, calls } = load([
    { results: [{ ok: false, error: "needs_live_match", result: {} }] },
    { providers: [{ provider: "PLEX", instance: "default" }], preview_id: "abc" },
    { results: [{ ok: true, result: { removed: 1 } }] },
  ]);
  const ref = { provider: "PLEX", instance: "default" };
  assert.equal((await api.editorRemove("history", ref, { type: "movie", ids: { tmdb: "1" } }, "Plex")).ok, true);
  assert.deepEqual(calls.map((call) => call.url), ["/api/editor/send", "/api/editor/send/preview", "/api/editor/send"]);
  assert.equal(calls[2].body.preview_id, "abc");
  assert.equal(calls[2].body.direct, undefined);
});

test("watchlist removal sends the key and aliases and explains a missing record", async () => {
  const { api, calls } = load([
    { deleted_ok: 1 },
    { deleted_ok: 0, results: [{ per_key: [{ reason: "not_in_state" }] }] },
  ]);
  const ref = { provider: "PLEX", instance: "home" };
  assert.equal((await api.watchlistRemove(ref, { key: "tmdb:1", aliases: ["imdb:tt1"] }, "Plex")).ok, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body)), { provider: "PLEX", provider_instance: "home", keys: [{ key: "tmdb:1", aliases: ["imdb:tt1"] }] });
  assert.match((await api.watchlistRemove(ref, { key: "tmdb:1" }, "Plex")).error, /no watchlist record/);
});

test("watchlist removal without a stored key falls back to id keys", async () => {
  const { api, calls } = load([{ deleted_ok: 1 }]);
  assert.deepEqual(JSON.parse(JSON.stringify(api.watchlistKeys({ type: "movie", ids: { tmdb: "11", imdb: "tt1" } }))), ["imdb:tt1", "tmdb:11"]);
  const result = await api.watchlistRemove({ provider: "TRAKT", instance: "default" }, { type: "movie", ids: { tmdb: "11", imdb: "tt1" } }, "Trakt");
  assert.equal(result.ok, true);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body.keys)), [{ key: "imdb:tt1", aliases: ["tmdb:11"] }]);
});

test("watchlist fallback keys keep shows apart from movies with the same TMDB id", () => {
  const { api } = load([]);
  assert.deepEqual(JSON.parse(JSON.stringify(api.watchlistKeys({ type: "show", ids: { tmdb: "11", tvdb: "5" } }))), ["tmdb:11#show", "tvdb:5"]);
  assert.deepEqual(JSON.parse(JSON.stringify(api.watchlistKeys({ type: "movie", ids: { tmdb: "11" } }))), ["tmdb:11"]);
});

test("undated plays are skipped on providers that need a date", async () => {
  const { api } = load([{ results: [{ ok: false, error: "undated_not_supported" }] }]);
  const result = await api.sendAdd("history", { provider: "PLEX", instance: "default" }, { watched: true, _cw_watched_state: true }, "Plex");
  assert.equal(result.ok, false);
  assert.equal(result.skipped, true);
  assert.match(result.error, /needs a watch date/);
});

test("resume point actions post the record and report the provider message", async () => {
  const record = { provider: "PLEX", instance_id: "home", remote_id: "42", canonical_key: "tmdb:1", can_mark_watched: true };
  const { api, calls } = load([
    { ok: true },
    { status: 400, body: { ok: false, message: "Plex refused the change" } },
  ]);
  const watched = api.progressTask("mark_watched", { provider: "PLEX", instance: "home" }, record, "Plex");
  assert.equal((await watched.exec()).ok, true);
  assert.equal(calls[0].url, "/api/playback_progress/actions/mark_watched");
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0].body)), { provider: "PLEX", instance_id: "home", remote_id: "42", canonical_key: "tmdb:1", record });
  const removed = api.progressTask("remove", { provider: "PLEX", instance: "home" }, record, "Plex");
  await assert.rejects(removed.exec(), /Plex refused the change/);
  assert.equal(calls[1].url, "/api/playback_progress/actions/remove");
});

test("targets carry the accounts outside the pairs and whether a pair exists", async () => {
  const { actions } = load([{ providers: [], hidden: [], others: [{ provider: "mdblist", instance: "default", in_pair: false }], has_pair: false }]);
  const result = await actions.targets("ratings", { media_type: "movie" });
  assert.equal(result.hasPair, false);
  assert.equal(result.others[0].provider, "MDBLIST");
});

test("targets from an older reply count as paired with nothing extra", async () => {
  const { actions } = load([{ providers: [], hidden: [] }]);
  const result = await actions.targets("ratings", {});
  assert.equal(result.hasPair, true);
  assert.equal(result.others.length, 0);
});

const anchor = () => ({ isConnected: true, classList: { add() {}, remove() {} }, contains: () => false, focus() {}, getBoundingClientRect: () => ({ left: 0, top: 0, bottom: 0 }) });
const unpaired = { provider: "MDBLIST", instance: "default", in_pair: false };
const paired = { provider: "TRAKT", instance: "default", in_pair: true };

test("the coverage menu keeps accounts outside the pairs out of its missing list", async () => {
  const { actions, created } = load([{ providers: [paired], hidden: [], others: [unpaired], has_pair: true }]);
  await actions.openMenu({ anchor: anchor(), feature: "ratings", item: { title: "Michael", type: "movie" }, refs: { present: [], missing: [{ provider: "MDBLIST", instance: "default" }] } });
  const html = created[0].innerHTML;
  assert.match(html, /data-ia-ref="TRAKT:default"/);
  assert.doesNotMatch(html, /data-ia-ref="MDBLIST:default"/);
});

test("Add all sends every missing paired account and leaves the others out", async () => {
  const { actions, calls, clicks } = load([
    { providers: [paired], hidden: [], others: [unpaired], has_pair: true },
    { results: [{ ok: true, result: { confirmed: 1 } }] },
  ]);
  await actions.openMenu({ anchor: anchor(), feature: "watchlist", item: { title: "Michael", type: "movie", ids: { tmdb: "1" } }, refs: { present: [], missing: [{ provider: "MDBLIST", instance: "default" }] } });
  clicks[0]({ target: { closest: () => ({ dataset: { iaAct: "add-all" } }) } });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(calls[1].url, "/api/editor/send");
  assert.deepEqual(JSON.parse(JSON.stringify(calls[1].body.providers)), [{ provider: "TRAKT", instance: "default" }]);
});

test("a title added once can be removed and says it is not in a pair", async () => {
  const { actions, created } = load([{ providers: [paired], hidden: [], others: [unpaired], has_pair: true }]);
  await actions.openMenu({ anchor: anchor(), feature: "ratings", item: { title: "Michael", type: "movie" }, focus: unpaired, refs: { present: [{ ...unpaired, rating: 8 }], missing: [] } });
  const html = created[0].innerHTML;
  assert.match(html, /Remove rating on/);
  assert.match(html, /Not in a sync pair for ratings</);
});

test("write access follows the role and view-as mode", () => {
  assert.equal(load([], { cwRole: "user" }).actions.canWrite(), false);
  assert.equal(load([], { cwRole: "user", cwPermWrite: "on" }).actions.canWrite(), true);
  assert.equal(load([], {}).actions.canWrite(), true);
});

test("compact stars preserve half scores and save directly to the focused provider", async () => {
  const target = {provider:"CROSSWATCH", instance:"default", rating_scale:{values:Array.from({length:20}, (_, i)=>(i+1)/2), max:10, display_factor:1}};
  const { actions, created, clicks, calls, focused } = load([{ providers: [target], hidden: [], others: [], has_pair: true }, { results: [{ ok: true, result: { confirmed: 1 } }] }]);
  await actions.openMenu({ anchor: anchor(), feature: "ratings", item: { title: "Movie", type: "movie" }, focus: target, refs: { present: [{ ...target, rating: 7.5 }], missing: [] } });
  assert.match(created[0].innerHTML, /data-ia-score-value>7.5</);
  assert.equal(focused[0], '[data-ia-score="7.5"]');
  assert.doesNotMatch(created[0].innerHTML, /Rated 7.5\/10/);
  assert.equal((created[0].innerHTML.match(/data-ia-act="score"/g) || []).length, 20);
  assert.doesNotMatch(created[0].innerHTML, /score-down|score-up/);
  clicks[0]({ target: { closest: () => ({ dataset: { iaAct: "score", iaScore: "0.5" } }) } });
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.doesNotMatch(created[0].innerHTML, /Change to/);
  assert.equal(calls[1].url, "/api/editor/send");
  assert.equal(calls[1].body.items[0].rating, 0.5);
  assert.deepEqual(calls[1].body.providers, [{provider: "CROSSWATCH", instance: "default"}]);
});

test("whole-point providers expose full stars and cannot submit half points", async () => {
  const target = {provider:"SIMKL",instance:"default",rating_scale:{values:Array.from({length:10},(_,i)=>i+1),max:10,display_factor:1}};
  const {actions,created,clicks,calls} = load([{providers:[target]}, {results:[{ok:true,result:{confirmed:1}}]}]);
  await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},focus:target,refs:{present:[{...target,rating:7}]}});
  assert.match(created[0].innerHTML,/cw-ia-stars is-whole/);
  assert.equal((created[0].innerHTML.match(/data-ia-act="score"/g)||[]).length,10);
  assert.doesNotMatch(created[0].innerHTML,/data-ia-score="7.5"/);
  clicks[0]({target:{closest:()=>({dataset:{iaAct:"score",iaScore:"7.5"}})}});
  assert.equal(calls.length,1);
  clicks[0]({target:{closest:()=>({dataset:{iaAct:"score",iaScore:"8"}})}});
  await new Promise(resolve=>setTimeout(resolve,0));
  assert.equal(calls[1].body.items[0].rating,8);
});

for (const focus of [false,true]) {
  test(`Profile ${focus ? "provider" : "coverage"} picker requires a server rating scale`, async () => {
    const target = {provider:"SIMKL",instance:"default"};
    const {actions,created,clicks,calls} = load([{providers:[target]}]);
    await actions.openMenu({anchor:anchor(),feature:"ratings",rating:7.5,item:{type:"movie"},focus:focus ? target : null,refs:{present:[],missing:[target]}});
    assert.match(created[0].innerHTML,/Rating choices are unavailable/);
    assert.doesNotMatch(created[0].innerHTML,/data-ia-score="|data-ia-act="add-all"/);
    if (!focus) assert.match(created[0].innerHTML,/data-ia-on="0" disabled/);
    clicks[0]({target:{closest:()=>({dataset:{iaAct:"score",iaScore:"8"}})}});
    clicks[0]({target:{closest:()=>({dataset:{iaAct:"add-all"}})}});
    assert.equal(calls.length,1);
  });
}

for (const [provider,scale,score,label] of [
  ["SIMKL",{values:[1,2,3,4,5,6,7,8,9,10],max:10,display_factor:1},7.5,"8/10"],
  ["SILO",{values:[2,4,6,8,10],max:5,display_factor:0.5,round_step:2},7,"4/5"],
]) {
  for (const focused of [false,true]) {
    test(`${provider} missing ${focused ? "icon" : "coverage"} starts at the nearest valid score`, async () => {
      const target = {provider,instance:"default",rating_scale:scale};
      const cw = {provider:"CROSSWATCH",instance:"default",rating:score};
      const {actions,created,clicks,calls} = load([{providers:[target]},{results:[{ok:true,result:{confirmed:1}}]}]);
      await actions.openMenu({anchor:anchor(),feature:"ratings",rating:score,item:{type:"movie"},focus:focused ? target : null,refs:{present:[cw],missing:[target]}});
      assert.match(created[0].innerHTML,/data-ia-score="8"[^>]*aria-pressed="true"/);
      if (!focused) assert.ok(created[0].innerHTML.includes(`Rate ${label} on ${provider}`));
      assert.equal(calls.length,1);
      clicks[0]({target:{closest:()=>({dataset:focused ? {iaAct:"score",iaScore:"8"} : {iaAct:"add-all"}})}});
      await new Promise(resolve=>setTimeout(resolve,0));
      assert.equal(calls[1].body.items[0].rating,8);
      assert.equal(calls[1].body.providers[0].provider,provider);
    });
  }
}

test("multi-provider ratings use the intersection of supported scores", async () => {
  const half = {provider:"CROSSWATCH",instance:"default",rating_scale:{values:Array.from({length:20},(_,i)=>(i+1)/2),max:10,display_factor:1}};
  const whole = {provider:"SIMKL",instance:"default",rating_scale:{values:Array.from({length:10},(_,i)=>i+1),max:10,display_factor:1}};
  const {actions,created} = load([{providers:[half,whole]}]);
  await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},refs:{present:[],missing:[half,whole]}});
  assert.equal((created[0].innerHTML.match(/data-ia-act="score"/g)||[]).length,10);
  assert.doesNotMatch(created[0].innerHTML,/data-ia-score="0.5"/);
});

test("five-star providers display native scores but send normalized values", async () => {
  const target={provider:"SILO",instance:"default",rating_scale:{values:[2,4,6,8,10],max:5,display_factor:0.5}};
  const {actions,created,clicks,calls}=load([{providers:[target]},{results:[{ok:true,result:{confirmed:1}}]}]);
  await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},focus:target,refs:{present:[{...target,rating:8}]}});
  assert.match(created[0].innerHTML,/data-ia-score-value>4<\/b><small>\/5/);
  assert.equal((created[0].innerHTML.match(/data-ia-act="score"/g)||[]).length,5);
  clicks[0]({target:{closest:()=>({dataset:{iaAct:"score",iaScore:"10"}})}});
  await new Promise(resolve=>setTimeout(resolve,0));
  assert.equal(calls[1].body.items[0].rating,10);
});

test("unsupported rating responses explain the provider scale", async () => {
  const {api}=load([{results:[{ok:false,error:"unsupported_rating"}]}]);
  const result=await api.sendAdd("ratings",paired,{rating:7.5},"Trakt");
  assert.equal(result.ok,false);
  assert.match(result.error,/does not support that score/);
});

test("keyboard navigation follows the provider's allowed scores", async () => {
  const target={provider:"SIMKL",instance:"default",rating_scale:{values:Array.from({length:10},(_,i)=>i+1),max:10,display_factor:1}};
  const {actions,keydowns,focused}=load([{providers:[target]}]);
  await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},focus:target,refs:{present:[{...target,rating:7}]}});
  keydowns[0]({key:"ArrowRight",preventDefault(){},target:{closest:()=>({dataset:{iaScore:"7"}})}});
  assert.equal(focused.at(-1),'[data-ia-score="8"]');
  keydowns[0]({key:"Home",preventDefault(){},target:{closest:()=>({dataset:{iaScore:"8"}})}});
  assert.equal(focused.at(-1),'[data-ia-score="1"]');
});

test("reaction providers offer their labels instead of numeric stars", async () => {
  const target={provider:"STREMIO",instance:"default",rating_scale:{values:[6,8],labels:{6:"Liked",8:"Loved"},max:10,display_factor:1}};
  const {actions,created}=load([{providers:[target]}]);
  await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},focus:target,refs:{present:[]}});
  assert.match(created[0].innerHTML,/aria-label="Liked"/);
  assert.match(created[0].innerHTML,/aria-label="Loved"/);
  assert.doesNotMatch(created[0].innerHTML,/class="cw-ia-star/);
});

test("MDBList keeps half-star alignment while disabling the unsupported 0.5 score", async () => {
  const target={provider:"MDBLIST",instance:"default",rating_scale:{values:Array.from({length:19},(_,i)=>1+i/2),max:10,display_factor:1}};
  const {actions,created,clicks,calls}=load([{providers:[target]}]);
  await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},focus:target,refs:{present:[]}});
  assert.match(created[0].innerHTML,/data-ia-score="0.5" disabled/);
  assert.match(created[0].innerHTML,/data-ia-score="1.5" aria-label/);
  clicks[0]({target:{closest:()=>({dataset:{iaAct:"score",iaScore:"0.5"}})}});
  assert.equal(calls.length,1);
});

const siloScale = {values:[2,4,6,8,10],max:5,display_factor:0.5,round_step:2};
const reactionScale = {values:[6,9],labels:{6:"Liked",9:"Loved"},bands:[{min:6,max:9,value:6},{min:9,max:10.5,value:9}],max:10,display_factor:1};

for (const [provider, scale, saved, label, selected] of [["SILO",siloScale,7,"4",8],["STREMIO",reactionScale,8,"Liked",6]]) {
  test(`${provider} displays cached scores using the adapter's grouping`, async () => {
    const target = {provider,instance:"default",rating_scale:scale};
    const {actions,created,focused,clicks,calls} = load([{providers:[target]}]);
    await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},focus:target,refs:{present:[{...target,rating:saved}]}});
    assert.ok(created[0].innerHTML.includes(`data-ia-score-value>${label}</b>`));
    assert.equal(focused.at(-1),`[data-ia-score="${selected}"]`);
    clicks[0]({target:{closest:()=>({dataset:{iaAct:"score",iaScore:String(selected)}})}});
    assert.equal(calls.length,1);
  });
  test(`two ${provider} accounts preserve the native score display`, async () => {
    const targets = ["default","second"].map(instance=>({provider,instance,rating_scale:scale}));
    const {actions,created} = load([{providers:targets}]);
    await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},refs:{present:[],missing:targets}});
    const html = created[0].innerHTML;
    if (provider === "SILO") {
      assert.match(html,/Rate 4 out of 5/);
      assert.match(html,/Rate 4\/5/);
      assert.doesNotMatch(html,/\/10/);
    } else {
      assert.match(html,/aria-label="Liked"/);
      assert.match(html,/Rate Liked/);
      assert.doesNotMatch(html,/Rate 6/);
    }
  });
  test(`${provider} account badge displays the native saved score`, async () => {
    const target = {provider,instance:"default",rating_scale:scale};
    const {actions,created} = load([{providers:[target]}]);
    await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},refs:{present:[{...target,rating:saved}]}});
    assert.ok(created[0].innerHTML.includes(`cw-ia-pill is-on">${provider === "SILO" ? "4/5" : "Liked"}</span>`));
  });
}

test("disjoint rating choices open a working per-account picker", async () => {
  const first = {provider:"SILO",instance:"default",rating_scale:siloScale};
  const second = {provider:"STREMIO",instance:"default",rating_scale:{...reactionScale,values:[7,9],labels:{7:"Liked",9:"Loved"},bands:[{min:7,max:9,value:7},{min:9,max:10.5,value:9}]}};
  const {actions,created,clicks,calls} = load([{providers:[first,second]},{results:[{ok:true,result:{confirmed:1}}]}]);
  await actions.openMenu({anchor:anchor(),feature:"ratings",item:{type:"movie"},refs:{present:[],missing:[first,second]}});
  assert.match(created[0].innerHTML,/no shared rating choices/);
  assert.doesNotMatch(created[0].innerHTML,/Rate 0|data-ia-act="add-all"/);
  clicks[0]({target:{closest:()=>({dataset:{iaAct:"choose-rating",iaRef:"STREMIO:default"}})}});
  assert.match(created[0].innerHTML,/data-ia-score="9" aria-label="Loved"/);
  clicks[0]({target:{closest:()=>({dataset:{iaAct:"score",iaScore:"9"}})}});
  await new Promise(resolve=>setTimeout(resolve,0));
  assert.equal(calls[1].body.items[0].rating,9);
  assert.equal(calls[1].body.providers[0].provider,"STREMIO");
});
