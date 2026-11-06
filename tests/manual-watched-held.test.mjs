/* tests/manual-watched-held.test.mjs */
/* CrossWatch - Quick add notes for titles an account already holds */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/modals/manual-watched/index.js", import.meta.url), "utf8");
const helper = source.slice(source.indexOf("const providerKey ="), source.indexOf("\n", source.indexOf("const providerKey =")));
const notes = source.slice(source.indexOf("const HELD_SOURCES"), source.indexOf("export default"));
const context = {};
vm.runInNewContext(`${helper}\n${notes}\nglobalThis.heldNotes = heldNotes;\nglobalThis.watchedTargets = watchedTargets;\nglobalThis.watchedEpisodes = watchedEpisodes;\nglobalThis.historyFits = historyFits;\nglobalThis.oneOffNote = oneOffNote;`, context);
const heldNotes = (...args) => [...context.heldNotes(...args)];
const watchedTargets = (...args) => [...context.watchedTargets(...args)];

const trakt = { provider: "TRAKT", instance: "default", display: "Trakt", rewatch_write: true };
const simkl = { provider: "SIMKL", instance: "default", display: "SIMKL" };
const second = { provider: "TRAKT", instance: "second", display: "Trakt (second)" };
const held = {
  synced: { present: [{ provider: "TRAKT", instance: "default" }], episodes: [{ season: 1, episode: 2, present: [{ provider: "TRAKT", instance: "default" }] }] },
  ratings: { present: [{ provider: "TRAKT", instance: "default", rating: 7 }] },
  watchlist: { present: [{ provider: "SIMKL", instance: "default" }] },
  collection: null,
};

test("a watched movie says a rewatch is added on the selected account", () => {
  assert.deepEqual(heldNotes(held, ["history"], [trakt]), ["Already watched on Trakt. Sending adds a rewatch."]);
});

test("a provider that keeps one play is named apart and skipped", () => {
  const plex = { provider: "TRAKT", instance: "default", display: "Plex", rewatch_write: false };
  assert.deepEqual(heldNotes(held, ["history"], [plex]), ["Already watched on Plex, which keeps one play. History is skipped there."]);
});

test("picked episodes only count when that episode is watched", () => {
  assert.deepEqual(heldNotes(held, ["history"], [trakt], { show: true, picked: [{ season: 1, episode: 2 }] }), ["Already watched on Trakt. Sending adds a rewatch."]);
  assert.deepEqual(heldNotes(held, ["history"], [trakt], { show: true, picked: [{ season: 1, episode: 3 }] }), []);
  assert.deepEqual(watchedTargets(held, [trakt], [{ season: 1, episode: 3 }]), []);
  assert.equal(watchedTargets(held, [trakt], [{ season: 1, episode: 2 }]).length, 1);
});

test("only selected accounts and picked actions are named", () => {
  assert.deepEqual(heldNotes(held, ["history"], [simkl]), []);
  assert.deepEqual(heldNotes(held, ["history"], [second]), []);
  assert.deepEqual(heldNotes(held, ["collection"], [trakt, simkl]), []);
  assert.deepEqual(heldNotes(held, ["watchlist"], [trakt, simkl]), ["Already in your watchlist on SIMKL."]);
});

test("a rating shows the stored score", () => {
  assert.deepEqual(heldNotes(held, ["rating"], [trakt]), ["Already rated on Trakt (7/10). Sending may replace the score."]);
});

test("a show says episodes were watched, not that a new add is a repeat", () => {
  assert.deepEqual(heldNotes(held, ["history"], [trakt], { show: true }), ["Has watched episodes on Trakt."]);
});

test("only the watched episodes of a pick are marked per account", () => {
  const rows = context.watchedEpisodes(held, trakt, [{ season: 1, episode: 2 }, { season: 1, episode: 3 }]);
  assert.deepEqual(JSON.parse(JSON.stringify(rows)), [{ season: 1, episode: 2 }]);
  assert.equal(context.watchedEpisodes(held, simkl, [{ season: 1, episode: 2 }]).length, 0);
});

test("one-play providers say the watched episodes are skipped, not all history", () => {
  const plex = { provider: "TRAKT", instance: "default", display: "Plex", rewatch_write: false };
  assert.deepEqual(heldNotes(held, ["history"], [plex], { show: true, picked: [{ season: 1, episode: 2 }] }), ["Already watched on Plex, which keeps one play. Those episodes are skipped there."]);
});

test("whole show and picked episodes each follow their own history rule", () => {
  const floppy = { history_whole_show: false, history_episodes: true };
  assert.equal(context.historyFits(floppy, true, false), false);
  assert.equal(context.historyFits(floppy, true, true), true);
  assert.equal(context.historyFits(floppy, false, false), true);
  assert.equal(context.historyFits({}, true, false), true);
});

test("a one-off add names the feature in the sync pair note", () => {
  assert.equal(context.oneOffNote(["rating"]), "Not in a sync pair for ratings. This is added once and stays on that provider only.");
  assert.match(context.oneOffNote(["history"]), /sync pair for history\./);
});

async function mountQuickAdd(props, providers, remembered = []) {
  const root = { innerHTML: "", style: { setProperty() {}, removeProperty() {} }, closest: () => null, addEventListener(name, handler) { this[name] = handler; }, querySelector: () => null, querySelectorAll: () => [] };
  const page = {
    window: { CW: {}, addEventListener() {}, removeEventListener() {}, setTimeout, clearTimeout },
    document: { getElementById: () => ({}), documentElement: { dataset: {}, clientHeight: 800 }, addEventListener() {}, removeEventListener() {} },
    localStorage: { getItem: () => JSON.stringify(remembered), setItem() {} },
    URLSearchParams,
    AbortSignal,
    fetch: async (url) => ({ ok: true, json: async () => (url.includes("/api/manual/providers") ? { providers } : {}) }),
  };
  vm.runInNewContext(source.replace("export default {", "globalThis.quickAdd = {"), page);
  await page.quickAdd.mount(root, props);
  return root;
}

const both = { provider: "MDBLIST", instance: "default", history_enabled: true, ratings_enabled: true, watchlist_enabled: true, collection_enabled: true, rating_scale:{values:Array.from({length:19},(_,i)=>1+i/2),max:10,display_factor:1} };
const locked = (action) => ({ action, item: { tmdb: "1", type: "movie", title: "Michael" }, only: ["MDBLIST:default"] });

for (const rating_scale of [undefined,null,{}]) {
  test(`Quick add does not guess choices for ${JSON.stringify(rating_scale)} scale metadata`, async () => {
    const root = await mountQuickAdd(locked("rating"),[{...both,rating_scale}]);
    assert.doesNotMatch(root.innerHTML,/data-rating="/);
    assert.match(root.innerHTML,/Rating choices are unavailable/);
    assert.match(root.innerHTML,/data-role="send" disabled/);
  });
}

test("Quick add does not report success for a rejected provider rating", async () => {
  const code = source.slice(source.indexOf("const fjson ="), source.indexOf("const esc ="));
  const request = vm.runInNewContext(code + "fjson", {fetch:async()=>({ok:true,json:async()=>({ok:false,results:[{provider:"SIMKL",rating:{ok:false,error:"unsupported_rating"}}]})})});
  await assert.rejects(request("/api/manual/watched"),/Rating not saved on SIMKL/);
});

test("Quick add offers half points only when the selected provider supports them", async () => {
  const props = locked("rating");
  const half = {...both, rating_scale:{values:Array.from({length:20},(_,i)=>(i+1)/2),max:10,display_factor:1}};
  const halfRoot = await mountQuickAdd(props,[half]);
  assert.match(halfRoot.innerHTML,/data-rating="7.5"/);
  const whole = {...both, rating_scale:{values:Array.from({length:10},(_,i)=>i+1),max:10,display_factor:1}};
  const wholeRoot = await mountQuickAdd(props,[whole]);
  assert.doesNotMatch(wholeRoot.innerHTML,/data-rating="7.5"/);
  assert.match(wholeRoot.innerHTML,/data-rating="8"/);
});

for (const [provider,scale] of [
  ["SIMKL",{values:[1,2,3,4,5,6,7,8,9,10],max:10,display_factor:1}],
  ["SILO",{values:[2,4,6,8,10],max:5,display_factor:0.5,round_step:2}],
]) {
  test(`Quick add keeps the nearest score when adding ${provider}`, async () => {
    const cw = {...both,provider:"CROSSWATCH",rating_scale:{values:Array.from({length:20},(_,i)=>(i+1)/2),max:10,display_factor:1}};
    const other = {...both,provider,rating_scale:scale};
    const root = await mountQuickAdd({...locked("rating"),only:undefined},[cw,other],["CROSSWATCH:default"]);
    const click = (attribute,value) => {
      const button = {getAttribute:()=>value,closest:selector=>selector === `[${attribute}]` ? button : null};
      root.click({target:button,preventDefault(){}});
    };
    click("data-rating","7.5");
    click("data-provider-key",`${provider}:default`);
    assert.match(root.innerHTML,/Rate 8\/10 on 2 providers/);
    assert.doesNotMatch(root.innerHTML,/Rate [12]\/10 on 2 providers/);
    click("data-provider-key",`${provider}:default`);
    assert.match(root.innerHTML,/Rate 8\/10 on 1 provider/);
  });
}

test("Quick add load failures do not claim a save failed", async () => {
  const code = source.slice(source.indexOf("const fjson ="), source.indexOf("const esc ="));
  const request = vm.runInNewContext(code + "fjson", {fetch:async()=>({ok:false,json:async()=>({})})});
  await assert.rejects(request("/api/manual/search"),/information could not be loaded/);
  await assert.rejects(request("/api/manual/watched",{method:"POST"}),/changes could not be saved/);
});

test("Quick add defaults to a valid Stremio reaction and highlights only that reaction", async () => {
  const target = {...both,provider:"STREMIO",rating_scale:{values:[7,9.5],labels:{7:"Liked",9.5:"Loved"},bands:[{min:6.7,max:9.2,value:7},{min:9.2,max:10.5,value:9.5}],max:10,display_factor:1}};
  const root = await mountQuickAdd({...locked("rating"),only:["STREMIO:default"]},[target]);
  assert.match(root.innerHTML,/cw-mw-reaction active" data-rating="7"/);
  assert.doesNotMatch(root.innerHTML,/cw-mw-score|Choose a rating supported/);
  const button = {getAttribute:()=>"9.5",closest:selector=>selector.includes("[data-rating]") ? button : null};
  root.click({target:button,preventDefault(){},stopPropagation(){}});
  assert.match(root.innerHTML,/cw-mw-reaction active" data-rating="9.5"/);
  assert.doesNotMatch(root.innerHTML,/cw-mw-reaction active" data-rating="7"/);
});

for (const [provider,scale] of [
  ["SILO",{values:[2,4,6,8,10],max:5,display_factor:0.5}],
  ["STREMIO",{values:[6,8],labels:{6:"Liked",8:"Loved"},max:10,display_factor:1}],
]) {
  test(`Quick add preserves native choices for two ${provider} accounts`, async () => {
    const targets = ["default","second"].map(instance=>({...both,provider,instance,rating_scale:scale}));
    const keys = targets.map(target=>`${provider}:${target.instance}`);
    const root = await mountQuickAdd({...locked("rating"),only:undefined},targets,keys);
    assert.match(root.innerHTML,provider === "SILO" ? /aria-label="Rate 4\/5"/ : /aria-label="Rate Loved"/);
    assert.doesNotMatch(root.innerHTML,/aria-label="Rate 8\/10"/);
  });
}

for (const [action, noun] of [["rating", "ratings"], ["watchlist", "watchlist"], ["collection", "collection"], ["history", "history"]]) {
  test(`a locked Quick add keeps the requested ${action} action`, async () => {
    const root = await mountQuickAdd(locked(action), [both]);
    assert.ok(root.innerHTML.includes(`Not in a sync pair for ${noun}.`));
    assert.equal(root.innerHTML.includes("cw-mw-stars"), action === "rating");
  });
}

test("a locked Quick add never swaps to another feature when its own is not supported", async () => {
  const root = await mountQuickAdd(locked("rating"), [{ ...both, ratings_enabled: false }]);
  assert.match(root.innerHTML, /Not in a sync pair for ratings\./);
  assert.match(root.innerHTML, /data-role="send" disabled/);
  assert.match(root.innerHTML, /Select at least one compatible provider\./);
});

test("a normal Quick add still opens on the action it was asked for", async () => {
  const root = await mountQuickAdd({ action: "rating", item: { tmdb: "1", type: "movie", title: "Michael" } }, [both]);
  assert.match(root.innerHTML, /data-action="rating"/);
  assert.match(root.innerHTML, /cw-mw-chip active" data-action="rating"/);
  assert.doesNotMatch(root.innerHTML, /sync pair/);
});

test("provider buttons show the custom account name", async () => {
  const named = { ...both, instance: "CW-P01", instance_label: "Living room", display: "MDBList (Living room)" };
  const root = await mountQuickAdd({ action: "rating", item: { tmdb: "1", type: "movie", title: "Michael" }, only: ["MDBLIST:CW-P01"] }, [named]);
  assert.ok(root.innerHTML.includes("<small>Living room</small>"));
  assert.ok(!root.innerHTML.includes("<small>CW-P01</small>"));
});

test("provider buttons fall back to the instance id without a custom name", async () => {
  const plain = { ...both, instance: "CW-P01" };
  const root = await mountQuickAdd({ action: "rating", item: { tmdb: "1", type: "movie", title: "Michael" }, only: ["MDBLIST:CW-P01"] }, [plain]);
  assert.ok(root.innerHTML.includes("<small>CW-P01</small>"));
});

test("no stored data means no note", () => {
  assert.deepEqual(heldNotes(null, ["history", "rating"], [trakt]), []);
  assert.deepEqual(heldNotes({}, ["history"], [trakt]), []);
});

test("several actions give one note each", () => {
  assert.equal(heldNotes(held, ["history", "rating"], [trakt]).length, 2);
});
