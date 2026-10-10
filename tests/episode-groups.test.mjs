/* tests/episode-groups.test.mjs */
/* CrossWatch - Episode group coordinate entry regressions */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import {parseEpisodes} from "../assets/js/editor/episode-groups.js";

test("episode entry preserves gaps, specials and explicit season boundaries", () => {
  const rows = parseEpisodes("S00E01, s04e23; S04E25\nS05E01", {tvdb:"79168"}, "Example");
  assert.deepEqual(rows.map(row=>[row.season,row.episode]), [[0,1],[4,23],[4,25],[5,1]]);
  assert.ok(rows.every(row=>row.show_ids.tvdb === "79168" && row.type === "episode"));
  assert.ok(rows.every(row=>!("watched_at" in row)));
});

test("malformed coordinates and oversized groups cannot become a draft", () => {
  for (const value of ["", "S04E0", "4:23", "S04E23-24", "S04E23 bad", Array(21).fill("S04E23").join(",")]) {
    assert.throws(()=>parseEpisodes(value,{tmdb:"100"},"Example"));
  }
});

async function openFromEpisode(pairs, options = {}) {
  const field = () => ({value:"", textContent:"", disabled:false, replaceChildren() {}, append() {}});
  const fields = new Map();
  const get = selector => {
    if (!fields.has(selector)) fields.set(selector, field());
    return fields.get(selector);
  };
  const pairSelect = get("[data-pair]");
  Object.defineProperty(pairSelect, "innerHTML", {set(html) {
    this.value = /<option value="([^"]+)"/.exec(html)?.[1] || "";
  }});
  for (const side of ["source", "target"]) {
    get('[data-side="' + side + '"]').querySelector = selector => get(side + selector);
  }
  const dialog = {setAttribute() {}, querySelector:get, showModal() {}, addEventListener() {}};
  const context = vm.createContext({AbortController, window:{addEventListener() {}},
    document:{createElement:tag=>tag === "dialog" ? dialog : field(), body:{append() {}}, addEventListener() {}},
    fetch:async()=>({ok:true,json:async()=>({pairs})}),
  });
  vm.runInContext(readFileSync(new URL("../assets/js/editor/episode-groups.js", import.meta.url), "utf8")
    .replaceAll("export ", ""), context);
  await context.openEpisodeGroups(null, {provider:"PLEX", instance:"living-room",
    original:{type:"episode",title:"Example",show_ids:{tmdb:"100"},season:4,episode:23}, ...options});
  return get;
}

const pair = (id, provider, instance = "living-room", reverse = false) => ({id, name:id, groups:[],
  source:reverse ? {provider:"TRAKT",instance:"default"} : {provider,instance},
  target:reverse ? {provider,instance} : {provider:"TRAKT",instance:"default"}});

test("episode entry selects a pair containing the exact provider instance and prefills either side", async () => {
  for (const reverse of [false,true]) {
    const get = await openFromEpisode([pair("unrelated","EMBY"), pair("other-instance","PLEX","bedroom"),
      pair("matching","PLEX","living-room",reverse)]);
    assert.equal(get("[data-pair]").value,"matching");
    const side = reverse ? "target" : "source";
    assert.equal(get(side + "[data-show-id]").value,"100");
    assert.equal(get(side + "[data-episodes]").value,"S04E23");
  }
});

test("episode entry preserves an explicit matching pair selection", async () => {
  const get = await openFromEpisode([pair("first","PLEX"),pair("selected","PLEX")],{pairId:"selected"});
  assert.equal(get("[data-pair]").value,"selected");
});

test("episode entry cannot default to an unrelated provider when no pair matches", async () => {
  const get = await openFromEpisode([pair("unrelated","EMBY"),pair("other-instance","PLEX","bedroom")]);
  assert.equal(get("[data-pair]").value,"");
  assert.equal(get("[data-save]").disabled,true);
});

test("opening group management without an episode keeps every pair available", async () => {
  const get = await openFromEpisode([pair("first","EMBY"),pair("second","PLEX")],{provider:undefined,original:undefined,pairId:"first"});
  assert.equal(get("[data-pair]").value,"first");
  assert.equal(get("[data-save]").disabled,false);
});
