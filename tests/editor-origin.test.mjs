/* tests/editor-origin.test.mjs */
/* CrossWatch - Editor item origin row action and popover */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

function fakeDom() {
  const elements = [];
  const element = tag => {
    const node = {tag, children:[], style:{setProperty(name, value) { this[name] = value; }}, dataset:{}, isConnected:true, _text:"",
      classList:{add(name) { node.className = `${node.className || ""} ${name}`.trim(); }, toggle() {}},
      get textContent() { return node._text + node.children.map(child => child.textContent || "").join(""); },
      set textContent(value) { node._text = String(value); node.children = []; },
      appendChild(child) { this.children.push(child); }, append(...children) { this.children.push(...children); },
      replaceChildren(...children) { this._text = ""; this.children = [...children]; },
      hasAttribute(name) { return Object.hasOwn(this, name); },
      querySelectorAll(selector) { return this.children.flatMap(child => [
        ...(selector.split(",").includes(child.tag) ? [child] : []), ...(child.querySelectorAll?.(selector) || []),
      ]); },
      setAttribute(name, value) { this[name] = value; }};
    elements.push(node);
    return node;
  };
  return {elements, document:{createElement:element}};
}

function rowFor(canTrace, opened) {
  const {elements, document} = fakeDom();
  const context = vm.createContext({window:{}, document});
  vm.runInContext(readFileSync(new URL("../assets/js/editor/table.js", import.meta.url), "utf8"), context);
  const row = {_rid:1, type:"anime", title:"Dragon Ball Z", raw:{ids:{anilist:"813"}}};
  context.window.CW.Editor.Table.createRowElement(row, {state:{kind:"watchlist"}, merged:false,
    isPolicySource:() => true, isRowLocked:() => false, canReplaceRow:() => false,
    canTraceOrigin:() => canTrace, openItemOrigin:(value, anchor) => opened.push({value, anchor})});
  return {row, button:elements.find(el => el.title === "Where does this come from?")};
}

test("rows in a provider view get an origin action that opens the popover", () => {
  const opened = [];
  const {row, button} = rowFor(true, opened);
  assert.ok(button);
  assert.match(button.innerHTML, /alt_route/);
  button.onclick();
  assert.equal(opened[0].value, row);
  assert.equal(opened[0].anchor, button);
});

test("rows the Editor cannot trace get no origin action", () => {
  assert.equal(rowFor(false, []).button, undefined);
});

async function openOrigin(data, overrides = {}) {
  const {document} = fakeDom();
  const source = readFileSync(new URL("../assets/js/editor/origin.js", import.meta.url), "utf8")
    .replace("export function openItemOrigin", "function openItemOrigin") + "\nthis.openItemOrigin = openItemOrigin;";
  const context = vm.createContext({window:overrides.window || {}, document, structuredClone});
  vm.runInContext(source, context);
  const pop = document.createElement("div");
  const requests = [];
  let titled = "", actions = [];
  const ctx = {
    state:{snapshot:"ANILIST", instance:"default", kind:"watchlist"},
    openPopup(anchor, builder) { pop.isConnected = true; builder(pop, () => { pop.isConnected = false; }); },
    appendPopupTitle(_pop, text) { titled = text; },
    appendPopupActions(_pop, defs) { actions = defs; },
    fetchJSON(url, opts) { requests.push({url, body:JSON.parse(opts.body)}); return Promise.resolve(data); },
    ...overrides.ctx,
  };
  const anchor = {};
  const click = () => context.openItemOrigin(overrides.row || {type:"anime", title:"Dragon Ball Z", raw:{ids:{anilist:"813"}}}, anchor, ctx);
  click();
  await new Promise(resolve => setImmediate(resolve));
  return {pop, titled, actions, requests, click};
}

test("clicking the origin icon again closes it without another lookup and allows reopening", async () => {
  const {pop, requests, click} = await openOrigin({mode:"ids", sources:[]});
  click();
  assert.equal(pop.isConnected, false);
  assert.equal(requests.length, 1);
  click();
  assert.equal(pop.isConnected, true);
  assert.equal(requests.length, 2);
});

test("origin popovers track their row when the Editor scrolls", async () => {
  let options;
  await openOrigin({}, {ctx:{openPopup(_anchor, _builder, opts) { options = opts; }}});
  assert.equal(options.trackAnchorOnScroll, true);
});

test("popover shows the chain from source title to anime entry", async () => {
  const {pop, titled, actions, requests} = await openOrigin({
    title:"Dragon Ball Z", mode:"anime", mapping_ready:true, anime:[{namespace:"anilist", id:"813", via:"direct", episodes:[]}],
    pairs:[{pair_id:"p1"}], links:[], links_total:0, sources_total:1,
    sources:[{pair_label:"Trakt → AniList", provider:"TRAKT", instance:"default", title:"Dragon Ball Z", year:1989,
      via:"mapping", link:{namespace:"tmdb", id:"12971", kind:"show", season:1}, season:1, count:39,
      first_episode:1, last_episode:39, anime:{namespace:"anilist", id:"813"}}],
  });
  assert.equal(titled, "Where does this come from?");
  assert.equal(requests[0].url, "/api/editor/origin");
  assert.deepEqual({...requests[0].body, original:undefined},
    {provider:"ANILIST", instance:"default", feature:"watchlist", key:"", pair_id:"", original:undefined});
  const text = pop.textContent;
  assert.match(text, /Comes from/);
  assert.match(text, /TraktDefault instanceDragon Ball Z \(1989\)Trakt → AniList/);
  assert.match(text, /TMDB 12971 S1 · E1-39 · 39 episodes/);
  assert.match(text, /Anime ID Mapping/);
  assert.match(text, /AniList #813/);
  assert.deepEqual([...actions].map(a => a.label), ["Custom anime mappings", "Close"]);
});

test("popover explains when no source maps to the entry", async () => {
  const {pop} = await openOrigin({
    title:"Kai", mode:"anime", mapping_ready:true, anime:[{namespace:"anilist", id:"12345", via:"direct", episodes:[]}],
    pairs:[{pair_id:"p1"}], sources:[], sources_total:0, links_total:1,
    links:[{via:"mapping", namespace:"tmdb", id:"61709", kind:"show", season:1, episodes:"1-98"}],
  });
  assert.match(pop.textContent, /No title in your sources maps to this entry/);
  assert.match(pop.textContent, /TMDB 61709 S1 E1-98/);
});

test("popover warns when a show matched through a movie mapping", async () => {
  const {pop} = await openOrigin({
    title:"Anime Movie", mode:"anime", mapping_ready:true, anime:[{namespace:"anilist", id:"9001", via:"direct", episodes:[]}],
    pairs:[{pair_id:"p1"}], links:[], links_total:0, sources_total:1,
    sources:[{pair_label:"Trakt → AniList", provider:"TRAKT", instance:"default", title:"Show 603", kind:"show",
      via:"mapping", link:{namespace:"tmdb", id:"603", kind:"movie", season:null}, season:null, count:1,
      first_episode:null, last_episode:null, anime:{namespace:"anilist", id:"9001"}, kind_mismatch:true}],
  });
  assert.match(pop.textContent, /TMDB 603 \(movie\)/);
  assert.match(pop.textContent, /Matched through a TMDB movie with the same ID as this show/);
});

function canTraceOriginWith(state, merged = false) {
  const source = readFileSync(new URL("../assets/js/editor.js", import.meta.url), "utf8");
  const start = source.indexOf("  function canTraceOrigin(row) {");
  const body = source.slice(start, source.indexOf("\n  }\n", start) + 4);
  const context = vm.createContext({state, isProviderPickerSource:() => true, isMergedView:() => merged});
  vm.runInContext(`${body}\nthis.canTraceOrigin = canTraceOrigin;`, context);
  return context.canTraceOrigin;
}

test("every row in a single provider view can be traced, merged view rows cannot", () => {
  const row = {type:"movie", raw:{ids:{tmdb:"603"}}};
  assert.equal(canTraceOriginWith({snapshot:"TRAKT"})(row), true);
  assert.equal(canTraceOriginWith({snapshot:"TRAKT"}, true)(row), false);
  assert.equal(canTraceOriginWith({snapshot:""})(row), false);
});

test("popover header says how a non-anime row maps to anime trackers", async () => {
  const {pop} = await openOrigin({
    title:"Some Movie", mode:"ids", mapping_ready:true, anime:[{namespace:"anilist", id:"5555", via:"custom", episodes:[]}],
    pairs:[], sources:[], sources_total:0, links:[], links_total:0,
  });
  assert.match(pop.textContent, /On anime trackers: AniList #5555/);
  assert.doesNotMatch(pop.textContent, /via your custom mapping/);
});

test("popover shows saved fixes on sources and on the row itself", async () => {
  const fix = {scope:"pair", key:"tmdb:222", original_key:"tmdb:111",
    original:{type:"movie", title:"Masters of the Universe", year:1987, ids:{tmdb:"111"}}};
  const {pop, actions, requests} = await openOrigin({
    title:"Masters of the Universe", mode:"ids", mapping_ready:true, anime:[], row_fix:{...fix, scope:"shared"},
    pairs:[{pair_id:"p1"}], links:[], links_total:0, sources_total:1,
    sources:[{pair_label:"Plex → Trakt", provider:"PLEX", instance:"default", title:"Masters of the Universe", year:2026,
      via:"ids", link:{namespace:"tmdb", id:"222", kind:"movie", season:null}, season:null, count:1,
      first_episode:null, last_episode:null, anime:null, fix}],
  }, {row:{type:"movie", key:"tmdb:222", title:"Masters of the Universe", raw:{ids:{tmdb:"222"}}},
    ctx:{state:{snapshot:"TRAKT", instance:"default", kind:"watchlist", mappingPair:"p1"},
      canReplaceRow:() => true, openMapping() {}}});
  assert.equal(requests[0].body.key, "tmdb:222");
  assert.equal(requests[0].body.pair_id, "p1");
  const text = pop.textContent;
  assert.match(text, /This row is your fix \(all pairs\)\. Original: Masters of the Universe \(1987\) · TMDB 111/);
  assert.match(text, /Your fix \(this pair\), was: Masters of the Universe \(1987\) · TMDB 111/);
  assert.match(text, /TMDB 222 \(movie\)/);
  assert.match(text, /Same IDs/);
  assert.doesNotMatch(text, /AniList/);
  assert.deepEqual([...actions].map(a => a.label), ["Edit mapping", "Close"]);
});

test("popover explains a manual add and an untraceable plain row", async () => {
  const {pop} = await openOrigin({
    title:"Thing", mode:"ids", mapping_ready:true, anime:[], row_fix:{scope:"pair", key:"tmdb:5", original_key:null, original:null},
    pairs:[{pair_id:"p1"}], links:[], links_total:0, sources:[], sources_total:0,
  });
  assert.match(pop.textContent, /This row was added by you in the Editor \(this pair\)/);
  assert.match(pop.textContent, /No title in your sources has the same IDs/);
});

test("popover says when the mapping search was cut off", async () => {
  const {pop} = await openOrigin({
    title:"Big", mode:"anime", mapping_ready:true, anime:[{namespace:"anilist", id:"1", via:"direct", episodes:[]}],
    pairs:[{pair_id:"p1"}], links:[], links_total:0, sources:[], sources_total:0, search_truncated:true,
  });
  assert.match(pop.textContent, /The mapping search hit its limit, so this list may be incomplete/);
});


test("provider chips use brand logo, colour and the custom profile name", async () => {
  const meta = {
    label:key => ({PLEX:"Plex", ANILIST:"AniList"})[key] || key,
    tone:key => ({PLEX:{solid:"#e5a000"}, ANILIST:{solid:"#02a9ff"}})[key],
    logLogoPath:key => `/assets/img/${key}-log.svg`,
    instanceLabel:(key, id) => ({"PLEX:PLEX-P01":"Living room", "ANILIST:default":"Default instance"})[`${key}:${id}`] || id,
  };
  const {pop} = await openOrigin({
    title:"Dragon Ball Z", mode:"anime", mapping_ready:true, anime:[{namespace:"anilist", id:"813", via:"direct", episodes:[]}],
    pairs:[{pair_id:"p1"}], links:[], links_total:0, sources_total:1,
    sources:[{pair_label:"Plex to AniList", provider:"PLEX", instance:"PLEX-P01~alice", title:"Dragon Ball Z", year:1989,
      via:"mapping", link:{namespace:"tmdb", id:"12971", kind:"show", season:1}, season:1, count:1,
      first_episode:1, last_episode:1, anime:{namespace:"anilist", id:"813"}}],
  }, {window:{CW:{ProviderMeta:meta}}});
  const chips = [];
  const walk = node => { if (node.className === "cw-origin-provider") chips.push(node); (node.children || []).forEach(walk); };
  walk(pop);
  assert.equal(chips.length, 2);
  assert.equal(chips[0].textContent, "AniListDefault instance");
  assert.equal(chips[1].textContent, "PlexLiving room · alice");
  assert.equal(chips[1].style["--cw-origin-tone"], "#e5a000");
  assert.equal(chips[1].children[0].src, "/assets/img/PLEX-log.svg");
});
