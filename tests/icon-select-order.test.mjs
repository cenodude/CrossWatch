/* tests/icon-select-order.test.mjs */
/* CrossWatch - Alphabetical dropdown ordering tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const read = (path) => readFileSync(new URL(`../assets/${path}`, import.meta.url), "utf8");

function element() {
  return {
    children: [], dataset: {},
    get childNodes() { return this.children; },
    set innerHTML(value) { this.children = []; this.html = value; },
    appendChild(child) { this.children.push(child); },
    setAttribute() {},
    addEventListener(type, handler) { this[type] = handler; },
  };
}

function harness() {
  const window = {};
  const document = { readyState: "loading", addEventListener() {}, createElement: element };
  const context = vm.createContext({ window, document, Event });
  vm.runInContext(read("helpers/icon-select.js").replace("window.CW.IconSelect = {", "window.buildMenu = buildMenu; window.positionMenu = positionMenu; window.syncSelect = sync; window.CW.IconSelect = {"), context);
  vm.runInContext(read("helpers/profile-select.js"), context);
  const menu = element();
  const events = [];
  const select = {
    dataset: {}, value: "z",
    options: [
      { value: "z", textContent: "Zulu" },
      { value: "10", textContent: "alpha 10" },
      { value: "all", textContent: "All profiles" },
      { value: "", textContent: "Choose a profile" },
      { value: "2", textContent: "Alpha 2", disabled: true },
    ],
    dispatchEvent(event) { events.push(event.type); },
  };
  return { window, context, select, menu, events, render(cfg = {}) {
    window.buildMenu(select, { __cwMenu: menu }, cfg);
    return menu.children.map((item) => item.dataset.value);
  } };
}

test("alphabetical menus preserve selection, disabled entries and change events", () => {
  const h = harness();
  const original = h.select.options.map((option) => option.value);
  assert.deepEqual(h.render({ sortAlphabetically: true, sortFirstValues: ["all"] }), ["all", "", "2", "10", "z"]);
  assert.equal(h.select.value, "z");
  assert.deepEqual(h.select.options.map((option) => option.value), original);
  assert.equal(h.menu.children[2].disabled, true);
  h.menu.children[3].click({ preventDefault() {} });
  assert.equal(h.select.value, "10");
  assert.deepEqual(h.events, ["change", "input"]);
  h.select.options.push({ value: "b", textContent: "Beta" });
  assert.deepEqual(h.render({ sortAlphabetically: true, sortFirstValues: ["all"] }), ["all", "", "2", "10", "b", "z"]);
});

test("plain settings retain their order and named lists can opt in", () => {
  const h = harness();
  const original = h.select.options.map((option) => option.value);
  assert.deepEqual(h.render(), original);
  h.select.dataset.cwSort = "alphabetical";
  assert.deepEqual(h.render(), ["", "all", "2", "10", "z"]);
  assert.deepEqual(h.render({ sortAlphabetically: false }), original);
});

test("provider and profile helpers enable sorting by their displayed labels", () => {
  const h = harness();
  h.window.CW.IconSelect.enhance = (_, cfg) => cfg;
  for (const method of ["enhanceProvider", "enhanceProfile", "enhanceUserProfile"]) {
    const cfg = h.window.CW.ProfileSelect[method](h.select);
    assert.equal(cfg.sortAlphabetically, true);
    h.select.options = [
      { value: "a", textContent: "First ID", dataset: { label: "Zulu" } },
      { value: "z", textContent: "Last ID", dataset: { label: "Alpha" } },
    ];
    assert.deepEqual(h.render(cfg), ["z", "a"]);
  }
});

test("profile page provider filters sort by label and keep All providers first", () => {
  const h = harness();
  const source = read("js/profile-page.js");
  const start = source.indexOf("  function enhanceCollectionProviderSelect(");
  const end = source.indexOf("  function enhanceCollectionSortSelect(", start);
  h.context.visibleProviderLabel = (value) => value;
  h.context.providerLogLogo = () => "";
  h.window.CW.IconSelect.enhance = (_, cfg) => { h.context.cfg = cfg; };
  vm.runInContext(source.slice(start, end), h.context);
  h.context.select = h.select;
  vm.runInContext("enhanceCollectionProviderSelect(select)", h.context);
  h.select.options = ["Trakt Default", "SIMKL Default", "MDBList Default", "CrossWatch Default"]
    .map((label) => ({ value: label, textContent: label, dataset: { label } }));
  h.select.options.push({ value: "", textContent: "All providers" });
  assert.deepEqual(h.render(h.context.cfg), ["", "CrossWatch Default", "MDBList Default", "SIMKL Default", "Trakt Default"]);
});


test("dropdown enhancement ignores the document root and other non-select elements", () => {
  const h = harness();
  for (const tagName of ["HTML", "BODY", "DIV", "INPUT"]) {
    const node = { tagName, dataset: { cwMediaCard: "full" } };
    assert.equal(h.window.CW.IconSelect.enhance(node), node);
    assert.equal(node.__cwIconSelectCfg, undefined);
  }
});

test("media card preference refresh only enhances dropdowns, never the page root", () => {
  const root = { tagName: "HTML", dataset: { cwMediaCard: "full" } };
  const controls = [{ tagName: "SELECT", value: "full" }, { tagName: "SELECT", value: "full" }];
  const enhanced = [];
  const window = { CW: { IconSelect: { enhance(node) { enhanced.push(node); } } } };
  const document = {
    documentElement: root,
    querySelectorAll(selector) {
      return selector === "select[data-cw-media-card]" ? controls : [root, ...controls];
    },
  };
  const context = vm.createContext({ window, document, navigator: { language: "en" }, Intl });
  vm.runInContext(read("js/profile-media-modal.js"), context);
  for (const style of ["compact", "full", "compact"]) {
    window.CW.ProfileMediaModal.setStyle(style);
    assert.ok(controls.every((control) => control.value === style));
    assert.equal(window.CW.ProfileMediaModal.canOpen({ tmdb: "123" }), style === "full");
  }
  assert.equal(enhanced.length, 6);
  assert.ok(enhanced.every((node) => node.tagName === "SELECT"));
  assert.equal(root.value, undefined);
});


test("dropdowns fit their content and remain within the viewport", () => {
  const h = harness();
  Object.assign(h.window, { innerWidth: 800, innerHeight: 600 });
  const rect = { left: 690, top: 80, bottom: 120, width: 90 };
  const btn = { getBoundingClientRect: () => rect };
  const cfg = {};
  const wrap = { __cwNativeSelect: { __cwIconSelectCfg: cfg } };
  const menu = {
    classList: { contains: () => false }, style: {}, scrollHeight: 180, offsetHeight: 180,
    getBoundingClientRect: () => ({ width: 360 }),
  };
  h.window.positionMenu(wrap, btn, menu);
  assert.equal(menu.style.width, "360px");
  assert.equal(menu.style.left, "428px");
  assert.equal(menu.style.top, "128px");
  cfg.menuMinWidth = 500;
  h.window.positionMenu(wrap, btn, menu);
  assert.equal(menu.style.width, "500px");
  h.window.innerWidth = 320;
  h.window.positionMenu(wrap, btn, menu);
  assert.equal(menu.style.width, "296px");
  assert.equal(menu.style.left, "12px");
  assert.equal(menu.style.maxWidth, "296px");
  cfg.menuMinWidth = 0;
  h.window.innerWidth = 800;
  rect.width = 420;
  rect.top = 530;
  rect.bottom = 570;
  h.window.positionMenu(wrap, btn, menu);
  assert.equal(menu.style.width, "420px");
  assert.equal(menu.style.top, "342px");
});

test("full selection tooltips survive compact labels, updates and empty options", () => {
  const h = harness();
  const btn = { insertBefore() {}, querySelector() {} };
  const label = { replaceWith() {} };
  const wrap = { classList: { toggle() {} }, querySelector: name => name.endsWith("-btn") ? btn : label };
  h.select.selectedIndex = 0;
  const cfg = { getOptionData: () => ({ label: "Only Pair 2 - JELLYFIN to TRAKT", selectedLabel: "Pair 2", note: "Living room" }) };
  h.window.syncSelect(h.select, wrap, cfg);
  assert.equal(btn.title, "Only Pair 2 - JELLYFIN to TRAKT \u00b7 Living room");
  h.render(cfg);
  assert.equal(h.menu.children[0].title, btn.title);
  h.select.disabled = true;
  h.select.selectedIndex = -1;
  h.window.syncSelect(h.select, wrap, {});
  assert.equal(btn.disabled, true);
  assert.equal(btn.title, "-");
});

test("html rows replace the row content and keep the option text as tooltip", () => {
  const h = harness();
  h.render({ getOptionData: (value) => value === "z" ? { html: "<b>Plex</b>" } : {} });
  const item = h.menu.children.find((row) => row.dataset.value === "z");
  const main = item.children[0];
  assert.equal(item.title, "Zulu");
  assert.equal(main.children.length, 1);
  assert.equal(main.children[0].className, "cw-icon-select-html");
  assert.equal(main.children[0].html, "<b>Plex</b>");
});

test("selected rows can show shorter html than the menu row", () => {
  const h = harness();
  const btn = { insertBefore() {}, querySelector: () => null };
  const label = { replaceWith(next) { btn.main = next; } };
  const wrap = { classList: { toggle() {} }, querySelector: name => name.endsWith("-btn") ? btn : label };
  h.select.selectedIndex = 0;
  const cfg = { getOptionData: () => ({ html: "<b>1</b> Plex", selectedHtml: "Plex" }) };
  h.window.syncSelect(h.select, wrap, cfg);
  assert.equal(btn.main.children[0].html, "Plex");
  h.render(cfg);
  assert.equal(h.menu.children[0].children[0].children[0].html, "<b>1</b> Plex");
});

function syncHelpers() {
  const source = read("helpers/core.js");
  const context = vm.createContext({
    META: { brandInfo: (provider) => ({ label: provider, icon: `/${provider}.svg` }), instanceLabel: (_, id) => (id === "default" ? "Default instance" : id) },
    providerLabel: (provider) => provider,
    escapeHtml: (value) => String(value).replace(/</g, "&lt;"),
    featureEnabled: (value) => value === true || !!value?.enable,
  });
  vm.runInContext(source.slice(source.indexOf("  const SYNC_FEATURE_DOT_CLASSES = {"), source.indexOf("  function pairTitle(")), context);
  return context;
}

function schedulerPairs(pairs) {
  const source = read("js/scheduler.js");
  const slice = (from, to) => source.slice(source.indexOf(from), source.indexOf(to));
  const helpers = syncHelpers();
  const window = { CW: {
    syncPairOptionData: vm.runInContext("syncPairOptionData", helpers),
    syncFeatureDotsHTML: vm.runInContext("syncFeatureDotsHTML", helpers),
  } };
  const context = vm.createContext({ window, _pairs: pairs });
  vm.runInContext(slice("  const pairOptionData = ", "  const enhanceIconSelect = "), context);
  vm.runInContext(slice("  const pairOptions = ", "  const softenStatus = "), context);
  return context;
}

test("schedule pairs redraw on opening the pane only when they changed", async () => {
  const source = read("js/scheduler.js");
  const start = source.indexOf("  const refreshPairs = async () => {");
  const end = source.indexOf('  document.addEventListener("cw-settings-pane-changed"', start);
  const renders = [];
  const render = (name) => () => renders.push(name);
  const context = vm.createContext({
    _pairs: [{ id: "a" }],
    _loading: false,
    authSetupPending: () => false,
    $: () => ({}),
    fetchPairs: async () => {},
    renderJobs: render("jobs"),
    renderWorkflows: render("workflows"),
    renderEventRules: render("events"),
  });
  vm.runInContext(`var _pairs = this._pairs, _loading = false; ${source.slice(start, end)}; this.refreshPairs = refreshPairs;`, context);
  await context.refreshPairs();
  assert.deepEqual(renders, []);
  vm.runInContext('fetchPairs = async () => { _pairs = [{ id: "a", features: { ratings: true } }]; }', context);
  await context.refreshPairs();
  assert.deepEqual(renders, ["jobs", "workflows", "events"]);
  renders.length = 0;
  vm.runInContext("_loading = true", context);
  await context.refreshPairs();
  assert.deepEqual(renders, []);
});

test("schedule pair dropdown keeps the Sync pairs order", () => {
  const context = schedulerPairs([
    { id: "b", order: 1, label: "TRAKT -> PLEX one-way", enabled: true },
    { id: "a", order: 2, label: "PLEX -> TRAKT two-way", enabled: false },
  ]);
  const ids = vm.runInContext("pairOptions(null).map((row) => row[0])", context);
  assert.deepEqual([...ids], ["", "b", "a"]);
});

test("schedule pair rows number the pair and mark steps that already use it", () => {
  const context = schedulerPairs([
    { id: "a", order: 3, source: "PLEX", target: "TRAKT", enabled: true, features: { ratings: true } },
    { id: "b", order: 4, source: "TRAKT", target: "PLEX", enabled: false },
  ]);
  context.workflow = { steps: [{ pair_id: "a" }, { pair_id: "b" }, { pair_id: "a" }] };
  const used = vm.runInContext("stepsUsingPair(workflow, workflow.steps[1], 'a')", context);
  assert.deepEqual([...used], [1, 3]);
  assert.deepEqual([...vm.runInContext("stepsUsingPair(workflow, workflow.steps[0], '')", context)], []);
  const row = vm.runInContext("pairOptionData('a', [1, 3])", context);
  assert.match(row.html, /cw-sync-pair-order">3</);
  assert.match(row.html, /Step 1, 3/);
  assert.match(row.selectedHtml, /cw-sync-feature-dot rt/);
  assert.doesNotMatch(row.selectedHtml, /cw-sync-pair-order|Step/);
  const off = vm.runInContext("pairOptionData('b')", context);
  assert.match(off.selectedHtml, /Disabled/);
  assert.match(off.selectedHtml, /No features/);
  assert.equal(vm.runInContext("pairOptionData('missing')", context), null);
});

test("analyzer pair dropdown keeps the Sync pairs order with pills and dots", () => {
  const source = read("js/analyzer/index.js");
  assert.match(source, /menuClassName: "an-pair-menu", getOptionData: pairOptionData/);
  assert.match(source, /array\(rawPairs\)\.map\(\(pair, index\) => \(\{ \.\.\.pair, order: index \+ 1 \}\)\)\.filter/);
  const start = source.indexOf("    const pairOptionData = value => {");
  const end = source.indexOf("    const rowPairName = row => {");
  const window = { CW: { syncPairOptionData: (pair) => ({ html: `<row>${pair.order}</row>` }) } };
  const context = vm.createContext({ window, pairs: [{ id: "b", order: 4 }] });
  vm.runInContext(`${source.slice(start, end)}; this.pairOptionData = pairOptionData;`, context);
  assert.equal(context.pairOptionData("b").html, "<row>4</row>");
  assert.equal(context.pairOptionData(""), null);
});

test("pair option data accepts feature lists and separates tags per view", () => {
  const context = syncHelpers();
  context.pair = { source: "PLEX", target: "TRAKT", order: 2, features: ["ratings", "history"] };
  const row = vm.runInContext('syncPairOptionData(pair, { tags: ["3 items"], menuTags: ["Step 1"] })', context);
  assert.deepEqual(row.html.match(/cw-sync-feature-dot \w+/g), ["cw-sync-feature-dot rt", "cw-sync-feature-dot hi"]);
  assert.match(row.html, /cw-sync-pair-order">2</);
  assert.match(row.html, /Step 1/);
  assert.match(row.selectedHtml, /3 items/);
  assert.doesNotMatch(row.selectedHtml, /cw-sync-pair-order|Step 1/);
  context.pair = { source: "PLEX", target: "TRAKT" };
  const bare = vm.runInContext("syncPairOptionData(pair)", context);
  assert.doesNotMatch(bare.html, /cw-sync-pair-order|cw-sync-feature-dot/);
});

test("per-user instances show the sync user name in pills", () => {
  const context = syncHelpers();
  const label = (id) => {
    context.id = id;
    return vm.runInContext('syncMenuInstanceLabel("PLEX", id)', context);
  };
  assert.equal(label("default~alice"), "alice");
  assert.equal(label("plex-P02~alice"), "P02 · alice");
  vm.runInContext('syncUserNames = { PLEX: { "default~alice": "Alice Smith" } }', context);
  assert.equal(label("default~alice"), "Alice Smith");
  assert.equal(label("default"), "");
});

test("logs, maintenance, support and episode groups use the shared pair rows", () => {
  for (const file of ["js/logs.js", "js/maintenance/index.js", "js/modals/support/index.js", "js/editor/episode-groups.js", "js/analyzer/index.js", "js/scheduler.js"]) {
    assert.match(read(file), /CW\?\.syncPairOptionData\?\.\(/, file);
  }
  assert.doesNotMatch(read("js/logs.js"), /\.\.\.pairs\]\.sort/);
  assert.doesNotMatch(read("js/modals/support/index.js"), /localeCompare/);
});

test("feature dots name each enabled feature and what it writes on hover", () => {
  const source = read("helpers/core.js");
  const start = source.indexOf("  const SYNC_FEATURE_DOT_CLASSES = {");
  const end = source.indexOf("  function syncMenuInstanceLabel(");
  const context = vm.createContext({
    escapeHtml: (value) => String(value).replace(/</g, "&lt;"),
    featureEnabled: (value) => value === true || !!value?.enable,
  });
  vm.runInContext(source.slice(start, end), context);
  const dots = (features) => {
    context.pair = { features };
    return vm.runInContext("syncFeatureDotsHTML(pair)", context);
  };
  const html = dots({
    history: { enable: true, add: true, remove: true },
    ratings: { enable: true, add: true },
    watchlist: { enable: false, add: true },
    collection: true,
  });
  assert.deepEqual(html.match(/cw-sync-feature-dot \w+/g), ["cw-sync-feature-dot rt", "cw-sync-feature-dot hi", "cw-sync-feature-dot co"]);
  assert.match(html, /title="Ratings: adds only"/);
  assert.match(html, /title="History: adds and removes"/);
  assert.match(html, /title="Collections enabled"/);
  assert.equal(dots({ watchlist: { enable: false } }), "");
});

test("schedule pills show the provider icon with the profile name", () => {
  const source = read("helpers/core.js");
  const start = source.indexOf("  function syncMenuInstanceLabel(");
  const end = source.indexOf("  function pairTitle(");
  const labels = { "plex:living": "Living room", "trakt:default": "My main Trakt" };
  const context = vm.createContext({
    META: {
      brandInfo: (provider) => (provider === "nope" ? { label: "Nope" } : { label: provider.toUpperCase(), icon: `/${provider}.svg` }),
      instanceLabel: (provider, id) => labels[`${provider}:${id}`] || (id === "default" ? "Default instance" : id),
    },
    providerLabel: (provider) => provider,
    escapeHtml: (value) => String(value).replace(/</g, "&lt;"),
  });
  vm.runInContext(source.slice(start, end), context);
  const names = (pair) => {
    context.pair = pair;
    return vm.runInContext("syncProfileRouteHTML(pair)", context).match(/cw-sync-provider-name">[^<]*/g).map((text) => text.split(">")[1]);
  };
  assert.deepEqual(names({ source: "plex", source_instance: "living", target: "trakt", target_instance: "default" }), ["Living room", "My main Trakt"]);
  assert.deepEqual(names({ source: "simkl", target: "mdblist", target_instance: "mdblist-P02" }), ["Default", "P02"]);
  assert.deepEqual(names({ source: "plex" }), ["Default"]);
  assert.deepEqual(names({ source: "nope" }), ["Nope", "Default"]);
  context.pair = { source: "plex", target: "trakt", mode: "two-way" };
  const html = vm.runInContext("syncProfileRouteHTML(pair)", context);
  assert.match(html, /title="PLEX · Default"/);
  assert.match(html, /alt="PLEX"/);
  assert.match(html, /↔/);
  assert.doesNotMatch(vm.runInContext("syncPairRouteHTML(pair)", context), />PLEX</);
});

test("dropdowns stay inside a keyboard-reduced and panned visual viewport", () => {
  const h = harness();
  Object.assign(h.window, {innerWidth:390, innerHeight:844,
    visualViewport:{width:300,height:180,offsetLeft:30,offsetTop:200}});
  const btn = {getBoundingClientRect:() => ({left:280,top:350,bottom:394,width:100})};
  const menu = {classList:{contains:() => false},style:{},scrollHeight:400,offsetHeight:400,
    getBoundingClientRect:() => ({width:350})};
  h.window.positionMenu({}, btn, menu);
  const left = Number.parseFloat(menu.style.left);
  const top = Number.parseFloat(menu.style.top);
  const width = Number.parseFloat(menu.style.width);
  const height = Number.parseFloat(menu.style.maxHeight);
  assert.ok(left >= 42);
  assert.ok(left + width <= 318);
  assert.ok(top >= 212);
  assert.ok(top + height <= 368);
  assert.ok(height > 0);
});
