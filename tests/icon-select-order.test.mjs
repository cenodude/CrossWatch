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
    set innerHTML(value) { this.children = []; },
    appendChild(child) { this.children.push(child); },
    setAttribute() {},
    addEventListener(type, handler) { this[type] = handler; },
  };
}

function harness() {
  const window = {};
  const document = { readyState: "loading", addEventListener() {}, createElement: element };
  const context = vm.createContext({ window, document, Event });
  vm.runInContext(read("helpers/icon-select.js").replace("window.CW.IconSelect = {", "window.buildMenu = buildMenu; window.CW.IconSelect = {"), context);
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
