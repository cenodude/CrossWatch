/* tests/hidden-providers.test.mjs */
/* CrossWatch - Hidden providers stay out of every provider list */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";
import './i18n-vm-setup.mjs';

const metaSource = readFileSync(new URL("../assets/helpers/provider-meta.js", import.meta.url), "utf8");
const coreSource = readFileSync(new URL("../assets/helpers/core.js", import.meta.url), "utf8");

function load() {
  const noop = () => {};
  const classList = {add: noop, remove: noop, toggle: noop, contains: () => false};
  const document = {
    getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], addEventListener: noop, dispatchEvent: noop,
    readyState: "complete", documentElement: {dataset: {}, classList}, body: {dataset: {}, classList},
    createElement: () => ({style: {}, classList, setAttribute: noop, appendChild: noop}),
  };
  const window = {CW: {}, addEventListener: noop, dispatchEvent: noop, location: {hash: "", pathname: "/", search: ""}, matchMedia: () => ({matches: false, addEventListener: noop})};
  window.window = window;
  class Event { constructor(type, init = {}) { this.type = type; Object.assign(this, init); } }
  const storage = {getItem: () => null, setItem: noop, removeItem: noop};
  const context = vm.createContext({
    window, document, location: window.location, console: {log: noop, warn: noop, error: noop, info: noop, debug: noop},
    setTimeout: () => 0, clearTimeout: noop, setInterval: () => 0, clearInterval: noop, queueMicrotask: noop, requestAnimationFrame: noop,
    MutationObserver: class { observe() {} disconnect() {} }, Event, CustomEvent: Event, AbortController, URL, URLSearchParams,
    fetch: () => new Promise(noop), navigator: {}, localStorage: storage, sessionStorage: storage,
  });
  vm.runInContext(metaSource, context);
  vm.runInContext(coreSource, context);
  return window;
}

test("Movie Paradise is hidden from every provider list, also when connected", () => {
  const window = load();
  const meta = window.CW.ProviderMeta;
  const connected = {movieparadise: {access_token: "token"}, trakt: {access_token: "token"}};

  assert.equal(meta.isHidden("MOVIEPARADISE"), true);
  assert.equal(meta.sectionId("MOVIEPARADISE"), "");
  assert.equal(meta.order.includes("MOVIEPARADISE"), false);
  assert.equal(meta.authProviders().some((info) => info.key === "MOVIEPARADISE"), false);
  assert.equal(meta.statusProviders().some((info) => info.key === "MOVIEPARADISE"), false);
  assert.equal(meta.label("MOVIEPARADISE"), "Movie Paradise");

  const configured = [...window.getConfiguredProviders(connected)];
  assert.equal(configured.includes("MOVIEPARADISE"), false);
  assert.equal(configured.includes("TRAKT"), true);
});
