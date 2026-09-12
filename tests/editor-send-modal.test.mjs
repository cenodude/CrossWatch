/* CrossWatch - Send/Remove modal behavior without provider mutations */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/editor/send-modal.js", import.meta.url), "utf8");
function deferred() {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return {promise, resolve};
}
function fixture(kind = "watchlist") {
  class Element {
    constructor(selector = "") {
      this.selector = selector; this.innerHTML = ""; this.textContent = "";
      this.dataset = {}; this.attrs = {}; this.isConnected = true;
      const classes = new Set();
      this.classList = {add: x => classes.add(x), remove: x => classes.delete(x),
        toggle: (x, on) => on ? classes.add(x) : classes.delete(x)};
    }
    setAttribute(k, v) { this.attrs[k] = v; }
    getAttribute(k) { return this.attrs[k]; }
    closest(selector) { return this.selector === selector ? this : null; }
    remove() { this.isConnected = false; }
    addEventListener(name, fn) { this[name] = fn; }
    scrollTo() {}
    appendChild() {}
  }
  const nodes = new Map();
  const node = selector => {
    if (!nodes.has(selector)) nodes.set(selector, new Element(selector));
    return nodes.get(selector);
  };
  const modes = ["add", "remove"].map(operation => {
    const button = new Element("[data-operation]"); button.dataset.operation = operation; return button;
  });
  const summary = Array.from({length: 3}, () => new Element());
  const shell = new Element();
  shell.querySelector = selector => selector === ".cw-editor-send-history" ? null : node(selector);
  shell.querySelectorAll = selector => {
    if (selector === ".cw-editor-send-data-grid b") return summary;
    if (selector === "[data-operation]") return modes;
    const providers = [...node(".cw-editor-send-list").innerHTML.matchAll(/data-provider-key="([^"]+)"/g)].map(match => {
      const button = node(match[1]); button.selector = "[data-provider-key]";
      button.setAttribute("data-provider-key", match[1]); return button;
    });
    return selector === "[data-provider-key]" ? providers : [...providers, ...modes, node("[data-send-close]")];
  };
  let mounted = false, cleared = 0, refreshed = 0;
  const requests = [];
  const provider = {provider: "TRAKT", instance: "family", display: "Trakt (Family)", count: 1, matched: 1, [kind + "_enabled"]: true};
  let preview = () => ({providers: [provider], preview_id: "preview-1"});
  let send = () => ({ok: true, operation: "remove", attempted: 1, confirmed: 1,
    results: [{...provider, ok: true, result: {confirmed: 1}}]});
  const context = vm.createContext({Element, setTimeout: () => 1, clearTimeout() {},
    CustomEvent: class {}, localStorage: {getItem: () => "[]", setItem() {}},
    window: {requestAnimationFrame: fn => fn(), dispatchEvent() {}},
    document: {getElementById: () => mounted ? shell : null, createElement: () => shell,
      body: {appendChild() {mounted = true;}}},
  });
  vm.runInContext(source, context);
  const ctx = {
    state: {kind, snapshot: "PLEX", instance: "second", mappingPair: "pair-1"},
    selectedRowsForSend: () => [{type: "movie", ids: {tmdb: "1"}}], rowToSendItem: row => row,
    isProviderPickerSource: () => true, setStatusSticky() {},
    clearSelection() {cleared++;}, async loadState() {refreshed++;},
    async fetchJSON(url, options) {
      const body = options?.body ? JSON.parse(options.body) : null;
      requests.push({url, body});
      if (url.endsWith("/preview")) return preview();
      if (url.endsWith("/send")) return send();
      return {providers: [provider]};
    },
  };
  return {node, requests, modes, provider, open: () => context.window.CW.Editor.SendModal.open(ctx),
    click: target => shell.click({target}), preview: fn => {preview = fn;}, send: fn => {send = fn;},
    get cleared() {return cleared;}, get refreshed() {return refreshed;}};
}

test("Remove requires explicit target selection and confirmation; results preserve instance and preview scope", async () => {
  const f = fixture(); await f.open();
  assert.equal(f.node("[data-send-submit]").disabled, false); // existing single-target Add behavior
  await f.click(f.modes[1]);
  assert.equal(f.node("[data-send-submit]").disabled, true);
  assert.match(f.node(".cw-editor-send-list").innerHTML, /Trakt \(Family\)/);
  await f.click(f.node("TRAKT:family"));
  await f.click(f.node("[data-send-submit]"));
  assert.equal(f.requests.filter(r => r.url.endsWith("/send")).length, 0);
  assert.match(f.node("[data-send-submit]").innerHTML, /Confirm removal/);
  await f.click(f.node("[data-send-submit]"));
  const sent = f.requests.find(r => r.url.endsWith("/send")).body;
  assert.deepEqual(sent.providers, [{provider: "TRAKT", instance: "family"}]);
  assert.equal(sent.preview_id, "preview-1");
  assert.equal(sent.confirmed, true);
  assert.equal(sent.pair_id, "pair-1");
  assert.equal(sent.source_instance, "second");
  assert.match(f.node("[data-send-status]").innerHTML, /Removal complete/);
  assert.equal(f.cleared, 1); assert.equal(f.refreshed, 1);
  assert.equal(f.node("[data-send-submit]").disabled, true);
});

test("Changing the target resets removal confirmation", async () => {
  const f = fixture(); await f.open(); await f.click(f.modes[1]);
  await f.click(f.node("TRAKT:family")); await f.click(f.node("[data-send-submit]"));
  await f.click(f.node("TRAKT:family")); await f.click(f.node("TRAKT:family"));
  await f.click(f.node("[data-send-submit]"));
  assert.equal(f.requests.filter(r => r.url.endsWith("/send")).length, 0);
});

test("A late removal preview cannot replace the Add targets after switching modes", async () => {
  const f = fixture(); await f.open();
  const pending = deferred(); f.preview(() => pending.promise);
  const removing = f.click(f.modes[1]);
  assert.equal(f.node("[data-send-submit]").disabled, true);
  await f.click(f.modes[0]);
  pending.resolve({providers: [], preview_id: "stale"}); await removing;
  assert.match(f.node(".cw-editor-send-list").innerHTML, /Trakt \(Family\)/);
  await f.click(f.node("[data-send-submit]"));
  const sent = f.requests.find(r => r.url.endsWith("/send")).body;
  assert.equal(sent.operation, "add"); assert.equal(sent.preview_id, undefined);
});

test("Removal failure retains the selection, shows the error and requires a fresh preview", async () => {
  const f = fixture(); await f.open(); await f.click(f.modes[1]);
  f.send(() => {throw new Error("Provider unavailable");});
  await f.click(f.node("TRAKT:family"));
  await f.click(f.node("[data-send-submit]")); await f.click(f.node("[data-send-submit]"));
  assert.match(f.node("[data-send-status]").innerHTML, /Removal failed.*Provider unavailable/s);
  assert.equal(f.cleared, 0);
  assert.equal(f.requests.filter(r => r.url.endsWith("/preview")).length, 2);
  assert.equal(f.node("[data-send-submit]").disabled, true);
});

test("History removal describes whole watched status and displays rejection of dated rows", async () => {
  const f = fixture("history"); await f.open();
  f.preview(() => {throw new Error("Individual watch dates cannot be removed here. Select a watched-status row.");});
  await f.click(f.modes[1]);
  assert.match(f.node(".cw-editor-send-warning div").textContent, /watched status and all recorded viewings/);
  assert.match(f.node("[data-send-status]").innerHTML, /Individual watch dates cannot be removed here/);
  assert.equal(f.node("[data-send-submit]").disabled, true);
  assert.equal(f.requests.filter(r => r.url.endsWith("/send")).length, 0);
});
