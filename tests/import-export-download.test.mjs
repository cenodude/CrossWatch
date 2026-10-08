/* tests/import-export-download.test.mjs */
/* CrossWatch - Export download shortcut regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/import-export/index.js", import.meta.url), "utf8");

function fixture() {
  const nodes = new Map();
  const requests = [], downloads = [];
  const $ = (id) => {
    if (!nodes.has(id)) nodes.set(id, { id: id.slice(1), dataset: {}, selectedOptions: [{ textContent: "Default" }], scrollIntoView() {} });
    return nodes.get(id);
  };
  const context = vm.createContext({
    $, host: {}, mode: "export", busy: false, loading: false, data: {}, count: 3,
    all: false, selected: new Set(["movie:1", "episode:2", "movie:3"]), excluded: new Set(),
    selectedCount: () => context.count, selectableTotal: () => 3,
    isImport: () => context.mode === "import", number: String, icon: (name) => name,
    parameters: () => ({ provider: "TRAKT", provider_instance: "default", feature: "history", format: "csv", q: "test" }),
    cancelRead() {}, notice() {},
    progress() { context.busy = true; context.updateSelection(); },
    finishProgress() { context.busy = false; context.updateSelection(); },
    on(_host, _event, handler) { context.click = handler; },
    fetch: async (url, options) => {
      requests.push({ url, body: JSON.parse(options.body) });
      return { ok: true, blob: async () => ({ type: "text/csv", size: 100 }), headers: { get: () => 'attachment; filename="history.csv"' } };
    },
    URL: { createObjectURL: () => "blob:export", revokeObjectURL() {} },
    document: { body: { appendChild() {} }, createElement: () => ({ click() { downloads.push(this.download); }, remove() {} }) },
    setTimeout() {},
  });
  const selection = source.slice(source.indexOf("    function updateSelection()"), source.indexOf("    function render()"));
  const action = source.slice(source.indexOf("    async function apply()"), source.indexOf('    on(host,"change"'));
  vm.runInContext(selection + action, context);
  context.updateSelection();
  return { context, $, requests, downloads, click(id) {
    context.click({ target: { closest: () => $(id) } });
  } };
}

test("export shortcut is an accessible, initially disabled button", () => {
  assert.match(source, /<button type="button" id="ie-download"[^>]*aria-label="Download selected items"[^>]*disabled>/);
});

test("both download buttons export the same selected items and filters", async () => {
  const results = [];
  for (const id of ["#ie-download", "#ie-apply"]) {
    const f = fixture();
    f.click(id);
    assert.equal(f.$("#ie-download").disabled, true);
    assert.equal(f.$("#ie-apply").disabled, true);
    f.click(id);
    await new Promise(setImmediate);
    assert.equal(f.requests.length, 1);
    assert.deepEqual(f.downloads, ["history.csv"]);
    assert.equal(f.$("#ie-download").disabled, false);
    results.push(f.requests[0]);
  }
  assert.deepEqual(results[0], results[1]);
  assert.deepEqual(results[0].body.row_ids, ["movie:1", "episode:2", "movie:3"]);
  assert.equal(results[0].url, "/api/export/file");
  assert.equal(results[0].body.q, "test");
});

test("shortcut respects empty, loading, busy and non-export states", () => {
  for (const state of [{ count: 0 }, { loading: true }, { busy: true }, { data: null }, { mode: "import" }, { mode: "recovery" }]) {
    const f = fixture();
    Object.assign(f.context, state);
    f.context.updateSelection();
    assert.equal(f.$("#ie-download").disabled, true);
    f.click("#ie-download");
    assert.equal(f.requests.length, 0);
  }
});

test("shortcut retains exclusions when exporting all matching items", async () => {
  const f = fixture();
  f.context.all = true;
  f.context.selected.clear();
  f.context.excluded.add("movie:3");
  f.click("#ie-download");
  await new Promise(setImmediate);
  assert.equal(f.requests[0].body.mode, "all");
  assert.deepEqual(f.requests[0].body.excluded_row_ids, ["movie:3"]);
});
