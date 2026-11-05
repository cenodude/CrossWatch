/* tests/timestamp-editors.test.mjs */
/* CrossWatch - Timestamp precision and collapsed seconds editor tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const read = path => readFileSync(new URL(path, import.meta.url), "utf8");
const element = tag => ({ tag, children: [], value: "", append(...nodes) { this.children.push(...nodes); }, appendChild(node) { this.children.push(node); } });

function editor() {
  const window = {};
  vm.runInNewContext(read("../assets/js/editor/datetime.js"), { window, document: { createElement: element } });
  vm.runInNewContext(read("../assets/js/editor/extra-editors.js"), { window, document: { createElement: element } });
  return window.CW.Editor;
}

const descendants = node => [node, ...node.children.flatMap(descendants)];

for (const [method, field] of [["openHistoryEditor", "watched_at"], ["openCollectionEditor", "collected_at"], ["openProgressEditor", "progress_at"], ["openRatingEditor", "rated_at"]]) {
  test(`${field} retains hidden seconds and allows an explicit zero`, () => {
    const app = editor();
    const row = { raw: { [field]: "2026-11-03T04:32:25+02:00", rating: 8 } };
    const pop = element("div");
    const actions = [];
    const ctx = {
      openPopup: (_, render) => render(pop, () => {}),
      appendPopupTitle() {},
      appendPopupActions: (_, buttons) => actions.push(...buttons),
      finishExtraChange() {},
    };
    const create = element;
    const document = { createElement: tag => ({ ...create(tag), focus() {} }) };
    const window = { CW: { Editor: app } };
    vm.runInNewContext(read("../assets/js/editor/extra-editors.js"), { window, document });
    window.CW.Editor.ExtraEditors[method](row, null, null, ctx);
    const nodes = descendants(pop);
    const select = nodes.find(node => node.tag === "select");
    assert.equal(select.value, "25");
    assert.equal(select.children.length, 60);
    assert.ok(nodes.filter(node => node.tag === "details").every(node => !node.open));
    const date = nodes.find(node => node.type === "date");
    date.value = "2026-11-04";
    const save = actions.find(action => action.label === "Save" || action.label === "Save date");
    save.onClick();
    assert.equal(row.raw[field], "2026-11-04T02:32:25.000Z");
    select.value = "0";
    save.onClick();
    assert.equal(row.raw[field], "2026-11-04T02:32:00.000Z");
    if (field === "rated_at") {
      select.value = "59";
      nodes.find(node => node.tag === "button" && node.textContent === "9").onclick();
      assert.equal(row.raw.rating, 9);
      assert.equal(row.raw.rated_at, "2026-11-04T02:32:59.000Z");
    }
  });
}

test("Tracker date edits preserve seconds and untouched timestamps", () => {
  const source = read("../assets/js/tracker.js");
  const fields = {};
  const item = { date: "2026-11-03T02:32:25.123Z", title: "Heat", year: 1995 };
  const form = {
    querySelector: selector => fields[selector.match(/="(.*?)"/)[1]] ?? null,
    querySelectorAll: () => [],
  };
  const context = vm.createContext({ sheet: { item, el: { querySelector: () => form } }, isChild: () => false, pad: n => String(n).padStart(2, "0") });
  vm.runInContext(source.slice(source.indexOf("  function localInput("), source.indexOf("  function artUrl(")) + source.slice(source.indexOf("  function formChanges("), source.indexOf("  async function afterWrite(")), context);
  for (const [key, value] of Object.entries({ date: vm.runInContext("localInput(sheet.item.date)", context), seconds: "25", title: "Heat", year: "1995" })) fields[key] = { value };
  assert.equal(JSON.stringify(vm.runInContext("formChanges()", context)), "{}");
  fields.date.value = fields.date.value.replace("2026-11-03", "2026-11-04");
  assert.equal(vm.runInContext("formChanges().date", context), "2026-11-04T02:32:25.000Z");
  fields.seconds.value = "0";
  assert.equal(vm.runInContext("formChanges().date", context), "2026-11-04T02:32:00.000Z");
  fields.date.value = vm.runInContext("localInput(sheet.item.date)", context);
  assert.equal(vm.runInContext("formChanges().date", context), "2026-11-03T02:32:00.000Z");
});

test("Tracker watch logging sends the selected seconds", async () => {
  const source = read("../assets/js/tracker.js");
  const calls = [];
  const context = vm.createContext({
    sheet: { log: { pick: { type: "movie", title: "Heat", ids: { tmdb: "949" } }, target: "history", when: "2026-11-03T02:32", seconds: 47 } },
    state: { instance: "default" }, renderSheet() {},
    getJSON: async (url, body) => { calls.push(body); return {}; },
    afterWrite() {}, writeError: String,
  });
  const start = source.indexOf("  async function saveLog()");
  const end = source.indexOf("\n  async function ", start + 1);
  vm.runInContext(source.slice(start, end), context);
  await vm.runInContext("saveLog()", context);
  assert.equal(new Date(calls[0].items[0].watched_at).getSeconds(), 47);
});
