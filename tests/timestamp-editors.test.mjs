/* tests/timestamp-editors.test.mjs */
/* CrossWatch - Timestamp precision and native seconds editor tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const read = path => readFileSync(new URL(path, import.meta.url), "utf8");
const element = tag => ({ tag, children: [], value: "", focus() {}, setAttribute(name, value) { this[name] = value; }, append(...nodes) { this.children.push(...nodes); }, appendChild(node) { this.children.push(node); } });

function editor() {
  const window = {};
  vm.runInNewContext(read("../assets/js/editor/datetime.js"), { window, document: { createElement: element } });
  vm.runInNewContext(read("../assets/js/editor/extra-editors.js"), { window, document: { createElement: element } });
  return window.CW.Editor;
}

const descendants = node => [node, ...node.children.flatMap(descendants)];

for (const [method, field] of [["openHistoryEditor", "watched_at"], ["openCollectionEditor", "collected_at"], ["openProgressEditor", "progress_at"], ["openRatingEditor", "rated_at"]]) {
  test(`${field} retains seconds in the time input and allows an explicit zero`, () => {
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
    const time = nodes.find(node => node.type === "datetime-local");
    assert.equal(time.value, app.DateTime.localInput(row.raw[field]));
    assert.equal(time.step, 1);
    assert.ok(nodes.filter(node => node.tag === "details").every(node => !node.open));
    time.value = app.DateTime.localInput("2026-11-04T02:32:25.000Z");
    const save = actions.find(action => action.label === "Save" || action.label === "Save date");
    save.onClick();
    assert.equal(row.raw[field], "2026-11-04T02:32:25.000Z");
    time.value = time.value.replace(/:25$/, ":00");
    save.onClick();
    assert.equal(row.raw[field], "2026-11-04T02:32:00.000Z");
    if (field === "rated_at") {
      time.value = time.value.replace(/:00$/, ":59");
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
  for (const [key, value] of Object.entries({ date: vm.runInContext("localInput(sheet.item.date)", context), title: "Heat", year: "1995" })) fields[key] = { value };
  assert.equal(JSON.stringify(vm.runInContext("formChanges()", context)), "{}");
  fields.date.value = fields.date.value.replace("2026-11-03", "2026-11-04");
  assert.equal(vm.runInContext("formChanges().date", context), "2026-11-04T02:32:25.000Z");
  fields.date.value = fields.date.value.replace(/:25$/, ":00");
  assert.equal(vm.runInContext("formChanges().date", context), "2026-11-04T02:32:00.000Z");
  fields.date.value = vm.runInContext("localInput(sheet.item.date)", context).replace(/:25$/, ":00");
  assert.equal(vm.runInContext("formChanges().date", context), "2026-11-03T02:32:00.000Z");
  item.date = "2026-11-03T02:32:00.123Z";
  fields.date.value = vm.runInContext("localInput(sheet.item.date)", context).slice(0, 16);
  assert.equal(JSON.stringify(vm.runInContext("formChanges()", context)), "{}");
});

test("Tracker watch logging sends the selected seconds", async () => {
  const source = read("../assets/js/tracker.js");
  const calls = [];
  const context = vm.createContext({
    sheet: { log: { pick: { type: "movie", title: "Heat", ids: { tmdb: "949" } }, target: "history", when: "2026-11-03T02:32:47" } },
    state: { instance: "default" }, renderSheet() {},
    getJSON: async (url, body) => { calls.push(body); return {}; },
    afterWrite() {}, writeError: String,
  });
  const start = source.indexOf("  async function logWatchTimes(");
  const end = source.indexOf("\n  async function ", source.indexOf("  async function saveLog()") + 1);
  vm.runInContext(source.slice(start, end), context);
  await vm.runInContext("saveLog()", context);
  assert.equal(new Date(calls[0].items[0].watched_at).getSeconds(), 47);
});

test("Editor and Tracker use the same local timestamp and preserve unchanged values", () => {
  const app = editor();
  const source = read("../assets/js/tracker.js");
  const context = vm.createContext({ pad: n => String(n).padStart(2, "0") });
  vm.runInContext(source.slice(source.indexOf("  function localInput("), source.indexOf("  function artUrl(")), context);
  for (const stamp of ["2026-07-05T00:07:25Z", "2026-11-05T14:05:00Z", "2026-10-25T01:30:25.123Z"]) {
    context.stamp = stamp;
    const input = app.DateTime.localInput(stamp);
    assert.equal(input, vm.runInContext("localInput(stamp)", context));
    assert.equal(app.DateTime.dateTimeInputToIso(input, stamp), stamp);
  }
  assert.equal(app.DateTime.dateTimeInputToIso(""), null);
  assert.equal(app.DateTime.dateTimeInputToIso("invalid"), null);
});

test("inline Editor dates open the picker and stage valid changes without a popup", () => {
  const app = editor();
  for (const [kind, field] of [["history", "watched_at"], ["collection", "collected_at"]]) {
    const row = { raw: { [field]: "2026-11-03T02:32:25.000Z" } };
    let saves = 0, opens = 0;
    const input = app.ExtraEditors.createDateTimeEditor(row, {
      state: { kind }, finishExtraChange: () => { saves++; },
    });
    input.checkValidity = () => true;
    input.showPicker = () => { opens++; };
    input.onclick();
    assert.equal(opens, 1);
    input.onchange();
    assert.equal(saves, 0);
    input.value = app.DateTime.localInput("2026-11-04T02:32:59.000Z");
    input.onchange();
    assert.equal(row.raw[field], "2026-11-04T02:32:59.000Z");
    assert.equal(saves, 1);
    input.checkValidity = () => false;
    input.value = "";
    input.onchange();
    assert.equal(saves, 1);
    input.checkValidity = () => true;
    input.onchange();
    assert.equal(row.raw[field], null);
    assert.equal(saves, 2);
  }
});

test("Tracker release dates use movie metadata and each episode air date", async () => {
  const source = read("../assets/js/tracker.js");
  let response = { result: { detail: { release_date: "2025-08-20" } } };
  const context = vm.createContext({ getJSON: async () => response });
  vm.runInContext(source.slice(source.indexOf("  async function logWatchTimes("), source.indexOf("  async function saveLog()")), context);
  context.log = { whenMode: "release", pick: { type: "movie", ids: { tmdb: "1" } } };
  context.items = [{ type: "movie" }];
  let result = await vm.runInContext("logWatchTimes(log, items)", context);
  assert.equal(result[0].watched_at, "2025-08-20T12:00:00Z");
  context.log = { whenMode: "release", pick: { type: "show", tmdb: "2" }, season: 1 };
  context.items = [{ type: "episode", episode: 1 }, { type: "episode", episode: 2 }];
  response = { episodes: [{ episode: 1, air_date: "2025-01-01" }, { episode: 2, air_date: "2025-01-08" }] };
  result = await vm.runInContext("logWatchTimes(log, items)", context);
  assert.equal(result[0].watched_at, "2025-01-01T12:00:00Z");
  assert.equal(result[1].watched_at, "2025-01-08T12:00:00Z");
  response.episodes.pop();
  await assert.rejects(vm.runInContext("logWatchTimes(log, items)", context), /No release date/);
  response.episodes.push({ episode: 2, air_date: "2999-01-01" });
  await assert.rejects(vm.runInContext("logWatchTimes(log, items)", context), /future/);
  context.log.whenMode = "today";
  const before = Date.now();
  result = await vm.runInContext("logWatchTimes(log, items)", context);
  assert.ok(Date.parse(result[0].watched_at) >= before);
  assert.equal(result[0].watched_at, result[1].watched_at);
  context.log.whenMode = "custom";
  context.log.when = "";
  await assert.rejects(vm.runInContext("logWatchTimes(log, items)", context), /valid watch date/);
});
