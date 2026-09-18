/* CrossWatch - Editor history viewings are grouped per title */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

function editor() {
  const context = vm.createContext({window: {}});
  for (const file of ["../assets/js/editor/rows.js", "../assets/js/editor/table-controller.js"]) {
    vm.runInContext(readFileSync(new URL(file, import.meta.url), "utf8"), context);
  }
  return context.window.CW.Editor;
}

const row = (rid, key, watched_at) => ({_rid: rid, key, raw: {watched_at}});

test("Viewing keys resolve to their title key", () => {
  const {Rows} = editor();
  assert.equal(Rows.viewingBaseKey("tmdb:862@1735725600"), "tmdb:862");
  assert.equal(Rows.viewingBaseKey("tmdb:22#s01e02@id:abc"), "tmdb:22#s01e02");
  assert.equal(Rows.viewingBaseKey("tmdb:862"), "");
});

test("History rows group per title with the newest watch first", () => {
  const {TableController} = editor();
  const rows = [
    row(1, "tmdb:862@1735725600", "2025-01-01T10:00:00Z"),
    row(2, "tmdb:603", "2025-01-02T10:00:00Z"),
    row(3, "tmdb:862@1740823200", "2025-03-01T10:00:00Z"),
    row(4, "tmdb:604@1740823200", "2025-03-01T10:00:00Z"),
  ];
  const units = TableController.viewingUnits(rows, {state: {kind: "history"}});
  assert.equal(units.length, 3);
  assert.equal(units[0].base, "tmdb:862");
  assert.deepEqual([...units[0].rows].map(r => r._rid), [3, 1]);
  assert.equal(units[1].row._rid, 2);
  assert.equal(units[2].row._rid, 4);
});

test("Other features never group rows", () => {
  const {TableController} = editor();
  const rows = [row(1, "tmdb:862@1735725600"), row(2, "tmdb:862@1740823200")];
  const units = TableController.viewingUnits(rows, {state: {kind: "ratings"}});
  assert.deepEqual([...units].map(u => u.row._rid), [1, 2]);
});
