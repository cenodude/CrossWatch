/* tests/tracker-import.test.mjs */
/* CrossWatch - Tracker import choices and destination handling */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/js/tracker.js", import.meta.url), "utf8");
const functions = source.slice(source.indexOf("  function importPick("), source.indexOf("  function noteDraft("));

function setup({canCreate = true, failImport = false} = {}) {
  const calls = [];
  const context = vm.createContext({
    sheet: {mode: "", busy: false},
    state: {instance: "desk", picked: new Set()},
    FEATURES: [{id: "history", label: "History", icon: "history"}],
    esc: value => String(value ?? "").replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;"),
    icon: () => "", symbol: () => "", sheetHead: () => "", emptyState: value => value,
    renderSheet() {}, refresh: async () => {}, writeError: error => error.message,
    saveView() {}, toast: {show() {}}, window: {dispatchEvent() {}, location:{hash:""}}, CustomEvent: class {},
    openSheet(mode, data) { Object.assign(context.sheet, {mode, busy: false}, data); },
    closeSheet() { context.sheet.mode = ""; },
    async getJSON(url, body) {
      calls.push({url, body});
      if (url.endsWith("/sources")) return {
        sources: [
          {id: "PLEX", label: "Plex", features: ["history"], instances: [{id: "default", label: "Default"}]},
          {id: "CROSSWATCH", label: "CrossWatch", features: ["history"], instances: [{id: "desk", label: "Desk"}]},
        ],
        instances: [{id: "default", label: "Default"}, {id: "desk", label: "Desk"}], can_create: canCreate,
      };
      if (url.endsWith("/next")) return {id: "new-tracker"};
      if (failImport) { failImport = false; throw new Error("Import unavailable"); }
      if (url.endsWith("/preview")) return {provider_instance: body.provider_instance, session:{id:"review-1"}};
      return {provider_instance: body.provider_instance, job: {state: "running"}};
    },
  });
  vm.runInContext(functions, context);
  return {context, calls};
}

test("import defaults to merging into the current tracker without creating an instance", async () => {
  const {context: app, calls} = setup();
  await app.openImport();
  assert.equal(app.sheet.imp.target, "desk");
  const html = app.renderImport();
  assert.match(html, /value="merge" selected/);
  assert.match(html, /Merge into an existing tracker/);
  assert.match(html, /Import into a new tracker/);
  assert.match(html, /existing ratings and playback progress may be updated/);
  assert.doesNotMatch(html, /data-trk-imp="name"/);
  await app.startImport();
  assert.equal(calls.length, 2);
  assert.equal(calls[1].url, "/api/tracker/import/preview");
  assert.equal(app.window.location.hash, "interactive_sync?session=review-1");
  assert.equal(calls[1].body.provider_instance, "desk");
});

test("new tracker choice preserves the merge destination and creates only once on retry", async () => {
  const {context: app, calls} = setup({failImport: true});
  await app.openImport();
  const imp = app.sheet.imp;
  imp.review = false;
  app.setImportMode(imp, "new");
  imp.name = "Plex comparison";
  const html = app.renderImport();
  assert.match(html, /value="new" selected/);
  assert.match(html, /Starts with an empty tracker/);
  assert.match(html, /New tracker name/);
  assert.doesNotMatch(html, /data-trk-imp="target"/);
  app.setImportMode(imp, "merge");
  assert.equal(imp.target, "desk");
  app.setImportMode(imp, "new");
  await app.startImport();
  assert.equal(imp.target, "new-tracker");
  assert.equal(app.sheet.error, "Import unavailable");
  assert.equal(app.sheet.busy, false);
  await app.startImport();
  const creations = calls.filter(call => call.url.endsWith("/next"));
  const imports = calls.filter(call => call.url === "/api/tracker/import");
  assert.equal(creations.length, 1);
  assert.equal(creations[0].body.label, "Plex comparison");
  assert.equal(imports.length, 2);
  assert.ok(imports.every(call => call.body.provider_instance === "new-tracker"));
});

test("switching back to merge cannot select the CrossWatch source as its destination", async () => {
  const {context: app} = setup();
  await app.openImport();
  const imp = app.sheet.imp;
  app.setImportMode(imp, "new");
  app.setImportSource(imp, "CROSSWATCH|desk");
  app.setImportMode(imp, "merge");
  assert.equal(imp.target, "default");
  imp.instances = [{id: "desk", label: "Desk"}];
  app.setImportSource(imp, "CROSSWATCH|desk");
  assert.equal(imp.target, "new");
  assert.match(app.renderImport(), /value="merge" disabled/);
});

test("users without creation permission cannot select or submit a new tracker", async () => {
  const {context: app, calls} = setup({canCreate: false});
  await app.openImport();
  const imp = app.sheet.imp;
  assert.doesNotMatch(app.renderImport(), /Import into a new tracker/);
  app.setImportMode(imp, "new");
  assert.equal(imp.target, "desk");
  imp.instances = [{id: "desk", label: "Desk"}];
  app.setImportSource(imp, "CROSSWATCH|desk");
  assert.equal(imp.target, "");
  assert.match(app.renderImport(), /No other tracker is available/);
  await app.startImport();
  imp.target = "new";
  await app.startImport();
  assert.equal(calls.length, 1);
});


test("unchecking review retains the background import flow", async () => {
  const {context:app,calls} = setup();
  await app.openImport();
  app.sheet.imp.review = false;
  await app.startImport();
  assert.equal(calls[1].url, "/api/tracker/import");
  assert.equal(app.state.imp.state, "running");
  assert.equal(app.window.location.hash, "");
});


test("new tracker previews and failed retries never create or switch Tracker instances", async () => {
  const {context:app,calls} = setup({failImport:true});
  await app.openImport();
  app.setImportMode(app.sheet.imp,"new");
  app.sheet.imp.name="Plex comparison";
  assert.match(app.renderImport(),/created only when you apply the review/);
  await app.startImport();
  assert.equal(app.sheet.imp.target,"new");
  assert.equal(app.state.instance,"desk");
  assert.equal(app.sheet.error,"Import unavailable");
  await app.startImport();
  assert.equal(app.state.instance,"desk");
  assert.equal(app.window.location.hash,"interactive_sync?session=review-1");
  assert.equal(calls.filter(call=>call.url.endsWith("/next")).length,0);
  const previews=calls.filter(call=>call.url.endsWith("/preview"));
  assert.equal(previews.length,2);
  assert(previews.every(call=>call.body.provider_instance==="new" && call.body.name==="Plex comparison"));
});
