/* tests/tracker-notifications.test.mjs */
/* CrossWatch - Tracker metadata check notification tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";

const script = readFileSync(new URL("../assets/js/tracker-notifications.js", import.meta.url), "utf8");
const flush = () => new Promise(resolve => setImmediate(resolve));

async function setup(role = "admin", tracker = "") {
  const events = {}, sources = [], timers = new Map();
  let nextTimer = 0, requests = 0, code = 200;
  let job = null, migration = null, imp = null;
  const document = {hidden: false, documentElement: {dataset: {cwRole: role, ...(tracker ? {cwPermTracker: tracker} : {})}}, addEventListener: (name, fn) => { events[name] = fn; }};
  const dispatched = [];
  const window = {CW: {Notifications: {setSource: (name, source) => sources.push({name, ...source})}},
    addEventListener: (name, fn) => { events[name] = fn; }, dispatchEvent: event => dispatched.push(event.type)};
  vm.runInNewContext(script, {window, document, AbortController, CustomEvent: class { constructor(type) { this.type = type; } },
    setTimeout: (fn, ms) => { timers.set(++nextTimer, {fn, ms}); return nextTimer; },
    clearTimeout: id => timers.delete(id),
    fetch: async (url, options) => {
      requests++;
      assert.equal(url, "/api/tracker/check/current");
      assert.equal(options.credentials, "same-origin");
      return {ok: code === 200, status: code, json: async () => ({ok: true, job, migration, import: imp})};
    },
  });
  await flush();
  return {timers, dispatched, requests: () => requests, latest: () => sources.at(-1),
    async publish(value, status = 200, move = null, run = null) { job = value; migration = move; imp = run; code = status; await events["cw:tracker-check-changed"](); },
  };
}

test("a running check reports progress and polls quickly without a new notification per step", async () => {
  const app = await setup();
  const job = {id: "c1", instance: "default", label: "Default", state: "running", done: 120, total: 1742, started_at: 10};
  await app.publish(job);
  const first = app.latest().items[0];
  assert.equal(app.latest().name, "tracker-check");
  assert.equal(app.latest().scope, "account");
  assert.match(first.title, /Metadata check - running/);
  assert.match(first.detail, /^120 of 1[.,]742 records checked\.$/);
  assert.equal(first.href, "/#tracker");
  assert.ok([...app.timers.values()].some(timer => timer.ms === 3000));
  await app.publish({...job, done: 900});
  assert.equal(app.latest().items[0].revision, first.revision);
});

test("a finished check links straight to the records that need attention", async () => {
  const app = await setup();
  await app.publish({id: "c2", instance: "CW-P01", label: "Desk", state: "completed", attention: 12, finished_at: 20});
  const item = app.latest().items[0];
  assert.match(item.title, /needs attention \(Desk\)/);
  assert.match(item.detail, /12 records need attention/);
  assert.equal(item.href, "/#tracker?attention=1");
  assert.equal(item.icon, "error_outline");
  await app.publish({id: "c3", instance: "default", state: "completed", attention: 0, finished_at: 30});
  assert.match(app.latest().items[0].detail, /Every record matches TMDB/);
  assert.equal(app.latest().items[0].href, "/#tracker");
});

test("stopped checks explain why", async () => {
  const app = await setup();
  await app.publish({id: "c4", state: "failed", error: "tmdb_key_invalid"});
  assert.match(app.latest().items[0].detail, /rejected the API key/);
  await app.publish({id: "c4", state: "interrupted"});
  assert.match(app.latest().items[0].detail, /restarted/);
  await app.publish({id: "c5", state: "cancelled"});
  assert.match(app.latest().items[0].title, /cancelled/);
});

test("managed users never ask for the check", async () => {
  const app = await setup("user");
  assert.equal(app.requests(), 0);
  assert.equal(app.latest().items.length, 0);
});

test("the tracker move shows its progress, then where the old files went", async () => {
  const app = await setup();
  const move = {id: "m1", state: "running", instance: "default", instance_index: 1, instance_count: 2, files_done: 312, files_total: 1200, bytes_done: 250e6, bytes_total: 1000e6, started_at: 5};
  await app.publish(null, 200, move);
  const item = app.latest().items[0];
  assert.match(item.title, /Tracker upgrade - running/);
  assert.match(item.detail, /312 of 1[.,]200 files \(250 of 1[.,]000 MB\) · tracker 1 of 2\. Syncs wait/);
  assert.equal(item.icon, "database");
  assert.ok([...app.timers.values()].some(timer => timer.ms === 3000));
  await app.publish(null, 200, {...move, state: "completed", finished_at: 9, results: [{instance: "default", backup: "json-backup-20261104T100000Z"}]});
  assert.match(app.latest().items[0].detail, /original files are in json-backup-20261104T100000Z/);
  assert.notEqual(app.latest().items[0].revision, item.revision);
  assert.deepEqual([...app.dispatched], ["cw:tracker-migrated"]);
});

test("a tracker that could not be moved says which file to fix", async () => {
  const app = await setup();
  await app.publish({id: "c9", state: "completed", attention: 0, finished_at: 3}, 200,
    {id: "m2", state: "failed", results: [{instance: "CW-P01", backup: "json-backup-x"}], errors: [{instance: "default", file: "history.json"}]});
  const [move, check] = app.latest().items;
  assert.match(move.title, /needs attention/);
  assert.match(move.detail, /history\.json could not be read, so the tracker still uses its old files.*1 other tracker was moved/);
  assert.match(check.title, /Metadata check - completed/);
});

test("an import shows its phase, then how much it added", async () => {
  const app = await setup();
  const run = {id: "i1", state: "running", instance: "CW-P01", label: "Desk", source: "TRAKT", source_label: "Trakt", source_instance: "default", phase: "reading", started_at: 4};
  await app.publish(null, 200, null, run);
  const item = app.latest().items[0];
  assert.equal(item.id, "import:i1");
  assert.match(item.title, /Import - running into Desk/);
  assert.match(item.detail, /From Trakt\. Reading Trakt and matching IDs\./);
  assert.ok([...app.timers.values()].some(timer => timer.ms === 3000));
  await app.publish(null, 200, null, {...run, phase: "writing", done: 1200, total: 4800});
  assert.match(app.latest().items[0].detail, /Writing 1[.,]200 of 4[.,]800\./);
  assert.equal(app.latest().items[0].revision, item.revision);
  await app.publish(null, 200, null, {...run, state: "completed", added: 4700, unresolved: 3, finished_at: 9});
  const done = app.latest().items[0];
  assert.match(done.title, /Import - completed into Desk/);
  assert.match(done.detail, /^4[.,]700 records added from Trakt\. 3 records could not be matched\.$/);
  assert.notEqual(done.revision, item.revision);
  await app.publish(null, 200, null, {...run, state: "interrupted"});
  assert.match(app.latest().items[0].detail, /restarted during the import/);
});

test("managed users with a tracker of their own get its notifications", async () => {
  const app = await setup("user", "on");
  assert.equal(app.requests(), 1);
  await app.publish({id: "c7", instance: "CW-P01", label: "Ann", state: "completed", attention: 2, finished_at: 5});
  assert.match(app.latest().items[0].title, /needs attention \(Ann\)/);
});
